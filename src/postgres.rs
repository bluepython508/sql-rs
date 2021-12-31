use std::{error::Error, str::FromStr};

use crate::{
    db_value::{DbType, DbTypeE},
    ColumnTuple, Database, DbValue, Pool, Result, Table,
};
use anyhow::anyhow;
use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig};
use deadpool_sqlite::rusqlite::types::Value;
use futures::{StreamExt, TryStreamExt};
use tokio_postgres::{
    types::{FromSql, Type},
    Config, NoTls,
};

pub struct Postgres;
#[async_trait]
impl Database for Postgres {
    type Connection = deadpool::managed::Pool<deadpool_postgres::Manager>;

    type Context = usize;

    fn param(ctx: &mut Self::Context) -> String {
        *ctx += 1;
        format!("${}", *ctx)
    }

    async fn execute(
        connection: &Self::Connection,
        query: String,
        params: Vec<DbValue>,
    ) -> Result<()> {
        connection.get().await?.execute_raw(&query, params).await?;
        Ok(())
    }

    async fn query<T, Columns, U>(
        connection: &Self::Connection,
        columns: Columns,
        query: String,
        params: Vec<DbValue>,
    ) -> Result<Vec<U>>
    where
        T: Table,
        Columns: ColumnTuple<T>,
        U: From<Columns::Out>,
        U: Send,
    {
        Ok(connection
            .get()
            .await?
            .query_raw(&query, params)
            .await?
            .map(|row| {
                let mut n = 0usize;
                let row = row?;
                columns.try_from_values(|db_type: DbType| {
                    n += 1;
                    DbValue::from_postgres_value(row.get(n - 1), db_type).unwrap()
                })
            })
            .map(|r| r.map(U::from))
            .try_collect()
            .await?)
    }
}

impl Pool<Postgres> {
    pub async fn connect(connection_string: impl AsRef<str>) -> Result<Self> {
        let pool = deadpool::managed::Pool::builder(Manager::from_config(
            Config::from_str(connection_string.as_ref())?,
            NoTls,
            ManagerConfig {
                recycling_method: deadpool_postgres::RecyclingMethod::Fast,
            },
        ))
        .build()?;
        Ok(Self(pool))
    }
}

mod to_sql {
    use std::error::Error;

    use deadpool_sqlite::rusqlite::types::Value;
    use tokio_postgres::types::{private::BytesMut, to_sql_checked, IsNull, ToSql, Type};

    use crate::DbValue;

    impl ToSql for DbValue {
        fn to_sql(
            &self,
            ty: &Type,
            out: &mut BytesMut,
        ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
            match &self.0 {
                Value::Null => Ok(IsNull::Yes),
                Value::Integer(i) => i.to_sql(ty, out),
                Value::Real(r) => r.to_sql(ty, out),
                Value::Text(t) => t.to_sql(ty, out),
                Value::Blob(b) => b.to_sql(ty, out),
            }
        }

        fn accepts(ty: &Type) -> bool {
            i64::accepts(ty) || f64::accepts(ty) || String::accepts(ty)
        }

        to_sql_checked!();
    }
}
struct PostgresValue<'a>(Type, Option<&'a [u8]>);

impl<'a> FromSql<'a> for PostgresValue<'a> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(ty.clone(), Some(raw)))
    }

    fn accepts(ty: &Type) -> bool {
        i64::accepts(ty) || f64::accepts(ty) || String::accepts(ty)
    }

    fn from_sql_null(ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(ty.clone(), None))
    }

    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&'a [u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(ty.clone(), raw))
    }
}

impl DbValue {
    fn from_postgres_value(val: PostgresValue<'_>, db_type: DbType) -> super::Result<Self> {
        fn from_raw(pg_ty: Type, val: Option<&[u8]>, ty: &DbTypeE) -> super::Result<DbValue> {
            match (val, ty) {
                (None, DbTypeE::Nullable(_)) => Ok(DbValue(Value::Null)),
                (Some(raw), &DbTypeE::Nullable(ref t)) => from_raw(pg_ty, Some(raw), &*t),
                (Some(raw), &DbTypeE::Integer) if i64::accepts(&pg_ty) => Ok(DbValue(
                    Value::Integer(i64::from_sql(&pg_ty, raw).map_err(|e| anyhow!(e))?),
                )),
                (Some(raw), &DbTypeE::Text) if String::accepts(&pg_ty) => Ok(DbValue(Value::Text(
                    String::from_sql(&pg_ty, raw).map_err(|e| anyhow!(e))?,
                ))),
                (Some(raw), &DbTypeE::Real) if f64::accepts(&pg_ty) => Ok(DbValue(Value::Real(
                    f64::from_sql(&pg_ty, raw).map_err(|e| anyhow!(e))?,
                ))),
                (None, _) => Err(anyhow!(
                    "Unexpected null for type {:?} (PG type {})",
                    ty,
                    pg_ty
                )),
                (Some(_), _) => Err(anyhow!("Invalid PG type {} for type {:?}", pg_ty, ty)),
            }
        }

        from_raw(val.0, val.1, &db_type.0)
    }
}
