use std::path::Path;

use async_trait::async_trait;
use deadpool::managed::{Hook, HookError, HookErrorCause, Manager};
use deadpool_sqlite::rusqlite::params_from_iter;
use futures::FutureExt;

use crate::{ColumnTuple, Database, DbValue, Pool, Result, Table};

pub struct Sqlite;

#[async_trait]
impl Database for Sqlite {
    type Connection = deadpool::managed::Pool<deadpool_sqlite::Manager>;

    type Context = ();

    fn param(_: &mut Self::Context) -> String {
        "?".to_owned()
    }

    async fn execute(
        connection: &Self::Connection,
        query: String,
        params: Vec<DbValue>,
    ) -> Result<()> {
        connection
            .get()
            .await?
            .interact(move |conn| {
                conn.execute(&query, params_from_iter(params.into_iter().map(|d| d.0)))
            })
            .await
            .unwrap()?; // The unwrap unwraps an `InteractError`, only given if the closure above panics or aborts
        Ok(())
    }

    async fn query<T: Table, Columns: ColumnTuple<T>, U: From<Columns::Out>>(
        connection: &Self::Connection,
        columns: Columns,
        query: String,
        params: Vec<DbValue>,
    ) -> Result<Vec<U>>
    where
        U: 'static + Send,
    {
        connection
            .get()
            .await?
            .interact(move |conn| {
                conn.prepare(&query)?
                    .query_map(params_from_iter(params.into_iter().map(|d| d.0)), |row| {
                        let mut idx: usize = 0;
                        Ok(columns.try_from_values(|_| {
                            idx += 1;
                            DbValue(row.get_unwrap(idx - 1))
                        }))
                    })?
                    .map(|r| r?)
                    .map(|u| u.map(U::from))
                    .collect::<Result<_, _>>()
            })
            .await
            .unwrap()
    }
}

impl Pool<Sqlite> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let pool = deadpool_sqlite::Config::new(path.as_ref())
            .builder(deadpool::Runtime::Tokio1)?
            .post_create(Hook::async_fn(
                |conn: &mut <deadpool_sqlite::Manager as Manager>::Type, _| {
                    async {
                        conn.interact(|conn| conn.execute("PRAGMA foreign_keys=on;", []).unwrap())
                            .await
                            .map_err(|_| {
                                HookError::Abort(HookErrorCause::StaticMessage(
                                    "Failed to setup pragmas",
                                ))
                            })?;
                        Ok(())
                    }
                    .boxed()
                },
            ))
            .build()?;
        Ok(Self(pool))
    }

    pub fn in_memory() -> Self {
        Self::open(":memory:").unwrap()
    }
}
