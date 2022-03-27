use anyhow::bail;

use crate::{DbColumnType, Result};

use deadpool_sqlite::rusqlite::types::Value;

#[derive(Debug, Clone)]
pub struct DbValue(pub(crate) Value);

#[derive(Debug)]
pub struct DbType(pub(crate) DbTypeE);

impl DbType {
    pub fn name(&self) -> &'static str {
        self.0.name()
    }
}
#[derive(Debug)]
pub(crate) enum DbTypeE {
    Integer,
    Real,
    Text,
    Nullable(Box<DbTypeE>),
}

impl DbTypeE {
    fn name(&self) -> &'static str {
        match self {
            DbTypeE::Integer => "INT8 NOT NULL",
            DbTypeE::Text => "TEXT NOT NULL",
            DbTypeE::Real => "DOUBLE PRECISION NOT NULL",
            DbTypeE::Nullable(t) => t.name().strip_suffix(" NOT NULL").unwrap_or_else(|| t.name()),
        }
    }
}
macro_rules! int_db_column_type {
    ($($t:ty)*) => {
        $(
            impl DbColumnType for $t {
                fn from_db(db_value: &DbValue) -> Result<Self> {
                    match db_value.0 {
                        Value::Integer(i) => Ok(<$t>::try_from(i)?),
                        _ => bail!("Expected integer, found {:?}", db_value.0),
                    }
                }
            
                fn to_db(&self) -> DbValue {
                    DbValue(Value::Integer((*self).into()))
                }
            
                fn db_type() -> DbType {
                    DbType(DbTypeE::Integer)
                }
            }
            
        )*
    }
}

int_db_column_type!(u8 i8 u16 i16 u32 i32 i64);

impl DbColumnType for f32 {
    fn from_db(db_value: &DbValue) -> Result<Self> {
        match db_value.0 {
            Value::Real(i) => Ok(i as _),
            _ => bail!("Expected float, found {:?}", db_value.0),
        }
    }

    fn to_db(&self) -> DbValue {
        DbValue(Value::Real(*self as _))
    }

    fn db_type() -> DbType {
        DbType(DbTypeE::Real)
    }
}

impl DbColumnType for f64 {
    fn from_db(db_value: &DbValue) -> Result<Self> {
        match db_value.0 {
            Value::Real(i) => Ok(i as _),
            _ => bail!("Expected float, found {:?}", db_value.0),
        }
    }

    fn to_db(&self) -> DbValue {
        DbValue(Value::Real(*self as _))
    }

    fn db_type() -> DbType {
        DbType(DbTypeE::Real)
    }
}

impl DbColumnType for String {
    fn from_db(db_value: &DbValue) -> Result<Self> {
        match &db_value.0 {
            Value::Text(t) => Ok(t.clone()),
            _ => bail!("Expected string, found {:?}", db_value.0),
        }
    }

    fn to_db(&self) -> DbValue {
        DbValue(Value::Text(self.clone()))
    }

    fn db_type() -> DbType {
        DbType(DbTypeE::Text)
    }
}

impl<T: DbColumnType> DbColumnType for Option<T> {
    fn from_db(db_value: &DbValue) -> Result<Self> {
        match &db_value.0 {
            Value::Null => Ok(None),
            _ => T::from_db(db_value).map(Some),
        }
    }

    fn to_db(&self) -> DbValue {
        self.as_ref()
            .map(DbColumnType::to_db)
            .unwrap_or(DbValue(Value::Null))
    }

    fn db_type() -> DbType {
        DbType(DbTypeE::Nullable(Box::new(T::db_type().0)))
    }
}
