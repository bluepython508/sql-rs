#![feature(const_fn_trait_bound, const_fn_fn_ptr_basics)]
use std::marker::PhantomData;

pub use anyhow::Result;

use async_trait::async_trait;
pub use derive::Table;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Ordering {
    Ascending,
    Descending,
}

mod sqlite;
use db_value::DbType;
pub use sqlite::Sqlite;
mod postgres;
pub use postgres::Postgres;

mod db_value;
pub use db_value::DbValue;

#[async_trait]
pub trait Database {
    type Connection: Clone + Sync;

    type Context: Default;

    fn param(ctx: &mut Self::Context) -> String;

    async fn execute(
        connection: &Self::Connection,
        query: String,
        params: Vec<DbValue>,
    ) -> Result<()>;

    async fn query<T: Table, Columns: ColumnTuple<T>, U: From<Columns::Out>>(
        connection: &Self::Connection,
        columns: Columns,
        query: String,
        params: Vec<DbValue>,
    ) -> Result<Vec<U>>
    where
        U: Send + 'static;
}

pub struct Pool<Db: Database>(Db::Connection);

impl<Db: Database> Clone for Pool<Db> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<Db: Database> Pool<Db> {
    pub fn into_inner(self) -> Db::Connection {
        self.0
    }

    pub fn from_connection(connection: Db::Connection) -> Self {
        Self(connection)
    }
}

pub trait Table: 'static + Sized {
    const TABLE_NAME: &'static str;
    type Columns: ColumnTuple<Self>;
    const COLUMNS: Self::Columns;
}

pub trait DbColumnType: 'static + Sized {
    fn from_db(db_value: &DbValue) -> Result<Self>;
    fn to_db(&self) -> DbValue;

    fn db_type() -> DbType;
}

pub mod create_table;
pub mod delete;
pub mod insert;
pub mod select;
pub mod update;

mod sealed_dyncolumn {
    pub trait Sealed {}
}
pub trait DynColumn<T: Table>: sealed_dyncolumn::Sealed {
    fn name(&self) -> &'static str;
    fn db_type(&self) -> DbType;
    fn foreign_key(&self) -> Option<String>;
    fn unique(&self) -> bool;
}

pub trait ForeignKey<U: DbColumnType, V>: sealed_dyncolumn::Sealed {
    fn table(&self) -> &'static str;
    fn column(&self) -> &'static str;

    fn spec(&self) -> String {
        format!("\"{}\"(\"{}\")", self.table(), self.column())
    }
}

pub struct Column<T: Table, U: DbColumnType, V: 'static> {
    phantom: PhantomData<fn(T) -> T>,
    name: &'static str,
    foreign_key: ForeignKeySpec<U, V>,
    unique: bool,
    conversion: Conversion<U, V>,
}

impl<T, U, V> Clone for Column<T, U, V>
where
    T: Table,
    U: DbColumnType,
    V: 'static,
{
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, U, V> Copy for Column<T, U, V>
where
    T: Table,
    U: DbColumnType,
    V: 'static,
{
}

type ForeignKeySpec<U, V> = Option<(
    &'static (dyn ForeignKey<U, V> + Sync),
    Option<&'static str>,
    Option<&'static str>,
)>;
type Conversion<U, V> = (fn(V) -> U, fn(U) -> Result<V>);

impl<T, U, V> Column<T, U, V>
where
    T: Table,
    U: DbColumnType,
{
    pub const fn new(
        name: &'static str,
        foreign_key: ForeignKeySpec<U, V>,
        unique: bool,
        conversion: Conversion<U, V>,
    ) -> Self {
        Self {
            phantom: PhantomData,
            name,
            foreign_key,
            unique,
            conversion,
        }
    }

    pub fn to_db(&self, value: V) -> DbValue {
        self.conversion.0(value).to_db()
    }

    pub fn from_db(&self, value: DbValue) -> Result<V> {
        self.conversion.1(U::from_db(&value)?)
    }
}

impl<T: Table, U: DbColumnType, V> sealed_dyncolumn::Sealed for Column<T, U, V> {}
impl<T: Table, U: DbColumnType, V> DynColumn<T> for Column<T, U, V> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn db_type(&self) -> DbType {
        U::db_type()
    }

    fn foreign_key(&self) -> Option<String> {
        self.foreign_key.map(|(k, on_update, on_delete)| {
            format!(
                "REFERENCES {} ON UPDATE {} ON DELETE {}",
                k.spec(),
                on_update.unwrap_or("NO ACTION"),
                on_delete.unwrap_or("NO ACTION")
            )
        })
    }

    fn unique(&self) -> bool {
        self.unique
    }
}

impl<T: Table, U: DbColumnType, V> ForeignKey<U, V> for Column<T, U, V> {
    fn table(&self) -> &'static str {
        T::TABLE_NAME
    }

    fn column(&self) -> &'static str {
        self.name()
    }
}

mod cond_expr;

pub(crate) use cond_expr::build_condition_query;
pub use cond_expr::CondExpr;

mod column_tuple;
pub use column_tuple::ColumnTuple;
