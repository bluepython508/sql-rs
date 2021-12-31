use std::marker::PhantomData;

use crate::{ColumnTuple, Database, Pool, Result, Table};

pub struct CreateTableBuilder<'pool, Db: Database, T: Table> {
    pool: &'pool Pool<Db>,
    phantom: PhantomData<T>,
    if_not_exists: bool,
}

impl<Db: Database> Pool<Db> {
    pub fn create<T: Table>(&self) -> CreateTableBuilder<'_, Db, T> {
        CreateTableBuilder {
            pool: self,
            phantom: PhantomData,
            if_not_exists: false,
        }
    }
}

impl<Db: Database, T: Table> CreateTableBuilder<'_, Db, T> {
    #[must_use]
    pub fn if_not_exists(self) -> Self {
        Self {
            if_not_exists: true,
            ..self
        }
    }
}

impl<Db: Database, T: Table> CreateTableBuilder<'_, Db, T> {
    fn build_query(&self) -> String {
        let colspec = {
            let mut spec = vec![];
            T::COLUMNS.apply_columns(|col| {
                spec.push(format!(
                    "\"{}\" {} {} {}",
                    col.name(),
                    col.db_type().name(),
                    col.foreign_key().unwrap_or_default(),
                    if col.unique() { "UNIQUE" } else { "" },
                ))
            });
            spec.join(", ")
        };
        format!(
            "CREATE TABLE {} \"{}\"({})",
            if self.if_not_exists {
                "IF NOT EXISTS"
            } else {
                ""
            },
            T::TABLE_NAME,
            colspec
        )
    }

    pub async fn execute(self) -> Result<()> {
        let query = self.build_query();
        Db::execute(&self.pool.0, query, vec![]).await
    }
}
