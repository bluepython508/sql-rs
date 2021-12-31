use std::marker::PhantomData;

use crate::{ColumnTuple, Database, DbValue, Pool, Table, Result};

pub struct InsertBuilder<'pool, Db: Database, T: Table, Columns: ColumnTuple<T>> {
    pool: &'pool Pool<Db>,
    phantom: PhantomData<T>,
    columns: Columns,
    values: Vec<Columns::Out>,
}

impl<Db: Database> Pool<Db> {
    pub fn insert_into<T: Table, Columns: ColumnTuple<T>>(
        &self,
        columns: Columns,
    ) -> InsertBuilder<'_, Db, T, Columns> {
        InsertBuilder {
            phantom: PhantomData,
            pool: self,
            columns,
            values: vec![],
        }
    }
}

impl<T: Table, Db: Database, Columns: ColumnTuple<T>> InsertBuilder<'_, Db, T, Columns> {
    #[must_use]
    pub fn values(mut self, values: Columns::Out) -> Self {
        self.values.push(values);
        self
    }
}

impl<T: Table, Db: Database, Columns: ColumnTuple<T>> InsertBuilder<'_, Db, T, Columns> {
    fn build_query(self) -> (String, Vec<DbValue>) {
        let columns = {
            let mut names = Vec::with_capacity(Columns::N);
            self.columns
                .apply_columns(|col| names.push(format!("\"{}\"", col.name())));
            names
        };

        let mut next_param = {
            let mut context = Default::default();
            move || Db::param(&mut context)
        };
        let query = format!(
            "INSERT INTO \"{}\"({}) VALUES {}",
            T::TABLE_NAME,
            columns.join(", "),
            (0..self.values.len())
                .map(|_| format!(
                    "({})",
                    (0..Columns::N)
                        .map(|_| next_param())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let params = self.values
            .into_iter()
            .flat_map(|values| self.columns.to_values(values)).collect();
        (query, params)
    }
    
    pub async fn execute(self) -> Result<()> {
        let pool = self.pool;
        let (query, params) = self.build_query();
        Db::execute(&pool.0, query, params).await
    }
}
