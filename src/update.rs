use std::marker::PhantomData;

use crate::{
    Column, CondExpr, Database, DbColumnType, DbValue, DynColumn,
    Pool, Table, build_condition_query, Result
};

pub struct UpdateBuilder<'pool, Db: Database, T: Table> {
    pool: &'pool Pool<Db>,
    phantom: PhantomData<T>,
    set: Vec<(Box<dyn DynColumn<T> + Send>, DbValue)>,
    condition: CondExpr<T>,
}

impl<Db: Database> Pool<Db> {
    pub fn update<T: Table>(&self) -> UpdateBuilder<'_, Db, T> {
        UpdateBuilder {
            pool: self,
            phantom: PhantomData,
            set: vec![],
            condition: CondExpr::TRUE,
        }
    }
}

impl<Db: Database, T: Table> UpdateBuilder<'_, Db, T> {
    #[must_use]
    pub fn set<U: DbColumnType, V>(mut self, column: Column<T, U, V>, value: V) -> Self {
        self.set.push((Box::new(column), column.to_db(value)));
        self
    }

    #[must_use]
    pub fn r#where(self, condition: CondExpr<T>) -> Self {
        Self { condition, ..self }
    }
}

impl<Db: Database, T: Table> UpdateBuilder<'_, Db, T> {
    fn build_query(self) -> (String, Vec<DbValue>) {
        let mut next_param = {
            let mut context = Default::default();
            move || Db::param(&mut context)
        };
        let mut params = vec![];
        let set_spec = self.set
            .into_iter()
            .map(|(col, val)| {
                params.push(val);
                format!("\"{}\" = {}", col.name(), next_param())
            })
            .collect::<Vec<_>>()
            .join(", ");
        let cond = build_condition_query(
            self.condition.0,
            &mut params,
            &mut next_param,
        );
        let query = format!(
            "UPDATE \"{}\" SET {} WHERE {}",
            T::TABLE_NAME,
            set_spec,
            cond
        );
        (query, params)
    }

    pub async fn execute(self) -> Result<()> {
        if self.set.len() == 0 { return Ok(()) }
        let pool = self.pool;
        let (query, params) = self.build_query();
        Db::execute(&pool.0, query, params).await
    }
}
