use std::marker::PhantomData;

use crate::{cond_expr::build_condition_query, CondExpr, Database, DbValue, Pool, Table, Result};

pub struct DeleteBuilder<'pool, Db: Database, T: Table> {
    pool: &'pool Pool<Db>,
    phantom: PhantomData<T>,
    condition: CondExpr<T>,
}

impl<Db: Database> Pool<Db> {
    pub fn delete_where<T: Table>(&self, condition: CondExpr<T>) -> DeleteBuilder<'_, Db, T> {
        DeleteBuilder {
            pool: self,
            phantom: PhantomData,
            condition,
        }
    }
}

impl<Db: Database, T: Table> DeleteBuilder<'_, Db, T> {
    fn build_query(self) -> (String, Vec<DbValue>) {
        let mut params = vec![];
        let mut next_param = {
            let mut context = Default::default();
            move || Db::param(&mut context)
        };
        let condition = build_condition_query(
            self.condition.0,
            &mut params,
            &mut next_param,
        );
        let query = format!("DELETE FROM \"{}\" WHERE {}", T::TABLE_NAME, condition);
        (query, params)
    }

    pub async fn execute(self) -> Result<()> {
        let pool = self.pool;
        let (query, params) = self.build_query();
        Db::execute(&pool.0, query, params).await?;
        Ok(())
    }
}
