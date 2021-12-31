use crate::{
    column_tuple::ColumnTuple, Column, CondExpr, Database, DbColumnType, DbValue,
    DynColumn, Ordering, Pool, Table, build_condition_query, Result
};
use std::marker::PhantomData;



pub struct QueryBuilder<'pool, Db: Database, T: Table, Columns> {
    pool: &'pool Pool<Db>,
    table: PhantomData<T>,
    columns: Columns,
    condition: CondExpr<T>,
    limit: Option<usize>,
    ordering: Option<(Box<dyn DynColumn<T> + 'pool + Send>, Ordering)>,
}

impl<Db: Database> Pool<Db> {
    pub fn select<T, Columns: ColumnTuple<T>>(
        &self,
        columns: Columns,
    ) -> QueryBuilder<'_, Db, T, Columns>
    where
        T: Table,
    {
        QueryBuilder {
            table: PhantomData,
            pool: self,
            columns,
            condition: CondExpr::TRUE,
            limit: None,
            ordering: None,
        }
    }
}

impl<T: Table, Db: Database, Columns: ColumnTuple<T>> QueryBuilder<'_, Db, T, Columns> {
    #[must_use]
    pub fn r#where(self, condition: CondExpr<T>) -> Self {
        Self { condition, ..self }
    }

    #[must_use]
    pub fn order_by<U: DbColumnType, V>(self, column: Column<T, U, V>, ordering: Ordering) -> Self {
        Self {
            ordering: Some((Box::new(column), ordering)),
            ..self
        }
    }

    #[must_use]
    pub fn limit(self, limit: impl Into<Option<usize>>) -> Self {
        Self {
            limit: limit.into(),
            ..self
        }
    }
}

impl<T: Table, Db: Database, Columns: ColumnTuple<T>> QueryBuilder<'_, Db, T, Columns> {
    fn build_query(
        self,
    ) -> (String, Vec<DbValue>) {
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
        let mut params = vec![];
        let condition = build_condition_query(
            self.condition.0,
            &mut params,
            &mut next_param,
        );
        let order_by = if let Some((col, dir)) = &self.ordering {
            format!(
                "ORDER BY \"{}\" {}",
                col.name(),
                match dir {
                    Ordering::Ascending => "ASC",
                    Ordering::Descending => "DESC",
                }
            )
        } else {
            String::new()
        };

        let limit = if let Some(limit) = self.limit {
            format!("LIMIT {}", limit)
        } else {
            String::new()
        };
        let query = format!(
            "SELECT {} FROM \"{}\" WHERE {} {} {}",
            columns.join(", "),
            T::TABLE_NAME,
            condition,
            order_by,
            limit
        );

        (query, params)
    }

    pub async fn fetch_all<U: From<Columns::Out> + Send + 'static>(self) -> Result<Vec<U>> {
        let pool = self.pool;
        let columns = self.columns;
        let (query, params) = self.build_query();
        Db::query::<T, Columns, U>(&pool.0, columns, query, params).await
    }
}
