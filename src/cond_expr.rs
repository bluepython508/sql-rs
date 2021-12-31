use crate::{Column, DbColumnType, DbValue, DynColumn, Table};

pub struct CondExpr<T: Table>(pub(crate) CondExprE<T>);
pub(crate) enum CondExprE<T: Table> {
    ColumnEquals(Box<dyn DynColumn<T> + Send>, DbValue),
    ColumnIsNotNull(Box<dyn DynColumn<T> + Send>),
    ColumnIsNull(Box<dyn DynColumn<T> + Send>),
    All(Vec<CondExprE<T>>),
    Any(Vec<CondExprE<T>>),
    True,
    False,
}

impl<T: Table> CondExpr<T> {
    #[must_use]
    pub fn and(self, other: CondExpr<T>) -> Self {
        Self::all([self, other])
    }
    #[must_use]
    pub fn or(self, other: CondExpr<T>) -> Self {
        Self::any([self, other])
    }
    pub fn all(all: impl IntoIterator<Item = Self>) -> Self {
        Self(CondExprE::All(all.into_iter().map(|x| x.0).collect()))
    }
    pub fn any(any: impl IntoIterator<Item = Self>) -> Self {
        Self(CondExprE::Any(any.into_iter().map(|x| x.0).collect()))
    }

    pub const TRUE: Self = Self(CondExprE::True);
    pub const FALSE: Self = Self(CondExprE::False);
}

impl<T: Table, U: DbColumnType, V> Column<T, U, V> {
    pub fn equals(self, v: V) -> CondExpr<T> {
        let u = self.to_db(v);
        CondExpr(CondExprE::ColumnEquals(Box::new(self), u))
    }
}

impl<T: Table, U: DbColumnType, V> Column<T, Option<U>, V> {
    pub fn is_not_null(self) -> CondExpr<T> {
        CondExpr(CondExprE::ColumnIsNotNull(Box::new(self)))
    }

    pub fn is_null(self) -> CondExpr<T> {
        CondExpr(CondExprE::ColumnIsNull(Box::new(self)))
    }
}


pub(crate) fn build_condition_query(
    cond: CondExprE<impl Table>,
    params: &mut Vec<DbValue>,
    next_param: &mut impl FnMut() -> String,
) -> String {
    match cond {
        CondExprE::ColumnEquals(col, val) => {
            params.push(val);
            format!("\"{}\" = {}", col.name(), next_param())
        }
        CondExprE::ColumnIsNotNull(col) => format!("\"{}\" IS NOT NULL", col.name()),
        CondExprE::ColumnIsNull(col) => format!("\"{}\" IS NULL", col.name()),
        CondExprE::All(conds) => conds
            .into_iter()
            .map(|cond| build_condition_query(cond, params, next_param))
            .collect::<Vec<_>>()
            .join(" AND "),
        CondExprE::Any(conds) => conds
            .into_iter()
            .map(|cond| build_condition_query(cond, params, next_param))
            .collect::<Vec<_>>()
            .join(" OR "),
        CondExprE::True => "TRUE".to_owned(),
        CondExprE::False => "FALSE".to_owned(),
    }
}