use crate::{db_value::DbType, Column, DbColumnType, DbValue, DynColumn, Result, Table};

macro_rules! count {
    () => { 0 };
    ($t:tt $($rest:tt)*) => {
        1 + count!($($rest)*)
    }
}

pub trait Sealed {}
pub trait ColumnTuple<T: Table>: Sealed + Copy + Send + Sync + 'static {
    type Out;

    const N: usize;

    fn try_from_values(&self, values: impl FnMut(DbType) -> DbValue) -> Result<Self::Out>;

    fn to_values(&self, values: Self::Out) -> Vec<DbValue>;

    fn apply_columns<'a>(&'a self, f: impl FnMut(&'a dyn DynColumn<T>));
}

macro_rules! impl_tuple {
    ($($types:ident)*) => {
        paste::paste! {
            impl<Ta: Table, $([< DBTy $types >]: DbColumnType, [< RSTy $types >]),*> Sealed for ($(Column<Ta, [< DBTy $types >], [< RSTy $types >]>,)*) {}
            impl<Ta: Table, $([< DBTy $types >]: DbColumnType, [< RSTy $types >]),*> ColumnTuple<Ta> for ($(Column<Ta, [< DBTy $types >], [< RSTy $types >]>,)*) {
                type Out = ($([< RSTy $types >],)*);
                const N: usize = count!($($types)*);

                fn try_from_values(&self, mut values: impl FnMut(DbType) -> DbValue) -> Result<Self::Out> {
                    #[allow(non_snake_case)]
                    let ($($types,)*) = self;
                    Ok((
                        $(
                            $types.from_db(values([< DBTy $types >]::db_type()))?,
                        )*
                    ))
                }

                fn to_values(&self, values: Self::Out) -> Vec<DbValue> {
                    #![allow(non_snake_case)]
                    let ($([< Col $types >],)*) = self;
                    let ($([< Val $types >],)*) = values;
                    vec![
                        $([< Col $types >].to_db([< Val $types >])),*
                    ]
                }

                fn apply_columns<'a>(&'a self, mut f: impl FnMut(&'a dyn DynColumn<Ta>)) {
                    #[allow(non_snake_case)]
                    let ($($types,)*) = self;
                    $(f($types);)*
                }
            }
        }
    }
}

macro_rules! for_idents {
    ($macro:ident; $id:ident $($rest:ident)*) => {
        $macro! {
            $id $($rest)*
        }
        for_idents! {
            $macro; $($rest)*
        }
    };
    ($macro:ident; ) => {}
}

for_idents!(impl_tuple; A B C D E F G H I J K L M N O P Q R S T U V W X Y Z);
