use crate::query_builder::args::IntoQBArg;
use crate::query_builder::{QueryBuilder, args::ArgList};

impl QueryBuilder {
    /// WHERE <col> [NOT] IN (<values...>) | (<subquery>)
    /// values принимаем как ArgList: массив/кортеж/вектор значений (Expression/подзапрос/closure).
    pub fn where_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, false) {
            self.attach_where_with_and(pred, params);
        }
        self
    }

    pub fn or_where_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, false) {
            self.attach_where_with_or(pred, params);
        }
        self
    }

    pub fn where_not_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, true) {
            self.attach_where_with_and(pred, params);
        }
        self
    }

    pub fn or_where_not_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, true) {
            self.attach_where_with_or(pred, params);
        }
        self
    }
}
