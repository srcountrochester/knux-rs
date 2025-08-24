use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, IntoQBArg};

impl<'a, T> QueryBuilder<'a, T> {
    /// HAVING <expr> IN (<values...>) | (<subquery>)
    pub fn having_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, false) {
            self.attach_having_with_and(pred, params);
        }
        self
    }

    /// OR HAVING <expr> IN (...)
    pub fn or_having_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, false) {
            self.attach_having_with_or(pred, params);
        }
        self
    }

    /// HAVING <expr> NOT IN (...)
    pub fn having_not_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, true) {
            self.attach_having_with_and(pred, params);
        }
        self
    }

    /// OR HAVING <expr> NOT IN (...)
    pub fn or_having_not_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        if let Some((pred, params)) = self.build_in_predicate(column.into_qb_arg(), values, true) {
            self.attach_having_with_or(pred, params);
        }
        self
    }
}
