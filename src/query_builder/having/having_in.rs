use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, IntoQBArg};

impl QueryBuilder {
    /// HAVING <expr> IN (<values...>) | (<subquery>)
    pub fn having_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some(pred) = self.build_in_predicate(column.into_qb_arg(), values, false) {
            self.attach_having_with_and(pred);
        }
        self
    }

    /// OR HAVING <expr> IN (...)
    pub fn or_having_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some(pred) = self.build_in_predicate(column.into_qb_arg(), values, false) {
            self.attach_having_with_or(pred);
        }
        self
    }

    /// HAVING <expr> NOT IN (...)
    pub fn having_not_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some(pred) = self.build_in_predicate(column.into_qb_arg(), values, true) {
            self.attach_having_with_and(pred);
        }
        self
    }

    /// OR HAVING <expr> NOT IN (...)
    pub fn or_having_not_in<C, A>(mut self, column: C, values: A) -> Self
    where
        C: IntoQBArg,
        A: ArgList,
    {
        if let Some(pred) = self.build_in_predicate(column.into_qb_arg(), values, true) {
            self.attach_having_with_or(pred);
        }
        self
    }
}
