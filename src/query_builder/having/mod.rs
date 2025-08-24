mod __tests__;
mod core_fn;
mod having_between;
mod having_exists;
mod having_in;
mod having_null;

use super::where_clause::utils::parse_where_expr;
use crate::query_builder::QueryBuilder;
use crate::query_builder::args::ArgList;

pub(crate) use core_fn::HavingNode;
use smallvec::SmallVec;

impl<'a, T> QueryBuilder<'a, T> {
    /// HAVING <exprs...>  (группа объединяется через AND)
    pub fn having<A>(mut self, args: A) -> Self
    where
        A: ArgList<'a>,
    {
        if let Some((pred, params)) = self.resolve_having_group(args) {
            self.attach_having_with_and(pred, params);
        }
        self
    }

    /// OR HAVING <exprs...>  (группа объединяется через AND, затем OR с текущим having)
    pub fn or_having<A>(mut self, args: A) -> Self
    where
        A: ArgList<'a>,
    {
        if let Some((pred, params)) = self.resolve_having_group(args) {
            self.attach_having_with_or(pred, params);
        }
        self
    }

    /// HAVING <raw SQL> — парсится GenericDialect'ом (через `SELECT 1 WHERE <raw>`).
    pub fn having_raw(mut self, raw: &str) -> Self {
        match parse_where_expr(raw) {
            Ok(expr) => self.attach_having_with_and(expr, SmallVec::new()),
            Err(e) => self.push_builder_error(format!("having_raw(): {}", e)),
        }
        self
    }

    /// OR HAVING <raw SQL>
    pub fn or_having_raw(mut self, raw: &str) -> Self {
        match parse_where_expr(raw) {
            Ok(expr) => self.attach_having_with_or(expr, SmallVec::new()),
            Err(e) => self.push_builder_error(format!("or_having_raw(): {}", e)),
        }
        self
    }

    pub fn and_having<A>(self, args: A) -> Self
    where
        A: ArgList<'a>,
    {
        self.having(args)
    }
}
