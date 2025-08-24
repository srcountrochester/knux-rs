use smallvec::smallvec;

use super::utils::parse_where_expr;
use crate::query_builder::QueryBuilder;

impl<'a, T> QueryBuilder<'a, T> {
    /// WHERE <raw SQL>, парсится через sqlparser (GenericDialect).
    pub fn where_raw(mut self, raw: &str) -> Self {
        match parse_where_expr(raw) {
            Ok(expr) => self.attach_where_with_and(expr, smallvec![]),
            Err(e) => self.push_builder_error(format!("where_raw(): {}", e)),
        }
        self
    }

    pub fn or_where_raw(mut self, raw: &str) -> Self {
        match parse_where_expr(raw) {
            Ok(expr) => self.attach_where_with_or(expr, smallvec![]),
            Err(e) => self.push_builder_error(format!("or_where_raw(): {}", e)),
        }
        self
    }
}
