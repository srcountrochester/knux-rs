use smallvec::SmallVec;
use sqlparser::ast::Expr as SqlExpr;

use crate::query_builder::QueryBuilder;
use crate::query_builder::args::IntoQBArg;

impl QueryBuilder {
    pub fn where_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNull(Box::new(e));
            self.attach_where_with_and(pred, p);
        }
        self
    }

    pub fn or_where_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNull(Box::new(e));
            self.attach_where_with_or(pred, p);
        }
        self
    }

    pub fn where_not_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNotNull(Box::new(e));
            self.attach_where_with_and(pred, p);
        }
        self
    }

    pub fn or_where_not_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNotNull(Box::new(e));
            self.attach_where_with_or(pred, p);
        }
        self
    }
}
