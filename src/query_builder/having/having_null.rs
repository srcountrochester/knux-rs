use sqlparser::ast::Expr as SqlExpr;

use crate::query_builder::QueryBuilder;
use crate::query_builder::args::IntoQBArg;

impl<'a, K> QueryBuilder<'a, K> {
    pub fn having_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg<'a>,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNull(Box::new(e));
            self.attach_having_with_and(pred, p);
        }
        self
    }

    pub fn or_having_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg<'a>,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNull(Box::new(e));
            self.attach_having_with_or(pred, p);
        }
        self
    }

    pub fn having_not_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg<'a>,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNotNull(Box::new(e));
            self.attach_having_with_and(pred, p);
        }
        self
    }

    pub fn or_having_not_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg<'a>,
    {
        if let Ok((e, p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNotNull(Box::new(e));
            self.attach_having_with_or(pred, p);
        }
        self
    }
}
