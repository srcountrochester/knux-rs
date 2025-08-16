use sqlparser::ast::Expr as SqlExpr;

use crate::query_builder::QueryBuilder;
use crate::query_builder::args::IntoQBArg;

impl QueryBuilder {
    pub fn having_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, mut p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNull(Box::new(e));
            self.params.append(&mut p);
            self.attach_having_with_and(pred);
        }
        self
    }

    pub fn or_having_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, mut p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNull(Box::new(e));
            self.params.append(&mut p);
            self.attach_having_with_or(pred);
        }
        self
    }

    pub fn having_not_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, mut p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNotNull(Box::new(e));
            self.params.append(&mut p);
            self.attach_having_with_and(pred);
        }
        self
    }

    pub fn or_having_not_null<T>(mut self, expr: T) -> Self
    where
        T: IntoQBArg,
    {
        if let Ok((e, mut p)) = self.resolve_qbarg_into_expr(expr.into_qb_arg()) {
            let pred = SqlExpr::IsNotNull(Box::new(e));
            self.params.append(&mut p);
            self.attach_having_with_or(pred);
        }
        self
    }
}
