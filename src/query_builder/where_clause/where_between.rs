use smallvec::SmallVec;
use sqlparser::ast::Expr as SqlExpr;

use crate::query_builder::QueryBuilder;
use crate::query_builder::args::IntoQBArg;

impl QueryBuilder {
    pub fn where_between<T, L, H>(mut self, target: T, low: L, high: H) -> Self
    where
        T: IntoQBArg,
        L: IntoQBArg,
        H: IntoQBArg,
    {
        if let (Ok((t, mut pt)), Ok((l, mut pl)), Ok((h, mut ph))) = (
            self.resolve_qbarg_into_expr(target.into_qb_arg()),
            self.resolve_qbarg_into_expr(low.into_qb_arg()),
            self.resolve_qbarg_into_expr(high.into_qb_arg()),
        ) {
            let pred = SqlExpr::Between {
                expr: Box::new(t),
                low: Box::new(l),
                high: Box::new(h),
                negated: false,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pt);
            buf.append(&mut pl);
            buf.append(&mut ph);
            self.attach_where_with_and(pred, buf);
        }
        self
    }

    pub fn or_where_between<T, L, H>(mut self, target: T, low: L, high: H) -> Self
    where
        T: IntoQBArg,
        L: IntoQBArg,
        H: IntoQBArg,
    {
        if let (Ok((t, mut pt)), Ok((l, mut pl)), Ok((h, mut ph))) = (
            self.resolve_qbarg_into_expr(target.into_qb_arg()),
            self.resolve_qbarg_into_expr(low.into_qb_arg()),
            self.resolve_qbarg_into_expr(high.into_qb_arg()),
        ) {
            let pred = SqlExpr::Between {
                expr: Box::new(t),
                low: Box::new(l),
                high: Box::new(h),
                negated: false,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pt);
            buf.append(&mut pl);
            buf.append(&mut ph);
            self.attach_where_with_or(pred, buf);
        }
        self
    }

    pub fn where_not_between<T, L, H>(mut self, target: T, low: L, high: H) -> Self
    where
        T: IntoQBArg,
        L: IntoQBArg,
        H: IntoQBArg,
    {
        if let (Ok((t, mut pt)), Ok((l, mut pl)), Ok((h, mut ph))) = (
            self.resolve_qbarg_into_expr(target.into_qb_arg()),
            self.resolve_qbarg_into_expr(low.into_qb_arg()),
            self.resolve_qbarg_into_expr(high.into_qb_arg()),
        ) {
            let pred = SqlExpr::Between {
                expr: Box::new(t),
                low: Box::new(l),
                high: Box::new(h),
                negated: true,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pt);
            buf.append(&mut pl);
            buf.append(&mut ph);
            self.attach_where_with_and(pred, buf);
        }
        self
    }

    pub fn or_where_not_between<T, L, H>(mut self, target: T, low: L, high: H) -> Self
    where
        T: IntoQBArg,
        L: IntoQBArg,
        H: IntoQBArg,
    {
        if let (Ok((t, mut pt)), Ok((l, mut pl)), Ok((h, mut ph))) = (
            self.resolve_qbarg_into_expr(target.into_qb_arg()),
            self.resolve_qbarg_into_expr(low.into_qb_arg()),
            self.resolve_qbarg_into_expr(high.into_qb_arg()),
        ) {
            let pred = SqlExpr::Between {
                expr: Box::new(t),
                low: Box::new(l),
                high: Box::new(h),
                negated: true,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pt);
            buf.append(&mut pl);
            buf.append(&mut ph);
            self.attach_where_with_or(pred, buf);
        }
        self
    }
}
