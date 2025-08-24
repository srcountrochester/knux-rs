use smallvec::SmallVec;
use sqlparser::ast::Expr as SqlExpr;

use crate::query_builder::QueryBuilder;
use crate::query_builder::args::IntoQBArg;

impl<'a, T> QueryBuilder<'a, T> {
    pub fn where_like<L, R>(mut self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        if let (Ok((l, mut pl)), Ok((r, mut pr))) = (
            self.resolve_qbarg_into_expr(left.into_qb_arg()),
            self.resolve_qbarg_into_expr(pattern.into_qb_arg()),
        ) {
            let pred = SqlExpr::Like {
                negated: false,
                any: false,
                expr: Box::new(l),
                pattern: Box::new(r),
                escape_char: None,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pl);
            buf.append(&mut pr);
            self.attach_where_with_and(pred, buf);
        }
        self
    }

    pub fn or_where_like<L, R>(mut self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        if let (Ok((l, mut pl)), Ok((r, mut pr))) = (
            self.resolve_qbarg_into_expr(left.into_qb_arg()),
            self.resolve_qbarg_into_expr(pattern.into_qb_arg()),
        ) {
            let pred = SqlExpr::Like {
                negated: false,
                any: false,
                expr: Box::new(l),
                pattern: Box::new(r),
                escape_char: None,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pl);
            buf.append(&mut pr);
            self.attach_where_with_or(pred, buf);
        }
        self
    }

    pub fn where_ilike<L, R>(mut self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        if let (Ok((l, mut pl)), Ok((r, mut pr))) = (
            self.resolve_qbarg_into_expr(left.into_qb_arg()),
            self.resolve_qbarg_into_expr(pattern.into_qb_arg()),
        ) {
            let pred = SqlExpr::ILike {
                negated: false,
                any: false,
                expr: Box::new(l),
                pattern: Box::new(r),
                escape_char: None,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pl);
            buf.append(&mut pr);
            self.attach_where_with_and(pred, buf);
        }
        self
    }

    pub fn or_where_ilike<L, R>(mut self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        if let (Ok((l, mut pl)), Ok((r, mut pr))) = (
            self.resolve_qbarg_into_expr(left.into_qb_arg()),
            self.resolve_qbarg_into_expr(pattern.into_qb_arg()),
        ) {
            let pred = SqlExpr::ILike {
                negated: false,
                any: false,
                expr: Box::new(l),
                pattern: Box::new(r),
                escape_char: None,
            };
            let mut buf = SmallVec::new();
            buf.append(&mut pl);
            buf.append(&mut pr);
            self.attach_where_with_or(pred, buf);
        }
        self
    }
}
