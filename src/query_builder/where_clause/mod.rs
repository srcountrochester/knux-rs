mod __tests__;
mod core_fn;
pub mod utils;
mod where_between;
mod where_exists;
mod where_in;
mod where_json;
mod where_like;
mod where_null;
mod where_raw;

use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, UnaryOperator};

use super::Result;
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};

pub(crate) use core_fn::WhereNode;

impl QueryBuilder {
    /// WHERE <expr> [AND <expr> ...]
    ///
    /// Поддерживает любой `ArgList`:
    /// - одиночный аргумент: `.where(col("a").eq(val(1)))`
    /// - кортеж/массив/вектор: `.where((col("a").eq(val(1)), col("b").eq(val(2))))`
    /// - подзапросы/замыкания: `.where(|qb| qb.from("t").select("..."))` → `Expr::Subquery`
    pub fn r#where<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        if let Some((group, params)) = self.resolve_where_group(args) {
            // слепит с уже существующим WHERE через AND и добавит параметры группы
            self.attach_where_with_and(group, params);
        }
        self
    }

    #[inline]
    /// WHERE <expr> [AND <expr> ...]
    ///
    /// Поддерживает любой `ArgList`:
    /// - одиночный аргумент: `.where(col("a").eq(val(1)))`
    /// - кортеж/массив/вектор: `.where((col("a").eq(val(1)), col("b").eq(val(2))))`
    /// - подзапросы/замыкания: `.where(|qb| qb.from("t").select("..."))` → `Expr::Subquery`
    pub fn where_<A>(self, args: A) -> Self
    where
        A: ArgList,
    {
        self.r#where(args)
    }

    /// AND <group>
    pub fn and_where<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        if let Some((group, params)) = self.resolve_where_group(args) {
            self.attach_where_with_and(group, params);
        }
        self
    }

    /// OR <group>
    pub fn or_where<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        if let Some((group, params)) = self.resolve_where_group(args) {
            self.attach_where_with_or(group, params);
        }
        self
    }

    /// WHERE NOT (<group>) — внутри группы условия склеиваются AND.
    pub fn where_not<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        if let Some((group, params)) = self.resolve_where_group(args) {
            let pred = SqlExpr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(group),
            };
            self.attach_where_with_and(pred, params);
        }
        self
    }

    pub fn and_where_not<A>(self, args: A) -> Self
    where
        A: ArgList,
    {
        self.where_not(args)
    }

    pub fn or_where_not<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        if let Some((group, params)) = self.resolve_where_group(args) {
            let pred = SqlExpr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(group),
            };
            self.attach_where_with_or(pred, params);
        }
        self
    }
}
