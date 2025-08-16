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
        let items: Vec<QBArg> = args.into_vec();
        if items.is_empty() {
            return self;
        }

        // Собираем итоговый Expr через AND
        let mut combined: Option<SqlExpr> = None;

        for item in items {
            match self.resolve_qbarg_into_expr(item) {
                Ok((expr, mut params)) => {
                    // AND-цепочка
                    combined = Some(match combined.take() {
                        Some(acc) => SqlExpr::BinaryOp {
                            left: Box::new(acc),
                            op: BO::And,
                            right: Box::new(expr),
                        },
                        None => expr,
                    });
                    // переносим параметры
                    self.params.append(&mut params);
                }
                Err(e) => {
                    self.push_builder_error(format!("where(): {}", e));
                }
            }
        }

        if let Some(new_expr) = combined {
            self.where_clause = Some(match self.where_clause.take() {
                Some(prev) => SqlExpr::BinaryOp {
                    left: Box::new(prev),
                    op: BO::And,
                    right: Box::new(new_expr),
                },
                None => new_expr,
            });
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
        if let Some(group) = self.resolve_where_group(args) {
            self.where_clause = Some(match self.where_clause.take() {
                Some(prev) => SqlExpr::BinaryOp {
                    left: Box::new(prev),
                    op: BO::And,
                    right: Box::new(group),
                },
                None => group,
            });
        }
        self
    }

    /// OR <group>
    pub fn or_where<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        if let Some(group) = self.resolve_where_group(args) {
            self.where_clause = Some(match self.where_clause.take() {
                Some(prev) => SqlExpr::BinaryOp {
                    left: Box::new(prev),
                    op: BO::Or,
                    right: Box::new(group),
                },
                None => group,
            });
        }
        self
    }

    /// WHERE NOT (<group>) — внутри группы условия склеиваются AND.
    pub fn where_not<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        if let Some(group) = self.resolve_where_group(args) {
            let pred = SqlExpr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(group),
            };
            self.attach_where_with_and(pred);
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
        if let Some(group) = self.resolve_where_group(args) {
            let pred = SqlExpr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(group),
            };
            self.attach_where_with_or(pred);
        }
        self
    }
}
