use smallvec::SmallVec;
use sqlparser::ast::{Expr as SqlExpr, OrderByExpr, OrderByOptions};

use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, QBArg};

#[derive(Debug, Clone)]
pub(crate) struct OrderByNode {
    pub expr: OrderByExpr,
    pub params: SmallVec<[Param; 8]>,
}

impl OrderByNode {
    #[inline]
    pub fn new(expr: OrderByExpr, params: SmallVec<[Param; 8]>) -> Self {
        Self { expr, params }
    }
}

impl QueryBuilder {
    /// ORDER BY <expr1>, <expr2>, ...
    ///
    /// Поддерживает:
    /// - `&str` / `String` → трактуется как колонка (`col("...")`) через ваш IntoQBArg
    /// - `Expression` → как есть
    /// - ⛔ `QueryBuilder`/замыкания — не поддерживаются в ORDER BY (фиксируем ошибку)
    pub fn order_by<A>(mut self, args: A) -> Self
    where
        A: ArgList,
    {
        let items = args.into_vec();
        if items.is_empty() {
            return self;
        }

        for it in items {
            match it {
                QBArg::Expr(e) => {
                    let expr: SqlExpr = e.expr;
                    let mut params: SmallVec<[Param; 8]> = e.params;

                    let ob = OrderByExpr {
                        expr,
                        options: OrderByOptions {
                            asc: None,
                            nulls_first: None,
                        },
                        with_fill: None,
                    };

                    // кладём (expr + его параметры) как единую ноду ORDER BY
                    self.order_by_items.push(OrderByNode::new(ob, {
                        let mut buf: SmallVec<[Param; 8]> = SmallVec::new();
                        buf.append(&mut params);
                        buf
                    }));
                }
                QBArg::Subquery(_) | QBArg::Closure(_) => {
                    self.push_builder_error(
                        "order_by(): подзапросы/замыкания в ORDER BY не поддерживаются",
                    );
                }
            }
        }

        self
    }
}
