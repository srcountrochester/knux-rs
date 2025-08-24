use smallvec::SmallVec;
use sqlparser::ast::Expr as SqlExpr;

use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, QBArg};

#[derive(Debug, Clone)]
pub(crate) struct GroupByNode {
    pub expr: SqlExpr,
    pub params: SmallVec<[Param; 8]>,
}

impl GroupByNode {
    #[inline]
    pub fn new(expr: SqlExpr, params: SmallVec<[Param; 8]>) -> Self {
        Self { expr, params }
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    /// Добавляет выражения в GROUP BY.
    ///
    /// Поддерживаются:
    /// - строковые литералы (интерпретируются как `col("...")`)
    /// - выражения из модуля `expression`
    /// - ❌ подзапросы и замыкания для GROUP BY не поддерживаются (регистрируется ошибка билдера)
    pub fn group_by<A>(mut self, args: A) -> Self
    where
        A: ArgList<'a>,
    {
        let items: Vec<QBArg> = args.into_vec();
        if items.is_empty() {
            return self;
        }

        for it in items {
            match it {
                QBArg::Expr(e) => {
                    let expr: SqlExpr = e.expr;
                    let mut params: SmallVec<[Param; 8]> = e.params;

                    self.group_by_items.push(GroupByNode::new(expr, {
                        let mut buf: SmallVec<[Param; 8]> = SmallVec::new();
                        buf.append(&mut params);
                        buf
                    }));
                }
                QBArg::Subquery(_) | QBArg::Closure(_) => {
                    self.push_builder_error(
                        "group_by(): подзапросы/замыкания в GROUP BY не поддерживаются",
                    );
                }
            }
        }

        self
    }
}
