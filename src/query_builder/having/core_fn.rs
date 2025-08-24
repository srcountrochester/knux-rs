use smallvec::SmallVec;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, QBArg};

#[derive(Debug, Clone)]
pub(crate) struct HavingNode {
    pub expr: SqlExpr,
    pub params: SmallVec<[Param; 8]>,
}

impl HavingNode {
    #[inline]
    pub fn new(expr: SqlExpr, params: SmallVec<[Param; 8]>) -> Self {
        Self { expr, params }
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    #[inline]
    pub(crate) fn attach_having_with_and(
        &mut self,
        pred: SqlExpr,
        mut params: SmallVec<[Param; 8]>,
    ) {
        self.having_clause = Some(match self.having_clause.take() {
            Some(mut node) => {
                node.expr = SqlExpr::BinaryOp {
                    left: Box::new(node.expr),
                    op: BO::And,
                    right: Box::new(pred),
                };
                node.params.append(&mut params);
                node
            }
            None => HavingNode::new(pred, params),
        });
    }

    #[inline]
    pub(crate) fn attach_having_with_or(
        &mut self,
        pred: SqlExpr,
        mut params: SmallVec<[Param; 8]>,
    ) {
        self.having_clause = Some(match self.having_clause.take() {
            Some(mut node) => {
                node.expr = SqlExpr::BinaryOp {
                    left: Box::new(node.expr),
                    op: BO::Or,
                    right: Box::new(pred),
                };
                node.params.append(&mut params);
                node
            }
            None => HavingNode::new(pred, params),
        });
    }

    /// Собирает группу HAVING-условий из ArgList, соединяя через AND.
    /// Параметры выражений добавляются в self.params.
    pub(crate) fn resolve_having_group<A>(
        &mut self,
        args: A,
    ) -> Option<(SqlExpr, SmallVec<[Param; 8]>)>
    where
        A: ArgList<'a>,
    {
        let items: Vec<QBArg> = args.into_vec();
        if items.is_empty() {
            return None;
        }

        let mut combined: Option<SqlExpr> = None;
        let mut out_params: SmallVec<[Param; 8]> = SmallVec::new();

        for item in items {
            match self.resolve_qbarg_into_expr(item) {
                Ok((expr, mut params)) => {
                    combined = Some(match combined.take() {
                        Some(acc) => SqlExpr::BinaryOp {
                            left: Box::new(acc),
                            op: BO::And,
                            right: Box::new(expr),
                        },
                        None => expr,
                    });
                    out_params.append(&mut params);
                }
                Err(e) => self.push_builder_error(format!("having(): {}", e)),
            }
        }

        combined.map(|e| (e, out_params))
    }
}
