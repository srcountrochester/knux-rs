use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{ArgList, QBArg};

impl QueryBuilder {
    #[inline]
    pub(crate) fn attach_having_with_and(&mut self, pred: SqlExpr) {
        self.having_clause = Some(match self.having_clause.take() {
            Some(prev) => SqlExpr::BinaryOp {
                left: Box::new(prev),
                op: BO::And,
                right: Box::new(pred),
            },
            None => pred,
        });
    }

    #[inline]
    pub(crate) fn attach_having_with_or(&mut self, pred: SqlExpr) {
        self.having_clause = Some(match self.having_clause.take() {
            Some(prev) => SqlExpr::BinaryOp {
                left: Box::new(prev),
                op: BO::Or,
                right: Box::new(pred),
            },
            None => pred,
        });
    }

    /// Собирает группу HAVING-условий из ArgList, соединяя через AND.
    /// Параметры выражений добавляются в self.params.
    pub(crate) fn resolve_having_group<A>(&mut self, args: A) -> Option<SqlExpr>
    where
        A: ArgList,
    {
        let items: Vec<QBArg> = args.into_vec();
        if items.is_empty() {
            return None;
        }

        let mut combined: Option<SqlExpr> = None;

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
                    // SmallVec -> SmallVec
                    self.params.append(&mut params);
                }
                Err(e) => self.push_builder_error(format!("having(): {}", e)),
            }
        }

        combined
    }
}
