use smallvec::SmallVec;
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

use super::Result;
use crate::param::Param;
use crate::query_builder::args::IntoQBArg;
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};

impl QueryBuilder {
    #[inline]
    pub(crate) fn attach_where_with_and(&mut self, pred: SqlExpr) {
        self.where_clause = Some(match self.where_clause.take() {
            Some(prev) => SqlExpr::BinaryOp {
                left: Box::new(prev),
                op: BO::And,
                right: Box::new(pred),
            },
            None => pred,
        });
    }

    #[inline]
    pub(crate) fn attach_where_with_or(&mut self, pred: SqlExpr) {
        self.where_clause = Some(match self.where_clause.take() {
            Some(prev) => SqlExpr::BinaryOp {
                left: Box::new(prev),
                op: BO::Or,
                right: Box::new(pred),
            },
            None => pred,
        });
    }

    /// Строим IN / NOT IN.
    pub(crate) fn build_in_predicate<C, A>(
        &mut self,
        column: C,
        values: A,
        negated: bool,
    ) -> Option<SqlExpr>
    where
        C: IntoQBArg,
        A: ArgList,
    {
        // левая часть
        let (left, mut lp) = match self.resolve_qbarg_into_expr(column.into_qb_arg()) {
            Ok(v) => v,
            Err(e) => {
                self.push_builder_error(format!("where_in(): {e}"));
                return None;
            }
        };
        // SmallVec -> SmallVec: append ок
        self.params.append(&mut lp);

        // значения
        let mut vals: Vec<QBArg> = values.into_vec();
        if vals.is_empty() {
            self.push_builder_error("where_in(): пустой список значений");
            return None;
        }

        // особый случай: ровно один аргумент и он — подзапрос
        if vals.len() == 1 {
            match vals.pop().unwrap() {
                QBArg::Subquery(qb) => {
                    match qb.build_query_ast() {
                        Ok((q, p)) => {
                            // Vec<Param> -> SmallVec: через extend
                            self.extend_params(p);
                            return Some(SqlExpr::InSubquery {
                                expr: Box::new(left),
                                subquery: Box::new(q),
                                negated,
                            });
                        }
                        Err(e) => {
                            self.push_builder_error(format!("where_in(): {e}"));
                            return None;
                        }
                    }
                }
                QBArg::Closure(c) => {
                    let built = c.apply(QueryBuilder::new_empty());
                    match built.build_query_ast() {
                        Ok((q, p)) => {
                            self.extend_params(p);
                            return Some(SqlExpr::InSubquery {
                                expr: Box::new(left),
                                subquery: Box::new(q),
                                negated,
                            });
                        }
                        Err(e) => {
                            self.push_builder_error(format!("where_in(): {e}"));
                            return None;
                        }
                    }
                }
                other => {
                    // это не подзапрос — идём в обычный путь IN (expr_list)
                    vals = vec![other];
                }
            }
        }

        // общий путь: IN (list of expr)
        let mut list_exprs: Vec<SqlExpr> = Vec::with_capacity(vals.len());
        for it in vals.into_iter() {
            match self.resolve_qbarg_into_expr(it) {
                Ok((e, mut ps)) => {
                    list_exprs.push(e);
                    // SmallVec -> SmallVec
                    self.params.append(&mut ps);
                }
                Err(err) => self.push_builder_error(format!("where_in(): {err}")),
            }
        }

        if list_exprs.is_empty() {
            None
        } else {
            Some(SqlExpr::InList {
                expr: Box::new(left),
                list: list_exprs,
                negated,
            })
        }
    }

    /// Заглушка для JSON-методов — аккуратно регистрируем ошибку билдера.
    pub(crate) fn with_json_todo(mut self, name: &str) -> Self {
        self.push_builder_error(format!(
            "{name}(): ещё не реализовано для текущего диалекта"
        ));
        self
    }

    /// Унифицированный резолв QBArg → (Expr, params) для WHERE-контекста.
    pub(crate) fn resolve_qbarg_into_expr(
        &self,
        arg: QBArg,
    ) -> Result<(SqlExpr, SmallVec<[Param; 8]>)> {
        match arg {
            QBArg::Expr(e) => Ok((e.expr, e.params)), // Expression → как есть
            QBArg::Subquery(qb) => {
                let (q, params) = qb.build_query_ast()?;
                Ok((SqlExpr::Subquery(Box::new(q)), params.into()))
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                let (q, params) = built.build_query_ast()?;
                Ok((SqlExpr::Subquery(Box::new(q)), params.into()))
            }
        }
    }

    /// Собирает группу условий из ArgList: внутри группы — AND.
    /// Возвращает None, если все элементы невалидны.
    pub(crate) fn resolve_where_group<A>(&mut self, args: A) -> Option<SqlExpr>
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
                    self.params.append(&mut params);
                }
                Err(e) => self.push_builder_error(format!("where(): {}", e)),
            }
        }

        combined
    }
}
