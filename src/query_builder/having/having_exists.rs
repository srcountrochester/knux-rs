use sqlparser::ast::Expr as SqlExpr;

use crate::query_builder::QueryBuilder;
use crate::query_builder::args::{IntoQBArg, QBArg};

impl QueryBuilder {
    pub fn having_exists<T>(mut self, sub: T) -> Self
    where
        T: IntoQBArg,
    {
        match sub.into_qb_arg() {
            QBArg::Subquery(qb) => {
                if let Ok((q, p)) = qb.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: false,
                    };
                    self.attach_having_with_and(pred, p.into());
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: false,
                    };
                    self.attach_having_with_and(pred, p.into());
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "having_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }

    pub fn or_having_exists<T>(mut self, sub: T) -> Self
    where
        T: IntoQBArg,
    {
        match sub.into_qb_arg() {
            QBArg::Subquery(qb) => {
                if let Ok((q, p)) = qb.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: false,
                    };
                    self.attach_having_with_or(pred, p.into());
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: false,
                    };
                    self.attach_having_with_or(pred, p.into());
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "or_having_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }

    pub fn having_not_exists<T>(mut self, sub: T) -> Self
    where
        T: IntoQBArg,
    {
        match sub.into_qb_arg() {
            QBArg::Subquery(qb) => {
                if let Ok((q, p)) = qb.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: true,
                    };
                    self.attach_having_with_and(pred, p.into());
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: true,
                    };
                    self.attach_having_with_and(pred, p.into());
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "having_not_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }

    pub fn or_having_not_exists<T>(mut self, sub: T) -> Self
    where
        T: IntoQBArg,
    {
        match sub.into_qb_arg() {
            QBArg::Subquery(qb) => {
                if let Ok((q, p)) = qb.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: true,
                    };
                    self.attach_having_with_or(pred, p.into());
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: true,
                    };
                    self.attach_having_with_or(pred, p.into());
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "or_having_not_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }
}
