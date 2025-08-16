use sqlparser::ast::Expr as SqlExpr;

use crate::query_builder::args::IntoQBArg;
use crate::query_builder::{QueryBuilder, args::QBArg};

impl QueryBuilder {
    pub fn where_exists<T>(mut self, sub: T) -> Self
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
                    self.extend_params(p);
                    self.attach_where_with_and(pred);
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: false,
                    };
                    self.extend_params(p);
                    self.attach_where_with_and(pred);
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "where_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }

    pub fn or_where_exists<T>(mut self, sub: T) -> Self
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
                    self.extend_params(p);
                    self.attach_where_with_or(pred);
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: false,
                    };
                    self.extend_params(p);
                    self.attach_where_with_or(pred);
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "or_where_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }

    pub fn where_not_exists<T>(mut self, sub: T) -> Self
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
                    self.extend_params(p);
                    self.attach_where_with_and(pred);
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: true,
                    };
                    self.extend_params(p);
                    self.attach_where_with_and(pred);
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "where_not_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }

    pub fn or_where_not_exists<T>(mut self, sub: T) -> Self
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
                    self.extend_params(p);
                    self.attach_where_with_or(pred);
                }
            }
            QBArg::Closure(c) => {
                let built = c.apply(QueryBuilder::new_empty());
                if let Ok((q, p)) = built.build_query_ast() {
                    let pred = SqlExpr::Exists {
                        subquery: Box::new(q),
                        negated: true,
                    };
                    self.extend_params(p);
                    self.attach_where_with_or(pred);
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "or_where_not_exists(): требуется подзапрос (QueryBuilder или замыкание)",
                );
            }
        }
        self
    }
}
