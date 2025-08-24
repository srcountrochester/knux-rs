use smallvec::SmallVec;
use sqlparser::ast::{
    Cte, CteAsMaterialized, Ident, Query, TableAlias, With, helpers::attached_token::AttachedToken,
};

use crate::param::Param;
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};

#[derive(Debug, Clone)]
pub(crate) struct WithItemNode {
    pub cte: Cte,
    pub params: SmallVec<[Param; 8]>,
}

impl WithItemNode {
    #[inline]
    fn new(cte: Cte, params: Vec<Param>) -> Self {
        Self {
            cte,
            params: params.into(),
        }
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    /// WITH <name> AS (<subquery>)
    /// Второй аргумент — стандартный для библиотеки `ArgList`.
    /// Допустимы: `QueryBuilder` или `|qb| { ... }`. `Expr` здесь не принимается.
    pub fn with<L>(mut self, name: &str, body: L) -> Self
    where
        L: ArgList<'a>,
    {
        let mut args = body.into_vec();

        if args.is_empty() {
            self.push_builder_error("with(): expected a subquery (got 0 arguments)");
            return self;
        }
        if args.len() > 1 {
            self.push_builder_error(format!(
                "with(): expected 1 argument (subquery), got {} — extra args are ignored",
                args.len()
            ));
        }

        match args.remove(0) {
            QBArg::Subquery(qb) => match qb.build_query_ast() {
                Ok((q, params)) => self.push_cte_with(&name, q, params, None, None),
                Err(e) => self.push_builder_error(format!("with(): {e}")),
            },
            QBArg::Closure(c) => {
                let built = c.call(QueryBuilder::new_empty());
                match built.build_query_ast() {
                    Ok((q, params)) => self.push_cte_with(&name, q, params, None, None),
                    Err(e) => self.push_builder_error(format!("with(): {e}")),
                }
            }
            QBArg::Expr(_) => {
                self.push_builder_error(
                    "with(): expression is not allowed here; pass a subquery or closure",
                );
            }
        }

        self
    }

    pub fn with_recursive<L>(mut self, name: &str, body: L) -> Self
    where
        L: ArgList<'a>,
    {
        self.with_recursive = true;
        self.with(name, body)
    }

    pub fn with_materialized<L>(mut self, name: &str, body: L) -> Self
    where
        L: ArgList<'a>,
    {
        let mut args = body.into_vec();
        if args.is_empty() {
            self.push_builder_error("with_materialized(): expected a subquery");
            return self;
        }
        // берём первый аргумент как subquery (аналогично with)
        match args.remove(0) {
            QBArg::Subquery(qb) => match qb.build_query_ast() {
                Ok((q, params)) => {
                    self.push_cte_with(
                        name,
                        q,
                        params,
                        None,
                        Some(CteAsMaterialized::Materialized),
                    );
                }
                Err(e) => self.push_builder_error(format!("with_materialized(): {e}")),
            },
            QBArg::Closure(c) => {
                let built = c.call(QueryBuilder::new_empty());
                match built.build_query_ast() {
                    Ok((q, params)) => {
                        self.push_cte_with(
                            name,
                            q,
                            params,
                            None,
                            Some(CteAsMaterialized::Materialized),
                        );
                    }
                    Err(e) => self.push_builder_error(format!("with_materialized(): {e}")),
                }
            }
            _ => self.push_builder_error(
                "with_materialized(): expression is not allowed; pass a subquery or closure",
            ),
        }
        self
    }

    pub fn with_not_materialized<L>(mut self, name: &str, body: L) -> Self
    where
        L: ArgList<'a>,
    {
        let mut args = body.into_vec();
        if args.is_empty() {
            self.push_builder_error("with_not_materialized(): expected a subquery");
            return self;
        }
        match args.remove(0) {
            QBArg::Subquery(qb) => match qb.build_query_ast() {
                Ok((q, params)) => {
                    self.push_cte_with(
                        name,
                        q,
                        params,
                        None,
                        Some(CteAsMaterialized::NotMaterialized),
                    );
                }
                Err(e) => self.push_builder_error(format!("with_not_materialized(): {e}")),
            },
            QBArg::Closure(c) => {
                let built = c.call(QueryBuilder::new_empty());
                match built.build_query_ast() {
                    Ok((q, params)) => {
                        self.push_cte_with(
                            name,
                            q,
                            params,
                            None,
                            Some(CteAsMaterialized::NotMaterialized),
                        );
                    }
                    Err(e) => self.push_builder_error(format!("with_not_materialized(): {e}")),
                }
            }
            _ => self.push_builder_error(
                "with_not_materialized(): expression is not allowed; pass a subquery or closure",
            ),
        }
        self
    }

    pub fn with_from<L>(mut self, name: &str, from: &str, body: L) -> Self
    where
        L: ArgList<'a>,
    {
        let mut args = body.into_vec();
        if args.is_empty() {
            self.push_builder_error("with_from(): expected a subquery");
            return self;
        }

        let built = match args.remove(0) {
            QBArg::Subquery(qb) => qb.build_query_ast(),
            QBArg::Closure(c) => c.call(QueryBuilder::new_empty()).build_query_ast(),
            _ => {
                self.push_builder_error(
                    "with_from(): expression is not allowed; pass a subquery or closure",
                );
                return self;
            }
        };

        match built {
            Ok((q, params)) => {
                self.push_cte_with(name, q, params, Some(Ident::new(from)), None);
            }
            Err(e) => self.push_builder_error(format!("with_from(): {e}")),
        }

        self
    }

    #[inline]
    fn push_cte_with(
        &mut self,
        name: &str,
        query: Query,
        params: Vec<Param>,
        from: Option<Ident>,
        materialized: Option<CteAsMaterialized>,
    ) {
        let cte = Cte {
            alias: TableAlias {
                name: Ident::new(name),
                columns: vec![],
            },
            query: Box::new(query),
            from,
            materialized,
            closing_paren_token: AttachedToken::empty(),
        };
        self.with_items.push(WithItemNode::new(cte, params));
    }

    /// Собирает `With` и переносит параметры из CTE в общий список.
    #[inline]
    pub(crate) fn take_with_ast(&mut self) -> Option<With> {
        if self.with_items.is_empty() {
            return None;
        }
        let nodes = std::mem::take(&mut self.with_items);
        let mut ctes = Vec::with_capacity(nodes.len());
        for node in nodes {
            ctes.push(node.cte);
            if !node.params.is_empty() {
                self.params.extend(node.params.into_vec());
            }
        }
        Some(With {
            with_token: AttachedToken::empty(),
            recursive: self.with_recursive,
            cte_tables: ctes,
        })
    }
}
