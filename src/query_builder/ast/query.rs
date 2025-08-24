use std::mem;

use crate::{param::Param, utils::num_expr};
use smallvec::SmallVec;
use sqlparser::ast::{
    Distinct, Expr as SqlExpr, GroupByExpr, Ident, Join, LimitClause, Offset, OffsetRows, OrderBy,
    OrderByExpr, OrderByKind, Query, Select, SelectFlavor, SelectItem, SetExpr, TableAlias,
    TableFactor, TableWithJoins, helpers::attached_token::AttachedToken,
};

use super::super::{BuilderErrorList, Error, QueryBuilder, Result};
use super::FromItem;

impl<'a, T> QueryBuilder<'a, T> {
    // TODO: заменить на использование form_query_ast
    pub(crate) fn build_query_ast(mut self) -> Result<(Query, Vec<Param>)> {
        if let Some(list) = self.take_builder_error_list() {
            return Err(Error::BuilderErrors(list));
        }

        let limit_clause = self.build_limit_clause();
        let with = self.take_with_ast();

        // соберём params и FROM сразу
        let params_sv = mem::take(&mut self.params);
        let from_items = mem::take(&mut self.from_items);
        let (from, mut params) = self.form_from_items(params_sv, from_items)?;

        // --- projection + select_params в одном проходе ---
        let (projection, select_params): (Vec<SelectItem>, Vec<Param>) =
            if self.select_items.is_empty() {
                (vec![SelectItem::Wildcard(Default::default())], Vec::new())
            } else {
                let cap = self.select_items.len();
                let mut proj: Vec<SelectItem> = Vec::with_capacity(cap);
                let mut sel_params: Vec<Param> = Vec::new();
                for node in self.select_items.drain(..) {
                    proj.push(node.item);
                    if !node.params.is_empty() {
                        sel_params.extend(node.params.into_iter());
                    }
                }
                (proj, sel_params)
            };

        let selection = self.where_clause.as_ref().map(|n| n.expr.clone());
        let having = self.having_clause.as_ref().map(|n| n.expr.clone());

        // --- group_by + group_params в одном проходе ---
        let (group_by_exprs, group_params): (Vec<SqlExpr>, Vec<Param>) = {
            let mut exprs = Vec::with_capacity(self.group_by_items.len());
            let mut gparams: Vec<Param> = Vec::new();
            for node in self.group_by_items.drain(..) {
                exprs.push(node.expr);
                if !node.params.is_empty() {
                    gparams.extend(node.params.into_iter());
                }
            }
            (exprs, gparams)
        };

        // --- DISTINCT/ON без лишних аллокаций ---
        let distinct_opt = if !self.distinct_on_items.is_empty() {
            let mut on_exprs: Vec<SqlExpr> = Vec::with_capacity(self.distinct_on_items.len());
            for n in self.distinct_on_items.drain(..) {
                on_exprs.push(n.expr);
                if !n.params.is_empty() {
                    params.extend(n.params.into_iter());
                }
            }
            Some(Distinct::On(on_exprs))
        } else if self.select_distinct {
            Some(Distinct::Distinct)
        } else {
            None
        };

        // --- ORDER BY + его параметры за один проход ---
        let (order_by_opt, order_params): (Option<OrderBy>, Vec<Param>) =
            if self.order_by_items.is_empty() {
                (None, Vec::new())
            } else {
                let mut exprs: Vec<OrderByExpr> = Vec::with_capacity(self.order_by_items.len());
                let mut ob_params: Vec<Param> = Vec::new();
                for node in self.order_by_items.drain(..) {
                    exprs.push(node.expr);
                    if !node.params.is_empty() {
                        ob_params.extend(node.params.into_iter());
                    }
                }
                (
                    Some(OrderBy {
                        kind: OrderByKind::Expressions(exprs),
                        interpolate: None,
                    }),
                    ob_params,
                )
            };

        let select = Select {
            distinct: distinct_opt,
            top: None,
            projection,
            into: None,
            from,
            lateral_views: vec![],
            selection,
            group_by: GroupByExpr::Expressions(group_by_exprs, vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having,
            named_window: vec![],
            qualify: None,
            connect_by: None,
            exclude: None,
            prewhere: None,
            value_table_mode: None,
            top_before_distinct: false,
            window_before_qualify: false,
            flavor: SelectFlavor::Standard,
            select_token: AttachedToken::empty(),
        };

        let mut body = SetExpr::Select(Box::new(select));

        // set-ops: без промежуточного Vec
        if !self.set_ops.is_empty() {
            for node in self.set_ops.drain(..) {
                if !node.params.is_empty() {
                    params.extend(node.params.into_iter());
                }
                body = SetExpr::SetOperation {
                    op: node.op,
                    set_quantifier: node.quantifier,
                    left: Box::new(body),
                    right: Box::new(SetExpr::Query(Box::new(node.right))),
                };
            }
        }

        let query = Query {
            with,
            body: Box::new(body),
            order_by: order_by_opt,
            fetch: None,
            locks: vec![],
            for_clause: None,
            format_clause: None,
            limit_clause,
            pipe_operators: vec![],
            settings: None,
        };

        if !self.builder_errors.is_empty() {
            return Err(Error::BuilderErrors(BuilderErrorList::from(
                std::mem::take(&mut self.builder_errors),
            )));
        }

        // WHERE/HAVING params в конец
        if let Some(node) = self.where_clause.take() {
            if !node.params.is_empty() {
                params.extend(node.params.into_iter());
            }
        }
        if let Some(node) = self.having_clause.take() {
            if !node.params.is_empty() {
                params.extend(node.params.into_iter());
            }
        }

        // ORDER BY уже собран (order_params), SELECT-параметры тоже
        params.extend(order_params);
        params.extend(select_params);
        params.extend(group_params);

        Ok((query, params))
    }

    // #[inline]
    // pub(crate) fn build_query_ast(self) -> Result<(Query, Vec<Param>)> {
    //     let mut me = self;
    //     me.form_query_ast()
    // }

    pub(crate) fn form_query_ast(&mut self) -> Result<(Query, Vec<Param>)> {
        if let Some(list) = self.take_builder_error_list() {
            return Err(Error::BuilderErrors(list));
        }

        let limit_clause = self.build_limit_clause();
        let with = self.take_with_ast();

        // соберём params и FROM сразу
        let params_sv = mem::take(&mut self.params);
        let from_items = mem::take(&mut self.from_items);
        let (from, mut params) = self.form_from_items(params_sv, from_items)?;

        // --- projection + select_params в одном проходе ---
        let (projection, select_params): (Vec<SelectItem>, Vec<Param>) =
            if self.select_items.is_empty() {
                (vec![SelectItem::Wildcard(Default::default())], Vec::new())
            } else {
                let cap = self.select_items.len();
                let mut proj: Vec<SelectItem> = Vec::with_capacity(cap);
                let mut sel_params: Vec<Param> = Vec::new();
                for node in self.select_items.drain(..) {
                    proj.push(node.item);
                    if !node.params.is_empty() {
                        sel_params.extend(node.params.into_iter());
                    }
                }
                (proj, sel_params)
            };

        let selection = self.where_clause.as_ref().map(|n| n.expr.clone());
        let having = self.having_clause.as_ref().map(|n| n.expr.clone());

        // --- group_by + group_params в одном проходе ---
        let (group_by_exprs, group_params): (Vec<SqlExpr>, Vec<Param>) = {
            let mut exprs = Vec::with_capacity(self.group_by_items.len());
            let mut gparams: Vec<Param> = Vec::new();
            for node in self.group_by_items.drain(..) {
                exprs.push(node.expr);
                if !node.params.is_empty() {
                    gparams.extend(node.params.into_iter());
                }
            }
            (exprs, gparams)
        };

        // --- DISTINCT/ON ---
        let distinct_opt = if !self.distinct_on_items.is_empty() {
            let mut on_exprs: Vec<SqlExpr> = Vec::with_capacity(self.distinct_on_items.len());
            for n in self.distinct_on_items.drain(..) {
                on_exprs.push(n.expr);
                if !n.params.is_empty() {
                    params.extend(n.params.into_iter());
                }
            }
            Some(Distinct::On(on_exprs))
        } else if self.select_distinct {
            Some(Distinct::Distinct)
        } else {
            None
        };

        // --- ORDER BY ---
        let (order_by_opt, order_params): (Option<OrderBy>, Vec<Param>) =
            if self.order_by_items.is_empty() {
                (None, Vec::new())
            } else {
                let mut exprs: Vec<OrderByExpr> = Vec::with_capacity(self.order_by_items.len());
                let mut ob_params: Vec<Param> = Vec::new();
                for node in self.order_by_items.drain(..) {
                    exprs.push(node.expr);
                    if !node.params.is_empty() {
                        ob_params.extend(node.params.into_iter());
                    }
                }
                (
                    Some(OrderBy {
                        kind: OrderByKind::Expressions(exprs),
                        interpolate: None,
                    }),
                    ob_params,
                )
            };

        let select = Select {
            distinct: distinct_opt,
            top: None,
            projection,
            into: None,
            from,
            lateral_views: vec![],
            selection,
            group_by: GroupByExpr::Expressions(group_by_exprs, vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having,
            named_window: vec![],
            qualify: None,
            connect_by: None,
            exclude: None,
            prewhere: None,
            value_table_mode: None,
            top_before_distinct: false,
            window_before_qualify: false,
            flavor: SelectFlavor::Standard,
            select_token: AttachedToken::empty(),
        };

        let mut body = SetExpr::Select(Box::new(select));

        // SET-операции
        if !self.set_ops.is_empty() {
            for node in self.set_ops.drain(..) {
                if !node.params.is_empty() {
                    params.extend(node.params.into_iter());
                }
                body = SetExpr::SetOperation {
                    op: node.op,
                    set_quantifier: node.quantifier,
                    left: Box::new(body),
                    right: Box::new(SetExpr::Query(Box::new(node.right))),
                };
            }
        }

        let query = Query {
            with,
            body: Box::new(body),
            order_by: order_by_opt,
            fetch: None,
            locks: vec![],
            for_clause: None,
            format_clause: None,
            limit_clause,
            pipe_operators: vec![],
            settings: None,
        };

        if !self.builder_errors.is_empty() {
            return Err(Error::BuilderErrors(BuilderErrorList::from(mem::take(
                &mut self.builder_errors,
            ))));
        }

        // WHERE/HAVING params в конец
        if let Some(node) = self.where_clause.take() {
            if !node.params.is_empty() {
                params.extend(node.params.into_iter());
            }
        }
        if let Some(node) = self.having_clause.take() {
            if !node.params.is_empty() {
                params.extend(node.params.into_iter());
            }
        }

        params.extend(order_params);
        params.extend(select_params);
        params.extend(group_params);

        Ok((query, params))
    }

    /// Собрать LimitClause с учётом диалекта.
    /// - PG/SQLite: `LIMIT n [OFFSET m]`, допускается `OFFSET m` без LIMIT
    /// - MySQL:
    ///   - `LIMIT n`
    ///   - `LIMIT off, lim` при наличии обоих
    ///   - только `OFFSET m` эмулируется как `LIMIT m, 18446744073709551615`
    #[inline]
    fn build_limit_clause(&self) -> Option<LimitClause> {
        let lim = self.limit_num;
        let off = self.offset_num;

        match (lim, off) {
            (None, None) => None,

            // ----- MySQL-ветка -----
            _ if self.is_mysql() => match (lim, off) {
                (Some(l), Some(o)) => Some(LimitClause::OffsetCommaLimit {
                    offset: num_expr(o),
                    limit: num_expr(l),
                }),
                (Some(l), None) => Some(LimitClause::LimitOffset {
                    limit: Some(num_expr(l)),
                    offset: None,
                    limit_by: vec![],
                }),
                (None, Some(o)) => Some(LimitClause::LimitOffset {
                    limit: None,
                    offset: Some(Offset {
                        value: num_expr(o),
                        rows: OffsetRows::None,
                    }),
                    limit_by: vec![],
                }),
                (None, None) => None,
            },

            // ----- Стандартная ветка (PG/SQLite и др.) -----
            (Some(l), None) => Some(LimitClause::LimitOffset {
                limit: Some(num_expr(l)),
                offset: None,
                limit_by: vec![],
            }),
            (None, Some(o)) => Some(LimitClause::LimitOffset {
                limit: None,
                offset: Some(Offset {
                    value: num_expr(o),
                    rows: OffsetRows::None,
                }),
                limit_by: vec![],
            }),
            (Some(l), Some(o)) => Some(LimitClause::LimitOffset {
                limit: Some(num_expr(l)),
                offset: Some(Offset {
                    value: num_expr(o),
                    rows: OffsetRows::None,
                }),
                limit_by: vec![],
            }),
        }
    }

    fn form_from_items(
        &mut self,
        params: SmallVec<[Param; 8]>,
        from_items: SmallVec<[FromItem; 1]>,
    ) -> Result<(Vec<TableWithJoins>, Vec<Param>)> {
        let mut params = params.into_vec();
        let mut from: Vec<TableWithJoins> = Vec::with_capacity(from_items.len());

        for (i, item) in from_items.into_iter().enumerate() {
            let joins_vec: Vec<Join> = if i < self.from_joins.len() {
                let nodes_sv = std::mem::take(&mut self.from_joins[i]);
                let mut jv: Vec<Join> = Vec::with_capacity(nodes_sv.len());
                for node in nodes_sv {
                    jv.push(node.join);
                    if !node.params.is_empty() {
                        params.extend(node.params.into_vec());
                    }
                }
                jv
            } else {
                Vec::new()
            };

            match item {
                FromItem::TableName(name) => from.push(TableWithJoins {
                    joins: joins_vec,
                    relation: TableFactor::Table {
                        name,
                        alias: None,
                        args: None,
                        with_hints: vec![],
                        partitions: vec![],
                        version: None,
                        index_hints: vec![],
                        json_path: None,
                        sample: None,
                        with_ordinality: false,
                    },
                }),
                FromItem::Subquery(qb) => {
                    let alias = qb.alias.clone();
                    let (q, p) = qb.build_query_ast()?;
                    if !p.is_empty() {
                        params.extend(p);
                    }
                    from.push(TableWithJoins {
                        joins: joins_vec,
                        relation: TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        },
                    });
                }
                FromItem::SubqueryClosure(closure) => {
                    let built = closure.call(QueryBuilder::new_empty());
                    let alias = built.alias.clone();
                    let (q, p) = built.build_query_ast()?;
                    if !p.is_empty() {
                        params.extend(p);
                    }
                    from.push(TableWithJoins {
                        joins: joins_vec,
                        relation: TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        },
                    });
                }
            }
        }

        Ok((from, params))
    }
}
