use std::mem;

use crate::{
    param::Param,
    query_builder::{
        InsertBuilder,
        args::QBClosure,
        delete::DeleteBuilder,
        insert::{Assignment, ConflictAction, MergeValue},
        update::UpdateBuilder,
    },
    renderer::Dialect,
    utils::num_expr,
};
use smallvec::SmallVec;
use sqlparser::ast::{
    self as S, Distinct, Expr as SqlExpr, GroupByExpr, Ident, Join, LimitClause, ObjectName,
    Offset, OffsetRows, OrderBy, OrderByExpr, OrderByKind, Query, Select, SelectFlavor, SelectItem,
    SetExpr, TableAlias, TableFactor, TableWithJoins, helpers::attached_token::AttachedToken,
};

use super::{BuilderErrorList, Error, QueryBuilder, Result};

#[derive(Debug)]
pub enum FromItem {
    TableName(ObjectName),
    Subquery(Box<QueryBuilder>),
    SubqueryClosure(QBClosure),
}

impl QueryBuilder {
    pub(crate) fn build_query_ast(mut self) -> Result<(Query, Vec<Param>)> {
        if let Some(list) = self.take_builder_error_list() {
            return Err(Error::BuilderErrors(list));
        }

        let limit_clause = self.build_limit_clause();
        let with = self.take_with_ast();
        let params_sv = mem::take(&mut self.params);
        let from_items = mem::take(&mut self.from_items);
        let (from, mut params) = self.form_from_items(params_sv, from_items)?;

        // проекция по умолчанию: SELECT *
        let projection: SmallVec<[SelectItem; 4]> = if self.select_items.is_empty() {
            let mut sv = SmallVec::new();
            sv.push(SelectItem::Wildcard(Default::default()));
            sv
        } else {
            self.select_items.iter().map(|n| n.item.clone()).collect()
        };

        let selection = self.where_clause.as_ref().map(|n| n.expr.clone());
        let having = self.having_clause.as_ref().map(|n| n.expr.clone());

        let (group_by_exprs, group_params): (Vec<SqlExpr>, Vec<Param>) = {
            let mut exprs = Vec::with_capacity(self.group_by_items.len());
            let mut gparams: Vec<Param> = Vec::new();
            for node in self.group_by_items.drain(..) {
                exprs.push(node.expr);
                gparams.extend(node.params.into_vec());
            }
            (exprs, gparams)
        };

        let distinct_opt = if !self.distinct_on_items.is_empty() {
            let mut on_exprs: Vec<SqlExpr> = Vec::with_capacity(self.distinct_on_items.len());
            for mut n in self.distinct_on_items.into_vec() {
                on_exprs.push(n.expr);
                if !n.params.is_empty() {
                    params.extend(n.params.drain(..).collect::<Vec<_>>());
                }
            }
            Some(Distinct::On(on_exprs))
        } else if self.select_distinct {
            Some(Distinct::Distinct)
        } else {
            None
        };

        let select = Select {
            distinct: distinct_opt,
            top: None,
            projection: projection.into_vec(),
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

        let order_by_opt = if self.order_by_items.is_empty() {
            None
        } else {
            let exprs: Vec<OrderByExpr> =
                self.order_by_items.iter().map(|n| n.expr.clone()).collect();
            Some(OrderBy {
                kind: OrderByKind::Expressions(exprs),
                interpolate: None,
            })
        };

        let mut body = SetExpr::Select(Box::new(select));

        // Последовательно навешиваем UNION / UNION ALL справа налево, аккумулируя параметры
        if !self.set_ops.is_empty() {
            for node in self.set_ops.into_vec() {
                // порядок параметров: сначала то, что уже накоплено, затем RHS текущего set-op
                if !node.params.is_empty() {
                    params.extend(node.params.into_vec());
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

        if let Some(node) = self.where_clause.take() {
            params.extend(node.params.into_vec());
        }

        if let Some(node) = self.having_clause.take() {
            params.extend(node.params.into_vec());
        }

        for node in self.order_by_items.drain(..) {
            params.extend(node.params.into_vec());
        }

        for node in self.select_items.drain(..) {
            if !node.params.is_empty() {
                params.extend(node.params.into_vec());
            }
        }

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
                (None, Some(o)) => {
                    // MySQL не поддерживает "OFFSET m" без LIMIT.
                    // Эмулируем "безлимитный" оффсет через огромный лимит.
                    Some(LimitClause::OffsetCommaLimit {
                        offset: num_expr(o),
                        limit: num_expr(u64::MAX),
                    })
                }
                (None, None) => None, // уже покрыто, для полноты match'а
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
                    let (q, mut p) = qb.build_query_ast()?;
                    if !p.is_empty() {
                        params.append(&mut p);
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
                    let built = closure.apply(QueryBuilder::new_empty());
                    let alias = built.alias.clone();
                    let (q, mut p) = built.build_query_ast()?;
                    if !p.is_empty() {
                        params.append(&mut p);
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

impl InsertBuilder {
    pub(crate) fn build_insert_ast(self) -> Result<(S::Statement, Vec<Param>)> {
        // 1) минимальная валидация
        let table = match &self.table {
            Some(t) => t.clone(),
            None => {
                return Err(Error::InvalidExpression {
                    reason: "insert: table is not set".into(),
                });
            }
        };
        if self.rows.is_empty() {
            return Err(Error::InvalidExpression {
                reason: "insert: no VALUES rows".into(),
            });
        }
        if !self.builder_errors.is_empty() {
            return Err(Error::InvalidExpression {
                reason: format!("insert: build errors: {:?}", self.builder_errors).into(),
            });
        }

        // 2) VALUES → Query(SetExpr::Values)
        let rows_exprs: Vec<Vec<S::Expr>> = self
            .rows
            .iter()
            .map(|r| r.values.iter().cloned().collect())
            .collect();

        let values = S::Values {
            rows: rows_exprs,
            explicit_row: false,
        };

        let query = S::Query {
            with: None,
            body: Box::new(S::SetExpr::Values(values)),
            fetch: None,
            for_clause: None,
            format_clause: None,
            limit_clause: None,
            locks: vec![],
            order_by: None,
            pipe_operators: vec![],
            settings: None,
        };

        // 3) RETURNING
        let returning = if self.returning.is_empty() {
            None
        } else {
            Some(self.returning.into_vec())
        };

        // 4) ON / IGNORE (по диалектам)
        let on: Option<S::OnInsert> = match self.dialect {
            Dialect::Postgres => {
                if let Some(spec) = &self.on_conflict {
                    use S::{ConflictTarget, DoUpdate, OnConflict, OnConflictAction};
                    let target = if !spec.target_columns.is_empty() {
                        Some(ConflictTarget::Columns(
                            spec.target_columns.iter().cloned().collect(),
                        ))
                    } else {
                        None
                    };
                    match &spec.action {
                        None => {
                            // если пользователь вызвал .ignore() без явного действия — DO NOTHING
                            if self.insert_ignore {
                                Some(S::OnInsert::OnConflict(OnConflict {
                                    conflict_target: target,
                                    action: OnConflictAction::DoNothing,
                                }))
                            } else {
                                Some(S::OnInsert::OnConflict(OnConflict {
                                    conflict_target: target,
                                    action: OnConflictAction::DoNothing,
                                }))
                            }
                        }
                        Some(ConflictAction::DoNothing) => {
                            Some(S::OnInsert::OnConflict(OnConflict {
                                conflict_target: target,
                                action: OnConflictAction::DoNothing,
                            }))
                        }
                        Some(ConflictAction::DoUpdate {
                            set,
                            where_predicate,
                        }) => {
                            let assignments = make_update_assignments_pg_sqlite(set);
                            Some(S::OnInsert::OnConflict(OnConflict {
                                conflict_target: target,
                                action: OnConflictAction::DoUpdate(DoUpdate {
                                    assignments,
                                    selection: where_predicate.clone(),
                                }),
                            }))
                        }
                    }
                } else if self.insert_ignore {
                    // Без цели конфликта тоже допустимо: ON CONFLICT DO NOTHING
                    use S::{OnConflict, OnConflictAction};
                    Some(S::OnInsert::OnConflict(OnConflict {
                        conflict_target: None,
                        action: OnConflictAction::DoNothing,
                    }))
                } else {
                    None
                }
            }

            Dialect::SQLite => {
                if let Some(spec) = &self.on_conflict {
                    use S::{ConflictTarget, DoUpdate, OnConflict, OnConflictAction};
                    let target = if !spec.target_columns.is_empty() {
                        Some(ConflictTarget::Columns(
                            spec.target_columns.iter().cloned().collect(),
                        ))
                    } else {
                        None
                    };
                    match &spec.action {
                        None => {
                            // если пользователь вызвал .ignore() без явного действия — DO NOTHING
                            if self.insert_ignore {
                                Some(S::OnInsert::OnConflict(OnConflict {
                                    conflict_target: target,
                                    action: OnConflictAction::DoNothing,
                                }))
                            } else {
                                Some(S::OnInsert::OnConflict(OnConflict {
                                    conflict_target: target,
                                    action: OnConflictAction::DoNothing,
                                }))
                            }
                        }
                        Some(ConflictAction::DoNothing) => {
                            Some(S::OnInsert::OnConflict(OnConflict {
                                conflict_target: target,
                                action: OnConflictAction::DoNothing,
                            }))
                        }
                        Some(ConflictAction::DoUpdate {
                            set,
                            where_predicate,
                        }) => {
                            let assignments = make_update_assignments_pg_sqlite(set);
                            Some(S::OnInsert::OnConflict(OnConflict {
                                conflict_target: target,
                                action: OnConflictAction::DoUpdate(DoUpdate {
                                    assignments,
                                    selection: where_predicate.clone(),
                                }),
                            }))
                        }
                    }
                } else if self.insert_ignore {
                    None
                } else {
                    None
                }
            }

            Dialect::MySQL => {
                // В MySQL апсерт — через DUPLICATE KEY UPDATE; ignore() печатается префиксом INSERT IGNORE
                if let Some(spec) = &self.on_conflict {
                    if let Some(ConflictAction::DoUpdate {
                        set,
                        where_predicate,
                    }) = &spec.action
                    {
                        let assignments = make_update_assignments_mysql(set);
                        let _ = where_predicate;
                        Some(S::OnInsert::DuplicateKeyUpdate(assignments))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };

        // 5) Сборка самого Insert
        let insert = S::Insert {
            table: S::TableObject::TableName(table),
            columns: self.columns.into_vec(),
            source: Some(Box::new(query)),
            on,
            returning,
            ignore: self.insert_ignore,
            after_columns: vec![],
            assignments: vec![],
            format_clause: None,
            has_table_keyword: false,
            insert_alias: None,
            into: false,
            or: None,
            overwrite: false,
            partitioned: None,
            priority: None,
            replace_into: false,
            settings: None,
            table_alias: None,
        };

        // 6) Параметры — порядок важен: сначала VALUES, потом RHS из merge()
        let mut params: Vec<Param> = Vec::new();
        for r in self.rows {
            if !r.params.is_empty() {
                params.extend(r.params.into_iter());
            }
        }
        if !self.params.is_empty() {
            params.extend(self.params.into_iter());
        }

        Ok((S::Statement::Insert(insert), params))
    }
}

impl UpdateBuilder {
    pub(crate) fn build_update_ast(self) -> Result<(S::Statement, Vec<Param>)> {
        // 1) проверки
        let table = match &self.table {
            Some(t) => t.clone(),
            None => {
                return Err(Error::InvalidExpression {
                    reason: "update: table is not set".into(),
                });
            }
        };
        if self.set.is_empty() {
            return Err(Error::InvalidExpression {
                reason: "update: SET is empty".into(),
            });
        }
        if !self.builder_errors.is_empty() {
            return Err(Error::InvalidExpression {
                reason: format!("update: build errors: {:?}", self.builder_errors).into(),
            });
        }

        // 2) assignments
        let assignments: Vec<S::Assignment> = self
            .set
            .into_iter()
            .map(|a| S::Assignment {
                target: S::AssignmentTarget::ColumnName(S::ObjectName::from(vec![S::Ident::new(
                    a.col,
                )])),
                value: a.value,
            })
            .collect();

        // 3) WHERE
        let selection = self.where_predicate;

        // 4) RETURNING
        let returning = if self.returning.is_empty() {
            None
        } else {
            Some(self.returning.into_vec())
        };

        // 5) Сборка Update
        // Простейший UPDATE <schema?.>table SET ... [WHERE ...] [RETURNING ...]
        // Без FROM/JOIN — при необходимости можно будет расширить.
        let table_factor = S::TableFactor::Table {
            name: table,
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
            with_ordinality: false,
            index_hints: vec![],
            json_path: None,
            sample: None,
        };
        let table_with_joins = S::TableWithJoins {
            relation: table_factor,
            joins: vec![],
        };

        let from: Option<S::UpdateTableFromKind> = if self.from_items.is_empty() {
            None
        } else {
            let mut tables: Vec<S::TableWithJoins> = Vec::with_capacity(self.from_items.len());
            for it in self.from_items {
                match it {
                    FromItem::TableName(name) => {
                        let tf = S::TableFactor::Table {
                            name,
                            alias: None,
                            args: None,
                            with_hints: vec![],
                            version: None,
                            partitions: vec![],
                            with_ordinality: false,
                            index_hints: vec![],
                            json_path: None,
                            sample: None,
                        };
                        tables.push(S::TableWithJoins {
                            relation: tf,
                            joins: vec![],
                        });
                    }
                    FromItem::Subquery(_) | FromItem::SubqueryClosure(_) => {
                        return Err(Error::InvalidExpression {
                            reason: "update.from(): subqueries are not supported yet".into(),
                        });
                    }
                }
            }
            Some(S::UpdateTableFromKind::AfterSet(tables))
        };

        // SQLite OR
        let or_clause = self.sqlite_or;

        Ok((
            S::Statement::Update {
                assignments,
                from,
                selection: selection,
                or: or_clause,
                returning: returning,
                table: table_with_joins,
            },
            self.params.into_vec(),
        ))
    }
}

impl DeleteBuilder {
    /// Построить sqlparser AST для DELETE
    pub(crate) fn build_delete_ast(self) -> Result<(S::Statement, Vec<Param>)> {
        // 1) проверки
        let table = match &self.table {
            Some(t) => t.clone(),
            None => {
                return Err(Error::InvalidExpression {
                    reason: "delete: table is not set".into(),
                });
            }
        };
        if !self.builder_errors.is_empty() {
            return Err(Error::InvalidExpression {
                reason: self
                    .builder_errors
                    .into_iter()
                    .map(|c| c.into_owned())
                    .collect::<Vec<_>>()
                    .join("; ")
                    .into(),
            });
        }

        // 2) целевая таблица (DELETE FROM <table>)
        let table_factor = S::TableFactor::Table {
            name: table,
            alias: None,
            args: None,
            with_hints: vec![],
            version: None,
            partitions: vec![],
            with_ordinality: false,
            index_hints: vec![],
            json_path: None,
            sample: None,
        };
        let from_vec = vec![S::TableWithJoins {
            relation: table_factor,
            joins: vec![],
        }];

        // 3) USING (если задано)
        let using = if self.using_items.is_empty() {
            None
        } else {
            let mut params = Vec::<Param>::new(); // на случай будущих подзапросов
            let mut list: Vec<S::TableWithJoins> = Vec::with_capacity(self.using_items.len());
            for it in self.using_items {
                match it {
                    FromItem::TableName(name) => {
                        let tf = S::TableFactor::Table {
                            name,
                            alias: None,
                            args: None,
                            with_hints: vec![],
                            version: None,
                            partitions: vec![],
                            with_ordinality: false,
                            index_hints: vec![],
                            json_path: None,
                            sample: None,
                        };
                        list.push(S::TableWithJoins {
                            relation: tf,
                            joins: vec![],
                        });
                    }
                    FromItem::Subquery(_) | FromItem::SubqueryClosure(_) => {
                        return Err(Error::InvalidExpression {
                            reason: "delete.using(): subqueries are not supported yet".into(),
                        });
                    }
                }
            }
            // params (если появятся) можно будет добавить к self.params
            drop(params);
            Some(list)
        };

        // 4) WHERE / RETURNING
        let selection = self.where_predicate;
        let returning = if self.returning.is_empty() {
            None
        } else {
            Some(self.returning.into_vec())
        };

        // 5) Сборка Delete
        let del = S::Delete {
            tables: Vec::new(), // multi-table DELETE (MySQL) — не используется
            from: S::FromTable::WithFromKeyword(from_vec),
            using, // USING ... (PG/MySQL)
            selection,
            returning,            // PG/SQLite (на рендере для MySQL будет игнор)
            order_by: Vec::new(), // MySQL-специфика — пока не поддерживаем
            limit: None,          // MySQL-специфика — пока не поддерживаем
        };

        // 6) Параметры
        let params = self.params.into_vec();

        Ok((S::Statement::Delete(del), params))
    }
}

#[inline]
fn make_update_assignments_pg_sqlite(set: &[Assignment]) -> Vec<S::Assignment> {
    set.iter()
        .map(|a| {
            let target = S::AssignmentTarget::ColumnName(S::ObjectName::from(vec![a.col.clone()]));
            let value = match &a.value {
                MergeValue::Expr(e) => e.clone(),
                MergeValue::FromInserted(id) => {
                    S::Expr::CompoundIdentifier(vec![S::Ident::new("EXCLUDED"), id.clone()])
                }
            };
            S::Assignment { target, value }
        })
        .collect()
}

#[inline]
fn make_update_assignments_mysql(set: &[Assignment]) -> Vec<S::Assignment> {
    set.iter()
        .map(|a| {
            let target = S::AssignmentTarget::ColumnName(S::ObjectName::from(vec![a.col.clone()]));
            let value = match &a.value {
                MergeValue::Expr(e) => e.clone(),
                MergeValue::FromInserted(id) => {
                    // В рендере INSERT мы добавим "AS new", здесь ссылаемся на new.col
                    S::Expr::CompoundIdentifier(vec![S::Ident::new("new"), id.clone()])
                }
            };
            S::Assignment { target, value }
        })
        .collect()
}
