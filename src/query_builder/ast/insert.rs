use std::mem;

use crate::{
    param::Param,
    query_builder::{
        InsertBuilder,
        ast::{make_update_assignments_mysql, make_update_assignments_pg_sqlite},
        insert::ConflictAction,
    },
    renderer::Dialect,
};
use sqlparser::ast::{self as S};

use super::super::{Error, Result};

impl<'a, T> InsertBuilder<'a, T> {
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
                params.extend(r.params);
            }
        }
        if !self.params.is_empty() {
            params.extend(self.params);
        }

        Ok((S::Statement::Insert(insert), params))
    }

    // #[inline]
    // pub(crate) fn build_insert_ast(mut self) -> Result<(S::Statement, Vec<Param>)> {
    //     self.form_insert_ast()
    // }

    pub(crate) fn form_insert_ast(&mut self) -> Result<(S::Statement, Vec<Param>)> {
        // 1) минимальная валидация
        let Some(table) = self.table.clone() else {
            return Err(Error::InvalidExpression {
                reason: "insert: table is not set".into(),
            });
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

        // 2) Вынуть строки целиком, чтобы переместить values/params без клонирования
        let rows = mem::take(&mut self.rows);

        // VALUES → Query(SetExpr::Values)
        let mut params: Vec<Param> = Vec::new();
        let mut rows_exprs: Vec<Vec<S::Expr>> = Vec::with_capacity(rows.len());
        for mut r in rows {
            rows_exprs.push(mem::take(&mut r.values).into_vec()); // move
            if !r.params.is_empty() {
                params.extend(mem::take(&mut r.params)); // move
            }
        }

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

        // 3) RETURNING (заберём список, если был)
        let returning = {
            let ret = mem::take(&mut self.returning).into_vec();
            if ret.is_empty() { None } else { Some(ret) }
        };

        // 4) ON / IGNORE по диалектам — как было
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
                        None | Some(ConflictAction::DoNothing) => {
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
                        None | Some(ConflictAction::DoNothing) => {
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
                if let Some(spec) = &self.on_conflict {
                    if let Some(ConflictAction::DoUpdate {
                        set,
                        where_predicate: _,
                    }) = &spec.action
                    {
                        let assignments = make_update_assignments_mysql(set);
                        Some(S::OnInsert::DuplicateKeyUpdate(assignments))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        };

        // 5) Собрать Insert (колонки тоже вынимаем без копии)
        let columns = mem::take(&mut self.columns).into_vec();
        let insert = S::Insert {
            table: S::TableObject::TableName(table),
            columns,
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

        // 6) Параметры RHS из self.params (move)
        let rhs = mem::take(&mut self.params);
        if !rhs.is_empty() {
            params.extend(rhs);
        }

        Ok((S::Statement::Insert(insert), params))
    }
}
