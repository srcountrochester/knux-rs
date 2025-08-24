use std::mem;

use crate::{param::Param, query_builder::update::UpdateBuilder};
use sqlparser::ast::{self as S};

use super::super::{Error, Result};
use super::FromItem;

impl<'a, T> UpdateBuilder<'a, T> {
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

    // #[inline]
    // pub(crate) fn build_update_ast(mut self) -> Result<(S::Statement, Vec<Param>)> {
    //     self.form_update_ast()
    // }

    pub(crate) fn form_update_ast(&mut self) -> Result<(S::Statement, Vec<Param>)> {
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

        // 2) assignments (переносим set из SmallVec)
        let set_items = mem::take(&mut self.set).into_vec();
        let assignments: Vec<S::Assignment> = set_items
            .into_iter()
            .map(|a| S::Assignment {
                target: S::AssignmentTarget::ColumnName(S::ObjectName::from(vec![S::Ident::new(
                    a.col,
                )])),
                value: a.value,
            })
            .collect();

        // 3) WHERE (забираем предикат)
        let selection = self.where_predicate.take();

        // 4) RETURNING (переносим из SmallVec)
        let returning_vec = mem::take(&mut self.returning).into_vec();
        let returning = if returning_vec.is_empty() {
            None
        } else {
            Some(returning_vec)
        };

        // 5) FROM (переносим список источников)
        let from_items = mem::take(&mut self.from_items).into_vec();
        let from: Option<S::UpdateTableFromKind> = if from_items.is_empty() {
            None
        } else {
            let mut tables: Vec<S::TableWithJoins> = Vec::with_capacity(from_items.len());
            for it in from_items {
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

        // 6) SQLite OR (переносим значение)
        let or_clause = self.sqlite_or.take();

        // 7) таблица-цель
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

        // 8) параметры (переносим из SmallVec)
        let params = mem::take(&mut self.params).into_vec();

        Ok((
            S::Statement::Update {
                assignments,
                from,
                selection,
                or: or_clause,
                returning,
                table: table_with_joins,
            },
            params,
        ))
    }
}
