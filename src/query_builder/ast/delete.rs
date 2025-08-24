use std::mem;

use crate::{param::Param, query_builder::delete::DeleteBuilder};
use sqlparser::ast::{self as S};

use super::super::{Error, Result};
use super::FromItem;

impl<'a, T> DeleteBuilder<'a, T> {
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

    // #[inline]
    // pub(crate) fn build_delete_ast(mut self) -> Result<(S::Statement, Vec<Param>)> {
    //     self.form_delete_ast()
    // }

    pub(crate) fn form_delete_ast(&mut self) -> Result<(S::Statement, Vec<Param>)> {
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
            // аккуратно вынем сообщения и склеим
            let errs = mem::take(&mut self.builder_errors);
            return Err(Error::InvalidExpression {
                reason: errs
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

        // 3) USING (если задано) — вынем элементы без копий
        let using_items = mem::take(&mut self.using_items).into_vec();
        let using = if using_items.is_empty() {
            None
        } else {
            let mut list: Vec<S::TableWithJoins> = Vec::with_capacity(using_items.len());
            for it in using_items {
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
            Some(list)
        };

        // 4) WHERE / RETURNING — тоже переносим
        let selection = self.where_predicate.take();
        let returning_vec = mem::take(&mut self.returning).into_vec();
        let returning = if returning_vec.is_empty() {
            None
        } else {
            Some(returning_vec)
        };

        // 5) Сборка Delete
        let del = S::Delete {
            tables: Vec::new(), // multi-table DELETE (MySQL) — не используется
            from: S::FromTable::WithFromKeyword(from_vec),
            using, // USING ... (PG/MySQL)
            selection,
            returning, // PG/SQLite (на рендере для MySQL будет игнор)
            order_by: Vec::new(),
            limit: None,
        };

        // 6) Параметры — вынем одним движением
        let params = mem::take(&mut self.params).into_vec();

        Ok((S::Statement::Delete(del), params))
    }
}
