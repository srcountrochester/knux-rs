use crate::param::Param;
use crate::query_builder::insert::utils::{expr_last_ident, object_name_from_default};
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};
use crate::renderer::Dialect;
use smallvec::SmallVec;
use sqlparser::ast::{Expr as SqlExpr, Ident, ObjectName, SelectItem};

use super::utils::{ConflictSpec, InsertRowNode};

/// Билдер INSERT INTO ... VALUES ...
#[derive(Debug, Clone)]
pub struct InsertBuilder {
    pub(crate) table: Option<ObjectName>,
    pub(crate) columns: SmallVec<[Ident; 8]>,
    pub(crate) rows: SmallVec<[InsertRowNode; 1]>,
    pub(crate) params: SmallVec<[Param; 8]>,
    pub(crate) returning: SmallVec<[SelectItem; 4]>,
    pub(crate) on_conflict: Option<ConflictSpec>,
    pub(crate) insert_ignore: bool, // для MySQL/SQLite/PG-DO-NOTHING

    // ошибки сбора (аналогично QueryBuilder)
    pub(crate) builder_errors: SmallVec<[std::borrow::Cow<'static, str>; 2]>,

    // контекст (для резолва подзапросов в значениях)
    pub(crate) default_schema: Option<String>,
    pub(crate) dialect: Dialect,
}

impl InsertBuilder {
    #[inline]
    pub(crate) fn from_qb(qb: QueryBuilder) -> Self {
        Self {
            table: None,
            columns: SmallVec::new(),
            rows: SmallVec::new(),
            params: qb.params, // переносим накопленные параметры (если были)
            builder_errors: SmallVec::new(),
            default_schema: qb.default_schema,
            dialect: qb.dialect,
            returning: SmallVec::new(),
            on_conflict: None,
            insert_ignore: false,
        }
    }

    /// Целевая таблица: `INSERT INTO <table>`
    #[inline]
    pub fn into(mut self, table: &str) -> Self {
        self.table = Some(object_name_from_default(
            self.default_schema.as_deref(),
            table,
        ));
        self
    }

    /// Явно задать список колонок: `INSERT INTO t (c1, c2, ...)`
    pub fn columns<L>(mut self, cols: L) -> Self
    where
        L: ArgList,
    {
        let items = cols.into_vec();
        if items.is_empty() {
            self.push_builder_error("columns(): empty column list");
            return self;
        }

        for it in items {
            match it.try_into_expr() {
                Ok((expr, _p)) => match expr_last_ident(expr) {
                    Ok(id) => self.columns.push(id),
                    Err(_) => self.push_builder_error("columns(): expected identifiers"),
                },
                Err(e) => self.push_builder_error(format!("columns(): {e}")),
            }
        }
        self
    }

    /// Данные для вставки.
    pub fn insert<L>(mut self, data: L) -> Self
    where
        L: ArgList,
    {
        let items = data.into_vec();

        if self.columns.is_empty() {
            // одна запись через пары (col, value)
            if let Some(row) = self.parse_row_from_pairs(items) {
                self.rows.push(row);
            }
            return self;
        }

        // --- колонки заданы ---
        let n = self.columns.len();
        if items.is_empty() {
            self.push_builder_error("insert(): no values provided");
            return self;
        }
        if items.len() % n != 0 {
            self.push_builder_error(format!(
                "insert(): expected number of values multiple of {n}, got {}",
                items.len()
            ));
            return self;
        }

        // последовательно собираем выражения и параметры, каждые n штук — новая строка
        let mut cur_vals: SmallVec<[SqlExpr; 8]> = SmallVec::with_capacity(n);
        let mut cur_params: SmallVec<[Param; 8]> = SmallVec::new();
        let mut take = 0usize;

        for it in items {
            match it.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((expr, p)) => {
                    cur_vals.push(expr);
                    cur_params.extend(p);
                    take += 1;

                    if take == n {
                        self.rows.push(InsertRowNode::new(
                            core::mem::take(&mut cur_vals),
                            core::mem::take(&mut cur_params),
                        ));
                        take = 0;
                    }
                }
                Err(e) => {
                    self.push_builder_error(format!("insert(): {e}"));
                    return self;
                }
            }
        }

        self
    }

    // ===== вспомогательные =====

    /// Интерпретировать плоский список как (col, value) пары.
    fn parse_row_from_pairs(&mut self, flat: Vec<QBArg>) -> Option<InsertRowNode> {
        if flat.is_empty() {
            self.push_builder_error("insert(): empty data");
            return None;
        }
        if flat.len() % 2 != 0 {
            self.push_builder_error("insert(): expected pairs (col, value)");
            return None;
        }

        let mut col_names: SmallVec<[Ident; 8]> = SmallVec::new();
        let mut values: SmallVec<[SqlExpr; 8]> = SmallVec::new();
        let mut params: SmallVec<[Param; 8]> = SmallVec::new();

        let mut it = flat.into_iter();
        while let Some(col_arg) = it.next() {
            let Some(val_arg) = it.next() else {
                self.push_builder_error("insert(): broken (col, value) pair");
                return None;
            };

            // колонка: только идентификатор (одиночный или составной) — берём последний сегмент
            let Ok((col_expr, _)) = col_arg.try_into_expr() else {
                self.push_builder_error(
                    "insert(): column name must be identifier/str/expression-ident",
                );
                return None;
            };
            match expr_last_ident(col_expr) {
                Ok(id) => col_names.push(id),
                Err(_) => {
                    self.push_builder_error("insert(): column must be identifier");
                    return None;
                }
            }

            // значение — любое выражение/подзапрос
            match val_arg.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((expr, p)) => {
                    values.push(expr);
                    params.extend(p);
                }
                Err(e) => {
                    self.push_builder_error(format!("insert(): value build failed: {e}"));
                    return None;
                }
            }
        }

        // если columns ещё не задан — зафиксируем «эталон» колонок из пары
        if self.columns.is_empty() {
            self.columns = col_names;
        } else if self.columns.len() != col_names.len()
            || self
                .columns
                .iter()
                .zip(col_names.iter())
                .any(|(a, b)| a.value != b.value)
        {
            self.push_builder_error("insert(): columns mismatch with previously defined columns");
            return None;
        }

        Some(InsertRowNode::new(values, params))
    }

    #[inline]
    pub(crate) fn push_builder_error<S: Into<std::borrow::Cow<'static, str>>>(&mut self, msg: S) {
        self.builder_errors.push(msg.into());
    }
}

impl QueryBuilder {
    /// Начать INSERT сразу с данными (таблицу можно указать потом через .into())
    pub fn insert<L>(self, row_or_values: L) -> InsertBuilder
    where
        L: ArgList,
    {
        InsertBuilder::from_qb(self).insert(row_or_values)
    }

    /// Начать INSERT с указанием таблицы (данные можно передать потом через .insert(...))
    pub fn into(self, table: &str) -> InsertBuilder {
        InsertBuilder::from_qb(self).into(table)
    }
}
