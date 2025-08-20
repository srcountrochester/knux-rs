use smallvec::SmallVec;
use sqlparser::ast::{
    Expr as SqlExpr, Ident, ObjectName, SelectItem, SelectItemQualifiedWildcardKind,
    WildcardAdditionalOptions,
};

use crate::param::Param;
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};
use crate::renderer::Dialect;

enum ArgKind {
    Ident(Ident),
    Expr {
        expr: SqlExpr,
        params: SmallVec<[Param; 8]>,
    },
}

/// Одна строка для VALUES(...)
#[derive(Debug, Clone)]
pub(crate) struct InsertRowNode {
    pub values: SmallVec<[SqlExpr; 8]>,
    pub params: SmallVec<[Param; 8]>,
}

impl InsertRowNode {
    #[inline]
    fn new(values: SmallVec<[SqlExpr; 8]>, params: SmallVec<[Param; 8]>) -> Self {
        Self { values, params }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum MergeValue {
    Expr(SqlExpr),       // обычное выражение справа
    FromInserted(Ident), // взять значение из вставляемой строки (EXCLUDED/new)
}

#[derive(Debug, Clone)]
pub(crate) struct Assignment {
    pub col: Ident,
    pub value: MergeValue,
}

#[derive(Debug, Clone)]
pub(crate) enum ConflictAction {
    DoNothing,
    DoUpdate {
        set: SmallVec<[Assignment; 8]>,
        where_predicate: Option<SqlExpr>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ConflictSpec {
    /// Целевые колонки (конфликтная цель). Если пусто — в рендере решаем по диалекту:
    ///   PG: для DO UPDATE нужно заполнить, для DO NOTHING — можно опустить.
    ///   SQLite: можно опустить в последней ON CONFLICT-ветке.
    ///   MySQL: будет преобразовано в ON DUPLICATE KEY UPDATE (target не нужен).
    pub target_columns: SmallVec<[Ident; 4]>,
    pub action: Option<ConflictAction>,
}

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
        self.table = Some(self.object_name_from(table));
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
                Ok((expr, _p)) => match expr {
                    SqlExpr::Identifier(id) => self.columns.push(id),
                    SqlExpr::CompoundIdentifier(mut parts) => {
                        if let Some(last) = parts.pop() {
                            self.columns.push(last);
                        } else {
                            self.push_builder_error("columns(): invalid identifier");
                        }
                    }
                    _ => self.push_builder_error("columns(): expected identifiers"),
                },
                Err(e) => self.push_builder_error(format!("columns(): {e}")),
            }
        }
        self
    }

    /// Данные для вставки.
    ///
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
        let mut cur_vals = SmallVec::<[SqlExpr; 8]>::new();
        let mut cur_params = SmallVec::<[Param; 8]>::new();
        let mut take = 0usize;

        for it in items {
            match it.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((expr, p)) => {
                    cur_vals.push(expr);
                    cur_params.extend(p.into_iter());
                    take += 1;

                    if take == n {
                        self.rows.push(InsertRowNode::new(cur_vals, cur_params));
                        // начать следующую строку
                        take = 0;
                        cur_vals = SmallVec::new();
                        cur_params = SmallVec::new();
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

    /// RETURNING <expr, ...>
    pub fn returning<L>(mut self, items: L) -> Self
    where
        L: ArgList,
    {
        let list = items.into_vec();
        if list.is_empty() {
            self.push_builder_error("returning(): empty list");
            return self;
        }
        for a in list {
            match a.try_into_expr() {
                Ok((expr, _p)) => self.returning.push(SelectItem::UnnamedExpr(expr)),
                Err(e) => self.push_builder_error(format!("returning(): {e}")),
            }
        }
        self
    }

    /// RETURNING ровно одного выражения. Перезаписывает ранее заданный returning.
    pub fn returning_one<L>(mut self, item: L) -> Self
    where
        L: ArgList,
    {
        let mut args = item.into_vec();
        if args.is_empty() {
            self.push_builder_error("returning_one(): expected a single expression");
            return self;
        }
        if args.len() != 1 {
            self.push_builder_error(format!(
                "returning_one(): expected 1 item, got {}",
                args.len()
            ));
            // берём первый корректный, чтобы не срывать пайплайн
        }
        // Берём первый и пытаемся сконвертировать в Expr
        match args.remove(0).try_into_expr() {
            Ok((expr, _)) => {
                self.returning.clear();
                self.returning.push(SelectItem::UnnamedExpr(expr));
            }
            Err(e) => self.push_builder_error(format!("returning_one(): {e}")),
        }
        self
    }

    /// RETURNING * — вернуть все колонки вставленных строк.
    /// (Поддерживается PG и SQLite; для MySQL рендер позже аккуратно отключит/свалидирует)
    pub fn returning_all(mut self) -> Self {
        self.returning.clear();
        self.returning
            .push(SelectItem::Wildcard(WildcardAdditionalOptions::default()));
        self
    }

    /// RETURNING <qualifier>.*
    pub fn returning_all_from(mut self, qualifier: &str) -> Self {
        self.returning.clear();

        // поддерживаем alias.* и schema.table.* (разбиваем по '.')
        let obj = ObjectName::from(qualifier.split('.').map(Ident::new).collect::<Vec<_>>());

        let kind = SelectItemQualifiedWildcardKind::ObjectName(obj);
        self.returning.push(SelectItem::QualifiedWildcard(
            kind,
            WildcardAdditionalOptions::default(),
        ));

        self
    }

    /// Указать цель конфликта: on_conflict((col1, col2, ...))
    /// Действие задаётся отдельно через ignore() или merge().
    pub fn on_conflict<L>(mut self, target_cols: L) -> Self
    where
        L: ArgList,
    {
        let mut cols = SmallVec::<[Ident; 4]>::new();
        let list = target_cols.into_vec();
        if list.is_empty() {
            self.push_builder_error("on_conflict(): expected at least one column");
            return self;
        }
        for a in list {
            match a.try_into_expr() {
                Ok((SqlExpr::Identifier(id), _)) => cols.push(id),
                Ok((SqlExpr::CompoundIdentifier(mut parts), _)) => {
                    if let Some(last) = parts.pop() {
                        cols.push(last);
                    } else {
                        self.push_builder_error("on_conflict(): invalid identifier");
                        return self;
                    }
                }
                Ok((_other, _)) => {
                    self.push_builder_error("on_conflict(): only identifiers are allowed");
                    return self;
                }
                Err(e) => {
                    self.push_builder_error(format!("on_conflict(): {e}"));
                    return self;
                }
            }
        }
        let spec = self.on_conflict.get_or_insert(ConflictSpec {
            target_columns: SmallVec::new(),
            action: None,
        });
        spec.target_columns = cols;
        self
    }

    /// Игнорировать конфликты вставки:
    ///   PG: ON CONFLICT [target?] DO NOTHING
    ///   SQLite: INSERT OR IGNORE
    ///   MySQL: INSERT IGNORE
    pub fn ignore(mut self) -> Self {
        self.insert_ignore = true;
        if let Some(spec) = &mut self.on_conflict {
            if spec.action.is_none() {
                spec.action = Some(ConflictAction::DoNothing);
            }
        }
        self
    }

    /// merge((col1, val1, col2, val2, ...)) — набор присваиваний для upsert.
    /// Для PG/SQLite попадёт в `ON CONFLICT ... DO UPDATE SET ...`,
    /// для MySQL — в `ON DUPLICATE KEY UPDATE ...`.
    pub fn merge<L>(mut self, assignments: L) -> Self
    where
        L: ArgList,
    {
        let Some(set) = self.parse_assignments(assignments.into_vec()) else {
            return self; // ошибки уже записаны
        };
        let spec = self.on_conflict.get_or_insert(ConflictSpec {
            target_columns: SmallVec::new(),
            action: None,
        });
        spec.action = Some(ConflictAction::DoUpdate {
            set,
            where_predicate: None, // добавим отдельным методом при необходимости
        });
        self
    }

    /// Обновить **все** колонки значениями из вставляемой строки.
    /// Требует, чтобы список колонок был известен (через `.columns(...)`
    /// или получен из пар `(col, value)` в первой вставке).
    pub fn merge_all(mut self) -> Self {
        if self.columns.is_empty() {
            self.push_builder_error(
                "merge_all(): columns are unknown; call columns(...) or pass (col, value) pairs first",
            );
            return self;
        }

        // Сконструируем SET col = <from-inserted>
        let mut set = SmallVec::<[Assignment; 8]>::new();
        for id in &self.columns {
            set.push(Assignment {
                col: id.clone(),
                value: MergeValue::FromInserted(id.clone()),
            });
        }

        let spec = self.on_conflict.get_or_insert(ConflictSpec {
            target_columns: SmallVec::new(),
            action: None,
        });
        spec.action = Some(ConflictAction::DoUpdate {
            set,
            where_predicate: None,
        });
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
            let ident = match col_expr {
                SqlExpr::Identifier(id) => id,
                SqlExpr::CompoundIdentifier(mut parts) => {
                    if let Some(last) = parts.pop() {
                        last
                    } else {
                        self.push_builder_error("insert(): invalid column identifier");
                        return None;
                    }
                }
                _ => {
                    self.push_builder_error("insert(): column must be identifier");
                    return None;
                }
            };
            col_names.push(ident);

            // значение — любое выражение/подзапрос
            match val_arg.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((expr, p)) => {
                    values.push(expr);
                    params.extend(p.into_iter());
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

    /// Конструирует ObjectName c учётом default_schema.
    fn object_name_from(&self, table: &str) -> ObjectName {
        if table.contains('.') {
            // schema.table (или многосоставное имя)
            let idents: Vec<Ident> = table.split('.').map(|s| Ident::new(s)).collect();
            ObjectName::from(idents)
        } else if let Some(schema) = &self.default_schema {
            // префиксуем дефолтной схемой
            ObjectName::from(vec![Ident::new(schema.clone()), Ident::new(table)])
        } else {
            // простое имя
            ObjectName::from(vec![Ident::new(table)])
        }
    }

    fn parse_assignments(&mut self, flat: Vec<QBArg>) -> Option<SmallVec<[Assignment; 8]>> {
        if flat.is_empty() {
            self.push_builder_error("merge(): empty assignment list");
            return None;
        }

        let mut kinds: Vec<ArgKind> = Vec::with_capacity(flat.len());
        for a in flat.into_iter() {
            match a.try_into_expr() {
                Ok((SqlExpr::Identifier(id), _p)) => kinds.push(ArgKind::Ident(id)),
                Ok((SqlExpr::CompoundIdentifier(mut parts), _p)) => {
                    if let Some(last) = parts.pop() {
                        kinds.push(ArgKind::Ident(last));
                    } else {
                        self.push_builder_error("merge(): invalid compound identifier");
                        return None;
                    }
                }
                Ok((expr, p)) => kinds.push(ArgKind::Expr {
                    expr,
                    params: p.into(),
                }),
                Err(e) => {
                    self.push_builder_error(format!("merge(): {e}"));
                    return None;
                }
            }
        }

        // Режим A: все элементы — идентификаторы ⇒ короткая форма (обновить значениями из вставки)
        if kinds.iter().all(|k| matches!(k, ArgKind::Ident(_))) {
            let mut set = SmallVec::<[Assignment; 8]>::new();
            for k in kinds {
                if let ArgKind::Ident(id) = k {
                    set.push(Assignment {
                        col: id.clone(),
                        value: MergeValue::FromInserted(id),
                    });
                }
            }
            return Some(set);
        }

        // Режим B: пары (col, value) — длина должна быть чётной
        if kinds.len() % 2 != 0 {
            self.push_builder_error("merge(): expected pairs (col, value) or columns-only");
            return None;
        }

        let mut set = SmallVec::<[Assignment; 8]>::new();
        let mut it = kinds.into_iter();
        while let (Some(kc), Some(kv)) = (it.next(), it.next()) {
            let col = match kc {
                ArgKind::Ident(id) => id,
                ArgKind::Expr { .. } => {
                    self.push_builder_error("merge(): left item must be a column identifier");
                    return None;
                }
            };

            match kv {
                // Значение — выражение: переносим его параметры в общий буфер билдерa
                ArgKind::Expr { expr, mut params } => {
                    if !params.is_empty() {
                        self.params.extend(params.drain(..));
                    }
                    set.push(Assignment {
                        col,
                        value: MergeValue::Expr(expr),
                    });
                }
                // Разрешаем и идентификатор справа как обычное выражение (не FromInserted!)
                ArgKind::Ident(id) => {
                    set.push(Assignment {
                        col,
                        value: MergeValue::Expr(SqlExpr::Identifier(id)),
                    });
                }
            }
        }

        Some(set)
    }

    #[inline]
    fn push_builder_error<S: Into<std::borrow::Cow<'static, str>>>(&mut self, msg: S) {
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
