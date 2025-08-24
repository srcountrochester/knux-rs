use smallvec::SmallVec;
use sqlparser::ast::{Expr as SqlExpr, Ident};

use super::core_fn::InsertBuilder;
use super::utils::{Assignment, ConflictAction, ConflictSpec, MergeValue};
use crate::param::Param;
use crate::query_builder::args::{ArgList, QBArg};

enum ArgKind {
    Ident(Ident),
    Expr {
        expr: SqlExpr,
        params: SmallVec<[Param; 8]>,
    },
}

impl<'a, T> InsertBuilder<'a, T> {
    /// merge((col1, val1, col2, val2, ...)) — набор присваиваний для upsert.
    /// Для PG/SQLite попадёт в `ON CONFLICT ... DO UPDATE SET ...`,
    /// для MySQL — в `ON DUPLICATE KEY UPDATE ...`.
    pub fn merge<L>(mut self, assignments: L) -> Self
    where
        L: ArgList<'a>,
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
        let mut set = SmallVec::<[Assignment; 8]>::with_capacity(self.columns.len().max(8));
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

    fn parse_assignments(&mut self, flat: Vec<QBArg>) -> Option<SmallVec<[Assignment; 8]>> {
        if flat.is_empty() {
            self.push_builder_error("merge(): empty assignment list");
            return None;
        }

        let mut kinds: SmallVec<[ArgKind; 8]> = SmallVec::with_capacity(flat.len().min(8));
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
                ArgKind::Expr { expr, params } => {
                    if !params.is_empty() {
                        self.params.extend(params);
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
}
