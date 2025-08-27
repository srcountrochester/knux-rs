use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Ident};

use super::core_fn::UpdateBuilder;
use super::set::Assignment;
use crate::query_builder::args::IntoQBArg;

impl<'a, T> UpdateBuilder<'a, T> {
    /// Увеличить значение столбца на указанное выражение: `SET <col> = <col> + <value>`.
    ///
    /// Первый аргумент — колонка (строка или `Expression`), второй — выражение-значение (обычно `val(...)`).
    ///
    /// Пример:
    /// ```
    /// use knux::{QueryBuilder, UpdateBuilder, col, table, val};
    /// let _b: UpdateBuilder<'_, ()> = QueryBuilder::new_empty()
    ///     .update(table("users"))
    ///     .where_(col("id").eq(val(1)))
    ///     .increment("balance", val(100));
    /// ```
    pub fn increment<L, R>(mut self, left: L, right: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        match (
            left.into_qb_arg().try_into_expr(),
            right.into_qb_arg().try_into_expr(),
        ) {
            (Ok((l_expr, _)), Ok((r_expr, mut r_params))) => {
                // Извлечь имя колонки из идентификатора (последний сегмент)
                let col = match l_expr {
                    SqlExpr::Identifier(id) => id.value,
                    SqlExpr::CompoundIdentifier(mut parts) => match parts.pop() {
                        Some(id) => id.value,
                        None => {
                            self.push_builder_error("increment(): invalid compound identifier");
                            return self;
                        }
                    },
                    _ => {
                        self.push_builder_error(
                            "increment(): left item must be a column identifier",
                        );
                        return self;
                    }
                };

                // RHS: <col> + <expr>
                let value = SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Identifier(Ident::new(col.clone()))),
                    op: BinaryOperator::Plus,
                    right: Box::new(r_expr),
                };

                if !r_params.is_empty() {
                    self.params.append(&mut r_params);
                }
                self.set.push(Assignment { col, value });
            }
            (Err(e), _) => self.push_builder_error(format!("increment(): {e}")),
            (_, Err(e)) => self.push_builder_error(format!("increment(): {e}")),
        }
        self
    }

    /// Уменьшить значение столбца на указанное выражение: `SET <col> = <col> - <value>`.
    ///
    /// Первый аргумент — колонка (строка или `Expression`), второй — выражение-значение (обычно `val(...)`).
    ///
    /// Пример:
    /// ```
    /// use knux::{QueryBuilder, UpdateBuilder,col, table, val};
    /// let _b: UpdateBuilder<'_, ()> = QueryBuilder::new_empty()
    ///     .update(table("users"))
    ///     .where_(col("id").eq(val(1)))
    ///     .decrement(col("balance"), val(100));
    /// ```
    pub fn decrement<L, R>(mut self, left: L, right: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        match (
            left.into_qb_arg().try_into_expr(),
            right.into_qb_arg().try_into_expr(),
        ) {
            (Ok((l_expr, _)), Ok((r_expr, mut r_params))) => {
                let col = match l_expr {
                    SqlExpr::Identifier(id) => id.value,
                    SqlExpr::CompoundIdentifier(mut parts) => match parts.pop() {
                        Some(id) => id.value,
                        None => {
                            self.push_builder_error("decrement(): invalid compound identifier");
                            return self;
                        }
                    },
                    _ => {
                        self.push_builder_error(
                            "decrement(): left item must be a column identifier",
                        );
                        return self;
                    }
                };

                let value = SqlExpr::BinaryOp {
                    left: Box::new(SqlExpr::Identifier(Ident::new(col.clone()))),
                    op: BinaryOperator::Minus,
                    right: Box::new(r_expr),
                };

                if !r_params.is_empty() {
                    self.params.append(&mut r_params);
                }
                self.set.push(Assignment { col, value });
            }
            (Err(e), _) => self.push_builder_error(format!("decrement(): {e}")),
            (_, Err(e)) => self.push_builder_error(format!("decrement(): {e}")),
        }
        self
    }

    /// Сбросить счётчики инкрементов/декрементов в `SET`.
    ///
    /// Удаляет присваивания вида `SET <col> = <col> + <expr>` и `SET <col> = <col> - <expr>`.
    /// Остальные `SET` (обычные присваивания) не трогаются.
    ///
    /// Пример:
    /// ```rust,ignore
    /// use knux::{QueryBuilder, col, table, val};
    /// let b: UpdateBuilder<'_, ()> = QueryBuilder::new_empty()
    ///     .update(table("users"))
    ///     .set((col("a"), val(0)))
    ///     .increment("a", val(1))
    ///     .decrement(col("b"), val(2))
    ///     .clear_counters();
    /// // останется только SET "a" = 0
    /// ```
    #[inline]
    pub fn clear_counters(mut self) -> Self {
        use sqlparser::ast::Expr as E;

        self.set.retain(|a| {
            match &a.value {
                E::BinaryOp { left, op, .. } => {
                    // левая часть бинарной операции должна совпадать с именем колонки присваивания
                    let left_col = match &**left {
                        E::Identifier(id) => id.value.as_str(),
                        E::CompoundIdentifier(parts) if !parts.is_empty() => {
                            parts.last().unwrap().value.as_str()
                        }
                        _ => return true, // не считаем это счётчиком
                    };
                    // если это col +/- expr по той же колонке — удаляем (retain=false)
                    !(left_col == a.col
                        && matches!(op, BinaryOperator::Plus | BinaryOperator::Minus))
                }
                _ => true, // другие выражения оставляем
            }
        });
        self
    }
}
