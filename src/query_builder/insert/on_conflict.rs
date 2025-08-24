use super::core_fn::InsertBuilder;
use super::utils::{ConflictAction, ConflictSpec};
use smallvec::SmallVec;
use sqlparser::ast::Ident;

use crate::query_builder::args::ArgList;

impl<'a, T> InsertBuilder<'a, T> {
    /// Указать цель конфликта: on_conflict((col1, col2, ...))
    /// Действие задаётся отдельно через ignore() или merge().
    pub fn on_conflict<L>(mut self, target_cols: L) -> Self
    where
        L: ArgList<'a>,
    {
        let mut cols = SmallVec::<[Ident; 4]>::new();
        let list = target_cols.into_vec();
        if list.is_empty() {
            self.push_builder_error("on_conflict(): expected at least one column");
            return self;
        }
        for a in list {
            match a.try_into_expr() {
                Ok((expr, _)) => match super::utils::expr_last_ident(expr) {
                    Ok(id) => cols.push(id),
                    Err(_) => {
                        self.push_builder_error("on_conflict(): only identifiers are allowed");
                        return self;
                    }
                },
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
}
