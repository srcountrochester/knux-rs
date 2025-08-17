mod __tests__;
use std::borrow::Cow;

use crate::param::Param;
use smallvec::SmallVec;
use sqlparser::ast::{self, Ident};

/// Чейнящийся конструктор выражений на базе AST.
/// Параметры (binds) собираются в `params` и позже подцепятся к запросу.
#[derive(Clone, Debug)]
pub struct Expression {
    pub(crate) expr: ast::Expr,
    pub alias: Option<Cow<'static, str>>,
    pub(crate) params: SmallVec<[Param; 8]>,
    pub(crate) mark_distinct_for_next: bool,
}

impl Expression {
    /// Доступ к внутреннему AST (нужно для интеграции внутри билдера)
    pub(crate) fn __into_parts(self) -> (ast::Expr, Option<String>, Vec<Param>) {
        (
            self.expr,
            self.alias.map_or(None, |a| Some(a.into_owned())),
            self.params.into_vec(),
        )
    }

    /// Вынуть (alias, expr, params) для контекста SELECT-проекции.
    /// Если алиаса нет — вернёт None.
    pub(crate) fn into_projection_parts(
        mut self,
    ) -> (Option<Ident>, ast::Expr, SmallVec<[crate::param::Param; 8]>) {
        // Если у тебя поле называется иначе — поправь (`self.projection_alias`, и т.д.)
        let alias_opt = self.alias.take().map(Ident::new);
        (alias_opt, self.expr, self.params)
    }
}

pub mod aggr;
pub mod alias;
pub mod cmp;
pub mod helpers;
pub mod join_on;
pub mod logic;
pub mod math;
pub mod path;

pub use helpers::{col, lit, raw, schema, table, val};
pub use join_on::{JoinOnBuilder, JoinOnExt};
