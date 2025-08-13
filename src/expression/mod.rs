mod __tests__;
use crate::param::Param;
use sqlparser::ast;

/// Чейнящийся конструктор выражений на базе AST.
/// Параметры (binds) собираются в `params` и позже подцепятся к запросу.
#[derive(Clone, Debug)]
pub struct Expression {
    pub(crate) expr: ast::Expr,
    pub(crate) alias: Option<String>,
    pub(crate) params: Vec<Param>,
    pub(crate) mark_distinct_for_next: bool,
}

impl Expression {
    /// Доступ к внутреннему AST (нужно для интеграции внутри билдера)
    pub fn __into_parts(self) -> (ast::Expr, Option<String>, Vec<Param>) {
        (self.expr, self.alias, self.params)
    }
}

pub mod aggr;
pub mod alias;
pub mod cmp;
pub mod helpers;
pub mod logic;
pub mod math;

pub use helpers::{col, lit, raw, val};
