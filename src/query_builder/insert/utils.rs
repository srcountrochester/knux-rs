use crate::param::Param;
use smallvec::SmallVec;
use sqlparser::ast::{Expr as SqlExpr, Ident};

/// Одна строка для VALUES(...)
#[derive(Debug, Clone)]
pub(crate) struct InsertRowNode {
    pub values: SmallVec<[SqlExpr; 8]>,
    pub params: SmallVec<[Param; 8]>,
}

impl InsertRowNode {
    #[inline]
    pub(crate) fn new(values: SmallVec<[SqlExpr; 8]>, params: SmallVec<[Param; 8]>) -> Self {
        Self { values, params }
    }
}

#[derive(Debug, Clone)]
pub enum MergeValue {
    Expr(SqlExpr),       // обычное выражение справа
    FromInserted(Ident), // взять значение из вставляемой строки (EXCLUDED/new)
}

#[derive(Debug, Clone)]
pub struct Assignment {
    pub col: Ident,
    pub value: MergeValue,
}

#[derive(Debug, Clone)]
pub enum ConflictAction {
    DoNothing,
    DoUpdate {
        set: SmallVec<[Assignment; 8]>,
        where_predicate: Option<SqlExpr>,
    },
}

#[derive(Debug, Clone)]
pub struct ConflictSpec {
    /// Целевые колонки (конфликтная цель). Если пусто — в рендере решаем по диалекту:
    ///   PG: для DO UPDATE нужно заполнить, для DO NOTHING — можно опустить.
    ///   SQLite: можно опустить в последней ON CONFLICT-ветке.
    ///   MySQL: будет преобразовано в ON DUPЛICATE KEY UPDATE (target не нужен).
    pub target_columns: SmallVec<[Ident; 4]>,
    pub action: Option<ConflictAction>,
}

/// Возвращает последний сегмент идентификатора:
/// - Identifier("a")        -> Ok("a")
/// - CompoundIdentifier(a.b)-> Ok("b")
/// Иначе Err(...)
#[inline]
pub(crate) fn expr_last_ident(expr: SqlExpr) -> Result<Ident, &'static str> {
    match expr {
        SqlExpr::Identifier(id) => Ok(id),
        SqlExpr::CompoundIdentifier(mut parts) => parts.pop().ok_or("invalid compound identifier"),
        _ => Err("expression is not an identifier"),
    }
}
