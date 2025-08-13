use std::borrow::Cow;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Невалидный аргумент/выражение для данного контекста
    #[error("Invalid expression: {reason}")]
    InvalidExpression { reason: Cow<'static, str> },

    /// Резолв подзапроса невозможен без билдера (вызвали не тот метод)
    #[error("Subquery requires a builder function (missing context)")]
    MissingSubqueryBuilder,
}
