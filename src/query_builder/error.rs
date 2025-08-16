use core::fmt;
use std::borrow::Cow;

use smallvec::SmallVec;

use crate::renderer;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Невалидный аргумент/выражение для данного контекста
    #[error("Invalid expression: {reason}")]
    InvalidExpression { reason: Cow<'static, str> },

    /// Резолв подзапроса невозможен без билдера (вызвали не тот метод)
    #[error("Subquery requires a builder function (missing context)")]
    MissingSubqueryBuilder,

    #[error(transparent)]
    SQLRenderError(#[from] renderer::Error),

    #[error("Builder errors:\n{0}")]
    BuilderErrors(BuilderErrorList),
}

#[derive(Debug, Default)]
pub struct BuilderErrorList(pub Vec<String>);

impl From<Vec<String>> for BuilderErrorList {
    fn from(v: Vec<String>) -> Self {
        Self(v)
    }
}

impl From<Vec<Cow<'static, str>>> for BuilderErrorList {
    fn from(v: Vec<Cow<'static, str>>) -> Self {
        Self(v.into_iter().map(|c| c.into_owned()).collect())
    }
}

impl From<SmallVec<[Cow<'static, str>; 2]>> for BuilderErrorList {
    fn from(mut v: SmallVec<[Cow<'static, str>; 2]>) -> Self {
        // drain чтобы избежать копий
        Self(v.drain(..).map(|c| c.into_owned()).collect())
    }
}

impl fmt::Display for BuilderErrorList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return write!(f, "no details");
        }
        for (i, line) in self.0.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            write!(f, "- {line}")?;
        }
        Ok(())
    }
}
