use std::borrow::Cow;

use super::Expression;

impl Expression {
    /// Псевдоним: .as("max_id") — имя метода как в Knex
    pub fn r#as<S: Into<Cow<'static, str>>>(mut self, alias: S) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Синоним
    pub fn alias<S: Into<Cow<'static, str>>>(self, alias: S) -> Self {
        self.r#as(alias)
    }
}
