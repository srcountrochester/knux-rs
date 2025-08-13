use super::Expression;

impl Expression {
    /// Псевдоним: .as("max_id") — имя метода как в Knex
    pub fn r#as(mut self, alias: &str) -> Self {
        self.alias = Some(alias.to_string());
        self
    }

    /// Синоним
    pub fn alias(self, alias: &str) -> Self {
        self.r#as(alias)
    }
}
