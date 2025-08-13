use crate::query_builder::QueryBuilder;

impl QueryBuilder {
    /// Псевдоним подзапроса: используется, когда этот билдер окажется в FROM (... ) AS <alias>
    pub fn r#as<S: Into<String>>(mut self, alias: S) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn alias<S: Into<String>>(self, alias: S) -> Self {
        self.r#as(alias)
    }
}
