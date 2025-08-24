use crate::query_builder::QueryBuilder;

impl<'a, T> QueryBuilder<'a, T> {
    #[inline]
    /// Псевдоним подзапроса: используется, когда этот билдер окажется в FROM (... ) AS <alias>
    pub fn r#as<S: Into<String>>(mut self, alias: S) -> Self {
        self.alias = Some(alias.into());
        self
    }

    #[inline]
    pub fn alias<S: Into<String>>(self, alias: S) -> Self {
        self.r#as(alias)
    }
}
