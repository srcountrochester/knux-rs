use crate::query_builder::QueryBuilder;

impl QueryBuilder {
    /// Принудительно задать схему для следующего FROM
    pub fn schema<S: Into<String>>(mut self, schema: S) -> Self {
        self.pending_schema = Some(schema.into());
        self
    }
}
