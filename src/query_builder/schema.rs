use crate::query_builder::QueryBuilder;

impl<'a, T> QueryBuilder<'a, T> {
    /// Принудительно задать схему для следующего FROM
    pub fn schema<S: Into<String>>(mut self, schema: S) -> Self {
        self.pending_schema = Some(schema.into());
        self
    }

    #[inline]
    pub(super) fn active_schema(&self) -> Option<&str> {
        // если добавил pending_schema — сначала она, иначе default
        #[allow(unused)]
        if let Some(s) = self.pending_schema.as_deref() {
            return Some(s);
        }
        self.default_schema.as_deref()
    }
}
