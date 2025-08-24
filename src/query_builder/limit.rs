use crate::query_builder::QueryBuilder;

impl<'a, T> QueryBuilder<'a, T> {
    /// LIMIT <n>
    #[inline]
    pub fn limit(mut self, n: u64) -> Self {
        self.limit_num = Some(n);
        self
    }

    /// OFFSET <n>
    #[inline]
    pub fn offset(mut self, n: u64) -> Self {
        self.offset_num = Some(n);
        self
    }

    /// LIMIT <limit> OFFSET <offset>
    #[inline]
    pub fn limit_offset(mut self, limit: u64, offset: u64) -> Self {
        self.limit_num = Some(limit);
        self.offset_num = Some(offset);
        self
    }
}
