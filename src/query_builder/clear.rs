use crate::query_builder::QueryBuilder;

impl QueryBuilder {
    /// Универсальный сброс части запроса.
    ///
    /// Поддерживаемые значения:
    /// - "select", "columns" → см. `.clear_select()`
    /// - "where"            → см. `.clear_where()`
    /// - "group"            → см. `.clear_group()`
    /// - "order"            → см. `.clear_order()`
    /// - "having"           → см. `.clear_having()`
    /// - "join"             → см. `.clear_join()`
    /// - "limit"            → см. `.clear_limit()`
    /// - "offset"           → см. `.clear_offset()`
    /// - "counter", "counters" → см. `.clear_counters()`
    /// - "with", "union"    → пока не поддерживаем — запишем ошибку билдера.
    pub fn clear(mut self, op: &str) -> Self {
        let key = op.to_ascii_lowercase();
        match key.as_str() {
            "select" | "columns" => self.clear_select(),
            "where" => self.clear_where(),
            "group" => self.clear_group(),
            "order" => self.clear_order(),
            "having" => self.clear_having(),
            "join" => self.clear_join(),
            "limit" => self.clear_limit(),
            "offset" => self.clear_offset(),
            "counter" | "counters" => self.clear_counters(),
            "with" | "union" => {
                self.push_builder_error(format!("clear('{op}'): оператор пока не поддерживается"));
                self
            }
            _ => {
                self.push_builder_error(format!("clear(): неизвестный оператор '{op}'"));
                self
            }
        }
    }

    /// Сбросить список выбранных столбцов (SELECT …).
    /// По умолчанию потом рендерится `SELECT *`, если ничего не добавить.
    #[inline]
    pub fn clear_select(mut self) -> Self {
        self.select_items.clear();
        self
    }

    /// Сбросить WHERE-условие целиком.
    #[inline]
    pub fn clear_where(mut self) -> Self {
        self.where_clause = None;
        self
    }

    /// Сбросить GROUP BY.
    #[inline]
    pub fn clear_group(mut self) -> Self {
        self.group_by_items.clear();
        self
    }

    /// Сбросить ORDER BY.
    #[inline]
    pub fn clear_order(mut self) -> Self {
        self.order_by_items.clear();
        self
    }

    /// Сбросить HAVING-условие целиком.
    #[inline]
    pub fn clear_having(mut self) -> Self {
        self.having_clause = None;
        self
    }

    /// Сбросить все JOIN-ы для всех источников в FROM.
    #[inline]
    pub fn clear_join(mut self) -> Self {
        for joins in &mut self.from_joins {
            joins.clear();
        }
        self
    }

    /// Сбросить LIMIT.
    #[inline]
    pub fn clear_limit(mut self) -> Self {
        self.limit_num = None;
        self
    }

    /// Сбросить OFFSET.
    #[inline]
    pub fn clear_offset(mut self) -> Self {
        self.offset_num = None;
        self
    }

    /// Сбросить LIMIT/OFFSET (на случай, если билдер переиспользуется)
    #[inline]
    pub fn clear_limit_offset(&mut self) -> &mut Self {
        self.limit_num = None;
        self.offset_num = None;
        self
    }

    /// Сбросить счётчики инкрементов/декрементов (пока не реализовано).
    #[inline]
    pub fn clear_counters(mut self) -> Self {
        self.push_builder_error("clear_counters(): TODO — ещё не реализовано");
        self
    }
}
