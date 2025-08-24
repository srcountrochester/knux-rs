use crate::{
    query_builder::{
        FromItem, QueryBuilder,
        args::{ArgList, QBArg},
    },
    utils::expr_to_object_name,
};

impl<'a, T> QueryBuilder<'a, T> {
    /// FROM <table | (subquery)>
    /// Поддерживает:
    /// - &str / String: имя таблицы (если без схемы — подставит default_schema, если есть)
    /// - подзапросы (QueryBuilder / |qb| {...}) → FROM ( ... )
    pub fn from<L>(mut self, table: L) -> Self
    where
        L: ArgList<'a>,
    {
        let args = table.into_vec();
        self.from_items.reserve(args.len());

        for arg in args {
            match arg {
                // Строки/Expression (через helpers::col) → имя таблицы
                QBArg::Expr(e) => {
                    let mut p = e.params;
                    if !p.is_empty() {
                        self.params.append(&mut p);
                    }

                    if let Some(name) = expr_to_object_name(e.expr, self.active_schema()) {
                        self.from_items.push(FromItem::TableName(name));
                    }
                }

                // Подзапрос: FROM ( <built-query> )
                QBArg::Subquery(qb) => self.from_items.push(FromItem::Subquery(Box::new(qb))),

                // Closure → строим внутренний QueryBuilder и превращаем в subquery
                QBArg::Closure(c) => self.from_items.push(FromItem::SubqueryClosure(c)),
            }
        }
        self
    }

    #[inline]
    pub fn from_mut<L>(&mut self, items: L) -> &mut Self
    where
        L: ArgList<'a>,
    {
        let v = std::mem::take(&mut *self);
        *self = v.from(items);
        self
    }
}
