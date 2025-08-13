use crate::query_builder::{
    FromItem, QueryBuilder,
    args::{ArgList, QBArg},
};
use sqlparser::ast::{Expr, Ident, ObjectName, ObjectNamePart};

impl QueryBuilder {
    /// FROM <table | (subquery)>
    /// Поддерживает:
    /// - &str / String: имя таблицы (если без схемы — подставит default_schema, если есть)
    /// - подзапросы (QueryBuilder / |qb| {...}) → FROM ( ... )
    pub fn from<L>(mut self, table: L) -> Self
    where
        L: ArgList,
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

                    if let Some(name) = Self::expr_to_object_name(e.expr, self.active_schema()) {
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
        L: ArgList,
    {
        let v = std::mem::take(&mut *self);
        *self = v.from(items);
        self
    }

    #[inline]
    /// Попытка интерпретировать Expr как имя таблицы:
    /// - Identifier("users")  → [default_schema?].users
    /// - CompoundIdentifier(["s","t"]) → s.t
    fn expr_to_object_name(expr: Expr, default_schema: Option<&str>) -> Option<ObjectName> {
        match expr {
            Expr::Identifier(ident) => {
                let mut parts = Vec::new();
                if let Some(s) = default_schema {
                    parts.push(ObjectNamePart::Identifier(Ident::new(s)));
                }
                parts.push(ObjectNamePart::Identifier(ident));
                Some(ObjectName(parts))
            }
            Expr::CompoundIdentifier(idents) => {
                let parts = idents
                    .into_iter()
                    .map(ObjectNamePart::Identifier)
                    .collect::<Vec<_>>();
                Some(ObjectName(parts))
            }
            _ => None,
        }
    }
}
