use crate::query_builder::{
    QueryBuilder, Result,
    args::{ArgList, QBArg},
};
use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectNamePart, TableAlias, TableFactor, TableWithJoins,
};

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
        let active_schema = self
            .pending_schema
            .take()
            .or_else(|| self.default_schema.clone());

        for arg in args {
            match arg {
                // Строки/Expression (через helpers::col) → имя таблицы
                QBArg::Expr(e) => {
                    let mut p = e.params;
                    self.params.append(&mut p);

                    if let Some(name) = Self::expr_to_object_name(e.expr, active_schema.as_deref())
                    {
                        let tf = TableFactor::Table {
                            name,
                            alias: None,
                            args: None,
                            with_hints: vec![],
                            partitions: vec![],
                            version: None,
                            index_hints: vec![],
                            json_path: None,
                            sample: None,
                            with_ordinality: false,
                        };
                        self.from_tables.push(TableWithJoins {
                            relation: tf,
                            joins: vec![],
                        });
                    }
                }

                // Подзапрос: FROM ( <built-query> )
                QBArg::Subquery(qb) => {
                    let alias = qb.alias.clone();
                    if let Ok((q, mut p)) = qb.build_query_ast() {
                        self.params.append(&mut p);

                        let tf = TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        };
                        self.from_tables.push(TableWithJoins {
                            relation: tf,
                            joins: vec![],
                        });
                    }
                }

                // Closure → строим внутренний QueryBuilder и превращаем в subquery
                QBArg::Closure(c) => {
                    let built = c.apply(QueryBuilder::new_empty());
                    let alias = built.alias.clone();

                    if let Ok((q, mut p)) = built.build_query_ast() {
                        self.params.append(&mut p);

                        let tf = TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        };
                        self.from_tables.push(TableWithJoins {
                            relation: tf,
                            joins: vec![],
                        });
                    }
                }
            }
        }
        self
    }

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
