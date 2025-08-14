use crate::{executor::DbPool, param::Param, query_builder::args::QBClosure, renderer::Dialect};
use smallvec::{SmallVec, smallvec};
use sqlparser::ast::{
    self, Expr, GroupByExpr, Ident, Query, Select, SelectFlavor, SelectItem, TableAlias,
    TableFactor, TableWithJoins, WildcardAdditionalOptions, helpers::attached_token::AttachedToken,
};

mod __tests__;
mod alias;
mod args;
mod error;
mod from;
mod schema;
mod select;
mod sql;

pub use error::{Error, Result};

#[derive(Debug)]
enum FromItem {
    TableName(sqlparser::ast::ObjectName),
    Subquery(Box<QueryBuilder>),
    SubqueryClosure(QBClosure),
}

#[derive(Debug)]
pub struct QueryBuilder {
    pub pool: Option<DbPool>,
    pub select_items: SmallVec<[SelectItem; 4]>,
    pub(self) from_items: SmallVec<[FromItem; 1]>,
    pub where_clause: Option<Expr>,
    pub params: SmallVec<[Param; 8]>,
    pub default_schema: Option<String>,
    pub(crate) pending_schema: Option<String>,
    pub alias: Option<String>,
    pub(crate) dialect: Dialect,
}

impl QueryBuilder {
    pub fn new(pool: DbPool, schema: Option<String>) -> Self {
        Self {
            pool: Some(pool),
            select_items: smallvec![],
            from_items: smallvec![],
            where_clause: None,
            params: smallvec![],
            default_schema: schema,
            pending_schema: None,
            alias: None,
            #[cfg(feature = "sqlite")]
            dialect: Dialect::SQLite,
            #[cfg(feature = "postgres")]
            dialect: Dialect::Postgres,
            #[cfg(feature = "mysql")]
            dialect: Dialect::MySQL,
        }
    }

    /// Пустой QueryBuilder без пула — удобно для замыканий |qb| qb.select(...)
    pub fn new_empty() -> Self {
        Self {
            pool: None,
            select_items: smallvec![],
            from_items: smallvec![],
            where_clause: None,
            params: smallvec![],
            default_schema: None,
            pending_schema: None,
            alias: None,
            #[cfg(feature = "sqlite")]
            dialect: Dialect::SQLite,
            #[cfg(feature = "postgres")]
            dialect: Dialect::Postgres,
            #[cfg(feature = "mysql")]
            dialect: Dialect::MySQL,
        }
    }

    #[inline]
    pub fn with_default_schema(mut self, schema: Option<String>) -> Self {
        self.default_schema = schema;
        self
    }

    #[inline]
    pub fn with_estimated_select_capacity(mut self, cap: usize) -> Self {
        self.select_items.reserve(cap);
        self
    }

    #[inline]
    pub fn with_estimated_from_capacity(mut self, cap: usize) -> Self {
        self.from_items.reserve(cap);
        self
    }

    #[inline]
    pub fn with_estimated_param_capacity(mut self, cap: usize) -> Self {
        self.params.reserve(cap);
        self
    }

    #[inline]
    /// Очищает накопленные параметры
    pub fn clear_params(&mut self) -> &mut Self {
        self.params.clear();
        self
    }

    pub(crate) fn build_query_ast(mut self) -> Result<(Query, Vec<Param>)> {
        // проекция по умолчанию: SELECT *
        let projection = if self.select_items.is_empty() {
            let mut sv = SmallVec::new();
            sv.push(SelectItem::Wildcard(Default::default()));
            sv
        } else {
            self.select_items
        };

        let mut params = self.params.into_vec();
        // FROM: либо один TableWithJoins, либо пусто
        let mut from: Vec<TableWithJoins> = Vec::with_capacity(self.from_items.len());

        for item in self.from_items.into_iter() {
            match item {
                FromItem::TableName(name) => from.push(TableWithJoins {
                    joins: vec![],
                    relation: TableFactor::Table {
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
                    },
                }),
                FromItem::Subquery(qb) => {
                    let alias = qb.alias.clone();
                    let (q, mut p) = qb.build_query_ast()?;
                    if !p.is_empty() {
                        params.append(&mut p);
                    }
                    from.push(TableWithJoins {
                        joins: vec![],
                        relation: TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        },
                    })
                }
                FromItem::SubqueryClosure(closure) => {
                    let built = closure.apply(QueryBuilder::new_empty());
                    let alias = built.alias.clone();
                    let (q, mut p) = built.build_query_ast()?;
                    if !p.is_empty() {
                        params.append(&mut p);
                    }
                    from.push(TableWithJoins {
                        joins: vec![],
                        relation: TableFactor::Derived {
                            lateral: false,
                            subquery: Box::new(q),
                            alias: alias.map(|a| TableAlias {
                                name: Ident::new(a),
                                columns: vec![],
                            }),
                        },
                    })
                }
            }
        }

        let selection = self.where_clause;

        let select = Select {
            distinct: None,
            top: None,
            projection: projection.into_vec(),
            into: None,
            from,
            lateral_views: vec![],
            selection,
            group_by: GroupByExpr::Expressions(vec![], vec![]),
            cluster_by: vec![],
            distribute_by: vec![],
            sort_by: vec![],
            having: None,
            named_window: vec![],
            qualify: None,
            connect_by: None,
            exclude: None,
            prewhere: None,
            value_table_mode: None,
            top_before_distinct: false,
            window_before_qualify: false,
            flavor: SelectFlavor::Standard,
            select_token: AttachedToken::empty(),
        };

        let query = Query {
            with: None,
            body: Box::new(ast::SetExpr::Select(Box::new(select))),
            order_by: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
            format_clause: None,
            limit_clause: None,
            pipe_operators: vec![],
            settings: None,
        };

        Ok((query, params))
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new_empty()
    }
}
