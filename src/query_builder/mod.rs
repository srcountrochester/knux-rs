use crate::{executor::DbPool, param::Param};
use smallvec::{SmallVec, smallvec};
use sqlparser::ast::{
    self, Expr, GroupByExpr, Query, Select, SelectFlavor, SelectItem, TableWithJoins,
    WildcardAdditionalOptions, helpers::attached_token::AttachedToken,
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
pub struct QueryBuilder {
    pub pool: Option<DbPool>,
    pub select_items: SmallVec<[SelectItem; 4]>,
    pub from_tables: SmallVec<[TableWithJoins; 1]>,
    pub where_clause: Option<Expr>,
    pub params: SmallVec<[Param; 8]>,
    pub default_schema: Option<String>,
    pub(crate) pending_schema: Option<String>,
    pub alias: Option<String>,
}

impl QueryBuilder {
    pub fn new(pool: DbPool, schema: Option<String>) -> Self {
        Self {
            pool: Some(pool),
            select_items: smallvec![],
            from_tables: smallvec![],
            where_clause: None,
            params: smallvec![],
            default_schema: schema,
            pending_schema: None,
            alias: None,
        }
    }

    /// Пустой QueryBuilder без пула — удобно для замыканий |qb| qb.select(...)
    pub fn new_empty() -> Self {
        Self {
            pool: None,
            select_items: smallvec![],
            from_tables: smallvec![],
            where_clause: None,
            params: smallvec![],
            default_schema: None,
            pending_schema: None,
            alias: None,
        }
    }

    pub fn with_default_schema(mut self, schema: Option<String>) -> Self {
        self.default_schema = schema;
        self
    }

    /// Очищает накопленные параметры
    pub fn clear_params(&mut self) -> &mut Self {
        self.params.clear();
        self
    }

    pub(crate) fn build_query_ast(self) -> Result<(Query, SmallVec<[Param; 8]>)> {
        // проекция по умолчанию: SELECT *
        let projection = if self.select_items.is_empty() {
            smallvec![SelectItem::Wildcard(WildcardAdditionalOptions::default()),]
        } else {
            self.select_items
        };

        // FROM: либо один TableWithJoins, либо пусто
        let from: Vec<TableWithJoins> = self.from_tables.into_vec();

        let select = Select {
            distinct: None,
            top: None,
            projection: projection.into_vec(),
            into: None,
            from,
            lateral_views: vec![],
            selection: self.where_clause,
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

        Ok((query, self.params))
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new_empty()
    }
}
