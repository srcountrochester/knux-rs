use crate::{executor::DbPool, param::Param};
use sqlparser::ast::{
    self, Expr, GroupByExpr, Query, Select, SelectFlavor, SelectItem, TableWithJoins,
    WildcardAdditionalOptions, helpers::attached_token::AttachedToken,
};

mod __tests__;
mod args;
mod error;
mod select;

pub use error::{Error, Result};

#[derive(Debug)]
pub struct QueryBuilder {
    pub pool: Option<DbPool>,
    pub select_items: Vec<SelectItem>,
    pub from_table: Option<TableWithJoins>,
    pub where_clause: Option<Expr>,
    pub params: Vec<Param>,
    pub default_schema: Option<String>,
}

impl QueryBuilder {
    pub fn new(pool: DbPool, schema: Option<String>) -> Self {
        Self {
            pool: Some(pool),
            select_items: Vec::new(),
            from_table: None,
            where_clause: None,
            params: Vec::new(),
            default_schema: schema,
        }
    }

    /// Пустой QueryBuilder без пула — удобно для замыканий |qb| qb.select(...)
    pub fn new_empty() -> Self {
        Self {
            pool: None,
            select_items: Vec::new(),
            from_table: None,
            where_clause: None,
            params: Vec::new(),
            default_schema: None,
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

    pub(crate) fn build_query_ast(self) -> Result<(Query, Vec<Param>)> {
        // проекция по умолчанию: SELECT *
        let projection = if self.select_items.is_empty() {
            vec![SelectItem::Wildcard(WildcardAdditionalOptions::default())]
        } else {
            self.select_items
        };

        // FROM: либо один TableWithJoins, либо пусто
        let from: Vec<TableWithJoins> = match self.from_table {
            Some(t) => vec![t],
            None => Vec::new(),
        };

        let select = Select {
            distinct: None,
            top: None,
            projection,
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
