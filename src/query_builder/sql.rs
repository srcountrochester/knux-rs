use crate::{
    param::Param,
    query_builder::{QueryBuilder, Result},
    renderer::{self, Dialect, map::map_to_render_ast},
};

impl QueryBuilder {
    #[inline]
    // pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
    //     let (query, params) = self.build_query_ast()?;
    //     Ok((query.to_string(), params.to_vec()))
    // }
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (query_ast, params) = self.build_query_ast()?;
        let rsel: renderer::ast::Select = map_to_render_ast(&query_ast);

        let cfg = match dialect {
            Dialect::Postgres => renderer::cfg_postgres_knex(),
            Dialect::MySQL => renderer::cfg_mysql_knex(),
            Dialect::SQLite => renderer::cfg_sqlite_knex(),
        };

        let sql = renderer::render_sql_select(&rsel, &cfg);
        Ok((sql, params))
    }
}
