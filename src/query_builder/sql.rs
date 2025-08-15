use crate::{
    param::Param,
    query_builder::{QueryBuilder, Result},
    renderer::{self, Dialect, FeaturePolicy},
};

impl QueryBuilder {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (query_ast, params) = self.build_query_ast()?;
        let rq = renderer::map_to_render_query(&query_ast);

        let cfg = match dialect {
            Dialect::Postgres => renderer::cfg_postgres_knex(),
            Dialect::MySQL => renderer::cfg_mysql_knex(),
            Dialect::SQLite => renderer::cfg_sqlite_knex(),
        };
        let sql = if matches!(cfg.policy, FeaturePolicy::Strict) {
            renderer::try_render_sql_query(&rq, &cfg)?
        } else {
            renderer::render_sql_query(&rq, &cfg)
        };

        Ok((sql, params))
    }
}
