use crate::{
    param::Param,
    query_builder::{
        InsertBuilder, QueryBuilder, Result, delete::DeleteBuilder, update::UpdateBuilder,
    },
    renderer::{self, Dialect, FeaturePolicy},
};

impl<'a, T> QueryBuilder<'a, T> {
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

    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (query_ast, params) = self.form_query_ast()?;
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

impl<'a, T> InsertBuilder<'a, T> {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (stmt_ast, params) = self.build_insert_ast()?;

        // маппим В ЗАВИСИМОСТИ ОТ ТИПА — в нашем случае это Statement::Insert
        let rstmt = renderer::map_to_render_stmt(&stmt_ast);

        let cfg = match dialect {
            Dialect::Postgres => renderer::cfg_postgres_knex(),
            Dialect::MySQL => renderer::cfg_mysql_knex(),
            Dialect::SQLite => renderer::cfg_sqlite_knex(),
        };

        let sql = if matches!(cfg.policy, FeaturePolicy::Strict) {
            renderer::try_render_sql_stmt(&rstmt, &cfg)?
        } else {
            renderer::render_sql_stmt(&rstmt, &cfg)
        };

        Ok((sql, params))
    }

    #[inline]
    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (stmt_ast, params) = self.form_insert_ast()?;

        // маппим В ЗАВИСИМОСТИ ОТ ТИПА — в нашем случае это Statement::Insert
        let rstmt = renderer::map_to_render_stmt(&stmt_ast);

        let cfg = match dialect {
            Dialect::Postgres => renderer::cfg_postgres_knex(),
            Dialect::MySQL => renderer::cfg_mysql_knex(),
            Dialect::SQLite => renderer::cfg_sqlite_knex(),
        };

        let sql = if matches!(cfg.policy, FeaturePolicy::Strict) {
            renderer::try_render_sql_stmt(&rstmt, &cfg)?
        } else {
            renderer::render_sql_stmt(&rstmt, &cfg)
        };

        Ok((sql, params))
    }
}

impl<'a, T> UpdateBuilder<'a, T> {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (stmt_ast, params) = self.build_update_ast()?; // см. ниже п.7

        let rstmt = renderer::map_to_render_stmt(&stmt_ast);

        let cfg = match dialect {
            Dialect::Postgres => renderer::cfg_postgres_knex(),
            Dialect::MySQL => renderer::cfg_mysql_knex(),
            Dialect::SQLite => renderer::cfg_sqlite_knex(),
        };

        let sql = if matches!(cfg.policy, renderer::FeaturePolicy::Strict) {
            renderer::try_render_sql_stmt(&rstmt, &cfg)?
        } else {
            renderer::render_sql_stmt(&rstmt, &cfg)
        };

        Ok((sql, params))
    }

    #[inline]
    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (stmt_ast, params) = self.form_update_ast()?; // см. ниже п.7

        let rstmt = renderer::map_to_render_stmt(&stmt_ast);

        let cfg = match dialect {
            Dialect::Postgres => renderer::cfg_postgres_knex(),
            Dialect::MySQL => renderer::cfg_mysql_knex(),
            Dialect::SQLite => renderer::cfg_sqlite_knex(),
        };

        let sql = if matches!(cfg.policy, renderer::FeaturePolicy::Strict) {
            renderer::try_render_sql_stmt(&rstmt, &cfg)?
        } else {
            renderer::render_sql_stmt(&rstmt, &cfg)
        };

        Ok((sql, params))
    }
}

impl<'a, T> DeleteBuilder<'a, T> {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (stmt_ast, params) = self.build_delete_ast()?;

        let rstmt = crate::renderer::map_to_render_stmt(&stmt_ast);

        let cfg = match dialect {
            Dialect::Postgres => crate::renderer::cfg_postgres_knex(),
            Dialect::MySQL => crate::renderer::cfg_mysql_knex(),
            Dialect::SQLite => crate::renderer::cfg_sqlite_knex(),
        };

        let sql = if matches!(cfg.policy, crate::renderer::FeaturePolicy::Strict) {
            crate::renderer::try_render_sql_stmt(&rstmt, &cfg)?
        } else {
            crate::renderer::render_sql_stmt(&rstmt, &cfg)
        };

        Ok((sql, params))
    }

    #[inline]
    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let (stmt_ast, params) = self.form_delete_ast()?;

        let rstmt = crate::renderer::map_to_render_stmt(&stmt_ast);

        let cfg = match dialect {
            Dialect::Postgres => crate::renderer::cfg_postgres_knex(),
            Dialect::MySQL => crate::renderer::cfg_mysql_knex(),
            Dialect::SQLite => crate::renderer::cfg_sqlite_knex(),
        };

        let sql = if matches!(cfg.policy, crate::renderer::FeaturePolicy::Strict) {
            crate::renderer::try_render_sql_stmt(&rstmt, &cfg)?
        } else {
            crate::renderer::render_sql_stmt(&rstmt, &cfg)
        };

        Ok((sql, params))
    }
}
