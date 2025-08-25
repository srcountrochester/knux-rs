use crate::{
    optimizer,
    param::Param,
    query_builder::{
        InsertBuilder, QueryBuilder, Result, delete::DeleteBuilder, update::UpdateBuilder,
    },
    renderer::{self, Dialect, FeaturePolicy, ast as R},
};

impl<'a, T> QueryBuilder<'a, T> {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut query_ast, params) = self.build_query_ast()?;
        optimizer::apply_query(&mut query_ast, &opt_cfg);

        let rq = renderer::map_to_render_query(&query_ast);
        let sql = form_sql(dialect, rq)?;

        Ok((sql, params))
    }

    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut query_ast, params) = self.form_query_ast()?;
        optimizer::apply_query(&mut query_ast, &opt_cfg);

        let rq = renderer::map_to_render_query(&query_ast);
        let sql = form_sql(dialect, rq)?;

        Ok((sql, params))
    }
}

impl<'a, T> InsertBuilder<'a, T> {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut stmt_ast, params) = self.build_insert_ast()?;
        optimizer::apply(&mut stmt_ast, &opt_cfg);

        let rstmt = renderer::map_to_render_stmt(&stmt_ast);
        let sql = form_dml_sql(dialect, rstmt)?;

        Ok((sql, params))
    }

    #[inline]
    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut stmt_ast, params) = self.form_insert_ast()?;
        optimizer::apply(&mut stmt_ast, &opt_cfg);

        let rstmt = renderer::map_to_render_stmt(&stmt_ast);
        let sql = form_dml_sql(dialect, rstmt)?;

        Ok((sql, params))
    }
}

impl<'a, T> UpdateBuilder<'a, T> {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut stmt_ast, params) = self.build_update_ast()?;
        optimizer::apply(&mut stmt_ast, &opt_cfg);

        let rstmt = renderer::map_to_render_stmt(&stmt_ast);
        let sql = form_dml_sql(dialect, rstmt)?;

        Ok((sql, params))
    }

    #[inline]
    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut stmt_ast, params) = self.form_update_ast()?;
        optimizer::apply(&mut stmt_ast, &opt_cfg);

        let rstmt = renderer::map_to_render_stmt(&stmt_ast);
        let sql = form_dml_sql(dialect, rstmt)?;

        Ok((sql, params))
    }
}

impl<'a, T> DeleteBuilder<'a, T> {
    #[inline]
    pub fn to_sql(self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut stmt_ast, params) = self.build_delete_ast()?;
        optimizer::apply(&mut stmt_ast, &opt_cfg);

        let rstmt = crate::renderer::map_to_render_stmt(&stmt_ast);
        let sql = form_dml_sql(dialect, rstmt)?;

        Ok((sql, params))
    }

    #[inline]
    pub(crate) fn render_sql(&mut self) -> Result<(String, Vec<Param>)> {
        let dialect = self.dialect.clone();
        let opt_cfg = self.optimize_cfg.clone();

        let (mut stmt_ast, params) = self.form_delete_ast()?;
        optimizer::apply(&mut stmt_ast, &opt_cfg);

        let rstmt = crate::renderer::map_to_render_stmt(&stmt_ast);
        let sql = form_dml_sql(dialect, rstmt)?;

        Ok((sql, params))
    }
}

fn form_sql(dialect: Dialect, rq: R::Query) -> Result<String> {
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

    Ok(sql)
}

fn form_dml_sql(dialect: Dialect, rstmt: R::Stmt) -> Result<String> {
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

    Ok(sql)
}
