use std::marker::PhantomData;
use std::pin::Pin;

use crate::executor::{DbPool, DbRow, Error as ExecError, Result as ExecResult};
use crate::optimizer::OptimizeConfig;
use crate::param::Param;
use crate::query_builder::ExecCtx;
use crate::query_builder::insert::utils::expr_last_ident;
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};
use crate::renderer::Dialect;
use crate::utils::expr_to_object_name;
use smallvec::SmallVec;
use sqlparser::ast::{Expr as SqlExpr, Ident, ObjectName, SelectItem};

#[cfg(feature = "mysql")]
use crate::executor::utils::fetch_typed_mysql;
#[cfg(feature = "postgres")]
use crate::executor::utils::fetch_typed_pg;
#[cfg(feature = "sqlite")]
use crate::executor::utils::fetch_typed_sqlite;

#[cfg(feature = "mysql")]
use crate::executor::transaction_utils::fetch_typed_mysql_exec;
#[cfg(feature = "postgres")]
use crate::executor::transaction_utils::fetch_typed_pg_exec;
#[cfg(feature = "sqlite")]
use crate::executor::transaction_utils::fetch_typed_sqlite_exec;

use super::utils::{ConflictSpec, InsertRowNode};

/// Билдер INSERT INTO ... VALUES ...
#[derive(Debug, Clone)]
pub struct InsertBuilder<'a, T = ()> {
    pub(crate) table: Option<ObjectName>,
    pub(crate) columns: SmallVec<[Ident; 8]>,
    pub(crate) rows: SmallVec<[InsertRowNode; 1]>,
    pub(crate) params: SmallVec<[Param; 8]>,
    pub(crate) returning: SmallVec<[SelectItem; 4]>,
    pub(crate) on_conflict: Option<ConflictSpec>,
    pub(crate) insert_ignore: bool, // для MySQL/SQLite/PG-DO-NOTHING

    // ошибки сбора (аналогично QueryBuilder)
    pub(crate) builder_errors: SmallVec<[std::borrow::Cow<'static, str>; 2]>,

    // контекст (для резолва подзапросов в значениях)
    pub(crate) default_schema: Option<String>,
    pub(crate) dialect: Dialect,
    pub(crate) exec_ctx: ExecCtx<'a>,
    pub(crate) optimize_cfg: OptimizeConfig,
    _t: PhantomData<T>,
}

impl<'a, T> InsertBuilder<'a, T> {
    #[inline]
    pub(crate) fn from_qb(qb: QueryBuilder<'a, T>) -> Self {
        Self {
            table: None,
            columns: SmallVec::new(),
            rows: SmallVec::new(),
            params: qb.params, // переносим накопленные параметры (если были)
            builder_errors: SmallVec::new(),
            default_schema: qb.default_schema,
            dialect: qb.dialect,
            returning: SmallVec::new(),
            on_conflict: None,
            insert_ignore: false,
            exec_ctx: qb.exec_ctx,
            optimize_cfg: qb.optimize_cfg,
            _t: PhantomData,
        }
    }

    /// Целевая таблица: `INSERT INTO <table>`
    #[inline]
    pub fn into<L>(mut self, table_arg: L) -> Self
    where
        L: ArgList<'a>,
    {
        let mut args = table_arg.into_vec();
        if args.is_empty() {
            self.push_builder_error("into(): table is not set");
            return self;
        }
        if args.len() > 1 {
            self.push_builder_error("into(): expected a single table argument");
        }

        match args.swap_remove(0).try_into_expr() {
            Ok((expr, _)) => {
                if let Some(obj) = expr_to_object_name(expr, self.default_schema.as_deref()) {
                    self.table = Some(obj);
                } else {
                    self.push_builder_error(
                        "into(): invalid table reference; expected identifier or schema.table",
                    );
                }
            }
            Err(e) => self.push_builder_error(format!("into(): {e}")),
        }
        self
    }

    /// Явно задать список колонок: `INSERT INTO t (c1, c2, ...)`
    pub fn columns<L>(mut self, cols: L) -> Self
    where
        L: ArgList<'a>,
    {
        let items = cols.into_vec();
        if items.is_empty() {
            self.push_builder_error("columns(): empty column list");
            return self;
        }

        for it in items {
            match it.try_into_expr() {
                Ok((expr, _p)) => match expr_last_ident(expr) {
                    Ok(id) => self.columns.push(id),
                    Err(_) => self.push_builder_error("columns(): expected identifiers"),
                },
                Err(e) => self.push_builder_error(format!("columns(): {e}")),
            }
        }
        self
    }

    /// Данные для вставки.
    pub fn insert<L>(mut self, data: L) -> Self
    where
        L: ArgList<'a>,
    {
        let items = data.into_vec();

        if self.columns.is_empty() {
            // одна запись через пары (col, value)
            if let Some(row) = self.parse_row_from_pairs(items) {
                self.rows.push(row);
            }
            return self;
        }

        // --- колонки заданы ---
        let n = self.columns.len();
        if items.is_empty() {
            self.push_builder_error("insert(): no values provided");
            return self;
        }
        if items.len() % n != 0 {
            self.push_builder_error(format!(
                "insert(): expected number of values multiple of {n}, got {}",
                items.len()
            ));
            return self;
        }

        // последовательно собираем выражения и параметры, каждые n штук — новая строка
        let mut cur_vals: SmallVec<[SqlExpr; 8]> = SmallVec::with_capacity(n);
        let mut cur_params: SmallVec<[Param; 8]> = SmallVec::new();
        let mut take = 0usize;

        for it in items {
            match it.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((expr, p)) => {
                    cur_vals.push(expr);
                    cur_params.extend(p);
                    take += 1;

                    if take == n {
                        self.rows.push(InsertRowNode::new(
                            core::mem::take(&mut cur_vals),
                            core::mem::take(&mut cur_params),
                        ));
                        take = 0;
                    }
                }
                Err(e) => {
                    self.push_builder_error(format!("insert(): {e}"));
                    return self;
                }
            }
        }

        self
    }

    /// Выполнить INSERT **без** `RETURNING`. Возвращает `rows_affected`.
    pub async fn exec(mut self) -> ExecResult<u64> {
        let (sql, params) = self.render_sql().map_err(ExecError::from)?;
        self.exec_ctx.execute(&sql, params).await
    }

    pub fn exec_send(
        mut self,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<u64>> + Send + 'static> {
        let (sql, params) = self.render_sql().map_err(ExecError::from)?;
        let ctx = self.exec_ctx.clone();
        ctx.execute_send(sql, params)
    }

    // pub fn into_send<R>(
    //     mut self,
    // ) -> ExecResult<impl core::future::Future<Output = ExecResult<Vec<R>>> + Send + 'static>
    // where
    //     R: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
    // {
    //     if self.returning.is_empty() {
    //         return Err(ExecError::Unsupported(
    //             "INSERT без RETURNING: используйте .exec(). Для чтения результатов добавьте .returning(...).".into()
    //         ));
    //     }

    //     if self.dialect == Dialect::MySQL {
    //         return Err(ExecError::Unsupported(
    //             "MySQL не поддерживает INSERT ... RETURNING; выполните .exec() и, при необходимости, отдельный SELECT."
    //                 .into(),
    //         ));
    //     }

    //     let (sql, params) = self.render_sql().map_err(ExecError::from)?;
    //     let ctx = self.exec_ctx.clone();
    //     ctx.select_send::<R>(sql, params)
    // }

    // ===== вспомогательные =====

    /// Интерпретировать плоский список как (col, value) пары.
    fn parse_row_from_pairs(&mut self, flat: Vec<QBArg>) -> Option<InsertRowNode> {
        if flat.is_empty() {
            self.push_builder_error("insert(): empty data");
            return None;
        }
        if flat.len() % 2 != 0 {
            self.push_builder_error("insert(): expected pairs (col, value)");
            return None;
        }

        let mut col_names: SmallVec<[Ident; 8]> = SmallVec::new();
        let mut values: SmallVec<[SqlExpr; 8]> = SmallVec::new();
        let mut params: SmallVec<[Param; 8]> = SmallVec::new();

        let mut it = flat.into_iter();
        while let Some(col_arg) = it.next() {
            let Some(val_arg) = it.next() else {
                self.push_builder_error("insert(): broken (col, value) pair");
                return None;
            };

            // колонка: только идентификатор (одиночный или составной) — берём последний сегмент
            let Ok((col_expr, _)) = col_arg.try_into_expr() else {
                self.push_builder_error(
                    "insert(): column name must be identifier/str/expression-ident",
                );
                return None;
            };
            match expr_last_ident(col_expr) {
                Ok(id) => col_names.push(id),
                Err(_) => {
                    self.push_builder_error("insert(): column must be identifier");
                    return None;
                }
            }

            // значение — любое выражение/подзапрос
            match val_arg.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((expr, p)) => {
                    values.push(expr);
                    params.extend(p);
                }
                Err(e) => {
                    self.push_builder_error(format!("insert(): value build failed: {e}"));
                    return None;
                }
            }
        }

        // если columns ещё не задан — зафиксируем «эталон» колонок из пары
        if self.columns.is_empty() {
            self.columns = col_names;
        } else if self.columns.len() != col_names.len()
            || self
                .columns
                .iter()
                .zip(col_names.iter())
                .any(|(a, b)| a.value != b.value)
        {
            self.push_builder_error("insert(): columns mismatch with previously defined columns");
            return None;
        }

        Some(InsertRowNode::new(values, params))
    }

    #[inline]
    pub(crate) fn push_builder_error<S: Into<std::borrow::Cow<'static, str>>>(&mut self, msg: S) {
        self.builder_errors.push(msg.into());
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    /// Начать INSERT сразу с данными (таблицу можно указать потом через .into())
    pub fn insert<L>(self, row_or_values: L) -> InsertBuilder<'a, T>
    where
        L: ArgList<'a>,
    {
        InsertBuilder::from_qb(self).insert(row_or_values)
    }

    /// Начать INSERT с указанием таблицы (данные можно передать потом через .insert(...))
    pub fn into<L>(self, table_arg: L) -> InsertBuilder<'a, T>
    where
        L: ArgList<'a>,
    {
        InsertBuilder::from_qb(self).into(table_arg)
    }
}

impl<'a, T> std::future::IntoFuture for InsertBuilder<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = ExecResult<Vec<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // Без RETURNING «ожидать» нельзя — используйте .exec()
            if self.returning.is_empty() {
                return Err(ExecError::Unsupported(
                    "INSERT без RETURNING: используйте .exec(). Для чтения результатов добавьте .returning(...).".into()
                ));
            }
            let (sql, params) = self.render_sql().map_err(ExecError::from)?;

            match self.exec_ctx {
                ExecCtx::None => Err(ExecError::MissingConnection),

                // ---- исполнение через пул ----
                ExecCtx::Pool(pool) => match pool {
                    #[cfg(feature = "postgres")]
                    DbPool::Postgres(p) => fetch_typed_pg::<T>(&p, &sql, params)
                        .await
                        .map_err(Into::into),
                    #[cfg(feature = "mysql")]
                    DbPool::MySql(p) => {
                        // В MySQL UPDATE … RETURNING нет — отдаём понятную ошибку
                        Err(ExecError::Unsupported(
                            "MySQL не поддерживает INSERT ... RETURNING; выполните .exec() и, при необходимости, отдельный SELECT."
                                .into(),
                        ))
                    }
                    #[cfg(feature = "sqlite")]
                    DbPool::Sqlite(p) => fetch_typed_sqlite::<T>(&p, &sql, params)
                        .await
                        .map_err(Into::into),
                },

                // ---- исполнение ВНУТРИ транзакции ----
                #[cfg(feature = "postgres")]
                ExecCtx::PgConn(conn) => fetch_typed_pg_exec::<_, T>(conn, &sql, params).await,
                #[cfg(feature = "mysql")]
                ExecCtx::MySqlConn(conn) => {
                    // В MySQL UPDATE … RETURNING нет — отдаём понятную ошибку
                    Err(ExecError::Unsupported(
                        "MySQL не поддерживает INSERT ... RETURNING; выполните .exec() и, при необходимости, отдельный SELECT."
                            .into(),
                    ))
                }
                #[cfg(feature = "sqlite")]
                ExecCtx::SqliteConn(conn) => {
                    fetch_typed_sqlite_exec::<_, T>(conn, &sql, params).await
                }
            }
        })
    }
}
