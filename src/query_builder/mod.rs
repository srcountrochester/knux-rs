use std::{borrow::Cow, marker::PhantomData};

use crate::{
    executor::{DbPool, DbRow},
    param::Param,
    renderer::Dialect,
};
use smallvec::{SmallVec, smallvec};

mod __tests__;
mod alias;
mod args;
mod ast;
mod clear;
mod delete;
mod distinct;
mod error;
mod exec_ctx;
mod from;
mod group_by;
mod having;
pub mod insert;
mod join;
mod limit;
mod order_by;
mod schema;
mod select;
mod sql;
mod union;
mod update;
mod where_clause;
mod with;

use ast::FromItem;
use distinct::DistinctOnNode;
pub use error::{BuilderErrorList, Error, Result};
pub use exec_ctx::ExecCtx;
use group_by::GroupByNode;
use having::HavingNode;
pub use insert::InsertBuilder;
use join::JoinNode;
use order_by::OrderByNode;
use select::SelectItemNode;
use union::SetOpNode;
use where_clause::WhereNode;
use with::WithItemNode;

pub use join::on;

#[cfg(feature = "postgres")]
const DEFAULT_DIALECT: Dialect = Dialect::Postgres;
#[cfg(feature = "mysql")]
const DEFAULT_DIALECT: Dialect = Dialect::MySQL;
#[cfg(feature = "sqlite")]
const DEFAULT_DIALECT: Dialect = Dialect::SQLite;

pub struct QueryOne<'a, T>(pub(super) QueryBuilder<'a, T>);
pub struct QueryOptional<'a, T>(pub(super) QueryBuilder<'a, T>);

#[derive(Debug, Clone)]
pub struct QueryBuilder<'a, T = ()> {
    pub(self) select_items: SmallVec<[SelectItemNode; 4]>,
    pub(self) from_items: SmallVec<[FromItem<'a>; 1]>,
    pub(self) where_clause: Option<WhereNode>,
    pub params: SmallVec<[Param; 8]>,
    pub default_schema: Option<String>,
    pub(crate) pending_schema: Option<String>,
    pub alias: Option<String>,
    pub(crate) dialect: Dialect,
    builder_errors: SmallVec<[Cow<'static, str>; 2]>,
    pub(self) from_joins: SmallVec<[SmallVec<[JoinNode; 2]>; 1]>,
    pub(self) group_by_items: SmallVec<[GroupByNode; 4]>,
    pub(self) order_by_items: SmallVec<[OrderByNode; 4]>,
    pub(self) limit_num: Option<u64>,
    pub(self) offset_num: Option<u64>,
    pub(self) having_clause: Option<HavingNode>,
    pub(self) select_distinct: bool,
    pub(self) distinct_on_items: SmallVec<[DistinctOnNode; 2]>,
    pub(self) with_items: SmallVec<[WithItemNode; 1]>,
    pub(self) with_recursive: bool,
    pub(self) set_ops: SmallVec<[SetOpNode; 1]>,
    pub(crate) exec_ctx: ExecCtx<'a>,
    _t: PhantomData<T>,
}

impl<'a, T> QueryBuilder<'a, T> {
    #[inline]
    pub fn new_pool(pool: DbPool, schema: Option<String>) -> Self {
        Self {
            select_items: smallvec![],
            from_items: smallvec![],
            where_clause: None,
            params: smallvec![],
            builder_errors: smallvec![],
            default_schema: schema,
            pending_schema: None,
            from_joins: smallvec![],
            group_by_items: smallvec![],
            order_by_items: smallvec![],
            having_clause: None,
            alias: None,
            dialect: DEFAULT_DIALECT,
            limit_num: None,
            offset_num: None,
            select_distinct: false,
            distinct_on_items: smallvec![],
            with_items: smallvec![],
            with_recursive: false,
            set_ops: smallvec![],
            exec_ctx: ExecCtx::Pool(pool),
            _t: PhantomData,
        }
    }

    #[inline]
    pub fn new_tx(schema: Option<String>, exec_ctx: ExecCtx<'a>) -> Self {
        Self {
            select_items: smallvec![],
            from_items: smallvec![],
            where_clause: None,
            params: smallvec![],
            builder_errors: smallvec![],
            default_schema: schema,
            pending_schema: None,
            from_joins: smallvec![],
            group_by_items: smallvec![],
            order_by_items: smallvec![],
            having_clause: None,
            alias: None,
            dialect: DEFAULT_DIALECT,
            limit_num: None,
            offset_num: None,
            select_distinct: false,
            distinct_on_items: smallvec![],
            with_items: smallvec![],
            with_recursive: false,
            set_ops: smallvec![],
            exec_ctx,
            _t: PhantomData,
        }
    }

    /// Пустой QueryBuilder без пула — удобно для замыканий |qb| qb.select(...)
    pub fn new_empty() -> Self {
        Self {
            select_items: smallvec![],
            from_items: smallvec![],
            where_clause: None,
            params: smallvec![],
            builder_errors: smallvec![],
            from_joins: smallvec![],
            default_schema: None,
            pending_schema: None,
            group_by_items: smallvec![],
            order_by_items: smallvec![],
            having_clause: None,
            alias: None,
            dialect: DEFAULT_DIALECT,
            limit_num: None,
            offset_num: None,
            select_distinct: false,
            distinct_on_items: smallvec![],
            with_items: smallvec![],
            with_recursive: false,
            set_ops: smallvec![],
            exec_ctx: ExecCtx::None,
            _t: PhantomData,
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

    #[inline]
    pub(crate) fn push_builder_error<S: Into<Cow<'static, str>>>(&mut self, msg: S) {
        self.builder_errors.push(msg.into());
    }

    /// Быстрая проверка наличия ошибок билдера.
    #[inline]
    pub fn has_builder_errors(&self) -> bool {
        !self.builder_errors.is_empty()
    }

    #[inline]
    pub fn dialect(mut self, dialect: Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    #[inline]
    pub fn one(self) -> QueryOne<'a, T> {
        QueryOne(self)
    }

    #[inline]
    pub fn optional(self) -> QueryOptional<'a, T> {
        QueryOptional(self)
    }

    #[inline]
    fn take_builder_error_list(&mut self) -> Option<BuilderErrorList> {
        if self.builder_errors.is_empty() {
            None
        } else {
            // Передаём SmallVec в From — он сконвертит в BuilderErrorList
            Some(BuilderErrorList::from(std::mem::take(
                &mut self.builder_errors,
            )))
        }
    }

    #[inline]
    fn is_mysql(&self) -> bool {
        self.dialect == crate::renderer::Dialect::MySQL
    }
}

impl<'a, T> Default for QueryBuilder<'a, T> {
    fn default() -> Self {
        Self::new_empty()
    }
}

use crate::executor::{Error as ExecError, Result as ExecResult};

#[cfg(feature = "mysql")]
use crate::executor::utils::{
    fetch_one_typed_mysql, fetch_optional_typed_mysql, fetch_typed_mysql,
};
#[cfg(feature = "postgres")]
use crate::executor::utils::{fetch_one_typed_pg, fetch_optional_typed_pg, fetch_typed_pg};
#[cfg(feature = "sqlite")]
use crate::executor::utils::{
    fetch_one_typed_sqlite, fetch_optional_typed_sqlite, fetch_typed_sqlite,
};

#[cfg(feature = "mysql")]
use crate::executor::transaction_utils::{
    fetch_typed_mysql_exec, /* fetch_one_typed_mysql_exec, fetch_optional_typed_mysql_exec */
};
#[cfg(feature = "postgres")]
use crate::executor::transaction_utils::{
    fetch_typed_pg_exec, /* fetch_one_typed_pg_exec, fetch_optional_typed_pg_exec */
};
#[cfg(feature = "sqlite")]
use crate::executor::transaction_utils::{
    fetch_typed_sqlite_exec, /* fetch_one_typed_sqlite_exec, fetch_optional_typed_sqlite_exec */
};

use std::{future::Future, pin::Pin};

// === Vec<T> ===
impl<'a, T> std::future::IntoFuture for QueryBuilder<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = crate::executor::Result<Vec<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
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
                    DbPool::MySql(p) => fetch_typed_mysql::<T>(&p, &sql, params)
                        .await
                        .map_err(Into::into),
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
                    fetch_typed_mysql_exec::<_, T>(conn, &sql, params).await
                }
                #[cfg(feature = "sqlite")]
                ExecCtx::SqliteConn(conn) => {
                    fetch_typed_sqlite_exec::<_, T>(conn, &sql, params).await
                }
            }
        })
    }
}

// === one() ===
impl<'a, T> std::future::IntoFuture for QueryOne<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = crate::executor::Result<T>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let (sql, params) = self.0.render_sql()?;
            match self.0.exec_ctx {
                ExecCtx::None => Err(ExecError::MissingConnection),

                ExecCtx::Pool(pool) => match pool {
                    #[cfg(feature = "postgres")]
                    DbPool::Postgres(p) => fetch_one_typed_pg(&p, &sql, params).await,
                    #[cfg(feature = "mysql")]
                    DbPool::MySql(p) => fetch_one_typed_mysql(&p, &sql, params).await,
                    #[cfg(feature = "sqlite")]
                    DbPool::Sqlite(p) => fetch_one_typed_sqlite(&p, &sql, params).await,
                },

                #[cfg(feature = "postgres")]
                ExecCtx::PgConn(conn) => {
                    let rows = fetch_typed_pg_exec::<_, T>(conn, &sql, params).await?;
                    rows.into_iter().next().ok_or(ExecError::NotFound)
                }
                #[cfg(feature = "mysql")]
                ExecCtx::MySqlConn(conn) => {
                    let rows = fetch_typed_mysql_exec::<_, T>(conn, &sql, params).await?;
                    rows.into_iter().next().ok_or(ExecError::NotFound)
                }
                #[cfg(feature = "sqlite")]
                ExecCtx::SqliteConn(conn) => {
                    let rows = fetch_typed_sqlite_exec::<_, T>(conn, &sql, params).await?;
                    rows.into_iter().next().ok_or(ExecError::NotFound)
                }
            }
        })
    }
}

// === optional() ===
impl<'a, T> std::future::IntoFuture for QueryOptional<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = crate::executor::Result<Option<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let (sql, params) = self.0.render_sql()?;
            match self.0.exec_ctx {
                ExecCtx::None => Err(ExecError::MissingConnection),

                ExecCtx::Pool(pool) => match pool {
                    #[cfg(feature = "postgres")]
                    DbPool::Postgres(p) => fetch_optional_typed_pg(&p, &sql, params).await,
                    #[cfg(feature = "mysql")]
                    DbPool::MySql(p) => fetch_optional_typed_mysql(&p, &sql, params).await,
                    #[cfg(feature = "sqlite")]
                    DbPool::Sqlite(p) => fetch_optional_typed_sqlite(&p, &sql, params).await,
                },

                #[cfg(feature = "postgres")]
                ExecCtx::PgConn(conn) => {
                    let rows = fetch_typed_pg_exec::<_, T>(conn, &sql, params).await?;
                    Ok(rows.into_iter().next())
                }
                #[cfg(feature = "mysql")]
                ExecCtx::MySqlConn(conn) => {
                    let rows = fetch_typed_mysql_exec::<_, T>(conn, &sql, params).await?;
                    Ok(rows.into_iter().next())
                }
                #[cfg(feature = "sqlite")]
                ExecCtx::SqliteConn(conn) => {
                    let rows = fetch_typed_sqlite_exec::<_, T>(conn, &sql, params).await?;
                    Ok(rows.into_iter().next())
                }
            }
        })
    }
}

impl<'a, T> QueryBuilder<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
{
    /// Future, пригодный для tokio::spawn (Send + 'static). Только для ExecCtx::Pool.
    pub fn into_send(
        mut self,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<Vec<T>>> + Send + 'static>
    where
        T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
    {
        let (sql, params) = self.render_sql()?;
        // перемещаем контекст (если нельзя — возьмите clone()):
        let ctx = self.exec_ctx.clone(); // ExecCtx: Clone у вас уже есть
        ctx.select_send::<T>(sql, params)
    }

    pub fn one_send(
        mut self,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<T>> + Send + 'static>
    where
        T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
    {
        let (sql, params) = self.render_sql()?;
        let ctx = self.exec_ctx.clone();
        ctx.select_one_send::<T>(sql, params)
    }

    pub fn optional_send(
        mut self,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<Option<T>>> + Send + 'static>
    where
        T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
    {
        let (sql, params) = self.render_sql()?;
        let ctx = self.exec_ctx.clone();
        ctx.select_optional_send::<T>(sql, params)
    }
}
