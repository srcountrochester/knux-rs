use std::marker::PhantomData;
use std::pin::Pin;

use crate::executor::{DbPool, DbRow, Error as ExecError, Result as ExecResult};
use crate::query_builder::args::{ArgList, QBArg};
use crate::query_builder::ast::FromItem;
use crate::query_builder::{ExecCtx, QueryBuilder};
use crate::renderer::Dialect;
use crate::{param::Param, utils::expr_to_object_name};
use smallvec::{SmallVec, smallvec};
use sqlparser::ast::{Expr as SqlExpr, ObjectName, SelectItem, SqliteOnConflict};

use super::set::parse_assignments_pairs;

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

/// Билдер UPDATE ... SET ... [WHERE ...] [RETURNING ...]
#[derive(Debug)]
pub struct UpdateBuilder<'a, T> {
    pub(crate) table: Option<ObjectName>,
    pub(crate) set: SmallVec<[super::set::Assignment; 8]>,
    pub(crate) where_predicate: Option<SqlExpr>,
    pub(crate) params: SmallVec<[Param; 8]>,
    pub(crate) returning: SmallVec<[SelectItem; 4]>,
    pub(crate) from_items: SmallVec<[FromItem<'a>; 2]>,
    pub(crate) sqlite_or: Option<SqliteOnConflict>,

    // ошибки сбора
    pub(crate) builder_errors: SmallVec<[std::borrow::Cow<'static, str>; 2]>,

    // контекст
    pub(crate) default_schema: Option<String>,
    pub(crate) dialect: Dialect,
    pub(crate) exec_ctx: ExecCtx<'a>,
    _t: PhantomData<T>,
}

impl<'a, T> UpdateBuilder<'a, T> {
    #[inline]
    pub(crate) fn from_qb(qb: QueryBuilder<'a, T>) -> Self {
        Self {
            table: None,
            set: smallvec![],
            where_predicate: None,
            params: qb.params,
            returning: smallvec![],
            builder_errors: smallvec![],
            default_schema: qb.default_schema,
            dialect: qb.dialect,

            from_items: smallvec![],
            sqlite_or: None,
            exec_ctx: qb.exec_ctx,
            _t: PhantomData,
        }
    }

    /// SET (col1, val1, col2, val2, ...)
    pub fn set<L>(mut self, assignments: L) -> Self
    where
        L: ArgList<'a>,
    {
        let flat = assignments.into_vec();
        match parse_assignments_pairs(&mut self.params, flat) {
            Ok(items) => self.set.extend(items),
            Err(msg) => self.push_builder_error(msg),
        }
        self
    }

    /// WHERE <expr>[, <expr2>, ...] — элементы связываются AND
    pub fn r#where<A>(mut self, args: A) -> Self
    where
        A: ArgList<'a>,
    {
        match self.resolve_where_group(args) {
            Ok(Some((expr, params))) => {
                self.attach_where_with_and(expr, params);
            }
            Ok(None) => {} // пустой список — игнорируем
            Err(msg) => self.push_builder_error(msg),
        }
        self
    }

    /// WHERE <expr>[, <expr2>, ...] — элементы связываются AND
    pub fn where_<A>(self, args: A) -> Self
    where
        A: ArgList<'a>,
    {
        self.r#where(args)
    }

    /// RETURNING <expr, ...> (PG/SQLite; в MySQL будет проигнорировано на рендере)
    pub fn returning<L>(mut self, items: L) -> Self
    where
        L: ArgList<'a>,
    {
        if let Err(msg) = super::returning::push_returning_list(&mut self.returning, items) {
            self.push_builder_error(msg);
        }
        self
    }

    /// RETURNING один элемент, перезаписывает предыдущий список
    pub fn returning_one<L>(mut self, item: L) -> Self
    where
        L: ArgList<'a>,
    {
        if let Err(msg) = super::returning::set_returning_one(&mut self.returning, item) {
            self.push_builder_error(msg);
        }
        self
    }

    /// RETURNING *
    pub fn returning_all(mut self) -> Self {
        super::returning::set_returning_all(&mut self.returning);
        self
    }

    /// RETURNING <qualifier>.*
    pub fn returning_all_from(mut self, qualifier: &str) -> Self {
        super::returning::set_returning_all_from(&mut self.returning, qualifier);
        self
    }

    /// FROM <tables | (subquery)> — как в QueryBuilder::from(...)
    pub fn from<L>(mut self, items: L) -> Self
    where
        L: ArgList<'a>,
    {
        let args = items.into_vec();
        self.from_items.reserve(args.len());

        for arg in args {
            match arg {
                // Имя таблицы из Expr (col/ident/строка)
                QBArg::Expr(e) => {
                    let mut p = e.params;
                    if !p.is_empty() {
                        self.params.append(&mut p);
                    }
                    if let Some(name) = expr_to_object_name(e.expr, self.default_schema.as_deref())
                    {
                        self.from_items.push(FromItem::TableName(name));
                    } else {
                        self.push_builder_error("from(): invalid table reference");
                    }
                }
                // Подзапрос (как есть)
                QBArg::Subquery(qb) => self.from_items.push(FromItem::Subquery(Box::new(qb))),
                // Замыкание-подзапрос
                QBArg::Closure(c) => self.from_items.push(FromItem::SubqueryClosure(c)),
            }
        }
        self
    }

    /// Выполнить UPDATE без RETURNING и вернуть rows_affected.
    pub async fn exec(mut self) -> ExecResult<u64> {
        let (sql, params) = self.render_sql().map_err(ExecError::from)?;
        let exec_ctx = self.exec_ctx;

        match exec_ctx {
            ExecCtx::None => Err(ExecError::MissingConnection),

            ExecCtx::Pool(pool) => match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => crate::executor::utils::execute_pg(&p, &sql, params).await,
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => crate::executor::utils::execute_mysql(&p, &sql, params).await,
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => crate::executor::utils::execute_sqlite(&p, &sql, params).await,
            },

            #[cfg(feature = "postgres")]
            ExecCtx::PgConn(conn) => {
                crate::executor::transaction_utils::execute_pg_exec(conn, &sql, params).await
            }
            #[cfg(feature = "mysql")]
            ExecCtx::MySqlConn(conn) => {
                crate::executor::transaction_utils::execute_mysql_exec(conn, &sql, params).await
            }
            #[cfg(feature = "sqlite")]
            ExecCtx::SqliteConn(conn) => {
                crate::executor::transaction_utils::execute_sqlite_exec(conn, &sql, params).await
            }
        }
    }

    /// SQLite: UPDATE OR REPLACE ...
    #[inline]
    pub fn or_replace(mut self) -> Self {
        self.sqlite_or = Some(SqliteOnConflict::Replace);
        self
    }

    /// SQLite: UPDATE OR IGNORE ...
    #[inline]
    pub fn or_ignore(mut self) -> Self {
        self.sqlite_or = Some(SqliteOnConflict::Ignore);
        self
    }

    // ====== вспомогательные ======

    #[inline]
    fn attach_where_with_and(&mut self, expr: SqlExpr, mut params: SmallVec<[Param; 8]>) {
        if let Some(prev) = self.where_predicate.take() {
            self.where_predicate = Some(SqlExpr::BinaryOp {
                left: Box::new(prev),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(expr),
            });
        } else {
            self.where_predicate = Some(expr);
        }
        if !params.is_empty() {
            self.params.extend(params.drain(..));
        }
    }

    /// Собрать WHERE-группу из ArgList, разрешая подзапросы
    fn resolve_where_group<A>(
        &mut self,
        args: A,
    ) -> Result<Option<(SqlExpr, SmallVec<[Param; 8]>)>, std::borrow::Cow<'static, str>>
    where
        A: ArgList<'a>,
    {
        let items = args.into_vec();
        if items.is_empty() {
            return Ok(None);
        }

        let mut exprs: SmallVec<[SqlExpr; 4]> = SmallVec::with_capacity(items.len());
        let mut params: SmallVec<[Param; 8]> = SmallVec::new();

        for it in items {
            match it.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((e, p)) => {
                    exprs.push(e);
                    if !p.is_empty() {
                        params.extend(p);
                    }
                }
                Err(e) => return Err(format!("where(): {e}").into()),
            }
        }

        // Склеиваем через AND
        let mut it = exprs.into_iter();
        let Some(mut acc) = it.next() else {
            return Ok(None);
        };
        for e in it {
            acc = SqlExpr::BinaryOp {
                left: Box::new(acc),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(e),
            };
        }
        Ok(Some((acc, params)))
    }

    #[inline]
    pub(crate) fn push_builder_error<S: Into<std::borrow::Cow<'static, str>>>(&mut self, msg: S) {
        self.builder_errors.push(msg.into());
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    /// Начать UPDATE с указанием таблицы (поддерживает выражения: table("users").schema("public"))
    pub fn update<L>(self, table_arg: L) -> UpdateBuilder<'a, T>
    where
        L: ArgList<'a>,
    {
        let mut b = UpdateBuilder::from_qb(self);

        let mut args = table_arg.into_vec();
        if args.is_empty() {
            b.push_builder_error("update(): table is not set");
            return b;
        }
        if args.len() > 1 {
            b.push_builder_error("update(): expected a single table argument");
        }

        // Берём первый аргумент и пробуем интерпретировать как имя таблицы
        match args.swap_remove(0).try_into_expr() {
            Ok((expr, _params)) => {
                if let Some(obj) = expr_to_object_name(expr, b.default_schema.as_deref()) {
                    b.table = Some(obj);
                } else {
                    b.push_builder_error(
                        "update(): invalid table reference; expected identifier or schema.table",
                    );
                }
            }
            Err(e) => b.push_builder_error(format!("update(): {e}")),
        }

        b
    }
}

impl<'a, T> std::future::IntoFuture for UpdateBuilder<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = ExecResult<Vec<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

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
                    DbPool::MySql(p) => {
                        // В MySQL UPDATE … RETURNING нет — отдаём понятную ошибку
                        Err(ExecError::Unsupported(
                            "MySQL does not support UPDATE ... RETURNING; use .exec() then SELECT"
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
                        "MySQL does not support UPDATE ... RETURNING; use .exec() then SELECT"
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
