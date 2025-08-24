use std::marker::PhantomData;
use std::pin::Pin;

use crate::executor::{DbPool, DbRow, Error as ExecError, Result as ExecResult};
use crate::param::Param;
use crate::query_builder::args::{ArgList, QBArg};
use crate::query_builder::ast::FromItem;
use crate::query_builder::{ExecCtx, QueryBuilder};
use crate::renderer::Dialect;
use crate::utils::expr_to_object_name;
use smallvec::{SmallVec, smallvec};
use sqlparser::ast::{Expr as SqlExpr, ObjectName, SelectItem};

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

/// Билдер DELETE FROM ... [USING ...] [WHERE ...] [RETURNING ...]
#[derive(Debug, Clone)]
pub struct DeleteBuilder<'a, T> {
    pub(crate) table: Option<ObjectName>,
    pub(crate) using_items: SmallVec<[FromItem<'a>; 2]>,
    pub(crate) where_predicate: Option<SqlExpr>,
    pub(crate) returning: SmallVec<[SelectItem; 4]>,
    pub(crate) params: SmallVec<[Param; 8]>,

    // ошибки сбора
    pub(crate) builder_errors: SmallVec<[std::borrow::Cow<'static, str>; 2]>,

    // контекст
    pub(crate) default_schema: Option<String>,
    pub(crate) dialect: Dialect,
    pub(crate) exec_ctx: ExecCtx<'a>,
    _t: PhantomData<T>,
}

impl<'a, T> DeleteBuilder<'a, T> {
    #[inline]
    pub(crate) fn from_qb(qb: QueryBuilder<'a, T>) -> Self {
        Self {
            table: None,
            using_items: smallvec![],
            where_predicate: None,
            returning: smallvec![],
            params: qb.params,
            builder_errors: smallvec![],
            default_schema: qb.default_schema,
            dialect: qb.dialect,
            exec_ctx: qb.exec_ctx,
            _t: PhantomData,
        }
    }

    /// USING <tables...> — дополнительные таблицы (PG/MySQL)
    pub fn using<L>(mut self, items: L) -> Self
    where
        L: ArgList<'a>,
    {
        let args = items.into_vec();
        self.using_items.reserve(args.len());
        for arg in args {
            self.push_using_item(arg);
        }
        self
    }

    /// WHERE <expr>[, <expr2>, ...] — элементы связываются AND
    pub fn r#where<A>(mut self, args: A) -> Self
    where
        A: ArgList<'a>,
    {
        match self.resolve_where_group(args) {
            Ok(Some((expr, params))) => self.attach_where_with_and(expr, params),
            Ok(None) => {}
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

    /// Выполнить DELETE **без** RETURNING. Возвращает rows_affected.
    pub async fn exec(mut self) -> ExecResult<u64> {
        if !self.returning.is_empty() {
            return Err(ExecError::Unsupported(
                "DELETE c RETURNING: используйте `.await` (IntoFuture), а не `.exec()`.".into(),
            ));
        }

        let (sql, params) = self.render_sql().map_err(ExecError::from)?;
        self.exec_ctx.execute(&sql, params).await
    }

    pub fn exec_send(
        mut self,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<u64>> + Send + 'static> {
        if !self.returning.is_empty() {
            return Err(ExecError::Unsupported(
                "DELETE c RETURNING: используйте `.await` (IntoFuture), а не `.exec()`.".into(),
            ));
        }

        if self.dialect == Dialect::MySQL {
            return Err(ExecError::Unsupported(
                "MySQL не поддерживает DELETE ... RETURNING; выполните .exec() и, при необходимости, отдельный SELECT."
                    .into(),
            ));
        }

        let (sql, params) = self.render_sql().map_err(ExecError::from)?;
        let ctx = self.exec_ctx.clone();
        ctx.execute_send(sql, params)
    }

    pub fn into_send<R>(
        mut self,
    ) -> ExecResult<impl core::future::Future<Output = ExecResult<Vec<R>>> + Send + 'static>
    where
        R: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
    {
        if self.returning.is_empty() {
            return Err(ExecError::Unsupported(
                "DELETE without RETURNING: use `.exec()` instead.".into(),
            ));
        }

        let (sql, params) = self.render_sql().map_err(ExecError::from)?;
        let ctx = self.exec_ctx.clone();
        ctx.select_send::<R>(sql, params)
    }

    // ===== helpers =====

    #[inline]
    fn attach_where_with_and(&mut self, expr: SqlExpr, params: SmallVec<[Param; 8]>) {
        self.where_predicate = Some(match self.where_predicate.take() {
            Some(prev) => SqlExpr::BinaryOp {
                left: Box::new(prev),
                op: sqlparser::ast::BinaryOperator::And,
                right: Box::new(expr),
            },
            None => expr,
        });
        if !params.is_empty() {
            self.params.extend(params);
        }
    }

    /// Собрать WHERE-группу из ArgList (разрешая подзапросы)
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

        let mut acc: Option<SqlExpr> = None;
        let mut params: SmallVec<[Param; 8]> = SmallVec::new();

        for it in items {
            match it.resolve_into_expr_with(|qb| qb.build_query_ast()) {
                Ok((e, p)) => {
                    if !p.is_empty() {
                        params.extend(p);
                    }
                    acc = Some(match acc {
                        Some(prev) => SqlExpr::BinaryOp {
                            left: Box::new(prev),
                            op: sqlparser::ast::BinaryOperator::And,
                            right: Box::new(e),
                        },
                        None => e,
                    });
                }
                Err(e) => return Err(format!("where(): {e}").into()),
            }
        }

        Ok(acc.map(|e| (e, params)))
    }

    #[inline]
    pub(crate) fn push_builder_error<S: Into<std::borrow::Cow<'static, str>>>(&mut self, msg: S) {
        self.builder_errors.push(msg.into());
    }

    #[inline]
    fn push_using_item(&mut self, arg: QBArg) {
        match arg {
            // Имя таблицы из Expr (col/ident/строка)
            QBArg::Expr(e) => {
                let mut p = e.params;
                if !p.is_empty() {
                    self.params.append(&mut p);
                }
                if let Some(name) = expr_to_object_name(e.expr, self.default_schema.as_deref()) {
                    self.using_items.push(FromItem::TableName(name));
                } else {
                    self.push_builder_error("using(): invalid table reference");
                }
            }
            // Подзапросы пока не поддерживаем (как и в update.from())
            QBArg::Subquery(_) | QBArg::Closure(_) => {
                self.push_builder_error("using(): subqueries are not supported yet");
            }
        }
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    /// Начать DELETE с указанием таблицы (поддерживает выражения: table("t").schema("s"))
    pub fn delete<L>(self, table_arg: L) -> DeleteBuilder<'a, T>
    where
        L: ArgList<'a>,
    {
        let mut b = DeleteBuilder::from_qb(self);

        let args = table_arg.into_vec();
        if args.is_empty() {
            b.push_builder_error("delete(): table is not set");
            return b;
        }
        if args.len() > 1 {
            b.push_builder_error("delete(): expected a single table argument");
        }

        // Берём первый аргумент и пробуем интерпретировать как имя таблицы
        let first = args.into_iter().next().unwrap(); // safe: уже проверили is_empty()
        match first.try_into_expr() {
            Ok((expr, _params)) => {
                if let Some(obj) = expr_to_object_name(expr, b.default_schema.as_deref()) {
                    b.table = Some(obj);
                } else {
                    b.push_builder_error(
                        "delete(): invalid table reference; expected identifier or schema.table",
                    );
                }
            }
            Err(e) => b.push_builder_error(format!("delete(): {e}")),
        }

        b
    }
}

impl<'a, T> std::future::IntoFuture for DeleteBuilder<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = ExecResult<Vec<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // Без RETURNING – просим использовать exec()
            if self.returning.is_empty() {
                return Err(ExecError::Unsupported(
                    "DELETE без RETURNING: используйте .exec(). Для получения строк добавьте .returning(...).".into()
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
                        Err(ExecError::Unsupported(
                            "MySQL не поддерживает DELETE ... RETURNING; выполните .exec() и при необходимости отдельный SELECT."
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
                    Err(ExecError::Unsupported(
                        "MySQL не поддерживает DELETE ... RETURNING; выполните .exec() и при необходимости отдельный SELECT."
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
