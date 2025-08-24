use std::pin::Pin;

use tokio::task::JoinHandle;

use crate::{
    executor::{DbPool, DbRow, Error as ExecError, Result as ExecResult, utils},
    param::Param,
    query_builder::{
        InsertBuilder, delete::DeleteBuilder, typestate::send_query::SendQuery,
        update::UpdateBuilder,
    },
    renderer::Dialect,
};

// --- EXEC (INSERT/UPDATE/DELETE) ---
pub struct SendExec {
    pool: DbPool,
    sql: String,
    params: Vec<Param>,
}

impl std::future::IntoFuture for SendExec {
    type Output = ExecResult<u64>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'static>>;

    fn into_future(self) -> Self::IntoFuture {
        let SendExec { pool, sql, params } = self;
        Box::pin(async move {
            match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => utils::execute_pg(&p, &sql, params).await,
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => utils::execute_mysql(&p, &sql, params).await,
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => utils::execute_sqlite(&p, &sql, params).await,
            }
        })
    }
}

// Вспомогательный удобный spawn для обоих типов:
impl<T> SendQuery<T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
{
    #[inline]
    pub fn spawn(self) -> JoinHandle<ExecResult<Vec<T>>> {
        tokio::spawn(self.into_future())
    }
}
impl SendExec {
    #[inline]
    pub fn spawn(self) -> JoinHandle<ExecResult<u64>> {
        tokio::spawn(self.into_future())
    }
}

impl<'a, T> InsertBuilder<'a, T> {
    pub fn into_send(mut self) -> ExecResult<SendExec> {
        use crate::query_builder::ExecCtx;
        if self.returning.is_empty() {
            return Err(ExecError::Unsupported(
                "INSERT без RETURNING: используйте .exec(). Для чтения результатов добавьте .returning(...).".into()
            ));
        }

        if self.dialect == Dialect::MySQL {
            return Err(ExecError::Unsupported(
                "MySQL не поддерживает INSERT ... RETURNING; выполните .exec() и, при необходимости, отдельный SELECT."
                    .into(),
            ));
        }

        let (sql, params) = self.render_sql()?; // как и раньше
        let pool = match self.exec_ctx {
            ExecCtx::Pool(p) => p,
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(SendExec { pool, sql, params })
    }
}

impl<'a, T> UpdateBuilder<'a, T> {
    pub fn into_send(mut self) -> ExecResult<SendExec> {
        use crate::query_builder::ExecCtx;
        if self.returning.is_empty() {
            return Err(ExecError::Unsupported(
                "UPDATE without RETURNING: use `.exec()` instead.".into(),
            ));
        }

        if self.dialect == Dialect::MySQL {
            return Err(ExecError::Unsupported(
                "MySQL не поддерживает UPDATE ... RETURNING; выполните .exec() и, при необходимости, отдельный SELECT."
                    .into(),
            ));
        }

        let (sql, params) = self.render_sql()?; // как и раньше
        let pool = match self.exec_ctx {
            ExecCtx::Pool(p) => p,
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(SendExec { pool, sql, params })
    }
}

impl<'a, T> DeleteBuilder<'a, T> {
    pub fn into_send(mut self) -> ExecResult<SendExec> {
        use crate::query_builder::ExecCtx;
        if self.returning.is_empty() {
            return Err(ExecError::Unsupported(
                "DELETE without RETURNING: use `.exec()` instead.".into(),
            ));
        }

        let (sql, params) = self.render_sql()?; // как и раньше
        let pool = match self.exec_ctx {
            ExecCtx::Pool(p) => p,
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(SendExec { pool, sql, params })
    }
}
