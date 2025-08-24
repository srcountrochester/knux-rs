use std::{marker::PhantomData, pin::Pin};

use crate::{
    executor::{DbPool, DbRow, Error as ExecError, Result as ExecResult, utils},
    param::Param,
};

pub struct SendQuery<T> {
    pool: DbPool,
    sql: String,
    params: Vec<Param>,
    _t: PhantomData<T>,
}

impl<T> std::future::IntoFuture for SendQuery<T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'static,
{
    type Output = ExecResult<Vec<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'static>>;

    fn into_future(self) -> Self::IntoFuture {
        let SendQuery {
            pool, sql, params, ..
        } = self;
        Box::pin(async move {
            match pool {
                #[cfg(feature = "postgres")]
                DbPool::Postgres(p) => utils::fetch_typed_pg::<T>(&p, &sql, params).await,
                #[cfg(feature = "mysql")]
                DbPool::MySql(p) => utils::fetch_typed_mysql::<T>(&p, &sql, params).await,
                #[cfg(feature = "sqlite")]
                DbPool::Sqlite(p) => utils::fetch_typed_sqlite::<T>(&p, &sql, params).await,
            }
            .map_err(Into::into)
        })
    }
}

impl<'a, T> super::PoolQuery<'a, T> {
    pub fn into_send(mut self) -> ExecResult<SendQuery<T>> {
        use crate::query_builder::ExecCtx;
        let (sql, params) = self.0.render_sql()?;
        let pool = match self.0.exec_ctx {
            ExecCtx::Pool(p) => p,
            _ => return Err(ExecError::MissingConnection),
        };
        Ok(SendQuery {
            pool,
            sql,
            params,
            _t: PhantomData,
        })
    }
}
