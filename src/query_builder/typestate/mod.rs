use std::pin::Pin;

use crate::executor::{DbRow, Result as ExecResult};

mod pool_query;
mod send_exec;
mod send_query;
mod tx_query;

pub use pool_query::PoolQuery;
pub use tx_query::TxQuery;

impl<'a, T> std::future::IntoFuture for PoolQuery<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = ExecResult<Vec<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        self.0.into_future()
    }
}

impl<'a, T> std::future::IntoFuture for TxQuery<'a, T>
where
    T: for<'r> sqlx::FromRow<'r, DbRow> + Send + Unpin + 'a,
{
    type Output = ExecResult<Vec<T>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + 'a>>;

    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        self.0.into_future()
    }
}
