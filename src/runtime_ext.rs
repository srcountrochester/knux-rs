use std::future::Future;
use tokio::task::JoinHandle;

pub trait SpawnExt: Future + Send + 'static
where
    Self::Output: Send + 'static,
{
    fn spawn(self) -> JoinHandle<Self::Output>;
}

impl<F> SpawnExt for F
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    #[inline]
    fn spawn(self) -> JoinHandle<F::Output> {
        tokio::spawn(self)
    }
}
