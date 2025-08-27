mod __tests__;
pub mod executor;
pub mod expression;
pub mod optimizer;
pub mod param;
pub mod query_builder;
pub mod renderer;
mod runtime_ext;
mod tests;
pub mod type_helpers;
mod utils;

pub use executor::{ExecutorConfig, QueryExecutor};
pub use expression::helpers::*;
pub use query_builder::{DeleteBuilder, InsertBuilder, QueryBuilder, UpdateBuilder};
pub use runtime_ext::SpawnExt;
pub use type_helpers::QBClosureHelper;

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
compile_error!("Enable exactly one DB feature: `postgres`, `mysql`, or `sqlite`.");

#[cfg(all(feature = "postgres", any(feature = "mysql", feature = "sqlite")))]
compile_error!("Enable only one DB feature at a time (postgres vs mysql/sqlite).");

#[cfg(all(feature = "mysql", feature = "sqlite"))]
compile_error!("Enable only one DB feature at a time (mysql vs sqlite).");
