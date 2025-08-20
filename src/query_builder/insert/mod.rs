mod __tests__;
mod core_fn;
mod merge;
mod on_conflict;
mod returning;
mod utils;

pub use core_fn::InsertBuilder;
pub use utils::{Assignment, ConflictAction, ConflictSpec, MergeValue};
