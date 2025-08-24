use crate::query_builder::QueryBuilder;

pub type QBClosureHelper<T> = for<'a> fn(QueryBuilder<'a, T>) -> QueryBuilder<'a, T>;
