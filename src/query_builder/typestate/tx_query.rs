use crate::{
    expression::JoinOnBuilder,
    param::Param,
    query_builder::{
        InsertBuilder, QueryBuilder, QueryOne, QueryOptional, Result as QBResult,
        args::{ArgList, IntoQBArg},
        delete::DeleteBuilder,
        join::JoinOnArg,
        update::UpdateBuilder,
    },
};

/// Контекст транзакции: только последовательный await
#[derive(Clone)]
pub struct TxQuery<'a, T = ()>(pub(crate) QueryBuilder<'a, T>);

impl<'a, T> TxQuery<'a, T> {
    pub(crate) fn new(inner: QueryBuilder<'a, T>) -> Self {
        Self(inner)
    }
}

impl<'a, T> std::ops::Deref for TxQuery<'a, T> {
    type Target = QueryBuilder<'a, T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl<'a, T> std::ops::DerefMut for TxQuery<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// === Public interface for TxQuery ===
impl<'a, T> TxQuery<'a, T> {
    // ALIAS

    #[inline]
    pub fn r#as<S: Into<String>>(self, alias: S) -> Self {
        Self(self.0.r#as(alias))
    }

    #[inline]
    pub fn alias<S: Into<String>>(self, alias: S) -> Self {
        Self(self.0.alias(alias))
    }

    // CLEAR

    #[inline]
    pub fn clear(self, op: &str) -> Self {
        Self(self.0.alias(op))
    }

    #[inline]
    pub fn clear_select(self) -> Self {
        Self(self.0.clear_select())
    }

    #[inline]
    pub fn clear_where(self) -> Self {
        Self(self.0.clear_where())
    }

    #[inline]
    pub fn clear_group(self) -> Self {
        Self(self.0.clear_group())
    }

    #[inline]
    pub fn clear_order(self) -> Self {
        Self(self.0.clear_order())
    }

    #[inline]
    pub fn clear_having(self) -> Self {
        Self(self.0.clear_having())
    }

    #[inline]
    pub fn clear_join(self) -> Self {
        Self(self.0.clear_join())
    }

    #[inline]
    pub fn clear_limit(self) -> Self {
        Self(self.0.clear_limit())
    }

    #[inline]
    pub fn clear_offset(self) -> Self {
        Self(self.0.clear_offset())
    }

    #[inline]
    pub fn clear_limit_offset(self) -> Self {
        Self(self.0.clear_limit_offset())
    }

    #[inline]
    pub fn clear_counters(self) -> Self {
        Self(self.0.clear_counters())
    }

    #[inline]
    pub fn clear_distinct(self) -> Self {
        Self(self.0.clear_distinct())
    }

    // DISTINCT

    #[inline]
    pub fn distinct<A: ArgList<'a>>(self, items: A) -> Self {
        Self(self.0.distinct(items))
    }

    #[inline]
    pub fn distinct_on<A: ArgList<'a>>(self, items: A) -> Self {
        Self(self.0.distinct_on(items))
    }

    // FROM

    #[inline]
    pub fn from<L: ArgList<'a>>(self, tables: L) -> Self {
        Self(self.0.from(tables))
    }

    #[inline]
    pub fn from_mut<L>(&mut self, items: L) -> &mut Self
    where
        L: ArgList<'a>,
        QueryBuilder<'a, T>: Clone,
    {
        let new_inner = self.0.clone().from(items);
        self.0 = new_inner;
        self
    }

    // GROUP BY

    #[inline]
    pub fn group_by<A: ArgList<'a>>(self, items: A) -> Self {
        Self(self.0.group_by(items))
    }

    // LIMIT

    #[inline]
    pub fn limit(self, limit: u64) -> Self {
        Self(self.0.limit(limit))
    }

    #[inline]
    pub fn offset(self, offset: u64) -> Self {
        Self(self.0.offset(offset))
    }

    #[inline]
    pub fn limit_offset(self, limit: u64, offset: u64) -> Self {
        Self(self.0.limit_offset(limit, offset))
    }

    // ORDER BY

    #[inline]
    pub fn order_by<A: ArgList<'a>>(self, items: A) -> Self {
        Self(self.0.order_by(items))
    }

    // SCHEMA

    #[inline]
    pub fn schema<S: Into<String>>(self, schema: S) -> Self {
        Self(self.0.schema(schema))
    }

    // SELECT

    #[inline]
    pub fn select<L: ArgList<'a>>(self, items: L) -> Self {
        Self(self.0.select(items))
    }

    #[inline]
    pub fn select_mut<L>(&mut self, items: L) -> &mut Self
    where
        L: ArgList<'a>,
        QueryBuilder<'a, T>: Clone,
    {
        let new_inner = self.0.clone().select(items);
        self.0 = new_inner;
        self
    }

    // SQL

    #[inline]
    pub fn to_sql(self) -> QBResult<(String, Vec<Param>)> {
        self.0.to_sql()
    }

    // UNION

    #[inline]
    pub fn union<L: ArgList<'a>>(self, rhs: L) -> Self {
        Self(self.0.union(rhs))
    }

    #[inline]
    pub fn union_all<L: ArgList<'a>>(self, rhs: L) -> Self {
        Self(self.0.union_all(rhs))
    }

    #[inline]
    pub fn intersect<L: ArgList<'a>>(self, rhs: L) -> Self {
        Self(self.0.intersect(rhs))
    }

    #[inline]
    pub fn intersect_all<L: ArgList<'a>>(self, rhs: L) -> Self {
        Self(self.0.intersect_all(rhs))
    }

    #[inline]
    pub fn except<L: ArgList<'a>>(self, rhs: L) -> Self {
        Self(self.0.except(rhs))
    }

    #[inline]
    pub fn except_all<L: ArgList<'a>>(self, rhs: L) -> Self {
        Self(self.0.except_all(rhs))
    }

    // WITH

    #[inline]
    pub fn with<L: ArgList<'a>>(self, name: &str, body: L) -> Self {
        Self(self.0.with(name, body))
    }

    #[inline]
    pub fn with_recursive<L: ArgList<'a>>(self, name: &str, body: L) -> Self {
        Self(self.0.with_recursive(name, body))
    }

    #[inline]
    pub fn with_materialized<L: ArgList<'a>>(self, name: &str, body: L) -> Self {
        Self(self.0.with_materialized(name, body))
    }

    #[inline]
    pub fn with_not_materialized<L: ArgList<'a>>(self, name: &str, body: L) -> Self {
        Self(self.0.with_not_materialized(name, body))
    }

    #[inline]
    pub fn with_from<L: ArgList<'a>>(self, name: &str, from: &str, body: L) -> Self {
        Self(self.0.with_from(name, from, body))
    }

    // HAVING

    #[inline]
    pub fn having<A: ArgList<'a>>(self, args: A) -> Self {
        Self(self.0.having(args))
    }

    #[inline]
    pub fn or_having<A: ArgList<'a>>(self, args: A) -> Self {
        Self(self.0.or_having(args))
    }

    #[inline]
    pub fn having_raw(self, raw: &str) -> Self {
        Self(self.0.having_raw(raw))
    }

    #[inline]
    pub fn or_having_raw(self, raw: &str) -> Self {
        Self(self.0.or_having_raw(raw))
    }

    #[inline]
    pub fn and_having<A: ArgList<'a>>(self, args: A) -> Self {
        Self(self.0.and_having(args))
    }

    #[inline]
    pub fn having_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.having_between(target, low, high))
    }

    #[inline]
    pub fn or_having_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.or_having_between(target, low, high))
    }

    #[inline]
    pub fn having_not_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.having_not_between(target, low, high))
    }

    #[inline]
    pub fn or_having_not_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.or_having_not_between(target, low, high))
    }

    #[inline]
    pub fn having_exists<K>(self, sub: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.having_exists(sub))
    }

    #[inline]
    pub fn or_having_exists<K>(self, sub: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.or_having_exists(sub))
    }

    #[inline]
    pub fn having_not_exists<K>(self, sub: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.having_not_exists(sub))
    }

    #[inline]
    pub fn or_having_not_exists<K>(self, sub: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.or_having_not_exists(sub))
    }

    #[inline]
    pub fn having_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.having_in(column, values))
    }

    #[inline]
    pub fn or_having_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.or_having_in(column, values))
    }

    #[inline]
    pub fn having_not_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.having_not_in(column, values))
    }

    #[inline]
    pub fn or_having_not_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.or_having_not_in(column, values))
    }

    #[inline]
    pub fn having_null<K>(self, expr: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.having_null(expr))
    }

    #[inline]
    pub fn or_having_null<K>(self, expr: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.or_having_null(expr))
    }

    #[inline]
    pub fn having_not_null<K>(self, expr: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.having_not_null(expr))
    }

    #[inline]
    pub fn or_having_not_null<K>(self, expr: K) -> Self
    where
        K: IntoQBArg<'a>,
    {
        Self(self.0.or_having_not_null(expr))
    }

    // JOIN

    #[inline]
    pub fn join<L, O>(self, target: L, on: O) -> Self
    where
        L: IntoQBArg<'a>,
        O: Into<JoinOnArg>,
    {
        Self(self.0.join(target, on))
    }

    #[inline]
    pub fn left_join<L, O>(self, target: L, on: O) -> Self
    where
        L: IntoQBArg<'a>,
        O: Into<JoinOnArg>,
    {
        Self(self.0.left_join(target, on))
    }

    #[inline]
    pub fn right_join<L, O>(self, target: L, on: O) -> Self
    where
        L: IntoQBArg<'a>,
        O: Into<JoinOnArg>,
    {
        Self(self.0.right_join(target, on))
    }

    #[inline]
    pub fn full_join<L, O>(self, target: L, on: O) -> Self
    where
        L: IntoQBArg<'a>,
        O: Into<JoinOnArg>,
    {
        Self(self.0.full_join(target, on))
    }

    #[inline]
    pub fn cross_join<L>(self, target: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.cross_join(target))
    }

    #[inline]
    pub fn natural_join<L>(self, target: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.natural_join(target))
    }

    #[inline]
    pub fn natural_left_join<L>(self, target: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.natural_left_join(target))
    }

    #[inline]
    pub fn natural_right_join<L>(self, target: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.natural_right_join(target))
    }

    #[inline]
    pub fn natural_full_join<L>(self, target: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.natural_full_join(target))
    }

    #[inline]
    pub fn join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.join_with(target, f))
    }

    #[inline]
    pub fn left_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.left_join_with(target, f))
    }

    #[inline]
    pub fn right_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.right_join_with(target, f))
    }

    #[inline]
    pub fn full_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.full_join_with(target, f))
    }

    #[inline]
    pub fn cross_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.cross_join_with(target, f))
    }

    #[inline]
    pub fn natural_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.natural_join_with(target, f))
    }

    #[inline]
    pub fn natural_left_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.natural_left_join_with(target, f))
    }

    #[inline]
    pub fn natural_right_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.natural_right_join_with(target, f))
    }

    #[inline]
    pub fn natural_full_join_with<L, F>(self, target: L, f: F) -> Self
    where
        L: IntoQBArg<'a>,
        F: FnOnce(JoinOnBuilder) -> JoinOnBuilder + Send + 'static,
    {
        Self(self.0.natural_full_join_with(target, f))
    }

    // CRUD

    #[inline]
    pub fn delete<L: ArgList<'a>>(self, what: L) -> DeleteBuilder<'a, T> {
        self.0.delete(what)
    }

    #[inline]
    pub fn update<L: ArgList<'a>>(self, table: L) -> UpdateBuilder<'a, T> {
        self.0.update(table)
    }

    #[inline]
    pub fn into<L: ArgList<'a>>(self, table: L) -> InsertBuilder<'a, T> {
        self.0.into(table)
    }

    #[inline]
    pub fn insert<L: ArgList<'a>>(self, values: L) -> InsertBuilder<'a, T> {
        self.0.insert(values)
    }

    // WHERE

    #[inline]
    pub fn where_<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.where_(cond))
    }

    #[inline]
    pub fn r#where<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.where_(cond))
    }

    #[inline]
    pub fn and_where<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.and_where(cond))
    }

    #[inline]
    pub fn or_where<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.or_where(cond))
    }

    #[inline]
    pub fn where_not<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.where_not(cond))
    }

    #[inline]
    pub fn and_where_not<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.and_where_not(cond))
    }

    #[inline]
    pub fn or_where_not<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.or_where_not(cond))
    }

    #[inline]
    pub fn where_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.where_between(target, low, high))
    }

    #[inline]
    pub fn or_where_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.or_where_between(target, low, high))
    }

    #[inline]
    pub fn where_not_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.where_not_between(target, low, high))
    }

    #[inline]
    pub fn or_where_not_between<K, L, H>(self, target: K, low: L, high: H) -> Self
    where
        K: IntoQBArg<'a>,
        L: IntoQBArg<'a>,
        H: IntoQBArg<'a>,
    {
        Self(self.0.or_where_not_between(target, low, high))
    }

    #[inline]
    pub fn where_exists<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.where_exists(cond))
    }

    #[inline]
    pub fn or_where_exists<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.or_where_exists(cond))
    }

    #[inline]
    pub fn where_not_exists<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.where_not_exists(cond))
    }

    #[inline]
    pub fn or_where_not_exists<C>(self, cond: C) -> Self
    where
        C: IntoQBArg<'a>,
    {
        Self(self.0.or_where_not_exists(cond))
    }

    #[inline]
    pub fn where_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.where_in(column, values))
    }

    #[inline]
    pub fn or_where_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.or_where_in(column, values))
    }

    #[inline]
    pub fn where_not_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.where_not_in(column, values))
    }

    #[inline]
    pub fn or_where_not_in<C, A>(self, column: C, values: A) -> Self
    where
        C: IntoQBArg<'a>,
        A: ArgList<'a>,
    {
        Self(self.0.or_where_not_in(column, values))
    }

    #[inline]
    pub fn where_json_object(self, col: &str, json: &str) -> Self {
        Self(self.0.where_json_object(col, json))
    }

    #[inline]
    pub fn where_json_path(self, col: &str, json: &str) -> Self {
        Self(self.0.where_json_path(col, json))
    }

    #[inline]
    pub fn where_json_superset_of(self, col: &str, json: &str) -> Self {
        Self(self.0.where_json_superset_of(col, json))
    }

    #[inline]
    pub fn where_json_subset_of(self, col: &str, json: &str) -> Self {
        Self(self.0.where_json_subset_of(col, json))
    }

    #[inline]
    pub fn where_like<L, R>(self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        Self(self.0.where_like(left, pattern))
    }

    #[inline]
    pub fn or_where_like<L, R>(self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        Self(self.0.or_where_like(left, pattern))
    }

    #[inline]
    pub fn where_ilike<L, R>(self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        Self(self.0.where_ilike(left, pattern))
    }

    #[inline]
    pub fn or_where_ilike<L, R>(self, left: L, pattern: R) -> Self
    where
        L: IntoQBArg<'a>,
        R: IntoQBArg<'a>,
    {
        Self(self.0.or_where_ilike(left, pattern))
    }

    #[inline]
    pub fn where_null<L>(self, expr: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.where_null(expr))
    }

    #[inline]
    pub fn or_where_null<L>(self, expr: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.or_where_null(expr))
    }

    #[inline]
    pub fn where_not_null<L>(self, expr: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.where_not_null(expr))
    }

    #[inline]
    pub fn or_where_not_null<L>(self, expr: L) -> Self
    where
        L: IntoQBArg<'a>,
    {
        Self(self.0.or_where_not_null(expr))
    }

    #[inline]
    pub fn where_raw(self, raw: &str) -> Self {
        Self(self.0.where_raw(raw))
    }

    #[inline]
    pub fn or_where_raw(self, raw: &str) -> Self {
        Self(self.0.or_where_raw(raw))
    }

    // FETCH

    #[inline]
    pub fn one(self) -> QueryOne<'a, T> {
        self.0.one()
    }

    #[inline]
    pub fn optional(self) -> QueryOptional<'a, T> {
        self.0.optional()
    }
}
