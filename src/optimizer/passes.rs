use sqlparser::ast as S;

#[inline]
pub fn rm_subquery_order_by(_stmt: &mut S::Statement) {
    // TODO(#opt1): remove ORDER BY in subqueries without LIMIT
}

#[inline]
pub fn simplify_exists(_stmt: &mut S::Statement) {
    // TODO(#opt2): normalize EXISTS subqueries
}

#[inline]
pub fn predicate_pushdown(_stmt: &mut S::Statement) {
    // TODO(#opt3): push predicates into subqueries / joins (conservative)
}

#[inline]
pub fn flatten_simple_subqueries(_stmt: &mut S::Statement) {
    // TODO(#opt4): flatten trivial SELECT ... FROM (SELECT ...) cases
}

#[inline]
pub fn dedup_in_list(_stmt: &mut S::Statement) {
    // TODO(#opt5): deduplicate constants inside IN (...)
}

#[inline]
pub fn in_to_exists(_stmt: &mut S::Statement) {
    // TODO(#opt6): rewrite IN (subquery) to EXISTS (manual opt-in)
}
