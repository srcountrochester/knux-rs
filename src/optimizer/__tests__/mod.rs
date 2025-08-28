#[cfg(test)]
mod passes;

#[cfg(test)]
mod rm_subquery_order_by;

#[cfg(test)]
mod simplify_exists;

#[cfg(test)]
mod predicate_pullup;

#[cfg(test)]
mod predicate_pushdown;

#[cfg(test)]
mod flatten_simple_subqueries;

#[cfg(test)]
mod dedup_in_list;

#[cfg(test)]
mod in_to_exists;

#[cfg(test)]
mod utils;
