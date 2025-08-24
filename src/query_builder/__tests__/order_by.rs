use super::super::*;
use crate::{
    expression::helpers::{col, val},
    tests::dialect_test_helpers::qi,
    type_helpers::QBClosureHelper,
};

type QB = QueryBuilder<'static, ()>;

#[test]
fn order_by_single_and_multiple_columns() {
    let (sql, params) = QB::new_empty()
        .from("orders")
        .select(("id", "user_id"))
        .order_by(("user_id", "id"))
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty(), "plain columns don't add params");

    let sql = sql.replace('\n', " ");

    let needle_plain = format!("ORDER BY {}, {}", qi("user_id"), qi("id"));
    let needle_asc = format!("ORDER BY {} ASC, {} ASC", qi("user_id"), qi("id"));

    assert!(
        sql.contains(&needle_plain) || sql.contains(&needle_asc),
        "expected `{}` or `{}` in SQL, got:\n{}",
        needle_plain,
        needle_asc,
        sql
    );
}

#[test]
fn order_by_expression_collects_params() {
    // ORDER BY (age + 1)
    let (sql, params) = QB::new_empty()
        .from("users")
        .select(("id",))
        .order_by(col("age").add(val(1)))
        .to_sql()
        .expect("to_sql");

    assert_eq!(params.len(), 1, "one param from val(1) must be collected");
    assert!(
        sql.contains("ORDER BY"),
        "ORDER BY must be present, got:\n{sql}"
    );
}

#[test]
fn order_by_empty_list_ignored() {
    let empty: Vec<&str> = vec![];
    let (sql, _params) = QB::new_empty()
        .from("t")
        .select(("*",))
        .order_by(empty)
        .to_sql()
        .expect("to_sql");

    assert!(
        !sql.contains("ORDER BY"),
        "ORDER BY must not appear for empty arg list, got:\n{sql}"
    );
}

#[test]
fn order_by_rejects_subquery_and_closure() {
    // Subquery
    let sub = QB::new_empty().from("t2").select(("x",));
    let err1 = QB::new_empty()
        .from("t1")
        .select(("y",))
        .order_by(sub)
        .to_sql()
        .unwrap_err();
    assert!(
        err1.to_string()
            .contains("order_by(): подзапросы/замыкания в ORDER BY не поддерживаются"),
        "expected builder error for subquery"
    );

    // Closure
    let err2 = QB::new_empty()
        .from("t1")
        .select(("y",))
        .order_by::<QBClosureHelper<()>>(|qb| qb.from("t2").select(("z",)))
        .to_sql()
        .unwrap_err();
    assert!(
        err2.to_string()
            .contains("order_by(): подзапросы/замыкания в ORDER BY не поддерживаются"),
        "expected builder error for closure"
    );
}
