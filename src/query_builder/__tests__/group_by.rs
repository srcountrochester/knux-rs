use super::super::*;
use crate::{
    expression::helpers::{col, val},
    tests::dialect_test_helpers::qi,
    type_helpers::QBClosureHelper,
};

type QB = QueryBuilder<'static, ()>;

#[test]
fn group_by_single_and_multiple_columns() {
    let (sql, params) = QB::new_empty()
        .from("orders")
        .select(("user_id", "status"))
        .group_by(("user_id", "status"))
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty(), "no params expected for plain columns");

    // Проверяем, что есть GROUP BY и оба столбца в нужном порядке
    let sql = sql.replace('\n', " ");
    let needle = format!("GROUP BY {}, {}", qi("user_id"), qi("status"));
    assert!(
        sql.contains(&needle),
        "expected `{needle}` in SQL, got:\n{sql}"
    );
}

#[test]
fn group_by_expression_collects_params() {
    let (sql, params) = QB::new_empty()
        .from("users")
        .select(("id",))
        .group_by(col("age").add(val(1)))
        .to_sql()
        .expect("to_sql");

    assert_eq!(params.len(), 1, "one param from val(1) must be collected");

    // Минимальная проверка: наличие GROUP BY
    assert!(
        sql.contains("GROUP BY"),
        "GROUP BY must be present in SQL, got:\n{sql}"
    );
}

#[test]
fn group_by_ignores_empty_list() {
    let empty: Vec<&str> = Vec::new();
    let (sql, _params) = QB::new_empty()
        .from("logs")
        .select(("*",))
        .group_by(empty) // пустой список должен игнорироваться
        .to_sql()
        .expect("to_sql");

    assert!(
        !sql.contains("GROUP BY"),
        "GROUP BY must not appear for empty list, got:\n{sql}"
    );
}

#[test]
fn group_by_rejects_subquery() {
    // подзапрос в GROUP BY не поддерживается — должен быть builder error
    let sub = QB::new_empty().from("t").select(("x",));

    let err = QB::new_empty()
        .from("t")
        .select(("x",))
        .group_by(sub) // передаём подзапрос как аргумент
        .to_sql()
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("group_by(): подзапросы/замыкания в GROUP BY не поддерживаются"),
        "expected builder error about unsupported subquery, got: {msg}"
    );
}

#[test]
fn group_by_rejects_closure() {
    // closure → тоже подзапрос, должен дать builder error
    let err = QB::new_empty()
        .from("t")
        .select(("x",))
        .group_by::<QBClosureHelper<()>>(|qb| qb.from("u").select(("y",)))
        .to_sql()
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("group_by(): подзапросы/замыкания в GROUP BY не поддерживаются"),
        "expected builder error about unsupported closure, got: {msg}"
    );
}
