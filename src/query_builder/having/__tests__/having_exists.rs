use super::super::super::*;
use crate::expression::helpers::{col, val};
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, Query, SetExpr};

/// Достаём HAVING из AST
fn extract_having(q: &Query) -> Option<&SqlExpr> {
    match q.body.as_ref() {
        SetExpr::Select(sel) => sel.having.as_ref(),
        _ => None,
    }
}

#[test]
fn having_exists_and_not_exists() {
    // подзапрос через замыкание
    let sub = |qb: QueryBuilder| {
        qb.from("orders")
            .select(("*",))
            .r#where(col("amount").gt(val(100)))
    };

    // EXISTS
    let qb1 = QueryBuilder::new_empty()
        .from("users")
        .select(("user_id",))
        .group_by(("user_id",))
        .having_exists(sub);

    let (q1, p1) = qb1.build_query_ast().expect("ok");
    assert_eq!(p1.len(), 1, "из подзапроса должен прийти 1 параметр (100)");
    assert!(
        matches!(
            extract_having(&q1).unwrap(),
            SqlExpr::Exists { negated: false, .. }
        ),
        "ожидался EXISTS (negated=false)"
    );

    // NOT EXISTS
    let qb2 = QueryBuilder::new_empty()
        .from("users")
        .select(("user_id",))
        .group_by(("user_id",))
        .having_not_exists(sub);

    let (q2, _p2) = qb2.build_query_ast().expect("ok");
    assert!(
        matches!(
            extract_having(&q2).unwrap(),
            SqlExpr::Exists { negated: true, .. }
        ),
        "ожидался NOT EXISTS (negated=true)"
    );
}

#[test]
fn or_having_exists_builds_or_tree() {
    let sub1 = |qb: QueryBuilder| qb.from("t1").select(("1",)).r#where(col("x").gt(val(10)));
    let sub2 = |qb: QueryBuilder| qb.from("t2").select(("1",)).r#where(col("y").lt(val(5)));

    let qb = QueryBuilder::new_empty()
        .from("agg")
        .select(("k",))
        .group_by(("k",))
        .having_exists(sub1)
        .or_having_exists(sub2);

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(
        params.len(),
        2,
        "ожидались параметры из обоих подзапросов (10 и 5)"
    );

    let h = extract_having(&q).unwrap();
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or, "верхний оператор должен быть OR");
            assert!(matches!(**left, SqlExpr::Exists { negated: false, .. }));
            assert!(matches!(**right, SqlExpr::Exists { negated: false, .. }));
        }
        other => panic!("ожидался BinaryOp(OR) над EXISTS, получено: {:?}", other),
    }
}

#[test]
fn having_exists_with_expression_is_builder_error() {
    // Передаём выражение вместо подзапроса — должна быть ошибка билдера
    let err = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_exists(col("x"))
        .to_sql()
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("having_exists(): требуется подзапрос"),
        "ожидалась ошибка о том, что нужен подзапрос, got: {msg}"
    );
}

#[test]
fn or_having_not_exists_with_expression_is_builder_error() {
    let err = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .or_having_not_exists(col("x"))
        .to_sql()
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("or_having_not_exists(): требуется подзапрос")
            || msg.contains("having_not_exists(): требуется подзапрос"),
        "ожидалась ошибка про необходимость подзапроса, got: {msg}"
    );
}

#[test]
fn having_exists_collects_params_from_subquery() {
    // Два подзапроса с параметрами: 42 и 7 — в таком порядке
    let sub1 = QueryBuilder::new_empty()
        .from("a")
        .select(("*",))
        .r#where(col("n").eq(val(42)));

    let sub2 = |qb: QueryBuilder| qb.from("b").select(("*",)).r#where(col("m").eq(val(7)));

    let qb = QueryBuilder::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_exists(sub1)
        .or_having_exists(sub2);

    let (_q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 2);
    assert!(matches!(params[0], crate::param::Param::I32(42)));
    assert!(matches!(params[1], crate::param::Param::I32(7)));
}
