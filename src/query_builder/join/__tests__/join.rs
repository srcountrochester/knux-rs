use super::super::*;
use crate::{
    expression::{
        JoinOnExt,
        helpers::{col, table, val},
    },
    type_helpers::QBClosureHelper,
};
use sqlparser::ast::{
    BinaryOperator as BO, Expr as SqlExpr, Join, JoinConstraint, JoinOperator, Query, SetExpr,
    TableFactor,
};

type QB = QueryBuilder<'static, ()>;

/// Аккуратный доступ к первому JOIN без перемещений.
fn first_join_from(q: &Query) -> &Join {
    let select = match q.body.as_ref() {
        SetExpr::Select(select_box) => select_box.as_ref(),
        _ => panic!("expected SELECT body"),
    };

    let from = &select.from;
    assert!(!from.is_empty(), "FROM must not be empty");

    let twj = &from[0];
    assert!(!twj.joins.is_empty(), "expected at least one JOIN");

    &twj.joins[0]
}

#[test]
fn inner_join_with_on_string() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .join("accounts", "users.id = accounts.user_id");

    let (query, _params) = qb.build_query_ast().expect("build ok");

    let j = first_join_from(&query);
    match &j.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr)) => match expr {
            SqlExpr::BinaryOp { op, .. } => assert!(matches!(op, BO::Eq)),
            other => panic!("expected Eq, got {:?}", other),
        },
        other => panic!("expected INNER ... ON <expr>, got {:?}", other),
    }
}

#[test]
fn inner_join_with_on_expression_chain_and_on() {
    let qb = QB::new_empty().from("users").select("*").join(
        table("accounts"),
        col("users.id")
            .eq(col("accounts.user_id"))
            .and_on(col("users.is_active").eq(val(true))),
    );

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let j = first_join_from(&query);
    match &j.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr)) => match expr {
            SqlExpr::BinaryOp { op, left, right } => {
                assert!(matches!(op, BO::And), "top op must be AND, got {:?}", op);
                if let SqlExpr::BinaryOp { op: l_op, .. } = left.as_ref() {
                    assert!(matches!(l_op, BO::Eq));
                } else {
                    panic!("left side of AND is not Eq");
                }
                if let SqlExpr::BinaryOp { op: r_op, .. } = right.as_ref() {
                    assert!(matches!(r_op, BO::Eq));
                } else {
                    panic!("right side of AND is not Eq");
                }
            }
            other => panic!("expected AND(Eq, Eq), got {:?}", other),
        },
        other => panic!("expected INNER ... ON <expr>, got {:?}", other),
    }
}

#[test]
fn cross_join_has_no_constraint() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .cross_join("accounts");

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let j = first_join_from(&query);
    assert!(matches!(j.join_operator, JoinOperator::CrossJoin));
}

#[test]
fn left_join_with_on_string() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .left_join("accounts", "users.id = accounts.user_id");

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let j = first_join_from(&query);
    assert!(
        matches!(
            j.join_operator,
            JoinOperator::LeftOuter(JoinConstraint::On(_))
        ),
        "expected LEFT OUTER ... ON <expr>, got {:?}",
        j.join_operator
    );
}

#[test]
fn left_join_without_on_registers_error() {
    // Не задаём ON — должен появиться builder error
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .left_join("accounts", |on| on);

    let err = qb.build_query_ast().unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("LEFT JOIN") && s.contains("требуется ON-условие"),
        "unexpected error text: {s}"
    );
}

#[test]
fn right_join_behavior_is_dialect_agnostic() {
    // Тест устойчив к диалекту: в SQLite ожидаем BuilderErrors;
    // в других — проверяем оператор RIGHT OUTER.
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .right_join("accounts", "users.id = accounts.user_id");

    match qb.build_query_ast() {
        Ok((query, _)) => {
            let j = first_join_from(&query);
            assert!(
                matches!(
                    j.join_operator,
                    JoinOperator::RightOuter(JoinConstraint::On(_))
                ),
                "expected RIGHT OUTER ... ON <expr>, got {:?}",
                j.join_operator
            );
        }
        Err(e) => {
            // В SQLite получим агрегированную ошибку
            let s = e.to_string();
            assert!(
                s.contains("SQLite") && (s.contains("RIGHT") || s.contains("RIGHT/FULL")),
                "unexpected error text for SQLite: {s}"
            );
        }
    }
}

#[test]
fn full_join_behavior_is_dialect_agnostic() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .full_join("accounts", "users.id = accounts.user_id");

    match qb.build_query_ast() {
        Ok((query, _)) => {
            let j = first_join_from(&query);
            assert!(
                matches!(
                    j.join_operator,
                    JoinOperator::FullOuter(JoinConstraint::On(_))
                ),
                "expected FULL OUTER ... ON <expr>, got {:?}",
                j.join_operator
            );
        }
        Err(e) => {
            // В SQLite — ошибка запрета FULL JOIN
            let s = e.to_string();
            assert!(
                s.contains("SQLite") && (s.contains("FULL") || s.contains("RIGHT/FULL")),
                "unexpected error text for SQLite: {s}"
            );
        }
    }
}

#[test]
fn join_with_subquery_target_produces_derived_relation() {
    let sub = QB::new_empty()
        .from("accounts")
        .select("*")
        .r#where(col("user_id").eq(val(1))); // имя метода where у тебя может быть `where_`

    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .join(sub, "users.id = accounts.user_id");

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let j = first_join_from(&query);
    match &j.relation {
        TableFactor::Derived { .. } => {}
        other => panic!("expected Derived subquery as JOIN target, got {:?}", other),
    }
}

#[test]
fn join_with_closure_target_produces_derived_relation() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .join::<QBClosureHelper<()>, _>(
            |qb: QueryBuilder| {
                qb.from("accounts")
                    .select("*")
                    .r#where(col("user_id").eq(val(1)))
            },
            "users.id = accounts.user_id",
        );

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let j = first_join_from(&query);
    match &j.relation {
        TableFactor::Derived { .. } => {}
        other => panic!("expected Derived subquery as JOIN target, got {:?}", other),
    }
}

#[test]
fn join_without_from_registers_error() {
    // .join без .from — должны получить builder error
    let qb = QB::new_empty().select("*").join("accounts", "1=1");
    let err = qb.build_query_ast().unwrap_err();
    let s = err.to_string();
    assert!(
        s.contains("вызови .from") || s.contains("отсутствует источник FROM"),
        "unexpected error text: {s}"
    );
}

#[test]
fn natural_inner_join_builds_ast() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .natural_join("accounts");

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let j = first_join_from(&query);
    assert!(matches!(
        j.join_operator,
        JoinOperator::Inner(JoinConstraint::Natural)
    ));
}

#[test]
fn natural_left_join_builds_ast() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .natural_left_join("accounts");

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let j = first_join_from(&query);
    assert!(matches!(
        j.join_operator,
        JoinOperator::LeftOuter(JoinConstraint::Natural)
    ));
}

#[test]
fn natural_right_join_behavior_is_dialect_agnostic() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .natural_right_join("accounts");

    match qb.build_query_ast() {
        Ok((query, _)) => {
            let j = first_join_from(&query);
            assert!(matches!(
                j.join_operator,
                JoinOperator::RightOuter(JoinConstraint::Natural)
            ));
        }
        Err(e) => {
            // В SQLite ожидаем запрет (совпадает с обычным RIGHT/FULL)
            let s = e.to_string();
            assert!(
                s.contains("SQLite") && (s.contains("RIGHT") || s.contains("RIGHT/FULL")),
                "unexpected error: {s}"
            );
        }
    }
}

#[test]
fn natural_full_join_behavior_is_dialect_agnostic() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .natural_full_join("accounts");

    match qb.build_query_ast() {
        Ok((query, _)) => {
            let j = first_join_from(&query);
            assert!(matches!(
                j.join_operator,
                JoinOperator::FullOuter(JoinConstraint::Natural)
            ));
        }
        Err(e) => {
            let s = e.to_string();
            assert!(
                s.contains("SQLite") && (s.contains("FULL") || s.contains("RIGHT/FULL")),
                "unexpected error: {s}"
            );
        }
    }
}
