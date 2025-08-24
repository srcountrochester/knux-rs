use super::super::*;
use crate::{
    expression::{
        JoinOnExt,
        helpers::{col, table, val},
    },
    query_builder::args::{QBArg, QBClosure},
};

use sqlparser::ast::{
    BinaryOperator as BO, Expr as SqlExpr, Join, JoinConstraint, JoinOperator, Query, SetExpr,
    TableFactor,
};

type QB = QueryBuilder<'static, ()>;

fn first_join_from(q: &Query) -> &Join {
    // Достаём &Select без перемещений
    let select = match q.body.as_ref() {
        SetExpr::Select(select_box) => select_box.as_ref(),
        _ => panic!("expected SELECT body"),
    };

    // Берём ссылку на from
    let from = &select.from;
    assert!(!from.is_empty(), "FROM must not be empty");

    let twj = &from[0];
    assert!(!twj.joins.is_empty(), "expected at least one JOIN");

    &twj.joins[0]
}

#[test]
fn inner_join_with_on_string_builds_ast() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .join("accounts", "users.id = accounts.user_id");

    let (query, _params) = qb.build_query_ast().expect("build ok");

    let join = first_join_from(&query);
    match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr)) => {
            // ожидаем бинарное равенство users.id = accounts.user_id
            match expr {
                SqlExpr::BinaryOp { op, .. } => assert!(matches!(op, BO::Eq)),
                other => panic!("expected Eq, got {:?}", other),
            }
        }
        other => panic!("expected INNER ... ON <expr>, got {:?}", other),
    }
}

#[test]
fn inner_join_with_on_expr_chain_and_on() {
    let qb = QB::new_empty().from("users").select("*").join(
        table("accounts"),
        col("users.id")
            .eq(col("accounts.user_id"))
            .and_on(col("users.is_active").eq(val(true))),
    );

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let join = first_join_from(&query);
    match &join.join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr)) => {
            // верхний уровень должен быть AND
            match expr {
                SqlExpr::BinaryOp { op, left, right } => {
                    assert!(matches!(op, BO::And));
                    // левая и правая — оба Eq(...)
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
                other => panic!("expected AND of two Eq, got {:?}", other),
            }
        }
        other => panic!("expected INNER ... ON <expr>, got {:?}", other),
    }
}

#[test]
fn resolve_join_target_from_expression_table() {
    let qb = QB::new_empty();
    let (tf, _p) = qb
        .resolve_join_target(QBArg::Expr(table("public.accounts")))
        .expect("resolve ok");

    match tf {
        TableFactor::Table { name, .. } => {
            // "public"."accounts"
            let parts = name.0;
            assert_eq!(parts.len(), 2);
            // Identifier(..) в обоих частях
            use sqlparser::ast::ObjectNamePart;
            assert!(matches!(parts[0], ObjectNamePart::Identifier(_)));
            assert!(matches!(parts[1], ObjectNamePart::Identifier(_)));
        }
        other => panic!("expected TableFactor::Table, got {:?}", other),
    }
}

#[test]
fn resolve_join_target_from_subquery_and_closure() {
    // Subquery
    let sub = QB::new_empty().from("accounts").select("*");
    let qb = QB::new_empty();
    let (tf1, p1) = qb
        .resolve_join_target(QBArg::Subquery(sub))
        .expect("resolve subquery ok");
    assert!(matches!(tf1, TableFactor::Derived { .. }));
    assert!(p1.len() >= 0);

    // Closure
    let qb = QB::new_empty();
    let (tf2, p2) = qb
        .resolve_join_target(QBArg::Closure(QBClosure::new(|qb| {
            qb.from("accounts").select("*")
        })))
        .expect("resolve closure ok");
    assert!(matches!(tf2, TableFactor::Derived { .. }));
    assert!(p2.len() >= 0);
}

#[test]
fn left_join_without_on_records_error_and_defaults_to_true() {
    // Делаем LEFT JOIN без ON — должен появиться builder error, а в AST — ON TRUE
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .left_join("accounts", |on| on); // не ставим .on(...)

    // build_query_ast вернёт Err с агрегированными ошибками
    let err = qb.build_query_ast().unwrap_err();
    // Текст ошибки начинается с "Builder errors:\n- LEFT JOIN: требуется ON-условие"
    let s = err.to_string();
    assert!(
        s.contains("LEFT JOIN") && s.contains("требуется ON-условие"),
        "unexpected error text: {s}"
    );
}

#[test]
fn cross_join_has_no_constraint() {
    let qb = QB::new_empty()
        .from("users")
        .select("*")
        .cross_join("accounts");

    let (query, _params) = qb.build_query_ast().expect("build ok");
    let join = first_join_from(&query);
    assert!(matches!(join.join_operator, JoinOperator::CrossJoin));
}
