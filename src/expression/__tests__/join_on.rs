use super::super::*;
use crate::expression::helpers::{col, val};
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

fn is_eq(lhs: &SqlExpr, rhs: &SqlExpr, node: &SqlExpr) -> bool {
    match node {
        SqlExpr::BinaryOp { left, op, right } => {
            matches!(op, BO::Eq) && left.as_ref() == lhs && right.as_ref() == rhs
        }
        _ => false,
    }
}

fn is_and(a: &SqlExpr, b: &SqlExpr, node: &SqlExpr) -> bool {
    match node {
        SqlExpr::BinaryOp { left, op, right } => {
            matches!(op, BO::And) && left.as_ref() == a && right.as_ref() == b
        }
        _ => false,
    }
}

fn is_or(a: &SqlExpr, b: &SqlExpr, node: &SqlExpr) -> bool {
    match node {
        SqlExpr::BinaryOp { left, op, right } => {
            matches!(op, BO::Or) && left.as_ref() == a && right.as_ref() == b
        }
        _ => false,
    }
}

#[test]
fn and_on_is_alias_for_and() {
    let left = col("u.id");
    let right = col("a.user_id");
    let extra = col("u.is_active").eq(val(true));

    let base_eq = left.clone().eq(right.clone());
    let combined = base_eq.clone().and_on(extra.clone());

    // ожидаем: (u.id = a.user_id) AND (u.is_active = true)
    match &combined.expr {
        SqlExpr::BinaryOp {
            left: and_l,
            op,
            right: and_r,
        } => {
            assert!(matches!(op, BO::And));

            // проверяем левую часть AND — это Eq(u.id, a.user_id)
            assert!(matches!(and_l.as_ref(), SqlExpr::BinaryOp { .. }));
            // правая часть AND — это Eq(u.is_active, true)
            assert!(matches!(and_r.as_ref(), SqlExpr::BinaryOp { .. }));

            // Более строгая проверка: сверяем поддеревья
            if let (
                SqlExpr::BinaryOp {
                    left: l1,
                    op: op1,
                    right: r1,
                },
                SqlExpr::BinaryOp {
                    left: l2,
                    op: op2,
                    right: r2,
                },
            ) = (and_l.as_ref(), and_r.as_ref())
            {
                assert!(matches!(op1, BO::Eq));
                assert!(matches!(op2, BO::Eq));
                // простая гарантия структуры; детализацию колонок оставим рендереру
                assert_ne!(l1.to_string(), r1.to_string());
                assert_ne!(l2.to_string(), r2.to_string());
            }
        }
        other => panic!("expected BinaryOp(AND, Eq, Eq), got {:?}", other),
    }
}

#[test]
fn or_on_is_alias_for_or() {
    let base = col("u.id").eq(col("a.user_id"));
    let extra = col("u.is_active").eq(val(true));
    let combined = base.or_on(extra);
    match &combined.expr {
        SqlExpr::BinaryOp { op, .. } => assert!(matches!(op, BO::Or)),
        other => panic!("expected BinaryOp(OR, ..), got {:?}", other),
    }
}

#[test]
fn join_on_builder_empty_and_non_empty() {
    // пустой билдер
    let b = JoinOnBuilder::default();
    assert!(b.is_empty());
    assert!(b.build().is_none());

    // .on(...)
    let b = JoinOnBuilder::default().on(col("u.id").eq(col("a.user_id")));
    assert!(!b.is_empty());
    let built = b.build().expect("should have expression");
    match built.expr {
        SqlExpr::BinaryOp { op, .. } => assert!(matches!(op, BO::Eq)),
        other => panic!("expected Eq, got {:?}", other),
    }
}

#[test]
fn join_on_builder_and_on_or_on_chain() {
    use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr};

    let on_expr = JoinOnBuilder::default()
        .on(col("u.id").eq(col("a.user_id")))
        .and_on(col("u.is_active").eq(val(true)))
        .or_on(col("u.is_admin").eq(val(true)))
        .build()
        .expect("built");

    // ВАЖНО: матчимся по on_expr.expr, а не по on_expr
    match &on_expr.expr {
        // верхний уровень — OR
        SqlExpr::BinaryOp {
            op: top_op,
            left: top_l,
            right: top_r,
        } => {
            assert!(matches!(top_op, BO::Or));

            // левая часть OR — это AND(...)
            match top_l.as_ref() {
                SqlExpr::BinaryOp { op: and_op, .. } => assert!(matches!(and_op, BO::And)),
                other => panic!("expected left to be AND node, got {:?}", other),
            }

            // правая часть OR — это Eq(...)
            match top_r.as_ref() {
                SqlExpr::BinaryOp { op: eq_op, .. } => assert!(matches!(eq_op, BO::Eq)),
                other => panic!("expected right to be Eq node, got {:?}", other),
            }
        }
        other => panic!(
            "expected BinaryOp(OR, BinaryOp(AND, ..), Eq), got {:?}",
            other
        ),
    }
}
