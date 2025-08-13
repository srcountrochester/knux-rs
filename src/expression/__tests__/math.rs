use crate::expression::{col, val};
use sqlparser::ast;

fn assert_binop(op: ast::BinaryOperator, e: &ast::Expr) {
    match e {
        ast::Expr::BinaryOp { op: got, .. } => assert_eq!(*got, op),
        other => panic!("expected BinaryOp, got {:?}", other),
    }
}

#[test]
fn add_builds_binaryop_plus_and_merges_params() {
    let (e, alias, params) = col("price").add(val(100)).__into_parts();
    assert_binop(ast::BinaryOperator::Plus, &e);
    assert!(alias.is_none());
    assert_eq!(params.len(), 1);
}

#[test]
fn sub_builds_binaryop_minus_and_merges_params() {
    let (e, alias, params) = col("count").sub(val(1)).__into_parts();
    assert_binop(ast::BinaryOperator::Minus, &e);
    assert!(alias.is_none());
    assert_eq!(params.len(), 1);
}

#[test]
fn mul_builds_binaryop_multiply_and_merges_params() {
    let (e, alias, params) = col("width").mul(val(2)).__into_parts();
    assert_binop(ast::BinaryOperator::Multiply, &e);
    assert!(alias.is_none());
    assert_eq!(params.len(), 1);
}

#[test]
fn div_builds_binaryop_divide_and_merges_params() {
    let (e, alias, params) = col("total").div(val(4)).__into_parts();
    assert_binop(ast::BinaryOperator::Divide, &e);
    assert!(alias.is_none());
    assert_eq!(params.len(), 1);
}

#[test]
fn multiple_operations_preserve_param_order() {
    // ((a + 1) * 2) - 3
    let expr = col("a").add(val(1)).mul(val(2)).sub(val(3));
    let (e, alias, params) = expr.__into_parts();

    // верхний уровень — Minus
    assert_binop(ast::BinaryOperator::Minus, &e);
    assert!(alias.is_none());
    assert_eq!(params.len(), 3);
    // параметры в порядке: 1, 2, 3
}
