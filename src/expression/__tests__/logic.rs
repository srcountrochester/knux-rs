use crate::expression::{col, val};
use sqlparser::ast;

fn assert_binop(op: ast::BinaryOperator, e: &ast::Expr) {
    match e {
        ast::Expr::BinaryOp { op: got, .. } => assert!(matches!(got, o if *o == op)),
        other => panic!("expected BinaryOp, got: {:?}", other),
    }
}

#[test]
fn and_builds_binaryop_and_merges_params() {
    let (e, alias, params) = col("age")
        .gt(val(18))
        .and(col("active").eq(val(true)))
        .__into_parts();
    // верхний оператор — AND
    assert_binop(ast::BinaryOperator::And, &e);
    // алиас не устанавливается
    assert!(alias.is_none());
    // два параметра: 18 и true
    assert_eq!(params.len(), 2);
}

#[test]
fn or_builds_binaryop_and_merges_params() {
    let (e, alias, params) = col("role")
        .eq(val("admin"))
        .or(col("role").eq(val("owner")))
        .__into_parts();
    assert_binop(ast::BinaryOperator::Or, &e);
    assert!(alias.is_none());
    assert_eq!(params.len(), 2);
}

#[test]
fn not_wraps_unary_and_keeps_params() {
    let (e, alias, params) = col("deleted_at").is_null().not().__into_parts();
    match e {
        ast::Expr::UnaryOp { op, expr } => {
            assert!(matches!(op, ast::UnaryOperator::Not));
            // внутри — IsNull(...)
            assert!(matches!(*expr.clone(), ast::Expr::IsNull(_)));
        }
        other => panic!("expected UnaryOp(Not), got {:?}", other),
    }
    assert!(alias.is_none());
    assert!(params.is_empty(), "NOT не должен добавлять параметры");
}

#[test]
fn chaining_and_or_preserves_structure() {
    // (a > 1) AND (b < 5) OR (c = 10)
    let expr = col("a")
        .gt(val(1))
        .and(col("b").lt(val(5)))
        .or(col("c").eq(val(10)));
    let (e, _, params) = expr.__into_parts();

    // верхний уровень — OR
    assert_binop(ast::BinaryOperator::Or, &e);

    // всего три значения-параметра
    assert_eq!(params.len(), 3);
}

#[test]
fn params_order_left_then_right() {
    // проверяем порядок слияния параметров: сначала слева, потом справа
    let (e, _, params) = val(1).and(val(2)).or(val(3)).__into_parts();

    // верхний — OR
    assert_binop(ast::BinaryOperator::Or, &e);
    // три параметра: [1, 2, 3]
    assert_eq!(params.len(), 3);
}

#[test]
fn not_preserves_params_inside() {
    use crate::expression::{col, val};
    use sqlparser::ast;

    let expr = col("a").gt(val(10)).and(col("b").lt(val(20))).not();
    let (e, _, params) = expr.__into_parts();

    match e {
        ast::Expr::UnaryOp { op, .. } => assert!(matches!(op, ast::UnaryOperator::Not)),
        _ => panic!("expected UnaryOp(Not)"),
    }
    assert_eq!(params.len(), 2);
}
