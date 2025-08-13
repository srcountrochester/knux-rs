use crate::expression::{col, val};
use sqlparser::ast;

fn assert_binop(op: ast::BinaryOperator, e: &ast::Expr) {
    match e {
        ast::Expr::BinaryOp { op: got, .. } => assert!(matches!(got, o if *o == op)),
        other => panic!("expected BinaryOp, got: {:?}", other),
    }
}

#[test]
fn eq_builds_binaryop_and_binds_param() {
    let (e, alias, params) = col("age").eq(val(18)).__into_parts();
    assert_binop(ast::BinaryOperator::Eq, &e);
    // alias не устанавливается
    assert!(alias.is_none());
    // один параметр 18 должен попасть в params
    assert_eq!(params.len(), 1);
}

#[test]
fn ne_gt_gte_lt_lte_all_build_binaryops() {
    let (e1, _, _) = col("a").ne(val(1)).__into_parts();
    assert_binop(ast::BinaryOperator::NotEq, &e1);

    let (e2, _, _) = col("a").gt(val(1)).__into_parts();
    assert_binop(ast::BinaryOperator::Gt, &e2);

    let (e3, _, _) = col("a").gte(val(1)).__into_parts();
    assert_binop(ast::BinaryOperator::GtEq, &e3);

    let (e4, _, _) = col("a").lt(val(1)).__into_parts();
    assert_binop(ast::BinaryOperator::Lt, &e4);

    let (e5, _, _) = col("a").lte(val(1)).__into_parts();
    assert_binop(ast::BinaryOperator::LtEq, &e5);
}

#[test]
fn in_list_collects_params_and_sets_flag() {
    let items = vec![val(10), val(20), val(30)];
    let (e, alias, params) = col("id").isin(items).__into_parts();

    match e {
        ast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            // слева — наше исходное выражение
            assert!(matches!(*expr, ast::Expr::Identifier(_)));
            // три элемента в списке
            assert_eq!(list.len(), 3);
            // IN ( ... ), не NOT IN
            assert!(!negated);
        }
        other => panic!("expected Expr::InList, got {:?}", other),
    }

    // alias не ставится
    assert!(alias.is_none());
    // все три значения попали в params в исходном порядке
    assert_eq!(params.len(), 3);
}

#[test]
fn not_in_list_sets_negated_and_params() {
    let items = vec![val("x"), val("y")];
    let (e, _, params) = col("tag").notin(items).__into_parts();

    match e {
        ast::Expr::InList {
            expr,
            list,
            negated,
        } => {
            assert!(matches!(*expr, ast::Expr::Identifier(_)));
            assert_eq!(list.len(), 2);
            assert!(negated);
        }
        other => panic!("expected Expr::InList, got {:?}", other),
    }

    assert_eq!(params.len(), 2);
}

#[test]
fn is_null_and_is_not_null_have_no_params() {
    let (e1, alias1, params1) = col("deleted_at").is_null().__into_parts();
    match e1 {
        ast::Expr::IsNull(inner) => assert!(matches!(*inner, ast::Expr::Identifier(_))),
        other => panic!("expected Expr::IsNull, got {:?}", other),
    }
    assert!(alias1.is_none());
    assert!(params1.is_empty());

    let (e2, alias2, params2) = col("deleted_at").is_not_null().__into_parts();
    match e2 {
        ast::Expr::IsNotNull(inner) => assert!(matches!(*inner, ast::Expr::Identifier(_))),
        other => panic!("expected Expr::IsNotNull, got {:?}", other),
    }
    assert!(alias2.is_none());
    assert!(params2.is_empty());
}

#[test]
fn params_order_is_left_then_right_for_binaryops() {
    // проверяем порядок слияния params: сначала слева, потом справа
    let (e, _, params) = col("a").gt(val(1)).lt(val(2)).__into_parts();
    // верхний бинарный op — должен быть Lt (потому что (a > 1) LT 2)
    assert_binop(ast::BinaryOperator::Lt, &e);
    // две константы-параметра
    assert_eq!(params.len(), 2);
}

#[test]
fn in_list_collects_params_from_expressions() {
    use crate::expression::{col, val};
    use sqlparser::ast;

    // элементы списка — сложные выражения с bind-параметрами
    let items = vec![
        col("a").add(val(1)), // +1 => 1 параметр
        col("b").sub(val(2)), // -2 => 1 параметр
    ];
    let (e, _, params) = col("id").isin(items).__into_parts();

    match e {
        ast::Expr::InList { list, .. } => assert_eq!(list.len(), 2),
        _ => panic!("expected InList"),
    }
    // оба параметра собрались
    assert_eq!(params.len(), 2);
}
