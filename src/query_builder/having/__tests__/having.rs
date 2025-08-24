use super::super::*;
use crate::{
    expression::helpers::{col, val},
    tests::dialect_test_helpers::qi,
};
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, Query, SetExpr};

type QB = QueryBuilder<'static, ()>;

/// Достаём HAVING из AST
fn extract_having(q: &Query) -> Option<&SqlExpr> {
    match q.body.as_ref() {
        SetExpr::Select(sel) => sel.having.as_ref(),
        _ => None,
    }
}

#[test]
fn having_single_expr() {
    let qb = QB::new_empty()
        .from("orders")
        .select(("*",))
        .group_by(("user_id",))
        .having(col("total").gt(val(100)));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 1, "param from val(100) must be collected");
    let h = extract_having(&q).expect("HAVING must exist");
    match h {
        SqlExpr::BinaryOp { op, .. } => assert_eq!(*op, BO::Gt),
        other => panic!("expected BinaryOp in HAVING, got {:?}", other),
    }
}

#[test]
fn having_arglist_combines_with_and() {
    let qb = QB::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having((col("cnt").gt(val(1)), col("cnt").lt(val(10))));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 2);

    let h = extract_having(&q).unwrap();
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::And, "right side must be AND");
            // обе стороны — бинарные сравнения
            assert!(matches!(**left, SqlExpr::BinaryOp { .. }));
            assert!(matches!(**right, SqlExpr::BinaryOp { .. }));
        }
        other => panic!("expected BinaryOp, got {:?}", other),
    }
}

#[test]
fn and_having_and_or_having() {
    let qb = QB::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having(col("sum").gt(val(10)))
        .and_having((col("sum").lt(val(100)),)) // эквивалент ещё одного having()
        .or_having(col("cnt").gt(val(1)));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 3);

    let h = extract_having(&q).unwrap();
    // верхний узел должен быть OR
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or);
            assert!(matches!(**right, SqlExpr::BinaryOp { .. })); // cnt > 1
            // левая часть — цепочка с AND
            match left.as_ref() {
                SqlExpr::BinaryOp { op, .. } => assert_eq!(*op, BO::And),
                other => panic!("left must be AND-chain, got {:?}", other),
            }
        }
        other => panic!("expected BinaryOp(OR) at top, got {:?}", other),
    }
}

#[test]
fn having_raw_parses_and_or_having_raw() {
    use sqlparser::ast::{BinaryOperator as BO, Expr as E};

    let qb = QB::new_empty()
        .from("t")
        .select((qi("x"),)) // имя не критично
        .group_by((qi("x"),))
        .having_raw("COUNT(x) > 1")
        .or_having_raw("SUM(x) < 100");

    let (q, _params) = qb.build_query_ast().expect("ok");
    let h = extract_having(&q).unwrap();

    // верх — OR между двумя бинарными сравнениями
    match h {
        E::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or);

            // левая часть: COUNT(x) > 1
            match left.as_ref() {
                E::BinaryOp {
                    left: l_fun,
                    op: l_op,
                    right: l_val,
                } => {
                    assert!(
                        matches!(l_fun.as_ref(), E::Function(_)),
                        "left side must start with Function(..)"
                    );
                    assert_eq!(*l_op, BO::Gt, "left comparison must be >");
                    // правую часть можно не детализировать, это литерал/число
                    assert!(
                        !matches!(l_val.as_ref(), E::Function(_)),
                        "right side should not be Function"
                    );
                }
                other => panic!("left must be BinaryOp(Function ?, ?), got {:?}", other),
            }

            // правая часть: SUM(x) < 100
            match right.as_ref() {
                E::BinaryOp {
                    left: r_fun,
                    op: r_op,
                    right: r_val,
                } => {
                    assert!(
                        matches!(r_fun.as_ref(), E::Function(_)),
                        "right side must start with Function(..)"
                    );
                    assert_eq!(*r_op, BO::Lt, "right comparison must be <");
                    assert!(
                        !matches!(r_val.as_ref(), E::Function(_)),
                        "right value should not be Function"
                    );
                }
                other => panic!("right must be BinaryOp(Function ?, ?), got {:?}", other),
            }
        }
        other => panic!("expected OR between BinaryOp comparisons, got {:?}", other),
    }
}

#[test]
fn having_raw_records_parse_error() {
    // некорректный raw должен дать ошибку билдера
    let err = QB::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_raw("a =") // синтаксическая ошибка
        .to_sql()
        .unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("having_raw():"),
        "expected having_raw parse error, got: {msg}"
    );
}
