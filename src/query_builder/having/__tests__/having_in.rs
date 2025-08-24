use super::super::super::*; // QueryBuilder и др.
use crate::expression::helpers::{col, val};
use sqlparser::ast::{BinaryOperator as BO, Expr as SqlExpr, Query, SetExpr};

type QB = QueryBuilder<'static, ()>;

/// Достаём HAVING из AST без клонов.
fn extract_having(q: &Query) -> Option<&SqlExpr> {
    match q.body.as_ref() {
        SetExpr::Select(sel) => sel.having.as_ref(),
        _ => None,
    }
}

#[test]
fn having_in_with_list() {
    let qb = QB::new_empty()
        .from("users")
        .select(("status",))
        .group_by(("status",))
        .having_in(col("status"), (val("new"), val("paid"), val("archived")));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 3, "должно собраться 3 параметра");
    assert!(matches!(params[0], crate::param::Param::Str(ref s) if s == "new"));
    assert!(matches!(params[1], crate::param::Param::Str(ref s) if s == "paid"));
    assert!(matches!(params[2], crate::param::Param::Str(ref s) if s == "archived"));

    let h = extract_having(&q).expect("HAVING must exist");
    assert!(
        matches!(h, SqlExpr::InList { negated: false, .. }),
        "ожидался InList(negated=false), got: {h:?}"
    );
}

#[test]
fn having_in_with_subquery() {
    // подзапрос: SELECT id FROM admins
    let sub = QB::new_empty().from("admins").select(("id",));

    let qb = QB::new_empty()
        .from("users")
        .select(("id",))
        .group_by(("id",))
        .having_in(col("id"), sub);

    let (q, params) = qb.build_query_ast().expect("ok");
    assert!(params.is_empty(), "в подзапросе без val(..) параметров нет");

    let h = extract_having(&q).expect("HAVING must exist");
    assert!(
        matches!(h, SqlExpr::InSubquery { negated: false, .. }),
        "ожидался InSubquery(negated=false), got: {h:?}"
    );
}

#[test]
fn or_having_in_combines_with_or() {
    let qb = QB::new_empty()
        .from("t")
        .select(("status",))
        .group_by(("status",))
        .having_in(col("status"), (val("new"),))
        .or_having_in(col("status"), (val("paid"),));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 2, "ожидалось 2 параметра");

    let h = extract_having(&q).unwrap();
    match h {
        SqlExpr::BinaryOp { op, left, right } => {
            assert_eq!(*op, BO::Or, "верхний оператор должен быть OR");
            assert!(
                matches!(**left, SqlExpr::InList { .. }),
                "левая часть должна быть InList"
            );
            assert!(
                matches!(**right, SqlExpr::InList { .. }),
                "правая часть должна быть InList"
            );
        }
        other => panic!("ожидался BinaryOp(OR), получено: {:?}", other),
    }
}

#[test]
fn having_not_in_with_list() {
    let qb = QB::new_empty()
        .from("users")
        .select(("role",))
        .group_by(("role",))
        .having_not_in(col("role"), (val("banned"), val("suspended")));

    let (q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 2);

    let h = extract_having(&q).expect("HAVING must exist");
    match h {
        SqlExpr::InList { negated, .. } => assert!(*negated, "ожидался NOT IN (negated=true)"),
        other => panic!("ожидался InList, получено: {:?}", other),
    }
}

#[test]
fn having_in_empty_values_records_builder_error() {
    // Пустой список значений должен зарегистрировать ошибку билдера.
    let empty: Vec<&str> = vec![]; // ArgList поддерживает Vec, пустой — кейс ошибки

    let err = QB::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_in(col("x"), empty)
        .to_sql()
        .unwrap_err();

    let msg = err.to_string();
    // Внутри используется общий конструктор IN; достаточно проверить что
    // зафиксирована ошибка про пустой список.
    assert!(
        msg.contains("пустой список"),
        "ожидалась ошибка про пустой список значений, got: {msg}"
    );
}

#[test]
fn having_in_collects_params_order() {
    let qb = QB::new_empty()
        .from("t")
        .select(("x",))
        .group_by(("x",))
        .having_in(col("x"), (val(10), val(20), val(30)));

    let (_q, params) = qb.build_query_ast().expect("ok");
    assert_eq!(params.len(), 3);
    assert!(matches!(params[0], crate::param::Param::I32(10)));
    assert!(matches!(params[1], crate::param::Param::I32(20)));
    assert!(matches!(params[2], crate::param::Param::I32(30)));
}
