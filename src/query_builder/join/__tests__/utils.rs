use super::super::super::error::Error;
use super::super::utils::*;
use super::super::*;
use crate::expression::helpers::val;
use sqlparser::ast::{Expr as SqlExpr, JoinConstraint, Value};

type QB = QueryBuilder<'static, ()>;

#[test]
fn clone_params_copies_params() {
    // берём любое выражение; val(..) обычно добавляет Param
    let e: Expression = val(123);
    let cloned = clone_params(&e);
    assert_eq!(cloned.len(), e.params.len());
    // Не требуем non-empty — зависит от реализации val(..)
}

#[test]
fn clone_params_from_expr_copies_params() {
    let e: Expression = val(true);
    let cloned = clone_params_from_expr(&e);
    assert_eq!(cloned.len(), e.params.len());
}

#[test]
fn must_have_constraint_none_adds_error_and_returns_none() {
    let mut qb = QB::new_empty();

    let out = must_have_constraint("LEFT JOIN", None, &mut qb);
    assert!(out.is_none(), "expected None constraint when input is None");

    // Ошибка должна агрегироваться в BuilderErrors
    let err = qb.build_query_ast().unwrap_err();
    match err {
        Error::BuilderErrors(list) => {
            let s = list.to_string();
            assert!(
                s.contains("LEFT JOIN") && s.contains("требуется ON-условие"),
                "unexpected error text: {s}"
            );
        }
        other => panic!("expected BuilderErrors, got {:?}", other),
    }
}

#[test]
fn must_have_constraint_some_passthrough() {
    let mut qb = QB::new_empty();
    let jc = JoinConstraint::On(SqlExpr::Value(Value::Boolean(true).into()));

    let out = must_have_constraint("INNER JOIN", Some(jc.clone()), &mut qb);
    assert!(matches!(out, Some(_)));

    // Не должно быть ошибок билдера
    let res = qb.build_query_ast();
    assert!(res.is_ok(), "unexpected builder error: {:?}", res.err());
}
