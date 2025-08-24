use super::super::args::{ArgList, IntoQBArg, QBArg, collect_args};
use crate::expression::helpers::col;
use crate::param::Param;
use crate::query_builder::{Error, QueryBuilder, Result};
use crate::type_helpers::QBClosureHelper;
use sqlparser::ast;

#[test]
fn into_qb_arg_from_str_and_string() {
    let arg1: QBArg = "users.id".into_qb_arg();
    match arg1 {
        QBArg::Expr(expr) => {
            assert!(matches!(expr.expr, ast::Expr::CompoundIdentifier(_)));
        }
        _ => panic!("Expected QBArg::Expr"),
    }

    let arg2: QBArg = String::from("name").into_qb_arg();
    match arg2 {
        QBArg::Expr(expr) => {
            assert!(matches!(expr.expr, ast::Expr::Identifier(_)));
        }
        _ => panic!("Expected QBArg::Expr"),
    }
}

#[test]
fn into_qb_arg_from_expression() {
    let expr = col("foo");
    let arg: QBArg = expr.clone().into_qb_arg();
    match arg {
        QBArg::Expr(e) => assert_eq!(format!("{:?}", e.expr), format!("{:?}", expr.expr)),
        _ => panic!("Expected QBArg::Expr"),
    }
}

#[test]
fn into_qb_arg_from_querybuilder() {
    let qb = QueryBuilder::new_empty();
    let arg: QBArg = qb.into_qb_arg();
    assert!(matches!(arg, QBArg::Subquery(_)));
}

#[test]
fn into_qb_arg_from_closure() {
    let scalar_subq: QBClosureHelper<()> = |q| q;
    let closure_arg: QBArg = scalar_subq.into_qb_arg();
    assert!(matches!(closure_arg, QBArg::Closure(_)));
}

#[test]
fn try_into_expr_success_for_expr() {
    let expr = col("foo");
    let arg: QBArg = expr.into_qb_arg();
    let (ast_expr, params) = arg.try_into_expr().expect("Should succeed");
    assert!(matches!(ast_expr, ast::Expr::Identifier(_)));
    assert!(params.is_empty());
}

#[test]
fn try_into_expr_fails_for_subquery_and_closure() {
    let qb = QueryBuilder::new_empty();
    let subq_arg: QBArg = qb.into_qb_arg();
    let err = subq_arg.try_into_expr().unwrap_err();
    assert!(matches!(err, Error::InvalidExpression { .. }));

    let scalar_subq: QBClosureHelper<()> = |q| q;
    let clos_arg: QBArg = scalar_subq.into_qb_arg();
    let err2 = clos_arg.try_into_expr().unwrap_err();
    assert!(matches!(err2, Error::InvalidExpression { .. }));
}

#[test]
fn resolve_into_expr_with_works_for_expr() {
    let expr = col("foo");
    let arg: QBArg = expr.into_qb_arg();
    let (ast_expr, params) = arg
        .resolve_into_expr_with(|_| panic!("Should not be called"))
        .expect("ok");
    assert!(matches!(ast_expr, ast::Expr::Identifier(_)));
    assert!(params.is_empty());
}

#[test]
fn resolve_into_expr_with_for_subquery_and_closure() {
    fn dummy_builder(_: QueryBuilder) -> Result<(ast::Query, Vec<Param>)> {
        Ok((
            ast::Query {
                with: None,
                body: Box::new(ast::SetExpr::Values(ast::Values {
                    explicit_row: false,
                    rows: vec![],
                })),
                order_by: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                format_clause: None,
                limit_clause: None,
                pipe_operators: vec![],
                settings: None,
            },
            vec![Param::I32(42)],
            // smallvec![Param::I32(42)],
        ))
    }

    // Subquery
    let qb = QueryBuilder::new_empty();
    let subq_arg: QBArg = qb.into_qb_arg();
    let (expr, params) = subq_arg.resolve_into_expr_with(dummy_builder).unwrap();
    assert!(matches!(expr, ast::Expr::Subquery(_)));
    assert_eq!(params.len(), 1);

    // Closure
    let scalar_subq: QBClosureHelper<()> = |q| q;
    let clos_arg: QBArg = scalar_subq.into_qb_arg();
    let (expr2, params2) = clos_arg.resolve_into_expr_with(dummy_builder).unwrap();
    assert!(matches!(expr2, ast::Expr::Subquery(_)));
    assert_eq!(params2.len(), 1);
}

#[test]
fn collect_args_mixed_inputs() {
    let scalar_subq: QBClosureHelper<()> = |q| q;
    let args = collect_args(vec![
        "id".into_qb_arg(),
        col("foo").into_qb_arg(),
        QueryBuilder::new_empty().into_qb_arg(),
        scalar_subq.into_qb_arg(),
    ]);
    assert_eq!(args.len(), 4);
    assert!(matches!(args[0], QBArg::Expr(_)));
    assert!(matches!(args[1], QBArg::Expr(_)));
    assert!(matches!(args[2], QBArg::Subquery(_)));
    assert!(matches!(args[3], QBArg::Closure(_)));
}

#[test]
fn tuple_variadic_mixed_types() {
    let scalar_subq: QBClosureHelper<()> = |q| q.select(("x",));

    let t = ("id", col("name"), scalar_subq);
    let args = t.into_vec();
    assert_eq!(args.len(), 3);
    assert!(matches!(args[0], QBArg::Expr(_)));
    assert!(matches!(args[1], QBArg::Expr(_)));
    assert!(matches!(args[2], QBArg::Closure(_)));
}

#[test]
fn vec_of_strs_and_slice_of_strs() {
    let v = vec!["id", "age"];
    let args = v.into_vec();
    assert!(matches!(args[0], QBArg::Expr(_)));
    assert!(matches!(args[1], QBArg::Expr(_)));

    let s: &[&str] = &["a", "b", "c"];
    let args2 = s.into_vec();
    assert_eq!(args2.len(), 3);
    assert!(matches!(args2[2], QBArg::Expr(_)));
}

#[test]
fn into_qbarg_still_works_explicitly() {
    let scalar_subq: QBClosureHelper<()> = |q| q;
    let args: Vec<QBArg> = vec![
        "id".into_qb_arg(),
        col("foo").into_qb_arg(),
        QueryBuilder::new_empty().into_qb_arg(),
        scalar_subq.into_qb_arg(),
    ];
    assert_eq!(args.len(), 4);
    assert!(matches!(args[0], QBArg::Expr(_)));
    assert!(matches!(args[2], QBArg::Subquery(_)));
    assert!(matches!(args[3], QBArg::Closure(_)));
}
