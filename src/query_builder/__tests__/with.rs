use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use crate::query_builder::error::Error;
use crate::tests::dialect_test_helpers::qi;
use crate::type_helpers::QBClosureHelper;
use sqlparser::ast::CteAsMaterialized;

type QB = QueryBuilder<'static, ()>;

fn norm(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn with_pushes_cte_and_bubbles_params() {
    // WITH cte AS (SELECT $1) SELECT "id" FROM "cte"
    let qb = QB::new_empty()
        .with::<QBClosureHelper<()>>("cte", |q| q.select((val(100i32),)))
        .from("cte")
        .select((col("id"),));

    // 1) AST: WITH присутствует, один CTE с именем "cte"
    let (query, params) = qb.build_query_ast().expect("build ok");
    assert_eq!(params.len(), 1, "param from CTE must bubble up");

    let with = query.with.expect("WITH must be present");
    assert!(!with.recursive, "recursive must be false by default");
    assert_eq!(with.cte_tables.len(), 1);
    let cte = &with.cte_tables[0];
    assert_eq!(cte.alias.name.value, "cte");
    assert!(cte.from.is_none());
    assert!(cte.materialized.is_none());

    // 2) SQL: есть WITH, имя CTE корректно квотится, FROM "cte" используется
    let (sql, _p) = QB::new_empty()
        .with::<QBClosureHelper<()>>("cte", |q| q.select((val(100i32),)))
        .from("cte")
        .select((col("id"),))
        .to_sql()
        .expect("to_sql");
    let s = norm(&sql).to_uppercase();
    assert!(s.starts_with("WITH "), "expected WITH..., got: {s}");
    assert!(s.contains(&format!("WITH {} AS (", qi("cte").to_uppercase())));
    assert!(s.contains(&format!("FROM {}", qi("cte").to_uppercase())));
}

#[test]
fn with_recursive_sets_flag_and_renders_recursive() {
    let (query, _params) = QB::new_empty()
        .with_recursive::<QBClosureHelper<()>>("r", |q| q.select((val(1i32),)))
        .select((col("x"),))
        .from("r")
        .build_query_ast()
        .expect("build ok");

    let w = query.with.expect("WITH missing");
    assert!(w.recursive, "recursive flag must be set");

    // Рендер должен содержать WITH RECURSIVE
    let (sql, _p) = QB::new_empty()
        .with_recursive::<QBClosureHelper<()>>("r", |q| q.select((val(1i32),)))
        .select((col("x"),))
        .from("r")
        .to_sql()
        .expect("to_sql");
    let s = norm(&sql).to_uppercase();
    assert!(
        s.starts_with("WITH RECURSIVE "),
        "expected WITH RECURSIVE..., got: {s}"
    );
}

#[test]
fn with_materialized_and_not_materialized_set_ast_only() {
    // MATERIALIZED
    let (q1, _p1) = QB::new_empty()
        .with_materialized::<QBClosureHelper<()>>("m", |q| q.select((val(1i32),)))
        .build_query_ast()
        .expect("build ok");
    let w1 = q1.with.expect("WITH missing");
    assert_eq!(w1.cte_tables.len(), 1);
    assert!(matches!(
        w1.cte_tables[0].materialized,
        Some(CteAsMaterialized::Materialized)
    ));

    // NOT MATERIALIZED
    let (q2, _p2) = QB::new_empty()
        .with_not_materialized::<QBClosureHelper<()>>("n", |q| q.select((val(1i32),)))
        .build_query_ast()
        .expect("build ok");
    let w2 = q2.with.expect("WITH missing");
    assert!(matches!(
        w2.cte_tables[0].materialized,
        Some(CteAsMaterialized::NotMaterialized)
    ));

    // Примечание: текущий рендер не печатает MATERIALIZED/NOT MATERIALIZED —
    // проверяем именно AST (см. маппинг в renderer::map).
}

#[test]
fn with_from_sets_from_ident_in_ast() {
    let (q, _p) = QB::new_empty()
        .with_from::<QBClosureHelper<()>>("t", "base", |q| q.select((col("id"),)))
        .build_query_ast()
        .expect("build ok");

    let w = q.with.expect("WITH missing");
    let cte = &w.cte_tables[0];
    assert_eq!(cte.alias.name.value, "t");
    assert_eq!(cte.from.as_ref().map(|i| i.value.as_str()), Some("base"));
}

#[test]
fn with_rejects_expression_body_and_reports_builder_error() {
    // Передача Expr вместо подзапроса должна давать builder error
    let qb = QB::new_empty().with("bad", (col("id"),));
    let err = qb.build_query_ast().err().expect("expected builder error");
    match err {
        Error::BuilderErrors(_) => { /* ok */ }
        other => panic!("expected BuilderErrors, got {:?}", other),
    }
}
