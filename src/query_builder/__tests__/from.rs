use sqlparser::ast::{ObjectNamePart, SetExpr, TableFactor};

use crate::expression::helpers::val;
use crate::param::Param;
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

#[test]
fn from_multiple_plain_tables_with_default_schema() {
    // schema подставляется только для одиночных идентификаторов
    let qb = QB::new_empty()
        .with_default_schema(Some("app".into()))
        .select(("id",))
        .from(("users", "auth.roles", "logs"));

    let (query, params) = qb.build_query_ast().expect("ast");
    assert!(params.is_empty());

    let sel = match *query.body {
        SetExpr::Select(s) => s,
        other => panic!("expected Select, got {:?}", other),
    };

    // ожидаем 3 источника: users, auth.roles, logs
    assert_eq!(sel.from.len(), 3);

    // 0: app.users (подставили schema)
    match &sel.from[0].relation {
        TableFactor::Table { name, .. } => {
            assert_eq!(name.0.len(), 2);
            assert!(matches!(name.0[0], ObjectNamePart::Identifier(ref i) if i.value == "app"));
            assert!(matches!(name.0[1], ObjectNamePart::Identifier(ref i) if i.value == "users"));
        }
        other => panic!("expected TableFactor::Table, got {:?}", other),
    }

    // 1: auth.roles (составной — не меняем)
    match &sel.from[1].relation {
        TableFactor::Table { name, .. } => {
            assert_eq!(name.0.len(), 2);
            assert!(matches!(name.0[0], ObjectNamePart::Identifier(ref i) if i.value == "auth"));
            assert!(matches!(name.0[1], ObjectNamePart::Identifier(ref i) if i.value == "roles"));
        }
        other => panic!("expected TableFactor::Table, got {:?}", other),
    }

    // 2: app.logs (подставили schema)
    match &sel.from[2].relation {
        TableFactor::Table { name, .. } => {
            assert_eq!(name.0.len(), 2);
            assert!(matches!(name.0[0], ObjectNamePart::Identifier(ref i) if i.value == "app"));
            assert!(matches!(name.0[1], ObjectNamePart::Identifier(ref i) if i.value == "logs"));
        }
        other => panic!("expected TableFactor::Table, got {:?}", other),
    }
}

#[test]
fn from_mixed_table_subquery_and_closure_collects_params_and_preserves_order() {
    // subquery 1: SELECT ?
    let sub = QB::new_empty().select((val(10i32),));
    let scalar_subq: QBClosureHelper<()> = |q| q.select((val(20i32),));
    // closure-subquery: SELECT ?
    let qb = QB::new_empty()
        .select(("x",))
        .from(("users", sub, scalar_subq));

    let (query, params) = qb.build_query_ast().expect("ast");
    assert_eq!(params.len(), 2, "params from both subqueries are collected");
    match (&params[0], &params[1]) {
        (Param::I32(a), Param::I32(b)) => assert_eq!((*a, *b), (10, 20)),
        other => panic!("expected [I32(10), I32(20)], got {:?}", other),
    }

    let sel = match *query.body {
        SetExpr::Select(s) => s,
        other => panic!("expected Select, got {:?}", other),
    };
    assert_eq!(sel.from.len(), 3, "three sources in FROM in order");

    // 0: users (Table)
    assert!(matches!(sel.from[0].relation, TableFactor::Table { .. }));
    // 1: subquery (Derived)
    assert!(matches!(sel.from[1].relation, TableFactor::Derived { .. }));
    // 2: closure-subquery (Derived)
    assert!(matches!(sel.from[2].relation, TableFactor::Derived { .. }));
}
