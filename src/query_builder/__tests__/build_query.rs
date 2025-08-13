use crate::query_builder::QueryBuilder;
use sqlparser::ast::{SelectItem, SetExpr};

#[test]
fn build_query_ast_defaults_to_wildcard_projection() {
    let (query, params) = QueryBuilder::new_empty().build_query_ast().expect("ok");

    assert!(params.is_empty());

    match &*query.body {
        SetExpr::Select(select) => {
            let sel = select.as_ref();
            assert_eq!(sel.projection.len(), 1);

            match &sel.projection[0] {
                SelectItem::Wildcard(_) => {}
                other => panic!("expected SelectItem::Wildcard, got {:?}", other),
            }

            assert!(sel.from.is_empty());
            assert!(sel.selection.is_none());
        }
        other => panic!("expected SetExpr::Select, got {:?}", other),
    }
}
