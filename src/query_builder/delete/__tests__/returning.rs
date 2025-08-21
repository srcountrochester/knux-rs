use crate::{
    expression::col,
    query_builder::delete::returning::{
        push_returning_list, set_returning_all, set_returning_all_from, set_returning_one,
    },
};
use smallvec::{SmallVec, smallvec};
use sqlparser::ast::{
    Expr as SqlExpr, Ident, SelectItem, SelectItemQualifiedWildcardKind, WildcardAdditionalOptions,
};

#[test]
fn push_returning_list_appends_items() {
    let mut buf: SmallVec<[SelectItem; 4]> =
        smallvec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];

    // добавляем два выражения
    push_returning_list(&mut buf, (col("a"), col("b"))).expect("ok");

    assert_eq!(buf.len(), 3);
    assert!(matches!(buf[0], SelectItem::Wildcard(_)));

    match &buf[1] {
        SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => assert_eq!(id.value, "a"),
        other => panic!("expected UnnamedExpr(Identifier a), got {:?}", other),
    }
    match &buf[2] {
        SelectItem::UnnamedExpr(SqlExpr::Identifier(id)) => assert_eq!(id.value, "b"),
        other => panic!("expected UnnamedExpr(Identifier b), got {:?}", other),
    }
}

#[test]
fn push_returning_list_empty_error() {
    let mut buf: SmallVec<[SelectItem; 4]> = SmallVec::new();
    let err = push_returning_list(&mut buf, ()).unwrap_err();
    assert!(err.contains("empty list"), "err: {}", err);
    assert!(buf.is_empty());
}

#[test]
fn set_returning_one_replaces_and_keeps_first() {
    let mut buf: SmallVec<[SelectItem; 4]> =
        smallvec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];

    // передаём два элемента — должен быть взят первый
    set_returning_one(&mut buf, (col("x"), col("y"))).expect("ok");

    assert_eq!(buf.len(), 1);
    match &buf[0] {
        SelectItem::UnnamedExpr(SqlExpr::Identifier(Ident { value, .. })) => {
            assert_eq!(value, "x")
        }
        other => panic!("expected UnnamedExpr(Identifier x), got {:?}", other),
    }
}

#[test]
fn set_returning_one_empty_error() {
    let mut buf: SmallVec<[SelectItem; 4]> =
        smallvec![SelectItem::Wildcard(WildcardAdditionalOptions::default())];

    let err = set_returning_one(&mut buf, ()).unwrap_err();
    assert!(err.contains("expected a single expression"), "err: {}", err);

    // буфер не должен измениться
    assert_eq!(buf.len(), 1);
    assert!(matches!(buf[0], SelectItem::Wildcard(_)));
}

#[test]
fn set_returning_all_overwrites_with_star() {
    let mut buf: SmallVec<[SelectItem; 4]> = SmallVec::new();

    // предварительно что-то добавим
    push_returning_list(&mut buf, (col("a"), col("b"))).expect("ok");
    assert_eq!(buf.len(), 2);

    set_returning_all(&mut buf);

    assert_eq!(buf.len(), 1);
    assert!(matches!(buf[0], SelectItem::Wildcard(_)));
}

#[test]
fn set_returning_all_from_simple() {
    let mut buf: SmallVec<[SelectItem; 4]> = SmallVec::new();

    set_returning_all_from(&mut buf, "u");

    assert_eq!(buf.len(), 1);
    match &buf[0] {
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(obj), _) => {
            assert_eq!(obj.to_string(), "u");
        }
        other => panic!("expected QualifiedWildcard(ObjectName u), got {:?}", other),
    }
}

#[test]
fn set_returning_all_from_schema_table() {
    let mut buf: SmallVec<[SelectItem; 4]> = SmallVec::new();

    set_returning_all_from(&mut buf, "s.t");

    assert_eq!(buf.len(), 1);
    match &buf[0] {
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(obj), _) => {
            assert_eq!(obj.to_string(), "s.t");
        }
        other => panic!(
            "expected QualifiedWildcard(ObjectName s.t), got {:?}",
            other
        ),
    }
}
