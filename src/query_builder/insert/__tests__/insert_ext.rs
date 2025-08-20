use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;

// Внутренние типы билдера доступны внутри крейта:
use super::super::{ConflictAction, MergeValue};

use sqlparser::ast::{SelectItem, SelectItemQualifiedWildcardKind};

fn names_idents(idents: &[sqlparser::ast::Ident]) -> Vec<String> {
    idents.iter().map(|i| i.value.clone()).collect()
}

#[test]
fn returning_one_sets_single_item_and_overwrites_previous() {
    let ins = QueryBuilder::new_empty()
        .into("users")
        .insert((col("email"), val("a@ex.com")))
        .returning((col("id"), col("email"))) // сначала два
        .returning_one((col("id"),)); // затем один

    assert_eq!(ins.returning.len(), 1);
    match &ins.returning[0] {
        SelectItem::UnnamedExpr(expr) => {
            // должно быть именно выражение (id)
            let s = format!("{expr:?}");
            assert!(
                s.contains("Identifier") && s.contains("id"),
                "unexpected expr: {s}"
            );
        }
        other => panic!("expected UnnamedExpr, got {other:?}"),
    }
}

#[test]
fn returning_all_sets_wildcard() {
    let ins = QueryBuilder::new_empty()
        .into("tags")
        .columns((col("name"),))
        .insert((val("red"),))
        .returning_all();

    assert_eq!(ins.returning.len(), 1);
    matches!(&ins.returning[0], SelectItem::Wildcard(_))
        .then_some(())
        .expect("RETURNING * must be a Wildcard");
}

#[test]
fn returning_all_from_builds_qualified_wildcard() {
    let ins = QueryBuilder::new_empty()
        .into("t")
        .insert((col("id"), val(1_i32)))
        .returning_all_from("t");

    assert_eq!(ins.returning.len(), 1);
    match &ins.returning[0] {
        SelectItem::QualifiedWildcard(kind, _opts) => {
            match kind {
                SelectItemQualifiedWildcardKind::ObjectName(obj) => {
                    // "t.*" или "schema.t.*" — тут проверим хотя бы последний сегмент
                    let parts = obj.0.iter().map(|p| p.to_string()).collect::<Vec<_>>();
                    assert!(
                        parts.last() == Some(&"t".to_string()),
                        "unexpected qualifier: {:?}",
                        parts
                    );
                }
                other => panic!("expected ObjectName kind, got {other:?}"),
            }
        }
        other => panic!("expected QualifiedWildcard, got {other:?}"),
    }
}

#[test]
fn on_conflict_sets_target_columns() {
    let ins = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"), col("name")))
        .insert((val("a@ex.com"), val("Alice")))
        .on_conflict((col("email"),));

    let spec = ins.on_conflict.as_ref().expect("on_conflict must be set");
    assert_eq!(names_idents(&spec.target_columns), vec!["email"]);
    assert!(
        spec.action.is_none(),
        "action is not set until ignore()/merge()"
    );
}

#[test]
fn ignore_sets_insert_ignore_and_action_do_nothing_when_target_present() {
    let ins = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"),))
        .insert((val("a@ex.com"),))
        .on_conflict((col("email"),))
        .ignore();

    assert!(ins.insert_ignore, "insert_ignore flag must be set");

    let spec = ins
        .on_conflict
        .as_ref()
        .expect("on_conflict must be present");
    match spec.action.as_ref().expect("action must be set") {
        ConflictAction::DoNothing => {}
        other => panic!("expected DoNothing, got {other:?}"),
    }
}

#[test]
fn merge_columns_only_short_form_maps_to_from_inserted() {
    let ins = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"), col("name"), col("age")))
        .insert((val("a@ex.com"), val("Alice"), val(33_i32)))
        .on_conflict((col("email"),))
        .merge((col("name"), col("age"))); // короткая форма

    let spec = ins
        .on_conflict
        .as_ref()
        .expect("on_conflict")
        .action
        .as_ref()
        .expect("action");
    match spec {
        ConflictAction::DoUpdate {
            set,
            where_predicate,
        } => {
            assert!(where_predicate.is_none());
            // Должны быть назначены только name и age
            let cols: Vec<_> = set.iter().map(|a| a.col.value.clone()).collect();
            assert_eq!(cols, vec!["name".to_string(), "age".to_string()]);
            // И оба значения — FromInserted соответствующего столбца
            for a in set {
                match &a.value {
                    MergeValue::FromInserted(id) => assert_eq!(id.value, a.col.value),
                    other => panic!("expected FromInserted, got {other:?}"),
                }
            }
        }
        other => panic!("expected DoUpdate, got {other:?}"),
    }
}

#[test]
fn merge_pairs_col_value_collects_params_and_builds_expr_values() {
    // два присваивания: v = $1, ts = $2
    let ins = QueryBuilder::new_empty()
        .into("kv")
        .columns((col("k"), col("v"), col("ts")))
        .insert((val("lang"), val("ru"), val(1_726_500_000_i64)))
        .on_conflict((col("k"),))
        .merge((col("v"), val("en"), col("ts"), val(1_800_000_000_i64)));

    // Параметры из merge() складываются в общий буфер билдера
    assert!(
        ins.params.len() >= 2,
        "expected params from merge() expressions"
    );

    let spec = ins.on_conflict.as_ref().unwrap().action.as_ref().unwrap();
    match spec {
        ConflictAction::DoUpdate { set, .. } => {
            assert_eq!(set.len(), 2);
            // оба — Expr(...)
            for a in set {
                matches!(&a.value, MergeValue::Expr(_))
                    .then_some(())
                    .expect("MergeValue must be Expr for col/value pairs");
            }
        }
        other => panic!("expected DoUpdate, got {other:?}"),
    }
}

#[test]
fn merge_all_requires_known_columns_and_populates_assignments() {
    // вариант без известных колонок => ошибка
    let ins_err = QueryBuilder::new_empty()
        .into("t")
        .on_conflict((col("id"),))
        .merge_all();
    assert!(
        !ins_err.builder_errors.is_empty(),
        "merge_all() without known columns should record an error"
    );

    // вариант с известными колонками
    let ins_ok = QueryBuilder::new_empty()
        .into("t")
        .columns((col("a"), col("b")))
        .insert((val(1_i32), val(2_i32)))
        .on_conflict((col("a"),))
        .merge_all();

    let spec = ins_ok
        .on_conflict
        .as_ref()
        .unwrap()
        .action
        .as_ref()
        .unwrap();
    match spec {
        ConflictAction::DoUpdate { set, .. } => {
            assert_eq!(
                set.len(),
                2,
                "merge_all must generate assignments for all columns"
            );
            for a in set {
                match &a.value {
                    MergeValue::FromInserted(id) => assert_eq!(id.value, a.col.value),
                    other => panic!("expected FromInserted, got {other:?}"),
                }
            }
        }
        other => panic!("expected DoUpdate, got {other:?}"),
    }
}

#[test]
fn combine_upsert_with_returning_variants() {
    // RETURNING один столбец
    let one = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"), col("name")))
        .insert((val("a@ex.com"), val("Alice")))
        .on_conflict((col("email"),))
        .merge((col("name"),))
        .returning_one((col("id"),));
    assert_eq!(one.returning.len(), 1);

    // RETURNING *
    let all = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"),))
        .insert((val("a@ex.com"),))
        .ignore()
        .returning_all();
    matches!(&all.returning[0], SelectItem::Wildcard(_))
        .then_some(())
        .expect("RETURNING * must be Wildcard");

    // RETURNING t.*
    let q = QueryBuilder::new_empty()
        .into("t")
        .insert((col("id"), val(1_i32)))
        .returning_all_from("t");
    match &q.returning[0] {
        SelectItem::QualifiedWildcard(kind, _) => {
            use sqlparser::ast::SelectItemQualifiedWildcardKind as K;
            match kind {
                K::ObjectName(obj) => {
                    let last = obj.0.last().expect("non-empty");
                    assert_eq!(last.to_string(), "t".to_string()); // ← ровно то, что нужно
                }
                other => panic!("expected ObjectName kind, got {other:?}"),
            }
        }
        other => panic!("expected QualifiedWildcard, got {other:?}"),
    }
}
