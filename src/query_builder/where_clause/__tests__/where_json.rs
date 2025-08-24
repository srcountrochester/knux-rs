use super::extract_where;
use crate::query_builder::QueryBuilder;

type QB = QueryBuilder<'static, ()>;
#[test]
fn where_json_object_basic() {
    let qb = QB::new_empty()
        .from("events")
        .select("*")
        .where_json_object("payload", r#"{"a":1}"#);

    // ожидаем успешную сборку и наличие WHERE
    let (q, _params) = qb.build_query_ast().expect("ok");
    assert!(extract_where(&q).is_some(), "WHERE must be present");
}

#[test]
fn where_json_path_basic() {
    let qb = QB::new_empty()
        .from("events")
        .select("*")
        .where_json_path("payload", "$.user.id");

    let (q, _params) = qb.build_query_ast().expect("ok");
    assert!(extract_where(&q).is_some(), "WHERE must be present");
}

#[test]
fn where_json_superset_of_basic() {
    let qb = QB::new_empty()
        .from("docs")
        .select("*")
        .where_json_superset_of("payload", r#"{"k":"v"}"#);

    let (q, _params) = qb.build_query_ast().expect("ok");
    assert!(extract_where(&q).is_some(), "WHERE must be present");
}

#[test]
fn where_json_subset_of_basic() {
    let qb = QB::new_empty()
        .from("docs")
        .select("*")
        .where_json_subset_of("payload", r#"{"k":"v"}"#);

    let (q, _params) = qb.build_query_ast().expect("ok");
    assert!(extract_where(&q).is_some(), "WHERE must be present");
}

#[test]
fn where_json_object_builds_where() {
    let qb = QB::new_empty()
        .from("events")
        .select("*")
        .where_json_object("payload", r#"{"a":1}"#);

    let (q, _params) = qb.build_query_ast().expect("ok");
    assert!(extract_where(&q).is_some(), "WHERE must be present");
}

#[test]
fn where_json_path_builds_where() {
    let qb = QB::new_empty()
        .from("events")
        .select("*")
        .where_json_path("payload", "$.user.id");

    let (q, _params) = qb.build_query_ast().expect("ok");
    assert!(extract_where(&q).is_some(), "WHERE must be present");
}

#[test]
fn where_json_superset_subset_build_where() {
    let qb1 = QB::new_empty()
        .from("docs")
        .select("*")
        .where_json_superset_of("payload", r#"{"k":"v"}"#);
    let (q1, _) = qb1.build_query_ast().expect("ok");
    assert!(extract_where(&q1).is_some());

    let qb2 = QB::new_empty()
        .from("docs")
        .select("*")
        .where_json_subset_of(r#"{"k":"v"}"#, "payload");
    let (q2, _) = qb2.build_query_ast().expect("ok");
    assert!(extract_where(&q2).is_some());
}

//
// --- Диалект-специфичные проверки ---
//

// MySQL: JSON_CONTAINS / JSON_CONTAINS_PATH
#[cfg(feature = "mysql")]
#[test]
fn mysql_where_json_uses_functions() {
    use sqlparser::ast::Expr as E;

    // JSON_CONTAINS
    let qb1 = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_object("payload", r#"{"a":1}"#);
    let (q1, _) = qb1.build_query_ast().expect("ok");
    match extract_where(&q1).unwrap() {
        E::Function(f) => {
            let name = f.name.to_string();
            assert_eq!(name.to_uppercase(), "JSON_CONTAINS");
        }
        other => panic!("expected Function(JSON_CONTAINS), got {:?}", other),
    }

    // JSON_CONTAINS_PATH
    let qb2 = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_path("payload", "$.a");
    let (q2, _) = qb2.build_query_ast().expect("ok");
    match extract_where(&q2).unwrap() {
        E::Function(f) => {
            let name = f.name.to_string();
            assert_eq!(name.to_uppercase(), "JSON_CONTAINS_PATH");
        }
        other => panic!("expected Function(JSON_CONTAINS_PATH), got {:?}", other),
    }
}

// Postgres: jsonb_path_exists(...) и операторы @>/<@
#[cfg(feature = "postgres")]
#[test]
fn postgres_where_json_path_uses_jsonb_path_exists() {
    use sqlparser::ast::Expr as E;
    let qb = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_path("payload", "$.a");

    let (q, _) = qb.build_query_ast().expect("ok");
    match extract_where(&q).unwrap() {
        E::Function(f) => {
            let name = f.name.to_string();
            assert_eq!(name.to_lowercase(), "jsonb_path_exists");
        }
        other => panic!("expected Function(jsonb_path_exists), got {:?}", other),
    }
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_where_json_object_and_subset_superset_parse() {
    // Здесь не матчим сам оператор @>/ <@, т.к. представление зависит от версии sqlparser.
    let qb1 = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_object("payload", r#"{"a":1}"#);
    let (q1, _) = qb1.build_query_ast().expect("ok");
    assert!(extract_where(&q1).is_some());

    let qb2 = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_superset_of("payload", r#"{"a":1}"#);
    let (q2, _) = qb2.build_query_ast().expect("ok");
    assert!(extract_where(&q2).is_some());

    let qb3 = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_subset_of(r#"{"a":1}"#, "payload");
    let (q3, _) = qb3.build_query_ast().expect("ok");
    assert!(extract_where(&q3).is_some());
}

// SQLite: path → IsNotNull(json_extract(...)), superset/subset → NOT EXISTS(...)
#[cfg(feature = "sqlite")]
#[test]
fn sqlite_where_json_path_is_is_not_null_over_json_extract() {
    use sqlparser::ast::Expr as E;

    let qb = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_path("payload", "$.a");
    let (q, _) = qb.build_query_ast().expect("ok");

    match extract_where(&q).unwrap() {
        E::IsNotNull(inner) => {
            // внутри — вызов функции json_extract(...)
            match inner.as_ref() {
                E::Function(_f) => { /* ок: форма json_extract(...) */ }
                other => panic!(
                    "expected Function(json_extract(...)) inside IsNotNull, got {:?}",
                    other
                ),
            }
        }
        other => panic!("expected IsNotNull(json_extract(...)), got {:?}", other),
    }
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_where_json_superset_subset_are_not_exists() {
    use sqlparser::ast::Expr as E;

    let qb1 = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_superset_of("payload", r#"{"a":1}"#);
    let (q1, _) = qb1.build_query_ast().expect("ok");
    match extract_where(&q1).unwrap() {
        E::Exists { negated, .. } => assert!(*negated, "expected NOT EXISTS for superset"),
        other => panic!("expected NOT EXISTS, got {:?}", other),
    }

    let qb2 = QB::new_empty()
        .from("t")
        .select("*")
        .where_json_subset_of(r#"{"a":1}"#, "payload");
    let (q2, _) = qb2.build_query_ast().expect("ok");
    match extract_where(&q2).unwrap() {
        E::Exists { negated, .. } => assert!(*negated, "expected NOT EXISTS for subset"),
        other => panic!("expected NOT EXISTS, got {:?}", other),
    }
}
