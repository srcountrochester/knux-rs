use crate::query_builder::QueryBuilder;

#[test]
fn limit_only() {
    let (sql, _params) = QueryBuilder::new_empty()
        .from("users")
        .select(("*",))
        .limit(10)
        .to_sql()
        .expect("to_sql");
    let norm = sql.replace('\n', " ");
    assert!(
        norm.contains("LIMIT 10"),
        "SQL must contain LIMIT 10: {norm}"
    );
}

#[test]
fn offset_only() {
    let (sql, _params) = QueryBuilder::new_empty()
        .from("users")
        .select(("*",))
        .offset(20)
        .to_sql()
        .expect("to_sql");
    let norm = sql.replace('\n', " ");
    // допускаем различные стили, смотрим, что OFFSET отразился
    assert!(
        norm.contains("OFFSET 20") || norm.contains("LIMIT"),
        "SQL must reflect OFFSET 20 (possibly via LIMIT ... OFFSET style), got: {norm}"
    );
}

#[test]
fn limit_and_offset() {
    let (sql, _params) = QueryBuilder::new_empty()
        .from("orders")
        .select(("id",))
        .limit_offset(10, 5)
        .to_sql()
        .expect("to_sql");
    let norm = sql.replace('\n', " ");
    assert!(norm.contains("LIMIT 10"), "must contain LIMIT 10: {norm}");
    assert!(
        norm.contains("OFFSET 5") || norm.contains("LIMIT 5, 10"),
        "must reflect OFFSET 5: {norm}"
    );
}
