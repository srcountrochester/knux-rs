use crate::expression::helpers::val;
use crate::query_builder::QueryBuilder;

fn norm(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

#[test]
fn from_subquery_with_alias_via_builder_as() {
    let sub = QueryBuilder::new_empty()
        .select(("id",))
        .from(("users",))
        .r#as("u");

    let (sql, params) = QueryBuilder::new_empty()
        .select(("x",))
        .from((sub,))
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty());

    let sql = norm(&sql);

    // Принимаем оба стиля печати алиаса: с "AS" и без.
    let ok = sql.contains("(SELECT \"id\" FROM \"users\") AS \"u\"")
        || sql.contains("(SELECT \"id\" FROM \"users\") \"u\"");

    assert!(ok, "got: {sql}");
}

#[test]
fn from_closure_subquery_with_alias() {
    let (sql, params) = QueryBuilder::new_empty()
        .select(("x",))
        .from(|q: QueryBuilder| q.select((val(1i32),)).r#as("t1"))
        .to_sql()
        .expect("to_sql");

    assert_eq!(params.len(), 1);
    assert!(matches!(params[0], crate::param::Param::I32(1)));

    let sql = norm(&sql);

    let ok = (sql.contains("FROM (SELECT ?") && sql.contains(") AS \"t1\""))
        || (sql.contains("FROM (SELECT ?") && sql.contains(") \"t1\""));

    assert!(ok, "got: {sql}");
}
