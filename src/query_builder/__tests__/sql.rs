use crate::expression::helpers::{col, table, val};
use crate::query_builder::QueryBuilder;
use crate::tests::dialect_test_helpers::{col_list, qi, qn};
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

/// Грубая нормализация пробелов: схлопываем последовательности в один пробел,
/// убираем ведущие/замыкающие пробелы. Помогает сделать проверки более стабильными.
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
fn simple_select_from_table() {
    let (sql, params) = QB::new_empty()
        .select(&["id", "name"])
        .from("users")
        .to_sql()
        .unwrap();

    assert!(
        sql.contains(&format!(
            "SELECT {} FROM {}",
            col_list(&["id", "name"]),
            qi("users")
        )),
        "got: {sql}"
    );
    assert!(params.is_empty());
}

#[test]
fn select_from_table_with_default_schema() {
    let qb = QB::new_empty()
        .with_default_schema(Some("public".into()))
        .select(("id", "name"))
        .from("users");

    let (sql, params) = qb.to_sql().expect("to_sql");
    assert!(
        sql.contains(&format!(
            "SELECT {} FROM {}",
            col_list(&["id", "name"]),
            qn(&["public", "users"])
        )) || sql.contains(&format!("FROM {}", qn(&["public", "users"]))),
        "got: {sql}"
    );
    assert!(params.is_empty());
}

#[test]
fn select_from_qualified_table() {
    let (sql, _) = QB::new_empty()
        .select(("id",))
        .from("app.users")
        .to_sql()
        .unwrap();
    assert!(
        sql.contains(&format!("FROM {}", qn(&["app", "users"]))),
        "got: {sql}"
    );
}

#[test]
fn select_from_subquery_and_collect_params() {
    // subquery: SELECT ?
    let sub = QB::new_empty().select((val(10i32),));
    // closure-subquery: SELECT ?
    let scalar_subq: QBClosureHelper<()> = |q| q.select((val(20i32),));

    let qb = QB::new_empty().select(("x",)).from((sub, scalar_subq));

    let (sql, params) = qb.to_sql().unwrap();
    assert!(sql.contains("FROM (SELECT"));
    assert_eq!(params.len(), 2);
}

#[test]
fn to_sql_from_multiple_plain_tables_with_default_schema() {
    let (sql, params) = QB::new_empty()
        .with_default_schema(Some("app".into()))
        .select(("id",))
        .from(("users", "auth.roles", "logs"))
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty(), "no params expected");

    let sql = norm(&sql);

    // Проверяем, что все источники присутствуют и в правильном порядке:
    // app.users, auth.roles, app.logs
    let from_users = format!("FROM {}", qn(&["app", "users"]));
    let i_users = sql
        .find(&from_users)
        .expect(&format!(r#"{} not found in: {}"#, from_users, sql));

    let roles_pat = qn(&["auth", "roles"]);
    let i_roles = sql
        .find(&roles_pat)
        .expect(&format!(r#""{}" not found in: {}"#, roles_pat, sql));

    let logs_pat = qn(&["app", "logs"]);
    let i_logs = sql
        .find(&logs_pat)
        .expect(&format!(r#""{}" not found in: {}"#, logs_pat, sql));

    assert!(
        i_users < i_roles && i_roles < i_logs,
        "sources must keep order: users, roles, logs; got: {sql}"
    );

    assert!(
        sql.contains(&format!("FROM {}", qn(&["app", "users"]))),
        r#"FROM {} not found in: {}"#,
        qn(&["app", "users"]),
        sql
    );
}

#[test]
fn to_sql_from_mixed_table_subquery_and_closure() {
    // subquery: SELECT ?
    let sub = QB::new_empty().select((val(10i32),));
    let scalar_subq: QBClosureHelper<()> = |q| q.select((val(20i32),));
    // closure-subquery: SELECT ?
    let (sql, params) = QB::new_empty()
        .select(("x",))
        .from(("users", sub, scalar_subq))
        .to_sql()
        .expect("to_sql");

    // Два параметра: 10 и 20 — в таком порядке.
    assert_eq!(
        params.len(),
        2,
        "params from both subqueries must be collected"
    );
    assert!(matches!(params[0], crate::param::Param::I32(10)));
    assert!(matches!(params[1], crate::param::Param::I32(20)));

    let sql = norm(&sql);

    // Должно быть: сначала users, затем два подзапроса "(SELECT ..."
    let from_users = format!("FROM {}", qi("users"));
    let i_users = sql
        .find(&from_users)
        .expect(&format!(r#"{} not found in: {}"#, from_users, sql));

    // Найдём два вхождения "(SELECT"
    let mut idx = 0usize;
    let mut hits = Vec::new();
    while let Some(pos) = sql[idx..].find("(SELECT") {
        let p = idx + pos;
        hits.push(p);
        idx = p + 7; // длина "(SELECT"
    }
    assert_eq!(
        hits.len(),
        2,
        "expected two subqueries in FROM, got {} in: {sql}",
        hits.len()
    );

    assert!(
        i_users < hits[0] && hits[0] < hits[1],
        "expected order: users, subquery1, subquery2; got: {sql}"
    );

    // Убедимся, что SELECT-часть тоже адекватна
    assert!(
        sql.contains(&format!("FROM {}", qi("users"))),
        r#"FROM {} not found in: {}"#,
        qi("users"),
        sql
    );
}

#[test]
fn insert_into_with_columns_values_and_params() {
    // INSERT INTO users (id, age) VALUES (?, ?)
    let (sql, params) = QB::new_empty()
        .into("users")
        .columns(("id", "age"))
        .insert((val(1i32), val(2i32)))
        .to_sql()
        .expect("insert to_sql");

    let sqln = norm(&sql);
    assert!(
        sqln.contains(&format!("INSERT INTO {}", qi("users"))),
        "got: {sql}"
    );
    assert!(
        sqln.contains(&format!("( {} )", col_list(&["id", "age"]))) || sqln.contains(" ("),
        "got: {sql}"
    );
    assert!(sqln.contains("VALUES"), "got: {sql}");
    assert_eq!(params.len(), 2, "params must contain 2 values");
}

#[test]
fn insert_into_with_default_schema() {
    // default_schema префиксует простое имя
    let (sql, _params) = QB::new_empty()
        .with_default_schema(Some("public".into()))
        .into("users")
        .columns(("id",))
        .insert((val(10i32),))
        .to_sql()
        .expect("insert");

    assert!(
        sql.contains(&format!("INSERT INTO {}", qn(&["public", "users"]))),
        "got: {sql}"
    );
}

#[cfg(not(feature = "mysql"))]
#[test]
fn insert_returning_is_emitted() {
    let (sql, _params) = QB::new_empty()
        .into("t")
        .columns(("x",))
        .insert((val(1i32),))
        .returning(("x",))
        .to_sql()
        .expect("insert returning");

    assert!(sql.contains("RETURNING"), "got: {sql}");
}

#[test]
fn update_set_and_where_to_sql() {
    // UPDATE users SET age = ? WHERE id = ?
    let (sql, params) = QB::new_empty()
        .update("users")
        .set(("age", val(30i32)))
        .r#where((col("id").eq(val(1i32)),))
        .to_sql()
        .expect("update");
    let sqln = norm(&sql);
    assert!(
        sqln.starts_with("UPDATE") || sqln.contains(" UPDATE "),
        "got: {sql}"
    );
    assert!(
        sqln.contains(&format!("UPDATE {}", qi("users"))),
        "got: {sql}"
    );
    assert!(sqln.contains(" SET "), "got: {sql}");
    assert!(sqln.contains(" WHERE "), "got: {sql}");
    assert_eq!(params.len(), 2, "one param from SET, one from WHERE");
}

#[test]
fn update_with_from_sources() {
    // UPDATE t SET x = ? FROM a, b
    let (sql, params) = QB::new_empty()
        .update("t")
        .set(("x", val(1i32)))
        .from(("a", "b"))
        .to_sql()
        .expect("update with from");
    let sqln = norm(&sql);
    assert!(sqln.contains(&format!("UPDATE {}", qi("t"))), "got: {sql}");
    assert!(sqln.contains(" SET "), "got: {sql}");

    #[cfg(feature = "mysql")]
    {
        assert!(
            !sqln.contains(" FROM "),
            "MySQL не должен печатать FROM в UPDATE: {sql}"
        );
    }
    #[cfg(not(feature = "mysql"))]
    {
        assert!(
            sqln.contains(&format!("FROM {}, {}", qi("a"), qi("b"))),
            "got: {sql}"
        );
    }

    assert_eq!(params.len(), 1);
}

#[test]
fn delete_basic_where_and_returning() {
    // DELETE FROM t WHERE id = ? RETURNING *
    let (sql, params) = QB::new_empty()
        .delete(table("t"))
        .r#where((col("id").eq(val(10i32)),))
        .returning_all()
        .to_sql()
        .expect("delete");
    let sqln = norm(&sql);
    assert!(
        sqln.starts_with("DELETE") || sqln.contains(" DELETE "),
        "got: {sql}"
    );
    assert!(sqln.contains(&format!("FROM {}", qi("t"))), "got: {sql}");
    assert!(sqln.contains(" WHERE "), "got: {sql}");

    #[cfg(feature = "mysql")]
    {
        assert!(
            !sql.contains("RETURNING"),
            "MySQL не должен печатать RETURNING: {sql}"
        );
    }
    #[cfg(not(feature = "mysql"))]
    {
        assert!(sql.contains("RETURNING"), "got: {sql}");
    }

    assert_eq!(params.len(), 1);
}

#[test]
fn delete_using_multiple_tables() {
    // DELETE FROM t USING a, b
    let (sql, params) = QB::new_empty()
        .delete("t")
        .using(("a", "b"))
        .to_sql()
        .expect("delete using");
    let sqln = norm(&sql);
    assert!(sqln.contains(&format!("FROM {}", qi("t"))), "got: {sql}");
    assert!(
        sqln.contains(&format!("USING {}, {}", qi("a"), qi("b"))),
        "got: {sql}"
    );
    assert!(params.is_empty());
}
