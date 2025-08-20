use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use crate::renderer::Dialect;
use crate::tests::dialect_test_helpers::qi;

fn assert_contains(haystack: &str, needle: &str) {
    assert!(
        haystack.contains(needle),
        "expected SQL to contain `{needle}`, got:\n{haystack}"
    );
}

#[test]
fn pg_basic_single_row_with_returning_all() {
    let (sql, params) = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"), col("name")))
        .insert((val("a@ex.com"), val("Alice")))
        .returning_all()
        .to_sql()
        .expect("to_sql ok");

    // базовые куски
    assert_contains(&sql, "INSERT INTO");
    assert_contains(&sql, &qi("users"));
    assert_contains(&sql, &format!("({}, {})", qi("email"), qi("name")));
    assert_contains(&sql, "VALUES");
    assert_contains(&sql, "RETURNING *");
    assert_eq!(params.len(), 2, "2 значения в VALUES ⇒ 2 параметра");
}

#[test]
fn pg_multi_rows_values_and_param_order() {
    let (sql, params) = QueryBuilder::new_empty()
        .into("tags")
        .columns((col("name"),))
        .insert((val("red"), val("green"), val("blue")))
        .to_sql()
        .expect("to_sql ok");

    // Берём только кусок после VALUES
    let values_pos = sql.find("VALUES").expect("SQL must contain VALUES");
    let values_sql = &sql[values_pos..];

    // Между кортежами должен быть разделитель "), ("
    assert!(
        values_sql.contains("), ("),
        "ожидали несколько кортежей в VALUES, got:\n{sql}"
    );

    // Ровно 3 кортежа: считаем открывающие скобки в части после VALUES
    let tuples = values_sql.matches('(').count();
    assert_eq!(tuples, 3, "ожидали 3 кортежа в VALUES, got SQL:\n{sql}");

    assert_eq!(params.len(), 3, "3 значения ⇒ 3 параметра");
}

#[test]
fn pg_on_conflict_do_nothing() {
    let (sql, _params) = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"),))
        .insert((val("a@ex.com"),))
        .on_conflict((col("email"),))
        .ignore()
        .to_sql()
        .expect("to_sql ok");

    assert_contains(&sql, "ON CONFLICT");
    assert_contains(&sql, &format!("({})", qi("email")));
    assert_contains(&sql, "DO NOTHING");
}

#[test]
fn pg_on_conflict_merge_columns_only_uses_excluded() {
    let (sql, _params) = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"), col("name"), col("age")))
        .insert((val("a@ex.com"), val("Alice"), val(33_i32)))
        .on_conflict((col("email"),))
        .merge((col("name"), col("age"))) // короткая форма
        .to_sql()
        .expect("to_sql ok");

    assert_contains(&sql, "ON CONFLICT");
    assert_contains(&sql, "DO UPDATE SET");
    // оба присваивания должны ссылаться на EXCLUDED
    assert_contains(&sql, &format!("{} = EXCLUDED.{}", qi("name"), qi("name")));
    assert_contains(&sql, &format!("{} = EXCLUDED.{}", qi("age"), qi("age")));
}

#[test]
fn pg_returning_all_from_qualified_star() {
    let (sql, _params) = QueryBuilder::new_empty()
        .into("users")
        .insert((col("email"), val("a@ex.com")))
        .returning_all_from("users")
        .to_sql()
        .expect("to_sql ok");

    // RETURNING "users".*
    assert_contains(&sql, "RETURNING");
    assert_contains(&sql, &format!("{}.*", qi("users")));
}

#[test]
fn sqlite_insert_or_ignore_without_do_update() {
    let mut b = QueryBuilder::new_empty()
        .into("kv")
        .columns((col("k"), col("v")))
        .insert((val("lang"), val("ru")))
        .ignore(); // для SQLite без апсерта это превратится в INSERT OR IGNORE

    b.dialect = Dialect::SQLite; // переключаем диалект для этого билда
    let (sql, params) = b.to_sql().expect("to_sql ok");

    assert!(sql.starts_with("INSERT OR IGNORE INTO"), "{sql}");
    assert_contains(&sql, &qi("kv"));
    assert_eq!(params.len(), 2);
    // не должно быть ON CONFLICT (если нет явного апсерта)
    assert!(
        !sql.contains("ON CONFLICT"),
        "без апсерта SQLite может печатать именно OR IGNORE"
    );
}

#[test]
fn sqlite_on_conflict_do_update_with_excluded() {
    let mut b = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"), col("name")))
        .insert((val("a@ex.com"), val("Alice")))
        .on_conflict((col("email"),))
        .merge((col("name"),)); // короткая форма: name = EXCLUDED.name

    b.dialect = Dialect::SQLite;
    let (sql, _params) = b.to_sql().expect("to_sql ok");

    assert_contains(&sql, "ON CONFLICT");
    assert_contains(&sql, "DO UPDATE SET");
    assert_contains(&sql, &format!("{} = EXCLUDED.{}", qi("name"), qi("name")));
    // RETURNING тоже поддерживается, но здесь не используем
}

#[test]
fn mysql_insert_ignore_and_duplicate_key_update_with_new_alias() {
    let mut b = QueryBuilder::new_empty()
        .into("users")
        .columns((col("email"), col("name")))
        .insert((val("a@ex.com"), val("Alice")))
        .on_conflict((col("email"),))
        .merge((col("name"),)) // короткая форма → new.name
        .ignore();

    b.dialect = Dialect::MySQL;
    let (sql, _params) = b.to_sql().expect("to_sql ok");

    // Префикс IGNORE
    assert!(sql.starts_with("INSERT IGNORE INTO"), "{sql}");

    // Алиас стоит СРАЗУ ПОСЛЕ имени таблицы (до VALUES)
    assert_contains(&sql, " AS `new` ");

    // Отдельно проверяем наличие секции апсерта
    assert_contains(&sql, " ON DUPLICATE KEY UPDATE ");

    // В SET должен быть new.name
    assert_contains(&sql, &format!("`name` = new.`name`"));

    // В MySQL нет RETURNING
    assert!(
        !sql.contains("RETURNING"),
        "MySQL не должен печатать RETURNING"
    );
}

#[test]
fn values_then_merge_params_ordering() {
    let b = QueryBuilder::new_empty()
        .into("t")
        .columns((col("a"), col("b")))
        .insert((val(1_i32), val(2_i32)))
        .on_conflict((col("a"),))
        .merge((
            col("b"),
            val(3_i32), // RHS даёт дополнительные параметры
        ));

    // оставим PG по умолчанию
    let (sql, params) = b.to_sql().expect("to_sql ok");

    assert_contains(&sql, "INSERT INTO");
    assert!(
        params.len() >= 3,
        "ожидаем, что после 2 параметров в VALUES добавятся параметры из merge()"
    );
}
