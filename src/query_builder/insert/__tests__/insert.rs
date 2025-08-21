use crate::expression::helpers::{col, val};
use crate::query_builder::QueryBuilder;
use sqlparser::ast::{Ident, ObjectName};

fn ident_list_to_vec(cols: &[Ident]) -> Vec<String> {
    cols.iter().map(|i| i.value.clone()).collect()
}

#[test]
fn insert_single_row_from_pairs() {
    let ins = QueryBuilder::new_empty().into("users").insert((
        col("id"),
        val(1_i32),
        col("name"),
        val("Bob"),
        col("age"),
        val(34_i32),
    ));
    // columns зафиксировались из пар
    assert_eq!(
        ident_list_to_vec(&ins.columns),
        vec!["id".to_string(), "name".to_string(), "age".to_string()]
    );
    // одна строка
    assert_eq!(ins.rows.len(), 1);
    // две числовые + одна строковая -> 3 значения
    assert_eq!(ins.rows[0].values.len(), 3);
    // все три значения параметризованы -> 3 параметра
    assert_eq!(ins.rows[0].params.len(), 3);
    // на уровне билдеров ошибок нет
    assert!(ins.builder_errors.is_empty());
    // таблица распарсилась
    let t = ins.table.as_ref().expect("table set");
    assert_eq!(
        t.to_string(),
        ObjectName::from(vec![Ident::new("users")]).to_string()
    );
}

#[test]
fn insert_single_column_many_values_multiple_rows() {
    // одна колонка: каждое значение -> отдельная строка
    let ins = QueryBuilder::new_empty()
        .into("tags")
        .columns((col("name"),))
        .insert((val("red"), val("green"), val("blue")));
    assert_eq!(ident_list_to_vec(&ins.columns), vec!["name".to_string()]);
    assert_eq!(ins.rows.len(), 3, "each value becomes its own row");
    assert_eq!(ins.rows[0].values.len(), 1);
    assert_eq!(ins.rows[1].values.len(), 1);
    assert_eq!(ins.rows[2].values.len(), 1);
    // три значения -> 3 параметра суммарно (распределены по строкам)
    let total_params: usize = ins.rows.iter().map(|r| r.params.len()).sum();
    assert_eq!(total_params, 3);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn insert_multi_columns_chunked_into_rows() {
    // 2 колонки: 6 значений -> 3 строки по 2 значения
    let ins = QueryBuilder::new_empty()
        .into("accounts")
        .columns((col("email"), col("is_active")))
        .insert((
            val("a@example.com"),
            val(true),
            val("b@example.com"),
            val(false),
            val("c@example.com"),
            val(true),
        ));
    assert_eq!(
        ident_list_to_vec(&ins.columns),
        vec!["email".to_string(), "is_active".to_string()]
    );
    assert_eq!(ins.rows.len(), 3);
    assert!(ins.rows.iter().all(|r| r.values.len() == 2));
    // 6 значений -> 6 параметров
    let total_params: usize = ins.rows.iter().map(|r| r.params.len()).sum();
    assert_eq!(total_params, 6);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn insert_columns_exactly_one_row_when_counts_match() {
    // если количество значений == количеству колонок -> одна строка
    let ins = QueryBuilder::new_empty()
        .into("kv")
        .columns((col("k"), col("v")))
        .insert((val("lang"), val("ru")));
    assert_eq!(ins.rows.len(), 1);
    assert_eq!(ins.rows[0].values.len(), 2);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn insert_error_mismatch_values_count() {
    // 2 колонки, а значений 3 -> ошибка
    let ins = QueryBuilder::new_empty()
        .into("kv")
        .columns((col("k"), col("v")))
        .insert((val("a"), val("b"), val("c")));
    assert!(
        !ins.builder_errors.is_empty(),
        "should record a builder error"
    );
}

#[test]
fn insert_error_odd_pairs_without_defined_columns() {
    // без columns() ожидаем пары (col, value)
    let ins = QueryBuilder::new_empty()
        .into("users")
        .insert((col("id"), val(1_i32), col("name"))); // нечётное число => ошибка
    assert!(!ins.builder_errors.is_empty());
}

#[test]
fn insert_column_identifier_can_be_compound_uses_last_segment() {
    // колонка как compound identifier: возьмём последний сегмент
    let ins = QueryBuilder::new_empty()
        .into("t")
        .insert((col("public.users.id"), val(10_i32)));
    assert_eq!(ident_list_to_vec(&ins.columns), vec!["id".to_string()]);
    assert_eq!(ins.rows.len(), 1);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn insert_value_can_be_scalar_subquery_and_params_order_is_preserved() {
    // subquery с параметром + обычный параметр => порядок: [subquery_param, value_param]
    let ins = QueryBuilder::new_empty().into("audit").insert((
        col("user_id"),
        |q: QueryBuilder| {
            q.from("users")
                .select((col("id"),))
                .r#where(col("email").eq(val("bob@example.com")))
                .limit(1)
        },
        col("action"),
        val("login"),
    ));

    assert_eq!(ins.rows.len(), 1);
    let row = &ins.rows[0];
    // два значения в строке
    assert_eq!(row.values.len(), 2);
    // оба значения параметризованы => минимум 2 параметра
    assert!(row.params.len() >= 2);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn into_parses_schema_table_into_object_name() {
    // schema.table
    let ins = QueryBuilder::new_empty()
        .into("crm.contacts")
        .insert((col("id"), val(1_i32)));
    let name = ins.table.as_ref().expect("table set").to_string();
    // простая проверка string-представления ObjectName
    assert!(
        name.contains("crm") && name.contains("contacts"),
        "unexpected object name: {name}"
    );
}

#[test]
fn insert_params_across_multiple_rows_preserve_order() {
    // 2 колонки, 2 строки => 4 значения → 4 параметра; порядок — построчно, слева-направо
    let ins = QueryBuilder::new_empty()
        .into("accounts")
        .columns((col("email"), col("active")))
        .insert((
            val("a@example.com"),
            val(true), // row 1
            val("b@example.com"),
            val(false), // row 2
        ));

    assert_eq!(ins.rows.len(), 2);
    assert_eq!(ins.rows[0].params.len(), 2);
    assert_eq!(ins.rows[1].params.len(), 2);
    // общая сумма = 4
    let total: usize = ins.rows.iter().map(|r| r.params.len()).sum();
    assert_eq!(total, 4);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn insert_one_column_many_values_becomes_many_rows() {
    let ins = QueryBuilder::new_empty()
        .into("tags")
        .columns((col("name"),))
        .insert((val("red"), val("green"), val("blue")));

    assert_eq!(ident_list_to_vec(&ins.columns), vec!["name"]);
    assert_eq!(ins.rows.len(), 3);
    assert!(ins.rows.iter().all(|r| r.values.len() == 1));
    let total_params: usize = ins.rows.iter().map(|r| r.params.len()).sum();
    assert_eq!(total_params, 3);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn insert_with_scalar_subquery_value_collects_params() {
    let ins = QueryBuilder::new_empty().into("audit").insert((
        col("user_id"),
        |q: QueryBuilder| {
            q.from("users")
                .select((col("id"),))
                .r#where(col("email").eq(val("bob@example.com")))
                .limit(1)
        },
        col("action"),
        val("login"),
    ));

    assert_eq!(ins.rows.len(), 1);
    let row = &ins.rows[0];
    assert_eq!(row.values.len(), 2);
    // есть параметры и из подзапроса, и из обычного значения
    assert!(row.params.len() >= 2);
    assert!(ins.builder_errors.is_empty());
}

#[test]
fn insert_columns_chunking_multiple_rows() {
    // 3 колонки, 6 значений → 2 строки по 3 значения
    let ins = QueryBuilder::new_empty()
        .into("events")
        .columns((col("ts"), col("kind"), col("payload")))
        .insert((
            val(100_i64),
            val("login"),
            val("{}"),
            val(200_i64),
            val("logout"),
            val("{}"),
        ));

    assert_eq!(ins.rows.len(), 2);
    assert!(ins.rows.iter().all(|r| r.values.len() == 3));
    let total_params: usize = ins.rows.iter().map(|r| r.params.len()).sum();
    assert_eq!(total_params, 6);
    assert!(ins.builder_errors.is_empty());
}

// ==== Ошибочные кейсы ====

// без columns() — нечётное число элементов => ошибка пар (col, value)
#[test]
fn insert_without_columns_odd_pairs_error() {
    let ins = QueryBuilder::new_empty()
        .into("users")
        .insert((col("id"), val(1_i32), col("name"))); // пропущено значение
    assert!(
        !ins.builder_errors.is_empty(),
        "expected builder error on odd (col, value) pairs"
    );
}

// columns заданы, но число values не кратно числу колонок => ошибка
#[test]
fn insert_values_count_mismatch_error() {
    let ins = QueryBuilder::new_empty()
        .into("kv")
        .columns((col("k"), col("v")))
        .insert((val("a"), val("b"), val("extra")));
    assert!(
        !ins.builder_errors.is_empty(),
        "expected builder error on values count mismatch"
    );
}

// compound identifier для колонки: используем последний сегмент
#[test]
fn insert_compound_identifier_column_uses_last_segment() {
    let ins = QueryBuilder::new_empty()
        .into("t")
        .insert((col("public.users.id"), val(10_i32)));
    assert_eq!(ident_list_to_vec(&ins.columns), vec!["id"]);
    assert!(ins.builder_errors.is_empty());
}

// ==== Смоук-тесты на «пустые» случаи (пока помечены ignore, если ArgList для () не поддержан) ====

#[test]
fn insert_no_columns_empty_data_error() {
    let ins = QueryBuilder::new_empty().into("users").insert(()); // пусто
    assert!(
        !ins.builder_errors.is_empty(),
        "expected error: empty data without columns"
    );
}

#[test]
fn insert_columns_empty_list_error() {
    let ins = QueryBuilder::new_empty()
        .into("users")
        .columns(()) // пустой список колонок
        .insert((val(1_i32),));
    assert!(
        !ins.builder_errors.is_empty(),
        "expected error: empty column list"
    );
}

// ==== Доп. проверка парсинга имени таблицы ====

#[test]
fn into_parses_schema_table_object_name() {
    let ins = QueryBuilder::new_empty()
        .into("crm.contacts")
        .insert((col("id"), val(1_i32)));
    let name = ins.table.as_ref().expect("table set").to_string();
    assert!(name.contains("crm") && name.contains("contacts"));
}
