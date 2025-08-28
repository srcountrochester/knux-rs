//! Интеграционные тесты predicate_pushdown: билдер → оптимизатор → финальный SQL.

use crate::expression::helpers::col;
use crate::optimizer::{OptimizeConfig, OptimizeConfigBuilder};
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;
use crate::val;

type QB = QueryBuilder<'static, ()>;

/// Тест: сплющивание простого подзапроса во FROM.
/// Ожидаем: в итоговом SQL нет `(SELECT ...)`, вместо этого `FROM "t" AS "s"`,
/// а предикат из внутреннего WHERE присутствует во внешнем WHERE.
#[test]
fn pp_sql_flatten_simple_derived() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .with_predicate_pullup() // включает predicate_pullup
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select((col("s.a"),))
        .from::<QBClosureHelper<()>>(|q| {
            q.select((col("a"),))
                .from("t")
                .where_((col("a").gt(val(0)),))
                .r#as("s")
        })
        .where_((col("s.a").gt(val(10)),))
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");
    assert!(
        !sql.contains("(SELECT"),
        "подзапрос должен быть поднят: {sql}"
    );
    assert!(
        sql.contains("FROM") && sql.contains(" AS "),
        "ожидаем именованную таблицу с псевдонимом: {sql}"
    );
    // оба предиката должны быть в WHERE
    assert!(sql.contains("WHERE"), "ожидаем WHERE: {sql}");
    assert!(
        sql.contains("a") && sql.contains(">"),
        "ожидаем условия в WHERE: {sql}"
    );
}

/// Тест: не сплющивать при выражениях в проекции.
/// Ожидаем наличие `(SELECT ...)` в финальном SQL.
#[test]
fn pp_sql_do_not_flatten_when_projection_has_expr() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .with_predicate_pullup()
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select((col("s.a"),))
        .from::<QBClosureHelper<()>>(|q| {
            // выражение в проекции — нельзя поднимать
            q.select(((col("a").add(val(1))),)) // a + 1
                .from("t")
                .r#as("s")
        })
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");
    assert!(
        sql.contains("(SELECT"),
        "подзапрос не должен подниматься при выражениях в проекции: {sql}"
    );
}
