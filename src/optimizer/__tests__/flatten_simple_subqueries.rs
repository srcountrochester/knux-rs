//! Интеграционные тесты flatten_simple_subqueries: билдер → оптимизатор → SQL.

use crate::expression::helpers::col;
use crate::optimizer::{OptimizeConfig, OptimizeConfigBuilder};
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;
use crate::val;

type QB = QueryBuilder<'static, ()>;

/// Тест: `(SELECT a FROM t WHERE a > 0) AS s` сплющивается в `FROM "t" AS "s"`,
/// а условие переносится наружу. В финальном SQL не должно быть `(SELECT`.
#[test]
fn flatten_sql_basic() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_flatten_simple_subqueries()
        // отключим другие агрессивные, чтобы не смешивать эффекты:
        .without_predicate_pushdown()
        .without_predicate_pullup()
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
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");
    assert!(
        !sql.contains("(SELECT"),
        "подзапрос должен быть сплющен: {sql}"
    );
    assert!(
        sql.contains("FROM") && sql.contains(" AS "),
        "ожидаем базовую таблицу с псевдонимом: {sql}"
    );
    assert!(
        sql.contains("WHERE"),
        "условие из внутреннего WHERE должно сохраниться: {sql}"
    );
}

/// Тест: `(SELECT a FROM t) AS s` без WHERE — сплющивается до `FROM "t" AS "s"`.
#[test]
fn flatten_sql_projection_only() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_flatten_simple_subqueries()
        .without_predicate_pushdown()
        .without_predicate_pullup()
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select(("*",))
        .from::<QBClosureHelper<()>>(|q| q.select((col("a"),)).from("t").r#as("s"))
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");
    assert!(
        !sql.contains("(SELECT"),
        "подзапрос должен быть сплющен: {sql}"
    );
    assert!(
        sql.contains("FROM") && sql.contains(" AS "),
        "ожидаем базовую таблицу с псевдонимом: {sql}"
    );
    assert!(
        !sql.contains("ORDER BY"),
        "никаких побочных изменений не ожидается: {sql}"
    );
}
