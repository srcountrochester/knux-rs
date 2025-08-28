//! Интеграционные тесты predicate_pushdown: билдер → оптимизатор → SQL.

use crate::expression::helpers::{col, table, val};
use crate::optimizer::{OptimizeConfig, OptimizeConfigBuilder};
use crate::query_builder::QueryBuilder;
use crate::tests::dialect_test_helpers::{ph, qi};
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

/// Тест: переносим `WHERE "s"."a" > $1` внутрь `(SELECT a FROM t) AS s`.
/// Ожидаем: внешний WHERE исчезает, внутри подзапроса появляется `WHERE a > $1`.
#[test]
fn predicate_pushdown_moves_where_into_subquery_sql() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_predicate_pushdown()
        .without_predicate_pullup() // чтобы не «схлопнуть» подзапрос дальше
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select(("*",))
        .from::<QBClosureHelper<()>>(|q| q.select((col("a"),)).from(table("t")).r#as("s"))
        .where_((col("s.a").gt(val(10)),))
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");

    // Ожидаем, что подзапрос сохранён и внутрь перенесён WHERE по `a`
    let expected_sub = format!(
        "FROM (SELECT {} FROM {} WHERE {} > {}) AS {}",
        qi("a"),
        qi("t"),
        qi("a"),
        ph(1),
        qi("s")
    );
    assert!(
        sql.contains(&expected_sub),
        "внутри подзапроса должен появиться WHERE по a: {sql}"
    );

    // Дополнительно убеждаемся, что внешнего WHERE после `AS s` нет
    let after_alias_where = format!("{} WHERE", qi("s"));
    assert!(
        !sql.contains(&after_alias_where),
        "внешний WHERE не должен оставаться: {sql}"
    );
}

/// Тест: predicate pushdown не выполняется при DISTINCT в подзапросе.
/// Ожидаем: внешний WHERE остаётся снаружи.
#[test]
fn predicate_pushdown_not_applied_with_distinct_sql() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_predicate_pushdown()
        .without_predicate_pullup()
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select(("*",))
        .from::<QBClosureHelper<()>>(|q| {
            q.select((col("a"),))
                .distinct([] as [&str; 0])
                .from(table("t"))
                .r#as("s")
        })
        .where_((col("s.a").gt(val(10)),))
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");
    assert!(
        sql.contains("FROM (SELECT DISTINCT"),
        "подзапрос с DISTINCT должен сохраниться: {sql}"
    );
    assert!(
        sql.contains(" WHERE "),
        "внешний WHERE должен остаться: {sql}"
    );
}
