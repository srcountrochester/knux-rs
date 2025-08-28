//! Интеграционные тесты: включаем `simplify_exists` и проверяем итоговый SQL.

use crate::expression::helpers::col;
use crate::optimizer::{OptimizeConfig, OptimizeConfigBuilder};
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

/// Тест: `WHERE EXISTS (подзапрос)` — оптимизатор должен заменить
/// проекцию на `SELECT 1` и удалить `ORDER BY` внутри EXISTS.
#[test]
fn qb_where_exists_simplified_sql() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .with_simplify_exists()
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select(("*",))
        .from("users")
        .where_exists::<QBClosureHelper<()>>(|q| {
            q.select((col("id"),)).from("orders").order_by(("id",))
        })
        .to_sql()
        .expect("to_sql");

    let sql_norm = sql.replace('\n', " ");
    assert!(
        sql_norm.contains("EXISTS (SELECT 1 FROM"),
        "должно быть SELECT 1 внутри EXISTS: {sql_norm}"
    );
    assert!(
        !sql_norm.contains("ORDER BY"),
        "ORDER BY внутри EXISTS должен быть удалён: {sql_norm}"
    );
}

/// Тест: `HAVING EXISTS (подзапрос с LIMIT)` — при наличии LIMIT
/// ORDER BY в подзапросе всё равно удаляется, а проекция = `SELECT 1`.
#[test]
fn qb_having_exists_with_limit_simplified_sql() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .with_simplify_exists()
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select((col("u"),))
        .from("users")
        .group_by((col("u"),))
        .having_exists::<QBClosureHelper<()>>(|q| {
            q.select((col("id"),))
                .from("orders")
                .order_by(("id",))
                .limit(10)
        })
        .to_sql()
        .expect("to_sql");

    let sql_norm = sql.replace('\n', " ");
    assert!(
        sql_norm.contains("EXISTS (SELECT 1 FROM"),
        "должно быть SELECT 1 внутри EXISTS: {sql_norm}"
    );
    assert!(
        !sql_norm.contains("ORDER BY"),
        "ORDER BY внутри EXISTS должен быть удалён: {sql_norm}"
    );
}
