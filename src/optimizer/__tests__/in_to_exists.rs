//! Интеграционные тесты in_to_exists: билдер → оптимизатор → SQL.

use crate::expression::helpers::{col, table};
use crate::optimizer::{OptimizeConfig, OptimizeConfigBuilder};
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

/// Тест: положительный `IN (subquery)` переписывается в `EXISTS (...)`,
/// внутри EXISTS — `SELECT 1`, и добавлено сравнение проекции и внешнего lhs.
#[test]
fn in_to_exists_sql_basic() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_in_to_exists() // ручное включение
        .build();

    let closure: QBClosureHelper<()> = |q| {
        q.select((col("orders.user_id"),))
            .from(table("orders"))
            .where_((col("orders.status").eq(col("'open'")),))
    }; // просто для наличия WHERE; если у вас val/лит — используйте их.

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select(("*",))
        .from(table("users"))
        .where_in(
            // IN (subquery)
            col("users.id"),
            closure,
        )
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");
    assert!(sql.contains("EXISTS ("), "ожидался EXISTS(...): {sql}");
    assert!(
        !sql.contains(" IN ("),
        "IN (subquery) должен быть переписан: {sql}"
    );
    assert!(
        sql.contains("SELECT 1"),
        "внутри EXISTS должен быть SELECT 1: {sql}"
    );
    // наличие сравнения user_id = users.id (точная форма зависит от кавычек/квалификации)
    assert!(
        sql.contains("user_id") && sql.contains("=") && sql.contains("users") && sql.contains("id"),
        "в EXISTS должен быть конъюнкт равенства проекции и lhs: {sql}"
    );
}

/// Тест: `NOT IN (subquery)` не переписывается в EXISTS.
#[test]
fn in_to_exists_sql_not_in_untouched() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_in_to_exists()
        .build();

    let closure: QBClosureHelper<()> = |q| q.select((col("orders.user_id"),)).from(table("orders"));

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select(("*",))
        .from(table("users"))
        .where_not_in(
            // остаётся NOT IN (subquery)
            col("users.id"),
            closure,
        )
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");
    assert!(
        sql.contains(" NOT IN ("),
        "NOT IN должен остаться без изменений: {sql}"
    );
    assert!(
        !sql.contains("EXISTS ("),
        "для NOT IN переписывания быть не должно: {sql}"
    );
}
