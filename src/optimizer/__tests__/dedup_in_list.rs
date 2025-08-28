//! Интеграционные тесты dedup_in_list: билдер → оптимизатор → финальный SQL.

use crate::expression::helpers::{col, val};
use crate::lit;
use crate::optimizer::{OptimizeConfig, OptimizeConfigBuilder};
use crate::query_builder::QueryBuilder;
use crate::tests::dialect_test_helpers::{ph, qi};

type QB = QueryBuilder<'static, ()>;

/// Тест: дубликаты чисел в IN-списке удаляются — в SQL остаются только уникальные плейсхолдеры.
#[test]
fn dedup_in_list_sql_numbers() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_dedup_in_list()
        .build();

    let (sql, _params) = QB::new_empty()
        .select(("*",))
        .from("users")
        .where_in(
            col("id"),
            vec![
                lit("1"), // ← ЛИТЕРАЛЫ, не val()
                lit("1"),
                lit("2"),
                lit("2"),
                lit("3"),
            ],
        )
        .with_optimize(cfg)
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");

    // Проверяем, что остались ровно три уникальные константы,
    // и нет лишних повторов.
    assert_eq!(
        sql.matches("'1'").count(),
        1,
        "должна остаться одна '1': {sql}"
    );
    assert_eq!(
        sql.matches("'2'").count(),
        1,
        "должна остаться одна '2': {sql}"
    );
    assert_eq!(
        sql.matches("'3'").count(),
        1,
        "должна остаться одна '3': {sql}"
    );

    // И никаких плейсхолдеров в списке (мы не использовали val()).
    assert!(
        !sql.contains('?') && !sql.contains("$1"),
        "в списке не должно быть плейсхолдеров: {sql}"
    );
}

/// Тест: смешанный список — дубли удаляются только у констант, выражения остаются.
/// Пример: `a IN (1, upper(b), 1, upper(b), 2)` → `IN (1, upper(b), upper(b), 2)`.
#[test]
fn dedup_in_list_sql_mixed() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .none()
        .with_dedup_in_list()
        .build();

    let (sql, _params) = QB::new_empty()
        .select(("*",))
        .from("t")
        .where_in(
            col("a"),
            vec![
                lit("1"), // ← ЛИТЕРАЛ
                col("b"), // ← выражение (идентификатор)
                lit("1"), // ← дубликат литерала — должен быть удалён
                col("b"), // ← дубликат выражения — ДОЛЖЕН ОСТАТЬСЯ
                lit("2"),
            ],
        )
        .with_optimize(cfg)
        .to_sql()
        .expect("to_sql");

    let sql = sql.replace('\n', " ");

    // Ожидаемый эффект: из двух '1' останется одна; оба `b` сохраняются.
    assert_eq!(
        sql.matches("'1'").count(),
        1,
        "константа '1' должна быть единожды: {sql}"
    );
    assert_eq!(
        sql.matches("'2'").count(),
        1,
        "константа '2' должна быть единожды: {sql}"
    );
    assert_eq!(
        sql.matches(&qi("b")).count(),
        2,
        "оба вхождения выражения b должны сохраниться: {sql}"
    );

    // Для надёжности — в IN должны быть 4 элемента после дедупа.
    // Нормализуем и считаем запятые внутри ближайшей пары скобок.
    let start = sql.find(" IN (").expect("IN ( not found");
    let tail = &sql[start + 5..]; // после " IN ("
    let end = tail.find(')').expect(") not found");
    let inside = &tail[..end];
    let items = inside.split(',').map(|s| s.trim()).collect::<Vec<_>>();
    assert_eq!(items.len(), 4, "ожидаем 4 элемента внутри IN: {inside}");
}
