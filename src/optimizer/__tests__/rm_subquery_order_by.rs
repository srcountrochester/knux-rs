//! Интеграционные тесты: включаем оптимизацию и проверяем итоговый SQL.

use crate::expression::helpers::col;
use crate::optimizer::{OptimizeConfig, OptimizeConfigBuilder};
use crate::query_builder::QueryBuilder;
use crate::type_helpers::QBClosureHelper;

type QB = QueryBuilder<'static, ()>;

/// Проверяем: подзапрос во FROM c `ORDER BY`, но без LIMIT —
/// оптимизатор удаляет внутренний `ORDER BY` (в SQL его быть не должно).
#[test]
fn qb_removes_order_in_derived_without_limit_sql() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .with_rm_subquery_order_by()
        .build();

    let (sql, params) = QB::new_empty()
        .with_optimize(cfg)
        .select(("*",))
        .from::<QBClosureHelper<()>>(|q| q.select((col("a"),)).from("t").order_by(("a",)).r#as("s"))
        .to_sql()
        .expect("to_sql");

    assert!(params.is_empty());
    let sql_norm = sql.replace('\n', " ");
    assert!(
        !sql_norm.contains("ORDER BY"),
        "В подзапросе не должно быть ORDER BY: {sql_norm}"
    );
}

/// Проверяем: если в подзапросе есть LIMIT — внутренний `ORDER BY` сохраняется.
#[test]
fn qb_keeps_order_when_limit_present_sql() {
    let cfg: OptimizeConfig = OptimizeConfigBuilder::default()
        .with_rm_subquery_order_by()
        .build();

    let (sql, _params) = QB::new_empty()
        .with_optimize(cfg)
        .select((col("x"),))
        .from::<QBClosureHelper<()>>(|q| {
            q.select((col("a"),))
                .from("t")
                .order_by(("a",))
                .limit(5)
                .r#as("s")
        })
        .to_sql()
        .expect("to_sql");

    let sql_norm = sql.replace('\n', " ");
    assert!(
        sql_norm.contains("ORDER BY") && sql_norm.contains("LIMIT"),
        "При наличии LIMIT внутренний ORDER BY должен остаться: {sql_norm}"
    );
}
