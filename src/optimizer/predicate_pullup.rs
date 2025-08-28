use sqlparser::ast::{self as S};

use crate::optimizer::utils::{
    and_merge, projection_is_direct_columns, select_is_simple_no_cardinality,
};

/// Подъём простых подзапросов во FROM (subquery pull-up) и слияние предикатов.
///
/// Консервативное правило: заменяем `FROM (SELECT col_list FROM T [WHERE ...]) AS s`
/// на `FROM T AS s`, если соблюдены ВСЕ условия:
/// - подзапрос — это `Query { body: Select(...) }` без `WITH/ORDER BY/LIMIT/FETCH`;
/// - внутри `Select` НЕТ `DISTINCT`, `GROUP BY`, `HAVING`, `SET`-операций, окон и т.п.;
/// - `FROM` внутри подзапроса состоит ровно из одного источника и без `JOIN`;
/// - проекции — только прямые ссылки на колонки (идентификаторы) без переименования.
///
/// При подъёме предикат из `WHERE` подзапроса конъюнктится (AND) с внешним `WHERE`.
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
#[inline]
pub fn predicate_pullup(stmt: &mut S::Statement) {
    use crate::optimizer::utils::walk_statement_mut;

    #[inline]
    fn subquery_ok(q: &S::Query) -> bool {
        // без WITH/ORDER BY/LIMIT/FETCH
        q.with.is_none() && q.order_by.is_none() && q.limit_clause.is_none() && q.fetch.is_none()
    }

    #[inline]
    fn can_pull(tf: &S::TableFactor) -> bool {
        match tf {
            S::TableFactor::Derived { subquery, .. } => {
                if !subquery_ok(subquery) {
                    return false;
                }
                match subquery.body.as_ref() {
                    S::SetExpr::Select(sel_box) => {
                        let sel = sel_box.as_ref();
                        select_is_simple_no_cardinality(sel)
                            && sel.from.len() == 1
                            && sel.from[0].joins.is_empty()
                            && projection_is_direct_columns(sel)
                    }
                    _ => false,
                }
            }
            _ => false,
        }
    }

    // Обрабатываем все SELECT в дереве
    walk_statement_mut(
        stmt,
        &mut |q: &mut S::Query, _top| {
            if let S::SetExpr::Select(sel_box) = q.body.as_mut() {
                let sel = sel_box.as_mut();

                for twj in &mut sel.from {
                    // интересует только relation (без JOIN'ов)
                    if !can_pull(&twj.relation) {
                        continue;
                    }

                    // забираем внутренний WHERE
                    let mut inner_where: Option<S::Expr> = None;

                    // заменяем relation: вытаскиваем базовую таблицу и переносим alias
                    if let S::TableFactor::Derived {
                        subquery,
                        alias: outer_alias,
                        ..
                    } = &mut twj.relation
                    {
                        if let S::SetExpr::Select(inner_sel_box) = subquery.body.as_mut() {
                            let inner_sel = inner_sel_box.as_mut();

                            inner_where = inner_sel.selection.take();

                            // единственный источник внутри
                            let mut inner_twj = inner_sel.from.remove(0);

                            // relation должен быть базовой таблицей
                            if let S::TableFactor::Table {
                                alias: base_alias, ..
                            } = &mut inner_twj.relation
                            {
                                *base_alias = outer_alias.clone(); // сохраняем внешний алиас
                                twj.relation = inner_twj.relation; // поднимаем таблицу
                            } else {
                                // защитный выход (не должен происходить по can_pull)
                                continue;
                            }
                        }
                    }

                    // склеиваем предикаты: outer.where = outer.where AND inner.where
                    and_merge(&mut sel.selection, inner_where.take());
                }
            }
        },
        &mut |_e: &mut S::Expr| {},
    );
}
