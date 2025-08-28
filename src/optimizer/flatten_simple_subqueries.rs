use crate::optimizer::utils::{
    and_merge, projection_is_direct_columns, select_is_simple_no_cardinality, walk_statement_mut,
};
use sqlparser::ast as S;

/// Сплющивание тривиальных подзапросов `SELECT ... FROM (SELECT ...) AS s`.
///
/// Условия (консервативно, без изменения кардинальности):
/// - подзапрос во `FROM` — это `Query { body: Select(...) }` без `LIMIT/FETCH/ORDER BY`;
/// - внутри `Select` НЕТ `DISTINCT/GROUP BY/HAVING`, один источник во `FROM` и без `JOIN`;
/// - проекция подзапроса — только прямые ссылки на колонки (`col` или `u.col`), без `*`, алиасов и выражений.
///
/// Действия:
/// - заменить `FROM (SELECT col_list FROM T [WHERE ...]) AS s` на `FROM T AS s`;
/// - предикат из внутреннего `WHERE` склеить конъюнкцией с внешним `WHERE`.
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
#[inline]
pub fn flatten_simple_subqueries(stmt: &mut sqlparser::ast::Statement) {
    #[inline]
    fn query_has_no_limit_or_order(q: &S::Query) -> bool {
        q.order_by.is_none() && q.limit_clause.is_none() && q.fetch.is_none()
    }

    #[inline]
    fn derived_is_trivial(tf: &S::TableFactor) -> bool {
        match tf {
            S::TableFactor::Derived { subquery, .. } => {
                if !query_has_no_limit_or_order(subquery) {
                    return false;
                }
                match subquery.body.as_ref() {
                    S::SetExpr::Select(sel) => {
                        let sel = sel.as_ref();
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

    fn try_flatten_in_select(sel: &mut S::Select) {
        if sel.from.is_empty() {
            return;
        }
        let can_flatten = matches!(&sel.from[0].relation, S::TableFactor::Derived { .. })
            && derived_is_trivial(&sel.from[0].relation);
        if !can_flatten {
            return;
        }

        // забираем inner WHERE до замены relation
        let mut inner_where: Option<S::Expr> = None;
        if let S::TableFactor::Derived { subquery, .. } = &mut sel.from[0].relation {
            if let S::SetExpr::Select(inner_sel) = subquery.body.as_mut() {
                inner_where = inner_sel.as_mut().selection.take();
            }
        }

        // заменить relation: берём единственный источник из inner FROM и переносим alias
        if let S::TableFactor::Derived {
            subquery, alias, ..
        } = &mut sel.from[0].relation
        {
            let outer_alias = alias.clone();
            if let S::SetExpr::Select(inner_sel) = subquery.body.as_mut() {
                let inner = inner_sel.as_mut();
                let mut inner_twj = inner.from.remove(0);
                match &mut inner_twj.relation {
                    S::TableFactor::Table {
                        alias: base_alias, ..
                    } => {
                        *base_alias = outer_alias.clone();
                    }
                    _ => return, // защитный ранний выход
                }
                sel.from[0].relation = inner_twj.relation;
            }
        }

        // слить предикаты
        and_merge(&mut sel.selection, inner_where.take());
    }

    // проход по всему Statement
    walk_statement_mut(
        stmt,
        &mut |q: &mut S::Query, _top_level: bool| {
            if let S::SetExpr::Select(sel_box) = q.body.as_mut() {
                try_flatten_in_select(sel_box.as_mut());
            }
        },
        &mut |_e: &mut S::Expr| {},
    );
}
