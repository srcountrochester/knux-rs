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
    // ===== helpers =====

    #[inline]
    fn query_has_no_limit_or_order(q: &S::Query) -> bool {
        q.order_by.is_none() && q.limit_clause.is_none() && q.fetch.is_none()
    }

    // Конструкция (Derived subquery) → можно ли поднять?
    fn derived_can_be_pulled(tf: &S::TableFactor) -> bool {
        match tf {
            S::TableFactor::Derived { subquery, .. } => {
                if !query_has_no_limit_or_order(subquery) {
                    return false;
                }
                match &subquery.body.as_ref() {
                    S::SetExpr::Select(sel) => {
                        if !select_is_simple_no_cardinality(&sel) {
                            return false;
                        }
                        // FROM внутри: ровно один источник и без JOIN
                        if sel.from.len() != 1 || !sel.from[0].joins.is_empty() {
                            return false;
                        }
                        // Проекции — только прямые колонки, без alias’ов
                        projection_is_direct_columns(sel)
                    }
                    _ => false, // любые SET-операции запрещаем
                }
            }
            _ => false,
        }
    }

    // Поднять один derived-подзапрос в конкретном `TableWithJoins`
    fn pull_up_in_table_with_joins(twj: &mut S::TableWithJoins) {
        // обрабатываем только сам relation; JOIN’ы не трогаем (консервативно)
        if !derived_can_be_pulled(&twj.relation) {
            return;
        }

        // безопасно распаковано по условиям derived_can_be_pulled()
        if let S::TableFactor::Derived {
            subquery, alias, ..
        } = &mut twj.relation
        {
            let outer_alias = alias.clone(); // alias должен быть у derived; если его нет — не меняем (но билдерами он задаётся)

            // извлечь Select внутри subquery
            let inner_sel: &mut S::Select = match subquery.body.as_mut() {
                S::SetExpr::Select(s) => s.as_mut(), // s: &mut Box<S::Select> → &mut S::Select
                _ => return,
            };

            // единственный источник внутри подзапроса
            let mut inner_twj = inner_sel.from.remove(0);

            // relation базовой таблицы — ожидаем именованный источник
            match &mut inner_twj.relation {
                S::TableFactor::Table {
                    alias: base_alias, ..
                } => {
                    // присвоить alias от внешнего derived (сохраняем внешний псевдоним)
                    *base_alias = outer_alias.clone();
                }
                // любые другие факторы (UNNEST, функции и пр.) — не трогаем
                _ => return,
            }

            // перенести relation наружу
            twj.relation = inner_twj.relation;

            // слить предикаты: WHERE из подзапроса → во внешний SELECT
            // для этого нужен доступ к внешнему SELECT — делаем это в вызывающем месте
            // (здесь только возвращаем inner_sel.selection через Option, см. ниже)
        }
    }

    // визиторы

    fn visit_expr(e: &mut S::Expr) {
        use S::Expr::*;
        match e {
            Subquery(q) => visit_query(q),
            Exists { subquery, .. } => visit_query(subquery),
            InSubquery { expr, subquery, .. } => {
                visit_expr(expr);
                visit_query(subquery);
            }

            UnaryOp { expr, .. } => visit_expr(expr),
            BinaryOp { left, right, .. } => {
                visit_expr(left);
                visit_expr(right);
            }
            Between {
                expr, low, high, ..
            } => {
                visit_expr(expr);
                visit_expr(low);
                visit_expr(high);
            }
            Cast { expr, .. } => visit_expr(expr),
            Extract { expr, .. } => visit_expr(expr),
            Nested(inner) => visit_expr(inner),

            S::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(op) = operand.as_mut() {
                    visit_expr(op);
                }
                for when in conditions.iter_mut() {
                    visit_expr(&mut when.condition);
                    visit_expr(&mut when.result);
                }
                if let Some(er) = else_result.as_mut() {
                    visit_expr(er);
                }
            }

            S::Expr::Function(S::Function { args, .. }) => {
                use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
                match args {
                    FunctionArguments::List(list) => {
                        for a in &mut list.args {
                            match a {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(x))
                                | FunctionArg::Named {
                                    arg: FunctionArgExpr::Expr(x),
                                    ..
                                } => visit_expr(x),
                                _ => {}
                            }
                        }
                    }
                    FunctionArguments::Subquery(q) => visit_query(q),
                    _ => {}
                }
            }

            _ => {}
        }
    }

    fn visit_join(j: &mut S::Join) {
        use S::{JoinConstraint, JoinOperator};
        match &mut j.join_operator {
            JoinOperator::Join(c)
            | JoinOperator::Inner(c)
            | JoinOperator::Left(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::Right(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c)
            | JoinOperator::Semi(c)
            | JoinOperator::LeftSemi(c)
            | JoinOperator::RightSemi(c)
            | JoinOperator::Anti(c)
            | JoinOperator::LeftAnti(c)
            | JoinOperator::RightAnti(c)
            | JoinOperator::StraightJoin(c) => {
                if let JoinConstraint::On(e) = c {
                    visit_expr(e);
                }
            }
            JoinOperator::AsOf {
                match_condition,
                constraint,
            } => {
                visit_expr(match_condition);
                if let S::JoinConstraint::On(e) = constraint {
                    visit_expr(e);
                }
            }
            JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {}
        }
    }

    // Внешний SELECT: пробуем поднять одинокий derived-источник без JOIN,
    // после чего сливаем предикаты inner WHERE → outer WHERE.
    fn visit_select(sel: &mut S::Select) {
        // сначала обойти все вложенные конструкции
        for it in &mut sel.projection {
            if let S::SelectItem::UnnamedExpr(e) | S::SelectItem::ExprWithAlias { expr: e, .. } = it
            {
                visit_expr(e);
            }
        }
        if let Some(e) = &mut sel.selection {
            visit_expr(e);
        }
        if let Some(e) = &mut sel.having {
            visit_expr(e);
        }
        if let S::GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
            for e in exprs {
                visit_expr(e);
            }
        }
        for twj in &mut sel.from {
            // рекурсивно обойти relation/joins
            visit_table_with_joins(twj);
        }

        // Консервативная часть: поднимать допускаем только если этот SELECT
        // имеет единственный источник во FROM и без JOIN'ов.
        let can_pull = {
            let rel = &sel.from[0].relation;
            derived_can_be_pulled(rel)
        };

        if can_pull {
            let mut inner_where: Option<S::Expr> = None;

            if let S::TableFactor::Derived { subquery, .. } = &mut sel.from[0].relation {
                if let S::SetExpr::Select(inner_sel) = subquery.body.as_mut() {
                    inner_where = inner_sel.as_mut().selection.take();
                }
            }

            // Заменяем relation и сливаем предикаты
            pull_up_in_table_with_joins(&mut sel.from[0]);
            and_merge(&mut sel.selection, inner_where.take());
        }
    }

    fn visit_table_with_joins(twj: &mut S::TableWithJoins) {
        visit_table_factor(&mut twj.relation);
        for j in &mut twj.joins {
            visit_table_factor(&mut j.relation);
            visit_join(j);
        }
    }
    fn visit_table_factor(tf: &mut S::TableFactor) {
        match tf {
            S::TableFactor::Derived { subquery, .. } => visit_query(subquery),
            S::TableFactor::NestedJoin {
                table_with_joins, ..
            } => visit_table_with_joins(table_with_joins),
            _ => {}
        }
    }

    fn visit_set_expr(se: &mut S::SetExpr) {
        match se {
            S::SetExpr::Select(s) => visit_select(s),
            S::SetExpr::Query(q) => visit_query(q),
            S::SetExpr::SetOperation { left, right, .. } => {
                visit_set_expr(left);
                visit_set_expr(right);
            }
            _ => {}
        }
    }

    fn visit_query(q: &mut S::Query) {
        if let Some(w) = &mut q.with {
            for cte in &mut w.cte_tables {
                visit_query(&mut cte.query);
            }
        }
        visit_set_expr(&mut q.body);
        // НЕЛЬЗЯ менять сам q.order_by/limit в этом проходе — это делает другой pass
    }

    // точка входа
    match stmt {
        S::Statement::Query(q) => visit_query(q),

        S::Statement::Insert(S::Insert { source, .. }) => {
            if let Some(q) = source.as_mut() {
                visit_query(q);
            }
        }

        S::Statement::Update {
            selection,
            assignments,
            from,
            table: twj,
            ..
        } => {
            for a in assignments {
                visit_expr(&mut a.value);
            }
            if let Some(e) = selection {
                visit_expr(e);
            }
            visit_table_with_joins(twj);
            if let Some(kind) = from {
                match kind {
                    S::UpdateTableFromKind::BeforeSet(list)
                    | S::UpdateTableFromKind::AfterSet(list) => {
                        for t in list {
                            visit_table_with_joins(t);
                        }
                    }
                }
            }
        }

        S::Statement::Delete(S::Delete {
            selection,
            using,
            from,
            ..
        }) => {
            if let Some(e) = selection {
                visit_expr(e);
            }
            match from {
                S::FromTable::WithoutKeyword(list) | S::FromTable::WithFromKeyword(list) => {
                    for twj in list {
                        visit_table_with_joins(twj);
                    }
                }
            }
            if let Some(list) = using {
                for t in list {
                    visit_table_with_joins(t);
                }
            }
        }

        _ => {}
    }
}
