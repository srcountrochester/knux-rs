use sqlparser::ast::{self as S, Insert, Join, JoinConstraint, JoinOperator};

/// Удаляет `ORDER BY` из **подзапросов** (не верхнего уровня) там, где он
/// не влияет на результат: при отсутствии `LIMIT`/`OFFSET`/`FETCH`.
///
/// Обоснование:
/// - В PostgreSQL порядок строк без верхнего `ORDER BY` не определён; вложенная
///   сортировка результата не гарантирует порядок снаружи. Это даёт право
///   удалять внутренний `ORDER BY`, если он не ограничивает выборку
///   (`LIMIT`/`FETCH`). См. док.: «A particular output ordering can only be
///   guaranteed if the sort step is explicitly chosen.» :contentReference[oaicite:3]{index=3}
/// - В MySQL для подзапросов в `IN/EXISTS` (семиджоины) `ORDER BY` допускается,
///   но игнорируется оптимизатором, т.к. для логики предиката порядок не важен. :contentReference[oaicite:4]{index=4}
/// - В SQLite реализована оптимизация исключения `ORDER BY` у подзапросов
///   (в т.ч. во `FROM`), если выполняется ряд условий, ключевое — отсутствие
///   `LIMIT`. :contentReference[oaicite:5]{index=5}
///
/// Функция **мутирует** переданный `Statement` и ничего не возвращает.
/// Условия удаления:
/// - узел — именно *подзапрос* (не верхний);
/// - у подзапроса нет `limit_clause` и `fetch`;
/// - у подзапроса есть `order_by`.
#[inline]
pub fn rm_subquery_order_by(stmt: &mut S::Statement) {
    // Вспомогательные рекурсивные обходчики

    // Проверка наличия ограничения выборки (любая форма LIMIT/OFFSET/FETCH)
    #[inline]
    fn has_limit_or_fetch(q: &S::Query) -> bool {
        q.limit_clause.is_some() || q.fetch.is_some()
    }

    // Главный обработчик запроса; `top_level = true` только для корня
    fn visit_query(q: &mut S::Query, top_level: bool) {
        // 1) WITH: обойти CTE как подзапросы
        if let Some(w) = &mut q.with {
            for cte in &mut w.cte_tables {
                visit_query(&mut cte.query, false);
            }
        }

        // 2) Тело запроса
        visit_set_expr(&mut q.body, false);

        // 3) Сам q: если это подзапрос и у него нет LIMIT/OFFSET/FETCH — снимаем ORDER BY
        if !top_level && q.order_by.is_some() && !has_limit_or_fetch(q) {
            q.order_by = None;
        }
    }

    // Обход SetExpr
    fn visit_set_expr(se: &mut S::SetExpr, _in_subquery: bool) {
        use S::SetExpr::*;
        match se {
            Select(sel) => visit_select(sel),
            Query(subq) => visit_query(subq, false),
            SetOperation { left, right, .. } => {
                visit_set_expr(left, true);
                visit_set_expr(right, true);
            }
            _ => {}
        }
    }

    // Обход SELECT
    fn visit_select(sel: &mut S::Select) {
        // FROM (включая JOIN)
        for twj in &mut sel.from {
            visit_table_with_joins(twj);
        }

        // Проекция может содержать подзапросы в выражениях
        for it in &mut sel.projection {
            if let S::SelectItem::UnnamedExpr(e) | S::SelectItem::ExprWithAlias { expr: e, .. } = it
            {
                visit_expr(e);
            }
        }

        // WHERE
        if let Some(e) = &mut sel.selection {
            visit_expr(e);
        }
        // HAVING
        if let Some(e) = &mut sel.having {
            visit_expr(e);
        }
        // GROUP BY (только выражения)
        if let S::GroupByExpr::Expressions(exprs, _mods) = &mut sel.group_by {
            for e in exprs {
                visit_expr(e);
            }
        }
        // DISTINCT ON (...)
        if let Some(S::Distinct::On(list)) = &mut sel.distinct {
            for e in list {
                visit_expr(e);
            }
        }
        // Остальные поля SELECT подзапросов не содержат.
    }

    // FROM/JOIN
    fn visit_table_with_joins(twj: &mut S::TableWithJoins) {
        visit_table_factor(&mut twj.relation);
        for j in &mut twj.joins {
            visit_table_factor(&mut j.relation);
            visit_join(j, visit_expr);
        }
    }
    fn visit_join(j: &mut Join, mut visit_expr: impl FnMut(&mut S::Expr)) {
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
                // Snowflake ASOF: отдельное поле match_condition + constraint
                visit_expr(match_condition);
                if let JoinConstraint::On(e) = constraint {
                    visit_expr(e);
                }
            }
            JoinOperator::CrossJoin | JoinOperator::CrossApply | JoinOperator::OuterApply => {
                // нет выражений
            }
        }
    }

    fn visit_table_factor(tf: &mut S::TableFactor) {
        match tf {
            S::TableFactor::Derived { subquery, .. } => visit_query(subquery, false),
            S::TableFactor::NestedJoin {
                table_with_joins, ..
            } => visit_table_with_joins(table_with_joins),
            _ => {}
        }
    }

    fn visit_case(
        operand: &mut Option<Box<S::Expr>>,
        whens: &mut Vec<S::CaseWhen>,
        else_result: &mut Option<Box<S::Expr>>,
        mut visit_expr: impl FnMut(&mut S::Expr),
    ) {
        if let Some(op) = operand.as_mut() {
            visit_expr(op);
        }
        for when in whens.iter_mut() {
            visit_expr(&mut when.condition);
            visit_expr(&mut when.result);
        }
        if let Some(er) = else_result.as_mut() {
            visit_expr(er);
        }
    }

    // Обход выражений — обрабатываем только те варианты, где встречаются подзапросы;
    // остальное проходим «по детям», чтобы добраться до подзапросов внутри.
    fn visit_expr(e: &mut S::Expr) {
        use S::Expr::*;
        match e {
            Subquery(q) => visit_query(q, false),
            Exists { subquery, .. } => visit_query(subquery, false),
            InSubquery { expr, subquery, .. } => {
                visit_expr(expr);
                visit_query(subquery, false);
            }

            // Рекурсивный спуск по вложенным выражениям
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
            Case {
                operand,
                conditions,
                else_result,
                ..
            } => visit_case(operand, conditions, else_result, |e| visit_expr(e)),

            // Функции: возможен подзапрос в аргументах
            Function(S::Function { args, .. }) => {
                use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
                match args {
                    FunctionArguments::List(list) => {
                        for a in &mut list.args {
                            match a {
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(x)) => visit_expr(x),
                                FunctionArg::Named {
                                    arg: FunctionArgExpr::Expr(x),
                                    ..
                                } => visit_expr(x),
                                _ => {}
                            }
                        }
                    }
                    FunctionArguments::Subquery(q) => visit_query(q, false),
                    _ => {}
                }
            }

            // Остальные варианты выражений не содержат подзапросов
            _ => {}
        }
    }

    // Старт: обойти Statement
    match stmt {
        S::Statement::Query(q) => visit_query(q, true),

        S::Statement::Insert(Insert { source, .. }) => {
            if let Some(q) = source.as_mut() {
                visit_query(q, false);
            }
        }

        S::Statement::Update {
            selection,
            assignments,
            from,
            table: twj,
            ..
        } => {
            // SET col = <expr>
            for a in assignments {
                visit_expr(&mut a.value);
            }
            // WHERE
            if let Some(e) = selection {
                visit_expr(e);
            }
            // Основная таблица может быть NestedJoin
            visit_table_with_joins(twj);
            // FROM (таблицы; подзапросы в текущих билдерах не используются, но обойдём на всякий случай)
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
            // FROM
            match from {
                S::FromTable::WithFromKeyword(list) | S::FromTable::WithoutKeyword(list) => {
                    for t in list {
                        visit_table_with_joins(t);
                    }
                }
            }
            if let Some(list) = using {
                for t in list {
                    visit_table_with_joins(t);
                }
            }
        }

        // Остальные виды Statement игнорируем
        _ => {}
    }
}
