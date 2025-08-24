use smallvec::SmallVec;
use sqlparser::ast::{Expr as SqlExpr, Query, SelectItem};

use crate::param::Param;
use crate::query_builder::select::SelectItemNode;
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};

#[derive(Debug, Clone)]
pub(crate) struct DistinctOnNode {
    pub expr: SqlExpr,
    pub params: SmallVec<[Param; 8]>,
}

impl DistinctOnNode {
    #[inline]
    pub fn new(expr: SqlExpr, params: SmallVec<[Param; 8]>) -> Self {
        Self { expr, params }
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    /// DISTINCT [<expr>, <expr>, ...]
    ///
    /// Поведение «в духе knex»:
    /// - Если переданы выражения — они добавляются в SELECT (как `select(...)`) и
    ///   включается флаг DISTINCT.
    /// - Если список пуст — просто включается DISTINCT, а проектирование остаётся как было
    ///   (или в рендере упадёт на `*`, если в SELECT ничего не добавлялось).
    pub fn distinct<A>(mut self, items: A) -> Self
    where
        A: ArgList<'a>,
    {
        let list = items.into_vec();
        for it in list {
            match it {
                QBArg::Expr(e) => {
                    // кладём в SELECT как UnnamedExpr + локальные параметры узла
                    self.push_select_expr(e.expr, e.params);
                }
                QBArg::Subquery(qb) => {
                    if let Ok((q, p)) = qb.build_query_ast() {
                        self.push_select_subquery(q, p);
                    }
                }
                QBArg::Closure(c) => {
                    let built = c.call(QueryBuilder::new_empty());
                    if let Ok((q, p)) = built.build_query_ast() {
                        self.push_select_subquery(q, p);
                    }
                }
            }
        }
        self.select_distinct = true;
        self
    }

    /// DISTINCT ON (<exprs...>) — как в knex.distinctOn(...)
    ///
    /// PostgreSQL-only по стандарту. Здесь просто сохраняем выражения;
    /// валидатор/рендер позаботится о диалектной специфике (в строгом режиме).
    /// Проекция (SELECT ...) **не меняется**.
    pub fn distinct_on<A>(mut self, items: A) -> Self
    where
        A: ArgList<'a>,
    {
        let list = items.into_vec();
        for it in list {
            match it {
                QBArg::Expr(e) => {
                    self.distinct_on_items.push(DistinctOnNode {
                        expr: e.expr,
                        params: e.params,
                    });
                }
                QBArg::Subquery(qb) => {
                    if let Ok((q, p)) = qb.build_query_ast() {
                        let expr = SqlExpr::Subquery(Box::new(q));
                        let mut sv = SmallVec::new();
                        sv.extend(p);
                        self.distinct_on_items
                            .push(DistinctOnNode { expr, params: sv });
                    }
                }
                QBArg::Closure(c) => {
                    let built = c.call(QueryBuilder::new_empty());
                    if let Ok((q, p)) = built.build_query_ast() {
                        let expr = SqlExpr::Subquery(Box::new(q));
                        let mut sv = SmallVec::new();
                        sv.extend(p);
                        self.distinct_on_items
                            .push(DistinctOnNode { expr, params: sv });
                    }
                }
            }
        }
        self
    }

    #[inline]
    pub(crate) fn push_select_expr(&mut self, expr: SqlExpr, mut params: SmallVec<[Param; 8]>) {
        self.select_items.push(SelectItemNode {
            item: SelectItem::UnnamedExpr(expr),
            params: {
                let mut out = SmallVec::new();
                out.append(&mut params);
                out
            },
        });
    }

    /// Добавить в SELECT подзапрос + его параметры
    #[inline]
    pub(crate) fn push_select_subquery(&mut self, q: Query, params: Vec<Param>) {
        let expr = SqlExpr::Subquery(Box::new(q));
        let mut sv: SmallVec<[Param; 8]> = SmallVec::new();
        sv.extend(params);
        self.select_items.push(SelectItemNode {
            item: SelectItem::UnnamedExpr(expr),
            params: sv,
        });
    }
}
