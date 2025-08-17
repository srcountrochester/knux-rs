use sqlparser::ast;

use crate::expression::Expression;

/// Трейт-расширение: позволяет писать
/// col("a").eq(col("b")).andOn(col("x").eq(val(1)))
pub trait JoinOnExt {
    fn and_on(self, rhs: Expression) -> Expression;
    fn or_on(self, rhs: Expression) -> Expression;

    // IN / NOT IN
    fn on_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>;
    fn or_on_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>;

    fn on_not_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>;
    fn or_on_not_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>;

    // NULL checks
    fn on_null(self, expr: Expression) -> Expression;
    fn or_on_null(self, expr: Expression) -> Expression;

    fn on_not_null(self, expr: Expression) -> Expression;
    fn or_on_not_null(self, expr: Expression) -> Expression;

    // BETWEEN
    fn on_between(self, target: Expression, low: Expression, high: Expression) -> Expression;
    fn or_on_between(self, target: Expression, low: Expression, high: Expression) -> Expression;

    fn on_not_between(self, target: Expression, low: Expression, high: Expression) -> Expression;
    fn or_on_not_between(self, target: Expression, low: Expression, high: Expression)
    -> Expression;

    // EXISTS / NOT EXISTS (под капотом ожидаем подзапрос)
    fn on_exists(self, subquery: Expression) -> Expression;
    fn or_on_exists(self, subquery: Expression) -> Expression;

    fn on_not_exists(self, subquery: Expression) -> Expression;
    fn or_on_not_exists(self, subquery: Expression) -> Expression;
}

impl JoinOnExt for Expression {
    #[inline]
    fn and_on(self, rhs: Expression) -> Expression {
        self.and(rhs)
    }
    #[inline]
    fn or_on(self, rhs: Expression) -> Expression {
        self.or(rhs)
    }

    // ----- IN / NOT IN -----
    #[inline]
    fn on_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>,
    {
        self.and(target.isin(items))
    }

    #[inline]
    fn or_on_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>,
    {
        self.or(target.isin(items))
    }

    #[inline]
    fn on_not_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>,
    {
        self.and(target.notin(items))
    }

    #[inline]
    fn or_on_not_in<I>(self, target: Expression, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>,
    {
        self.or(target.notin(items))
    }

    // ----- NULL / NOT NULL -----
    #[inline]
    fn on_null(self, expr: Expression) -> Expression {
        self.and(expr.is_null())
    }

    #[inline]
    fn or_on_null(self, expr: Expression) -> Expression {
        self.or(expr.is_null())
    }

    #[inline]
    fn on_not_null(self, expr: Expression) -> Expression {
        self.and(expr.is_not_null())
    }

    #[inline]
    fn or_on_not_null(self, expr: Expression) -> Expression {
        self.or(expr.is_not_null())
    }

    // ----- BETWEEN / NOT BETWEEN -----
    #[inline]
    fn on_between(self, target: Expression, low: Expression, high: Expression) -> Expression {
        let between = build_between_expr(target, low, high, false);
        self.and(between)
    }

    #[inline]
    fn or_on_between(self, target: Expression, low: Expression, high: Expression) -> Expression {
        let between = build_between_expr(target, low, high, false);
        self.or(between)
    }

    #[inline]
    fn on_not_between(self, target: Expression, low: Expression, high: Expression) -> Expression {
        let between = build_between_expr(target, low, high, true);
        self.and(between)
    }

    #[inline]
    fn or_on_not_between(
        self,
        target: Expression,
        low: Expression,
        high: Expression,
    ) -> Expression {
        let between = build_between_expr(target, low, high, true);
        self.or(between)
    }

    // ----- EXISTS / NOT EXISTS -----
    #[inline]
    fn on_exists(self, sub: Expression) -> Expression {
        match sub.expr {
            ast::Expr::Subquery(q) => {
                let exists = Expression {
                    expr: ast::Expr::Exists {
                        subquery: q,
                        negated: false,
                    },
                    alias: None,
                    params: sub.params,
                    mark_distinct_for_next: false,
                };
                self.and(exists)
            }
            ast::Expr::Exists { .. } => self.and(sub),
            _ => self.and(sub), // если это не подзапрос — рассматриваем как обычное булево выражение
        }
    }

    #[inline]
    fn or_on_exists(self, sub: Expression) -> Expression {
        match sub.expr {
            ast::Expr::Subquery(q) => {
                let exists = Expression {
                    expr: ast::Expr::Exists {
                        subquery: q,
                        negated: false,
                    },
                    alias: None,
                    params: sub.params,
                    mark_distinct_for_next: false,
                };
                self.or(exists)
            }
            ast::Expr::Exists { .. } => self.or(sub),
            _ => self.or(sub),
        }
    }

    #[inline]
    fn on_not_exists(self, sub: Expression) -> Expression {
        match sub.expr {
            ast::Expr::Subquery(q) => {
                let exists = Expression {
                    expr: ast::Expr::Exists {
                        subquery: q,
                        negated: true,
                    },
                    alias: None,
                    params: sub.params,
                    mark_distinct_for_next: false,
                };
                self.and(exists)
            }
            ast::Expr::Exists { .. } => {
                let neg = Expression {
                    expr: ast::Expr::UnaryOp {
                        op: ast::UnaryOperator::Not,
                        expr: Box::new(sub.expr),
                    },
                    alias: None,
                    params: sub.params,
                    mark_distinct_for_next: false,
                };
                self.and(neg)
            }
            _ => self.and(sub.not()),
        }
    }

    #[inline]
    fn or_on_not_exists(self, sub: Expression) -> Expression {
        match sub.expr {
            ast::Expr::Subquery(q) => {
                let exists = Expression {
                    expr: ast::Expr::Exists {
                        subquery: q,
                        negated: true,
                    },
                    alias: None,
                    params: sub.params,
                    mark_distinct_for_next: false,
                };
                self.or(exists)
            }
            ast::Expr::Exists { .. } => {
                let neg = Expression {
                    expr: ast::Expr::UnaryOp {
                        op: ast::UnaryOperator::Not,
                        expr: Box::new(sub.expr),
                    },
                    alias: None,
                    params: sub.params,
                    mark_distinct_for_next: false,
                };
                self.or(neg)
            }
            _ => self.or(sub.not()),
        }
    }
}

/// Билдер для замыкания: |on| on.on(...).andOn(...).orOn(...)
#[derive(Default)]
pub struct JoinOnBuilder {
    current: Option<Expression>,
}

impl JoinOnBuilder {
    /// Инициализирует (или заменяет) текущее ON-выражение
    pub fn on(mut self, expr: Expression) -> Self {
        self.current = Some(expr);
        self
    }

    /// Добавляет `AND <expr>` к текущему ON (если on(...) ещё не вызывали — эквивалент .on(expr))
    pub fn and_on(mut self, expr: Expression) -> Self {
        self.current = Some(match self.current {
            Some(acc) => acc.and(expr),
            None => expr,
        });
        self
    }

    /// Добавляет `OR <expr>` к текущему ON (если on(...) ещё не вызывали — эквивалент .on(expr))
    pub fn or_on(mut self, expr: Expression) -> Self {
        self.current = Some(match self.current {
            Some(acc) => acc.or(expr),
            None => expr,
        });
        self
    }

    // ---------- IN / NOT IN ----------

    /// AND <target> IN (<items...>)
    pub fn on_in<I>(mut self, target: Expression, items: I) -> Self
    where
        I: IntoIterator<Item = Expression>,
    {
        let cond = target.isin(items);
        self = self.and_on(cond);
        self
    }

    /// OR <target> IN (<items...>)
    pub fn or_on_in<I>(mut self, target: Expression, items: I) -> Self
    where
        I: IntoIterator<Item = Expression>,
    {
        let cond = target.isin(items);
        self = self.or_on(cond);
        self
    }

    /// AND <target> NOT IN (<items...>)
    pub fn on_not_in<I>(mut self, target: Expression, items: I) -> Self
    where
        I: IntoIterator<Item = Expression>,
    {
        let cond = target.notin(items);
        self = self.and_on(cond);
        self
    }

    /// OR <target> NOT IN (<items...>)
    pub fn or_on_not_in<I>(mut self, target: Expression, items: I) -> Self
    where
        I: IntoIterator<Item = Expression>,
    {
        let cond = target.notin(items);
        self = self.or_on(cond);
        self
    }

    // ---------- NULL / NOT NULL ----------

    /// AND <expr> IS NULL
    pub fn on_null(mut self, expr: Expression) -> Self {
        let cond = expr.is_null();
        self = self.and_on(cond);
        self
    }

    /// OR <expr> IS NULL
    pub fn or_on_null(mut self, expr: Expression) -> Self {
        let cond = expr.is_null();
        self = self.or_on(cond);
        self
    }

    /// AND <expr> IS NOT NULL
    pub fn on_not_null(mut self, expr: Expression) -> Self {
        let cond = expr.is_not_null();
        self = self.and_on(cond);
        self
    }

    /// OR <expr> IS NOT NULL
    pub fn or_on_not_null(mut self, expr: Expression) -> Self {
        let cond = expr.is_not_null();
        self = self.or_on(cond);
        self
    }

    // ---------- BETWEEN / NOT BETWEEN ----------

    /// AND <target> BETWEEN <low> AND <high>
    pub fn on_between(mut self, target: Expression, low: Expression, high: Expression) -> Self {
        let cond = build_between_expr(target, low, high, false);
        self = self.and_on(cond);
        self
    }

    /// OR <target> BETWEEN <low> AND <high>
    pub fn or_on_between(mut self, target: Expression, low: Expression, high: Expression) -> Self {
        let cond = build_between_expr(target, low, high, false);
        self = self.or_on(cond);
        self
    }

    /// AND <target> NOT BETWEEN <low> AND <high>
    pub fn on_not_between(mut self, target: Expression, low: Expression, high: Expression) -> Self {
        let cond = build_between_expr(target, low, high, true);
        self = self.and_on(cond);
        self
    }

    /// OR <target> NOT BETWEEN <low> AND <high>
    pub fn or_on_not_between(
        mut self,
        target: Expression,
        low: Expression,
        high: Expression,
    ) -> Self {
        let cond = build_between_expr(target, low, high, true);
        self = self.or_on(cond);
        self
    }

    // ---------- EXISTS / NOT EXISTS ----------

    /// AND EXISTS(<subquery>)
    pub fn on_exists(mut self, sub: Expression) -> Self {
        let cond = build_exists_expr(sub, false);
        self = self.and_on(cond);
        self
    }

    /// OR EXISTS(<subquery>)
    pub fn or_on_exists(mut self, sub: Expression) -> Self {
        let cond = build_exists_expr(sub, false);
        self = self.or_on(cond);
        self
    }

    /// AND NOT EXISTS(<subquery>)
    pub fn on_not_exists(mut self, sub: Expression) -> Self {
        let cond = build_exists_expr(sub, true);
        self = self.and_on(cond);
        self
    }

    /// OR NOT EXISTS(<subquery>)
    pub fn or_on_not_exists(mut self, sub: Expression) -> Self {
        let cond = build_exists_expr(sub, true);
        self = self.or_on(cond);
        self
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.current.is_none()
    }

    pub fn build(self) -> Option<Expression> {
        self.current
    }

    pub fn build_or(self, default: Expression) -> Expression {
        self.current.unwrap_or(default)
    }
}

#[inline]
fn build_between_expr(
    target: Expression,
    mut low: Expression,
    mut high: Expression,
    negated: bool,
) -> Expression {
    let mut params = target.params;
    params.append(&mut low.params);
    params.append(&mut high.params);

    Expression {
        expr: ast::Expr::Between {
            expr: Box::new(target.expr),
            negated,
            low: Box::new(low.expr),
            high: Box::new(high.expr),
        },
        alias: None,
        params,
        mark_distinct_for_next: false,
    }
}

#[inline]
fn build_exists_expr(sub: Expression, negated: bool) -> Expression {
    match sub.expr {
        ast::Expr::Subquery(q) => Expression {
            expr: ast::Expr::Exists {
                subquery: q,
                negated,
            },
            alias: None,
            params: sub.params,
            mark_distinct_for_next: false,
        },
        ast::Expr::Exists { .. } => {
            if negated {
                // NOT (EXISTS ...)
                Expression {
                    expr: ast::Expr::UnaryOp {
                        op: ast::UnaryOperator::Not,
                        expr: Box::new(sub.expr),
                    },
                    alias: None,
                    params: sub.params,
                    mark_distinct_for_next: false,
                }
            } else {
                // уже EXISTS — используем как есть
                sub
            }
        }
        _ => {
            // не подзапрос: трактуем как булево выражение (+ NOT при необходимости)
            if negated { sub.not() } else { sub }
        }
    }
}
