use crate::expression::Expression;

/// Трейт-расширение: позволяет писать
/// col("a").eq(col("b")).andOn(col("x").eq(val(1)))
pub trait JoinOnExt {
    fn and_on(self, rhs: Expression) -> Expression;
    fn or_on(self, rhs: Expression) -> Expression;
}

impl JoinOnExt for Expression {
    fn and_on(self, rhs: Expression) -> Expression {
        self.and(rhs)
    }
    fn or_on(self, rhs: Expression) -> Expression {
        self.or(rhs)
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

impl Expression {
    /// `(self) AND (rhs)` — удобнее читать в контексте JOIN ON
    #[inline]
    pub fn and_on(self, rhs: Expression) -> Expression {
        self.and(rhs)
    }

    /// `(self) OR (rhs)` — удобнее читать в контексте JOIN ON
    #[inline]
    pub fn or_on(self, rhs: Expression) -> Expression {
        self.or(rhs)
    }
}
