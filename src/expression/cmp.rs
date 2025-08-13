use super::Expression;
use smallvec::smallvec;
use sqlparser::ast;

fn bin(op: ast::BinaryOperator, left: Expression, mut right: Expression) -> Expression {
    let mut params = left.params;
    params.append(&mut right.params);
    Expression {
        expr: ast::Expr::BinaryOp {
            left: Box::new(left.expr),
            op,
            right: Box::new(right.expr),
        },
        alias: None,
        params,
        mark_distinct_for_next: false,
    }
}

impl Expression {
    pub fn eq(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::Eq, self, rhs)
    }
    pub fn ne(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::NotEq, self, rhs)
    }
    pub fn gt(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::Gt, self, rhs)
    }
    pub fn gte(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::GtEq, self, rhs)
    }
    pub fn lt(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::Lt, self, rhs)
    }
    pub fn lte(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::LtEq, self, rhs)
    }

    /// `IN ( ... )` — элементы как выражения (поддерживают параметры)
    pub fn isin<I>(self, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>,
    {
        let mut params = self.params;
        let mut exprs = vec![];
        for mut e in items.into_iter() {
            params.append(&mut e.params);
            exprs.push(e.expr);
        }
        Expression {
            expr: ast::Expr::InList {
                expr: Box::new(self.expr),
                list: exprs,
                negated: false,
            },
            alias: None,
            params,
            mark_distinct_for_next: false,
        }
    }

    pub fn notin<I>(self, items: I) -> Expression
    where
        I: IntoIterator<Item = Expression>,
    {
        let mut params = self.params;
        let mut exprs = vec![];
        for mut e in items.into_iter() {
            params.append(&mut e.params);
            exprs.push(e.expr);
        }
        Expression {
            expr: ast::Expr::InList {
                expr: Box::new(self.expr),
                list: exprs,
                negated: true,
            },
            alias: None,
            params,
            mark_distinct_for_next: false,
        }
    }

    pub fn is_null(self) -> Expression {
        Expression {
            expr: ast::Expr::IsNull(Box::new(self.expr)),
            alias: None,
            params: smallvec![],
            mark_distinct_for_next: false,
        }
    }

    pub fn is_not_null(self) -> Expression {
        Expression {
            expr: ast::Expr::IsNotNull(Box::new(self.expr)),
            alias: None,
            params: smallvec![],
            mark_distinct_for_next: false,
        }
    }
}
