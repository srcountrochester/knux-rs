use super::Expression;
use sqlparser::ast;

fn combine(op: ast::BinaryOperator, mut left: Expression, mut right: Expression) -> Expression {
    let mut params = vec![];
    params.append(&mut left.params);
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
    pub fn and(self, rhs: Expression) -> Expression {
        combine(ast::BinaryOperator::And, self, rhs)
    }
    pub fn or(self, rhs: Expression) -> Expression {
        combine(ast::BinaryOperator::Or, self, rhs)
    }
    pub fn not(self) -> Expression {
        Expression {
            expr: ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Not,
                expr: Box::new(self.expr),
            },
            alias: None,
            params: self.params,
            mark_distinct_for_next: false,
        }
    }
}
