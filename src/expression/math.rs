use super::Expression;
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
    pub fn add(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::Plus, self, rhs)
    }
    pub fn sub(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::Minus, self, rhs)
    }
    pub fn mul(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::Multiply, self, rhs)
    }
    pub fn div(self, rhs: Expression) -> Expression {
        bin(ast::BinaryOperator::Divide, self, rhs)
    }
}
