use super::Expression;
use sqlparser::ast::{
    DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentClause,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart,
};

pub fn fn_call_with_clauses(
    name: &str,
    args: Vec<Expr>,
    is_distinct: bool,
    clauses: Vec<FunctionArgumentClause>,
) -> Expr {
    let list = FunctionArgumentList {
        duplicate_treatment: Some(if is_distinct {
            DuplicateTreatment::Distinct
        } else {
            DuplicateTreatment::All
        }),
        args: args
            .into_iter()
            .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
            .collect(),
        clauses,
    };

    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(list),
        filter: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
        null_treatment: None,
    })
}

pub fn fn_call(name: &str, args: Vec<Expr>, is_distinct: bool) -> Expr {
    fn_call_with_clauses(name, args, is_distinct, Vec::new())
}

// COUNT(*)
fn fn_count_star(is_distinct: bool) -> Expr {
    let list = FunctionArgumentList {
        duplicate_treatment: Some(match is_distinct {
            true => DuplicateTreatment::Distinct,
            false => DuplicateTreatment::All,
        }),
        args: vec![FunctionArg::Unnamed(FunctionArgExpr::Wildcard)],
        clauses: vec![],
    };

    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("COUNT"))]),
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(list),
        filter: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
        null_treatment: None,
    })
}

impl Expression {
    /// COUNT(expr)
    pub fn count(mut self) -> Self {
        let is_distinct = self.mark_distinct_for_next;
        self.mark_distinct_for_next = false;

        self.alias = None;
        self.expr = fn_call("COUNT", vec![self.expr], is_distinct);
        self
    }

    /// COUNT(*)
    pub fn count_all(mut self) -> Self {
        let is_distinct = self.mark_distinct_for_next;
        self.mark_distinct_for_next = false;

        self.alias = None;
        self.expr = fn_count_star(is_distinct);
        self
    }

    pub fn max(mut self) -> Self {
        let is_distinct = self.mark_distinct_for_next;
        self.mark_distinct_for_next = false;

        self.alias = None;
        self.expr = fn_call("MAX", vec![self.expr], is_distinct);
        self
    }
    pub fn min(mut self) -> Self {
        let is_distinct = self.mark_distinct_for_next;
        self.mark_distinct_for_next = false;

        self.alias = None;
        self.expr = fn_call("MIN", vec![self.expr], is_distinct);
        self
    }
    pub fn sum(mut self) -> Self {
        let is_distinct = self.mark_distinct_for_next;
        self.mark_distinct_for_next = false;

        self.alias = None;
        self.expr = fn_call("SUM", vec![self.expr], is_distinct);
        self
    }
    pub fn avg(mut self) -> Self {
        let is_distinct = self.mark_distinct_for_next;
        self.mark_distinct_for_next = false;

        self.alias = None;
        self.expr = fn_call("AVG", vec![self.expr], is_distinct);
        self
    }

    /// Делает текущую функцию DISTINCT, либо помечает «следующий вызов функции будет DISTINCT».
    ///
    /// Примеры:
    /// - `col("id").count().distinct()` -> `COUNT(DISTINCT id)`
    /// - `col("id").distinct().count()` -> `COUNT(DISTINCT id)`
    pub fn distinct(mut self) -> Self {
        match &mut self.expr {
            Expr::Function(Function {
                args: FunctionArguments::List(list),
                ..
            }) => {
                list.duplicate_treatment = Some(DuplicateTreatment::Distinct);
            }
            _ => {
                self.mark_distinct_for_next = true;
            }
        }
        self
    }
}
