use smallvec::SmallVec;
use sqlparser::ast::{Query as SqlQuery, SetOperator, SetQuantifier};

use crate::param::Param;
use crate::query_builder::{
    QueryBuilder,
    args::{ArgList, QBArg},
};

#[derive(Debug, Clone)]
pub(crate) struct SetOpNode {
    pub op: SetOperator,
    pub quantifier: SetQuantifier, // None (=> DISTINCT по умолчанию) или All
    pub right: SqlQuery,           // правая часть UNION
    pub params: SmallVec<[Param; 8]>,
}

impl SetOpNode {
    #[inline]
    pub fn new(
        op: SetOperator,
        quantifier: SetQuantifier,
        right: SqlQuery,
        params: Vec<Param>,
    ) -> Self {
        Self {
            op,
            quantifier,
            right,
            params: params.into(),
        }
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    /// UNION (по умолчанию — DISTINCT)
    pub fn union<L>(mut self, rhs: L) -> Self
    where
        L: ArgList<'a>,
    {
        self.push_setop_from_args(SetOperator::Union, SetQuantifier::None, rhs, "union()");
        self
    }

    /// UNION ALL
    pub fn union_all<L>(mut self, rhs: L) -> Self
    where
        L: ArgList<'a>,
    {
        self.push_setop_from_args(SetOperator::Union, SetQuantifier::All, rhs, "union_all()");
        self
    }

    /// INTERSECT (по умолчанию DISTINCT)
    pub fn intersect<L>(mut self, rhs: L) -> Self
    where
        L: ArgList<'a>,
    {
        self.push_setop_from_args(
            SetOperator::Intersect,
            SetQuantifier::None,
            rhs,
            "intersect()",
        );
        self
    }

    /// INTERSECT ALL
    pub fn intersect_all<L>(mut self, rhs: L) -> Self
    where
        L: ArgList<'a>,
    {
        self.push_setop_from_args(
            SetOperator::Intersect,
            SetQuantifier::All,
            rhs,
            "intersect_all()",
        );
        self
    }

    /// EXCEPT (по умолчанию DISTINCT)
    pub fn except<L>(mut self, rhs: L) -> Self
    where
        L: ArgList<'a>,
    {
        self.push_setop_from_args(SetOperator::Except, SetQuantifier::None, rhs, "except()");
        self
    }

    /// EXCEPT ALL
    pub fn except_all<L>(mut self, rhs: L) -> Self
    where
        L: ArgList<'a>,
    {
        self.push_setop_from_args(SetOperator::Except, SetQuantifier::All, rhs, "except_all()");
        self
    }

    #[inline]
    fn push_setop_from_args<L>(
        &mut self,
        op: SetOperator,
        quantifier: SetQuantifier,
        rhs: L,
        ctx: &str,
    ) where
        L: ArgList<'a>,
    {
        let mut args = rhs.into_vec();

        if args.is_empty() {
            self.push_builder_error(format!("{ctx}: expected a subquery (got 0 arguments)"));
            return;
        }
        if args.len() > 1 {
            self.push_builder_error(format!(
                "{ctx}: expected 1 argument (subquery), got {} — extra args are ignored",
                args.len()
            ));
        }

        let built = match args.remove(0) {
            QBArg::Subquery(qb) => qb.build_query_ast(),
            QBArg::Closure(c) => c.call(QueryBuilder::new_empty()).build_query_ast(),
            _ => {
                self.push_builder_error(format!(
                    "{ctx}: expression is not allowed; pass a subquery or closure"
                ));
                return;
            }
        };

        match built {
            Ok((q, params)) => {
                self.set_ops.push(SetOpNode::new(op, quantifier, q, params));
            }
            Err(e) => self.push_builder_error(format!("{ctx}: {e}")),
        }
    }
}
