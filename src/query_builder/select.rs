use smallvec::SmallVec;
use sqlparser::ast::{Expr as SqlExpr, SelectItem};

use crate::{
    param::Param,
    query_builder::{
        QueryBuilder,
        args::{ArgList, QBArg},
    },
};

#[derive(Debug, Clone)]
pub(crate) struct SelectItemNode {
    pub item: SelectItem,
    pub params: SmallVec<[Param; 8]>,
}

impl SelectItemNode {
    #[inline]
    pub fn new(item: SelectItem, params: SmallVec<[Param; 8]>) -> Self {
        Self { item, params }
    }
}

impl<'a, T> QueryBuilder<'a, T> {
    pub fn select<L>(mut self, items: L) -> Self
    where
        L: ArgList<'a>,
    {
        let args = items.into_vec();
        self.select_items.reserve(args.len());

        for arg in args {
            match arg {
                // ===== Expression =====
                QBArg::Expr(e) => {
                    let (alias_opt, expr, params) = e.into_projection_parts();

                    let item = if let Some(alias) = alias_opt {
                        // expr AS alias
                        SelectItem::ExprWithAlias { expr, alias }
                    } else if matches!(&expr, SqlExpr::Identifier(id) if id.value == "*") {
                        // `*`
                        SelectItem::Wildcard(Default::default())
                    } else {
                        // просто выражение
                        SelectItem::UnnamedExpr(expr)
                    };

                    self.select_items.push(SelectItemNode::new(item, params));
                }

                // ===== Subquery =====
                QBArg::Subquery(qb) => match qb.build_query_ast() {
                    Ok((q, p)) => {
                        let item = SelectItem::UnnamedExpr(SqlExpr::Subquery(Box::new(q)));
                        self.select_items.push(SelectItemNode::new(item, p.into()));
                    }
                    Err(e) => self.push_builder_error(format!("select(): {e}")),
                },

                // ===== Closure → Subquery =====
                QBArg::Closure(c) => {
                    let built = c.call(QueryBuilder::new_empty());
                    match built.build_query_ast() {
                        Ok((q, p)) => {
                            let item = SelectItem::UnnamedExpr(SqlExpr::Subquery(Box::new(q)));
                            self.select_items.push(SelectItemNode::new(item, p.into()));
                        }
                        Err(e) => self.push_builder_error(format!("select(): {e}")),
                    }
                }
            }
        }
        self
    }

    #[inline]
    pub fn select_mut<L>(&mut self, items: L) -> &mut Self
    where
        L: ArgList<'a>,
    {
        let v = std::mem::take(&mut *self); // или дублируй логику без take
        *self = v.select(items);
        self
    }
}
