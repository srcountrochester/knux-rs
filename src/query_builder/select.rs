use smallvec::SmallVec;
use sqlparser::ast::{Ident, SelectItem};

use crate::{
    param::Param,
    query_builder::{
        QueryBuilder,
        args::{ArgList, QBArg},
    },
};

impl QueryBuilder {
    pub fn select<L>(mut self, items: L) -> Self
    where
        L: ArgList,
    {
        let args = items.into_vec();
        self.select_items.reserve(args.len());

        for arg in args {
            match arg {
                QBArg::Expr(e) => {
                    let (item, mut p) = Self::expr_to_select_item(e);
                    self.select_items.push(item);
                    self.params.append(&mut p);
                }
                other => {
                    if let Ok((expr_ast, mut params)) =
                        other.resolve_into_expr_with(|qb| qb.build_query_ast())
                    {
                        self.select_items.push(SelectItem::UnnamedExpr(expr_ast));
                        self.params.append(&mut params);
                    }
                }
            }
        }
        self
    }

    /// Очищает список select-полей, не трогая остальное
    pub fn clear_select(&mut self) -> &mut Self {
        self.select_items.clear();
        self
    }

    fn expr_to_select_item(
        expr: crate::expression::Expression,
    ) -> (SelectItem, SmallVec<[Param; 8]>) {
        let params = expr.params;
        let item = match expr.alias {
            Some(a) => SelectItem::ExprWithAlias {
                expr: expr.expr,
                alias: Ident::new(a.into_owned()),
            },
            None => SelectItem::UnnamedExpr(expr.expr),
        };
        (item, params)
    }
}
