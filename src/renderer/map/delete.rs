use super::utils::{map_expr, map_select_item, map_table_factor_named};
use crate::renderer::ast as R;
use sqlparser::ast as S;

pub(crate) fn map_delete(d: &S::Delete) -> R::Delete {
    // target table (оба варианта FROM обрабатываем одной веткой)
    let table = match &d.from {
        S::FromTable::WithFromKeyword(list) | S::FromTable::WithoutKeyword(list) => {
            let twj = list.first().expect("DELETE must have a target table");
            map_table_factor_named(&twj.relation)
        }
    };

    // USING (c предвыделением ёмкости)
    let using = d.using.as_ref().map_or_else(Vec::new, |list| {
        let mut v = Vec::with_capacity(list.len());
        for twj in list {
            v.push(map_table_factor_named(&twj.relation));
        }
        v
    });

    // WHERE
    let r#where = d.selection.as_ref().map(map_expr);

    // RETURNING (c предвыделением ёмкости)
    let returning = d.returning.as_ref().map_or_else(Vec::new, |items| {
        let mut v = Vec::with_capacity(items.len());
        for it in items {
            v.push(map_select_item(it));
        }
        v
    });

    R::Delete {
        table,
        using,
        r#where,
        returning,
    }
}
