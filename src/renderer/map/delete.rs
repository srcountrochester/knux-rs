use super::utils::{map_expr, map_select_item, map_table_factor_named};
use crate::renderer::ast as R;
use sqlparser::ast as S;

pub(crate) fn map_delete(d: &S::Delete) -> R::Delete {
    // target table: FROM
    let table = match &d.from {
        // DELETE FROM <...>  (с ключевым словом FROM)
        S::FromTable::WithFromKeyword(list) => {
            let twj = list.first().expect("DELETE FROM must have a table");
            map_table_factor_named(&twj.relation)
        }
        // DELETE <...>  (без ключевого слова FROM)
        S::FromTable::WithoutKeyword(list) => {
            let twj = list
                .first()
                .expect("DELETE (without FROM) must have a table");
            map_table_factor_named(&twj.relation)
        }
    };

    // USING (если есть)
    let using = d
        .using
        .as_ref()
        .map(|list| {
            list.iter()
                .map(|twj| map_table_factor_named(&twj.relation))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    // WHERE
    let r#where = d.selection.as_ref().map(map_expr);

    // RETURNING
    let returning = d
        .returning
        .as_ref()
        .map(|v| v.iter().map(map_select_item).collect())
        .unwrap_or_default();

    R::Delete {
        table,
        using,
        r#where,
        returning,
    }
}
