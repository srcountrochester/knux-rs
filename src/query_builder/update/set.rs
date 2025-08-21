use crate::param::Param;
use crate::query_builder::args::QBArg;
use smallvec::SmallVec;
use sqlparser::ast::Expr as SqlExpr;

/// Элемент присваивания для UPDATE
#[derive(Debug, Clone)]
pub(crate) struct Assignment {
    pub col: String,
    pub value: SqlExpr,
}

/// Парсинг плоского списка: (col1, val1, col2, val2, ...)
pub(crate) fn parse_assignments_pairs(
    carry_params: &mut SmallVec<[Param; 8]>,
    flat: Vec<QBArg>,
) -> Result<SmallVec<[Assignment; 8]>, std::borrow::Cow<'static, str>> {
    if flat.is_empty() {
        return Err("set(): empty assignment list".into());
    }
    if flat.len() % 2 != 0 {
        return Err("set(): expected pairs (col, value)".into());
    }

    // Собираем попарно
    let mut out: SmallVec<[Assignment; 8]> = SmallVec::with_capacity(flat.len() / 2);
    let mut it = flat.into_iter();
    while let (Some(kc), Some(kv)) = (it.next(), it.next()) {
        // Левая часть — идентификатор (берём последний сегмент)
        let col = match kc.try_into_expr() {
            Ok((SqlExpr::Identifier(id), _)) => id.value,
            Ok((SqlExpr::CompoundIdentifier(mut parts), _)) => parts
                .pop()
                .map(|i| i.value)
                .ok_or::<std::borrow::Cow<'static, str>>(
                "set(): invalid compound identifier".into(),
            )?,
            Ok((_other, _)) => return Err("set(): left item must be a column identifier".into()),
            Err(e) => return Err(format!("set(): {e}").into()),
        };

        // Правая часть — произвольное выражение (в т.ч. с параметрами/подзапросом)
        match kv.resolve_into_expr_with(|qb| qb.build_query_ast()) {
            Ok((expr, mut params)) => {
                if !params.is_empty() {
                    carry_params.extend(params.drain(..));
                }
                out.push(Assignment { col, value: expr });
            }
            Err(e) => return Err(format!("set(): value build failed: {e}").into()),
        }
    }

    Ok(out)
}
