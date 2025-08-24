use crate::query_builder::args::ArgList;
use smallvec::SmallVec;
use sqlparser::ast::{
    ObjectName, SelectItem, SelectItemQualifiedWildcardKind, WildcardAdditionalOptions,
};

/// Добавить список RETURNING <expr,...>
pub(crate) fn push_returning_list<'a, L>(
    buf: &mut SmallVec<[SelectItem; 4]>,
    items: L,
) -> Result<(), std::borrow::Cow<'static, str>>
where
    L: ArgList<'a>,
{
    let list = items.into_vec();
    if list.is_empty() {
        return Err("returning(): empty list".into());
    }
    for a in list {
        match a.try_into_expr() {
            Ok((expr, _p)) => buf.push(SelectItem::UnnamedExpr(expr)),
            Err(e) => return Err(format!("returning(): {e}").into()),
        }
    }
    Ok(())
}

/// RETURNING один элемент, перезаписать список
pub(crate) fn set_returning_one<'a, L>(
    buf: &mut SmallVec<[SelectItem; 4]>,
    item: L,
) -> Result<(), std::borrow::Cow<'static, str>>
where
    L: ArgList<'a>,
{
    let mut args = item.into_vec();
    if args.is_empty() {
        return Err("returning_one(): expected a single expression".into());
    }
    if args.len() != 1 {
        // не срываем пайплайн — берём первый валидный
    }
    match args.swap_remove(0).try_into_expr() {
        Ok((expr, _)) => {
            buf.clear();
            buf.push(SelectItem::UnnamedExpr(expr));
            Ok(())
        }
        Err(e) => Err(format!("returning_one(): {e}").into()),
    }
}

/// RETURNING *
pub(crate) fn set_returning_all(buf: &mut SmallVec<[SelectItem; 4]>) {
    buf.clear();
    buf.push(SelectItem::Wildcard(WildcardAdditionalOptions::default()));
}

/// RETURNING <qualifier>.*
pub(crate) fn set_returning_all_from(buf: &mut SmallVec<[SelectItem; 4]>, qualifier: &str) {
    buf.clear();
    let obj = ObjectName::from(
        qualifier
            .split('.')
            .map(sqlparser::ast::Ident::new)
            .collect::<Vec<_>>(),
    );
    let kind = SelectItemQualifiedWildcardKind::ObjectName(obj);
    buf.push(SelectItem::QualifiedWildcard(
        kind,
        WildcardAdditionalOptions::default(),
    ));
}
