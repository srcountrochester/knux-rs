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

    let start_len = buf.len();
    buf.reserve(list.len());
    for a in list {
        match a.try_into_expr() {
            Ok((expr, _)) => buf.push(SelectItem::UnnamedExpr(expr)),
            Err(e) => {
                // откат, чтобы не оставлять частично добавленные элементы
                buf.truncate(start_len);
                return Err(format!("returning(): {e}").into());
            }
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
    let mut it = item.into_vec().into_iter();
    let Some(first) = it.next() else {
        return Err("returning_one(): expected a single expression".into());
    };

    match first.try_into_expr() {
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

    // Предварительно оцениваем ёмкость по числу '.' + 1
    let parts_cap = qualifier.as_bytes().iter().filter(|&&b| b == b'.').count() + 1;
    let mut idents = Vec::with_capacity(parts_cap);
    for part in qualifier.split('.') {
        idents.push(sqlparser::ast::Ident::new(part));
    }
    let obj = ObjectName::from(idents);

    let kind = SelectItemQualifiedWildcardKind::ObjectName(obj);
    buf.push(SelectItem::QualifiedWildcard(
        kind,
        WildcardAdditionalOptions::default(),
    ));
}
