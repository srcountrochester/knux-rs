#![allow(dead_code)]

#[cfg(feature = "mysql")]
pub const Q: char = '`';
#[cfg(not(feature = "mysql"))]
pub const Q: char = '"';

#[inline]
pub fn qi(s: &str) -> String {
    // quote ident
    let mut out = String::with_capacity(s.len() + 2);
    out.push(Q);
    out.push_str(s);
    out.push(Q);
    out
}

#[inline]
pub fn qn(parts: &[&str]) -> String {
    // qualify: "schema"."table"  /  `schema`.`table`
    parts.iter().map(|p| qi(p)).collect::<Vec<_>>().join(".")
}

#[inline]
pub fn col_list(cols: &[&str]) -> String {
    // "id", "name" / `id`, `name`
    cols.iter().map(|c| qi(c)).collect::<Vec<_>>().join(", ")
}
