use crate::expression::{col, lit, val};
use crate::query_builder::args::ArgList;
use crate::query_builder::update::set::parse_assignments_pairs;
use smallvec::SmallVec;

#[test]
fn set_parse_single_pair_collects_param() {
    // (a = val(1))
    let flat: Vec<crate::query_builder::args::QBArg> = (col("a"), val(1)).into_vec();
    let mut carry: SmallVec<[crate::param::Param; 8]> = SmallVec::new();

    let res = parse_assignments_pairs(&mut carry, flat).expect("ok");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].col, "a");
    // Правое выражение могло дать bind-параметр (если val -> bind)
    assert!(
        carry.len() >= 1,
        "expected at least 1 param, got {}",
        carry.len()
    );
}

#[test]
fn set_parse_multiple_pairs_preserves_order_and_params() {
    // (a = val(1), b = val(2))
    let flat = (col("a"), val(1), col("b"), val(2)).into_vec();
    let mut carry: SmallVec<[crate::param::Param; 8]> = SmallVec::new();

    let res = parse_assignments_pairs(&mut carry, flat).expect("ok");
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].col, "a");
    assert_eq!(res[1].col, "b");
    assert!(carry.len() >= 2, "expected >=2 params, got {}", carry.len());
}

#[test]
fn set_left_compound_identifier_takes_last_segment() {
    // ("t.a" = val(1)) -> col = "a"
    let flat = (col("t.a"), val(1)).into_vec();
    let mut carry: SmallVec<[crate::param::Param; 8]> = SmallVec::new();

    let res = parse_assignments_pairs(&mut carry, flat).expect("ok");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].col, "a");
}

#[test]
fn set_empty_list_error() {
    let mut carry: SmallVec<[crate::param::Param; 8]> = SmallVec::new();
    let err = parse_assignments_pairs(&mut carry, ().into_vec()).unwrap_err();
    assert!(
        err.contains("empty assignment list"),
        "unexpected err: {err}"
    );
}

#[test]
fn set_odd_number_of_items_error() {
    // (a = 1, b) — отсутствует значение для b
    let flat = (col("a"), val(1), col("b")).into_vec();
    let mut carry: SmallVec<[crate::param::Param; 8]> = SmallVec::new();

    let err = parse_assignments_pairs(&mut carry, flat).unwrap_err();
    assert!(
        err.contains("expected pairs (col, value)"),
        "unexpected err: {err}"
    );
}

#[test]
fn set_left_not_identifier_error() {
    // ("not_ident" = 1) — левая часть не идентификатор
    let flat = (lit("not_ident"), val(1)).into_vec();
    let mut carry: SmallVec<[crate::param::Param; 8]> = SmallVec::new();

    let err = parse_assignments_pairs(&mut carry, flat).unwrap_err();
    assert!(
        err.contains("left item must be a column identifier")
            || err.contains("invalid compound identifier"),
        "unexpected err: {err}"
    );
}
