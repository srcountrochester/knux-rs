use crate::renderer::config::PlaceholderStyle;
use crate::renderer::writer::SqlWriter;

#[test]
fn push_and_finish_basic() {
    let mut w = SqlWriter::new(16, PlaceholderStyle::Question);
    w.push("SELECT ");
    w.push_char('*');
    w.push(" FROM ");
    let tbl = String::from("users");
    w.push(tbl); // проверяем, что принимает String (AsRef<str>)
    let out = w.finish();
    assert_eq!(out, "SELECT * FROM users");
}

#[test]
fn question_placeholders_do_not_increment_index() {
    let mut w = SqlWriter::new(8, PlaceholderStyle::Question);
    assert_eq!(w.next_param_idx, 1);
    w.push_placeholder();
    w.push_placeholder();
    w.push_placeholder();
    assert_eq!(w.next_param_idx, 1);
    assert_eq!(w.finish(), "???");
    // next_param_idx не меняется для '?'
}

#[test]
fn numbered_placeholders_increment_index() {
    let mut w = SqlWriter::new(8, PlaceholderStyle::Numbered);
    assert_eq!(w.next_param_idx, 1);
    w.push_placeholder(); // $1
    assert_eq!(w.next_param_idx, 2);
    w.push_placeholder(); // $2
    assert_eq!(w.next_param_idx, 3);
    w.push_placeholder(); // $3
    assert_eq!(w.next_param_idx, 4);
    assert_eq!(w.finish(), "$1$2$3");
    // после трёх вставок счётчик указывает на следующий номер
}

#[test]
fn compose_full_sql_with_question_placeholders() {
    let mut w = SqlWriter::new(64, PlaceholderStyle::Question);
    w.push("SELECT ");
    w.push_char('*');
    w.push(" FROM users WHERE a = ");
    w.push_placeholder();
    w.push(" AND b = ");
    w.push_placeholder();
    assert_eq!(w.finish(), "SELECT * FROM users WHERE a = ? AND b = ?");
}

#[test]
fn compose_full_sql_with_numbered_placeholders() {
    let mut w = SqlWriter::new(64, PlaceholderStyle::Numbered);
    w.push("SELECT * FROM users WHERE a = ");
    w.push_placeholder(); // $1
    w.push(" AND b = ");
    w.push_placeholder(); // $2
    w.push(" OR c IN (");
    w.push_placeholder(); // $3
    w.push(", ");
    w.push_placeholder(); // $4
    w.push(")");
    assert_eq!(w.next_param_idx, 5);
    assert_eq!(
        w.finish(),
        "SELECT * FROM users WHERE a = $1 AND b = $2 OR c IN ($3, $4)"
    );
}

#[test]
fn unicode_char_and_strings_ok() {
    let mut w = SqlWriter::new(16, PlaceholderStyle::Question);
    w.push("π=");
    w.push_char('λ');
    w.push(" ");
    w.push("テスト");
    assert_eq!(w.finish(), "π=λ テスト");
}

#[test]
fn separate_writers_are_independent() {
    let mut w1 = SqlWriter::new(8, PlaceholderStyle::Numbered);
    let mut w2 = SqlWriter::new(8, PlaceholderStyle::Question);

    w1.push_placeholder(); // $1
    w1.push_placeholder(); // $2
    w2.push_placeholder(); // ?

    assert_eq!(w1.finish(), "$1$2");
    assert_eq!(w2.finish(), "?");
}
