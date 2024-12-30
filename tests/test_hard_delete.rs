use jankenstore::{crud::shift::val::v_txt, TblRep};

use anyhow::Result;
use rusqlite::{types, Connection};
use std::collections::HashMap;

#[test]
fn test_hard_delete() -> Result<()> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )
    .unwrap();
    let tbl_rep = TblRep::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Text("".to_string())),
            ("count", types::Value::Integer(2)),
        ],
        &["name"],
    )?;
    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    tbl_rep.insert(&conn, &input, true).unwrap();

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(2)),
        ("name".to_string(), types::Value::Text("test2".to_string())),
        ("count".to_string(), types::Value::Integer(6)),
    ]);
    tbl_rep.insert(&conn, &input, true).unwrap();

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(3)),
        ("name".to_string(), types::Value::Text("test3".to_string())),
    ]);
    tbl_rep.insert(&conn, &input, true).unwrap();

    let rows = tbl_rep.list(&conn, None, (false, None)).unwrap();
    assert_eq!(rows.len(), 3);
    tbl_rep.del_by_pk(&conn, &["1"].map(v_txt), None).unwrap();
    let rows = tbl_rep.list_by_pk(&conn, &["1"].map(v_txt), None)?;
    let row = rows.first();
    assert_eq!(row, None);
    let count = tbl_rep.count(&conn, None, None)?;
    assert_eq!(count, 2);

    let rows = tbl_rep
        .list(
            &conn,
            Some(("id = ?", &[types::Value::Integer(2)])),
            (false, None),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    let name = row.get("name").unwrap();
    match name {
        types::Value::Text(name) => assert_eq!(name, "test2"),
        _ => panic!("Unexpected value"),
    }

    let rows = tbl_rep.list_by_pk(
        &conn,
        &["2"].map(v_txt),
        Some(("count = ?", &[types::Value::Integer(5)])),
    )?;
    let count = tbl_rep.count(
        &conn,
        None,
        Some(("count = ?", &[types::Value::Integer(5)])),
    )?;
    assert_eq!(count, 0);

    let row = rows.first();
    assert_eq!(row, None);

    Ok(())
}
