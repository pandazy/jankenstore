use insta::assert_snapshot;
use jankenstore::{crud::shift::val::v_txt, TblRep};

use anyhow::Result;
use rusqlite::{types, Connection};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct TestEntity {
    id: Option<i64>,
    name: Option<String>,
    count: Option<i64>,
}

#[test]
fn test_create_or_update_tbl_rep() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )?;
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
    let all = tbl_rep.list(&conn, None, (false, None))?;
    assert_eq!(all.len(), 0);

    let input = HashMap::from([("name".to_string(), v_txt("test0"))]);
    tbl_rep.insert(&conn, &input, true)?;
    let rows = tbl_rep.list_by_pk(&conn, &[v_txt("0")], None)?;
    let name = rows[0].get("name").unwrap();
    match name {
        types::Value::Text(name) => assert_eq!(name, "test0"),
        _ => panic!("Unexpected value"),
    }
    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    tbl_rep.insert(&conn, &input, true)?;
    let rows = tbl_rep.list_by_pk(&conn, &[v_txt("1")], None)?;
    let name = rows[0].get("name").unwrap();
    match name {
        types::Value::Text(name) => assert_eq!(name, "test"),
        _ => panic!("Unexpected value"),
    }
    let count = rows[0].get("count").unwrap();
    match count {
        types::Value::Integer(count) => assert_eq!(count, &2),
        _ => panic!("Unexpected value"),
    }
    let all = tbl_rep.list(&conn, None, (false, None))?;
    assert_eq!(all.len(), 2);

    let all = tbl_rep.list_as::<TestEntity>(&conn, None, (false, None))?;
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].name.clone().unwrap(), "test0");
    assert_eq!(all[1].name.clone().unwrap(), "test");

    let update_input = HashMap::new();
    let err = tbl_rep
        .upd_by_pk(&conn, &[v_txt("1")], &update_input, None)
        .err()
        .unwrap();
    assert_eq!(err.to_string(), "(table: test) The input has no items");
    let update_input = HashMap::from([("name".to_string(), types::Value::Null)]);
    let update_null_name_error = tbl_rep
        .upd_by_pk(&conn, &[v_txt("1")], &update_input, None)
        .err()
        .unwrap();
    assert_snapshot!(update_null_name_error.to_string());
    let update_input = HashMap::from([
        ("name".to_string(), types::Value::Text("test2".to_string())),
        ("count".to_string(), types::Value::Integer(6)),
    ]);
    tbl_rep.upd_by_pk(&conn, &[v_txt("1")], &update_input, None)?;
    let rows = tbl_rep.list_by_pk(&conn, &[v_txt("1")], None)?;
    let count = rows[0].get("count").unwrap();
    match count {
        types::Value::Integer(count) => assert_eq!(count, &6),
        _ => panic!("Unexpected value"),
    }

    let rows = tbl_rep.list_by_pk_as::<TestEntity>(&conn, &[v_txt("1")], None)?;
    let row = rows.first().unwrap();
    assert_eq!(row.id.unwrap(), 1);
    assert_eq!(row.name.clone().unwrap(), "test2");
    assert_eq!(row.count.unwrap(), 6);

    let rows = tbl_rep.list_by_pk_as::<TestEntity>(&conn, &[v_txt("-1")], None)?;
    assert!(rows.is_empty());

    Ok(())
}

#[test]
fn test_fetching_by_multiple_primary_keys() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )?;
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
    [
        ("test0", 0, 3),
        ("test1", 1, 5),
        ("test2", 2, 3),
        ("test3", 3, 6),
        ("test4", 4, 7),
    ]
    .iter()
    .for_each(|(name, id, count)| {
        let input = HashMap::from([
            ("id".to_string(), types::Value::Integer(*id)),
            ("name".to_string(), types::Value::Text(name.to_string())),
            ("count".to_string(), types::Value::Integer(*count)),
        ]);
        tbl_rep.insert(&conn, &input, true).unwrap();
    });
    let rows = tbl_rep.list_by_pk(&conn, &["0", "1", "2"].map(v_txt), None)?;
    assert_eq!(rows.len(), 3);
    let count = tbl_rep.count_by_pk(&conn, &["0", "1", "2"].map(v_txt), None, None)?;
    assert_eq!(count, 3);

    let rows = tbl_rep.list_by_pk(&conn, &["0", "1", "2", "3", "4"].map(v_txt), None)?;
    assert_eq!(rows.len(), 5);
    let count = tbl_rep.count_by_pk(&conn, &["0", "1", "2", "3", "4"].map(v_txt), None, None)?;
    assert_eq!(count, 5);

    let rows_by_condition = tbl_rep.list_by_pk(
        &conn,
        &["0", "1", "2", "3", "4"].map(v_txt),
        Some(("count = ?", &[types::Value::Integer(3)])),
    )?;
    assert_eq!(rows_by_condition.len(), 2);
    let count_by_condition = tbl_rep.count_by_pk(
        &conn,
        &["0", "1", "2", "3", "4"].map(v_txt),
        None,
        Some(("count = ?", &[types::Value::Integer(3)])),
    )?;
    assert_eq!(count_by_condition, 2);
    assert_eq!(
        rows_by_condition[0].get("id"),
        Some(&types::Value::Integer(0))
    );
    assert_eq!(
        rows_by_condition[1].get("id"),
        Some(&types::Value::Integer(2))
    );
    Ok(())
}
