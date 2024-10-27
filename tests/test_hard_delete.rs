use jankenstore::UnitResource;

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
    let resource = UnitResource::new(
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
    resource.insert(&conn, &input, true).unwrap();

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(2)),
        ("name".to_string(), types::Value::Text("test2".to_string())),
        ("count".to_string(), types::Value::Integer(6)),
    ]);
    resource.insert(&conn, &input, true).unwrap();

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(3)),
        ("name".to_string(), types::Value::Text("test3".to_string())),
    ]);
    resource.insert(&conn, &input, true).unwrap();

    let rows = resource.fetch_all(&conn, false, None, None).unwrap();
    assert_eq!(rows.len(), 3);
    resource.hard_del(&conn, "1", None).unwrap();
    let row = resource.fetch_one(&conn, "1", None).unwrap();
    assert_eq!(row, None);
    let rows = resource.fetch_all(&conn, false, None, None).unwrap();
    assert_eq!(rows.len(), 2);

    let rows = resource
        .fetch_all(
            &conn,
            false,
            None,
            Some(("id = ?", &[types::Value::Integer(2)])),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = rows.first().unwrap();
    let name = row.get("name").unwrap();
    match name {
        types::Value::Text(name) => assert_eq!(name, "test2"),
        _ => panic!("Unexpected value"),
    }

    let row = resource
        .fetch_one(&conn, "2", Some(("count = ?", &[types::Value::Integer(5)])))
        .unwrap();
    assert_eq!(row, None);

    Ok(())
}
