use insta::assert_snapshot;
use jankenstore::UnitResource;

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
fn test_create_or_update_unit_resource() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )?;
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
    let all = resource.fetch_all(&conn, false, None, None)?;
    assert_eq!(all.len(), 0);

    let input = HashMap::from([("name".to_string(), types::Value::Text("test0".to_string()))]);
    resource.insert(&conn, &input, true)?;
    let row = resource.fetch_one(&conn, "0", None)?.unwrap();
    let name = row.get("name").unwrap();
    match name {
        types::Value::Text(name) => assert_eq!(name, "test0"),
        _ => panic!("Unexpected value"),
    }
    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    resource.insert(&conn, &input, true)?;
    let row = resource.fetch_one(&conn, "1", None)?.unwrap();
    let name = row.get("name").unwrap();
    match name {
        types::Value::Text(name) => assert_eq!(name, "test"),
        _ => panic!("Unexpected value"),
    }
    let count = row.get("count").unwrap();
    match count {
        types::Value::Integer(count) => assert_eq!(count, &2),
        _ => panic!("Unexpected value"),
    }
    let all = resource.fetch_all(&conn, false, None, None)?;
    assert_eq!(all.len(), 2);

    let all = resource.fetch_all_as::<TestEntity>(&conn, false, None, None)?;
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].name.clone().unwrap(), "test0");
    assert_eq!(all[1].name.clone().unwrap(), "test");

    let update_input = HashMap::new();
    let err = resource
        .update(&conn, "1", &update_input, None)
        .err()
        .unwrap();
    assert_eq!(err.to_string(), "(table: test) The input has no items");
    let update_input = HashMap::from([("name".to_string(), types::Value::Null)]);
    let update_null_name_error = resource
        .update(&conn, "1", &update_input, None)
        .err()
        .unwrap();
    assert_snapshot!(update_null_name_error.to_string());
    let update_input = HashMap::from([
        ("name".to_string(), types::Value::Text("test2".to_string())),
        ("count".to_string(), types::Value::Integer(6)),
    ]);
    resource.update(&conn, "1", &update_input, None)?;
    let row = resource.fetch_one(&conn, "1", None)?.unwrap();
    let count = row.get("count").unwrap();
    match count {
        types::Value::Integer(count) => assert_eq!(count, &6),
        _ => panic!("Unexpected value"),
    }

    let row = resource
        .fetch_one_as::<TestEntity>(&conn, "1", None)?
        .unwrap();
    assert_eq!(row.id.unwrap(), 1);
    assert_eq!(row.name.unwrap(), "test2");
    assert_eq!(row.count.unwrap(), 6);

    let row = resource.fetch_one_as::<TestEntity>(&conn, "-1", None)?;
    assert!(row.is_none());

    Ok(())
}
