use jankenstore::UnitResource;

use insta::assert_snapshot;
use anyhow::Result;
use rusqlite::{types, Connection};
use std::collections::HashMap;

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
        &["name", "id", "count"],
        &["name"],
        &[("name", types::Value::Text("".to_string()))],
    );
    let all = resource.fetch_all(&conn, false, None, None)?;
    assert_eq!(all.len(), 0);

    let input = HashMap::new();
    let err = resource.verify_op_required(&input, true).err().unwrap();
    assert_eq!(
        err.to_string(),
        "The input for the operation of test has no items"
    );
    let input = HashMap::from([("name".to_string(), types::Value::Text("test".to_string()))]);
    let err = resource.insert(&conn, &input).err().unwrap();
    assert_eq!(
        err.to_string(),
        "The input for the operation of test requires the value of 'id'"
    );
    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    resource.insert(&conn, &input)?;
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
    assert_eq!(all.len(), 1);

    let update_input = HashMap::new();
    let err = resource
        .update(&conn, "1", &update_input, None)
        .err()
        .unwrap();
    assert_eq!(
        err.to_string(),
        "The input for the operation of test has no items"
    );
    let update_input = HashMap::from([("name".to_string(), types::Value::Null)]);
    let err = resource
        .update(&conn, "1", &update_input, None)
        .err()
        .unwrap();
    assert_eq!(
        err.to_string(),
        "The input for the operation of test requires the value of 'name'"
    );
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

    Ok(())
}

#[test]
fn test_query_input_erros() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )?;
    let resource = UnitResource::new(
        "test",
        "id",
        &["name", "id", "count"],
        &["name"],
        &[("name", types::Value::Text("".to_string()))],
    );

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    resource.insert(&conn, &input)?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(2)),
        ("name".to_string(), types::Value::Text("test2".to_string())),
        ("count".to_string(), types::Value::Integer(6)),
    ]);
    resource.insert(&conn, &input)?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(3)),
        ("name".to_string(), types::Value::Text("test3".to_string())),
    ]);
    resource.insert(&conn, &input)?;

    let query = resource.fetch_all(&conn, false, None, None)?;
    assert_eq!(query.len(), 3);

    let err = resource
        .fetch_one(&conn, "2", Some(("", &[])))
        .err()
        .unwrap();
    assert_snapshot!(err.to_string());

    Ok(())
}
