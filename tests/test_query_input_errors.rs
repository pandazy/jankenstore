use jankenstore::{crud::fetch_all, UnitResource};

use anyhow::Result;
use insta::assert_snapshot;
use rusqlite::{types, Connection};
use std::collections::HashMap;

#[test]
fn test_query_input_errors() -> Result<()> {
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

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    resource.insert(&conn, &input, true)?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(2)),
        ("name".to_string(), types::Value::Text("test2".to_string())),
        ("count".to_string(), types::Value::Integer(6)),
    ]);
    resource.insert(&conn, &input, true)?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(3)),
        ("name".to_string(), types::Value::Text("test3".to_string())),
    ]);
    resource.insert(&conn, &input, true)?;

    let no_table_name_err = fetch_all(&conn, "", false, None, None).err().unwrap();
    assert_eq!(
        no_table_name_err.to_string(),
        "The table name cannot be an empty string"
    );

    let query = resource.fetch_all(&conn, false, None, None)?;
    assert_eq!(query.len(), 3);

    let no_where_clause_err = resource
        .fetch_one(&conn, "2", Some(("", &[])))
        .err()
        .unwrap();
    assert_snapshot!(no_where_clause_err.to_string());

    Ok(())
}

#[test]
fn test_write_op_query_errors() -> Result<()> {
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

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    let update_where_err = resource
        .update(&conn, "1", &input, Some(("", &[])))
        .err()
        .unwrap();
    assert_snapshot!(update_where_err.to_string());

    let delete_error = resource
        .hard_del(&conn, "1", Some(("", &[])))
        .err()
        .unwrap();
    assert_snapshot!(delete_error.to_string());
    Ok(())
}

#[test]
fn test_invalid_inputs() -> Result<()> {
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

    let input = HashMap::new();
    let err = resource.insert(&conn, &input, false);
    assert_eq!(
        err.err().unwrap().to_string(),
        "(table: test) The input has no items"
    );

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
        ("age".to_string(), types::Value::Integer(18)),
    ]);
    let err = resource.insert(&conn, &input, true).err().unwrap();
    assert_eq!(
        err.to_string(),
        "(table: test) The input has a key 'age' that is not allowed"
    );

    Ok(())
}
