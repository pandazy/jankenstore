mod helpers;
use helpers::initialize_db;

use jankenstore::{action::commands::DeleteCommand, sqlite::schema::fetch_schema_family};

use anyhow::Result;
use rusqlite::Connection;
use serde_json::json;

use insta::assert_snapshot;

#[test]
fn test_delete_wrong_table() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let DeleteCommand { op: del_op } = serde_json::from_value(json!({
        "op": {"Delete": {
            "src": "wrong_table",
            "keys": [1]
        }}
    }))?;
    let result = del_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());
    Ok(())
}

#[test]
fn test_delete_wrong_parenthood() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let DeleteCommand { op: del_op } = serde_json::from_value(json!({
        "op": {"DeleteChildren": {
            "src": "song",
            "parents": { "album": [1] }
        }}
    }))?;

    let result = del_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());
    Ok(())
}
