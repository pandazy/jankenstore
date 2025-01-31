mod helpers;
use helpers::initialize_db;

use jankenstore::{action::DelOp, schema::fetch_schema_family};

use anyhow::Result;
use rusqlite::Connection;
use serde_json::json;

use insta::assert_snapshot;

#[test]
fn test_delete_wrong_table() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let create_op = DelOp::Delete("wrong_table".to_string(), vec![json!(1)]);
    let result = create_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());
    Ok(())
}

#[test]
fn test_delete_wrong_parenthood() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let create_op = DelOp::DeleteChildren(
        "song".to_string(),
        vec![("album".to_string(), vec![json!(1)])],
    );
    let result = create_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());
    Ok(())
}
