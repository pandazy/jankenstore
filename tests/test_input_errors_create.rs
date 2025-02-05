mod helpers;
use helpers::initialize_db;

use jankenstore::{
    action::commands::CreateCommand,
    sqlite::{
        add::create,
        schema::fetch_schema_family,
        shift::{json_to_val_map, val::v_txt},
    },
};

use anyhow::Result;
use insta::assert_snapshot;
use rusqlite::{types, Connection};
use serde_json::{from_value, json};
use std::collections::HashMap;

#[test]
fn test_wrong_table() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let input = json!({
        "artist_id": 1,
        "memo": "test"
    });

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"Create": ["wrong_table", input]}
    }))?;

    let result = create_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_missing_empty_fields() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let input = json!({
        "artist_id": 1,
        "memo": ""
    });

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"Create": ["song", input.clone()]}
    }))?;

    let result = create_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"Create": ["song", {
            "name": "",
            "artist_id": 1,
        }]}
    }))?;
    let result = create_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let result = create(
        &conn,
        &schema_family,
        "song",
        &json_to_val_map(&schema_family.try_get_schema("song")?.types, &input)?,
        false,
    );
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_unknown_fields() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let input = json!({
        "name": "foobar",
        "artist_id": 1,
        "memo": "test",
        "unknown_field": "UNKNOWN"
    });

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"Create": ["song", input]}
    }))?;
    let result = create_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_wrong_type_fields() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let input = json!({
        "name": 42,
        "artist_id": "abc",
        "memo": "test"
    });

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"Create": ["song", input]}
    }))?;
    let result = create_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"Create": ["song", {
            "name": "42",
            "artist_id": "22",
            "memo": "test",
            "file": "N/A"
        }]}
    }))?;
    let result = create_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let low_lever_result = create(
        &conn,
        &schema_family,
        "song",
        &[
            ("name", v_txt("42")),
            ("artist_id", v_txt("abc")),
            ("memo", v_txt("test")),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect::<HashMap<String, types::Value>>(),
        false,
    );
    assert!(low_lever_result.is_err());
    assert_snapshot!(low_lever_result.unwrap_err());

    Ok(())
}
