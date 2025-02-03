mod helpers;

use helpers::initialize_db;

use jankenstore::{
    action::ReaderOp,
    sqlite::{
        basics::{CountConfig, FetchConfig},
        read::{self, count},
        schema::fetch_schema_family,
    },
};

use anyhow::Result;
use rusqlite::Connection;
use serde_json::{from_value, json};

use insta::assert_snapshot;

#[derive(serde::Deserialize, serde::Serialize)]
struct ReadCommand {
    op: ReaderOp,
}

#[test]
fn test_wrong_table() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"ByPk": [ "wrong_table", [1]]}
    }))?;
    let result = read_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let result = count(&conn, &schema_family, "another_wrong_table", None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_wrong_field() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let result = read::all(
        &conn,
        &schema_family,
        "song",
        Some(FetchConfig {
            display_cols: Some(&["wrong_field"]),
            ..Default::default()
        }),
    );
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let result = count(
        &conn,
        &schema_family,
        "song",
        Some({
            CountConfig {
                distinct_field: Some("wrong_field"),
                ..Default::default()
            }
        }),
    );
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_wrong_parenthood() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Children": ["song", [["album", [1]]]]}
    }))?;
    let result = read_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Children": ["album", [["song", [1]]]]}
    }))?;
    let result = read_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_wrong_peer() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Peers": ["artist", [["album", [2]]]]}
    }))?;
    let result = read_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Peers": ["song", [["artist", [2]]]]}
    }))?;
    let result = read_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_wrong_search_keyword() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: search_op } = from_value(json!({
        "op": {"Search": ["song", ["id", "1"]]}
    }))?;

    let result = search_op.with_schema(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}
