mod helpers;

use helpers::initialize_db;

use jankenstore::{
    action::{payload::ReadSrc, ReadOp},
    sqlite::{
        basics::{CountConfig, FetchConfig, ILLEGAL_BY_CHARS},
        read::{self, count},
        schema::fetch_schema_family,
    },
};

use anyhow::Result;
use rusqlite::Connection;
use serde_json::{from_value, json};

use insta::assert_snapshot;

#[test]
fn test_wrong_table() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op: ReadOp = from_value(json!({"ByPk": [ "wrong_table", [1]]}))?;
    let result = read_op.run(&conn, &schema_family, None);
    assert_eq!(read_op.src(), "wrong_table");
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

    let read_op: ReadOp = from_value(json!({
        "Children": {
            "src": "song",
            "parents": {
                "album": [1]
            }
        }
    }))?;
    let result = read_op.run(&conn, &schema_family, None);
    assert_eq!(read_op.src(), "song");
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let read_op: ReadOp = from_value(json!({
        "Children": {
            "src": "album",
            "parents": {
                "song": [1]
            }
        }
    }))?;
    let result = read_op.run(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_wrong_peer() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op: ReadOp = from_value(json!({
        "Peers": {
            "src": "artist",
            "peers": {
                "album": [2]
            }
        }
    }))?;
    let result = read_op.run(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let read_op: ReadOp = from_value(json!({
        "Peers": {
            "src": "song",
            "peers": {
                "artist": [2]
            }
        }
    }))?;
    let result = read_op.run(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_wrong_search_keyword() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let search_op: ReadOp = from_value(json!({"Search": {
        "table": "song",
        "col": "id",
        "keyword": "1",
    }}))?;

    let result = search_op.run(&conn, &schema_family, None);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_custom_sql_injection_prevention() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op: ReadOp = from_value(json!({
        "ByPk": {
            "src": "song",
            "keys": [1]
        }
    }))?;
    for c in ILLEGAL_BY_CHARS {
        for opt in [
            Some(FetchConfig {
                group_by: Some(c.to_string().as_str()),
                ..Default::default()
            }),
            Some(FetchConfig {
                order_by: Some(c.to_string().as_str()),
                ..Default::default()
            }),
        ] {
            let result = read_op.run(&conn, &schema_family, opt);
            assert!(result.is_err());
            assert_snapshot!(result.unwrap_err());
        }
    }

    Ok(())
}
