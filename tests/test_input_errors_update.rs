mod helpers;
use helpers::initialize_db;

use jankenstore::{
    action::UpdateOp,
    sqlite::{
        schema::fetch_schema_family,
        shift::{json_to_val_map, val::v_int},
        update::update_by_pk,
    },
};

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

use insta::assert_snapshot;

#[derive(Debug, Deserialize, Serialize)]
struct UpdateCommand {
    op: UpdateOp,
}

#[test]
fn test_wrong_table() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let input = json!({
        "artist_id": 1,
        "memo": "test"
    });

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": { "Update": [{
            "src": "wrong_table",
            "keys": [1],
        }, input] }
    }))?;
    let result = update_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_missing_empty_fields() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": { "Update": [{
            "src": "song",
            "keys": [1],
        }, { "name": "" }] }
    }))?;
    let result = update_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let lower_level_result = update_by_pk(
        &conn,
        &schema_family,
        "song",
        &json_to_val_map(
            &schema_family.try_get_schema("song")?.types,
            &json!({
                "name": "",
            }),
        )?,
        &[v_int(1)],
        None,
        false,
    );
    assert!(lower_level_result.is_err());
    assert_snapshot!(lower_level_result.unwrap_err());

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

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": { "Update": [{
            "src": "song",
            "keys": [1],
        }, input] }
    }))?;

    let result = update_op.with_schema(&conn, &schema_family);
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

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": { "Update": [{
            "src": "song",
            "keys": [1],
        }, input] }
    }))?;

    let result = update_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let album_update = json!({
        "price": "ninety-nine"
    });

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": { "Update": [{
            "src": "album",
            "keys": [1],
        }, album_update] }
    }))?;

    let result = update_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let album_update = json!({
        "price": [99.9]
    });

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": { "Update": [{
            "src": "album",
            "keys": [1],
        }, album_update] }
    }))?;

    let result = update_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}

#[test]
fn test_update_fk() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let input = json!({
        "id": "6",
        "memo": "test"
    });

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": { "Update": [{
            "src": "song",
            "keys": [1],
        }, input] }
    }))?;

    let result = update_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}
