mod helpers;
use std::collections::HashMap;

use helpers::initialize_db;

use jankenstore::{
    action::PeerOp,
    sqlite::{peer::link, schema::fetch_schema_family, shift::val::v_int},
};

use anyhow::Result;
use rusqlite::Connection;

use insta::assert_snapshot;
use serde_json::{from_value, json};

#[test]
fn test_link_of_wrong_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let rel_op: PeerOp = from_value(json!({
        "Link": {
            "album": [1],
            "artist": [1]
        }
    }))?;
    let result = rel_op.run(&conn, &schema_family);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("is not a peer of"));

    Ok(())
}

#[test]
fn test_wrong_numbers_of_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let rel_op: PeerOp = from_value(json!({
        "Link": {
            "album": [1],
            "artist": [1],
            "song": [1, 3]
        }
    }))?;
    let result = rel_op.run(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let rel_op: PeerOp = from_value(json!({
        "Link": {
            "album": [1]
        }
    }))?;
    let result = rel_op.run(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    let results = link(
        &conn,
        &schema_family,
        &HashMap::from([
            ("album".to_string(), vec![v_int(1)]),
            ("artist".to_string(), vec![v_int(1)]),
            ("song".to_string(), vec![v_int(1), v_int(3)]),
        ]),
    );
    assert!(results.is_err());
    assert_snapshot!(results.unwrap_err());

    Ok(())
}
