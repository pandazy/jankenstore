mod helpers;
use helpers::initialize_db;

use jankenstore::{action::RelOp, sqlite::schema::fetch_schema_family};

use anyhow::Result;
use rusqlite::Connection;

use insta::assert_snapshot;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Deserialize, Serialize)]
struct RelCommand {
    op: RelOp,
}

#[test]
fn test_link_of_wrong_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let RelCommand { op: rel_op } = from_value(json!({
        "op": {"Link": [["album", [1]], ["artist", [1]]]}
    }))?;
    let result = rel_op.with_schema(&conn, &schema_family);
    assert!(result.is_err());
    assert_snapshot!(result.unwrap_err());

    Ok(())
}
