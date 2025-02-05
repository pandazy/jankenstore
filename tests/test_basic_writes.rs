mod helpers;
use helpers::initialize_db;

use jankenstore::{
    action::commands::{CreateCommand, DeleteCommand, PeerCommand, ReadCommand, UpdateCommand}, sqlite::schema::fetch_schema_family
};

use anyhow::Result;
use rusqlite::Connection;
use serde_json::{from_value, json};

#[test]
fn test_create() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let input = json!({
        "id": 42,
        "name": "test",
        "artist_id": 24,
        "file": [123,45,67],
        "memo": "gogo"
    });

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"Create": ["song", input]}
    }))?;
    create_op.with_schema(&conn, &schema_family)?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"ByPk": {
            "src": "song",
            "keys": [42]
        }}
    }))?;
    let record = read_op.with_schema(&conn, &schema_family, None)?;

    assert!(record.len() == 1);
    assert_eq!(record[0]["id"], json!(42));
    assert_eq!(record[0]["name"], json!("test"));
    assert_eq!(record[0]["artist_id"], json!(24));
    assert_eq!(record[0]["memo"], json!("gogo"));

    Ok(())
}

#[test]
fn test_create_child() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Children": {
            "src": "song",
            "parents": {
                "artist": [3]
             }
        }}
    }))?;
    let records = read_op.with_schema(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("A Hard Day's Night"));

    let input = json!({
        "id": 999,
        "name": "Yellow Submarine",
        "memo": "1966"
    });

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"CreateChild": [{
            "src": "song",
            "parents": { "artist":  3 }
        }, input]}
    }))?;
    create_op.with_schema(&conn, &schema_family)?;

    let records = read_op.with_schema(&conn, &schema_family, None)?;

    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["id"], json!(999));
    assert_eq!(records[1]["name"], json!("Yellow Submarine"));
    assert_eq!(records[1]["memo"], json!("1966"));

    Ok(())
}

#[test]
fn test_update() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let input = json!({
        "name": "updated",
        "memo": "updated"
    });

    let UpdateCommand { op: update_op } = serde_json::from_value(json!({
        "op": {"Update": [{ "src": "song", "keys": [1] }, input]}
    }))?;
    update_op.with_schema(&conn, &schema_family)?;
    let ReadCommand { op: read_song } = serde_json::from_value(json!({
        "op": {"ByPk": {
            "src": "song",
            "keys": [1]
        }}
    }))?;
    let record = read_song.with_schema(&conn, &schema_family, None)?;
    assert!(record.len() == 1);
    assert_eq!(record[0]["name"], json!("updated"));
    assert_eq!(record[0]["memo"], json!("updated"));

    let input = json!({
        "price": 20.8,
        "memo": 2025
    });
    let UpdateCommand { op: update_album } = serde_json::from_value(json!({
        "op": {"Update": [{ "src": "album", "keys": [1] }, input]}
    }))?;
    update_album.with_schema(&conn, &schema_family)?;
    let ReadCommand { op: read_album } = serde_json::from_value(json!({
        "op": {"ByPk": {
            "src": "album",
            "keys": [1]
        }}
    }))?;
    let record = read_album.with_schema(&conn, &schema_family, None)?;
    assert!(record.len() == 1);
    assert_eq!(record[0]["price"], json!(20.8));
    assert_eq!(record[0]["memo"], json!("2025"));

    Ok(())
}

#[test]
fn test_update_children() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Children": {
            "src": "song",
            "parents": { "artist": [3] }
        }}
    }))?;

    let input = json!({
        "id": 999,
        "name": "Yellow Submarine",
        "memo": "1966"
    });
    let CreateCommand { op: create_op } = from_value(json!({
      "op": {"CreateChild": [{
        "src": "song",
        "parents": { "artist": 3}
      }, input]}
    }))?;

    // Confirm the state before update
    create_op.with_schema(&conn, &schema_family)?;
    let records = read_op.with_schema(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["memo"], json!("60s"));
    assert_eq!(records[1]["memo"], json!("1966"));

    // Update the child(or children)
    let input = json!({
        "memo": "Congrats!"
    });

    let UpdateCommand { op: update_op } = from_value(json!({
        "op": {"UpdateChildren": [{
            "src": "song",
            "parents": { "artist": [3] }
        }, input]}
    }))?;

    update_op.with_schema(&conn, &schema_family)?;

    let records = read_op.with_schema(&conn, &schema_family, None)?;

    // Verify the state after update
    assert_eq!(records.len(), 2);
    for record in records.iter() {
        assert_eq!(record["memo"], json!("Congrats!"));
    }

    Ok(())
}

#[test]
fn test_delete() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"ByPk": {
            "src": "song",
            "keys": [1]
        }}
    }))?;

    assert_eq!(read_op.with_schema(&conn, &schema_family, None)?.len(), 1);

    let DeleteCommand { op: del_op } = from_value(json!({
        "op": {"Delete": {
            "src": "song",
            "keys": [1]
        }}
    }))?;
    del_op.with_schema(&conn, &schema_family, None)?;

    assert_eq!(read_op.with_schema(&conn, &schema_family, None)?.len(), 0);
    Ok(())
}

#[test]
fn test_delete_children() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Children": {
            "src": "song",
            "parents": { "artist" : [3] }
        }}
    }))?;
    let input = json!({
        "id": 999,
        "name": "Yellow Submarine",
        "memo": "1966"
    });

    let CreateCommand { op: create_op } = from_value(json!({
        "op": {"CreateChild": [{
            "src": "song",
            "parents": { "artist": 3 }
        }, input]}
    }))?;

    // Confirm the state before delete
    create_op.with_schema(&conn, &schema_family)?;
    let records = read_op.with_schema(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 2);

    // Delete the child(or children)
    let DeleteCommand { op: del_op } = from_value(json!({
        "op": {"DeleteChildren": {
            "src": "song",
            "parents": { "artist" : [3] }
        }}
    }))?;
    del_op.with_schema(&conn, &schema_family, None)?;

    let records = read_op.with_schema(&conn, &schema_family, None)?;

    // Verify the state after delete
    assert_eq!(records.len(), 0);

    Ok(())
}

#[test]
fn test_unlink_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    // Confirm before
    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Peers": {
            "src": "song",
            "peers": { "album": [1] }
        }}
    }))?;
    let ReadCommand { op: read_op_2 } = from_value(json!({
        "op": {"Peers": {
            "src": "song",
            "peers": { "album": [2] }
        }}
    }))?;
    assert_eq!(read_op.with_schema(&conn, &schema_family, None)?.len(), 4);
    assert_eq!(read_op_2.with_schema(&conn, &schema_family, None)?.len(), 1);

    // Change
    let PeerCommand { op: rel_op } = from_value(json!({
        "op": {"Unlink": { "song": [1], "album": [1] }}
    }))?;
    rel_op.with_schema(&conn, &schema_family)?;

    let PeerCommand { op: rel_op } = from_value(json!({
        "op": {"Unlink": { "song" : [2], "album": [1] }
        }
    }))?;
    rel_op.with_schema(&conn, &schema_family)?;

    let PeerCommand { op: rel_op } = from_value(json!({
        "op": {"Unlink": { "song": [5], "album": [1]}
        }

    }))?;
    rel_op.with_schema(&conn, &schema_family)?;

    assert_eq!(read_op.with_schema(&conn, &schema_family, None)?.len(), 1);

    let PeerCommand { op: rel_op } = from_value(json!({
        "op": {"Unlink": { "song" : [5], "album": [2] }
        }
    }))?;
    rel_op.with_schema(&conn, &schema_family)?;

    assert_eq!(read_op_2.with_schema(&conn, &schema_family, None)?.len(), 0);

    Ok(())
}

#[test]
fn test_link_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    // Confirm before
    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Peers": {
            "src": "song",
            "peers": { "album": [3] }
        }}
    }))?;
    assert_eq!(read_op.with_schema(&conn, &schema_family, None)?.len(), 0);

    // Change
    let PeerCommand { op: rel_op } = from_value(json!({
        "op": {"Link": { "song": [1], "album": [3] }}
    }))?;
    rel_op.with_schema(&conn, &schema_family)?;

    let PeerCommand { op: rel_op } = from_value(json!({
        "op": {"Link": { "song": [2], "album": [3] }}
    }))?;
    rel_op.with_schema(&conn, &schema_family)?;

    let PeerCommand { op: rel_op } = from_value(json!({
        "op": {"Link": { "song": [3], "album": [3] }
        }
    }))?;
    rel_op.with_schema(&conn, &schema_family)?;

    assert_eq!(read_op.with_schema(&conn, &schema_family, None)?.len(), 3);

    Ok(())
}
