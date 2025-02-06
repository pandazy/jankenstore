mod helpers;
use helpers::initialize_db;

use jankenstore::{
    action::{payload::ParsableOp, CreateOp, DelOp, PeerOp, ReadOp, UpdateOp},
    sqlite::{schema::fetch_schema_family, shift::val::v_txt},
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

    let create_op: CreateOp = from_value(json!({"Create": ["song", input]}))?;
    create_op.run(&conn, &schema_family)?;

    let by_pk_input = json!({
        "src": "song",
        "keys": [42]
    });
    let read_op: ReadOp = from_value(json!({"ByPk": by_pk_input}))?;
    let record = read_op.run(&conn, &schema_family, None)?;

    assert!(record.len() == 1);
    assert_eq!(record[0]["id"], json!(42));
    assert_eq!(record[0]["name"], json!("test"));
    assert_eq!(record[0]["artist_id"], json!(24));
    assert_eq!(record[0]["memo"], json!("gogo"));

    Ok(())
}

#[test]
fn test_create_with_input_map() -> Result<()> {
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

    let create_op: CreateOp = from_value(json!({"Create": ["song", input]}))?;
    create_op.run_map(&conn, &schema_family, |record, src| {
        assert_eq!(src, "song");

        let mut record = record.clone();
        record.insert("memo".to_owned(), v_txt("Roger that!"));
        Ok(record)
    })?;

    let by_pk_input = json!({
        "src": "song",
        "keys": [42]
    });
    let read_op: ReadOp = from_value(json!({"ByPk": by_pk_input}))?;
    let record = read_op.run(&conn, &schema_family, None)?;

    assert!(record.len() == 1);
    assert_eq!(record[0]["memo"], json!("Roger that!"));

    Ok(())
}

#[test]
fn test_create_child() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let children_read_input = json!({
        "src": "song",
        "parents": { "artist": [3] }
    });
    let read_op: ReadOp = from_value(json!({
        "Children": children_read_input
    }))?;
    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("A Hard Day's Night"));

    let create_op = CreateOp::from_str(
        r#"
        {
            "CreateChild": [
            {
                "src": "song",
                "parents": { "artist":  3 }
            }, 
            {
                "id": 999,
                "name": "Yellow Submarine",
                "memo": "1966"
            }]
        }
        "#,
    )?;
    create_op.run(&conn, &schema_family)?;

    let records = read_op.run(&conn, &schema_family, None)?;

    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["id"], json!(999));
    assert_eq!(records[1]["name"], json!("Yellow Submarine"));
    assert_eq!(records[1]["memo"], json!("1966"));

    Ok(())
}

#[test]
fn test_create_child_with_input_map() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let children_read_input = json!({
        "src": "song",
        "parents": { "artist": [3] }
    });
    let read_op: ReadOp = from_value(json!({
        "Children": children_read_input
    }))?;
    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("A Hard Day's Night"));

    let create_op = CreateOp::from_str(
        r#"
        {
            "CreateChild": [
            {
                "src": "song",
                "parents": { "artist":  3 }
            }, 
            {
                "id": 999,
                "name": "Yellow Submarine",
                "memo": "1966"
            }]
        }
        "#,
    )?;
    create_op.run_map(&conn, &schema_family, |record, _| {
        let mut record = record.clone();
        record.insert("memo".to_owned(), v_txt("60s!"));
        Ok(record)
    })?;

    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["memo"], json!("60s!"));

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
    let update_op: UpdateOp = from_value(json!({
        "Update": [{ "src": "song", "keys": [1] }, input]
    }))?;
    update_op.run(&conn, &schema_family)?;

    let read_song: ReadOp = from_value(json!({
        "ByPk": {
            "src": "song",
            "keys": [1]
        }
    }))?;
    let record = read_song.run(&conn, &schema_family, None)?;
    assert!(record.len() == 1);
    assert_eq!(record[0]["name"], json!("updated"));
    assert_eq!(record[0]["memo"], json!("updated"));

    let input = json!({
        "price": 20.8,
        "memo": 2025
    });
    let update_album: UpdateOp = from_value(json!({
        "Update": [{ "src": "album", "keys": [1] }, input]
    }))?;
    update_album.run(&conn, &schema_family)?;
    let read_album: ReadOp = from_value(json!({
        "ByPk": {
            "src": "album",
            "keys": [1]
        }
    }))?;
    let record = read_album.run(&conn, &schema_family, None)?;
    assert!(record.len() == 1);
    assert_eq!(record[0]["price"], json!(20.8));
    assert_eq!(record[0]["memo"], json!("2025"));

    Ok(())
}

#[test]
fn test_update_with_run_map() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let input = json!({
        "name": "updated",
        "memo": "updated"
    });
    let update_op: UpdateOp = from_value(json!({
        "Update": [{ "src": "song", "keys": [1] }, input]
    }))?;
    update_op.run_map(&conn, &schema_family, |record, src| {
        assert_eq!(src, "song");

        let mut record = record.clone();
        record.insert("memo".to_owned(), v_txt("Roger that!"));
        Ok(record)
    })?;

    let read_song: ReadOp = from_value(json!({
        "ByPk": {
            "src": "song",
            "keys": [1]
        }
    }))?;
    let record = read_song.run(&conn, &schema_family, None)?;
    assert!(record.len() == 1);
    assert_eq!(record[0]["memo"], json!("Roger that!"));

    Ok(())
}

#[test]
fn test_update_children_with_run_map() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let read_op: ReadOp = from_value(json!({
        "Children": {
            "src": "song",
            "parents": { "artist": [3] }
        }
    }))?;

    let input = json!({
        "id": 999,
        "name": "Yellow Submarine",
        "memo": "1966"
    });
    let create_op: CreateOp = from_value(json!({
        "CreateChild": [{
            "src": "song",
            "parents": { "artist": 3 }
        }, input]
    }))?;
    create_op.run(&conn, &schema_family)?;

    // Confirm the state before update
    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["memo"], json!("60s"));
    assert_eq!(records[1]["memo"], json!("1966"));

    // Update the child(or children)
    let update_op = UpdateOp::from_str(
        r#"
        {
            "UpdateChildren": [
                { "src": "song", "parents": { "artist": [3] }}, 
                { "memo": "Congrats!" }
            ]
        }
        "#,
    )?;
    update_op.run_map(&conn, &schema_family, |record, _| {
        let mut record = record.clone();
        record.insert("memo".to_owned(), v_txt("Roger that!"));
        Ok(record)
    })?;
    let records = read_op.run(&conn, &schema_family, None)?;

    // Verify the state after update
    assert_eq!(records.len(), 2);
    for record in records.iter() {
        assert_eq!(record["memo"], json!("Roger that!"));
    }

    Ok(())
}

#[test]
fn test_update_children() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let read_op: ReadOp = from_value(json!({
        "Children": {
            "src": "song",
            "parents": { "artist": [3] }
        }
    }))?;

    let input = json!({
        "id": 999,
        "name": "Yellow Submarine",
        "memo": "1966"
    });
    let create_op: CreateOp = from_value(json!({
        "CreateChild": [{
            "src": "song",
            "parents": { "artist": 3 }
        }, input]
    }))?;

    // Confirm the state before update
    create_op.run(&conn, &schema_family)?;
    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["memo"], json!("60s"));
    assert_eq!(records[1]["memo"], json!("1966"));

    // Update the child(or children)
    let update_op = UpdateOp::from_str(
        r#"
        {
            "UpdateChildren": [
                { "src": "song", "parents": { "artist": [3] }}, 
                { "memo": "Congrats!" }
            ]
        }
        "#,
    )?;
    update_op.run(&conn, &schema_family)?;
    let records = read_op.run(&conn, &schema_family, None)?;

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

    let read_op: ReadOp = from_value(json!({
        "ByPk": {
            "src": "song",
            "keys": [1]
        }
    }))?;

    assert_eq!(read_op.run(&conn, &schema_family, None)?.len(), 1);

    let del_op = DelOp::from_str(r#"{ "Delete": { "src": "song", "keys": [1] } }"#)?;
    del_op.run(&conn, &schema_family, None)?;

    assert_eq!(read_op.run(&conn, &schema_family, None)?.len(), 0);
    Ok(())
}

#[test]
fn test_delete_children() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let input = json!({
        "id": 999,
        "name": "Yellow Submarine",
        "memo": "1966"
    });
    let create_op: CreateOp = from_value(json!({
        "CreateChild": [{
            "src": "song",
            "parents": { "artist": 3 }
        }, input]
    }))?;
    create_op.run(&conn, &schema_family)?;

    let read_op: ReadOp = from_value(json!({
        "Children": {
            "src": "song",
            "parents": { "artist": [3] }
        }
    }))?;

    // Confirm the state before delete
    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 2);

    // Delete the child(or children)
    let del_op: DelOp = from_value(json!({
        "DeleteChildren": {
            "src": "song",
            "parents": { "artist": [3] }
        }
    }))?;
    del_op.run(&conn, &schema_family, None)?;

    let records = read_op.run(&conn, &schema_family, None)?;

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
    let read_op: ReadOp = from_value(json!({
        "Peers": {
            "src": "song",
            "peers": { "album": [1] }
        }
    }))?;
    let read_op_2 = ReadOp::from_str(
        r#"
        {
            "Peers": {
                "src": "song",
                "peers": { "album": [2] }
            }
        }
        "#,
    )?;
    assert_eq!(read_op.run(&conn, &schema_family, None)?.len(), 4);
    assert_eq!(read_op_2.run(&conn, &schema_family, None)?.len(), 1);

    // Change
    let rel_op: PeerOp = from_value(json!({
        "Unlink": { "song": [1], "album": [1] }
    }))?;
    rel_op.run(&conn, &schema_family)?;

    let rel_op = PeerOp::from_str(
        r#"
        { "Unlink": { "song": [2], "album": [1] }}
        "#,
    )?;
    rel_op.run(&conn, &schema_family)?;

    let rel_op: PeerOp = from_value(json!({
        "Unlink": { "song": [5], "album": [1] }
    }))?;
    rel_op.run(&conn, &schema_family)?;

    assert_eq!(read_op.run(&conn, &schema_family, None)?.len(), 1);

    let rel_op: PeerOp = from_value(json!({
        "Unlink": { "song": [5], "album": [2] }
    }))?;
    rel_op.run(&conn, &schema_family)?;

    assert_eq!(read_op_2.run(&conn, &schema_family, None)?.len(), 0);

    Ok(())
}

#[test]
fn test_link_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    // Confirm before
    let read_op: ReadOp = from_value(json!({
        "Peers": {
            "src": "song",
            "peers": { "album": [3] }
        }
    }))?;
    assert_eq!(read_op.run(&conn, &schema_family, None)?.len(), 0);

    // Change
    let rel_op: PeerOp = from_value(json!({
        "Link": { "song": [1], "album": [3] }
    }))?;
    rel_op.run(&conn, &schema_family)?;

    let rel_op: PeerOp = from_value(json!({
        "Link": { "song": [2], "album": [3] }
    }))?;
    rel_op.run(&conn, &schema_family)?;

    let rel_op: PeerOp = from_value(json!({
        "Link": { "song": [3], "album": [3] }
    }))?;
    rel_op.run(&conn, &schema_family)?;

    assert_eq!(read_op.run(&conn, &schema_family, None)?.len(), 3);

    Ok(())
}
