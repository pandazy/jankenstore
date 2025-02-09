mod helpers;
use std::collections::HashSet;

use helpers::initialize_db;

use insta::assert_snapshot;
use jankenstore::sqlite::schema::{fetch_schema_family, SchemaFamily};
use rusqlite::Connection;
use serde_json::json;

#[test]
fn test_fetch_schema_with_wrong_column_type() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    conn.execute(
        "
        CREATE TABLE log (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            attachment BLOB,
            price REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )",
        [],
    )?;

    let schema_family = SchemaFamily::fetch(&conn, &[], "", "");
    assert!(schema_family.is_err());
    assert_snapshot!(schema_family.unwrap_err());
    Ok(())
}

#[test]
fn test_fetch_schema() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    conn.execute(
        "
        CREATE TABLE log (
            id INTEGER PRIMARY KEY,
            content TEXT NOT NULL,
            attachment BLOB,
            price REAL
        )",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let schema_json = schema_family.json()?;
    let schema_info_map = schema_json.as_object().unwrap();

    assert_eq!(
        schema_info_map
            .get("map")
            .unwrap()
            .as_object()
            .unwrap()
            .len(),
        5
    );

    // assert peers
    let peers = schema_info_map.get("peers").unwrap().as_object().unwrap();
    assert_eq!(peers.get("song").unwrap().as_array().unwrap(), &["album"]);
    assert_eq!(peers.get("album").unwrap().as_array().unwrap(), &["song"]);

    assert!(schema_info_map.contains_key("parents"));

    Ok(())
}

#[test]
fn test_fetch_schema_with_invalid_peer_table() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE rel_writer_audience_company (id INTEGER PRIMARY KEY, name TEXT NOT NULL, artist_id INTEGER, memo TEXT DEFAULT '')",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "");
    assert!(schema_family.is_err());
    assert_snapshot!(schema_family.unwrap_err());
    Ok(())
}

#[test]
fn test_fetch_schema_with_peer_tables_missing_link_columns() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE rel_artist_album(id INTEGER PRIMARY KEY, writer_id TEXT NOT NULL, artist_id INTEGER, memo TEXT DEFAULT '')",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "");
    assert!(schema_family.is_err());
    assert_snapshot!(schema_family.unwrap_err());
    Ok(())
}

#[test]
fn test_fetch_schema_with_unknown_peer_tables() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE rel_artist_audience(id INTEGER PRIMARY KEY, artist_id TEXT NOT NULL, audience_id INTEGER)",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "");
    assert!(schema_family.is_err());
    assert_snapshot!(schema_family.unwrap_err());
    Ok(())
}

#[test]
fn test_fetch_schema_with_multiple_peers() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE rel_artist_album(id INTEGER PRIMARY KEY, artist_id TEXT NOT NULL, album_id INTEGER)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE chatgroup(id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE rel_song_chatgroup(id INTEGER PRIMARY KEY, song_id TEXT NOT NULL, chatgroup_id INTEGER)",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let peers_of_song = schema_family.peers.get("song").unwrap();
    assert_eq!(peers_of_song.len(), 2);
    assert_eq!(
        peers_of_song,
        &["album", "chatgroup"]
            .iter()
            .map(|v| v.to_string())
            .collect::<HashSet<String>>()
    );

    let peers_of_album = schema_family.peers.get("album").unwrap();
    assert_eq!(
        peers_of_album,
        &["song", "artist"]
            .iter()
            .map(|v| v.to_string())
            .collect::<HashSet<String>>()
    );

    Ok(())
}

#[test]
fn test_fetch_schema_with_multiple_parenthood() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE log(id INTEGER PRIMARY KEY, artist_id INTEGER NOT NULL, audience_id INTEGER, company_id INTEGER)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE company(id INTEGER PRIMARY KEY, memo TEXT NOT NULL)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE audience(id INTEGER PRIMARY KEY, location TEXT NOT NULL)",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let schema_family_json = schema_family.json()?;
    let schema_info_map = &schema_family_json.as_object().unwrap();
    let parents = schema_info_map.get("parents").unwrap().as_object().unwrap();

    assert_eq!(parents.len(), 2);

    assert_eq!(parents.get("log").unwrap().as_array().unwrap().len(), 3);
    for parent in ["artist", "audience", "company"] {
        assert!(parents
            .get("log")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect::<HashSet<&str>>()
            .contains(&parent));
    }

    assert_eq!(
        parents.get("song").unwrap().as_array().unwrap(),
        &["artist"]
    );

    let children = schema_info_map
        .get("children")
        .unwrap()
        .as_object()
        .unwrap();
    assert_eq!(children.len(), 3);

    let artist_children = children.get("artist").unwrap().as_array().unwrap();
    assert_eq!(artist_children.len(), 2);
    assert!(artist_children.contains(&json!("song")));
    assert!(artist_children.contains(&json!("log")));

    assert_eq!(
        children.get("audience").unwrap().as_array().unwrap(),
        &["log"]
    );
    assert_eq!(
        children.get("company").unwrap().as_array().unwrap(),
        &["log"]
    );
    Ok(())
}

#[test]
fn test_fetch_schema_skip_table() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE log(id INTEGER PRIMARY KEY, artist_id TEXT NOT NULL, audience_id INTEGER, company_id INTEGER)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE company(id INTEGER PRIMARY KEY, memo TEXT NOT NULL)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE audience(id INTEGER PRIMARY KEY, location TEXT NOT NULL)",
        [],
    )?;

    let excludes = ["company", "log", "audience"];
    let schema_family = fetch_schema_family(&conn, &excludes, "", "")?;
    let schema_family_json = schema_family.json()?;
    let schema_info_map = &schema_family_json.as_object().unwrap();
    assert_eq!(schema_info_map.len(), 4);
    excludes.iter().for_each(|table| {
        assert!(!schema_info_map.contains_key(*table));
    });
    Ok(())
}

#[test]
fn test_custom_prefix_splitter() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE link__artist__album (id INTEGER PRIMARY KEY, artist_id TEXT NOT NULL, album_id INTEGER)",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "link", "__")?;
    let schema_family_json = schema_family.json()?;
    let schema_info_map = &schema_family_json.as_object().unwrap();
    assert_eq!(schema_info_map.len(), 4);
    assert_eq!(
        schema_family.peers.get("artist").unwrap(),
        &["album"]
            .iter()
            .cloned()
            .map(String::from)
            .collect::<HashSet<String>>()
    );
    Ok(())
}

#[test]
fn test_wrong_fk_types() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE chat_history(serial INTEGER PRIMARY KEY, artist_id Integer NOT NULL, content TEXT NOT NULL)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE chat_video(id INTEGER PRIMARY KEY, chat_history_serial TEXT NOT NULL, video BLOB)",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "");
    assert!(schema_family.is_err());
    assert_snapshot!(schema_family.unwrap_err());

    Ok(())
}

#[test]
fn test_different_pk_names() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    conn.execute(
        "CREATE TABLE chathistory(serial INTEGER PRIMARY KEY, artist_id Integer NOT NULL, content TEXT NOT NULL)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE chatvideo(id INTEGER PRIMARY KEY, chathistory_serial INTEGER NOT NULL, video BLOB)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE chatcategory(uuid TEXT PRIMARY KEY, TEXT TEXT NOT NULL)",
        [],
    )?;

    conn.execute(
        "CREATE TABLE rel_chathistory_chatcategory(id INTEGER PRIMARY KEY, chathistory_serial INTEGER NOT NULL, chatcategory_uuid TEXT NOT NULL)",
        [],
    )?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    assert!(&schema_family
        .peers
        .get("chathistory")
        .unwrap()
        .contains("chatcategory"));

    assert!(&schema_family
        .peers
        .get("chatcategory")
        .unwrap()
        .contains("chathistory"));

    Ok(())
}
