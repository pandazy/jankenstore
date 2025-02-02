mod helpers;
use helpers::initialize_db;

use insta::assert_snapshot;
use jankenstore::{
    action::ReaderOp,
    basics::{CountConfig, FetchConfig},
    read::count,
    schema::fetch_schema_family,
    shift::val::v_txt,
};

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};

#[derive(Deserialize, Serialize)]
struct ReadCommand {
    op: ReaderOp,
}

#[test]
fn test_count() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let result = count(&conn, &schema_family, "song", None)?;
    assert_eq!(result, 6);

    let result = count(
        &conn,
        &schema_family,
        "song",
        Some(CountConfig {
            distinct_field: None,
            where_config: Some(("name like '%'||?||'%'", &[v_txt("ar")])),
        }),
    )?;
    assert_eq!(result, 4);

    let result = count(
        &conn,
        &schema_family,
        "song",
        Some(CountConfig {
            distinct_field: Some("memo"),
            where_config: Some(("name like '%'||?||'%'", &[v_txt("ar")])),
        }),
    )?;
    assert_eq!(result, 3);

    Ok(())
}

#[test]
fn test_reading_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
      "op": {"Peers": ["song", [["album", [1]]]]}
    }))?;

    let records = read_op.with_schema(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 4);
    assert_eq!(records[0]["name"], json!("When the Saints Go Marching In"));

    let records = read_op.with_schema(
        &conn,
        &schema_family,
        Some(FetchConfig {
            display_cols: Some(&["name"]),
            where_config: Some(("name like '%'||?||'%'", &[v_txt("Marching")])),
            ..Default::default()
        }),
    )?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("When the Saints Go Marching In"));

    Ok(())
}

#[test]
fn test_search() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Search": ["song", ["name", "Marching"]]}
    }))?;

    let records = read_op.with_schema(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("When the Saints Go Marching In"));

    let ReadCommand { op: read_op } = from_value(json!({
        "op": {"Search": ["song", ["name", "ar"]]}
    }))?;

    let records = read_op.with_schema(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 4);
    let names = records
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_snapshot!(names.join("\n"));
    Ok(())
}
