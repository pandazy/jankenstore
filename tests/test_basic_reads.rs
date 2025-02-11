mod helpers;
use helpers::initialize_db;

use insta::assert_snapshot;
use jankenstore::{
    action::{
        payload::{ParsableOp, ReadSrc},
        ReadOp,
    },
    sqlite::{
        basics::{CountConfig, FetchConfig},
        read::count,
        schema::fetch_schema_family,
        shift::val::v_txt,
    },
};

use anyhow::Result;
use rusqlite::Connection;
use serde_json::{from_value, json};

#[test]
fn test_count() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;
    let result = count(&conn, &schema_family, "song", None)?;
    assert_eq!(result, 6);

    let search_op = ReadOp::from_str(
        r#"
        {
            "Search": {
                "table": "song",
                "col": "name",
                "keyword": "ar"
            }
        }
    "#,
    )?;

    let result = search_op.run(&conn, &schema_family, None);
    assert_eq!(search_op.src(), "song");
    assert_eq!(result?.len(), 4);

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
fn test_read_all() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op = ReadOp::from_str(r#"{ "All": "song" }"#)?;
    assert_eq!(read_op.src(), "song");

    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 6);
    Ok(())
}

#[test]
fn test_read_by_pagination() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op = ReadOp::from_str(r#"{ "All": "song" }"#)?;

    let records = read_op.run(
        &conn,
        &schema_family,
        Some(FetchConfig {
            limit: Some(2),
            offset: Some(0),
            ..Default::default()
        }),
    )?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], json!("When the Saints Go Marching In"));
    assert_eq!(records[1]["name"], json!("Scarborough Fair / Canticle"));

    let records = read_op.run(
        &conn,
        &schema_family,
        Some(FetchConfig {
            limit: Some(2),
            offset: Some(2),
            ..Default::default()
        }),
    )?;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], json!("A Hard Day's Night"));
    assert_eq!(records[1]["name"], json!("Makafushigi Adventure"));

    let records = read_op.run(
        &conn,
        &schema_family,
        Some(FetchConfig {
            limit: Some(2),
            offset: Some(4),
            ..Default::default()
        }),
    )?;

    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], json!("We Are!"));
    assert_eq!(records[1]["name"], json!("We Go!"));

    Ok(())
}

#[test]
fn test_group_by() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op = ReadOp::from_str(r#"{ "All": "song" }"#)?;

    let records = read_op.run(
        &conn,
        &schema_family,
        Some(FetchConfig {
            display_cols: Some(&["artist_id", "count(*) as count"]),
            group_by: Some("artist_id "),
            ..Default::default()
        }),
    )?;
    assert_eq!(records.len(), 5);
    assert_eq!(records[0]["artist_id"], json!(1));
    assert_eq!(records[0]["count"], json!(1));

    assert_eq!(records[1]["artist_id"], json!(2));
    assert_eq!(records[1]["count"], json!(1));

    assert_eq!(records[2]["artist_id"], json!(3));
    assert_eq!(records[2]["count"], json!(1));

    assert_eq!(records[3]["artist_id"], json!(4));
    assert_eq!(records[3]["count"], json!(1));

    assert_eq!(records[4]["artist_id"], json!(5));
    assert_eq!(records[4]["count"], json!(2));

    Ok(())
}

#[test]
fn test_order_by() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op = ReadOp::from_str(r#"{ "All": "song" }"#)?;

    let records = read_op.run(
        &conn,
        &schema_family,
        Some(FetchConfig {
            order_by: Some("name desc"),
            ..Default::default()
        }),
    )?;
    assert_eq!(records.len(), 6);
    [
        "When the Saints Go Marching In",
        "We Go!",
        "We Are!",
        "Scarborough Fair / Canticle",
        "Makafushigi Adventure",
        "A Hard Day's Night",
    ]
    .iter()
    .enumerate()
    .for_each(|(i, name)| {
        assert_eq!(records[i]["name"], json!(name));
    });

    Ok(())
}

#[test]
fn test_reading_peers() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let schema_family = fetch_schema_family(&conn, &[], "", "")?;

    let read_op = ReadOp::from_str(
        r#"{
                "Peers": {
                    "src": "song",
                    "peers": { "album": [1] }
                }
            }"#,
    )?;

    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(read_op.src(), "song");
    assert_eq!(records.len(), 4);
    assert_eq!(records[0]["name"], json!("When the Saints Go Marching In"));

    let records = read_op.run(
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

    let read_op = ReadOp::from_str(
        r#"{ "Search": {
            "table": "song",
            "col": "name",
            "keyword": "Marching",
            "exact": false }}"#,
    )?;

    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("When the Saints Go Marching In"));

    let read_op: ReadOp = from_value(json!({
        "Search": {"table": "song", "col": "name", "keyword": "ar"}
    }))?;

    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 4);
    let names = records
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_snapshot!(names.join("\n"));

    let read_op: ReadOp = from_value(json!({
        "Search": {"table": "song", "col": "name", "keyword": "When the Saints Go Marching In", "exact": true}
    }))?;
    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["name"], json!("When the Saints Go Marching In"));

    let read_op: ReadOp = from_value(json!({
        "Search": {"table": "song", "col": "name", "keyword": "When", "exact": true}
    }))?;
    let records = read_op.run(&conn, &schema_family, None)?;
    assert_eq!(records.len(), 0);
    Ok(())
}
