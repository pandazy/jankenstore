use jankenstore::{
    crud::{fetch, shift::val::v_txt},
    TblRep,
};

use anyhow::Result;
use insta::assert_snapshot;
use rusqlite::{types, Connection};
use std::collections::HashMap;

#[test]
fn test_query_input_errors() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )?;
    let tbl_rep = TblRep::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Text("".to_string())),
            ("count", types::Value::Integer(2)),
        ],
        &["name"],
    )?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    tbl_rep.insert(&conn, &input, true)?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(2)),
        ("name".to_string(), types::Value::Text("test2".to_string())),
        ("count".to_string(), types::Value::Integer(6)),
    ]);
    tbl_rep.insert(&conn, &input, true)?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(3)),
        ("name".to_string(), types::Value::Text("test3".to_string())),
    ]);
    tbl_rep.insert(&conn, &input, true)?;

    let no_table_name_err = fetch::f_all(&conn, "", false, None, None).err().unwrap();
    assert_eq!(
        no_table_name_err.to_string(),
        "The table name cannot be an empty string"
    );

    let query = tbl_rep.list(&conn, false, None, None)?;
    assert_eq!(query.len(), 3);

    let no_where_clause_err = tbl_rep
        .list_by_pk(&conn, &["2"].map(v_txt), Some(("", &[])))
        .err()
        .unwrap();
    assert_snapshot!(no_where_clause_err.to_string());

    Ok(())
}

#[test]
fn test_write_op_query_errors() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )?;
    let tbl_rep = TblRep::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Text("".to_string())),
            ("count", types::Value::Integer(2)),
        ],
        &["name"],
    )?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    let update_where_err = tbl_rep
        .upd_by_pk(&conn, &["1"].map(v_txt), &input, Some(("", &[])))
        .err()
        .unwrap();
    assert_snapshot!(update_where_err.to_string());

    let delete_error = tbl_rep
        .del_by_pk(&conn, &["1"].map(v_txt), Some(("", &[])))
        .err()
        .unwrap();
    assert_snapshot!(delete_error.to_string());

    let delete_error = tbl_rep.del_by_pk(&conn, &[], None).err().unwrap();
    assert_snapshot!(delete_error.to_string());

    let delete_error = tbl_rep
        .del_by_pk(&conn, &["1", "", "3"].map(v_txt), None)
        .err()
        .unwrap();
    assert_snapshot!(delete_error.to_string());

    let delete_error = tbl_rep
        .del_by_pk(&conn, &["1", " ", "3"].map(v_txt), None)
        .err()
        .unwrap();
    assert_snapshot!(delete_error.to_string());

    Ok(())
}

#[test]
fn test_invalid_inputs() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )?;

    let problematic_tbl_rep_error = TblRep::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Null),
            ("count", types::Value::Integer(2)),
        ],
        &["name"],
    )
    .err()
    .unwrap();
    assert_snapshot!(problematic_tbl_rep_error.to_string());

    let tbl_rep = TblRep::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Text("".to_string())),
            ("count", types::Value::Integer(2)),
        ],
        &["name"],
    )?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Text("abc".to_string())),
        ("name".to_string(), types::Value::Text("test".to_string())),
    ]);
    let mismatch_err = tbl_rep.insert(&conn, &input, true).err().unwrap();
    assert_snapshot!(mismatch_err.to_string());

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        (
            "name".to_string(),
            types::Value::Blob("test".as_bytes().to_vec()),
        ),
    ]);
    let mismatch_blob_err = tbl_rep.insert(&conn, &input, true).err().unwrap();
    assert_snapshot!(mismatch_blob_err.to_string());

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
        ("count".to_string(), types::Value::Real(2.0)),
    ]);
    let mismatch_real_err = tbl_rep.insert(&conn, &input, true).err().unwrap();
    assert_snapshot!(mismatch_real_err.to_string());

    let input = HashMap::new();
    let err = tbl_rep.insert(&conn, &input, false);
    assert_eq!(
        err.err().unwrap().to_string(),
        "(table: test) The input has no items"
    );

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
        ("age".to_string(), types::Value::Integer(18)),
    ]);
    let err = tbl_rep.insert(&conn, &input, true).err().unwrap();
    assert_eq!(
        err.to_string(),
        "(table: test) The input has a key 'age' that is not allowed"
    );

    let input = HashMap::from([("id".to_string(), types::Value::Integer(1))]);

    let no_required_name_err = tbl_rep.insert(&conn, &input, true).err().unwrap();
    assert_eq!(
        no_required_name_err.to_string(),
        "(table: test) The input requires the value of 'name'"
    );

    let input: HashMap<String, types::Value> =
        HashMap::from([("name".to_string(), types::Value::Text("".to_string()))]);
    let no_required_name_err_for_empty = tbl_rep
        .upd_by_pk(&conn, &["1"].map(v_txt), &input, None)
        .err()
        .unwrap();
    assert_eq!(
        no_required_name_err_for_empty.to_string(),
        "(table: test) The input requires the value of 'name'"
    );

    let input: HashMap<String, types::Value> =
        HashMap::from([("name".to_string(), types::Value::Text("alice".to_string()))]);
    let empty_pk_error = tbl_rep.upd_by_pk(&conn, &[], &input, None).err().unwrap();
    assert_snapshot!(empty_pk_error.to_string());

    let blob_tbl_rep = TblRep::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Text("".to_string())),
            ("count", types::Value::Integer(2)),
            ("data", types::Value::Blob(vec![])),
        ],
        &["name", "data"],
    )?;

    let input = HashMap::from([
        ("id".to_string(), types::Value::Integer(1)),
        ("name".to_string(), types::Value::Text("test".to_string())),
        ("data".to_string(), types::Value::Blob(vec![])),
    ]);
    let no_required_blob_err_for_empty = blob_tbl_rep
        .upd_by_pk(&conn, &["1"].map(v_txt), &input, None)
        .err()
        .unwrap();
    assert_eq!(
        no_required_blob_err_for_empty.to_string(),
        "(table: test) The input requires the value of 'data'"
    );

    Ok(())
}
