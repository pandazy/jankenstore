use jankenstore::{crud::shift::val_to_json, TblRep};

use anyhow::Result;
use rusqlite::{types, Connection};

#[test]
fn test_observability() -> Result<()> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT NOT NULL, count INTEGER DEFAULT 2)",
        [],
    )
    .unwrap();
    let tbl_rep = TblRep::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Text("def".to_string())),
            ("count", types::Value::Integer(2)),
        ],
        &["name"],
    )?;
    assert_eq!(tbl_rep.get_name(), "test");
    assert_eq!(tbl_rep.get_pk_name(), "id");
    assert_eq!(tbl_rep.get_required_fields().len(), 2);
    assert_eq!(tbl_rep.get_defaults().len(), 3);
    assert_eq!(
        tbl_rep.get_defaults().get("name").unwrap(),
        &types::Value::Text("def".to_string())
    );
    assert_eq!(
        tbl_rep.get_defaults().get("count").unwrap(),
        &types::Value::Integer(2)
    );
    assert_eq!(
        tbl_rep.get_defaults().get("id").unwrap(),
        &types::Value::Integer(0)
    );
    Ok(())
}

#[test]
fn test_json_conversion() -> Result<()> {
    let mut map = std::collections::HashMap::new();
    map.insert("id".to_string(), types::Value::Integer(1));
    map.insert("name".to_string(), types::Value::Text("test".to_string()));
    map.insert("count".to_string(), types::Value::Integer(2));
    map.insert("statistics".to_string(), types::Value::Real(3.15));
    map.insert("file".to_string(), types::Value::Blob(vec![1, 2, 3]));
    map.insert("joke".to_string(), types::Value::Null);
    let json = val_to_json(&map)?;
    assert_eq!(
        json["id"],
        serde_json::Value::Number(serde_json::Number::from(1))
    );
    assert_eq!(json["name"], serde_json::Value::String("test".to_string()));
    assert_eq!(
        json["count"],
        serde_json::Value::Number(serde_json::Number::from(2))
    );
    assert_eq!(
        json["statistics"],
        serde_json::Value::Number(serde_json::Number::from_f64(3.15).unwrap())
    );
    assert_eq!(
        json.get("file")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect::<Vec<u64>>(),
        vec![1, 2, 3]
    );
    assert_eq!(json["joke"], serde_json::Value::Null);
    Ok(())
}
