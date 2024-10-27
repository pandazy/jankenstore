use jankenstore::UnitResource;

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
    let resource = UnitResource::new(
        "test",
        "id",
        &[
            ("id", types::Value::Integer(0)),
            ("name", types::Value::Text("def".to_string())),
            ("count", types::Value::Integer(2)),
        ],
        &["name"],
    )?;
    assert_eq!(resource.get_name(), "test");
    assert_eq!(resource.get_pk_name(), "id");
    assert_eq!(resource.get_required_fields().len(), 2);
    assert_eq!(resource.get_defaults().len(), 3);
    assert_eq!(
        resource.get_defaults().get("name").unwrap(),
        &types::Value::Text("def".to_string())
    );
    assert_eq!(
        resource.get_defaults().get("count").unwrap(),
        &types::Value::Integer(2)
    );
    assert_eq!(
        resource.get_defaults().get("id").unwrap(),
        &types::Value::Integer(0)
    );
    Ok(())
}
