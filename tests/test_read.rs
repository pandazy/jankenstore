mod helpers;
use helpers::initialize_db;

use jankenstore::crud;
use rusqlite::Connection;

#[test]
fn test_count() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let count = crud::total::t_all(&conn, "song", Some("name"), None)?;

    assert_eq!(count, 6);
    Ok(())
}
