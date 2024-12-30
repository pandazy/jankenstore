mod helpers;
use helpers::initialize_db;

use jankenstore::crud;
use rusqlite::Connection;

#[test]
fn test_count() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let log_count = crud::total::t_all(&conn, "log", None, None)?;
    assert_eq!(log_count, 0);

    let record_count = crud::total::t_all(&conn, "song", Some("name"), None)?;
    assert_eq!(record_count, 6);

    Ok(())
}
