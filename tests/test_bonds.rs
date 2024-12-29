use jankenstore::{
    bond,
    crud::shift::val::{v_int, v_txt},
};
use rusqlite::Connection;

fn insert_songs(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO song (id, name, artist_id, memo) VALUES (1, 'When the Saints Go Marching In', 1, '30s')",
        [],
    )?;

    conn.execute(
        "INSERT INTO song (id, name, artist_id, memo) VALUES (2, 'Scarborough Fair / Canticle', 2, '60s')",
        [],
    )?;

    conn.execute(
        "INSERT INTO song (id, name, artist_id, memo) VALUES (3, \"A Hard Day's Night\", 3, '60s')",
        [],
    )?;

    // ani-songs
    conn.execute(
        "INSERT INTO song (id, name, artist_id, memo) VALUES (4, 'Makafushigi Adventure', 4, '80s')",
        [],
    )?;

    conn.execute(
        "INSERT INTO song (id, name, artist_id, memo) VALUES (5, 'We Are!', 5, '90s')",
        [],
    )?;

    conn.execute(
        "INSERT INTO song (id, name, artist_id, memo) VALUES (6, 'We Go!', 5, '2000s')",
        [],
    )?;

    Ok(())
}

fn insert_artists(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO artist (id, name) VALUES (1, 'Louis Armstrong')",
        [],
    )?;

    conn.execute(
        "INSERT INTO artist (id, name) VALUES (2, 'Simon & Garfunkel')",
        [],
    )?;

    conn.execute(
        "INSERT INTO artist (id, name) VALUES (3, 'The Beatles')",
        [],
    )?;

    conn.execute(
        "INSERT INTO artist (id, name) VALUES (4, 'Hiroki Takahashi')",
        [],
    )?;

    conn.execute(
        "INSERT INTO artist (id, name) VALUES (5, 'Hiroshi Kitadani')",
        [],
    )?;

    Ok(())
}

fn insert_albums(conn: &Connection) -> anyhow::Result<()> {
    conn.execute("INSERT INTO album (id, name) VALUES (1, 'Old Songs 1')", [])?;

    conn.execute(
        "INSERT INTO album (id, name) VALUES (2, 'Anime Songs 1')",
        [],
    )?;

    Ok(())
}

fn link_albums_to_songs(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO rel_album_song (album_id, song_id) VALUES (1, 1)",
        [],
    )?;

    conn.execute(
        "INSERT INTO rel_album_song (album_id, song_id) VALUES (1, 2)",
        [],
    )?;

    conn.execute(
        "INSERT INTO rel_album_song (album_id, song_id) VALUES (1, 3)",
        [],
    )?;

    conn.execute(
        "INSERT INTO rel_album_song (album_id, song_id) VALUES (1, 5)",
        [],
    )?;

    conn.execute(
        "INSERT INTO rel_album_song (album_id, song_id) VALUES (2, 5)",
        [],
    )?;

    Ok(())
}

fn initialize_db(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TABLE song (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            artist_id INTEGER,
            memo TEXT DEFAULT ''
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE artist (
            id INTEGER PRIMARY KEY,
            name TEXT
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE album (
            id INTEGER PRIMARY KEY,
            name TEXT
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE rel_album_song (
            album_id INTEGER NOT NULL,
            song_id INTEGER NOT NULL,
            PRIMARY KEY (album_id, song_id),
            FOREIGN KEY (album_id) REFERENCES album(id),
            FOREIGN KEY (song_id) REFERENCES song(id)
        )",
        [],
    )?;

    insert_songs(conn)?;
    insert_artists(conn)?;
    insert_albums(conn)?;
    link_albums_to_songs(conn)?;

    Ok(())
}

#[test]
fn test_read_bonds() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let songs_of_artists =
        bond::fetch::list_n_of_1(&conn, "song", "artist_id", &[v_int(5)], None, None)?;

    assert_eq!(songs_of_artists.len(), 2);
    assert_eq!(songs_of_artists[0].get("name"), Some(&v_txt("We Are!")));
    assert_eq!(songs_of_artists[1].get("name"), Some(&v_txt("We Go!")));

    let albums_of_songs = bond::fetch::list_n_of_n(
        &conn,
        ("album", "id", "album_id"),
        ("rel_album_song", "song_id"),
        &[v_int(5)],
        None,
        None,
    )?;

    assert_eq!(albums_of_songs.len(), 2);
    assert_eq!(albums_of_songs[0].get("name"), Some(&v_txt("Old Songs 1")));
    assert_eq!(
        albums_of_songs[1].get("name"),
        Some(&v_txt("Anime Songs 1"))
    );

    let songs_of_album = bond::fetch::list_n_of_n(
        &conn,
        ("song", "id", "song_id"),
        ("rel_album_song", "album_id"),
        &[v_int(1)],
        None,
        None,
    )?;

    assert_eq!(songs_of_album.len(), 4);
    assert_eq!(
        songs_of_album[0].get("name"),
        Some(&v_txt("When the Saints Go Marching In"))
    );
    assert_eq!(
        songs_of_album[1].get("name"),
        Some(&v_txt("Scarborough Fair / Canticle"))
    );
    assert_eq!(
        songs_of_album[2].get("name"),
        Some(&v_txt("A Hard Day's Night"))
    );
    assert_eq!(songs_of_album[3].get("name"), Some(&v_txt("We Are!")));

    let songs_of_album_with_condition = bond::fetch::list_n_of_n(
        &conn,
        ("song", "id", "song_id"),
        ("rel_album_song", "album_id"),
        &[v_int(1)],
        Some(&["memo"]),
        Some(("artist_id = ?", &[v_int(5)])),
    )?;

    assert_eq!(songs_of_album_with_condition.len(), 1);
    assert_eq!(
        songs_of_album_with_condition[0].get("memo"),
        Some(&v_txt("90s"))
    );

    Ok(())
}
