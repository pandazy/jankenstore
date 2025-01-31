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

pub fn initialize_db(conn: &Connection) -> anyhow::Result<()> {
    conn.execute(
        "CREATE TABLE song (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          artist_id INTEGER,
          file BLOB,
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
          name TEXT,
          price REAL DEFAULT 0.0,
          memo TEXT DEFAULT ''
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
