use helpers::initialize_db;
use jankenstore::{
    bond::{self, relink},
    crud::{
        fetch,
        shift::val::{v_int, v_txt},
    },
};
mod helpers;

use rusqlite::Connection;

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

#[test]
fn test_relink_by_id() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let songs = fetch::f_by_pk(&conn, "song", "id", &[v_int(4)], None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(4)));

    relink::n1_by_pk(
        &conn,
        "song",
        ("artist_id", &v_int(1)),
        ("id", &[v_int(4)]),
        None,
    )?;

    let songs = fetch::f_by_pk(&conn, "song", "id", &[v_int(4)], None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(1)));

    Ok(())
}

#[test]
fn test_relink_by_old_fk() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let songs = fetch::f_by_pk(&conn, "song", "id", &[v_int(4)], None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(4)));

    relink::n1_by_ofk(&conn, "song", ("artist_id", &v_int(4), &v_int(1)), None)?;

    let songs = fetch::f_by_pk(&conn, "song", "id", &[v_int(4)], None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(1)));

    Ok(())
}
