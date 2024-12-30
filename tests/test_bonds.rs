use std::collections::HashMap;

use helpers::initialize_db;
use jankenstore::{
    bond::{self, create, relink},
    crud::{
        self, fetch,
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

    // do nothing if the same artist
    relink::n1_by_ofk(&conn, "song", ("artist_id", &v_int(4), &v_int(4)), None)?;

    // only change if the artist is different
    relink::n1_by_ofk(&conn, "song", ("artist_id", &v_int(4), &v_int(1)), None)?;

    let songs = fetch::f_by_pk(&conn, "song", "id", &[v_int(4)], None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(1)));

    Ok(())
}

#[test]
fn test_relink_nn() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let albums_of_songs = bond::fetch::list_n_of_n(
        &conn,
        ("album", "id", "album_id"),
        ("rel_album_song", "song_id"),
        &[v_int(4)],
        None,
        None,
    )?;

    assert_eq!(albums_of_songs.len(), 0);

    relink::nn(
        &conn,
        "rel_album_song",
        ("album_id", &[v_int(2)]),
        ("song_id", &[v_int(4)]),
    )?;

    let albums_of_songs = bond::fetch::list_n_of_n(
        &conn,
        ("album", "id", "album_id"),
        ("rel_album_song", "song_id"),
        &[v_int(4)],
        None,
        None,
    )?;

    assert_eq!(albums_of_songs.len(), 1);
    assert_eq!(
        albums_of_songs[0].get("name"),
        Some(&v_txt("Anime Songs 1"))
    );

    Ok(())
}

#[test]
fn test_delete_nn_bond() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let songs_of_albums = bond::fetch::list_n_of_n(
        &conn,
        ("song", "id", "song_id"),
        ("rel_album_song", "album_id"),
        &[v_int(1)],
        None,
        None,
    )?;

    assert_eq!(songs_of_albums.len(), 4);

    relink::d_all(
        &conn,
        "rel_album_song",
        ("song_id", &[v_int(1), v_int(2), v_int(3), v_int(5)]),
        ("album_id", &[v_int(1)]),
    )?;

    let songs_of_albums = bond::fetch::list_n_of_n(
        &conn,
        ("song", "id", "song_id"),
        ("rel_album_song", "album_id"),
        &[v_int(1)],
        None,
        None,
    )?;

    assert_eq!(songs_of_albums.len(), 0);

    Ok(())
}

#[test]
fn test_insert_with_n1() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let songs_of_beetles =
        bond::fetch::list_n_of_1(&conn, "song", "artist_id", &[v_int(3)], None, None)?;
    assert_eq!(songs_of_beetles.len(), 1);

    create::n1(
        &conn,
        "song",
        ("artist_id", &v_int(3)),
        &HashMap::from([
            ("id".to_string(), v_int(7)),
            ("name".to_string(), v_txt("Yellow Submarine")),
            ("memo".to_string(), v_txt("60s")),
        ]),
        None,
    )?;

    let songs_of_beetles =
        bond::fetch::list_n_of_1(&conn, "song", "artist_id", &[v_int(3)], None, None)?;

    assert_eq!(songs_of_beetles.len(), 2);
    assert_eq!(
        songs_of_beetles[0].get("name"),
        Some(&v_txt("A Hard Day's Night"))
    );
    assert_eq!(
        songs_of_beetles[1].get("name"),
        Some(&v_txt("Yellow Submarine"))
    );

    Ok(())
}

#[test]
fn test_insert_with_nn() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let new_artist = HashMap::from([
        ("id".to_string(), v_int(6)),
        ("name".to_string(), v_txt("LiSA")),
    ]);

    crud::create::i_one(&conn, "artist", &new_artist, None)?;

    let new_album = HashMap::from([
        ("id".to_string(), v_int(3)),
        ("name".to_string(), v_txt("AniSong 2024")),
    ]);

    let new_album_2 = HashMap::from([
        ("id".to_string(), v_int(4)),
        ("name".to_string(), v_txt("Latest AniSong")),
    ]);

    for new_album in [new_album, new_album_2] {
        crud::create::i_one(&conn, "album", &new_album, None)?;
    }

    // create bonds
    create::nn(
        &conn,
        &HashMap::from([
            ("id".to_string(), v_int(8)),
            ("name".to_string(), v_txt("Queen")),
            ("artist_id".to_string(), v_int(6)),
            ("memo".to_string(), v_txt("Fall 2024")),
        ]),
        ("song", "id"),
        (
            "rel_album_song",
            "album_id",
            "song_id",
            &[v_int(3), v_int(4)],
        ),
        None,
    )?;

    create::nn(
        &conn,
        &HashMap::from([
            ("id".to_string(), v_int(9)),
            ("name".to_string(), v_txt("Shouted Serenade")),
            ("artist_id".to_string(), v_int(6)),
            ("memo".to_string(), v_txt("Spring 2024")),
        ]),
        ("song", "id"),
        (
            "rel_album_song",
            "album_id",
            "song_id",
            &[v_int(3), v_int(4)],
        ),
        None,
    )?;

    [v_int(8), v_int(9)].iter().for_each(|song_id| {
        let albums_of_song = bond::fetch::list_n_of_n(
            &conn,
            ("album", "id", "album_id"),
            ("rel_album_song", "song_id"),
            &[song_id.clone()],
            None,
            None,
        )
        .unwrap();

        assert_eq!(albums_of_song.len(), 2);
        assert_eq!(albums_of_song[0].get("name"), Some(&v_txt("AniSong 2024")));
        assert_eq!(
            albums_of_song[1].get("name"),
            Some(&v_txt("Latest AniSong"))
        );
    });

    [v_int(3), v_int(4)].iter().for_each(|album_id| {
        let songs_of_album = bond::fetch::list_n_of_n(
            &conn,
            ("song", "id", "song_id"),
            ("rel_album_song", "album_id"),
            &[album_id.clone()],
            None,
            None,
        )
        .unwrap();

        assert_eq!(songs_of_album.len(), 2);
        assert_eq!(songs_of_album[0].get("name"), Some(&v_txt("Queen")));
        assert_eq!(
            songs_of_album[1].get("name"),
            Some(&v_txt("Shouted Serenade"))
        );
    });

    Ok(())
}
