use std::collections::HashMap;

use helpers::initialize_db;
use insta::assert_snapshot;
use jankenstore::{
    bond::{
        self, create, relink,
        wrap::{N1Wrap, NnWrap},
    },
    crud::{
        self, fetch,
        shift::val::{v_int, v_txt},
    },
    TblRep,
};
mod helpers;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

fn get_infos() -> anyhow::Result<(TblRep, TblRep, TblRep)> {
    let artist_rep = TblRep::new(
        "artist",
        "id",
        &[("id", v_int(0)), ("name", v_txt(""))],
        &["name"],
    )?;
    let song_rep = TblRep::new(
        "song",
        "id",
        &[
            ("id", v_int(0)),
            ("name", v_txt("")),
            ("artist_id", v_int(0)),
            ("memo", v_txt("")),
        ],
        &["name", "artist_id"],
    )?;

    let album_rep = TblRep::new(
        "album",
        "id",
        &[("id", v_int(0)), ("name", v_txt(""))],
        &["name"],
    )?;

    Ok((artist_rep, song_rep, album_rep))
}

fn get_wraps<'a>(
    (artist_rep, song_rep, album_rep): (&'a TblRep, &'a TblRep, &'a TblRep),
) -> anyhow::Result<(N1Wrap<'a>, NnWrap<'a>)> {
    let n1_wrap = N1Wrap::new((song_rep, "artist_id"), artist_rep);
    let n2_wrap = NnWrap::new(
        song_rep,
        album_rep,
        ("rel_album_song", "song_id", "album_id"),
    );

    Ok((n1_wrap, n2_wrap))
}

#[derive(Debug, Deserialize, Serialize)]
struct Artist {
    id: i64,
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Song {
    id: i64,
    name: String,
    artist_id: i64,
    memo: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Album {
    id: i64,
    name: String,
}

#[test]
fn test_read_bonds() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    let (artist_rep, song_rep, album_rep) = get_infos()?;
    let (songs_artists_n1, songs_albums_nn) = get_wraps((&artist_rep, &song_rep, &album_rep))?;
    assert_eq!(songs_artists_n1.get_tn().get_name(), "song");
    assert_eq!(songs_artists_n1.get_t1().get_name(), "artist");
    assert_eq!(songs_albums_nn.get_t1().get_name(), "song");
    assert_eq!(songs_albums_nn.get_t2().get_name(), "album");

    let songs_of_artists_raw = songs_artists_n1.list_kids(&conn, &[v_int(5)], None, None)?;
    assert_eq!(songs_of_artists_raw.len(), 2);

    let songs_of_artists = songs_artists_n1.list_kids_as::<Song>(&conn, &[v_int(5)], None, None)?;

    assert_eq!(songs_of_artists.len(), 2);
    assert_eq!(songs_of_artists[0].name, "We Are!");
    assert_eq!(songs_of_artists[1].name, "We Go!");

    let albums_of_songs_raw = songs_albums_nn.peers_of_t2(&conn, &[v_int(5)], None, None)?;
    assert_eq!(albums_of_songs_raw.len(), 2);

    let albums_of_songs =
        songs_albums_nn.peers_of_t2_as::<Album>(&conn, &[v_int(5)], None, None)?;

    assert_eq!(albums_of_songs.len(), 2);
    assert_eq!(albums_of_songs[0].name, "Old Songs 1");
    assert_eq!(albums_of_songs[1].name, "Anime Songs 1");

    let songs_of_albums_raw = songs_albums_nn.peers_of_t1(&conn, &[v_int(1)], None, None)?;
    assert_eq!(songs_of_albums_raw.len(), 4);

    let songs_of_album = songs_albums_nn.peers_of_t1_as::<Song>(&conn, &[v_int(1)], None, None)?;

    assert_eq!(songs_of_album.len(), 4);
    assert_eq!(songs_of_album[0].name, "When the Saints Go Marching In");
    assert_eq!(songs_of_album[1].name, "Scarborough Fair / Canticle");
    assert_eq!(songs_of_album[2].name, "A Hard Day's Night");
    assert_eq!(songs_of_album[3].name, "We Are!");

    let songs_of_album_with_condition = songs_albums_nn.peers_of_t1_as::<Song>(
        &conn,
        &[v_int(1)],
        Some(&["id", "name", "artist_id", "memo"]),
        Some(("artist_id = ?", &[v_int(5)])),
    )?;

    assert_eq!(songs_of_album_with_condition.len(), 1);
    assert_eq!(songs_of_album_with_condition[0].memo, "90s");

    Ok(())
}

#[test]
fn test_relink_by_id() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let songs = fetch::f_by_pk(&conn, "song", ("id", &[v_int(4)]), None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(4)));

    relink::n1_by_pk(
        &conn,
        "song",
        ("artist_id", &v_int(1)),
        ("id", &[v_int(4)]),
        None,
    )?;

    let songs = fetch::f_by_pk(&conn, "song", ("id", &[v_int(4)]), None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(1)));

    Ok(())
}

#[test]
fn test_relink_by_old_fk() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;
    let (artist_rep, song_rep, album_rep) = get_infos()?;
    let (songs_artists_n1, _) = get_wraps((&artist_rep, &song_rep, &album_rep))?;

    let songs = fetch::f_by_pk(&conn, "song", ("id", &[v_int(4)]), None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(4)));

    // do nothing if the same artist
    songs_artists_n1.relink(&conn, &v_int(4), &v_int(4), None)?;

    // only change if the artist is different
    songs_artists_n1.relink(&conn, &v_int(4), &v_int(1), None)?;

    let songs = fetch::f_by_pk(&conn, "song", ("id", &[v_int(4)]), None, None)?;
    assert_eq!(songs[0].get("artist_id"), Some(&v_int(1)));

    Ok(())
}

#[test]
fn test_relink_nn() -> anyhow::Result<()> {
    let conn = Connection::open_in_memory()?;
    initialize_db(&conn)?;

    let (artist_rep, song_rep, album_rep) = get_infos()?;
    let (_, songs_albums_nn) = get_wraps((&artist_rep, &song_rep, &album_rep))?;

    let albums_of_songs = songs_albums_nn.peers_of_t2(&conn, &[v_int(4)], None, None)?;
    assert_eq!(albums_of_songs.len(), 0);

    songs_albums_nn.link(&conn, &[v_int(4)], &[v_int(2)])?;

    let albums_of_songs = bond::fetch::list_n_of_n(
        &conn,
        ("album", "id", "album_id"),
        ("rel_album_song", "song_id", &[v_int(4)]),
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

    let (artist_rep, song_rep, album_rep) = get_infos()?;
    let (_, songs_albums_nn) = get_wraps((&artist_rep, &song_rep, &album_rep))?;

    let songs_of_albums = bond::fetch::list_n_of_n(
        &conn,
        ("song", "id", "song_id"),
        ("rel_album_song", "album_id", &[v_int(1)]),
        None,
        None,
    )?;

    assert_eq!(songs_of_albums.len(), 4);

    songs_albums_nn.unlink(
        &conn,
        &[v_int(1), v_int(2), v_int(3), v_int(5)],
        &[v_int(1)],
    )?;

    let songs_of_albums = bond::fetch::list_n_of_n(
        &conn,
        ("song", "id", "song_id"),
        ("rel_album_song", "album_id", &[v_int(1)]),
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

    let (artist_rep, song_rep, album_rep) = get_infos()?;
    let (songs_artists_n1, _) = get_wraps((&artist_rep, &song_rep, &album_rep))?;

    let songs_of_beetles = songs_artists_n1.list_kids_as::<Song>(&conn, &[v_int(3)], None, None)?;
    assert_eq!(songs_of_beetles.len(), 1);

    songs_artists_n1.ins(
        &conn,
        &v_int(3),
        &HashMap::from([
            ("id".to_string(), v_int(7)),
            ("name".to_string(), v_txt("Yellow Submarine")),
            ("memo".to_string(), v_txt("60s")),
        ]),
        None,
    )?;

    let songs_of_beetles =
        bond::fetch::list_n_of_1(&conn, "song", ("artist_id", &[v_int(3)]), None, None)?;

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
    let (artist_rep, song_rep, album_rep) = get_infos()?;
    let (_, songs_albums_nn) = get_wraps((&artist_rep, &song_rep, &album_rep))?;

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

    songs_albums_nn.ins_t1(
        &conn,
        &HashMap::from([
            ("id".to_string(), v_int(8)),
            ("name".to_string(), v_txt("Queen")),
            ("artist_id".to_string(), v_int(6)),
            ("memo".to_string(), v_txt("Fall 2024")),
        ]),
        &[v_int(3), v_int(4)],
        None,
    )?;

    songs_albums_nn.ins_t1(
        &conn,
        &HashMap::from([
            ("id".to_string(), v_int(9)),
            ("name".to_string(), v_txt("Shouted Serenade")),
            ("artist_id".to_string(), v_int(6)),
            ("memo".to_string(), v_txt("Spring 2024")),
        ]),
        &[v_int(3), v_int(4)],
        None,
    )?;

    [v_int(8), v_int(9)].iter().for_each(|song_id| {
        let albums_of_song = bond::fetch::list_n_of_n(
            &conn,
            ("album", "id", "album_id"),
            ("rel_album_song", "song_id", &[song_id.clone()]),
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
            ("rel_album_song", "album_id", &[album_id.clone()]),
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

    songs_albums_nn.ins_t2(
        &conn,
        &HashMap::from([
            ("id".to_string(), v_int(10)),
            ("name".to_string(), v_txt("Spring 2024 Songs")),
        ]),
        &[v_int(9)],
        None,
    )?;

    let songs_of_album = songs_albums_nn.peers_of_t1_as::<Song>(&conn, &[v_int(10)], None, None)?;

    assert_eq!(songs_of_album.len(), 1);
    assert_eq!(songs_of_album[0].name, "Shouted Serenade");

    Ok(())
}

#[test]
fn test_insert_with_nn_errors() -> anyhow::Result<()> {
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
    let empty_pk_error = create::nn(
        &conn,
        &HashMap::from([
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
    )
    .err()
    .unwrap();

    assert_snapshot!(empty_pk_error.to_string());

    let empty_fk_error = create::nn(
        &conn,
        &HashMap::from([
            ("id".to_string(), v_int(8)),
            ("name".to_string(), v_txt("Queen")),
            ("artist_id".to_string(), v_int(6)),
            ("memo".to_string(), v_txt("Fall 2024")),
        ]),
        ("song", "id"),
        ("rel_album_song", "album_id", "song_id", &[]),
        None,
    )
    .err()
    .unwrap();

    assert_snapshot!(empty_fk_error.to_string());

    Ok(())
}
