//! # Overview
//!
//! This library is designed to provide simple interfaces
//! to complete basic CRUD operations in a SQLite database, by leveraging [rusqlite].
//!
//! This should satisfy 90% of the creator's local app needs
//!
//! # Highlighted Features
//! ## Actions
//! They are Serializeable (see [serde]) and Deserializeable enums that can be used
//! to easily translate JSON requests into SQL operation.
//! Then they can be used to build generic CRUD services,
//! such as web services using [Axum](https://docs.rs/axum/latest/axum/)
//!
//! Currently, the following actions are supported:
//! - [action::CreateOp]
//!    - [action::CreateOp::Create]
//!    - [action::CreateOp::CreateChild]
//! - [action::ReadOp]
//!    - [action::ReadOp::All]
//!    - [action::ReadOp::ByPk]
//!    - [action::ReadOp::Children]
//!    - [action::ReadOp::Peers]
//!    - [action::ReadOp::Search]
//! - [action::UpdateOp]
//!    - [action::UpdateOp::Update]
//!    - [action::UpdateOp::UpdateChildren]
//! - [action::DelOp]
//!    - [action::DelOp::Delete]
//!    - [action::DelOp::DeleteChildren]
//! - [action::PeerOp]
//!    - [action::PeerOp::Link]
//!    - [action::PeerOp::Unlink]
//!
//! ## Schema
//! [sqlite::schema::fetch_schema_family] can be used to automatically extract the schema of the database
//! and use it to validate the input data, reducing the risk of malicious attacks
//!
//! * It should be used together with the actions' [run](action::ReadOp::run) (or additionally, for Create/Update ops, [run_map](action::CreateOp::run_map)) method to validate the input data
//!
//!
//! ## Example of using a Read action
//!
//! See
//! - [this example](https://github.com/pandazy/jankenoboe/blob/main/src/main.rs) of using this library together with [Axum](https://docs.rs/axum/latest/axum/) to create a simple web service
//! - [related frontend code](https://github.com/pandazy/jankenamq-web) that uses the web service above to memorize Anime songs locally
//!
//! Also, see the example below
//! ### Quick code example of how to use a Read action to get data from a SQLite database
//!
//! ```rust
//! use jankenstore::action::{payload::ParsableOp, ReadOp};
//! use jankenstore::sqlite::{
//!     schema::fetch_schema_family,
//!     shift::val::v_txt,
//!     basics::FetchConfig
//! };
//!
//! use rusqlite::Connection;
//! use serde_json::{json, from_value};
//!
//!
//! let conn = Connection::open_in_memory().unwrap();
//!
//! conn.execute_batch(
//!   r#"
//!      CREATE TABLE myexample (
//!        id INTEGER PRIMARY KEY,
//!        name TEXT NOT NULL,
//!        memo TEXT DEFAULT ''
//!     );
//!     INSERT INTO myexample (id, name, memo) VALUES (1, 'Alice', 'big');
//!     INSERT INTO myexample (id,name, memo) VALUES (2, 'Alice', 'little');
//!     INSERT INTO myexample (id, name, memo) VALUES (3, 'Bob', 'big');
//!  "#
//! ).unwrap();
//!
//! /*
//!  Schema family is a collection of table definitions as well as their relationships
//!  following certain conventions, the function below will automatically extract them
//!  and use them as basic violation checks to reduce malicious attacks
//!  */
//! let schema_family = fetch_schema_family(&conn, &[], "", "").unwrap();
//!
//! // get all records that have the primary key 2
//! let op: ReadOp = from_value(json!(
//!       {
//!            "ByPk": {
//!               "src": "myexample",
//!               "keys": [2]
//!            }
//!       })).unwrap();
//! let (results, total) = op.run(&conn, &schema_family, None).unwrap();
//! assert_eq!(results.len(), 1);
//! assert_eq!(results[0]["name"], "Alice");
//! assert_eq!(results[0]["memo"], "little");
//! assert_eq!(total, 1);
//!
//!
//! // get all records by search keyword in the name column
//! // the action can also be created from a string
//! // a practical use case might be if on a API endpoint handler,
//! // the JSON request is received as a string, then
//! let query_param = r#"{ "Search": {
//!       "table": "myexample",
//!       "col": "name",
//!       "keyword": "Alice"
//!    }
//! }"#;
//! let op = ReadOp::from_str(query_param).unwrap();
//! let (results, total) = op.run(&conn, &schema_family, None).unwrap();
//! assert_eq!(results.len(), 2);
//! assert_eq!(results[0]["name"], "Alice");
//! assert_eq!(results[1]["name"], "Alice");
//! assert_eq!(total, 2);
//!
//! // Add further condition to the search by using a FetchConfig
//! let (results, total) = op.run(&conn, &schema_family, Some(FetchConfig{
//!    display_cols: Some(&["name", "memo"]),
//!    is_distinct: true,
//!    where_config: Some(("memo like '%'||?||'%'", &[v_txt("big")])),
//!    group_by: None,
//!    order_by: None,
//!    limit: None,
//!    offset: None
//! })).unwrap();
//! assert_eq!(results.len(), 1);
//! assert_eq!(results[0]["name"], "Alice");
//! assert_eq!(results[0]["memo"], "big");
//! assert_eq!(total, 1);
//!
//! ```

pub mod action;
pub mod sqlite;
