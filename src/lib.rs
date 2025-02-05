//! # Overview
//!
//! This library is designed to provide simple interfaces
//! to complete basic CRUD operations in a SQLite database, by leveraging [rusqlite]
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
//! * It should be used together with the actions' `with_schema` method to validate the input data
//!
//!
//! ## Example of using a Read action
//! ```rust
//! use jankenstore::action::ReadCommand;
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
//! let ReadCommand { op } = from_value(json!({
//!       "op": {
//!            "ByPk": {
//!               "src": "myexample",
//!               "keys": [2]
//!            }
//!       }
//!    })).unwrap();
//! let result = op.with_schema(&conn, &schema_family, None).unwrap();
//! assert_eq!(result.len(), 1);
//! assert_eq!(result[0]["name"], "Alice");
//! assert_eq!(result[0]["memo"], "little");
//!
//!
//! // get all records by search keyword in the name column
//! let ReadCommand { op } = from_value(json!({
//!       "op": {
//!            "Search": ["myexample", "name", "Alice"]
//!       }
//!    })).unwrap();
//! let result = op.with_schema(&conn, &schema_family, None).unwrap();
//! assert_eq!(result.len(), 2);
//! assert_eq!(result[0]["name"], "Alice");
//! assert_eq!(result[1]["name"], "Alice");
//!
//! // Add further condition to the search by using a FetchConfig
//! let result = op.with_schema(&conn, &schema_family, Some(FetchConfig{
//!    display_cols: Some(&["name", "memo"]),
//!    is_distinct: true,
//!    where_config: Some(("memo like '%'||?||'%'", &[v_txt("big")]))
//! })).unwrap();
//! assert_eq!(result.len(), 1);
//! assert_eq!(result[0]["name"], "Alice");
//! assert_eq!(result[0]["memo"], "big");
//!
//! ```

pub mod action;
pub mod sqlite;
