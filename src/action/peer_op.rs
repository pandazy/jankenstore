use std::collections::HashMap;

use super::get_peer_pair;
use crate::sqlite::{
    peer::{link, unlink},
    schema::SchemaFamily,
};

use anyhow::Result;
use rusqlite::{types, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

///
/// Providing the operations that can create or delete peer relationships
/// using JSON-compatible parameters
#[derive(Debug, Serialize, Deserialize)]
pub enum PeerOp {
    ///
    /// Link records into peer (n-to-n) relationships. See also [link]
    /// # Arguments
    /// * `HashMap<String, Vec<JsonValue>>` - The 2 types of peers and their primary key values to link
    ///                                       - it should have EXACTLY 2 items
    Link(HashMap<String, Vec<JsonValue>>),

    ///
    /// Unlink records of peer (n-to-n) relationships. See also [unlink]
    /// # Arguments
    /// * `HashMap<String, Vec<JsonValue>>` - The 2 types of peers and their primary key values to unlink
    ///                                       - it should have EXACTLY 2 items
    Unlink(HashMap<String, Vec<JsonValue>>),
}

impl PeerOp {
    ///
    /// Execute the operation on the databases
    /// # Arguments
    /// * `conn` - A connection to the database
    /// * `schema_family` - The schema family of the database
    pub fn with_schema(&self, conn: &Connection, schema_family: &SchemaFamily) -> Result<()> {
        let get_input = |peer_map| -> Result<HashMap<String, Vec<types::Value>>> {
            let pair = get_peer_pair(schema_family, peer_map)?;
            Ok(HashMap::from([pair.0, pair.1]))
        };
        match self {
            Self::Link(peer_map) => {
                link(conn, schema_family, &get_input(peer_map)?)?;
            }
            Self::Unlink(peer_map) => {
                unlink(conn, schema_family, &get_input(peer_map)?)?;
            }
        }
        Ok(())
    }
}
