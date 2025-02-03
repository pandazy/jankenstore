use super::{get_peer_pair, RelConfigClientInput};
use crate::{
    peer::{link, unlink},
    schema::SchemaFamily,
};

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RelOp {
    /// Link records into peer (n-to-n) relationships. See also [link]
    /// # Arguments
    /// * `RelConfigClientInput` - The first peer's table and primary key values
    /// * `RelConfigClientInput` - The second peer's table and primary key values
    Link(RelConfigClientInput, RelConfigClientInput),

    /// Unlink records of peer (n-to-n) relationships. See also [unlink]
    /// # Arguments
    /// * `RelConfigClientInput` - The first peer's table and primary key values
    /// * `RelConfigClientInput` - The second peer's table and primary key values
    Unlink(RelConfigClientInput, RelConfigClientInput),
}

impl RelOp {
    ///
    /// Execute the relationship operation on the databases
    /// # Arguments
    /// * `conn` - A connection to the database
    /// * `schema_family` - The schema family of the database
    pub fn with_schema(&self, conn: &Connection, schema_family: &SchemaFamily) -> Result<()> {
        match self {
            Self::Link(peer1_input, peer2_input) => {
                let pair = get_peer_pair(schema_family, peer1_input, peer2_input)?;
                let (peer1, peer2) = pair;
                link(
                    conn,
                    schema_family,
                    (peer1.0.as_str(), peer1.1.as_slice()),
                    (peer2.0.as_str(), peer2.1.as_slice()),
                )?;
            }
            Self::Unlink(peer1_input, peer2_input) => {
                let pair = get_peer_pair(schema_family, peer1_input, peer2_input)?;
                let (peer1, peer2) = pair;
                unlink(
                    conn,
                    schema_family,
                    (peer1.0.as_str(), peer1.1.as_slice()),
                    (peer2.0.as_str(), peer2.1.as_slice()),
                )?;
            }
        }
        Ok(())
    }
}
