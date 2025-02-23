use std::{collections::HashMap, fmt::Debug};

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

///
/// A struct for representing the source and keys of a record.
///
/// For example, if we want to delete a record from the table "users",
/// we need to specify the `src` as "users" and the `keys` as a list of primary key values.
/// of the users
///
/// # Fields
/// * `src` - The source of the record
/// * `keys` - The keys of the record
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SrcAndKeys {
    pub src: String,
    pub keys: Vec<JsonValue>,
}

///
/// A struct for representing the source and parents of a record.
///
/// For example, if we want to find parent records of a record in the table "users",
/// we need to specify the `src` as "users" and the `parents` as a map of parent table names and parent key values, such as:
/// ```json
/// {
///     "src": "users",
///     "parents": {
///         "source": "sns",
///         "year": "2024"
///     }
/// }
/// ```
/// Note that each parent table can only have one parent record.
///
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct OneOnOneParentBond {
    pub src: String,
    pub parents: HashMap<String, JsonValue>,
}

///
/// A struct for representing the source and parents of a record.
///
/// For example, if we want to find parent records of a record in the table "users",
/// we need to specify the `src` as "users" and the `parents` as a map of parent table names and parent key values, such as:
/// ```json
/// {
///     "src": "users",
///     "parents": {
///         "org": ["dev", "test"]
///     }
/// }
/// ```
///
/// Note that each parent table can have multiple parent keys.
///
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ParentHood {
    pub src: String,
    pub parents: HashMap<String, Vec<JsonValue>>,
}

///
/// A struct for representing the source and peers of a record.
///
/// For example, if we want to find peer records of a record in the table "show",
/// we need to specify the `src` as "show" and the `peers` as a map of peer table names and peer key values, such as:
/// ```json
/// {
///     "src": "show",
///     "peers": {
///         "song": ["1232", "7889"]
///     }
/// }
/// ```
///
/// Note that each peer table can have multiple peer keys.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct PeerHood {
    pub src: String,
    pub peers: HashMap<String, Vec<JsonValue>>,
}

///
/// A trait for parsing an action op from a string
pub trait ParsableOp<'a>: Debug + Serialize + Deserialize<'a> {
    ///
    /// Parse an action op from a string (JSON)
    /// # Arguments
    /// * `cmd` - The string to parse, it must be a valid JSON string
    fn from_str(cmd: &'a str) -> anyhow::Result<Self> {
        let op: Self = serde_json::from_str(cmd)?;
        Ok(op)
    }
}

///
/// A trait for reading the table source of an action op
pub trait ReadSrc {
    ///
    /// Get the table source of the action op
    fn src(&self) -> &str;
}
