use std::{collections::HashMap, fmt::Debug};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SrcAndKeys {
    pub src: String,
    pub keys: Vec<JsonValue>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct OneOnOneParentBond {
    pub src: String,
    pub parents: HashMap<String, JsonValue>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ParentHood {
    pub src: String,
    pub parents: HashMap<String, Vec<JsonValue>>,
}

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
        let op: Self = serde_json::from_str(cmd)
            .with_context(|| format!("Failed to parse an action op from string: {}", cmd))?;
        Ok(op)
    }
}
