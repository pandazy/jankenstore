use std::collections::HashMap;

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
