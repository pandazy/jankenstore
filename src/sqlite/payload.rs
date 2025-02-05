use std::collections::HashMap;

use rusqlite::types;

#[derive(Debug, Default, Clone)]
pub struct DbSrcAndKeys {
    pub src: String,
    pub keys: Vec<types::Value>,
}

#[derive(Debug, Default, Clone)]
pub struct DbOneOnOneParentBond {
    pub src: String,
    pub parents: HashMap<String, types::Value>,
}

#[derive(Debug, Default, Clone)]
pub struct DbParentHood {
    pub src: String,
    pub parents: HashMap<String, Vec<types::Value>>,
}

#[derive(Debug, Default, Clone)]
pub struct DbPeerHood {
    pub src: String,
    pub peers: HashMap<String, Vec<types::Value>>,
}
