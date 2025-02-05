use std::collections::HashMap;

use crate::sqlite::{
    input_utils::{json_to_fk_by_schema, json_to_pk_val_by_schema, json_to_val_by_schema},
    schema::SchemaFamily,
};

use anyhow::Result;
use rusqlite::types;
use serde_json::Value as JsonValue;

///
/// Convert the peer information from JSON to a Rusqlite equivalent
pub fn get_peer_info(
    schema_family: &SchemaFamily,
    peers: &HashMap<String, Vec<JsonValue>>,
) -> Result<HashMap<String, Vec<types::Value>>> {
    let mut results = HashMap::new();
    for (peer, keys) in peers {
        let mut peer_info = vec![];
        for peer_val in keys {
            peer_info.push(json_to_pk_val_by_schema(schema_family, peer, peer_val)?);
        }
        results.insert(peer.clone(), peer_info);
    }
    Ok(results)
}

///
/// Convert the parent (>0p) information from JSON to a Rusqlite equivalent
pub fn get_parent_info(
    schema_family: &SchemaFamily,
    data_src: &str,
    parents: &HashMap<String, Vec<JsonValue>>,
) -> Result<HashMap<String, Vec<types::Value>>> {
    let mut results = HashMap::new();
    for (parent, keys) in parents {
        let mut parent_info = vec![];
        for parent_val in keys {
            parent_info.push(json_to_fk_by_schema(
                schema_family,
                data_src,
                parent,
                parent_val,
            )?);
        }
        results.insert(parent.clone(), parent_info);
    }
    Ok(results)
}

///
/// Convert the parent (>0)  information from JSON to a Rusqlite equivalent
pub fn get_one_on_one_parent_info(
    schema_family: &SchemaFamily,
    data_src: &str,
    parents: &HashMap<String, JsonValue>,
) -> Result<HashMap<String, types::Value>> {
    let mut parent_info = HashMap::new();
    for (parent, key) in parents {
        parent_info.insert(
            parent.clone(),
            json_to_fk_by_schema(schema_family, data_src, parent, key)?,
        );
    }
    Ok(parent_info)
}

///
/// Convert the primary key values from JSON to a Rusqlite equivalent
pub fn get_pk_vals(
    schema_family: &SchemaFamily,
    data_src: &str,
    pk_vals: &[JsonValue],
) -> Result<Vec<types::Value>> {
    let mut results = vec![];
    for pk_val in pk_vals {
        results.push(json_to_pk_val_by_schema(schema_family, data_src, pk_val)?);
    }
    Ok(results)
}

pub type PeerPair = ((String, Vec<types::Value>), (String, Vec<types::Value>));

///
/// Convert the peer information from JSON to a Rusqlite equivalent
pub fn get_peer_pair(
    schema_family: &SchemaFamily,
    peer_map: &HashMap<String, Vec<JsonValue>>,
) -> Result<PeerPair> {
    if peer_map.len() != 2 {
        return Err(anyhow::anyhow!(
            "Peer map must contain exactly 2 peers, but found {}. \nSpecifically the invalid inputs are: {:?}",
            peer_map.len(),
           {
                let mut invalid_peers = peer_map.iter().collect::<Vec<_>>();
                invalid_peers.sort_by(|a, b| a.0.cmp(b.0));
                invalid_peers
           }
        ));
    }
    let mut db_peers = [(String::new(), vec![]), (String::new(), vec![])];
    for (i, (peer, keys)) in peer_map.iter().enumerate() {
        for json in keys {
            let fk = json_to_val_by_schema(
                schema_family,
                peer,
                schema_family.try_get_schema(peer)?.pk.as_str(),
                json,
            )?;
            db_peers[i] = (peer.clone(), [db_peers[i].1.clone(), vec![fk]].concat());
        }
    }
    Ok((db_peers[0].clone(), db_peers[1].clone()))
}
