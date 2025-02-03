use crate::{
    input_utils::{json_to_fk_by_schema, json_to_pk_val_by_schema, json_to_val_by_schema},
    schema::SchemaFamily,
};

use anyhow::Result;
use rusqlite::types;
use serde_json::Value as JsonValue;

///
/// The input for a relationship (parent/peer) configuration
/// `String` - The name of the table
/// `Vec<JsonValue>` - The primary/foreign key values (SerdeJSON) of the records
pub type RelConfigClientInput = (String, Vec<JsonValue>);

///
/// The input for a single relationship (parent/peer) configuration.
/// `String` - The name of the table
/// `JsonValue` - The primary/foreign key value (SerdeJSON) of the record
pub type RelConfigClientInputSingle = (String, JsonValue);

/// The Rusqlite equivalent of a relationship(parent/peer) configuration
/// `String` - The name of the table
/// `Vec<types::Value>` - The primary/foreign key values (Rusqlite) of the records
pub type RelConfigRus = (String, Vec<types::Value>);

///
/// The Rusqlite equivalent of a single relationship(parent/peer) configuration
/// `String` - The name of the table
/// `types::Value` - The primary/foreign key value (Rusqlite) of the record
pub type RelConfigRusSingle = (String, types::Value);

///
/// Convert the peer information from JSON to a Rusqlite equivalent
pub fn get_peer_info(
    schema_family: &SchemaFamily,
    peers: &Vec<RelConfigClientInput>,
) -> Result<Vec<RelConfigRus>> {
    let mut results = vec![];
    for (peer_table, peer_vals) in peers {
        let mut peer_info = vec![];
        for peer_val in peer_vals {
            peer_info.push(json_to_pk_val_by_schema(
                schema_family,
                peer_table,
                peer_val,
            )?);
        }
        results.push((peer_table.clone(), peer_info));
    }
    Ok(results)
}

///
/// Convert the parent (>0p) information from JSON to a Rusqlite equivalent
pub fn get_parent_info(
    schema_family: &SchemaFamily,
    data_src: &str,
    parents: &Vec<RelConfigClientInput>,
) -> Result<Vec<RelConfigRus>> {
    let mut results = vec![];
    for (parent_table, parent_vals) in parents {
        let mut parent_info = vec![];
        for parent_val in parent_vals {
            parent_info.push(json_to_fk_by_schema(
                schema_family,
                data_src,
                parent_table,
                parent_val,
            )?);
        }
        results.push((parent_table.clone(), parent_info));
    }
    Ok(results)
}

///
/// Convert the parent (>0)  information from JSON to a Rusqlite equivalent
pub fn get_parent_info_single(
    schema_family: &SchemaFamily,
    data_src: &str,
    parents: &Vec<RelConfigClientInputSingle>,
) -> Result<Vec<RelConfigRusSingle>> {
    let mut parent_info = vec![];
    for (parent_table, parent_val) in parents {
        parent_info.push((
            parent_table.clone(),
            json_to_fk_by_schema(schema_family, data_src, parent_table, parent_val)?,
        ));
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

///
/// Convert the peer information from JSON to a Rusqlite equivalent
pub fn get_peer_pair(
    schema_family: &SchemaFamily,
    peer1: &RelConfigClientInput,
    peer2: &RelConfigClientInput,
) -> Result<(RelConfigRus, RelConfigRus)> {
    let mut peers_rus = [(String::new(), Vec::new()), (String::new(), Vec::new())];
    for (i, (table_name, json_vals)) in [peer1, peer2].iter().enumerate() {
        for json in json_vals {
            let fk = json_to_val_by_schema(
                schema_family,
                table_name,
                schema_family.try_get_schema(table_name)?.pk.as_str(),
                json,
            )?;
            peers_rus[i].0 = table_name.clone();
            peers_rus[i].1.push(fk);
        }
    }
    Ok((peers_rus[0].clone(), peers_rus[1].clone()))
}
