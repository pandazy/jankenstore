use crate::{
    add, delete,
    input_utils::{
        json_to_fk_by_schema, json_to_pk_val_by_schema, json_to_val_by_schema,
        json_to_val_map_by_schema,
    },
    read::RecordOwned,
    rel::{link, unlink},
    schema::SchemaFamily,
    update,
};

use anyhow::Result;
use rusqlite::{types, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Serialize, Deserialize)]
pub enum WriterOp {
    Create(String),
    CreateChild(String, Vec<RelConfigClientInput>),
    Update(String, Vec<JsonValue>),
    UpdateChild(String, Vec<RelConfigClientInput>),
    Delete(String, Vec<JsonValue>),
    DeleteChild(String, Vec<RelConfigClientInput>),
    RelLink(RelConfigClientInput, RelConfigClientInput),
    RelUnlink(RelConfigClientInput, RelConfigClientInput),
}

pub type RelConfigClientInput = (String, Vec<JsonValue>);

pub type RelConfigRus = (String, Vec<types::Value>);

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteCommandMap(pub WriterOp);

impl WriteCommandMap {
    pub fn write_with_schema(
        &self,
        conn: &Connection,
        schema_family: &SchemaFamily,
        payload: &JsonValue,
    ) -> Result<()> {
        let WriteCommandMap(op) = self;
        let get_payload_map = |data_src: &str| -> Result<RecordOwned> {
            json_to_val_map_by_schema(schema_family, data_src, payload)
        };
        let get_parent_info =
            |data_src, parents: &Vec<RelConfigClientInput>| -> Result<Vec<RelConfigRus>> {
                let mut results = vec![];
                for (parent_table, parent_vals) in parents {
                    if parent_table.trim().is_empty() {
                        return Err(anyhow::anyhow!(
                            "Parent table not specified. CommandMap: {:?}",
                            self
                        ));
                    }
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
            };
        let get_pk_vals = |data_src, pk_vals: &[JsonValue]| -> Result<Vec<types::Value>> {
            let mut results = vec![];
            for pk_val in pk_vals {
                if pk_val.is_null() {
                    return Err(anyhow::anyhow!(
                        "Primary key value cannot be null. CommandMap: {:?}",
                        self
                    ));
                }
                results.push(json_to_pk_val_by_schema(schema_family, data_src, pk_val)?);
            }
            Ok(results)
        };
        let get_peer_pair = |peer1: &RelConfigClientInput,
                             peer2: &RelConfigClientInput|
         -> Result<(RelConfigRus, RelConfigRus)> {
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
        };
        match op {
            WriterOp::Create(data_src) => {
                add::create(
                    conn,
                    schema_family,
                    data_src,
                    &get_payload_map(data_src)?,
                    true,
                )?;
            }
            WriterOp::CreateChild(data_src, parents) => {
                let parent_info = get_parent_info(data_src, parents)?;
                let mut parents = vec![];
                for (parent_table, parent_fk_vals) in parent_info {
                    if parent_fk_vals.len() > 1 {
                        return Err(anyhow::anyhow!(
                            "Creating children for multiple parents is not supported. CommandMap: {:?}",
                            self
                        ));
                    }
                    parents.push((parent_table, parent_fk_vals[0].clone()));
                }
                add::create_child_of(
                    conn,
                    schema_family,
                    data_src,
                    &parents
                        .iter()
                        .map(|(t, v)| (t.as_str(), v.clone()))
                        .collect::<Vec<(&str, types::Value)>>(),
                    &get_payload_map(data_src)?,
                    true,
                )?;
            }
            WriterOp::Update(data_src, pk_vals) => {
                update::update_by_pk(
                    conn,
                    schema_family,
                    data_src,
                    &get_payload_map(data_src)?,
                    get_pk_vals(data_src, pk_vals)?.as_slice(),
                    None,
                    true,
                )?;
            }
            WriterOp::UpdateChild(data_src, parents) => {
                update::update_children_of(
                    conn,
                    schema_family,
                    data_src,
                    &get_parent_info(data_src, parents)?
                        .iter()
                        .map(|(t, v)| (t.as_str(), v.as_slice()))
                        .collect::<Vec<(&str, &[types::Value])>>(),
                    &get_payload_map(data_src)?,
                    None,
                    true,
                )?;
            }
            WriterOp::Delete(data_src, pk_vals) => {
                delete::delete(
                    conn,
                    schema_family,
                    data_src,
                    get_pk_vals(data_src, pk_vals)?.as_slice(),
                    None,
                )?;
            }
            WriterOp::DeleteChild(data_src, parents) => {
                delete::delete_children_of(
                    conn,
                    schema_family,
                    data_src,
                    &get_parent_info(data_src, parents)?
                        .iter()
                        .map(|(t, v)| (t.as_str(), v.as_slice()))
                        .collect::<Vec<(&str, &[types::Value])>>(),
                    None,
                )?;
            }
            WriterOp::RelLink(peer1_input, peer2_input) => {
                let pair = get_peer_pair(peer1_input, peer2_input)?;
                let (peer1, peer2) = pair;
                link(
                    conn,
                    schema_family,
                    (peer1.0.as_str(), peer1.1.as_slice()),
                    (peer2.0.as_str(), peer2.1.as_slice()),
                )?;
            }
            WriterOp::RelUnlink(peer1_input, peer2_input) => {
                let pair = get_peer_pair(peer1_input, peer2_input)?;
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

pub enum ReaderOp {
    Read(String, Vec<JsonValue>),
    ReadChild(String, Vec<RelConfigClientInput>),
    ReadPeer(String, Vec<RelConfigClientInput>),
}

pub struct ReadCommandMap(pub ReaderOp);
