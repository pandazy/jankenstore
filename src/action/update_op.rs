use super::{
    get_parent_info, get_pk_vals,
    payload::{ParentHood, ParsableOp, SrcAndKeys},
};
use crate::sqlite::{
    input_utils::json_to_val_map_by_schema, schema::SchemaFamily, shift::RecordOwned, update,
};

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

///
/// Providing generic update operations using JSON-compatible parameters
#[derive(Debug, Serialize, Deserialize)]
pub enum UpdateOp {
    ///
    /// Update record in a table by their primary keys
    /// # Arguments
    /// * `SrcAndValues` - The primary key values of the record to update from the specified table
    /// * `JsonValue` - The payload for updating the specified records
    Update(SrcAndKeys, JsonValue),

    ///
    /// Update all records in a table that are children of specified parent records in another table
    /// # Arguments
    /// * `ParentHood` - The table where the records will be updated and the parent table and the parent record's primary key values
    /// * `JsonValue` - The payload for updating the specified records
    UpdateChildren(ParentHood, JsonValue),
}

impl UpdateOp {
    ///
    /// Execute the operation on the database
    /// # Arguments
    /// * `conn` - A connection to the database
    /// * `schema_family` - The schema family of the database
    pub fn run(&self, conn: &Connection, schema_family: &SchemaFamily) -> Result<()> {
        let get_payload_map = |data_src: &str, payload| -> Result<RecordOwned> {
            json_to_val_map_by_schema(schema_family, data_src, payload)
        };
        match self {
            Self::Update(SrcAndKeys { src, keys }, payload) => {
                let keys = get_pk_vals(schema_family, src, keys)?;
                let payload = get_payload_map(src, payload)?;
                update::update_by_pk(conn, schema_family, src, &payload, &keys, None, true)?;
            }
            Self::UpdateChildren(ParentHood { src, parents }, payload) => {
                let parents = get_parent_info(schema_family, src, parents)?;
                let payload = get_payload_map(src, payload)?;
                update::update_children_of(
                    conn,
                    schema_family,
                    src,
                    &parents,
                    &payload,
                    None,
                    true,
                )?;
            }
        }
        Ok(())
    }

    ///
    /// Execute the operation on the database with a map function
    pub fn run_map<T>(
        &self,
        conn: &Connection,
        schema_family: &SchemaFamily,
        map_input: T,
    ) -> Result<()>
    where
        T: FnOnce(&RecordOwned) -> RecordOwned,
    {
        let get_payload_map = |data_src: &str, payload| -> Result<RecordOwned> {
            let fresh_map = json_to_val_map_by_schema(schema_family, data_src, payload);
            fresh_map.map(|input| map_input(&input))
        };
        match self {
            Self::Update(SrcAndKeys { src, keys }, payload) => {
                update::update_by_pk(
                    conn,
                    schema_family,
                    src,
                    &get_payload_map(src, payload)?,
                    get_pk_vals(schema_family, src, keys)?.as_slice(),
                    None,
                    true,
                )?;
            }
            Self::UpdateChildren(ParentHood { src, parents }, payload) => {
                update::update_children_of(
                    conn,
                    schema_family,
                    src,
                    &get_parent_info(schema_family, src, parents)?,
                    &get_payload_map(src, payload)?,
                    None,
                    true,
                )?;
            }
        }
        Ok(())
    }
}

impl ParsableOp<'_> for UpdateOp {}
