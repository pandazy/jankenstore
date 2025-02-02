use super::{
    get_parent_info, get_parent_info_single, get_pk_vals, RelConfigClientInput,
    RelConfigClientInputSingle,
};
use crate::{
    add, input_utils::json_to_val_map_by_schema, schema::SchemaFamily, shift::RecordOwned, update,
};

use anyhow::Result;
use rusqlite::{types, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

///
/// The utility set of write actions that can be performed on the database
#[derive(Debug, Serialize, Deserialize)]
pub enum ModifyOp {
    /// Create a record in a table
    /// # Arguments
    /// * `String` - The name of the table where the record will be created
    Create(String),

    /// Create a record in a table that is a child of another table(s).
    /// To avoid ambiguity, only one child is allowed to be created at a time
    /// But this child can have multiple types of parents (each type only has one parent)
    /// # Arguments
    /// * `String` - The name of the table where the record will be created
    /// * `Vec<RelConfigClientInputSingle>` - The parent table and the parent record's primary key values
    CreateChild(String, Vec<RelConfigClientInputSingle>),

    /// Update record in a table by their primary keys
    /// # Arguments
    /// * `String` - The name of the table where the records will be updated
    /// * `Vec<JsonValue>` - The primary key values of the records to update
    Update(String, Vec<JsonValue>),

    ///
    /// Update all records in a table that are children of specified parent records in another table
    /// # Arguments
    /// * `String` - The name of the table where the records will be updated
    /// * `Vec<RelConfigClientInput>` - The parent table and the parent record's primary key values
    UpdateChildren(String, Vec<RelConfigClientInput>),
}

impl ModifyOp {
    ///
    /// Execute the write operation on the database
    /// # Arguments
    /// * `conn` - A connection to the database
    /// * `schema_family` - The schema family of the database
    /// * `payload` - The data to write
    ///
    pub fn with_schema(
        &self,
        conn: &Connection,
        schema_family: &SchemaFamily,
        payload: &JsonValue,
    ) -> Result<()> {
        let get_payload_map = |data_src: &str| -> Result<RecordOwned> {
            json_to_val_map_by_schema(schema_family, data_src, payload)
        };
        match self {
            Self::Create(data_src) => {
                add::create(
                    conn,
                    schema_family,
                    data_src,
                    &get_payload_map(data_src)?,
                    true,
                )?;
            }
            Self::CreateChild(data_src, parent) => {
                let parent_info = get_parent_info_single(schema_family, data_src, parent)?;
                add::create_child_of(
                    conn,
                    schema_family,
                    data_src,
                    &parent_info
                        .iter()
                        .map(|(t, v)| (t.as_str(), v.clone()))
                        .collect::<Vec<(&str, types::Value)>>(),
                    &get_payload_map(data_src)?,
                    true,
                )?;
            }
            Self::Update(data_src, pk_vals) => {
                update::update_by_pk(
                    conn,
                    schema_family,
                    data_src,
                    &get_payload_map(data_src)?,
                    get_pk_vals(schema_family, data_src, pk_vals)?.as_slice(),
                    None,
                    true,
                )?;
            }
            Self::UpdateChildren(data_src, parents) => {
                update::update_children_of(
                    conn,
                    schema_family,
                    data_src,
                    &get_parent_info(schema_family, data_src, parents)?
                        .iter()
                        .map(|(t, v)| (t.as_str(), v.as_slice()))
                        .collect::<Vec<(&str, &[types::Value])>>(),
                    &get_payload_map(data_src)?,
                    None,
                    true,
                )?;
            }
        }
        Ok(())
    }
}
