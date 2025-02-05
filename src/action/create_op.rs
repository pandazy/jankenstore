use super::{get_one_on_one_parent_info, payload::OneOnOneParentBond};
use crate::sqlite::{
    add, input_utils::json_to_val_map_by_schema, schema::SchemaFamily, shift::RecordOwned,
};

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

///
/// Providing generic create operations using JSON-compatible parameters
#[derive(Debug, Serialize, Deserialize)]
pub enum CreateOp {
    ///
    /// Create a record in a table
    /// # Arguments
    /// * `String` - The name of the table where the record will be created
    /// * `JsonValue` - The payload for creating the record, matching the schema of the table
    Create(String, JsonValue),

    ///
    /// Create a record in a table that is a child of another table(s).
    /// To avoid ambiguity, only one child is allowed to be created at a time
    /// But this child can have multiple types of parents (each type only has one parent)
    /// # Arguments
    /// * `OneOnOneParentBond` - The relationship between the child and the parent(s)
    /// * `JsonValue` - The payload for creating the child record, matching the schema of the child table
    CreateChild(OneOnOneParentBond, JsonValue),
}

impl CreateOp {
    /// Execute the operation on the database
    pub fn with_schema(&self, conn: &Connection, schema_family: &SchemaFamily) -> Result<()> {
        let get_payload_map = |data_src: &str, payload| -> Result<RecordOwned> {
            json_to_val_map_by_schema(schema_family, data_src, payload)
        };
        match self {
            Self::Create(data_src, payload) => {
                add::create(
                    conn,
                    schema_family,
                    data_src,
                    &get_payload_map(data_src, payload)?,
                    true,
                )?;
            }
            Self::CreateChild(OneOnOneParentBond { src, parents }, payload) => {
                let parent_info = get_one_on_one_parent_info(schema_family, src, parents)?;
                add::create_child_of(
                    conn,
                    schema_family,
                    src,
                    &parent_info,
                    &get_payload_map(src, payload)?,
                    true,
                )?;
            }
        }
        Ok(())
    }
}
