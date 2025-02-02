use super::utils::{get_parent_info, get_pk_vals, RelConfigClientInput};
use crate::{delete, schema::SchemaFamily, sql::WhereConfig};

use anyhow::Result;
use rusqlite::{types, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

///
/// The utility set of write actions that can be performed on the database
#[derive(Debug, Serialize, Deserialize)]
pub enum DelOp {
    ///
    /// Delete records in a table by their primary keys
    /// # Arguments
    /// * `String` - The name of the table where the records will be deleted
    /// * `Vec<JsonValue>` - The primary key values of the records to delete
    Delete(String, Vec<JsonValue>),

    /// Delete all records in a table that are children of specified parent records in another table
    /// # Arguments
    /// * `String` - The name of the table where the records will be deleted
    /// * `Vec<RelConfigClientInput>` - The parent table and the parent record's primary key values
    DeleteChildren(String, Vec<RelConfigClientInput>),
}

impl DelOp {
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
        where_config: Option<WhereConfig>,
    ) -> Result<()> {
        match self {
            Self::Delete(data_src, pk_vals) => {
                delete::delete(
                    conn,
                    schema_family,
                    data_src,
                    get_pk_vals(schema_family, data_src, pk_vals)?.as_slice(),
                    where_config,
                )?;
            }
            Self::DeleteChildren(data_src, parents) => {
                delete::delete_children_of(
                    conn,
                    schema_family,
                    data_src,
                    &get_parent_info(schema_family, data_src, parents)?
                        .iter()
                        .map(|(t, v)| (t.as_str(), v.as_slice()))
                        .collect::<Vec<(&str, &[types::Value])>>(),
                    None,
                )?;
            }
        }
        Ok(())
    }
}
