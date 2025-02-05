use super::{
    payload::{ParentHood, SrcAndKeys},
    utils::{get_parent_info, get_pk_vals},
};
use crate::sqlite::{delete, schema::SchemaFamily, sql::WhereConfig};

use anyhow::Result;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

///
/// Providing generic delete operations using JSON-compatible parameters
#[derive(Debug, Serialize, Deserialize)]
pub enum DelOp {
    ///
    /// Delete records in a table by their primary keys
    /// # Arguments
    /// * `SrcAndKeys` - The primary key values of the records to delete from the specified table
    ///                    - `src`: the table where the records will be deleted
    ///                    - `keys`: the primary key values of the records to delete
    Delete(SrcAndKeys),

    ///
    /// Delete all records in a table that are children of specified parent records in another table
    /// # Arguments
    /// * `ParentHood` - The table where the records will be deleted corresponding to the parent records
    DeleteChildren(ParentHood),
}

impl DelOp {
    ///
    /// Execute the operation on the database
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
            Self::Delete(SrcAndKeys { src, keys }) => {
                delete::delete(
                    conn,
                    schema_family,
                    src,
                    get_pk_vals(schema_family, src, keys)?.as_slice(),
                    where_config,
                )?;
            }
            Self::DeleteChildren(ParentHood { src, parents }) => {
                delete::delete_children_of(
                    conn,
                    schema_family,
                    src,
                    &get_parent_info(schema_family, src, parents)?,
                    None,
                )?;
            }
        }
        Ok(())
    }
}
