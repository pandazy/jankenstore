use super::{
    get_parent_info, get_peer_info,
    payload::{ParentHood, ParsableOp, PeerHood, ReadSrc, SrcAndKeys},
};
use crate::sqlite::{
    basics::FetchConfig,
    input_utils::json_to_pk_val_by_schema,
    read::{self},
    schema::SchemaFamily,
    shift::{json_to_val, list_to_json, JsonListOwned},
    sql::merge_q_configs,
};

use anyhow::{anyhow, Ok, Result};
use rusqlite::{types, Connection};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchConfig {
    pub table: String,
    pub col: String,
    pub keyword: String,
    pub exact: Option<bool>,
}

///
/// Providing generic read operations using JSON-compatible parameters
#[derive(Debug, Serialize, Deserialize)]
pub enum ReadOp {
    ///
    /// Read all records in a table
    All(String),

    ///
    /// Read records in a table by their primary keys
    /// # Arguments
    /// * `SrcAndKeys` - The primary key values of the records to read from the specified table
    ByPk(SrcAndKeys),

    ///
    /// Read all records in a table that are children of specified parent records in another table
    /// # Arguments
    /// * `ParentHood` - The table where the records will be read corresponding to the parent records
    Children(ParentHood),

    ///
    /// Read all records in a table that are peers of specified records in another table
    /// # Arguments
    /// * `PeerHood` - The table where the records will be read corresponding to the peer records
    Peers(PeerHood),

    ///
    /// Search records in a table by a keyword in a text column
    Search(SearchConfig),
}

impl ReadOp {
    ///
    /// Execute the operation on the database
    /// # Arguments
    /// * `conn` - A connection to the database
    /// * `schema_family` - The schema family of the database
    /// * `fetch_opt` - The configuration for fetching the records
    pub fn run(
        &self,
        conn: &Connection,
        schema_family: &SchemaFamily,
        fetch_opt: Option<FetchConfig>,
    ) -> Result<(JsonListOwned, u64)> {
        let get_pk_vals = |data_src, pk_vals: &[JsonValue]| -> Result<Vec<types::Value>> {
            let mut results = vec![];
            for pk_val in pk_vals {
                results.push(json_to_pk_val_by_schema(schema_family, data_src, pk_val)?);
            }
            Ok(results)
        };
        let results = match self {
            Self::All(table) => read::all(conn, schema_family, table, fetch_opt, false),
            Self::ByPk(SrcAndKeys { src, keys }) => {
                let pk_vals = get_pk_vals(src, keys)?;
                read::by_pk(conn, schema_family, src, &pk_vals, fetch_opt, false)
            }
            Self::Children(ParentHood { src, parents }) => {
                let parent_info = get_parent_info(schema_family, src, parents)?;
                read::children_of(conn, schema_family, src, &parent_info, fetch_opt, false)
            }
            Self::Peers(PeerHood { src, peers }) => {
                let peer_info = get_peer_info(schema_family, peers)?;
                read::peers_of(conn, schema_family, src, &peer_info, fetch_opt, false)
            }
            Self::Search(SearchConfig {
                table,
                col,
                keyword,
                exact,
            }) => {
                let schema = schema_family.try_get_schema(table)?;
                let col_type = schema.types.get(col).unwrap_or(&types::Type::Null);
                if !col_type.eq(&types::Type::Text) {
                    return Err(anyhow!(
                        "The column '{}'@'{}' is not a text column, but it's specified as a search keyword",
                        col,
                        table
                    ));
                }
                let exact = exact.unwrap_or(false);
                let col_type = schema.types.get(col).unwrap_or(&types::Type::Null);
                let sql_col_val = json_to_val(col_type, &json!(keyword))?;
                let search_params = vec![sql_col_val];
                let search_config = if exact {
                    (format!("{col} = ?"), &search_params)
                } else {
                    (format!("{col} like '%'||?||'%'"), &search_params)
                };
                let where_config = fetch_opt.and_then(|cfg| cfg.where_config);
                let combined_config = merge_q_configs(
                    Some((search_config.0.as_str(), search_config.1)),
                    where_config,
                    "AND",
                );
                read::all(
                    conn,
                    schema_family,
                    table,
                    {
                        let mut fetch_config = fetch_opt.unwrap_or_default();
                        fetch_config.where_config =
                            Some((combined_config.0.as_str(), combined_config.1.as_slice()));
                        Some(fetch_config)
                    },
                    false,
                )
            }
        }?;
        Ok((list_to_json(&results.0)?, results.1))
    }
}

impl ParsableOp<'_> for ReadOp {}
impl ReadSrc for ReadOp {
    fn src(&self) -> &str {
        match self {
            Self::All(table) => table,
            Self::ByPk(SrcAndKeys { src, .. }) => src,
            Self::Children(ParentHood { src, .. }) => src,
            Self::Peers(PeerHood { src, .. }) => src,
            Self::Search(search_config, ..) => search_config.table.as_str(),
        }
    }
}
