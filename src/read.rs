use crate::{
    input_utils::{fk_name, verify_parenthood},
    rel::peer_matching_clause,
    sql::{get_fk_match_config, in_them},
};

use super::{
    basics::{self, FetchConfig},
    input_utils::verify_pk,
    schema::SchemaFamily,
    sql::{in_them_and, merge_q_configs},
};

use anyhow::{anyhow, Result};
use rusqlite::{types, Connection};

use std::collections::HashMap;

///
/// A record representation returned from the database,
/// key-value pairs of column names and their corresponding values, Rust-friendly
/// For example:
/// A JSON record below
///
/// ```json
/// {
///   "id": 1,
///   "name": "Alice"
/// }
/// ```
///
/// Can be translated into Rust code below:
/// ```
/// use jankenstore::{read::RecordOwned, shift::val::{v_int, v_txt}};
/// use std::collections::HashMap;
///
/// use rusqlite::types;
/// let record: RecordOwned = HashMap::from([("id".to_string(), v_int(1)), ("name".to_string(), v_txt("Alice"))]);
/// ```
pub type RecordOwned = HashMap<String, types::Value>;

///
/// A list of records returned from the database.
/// Each record is a hashmap of column names and their corresponding values.
/// For example:
/// A list of JSON records below
/// ```json
/// [
///    {
///      "id": 1,
///      "name": "Alice"
///   },
///   {
///      "id": 2,
///      "name": "Bob"
///   }
/// ]
/// ```
/// Can be translated into Rust code below:
/// ```
/// use jankenstore::{read::RecordListOwned, shift::val::{v_int, v_txt}};
/// use std::collections::HashMap;
///
/// use rusqlite::types;
/// let list: RecordListOwned = vec![
///    HashMap::from([("id".to_string(), v_int(1)), ("name".to_string(), v_txt("Alice"))]),
///    HashMap::from([("id".to_string(), v_int(2)), ("name".to_string(), v_txt("Bob"))])
/// ];
/// ```
pub type RecordListOwned = Vec<HashMap<String, types::Value>>;

///
/// Read all records
/// from the table.
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family of the database for validation
/// * `table` - the name of the table
/// * `fetch_config_opt` - the configuration for fetching the records
pub fn all(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    fetch_config_opt: Option<FetchConfig>,
) -> Result<RecordListOwned> {
    let schema = schema_family.try_get_schema(table)?;
    let display_cols = fetch_config_opt.unwrap_or_default().display_cols;
    let unknown_col = match display_cols {
        Some(cols) => schema.find_unknown_field(cols),
        None => None,
    };

    if let Some(unknown_col) = unknown_col {
        return Err(anyhow!(
            "Unknown column {} in table {}. \nThe schema is {}",
            unknown_col,
            table,
            schema.json()?
        ));
    }
    basics::read(conn, table, fetch_config_opt)
}

///
/// Read a record from the table by its primary key.
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family of the database for validation
/// * `table` - the name of the table
/// * `pk_values` - the values of the primary key
/// * `fetch_config_opt` - the configuration for fetching the records
pub fn by_pk(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    pk_values: &[types::Value],
    fetch_config_opt: Option<FetchConfig>,
) -> Result<RecordListOwned> {
    let mut fetch_config = fetch_config_opt.unwrap_or_default();
    let schema = schema_family.try_get_schema(table)?;
    verify_pk(schema_family, table, pk_values)?;
    let combined_q_config = in_them_and(&schema.pk, pk_values, fetch_config.where_config)?;
    fetch_config.where_config =
        Some((combined_q_config.0.as_str(), combined_q_config.1.as_slice()));
    all(conn, schema_family, table, Some(fetch_config))
}

///
/// Read the children of a record from the table by the parent's primary key.
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family of the database for validation
/// * `child_table` - the name of the main data source table
/// * `parent_table` - the name of the parent table
///                    linking to the main data source table by a foreign key column
///                    for example: `song` is a child table with `artist_id` indicating
///                    it's a child of `artist` of that `artist_id`,
///                    and `artist_id` has the same value as the primary key of `artist`
/// * `parent_val` - the value of the parent's primary key
/// * `fetch_config_opt` - the configuration for fetching the records
/// # Returns
/// * a list of records of the children
pub fn children_of(
    conn: &Connection,
    schema_family: &SchemaFamily,
    child_table: &str,
    parent_info: &[(&str, &[types::Value])],
    fetch_config_opt: Option<FetchConfig>,
) -> Result<RecordListOwned> {
    schema_family.try_get_schema(child_table)?;
    for (parent_table, parent_vals) in parent_info {
        verify_parenthood(schema_family, child_table, parent_table, parent_vals)?;
    }
    let mut fetch_config = fetch_config_opt.unwrap_or_default();
    let combined_q_config = get_fk_match_config(parent_info, fetch_config.where_config)?;
    all(
        conn,
        schema_family,
        child_table,
        Some({
            fetch_config.where_config =
                Some((combined_q_config.0.as_str(), combined_q_config.1.as_slice()));
            fetch_config
        }),
    )
}

///
/// Read records from the table by its peers' primary keys
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family of the database for validation
/// * `source_table` - the name of the main data source table
/// * `peer_config` - the configuration for fetching the records
/// * `fetch_config_opt` - the configuration for fetching the records
pub fn peers_of(
    conn: &Connection,
    schema_family: &SchemaFamily,
    source_table: &str,
    peer_config: (&str, &[types::Value]),
    fetch_config_opt: Option<FetchConfig>,
) -> Result<RecordListOwned> {
    let (peer_table, peer_pk_vals) = peer_config;
    let fetch_config = fetch_config_opt.unwrap_or_default();
    let rel_table = schema_family.try_get_peer_link_table_of(source_table)?;
    let fk_name = fk_name(peer_table);
    let mut peer_fk_vals_config = in_them(&fk_name, peer_pk_vals);
    let matching_clause = peer_matching_clause(
        rel_table,
        &fk_name,
        (
            source_table,
            schema_family.try_get_schema(source_table)?.pk.as_str(),
        ),
        peer_fk_vals_config.0.as_str(),
    );
    peer_fk_vals_config.0 = matching_clause.clone();
    let combined_config = merge_q_configs(
        Some((
            peer_fk_vals_config.0.as_str(),
            peer_fk_vals_config.1.as_slice(),
        )),
        fetch_config.where_config,
        "AND",
    )?;
    let fetch_config = FetchConfig {
        where_config: Some((combined_config.0.as_str(), combined_config.1.as_slice())),
        ..fetch_config
    };
    all(conn, schema_family, source_table, Some(fetch_config))
}
