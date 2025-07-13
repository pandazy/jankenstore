use std::collections::HashMap;

use super::{
    basics::{total, CountConfig},
    input_utils::{get_fk_name, verify_parenthood},
    peer::peer_matching_clause,
    schema::Schema,
    shift::RecordListOwned,
    sql::get_fk_union_config,
};

use super::{
    basics::{self, FetchConfig},
    input_utils::verify_pk,
    schema::SchemaFamily,
    sql::{in_them_and, merge_q_configs},
};

use anyhow::{anyhow, Result};
use rusqlite::{types, Connection};
use serde_json::json;

///
/// Verify if all columns are defined in the schema
fn verify_cols(schema: &Schema, cols: &[&str]) -> Result<()> {
    let unknown_col = schema.find_unknown_field(cols);
    if let Some(unknown_col) = unknown_col {
        return Err(anyhow!(
            "Unknown column '{}' in table '{}'. \nAvailable columns: {}",
            unknown_col,
            schema.name,
            {
                let mut cols = schema.types.keys().collect::<Vec<_>>();
                cols.sort();
                json!(cols)
            }
        ));
    }
    Ok(())
}

///
/// Read all records
/// from the table.
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family of the database for validation
/// * `table` - the name of the table
/// * `fetch_config_opt` - the configuration for fetching the records
/// * `skip_count` - whether to skip the count, if false, return the total count regardless of the limit and offset
pub fn all(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    fetch_config_opt: Option<FetchConfig>,
    skip_count: bool,
) -> Result<(RecordListOwned, u64)> {
    let schema = schema_family.try_get_schema(table)?;
    let display_cols = fetch_config_opt.and_then(|cfg| cfg.display_cols);

    // only verify columns if group_by is not set
    let group_by = fetch_config_opt
        .unwrap_or_default()
        .group_by
        .unwrap_or_default();
    if group_by.trim().is_empty() {
        verify_cols(schema, display_cols.unwrap_or_default())?;
    }
    basics::read(conn, table, fetch_config_opt, skip_count)
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
    skip_count: bool,
) -> Result<(RecordListOwned, u64)> {
    let where_config = fetch_config_opt.and_then(|cfg| cfg.where_config);
    let schema = schema_family.try_get_schema(table)?;
    verify_pk(schema_family, table, pk_values)?;
    let combined_q_config = in_them_and(&schema.pk, pk_values, where_config);
    let inherited_config = FetchConfig {
        where_config: Some((combined_q_config.0.as_str(), combined_q_config.1.as_slice())),
        ..fetch_config_opt.unwrap_or_default()
    };
    let fetch_opt = Some(inherited_config);
    all(conn, schema_family, table, fetch_opt, skip_count)
}

///
/// Return the number of specified records in the table.
pub fn count(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    count_config_opt: Option<CountConfig>,
) -> Result<i64> {
    let distinct_field = count_config_opt.and_then(|cfg| cfg.distinct_field);
    let where_config = count_config_opt.and_then(|cfg| cfg.where_config);
    let schema = schema_family.try_get_schema(table)?;
    let distinct_field = if let Some(distinct_field) = distinct_field {
        verify_cols(schema, &[distinct_field])?;
        Some(distinct_field)
    } else {
        None
    };
    total(conn, table, distinct_field, where_config)
}

///
/// Read the children of a record from the table by the parent's primary key.
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family of the database for validation
/// * `child_table` - the name of the main data source table
/// * `parent_info` - the parent information`
/// * `fetch_config_opt` - the configuration for fetching the records
/// # Returns
/// * a list of records of the children
pub fn children_of(
    conn: &Connection,
    schema_family: &SchemaFamily,
    child_table: &str,
    parent_info: &HashMap<String, Vec<types::Value>>,
    fetch_config_opt: Option<FetchConfig>,
    skip_count: bool,
) -> Result<(RecordListOwned, u64)> {
    schema_family.try_get_schema(child_table)?;
    for (parent_table, parent_vals) in parent_info {
        verify_parenthood(schema_family, child_table, parent_table, parent_vals)?;
    }
    let where_config = fetch_config_opt.and_then(|cfg| cfg.where_config);
    let combined_q_config = get_fk_union_config(schema_family, parent_info, where_config)?;
    let updated_fetch_config = FetchConfig {
        where_config: Some((combined_q_config.0.as_str(), combined_q_config.1.as_slice())),
        ..fetch_config_opt.unwrap_or_default()
    };
    let fetch_opt = Some(updated_fetch_config);
    all(conn, schema_family, child_table, fetch_opt, skip_count)
}

fn verify_peers(schema_family: &SchemaFamily, peer_tables: &[&str]) -> Result<()> {
    for peer_table in peer_tables {
        schema_family.try_get_schema(peer_table)?;
        schema_family.try_get_peer_link_table_of(peer_table)?;
    }
    Ok(())
}

///
/// Read records from the table by its peers' primary keys
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family of the database for validation
/// * `source_table` - the name of the main data source table
/// * `peer_config` - the configuration for fetching the records
/// * `fetch_config_opt` - the configuration for fetching the records
/// * `skip_count` - whether to skip the count, if false, return the total count regardless of the limit and offset
pub fn peers_of(
    conn: &Connection,
    schema_family: &SchemaFamily,
    source_table: &str,
    peer_config: &HashMap<String, Vec<types::Value>>,
    fetch_config_opt: Option<FetchConfig>,
    skip_count: bool,
) -> anyhow::Result<(RecordListOwned, u64)> {
    let where_config = fetch_config_opt.and_then(|cfg| cfg.where_config);
    let rel_table = schema_family.try_get_peer_link_table_of(source_table)?;
    verify_peers(
        schema_family,
        &peer_config.keys().map(|t| t.as_str())
            .collect::<Vec<_>>(),
    )?;
    let mut fk_union_config = get_fk_union_config(schema_family, peer_config, where_config)?;
    let source_fk_name = get_fk_name(source_table, schema_family)?;
    let matching_clause = peer_matching_clause(
        rel_table,
        &source_fk_name,
        (
            source_table,
            schema_family.try_get_schema(source_table)?.pk.as_str(),
        ),
        fk_union_config.0.as_str(),
    );
    fk_union_config.0 = matching_clause.clone();
    let combined_config = merge_q_configs(
        Some((fk_union_config.0.as_str(), fk_union_config.1.as_slice())),
        where_config,
        "AND",
    );
    let fetch_config = FetchConfig {
        where_config: Some((combined_config.0.as_str(), combined_config.1.as_slice())),
        ..fetch_config_opt.unwrap_or_default()
    };
    let fetch_opt = Some(fetch_config);
    all(conn, schema_family, source_table, fetch_opt, skip_count)
}

#[cfg(test)]
mod tests {
    use crate::sqlite::{basics::FetchConfig, read, schema::fetch_schema_family};

    use anyhow::Result;
    use rusqlite::Connection;

    #[test]
    fn test_read_edge_cases() -> Result<()> {
        let conn = Connection::open_in_memory()?;

        conn.execute_batch(
            r#"
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                memo TEXT DEFAULT ''
            );
            INSERT INTO users (name, memo) VALUES ('Alice', 'big');
            INSERT INTO users (name, memo) VALUES ('Alice', 'little');
            "#,
        )?;

        let schema_family = fetch_schema_family(&conn, &[], "", "")?;
        let (records, total) = read::all(&conn, &schema_family, "users", None, false)?;
        assert_eq!(records.len(), 2);
        assert_eq!(total, 2);

        let (records, total) = read::all(
            &conn,
            &schema_family,
            "users",
            Some(FetchConfig {
                display_cols: Some(&["name"]),
                is_distinct: true,
                where_config: None,
                group_by: None,
                order_by: None,
                limit: None,
                offset: None,
            }),
            true,
        )?;
        assert_eq!(records.len(), 1);
        assert_eq!(total, 1);
        Ok(())
    }
}
