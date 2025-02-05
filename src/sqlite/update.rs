use super::{input_utils::verify_parenthood, shift::val_to_json, sql::get_fk_union_config};

use super::{
    basics::update,
    input_utils::{self, VerifyConf},
    schema::SchemaFamily,
    sql::{in_them_and, WhereConfig},
};

use anyhow::{anyhow, Result};
use rusqlite::{types, Connection};

use std::collections::HashMap;

///
/// Update all records in a table that match the given condition.
/// # Arguments
/// * `conn` - A connection to the database
/// * `schema_family` - The schema family of the database
/// * `table` - The name of the table
/// * `input` - The new values to update
/// * `where_config` - The condition to match the records to update
/// * `default_if_absent` - Whether to use the default value if a field is absent or empty
pub fn update_all(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    input: &HashMap<String, types::Value>,
    where_config: WhereConfig,
    default_if_absent: bool,
) -> Result<()> {
    let schema = schema_family.try_get_schema(table)?;
    input_utils::get_verified_input(
        schema_family,
        table,
        input,
        VerifyConf {
            default_if_absent,
            must_have_every_col: false,
        },
    )?;
    if input.contains_key(&schema.pk) {
        return Err(anyhow!(
            "'{}' cannot be updated. It's \"{}\"'s primary key. The attempted update was {}",
            schema.pk,
            table,
            val_to_json(input)?
        ));
    }
    update(conn, table, input, where_config)
}

///
/// Update a record in a table by its primary key.
/// # Arguments
/// * `conn` - A connection to the database
/// * `schema_family` - The schema family of the database
/// * `table` - The name of the table
/// * `input` - The new values to update
/// * `pk_values` - The primary key values of the record to update
/// * `where_config` - The condition to match the record to update
/// * `default_if_absent` - Whether to use the default value if a field is absent or empty
pub fn update_by_pk(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    input: &HashMap<String, types::Value>,
    pk_values: &[types::Value],
    where_config: Option<WhereConfig>,
    default_if_absent: bool,
) -> anyhow::Result<()> {
    let schema = schema_family.try_get_schema(table)?;
    let combined_q_config = in_them_and(&schema.pk, pk_values, where_config);
    update_all(
        conn,
        schema_family,
        table,
        input,
        (combined_q_config.0.as_str(), combined_q_config.1.as_slice()),
        default_if_absent,
    )
}

///
/// Update all records in a table that are children of specified parent records in another table.
/// # Arguments
/// * `conn` - A connection to the database
/// * `schema_family` - The schema family of the database
/// * `child_table` - The name of the table to update
/// * `parent_table` - The name of the parent table
/// * `parent_vals` - The specified parent records' primary key values
/// * `input` - The new values to update (can be just part of the whole record)
/// * `where_config_opt` - The condition to match the records to update
pub fn update_children_of(
    conn: &Connection,
    schema_family: &SchemaFamily,
    child_table: &str,
    parent_info: &HashMap<String, Vec<types::Value>>,
    input: &HashMap<String, types::Value>,
    where_config_opt: Option<WhereConfig>,
    default_if_absent: bool,
) -> anyhow::Result<()> {
    for (parent_table, parent_vals) in parent_info {
        verify_parenthood(schema_family, child_table, parent_table, parent_vals)?;
    }
    let combined_q_config = get_fk_union_config(parent_info, where_config_opt);
    update_all(
        conn,
        schema_family,
        child_table,
        input,
        (combined_q_config.0.as_str(), combined_q_config.1.as_slice()),
        default_if_absent,
    )
}
