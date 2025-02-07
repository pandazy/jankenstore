use super::input_utils::{get_fk_name, verify_parenthood};

use super::{
    basics,
    input_utils::{self, VerifyConf},
    schema::SchemaFamily,
};

use rusqlite::{types, Connection};

use std::collections::HashMap;

///
/// Create a new record in a table.
/// # Arguments
/// * `conn` - A connection to the database
/// * `schema_family` - The schema family of the database
/// * `table` - The name of the table
/// * `input` - The new values to insert
/// * `default_if_absent` - Whether to use the default value if a field is absent or empty
pub fn create(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    input: &HashMap<String, types::Value>,
    default_if_absent: bool,
) -> anyhow::Result<()> {
    let verified_input = input_utils::get_verified_input(
        schema_family,
        table,
        input,
        VerifyConf {
            default_if_absent,
            must_have_every_col: true,
        },
    )?;
    basics::insert(conn, table, &verified_input)
}

///
/// Create a new record in a table that is a child of another table.
/// # Arguments
/// * `conn` - A connection to the database
/// * `schema_family` - The schema family of the database
/// * `child_table` - The name of the table
/// * `(parent_table, parent_val)` - Parent table information
///                                  - `parent_table` - The name of the parent table
///                                  - `parent_val` - The value of the parent record's primary key
/// * `input` - The new values to insert
/// * `default_if_absent` - Whether to use the default value if a field is absent or empty
pub fn create_child_of(
    conn: &Connection,
    schema_family: &SchemaFamily,
    child_table: &str,
    parent_info: &HashMap<String, types::Value>,
    input: &HashMap<String, types::Value>,
    default_if_absent: bool,
) -> anyhow::Result<()> {
    let mut updated_input: HashMap<String, types::Value> = input.clone();
    for (parent_table, parent_val) in parent_info {
        let parent_val = parent_val.to_owned();
        verify_parenthood(
            schema_family,
            child_table,
            parent_table,
            &[parent_val.clone()],
        )?;
        updated_input.insert(get_fk_name(parent_table, schema_family)?, parent_val);
    }
    create(
        conn,
        schema_family,
        child_table,
        &updated_input,
        default_if_absent,
    )
}
