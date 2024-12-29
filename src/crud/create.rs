use std::collections::{HashMap, HashSet};

use rusqlite::{params_from_iter, types, Connection};

use super::verify::{get_verified_insert_inputs, verify_table_name};

///
/// insert a new record into the table
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table to insert into
/// * `input` - the new record to be inserted
/// * `verification_options` - the options for verification, if None, no verification is performed
pub fn i_one(
    conn: &Connection,
    table_name: &str,
    input: &HashMap<String, types::Value>,
    verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
) -> anyhow::Result<()> {
    verify_table_name(table_name)?;
    let verified_input = get_verified_insert_inputs(table_name, input, verification_options)?;
    let mut params = vec![];
    let mut columns = vec![];
    let mut values = vec![];
    for (key, value) in verified_input {
        columns.push(key);
        values.push("?");
        params.push(value);
    }

    let column_expression = columns.join(", ");
    let value_expression = values.join(", ");
    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name, column_expression, value_expression
    );

    conn.execute(&sql, params_from_iter(&params))?;
    Ok(())
}
