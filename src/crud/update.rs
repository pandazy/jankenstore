use super::{
    sql,
    verify::{get_verified_write_inputs, verify_table_name, verify_values_required, VerifyConfig},
};

use rusqlite::{params_from_iter, types, Connection};

use std::collections::HashMap;

/// update all matching records in the table
/// # Arguments
///
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `input` - the new values for the record
/// * `where_q_config` - the where clause and the parameters for the where clause,
///                      to reduce the chance of unwanted updates,
///                      this is not an Option and cannot contain empty clause
/// * `verification_options` - the options for verification, if None, no verification is performed
pub fn u_all(
    conn: &Connection,
    table_name: &str,
    input: &HashMap<String, types::Value>,
    where_q_config: (&str, &[types::Value]),
    verification_options: Option<VerifyConfig>,
) -> anyhow::Result<()> {
    verify_table_name(table_name)?;
    let input = get_verified_write_inputs(false, table_name, input, verification_options)?;
    let mut set_clause = vec![];
    let mut set_params = vec![];
    for (key, value) in input {
        set_clause.push(format!("{} = ?", key));
        set_params.push(value.clone());
    }
    let (where_clause, where_params) = sql::standardize_q_config(Some(where_q_config), "WHERE")?;
    let params = [set_params, where_params].concat();
    let sql = format!(
        "UPDATE {} SET {} {}",
        table_name,
        set_clause.join(", "),
        where_clause,
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&params))?;
    Ok(())
}

///
/// update an existing record in the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `defaults` - the default values for the table
/// * `required_fields` - the required fields for the table
/// * `pk_name` - the name of the primary key
/// * `pk_values` - records to be updated represented by their primary key values
/// * `input` - the new values for the record
/// * `where_q_config` - the where clause and the parameters for the where clause
/// # Returns
/// * `Ok(())` - if the record is updated successfully
pub fn u_by_pk(
    conn: &Connection,
    table_name: &str,
    pk_name: &str,
    pk_values: &[types::Value],
    input: &HashMap<String, types::Value>,
    where_q_config: Option<(&str, &[types::Value])>,
    verification_options: Option<VerifyConfig>,
) -> anyhow::Result<()> {
    verify_values_required(pk_values, table_name, pk_name)?;
    let (pk_where_clause, pk_where_params) = sql::in_them(pk_name, pk_values);
    let pk_where_refs = (pk_where_clause.as_str(), pk_where_params.as_slice());
    let where_q_config = sql::merge_q_configs(Some(pk_where_refs), where_q_config, "AND")?;
    u_all(
        conn,
        table_name,
        input,
        (where_q_config.0.as_str(), &where_q_config.1),
        verification_options,
    )?;
    Ok(())
}
