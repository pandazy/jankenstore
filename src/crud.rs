use anyhow::Result;
use rusqlite::{params_from_iter, types, Connection};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};

use crate::{
    convert::{self, row_to_map, standardize_where_items},
    verify::{verify_required_fields_for_write_ops, verify_table_name},
};

///
/// fetch one record from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `pk` - the primary key and its value, represented as a tuple (pk_name, pk_value)
/// * `where_input` - the where clause and the parameters for the where clause
pub fn fetch_one(
    conn: &Connection,
    table_name: &str,
    (pk_name, pk_value): (&str, &str),
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Option<HashMap<String, types::Value>>> {
    verify_table_name(table_name)?;
    let sql = format!("SELECT * FROM {} WHERE {} = ?", table_name, pk_name);
    let (where_clause, where_params) = standardize_where_items(where_input, "AND")?;
    let params = [vec![types::Value::Text(pk_value.to_string())], where_params].concat();
    let sql = format!("{} {}", sql, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&params))?;
    let row_op = rows.next()?;
    match row_op {
        Some(row) => {
            let row_record = row_to_map(row)?;
            Ok(Some(row_record))
        }
        None => Ok(None),
    }
}

pub fn fetch_one_as<T: DeserializeOwned>(
    conn: &Connection,
    table_name: &str,
    (pk_name, pk_value): (&str, &str),
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Option<T>> {
    let row = fetch_one(conn, table_name, (pk_name, pk_value), where_input)?;
    match row {
        Some(row) => Ok(Some(serde_json::from_value(convert::val_to_json(&row)?)?)),
        None => Ok(None),
    }
}

///
/// fetch all matching records from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
/// * `display_fields` - the fields to be displayed in the result
/// * `where_input` - the where clause and the parameters for the where clause
pub fn fetch_all(
    conn: &Connection,
    table_name: &str,
    is_distinct: bool,
    display_fields: Option<&[&str]>,
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Vec<HashMap<String, types::Value>>> {
    verify_table_name(table_name)?;
    let default_fields = vec!["*"];
    let display_fields = display_fields.unwrap_or_else(|| &default_fields);
    let distinct_word = if is_distinct { "DISTINCT" } else { "" };
    let sql = format!(
        "SELECT {} {} FROM {}",
        distinct_word,
        display_fields.join(", "),
        table_name
    );
    let (where_clause, where_params) = standardize_where_items(where_input, "WHERE")?;
    let sql = format!("{} {}", sql, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&where_params))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(row_to_map(row)?);
    }
    Ok(result)
}

pub fn fetch_all_as<T: DeserializeOwned>(
    conn: &Connection,
    table_name: &str,
    is_distinct: bool,
    display_fields: Option<&[&str]>,
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Vec<T>> {
    let rows = fetch_all(conn, table_name, is_distinct, display_fields, where_input)?;
    let mut result = Vec::new();
    for row in &rows {
        result.push(serde_json::from_value(convert::val_to_json(row)?)?);
    }
    Ok(result)
}

///
/// delete a record from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `pk` - the primary key and its value, represented as a tuple (pk_name, pk_value)
/// * `where_input` - the where clause and the parameters for the where clause
pub fn hard_del(
    conn: &Connection,
    table_name: &str,
    pk: (&str, &str),
    where_input: Option<(&str, &[types::Value])>,
) -> Result<()> {
    verify_table_name(table_name)?;
    let (pk_name, pk_value) = pk;
    let (where_clause, where_params) = standardize_where_items(where_input, "AND")?;
    let params = [
        vec![types::Value::Text(pk_value.to_string())],
        where_params.to_vec(),
    ]
    .concat();
    let sql = format!(
        "DELETE FROM {} WHERE {} = ? {}",
        table_name, pk_name, where_clause
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&params))?;
    Ok(())
}

///
/// Make a record based on an input,
/// if a field is absent in the input, the default value is used if available
pub fn defaults_if_absent(
    defaults: &HashMap<String, types::Value>,
    input: &HashMap<String, types::Value>,
) -> HashMap<String, types::Value> {
    let mut ret = defaults.clone();
    for (key, value) in input {
        ret.insert(key.clone(), value.clone());
    }
    ret
}

///
/// insert a new record into the table
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `input` - the new record to be inserted
pub fn insert(
    conn: &Connection,
    (table_name, defaults, required_fields): (
        &str,
        &HashMap<String, types::Value>,
        &HashSet<String>,
    ),
    input: &HashMap<String, types::Value>,
    default_if_absent: bool,
) -> Result<()> {
    let input = if default_if_absent {
        defaults_if_absent(defaults, input)
    } else {
        input.clone()
    };
    verify_required_fields_for_write_ops(&input, table_name, required_fields, defaults, true)?;
    let mut params = vec![];
    let mut columns = vec![];
    let mut values = vec![];
    for (key, value) in input {
        columns.push(key.clone());
        values.push("?");
        params.push(value.clone());
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

///
/// update an existing record in the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `pk_value` - the value of the primary key
/// * `input` - the new values for the record
/// * `where_input` - the where clause and the parameters for the where clause
/// # Returns
/// * `Ok(())` - if the record is updated successfully
pub fn update(
    conn: &Connection,
    (table_name, defaults, required_fields): (
        &str,
        &HashMap<String, types::Value>,
        &HashSet<String>,
    ),
    (pk_name, pk_value): (&str, &str),
    input: &HashMap<String, types::Value>,
    where_input: Option<(&str, &[types::Value])>,
) -> Result<()> {
    verify_required_fields_for_write_ops(input, table_name, required_fields, defaults, false)?;
    let mut set_clause = vec![];
    let mut set_params = vec![];
    for (key, value) in input {
        set_clause.push(format!("{} = ?", key));
        set_params.push(value.clone());
    }
    let (where_clause, where_params) = convert::standardize_where_items(where_input, "AND")?;
    let params = [
        set_params,
        vec![types::Value::Text(pk_value.to_string())],
        where_params,
    ]
    .concat();
    let sql = format!(
        "UPDATE {} SET {} where {}=? {}",
        table_name,
        set_clause.join(", "),
        pk_name,
        where_clause
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&params))?;
    Ok(())
}
