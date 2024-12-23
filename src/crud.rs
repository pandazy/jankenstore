use anyhow::Result;
use rusqlite::{params_from_iter, types, Connection};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};

use crate::{
    convert::{self, merge_wheres, row_to_map, standardize_where_items},
    verify::{verify_required_fields_for_write_ops, verify_required_pk_values, verify_table_name},
};

fn pk_where(pk_name: &str, pk_values: &[&str]) -> (String, Vec<types::Value>) {
    let pk_value_placeholders = pk_values
        .iter()
        .map(|_| "?")
        .collect::<Vec<&str>>()
        .join(", ");
    let clause = format!("{} IN ({})", pk_name, pk_value_placeholders);
    let params = pk_values
        .iter()
        .map(|v| types::Value::Text(v.to_string()))
        .collect::<Vec<types::Value>>();
    (clause, params)
}

///
/// count all matching records from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
/// * `where_input` - the where clause and the parameters for the where clause
pub fn count_all(
    conn: &Connection,
    table_name: &str,
    is_distinct: bool,
    where_input: Option<(&str, &[types::Value])>,
) -> Result<i64> {
    verify_table_name(table_name)?;
    let distinct_word = if is_distinct { "DISTINCT" } else { "" };
    let sql = format!("SELECT COUNT({} *) FROM {}", distinct_word, table_name);
    let (where_clause, where_params) = standardize_where_items(where_input, "WHERE")?;
    let sql = format!("{} {}", sql, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&where_params))?;
    let row = rows.next()?;
    let count = row.unwrap().get(0)?;
    Ok(count)
}

pub fn count_by_pk(
    conn: &Connection,
    table_name: &str,
    pk_name: &str,
    pk_values: &[&str],
    where_input: Option<(&str, &[types::Value])>,
) -> Result<i64> {
    let (pk_where_clause, pk_where_params) = pk_where(pk_name, pk_values);
    let pk_where_refs = (pk_where_clause.as_str(), pk_where_params.as_slice());
    let (where_clause, where_params) = merge_wheres(Some(pk_where_refs), where_input, "AND")?;
    let result = count_all(
        conn,
        table_name,
        false,
        Some((where_clause.as_str(), &where_params)),
    )?;
    Ok(result)
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

pub fn fetch_by_pk(
    conn: &Connection,
    table_name: &str,
    pk_name: &str,
    pk_values: &[&str],
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Vec<HashMap<String, types::Value>>> {
    let (pk_where_clause, pk_where_params) = pk_where(pk_name, pk_values);
    let pk_where_refs = (pk_where_clause.as_str(), pk_where_params.as_slice());
    let (where_clause, where_params) = merge_wheres(Some(pk_where_refs), where_input, "AND")?;
    let result = fetch_all(
        conn,
        table_name,
        false,
        None,
        Some((where_clause.as_str(), &where_params)),
    )?;
    Ok(result)
}

pub fn fetch_by_pk_as<T: DeserializeOwned>(
    conn: &Connection,
    table_name: &str,
    pk_name: &str,
    pk_values: &[&str],
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Vec<T>> {
    let rows = fetch_by_pk(conn, table_name, pk_name, pk_values, where_input)?;
    let mut result = Vec::new();
    for row in &rows {
        result.push(serde_json::from_value(convert::val_to_json(row)?)?);
    }
    Ok(result)
}

/// The bond between two resources (tables)
/// See also [`Bond::OwnedBy`] (n-1), [`Bond::PeerLeftIs`] (n-n) and [`Bond::PeerRightIs`] (n-n)
///
#[derive(Debug, Clone)]
pub enum Bond {
    /// the bond between the assigned resource and the input String is that
    /// the resource represented by the inputs String is the parent node of the assigned resource
    /// it represents a n-to-1 relationship between the assigned resource and the input String
    /// for example, if a resource called "user" has a bond of OwnedBy("company"),
    /// then the "user" resource is owned by the "company" resource
    /// user will have a field called "company_id" which is a foreign key to the "company" resource
    OwnedBy(String),

    /// the bond between the assigned resource and the input String is that
    /// the resource represented by the inputs String is a peer of the assigned resource
    /// it represents a n-to-n relationship between the assigned resource and the input String
    /// they are stored in a separate table with the name
    /// `rel_{input_resource_name}_{assigned_resource_name}`, and the table at least has two fields
    /// `{input_resource_name}_id` and `{assigned_resource_name}_id`
    PeerLeftIs(String),

    /// the bond between the assigned resource and the input String is that
    /// similar to [`Bond::PeerLeftIs`], but the assigned resource is the left peer, so
    /// the table name will be `rel_{assigned_resource_name}_{input_resource_name}`
    PeerRightIs(String),
}

///
/// delete a record from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `pk_name` - the name of the primary key
/// * `pk_values` - records to be deleted represented by their primary key values
/// * `where_input` - the where clause and the parameters for the where clause
pub fn hard_del_by_pk(
    conn: &Connection,
    table_name: &str,
    pk_name: &str,
    pk_values: &[&str],
    where_input: Option<(&str, &[types::Value])>,
) -> Result<()> {
    verify_required_pk_values(pk_values, table_name, pk_name)?;
    verify_table_name(table_name)?;
    let (pk_where_clause, pk_where_params) = pk_where(pk_name, pk_values);
    let pk_where_refs = (pk_where_clause.as_str(), pk_where_params.as_slice());
    let (where_clause, where_params) = merge_wheres(Some(pk_where_refs), where_input, "AND")?;
    let sql = format!("DELETE FROM {} WHERE {}", table_name, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&where_params))?;
    Ok(())
}

///
/// Make a record based on an input,
/// if a field is absent in the input, the default value is used if available
fn defaults_if_absent(
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
/// * `table_name` - the name of the table
/// * `defaults` - the default values for the table
/// * `required_fields` - the required fields for the table
/// * `pk_name` - the name of the primary key
/// * `pk_values` - records to be updated represented by their primary key values
/// * `input` - the new values for the record
/// * `where_input` - the where clause and the parameters for the where clause
/// # Returns
/// * `Ok(())` - if the record is updated successfully
pub fn update_by_pk(
    conn: &Connection,
    (table_name, defaults, required_fields): (
        &str,
        &HashMap<String, types::Value>,
        &HashSet<String>,
    ),
    pk_name: &str,
    pk_values: &[&str],
    input: &HashMap<String, types::Value>,
    where_input: Option<(&str, &[types::Value])>,
) -> Result<()> {
    verify_required_pk_values(pk_values, table_name, pk_name)?;
    verify_required_fields_for_write_ops(input, table_name, required_fields, defaults, false)?;
    let mut set_clause = vec![];
    let mut set_params = vec![];
    let (pk_where_clause, pk_where_params) = pk_where(pk_name, pk_values);
    let pk_where_refs = (pk_where_clause.as_str(), pk_where_params.as_slice());
    for (key, value) in input {
        set_clause.push(format!("{} = ?", key));
        set_params.push(value.clone());
    }
    let (merged_where_clause, merged_where_params) =
        merge_wheres(Some(pk_where_refs), where_input, "AND")?;
    let params = [set_params, merged_where_params].concat();
    let sql = format!(
        "UPDATE {} SET {} WHERE {}",
        table_name,
        set_clause.join(", "),
        merged_where_clause,
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&params))?;
    Ok(())
}
