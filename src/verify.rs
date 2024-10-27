use std::collections::{HashMap, HashSet};

use crate::verify;
use anyhow::{anyhow, Result};
use rusqlite::types;

pub fn verify_table_name(table_name: &str) -> Result<()> {
    if table_name.is_empty() {
        return Err(anyhow!("The table name cannot be an empty string"));
    }
    Ok(())
}

pub fn verify_where_clause(where_clause: &str) -> Result<()> {
    if where_clause.trim().is_empty() {
        return Err(anyhow!(
      "The where clause cannot be an empty string, if you don't want to use a where clause, specify where_input as None"
  ));
    }
    Ok(())
}

///
/// Verify if two values are of the same stored column value type used by rusqlite
/// # Arguments
/// * `val1` - the first value
/// * `val2` - the second value
/// # Returns
/// * `bool` - whether the two values are of the same type
pub fn are_same_type(val1: &types::Value, val2: &types::Value) -> bool {
    matches!(
        (val1, val2),
        (types::Value::Null, types::Value::Null)
            | (types::Value::Integer(_), types::Value::Integer(_))
            | (types::Value::Real(_), types::Value::Real(_))
            | (types::Value::Text(_), types::Value::Text(_))
            | (types::Value::Blob(_), types::Value::Blob(_))
    )
}

pub fn verify_basic_write_ops(
    input: &HashMap<String, types::Value>,
    table_name: &str,
    defaults: &HashMap<String, types::Value>,
) -> Result<()> {
    if input.keys().len() == 0 {
        return Err(anyhow!("(table: {}) The input has no items", table_name));
    }
    let trespasser_option = input
        .keys()
        .find(|key| !defaults.contains_key(&key.to_string()));
    if let Some(trespasser) = trespasser_option {
        return Err(anyhow!(
            "(table: {}) The input has a key '{}' that is not allowed",
            table_name,
            trespasser,
        ));
    }
    let first_mismatch = input.keys().find(|key| {
        let val = input.get(&key.to_string());
        match val {
            Some(val) => !verify::are_same_type(val, defaults.get(&key.to_string()).unwrap()),
            None => false,
        }
    });
    if let Some(mismatch) = first_mismatch {
        return Err(anyhow!(
      "(table: {}) The input's value type for '{}' must be something like {:?}, but received {:?}",
      table_name,
      mismatch,
      defaults.get(mismatch).unwrap(),
      input.get(mismatch).unwrap()
  ));
    }
    Ok(())
}

pub fn is_empty(val1: &types::Value) -> bool {
    match val1 {
        types::Value::Null => true,
        types::Value::Text(s) => s.is_empty(),
        types::Value::Blob(b) => b.is_empty(),
        _ => false,
    }
}

pub fn is_violating_required(input: &HashMap<String, types::Value>, key: &str) -> bool {
    input.get(key).is_none() || is_empty(input.get(key).unwrap())
}

///
/// verify the presence of required fields of the input for the operation of the resource
/// # Arguments
/// * `input` - the input for the operation
/// * `all_required` - whether all required fields are needed, if false,
///   only the required fields that are present in the input are checked,
///   for example, false is used for the update operation, true is used for the insert operation
pub fn verify_required_fields_for_write_ops(
    input: &HashMap<String, types::Value>,
    table_name: &str,
    required_fields: &HashSet<String>,
    defaults: &HashMap<String, types::Value>,
    all_required: bool,
) -> Result<()> {
    verify_basic_write_ops(input, table_name, defaults)?;
    let first_none = if all_required {
        required_fields
            .iter()
            .find(|required_field| is_violating_required(input, required_field))
    } else {
        input
            .keys()
            .find(|key| required_fields.contains(*key) && is_violating_required(input, key))
    };
    if let Some(invalid) = first_none {
        return Err(anyhow!(
            "(table: {}) The input requires the value of '{}'",
            table_name,
            invalid
        ));
    }
    Ok(())
}
