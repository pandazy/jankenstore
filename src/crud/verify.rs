use super::verify;

use anyhow::{anyhow, Result};
use rusqlite::types;

use std::collections::{HashMap, HashSet};

/// Do basic verification for the table name
pub fn verify_table_name(table_name: &str) -> Result<()> {
    if table_name.is_empty() {
        return Err(anyhow!("The table name cannot be an empty string"));
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

///
/// Verify the basic write operations for the table
///
/// * The main purpose of this function is to provide basic consistency check for the input
///   to avoid mistakes including:
///   - specifying a column that does not exist in the table
///   - specifying a value of a wrong type
/// * Note: the argument `defaults` is assumed to contain all the columns that will be used in the related operation,
///   even if they are not specified in the input
/// # Arguments
/// * `input` - the input for the operation, it
///             - cannot contains keys that are not defined in the `defaults`
///             - must contains values that are of the same type as the ones in the `defaults`
/// * `table_name` - the name of the table
/// * `defaults` - the default values for `all` the columns that will be operated on
///                (including the ones that are not provided in the input but are defined by the operation)
///
pub fn verify_basic_write_ops(
    input: &HashMap<String, types::Value>,
    table_name: &str,
    defaults: &HashMap<String, types::Value>,
) -> Result<()> {
    if input.keys().len() == 0 {
        return Err(anyhow!("(table: {}) The input has no items", table_name));
    }
    let first_mismatch = input.iter().find(|(key, input_value)| {
        let default_value = match defaults.get(*key) {
            Some(v) => v,
            None => return true,
        };
        !verify::are_same_type(input_value, default_value)
    });

    if let Some(mismatch) = first_mismatch {
        let (mismatched_key, mismatched_val) = mismatch;
        match defaults.get(mismatched_key) {
            None => {
                return Err(anyhow!(
                    "(table: {}) The input has a key '{}' that is not allowed",
                    table_name,
                    mismatched_key,
                ));
            }
            Some(default_value) => {
                return Err(anyhow!(
                    "(table: {}) The input's value type for '{}' must be something like {:?}, but received {:?}",
                    table_name,
                    mismatched_key,
                    default_value,
                    mismatched_val,
                ));
            }
        }
    }
    Ok(())
}

pub fn is_empty(val1: &types::Value) -> bool {
    match val1 {
        types::Value::Null => true,
        types::Value::Text(s) => s.trim().is_empty(),
        types::Value::Blob(b) => b.is_empty(),
        _ => false,
    }
}

pub fn is_violating_required_rule(input: &HashMap<String, types::Value>, key: &str) -> bool {
    match input.get(key) {
        Some(val) => is_empty(val),
        None => true,
    }
}

///
/// verify the presence of required fields of the input for the operation of the table
/// # Arguments
/// * `input` - see `input` of [`verify_basic_write_ops`]
/// * `table_name` - see `table_name` of [`verify_basic_write_ops`]
/// * `required_fields` - the names of fields that cannot be left unspecified
/// * `all_required` - whether all required fields are needed, if false,
///                    only the required fields that are present in the input are checked,
///                    for example,
///                    - `false` is used for the update operation
///                    - `true` is used for the insert operation
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
            .find(|required_field| is_violating_required_rule(input, required_field))
    } else {
        input
            .keys()
            .find(|key| required_fields.contains(*key) && is_violating_required_rule(input, key))
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

/// Verify the values for the operation of the table
/// * The main purpose of this function is to avoid some operations such as updates or deletes to accidentally modify or delete all rows
/// # Arguments
/// * `values` - the input values for the operation
/// * `table_name` - the name of the table
/// * `field_name` - the name of the column that the values are for
///
/// # Returns
/// * `Result<()>` - if valid then Ok, otherwise Err with the error message
pub fn verify_values_required(
    values: &[types::Value],
    table_name: &str,
    field_name: &str,
) -> Result<()> {
    if values.is_empty()
        || values.iter().any(|pk_value| {
            if let types::Value::Text(s) = pk_value {
                s.trim().is_empty()
            } else {
                false
            }
        })
    {
        return Err(anyhow!(
            "(table: {}) At least 1 value for '{}' is required for this operation, and none of them can be an empty string\nBut here are received values: {:?}",
            table_name,
            field_name,
            values,
        ));
    }
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

/// Get the verified insert inputs for the table, see also:
/// - [`verify_basic_write_ops`]
/// - [`verify_required_fields_for_write_ops`]
pub fn get_verified_insert_inputs(
    table_name: &str,
    input: &HashMap<String, types::Value>,
    verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
) -> Result<HashMap<String, types::Value>> {
    if let Some((defaults, required_fields, default_if_absent)) = verification_options {
        let input_before_verify = if default_if_absent {
            defaults_if_absent(defaults, input)
        } else {
            input.clone()
        };
        verify_required_fields_for_write_ops(
            &input_before_verify,
            table_name,
            required_fields,
            defaults,
            true,
        )?;
        Ok(input_before_verify)
    } else {
        Ok(input.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // this is only a case in theory, because
    // - this module is not public outside the crate
    // - since the user is not allowed specify Null as a default value,
    //   when the user tries to specify Null, it will be always blocked by type difference error
    // This test is only to make sure 100% coverage
    #[test]
    fn test_is_empty_null() {
        assert!(is_empty(&types::Value::Null));
    }

    // this is only a case in theory, because
    // - basic verification doesn't allow columns without default values
    // - the input will never contains a column that does not exist when is_violating_required_rule is used
    // so this test is only to make sure 100% coverage
    #[test]
    fn test_is_violating_required() {
        let input = HashMap::from([("count".to_string(), types::Value::Integer(0))]);
        assert!(is_violating_required_rule(&input, "name"));
        assert!(!is_violating_required_rule(&input, "count"));
    }
}
