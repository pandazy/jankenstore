use super::{
    basics::is_empty,
    read::RecordOwned,
    schema::SchemaFamily,
    shift::{json_to_val, json_to_val_map},
};

use anyhow::{anyhow, Result};
use rusqlite::types;
use std::collections::HashMap;

///
/// Configuration for verifying the input of certain write (e.g, create or update) operations
/// # Fields
/// * `default_if_absent` - whether to use the default value if the input is absent or empty
/// * `must_have_every_col` - whether the input must have every column in the schema
///                           For example:
///                           - for a create operation, this should be true
///                           - for an update operation, this should be false     
pub struct VerifyConf {
    pub default_if_absent: bool,
    pub must_have_every_col: bool,
}

///
/// Get the verified input for a table
///
/// - If the input contains a field that is not in the schema's defaults
///   an error is returned to mitigate malicious attempts or typo.
/// - Each field in the input's type should be the same as its corresponding default value in the schema
/// # Arguments
/// * `schema_family` - the schema family containing the schema for the table. See [SchemaFamily]
/// * `table` - the name of the table
/// * `input` - the input to be verified
/// * `config` - the configuration for verifying the input. See [VerifyConf]
pub fn get_verified_input(
    schema_family: &SchemaFamily,
    table: &str,
    input: &RecordOwned,
    config: VerifyConf,
) -> Result<RecordOwned> {
    let VerifyConf {
        default_if_absent,
        must_have_every_col,
    } = config;
    let schema = schema_family.try_get_schema(table)?;
    let mut updated_inputs = HashMap::new();
    let mut col_types_to_check = schema.types.clone();
    let verify_required_field_with_value = |field: &str, value: &types::Value| -> Result<()> {
        if schema.required_fields.contains(field) && is_empty(value) {
            return Err(anyhow!(
                "`{}`@`{}` is required but is empty. \nSchema: {:?}",
                field,
                table,
                schema
            ));
        }
        Ok(())
    };
    let get_col_default = |col_name: &str| -> &types::Value {
        schema.defaults.get(col_name).unwrap_or(&types::Value::Null)
    };
    for (col_name, col_val) in input {
        if !schema.types.contains_key(col_name) {
            return Err(anyhow::anyhow!(
                "`{}`@`{}` is not defined. \nSchema: {:?}",
                table,
                col_name,
                schema
            ));
        }
        let updated_value = if is_empty(col_val) && default_if_absent {
            get_col_default(col_name)
        } else {
            col_val
        };
        verify_column_val(schema_family, table, col_name, updated_value)?;
        verify_required_field_with_value(col_name, updated_value)?;
        updated_inputs.insert(col_name.to_owned(), updated_value.to_owned());
        col_types_to_check.remove(col_name);
    }
    if must_have_every_col && !col_types_to_check.is_empty() {
        for key in col_types_to_check.keys() {
            let default_val = get_col_default(key);
            verify_required_field_with_value(key, default_val)?;
            if !updated_inputs.contains_key(key) {
                updated_inputs.insert(key.to_owned(), default_val.clone());
            }
        }
    }
    Ok(updated_inputs)
}

///
/// Convert a JSON value to a rusqlite value
/// # Arguments
/// * `schema_family` - the schema family containing the schema for the table
/// * `table_name` - the name of the table
/// * `col_name` - the name of the column
/// * `json` - the JSON value to be converted
pub fn json_to_val_by_schema(
    schema_family: &SchemaFamily,
    table_name: &str,
    col_name: &str,
    json: &serde_json::Value,
) -> Result<types::Value> {
    let schema = schema_family.try_get_schema(table_name)?;
    let the_type = schema.types.get(col_name).ok_or_else(|| {
        anyhow!(
            "Column '{}'@`{}` does not have a defined type. \nSchema: {:?}",
            table_name,
            col_name,
            schema
        )
    })?;
    json_to_val(the_type, json)
}

///
/// Convert a JSON value of foreign key to a rusqlite value
/// # Arguments
/// * `schema_family` - the schema family containing the schema for the table
/// * `table_name` - the name of the table
/// * `parent_name` - the name of the parent table that the foreign key is pointing to
/// * `json` - the JSON value to be converted
pub fn json_to_fk_by_schema(
    schema_family: &SchemaFamily,
    table_name: &str,
    parent_name: &str,
    json: &serde_json::Value,
) -> Result<types::Value> {
    json_to_val_by_schema(schema_family, table_name, &fk_name(parent_name), json)
}

///
/// Get the foreign key column name of a main in its reference table
pub fn fk_name(main_table_name: &str) -> String {
    format!("{}_id", main_table_name)
}

///
/// Convert a JSON value of primary key to a rusqlite value
/// # Arguments
/// * `schema_family` - the schema family containing the schema for the table
/// * `table_name` - the name of the table
/// * `json` - the JSON value to be converted
pub fn json_to_pk_val_by_schema(
    schema_family: &SchemaFamily,
    table_name: &str,
    json: &serde_json::Value,
) -> Result<types::Value> {
    let schema = schema_family.try_get_schema(table_name)?;
    let pk_name = &schema.pk;
    json_to_val_by_schema(schema_family, table_name, pk_name, json)
}

///
/// Convert a JSON value to a HashMap containing a rusqlite record
/// # Arguments
/// * `schema_family` - the schema family containing the schema for the table
/// * `table_name` - the name of the table
/// * `json` - the JSON representation of the record
pub fn json_to_val_map_by_schema(
    schema_family: &SchemaFamily,
    table_name: &str,
    json: &serde_json::Value,
) -> Result<RecordOwned> {
    let schema = schema_family.try_get_schema(table_name)?;
    json_to_val_map(&schema.types, json)
}

///
/// Verify the column value for a table
pub fn verify_column_val(
    schema_family: &SchemaFamily,
    table: &str,
    col_name: &str,
    col_val: &types::Value,
) -> Result<()> {
    let schema = schema_family.try_get_schema(table)?;
    let types = schema.types.get(col_name).ok_or_else(|| {
        anyhow!(
            " `{}`@`{}` not found. \nSchema: {:?}",
            col_name,
            table,
            schema
        )
    })?;
    if !col_val.data_type().eq(types) {
        return Err(anyhow!(
            "`{}`@`{}` 's value {:?} is of the wrong type. Expected {:?}",
            col_name,
            table,
            col_val,
            schema.types.get(col_name)
        ));
    }
    Ok(())
}

///
/// Verify the primary key values for a table
pub fn verify_pk(
    schema_family: &SchemaFamily,
    table: &str,
    pk_values: &[types::Value],
) -> Result<()> {
    let schema = schema_family.try_get_schema(table)?;
    for pk_val in pk_values {
        verify_column_val(schema_family, table, &schema.pk, pk_val)?;
    }
    Ok(())
}

///
/// Verify the foreign key values for a child table
/// the `parent_table` has the parenthood relationship with the `child_table`
pub fn verify_fk(
    schema_family: &SchemaFamily,
    table: &str,
    parent_table: &str,
    fk_val: &[types::Value],
) -> Result<()> {
    for fk in fk_val {
        verify_column_val(schema_family, table, &fk_name(parent_table), fk)?;
    }
    Ok(())
}

///
/// Verify the basic schema of a parenthood relationship
/// # Arguments
/// * `schema_family` - the schema family containing the schema for the table
/// * `child_table` - the name of the child table
/// * `parent_table` - the name of the parent table
/// * `parent_vals` - the values of the parent table's primary key, their types will be verified
pub fn verify_parenthood(
    schema_family: &SchemaFamily,
    child_table: &str,
    parent_table: &str,
    parent_vals: &[types::Value],
) -> Result<()> {
    schema_family.verify_child_of(child_table, parent_table)?;
    verify_pk(schema_family, parent_table, parent_vals)?;
    verify_fk(schema_family, child_table, parent_table, parent_vals)
}
