use std::collections::HashMap;

use super::{
    input_utils::verify_parenthood,
    sql::{get_fk_union_config, WhereConfig},
};

use super::{basics::del, input_utils::verify_pk, schema::SchemaFamily, sql::in_them_and};

use rusqlite::{types, Connection};

///
/// Delete records in a table by their primary keys
pub fn delete(
    conn: &Connection,
    schema_family: &SchemaFamily,
    table: &str,
    pk_values: &[types::Value],
    where_config: Option<WhereConfig>,
) -> anyhow::Result<()> {
    let schema = schema_family.try_get_schema(table)?;
    verify_pk(schema_family, table, pk_values)?;
    let combined_q_config = in_them_and(&schema.pk, pk_values, where_config);
    del(
        conn,
        table,
        (combined_q_config.0.as_str(), combined_q_config.1.as_slice()),
    )
}

///
/// Delete all records in a table that are children of specified parent records in another table.
pub fn delete_children_of(
    conn: &Connection,
    schema_family: &SchemaFamily,
    child_table: &str,
    parent_info: &HashMap<String, Vec<types::Value>>,
    where_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<()> {
    schema_family.try_get_schema(child_table)?;
    for (parent_table, parent_vals) in parent_info {
        verify_parenthood(
            schema_family,
            child_table,
            parent_table.as_str(),
            parent_vals,
        )?;
    }
    let combined_q_config = get_fk_union_config(schema_family, parent_info, where_config)?;
    del(
        conn,
        child_table,
        (combined_q_config.0.as_str(), combined_q_config.1.as_slice()),
    )
}
