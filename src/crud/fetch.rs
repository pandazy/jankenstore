use super::{
    shift::{self, row_to_map},
    sql::{self, WhereConfig},
    verify::verify_table_name,
};

use anyhow::Result;
use rusqlite::{params_from_iter, types, Connection};
use serde::de::DeserializeOwned;

use std::collections::HashMap;

///
/// fetch all matching records from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
/// * `display_fields` - the fields to be displayed in the result
/// * `where_q_config` - the where clause and the parameters for the where clause
pub fn f_all(
    conn: &Connection,
    table_name: &str,
    where_q_config: Option<WhereConfig>,
    (is_distinct, display_fields): (bool, Option<&[&str]>),
) -> Result<Vec<HashMap<String, types::Value>>> {
    verify_table_name(table_name)?;
    let default_fields = vec!["*"];
    let display_fields = match display_fields {
        Some(fields) => fields.to_vec(),
        None => default_fields,
    };
    let distinct_word = if is_distinct { "DISTINCT" } else { "" };
    let sql = format!(
        "SELECT {} {} FROM {}",
        distinct_word,
        display_fields.join(", "),
        table_name
    );
    let (where_q_clause, where_q_params) = sql::standardize_q_config(where_q_config, "WHERE")?;
    let sql = format!("{} {}", sql, where_q_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&where_q_params))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(row_to_map(row)?);
    }
    Ok(result)
}

pub fn f_all_as<T: DeserializeOwned>(
    conn: &Connection,
    table_name: &str,
    where_q_config: Option<(&str, &[types::Value])>,
    (is_distinct, display_fields): (bool, Option<&[&str]>),
) -> Result<Vec<T>> {
    let display_opt = (is_distinct, display_fields);
    let rows = f_all(conn, table_name, where_q_config, display_opt)?;
    let mut result = Vec::new();
    for row in &rows {
        result.push(serde_json::from_value(shift::val_to_json(row)?)?);
    }
    Ok(result)
}

pub fn f_by_pk(
    conn: &Connection,
    table_name: &str,
    (pk_name, pk_values): (&str, &[types::Value]),
    where_q_config: Option<WhereConfig>,
    display_fields: Option<&[&str]>,
) -> Result<Vec<HashMap<String, types::Value>>> {
    let (pk_find_clause, pk_find_params) = sql::in_them(pk_name, pk_values);
    let pk_find_refs = (pk_find_clause.as_str(), pk_find_params.as_slice());
    let (where_clause, where_params) =
        sql::merge_q_configs(Some(pk_find_refs), where_q_config, "AND")?;
    let result = f_all(
        conn,
        table_name,
        Some((where_clause.as_str(), &where_params)),
        (false, display_fields),
    )?;
    Ok(result)
}

pub fn f_by_pk_as<T: DeserializeOwned>(
    conn: &Connection,
    table_name: &str,
    pk_config: (&str, &[types::Value]),
    where_q_config: Option<WhereConfig>,
    display_fields: Option<&[&str]>,
) -> Result<Vec<T>> {
    let rows = f_by_pk(conn, table_name, pk_config, where_q_config, display_fields)?;
    let mut result = Vec::new();
    for row in &rows {
        result.push(serde_json::from_value(shift::val_to_json(row)?)?);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::f_all_as;

    use super::*;

    #[test]
    fn test_fetch_all_as() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO test (name) VALUES ('test')", [])
            .unwrap();
        let result: Vec<serde_json::Value> = f_all_as(&conn, "test", None, (false, None)).unwrap();
        assert_eq!(result.len(), 1);
    }
}
