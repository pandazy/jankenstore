use super::{sql, verify::verify_table_name};

use anyhow::Result;
use rusqlite::{params_from_iter, types, Connection};

///
/// count all matching records from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `distinct_field` - if provided, count distinct values in this field
/// * `where_q_config` - the where clause and the parameters for the where clause
pub fn t_all(
    conn: &Connection,
    table_name: &str,
    distinct_field: Option<&str>,
    where_q_config: Option<(&str, &[types::Value])>,
) -> Result<i64> {
    verify_table_name(table_name)?;
    let distinct_word = if let Some(field) = distinct_field {
        format!("DISTINCT {}", field)
    } else {
        String::from("*")
    };
    let sql = format!("SELECT COUNT({}) FROM {}", distinct_word, table_name);
    let (where_q_clause, where_q_params) = sql::standardize_q_config(where_q_config, "WHERE")?;
    let sql = format!("{} {}", sql, where_q_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&where_q_params))?;
    let count = match rows.next()? {
        Some(row) => row.get(0)?,
        None => 0,
    };
    Ok(count)
}

pub fn t_by_pk(
    conn: &Connection,
    table_name: &str,
    pk_name: &str,
    pk_values: &[types::Value],
    distinct_field: Option<&str>,
    where_q_config: Option<(&str, &[types::Value])>,
) -> Result<i64> {
    let (pk_query_clause, pk_query_params) = sql::in_them(pk_name, pk_values);
    let pk_where_refs = (pk_query_clause.as_str(), pk_query_params.as_slice());
    let (where_q_clause, where_q_params) =
        sql::merge_q_configs(Some(pk_where_refs), where_q_config, "AND")?;
    let result = t_all(
        conn,
        table_name,
        distinct_field,
        Some((where_q_clause.as_str(), &where_q_params)),
    )?;
    Ok(result)
}
