use super::{
    shift,
    sql::{self, WhereConfig},
};

use anyhow::Result;
use rusqlite::{params_from_iter, types, Connection};

use std::collections::HashMap;

///
/// Check is a [`types::Value`] is empty
/// # Arguments
/// * `val1` - the value to be checked
/// # Examples
/// ```
/// use rusqlite::types;
/// use jankenstore::basics;
/// let val = types::Value::Null;
/// assert_eq!(basics::is_empty(&val), true);
///
/// let val = types::Value::Text("".to_string());
/// assert_eq!(basics::is_empty(&val), true);
///
/// let val = types::Value::Text(" ".to_string());
/// assert_eq!(basics::is_empty(&val), true);
///
/// let val = types::Value::Text("a".to_string());
/// assert_eq!(basics::is_empty(&val), false);
/// ```
pub fn is_empty(val1: &types::Value) -> bool {
    match val1 {
        types::Value::Null => true,
        types::Value::Text(s) => s.trim().is_empty(),
        types::Value::Blob(b) => b.is_empty(),
        _ => false,
    }
}

///
/// Configuration for fetching records from the table
/// # Fields
/// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
/// * `display_cols` - the fields to be displayed in the result
/// * `where_config` - the where clause and the parameters for the condition of the query
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct FetchConfig<'a> {
    pub is_distinct: bool,
    pub display_cols: Option<&'a [&'a str]>,
    pub where_config: Option<WhereConfig<'a>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct CountConfig<'a> {
    pub distinct_field: Option<&'a str>,
    pub where_config: Option<WhereConfig<'a>>,
}

///
/// fetch all matching records from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `fetch_config_opt` - the configuration for fetching the records
pub fn read(
    conn: &Connection,
    table_name: &str,
    fetch_config_opt: Option<FetchConfig>,
) -> Result<Vec<HashMap<String, types::Value>>> {
    let default_fields = vec!["*"];
    let fetch_config = fetch_config_opt.unwrap_or_default();
    let display_fields = fetch_config.display_cols.unwrap_or(&default_fields);
    let distinct_word = if fetch_config.is_distinct {
        "DISTINCT"
    } else {
        ""
    };
    let sql = format!(
        "SELECT {} {} FROM {}",
        distinct_word,
        display_fields.join(", "),
        table_name
    );
    let where_config = match fetch_config_opt {
        Some(cfg) => cfg.where_config,
        None => None,
    };
    let (where_q_clause, where_q_params) = sql::standardize_q_config(where_config, "WHERE");
    let sql = format!("{} {}", sql, where_q_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&where_q_params))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(shift::row_to_map(row)?);
    }
    Ok(result)
}

///
/// insert a new record into the table
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table to insert into
/// * `input` - the new record to be inserted
pub fn insert(
    conn: &Connection,
    table_name: &str,
    input: &HashMap<String, types::Value>,
) -> anyhow::Result<()> {
    let mut params = vec![];
    let mut columns = vec![];
    let mut values = vec![];
    for (key, value) in input {
        columns.push(key.clone());
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

///
/// delete all matching records from the table that meet the conditions.
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `where_config` - the where clause and the parameters for the where clause,
///                      to reduce the chance of unwanted deletions,
///                      this is not an Option and cannot contain empty clause
/// # Returns
pub fn del(conn: &Connection, table_name: &str, where_config: WhereConfig) -> anyhow::Result<()> {
    let (where_clause, where_params) = sql::standardize_q_config(Some(where_config), "WHERE");
    let sql = format!("DELETE FROM {} {}", table_name, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&where_params))?;
    Ok(())
}

/// update all matching records in the table
/// # Arguments
///
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `input` - the new values for the record
/// * `where_config` - the where clause and the parameters for the where clause,
///                      to reduce the chance of unwanted updates,
///                      this is not an Option and cannot contain empty clause
pub fn update(
    conn: &Connection,
    table_name: &str,
    input: &HashMap<String, types::Value>,
    where_config: (&str, &[types::Value]),
) -> anyhow::Result<()> {
    let mut set_clause = vec![];
    let mut set_params = vec![];
    for (key, value) in input {
        set_clause.push(format!("{} = ?", key));
        set_params.push(value.clone());
    }
    let (where_clause, where_params) = sql::standardize_q_config(Some(where_config), "WHERE");
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
/// count all matching records from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `distinct_field` - if provided, count distinct values in this field
/// * `where_config` - the where clause and the parameters for the where clause
pub fn total(
    conn: &Connection,
    table_name: &str,
    distinct_field: Option<&str>,
    where_config: Option<(&str, &[types::Value])>,
) -> Result<i64> {
    let distinct_word = if let Some(field) = distinct_field {
        format!("DISTINCT {}", field)
    } else {
        String::from("*")
    };
    let sql = format!("SELECT COUNT({}) FROM {}", distinct_word, table_name);
    let (where_q_clause, where_q_params) = sql::standardize_q_config(where_config, "WHERE");
    let sql = format!("{} {}", sql, where_q_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&where_q_params))?;
    let count = rows
        .next()?
        .ok_or(anyhow::anyhow!("No rows returned from query: {}", sql))?
        .get(0)?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::is_empty;
    use rusqlite::types;

    #[test]
    fn test_special_is_empty() {
        let val = types::Value::Null;
        assert!(is_empty(&val));

        assert!(is_empty(&types::Value::Blob(vec![])));

        assert!(!is_empty(&types::Value::Blob(vec![1])));
    }
}
