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
/// use jankenstore::sqlite::basics;
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
/// * `order_by` - the field to order the results by
/// * `limit` - the maximum number of records to return
/// * `offset` - the number of records to skip before returning the results
/// * `group_by` - the field to group the results by
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct FetchConfig<'a> {
    pub is_distinct: bool,
    pub display_cols: Option<&'a [&'a str]>,
    pub where_config: Option<WhereConfig<'a>>,
    pub order_by: Option<&'a str>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
    pub group_by: Option<&'a str>,
}

///
/// Configuration for counting records from the table
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct CountConfig<'a> {
    pub distinct_field: Option<&'a str>,
    pub where_config: Option<WhereConfig<'a>>,
}

pub const ILLEGAL_BY_CHARS: [char; 16] = [
    '@', '!', '#', '$', '%', '^', '&', '*', '=', '{', '}', '[', ']', '<', '>', '~',
];

///
/// group by, order by, limit, and offset do not work well with Rusqlite's parameterized queries
/// this is a workaround to prevent SQL injection
fn contains_illegal_by_chars(s: &str) -> bool {
    s.contains(['\n', '\r', '\t'])
        || s.contains("--")
        || s.contains("/*")
        || s.contains("*/")
        // special characters need to be wrapped in quotes
        || s.contains(ILLEGAL_BY_CHARS)
}

///
/// fetch all matching records from the table with total count
///
/// Using count is useful for pagination.
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `fetch_config_opt` - the configuration for fetching the records
/// * `skip_count` - whether to skip the count
/// # Returns
/// A tuple of the records and the total count
pub fn read_with_total(
    conn: &Connection,
    table_name: &str,
    fetch_config_opt: Option<FetchConfig>,
    skip_count: bool,
) -> Result<(Vec<HashMap<String, types::Value>>, u64)> {
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
    let group_by = match fetch_config.group_by {
        Some(field) => format!(
            " GROUP BY {}",
            field
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>()
        ),
        None => String::new(),
    };
    let order_by = match fetch_config.order_by {
        Some(field) => format!(" ORDER BY {}", field.trim()),
        None => String::new(),
    };
    let limit = match fetch_config.limit {
        Some(limit) => format!(" LIMIT {}", limit),
        None => String::new(),
    };
    let offset = match fetch_config.offset {
        Some(offset) => format!(" OFFSET {}", offset),
        None => String::new(),
    };

    for clause in [group_by.as_str(), order_by.as_str()] {
        if contains_illegal_by_chars(clause) {
            return Err(anyhow::anyhow!(
                "Illegal characters in the clause: {}",
                clause
            ));
        }
    }
    let where_config = match fetch_config_opt {
        Some(cfg) => cfg.where_config,
        None => None,
    };
    let (where_q_clause, where_q_params) = sql::standardize_q_config(where_config, "WHERE");
    let sql = format!("{} {}", sql, where_q_clause);
    let sql_without_pagination = format!("{}{}{}", sql, group_by, order_by);
    let sql_with_pagination = format!("{}{}{}{}{}", sql, group_by, order_by, limit, offset);
    let mut stmt = conn.prepare(&sql_with_pagination)?;
    let mut rows = stmt.query(params_from_iter(&where_q_params))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(shift::row_to_map(row)?);
    }

    if skip_count {
        let total = &result.len();
        return Ok((result.clone(), *total as u64));
    }

    let total_sql = format!("SELECT COUNT(*) FROM ({})", sql_without_pagination);
    let mut stmt = conn.prepare(&total_sql)?;
    let total = stmt.query_row(params_from_iter(&where_q_params), |row| row.get(0))?;
    Ok((result, total))
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
    let (result, _) = read_with_total(conn, table_name, fetch_config_opt, true)?;
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

///
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
