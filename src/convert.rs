use crate::verify::verify_where_clause;
use anyhow::Result;
use rusqlite::{types, Row};
use std::collections::HashMap;

pub fn row_to_map(row: &Row) -> Result<HashMap<String, types::Value>> {
    let mut map = HashMap::new();
    for (i, column_name) in row.as_ref().column_names().iter().enumerate() {
        let value = row.get(i)?;
        map.insert(column_name.to_string(), value);
    }
    Ok(map)
}

pub fn standardize_where_items(
    where_input: Option<(&str, &[types::Value])>,
    link_word: &str,
) -> Result<(String, Vec<types::Value>)> {
    match where_input {
        Some((where_clause, where_params)) => {
            verify_where_clause(where_clause)?;
            Ok((
                format!("{} {}", link_word, where_clause),
                where_params.to_vec(),
            ))
        }
        None => Ok(("".to_string(), vec![])),
    }
}
