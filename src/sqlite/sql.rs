use std::collections::HashMap;

use super::{input_utils::get_fk_name, schema::SchemaFamily};

use rusqlite::types;

/// Used as inputs to generate where conditions for SQL queries
///
/// # Arguments
/// * `clause` - the where clause
/// * `params` - the parameters for the where clause
///
/// # Examples
/// ```
/// use jankenstore::sqlite::sql::WhereConfig;
/// use rusqlite::types;
/// let (clause, params): WhereConfig = ("id = ?", &vec![types::Value::Integer(1)]);
/// ```
pub type WhereConfig<'a> = (&'a str, &'a [types::Value]);

/// The owned version of [WhereConfig],
/// used as outputs, e.g., for functions that generate where conditions for SQL queries
pub type WhereConfigOwned = (String, Vec<types::Value>);

///
/// Create a where clause for a column to be in a list of values
/// # Arguments
/// * `col_name` - the name of the column
/// * `col_values` - the values to be matched
/// # Returns
/// * `String` - the where clause, e.g., `id IN (?, ?, ?)`
pub fn in_them_clause(col_name: &str, col_values: &[types::Value]) -> String {
    let pk_value_placeholders = col_values
        .iter()
        .map(|_| "?")
        .collect::<Vec<&str>>()
        .join(", ");
    format!("{col_name} IN ({pk_value_placeholders})")
}

///
/// Create a where clause for a column to be in a list of values
/// # Arguments
/// * `col_name` - the name of the column
/// * `col_values` - the values to be matched
pub fn in_them(col_name: &str, col_values: &[types::Value]) -> WhereConfigOwned {
    (in_them_clause(col_name, col_values), col_values.to_vec())
}

///
/// Standardize the where clause and parameters for a SQL query
/// # Arguments
/// * `q_config` - the where clause and the parameters for the where clause
/// * `link_word` - the word to link the where clause to the previous clause
/// # Examples
/// ```
/// use jankenstore::sqlite::sql::standardize_q_config;
/// use rusqlite::types;
/// let params = vec![types::Value::Integer(1)];
/// let q_config = Some(("id = ?", params.as_slice()));
/// let (clause, params) = standardize_q_config(q_config, "AND");
///
/// assert_eq!(clause, "AND id = ?");
/// assert_eq!(params, vec![types::Value::Integer(1)]);
/// ```
pub fn standardize_q_config(q_config: Option<WhereConfig>, link_word: &str) -> WhereConfigOwned {
    match q_config {
        Some((clause, params)) => {
            if clause.trim().is_empty() {
                return ("".to_string(), vec![]);
            }
            (
                if link_word.is_empty() {
                    clause.to_string()
                } else {
                    format!("{link_word} {clause}")
                },
                params.to_vec(),
            )
        }
        None => ("".to_string(), vec![]),
    }
}

///
/// Merge two where clauses and their parameters
/// # Arguments
/// * `q_config1` - the first option of where clause and the parameters for the where clause
/// * `q_config2` - the second option of where clause and the parameters for the where clause
/// * `link_word` - the word to link the where clause to the previous clause, e.g., "AND", "OR"
/// # Examples
/// ```
/// use jankenstore::sqlite::sql::merge_q_configs;
/// use rusqlite::types;
/// let params1 = vec![types::Value::Integer(1)];
/// let q_config1 = Some(("id = ?", params1.as_slice()));
/// let params2 = vec![types::Value::Integer(2)];
/// let q_config2 = Some(("name = ?", params2.as_slice()));
/// let (clause, params) = merge_q_configs(q_config1, q_config2, "AND");
/// assert_eq!(clause, "(id = ? AND name = ?)");
/// assert_eq!(params, vec![types::Value::Integer(1), types::Value::Integer(2)]);
/// ```
pub fn merge_q_configs(
    q_config1: Option<WhereConfig>,
    q_config2: Option<WhereConfig>,
    link_word: &str,
) -> WhereConfigOwned {
    let (clause1, params1) = standardize_q_config(q_config1, "");
    let (clause2, params2) = standardize_q_config(q_config2, link_word);
    (
        format!("({clause1} {clause2})"),
        [params1.to_vec(), params2].concat(),
    )
}

///
/// A combination of [in_them] and [merge_q_configs] with the link word "AND"
/// See also:
/// - [in_them]
/// - [merge_q_configs]
pub fn in_them_and(
    col_name: &str,
    col_values: &[types::Value],
    q_config: Option<WhereConfig>,
) -> WhereConfigOwned {
    let in_q_config = in_them(col_name, col_values);
    merge_q_configs(
        Some((in_q_config.0.as_str(), in_q_config.1.as_slice())),
        q_config,
        "AND",
    )
}

///
/// Get the query conditions for a list of foreign-key related (parent/peer) records
/// # Arguments
/// * `link_config` - the list of related dependency table names and their primary key values
/// * `where_config` - generic where clause and the parameters for the where clause apart from the parent records
pub fn get_fk_union_config(
    schema_family: &SchemaFamily,
    link_config: &HashMap<String, Vec<types::Value>>,
    where_config: Option<WhereConfig>,
) -> anyhow::Result<WhereConfigOwned> {
    let mut combined_q_configs = (String::new(), Vec::new());
    for (fk_main_table, fk_vals) in link_config {
        let in_them_config = in_them(
            &get_fk_name(fk_main_table.as_str(), schema_family)?,
            fk_vals,
        );
        combined_q_configs = merge_q_configs(
            Some((in_them_config.0.as_str(), in_them_config.1.as_slice())),
            Some((
                combined_q_configs.0.as_str(),
                combined_q_configs.1.as_slice(),
            )),
            "OR",
        );
    }
    Ok(merge_q_configs(
        Some((
            combined_q_configs.0.as_str(),
            combined_q_configs.1.as_slice(),
        )),
        where_config,
        "AND",
    ))
}
