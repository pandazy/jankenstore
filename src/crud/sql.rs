use anyhow::{anyhow, Result};
use rusqlite::types;

/// Used as inputs to generate where conditions for SQL queries
///
/// # Arguments
/// * `clause` - the where clause
/// * `params` - the parameters for the where clause
///
/// # Examples
/// ```
/// use jankenstore::crud::sql::WhereConfig;
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
pub fn in_them(col_name: &str, col_values: &[types::Value]) -> WhereConfigOwned {
    let pk_value_placeholders = col_values
        .iter()
        .map(|_| "?")
        .collect::<Vec<&str>>()
        .join(", ");
    let clause = format!("{} IN ({})", col_name, pk_value_placeholders);
    (clause, col_values.to_vec())
}

pub fn standardize_q_config(
    q_config: Option<WhereConfig>,
    link_word: &str,
) -> Result<WhereConfigOwned> {
    match q_config {
        Some((clause, params)) => {
            if clause.trim().is_empty() {
                return Err(anyhow!(
                    "Empty clause is confusing, if you don't need it, specify the `q_config` as None"
                ));
            }
            Ok((
                if link_word.is_empty() {
                    clause.to_string()
                } else {
                    format!("{} {}", link_word, clause)
                },
                params.to_vec(),
            ))
        }
        None => Ok(("".to_string(), vec![])),
    }
}

pub fn merge_q_configs(
    q_config1: Option<WhereConfig>,
    q_config2: Option<WhereConfig>,
    link_word: &str,
) -> Result<WhereConfigOwned> {
    let (clause1, params1) = standardize_q_config(q_config1, "")?;
    let (clause2, params2) = standardize_q_config(q_config2, link_word)?;
    Ok((
        format!("{} {}", clause1, clause2),
        [params1.to_vec(), params2].concat(),
    ))
}
