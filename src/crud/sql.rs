use anyhow::{anyhow, Result};
use rusqlite::types;

pub fn in_them(col_name: &str, col_values: &[types::Value]) -> (String, Vec<types::Value>) {
    let pk_value_placeholders = col_values
        .iter()
        .map(|_| "?")
        .collect::<Vec<&str>>()
        .join(", ");
    let clause = format!("{} IN ({})", col_name, pk_value_placeholders);
    (clause, col_values.to_vec())
}

pub fn standardize_q_config(
    q_config: Option<(&str, &[types::Value])>,
    link_word: &str,
) -> Result<(String, Vec<types::Value>)> {
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
    q_config1: Option<(&str, &[types::Value])>,
    q_config2: Option<(&str, &[types::Value])>,
    link_word: &str,
) -> Result<(String, Vec<types::Value>)> {
    let (clause1, params1) = standardize_q_config(q_config1, "")?;
    let (clause2, params2) = standardize_q_config(q_config2, link_word)?;
    Ok((
        format!("{} {}", clause1, clause2),
        [params1.to_vec(), params2].concat(),
    ))
}
