use anyhow::{anyhow, Result};
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

/// Convert a HashMap containing a rusqlite record to a serde_json::Value
/// So that it can be used in JSON related functionalities
/// # Arguments
/// * `map` - the HashMap containing the rusqlite record
/// # Returns
/// * `serde_json::Value` - the JSON representation of the record
pub fn val_to_json(map: &HashMap<String, types::Value>) -> Result<serde_json::Value> {
    let mut json_map = serde_json::Map::new();
    for (key, value) in map.iter() {
        let json_value = match value {
            types::Value::Null => serde_json::Value::Null,
            types::Value::Integer(int) => serde_json::Value::Number(serde_json::Number::from(*int)),
            types::Value::Real(float) => serde_json::Value::Number(
                serde_json::Number::from_f64(*float).ok_or(anyhow!("Invalid float"))?,
            ),
            types::Value::Text(text) => serde_json::Value::String(text.to_string()),
            types::Value::Blob(blob) => serde_json::Value::Array(
                blob.to_vec()
                    .iter()
                    .map(|b| serde_json::Value::Number(serde_json::Number::from(*b)))
                    .collect(),
            ),
        };
        json_map.insert(key.to_string(), json_value);
    }
    Ok(serde_json::Value::Object(json_map))
}

pub mod val {
    use rusqlite::types;

    pub fn v_txt(id: &str) -> types::Value {
        types::Value::Text(id.to_string())
    }

    pub fn v_int(id: i64) -> types::Value {
        types::Value::Integer(id)
    }

    pub fn v_flo(id: f64) -> types::Value {
        types::Value::Real(id)
    }

    pub fn v_blo(id: &[u8]) -> types::Value {
        types::Value::Blob(id.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::val;
    use rusqlite::types;

    #[test]
    fn test_val_converts() {
        assert_eq!(val::v_txt("test"), types::Value::Text("test".to_string()));
        assert_eq!(val::v_int(1), types::Value::Integer(1));
        assert_eq!(val::v_flo(1.0), types::Value::Real(1.0));
        assert_eq!(val::v_blo(&[1, 2, 3]), types::Value::Blob(vec![1, 2, 3]));
    }
}
