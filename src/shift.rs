use anyhow::{anyhow, Context, Result};
use rusqlite::{
    types::{self},
    Row,
};

use std::collections::HashMap;

use super::read::RecordOwned;

/// Convert a rusqlite::[Row] to a HashMap
/// So that it can be used in JSON related functionalities
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

fn json_to_i64(json: &serde_json::Value) -> Result<i64> {
    let straight_num = json.as_i64();
    if let Some(num) = straight_num {
        Ok(num)
    } else {
        let as_str = json.as_str().ok_or_else(|| {
            anyhow!(
                "Failed to convert JSON value to str before parsing into integer, JSON value: {}",
                json.to_string()
            )
        })?;
        let parsed = as_str.parse::<i64>().map_err(|e| {
            anyhow!(
                "Failed to parse JSON value into integer, JSON value: {}, error: {}",
                json.to_string(),
                e
            )
        })?;
        Ok(parsed)
    }
}

fn json_to_f64(json: &serde_json::Value) -> Result<f64> {
    let straight_num = json.as_f64();
    if let Some(num) = straight_num {
        Ok(num)
    } else {
        let as_str = json.as_str().ok_or_else(|| {
            anyhow!(
                "Failed to convert JSON value to str before parsing into float, JSON value: {}",
                json.to_string()
            )
        })?;
        let parsed = as_str.parse::<f64>().map_err(|e| {
            anyhow!(
                "Failed to parse JSON value into float, JSON value: {}, error: {}",
                json.to_string(),
                e
            )
        })?;
        Ok(parsed)
    }
}

pub fn json_to_val(the_type: &types::Type, json: &serde_json::Value) -> Result<types::Value> {
    let throw = || {
        anyhow!(
            "Column requires {}, but saw invalid value {}",
            the_type,
            json,
        )
    };
    let val = match the_type {
        types::Type::Integer => {
            let val = json_to_i64(json).with_context(throw)?;
            types::Value::Integer(val)
        }
        types::Type::Real => {
            let val = json_to_f64(json).with_context(throw)?;
            types::Value::Real(val)
        }
        types::Type::Text => {
            let val = json.as_str().ok_or_else(throw)?;
            types::Value::Text(val.to_string())
        }
        types::Type::Blob => {
            let val = json
                .as_array()
                .ok_or_else(throw)?
                .iter()
                .map(|v| {
                    let val = v.as_u64().ok_or_else(throw)?;
                    Ok(val as u8)
                })
                .collect::<Result<Vec<u8>>>()?;
            types::Value::Blob(val)
        }
        types::Type::Null => types::Value::Null,
    };
    Ok(val)
}

///
/// Convert a serde_json::Value to a HashMap containing a rusqlite record
/// # Arguments
/// * `json` - the JSON representation of the record
/// * `type_map` - a HashMap containing the column names and their corresponding types
pub fn json_to_val_map(
    type_map: &HashMap<String, types::Type>,
    json: &serde_json::Value,
) -> Result<RecordOwned> {
    let mut map = HashMap::new();
    for (key, tp) in type_map {
        if json[&key].is_null() {
            continue;
        }
        let val = json_to_val(tp, &json[&key])?;
        map.insert(key.to_string(), val);
    }
    Ok(map)
}

pub mod val {
    use rusqlite::types;

    /// Create a text value
    pub fn v_txt(id: &str) -> types::Value {
        types::Value::Text(id.to_string())
    }

    /// Create an integer value
    pub fn v_int(id: i64) -> types::Value {
        types::Value::Integer(id)
    }

    /// Create a float value
    pub fn v_flo(id: f64) -> types::Value {
        types::Value::Real(id)
    }

    /// Create a blob value
    pub fn v_blo(id: &[u8]) -> types::Value {
        types::Value::Blob(id.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::val;
    use crate::shift::val_to_json;

    use rusqlite::types;

    #[test]
    fn test_val_converts() {
        assert_eq!(val::v_txt("test"), types::Value::Text("test".to_string()));
        assert_eq!(val::v_int(1), types::Value::Integer(1));
        assert_eq!(val::v_flo(1.0), types::Value::Real(1.0));
        assert_eq!(val::v_blo(&[1, 2, 3]), types::Value::Blob(vec![1, 2, 3]));
    }

    #[test]
    fn test_json_conversion() -> anyhow::Result<()> {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), types::Value::Integer(1));
        map.insert("name".to_string(), types::Value::Text("test".to_string()));
        map.insert("count".to_string(), types::Value::Integer(2));
        map.insert("statistics".to_string(), types::Value::Real(3.15));
        map.insert("file".to_string(), types::Value::Blob(vec![1, 2, 3]));
        map.insert("joke".to_string(), types::Value::Null);
        let json = val_to_json(&map)?;
        assert_eq!(
            json["id"],
            serde_json::Value::Number(serde_json::Number::from(1))
        );
        assert_eq!(json["name"], serde_json::Value::String("test".to_string()));
        assert_eq!(
            json["count"],
            serde_json::Value::Number(serde_json::Number::from(2))
        );
        assert_eq!(
            json["statistics"],
            serde_json::Value::Number(serde_json::Number::from_f64(3.15).unwrap())
        );
        assert_eq!(
            json.get("file")
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_u64().unwrap())
                .collect::<Vec<u64>>(),
            vec![1, 2, 3]
        );
        assert_eq!(json["joke"], serde_json::Value::Null);
        Ok(())
    }
}
