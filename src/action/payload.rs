use serde_json::Value as JsonValue;

pub type KeyListPair = (String, Vec<JsonValue>);
pub type KeyValuePair = (String, JsonValue);
pub type KeyAndKeyListPair = (String, Vec<KeyListPair>);
pub type KeyAndKeyValuePair = (String, KeyValuePair);
