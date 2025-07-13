use super::shift::val_to_json;

use anyhow::Result;
use rusqlite::{types, Connection};
use serde_json::{json, Value};

use std::collections::{HashMap, HashSet};

///
/// The data column types that can be used for client side labeling
fn get_type_display(t: &types::Type) -> String {
    match t {
        types::Type::Integer => "INTEGER",
        types::Type::Real => "REAL",
        types::Type::Text => "TEXT",
        types::Type::Blob => "BLOB",
        _ => "NULL",
    }
    .to_string()
}

///
/// Convert database value type label to the corresponding type
fn get_type_from_str(t: &str) -> types::Type {
    match t.trim().to_uppercase().as_str() {
        "INTEGER" => types::Type::Integer,
        "REAL" => types::Type::Real,
        "TEXT" => types::Type::Text,
        "BLOB" => types::Type::Blob,
        _ => types::Type::Null,
    }
}

///
/// The Schema struct represents the schema of a table in the database
/// # Fields
/// * `name` - the name of the table
/// * `pk` - the name of the primary key
///   - currently only single primary key is supported
/// * `required_fields` - the names of the required fields (especially needed in write operations),
///   it includes 2 cases:
///   - the field is required (cannot be NULL)
///   - the field is pk (primary key)
/// * `types` - the data types of the columns in the table
/// * `defaults` - the default values for the columns in the table
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub name: String,
    pub pk: String,
    pub required_fields: HashSet<String>,
    pub types: HashMap<String, types::Type>,
    pub defaults: HashMap<String, types::Value>,
}

impl Schema {
    /// Check if a given column is defined by the schema
    pub fn find_unknown_field(&self, challenges: &[&str]) -> Option<String> {
        challenges
            .iter()
            .find(|f| !self.types.contains_key(**f))
            .map(|s| s.to_string())
    }

    ///
    /// create a new Schema instance as a representation of a table in the database
    /// which can be consumed by clients such as web applications
    pub fn json(&self) -> anyhow::Result<Value> {
        let Schema {
            name,
            pk,
            required_fields,
            defaults,
            types,
        } = self;
        let defaults = val_to_json(
            &defaults
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )?;
        let types_display = types
            .iter()
            .map(|(k, v)| (k.to_string(), get_type_display(v)))
            .collect::<HashMap<String, String>>();
        Ok(json!({
            "name": name,
            "pk": pk,
            "types": types_display,
            "requiredFields": json!(required_fields),
            "defaults": json!(defaults),
        }))
    }
}

///
/// The SchemaFamily struct represents a family of schema information in the database
/// It will be used to verify CRUD operations to improve data integrity
/// # Fields
/// * `map` - a map of table names to their corresponding schema
/// * `parents` - a map of table names to their parents
///   - key: child table name
///   - value: parent table name(s) of the key table
/// * `children` - a map of table names to their children
///   - key: parent table name
///   - value: child table name(s) of the key table
/// * `peers` - a map of peer tables (n-n relationship)
///   - key: table name
///   - value: peer table name(s) of the key table
/// * `peer_link_tables` - a map of tables that saves the relationship between the peer tables
///   - key: peer table name
///   - value: the relationship table name
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaFamily {
    pub map: HashMap<String, Schema>,
    pub parents: HashMap<String, HashSet<String>>,
    pub children: HashMap<String, HashSet<String>>,
    pub peers: HashMap<String, HashSet<String>>,
    pub peer_link_tables: HashMap<String, String>,
}

impl SchemaFamily {
    ///
    /// fetch the schema family of the connected database. See [fetch_schema_family]
    /// # Returns
    pub fn fetch(
        conn: &Connection,
        excluded_tables: &[&str],
        peer_prefix: &str,
        peer_splitter: &str,
    ) -> anyhow::Result<Self> {
        fetch_schema_family(conn, excluded_tables, peer_prefix, peer_splitter)
    }

    ///
    /// create a new SchemaFamily instance as a representation of a family of schema information in the database
    /// which can be consumed by clients such as web applications
    pub fn json(&self) -> anyhow::Result<Value> {
        let mut schemas = HashMap::new();
        for (name, schema) in &self.map {
            schemas.insert(name.to_string(), schema.json()?);
        }
        let parents = json!(self.parents);
        let peers = json!(self.peers);
        let children = json!(self.children);

        let family = json!({
            "map": schemas,
            "parents": parents,
            "peers": peers,
            "children": children,
        });
        Ok(family)
    }

    ///
    /// get the schema of a table by its name
    /// # Arguments
    /// * `table_name` - the name of the table that may or may not exist in the schema family
    pub fn try_get_schema(&self, table_name: &str) -> anyhow::Result<&Schema> {
        let schema = self.map.get(table_name);
        match schema {
            Some(schema) => Ok(schema),
            None => Err(anyhow::anyhow!(
                "Table '{}' not found in schema family. \nAvailable tables are: {}",
                table_name,
                {
                    let mut keys = self.map.keys().map(|s| s.as_str()).collect::<Vec<&str>>();
                    keys.sort();
                    keys.join(", ")
                }
            )),
        }
    }

    ///
    /// verify the validity of the parent-child relationship
    pub fn verify_child_of(&self, child_name: &str, parent_name: &str) -> anyhow::Result<()> {
        let parents = self.parents.get(child_name);
        let is_right_parenthood = match parents {
            Some(parents) => parents.contains(parent_name),
            None => false,
        };
        if !is_right_parenthood {
            return Err(anyhow::anyhow!(
                "Table '{}' is not a child of '{}'. \nAvailable parent tables are {:?}",
                child_name,
                parent_name,
                self.get_parents_of(child_name)
            ));
        }
        Ok(())
    }

    ///
    /// get the relationship table name of the given peer table
    pub fn try_get_peer_link_table_of(&self, table_name: &str) -> anyhow::Result<&str> {
        self.peer_link_tables
            .get(table_name)
            .map(|s| s.as_str())
            .ok_or(anyhow::anyhow!(
                "Table '{}' does not have peers defined",
                table_name
            ))
    }

    ///
    /// verify the validity of the peer-peer relationship
    pub fn verify_peer_of(&self, peer1_name: &str, peer2_name: &str) -> anyhow::Result<()> {
        let default_peers = HashSet::new();
        let peers1 = self.peers.get(peer1_name).unwrap_or(&default_peers);
        if !peers1.contains(peer2_name) {
            return Err(anyhow::anyhow!(
                "Table '{}' is not a peer of '{}'. \nAvailable peer tables of '{}' are {:?}",
                peer1_name,
                peer2_name,
                peer1_name,
                peers1
            ));
        }
        Ok(())
    }

    ///
    /// get the parent tables of a child table
    pub fn get_parents_of(&self, child_name: &str) -> Vec<&str> {
        let parents = self.parents.get(child_name);
        match parents {
            Some(parents) => parents.iter().map(|s| s.as_str()).collect(),
            None => vec![],
        }
    }
}

const TABLE_READ_QUERY: &str = r#"
 SELECT name FROM sqlite_master
 WHERE type='table' AND name NOT LIKE 'sqlite_%'
 %(condition)s
 ORDER BY name;
"#;

const COLUMN_READ_QUERY: &str = "PRAGMA table_info(%(table_name)s);";

///
/// The ColumnMeta struct represents the metadata of a column in the database
/// # Fields
/// * `name` - the name of the column
/// * `col_type` - the data type of the column
/// * `is_required` - whether the column is required (cannot be NULL)
/// * `default` - the default value of the column
/// * `is_pk` - whether the column is a primary key
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMeta {
    /// the name of the column
    pub name: String,

    /// The data type of the column
    pub col_type: types::Type,

    ///
    /// whether the column is required (cannot be NULL), it includes 3 cases:
    /// - explicitly marked as required
    /// - primary key
    /// - ends with '_id' which indicates whose parent this record belongs to
    pub is_required: bool,

    /// the default value of the column
    pub default: types::Value,

    /// whether the column is a primary key
    /// - currently only single primary key is supported
    pub is_pk: bool,
}

///
/// convert the column metadata items to a schema
/// # Arguments
/// * `table_name` - the name of the table
/// * `column_meta` - the metadata of the columns in the table
pub fn column_meta_items_to_schema(
    table_name: &str,
    column_meta: &HashMap<String, ColumnMeta>,
) -> anyhow::Result<Schema> {
    let mut required_fields = HashSet::new();
    let mut defaults = HashMap::new();
    let mut types = HashMap::new();
    let mut pk = "".to_string();
    for (name, meta) in column_meta {
        if meta.is_required || meta.is_pk || name.ends_with("_id") {
            required_fields.insert(name.clone());
        }
        if meta.is_pk {
            pk = name.clone();
        }
        defaults.insert(name.to_string(), meta.default.clone());
        types.insert(name.clone(), meta.col_type);
    }
    Ok(Schema {
        name: table_name.to_string(),
        pk,
        required_fields,
        types,
        defaults,
    })
}

fn get_default_db_value(col_type: types::Type) -> types::Value {
    match col_type {
        types::Type::Integer => types::Value::Integer(0),
        types::Type::Real => types::Value::Real(0.0),
        types::Type::Text => types::Value::Text("".to_string()),
        types::Type::Blob => types::Value::Blob(vec![]),
        _ => types::Value::Null,
    }
}

///
/// get the metadata of the columns in a table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table` - the name of the table
pub fn get_columns_meta(
    conn: &Connection,
    table: &str,
) -> anyhow::Result<HashMap<String, ColumnMeta>> {
    let sql = COLUMN_READ_QUERY.replace("%(table_name)s", table);
    let mut stmt = conn.prepare(sql.as_str())?;
    let mut rows = stmt.query([])?;
    let mut results = HashMap::new();
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        let col_type: String = row.get(2)?;
        let col_type = get_type_from_str(col_type.as_str());
        // NULL type leads to definition gap which needs further investigation
        if col_type == types::Type::Null {
            return Err(anyhow::anyhow!(
                "Invalid type: '{}' for the column '{}@{}'",
                col_type,
                name,
                table
            ));
        }
        let is_required: bool = row.get(3)?;
        let default: types::Value = row.get(4)?;
        let default = match &default {
            types::Value::Text(text) => {
                // correct empty text fetched from metadata
                if text.is_empty() || text == "''" || text == r#""""# {
                    types::Value::Text("".to_string())
                } else {
                    default.clone()
                }
            }
            _ => default.clone(),
        };
        // if the default value is NULL, replace it with a default value of the corresponding type
        // to avoid unnecessary ambiguity
        let default = match default {
            types::Value::Null => get_default_db_value(col_type),
            _ => default,
        };
        let is_pk: bool = row.get(5)?;
        let meta = ColumnMeta {
            name: name.clone(),
            col_type,
            is_required,
            default,
            is_pk,
        };
        results.insert(name, meta);
    }
    Ok(results)
}

const DEFAULT_PEER_PREFIX: &str = "rel";

const DEFAULT_PEER_SPLITTER: &str = "_";

fn get_peer_table_name_tips(peer_prefix: &str, peer_splitter: &str) -> String {
    format!(
        "If you do not expect this table to represent a peer relationship, please rename it to a different one, so it doesn't start with '{peer_prefix}{peer_splitter}'"
    )
}

///
/// get the names of the peer tables from the relationship table name
/// # Arguments
/// * `table_name` - the name of the relationship table
/// * `peer_prefix` - the prefix for the peer tables (default is [DEFAULT_PEER_PREFIX])
/// * `peer_splitter` - the splitter for the peer tables (default is [DEFAULT_PEER_SPLITTER])
/// # Returns
/// * a tuple of the peer table names e.g., (table1, table2), the order follows table name
fn get_peer_names(
    pk_name_map: &HashMap<String, String>,
    table_name: &str,
    peer_prefix: &str,
    peer_splitter: &str,
    columns: &HashMap<String, ColumnMeta>,
) -> anyhow::Result<(String, String)> {
    let peer_name_section = table_name
        .split(peer_splitter)
        .collect::<Vec<&str>>()
        .iter()
        .skip(1)
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    if peer_name_section.len() != 2 {
        return Err(anyhow::anyhow!(
          "Invalid peer table name '{}', it should be in the format of '{}', with exactly 2 tables. \n{}",
          table_name, [peer_prefix, "table1", "table2"].join(peer_splitter), get_peer_table_name_tips(peer_prefix, peer_splitter)
      ));
    }

    for p_name in [&peer_name_section[0], &peer_name_section[1]] {
        if let Some(pk_name) = pk_name_map.get(p_name) {
            let fk_name = format!("{p_name}_{pk_name}");
            if !columns.contains_key(fk_name.as_str()) {
                return Err(anyhow::anyhow!(
                    "Table '{}' is missing the peer foreign-key column: '{}'\n{}",
                    table_name,
                    fk_name,
                    get_peer_table_name_tips(peer_prefix, peer_splitter)
                ));
            }
        }
    }

    Ok((
        peer_name_section[0].to_owned(),
        peer_name_section[1].to_owned(),
    ))
}

type SchemaMetadata = HashMap<String, (Schema, HashMap<String, ColumnMeta>)>;

fn extract_schema_metadata(conn: &Connection, excluded_tables: &[&str]) -> Result<SchemaMetadata> {
    let excludes = excluded_tables
        .iter()
        .map(|name| format!("AND name NOT LIKE '{}'", name.trim()))
        .collect::<Vec<String>>()
        .join(" ");
    let query = TABLE_READ_QUERY.replace("%(condition)s", &excludes);
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query([])?;
    let mut map = HashMap::new();
    while let Some(row) = rows.next()? {
        let table_name = row.get::<usize, String>(0)?.to_owned();
        let columns = get_columns_meta(conn, table_name.as_str())?;
        let schema = column_meta_items_to_schema(table_name.as_str(), &columns)?;
        map.insert(table_name.clone(), (schema, columns));
    }
    Ok(map)
}

///
/// get the schema family of the database.
/// # WARNING
/// Rusqlite's param binding is unable to be applied to `PRAGMA` command. (See [COLUMN_READ_QUERY])
/// To avoid potential security risk, don't use this in functions that are subject client requests.
/// For example, it's ok to use it during the server-side app initialization, because it's not exposed to the client.
/// But don't dynamically fetch the schema family based on user input.
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `excluded_tables` - the tables to be excluded from the schema family
/// * `peer_prefix` - the prefix for sibling tables (default is [DEFAULT_PEER_PREFIX]),
///   sibling maps will be automatically generated based on this prefix
/// * `peer_splitter` - the splitter for sibling tables from each relationship table (default is [DEFAULT_PEER_SPLITTER]
pub fn fetch_schema_family(
    conn: &Connection,
    excluded_tables: &[&str],
    peer_prefix: &str,
    peer_splitter: &str,
) -> anyhow::Result<SchemaFamily> {
    let peer_prefix = if peer_prefix.trim().is_empty() {
        DEFAULT_PEER_PREFIX
    } else {
        peer_prefix
    };
    let peer_splitter = if peer_splitter.trim().is_empty() {
        DEFAULT_PEER_SPLITTER
    } else {
        peer_splitter
    };
    let schema_metadata = extract_schema_metadata(conn, excluded_tables)?;
    let all_pk_name_map = schema_metadata
        .iter()
        .map(|(name, (schema, _))| (name.clone(), schema.pk.clone()))
        .collect::<HashMap<String, String>>();
    let mut map = HashMap::new();
    let mut peers = HashMap::new();
    let mut peer_pair_candidates = vec![];
    let mut peer_tables = HashMap::new();
    let mut parents = HashMap::new();
    let mut children = HashMap::new();
    let mut parent_candidates = vec![];
    let mut possible_fks = HashMap::new();
    let mut column_map = HashMap::new();
    let is_peer_link = |table_name: &str| table_name.starts_with(peer_prefix);
    let mut all_pk_name = HashMap::new();
    for (table, (schema, columns)) in &schema_metadata {
        all_pk_name.insert(table.clone(), schema.pk.clone());
        map.insert(table.clone(), schema.clone());
        if is_peer_link(table.as_str()) {
            let (p1, p2) = get_peer_names(
                &all_pk_name_map,
                table.as_str(),
                peer_prefix,
                peer_splitter,
                columns,
            )?;
            peer_tables.insert(p1.clone(), table.clone());
            peer_tables.insert(p2.clone(), table.clone());
            peer_pair_candidates.push((p1.to_owned(), p2.to_owned(), table.clone()));
            continue;
        }
        column_map.insert(table.clone(), columns.clone());
        let pk_type = *schema
            .types
            .get(schema.pk.as_str())
            .unwrap_or(&types::Type::Null);
        possible_fks.insert(format!("{}_{}", table, schema.pk), (table.clone(), pk_type));
    }
    for child_table in map.keys() {
        if let Some(column) = column_map.get(child_table) {
            for ColumnMeta {
                name: fk_col_name,
                col_type,
                ..
            } in column.values()
            {
                if let Some((parent_table, expected_type)) = possible_fks.get(fk_col_name) {
                    if col_type != expected_type {
                        return Err(anyhow::anyhow!(
                            "The '{}'@'{}' is expected to be a foreign key to table '{}' with the type of '{}', but it's actually '{}'. \n{}",
                            fk_col_name,
                            child_table,
                            parent_table,
                            expected_type,
                            col_type,
                            "Please check the column type and the primary key type of the parent table and fix them first"
                        ));
                    }
                    parent_candidates.push((
                        parent_table.clone(),
                        child_table.clone(),
                        fk_col_name.clone(),
                    ));
                }
            }
        }
    }
    for (p1, p2, table_name) in peer_pair_candidates {
        for p_name in &[&p1, &p2] {
            if !map.contains_key(*p_name) {
                return Err(anyhow::anyhow!(
                    "Table '{}' does not exist, but it's specified by the peer-relationship table '{}'\n{}",
                    *p_name,
                    table_name,
                    get_peer_table_name_tips(peer_prefix, peer_splitter)
                ));
            }
        }
        let p1_peers = peers.entry(p1.clone()).or_insert_with(HashSet::new);
        p1_peers.insert(p2.clone());
        let p2_peers = peers.entry(p2.clone()).or_insert_with(HashSet::new);
        p2_peers.insert(p1.clone());
    }
    for (parent_name, child_name, context_column) in parent_candidates {
        if !map.contains_key(&parent_name) {
            return Err(anyhow::anyhow!(
                "Table '{}' which is parent of '{}' does not exist, but it's specified by '{}@{}'",
                parent_name,
                child_name,
                child_name,
                context_column
            ));
        }
        let current_parents = parents
            .entry(child_name.clone())
            .or_insert_with(HashSet::new);
        current_parents.insert(parent_name.clone());
        let current_children = children
            .entry(parent_name.clone())
            .or_insert_with(HashSet::new);
        current_children.insert(child_name.clone());
    }
    Ok(SchemaFamily {
        map,
        parents,
        peers,
        children,
        peer_link_tables: peer_tables,
    })
}

#[cfg(test)]
mod tests {
    use crate::sqlite::schema::get_default_db_value;

    use rusqlite::types;

    #[test]
    fn test_uncovered_types() {
        assert_eq!(super::get_type_display(&types::Type::Null), "NULL");

        assert_eq!(get_default_db_value(types::Type::Null), types::Value::Null)
    }
}
