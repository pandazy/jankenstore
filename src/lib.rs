mod convert;
pub mod crud;
mod verify;
pub use convert::val_to_json;

use anyhow::{anyhow, Result};
use crud::{fetch_all, fetch_one};
use rusqlite::{types, Connection};
use std::collections::{HashMap, HashSet};

///
/// The UnitResource is a representation of a table in the database
#[derive(Debug, Clone)]
pub struct UnitResource {
    name: String,
    pk_name: String,
    defaults: HashMap<String, types::Value>,
    required_fields: HashSet<String>,
}

impl UnitResource {
    ///
    /// create a new UnitResource instance as a representation of a table in the database
    /// # Arguments
    /// * `name` - the name of the table
    /// * `pk_name` - the name of the primary key
    /// * `defaults` - the default values for all the columns in the table,
    ///     * this field is used for mainly 2 purposes:
    ///        * provide definitions of all columns and their data types
    ///        * provide default values for write operations (insert, update)
    ///     * **each column must have a default value**
    ///     * the default value can't be Value::Null because it does not clearly indicate the data type
    /// * `required_fields` - the names of the required fields
    pub fn new(
        name: &str,
        pk_name: &str,
        defaults: &[(&str, types::Value)],
        required_fields: &[&str],
    ) -> Result<Self> {
        let mut set_required_fields: HashSet<String> =
            required_fields.iter().map(|f| f.to_string()).collect();
        if !set_required_fields.contains(pk_name) {
            set_required_fields.insert(pk_name.to_string());
        }
        let first_null = defaults
            .iter()
            .find(|(_, v)| matches!(v, types::Value::Null));
        if let Some((k, _)) = first_null {
            return Err(anyhow!(
                "(table: {}) The default value for the column '{}' cannot be Value::Null, please specify an empty value of the corresponding type",
                name,
                k
            ));
        }
        Ok(Self {
            name: name.to_string(),
            pk_name: pk_name.to_string(),
            required_fields: set_required_fields,
            defaults: defaults
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        })
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_pk_name(&self) -> &str {
        &self.pk_name
    }

    pub fn get_required_fields(&self) -> &HashSet<String> {
        &self.required_fields
    }

    pub fn get_defaults(&self) -> &HashMap<String, types::Value> {
        &self.defaults
    }

    ///
    /// fetch one record from the table
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `pk_value` - the value of the primary key
    /// * `where_input` - the where clause and the parameters for the where clause
    /// # Returns
    /// * `Ok(Some(row_record))` - if the record is found, represented by a HashMap with field names as keys
    pub fn fetch_one(
        &self,
        conn: &Connection,
        pk_value: &str,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<Option<HashMap<String, types::Value>>> {
        fetch_one(conn, &self.name, (&self.pk_name, pk_value), where_input)
    }

    ///
    /// fetch one record from the table and convert it to JSON
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `pk_value` - the value of the primary key
    /// * `where_input` - the where clause and the parameters for the where clause
    pub fn fetch_one_json(
        &self,
        conn: &Connection,
        pk_value: &str,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<Option<serde_json::Value>> {
        let row = self.fetch_one(conn, pk_value, where_input)?;
        match row {
            Some(row) => Ok(Some(val_to_json(&row)?)),
            None => Ok(None),
        }
    }

    ///
    /// fetch all matching records from the table
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
    /// * `display_fields` - the fields to be displayed in the result
    /// * `where_input` - the where clause and the parameters for the where clause
    /// # Returns
    /// * `Ok(Vec<row_records>)` - if the records are found, represented by a Vec of HashMaps with field names as keys
    pub fn fetch_all(
        &self,
        conn: &Connection,
        is_distinct: bool,
        display_fields: Option<&[&str]>,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<Vec<HashMap<String, types::Value>>> {
        fetch_all(conn, &self.name, is_distinct, display_fields, where_input)
    }

    ///
    /// fetch all matching records from the table and convert them to JSON
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
    /// * `display_fields` - the fields to be displayed in the result
    /// * `where_input` - the where clause and the parameters for the where clause
    pub fn fetch_all_json(
        &self,
        conn: &Connection,
        is_distinct: bool,
        display_fields: Option<&[&str]>,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<Vec<serde_json::Value>> {
        let rows = self.fetch_all(conn, is_distinct, display_fields, where_input)?;
        rows.iter().map(val_to_json).collect()
    }

    ///
    /// insert a new record into the table
    ///
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `input` - the new record to be inserted
    pub fn insert(
        &self,
        conn: &Connection,
        input: &HashMap<String, types::Value>,
        default_if_absent: bool,
    ) -> Result<()> {
        let schema_info = (self.name.as_str(), &self.defaults, &self.required_fields);
        crud::insert(conn, schema_info, input, default_if_absent)
    }

    ///
    /// update an existing record in the table
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `pk_value` - the value of the primary key
    /// * `input` - the new values for the record
    /// * `where_input` - the where clause and the parameters for the where clause
    /// # Returns
    /// * `Ok(())` - if the record is updated successfully
    pub fn update(
        &self,
        conn: &Connection,
        pk_value: &str,
        input: &HashMap<String, types::Value>,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<()> {
        let schema_info = (self.name.as_str(), &self.defaults, &self.required_fields);
        let pk = (self.pk_name.as_str(), pk_value);
        crud::update(conn, schema_info, pk, input, where_input)
    }

    ///
    /// delete a record from the table
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `pk_value` - the value of the primary key
    /// * `where_input` - the where clause and the parameters for the where clause
    /// # Returns
    /// * `Ok(())` - if the record is deleted successfully
    pub fn hard_del(
        &self,
        conn: &Connection,
        pk_value: &str,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<()> {
        crud::hard_del(conn, &self.name, (&self.pk_name, pk_value), where_input)
    }
}
