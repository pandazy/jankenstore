mod convert;
pub mod crud;
mod verify;
pub use convert::val_to_json;
use crud::Bond;
pub use rusqlite::{types, Connection};
pub use serde;
pub use serde_json;

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;
use std::collections::{HashMap, HashSet};

///
/// The UnitResource is a representation of a table in the database
#[derive(Debug, Clone)]
pub struct UnitResource {
    name: String,
    pk_name: String,
    defaults: HashMap<String, types::Value>,
    required_fields: HashSet<String>,
    bonds: Option<HashMap<String, Bond>>,
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
    /// * `bonds` - the relationships between other resources and this resource, see [`Bond`]
    pub fn new(
        name: &str,
        pk_name: &str,
        defaults: &[(&str, types::Value)],
        required_fields: &[&str],
        bonds: Option<&[(&str, Bond)]>,
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
            bonds: bonds.map(|bonds| {
                bonds
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone()))
                    .collect()
            }),
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

    pub fn count_all(
        &self,
        conn: &Connection,
        is_distinct: bool,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<i64> {
        crud::count_all(conn, &self.name, is_distinct, where_input)
    }

    pub fn count_by_pk(
        &self,
        conn: &Connection,
        pk_values: &[&str],
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<i64> {
        crud::count_by_pk(conn, &self.name, &self.pk_name, pk_values, where_input)
    }

    ///
    /// fetch all matching records from the table.
    /// See also [`crud::fetch_all`]
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
        crud::fetch_all(conn, &self.name, is_distinct, display_fields, where_input)
    }

    ///
    /// fetch all matching records from the table and convert them to JSON.
    /// See also [`crud::fetch_all_as`]
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
    /// * `display_fields` - the fields to be displayed in the result
    /// * `where_input` - the where clause and the parameters for the where clause
    pub fn fetch_all_as<T: DeserializeOwned>(
        &self,
        conn: &Connection,
        is_distinct: bool,
        display_fields: Option<&[&str]>,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<Vec<T>> {
        crud::fetch_all_as(conn, &self.name, is_distinct, display_fields, where_input)
    }

    pub fn fetch_by_pk(
        &self,
        conn: &Connection,
        pk_values: &[&str],
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<Vec<HashMap<String, types::Value>>> {
        crud::fetch_by_pk(conn, &self.name, &self.pk_name, pk_values, where_input)
    }

    pub fn fetch_by_pk_as<T: DeserializeOwned>(
        &self,
        conn: &Connection,
        pk_values: &[&str],
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<Vec<T>> {
        crud::fetch_by_pk_as(conn, &self.name, &self.pk_name, pk_values, where_input)
    }

    ///
    /// insert a new record into the table.
    /// See also [`crud::insert`]
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
    /// See also [`crud::update`]
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `pk_value` - the value of the primary key
    /// * `input` - the new values for the record
    /// * `where_input` - the where clause and the parameters for the where clause
    /// # Returns
    /// * `Ok(())` - if the record is updated successfully
    pub fn update_by_pk(
        &self,
        conn: &Connection,
        pk_values: &[&str],
        input: &HashMap<String, types::Value>,
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<()> {
        let schema_info = (self.name.as_str(), &self.defaults, &self.required_fields);
        crud::update_by_pk(
            conn,
            schema_info,
            &self.pk_name,
            pk_values,
            input,
            where_input,
        )
    }

    ///
    /// delete a record from the table
    /// See also [`crud::hard_del`]
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `pk_values` - records to be deleted represented by their primary key values
    /// * `where_input` - the where clause and the parameters for the where clause
    /// # Returns
    /// * `Ok(())` - if the record is deleted successfully
    pub fn hard_del_by_pk(
        &self,
        conn: &Connection,
        pk_values: &[&str],
        where_input: Option<(&str, &[types::Value])>,
    ) -> Result<()> {
        crud::hard_del_by_pk(conn, &self.name, &self.pk_name, pk_values, where_input)
    }
}
