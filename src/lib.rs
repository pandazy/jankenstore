use anyhow::{anyhow, Result};
use rusqlite::{params_from_iter, types, Connection, Row};
use std::collections::{HashMap, HashSet};

fn row_to_map(row: &Row) -> Result<HashMap<String, types::Value>> {
    let mut map = HashMap::new();

    for (i, column_name) in row.as_ref().column_names().iter().enumerate() {
        let value = row.get(i)?;
        map.insert(column_name.to_string(), value);
    }
    Ok(map)
}

fn verify_table_name(table_name: &str) -> Result<()> {
    if table_name.is_empty() {
        return Err(anyhow!("The table name cannot be an empty string"));
    }
    Ok(())
}

fn verify_where_clause(where_clause: &str) -> Result<()> {
    if where_clause.trim().is_empty() {
        return Err(anyhow!(
            "The where clause cannot be an empty string, if you don't want to use a where clause, specify where_input as None"
        ));
    }
    Ok(())
}

fn standardize_where_items(
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

///
/// fetch one record from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `pk` - the primary key and its value, represented as a tuple (pk_name, pk_value)
/// * `where_input` - the where clause and the parameters for the where clause
pub fn fetch_one(
    conn: &Connection,
    table_name: &str,
    pk: (&str, &str),
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Option<HashMap<String, types::Value>>> {
    verify_table_name(table_name)?;
    let (pk_name, pk_value) = pk;
    let sql = format!("SELECT * FROM {} WHERE {} = ?", table_name, pk_name);
    let (where_clause, where_params) = standardize_where_items(where_input, "AND")?;
    let params = [vec![types::Value::Text(pk_value.to_string())], where_params].concat();
    let sql = format!("{} {}", sql, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&params))?;
    let row_op = rows.next()?;
    match row_op {
        Some(row) => {
            let row_record = row_to_map(row)?;
            Ok(Some(row_record))
        }
        None => Ok(None),
    }
}

///
/// fetch all matching records from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `is_distinct` - whether to use the DISTINCT keyword in the SQL query
/// * `display_fields` - the fields to be displayed in the result
/// * `where_input` - the where clause and the parameters for the where clause
pub fn fetch_all(
    conn: &Connection,
    table_name: &str,
    is_distinct: bool,
    display_fields: Option<&[&str]>,
    where_input: Option<(&str, &[types::Value])>,
) -> Result<Vec<HashMap<String, types::Value>>> {
    verify_table_name(table_name)?;
    let default_fields = vec!["*"];
    let display_fields = display_fields.unwrap_or_else(|| &default_fields);
    let distinct_word = if is_distinct { "DISTINCT" } else { "" };
    let sql = format!(
        "SELECT {} {} FROM {}",
        distinct_word,
        display_fields.join(", "),
        table_name
    );
    let (where_clause, where_params) = standardize_where_items(where_input, "WHERE")?;
    let sql = format!("{} {}", sql, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(&where_params))?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(row_to_map(row)?);
    }
    Ok(result)
}

///
/// delete a record from the table
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `pk` - the primary key and its value, represented as a tuple (pk_name, pk_value)
/// * `where_input` - the where clause and the parameters for the where clause
pub fn hard_del(
    conn: &Connection,
    table_name: &str,
    pk: (&str, &str),
    where_input: Option<(&str, &[types::Value])>,
) -> Result<()> {
    verify_table_name(table_name)?;
    let (pk_name, pk_value) = pk;
    let (where_clause, where_params) = standardize_where_items(where_input, "AND")?;
    let params = [
        vec![types::Value::Text(pk_value.to_string())],
        where_params.to_vec(),
    ]
    .concat();
    let sql = format!(
        "DELETE FROM {} WHERE {} = ? {}",
        table_name, pk_name, where_clause
    );
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&params))?;
    Ok(())
}

///
/// The UnitResource is a representation of a table in the database
#[derive(Debug, Clone)]
pub struct UnitResource {
    name: String,
    pk_name: String,
    fields: HashSet<String>,
    required_fields: HashSet<String>,
    defaults: HashMap<String, types::Value>,
}

impl UnitResource {
    ///
    /// create a new UnitResource instance as a representation of a table in the database
    /// # Arguments
    /// * `name` - the name of the table
    /// * `pk_name` - the name of the primary key
    /// * `fields` - the names of the table fields (or columns)
    /// * `required_fields` - the names of the required fields
    /// * `defaults` - the default values for the fields
    pub fn new(
        name: &str,
        pk_name: &str,
        fields: &[&str],
        required_fields: &[&str],
        defaults: &[(&str, types::Value)],
    ) -> Self {
        let mut set_required_fields: HashSet<String> =
            required_fields.iter().map(|f| f.to_string()).collect();
        if !set_required_fields.contains(pk_name) {
            set_required_fields.insert(pk_name.to_string());
        }
        Self {
            name: name.to_string(),
            pk_name: pk_name.to_string(),
            fields: fields.iter().map(|f| f.to_string()).collect(),
            required_fields: set_required_fields,
            defaults: defaults
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_pk_name(&self) -> &str {
        &self.pk_name
    }

    pub fn get_fields(&self) -> &HashSet<String> {
        &self.fields
    }

    pub fn get_required_fields(&self) -> &HashSet<String> {
        &self.required_fields
    }

    pub fn get_defaults(&self) -> &HashMap<String, types::Value> {
        &self.defaults
    }

    ///
    /// verify the input for the operation of the resource, make sure it's a valid payload
    /// the input can be used for insert a new record or update an existing record
    /// the input must not be empty and must not contain any key that is not allowed
    pub fn verify_op_basic(&self, input: &HashMap<String, types::Value>) -> Result<()> {
        if input.keys().len() == 0 {
            return Err(anyhow!(
                "The input for the operation of {} has no items",
                self.get_name()
            ));
        }
        let trespasser_option = input
            .keys()
            .find(|key| !self.fields.contains(&key.to_string()));
        if let Some(trespasser) = trespasser_option {
            return Err(anyhow!(
                "The input for the operation of table '{}' has a key '{}' that is not allowed",
                self.get_name(),
                trespasser
            ));
        }
        Ok(())
    }

    pub fn is_absent(&self, input: &HashMap<String, types::Value>, key: &str) -> bool {
        input.get(key).is_none() || input.get(key) == Some(&types::Value::Null)
    }

    ///
    /// verify the presence of required fields of the input for the operation of the resource
    /// # Arguments
    /// * `input` - the input for the operation
    /// * `all_required` - whether all required fields are needed, if false,
    ///   only the required fields that are present in the input are checked,
    ///   for example, false is used for the update operation, true is used for the insert operation
    pub fn verify_op_required(
        &self,
        input: &HashMap<String, types::Value>,
        all_required: bool,
    ) -> Result<()> {
        self.verify_op_basic(input)?;
        let first_none = if all_required {
            self.required_fields
                .iter()
                .find(|required_field| self.is_absent(input, required_field))
        } else {
            input.keys().find(|key| {
                self.required_fields.contains(&key.to_string()) && self.is_absent(input, key)
            })
        };
        if let Some(invalid) = first_none {
            return Err(anyhow!(
                "The input for the operation of {} requires the value of '{}'",
                self.name,
                invalid
            ));
        }
        Ok(())
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
    /// Make a record based on an input,
    /// if a field is absent in the input, the default value is used if available
    pub fn defaults_if_absent(
        &self,
        input: &HashMap<String, types::Value>,
    ) -> HashMap<String, types::Value> {
        let mut ret = self.defaults.clone();
        for (key, value) in input {
            ret.insert(key.clone(), value.clone());
        }
        ret
    }

    ///
    /// insert a new record into the table
    ///
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `input` - the new record to be inserted
    pub fn insert(&self, conn: &Connection, input: &HashMap<String, types::Value>) -> Result<()> {
        let input = self.defaults_if_absent(input);
        self.verify_op_required(&input, true)?;
        let mut params = vec![];
        let mut columns = vec![];
        let mut values = vec![];
        for (key, value) in input {
            columns.push(key.clone());
            values.push("?");
            params.push(value.clone());
        }
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.name,
            columns.join(", "),
            values.join(", ")
        );
        conn.execute(&sql, params_from_iter(&params))?;
        Ok(())
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
        self.verify_op_required(input, false)?;
        let mut set_clause = vec![];
        let mut set_params = vec![];
        for (key, value) in input {
            set_clause.push(format!("{} = ?", key));
            set_params.push(value.clone());
        }
        let (where_clause, where_params) = standardize_where_items(where_input, "AND")?;
        let params = [
            set_params,
            vec![types::Value::Text(pk_value.to_string())],
            where_params,
        ]
        .concat();
        let sql = format!(
            "UPDATE {} SET {} where {}=? {}",
            self.name,
            set_clause.join(", "),
            self.pk_name,
            where_clause
        );
        let mut stmt = conn.prepare(&sql)?;
        stmt.execute(params_from_iter(&params))?;
        Ok(())
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
        hard_del(conn, &self.name, (&self.pk_name, pk_value), where_input)
    }
}
