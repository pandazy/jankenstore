use super::{
    sql::{self, WhereConfig},
    verify::{verify_table_name, verify_values_required},
};

use rusqlite::{params_from_iter, types, Connection};

///
/// delete all matching records from the table that meet the conditions.
///
/// # Arguments
///
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `where_q_config` - the where clause and the parameters for the where clause,
///                      to reduce the chance of unwanted deletions,
///                      this is not an Option and cannot contain empty clause
/// # Returns
pub fn d_all(
    conn: &Connection,
    table_name: &str,
    where_q_config: WhereConfig,
) -> anyhow::Result<()> {
    verify_table_name(table_name)?;
    let (where_clause, where_params) = sql::standardize_q_config(Some(where_q_config), "WHERE")?;
    let sql = format!("DELETE FROM {} {}", table_name, where_clause);
    let mut stmt = conn.prepare(&sql)?;
    stmt.execute(params_from_iter(&where_params))?;
    Ok(())
}

///
/// delete a record from the table by looking for its primary keys
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `table_name` - the name of the table
/// * `pk_name` - the name of the primary key
/// * `pk_values` - records to be deleted represented by their primary key values
/// * `where_q_config` - the extra where clause and the parameters for
///                   the where clause apart from the primary key values
pub fn d_by_pk(
    conn: &Connection,
    table_name: &str,
    pk_name: &str,
    pk_values: &[types::Value],
    where_q_config: Option<WhereConfig>,
) -> anyhow::Result<()> {
    verify_values_required(pk_values, table_name, pk_name)?;
    verify_table_name(table_name)?;
    let (pk_query_clause, pk_query_params) = sql::in_them(pk_name, pk_values);
    let pk_where_refs = (pk_query_clause.as_str(), pk_query_params.as_slice());
    let (where_clause, where_params) =
        sql::merge_q_configs(Some(pk_where_refs), where_q_config, "AND")?;
    d_all(conn, table_name, (where_clause.as_str(), &where_params))?;
    Ok(())
}
