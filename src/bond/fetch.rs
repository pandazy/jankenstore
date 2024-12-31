use std::collections::HashMap;

use rusqlite::{types, Connection};
use serde::de::DeserializeOwned;
use serde_json::from_value;

use crate::crud::{fetch, shift, sql};

fn get_peer_matching_clause(
    rel_name: &str,
    fk_name: &str,
    (my_name, my_pk_name): (&str, &str),
    bond_matching_clause: &str,
) -> String {
    let link_condition = format!("{} = {}.{}", fk_name, my_name, my_pk_name);
    format!(
        "EXISTS (SELECT 1 FROM {} WHERE {} AND {})",
        rel_name, link_condition, bond_matching_clause
    )
}

///
/// fetch all matching records from the child table
/// where the parent record is in the given list
/// # Arguments
///
/// * `conn` - the Rusqlite connection to the database
/// * `child_table_name` - the name of the child table (n in n-1)
/// * `parent_config` - the parent-related table matching settings (1 in n-1)
///                     - `tuple(column_name_of_parent_in_child_node_table, key_values_of_the_parent_nodes)`
///                       (it can return results that belong to multiple parent nodes)
/// * `display_fields` - the fields to be displayed in the result
/// * `where_q_config` - the where clause and the parameters for condition matching
pub fn list_n_of_1(
    conn: &Connection,
    child_table_name: &str,
    parent_config: (&str, &[types::Value]),
    display_fields: Option<&[&str]>,
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
    let (parent_col, parents) = parent_config;
    let (bond_matching_clause, bond_matching_params) = sql::in_them(parent_col, parents);
    let bond_match_refs = (
        bond_matching_clause.as_str(),
        bond_matching_params.as_slice(),
    );
    let (where_clause, where_params) =
        sql::merge_q_configs(Some(bond_match_refs), where_q_config, "AND")?;
    let result = fetch::f_all(
        conn,
        child_table_name,
        Some((where_clause.as_str(), &where_params)),
        (false, display_fields),
    )?;
    Ok(result)
}

pub fn list_n_of_1_as<T: DeserializeOwned>(
    conn: &Connection,
    child_table: &str,
    parent_config: (&str, &[types::Value]),
    d_fields: Option<&[&str]>,
    where_conf: Option<(&str, &[types::Value])>,
) -> anyhow::Result<Vec<T>> {
    let result = list_n_of_1(conn, child_table, parent_config, d_fields, where_conf)?;
    let mut result_as = Vec::new();
    for row in result {
        result_as.push(from_value(shift::val_to_json(&row)?)?);
    }
    Ok(result_as)
}

///
/// fetch all matching records from the main table
/// where the related records are in the given list.
/// They are related by a n-n relationship.
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `main_info_config` - the main table matching settings
///                        (the table that will contain the related details together with keys)
///                      - `tuple(main_table_name, main_table_primary_key_column_name, column_name_in_rel_table)`
/// * `rel_config` - the relationship table matching settings
///                  - `tuple(rel_table_name, rel_table_column_name_of_the_related_peer, key_values_of_the_related_peers)`
/// * `display_fields` - the fields to be displayed in the result
/// * `where_q_config` - the where clause and the parameters for condition matching
pub fn list_n_of_n(
    conn: &Connection,
    main_info_config: (&str, &str, &str),
    rel_config: (&str, &str, &[types::Value]),
    display_fields: Option<&[&str]>,
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
    let (main_table, main_pk_name, main_col_in_rel) = main_info_config;
    let (rel_name, rel_peer_col, related_to) = rel_config;
    let (bond_matching_clause, bond_matching_params) = sql::in_them(rel_peer_col, related_to);
    let (bond_matching_clause, bond_matching_params) = (
        get_peer_matching_clause(
            rel_name,
            main_col_in_rel,
            (main_table, main_pk_name),
            bond_matching_clause.as_str(),
        ),
        bond_matching_params,
    );
    let (where_clause, where_params) = sql::merge_q_configs(
        Some((
            bond_matching_clause.as_str(),
            bond_matching_params.as_slice(),
        )),
        where_q_config,
        "AND",
    )?;
    let result = fetch::f_all(
        conn,
        main_table,
        Some((where_clause.as_str(), &where_params)),
        (false, display_fields),
    )?;
    Ok(result)
}

pub fn list_n_of_n_as<T: DeserializeOwned>(
    conn: &Connection,
    info_config: (&str, &str, &str),
    rel_config: (&str, &str, &[types::Value]),
    d_fields: Option<&[&str]>,
    where_conf: Option<(&str, &[types::Value])>,
) -> anyhow::Result<Vec<T>> {
    let result = list_n_of_n(conn, info_config, rel_config, d_fields, where_conf)?;
    let mut result_as = Vec::new();
    for row in result {
        result_as.push(from_value(shift::val_to_json(&row)?)?);
    }
    Ok(result_as)
}
