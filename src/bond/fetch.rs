use std::collections::HashMap;

use rusqlite::{types, Connection};

use crate::crud::{fetch, sql};

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

pub fn list_n_of_1(
    conn: &Connection,
    table_name: &str,
    fk_name: &str,
    belongs_to: &[types::Value],
    display_fields: Option<&[&str]>,
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
    let (bond_matching_clause, bond_matching_params) = sql::in_them(fk_name, belongs_to);
    let bond_match_refs = (
        bond_matching_clause.as_str(),
        bond_matching_params.as_slice(),
    );
    let (where_clause, where_params) =
        sql::merge_q_configs(Some(bond_match_refs), where_q_config, "AND")?;
    let result = fetch::f_all(
        conn,
        table_name,
        Some((where_clause.as_str(), &where_params)),
        (false, display_fields),
    )?;
    Ok(result)
}

pub fn list_n_of_n(
    conn: &Connection,
    (main_table, main_pk_name, main_fk_in_rel): (&str, &str, &str),
    (rel_name, peer_fk_name): (&str, &str),
    n_peers: &[types::Value],
    display_fields: Option<&[&str]>,
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
    let (bond_matching_clause, bond_matching_params) = sql::in_them(peer_fk_name, n_peers);
    let (bond_matching_clause, bond_matching_params) = (
        get_peer_matching_clause(
            rel_name,
            main_fk_in_rel,
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
