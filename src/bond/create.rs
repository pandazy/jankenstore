use std::collections::{HashMap, HashSet};

use rusqlite::{types, Connection};

use crate::crud::{
    create,
    verify::{get_verified_write_inputs, verify_values_required},
};

use super::relink;

pub fn n1(
    conn: &Connection,
    table_name: &str,
    (p_col_name, p_val): (&str, &types::Value),
    input: &HashMap<String, types::Value>,
    verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
) -> anyhow::Result<()> {
    let fk_val = p_val.clone();
    verify_values_required(&[fk_val.clone()], table_name, p_col_name)?;
    let mut verified_input =
        get_verified_write_inputs(true, table_name, input, verification_options)?;
    verified_input.insert(p_col_name.to_string(), fk_val);
    create::i_one(conn, table_name, &verified_input, None)?;
    Ok(())
}

pub fn nn(
    conn: &Connection,
    input: &HashMap<String, types::Value>,
    (table_name, main_pk_name): (&str, &str),
    (rel_table_name, peer_col_in_rel, main_col_in_rel, fk_vals): (
        &str,
        &str,
        &str,
        &[types::Value],
    ),
    verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
) -> anyhow::Result<()> {
    let my_pk_val = match input.get(main_pk_name) {
        Some(val) => val.clone(),
        None => types::Value::Text("".to_string()),
    };
    verify_values_required(fk_vals, rel_table_name, peer_col_in_rel)?;
    verify_values_required(&[my_pk_val.clone()], rel_table_name, main_col_in_rel)?;
    create::i_one(conn, table_name, input, verification_options)?;
    for fk_val in fk_vals {
        relink::nn(
            conn,
            rel_table_name,
            (main_col_in_rel, &[my_pk_val.clone()]),
            (peer_col_in_rel, &[fk_val.clone()]),
        )?;
    }
    Ok(())
}
