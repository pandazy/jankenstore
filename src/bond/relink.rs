use std::collections::HashMap;

use rusqlite::{types, Connection};

use crate::crud::{del, sql::merge_q_configs, total, update, verify::verify_values_required};

///
/// build or rebuild the links of the target records to their parent record
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `child_table` - the name of the target records' table (child table, n in n-1)
/// * `parent_config` - the parent-related table matching settings (1 in n-1)
///                    - `tuple(column_name_of_the_parent_table_in_the_child_table, value_of_the_parent_table_primary_key)`
/// * `child_config` - the column information of the child table, similar to `parent_config`
/// * `where_q_config` - the where clause and the parameters for condition matching
///
pub fn n1_by_pk(
    conn: &Connection,
    child_table: &str,
    parent_config: (&str, &types::Value),
    child_config: (&str, &[types::Value]),
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<()> {
    let (parent_col, parent_val) = parent_config;
    let (child_pk_col, child_pk_vals) = child_config;
    let pr_val = parent_val.clone();
    verify_values_required(&[pr_val.clone()], child_table, parent_col)?;
    verify_values_required(child_pk_vals, child_table, child_pk_col)?;
    let input = HashMap::from([(parent_col.to_string(), pr_val)]);
    update::u_by_pk(
        conn,
        child_table,
        child_pk_col,
        child_pk_vals,
        &input,
        where_q_config,
        None,
    )?;

    Ok(())
}

///
/// build or rebuild the links of the target records to their parent record
/// by looking up the existing foreign key value
/// and updating the foreign key value to the new value
/// - [`n1_by_pk`] is preferred over this function.
/// - This function is created mainly to fill some operational gaps such as correcting invalid relationships
///
/// # Arguments
///
/// * `conn` - the Rusqlite connection to the database
/// * `child_table` - the name of the target records' table (child table, n in n-1)
/// * `parent_config` - the parent-related table matching settings (1 in n-1)
///                     - `tuple(column_name_of_parent_in_child_node_table, old_key_value_of_the_parent_node, new_key_value_of_the_parent_node)`
///                     - <b>WARNING:</b> There might be multiple children with the same parent node,
///                       be careful about what this value should be
/// * `where_q_config` - the where clause and the parameters for condition matching
pub fn n1_by_ofk(
    conn: &Connection,
    child_table: &str,
    parent_config: (&str, &types::Value, &types::Value),
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<()> {
    let (parent_col, parent_old_val, parent_new_val) = parent_config;
    if parent_old_val == parent_new_val {
        return Ok(());
    }
    verify_values_required(&[parent_new_val.clone()], child_table, parent_col)?;
    verify_values_required(&[parent_old_val.clone()], child_table, parent_col)?;
    let input = HashMap::from([(parent_col.to_string(), parent_new_val.clone())]);
    let (where_clause, where_params) = merge_q_configs(
        Some((&format!("{} = ?", parent_col), &[parent_old_val.clone()])),
        where_q_config,
        "AND",
    )?;
    update::u_all(
        conn,
        child_table,
        &input,
        (where_clause.as_str(), where_params.as_slice()),
        None,
    )?;
    Ok(())
}

///
/// check if the link between the target record and the peer record exists
fn nn_link_exists(
    conn: &Connection,
    rel_name: &str,
    a_config: (&str, &types::Value),
    b_config: (&str, &types::Value),
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<bool> {
    let (a_col, a_val) = a_config;
    let (b_col, b_val) = b_config;
    verify_values_required(&[a_val.clone()], rel_name, a_col)?;
    verify_values_required(&[b_val.clone()], rel_name, b_col)?;
    let (where_clause, where_params) = merge_q_configs(
        Some((
            format!("{} = ? AND {} = ?", a_col, b_col).as_str(),
            &[a_val.clone(), b_val.clone()],
        )),
        where_q_config,
        "AND",
    )?;
    let count = total::t_all(
        conn,
        rel_name,
        None,
        Some((where_clause.as_str(), where_params.as_slice())),
    )?;
    Ok(count > 0)
}

///
/// build or rebuild the links of the target records to their peers
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `rel_name` - the name of the table that represents the n-n relationship
/// * `a_config` - the table matching settings of the A side of the relationship
///               - `tuple(column name, values_of_the_primary_key_values_of_the_records_in_this_column)`
/// * `b_config` - the table matching settings of the B side of the relationship, similar to `a_config`
pub fn nn(
    conn: &Connection,
    rel_name: &str,
    a_config: (&str, &[types::Value]),
    b_config: (&str, &[types::Value]),
) -> anyhow::Result<()> {
    let (a_col, a_vals) = a_config;
    let (b_col, b_vals) = b_config;
    verify_values_required(a_vals, rel_name, a_col)?;
    verify_values_required(b_vals, rel_name, b_col)?;
    let mut deduped_a_vals = a_vals.to_vec();
    deduped_a_vals.dedup();

    let mut deduped_b_vals = b_vals.to_vec();
    deduped_b_vals.dedup();

    let mut pairs_to_insert = vec![];
    for a_val in &deduped_a_vals {
        for b_val in &deduped_b_vals {
            let existed = nn_link_exists(conn, rel_name, (a_col, a_val), (b_col, b_val), None)?;
            if !existed {
                pairs_to_insert.push((a_val, b_val));
            }
        }
    }
    for (a_val, b_val) in pairs_to_insert {
        let input = HashMap::from([
            (a_col.to_string(), a_val.clone()),
            (b_col.to_string(), b_val.clone()),
        ]);
        crate::crud::create::i_one(conn, rel_name, &input, None)?;
    }

    Ok(())
}

///
/// delete all the links of the target records to their peers
/// (the Cartesian product of the target records and the peer records)
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `rel_name` - the name of the table that represents the n-n relationship
/// * `a_config` - the table matching settings of the A side of the relationship
///                - `tuple(column name, values_of_the_primary_key_values_of_the_records_in_this_column)`
/// * `b_config` - the table matching settings of the B side of the relationship, similar to `a_config`
///
pub fn d_all(
    conn: &Connection,
    rel_name: &str,
    a_config: (&str, &[types::Value]),
    b_config: (&str, &[types::Value]),
) -> anyhow::Result<()> {
    let (a_col, a_vals) = a_config;
    let (b_col, b_vals) = b_config;
    verify_values_required(a_vals, rel_name, a_col)?;
    verify_values_required(b_vals, rel_name, b_col)?;
    let mut deduped_a_vals = a_vals.to_vec();
    deduped_a_vals.dedup();

    let mut deduped_b_vals = b_vals.to_vec();
    deduped_b_vals.dedup();

    for a_val in &deduped_a_vals {
        for b_val in &deduped_b_vals {
            del::d_all(
                conn,
                rel_name,
                (
                    format!("{} = ? AND {} = ?", a_col, b_col).as_str(),
                    &[a_val.clone(), b_val.clone()],
                ),
            )?;
        }
    }
    Ok(())
}
