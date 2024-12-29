use std::collections::HashMap;

use rusqlite::{types, Connection};

use crate::crud::{del, sql::merge_q_configs, total, update, verify::verify_values_required};

///
/// build or rebuild the links of the target records to their parent record
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `main_table` - the name of the target records' table
/// * `parent_pk_name` - the child table's column name that references the parent table
/// * `parent_pk_val` - the parent node record's primary key value
/// * `main_pk_name` - the column name of the target records' primary key
/// * `main_pk_vals` - the values of the records that will be owned by the parent table record
///
pub fn n1_by_pk(
    conn: &Connection,
    main_table: &str,
    (parent_pk_name, new_parent_pk_val): (&str, &types::Value),
    (main_pk_name, main_pk_vals): (&str, &[types::Value]),
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<()> {
    let pr_val = new_parent_pk_val.clone();
    verify_values_required(&[pr_val.clone()], main_table, parent_pk_name)?;
    verify_values_required(main_pk_vals, main_table, main_pk_name)?;
    let input = HashMap::from([(parent_pk_name.to_string(), pr_val)]);
    update::u_by_pk(
        conn,
        main_table,
        main_pk_name,
        main_pk_vals,
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
/// * `my_name` - the name of the target records' table (child table)
/// * `fk_name` - the column name of the foreign key that will be updated in the child record table
/// * `new_fk_val` - the new value of the foreign key
/// * `old_fk_val` - the old value of the foreign key that will be used to find the records to be updated.
///                  - <b>WARNING:</b> There might be multiple records with the same foreign key value,
///                  be careful about what this value should be
pub fn n1_by_ofk(
    conn: &Connection,
    my_name: &str,
    (fk_name, old_fk_val, new_fk_val): (&str, &types::Value, &types::Value),
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<()> {
    if new_fk_val == old_fk_val {
        return Ok(());
    }
    verify_values_required(&[new_fk_val.clone()], my_name, fk_name)?;
    verify_values_required(&[old_fk_val.clone()], my_name, fk_name)?;
    let input = HashMap::from([(fk_name.to_string(), new_fk_val.clone())]);
    let (where_clause, where_params) = merge_q_configs(
        Some((&format!("{} = ?", fk_name), &[old_fk_val.clone()])),
        where_q_config,
        "AND",
    )?;
    update::u_all(
        conn,
        my_name,
        &input,
        (where_clause.as_str(), where_params.as_slice()),
        None,
    )?;
    Ok(())
}

fn nn_link_exists(
    conn: &Connection,
    rel_name: &str,
    (my_fk_name, my_val): (&str, &types::Value),
    (peer_fk_name, peer_val): (&str, &types::Value),
    where_q_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<bool> {
    verify_values_required(&[my_val.clone()], rel_name, my_fk_name)?;
    verify_values_required(&[peer_val.clone()], rel_name, peer_fk_name)?;
    let (where_clause, where_params) = merge_q_configs(
        Some((
            format!("{} = ? AND {} = ?", my_fk_name, peer_fk_name).as_str(),
            &[my_val.clone(), peer_val.clone()],
        )),
        where_q_config,
        "AND",
    )?;
    let count = total::t_all(
        conn,
        rel_name,
        false,
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
/// * `my_fk_name` - the column name that references the target records in the relationship table
/// * `my_vals` - the values of the target records that will be linked to their peers
/// * `peer_fk_name` - the column name that references the peer records in the relationship table
/// * `peer_vals` - the values of the peer records that will be linked to the target records
pub fn nn(
    conn: &Connection,
    rel_name: &str,
    (my_fk_name, my_vals): (&str, &[types::Value]),
    (peer_fk_name, peer_vals): (&str, &[types::Value]),
) -> anyhow::Result<()> {
    verify_values_required(my_vals, rel_name, my_fk_name)?;
    verify_values_required(peer_vals, rel_name, peer_fk_name)?;
    let mut deduped_my_vals = my_vals.to_vec();
    deduped_my_vals.dedup();

    let mut deduped_peer_vals = peer_vals.to_vec();
    deduped_peer_vals.dedup();

    let mut fk_pairs_to_insert = vec![];
    for my_val in &deduped_my_vals {
        for peer_val in &deduped_peer_vals {
            let existed = nn_link_exists(
                conn,
                rel_name,
                (my_fk_name, my_val),
                (peer_fk_name, peer_val),
                None,
            )?;
            if !existed {
                fk_pairs_to_insert.push((my_val, peer_val));
            }
        }
    }
    for (my_val, peer_val) in fk_pairs_to_insert {
        let input = HashMap::from([
            (my_fk_name.to_string(), my_val.clone()),
            (peer_fk_name.to_string(), peer_val.clone()),
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
/// * `my_fk_name` - the column name that references the target records in the relationship table
/// * `my_vals` - the values of the target records that will be unlinked from their peers
/// * `peer_fk_name` - the column name that references the peer records in the relationship table
/// * `peer_vals` - the values of the peer records that will be unlinked from the target records
///
pub fn d_all(
    conn: &Connection,
    rel_name: &str,
    (my_fk_name, my_vals): (&str, &[types::Value]),
    (peer_fk_name, peer_vals): (&str, &[types::Value]),
) -> anyhow::Result<()> {
    verify_values_required(my_vals, rel_name, my_fk_name)?;
    verify_values_required(peer_vals, rel_name, peer_fk_name)?;
    let mut deduped_my_vals = my_vals.to_vec();
    deduped_my_vals.dedup();

    let mut deduped_peer_vals = peer_vals.to_vec();
    deduped_peer_vals.dedup();

    for my_val in &deduped_my_vals {
        for peer_val in &deduped_peer_vals {
            del::d_all(
                conn,
                rel_name,
                (
                    format!("{} = ? AND {} = ?", my_fk_name, peer_fk_name).as_str(),
                    &[my_val.clone(), peer_val.clone()],
                ),
            )?;
        }
    }
    Ok(())
}
