use crate::input_utils::fk_name;

use super::{
    basics::{del, insert, total},
    input_utils::verify_pk,
    schema::SchemaFamily,
    sql::merge_q_configs,
};

use rusqlite::{types, Connection};

use std::collections::HashMap;

///
/// get the matching clause for "Where" SQL for the related peer records
/// # Arguments
/// * `rel_name` - the name of the table that represents the n-n relationship
/// * `fk_name` - the name of the foreign key column in the relationship table
///               that links the main-source records to its peer, in the form of `<peer_table_name>_id`.
///               If we want to display all users who have a specific role
///               - the `fk_name` would be `role_id`.
///               - the `source_name` would be `user`.
///               - `rel_name`` can be `rel_user_role` or `rel_role_user`
/// * `source_name` - the name of the table that represents the main-source record details to display
/// * `source_pk` - the name of the primary key column in the source table
/// * `bond_matching_clause` - the extra matching clause for the relationship table
///                            apart from the foreign key connection to the source table.
///                            If it's empty, it will be ignored
pub fn peer_matching_clause(
    rel_name: &str,
    fk_name: &str,
    (source_name, source_pk): (&str, &str),
    bond_matching_clause: &str,
) -> String {
    let link_condition = format!("{} = {}.{}", fk_name, source_name, source_pk);
    let bond_matching_clause = if bond_matching_clause.is_empty() {
        bond_matching_clause.to_string()
    } else {
        format!("AND {}", bond_matching_clause)
    };
    format!(
        "EXISTS (SELECT 1 FROM {} WHERE {} {})",
        rel_name, link_condition, bond_matching_clause
    )
}

///
/// check if the link between the target record and the peer record exists
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `rel_name` - the name of the table that represents the n-n relationship
/// * `a_config` - the table matching settings of the A side of the relationship
///               - `tuple(source_a_table_name, source_a_pk_value)`
/// * `b_config` - the table matching settings of the B side of the relationship, similar to `a_config`
/// * `where_config` - the where clause and the parameters for the where clause,
fn nn_link_exists(
    conn: &Connection,
    rel_name: &str,
    a_config: (&str, &types::Value),
    b_config: (&str, &types::Value),
    where_config: Option<(&str, &[types::Value])>,
) -> anyhow::Result<bool> {
    let (a_col, a_val) = a_config;
    let (b_col, b_val) = b_config;
    let (where_clause, where_params) = merge_q_configs(
        Some((
            format!("{} = ? AND {} = ?", a_col, b_col).as_str(),
            &[a_val.clone(), b_val.clone()],
        )),
        where_config,
        "AND",
    );
    let count = total(
        conn,
        rel_name,
        None,
        Some((where_clause.as_str(), where_params.as_slice())),
    )?;
    Ok(count > 0)
}

///
/// build or rebuild the links of the target records to their peers
/// (the Cartesian product of the target records and the peer records)
///
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `rel_name` - the name of the table that represents the n-n relationship
/// * `a_config` - the table matching settings of the A side of the relationship
///               - `tuple(source_a_table_name, source_a_pk_value_list)`
/// * `b_config` - the table matching settings of the B side of the relationship, similar to `a_config`
fn nn(
    conn: &Connection,
    rel_name: &str,
    a_config: (&str, &[types::Value]),
    b_config: (&str, &[types::Value]),
) -> anyhow::Result<()> {
    let (a_col, a_vals) = a_config;
    let (b_col, b_vals) = b_config;
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
        insert(conn, rel_name, &input)?;
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
///                - `tuple(source_a_table_name, source_a_pk_value_list)`
/// * `b_config` - the table matching settings of the B side of the relationship, similar to `a_config`
///
fn d_all(
    conn: &Connection,
    rel_name: &str,
    a_config: (&str, &[types::Value]),
    b_config: (&str, &[types::Value]),
) -> anyhow::Result<()> {
    let (a_col, a_vals) = a_config;
    let (b_col, b_vals) = b_config;
    let mut deduped_a_vals = a_vals.to_vec();
    deduped_a_vals.dedup();

    let mut deduped_b_vals = b_vals.to_vec();
    deduped_b_vals.dedup();

    for a_val in &deduped_a_vals {
        for b_val in &deduped_b_vals {
            del(
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

///
/// link the target records to their peers
/// (the Cartesian product of the target records and the peer records)
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family containing the schema for the table, used for validation. See [SchemaFamily]
/// * `a_config` - the table matching settings of the A side of the relationship
///               - `tuple(source_a_table_name, source_a_pk_value)`
/// * `b_config` - the table matching settings of the B side of the relationship, similar to `a_config`
pub fn link(
    conn: &Connection,
    schema_family: &SchemaFamily,
    a_config: (&str, &[types::Value]),
    b_config: (&str, &[types::Value]),
) -> anyhow::Result<()> {
    let (a_table, a_val) = a_config;
    let (b_table, b_val) = b_config;
    schema_family.verify_peer_of(a_table, b_table)?;
    for (table, val) in [a_config, b_config] {
        verify_pk(schema_family, table, val)?;
    }
    let peer_link_table = schema_family.try_get_peer_link_table_of(a_table)?;
    let a_col = fk_name(a_table);
    let b_col = fk_name(b_table);
    nn(
        conn,
        peer_link_table,
        (a_col.as_str(), a_val),
        (b_col.as_str(), b_val),
    )
}

///
/// Remove the link between the target records and their peers
/// (the Cartesian product of the target records and the peer records)
/// # Arguments
/// * `conn` - the Rusqlite connection to the database
/// * `schema_family` - the schema family containing the schema for the table, used for validation. See [SchemaFamily]
/// * `a_config` - the table matching settings of the A side of the relationship
///              - `tuple(source_a_table_name, source_a_pk_value)`
/// * `b_config` - the table matching settings of the B side of the relationship, similar to `a_config`
pub fn unlink(
    conn: &Connection,
    schema_family: &SchemaFamily,
    a_config: (&str, &[types::Value]),
    b_config: (&str, &[types::Value]),
) -> anyhow::Result<()> {
    let (a_table, a_val) = a_config;
    let (b_table, b_val) = b_config;
    schema_family.verify_peer_of(a_table, b_table)?;
    for (table, val) in [a_config, b_config] {
        verify_pk(schema_family, table, val)?;
    }
    let peer_link_table = schema_family.try_get_peer_link_table_of(a_table)?;
    let a_col = fk_name(a_table);
    let b_col = fk_name(b_table);
    d_all(
        conn,
        peer_link_table,
        (a_col.as_str(), a_val),
        (b_col.as_str(), b_val),
    )
}

#[cfg(test)]
mod tests {
    use crate::peer::peer_matching_clause;

    #[test]
    fn test_peer_matching_clause_empty_bond() {
        let rel_name = "rel_user_role";
        let fk_name = "role_id";
        let source_name = "user";
        let source_pk = "id";
        let bond_matching_clause = "";
        let expected = "EXISTS (SELECT 1 FROM rel_user_role WHERE role_id = user.id )";
        let actual = peer_matching_clause(
            rel_name,
            fk_name,
            (source_name, source_pk),
            bond_matching_clause,
        );
        assert_eq!(expected, actual);
    }
}
