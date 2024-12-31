use super::create;
use crate::TblRep;

use rusqlite::{types, Connection};
use serde::de::DeserializeOwned;

use std::collections::{HashMap, HashSet};

/// N1Wrap is a wrapper for n-1 relationship
/// It contains two tables and the relationship between them
pub struct N1Wrap<'a> {
    tn: &'a TblRep,
    t1: &'a TblRep,
    parent_col: &'a str,
}

impl<'a> N1Wrap<'a> {
    /// Creates a new N1Wrap.
    /// # Arguments
    /// * `tn_config` - the [`TblRep`] of the child table and the column name of the parent table in the child table
    /// * `t1` - the [`TblRep`] of the parent table
    pub fn new(tn_config: (&'a TblRep, &'a str), t1: &'a TblRep) -> Self {
        let (tn, parent_col) = tn_config;
        Self { tn, t1, parent_col }
    }

    /// Returns the [`TblRep`] of the child table.
    pub fn get_tn(&self) -> &TblRep {
        self.tn
    }

    /// Returns the [`TblRep`] of the parent table.
    pub fn get_t1(&self) -> &TblRep {
        self.t1
    }

    /// Returns the column name of the parent table in the child table.
    pub fn get_parent_col(&self) -> &str {
        self.parent_col
    }

    /// Inserts a new record into the child table and tie it to the parent record.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `parent_val` - the value of the parent table's primary key
    /// * `input` - the input data to be inserted into the child table
    /// * `verification_options` - the options for verification, if None, no verification is performed
    pub fn ins(
        &self,
        conn: &Connection,
        parent_val: &types::Value,
        input: &HashMap<String, types::Value>,
        verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
    ) -> anyhow::Result<()> {
        let parent_config = (self.get_parent_col(), parent_val);
        let table_name = self.get_tn().get_name();
        create::n1(conn, table_name, parent_config, input, verification_options)
    }

    ///
    /// Relink the child records to the new parent record.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `parent_old_val` - the old value of the parent table's primary key
    /// * `parent_new_val` - the new value of the parent table's primary key
    /// * `where_q_config` - the where clause and the parameters for condition matching
    pub fn relink(
        &self,
        conn: &Connection,
        parent_old_val: &types::Value,
        parent_new_val: &types::Value,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<()> {
        let parent_config = (self.get_parent_col(), parent_old_val, parent_new_val);
        let table_name = self.get_tn().get_name();
        super::relink::n1_by_ofk(conn, table_name, parent_config, where_q_config)
    }

    /// List all child records of the parent record.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `parents` - the values of the parent table's primary key
    /// * `d_fields` - the fields to be displayed in the result
    /// * `where_q_config` - the where clause and the parameters for condition matching
    pub fn list_kids(
        &self,
        conn: &Connection,
        parents: &[types::Value],
        d_fields: Option<&[&str]>,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
        let parent_config = (self.get_parent_col(), parents);
        let table_name = self.get_tn().get_name();
        super::fetch::list_n_of_1(conn, table_name, parent_config, d_fields, where_q_config)
    }

    /// Similar to [`N1Wrap::list_kids`], but returns the result as a vector of the given type.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `parents` - the values of the parent table's primary key
    /// * `d_fields` - the fields to be displayed in the result
    /// * `where_q_config` - the where clause and the parameters for condition matching
    /// * `T` - the type of each element in the result
    pub fn list_kids_as<T: DeserializeOwned>(
        &self,
        conn: &Connection,
        parents: &[types::Value],
        d_fields: Option<&[&str]>,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<Vec<T>> {
        let parent_config = (self.get_parent_col(), parents);
        let table_name = self.get_tn().get_name();
        super::fetch::list_n_of_1_as(conn, table_name, parent_config, d_fields, where_q_config)
    }
}

///
/// NnInfoConfig is a tuple of (table_name, pk_name, main_col_in_rel)
/// NnInfoConfig[2] is only needed for reading, for writing, it will be left with an empty string
type NnInfoConfig<'b> = (&'b str, &'b str, &'b str);

/// NnRelConfig is a tuple of (rel_name, peer_col, main_col, peers)
/// NnRelConfig[2] is only needed for writing, for reading, it will be left with an empty string
type NnRelConfig<'b> = (&'b str, &'b str, &'b str, &'b [types::Value]);

/// NnWrap is a wrapper for n-n relationship
/// It contains two tables and the relationship between them
pub struct NnWrap<'a> {
    t1: &'a TblRep,
    t2: &'a TblRep,
    rel: (&'a str, &'a str, &'a str),
}

impl<'a> NnWrap<'a> {
    /// Creates a new NnWrap.
    /// # Arguments
    /// * `t1` - the [`TblRep`] of the first table
    /// * `t2` - the [`TblRep`] of the second table
    /// * `rel` - the relationship between the two tables
    ///          - `tuple(rel_name, t1_col, t2_col)`
    ///          - `rel_name` is the name of the table that represents the n-n relationship
    ///          - `t1_col` is the column name of the first table in the relationship table
    ///          - `t2_col` is the column name of the second table in the relationship table
    pub fn new(t1: &'a TblRep, t2: &'a TblRep, rel: (&'a str, &'a str, &'a str)) -> Self {
        Self {
            t1,
            t2,
            rel: (rel.0, rel.1, rel.2),
        }
    }

    /// Returns the [`TblRep`] of the first table.
    pub fn get_t1(&self) -> &TblRep {
        self.t1
    }

    /// Returns the [`TblRep`] of the second table.
    pub fn get_t2(&self) -> &TblRep {
        self.t2
    }

    /// Returns the relationship definition between the two tables.
    pub fn get_rel(&self) -> (&str, &str, &str) {
        (self.rel.0, self.rel.1, self.rel.2)
    }

    /// Relink the records of the two tables.
    /// It will remove all links that are in the Cartesian product of the two sets of values,
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `a_vals` - the values of the first table's primary key
    /// * `b_vals` - the values of the second table's primary key
    pub fn link(
        &self,
        conn: &Connection,
        a_vals: &[types::Value],
        b_vals: &[types::Value],
    ) -> anyhow::Result<()> {
        let (rel_name, a_col, b_col) = self.get_rel();
        super::relink::nn(conn, rel_name, (a_col, a_vals), (b_col, b_vals))
    }

    /// Unlink the records of the two tables.
    /// It will remove all links that are in the Cartesian product of the two sets of values.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `a_vals` - the values of the first table's primary key
    /// * `b_vals` - the values of the second table's primary key
    pub fn unlink(
        &self,
        conn: &Connection,
        a_vals: &[types::Value],
        b_vals: &[types::Value],
    ) -> anyhow::Result<()> {
        let (rel_name, a_col, b_col) = self.get_rel();
        super::relink::d_all(conn, rel_name, (a_col, a_vals), (b_col, b_vals))
    }

    fn conf_pair<'b>(
        &'b self,
        rep: &'b TblRep,
        peers: &'b [types::Value],
        is_write: bool,
    ) -> (NnInfoConfig, NnRelConfig) {
        let (rel_name, t1_col, t2_col) = self.get_rel();
        let table = rep.get_name();
        let (rel_main_col, rel_peer_col) = if table == self.get_t1().get_name() {
            (t1_col, t2_col)
        } else {
            (t2_col, t1_col)
        };
        let last_info_config_item = if is_write { "" } else { rel_main_col };
        let info_config = (table, rep.get_pk_name(), last_info_config_item);
        let second_col_in_rel_config = if is_write { rel_main_col } else { "" };
        let rel_config = (rel_name, rel_peer_col, second_col_in_rel_config, peers);
        (info_config, rel_config)
    }

    fn ins(
        &self,
        conn: &Connection,
        rep: &TblRep,
        input: &HashMap<String, types::Value>,
        peers: &[types::Value],
        verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
    ) -> anyhow::Result<()> {
        let (info_config, rel_config) = self.conf_pair(rep, peers, true);
        let info_config = (info_config.0, info_config.1);
        create::nn(conn, input, info_config, rel_config, verification_options)
    }

    /// Inserts a new record into the first table and tie it to the second table.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `input` - the input data to be inserted into the first table
    /// * `peers` - the values of the second table's primary key
    /// * `verification_options` - the options for verification, if None, no verification is performed
    pub fn ins_t1(
        &self,
        conn: &Connection,
        input: &HashMap<String, types::Value>,
        peers: &[types::Value],
        verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
    ) -> anyhow::Result<()> {
        self.ins(conn, self.get_t1(), input, peers, verification_options)
    }

    /// Inserts a new record into the second table and tie it to the first table.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `input` - the input data to be inserted into the second table
    /// * `peers` - the values of the first table's primary key
    /// * `verification_options` - the options for verification, if None, no verification is performed
    pub fn ins_t2(
        &self,
        conn: &Connection,
        input: &HashMap<String, types::Value>,
        peers: &[types::Value],
        verification_options: Option<(&HashMap<String, types::Value>, &HashSet<String>, bool)>,
    ) -> anyhow::Result<()> {
        self.ins(conn, self.get_t2(), input, peers, verification_options)
    }

    fn list_peers(
        &self,
        conn: &Connection,
        rep: &TblRep,
        peers: &[types::Value],
        d_fields: Option<&[&str]>,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
        let (info_config, rel_config) = self.conf_pair(rep, peers, false);
        let rel_config = (rel_config.0, rel_config.1, rel_config.3);
        super::fetch::list_n_of_n(conn, info_config, rel_config, d_fields, where_q_config)
    }

    /// List all peers of the given records in the first table.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `peers` - the values of the second table's primary key
    /// * `d_fields` - the fields to be displayed in the result
    /// * `where_q_config` - the where clause and the parameters for condition matching
    pub fn peers_of_t1(
        &self,
        conn: &Connection,
        peers: &[types::Value],
        d_fields: Option<&[&str]>,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
        self.list_peers(conn, self.get_t1(), peers, d_fields, where_q_config)
    }

    /// Similar to [`NnWrap::peers_of_t1`], but returns the result as a vector of the given type.
    /// # Arguments
    /// * `conn` - the Rusqlite connection to the database
    /// * `peers` - the values of the second table's primary key
    /// * `d_fields` - the fields to be displayed in the result
    /// * `where_q_config` - the where clause and the parameters for condition matching
    /// * `T` - the type of each element in the result
    pub fn peers_of_t1_as<T: DeserializeOwned>(
        &self,
        conn: &Connection,
        peers: &[types::Value],
        d_fields: Option<&[&str]>,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<Vec<T>> {
        let (info_config, rel_config) = self.conf_pair(self.get_t1(), peers, false);
        let rel_config = (rel_config.0, rel_config.1, rel_config.3);
        super::fetch::list_n_of_n_as(conn, info_config, rel_config, d_fields, where_q_config)
    }

    /// Similar to [`NnWrap::peers_of_t1`], but for the second table.
    pub fn peers_of_t2(
        &self,
        conn: &Connection,
        peers: &[types::Value],
        d_fields: Option<&[&str]>,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<Vec<HashMap<String, types::Value>>> {
        self.list_peers(conn, self.get_t2(), peers, d_fields, where_q_config)
    }

    /// Similar to [`NnWrap::peers_of_t1_as`], but for the second table.
    pub fn peers_of_t2_as<T: DeserializeOwned>(
        &self,
        conn: &Connection,
        peers: &[types::Value],
        d_fields: Option<&[&str]>,
        where_q_config: Option<(&str, &[types::Value])>,
    ) -> anyhow::Result<Vec<T>> {
        let (info_config, rel_config) = self.conf_pair(self.get_t2(), peers, false);
        let rel_config = (rel_config.0, rel_config.1, rel_config.3);
        super::fetch::list_n_of_n_as(conn, info_config, rel_config, d_fields, where_q_config)
    }
}
