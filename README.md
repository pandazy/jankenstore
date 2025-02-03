# jankenstore

Database operation helpers library

The [crate](https://crates.io/crates/jankenstore)

It creates a set of generic functions to operate on a SQLite database.

# Action enums:

Enums under `jankenstore::action::*`

They can be converted from serde_json::Value which which can be used by web server such as [Axum](https://github.com/tokio-rs/axum), in the form of request payloads.

| Enum     | Enumerator                                                        | Description                                                                                           |
| -------- | ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| ReaderOp | ByPk(String, Vec\<JsonValue\>)                                    | Read records by primary keys, with (table_name, pk_list) as inputs                                    |
|          | Children(String, Vec\<RelConfigClientInput\>)                     | Read child records which belongs to specific parents (n-1 relationship)                               |
|          | Peers(String, Vec\<RelConfigClientInput\>)                        | Read peer records which belongs to specific peers(n-n relationship)                                   |
|          | Search(String, (String, String))                                  | Search records by a specific field and value                                                          |
| ModifyOp | Create(String, JsonValue)                                         | Create a new record, with (table_name, payload) as inputs                                             |
|          | CreateChild(String, Vec\<RelConfigClientInputSingle\>, JsonValue) | Create a new child record, with (table_name, parents, payload) as inputs                              |
|          | Update(String, Vec\<JsonValue\>, JsonValue)                       | Update records by primary keys, with (table_name, pk_list, payload) as inputs                         |
|          | UpdateChildren(String, Vec\<RelConfigClientInput\>, JsonValue)    | Update child records which belongs to specific parents, with (table_name, parents, payload) as inputs |
| DelOp    | Delete(String, Vec\<JsonValue\>)                                  | Delete records by primary keys, with (table_name, pk_list) as inputs                                  |
|          | DeleteChildren(String, Vec\<RelConfigClientInput\>)               | Delete child records which belongs to specific parents, with (table_name, parents) as inputs          |
| RelOp    | Link(RelConfigClientInput, RelConfigClientInput)                  | Link records into peer (n-to-n) relationships, with (peer1_vals, peer2_vals) as inputs                |
|          | Unlink(RelConfigClientInput, RelConfigClientInput)                | Unlink records of peer (n-to-n) relationships, with (peer1_vals, peer2_vals) as inputs                |

Each enum has a `with_schema` function to perform the operation on a specific database with a specific schema configuration.
