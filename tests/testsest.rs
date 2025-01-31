use std::collections::HashMap;

use jankenstore::action::{WriterOp, RelConfigClientInput};
use serde::Deserialize;
use serde_json::{json, Value};

#[test]
fn testest() {
    #[derive(Debug, Deserialize)]
    enum Test {
        A(Vec<String>),
        B,
    }

    #[derive(Debug, Deserialize)]
    pub struct Test2 {
        pub op: WriterOp,
        pub peers: Option<(RelConfigClientInput, RelConfigClientInput)>,
        pub test: Option<Test>,
    }

    let a = json!({
     "op": {
      "Update": ["main_table", [1, 2, 3]]
     },
     "parents": [
        ["book", [1, 2, 3]],
        ["author", [1, 2, 3]]
     ],
      "peers": [
          ["album", [9, 8, 7]],
          ["group", [6, 5, 4]]
      ],
      "test": { "A": ["gogogo", "hahaa"] }
    });

    let t: Test2 = serde_json::from_value(a).unwrap();
    print!("{:?}", t);
}
