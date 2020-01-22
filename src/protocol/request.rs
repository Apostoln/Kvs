use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Rm {
        key: String,
    },
}