use serde::{Serialize, Deserialize};
use serde_json;

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}