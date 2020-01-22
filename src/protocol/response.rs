use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}