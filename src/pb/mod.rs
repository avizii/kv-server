pub mod abi;

use abi::*;
use crate::pb::abi::command_request::RequestData;

impl CommandRequest {
    pub fn new_hset<T>(table: T, key: T, value: Value) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hset(
                    Hset {
                        table: table.into(),
                        pair: Some(Kvpair::new(key, value)),
                    }
                )
            )
        }
    }
}

impl Kvpair {
    
    pub fn new<T>(key: T, value: Value) -> Self 
        where 
            T: Into<String>,
    {
        Self {
            key: key.into(),
            value: Some(value)
        }
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s))
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into()))
        }
    }
}