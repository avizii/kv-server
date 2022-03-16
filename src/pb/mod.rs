pub mod abi;

use http::StatusCode;
use abi::*;
use crate::KvError;
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

    pub fn new_hmset<T>(table: T, pairs: Vec<Kvpair>) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hmset(
                    Hmset {
                        table: table.into(),
                        pairs,
                    }
                )
            )
        }
    }

    pub fn new_hget<T>(table: T, key: T) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hget(
                    Hget {
                        table: table.into(),
                        key: key.into(),
                    }
                )
            )
        }
    }

    pub fn new_hmget<T>(table: T, keys: Vec<String>) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hmget(
                    Hmget {
                        table: table.into(),
                        keys,
                    }
                )
            )
        }
    }

    pub fn new_hgetall<T>(table: T) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hgetall(
                    Hgetall {
                        table: table.into()
                    }
                )
            )
        }
    }

    pub fn new_hdel<T>(table: T, key: T) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hdel(
                    Hdel {
                        table: table.into(),
                        key: key.into(),
                    }
                )
            )
        }
    }

    pub fn new_hmdel<T>(table: T, keys: Vec<String>) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hmdel(
                    Hmdel {
                        table: table.into(),
                        keys,
                    }
                )
            )
        }
    }

    pub fn new_hexist<T>(table: T, key: T) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hexist(
                    Hexist {
                        table: table.into(),
                        key: key.into(),
                    }
                )
            )
        }
    }

    pub fn new_hmexist<T>(table: T, keys: Vec<String>) -> Self
        where
            T: Into<String>,
    {
        Self {
            request_data: Some(
                RequestData::Hmexist(
                    Hmexist {
                        table: table.into(),
                        keys,
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
            value: Some(value),
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

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self {
            value: Some(value::Value::Integer(i))
        }
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Self {
            value: Some(value::Value::Bool(b))
        }
    }
}

impl From<Value> for CommandResponse {
    fn from(v: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: vec![v],
            ..Default::default()
        }
    }
}

impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut result = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            message: e.to_string(),
            values: vec![],
            pairs: vec![],
        };

        match e {
            KvError::NotFound(_, _) => result.status = StatusCode::NOT_FOUND.as_u16() as _,
            KvError::InvalidCommand(_) => result.status = StatusCode::BAD_REQUEST.as_u16() as _,
            KvError::ConvertError(_, _) => {}
            KvError::StorageError(_, _, _, _) => {}
            KvError::EncodeError(_) => {}
            KvError::DecodeError(_) => {}
            KvError::Internal(_) => {}
        };

        result
    }
}

impl From<Vec<Kvpair>> for CommandResponse {
    fn from(v: Vec<Kvpair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            pairs: v,
            ..Default::default()
        }
    }
}

impl From<Kvpair> for CommandResponse {
    fn from(pair: Kvpair) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            pairs: vec![pair],
            ..Default::default()
        }
    }
}

impl From<(Vec<(String, Value)>, Vec<(String, KvError)>)> for CommandResponse {
    fn from(tuple: (Vec<(String, Value)>, Vec<(String, KvError)>)) -> Self {
        let status = estimate_status_code_by_vec(&tuple.0);

        let pairs: Vec<_> = tuple.0.into_iter()
            .map(|(k, v)| Kvpair::new(k, v))
            .collect();

        let message = combine_error_messages(tuple.1);

        Self {
            status,
            pairs,
            message,
            ..Default::default()
        }
    }
}

impl From<bool> for CommandResponse {
    fn from(b: bool) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            values: vec![b.into()],
            ..Default::default()
        }
    }
}

impl From<(Vec<(String, bool)>, Vec<(String, KvError)>)> for CommandResponse {
    fn from(tuple: (Vec<(String, bool)>, Vec<(String, KvError)>)) -> Self {
        let status = estimate_status_code_by_vec(&tuple.0);
        let message = combine_error_messages(tuple.1);

        let v: Vec<_> = tuple.0.into_iter()
            .map(|(s, b)| Kvpair::new(s, b.into()))
            .collect();

        Self {
            status,
            message,
            pairs: v,
            ..Default::default()
        }
    }
}

impl From<(Vec<Kvpair>, Vec<(String, KvError)>)> for CommandResponse {
    fn from(tuple: (Vec<Kvpair>, Vec<(String, KvError)>)) -> Self {
        let status = estimate_status_code_by_vec(&tuple.0);

        let message = combine_error_messages(tuple.1);

        Self {
            status,
            pairs: tuple.0,
            message,
            ..Default::default()
        }
    }
}

fn estimate_status_code_by_vec<T>(data: &Vec<T>) -> u32 {
    let status;
    if data.len() > 0 {
        status = StatusCode::OK.as_u16() as _;
    } else {
        status = StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _;
    }
    status
}

fn combine_error_messages(errors: Vec<(String, KvError)>) -> String {
    let mut message = String::new();
    errors.into_iter()
        .for_each(|(k, v)| {
            let s = format!("request key: {}, message: {}\n", k, v.to_string());
            message.push_str(&s);
        });
    message
}