use std::sync::Arc;
use tracing::debug;
use crate::*;
use crate::command_request::RequestData;
use crate::storage::Storage;

mod command_service;

pub trait CommandService {
    fn execute(self, store: &dyn Storage) -> CommandResponse;
}

pub struct Service {
    store: Arc<dyn Storage>,
}

impl Clone for Service {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
        }
    }
}

impl Service {
    pub fn new<S: Storage>(store: S) -> Self {
        Self {
            store: Arc::new(store)
        }
    }

    pub fn execute(&self, cmd: CommandRequest) -> CommandResponse {
        debug!("Got Request: {:?}", cmd);

        let res = dispatch(cmd, self.store.as_ref());

        debug!("Executed Response: {:?}", res);

        res
    }
}

pub fn dispatch(cmd: CommandRequest, store: &dyn Storage) -> CommandResponse {
    match cmd.request_data {
        Some(RequestData::Hget(param)) => param.execute(store),
        Some(RequestData::Hgetall(param)) => param.execute(store),
        Some(RequestData::Hset(param)) => param.execute(store),
        Some(RequestData::Hmset(param)) => param.execute(store),
        Some(RequestData::Hmget(param)) => param.execute(store),
        Some(RequestData::Hexist(param)) => param.execute(store),
        Some(RequestData::Hmexist(param)) => param.execute(store),
        Some(RequestData::Hdel(param)) => param.execute(store),
        Some(RequestData::Hmdel(param)) => param.execute(store),
        None => KvError::InvalidCommand("Request has not data".into()).into(),
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use super::*;

    #[test]
    fn service_should_works() {
        let service = Service::new(MemTable::default());

        let svc = service.clone();

        let handle = thread::spawn(move || {
            let res = svc.execute(CommandRequest::new_hset("t1", "k1", "v1".into()));
            assert_res_ok(res, &[Value::default()], &[]);
        });

        handle.join().unwrap();

        let res = service.execute(CommandRequest::new_hget("t1", "k1"));
        assert_res_ok(res, &["v1".into()], &[]);
    }
}

#[cfg(test)]
pub fn assert_res_ok(mut res: CommandResponse, values: &[Value], pairs: &[Kvpair]) {
    res.pairs.sort_by(|a, b| a.partial_cmp(b).unwrap());

    assert_eq!(res.status, 200);
    assert_eq!(res.message, "");
    assert_eq!(res.values, values);
    assert_eq!(res.pairs, pairs);
}

#[cfg(test)]
pub fn assert_res_error(res: CommandResponse, code: u32, msg: &str) {
    assert_eq!(res.status, code);
    assert!(res.message.contains(msg));
    assert_eq!(res.values, &[]);
    assert_eq!(res.pairs, &[]);
}