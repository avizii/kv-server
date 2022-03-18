use crate::error::KvError;
use crate::*;

impl CommandService for Hset {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        match self.pair {
            None => Value::default().into(),
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
        }
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        let mut v1 = Vec::new();
        let mut v2 = Vec::new();

        for pair in self.pairs {
            match store.set(
                &self.table,
                pair.key.clone(),
                pair.value.unwrap_or_default(),
            ) {
                Ok(Some(v)) => v1.push((pair.key, v)),
                Ok(None) => v1.push((pair.key, Value::default())),
                Err(e) => v2.push((pair.key, e)),
            }
        }

        (v1, v2).into()
    }
}

impl CommandService for Hget {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(v)) => v.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        let mut v1 = Vec::new();
        let mut v2 = Vec::new();

        for key in self.keys {
            match store.get(&self.table, &key) {
                Ok(Some(v)) => v1.push((key, v)),
                Ok(None) => v2.push((key.clone(), KvError::NotFound(self.table.to_string(), key))),
                Err(e) => v2.push((key, e)),
            }
        }

        (v1, v2).into()
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(_)) => {
                let pair = Kvpair {
                    key: self.key,
                    value: Some(1.into()),
                };
                pair.into()
            }
            Ok(None) => {
                let pair = Kvpair {
                    key: self.key,
                    value: None,
                };
                pair.into()
            }
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        let mut v1 = Vec::new();
        let mut v2 = Vec::new();

        for key in self.keys {
            match store.del(&self.table, &key) {
                Ok(Some(_)) => {
                    let pair = Kvpair {
                        key,
                        value: Some(1.into()),
                    };
                    v1.push(pair);
                }
                Ok(None) => {
                    let pair = Kvpair { key, value: None };
                    v1.push(pair);
                }
                Err(e) => v2.push((key, e)),
            }
        }

        (v1, v2).into()
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(b) => b.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        let mut v1 = Vec::new();
        let mut v2 = Vec::new();

        for key in self.keys {
            match store.contains(&self.table, &key) {
                Ok(b) => v1.push((key, b)),
                Err(e) => v2.push((key, e)),
            }
        }

        (v1, v2).into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_request::RequestData;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hset("t1", "k1", "v2".into());
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["v1".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();

        let cmd = CommandRequest::new_hset("score", "u1", 10.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[10.into()], &[]);
    }

    #[test]
    fn hget_with_non_exist_key_should_return_404() {
        let store = MemTable::new();

        let cmd = CommandRequest::new_hget("score", "u1");
        let res = dispatch(cmd, &store);
        assert_res_error(res, 404, "Not found");
    }

    #[test]
    fn hgetall_should_work() {
        let store = MemTable::new();

        let cmds = vec![
            CommandRequest::new_hset("t1", "k1", 1.into()),
            CommandRequest::new_hset("t1", "k2", 2.into()),
            CommandRequest::new_hset("t1", "k3", 3.into()),
            CommandRequest::new_hset("t1", "k4", 4.into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let cmd = CommandRequest::new_hgetall("t1");
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", 1.into()),
                Kvpair::new("k2", 2.into()),
                Kvpair::new("k3", 3.into()),
                Kvpair::new("k4", 4.into()),
            ],
        );
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();

        let pairs = vec![
            Kvpair::new("k1", 1.into()),
            Kvpair::new("k2", 2.into()),
            Kvpair::new("k3", 3.into()),
        ];

        let cmd = CommandRequest::new_hmset("t1", pairs);
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hgetall("t1");
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", 1.into()),
                Kvpair::new("k2", 2.into()),
                Kvpair::new("k3", 3.into()),
            ],
        )
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();

        let pairs = vec![
            Kvpair::new("k1", 1.into()),
            Kvpair::new("k2", 2.into()),
            Kvpair::new("k3", 3.into()),
        ];

        let cmd = CommandRequest::new_hmset("t1", pairs);
        dispatch(cmd, &store);

        let keys = vec!["k1".to_string(), "k2".to_string(), "k3".to_string()];
        let cmd = CommandRequest::new_hmget("t1", keys);
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", 1.into()),
                Kvpair::new("k2", 2.into()),
                Kvpair::new("k3", 3.into()),
            ],
        )
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();

        let cmd = CommandRequest::new_hset("t1", "k1", 1.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hdel("t1", "k1");
        let res = dispatch(cmd, &store);

        assert_res_ok(res, &[], &[Kvpair::new("k1", 1.into())]);
        let cmd = CommandRequest::new_hdel("t1", "k1");
        let res = dispatch(cmd, &store);

        assert_res_ok(
            res,
            &[],
            &[Kvpair {
                key: "k1".to_string(),
                value: None,
            }],
        );
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();

        let pairs = vec![
            Kvpair::new("k1", 1.into()),
            Kvpair::new("k2", 2.into()),
            Kvpair::new("k3", 3.into()),
        ];

        let cmd = CommandRequest::new_hmset("t1", pairs);
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hmdel(
            "t1",
            vec!["k1".to_string(), "k2".to_string(), "k4".to_string()],
        );

        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", 1.into()),
                Kvpair::new("k2", 1.into()),
                Kvpair {
                    key: "k4".to_string(),
                    value: None,
                },
            ],
        );
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();

        let cmd = CommandRequest::new_hset("t1", "k1", 1.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hexist("t1", "k1");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[true.into()], &[]);

        let cmd = CommandRequest::new_hexist("t1", "k2");
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[false.into()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();

        let cmd = CommandRequest::new_hset("t1", "k1", 1.into());
        dispatch(cmd, &store);

        let cmd = CommandRequest::new_hmexist("t1", vec!["k1".to_string(), "k2".to_string()]);
        let res = dispatch(cmd, &store);
        assert_res_ok(
            res,
            &[],
            &[
                Kvpair::new("k1", true.into()),
                Kvpair::new("k2", false.into()),
            ],
        );
    }

    fn dispatch(cmd: CommandRequest, store: &dyn Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hget(v) => v.execute(store),
            RequestData::Hgetall(v) => v.execute(store),
            RequestData::Hset(v) => v.execute(store),
            RequestData::Hmset(v) => v.execute(store),
            RequestData::Hmget(v) => v.execute(store),
            RequestData::Hdel(v) => v.execute(store),
            RequestData::Hmdel(v) => v.execute(store),
            RequestData::Hexist(v) => v.execute(store),
            RequestData::Hmexist(v) => v.execute(store),
        }
    }
}
