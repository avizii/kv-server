use crate::*;
use crate::error::KvError;

impl CommandService for Hset {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        match self.pair {
            None => Value::default().into(),
            Some(v) => match store.set(&self.table, v.key, v.value.unwrap_or_default()) {
                Ok(Some(v)) => v.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            }
        }
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

impl CommandService for Hgetall {
    fn execute(self, store: &dyn Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command_request::RequestData;
    use super::*;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &[Value::default()], &[]);

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = dispatch(cmd, &store);
        assert_res_ok(res, &["world".into()], &[]);
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
        assert_res_ok(res, &[], &[
            Kvpair::new("k1", 1.into()),
            Kvpair::new("k2", 2.into()),
            Kvpair::new("k3", 3.into()),
            Kvpair::new("k4", 4.into()),
        ]);
    }

    fn dispatch(cmd: CommandRequest, store: &dyn Storage) -> CommandResponse {
        match cmd.request_data.unwrap() {
            RequestData::Hget(v) => v.execute(store),
            RequestData::Hgetall(v) => v.execute(store),
            RequestData::Hset(v) => v.execute(store),
            _ => todo!(),
        }
    }
}