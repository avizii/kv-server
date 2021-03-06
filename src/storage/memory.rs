use crate::error::KvError;
use crate::storage::Storage;
use crate::{Kvpair, StorageIter, Value};
use dashmap::mapref::one::Ref;
use dashmap::DashMap;

#[derive(Clone, Debug, Default)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    pub fn new() -> Self {
        Self::default()
    }

    fn get_or_create(&self, name: &str) -> Ref<String, DashMap<String, Value>> {
        match self.tables.get(name) {
            None => {
                let entry = self.tables.entry(name.into()).or_default();
                entry.downgrade()
            }
            Some(table) => table,
        }
    }
}

impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create(table);
        Ok(table.get(key).map(|v| v.value().clone()))
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create(table);
        Ok(table.insert(key, value))
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let table = self.get_or_create(table);
        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create(table);
        Ok(table.remove(key).map(|(_, v)| v))
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let table = self.get_or_create(table);
        Ok(table
            .iter()
            .map(|e| Kvpair::new(e.key(), e.value().clone()))
            .collect())
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let table = self.get_or_create(table);
        let iter = StorageIter::new(table.clone().into_iter());
        Ok(Box::new(iter))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::memory::MemTable;
    use crate::{test_basi_interface, test_get_all, test_get_iter};

    #[test]
    fn mem_table_basic_interface_should_work() {
        let store = MemTable::new();
        test_basi_interface(&store);
    }

    #[test]
    fn mem_table_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(&store);
    }

    #[test]
    fn mem_table_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(&store);
    }
}
