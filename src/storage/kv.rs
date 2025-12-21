use crate::{
    api::api::KVEngine,
    storage::{self, log::RecordType},
};

#[derive(Debug, Default)]
pub struct InMemoryKV {
    store: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

impl InMemoryKV {
    pub fn new() -> Self {
        InMemoryKV {
            store: std::collections::HashMap::new(),
        }
    }
}

impl KVEngine for InMemoryKV {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.store.get(key).cloned()
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        storage::log::store_log("app.log", key, value, RecordType::Put).unwrap();

        self.store.insert(key.to_vec(), value.to_vec());
    }

    fn delete(&mut self, key: &[u8]) {
        self.store.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_kv() {
        let mut kv = InMemoryKV::new();
        kv.put(b"key1", "value1".as_bytes());
        assert_eq!(kv.get(b"key1"), Some(b"value1".to_vec()));
        kv.delete(b"key1");
        assert_eq!(kv.get(b"key1"), None);
    }
}
