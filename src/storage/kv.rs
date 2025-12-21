use crate::{
    api::api::KVEngine,
    storage::{self, log::RecordType},
};

#[derive(Debug, Default)]
pub struct PersistentKV {
    store: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

impl PersistentKV {
    pub fn new() -> Self {
        PersistentKV {
            store: std::collections::HashMap::new(),
        }
    }
}

impl KVEngine for PersistentKV {
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
    use std::fs::File;

    use crate::storage::log::decode_record;

    use super::*;

    #[test]
    fn test_in_memory_kv() {
        let mut kv = PersistentKV::new();
        kv.put(b"key1", "value1".as_bytes());
        assert_eq!(kv.get(b"key1"), Some(b"value1".to_vec()));
        kv.delete(b"key1");
        assert_eq!(kv.get(b"key1"), None);
    }

    #[test]
    fn test_integration_decode_log() {
        let mut kv = PersistentKV::new();
        for i in 1..100 {
            kv.put(
                format!("key{i}").as_bytes(),
                &format!("value{i}").as_bytes(),
            );
        }

        let file = File::open("app.log").unwrap();
        // from the record we need to find key99
        let mut found = false;
        let mut offset = 0 as u64;
        while !found {
            let result = decode_record(&file, offset).unwrap();

            // adjust the offset accordingly
            offset = result.offset;

            // check the value
            if result.key == b"key44" {
                println!("offset were found at: {:?}", result);
                assert_eq!(result.val, b"value44");
                found = true;
            }
        }

        assert_eq!(found, true)
    }
}
