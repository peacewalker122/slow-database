use std::fs::File;

use crate::{
    api::api::KVEngine,
    error::DBError,
    storage::{
        self,
        log::{RecordType, decode_record},
        skiplist::SkipList,
    },
};

#[derive(Debug)]
pub struct PersistentKV {
    store: std::collections::HashMap<Vec<u8>, u64>,
    pub memtable: SkipList,
}

impl PersistentKV {
    pub fn new() -> Self {
        PersistentKV {
            store: std::collections::HashMap::new(),
            memtable: SkipList::new(),
        }
    }
}

impl KVEngine for PersistentKV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, DBError> {
        let offset = self.store.get(key);

        let file = File::open("app.log")?;

        // linearly search the file
        let result = decode_record(file, *offset.ok_or_else(|| DBError::NotFound)?)?;

        return Ok(result.val);
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DBError> {
        let offset = storage::log::store_log("app.log", key, value, RecordType::Put)?;

        self.store.insert(key.to_vec(), offset);
        self.memtable.insert(key.to_vec(), (RecordType::Put, value.to_vec()));

        storage::log::flush_memtable(&mut self.memtable)?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) {
        // TODO: handle to remove the file into the log.
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

        kv.put(b"key1", b"value1").expect("put failed");

        // --- assert value exists ---
        let result = kv.get(b"key1");
        assert_eq!(
            result.unwrap(),
            b"value1",
            "unexpected result from get(key1)"
        );

        // --- delete ---
        kv.delete(b"key1");

        // --- assert not found ---
        let result = kv.get(b"key1");
        assert!(
            matches!(result, Err(DBError::NotFound)),
            "expected NotFound, got: {:?}",
            result
        );
    }

    #[test]
    fn test_integration_decode_log() {
        let mut kv = PersistentKV::new();
        for i in 0..100 {
            kv.put(
                format!("key{i}").as_bytes(),
                &format!("value{i}").as_bytes(),
            )
            .unwrap();
        }

        let file = File::open("app.log").unwrap();
        // from the record we need to find key99
        let mut found = false;
        let offset = kv
            .store
            .get("key44".as_bytes())
            .expect("harusnya ada sihhh");

        let result = decode_record(&file, *offset).unwrap();
        println!("got value: {:?}", result);

        // check the value
        if result.key == b"key44" {
            println!("offset were found at: {:?}", result);
            assert_eq!(result.val, b"value44");
            found = true;
        }

        assert_eq!(found, true)
    }
}
