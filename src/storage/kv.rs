use std::fs::File;

use crate::{
    api::api::KVEngine,
    error::DBError,
    storage::{
        self,
        log::{RecordType, decode_record, read_sstable_footer, read_sstable_index, search_sstable},
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
        // search on memtable first, if not found search on file
        if let Some((record_type, value)) = self.memtable.get(key) {
            match record_type {
                RecordType::Put => return Ok(value),
                RecordType::Delete => return Ok(vec![]), // empty
            }
        }
        let file = File::open("app.db")?;
        let footer = read_sstable_footer(&file)?;
        let index = read_sstable_index(&file, &footer)?;
        let result = search_sstable(&file, key, &index)?;

        return match result {
            Some(val) => Ok(val),
            None => Ok(vec![]),
        };
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DBError> {
        storage::log::store_log("app.log", key, value, RecordType::Put)?; // WAL
        self.memtable
            .insert(key.to_vec(), (RecordType::Put, value.to_vec()));

        storage::log::flush_memtable(&mut self.memtable)?; // flush to SSTable when certain size reached
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) {
        self.memtable
            .insert(key.to_vec(), (RecordType::Delete, vec![]));
    }
}

#[cfg(test)]
mod tests {
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
            matches!(result.as_deref(), Ok([])),
            "expected NotFound, got: {:?}",
            result
        );
    }
}
