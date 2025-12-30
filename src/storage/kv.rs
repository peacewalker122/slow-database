use std::fs::File;

use crate::{
    api::api::KVEngine,
    error::DBError,
    storage::{
        self,
        log::{
            RecordType, read_sstable_bloom, read_sstable_footer, read_sstable_sparse_index,
            search_sstable_sparse,
        },
        skiplist::SkipList,
    },
};

#[derive(Debug)]
pub struct PersistentKV {
    store: std::collections::HashMap<Vec<u8>, u64>,
    pub memtable: SkipList,

    memtable_size: u64,
}

impl PersistentKV {
    pub fn new() -> Self {
        PersistentKV {
            store: std::collections::HashMap::new(),
            memtable: SkipList::new(),
            memtable_size: 0,
        }
    }
}

impl KVEngine for PersistentKV {
    fn get(&self, key: &[u8]) -> Result<Vec<u8>, DBError> {
        log::trace!("Getting key: {:?}", String::from_utf8_lossy(key));

        // search on memtable first, if not found search on file
        if let Some((record_type, value)) = self.memtable.get(key) {
            log::debug!("Key found in memtable: {:?}", String::from_utf8_lossy(key));
            match record_type {
                RecordType::Put => return Ok(value),
                RecordType::Delete => return Ok(vec![]), // empty
            }
        }

        log::debug!(
            "Key not in memtable, searching SSTable: {:?}",
            String::from_utf8_lossy(key)
        );
        let file = File::open("app.db")?;
        let footer = read_sstable_footer(&file)?;
        let bloom = read_sstable_bloom(&file, &footer)?;

        // Check bloom filter first - if it says key doesn't exist, we can skip searching
        if !bloom.contains(key) {
            log::debug!("Bloom filter: key definitely not in SSTable");
            return Ok(vec![]);
        }

        log::debug!("Bloom filter: key might be in SSTable, checking sparse index");
        let sparse_index = read_sstable_sparse_index(&file, &footer)?;
        let result = search_sstable_sparse(&file, key, &sparse_index)?;

        return match result {
            Some(val) => {
                log::debug!("Key found in SSTable: {:?}", String::from_utf8_lossy(key));
                Ok(val)
            }
            None => {
                log::debug!("Key not found: {:?}", String::from_utf8_lossy(key));
                Ok(vec![])
            }
        };
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DBError> {
        log::trace!(
            "Putting key: {:?}, value size: {} bytes",
            String::from_utf8_lossy(key),
            value.len()
        );

        storage::log::store_log("app.log", key, value, RecordType::Put)?; // WAL
        self.memtable
            .insert(key.to_vec(), (RecordType::Put, value.to_vec()));

        // add the size of key and value to memtable_size
        self.memtable_size += (key.len() + value.len()) as u64;

        log::debug!("Memtable size: {} bytes", self.memtable_size);

        if self.memtable_size >= storage::constant::MEMTABLE_SIZE_THRESHOLD {
            log::info!(
                "Memtable size threshold reached ({} >= {}), flushing to SSTable",
                self.memtable_size,
                storage::constant::MEMTABLE_SIZE_THRESHOLD
            );
            storage::log::flush_memtable(self.memtable.clone())?; // flush to SSTable when certain size reached

            // set the current memtable to a new one
            self.memtable = SkipList::new();
            self.memtable_size = 0;
            log::info!("Memtable flushed and reset");
        }

        Ok(())
    }

    fn delete(&mut self, key: &[u8]) {
        log::debug!("Deleting key: {:?}", String::from_utf8_lossy(key));
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

    #[test]
    fn test_active_data() {
        let mut kv = PersistentKV::new();

        (1..10000)
            .try_for_each(|i| -> Result<(), DBError> {
                kv.put(
                    format!("key{i}").as_bytes(),
                    format!("valuefromkey{i}").as_bytes(),
                )?;

                Ok(())
            })
            .unwrap();

        // check the missing one
        let result = kv.get(b"keyyangemangkosong").unwrap();
        assert_eq!(result, b"");

        // check one of the exist key
        let result2 = kv.get(b"key88").unwrap();
        assert_eq!(result2, b"valuefromkey88"); // NOTE: error since we don't yet implement compaction
    }
}
