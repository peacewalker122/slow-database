use crossbeam_skiplist::SkipMap;
use std::{borrow::Cow, fs::File};

use crate::{
    api::api::KVEngine,
    error::DBError,
    storage::{
        self,
        log::{
            RecordType, read_sstable_bloom, read_sstable_footer, read_sstable_sparse_index,
            search_sstable_sparse,
        },
        manifest,
    },
};

#[derive(Debug)]
pub struct PersistentKV {
    pub memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    pub levelstore: Vec<Vec<u8>>,

    memtable_size: u64,
}

impl PersistentKV {
    pub fn new() -> Self {
        // Read manifest on startup to populate levelstore
        let level_map = manifest::read_manifest().unwrap_or_else(|e| {
            log::warn!(
                "Failed to read manifest: {}, starting with empty levelstore",
                e
            );
            std::collections::HashMap::new()
        });

        // Convert HashMap<u32, Vec<String>> to Vec<Vec<u8>> for levelstore
        // This is a temporary structure until we refactor levelstore properly
        let mut levelstore = Vec::new();
        for (_level, files) in level_map.iter() {
            for filename in files {
                levelstore.push(filename.as_bytes().to_vec());
            }
        }

        log::info!(
            "PersistentKV initialized with {} files from manifest",
            levelstore.len()
        );

        PersistentKV {
            memtable: SkipMap::new(),
            levelstore,
            memtable_size: 0,
        }
    }
}

impl Default for PersistentKV {
    fn default() -> Self {
        Self::new()
    }
}

// TODO:
// Need find another way to much more friendly with the compiler itself

impl KVEngine for PersistentKV {
    fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        log::trace!("Getting key: {:?}", String::from_utf8_lossy(key));

        // 1. Search in memtable first (most recent data)
        if let Some(entry) = self.memtable.get(key) {
            log::debug!("Key found in memtable: {:?}", String::from_utf8_lossy(key));
            let (record_type, value) = entry.value();
            match record_type {
                RecordType::Put => return Ok(Some(Cow::Owned(value.clone()))), // Return value from memtable, Allocated
                RecordType::Delete => return Ok(None), // Tombstone - key deleted
            }
        }

        // 2. Key not in memtable, search through all SSTable files in levelstore
        log::debug!(
            "Key not in memtable, searching {} SSTable files",
            self.levelstore.len()
        );

        let mut buf = vec![];
        for (idx, filename_bytes) in self.levelstore.iter().enumerate() {
            let filename = String::from_utf8_lossy(filename_bytes);

            log::trace!(
                "Searching SSTable {}/{}: {}",
                idx + 1,
                self.levelstore.len(),
                filename
            );

            // Try to open the SSTable file
            let file = match File::open(filename.as_ref()) {
                Ok(f) => f,
                Err(e) => {
                    log::warn!("Failed to open SSTable {}: {}, skipping", filename, e);
                    continue;
                }
            };

            // Read footer and bloom filter
            let footer = match read_sstable_footer(&file) {
                Ok(f) => f,
                Err(e) => {
                    log::warn!("Failed to read footer from {}: {}, skipping", filename, e);
                    continue;
                }
            };

            let bloom = match read_sstable_bloom(&file, &footer) {
                Ok(b) => b,
                Err(e) => {
                    log::warn!(
                        "Failed to read bloom filter from {}: {}, skipping",
                        filename,
                        e
                    );
                    continue;
                }
            };

            // Check bloom filter first - if it says key doesn't exist, skip this file
            if !bloom.contains(key) {
                log::trace!("Bloom filter in {}: key definitely not present", filename);
                continue;
            }

            log::trace!(
                "Bloom filter in {}: key might be present, checking sparse index",
                filename
            );

            // Bloom filter says key might exist, read sparse index and search
            let sparse_index = match read_sstable_sparse_index(&file, &footer) {
                Ok(idx) => idx,
                Err(e) => {
                    log::warn!(
                        "Failed to read sparse index from {}: {}, skipping",
                        filename,
                        e
                    );

                    continue;
                }
            };

            match search_sstable_sparse(&file, key, &sparse_index, &mut buf)? {
                Some(val) => {
                    log::debug!(
                        "Key found in SSTable {}: {:?}",
                        filename,
                        String::from_utf8_lossy(key)
                    );

                    return Ok(Some(Cow::Owned(val)));
                }
                None => {
                    log::trace!(
                        "Key not found in {} (bloom filter false positive)",
                        filename
                    );
                    // Continue searching in next SSTable
                }
            }
        }

        // Key not found in any SSTable
        log::debug!(
            "Key not found in any SSTable: {:?}",
            String::from_utf8_lossy(key)
        );

        Ok(None)
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        log::trace!(
            "Putting key: {:?}, value size: {} bytes",
            String::from_utf8_lossy(&key),
            value.len()
        );

        // Write to WAL and get the LSN
        let (_offset, lsn) = storage::log::store_log("app.log", &key, &value, RecordType::Put)?;
        log::trace!("WAL write complete with LSN: {}", lsn);

        let size = key.len() + value.len();

        self.memtable.insert(key, (RecordType::Put, value));

        // add the size of key and value to memtable_size
        self.memtable_size += size as u64;

        log::debug!("Memtable size: {} bytes", self.memtable_size);

        if self.memtable_size >= storage::constant::MEMTABLE_SIZE_THRESHOLD {
            log::info!(
                "Memtable size threshold reached ({} >= {}), flushing to SSTable",
                self.memtable_size,
                storage::constant::MEMTABLE_SIZE_THRESHOLD
            );

            // Generate unique file ID using timestamp (nanoseconds since UNIX_EPOCH)
            let file_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            // Replace memtable with a new empty one, taking ownership of the old one
            let old_memtable = std::mem::replace(&mut self.memtable, SkipMap::new());
            self.memtable_size = 0;

            std::thread::spawn(move || {
                if let Err(e) = storage::log::flush_memtable(&old_memtable, "app.db", 0, file_id) {
                    log::error!("Failed to flush memtable to SSTable: {}", e);
                }
            });

            // Level 0 (L0) is used for memtable flushes in LSM-tree
            const FILENAME: &str = "app.db";

            // Record file creation in manifest
            manifest::add_file(0, FILENAME)?;
            log::debug!("Added {} to manifest at level 0", FILENAME);

            // set the current memtable to a new one
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
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        // Arrange - Setup KV store
        let mut kv = PersistentKV::new();

        // Act - Put a key-value pair
        kv.put(b"key1".to_vec(), b"value1".to_vec())
            .expect("put failed");

        // Assert - Verify value exists
        let result = kv.get(b"key1").expect("get failed");
        assert!(result.is_some(), "expected Some, got None");
        assert_eq!(
            result.as_ref().unwrap().as_slice(),
            b"value1",
            "unexpected result from get(key1)"
        );

        // Act - Delete the key
        kv.delete(b"key1");

        // Assert - Verify key is deleted (returns None)
        let result = kv.get(b"key1").expect("get failed");
        assert!(
            result.is_none(),
            "expected None after delete, got: {:?}",
            result
        );
    }

    #[test]
    fn test_active_data() {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        // Arrange - Setup KV store
        let mut kv = PersistentKV::new();

        // Act - Insert 999 key-value pairs
        (1..1000)
            .try_for_each(|i| -> Result<(), DBError> {
                kv.put(
                    format!("key{i}").as_bytes().to_vec(),
                    format!("valuefromkey{i}").as_bytes().to_vec(),
                )?;

                Ok(())
            })
            .unwrap();

        // Assert - Check non-existent key returns None
        let result = kv.get(b"keyyangemangkosong").unwrap();
        assert!(result.is_none(), "expected None for non-existent key");

        // Assert - Check existing key returns correct value
        let result2 = kv.get(b"key99").unwrap();
        assert!(result2.is_some(), "expected Some for key99");
        assert_eq!(
            result2.as_ref().unwrap().as_slice(),
            b"valuefromkey99",
            "unexpected value for key99"
        );
    }
}
