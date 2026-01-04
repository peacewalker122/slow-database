use std::fs::OpenOptions;
use std::io::Write;

// Re-export types from sub-modules for backward compatibility
pub use super::block::{Block, BlockBuilder};
pub use super::record::{
    decode_record, encode_record, encode_tombstone_record, DecodeRecordResult, RecordType,
};
pub use super::sstable::{
    flush_memtable, read_sstable_bloom, read_sstable_footer, read_sstable_index,
    read_sstable_sparse_index, search_sstable, search_sstable_sparse, search_sstable_with_bloom,
    IndexEntry, SSTableFooter, SparseIndexEntry,
};

/// Store a log entry to the Write-Ahead Log (WAL)
/// Returns the offset where the record was written
pub fn store_log(
    filename: &str,
    key: &[u8],
    value: &[u8],
    record_type: RecordType,
) -> Result<u64, std::io::Error> {
    log::trace!(
        "Writing to WAL: {:?} at {}",
        String::from_utf8_lossy(key),
        filename
    );

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;

    let offset = file.metadata()?.len();

    let record = match record_type {
        RecordType::Delete => encode_tombstone_record(key),
        RecordType::Put => encode_record(key, value, RecordType::Put),
    };

    // Write and sync to ensure durability
    file.write_all(&record)?;
    file.sync_data()?;

    log::trace!("WAL write complete at offset {}", offset);

    Ok(offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_and_decode_log() {
        let test_file = "test_wal.log";

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        // Store a record
        let offset = store_log(test_file, b"test_key", b"test_value", RecordType::Put).unwrap();
        assert_eq!(offset, 0);

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = decode_record(file, offset).unwrap();

        assert_eq!(result.key, b"test_key");
        assert_eq!(result.val, b"test_value");
        assert_eq!(result.record_type, RecordType::Put);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_store_tombstone() {
        let test_file = "test_wal_tombstone.log";

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        // Store a tombstone
        let offset = store_log(test_file, b"deleted_key", b"", RecordType::Delete).unwrap();

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = decode_record(file, offset).unwrap();

        assert_eq!(result.key, b"deleted_key");
        assert_eq!(result.record_type, RecordType::Delete);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }
}
