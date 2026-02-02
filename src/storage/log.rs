use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::storage::wal::{WALHeader, WALRecord};

// Re-export types from sub-modules for backward compatibility
pub use super::block::{Block, BlockBuilder};

pub use super::constant::{WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};

pub use super::record::{
    decode_record,
    encode_record,
    encode_tombstone_record,
    // Backward compatibility exports (deprecated)
    DecodeRecordResult,
    Record,
    RecordType,
};

pub use super::sstable::{
    flush_memtable, read_sstable_bloom, read_sstable_footer, read_sstable_index,
    read_sstable_sparse_index, search_sstable, search_sstable_sparse, search_sstable_with_bloom,
    IndexEntry, SSTableFooter, SparseIndexEntry,
};

/// Write WAL header to a file
fn write_wal_header<W: Write>(mut writer: W) -> Result<(), std::io::Error> {
    let header = WALHeader::new();
    writer.write_all(&header.encode())?;
    writer.flush()?;
    Ok(())
}

/// Read WAL header from a file
pub fn read_wal_header<R: Read>(mut reader: R) -> Result<WALHeader, std::io::Error> {
    let mut buf = [0u8; WAL_HEADER_SIZE as usize];
    reader.read_exact(&mut buf)?;
    WALHeader::decode(&buf)
}

/// Calculate the next LSN based on current file size
/// LSN is derived from the number of records in the file
fn calculate_next_lsn<R: Read + Seek>(mut reader: R) -> Result<u64, std::io::Error> {
    // Seek to right after the header
    reader.seek(SeekFrom::Start(WAL_HEADER_SIZE))?;

    let mut lsn = 0u64;
    let mut current_offset = WAL_HEADER_SIZE;

    // Count records by attempting to decode them
    loop {
        match decode_record(&mut reader, current_offset) {
            Ok(result) => {
                lsn = result.lsn + 1; // Next LSN is one more than the last record's LSN
                current_offset = result.offset;
            }
            Err(_) => break, // End of file or corrupted record
        }
    }

    Ok(lsn)
}

/// Store a log entry to the Write-Ahead Log (WAL)
/// Returns the offset where the record was written and its LSN
pub fn store_log(
    filename: &str,
    key: &[u8],
    value: &[u8],
    record_type: RecordType,
) -> Result<(u64, u64), std::io::Error> {
    log::trace!(
        "Writing to WAL: {:?} at {}",
        String::from_utf8_lossy(key),
        filename
    );

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(filename)?;

    let file_size = file.metadata()?.len();

    // If file is new (empty), write header
    if file_size == 0 {
        log::debug!("Creating new WAL file with header: {}", filename);
        write_wal_header(&mut file)?;
        file.sync_all()?;
    }

    // Calculate next LSN by reading existing records
    // TODO: need to find a more efficient way to track LSN without scanning the file
    let lsn = 1;

    // Get current position for offset (should be at end after calculate_next_lsn)
    let offset = file.seek(SeekFrom::End(0))?;

    log::trace!("Writing record at offset {} with LSN {}", offset, lsn);

    let wal = WALRecord::new(key, value, record_type, lsn);

    // Write and sync to ensure durability
    wal.encode(&mut file)?;
    file.sync_data()?;

    log::trace!("WAL write complete at offset {} with LSN {}", offset, lsn);

    Ok((offset, lsn))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_and_decode_log() {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        let test_file = "test_wal.log";

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        // Store a record
        let (offset, lsn) =
            store_log(test_file, b"test_key", b"test_value", RecordType::Put).unwrap();
        assert_eq!(offset, WAL_HEADER_SIZE); // First record is right after header
        assert_eq!(lsn, 0); // First LSN is 0

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = decode_record(file, offset).unwrap();

        assert_eq!(result.key, b"test_key");
        assert_eq!(result.val, b"test_value");
        assert_eq!(result.record_type, RecordType::Put);
        assert_eq!(result.lsn, 0);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_store_tombstone() {
        let test_file = "test_wal_tombstone.log";

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        // Store a tombstone
        let (offset, lsn) = store_log(test_file, b"deleted_key", b"", RecordType::Delete).unwrap();
        assert_eq!(offset, WAL_HEADER_SIZE);
        assert_eq!(lsn, 0);

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = decode_record(file, offset).unwrap();

        assert_eq!(result.key, b"deleted_key");
        assert_eq!(result.record_type, RecordType::Delete);
        assert_eq!(result.lsn, 0);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_wal_header() {
        let test_file = "test_wal_header.log";

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        // Create a new WAL file (header will be written automatically)
        store_log(test_file, b"key1", b"value1", RecordType::Put).unwrap();

        // Read and validate header
        let file = std::fs::File::open(test_file).unwrap();
        let header = read_wal_header(file).unwrap();

        assert_eq!(&header.magic, WAL_MAGIC);
        assert_eq!(header.version, WAL_VERSION);
        assert_eq!(header.last_checkpoint, 0);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_lsn_increment() {
        let test_file = "test_wal_lsn.log";

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        // Store multiple records
        let (_offset1, lsn1) = store_log(test_file, b"key1", b"value1", RecordType::Put).unwrap();
        let (_offset2, lsn2) = store_log(test_file, b"key2", b"value2", RecordType::Put).unwrap();
        let (_offset3, lsn3) = store_log(test_file, b"key3", b"value3", RecordType::Put).unwrap();

        // LSN should increment
        assert_eq!(lsn1, 0);
        assert_eq!(lsn2, 1);
        assert_eq!(lsn3, 2);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }
}
