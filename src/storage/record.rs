use std::io::{Cursor, Read, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};

/// Get current Unix timestamp in milliseconds
pub fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
}

/// Represents a key-value record with metadata
#[derive(Debug, Clone)]
pub struct Record<'a> {
    pub key: &'a [u8],
    pub value: &'a [u8],
    pub record_type: RecordType,
    pub timestamp: u64, // Unix timestamp in milliseconds
    pub offset: u64,
}

impl<'a> Record<'a> {
    pub fn new(key: &'a [u8], value: &'a [u8], record_type: RecordType, timestamp: u64) -> Self {
        Self {
            key,
            value,
            record_type,
            timestamp,
            offset: 0,
        }
    }

    /// Create a tombstone record (delete marker)
    pub fn tombstone(key: &'a [u8], timestamp: u64) -> Self {
        Self {
            key,
            value: &[],
            record_type: RecordType::Delete,
            timestamp,
            offset: 0,
        }
    }

    /// Encode the record to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 8 + 8 + 8 + self.key.len() + self.value.len() + 4);

        let checksum = crc32fast::hash(&self.value);

        buf.extend_from_slice(&(self.record_type as u8).to_be_bytes());
        buf.extend_from_slice(&self.timestamp.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&(self.value.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf.extend_from_slice(&checksum.to_be_bytes());

        buf
    }

    /// Decode a record from a reader at a specific offset
    pub fn decode(
        reader: &mut Cursor<&'a [u8]>,
        offset: u64,
    ) -> Result<Record<'a>, std::io::Error> {
        reader.seek(SeekFrom::Start(offset))?;

        let mut record_type_buf = [0u8; 1];
        reader.read_exact(&mut record_type_buf)?;
        let record_type_byte = record_type_buf[0];

        let mut u64_buf = [0u8; 8];
        reader.read_exact(&mut u64_buf)?;
        let timestamp = u64::from_be_bytes(u64_buf);

        reader.read_exact(&mut u64_buf)?;
        let key_len = u64::from_be_bytes(u64_buf) as usize;

        reader.read_exact(&mut u64_buf)?;
        let value_len = u64::from_be_bytes(u64_buf) as usize;

        // Use current cursor position to read key and value from slice
        let current_pos = reader.position() as usize;
        let slice = reader.get_ref();

        let key = &slice[current_pos..current_pos + key_len];
        let value = &slice[current_pos + key_len..current_pos + key_len + value_len];

        // Seek past key and value to checksum position
        reader.seek(SeekFrom::Start(
            reader.position() + key_len as u64 + value_len as u64,
        ))?;

        let mut checksum_buf = [0u8; 4];
        reader.read_exact(&mut checksum_buf)?;
        let checksum = u32::from_be_bytes(checksum_buf);

        if crc32fast::hash(&value) != checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        let record_type = match record_type_byte {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        let next_offset = offset + (1 + 8 + 8 + 8 + key_len + value_len + 4) as u64;

        Ok(Self {
            key: &key,
            value: &value,
            record_type,
            timestamp,
            offset: next_offset,
        })
    }
}

// Trait implementations for Record ordering and equality
// Note: Ord/PartialOrd compare by key first, then timestamp (descending - newer first)
// This is the typical LSM-tree pattern where we want the latest version of a key
// PartialEq/Eq compare all fields for strict equality

impl<'a> PartialEq for Record<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.value == other.value
            && self.record_type == other.record_type
            && self.timestamp == other.timestamp
    }
}

impl<'a> Eq for Record<'a> {}

impl<'a> PartialOrd for Record<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Record<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compare the key first.

        let res = self.key.cmp(other.key);
        if res != std::cmp::Ordering::Equal {
            return res;
        }

        // If keys are equal, compare timestamps in descending order (newer first)
        other.timestamp.cmp(&self.timestamp)
    }
}

// ============================================================================
// Backward Compatibility Layer for WAL operations
// ============================================================================

/// Result structure for decode_record function (backward compatibility)
/// This represents a decoded WAL record with LSN
#[derive(Debug)]
pub struct DecodeRecordResult {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub record_type: RecordType,
    pub lsn: u64,
    pub offset: u64,
}

/// Decode a WAL record from a reader at a specific offset (backward compatibility)
/// This function reads WAL format records which include LSN
pub fn decode_record<R: Read + Seek>(
    mut reader: R,
    offset: u64,
) -> Result<DecodeRecordResult, std::io::Error> {
    reader.seek(SeekFrom::Start(offset))?;

    // Read record type
    let mut record_type_buf = [0u8; 1];
    reader.read_exact(&mut record_type_buf)?;
    let record_type = match record_type_buf[0] {
        1 => RecordType::Put,
        2 => RecordType::Delete,
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid record type",
            ));
        }
    };

    // Read LSN
    let mut lsn_buf = [0u8; 8];
    reader.read_exact(&mut lsn_buf)?;
    let lsn = u64::from_be_bytes(lsn_buf);

    // Read key length
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf)?;
    let key_len = u64::from_be_bytes(len_buf) as usize;

    // Read value length
    reader.read_exact(&mut len_buf)?;
    let value_len = u64::from_be_bytes(len_buf) as usize;

    // Read key
    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key)?;

    // Read value
    let mut value = vec![0u8; value_len];
    reader.read_exact(&mut value)?;

    // Read and verify checksum
    let mut checksum_buf = [0u8; 4];
    reader.read_exact(&mut checksum_buf)?;
    let checksum = u32::from_be_bytes(checksum_buf);

    if crc32fast::hash(&value) != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Checksum mismatch",
        ));
    }

    // Calculate next offset
    let next_offset = offset + 1 + 8 + 8 + 8 + key_len as u64 + value_len as u64 + 4;

    Ok(DecodeRecordResult {
        key,
        val: value,
        record_type,
        lsn,
        offset: next_offset,
    })
}

/// Encode a WAL record (backward compatibility)
pub fn encode_record(key: &[u8], value: &[u8], lsn: u64) -> Vec<u8> {
    use super::wal::WALRecord;
    let wal = WALRecord::new(key, value, RecordType::Put, lsn);
    let mut buf = Vec::new();
    wal.encode(&mut buf).expect("Failed to encode record");
    buf
}

/// Encode a tombstone WAL record (backward compatibility)
pub fn encode_tombstone_record(key: &[u8], lsn: u64) -> Vec<u8> {
    use super::wal::WALRecord;
    let wal = WALRecord::new(key, &[], RecordType::Delete, lsn);
    let mut buf = Vec::new();
    wal.encode(&mut buf).expect("Failed to encode record");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // New tests using the Record struct API
    #[test]
    fn test_record_new_and_encode() {
        // Positive test: Verifies that a Record can be created, encoded, and decoded correctly
        let key = b"key1";
        let value = b"value1";
        let timestamp = 1234567890123_u64;
        let record = Record::new(key, value, RecordType::Put, timestamp);
        let encoded = record.encode();

        let decoded = Record::decode(&mut Cursor::new(&encoded), 0).unwrap();

        assert_eq!(decoded.key, b"key1");
        assert_eq!(decoded.value, b"value1");
        assert_eq!(decoded.record_type, RecordType::Put);
    }

    #[test]
    fn test_record_tombstone() {
        // Positive test: Verifies that tombstone (delete) records can be created and decoded correctly
        let key = b"deleted_key";
        let timestamp = 1234567890123_u64;
        let record = Record::tombstone(key, timestamp);
        let encoded = record.encode();

        let decoded = Record::decode(&mut Cursor::new(&encoded), 0).unwrap();

        assert_eq!(decoded.key, b"deleted_key");
        assert_eq!(decoded.value, b"");
        assert_eq!(decoded.record_type, RecordType::Delete);
    }

    #[test]
    fn test_record_decode_calculates_offset() {
        // Positive test: Verifies that the next record offset is calculated correctly after decoding
        let key = b"test";
        let value = b"data";
        let timestamp = 1234567890123_u64;
        let record = Record::new(key, value, RecordType::Put, timestamp);
        let encoded = record.encode();

        let decoded = Record::decode(&mut Cursor::new(&encoded), 0).unwrap();

        // Offset should be: 1 (type) + 8 (timestamp) + 8 (key_len) + 8 (val_len) + 4 (key) + 4 (val) + 4 (checksum) = 37
        assert_eq!(decoded.offset, 37);
    }

    #[test]
    fn test_record_checksum_validation() {
        // Negative test: Verifies that corrupted data is detected via checksum validation
        let key = b"key";
        let value = b"value";
        let timestamp = 1234567890123_u64;
        let record = Record::new(key, value, RecordType::Put, timestamp);
        let mut encoded = record.encode();

        // Corrupt the value section (not the metadata)
        // Format: 1 (type) + 8 (timestamp) + 8 (key_len) + 8 (val_len) + 3 (key) + 5 (value) + 4 (checksum)
        // Value starts at offset 28 (1 + 8 + 8 + 8 + 3), so corrupt the first byte of value
        let value_offset = 1 + 8 + 8 + 8 + 3; // After metadata and key
        encoded[value_offset] ^= 0xFF;

        let result = Record::decode(&mut Cursor::new(&encoded), 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn test_record_timestamp_encoding() {
        // Positive test: Verifies that timestamp is preserved through encode/decode cycle
        let key = b"timestamped_key";
        let value = b"timestamped_value";
        let timestamp = 1234567890123_u64;
        let record = Record::new(key, value, RecordType::Put, timestamp);
        let encoded = record.encode();

        let decoded = Record::decode(&mut Cursor::new(&encoded), 0).unwrap();

        assert_eq!(decoded.timestamp, timestamp);
        assert_eq!(decoded.key, key);
        assert_eq!(decoded.value, value);
    }

    #[test]
    fn test_record_timestamp_extreme_values() {
        // Positive test: Verifies that extreme timestamp values (0 and u64::MAX) are handled correctly
        let key = b"extreme_key";
        let value = b"extreme_value";

        // Test with timestamp = 0
        let record_min = Record::new(key, value, RecordType::Put, 0);
        let encoded_min = record_min.encode();
        let decoded_min = Record::decode(&mut Cursor::new(&encoded_min), 0).unwrap();
        assert_eq!(decoded_min.timestamp, 0);

        // Test with timestamp = u64::MAX
        let record_max = Record::new(key, value, RecordType::Put, u64::MAX);
        let encoded_max = record_max.encode();
        let decoded_max = Record::decode(&mut Cursor::new(&encoded_max), 0).unwrap();
        assert_eq!(decoded_max.timestamp, u64::MAX);
    }

    #[test]
    fn test_record_ordering_by_key() {
        // Positive test: Verifies that records with different keys are ordered correctly (lexicographically)
        let timestamp = 1234567890123_u64;
        let record_a = Record::new(b"apple", b"value1", RecordType::Put, timestamp);
        let record_b = Record::new(b"banana", b"value2", RecordType::Put, timestamp);
        let record_c = Record::new(b"cherry", b"value3", RecordType::Put, timestamp);

        assert!(record_a < record_b);
        assert!(record_b < record_c);
        assert!(record_a < record_c);
    }

    #[test]
    fn test_record_ordering_by_timestamp() {
        // Positive test: Verifies that records with same key are ordered by timestamp descending (newer first)
        let key = b"same_key";
        let record_old = Record::new(key, b"old_value", RecordType::Put, 1000);
        let record_mid = Record::new(key, b"mid_value", RecordType::Put, 2000);
        let record_new = Record::new(key, b"new_value", RecordType::Put, 3000);

        // Newer timestamps should come BEFORE older ones (descending order)
        assert!(record_new < record_mid);
        assert!(record_mid < record_old);
        assert!(record_new < record_old);
    }

    #[test]
    fn test_record_equality_same_timestamp() {
        // Positive test: Verifies that identical records (including timestamp) are considered equal
        let key = b"eq_key";
        let value = b"eq_value";
        let timestamp = 1234567890123_u64;

        let record1 = Record::new(key, value, RecordType::Put, timestamp);
        let record2 = Record::new(key, value, RecordType::Put, timestamp);

        assert_eq!(record1, record2);
    }

    #[test]
    fn test_record_equality_different_timestamp() {
        // Negative test: Verifies that records differing only in timestamp are NOT considered equal
        let key = b"eq_key";
        let value = b"eq_value";

        let record1 = Record::new(key, value, RecordType::Put, 1000);
        let record2 = Record::new(key, value, RecordType::Put, 2000);

        assert_ne!(record1, record2);
    }
}
