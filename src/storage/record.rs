use std::io::{Read, Seek, SeekFrom};

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum RecordType {
    Put = 1,
    Delete = 2,
}

#[derive(Debug)]
pub struct DecodeRecordResult {
    pub key: Vec<u8>,
    pub val: Vec<u8>,
    pub offset: u64,
    pub record_type: RecordType,
}

/// Encode a key-value record with checksum
pub fn encode_record(key: &[u8], value: &[u8], record_type: RecordType) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 8 + key.len() + value.len() + 4);

    let checksum = crc32fast::hash(value);

    buf.extend_from_slice(&(record_type as u8).to_be_bytes());
    buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
    buf.extend_from_slice(&(value.len() as u64).to_be_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    buf.extend_from_slice(&checksum.to_be_bytes());

    buf
}

/// Encode a tombstone record (delete marker)
pub fn encode_tombstone_record(key: &[u8]) -> Vec<u8> {
    encode_record(key, b"", RecordType::Delete)
}

/// Decode a record from a reader at a specific offset
pub fn decode_record<R>(mut reader: R, offset: u64) -> Result<DecodeRecordResult, std::io::Error>
where
    R: Read + Seek,
{
    reader.seek(SeekFrom::Start(offset))?;

    let mut record_type_buf = [0u8; 1];
    reader.read_exact(&mut record_type_buf)?;
    let record_type = record_type_buf[0];

    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf)?;
    let key_len = u64::from_be_bytes(len_buf) as usize;

    reader.read_exact(&mut len_buf)?;
    let value_len = u64::from_be_bytes(len_buf) as usize;

    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key)?;

    let mut value = vec![0u8; value_len];
    reader.read_exact(&mut value)?;

    let mut crc_buf = [0u8; 4];
    reader.read_exact(&mut crc_buf)?;
    let checksum = u32::from_be_bytes(crc_buf);

    if crc32fast::hash(&value) != checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Checksum mismatch",
        ));
    }

    let result = DecodeRecordResult {
        key,
        val: value,
        offset: offset + (1 + 16 + key_len + value_len + 4) as u64,
        record_type: match record_type {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        },
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_encode_decode_record() {
        let data = encode_record(b"key0", b"value0", RecordType::Put);
        let result = decode_record(Cursor::new(&data), 0).unwrap();

        assert_eq!(result.record_type, RecordType::Put);
        assert_eq!(result.key, b"key0");
        assert_eq!(result.val, b"value0");
        assert_eq!(result.offset, 31);
    }

    #[test]
    fn test_encode_tombstone() {
        let data = encode_tombstone_record(b"deleted_key");
        let result = decode_record(Cursor::new(&data), 0).unwrap();

        assert_eq!(result.record_type, RecordType::Delete);
        assert_eq!(result.key, b"deleted_key");
        assert_eq!(result.val, b"");
    }
}
