use std::io::Write;

use crate::storage::log::{RecordType, WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};

#[derive(Debug)]
struct WAL<'a> {
    header: Box<WALHeader>,
    records: Vec<WALRecord<'a>>,
}

impl<'a> WAL<'a> {
    pub fn new() -> Self {
        WAL {
            header: Box::new(WALHeader::new()),
            records: Vec::new(),
        }
    }

    pub fn update_header(&mut self, last_checkpoint: u64, last_checkpoint_offset: u64) {
        self.header.last_checkpoint = last_checkpoint;
        self.header.last_checkpoint_offset = last_checkpoint_offset;
    }
}

/// WAL file header structure
#[derive(Debug)]
pub struct WALHeader {
    pub magic: [u8; 8],              // Magic number: "WALMGIC\0"
    pub version: u64,                // WAL format version
    pub last_checkpoint: u64,        // LSN of last checkpoint
    pub last_checkpoint_offset: u64, // Reserved for future use
}

impl WALHeader {
    /// Create a new WAL header with default values
    pub fn new() -> Self {
        WALHeader {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            last_checkpoint: 0,
            last_checkpoint_offset: 0,
        }
    }

    /// Encode header to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(WAL_HEADER_SIZE as usize);
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint_offset.to_be_bytes());
        buf
    }

    /// Decode header from bytes
    pub fn decode(buf: &[u8]) -> Result<Self, std::io::Error> {
        if buf.len() < WAL_HEADER_SIZE as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Buffer too short for WAL header",
            ));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);

        if &magic != WAL_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid WAL magic number",
            ));
        }

        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[8..16]);
        let version = u64::from_be_bytes(b);

        b.copy_from_slice(&buf[16..24]);
        let last_checkpoint = u64::from_be_bytes(b);

        b.copy_from_slice(&buf[24..32]);
        let last_checkpoint_offset = u64::from_be_bytes(b);

        Ok(WALHeader {
            magic,
            version,
            last_checkpoint,
            last_checkpoint_offset,
        })
    }
}

impl Default for WALHeader {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct WALRecord<'a> {
    pub lsn: u64,
    pub record_type: RecordType,
    pub key: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> WALRecord<'a> {
    pub fn new(key: &'a [u8], value: &'a [u8], record_type: RecordType, lsn: u64) -> Self {
        WALRecord {
            lsn,
            key,
            value,
            record_type,
        }
    }

    pub fn encode<T: Write>(&self, writer: &mut T) -> Result<(), std::io::Error> {
        let checksum = crc32fast::hash(&self.value);

        writer.write_all(&(self.record_type as u8).to_be_bytes())?;
        writer.write_all(&self.lsn.to_be_bytes())?;
        writer.write_all(&(self.key.len() as u64).to_be_bytes())?;
        writer.write_all(&(self.value.len() as u64).to_be_bytes())?;
        writer.write_all(&self.key)?;
        writer.write_all(&self.value)?;
        writer.write_all(&checksum.to_be_bytes())?;

        Ok(())
    }

    pub fn decode(buf: &'a [u8]) -> Result<(Self, usize), std::io::Error> {
        let mut offset = 0;

        if buf.len() < 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF",
            ));
        }

        let record_type = match buf[offset] {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };
        offset += 1;

        if buf.len() < offset + 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF",
            ));
        }
        let mut len_buf = [0u8; 8];
        len_buf.copy_from_slice(&buf[offset..offset + 8]);
        let lsn = u64::from_be_bytes(len_buf);
        offset += 8;

        if buf.len() < offset + 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF",
            ));
        }
        len_buf.copy_from_slice(&buf[offset..offset + 8]);
        let key_len = u64::from_be_bytes(len_buf) as usize;
        offset += 8;

        if buf.len() < offset + 8 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF",
            ));
        }
        len_buf.copy_from_slice(&buf[offset..offset + 8]);
        let value_len = u64::from_be_bytes(len_buf) as usize;
        offset += 8;

        if buf.len() < offset + key_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF",
            ));
        }
        let key_bytes = &buf[offset..offset + key_len];
        offset += key_len;

        if buf.len() < offset + value_len {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF",
            ));
        }
        let value_bytes = &buf[offset..offset + value_len];
        offset += value_len;

        if buf.len() < offset + 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "EOF",
            ));
        }
        let mut crc_buf = [0u8; 4];
        crc_buf.copy_from_slice(&buf[offset..offset + 4]);
        let checksum = u32::from_be_bytes(crc_buf);
        offset += 4;

        if crc32fast::hash(&value_bytes) != checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        Ok((
            WALRecord {
                lsn,
                key: key_bytes,
                value: value_bytes,
                record_type,
            },
            offset,
        ))
    }
}

pub fn recover(data: &[u8]) -> Result<Vec<WALRecord>, std::io::Error> {
    let mut records = Vec::new();
    let header = WALHeader::decode(data)?;

    if header.magic != *WAL_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid WAL magic number",
        ));
    }

    let mut start_offset = header.last_checkpoint_offset as usize;
    if start_offset == 0 {
        start_offset = WAL_HEADER_SIZE as usize;
    }

    let mut current_offset = start_offset;
    while current_offset < data.len() {
        match WALRecord::decode(&data[current_offset..]) {
            Ok((record, consumed)) => {
                records.push(record);
                current_offset += consumed;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_wal_header_encode_decode() {
        let header = WALHeader::new();
        let encoded = header.encode();

        let decoded = WALHeader::decode(&encoded).unwrap();
        assert_eq!(header.magic, decoded.magic);
        assert_eq!(header.version, decoded.version);
        assert_eq!(header.last_checkpoint, decoded.last_checkpoint);
        assert_eq!(
            header.last_checkpoint_offset,
            decoded.last_checkpoint_offset
        );
    }

    #[test]
    fn test_wal_record_encode_decode() {
        let key = b"test_key";
        let value = b"test_value";
        let record_type = RecordType::Put;
        let lsn = 123;

        let record = WALRecord::new(key, value, record_type, lsn);

        let mut buf = Vec::new();
        record.encode(&mut buf).unwrap();

        let (decoded, _) = WALRecord::decode(&buf).unwrap();

        assert_eq!(decoded.key, key);
        assert_eq!(decoded.value, value);
        assert_eq!(decoded.record_type, record_type);
        assert_eq!(decoded.lsn, lsn);
    }

    #[test]
    fn test_recover() {
        let mut buf = Vec::new();

        // Write header
        let header = WALHeader::new();
        buf.extend_from_slice(&header.encode());

        // Write records
        let record1 = WALRecord::new(b"key1", b"val1", RecordType::Put, 1);
        record1.encode(&mut buf).unwrap();

        let record2 = WALRecord::new(b"key2", b"val2", RecordType::Delete, 2);
        record2.encode(&mut buf).unwrap();

        let records = recover(&buf).unwrap();

        assert_eq!(records.len(), 2);

        assert_eq!(records[0].key, b"key1");
        assert_eq!(records[0].value, b"val1");
        assert_eq!(records[0].record_type, RecordType::Put);
        assert_eq!(records[0].lsn, 1);

        assert_eq!(records[1].key, b"key2");
        assert_eq!(records[1].value, b"val2");
        assert_eq!(records[1].record_type, RecordType::Delete);
        assert_eq!(records[1].lsn, 2);
    }
}
