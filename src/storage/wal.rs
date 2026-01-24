use std::io::{Read, Seek, SeekFrom};

use crate::storage::log::{RecordType, WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};

#[derive(Debug)]
struct WAL {
    header: Box<WALHeader>,
    records: Vec<WALRecord>,
}

impl WAL {
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
    pub fn decode<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;

        if &magic != WAL_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid WAL magic number",
            ));
        }

        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        let version = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        let last_checkpoint = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        let last_checkpoint_offset = u64::from_be_bytes(buf);

        Ok(WALHeader {
            magic,
            version,
            last_checkpoint,
            last_checkpoint_offset,
        })
    }
}

#[derive(Debug)]
pub struct WALRecord {
    pub lsn: u64,
    pub record_type: RecordType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WALRecord {
    pub fn new(key: Vec<u8>, value: Vec<u8>, record_type: RecordType, lsn: u64) -> Self {
        WALRecord {
            lsn,
            key,
            value,
            record_type,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 8 + 8 + 8 + self.key.len() + self.value.len() + 4);

        let checksum = crc32fast::hash(&self.value);

        buf.extend_from_slice(&(self.record_type as u8).to_be_bytes());
        buf.extend_from_slice(&self.lsn.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&(self.value.len() as u64).to_be_bytes());
        buf.extend_from_slice(self.key.as_slice());
        buf.extend_from_slice(self.value.as_slice());
        buf.extend_from_slice(&checksum.to_be_bytes());

        buf
    }

    pub fn decode<F: Read + AsRef<[u8]>>(record_bytes: F) -> Result<Self, std::io::Error> {
        let mut cursor = std::io::Cursor::new(record_bytes);

        let mut record_type_buf = [0u8; 1];
        cursor.read_exact(&mut record_type_buf)?;
        let record_type = match record_type_buf[0] {
            0 => RecordType::Put,
            1 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        let mut len_buf = [0u8; 8];
        cursor.read_exact(&mut len_buf)?; // Skip LSN
        let lsn = u64::from_be_bytes(len_buf);

        cursor.read_exact(&mut len_buf)?;
        let key_len = u64::from_be_bytes(len_buf) as usize;

        cursor.read_exact(&mut len_buf)?;
        let value_len = u64::from_be_bytes(len_buf) as usize;

        let mut key_bytes = vec![0u8; key_len];
        cursor.read_exact(&mut key_bytes)?;

        let mut value_bytes = vec![0u8; value_len];
        cursor.read_exact(&mut value_bytes)?;

        let mut crc_buf = [0u8; 4];
        cursor.read_exact(&mut crc_buf)?;
        let checksum = u32::from_be_bytes(crc_buf);
        if crc32fast::hash(&value_bytes) != checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        Ok(WALRecord {
            lsn,
            key: key_bytes,
            value: value_bytes,
            record_type,
        })
    }
}

pub fn recover<F: Read + Seek>(f: F) -> Result<Vec<WALRecord>, std::io::Error> {
    let mut reader = std::io::BufReader::new(f);
    let mut records = Vec::new();
    let header = WALHeader::decode(&mut reader)?;

    if header.magic != *WAL_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid WAL magic number",
        ));
    }

    // find the last wal record, compare with header.last_checkpoint, if header.last_checkpoint is larger, truncate the wal file to that point
    // but for now, we just read all records
    // TODO: implement truncation based on last_checkpoint
    //
    // start read from the last_checkpoint_offset

    reader.seek(SeekFrom::Start(header.last_checkpoint_offset))?;

    loop {
        let mut record_type_buf = [0u8; 1];
        match reader.read_exact(&mut record_type_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }

        let mut lsn_buf = [0u8; 8];
        reader.read_exact(&mut lsn_buf)?;

        let mut len_buf = [0u8; 8];
        reader.read_exact(&mut len_buf)?;
        let key_len = u64::from_be_bytes(len_buf) as usize;

        reader.read_exact(&mut len_buf)?;
        let value_len = u64::from_be_bytes(len_buf) as usize;

        let mut key_bytes = vec![0u8; key_len];
        reader.read_exact(&mut key_bytes)?;

        let mut value_bytes = vec![0u8; value_len];
        reader.read_exact(&mut value_bytes)?;

        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf)?;
        let checksum = u32::from_be_bytes(crc_buf);
        if crc32fast::hash(&value_bytes) != checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        let record_type = match record_type_buf[0] {
            0 => RecordType::Put,
            1 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        records.push(WALRecord {
            lsn: u64::from_be_bytes(lsn_buf),
            key: key_bytes,
            value: value_bytes,
            record_type,
        });
    }

    Ok(records)
}
