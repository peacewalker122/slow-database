use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
};

use super::{bloom::BloomFilter, skiplist::SkipList};

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

pub fn store_log(
    filename: &str,
    key: &[u8],
    value: &[u8],
    is_tombstone: RecordType,
) -> Result<u64, std::io::Error> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)?;

    let offset = file.metadata()?.len();
    println!("offset from store_log: {}", offset);

    let record = match is_tombstone {
        RecordType::Delete => encode_tombstone_record(key),
        RecordType::Put => encode_record(key, value, RecordType::Put),
    };

    // NOTE: if the below section is failed, is it mean the data durability can't be guaranteed
    file.write_all(&record)?;
    file.sync_data().expect("Failed to sync data");

    Ok(offset)
}

pub fn flush_memtable(memtable: &mut SkipList) -> Result<(), std::io::Error> {
    // if memtable passed len threshold, flush it into an SSTable file
    // current threshold is 4KB (25 records of avg 160 bytes)
    if memtable.len() >= 25 {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("app.db")?;

        let data_block_start = file.metadata()?.len();

        // Build index and bloom filter as we write data blocks
        let mut index: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
        let mut data_blocks: Vec<u8> = Vec::new();

        // Create Bloom filter with 1% false positive rate
        let mut bloom_filter = BloomFilter::with_rate(memtable.len(), 0.01);

        // Write all records to data blocks and track offsets
        for (key, val) in memtable.iter() {
            let record_offset = data_block_start + data_blocks.len() as u64;

            // Store key -> offset mapping in index
            index.insert(key.clone(), record_offset);

            // Insert key into Bloom filter
            bloom_filter.insert(&key);

            // Encode the record
            let mut record = match val.0 {
                RecordType::Delete => encode_tombstone_record(&key),
                RecordType::Put => encode_record(&key, &val.1, RecordType::Put),
            };

            data_blocks.append(&mut record);
        }

        // Write data blocks to file
        file.write_all(&data_blocks)?;
        let data_block_end = file.metadata()?.len();

        // Build and write index block
        let index_block_start = data_block_end;
        let mut index_blocks: Vec<u8> = Vec::new();

        // Write number of index entries first
        index_blocks.extend_from_slice(&(index.len() as u64).to_be_bytes());

        // Write each index entry
        for (key, offset) in index.iter() {
            let entry = IndexEntry {
                key: key.clone(),
                offset: *offset,
            };
            index_blocks.append(&mut entry.encode());
        }

        file.write_all(&index_blocks)?;
        let index_block_end = file.metadata()?.len();

        // Calculate index block checksum
        let index_checksum = crc32fast::hash(&index_blocks);

        // Write Bloom filter block
        let bloom_block_start = index_block_end;
        let bloom_data = bloom_filter.encode();
        file.write_all(&bloom_data)?;
        let bloom_block_end = file.metadata()?.len();

        // Calculate bloom filter checksum
        let bloom_checksum = crc32fast::hash(&bloom_data);

        // Write footer
        let footer = SSTableFooter {
            data_block_start,
            data_block_end,
            index_block_start,
            index_block_end,
            index_checksum,
            bloom_block_start,
            bloom_block_end,
            bloom_checksum,
        };

        file.write_all(&footer.encode())?;
        file.sync_data()?;

        // Clear memtable after successful flush
        memtable.clear();

        println!(
            "Flushed SSTable: data=[{}-{}], index=[{}-{}], bloom=[{}-{}], index_crc=0x{:X}, bloom_crc=0x{:X}",
            data_block_start,
            data_block_end,
            index_block_start,
            index_block_end,
            bloom_block_start,
            bloom_block_end,
            index_checksum,
            bloom_checksum
        );
    }

    Ok(())
}

fn encode_record(key: &[u8], value: &[u8], record_type: RecordType) -> Vec<u8> {
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

fn encode_tombstone_record(key: &[u8]) -> Vec<u8> {
    encode_record(key, b"", RecordType::Delete)
}

pub fn decode_record<R>(record: R, offset: u64) -> Result<Box<DecodeRecordResult>, std::io::Error>
where
    R: Read + Seek,
{
    let mut reader = record;
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

    let result = Box::new(DecodeRecordResult {
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
    });

    return Ok(result);
}

/// SSTable Footer Structure:
/// - data_block_start: u64 (8 bytes) - where data blocks start
/// - data_block_end: u64 (8 bytes) - where data blocks end
/// - index_block_start: u64 (8 bytes) - where index block starts
/// - index_block_end: u64 (8 bytes) - where index block ends
/// - index_checksum: u32 (4 bytes) - CRC32 of index block
/// - bloom_block_start: u64 (8 bytes) - where bloom filter starts
/// - bloom_block_end: u64 (8 bytes) - where bloom filter ends
/// - bloom_checksum: u32 (4 bytes) - CRC32 of bloom filter
/// - magic_number: u32 (4 bytes) - validation marker (0xDB055555)
/// - footer_checksum: u32 (4 bytes) - CRC32 of footer data (excluding this field)
/// Total: 64 bytes
const FOOTER_SIZE: u64 = 64;
const MAGIC_NUMBER: u32 = 0xDB055555;

#[derive(Debug, Clone)]
pub struct SSTableFooter {
    pub data_block_start: u64,
    pub data_block_end: u64,
    pub index_block_start: u64,
    pub index_block_end: u64,
    pub index_checksum: u32,
    pub bloom_block_start: u64,
    pub bloom_block_end: u64,
    pub bloom_checksum: u32,
}

impl SSTableFooter {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FOOTER_SIZE as usize);
        buf.extend_from_slice(&self.data_block_start.to_be_bytes());
        buf.extend_from_slice(&self.data_block_end.to_be_bytes());
        buf.extend_from_slice(&self.index_block_start.to_be_bytes());
        buf.extend_from_slice(&self.index_block_end.to_be_bytes());
        buf.extend_from_slice(&self.index_checksum.to_be_bytes());
        buf.extend_from_slice(&self.bloom_block_start.to_be_bytes());
        buf.extend_from_slice(&self.bloom_block_end.to_be_bytes());
        buf.extend_from_slice(&self.bloom_checksum.to_be_bytes());
        buf.extend_from_slice(&MAGIC_NUMBER.to_be_bytes());

        // Calculate checksum of all footer data
        let footer_checksum = crc32fast::hash(&buf);
        buf.extend_from_slice(&footer_checksum.to_be_bytes());

        buf
    }

    pub fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
        let mut buf = [0u8; 8];
        let mut footer_data = Vec::with_capacity(60); // All data except final checksum

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let data_block_start = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let data_block_end = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let index_block_start = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let index_block_end = u64::from_be_bytes(buf);

        let mut checksum_buf = [0u8; 4];
        reader.read_exact(&mut checksum_buf)?;
        footer_data.extend_from_slice(&checksum_buf);
        let index_checksum = u32::from_be_bytes(checksum_buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let bloom_block_start = u64::from_be_bytes(buf);

        reader.read_exact(&mut buf)?;
        footer_data.extend_from_slice(&buf);
        let bloom_block_end = u64::from_be_bytes(buf);

        reader.read_exact(&mut checksum_buf)?;
        footer_data.extend_from_slice(&checksum_buf);
        let bloom_checksum = u32::from_be_bytes(checksum_buf);

        let mut magic_buf = [0u8; 4];
        reader.read_exact(&mut magic_buf)?;
        footer_data.extend_from_slice(&magic_buf);
        let magic = u32::from_be_bytes(magic_buf);

        if magic != MAGIC_NUMBER {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Invalid magic number: expected 0x{:X}, got 0x{:X}",
                    MAGIC_NUMBER, magic
                ),
            ));
        }

        // Verify footer checksum
        reader.read_exact(&mut checksum_buf)?;
        let stored_checksum = u32::from_be_bytes(checksum_buf);
        let calculated_checksum = crc32fast::hash(&footer_data);

        if stored_checksum != calculated_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Footer checksum mismatch: expected 0x{:X}, got 0x{:X}",
                    calculated_checksum, stored_checksum
                ),
            ));
        }

        Ok(SSTableFooter {
            data_block_start,
            data_block_end,
            index_block_start,
            index_block_end,
            index_checksum,
            bloom_block_start,
            bloom_block_end,
            bloom_checksum,
        })
    }
}

/// Index Entry: maps a key to its offset in the data block
#[derive(Debug, Clone)]
struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
}

impl IndexEntry {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 8 + self.key.len());
        buf.extend_from_slice(&(self.key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf
    }

    fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
        let mut len_buf = [0u8; 8];
        reader.read_exact(&mut len_buf)?;
        let key_len = u64::from_be_bytes(len_buf) as usize;

        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        let mut offset_buf = [0u8; 8];
        reader.read_exact(&mut offset_buf)?;
        let offset = u64::from_be_bytes(offset_buf);

        Ok(IndexEntry { key, offset })
    }
}

/// Read the footer from an SSTable file
pub fn read_sstable_footer<R: Read + Seek>(mut reader: R) -> Result<SSTableFooter, std::io::Error> {
    // Seek to footer location (last FOOTER_SIZE bytes)
    reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    SSTableFooter::decode(reader)
}

/// Read the index block from an SSTable file
pub fn read_sstable_index<R: Read + Seek>(
    mut reader: R,
    footer: &SSTableFooter,
) -> Result<BTreeMap<Vec<u8>, u64>, std::io::Error> {
    // Calculate index block size
    let index_size = footer.index_block_end - footer.index_block_start;

    // Seek to index block start and read entire block
    reader.seek(SeekFrom::Start(footer.index_block_start))?;
    let mut index_data = vec![0u8; index_size as usize];
    reader.read_exact(&mut index_data)?;

    // Verify index checksum
    let calculated_checksum = crc32fast::hash(&index_data);
    if calculated_checksum != footer.index_checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Index block checksum mismatch: expected 0x{:X}, got 0x{:X}",
                footer.index_checksum, calculated_checksum
            ),
        ));
    }

    // Parse index entries
    let mut cursor = std::io::Cursor::new(&index_data);

    // Read number of entries
    let mut count_buf = [0u8; 8];
    cursor.read_exact(&mut count_buf)?;
    let entry_count = u64::from_be_bytes(count_buf);

    // Read all index entries
    let mut index = BTreeMap::new();
    for _ in 0..entry_count {
        let entry = IndexEntry::decode(&mut cursor)?;
        index.insert(entry.key, entry.offset);
    }

    Ok(index)
}

/// Read the bloom filter from an SSTable file
pub fn read_sstable_bloom<R: Read + Seek>(
    mut reader: R,
    footer: &SSTableFooter,
) -> Result<super::bloom::BloomFilter, std::io::Error> {
    // Calculate bloom filter block size
    let bloom_size = footer.bloom_block_end - footer.bloom_block_start;

    // Seek to bloom filter block start and read entire block
    reader.seek(SeekFrom::Start(footer.bloom_block_start))?;
    let mut bloom_data = vec![0u8; bloom_size as usize];
    reader.read_exact(&mut bloom_data)?;

    // Verify bloom filter checksum
    let calculated_checksum = crc32fast::hash(&bloom_data);
    if calculated_checksum != footer.bloom_checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Bloom filter checksum mismatch: expected 0x{:X}, got 0x{:X}",
                footer.bloom_checksum, calculated_checksum
            ),
        ));
    }

    // Decode bloom filter
    let cursor = std::io::Cursor::new(&bloom_data);
    super::bloom::BloomFilter::decode(cursor)
}

/// Search for a key in the SSTable using the index
pub fn search_sstable<R: Read + Seek>(
    mut reader: R,
    key: &[u8],
    index: &BTreeMap<Vec<u8>, u64>,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    // Look up key in index
    if let Some(&offset) = index.get(key) {
        // Read the record at the offset
        let record = decode_record(&mut reader, offset)?;

        // Verify key matches
        if record.key == key {
            match record.record_type {
                RecordType::Put => Ok(Some(record.val)),
                RecordType::Delete => Ok(None), // Tombstone
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Search for a key in the SSTable using bloom filter and index
/// This is more efficient as it checks the bloom filter first
pub fn search_sstable_with_bloom<R: Read + Seek>(
    mut reader: R,
    key: &[u8],
    bloom: &super::bloom::BloomFilter,
    index: &BTreeMap<Vec<u8>, u64>,
) -> Result<Option<Vec<u8>>, std::io::Error> {
    // Check bloom filter first - if it returns false, key definitely doesn't exist
    if !bloom.contains(key) {
        return Ok(None);
    }

    // Bloom filter says key might exist, check the index
    search_sstable(&mut reader, key, index)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_decode_log() {
        let data: &[u8] = &encode_record(b"key0", b"value0", RecordType::Put);
        let result = decode_record(Cursor::new(data), 0).unwrap();

        assert_eq!(result.record_type, RecordType::Put);
        assert_eq!(result.key, b"key0");
        assert_eq!(result.val, b"value0");
        assert_eq!(result.offset, 31);
    }

    #[test]
    fn test_sstable_footer() {
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 1000,
            index_block_end: 1500,
            index_checksum: 0x12345678,
            bloom_block_start: 1500,
            bloom_block_end: 1600,
            bloom_checksum: 0x87654321,
        };

        let encoded = footer.encode();
        assert_eq!(encoded.len(), FOOTER_SIZE as usize);

        let decoded = SSTableFooter::decode(Cursor::new(&encoded)).unwrap();
        assert_eq!(decoded.data_block_start, footer.data_block_start);
        assert_eq!(decoded.data_block_end, footer.data_block_end);
        assert_eq!(decoded.index_block_start, footer.index_block_start);
        assert_eq!(decoded.index_block_end, footer.index_block_end);
        assert_eq!(decoded.index_checksum, footer.index_checksum);
        assert_eq!(decoded.bloom_block_start, footer.bloom_block_start);
        assert_eq!(decoded.bloom_block_end, footer.bloom_block_end);
        assert_eq!(decoded.bloom_checksum, footer.bloom_checksum);
    }

    #[test]
    fn test_index_entry() {
        let entry = IndexEntry {
            key: b"test_key".to_vec(),
            offset: 12345,
        };

        let encoded = entry.encode();
        let decoded = IndexEntry::decode(Cursor::new(&encoded)).unwrap();

        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.offset, entry.offset);
    }

    #[test]
    fn test_sstable_index() {
        let mut index = BTreeMap::new();
        index.insert(b"key1".to_vec(), 100u64);
        index.insert(b"key2".to_vec(), 200u64);
        index.insert(b"key3".to_vec(), 300u64);

        // Encode index block
        let mut index_block = Vec::new();
        index_block.extend_from_slice(&(index.len() as u64).to_be_bytes());

        for (key, offset) in index.iter() {
            let entry = IndexEntry {
                key: key.clone(),
                offset: *offset,
            };
            index_block.append(&mut entry.encode());
        }

        // Calculate checksum
        let index_checksum = crc32fast::hash(&index_block);

        // Create mock footer
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 0, // Index starts at beginning for this test
            index_block_end: index_block.len() as u64,
            index_checksum,
            bloom_block_start: 0,
            bloom_block_end: 0,
            bloom_checksum: 0,
        };

        // Decode index
        let decoded_index = read_sstable_index(Cursor::new(&index_block), &footer).unwrap();

        assert_eq!(decoded_index.len(), 3);
        assert_eq!(decoded_index.get(b"key1".as_ref()), Some(&100));
        assert_eq!(decoded_index.get(b"key2".as_ref()), Some(&200));
        assert_eq!(decoded_index.get(b"key3".as_ref()), Some(&300));
    }

    #[test]
    fn test_sstable_write_and_read() {
        use crate::storage::bloom::BloomFilter;
        use std::fs::File;
        use std::io::Write;

        // Create a temporary SSTable
        let test_file = "test_sstable.db";
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(test_file)
            .unwrap();

        let data_block_start = 0;

        // Build data, index, and bloom filter
        let mut data_blocks = Vec::new();
        let mut index = BTreeMap::new();
        let mut bloom_filter = BloomFilter::with_rate(3, 0.01);

        // Add some test records
        let test_data = vec![
            (b"apple".to_vec(), b"red fruit".to_vec()),
            (b"banana".to_vec(), b"yellow fruit".to_vec()),
            (b"cherry".to_vec(), b"red small fruit".to_vec()),
        ];

        for (key, value) in test_data.iter() {
            let offset = data_block_start + data_blocks.len() as u64;
            index.insert(key.clone(), offset);
            bloom_filter.insert(key);

            let mut record = encode_record(key, value, RecordType::Put);
            data_blocks.append(&mut record);
        }

        file.write_all(&data_blocks).unwrap();
        let data_block_end = file.metadata().unwrap().len();

        // Write index
        let index_block_start = data_block_end;
        let mut index_blocks = Vec::new();
        index_blocks.extend_from_slice(&(index.len() as u64).to_be_bytes());

        for (key, offset) in index.iter() {
            let entry = IndexEntry {
                key: key.clone(),
                offset: *offset,
            };
            index_blocks.append(&mut entry.encode());
        }

        file.write_all(&index_blocks).unwrap();
        let index_block_end = file.metadata().unwrap().len();

        // Calculate index checksum
        let index_checksum = crc32fast::hash(&index_blocks);

        // Write bloom filter
        let bloom_block_start = index_block_end;
        let bloom_data = bloom_filter.encode();
        file.write_all(&bloom_data).unwrap();
        let bloom_block_end = file.metadata().unwrap().len();

        // Calculate bloom checksum
        let bloom_checksum = crc32fast::hash(&bloom_data);

        // Write footer
        let footer = SSTableFooter {
            data_block_start,
            data_block_end,
            index_block_start,
            index_block_end,
            index_checksum,
            bloom_block_start,
            bloom_block_end,
            bloom_checksum,
        };
        file.write_all(&footer.encode()).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Now read back using search_sstable
        let file = File::open(test_file).unwrap();
        let footer = read_sstable_footer(&file).unwrap();
        let bloom = read_sstable_bloom(&file, &footer).unwrap();
        let index = read_sstable_index(&file, &footer).unwrap();

        let result = search_sstable_with_bloom(&file, b"banana", &bloom, &index).unwrap();
        assert_eq!(result, Some(b"yellow fruit".to_vec()));

        let result = search_sstable_with_bloom(&file, b"apple", &bloom, &index).unwrap();
        assert_eq!(result, Some(b"red fruit".to_vec()));

        let result = search_sstable_with_bloom(&file, b"nonexistent", &bloom, &index).unwrap();
        assert_eq!(result, None);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_checksum_validation() {
        // Test footer checksum validation
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 1000,
            index_block_end: 1500,
            index_checksum: 0x12345678,
            bloom_block_start: 1500,
            bloom_block_end: 1600,
            bloom_checksum: 0x87654321,
        };

        let mut encoded = footer.encode();

        // Corrupt the footer checksum (last 4 bytes)
        let len = encoded.len();
        encoded[len - 1] ^= 0xFF; // Flip bits in last byte

        let result = SSTableFooter::decode(Cursor::new(&encoded));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Footer checksum mismatch")
        );

        // Test index checksum validation
        let mut index = BTreeMap::new();
        index.insert(b"key1".to_vec(), 100u64);

        let mut index_block = Vec::new();
        index_block.extend_from_slice(&(index.len() as u64).to_be_bytes());

        let entry = IndexEntry {
            key: b"key1".to_vec(),
            offset: 100,
        };
        index_block.append(&mut entry.encode());

        // Correct checksum
        let correct_checksum = crc32fast::hash(&index_block);

        // Wrong checksum
        let wrong_checksum = correct_checksum ^ 0xFFFF;

        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: 1000,
            index_block_start: 0,
            index_block_end: index_block.len() as u64,
            index_checksum: wrong_checksum,
            bloom_block_start: 0,
            bloom_block_end: 0,
            bloom_checksum: 0,
        };

        let result = read_sstable_index(Cursor::new(&index_block), &footer);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Index block checksum mismatch")
        );
    }
}
