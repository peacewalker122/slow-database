use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
};

use super::{
    block::BlockBuilder,
    bloom::BloomFilter,
    record::{decode_record, encode_record, encode_tombstone_record, RecordType},
    skiplist::SkipList,
};

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
    pub fn encode(&self) -> Vec<u8> {
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
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub offset: u64,
}

impl IndexEntry {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + 8 + self.key.len());
        buf.extend_from_slice(&(self.key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf
    }

    pub fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
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

/// Sparse Index Entry: maps the first key of a block to the block's offset
/// This is more efficient for high-cardinality keys (like UUIDs)
#[derive(Debug, Clone)]
pub struct SparseIndexEntry {
    /// First key in the block
    pub first_key: Vec<u8>,
    /// Offset of the block in the file
    pub block_offset: u64,
    /// Last key in the block (for range checking)
    pub last_key: Vec<u8>,
    /// Number of records in this block
    pub record_count: u32,
}

impl SparseIndexEntry {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Encode first_key
        buf.extend_from_slice(&(self.first_key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.first_key);

        // Encode block_offset
        buf.extend_from_slice(&self.block_offset.to_be_bytes());

        // Encode last_key
        buf.extend_from_slice(&(self.last_key.len() as u64).to_be_bytes());
        buf.extend_from_slice(&self.last_key);

        // Encode record_count
        buf.extend_from_slice(&self.record_count.to_be_bytes());

        buf
    }

    pub fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
        let mut len_buf = [0u8; 8];

        // Decode first_key
        reader.read_exact(&mut len_buf)?;
        let first_key_len = u64::from_be_bytes(len_buf) as usize;
        let mut first_key = vec![0u8; first_key_len];
        reader.read_exact(&mut first_key)?;

        // Decode block_offset
        reader.read_exact(&mut len_buf)?;
        let block_offset = u64::from_be_bytes(len_buf);

        // Decode last_key
        reader.read_exact(&mut len_buf)?;
        let last_key_len = u64::from_be_bytes(len_buf) as usize;
        let mut last_key = vec![0u8; last_key_len];
        reader.read_exact(&mut last_key)?;

        // Decode record_count
        let mut count_buf = [0u8; 4];
        reader.read_exact(&mut count_buf)?;
        let record_count = u32::from_be_bytes(count_buf);

        Ok(SparseIndexEntry {
            first_key,
            block_offset,
            last_key,
            record_count,
        })
    }
}

/// Helper to verify index checksum
fn verify_index_checksum(data: &[u8], expected: u32) -> Result<(), std::io::Error> {
    let calculated = crc32fast::hash(data);
    if calculated != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Index block checksum mismatch: expected 0x{:X}, got 0x{:X}",
                expected, calculated
            ),
        ));
    }
    Ok(())
}

/// Helper to verify bloom filter checksum
fn verify_bloom_checksum(data: &[u8], expected: u32) -> Result<(), std::io::Error> {
    let calculated = crc32fast::hash(data);
    if calculated != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Bloom filter checksum mismatch: expected 0x{:X}, got 0x{:X}",
                expected, calculated
            ),
        ));
    }
    Ok(())
}

/// Flush memtable to SSTable with 4KB blocks
///
/// # Arguments
/// * `memtable` - The memtable to flush
/// * `level` - The LSM-tree level (0 for memtable flushes, 1+ for compaction)
/// * `file_id` - Unique identifier for this SSTable file (typically timestamp)
///
/// # File Naming Convention
/// Files are named as: `app-L{level}-{file_id}.db`
/// Example: `app-L0-1735948800.db` for a Level 0 SSTable with timestamp ID
pub fn flush_memtable(memtable: SkipList, level: u32, file_id: u64) -> Result<(), std::io::Error> {
    let filename = format!("app-L{}-{}.db", level, file_id);

    log::info!(
        "Starting memtable flush to SSTable '{}' with 4KB blocks, entries: {}",
        filename,
        memtable.len()
    );

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&filename)?;

    let data_block_start = file.metadata()?.len();

    // Build sparse index and bloom filter as we write data blocks
    let mut sparse_index: Vec<SparseIndexEntry> = Vec::new();
    let mut blocks: Vec<Vec<u8>> = Vec::new();

    // Create Bloom filter with 1% false positive rate
    let mut bloom_filter = BloomFilter::with_rate(memtable.len(), 0.01);

    // Create first block builder
    let mut current_offset = data_block_start;
    let mut block_builder = BlockBuilder::new(current_offset);

    log::debug!("Writing records to 4KB blocks...");

    // Write all records to blocks
    for (key, val) in memtable.iter() {
        // Insert key into Bloom filter
        bloom_filter.insert(&key);

        // Encode the record
        let record = match val.0 {
            RecordType::Delete => encode_tombstone_record(&key),
            RecordType::Put => encode_record(&key, &val.1, RecordType::Put),
        };

        // Try to add record to current block
        match block_builder.add_record(record.clone(), key.clone()) {
            Ok(()) => {
                // Record added successfully
            }
            Err(_record) => {
                // Block is full, finalize it and create a new one
                if let Some((block_meta, block_data)) = block_builder.build() {
                    log::trace!(
                        "Block filled: offset={}, size={} bytes, records={}, first_key={:?}, last_key={:?}",
                        block_meta.offset,
                        block_meta.data_size,
                        block_meta.record_count,
                        String::from_utf8_lossy(&block_meta.first_key),
                        String::from_utf8_lossy(&block_meta.last_key)
                    );

                    // Add to sparse index
                    sparse_index.push(SparseIndexEntry {
                        first_key: block_meta.first_key,
                        block_offset: block_meta.offset,
                        last_key: block_meta.last_key,
                        record_count: block_meta.record_count,
                    });

                    // Store block data
                    blocks.push(block_data);
                    current_offset += block_meta.data_size as u64;
                }

                // Create new block and add the record that didn't fit
                block_builder = BlockBuilder::new(current_offset);
                block_builder
                    .add_record(record, key.clone())
                    .expect("Fresh block should have space for record");
            }
        }
    }

    // Finalize the last block if it has data
    if !block_builder.is_empty() {
        if let Some((block_meta, block_data)) = block_builder.build() {
            log::trace!(
                "Final block: offset={}, size={} bytes, records={}, first_key={:?}, last_key={:?}",
                block_meta.offset,
                block_meta.data_size,
                block_meta.record_count,
                String::from_utf8_lossy(&block_meta.first_key),
                String::from_utf8_lossy(&block_meta.last_key)
            );

            sparse_index.push(SparseIndexEntry {
                first_key: block_meta.first_key,
                block_offset: block_meta.offset,
                last_key: block_meta.last_key,
                record_count: block_meta.record_count,
            });

            blocks.push(block_data);
        }
    }

    log::info!(
        "Created {} blocks from {} entries",
        blocks.len(),
        memtable.len()
    );

    // Write all blocks to file
    for block_data in &blocks {
        file.write_all(block_data)?;
    }
    let data_block_end = file.metadata()?.len();

    // Build and write sparse index block
    let index_block_start = data_block_end;
    let mut index_blocks: Vec<u8> = Vec::new();

    // Write number of sparse index entries
    index_blocks.extend_from_slice(&(sparse_index.len() as u64).to_be_bytes());

    // Write each sparse index entry
    for entry in sparse_index.iter() {
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

    log::info!(
        "Flushed SSTable '{}': {} blocks, data=[{}-{}], sparse_index=[{}-{}], bloom=[{}-{}], index_crc=0x{:X}, bloom_crc=0x{:X}",
        filename,
        blocks.len(),
        data_block_start,
        data_block_end,
        index_block_start,
        index_block_end,
        bloom_block_start,
        bloom_block_end,
        index_checksum,
        bloom_checksum
    );

    Ok(())
}

/// Read the footer from an SSTable file
pub fn read_sstable_footer<R: Read + Seek>(mut reader: R) -> Result<SSTableFooter, std::io::Error> {
    // Seek to footer location (last FOOTER_SIZE bytes)
    reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    SSTableFooter::decode(reader)
}

/// Read the sparse index block from an SSTable file
pub fn read_sstable_sparse_index<R: Read + Seek>(
    mut reader: R,
    footer: &SSTableFooter,
) -> Result<Vec<SparseIndexEntry>, std::io::Error> {
    // Calculate index block size
    let index_size = footer.index_block_end - footer.index_block_start;

    // Seek to index block start and read entire block
    reader.seek(SeekFrom::Start(footer.index_block_start))?;
    let mut index_data = vec![0u8; index_size as usize];
    reader.read_exact(&mut index_data)?;

    // Verify index checksum
    verify_index_checksum(&index_data, footer.index_checksum)?;

    // Parse sparse index entries
    let mut cursor = std::io::Cursor::new(&index_data);

    // Read number of entries
    let mut count_buf = [0u8; 8];
    cursor.read_exact(&mut count_buf)?;
    let entry_count = u64::from_be_bytes(count_buf);

    // Read all sparse index entries
    let mut sparse_index = Vec::new();
    for _ in 0..entry_count {
        let entry = SparseIndexEntry::decode(&mut cursor)?;
        sparse_index.push(entry);
    }

    Ok(sparse_index)
}

/// Read the index block from an SSTable file (legacy function for compatibility)
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
    verify_index_checksum(&index_data, footer.index_checksum)?;

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
) -> Result<BloomFilter, std::io::Error> {
    // Calculate bloom filter block size
    let bloom_size = footer.bloom_block_end - footer.bloom_block_start;

    // Seek to bloom filter block start and read entire block
    reader.seek(SeekFrom::Start(footer.bloom_block_start))?;
    let mut bloom_data = vec![0u8; bloom_size as usize];
    reader.read_exact(&mut bloom_data)?;

    // Verify bloom filter checksum
    verify_bloom_checksum(&bloom_data, footer.bloom_checksum)?;

    // Decode bloom filter
    let cursor = std::io::Cursor::new(&bloom_data);
    BloomFilter::decode(cursor)
}

/// Search for a key in the SSTable using sparse index and linear block scan
/// This is optimized for high-cardinality keys (like UUIDs)
pub fn search_sstable_sparse<R: Read + Seek>(
    mut reader: R,
    key: &[u8],
    sparse_index: &[SparseIndexEntry],
) -> Result<Option<Vec<u8>>, std::io::Error> {
    // Find the block that might contain the key using binary search
    // We need to find the block where: first_key <= key <= last_key
    let mut target_block: Option<&SparseIndexEntry> = None;

    // Binary search for the correct block
    let mut left = 0;
    let mut right = sparse_index.len();

    while left < right {
        let mid = left + (right - left) / 2;
        let entry = &sparse_index[mid];

        if key < entry.first_key.as_slice() {
            // Key is before this block
            right = mid;
        } else if key > entry.last_key.as_slice() {
            // Key is after this block
            left = mid + 1;
        } else {
            // Key is within this block's range (first_key <= key <= last_key)
            target_block = Some(entry);
            break;
        }
    }

    // If no block contains this key range, key doesn't exist
    let block = match target_block {
        Some(b) => b,
        None => return Ok(None),
    };

    log::trace!(
        "Scanning block at offset {} for key {:?}",
        block.block_offset,
        String::from_utf8_lossy(key)
    );

    // Seek to the block and scan linearly
    reader.seek(SeekFrom::Start(block.block_offset))?;

    // Read records in this block until we find the key or reach the end
    let mut current_offset = block.block_offset;
    let mut records_scanned = 0;

    loop {
        // Try to read a record at current offset
        match decode_record(&mut reader, current_offset) {
            Ok(record) => {
                records_scanned += 1;

                // Check if this is our key
                if record.key == key {
                    log::trace!("Key found after scanning {} records", records_scanned);
                    return match record.record_type {
                        RecordType::Put => Ok(Some(record.val)),
                        RecordType::Delete => Ok(None), // Tombstone
                    };
                }

                // Move to next record
                current_offset = record.offset;

                // If we've scanned all records in this block, stop
                if records_scanned >= block.record_count {
                    break;
                }
            }
            Err(_) => {
                // Error reading record, assume we've reached the end of the block
                break;
            }
        }
    }

    log::trace!(
        "Key not found after scanning {} records in block",
        records_scanned
    );
    Ok(None)
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
    bloom: &BloomFilter,
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
    use super::*;
    use std::io::Cursor;

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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Footer checksum mismatch"));

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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Index block checksum mismatch"));
    }
}
