use std::io::{Cursor, Seek, SeekFrom};

use crate::{error::DBError, storage::log::Record};

use super::constant::SSTABLE_BLOCK_SIZE;

/// A fixed-size block (4KB) containing sorted key-value records
/// Each block has a header with metadata and contains multiple records
#[derive(Debug, Clone)]
pub struct Block {
    /// Offset of this block in the SSTable file
    pub offset: u64,
    /// First key in this block (for sparse index)
    pub first_key: Vec<u8>,
    /// Last key in this block (for range checks)
    pub last_key: Vec<u8>,
    /// Number of records in this block
    pub record_count: u32,
    /// Actual size of data in this block (may be less than SSTABLE_BLOCK_SIZE)
    pub data_size: u32,
}

/// Builder for creating fixed-size blocks
/// Tracks size and manages the 4KB limit
pub struct BlockBuilder {
    /// Current block data
    data: Vec<u8>,
    /// First key in current block (empty if no records yet)
    first_key: Option<Vec<u8>>,
    /// Last key added to current block
    last_key: Option<Vec<u8>>,
    /// Number of records in current block
    record_count: u32,
    /// Starting offset for current block
    block_offset: u64,
}

impl BlockBuilder {
    pub fn new(block_offset: u64) -> Self {
        BlockBuilder {
            data: Vec::new(),
            first_key: None,
            last_key: None,
            record_count: 0,
            block_offset,
        }
    }

    /// Try to add a record to the current block
    /// Returns Ok(()) if added successfully
    /// Returns Err(record_bytes) if block is full and record couldn't be added
    pub fn add_record(&mut self, record: &Record) -> Result<(), Vec<u8>> {
        // Check if adding this record would exceed block size
        if self.data.len() + record.value.len() > SSTABLE_BLOCK_SIZE {
            return Err("Record too large to fit in block".as_bytes().to_vec());
        }

        // Track first key
        if self.first_key.is_none() {
            self.first_key = Some(record.key.to_vec());
        }

        // Update last key
        self.last_key = Some(record.key.to_vec());

        // Add record to block
        self.data.extend_from_slice(&record.encode());
        self.record_count += 1;

        Ok(())
    }

    /// Finalize the current block and return its metadata and data
    pub fn build(self) -> Option<(Block, Vec<u8>)> {
        if self.data.is_empty() {
            return None;
        }

        let block = Block {
            offset: self.block_offset,
            first_key: self.first_key.unwrap(),
            last_key: self.last_key.unwrap(),
            record_count: self.record_count,
            data_size: self.data.len() as u32,
        };

        // prepend the block header to the data
        let mut block_data = Vec::new();
        block_data.extend_from_slice(&block.encode());
        block_data.extend_from_slice(&self.data);

        Some((block, block_data))
    }

    /// Check if block is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get current size of block
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl Block {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Encode first key length and first key
        let first_key_len = self.first_key.len() as u32;
        buf.extend_from_slice(&first_key_len.to_be_bytes());
        buf.extend_from_slice(&self.first_key);

        // Encode last key length and last key
        let last_key_len = self.last_key.len() as u32;
        buf.extend_from_slice(&last_key_len.to_be_bytes());
        buf.extend_from_slice(&self.last_key);

        // Encode record count
        buf.extend_from_slice(&self.record_count.to_be_bytes());

        // Encode data size
        buf.extend_from_slice(&self.data_size.to_be_bytes());

        buf
    }

    pub fn decode(data: &mut Cursor<&[u8]>, offset: u64) -> Result<Self, DBError> {
        // 1. Move the cursor (purely for state consistency, though we use slice offsets)
        data.seek(SeekFrom::Start(offset))?;
        let slice = data.get_ref();
        let mut pos = offset as usize;

        // 2. Decode First Key
        let first_key_len = u32::from_be_bytes(slice[pos..pos + 4].try_into()?) as usize;
        pos += 4;
        let first_key = &slice[pos..pos + first_key_len];
        pos += first_key_len;

        // 3. Decode Last Key
        let last_key_len = u32::from_be_bytes(slice[pos..pos + 4].try_into()?) as usize;
        pos += 4;
        let last_key = &slice[pos..pos + last_key_len];
        pos += last_key_len;

        // 4. Decode record_count and data_size
        // Both are u32 as per the struct definition
        let record_count = u32::from_be_bytes(slice[pos..pos + 4].try_into()?);
        pos += 4;

        let data_size = u32::from_be_bytes(slice[pos..pos + 4].try_into()?);
        pos += 4;

        // Update cursor position to reflect the bytes we "consumed"
        data.set_position(pos as u64);

        Ok(Block {
            offset,
            first_key: first_key.to_vec(),
            last_key: last_key.to_vec(),
            record_count,
            data_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{Record, RecordType};

    #[test]
    fn test_block_builder() {
        // Test that BlockBuilder properly manages 4KB blocks
        let mut builder = BlockBuilder::new(0);

        assert!(builder.is_empty());
        assert_eq!(builder.size(), 0);

        // Add a small record
        let key1 = b"key1";
        let record = Record::new(key1, b"value1", RecordType::Put);
        let record_bytes = record.encode();

        let result = builder.add_record(&record);
        assert!(result.is_ok());
        assert!(!builder.is_empty());
        assert_eq!(builder.size(), record_bytes.len());

        // Try to add a record that would exceed block size
        let key2 = b"key2";
        let large_value = vec![0u8; SSTABLE_BLOCK_SIZE];
        let large_record = Record::new(key2, &large_value, RecordType::Put);
        let large_record_bytes = large_record.encode();

        let result = builder.add_record(&large_record);
        assert!(result.is_err()); // Should fail - block full

        // Build the block
        let result = builder.build();
        assert!(result.is_some());

        let (parent_block, data) = result.unwrap();

        // try to decode the block_header
        let mut cursor = Cursor::new(data.as_slice());
        let block = Block::decode(&mut cursor, 0).unwrap();

        assert_eq!(block.record_count, 1);
        assert_eq!(block.first_key, parent_block.first_key);
        assert_eq!(block.last_key, parent_block.last_key);

        // decode the record inside the block
        let pos = cursor.position();
        let decoded_record = Record::decode(&mut cursor, pos).unwrap();

        assert_eq!(decoded_record.key, b"key1");
        assert_eq!(decoded_record.value, b"value1");
        assert_eq!(decoded_record.record_type, RecordType::Put);
    }
}
