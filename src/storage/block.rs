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
    pub fn add_record(&mut self, record: Vec<u8>, key: Vec<u8>) -> Result<(), Vec<u8>> {
        // Check if adding this record would exceed block size
        if self.data.len() + record.len() > SSTABLE_BLOCK_SIZE {
            return Err(record);
        }

        // Track first key
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }

        // Update last key
        self.last_key = Some(key);

        // Add record to block
        self.data.extend_from_slice(&record);
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

        Some((block, self.data))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{encode_record, RecordType};

    #[test]
    fn test_block_builder() {
        // Test that BlockBuilder properly manages 4KB blocks
        let mut builder = BlockBuilder::new(0);

        assert!(builder.is_empty());
        assert_eq!(builder.size(), 0);

        // Add a small record
        let record = encode_record(b"key1", b"value1", RecordType::Put);
        let key = b"key1".to_vec();

        let result = builder.add_record(record.clone(), key.clone());
        assert!(result.is_ok());
        assert!(!builder.is_empty());
        assert_eq!(builder.size(), record.len());

        // Try to add a record that would exceed block size
        let large_value = vec![0u8; SSTABLE_BLOCK_SIZE];
        let large_record = encode_record(b"key2", &large_value, RecordType::Put);

        let result = builder.add_record(large_record.clone(), b"key2".to_vec());
        assert!(result.is_err()); // Should fail - block full

        // Build the block
        let result = builder.build();
        assert!(result.is_some());

        let (block, data) = result.unwrap();
        assert_eq!(block.first_key, b"key1");
        assert_eq!(block.last_key, b"key1");
        assert_eq!(block.record_count, 1);
        assert_eq!(block.data_size as usize, data.len());
    }
}
