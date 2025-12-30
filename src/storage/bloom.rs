use bloom::BloomFilter as BF;
use std::io::Read;

/// Wrapper around the bloom crate's BloomFilter
/// for efficient membership testing in SSTable blocks
///
/// Note: This implementation stores the bloom filter parameters for reconstruction
/// but doesn't serialize the actual filter state. For production use, you may want
/// to implement proper serialization or use a different bloom filter library.
pub struct BloomFilter {
    filter: BF,
    bitmap_size: usize,
    num_hashes: u32,
    // Store inserted keys for serialization/deserialization
    keys: Vec<Vec<u8>>,
}

impl BloomFilter {
    /// Create a new Bloom filter
    ///
    /// # Parameters
    /// - `bitmap_size`: Size of the bitmap in bits
    /// - `num_hashes`: Number of hash functions to use
    pub fn new(bitmap_size: usize, num_hashes: u32) -> Self {
        BloomFilter {
            filter: BF::with_size(bitmap_size, num_hashes),
            bitmap_size,
            num_hashes,
            keys: Vec::new(),
        }
    }

    /// Create a Bloom filter optimized for expected number of items
    ///
    /// # Parameters
    /// - `expected_items`: Expected number of items to insert
    /// - `false_positive_rate`: Desired false positive rate (e.g., 0.01 for 1%)
    pub fn with_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        // bloom crate expects f32 and u32
        let filter = BF::with_rate(false_positive_rate as f32, expected_items as u32);
        let bitmap_size = filter.num_bits();
        let num_hashes = filter.num_hashes();

        BloomFilter {
            filter,
            bitmap_size,
            num_hashes,
            keys: Vec::new(),
        }
    }

    /// Insert a key into the Bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        self.filter.insert(&key.to_vec());
        self.keys.push(key.to_vec());
    }

    /// Check if a key might be in the set
    /// Returns true if the key might be present (can have false positives)
    /// Returns false if the key is definitely not present (no false negatives)
    pub fn contains(&self, key: &[u8]) -> bool {
        self.filter.contains(&key.to_vec())
    }

    /// Encode the Bloom filter to bytes
    /// This stores the parameters and all inserted keys for reconstruction
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Write bitmap_size
        buf.extend_from_slice(&(self.bitmap_size as u64).to_be_bytes());

        // Write num_hashes
        buf.extend_from_slice(&(self.num_hashes as u64).to_be_bytes());

        // Write number of keys
        buf.extend_from_slice(&(self.keys.len() as u64).to_be_bytes());

        // Write all keys
        for key in &self.keys {
            buf.extend_from_slice(&(key.len() as u64).to_be_bytes());
            buf.extend_from_slice(key);
        }

        buf
    }

    /// Decode a Bloom filter from bytes
    /// This reconstructs the filter by re-inserting all stored keys
    pub fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
        let mut buf = [0u8; 8];

        // Read bitmap_size
        reader.read_exact(&mut buf)?;
        let bitmap_size = u64::from_be_bytes(buf) as usize;

        // Read num_hashes
        reader.read_exact(&mut buf)?;
        let num_hashes = u64::from_be_bytes(buf) as u32;

        // Read number of keys
        reader.read_exact(&mut buf)?;
        let num_keys = u64::from_be_bytes(buf) as usize;

        // Create new filter
        let mut bloom_filter = BloomFilter::new(bitmap_size, num_hashes);

        // Read and insert all keys
        for _ in 0..num_keys {
            reader.read_exact(&mut buf)?;
            let key_len = u64::from_be_bytes(buf) as usize;

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;

            bloom_filter.insert(&key);
        }

        Ok(bloom_filter)
    }

    /// Clear all bits in the filter
    pub fn clear(&mut self) {
        self.filter.clear();
        self.keys.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilter::with_rate(100, 0.01);

        // Insert some keys
        filter.insert(b"key1");
        filter.insert(b"key2");
        filter.insert(b"key3");

        // Should contain inserted keys
        assert!(filter.contains(b"key1"));
        assert!(filter.contains(b"key2"));
        assert!(filter.contains(b"key3"));

        // Should not contain non-inserted keys (with high probability)
        assert!(!filter.contains(b"key4"));
        assert!(!filter.contains(b"nonexistent"));
    }

    #[test]
    fn test_bloom_filter_encode_decode() {
        let mut filter = BloomFilter::with_rate(100, 0.01);

        filter.insert(b"apple");
        filter.insert(b"banana");
        filter.insert(b"cherry");

        // Encode
        let encoded = filter.encode();

        // Decode
        let decoded = BloomFilter::decode(Cursor::new(&encoded)).unwrap();

        // Verify decoded filter works correctly
        assert!(decoded.contains(b"apple"));
        assert!(decoded.contains(b"banana"));
        assert!(decoded.contains(b"cherry"));
        assert!(!decoded.contains(b"dragonfruit"));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut filter = BloomFilter::with_rate(1000, 0.01);

        // Insert 1000 keys
        for i in 0..1000 {
            let key = format!("key{}", i);
            filter.insert(key.as_bytes());
        }

        // Check false positive rate
        let mut false_positives = 0;
        let test_count = 10000;

        for i in 1000..1000 + test_count {
            let key = format!("key{}", i);
            if filter.contains(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let fpr = false_positives as f64 / test_count as f64;
        log::info!("False positive rate: {:.4} (expected ~0.01)", fpr);

        // FPR should be reasonably close to target (within 3x for small sample)
        assert!(fpr < 0.03, "False positive rate {} too high", fpr);
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut filter = BloomFilter::with_rate(100, 0.01);

        let keys = vec![b"test1", b"test2", b"test3", b"test4", b"test5"];

        // Insert all keys
        for key in &keys {
            filter.insert(*key);
        }

        // All inserted keys must be found (no false negatives)
        for key in &keys {
            assert!(
                filter.contains(*key),
                "False negative for key: {:?}",
                String::from_utf8_lossy(*key)
            );
        }
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut filter = BloomFilter::with_rate(100, 0.01);

        filter.insert(b"key1");
        filter.insert(b"key2");

        assert!(filter.contains(b"key1"));

        filter.clear();

        // After clear, should not contain anything
        assert!(!filter.contains(b"key1"));
        assert!(!filter.contains(b"key2"));
    }
}
