# SSTable Format Design

## Overview

The SSTable (Sorted String Table) format provides efficient storage and retrieval of key-value pairs with an index for fast lookups. All components include CRC32 checksums for data integrity verification.

## File Structure

```
┌─────────────────────────────────────────┐
│         DATA BLOCKS                     │
│  (Sorted key-value records)             │
│                                         │
│  Record 1: [type|key_len|val_len|       │
│             key|value|checksum]         │
│  Record 2: ...                          │
│  Record N: ...                          │
├─────────────────────────────────────────┤
│         INDEX BLOCK                     │
│  (BTree of key -> offset mappings)      │
│                                         │
│  Entry count: u64 (8 bytes)             │
│  Entry 1: [key_len|key|offset]          │
│  Entry 2: [key_len|key|offset]          │
│  Entry N: ...                           │
├─────────────────────────────────────────┤
│         FOOTER (44 bytes)               │
│                                         │
│  data_block_start:   u64 (8 bytes)      │
│  data_block_end:     u64 (8 bytes)      │
│  index_block_start:  u64 (8 bytes)      │
│  index_block_end:    u64 (8 bytes)      │
│  index_checksum:     u32 (4 bytes)      │
│  magic_number:       u32 (4 bytes)      │
│                      = 0xDB055555       │
│  footer_checksum:    u32 (4 bytes)      │
└─────────────────────────────────────────┘
```

## Components

### 1. Data Blocks

Contains sorted key-value records. Each record has:

```
┌──────────────┬──────────┬──────────┬─────┬───────┬──────────┐
│ record_type  │ key_len  │ val_len  │ key │ value │ checksum │
│   (1 byte)   │ (8 bytes)│ (8 bytes)│     │       │ (4 bytes)│
└──────────────┴──────────┴──────────┴─────┴───────┴──────────┘
```

- **record_type**: `1` = Put, `2` = Delete (tombstone)
- **key_len**: Length of key in bytes (u64)
- **val_len**: Length of value in bytes (u64)
- **key**: Raw key bytes
- **value**: Raw value bytes
- **checksum**: CRC32 of value for integrity verification

### 2. Index Block

Uses a BTree structure to map keys to their file offsets:

```
┌──────────────┐
│ entry_count  │  Number of index entries (u64)
├──────────────┤
│ Index Entry 1│
│  - key_len   │  (u64)
│  - key       │  (variable)
│  - offset    │  (u64) - byte offset in data block
├──────────────┤
│ Index Entry 2│
│   ...        │
└──────────────┘
```

The entire index block is protected by a CRC32 checksum stored in the footer.

The BTree provides:
- **O(log n)** search complexity
- **Sorted iteration** for range queries
- **Memory efficient** representation

### 3. Footer

Fixed 44-byte structure at the end of the file:

```rust
pub struct SSTableFooter {
    pub data_block_start: u64,   // Where data blocks begin
    pub data_block_end: u64,     // Where data blocks end
    pub index_block_start: u64,  // Where index block begins
    pub index_block_end: u64,    // Where index block ends
    pub index_checksum: u32,     // CRC32 of entire index block
    pub magic_number: u32 = 0xDB055555 (validation),
    pub footer_checksum: u32 (CRC32 of all footer fields except this one)
}
```

**Checksums in Footer:**
1. **index_checksum**: Verifies integrity of entire index block
2. **magic_number**: Validates file format (0xDB055555)
3. **footer_checksum**: Verifies integrity of footer metadata itself

## Checksum Strategy

### Three Levels of Protection:

1. **Data Record Level** (per record)
   - Each value has its own CRC32
   - Detects corruption in individual records
   - Computed on value bytes only

2. **Index Block Level** (entire index)
   - Single CRC32 for entire index block
   - Detects any corruption in key→offset mappings
   - Computed on all index entries

3. **Footer Level** (metadata)
   - CRC32 of all footer fields
   - Ensures metadata integrity
   - Prevents incorrect file navigation

### Verification Flow:

```
Read Request
    ↓
Read Footer → Verify footer_checksum ✓
    ↓
Read Index Block → Verify index_checksum ✓
    ↓
Lookup Key → Get Offset
    ↓
Read Data Record → Verify value checksum ✓
    ↓
Return Value
```

## Operations

### Write (Flush)

1. Collect sorted records from skiplist memtable
2. Write data blocks sequentially, tracking offsets
3. Build index (BTree) mapping keys → offsets
4. **Calculate index block CRC32**
5. Write index block
6. **Create footer with index checksum**
7. **Calculate footer CRC32**
8. Write footer with both checksums
9. Sync to disk

```rust
pub fn flush_memtable(memtable: &mut SkipList) -> Result<(), std::io::Error>
```

### Read (Search)

1. Read footer from end of file
2. **Verify footer checksum** ✓
3. Read index block using footer metadata
4. **Verify index checksum** ✓
5. Search BTree index for key → offset
6. Read record at offset from data block
7. **Verify record checksum** ✓
8. Return value

```rust
pub fn search_sstable<R: Read + Seek>(
    reader: R, 
    key: &[u8]
) -> Result<Option<Vec<u8>>, std::io::Error>
```

## Error Detection

The multi-layer checksum approach can detect:

- **Bit flips** in storage media
- **Partial writes** from crashes
- **Memory corruption** during I/O
- **Disk sector failures**
- **Software bugs** in encoding/decoding

## Benefits

1. **Fast Lookups**: O(log n) via BTree index instead of linear scan
2. **Sorted Data**: Natural ordering enables range queries
3. **Data Integrity**: Triple-layer CRC32 checksums detect corruption at every level
4. **Space Efficient**: Compact binary format with minimal overhead
5. **Immutable**: Once written, never modified (append-only)
6. **Self-Describing**: Footer contains all metadata
7. **Fail-Fast**: Corrupted data detected immediately on read

## Performance Characteristics

- **Write**: O(n) - sequential writes of sorted data in 4KB blocks
- **Read**: O(log m + k) - binary search on m blocks, linear scan of k records within block
- **Space Overhead**: 
  - Sparse index: ~32 bytes per block (first_key + last_key + metadata)
  - Bloom filter: ~16 bytes per key (1% FPR)
  - Block overhead: minimal (only block boundaries)
- **Checksum Cost**: ~0.5 GB/s throughput on modern CPUs (CRC32)

## Block-Based Storage (Phase 5)

### Fixed-Size Blocks

The SSTable is divided into fixed-size 4KB blocks:

```
┌─────────────────────────────────────────┐
│         BLOCK 1 (4KB)                   │
│  First Key: "apple"                     │
│  Records: 15                            │
│  [record1, record2, ..., record15]      │
├─────────────────────────────────────────┤
│         BLOCK 2 (4KB)                   │
│  First Key: "banana"                    │
│  Records: 12                            │
│  [record16, record17, ..., record27]    │
├─────────────────────────────────────────┤
│         SPARSE INDEX                    │
│  Block 1: first_key="apple"             │
│           last_key="avocado"            │
│           offset=0                      │
│           record_count=15               │
│  Block 2: first_key="banana"            │
│           last_key="cherry"             │
│           offset=4096                   │
│           record_count=12               │
└─────────────────────────────────────────┘
```

### Sparse Index

Instead of indexing every key (dense index), we only index the first key of each block:

- **Dense Index**: 100,000 keys → 100,000 index entries (~1.6MB)
- **Sparse Index**: 100,000 keys → ~30-50 blocks → 30-50 index entries (~1-2KB)

This dramatically reduces index size for high-cardinality keys (like UUIDs).

### Search Algorithm

1. **Check Bloom Filter**: Is key possibly in SSTable?
2. **Binary Search Sparse Index**: Find block where `first_key <= target <= last_key`
3. **Linear Scan Block**: Read and check each record in the 4KB block

Since blocks are small (4KB) and contain few records (~10-20), linear scan is efficient.

### Block Efficiency

**Ordered Keys** (e.g., `testkey:0001`, `testkey:0002`):
- High compression within blocks
- Fewer blocks needed
- Example: 100 keys → 1 block

**UUID Keys** (e.g., `40c0794d-33a0-4a8b-8eef-ce2129452b68`):
- Random distribution
- More blocks needed
- Example: 100 keys → 3 blocks

The sparse index handles both cases efficiently.

## Future Enhancements

- **Block-based compression**: Compress 4KB blocks
- **Multiple levels**: LSM-tree compaction with L0, L1, L2, etc.
- **xxHash**: Faster non-cryptographic hash (optional)
- **Prefix compression**: Store key prefixes to save space

## Alignment with PLAN.md

This implements **Phase 4 (LSM)** and **Phase 5 (SSTable Format & Blocks)**:

- ✅ Memtable → Flush → SSTable
- ✅ Binary search reads via index
- ✅ Footer metadata with checksums
- ✅ BTree index structure with checksum
- ✅ Data integrity verification (CRC32)
- ✅ Fixed-size 4KB blocks
- ✅ Bloom filters
- ✅ Sparse index for efficient high-cardinality key support
