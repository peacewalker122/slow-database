# Database-from-Scratch Roadmap

**Goal:** Build a correct, minimal KV database with high learning ROI, WASM-compatible, and a clear path to distribution later.

This roadmap is language-agnostic but optimized for **Rust-first** with optional **Zig deep dives**.

---

## Phase 0 — Mental Model & Constraints (Week 0)

### Objectives

* Fix scope
* Avoid premature complexity

### Decisions (Lock These In)

* **Type:** Embedded Key-Value Store
* **API:** `put(key, value)`, `get(key)`, `delete(key)`
* **Consistency:** Single-node, single-writer
* **Storage:** Append-only
* **No SQL, no transactions (yet)**
* **No distribution**

### Deliverable

* Design doc (1–2 pages) answering:
  * What is a key?
  * What is a value?
  * What guarantees does `put` give?

---

## Phase 1 — Persistence Without Indexes (Week 1)

### Concepts

* DB file lifecycle
* On-disk record format

### Implementation

* Single file: `data.log`
* Append-only writes
* Linear scan reads

```
| key_len | value_len | key | value | checksum |
```

### Learning Pain (Intentional)
* Binary encoding
* Endianness
* Partial writes

### Deliverable
* DB survives restart (It can survives, but need the actual flow to re-read the data back)
* Correctness > performance

---

## Phase 2 — In-Memory Index (Week 2)

### Concepts
* Indirection
* Index vs data separation

### Implementation
* HashMap<Key, FileOffset>
* Rebuild index on startup

### Deliverable
* O(1) reads
* Still append-only writes

---

## Phase 3 — Crash Safety & WAL (Week 3)

### Concepts
* Write-Ahead Logging
* Durability
* Recovery

### Implementation
* `data.log` acts as WAL
* fsync on write
* Replay on startup

### Tests
* Kill process mid-write
* Restart and validate

### Deliverable

* Crash-safe DB

---

## Phase 4 — Storage Engine Choice: LSM (Week 4)

### Concepts

* Log-Structured Merge Trees
    - Memtable -> Flush -> SSTable.
* Write amplification

### Implementation

* Memtable (in-memory map) 
    - Use BtreeMap for this.
* SSTable (sorted immutable file)
    - When Memtable reach some "threshold" it will be flushed and merged with the SSTable.
* Binary search reads
    - Build the file content from the SSTable? to be read in our program.
* Compaction

### Consideration
- Skiplist or BTree memtable.
    - Skiplist guarantee the insertion to be O(log(n)), while BTree give the same O(log(n)) on average. With worst case O(n).
    - Cache, Skiplist weren't support L1 cache, on the other hand BTree had better cache aligment. Trade off on the read performance.
### Deliverable
* No more linear scans, but Log(n) scan...

---

## Phase 5 — SSTable Format & Blocks (Week 5)

### Concepts

* Fixed-size blocks
* Footer index
* Bloom filters

### Implementation
* 4KB blocks
    - Question is, how do we track the size of each insertion of data here? 
        - Atomic Counter for each data insertion, process the key + data. How to maintain the consistency of the block that won't be exceed the given size
* Sparse index
    - Index that stored the range of value information, so when there's fetch of data came, it check the index first, check the key range from the blocks and then linearly scan the block since the cardinality is low. 
    - Index will be implemented as "list", for increasing the locality. For example each index contains few metadata:
        - first_key & last_key ~= 30-40 Bytes
        - block_offset = 8 bytes
        - record_count = 4 bytes
    And for example there's 100.000 data that were stored included the tombstone. Furthermore, if the average of the data we need to store is ~= 200 Bytes and each block store 4KB of data, each block will stored ~20 Data and there will be 5000 different index data for it using that metadata schema, approximately the index would add overhead arround 11.2 MB. 

### Challenge
If we trying to implement block in our database, think about the case where the key is having high cardinality. If we trying to built the index using it. How the index would be maintained?

To introduce, lets quantify how bad is "random" and "unpredictable" key to our database. Start with the block, we expect the block to contains an ordered key, for example: `testkey:1`, `testkey:2`, and so on. But if we put uuid to it such as `40c0794d-33a0-4a8b-8eef-ce2129452b68` it will made an overhead to the block and the index. on the first example we can compacted it into 1 block with 1 corresponding index. But on the second example it will add N blocks and N indexes.

So for example, we have an 100.000 incoming request, 40% Of it is ordered key, assume that the key and value only took approximately ~200 Bytes, with an block that only have 4KB in size, it will made ~200 blocks. And for the remainder 60% it will took 60.000 Different blocks ands indexes. 

So to solve this, we still using blocks and sparse index, the blocks still do their things, to store the keys and their value. While the index is storing the first value within the block.

### Zig Exercise (Optional)

* Implement SSTable writer in Zig

---

## Phase 6 — Compaction (Week 6)

### Concepts

* Merge
* Tombstones
* Space amplification

### Current Problem

The current implementation appends each memtable flush as a new SSTable section to `app.db`. This creates multiple SSTable "generations" in the same file, but only the **last** footer is readable. This means:
- Older data from previous flushes becomes inaccessible
- The `test_active_data` test fails because `key88` from an earlier flush cannot be found
- Disk space grows unbounded without reclaiming deleted keys

### Implementation Strategy: Compaction on Flush

Instead of implementing a full LSM-tree with multiple levels, we start with **simple compaction on every flush**:

#### Step 1: Read Existing SSTable
- Check if `app.db` exists
- If yes, read all records from the existing SSTable:
  - Read footer to locate data blocks
  - Read bloom filter and sparse index
  - Iterate through all blocks sequentially
  - Collect all key-value pairs into a temporary structure (BTreeMap for sorting)

#### Step 2: Merge with Memtable
- Combine existing SSTable data with new memtable data
- Merge strategy (newest wins):
  ```
  for each key:
    if key in memtable: use memtable value (newer)
    else if key in old_sstable: use old_sstable value
  ```
- Handle tombstones (RecordType::Delete):
  - If memtable has Delete for a key, remove that key entirely from merged result
  - Don't carry forward Delete tombstones from old SSTable (they've done their job)

#### Step 3: Write Merged SSTable
- Replace `app.db` with the merged result:
  - Write to temporary file `app.db.tmp`
  - Use existing `flush_memtable` logic (blocks, sparse index, bloom filter, footer)
  - Rename `app.db.tmp` → `app.db` (atomic on most filesystems)
- This ensures we never have corrupt intermediate states

#### Step 4: Helper Functions Needed

```rust
// Read all records from existing SSTable into BTreeMap
pub fn read_all_sstable_records(file_path: &str) 
    -> Result<BTreeMap<Vec<u8>, (RecordType, Vec<u8>)>, std::io::Error>

// Merge old SSTable data with new memtable, handling tombstones
pub fn merge_with_compaction(
    old_data: BTreeMap<Vec<u8>, (RecordType, Vec<u8>)>,
    new_memtable: &SkipList
) -> BTreeMap<Vec<u8>, (RecordType, Vec<u8>)>

// Modified flush that does compaction
pub fn flush_memtable_with_compaction(
    memtable: SkipList, 
    db_path: &str
) -> Result<(), std::io::Error>
```

### Trade-offs & Considerations

**Pros:**
- Simple implementation (no level management)
- Always have exactly one SSTable per file
- All data remains accessible
- Tombstones are cleaned up immediately
- Bounded disk usage

**Cons:**
- Write amplification: O(n) on every flush (read entire SSTable, rewrite everything)
- Not suitable for large databases (>1GB)
- No background compaction (blocks writes during compaction)

**Future Optimizations (Phase 6.5+):**
- Multi-level LSM: L0 → L1 → L2 with size-tiered compaction
- Background compaction thread
- Incremental merge (don't rewrite entire SSTable)
- Multiple SSTable files with manifest tracking

### Implementation Details

#### Tombstone Handling
- **During Merge:** If memtable has `Delete(key)`, remove `key` from result entirely
- **Old Tombstones:** Don't preserve Delete records from old SSTable (space reclamation)
- **Correctness:** Since we only have one SSTable, tombstones can be removed after merge

#### Atomicity
- Write to `app.db.tmp` first
- Only rename to `app.db` after successful write and fsync
- On crash, either old `app.db` exists (valid) or new one exists (valid)
- Never have partial/corrupt SSTable

#### Testing Strategy
1. Test single flush (baseline)
2. Test multiple flushes with reads (verify all data accessible)
3. Test tombstone handling (deleted keys don't appear after compaction)
4. Test crash during compaction (atomic rename)
5. Make `test_active_data` pass (key88 should be found)

### Deliverable

* Bounded disk usage
* All flushed data remains accessible across multiple flushes
* Tombstone cleanup and space reclamation
* `test_active_data` test passes

---

## Phase 7 — Concurrency Model (Week 7)

### Concepts

* Single-writer principle
* Read concurrency

### Implementation

* One writer thread
* Multiple readers
* RWLock or MVCC-lite

---

## Phase 8 — Public Interface & Testing (Week 8)

### Interfaces

* Native API (Rust)
* External protocol (Redis-like or HTTP)

### Tests

* Black-box tests
* Fuzz testing

---

## Phase 9 — WASM Compatibility (Week 9)

### Concepts

* Abstract filesystem
* Deterministic memory

### Implementation

* Storage trait
* WASM backend

### Deliverable

* Same DB runs in browser

---

## Phase 10 — Hardening & Observability (Week 10)

### Concepts

* Metrics
* Debuggability

### Implementation

* Stats endpoint
* Internal invariants

---

## Phase 11 — (Optional) Distribution & Consensus (Post-v1)

### Concepts

* Raft
* Replication
* Leader election

⚠️ **Do not start here**

---

## Summary

* Rust = correctness backbone
* Zig = targeted suffering
* C = optional historical pain

You now have a production-shaped learning path.
