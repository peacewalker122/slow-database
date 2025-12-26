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
* Block checksums
* Sparse index

### Zig Exercise (Optional)

* Implement SSTable writer in Zig

---

## Phase 6 — Compaction (Week 6)

### Concepts

* Merge
* Tombstones
* Space amplification

### Implementation

* Level 0 → Level 1 compaction
* Delete handling

### Deliverable

* Bounded disk usage

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
