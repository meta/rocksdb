# WritePrepared Internals

**Files:** `utilities/transactions/write_prepared_txn_db.h`, `utilities/transactions/write_prepared_txn.h`, `utilities/transactions/write_prepared_txn.cc`

## Overview

WritePrepared transactions write data to the memtable at prepare time and only write a commit marker at commit time. This requires data structures to track which prepared entries have been committed and to determine visibility for concurrent readers.

## CommitCache

The CommitCache is the core data structure of WritePrepared. It is a fixed-size array of `std::atomic<CommitEntry64b>` indexed by `prepare_seq % COMMIT_CACHE_SIZE`.

**Configuration:**
- `COMMIT_CACHE_SIZE` = `2^wp_commit_cache_bits` (see `TransactionDBOptions`, private member `wp_commit_cache_bits`, default 23 = 8M entries, ~64MB; not user-configurable, internal testing parameter)
- Each entry stores a `(prepare_seq, commit_seq)` pair encoded in 64 bits

**Encoding (CommitEntry64b):**

The 64-bit encoding exploits the array index to avoid storing redundant bits:
- The lower `INDEX_BITS` (= `wp_commit_cache_bits`) of `prepare_seq` are implied by the array index
- The upper `PREP_BITS` of `prepare_seq` are stored in the high bits of the 64-bit word
- The delta `commit_seq - prepare_seq + 1` is stored in the low `COMMIT_BITS` (= `INDEX_BITS + PAD_BITS`)
- `PAD_BITS` = 8 (upper bits of sequence number reserved for type tags)

This lock-free encoding allows reads and writes to be simple atomic loads/stores, which on x86_64 compile to plain memory operations thanks to hardware cache coherency.

**Eviction:** Each insertion overwrites the previous entry at that index. The `max_evicted_seq_` variable tracks the maximum sequence number that has been evicted. When `max_evicted_seq_` advances, maintenance operations run (see below).

**Capacity estimation:** With 80K transactions/sec and 8M entries, an entry remains in cache for ~50 seconds. In practice, the gap between prepare and commit is sub-millisecond.

## PreparedHeap

The `PreparedHeap` (see inner class in `utilities/transactions/write_prepared_txn_db.h`) tracks all prepared-but-not-yet-committed sequence numbers.

**Design:** A deque-based structure with an auxiliary min-heap for amortized O(1) erase:
- `heap_`: deque of prepare sequence numbers (insertion order)
- `erased_heap_`: min-heap of removed entries not yet at the front of `heap_`
- `heap_top_`: atomic variable for lock-free read of the minimum

**Operations:**
- `push(v)`: appends to deque (must be called in ascending order, enforced by primary write queue)
- `pop()`: removes from front; skips entries that appear in `erased_heap_`
- `erase(v)`: if `v` is at the front, pops it; otherwise adds to `erased_heap_`
- `top()`: returns `heap_top_` atomically (kMaxSequenceNumber if empty)

**Invariant:** Entries are always added in ascending sequence order because `AddPrepared()` is called only from the primary write queue's `PreReleaseCallback`.

## IsInSnapshot Algorithm

`WritePreparedTxnDB::IsInSnapshot(prep_seq, snapshot_seq, min_uncommitted, snap_released)` determines whether a value tagged with `prep_seq` is visible at `snapshot_seq`.

**Fast path:**

Step 0: If `prep_seq == 0`, return true immediately (sequence number zeroed by compaction, meaning fully committed and visible to all). Step 1: If `snapshot_seq < prep_seq`, return false (written after snapshot). Step 2: If `prep_seq < min_uncommitted`, return true (known committed before any live transaction). Step 3: Check CommitCache at `prep_seq % COMMIT_CACHE_SIZE`. If found with matching `prep_seq`, return `commit_seq <= snapshot_seq`.

**Slow path (cache miss):**

Step 4: If `prep_seq > max_evicted_seq_`, the entry was never in the cache and is still in PreparedHeap (not committed), return false. Step 5: Check `delayed_prepared_` set (prepared entries that were moved from PreparedHeap when `max_evicted_seq_` advanced past them). If found and not in `delayed_prepared_commits_`, still uncommitted. Step 6: If `max_evicted_seq_ < snapshot_seq`, the commit must have happened before the snapshot, return true. Step 7: Check `old_commit_map_` for evicted entries that overlap with live snapshots.

**Atomicity considerations:** The algorithm uses a do-while loop to handle the race between reading `max_evicted_seq_` and CommitCache. If `max_evicted_seq_` changes during the lookup, the loop retries.

## Delayed Prepared

When `max_evicted_seq_` advances past a `prepare_seq` still in `PreparedHeap`, the entry is moved to `delayed_prepared_` (a set protected by `prepared_mutex_`). This represents an abnormally long-running transaction.

The commit of a delayed prepared entry involves four non-atomic steps:
1. Update CommitCache
2. Add to `delayed_prepared_commits_`
3. Publish sequence number
4. Remove from `delayed_prepared_`

The `IsInSnapshot` algorithm handles this non-atomicity with a second CommitCache lookup after checking `delayed_prepared_`.

## old_commit_map

The `old_commit_map_` (`std::map<SequenceNumber, std::vector<uint64_t>>`) stores evicted commit entries that overlap with live snapshots:
- Key: snapshot sequence number
- Value: sorted vector of `prepare_seq` values that committed after that snapshot

This is needed for long-running read-only transactions (e.g., backups) whose snapshots are older than `max_evicted_seq_`. If a snapshot is not found in `old_commit_map_`, it has been released, and `IsInSnapshot` sets `*snap_released = true`.

**Garbage collection:** `old_commit_map_` entries are cleaned up when the corresponding snapshot is released, detected during the periodic snapshot list refresh when advancing `max_evicted_seq_`.

## max_evicted_seq Advancement

To reduce maintenance overhead, `max_evicted_seq_` is advanced by 1% of `COMMIT_CACHE_SIZE` at a time (rather than once per eviction). This amortizes the cost of:
- Checking PreparedHeap top against the new max
- Fetching live snapshot list (requires db mutex)
- Updating `old_commit_map_`

## Snapshot Cache

The snapshot list is cached in a lock-free array of `std::atomic<uint64_t>` for efficient lookup during `IsInSnapshot`:
- Size: `2^wp_snapshot_cache_bits` (default 7 = 128 entries)
- Single writer updates sorted in ascending order
- Concurrent readers scan from the end
- Overflow beyond array size stored in a mutex-protected vector

## Smallest Uncommitted Optimization

The smallest uncommitted sequence number (`min_uncommitted`) is tracked and stored with each snapshot. When `prep_seq < min_uncommitted`, IsInSnapshot can immediately return true without consulting CommitCache, reducing CPU cache misses.

Sources: `delayed_prepared_` minimum (if non-empty), otherwise `PreparedHeap::top()`.

## Duplicate Key Handling

WritePrepared assigns the same sequence number to all keys in a write batch. If the batch contains duplicate keys, the memtable rejects the second insertion (same sequence number for the same key). To handle this:

Step 1: The write batch is divided into sub-batches, one after each duplicate key. Step 2: Each sub-batch gets a separate sequence number. Step 3: The memtable inserter advances the sequence number when the memtable returns false.

When using the Transaction API, duplicate detection is done cheaply via `WriteBatchWithIndex`. When using `::CommitBatch` to write directly, the DB must iterate the batch to count sub-batches, which incurs overhead.

Important: If a column family is dropped, the WAL must not contain entries belonging to that column family. Otherwise, recovery cannot determine duplicates without the column family's comparator.
