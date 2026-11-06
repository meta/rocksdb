# Write Policies

**Files:** `utilities/transactions/pessimistic_transaction_db.h`, `utilities/transactions/pessimistic_transaction.cc`, `utilities/transactions/write_prepared_txn.h`, `utilities/transactions/write_unprepared_txn.h`, `include/rocksdb/utilities/transaction_db.h`

## Overview

The write policy determines when transaction data is written to the memtable. It is configured via `TransactionDBOptions::write_policy` (see `TxnDBWritePolicy` enum in `include/rocksdb/utilities/transaction_db.h`).

| Policy | When data is written | Commit phase | Memory usage | Maturity |
|--------|---------------------|--------------|--------------|----------|
| `WRITE_COMMITTED` (default) | At commit time | Heavy (writes data) | Buffers all writes | Most mature |
| `WRITE_PREPARED` | At prepare time | Light (writes marker) | Buffers all writes | Experimental |
| `WRITE_UNPREPARED` | During execution | Light (writes marker) | Minimal (flushes to DB) | Experimental |

## WriteCommitted

`WriteCommittedTxnDB` and `WriteCommittedTxn` (see `utilities/transactions/pessimistic_transaction_db.h` and `utilities/transactions/pessimistic_transaction.h`).

**Data flow:**

Step 1: `Put("k1", "v1")` -- buffered in `WriteBatchWithIndex` in memory. Step 2: `Put("k2", "v2")` -- buffered in `WriteBatchWithIndex` in memory. Step 3: `Commit()` -- entire batch written to WAL and memtable atomically via `DBImpl::WriteImpl()`.

**Advantages:**
- Simplest implementation; uncommitted data never visible to other transactions
- Supports all features including user-defined timestamps
- Rollback is trivial (discard the in-memory batch)

**Disadvantages:**
- High memory usage for large transactions (all writes buffered)
- Commit phase is heavyweight (all data written at once)
- In 2PC with serial commits (e.g., MySQL), commit latency is a throughput bottleneck

## WritePrepared

`WritePreparedTxnDB` and `WritePreparedTxn` (see `utilities/transactions/write_prepared_txn_db.h` and `utilities/transactions/write_prepared_txn.h`).

**Data flow:**

Step 1: `Put("k1", "v1")` -- buffered in `WriteBatchWithIndex` in memory. Step 2: `Prepare()` -- batch written to WAL + memtable. Data tagged with `prepare_seq`. Added to `PreparedHeap`. Step 3: `Commit()` -- commit marker written to WAL only. Mapping `(prepare_seq -> commit_seq)` added to CommitCache. Removed from PreparedHeap.

**Key difference from WriteCommitted:** The commit phase only writes a small marker to WAL and updates an in-memory commit map. It does NOT write data to memtable. This is the defining optimization.

**Dual write queue optimization:** When `two_write_queues` is enabled in `DBOptions` (recommended for WritePrepared but not the default; `DBOptions::two_write_queues` defaults to `false`), prepare writes go through the main write queue (WAL + memtable) and commits go through a second queue (WAL only). This prevents commit writes from blocking behind prepare writes.

**Visibility:** Data written at prepare time is in the memtable but not yet visible. Readers use `WritePreparedTxnDB::IsInSnapshot()` to check whether a value's `prepare_seq` has been committed before the reader's snapshot. See chapter 6 for the full algorithm.

## WriteUnprepared

`WriteUnpreparedTxnDB` and `WriteUnpreparedTxn` (see `utilities/transactions/write_unprepared_txn_db.h` and `utilities/transactions/write_unprepared_txn.h`).

**Data flow:**

Step 1: `Put()` -- if write batch size exceeds `write_batch_flush_threshold`, flush to DB with `kTypeBeginUnprepareXID` marker. Record sequence number in `unprep_seqs_`. Step 2: More `Put()` operations -- continue flushing when threshold exceeded. Step 3: `Prepare()` -- flush remaining batch with `kTypeBeginPersistedPrepareXID`. Step 4: `Commit()` -- write commit marker. Pre-release callback updates CommitCache for all entries in `unprep_seqs_`.

**Key difference from WritePrepared:** Data is written to the DB incrementally BEFORE prepare, not at prepare time. This allows transactions of arbitrary size without memory constraints.

**Configuration:** The flush threshold is controlled by `TransactionDBOptions::default_write_batch_flush_threshold` (DB-level default) or `TransactionOptions::write_batch_flush_threshold` (per-transaction override). Default is 0 (disabled).

## WAL Markers

Each write policy uses different WAL record type markers:

| Marker | Used by | Purpose |
|--------|---------|---------|
| `kTypeBeginPrepareXID` | WriteCommitted | Start of prepared batch (non-persisted prepare) |
| `kTypeBeginPersistedPrepareXID` | WritePrepared, WriteUnprepared | Start of prepared batch (persisted prepare) |
| `kTypeBeginUnprepareXID` | WriteUnprepared | Start of unprepared batch (data flushed before prepare) |
| `kTypeEndPrepareXID` | All (2PC) | End of prepared section, contains transaction name |
| `kTypeCommitXID` | All (2PC) | Commit marker, contains transaction name |
| `kTypeRollbackXID` | WritePrepared, WriteUnprepared | Rollback marker |

Important: The WAL format differs between WriteCommitted and WritePrepared/WriteUnprepared. Switching write policies requires emptying the WAL first (flush the DB).

## Write Policy Comparison

| Aspect | WriteCommitted | WritePrepared | WriteUnprepared |
|--------|---------------|---------------|-----------------|
| Data in memtable | Only after commit | After prepare | During execution |
| Commit cost | Heavy (data + WAL) | Light (WAL marker in common case) | Light (WAL marker in common case) |
| Memory for writes | Unbounded | Unbounded | Bounded by flush threshold |
| Rollback | Discard batch | Write compensating entries | Write compensating entries |
| Visibility tracking | Not needed | CommitCache + PreparedHeap | CommitCache + PreparedHeap + unprep_seqs |
| User-defined timestamps | Supported | Not supported | Not supported |
| Read overhead | None | ~1% (IsInSnapshot check) | ~1% + read-your-own-writes overhead |
| 2PC recovery | Replays from WAL | Rebuilds CommitCache | Rebuilds CommitCache + unprep_seqs |
