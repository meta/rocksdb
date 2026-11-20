# WAL Filter and Replication

**Files:** `include/rocksdb/wal_filter.h`, `include/rocksdb/transaction_log.h`, `include/rocksdb/db.h`, `db/wal_manager.h`, `db/wal_manager.cc`, `db/transaction_log_impl.h`, `db/transaction_log_impl.cc`

## WAL Filter

### Overview

`WalFilter` (in `include/rocksdb/wal_filter.h`) is a `Customizable` interface that allows applications to inspect and modify WAL records during recovery replay. It is invoked from a single thread during `DB::Open()`.

### Interface

The filter provides two main callbacks:

**ColumnFamilyLogNumberMap**: Called once before replay with mappings from column family IDs to their log numbers and from column family names to IDs. This allows the filter to determine which WAL records are relevant to which column families.

**LogRecordFound**: Called for each WAL record during replay. Parameters include:
- `log_number`: The WAL file's log number
- `log_file_name`: The WAL file path
- `batch`: The original `WriteBatch`
- `new_batch`: Output parameter for a modified batch
- `batch_changed`: Set to true if `new_batch` was populated

Returns a `WalProcessingOption`:

| Option | Effect |
|--------|--------|
| `kContinueProcessing` | Apply the record normally |
| `kIgnoreCurrentRecord` | Skip this record but continue replay |
| `kStopReplay` | Stop replay; remaining WAL data is discarded |
| `kCorruptedRecord` | Report the record as corrupted |

Important: The `new_batch` must NOT contain more records than the original batch. Violations cause recovery failure.

### Configuration

Set via `DBOptions::wal_filter` (see `include/rocksdb/options.h`). The filter is invoked from a single thread and does not need to be thread-safe. Exceptions must NOT propagate out of the filter into RocksDB.

## Replication via GetUpdatesSince

### Overview

`DB::GetUpdatesSince()` (see `include/rocksdb/db.h`) enables logical replication by returning a `TransactionLogIterator` that yields `WriteBatch` records from WAL files starting at a given sequence number.

### API

`DB::GetUpdatesSince(SequenceNumber seq_number, unique_ptr<TransactionLogIterator>* iter, const TransactionLogIterator::ReadOptions& read_options)`:
- Returns an iterator over WAL records with sequence numbers >= `seq_number`
- The iterator is continuous; it stops at any gap in sequence numbers

### TransactionLogIterator

`TransactionLogIterator` (in `include/rocksdb/transaction_log.h`) provides:
- `Valid()`: Whether the iterator is positioned at a valid record
- `Next()`: Advance to the next `WriteBatch`
- `status()`: Check for errors
- `GetBatch()`: Returns a `BatchResult` containing the sequence number and `WriteBatch`

### Implementation

`WalManager::GetUpdatesSince()` in `db/wal_manager.cc`:

Step 1: Get all sorted WAL files (live and archived) via `GetSortedWalFiles()`.

Step 2: Use binary search (`RetainProbableWalFiles()`) to find the WAL file containing the target sequence number. This avoids opening all files.

Step 3: Create a `TransactionLogIteratorImpl` that opens WAL files using `log::Reader` (not `FragmentBufferedReader`). Tailing is implemented by checking `IsEOF()` and calling `UnmarkEOF()` on the regular reader when the buffer is exhausted, allowing the reader to pick up newly appended data.

Note: `GetUpdatesSince()` is not supported when `seq_per_batch` is true (returns `Status::NotSupported`). This is because `seq_per_batch` mode assigns one sequence number per batch rather than per key, which breaks the sequence-based iteration contract.

### Iterator Tailing Behavior

`TransactionLogIteratorImpl` has nuanced behavior at the live WAL tail:
- It can become invalid with `Status::OK()` when no new data is available
- When new data arrives, `UnmarkEOF()` re-arms the reader and subsequent `ReadRecord()` calls pick up the new records
- If continuity is lost (e.g., a WAL is archived between reads), the iterator returns `Status::TryAgain()` indicating the caller should create a new iterator

### WAL Retention for Replication

For replication to work, WAL files must be retained long enough for the iterator to read them. This requires:
- Setting `WAL_ttl_seconds` or `WAL_size_limit_MB` to large values to archive WALs instead of deleting them
- Archived WALs are stored in `<wal_dir>/archive/`

`GetSortedWalFiles()` returns both live and archived WALs, sorted by log number. When live and archived WALs overlap (race condition during archival), the archived copy takes precedence.

## GetSortedWalFiles

`WalManager::GetSortedWalFiles()` in `db/wal_manager.cc`:

Step 1: List live WAL files in the WAL directory.

Step 2: List archived WAL files in the archive directory.

Step 3: Merge the two lists, deduplicating by preferring archived copies when a log number appears in both directories (handles the race condition where a WAL is moved to archive between the two directory scans).

The `need_seqnos` parameter controls whether the first record of each WAL is read to extract its starting sequence number. When false, sequence numbers are set to 0 (faster but less precise).

## ReadFirstRecord Cache

`WalManager` maintains a `read_first_record_cache_` (protected by `read_first_record_cache_mutex_`) that maps log numbers to their first sequence numbers. This avoids re-reading WAL files on repeated calls to `GetSortedWalFiles()` or `GetUpdatesSince()`. Cache entries are evicted when the corresponding WAL file is deleted.
