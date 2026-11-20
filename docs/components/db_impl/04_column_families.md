# Column Family Management

**Files:** `db/column_family.h`, `db/column_family.cc`, `db/db_impl/db_impl.h`, `db/db_impl/db_impl.cc`

## Overview

Column families provide logical partitioning of data within a single RocksDB database. Each column family has its own memtable, immutable memtable list, LSM tree (set of SST files organized into Versions), compaction picker, and options. All column families share a common WAL, MANIFEST, and DB mutex.

Every database has at least one column family -- the "default" column family (named `"default"`, ID 0), which cannot be dropped.

## Key Data Structures

### ColumnFamilyData

`ColumnFamilyData` (defined in `db/column_family.h`) holds all per-column-family state:

| Field | Type | Purpose |
|-------|------|---------|
| `id_` | `uint32_t` | Unique column family ID (monotonically increasing, never reused) |
| `name_` | `string` | Column family name |
| `mem_` | `MemTable*` | Current active (mutable) memtable |
| `imm_` | `MemTableList` | List of immutable memtables pending flush |
| `current_` | `Version*` | Current version (SST file set) |
| `super_version_` | `SuperVersion*` | Combined view of mem + imm + current |
| `internal_comparator_` | `InternalKeyComparator` | Comparator for internal keys |
| `table_cache_` | `unique_ptr<TableCache>` | Per-CF table cache |
| `compaction_picker_` | `unique_ptr<CompactionPicker>` | Picks compactions for this CF |
| `ioptions_` | `ImmutableOptions` | Immutable options snapshot |
| `mutable_cf_options_` | `MutableCFOptions` | Latest mutable options (may differ from current SuperVersion's options) |

`ColumnFamilyData` objects are linked in a circular doubly-linked list via `next_` and `prev_` pointers, managed by `ColumnFamilySet`.

### ColumnFamilySet

`ColumnFamilySet` (defined in `db/column_family.h`) manages all column families in a database:

- Maintains a `name -> id` map and an `id -> ColumnFamilyData*` map
- Provides iteration over all live column families (using a circular linked list with a dummy head node)
- Assigns monotonically increasing column family IDs via `GetNextColumnFamilyID()`
- Thread safety: mutations require both the DB mutex and the write thread. Reads require at least one of those

### ColumnFamilyHandleImpl

`ColumnFamilyHandleImpl` (defined in `db/column_family.h`) is the user-facing handle returned by `CreateColumnFamily()` and `DB::Open()`. It holds a pointer to `ColumnFamilyData`, the `DBImpl`, and the DB mutex. The destructor unrefs the `ColumnFamilyData`.

### RefedColumnFamilySet

A wrapper around `ColumnFamilySet` that refs each `ColumnFamilyData` during iteration and unrefs it when moving to the next entry. This allows the DB mutex to be released during the loop body without the risk of a concurrent `DropColumnFamily` destroying the current entry.

## Column Family Lifecycle

### Creation

**Step 1 -- Validate options**: Call `ColumnFamilyData::ValidateOptions()` to check the new column family's options against the current `DBOptions`.

**Step 2 -- Create directories**: If `cf_paths` is specified, create the necessary directories.

**Step 3 -- Build VersionEdit**: Construct a `VersionEdit` with `AddColumnFamily(name)`, the next available column family ID, the current WAL number, and the comparator name.

**Step 4 -- LogAndApply**: Acquire the DB mutex and enter the write thread (via `WriteThread::EnterUnbatched()`). Call `VersionSet::LogAndApply()` which atomically writes the creation record to the MANIFEST and creates the `ColumnFamilyData` object in `ColumnFamilySet`.

**Step 5 -- Post-creation setup**: Add data directories, install a SuperVersion via `InstallSuperVersionForConfigChange()`, mark the CF as initialized, and create a `ColumnFamilyHandleImpl`.

**Step 6 -- Persist OPTIONS**: After creating column families, `WrapUpCreateColumnFamilies()` persists the updated options file and seeds the seqno-to-time mapping if needed.

Important: The comparator's name is attached to the column family when it is created and checked on every subsequent `DB::Open()`. If the comparator name changes, the open will fail. Evolving key formats require versioning within the comparator, not renaming it.

### Dropping

**Step 1 -- Validate**: Check that the column family is not the default (ID 0) and not already dropped.

**Step 2 -- Build VersionEdit**: Construct a `VersionEdit` with `DropColumnFamily()`.

**Step 3 -- LogAndApply**: Under the DB mutex and write thread, call `LogAndApply()` to record the drop in the MANIFEST. This triggers `ColumnFamilyData::SetDropped()`.

**Step 4 -- Update state**: Subtract the column family's `write_buffer_size * max_write_buffer_number` from `max_total_in_memory_state_`. Signal `bg_cv_` so waiters (e.g., `Close()`) can detect the state change.

**Step 5 -- Cleanup**: Erase thread status info for the dropped CF.

Important: A dropped column family remains alive as long as references exist. Clients can still read from a dropped column family. Writes will fail unless `WriteOptions::ignore_missing_column_families` is true. Background work (flush, compaction) is no longer scheduled for dropped column families.

`SetDropped()` immediately removes the column family from the `ColumnFamilySet` (via `RemoveColumnFamily()`), so it is no longer visible to iteration or lookup by name. When the reference count eventually reaches zero, the `ColumnFamilyData` destructor removes it from the linked list and frees associated memory.

### Reference Counting

`ColumnFamilyData` uses atomic reference counting:

- `Ref()`: Increment the reference count. Can only be called when the caller can guarantee the object is alive (while holding a non-zero ref, holding the DB mutex, or as the write group leader).
- `UnrefAndTryDelete()`: Decrement and free if zero. Can only be called while holding the DB mutex or during single-threaded recovery.
- `SetDropped()` requires: DB mutex held, in single-threaded write thread, and in single-threaded `LogAndApply()`.

Various entities hold references to `ColumnFamilyData`:
- `ColumnFamilyHandleImpl` (user-held handle)
- `flush_queue_` entries
- `compaction_queue_` entries
- Active `SuperVersion` objects
- Background jobs (flush, compaction)

## WAL Sharing and Retention

All column families share a single write-ahead log. This enables atomic cross-column-family writes via `WriteBatch`. However, WAL sharing has an important consequence for WAL retention:

- Every time any single column family is flushed, a new WAL is created. All subsequent writes to all column families go to the new WAL.
- The old WAL cannot be deleted until all column families that have data in it have been flushed.
- A column family with a very low write rate can hold WAL files open for a long time, preventing their deletion and increasing disk usage and recovery time.

To mitigate WAL retention from slow column families, set `max_total_wal_size` (see `DBOptions` in `include/rocksdb/options.h`). When total WAL size exceeds this threshold, RocksDB forces a flush of the column family with the oldest live data in the WAL.

## Column Family Configuration

Each column family has independent options (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`):

| Option Category | Examples |
|----------------|----------|
| Memtable | `write_buffer_size`, `max_write_buffer_number`, `memtable_factory` |
| Compaction | `compaction_style`, `level0_file_num_compaction_trigger`, `max_bytes_for_level_base` |
| Compression | `compression`, `compression_per_level`, `bottommost_compression` |
| Comparator | `comparator` (immutable after creation) |
| Merge | `merge_operator` |
| Bloom filter | `table_factory` with filter policy |
| CF-specific paths | `cf_paths` |

Options can be changed dynamically via `DB::SetOptions()`, which calls `ColumnFamilyData::SetOptions()` under the DB mutex. Changed options take effect when a new SuperVersion is installed.

## Write Stall Interaction

Each column family independently tracks write stall conditions via `RecalculateWriteStallConditions()`. Trigger thresholds are in `ColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`):

| Condition | Trigger |
|-----------|---------|
| Stop | L0 files >= `level0_stop_writes_trigger` |
| Stop | Unflushed memtables >= `max_write_buffer_number` |
| Stop | Compaction-needed bytes exceed configured threshold |
| Slowdown | L0 files >= `level0_slowdown_writes_trigger` |
| Slowdown | Compaction-needed bytes exceed a lower threshold |

The static method `GetWriteStallConditionAndCause()` computes the condition from current metrics. The result is stored in the SuperVersion's `write_stall_condition` field.

## Atomic Flush

When `atomic_flush` is enabled (see `DBOptions` in `include/rocksdb/options.h`), participating column families are flushed together atomically. This ensures that the flushed state across column families is consistent at a single sequence number boundary. Not every column family is included in each atomic flush -- `SelectColumnFamiliesForAtomicFlush()` selects only CFs with unflushed immutable memtables, non-empty mutable memtables, or non-empty cached recoverable state. Idle CFs are excluded. Key behaviors:

- `SelectColumnFamiliesForAtomicFlush()` identifies which CFs need flushing
- `AssignAtomicFlushSeq()` assigns the same flush cutoff sequence number to all selected CFs
- A single `FlushRequest` covers multiple CFs
- `AtomicFlushMemTablesToOutputFiles()` executes the multi-CF flush

## ColumnFamilyMemTablesImpl

`ColumnFamilyMemTablesImpl` (defined in `db/column_family.h`) provides `WriteBatch` with a way to locate the correct memtable for each column family ID encountered during batch processing. It wraps `ColumnFamilySet` and provides `Seek(column_family_id)` to navigate to the right `ColumnFamilyData` and its active memtable.
