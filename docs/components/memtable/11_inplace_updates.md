# In-Place Updates

**Files:** `db/memtable.cc`, `db/memtable.h`, `include/rocksdb/options.h`

## Overview

RocksDB supports two forms of in-place value modification in the memtable, both requiring `inplace_update_support = true` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`). These features trade snapshot isolation for reduced memory usage by modifying existing entries rather than appending new ones.

Important: In-place updates disable snapshot support (`IsSnapshotSupported()` returns false) because modifying an existing entry destroys the previous version visible to snapshots.

## Update()

`MemTable::Update()` attempts to modify an existing entry in-place when:

1. The user key already exists in the memtable
2. The existing entry has the same `ValueType` as the new entry
3. The new value size is less than or equal to the existing value size

### Flow

Step 1: Seek to the key using `table_->GetDynamicPrefixIterator()`.

Step 2: Check if the found entry matches the user key and has the same value type.

Step 3: If the new value fits in the existing slot, acquire a `WriteLock` on the key and overwrite the value bytes. The varint-encoded value length is also updated.

Step 4: If the key does not exist or in-place update is not possible (value too large or type mismatch), fall back to `Add()` which appends a new entry.

Note: The sequence number of the in-place-updated entry retains its original value -- the new sequence number is discarded. Checksum is recalculated with the old sequence number and new value.

## UpdateCallback()

`MemTable::UpdateCallback()` applies a delta update to an existing value using the configured `inplace_callback` function (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`).

### Flow

Step 1: Seek to the key. The existing entry must have type `kTypeValue`.

Step 2: Acquire a `WriteLock` on the key and call the `inplace_callback` with the existing value buffer, the delta, and an output string.

Step 3: Dispatch on the callback return value:

| Return Value | Action |
|-------------|--------|
| `UPDATED_INPLACE` | Value was modified in the existing buffer. Update varint length if needed. |
| `UPDATED` | A new value was produced. Call `Add()` to insert it as a new entry. |
| `UPDATE_FAILED` | No update performed. Return `Status::OK()`. |

Step 4: If the key does not exist or has a non-`kTypeValue` type, return `Status::NotFound()`.

## RW Lock Striping

In-place updates and reads of in-place-updatable entries are protected by a striped RW lock array in `MemTable::locks_`. The lock for a key is determined by hashing the key to a lock index via `GetSliceRangedNPHash()`.

- **Writers** (`Update()`, `UpdateCallback()`) acquire a `WriteLock`
- **Readers** (`SaveValue` callback in `Get()`) acquire a `ReadLock` when `inplace_update_support` is true
- The number of locks is controlled by `inplace_update_num_locks` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`)

When `inplace_update_support` is false, the locks vector is empty and no locking occurs during reads.

## Iterator Value Pinning

When `inplace_update_support` is true, `MemTableIterator` sets `value_pinned_ = false` because values may be modified concurrently. This means iterator values must be copied rather than pinned (referenced without copying).

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `inplace_update_support` | false | Enable in-place update (disables snapshots) |
| `inplace_update_num_locks` | 10000 | Number of striped RW locks for concurrent access |
| `inplace_callback` | nullptr | Callback function for delta updates via `UpdateCallback()` |

## Limitations

- Snapshots are not supported when `inplace_update_support` is true
- In-place update only works when the new value is no larger than the old value
- `UpdateCallback()` only works on entries with `kTypeValue` type
- Concurrent memtable writes are incompatible with in-place updates. `CheckConcurrentWritesSupported()` rejects `inplace_update_support = true` when `allow_concurrent_memtable_write` is enabled, returning `Status::InvalidArgument`.
- `max_successive_merges` optimization (collapsing consecutive merge operands) interacts with the memtable but is separate from in-place updates
