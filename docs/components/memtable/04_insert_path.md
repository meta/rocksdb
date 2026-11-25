# Insert Path

**Files:** `db/memtable.cc`, `db/memtable.h`, `db/dbformat.h`

## Entry Encoding Format

Every key-value pair is encoded into a self-contained binary format before insertion into the memtable representation:

```
varint32(internal_key_size)    -- internal_key_size = user_key.size() + 8
user_key bytes                 -- raw user key
uint64 packed(seq, type)       -- sequence number (56 bits) | ValueType (8 bits)
varint32(value_size)
value bytes
[checksum]                     -- 0, 1, 2, 4, or 8 bytes per protection_bytes_per_key
```

The `PackSequenceAndType()` function in `db/dbformat.h` packs the sequence number and value type into a single 64-bit integer: `(seq << 8) | type`.

## ValueType Encoding

The `ValueType` enum in `db/dbformat.h` defines the operation type stored in each entry:

| ValueType | Value | Operation |
|-----------|-------|-----------|
| `kTypeDeletion` | 0x0 | Delete |
| `kTypeValue` | 0x1 | Put |
| `kTypeMerge` | 0x2 | Merge |
| `kTypeSingleDeletion` | 0x7 | SingleDelete |
| `kTypeRangeDeletion` | 0xF | DeleteRange |
| `kTypeDeletionWithTimestamp` | 0x14 | Delete with user-defined timestamp |
| `kTypeWideColumnEntity` | 0x16 | PutEntity (wide columns) |
| `kTypeValuePreferredSeqno` | 0x18 | Value with write-time information |

## MemTable::Add() Flow

`MemTable::Add()` is the core insertion method, called from the write path after WAL logging.

### Step 1: Encode the Entry

Compute the total encoded length including varint headers, key, value, and optional checksum bytes. Allocate space from the appropriate table (point entries go to `table_`, range deletions go to `range_del_table_`).

### Step 2: Write Entry Data

Write the encoded entry into the allocated buffer: varint-encoded key length, user key, packed sequence and type, varint-encoded value length, value bytes, and optional checksum.

### Step 3: Verify Checksum (if enabled)

If `kv_prot_info` is provided, call `VerifyEncodedEntry()` to validate the checksum immediately after encoding.

### Step 4: Insert into Representation

The insertion method depends on the concurrency mode:

**Sequential mode** (`allow_concurrent = false`):

1. If `insert_with_hint_prefix_extractor_` is configured and the key is in its domain, call `InsertKeyWithHint()` using the per-prefix hint from `insert_hints_`
2. Otherwise, call `InsertKey()`
3. Update metadata (counters) using relaxed stores -- safe because single-writer mode provides external synchronization
4. Update bloom filter using `Add()` (non-concurrent variant)
5. Update `first_seqno_` and `earliest_seqno_` if this is the first entry
6. Call `UpdateFlushState()` to check if flush should be triggered

**Concurrent mode** (`allow_concurrent = true`):

1. Call `InsertKeyConcurrently()` or `InsertKeyWithHintConcurrently()`
2. Accumulate metadata changes in the caller-provided `MemTablePostProcessInfo` (data_size, num_entries, num_deletes) -- deferred to avoid atomic contention during insertion. Note: only `kTypeDeletion` increments `num_deletes` in the concurrent path; `kTypeSingleDeletion` and `kTypeDeletionWithTimestamp` are not counted. The sequential path counts all three deletion types. This asymmetry means `NumDeletion()` may undercount in concurrent mode.
3. Update bloom filter using `AddConcurrently()` (lock-free variant)
4. Update `first_seqno_` and `earliest_seqno_` using CAS loops

### Step 5: Handle Range Deletions

If the entry is a range deletion (`kTypeRangeDeletion`), invalidate the per-core cached fragmented range tombstone list by creating a new `FragmentedRangeTombstoneListCache` and storing it into each core's slot. In concurrent mode, this is protected by `range_del_mutex_`.

### Step 6: Return

Return `Status::OK()` on success, or `Status::TryAgain("key+seq exists")` if the representation detected a duplicate.

## Sequential vs Concurrent Insert Comparison

| Aspect | Sequential | Concurrent |
|--------|------------|------------|
| Insert method | `InsertKey()` / `InsertKeyWithHint()` | `InsertKeyConcurrently()` / `InsertKeyWithHintConcurrently()` |
| Metadata update | Immediate relaxed store | Deferred via `MemTablePostProcessInfo` |
| Bloom filter | `Add()` | `AddConcurrently()` |
| Sequence tracking | Direct store | CAS loop |
| External sync | Write thread lock | None required |

## Update() -- In-Place Value Update

`MemTable::Update()` attempts to update an existing entry in-place if the new value is no larger than the old value and the value type matches. It seeks to the key, checks if the user key matches, and if the existing value is large enough, overwrites the value bytes under a per-key `WriteLock`. If in-place update is not possible, falls back to `Add()`.

## UpdateCallback() -- Delta Update

`MemTable::UpdateCallback()` uses the configured `inplace_callback` to apply a delta update to an existing value. The callback receives the existing value buffer and may modify it in place (`UPDATED_INPLACE`), produce a new value (`UPDATED`), or decline to update (`UPDATE_FAILED`). Only works when the existing entry has type `kTypeValue`.

## Duplicate Key Handling

When `MemTableRepFactory::CanHandleDuplicatedKey()` returns true (as with `SkipListFactory`), the representation detects duplicate `(key, seq)` pairs and returns false from `InsertKey*()`. In this case, `MemTable::Add()` returns `Status::TryAgain`, and the caller (in the write path) retries with the next available sequence number.
