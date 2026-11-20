# Public API and Interfaces

**Files:** `include/rocksdb/user_defined_index.h`

## Overview

The UDI framework defines four abstract interfaces that implementations must provide: a builder for SST construction, an iterator for reads, a reader that creates iterators, and a factory that creates builders and readers.

## UserDefinedIndexBuilder

The builder interface is called during SST file construction. RocksDB calls it in lockstep with the internal index builder through `UserDefinedIndexBuilderWrapper`.

### AddIndexEntry

Called at each data block boundary to record the separator key and block handle.

The keys are **user keys** (without the 8-byte internal key trailer). The wrapper strips internal key trailers before forwarding.

The UDI must compute a separator between the two user keys and return it. The separator must satisfy:

    last_key_in_current_block <= separator < first_key_in_next_block

in user-key order (ignoring sequence numbers).

The `IndexEntryContext` parameter provides sequence numbers for block boundary keys. These are needed when the same user key spans a data block boundary (see Chapter 4).

**Call order:** `AddIndexEntry` is called **before** `OnKeyAdded` for `first_key_in_next_block`.

### OnKeyAdded

Called for every key-value pair added to the SST file. Range tombstones (`kTypeRangeDeletion`) are never forwarded to UDI builders because `BlockBasedTableBuilder::Add()` routes them to a separate range-deletion meta block before calling the index builder. The wrapper's `MapToUDIValueType` does not handle `kTypeRangeDeletion` and will assert-fail if it encounters one (returning `kOther` in release mode).

UDI builders may override this to collect per-key information (e.g., for secondary indexes). Builders that only use separator keys from `AddIndexEntry()` (e.g., trie-based indexes) can leave this as a no-op.

**Thread safety:** For a given builder instance, `OnKeyAdded()` and `AddIndexEntry()` are always called from a single thread. Builders do not need internal synchronization.

### ValueType Mapping

The wrapper maps internal value types to a simplified UDI enum:

| Internal ValueType | UDI ValueType | Notes |
|-------------------|---------------|-------|
| `kTypeValue`, `kTypeValuePreferredSeqno` | `kValue` | Full user value (preferred seqno suffix stripped) |
| `kTypeDeletion`, `kTypeSingleDeletion`, `kTypeDeletionWithTimestamp` | `kDelete` | Value typically empty |
| `kTypeMerge` | `kMerge` | Merge operand |
| `kTypeBlobIndex`, `kTypeWideColumnEntity` | `kOther` | Value format is type-specific |

See `MapToUDIValueType()` in `table/block_based/user_defined_index_wrapper.h`.

### Finish

Finalizes the index and returns serialized contents via a `Slice` output parameter. The memory backing the Slice must remain valid until the builder is destructed — RocksDB holds a reference, not a copy.

The serialized UDI is stored as a meta block named `"rocksdb.user_defined_index.<factory_name>"` (see `kUserDefinedIndexPrefix` in `include/rocksdb/user_defined_index.h`).

## UserDefinedIndexIterator

The iterator interface is used at read time to navigate index entries.

### Prepare

Accepts an array of `ScanOptions` to prefetch and buffer results for batched scans (MultiScan). Implementations use this to minimize I/O round trips by reading all needed index regions upfront.

### SeekToFirstAndGetResult

Positions at the very first index entry. The default implementation delegates to `SeekAndGetResult` with an empty key, which works for `BytewiseComparator`. Implementations should override this if they can reach the first entry more efficiently or use a comparator where empty is not the smallest key.

### SeekAndGetResult

Seeks to the first index entry whose separator key is >= `target` (a user key). The `SeekContext` provides `target_seq` for snapshot isolation when the same user key spans multiple blocks (see Chapter 4).

The result must set `bound_check_result`:

| Value | Meaning |
|-------|---------|
| `kInbound` | Data block is definitely within scan bounds |
| `kOutOfBound` | No block satisfies the target (iteration stops) |
| `kUnknown` | Partially within bounds or iterator exhausted |

Important: `kOutOfBound` should only be returned when the implementation can **prove** the block is out of bounds. For implementations that store separator keys (upper bounds) rather than first-in-block keys, bounds checking against a limit key requires comparing against the **previous** index key, not the current separator.

The `UserDefinedIndexIteratorWrapper` treats any non-`kInbound` result as invalid for `Valid()` (both `kUnknown` and `kOutOfBound` set `valid_ = false`). However, `UpperBoundCheckResult()` returns the raw enum value, allowing callers to distinguish `kUnknown` (check manually) from `kOutOfBound` (definitely out of bounds).

### NextAndGetResult

Advances to the next index entry and populates the result the same way as `SeekAndGetResult`.

### value

Returns the `BlockHandle` (offset and size) of the current data block.

## UserDefinedIndexReader

A reader owns the deserialized index data and creates iterators.

- `NewIterator(read_options)`: Creates a new `UserDefinedIndexIterator` for scanning
- `ApproximateMemoryUsage()`: Reports memory consumption for monitoring

The `index_block` Slice passed to the reader is typically backed by a block cache entry. It remains valid for the reader's lifetime (tied to table reader lifetime or cache eviction).

## UserDefinedIndexFactory

The factory creates builders (during SST writes) and readers (during SST reads). It extends `Customizable` for string-based registration.

### Two API Layers

**Deprecated pure-virtual API** (required by base class):

- `NewBuilder()`: Returns raw pointer, no comparator
- `NewReader(Slice&)`: Returns unique_ptr, no comparator

**Option-aware virtual API** (preferred):

- `NewBuilder(UserDefinedIndexOption, unique_ptr&)`: Returns Status, receives comparator
- `NewReader(UserDefinedIndexOption, Slice&, unique_ptr&)`: Returns Status, receives comparator

The block-based table builder and reader always call the option-aware overloads. Implementations that need the comparator (e.g., `TrieIndexFactory`) override the option-aware methods and provide abort-stubs for the deprecated pure virtuals.

### UserDefinedIndexOption

Contains the `Comparator*` used by the column family, defaulting to `BytewiseComparator()`. Passed to both builders and readers so implementations can use the correct key ordering. If `nullptr` is passed explicitly, factories default it back to `BytewiseComparator()`.

### CreateFromString

Static method that uses `ObjectRegistry` to create a factory from a string identifier (e.g., `"trie_index"`). Enables configuration via options strings.
