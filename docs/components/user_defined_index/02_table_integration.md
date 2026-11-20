# Integration with Block-Based Table

**Files:** `table/block_based/user_defined_index_wrapper.h`, `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`, `table/block_based/block_type.h`, `table/block_based/block_cache.cc`

## Architecture

UDI integrates with the block-based table through three wrapper classes that delegate to both the internal index and the UDI:

| Wrapper | Wraps | Purpose |
|---------|-------|---------|
| `UserDefinedIndexBuilderWrapper` | `IndexBuilder` | Forwards `AddIndexEntry`/`OnKeyAdded` to both internal and UDI builders |
| `UserDefinedIndexIteratorWrapper` | `InternalIteratorBase<IndexValue>` | Adapts UDI iterator to RocksDB's internal iterator interface |
| `UserDefinedIndexReaderWrapper` | `BlockBasedTable::IndexReader` | Routes `NewIterator` calls to UDI or internal reader based on `ReadOptions` |

## Write Path

### Step 1: Wrapper Creation

During `BlockBasedTableBuilder::Rep` initialization, if `BlockBasedTableOptions::user_defined_index_factory` is set:

1. Validate parallel compression is not configured (fails with `InvalidArgument` if so)
2. Create a `UserDefinedIndexBuilder` via the factory's `NewBuilder(option, builder)` method, passing the user comparator
3. If the builder is non-null, wrap the internal `IndexBuilder` with `UserDefinedIndexBuilderWrapper`

If `NewBuilder` returns `Status::OK()` with `builder == nullptr`, no UDI is built for this SST file — the internal index builder remains unwrapped.

### Step 2: Key-Value Addition

For each key-value pair added via `BlockBasedTableBuilder::Add()`:

1. The wrapper's `OnKeyAdded` is called
2. It always forwards to `internal_index_builder_->OnKeyAdded()` first (the internal builder relies on this for state like `current_block_first_internal_key_`)
3. It parses the internal key to extract user key and type
4. For `kTypeValuePreferredSeqno`, it strips the packed preferred seqno suffix so the UDI receives only the user value
5. It maps the internal type to a UDI `ValueType` and forwards to `user_defined_index_builder_->OnKeyAdded()`

### Step 3: Block Boundary

When a data block is flushed, `AddIndexEntry` is called:

1. Parse internal keys to extract user keys and sequence numbers
2. Build an `IndexEntryContext` with sequence numbers
3. Forward user keys, block handle, and context to UDI builder
4. Forward internal keys and block handle to internal builder

Since `AddIndexEntry` returns a `Slice` (not a `Status`), errors during key parsing are cached in `status_` and returned during `Finish()`.

### Step 4: Finalization

During `Finish()`:

1. Call `user_defined_index_builder_->Finish()` to get serialized UDI contents (guarded by `udi_finished_` flag -- only called once even if the wrapper's `Finish()` is invoked multiple times, as happens with partitioned indexes)
2. Insert the UDI as a meta block with name `"rocksdb.user_defined_index.<name>"`  and type `BlockType::kUserDefinedIndex`
3. Call `internal_index_builder_->Finish()` to finalize the internal index (called unconditionally each time)

The UDI block appears in the SST file before the metaindex block and footer.

## Read Path

### Step 1: Meta Block Discovery

During `BlockBasedTable::Open()`, if `BlockBasedTableOptions::user_defined_index_factory` is set:

1. Construct the expected meta block name: `kUserDefinedIndexPrefix + factory->Name()`
2. Search the metaindex for this block via `FindMetaBlock()`

### Step 2: Missing UDI Handling

If the UDI meta block is not found:

- Increment `SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT` ticker
- If `fail_if_no_udi_on_open == true`: return `Status::Corruption` (hard error)
- If `fail_if_no_udi_on_open == false` (default): log a warning and continue (allows reading old SST files)

### Step 3: Block Loading

If the UDI block handle has non-zero size:

1. Load the block via `RetrieveBlock()` into `rep_->udi_block` (a `CachableEntry<Block_kUserDefinedIndex>`)
2. The block participates in the standard block cache with type `BlockType::kUserDefinedIndex`
3. Block tracing reports it as `TraceType::kBlockTraceIndexBlock`

### Step 4: Reader Creation

1. Create a `UserDefinedIndexReader` via `factory->NewReader(option, block_data, reader)`
2. If the reader is non-null, wrap the internal index reader with `UserDefinedIndexReaderWrapper`
3. If `NewReader` returns OK with `reader == nullptr`, this is treated as corruption

### Step 5: Iterator Routing

When `NewIterator()` is called on the reader wrapper:

- If `ReadOptions::table_index_factory` is set and its `Name()` matches the UDI name: create a `UserDefinedIndexIteratorWrapper` from the UDI reader
- If names don't match: return an error iterator
- If `table_index_factory` is null: delegate to the internal index reader

## Internal Key Conversion

The `UserDefinedIndexIteratorWrapper` converts between UDI's user-key world and RocksDB's internal-key world:

**Seek direction (internal -> UDI):** Parses the internal key target, extracts the user key and sequence number, calls `SeekAndGetResult` with user key and `SeekContext{target_seq}`.

**Result direction (UDI -> internal):** Constructs an internal key from the UDI's user-key separator using `InternalKey::Set(user_key, 0, kTypeValue)`. The sequence number 0 ensures the synthetic internal key compares as "greater than or equal to" any real data key with the same user key, which provides correct upper-bound semantics for an index separator.

## Empty and Nullable UDI Cases

| Scenario | Behavior |
|----------|----------|
| `NewBuilder` returns OK with null builder | No UDI built; internal index unwrapped |
| `NewReader` returns OK with null reader | Treated as corruption; returns error |
| UDI block handle has `size() == 0` | Treated as "no UDI"; skipped even if `fail_if_no_udi_on_open` is true |
| UDI name mismatch at read time | Error iterator returned with `InvalidArgument` |
