# Compaction and Lifecycle

**Files:** `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`

## Index Rebuilding During Compaction

UDI indexes are **rebuilt from scratch** for every output SST file during compaction:

Step 1 -- Input SSTs are read using the **internal index** (not UDI), regardless of whether they contain UDI blocks.

Step 2 -- The compaction merging iterator deduplicates keys and drops obsolete versions based on snapshot boundaries.

Step 3 -- Output SST files are built with a new UDI (if `user_defined_index_factory` is configured in current options). The block boundaries may differ from input SSTs due to merging, deduplication, and block size thresholds.

Step 4 -- When input SSTs are deleted, their UDI blocks are discarded.

Important: UDI indexes are **not merged or copied** between SST files. Each SST's UDI is an independent encoding of its data block boundaries.

## Flush (Memtable to L0)

When flushing a memtable to L0:

Step 1 -- `BlockBasedTableBuilder` is created with `user_defined_index_factory` if configured.

Step 2 -- The memtable iterator yields keys in internal key order (user key ascending, sequence number descending).

Step 3 -- For each key, the wrapper forwards `(user_key, type, value)` to the UDI builder via `OnKeyAdded`.

Step 4 -- When the data block size threshold is reached, `AddIndexEntry` is called with the block boundary keys and their sequence numbers.

Step 5 -- Both the internal index and UDI are finalized and written to the SST file.

Note: During flush, the same user key may appear multiple times if snapshots are active. UDI builders that use `OnKeyAdded()` must be prepared for duplicate user keys.

## L0 to L1+ Compaction

Compaction merges multiple SST files:

Step 1 -- A merging iterator reads keys from input SSTs using their **internal indexes**.

Step 2 -- Compaction drops old versions and tombstones based on active snapshots.

Step 3 -- New SST files are created with fresh UDI indexes reflecting the post-compaction key distribution.

The output SST's UDI reflects the post-compaction key distribution, not the input SSTs' distributions. Block boundaries are determined by the current block size threshold and flush policy, not by input block boundaries.

## Configuration Evolution

### Adding UDI

Setting `user_defined_index_factory` on a database that previously had none is safe:

- New flushes and compactions build UDI blocks
- Old SST files without UDI are read using the internal index
- The `SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT` ticker tracks how many old files are encountered
- Full compaction eventually rebuilds all files with UDI

### Removing UDI

Removing `user_defined_index_factory` from options is safe:

- New flushes and compactions omit UDI blocks
- Old SST files with UDI blocks are still readable (the UDI block is simply not loaded)
- No migration steps needed

### Changing UDI Type

Switching from one UDI factory to another requires care:

- Old SST files have UDI blocks with the old factory name (e.g., `"rocksdb.user_defined_index.trie_index"`)
- The new factory expects blocks with its own name (e.g., `"rocksdb.user_defined_index.hash_index"`)
- Name mismatch causes `FindMetaBlock` to fail, triggering the missing-UDI handling path

Safe migration: keep `fail_if_no_udi_on_open = false` during transition, let compaction rebuild all files, then optionally enable strict mode.

## Interaction with Other Components

### Partitioned Internal Index

UDI works with partitioned (two-level) internal indexes (`BlockBasedTableOptions::kTwoLevelIndexSearch`). The UDI wraps the top-level partitioned index builder/reader. The UDI block itself is always monolithic — only the internal index is partitioned.

### Block Cache

The UDI block participates in the block cache as `BlockType::kUserDefinedIndex`. It is loaded via `RetrieveBlock()` and stored in `rep_->udi_block` as a `CachableEntry<Block_kUserDefinedIndex>`. Cache eviction policies apply normally.

### Block Tracing

UDI block accesses are traced as `TraceType::kBlockTraceIndexBlock`, sharing the same trace type as internal index blocks.
