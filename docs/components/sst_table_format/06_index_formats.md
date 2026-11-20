# Index Block Formats

**Files:** `table/block_based/index_builder.h`, `table/block_based/index_builder.cc`, `table/block_based/index_reader_common.h`, `table/block_based/index_reader_common.cc`, `include/rocksdb/table.h`

## Overview

Index blocks map separator keys to `BlockHandle` values that point to data blocks within an SST file. Each index entry corresponds to one data block: the key is a separator that is greater than or equal to the last key in the data block and less than the first key in the next data block. The value is a `BlockHandle` encoding the offset and size of the data block.

RocksDB supports four index types, selected via `BlockBasedTableOptions::index_type` (see `BlockBasedTableOptions` in `include/rocksdb/table.h`):

| Index Type | Enum Value | Description | Builder Class |
|-----------|------------|-------------|---------------|
| Binary Search | `kBinarySearch` | Default. Single index block with sorted separators. | `ShortenedIndexBuilder` |
| Hash Search | `kHashSearch` | Binary search plus prefix hash metadata for faster prefix seeks. | `HashIndexBuilder` |
| Two-Level (Partitioned) | `kTwoLevelIndexSearch` | Multiple index partitions plus a top-level index. Reduces memory for large files. | `PartitionedIndexBuilder` |
| Binary Search With First Key | `kBinarySearchWithFirstKey` | Like binary search, but stores each block's first key. Allows deferring data block reads. | `ShortenedIndexBuilder` (with `include_first_key=true`) |

## Index Builder Architecture

All index builders inherit from the `IndexBuilder` base class in `table/block_based/index_builder.h`. The factory method `IndexBuilder::CreateIndexBuilder()` in `index_builder.cc` instantiates the appropriate builder based on the configured `IndexType`.

### Core Interface

The builder accumulates index entries as data blocks are written. Two modes of operation are supported:

**Sequential mode** (single-threaded): `AddIndexEntry()` is called with the last key of the current data block, the first key of the next data block (or nullptr for the last block), and the `BlockHandle` of the data block just written. The builder computes a separator key and adds the index entry.

**Parallel compression mode** (multi-threaded): The work is split across three methods:
1. `CreatePreparedIndexEntry()` -- allocates a holder for intermediate state
2. `PrepareIndexEntry()` -- called from the "emit thread" to compute the separator key (does not need the BlockHandle yet)
3. `FinishIndexEntry()` -- called from the "write thread" once the BlockHandle is known, completing the index entry

This split enables background compression workers to prepare index entries before the final file offset is determined.

### Index Size Estimation

`CurrentIndexSizeEstimate()` provides a running estimate of the index size, used during table construction to determine when to cut files. Note that not all implementations provide accurate estimates -- `HashIndexBuilder::CurrentIndexSizeEstimate()` returns 0, so the estimate is implementation-dependent.

## Binary Search Index (ShortenedIndexBuilder)

The default index type. `ShortenedIndexBuilder` in `index_builder.h` builds a single index block with sorted separator keys.

### Key Shortening

Rather than storing the full last key of each data block, the builder computes a shorter separator key. The behavior is controlled by `BlockBasedTableOptions::index_shortening` (default `kShortenSeparators`):

| Mode | Behavior |
|------|----------|
| `kShortenSeparators` | Shorten separators between blocks using `Comparator::FindShortestSeparator()`. Last block key uses `FindShortSuccessor()`. |
| `kShortenSeparatorsAndSuccessor` | Also shorten the last block's key using `FindShortSuccessor()`. |
| `kNoShortening` | Use the actual last key in each data block as the separator. Useful with `kBinarySearchWithFirstKey`. |

The shortening logic is in `ShortenedIndexBuilder::FindShortestInternalKeySeparator()` and `FindShortInternalKeySuccessor()` in `index_builder.cc`. The algorithm extracts user keys from the internal keys, calls the comparator's `FindShortestSeparator()`, and if the user key was shortened, appends `kMaxSequenceNumber` to produce a valid internal key.

### Sequence Number Stripping

`ShortenedIndexBuilder` maintains two `BlockBuilder` instances: `index_block_builder_` (with sequence numbers) and `index_block_builder_without_seq_` (user keys only). The atomic flag `must_use_separator_with_seq_` tracks whether any separator requires the sequence number for correctness (which happens when two adjacent data blocks share the same user key but differ only in sequence number).

At `Finish()`, the builder selects the appropriate block builder. If sequence numbers were never needed, the without-seq builder produces a smaller index block. This optimization is signaled by the `index_key_is_user_key` table property (format_version >= 3).

### First Key Support

When `index_type` is `kBinarySearchWithFirstKey`, the constructor sets `include_first_key=true`. The builder records each data block's first internal key via `OnKeyAdded()` and stores it in the `IndexValue` alongside the `BlockHandle`. This enables iterators to defer reading data blocks until the key is actually needed, reducing read amplification for short range scans.

Important: When user-defined timestamps are not persisted, the first internal key is explicitly stripped of its timestamp before being stored in the index value.

## Hash Search Index (HashIndexBuilder)

`HashIndexBuilder` in `index_builder.h` wraps a `ShortenedIndexBuilder` as its primary index and adds two metadata blocks that enable prefix-based hash lookups.

### Metadata Format

The builder collects prefix information via `OnKeyAdded()`, which uses the configured `SliceTransform` (prefix extractor) to compute key prefixes. For each unique prefix, it records:
- The prefix string itself (appended to `prefix_block_`)
- Metadata: prefix length, restart index, and number of data blocks spanned (appended to `prefix_meta_block_`)

At `Finish()`, these are emitted as two meta blocks:
- `kHashIndexPrefixesBlock` -- concatenated prefix strings
- `kHashIndexPrefixesMetadataBlock` -- per-prefix metadata (length, restart index, block count)

The primary index remains a standard binary search index, so the hash metadata is purely supplementary. If the hash lookup fails or is inapplicable, the reader falls back to binary search.

Note: `kHashSearch` requires `index_block_restart_interval == 1` (asserted in `CreateIndexBuilder()`).

## Partitioned (Two-Level) Index (PartitionedIndexBuilder)

For large SST files, `PartitionedIndexBuilder` in `index_builder.h` splits the index into multiple partitions, each containing a subset of index entries, plus a top-level index that maps separator keys to partition `BlockHandle`s.

### On-Disk Layout

```
[index partition 0] [index partition 1] ... [index partition P-1] [top-level index]
```

Each partition is an independent index block built by a `ShortenedIndexBuilder`. The top-level index maps the last separator key of each partition to its `BlockHandle`.

### Partition Sizing

Partition boundaries are determined by a `FlushBlockPolicy` configured with `BlockBasedTableOptions::metadata_block_size` (default 4096 bytes). When the active sub-index builder's estimated size reaches the target, a new partition is started.

Additionally, an external entity (such as the partitioned filter builder) can request a partition cut via `RequestPartitionCut()`. When coupled filter partitioning is used (deprecated behavior), filter and index partitions are aligned.

### Build Workflow

Step 1: As data blocks are added, `AddIndexEntry()` or `FinishIndexEntry()` delegates to the active `sub_index_builder_`. After each entry, `MaybeFlush()` checks the flush policy. If a flush is triggered, the completed partition is saved and a new `ShortenedIndexBuilder` is created via `MakeNewSubIndexBuilder()`.

Step 2: `Finish()` is called repeatedly. Each call returns `Status::Incomplete()` along with the next partition's index block contents. The caller writes the partition to the SST file and provides its `BlockHandle` back to the next `Finish()` call. The final `Finish()` call returns `Status::OK()` with the top-level index block.

Step 3: The top-level index entries are added as each partition's `BlockHandle` becomes known, using the same delta-encoding and sequence-number-stripping logic as the primary index.

### Sequence Number Propagation

The `must_use_separator_with_seq_` flag is propagated from sub-index builders to the top-level. If any partition requires sequence numbers, all partitions are rebuilt with sequence numbers at `Finish()` time. This ensures consistency across the entire index.

### Size Estimation

`PartitionedIndexBuilder::UpdateIndexSizeEstimate()` maintains a running estimate that accounts for completed partition sizes (tracked via `estimated_completed_partitions_size_`), the active partition size, and an estimated top-level index size (~70 bytes per partition entry). A 2x buffer is added for the expected next partition and top-level entry.

## Index Reader Architecture

On the read side, `IndexReaderCommon` in `index_reader_common.h` provides the base class for all index readers. It manages access to the index block, which may be:
- Owned by the reader (pinned in memory)
- Stored in the block cache (looked up on demand)
- Both (cached and pinned)

`GetOrReadIndexBlock()` handles the lookup: if the index block is owned, it returns a reference directly. Otherwise, it reads the block through the cache layer via `ReadIndexBlock()`.

For partitioned indexes, `PartitionIndexReader` in `partitioned_index_reader.h` creates a two-level iterator. If all partitions are pinned (via `CacheDependencies()`), it uses `NewTwoLevelIterator()` with a `partition_map_` for direct access. Otherwise, it creates a `PartitionedIndexIterator` that loads partitions on demand.

### Partition Caching

`PartitionIndexReader::CacheDependencies()` prefetches all index partitions in a single sequential read, then loads each partition into the block cache. If all partitions are successfully cached, they are saved in `partition_map_` (an all-or-nothing map). This enables the fast path of `NewTwoLevelIterator()` which avoids per-partition cache lookups.

## Configuration Reference

| Option | Default | Description |
|--------|---------|-------------|
| `index_type` | `kBinarySearch` | Index block format (see `BlockBasedTableOptions` in `include/rocksdb/table.h`) |
| `index_block_restart_interval` | 1 | Restart interval for index blocks. Increasing to 8-16 can halve index block size but increases CPU for lookups. |
| `index_shortening` | `kShortenSeparators` | Controls separator key shortening behavior |
| `metadata_block_size` | 4096 | Target size for index partitions (with `kTwoLevelIndexSearch`) |
| `format_version` | 7 | Controls index encoding. Version >= 3 strips sequence numbers. Version >= 4 delta-encodes block handles. |

## Format Version Evolution

| Version | Index Change |
|---------|-------------|
| <= 2 | Index keys are full internal keys (`<user_key, seq>`). Values are full `BlockHandle` (`<offset, size>`). |
| 3 (RocksDB 5.15) | Enables stripping sequence numbers from index keys when not needed. The `index_key_is_user_key` table property reflects whether stripping occurred. |
| 4 (RocksDB 5.16) | Delta-encodes `BlockHandle` values. Only the first entry per restart interval stores full `(offset, size)`. Subsequent entries store `Varsignedint64(delta_size)` where `delta_size = current_size - previous_size`. Sets `index_value_is_delta_encoded` table property. |
| 5 (RocksDB 6.6) | Full and partitioned filters use FastLocalBloom on-disk format. XXH3 becomes the default checksum type (independently of format_version). |

Note: Format versions >= 3 are forward-incompatible -- SST files written with these versions cannot be read by older RocksDB releases.
