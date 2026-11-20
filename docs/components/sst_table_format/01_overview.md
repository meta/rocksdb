# Table Format Overview

**Files:** `table/format.h`, `table/format.cc`, `include/rocksdb/table.h`, `table/block_based/block_based_table_factory.cc`

## Table Types

RocksDB supports three SST file formats, each identified by a unique 8-byte magic number at the end of the file:

| Format | Magic Number Source | Primary Use Case |
|--------|-------------------|-----------------|
| Block-based table | SHA1 of `rocksdb.table.block_based` | Default. General-purpose, optimized for disk/flash storage. |
| Plain table | SHA1 of `rocksdb.table.plain` | Low-latency lookups on pure-memory or very low-latency media. |
| Cuckoo table | SHA1 of `rocksdb.table.cuckoo` | Hash-based lookups with high space efficiency. |

The block-based table is the standard format and the focus of this documentation. It organizes data into fixed-size blocks with prefix-compressed keys, supports bloom/ribbon filters for fast negative lookups, and provides a hierarchical index for efficient seeking.

## Table Factory

`BlockBasedTableFactory` (see `table/block_based/block_based_table_factory.cc`) is the factory that creates table builders and readers for block-based tables. It is configured via `BlockBasedTableOptions` (see `include/rocksdb/table.h`).

Key responsibilities:
- Create `BlockBasedTableBuilder` instances for writing new SST files
- Create `BlockBasedTable` (reader) instances for reading SST files
- Manage `TailPrefetchStats` to adaptively optimize tail prefetching during table open

### TailPrefetchStats

`TailPrefetchStats` tracks the effective size of data read from the tail of recently opened SST files. It maintains a circular buffer of up to 32 recorded sizes. When opening a new SST file, `GetSuggestedPrefetchSize()` computes the maximum historical prefetch size that wastes no more than 12.5% (1/8) of total read bytes, capped at 512KB. This adaptive approach avoids both under-prefetching (multiple I/Os to read footer + meta blocks) and over-prefetching (wasting bandwidth on large prefetches).

## Format Versions

The `format_version` field in `BlockBasedTableOptions` controls the on-disk encoding format. It is stored in the SST footer and determines how various structures are encoded.

| Version | Key Features |
|---------|-------------|
| 2 | Minimum supported for read and write. Encodes index values using `IndexValue` with delta-encoded `BlockHandle`. |
| 3 | Optimized encoding for block-based filter (now obsolete). |
| 4 | Index blocks omit `value_length` varint since index values are self-describing (see `DecodeKeyV4()` in `table/block_based/block_util.h`). |
| 5 | Full and partitioned filters use a different on-disk format with improved memory utilization (FastLocalBloom). Legacy bloom and block-based filter deprecated. |
| 6 | Context-aware checksums. Footer includes its own checksum, `base_context_checksum`, and `metaindex_size`. Index handle moves from footer to metaindex block. |
| 7 | `TableProperties::compression_name` format changed for `CompressionManager` compatibility. |

The current latest version is 7 (see `kLatestBbtFormatVersion` in `table/format.h`).

### Version Support Policy

Format versions follow a deprecation protocol with two thresholds:

- **Write minimum** (`kMinSupportedBbtFormatVersionForWrite`): Oldest version allowed for creating new SST files. Currently 2.
- **Read minimum** (`kMinSupportedBbtFormatVersionForRead`): Oldest version that can be read. Currently 2.

When phasing out old versions, the write minimum is increased first. After at least 6 months, the read minimum is increased and the old implementation code can be removed.

### Version-Gated Features

Several helper functions in `table/format.h` gate behavior on format version:

| Function | Condition | Effect |
|----------|-----------|--------|
| `FormatVersionUsesContextChecksum()` | version >= 6 | Enables context-aware checksums using `base_context_checksum` |
| `FormatVersionUsesIndexHandleInFooter()` | version < 6 | Stores index block handle in footer (legacy); version >= 6 moves it to metaindex |
| `FormatVersionUsesCompressionManagerName()` | version >= 7 | Uses `CompressionManager` name format in table properties |

## Checksum Types

RocksDB supports multiple checksum algorithms for verifying block integrity, defined as `ChecksumType` in `include/rocksdb/table.h`:

| Value | Name | Notes |
|-------|------|-------|
| 0x0 | `kNoChecksum` | No verification |
| 0x1 | `kCRC32c` | CRC32c (hardware-accelerated on many platforms) |
| 0x2 | `kxxHash` | xxHash 32-bit |
| 0x3 | `kxxHash64` | xxHash 64-bit (truncated to 32 bits for storage) |
| 0x4 | `kXXH3` | XXH3 (current default). Supported since RocksDB 6.27. |

The default for new tables is `kXXH3` (see `BlockBasedTableOptions::checksum` in `include/rocksdb/table.h`).

### Context-Aware Checksums (format_version >= 6)

Standard checksums detect random corruption but may not detect misplaced data (e.g., blocks from another file or blocks at the wrong offset). Format version 6 introduces context-aware checksums via `ChecksumModifierForContext()` in `table/format.h`.

Each SST file has a `base_context_checksum` stored in the footer (a per-file semi-random value generated at write time). For each block, a modifier is computed from `base_context_checksum` and the block's offset, then added to the standard checksum. This ensures that blocks from different files or at different offsets produce different checksums, detecting misplaced data with high probability.

The modifier computation is designed to be:
- Fast (no branches on the hot path; uses a bitmask to disable when `base_context_checksum == 0`)
- Unique for nearby offsets (lower 32 bits of offset guarantee uniqueness within 4GB)
- Non-linearly correlated (XOR mixing prevents systematic collision patterns)

## Key Configuration Options

The most important `BlockBasedTableOptions` fields for table format behavior:

| Option | Default | Description |
|--------|---------|-------------|
| `block_size` | 4096 | Target uncompressed data block size in bytes |
| `block_size_deviation` | 10 | Percentage threshold for closing a block early |
| `block_restart_interval` | 16 | Keys between restart points for delta encoding |
| `index_block_restart_interval` | 1 | Restart interval for index blocks |
| `format_version` | 7 | On-disk format version |
| `checksum` | `kXXH3` | Checksum algorithm for block verification |
| `index_type` | `kBinarySearch` | Index block structure (binary search, hash, partitioned, or binary search with first key) |
| `data_block_index_type` | `kDataBlockBinarySearch` | Data block index (binary search or binary+hash) |
| `metadata_block_size` | 4096 | Target size for partitioned index/filter blocks |
| `cache_index_and_filter_blocks` | false | Store index/filter in block cache vs. table reader memory |
| `no_block_cache` | false | Disable block cache entirely |
