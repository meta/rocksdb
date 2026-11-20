# Block Cache Keys

**Files:** `cache/cache_key.h`, `cache/cache_key.cc`, `table/block_based/block_cache.h`

## CacheKey

`CacheKey` is a fixed 16-byte structure used as the key for all block cache entries. It consists of two 64-bit fields: `file_num_etc64_` and `offset_etc64_`.

Special factory methods:
- `CreateUniqueForCacheLifetime(cache)` -- uses `Cache::NewId()` for unique reservation keys
- `CreateUniqueForProcessLifetime()` -- uses a process-wide atomic counter (e.g., for `CacheEntryStatsCollector`)

An "empty" `CacheKey` (all zeros) is never returned by normal operations and serves as a sentinel.

## OffsetableCacheKey

`OffsetableCacheKey` generates per-file cache keys from `(db_id, db_session_id, file_number)`. Individual block keys within a file are derived by XOR-ing the file's `offset_etc64_` with the block offset, producing a `CacheKey` with `(file_num_etc64_, offset_etc64_ ^ block_offset)`.

### Properties

- **Stable across open/close**: because the inputs come from table properties that persist in the SST file, SST-derived cache keys survive DB restart, backup/restore, and import/export. Note: keys from `CreateUniqueForCacheLifetime()` and `CreateUniqueForProcessLifetime()` are deliberately ephemeral and not restart-stable.
- **Globally unique with very high probability**: uniqueness is guaranteed for files generated within a single process lifetime (by construction). Cross-process uniqueness relies on the randomness of `db_session_id`.
- **Fast hot-path operation**: computing a block's cache key is a single XOR
- **8-byte common prefix**: `file_num_etc64_` is shared by all blocks in a file, enabling efficient prefix-based operations

### Construction

`OffsetableCacheKey` can be constructed from:
- Direct inputs: `(db_id, db_session_id, file_number)`
- SST unique ID: `FromInternalUniqueId(UniqueIdPtr)` -- useful for deriving cache keys from manifest data before reading the file

Both paths produce identical cache keys for the same file.

## Block Type Wrappers

`block_cache.h` defines thin wrapper classes over `Block` that add compile-time `CacheEntryRole` and `BlockType` associations:

| Wrapper Class | CacheEntryRole | BlockType |
|---------------|---------------|-----------|
| `Block_kData` | `kDataBlock` | `kData` |
| `Block_kIndex` | `kIndexBlock` | `kIndex` |
| `Block_kFilterPartitionIndex` | `kFilterMetaBlock` | `kFilterPartitionIndex` |
| `ParsedFullFilterBlock` | `kFilterBlock` | `kFilterFull` |
| `Block_kRangeDeletion` | `kOtherBlock` | `kRangeDeletion` |
| `Block_kUserDefinedIndex` | `kIndexBlock` | `kUserDefinedIndex` |
| `DecompressorDict` | `kOtherBlock` | `kCompressionDictionary` |

Note: meta-index blocks (`kMetaIndex`) are not currently cached in the block cache -- their helper entries are explicitly `nullptr` in the helper arrays.

These wrappers enable `FullTypedCacheInterface<TBlocklike, BlockCreateContext>` (aliased as `BlockCacheInterface<T>`) to automatically select the correct `CacheItemHelper` based on the block type.

## BlockCreateContext

`BlockCreateContext` (extends `Cache::CreateContext`) carries the parameters needed to reconstruct a block from secondary cache data:

| Field | Purpose |
|-------|---------|
| `table_options` | Block-based table options (restart intervals, etc.) |
| `decompressor` | For decompressing blocks retrieved from secondary cache |
| `protection_bytes_per_key` | Key-value integrity protection level |
| `data_block_restart_interval` | From table properties, for block reconstruction |
| `index_block_restart_interval` | From table properties, for index block reconstruction |
| `raw_ucmp` | User comparator for prefix operations |

The `Create()` template method handles decompression (if needed) and constructs the appropriate block type.

## Key Design Rationale

The 16-byte fixed-size key format was chosen for several reasons:
- HyperClockCache requires fixed-size keys for its open-addressing hash table
- The XOR-based offset derivation is extremely fast (single instruction on the hot read path)
- The `(db_id, db_session_id, file_number)` basis ensures keys are stable across operations that preserve table properties
- The 8-byte common prefix enables efficient shard distribution

Note: The key format is endianness-dependent and not intended for cross-platform persistence.
