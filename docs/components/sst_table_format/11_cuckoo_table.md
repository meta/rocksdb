# CuckooTable Format

**Files:** `table/cuckoo/cuckoo_table_factory.h`, `table/cuckoo/cuckoo_table_builder.h`, `table/cuckoo/cuckoo_table_builder.cc`, `table/cuckoo/cuckoo_table_reader.h`, `table/cuckoo/cuckoo_table_reader.cc`, `include/rocksdb/table.h`

## Overview

CuckooTable is an SST file format optimized for fast point lookups using cuckoo hashing. Data is stored in a flat hash table where each bucket contains a fixed-size key-value pair. Lookups require checking at most `num_hash_func` x `cuckoo_block_size` locations, making them O(1) with excellent cache locality.

CuckooTable is designed for read-heavy workloads with fixed-length keys and values. It trades space efficiency and write flexibility for extremely fast point lookups. Compared to PlainTable (which targets sub-microsecond latency with prefix iteration support), CuckooTable targets higher raw point-lookup throughput by reducing memory accesses per lookup to 1-2.

## Configuration Options

CuckooTable is configured via `CuckooTableOptions` (see `include/rocksdb/table.h`).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `hash_table_ratio` | `double` | 0.9 | Hash table utilization. Smaller values mean more empty buckets and fewer collisions at the cost of larger files. |
| `max_search_depth` | `uint32_t` | 100 | Maximum BFS depth when displacing elements during hash table construction. Higher values improve build success rate but increase build time. |
| `cuckoo_block_size` | `uint32_t` | 5 | Number of consecutive buckets to check per hash function during lookup. Improves cache locality for lookups. Minimum is 1. |
| `identity_as_first_hash` | `bool` | false | When true, treats the first 8 bytes of the user key as the hash value for the first hash function. Useful for integer keys. |
| `use_module_hash` | `bool` | true | When true, uses modulo for hash-to-bucket mapping (better space efficiency). When false, uses bitwise AND (requires power-of-two table size, but faster). |

## File Layout

CuckooTable files consist of a flat hash table followed by metadata:

```
[bucket 0: key + value]
[bucket 1: key + value]
...
[bucket T-1: key + value]
[properties block]
[metaindex block]
[Footer]
```

where `T = hash_table_size + cuckoo_block_size - 1`. Each bucket has a fixed size of `key_size + value_size` bytes. Empty buckets are filled with an unused key (a key outside the range of all keys in the file).

The footer uses `kCuckooTableMagicNumber` (`0x926789d0c5f17873`) with format version 1 and `kNoChecksum`.

## Hash Function Selection

CuckooTable uses multiple hash functions based on MurmurHash (see `CuckooHash()` in `table/cuckoo/cuckoo_table_factory.h`).

The hash function for index `hash_cnt` computes:
- If `hash_cnt == 0` and `identity_as_first_hash` is true: the first 8 bytes of the user key are used directly as the hash value.
- Otherwise: `MurmurHash(user_key, seed)` where `seed = kCuckooMurmurSeedMultiplier * hash_cnt` and `kCuckooMurmurSeedMultiplier = 816922183`.

The hash value is mapped to a bucket index using:
- Modulo: `hash_value % table_size` (when `use_module_hash=true`)
- Bitwise AND: `hash_value & (table_size - 1)` (when `use_module_hash=false`, requires power-of-two table size)

## Build Workflow

The builder (`CuckooTableBuilder` in `table/cuckoo/cuckoo_table_builder.h`) constructs the hash table in memory, then writes it to disk.

Step 1 -- **Accumulate entries**: During `Add()`, `kTypeValue` entries are appended to an in-memory buffer (`kvs_`). Deleted keys are stored separately in `deleted_keys_`. The builder validates that all keys have the same size and all non-deletion values have the same size.

Step 2 -- **Determine last-level optimization**: If the first key has sequence number 0, the file is treated as a last-level file. In this case, internal key metadata (sequence number + type, 8 bytes) is stripped, and only user keys are stored.

Step 3 -- **Build hash table** (`MakeHashTable()`): For each entry, compute hash values using all hash functions and try to place it in an empty bucket. If all candidate buckets are occupied, invoke `MakeSpaceForKey()`.

Step 4 -- **Collision resolution** (`MakeSpaceForKey()`): Uses BFS to find a chain of displacements that frees a bucket. The search explores buckets by computing alternative hash positions for currently placed elements, up to `max_search_depth` levels deep. If BFS fails, a new hash function is added (up to `max_num_hash_func`) and the search continues without rehashing existing entries.

Step 5 -- **Determine unused key**: An unused key is computed by decrementing the smallest key or incrementing the largest key byte-by-byte until a key outside the range `[smallest, largest]` is found. This key fills empty buckets so the reader can distinguish empty from occupied slots.

Step 6 -- **Write to file**: Buckets are written sequentially. Empty buckets get the unused key. Occupied buckets get the actual key + value. After the hash table, property block, metaindex block, and footer are appended.

## Read Path

The reader (`CuckooTableReader` in `table/cuckoo/cuckoo_table_reader.h`) requires mmap mode (`allow_mmap_reads=true`). The entire file is memory-mapped at open time.

### Point Lookup

`CuckooTableReader::Get()` performs:

Step 1: For each hash function (0 to `num_hash_func - 1`):
  - Compute the bucket offset using `CuckooHash()`.
  - Check `cuckoo_block_size` consecutive buckets starting from that offset.
  - If the bucket contains the unused key, stop (key not found).
  - If the bucket's user key matches the target, extract the value and return.

Step 2: If no match is found across all hash functions, the key does not exist.

### Cache-Friendly Design

CuckooTable's `cuckoo_block_size` is designed so that a cuckoo block (consecutive buckets checked per hash function) fits within a CPU cache line. This is the key optimization: rather than jumping to random memory locations for each hash function, the reader checks consecutive memory within a single cache line. With the default `cuckoo_block_size=5` and typical key-value sizes, most lookups are served from the first cuckoo block, reducing the typical number of cache misses to 1-2.

`CuckooTableReader::Prepare()` prefetches the first cuckoo block (for hash function 0) into CPU cache using `PREFETCH` intrinsics. The prefetch covers cache lines spanning the cuckoo block, aligned to `CACHE_LINE_SIZE` boundaries. Since the majority of keys are found in the first cuckoo block, prefetching only this block provides the best cost-benefit ratio.

### Iterator

`CuckooTableIterator` supports ordered iteration by sorting all occupied bucket indices by user key at initialization time (`InitIfNeeded()`). This sort is deferred until the first seek or scan operation. The iterator supports `Seek()`, `SeekToFirst()`, `SeekToLast()`, `Next()`, and `Prev()`, but `SeekForPrev()` is not supported.

For last-level files, the iterator reconstructs internal keys by wrapping user keys with sequence number 0 and `kTypeValue`.

## Table Properties

CuckooTable stores several custom properties (see `CuckooTablePropertyNames` in `table/cuckoo/cuckoo_table_builder.cc`):

| Property | Description |
|----------|-------------|
| `kEmptyKey` | The unused key used to mark empty buckets |
| `kNumHashFunc` | Number of hash functions used |
| `kHashTableSize` | Number of buckets in the hash table |
| `kValueLength` | Fixed value length |
| `kIsLastLevel` | Whether the file uses user-key-only mode |
| `kCuckooBlockSize` | Cuckoo block size |
| `kIdentityAsFirstHash` | Whether identity hash was used |
| `kUseModuleHash` | Whether modulo hashing was used |
| `kUserKeyLength` | User key length |

These properties are read by `CuckooTableReader` during file open to reconstruct the hash table parameters. A missing property causes a `Status::Corruption` error.

## Limitations

| Limitation | Details |
|-----------|---------|
| Fixed key and value lengths | All keys must have the same size. All non-deletion values must have the same size. Deletions are stored separately and the reader synthesizes dummy values for their buckets. |
| No snapshots | Snapshot-based reads are not supported. |
| No merge operations | Only `kTypeValue` and `kTypeDeletion` are supported. Other types return `Status::NotSupported`. |
| No compression | Data is stored uncompressed. |
| No checksums | No block-level checksums. |
| No prefix bloom filters | Not supported by CuckooTable. |
| Requires mmap | Reader requires `allow_mmap_reads=true`; otherwise returns `Status::InvalidArgument`. |
| Max ~2^32 entries | Entry count is stored as `uint32_t`. |
| No `SeekForPrev()` | Asserts in debug builds; undefined behavior in release. |
| No `ApproximateOffsetOf()` / `ApproximateSize()` | Always returns 0. |
| Memory-intensive build | All key-value pairs are buffered in memory during construction. |

## Hash Table Sizing

When `use_module_hash=true`: `hash_table_size = num_entries / max_hash_table_ratio`. The actual table has `hash_table_size + cuckoo_block_size - 1` buckets to accommodate block lookups near the end.

When `use_module_hash=false`: `hash_table_size` starts at 2 and doubles as entries are added, ensuring it stays a power of two. The final size satisfies `hash_table_size >= num_entries / max_hash_table_ratio`.

## Interactions With Other Components

- **TableFactory**: `CuckooTableFactory` (see `table/cuckoo/cuckoo_table_factory.h`) implements `TableFactory` to create `CuckooTableBuilder` and `CuckooTableReader`.
- **Compaction**: CuckooTable files participate in compaction, but the iterator must sort all keys at initialization, making it less efficient than block-based tables for large scans.
- **Last-level optimization**: When the first key has sequence number 0, the builder strips internal key metadata, saving 8 bytes per key. The reader reconstructs internal keys as needed.
