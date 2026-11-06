# PlainTable Format

**Files:** `table/plain/plain_table_factory.h`, `table/plain/plain_table_factory.cc`, `table/plain/plain_table_builder.h`, `table/plain/plain_table_builder.cc`, `table/plain/plain_table_reader.h`, `table/plain/plain_table_reader.cc`, `table/plain/plain_table_index.h`, `table/plain/plain_table_index.cc`, `table/plain/plain_table_key_coding.h`, `table/plain/plain_table_key_coding.cc`, `table/plain/plain_table_bloom.h`, `include/rocksdb/table.h`

## Overview

PlainTable is an SST file format optimized for memory-mapped (mmap) file systems such as tmpfs. Unlike the block-based table format, data is not organized into fixed-size blocks -- key-value pairs are written sequentially with lightweight encoding. This design eliminates block decompression overhead and enables fast random access when the file is memory-mapped.

Important: PlainTable does not support compression or per-block checksums. It is designed for in-memory databases where data resides on fast storage (e.g., tmpfs, ramfs). Using PlainTable on disk-based file systems is not recommended.

## Configuration Options

PlainTable is configured via `PlainTableOptions` (see `include/rocksdb/table.h`).

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `user_key_len` | `uint32_t` | `kPlainTableVariableLength` | Fixed key length optimization. Set to `kPlainTableVariableLength` for variable-length keys. |
| `bloom_bits_per_key` | `int` | 10 | Bloom filter bits per prefix (or per key in total-order mode). Set to 0 to disable. |
| `hash_table_ratio` | `double` | 0.75 | Hash table utilization ratio (number of prefixes / number of buckets). Set to 0 to disable hash index and use binary search only. |
| `index_sparseness` | `size_t` | 16 | Number of keys between consecutive index records within the same prefix. Controls trade-off between index size and linear search cost. |
| `huge_page_tlb_size` | `size_t` | 0 | If > 0, allocate hash indexes and bloom filters from huge page TLB instead of malloc. |
| `encoding_type` | `EncodingType` | `kPlain` | Key encoding strategy: `kPlain` (full keys) or `kPrefix` (prefix-compressed keys). |
| `full_scan_mode` | `bool` | false | When true, no index is built. Point lookups and keyed `Seek()` are not supported. Sequential iteration via `SeekToFirst()` followed by `Next()` is the only supported access pattern. |
| `store_index_in_file` | `bool` | false | When true, the hash index and bloom filter are computed during file building and stored in the SST file. On read, the index is loaded directly instead of being recomputed. |

## File Layout

PlainTable stores all data as a single contiguous region of key-value records, followed by optional metadata blocks:

```
[key-value record 0]
[key-value record 1]
...
[key-value record N-1]
[meta block: bloom filter]       (optional, when store_index_in_file=true and bloom enabled)
[meta block: plain table index]  (optional, when store_index_in_file=true)
[meta block: properties]
[metaindex block]
[Footer]
```

The footer uses `kPlainTableMagicNumber` (`0x8242229663bf9564`) with footer format version 0 (always, regardless of encoding type) and `kNoChecksum`. The table property `format_version` is set to 0 for `kPlain` encoding or 1 for `kPrefix` encoding, but this is separate from the footer format version.

## Key Encoding

PlainTable supports two key encoding types, controlled by `encoding_type` in `PlainTableOptions` (see `include/rocksdb/table.h`).

### kPlain Encoding

Each key-value record is written as:

```
[key_size (varint32)]  -- only for variable-length keys
[internal_key]
[value_size (varint32)]
[value]
```

For fixed-length keys (`user_key_len != kPlainTableVariableLength`), the `key_size` field is omitted since the key length is known from table properties.

### kPrefix Encoding

Prefix encoding reduces key storage by sharing common prefixes between consecutive keys. Three encoding forms are used:

1. **Full Key**: Written when the prefix changes or at the start. Encodes the entire internal key.
2. **Prefix + Suffix**: Written when a key shares the same prefix as the previous full key. Only the prefix length and suffix bytes are stored.
3. **Suffix Only**: Written when a key shares the same prefix as the previous prefix-encoded key. Only the suffix bytes are stored.

Each key flag byte uses the top 2 bits for the type (full key = 00, prefix = 01, suffix = 10) and the lower 6 bits for size. If the size exceeds 63, a varint32 follows with the remaining length (actual_size - 63).

### Internal Key Optimization

For keys with sequence number 0 and value type, a special compact encoding is used: the internal key bytes are followed by a single byte `0xFF` instead of the full 8-byte sequence number and type field. This saves 7 bytes per key. See `kValueTypeSeqId0` in `PlainTableFactory` (see `table/plain/plain_table_factory.h`).

## Hash-Based Index

When `hash_table_ratio > 0` and a prefix extractor is configured, PlainTable builds a hash index for fast prefix-based lookups. The index is managed by `PlainTableIndex` and `PlainTableIndexBuilder` (see `table/plain/plain_table_index.h`).

### Index Structure

The index is an array of 32-bit entries, one per hash bucket. Each entry uses bit 31 as a flag:

| Flag | Meaning |
|------|---------|
| 0 | Bucket contains a single prefix. The lower 31 bits are the file offset of the first record with that prefix. |
| 1 | Bucket contains multiple prefixes or too many records for one prefix. The lower 31 bits point into a sub-index for binary search. |

The sub-index region stores, for each multi-prefix bucket:

```
[num_records: varint32]
[offset_0: fixedint32]
[offset_1: fixedint32]
...
[offset_N-1: fixedint32]
```

where each offset points to a record in the data region. Offsets are in ascending order to support binary search.

### File Size Limit

Because offsets are stored as 31-bit values, PlainTable has a maximum file size of 2 GB (`kMaxFileSize = (1u << 31) - 1`). Files exceeding this limit are rejected during `PlainTableReader::Open()`.

### Index Building

**At write time** (when `store_index_in_file=true`): `PlainTableBuilder` collects key/prefix hashes during `Add()` and uses `PlainTableIndexBuilder` to construct the hash index. The index and optional bloom filter are written as meta blocks before the properties block.

**At read time** (when index is not in file): `PlainTableReader::PopulateIndex()` scans the entire data region, builds `IndexRecord` entries via `PopulateIndexRecordList()`, and constructs the hash table in memory.

### Lookup Flow

Step 1: Hash the prefix to find the bucket.

Step 2: Check the bucket flag:
- If `kNoPrefixForBucket`: no matching prefix exists, return not found.
- If `kDirectToFile`: single prefix, jump directly to that file offset.
- If `kSubindex`: perform binary search in the sub-index to find the target offset.

Step 3: Scan forward from the found offset, comparing keys until a match is found or the prefix changes.

## Total-Order Mode

When no prefix extractor is configured (`prefix_extractor = nullptr`), PlainTable operates in total-order mode. In this mode:

- The hash table has a single bucket (index size = 1) that points to the start of the data.
- Bloom filter operates on full user keys instead of prefixes.
- `Seek()` with arbitrary keys is supported (unlike prefix mode, which only supports prefix-based seeks).
- `SeekToFirst()` is always supported regardless of mode.

Note: `SeekToLast()` and `SeekForPrev()` return `Status::NotSupported`. `Prev()` is not supported (asserts in debug builds, undefined behavior in release).

## Bloom Filter

PlainTable uses its own bloom filter implementation (`PlainTableBloomV1` in `table/plain/plain_table_bloom.h`), separate from the block-based table's bloom/ribbon filters.

- In prefix mode: bloom filter is built on prefix hashes with `bloom_bits_per_key` bits per prefix.
- In total-order mode: bloom filter is built on full user key hashes with `bloom_bits_per_key` bits per key.
- The bloom filter uses 6 probes (hardcoded in `PlainTableBuilder`).

When `store_index_in_file=true`, the bloom filter is serialized as a meta block and loaded directly on read. Otherwise, it is recomputed by scanning the data at open time.

## Limitations

| Limitation | Details |
|-----------|---------|
| No compression | Data is stored uncompressed. |
| No checksums | Block-level checksums are not computed or verified. |
| No range deletions | `Add()` returns `Status::NotSupported` for `kTypeRangeDeletion` entries. |
| No `SeekToLast()` / `Prev()` | Reverse iteration is not supported. `Prev()` asserts in debug builds and has undefined behavior in release. |
| No `SeekForPrev()` | Returns `Status::NotSupported`. |
| Max file size 2 GB | Due to 31-bit offsets in the hash index. |
| Requires mmap | Designed for `use_mmap_reads=true`. Non-mmap mode works but with reduced performance. |
| Prefix extractor compatibility | When a prefix extractor was used to build the file, the same extractor must be provided when opening it (validated by name comparison). |
| Max entries ~2^32 | Offsets are stored as `uint32_t`. |

## Interactions With Other Components

- **TableFactory**: `PlainTableFactory` (see `table/plain/plain_table_factory.h`) implements `TableFactory` to create `PlainTableBuilder` and `PlainTableReader` instances.
- **Compaction**: PlainTable files participate in normal compaction. Compaction creates iterators with `total_order_seek=true` but typically only calls `SeekToFirst()`.
- **Table Properties**: PlainTable stores encoding type, bloom version, and bloom block count as user-collected properties (see `PlainTablePropertyNames` in `table/plain/plain_table_factory.cc`).
- **Seqno-to-time mapping**: Not yet supported for PlainTable (see `SetSeqnoTimeTableProperties()` in `table/plain/plain_table_builder.cc`).
