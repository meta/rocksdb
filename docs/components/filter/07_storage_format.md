# Filter Storage Format

**Files:** `table/block_based/filter_policy.cc`, `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`

## SST File Metaindex Entries

Filters are stored as meta-blocks in the block-based table format. The metaindex block maps a string key to the filter's `BlockHandle`. The key prefix depends on the filter structure:

| Filter Type | Metaindex Key | Points To |
|---|---|---|
| Full filter | `"fullfilter.<CompatibilityName()>"` | The single filter block |
| Partitioned filter | `"partitionedfilter.<CompatibilityName()>"` | The top-level partition index block |
| Obsolete block-based filter | `"filter.<CompatibilityName()>"` | Legacy per-block filter |

For all built-in policies, `CompatibilityName()` is `"rocksdb.BuiltinBloomFilter"`, so the keys are:
- `"fullfilter.rocksdb.BuiltinBloomFilter"`
- `"partitionedfilter.rocksdb.BuiltinBloomFilter"`

During table open, the reader tries these prefixes in order: fullfilter. first, then partitionedfilter., then the obsolete filter. for backward compatibility. It breaks on the first successful match.

## Metadata Trailer Format

Every built-in filter block has a 5-byte metadata trailer appended after the filter payload (separate from the standard block trailer with compression type and checksum).

```
[filter payload: N bytes][metadata: 5 bytes]

Byte layout of metadata:
  Byte 0: discriminator / num_probes
  Bytes 1-4: type-specific metadata
```

### Discriminator Byte (Byte 0)

The first metadata byte acts as a version discriminator:

| Value | Meaning | Reader Used |
|---|---|---|
| 1-127 (positive) | Legacy Bloom `num_probes` | `LegacyBloomBitsReader` |
| 0 | Zero probes (always FP) | `AlwaysTrueFilter` |
| -1 (0xFF) | Newer Bloom implementations | `FastLocalBloomBitsReader` |
| -2 (0xFE) | Ribbon implementations | `Standard128RibbonBitsReader` |
| Other negative | Reserved (future use) | `AlwaysTrueFilter` (safe default) |

This design ensures forward compatibility: unknown negative values degrade to always-true (no filtering, but correct results), giving users degraded performance until compaction rebuilds filters with a known type.

### Legacy Bloom Trailer (byte 0 positive)

```
Byte 0: num_probes (1-127)
Bytes 1-4: num_lines (uint32 little-endian) - number of cache lines
```

The filter payload size is `num_lines * CACHE_LINE_SIZE`. The reader determines `log2_cache_line_size` by checking if `num_lines * CACHE_LINE_SIZE == payload_len`. If not (filter from a platform with different cache line size), it finds the power-of-two block size that satisfies `num_lines * block_size == payload_len`.

### FastLocalBloom Trailer (byte 0 = -1)

```
Byte 0: -1 (0xFF) - marker for newer Bloom
Byte 1: sub-implementation (0 = FastLocalBloom, others reserved)
Byte 2: block_and_probes
  - Top 3 bits: log2(block_bytes) - 6 (0 = 64-byte blocks, 1 = 128-byte, etc.)
  - Bottom 5 bits: num_probes (1-30; 0 and 31 reserved)
Bytes 3-4: reserved (currently 0)
```

For the current implementation, byte 2's top 3 bits are always 0 (64-byte cache lines), and the bottom 5 bits contain `num_probes`.

### Ribbon Trailer (byte 0 = -2)

```
Byte 0: -2 (0xFE) - marker for Ribbon
Byte 1: seed (uint8, 0-255) - hash seed used during construction
Bytes 2-4: num_blocks (24-bit little-endian) - number of 128-slot interleaved blocks
```

`num_blocks` must be >= 2. Values 0 and 1 are reserved/unsupported and cause the reader to fall back to `AlwaysTrueFilter`.

## Reader Dispatch

`BuiltinFilterPolicy::GetBuiltinFilterBitsReader()` reads the discriminator byte and dispatches:

Step 1 -- Check total size. If `<= 5` bytes (metadata only), return `AlwaysFalseFilter` (empty filter).
Step 2 -- Read byte 0 as `int8_t`.
Step 3 -- If positive (1-127): decode Legacy Bloom metadata and return `LegacyBloomBitsReader`.
Step 4 -- If negative: switch on value to select `GetBloomBitsReader()` (-1), `GetRibbonBitsReader()` (-2), or `AlwaysTrueFilter` (other).
Step 5 -- If zero: return `AlwaysTrueFilter`.

### Always-True and Always-False Filters

- `AlwaysTrueFilter`: `MayMatch()` returns `true` for everything. Used for invalid/unknown filter formats and zero-probes case.
- `AlwaysFalseFilter`: `MayMatch()` returns `false` for everything. Used for empty filters (no keys added). Represented by `Slice(nullptr, 0)`.

A special 6-byte encoding (`"\0\0\0\0\0\0"`) is used for always-true filters. The zero discriminator byte triggers `AlwaysTrueFilter` in the reader.

## Cross-Version Compatibility

All built-in filter policies share `CompatibilityName() == "rocksdb.BuiltinBloomFilter"`. This means:

- A `BloomFilterPolicy` can read Ribbon filters
- A `RibbonFilterPolicy` can read Bloom filters
- The actual reader selection happens via the trailer byte, not the policy configuration

However, Ribbon filters are only readable by RocksDB >= 6.15.0. Older versions treat the unknown `-2` discriminator as `AlwaysTrueFilter`, resulting in degraded performance (no filtering) but correct results. Compaction on the newer version will rebuild filters.
