# Compression Types and Selection

**Files:** `include/rocksdb/compression_type.h`, `util/compression.h`, `util/compression.cc`

## CompressionType Enum

`CompressionType` is an unsigned char enum defined in `include/rocksdb/compression_type.h`:

| Value | Name | Availability | Typical Ratio | Speed (comp/decomp) |
|-------|------|--------------|---------------|---------------------|
| 0x00 | kNoCompression | Always | 1.0x | N/A |
| 0x01 | kSnappyCompression | `#ifdef SNAPPY` | 2-3x | Fast/Fast |
| 0x02 | kZlibCompression | `#ifdef ZLIB` | 3-4x | Medium/Medium |
| 0x03 | kBZip2Compression | `#ifdef BZIP2` | 4-5x | Slow/Slow |
| 0x04 | kLZ4Compression | `#ifdef LZ4` | 2-3x | Very Fast/Very Fast |
| 0x05 | kLZ4HCCompression | `#ifdef LZ4` | 3-4x | Slow/Very Fast |
| 0x06 | kXpressCompression | Windows only | 2-3x | Fast/Fast |
| 0x07 | kZSTD | `#ifdef ZSTD` | 4-6x | Medium/Fast |
| 0x80-0xFE | kCustomCompression* | User-defined (managed compression) | Varies | Varies |

CompressionType values are part of the persistent SST format -- existing enum value order must be preserved and new types must be appended at the end. Each block stores its compression type in the block trailer. The managed compression range (`kCustomCompression*`) provides extensibility for adding new algorithms without modifying the core enum.

## Per-Level Compression Configuration

RocksDB determines compression type based on level and compaction output position. The relevant options are `compression_per_level` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) and `bottommost_compression` (see `include/rocksdb/options.h`).

**Compression selection logic** (see `GetCompressionType()` in `db/compaction/compaction_picker.cc`):

1. **Bottommost level** (level >= num_non_empty_levels - 1): Use `bottommost_compression` if set (not `kDisableCompressionOption`), otherwise fall through
2. **Per-level** (if `compression_per_level` non-empty): Index into vector with `idx = 0` for L0, `level - base_level + 1` for others, clamped to `[0, vec.size()-1]`
3. **Fallback**: Use `compression` (the single global setting)

**Edge case handling**: `kDisableCompressionOption` is a sentinel value meaning "not configured" -- it causes the bottommost compression setting to be skipped so the per-level or global compression applies instead.

**Note**: With `level_compaction_dynamic_level_bytes=true`, `compression_per_level[0]` determines L0 compression, but `compression_per_level[i]` (i>0) applies to base level + i - 1, **not** physical level i. See `ColumnFamilyOptions::compression_per_level` in `include/rocksdb/advanced_options.h` for details.

**Example configuration** for 7-level LSM:

```cpp
options.compression = kSnappyCompression;  // Default for all levels
options.compression_per_level = {
  kNoCompression,      // L0: No compression (short-lived)
  kLZ4Compression,     // L1: Fast compression
  kLZ4Compression,     // L2: Fast compression
  kLZ4Compression,     // L3: Fast compression
  kLZ4Compression,     // L4: Fast compression
  kLZ4Compression,     // L5: Fast compression
  kZSTD                // L6: High compression (last level)
};
options.bottommost_compression = kZSTD;  // Override last level
```

**Vector size behavior**: If `compression_per_level` has fewer entries than levels, the last value extends to all remaining levels. For example, `{kNoCompression, kLZ4Compression, kZSTD}` with 7 levels results in: L0=kNoCompression, L1=kLZ4Compression, L2-L6=kZSTD. See `ColumnFamilyOptions::compression_per_level` in `include/rocksdb/advanced_options.h`.
