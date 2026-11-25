# IO Classification and Tagging

**Files:** `include/rocksdb/file_system.h`, `include/rocksdb/env.h`, `include/rocksdb/types.h`

## Overview

RocksDB tags every I/O operation with multiple classification dimensions, enabling custom `FileSystem` implementations to make context-aware decisions about scheduling, caching, prioritization, and resource management. These tags are propagated through the storage stack via `IOOptions` and `FileOptions`.

## IOType

`IOType` (see `include/rocksdb/file_system.h`) classifies the type of data being read or written:

| Value | Meaning |
|-------|---------|
| `kData` | User data blocks |
| `kFilter` | Bloom filter blocks |
| `kIndex` | Index blocks |
| `kMetadata` | File metadata (properties, compression dictionary) |
| `kWAL` | Write-ahead log data |
| `kManifest` | MANIFEST file data |
| `kLog` | Info log data |
| `kUnknown` | Unspecified (default) |
| `kInvalid` | Invalid marker |

This tag is set in `IOOptions::type` and allows file systems to differentiate between data and metadata I/O for prioritization or routing to different storage tiers.

## IOActivity

`IOActivity` (see `Env::IOActivity` in `include/rocksdb/env.h`) classifies the RocksDB operation that triggered the I/O:

| Value | Code | Description |
|-------|------|-------------|
| `kFlush` | 0 | Background flush writes |
| `kCompaction` | 1 | Background compaction reads/writes |
| `kDBOpen` | 2 | Database open reads/writes |
| `kGet` | 3 | User `Get()` point lookup |
| `kMultiGet` | 4 | User `MultiGet()` batch lookup |
| `kDBIterator` | 5 | User iterator scan |
| `kVerifyDBChecksum` | 6 | DB checksum verification |
| `kVerifyFileChecksums` | 7 | File checksum verification |
| `kGetEntity` | 8 | Wide-column `GetEntity()` |
| `kMultiGetEntity` | 9 | Wide-column `MultiGetEntity()` |
| `kGetFileChecksumsFromCurrentManifest` | 10 | Manifest checksum reads |
| kCustomIOActivity80-kCustomIOActivityFE | 0x80-0xFE | Reserved for custom/internal use |
| kUnknown | 0xFF | Unknown/unspecified |

RocksDB automatically propagates `IOActivity` tags through the I/O stack. Custom `FileSystem` implementations can access the tag via `IOOptions::io_activity`.

### Per-Activity Statistics

RocksDB provides per-activity histograms for monitoring I/O latency by operation type:

**Read histograms:**
- `FILE_READ_FLUSH_MICROS`, `FILE_READ_COMPACTION_MICROS`, `FILE_READ_DB_OPEN_MICROS`
- `FILE_READ_GET_MICROS`, `FILE_READ_MULTIGET_MICROS`, `FILE_READ_DB_ITERATOR_MICROS`
- `FILE_READ_VERIFY_DB_CHECKSUM_MICROS`, `FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS`

**Write histograms:**
- `FILE_WRITE_FLUSH_MICROS`, `FILE_WRITE_COMPACTION_MICROS`, `FILE_WRITE_DB_OPEN_MICROS`

## Temperature

`Temperature` (see `include/rocksdb/types.h`) provides hints for tiered storage placement:

| Value | Code | Meaning |
|-------|------|---------|
| `kUnknown` | 0x00 | No temperature information |
| `kHot` | 0x04 | Frequently accessed data |
| `kWarm` | 0x08 | Moderately accessed data |
| `kCool` | 0x0A | Infrequently accessed data |
| `kCold` | 0x0C | Rarely accessed data |
| `kIce` | 0x10 | Archival data |

Temperature is set at file creation via `FileOptions::temperature` and tracked by `WritableFileWriter` and `RandomAccessFileReader`. File systems can use temperature hints to place files on appropriate storage tiers (e.g., hot data on NVMe, cold data on HDD or object storage).

`FSRandomAccessFile::GetTemperature()` can report the actual temperature of an open file, useful when external processes move files between tiers.

## WriteLifeTimeHint

`WriteLifeTimeHint` (see `Env` in `include/rocksdb/env.h`) provides hints about the expected lifetime of written data:

| Value | Meaning |
|-------|---------|
| `WLTH_NOT_SET` | No hint |
| `WLTH_NONE` | No lifetime information |
| `WLTH_SHORT` | Short-lived data (e.g., L0 SSTs) |
| `WLTH_MEDIUM` | Medium-lived data |
| `WLTH_LONG` | Long-lived data |
| `WLTH_EXTREME` | Extremely long-lived data (e.g., bottommost level) |

This hint is passed to the kernel via `fcntl(F_SET_RW_HINT)` on Linux, enabling the filesystem/device to optimize data placement for wear leveling on SSDs.

## IOPriority

`IOPriority` (see `Env::IOPriority` in `include/rocksdb/env.h`) controls rate limiter priority:

| Value | Code | Typical Use |
|-------|------|-------------|
| `IO_LOW` | 0 | Compaction |
| `IO_MID` | 1 | Available for custom use |
| `IO_HIGH` | 2 | Flush |
| `IO_USER` | 3 | User-triggered I/O |
| `IO_TOTAL` | 4 | Bypass rate limiting |

This is used by the rate limiter to determine queue priority and by `SequentialFileReader` to control rate limiter charging behavior.
