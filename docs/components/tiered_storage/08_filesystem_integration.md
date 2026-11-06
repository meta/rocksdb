# FileSystem and I/O Integration

**Files:** `include/rocksdb/file_system.h`, `include/rocksdb/listener.h`, `include/rocksdb/env.h`, `db/flush_job.cc`, `db/compaction/compaction_job.cc`, `db/table_cache.cc`, `file/random_access_file_reader.cc`

## Temperature Propagation

Temperature hints flow through the RocksDB I/O stack at multiple levels:

### File Creation

When creating new SST files during flush or compaction, temperature is set in `FileOptions::temperature` before calling `FileSystem::NewWritableFile()`. The FileSystem receives the temperature at file creation time and can use it to:

- Choose the storage path (e.g., SSD vs. HDD)
- Set up appropriate buffering or caching
- Apply storage-tier-specific I/O policies

For flush, this is set in `FlushJob::WriteLevel0Table()` (see `db/flush_job.cc`). For compaction, the temperature is determined by `Compaction::GetOutputTemperature()` and passed through `FileOptions`.

### File Reading

Temperature reaches the FileSystem through `FileOptions::temperature` when opening table files. The `TableCache` (see `db/table_cache.cc`) applies `default_temperature` for files whose MANIFEST temperature is `kUnknown`. Temperature is then carried in the `RandomAccessFileReader` (see `file/random_access_file_reader.cc`) and surfaced to listeners via `FileOperationInfo::temperature`. File objects may also expose temperature via `GetTemperature()` when the underlying FileSystem supports it.

### FileOperationInfo

The `FileOperationInfo` struct (see `include/rocksdb/listener.h`) carries temperature in its `temperature` field. This struct is passed to EventListener callbacks for each file operation (read, write, flush, sync, truncate, close, etc.).

## FileSystem Interface Points

The key FileSystem methods that receive temperature information through `FileOptions`:

| Method | When Called |
|--------|-----------|
| `NewWritableFile()` | Creating new SST files (flush, compaction output) |
| `NewRandomAccessFile()` | Opening SST files for reads |
| `NewSequentialFile()` | Sequential reads (e.g., during ingestion verification) |
| `ReopenWritableFile()` | Re-opening files during recovery |

## I/O Statistics by Temperature

RocksDB tracks read I/O statistics broken down by temperature. The `IOStatsContext` (see `include/rocksdb/iostats_context.h`) records bytes read and read counts per temperature tier via the `FileIOByTemperature` struct. Write operations are not tracked per temperature in `IOStatsContext`.

When `default_temperature` is set, files without an explicit temperature are counted under that temperature for I/O accounting purposes. This affects only the statistics, not actual file placement.

## Custom FileSystem Implementation

A tiered storage FileSystem implementation typically:

1. **Inspects `FileOptions::temperature`** at file creation to determine the target storage path
2. **Routes hot files** to fast local storage (SSD, NVMe)
3. **Routes cold files** to cheaper storage (HDD, network storage, cloud object store)
4. **Applies different I/O policies** per temperature (prefetch sizes, cache admission, rate limits)

The default `PosixFileSystem` ignores temperature hints entirely. Users must provide a custom FileSystem implementation to achieve actual tiered placement.
