# Monitoring and Statistics

**Files:** `include/rocksdb/listener.h`, `include/rocksdb/metadata.h`, `include/rocksdb/db.h`, `include/rocksdb/iostats_context.h`

## File Metadata APIs

Temperature is exposed through several file metadata APIs:

### DB::GetLiveFilesMetaData()

Returns a vector of `LiveFileMetaData` (see `include/rocksdb/metadata.h`). Each entry inherits from `FileStorageInfo` which includes a `temperature` field. This is the most direct way to inspect per-file temperatures.

### DB::GetColumnFamilyMetaData()

Returns `ColumnFamilyMetaData` with per-level file lists. Each `SstFileMetaData` entry (inheriting from `FileStorageInfo`) includes the file's temperature.

### DB::GetLiveFilesStorageInfo()

Returns `LiveFileStorageInfo` entries that include temperature from the `FileStorageInfo` base class.

## EventListener Callbacks

Temperature information is available through `FileOperationInfo` callbacks (see `include/rocksdb/listener.h`). The `FileOperationInfo` struct contains a `temperature` field.

To receive these callbacks, the EventListener must override `ShouldBeNotifiedOnFileIO()` to return `true`. Available per-operation callbacks include:

| Callback | Operation |
|----------|-----------|
| `OnFileReadFinish` | File read completed |
| `OnFileWriteFinish` | File write completed |
| `OnFileFlushFinish` | File flush completed |
| `OnFileSyncFinish` | File sync completed |
| `OnFileRangeSyncFinish` | Range sync completed |
| `OnFileTruncateFinish` | File truncation completed |
| `OnFileCloseFinish` | File close completed |

Note: `FlushJobInfo` and `CompactionJobInfo` do not expose per-file temperature fields directly. To monitor output file temperatures, use `FileOperationInfo` callbacks or query file metadata via `GetLiveFilesMetaData()` after the operation completes.

## Compaction Reason Tracking

The `CompactionReason::kChangeTemperature` enum value (see `include/rocksdb/listener.h`) identifies compactions triggered specifically to change file temperature. This is tracked in `DB::GetProperty("rocksdb.stats")` under compaction reasons and is available in `CompactionJobInfo::compaction_reason`.

## I/O Statistics by Temperature

`IOStatsContext` (see `include/rocksdb/iostats_context.h`) tracks bytes read and read counts broken down by file temperature. Write operations are not tracked per temperature in `IOStatsContext`. This enables monitoring of read I/O distribution across tiers and identifying workloads that would benefit from tiered storage.

When `default_temperature` is configured, files without explicit temperature use that value for I/O accounting.

## CompactForTieringCollector

RocksDB provides a built-in table-properties collector, `CompactForTieringCollectorFactory` (see `include/rocksdb/utilities/table_properties_collectors.h`), designed for tiered storage workloads. It counts entries eligible for last-level placement and can mark SST files as needing compaction when a sufficient fraction qualifies. For `TimedPut` entries, the collector extracts the packed preferred sequence number to compute eligibility using tiering semantics rather than the raw batch seqno, making it useful for historical backfill workloads.
