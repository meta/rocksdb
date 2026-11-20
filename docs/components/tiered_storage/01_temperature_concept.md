# Temperature Concept

**Files:** `include/rocksdb/types.h`, `include/rocksdb/file_system.h`, `include/rocksdb/listener.h`, `db/version_edit.h`

## Temperature Enum

RocksDB defines six temperature levels in the `Temperature` enum (see `include/rocksdb/types.h`):

| Value | Name | Intended Use |
|-------|------|-------------|
| 0x00 | kUnknown | No temperature assigned (default) |
| 0x04 | kHot | Frequently accessed data |
| 0x08 | kWarm | Moderately accessed data |
| 0x0A | kCool | Rarely accessed data |
| 0x0C | kCold | Very rarely accessed data |
| 0x10 | kIce | Archival data |

The enum values are intentionally sparse with gaps between them. This allows future insertion of intermediate tiers without breaking persistent format compatibility. `kLastTemperature` is a sentinel value used as an upper bound, not a valid temperature.

Note: `kUnknown` means "no explicit temperature assigned", not "unknown temperature". Files with `kUnknown` temperature use the FileSystem's default placement policy. It is distinct from `kHot`; explicitly set `default_write_temperature` to `kHot` if hot placement is desired.

## Temperature as a Hint

Temperature is strictly advisory. RocksDB attaches temperature metadata to file operations, but the actual behavior depends entirely on the FileSystem implementation:

- **Default FileSystem**: Ignores temperature hints entirely (all files go to the same storage)
- **Custom FileSystem**: Can route files to different storage media, apply different caching/prefetching policies, or adjust rate limiting based on temperature

The FileSystem receives temperature through `FileOptions::temperature` at file creation time. For read operations, temperature is carried via the `FileOptions` used when opening the file and is surfaced to listeners via `FileOperationInfo::temperature`. See `include/rocksdb/file_system.h` for the FileSystem interface.

## Temperature Persistence

Each SST file's temperature is stored in `FileMetaData::temperature` (see `db/version_edit.h`). Temperature is persisted in the MANIFEST file using the `kTemperature` custom tag in `NewFile` records. This means temperature survives DB restarts.

A file's temperature is normally assigned at creation and changed by rewriting the file through compaction (e.g., `kChangeTemperature` compaction in FIFO, or a regular compaction that produces output at a different level with a different temperature). Additionally, `experimental::UpdateManifestForFilesState()` can repair MANIFEST temperature metadata to match externally moved files without rewriting the SST data.

## Temperature Semantics by Tier

| Temperature | Typical Storage | Typical I/O Policy |
|-------------|----------------|-------------------|
| kHot | Fast local SSD | High cache priority, aggressive prefetching |
| kWarm | Standard SSD | Normal cache priority |
| kCool | Mixed SSD/HDD | Reduced cache priority |
| kCold | HDD or cloud storage | Low cache priority, rate-limited reads |
| kIce | Archival (tape, deep archive) | Minimal caching, strict rate limits |
