# Remote Compaction

**Files:** `db/db_impl/db_impl_secondary.cc`, `db/db_impl/db_impl_secondary.h`, `db/compaction/compaction_job.h`, `db/compaction/compaction_service_job.cc`, `include/rocksdb/db.h`, `include/rocksdb/options.h`

## Overview

RocksDB supports offloading compaction to remote hosts via `DB::OpenAndCompact()`. This feature uses the secondary instance infrastructure: the remote compaction worker opens the primary's database as a secondary instance, runs compaction, and writes output files to a separate directory -- without modifying the primary's LSM tree.

## API

`DB::OpenAndCompact()` has two overloads in `include/rocksdb/db.h`:

| Overload | Parameters |
|----------|-----------|
| With options | `OpenAndCompactOptions`, `name`, `output_directory`, `input`, `output*`, `CompactionServiceOptionsOverride` |
| Legacy (simple) | `name`, `output_directory`, `input`, `output*`, `CompactionServiceOptionsOverride` |

The legacy overload delegates to the first with default `OpenAndCompactOptions()`.

| Parameter | Description |
|-----------|-------------|
| `options` | `OpenAndCompactOptions` controlling cancellation and resumption (`allow_resumption` defaults to `false`) |
| `name` | Path to the primary database |
| `output_directory` | Where the remote compactor writes output SST files. Also serves as the `secondary_path` for the worker's secondary instance (info log, progress files) |
| `input` | Serialized `CompactionServiceInput` from the primary |
| `output` | Serialized `CompactionServiceResult` returned to the primary |
| `override_options` | `CompactionServiceOptionsOverride` for customizing comparator, merge operator, compaction filter, table factory, etc. |

## Workflow

Step 1: **Deserialize input**. `CompactionServiceInput::Read()` deserializes the input string into a `CompactionServiceInput` containing the column family name, input file list, output level, snapshots, `db_id`, `options_file_number`, and optional subcompaction key bounds (`has_begin`/`begin`, `has_end`/`end`) for bounded sub-range compaction.

Step 2: **Load options**. DB and CF options are loaded from the primary's `OPTIONS-<options_file_number>` file via `LoadOptionsFromFile()`. Options from `CompactionServiceOptionsOverride` are applied in two phases:

**DB-level overrides:**
- Arbitrary serializable options via `options_map` (parsed with `ignore_unknown_options=true`, so typos are silently ignored)
- Direct field overrides: `env`, `file_checksum_gen_factory`, `statistics`, `listeners`, `info_log`
- Hard-coded: `max_open_files = -1`, `compaction_service = nullptr`

**CF-level overrides** (applied only to the target CF specified in `CompactionServiceInput::cf_name`):
- Arbitrary serializable options via `options_map` (same silent-ignore behavior)
- Direct field overrides: `comparator`, `merge_operator`, `compaction_filter`, `compaction_filter_factory`, `prefix_extractor`, `table_factory`, `sst_partitioner_factory`, `table_properties_collector_factories`

Step 3: **Open as secondary**. `OpenAsSecondaryImpl()` opens the database with `recover_wal=false` -- remote compaction only needs LSM state from the MANIFEST, not memtable data from WAL replay. Only the default CF and target CF are opened.

Step 4: **Run compaction**. `CompactWithoutInstallation()` creates a `CompactionServiceCompactionJob` and runs it. The compaction reads input SST files from the primary's data directory and writes output SST files to `output_directory`. The `db_session_id` is the local (remote worker's) session ID, ensuring unique file IDs across compactors.

Step 5: **Serialize result**. `CompactionServiceResult::Write()` serializes the output file metadata and statistics.

Step 6: **Cleanup**. Column family handles are deleted, and the secondary DB is closed.

## CompactWithoutInstallation Details

`CompactWithoutInstallation()` (in `db/db_impl/db_impl_secondary.cc`) is the core of remote compaction:

1. **Check cancellation**: Returns `Status::Incomplete(kManualCompactionPaused)` if `options.canceled` is set
2. **Initialize workspace**: Creates the output directory and optionally prepares compaction progress state for resumption
3. **Build compaction inputs**: Uses `CompactionPicker::GetCompactionInputsFromFileNumbers()` to locate input files in the `VersionStorageInfo`
4. **Create compaction job**: Instantiates a `CompactionServiceCompactionJob` with the secondary's `versions_`, `table_cache_`, and other infrastructure
5. **Run**: Unlocks `mutex_`, runs the compaction job, re-locks
6. **Track resumed bytes**: If resuming from a previous progress file, records `REMOTE_COMPACT_RESUMED_BYTES` in statistics

## Compaction Progress and Resumability

Remote compaction supports resuming interrupted compactions via progress files stored in `secondary_path_`.

### Progress File Management

`CompactionProgressFilesScan` (see `db/db_impl/db_impl_secondary.h`) categorizes files found in the output directory:

| File Type | Description |
|-----------|-------------|
| Latest progress file | Most recent `kCompactionProgressFile`, identified by highest timestamp |
| Old progress files | Earlier progress files from crashed `InitializeCompactionWorkspace()` calls |
| Temporary progress files | `kTempFile` entries with the compaction progress prefix |
| Table file numbers | Output SST files |

### Initialization Workflow

`InitializeCompactionWorkspace()` creates the output directory and, when `allow_resumption=true`, calls `PrepareCompactionProgressState()`:

Step 1: **Scan directory** for existing progress files and output files.

Step 2: **Decide whether to resume**. If a latest progress file exists, attempt to load it. Otherwise, clean everything and start fresh.

Step 3: **Load progress** (if resuming). `ParseCompactionProgressFile()` reads `VersionEdit` records from the progress file via `SubcompactionProgressBuilder`, which tracks output file metadata for each subcompaction.

Step 4: **Cleanup**. Delete old/temporary progress files. If resuming, delete output SST files not tracked in the progress. If starting fresh, delete all output files.

### Progress File Format

Progress files use the same WAL record format (log::Writer/Reader) and contain `VersionEdit` records encoding `SubcompactionProgress`. Each record describes output files produced by a subcompaction before it was interrupted.

### Resumption Constraints

| Constraint | Reason |
|------------|--------|
| Single subcompaction only | Current implementation only supports resuming one subcompaction |
| Incompatible with output hash verification | Resumption loses hash state computed before interruption. Disabled when `paranoid_file_checks=true` or `verify_output_flags` contains `kVerifyIteration` |
| `output_directory` must be empty when `allow_resumption=false` | Existing files may cause correctness errors |
| Best-effort fallback | If progress file is missing or corrupt, system cleans up and starts fresh |

### Compaction Progress Writer

`FinalizeCompactionProgressWriter()` creates a temporary progress file, optionally writes initial progress (if resuming), renames it to a timestamped final name, and creates a new progress writer for the current compaction run. The rename ensures atomicity -- a crash between creating the temp file and renaming it leaves a temp file that is cleaned up on next startup.

## Integration with CompactionService

On the primary side, `CompactionService` (not in secondary instance code) provides `Start()` and `WaitForComplete()` methods. The primary serializes a `CompactionServiceInput`, sends it to a remote worker, and the worker calls `DB::OpenAndCompact()`. After completion, the primary receives the serialized `CompactionServiceResult` and installs the output files into its LSM tree.

The primary may also fall back to local compaction if the remote service is unavailable. Compaction priority information is carried in `CompactionServiceJobInfo` for the primary-side `CompactionService` scheduling callbacks, not in the serialized `CompactionServiceInput` sent to the remote worker.

## Remote Worker Observability

Remote workers only see a subset of events through `listeners`, and `statistics` are local to the worker unless the caller explicitly aggregates them. The remote worker does not receive the same event stream or statistics behavior as a local compaction.
