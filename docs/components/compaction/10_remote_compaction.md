# Remote Compaction

**Files:** `db/compaction/compaction_job.h`, `db/compaction/compaction_service_job.cc`, `include/rocksdb/options.h`

## Overview

Remote compaction offloads compaction execution to a separate process or host. The primary DB instance delegates the CPU-intensive merge work to a remote worker, then installs the resulting SST files locally. This separation provides performance benefits (isolating compaction CPU from serving reads/writes) and operational flexibility (scaling compaction workers independently).

The user implements the communication layer; RocksDB provides the serialization format and execution framework.

## Architecture

The remote compaction architecture involves two participants:

- **Primary host**: Owns the DB with write permission. Selects files for compaction, serializes the compaction request, and installs the result.
- **Compaction worker**: Opens the DB in read-only mode, executes the compaction, and writes output SST files to a temporary location.

## Four-Phase Protocol

Step 1 -- **Schedule**: The primary calls `CompactionService::Schedule()` (see `include/rocksdb/options.h`) with a serialized `CompactionServiceInput`. The user's implementation sends this to a remote worker. Returns a `CompactionServiceJobInfo` containing a scheduled job identifier.

Step 2 -- **Compact**: The worker opens the DB using `DB::OpenAndCompact()` with the received input. This creates a `CompactionServiceCompactionJob` (see `db/compaction/compaction_job.h`), which extends `CompactionJob` with a custom output path. The worker cannot modify the LSM tree; it only reads input files and writes output files.

Step 3 -- **Wait and return**: The primary calls `CompactionService::Wait()` with the job identifier, blocking until the remote worker completes. The worker returns a serialized `CompactionServiceResult` containing output file metadata, stats, and status.

Step 4 -- **Install and purge**: The primary renames the output SST files from the temporary location to the DB's SST directory, builds `FileMetaData` for each output file, and installs them via the normal `CompactionJob::Install()` path. Input files are then purged.

## CompactionServiceInput

`CompactionServiceInput` (see `db/compaction/compaction_job.h`) is the serializable input specification sent to the worker:

| Field | Description |
|-------|-------------|
| `cf_name` | Column family name |
| `snapshots` | Active snapshot sequence numbers |
| `input_files` | SST file names for all input levels |
| `output_level` | Target output level |
| `db_id` | Database identifier (used for unique SST ID generation on the remote worker) |
| `has_begin` / `begin` | Optional start key boundary for the subcompaction |
| `has_end` / `end` | Optional end key boundary for the subcompaction |
| `options_file_number` | Options file number for the worker to load DB/CF options |

Serialization uses RocksDB's `OptionTypeInfo` framework, producing a string-based format similar to options file serialization.

## CompactionServiceResult

`CompactionServiceResult` (see `db/compaction/compaction_job.h`) contains the compaction output:

| Field | Description |
|-------|-------------|
| `status` | Compaction execution status |
| `output_files` | Vector of `CompactionServiceOutputFile` with per-file metadata |
| `output_level` | Output level |
| `output_path` | Location of output files on the worker |
| `bytes_read` / `bytes_written` | I/O statistics |
| `stats` | Job-level `CompactionJobStats` |
| `internal_stats` | Per-level `CompactionStatsFull` for both output and proximal levels |

Each `CompactionServiceOutputFile` carries complete file metadata: file name, size, sequence number range, internal key range, timestamps, checksums, unique ID, table properties, and whether it is a proximal-level output.

## CompactionServiceCompactionJob

`CompactionServiceCompactionJob` (see `db/compaction/compaction_job.h`) is a private subclass of `CompactionJob` used on the worker side. Key differences from a regular `CompactionJob`:

- **Single subcompaction**: The worker processes exactly one subcompaction (the primary dispatches each subcompaction separately)
- **Custom output path**: Overrides `GetTableFileName()` to write SSTs to the user-specified temporary directory
- **No Install phase**: The worker only executes `Prepare()` and `Run()`. The primary handles installation.
- **Read-only DB**: The worker opens the DB read-only; it cannot modify the MANIFEST or WAL
- **Abort not supported**: Uses a static `kCompactionAbortedFalse` flag since abort signaling to remote workers is not yet implemented

## Installation on the Primary

When the primary receives a successful result, it performs installation in `ProcessKeyValueCompactionWithCompactionService()`:

Step 1 -- **Rename files**: For each output file, allocates a new file number from `VersionSet`, renames the file from the worker's output path to the DB's SST directory. This requires the temporary output directory to be on the same filesystem as the DB (rename semantics).

Step 2 -- **Build metadata**: Constructs `FileMetaData` from the `CompactionServiceOutputFile` fields and adds it to the appropriate `CompactionOutputs` (normal or proximal level).

Step 3 -- **Set stats**: Copies per-level stats and job stats from the result into the subcompaction state.

Step 4 -- **Notify service**: Calls `CompactionService::OnInstallation()` to inform the user's implementation of success or failure.

## Fallback to Local Compaction

The remote compaction path supports fallback at two points:

- `Schedule()` may return `kUseLocal` if the service decides the compaction should run locally
- `Wait()` may return `kUseLocal` if the remote execution fails in a recoverable way

When either returns `kUseLocal`, `ProcessKeyValueCompaction()` falls back to local execution using the standard `CompactionIterator` path.

## Output Verification

The primary can verify remote compaction output files using `verify_output_flags` (see `MutableCFOptions` in `options/cf_options.h`). The `kEnableForRemoteCompaction` flag enables verification specifically for remote compaction outputs. Available verification modes include block checksum verification, full iteration verification, and file checksum verification.

## Limitations

- The temporary output directory must be on the same filesystem as the DB for atomic rename. If not, the user must copy files before returning from `Wait()`.
- Each remote compaction processes a single subcompaction. The primary dispatches subcompactions individually.
- `CompactionService::CancelAwaitingJobs()` allows cancellation of awaiting remote compaction jobs, but abort signaling to actively running remote workers is not yet supported.
- Compaction resumption for remote compaction is supported but limited to a single subcompaction.
- The user implements all communication (scheduling, waiting, result transfer). No built-in RPC transport is provided.
