# File Lifecycle Events

**Files:** include/rocksdb/listener.h, db/event_helpers.h, db/event_helpers.cc, db/builder.cc, db/blob/blob_file_builder.cc, db/blob/blob_file_completion_callback.h, db/compaction/compaction_job.cc

## EventHelpers Dispatch Layer

File lifecycle events are dispatched through the EventHelpers static class in db/event_helpers.h. This class provides a centralized dispatch layer that both logs events as JSON to the info log and notifies all registered listeners.

The general pattern for each event:

Step 1: Early return if no event_logger and no listeners registered
Step 2: Build a JSONWriter object with event metadata and write to event log
Step 3: Build the notification info struct
Step 4: Iterate through all listeners and call the appropriate callback

Unlike flush/compaction events, file lifecycle events do not require mutex release/reacquire since they are called from code paths that already do not hold db_mutex_.

## SST File Events

### OnTableFileCreationStarted

Called before an SST file begins being written. Dispatched by EventHelpers::NotifyTableFileCreationStarted().

Call paths differ by context:
- **Flush and recovery**: Invoked from BuildTable() in db/builder.cc
- **Compaction**: Invoked from CompactionJob::OpenCompactionOutputFile() in db/compaction/compaction_job.cc

The TableFileCreationBriefInfo struct provides:

| Field | Description |
|-------|-------------|
| db_name | Database name |
| cf_name | Column family name |
| file_path | Path to the file being created |
| job_id | The flush or compaction job ID |
| reason | TableFileCreationReason (flush, compaction, recovery, misc) |

### OnTableFileCreated

Called after an SST file creation finishes (successfully or not). Dispatched by EventHelpers::LogAndNotifyTableFileCreationFinished(). Applications should check info.status to determine success or failure.

For failed or empty outputs, BuildTable() may report Status::Aborted("Empty SST file not kept") and the file path may be rewritten to "(nil)". The failed/empty file is cleaned up directly by BuildTable without going through the obsolete-file deletion path, so there will be no matching OnTableFileDeleted callback for such files.

The TableFileCreationInfo struct extends TableFileCreationBriefInfo with:

| Field | Description |
|-------|-------------|
| file_size | Size of the created file |
| table_properties | Full TableProperties of the SST file |
| status | Whether creation succeeded |
| file_checksum | Checksum of the table file |
| file_checksum_func_name | Name of the checksum function used |

The JSON event log entry includes detailed table properties: data size, index size, filter size, key/value statistics, compression info, sequence number ranges, and seqno-to-time mapping.

### OnTableFileDeleted

Called when an SST file is deleted. Dispatched by EventHelpers::LogAndNotifyTableFileDeletion(), invoked from DBImpl::DeleteObsoleteFileImpl() in db/db_impl/db_impl_files.cc.

The TableFileDeletionInfo struct provides db_name, file_path, job_id, and status.

Note: This callback is designed for external logging services. It provides string parameters rather than a DB* pointer. Applications building logic on file creation/deletion should use OnFlushCompleted and OnCompactionCompleted instead.

## Blob File Events

### OnBlobFileCreationStarted

Called before a blob file begins being written. Dispatched by EventHelpers::NotifyBlobFileCreationStarted(), invoked from BlobFileBuilder in db/blob/blob_file_builder.cc.

The BlobFileCreationBriefInfo struct includes a BlobFileCreationReason indicating whether the blob file was created during flush, compaction, or recovery.

### OnBlobFileCreated

Called after a blob file creation finishes (successfully or not). Dispatched by EventHelpers::LogAndNotifyBlobFileCreationFinished().

The BlobFileCreationInfo struct extends BlobFileCreationBriefInfo with:

| Field | Description |
|-------|-------------|
| total_blob_count | Number of blobs in the file |
| total_blob_bytes | Total bytes of blob data |
| status | Whether creation succeeded |
| file_checksum | Checksum of the blob file |
| file_checksum_func_name | Name of the checksum function used |

### OnBlobFileDeleted

Called when a blob file is deleted. Dispatched by EventHelpers::LogAndNotifyBlobFileDeletion().

The BlobFileDeletionInfo struct provides db_name, file_path, job_id, and status.

## Event Ordering

Event ordering depends on the operation type and compaction kind.

### Flush ordering

For a typical flush that produces both SST and blob files:

Step 1: OnFlushBegin
Step 2: OnTableFileCreationStarted (always fires first)
Step 3: Zero or more OnBlobFileCreationStarted / OnBlobFileCreated pairs (blob files are opened lazily when blob data is first written)
Step 4: OnTableFileCreated
Step 5: OnFlushCompleted

### Non-trivial compaction ordering

For a compaction that runs through CompactionJob:

Step 1: OnCompactionBegin
Step 2: For each subcompaction: OnSubcompactionBegin -> file creation events -> OnSubcompactionCompleted
Step 3: OnCompactionCompleted

### Trivial move / FIFO deletion ordering

For trivial moves and FIFO deletion compactions, only compaction begin/completed fire. No subcompaction or file creation callbacks are emitted.

Step 1: OnCompactionBegin
Step 2: OnCompactionCompleted

### FIFO trivial-copy compaction ordering

FIFO kChangeTemperature trivial-copy compactions create new SST files but intentionally skip OnTableFileCreationStarted and OnTableFileCreated callbacks. Applications correlating compaction events with file creation events should account for this asymmetry.

Step 1: OnCompactionBegin
Step 2: OnCompactionCompleted (no file creation callbacks despite new files being created)
