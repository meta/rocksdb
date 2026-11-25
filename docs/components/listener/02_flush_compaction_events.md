# Flush and Compaction Events

**Files:** include/rocksdb/listener.h, db/db_impl/db_impl_compaction_flush.cc, db/compaction/compaction_job.cc, db/compaction/compaction_job.h

## Flush Events

### OnFlushBegin

Called just before a flush job starts executing. The notification flow in DBImpl::NotifyOnFlushBegin():

Step 1: Assert db_mutex_ is held
Step 2: Check shutting_down_ flag; skip if set
Step 3: Compute write stall state (triggered_writes_slowdown, triggered_writes_stop) while holding mutex
Step 4: Release db_mutex_
Step 5: Build FlushJobInfo and call OnFlushBegin() on each listener
Step 6: Reacquire db_mutex_

Important: In the non-atomic flush path (FlushMemTableToOutputFile), OnFlushBegin is called after memtable picking (not before) to prevent a race where a snapshot taken between NotifyOnFlushBegin and memtable picking could see incorrect data. However, in the atomic flush path (AtomicFlushMemTablesToOutputFiles), OnFlushBegin is called BEFORE PickMemTable. This ordering difference means that in the atomic path, the memtable contents may not yet be finalized when OnFlushBegin fires.

### OnFlushCompleted

Called after a flush job finishes successfully and the new SST file is installed. The flow in DBImpl::NotifyOnFlushCompleted() follows the same mutex release/reacquire pattern.

Note: Flush result installation is delegatable. One flush job can commit another concurrent flush job's results and then emit OnFlushCompleted for both. This means the callback thread may differ from the thread that actually wrote the SST file; FlushJobInfo::thread_id records the original flush thread, not necessarily the thread invoking the callback.

The FlushJobInfo struct (see include/rocksdb/listener.h) provides:

| Field | Description |
|-------|-------------|
| cf_id, cf_name | Column family identifier |
| file_path, file_number | Output SST file location |
| job_id, thread_id | Job and thread identification |
| smallest_seqno, largest_seqno | Sequence number range in the output file |
| triggered_writes_slowdown | Whether L0 file count triggered write slowdown |
| triggered_writes_stop | Whether L0 file count triggered write stop |
| flush_reason | The FlushReason enum value |
| table_properties | Properties of the created SST file |
| blob_compression_type | Compression used for blob output files |
| blob_file_addition_infos | Blob files created during this flush |
| oldest_blob_file_number | The oldest blob file referenced by the newly created SST file |

Note: If mempurge occurs instead of a flush (memtable-to-memtable compaction), OnFlushCompleted is not called because no SST file is created.

### OnManualFlushScheduled

Called after a manual flush is scheduled via DB::Flush(). The ManualFlushInfo vector contains one entry per column family. The vector size is greater than 1 only when atomic flush is enabled (atomic_flush=true) and the DB has multiple column families.

This callback is invoked from DBImpl::FlushMemTable() and DBImpl::AtomicFlushMemTables(), on the user thread that requested the flush. It does not follow the standard mutex release/reacquire pattern -- it is called on the user thread outside db_mutex_ entirely. It does check shutting_down_ and skips if set.

Note: DB::FlushWAL() does not trigger this callback. FlushWAL only syncs the WAL to disk; it does not schedule memtable flushes.

## Compaction Events

### OnCompactionBegin

Called at the start of a compaction job. The flow in DBImpl::NotifyOnCompactionBegin():

Step 1: Early return if no listeners registered
Step 2: Assert db_mutex_ is held; check shutting_down_
Step 3: For manual compactions, check manual_compaction_paused_; skip if paused
Step 4: Mark the compaction with SetNotifyOnCompactionCompleted() to ensure the corresponding OnCompactionCompleted will fire
Step 5: Capture num_l0_files from the input version's storage info
Step 6: Release db_mutex_
Step 7: Build CompactionJobInfo via BuildCompactionJobInfo() and call each listener
Step 8: Reacquire db_mutex_

OnCompactionBegin is called for all compaction types: trivial move, trivial file-drop, FIFO deletion, FIFO trivial-copy, and full compactions.

### OnCompactionCompleted

Called after a compaction finishes. Only fires if OnCompactionBegin was called first (guarded by ShouldNotifyOnCompactionCompleted()).

The CompactionJobInfo struct (see include/rocksdb/listener.h) provides:

| Field | Description |
|-------|-------------|
| cf_id, cf_name | Column family identifier |
| status | Whether the compaction succeeded |
| job_id, thread_id | Job and thread identification |
| base_input_level, output_level | Input and output levels |
| input_files, output_files | File paths for inputs and outputs |
| input_file_infos, output_file_infos | Detailed file metadata (CompactionFileInfo) |
| table_properties | Properties keyed by file path |
| compaction_reason | The CompactionReason enum value |
| compression | Compression algorithm for output files |
| blob_compression_type | Compression used for blob output files |
| stats | Detailed CompactionJobStats |
| blob_file_addition_infos | Blob files created |
| blob_file_garbage_infos | Blob garbage tracked |
| aborted | Whether compaction was aborted via AbortAllCompactions() |
| num_l0_files | L0 file count at the time of the callback (before compaction in OnCompactionBegin, after compaction in OnCompactionCompleted) |

Note: A file may appear in both input_files and output_files if it was trivially moved to a different level.

Note: FIFO kChangeTemperature trivial-copy compactions emit OnCompactionBegin/Completed but intentionally skip OnTableFileCreationStarted and OnTableFileCreated callbacks. Applications correlating compaction events with file creation events should account for this gap.

## Subcompaction Events

### OnSubcompactionBegin / OnSubcompactionCompleted

Called before and after each subcompaction within a compaction job. These are dispatched from CompactionJob::NotifyOnSubcompactionBegin() and NotifyOnSubcompactionCompleted() in db/compaction/compaction_job.cc, called from ProcessKeyValueCompaction() which runs on each subcompaction thread.

Only non-trivial compactions that run through CompactionJob::Run() emit subcompaction callbacks. Trivial moves, FIFO deletion compactions, and FIFO trivial-copy compactions do not generate subcompaction events because they bypass CompactionJob entirely.

For non-trivial compactions, there is at least one subcompaction event pair per compaction. If a compaction is split into N subcompactions, there will be one OnCompactionBegin, N OnSubcompactionBegin/Completed pairs, and one OnCompactionCompleted.

The SubcompactionJobInfo struct (see include/rocksdb/listener.h) provides:

| Field | Description |
|-------|-------------|
| cf_id, cf_name | Column family identifier |
| status | Whether the subcompaction succeeded |
| thread_id | Thread that executed this subcompaction |
| job_id | Parent compaction job ID |
| subcompaction_job_id | Unique within a single compaction. Set to -1 for non-subcompaction jobs. |
| base_input_level | Base input level |
| output_level | Output level |
| compaction_reason | The CompactionReason enum value |
| compression | Compression for SST output files |
| stats | Per-subcompaction CompactionJobStats |
| blob_compression_type | Compression for blob output files |

Note: table_properties is not present in SubcompactionJobInfo. Use the parent OnCompactionBegin/Completed callbacks for table properties.

## CompactionReason Enum

The CompactionReason enum (see include/rocksdb/listener.h) identifies why a compaction was triggered:

| Value | Meaning |
|-------|---------|
| kUnknown | Default value (0); may appear when reason is not explicitly set |
| kLevelL0FilesNum | L0 file count exceeded level0_file_num_compaction_trigger |
| kLevelMaxLevelSize | Level size exceeded MaxBytesForLevel() |
| kUniversalSizeAmplification | Universal compaction for size amplification |
| kUniversalSizeRatio | Universal compaction for size ratio |
| kUniversalSortedRunNum | Universal sorted run count exceeded trigger |
| kFIFOMaxSize | FIFO total size exceeded max_table_files_size |
| kFIFOReduceNumFiles | FIFO reducing file count |
| kFIFOTtl | FIFO TTL expiration |
| kManualCompaction | Manual compaction via CompactRange() |
| kFilesMarkedForCompaction | Files marked via SuggestCompactRange() |
| kBottommostFiles | Bottommost level cleanup after snapshot release |
| kTtl | TTL-based compaction |
| kFlush | Flush treated as L0 compaction in internal stats |
| kExternalSstIngestion | External SST file ingestion |
| kPeriodicCompaction | SST file age exceeded periodic threshold |
| kChangeTemperature | File moved for temperature tier change |
| kForcedBlobGC | Forced blob garbage collection |
| kRoundRobinTtl | RoundRobin TTL compaction |
| kRefitLevel | DBImpl::ReFitLevel (internal only) |

## FlushReason Enum

The FlushReason enum (see include/rocksdb/listener.h) identifies why a flush was triggered:

| Value | Meaning |
|-------|---------|
| kOthers | Default/catch-all value (0x00) |
| kGetLiveFiles | Flush for GetLiveFiles() |
| kShutDown | Flush during DB shutdown |
| kExternalFileIngestion | Flush before external file ingestion |
| kManualCompaction | Flush triggered by manual compaction |
| kWriteBufferManager | Global WriteBufferManager triggered flush |
| kWriteBufferFull | Memtable reached write_buffer_size |
| kTest | Test-only flush |
| kDeleteFiles | Flush for DeleteFile() |
| kAutoCompaction | Flush triggered by auto-compaction |
| kManualFlush | User called DB::Flush() |
| kErrorRecovery | Flush during error recovery |
| kErrorRecoveryRetryFlush | Retry flush during error recovery without calling SwitchMemtable |
| kWalFull | WAL size triggered flush |
| kCatchUpAfterErrorRecovery | Catch-up flush after error recovery; SwitchMemtable will not be called |
