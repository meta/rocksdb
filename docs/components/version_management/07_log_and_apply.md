# LogAndApply

**Files:** `db/version_set.h`, `db/version_set.cc`

## Overview

LogAndApply() is the central metadata commit workflow. It writes VersionEdits to the MANIFEST file and installs new Versions as the current state. All metadata changes -- flushes, compactions, column family operations -- flow through this single entry point.

## Entry Points

VersionSet provides several LogAndApply() overloads that all funnel into the multi-CF batch version:

| Overload | Use Case |
|----------|----------|
| Single edit, single CF | Flush completing, simple compaction |
| Edit list, single CF | Multiple edits for one CF |
| Multi-CF batch | Atomic multi-CF flush, bulk ingestion |
| LogAndApplyToDefaultColumnFamily() | Convenience for default CF |

Note: ReactiveVersionSet (used by secondary/follower instances) overrides LogAndApply() to return NotSupported, since followers do not write MANIFEST.

## Group Commit

LogAndApply() uses group commit via manifest_writers_ (a std::deque<ManifestWriter*>):

Step 1: The caller creates a ManifestWriter and enqueues it. If another writer is already active, the caller waits on a condition variable.

Step 2: The leader (first writer in the queue) calls ProcessManifestWrites() and batches all compatible pending writers together. The batching loop iterates the queue and groups compatible writers until encountering a non-batchable one (CF add/drop operations and dummy skip-manifest-write edits are not grouped with others).

Step 3: After processing, the leader wakes up all writers it batched, each receiving the shared status.

This means multiple concurrent flushes or compactions completing at the same time are serialized into a single MANIFEST write, reducing I/O overhead.

## ProcessManifestWrites Workflow

This is the core implementation, called by the leader writer:

**Phase 1 -- Build (mutex held):**
- Iterate over batched writers
- For each writer's edit list, call LogAndApplyHelper() to apply edits to a VersionBuilder
- Skip edits for dropped column families (adjust atomic group remaining entries if needed)
- Create new Version objects for each affected CF
- Call VersionBuilder::SaveTo() to materialize new VersionStorageInfo

**Phase 2 -- Write (mutex released):**
- Call LoadTableHandlers() to open table readers for new files
- If rolling to a new MANIFEST: create the file, write full state snapshot via WriteCurrentStateToManifest()
- Call PrepareAppend() on each new Version (populates derived structures)
- Encode each VersionEdit and append to MANIFEST via log::Writer::AddRecord()
- Sync the MANIFEST file
- If new MANIFEST: update CURRENT file atomically via SetCurrentFile()

**Phase 3 -- Install (mutex re-acquired):**
- Apply WAL edits to the in-memory WalSet
- For CF add: call CreateColumnFamily()
- For CF drop: call SetDropped() and unref
- For normal edits: update per-CF log numbers, install new Versions via AppendVersion()
- Update descriptor_last_sequence_, manifest_file_number_, manifest_file_size_

## Sequence Number Tracking

ProcessManifestWrites() tracks max_last_sequence across all batched edits to ensure sequence numbers in MANIFEST are written in non-decreasing order. The descriptor_last_sequence_ is only updated after successful installation.

Important: The last_sequence_ written to MANIFEST reflects the latest sequence number assigned (including data still in memtables), not just the sequence number of the last flushed SST file. This means recovering from MANIFEST alone (without WAL replay) may see a higher sequence number than the data present in SST files. Full recovery requires replaying WAL files after MANIFEST replay to close this gap.

## Callbacks

LogAndApply() supports two callback hooks:

- manifest_wcb (manifest write callback): Called after the MANIFEST write completes, with the status. Used for notifying components about successful/failed commits.
- pre_cb (pre-callback): Called after the writer becomes the exclusive manifest writer (leader) but before ProcessManifestWrites() executes. If it returns non-OK, the write is skipped. This is used by DBImpl::SetOptions() to apply mutable option changes atomically with version appending.

## Skip-Manifest-Write Mode

When the first edit is marked with MarkNoManifestWriteDummy(), the MANIFEST write is skipped entirely. The only in-tree use is DBImpl::SetOptions(), which uses a dummy edit to append a new Version without writing MANIFEST and without releasing the DB mutex. This ensures mutable option changes take effect atomically with respect to other version appenders. In this mode, PrepareAppend() is called with the mutex held (unlike the normal path where the mutex is released).

## Error Recovery

If the MANIFEST write or sync fails:
- The io_status_ on the VersionSet is set to the error
- The ErrorHandler is notified to quarantine newly created files
- descriptor_log_ is reset to force a new MANIFEST on the next attempt
- New Versions are deleted (not installed)
- If CURRENT file update fails after a successful MANIFEST write, the old MANIFEST is quarantined
