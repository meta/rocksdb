// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_manifest.h"
#include "rocksdb/status.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

// Blob reference census
//
// Background: before indirect blob GC can move the live values out of a blob
// file or a relocation file and drop it, it must know which SSTs still
// reference that file and how many live bytes they keep in it. Ordinary reads
// and writes never use the census.
//
// A RocksDB Version (class Version: the live SST, blob and relocation files of
// one column family at one point in time) cannot answer this:
// - For each SST, FileMetaData keeps oldest_blob_file_number (the oldest blob
//   file that the SST references) and, for an exact SST, the descriptor of its
//   reference metadata. None of these fields lists the other files that the
//   SST references.
// - BlobFileTable maps a generation number to a live file, or to a route (an
//   old file number -> the relocation file that now holds its values). It says
//   where the values of a generation number are now, not which SSTs reference
//   it.
// So a Version has no mapping from a generation number to SSTs, and none can
// be derived from it. The full list of generation numbers that an SST
// references is stored only in that SST, in its exact reference metadata (see
// the top of db/blob/blob_reference_metadata.h).
//
// The census is that reverse index: for each generation number, the SSTs that
// reference it and the total bytes that those references keep. It is built by
// reading the reference metadata of every SST, lives only in memory, and is
// built again after each DB open.
//
// Terms:
//   - generation number: the file number that a reference uses to find its
//     value (see "Generation number" in db/blob/blob_index.h).
//     kIndirectFlattenedBlob stores it. kIndirectBlob does not store it,
//     because it equals the origin. The census is keyed by generation number.
//   - exact SST: an SST with exact reference metadata (a nonempty
//     FileMetaData::blob_reference_descriptor). The census counts only exact
//     SSTs, and in them only the kIndirectUnflattened and kIndirectFlattened
//     groups (IsCensusGroup()). kDirect groups are not counted.
//   - coarse SST and coarse frontier: exact metadata costs collector memory
//     while an SST is built, so an SST can skip it and record only a coarse
//     frontier (see "Coarse SST" in db/blob/blob_reference_metadata.h). GC then
//     gives up exact selection for the indirect-origin blob files at or above
//     the lowest frontier of the Version
//     (VersionStorageInfo::OldestCoarseIndirectOriginFileNumber()): such a file
//     is coarse pinned, and the census keeps it out of the candidate heap.
//   - SST summary (BlobReferenceSstSummary): the group totals of one exact SST,
//     in memory. The table builder makes it for a new SST. A reader decodes it
//     from the reference index of an existing SST.
//   - holder: an exact SST that has at least one counted group with a given
//     generation number. Several such groups in one SST make one holder.
//   - slot: a small dense number for each exact SST (BlobCensusSlotTable). A
//     new SST gets the lowest free slot.
//   - physical file (in the census): an indirect-origin blob file or a
//     relocation file. Such a file holds value bytes and can be a GC
//     candidate. Normal blob files written with the option off are not
//     tracked, because the census counts only indirect references.
//   - lost file: a relocation file that the Version marks lost
//     (BlobRelocationFileMetaData::IsMarkedLost()). Its values cannot be read.
//     A lost file that still has live bytes stays out of the candidate heap.
//   - forward state: the BlobRelocationState (routes and relocation files) of
//     a Version (see db/blob/blob_relocation_manifest.h). It maps a generation
//     number forward to its physical file.
//   - delta (BlobReferenceCensusDelta): the census change of one Version
//     install: the added and removed SSTs with their groups, and the changed
//     routes and physical files.
//   - change log: one ordered list of deltas. Install() appends one entry for
//     each Version install while the build cursor is open.
//   - cursor: the position of the census build in the change log (the build
//     cursor).
//   - ready: the census has applied every delta up to the current Version and
//     is published for that Version. The GC API works only on a ready census.
//   Groundwork term. The term below, and the GC job registration and selected
//   copy under "Readers", serve GC jobs (intended users: the GC job that
//   installs its relocation output, and route retirement). The census itself
//   does not need them, so a reader who wants only the census can skip them.
//   - metadata-only commit: a MANIFEST commit that changes relocation state
//     (routes, relocation files) but adds no SST
//     (BeginMetadataCommit() / EndMetadataCommit()). At most one runs per
//     census at a time, and no GC job starts while one runs.
//
// Ownership: a column family that has the per-CF format fence
// (VersionStorageInfo::HasBlobIndirectionFormat()) owns one census
// (ColumnFamilyData::GetOrCreateBlobReferenceCensus()). A column family with
// enable_blob_indirection also gets the census object when the DB has the
// DB-global format fence (see ColumnFamilyData::blob_reference_census()), so
// that its installs can be reserved.
//
// Who updates the census:
//   1. Build. At DB open, and in every rebuild (see "Errors and rebuild"), one
//      background job on the LOW pool (the census job) builds the census. It
//      reads the reference metadata of every exact SST, a few SSTs per run
//      and one column family at a time (see "Build").
//   2. Normal updates. After the build, the flush or compaction thread that
//      installed a new Version applies the delta of that install itself,
//      right after the install (see "Apply after an install"). So while the
//      DB runs normally, the census job has little or no work.
//   3. The rest. The census job applies the deltas that step 2 did not apply:
//      installs on other threads (for example a user CompactFiles()), and
//      column families that step 2 skipped because another thread held the
//      worker. It also runs every retry and every requested refresh of the
//      GC candidates (see "Scheduling").
// In this file, "the worker" means the code that changes the census
// contents. Only one thread runs it at a time (worker_run_mutex()). Only the
// worker changes the generation numbers, slots and holder sets. GC can only
// mark candidates in the heap as suppressed (SuppressWorkerCandidates()).
//
// Structure of one census:
//   ColumnFamilyData
//    `-- BlobReferenceCensus (one per column family that needs it)
//         |-- WorkerState: the census contents
//         |    |-- generations: generation number -> WorkerGeneration
//         |    |   (see "Per generation number")
//         |    |-- BlobCensusSlotTable: SST file number <-> slot
//         |    |-- BlobGcCandidateHeap: physical files ranked by garbage ratio
//         |    `-- forward state of the applied Version (the routes)
//         |-- BlobReferenceCensusBuildContext: the state of one build
//         `-- change log: BlobReferenceChangeLogEntry -> ... -> log tail
//              |   (each entry holds one shared, immutable delta)
//              `-- build cursor: the worker reads the log through it
//
//   - ResetWorkerState() creates the WorkerState when a build starts. A new
//     build, an invalidating install, FailBuild() or Cancel() swaps it out.
//   - VersionSet::PrepareBlobReferenceCensusInstalls() creates one delta for
//     each new Version of a column family that needs a census
//     (BuildBlobReferenceCensusDelta()). A log entry stays alive while a
//     cursor, an older entry or the log tail still points to it.
//   - DBImpl::BuildBlobReferenceCensus() creates the build context. It stays
//     attached after the build, so later runs keep the same build token (see
//     Build).
//   - A call under the DB mutex returns the state that it replaces in a
//     BlobReferenceCensusGarbage. The caller frees it without the DB mutex.
//
// Data flow of one install:
//   1. The table builder makes the summary of each new exact SST
//      (FileMetaData::blob_reference_summary). At job start, a compaction
//      reads the summary of each exact input SST
//      (CompactionJob::ReadInputBlobReferenceSummaries()). The VersionEdit
//      carries both.
//   2. Under the DB mutex, VersionSet::ReserveBlobReferenceCensusInstalls()
//      calls BeginInstallPreparation() (O(1)). This file calls this an install
//      reservation. Until step 4 of this list ends it, the worker does not
//      publish. The comment at BlobReferenceCensusInstallBatch in
//      db/version_set.h gives the same path in more detail, in its own four
//      steps (reserve, prepare, install, free).
//   3. Without the DB mutex, PrepareBlobReferenceCensusInstalls() builds the
//      delta. PrepareInstallWithCache() allocates the log entry and charges its
//      memory.
//   4. After the MANIFEST write, under the DB mutex, Install() links the entry
//      at the log tail without allocating, when a cursor is open.
//   5. The SuperVersion install queues the census job of the column family.
//   6. The worker runs without the DB mutex: in the flush or compaction thread
//      right after the install, or in the census job. It takes up to
//      kMaxDeltasPerRead (64, an estimate) deltas at a time, applies each one
//      and acknowledges it.
//   7. When the build token is valid, the build cursor is empty, no install is
//      being prepared, and the census has reached the current Version,
//      TryPublishWorker() marks it ready. DBImpl then stores the top GC
//      candidates, the empty-file candidates and the relocation garbage bytes
//      in that Version.
//
// Scheduling: DBImpl runs the census job (DBImpl::ScheduleBlobReferenceCensus()
// and BGWorkBlobReferenceCensus()). A column family is in one of four states:
//   - In the run queue: it has work (NeedsWorkerRun(): the census is not ready
//     for the current Version, or a candidate refresh was requested). Open, a
//     SuperVersion install and a census wakeup add it.
//   - Running: the job takes the column family at the head of the run queue
//     and runs one slice for it. If it still has work, it goes to the back of
//     the run queue.
//   - In the retry list: after a failed build (1 s, doubled after each failure,
//     at most 32 s), while a GC job copies its snapshot (1 ms), or while a
//     suppressed GC candidate waits for its retry time (candidate_retry_micros
//     in BlobReferenceCensusBuildContext). The periodic task
//     (PeriodicTaskType::kBlobReferenceCensus, once per second) moves it back
//     to the run queue when its time has come, or earlier when it has no
//     failures and has new work.
//   - Waiting for an install: when a publish finds an active install
//     reservation (TryPublishWorker() returns kInstallPending),
//     NeedsWorkerRun() is false, so the column family is not queued. Install()
//     or CancelInstallPreparation() wakes it (SetBuildWakeup()).
//
// Apply after an install: the census job shares the LOW pool with compactions
// and has no thread of its own. With few background threads, a delta could wait
// in the change log for a long time, and the census would stay unready and hold
// the delta's memory. So the flush and compaction threads also run the worker:
//   1. At the end of each background flush or compaction, the thread releases
//      the DB mutex (DBImpl::ApplyBlobReferenceCensusAfterInstall()).
//   2. It visits only the column families with a census that this job
//      flushed or compacted (also when the job failed), so a DB without blob
//      indirection has nothing to visit. A column family dropped before the
//      job ends is not visited. It visits them in any scheduling state, also in
//      the retry list, so a delta there does not wait for the periodic task.
//   3. For each column family, it tries worker_run_mutex(). If another thread
//      holds it (for example the census job), the thread skips that column
//      family and does not wait.
//   4. It skips a column family whose build has not started or has not finished
//      its scan of the SSTs, because the census job does the scan. It also
//      skips a build with a stored failure (BlobReferenceCensusBuildContext::
//      failure, below).
//   5. Otherwise it applies the queued deltas up to the end of the change log.
//      If it applied at least one delta, it then tries to publish the census
//      (DBImpl::ApplyBlobReferenceCensusDeltas()).
// This work never fails the flush or the compaction. When it cannot go on, it
// leaves the rest to the census job. The SuperVersion install has queued that
// job, or, for a column family in the retry list, the periodic task runs it:
//   - TryAgain (a registered GC job has not yet copied its snapshot) changes
//     nothing. The delta stays at the front of the queue, and the census job
//     applies it later.
//   - An overflow of the build cursor: the thread stops and stores no failure.
//     The census job restarts or fails the build (see "Errors and rebuild").
//     If a delta failed at the same time, the thread drops the build's queued
//     deltas, so that delta is never applied again.
//   - Any other failure may have changed the worker part way, so the delta must
//     never be applied again. The thread stores the failure in
//     BlobReferenceCensusBuildContext::failure and logs it once. Later
//     applies after an install skip the build (step 4), and the next census
//     job run fails the build with it.
//   - A census job run that fails stores its failure the same way before it
//     releases worker_run_mutex(), so an apply after an install that runs
//     between the failure and FailBuild() applies nothing and publishes
//     nothing.
//   - A publish that fails or must wait: the census job publishes later.
// The census job still runs every build and every retry. Installs on other
// threads, for example a user CompactFiles(), do not run this step. The census
// job, or the next flush or compaction of the same column family, applies
// their deltas.
//
// Build: DB open and every rebuild run the same build.
//   1. Under the DB mutex, BeginBuild() opens the build cursor and returns a
//      new token (a build id). The caller pins the current Version in the same
//      mutex hold, so the cursor sees every later install.
//   2. Each job run is one slice. It visits at most 16 SSTs of the pinned
//      Version (kMaxFilesPerSlice in DBImpl::BuildBlobReferenceCensus(), an
//      estimate) and reads the reference index of each exact one. Between
//      reads, it applies the deltas installed after the pin (catch-up). It
//      skips an SST of the pinned Version that such a delta removed. It uses a
//      summary that is already in memory (a seed from a rebuild install)
//      instead of a read. While a registered GC job has not yet copied its
//      snapshot (TryAgain), the slice ends early and the next run visits the
//      same SST again. A summary that the slice already read is kept as a seed,
//      so that SST is not read twice.
//   3. Until the scan ends, the slot changes of each generation number wait in
//      the pending lists of its WorkerGeneration. FinalizeWorkerBuild() then
//      builds each holder set once.
//   4. The census publishes as in step 7 above. The build cursor stays open,
//      so later deltas keep the census current.
//
// An install that invalidates the census, FailBuild() and Cancel() clear the
// build token. A run of an old build then fails its token check, so it cannot
// change the worker state of a newer build.
//
// Example of one build. Token 7 and Versions 100 to 103 are example values.
//   1. Under the DB mutex, BeginBuild() returns token 7 and pins Version 100.
//   2. Run 1 visits the first 16 SSTs of Version 100 and reads the exact
//      ones. Between reads, it applies the deltas up to Version 102.
//   3. Run 2 visits the next 16 SSTs and applies the delta to Version 103.
//   4. The last run visits the rest, applies the deltas that are still
//      queued, and calls FinalizeWorkerBuild().
//   5. The census publishes as ready for Version 103, the current Version.
//   6. From now on, the flush or compaction thread that installs a new
//      Version applies its delta and publishes again (see "Apply after an
//      install").
//
// Per generation number: `generations` maps each generation number to one
// WorkerGeneration. It holds the holder set (BlobCensusSlotSet), the totals
// (reference count, stored value bytes, record bytes), and the physical file
// that holds the values now: the generation number itself, or its route
// target. The worker adds the live bytes of each generation number to its
// physical file in the candidate heap (WorkerState::AdjustGenerationBytes()),
// which keeps the total bytes and the live bytes of each physical file. A
// holder set is a varint list or a bitmap of slots (see BlobCensusSlotSet for
// the switch rule).
//
// Example: two generation numbers, with 1,000 slots in use. All numbers are
// example values.
//   BlobCensusSlotTable: slot 5 = SST 500, slot 17 = SST 501, ...
//   generation 10:
//     - holders: list {5, 17}, which means SSTs 500 and 501 (2 bytes)
//     - totals: the reference count and bytes of both SSTs
//     - physical file: 10 (no route; an indirect-origin blob file)
//   generation 300:
//     - holders: a bitmap with 600 of 1,000 slots set. The highest slot is
//       999, so it takes 16 words of 64 bits (128 bytes).
//     - totals: the reference count and bytes of the 600 SSTs
//     - physical file: 900 (route 300 -> relocation file 900)
//   BlobGcCandidateHeap: the live bytes of file 10 include generation 10, and
//   the live bytes of file 900 include generation 300.
//
// Readers: a GC job registers itself at a ready Version (BeginGcJob()), and
// SnapshotSelected() copies only the generation numbers that it selected. From
// BeginGcJob() until SnapshotSelected() ends or the job ends (EndGcJob()), the
// worker does not apply deltas, so the copy shows exactly the census at the
// job's input Version. The job does not read later deltas. At most one GC job
// is registered per census.
//
// Threads and locks:
//   - Under the DB mutex, the census does only O(1) or bounded work, for
//     example BeginInstallPreparation(), Install(), BeginBuild(), FailBuild(),
//     Cancel(), the GC job calls, the metadata-only commit calls, and the
//     TryPublishWorker() call of DBImpl. These calls never allocate or free
//     state as large as the census.
//   - worker_run_mutex_ lets only one thread run the worker: the census job or
//     an installing thread (see "Apply after an install"). The worker takes the
//     DB mutex to publish while it holds worker_run_mutex_. So no code may take
//     worker_run_mutex_ while it holds the DB mutex. It is the first lock in
//     the lock order, before worker_mutex_.
//   - worker_mutex_ guards the WorkerState contents. The worker holds it to
//     apply a delta. GC holds it to copy generation numbers, to capture the
//     candidate revisions and to suppress candidates. No code takes it while
//     it holds the DB mutex.
//   - mutex_ (the log mutex) guards the change log, the build cursor, the GC
//     job registration, the build context pointer and some small flags.
//   - metadata_commit_mutex_ guards the metadata-only commit state: a commit
//     in progress, a commit with an unknown result, and the canceled flag.
//   - A caller may hold the DB mutex when it takes mutex_ or
//     metadata_commit_mutex_, so both are held only for short, bounded work.
//   - The lock order is worker_mutex_, then mutex_, then
//     metadata_commit_mutex_.
//   - worker_state_ is read and replaced under worker_state_mutex_
//     (LoadWorkerState() and ExchangeWorkerState()), so a call under the DB
//     mutex can drop the WorkerState without worker_mutex_. A worker call that
//     still runs keeps its own handle and then fails its token check.
//     worker_state_mutex_ is a leaf lock: no other lock is taken while it is
//     held, and each hold only copies or swaps one shared_ptr (a few atomic
//     operations). The old WorkerState is freed after the lock is released. A
//     mutex is used because std::atomic_load() and std::atomic_exchange() on a
//     shared_ptr are deprecated in C++20, and some standard libraries do not
//     fully support std::atomic<std::shared_ptr>.
//
// Why this design:
//   - Background worker: building or copying a reverse index as large as the
//     DB at the head of the MANIFEST queue would delay every Version install.
//     So an install only links one prepared entry.
//   - A delta at every install: a delta touches only the changed SSTs, and a
//     recount reads every SST. A full compaction also installs a delta. When
//     a compaction rewrites almost every SST, its delta can cost more CPU than
//     a rebuild. One install path is simpler.
//   - Summaries only on edits: a Version would keep each summary for the life
//     of its SST, but the census needs it only to add and to remove the SST. So
//     VersionBuilder drops it, and a compaction reads its input summaries again
//     (reference index only, fill_cache off). Keeping the summaries would use
//     much more memory, in the census and in every Version, than the reverse
//     index itself.
//   - One shared log entry per install: Install() runs under the DB mutex, so
//     it must not allocate. It links the one prepared entry.
//   - Bounded change log: a slow reader must not hold unbounded memory. After
//     the scan of a build ends, an installing thread usually applies each delta
//     right after its install, so the change log stays short. During the scan
//     of a build, installing threads skip the column family. So the deltas wait
//     in the log between build slices, and a build of a very large DB can take
//     minutes (an estimate). One limit, kMaxLogMemory (256 MB), covers all
//     memory that the build cursor keeps in the change log, including the SST
//     summaries that have their own cache charge. The value is an estimate, not
//     a measurement: only a long build under heavy write load should reach it.
//     When the cursor already holds the limit, it overflows at its next delta,
//     and the build restarts at once. The restarted build, and every later
//     build until one publishes, has a larger limit, kMaxRestartLogMemory (1
//     GB, 4 x kMaxLogMemory, an estimate, not tuned). The factor 4 leaves room
//     for one slow full rebuild under write load. If a build overflows the
//     larger limit too, it fails and retries after the normal backoff (see
//     "Errors and rebuild"). So overflows never restart builds in a loop
//     without a delay, and the log memory stays bounded. GC waits while the
//     census is not ready, so no value is lost.
//   - Slices: one LOW pool task never runs long, so compactions and other
//     column families can run between slices.
//   - Key per generation number, not per (generation number, origin): GC asks
//     which SSTs use a file. A key per origin would put one SST into several
//     holder sets.
//   - Slots and list/bitmap sets: an SST file number takes 8 bytes. A slot is
//     a small dense number. A list costs 1 byte per holder for gaps below 128
//     and 2 bytes for gaps below 16,384 (varint sizes). A bitmap costs 1 bit
//     per slot. Each set takes about the smaller of the two sizes.
//   - Pending lists during the build: encoding a list again costs time in
//     proportion to its size, so the build encodes each set only once.
//   - Routes in the delta: the worker maps each generation number through the
//     routes of the same Version as the SST change, also when it applies the
//     delta much later.
//   - Install reservation: the worker must not publish a Version that an
//     install is about to replace.
//
// Census memory charge: the census charges its memory to the block cache under
// CacheEntryRole::kBlobReferenceCensus, unless that role is disabled in
// cache_usage_options, so the memory counts against the block cache capacity.
// The charged memory is the worker state, the build state, the summaries that
// a build or a compaction reads, and the change log entries. An indirect blob
// GC copy of the selected generation numbers (BlobReferenceCensusState) belongs
// to the GC job. It is charged to CacheEntryRole::kIndirectBlobGcWorkingMemory
// in the GC job's block cache instead (see SnapshotSelected()).
//
// Errors and rebuild: a change that the census cannot apply as a delta makes
// it unready, and a new build starts. The GC API waits until the build
// publishes. DB open also runs a build, because the census is kept only in
// memory. The error cases:
//   - A delta with `rebuild` set: an exact SST was removed without its summary
//     (only CompactionJob passes the summaries of the SSTs it deletes, so for
//     example DeleteFile, DeleteFilesInRange, or a compaction that could not
//     read an input), or a new exact SST came without its summary. The
//     summaries that the install adds become seeds. The build starts at once.
//   - PrepareInstallWithCache() failed: PrepareBlobReferenceCensusInstalls()
//     prepares an invalidation instead (PrepareInvalidation()). The install
//     still commits, and Install() makes the census unready. A running
//     build fails its token check and waits for the normal retry delay below.
//   - The build cursor overflowed: the build restarts at once with the larger
//     limit kMaxRestartLogMemory (see "Bounded change log"). A second overflow
//     before a publish fails the build like any other error below. The census
//     job then logs a WARN line and counts it in the ticker
//     BLOB_DB_CENSUS_RESTART_OVERFLOW_COUNT.
//   - The apply after an install failed with an error other than TryAgain: the
//     delta may be partly applied, so it is never applied again. The next
//     census job run fails the build with that error
//     (BlobReferenceCensusBuildContext::failure).
//   - Any other build error (for example Corruption): FailBuild() drops the
//     worker state. The next try waits 1 s, doubled after each failure, at
//     most 32 s (PeriodicTaskType::kBlobReferenceCensus).
//   - Not a census error: an edit that adds an SST to a fenced column family
//     with no exact metadata, no coarse frontier and no legacy marker fails the
//     commit with Corruption (BuildBlobReferenceCensusDelta()).
//
// Self-check: debug builds compare the census with a fresh BuildState() of the
// same Version after 1 in 16 publications of the census job
// (DBImpl::CheckBlobReferenceCensus()). A publish by the apply after an install
// is not checked.

class Version;
struct BlobReferenceCensusDelta;

// The size and state of one physical file, as a Version install passes it to
// the candidate heap. The same delta also carries the route changes of that
// install. A total of 0 means the file is not a physical file of the census: it
// was removed, or it is a normal blob file. The heap drops it if it has it.
// Indirect-origin blob files and relocation files of one column family share
// one heap.
struct BlobGcPhysicalFileSummary {
  uint64_t file_number = 0;
  uint64_t total_record_bytes = 0;
  bool is_indirect_origin = false;
  bool is_lost = false;
};

// Indirect blob GC candidates of one column family. The map holds every
// physical file. The heap holds the files that GC may pick, ranked by garbage
// ratio (garbage bytes / total bytes), highest first. Suppressed files, coarse
// pinned files and lost files that still have live bytes stay out of the heap
// (see Activate()). Only the census worker changes it, and never under the DB
// mutex. Each file also knows its position in the heap, so a change to one file
// fixes the heap in O(log n), where n is the number of files in the heap.
//
// Live bytes of a file are the sum, over all generation numbers that map to the
// file, of the bytes their references keep. A value that several SSTs reference
// is counted once per reference, so the sum can be too high, never too low.
// This is safe for ranking, but it does not prove that a value is dead. GC
// checks the exact references of the files it selects before it moves any
// value.
class BlobGcCandidateHeap {
 public:
  static constexpr size_t kMaxSelectedFiles = kBlobGcMaxSelectedFiles;
  using Selection = std::array<uint64_t, kMaxSelectedFiles>;

  // The garbage bytes and total bytes of each file of a Selection, at the same
  // index. CutSelection() uses them.
  struct SelectedFileBytes {
    uint64_t garbage_bytes = 0;
    uint64_t total_bytes = 0;
  };
  using SelectionBytes = std::array<SelectedFileBytes, kMaxSelectedFiles>;

  // Keeps the leading files of `selection` whose garbage ratio is at least
  // `garbage_ratio` (the indirect_blob_gc_garbage_ratio option), and clears the
  // rest. Select() returns the files ranked by garbage ratio, so the files that
  // stay are the best files that pass the ratio.
  static void CutSelection(double garbage_ratio, const SelectionBytes& bytes,
                           Selection* selection);

  void SetPhysicalFile(const BlobGcPhysicalFileSummary& file);
  void SetCoarseIndirectOriginFrontier(uint64_t frontier);
  Status AdjustLiveBytes(uint64_t file_number, uint64_t bytes, bool add);
  void DeletePhysicalFile(uint64_t file_number);
  bool IsIndirectOrigin(uint64_t file_number) const;

  // True if a coarse SST may hold an unflattened reference with generation
  // number `generation`: the coarse frontier is set and `generation` is at or
  // above it. Coarse SSTs are not census holders, so route retirement keeps the
  // route of such a generation number. Same rule as
  // VersionStorageInfo::IsCoarseIndirectOriginBlobPinned(), applied to the
  // census copy of the frontier.
  bool IsCoarsePinnedGeneration(uint64_t generation) const {
    return coarse_indirect_origin_frontier_ != 0 &&
           generation >= coarse_indirect_origin_frontier_;
  }

  // Returns the best kMaxSelectedFiles (8) files of the heap, ranked by
  // garbage ratio, highest first, with no garbage ratio limit. When `bytes` is
  // not null, it gets the bytes of each returned file.
  Selection Select(SelectionBytes* bytes = nullptr) const;
  Selection SelectEmpty() const;
  uint64_t Revision(uint64_t file_number) const;
  size_t size() const { return candidates_.size(); }
  uint64_t RelocationGarbageBytes() const { return relocation_garbage_bytes_; }

  // Totals over the physical files, for monitoring. Each file counts in one
  // group: suppressed (Suppress()), else pinned by the coarse frontier, else
  // collectable. Garbage is total bytes minus live bytes. The heap keeps them
  // up to date at each change, so reading them is O(1).
  struct Totals {
    uint64_t candidate_count = 0;
    uint64_t collectable_garbage_bytes = 0;
    uint64_t suppressed_garbage_bytes = 0;
    uint64_t coarse_pinned_files = 0;
    uint64_t coarse_pinned_bytes = 0;
    uint64_t coarse_pinned_garbage_bytes = 0;
  };
  Totals GetTotals() const {
    Totals totals = totals_;
    totals.candidate_count = heap_.size();
    return totals;
  }
  size_t MemoryUsage(size_t additional_candidates) const;

  // The files that GC suppressed because their data or metadata is corrupt
  // (Suppress() with `corrupt` true), for the property
  // rocksdb.indirect-blob-gc-stats. `count` is the number of such files, and
  // `files` holds the lowest kMaxReportedCorruptFiles (8) of their numbers,
  // with 0 in the unused entries. A short fixed list keeps the property small
  // when many files are corrupt.
  static constexpr size_t kMaxReportedCorruptFiles = 8;
  struct CorruptFileReport {
    uint64_t count = 0;
    std::array<uint64_t, kMaxReportedCorruptFiles> files{};
  };
  CorruptFileReport GetCorruptFileReport() const;

  // GC reports a file it could not use, and the heap takes the file out
  // (suppresses it), so that Select() picks other files. Suppress(file, 0) is a
  // normal suppression. It stays across Version changes that do not touch the
  // file. A change of the file's total bytes, live bytes, flags
  // (is_indirect_origin, is_lost) or coarse pin ends it at once. A census
  // rebuild also ends it, because the rebuild starts a new heap. A rise of the
  // coarse frontier ends the suppression of every suppressed relocation file
  // above the old frontier, whatever the reason for it, because the heap
  // stores only whether a file is corrupt, not why GC skipped it (see
  // SetCoarseIndirectOriginFrontier()).
  // SuppressTransient() is for transient failures: the file comes back after a
  // retry delay (1 s, doubled per failure, at most 32 s). `corrupt` marks a
  // file whose data or metadata is corrupt (see GetCorruptFileReport()).
  // RetryDue() brings back the files whose delay has passed and returns the
  // next retry time (0 if none).
  void Suppress(uint64_t file_number, uint64_t retry_micros,
                bool corrupt = false);
  void SuppressTransient(uint64_t file_number, uint64_t now_micros);
  uint64_t RetryDue(uint64_t now_micros);

  static int CompareRatios(uint64_t lhs_garbage, uint64_t lhs_total,
                           uint64_t rhs_garbage, uint64_t rhs_total);

 private:
  static constexpr size_t kNotInHeap = std::numeric_limits<size_t>::max();
  struct Candidate {
    uint64_t file_number = 0;
    uint64_t total_bytes = 0;
    uint64_t live_bytes = 0;
    uint64_t retry_micros = 0;
    uint64_t revision = 0;
    size_t position = kNotInHeap;
    bool is_indirect_origin = false;
    bool is_lost = false;
    bool suppressed = false;
    bool corrupt = false;
    bool coarse_pinned = false;
    uint8_t failures = 0;
  };
  static bool Before(const Candidate* lhs, const Candidate* rhs);
  void Swap(size_t lhs, size_t rhs);
  void Fix(size_t position);
  void Activate(uint64_t file_number, Candidate* candidate);
  void Remove(Candidate* candidate);
  static uint64_t RelocationGarbage(const Candidate& candidate);
  void AdjustRelocationGarbage(uint64_t before, uint64_t after);
  // Adds (`add` true) or removes the contribution of `candidate` to totals_.
  // Every change of a candidate calls it once before and once after.
  void AccountTotals(const Candidate& candidate, bool add);
  Totals totals_;
  std::map<uint64_t, Candidate> candidates_;
  // std::map node addresses are stable, so the heap can hold direct pointers.
  // Each heap comparison is then O(1), without a map lookup at every level.
  std::vector<Candidate*> heap_;
  // (retry time, file number) of each suppressed file with a retry time. A
  // timer tick reads only the due entries, not every blob file.
  std::set<std::pair<uint64_t, uint64_t>> retries_;
  // The numbers of the suppressed files with `corrupt` set.
  std::set<uint64_t> corrupt_files_;
  uint64_t next_revision_ = 1;
  uint64_t coarse_indirect_origin_frontier_ = 0;
  uint64_t relocation_garbage_bytes_ = 0;
  bool relocation_garbage_overflow_ = false;
};

// Dense ids (slots) for the SSTs that hold at least one census group. Each new
// SST gets the lowest free slot, so slot numbers stay small. Small slot numbers
// keep the holder lists short and the holder bitmaps short.
class BlobCensusSlotTable {
 public:
  static constexpr uint32_t kNoSlot = std::numeric_limits<uint32_t>::max();

  // Gives `file_number` the lowest free slot. Corruption if it has one.
  Status Assign(uint64_t file_number, uint32_t* slot);

  // Frees the slot of `file_number` and returns it. Corruption if it has none.
  Status Release(uint64_t file_number, uint32_t* slot);

  // kNoSlot if `file_number` has no slot.
  uint32_t Find(uint64_t file_number) const;

  // 0 if `slot` is free or out of range.
  uint64_t FileNumber(uint32_t slot) const {
    return slot < files_.size() ? files_[slot] : 0;
  }

  // The number of slots, U: one past the highest slot in use.
  uint32_t slot_count() const { return static_cast<uint32_t>(files_.size()); }
  size_t live_count() const { return slots_.size(); }
  size_t MemoryUsage() const;

 private:
  // Slot -> file number; 0 means free.
  std::vector<uint64_t> files_;
  UnorderedMap<uint64_t, uint32_t> slots_;
  // Bit (i % 64) of word i / 64 is set when slot i (< files_.size()) is free.
  std::vector<uint64_t> free_words_;
  // No free slot is below 64 * first_free_word_.
  size_t first_free_word_ = 0;
};

// The holder set of one generation number: the slots of the SSTs that hold the
// generation number.
//
// Encoding: a small set is a list. The list stores the slots in ascending order
// as varints: the first slot, then the gap to each next slot. A large set is a
// bitmap with one bit per slot. Its words end at the word of the highest set
// bit.
//
// Switch: let U be the number of slots (BlobCensusSlotTable::slot_count()). A
// full bitmap takes U/8 bytes. A list longer than U/8 bytes becomes a bitmap. A
// bitmap becomes a list again only when the list would be shorter than U/16
// bytes. The gap between the two limits keeps a set near one limit from
// switching back and forth on each change.
//
// Updates: a bitmap is updated in place. A list is encoded again at its exact
// size, so no spare capacity is charged.
class BlobCensusSlotSet {
 public:
  uint32_t count() const { return count_; }
  bool empty() const { return count_ == 0; }
  bool is_bitmap() const { return bitmap_; }
  bool Contains(uint32_t slot) const;

  // Appends every slot in ascending order.
  void AppendTo(std::vector<uint32_t>* slots) const;

  // Replaces the set by (set - removed) + added in one pass. Both inputs are
  // sorted and unique. Corruption if a removed slot is absent or an added slot
  // is already present after the removals. `slot_count` is U. `scratch` is
  // reused between calls.
  Status Update(const std::vector<uint32_t>& removed,
                const std::vector<uint32_t>& added, uint32_t slot_count,
                std::vector<uint32_t>* scratch);

  // Replaces the set by `slots` (sorted and unique).
  void Assign(const std::vector<uint32_t>& slots, uint32_t slot_count);

  // Heap bytes of the list or bitmap, by capacity.
  size_t MemoryUsage() const {
    return list_.capacity() + 8 * words_.capacity();
  }

 private:
  void Encode(const std::vector<uint32_t>& slots, uint32_t slot_count);

  // Update() of a bitmap: sets and clears bits in place, and encodes again only
  // when the set may cross the list/bitmap switch point.
  Status UpdateBitmap(const std::vector<uint32_t>& removed,
                      const std::vector<uint32_t>& added, uint32_t slot_count,
                      std::vector<uint32_t>* scratch);
  std::string list_;
  std::vector<uint64_t> words_;
  uint32_t count_ = 0;
  bool bitmap_ = false;
};

// The candidate revisions that an indirect blob GC job sees when it starts,
// and the token of the census worker at that time (worker_token). Each
// candidate gets a new revision when its total bytes, live bytes, flags
// (is_indirect_origin, is_lost) or coarse pin change. A later failure report
// (SuppressWorkerCandidates()) is applied only when the worker is the same and
// the file's revision is unchanged. A census rebuild starts a new worker with a
// new token, so a report from before the rebuild is ignored. An unrelated
// Version change does not cancel the report, and a report about an old state of
// the file is ignored.
struct BlobGcCandidateStamp {
  uint64_t worker_token = 0;
  BlobGcCandidateHeap::Selection files{};
  std::array<uint64_t, BlobGcCandidateHeap::kMaxSelectedFiles> revisions{};
};

// The state of one census build (see "Build" at the top of this file) between
// runs of the background job: the pinned starting Version and the next SST to
// read.
//
// The deleter of `starting_version` (set by DBImpl) takes the DB mutex to unref
// the Version and then releases the column family pin. So the last handle must
// be dropped without the DB mutex held. Cancel() only detaches this context;
// the caller drops it later (see BlobReferenceCensusGarbage).
//
// Locks: the thread that runs the worker owns every field (it holds
// BlobReferenceCensus::worker_run_mutex()). There are two exceptions:
//   - token is set before the context is shared and never changes, so any
//     thread may read it.
//   - candidate_retry_micros: the census job reads it under the DB mutex after
//     the run, so it is written only under the DB mutex.
struct BlobReferenceCensusBuildContext {
  // The census job reads the failure of the current context at the start of
  // every run (the first catch-up in DBImpl::BuildBlobReferenceCensus()) and
  // fails the build if it is set. A context that a new build replaced, that
  // Cancel() detached, or that is dropped at shutdown is never read again.
  // There, failure is stored state, not a dropped result, so its destructor
  // needs no check.
  ~BlobReferenceCensusBuildContext() { failure.PermitUncheckedError(); }

  std::shared_ptr<CacheReservationManager> reservation_manager;
  // Charges the summary of the SST being read. Each summary is dropped after
  // the worker applies its groups.
  std::shared_ptr<CacheReservationManager> summary_reservation_manager;
  std::shared_ptr<Version> starting_version;
  uint64_t token = 0;
  int level = 0;
  size_t next_file = 0;
  std::set<uint64_t> deleted_starting_files;
  // Summaries already in memory: the added SSTs of the rebuild install that
  // started this build (for example, the outputs of a compaction that could not
  // read an input summary). The scan uses them instead of reading the SST.
  std::map<uint64_t, std::shared_ptr<const BlobReferenceSstSummary>> seeds;
  std::list<std::shared_ptr<const BlobReferenceCensusDelta>> pending_deltas;
  bool scan_complete = false;
  // The time (DB clock, in microseconds) when the next suppressed GC candidate
  // may come back (BlobGcCandidateHeap::RetryDue()), or 0 if none waits.
  uint64_t candidate_retry_micros = 0;
  // A failure that may have changed the worker: of an apply after an install,
  // or of a census job run (see "Apply after an install" in this file). While
  // it is set, applies after an install skip this build, and the census job
  // fails the build with it.
  Status failure;

  Status ReserveInitialFiles(size_t file_count);
};

// Totals of one generation number over all SSTs that hold it.
struct BlobCensusGenerationTotals {
  uint64_t reference_count = 0;
  uint64_t stored_value_bytes = 0;
  uint64_t record_bytes = 0;
};

// A copy of part of the census at one Version: for each generation number in
// the copy, the SSTs that hold it (sorted by file number) and its totals.
//
// SnapshotSelected() copies only the generation numbers that an indirect blob
// GC job selected. The job owns its copy alone. BuildState() copies every
// generation number of a list of SST summaries; debug builds use it to check
// the worker.
class BlobReferenceCensusState {
 public:
  explicit BlobReferenceCensusState(uint64_t version_number)
      : version_number_(version_number) {}

  // A copy would share reservation_manager_, which is not thread safe. A job
  // owns its copy alone.
  BlobReferenceCensusState(const BlobReferenceCensusState&) = delete;
  BlobReferenceCensusState& operator=(const BlobReferenceCensusState&) = delete;

  uint64_t version_number() const { return version_number_; }
  size_t generation_count() const { return generations_.size(); }
  const std::vector<uint64_t>* FindSstFiles(uint64_t generation) const;

  // Null if no SST holds `generation`.
  const BlobCensusGenerationTotals* FindTotals(uint64_t generation) const;
  size_t GetSstCount(uint64_t generation) const;

  // Every SST that holds a selected generation number, sorted.
  std::vector<uint64_t> SstFileNumbers() const;

  // True if both have the same generation numbers, and for each one the same
  // SSTs and totals. Otherwise describes the first difference in `*difference`.
  bool SameMemberships(const BlobReferenceCensusState& other,
                       std::string* difference) const;

 private:
  friend class BlobReferenceCensus;

  struct GenerationEntry {
    std::vector<uint64_t> sst_files;
    BlobCensusGenerationTotals totals;
  };
  using SstFilesByGeneration = std::map<uint64_t, GenerationEntry>;

  uint64_t version_number_;
  SstFilesByGeneration generations_;
  // The generation numbers in this copy; null means every generation number.
  std::shared_ptr<const std::set<uint64_t>> selected_generations_;
  // The block cache charge of this copy
  // (CacheEntryRole::kIndirectBlobGcWorkingMemory, because indirect blob GC
  // jobs and route retirement own such copies), or null when the copy is not
  // charged. The charge is the real size of the copy (SnapshotMemoryBound()).
  std::shared_ptr<CacheReservationManager> reservation_manager_;
  size_t snapshot_memory_bound_ = 0;
};

// The census change of one Version install, from the Version
// `base_version_number` to the Version `version_number`.
struct BlobReferenceCensusDelta {
  uint64_t base_version_number = 0;
  uint64_t version_number = 0;
  std::vector<uint64_t> removed_files;
  // The groups of every removed exact SST, sorted by file number. An SST
  // without exact metadata needs no summary.
  std::vector<std::shared_ptr<const BlobReferenceSstSummary>> removed_summaries;
  // The summaries of the added exact SSTs, sorted by file number.
  std::vector<std::shared_ptr<const BlobReferenceSstSummary>> added_files;
  // True if the change cannot be applied as a delta, in one of these cases:
  //   - An exact SST was removed without its groups.
  //   - A new exact SST came without its prepared summary.
  // Installing it invalidates the census. The next build runs on the new
  // Version and uses `added_files` as seeds, so it does not read those SSTs.
  bool rebuild = false;
  // The relocation state (routes and relocation files) of the new Version, and
  // the routes and physical files that this install changes. The worker maps
  // each generation number to its physical file through the routes of the same
  // Version as the SST change, also when it applies the delta much later.
  std::shared_ptr<const BlobRelocationState> forward_state;
  std::vector<uint64_t> changed_routes;
  std::vector<uint64_t> changed_physical_files;
  std::vector<BlobGcPhysicalFileSummary> physical_files;
  size_t retained_forward_state_bytes = 0;
  uint64_t coarse_indirect_origin_frontier = 0;

  // An upper bound of the bytes that this delta keeps in memory while it waits
  // in the change log. It includes the delta's own vectors, every SST summary
  // that it holds, and the parts of the relocation state that no other
  // reservation charges (see BlobRelocationState::IncrementalRetentionBytes()).
  // It leaves out the file table of the relocation state, which the file
  // metadata reservation already charges. The build cursor counts this weight
  // against its limit (kMaxLogMemory, or kMaxRestartLogMemory after an overflow
  // restart), and the worker acknowledges a delta with the same weight
  // (AcknowledgeBuildDeltas()).
  size_t QueueWeight() const;

  // The part of QueueWeight() that the change log charges to the block cache
  // (see PrepareInstallWithCache()). It leaves out each summary that has its
  // own cache charge (reservation_handle set), so that memory is not charged
  // twice. The code that creates such a summary charges it: the table builder
  // for the summary of a new SST, or the compaction's input read
  // (CompactionJob::ReadInputBlobReferenceSummaries()) for the summary of a
  // removed SST. The charge stays with the summary object until its last owner
  // (the delta, a FileMetaData or the census) drops it.
  size_t ChargeBytes() const;
};

using BlobReferenceCensusDeltaQueue =
    std::list<std::shared_ptr<const BlobReferenceCensusDelta>>;

// One node of the change log: a singly linked list of installed deltas that the
// build cursor reads. Only the tail's `next` changes; the inner links never
// change.
struct BlobReferenceChangeLogEntry {
  explicit BlobReferenceChangeLogEntry(
      std::shared_ptr<const BlobReferenceCensusDelta> value,
      std::unique_ptr<CacheReservationManager::CacheReservationHandle> charge)
      : delta(std::move(value)),
        retained_bytes(delta->QueueWeight()),
        reservation(std::move(charge)) {}
  ~BlobReferenceChangeLogEntry();
  const std::shared_ptr<const BlobReferenceCensusDelta> delta;
  std::shared_ptr<BlobReferenceChangeLogEntry> next;
  size_t retained_bytes;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle> reservation;
  // The census counter of change log bytes (see
  // BlobReferenceCensus::StayingMemoryBytes()), set when the census links this
  // entry. The destructor subtracts retained_bytes from it.
  std::shared_ptr<std::atomic<uint64_t>> log_bytes;
};

struct BlobReferenceCensusPreparedInstall {
  std::shared_ptr<const BlobReferenceCensusDelta> delta;
  // One log entry, allocated before the install. Install() links it and does
  // not allocate.
  std::shared_ptr<BlobReferenceChangeLogEntry> log_entry;
  bool invalidate_census = false;
  uint64_t reservation_id = 0;
  size_t delta_queue_weight = 0;
};

// State that a census call under the DB mutex hands back to the caller. The
// caller must destroy it after it releases the DB mutex, because it can be as
// large as the census.
struct BlobReferenceCensusGarbage {
  // The replaced WorkerState, with its type hidden. It is taken without
  // worker_mutex_. A worker run that is still active keeps its own handle until
  // it returns.
  std::shared_ptr<void> worker_state;
  std::shared_ptr<BlobReferenceCensusBuildContext> build_context;
  std::vector<std::shared_ptr<const BlobReferenceSstSummary>> summaries;
  // Change log handles. The build cursor has kSlotsPerCursor (3) slots,
  // starting at kBuildCursorSlot (0): its first, acknowledged and delivered
  // handles, in that order. kLogTailSlot (3) holds the log tail. Dropping the
  // last handle of a log prefix frees it.
  static constexpr size_t kSlotsPerCursor = 3;
  static constexpr size_t kBuildCursorSlot = 0;
  static constexpr size_t kLogTailSlot = kSlotsPerCursor;
  std::array<std::shared_ptr<BlobReferenceChangeLogEntry>, kLogTailSlot + 1>
      log_entries;
};

// The census of one column family (see the top of this file). It owns the
// worker state, the change log with its build cursor, the ready state and the
// metadata-only commit state. The log mutex (`mutex_`) is never held while
// taking the DB mutex, or while parsing, allocating, copying or destroying
// state as large as the census.
class BlobReferenceCensus {
 public:
  // The limit of QueueWeight() bytes that the build cursor may have queued and
  // not acknowledged (256 MB, an estimate; "Bounded change log" at the top of
  // this file gives the reason). QueueWeight() counts all memory that a delta
  // keeps in the change log, also the summaries that have their own cache
  // charge, so this one limit bounds the change log. A cursor whose queued
  // weight has already reached the limit overflows at its next delta, and the
  // build restarts. A cursor below the limit accepts a delta of any size, so it
  // holds at most the limit plus one delta, and a large change still goes
  // through.
  static constexpr size_t kMaxLogMemory = 256 * 1024 * 1024;
  // The limit of the build after an overflow restart, and of every later build
  // until a build publishes (1 GB: 4 x kMaxLogMemory, an estimate, not tuned;
  // see "Bounded change log" at the top of this file).
  static constexpr size_t kMaxRestartLogMemory = 4 * kMaxLogMemory;
  // The number of deltas that one TakeBuildDeltas() call returns at most (64,
  // an estimate). A fixed array holds them, so the log mutex is held for a
  // bounded time.
  static constexpr size_t kMaxDeltasPerRead = 64;

  enum class PublishResult : uint8_t {
    kPublished,
    kQueueNotEmpty,
    kInstallPending,
    kStale,
  };

  BlobReferenceCensus() = default;
  BlobReferenceCensus(const BlobReferenceCensus&) = delete;
  BlobReferenceCensus& operator=(const BlobReferenceCensus&) = delete;

  // Metadata-only commit (see the top of this file). REQUIRES: DB mutex held.
  // BeginMetadataCommit() marks that one such commit is in progress, from the
  // Version preparation without the DB mutex until the MANIFEST sync. It
  // returns TryAgain while another commit is in progress or while a commit
  // result is unknown, and ColumnFamilyDropped after Cancel(). While a commit
  // is in progress, BeginGcJob() returns TryAgain.
  // EndMetadataCommit(false) means the commit result is unknown (for example,
  // after a failed MANIFEST write). BeginGcJob() then returns Busy, and
  // BeginMetadataCommit() returns TryAgain, until
  // ResolveMetadataCommitAmbiguity(). Flushes and compactions never wait for a
  // metadata-only commit.
  Status BeginMetadataCommit();
  void EndMetadataCommit(bool result_known);

  // REQUIRES: DB mutex held. Call only after a new MANIFEST and CURRENT file
  // were written and the Version was installed. Only this proves which state is
  // durable. A later successful MANIFEST append alone is not enough.
  void ResolveMetadataCommitAmbiguity();

  // REQUIRES: DB mutex held. Marks that a Version install of this column family
  // is being prepared (O(1)). While it is active, the worker does not publish
  // (TryPublishWorker() returns kInstallPending), because an install that it
  // has not seen is about to come. Install() or CancelInstallPreparation() ends
  // it and wakes a waiting worker.
  uint64_t BeginInstallPreparation();
  void CancelInstallPreparation(uint64_t reservation_id);

  // PrepareInstall() and PrepareInstallWithCache() prepare the log entry of
  // `delta` without the DB mutex. PrepareInstallWithCache() also charges its
  // memory to `block_cache` if it is not null. For a rebuild delta, both
  // prepare an invalidation instead. PrepareInstall() (no cache charge) is for
  // tests.
  Status PrepareInstall(std::shared_ptr<const BlobReferenceCensusDelta> delta,
                        uint64_t reservation_id,
                        BlobReferenceCensusPreparedInstall* prepared) const;
  Status PrepareInstallWithCache(
      std::shared_ptr<const BlobReferenceCensusDelta> delta,
      uint64_t reservation_id, std::shared_ptr<Cache> block_cache,
      BlobReferenceCensusPreparedInstall* prepared) const;

  // Prepares an invalidation for when PrepareInstall() fails. It does not
  // allocate. The Version install still goes ahead, and Install() makes the
  // census unready, so no old or partial census stays visible.
  Status PrepareInvalidation(
      uint64_t reservation_id,
      BlobReferenceCensusPreparedInstall* prepared) const;

  // REQUIRES: DB mutex held. Links the prepared log entry without allocation,
  // or invalidates the census. Returns the replaced state, which the caller
  // destroys without the DB mutex.
  BlobReferenceCensusGarbage Install(
      BlobReferenceCensusPreparedInstall& prepared);

  // REQUIRES: DB mutex held. Opens the build cursor. The caller pins the
  // current Version under the same DB mutex hold, so the cursor sees every
  // later install. The returned token identifies this build attempt.
  uint64_t BeginBuild();

  // Returns the next deltas of the build cursor, to apply without the DB mutex
  // and the log mutex.
  Status TakeBuildDeltas(uint64_t token, BlobReferenceCensusDeltaQueue* deltas,
                         bool* overflowed);

  // Releases the queue budget of deltas that the builder has applied. A delta
  // that was returned but not yet applied stays counted, so a slow initial scan
  // cannot build up an unbounded private backlog. The caller passes `weight`,
  // the sum of QueueWeight() of the deltas it applied, in order. The cursor
  // limit (kMaxLogMemory, or kMaxRestartLogMemory after an overflow restart) is
  // in bytes, so the acknowledgment is in bytes too. The census moves this
  // cursor past the log entries whose weights add up to this sum. An entry is
  // freed when no cursor, older entry or log tail still holds it.
  Status AcknowledgeBuildDeltas(uint64_t token, size_t weight);

  // True if the build cursor of build `token` has overflowed. A failed
  // AcknowledgeBuildDeltas() can mean an overflow. The census job then handles
  // it like an overflow that TakeBuildDeltas() reports (see
  // UseOverflowRestart()).
  bool BuildCursorOverflowed(uint64_t token) const;

  // REQUIRES: DB mutex held. Ends the build `token` after an error: the worker
  // state is dropped, the census becomes unready (GC pauses) and the build
  // cursor closes. Returns the replaced state, which the caller destroys
  // without the DB mutex.
  BlobReferenceCensusGarbage FailBuild(uint64_t token);

  // Stops the census when its column family goes away: closes metadata-only
  // commits, closes the build cursor, ends the GC job registration and the
  // snapshot pause, and drops the worker state, the build state and the rebuild
  // seeds. Returns the replaced state, which the caller destroys without the DB
  // mutex. REQUIRES: DB mutex held (or exclusive use in tests). Because of the
  // DB mutex, clearing the wakeup callback cannot race with EndMetadataCommit
  // or ResolveMetadataCommitAmbiguity.
  BlobReferenceCensusGarbage Cancel();

  // One thread at a time runs the census worker: the census job (one build
  // slice) or an installing thread that applies the queued deltas (see "Apply
  // after an install" at the top of this file). The installing thread only
  // tries the lock, so it never waits for a build slice. Take it before the DB
  // mutex, never while holding it.
  std::mutex& worker_run_mutex() { return worker_run_mutex_; }

  // True if the census is published for `version_number`.
  bool IsReady(uint64_t version_number) const;
  bool IsSnapshotPending() const;

  // REQUIRES: DB mutex held. Registers an indirect blob GC job at the ready
  // Version `version_number` and returns its token. At most one GC job is
  // registered per column family. The worker then does not apply deltas until
  // SnapshotSelected() has copied the selected generation numbers, or the job
  // ends. Returns TryAgain if the census is not ready at `version_number` or a
  // metadata commit (BeginMetadataCommit()) is in progress. Returns Busy while
  // a metadata commit result is unknown, or if a GC job is already registered.
  // Returns Aborted after Cancel().
  //
  // `candidate_files` are the file numbers of the physical files that the GC
  // job may take as input (its candidates). While the job is registered, route
  // retirement skips every route whose target is one of them (IsGcInputFile()).
  // Reason: the GC install checks, as an invariant, that the number of routes
  // into each relocation input is unchanged (see ValidatePhysicalInputs()). A
  // route deletion would break it: debug builds assert, and release builds end
  // the job as stale with nothing installed.
  Status BeginGcJob(uint64_t version_number,
                    std::vector<uint64_t> candidate_files, uint64_t* token);
  Status BeginGcJob(uint64_t version_number, uint64_t* token) {
    return BeginGcJob(version_number, {}, token);
  }

  // True while the GC job `token` is registered and the census is not
  // canceled.
  bool IsGcJobActive(uint64_t token) const;

  // True while any GC job is registered (see BeginGcJob()).
  bool HasGcJob() const;

  // REQUIRES: DB mutex held. Ends the GC job `token` (no-op for another
  // token). O(1), no allocation.
  void EndGcJob(uint64_t token);

  // True if `file_number` is one of the `candidate_files` of the registered
  // indirect blob GC job (see BeginGcJob()).
  bool IsGcInputFile(uint64_t file_number) const;

  // Route retirement batching (see DBImpl::RetireBlobRelocationRoutes()).
  // Returns the time (in microseconds) when routes that route retirement can
  // delete were first left pending, or 0 when none are pending.
  uint64_t RetirementPendingSinceMicros() const {
    return retirement_pending_since_micros_.load(std::memory_order_relaxed);
  }
  void SetRetirementPendingSinceMicros(uint64_t micros) {
    retirement_pending_since_micros_.store(micros, std::memory_order_relaxed);
  }

  // Builds a copy of every generation number from a list of SST summaries.
  static Status BuildState(
      uint64_t version_number,
      const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>& files,
      std::shared_ptr<BlobReferenceCensusState>* state);

  // The summaries handed over by the last rebuild install, for the next build.
  std::vector<std::shared_ptr<const BlobReferenceSstSummary>>
  TakeRebuildSeeds();

  // True from a rebuild install until the next BeginBuild(). A build slice that
  // fails because of that install restarts at once, without backoff.
  bool RebuildRequested() const;

  // Before indirect blob GC copies a blob file, it reads the references of that
  // file from the SSTs and checks them against the census. GC calls
  // ReportMismatch() when the SSTs or the totals differ from the census for
  // blob file `file_number`.
  //
  // Census mismatch state: ReportMismatch(), ClearMismatch() and
  // TakeMismatchReport() share the file of the last mismatch report ("last
  // file", unresolved_mismatch_file_, 0 means none) and two flags,
  // mismatch_reported_ and rebuild_requested_. TryPublishWorker() returns
  // kStale while either flag is set, so a mismatching census cannot become
  // ready again. BeginBuild() clears rebuild_requested_. In the table, f is the
  // file number that GC passes.
  //
  //   Call                  Condition          Action                 Returns
  //   --------------------  -----------------  ---------------------  -------
  //   ReportMismatch(f)     f == last file     count a suppression    true
  //                         and f != 0
  //   ReportMismatch(f)     otherwise          last file = f; census  false
  //                                            not ready; set both
  //                                            flags
  //   ClearMismatch(f)      f == last file     last file = 0          -
  //   ClearMismatch(f)      f != last file     nothing                -
  //   TakeMismatchReport()  -                  clear                  the old
  //                                            mismatch_reported_     flag
  //
  // After a first report, the next build slice rebuilds the census from the
  // current Version. A repeat report (true in the table) means that the rebuild
  // did not clear the mismatch. GC then suppresses that file until the file
  // changes, the next census rebuild, or (for a relocation file) a rise of the
  // coarse frontier (see BlobGcCandidateHeap::Suppress()). The census remembers
  // only the last file, so reports for two files that alternate both request a
  // rebuild each time.
  bool ReportMismatch(uint64_t file_number);

  // GC calls this for each checked input of a job when no input of that job
  // mismatched the SSTs. When `file_number` is the file of the last mismatch
  // report, the census forgets that report, so a later mismatch of the file
  // requests a rebuild again. A match of another file keeps the report, so a
  // match of one file does not hide a repeat mismatch of a different file.
  void ClearMismatch(uint64_t file_number);

  // Returns how many times GC suppressed a candidate because a census rebuild
  // did not clear its mismatch.
  uint64_t gc_mismatch_suppress_count() const {
    return gc_mismatch_suppress_count_.load(std::memory_order_relaxed);
  }

  // Returns true if a ReportMismatch() requested a rebuild since the last call,
  // and clears the flag. The census build slice calls it to fail the running
  // build.
  bool TakeMismatchReport();

  // After a change log overflow, the build restarts at once, one time only.
  // From then until a build publishes, each build has the larger limit
  // kMaxRestartLogMemory. Returns false if that restart was already used
  // without a publish, which means that the restarted build overflowed its
  // limit too. The caller then fails the build, and it retries after the normal
  // backoff (see "Bounded change log" at the top of this file).
  bool UseOverflowRestart();
  uint64_t overflow_restart_count() const {
    return overflow_restart_count_.load(std::memory_order_relaxed);
  }

  // Counts one rebuild caused by a commit that added an exact SST without its
  // prepared summary. The census is then rebuilt from disk instead of failing
  // the commit.
  void NoteMissingSummaryRebuild() {
    missing_summary_rebuild_count_.fetch_add(1, std::memory_order_relaxed);
  }
  uint64_t missing_summary_rebuild_count() const {
    return missing_summary_rebuild_count_.load(std::memory_order_relaxed);
  }

  // Monitoring (the property rocksdb.indirect-blob-gc-stats). Most of these
  // read and write relaxed atomics only. StayingMemoryBytes() also takes
  // worker_state_mutex_, and the corrupt file report calls take
  // published_corrupt_mutex_, each for a short time. None of them takes the
  // worker mutex, so a property read never waits for a running census build.
  //
  // An estimate of the census memory that stays: the worker state (its last
  // cache reservation size) plus the change log entries that the census holds.
  // It leaves out the short-lived build context, GC copies and the summaries
  // that a compaction reads.
  uint64_t StayingMemoryBytes() const;
  // Number of builds started (BeginBuild()).
  uint64_t build_count() const {
    return build_count_.load(std::memory_order_relaxed);
  }
  // The census is unready from an install, a build start or a build failure
  // until a publish. NoteUnready() keeps the DB clock time of the first such
  // event; NoteReady() clears it at a publish and returns it. A build start
  // also remembers its time, so the publish that ends the build can log how
  // long it took (TakeBuildStartMicros()).
  void NoteUnready(uint64_t now_micros) {
    uint64_t expected = 0;
    unready_since_micros_.compare_exchange_strong(expected, now_micros,
                                                  std::memory_order_relaxed);
  }
  uint64_t NoteReady() {
    return unready_since_micros_.exchange(0, std::memory_order_relaxed);
  }
  uint64_t UnreadySinceMicros() const {
    return unready_since_micros_.load(std::memory_order_relaxed);
  }
  // The candidate totals of the last publish (see BlobGcCandidateHeap::Totals),
  // and the relocation garbage bytes it published.
  void SetPublishedTotals(const BlobGcCandidateHeap::Totals& totals,
                          uint64_t relocation_garbage_bytes) {
    published_candidate_count_.store(totals.candidate_count,
                                     std::memory_order_relaxed);
    published_collectable_garbage_.store(totals.collectable_garbage_bytes,
                                         std::memory_order_relaxed);
    published_suppressed_garbage_.store(totals.suppressed_garbage_bytes,
                                        std::memory_order_relaxed);
    published_coarse_pinned_files_.store(totals.coarse_pinned_files,
                                         std::memory_order_relaxed);
    published_coarse_pinned_bytes_.store(totals.coarse_pinned_bytes,
                                         std::memory_order_relaxed);
    published_coarse_pinned_garbage_.store(totals.coarse_pinned_garbage_bytes,
                                           std::memory_order_relaxed);
    published_relocation_garbage_.store(relocation_garbage_bytes,
                                        std::memory_order_relaxed);
  }
  BlobGcCandidateHeap::Totals PublishedTotals() const {
    BlobGcCandidateHeap::Totals totals;
    totals.candidate_count =
        published_candidate_count_.load(std::memory_order_relaxed);
    totals.collectable_garbage_bytes =
        published_collectable_garbage_.load(std::memory_order_relaxed);
    totals.suppressed_garbage_bytes =
        published_suppressed_garbage_.load(std::memory_order_relaxed);
    totals.coarse_pinned_files =
        published_coarse_pinned_files_.load(std::memory_order_relaxed);
    totals.coarse_pinned_bytes =
        published_coarse_pinned_bytes_.load(std::memory_order_relaxed);
    totals.coarse_pinned_garbage_bytes =
        published_coarse_pinned_garbage_.load(std::memory_order_relaxed);
    return totals;
  }
  uint64_t PublishedRelocationGarbageBytes() const {
    return published_relocation_garbage_.load(std::memory_order_relaxed);
  }
  // The corrupt file report of the last publish. A small mutex keeps the count
  // and the list from two different publishes apart.
  void SetPublishedCorruptFiles(
      const BlobGcCandidateHeap::CorruptFileReport& report) {
    std::lock_guard<std::mutex> lock(published_corrupt_mutex_);
    published_corrupt_files_ = report;
  }
  BlobGcCandidateHeap::CorruptFileReport PublishedCorruptFiles() const {
    std::lock_guard<std::mutex> lock(published_corrupt_mutex_);
    return published_corrupt_files_;
  }
  void NoteBuildStart(uint64_t now_micros) {
    build_start_micros_.store(now_micros, std::memory_order_relaxed);
  }
  // Returns the start time of the build that a publish ends, or 0 if no build
  // started (NoteBuildStart()) since the last call.
  uint64_t TakeBuildStartMicros() {
    return build_start_micros_.exchange(0, std::memory_order_relaxed);
  }

  // Worker API. The census worker changes the worker state only through these
  // calls. The worker state is derived: it is never copied into a Version, and
  // an install does not copy it.
  //
  // Locks (see the lock order at the top of this file):
  //   - A call that takes worker_mutex_ must run without the DB mutex.
  //   - NeedsWorkerRun(), TryPublishWorker(), GetBuildContext() and
  //     TrySetBuildContext() take only the log mutex, so DBImpl may call them
  //     with the DB mutex held.
  //   - SetBuildWakeup() requires the DB mutex.
  //   - CaptureCandidateStamp(), SuppressWorkerCandidates(), SnapshotSelected()
  //     and ReadSelectedSnapshot() serve GC selection, not the worker. They
  //     take worker_mutex_, so they run without the DB mutex.
  Status ResetWorkerState(
      uint64_t token, uint64_t version_number,
      std::shared_ptr<const BlobRelocationState> forward_state,
      std::shared_ptr<Cache> block_cache);
  Status AddStartingFile(uint64_t token,
                         std::shared_ptr<const BlobReferenceSstSummary> file);

  // Builds each holder set once, after the initial scan and every delta taken
  // during the scan are applied. Call before the first publication.
  Status FinalizeWorkerBuild(uint64_t token);
  Status SetWorkerPhysicalFiles(
      uint64_t token, const std::vector<BlobGcPhysicalFileSummary>& files);
  Status SetWorkerCoarseIndirectOriginFrontier(uint64_t token,
                                               uint64_t frontier);
  BlobGcCandidateHeap::Selection SelectWorkerCandidates(
      uint64_t token, BlobGcCandidateHeap::SelectionBytes* bytes = nullptr);
  BlobGcCandidateHeap::Selection SelectWorkerEmptyCandidates(uint64_t token);

  // Returns up to 64 (DBImpl's route retirement batch size,
  // kBlobRouteRetirementBatchSize) routed generation numbers with no holder. It
  // skips generation numbers at or above the coarse frontier, and generation
  // numbers whose route target is an input of the running indirect blob GC job
  // (IsGcInputFile()). It does not know the running compactions: route
  // retirement checks them later (see DBImpl::RetireBlobRelocationRoutes()).
  // The result is in visit order: it starts after the retirement cursor and
  // wraps around. Unused entries are 0.
  using RetirementSelection = std::array<uint64_t, 64>;
  RetirementSelection SelectWorkerRetirementSources(uint64_t token);
  uint64_t WorkerRelocationGarbageBytes(uint64_t token) const;
  BlobGcCandidateHeap::Totals WorkerCandidateTotals(uint64_t token) const;
  BlobGcCandidateHeap::CorruptFileReport WorkerCorruptFileReport(
      uint64_t token) const;
  uint64_t RetryWorkerCandidates(uint64_t token, uint64_t now_micros);
  bool NeedsWorkerRun(uint64_t version_number) const;

  // REQUIRES: DB mutex held. Sets the callback that schedules the census job of
  // this column family (`db` and `cfd` are its arguments). The census stores
  // only these raw pointers and holds no reference to the DB or the column
  // family. Cancel() clears them. The callback only queues work and never
  // blocks a LOW pool thread. When the callback queues the column family,
  // DBImpl takes its own reference to that column family. The callback takes
  // the log mutex (see NeedsWorkerRun()), so callers run it after they release
  // the log mutex and metadata_commit_mutex_, still under the DB mutex.
  using BuildWakeup = void (*)(void*, void*);
  void SetBuildWakeup(BuildWakeup wakeup, void* db, void* cfd);
  Status CaptureCandidateStamp(uint64_t version_number,
                               const std::vector<uint64_t>& files,
                               BlobGcCandidateStamp* stamp) const;

  // Call without the DB mutex. GC reports the files it could not use. A report
  // from a replaced worker, or for a file whose revision changed, is ignored.
  // Files in `transient_files` get a retry delay; the other files wait for a
  // change (see BlobGcCandidateHeap::Suppress()).
  void SuppressWorkerCandidates(const BlobGcCandidateStamp& stamp,
                                const std::vector<uint64_t>& files,
                                const std::set<uint64_t>& transient_files,
                                uint64_t now_micros,
                                const std::set<uint64_t>& corrupt_files = {});
  Status ApplyWorkerDelta(uint64_t token,
                          const BlobReferenceCensusDelta& delta);
  uint64_t WorkerVersionNumber() const;
  PublishResult TryPublishWorker(uint64_t token,
                                 uint64_t applied_version_number,
                                 uint64_t current_version_number);
  std::shared_ptr<BlobReferenceCensusBuildContext> GetBuildContext() const;

  // Attaches the context only while its token is still the build token. The
  // census can be invalidated while the context is prepared without the DB
  // mutex. Then this returns false and the caller drops the context without the
  // DB mutex. A token whose cursor overflowed still attaches, so the normal
  // failure path cleans it up.
  bool TrySetBuildContext(
      std::shared_ptr<BlobReferenceCensusBuildContext> context);

  // Call without the DB mutex, after BeginGcJob() registered the job at
  // `version_number`. Copies the holders of `generation_numbers` only. The copy
  // does not block an install. A non-null `gc_block_cache` is charged for the
  // copy under CacheEntryRole::kIndirectBlobGcWorkingMemory.
  Status SnapshotSelected(uint64_t token, uint64_t version_number,
                          const std::set<uint64_t>& generation_numbers,
                          const std::shared_ptr<Cache>& gc_block_cache,
                          std::shared_ptr<BlobReferenceCensusState>* state);

  // Same copy as SnapshotSelected(), without the GC job checks.
  Status ReadSelectedSnapshot(
      uint64_t version_number, const std::set<uint64_t>& generation_numbers,
      const std::shared_ptr<Cache>& gc_block_cache,
      std::shared_ptr<BlobReferenceCensusState>* state) const;

  // Test only: builds a full copy of every generation number. Call without the
  // DB mutex.
  std::shared_ptr<const BlobReferenceCensusState> TEST_GetReadyState(
      uint64_t version_number) const;

 private:
  struct WorkerState;
  static bool IsCensusGroup(const BlobReferenceGroup& group);

  // True if `worker` is the worker of build `token` and that build is still
  // active, so a run of a replaced build cannot change the worker of a newer
  // build. REQUIRES: worker_mutex_ held.
  bool IsActiveWorker(const WorkerState* worker, uint64_t token) const;

  // Sums the census groups of `file` per generation number, only for
  // `selected_names` when it is not null.
  static Status SumGroupsByGeneration(
      const BlobReferenceSstSummary& file,
      const std::set<uint64_t>* selected_names,
      std::map<uint64_t, BlobCensusGenerationTotals>* totals);
  static Status AddFile(const BlobReferenceSstSummary& file,
                        BlobReferenceCensusState* state);
  Status CopyWorkerSnapshot(
      const WorkerState& worker, uint64_t version_number,
      const std::set<uint64_t>& generation_numbers,
      const std::shared_ptr<Cache>& gc_block_cache,
      std::shared_ptr<BlobReferenceCensusState>* state) const;
  static size_t SnapshotMemoryBound(const BlobReferenceCensusState& state);

  // A reader position in the change log.
  //   - first: the entry where delivery starts while nothing is acknowledged.
  //     It is set when an entry is appended while nothing is delivered. The
  //     first acknowledgment clears it; after that, `acknowledged` marks the
  //     position.
  //   - delivered: the last entry that TakeBuildDeltas() returned.
  //   - acknowledged: the last entry that the reader acknowledged.
  //   - outstanding_bytes: the QueueWeight() of the entries appended to this
  //     cursor and not yet acknowledged.
  //   - overflowed: a delta arrived when the cursor already held its limit
  //     (kMaxLogMemory, or kMaxRestartLogMemory when restart_build is set), and
  //     the cursor was detached.
  struct ChangeLogCursor {
    uint64_t token = 0;
    std::shared_ptr<BlobReferenceChangeLogEntry> first;
    std::shared_ptr<BlobReferenceChangeLogEntry> acknowledged;
    std::shared_ptr<BlobReferenceChangeLogEntry> delivered;
    size_t outstanding_bytes = 0;
    bool overflowed = false;
    // Set for the builds after an overflow restart: their limit is
    // kMaxRestartLogMemory instead of kMaxLogMemory. A publish clears it.
    bool restart_build = false;
  };
  static bool HasPending(const ChangeLogCursor& cursor);
  static void DetachCursor(ChangeLogCursor* cursor,
                           BlobReferenceCensusGarbage* garbage,
                           size_t first_slot);
  void AppendChange(const BlobReferenceCensusPreparedInstall& prepared,
                    BlobReferenceCensusGarbage* garbage);

  // One record per generation number: its totals, its holder set, and the
  // physical file that holds its values now. `record_bytes` is the sum
  // of BlobReferenceGroup::record_bytes over the census groups with this
  // generation number (not kDirect groups, which the census does not count).
  struct WorkerGeneration {
    BlobCensusSlotSet holders;
    // Slot changes made during the initial build. Finalize() applies them to
    // `holders` in one pass when the build completes.
    std::vector<uint32_t> pending_added;
    std::vector<uint32_t> pending_removed;
    uint64_t reference_count = 0;
    uint64_t stored_value_bytes = 0;
    uint64_t record_bytes = 0;
    uint64_t physical_file_number = 0;
  };
  struct WorkerState {
    uint64_t token = 0;
    uint64_t version_number = 0;
    std::map<uint64_t, WorkerGeneration> generations;
    BlobCensusSlotTable slots;
    std::shared_ptr<const BlobRelocationState> forward_state;

    // Generation numbers that have no holder but still have a route. Route
    // retirement (DBImpl::RetireBlobRelocationRoutes()) can delete those
    // routes.
    std::set<uint64_t> retirement_generations;

    // The last generation number that the previous retirement selection
    // visited. The next selection starts after it in `retirement_generations`
    // and wraps around.
    uint64_t retirement_cursor = 0;

    // Adds generation number `generation` to retirement_generations if
    // forward_state has a route for it. The caller has found that no SST holds
    // `generation`.
    void MarkRetirementIfRouted(uint64_t generation);
    std::shared_ptr<Cache> block_cache;
    std::shared_ptr<CacheReservationManager> reservation_manager;
    // False until Finalize() at the end of the initial build. Before that, a
    // removed SST without a slot is a starting SST the scan has not reached.
    bool finalized = false;
    // Heap bytes of every holder set and pending slot vector, by capacity.
    size_t slot_set_bytes = 0;
    // The size of the last Reserve() call, for StayingMemoryBytes(). Only the
    // worker writes it; a property read loads it without a lock.
    std::atomic<size_t> reserved_bytes{0};
    std::vector<uint32_t> scratch;
    BlobGcCandidateHeap candidates;

    size_t MemoryUsage(size_t additional_slots,
                       size_t additional_candidates) const;
    Status Reserve(size_t additional_slots);
    Status Reserve(size_t additional_slots, size_t additional_candidates);

    // Removes the groups of `removed` and adds the groups of `added` as one
    // change. A removal must match what was added; anything else is Corruption.
    Status ApplyFiles(
        const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>&
            removed,
        const std::vector<std::shared_ptr<const BlobReferenceSstSummary>>&
            added);
    Status Finalize();
    static size_t GenerationBytes(const WorkerGeneration& entry);
    uint64_t ResolvePhysical(uint64_t generation) const;
    Status AdjustGenerationBytes(const WorkerGeneration& entry, bool add);
  };
  // Never take this mutex while holding the DB mutex. Changes to the worker
  // state and the selected copies for GC take it. Install and the ready checks
  // use atomics and the log mutex instead.
  mutable std::mutex worker_mutex_;
  // See worker_run_mutex().
  std::mutex worker_run_mutex_;
  // See "Threads and locks" at the top of this file.
  mutable std::mutex metadata_commit_mutex_;
  bool metadata_commit_pending_ = false;
  bool metadata_commit_ambiguous_ = false;
  bool metadata_commit_canceled_ = false;
  // Read and replace only with LoadWorkerState() and ExchangeWorkerState().
  std::shared_ptr<WorkerState> LoadWorkerState() const;
  std::shared_ptr<WorkerState> ExchangeWorkerState(
      std::shared_ptr<WorkerState> next);
  // Guards worker_state_ only (see "Threads and locks").
  mutable std::mutex worker_state_mutex_;
  std::shared_ptr<WorkerState> worker_state_;
  std::atomic<uint64_t> worker_active_token_{0};
  std::atomic<uint64_t> worker_ready_version_{0};
  std::atomic<bool> candidate_refresh_requested_{false};
  bool snapshot_pending_ = false;           // protected by the log mutex
  bool build_waiting_for_install_ = false;  // protected by the log mutex
  // Protected by the DB mutex; cleared before owner teardown.
  BuildWakeup build_wakeup_ = nullptr;
  void* build_wakeup_db_ = nullptr;
  void* build_wakeup_cfd_ = nullptr;

  mutable std::mutex mutex_;
  // One shared, thread-safe reservation for all log entries, so many small
  // entries share one cache charge unit. Each entry holds a handle. The build
  // cursor releases a handle, always without the DB mutex, and the handle keeps
  // the manager alive until it is destroyed.
  mutable std::shared_ptr<CacheReservationManager> log_reservation_manager_;
  mutable std::shared_ptr<Cache> log_block_cache_;
  std::shared_ptr<BlobReferenceCensusBuildContext> build_context_;
  // The last entry. Only its `next` link changes, and this pointer keeps it
  // alive. It is detached when the build cursor is drained or closed, and
  // released without the DB mutex and the log mutex. The inner links never
  // change, so freeing a prefix without a lock cannot race with an append.
  std::shared_ptr<BlobReferenceChangeLogEntry> log_tail_;
  ChangeLogCursor build_cursor_;
  // The token of the registered GC job, or 0 (see BeginGcJob()).
  uint64_t gc_job_token_ = 0;
  // The `candidate_files` of the registered indirect blob GC job (see
  // BeginGcJob()), sorted. Empty when no GC job is registered.
  std::vector<uint64_t> gc_input_files_;
  std::atomic<uint64_t> retirement_pending_since_micros_{0};
  uint64_t next_build_token_ = 1;
  uint64_t next_reservation_id_ = 1;
  std::atomic<uint64_t> active_install_reservation_{0};
  uint64_t next_gc_token_ = 1;
  bool canceled_ = false;
  std::vector<std::shared_ptr<const BlobReferenceSstSummary>> rebuild_seeds_;
  bool rebuild_requested_ = false;
  bool mismatch_reported_ = false;
  // The file of the last mismatch report. ClearMismatch() of this file resets
  // it to 0 (none).
  uint64_t unresolved_mismatch_file_ = 0;
  std::atomic<uint64_t> gc_mismatch_suppress_count_{0};
  // An overflow restart ran and no build has published since. While it is set,
  // BeginBuild() gives the build cursor the restart limit
  // (kMaxRestartLogMemory).
  bool overflow_restart_used_ = false;
  std::atomic<uint64_t> overflow_restart_count_{0};
  std::atomic<uint64_t> missing_summary_rebuild_count_{0};
  // See StayingMemoryBytes(). Shared with the linked log entries, which may be
  // freed after the census.
  std::shared_ptr<std::atomic<uint64_t>> log_bytes_ =
      std::make_shared<std::atomic<uint64_t>>(0);
  std::atomic<uint64_t> build_count_{0};
  std::atomic<uint64_t> unready_since_micros_{0};
  std::atomic<uint64_t> build_start_micros_{0};
  std::atomic<uint64_t> published_candidate_count_{0};
  std::atomic<uint64_t> published_collectable_garbage_{0};
  std::atomic<uint64_t> published_suppressed_garbage_{0};
  std::atomic<uint64_t> published_coarse_pinned_files_{0};
  std::atomic<uint64_t> published_coarse_pinned_bytes_{0};
  std::atomic<uint64_t> published_coarse_pinned_garbage_{0};
  std::atomic<uint64_t> published_relocation_garbage_{0};
  mutable std::mutex published_corrupt_mutex_;
  BlobGcCandidateHeap::CorruptFileReport published_corrupt_files_;
};

}  // namespace ROCKSDB_NAMESPACE
