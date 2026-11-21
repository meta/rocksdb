//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <algorithm>
#include <array>
#include <cinttypes>
#include <deque>
#include <limits>
#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "cache/cache_reservation_manager.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_reference_census.h"
#include "db/blob/blob_relocation_file.h"
#include "db/blob/blob_relocation_gc.h"
#include "db/blob/blob_relocation_manifest.h"
#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "logging/logging.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "options/options_helper.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/adaptive/adaptive_table_factory.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/embedded_blob_sst.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/concurrent_task_limiter_impl.h"
#include "util/defer.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// PlanLiveSetReads() walks the census SST list of each generation number of the
// inputs. It checks for shutdown or a compaction abort at the first SST of each
// list, and then once every 1024 SSTs (when the index in the list is a multiple
// of 1024). ReadLiveSets() checks before each SST it reads. The interval of
// 1024 is an estimate, not measured.
constexpr uint64_t kIndirectBlobGCAbortCheckMask = (1 << 10) - 1;

// Stored bytes of one read batch (1 MB, an estimate). A value larger than this
// is streamed in kBlobRelocationStreamChunkSize (256 KB) chunks instead. One
// coalesced read is also at most this size.
constexpr uint64_t kIndirectBlobGCReadBatchBytes = 1024 * 1024;
// kIndirectBlobGCBatchBytesPerValue (512 bytes, an estimate) is the fixed
// memory of one value in a read batch, beyond its stored bytes: the request,
// status and payload entries, and the record trailer. With the 1 MB batch, this
// bounds a batch to at most 2048 values.
constexpr uint64_t kIndirectBlobGCBatchBytesPerValue = 512;
// At most 8 inputs per indirect blob GC job (a fixed limit, not tuned). It
// matches kBlobGcMaxSelectedFiles (8), the number of candidates that the census
// publishes to the Version.
constexpr size_t kIndirectBlobGCMaxInputs = 8;
// At most 8 outputs per indirect blob GC job (a fixed limit, not tuned). When
// the inputs need more outputs, the largest input waits for a later job. The
// estimated size of each output is at most the target size (see
// kIndirectBlobGCMaxOutputBytes). An input with a larger estimate gets an
// output of its own. The SstFileManager space check reserves the estimated size
// of all outputs, capped at the total input size.
constexpr size_t kIndirectBlobGCMaxOutputs = 8;
// Cap for blob_file_size in indirect blob GC (1 GB, a fixed choice, not
// measured). The target output size is 90% of min(blob_file_size,
// kIndirectBlobGCMaxOutputBytes). The 90% is a fixed choice that leaves room
// for error in the output size estimate. An input larger than the target gets
// an output of its own, never larger than the input.
constexpr uint64_t kIndirectBlobGCMaxOutputBytes = uint64_t{1} << 30;

Status EstimateIndirectBlobGCPartitionBytes(uint64_t physical_live_record_bytes,
                                            uint64_t reference_count,
                                            uint64_t* estimated_bytes) {
  if (estimated_bytes == nullptr) {
    return Status::InvalidArgument("Null blob GC output size estimate");
  }
  // Output size estimate: 64 KB for the table tail and index, plus 48 bytes per
  // value for its share of the location entries (estimates). One location
  // entry packs up to 32 values of one origin, so a value needs much less than
  // 48 bytes. The high value keeps the estimate on the safe side.
  constexpr uint64_t kFixedOverhead = 64 * 1024;
  constexpr uint64_t kPerEntryOverhead = 48;
  if (reference_count >
          (std::numeric_limits<uint64_t>::max() - kFixedOverhead) /
              kPerEntryOverhead ||
      physical_live_record_bytes > std::numeric_limits<uint64_t>::max() -
                                       kFixedOverhead -
                                       reference_count * kPerEntryOverhead) {
    return Status::MemoryLimit("Blob relocation output estimate overflow");
  }
  *estimated_bytes = physical_live_record_bytes + kFixedOverhead +
                     reference_count * kPerEntryOverhead;
  return Status::OK();
}

void RecordAtomicFlushRequestReason(Statistics* stats,
                                    FlushReason flush_reason) {
  // Keep this aligned with the existing rocksdb.flush.reason.* counters: they
  // cover automatic flush triggers used for write-stall debugging. Other
  // FlushReason values are counted by the catch-all other ticker.
  switch (flush_reason) {
    case FlushReason::kWriteBufferFull:
      RecordTick(stats, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL);
      break;
    case FlushReason::kWriteBufferManager:
      RecordTick(stats, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER);
      break;
    case FlushReason::kMemtableMaxRangeDeletions:
      RecordTick(stats,
                 ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS);
      break;
    default:
      RecordTick(stats, ATOMIC_FLUSH_REQUEST_REASON_OTHER);
      break;
  }
}

Status TableBuilderNotSupported(const ColumnFamilyData& cfd) {
  return Status::NotSupported(
      "Column family's table factory cannot build tables", cfd.GetName());
}

std::vector<uint64_t> MemTableIds(const autovector<ReadOnlyMemTable*>& mems) {
  std::vector<uint64_t> ids;
  ids.reserve(mems.size());
  for (const ReadOnlyMemTable* mem : mems) {
    ids.push_back(mem->GetID());
  }
  return ids;
}

// The block cache to charge for indirect blob GC working memory, or null when
// there is no block-based writer cache or charging for the role is disabled.
std::shared_ptr<Cache> GetIndirectBlobGcWorkingMemoryCache(
    const MutableCFOptions& mutable_cf_options) {
  const BlockBasedTableOptions* const options =
      GetBlockBasedTableWriterOptions(mutable_cf_options.table_factory.get());
  if (options == nullptr || options->block_cache == nullptr) {
    return nullptr;
  }
  const CacheEntryRoleOptions::Decision charged =
      options->cache_usage_options.options_overrides
          .at(CacheEntryRole::kIndirectBlobGcWorkingMemory)
          .charged;
  return charged == CacheEntryRoleOptions::Decision::kEnabled ||
                 charged == CacheEntryRoleOptions::Decision::kFallback
             ? options->block_cache
             : nullptr;
}

}  // namespace

// Background: indirect blob GC collects whole blob files (normal blob files
// or relocation files) into new relocation files, without an LSM compaction.
// The overview at the top of db/blob/blob_relocation_gc.h lists the job steps
// and says why GC suppresses inputs. "Suppressed inputs" below lists every
// reason.
//
// DBImpl::RunIndirectBlobGCBatch() checks its arguments, creates one
// IndirectBlobGCJob on its stack and calls Run(). Run() calls one method per
// phase, in order. The job reads DBImpl private members through db_.
//
// Terms: input, generation numbers of an input, input Version, census snapshot
// and stale job are defined in the overview. Two more terms:
// - GC job registration: the job registers with the census
//   (BlobReferenceCensus::BeginGcJob()) and stays registered until Finish()
//   (EndGcJob()). The census worker does not apply deltas while the job copies
//   its census snapshot, and at most one GC job is registered per census. The
//   job does not follow the Version changes after its snapshot (see "Install
//   protocol" in Install()).
// - Candidate stamp (BlobGcCandidateStamp): the census worker and the file
//   revision of each candidate when the job starts. The census ignores a
//   suppression with a stamp from an older census worker or an older file
//   revision (see BlobGcCandidateStamp).
//
// State: the phases share their state through the data members below. A data
// member has the name that the phase code uses for it, so the data members
// have no trailing underscore.
//
// Job status: the job has two status channels. The return value of Start() says
// only whether the job began: when it is not OK, or when gc_job_token is 0, the
// job did not begin, and Run() returns that value at once. After Start() began
// the job, the data member `status` is the job result until Run() returns.
// Start() can set it to an error and still return OK. Each phase after Start()
// also runs when `status` is not OK. Its loops check `status` first, so the
// phase does no new work, but it still runs its cleanup and its test sync
// points. Install() and Finish() decide what the job returns.
//
// Input outcomes: the outcome of one input is recorded in the places below.
// For each outcome, the list says where it is recorded, which methods set it,
// and which methods read it.
// - Accepted (relocate, or remove when it has no live value): eligible_inputs
//   and PhysicalInput::eligible. DecideInputs() sets them. PackOutputs(),
//   OpenInputReaders(), WriteOutputs() and Install() read them.
// - Suppressed: suppressed_files and suppression_reasons, two parallel vectors
//   that each phase must keep in step. Any phase adds to them. Install() and
//   Finish() read them. When `status` is CompactionTooLarge, Finish() adds
//   capacity_deferred_files to suppressed_files (reason "deferred: outputs or
//   disk space") and to transient_suppressions. Finish() also adds every input
//   in corrupt_inputs that is not in suppressed_files yet (reason "corrupt
//   input"). Install() runs before Finish(), so it does not log these reasons
//   and does not suppress these inputs in the Version. "Suppressed inputs"
//   below lists the reasons and says where GC records them.
// - Suppressed with a backoff delay: also in transient_suppressions. Start(),
//   SelectInputs(), DecideInputs() and Finish() add to it. Finish() reads it.
// - Failed in the read phase (only this input fails): InputPlan::failed.
//   PlanLiveSetReads() and ReadLiveSets() set it (fail_input()). ReadLiveSets()
//   skips the reads of a failed input, and DecideInputs() suppresses it. When
//   DecideInputs() does not reach a failed input, Finish() suppresses it only
//   if it is corrupt (see corrupt_inputs).
// - Waits for a later job (memory budget): deferred_input and plan_end.
//   PlanLiveSetReads() sets both. DecideInputs() and PackOutputs() also set
//   deferred_input. ReadLiveSets() and DecideInputs() handle only the first
//   plan_end inputs. Install() reads deferred_input and schedules the column
//   family again.
// - Waits for a later job (outputs or disk space): capacity_deferred_files.
//   PackOutputs() sets it, and also sets deferred_input. Install() and Finish()
//   read it.
// - Candidate-local failure (ends the whole job): candidate_local_failure_input
//   and candidate_local_failure_reason. OpenInputReaders() and WriteOutputs()
//   set them. WriteOutputs() reads them and adds the input to suppressed_files.
//
// Suppressed inputs: Finish() reports every suppressed input to the census
// with the candidate stamp (BlobReferenceCensus::SuppressWorkerCandidates()),
// also when the job fails. Install() also suppresses the inputs in the current
// Version (VersionStorageInfo::SuppressBlobFileForIndirectBlobGC()), but only
// when the job's decisions are still true for that Version:
// - When the job ends without error and has no input to install, Install()
//   suppresses them at once.
// - When the job installs, Install() suppresses them only if no other Version
//   was installed during the job, because a newer Version can hold SST
//   metadata that makes an input eligible. Install() also suppresses each new
//   output in the current Version, because a new output has no garbage yet.
// - A stale or failed job does not suppress anything in the Version. When the
//   job fails with CompactionTooLarge and the Version did not change, Install()
//   moves the deferred inputs to the end of the Version's candidate list
//   (VersionStorageInfo::DeferBlobFilesForIndirectBlobGC()).
// The table lists every reason and the kind of its census suppression (see
// "Suppression" in the overview). "Every input" means every input of the job,
// and "every candidate" means every file that the job was given.
//
//   Reason                                          Kind
//   ----------------------------------------------  --------------------------
//   Start(): the census snapshot is over its        transient, every candidate
//     memory budget or cannot be charged
//   SelectInputs(): a coarse SST pins a             normal
//     generation number of the input
//   SelectInputs(): invalid input metadata (0       normal
//     physical bytes or 0 record bytes)
//   SelectInputs(): the I/O workspace cannot be     transient, every input
//     charged to the block cache
//   SelectInputs(): the table format is not         normal, every input
//     block-based
//   PlanLiveSetReads(): the census reference        normal
//     count overflows
//   ReadLiveSets() or DecideInputs(): corrupt       normal
//     exact SST metadata
//   ReadLiveSets(): a census mismatch remains       normal
//     after a rebuild
//   PlanLiveSetReads() or DecideInputs(): the live  transient
//     set is over the GC memory budget
//     (BlobRelocationMemoryTracker::kMaxBytes, 64
//     MB) and no earlier input holds a share
//   ReadLiveSets(): the SST reader or its           transient, every planned
//     reference index cannot be charged to the        input
//     GC memory budget
//   DecideInputs(): exact live bytes above the      normal
//     input's record bytes
//   DecideInputs(): a lost relocation file that     normal
//     SSTs still reference
//   DecideInputs(): enable_blob_files,              normal
//     enable_blob_indirection or
//     enable_blob_garbage_collection is off in the
//     latest options, and the input has live
//     values
//   DecideInputs(): exact garbage below             normal
//     indirect_blob_gc_garbage_ratio
//   DecideInputs(): the output size estimate        normal
//     overflows
//   DecideInputs(): an unexpected Corruption or     normal, every accepted
//     MemoryLimit ends the decide step                input (transient for
//                                                     MemoryLimit)
//   PackOutputs(): not enough disk space even for   transient, every deferred
//     one input (CompactionTooLarge)                  input
//   OpenInputReaders(): the input cannot be         normal
//     opened (MemoryLimit, NotSupported or
//     Corruption; with paranoid_checks true, a
//     Corruption also fails the job)
//   WriteOutputs(): a source record is too large,   normal
//     unsupported or corrupt (with
//     paranoid_checks true, a corrupt record also
//     fails the job)
//   WriteOutputs(): the outputs would not be        normal, every accepted
//     smaller than the inputs                         input
//   WriteOutputs(): an output file is larger than   normal, first input of
//     its inputs                                      that output
//
// Output, I/O and MANIFEST errors are not candidate-local. They use the normal
// background-error handling.
//
// Lifecycle of the memory charges: the job charges most of its working memory
// to memory_tracker. The total is at most
// BlobRelocationMemoryTracker::kMaxBytes (64 MB). The charges are:
// - I/O workspace: the buffers that the job uses to read and stream the input
//   values. Its charge is 2 x kIndirectBlobGCReadBatchBytes (1 MB each) plus
//   2 x kBlobRelocationStreamChunkSize (256 KB each), taken in SelectInputs().
//   Its share is held in a local variable (io_workspace_bytes), so it is freed
//   only when the last copy of memory_tracker goes away, when the job is
//   destroyed. The release Defers below keep copies of memory_tracker.
// - Planned read list (group_reads): taken in PlanLiveSetReads(). It grows by
//   doubling, from 16 entries. release_group_read_charge frees it when the job
//   is destroyed, after Run() returns, with the DB mutex held.
// - SST reader (selected_reader_bytes): 576 KB plus the decoded reference
//   index, taken in ReadLiveSets() for one SST at a time. The 576 KB is
//   kReaderBytes in ReadLiveSets(): a 64 KB reader estimate plus 2 x 256 KB
//   read batches. It is freed after each SST and in Finish().
//   release_selected_reader_charge is only a safety net.
// - Live sets: 40 bytes per census reference, reserved in PlanLiveSetReads().
//   WriteOutputs() frees them. PlanLiveSetReads() frees the live set of a
//   deferred or failed input at once.
// The census snapshot is charged to the same cache role, but not to
// memory_tracker. It is taken in Start(), and Finish() frees it. Neither
// release Defer is set when Start() ends the job.
class DBImpl::IndirectBlobGCJob {
 public:
  // The arguments are those of RunIndirectBlobGCBatch(), already checked.
  IndirectBlobGCJob(
      DBImpl* db, ColumnFamilyData* cfd_arg,
      const std::vector<uint64_t>& candidate_physical_file_numbers_arg,
      bool* made_progress_arg, uint64_t* sfm_reserved_bytes_arg,
      bool* retryable_stale_input_arg, LogBuffer* log_buffer_arg,
      int job_id_arg)
      : db_(db),
        cfd(cfd_arg),
        candidate_physical_file_numbers(candidate_physical_file_numbers_arg),
        made_progress(made_progress_arg),
        sfm_reserved_bytes(sfm_reserved_bytes_arg),
        log_buffer(log_buffer_arg),
        job_id(job_id_arg),
        column_family(*cfd_arg),
        stale_input_is_retryable(*retryable_stale_input_arg) {}

  // Runs the job and returns its status. The contract (DB mutex, out
  // parameters, return codes) is on RunIndirectBlobGCBatch() in db_impl.h.
  // Run() calls the methods below in this order. The word in brackets is the
  // name of the job step in the overview of db/blob/blob_relocation_gc.h that
  // the method belongs to. The first job step (the candidate pick) runs before
  // Run(), in BackgroundCompaction().
  // 1. Start() [Plan]: check the census and the input Version, take a reference
  //    to the input Version, register the GC job and release the DB mutex. Then
  //    take the candidate stamp and the census snapshot. When the job cannot
  //    begin, Run() returns at once.
  // 2. SelectInputs() [Plan]: build the inputs and their generation numbers,
  //    and suppress inputs with bad metadata or inputs that a coarse SST pins
  //    (see the coarse pin comment in SelectInputs()).
  // 3. PlanLiveSetReads() [Plan]: plan the SST reads of each input and reserve
  //    its live set.
  // 4. ReadLiveSets() [Read, Check]: read the planned generation numbers SST by
  //    SST and check the census totals.
  // 5. DecideInputs() [Decide]: decide each input in order.
  // 6. PackOutputs() [Pack]: pack the inputs into outputs, check the disk
  //    space, and get the output file numbers.
  // 7. OpenInputReaders() [Copy]: open a reader for each input with live
  //    values.
  // 8. WriteOutputs() [Copy]: write, sync and rename the outputs, then release
  //    the readers and the live sets.
  // 9. Install() [Install]: take the DB mutex, build the MANIFEST edit and
  //    install it with one LogAndApply() call.
  // 10. Finish() [no job step, cleanup]: end the GC job registration, release
  //     the input Version, suppress the skipped inputs, delete the outputs that
  //     are not installed, and schedule the census worker.
  //
  // REQUIRES: DB mutex held. Returns with the DB mutex held.
  Status Run();

 private:
  // One indirect blob GC input: a normal blob file or a relocation file.
  // `generations` holds its generation numbers: the input itself and, for a
  // relocation file, every old file routed to it.
  struct PhysicalInput {
    uint64_t file_number = 0;
    uint64_t physical_bytes = 0;
    uint64_t total_record_bytes = 0;
    std::shared_ptr<BlobFileMetaData> normal_metadata;
    std::shared_ptr<BlobRelocationFileMetaData> relocation_metadata;
    // Sorted: the input and the old files routed to it at job start.
    std::vector<uint64_t> generations;
    // Sorted: the old files routed to the input at job start.
    std::vector<uint64_t> routed_generations;
    std::unique_ptr<BlobRelocationLiveSet> live_set;
    size_t live_count = 0;
    uint64_t physical_live_record_bytes = 0;
    uint64_t estimated_bytes = 0;
    size_t output_index = std::numeric_limits<size_t>::max();
    bool eligible = false;
  };
  struct OutputState {
    uint64_t file_number = 0;
    std::string path;
    std::string temporary_path;
    std::string checksum_method;
    std::string checksum_value;
    BlobRelocationFileMetadata metadata;
    BlobRelocationDirectory directory;
    uint64_t file_size = 0;
    uint64_t input_bytes = 0;
    uint64_t value_bytes = 0;
    size_t live_count = 0;
    bool creation_started = false;
    // A rename can fail after it took effect, so after a tried rename the final
    // name may exist even when `renamed` is false.
    bool rename_attempted = false;
    bool renamed = false;
  };
  // The live sets are planned, read and checked with the types below. See the
  // comment at the start of PlanLiveSetReads().
  struct InputPlan {
    bool planned = false;
    bool failed = false;
    bool transient = false;
    const char* reason = nullptr;
  };
  // One read: every group of one generation number in one SST, and the census
  // record it is checked against.
  struct GroupRead {
    uint64_t sst_file_number = 0;
    BlobReferenceGroup group;
    size_t input_index = 0;
    size_t check_index = 0;
  };
  // The census record of one generation number, and what the SST reads found.
  // After the reads, both must be equal for the same census Version.
  struct GenerationCheck {
    uint64_t generation = 0;
    size_t input_index = 0;
    BlobCensusGenerationTotals expected;
    size_t expected_holders = 0;
    BlobCensusGenerationTotals found;
    size_t holders_found = 0;
  };
  // The open readers of one input. Only one of the two is set.
  struct InputReader {
    std::unique_ptr<BlobFileReader> normal;
    CacheHandleGuard<BlobFileReader> relocation;
  };

  // Takes a reference to the input Version (Ref()), checks that the census is
  // ready for it, registers the GC job and releases the DB mutex. Then it
  // takes the candidate stamp, finds the old files routed to each relocation
  // input, and takes the census snapshot. When the job cannot begin, it
  // releases the input Version if it took a reference, and returns the job's
  // final status with the DB mutex held and no GC job registration
  // (gc_job_token is 0):
  // 1. TryAgain when there is no census or no input Version, or the census is
  //    not ready for the input Version.
  // 2. Corruption when the table cache or the blob file cache is missing.
  // 3. OK when BeginGcJob() returns Busy. For any other error, it returns that
  //    error after locked_census_status().
  // Otherwise it returns OK, sets `status` and returns without the DB mutex.
  // REQUIRES: DB mutex held.
  Status Start();

  // Builds `inputs` and their generation numbers. It sets `status` to a stale
  // TryAgain when a candidate changed. It suppresses an input with bad metadata
  // or with a generation number pinned by a coarse SST. It also creates the
  // memory tracker and charges the I/O workspace. When that charge fails, it
  // gives every input a transient suppression. When the table format is not
  // block-based, it gives every input a normal suppression. Called without the
  // DB mutex.
  void SelectInputs();

  // Plans the SST reads of each input (group_reads, generation_checks) and
  // reserves its live set. An input that does not fit the memory budget waits
  // for a later job, with all later inputs (deferred_input), when an earlier
  // input already holds a reservation. Otherwise its plan is marked failed and
  // transient, so that DecideInputs() gives it a transient suppression. It sets
  // census_mismatch when an SST that the census lists is not in the input
  // Version. Called without the DB mutex.
  void PlanLiveSetReads();

  // Reads the planned generation numbers SST by SST into the live sets, then
  // checks the totals found against the census. On a first census mismatch it
  // takes the DB mutex for a short time to schedule a census rebuild. A
  // mismatch that remains after a rebuild suppresses only that input. Called
  // without the DB mutex.
  void ReadLiveSets();

  // Decides each planned input: relocate it, remove it (no live value), or
  // suppress it. An accepted input joins eligible_inputs. When an input does
  // not fit the memory budget after earlier inputs, it sets deferred_input.
  // Called without the DB mutex.
  void DecideInputs();

  // Packs whole inputs into at most kIndirectBlobGCMaxOutputs (8) outputs and
  // checks the disk space. When the inputs need more outputs or more space, the
  // largest input waits (it moves from eligible_inputs to
  // capacity_deferred_files). When only one input is left and the disk space
  // check still fails, it sets `status` to CompactionTooLarge. It takes the DB
  // mutex for a short time to get the output file numbers. Called without the
  // DB mutex.
  void PackOutputs();

  // Opens a reader for each eligible input that has live values (`readers`). An
  // input that cannot be opened (Corruption, MemoryLimit or NotSupported)
  // becomes candidate_local_failure_input. A candidate-local failure in this
  // phase or in WriteOutputs() ends the whole job: no output is installed, only
  // the failed input is suppressed, and the other inputs stay candidates for a
  // later job. Failures in the read phase (fail_input()) skip only their input.
  // Called without the DB mutex.
  void OpenInputReaders();

  // Writes each output, then syncs and renames the outputs. Then it reports the
  // outputs to the listeners, deletes the outputs that are not kept, and
  // releases the readers and the live sets. A candidate-local failure here ends
  // the whole job (see OpenInputReaders()). Called without the DB mutex.
  //
  // Error priority: a candidate-local failure blames one input. It is a source
  // value that is corrupt or unsupported (Corruption or NotSupported; in the
  // batched path also MemoryLimit, for a value that is too large), or an output
  // that is larger than its inputs (the first input of that output is blamed).
  // A source error of another kind (for example an I/O error) and an output
  // write error have priority over it: the job returns that error, and
  // candidate_local_failure_input stays unset. In the body, the test
  // `candidate_local_failure_input == SIZE_MAX` means "no candidate-local
  // failure yet".
  void WriteOutputs();

  // Takes the DB mutex. It ends the job without an edit when `status` is not
  // OK, when the column family was dropped, or when the job is interrupted. On
  // CompactionTooLarge it also defers the waiting inputs in the current
  // Version, if the Version did not change. When no input is left to install,
  // it suppresses the skipped inputs in the current Version and keeps `status`
  // OK. Otherwise it releases the DB mutex to build the MANIFEST edit, and
  // installs the edit with one LogAndApply() call that has a precondition (see
  // "Install protocol" in the body). It marks in delete_renamed_outputs the
  // outputs that Finish() must delete. Called without the DB mutex. Returns
  // with the DB mutex held.
  void Install();
  // Install() helpers: BuildEdit() builds the MANIFEST edit, and
  // ValidatePhysicalInputs() is the precondition of its LogAndApply() call.
  // See their definitions.
  Status BuildEdit();
  Status ValidatePhysicalInputs();

  // Ends the job: ends the GC job registration (EndGcJob()), releases the
  // input Version, and suppresses the skipped inputs in the census heap. On
  // CompactionTooLarge the inputs in capacity_deferred_files get a transient
  // suppression. It deletes the renamed outputs marked in
  // delete_renamed_outputs, releases most of the job's memory, and schedules
  // the census worker (ScheduleBlobReferenceCensus()). It releases the DB mutex
  // during this cleanup. The I/O workspace charge and the group read charge
  // stay until the job is destroyed, with the DB mutex held (see "Lifecycle"
  // above the class). Returns `status`.
  // REQUIRES: DB mutex held. Returns with the DB mutex held.
  Status Finish();

  // Helpers that several phases use. Their comments are at their
  // definitions.
  Status stale_input(const char* message);
  Status invariant_failed(const char* message);
  Status unlocked_census_status(Status census_status);
  Status locked_census_status(Status census_status);
  Status interrupted();
  void suppress_candidates_transiently(const char* reason);
  void fail_input(size_t input_index, const char* reason);

  DBImpl* const db_;

  // The arguments of RunIndirectBlobGCBatch().
  ColumnFamilyData* const cfd;
  const std::vector<uint64_t>& candidate_physical_file_numbers;
  bool* const made_progress;
  uint64_t* const sfm_reserved_bytes;
  LogBuffer* const log_buffer;
  const int job_id;

  // column_family is *cfd, and stale_input_is_retryable is the
  // retryable_stale_input argument of RunIndirectBlobGCBatch().
  ColumnFamilyData& column_family;
  bool& stale_input_is_retryable;

  // Set by Start(), except `inputs`, which SelectInputs() fills. input_version
  // is the input Version: Start() takes a reference to it (Ref()), and Finish()
  // releases it. census is the job's census snapshot (defined in the overview
  // in db/blob/blob_relocation_gc.h), not the census itself, which is
  // blob_reference_census.
  BlobReferenceCensus* blob_reference_census = nullptr;
  Version* input_version = nullptr;
  uint64_t input_version_number = 0;
  std::shared_ptr<BlobReferenceCensusState> census;
  VersionStorageInfo* input_storage = nullptr;
  std::vector<PhysicalInput> inputs;
  MutableCFOptions mutable_cf_options;
  bool allow_value_relocation = false;
  TableCache* table_cache = nullptr;
  BlobFileCache* blob_file_cache = nullptr;
  FileOptions blob_gc_file_options;
  std::string output_directory;

  // read_options is for the MANIFEST write (LogAndApply()). gc_read_options is
  // for the SST and blob reads and for the output table builder: it does not
  // fill the block cache and uses Env::IO_LOW.
  ReadOptions read_options;
  ReadOptions gc_read_options;
  WriteOptions write_options;
  WriteOptions manifest_write_options;

  // A copy of the background error, taken under the DB mutex in Start().
  // PackOutputs() runs without the DB mutex and passes it to the
  // SstFileManager.
  Status sfm_background_error;

  // The token of the job's census registration (BeginGcJob()). 0 means that
  // the job is not registered.
  uint64_t gc_job_token = 0;

  // The job status (see "Job status" in the class comment).
  Status status;
  std::shared_ptr<Cache> gc_block_cache;

  // The inputs to suppress when the job ends, and the reason for each. The
  // files in transient_suppressions get a transient suppression. Any phase can
  // add to them. Install() and Finish() use them.
  std::vector<uint64_t> suppressed_files;
  std::vector<std::string> suppression_reasons;
  std::set<uint64_t> transient_suppressions;
  BlobGcCandidateStamp candidate_stamp;

  // The old files routed to each relocation input, sorted.
  std::map<uint64_t, std::vector<uint64_t>> routed_generations_by_input;

  // source_table_options, memory_tracker and release_selected_reader_charge are
  // set by SelectInputs(). eligible_inputs holds the indexes in `inputs` of the
  // inputs that DecideInputs() accepts. DecideInputs() (on a live-set failure),
  // PackOutputs() and WriteOutputs() can remove inputs from it.
  // selected_reader_bytes is the charge of the SST reader that ReadLiveSets()
  // holds open. deferred_input is true when an input waits for a later job (set
  // by PlanLiveSetReads(), DecideInputs() or PackOutputs()).
  std::optional<BlockBasedTableOptions> source_table_options;
  std::shared_ptr<BlobRelocationMemoryTracker> memory_tracker;
  std::vector<size_t> eligible_inputs;
  size_t selected_reader_bytes = 0;
  std::optional<Defer> release_selected_reader_charge;
  bool deferred_input = false;

  // Set by PlanLiveSetReads() and ReadLiveSets(). The planned inputs are the
  // first plan_end inputs.
  std::vector<GenerationCheck> generation_checks;
  bool census_mismatch = false;

  // The input whose census check failed (SIZE_MAX: none).
  size_t mismatched_input = std::numeric_limits<size_t>::max();
  std::vector<InputPlan> plans;
  std::vector<GroupRead> group_reads;
  size_t group_read_bytes = 0;
  std::optional<Defer> release_group_read_charge;
  size_t plan_end = 0;

  // Set by PackOutputs(). bins[i] lists the inputs of outputs[i].
  uint64_t aggregate_input_bytes = 0;
  std::vector<uint64_t> capacity_deferred_files;
  std::vector<BlobRelocationPackingBin> bins;
  std::vector<OutputState> outputs;

  // Set by OpenInputReaders() and WriteOutputs(). An input that fails during
  // reading or writing is candidate_local_failure_input (SIZE_MAX: none).
  std::vector<InputReader> readers;
  size_t candidate_local_failure_input = std::numeric_limits<size_t>::max();
  const char* candidate_local_failure_reason = nullptr;

  // Set by WriteOutputs() and Install().
  std::unique_ptr<VersionEdit> edit;
  std::array<bool, kIndirectBlobGCMaxOutputs> output_needed{};
  size_t published_output_count = 0;
  size_t published_relocated_count = 0;
  uint64_t published_relocated_value_bytes = 0;
  // read_input_count is the number of inputs that the job accepted after
  // WriteOutputs(): the inputs it read and packed, plus inputs with no live
  // value, which it only removes. published_input_count is the number of inputs
  // that the installed edit removed; it stays 0 when the job installs nothing.
  size_t read_input_count = 0;
  size_t published_input_count = 0;
  std::array<bool, kIndirectBlobGCMaxOutputs> delete_renamed_outputs{};

  // Monitoring (tickers, the job histogram and the end-of-job line). Run()
  // sets start_micros and reads the others after Finish(). WriteOutputs() adds
  // to input_bytes_read and Install() sets bytes_written and installed.
  uint64_t start_micros = 0;
  uint64_t input_bytes_read = 0;
  uint64_t bytes_written = 0;
  bool installed = false;
  // Inputs skipped because of corruption: file number -> what was found.
  // `logged` is true when the code that found it already wrote an ERROR line.
  // `fails_job` is true when the input fails the job (corrupt blob data with
  // paranoid_checks). Finish() writes the other lines and counts
  // INDIRECT_BLOB_GC_CORRUPT_INPUTS.
  struct CorruptInput {
    std::string detail;
    bool logged = false;
    bool fails_job = false;
  };
  std::map<uint64_t, CorruptInput> corrupt_inputs;
  void note_corruption(uint64_t file_number, const Status& s,
                       bool logged = false) {
    CorruptInput& entry = corrupt_inputs[file_number];
    if (entry.detail.empty()) {
      entry.detail = s.ToString();
    }
    entry.logged = entry.logged || logged;
  }
  // Records the job outcome (tickers, histogram, end-of-job line). REQUIRES:
  // DB mutex held.
  void RecordOutcome(const Status& result, size_t skipped_count);
};

Status DBImpl::RunIndirectBlobGCBatch(
    ColumnFamilyData* cfd,
    const std::vector<uint64_t>& candidate_physical_file_numbers,
    bool* made_progress, uint64_t* sfm_reserved_bytes,
    bool* retryable_stale_input, LogBuffer* log_buffer, int job_id) {
  mutex_.AssertHeld();
  if (cfd == nullptr || made_progress == nullptr ||
      sfm_reserved_bytes == nullptr || retryable_stale_input == nullptr) {
    return Status::InvalidArgument("Null indirect blob GC batch argument");
  }
  if (candidate_physical_file_numbers.empty() ||
      candidate_physical_file_numbers.size() > kIndirectBlobGCMaxInputs) {
    return Status::InvalidArgument("Invalid indirect blob GC batch size");
  }
  IndirectBlobGCJob job(this, cfd, candidate_physical_file_numbers,
                        made_progress, sfm_reserved_bytes,
                        retryable_stale_input, log_buffer, job_id);
  return job.Run();
}

Status DBImpl::IndirectBlobGCJob::Run() {
  db_->mutex_.AssertHeld();
  Status start_status = Start();
  if (!start_status.ok() || gc_job_token == 0) {
    // The job stops before SelectInputs() and Finish(), so nothing reads
    // `status` or the copy of the background error.
    status.PermitUncheckedError();
    sfm_background_error.PermitUncheckedError();
    return start_status;
  }
  RecordTick(db_->stats_, INDIRECT_BLOB_GC_JOBS_STARTED);
  start_micros = db_->immutable_db_options_.clock->NowMicros();
  cfd->ClearIndirectBlobGcCandidates();
  SelectInputs();
  PlanLiveSetReads();
  ReadLiveSets();
  DecideInputs();
  PackOutputs();
  OpenInputReaders();
  WriteOutputs();
  Install();
  // Finish() clears suppressed_files, so count the skipped inputs first. The
  // inputs that Finish() defers for outputs or disk space count too.
  const size_t skipped_count =
      suppressed_files.size() +
      (status.IsCompactionTooLarge() ? capacity_deferred_files.size() : 0);
  Status result = Finish();
  RecordOutcome(result, skipped_count);
  return result;
}

Status DBImpl::IndirectBlobGCJob::Start() {
  assert(*sfm_reserved_bytes == 0);
  stale_input_is_retryable = false;

  blob_reference_census = cfd->blob_reference_census();
  if (blob_reference_census == nullptr) {
    return stale_input("Blob reference census is unavailable");
  }

  input_version = cfd->current();
  if (input_version == nullptr) {
    return stale_input("Indirect blob GC input Version is unavailable");
  }
  input_version->Ref();
  input_version_number = input_version->GetVersionNumber();
  if (!blob_reference_census->IsReady(input_version_number)) {
    input_version->UnrefAndFreeBlobRelocationState(&db_->mutex_);
    return stale_input("Blob reference census is not ready");
  }

  input_storage = input_version->storage_info();
  inputs.reserve(candidate_physical_file_numbers.size());
  mutable_cf_options = input_version->GetMutableCFOptions();
  const MutableCFOptions& latest_options = cfd->GetLatestMutableCFOptions();
  allow_value_relocation = latest_options.enable_blob_files &&
                           latest_options.enable_blob_indirection &&
                           latest_options.enable_blob_garbage_collection;
  table_cache = cfd->table_cache();
  blob_file_cache = cfd->blob_file_cache();
  if (table_cache == nullptr || blob_file_cache == nullptr) {
    input_version->UnrefAndFreeBlobRelocationState(&db_->mutex_);
    return Status::Corruption("Missing indirect blob GC reader cache");
  }
  blob_gc_file_options = db_->file_options_for_compaction_;
  output_directory = cfd->ioptions().cf_paths.front().path;
  read_options = ReadOptions(Env::IOActivity::kCompaction);
  gc_read_options = read_options;
  gc_read_options.fill_cache = false;
  gc_read_options.rate_limiter_priority = Env::IO_LOW;
  write_options = WriteOptions(Env::IO_LOW, Env::IOActivity::kCompaction);
  // Indirect blob GC data writes use IO_LOW. The MANIFEST write uses the
  // default priority, so a MANIFEST write at the head of the writer queue does
  // not wait behind the background rate limiter.
  manifest_write_options = WriteOptions(Env::IOActivity::kCompaction);
  sfm_background_error = db_->error_handler_.GetBGError();
  status =
      blob_reference_census->BeginGcJob(input_version_number, &gc_job_token);
  if (!status.ok()) {
    input_version->UnrefAndFreeBlobRelocationState(&db_->mutex_);
    if (status.IsBusy()) {
      // Busy: an earlier metadata commit has an unknown outcome until recovery,
      // or another indirect blob GC job is registered. Retrying now would only
      // spin; a later Version picks GC again.
      return Status::OK();
    }
    // TryAgain: a metadata commit is in progress, or the census is not ready
    // for this Version; retry later.
    stale_input_is_retryable = status.IsTryAgain();
    return locked_census_status(std::move(status));
  }

  db_->mutex_.Unlock();

  // Indirect blob GC working memory, including the selected census snapshot, is
  // charged to its own cache role,
  // CacheEntryRole::kIndirectBlobGcWorkingMemory.
  gc_block_cache = GetIndirectBlobGcWorkingMemoryCache(mutable_cf_options);

  status = blob_reference_census->CaptureCandidateStamp(
      input_version_number, candidate_physical_file_numbers, &candidate_stamp);
  if (status.IsTryAgain()) {
    // The census changed or was canceled (for example, the CF was dropped).
    stale_input_is_retryable = true;
  }
  // The old files routed to each relocation input, from one scan of the input
  // Version's routes (without the DB mutex). selected_sources holds the
  // generation numbers that the census snapshot copies: each candidate, and
  // each old file routed to a relocation candidate.
  std::set<uint64_t> selected_sources;
  for (uint64_t file_number : candidate_physical_file_numbers) {
    selected_sources.insert(file_number);
    if (input_storage->PeekBlobRelocationFileMetaData(file_number) != nullptr) {
      routed_generations_by_input[file_number];
    }
  }
  if (!routed_generations_by_input.empty()) {
    input_storage->ForEachBlobRelocationRoute(
        [&](const BlobRelocationRoute& route) {
          const auto input =
              routed_generations_by_input.find(route.GetTargetFileNumber());
          if (input != routed_generations_by_input.end()) {
            input->second.push_back(route.GetOldFileNumber());
            selected_sources.insert(route.GetOldFileNumber());
          }
        });
    for (auto& input : routed_generations_by_input) {
      std::sort(input.second.begin(), input.second.end());
    }
  }
  if (status.ok()) {
    status = blob_reference_census->SnapshotSelected(
        gc_job_token, input_version_number, selected_sources, gc_block_cache,
        &census);
    TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:SnapshotSelected",
                             &status);
    if (status.IsMemoryLimit()) {
      suppress_candidates_transiently(
          "selected census exceeds its memory budget");
      status = stale_input("Blob GC census snapshot exceeds the memory budget");
    } else {
      status = unlocked_census_status(std::move(status));
      if (status.IsTryAgain()) {
        stale_input_is_retryable = true;
      }
    }
  }
  selected_sources.clear();
  return Status::OK();
}

void DBImpl::IndirectBlobGCJob::SelectInputs() {
  if (status.ok()) {
    status = interrupted();
  }
  for (uint64_t file_number : candidate_physical_file_numbers) {
    if (!status.ok()) {
      break;
    }
    PhysicalInput input;
    input.file_number = file_number;
    input.normal_metadata = input_storage->GetBlobFileMetaData(file_number);
    input.relocation_metadata =
        input_storage->GetBlobRelocationFileMetaData(file_number);
    if ((input.normal_metadata == nullptr) ==
        (input.relocation_metadata == nullptr)) {
      status = stale_input("Indirect blob GC candidate changed");
      break;
    }
    input.generations.push_back(file_number);
    if (input.normal_metadata != nullptr) {
      if (!input.normal_metadata->IsIndirectOrigin() ||
          input.normal_metadata->GetTotalBlobCount() == 0) {
        status = stale_input("Indirect blob GC indirect-origin source changed");
        break;
      }
      input.physical_bytes = input.normal_metadata->GetBlobFileSize();
      input.total_record_bytes = input.normal_metadata->GetTotalBlobBytes();
    } else {
      // The input Version is this column family's, so it owns the file.
      input.physical_bytes = input.relocation_metadata->GetFileSize();
      input.total_record_bytes =
          input.relocation_metadata->GetTotalRecordBytes();
      input.routed_generations = routed_generations_by_input[file_number];
      input.generations.insert(input.generations.end(),
                               input.routed_generations.begin(),
                               input.routed_generations.end());
      std::sort(input.generations.begin(), input.generations.end());
    }
    // A coarse SST pins indirect-origin blob file numbers (see "Coarse SST" in
    // blob_reference_metadata.h), so indirect blob GC skips a pinned input:
    // a normal input whose file number is pinned, or a relocation input with
    // a pinned routed generation number (an old indirect-origin file number
    // that routes to it). In normal operation a coarse SST does not reference
    // a routed number, but GC keeps this pin so that it never collects a file
    // that a coarse SST may still read. When the pin goes away, the heap
    // gives the input a new revision and GC can pick it again. Suppressing
    // the input also removes it from the Version's GC candidate list, so a
    // job with no output does not reschedule itself.
    const bool coarse_pinned =
        input.normal_metadata != nullptr
            ? input_storage->IsCoarseIndirectOriginBlobPinned(file_number)
            : std::any_of(
                  input.routed_generations.begin(),
                  input.routed_generations.end(), [&](uint64_t generation) {
                    return input_storage->IsCoarseIndirectOriginBlobPinned(
                        generation);
                  });
    if (coarse_pinned) {
      suppressed_files.push_back(file_number);
      suppression_reasons.emplace_back(
          "coarse SST pins a generation number of the input");
      continue;
    }
    if (input.physical_bytes == 0 || input.total_record_bytes == 0) {
      // Bad metadata affects only this input: suppress it, and keep other
      // background work running.
      ROCKS_LOG_ERROR(db_->immutable_db_options_.info_log,
                      "[%s] Indirect blob GC skipped blob file #%" PRIu64
                      ": invalid input metadata",
                      cfd->GetName().c_str(), file_number);
      note_corruption(file_number, Status::Corruption("invalid input metadata"),
                      /*logged=*/true);
      suppressed_files.push_back(file_number);
      suppression_reasons.emplace_back("invalid input metadata");
      continue;
    }
    inputs.push_back(std::move(input));
  }

  const TableFactory* const table_factory =
      mutable_cf_options.table_factory.get();
  if (table_factory != nullptr) {
    const BlockBasedTableOptions* options =
        table_factory->GetOptions<BlockBasedTableOptions>();
    const AdaptiveTableFactory* const adaptive_table_factory =
        table_factory->CheckedCast<AdaptiveTableFactory>();
    if (adaptive_table_factory != nullptr) {
      options = adaptive_table_factory->GetBlockBasedTableWriterOptions();
    }
    if (options != nullptr) {
      source_table_options.emplace(*options);
    }
  }

  std::shared_ptr<CacheReservationManager> reservation_manager;
  if (gc_block_cache != nullptr) {
    reservation_manager = std::make_shared<CacheReservationManagerImpl<
        CacheEntryRole::kIndirectBlobGcWorkingMemory>>(gc_block_cache);
  }
  memory_tracker = std::make_shared<BlobRelocationMemoryTracker>(
      std::move(reservation_manager));
  bool workspace_charge_failed = false;
  // Charge the I/O workspace, the buffers that the job uses to read and stream
  // the input values. The charge has 2 x kIndirectBlobGCReadBatchBytes for the
  // read buffer of one batch, which holds the batch's stored bytes plus the
  // dead gaps that coalescing merges into its reads (at most one more batch).
  // Payloads are slices of this buffer. The charge also has 2 x
  // kBlobRelocationStreamChunkSize (256 KB each) for values that are streamed
  // in chunks, one value at a time: the chunk buffer of the stream reader
  // (StreamAndVerifyBlobStoredPayload), and, with direct I/O, the aligned
  // buffer that RandomAccessFileReader::Read allocates for each chunk. A
  // streamed value is copied between batches, never during one. Charge the
  // workspace before allocating exact live sets or read buffers.
  size_t io_workspace_bytes = 0;
  if (status.ok() && allow_value_relocation) {
    const Status reservation_status = memory_tracker->SetReservation(
        &io_workspace_bytes,
        2 * kIndirectBlobGCReadBatchBytes + 2 * kBlobRelocationStreamChunkSize);
    if (!reservation_status.ok()) {
      for (const PhysicalInput& input : inputs) {
        suppressed_files.push_back(input.file_number);
        transient_suppressions.insert(input.file_number);
        suppression_reasons.emplace_back(
            "blob GC I/O workspace cache pressure");
      }
      workspace_charge_failed = true;
      source_table_options.reset();
    }
  }

  release_selected_reader_charge.emplace(
      [memory_tracker = memory_tracker,
       &selected_reader_bytes = selected_reader_bytes] {
        memory_tracker->SetReservation(&selected_reader_bytes, 0)
            .PermitUncheckedError();
      });
  // The workspace charge failure above already suppressed every input
  // (transient), so suppress them for the table format only when the format
  // is the reason.
  if (status.ok() && !source_table_options.has_value() &&
      !workspace_charge_failed) {
    for (const PhysicalInput& input : inputs) {
      suppressed_files.push_back(input.file_number);
      suppression_reasons.emplace_back("block-based table format is required");
    }
  }
}

void DBImpl::IndirectBlobGCJob::PlanLiveSetReads() {
  // The live sets are read SST by SST. First each input's reads are planned and
  // its live set reserved, in input order; then each selected SST is opened
  // once, its reference index decoded once and each needed data block read at
  // most once; then each input is decided in order.
  plans.resize(inputs.size());
  release_group_read_charge.emplace([memory_tracker = memory_tracker,
                                     &group_read_bytes = group_read_bytes] {
    memory_tracker->SetReservation(&group_read_bytes, 0).PermitUncheckedError();
  });
  bool reserved_any = false;

  // Plan the reads of each input and reserve its live set from the census
  // reference counts of its generation numbers. When an input does not fit the
  // memory budget, it and all later inputs wait for a later job if an earlier
  // input holds a reservation. Otherwise the input is suppressed with a retry
  // time (transient).
  for (size_t input_index = 0;
       status.ok() && source_table_options.has_value() &&
       input_index < inputs.size();
       ++input_index) {
    status = interrupted();
    if (!status.ok()) {
      break;
    }
    PhysicalInput& input = inputs[input_index];
    plan_end = input_index + 1;
    InputPlan& plan = plans[input_index];
    plan.planned = true;
    input.live_set = std::make_unique<BlobRelocationLiveSet>(memory_tracker,
                                                             input.generations);
    const size_t first_group_read = group_reads.size();
    const size_t first_generation_check = generation_checks.size();
    uint64_t live_count = 0;
    for (uint64_t generation : input.generations) {
      const std::vector<uint64_t>* const sst_files =
          census->FindSstFiles(generation);
      const BlobCensusGenerationTotals* const totals =
          census->FindTotals(generation);
      if (sst_files == nullptr) {
        continue;
      }
      // The live set is reserved from the census totals.
      if (totals->reference_count > std::numeric_limits<size_t>::max() ||
          totals->reference_count >
              std::numeric_limits<uint64_t>::max() - live_count) {
        status = Status::Corruption("Exact blob reference count overflow");
        fail_input(input_index, "exact SST metadata is inconsistent");
        break;
      }
      live_count += totals->reference_count;
      const size_t check_index = generation_checks.size();
      generation_checks.push_back(
          {generation, input_index, *totals, sst_files->size(), {}, 0});
      BlobReferenceGroup key;
      key.generation_file_number = generation;
      for (size_t i = 0; status.ok() && i < sst_files->size(); ++i) {
        if ((i & kIndirectBlobGCAbortCheckMask) == 0) {
          status = interrupted();
          if (!status.ok()) {
            break;
          }
        }
        const uint64_t sst_file_number = (*sst_files)[i];
        if (input_storage->GetFileMetaDataByNumber(sst_file_number) ==
            nullptr) {
          census_mismatch = true;
          mismatched_input = input_index;
          break;
        }
        if (group_reads.size() == group_reads.capacity()) {
          const size_t capacity =
              group_reads.capacity() == 0 ? 16 : 2 * group_reads.capacity();
          status = memory_tracker->SetReservation(&group_read_bytes,
                                                  capacity * sizeof(GroupRead));
          if (!status.ok()) {
            break;
          }
          group_reads.reserve(capacity);
        }
        group_reads.push_back({sst_file_number, key, input_index, check_index});
      }
      if (!status.ok() || census_mismatch) {
        break;
      }
    }
    if (status.ok() && !census_mismatch) {
      status = input.live_set->Reserve(static_cast<size_t>(live_count));
      TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:ReserveLiveSet",
                               &status);
    }
    if (!status.ok()) {
      // A deferred or failed input reads nothing, so it has no totals to check
      // either.
      group_reads.resize(first_group_read);
      generation_checks.resize(first_generation_check);
      input.live_set.reset();
    }
    // Only the reference count overflow check above sets both Corruption and
    // plan.failed. The cleanup above has run, so only this input fails.
    if (status.IsCorruption() && plan.failed) {
      status = Status::OK();
      continue;
    }
    if (status.IsMemoryLimit()) {
      status = Status::OK();
      if (reserved_any) {
        plan.planned = false;
        plan_end = input_index;
        deferred_input = true;
        break;
      }
      plan.failed = true;
      plan.transient = true;
      plan.reason = "exact live set exceeds its memory budget";
      continue;
    }
    if (!status.ok() || census_mismatch) {
      break;
    }
    reserved_any = true;
  }
}

void DBImpl::IndirectBlobGCJob::ReadLiveSets() {
  // Read the planned generation numbers SST by SST. Corruption in a group or in
  // an SST's metadata fails only the inputs that read it.
  // TODO: when an SST's exact reference metadata is corrupt, GC suppresses
  // every input that the SST references, so their garbage is never collected.
  // A later change should fall back to reading the SST's key-value data (its
  // blob indexes) to build the live set for those inputs.
  std::sort(group_reads.begin(), group_reads.end(),
            [](const GroupRead& lhs, const GroupRead& rhs) {
              return std::make_tuple(lhs.sst_file_number,
                                     lhs.group.generation_file_number) <
                     std::make_tuple(rhs.sst_file_number,
                                     rhs.group.generation_file_number);
            });
  std::vector<BlobReferenceGroupRead> batch;
  std::vector<size_t> batch_inputs;
  std::vector<size_t> batch_reads;
  for (size_t begin = 0;
       status.ok() && !census_mismatch && begin < group_reads.size();) {
    size_t end = begin;
    while (end < group_reads.size() && group_reads[end].sst_file_number ==
                                           group_reads[begin].sst_file_number) {
      ++end;
    }
    status = interrupted();
    if (!status.ok()) {
      break;
    }
    batch.clear();
    batch_inputs.clear();
    batch_reads.clear();
    for (size_t i = begin; status.ok() && i < end; ++i) {
      const GroupRead& read = group_reads[i];
      if (plans[read.input_index].failed) {
        continue;
      }
      BlobReferenceGroupRead group_read;
      group_read.group = &read.group;
      group_read.whole_generation = true;
      group_read.sink = inputs[read.input_index].live_set.get();
      batch.push_back(std::move(group_read));
      batch_inputs.push_back(read.input_index);
      batch_reads.push_back(i);
    }
    const uint64_t sst_file_number = group_reads[begin].sst_file_number;
    begin = end;
    if (!status.ok() || batch.empty()) {
      continue;
    }
    const FileMetaData* const file =
        input_storage->GetFileMetaDataByNumber(sst_file_number);
    // One reader is held at a time. Its charge is 64 KB for the table reader
    // itself (an estimate, also charged when the reader comes from the table
    // cache, to keep the charge simple), plus 2 x kBlobReferenceReadBatchBytes
    // (256 KB each, 512 KB in total) for one batch of raw blocks and its
    // decompressed copies. The decoded reference index is added at its real
    // size once it is decoded: it grows with the groups and blocks of the SST
    // (one BlobReferenceIndexEntry per group), so an SST with many groups needs
    // more than any fixed estimate.
    constexpr size_t kReaderBytes =
        64 * 1024 + 2 * kBlobReferenceReadBatchBytes;
    status =
        memory_tracker->SetReservation(&selected_reader_bytes, kReaderBytes);
    TableCache::BlobReferenceReader reader;
    if (status.ok()) {
      status = table_cache->GetBlobReferenceReader(
          gc_read_options, blob_gc_file_options, cfd->internal_comparator(),
          *file, mutable_cf_options, &reader);
    }
    if (status.ok()) {
      status = reader->ReadBlobReferences(
          gc_read_options, file->blob_reference_descriptor,
          [&](size_t index_bytes) {
            if (index_bytes >
                std::numeric_limits<size_t>::max() - kReaderBytes) {
              return Status::MemoryLimit("Blob reference index is too large");
            }
            return memory_tracker->SetReservation(&selected_reader_bytes,
                                                  kReaderBytes + index_bytes);
          },
          &batch);
    }
    reader.Reset();
    Status release_status =
        memory_tracker->SetReservation(&selected_reader_bytes, 0);
    release_status.PermitUncheckedError();
    // Reports the batch size and the reader charge left after closing.
    std::pair<size_t, size_t> batch_report{batch.size(), selected_reader_bytes};
    TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:SstBatchRead",
                             &batch_report);
    if (status.IsCorruption()) {
      for (size_t input_index : batch_inputs) {
        fail_input(input_index, "exact SST metadata is corrupt");
        note_corruption(inputs[input_index].file_number, status);
      }
      status = Status::OK();
      continue;
    }
    if (!status.ok()) {
      break;
    }
    for (size_t i = 0; i < batch.size(); ++i) {
      if (batch[i].status.IsNotFound() || batch[i].status.IsAborted()) {
        // The SST lacks the generation number, or has more references than the
        // census.
        census_mismatch = true;
        mismatched_input = batch_inputs[i];
        break;
      }
      if (batch[i].status.IsCorruption()) {
        fail_input(batch_inputs[i], "exact SST metadata is corrupt");
        note_corruption(inputs[batch_inputs[i]].file_number, batch[i].status);
      } else if (!batch[i].status.ok()) {
        status = batch[i].status;
        break;
      } else {
        GenerationCheck& check =
            generation_checks[group_reads[batch_reads[i]].check_index];
        check.found.reference_count += batch[i].found_reference_count;
        check.found.stored_value_bytes += batch[i].found_stored_value_bytes;
        check.found.record_bytes += batch[i].found_record_bytes;
        ++check.holders_found;
      }
    }
  }
  // The census totals check: for each generation number of an input that read
  // all its SSTs, the number of SSTs found that hold the generation number, and
  // their totals, must equal the census record. A first mismatch rebuilds the
  // census and retries the job. A mismatch that remains after a rebuild
  // suppresses only that input (see below). Only inputs that were planned, read
  // and not failed are checked.
  const GenerationCheck* mismatched_check = nullptr;
  if (status.ok() && !census_mismatch) {
    // A normal input that no SST holds in the census, while an SST still links
    // to it as its oldest blob file: do not trust the census to remove the
    // input (blob file retirement) with no output.
    for (size_t input_index = 0; input_index < plan_end; ++input_index) {
      const PhysicalInput& input = inputs[input_index];
      if (plans[input_index].planned && !plans[input_index].failed &&
          input.normal_metadata != nullptr &&
          !input.normal_metadata->GetLinkedSsts().empty() &&
          census->FindSstFiles(input.file_number) == nullptr) {
        census_mismatch = true;
        mismatched_input = input_index;
        break;
      }
    }
  }
  if (status.ok() && !census_mismatch) {
    for (const GenerationCheck& check : generation_checks) {
      if (check.input_index >= plan_end || !plans[check.input_index].planned ||
          plans[check.input_index].failed) {
        continue;
      }
      if (check.holders_found != check.expected_holders ||
          check.found.reference_count != check.expected.reference_count ||
          check.found.stored_value_bytes != check.expected.stored_value_bytes ||
          check.found.record_bytes != check.expected.record_bytes) {
        census_mismatch = true;
        mismatched_check = &check;
        mismatched_input = check.input_index;
        break;
      }
    }
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:CensusTotalsCheck",
                           &census_mismatch);
  if (status.ok() && !census_mismatch) {
    // Every checked input matched the census. Forget a remembered mismatch of
    // such an input, so that a later mismatch of it requests a rebuild again
    // (see BlobReferenceCensus::ReportMismatch()).
    for (size_t input_index = 0; input_index < plan_end; ++input_index) {
      if (plans[input_index].planned && !plans[input_index].failed) {
        blob_reference_census->ClearMismatch(inputs[input_index].file_number);
      }
    }
  }
  if (census_mismatch && mismatched_input >= inputs.size() && plan_end > 0) {
    // Every real mismatch names its input; a test-injected one is charged to
    // the first input.
    mismatched_input = 0;
  }
  if (census_mismatch) {
    const uint64_t mismatched_file = mismatched_input < inputs.size()
                                         ? inputs[mismatched_input].file_number
                                         : 0;
    RecordTick(db_->stats_, INDIRECT_BLOB_GC_CENSUS_MISMATCHES);
    if (blob_reference_census->ReportMismatch(mismatched_file)) {
      // The rebuild did not clear the mismatch of this input. Skip only this
      // input, with a normal suppression (it ends when the file changes, the
      // census is rebuilt or, for a relocation file, the coarse frontier rises;
      // see BlobGcCandidateHeap::Suppress()), rather than rebuilding at every
      // Version. If it mismatches again after that, it is skipped again without
      // a rebuild, as long as no other file reported a mismatch in between,
      // because the census remembers only the last mismatch. This job decides
      // no input and publishes nothing; it is not retried at once, so the next
      // job, after the census drops this input from its candidates, takes the
      // other inputs.
      if (mismatched_check != nullptr) {
        ROCKS_LOG_ERROR(
            db_->immutable_db_options_.info_log,
            "[%s] Blob GC skips blob file #%" PRIu64
            " until it changes, the census is rebuilt or the coarse frontier "
            "rises (relocation files only): the census rebuild did not clear "
            "a mismatch for generation number %" PRIu64
            ": holders %" ROCKSDB_PRIszt "/%" ROCKSDB_PRIszt
            ", references %" PRIu64 "/%" PRIu64 " (found/expected)",
            cfd->GetName().c_str(), mismatched_file,
            mismatched_check->generation, mismatched_check->holders_found,
            mismatched_check->expected_holders,
            mismatched_check->found.reference_count,
            mismatched_check->expected.reference_count);
      } else {
        ROCKS_LOG_ERROR(
            db_->immutable_db_options_.info_log,
            "[%s] Blob GC skips blob file #%" PRIu64
            " until it changes, the census is rebuilt or the coarse frontier "
            "rises (relocation files only): the census rebuild "
            "did not clear a mismatch",
            cfd->GetName().c_str(), mismatched_file);
      }
      note_corruption(
          mismatched_file,
          Status::Corruption("census mismatch remains after a rebuild"),
          /*logged=*/true);
      suppressed_files.push_back(mismatched_file);
      suppression_reasons.emplace_back(
          "census mismatch remains after a rebuild");
      plan_end = 0;
    } else {
      ROCKS_LOG_ERROR(db_->immutable_db_options_.info_log,
                      "[%s] Blob GC found SSTs that do not match the blob "
                      "reference census for blob file #%" PRIu64
                      "; the census is rebuilt",
                      cfd->GetName().c_str(), mismatched_file);
      {
        InstrumentedMutexLock lock(&db_->mutex_);
        db_->ScheduleBlobReferenceCensus(cfd);
      }
      status = stale_input("Blob reference census does not match the SSTs");
    }
  }
  if (status.IsMemoryLimit()) {
    // Reader and request charges come from the same budget as the live sets.
    status = Status::OK();
    for (size_t input_index = 0; input_index < plan_end; ++input_index) {
      if (plans[input_index].planned && !plans[input_index].failed) {
        fail_input(input_index, "exact live set exceeds its memory budget");
        plans[input_index].transient = true;
      }
    }
  }

  TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:PlanEnd", &plan_end);
}

void DBImpl::IndirectBlobGCJob::DecideInputs() {
  // Decide each input in order.
  for (size_t input_index = 0; status.ok() && input_index < plan_end;
       ++input_index) {
    PhysicalInput& input = inputs[input_index];
    InputPlan& plan = plans[input_index];
    if (!plan.planned) {
      continue;
    }
    if (plan.transient) {
      transient_suppressions.insert(input.file_number);
    }
    bool candidate_local_failure = plan.failed;
    const char* candidate_reason = plan.reason;
    if (!candidate_local_failure) {
      status = input.live_set->Finalize();
      if (status.IsCorruption()) {
        candidate_local_failure = true;
        candidate_reason = "exact SST metadata is corrupt";
        note_corruption(input.file_number, status);
      }
    }
    uint64_t input_live_record_bytes = 0;
    if (status.ok() && !candidate_local_failure) {
      input.live_count = input.live_set->references().size();
      input_live_record_bytes = input.live_set->live_record_bytes();
      if (input.relocation_metadata != nullptr) {
        // A relocation file holds each value as stored bytes and a trailer.
        if (input.live_count > (std::numeric_limits<uint64_t>::max() -
                                input.live_set->live_stored_value_bytes()) /
                                   kSimpleGen2BlobTrailerSize) {
          status = Status::Corruption("Relocation live byte count overflow");
          candidate_local_failure = true;
          candidate_reason = "exact SST metadata is corrupt";
          note_corruption(input.file_number, status);
        } else {
          input_live_record_bytes =
              input.live_set->live_stored_value_bytes() +
              input.live_count * kSimpleGen2BlobTrailerSize;
        }
      }
      input.physical_live_record_bytes = input_live_record_bytes;
    }
    if (status.IsMemoryLimit()) {
      if (!eligible_inputs.empty()) {
        status = Status::OK();
        deferred_input = true;
        break;
      }
      candidate_local_failure = true;
      candidate_reason = "exact live set exceeds its memory budget";
      transient_suppressions.insert(input.file_number);
    }
    if (candidate_local_failure) {
      status = Status::OK();
      suppressed_files.push_back(input.file_number);
      suppression_reasons.emplace_back(candidate_reason);
      continue;
    }
    if (!status.ok()) {
      break;
    }
    if (input_live_record_bytes > input.total_record_bytes) {
      suppressed_files.push_back(input.file_number);
      suppression_reasons.emplace_back(
          "exact live bytes exceed physical record bytes");
      continue;
    }
    if (input.relocation_metadata != nullptr &&
        input.relocation_metadata->IsMarkedLost() &&
        input_live_record_bytes != 0) {
      // A lost relocation file (an accepted loss, recorded with
      // VersionEdit::MarkBlobPhysicalFileLost()) is never read in this
      // Version, so GC cannot copy the values that SSTs still reference.
      suppressed_files.push_back(input.file_number);
      suppression_reasons.emplace_back(
          "lost relocation file still has SST references");
      continue;
    }
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] Indirect blob GC exact candidate #%" PRIu64
                     ": live=%" PRIu64 " total=%" PRIu64,
                     cfd->GetName().c_str(), input.file_number,
                     input_live_record_bytes, input.total_record_bytes);
    if (!allow_value_relocation && input_live_record_bytes != 0) {
      suppressed_files.push_back(input.file_number);
      suppression_reasons.emplace_back("live-value relocation is disabled");
      continue;
    }
    // Same rule as the census selection (BlobGcCandidateHeap::CutSelection()).
    // Both read the options of the input Version.
    const double garbage_ratio =
        mutable_cf_options.indirect_blob_gc_garbage_ratio;
    if (static_cast<double>(input.total_record_bytes -
                            input_live_record_bytes) <
        garbage_ratio * static_cast<double>(input.total_record_bytes)) {
      char reason[96];
      snprintf(reason, sizeof(reason),
               "exact garbage ratio is below indirect_blob_gc_garbage_ratio "
               "(%g)",
               garbage_ratio);
      suppressed_files.push_back(input.file_number);
      suppression_reasons.emplace_back(reason);
      continue;
    }
    if (input.live_count != 0) {
      Status estimate_status = EstimateIndirectBlobGCPartitionBytes(
          input.physical_live_record_bytes, input.live_count,
          &input.estimated_bytes);
      if (!estimate_status.ok()) {
        estimate_status.PermitUncheckedError();
        suppressed_files.push_back(input.file_number);
        suppression_reasons.emplace_back(
            "relocation output size estimate overflow");
        continue;
      }
    }
    input.eligible = true;
    eligible_inputs.push_back(input_index);
  }

  TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:AfterLiveSet", &status);
  if (status.IsCorruption() || status.IsMemoryLimit()) {
    // This phase read only input and SST metadata, and created no output. So
    // its failures suppress the affected inputs and do not set a background
    // error.
    for (size_t input_index : eligible_inputs) {
      suppressed_files.push_back(inputs[input_index].file_number);
      suppression_reasons.emplace_back("source live-set validation failed");
      if (status.IsMemoryLimit()) {
        transient_suppressions.insert(inputs[input_index].file_number);
      }
    }
    eligible_inputs.clear();
    status = Status::OK();
  }
  // Lets concurrency tests install a new SST after the job's live set is fixed
  // but before output work begins.
  TEST_SYNC_POINT("DBImpl::RunIndirectBlobGC:BeforeOutputWork");
}

void DBImpl::IndirectBlobGCJob::PackOutputs() {
  capacity_deferred_files.reserve(eligible_inputs.size());
  auto* const sfm = static_cast<SstFileManagerImpl*>(
      db_->immutable_db_options_.sst_file_manager.get());
  while (status.ok() && !eligible_inputs.empty()) {
    // Whole inputs are packed; empty inputs need no output.
    std::vector<BlobRelocationPackingItem> items;
    std::vector<size_t> packed_inputs;
    for (size_t input_index : eligible_inputs) {
      PhysicalInput& input = inputs[input_index];
      input.output_index = std::numeric_limits<size_t>::max();
      if (input.estimated_bytes == 0) {
        continue;
      }
      BlobRelocationPackingItem item;
      item.input_file_number = input.file_number;
      item.estimated_bytes = input.estimated_bytes;
      for (const BlobRelocationOriginRun& run : input.live_set->origin_runs()) {
        item.origins.push_back(run.origin_file_number);
      }
      items.push_back(std::move(item));
      packed_inputs.push_back(input_index);
    }
    if (items.empty()) {
      bins.clear();
      break;
    }
    const uint64_t configured_output_bytes =
        mutable_cf_options.blob_file_size == 0
            ? 1
            : std::min(mutable_cf_options.blob_file_size,
                       kIndirectBlobGCMaxOutputBytes);
    const uint64_t target_output_bytes = std::max<uint64_t>(
        1, configured_output_bytes - configured_output_bytes / 10);
    status = PlanBlobRelocationPacking(items, target_output_bytes,
                                       kIndirectBlobGCMaxOutputs, &bins);
    if (status.ok()) {
      for (size_t output_index = 0; output_index < bins.size();
           ++output_index) {
        for (size_t& item_index : bins[output_index].item_indexes) {
          item_index = packed_inputs[item_index];
          inputs[item_index].output_index = output_index;
        }
      }
      aggregate_input_bytes = 0;
      uint64_t aggregate_estimated_bytes = 0;
      for (size_t input_index : eligible_inputs) {
        if (inputs[input_index].physical_bytes >
                std::numeric_limits<uint64_t>::max() - aggregate_input_bytes ||
            inputs[input_index].estimated_bytes >
                std::numeric_limits<uint64_t>::max() -
                    aggregate_estimated_bytes) {
          status = Status::Corruption("Indirect blob GC input byte overflow");
          break;
        }
        aggregate_input_bytes += inputs[input_index].physical_bytes;
        aggregate_estimated_bytes += inputs[input_index].estimated_bytes;
      }
      // The outputs need about their estimated size, never more than the
      // inputs, so the space check reserves the smaller of the two.
      const uint64_t reserved_bytes =
          std::min(aggregate_estimated_bytes, aggregate_input_bytes);
      if (!status.ok() || sfm == nullptr ||
          sfm->EnoughRoomForCompaction(reserved_bytes, output_directory,
                                       sfm_background_error)) {
        if (status.ok() && sfm != nullptr) {
          *sfm_reserved_bytes = reserved_bytes;
        }
        break;
      }
      if (eligible_inputs.size() == 1) {
        capacity_deferred_files.push_back(
            inputs[eligible_inputs.front()].file_number);
        status = Status::CompactionTooLarge();
        RecordTick(db_->stats_, COMPACTION_CANCELLED, 1);
        break;
      }
      status = Status::OK();
      deferred_input = true;
      bins.clear();
    }
    // Two paths reach the code below. (1) PlanBlobRelocationPacking() returned
    // MemoryLimit: the inputs need more than kIndirectBlobGCMaxOutputs
    // (8) outputs. That is not a memory error. (2) The disk space check above
    // failed with more than one input left, and `status` was set back to OK.
    if (!status.ok() && !status.IsMemoryLimit()) {
      break;
    }
    // Too many outputs or not enough space: the largest input waits for a later
    // job.
    status = Status::OK();
    const auto removed = std::max_element(
        eligible_inputs.begin(), eligible_inputs.end(),
        [&](size_t lhs, size_t rhs) {
          return std::tie(inputs[lhs].physical_bytes, inputs[lhs].file_number) <
                 std::tie(inputs[rhs].physical_bytes, inputs[rhs].file_number);
        });
    assert(removed != eligible_inputs.end());
    const size_t removed_input = *removed;
    capacity_deferred_files.push_back(inputs[removed_input].file_number);
    eligible_inputs.erase(removed);
    inputs[removed_input].eligible = false;
    inputs[removed_input].output_index = std::numeric_limits<size_t>::max();
    deferred_input = true;
  }
  sfm_background_error.PermitUncheckedError();

  outputs = std::vector<OutputState>(bins.size());
  if (status.ok() && !outputs.empty()) {
    db_->mutex_.Lock();
    Version* const current = cfd->current();
    if (cfd->IsDropped() || current == nullptr) {
      status = stale_input("Indirect blob GC column family changed");
    } else {
      for (OutputState& output : outputs) {
        output.file_number = db_->versions_->NewFileNumber();
        output.path = BlobFileName(output_directory, output.file_number);
        output.temporary_path =
            TempFileName(output_directory, output.file_number);
      }
    }
    db_->mutex_.Unlock();
  }
}

void DBImpl::IndirectBlobGCJob::OpenInputReaders() {
  readers = std::vector<InputReader>(inputs.size());
  // Indirect blob GC opens a relocation input through the blob file cache, and
  // GC's own open never pins its location blocks
  // (pin_blob_relocation_location_blocks): GC is about to reclaim the file, and
  // a pin would read every location block at IO_LOW while it holds a relocation
  // stripe mutex, and keep them charged until the purge. A user read may
  // already have cached a pinned reader, which GC then uses. If GC does not
  // reclaim the file, its unpinned reader stays cached.
  MutableCFOptions gc_open_options = mutable_cf_options;
  gc_open_options.pin_blob_relocation_location_blocks = false;
  for (size_t input_index : eligible_inputs) {
    if (!status.ok()) {
      break;
    }
    if (inputs[input_index].live_count == 0) {
      // Exact empty-file cleanup does not read source data or create output.
      continue;
    }
    const PhysicalInput& input = inputs[input_index];
    if (input.normal_metadata != nullptr) {
      const BlobFileOpenInfo source_file{
          input.file_number, input.normal_metadata->GetChecksumValue(),
          input.normal_metadata->GetChecksumMethod()};
      // Indirect blob GC input is cold data, and buffered io_uring reads of
      // coalesced ranges can read more device bytes than needed. So GC prefers
      // direct reads for normal blob file inputs (see
      // OpenBlobFileReaderUncached()). User reads keep their cached buffered
      // readers.
      status = blob_file_cache->OpenBlobFileReaderUncached(
          gc_read_options, source_file, &readers[input_index].normal,
          /*allow_footer_skip_retry=*/false, /*prefer_direct_reads=*/true);
      // A null reader is an invariant break. It is reported as Corruption on
      // purpose, so it follows the same paranoid_checks rule as corrupt data.
      if (status.ok() && readers[input_index].normal == nullptr) {
        status = Status::Corruption("Blob file cache returned a null reader");
      }
    } else {
      const BlobPhysicalFile source_file{
          BlobFileOpenInfo{input.file_number,
                           input.relocation_metadata->GetChecksumValue(),
                           input.relocation_metadata->GetChecksumMethod()},
          input.relocation_metadata->GetFileSize(), /*is_relocation=*/true,
          input.relocation_metadata->GetTailSize(),
          input.relocation_metadata->GetDirectoryDigest()};
      status = blob_file_cache->GetRelocationFileReader(
          gc_read_options, source_file, gc_open_options,
          &readers[input_index].relocation);
      if (status.ok() &&
          readers[input_index].relocation.GetValue() == nullptr) {
        status = Status::Corruption(
            "Blob file cache returned a null relocation reader");
      }
    }
    if (status.IsCorruption() || status.IsMemoryLimit() ||
        status.IsNotSupported()) {
      if (status.IsCorruption()) {
        note_corruption(inputs[input_index].file_number, status);
      }
      candidate_local_failure_input = input_index;
      candidate_local_failure_reason =
          "the input file cannot be opened for relocation";
      status = Status::OK();
      break;
    }
  }
  TEST_SYNC_POINT("DBImpl::RunIndirectBlobGC:InputsOpened");
}

void DBImpl::IndirectBlobGCJob::WriteOutputs() {
  uint64_t aggregate_output_bytes = 0;
  uint64_t total_relocated_value_bytes = 0;
  size_t total_relocated_count = 0;
  // This comparator must outlive each output's table builder.
  const InternalKeyComparator relocation_comparator(BytewiseComparator());
  for (size_t output_index = 0;
       status.ok() &&
       candidate_local_failure_input == std::numeric_limits<size_t>::max() &&
       output_index < outputs.size();
       ++output_index) {
    OutputState& output = outputs[output_index];
    status = interrupted();
    if (!status.ok()) {
      break;
    }
    output.creation_started = true;
    db_->blob_callback_.OnBlobFileCreationStarted(
        output.path, cfd->GetName(), job_id,
        BlobFileCreationReason::kIndirectBlobGC);
    FileOptions file_options = blob_gc_file_options;
    file_options.open_contract = FileOpenContract::kNoReopenForWrite |
                                 FileOpenContract::kNoReadersWhileOpenForWrite;
    std::unique_ptr<FSWritableFile> file;
    status = NewWritableFile(db_->fs_.get(), output.temporary_path, &file,
                             file_options);
    std::unique_ptr<WritableFileWriter> file_writer;
    std::unique_ptr<BlockBasedTableBuilder> table_builder;
    if (status.ok()) {
      if (file == nullptr) {
        status = Status::Corruption("File system returned a null blob GC file");
      } else {
        FSWritableFile& writable_file = *file;
        writable_file.SetIOPriority(write_options.rate_limiter_priority);
        FileTypeSet checksum_handoff_file_types =
            db_->immutable_db_options_.checksum_handoff_file_types;
        file_writer = std::make_unique<WritableFileWriter>(
            std::move(file), output.temporary_path, file_options,
            db_->immutable_db_options_.clock, db_->io_tracer_, db_->stats_,
            Histograms::BLOB_DB_BLOB_FILE_WRITE_MICROS,
            db_->immutable_db_options_.listeners,
            db_->immutable_db_options_.file_checksum_gen_factory.get(),
            checksum_handoff_file_types.Contains(FileType::kBlobFile), false);
        BlobRelocationTableFileInfo file_info;
        file_info.column_family_id = cfd->GetID();
        file_info.column_family_name = cfd->GetName();
        file_info.db_id = db_->db_id_;
        file_info.db_session_id = db_->db_session_id_;
        file_info.db_name = db_->dbname_;
        file_info.file_number = output.file_number;
        file_info.target_file_size = bins[output_index].estimated_bytes;
        status = NewBlobRelocationTableBuilder(
            cfd->ioptions(), mutable_cf_options, *source_table_options,
            gc_read_options, write_options, relocation_comparator, file_info,
            file_writer.get(), &table_builder);
      }
    }

    // The output gets whole inputs. Its values must be added in (origin,
    // original offset) order: each input's live set is in that order, and the
    // inputs of one output share no origin, so the inputs' origin runs are
    // merged by origin.
    struct OutputRun {
      size_t input_index = 0;
      BlobRelocationOriginRun run;
    };
    std::vector<OutputRun> output_runs;
    // The old file numbers this output can be routed from: its inputs and the
    // old files routed to them at job start. The builder stores them in the
    // file's directory as its served list. After the build, the output check
    // below compares that list with these file numbers.
    std::vector<uint64_t> served_generation_numbers;
    for (size_t input_index : bins[output_index].item_indexes) {
      const PhysicalInput& input = inputs[input_index];
      output.input_bytes += input.physical_bytes;
      served_generation_numbers.insert(served_generation_numbers.end(),
                                       input.generations.begin(),
                                       input.generations.end());
      for (const BlobRelocationOriginRun& run : input.live_set->origin_runs()) {
        output_runs.push_back({input_index, run});
      }
    }
    std::sort(output_runs.begin(), output_runs.end(),
              [](const OutputRun& lhs, const OutputRun& rhs) {
                return lhs.run.origin_file_number < rhs.run.origin_file_number;
              });
    std::sort(served_generation_numbers.begin(),
              served_generation_numbers.end());
    for (size_t i = 0; status.ok() && i < served_generation_numbers.size();
         ++i) {
      if (i == 0 ||
          served_generation_numbers[i - 1] != served_generation_numbers[i]) {
        status = table_builder->AddBlobRelocationServedFile(
            served_generation_numbers[i]);
      }
    }
    for (const OutputRun& output_run : output_runs) {
      if (!status.ok() ||
          candidate_local_failure_input != std::numeric_limits<size_t>::max()) {
        break;
      }
      const size_t run_input = output_run.input_index;
      const PhysicalInput& input = inputs[run_input];
      const std::vector<BlobRelocationLiveValue>& references =
          input.live_set->references();
      const uint64_t origin = output_run.run.origin_file_number;
      const size_t run_end = output_run.run.end;
      size_t next_reference = output_run.run.begin;
      while (status.ok() && next_reference < run_end) {
        status = interrupted();
        if (!status.ok()) {
          break;
        }
        const BlobRelocationLiveValue& next = references[next_reference];
        if (next.stored_value_size > kIndirectBlobGCReadBatchBytes) {
          BlobFileReader* const reader =
              input.normal_metadata != nullptr
                  ? readers[run_input].normal.get()
                  : readers[run_input].relocation.GetValue();
          if (reader == nullptr || table_builder == nullptr) {
            status = Status::Corruption("Missing streaming blob GC dependency");
            break;
          }
          std::string encoded;
          // Streaming looks the value up by its stable identity.
          BlobIndex::EncodeIndirectBlob(
              &encoded, origin,
              {.origin_file_number = origin,
               .original_value_offset = next.original_value_offset,
               .stored_value_size = next.stored_value_size,
               .compression = next.compression,
               .stable_checksum = next.stable_checksum,
               .stored_checksum = next.stored_checksum});
          BlobIndex index;
          status = index.DecodeFrom(encoded);
          if (!status.ok()) {
            break;
          }
          Status source_status;
          Status delivery_status;
          status = table_builder->AddBlobRelocation(
              origin, next.original_value_offset, next.stored_value_size,
              next.compression, next.stored_checksum,
              [&](const BlobChunkConsumer& consumer) {
                const auto deliver = [&](const Slice& chunk) {
                  const Status interrupt_status = interrupted();
                  delivery_status = interrupt_status.ok() ? consumer(chunk)
                                                          : interrupt_status;
                  return delivery_status;
                };
                source_status = reader->relocation() != nullptr
                                    ? reader->relocation()->StreamStoredBlob(
                                          gc_read_options, index, deliver)
                                    : reader->StreamStoredBlobForRelocation(
                                          gc_read_options, index, deliver);
                return source_status;
              });
          if (source_status.ok()) {
            // Count the bytes the stream read, as the batch path below does:
            // the stored bytes, plus the record trailer for a relocation file.
            input_bytes_read +=
                next.stored_value_size + (reader->relocation() != nullptr
                                              ? kSimpleGen2BlobTrailerSize
                                              : 0);
          }
          const bool source_fault =
              source_status.IsCorruption() || source_status.IsNotSupported();
          bool output_failed =
              !delivery_status.ok() || (!status.ok() && source_status.ok());
#ifndef NDEBUG
          const std::string stream_status = status.ToString();
#endif
          TEST_SYNC_POINT_CALLBACK(
              "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", &status);
#ifndef NDEBUG
          // In debug builds, a test-injected destination failure also counts as
          // an output failure. Status equality compares only the error code, so
          // the status text is compared instead.
          output_failed |= status.ToString() != stream_status;
#endif
          if (!source_status.ok() && !source_fault) {
            // A source error other than Corruption or NotSupported (for example
            // I/O or abort) is returned before a later output error, as in the
            // batch path below.
            status = source_status;
          }
          if (!status.ok()) {
            // A corrupt or unsupported source value fails its input, and the
            // unfinished output is not published. Keep a destination failure
            // even when the source also failed.
            if (source_fault && !output_failed) {
              if (source_status.IsCorruption()) {
                note_corruption(inputs[run_input].file_number, source_status);
              }
              candidate_local_failure_input = run_input;
              candidate_local_failure_reason = "blob stream source is corrupt";
              status = Status::OK();
            }
            break;
          }
          output.value_bytes += next.stored_value_size;
          ++output.live_count;
          ++next_reference;
          continue;
        }
        const size_t batch_begin = next_reference;
        // A batch counts each value's stored bytes plus
        // kIndirectBlobGCBatchBytesPerValue (see its comment), so it stays
        // within the charged workspace also for tiny values.
        // ReadCoalescedRanges() counts direct-read alignment in its buffer
        // limit.
        const uint64_t per_value_bytes = kIndirectBlobGCBatchBytesPerValue;
        uint64_t batch_bytes = 0;
        while (next_reference < run_end) {
          const uint64_t value_size =
              references[next_reference].stored_value_size;
          if (value_size > kIndirectBlobGCReadBatchBytes) {
            // A large value is streamed on its own by the branch above.
            assert(next_reference != batch_begin);
            break;
          }
          const uint64_t value_cost = value_size + per_value_bytes;
          if (next_reference != batch_begin &&
              value_cost >
                  kIndirectBlobGCReadBatchBytes -
                      std::min(batch_bytes, kIndirectBlobGCReadBatchBytes)) {
            break;
          }
          batch_bytes += value_cost;
          ++next_reference;
          if (batch_bytes >= kIndirectBlobGCReadBatchBytes) {
            break;
          }
        }
        const size_t batch_size = next_reference - batch_begin;
        std::vector<Slice> stored_payloads(batch_size);
        std::vector<Status> read_statuses(batch_size);
        CoalescedReadBuffers read_buffers;
        const uint64_t coalesce_bytes =
            mutable_cf_options.indirect_blob_gc_read_coalesce_bytes;
        uint64_t bytes_read = 0;
        if (input.normal_metadata != nullptr) {
          std::vector<StoredNormalBlobReadRequest> requests(batch_size);
          for (size_t i = 0; i < batch_size; ++i) {
            const BlobRelocationLiveValue& reference =
                references[batch_begin + i];
            requests[i] =
                StoredNormalBlobReadRequest{reference.original_value_offset,
                                            reference.stored_value_size,
                                            reference.compression,
                                            &stored_payloads[i],
                                            &read_statuses[i],
                                            reference.stored_checksum};
          }
          BlobFileReader* const reader = readers[run_input].normal.get();
          if (reader == nullptr) {
            status =
                Status::Corruption("Indirect blob GC reader is unavailable");
            break;
          }
          reader->MultiGetStoredBlobsForRelocation(
              gc_read_options, coalesce_bytes, requests.size(), requests.data(),
              kIndirectBlobGCReadBatchBytes, &read_buffers, &bytes_read);
        } else {
          std::vector<StoredRelocationFileReadRequest> requests(batch_size);
          for (size_t i = 0; i < batch_size; ++i) {
            const BlobRelocationLiveValue& reference =
                references[batch_begin + i];
            requests[i] =
                StoredRelocationFileReadRequest{origin,
                                                reference.original_value_offset,
                                                reference.stored_value_size,
                                                reference.compression,
                                                reference.stable_checksum,
                                                &stored_payloads[i],
                                                &read_statuses[i],
                                                reference.stored_checksum};
          }
          const BlobFileReader* const reader =
              readers[run_input].relocation.GetValue();
          if (reader == nullptr || reader->relocation() == nullptr) {
            status = Status::Corruption(
                "Indirect blob GC relocation reader is unavailable");
            break;
          }
          reader->relocation()->MultiGetStoredBlobs(
              gc_read_options, coalesce_bytes, requests.size(), requests.data(),
              kIndirectBlobGCReadBatchBytes, &read_buffers, &bytes_read);
        }
        input_bytes_read += bytes_read;
        // A read past the buffer limit is deferred (Incomplete). Keep the
        // values before the first deferred one; the next batch starts there.
        size_t read_count = batch_size;
        for (size_t i = 0; i < batch_size; ++i) {
          if (read_statuses[i].IsIncomplete()) {
            read_count = i;
            break;
          }
        }
        if (read_count == 0) {
          status = Status::Corruption("Indirect blob GC read made no progress");
          break;
        }
        next_reference = batch_begin + read_count;
        Status candidate_status;
        Status external_status;
        for (size_t i = 0; i < read_count; ++i) {
          const Status& read_status = read_statuses[i];
          if (read_status.ok()) {
            continue;
          }
          if (read_status.IsCorruption() || read_status.IsMemoryLimit() ||
              read_status.IsNotSupported()) {
            if (candidate_status.ok()) {
              candidate_status = read_status;
            }
          } else if (external_status.ok()) {
            external_status = read_status;
          }
        }
        // A corrupt, too large or unsupported source record fails only its
        // input. Record that first. A source I/O error or an output error still
        // has priority and is returned.
        const bool candidate_failed = !candidate_status.ok();
        if (!external_status.ok()) {
          status = external_status;
          break;
        }
        if (table_builder == nullptr) {
          status =
              Status::Corruption("Indirect blob GC builder is unavailable");
          break;
        }
        for (size_t i = 0; status.ok() && i < read_count; ++i) {
          if (!read_statuses[i].ok()) {
            continue;
          }
          const BlobRelocationLiveValue& reference =
              references[batch_begin + i];
          status = table_builder->AddBlobRelocation(
              origin, reference.original_value_offset, stored_payloads[i],
              reference.compression);
          TEST_SYNC_POINT_CALLBACK(
              "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", &status);
          if (status.ok()) {
            output.value_bytes += reference.stored_value_size;
            ++output.live_count;
          }
        }
        // Keep an output failure even when another record in this batch is
        // corrupt. All read statuses were checked first, so a source I/O error
        // also keeps its priority.
        if (status.ok() && candidate_failed) {
          if (candidate_status.IsCorruption()) {
            note_corruption(inputs[run_input].file_number, candidate_status);
          }
          candidate_local_failure_input = run_input;
          candidate_local_failure_reason =
              "blob source record is corrupt or too large";
          break;
        }
      }
    }

    if (table_builder != nullptr) {
      if (status.ok() &&
          candidate_local_failure_input == std::numeric_limits<size_t>::max()) {
        status = table_builder->Finish();
        output.file_size = table_builder->FileSize();
        if (status.ok()) {
          status =
              table_builder->GetBlobRelocationFileMetadata(&output.metadata);
        }
        if (status.ok()) {
          status = BlobRelocationDirectory::DecodeFrom(
              Slice(output.metadata.encoded_directory), &output.directory);
        }
        // Install check: the output holds exactly the live values of its
        // inputs. Its directory has one origin entry per origin run, with the
        // same count, first and last offset and record bytes, and it serves
        // exactly the generation numbers of its inputs.
        size_t expected_live_count = 0;
        for (size_t input_index : bins[output_index].item_indexes) {
          expected_live_count += inputs[input_index].live_count;
        }
        if (status.ok() &&
            (output.live_count != expected_live_count ||
             output.directory.total_entries() != expected_live_count ||
             output.directory.origins().size() != output_runs.size())) {
          status = Status::Corruption(
              "Blob relocation output does not match the live values");
        }
        for (size_t k = 0; status.ok() && k < output_runs.size(); ++k) {
          const BlobRelocationOriginRun& run = output_runs[k].run;
          const std::vector<BlobRelocationLiveValue>& values =
              inputs[output_runs[k].input_index].live_set->references();
          uint64_t run_record_bytes = 0;
          for (size_t i = run.begin; i < run.end; ++i) {
            run_record_bytes += uint64_t{values[i].stored_value_size} +
                                kSimpleGen2BlobTrailerSize;
          }
          const BlobRelocationOrigin& origin = output.directory.origins()[k];
          if (run.begin >= run.end ||
              origin.origin_file_number != run.origin_file_number ||
              origin.entry_count != run.end - run.begin ||
              origin.first_original_offset !=
                  values[run.begin].original_value_offset ||
              origin.last_original_offset !=
                  values[run.end - 1].original_value_offset ||
              origin.record_bytes != run_record_bytes) {
            status = Status::Corruption(
                "Blob relocation output does not match the live values");
          }
        }
        if (status.ok()) {
          std::vector<uint64_t> expected_served = served_generation_numbers;
          expected_served.erase(
              std::unique(expected_served.begin(), expected_served.end()),
              expected_served.end());
          if (output.directory.served_file_numbers() != expected_served) {
            status = Status::Corruption(
                "Blob relocation output does not serve its input generation "
                "numbers");
          }
        }
      } else {
        table_builder->Abandon();
      }
      IOStatus builder_io_status = table_builder->io_status();
      if (status.ok() && !builder_io_status.ok()) {
        status = builder_io_status;
      } else {
        builder_io_status.PermitUncheckedError();
      }
      table_builder.reset();
    }
    if (file_writer != nullptr) {
      IOOptions io_options;
      if (status.ok() &&
          candidate_local_failure_input == std::numeric_limits<size_t>::max()) {
        status =
            WritableFileWriter::PrepareIOOptions(write_options, io_options);
      }
      if (status.ok() &&
          candidate_local_failure_input == std::numeric_limits<size_t>::max()) {
        status = db_->immutable_db_options_.use_fsync
                     ? file_writer->Fsync(io_options)
                     : file_writer->Sync(io_options);
      }
      IOStatus close_status = file_writer->Close(io_options);
      if (status.ok() && !close_status.ok()) {
        status = close_status;
      } else {
        close_status.PermitUncheckedError();
      }
      if (status.ok()) {
        output.checksum_value = file_writer->GetFileChecksum();
        output.checksum_method = file_writer->GetFileChecksumFuncName();
        if (output.checksum_value == kUnknownFileChecksum) {
          output.checksum_value.clear();
        }
        if (output.checksum_method == kUnknownFileChecksumFuncName) {
          output.checksum_method.clear();
        }
        if (output.checksum_method.empty() != output.checksum_value.empty()) {
          status = Status::Corruption(
              "Incomplete blob relocation file checksum metadata");
        }
      }
      file_writer.reset();
    }
    if (status.ok() &&
        candidate_local_failure_input == std::numeric_limits<size_t>::max()) {
      // Check that the output file is not larger than its inputs.
      if (output.file_size > output.input_bytes ||
          output.file_size >
              std::numeric_limits<uint64_t>::max() - aggregate_output_bytes ||
          output.value_bytes > std::numeric_limits<uint64_t>::max() -
                                   total_relocated_value_bytes ||
          output.live_count >
              std::numeric_limits<size_t>::max() - total_relocated_count) {
        candidate_local_failure_input = bins[output_index].item_indexes.front();
        candidate_local_failure_reason =
            "relocation output exceeds its bounded packing plan";
      } else {
        aggregate_output_bytes += output.file_size;
        total_relocated_value_bytes += output.value_bytes;
        total_relocated_count += output.live_count;
      }
    }
  }

  if (status.ok() &&
      candidate_local_failure_input == std::numeric_limits<size_t>::max() &&
      !outputs.empty() && aggregate_output_bytes >= aggregate_input_bytes) {
    for (size_t input_index : eligible_inputs) {
      suppressed_files.push_back(inputs[input_index].file_number);
      suppression_reasons.emplace_back(
          "packed relocation outputs would not reclaim bytes");
      inputs[input_index].eligible = false;
    }
    eligible_inputs.clear();
  }

  if (status.ok() &&
      candidate_local_failure_input == std::numeric_limits<size_t>::max() &&
      !eligible_inputs.empty()) {
    IOOptions io_options;
    TEST_SYNC_POINT("DBImpl::RunIndirectBlobGC:BeforeRenameOutputs");
    for (OutputState& output : outputs) {
      output.rename_attempted = true;
      status =
          db_->fs_->RenameFile(output.temporary_path, output.path, io_options,
                               /*dbg=*/nullptr);
      if (!status.ok()) {
        break;
      }
      output.renamed = true;
    }
    if (status.ok() && !outputs.empty()) {
      FSDirectory* const data_dir = db_->GetDataDir(cfd, 0);
      status = data_dir == nullptr
                   ? Status::Corruption("Missing blob GC data directory")
                   : data_dir->FsyncWithDirOptions(
                         IOOptions(), nullptr,
                         DirFsyncOptions(
                             DirFsyncOptions::FsyncReason::kNewFileSynced));
    }
  }

  for (OutputState& output : outputs) {
    if (!output.creation_started) {
      continue;
    }
    Status callback_input_status = status;
    if (callback_input_status.ok() &&
        (candidate_local_failure_input != std::numeric_limits<size_t>::max() ||
         eligible_inputs.empty())) {
      callback_input_status =
          Status::TryAgain("Blob relocation batch was not published");
    }
    if (!output.renamed) {
      // The output was not renamed, or the rename reported failure. Report a
      // failed creation and do not register the file with the SstFileManager.
      // The cleanup deletes the final name of a tried rename (see
      // rename_attempted).
      db_->blob_callback_.OnBlobFileAbandoned(
          output.path, cfd->GetName(), job_id, output.file_number,
          BlobFileCreationReason::kIndirectBlobGC,
          callback_input_status.ok()
              ? Status::TryAgain("Blob relocation output was not renamed")
              : callback_input_status);
      callback_input_status.PermitUncheckedError();
      continue;
    }
    const Status completion_status = db_->blob_callback_.OnBlobFileCompleted(
        output.path, cfd->GetName(), job_id, output.file_number,
        BlobFileCreationReason::kIndirectBlobGC, callback_input_status,
        output.checksum_value, output.checksum_method, output.live_count,
        output.file_size, /*set_bg_error_on_space_limit=*/true);
    callback_input_status.PermitUncheckedError();
    if (status.ok() &&
        candidate_local_failure_input == std::numeric_limits<size_t>::max() &&
        !eligible_inputs.empty()) {
      status = completion_status;
    } else {
      completion_status.PermitUncheckedError();
    }
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:AfterOutputWork",
                           &status);

  if (candidate_local_failure_input != std::numeric_limits<size_t>::max()) {
    const uint64_t failed_file =
        inputs[candidate_local_failure_input].file_number;
    suppressed_files.push_back(failed_file);
    suppression_reasons.emplace_back(candidate_local_failure_reason);
    eligible_inputs.clear();
    // Corrupt blob data (a record, or the header or footer of the file) means
    // that user values cannot be read. Compaction fails on such data, so with
    // paranoid_checks (the default) GC fails too: the job returns Corruption,
    // and the caller sets a background error with reason kCompaction. Without
    // paranoid_checks, GC only skips the file. Finish() logs the file as an
    // ERROR and counts INDIRECT_BLOB_GC_CORRUPT_INPUTS in both cases.
    const auto corrupt = corrupt_inputs.find(failed_file);
    if (corrupt != corrupt_inputs.end() && status.ok() &&
        db_->immutable_db_options_.paranoid_checks) {
      status = Status::Corruption("Indirect blob GC input #" +
                                      std::to_string(failed_file) +
                                      " has corrupt blob data",
                                  corrupt->second.detail);
      corrupt->second.fails_job = true;
    }
  }

  const bool keep_outputs = status.ok() && !eligible_inputs.empty();
  if (!keep_outputs) {
    for (OutputState& output : outputs) {
      if (!output.creation_started) {
        continue;
      }
      db_->DeleteObsoleteFileImpl(job_id, output.temporary_path,
                                  output_directory, kTempFile,
                                  output.file_number);
      if (output.rename_attempted) {
        db_->DeleteObsoleteFileImpl(job_id, output.path, output_directory,
                                    kBlobFile, output.file_number);
      }
      output.renamed = false;
    }
  }

  readers.clear();
  for (PhysicalInput& input : inputs) {
    if (input.live_set != nullptr) {
      Status release_status = input.live_set->ReleaseWorkingMemory();
      if (status.ok() && !release_status.ok()) {
        status = release_status;
      } else {
        release_status.PermitUncheckedError();
      }
      input.live_set.reset();
    }
  }

  read_input_count = eligible_inputs.size();
  bins.clear();
  source_table_options.reset();
}

// Builds the MANIFEST edit of the job from the census snapshot and the
// routes of the input Version (see "Install protocol" in Install()). Called
// without the DB mutex.
Status DBImpl::IndirectBlobGCJob::BuildEdit() {
  if (census == nullptr) {
    return Status::Corruption("Blob GC census snapshot is unavailable");
  }
  output_needed.fill(false);
  published_output_count = 0;
  published_relocated_count = 0;
  published_relocated_value_bytes = 0;

  // A generation number is live if an SST holds it in the census snapshot,
  // which shows the input Version. A running compaction can write only
  // generation numbers that its input SSTs hold, so this is enough (see
  // "Install protocol" in Install()).
  const auto generation_is_live = [&](uint64_t generation) {
    return census->GetSstCount(generation) != 0;
  };
  const auto output_of = [&](const PhysicalInput& input) -> size_t {
    return input.output_index < outputs.size()
               ? input.output_index
               : std::numeric_limits<size_t>::max();
  };
  for (size_t input_index : eligible_inputs) {
    const PhysicalInput& input = inputs[input_index];
    if (std::none_of(input.generations.begin(), input.generations.end(),
                     generation_is_live)) {
      continue;
    }
    const size_t output_index = output_of(input);
    if (output_index == std::numeric_limits<size_t>::max()) {
      // Invariant: the same census snapshot decides liveness here and the
      // outputs in DecideInputs(), which gives an output to every input
      // with a live generation number.
      return invariant_failed("Indirect blob GC live input has no output");
    }
    output_needed[output_index] = true;
  }

  edit = std::make_unique<VersionEdit>();
  edit->SetColumnFamily(column_family.GetID());
  for (size_t i = 0; i < outputs.size(); ++i) {
    if (!output_needed[i]) {
      continue;
    }
    const OutputState& output = outputs[i];
    edit->AddBlobRelocationFile(BlobRelocationFileAddition(
        output.file_number, output.file_size, output.checksum_method,
        output.checksum_value, output.metadata.encoded_directory,
        output.metadata.tail_size));
    ++published_output_count;
    if (output.live_count >
        std::numeric_limits<size_t>::max() - published_relocated_count) {
      return Status::Corruption("Published blob GC count overflow");
    }
    published_relocated_count += output.live_count;
    if (output.value_bytes > std::numeric_limits<uint64_t>::max() -
                                 published_relocated_value_bytes) {
      return Status::Corruption("Published blob GC byte count overflow");
    }
    published_relocated_value_bytes += output.value_bytes;
  }

  // Each live generation number of an input is routed to the input's output
  // (for a relocation input this re-points the routes into it); a routed
  // generation number that no SST uses loses its route. The input leaves the
  // Version.
  for (size_t input_index : eligible_inputs) {
    const PhysicalInput& input = inputs[input_index];
    const size_t output_index = output_of(input);
    const uint64_t output_file_number =
        output_index != std::numeric_limits<size_t>::max() &&
                output_needed[output_index]
            ? outputs[output_index].file_number
            : kInvalidBlobFileNumber;
    for (uint64_t generation : input.generations) {
      const bool routed = generation != input.file_number;
      if (generation_is_live(generation)) {
        if (output_file_number == kInvalidBlobFileNumber) {
          // Invariant: the same census snapshot decides liveness here and
          // the outputs in DecideInputs() (see the check above).
          return invariant_failed(
              "Indirect blob GC live generation number has no output");
        }
        edit->SetBlobRelocationRoute(
            BlobRelocationRoute(generation, output_file_number));
      } else if (routed) {
        edit->DeleteBlobRelocationRoute(generation);
      }
    }
    if (input.normal_metadata != nullptr) {
      edit->RetireBlobFile(input.file_number);
    } else {
      edit->DeleteBlobRelocationFile(input.file_number);
    }
  }
  return Status::OK();
}

// At the MANIFEST queue head, check that each input is still valid:
//   - A normal input is still an indirect origin with the same shared
//     metadata. A compaction that adds garbage to it keeps its shared
//     metadata, and only an indirect blob GC install retires an indirect
//     origin. So this check is defensive. A change makes a normal stale job.
//   - No input became pinned by a coarse SST (see the selection above for
//     what pins an input). This is a normal stale job too. Example: a flush
//     takes blob file number 25, indirect blob GC selects normal input 28,
//     and then the flush installs its coarse SST with frontier 25 (see
//     "Coarse SST" in db/blob/blob_reference_metadata.h), which pins file 28.
//   - Invariant check (invariant_failed()), which cannot fail: a relocation
//     input has the same metadata and the same number of routes into it.
//     Only indirect blob GC adds or removes routes and relocation files,
//     and only one GC job runs per CF (indirect_blob_gc_cfs_in_progress_),
//     so an equal route count means the same routes.
Status DBImpl::IndirectBlobGCJob::ValidatePhysicalInputs() {
  db_->mutex_.AssertHeld();
  Version* const current = column_family.current();
  if (column_family.IsDropped() || current == nullptr) {
    return stale_input("Indirect blob GC column family changed");
  }
  VersionStorageInfo* const current_storage = current->storage_info();
  for (size_t input_index : eligible_inputs) {
    const PhysicalInput& input = inputs[input_index];
    const bool coarse_pinned =
        input.normal_metadata != nullptr
            ? current_storage->IsCoarseIndirectOriginBlobPinned(
                  input.file_number)
            : std::any_of(
                  input.routed_generations.begin(),
                  input.routed_generations.end(), [&](uint64_t generation) {
                    return current_storage->IsCoarseIndirectOriginBlobPinned(
                        generation);
                  });
    if (coarse_pinned) {
      return stale_input(
          "Indirect blob GC input generation number became coarse");
    }
    if (input.normal_metadata != nullptr) {
      const std::shared_ptr<BlobFileMetaData> current_metadata =
          current_storage->GetBlobFileMetaData(input.file_number);
      if (current_metadata == nullptr ||
          !current_metadata->IsIndirectOrigin() ||
          current_metadata->GetSharedMeta() !=
              input.normal_metadata->GetSharedMeta()) {
        return stale_input("Indirect blob GC indirect-origin input changed");
      }
      continue;
    }
    if (current_storage->GetBlobRelocationFileMetaData(input.file_number) !=
            input.relocation_metadata ||
        current_storage->GetBlobRelocationRouteTargetCount(input.file_number) !=
            input.routed_generations.size()) {
      return invariant_failed("Indirect blob GC relocation input changed");
    }
  }
  return Status::OK();
}

void DBImpl::IndirectBlobGCJob::Install() {
  db_->mutex_.Lock();

  if (!status.ok()) {
    Version* const current = cfd->current();
    if (status.IsCompactionTooLarge() && !cfd->IsDropped() &&
        current != nullptr &&
        current->GetVersionNumber() == input_version_number) {
      current->storage_info()->DeferBlobFilesForIndirectBlobGC(
          capacity_deferred_files);
    }
    delete_renamed_outputs.fill(true);
    return;
  }
  Version* current = cfd->current();
  if (cfd->IsDropped() || current == nullptr) {
    status = stale_input("Indirect blob GC column family changed");
    delete_renamed_outputs.fill(true);
    return;
  }
  status = interrupted();
  if (!status.ok()) {
    delete_renamed_outputs.fill(true);
    return;
  }
  if (read_input_count == 0) {
    VersionStorageInfo* const current_storage = current->storage_info();
    for (size_t i = 0; i < suppressed_files.size(); ++i) {
      current_storage->SuppressBlobFileForIndirectBlobGC(suppressed_files[i]);
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Indirect blob GC skipped file #%" PRIu64 ": %s",
                       cfd->GetName().c_str(), suppressed_files[i],
                       suppression_reasons[i].c_str());
    }
    if (deferred_input ||
        current_storage->BlobPhysicalFileForIndirectBlobGC() !=
            kInvalidBlobFileNumber) {
      db_->EnqueuePendingCompaction(cfd);
    }
    // `status` is OK here, so the job returns OK.
    return;
  }

  db_->mutex_.Unlock();
  status = BuildEdit();
  db_->mutex_.Lock();
  status = locked_census_status(std::move(status));
  if (!status.ok()) {
    delete_renamed_outputs.fill(true);
    return;
  }

  SuperVersionContext superversion_context(/*create_superversion=*/true);
  // Install protocol: the job installs its edit with one LogAndApply() call.
  // The edit comes from the census snapshot and the routes of the input
  // Version. The job does not follow the Version changes after its snapshot.
  //
  // Why the snapshot is enough: the edit removes the route of a generation
  // number that the snapshot shows as unused, and it removes every input. This
  // is safe only if no SST that the snapshot does not count holds a
  // generation number of an input. No job adds such an SST after the
  // snapshot:
  // - A compaction keeps or drops the references of its input SSTs and never
  //   adds a reference to an existing blob file. So every SST added after the
  //   snapshot got its references from SSTs of the input Version, directly or
  //   through earlier compactions.
  // - A flush references only the new blob files that it writes. They are not
  //   inputs of this job.
  // - Ingestion and import are assumed to add no SST with kIndirectBlob
  //   references. This code does not check it.
  // So the snapshot can only count a holder too many (an SST that a later
  // compaction removed). The edit then keeps a route that no SST needs, which
  // is safe. The routes of the input Version are also still current: only
  // indirect blob GC changes routes, and this job holds the only GC job
  // registration of the census. ValidatePhysicalInputs() also checks that the
  // number of routes into each relocation input is unchanged.
  //
  // The precondition of the call runs under the DB mutex at the MANIFEST queue
  // head, and again after the unlocked apply of the edit (see the pre_cb
  // paragraph of VersionSet::LogAndApply()). Each run does these steps:
  //   1. It returns the interrupted() status at shutdown or compaction abort.
  //   2. It checks that the column family is still alive and that the job is
  //      still registered (IsGcJobActive()).
  //   3. It records in other_install_during_job whether the current Version is
  //      still the input Version.
  //   4. It checks the inputs again (ValidatePhysicalInputs()).
  //   5. On its first run, it starts the metadata commit
  //      (BeginMetadataCommit()). Until EndMetadataCommit() after the MANIFEST
  //      write, no other GC job or metadata commit can start. When the write
  //      result is unknown, BeginGcJob() returns Busy until recovery.
  // Flushes and compactions do not wait for the GC job or its commit. A
  // compaction that runs during the job writes only generation numbers that
  // its input SSTs hold, and those SSTs are in the census snapshot.
  //
  // metadata_commit_intent is true after BeginMetadataCommit() succeeded, so
  // that EndMetadataCommit() runs after LogAndApply(). other_install_during_job
  // stays true when the precondition never runs.
  bool metadata_commit_intent = false;
  bool other_install_during_job = true;
  status = db_->versions_->LogAndApply(
      cfd, read_options, manifest_write_options, edit.get(), &db_->mutex_,
      db_->directories_.GetDbDir(),
      /*new_descriptor_log=*/false,
      /*column_family_options=*/nullptr,
      /*manifest_wcb=*/{}, [&]() {
        Status precondition_status = interrupted();
        if (!precondition_status.ok()) {
          return precondition_status;
        }
        Version* const precondition_current = column_family.current();
        if (column_family.IsDropped() || precondition_current == nullptr ||
            census == nullptr) {
          return stale_input("Indirect blob GC column family changed");
        }
        if (!blob_reference_census->IsGcJobActive(gc_job_token)) {
          return stale_input("Blob GC job registration ended");
        }
        other_install_during_job =
            precondition_current->GetVersionNumber() != input_version_number;
        precondition_status = ValidatePhysicalInputs();
        bool installable = precondition_status.ok();
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::RunIndirectBlobGC:InstallPrecondition", &installable);
        if (precondition_status.ok() && !installable) {
          precondition_status = stale_input("Indirect blob GC input changed");
        }
        if (precondition_status.ok() && !metadata_commit_intent) {
          precondition_status = blob_reference_census->BeginMetadataCommit();
          metadata_commit_intent = precondition_status.ok();
        }
        return precondition_status;
      });
  if (metadata_commit_intent) {
    // If the MANIFEST write fails with an I/O error, the edit may still be in
    // the MANIFEST. Then indirect blob GC and metadata commits stay blocked
    // until recovery shows the result. A rejection before the append is a clear
    // failure, and the block is released.
    blob_reference_census->EndMetadataCommit(status.ok() ||
                                             db_->versions_->io_status().ok());
  }
  if (status.ok()) {
    db_->InstallSuperVersionAndScheduleWork(cfd, &superversion_context,
                                            /*defer_snapshot_refresh=*/false);
    TEST_SYNC_POINT("DBImpl::RunIndirectBlobGC:Installed");
    *made_progress = true;
    installed = true;
    published_input_count = read_input_count;
    current = cfd->current();
    TEST_SYNC_POINT_CALLBACK("DBImpl::RunIndirectBlobGC:AfterMetadataInstall",
                             cfd);
    if (current == nullptr) {
      status = Status::Corruption(
          "Indirect blob GC installed an unavailable Version");
    }
  }
  if (status.ok() && current != nullptr) {
    VersionStorageInfo* const current_storage = current->storage_info();
    // The job decided its suppressions from the input Version. A newer Version
    // can hold SST metadata that makes a suppressed input eligible, so the
    // suppressions apply to the current Version only when no other Version was
    // installed during the job.
    // other_install_during_job comes from the last precondition run, which
    // LogAndApply() makes right before this install.
    const bool starting_suppressions_are_current = !other_install_during_job;
    if (starting_suppressions_are_current) {
      for (uint64_t file_number : suppressed_files) {
        current_storage->SuppressBlobFileForIndirectBlobGC(file_number);
      }
    }
    // A new output has no garbage yet. Suppress it in this Version, so that the
    // next pick in this Version takes other candidates instead.
    for (size_t i = 0; i < outputs.size(); ++i) {
      if (output_needed[i]) {
        current_storage->SuppressBlobFileForIndirectBlobGC(
            outputs[i].file_number);
      } else {
        delete_renamed_outputs[i] = true;
      }
    }
    if (!starting_suppressions_are_current || deferred_input ||
        current_storage->BlobPhysicalFileForIndirectBlobGC() !=
            kInvalidBlobFileNumber) {
      db_->EnqueuePendingCompaction(cfd);
    }
    RecordTick(db_->stats_, BLOB_DB_GC_NUM_FILES, published_input_count);
    RecordTick(db_->stats_, BLOB_DB_GC_NUM_NEW_FILES, published_output_count);
    RecordTick(db_->stats_, BLOB_DB_GC_NUM_KEYS_RELOCATED,
               published_relocated_count);
    RecordTick(db_->stats_, BLOB_DB_GC_BYTES_RELOCATED,
               published_relocated_value_bytes);
    RecordTick(db_->stats_, INDIRECT_BLOB_GC_KEYS_RELOCATED,
               published_relocated_count);
    RecordTick(db_->stats_, INDIRECT_BLOB_GC_BYTES_RELOCATED,
               published_relocated_value_bytes);
    uint64_t removed_bytes = 0;
    for (size_t input_index : eligible_inputs) {
      removed_bytes += inputs[input_index].physical_bytes;
    }
    for (size_t i = 0; i < outputs.size(); ++i) {
      if (output_needed[i]) {
        bytes_written += outputs[i].file_size;
      }
    }
    RecordTick(db_->stats_, INDIRECT_BLOB_GC_BYTES_RECLAIMED,
               removed_bytes - std::min(removed_bytes, bytes_written));
    ROCKS_LOG_BUFFER(log_buffer,
                     "[%s] Indirect blob GC packed %" ROCKSDB_PRIszt
                     " physical inputs into %" ROCKSDB_PRIszt
                     " relocation files (%" ROCKSDB_PRIszt " live blobs)",
                     cfd->GetName().c_str(), published_input_count,
                     published_output_count, published_relocated_count);
  }

  if (!status.ok()) {
    const autovector<uint64_t>& quarantined =
        db_->error_handler_.GetFilesToQuarantine();
    for (size_t i = 0; i < outputs.size(); ++i) {
      if (outputs[i].renamed) {
        delete_renamed_outputs[i] =
            std::find(quarantined.begin(), quarantined.end(),
                      outputs[i].file_number) == quarantined.end();
      }
    }
  }
  db_->mutex_.Unlock();
  superversion_context.Clean();
  db_->mutex_.Lock();
}

void DBImpl::IndirectBlobGCJob::RecordOutcome(const Status& result,
                                              size_t skipped_count) {
  db_->mutex_.AssertHeld();
  Statistics* const stats = db_->stats_;
  const char* outcome = nullptr;
  if (installed) {
    outcome = "installed";
    RecordTick(stats, INDIRECT_BLOB_GC_JOBS_INSTALLED);
  } else if (result.IsTryAgain() && stale_input_is_retryable) {
    outcome = "stale";
    RecordTick(stats, INDIRECT_BLOB_GC_JOBS_STALE);
  } else if (result.ok() || result.IsCompactionTooLarge() ||
             result.IsIncomplete() || result.IsShutdownInProgress() ||
             result.IsColumnFamilyDropped()) {
    outcome = "nothing installed";
    RecordTick(stats, INDIRECT_BLOB_GC_JOBS_NOTHING_INSTALLED);
  } else {
    outcome = "failed";
    RecordTick(stats, INDIRECT_BLOB_GC_JOBS_FAILED);
    RecordTick(stats, BLOB_DB_GC_FAILURES);
  }
  RecordTick(stats, INDIRECT_BLOB_GC_BYTES_READ, input_bytes_read);
  const uint64_t now_micros = db_->immutable_db_options_.clock->NowMicros();
  RecordInHistogram(stats, INDIRECT_BLOB_GC_JOB_MICROS,
                    now_micros - std::min(now_micros, start_micros));
  ROCKS_LOG_BUFFER(
      log_buffer,
      "[%s] [JOB %d] Indirect blob GC job ended: %s; %" ROCKSDB_PRIszt
      " candidates, %" ROCKSDB_PRIszt " inputs read, %" ROCKSDB_PRIszt
      " installed, %" ROCKSDB_PRIszt " skipped, %" ROCKSDB_PRIszt
      " outputs, %" ROCKSDB_PRIszt " values relocated, %" PRIu64
      " bytes read, %" PRIu64 " bytes written; status: %s",
      cfd->GetName().c_str(), job_id, outcome,
      candidate_physical_file_numbers.size(), read_input_count,
      published_input_count, skipped_count, published_output_count,
      published_relocated_count, input_bytes_read, bytes_written,
      result.ToString().c_str());
  // Candidates that remain start the idle time again (see
  // ColumnFamilyData::NoteIndirectBlobGcCandidates()).
  if (!cfd->IsDropped() &&
      cfd->current()->storage_info()->BlobPhysicalFileForIndirectBlobGC() !=
          kInvalidBlobFileNumber) {
    cfd->NoteIndirectBlobGcCandidates(now_micros);
  }
}

Status DBImpl::IndirectBlobGCJob::Finish() {
  db_->mutex_.AssertHeld();
  blob_reference_census->EndGcJob(gc_job_token);
  gc_job_token = 0;
  input_version->UnrefAndFreeBlobRelocationState(&db_->mutex_);
  db_->mutex_.Unlock();
  if (status.IsCompactionTooLarge()) {
    for (uint64_t file_number : capacity_deferred_files) {
      suppressed_files.push_back(file_number);
      // Keeps suppression_reasons parallel to suppressed_files.
      suppression_reasons.emplace_back("deferred: outputs or disk space");
      transient_suppressions.insert(file_number);
    }
  }
  // A corrupt input reaches suppressed_files in the phase that finds it. The
  // exception is corrupt exact SST metadata that ReadLiveSets() finds:
  // DecideInputs() adds that input. DecideInputs() does not reach the input in
  // three cases:
  // 1. The read phase ends with an error status (a census mismatch, an
  //    interrupt or a read error in a later SST batch), so DecideInputs()
  //    decides nothing.
  // 2. A census mismatch remains after a rebuild, so plan_end is 0.
  // 3. DecideInputs() stops early, at a memory deferral of an earlier input or
  //    at an error.
  // The input would then be dropped, so add every corrupt input that is not in
  // suppressed_files yet. The loop below logs it and counts it, and
  // SuppressWorkerCandidates() suppresses it in the census.
  for (const auto& corrupt : corrupt_inputs) {
    if (std::find(suppressed_files.begin(), suppressed_files.end(),
                  corrupt.first) == suppressed_files.end()) {
      suppressed_files.push_back(corrupt.first);
      // Keeps suppression_reasons parallel to suppressed_files.
      suppression_reasons.emplace_back("corrupt input");
    }
  }
  // Install() logs the skipped inputs (INFO) only when the job accepted no
  // input, so log them here when the job installed inputs. Every corrupt input
  // counts INDIRECT_BLOB_GC_CORRUPT_INPUTS here and gets one ERROR line here,
  // unless the code that found it already wrote one.
  for (size_t i = 0; i < suppressed_files.size(); ++i) {
    const uint64_t file_number = suppressed_files[i];
    const auto corrupt = corrupt_inputs.find(file_number);
    if (corrupt != corrupt_inputs.end()) {
      RecordTick(db_->stats_, INDIRECT_BLOB_GC_CORRUPT_INPUTS);
      if (!corrupt->second.logged) {
        corrupt->second.logged = true;
        ROCKS_LOG_ERROR(db_->immutable_db_options_.info_log,
                        "[%s] [JOB %d] Indirect blob GC found corrupt input "
                        "#%" PRIu64 ": %s; %s",
                        cfd->GetName().c_str(), job_id, file_number,
                        corrupt->second.detail.c_str(),
                        corrupt->second.fails_job
                            ? "the job fails with a background error "
                              "(paranoid_checks)"
                            : "the file is skipped until it changes, the "
                              "census is rebuilt or the coarse frontier "
                              "rises (relocation files only)");
      }
    } else if (published_input_count > 0 && i < suppression_reasons.size()) {
      ROCKS_LOG_BUFFER(
          log_buffer, "[%s] Indirect blob GC skipped file #%" PRIu64 ": %s",
          cfd->GetName().c_str(), file_number, suppression_reasons[i].c_str());
    }
  }
  std::set<uint64_t> corrupt_files;
  for (const auto& corrupt : corrupt_inputs) {
    corrupt_files.insert(corrupt.first);
  }
  blob_reference_census->SuppressWorkerCandidates(
      candidate_stamp, suppressed_files, transient_suppressions,
      db_->immutable_db_options_.clock->NowMicros(), corrupt_files);
  for (size_t i = 0; i < outputs.size(); ++i) {
    OutputState& output = outputs[i];
    if (output.renamed && delete_renamed_outputs[i]) {
      db_->DeleteObsoleteFileImpl(job_id, output.path, output_directory,
                                  kBlobFile, output.file_number);
      output.renamed = false;
    }
  }
  edit.reset();
  outputs.clear();
  eligible_inputs.clear();
  inputs.clear();
  // Release the SST reader charge (selected_reader_bytes) here, without the DB
  // mutex, not in the return-scope cleanup after the caller takes the DB mutex
  // again.
  memory_tracker->SetReservation(&selected_reader_bytes, 0)
      .PermitUncheckedError();
  memory_tracker.reset();
  census.reset();
  suppressed_files.clear();
  suppression_reasons.clear();
  db_->mutex_.Lock();
  db_->ScheduleBlobReferenceCensus(cfd);
  return status;
}

// Marks the job as a stale job that a later job can retry, and returns TryAgain
// with `message`.
Status DBImpl::IndirectBlobGCJob::stale_input(const char* message) {
  stale_input_is_retryable = true;
  return Status::TryAgain(message);
}

// An invariant check that should never fail: `message` names a state that the
// rules of indirect blob GC exclude (see "Install protocol" in Install()).
// Debug builds assert. Release builds log a WARN line and end the job as a
// stale job, which installs nothing; a later job starts from the current
// Version. It is not Corruption, because the job installs nothing.
Status DBImpl::IndirectBlobGCJob::invariant_failed(const char* message) {
  assert(false);
  ROCKS_LOG_WARN(db_->immutable_db_options_.info_log,
                 "[%s] Indirect blob GC invariant check failed: %s",
                 cfd->GetName().c_str(), message);
  return stale_input(message);
}

// Maps a census status without the DB mutex. Start() uses it for the status of
// SnapshotSelected(). An Aborted status means that the job registration ended
// (CF drop, census cancel). It makes the job stale, so a later job retries it.
// At shutdown the job returns ShutdownInProgress. Neither is a background
// error. With the DB mutex held, use locked_census_status() instead.
Status DBImpl::IndirectBlobGCJob::unlocked_census_status(Status census_status) {
  if (census_status.IsAborted()) {
    if (db_->shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress("DB shutdown during indirect blob GC");
    }
    return stale_input("Blob GC job registration ended");
  }
  return census_status;
}

// Maps a census status with the DB mutex held (Start() and Install()). When the
// status is Aborted (the job registration ended), it returns
// ColumnFamilyDropped if the column family was dropped, ShutdownInProgress if
// the DB is shutting down, or else a stale job. Other statuses stay unchanged.
Status DBImpl::IndirectBlobGCJob::locked_census_status(Status census_status) {
  db_->mutex_.AssertHeld();
  if (!census_status.IsAborted()) {
    return census_status;
  }
  if (column_family.IsDropped()) {
    return Status::ColumnFamilyDropped(
        "Column family dropped during indirect blob GC");
  }
  if (db_->shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress("DB shutdown during indirect blob GC");
  }
  return stale_input("Blob GC job registration ended");
}

// Returns ShutdownInProgress at DB shutdown, Incomplete when compactions are
// aborted for the whole DB or for this column family, and OK otherwise.
Status DBImpl::IndirectBlobGCJob::interrupted() {
  if (db_->shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }
  if (db_->IsCompactionAborted(cfd)) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }
  return Status::OK();
}

// Cache pressure is transient: back off every candidate of this job, so the
// retried pick can run other candidates instead of the same ones again. The
// census retries them later; the delay doubles after each failure, from 1 s up
// to 32 s (fixed choices, not measured).
void DBImpl::IndirectBlobGCJob::suppress_candidates_transiently(
    const char* reason) {
  for (uint64_t file_number : candidate_physical_file_numbers) {
    if (transient_suppressions.insert(file_number).second) {
      suppressed_files.push_back(file_number);
      suppression_reasons.emplace_back(reason);
    }
  }
}

// Marks the input at `input_index` as failed in its plan. Only the first reason
// is kept.
void DBImpl::IndirectBlobGCJob::fail_input(size_t input_index,
                                           const char* reason) {
  InputPlan& plan = plans[input_index];
  if (!plan.failed) {
    plan.failed = true;
    plan.reason = reason;
  }
}

bool DBImpl::EnoughRoomForCompaction(
    ColumnFamilyData* cfd, const std::vector<CompactionInputFiles>& inputs,
    bool* sfm_reserved_compact_space, LogBuffer* log_buffer) {
  // Check if we have enough room to do the compaction
  bool enough_room = true;
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm) {
    // Pass the current bg_error_ to SFM so it can decide what checks to
    // perform. If this DB instance hasn't seen any error yet, the SFM can be
    // optimistic and not do disk space checks
    Status bg_error = error_handler_.GetBGError();
    enough_room = sfm->EnoughRoomForCompaction(cfd, inputs, bg_error);
    bg_error.PermitUncheckedError();  // bg_error is just a copy of the Status
                                      // from the error_handler_
    if (enough_room) {
      *sfm_reserved_compact_space = true;
    }
  }
  if (!enough_room) {
    // Just in case tests want to change the value of enough_room
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction():CancelledCompaction", &enough_room);
    ROCKS_LOG_BUFFER(log_buffer,
                     "Cancelled compaction because not enough room");
    RecordTick(stats_, COMPACTION_CANCELLED, 1);
  }
  return enough_room;
}

bool DBImpl::RequestCompactionToken(ColumnFamilyData* cfd, bool force,
                                    std::unique_ptr<TaskLimiterToken>* token,
                                    LogBuffer* log_buffer) {
  assert(*token == nullptr);
  auto limiter = static_cast<ConcurrentTaskLimiterImpl*>(
      cfd->ioptions().compaction_thread_limiter.get());
  if (limiter == nullptr) {
    return true;
  }
  *token = limiter->GetToken(force);
  if (*token != nullptr) {
    ROCKS_LOG_BUFFER(log_buffer,
                     "Thread limiter [%s] increase [%s] compaction task, "
                     "force: %s, tasks after: %d",
                     limiter->GetName().c_str(), cfd->GetName().c_str(),
                     force ? "true" : "false", limiter->GetOutstandingTask());
    return true;
  }
  return false;
}

bool DBImpl::ShouldRescheduleFlushRequestToRetainUDT(
    const FlushRequest& flush_req) {
  mutex_.AssertHeld();
  assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
  ColumnFamilyData* cfd = flush_req.cfd_to_max_mem_id_to_persist.begin()->first;
  if (cfd->GetAndClearFlushSkipReschedule()) {
    return false;
  }
  uint64_t max_memtable_id =
      flush_req.cfd_to_max_mem_id_to_persist.begin()->second;
  if (cfd->IsDropped() ||
      !cfd->ShouldPostponeFlushToRetainUDT(max_memtable_id)) {
    return false;
  }
  // Check if holding on the flush will cause entering write stall mode.
  // Write stall entered because of the accumulation of write buffers can be
  // alleviated if we continue with the flush instead of postponing it.
  const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();

  // Use the same criteria as WaitUntilFlushWouldNotStallWrites does w.r.t
  // defining what a write stall is about to happen means. If this uses a
  // stricter criteria, for example, a write stall is about to happen if the
  // last memtable is 10% full, there is a possibility that manual flush could
  // be waiting in `WaitUntilFlushWouldNotStallWrites` with the incorrect
  // expectation that others will clear up the excessive memtables and
  // eventually let it proceed. The others in this case won't start clearing
  // until the last memtable is 10% full. To avoid that scenario, the criteria
  // this uses should be the same or less strict than
  // `WaitUntilFlushWouldNotStallWrites` does.
  WriteStallCondition write_stall =
      ColumnFamilyData::GetWriteStallConditionAndCause(
          cfd->GetUnflushedMemTableCountForWriteStallCheck(),
          /*num_l0_files=*/0,
          /*num_compaction_needed_bytes=*/0, mutable_cf_options,
          cfd->ioptions(), cfd->is_table_builder_supported())
          .first;
  if (write_stall != WriteStallCondition::kNormal) {
    return false;
  }
  return true;
}

IOStatus DBImpl::SyncClosedWals(const WriteOptions& write_options,
                                JobContext* job_context,
                                VersionEdit* synced_wals,
                                bool error_recovery_in_prog) {
  TEST_SYNC_POINT("DBImpl::SyncClosedWals:Start");

  IOStatus io_s = SyncWalImpl(/*include_current_wal*/ false, write_options,
                              job_context, synced_wals, error_recovery_in_prog);
  if (!io_s.ok()) {
    TEST_SYNC_POINT("DBImpl::SyncClosedWals:Failed");
  } else {
    TEST_SYNC_POINT("DBImpl::SyncClosedWals:end");
  }
  return io_s;
}

Status DBImpl::FlushMemTableToOutputFile(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    bool* made_progress, JobContext* job_context, FlushReason flush_reason,
    SuperVersionContext* superversion_context, LogBuffer* log_buffer,
    Env::Priority thread_pri) {
  mutex_.AssertHeld();
  assert(cfd);
  assert(cfd->imm());
  assert(cfd->imm()->NumNotFlushed() != 0);
  assert(cfd->imm()->IsFlushPending());
  assert(versions_);
  assert(versions_->GetColumnFamilySet());
  const ReadOptions read_options(Env::IOActivity::kFlush);
  const WriteOptions write_options(Env::IOActivity::kFlush);
  // If there are more than one column families, we need to make sure that
  // all the log files except the most recent one are synced. Otherwise if
  // the host crashes after flushing and before WAL is persistent, the
  // flushed SST may contain data from write batches whose updates to
  // other (unflushed) column families are missing.
  //
  // When 2PC is enabled, non-recent WAL(s) may be needed for crash-recovery,
  // even when there is only one CF in the DB, for prepared transactions that
  // had not been committed yet. Make sure we sync them to keep the persisted
  // WAL state at least as new as the persisted SST state.
  const bool needs_to_sync_closed_wals =
      cur_wal_number_ > 0 &&
      (versions_->GetColumnFamilySet()->NumberOfColumnFamilies() > 1 ||
       allow_2pc());

  // If needs_to_sync_closed_wals is true, we need to record the current
  // maximum memtable ID of this column family so that a later PickMemtables()
  // call will not pick memtables whose IDs are higher. This is due to the fact
  // that SyncClosedWals() may release the db mutex, and memtable switch can
  // happen for this column family in the meantime. The newly created memtables
  // have their data backed by unsynced WALs, thus they cannot be included in
  // this flush job.
  // Another reason why we must record the current maximum memtable ID of this
  // column family: SyncClosedWals() may release db mutex, thus it's possible
  // for application to continue to insert into memtables increasing db's
  // sequence number. The application may take a snapshot, but this snapshot is
  // not included in `snapshot_seqs` which will be passed to flush job because
  // `snapshot_seqs` has already been computed before this function starts.
  // Recording the max memtable ID ensures that the flush job does not flush
  // a memtable without knowing such snapshot(s).
  uint64_t max_memtable_id =
      needs_to_sync_closed_wals
          ? cfd->imm()->GetLatestMemTableID(false /* for_atomic_flush */)
          : std::numeric_limits<uint64_t>::max();

  // If needs_to_sync_closed_wals is false, the flush job picks all existing
  // immutable memtables before NotifyOnFlushBegin() releases the DB mutex.
  // Until that pick, the DB mutex freezes the eligible input boundary; after
  // the pick, later writes remain outside this flush. Transaction modes that
  // need an additional visibility boundary install a managed snapshot or a
  // published-sequence pin in InitSnapshotContext(). Together these boundaries
  // preserve snapshots created after the snapshot-list scan even though their
  // creation does not require the DB mutex.

  FlushJob flush_job(
      dbname_, cfd, immutable_db_options_, mutable_cf_options, max_memtable_id,
      file_options_for_compaction_, versions_.get(), &mutex_, &shutting_down_,
      job_context, flush_reason, log_buffer, directories_.GetDbDir(),
      GetDataDir(cfd, 0U),
      GetCompressionFlush(cfd->ioptions(), mutable_cf_options), stats_,
      &event_logger_, mutable_cf_options.report_bg_io_stats,
      true /* sync_output_directory */, true /* write_manifest */, thread_pri,
      io_tracer_, cfd->GetSuperVersion()->ShareSeqnoToTimeMapping(), db_id_,
      db_session_id_, cfd->GetFullHistoryTsLow(), &blob_callback_,
      mutable_db_options_.fast_sst_open);
  FileMetaData file_meta;

  Status s;
  bool need_cancel = false;
  IOStatus log_io_s = IOStatus::OK();
  if (needs_to_sync_closed_wals) {
    // SyncClosedWals() may unlock and re-lock the wal_write_mutex multiple
    // times.
    VersionEdit synced_wals;
    bool error_recovery_in_prog = error_handler_.IsRecoveryInProgress();
    mutex_.Unlock();
    log_io_s = SyncClosedWals(write_options, job_context, &synced_wals,
                              error_recovery_in_prog);
    mutex_.Lock();
    if (log_io_s.ok() && synced_wals.IsWalAddition()) {
      log_io_s = status_to_io_status(
          ApplyWALToManifest(read_options, write_options, &synced_wals));
      TEST_SYNC_POINT_CALLBACK("DBImpl::FlushMemTableToOutputFile:CommitWal:1",
                               nullptr);
    }

    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
    }
  } else {
    TEST_SYNC_POINT("DBImpl::SyncClosedWals:Skip");
  }
  s = log_io_s;

  // If the log sync failed, we do not need to pick memtable. Otherwise,
  // num_flush_not_started_ needs to be rollback.
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:BeforePickMemtables");
  // Exit a flush due to bg error should not set bg error again.
  bool skip_set_bg_error = false;
  if (s.ok() && !error_handler_.GetBGError().ok() &&
      error_handler_.IsBGWorkStopped() &&
      flush_reason != FlushReason::kErrorRecovery &&
      flush_reason != FlushReason::kErrorRecoveryRetryFlush) {
    // Error recovery in progress, should not pick memtable which excludes
    // them from being picked up by recovery flush.
    // This ensures that when bg error is set, no new flush can pick
    // memtables.
    skip_set_bg_error = true;
    s = error_handler_.GetBGError();
    assert(!s.ok());
    ROCKS_LOG_BUFFER(log_buffer,
                     "[JOB %d] Skip flush due to background error %s",
                     job_context->job_id, s.ToString().c_str());
  }

  if (s.ok()) {
    flush_job.PickMemTable();
    need_cancel = true;
  }
  TEST_SYNC_POINT_CALLBACK(
      "DBImpl::FlushMemTableToOutputFile:AfterPickMemtables", &flush_job);

  // may temporarily unlock and lock the mutex.
  NotifyOnFlushBegin(cfd, &file_meta, mutable_cf_options, job_context->job_id,
                     flush_reason);

  bool switched_to_mempurge = false;
  std::vector<uint64_t> prepared_blob_memtable_ids;
  // Within flush_job.Run, rocksdb may call event listener to notify
  // file creation and deletion.
  //
  // Note that flush_job.Run will unlock and lock the db_mutex,
  // and EventListener callback will be called when the db_mutex
  // is unlocked by the current thread.
  if (s.ok()) {
    auto mgr_handle = cfd->blob_partition_manager_handle();
    auto* mgr = mgr_handle.get();
    if (mgr != nullptr) {
      prepared_blob_memtable_ids = MemTableIds(flush_job.GetMemTables());
      std::vector<BlobFileAddition> write_path_additions;
      std::vector<BlobFileGarbage> write_path_garbages;
      std::vector<std::vector<uint64_t>> generation_blob_file_numbers;
      mutex_.Unlock();
      s = mgr->PrepareFlushAdditions(
          write_options, prepared_blob_memtable_ids, &write_path_additions,
          &write_path_garbages, &generation_blob_file_numbers);
      mutex_.Lock();
      if (!s.ok()) {
        prepared_blob_memtable_ids.clear();
      } else if (!write_path_additions.empty()) {
        const auto& memtables = flush_job.GetMemTables();
        assert(generation_blob_file_numbers.size() == memtables.size());
        for (size_t i = 0; i < generation_blob_file_numbers.size(); ++i) {
          // Old SuperVersions can keep these memtables alive after the flush
          // commits, so keep their sealed direct-write blob files protected
          // from obsolete-file purge until the memtable is finally released.
          memtables[i]->ProtectSealedBlobFiles(mgr_handle,
                                               generation_blob_file_numbers[i]);
        }
        flush_job.AddExternalBlobFileAdditions(std::move(write_path_additions));
        flush_job.AddExternalBlobFileGarbages(std::move(write_path_garbages));
      } else if (!write_path_garbages.empty()) {
        flush_job.AddExternalBlobFileGarbages(std::move(write_path_garbages));
      }
    }
  }
  if (s.ok()) {
    s = flush_job.Run(&logs_with_prep_tracker_, &file_meta,
                      &switched_to_mempurge, &skip_set_bg_error,
                      &error_handler_);
    need_cancel = false;
  }

  // Reserve a pending flush-completion notification before the DB mutex can be
  // released, so that a Flush(wait=true, listener_wait=true) caller cannot
  // observe this flush as finished until NotifyOnFlushCompleted() below has
  // run. flush_job.Run() removed the memtables from the immutable list (which
  // is what unblocks the waiter) and the DB mutex has been held continuously
  // since, so there is no window in which the waiter could see the flush
  // committed with a zero count. Reserved only when a notification will
  // actually fire (matches the NotifyOnFlushCompleted call below): flush
  // committed successfully, not a mempurge, listeners are registered, and this
  // job committed at least one flush result to report.
  const bool reserved_flush_notification =
      s.ok() && !switched_to_mempurge &&
      !immutable_db_options_.listeners.empty() &&
      !flush_job.GetCommittedFlushJobsInfo()->empty();
  if (reserved_flush_notification) {
    cfd->imm()->AddPendingFlushNotifications(1);
  }
  // Release the reservation on every exit path (RAII) so a future early return
  // cannot leak the count and hang listener_wait waiters. The DB mutex is held
  // throughout this function, including at scope exit, which
  // SubPendingFlushNotifications() requires. Runs after
  // NotifyOnFlushCompleted() below has returned.
  Defer release_flush_notification([&] {
    if (reserved_flush_notification) {
      cfd->imm()->SubPendingFlushNotifications(1);
    }
  });

  if (cfd->blob_partition_manager() != nullptr &&
      !prepared_blob_memtable_ids.empty()) {
    auto unconsumed_additions = flush_job.TakeExternalBlobFileAdditions();
    auto unconsumed_garbages = flush_job.TakeExternalBlobFileGarbages();
    if (!s.ok() || !unconsumed_additions.empty() ||
        !unconsumed_garbages.empty()) {
      // Skip committing this attempt, but keep the prepared generations queued
      // in pending_generations_. The same immutable memtables still reference
      // these sealed blob files, so a later flush retry must publish the exact
      // same additions rather than abandoning them here.
      prepared_blob_memtable_ids.clear();
    }
  }

  if (!s.ok() && need_cancel) {
    cfd->imm()->RollbackMemtableFlush(flush_job.GetMemTables(),
                                      /*rollback_succeeding_memtables=*/false);
    flush_job.Cancel();
  }

  if (s.ok() && !prepared_blob_memtable_ids.empty()) {
    cfd->blob_partition_manager()->CommitFlushedGenerations(
        cfd->imm()->GetEarliestMemTableID());
  }

  if (s.ok()) {
    InstallSuperVersionAndScheduleWork(cfd, superversion_context,
                                       /*defer_snapshot_refresh=*/false);
    if (made_progress) {
      *made_progress = true;
    }

    const std::string& column_family_name = cfd->GetName();

    Version* const current = cfd->current();
    assert(current);

    const VersionStorageInfo* const storage_info = current->storage_info();
    assert(storage_info);

    VersionStorageInfo::LevelSummaryStorage tmp{};
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                     column_family_name.c_str(),
                     storage_info->LevelSummary(&tmp));

    const auto& blob_files = storage_info->GetBlobFiles();
    if (!blob_files.empty()) {
      assert(blob_files.front());
      assert(blob_files.back());

      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
          column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
          blob_files.back()->GetBlobFileNumber());
    }
  }

  if (!s.ok() && !s.IsShutdownInProgress() && !s.IsColumnFamilyDropped() &&
      !skip_set_bg_error) {
    if (log_io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      if (!versions_->io_status().ok()) {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the Manifest write will be map to soft error.
        // TODO: kManifestWriteNoWAL and kFlushNoWAL are misleading. Refactor is
        // needed.
        error_handler_.SetBGError(s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the other SST file write errors will be set as
        // kFlushNoWAL.
        error_handler_.SetBGError(s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      assert(s == log_io_s);
      error_handler_.SetBGError(s, BackgroundErrorReason::kFlush);
    }
  }
  // If flush ran smoothly and no mempurge happened
  // install new SST file path.
  if (s.ok() && (!switched_to_mempurge)) {
    // may temporarily unlock and lock the mutex.
    NotifyOnFlushCompleted(cfd, mutable_cf_options,
                           flush_job.GetCommittedFlushJobsInfo());
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm) {
      // Notify sst_file_manager that a new file was added
      std::string file_path = MakeTableFileName(
          cfd->ioptions().cf_paths[0].path, file_meta.fd.GetNumber());
      // TODO (PR7798).  We should only add the file to the FileManager if it
      // exists. Otherwise, some tests may fail.  Ignore the error in the
      // interim.
      sfm->OnAddFile(file_path).PermitUncheckedError();
      if (sfm->IsMaxAllowedSpaceReached()) {
        Status new_bg_error =
            Status::SpaceLimit("Max allowed space was reached");
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
            &new_bg_error);
        error_handler_.SetBGError(new_bg_error, BackgroundErrorReason::kFlush);
      }
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTableToOutputFile:Finish");
  return s;
}

Status DBImpl::FlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
#ifndef NDEBUG
  for (const auto& bg_flush_arg : bg_flush_args) {
    assert(bg_flush_arg.atomic_flush_ == bg_flush_args[0].atomic_flush_);
  }
#endif /* !NDEBUG */
  if (bg_flush_args[0].atomic_flush_) {
    return AtomicFlushMemTablesToOutputFiles(
        bg_flush_args, made_progress, job_context, log_buffer, thread_pri);
  }
  assert(bg_flush_args.size() == 1);
  InitSnapshotContext(job_context);

  const auto& bg_flush_arg = bg_flush_args[0];
  ColumnFamilyData* cfd = bg_flush_arg.cfd_;
  // intentional infrequent copy for each flush
  const MutableCFOptions mutable_cf_options_copy =
      cfd->GetLatestMutableCFOptions();
  SuperVersionContext* superversion_context =
      bg_flush_arg.superversion_context_;
  FlushReason flush_reason = bg_flush_arg.flush_reason_;
  Status s = FlushMemTableToOutputFile(
      cfd, mutable_cf_options_copy, made_progress, job_context, flush_reason,
      superversion_context, log_buffer, thread_pri);
  return s;
}

/*
 * Atomically flushes multiple column families.
 *
 * For each column family, all memtables with ID smaller than or equal to the
 * ID specified in bg_flush_args will be flushed. Only after all column
 * families finish flush will this function commit to MANIFEST. If any of the
 * column families are not flushed successfully, this function does not have
 * any side-effect on the state of the database.
 */
Status DBImpl::AtomicFlushMemTablesToOutputFiles(
    const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
    JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri) {
  mutex_.AssertHeld();
  const ReadOptions read_options(Env::IOActivity::kFlush);
  const WriteOptions write_options(Env::IOActivity::kFlush);

  autovector<ColumnFamilyData*> cfds;
  for (const auto& arg : bg_flush_args) {
    cfds.emplace_back(arg.cfd_);
  }

#ifndef NDEBUG
  for (const auto cfd : cfds) {
    assert(cfd->imm()->NumNotFlushed() != 0);
    assert(cfd->imm()->IsFlushPending());
  }
  for (const auto& bg_flush_arg : bg_flush_args) {
    assert(bg_flush_arg.flush_reason_ == bg_flush_args[0].flush_reason_);
  }
#endif /* !NDEBUG */

  InitSnapshotContext(job_context);
  autovector<FSDirectory*> distinct_output_dirs;
  autovector<std::string> distinct_output_dir_paths;
  std::vector<std::unique_ptr<FlushJob>> jobs;
  std::vector<MutableCFOptions> all_mutable_cf_options;
  int num_cfs = static_cast<int>(cfds.size());
  all_mutable_cf_options.reserve(num_cfs);
  for (int i = 0; i < num_cfs; ++i) {
    auto cfd = cfds[i];
    FSDirectory* data_dir = GetDataDir(cfd, 0U);
    const std::string& curr_path = cfd->ioptions().cf_paths[0].path;

    // Add to distinct output directories if eligible. Use linear search. Since
    // the number of elements in the vector is not large, performance should be
    // tolerable.
    bool found = false;
    for (const auto& path : distinct_output_dir_paths) {
      if (path == curr_path) {
        found = true;
        break;
      }
    }
    if (!found) {
      distinct_output_dir_paths.emplace_back(curr_path);
      distinct_output_dirs.emplace_back(data_dir);
    }

    all_mutable_cf_options.emplace_back(cfd->GetLatestMutableCFOptions());
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.back();
    uint64_t max_memtable_id = bg_flush_args[i].max_memtable_id_;
    FlushReason flush_reason = bg_flush_args[i].flush_reason_;
    jobs.emplace_back(new FlushJob(
        dbname_, cfd, immutable_db_options_, mutable_cf_options,
        max_memtable_id, file_options_for_compaction_, versions_.get(), &mutex_,
        &shutting_down_, job_context, flush_reason, log_buffer,
        directories_.GetDbDir(), data_dir,
        GetCompressionFlush(cfd->ioptions(), mutable_cf_options), stats_,
        &event_logger_, mutable_cf_options.report_bg_io_stats,
        false /* sync_output_directory */, false /* write_manifest */,
        thread_pri, io_tracer_,
        cfd->GetSuperVersion()->ShareSeqnoToTimeMapping(), db_id_,
        db_session_id_, cfd->GetFullHistoryTsLow(), &blob_callback_,
        mutable_db_options_.fast_sst_open));
  }

  std::vector<FileMetaData> file_meta(num_cfs);
  // Use of deque<bool> because vector<bool>
  // is specific and doesn't allow &v[i].
  std::deque<bool> switched_to_mempurge(num_cfs, false);
  Status s;
  IOStatus log_io_s = IOStatus::OK();
  assert(num_cfs == static_cast<int>(jobs.size()));

  for (int i = 0; i != num_cfs; ++i) {
    const MutableCFOptions& mutable_cf_options = all_mutable_cf_options.at(i);
    // may temporarily unlock and lock the mutex.
    FlushReason flush_reason = bg_flush_args[i].flush_reason_;
    NotifyOnFlushBegin(cfds[i], &file_meta[i], mutable_cf_options,
                       job_context->job_id, flush_reason);
  }

  if (cur_wal_number_ > 0) {
    // TODO (yanqin) investigate whether we should sync the closed logs for
    // single column family case.
    VersionEdit synced_wals;
    bool error_recovery_in_prog = error_handler_.IsRecoveryInProgress();
    mutex_.Unlock();
    log_io_s = SyncClosedWals(write_options, job_context, &synced_wals,
                              error_recovery_in_prog);
    mutex_.Lock();
    if (log_io_s.ok() && synced_wals.IsWalAddition()) {
      log_io_s = status_to_io_status(
          ApplyWALToManifest(read_options, write_options, &synced_wals));
    }

    if (!log_io_s.ok() && !log_io_s.IsShutdownInProgress() &&
        !log_io_s.IsColumnFamilyDropped()) {
      if (wals_total_size_.LoadRelaxed() > 0) {
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlush);
      } else {
        // If the WAL is empty, we use different error reason
        error_handler_.SetBGError(log_io_s, BackgroundErrorReason::kFlushNoWAL);
      }
    }
  }
  s = log_io_s;

  // exec_status stores the execution status of flush_jobs as
  // <bool /* executed */, Status /* status code */>
  autovector<std::pair<bool, Status>> exec_status;
  std::vector<bool> pick_status;
  std::vector<std::vector<uint64_t>> prepared_blob_memtable_ids(num_cfs);
  for (int i = 0; i != num_cfs; ++i) {
    // Initially all jobs are not executed, with status OK.
    exec_status.emplace_back(false, Status::OK());
    pick_status.push_back(false);
  }

  bool flush_for_recovery =
      bg_flush_args[0].flush_reason_ == FlushReason::kErrorRecovery ||
      bg_flush_args[0].flush_reason_ == FlushReason::kErrorRecoveryRetryFlush;
  bool skip_set_bg_error = false;

  if (s.ok() && !error_handler_.GetBGError().ok() &&
      error_handler_.IsBGWorkStopped() && !flush_for_recovery) {
    s = error_handler_.GetBGError();
    skip_set_bg_error = true;
    assert(!s.ok());
    ROCKS_LOG_BUFFER(log_buffer,
                     "[JOB %d] Skip flush due to background error %s",
                     job_context->job_id, s.ToString().c_str());
  }

  if (s.ok()) {
    for (int i = 0; i != num_cfs; ++i) {
      jobs[i]->PickMemTable();
      pick_status[i] = true;
    }
  }

  if (s.ok()) {
    for (int i = 0; i != num_cfs; ++i) {
      auto mgr_handle = cfds[i]->blob_partition_manager_handle();
      auto* mgr = mgr_handle.get();
      if (mgr == nullptr) {
        continue;
      }

      prepared_blob_memtable_ids[i] = MemTableIds(jobs[i]->GetMemTables());
      std::vector<BlobFileAddition> write_path_additions;
      std::vector<BlobFileGarbage> write_path_garbages;
      std::vector<std::vector<uint64_t>> generation_blob_file_numbers;
      mutex_.Unlock();
      s = mgr->PrepareFlushAdditions(
          write_options, prepared_blob_memtable_ids[i], &write_path_additions,
          &write_path_garbages, &generation_blob_file_numbers);
      mutex_.Lock();
      if (!s.ok()) {
        prepared_blob_memtable_ids[i].clear();
        break;
      }
      if (!write_path_additions.empty()) {
        const auto& memtables = jobs[i]->GetMemTables();
        assert(generation_blob_file_numbers.size() == memtables.size());
        for (size_t j = 0; j < generation_blob_file_numbers.size(); ++j) {
          memtables[j]->ProtectSealedBlobFiles(mgr_handle,
                                               generation_blob_file_numbers[j]);
        }
        jobs[i]->AddExternalBlobFileAdditions(std::move(write_path_additions));
      }
      if (!write_path_garbages.empty()) {
        jobs[i]->AddExternalBlobFileGarbages(std::move(write_path_garbages));
      }
    }
  }

  if (s.ok()) {
    assert(switched_to_mempurge.size() ==
           static_cast<long unsigned int>(num_cfs));
    // TODO (yanqin): parallelize jobs with threads.
    for (int i = 1; i != num_cfs; ++i) {
      exec_status[i].second =
          jobs[i]->Run(&logs_with_prep_tracker_, &file_meta[i],
                       &(switched_to_mempurge.at(i)));
      exec_status[i].first = true;
    }
    if (num_cfs > 1) {
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1");
      TEST_SYNC_POINT(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2");
    }
    assert(exec_status.size() > 0);
    assert(!file_meta.empty());
    exec_status[0].second = jobs[0]->Run(
        &logs_with_prep_tracker_, file_meta.data() /* &file_meta[0] */,
        switched_to_mempurge.empty() ? nullptr : &(switched_to_mempurge.at(0)));
    exec_status[0].first = true;

    Status error_status;
    for (const auto& e : exec_status) {
      if (!e.second.ok()) {
        s = e.second;
        if (!e.second.IsShutdownInProgress() &&
            !e.second.IsColumnFamilyDropped()) {
          // If a flush job did not return OK, and the CF is not dropped, and
          // the DB is not shutting down, then we have to return this result to
          // caller later.
          error_status = e.second;
        }
      }
    }

    s = error_status.ok() ? s : error_status;
  }

  if (s.IsColumnFamilyDropped()) {
    s = Status::OK();
  }

  if (s.ok() || s.IsShutdownInProgress()) {
    // Sync on all distinct output directories.
    for (auto dir : distinct_output_dirs) {
      if (dir != nullptr) {
        IOOptions io_options;
        Status error_status =
            WritableFileWriter::PrepareIOOptions(write_options, io_options);
        if (error_status.ok()) {
          error_status = dir->FsyncWithDirOptions(
              io_options, nullptr,
              DirFsyncOptions(DirFsyncOptions::FsyncReason::kNewFileSynced));
        }
        if (!error_status.ok()) {
          s = error_status;
          break;
        }
      }
    }
  } else if (!skip_set_bg_error) {
    // When `skip_set_bg_error` is true, no memtable is picked so
    // there is no need to call Cancel() or RollbackMemtableFlush().
    //
    // Need to undo atomic flush if something went wrong, i.e. s is not OK and
    // it is not because of CF drop.
    // Have to cancel the flush jobs that have NOT executed because we need to
    // unref the versions and make their picked memtables flushable again.
    for (int i = 0; i != num_cfs; ++i) {
      if (pick_status[i] && !exec_status[i].first) {
        cfds[i]->imm()->RollbackMemtableFlush(
            jobs[i]->GetMemTables(), /*rollback_succeeding_memtables=*/false);
        jobs[i]->Cancel();
      }
    }
    for (int i = 0; i != num_cfs; ++i) {
      if (exec_status[i].second.ok() && exec_status[i].first) {
        auto& mems = jobs[i]->GetMemTables();
        cfds[i]->imm()->RollbackMemtableFlush(
            mems, /*rollback_succeeding_memtables=*/false);
      }
    }
  }

  if (s.ok()) {
    const auto wait_to_install_func =
        [&]() -> std::pair<Status, bool /*continue to wait*/> {
      if (!versions_->io_status().ok()) {
        // Something went wrong elsewhere, we cannot count on waiting for our
        // turn to write/sync to MANIFEST or CURRENT. Just return.
        return std::make_pair(versions_->io_status(), false);
      } else if (shutting_down_.load(std::memory_order_acquire)) {
        return std::make_pair(Status::ShutdownInProgress(), false);
      }
      bool ready = true;
      for (size_t i = 0; i != cfds.size(); ++i) {
        const auto& mems = jobs[i]->GetMemTables();
        if (cfds[i]->IsDropped()) {
          // If the column family is dropped, then do not wait.
          continue;
        } else if (!mems.empty() &&
                   cfds[i]->imm()->GetEarliestMemTableID() < mems[0]->GetID()) {
          // If a flush job needs to install the flush result for mems and
          // mems[0] is not the earliest memtable, it means another thread must
          // be installing flush results for the same column family, then the
          // current thread needs to wait.
          ready = false;
          break;
        } else if (mems.empty() && cfds[i]->imm()->GetEarliestMemTableID() <=
                                       bg_flush_args[i].max_memtable_id_) {
          // If a flush job does not need to install flush results, then it has
          // to wait until all memtables up to max_memtable_id_ (inclusive) are
          // installed.
          ready = false;
          break;
        }
      }
      return std::make_pair(Status::OK(), !ready);
    };

    bool resuming_from_bg_err =
        error_handler_.IsDBStopped() || flush_for_recovery;
    while ((!resuming_from_bg_err || error_handler_.GetRecoveryError().ok())) {
      std::pair<Status, bool> res = wait_to_install_func();

      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitToCommit", &res);

      if (!res.first.ok()) {
        s = res.first;
        break;
      } else if (!res.second) {
        // we are the oldest immutable memtable
        break;
      }
      // We are not the oldest immutable memtable
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitCV", &res);
      //
      // If bg work is stopped, recovery thread first calls
      // WaitForBackgroundWork() before proceeding to flush for recovery. This
      // flush can block WaitForBackgroundWork() while waiting for recovery
      // flush to install result. To avoid this deadlock, we should abort here
      // if there is background error.
      if (!flush_for_recovery && error_handler_.IsBGWorkStopped() &&
          !error_handler_.GetBGError().ok()) {
        s = error_handler_.GetBGError();
        assert(!s.ok());
        break;
      }
      atomic_flush_install_cv_.Wait();

      resuming_from_bg_err = error_handler_.IsDBStopped() || flush_for_recovery;
    }

    if (!resuming_from_bg_err) {
      // If not resuming from bg err, then we determine future action based on
      // whether we hit background error.
      if (s.ok()) {
        s = error_handler_.GetBGError();
      }
    } else if (s.ok()) {
      // If resuming from bg err, we still rely on wait_to_install_func()'s
      // result to determine future action. If wait_to_install_func() returns
      // non-ok already, then we should not proceed to flush result
      // installation.
      s = error_handler_.GetRecoveryError();
    }
    // Since we are not installing these memtables, need to rollback
    // to allow future flush job to pick up these memtables.
    if (!s.ok()) {
      for (int i = 0; i != num_cfs; ++i) {
        assert(exec_status[i].first);
        assert(exec_status[i].second.ok());
        auto& mems = jobs[i]->GetMemTables();
        cfds[i]->imm()->RollbackMemtableFlush(
            mems, /*rollback_succeeding_memtables=*/false);
      }
    }
  }

  if (s.ok()) {
    autovector<ColumnFamilyData*> tmp_cfds;
    autovector<const autovector<ReadOnlyMemTable*>*> mems_list;
    autovector<FileMetaData*> tmp_file_meta;
    autovector<std::list<std::unique_ptr<FlushJobInfo>>*>
        committed_flush_jobs_info;
    for (int i = 0; i != num_cfs; ++i) {
      const auto& mems = jobs[i]->GetMemTables();
      if (!cfds[i]->IsDropped() && !mems.empty()) {
        tmp_cfds.emplace_back(cfds[i]);
        mems_list.emplace_back(&mems);
        tmp_file_meta.emplace_back(&file_meta[i]);
        committed_flush_jobs_info.emplace_back(
            jobs[i]->GetCommittedFlushJobsInfo());
      }
    }

    s = InstallMemtableAtomicFlushResults(
        nullptr /* imm_lists */, tmp_cfds, mems_list, versions_.get(),
        &logs_with_prep_tracker_, &mutex_, tmp_file_meta,
        committed_flush_jobs_info, &job_context->memtables_to_free,
        directories_.GetDbDir(), log_buffer);
  }

  // Reserve a pending flush-completion notification for every CF that the
  // NotifyOnFlushCompleted loop below will service, while the DB mutex is still
  // held continuously since InstallMemtableAtomicFlushResults removed the
  // memtables. This closes the window where a
  // Flush(wait=true, listener_wait=true) waiter could observe an atomic flush
  // as finished before a later CF's OnFlushCompleted has run, since the notify
  // loop releases and re-acquires the mutex between CFs. The reservations are
  // released by a matching pass after the notify loop.
  autovector<uint8_t> reserved_flush_notification;
  reserved_flush_notification.reserve(num_cfs);
  {
    const bool has_listeners = !immutable_db_options_.listeners.empty();
    for (int i = 0; i != num_cfs; ++i) {
      const bool reserve = s.ok() && has_listeners &&
                           !switched_to_mempurge[i] && !cfds[i]->IsDropped() &&
                           !jobs[i]->GetCommittedFlushJobsInfo()->empty();
      if (reserve) {
        cfds[i]->imm()->AddPendingFlushNotifications(1);
      }
      reserved_flush_notification.push_back(reserve ? 1 : 0);
    }
  }
  // Release the reservations on every exit path (RAII) so a future early return
  // cannot leak the count and hang listener_wait waiters. Done independently of
  // the notify loop's skips so the count stays balanced even if a CF was
  // dropped after it was reserved. The DB mutex is held throughout this
  // function, including at scope exit, which SubPendingFlushNotifications()
  // requires.
  Defer release_flush_notifications([&] {
    for (int i = 0; i != num_cfs; ++i) {
      if (reserved_flush_notification[i]) {
        cfds[i]->imm()->SubPendingFlushNotifications(1);
      }
    }
  });

  if (!s.ok()) {
    // If the atomic flush's combined MANIFEST write failed, the output files
    // are cached in the table cache (added by BuildTable during FlushJob::Run)
    // but never installed into any Version. Evict them now so that
    // TEST_VerifyNoObsoleteFilesCached does not fire during Close() when the
    // FindObsoleteFiles full-scan backstop is disabled by
    // metadata_read_fault_one_in.
    for (int i = 0; i != num_cfs; ++i) {
      if (exec_status[i].first && exec_status[i].second.ok() &&
          !switched_to_mempurge[i] && file_meta[i].fd.GetFileSize() > 0) {
        TableCache::ReleaseObsolete(
            cfds[i]->table_cache()->get_cache().get(),
            file_meta[i].fd.GetNumber(), nullptr /*handle*/,
            all_mutable_cf_options[i].uncache_aggressiveness);
      }
    }
  }

  for (int i = 0; i != num_cfs; ++i) {
    auto* mgr = cfds[i]->blob_partition_manager();
    if (mgr == nullptr || prepared_blob_memtable_ids[i].empty()) {
      continue;
    }

    auto unconsumed_additions = jobs[i]->TakeExternalBlobFileAdditions();
    auto unconsumed_garbages = jobs[i]->TakeExternalBlobFileGarbages();
    if (!s.ok() || !unconsumed_additions.empty() ||
        !unconsumed_garbages.empty()) {
      // Same retry semantics as the single-CF flush path above: do not
      // consume these prepared generations yet, because the rolled-back
      // memtables still need to reuse the same sealed blob files on retry.
      prepared_blob_memtable_ids[i].clear();
      continue;
    }

    mgr->CommitFlushedGenerations(cfds[i]->imm()->GetEarliestMemTableID());
  }

  if (s.ok()) {
    assert(num_cfs ==
           static_cast<int>(job_context->superversion_contexts.size()));
    for (int i = 0; i != num_cfs; ++i) {
      assert(cfds[i]);

      if (cfds[i]->IsDropped()) {
        continue;
      }
      InstallSuperVersionAndScheduleWork(cfds[i],
                                         &job_context->superversion_contexts[i],
                                         /*defer_snapshot_refresh=*/false);

      const std::string& column_family_name = cfds[i]->GetName();

      Version* const current = cfds[i]->current();
      assert(current);

      const VersionStorageInfo* const storage_info = current->storage_info();
      assert(storage_info);

      VersionStorageInfo::LevelSummaryStorage tmp{};
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Level summary: %s\n",
                       column_family_name.c_str(),
                       storage_info->LevelSummary(&tmp));

      const auto& blob_files = storage_info->GetBlobFiles();
      if (!blob_files.empty()) {
        assert(blob_files.front());
        assert(blob_files.back());

        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Blob file summary: head=%" PRIu64 ", tail=%" PRIu64 "\n",
            column_family_name.c_str(), blob_files.front()->GetBlobFileNumber(),
            blob_files.back()->GetBlobFileNumber());
      }
    }
    if (made_progress) {
      *made_progress = true;
    }
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    assert(all_mutable_cf_options.size() == static_cast<size_t>(num_cfs));
    for (int i = 0; s.ok() && i != num_cfs; ++i) {
      // If mempurge happened instead of Flush,
      // no NotifyOnFlushCompleted call (no SST file created).
      if (switched_to_mempurge[i]) {
        continue;
      }
      if (cfds[i]->IsDropped()) {
        continue;
      }
      NotifyOnFlushCompleted(cfds[i], all_mutable_cf_options[i],
                             jobs[i]->GetCommittedFlushJobsInfo());
      if (sfm) {
        std::string file_path = MakeTableFileName(
            cfds[i]->ioptions().cf_paths[0].path, file_meta[i].fd.GetNumber());
        // TODO (PR7798).  We should only add the file to the FileManager if it
        // exists. Otherwise, some tests may fail.  Ignore the error in the
        // interim.
        sfm->OnAddFile(file_path).PermitUncheckedError();
        if (sfm->IsMaxAllowedSpaceReached() &&
            error_handler_.GetBGError().ok()) {
          Status new_bg_error =
              Status::SpaceLimit("Max allowed space was reached");
          error_handler_.SetBGError(new_bg_error,
                                    BackgroundErrorReason::kFlush);
        }
      }
    }
  }

  // Need to undo atomic flush if something went wrong, i.e. s is not OK and
  // it is not because of CF drop.
  if (!s.ok() && !s.IsColumnFamilyDropped() && !skip_set_bg_error) {
    if (log_io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      if (!versions_->io_status().ok()) {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the Manifest write will be map to soft error.
        // TODO: kManifestWriteNoWAL and kFlushNoWAL are misleading. Refactor
        // is needed.
        error_handler_.SetBGError(s,
                                  BackgroundErrorReason::kManifestWriteNoWAL);
      } else {
        // If WAL sync is successful (either WAL size is 0 or there is no IO
        // error), all the other SST file write errors will be set as
        // kFlushNoWAL.
        error_handler_.SetBGError(s, BackgroundErrorReason::kFlushNoWAL);
      }
    } else {
      assert(s == log_io_s);
      error_handler_.SetBGError(s, BackgroundErrorReason::kFlush);
    }
  }

  return s;
}

void DBImpl::NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                                const MutableCFOptions& mutable_cf_options,
                                int job_id, FlushReason flush_reason) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  // The stall decision counts L0 sorted runs, where an exact epoch run counts
  // once (see VersionStorageInfo::NumL0SortedRuns()), so these flags do too.
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumL0SortedRuns() >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumL0SortedRuns() >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    FlushJobInfo info{};
    info.cf_id = cfd->GetID();
    info.cf_name = cfd->GetName();
    // TODO(yhchiang): make db_paths dynamic in case flush does not
    //                 go to L0 in the future.
    const uint64_t file_number = file_meta->fd.GetNumber();
    info.file_path =
        MakeTableFileName(cfd->ioptions().cf_paths[0].path, file_number);
    info.file_number = file_number;
    info.thread_id = env_->GetThreadID();
    info.job_id = job_id;
    info.triggered_writes_slowdown = triggered_writes_slowdown;
    info.triggered_writes_stop = triggered_writes_stop;
    info.smallest_seqno = file_meta->fd.smallest_seqno;
    info.largest_seqno = file_meta->fd.largest_seqno;
    info.flush_reason = flush_reason;
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnFlushBegin(this, info);
    }
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

void DBImpl::NotifyOnFlushCompleted(
    ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    std::list<std::unique_ptr<FlushJobInfo>>* flush_jobs_info) {
  assert(flush_jobs_info != nullptr);
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  // The stall decision counts L0 sorted runs, where an exact epoch run counts
  // once (see VersionStorageInfo::NumL0SortedRuns()), so these flags do too.
  bool triggered_writes_slowdown =
      (cfd->current()->storage_info()->NumL0SortedRuns() >=
       mutable_cf_options.level0_slowdown_writes_trigger);
  bool triggered_writes_stop =
      (cfd->current()->storage_info()->NumL0SortedRuns() >=
       mutable_cf_options.level0_stop_writes_trigger);
  // release lock while notifying events
  mutex_.Unlock();
  {
    for (auto& info : *flush_jobs_info) {
      info->triggered_writes_slowdown = triggered_writes_slowdown;
      info->triggered_writes_stop = triggered_writes_stop;
      for (const auto& listener : immutable_db_options_.listeners) {
        listener->OnFlushCompleted(this, *info);
      }
      TEST_SYNC_POINT(
          "DBImpl::NotifyOnFlushCompleted::PostAllOnFlushCompleted");
    }
    flush_jobs_info->clear();
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

Status DBImpl::CompactRange(const CompactRangeOptions& options,
                            ColumnFamilyHandle* column_family,
                            const Slice* begin_without_ts,
                            const Slice* end_without_ts) {
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  assert(cfd);

  if (!cfd->is_table_builder_supported()) {
    return TableBuilderNotSupported(*cfd);
  }

  if (manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  if (IsCompactionAborted(cfd)) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }

  if (options.canceled && options.canceled->load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz == 0) {
    return CompactRangeInternal(options, column_family, begin_without_ts,
                                end_without_ts, "" /*trim_ts*/);
  }

  std::string begin_str;
  std::string end_str;
  auto [begin, end] =
      MaybeAddTimestampsToRange(OptSlice::CopyFromPtr(begin_without_ts),
                                OptSlice::CopyFromPtr(end_without_ts), ts_sz,
                                &begin_str, &end_str, false /*exclusive_end*/);

  return CompactRangeInternal(
      options, column_family, begin.has_value() ? &begin.value() : nullptr,
      end.has_value() ? &end.value() : nullptr, "" /*trim_ts*/);
}

Status DBImpl::IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                        std::string ts_low) {
  ColumnFamilyData* cfd = nullptr;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
    assert(cfh != nullptr);
    cfd = cfh->cfd();
  }
  assert(cfd != nullptr && cfd->user_comparator() != nullptr);
  if (cfd->user_comparator()->timestamp_size() == 0) {
    return Status::InvalidArgument(
        "Timestamp is not enabled in this column family");
  }
  if (cfd->user_comparator()->timestamp_size() != ts_low.size()) {
    return Status::InvalidArgument("ts_low size mismatch");
  }
  return IncreaseFullHistoryTsLowImpl(cfd, ts_low);
}

Status DBImpl::IncreaseFullHistoryTsLowImpl(ColumnFamilyData* cfd,
                                            const std::string& ts_low) {
  VersionEdit edit;
  edit.SetColumnFamily(cfd->GetID());
  edit.SetFullHistoryTsLow(ts_low);

  // TODO: plumb Env::IOActivity, Env::IOPriority
  const ReadOptions read_options;
  const WriteOptions write_options;

  TEST_SYNC_POINT_CALLBACK("DBImpl::IncreaseFullHistoryTsLowImpl:BeforeEdit",
                           &edit);

  InstrumentedMutexLock l(&mutex_);
  std::string current_ts_low = cfd->GetFullHistoryTsLow();
  const Comparator* ucmp = cfd->user_comparator();
  assert(ucmp->timestamp_size() == ts_low.size() && !ts_low.empty());
  if (!current_ts_low.empty() &&
      ucmp->CompareTimestamp(ts_low, current_ts_low) < 0) {
    std::stringstream oss;
    oss << "Current full_history_ts_low: "
        << ucmp->TimestampToString(current_ts_low)
        << " is higher than provided ts: " << ucmp->TimestampToString(ts_low)
        << std::endl;
    return Status::InvalidArgument(oss.str());
  }

  Status s = versions_->LogAndApply(cfd, read_options, write_options, &edit,
                                    &mutex_, directories_.GetDbDir());
  if (!s.ok()) {
    return s;
  }
  current_ts_low = cfd->GetFullHistoryTsLow();
  if (!current_ts_low.empty() &&
      ucmp->CompareTimestamp(current_ts_low, ts_low) > 0) {
    std::stringstream oss;
    oss << "full_history_ts_low: " << Slice(current_ts_low).ToString(true)
        << " is set to be higher than the requested "
           "timestamp: "
        << Slice(ts_low).ToString(true) << std::endl;
    return Status::TryAgain(oss.str());
  }
  return Status::OK();
}

Status DBImpl::CompactRangeInternal(const CompactRangeOptions& options,
                                    ColumnFamilyHandle* column_family,
                                    const Slice* begin, const Slice* end,
                                    const std::string& trim_ts) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  auto cfd = cfh->cfd();

  if (options.target_path_id >= cfd->ioptions().cf_paths.size()) {
    return Status::InvalidArgument("Invalid target path ID");
  }
  if (options.change_level &&
      cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    return Status::NotSupported(
        "FIFO compaction does not support change_level.");
  }
  if (options.skip_missing_blob_files &&
      cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    return Status::NotSupported(
        "FIFO compaction does not support missing blob file repair.");
  }

  bool flush_needed = true;

  // Update full_history_ts_low if it's set
  if (options.full_history_ts_low != nullptr &&
      !options.full_history_ts_low->empty()) {
    std::string ts_low = options.full_history_ts_low->ToString();
    if (begin != nullptr || end != nullptr) {
      return Status::InvalidArgument(
          "Cannot specify compaction range with full_history_ts_low");
    }
    Status s = IncreaseFullHistoryTsLowImpl(cfd, ts_low);
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  Status s;
  if (begin != nullptr && end != nullptr) {
    // TODO(ajkr): We could also optimize away the flush in certain cases where
    // one/both sides of the interval are unbounded. But it requires more
    // changes to RangesOverlapWithMemtables.
    UserKeyRange range(*begin, *end);
    SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
    s = cfd->RangesOverlapWithMemtables(
        {range}, super_version, immutable_db_options_.allow_data_in_errors,
        &flush_needed, /*range_limit_exclusive=*/false);
    CleanupSuperVersion(super_version);
  }

  if (s.ok() && flush_needed) {
    FlushOptions fo;
    fo.allow_write_stall = options.allow_write_stall;
    if (immutable_db_options_.atomic_flush) {
      s = AtomicFlushMemTables(fo, FlushReason::kManualCompaction);
      if (s.ok() && cfd->IsDropped()) {
        s = Status::ColumnFamilyDropped();
      }
    } else {
      s = FlushMemTable(cfd, fo, FlushReason::kManualCompaction);
    }
    if (!s.ok()) {
      LogFlush(immutable_db_options_.info_log);
      return s;
    }
  }

  constexpr int kInvalidLevel = -1;
  int final_output_level = kInvalidLevel;
  bool exclusive = options.exclusive_manual_compaction;
  if (cfd->ioptions().compaction_style == kCompactionStyleUniversal &&
      cfd->NumberLevels() > 1) {
    // Always compact all files together.
    final_output_level = cfd->NumberLevels() - 1;
    if (cfd->AllowIngestBehind()) {
      final_output_level--;
    }
    s = RunManualCompaction(cfd, ColumnFamilyData::kCompactAllLevels,
                            final_output_level, options, begin, end, exclusive,
                            false /* disable_trivial_move */,
                            std::numeric_limits<uint64_t>::max(), trim_ts);
  } else if (cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    // FIFOCompactionPicker::CompactRange() will ignore the input key range
    // [begin, end] and just try to pick compaction based on the configured
    // option `compaction_options_fifo`. So we skip checking if [begin, end]
    // overlaps with the DB here.
    final_output_level = 0;
    s = RunManualCompaction(cfd, /*input_level=*/0, final_output_level, options,
                            begin, end, exclusive,
                            false /* disable_trivial_move */,
                            std::numeric_limits<uint64_t>::max(), trim_ts);
  } else {
    int first_overlapped_level = kInvalidLevel;
    {
      SuperVersion* super_version = cfd->GetReferencedSuperVersion(this);
      Version* current_version = super_version->current;

      // Might need to query the partitioner
      SstPartitionerFactory* partitioner_factory =
          current_version->cfd()->ioptions().sst_partitioner_factory.get();
      std::unique_ptr<SstPartitioner> partitioner;
      if (partitioner_factory && begin != nullptr && end != nullptr) {
        SstPartitioner::Context context;
        context.is_full_compaction = false;
        context.is_manual_compaction = true;
        context.output_level = /*unknown*/ -1;
        // Small lies about compaction range
        context.smallest_user_key = *begin;
        context.largest_user_key = *end;
        partitioner = partitioner_factory->CreatePartitioner(context);
      }

      ReadOptions ro;
      ro.total_order_seek = true;
      ro.io_activity = Env::IOActivity::kCompaction;
      bool overlap;
      for (int level = 0;
           level < current_version->storage_info()->num_non_empty_levels();
           level++) {
        overlap = true;

        // Whether to look at specific keys within files for overlap with
        // compaction range, other than largest and smallest keys of the file
        // known in Version metadata.
        bool check_overlap_within_file = false;
        if (begin != nullptr && end != nullptr) {
          // Typically checking overlap within files in this case
          check_overlap_within_file = true;
          // WART: Not known why we don't check within file in one-sided bound
          // cases
          if (partitioner) {
            // Especially if the partitioner is new, the manual compaction
            // might be used to enforce the partitioning. Checking overlap
            // within files might miss cases where compaction is needed to
            // partition the files, as in this example:
            // * File has two keys "001" and "111"
            // * Compaction range is ["011", "101")
            // * Partition boundary at "100"
            // In cases like this, file-level overlap with the compaction
            // range is sufficient to force any partitioning that is needed
            // within the compaction range.
            //
            // But if there's no partitioning boundary within the compaction
            // range, we can be sure there's no need to fix partitioning
            // within that range, thus safe to check overlap within file.
            //
            // Use a hypothetical trivial move query to check for partition
            // boundary in range. (NOTE: in defiance of all conventions,
            // `begin` and `end` here are both INCLUSIVE bounds, which makes
            // this analogy to CanDoTrivialMove() accurate even when `end` is
            // the first key in a partition.)
            if (!partitioner->CanDoTrivialMove(*begin, *end)) {
              check_overlap_within_file = false;
            }
          }
        }
        if (check_overlap_within_file) {
          Status status = current_version->OverlapWithLevelIterator(
              ro, file_options_, *begin, *end, level, &overlap);
          if (!status.ok()) {
            check_overlap_within_file = false;
          }
        }
        if (!check_overlap_within_file) {
          overlap = current_version->storage_info()->OverlapInLevel(level,
                                                                    begin, end);
        }
        if (overlap) {
          first_overlapped_level = level;
          break;
        }
      }
      CleanupSuperVersion(super_version);
    }
    if (s.ok() && first_overlapped_level != kInvalidLevel) {
      if (cfd->ioptions().compaction_style == kCompactionStyleUniversal) {
        assert(first_overlapped_level == 0);
        s = RunManualCompaction(
            cfd, first_overlapped_level, first_overlapped_level, options, begin,
            end, exclusive, true /* disallow_trivial_move */,
            std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
            trim_ts);
        final_output_level = first_overlapped_level;
      } else {
        assert(cfd->ioptions().compaction_style == kCompactionStyleLevel);
        uint64_t next_file_number = versions_->current_next_file_number();
        // Start compaction from `first_overlapped_level`, one level down at a
        // time, until output level >= max_overlapped_level.
        // When max_overlapped_level == 0, we will still compact from L0 -> L1
        // (or LBase), and followed by a bottommost level intra-level compaction
        // at L1 (or LBase), if applicable.
        int level = first_overlapped_level;
        final_output_level = level;
        int output_level = 0;
        int base_level = 0;
        for (;;) {
          // Always allow L0 -> L1 compaction
          if (level > 0) {
            if (cfd->ioptions().level_compaction_dynamic_level_bytes) {
              assert(final_output_level < cfd->ioptions().num_levels);
              if (final_output_level + 1 == cfd->ioptions().num_levels) {
                break;
              }
            } else {
              // TODO(cbi): there is still a race condition here where
              //  if a background compaction compacts some file beyond
              //  current()->storage_info()->num_non_empty_levels() right after
              //  the check here.This should happen very infrequently and should
              //  not happen once a user populates the last level of the LSM.
              InstrumentedMutexLock l(&mutex_);
              // num_non_empty_levels may be lower after a compaction, so
              // we check for >= here.
              if (final_output_level + 1 >=
                  cfd->current()->storage_info()->num_non_empty_levels()) {
                break;
              }
            }
          }
          output_level = level + 1;
          if (cfd->ioptions().level_compaction_dynamic_level_bytes &&
              level == 0) {
            output_level = ColumnFamilyData::kCompactToBaseLevel;
          }
          // Use max value for `max_file_num_to_ignore` to always compact
          // files down.
          s = RunManualCompaction(
              cfd, level, output_level, options, begin, end, exclusive,
              !trim_ts.empty() /* disallow_trivial_move */,
              std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
              trim_ts,
              output_level == ColumnFamilyData::kCompactToBaseLevel
                  ? &base_level
                  : nullptr);
          if (!s.ok()) {
            break;
          }
          if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
            assert(base_level > 0);
            level = base_level;
          } else {
            ++level;
          }
          final_output_level = level;
          TEST_SYNC_POINT("DBImpl::RunManualCompaction()::1");
          TEST_SYNC_POINT("DBImpl::RunManualCompaction()::2");
        }
        if (s.ok()) {
          assert(final_output_level > 0);
          // bottommost level intra-level compaction
          if (options.skip_missing_blob_files ||
              (options.bottommost_level_compaction ==
                   BottommostLevelCompaction::kIfHaveCompactionFilter &&
               (cfd->ioptions().compaction_filter != nullptr ||
                cfd->ioptions().compaction_filter_factory != nullptr)) ||
              options.bottommost_level_compaction ==
                  BottommostLevelCompaction::kForceOptimized ||
              options.bottommost_level_compaction ==
                  BottommostLevelCompaction::kForce) {
            // Use `next_file_number` as `max_file_num_to_ignore` to avoid
            // rewriting newly compacted files during repair, kForceOptimized,
            // or kIfHaveCompactionFilter with compaction filter set.
            s = RunManualCompaction(
                cfd, final_output_level, final_output_level, options, begin,
                end, exclusive, true /* disallow_trivial_move */,
                next_file_number /* max_file_num_to_ignore */, trim_ts);
          }
        }
      }
    }
  }
  if (!s.ok() || final_output_level == kInvalidLevel) {
    LogFlush(immutable_db_options_.info_log);
    return s;
  }

  if (options.change_level) {
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:1");
    TEST_SYNC_POINT("DBImpl::CompactRange:BeforeRefit:2");

    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[RefitLevel] waiting for background threads to stop");
    // TODO(hx235): remove `Enable/DisableManualCompaction` and
    // `Continue/PauseBackgroundWork` once we ensure registering RefitLevel()'s
    // range is sufficient (if not, what else is needed) for avoiding range
    // conflicts with other activities (e.g, compaction, flush) that are
    // currently avoided by `Enable/DisableManualCompaction` and
    // `Continue/PauseBackgroundWork`.
    DisableManualCompaction();
    s = PauseBackgroundWork();
    if (s.ok()) {
      TEST_SYNC_POINT("DBImpl::CompactRange:PreRefitLevel");
      s = ReFitLevel(cfd, final_output_level, options.target_level);
      TEST_SYNC_POINT("DBImpl::CompactRange:PostRefitLevel");
      // ContinueBackgroundWork always return Status::OK().
      Status temp_s = ContinueBackgroundWork();
      assert(temp_s.ok());
    }
    EnableManualCompaction();
    TEST_SYNC_POINT(
        "DBImpl::CompactRange:PostRefitLevel:ManualCompactionEnabled");
  }
  LogFlush(immutable_db_options_.info_log);

  {
    InstrumentedMutexLock l(&mutex_);
    // an automatic compaction that has been scheduled might have been
    // preempted by the manual compactions. Need to schedule it back.
    MaybeScheduleFlushOrCompaction();
  }

  return s;
}

Status DBImpl::CompactFiles(const CompactionOptions& compact_options,
                            ColumnFamilyHandle* column_family,
                            const std::vector<std::string>& input_file_names,
                            const int output_level, const int output_path_id,
                            std::vector<std::string>* const output_file_names,
                            CompactionJobInfo* compaction_job_info) {
  if (column_family == nullptr) {
    return Status::InvalidArgument("ColumnFamilyHandle must be non-null.");
  }

  auto cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(column_family)->cfd();
  assert(cfd);

  if (!cfd->is_table_builder_supported()) {
    return TableBuilderNotSupported(*cfd);
  }

  {
    InstrumentedMutexLock l(&mutex_);
    BeginInFlightCompaction(cfd);
  }
  Defer end_in_flight_compaction([&]() {
    InstrumentedMutexLock l(&mutex_);
    // The caller holds a live handle to this CF, so this can never be the final
    // unref (the return can only be true when the last reference is dropped).
    [[maybe_unused]] const bool cfd_deleted = EndInFlightCompaction(cfd);
    assert(!cfd_deleted);
    if (per_cf_compaction_abort_waiters_ > 0) {
      bg_cv_.SignalAll();
    }
  });

  Status s;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  if (compact_options.compression !=
      CompressionType::kDisableCompressionOption) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Found use of deprecated option "
                   "`CompactionOptions::compression`",
                   cfd->GetName().c_str(), job_context.job_id);
  }

  // Perform CompactFiles
  TEST_SYNC_POINT("TestCompactFiles::IngestExternalFile2");
  TEST_SYNC_POINT_CALLBACK("TestCompactFiles:PausingManualCompaction:3",
                           static_cast<void*>(const_cast<std::atomic<int>*>(
                               &manual_compaction_paused_)));
  TEST_SYNC_POINT_CALLBACK("TestCancelCompactFiles:SuccessfulCompaction",
                           static_cast<void*>(const_cast<std::atomic<int>*>(
                               &manual_compaction_paused_)));
  {
    InstrumentedMutexLock l(&mutex_);
    auto* current = cfd->current();
    current->Ref();

    s = CompactFilesImpl(compact_options, cfd, current, input_file_names,
                         output_file_names, output_level, output_path_id,
                         &job_context, &log_buffer, compaction_job_info);

    current->Unref();
  }

  // Find and delete obsolete files
  {
    InstrumentedMutexLock l(&mutex_);
    // If !s.ok(), this means that Compaction failed. In that case, we want
    // to delete all obsolete files we might have created and we force
    // FindObsoleteFiles(). This is because job_context does not
    // catch all created files if compaction failed.
    FindObsoleteFiles(&job_context, !s.ok());
  }  // release the mutex

  // delete unnecessary files if any, this is done outside the mutex
  if (job_context.HaveSomethingToClean() ||
      job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
    // Have to flush the info logs before bg_compaction_scheduled_--
    // because if bg_flush_scheduled_ becomes 0 and the lock is
    // released, the deconstructor of DB can kick in and destroy all the
    // states of DB so info_log might not be available after that point.
    // It also applies to access other states that DB owns.
    log_buffer.FlushBufferToLog();
    if (job_context.HaveSomethingToDelete()) {
      // no mutex is locked here.  No need to Unlock() and Lock() here.
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
  }

  return s;
}

void DBImpl::PrepareTrivialMoveEdit(Compaction& c, LogBuffer* log_buffer,
                                    size_t& moved_files, size_t& moved_bytes) {
  mutex_.AssertHeld();

  ROCKS_LOG_BUFFER(log_buffer, "[%s] Moving %d files to level-%d\n",
                   c.column_family_data()->GetName().c_str(),
                   static_cast<int>(c.num_input_files(0)), c.output_level());

  // Move files to the output level by editing the manifest
  for (unsigned int l = 0; l < c.num_input_levels(); l++) {
    if (c.level(l) == c.output_level()) {
      continue;
    }
    for (size_t i = 0; i < c.num_input_files(l); i++) {
      FileMetaData* f = c.input(l, i);
      c.edit()->DeleteFile(c.level(l), f->fd.GetNumber());
      c.edit()->AddExistingFile(c.output_level(), *f,
                                kProvenanceMovedBetweenLevels);
      moved_bytes += static_cast<size_t>(c.input(l, i)->fd.GetFileSize());
      ROCKS_LOG_BUFFER(
          log_buffer, "[%s] Moved #%" PRIu64 " to level-%d %" PRIu64 " bytes\n",
          c.column_family_data()->GetName().c_str(), f->fd.GetNumber(),
          c.output_level(), f->fd.GetFileSize());
    }
    moved_files += c.num_input_files(l);
  }
}

Status DBImpl::CommitTrivialMove(Compaction& c, bool& compaction_released) {
  mutex_.AssertHeld();

  // Install the new version
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);
  Status status = versions_->LogAndApply(
      c.column_family_data(), read_options, write_options, c.edit(), &mutex_,
      directories_.GetDbDir(), /*new_descriptor_log=*/false,
      /*column_family_options=*/nullptr,
      [&c, &compaction_released](const Status& s) {
        c.ReleaseCompactionFiles(s);
        compaction_released = true;
      });

  return status;
}

Status DBImpl::CompactFilesImpl(
    const CompactionOptions& compact_options, ColumnFamilyData* cfd,
    Version* version, const std::vector<std::string>& input_file_names,
    std::vector<std::string>* const output_file_names, const int output_level,
    int output_path_id, JobContext* job_context, LogBuffer* log_buffer,
    CompactionJobInfo* compaction_job_info) {
  mutex_.AssertHeld();

  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }

  if (IsCompactionAborted(cfd)) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }

  // triggered by DisableManualCompactions or by user-set canceled flag in
  // CompactionOptions
  if (manual_compaction_paused_.load(std::memory_order_acquire) > 0 ||
      (compact_options.canceled &&
       compact_options.canceled->load(std::memory_order_acquire))) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }

  std::unordered_set<uint64_t> input_set;
  for (const auto& file_name : input_file_names) {
    input_set.insert(TableFileNameToNumber(file_name));
  }

  if (output_path_id < 0) {
    if (cfd->ioptions().cf_paths.size() == 1U) {
      output_path_id = 0;
    } else {
      return Status::NotSupported(
          "Automatic output path selection is not "
          "yet supported in CompactFiles()");
    }
  }

  if (cfd->AllowIngestBehind() &&
      output_level >= cfd->ioptions().num_levels - 1) {
    return Status::InvalidArgument(
        "Exceed the maximum output level defined by "
        "the current compaction algorithm with ingest_behind --- " +
        std::to_string(cfd->ioptions().num_levels - 1));
  }

  std::vector<CompactionInputFiles> input_files;
  Status s = cfd->compaction_picker()->SanitizeAndConvertCompactionInputFiles(
      &input_set, output_level, version, &input_files);
  TEST_SYNC_POINT(
      "DBImpl::CompactFilesImpl::PostSanitizeAndConvertCompactionInputFiles");
  if (!s.ok()) {
    return s;
  }

  for (const auto& inputs : input_files) {
    if (cfd->compaction_picker()->AreFilesInCompaction(inputs.files)) {
      return Status::Aborted(
          "Some of the necessary compaction input "
          "files are already being compacted");
    }
  }
  bool sfm_reserved_compact_space = false;
  // First check if we have enough room to do the compaction
  bool enough_room = EnoughRoomForCompaction(
      cfd, input_files, &sfm_reserved_compact_space, log_buffer);

  if (!enough_room) {
    // m's vars will get set properly at the end of this function,
    // as long as status == CompactionTooLarge
    return Status::CompactionTooLarge();
  }

  // At this point, CompactFiles will be run.
  const bool separate_remote_pool = UseSeparateRemoteCompactionPool();
  int& scheduled_compactions = separate_remote_pool
                                   ? remote_coordinators_scheduled_
                                   : bg_compaction_scheduled_;
  ++scheduled_compactions;

  std::unique_ptr<Compaction> c;
  assert(cfd->compaction_picker());
  c.reset(cfd->compaction_picker()->PickCompactionForCompactFiles(
      compact_options, input_files, output_level, version->storage_info(),
      cfd->GetLatestMutableCFOptions(), mutable_db_options_, output_path_id));
  // we already sanitized the set of input files and checked for conflicts
  // without releasing the lock, so we're guaranteed a compaction can be formed.
  assert(c != nullptr);

  c->FinalizeInputInfo(version);

  // deletion compaction currently not allowed in CompactFiles.
  assert(!c->deletion_compaction());

  // Check if this can be a trivial move (metadata-only update)
  // Similar to the logic in DBImpl::BackgroundCompaction
  // Note: We disable trivial move when compaction_service is present because
  // the service expects all compactions to go through CompactionJob for
  // tracking
  bool is_trivial_move = compact_options.allow_trivial_move &&
                         c->IsTrivialMove() &&
                         immutable_db_options().compaction_service == nullptr;

  if (is_trivial_move) {
    // Perform trivial move: just update manifest without rewriting data
    TEST_SYNC_POINT("DBImpl::CompactFilesImpl:TrivialMove");

    bool compaction_released = false;
    size_t moved_files = 0;
    size_t moved_bytes = 0;
    PrepareTrivialMoveEdit(*c.get(), log_buffer, moved_files, moved_bytes);
    Status status = CommitTrivialMove(*c.get(), compaction_released);

    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          c->column_family_data(), job_context->superversion_contexts.data(),
          /*defer_snapshot_refresh=*/false);

      // Populate output file names for trivial move
      if (output_file_names != nullptr) {
        for (const auto& newf : c->edit()->GetNewFiles()) {
          output_file_names->push_back(TableFileName(
              c->immutable_options().cf_paths, newf.second.fd.GetNumber(),
              newf.second.fd.GetPathId()));
        }
      }

      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Trivial move succeeded for %zu files, %zu bytes total\n",
          c->column_family_data()->GetName().c_str(), moved_files, moved_bytes);
    } else {
      if (!compaction_released) {
        c->ReleaseCompactionFiles(status);
      }
      RescoreAndEnqueuePendingCompaction(c->column_family_data());
      ROCKS_LOG_BUFFER(log_buffer, "[%s] Trivial move failed: %s\n",
                       c->column_family_data()->GetName().c_str(),
                       status.ToString().c_str());
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }

    c.reset();
    --scheduled_compactions;
    if (separate_remote_pool || bg_compaction_scheduled_ == 0 ||
        per_cf_compaction_abort_waiters_ > 0) {
      bg_cv_.SignalAll();
    }
    MaybeScheduleFlushOrCompaction();

    return status;
  }

  // Not a trivial move, proceed with full compaction
  InitSnapshotContext(job_context);

  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem(
      new std::list<uint64_t>::iterator(
          CaptureCurrentFileNumberInPendingOutputs()));

  assert(is_snapshot_supported_ || snapshots_.empty());
  CompactionJobStats compaction_job_stats;
  CompactionJob compaction_job(
      job_context->job_id, c.get(), immutable_db_options_, mutable_db_options_,
      file_options_for_compaction_, versions_.get(), &shutting_down_,
      log_buffer, directories_.GetDbDir(),
      GetDataDir(c->column_family_data(), c->output_path_id()),
      GetDataDir(c->column_family_data(), 0), stats_, &mutex_, &error_handler_,
      job_context, table_cache_, &event_logger_,
      c->mutable_cf_options().paranoid_file_checks,
      c->mutable_cf_options().report_bg_io_stats, dbname_,
      &compaction_job_stats, Env::Priority::USER, io_tracer_,
      kManualCompactionCanceledFalse_, compaction_aborted_, db_id_,
      db_session_id_, c->column_family_data()->GetFullHistoryTsLow(),
      c->trim_ts(), &blob_callback_, &bg_compaction_scheduled_,
      &bg_bottom_compaction_scheduled_, &num_running_remote_compactions_);
  ConfigureCompactionExecutors(&compaction_job, c->column_family_data(),
                               kManualCompactionCanceledFalse_, false,
                               separate_remote_pool);
  c->SetSeparateRemoteCompactionPoolSelected(separate_remote_pool);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here.
  version->storage_info()->ComputeCompactionScore(
      cfd->ioptions(), c->mutable_cf_options(), cfd->GetFullHistoryTsLow());

  compaction_job.Prepare(std::nullopt /*subcompact to be computed*/);

  std::unique_ptr<std::list<uint64_t>::iterator> min_options_file_number_elem;
  if (immutable_db_options().compaction_service != nullptr) {
    min_options_file_number_elem.reset(
        new std::list<uint64_t>::iterator(CaptureOptionsFileNumber()));
  }

  mutex_.Unlock();
  TEST_SYNC_POINT("CompactFilesImpl:0");
  TEST_SYNC_POINT("CompactFilesImpl:1");
  // Ignore the status here, as it will be checked in the Install down below...
  compaction_job.Run().PermitUncheckedError();
  TEST_SYNC_POINT("CompactFilesImpl:2");
  TEST_SYNC_POINT("CompactFilesImpl:3");
  mutex_.Lock();

  if (immutable_db_options().compaction_service != nullptr) {
    ReleaseOptionsFileNumber(min_options_file_number_elem);
  }

  bool compaction_released = false;
  Status status = compaction_job.Install(&compaction_released);
  if (!compaction_released) {
    c->ReleaseCompactionFiles(s);
  }
  if (status.ok()) {
    assert(compaction_job.io_status().ok());
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);
  } else {
    RescoreAndEnqueuePendingCompaction(c->column_family_data());
  }
  // status above captures any error during compaction_job.Install, so its ok
  // not check compaction_job.io_status() explicitly if we're not calling
  // SetBGError
  compaction_job.io_status().PermitUncheckedError();
  // Need to make sure SstFileManager does its bookkeeping
  auto sfm = static_cast<SstFileManagerImpl*>(
      immutable_db_options_.sst_file_manager.get());
  if (sfm && sfm_reserved_compact_space) {
    sfm->OnCompactionCompletion(c.get());
  }

  ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

  mutex_.Unlock();
  if (compaction_job_info != nullptr) {
    BuildCompactionJobInfo(cfd, c.get(), s, compaction_job_stats,
                           job_context->job_id, compaction_job_info);
  }
  mutex_.Lock();

  if (status.ok()) {
    // Done
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else if (status.IsManualCompactionPaused()) {
    // Don't report stopping manual compaction as error
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Stopping manual compaction",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id);
  } else if (status.IsCompactionAborted()) {
    // Don't report aborted compaction as error
    ROCKS_LOG_INFO(
        immutable_db_options_.info_log, "[%s] [JOB %d] Compaction aborted",
        c->column_family_data()->GetName().c_str(), job_context->job_id);
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "[%s] [JOB %d] Compaction error: %s",
                   c->column_family_data()->GetName().c_str(),
                   job_context->job_id, status.ToString().c_str());
    IOStatus io_s = compaction_job.io_status();
    if (!io_s.ok()) {
      error_handler_.SetBGError(io_s, BackgroundErrorReason::kCompaction);
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }
  }

  if (output_file_names != nullptr) {
    for (const auto& newf : c->edit()->GetNewFiles()) {
      output_file_names->push_back(TableFileName(
          c->immutable_options().cf_paths, newf.second.fd.GetNumber(),
          newf.second.fd.GetPathId()));
    }

    for (const auto& blob_file : c->edit()->GetBlobFileAdditions()) {
      output_file_names->push_back(
          BlobFileName(c->immutable_options().cf_paths.front().path,
                       blob_file.GetBlobFileNumber()));
    }
  }

  c.reset();

  --scheduled_compactions;
  if (separate_remote_pool || bg_compaction_scheduled_ == 0 ||
      per_cf_compaction_abort_waiters_ > 0) {
    bg_cv_.SignalAll();
  }
  MaybeScheduleFlushOrCompaction();
  TEST_SYNC_POINT("CompactFilesImpl:End");

  return status;
}

Status DBImpl::PauseBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  bg_compaction_paused_++;
  // The blob reference census job is background work too: it reads SSTs,
  // stores GC candidates in the current Version and schedules compactions. So
  // wait for it. ScheduleBlobReferenceCensus() does not start it again while
  // paused.
  while (CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0 ||
         bg_flush_scheduled_ > 0 || bg_wbm_flush_scheduled_ > 0 ||
         bg_blob_reference_census_state_ ==
             BlobReferenceCensusJobState::kScheduled) {
    TEST_SYNC_POINT("DBImpl::PauseBackgroundWork:Wait");
    bg_cv_.Wait();
  }
  bg_work_paused_++;
  return Status::OK();
}

Status DBImpl::ContinueBackgroundWork() {
  InstrumentedMutexLock guard_lock(&mutex_);
  if (bg_work_paused_ == 0) {
    return Status::InvalidArgument("Background work already unpaused");
  }
  assert(bg_work_paused_ > 0);
  assert(bg_compaction_paused_ > 0);
  bg_compaction_paused_--;
  bg_work_paused_--;
  // It's sufficient to check just bg_work_paused_ here since
  // bg_work_paused_ is always no greater than bg_compaction_paused_
  if (bg_work_paused_ == 0) {
    MaybeScheduleFlushOrCompaction();
    // Start the census job again for the column families that wait in its
    // queue (see ScheduleBlobReferenceCensus()).
    ScheduleBlobReferenceCensus();
  }
  return Status::OK();
}

void DBImpl::NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                                     const Status& st,
                                     const CompactionJobStats& job_stats,
                                     int job_id) {
  if (immutable_db_options_.listeners.empty()) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  if (c->is_manual_compaction() &&
      manual_compaction_paused_.load(std::memory_order_acquire) > 0) {
    return;
  }

  c->SetNotifyOnCompactionCompleted();
  int num_l0_files = c->input_version()->storage_info()->NumLevelFiles(0);
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionBegin::UnlockMutex");
  {
    CompactionJobInfo info{};
    info.num_l0_files = num_l0_files;
    BuildCompactionJobInfo(cfd, c, st, job_stats, job_id, &info);
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnCompactionBegin(this, info);
    }
    info.status.PermitUncheckedError();
  }
  mutex_.Lock();
}

void DBImpl::NotifyOnCompactionCompleted(
    ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  if (c->ShouldNotifyOnCompactionCompleted() == false) {
    return;
  }

  int num_l0_files = cfd->current()->storage_info()->NumLevelFiles(0);
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionCompleted::UnlockMutex");
  {
    CompactionJobInfo info{};
    info.num_l0_files = num_l0_files;
    BuildCompactionJobInfo(cfd, c, st, compaction_job_stats, job_id, &info);
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnCompactionCompleted(this, info);
    }
  }
  mutex_.Lock();
  // no need to signal bg_cv_ as it will be signaled at the end of the
  // flush process.
}

void DBImpl::NotifyOnCompactionPreCommit(
    ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  // Only fire if OnCompactionBegin has fired for this compaction.
  if (c->ShouldNotifyOnCompactionCompleted() == false) {
    return;
  }
  // Idempotency: ensure listeners observe at most one
  // OnCompactionPreCommit per compaction lifecycle, even though the
  // notify helper is invoked at multiple potential ReleaseCompactionFiles()
  // sites for safety.
  if (c->WasNotifyOnCompactionPreCommitCalled()) {
    return;
  }
  c->SetNotifyOnCompactionPreCommitCalled();

  int num_l0_files = cfd->current()->storage_info()->NumLevelFiles(0);
  // release lock while notifying events
  mutex_.Unlock();
  TEST_SYNC_POINT("DBImpl::NotifyOnCompactionPreCommit::UnlockMutex");
  {
    CompactionJobInfo info{};
    info.num_l0_files = num_l0_files;
    BuildCompactionJobInfo(cfd, c, st, compaction_job_stats, job_id, &info);
    for (const auto& listener : immutable_db_options_.listeners) {
      listener->OnCompactionPreCommit(this, info);
    }
    info.status.PermitUncheckedError();
  }
  mutex_.Lock();
}

// REQUIREMENT: block all background work by calling PauseBackgroundWork()
// before calling this function
// TODO (hx235): Replace Status::NotSupported() with Status::Aborted() for
// better semantics like CompactFiles()
Status DBImpl::ReFitLevel(ColumnFamilyData* cfd, int level, int target_level) {
  assert(level < cfd->NumberLevels());
  if (target_level >= cfd->NumberLevels()) {
    return Status::InvalidArgument("Target level exceeds number of levels");
  }

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);

  SuperVersionContext sv_context(/* create_superversion */ true);

  InstrumentedMutexLock guard_lock(&mutex_);

  auto* vstorage = cfd->current()->storage_info();
  if (vstorage->LevelFiles(level).empty()) {
    return Status::OK();
  }
  // only allow one thread refitting
  if (refitting_level_) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[ReFitLevel] another thread is refitting");
    return Status::NotSupported("another thread is refitting");
  }
  refitting_level_ = true;

  const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
  // move to a smaller level
  int to_level = target_level;
  if (target_level < 0) {
    to_level = FindMinimumEmptyLevelFitting(cfd, mutable_cf_options, level);
  }

  if (to_level != level) {
    std::vector<CompactionInputFiles> input(1);
    input[0].level = level;
    // TODO (hx235): Only refit the output files in the current manual
    // compaction instead of all the files in the output level
    for (auto& f : vstorage->LevelFiles(level)) {
      input[0].files.push_back(f);
    }
    InternalKey refit_level_smallest;
    InternalKey refit_level_largest;
    cfd->compaction_picker()->GetRange(input[0], &refit_level_smallest,
                                       &refit_level_largest);
    if (to_level > level) {
      if (level == 0) {
        refitting_level_ = false;
        return Status::NotSupported(
            "Cannot change from level 0 to other levels.");
      }
      // Check levels are empty for a trivial move
      for (int l = level + 1; l <= to_level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
        if (cfd->RangeOverlapWithCompaction(refit_level_smallest.user_key(),
                                            refit_level_largest.user_key(), l,
                                            /*range_limit_exclusive=*/false)) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target "
              "will have some ongoing compaction's output.");
        }
      }
    } else {
      // to_level < level
      if (to_level == 0 && input[0].files.size() > 1) {
        refitting_level_ = false;
        return Status::Aborted(
            "Moving more than 1 file from non-L0 to L0 is not allowed as it "
            "does not bring any benefit to read nor write throughput.");
      }
      // Check levels are empty for a trivial move
      for (int l = to_level; l < level; l++) {
        if (vstorage->NumLevelFiles(l) > 0) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target are not empty for a move.");
        }
        if (cfd->RangeOverlapWithCompaction(refit_level_smallest.user_key(),
                                            refit_level_largest.user_key(), l,
                                            /*range_limit_exclusive=*/false)) {
          refitting_level_ = false;
          return Status::NotSupported(
              "Levels between source and target "
              "will have some ongoing compaction's output.");
        }
      }
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Before refitting:\n%s", cfd->GetName().c_str(),
                    cfd->current()->DebugString().data());

    std::unique_ptr<Compaction> c(new Compaction(
        vstorage, cfd->ioptions(), mutable_cf_options, mutable_db_options_,
        {input}, to_level,
        MaxFileSizeForLevel(
            mutable_cf_options, to_level,
            cfd->ioptions()
                .compaction_style) /* output file size limit, not applicable */
        ,
        LLONG_MAX /* max compaction bytes, not applicable */,
        0 /* output path ID, not applicable */, mutable_cf_options.compression,
        mutable_cf_options.compression_opts, Temperature::kUnknown,
        0 /* max_subcompactions, not applicable */,
        {} /* grandparents, not applicable */,
        std::nullopt /* earliest_snapshot */, nullptr /* snapshot_checker */,
        CompactionReason::kRefitLevel, "" /* trim_ts */,
        -1 /* score, not applicable */,
        false /* l0_files_might_overlap, not applicable */));
    cfd->compaction_picker()->RegisterCompaction(c.get());
    TEST_SYNC_POINT("DBImpl::ReFitLevel:PostRegisterCompaction");
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());

    for (const auto& f : vstorage->LevelFiles(level)) {
      edit.DeleteFile(level, f->fd.GetNumber());
      edit.AddExistingFile(to_level, *f, kProvenanceMovedBetweenLevels);
    }
    ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                    "[%s] Apply version edit:\n%s", cfd->GetName().c_str(),
                    edit.DebugString().data());

    Status status =
        versions_->LogAndApply(cfd, read_options, write_options, &edit, &mutex_,
                               directories_.GetDbDir());
    c->MarkFilesBeingCompacted(false);
    cfd->compaction_picker()->UnregisterCompaction(c.get());
    c.reset();

    InstallSuperVersionAndScheduleWork(cfd, &sv_context,
                                       /*defer_snapshot_refresh=*/false);

    ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] LogAndApply: %s\n",
                    cfd->GetName().c_str(), status.ToString().data());

    if (status.ok()) {
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] After refitting:\n%s", cfd->GetName().c_str(),
                      cfd->current()->DebugString().data());
    }
    sv_context.Clean();
    refitting_level_ = false;

    return status;
  }

  refitting_level_ = false;
  return Status::OK();
}

int DBImpl::NumberLevels(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  return cfh->cfd()->NumberLevels();
}

int DBImpl::Level0StopWriteTrigger(ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  InstrumentedMutexLock l(&mutex_);
  return cfh->cfd()
      ->GetSuperVersion()
      ->mutable_cf_options.level0_stop_writes_trigger;
}

Status DBImpl::FlushAllColumnFamilies(const FlushOptions& flush_options,
                                      FlushReason flush_reason) {
  mutex_.AssertHeld();
  Status status;
  if (immutable_db_options_.atomic_flush || flush_options.force_atomic_flush) {
    mutex_.Unlock();
    TEST_SYNC_POINT(
        "DBImpl::FlushAllColumnFamilies:BeforeAtomicFlushMemTables");
    status = AtomicFlushMemTables(flush_options, flush_reason);
    mutex_.Lock();
  } else {
    for (auto cfd : versions_->GetRefedColumnFamilySet()) {
      if (cfd->IsDropped()) {
        continue;
      }
      mutex_.Unlock();
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:BeforeFlushMemTable");
      status = FlushMemTable(cfd, flush_options, flush_reason);
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:1");
      TEST_SYNC_POINT("DBImpl::FlushAllColumnFamilies:2");
      mutex_.Lock();
      if (!status.ok() && !status.IsColumnFamilyDropped()) {
        break;
      } else if (status.IsColumnFamilyDropped()) {
        status = Status::OK();
      }
    }
  }
  return status;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     ColumnFamilyHandle* column_family) {
  auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  ROCKS_LOG_INFO(immutable_db_options_.info_log, "[%s] Manual flush start.",
                 cfh->GetName().c_str());
  Status s;
  if (immutable_db_options_.atomic_flush || flush_options.force_atomic_flush) {
    s = AtomicFlushMemTables(flush_options, FlushReason::kManualFlush,
                             {cfh->cfd()});
  } else {
    s = FlushMemTable(cfh->cfd(), flush_options, FlushReason::kManualFlush);
  }

  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] Manual flush finished, status: %s\n",
                 cfh->GetName().c_str(), s.ToString().c_str());
  return s;
}

Status DBImpl::Flush(const FlushOptions& flush_options,
                     const std::vector<ColumnFamilyHandle*>& column_families) {
  Status s;
  if (!immutable_db_options_.atomic_flush &&
      !flush_options.force_atomic_flush) {
    for (auto cfh : column_families) {
      s = Flush(flush_options, cfh);
      if (!s.ok()) {
        break;
      }
    }
  } else {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush start.\n"
                   "=====Column families:=====");
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
    autovector<ColumnFamilyData*> cfds;
    std::for_each(column_families.begin(), column_families.end(),
                  [&cfds](ColumnFamilyHandle* elem) {
                    auto cfh = static_cast<ColumnFamilyHandleImpl*>(elem);
                    cfds.emplace_back(cfh->cfd());
                  });
    s = AtomicFlushMemTables(flush_options, FlushReason::kManualFlush, cfds);
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Manual atomic flush finished, status: %s\n"
                   "=====Column families:=====",
                   s.ToString().c_str());
    for (auto cfh : column_families) {
      auto cfhi = static_cast<ColumnFamilyHandleImpl*>(cfh);
      ROCKS_LOG_INFO(immutable_db_options_.info_log, "%s",
                     cfhi->GetName().c_str());
    }
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "=====End of column families list=====");
  }
  return s;
}

Status DBImpl::RunManualCompaction(
    ColumnFamilyData* cfd, int input_level, int output_level,
    const CompactRangeOptions& compact_range_options, const Slice* begin,
    const Slice* end, bool exclusive, bool disallow_trivial_move,
    uint64_t max_file_num_to_ignore, const std::string& trim_ts,
    int* final_output_level) {
  assert(input_level == ColumnFamilyData::kCompactAllLevels ||
         input_level >= 0);

  InternalKey begin_storage;
  InternalKey end_storage;
  CompactionArg* ca = nullptr;

  bool scheduled = false;
  bool unscheduled = false;
  Env::Priority thread_pool_priority = Env::Priority::TOTAL;
  bool manual_conflict = false;

  ManualCompactionState manual(
      cfd, input_level, output_level, compact_range_options.target_path_id,
      exclusive,
      disallow_trivial_move || compact_range_options.skip_missing_blob_files,
      compact_range_options.canceled);
  // For universal compaction, we enforce every manual compaction to compact
  // all files.
  if (begin == nullptr ||
      cfd->ioptions().compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    manual.begin = nullptr;
  } else {
    begin_storage.SetMinPossibleForUserKey(*begin);
    manual.begin = &begin_storage;
  }
  if (end == nullptr ||
      cfd->ioptions().compaction_style == kCompactionStyleUniversal ||
      cfd->ioptions().compaction_style == kCompactionStyleFIFO) {
    manual.end = nullptr;
  } else {
    end_storage.SetMaxPossibleForUserKey(*end);
    manual.end = &end_storage;
  }

  TEST_SYNC_POINT("DBImpl::RunManualCompaction:0");
  TEST_SYNC_POINT("DBImpl::RunManualCompaction:1");
  InstrumentedMutexLock l(&mutex_);

  if (manual_compaction_paused_ > 0) {
    // Does not make sense to `AddManualCompaction()` in this scenario since
    // `DisableManualCompaction()` just waited for the manual compaction queue
    // to drain. So return immediately.
    TEST_SYNC_POINT("DBImpl::RunManualCompaction:PausedAtStart");
    manual.status =
        Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    manual.done = true;
    return manual.status;
  }

  if (IsCompactionAborted(cfd)) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "[%s] RunManualCompaction: aborting compaction",
                   cfd->GetName().c_str());
    manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
    manual.done = true;
    return manual.status;
  }

  // When a manual compaction arrives, temporarily disable scheduling of
  // non-manual compactions and wait until the number of scheduled compaction
  // jobs drops to zero. This used to be needed to ensure that this manual
  // compaction can compact any range of keys/files. Now it is optional
  // (see `CompactRangeOptions::exclusive_manual_compaction`). The use case for
  // `exclusive_manual_compaction=true` is unclear beyond not trusting the code.
  //
  // HasPendingManualCompaction() is true when at least one thread is inside
  // RunManualCompaction(), i.e. during that time no other compaction will
  // get scheduled (see MaybeScheduleFlushOrCompaction).
  //
  // Note that the following loop doesn't stop more that one thread calling
  // RunManualCompaction() from getting to the second while loop below.
  // However, only one of them will actually schedule compaction, while
  // others will wait on a condition variable until it completes.

  AddManualCompaction(&manual);
  TEST_SYNC_POINT_CALLBACK("DBImpl::RunManualCompaction:NotScheduled", &mutex_);
  if (exclusive) {
    // Limitation: there's no way to wake up the below loop when user sets
    // `*manual.canceled`. So `CompactRangeOptions::exclusive_manual_compaction`
    // and `CompactRangeOptions::canceled` might not work well together.
    while (CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0) {
      if (IsCompactionAborted(cfd)) {
        // Pretend the error came from compaction so the below cleanup/error
        // handling code can process it.
        manual.done = true;
        manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
        break;
      }
      if (manual_compaction_paused_ > 0 || manual.canceled == true) {
        // Pretend the error came from compaction so the below cleanup/error
        // handling code can process it.
        manual.done = true;
        manual.status =
            Status::Incomplete(Status::SubCode::kManualCompactionPaused);
        break;
      }
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:WaitScheduled");
      ROCKS_LOG_INFO(
          immutable_db_options_.info_log,
          "[%s] Manual compaction waiting for all other scheduled background "
          "compactions to finish",
          cfd->GetName().c_str());
      bg_cv_.Wait();
    }
  }

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());

  ROCKS_LOG_BUFFER(&log_buffer, "[%s] Manual compaction starting",
                   cfd->GetName().c_str());

  // We don't check bg_error_ here, because if we get the error in compaction,
  // the compaction will set manual.status to bg_error_ and set manual.done to
  // true.
  while (!manual.done) {
    assert(HasPendingManualCompaction());
    if (IsCompactionAborted(cfd) && !scheduled && !manual.in_progress) {
      manual.done = true;
      manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
      break;
    }
    manual_conflict = false;
    Compaction* compaction = nullptr;
    if (ShouldntRunManualCompaction(&manual) || (manual.in_progress == true) ||
        scheduled ||
        (((manual.manual_end = &manual.tmp_storage1) != nullptr) &&
         ((compaction = manual.cfd->CompactRange(
               manual.cfd->GetLatestMutableCFOptions(), mutable_db_options_,
               manual.input_level, manual.output_level, compact_range_options,
               manual.begin, manual.end, &manual.manual_end, &manual_conflict,
               max_file_num_to_ignore, trim_ts)) == nullptr &&
          manual_conflict))) {
      if (!manual.done) {
        bg_cv_.Wait();
      }
      if (manual_compaction_paused_ > 0 && scheduled && !unscheduled) {
        assert(thread_pool_priority != Env::Priority::TOTAL);
        // unschedule all manual compactions
        auto unscheduled_task_num = env_->UnSchedule(
            GetTaskTag(TaskType::kManualCompaction), thread_pool_priority);
        if (unscheduled_task_num > 0) {
          ROCKS_LOG_INFO(
              immutable_db_options_.info_log,
              "[%s] Unscheduled %d number of manual compactions from the "
              "thread-pool",
              cfd->GetName().c_str(), unscheduled_task_num);
          // it may unschedule other manual compactions, notify others.
          bg_cv_.SignalAll();
        }
        unscheduled = true;
        TEST_SYNC_POINT("DBImpl::RunManualCompaction:Unscheduled");
      }
      if (scheduled && manual.incomplete == true) {
        assert(!manual.in_progress);
        scheduled = false;
        manual.incomplete = false;
      }
    } else if (!scheduled) {
      if (compaction == nullptr) {
        manual.done = true;
        if (final_output_level) {
          // No compaction needed or there is a conflicting compaction.
          // Still set `final_output_level` to the level where we would
          // have compacted to.
          *final_output_level = output_level;
          if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
            *final_output_level = cfd->current()->storage_info()->base_level();
          }
        }
        bg_cv_.SignalAll();
        continue;
      }
      ca = new CompactionArg;
      ca->db = this;
      ca->prepicked_compaction = new PrepickedCompaction;
      ca->prepicked_compaction->manual_compaction_state = &manual;
      ca->prepicked_compaction->compaction = compaction;
      if (!RequestCompactionToken(
              cfd, true, &ca->prepicked_compaction->task_token, &log_buffer)) {
        // Don't throttle manual compaction, only count outstanding tasks.
        assert(false);
      }
      ca->prepicked_compaction->need_repick = false;
      manual.incomplete = false;
      if (compaction->bottommost_level() &&
          env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
        ca->compaction_pri_ = Env::Priority::BOTTOM;
        thread_pool_priority = Env::Priority::BOTTOM;
      } else {
        ca->compaction_pri_ = Env::Priority::LOW;
        thread_pool_priority = Env::Priority::LOW;
      }
      Status schedule_status =
          ScheduleCompaction(ca, GetTaskTag(TaskType::kManualCompaction));
      if (!schedule_status.ok()) {
        manual.status = std::move(schedule_status);
        manual.done = true;
        bg_cv_.SignalAll();
        continue;
      }
      scheduled = true;
      TEST_SYNC_POINT("DBImpl::RunManualCompaction:Scheduled");
      if (final_output_level) {
        *final_output_level = compaction->output_level();
      }
    }
    if (!scheduled) {
      // There is nothing scheduled to wait on, so any cancellation can end the
      // manual now.
      if (IsCompactionAborted(cfd)) {
        // Stop waiting since it was canceled. Pretend the error came from
        // compaction so the below cleanup/error handling code can process it.
        manual.done = true;
        manual.status = Status::Incomplete(Status::SubCode::kCompactionAborted);
      } else if (manual_compaction_paused_ > 0 || manual.canceled == true) {
        // Stop waiting since it was canceled. Pretend the error came from
        // compaction so the below cleanup/error handling code can process it.
        manual.done = true;
        manual.status =
            Status::Incomplete(Status::SubCode::kManualCompactionPaused);
      }
    }
  }

  log_buffer.FlushBufferToLog();
  assert(!manual.in_progress);
  assert(HasPendingManualCompaction());
  RemoveManualCompaction(&manual);
  // If the manual job did not succeed, try to schedule other jobs in case
  // there is any unscheduled compaction job which was blocked by exclusive
  // manual compaction, including the column family that the failed manual
  // compaction requeued when it released its input files.
  if (!manual.status.ok()) {
    MaybeScheduleFlushOrCompaction();
  }
  bg_cv_.SignalAll();
  return manual.status;
}

void DBImpl::GenerateFlushRequest(const autovector<ColumnFamilyData*>& cfds,
                                  FlushReason flush_reason, FlushRequest* req) {
  assert(req != nullptr);
  req->flush_reason = flush_reason;
  req->cfd_to_max_mem_id_to_persist.reserve(cfds.size());
  for (const auto cfd : cfds) {
    if (nullptr == cfd) {
      // cfd may be null, see DBImpl::ScheduleFlushes
      continue;
    }
    uint64_t max_memtable_id = cfd->imm()->GetLatestMemTableID(
        req->atomic_flush /* for_atomic_flush */);
    req->cfd_to_max_mem_id_to_persist.emplace(cfd, max_memtable_id);
  }
}

void DBImpl::NotifyOnManualFlushScheduled(
    const autovector<ColumnFamilyData*>& cfds, FlushReason flush_reason) {
  if (immutable_db_options_.listeners.size() == 0U) {
    return;
  }
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  std::vector<ManualFlushInfo> info;
  for (ColumnFamilyData* cfd : cfds) {
    info.push_back({cfd->GetID(), cfd->GetName(), flush_reason});
  }
  for (const auto& listener : immutable_db_options_.listeners) {
    listener->OnManualFlushScheduled(this, info);
  }
}

void DBImpl::MaybeSyncLastSequenceWithAllocatedForRecovery(
    FlushReason flush_reason) {
  mutex_.AssertHeld();
  if (two_write_queues_ && IsRecoveryFlush(flush_reason)) {
    versions_->SyncLastSequenceWithAllocated();
  }
}

Status DBImpl::EnterWriteThreadForNonBlockingFlush(
    WriteThread::Writer* w, WriteThread::Writer* nonmem_w) {
  mutex_.AssertHeld();
  if (!write_thread_.EnterUnbatchedNonBlocking(w, &mutex_)) {
    return Status::Incomplete(
        "Write stall in progress, unable to join the write thread to switch "
        "memtables");
  }
  if (two_write_queues_ &&
      !nonmem_write_thread_.EnterUnbatchedNonBlocking(nonmem_w, &mutex_)) {
    write_thread_.ExitUnbatched(w);
    return Status::Incomplete(
        "Write stall in progress, unable to join the write thread to switch "
        "memtables");
  }
  if (!error_handler_.IsBGWorkStopped() && !write_controller_.IsStopped() &&
      !WouldBlockJoiningWriteThread()) {
    return Status::OK();
  }
  write_thread_.ExitUnbatched(w);
  if (two_write_queues_) {
    nonmem_write_thread_.ExitUnbatched(nonmem_w);
  }
  return Status::Incomplete(
      "Write stop started while joining the write thread to switch memtables");
}

Status DBImpl::FlushMemTable(ColumnFamilyData* cfd,
                             const FlushOptions& flush_options,
                             FlushReason flush_reason,
                             bool entered_write_thread) {
  return FlushMemTableImpl(cfd, flush_options, flush_reason,
                           entered_write_thread, WriteThreadJoinMode::kBlocking,
                           nullptr /* made_progress */);
}

Status DBImpl::FlushMemTableImpl(ColumnFamilyData* cfd,
                                 const FlushOptions& flush_options,
                                 FlushReason flush_reason,
                                 bool entered_write_thread,
                                 WriteThreadJoinMode join_mode,
                                 bool* made_progress) {
  if (made_progress != nullptr) {
    *made_progress = false;
  }
  // This method should not be called if atomic_flush is true.
  assert(!immutable_db_options_.atomic_flush);
  assert(!IsRecoveryFlush(flush_reason));
  if (!flush_options.wait && write_controller_.IsStopped()) {
    std::ostringstream oss;
    oss << "Writes have been stopped, thus unable to perform manual flush. "
           "Please try again later after writes are resumed";
    return Status::TryAgain(oss.str());
  }
  if (flush_options.wait) {
    InstrumentedMutexLock lock(&mutex_);
    if (lock_wal_owner_thread_id_counts_.count(env_->GetThreadID()) > 0) {
      return Status::Aborted(
          "Likely deadlock as the same thread called LockWAL()");
    }
  }
  Status s;
  if (!flush_options.allow_write_stall) {
    bool flush_needed = true;
    s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
    TEST_SYNC_POINT("DBImpl::FlushMemTable:StallWaitDone");
    if (!s.ok() || !flush_needed) {
      return s;
    }
  }

  const bool needs_to_join_write_thread = !entered_write_thread;
  autovector<FlushRequest> flush_reqs;
  autovector<uint64_t> memtable_ids_to_wait;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (needs_to_join_write_thread) {
      if (join_mode == WriteThreadJoinMode::kNonBlocking) {
        s = EnterWriteThreadForNonBlockingFlush(&w, &nonmem_w);
        if (!s.ok()) {
          return s;
        }
      } else {
        write_thread_.EnterUnbatched(&w, &mutex_);
        if (two_write_queues_) {
          nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
        }
      }
    }
    if (join_mode == WriteThreadJoinMode::kNonBlocking && cfd->IsDropped()) {
      TEST_SYNC_POINT("DBImpl::FlushMemTableImpl:Dropped");
      if (needs_to_join_write_thread) {
        write_thread_.ExitUnbatched(&w);
        if (two_write_queues_) {
          nonmem_write_thread_.ExitUnbatched(&nonmem_w);
        }
      }
      s.PermitUncheckedOk();
      return Status::ColumnFamilyDropped();
    }
    WaitForPendingWrites();

    if (join_mode == WriteThreadJoinMode::kNonBlocking &&
        flush_reason == FlushReason::kWriteBufferManager &&
        cfd->imm()->IsFlushPendingOrRunning()) {
      TEST_SYNC_POINT("DBImpl::FlushMemTableImpl:AlreadyFlushing");
      if (needs_to_join_write_thread) {
        write_thread_.ExitUnbatched(&w);
        if (two_write_queues_) {
          nonmem_write_thread_.ExitUnbatched(&nonmem_w);
        }
      }
      return Status::Incomplete(
          "Column family started flushing before the WBM handoff completed");
    }

    // Recovery may have released `mutex_` after the earlier `ResumeImpl()`
    // sync. Refresh sequence state at the actual memtable-switch fence, after
    // both write queues are drained and before `SwitchMemtable()` consumes
    // `LastSequence()`.
    MaybeSyncLastSequenceWithAllocatedForRecovery(flush_reason);

    if (!cfd->mem()->IsEmpty() || !cached_recoverable_state_empty_.load() ||
        IsRecoveryFlush(flush_reason)) {
      s = SwitchMemtable(cfd, &context);
      if (s.ok() && made_progress != nullptr) {
        *made_progress = true;
      }
    }
    const uint64_t flush_memtable_id = std::numeric_limits<uint64_t>::max();
    if (s.ok()) {
      if (cfd->imm()->NumNotFlushed() != 0 || !cfd->mem()->IsEmpty() ||
          !cached_recoverable_state_empty_.load() ||
          IsRecoveryFlush(flush_reason)) {
        FlushRequest req{
            flush_reason, false /* atomic_flush */, {{cfd, flush_memtable_id}}};
        flush_reqs.emplace_back(std::move(req));
        memtable_ids_to_wait.emplace_back(
            cfd->imm()->GetLatestMemTableID(false /* for_atomic_flush */));
      }
      if (immutable_db_options_.persist_stats_to_disk) {
        ColumnFamilyData* cfd_stats =
            versions_->GetColumnFamilySet()->GetColumnFamily(
                kPersistentStatsColumnFamilyName);
        if (cfd_stats != nullptr && cfd_stats != cfd &&
            !cfd_stats->mem()->IsEmpty()) {
          // only force flush stats CF when it will be the only CF lagging
          // behind after the current flush
          bool stats_cf_flush_needed = true;
          for (auto* loop_cfd : *versions_->GetColumnFamilySet()) {
            if (loop_cfd == cfd_stats || loop_cfd == cfd) {
              continue;
            }
            if (loop_cfd->GetLogNumber() <= cfd_stats->GetLogNumber()) {
              stats_cf_flush_needed = false;
            }
          }
          if (stats_cf_flush_needed) {
            ROCKS_LOG_INFO(immutable_db_options_.info_log,
                           "Force flushing stats CF with manual flush of %s "
                           "to avoid holding old logs",
                           cfd->GetName().c_str());
            s = SwitchMemtable(cfd_stats, &context);
            FlushRequest req{flush_reason,
                             false /* atomic_flush */,
                             {{cfd_stats, flush_memtable_id}}};
            flush_reqs.emplace_back(std::move(req));
            memtable_ids_to_wait.emplace_back(
                cfd_stats->imm()->GetLatestMemTableID(
                    false /* for_atomic_flush */));
          }
        }
      }
    }

    if (s.ok() && !flush_reqs.empty()) {
      for (const auto& req : flush_reqs) {
        assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
        ColumnFamilyData* loop_cfd =
            req.cfd_to_max_mem_id_to_persist.begin()->first;
        loop_cfd->imm()->FlushRequested();
      }
      // If the caller wants to wait for this flush to complete, it indicates
      // that the caller expects the ColumnFamilyData not to be free'ed by
      // other threads which may drop the column family concurrently.
      // Therefore, we increase the cfd's ref count.
      if (flush_options.wait) {
        for (const auto& req : flush_reqs) {
          assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
          ColumnFamilyData* loop_cfd =
              req.cfd_to_max_mem_id_to_persist.begin()->first;
          loop_cfd->Ref();
        }
      }
      for (const auto& req : flush_reqs) {
        assert(req.cfd_to_max_mem_id_to_persist.size() == 1);
        ColumnFamilyData* loop_cfd =
            req.cfd_to_max_mem_id_to_persist.begin()->first;
        bool already_queued_for_flush = loop_cfd->queued_for_flush();
        bool flush_req_enqueued = EnqueuePendingFlush(req);
        if (flush_req_enqueued && made_progress != nullptr) {
          *made_progress = true;
        }
        if (flush_reason != FlushReason::kWriteBufferManager &&
            (already_queued_for_flush || flush_req_enqueued)) {
          loop_cfd->SetFlushSkipReschedule();
        }
      }
      MaybeScheduleFlushOrCompaction();
    }

    if (needs_to_join_write_thread) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }

  if (flush_reason != FlushReason::kWriteBufferManager) {
    NotifyOnManualFlushScheduled({cfd}, flush_reason);
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::FlushMemTable:BeforeWaitForBgFlush");
  if (s.ok() && flush_options.wait) {
    autovector<ColumnFamilyData*> cfds;
    autovector<const uint64_t*> flush_memtable_ids;
    assert(flush_reqs.size() == memtable_ids_to_wait.size());
    for (size_t i = 0; i < flush_reqs.size(); ++i) {
      assert(flush_reqs[i].cfd_to_max_mem_id_to_persist.size() == 1);
      cfds.push_back(flush_reqs[i].cfd_to_max_mem_id_to_persist.begin()->first);
      flush_memtable_ids.push_back(&(memtable_ids_to_wait[i]));
    }
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        flush_reason == FlushReason::kErrorRecovery /* resuming_from_bg_err */,
        flush_reason, flush_options.listener_wait);
    InstrumentedMutexLock lock_guard(&mutex_);
    for (auto* tmp_cfd : cfds) {
      tmp_cfd->UnrefAndTryDelete();
    }
  }
  TEST_SYNC_POINT("DBImpl::FlushMemTable:FlushMemTableFinished");
  return s;
}

Status DBImpl::AtomicFlushMemTables(
    const FlushOptions& flush_options, FlushReason flush_reason,
    const autovector<ColumnFamilyData*>& provided_candidate_cfds,
    bool entered_write_thread) {
  return AtomicFlushMemTablesImpl(flush_options, flush_reason,
                                  provided_candidate_cfds, entered_write_thread,
                                  WriteThreadJoinMode::kBlocking,
                                  nullptr /* made_progress */);
}

Status DBImpl::AtomicFlushMemTablesImpl(
    const FlushOptions& flush_options, FlushReason flush_reason,
    const autovector<ColumnFamilyData*>& provided_candidate_cfds,
    bool entered_write_thread, WriteThreadJoinMode join_mode,
    bool* made_progress) {
  if (made_progress != nullptr) {
    *made_progress = false;
  }
  if (!flush_options.wait && write_controller_.IsStopped()) {
    std::ostringstream oss;
    oss << "Writes have been stopped, thus unable to perform manual flush. "
           "Please try again later after writes are resumed";
    return Status::TryAgain(oss.str());
  }
  if (flush_options.wait) {
    InstrumentedMutexLock lock(&mutex_);
    if (lock_wal_owner_thread_id_counts_.count(env_->GetThreadID()) > 0) {
      return Status::Aborted(
          "Likely deadlock as the same thread called LockWAL()");
    }
  }
  Status s;
  autovector<ColumnFamilyData*> candidate_cfds;
  if (provided_candidate_cfds.empty()) {
    // Generate candidate cfds if not provided
    {
      InstrumentedMutexLock l(&mutex_);
      for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
        if (!cfd->IsDropped() && cfd->initialized()) {
          cfd->Ref();
          candidate_cfds.push_back(cfd);
        }
      }
    }
  } else {
    candidate_cfds = provided_candidate_cfds;
  }

  const auto unref_generated_candidates = [&]() {
    if (!provided_candidate_cfds.empty()) {
      return;
    }
    for (auto candidate_cfd : candidate_cfds) {
      candidate_cfd->UnrefAndTryDelete();
    }
    candidate_cfds.clear();
  };

  if (!flush_options.allow_write_stall) {
    int num_cfs_to_flush = 0;
    for (auto cfd : candidate_cfds) {
      bool flush_needed = true;
      s = WaitUntilFlushWouldNotStallWrites(cfd, &flush_needed);
      if (s.IsColumnFamilyDropped() && provided_candidate_cfds.empty()) {
        // A generated candidate dropped while waiting has nothing to flush.
        s = Status::OK();
      } else if (!s.ok()) {
        unref_generated_candidates();
        return s;
      } else if (flush_needed) {
        ++num_cfs_to_flush;
      }
    }
    if (0 == num_cfs_to_flush) {
      unref_generated_candidates();
      return s;
    }
  }
  const bool needs_to_join_write_thread = !entered_write_thread;
  FlushRequest flush_req;
  flush_req.atomic_flush = true;
  autovector<ColumnFamilyData*> cfds;
  {
    WriteContext context;
    InstrumentedMutexLock guard_lock(&mutex_);

    WriteThread::Writer w;
    WriteThread::Writer nonmem_w;
    if (needs_to_join_write_thread) {
      if (join_mode == WriteThreadJoinMode::kNonBlocking) {
        s = EnterWriteThreadForNonBlockingFlush(&w, &nonmem_w);
        if (!s.ok()) {
          unref_generated_candidates();
          return s;
        }
      } else {
        write_thread_.EnterUnbatched(&w, &mutex_);
        if (two_write_queues_) {
          nonmem_write_thread_.EnterUnbatched(&nonmem_w, &mutex_);
        }
      }
    }
    WaitForPendingWrites();

    // Recovery may have released `mutex_` after the earlier `ResumeImpl()`
    // sync. Refresh sequence state at the atomic memtable-switch fence, after
    // both write queues are drained and before `SwitchMemtable()` consumes
    // `LastSequence()`.
    MaybeSyncLastSequenceWithAllocatedForRecovery(flush_reason);

    SelectColumnFamiliesForAtomicFlush(&cfds, candidate_cfds, flush_reason);

    unref_generated_candidates();

    const bool defer_wbm_accounting_refresh = cfds.size() > 1;
    bool switch_attempted = false;
    for (auto cfd : cfds) {
      if (cfd->mem()->IsEmpty() && cached_recoverable_state_empty_.load() &&
          !IsRecoveryFlush(flush_reason)) {
        continue;
      }
      cfd->Ref();
      switch_attempted = true;
      s = defer_wbm_accounting_refresh
              ? SwitchMemtableImpl(cfd, &context, nullptr, 0,
                                   false /* refresh_wbm_accounting */)
              : SwitchMemtable(cfd, &context);
      cfd->UnrefAndTryDelete();
      if (!s.ok()) {
        break;
      }
      if (made_progress != nullptr) {
        *made_progress = true;
      }
    }
    if (defer_wbm_accounting_refresh && switch_attempted) {
      UpdateWBMAccountingAfterMemtableSwitch(true /* refresh_now */);
    }
    if (s.ok()) {
      AssignAtomicFlushSeq(cfds);
      for (auto cfd : cfds) {
        cfd->imm()->FlushRequested();
      }
      // If the caller wants to wait for this flush to complete, it indicates
      // that the caller expects the ColumnFamilyData not to be free'ed by
      // other threads which may drop the column family concurrently.
      // Therefore, we increase the cfd's ref count.
      if (flush_options.wait) {
        for (auto cfd : cfds) {
          cfd->Ref();
        }
      }
      GenerateFlushRequest(cfds, flush_reason, &flush_req);
      if (EnqueuePendingFlush(flush_req) && made_progress != nullptr) {
        *made_progress = true;
      }
      MaybeScheduleFlushOrCompaction();
    }

    if (needs_to_join_write_thread) {
      write_thread_.ExitUnbatched(&w);
      if (two_write_queues_) {
        nonmem_write_thread_.ExitUnbatched(&nonmem_w);
      }
    }
  }
  if (flush_reason != FlushReason::kWriteBufferManager) {
    NotifyOnManualFlushScheduled(cfds, flush_reason);
  }
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:AfterScheduleFlush");
  TEST_SYNC_POINT("DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush");
  if (s.ok() && flush_options.wait) {
    // Walk cfds and look each one up, so the ids line up with cfds by
    // construction. Iterating the map instead pairs ids with the wrong CF
    // (its order is unrelated to cfds), and a CF that dropped out of the map
    // (an idle CF de-selected during the switch loop, or a duplicate handle
    // collapsed by the map) would make the two vectors different lengths.
    autovector<const uint64_t*> flush_memtable_ids;
    flush_memtable_ids.reserve(cfds.size());
    for (auto* cfd : cfds) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfd);
      flush_memtable_ids.push_back(
          iter == flush_req.cfd_to_max_mem_id_to_persist.end()
              ? nullptr
              : &(iter->second));
    }
#ifndef NDEBUG
    // WaitForFlushMemTables pairs flush_memtable_ids[i] with cfds[i], so each
    // entry must point at this cfd's own map id (or be null when absent). Fires
    // if the ids are ever rebuilt in the request map's order.
    assert(flush_memtable_ids.size() == cfds.size());
    for (size_t i = 0; i < cfds.size(); ++i) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfds[i]);
      assert(flush_memtable_ids[i] ==
             (iter == flush_req.cfd_to_max_mem_id_to_persist.end()
                  ? nullptr
                  : &(iter->second)));
    }
#endif  // NDEBUG
    s = WaitForFlushMemTables(
        cfds, flush_memtable_ids,
        flush_reason == FlushReason::kErrorRecovery /* resuming_from_bg_err */,
        flush_reason, flush_options.listener_wait);
    if (s.IsColumnFamilyDropped() && provided_candidate_cfds.empty()) {
      // Every selected CF was dropped. Generated candidates that were not
      // selected had nothing to flush.
      s = Status::OK();
    }
    InstrumentedMutexLock lock_guard(&mutex_);
    for (auto* cfd : cfds) {
      cfd->UnrefAndTryDelete();
    }
  }
  return s;
}

Status DBImpl::RetryFlushesForErrorRecovery(FlushReason flush_reason,
                                            bool wait) {
  mutex_.AssertHeld();
  assert(flush_reason == FlushReason::kErrorRecoveryRetryFlush ||
         flush_reason == FlushReason::kCatchUpAfterErrorRecovery);

  // Collect referenced CFDs.
  autovector<ColumnFamilyData*> cfds;
  for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
    if (!cfd->IsDropped() && cfd->initialized() &&
        cfd->imm()->NumNotFlushed() != 0) {
      cfd->Ref();
      cfd->imm()->FlushRequested();
      cfds.push_back(cfd);
    }
  }

  // Submit flush requests for all immutable memtables needing flush.
  // `flush_memtable_ids` will be populated such that all immutable
  // memtables eligible for flush are waited on before this function
  // returns.
  autovector<uint64_t> flush_memtable_ids;
  if (immutable_db_options_.atomic_flush) {
    FlushRequest flush_req;
    flush_req.atomic_flush = true;
    GenerateFlushRequest(cfds, flush_reason, &flush_req);
    EnqueuePendingFlush(flush_req);
    // Keep flush_memtable_ids aligned with cfds by walking cfds, since the
    // request map's order is unrelated to cfds. A CF not in the map waits on
    // its whole imm list, matching the max-bound the non-atomic branch uses.
    for (auto* cfd : cfds) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfd);
      flush_memtable_ids.push_back(
          iter == flush_req.cfd_to_max_mem_id_to_persist.end()
              ? std::numeric_limits<uint64_t>::max()
              : iter->second);
    }
#ifndef NDEBUG
    // WaitForFlushMemTables pairs flush_memtable_ids[i] with cfds[i], so each
    // entry must be this cfd's own max id. Fires if the ids are ever rebuilt in
    // the request map's order, which is unrelated to cfds order.
    assert(flush_memtable_ids.size() == cfds.size());
    for (size_t i = 0; i < cfds.size(); ++i) {
      auto iter = flush_req.cfd_to_max_mem_id_to_persist.find(cfds[i]);
      assert(flush_memtable_ids[i] ==
             (iter == flush_req.cfd_to_max_mem_id_to_persist.end()
                  ? std::numeric_limits<uint64_t>::max()
                  : iter->second));
    }
#endif  // NDEBUG
  } else {
    for (auto cfd : cfds) {
      flush_memtable_ids.push_back(
          cfd->imm()->GetLatestMemTableID(false /* for_atomic_flush */));
      // Impose no bound on the highest memtable ID flushed. There is no
      // reason to do so outside of atomic flush.
      FlushRequest flush_req{
          flush_reason,
          false /* atomic_flush */,
          {{cfd,
            std::numeric_limits<uint64_t>::max() /* max_mem_id_to_persist */}}};
      if (EnqueuePendingFlush(flush_req)) {
        cfd->SetFlushSkipReschedule();
      };
    }
  }
  MaybeScheduleFlushOrCompaction();

  Status s;
  if (wait) {
    mutex_.Unlock();
    autovector<const uint64_t*> flush_memtable_id_ptrs;
    for (auto& flush_memtable_id : flush_memtable_ids) {
      flush_memtable_id_ptrs.push_back(&flush_memtable_id);
    }
    s = WaitForFlushMemTables(cfds, flush_memtable_id_ptrs,
                              true /* resuming_from_bg_err */, flush_reason);
    mutex_.Lock();
  }

  for (auto* cfd : cfds) {
    cfd->UnrefAndTryDelete();
  }
  return s;
}

// Calling FlushMemTable(), whether from DB::Flush() or from Backup Engine, can
// cause write stall, for example if one memtable is being flushed already.
// This method tries to avoid write stall (similar to CompactRange() behavior)
// it emulates how the SuperVersion / LSM would change if flush happens, checks
// it against various constrains and delays flush if it'd cause write stall.
// Caller should check status and flush_needed to see if flush already happened.
Status DBImpl::WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                                 bool* flush_needed) {
  {
    *flush_needed = true;
    InstrumentedMutexLock l(&mutex_);
    uint64_t orig_active_memtable_id = cfd->mem()->GetID();
    WriteStallCondition write_stall_condition = WriteStallCondition::kNormal;
    do {
      if (write_stall_condition != WriteStallCondition::kNormal) {
        // Same error handling as user writes: Don't wait if there's a
        // background error, even if it's a soft error. We might wait here
        // indefinitely as the pending flushes/compactions may never finish
        // successfully, resulting in the stall condition lasting indefinitely
        if (error_handler_.IsBGWorkStopped()) {
          return error_handler_.GetBGError();
        }

        TEST_SYNC_POINT("DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait");
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "[%s] WaitUntilFlushWouldNotStallWrites"
                       " waiting on stall conditions to clear",
                       cfd->GetName().c_str());
        bg_cv_.Wait();
      }
      if (cfd->IsDropped()) {
        return Status::ColumnFamilyDropped();
      }
      if (shutting_down_.load(std::memory_order_acquire)) {
        return Status::ShutdownInProgress();
      }

      uint64_t earliest_memtable_id =
          std::min(cfd->mem()->GetID(), cfd->imm()->GetEarliestMemTableID());
      if (earliest_memtable_id > orig_active_memtable_id) {
        // We waited so long that the memtable we were originally waiting on was
        // flushed.
        *flush_needed = false;
        return Status::OK();
      }

      const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
      const auto* vstorage = cfd->current()->storage_info();

      // Skip stalling check if we're below auto-flush and auto-compaction
      // triggers. If it stalled in these conditions, that'd mean the stall
      // triggers are so low that stalling is needed for any background work. In
      // that case we shouldn't wait since background work won't be scheduled.
      if (cfd->imm()->NumNotFlushed() <
              cfd->ioptions().min_write_buffer_number_to_merge &&
          vstorage->l0_delay_trigger_count() <
              mutable_cf_options.level0_file_num_compaction_trigger) {
        break;
      }

      // check whether one extra immutable memtable or an extra L0 file would
      // cause write stalling mode to be entered. It could still enter stall
      // mode due to pending compaction bytes, but that's less common
      // No extra immutable Memtable will be created if the current Memtable is
      // empty.
      write_stall_condition =
          ColumnFamilyData::GetWriteStallConditionAndCause(
              cfd->GetUnflushedMemTableCountForWriteStallCheck(),
              vstorage->l0_delay_trigger_count() + 1,
              vstorage->estimated_compaction_needed_bytes(), mutable_cf_options,
              cfd->ioptions(), cfd->is_table_builder_supported())
              .first;
    } while (write_stall_condition != WriteStallCondition::kNormal);
  }
  return Status::OK();
}

// Wait for memtables to be flushed for multiple column families.
// let N = cfds.size()
// for i in [0, N),
//  1) if flush_memtable_ids[i] is not null, then the memtables with lower IDs
//     have to be flushed for THIS column family;
//  2) if flush_memtable_ids[i] is null, then all memtables in THIS column
//     family have to be flushed.
// Finish waiting when ALL column families finish flushing memtables.
// resuming_from_bg_err indicates whether the caller is trying to resume from
// background error or in normal processing.
// Note that the wait finishes when the flush result is installed to column
// families' Versions and persisted in MANIFEST. It doesn't wait until
// SuperVersion to reflect the flush result, except for the case when
// flush_reason is `kExternalFileIngestion`.
Status DBImpl::WaitForFlushMemTables(
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const uint64_t*>& flush_memtable_ids,
    bool resuming_from_bg_err, std::optional<FlushReason> flush_reason,
    bool wait_for_listener_notifications) {
  int num = static_cast<int>(cfds.size());
  // Wait until the compaction completes
  InstrumentedMutexLock l(&mutex_);
  Status s;
  // If the caller is trying to resume from bg error, then
  // error_handler_.IsDBStopped() is true.
  while (resuming_from_bg_err || !error_handler_.IsDBStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      s = Status::ShutdownInProgress();
      return s;
    }
    // If an error has occurred during resumption, then no need to wait.
    // But flush operation may fail because of this error, so need to
    // return the status.
    if (!error_handler_.GetRecoveryError().ok()) {
      s = error_handler_.GetRecoveryError();
      break;
    }
    // If BGWorkStopped, which indicate that there is a BG error and
    // 1) soft error but requires no BG work, 2) no in auto_recovery_
    if (!resuming_from_bg_err && error_handler_.IsBGWorkStopped() &&
        error_handler_.GetBGError().severity() < Status::Severity::kHardError) {
      s = error_handler_.GetBGError();
      return s;
    }

    // Number of column families that have been dropped.
    int num_dropped = 0;
    // Number of column families that have finished flush.
    int num_finished = 0;
    for (int i = 0; i < num; ++i) {
      if (cfds[i]->IsDropped()) {
        ++num_dropped;
      } else if (cfds[i]->imm()->NumNotFlushed() == 0 ||
                 (flush_memtable_ids[i] != nullptr &&
                  cfds[i]->imm()->GetEarliestMemTableID() >
                      *flush_memtable_ids[i])) {
        // Make file ingestion's flush wait until SuperVersion is also updated
        // since after flush, it does range overlapping check and file level
        // assignment with the current SuperVersion.
        if (!flush_reason.has_value() ||
            flush_reason.value() != FlushReason::kExternalFileIngestion ||
            cfds[i]->GetSuperVersion()->imm->GetID() ==
                cfds[i]->imm()->current()->GetID()) {
          // When the caller requested it, also wait for the flush-completion
          // listener callbacks for this CF to finish before counting it as
          // finished (see FlushOptions::listener_wait).
          if (!wait_for_listener_notifications ||
              cfds[i]->imm()->NumPendingFlushNotifications() == 0) {
            ++num_finished;
          }
        }
      }
    }
    if (1 == num_dropped && 1 == num) {
      s = Status::ColumnFamilyDropped();
      return s;
    }
    // Column families involved in this flush request have either been dropped
    // or finished flush. Then it's time to finish waiting.
    if (num_dropped + num_finished == num) {
      break;
    }
    bg_cv_.Wait();
  }
  // If not resuming from bg error, and an error has caused the DB to stop,
  // then report the bg error to caller.
  if (!resuming_from_bg_err && error_handler_.IsDBStopped()) {
    s = error_handler_.GetBGError();
  }
  return s;
}

Status DBImpl::EnableAutoCompaction(
    const std::vector<ColumnFamilyHandle*>& column_family_handles) {
  return SetOptions(column_family_handles,
                    {{"disable_auto_compactions", "false"}});
}

// NOTE: Calling DisableManualCompaction() may overwrite the
// user-provided canceled variable in CompactRangeOptions
void DBImpl::DisableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  manual_compaction_paused_.fetch_add(1, std::memory_order_release);

  // Mark the canceled as true when the cancellation is triggered by
  // manual_compaction_paused (may overwrite user-provided `canceled`)
  for (const auto& manual_compaction : manual_compaction_dequeue_) {
    manual_compaction->canceled = true;
  }

  // Wake up manual compactions waiting to start.
  bg_cv_.SignalAll();

  // Wait for any pending manual compactions to finish (typically through
  // failing with `Status::Incomplete`) prior to returning. This way we are
  // guaranteed no pending manual compaction will commit while manual
  // compactions are "disabled".
  while (HasPendingManualCompaction()) {
    bg_cv_.Wait();
  }
}

// NOTE: In contrast to DisableManualCompaction(), calling
// EnableManualCompaction() does NOT overwrite the user-provided *canceled
// variable to be false since there is NO CHANCE a canceled compaction
// is uncanceled. In other words, a canceled compaction must have been
// dropped out of the manual compaction queue, when we disable it.
void DBImpl::EnableManualCompaction() {
  InstrumentedMutexLock l(&mutex_);
  assert(manual_compaction_paused_ > 0);
  manual_compaction_paused_.fetch_sub(1, std::memory_order_release);
}

bool DBImpl::IsCompactionAborted(ColumnFamilyData* cfd) const {
  return compaction_aborted_.load(std::memory_order_acquire) > 0 ||
         cfd->compaction_aborted() > 0;
}

bool DBImpl::MustWaitForCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  // num_in_flight_compactions_ and the picker's compactions_in_progress_ set
  // cover DIFFERENT windows and neither subsumes the other:
  //  - the counter covers a picked Compaction from pick through the end of
  //    BackgroundCompaction (including NotifyOnCompactionCompleted), which the
  //    picker set does not -- it is cleared during Install's manifest write.
  //  - the picker set covers the bottom-priority handoff: when a LOW thread
  //    forwards a bottom compaction, its EndInFlightCompaction() dips the
  //    counter to 0 while the compaction is still registered and queued for the
  //    BOTTOM pool, and the picker set keeps AbortCompactions() waiting across
  //    that gap. (The re-registered "intended" bottom compaction inherits the
  //    original compaction_reason(), so it is not filtered out below.)
  if (cfd->num_in_flight_compactions(&mutex_) > 0) {
    return true;
  }
  for (const auto* compaction :
       *cfd->compaction_picker()->compactions_in_progress()) {
    const auto reason = compaction->compaction_reason();
    // These are picker reservations for conflict tracking, not CompactionJobs,
    // so they never observe the abort atomic in ProcessKeyValue().
    if (reason != CompactionReason::kExternalSstIngestion &&
        reason != CompactionReason::kRefitLevel) {
      return true;
    }
  }
  return false;
}

void DBImpl::BeginInFlightCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  cfd->Ref();
  cfd->inc_num_in_flight_compactions(&mutex_);
}

bool DBImpl::EndInFlightCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  cfd->dec_num_in_flight_compactions(&mutex_);
  return cfd->UnrefAndTryDelete();
}

void DBImpl::AbortAllCompactions() {
  InstrumentedMutexLock l(&mutex_);

  // Increment the abort counter to signal all compactions to abort
  compaction_aborted_.fetch_add(1, std::memory_order_release);

  TEST_SYNC_POINT("DBImpl::AbortAllCompactions:FlagSet");

  // Mark all manual compactions as canceled
  for (const auto& manual_compaction : manual_compaction_dequeue_) {
    manual_compaction->canceled = true;
  }

  // Wake up any waiting compaction threads to check the abort signal
  bg_cv_.SignalAll();

  // Wait for all running compactions (both manual and automatic) to finish
  // or abort before returning.
  // Note: bg_cv_.Wait() releases the mutex while waiting, so other threads
  // can make progress and signal when compactions complete.
  while (CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0 ||
         HasPendingManualCompaction()) {
    TEST_SYNC_POINT("DBImpl::AbortAllCompactions:Wait");
    bg_cv_.Wait();
  }
}

void DBImpl::AbortCompactions(ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  assert(column_family != nullptr);
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh->db() == this);
  auto* cfd = cfh->cfd();
  assert(cfd != nullptr);

  cfd->inc_compaction_aborted(&mutex_);
  TEST_SYNC_POINT("DBImpl::AbortCompactions:FlagSet");

  for (const auto* manual_compaction : manual_compaction_dequeue_) {
    if (manual_compaction->cfd == cfd) {
      manual_compaction->canceled.store(true, std::memory_order_release);
    }
  }
  bg_cv_.SignalAll();

  // Wait for this CF's running compactions to finish or abort. If the DB
  // is shutting down, return early -- the counter stays incremented but the
  // CFD and DB will be torn down anyway. Callers that use SCOPE_EXIT for
  // the matching ResumeCompactions() are safe: the resume is a no-op on a
  // DB that is already closed.
  ++per_cf_compaction_abort_waiters_;
  while (!shutting_down_.load(std::memory_order_acquire) &&
         (MustWaitForCompaction(cfd) || HasPendingManualCompaction(cfd))) {
    TEST_SYNC_POINT("DBImpl::AbortCompactions:Waiting");
    bg_cv_.Wait();
  }
  --per_cf_compaction_abort_waiters_;
  TEST_SYNC_POINT("DBImpl::AbortCompactions:Complete");
}

void DBImpl::ResumeCompactions(ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    return;
  }

  assert(column_family != nullptr);
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(column_family);
  assert(cfh->db() == this);
  auto* cfd = cfh->cfd();
  assert(cfd != nullptr);

  const int before = cfd->compaction_aborted();
  if (before <= 0) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "ResumeCompactions called without prior AbortCompactions "
                   "for column family %s (counter=%d)",
                   cfd->GetName().c_str(), before);
    return;
  }

  const int current = cfd->dec_compaction_aborted(&mutex_);
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "[%s] ResumeCompactions: counter %d -> %d",
                 cfd->GetName().c_str(), before, current);
  if (current == 0) {
    // A compaction aborted after it was picked rescores the column family
    // when it releases its inputs, but does not queue it.
    if (!RestoreParkedCompaction(cfd) && !cfd->IsDropped()) {
      EnqueuePendingCompaction(cfd);
    }
    MaybeScheduleFlushOrCompaction();
    bg_cv_.SignalAll();
  }
}

void DBImpl::ResumeAllCompactions() {
  InstrumentedMutexLock l(&mutex_);
  int before = compaction_aborted_.load(std::memory_order_acquire);

  // Guard against calling Resume without prior Abort
  if (before <= 0) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "ResumeAllCompactions called without prior "
                   "AbortAllCompactions (counter=%d)",
                   before);
    return;
  }

  // Decrement the abort counter
  compaction_aborted_.fetch_sub(1, std::memory_order_release);

  // As the operation is executed under db mutex, we could just use before value
  // to calculate the current value.
  int current = before - 1;
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "ResumeAllCompactions: counter %d -> %d", before, current);

  // If this is the last resume call (abort counter back to 0), schedule
  // compactions that may have been waiting
  if (current == 0) {
    // A compaction aborted after it was picked rescores its column family
    // when it releases its inputs, but does not queue it. Queue every column
    // family that needs compaction, except those with their own abort still
    // active: ResumeCompactions() queues them.
    // Read-only and secondary DBs never schedule compactions, so queuing
    // here would make WaitForCompact() wait forever.
    if (opened_successfully_ && !read_only_ &&
        !shutting_down_.load(std::memory_order_acquire)) {
      for (ColumnFamilyData* cfd : *versions_->GetColumnFamilySet()) {
        if (!cfd->IsDropped() && cfd->initialized() &&
            cfd->compaction_aborted() == 0) {
          EnqueuePendingCompaction(cfd);
        }
      }
    }
    MaybeScheduleFlushOrCompaction();
  }
}

void DBImpl::MaybeScheduleFlushOrCompaction() {
  mutex_.AssertHeld();
  TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Start");
  if (!opened_successfully_ || read_only_) {
    // Compaction may introduce data race to DB open
    return;
  }
  if (bg_work_paused_ > 0) {
    // we paused the background work
    return;
  } else if (error_handler_.IsBGWorkStopped() &&
             !error_handler_.IsRecoveryInProgress()) {
    // Recovery must be able to schedule its flushes while background work is
    // stopped. New non-recovery requests are rejected by EnqueuePendingFlush,
    // and BackgroundFlush drops any request queued before the error.
    return;
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB is being deleted; no more background compactions
    return;
  }
  auto bg_job_limits = GetBGJobLimits();
  bool is_flush_pool_empty =
      env_->GetBackgroundThreads(Env::Priority::HIGH) == 0;
  while (!is_flush_pool_empty && unscheduled_flushes_ > 0 &&
         bg_flush_scheduled_ < bg_job_limits.max_flushes) {
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::MaybeScheduleFlushOrCompaction:BeforeSchedule",
        &unscheduled_flushes_);
    bg_flush_scheduled_++;
    FlushThreadArg* fta = new FlushThreadArg;
    fta->db_ = this;
    fta->thread_pri_ = Env::Priority::HIGH;
    env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::HIGH, this,
                   &DBImpl::UnscheduleFlushCallback);
    --unscheduled_flushes_;
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::MaybeScheduleFlushOrCompaction:AfterSchedule:0",
        &unscheduled_flushes_);
  }

  // special case -- if high-pri (flush) thread pool is empty, then schedule
  // flushes in low-pri (compaction) thread pool.
  if (is_flush_pool_empty) {
    while (unscheduled_flushes_ > 0 &&
           bg_flush_scheduled_ + bg_compaction_scheduled_ <
               bg_job_limits.max_flushes) {
      bg_flush_scheduled_++;
      FlushThreadArg* fta = new FlushThreadArg;
      fta->db_ = this;
      fta->thread_pri_ = Env::Priority::LOW;
      env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::LOW, this,
                     &DBImpl::UnscheduleFlushCallback);
      --unscheduled_flushes_;
    }
  }

  if (bg_compaction_paused_ > 0) {
    // we paused the background compaction
    return;
  } else if (compaction_aborted_.load(std::memory_order_acquire) > 0) {
    // we are aborting all compactions
    return;
  } else if (error_handler_.IsBGWorkStopped()) {
    // Compaction is not part of the recovery sequence from a hard error. We
    // might get here because recovery might do a flush and install a new
    // super version, which will try to schedule pending compactions. Bail
    // out here and let the higher level recovery handle compactions
    return;
  }

  if (HasExclusiveManualCompaction()) {
    // only manual compactions are allowed to run. don't schedule automatic
    // compactions
    TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Conflict");
    return;
  }

  while ((UseSeparateRemoteCompactionPool() ||
          LocalCompactionsScheduled() < bg_job_limits.max_compactions) &&
         unscheduled_compactions_ > 0) {
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->compaction_pri_ = Env::Priority::LOW;
    ca->prepicked_compaction = nullptr;
    Status schedule_status = ScheduleCompaction(ca, this);
    if (!schedule_status.ok()) {
      error_handler_.SetBGError(schedule_status,
                                BackgroundErrorReason::kCompaction);
      bg_cv_.SignalAll();
      break;
    }
    unscheduled_compactions_--;
  }
}

class DBImpl::RemoteCompactionCompletionGuard {
 public:
  explicit RemoteCompactionCompletionGuard(DBImpl& db) : db_(db) {}

  ~RemoteCompactionCompletionGuard() {
    db_.remote_compaction_pool_->CompleteJob();
    InstrumentedMutexLock completed(&db_.mutex_);
    --db_.remote_compaction_pool_jobs_;
    db_.bg_cv_.SignalAll();
  }

  RemoteCompactionCompletionGuard(const RemoteCompactionCompletionGuard&) =
      delete;
  RemoteCompactionCompletionGuard& operator=(
      const RemoteCompactionCompletionGuard&) = delete;
  RemoteCompactionCompletionGuard(RemoteCompactionCompletionGuard&&) = delete;
  RemoteCompactionCompletionGuard& operator=(
      RemoteCompactionCompletionGuard&&) = delete;

 private:
  DBImpl& db_;
};

void DBImpl::ScheduleRemoteCompaction(std::function<void()> work) {
  InstrumentedMutexLock lock(&mutex_);
  ScheduleRemoteCompactionLocked(std::move(work));
}

void DBImpl::ScheduleRemoteCompactionLocked(std::function<void()> work) {
  mutex_.AssertHeld();
  if (!remote_compaction_pool_) {
    remote_compaction_pool_ = RemoteCompactionThreadPool::GetShared();
  }
#ifndef NDEBUG
  std::function<void()> before_submit;
  TEST_SYNC_POINT_CALLBACK("DBImpl::ScheduleRemoteCompaction:BeforeSubmit",
                           &before_submit);
  if (before_submit) {
    before_submit();
  }
#endif
  remote_compaction_pool_->SubmitJob(
      [this, work = std::move(work)] {
        RemoteCompactionCompletionGuard completed(*this);
        work();
      },
      this, RemoteCompactionPoolBaseline());
  ++remote_compaction_pool_jobs_;
}

int DBImpl::RemoteCompactionPoolBaseline() const {
  mutex_.AssertHeld();
  return std::max(1, env_->GetBackgroundThreads(Env::Priority::LOW) +
                         env_->GetBackgroundThreads(Env::Priority::BOTTOM));
}

void DBImpl::ShrinkRemoteCompactionPool() {
  InstrumentedMutexLock lock(&mutex_);
  if (remote_compaction_pool_) {
    remote_compaction_pool_->Shrink(this, UseSeparateRemoteCompactionPool()
                                              ? RemoteCompactionPoolBaseline()
                                              : 0);
  }
}

void DBImpl::ReleaseRemoteCompactionPool() {
  if (remote_compaction_pool_) {
    remote_compaction_pool_->RemoveClient(this);
    remote_compaction_pool_.reset();
  }
}

Status DBImpl::ScheduleCompaction(CompactionArg* arg, void* tag) {
  mutex_.AssertHeld();
  const Env::Priority priority = arg->compaction_pri_;
  const bool separate_remote_pool = UseSeparateRemoteCompactionPool();
  void (*worker)(void*) = priority == Env::Priority::BOTTOM
                              ? &DBImpl::BGWorkBottomCompaction
                              : &DBImpl::BGWorkCompaction;
  arg->separate_remote_pool = separate_remote_pool;
  try {
    if (separate_remote_pool) {
      ScheduleRemoteCompactionLocked([worker, arg] { worker(arg); });
    } else {
      env_->Schedule(worker, arg, priority, tag,
                     &DBImpl::UnscheduleCompactionCallback);
    }
  } catch (...) {
    Status status = Status::Aborted();
    std::unique_ptr<CompactionArg> rejected(arg);
    std::unique_ptr<PrepickedCompaction> prepicked(arg->prepicked_compaction);
    if (prepicked && prepicked->compaction) {
      std::unique_ptr<Compaction> compaction(prepicked->compaction);
      compaction->ReleaseCompactionFiles(status);
    }
    return status;
  }
  if (separate_remote_pool) {
    ++remote_coordinators_scheduled_;
  } else if (priority == Env::Priority::BOTTOM) {
    ++bg_bottom_compaction_scheduled_;
  } else {
    ++bg_compaction_scheduled_;
  }
  return Status::OK();
}

void DBImpl::ConfigureCompactionExecutors(CompactionJob* job,
                                          ColumnFamilyData* cfd,
                                          const std::atomic<bool>& canceled,
                                          bool is_manual,
                                          bool separate_remote_pool) {
  if (!separate_remote_pool) {
    return;
  }
  job->SetCompactionExecutors(
      [this](std::function<void()> work) {
        ScheduleRemoteCompaction(std::move(work));
      },
      [this, cfd, &canceled, is_manual](std::function<void()> work,
                                        Env::Priority priority) {
        return RunLocalCompaction(std::move(work), priority, cfd, canceled,
                                  is_manual);
      });
}

Status DBImpl::LocalCompactionStatus(ColumnFamilyData* cfd,
                                     const std::atomic<bool>& canceled) {
  mutex_.AssertHeld();
  if (shutting_down_.load(std::memory_order_acquire)) {
    return Status::ShutdownInProgress();
  }
  if (IsCompactionAborted(cfd)) {
    return Status::Incomplete(Status::SubCode::kCompactionAborted);
  }
  if (canceled.load(std::memory_order_acquire)) {
    return Status::Incomplete(Status::SubCode::kManualCompactionPaused);
  }
  if (cfd->IsDropped()) {
    return Status::ColumnFamilyDropped();
  }
  if (error_handler_.IsBGWorkStopped()) {
    try {
#ifndef NDEBUG
      std::function<void()> before_copy;
      TEST_SYNC_POINT_CALLBACK("DBImpl::LocalCompactionStatus:BeforeGetBGError",
                               &before_copy);
      if (before_copy) {
        before_copy();
      }
#endif
      return error_handler_.GetBGError();
    } catch (...) {
      return Status::Aborted();
    }
  }
  return Status::OK();
}

Status DBImpl::RunLocalCompaction(std::function<void()> work,
                                  Env::Priority priority, ColumnFamilyData* cfd,
                                  const std::atomic<bool>& canceled,
                                  bool is_manual) {
  if (priority == Env::Priority::USER) {
    work();
    return Status::OK();
  }
  InstrumentedMutexLock lock(&mutex_);
  constexpr uint64_t kCancellationPollMicros = 100000;
  Status status;
  while ((status = LocalCompactionStatus(cfd, canceled)).ok() &&
         LocalCompactionsScheduled() >= GetBGJobLimits().max_compactions) {
    TEST_SYNC_POINT("DBImpl::RunLocalCompaction:WaitForCredit");
    if (is_manual) {
      bg_cv_.TimedWait(immutable_db_options_.clock->NowMicros() +
                       kCancellationPollMicros);
    } else {
      bg_cv_.Wait();
    }
  }
  if (!status.ok()) {
    return status;
  }
  assert(priority == Env::Priority::LOW || priority == Env::Priority::BOTTOM);
  LocalCompactionTask task{this,     cfd,         &canceled, std::move(work),
                           priority, Status::OK()};
  bool task_submitted = false;
  Defer drain_accepted_task([this, &task, &task_submitted] {
    while (task_submitted && !task.done) {
      bg_cv_.Wait();
    }
  });
  if (priority == Env::Priority::BOTTOM) {
    ++bg_bottom_compaction_scheduled_;
  } else {
    ++bg_compaction_scheduled_;
  }
  try {
    env_->Schedule(&DBImpl::BGWorkLocalCompaction, &task, priority, &task,
                   &DBImpl::UnscheduleLocalCompaction);
  } catch (...) {
    if (priority == Env::Priority::BOTTOM) {
      --bg_bottom_compaction_scheduled_;
    } else {
      --bg_compaction_scheduled_;
    }
    bg_cv_.SignalAll();
    return Status::Aborted();
  }
  task_submitted = true;
  TEST_SYNC_POINT("DBImpl::RunLocalCompaction:Scheduled");
  bool cancellation_requested = false;
  while (!task.done) {
    if (!cancellation_requested && !LocalCompactionStatus(cfd, canceled).ok()) {
      try {
        env_->UnSchedule(&task, priority);
        cancellation_requested = true;
      } catch (...) {
        if (!task.done) {
          bg_cv_.TimedWait(immutable_db_options_.clock->NowMicros() +
                           kCancellationPollMicros);
        }
        continue;
      }
      if (task.done) {
        break;
      }
    }
    if (is_manual) {
      bg_cv_.TimedWait(immutable_db_options_.clock->NowMicros() +
                       kCancellationPollMicros);
    } else {
      bg_cv_.Wait();
    }
  }
  return task.status;
}

IOStatus DBImpl::CopyTrivialCompactionFiles(
    Compaction* c, LogBuffer* log_buffer, std::vector<FileMetaData>* out_files,
    int job_id) {
  IOStatus io_s;
  ColumnFamilyData* const cfd = c->column_family_data();
  // Output whose creation was announced to listeners but has not finished.
  std::string unfinished_out_fname;
  for (const auto& in_file : *c->inputs(0)) {
    const uint64_t out_file_number = versions_->NewFileNumber();
    const std::string in_fname =
        TableFileName(c->immutable_options().cf_paths, in_file->fd.GetNumber(),
                      in_file->fd.GetPathId());
    const std::string out_fname = TableFileName(
        c->immutable_options().cf_paths, out_file_number, c->output_path_id());

    // The output is a byte-for-byte copy of the input, so its table
    // properties are exactly the input's, including the SST unique ID.
    std::shared_ptr<const TableProperties> out_table_properties;
    const TablePropertiesCollection& input_table_properties =
        c->GetOrInitInputTableProperties();
    auto input_table_properties_it = input_table_properties.find(in_fname);
    if (input_table_properties_it != input_table_properties.end()) {
      out_table_properties = input_table_properties_it->second;
    }
    EventHelpers::NotifyTableFileCreationStarted(
        cfd->ioptions().listeners, dbname_, cfd->GetName(), out_fname, job_id,
        TableFileCreationReason::kCompaction);
    unfinished_out_fname = out_fname;

    int64_t tmp_current_time = 0;
    auto get_time_status =
        immutable_db_options_.clock->GetCurrentTime(&tmp_current_time);
    if (!get_time_status.ok()) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] WARNING: Failed to get current time %s "
                       "status=%s",
                       c->column_family_data()->GetName().c_str(),
                       get_time_status.ToString().c_str());
    }
    // The copy is a new file for this DB to hold, but has the same contents
    // (and file_creation_time) as the input
    uint64_t copy_time = static_cast<uint64_t>(tmp_current_time);

    FileOptions copied_file_options = file_options_;
    copied_file_options.temperature = c->GetOutputTemperature();
    std::unique_ptr<WritableFileWriter> dest_writer;
    {
      std::unique_ptr<FSWritableFile> dest_file;
      IOStatus writable_file_io_status =
          immutable_db_options_.fs.get()->NewWritableFile(
              out_fname, copied_file_options, &dest_file, nullptr /* dbg */);
      TEST_SYNC_POINT_CALLBACK(
          "NewWritableFile::FileOptions.temperature",
          const_cast<Temperature*>(&copied_file_options.temperature));
      if (!writable_file_io_status.ok()) {
        io_s = writable_file_io_status;
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Error: Abort trivial copy compaction, failed to open "
            "NewWritableFile %s\n"
            " out_fname=%s, temperature=%s, io_status=%s",
            c->column_family_data()->GetName().c_str(), out_fname.c_str(),
            out_fname.c_str(),
            temperature_to_string[c->GetOutputTemperature()].c_str(),
            io_s.ToString().c_str());
        break;
      }

      FileTypeSet tmp_set = immutable_db_options_.checksum_handoff_file_types;
      dest_writer.reset(new WritableFileWriter(
          std::move(dest_file), out_fname, copied_file_options,
          immutable_db_options_.clock, io_tracer_, immutable_db_options_.stats,
          Histograms::SST_WRITE_MICROS, c->immutable_options().listeners,
          immutable_db_options_.file_checksum_gen_factory.get(),
          tmp_set.Contains(FileType::kTableFile),
          tmp_set.Contains(FileType::kTableFile)));
    }

    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Started copying from: %s\n"
        " temperature=%s, to: %s, temperature=%s, buffer_size=%" PRIu64,
        c->column_family_data()->GetName().c_str(), in_fname.c_str(),
        temperature_to_string[in_file->temperature].c_str(), out_fname.c_str(),
        temperature_to_string[c->GetOutputTemperature()].c_str(),
        c->mutable_cf_options()
            .compaction_options_fifo.trivial_copy_buffer_size);
    // Add IO_LOW HINT for compaction
    IOOptions copy_files_compaction_io_options;
    copy_files_compaction_io_options.rate_limiter_priority =
        Env::IOPriority::IO_LOW;
    copy_files_compaction_io_options.type = IOType::kData;
    copy_files_compaction_io_options.io_activity = Env::IOActivity::kCompaction;

    TEST_SYNC_POINT("DBImpl::CopyTrivialCompactionFiles:BeforeCopyFile");
    IOStatus copy_file_io_status = CopyFile(
        immutable_db_options_.fs.get() /* fileSystem */, in_fname /* source */,
        in_file->temperature /* src_temp_hint */, dest_writer /* dest_writer */,
        0 /* size */, true /* use_fsync */, io_tracer_ /* io_tracer*/,
        c->mutable_cf_options()
            .compaction_options_fifo
            .trivial_copy_buffer_size /* max_read_buffer_size
                                       */
        ,
        copy_files_compaction_io_options /* readIOOptions */,
        copy_files_compaction_io_options /* writeIOOptions */);
    if (dest_writer) {
      IOOptions close_files_compaction_io_options;
      close_files_compaction_io_options.rate_limiter_priority =
          Env::IOPriority::IO_LOW;
      close_files_compaction_io_options.type = IOType::kData;
      close_files_compaction_io_options.io_activity =
          Env::IOActivity::kCompaction;
      // Close the dest_write
      io_s = dest_writer->Close(close_files_compaction_io_options);
      if (!io_s.ok()) {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Failed to close the writer. Failed to copy from: %s\n"
            " temperature=%s, to=%s, temperature=%s, io_status=%s",
            c->column_family_data()->GetName().c_str(), in_fname.c_str(),
            temperature_to_string[in_file->temperature].c_str(),
            out_fname.c_str(),
            temperature_to_string[c->GetOutputTemperature()].c_str(),
            io_s.ToString().c_str());
        break;
      }
    }

    io_s = copy_file_io_status;

    if (!io_s.ok()) {
      ROCKS_LOG_BUFFER(log_buffer,
                       "[%s] Failed to copy from: %s\n"
                       " temperature=%s, to=%s, temperature=%s, io_status=%s",
                       c->column_family_data()->GetName().c_str(),
                       in_fname.c_str(),
                       temperature_to_string[in_file->temperature].c_str(),
                       out_fname.c_str(),
                       temperature_to_string[c->GetOutputTemperature()].c_str(),
                       io_s.ToString().c_str());
      break;
    }
    ROCKS_LOG_BUFFER(
        log_buffer,
        "[%s] Successfully copying from: %s\n"
        " temperature=%s, to=%s, temperature=%s, io_status=%s",
        c->column_family_data()->GetName().c_str(), in_fname.c_str(),
        temperature_to_string[in_file->temperature].c_str(), out_fname.c_str(),
        temperature_to_string[c->GetOutputTemperature()].c_str(),
        io_s.ToString().c_str());

    FileMetaData out_file_metadata{out_file_number,
                                   c->output_path_id(),
                                   in_file->fd.GetFileSize(),
                                   in_file->smallest,
                                   in_file->largest,
                                   in_file->fd.smallest_seqno,
                                   in_file->fd.largest_seqno,
                                   false /* marked_for_compact */,
                                   c->GetOutputTemperature() /* temperature */,
                                   in_file->oldest_blob_file_number,
                                   in_file->oldest_ancestor_time,
                                   in_file->file_creation_time,
                                   c->MinInputFileEpochNumber(),
                                   dest_writer->GetFileChecksum(),
                                   dest_writer->GetFileChecksumFuncName(),
                                   in_file->unique_id,
                                   in_file->compensated_range_deletion_size,
                                   in_file->tail_size,
                                   in_file->user_defined_timestamps_persisted,
                                   in_file->min_timestamp,
                                   in_file->max_timestamp};
    out_file_metadata.db_ownership_start_time = copy_time;
    out_file_metadata.data_file_provenance =
        in_file->data_file_provenance | kProvenanceTemperatureChangedByCopy;
    out_file_metadata.blob_reference_descriptor =
        in_file->blob_reference_descriptor;
    out_file_metadata.blob_reference_legacy_metadata =
        in_file->blob_reference_legacy_metadata;

    if (out_table_properties) {
      c->SetOutputTableProperties(out_fname, out_table_properties);
    }
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        &event_logger_, cfd->ioptions().listeners, dbname_, cfd->GetName(),
        out_fname, job_id, out_file_metadata.fd,
        out_file_metadata.oldest_blob_file_number,
        out_table_properties ? *out_table_properties : TableProperties(),
        TableFileCreationReason::kCompaction, io_s,
        out_file_metadata.file_checksum,
        out_file_metadata.file_checksum_func_name,
        /*blob_references=*/nullptr);
    unfinished_out_fname.clear();

    out_files->push_back(std::move(out_file_metadata));
  }

  if (!unfinished_out_fname.empty()) {
    assert(!io_s.ok());
    EventHelpers::LogAndNotifyTableFileCreationFinished(
        &event_logger_, cfd->ioptions().listeners, dbname_, cfd->GetName(),
        unfinished_out_fname, job_id, FileDescriptor(), kInvalidBlobFileNumber,
        TableProperties(), TableFileCreationReason::kCompaction, io_s,
        kUnknownFileChecksum, kUnknownFileChecksumFuncName,
        /*blob_references=*/nullptr);
  }
  return io_s;
}

void DBImpl::BGWorkLocalCompaction(void* arg) {
  auto* task = static_cast<LocalCompactionTask*>(arg);
  DBImpl* db = task->db;
  const int priority_index = task->priority == Env::Priority::BOTTOM ? 0 : 1;
  IOSTATS_SET_THREAD_POOL_ID(task->priority);
  db->mutex_.Lock();
  ++db->local_compactions_running_by_priority_[priority_index];
  TEST_SYNC_POINT("DBImpl::BGWorkLocalCompaction:BeforeStatus");
  task->status = db->LocalCompactionStatus(task->cfd, *task->canceled);
  db->mutex_.Unlock();
  if (task->status.ok()) {
    TEST_SYNC_POINT("DBImpl::BGWorkLocalCompaction:Start");
    try {
      task->work();
    } catch (...) {
      task->status = Status::Aborted();
    }
    TEST_SYNC_POINT("DBImpl::BGWorkLocalCompaction:Finished");
  }
  InstrumentedMutexLock lock(&db->mutex_);
  if (task->priority == Env::Priority::BOTTOM) {
    --db->bg_bottom_compaction_scheduled_;
  } else {
    --db->bg_compaction_scheduled_;
  }
  --db->local_compactions_running_by_priority_[priority_index];
  task->done = true;
  db->MaybeScheduleFlushOrCompaction();
  db->bg_cv_.SignalAll();
}

void DBImpl::UnscheduleLocalCompaction(void* arg) {
  auto* task = static_cast<LocalCompactionTask*>(arg);
  task->db->mutex_.AssertHeld();
  task->status = task->db->LocalCompactionStatus(task->cfd, *task->canceled);
  if (task->status.ok()) {
    task->status = Status::ShutdownInProgress();
  }
  if (task->priority == Env::Priority::BOTTOM) {
    --task->db->bg_bottom_compaction_scheduled_;
  } else {
    --task->db->bg_compaction_scheduled_;
  }
  task->done = true;
  task->db->MaybeScheduleFlushOrCompaction();
  task->db->bg_cv_.SignalAll();
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits() const {
  mutex_.AssertHeld();
  return GetBGJobLimits(mutable_db_options_.max_background_flushes,
                        mutable_db_options_.max_background_compactions,
                        mutable_db_options_.max_background_jobs,
                        write_controller_.NeedSpeedupCompaction());
}

DBImpl::BGJobLimits DBImpl::GetBGJobLimits(int max_background_flushes,
                                           int max_background_compactions,
                                           int max_background_jobs,
                                           bool parallelize_compactions) {
  BGJobLimits res{};
  if (max_background_flushes == -1 && max_background_compactions == -1) {
    // for our first stab implementing max_background_jobs, simply allocate a
    // quarter of the threads to flushes.
    res.max_flushes = std::max(1, max_background_jobs / 4);
    res.max_compactions = std::max(1, max_background_jobs - res.max_flushes);
  } else {
    // compatibility code in case users haven't migrated to max_background_jobs,
    // which automatically computes flush/compaction limits
    res.max_flushes = std::max(1, max_background_flushes);
    res.max_compactions = std::max(1, max_background_compactions);
  }
  if (!parallelize_compactions) {
    // throttle background compactions until we deem necessary
    res.max_compactions = 1;
  }
  return res;
}

BackgroundJobPressure DBImpl::CaptureBackgroundJobPressure() const {
  mutex_.AssertHeld();

  BackgroundJobPressure snapshot;

  snapshot.compaction_scheduled = CompactionsScheduled();
  snapshot.compaction_running = num_running_compactions_;

  // Per-priority breakdown for pool-specific expansion
  assert(num_running_compactions_ >= num_running_bottom_compactions_);
  snapshot.compaction_low_scheduled = bg_compaction_scheduled_;
  snapshot.compaction_low_running = local_compactions_running_by_priority_[1];
  snapshot.compaction_bottom_scheduled = bg_bottom_compaction_scheduled_;
  snapshot.compaction_bottom_running =
      local_compactions_running_by_priority_[0];

  // Flush
  snapshot.flush_scheduled = bg_flush_scheduled_;
  snapshot.flush_running = num_running_flushes_;

  snapshot.compaction_speedup_active =
      write_controller_.NeedSpeedupCompaction();

  // Compute write_stall_proximity_pct: max across all CFs.
  // Uses the same inputs as RecalculateWriteStallConditions() in
  // column_family.cc:
  //   - l0_delay_trigger_count(): sorted run count used for write stall
  //     decisions. Equals the L0 sorted run count for level compaction (see
  //     VersionStorageInfo::NumL0SortedRuns()); for universal compaction, also
  //     counts non-empty levels from L1+ as sorted runs.
  //   - estimated_compaction_needed_bytes(): pending compaction bytes.
  // Prefers slowdown/soft triggers (gradual); falls back to stop/hard triggers.
  // level0_slowdown_writes_trigger is sanitized to always be > 0;
  // soft_pending_compaction_bytes_limit can be 0 (disabled).
  int max_proximity = 0;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    const auto* vstorage = cfd->current()->storage_info();
    const auto& mutable_cf_options = cfd->GetCurrentMutableCFOptions();

    int l0_proximity = 0;
    int l0_trigger = mutable_cf_options.level0_slowdown_writes_trigger > 0
                         ? mutable_cf_options.level0_slowdown_writes_trigger
                         : mutable_cf_options.level0_stop_writes_trigger;
    if (l0_trigger > 0) {
      l0_proximity = static_cast<int>(
          100.0 * vstorage->l0_delay_trigger_count() / l0_trigger);
    }

    int bytes_proximity = 0;
    uint64_t bytes_limit =
        mutable_cf_options.soft_pending_compaction_bytes_limit > 0
            ? mutable_cf_options.soft_pending_compaction_bytes_limit
            : mutable_cf_options.hard_pending_compaction_bytes_limit;
    if (bytes_limit > 0) {
      bytes_proximity = static_cast<int>(
          100.0 *
          static_cast<double>(vstorage->estimated_compaction_needed_bytes()) /
          static_cast<double>(bytes_limit));
    }

    max_proximity =
        std::max(max_proximity, std::max(l0_proximity, bytes_proximity));
  }
  snapshot.write_stall_proximity_pct = max_proximity;

  return snapshot;
}

void DBImpl::NotifyOnBackgroundJobPressureChanged() {
  mutex_.AssertHeld();
  if (immutable_db_options_.listeners.empty()) {
    return;
  }
  BackgroundJobPressure pressure = CaptureBackgroundJobPressure();
  bg_pressure_callback_in_progress_++;
  mutex_.Unlock();
  for (const auto& listener : immutable_db_options_.listeners) {
    listener->OnBackgroundJobPressureChanged(this, pressure);
  }
  mutex_.Lock();
  bg_pressure_callback_in_progress_--;
}

void DBImpl::AddToCompactionQueue(ColumnFamilyData* cfd) {
  assert(!cfd->queued_for_compaction());
  cfd->Ref();
  compaction_queue_.push_back(cfd);
  cfd->set_queued_for_compaction(true);
  ++unscheduled_compactions_;
}

bool DBImpl::RestoreParkedCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  auto it = parked_compaction_cfds_.find(cfd);
  if (it == parked_compaction_cfds_.end()) {
    return false;
  }

  parked_compaction_cfds_.erase(it);
  assert(cfd->queued_for_compaction());
  if (!cfd->IsDropped() && cfd->NeedsCompaction()) {
    TEST_SYNC_POINT_CALLBACK("DBImpl::RestoreParkedCompaction:cfd",
                             static_cast<void*>(cfd));
    compaction_queue_.push_back(cfd);
    ++unscheduled_compactions_;
  } else {
    cfd->set_queued_for_compaction(false);
    cfd->UnrefAndTryDelete();
  }
  return true;
}

ColumnFamilyData* DBImpl::PopFirstFromCompactionQueue() {
  assert(!compaction_queue_.empty());
  auto cfd = *compaction_queue_.begin();
  compaction_queue_.pop_front();
  assert(cfd->queued_for_compaction());
  cfd->set_queued_for_compaction(false);
  return cfd;
}

DBImpl::FlushRequest DBImpl::PopFirstFromFlushQueue() {
  assert(!flush_queue_.empty());
  FlushRequest flush_req = std::move(flush_queue_.front());
  flush_queue_.pop_front();
  if (!flush_req.atomic_flush) {
    assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
  }
  for (const auto& elem : flush_req.cfd_to_max_mem_id_to_persist) {
    if (!flush_req.atomic_flush) {
      ColumnFamilyData* cfd = elem.first;
      assert(cfd);
      assert(cfd->queued_for_flush());
      cfd->set_queued_for_flush(false);
    }
  }
  return flush_req;
}

DBImpl::CompactionQueuePickResult DBImpl::PickCompactionFromQueue(
    std::unique_ptr<TaskLimiterToken>* token, LogBuffer* log_buffer) {
  assert(!compaction_queue_.empty());
  assert(*token == nullptr);
  autovector<ColumnFamilyData*> throttled_candidates;
  CompactionQueuePickResult result = CompactionQueueThrottled{};
  while (!compaction_queue_.empty()) {
    auto first_cfd = *compaction_queue_.begin();
    compaction_queue_.pop_front();
    assert(first_cfd->queued_for_compaction());
    if (first_cfd->IsDropped()) {
      first_cfd->set_queued_for_compaction(false);
      result = CompactionQueuePicked{first_cfd};
      break;
    }
    if (first_cfd->compaction_aborted() > 0) {
      // Parking consumes this worker's credit. Remaining queue entries retain
      // their own credits and are scheduled when this worker returns.
      [[maybe_unused]] const bool inserted =
          parked_compaction_cfds_.insert(first_cfd).second;
      assert(inserted);
      TEST_SYNC_POINT("DBImpl::PickCompactionFromQueue:Parked");
      result = CompactionQueueParked{};
      break;
    }
    if (!RequestCompactionToken(first_cfd, false, token, log_buffer)) {
      throttled_candidates.push_back(first_cfd);
      continue;
    }
    first_cfd->set_queued_for_compaction(false);
    result = CompactionQueuePicked{first_cfd};
    break;
  }
  // Add throttled compaction candidates back to queue in the original order.
  for (auto iter = throttled_candidates.rbegin();
       iter != throttled_candidates.rend(); ++iter) {
    compaction_queue_.push_front(*iter);
  }
  return result;
}

bool DBImpl::EnqueuePendingFlush(const FlushRequest& flush_req) {
  mutex_.AssertHeld();
  bool enqueued = false;
  if (reject_new_background_jobs_) {
    return enqueued;
  }
  if (error_handler_.IsBGWorkStopped() &&
      !IsRecoveryFlush(flush_req.flush_reason)) {
    return enqueued;
  }
  if (flush_req.cfd_to_max_mem_id_to_persist.empty()) {
    return enqueued;
  }
  if (!flush_req.atomic_flush) {
    // For the non-atomic flush case, we never schedule multiple column
    // families in the same flush request.
    assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
    ColumnFamilyData* cfd =
        flush_req.cfd_to_max_mem_id_to_persist.begin()->first;
    assert(cfd);

    if (!cfd->queued_for_flush() && cfd->imm()->IsFlushPending()) {
      cfd->Ref();
      cfd->set_queued_for_flush(true);
      ++unscheduled_flushes_;
      flush_queue_.push_back(flush_req);
      enqueued = true;
    }
  } else {
    if (flush_req.flush_reason == FlushReason::kWriteBufferManager &&
        write_buffer_manager_->ShouldTrackFlushInitiator()) {
      for (const FlushRequest& queued : flush_queue_) {
        if (!queued.atomic_flush) {
          continue;
        }
        bool covered = true;
        for (const auto& requested : flush_req.cfd_to_max_mem_id_to_persist) {
          const auto queued_entry =
              queued.cfd_to_max_mem_id_to_persist.find(requested.first);
          if (queued_entry == queued.cfd_to_max_mem_id_to_persist.end() ||
              queued_entry->second < requested.second) {
            covered = false;
            break;
          }
        }
        if (covered) {
          TEST_SYNC_POINT(
              "DBImpl::EnqueuePendingFlush:AtomicWBMRequestDeduplicated");
          return false;
        }
      }
    }
    // Atomic flush requests normally bypass the per-CF queued_for_flush()
    // guard. Cross-DB WBM requests are deduplicated above because its
    // autonomous retry loop must not repeatedly enqueue the same atomic cut
    // while HIGH workers are unavailable. Other atomic requests retain their
    // existing behavior.
    for (auto& iter : flush_req.cfd_to_max_mem_id_to_persist) {
      ColumnFamilyData* cfd = iter.first;
      cfd->Ref();
    }
    ++unscheduled_flushes_;
    flush_queue_.push_back(flush_req);
    RecordAtomicFlushRequestReason(stats_, flush_req.flush_reason);
    enqueued = true;
  }
  return enqueued;
}

void DBImpl::EnqueuePendingCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  if (reject_new_background_jobs_ || cfd->IsDropped()) {
    return;
  }
  if (!cfd->queued_for_compaction() && cfd->NeedsCompaction()) {
    TEST_SYNC_POINT_CALLBACK("EnqueuePendingCompaction::cfd",
                             static_cast<void*>(cfd));
    if (cfd->compaction_aborted() > 0) {
      cfd->Ref();
      cfd->set_queued_for_compaction(true);
      [[maybe_unused]] const bool inserted =
          parked_compaction_cfds_.insert(cfd).second;
      assert(inserted);
    } else {
      AddToCompactionQueue(cfd);
    }
  }
}

void DBImpl::RescoreAndEnqueuePendingCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  // Without this, a column family that stopped writes because of too many L0
  // files can keep a stale low score: with writes stopped there are no
  // flushes, so no new version is installed to fix the score, and writes stay
  // stopped.
  cfd->current()->storage_info()->ComputeCompactionScore(
      cfd->ioptions(), cfd->GetLatestMutableCFOptions(),
      cfd->GetFullHistoryTsLow());
  // While compactions are aborted, ResumeCompactions() or
  // ResumeAllCompactions() queues the column family instead. Queuing it here
  // would make WaitForCompact() wait until compactions are resumed.
  if (!IsCompactionAborted(cfd)) {
    EnqueuePendingCompaction(cfd);
  }
}

void DBImpl::SchedulePendingPurge(std::string fname, std::string dir_to_sync,
                                  FileType type, uint64_t number, int job_id) {
  mutex_.AssertHeld();
  if (reject_new_background_jobs_) {
    return;
  }
  PurgeFileInfo file_info(std::move(fname), std::move(dir_to_sync), type,
                          number, job_id);
  purge_files_.insert({{number, std::move(file_info)}});
}

void DBImpl::BGWorkFlush(void* arg) {
  FlushThreadArg fta = *(static_cast<FlushThreadArg*>(arg));
  delete static_cast<FlushThreadArg*>(arg);

  IOSTATS_SET_THREAD_POOL_ID(fta.thread_pri_);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush");
  static_cast_with_check<DBImpl>(fta.db_)->BackgroundCallFlush(fta.thread_pri_);
  TEST_SYNC_POINT("DBImpl::BGWorkFlush:done");
}

void DBImpl::BGWorkCompaction(void* arg) {
  CompactionArg ca = *(static_cast<CompactionArg*>(arg));
  delete static_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::LOW);
  TEST_SYNC_POINT("DBImpl::BGWorkCompaction");
  auto prepicked_compaction =
      static_cast<PrepickedCompaction*>(ca.prepicked_compaction);
  static_cast_with_check<DBImpl>(ca.db)->BackgroundCallCompaction(
      prepicked_compaction, Env::Priority::LOW, ca.separate_remote_pool);
  delete prepicked_compaction;
}

void DBImpl::BGWorkBottomCompaction(void* arg) {
  CompactionArg ca = *(static_cast<CompactionArg*>(arg));
  delete static_cast<CompactionArg*>(arg);
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::BOTTOM);
  TEST_SYNC_POINT("DBImpl::BGWorkBottomCompaction");
  auto* prepicked_compaction = ca.prepicked_compaction;
  assert(prepicked_compaction && prepicked_compaction->compaction);
  ca.db->BackgroundCallCompaction(prepicked_compaction, Env::Priority::BOTTOM,
                                  ca.separate_remote_pool);
  delete prepicked_compaction;
}

void DBImpl::BGWorkPurge(void* db) {
  IOSTATS_SET_THREAD_POOL_ID(Env::Priority::HIGH);
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:start");
  static_cast<DBImpl*>(db)->BackgroundCallPurge();
  TEST_SYNC_POINT("DBImpl::BGWorkPurge:end");
}

void DBImpl::UnscheduleCompactionCallback(void* arg) {
  CompactionArg* ca_ptr = static_cast<CompactionArg*>(arg);
  Env::Priority compaction_pri = ca_ptr->compaction_pri_;
  if (Env::Priority::BOTTOM == compaction_pri) {
    // Decrement bg_bottom_compaction_scheduled_ if priority is BOTTOM
    ca_ptr->db->bg_bottom_compaction_scheduled_--;
  } else if (Env::Priority::LOW == compaction_pri) {
    // Decrement bg_compaction_scheduled_ if priority is LOW
    ca_ptr->db->bg_compaction_scheduled_--;
  }
  CompactionArg ca = *(ca_ptr);
  delete static_cast<CompactionArg*>(arg);
  if (ca.prepicked_compaction != nullptr) {
    // if it's a manual compaction, set status to ManualCompactionPaused
    if (ca.prepicked_compaction->manual_compaction_state) {
      ca.prepicked_compaction->manual_compaction_state->done = true;
      ca.prepicked_compaction->manual_compaction_state->status =
          Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    }
    if (ca.prepicked_compaction->compaction != nullptr) {
      Compaction* c = ca.prepicked_compaction->compaction;
      // The compaction still holds a reference to its column family.
      ColumnFamilyData* cfd = c->column_family_data();
      c->ReleaseCompactionFiles(
          Status::Incomplete(Status::SubCode::kManualCompactionPaused));
      // The caller holds the DB mutex.
      ca.db->RescoreAndEnqueuePendingCompaction(cfd);
      delete c;
    }
    delete ca.prepicked_compaction;
  }
  TEST_SYNC_POINT("DBImpl::UnscheduleCompactionCallback");
}

void DBImpl::UnscheduleFlushCallback(void* arg) {
  // Decrement bg_flush_scheduled_ in flush callback
  static_cast<FlushThreadArg*>(arg)->db_->bg_flush_scheduled_--;
  Env::Priority flush_pri = static_cast<FlushThreadArg*>(arg)->thread_pri_;
  if (Env::Priority::LOW == flush_pri) {
    TEST_SYNC_POINT("DBImpl::UnscheduleLowFlushCallback");
  } else if (Env::Priority::HIGH == flush_pri) {
    TEST_SYNC_POINT("DBImpl::UnscheduleHighFlushCallback");
  }
  delete static_cast<FlushThreadArg*>(arg);
  TEST_SYNC_POINT("DBImpl::UnscheduleFlushCallback");
}

void DBImpl::RefreshFlushableMemAccounting() {
  mutex_.AssertHeld();
  versions_->GetColumnFamilySet()->RefreshFlushableMemAccounting();
}

bool DBImpl::TryRefreshFlushableMemAccounting() {
  if (!mutex_.TryLock()) {
    return false;
  }
  Defer unlock_mutex([this] { mutex_.Unlock(); });
  RefreshFlushableMemAccounting();
  return wbm_flush_initiator_ != nullptr &&
         wbm_flush_initiator_->HasAccurateFlushableMemUsage();
}

DBImpl::FlushableCFs DBImpl::CollectFlushableCFs(
    bool include_waiting_immutable) {
  mutex_.AssertHeld();
  FlushableCFs result;
  SequenceNumber oldest_seq = kMaxSequenceNumber;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    if (cfd->IsDropped() || !cfd->initialized()) {
      continue;
    }
    const size_t mutable_mem =
        cfd->mem()->IsEmpty() ? 0 : cfd->mem()->WBMTrackedMemoryUsage();
    size_t waiting_immutable_mem = 0;
    if (include_waiting_immutable) {
      if (immutable_db_options_.atomic_flush) {
        waiting_immutable_mem =
            cfd->imm()->WBMTrackedUnstartedMemTablesMemoryUsage();
      } else if (!cfd->imm()->IsFlushPendingOrRunning()) {
        waiting_immutable_mem =
            cfd->imm()->WBMTrackedUnflushedMemTablesMemoryUsage();
      }
    }
    result.total_mem += mutable_mem + waiting_immutable_mem;
    // Non-atomic flushes skip CFs already flushing.
    if (cfd->imm()->IsFlushPendingOrRunning()) {
      continue;
    }
    size_t mem = mutable_mem;
    if (include_waiting_immutable) {
      mem += waiting_immutable_mem;
    }
    if (mem == 0) {
      continue;
    }
    if (result.largest == nullptr || mem > result.largest_mem) {
      result.largest = cfd;
      result.largest_mem = mem;
    }
    const SequenceNumber seq = cfd->mem()->GetCreationSeq();
    if (result.oldest == nullptr || seq < oldest_seq) {
      result.oldest = cfd;
      oldest_seq = seq;
    }
  }
  return result;
}

bool DBImpl::ScheduleWriteBufferManagerFlush() {
  {
    if (!mutex_.TryLock()) {
      return false;
    }
    Defer unlock_mutex([this] { mutex_.Unlock(); });
    // Recheck bid eligibility because DB state may have changed.
    if (!write_buffer_manager_->ShouldTrackFlushInitiator() ||
        shutdown_initiated_.load(std::memory_order_acquire) ||
        shutting_down_.load(std::memory_order_acquire) ||
        reject_new_background_jobs_ || !opened_successfully_ || read_only_ ||
        error_handler_.IsBGWorkStopped() || write_controller_.IsStopped() ||
        // Reject both the pause drain and fully paused states.
        bg_work_paused_ > 0 || bg_compaction_paused_ > 0 ||
        // Never queue work that would wait behind this DB's stall.
        WouldBlockJoiningWriteThread()) {
      return false;
    }
    // A queued or running job does not count as new progress. Returning false
    // lets the coordinator try another DB instead of queuing more work behind
    // a busy LOW pool.
    if (bg_wbm_flush_scheduled_ > 0) {
      return false;
    }
    const FlushableCFs flushable =
        CollectFlushableCFs(true /* include_waiting_immutable */);
    const bool has_flushable_cf = immutable_db_options_.atomic_flush
                                      ? flushable.total_mem > 0
                                      : flushable.largest != nullptr;
    if (!has_flushable_cf) {
      wbm_flush_initiator_->SetHasFlushableCF(false);
      write_buffer_manager_->NotifyFlushInitiatorChanged();
      return false;
    }
    wbm_flush_initiator_->SetHasFlushableCF(false);
    write_buffer_manager_->NotifyFlushInitiatorChanged();
    ++bg_wbm_flush_scheduled_;
  }

  // The WBM-owned executor provides bounded capacity independent of Env::LOW.
  // Run outside mutex_ so DB-local work cannot block other DB mutex users.
  BackgroundCallWBMFlush();
  return true;
}

void DBImpl::BackgroundCallWBMFlush() {
  const bool atomic_flush = immutable_db_options_.atomic_flush;
  ColumnFamilyData* cfd_to_flush = nullptr;
  bool has_flushable_cf = false;
  {
    InstrumentedMutexLock l(&mutex_);
    if (!shutdown_initiated_.load(std::memory_order_acquire) &&
        !shutting_down_.load(std::memory_order_acquire) &&
        !reject_new_background_jobs_ && opened_successfully_ && !read_only_ &&
        !error_handler_.IsBGWorkStopped() && !write_controller_.IsStopped() &&
        bg_work_paused_ == 0 && bg_compaction_paused_ == 0 &&
        !WouldBlockJoiningWriteThread()) {
      const FlushableCFs flushable =
          CollectFlushableCFs(true /* include_waiting_immutable */);
      has_flushable_cf =
          atomic_flush ? flushable.total_mem > 0 : flushable.largest != nullptr;
      if (!atomic_flush) {
        cfd_to_flush = flushable.largest;
        if (cfd_to_flush != nullptr) {
          cfd_to_flush->Ref();
        }
      }
    }
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallWBMFlush:AfterPick", this);

  FlushOptions flush_options;
  flush_options.wait = false;
  flush_options.allow_write_stall = true;
  bool made_progress = false;
  if (has_flushable_cf) {
    Status s;
    if (atomic_flush) {
      s = AtomicFlushMemTablesImpl(
          flush_options, FlushReason::kWriteBufferManager,
          {} /* provided_candidate_cfds */, false /* entered_write_thread */,
          WriteThreadJoinMode::kNonBlocking, &made_progress);
    } else {
      s = FlushMemTableImpl(cfd_to_flush, flush_options,
                            FlushReason::kWriteBufferManager,
                            false /* entered_write_thread */,
                            WriteThreadJoinMode::kNonBlocking, &made_progress);
    }
    if (!s.ok()) {
      ROCKS_LOG_WARN(
          immutable_db_options_.info_log,
          "Flush on behalf of a shared WriteBufferManager failed: %s",
          s.ToString().c_str());
    }
    s.PermitUncheckedError();
  }
  TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallWBMFlush:MadeProgress",
                           &made_progress);

  InstrumentedMutexLock l(&mutex_);
  if (cfd_to_flush != nullptr) {
    cfd_to_flush->UnrefAndTryDelete();
  }
  RefreshFlushableMemAccounting();
  --bg_wbm_flush_scheduled_;
  write_buffer_manager_->NotifyFlushInitiatorFlushCompleted(
      wbm_flush_initiator_.get(), made_progress);
  TEST_SYNC_POINT_CALLBACK("DBImpl::BGWorkWBMFlush:done", this);
  bg_cv_.SignalAll();
}

Status DBImpl::BackgroundFlush(bool* made_progress, JobContext* job_context,
                               LogBuffer* log_buffer, FlushReason* reason,
                               bool* flush_rescheduled_to_retain_udt,
                               Env::Priority thread_pri,
                               autovector<ColumnFamilyData*>* census_cfds) {
  mutex_.AssertHeld();

  Status status;
  *reason = FlushReason::kOthers;
  // During recovery, allow flush workers to inspect the queue. Non-recovery
  // requests are dropped below.
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    }
  } else if (!error_handler_.IsRecoveryInProgress()) {
    status = error_handler_.GetBGError();
  }

  if (!status.ok()) {
    return status;
  }

  autovector<BGFlushArg> bg_flush_args;
  std::vector<SuperVersionContext>& superversion_contexts =
      job_context->superversion_contexts;
  autovector<ColumnFamilyData*> column_families_not_to_flush;
  while (!flush_queue_.empty()) {
    // This cfd is already referenced
    FlushRequest flush_req = PopFirstFromFlushQueue();
    FlushReason flush_reason = flush_req.flush_reason;
    if (!error_handler_.GetBGError().ok() && error_handler_.IsBGWorkStopped() &&
        !IsRecoveryFlush(flush_reason)) {
      // A request queued before the error can reach a worker before recovery
      // clears the queue. Drop it; recovery rebuilds flush work for all CFs.
      status = error_handler_.GetBGError();
      assert(!status.ok());
      ROCKS_LOG_BUFFER(log_buffer,
                       "[JOB %d] Abort flush due to background error %s",
                       job_context->job_id, status.ToString().c_str());
      *reason = flush_reason;
      for (auto item : flush_req.cfd_to_max_mem_id_to_persist) {
        item.first->UnrefAndTryDelete();
      }
      return status;
    }
    if (!flush_req.atomic_flush &&
        ShouldRescheduleFlushRequestToRetainUDT(flush_req)) {
      assert(flush_req.cfd_to_max_mem_id_to_persist.size() == 1);
      ColumnFamilyData* cfd =
          flush_req.cfd_to_max_mem_id_to_persist.begin()->first;
      if (cfd->UnrefAndTryDelete()) {
        return Status::OK();
      }
      ROCKS_LOG_BUFFER(log_buffer,
                       "FlushRequest for column family %s is re-scheduled to "
                       "retain user-defined timestamps.",
                       cfd->GetName().c_str());
      // Reschedule the `FlushRequest` as is without checking dropped column
      // family etc. The follow-up job will do the check anyways, so save the
      // duplication. Column family is deduplicated by `SchdulePendingFlush` and
      // `PopFirstFromFlushQueue` contains at flush request enqueueing and
      // dequeueing time.
      // This flush request is rescheduled right after it's popped from the
      // queue while the db mutex is held, so there should be no other
      // FlushRequest for the same column family with higher `max_memtable_id`
      // in the queue to block the reschedule from succeeding.
#ifndef NDEBUG
      flush_req.reschedule_count += 1;
#endif /* !NDEBUG */
      EnqueuePendingFlush(flush_req);
      *reason = flush_reason;
      *flush_rescheduled_to_retain_udt = true;
      return Status::TryAgain();
    }
    superversion_contexts.clear();
    superversion_contexts.reserve(
        flush_req.cfd_to_max_mem_id_to_persist.size());

    for (const auto& [cfd, max_memtable_id] :
         flush_req.cfd_to_max_mem_id_to_persist) {
      if (cfd->GetMempurgeUsed()) {
        // If imm() contains silent memtables (e.g.: because
        // MemPurge was activated), requesting a flush will
        // mark the imm_needed as true.
        cfd->imm()->FlushRequested();
      }

      if (cfd->IsDropped() || !cfd->imm()->IsFlushPending()) {
        // can't flush this CF, try next one
        column_families_not_to_flush.push_back(cfd);
        continue;
      }
      superversion_contexts.emplace_back(true);
      bg_flush_args.emplace_back(cfd, max_memtable_id,
                                 &(superversion_contexts.back()), flush_reason,
                                 flush_req.atomic_flush);
    }
    // `MaybeScheduleFlushOrCompaction` schedules as many `BackgroundCallFlush`
    // jobs as the number of `FlushRequest` in the `flush_queue_`, a.k.a
    // `unscheduled_flushes_`. So it's sufficient to make each `BackgroundFlush`
    // handle one `FlushRequest` and each have a Status returned.
    if (!bg_flush_args.empty() || !column_families_not_to_flush.empty()) {
      TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundFlush:CheckFlushRequest:cb",
                               const_cast<int*>(&flush_req.reschedule_count));
      break;
    }
  }

  if (!bg_flush_args.empty()) {
    auto bg_job_limits = GetBGJobLimits();
    for (const auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "Calling FlushMemTableToOutputFile with column "
          "family [%s], flush slots available %d, compaction slots available "
          "%d, "
          "flush slots scheduled %d, compaction slots scheduled %d",
          cfd->GetName().c_str(), bg_job_limits.max_flushes,
          bg_job_limits.max_compactions, bg_flush_scheduled_,
          bg_compaction_scheduled_);
    }
    status = FlushMemTablesToOutputFiles(bg_flush_args, made_progress,
                                         job_context, log_buffer, thread_pri);
    TEST_SYNC_POINT("DBImpl::BackgroundFlush:BeforeFlush");
// All the CFD/bg_flush_arg in the FlushReq must have the same flush reason, so
// just grab the first one
#ifndef NDEBUG
    for (const auto& bg_flush_arg : bg_flush_args) {
      assert(bg_flush_arg.flush_reason_ == bg_flush_args[0].flush_reason_);
    }
#endif /* !NDEBUG */
    *reason = bg_flush_args[0].flush_reason_;
    for (auto& arg : bg_flush_args) {
      ColumnFamilyData* cfd = arg.cfd_;
      if (cfd->blob_reference_census() != nullptr && !cfd->IsDropped()) {
        // The caller applies the census of `cfd` and then drops this
        // reference. A dropped column family is released here as before, so
        // FindObsoleteFiles() in the caller still sees its last reference go.
        census_cfds->push_back(cfd);
        mutex_.AssertHeld();
        ++blob_reference_census_pending_applies_;
      } else if (cfd->UnrefAndTryDelete()) {
        arg.cfd_ = nullptr;
      }
    }
  }
  for (auto cfd : column_families_not_to_flush) {
    cfd->UnrefAndTryDelete();
  }
  return status;
}

void DBImpl::BackgroundCallFlush(Env::Priority thread_pri) {
  bool made_progress = false;
  JobContext job_context(next_job_id_.fetch_add(1), true);

  TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallFlush:start", nullptr);

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:Start:1");
  TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:Start:2");
  {
    InstrumentedMutexLock l(&mutex_);
    assert(bg_flush_scheduled_);
    num_running_flushes_++;

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs()));
    // Flushes are short, so this registers for OnFlushCompleted() from the
    // start of the job (see listener_pending_outputs_).
    std::optional<std::multiset<uint64_t>::iterator> listener_pending_output =
        CaptureListenerPendingOutput(**pending_outputs_inserted_elem);
    FlushReason reason;
    bool flush_rescheduled_to_retain_udt = false;
    // The column families of this flush that have a census, each with a
    // reference (see ApplyBlobReferenceCensusAfterInstall()).
    autovector<ColumnFamilyData*> census_cfds;
    Status s = BackgroundFlush(&made_progress, &job_context, &log_buffer,
                               &reason, &flush_rescheduled_to_retain_udt,
                               thread_pri, &census_cfds);
    if (s.IsTryAgain() && flush_rescheduled_to_retain_udt) {
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      TEST_SYNC_POINT_CALLBACK("DBImpl::AfterRetainUDTReschedule:cb", nullptr);
      immutable_db_options_.clock->SleepForMicroseconds(
          100000);  // prevent hot loop
      mutex_.Lock();
    } else if (!s.ok() && !s.IsShutdownInProgress() &&
               !s.IsColumnFamilyDropped() &&
               reason != FlushReason::kErrorRecovery) {
      // Wait a little bit before retrying background flush in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed flushes for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "[JOB %d] Waiting after background flush error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      job_context.job_id, s.ToString().c_str(), error_cnt);
      log_buffer.FlushBufferToLog();
      LogFlush(immutable_db_options_.info_log);
      immutable_db_options_.clock->SleepForMicroseconds(1000000);
      mutex_.Lock();
    }

    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FlushFinish:0");
    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);
    ReleaseListenerPendingOutput(listener_pending_output);
    // There is no need to find obsolete files if the flush job is rescheduled
    // to retain user-defined timestamps because the job doesn't get to the
    // stage of actually flushing the MemTables.
    if (!flush_rescheduled_to_retain_udt) {
      // If flush failed, we want to delete all temporary files that we might
      // have created. Thus, we force full scan in FindObsoleteFiles()
      FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                          !s.IsColumnFamilyDropped());
    }
    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:FilesFound");
      // Have to flush the info logs before bg_flush_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
      }
      job_context.Clean();
      mutex_.Lock();
    }
    TEST_SYNC_POINT("DBImpl::BackgroundCallFlush:ContextCleanedUp");
    ApplyBlobReferenceCensusAfterInstall(census_cfds);

    assert(num_running_flushes_ > 0);
    num_running_flushes_--;
    bg_flush_scheduled_--;
    if (write_buffer_manager_->ShouldTrackFlushInitiator()) {
      RefreshFlushableMemAccounting();
    }
    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();

    NotifyOnBackgroundJobPressureChanged();

    atomic_flush_install_cv_.SignalAll();
    bg_cv_.SignalAll();
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

void DBImpl::BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                      Env::Priority bg_thread_pri,
                                      bool separate_remote_pool) {
  bool made_progress = false;
  int deferred_compactions = 0;
  JobContext job_context(next_job_id_.fetch_add(1), true);
  TEST_SYNC_POINT("BackgroundCallCompaction:0");
  if (bg_thread_pri == Env::Priority::BOTTOM) {
    TEST_SYNC_POINT("BackgroundCallCompaction:0:BottomPri");
  }

  LogBuffer log_buffer(InfoLogLevel::INFO_LEVEL,
                       immutable_db_options_.info_log.get());
  {
    InstrumentedMutexLock l(&mutex_);

    num_running_compactions_++;
    const int priority_index = bg_thread_pri == Env::Priority::BOTTOM ? 0 : 1;
    if (!separate_remote_pool) {
      ++local_compactions_running_by_priority_[priority_index];
    }
    if (bg_thread_pri == Env::Priority::BOTTOM) {
      num_running_bottom_compactions_++;
    }

    std::unique_ptr<std::list<uint64_t>::iterator>
        pending_outputs_inserted_elem(new std::list<uint64_t>::iterator(
            CaptureCurrentFileNumberInPendingOutputs()));

    assert(bg_thread_pri == Env::Priority::BOTTOM ||
           bg_thread_pri == Env::Priority::LOW);
    assert(separate_remote_pool ? remote_coordinators_scheduled_ > 0
                                : (bg_thread_pri == Env::Priority::BOTTOM
                                       ? bg_bottom_compaction_scheduled_ > 0
                                       : bg_compaction_scheduled_ > 0));
    // The column family of this compaction if it has a census, with a
    // reference (see ApplyBlobReferenceCensusAfterInstall()).
    autovector<ColumnFamilyData*> census_cfds;
    Status s = BackgroundCompaction(
        &made_progress, &job_context, &log_buffer, prepicked_compaction,
        bg_thread_pri, separate_remote_pool, &deferred_compactions,
        **pending_outputs_inserted_elem, &census_cfds);
    TEST_SYNC_POINT("BackgroundCallCompaction:1");
    if (s.IsBusy()) {
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallCompaction:BeforeBackoff",
                               &s);
      immutable_db_options_.clock->SleepForMicroseconds(
          10000);  // prevent hot loop
      mutex_.Lock();
    } else if (!s.ok() && !s.IsShutdownInProgress() &&
               !s.IsManualCompactionPaused() && !s.IsColumnFamilyDropped() &&
               !s.IsCompactionAborted()) {
      // Wait a little bit before retrying background compaction in
      // case this is an environmental problem and we do not want to
      // chew up resources for failed compactions for the duration of
      // the problem.
      uint64_t error_cnt =
          default_cf_internal_stats_->BumpAndGetBackgroundErrorCount();
      bg_cv_.SignalAll();  // In case a waiter can proceed despite the error
      mutex_.Unlock();
      TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCallCompaction:BeforeBackoff",
                               &s);
      log_buffer.FlushBufferToLog();
      ROCKS_LOG_ERROR(immutable_db_options_.info_log,
                      "Waiting after background compaction error: %s, "
                      "Accumulated background error counts: %" PRIu64,
                      s.ToString().c_str(), error_cnt);
      LogFlush(immutable_db_options_.info_log);
      immutable_db_options_.clock->SleepForMicroseconds(1000000);
      mutex_.Lock();
    } else if (s.IsManualCompactionPaused()) {
      // A manual compaction ends with this status when the user pauses or
      // cancels it. An automatic compaction can also end with it: a remote
      // compaction returns it when the CompactionService cancels the job, for
      // example through OpenAndCompactOptions::canceled after the DB calls
      // CompactionService::CancelAwaitingJobs() during close. An automatic
      // compaction has no ManualCompactionState: its prepicked_compaction is
      // null, or has a null manual_compaction_state when the compaction was
      // forwarded to the bottom-priority pool.
      const ManualCompactionState* m =
          prepicked_compaction != nullptr
              ? prepicked_compaction->manual_compaction_state
              : nullptr;
      if (m != nullptr) {
        ROCKS_LOG_BUFFER(&log_buffer, "[%s] [JOB %d] Manual compaction paused",
                         m->cfd->GetName().c_str(), job_context.job_id);
      } else {
        ROCKS_LOG_BUFFER(&log_buffer,
                         "[JOB %d] Automatic compaction canceled: %s",
                         job_context.job_id, s.ToString().c_str());
      }
    }

    ReleaseFileNumberFromPendingOutputs(pending_outputs_inserted_elem);

    // If compaction failed, we want to delete all temporary files that we
    // might have created (they might not be all recorded in job_context in
    // case of a failure). Thus, we force full scan in FindObsoleteFiles()
    FindObsoleteFiles(&job_context, !s.ok() && !s.IsShutdownInProgress() &&
                                        !s.IsManualCompactionPaused() &&
                                        !s.IsCompactionAborted() &&
                                        !s.IsColumnFamilyDropped() &&
                                        !s.IsBusy());
    TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:FoundObsoleteFiles");

    // delete unnecessary files if any, this is done outside the mutex
    if (job_context.HaveSomethingToClean() ||
        job_context.HaveSomethingToDelete() || !log_buffer.IsEmpty()) {
      mutex_.Unlock();
      // Have to flush the info logs before bg_compaction_scheduled_--
      // because if bg_flush_scheduled_ becomes 0 and the lock is
      // released, the deconstructor of DB can kick in and destroy all the
      // states of DB so info_log might not be available after that point.
      // It also applies to access other states that DB owns.
      log_buffer.FlushBufferToLog();
      if (job_context.HaveSomethingToDelete()) {
        PurgeObsoleteFiles(job_context);
        TEST_SYNC_POINT("DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles");
      }
      job_context.Clean();
      mutex_.Lock();
    }
    ApplyBlobReferenceCensusAfterInstall(census_cfds);

    assert(num_running_compactions_ > 0);
    num_running_compactions_--;
    if (separate_remote_pool) {
      --remote_coordinators_scheduled_;
    } else {
      --local_compactions_running_by_priority_[priority_index];
      if (bg_thread_pri == Env::Priority::BOTTOM) {
        --bg_bottom_compaction_scheduled_;
      } else {
        --bg_compaction_scheduled_;
      }
    }

    if (bg_thread_pri == Env::Priority::BOTTOM) {
      assert(num_running_bottom_compactions_ > 0);
      num_running_bottom_compactions_--;
    }

    unscheduled_compactions_ += deferred_compactions;
    // See if there's more work to be done
    MaybeScheduleFlushOrCompaction();

    NotifyOnBackgroundJobPressureChanged();

    if (prepicked_compaction != nullptr &&
        prepicked_compaction->task_token != nullptr) {
      // Releasing task tokens affects (and asserts on) the DB state, so
      // must be done before we potentially signal the DB close process to
      // proceed below.
      prepicked_compaction->task_token.reset();
    }

    if (separate_remote_pool || made_progress ||
        per_cf_compaction_abort_waiters_ > 0 || CompactionsScheduled() == 0 ||
        HasPendingManualCompaction() || unscheduled_compactions_ == 0) {
      // signal if
      // * made_progress -- need to wakeup DelayWrite
      // * bg_{bottom,}_compaction_scheduled_ == 0 -- need to wakeup ~DBImpl
      // * HasPendingManualCompaction -- need to wakeup RunManualCompaction
      // If none of this is true, there is no need to signal since nobody is
      // waiting for it
      bg_cv_.SignalAll();
    }
    // IMPORTANT: there should be no code after calling SignalAll. This call may
    // signal the DB destructor that it's OK to proceed with destruction. In
    // that case, all DB variables will be dealloacated and referencing them
    // will cause trouble.
  }
}

// Precondition: mutex_ must be held when calling this function.
Status DBImpl::BackgroundCompaction(
    bool* made_progress, JobContext* job_context, LogBuffer* log_buffer,
    PrepickedCompaction* prepicked_compaction, Env::Priority thread_pri,
    bool separate_remote_pool, int* deferred_compactions,
    uint64_t min_output_file_number,
    autovector<ColumnFamilyData*>* census_cfds) {
  ManualCompactionState* manual_compaction =
      prepicked_compaction == nullptr
          ? nullptr
          : prepicked_compaction->manual_compaction_state;
  *made_progress = false;
  mutex_.AssertHeld();
  const auto restore_compaction_credit = [&] {
    if (separate_remote_pool) {
      ++*deferred_compactions;
    } else {
      ++unscheduled_compactions_;
    }
  };
  // Use after a compaction released its input files without installing a new
  // version. With the separate remote pool, a new credit is deferred until
  // this coordinator finishes, like the failed automatic compaction retry
  // below.
  const auto rescore_and_requeue = [&](ColumnFamilyData* cfd) {
    const int unscheduled_before = unscheduled_compactions_;
    RescoreAndEnqueuePendingCompaction(cfd);
    if (separate_remote_pool && unscheduled_compactions_ > unscheduled_before) {
      --unscheduled_compactions_;
      restore_compaction_credit();
    }
  };
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Start");

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  const WriteOptions write_options(Env::IOActivity::kCompaction);

  bool is_manual = (manual_compaction != nullptr);
  std::unique_ptr<Compaction> c;
  if (prepicked_compaction != nullptr &&
      prepicked_compaction->compaction != nullptr) {
    c.reset(prepicked_compaction->compaction);
  }
  // Track this CF as having an in-flight compaction for the duration of this
  // BackgroundCompaction (which covers NotifyOnCompactionCompleted). The Defer
  // ends tracking on every return path. BeginInFlightCompaction()'s Ref keeps a
  // dropped CF alive across any c.reset() until then; if this ends the CF's
  // last reference, the FindObsoleteFiles() pass in BackgroundCallCompaction
  // cleans up its files.
  ColumnFamilyData* in_flight_cfd = nullptr;
  Defer end_in_flight_compaction([&]() {
    if (in_flight_cfd != nullptr) {
      if (in_flight_cfd->blob_reference_census() != nullptr &&
          !in_flight_cfd->IsDropped()) {
        // The caller applies the census of this column family and then drops
        // this reference. A dropped column family gets no extra reference, so
        // the comment above stays true for it.
        in_flight_cfd->Ref();
        census_cfds->push_back(in_flight_cfd);
        mutex_.AssertHeld();
        ++blob_reference_census_pending_applies_;
      }
      EndInFlightCompaction(in_flight_cfd);
    }
  });
  if (c != nullptr) {
    in_flight_cfd = c->column_family_data();
    BeginInFlightCompaction(in_flight_cfd);
  }
  // Set just before new output files are installed, and released on return,
  // after NotifyOnCompactionCompleted() (see listener_pending_outputs_).
  std::optional<std::multiset<uint64_t>::iterator> listener_pending_output;
  Defer release_listener_pending_output(
      [&]() { ReleaseListenerPendingOutput(listener_pending_output); });
  bool is_prepicked = is_manual || c;
  ColumnFamilyData* prepicked_cfd = nullptr;
  if (c) {
    prepicked_cfd = c->column_family_data();
  } else if (manual_compaction != nullptr) {
    prepicked_cfd = manual_compaction->cfd;
  }

  // (manual_compaction->in_progress == false);
  bool trivial_move_disallowed =
      is_manual && manual_compaction->disallow_trivial_move;

  CompactionJobStats compaction_job_stats;
  // Set is_remote_compaction to true on CompactionBegin Event if
  // compaction_service is set except for trivial moves. We do not know whether
  // remote compaction will actually be successfully scheduled, or fall back to
  // local at this time. CompactionCompleted event will tell the truth where
  // the compaction actually happened.
  compaction_job_stats.is_remote_compaction =
      immutable_db_options().compaction_service != nullptr;

  Status status;
  if (!error_handler_.IsBGWorkStopped()) {
    if (shutting_down_.load(std::memory_order_acquire)) {
      status = Status::ShutdownInProgress();
    } else if (compaction_aborted_.load(std::memory_order_acquire) > 0) {
      status = Status::Incomplete(Status::SubCode::kCompactionAborted);
      if (!is_prepicked) {
        // This automatic compaction was scheduled from compaction_queue_, but
        // abort happened before PickCompactionFromQueue() could remove the
        // queued CF. Restore the unscheduled count so ResumeAllCompactions()
        // can schedule the still-queued work.
        restore_compaction_credit();
      }
    } else if (prepicked_cfd != nullptr &&
               prepicked_cfd->compaction_aborted() > 0) {
      status = Status::Incomplete(Status::SubCode::kCompactionAborted);
    } else if (is_manual &&
               manual_compaction->canceled.load(std::memory_order_acquire)) {
      status = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
    }
  } else {
    status = error_handler_.GetBGError();
    // If we get here, it means a hard error happened after this compaction
    // was scheduled by MaybeScheduleFlushOrCompaction(), but before it got a
    // chance to execute. Since non-prepicked work consumed an unscheduled
    // compaction credit without popping a cfd from the compaction queue,
    // restore the credit here. Prepicked work was scheduled directly and did
    // not consume unscheduled_compactions_.
    if (!is_prepicked) {
      restore_compaction_credit();
    }
  }

  if (!status.ok()) {
    if (is_manual) {
      manual_compaction->status = status;
      manual_compaction->done = true;
      manual_compaction->in_progress = false;
      manual_compaction = nullptr;
    }
    if (c) {
      c->ReleaseCompactionFiles(status);
      rescore_and_requeue(c->column_family_data());
      c.reset();
    }
    return status;
  }

  if (is_manual) {
    // another thread cannot pick up the same work
    manual_compaction->in_progress = true;
  }

  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:InProgress");

  std::unique_ptr<TaskLimiterToken> task_token;

  bool sfm_reserved_compact_space = false;
  uint64_t sfm_reserved_blob_gc_bytes = 0;
  bool ran_indirect_blob_gc = false;
  bool indirect_blob_gc_stale_input = false;
  ColumnFamilyData* indirect_blob_gc_cfd = nullptr;
  IOStatus io_s;
  // Some early paths return before any I/O runs, so mark the default status as
  // checked. Each later assignment replaces it and is checked before return.
  io_s.PermitUncheckedError();
  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    assert(m->in_progress);
    if (!c) {
      m->done = true;
      m->manual_end = nullptr;
      ROCKS_LOG_BUFFER(
          log_buffer,
          "[%s] Manual compaction from level-%d from %s .. "
          "%s; nothing to do\n",
          m->cfd->GetName().c_str(), m->input_level,
          (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
          (m->end ? m->end->DebugString(true).c_str() : "(end)"));
    } else {
      // First check if we have enough room to do the compaction
      bool enough_room = EnoughRoomForCompaction(
          m->cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

      if (!enough_room) {
        // Then don't do the compaction
        c->ReleaseCompactionFiles(status);
        rescore_and_requeue(c->column_family_data());
        c.reset();
        // m's vars will get set properly at the end of this function,
        // as long as status == CompactionTooLarge
        status = Status::CompactionTooLarge();
      } else {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Manual compaction from level-%d to level-%d from %s .. "
            "%s; will stop at %s\n",
            m->cfd->GetName().c_str(), m->input_level, c->output_level(),
            (m->begin ? m->begin->DebugString(true).c_str() : "(begin)"),
            (m->end ? m->end->DebugString(true).c_str() : "(end)"),
            ((m->done || m->manual_end == nullptr)
                 ? "(end)"
                 : m->manual_end->DebugString(true).c_str()));
      }
    }
  } else if (ShouldPickCompaction(is_prepicked, prepicked_compaction)) {
    bool need_repick = is_prepicked && prepicked_compaction->need_repick;
    if (HasExclusiveManualCompaction()) {
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction()::Conflict");

      // TODO(hx235): Resolve conflict between intended
      // bottom-priority compaction (requiring repick, i.e., need_repick = true)
      // and exclusive manual compaction by releasing the intended
      // bottom-priority compaction.
      if (!need_repick) {
        // Can't compact right now, but try again later
        //
        // Increase `unscheduled_compactions_` directly so we
        // don't need to
        // dequeue and enqueue the CFD again in the compaction queue and thus
        // keep the CFD's position in the queue
        restore_compaction_credit();

        return Status::OK();
      }
    }

    ColumnFamilyData* cfd = nullptr;

    if (!need_repick) {
      auto pick_result = PickCompactionFromQueue(&task_token, log_buffer);
      if (std::holds_alternative<CompactionQueueThrottled>(pick_result)) {
        // Can't find any executable task from the compaction queue.
        // All tasks have been throttled by compaction thread limiter.
        restore_compaction_credit();
        return Status::Busy();
      }
      if (std::holds_alternative<CompactionQueueParked>(pick_result)) {
        return Status::Incomplete(Status::SubCode::kCompactionAborted);
      }
      cfd = std::get<CompactionQueuePicked>(pick_result).cfd;
      assert(cfd != nullptr);

      // We unreference here because the following code will take a Ref() on
      // this cfd if it is going to use it (Compaction class holds a
      // reference).
      // This will all happen under a mutex so we don't have to be afraid of
      // somebody else deleting it.
      if (cfd->UnrefAndTryDelete()) {
        // This was the last reference of the column family, so no need to
        // compact.
        return Status::OK();
      }
    } else {
      cfd = c->column_family_data();
      assert(cfd);
      ResetBottomPriCompactionIntent(cfd, c);
      assert(c == nullptr);
    }

    // Pick up latest mutable CF Options and use it throughout the
    // compaction job
    // Compaction makes a copy of the latest MutableCFOptions. It should be used
    // throughout the compaction procedure to make sure consistency. It will
    // eventually be installed into SuperVersion
    const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
    if (!mutable_cf_options.disable_auto_compactions && !cfd->IsDropped()) {
      // NOTE: try to avoid unnecessary copy of MutableCFOptions if
      // compaction is not necessary. Need to make sure mutex is held
      // until we make a copy in the following code
      TEST_SYNC_POINT("DBImpl::BackgroundCompaction():BeforePickCompaction");
      // This info is not useful for other scenarios, so save querying existing
      // snapshots for those cases.
      if (cfd->ioptions().compaction_style == kCompactionStyleUniversal &&
          cfd->user_comparator()->timestamp_size() == 0) {
        InitSnapshotContext(job_context);
        assert(is_snapshot_supported_ || snapshots_.empty());
      }
      c.reset(cfd->PickCompaction(
          mutable_cf_options, mutable_db_options_, job_context->snapshot_seqs,
          job_context->snapshot_checker, log_buffer,
          thread_pri == Env::Priority::BOTTOM /* require_max_output_level */));
      if (c != nullptr && in_flight_cfd == nullptr) {
        in_flight_cfd = cfd;
        BeginInFlightCompaction(in_flight_cfd);
      }
      if (thread_pri == Env::Priority::LOW) {
        TEST_SYNC_POINT("DBImpl::BackgroundCompaction():AfterPickCompaction");
      } else if (thread_pri == Env::Priority::BOTTOM) {
        TEST_SYNC_POINT_CALLBACK(
            "DBImpl::BackgroundCompaction():AfterPickCompactionBottomPri",
            c.get());
      }

      if (c != nullptr) {
        bool enough_room = EnoughRoomForCompaction(
            cfd, *(c->inputs()), &sfm_reserved_compact_space, log_buffer);

        if (!enough_room) {
          // Then don't do the compaction
          c->ReleaseCompactionFiles(status);
          c->column_family_data()
              ->current()
              ->storage_info()
              ->ComputeCompactionScore(c->immutable_options(),
                                       c->mutable_cf_options(),
                                       cfd->GetFullHistoryTsLow());
          EnqueuePendingCompaction(cfd);

          c.reset();
          // Don't need to sleep here, because BackgroundCallCompaction
          // will sleep if !s.ok()
          status = Status::CompactionTooLarge();
        } else {
          // update statistics
          size_t num_files = 0;
          for (auto& each_level : *c->inputs()) {
            num_files += each_level.files.size();
          }
          RecordInHistogram(stats_, NUM_FILES_IN_SINGLE_COMPACTION, num_files);

          // There are three things that can change compaction score:
          // 1) When flush or compaction finish. This case is covered by
          // InstallSuperVersionAndScheduleWork
          // 2) When MutableCFOptions changes. This case is also covered by
          // InstallSuperVersionAndScheduleWork, because this is when the new
          // options take effect.
          // 3) When we Pick a new compaction, we "remove" those files being
          // compacted from the calculation, which then influences compaction
          // score. Inside EnqueuePendingCompaction(),  we check if we need
          // the new compaction even without the files that are currently being
          // compacted. If we need another compaction, we might be able to
          // execute it in parallel, so we add it to the queue and schedule a
          // new thread.
          EnqueuePendingCompaction(cfd);
          MaybeScheduleFlushOrCompaction();
        }
      } else if (is_prepicked) {
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Pre-picked compaction repicked files for compaction as "
            "required, "
            "but upon re-evaluation, no compaction was found necessary \n",
            cfd->GetName().c_str());
      }

      // Normal LSM compaction comes first. Indirect blob GC runs only on a
      // LOW-pool worker that has no pre-picked compaction and found no LSM
      // compaction. Blob files, blob GC and blob indirection must be on, and
      // the census must be ready. GC takes up to kIndirectBlobGCMaxInputs (8)
      // candidates in the Version's rank order
      // (GetBlobPhysicalFilesForIndirectBlobGC()), and RunIndirectBlobGCBatch()
      // checks them again in full. Only one GC job runs per column family at a
      // time (indirect_blob_gc_cfs_in_progress_).
      BlobReferenceCensus* const blob_reference_census =
          cfd == nullptr ? nullptr : cfd->blob_reference_census();
      Version* const current_version =
          cfd == nullptr ? nullptr : cfd->current();
      if (status.ok() && c == nullptr && !is_prepicked && cfd != nullptr &&
          job_context != nullptr && thread_pri == Env::Priority::LOW &&
          mutable_cf_options.enable_blob_files &&
          mutable_cf_options.enable_blob_garbage_collection &&
          mutable_cf_options.enable_blob_indirection &&
          blob_reference_census != nullptr && current_version != nullptr &&
          blob_reference_census->IsReady(current_version->GetVersionNumber())) {
        VersionStorageInfo* const storage = current_version->storage_info();
        std::vector<uint64_t> candidates;
        storage->GetBlobPhysicalFilesForIndirectBlobGC(kIndirectBlobGCMaxInputs,
                                                       &candidates);
        if (!candidates.empty() &&
            indirect_blob_gc_cfs_in_progress_.insert(cfd->GetID()).second) {
          indirect_blob_gc_cfd = cfd;
          ran_indirect_blob_gc = true;
          if (in_flight_cfd == nullptr) {
            in_flight_cfd = cfd;
            BeginInFlightCompaction(in_flight_cfd);
          }
          // With a separate remote compaction pool this thread is a remote
          // coordinator. Indirect blob GC then runs as local work: it waits for
          // a local compaction credit, is counted in bg_compaction_scheduled_,
          // and runs in the LOW pool. If RunLocalCompaction returns without
          // running the job, its status is returned and GC did not run.
          const auto run_indirect_blob_gc_job =
              [&](const std::function<Status()>& gc) {
                if (!separate_remote_pool) {
                  return gc();
                }
                const std::atomic<bool> never_canceled{false};
                bool gc_ran = false;
                Status gc_status;
                mutex_.Unlock();
                Status local_status = RunLocalCompaction(
                    [&]() {
                      InstrumentedMutexLock gc_lock(&mutex_);
                      gc_ran = true;
                      gc_status = gc();
                    },
                    Env::Priority::LOW, cfd, never_canceled,
                    /*is_manual=*/false);
                mutex_.Lock();
                if (gc_ran) {
                  local_status.PermitUncheckedError();
                  return gc_status;
                }
                return local_status;
              };
          status = run_indirect_blob_gc_job([&]() {
            return RunIndirectBlobGCBatch(
                cfd, candidates, made_progress, &sfm_reserved_blob_gc_bytes,
                &indirect_blob_gc_stale_input, log_buffer, job_context->job_id);
          });
          io_s = versions_->io_status();
          indirect_blob_gc_cfs_in_progress_.erase(cfd->GetID());
        }
      }
    }
  }

  bool compaction_released = false;
  if (c) {
    c->SetSeparateRemoteCompactionPoolSelected(separate_remote_pool);
  }
  if (ran_indirect_blob_gc) {
    // The indirect blob GC job (RunIndirectBlobGCBatch()) completed the work
    // and returned with mutex_ held.
  } else if (!c) {
    // Nothing to do
    ROCKS_LOG_BUFFER(log_buffer, "Compaction nothing to do");
  } else if (c->deletion_compaction()) {
    // TODO(icanadi) Do we want to honor snapshots here? i.e. not delete old
    // file if there is alive snapshot pointing to it
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    assert(c->num_input_files(1) == 0);
    assert(c->column_family_data()->ioptions().compaction_style ==
           kCompactionStyleFIFO);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    for (const auto& f : *c->inputs(0)) {
      c->edit()->DeleteFile(c->level(), f->fd.GetNumber());
    }
    NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
    status = versions_->LogAndApply(
        c->column_family_data(), read_options, write_options, c->edit(),
        &mutex_, directories_.GetDbDir(),
        /*new_descriptor_log=*/false, /*column_family_options=*/nullptr,
        [&c, &compaction_released](const Status& s) {
          c->ReleaseCompactionFiles(s);
          compaction_released = true;
        });
    io_s = versions_->io_status();
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);
    ROCKS_LOG_BUFFER(log_buffer, "[%s] Deleted %d files\n",
                     c->column_family_data()->GetName().c_str(),
                     c->num_input_files(0));
    if (status.ok() && io_s.ok()) {
      UpdateFIFOCompactionStatus(c);
    }
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  } else if (c->is_trivial_copy_compaction()) {
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction:TriviaCopyBeforeCompaction",
        c->column_family_data());
    assert(c->num_input_files(1) == 0);
    assert(c->column_family_data()->ioptions().compaction_style ==
           kCompactionStyleFIFO);
    assert(c->compaction_reason() == CompactionReason::kChangeTemperature);

    compaction_job_stats.num_input_files = c->num_input_files(0);

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    std::vector<FileMetaData> out_files;
    // As for other compactions, the copy runs without the DB mutex. The inputs
    // are protected by being compacted and referenced by the input version, and
    // the new file numbers by pending_outputs_.
    mutex_.Unlock();
    if (separate_remote_pool) {
      try {
        status = RunLocalCompaction(
            [&] {
              io_s = CopyTrivialCompactionFiles(c.get(), log_buffer, &out_files,
                                                job_context->job_id);
            },
            thread_pri, c->column_family_data(),
            is_manual ? manual_compaction->canceled
                      : kManualCompactionCanceledFalse_,
            is_manual);
      } catch (...) {
        status = Status::Aborted();
      }
    } else {
      io_s = CopyTrivialCompactionFiles(c.get(), log_buffer, &out_files,
                                        job_context->job_id);
    }
    mutex_.Lock();

    // Update version set
    if (status.ok() && io_s.ok()) {
      // NOTE: ChangeTemperature should only copy one file at one file
      // hence *c->inputs(0) == out_files.size() == 1 if copy succeeded
      assert(c->inputs(0)->size() == 1);
      assert(out_files.size() == 1);

      auto out_file_metadata_it = out_files.begin();
      for (const auto& in_file : *c->inputs(0)) {
        if (out_file_metadata_it == out_files.end()) {
          break;
        }

        c->edit()->DeleteFile(c->level(), in_file->fd.GetNumber());
        c->edit()->AddFile(c->level(), *out_file_metadata_it);
        ++out_file_metadata_it;
      }

      NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                  compaction_job_stats, job_context->job_id);
      listener_pending_output =
          CaptureListenerPendingOutput(min_output_file_number);
      status = versions_->LogAndApply(
          c->column_family_data(), read_options, write_options, c->edit(),
          &mutex_, directories_.GetDbDir(),
          /*new_descriptor_log=*/false, /*column_family_options=*/nullptr,
          [&c, &compaction_released](const Status& s) {
            c->ReleaseCompactionFiles(s);
            compaction_released = true;
          });
    }

    if (io_s.ok()) {
      io_s = versions_->io_status();
    }

    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);
    if (status.ok() && io_s.ok()) {
      UpdateFIFOCompactionStatus(c);
    } else {
      for (const auto& in_file : *c->inputs(0)) {
        const std::string in_fname =
            TableFileName(c->immutable_options().cf_paths,
                          in_file->fd.GetNumber(), in_file->fd.GetPathId());
        ROCKS_LOG_BUFFER(
            log_buffer,
            "[%s] Failed to do trvial copy compaction: %s"
            " temperature=%s, to temperature=%s, status=%s, io_status=%s",
            c->column_family_data()->GetName().c_str(), in_fname.c_str(),
            temperature_to_string[in_file->temperature].c_str(),
            temperature_to_string[c->GetOutputTemperature()].c_str(),
            status.ToString().c_str(), io_s.ToString().c_str());
      }
    }
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK(
        "DBImpl::BackgroundCompaction:TriviaCopyAfterCompaction",
        c->column_family_data());
  } else if (!trivial_move_disallowed && c->IsTrivialMove()) {
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:TrivialMove");
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    // Instrument for event update
    // TODO(yhchiang): add op details for showing trivial-move.
    ThreadStatusUtil::SetColumnFamily(c->column_family_data());
    ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);

    compaction_job_stats.num_input_files = c->num_input_files(0);
    // Trivial moves do not get compacted remotely
    compaction_job_stats.is_remote_compaction = false;
    compaction_job_stats.num_input_files_trivially_moved =
        compaction_job_stats.num_input_files;

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);

    if (c->compaction_reason() == CompactionReason::kLevelMaxLevelSize &&
        c->immutable_options().compaction_pri == kRoundRobin) {
      int start_level = c->start_level();
      if (start_level > 0) {
        auto vstorage = c->input_version()->storage_info();
        c->edit()->AddCompactCursor(
            start_level,
            vstorage->GetNextCompactCursor(start_level, c->num_input_files(0)));
      }
    }

    // Perform the trivial move
    size_t moved_files = 0;
    size_t moved_bytes = 0;
    // Populate the edit first so that CompactionJobInfo has output file
    // info when OnCompactionPreCommit fires.
    PrepareTrivialMoveEdit(*c.get(), log_buffer, moved_files, moved_bytes);
    NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
    status = CommitTrivialMove(*c.get(), compaction_released);
    io_s = versions_->io_status();
    InstallSuperVersionAndScheduleWork(
        c->column_family_data(), job_context->superversion_contexts.data(),
        /*defer_snapshot_refresh=*/false);

    VersionStorageInfo::LevelSummaryStorage tmp{};
    c->column_family_data()->internal_stats()->IncBytesMoved(c->output_level(),
                                                             moved_bytes);
    {
      event_logger_.LogToBuffer(log_buffer)
          << "job" << job_context->job_id << "event" << "trivial_move"
          << "destination_level" << c->output_level() << "files" << moved_files
          << "total_files_size" << moved_bytes;
    }
    ROCKS_LOG_BUFFER(
        log_buffer, "[%s] Moved #%d files to level-%zu %zu bytes %s: %s\n",
        c->column_family_data()->GetName().c_str(), moved_files,
        c->output_level(), moved_bytes, status.ToString().c_str(),
        c->column_family_data()->current()->storage_info()->LevelSummary(&tmp));
    *made_progress = true;

    // Clear Instrument
    ThreadStatusUtil::ResetThreadStatus();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  } else if (!is_prepicked &&
             Compaction::OutputToNonZeroMaxOutputLevel(
                 c->output_level(),
                 c->column_family_data()
                     ->current()
                     ->storage_info()
                     ->MaxOutputLevel(
                         c->immutable_options().cf_allow_ingest_behind ||
                         immutable_db_options_.allow_ingest_behind)) &&
             env_->GetBackgroundThreads(Env::Priority::BOTTOM) > 0) {
    assert(thread_pri == Env::Priority::LOW);
    // Forward compactions involving last level to the bottom pool if it exists,
    // such that compactions unlikely to contribute to write stalls can be
    // delayed or deprioritized.
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:ForwardToBottomPriPool");
    CompactionArg* ca = new CompactionArg;
    ca->db = this;
    ca->compaction_pri_ = Env::Priority::BOTTOM;
    ca->prepicked_compaction = new PrepickedCompaction;

    // If `universal_reduce_file_locking` is true, we only lock a limited set of
    // input files by creating an intended compaction to forward to bottom
    // priority pool and repicking files when bottom priority thread
    // gets to execute this intended compaction
    const bool need_repick =
        c->mutable_cf_options()
            .compaction_options_universal.reduce_file_locking;
    if (need_repick) {
      ca->prepicked_compaction->compaction =
          CreateIntendedCompactionForwardedToBottomPriorityPool(c.get());
      c.reset();
      ca->prepicked_compaction->need_repick = true;
    } else {
      ca->prepicked_compaction->compaction = c.release();
      ca->prepicked_compaction->need_repick = false;
    }
    ca->prepicked_compaction->manual_compaction_state = nullptr;
    // Transfer requested token, so it doesn't need to do it again.
    ca->prepicked_compaction->task_token = std::move(task_token);
    assert(c == nullptr);
    status = ScheduleCompaction(ca, this);
  } else {
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:BeforeCompaction",
                             c->column_family_data());
    int output_level __attribute__((__unused__));
    output_level = c->output_level();
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:NonTrivial",
                             &output_level);
    InitSnapshotContext(job_context);
    assert(is_snapshot_supported_ || snapshots_.empty());

    CompactionJob compaction_job(
        job_context->job_id, c.get(), immutable_db_options_,
        mutable_db_options_, file_options_for_compaction_, versions_.get(),
        &shutting_down_, log_buffer, directories_.GetDbDir(),
        GetDataDir(c->column_family_data(), c->output_path_id()),
        GetDataDir(c->column_family_data(), 0), stats_, &mutex_,
        &error_handler_, job_context, table_cache_, &event_logger_,
        c->mutable_cf_options().paranoid_file_checks,
        c->mutable_cf_options().report_bg_io_stats, dbname_,
        &compaction_job_stats, thread_pri, io_tracer_,
        is_manual ? manual_compaction->canceled
                  : kManualCompactionCanceledFalse_,
        compaction_aborted_, db_id_, db_session_id_,
        c->column_family_data()->GetFullHistoryTsLow(), c->trim_ts(),
        &blob_callback_, &bg_compaction_scheduled_,
        &bg_bottom_compaction_scheduled_, &num_running_remote_compactions_);
    ConfigureCompactionExecutors(&compaction_job, c->column_family_data(),
                                 is_manual ? manual_compaction->canceled
                                           : kManualCompactionCanceledFalse_,
                                 is_manual, separate_remote_pool);
    compaction_job.Prepare(std::nullopt /*subcompact to be computed*/);

    std::unique_ptr<std::list<uint64_t>::iterator> min_options_file_number_elem;
    if (immutable_db_options().compaction_service != nullptr) {
      min_options_file_number_elem.reset(
          new std::list<uint64_t>::iterator(CaptureOptionsFileNumber()));
    }

    NotifyOnCompactionBegin(c->column_family_data(), c.get(), status,
                            compaction_job_stats, job_context->job_id);
    mutex_.Unlock();
    if (thread_pri == Env::Priority::LOW) {
      TEST_SYNC_POINT_CALLBACK(
          "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun", nullptr);
    } else {
      assert(thread_pri == Env::Priority::BOTTOM);
      TEST_SYNC_POINT(
          "DBImpl::BackgroundCompaction:NonTrivial:BeforeRunBottomPri");
    }

    // Should handle error?
    compaction_job.Run().PermitUncheckedError();
    TEST_SYNC_POINT("DBImpl::BackgroundCompaction:NonTrivial:AfterRun");
    mutex_.Lock();

    if (immutable_db_options().compaction_service != nullptr) {
      ReleaseOptionsFileNumber(min_options_file_number_elem);
    }

    // Fire OnCompactionPreCommit before compaction_job.Install runs
    // ReleaseCompactionFiles in its manifest write callback.
    NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
    listener_pending_output =
        CaptureListenerPendingOutput(min_output_file_number);
    status = compaction_job.Install(&compaction_released);
    io_s = compaction_job.io_status();
    if (status.ok()) {
      InstallSuperVersionAndScheduleWork(
          c->column_family_data(), job_context->superversion_contexts.data(),
          /*defer_snapshot_refresh=*/false);
    }
    *made_progress = true;
    TEST_SYNC_POINT_CALLBACK("DBImpl::BackgroundCompaction:AfterCompaction",
                             c->column_family_data());
  }

  if (status.ok() && !io_s.ok()) {
    status = io_s;
  } else {
    io_s.PermitUncheckedError();
  }

  if (c != nullptr) {
    if (!compaction_released) {
      // Safety net: if any of the paths above did not fire the
      // OnCompactionPreCommit notification (e.g. status was not OK and
      // an early-out skipped the LogAndApply / Install), still fire the
      // notify here while files are about to be released. The notify is
      // idempotent and is a no-op if it has already fired (or if Begin never
      // fired).
      NotifyOnCompactionPreCommit(c->column_family_data(), c.get(), status,
                                  compaction_job_stats, job_context->job_id);
      c->ReleaseCompactionFiles(status);
    } else {
#ifndef NDEBUG
      // Sanity checking that compaction files are freed.
      for (size_t i = 0; i < c->num_input_levels(); i++) {
        for (size_t j = 0; j < c->inputs(i)->size(); j++) {
          // When status is not OK, compaction's result installation failed and
          // no new Version installed. The files could have been released and
          // picked up again by other compaction attempts.
          assert(!c->input(i, j)->being_compacted || !status.ok());
        }
      }
      std::unordered_set<Compaction*>* cip = c->column_family_data()
                                                 ->compaction_picker()
                                                 ->compactions_in_progress();
      assert(cip->find(c.get()) == cip->end());
#endif
    }

    *made_progress = true;

    // Need to make sure SstFileManager does its bookkeeping
    auto sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    if (sfm && sfm_reserved_compact_space) {
      sfm->OnCompactionCompletion(c.get());
    }

    NotifyOnCompactionCompleted(c->column_family_data(), c.get(), status,
                                compaction_job_stats, job_context->job_id);
  }

  if (sfm_reserved_blob_gc_bytes != 0) {
    auto* const sfm = static_cast<SstFileManagerImpl*>(
        immutable_db_options_.sst_file_manager.get());
    assert(sfm != nullptr);
    sfm->OnCompactionCompletion(sfm_reserved_blob_gc_bytes);
  }

  // Puts the column family that indirect blob GC ran on back in the compaction
  // queue, so that a later worker picks GC again. With a separate remote pool,
  // the new queue entry is counted in *deferred_compactions instead of
  // unscheduled_compactions_, the same way as the compaction retry below.
  // BackgroundCallCompaction() adds that count back to unscheduled_compactions_
  // when this job ends.
  const auto retry_indirect_blob_gc = [&]() {
    if (indirect_blob_gc_cfd != nullptr && !indirect_blob_gc_cfd->IsDropped() &&
        !error_handler_.IsBGWorkStopped()) {
      const int unscheduled_before_retry = unscheduled_compactions_;
      EnqueuePendingCompaction(indirect_blob_gc_cfd);
      if (separate_remote_pool &&
          unscheduled_compactions_ > unscheduled_before_retry) {
        --unscheduled_compactions_;
        restore_compaction_credit();
      }
    }
  };

  if (status.ok() || status.IsCompactionTooLarge() ||
      status.IsManualCompactionPaused() || status.IsCompactionAborted()) {
    if (status.IsCompactionTooLarge() || status.IsCompactionAborted()) {
      retry_indirect_blob_gc();
    }
    // Done
  } else if (status.IsTryAgain() && ran_indirect_blob_gc &&
             indirect_blob_gc_stale_input) {
    // A stale input is not an error, so it becomes Busy. Busy sets no
    // background error and does not count as one. BackgroundCallCompaction()
    // then waits only 10 ms (to prevent a hot loop) instead of the 1 s wait
    // after an error, and it skips the full obsolete-file scan.
    retry_indirect_blob_gc();
    status = Status::Busy("Indirect blob GC input changed");
  } else if (status.IsColumnFamilyDropped() || status.IsShutdownInProgress()) {
    // Ignore compaction errors found during shutting down
  } else {
    ROCKS_LOG_WARN(immutable_db_options_.info_log, "%s error: %s",
                   ran_indirect_blob_gc ? "Indirect blob GC" : "Compaction",
                   status.ToString().c_str());
    if (!io_s.ok()) {
      // Error while writing to MANIFEST.
      // In fact, versions_->io_status() can also be the result of renaming
      // CURRENT file. With current code, it's just difficult to tell. So just
      // be pessimistic and try write to a new MANIFEST.
      // TODO: distinguish between MANIFEST write and CURRENT renaming
      auto err_reason = versions_->io_status().ok()
                            ? BackgroundErrorReason::kCompaction
                            : BackgroundErrorReason::kManifestWrite;
      error_handler_.SetBGError(io_s, err_reason);
    } else {
      error_handler_.SetBGError(status, BackgroundErrorReason::kCompaction);
    }
    if (c != nullptr && !is_manual && !error_handler_.IsBGWorkStopped()) {
      // Put this cfd back in the compaction queue so we can retry after some
      // time
      auto cfd = c->column_family_data();
      assert(cfd != nullptr);
      // Since this compaction failed, we need to recompute the score so it
      // takes the original input files into account
      c->column_family_data()
          ->current()
          ->storage_info()
          ->ComputeCompactionScore(c->immutable_options(),
                                   c->mutable_cf_options(),
                                   cfd->GetFullHistoryTsLow());
      const int unscheduled_before_retry = unscheduled_compactions_;
      EnqueuePendingCompaction(cfd);
      if (separate_remote_pool &&
          unscheduled_compactions_ > unscheduled_before_retry) {
        --unscheduled_compactions_;
        restore_compaction_credit();
      }
    } else if (ran_indirect_blob_gc) {
      // Indirect blob GC hit a background error. If the error did not stop
      // background work (for example, a retryable I/O error), put the CF back
      // in the queue, the same as the compaction retry above.
      // retry_indirect_blob_gc() does nothing if background work stopped.
      retry_indirect_blob_gc();
    }
  }
  if (c != nullptr && !status.ok() &&
      (is_manual || status.IsCompactionAborted())) {
    // The compaction released its input files without installing a new
    // version. A failed automatic compaction is requeued above.
    rescore_and_requeue(c->column_family_data());
  }
  // this will unref its input_version and column_family_data
  c.reset();

  if (is_manual) {
    ManualCompactionState* m = manual_compaction;
    if (!status.ok()) {
      m->status = status;
      m->done = true;
    }
    // For universal compaction:
    //   Because universal compaction always happens at level 0, so one
    //   compaction will pick up all overlapped files. No files will be
    //   filtered out due to size limit and left for a successive compaction.
    //   So we can safely conclude the current compaction.
    //
    //   Also note that, if we don't stop here, then the current compaction
    //   writes a new file back to level 0, which will be used in successive
    //   compaction. Hence the manual compaction will never finish.
    //
    // Stop the compaction if manual_end points to nullptr -- this means
    // that we compacted the whole range. manual_end should always point
    // to nullptr in case of universal compaction
    if (m->manual_end == nullptr) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      // Universal and FIFO compactions should always compact the whole range
      assert(m->cfd->ioptions().compaction_style != kCompactionStyleUniversal ||
             m->cfd->ioptions().num_levels > 1);
      assert(m->cfd->ioptions().compaction_style != kCompactionStyleFIFO);
      m->tmp_storage = *m->manual_end;
      m->begin = &m->tmp_storage;
      m->incomplete = true;
    }
    m->in_progress = false;  // not being processed anymore
  }
  TEST_SYNC_POINT("DBImpl::BackgroundCompaction:Finish");
  return status;
}

// Create an intended compaction to forward based on the original picked
// compaction. It serves two purposes while it is waiting
// for a bottom-priority thread becomes available to run:
// - Prevent the last input file (or sorted run if non-L0) from
// being included in compaction score calculations unnecessarily since the
// intended compaction is already scheduled to compact it
// - Allow other input files to be picked by low-priority compactions that can
// run right away
//
// Once a bottom-priority available to run this intended compaction, it will
// repick files to consider the LSM updates that occurred during the waiting
// period.
Compaction* DBImpl::CreateIntendedCompactionForwardedToBottomPriorityPool(
    Compaction* c) {
  auto* cfd = c->column_family_data();
  const auto& io = c->immutable_options();
  const auto& mo = c->mutable_cf_options();
  auto* vstorage = c->input_version()->storage_info();

  std::vector<CompactionInputFiles> inputs(1);

  const std::vector<FileMetaData*>* max_intput_level_files = nullptr;
  int max_intput_level = 0;

  for (size_t i = c->num_input_levels(); i >= 1; --i) {
    size_t which = i - 1;
    if (c->num_input_files(which) > 0) {
      max_intput_level = c->level(which);
      max_intput_level_files = c->inputs(which);
      break;
    }
  }

  assert(max_intput_level_files);
  assert(!max_intput_level_files->empty());
  inputs[0].level = max_intput_level;

  if (max_intput_level == 0) {
    // The last input file
    inputs[0].files.push_back(
        (*max_intput_level_files)[max_intput_level_files->size() - 1]);
  } else {
    // The last input sorted run
    for (FileMetaData* f : (*max_intput_level_files)) {
      inputs[0].files.push_back(f);
    }
  }

  c->ReleaseCompactionFiles(Status::OK());

  Compaction* intended_compaction =
      new Compaction(vstorage, io, mo, mutable_db_options_, std::move(inputs),
                     c->output_level(), c->target_output_file_size(),
                     c->max_compaction_bytes(), c->output_path_id(),
                     c->output_compression(), c->output_compression_opts(),
                     c->GetOutputTemperature(), c->max_subcompactions(),
                     c->grandparents(), std::nullopt /* earliest_snapshot */,
                     nullptr /* snapshot_checker */, c->compaction_reason());

  cfd->compaction_picker()->RegisterCompaction(intended_compaction);
  vstorage->ComputeCompactionScore(io, mo, cfd->GetFullHistoryTsLow());
  intended_compaction->FinalizeInputInfo(cfd->current());

  return intended_compaction;
}

bool DBImpl::HasPendingManualCompaction() {
  mutex_.AssertHeld();
  return (!manual_compaction_dequeue_.empty());
}

bool DBImpl::HasPendingManualCompaction(ColumnFamilyData* cfd) {
  mutex_.AssertHeld();
  return std::any_of(manual_compaction_dequeue_.begin(),
                     manual_compaction_dequeue_.end(),
                     [cfd](const ManualCompactionState* manual) {
                       return manual->cfd == cfd;
                     });
}

void DBImpl::AddManualCompaction(DBImpl::ManualCompactionState* m) {
  assert(manual_compaction_paused_ == 0);
  manual_compaction_dequeue_.push_back(m);
}

void DBImpl::RemoveManualCompaction(DBImpl::ManualCompactionState* m) {
  // Remove from queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      it = manual_compaction_dequeue_.erase(it);
      return;
    }
    ++it;
  }
  assert(false);
}

bool DBImpl::ShouldntRunManualCompaction(ManualCompactionState* m) {
  if (m->exclusive) {
    return CompactionsScheduled() > 0 || remote_compaction_pool_jobs_ > 0;
  }
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  bool seen = false;
  while (it != manual_compaction_dequeue_.end()) {
    if (m == (*it)) {
      ++it;
      seen = true;
      continue;
    } else if (MCOverlap(m, (*it)) && (!seen && !(*it)->in_progress)) {
      // Consider the other manual compaction *it, conflicts if:
      // overlaps with m
      // and (*it) is ahead in the queue and is not yet in progress
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::HaveManualCompaction(ColumnFamilyData* cfd) {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    if ((cfd == (*it)->cfd) && (!((*it)->in_progress || (*it)->done))) {
      // Allow automatic compaction if manual compaction is
      // in progress
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::HasExclusiveManualCompaction() {
  // Remove from priority queue
  std::deque<ManualCompactionState*>::iterator it =
      manual_compaction_dequeue_.begin();
  while (it != manual_compaction_dequeue_.end()) {
    if ((*it)->exclusive) {
      return true;
    }
    ++it;
  }
  return false;
}

bool DBImpl::MCOverlap(ManualCompactionState* m, ManualCompactionState* m1) {
  if ((m->exclusive) || (m1->exclusive)) {
    return true;
  }
  if (m->cfd != m1->cfd) {
    return false;
  }
  return false;
}

void DBImpl::UpdateFIFOCompactionStatus(const std::unique_ptr<Compaction>& c) {
  if (c == nullptr) {
    return;
  }

  CompactionReason reason = c->compaction_reason();

  switch (reason) {
    case CompactionReason::kFIFOMaxSize:
      RecordTick(stats_, FIFO_MAX_SIZE_COMPACTIONS);
      break;
    case CompactionReason::kFIFOTtl:
      RecordTick(stats_, FIFO_TTL_COMPACTIONS);
      break;
    case CompactionReason::kChangeTemperature:
      RecordTick(stats_, FIFO_CHANGE_TEMPERATURE_COMPACTIONS);
      break;
    default:
      assert(false);
      break;
  }
}

void DBImpl::BuildCompactionJobInfo(
    const ColumnFamilyData* cfd, Compaction* c, const Status& st,
    const CompactionJobStats& compaction_job_stats, const int job_id,
    CompactionJobInfo* compaction_job_info) const {
  assert(compaction_job_info != nullptr);
  compaction_job_info->cf_id = cfd->GetID();
  compaction_job_info->cf_name = cfd->GetName();
  compaction_job_info->status = st;
  compaction_job_info->aborted = st.IsCompactionAborted();
  compaction_job_info->thread_id = env_->GetThreadID();
  compaction_job_info->job_id = job_id;
  compaction_job_info->separate_remote_compaction_pool_selected =
      c->separate_remote_compaction_pool_selected();
  compaction_job_info->base_input_level = c->start_level();
  compaction_job_info->output_level = c->output_level();
  compaction_job_info->stats = compaction_job_stats;
  const auto& input_table_properties = c->GetOrInitInputTableProperties();
  const auto& output_table_properties = c->GetOutputTableProperties();
  compaction_job_info->table_properties.insert(input_table_properties.begin(),
                                               input_table_properties.end());
  compaction_job_info->table_properties.insert(output_table_properties.begin(),
                                               output_table_properties.end());
  compaction_job_info->compaction_reason = c->compaction_reason();
  compaction_job_info->compression = c->output_compression();

  const ReadOptions read_options(Env::IOActivity::kCompaction);
  for (size_t i = 0; i < c->num_input_levels(); ++i) {
    for (const auto fmd : *c->inputs(i)) {
      const FileDescriptor& desc = fmd->fd;
      const uint64_t file_number = desc.GetNumber();
      auto fn = TableFileName(c->immutable_options().cf_paths, file_number,
                              desc.GetPathId());
      compaction_job_info->input_files.push_back(fn);
      compaction_job_info->input_file_infos.push_back(CompactionFileInfo{
          static_cast<int>(i), file_number, fmd->oldest_blob_file_number});
    }
  }

  for (const auto& newf : c->edit()->GetNewFiles()) {
    const FileMetaData& meta = newf.second;
    const FileDescriptor& desc = meta.fd;
    const uint64_t file_number = desc.GetNumber();
    compaction_job_info->output_files.push_back(TableFileName(
        c->immutable_options().cf_paths, file_number, desc.GetPathId()));
    compaction_job_info->output_file_infos.push_back(CompactionFileInfo{
        newf.first, file_number, meta.oldest_blob_file_number});
  }
  compaction_job_info->blob_compression_type =
      c->mutable_cf_options().blob_compression_type;

  // Update BlobFilesInfo.
  for (const auto& blob_file : c->edit()->GetBlobFileAdditions()) {
    BlobFileAdditionInfo blob_file_addition_info(
        BlobFileName(c->immutable_options().cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetTotalBlobCount(),
        blob_file.GetTotalBlobBytes());
    compaction_job_info->blob_file_addition_infos.emplace_back(
        std::move(blob_file_addition_info));
  }

  // Update BlobFilesGarbageInfo.
  for (const auto& blob_file : c->edit()->GetBlobFileGarbages()) {
    BlobFileGarbageInfo blob_file_garbage_info(
        BlobFileName(c->immutable_options().cf_paths.front().path,
                     blob_file.GetBlobFileNumber()) /*blob_file_path*/,
        blob_file.GetBlobFileNumber(), blob_file.GetGarbageBlobCount(),
        blob_file.GetGarbageBlobBytes());
    compaction_job_info->blob_file_garbage_infos.emplace_back(
        std::move(blob_file_garbage_info));
  }
}

// SuperVersionContext gets created and destructed outside of the lock --
// we use this conveniently to:
// * malloc one SuperVersion() outside of the lock -- new_superversion
// * delete SuperVersion()s outside of the lock -- superversions_to_free
//
// However, if InstallSuperVersionAndScheduleWork() gets called twice with the
// same sv_context, we can't reuse the SuperVersion() that got
// malloced because
// first call already used it. In that rare case, we take a hit and create a
// new SuperVersion() inside of the mutex. We do similar thing
// for superversion_to_free

void DBImpl::InstallSuperVersionAndScheduleWork(
    ColumnFamilyData* cfd, SuperVersionContext* sv_context,
    bool defer_snapshot_refresh,
    std::optional<std::shared_ptr<SeqnoToTimeMapping>>
        new_seqno_to_time_mapping) {
  mutex_.AssertHeld();

  // Update max_total_in_memory_state_
  size_t old_memtable_size = 0;
  auto* old_sv = cfd->GetSuperVersion();
  if (old_sv) {
    old_memtable_size = old_sv->mutable_cf_options.write_buffer_size *
                        old_sv->mutable_cf_options.max_write_buffer_number;
  }

  // this branch is unlikely to step in
  if (UNLIKELY(sv_context->new_superversion == nullptr)) {
    sv_context->NewSuperVersion();
  }
  cfd->InstallSuperVersion(sv_context, &mutex_,
                           std::move(new_seqno_to_time_mapping));
  // Queue the census of this CF if it needs a run, for example when the CF got
  // the format fence after open. The call is O(1).
  ScheduleBlobReferenceCensus(cfd);

  // Refresh the seqno->time preserve-window bound used to gate bottommost file
  // marking, so a file whose largest seqno is still within the preserve window
  // is not marked for a compaction that cannot zero it out (infinite loop).
  MaybeUpdatePreserveTimeMinSeqno(cfd);

  if (defer_snapshot_refresh) {
    // Atomic multi-CF ingestion can already hold many per-CF locks. Reconcile
    // the installed Version against the last exact observation now and defer
    // the all-shard scan until those locks have been released.
    ReconcileOldestSnapshotForColumnFamily(cfd,
                                           last_processed_oldest_snapshot_);
  } else {
    TEST_SYNC_POINT(
        "DBImpl::InstallSuperVersionAndScheduleWork:BeforeSnapshotRefresh");
    uint64_t snapshot_release_count = 0;
    const SequenceNumber oldest_snapshot =
        snapshots_.OldestOr([this]() { return GetLastPublishedSequence(); },
                            &snapshot_release_count);
    UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                        cfd);
  }

  // Whenever we install new SuperVersion, we might need to issue new flushes or
  // compactions.
  EnqueuePendingCompaction(cfd);
  MaybeScheduleFlushOrCompaction();

  // Update max_total_in_memory_state_
  max_total_in_memory_state_ =
      max_total_in_memory_state_ - old_memtable_size +
      cfd->GetLatestMutableCFOptions().write_buffer_size *
          cfd->GetLatestMutableCFOptions().max_write_buffer_number;
}

// ShouldPurge is called by FindObsoleteFiles when doing a full scan,
// and db mutex (mutex_) should already be held.
// Actually, the current implementation of FindObsoleteFiles with
// full_scan=true can issue I/O requests to obtain list of files in
// directories, e.g. env_->getChildren while holding db mutex.
bool DBImpl::ShouldPurge(uint64_t file_number) const {
  return files_grabbed_for_purge_.find(file_number) ==
             files_grabbed_for_purge_.end() &&
         purge_files_.find(file_number) == purge_files_.end();
}

// MarkAsGrabbedForPurge is called by FindObsoleteFiles, and db mutex
// (mutex_) should already be held.
void DBImpl::MarkAsGrabbedForPurge(uint64_t file_number) {
  files_grabbed_for_purge_.insert(file_number);
}

void DBImpl::EnableTrackPublishedSeqInSnapshotContext() {
  InstrumentedMutexLock l(&mutex_);
  track_published_seq_in_snapshot_context_ = true;
}

void DBImpl::SetSnapshotChecker(SnapshotChecker* snapshot_checker) {
  InstrumentedMutexLock l(&mutex_);
  // snapshot_checker_ should only set once. If we need to set it multiple
  // times, we need to make sure the old one is not deleted while it is still
  // using by a compaction job.
  assert(!snapshot_checker_);
  snapshot_checker_.reset(snapshot_checker);
}

void DBImpl::InitSnapshotContext(JobContext* job_context) {
  mutex_.AssertHeld();
  assert(job_context != nullptr);
  if (job_context->snapshot_context_initialized) {
    return;
  }
  SnapshotChecker* snapshot_checker = snapshot_checker_.get();
  if (use_custom_gc_ && !snapshot_checker) {
    snapshot_checker = DisableGCSnapshotChecker::Instance();
  }
  std::unique_ptr<ManagedSnapshot> managed_snapshot = nullptr;
  if (snapshot_checker) {
    // If snapshot_checker is used, that means the flush/compaction may
    // contain values not visible to snapshot taken after
    // flush/compaction job starts. Take a snapshot and it will appear
    // in snapshot_seqs and force compaction iterator to consider such
    // snapshots.
    const Snapshot* snapshot = GetSnapshotImpl(
        /*is_write_conflict_boundary=*/false, SnapshotLockMode::kDBMutexHeld);
    managed_snapshot.reset(new ManagedSnapshot(this, snapshot));
  }
  // The SnapshotChecker path above already adds a managed snapshot to
  // snapshot_seqs.
  const bool pin_published_seq =
      track_published_seq_in_snapshot_context_ && !snapshot_checker;
  // Read the published sequence number before loading the snapshots.
  // GetSnapshot() does not take mutex_, so a snapshot that the load below
  // misses reads its sequence number later and is no smaller than this pin.
  const SequenceNumber published_seq =
      pin_published_seq ? GetLastPublishedSequence() : 0;
  SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber;
  std::vector<SequenceNumber> snapshot_seqs;
  uint64_t snapshot_release_count = 0;
  const SequenceNumber oldest_snapshot =
      LoadSnapshots(&snapshot_seqs, &earliest_write_conflict_snapshot,
                    kMaxSequenceNumber, &snapshot_release_count);
  UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                      /*installed_cfd=*/nullptr);

  if (pin_published_seq) {
    // Pin the published-sequence boundary into snapshot_seqs without taking
    // a real snapshot. This prevents flush/compaction from collapsing a version
    // visible at the published boundary into a newer unpublished seqno.
    auto it = std::lower_bound(snapshot_seqs.begin(), snapshot_seqs.end(),
                               published_seq);
    if (it == snapshot_seqs.end() || *it != published_seq) {
      snapshot_seqs.insert(it, published_seq);
    }
  }
  TEST_SYNC_POINT("DBImpl::InitSnapshotContext:BeforeInit");
  job_context->InitSnapshotContext(
      snapshot_checker, std::move(managed_snapshot),
      earliest_write_conflict_snapshot, std::move(snapshot_seqs));
}

Status DBImpl::WaitForCompact(
    const WaitForCompactOptions& wait_for_compact_options) {
  InstrumentedMutexLock l(&mutex_);
  // `close_db=true` needs the same direct-write cleanup guarantee as DB close:
  // register live blob files before relying on the DB being reopenable.
  const bool flush_for_blob_direct_write =
      HasAnyBlobDirectWriteColumnFamilyWithLockHeld();
  if (wait_for_compact_options.flush) {
    Status s = DBImpl::FlushAllColumnFamilies(FlushOptions(),
                                              FlushReason::kManualFlush);
    if (!s.ok()) {
      return s;
    }
  } else if (wait_for_compact_options.close_db &&
             (flush_for_blob_direct_write ||
              (has_unpersisted_data_.load(std::memory_order_relaxed) &&
               !mutable_db_options_.avoid_flush_during_shutdown))) {
    Status s =
        DBImpl::FlushAllColumnFamilies(FlushOptions(), FlushReason::kShutDown);
    if (!s.ok()) {
      return s;
    }
  }
  const auto deadline = immutable_db_options_.clock->NowMicros() +
                        wait_for_compact_options.timeout.count();
  TEST_SYNC_POINT("DBImpl::WaitForCompact:StartWaiting");
  for (;;) {
    // The coordinator reserves this DB before invoking its non-blocking
    // scheduling callback. Drain that DB-specific reservation without holding
    // mutex_; successful callbacks publish bg_wbm_flush_scheduled_ before
    // releasing the reservation.
    if (write_buffer_manager_ != nullptr &&
        write_buffer_manager_->ShouldTrackFlushInitiator() &&
        wbm_flush_initiator_ != nullptr) {
      // Fence all coordinator requests visible before this local quiescence
      // check. Candidate retrieval either observes this generation and skips
      // the DB, or atomically publishes a handoff reservation first.
      write_buffer_manager_->FenceFlushInitiator(wbm_flush_initiator_.get());
    }
    if (wbm_flush_initiator_ != nullptr &&
        wbm_flush_initiator_->HasPendingFlushHandoff()) {
      std::chrono::microseconds handoff_timeout{0};
      if (wait_for_compact_options.timeout.count() != 0) {
        const uint64_t now = immutable_db_options_.clock->NowMicros();
        if (now >= deadline) {
          return Status::TimedOut();
        }
        handoff_timeout = std::chrono::microseconds(deadline - now);
      }
      mutex_.Unlock();
      const bool handoff_finished =
          wbm_flush_initiator_->WaitForPendingFlushHandoff(handoff_timeout);
      mutex_.Lock();
      if (!handoff_finished) {
        return Status::TimedOut();
      }
      continue;
    }
    if (shutting_down_.load(std::memory_order_acquire)) {
      return Status::ShutdownInProgress();
    }
    if (bg_work_paused_ && wait_for_compact_options.abort_on_pause) {
      return Status::Aborted();
    }
    uint64_t snapshot_release_count = 0;
    const SequenceNumber oldest_snapshot =
        snapshots_.OldestOr([this]() { return GetLastPublishedSequence(); },
                            &snapshot_release_count);
    UpdateOldestSnapshotAndScheduleWork(oldest_snapshot, snapshot_release_count,
                                        /*installed_cfd=*/nullptr);
    // A WBM job can enqueue a regular flush, so wait for both to quiesce.
    if ((CompactionsScheduled() || remote_compaction_pool_jobs_ ||
         bg_flush_scheduled_ || bg_wbm_flush_scheduled_ ||
         unscheduled_compactions_ || !parked_compaction_cfds_.empty() ||
         (wait_for_compact_options.wait_for_purge && bg_purge_scheduled_) ||
         unscheduled_flushes_ || error_handler_.IsRecoveryInProgress()) &&
        (error_handler_.GetBGError().ok())) {
      if (wait_for_compact_options.timeout.count()) {
        if (bg_cv_.TimedWait(deadline)) {
          return Status::TimedOut();
        }
      } else {
        TEST_SYNC_POINT("DBImpl::WaitForCompact:InsideLoop");
        bg_cv_.Wait();
      }
    } else if (wait_for_compact_options.close_db) {
      reject_new_background_jobs_ = true;
      mutex_.Unlock();
      Status s = Close();
      mutex_.Lock();
      if (!s.ok()) {
        reject_new_background_jobs_ = false;
      }
      return s;
    } else {
      return error_handler_.GetBGError();
    }
  }
}

bool DBImpl::ShouldPickCompaction(
    bool is_prepicked, const PrepickedCompaction* prepicked_compaction) {
  return (!is_prepicked && !compaction_queue_.empty()) ||
         (is_prepicked && prepicked_compaction->need_repick);
}

void DBImpl::ResetBottomPriCompactionIntent(ColumnFamilyData* cfd,
                                            std::unique_ptr<Compaction>& c) {
  c->ReleaseCompactionFiles(Status::OK());
  cfd->current()->storage_info()->ComputeCompactionScore(
      c->immutable_options(), c->mutable_cf_options(),
      cfd->GetFullHistoryTsLow());
  c.reset();
}

}  // namespace ROCKSDB_NAMESPACE
