// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_manifest.h"
#include "rocksdb/status.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

class CacheReservationManager;

// Indirect blob GC building blocks
//
// Intended user: indirect blob GC.
//
// Indirect blob GC copies the live values of mostly-garbage blob files into
// new relocation files, and rewrites no SST. One GC job collects a few files of
// one column family. A job is built from five parts that work without the DB.
// This file holds the first three:
// 1. Live set (BlobRelocationLiveSet): the live references of one input.
// 2. Memory budget (BlobRelocationMemoryTracker): bounds the working memory of
//    the job.
// 3. Packing plan (PlanBlobRelocationPacking()): puts the inputs into outputs.
// 4. Batch reference reader (TableReader::ReadBlobReferences() in
//    table/table_reader.h): reads the references of many groups from one SST.
// 5. Stored reads and writes: BlobFileReader and BlobRelocationFileReader in
//    db/blob/blob_file_reader.h read the stored bytes of values. The two
//    BlockBasedTableBuilder::AddBlobRelocation() overloads write them into an
//    output. The streaming overload uses WriteSimpleGen2BlobRecordStream() in
//    db/blob/blob_gen2_format.h.
//
// Terms:
// - Input: a file that one GC job collects. An input is a normal blob file or
//   a relocation file. A normal input is an indirect-origin blob file: a normal
//   blob file written with enable_blob_indirection on
//   (BlobFileMetaData::IsIndirectOrigin()), so it is the origin of
//   kIndirectBlob references.
// - Route: a MANIFEST record that maps an old file number to the relocation
//   file that now holds its live values (see
//   db/blob/blob_relocation_manifest.h).
// - Generation numbers of an input: the file numbers that SSTs use to reference
//   the input's live values. They are the input's own file number and, for a
//   relocation file, every old file number that has a route to it.
// - Output: a relocation file that the job writes.
// - Live set: the references to one input that some SST still keeps (see
//   BlobRelocationLiveSet below). Each input has its own live set.
// - Record bytes: the bytes of one value's record in its file. In a normal
//   blob file they are the blob log record header, the user key and the
//   stored value. In a relocation file they are the stored value plus the
//   5-byte trailer. The record bytes of an input are the sum over all its
//   values.
// - Stored CRC: the CRC32C of the stored (maybe compressed) bytes of a value
//   plus its compression byte.
// - Sink: an object that receives the references of a batch read in a form it
//   chooses (BlobReferenceSink in table/table_reader.h). A live set is a sink.
// - Location block: a data block of a relocation file. It holds location
//   entries (where each value is in the file), not values.
// - Coalesced read: one read that covers several neighbor values and the bytes
//   between them (see ReadCoalescedRanges() in db/blob/blob_gen2_format.h).
//
// A GC job uses the parts in this order:
//
//   census totals --> LiveSet::Reserve()           charge the memory first
//   each SST      --> ReadBlobReferences()         fill the live set (a sink)
//   each live set --> LiveSet::Finalize()          sort, merge, origin runs
//   all inputs    --> PlanBlobRelocationPacking()  inputs -> outputs
//   each output   --> MultiGetStored*(), Stream*() read the stored bytes and
//                                                  check the stored CRC
//                 --> AddBlobRelocation()          write each record
//
// The census totals are the reference counts that the census (see
// db/blob/blob_reference_census.h) keeps for the input's generation numbers.
// They give the size of the live set before any SST is read. LiveSet is short
// for BlobRelocationLiveSet. Origin runs are described at
// BlobRelocationOriginRun below.
//
// Indirect blob GC job
//
// Background: compaction-time blob GC (enable_blob_garbage_collection) moves a
// value only when compaction rewrites the SST that references it. So a blob
// file that is mostly garbage can stay as long as its SSTs are not compacted.
// Indirect blob GC is a background job that is not part of any compaction. It
// runs on the same LOW-pool workers as compaction (see "Job steps").
//
// One job has 1 to kIndirectBlobGCMaxInputs (8) inputs of one column family,
// and writes at most kIndirectBlobGCMaxOutputs (8) outputs.
//
// Job terms (the terms above also apply):
// - Stale job: the job found that an input or the census changed while it ran.
//   The causes are: the census was not ready or was invalidated before the
//   job took its census snapshot, a normal input left the Version or its
//   metadata changed, an input became pinned by an SST without exact
//   reference metadata (a coarse SST), the census disagrees with the SSTs,
//   the column family was dropped, or the job's census registration ended.
//   The coarse pin can appear during a job: a flush takes its blob file
//   number before GC selects the input, and installs its coarse SST after.
//   A stale job installs nothing. It returns TryAgain, and
//   BackgroundCompaction() turns that into Busy and enqueues the column
//   family again. Two states that the GC rules exclude are invariant checks
//   instead (an assert in debug builds, a WARN line and a stale job in
//   release builds): a relocation input that changed, and a live input
//   without an output.
//   (A coarse SST pins every indirect-origin file number at or above its
//   frontier: GC must treat each such file number as used. See "Coarse SST"
//   in db/blob/blob_reference_metadata.h.)
// - Input Version: the Version that the job holds a reference to when it
//   starts. GC reads the inputs, their routes and the SST list from it.
// - Candidate-local failure: a failure that affects only one input, such as
//   Corruption in the input's records or in the exact SST metadata. GC then
//   suppresses that input and sets no DB-wide background error.
// - Deferred input: an input that this job leaves for a later job because it
//   does not fit. When the inputs need more than 8 outputs or the disk space
//   check fails, the largest input waits first. When the GC memory budget is
//   used by earlier inputs, the input that does not fit and all later inputs
//   wait.
// - Census entry: the SSTs that hold references with one generation number,
//   and their totals. See db/blob/blob_reference_census.h.
// - Census snapshot: GC's copy of the census entries of the inputs'
//   generation numbers at the input Version (BlobReferenceCensusState). The
//   job does not update it with the Version changes made while it runs (see
//   "Install protocol" in DBImpl::IndirectBlobGCJob::Install()).
// - Census heap: the per-column-family list of GC candidates that the census
//   ranks by garbage ratio (BlobGcCandidateHeap in
//   db/blob/blob_reference_census.h).
// - Blob file retirement (kBlobFileRetirement): a MANIFEST record that removes
//   a collected normal blob file (see db/blob/blob_relocation_manifest.h).
//
// Job steps (step 1 is in BackgroundCompaction(); steps 2-8 are in
// DBImpl::IndirectBlobGCJob, which DBImpl::RunIndirectBlobGCBatch() runs):
// 1. BackgroundCompaction() takes up to 8 candidates that the census publishes
//    to the Version
//    (VersionStorageInfo::GetBlobPhysicalFilesForIndirectBlobGC()), on a
//    LOW-pool worker that found no LSM compaction.
// 2. Plan. GC takes the census snapshot
//    (BlobReferenceCensus::SnapshotSelected()) and reserves each live set from
//    the census reference counts.
// 3. Read. GC reads the needed SSTs one at a time. For each SST it gets a
//    reader with TableCache::GetBlobReferenceReader() (the SST's table cache
//    reader, or an ephemeral reader on a miss) and reads all needed generation
//    numbers in one batch (TableReader::ReadBlobReferences()) into the live
//    sets.
// 4. Check. GC compares the SSTs and totals that it found with the census
//    snapshot. On a mismatch, the census rebuilds once
//    (BlobReferenceCensus::ReportMismatch()).
// 5. Decide. Each input is relocated, gets only its removal (it has no live
//    value), or is suppressed. GC relocates an input only if its exact garbage
//    is at least 50% of its record bytes. The 50% threshold is a fixed choice,
//    not measured.
// 6. Pack. GC puts whole inputs into outputs, largest first
//    (PlanBlobRelocationPacking()).
// 7. Copy. GC copies the stored bytes of each live value into its output and
//    checks each value with its stored CRC.
// 8. Install. GC builds one MANIFEST edit from its census snapshot and
//    installs it with one LogAndApply() call. When the edit is first in the
//    MANIFEST writer queue, GC checks under the DB mutex that the job is still
//    registered with the census, that each input is unchanged, and that no
//    coarse SST pins an input (see "Stale job"). The edit adds the
//    outputs and routes generation numbers to them:
//    - A relocation input that gets an output routes all its generation
//      numbers to that output, held or not.
//    - A normal input routes its file number (its only generation number) to
//      its output only when that number is live.
//    - An input without an output loses every route into it. None of its
//      generation numbers can be live (an invariant check; see "Stale job").
//    Then the edit writes the blob file retirement of each normal input, and
//    deletes each relocation input.
//
// Suppression: GC skips an input that it cannot or should not collect. It
// suppresses the input at two levels:
// - In the census heap (BlobReferenceCensus::SuppressWorkerCandidates()), so
//   that later Versions do not select it again. A normal suppression lasts
//   until the file changes (its total bytes, its live bytes, its
//   is_indirect_origin or is_lost flag, or its coarse pin) or until the next
//   census rebuild (see BlobGcCandidateHeap::Suppress()). A rise of the coarse
//   frontier also brings back every suppressed relocation file above the old
//   frontier, whatever the reason for its suppression, because the heap does
//   not store the reason (see
//   BlobGcCandidateHeap::SetCoarseIndirectOriginFrontier()). A transient
//   suppression lasts until a retry delay passes. The delay starts at 1 s and
//   doubles after each failure, up to 32 s (fixed choices, not measured): a
//   short failure retries soon, and a lasting failure costs at most one attempt
//   every 32 s (see BlobGcCandidateHeap::SuppressTransient()). Each census
//   suppression carries a stamp (BlobGcCandidateStamp) taken at the input
//   Version. The census ignores a stamp from an older census worker, or from an
//   older revision of the file. A file gets a new revision when it changes.
// - In the current Version
//   (VersionStorageInfo::SuppressBlobFileForIndirectBlobGC()), so that the next
//   pick in this Version takes other candidates. When the job ends without
//   error and no input is left to install, GC does this at once. When the job
//   installs, GC does this only if no other Version was installed during the
//   job, because a newer Version can hold SST metadata that makes the input
//   eligible. A stale or failed job does not suppress anything in the Version.
//   When a job fails with CompactionTooLarge and the Version did not change,
//   the deferred inputs move to the end of the Version's candidate list
//   (VersionStorageInfo::DeferBlobFilesForIndirectBlobGC()).
//
// The main reasons, and the kind of census suppression for each (see
// DBImpl::IndirectBlobGCJob for the full list):
//
//   Reason                                           Census suppression
//   -----------------------------------------------  ------------------
//   invalid input metadata                           normal
//   table format is not block-based                  normal
//   a coarse SST pins a generation number of the     normal
//     input
//   census mismatch remains after a rebuild          normal
//   corrupt exact SST metadata                       normal
//   exact live bytes above the input's record bytes  normal
//   a lost relocation file that SSTs still           normal
//     reference
//   enable_blob_files, enable_blob_indirection or    normal
//     enable_blob_garbage_collection is off in the
//     latest options of the column family, and the
//     input has live values
//   exact garbage below 50%                          normal
//   the output size estimate overflows               normal
//   outputs would not be smaller than the inputs     normal, for every
//                                                      eligible input
//   an output file is larger than its inputs         normal, for the first
//                                                      input of that output
//   the input cannot be opened for relocation        normal
//     (Corruption, MemoryLimit or NotSupported), or
//     a source value is corrupt
//   live set over the 64 MB GC memory budget, and    transient
//     no earlier input holds a share
//   census snapshot cannot be charged to the block   transient, for every
//     cache, or the GC read and write buffers        input of the job
//     cannot be charged
//   not enough disk space even for one input (the    transient, for every
//     job returns CompactionTooLarge)                input the job deferred
//
// 64 MB is BlobRelocationMemoryTracker::kMaxBytes. Output, I/O and MANIFEST
// errors are not candidate-local. They use the normal background-error
// handling.

// Bounds the memory of one indirect blob GC job, so that many inputs together
// cannot use more than kMaxBytes (64 MB, an estimate). The budget is charged to
// the block cache. Each user (for example, the live set of one input) keeps its
// own capacity, but updates this total reservation before it grows and when it
// releases its memory.
//
// In a GC job, the live sets of all inputs and the GC buffers (I/O workspace,
// SST readers, group reads) share one tracker (see DBImpl::IndirectBlobGCJob).
class BlobRelocationMemoryTracker {
 public:
  explicit BlobRelocationMemoryTracker(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager);

  // Changes one user's share of the budget. `*current_bytes` is that user's
  // current share. On success, the total becomes total - *current_bytes +
  // requested_bytes, and *current_bytes is set to requested_bytes. Pass 0 to
  // release the share. Returns MemoryLimit if the new total would be more than
  // kMaxBytes, the block cache error if the charge fails, and Corruption if
  // *current_bytes is larger than the total.
  Status SetReservation(size_t* current_bytes, size_t requested_bytes);

  size_t reserved_bytes() const { return reserved_bytes_; }

 private:
  static constexpr size_t kMaxBytes = 64 * 1024 * 1024;

  std::shared_ptr<CacheReservationManager> cache_reservation_manager_;
  size_t reserved_bytes_ = 0;
};

// One live value of an indirect blob GC input's live set. stable_checksum is
// the logical CRC (CRC32C of the uncompressed value). stored_checksum is the
// stored CRC (CRC32C of the stored bytes plus the compression byte).
struct BlobRelocationLiveValue {
  uint64_t origin_file_number = 0;
  uint64_t original_value_offset = 0;
  uint32_t stored_value_size = 0;
  // Record bytes minus stored size: the record header and key, or trailer.
  uint32_t record_overhead = 0;
  uint32_t stable_checksum = 0;
  uint32_t stored_checksum = 0;
  CompressionType compression = kNoCompression;

  uint64_t record_bytes() const {
    return uint64_t{stored_value_size} + record_overhead;
  }
};
static_assert(sizeof(BlobRelocationLiveValue) <= 40,
              "A live value is 40 bytes");

// An origin run: the values of a finalized live set that have one origin,
// values [begin, end). An output stores the values of one origin run together.
// It is not the same as a run of the reference metadata (see "run" in
// db/blob/blob_reference_metadata.h).
struct BlobRelocationOriginRun {
  uint64_t origin_file_number = 0;
  size_t begin = 0;
  size_t end = 0;
};

// Exact live set of one indirect blob GC input, charged to the GC memory
// budget. It is read from the SST references of the input's generation numbers
// `generation_numbers` (sorted, unique): the input's own file number and every
// old file number routed to it. Each appended reference is checked: its
// generation number must be one of `generation_numbers`, and its encoding class
// must match its generation number (unflattened if and only if generation
// number == origin). It is then kept as a 40-byte BlobRelocationLiveValue.
class BlobRelocationLiveSet : public BlobReferenceSink {
 public:
  // Makes a private memory tracker, for a live set used alone (tests).
  BlobRelocationLiveSet(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager,
      std::vector<uint64_t> generation_numbers);

  // Uses a shared memory tracker, so that several live sets share one 64 MB
  // budget. A GC job uses this constructor for all its live sets.
  BlobRelocationLiveSet(
      std::shared_ptr<BlobRelocationMemoryTracker> memory_tracker,
      std::vector<uint64_t> generation_numbers);

  ~BlobRelocationLiveSet() override;
  BlobRelocationLiveSet(const BlobRelocationLiveSet&) = delete;
  BlobRelocationLiveSet& operator=(const BlobRelocationLiveSet&) = delete;
  BlobRelocationLiveSet(BlobRelocationLiveSet&&) = delete;
  BlobRelocationLiveSet& operator=(BlobRelocationLiveSet&&) = delete;

  // Reserves charged capacity for `additional` more values.
  Status Reserve(size_t additional);

  // Calls Reserve() once, then Append() for each reference.
  Status Add(const std::vector<BlobReference>& references);

  // Sorts by (origin, original offset) and merges duplicates of one identity
  // (the same value can be referenced under several generation numbers).
  // Copies must agree on size, compression and checksums.
  //
  // Copies may differ in record_overhead, and only in a relocation input. An
  // unflattened reference (generation number = origin) reaches the input
  // through a route from its normal blob file, so it carries the record size
  // of that file (the blob log record header and the user key). A flattened
  // reference carries the size of a relocation file record trailer (5 bytes).
  // The value really lives in a relocation file, so the trailer size is the
  // correct one. It is always the smaller one, because a blob log record
  // header alone is 32 bytes. So Finalize() keeps the smallest
  // record_overhead.
  Status Finalize();

  // Frees the values and releases their share of the GC memory budget.
  Status ReleaseWorkingMemory();

  // BlobReferenceSink.
  size_t size() const override { return values_.size(); }
  size_t Room() const override { return values_.capacity() - values_.size(); }
  Status Append(const BlobReference& reference) override;
  void Truncate(size_t size) override { values_.resize(size); }

  const std::vector<BlobRelocationLiveValue>& references() const {
    return values_;
  }

  // After Finalize(): one run per origin, in origin order.
  const std::vector<BlobRelocationOriginRun>& origin_runs() const {
    return origin_runs_;
  }
  uint64_t live_record_bytes() const { return live_record_bytes_; }
  uint64_t live_stored_value_bytes() const { return live_stored_value_bytes_; }

  size_t TEST_ReservedMemory() const { return ReservedMemory(); }

 private:
  Status UpdateReservation();
  size_t ReservedMemory() const;

  std::shared_ptr<BlobRelocationMemoryTracker> memory_tracker_;
  const std::vector<uint64_t> generation_numbers_;
  std::vector<BlobRelocationLiveValue> values_;
  std::vector<BlobRelocationOriginRun> origin_runs_;
  size_t charged_memory_ = 0;
  uint64_t live_record_bytes_ = 0;
  uint64_t live_stored_value_bytes_ = 0;
  bool finalized_ = false;
};

// One whole indirect blob GC input to pack.
struct BlobRelocationPackingItem {
  uint64_t input_file_number = 0;

  // The expected size of the input's values in an output. The planner only
  // compares and adds these values. The indirect blob GC job estimates them
  // from the live record bytes and the live value count of the input (see
  // EstimateIndirectBlobGCPartitionBytes() in
  // db/db_impl/db_impl_compaction_flush.cc).
  uint64_t estimated_bytes = 0;

  // The origins of the input's live values, sorted and unique: the origin of
  // each origin run of the input's finalized live set.
  std::vector<uint64_t> origins;
};

// One planned output ("bin" means output here): the inputs that go into one
// relocation file, as indexes into `items` of PlanBlobRelocationPacking(), and
// the sum of their estimated_bytes.
struct BlobRelocationPackingBin {
  std::vector<size_t> item_indexes;
  uint64_t estimated_bytes = 0;
};

// Packs whole inputs into outputs, deterministically. Inputs are sorted by
// estimated size, largest first. Each goes to the first output that has room
// and shares no origin with it. Each input goes to exactly one output and is
// never split. An input larger than `target_output_bytes` gets an output of its
// own. Two inputs that hold the same origin (possible after RepairDB) never
// share an output, since an output holds one entry per (origin, original
// offset). Returns MemoryLimit if the inputs need more than `max_outputs`
// outputs. Here MemoryLimit means "over a resource limit", not a memory error.
// The indirect blob GC job then defers the largest input and plans again (see
// "Deferred input" above).
Status PlanBlobRelocationPacking(
    const std::vector<BlobRelocationPackingItem>& items,
    uint64_t target_output_bytes, size_t max_outputs,
    std::vector<BlobRelocationPackingBin>* bins);

}  // namespace ROCKSDB_NAMESPACE
