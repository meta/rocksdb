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

// Indirect blob GC
//
// Background: compaction-time blob GC (enable_blob_garbage_collection) moves a
// value only when compaction rewrites the SST that references it. So a blob
// file that has a lot of garbage can stay as long as its SSTs are not
// compacted. Indirect blob GC is a background job that is not part of any
// compaction. It copies the live values of blob files that have a lot of
// garbage into new relocation files, and rewrites no SST. It runs on the same
// LOW-pool workers as compaction (see "Job steps").
//
// Terms:
// - Input: a file that one GC job collects. An input is a normal blob file or a
//   relocation file. A normal input is an indirect-origin blob file: a normal
//   blob file written with enable_blob_indirection on
//   (BlobFileMetaData::IsIndirectOrigin()), so it is the origin of
//   kIndirectBlob references.
// - Relocation file: a file that indirect blob GC writes. It holds live values
//   copied out of other blob files (see db/blob/blob_relocation_file.h).
// - Route: a MANIFEST record that maps an old file number to the relocation
//   file that now holds its live values (see
//   db/blob/blob_relocation_manifest.h).
// - Generation numbers of an input: the file numbers that SSTs use to reference
//   the input's live values. They are the input's own file number and, for a
//   relocation file, every old file number that has a route to it.
// - Output: a relocation file that the job writes.
// - Live set: the references to one input that some SST still keeps (see
//   BlobRelocationLiveSet below). Each input has its own live set.
// - Origin run: the values of a finalized live set that have one origin. The
//   origin of a value is the blob file where the value was first written (see
//   "Stable identity" in db/blob/blob_index.h and BlobRelocationOriginRun
//   below).
// - Record bytes: the bytes of one value's record in its file. In a normal blob
//   file they are the blob log record header, the user key and the stored
//   value. In a relocation file they are the stored value plus the 5-byte
//   trailer. The record bytes of an input are the sum over all its values.
// - Stored CRC: the CRC32C of the stored (maybe compressed) bytes of a value
//   plus its compression byte.
// - Sink: an object that receives the references of a batch read in a form it
//   chooses (BlobReferenceSink in table/table_reader.h). A live set is a sink.
// - Location block: a data block of a relocation file. It holds location
//   entries (where each value is in the file), not values.
// - Coalesced read: one read that covers several neighbor values and the bytes
//   between them (see ReadCoalescedRanges() in db/blob/blob_gen2_format.h).
// - Input Version: the Version that the job holds a reference to when it
//   starts. GC reads the inputs, their routes and the SST list from it.
// - Census entry: the SSTs that hold references with one generation number, and
//   their totals. The census totals are the reference counts in the census
//   entries of an input's generation numbers. They give the size of the live
//   set before any SST is read. See db/blob/blob_reference_census.h.
// - Census snapshot: GC's copy of the census entries of the inputs' generation
//   numbers at the input Version (BlobReferenceCensusState). The job does not
//   update it with the Version changes made while it runs (see "Install
//   protocol" in DBImpl::IndirectBlobGCJob::Install()).
// - Census heap: the per-column-family list of GC candidates that the census
//   ranks by garbage ratio (BlobGcCandidateHeap in
//   db/blob/blob_reference_census.h).
// - Coarse SST: an SST without exact reference metadata. It pins every
//   indirect-origin file number at or above its frontier: GC must treat each
//   such file number as used. See "Coarse SST" in
//   db/blob/blob_reference_metadata.h.
// - GC memory budget: the limit on the working memory of one job,
//   BlobRelocationMemoryTracker::kMaxBytes (64 MB). The live sets of all inputs
//   and the GC buffers (I/O workspace, SST readers and group reads) share it
//   (see BlobRelocationMemoryTracker below).
// - Stale job: the job found that an input or the census changed while it ran.
//   The causes are: the census was not ready or was invalidated before the job
//   took its census snapshot, a normal input left the Version or its metadata
//   changed, a coarse SST started to pin an input, the census disagrees with
//   the SSTs, the column family was dropped, or the job's census registration
//   ended. The coarse pin can appear during a job: a flush takes its blob file
//   number before GC selects the input, and installs its coarse SST after. A
//   stale job installs nothing. It returns TryAgain, and BackgroundCompaction()
//   turns that into Busy and enqueues the column family again. Two states that
//   the GC rules exclude are invariant checks instead (an assert in debug
//   builds, a WARN line and a stale job in release builds): a relocation input
//   that changed, and a live input without an output.
// - Candidate-local failure: a failure that affects only one input, such as
//   corrupt exact SST metadata, or a corrupt record in the input when
//   paranoid_checks is false. GC then suppresses that input and sets no DB-wide
//   background error (see "Corruption" below).
// - Deferred input: an input that this job leaves for a later job because it
//   does not fit. When the inputs need more than 8 outputs or the disk space
//   check fails, the largest input waits first. When the GC memory budget is
//   used by earlier inputs, the input that does not fit and all later inputs
//   wait.
// - Blob file retirement (kBlobFileRetirement): a MANIFEST record that removes
//   a collected normal blob file (see db/blob/blob_relocation_manifest.h).
//
// Job shape: one job has 1 to kIndirectBlobGCMaxInputs (8) inputs of one column
// family, and writes at most kIndirectBlobGCMaxOutputs (8) outputs. The
// comments on these constants in db/db_impl/db_impl_compaction_flush.cc explain
// the limits.
//
// Invariant: every live value of one input goes to the same output. A route
// maps one old file number to one relocation file. So all the values that SSTs
// reference through one generation number must be in one output. When GC keeps
// each input whole, the values of each of its generation numbers stay in one
// output. PlanBlobRelocationPacking() never splits an input. An input whose
// estimated output size is larger than the target output size gets an output of
// its own. The target output size is 90% of min(blob_file_size, 1 GB). The
// comment on kIndirectBlobGCMaxOutputBytes in
// db/db_impl/db_impl_compaction_flush.cc explains these values.
//
// Job steps (step 1 is in BackgroundCompaction(); steps 2-8 are in
// DBImpl::IndirectBlobGCJob, which DBImpl::RunIndirectBlobGCBatch() runs):
// 1. BackgroundCompaction() takes up to 8 candidates that the census publishes
//    to the Version
//    (VersionStorageInfo::GetBlobPhysicalFilesForIndirectBlobGC()), on a
//    LOW-pool worker that found no LSM compaction.
// 2. Plan. GC takes the census snapshot
//    (BlobReferenceCensus::SnapshotSelected()). It reserves each live set from
//    the census totals (BlobRelocationLiveSet::Reserve()), so the memory is
//    charged before any SST is read.
// 3. Read. GC reads the needed SSTs one at a time. For each SST it gets a
//    reader with TableCache::GetBlobReferenceReader() (the SST's table cache
//    reader, or an ephemeral reader on a miss) and reads all needed generation
//    numbers in one batch (TableReader::ReadBlobReferences()) into the live
//    sets.
// 4. Check. GC compares the SSTs and totals that it found with the census
//    snapshot. On a mismatch, the census rebuilds once
//    (BlobReferenceCensus::ReportMismatch()).
// 5. Decide. GC finalizes each live set (BlobRelocationLiveSet::Finalize():
//    sort, merge duplicates, build the origin runs). Each input is then
//    relocated, gets only its removal (it has no live value), or is suppressed.
//    GC relocates an input only if its exact garbage divided by its record
//    bytes is at least indirect_blob_gc_garbage_ratio (a column family option;
//    the default is 0.5). At 0.5, GC never copies more live bytes than the
//    garbage bytes it removes.
// 6. Pack. GC puts whole inputs into outputs, largest first
//    (PlanBlobRelocationPacking(); see "Invariant" above). It then checks the
//    disk space with the SstFileManager. When the inputs need more than 8
//    outputs or the space check fails, the largest input waits (see "Deferred
//    input").
// 7. Copy. For each output, GC reads the stored bytes of each live value
//    (BlobFileReader::MultiGetStoredBlobsForRelocation() and
//    StreamStoredBlobForRelocation(), or the BlobRelocationFileReader
//    MultiGetStoredBlobs() and StreamStoredBlob()) and checks each value with
//    its stored CRC. It writes each value into the output with
//    BlockBasedTableBuilder::AddBlobRelocation().
// 8. Install. GC builds one MANIFEST edit from its census snapshot and installs
//    it with one LogAndApply() call. When the edit is first in the MANIFEST
//    writer queue, GC checks under the DB mutex that the job is still
//    registered with the census, that each input is unchanged, and that no
//    coarse SST pins an input (see "Stale job"). The edit adds the outputs and
//    routes generation numbers to them:
//    - A relocation input that gets an output routes all its generation
//      numbers to that output, held or not.
//    - A normal input routes its file number (its only generation number) to
//      its output only when that number is live.
//    - An input without an output loses every route into it. None of its
//      generation numbers can be live (an invariant check; see "Stale job").
//    Then the edit writes the blob file retirement of each normal input, and
//    deletes each relocation input.
//
// Parts: the job is built from five parts that work without the DB. This file
// holds the first three.
// 1. Live set (BlobRelocationLiveSet, below): holds the live references of one
//    input. Steps 2, 3 and 5 use it.
// 2. Memory budget (BlobRelocationMemoryTracker, below): keeps the job within
//    the GC memory budget, from step 2 until the job ends.
// 3. Packing plan (PlanBlobRelocationPacking(), below): puts the inputs into
//    outputs in step 6.
// 4. Batch reference reader (TableReader::ReadBlobReferences() in
//    table/table_reader.h): reads the references of many generation numbers
//    from one SST in one batch (step 3).
// 5. Stored reads and writes (step 7): BlobFileReader and
//    BlobRelocationFileReader in db/blob/blob_file_reader.h read the stored
//    bytes of values. The two BlockBasedTableBuilder::AddBlobRelocation()
//    overloads write them into an output. The streaming overload uses
//    WriteSimpleGen2BlobRecordStream() in db/blob/blob_gen2_format.h.
//
// Suppression: GC skips an input that it cannot or should not collect now, for
// example because its garbage is below indirect_blob_gc_garbage_ratio, a coarse
// SST pins it, or its metadata is corrupt. Without suppression, GC would pick
// the same file again on the next run and make no progress. Suppression takes
// the file out of the candidate list for a while, so GC moves on to other blob
// files. GC records a suppression in two places:
// - In the census candidate heap, so that later Versions do not pick the file
//   (BlobGcCandidateHeap::Suppress()).
// - In the current Version, so that the next pick in this Version takes other
//   candidates (VersionStorageInfo::SuppressBlobFileForIndirectBlobGC()).
// A suppression is one of two kinds:
// - Normal: it lasts until the file changes, the census rebuilds or (for a
//   relocation file) the coarse frontier rises (details at
//   BlobGcCandidateHeap::Suppress()).
// - Transient: it is for a short resource problem (memory budget, cache charge
//   or disk space). It lasts until a retry delay passes
//   (BlobGcCandidateHeap::SuppressTransient()).
// DBImpl::IndirectBlobGCJob lists every reason and its kind.
//
// Corruption: GC handles corrupt blob data like compaction does. Corrupt data
// is a bad record CRC or record format, or a corrupt header or footer when GC
// opens the file, in a blob file or a relocation file. With both settings of
// paranoid_checks, GC suppresses the input, writes an ERROR line to the info
// log and counts INDIRECT_BLOB_GC_CORRUPT_INPUTS. With paranoid_checks true,
// the job also fails with Corruption, and the caller sets a background error
// (reason kCompaction). Corrupt exact SST metadata, and a census mismatch that
// remains after a census rebuild, never set a background error, because the
// user data is not damaged. In these cases GC also suppresses the input, writes
// an ERROR line and counts the same ticker.

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
// own. As a defensive check, two inputs that hold the same origin never share
// an output, since an output holds one entry per (origin, original offset).
// Returns MemoryLimit if the inputs need more than `max_outputs` outputs. Here
// MemoryLimit means "over a resource limit", not a memory error. The indirect
// blob GC job then defers the largest input and plans again (see "Deferred
// input" above).
Status PlanBlobRelocationPacking(
    const std::vector<BlobRelocationPackingItem>& items,
    uint64_t target_output_bytes, size_t max_outputs,
    std::vector<BlobRelocationPackingBin>* bins);

}  // namespace ROCKSDB_NAMESPACE
