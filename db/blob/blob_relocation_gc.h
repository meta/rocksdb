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
// Indirect blob GC copies the live values of blob files that have a lot of
// garbage into new relocation files, and rewrites no SST. One GC job collects a
// few files of one column family. A job is built from five parts that work
// without the DB. This file holds the first three:
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

// Bounds the memory of one indirect blob GC job, so that many inputs together
// cannot use more than kMaxBytes (64 MB, an estimate). The budget is charged to
// the block cache. Each user (for example, the live set of one input) keeps its
// own capacity, but updates this total reservation before it grows and when it
// releases its memory.
//
// Limit: a live set charges 40 bytes per reference (see
// BlobRelocationLiveValue), and it reserves room for every SST's copy of a
// reference before the copies are merged. When the caller reserves the full
// census total at once (as indirect blob GC does), an input with more than
// about 1.68 million references (64 MB / 40 bytes) never fits in the budget,
// even alone, and Reserve() returns MemoryLimit for it.
//
// Threads: the tracker is not thread-safe. All its users must run on one
// thread, or the caller must synchronize them. One GC job uses one tracker
// on one thread.
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
  // budget.
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
  // Record bytes: in a relocation input, every value is in the relocation
  // file, so its record is the stored value plus the 5-byte trailer
  // (kSimpleGen2BlobTrailerSize). A flattened reference already carries that
  // size. An unflattened reference (generation number = origin) reaches the
  // input through a route from its old normal blob file, so it carries the
  // record size of that file (the blob log record header and the user key).
  // Append() replaces that size with the trailer size when the input has more
  // than one generation number, which only a relocation input has. So copies
  // of one value agree on record_overhead, and live_record_bytes() is the
  // size of the values in the input file. If copies still differed, Finalize()
  // would keep the smallest record_overhead.
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
  // compares and adds these values. The caller chooses how to estimate them,
  // for example from live_record_bytes() of the input's live set.
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
// MemoryLimit means "over a resource limit", not a memory error. The caller can
// then plan again with fewer inputs.
Status PlanBlobRelocationPacking(
    const std::vector<BlobRelocationPackingItem>& items,
    uint64_t target_output_bytes, size_t max_outputs,
    std::vector<BlobRelocationPackingBin>* bins);

}  // namespace ROCKSDB_NAMESPACE
