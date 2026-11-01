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

namespace ROCKSDB_NAMESPACE {

class CacheReservationManager;

// Shares one bounded, cache-charged memory budget across all origin live sets
// in a packing job. Individual vectors retain their own capacity but update
// this aggregate reservation before growing and when releasing it.
class BlobRelocationMemoryTracker {
 public:
  explicit BlobRelocationMemoryTracker(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager);

  Status SetReservation(size_t* current_bytes, size_t requested_bytes);
  size_t reserved_bytes() const { return reserved_bytes_; }

 private:
  static constexpr size_t kMaxBytes = 64 * 1024 * 1024;

  std::shared_ptr<CacheReservationManager> cache_reservation_manager_;
  size_t reserved_bytes_ = 0;
};

// Accounted exact live set for one or more route sources. D6 uses one source;
// D7 can reuse the same collector while packing several origin partitions.
class BlobRelocationLiveSet {
 public:
  explicit BlobRelocationLiveSet(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager);
  explicit BlobRelocationLiveSet(
      std::shared_ptr<BlobRelocationMemoryTracker> memory_tracker);
  ~BlobRelocationLiveSet();
  BlobRelocationLiveSet(const BlobRelocationLiveSet&) = delete;
  BlobRelocationLiveSet& operator=(const BlobRelocationLiveSet&) = delete;
  BlobRelocationLiveSet(BlobRelocationLiveSet&&) = delete;
  BlobRelocationLiveSet& operator=(BlobRelocationLiveSet&&) = delete;

  // Reserves accounted space for `additional` entries and returns the vector
  // that a metadata reader may append to. The caller must not append more
  // than `additional` entries.
  Status PrepareAppend(size_t additional,
                       std::vector<BlobReference>** references);
  Status Add(std::vector<BlobReference> references);
  Status Finalize(const BlobRelocationRouteSource& expected_source);
  Status Finalize(
      uint64_t expected_origin,
      const std::vector<BlobRelocationRouteSource>& expected_sources);
  Status ReleaseWorkingMemory();

  const std::vector<BlobReference>& references() const { return references_; }
  uint64_t live_record_bytes() const { return live_record_bytes_; }
  uint64_t live_stored_value_bytes() const { return live_stored_value_bytes_; }

  size_t TEST_ReservedMemory() const { return ReservedMemory(); }

 private:
  Status Reserve(size_t additional);
  Status UpdateReservation();
  size_t ReservedMemory() const;

  std::shared_ptr<BlobRelocationMemoryTracker> memory_tracker_;
  std::vector<BlobReference> references_;
  size_t charged_memory_ = 0;
  uint64_t live_record_bytes_ = 0;
  uint64_t live_stored_value_bytes_ = 0;
  bool finalized_ = false;
};

struct BlobRelocationPackingItem {
  uint64_t origin_file_number = 0;
  uint64_t source_generation_file_number = 0;
  uint64_t estimated_bytes = 0;
};

struct BlobRelocationPackingBin {
  std::vector<size_t> item_indexes;
  uint64_t estimated_bytes = 0;
};

// Deterministic whole-partition first-fit-decreasing packing. Each item is
// assigned to exactly one output and is never split.
Status PlanBlobRelocationPacking(
    const std::vector<BlobRelocationPackingItem>& items,
    uint64_t target_output_bytes, uint64_t hard_output_bytes,
    size_t max_origins_per_output, size_t max_outputs,
    std::vector<BlobRelocationPackingBin>* bins);

}  // namespace ROCKSDB_NAMESPACE
