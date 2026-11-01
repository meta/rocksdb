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

// Accounted exact live set for one or more route sources. D6 uses one source;
// D7 can reuse the same collector while packing several origin partitions.
class BlobRelocationLiveSet {
 public:
  explicit BlobRelocationLiveSet(
      std::shared_ptr<CacheReservationManager> cache_reservation_manager);
  ~BlobRelocationLiveSet();
  BlobRelocationLiveSet(const BlobRelocationLiveSet&) = delete;
  BlobRelocationLiveSet& operator=(const BlobRelocationLiveSet&) = delete;

  // Reserves accounted space for `additional` entries and returns the vector
  // that a metadata reader may append to. The caller must not append more
  // than `additional` entries.
  Status PrepareAppend(size_t additional,
                       std::vector<BlobReference>** references);
  Status Add(std::vector<BlobReference> references);
  Status Finalize(const BlobRelocationRouteSource& expected_source);
  Status ReleaseWorkingMemory();

  const std::vector<BlobReference>& references() const { return references_; }
  uint64_t live_record_bytes() const { return live_record_bytes_; }
  uint64_t live_stored_value_bytes() const { return live_stored_value_bytes_; }

  size_t TEST_ReservedMemory() const { return ReservedMemory(); }

 private:
  Status Reserve(size_t additional);
  Status UpdateReservation();
  size_t ReservedMemory() const;

  std::shared_ptr<CacheReservationManager> cache_reservation_manager_;
  std::vector<BlobReference> references_;
  uint64_t live_record_bytes_ = 0;
  uint64_t live_stored_value_bytes_ = 0;
  bool finalized_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
