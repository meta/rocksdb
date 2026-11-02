// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_relocation_gc.h"

#include <algorithm>
#include <iterator>
#include <limits>
#include <tuple>
#include <utility>

#include "cache/cache_reservation_manager.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr size_t kMaxBlobRelocationLiveSetBytes = 64 * 1024 * 1024;

auto StableId(const BlobReference& reference) {
  return std::tie(reference.origin_file_number,
                  reference.original_value_offset);
}

auto ImmutableFields(const BlobReference& reference) {
  return std::tie(reference.stored_value_size, reference.record_bytes,
                  reference.compression, reference.stable_checksum_version,
                  reference.stable_checksum);
}

}  // namespace

BlobRelocationLiveSet::BlobRelocationLiveSet(
    std::shared_ptr<CacheReservationManager> cache_reservation_manager)
    : cache_reservation_manager_(std::move(cache_reservation_manager)) {}

BlobRelocationLiveSet::~BlobRelocationLiveSet() {
  ReleaseWorkingMemory().PermitUncheckedError();
}

Status BlobRelocationLiveSet::Reserve(size_t additional) {
  if (additional == 0) {
    return Status::OK();
  }
  if (additional > references_.max_size() - references_.size()) {
    return Status::MemoryLimit("Blob relocation live set is too large");
  }
  const size_t target = references_.size() + additional;
  if (target <= references_.capacity()) {
    return Status::OK();
  }
  size_t capacity = references_.capacity() == 0 ? 1 : references_.capacity();
  while (capacity < target) {
    if (capacity > references_.max_size() / 2) {
      capacity = target;
      break;
    }
    capacity *= 2;
  }
  if (capacity > std::numeric_limits<size_t>::max() / sizeof(BlobReference)) {
    return Status::MemoryLimit("Blob relocation live set overflow");
  }
  if (capacity * sizeof(BlobReference) > kMaxBlobRelocationLiveSetBytes) {
    return Status::MemoryLimit("Blob relocation live set exceeds limit");
  }
  if (cache_reservation_manager_ != nullptr) {
    Status status = cache_reservation_manager_->UpdateCacheReservation(
        capacity * sizeof(BlobReference));
    if (!status.ok()) {
      return status;
    }
  }
  references_.reserve(capacity);
  return UpdateReservation();
}

Status BlobRelocationLiveSet::Add(std::vector<BlobReference> references) {
  std::vector<BlobReference>* output = nullptr;
  Status status = PrepareAppend(references.size(), &output);
  if (!status.ok()) {
    return status;
  }
  output->insert(output->end(), std::make_move_iterator(references.begin()),
                 std::make_move_iterator(references.end()));
  return UpdateReservation();
}

Status BlobRelocationLiveSet::PrepareAppend(
    size_t additional, std::vector<BlobReference>** references) {
  if (references == nullptr) {
    return Status::InvalidArgument("Null blob relocation live set output");
  }
  if (finalized_) {
    return Status::InvalidArgument("Blob relocation live set is finalized");
  }
  Status status = Reserve(additional);
  if (!status.ok()) {
    return status;
  }
  *references = &references_;
  return Status::OK();
}

Status BlobRelocationLiveSet::Finalize(
    const BlobRelocationRouteSource& expected_source) {
  if (finalized_) {
    return Status::InvalidArgument("Blob relocation live set is finalized");
  }
  finalized_ = true;
  std::sort(references_.begin(), references_.end(),
            [](const BlobReference& lhs, const BlobReference& rhs) {
              return std::tie(lhs.origin_file_number, lhs.original_value_offset,
                              lhs.stored_value_size, lhs.record_bytes,
                              lhs.compression, lhs.stable_checksum_version,
                              lhs.stable_checksum) <
                     std::tie(rhs.origin_file_number, rhs.original_value_offset,
                              rhs.stored_value_size, rhs.record_bytes,
                              rhs.compression, rhs.stable_checksum_version,
                              rhs.stable_checksum);
            });

  size_t output = 0;
  for (size_t i = 0; i < references_.size(); ++i) {
    BlobReference& reference = references_[i];
    if (!reference.HasStableIdentity() ||
        reference.generation_file_number !=
            expected_source.generation_file_number ||
        reference.origin_file_number != expected_source.origin_file_number ||
        reference.record_bytes < reference.stored_value_size) {
      return Status::Corruption("Unexpected blob relocation reference");
    }
    if (output != 0 &&
        StableId(references_[output - 1]) == StableId(reference)) {
      if (ImmutableFields(references_[output - 1]) !=
          ImmutableFields(reference)) {
        return Status::Corruption(
            "Conflicting metadata for one stable blob identity");
      }
      continue;
    }
    if (reference.record_bytes >
            std::numeric_limits<uint64_t>::max() - live_record_bytes_ ||
        reference.stored_value_size >
            std::numeric_limits<uint64_t>::max() - live_stored_value_bytes_) {
      return Status::Corruption("Blob relocation live byte overflow");
    }
    live_record_bytes_ += reference.record_bytes;
    live_stored_value_bytes_ += reference.stored_value_size;
    if (output != i) {
      references_[output] = std::move(reference);
    }
    ++output;
  }
  references_.resize(output);
  return UpdateReservation();
}

Status BlobRelocationLiveSet::ReleaseWorkingMemory() {
  std::vector<BlobReference>().swap(references_);
  if (cache_reservation_manager_ == nullptr) {
    return Status::OK();
  }
  return cache_reservation_manager_->UpdateCacheReservation(0);
}

size_t BlobRelocationLiveSet::ReservedMemory() const {
  return references_.capacity() * sizeof(BlobReference);
}

Status BlobRelocationLiveSet::UpdateReservation() {
  if (cache_reservation_manager_ == nullptr) {
    return Status::OK();
  }
  return cache_reservation_manager_->UpdateCacheReservation(ReservedMemory());
}

}  // namespace ROCKSDB_NAMESPACE
