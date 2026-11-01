// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_relocation_gc.h"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <limits>
#include <tuple>
#include <utility>

#include "cache/cache_reservation_manager.h"

namespace ROCKSDB_NAMESPACE {

namespace {

auto StableId(const BlobReference& reference) {
  return std::tie(reference.origin_file_number,
                  reference.original_value_offset);
}

auto ImmutableFields(const BlobReference& reference) {
  return std::tie(reference.stored_value_size, reference.record_bytes,
                  reference.compression, reference.stable_checksum_version,
                  reference.stable_checksum, reference.stored_checksum);
}

}  // namespace

BlobRelocationMemoryTracker::BlobRelocationMemoryTracker(
    std::shared_ptr<CacheReservationManager> cache_reservation_manager)
    : cache_reservation_manager_(std::move(cache_reservation_manager)) {}

Status BlobRelocationMemoryTracker::SetReservation(size_t* current_bytes,
                                                   size_t requested_bytes) {
  if (current_bytes == nullptr || *current_bytes > reserved_bytes_) {
    return Status::Corruption("Invalid blob relocation memory reservation");
  }
  const size_t retained = reserved_bytes_ - *current_bytes;
  if (requested_bytes > kMaxBytes - retained) {
    return Status::MemoryLimit("Blob relocation live sets exceed limit");
  }
  const size_t target = retained + requested_bytes;
  if (cache_reservation_manager_ != nullptr) {
    Status status = cache_reservation_manager_->UpdateCacheReservation(target);
    if (!status.ok()) {
      return status;
    }
  }
  reserved_bytes_ = target;
  *current_bytes = requested_bytes;
  return Status::OK();
}

BlobRelocationLiveSet::BlobRelocationLiveSet(
    std::shared_ptr<CacheReservationManager> cache_reservation_manager)
    : BlobRelocationLiveSet(std::make_shared<BlobRelocationMemoryTracker>(
          std::move(cache_reservation_manager))) {}

BlobRelocationLiveSet::BlobRelocationLiveSet(
    std::shared_ptr<BlobRelocationMemoryTracker> memory_tracker)
    : memory_tracker_(std::move(memory_tracker)) {
  assert(memory_tracker_ != nullptr);
}

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
  Status status = memory_tracker_->SetReservation(
      &charged_memory_, capacity * sizeof(BlobReference));
  if (!status.ok()) {
    return status;
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
  if (output == nullptr) {
    return Status::Corruption("Blob relocation live set output is unavailable");
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
  return Finalize(expected_source.origin_file_number, {expected_source});
}

Status BlobRelocationLiveSet::Finalize(
    uint64_t expected_origin,
    const std::vector<BlobRelocationRouteSource>& expected_sources) {
  if (finalized_) {
    return Status::InvalidArgument("Blob relocation live set is finalized");
  }
  if (expected_origin == 0 ||
      !std::is_sorted(expected_sources.begin(), expected_sources.end()) ||
      std::adjacent_find(expected_sources.begin(), expected_sources.end()) !=
          expected_sources.end()) {
    return Status::InvalidArgument("Invalid blob relocation route sources");
  }
  finalized_ = true;
  std::sort(references_.begin(), references_.end(),
            [](const BlobReference& lhs, const BlobReference& rhs) {
              return std::tie(lhs.origin_file_number, lhs.original_value_offset,
                              lhs.stored_value_size, lhs.record_bytes,
                              lhs.compression, lhs.stable_checksum_version,
                              lhs.stable_checksum, lhs.stored_checksum) <
                     std::tie(rhs.origin_file_number, rhs.original_value_offset,
                              rhs.stored_value_size, rhs.record_bytes,
                              rhs.compression, rhs.stable_checksum_version,
                              rhs.stable_checksum, rhs.stored_checksum);
            });

  size_t output = 0;
  for (size_t i = 0; i < references_.size(); ++i) {
    BlobReference& reference = references_[i];
    const BlobRelocationRouteSource reference_source{
        reference.generation_file_number, reference.origin_file_number};
    if (!reference.HasStableIdentity() ||
        reference.origin_file_number != expected_origin ||
        !std::binary_search(expected_sources.begin(), expected_sources.end(),
                            reference_source) ||
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
  return memory_tracker_->SetReservation(&charged_memory_, 0);
}

size_t BlobRelocationLiveSet::ReservedMemory() const {
  return references_.capacity() * sizeof(BlobReference);
}

Status BlobRelocationLiveSet::UpdateReservation() {
  return memory_tracker_->SetReservation(&charged_memory_, ReservedMemory());
}

Status PlanBlobRelocationPacking(
    const std::vector<BlobRelocationPackingItem>& items,
    uint64_t target_output_bytes, uint64_t hard_output_bytes,
    size_t max_origins_per_output, size_t max_outputs,
    std::vector<BlobRelocationPackingBin>* bins) {
  if (bins == nullptr || target_output_bytes == 0 ||
      target_output_bytes > hard_output_bytes || max_origins_per_output == 0 ||
      max_outputs == 0) {
    return Status::InvalidArgument("Invalid blob relocation packing limits");
  }
  bins->clear();
  std::vector<size_t> order(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    if (items[i].origin_file_number == 0 ||
        items[i].source_generation_file_number == 0 ||
        items[i].estimated_bytes == 0 ||
        items[i].estimated_bytes > hard_output_bytes) {
      return Status::MemoryLimit("Blob relocation partition exceeds limit");
    }
    order[i] = i;
  }
  std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
    const BlobRelocationPackingItem& left = items[lhs];
    const BlobRelocationPackingItem& right = items[rhs];
    if (left.estimated_bytes != right.estimated_bytes) {
      return left.estimated_bytes > right.estimated_bytes;
    }
    return std::tie(left.origin_file_number,
                    left.source_generation_file_number) <
           std::tie(right.origin_file_number,
                    right.source_generation_file_number);
  });

  for (size_t item_index : order) {
    const BlobRelocationPackingItem& item = items[item_index];
    size_t destination = bins->size();
    for (size_t i = 0; i < bins->size(); ++i) {
      BlobRelocationPackingBin& bin = (*bins)[i];
      const bool duplicate_origin =
          std::any_of(bin.item_indexes.begin(), bin.item_indexes.end(),
                      [&](size_t existing_index) {
                        return items[existing_index].origin_file_number ==
                               item.origin_file_number;
                      });
      if (bin.item_indexes.size() < max_origins_per_output &&
          !duplicate_origin &&
          item.estimated_bytes <=
              target_output_bytes -
                  std::min(bin.estimated_bytes, target_output_bytes)) {
        destination = i;
        break;
      }
    }
    if (destination == bins->size()) {
      if (bins->size() == max_outputs) {
        bins->clear();
        return Status::MemoryLimit("Too many blob relocation outputs");
      }
      bins->emplace_back();
    }
    BlobRelocationPackingBin& bin = (*bins)[destination];
    if (item.estimated_bytes >
        std::numeric_limits<uint64_t>::max() - bin.estimated_bytes) {
      bins->clear();
      return Status::Corruption("Blob relocation packing byte overflow");
    }
    bin.item_indexes.push_back(item_index);
    bin.estimated_bytes += item.estimated_bytes;
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
