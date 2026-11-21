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

// The record overhead is not compared: one value can be referenced unflattened
// (a record header and key in its origin) and flattened (a trailer in a
// relocation file) at the same time.
auto ImmutableFields(const BlobRelocationLiveValue& value) {
  return std::tie(value.stored_value_size, value.compression,
                  value.stable_checksum, value.stored_checksum);
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
    std::shared_ptr<CacheReservationManager> cache_reservation_manager,
    std::vector<uint64_t> generation_numbers)
    : BlobRelocationLiveSet(std::make_shared<BlobRelocationMemoryTracker>(
                                std::move(cache_reservation_manager)),
                            std::move(generation_numbers)) {}

BlobRelocationLiveSet::BlobRelocationLiveSet(
    std::shared_ptr<BlobRelocationMemoryTracker> memory_tracker,
    std::vector<uint64_t> generation_numbers)
    : memory_tracker_(std::move(memory_tracker)),
      generation_numbers_(std::move(generation_numbers)) {
  assert(memory_tracker_ != nullptr);
}

BlobRelocationLiveSet::~BlobRelocationLiveSet() {
  ReleaseWorkingMemory().PermitUncheckedError();
}

Status BlobRelocationLiveSet::Reserve(size_t additional) {
  if (finalized_) {
    return Status::InvalidArgument("Blob relocation live set is finalized");
  }
  if (generation_numbers_.empty() || generation_numbers_.front() == 0 ||
      !std::is_sorted(generation_numbers_.begin(), generation_numbers_.end()) ||
      std::adjacent_find(generation_numbers_.begin(),
                         generation_numbers_.end()) !=
          generation_numbers_.end()) {
    return Status::InvalidArgument(
        "Invalid blob relocation input generation numbers");
  }
  if (additional == 0) {
    return Status::OK();
  }
  if (additional > values_.max_size() - values_.size()) {
    return Status::MemoryLimit("Blob relocation live set is too large");
  }
  const size_t target = values_.size() + additional;
  if (target <= values_.capacity()) {
    return Status::OK();
  }
  // Reserve the exact size, not a doubled capacity: the caller reserves the
  // full census total in one call, so doubling would only double the charge.
  const size_t capacity = target;
  if (capacity >
      std::numeric_limits<size_t>::max() / sizeof(BlobRelocationLiveValue)) {
    return Status::MemoryLimit("Blob relocation live set overflow");
  }
  Status status = memory_tracker_->SetReservation(
      &charged_memory_, capacity * sizeof(BlobRelocationLiveValue));
  if (!status.ok()) {
    return status;
  }
  values_.reserve(capacity);
  return UpdateReservation();
}

Status BlobRelocationLiveSet::Add(
    const std::vector<BlobReference>& references) {
  Status status = Reserve(references.size());
  for (size_t i = 0; status.ok() && i < references.size(); ++i) {
    status = Append(references[i]);
  }
  return status;
}

Status BlobRelocationLiveSet::Append(const BlobReference& reference) {
  if (finalized_ || values_.size() == values_.capacity()) {
    return Status::Corruption("Blob relocation live set has no room");
  }
  // The generation number of an unflattened reference is its origin. The
  // generation number of a flattened one is the relocation file it was
  // flattened to.
  const bool unflattened = reference.encoding_class ==
                           BlobReferenceEncodingClass::kIndirectUnflattened;
  if (!reference.HasStableIdentity() || reference.origin_file_number == 0 ||
      unflattened !=
          (reference.generation_file_number == reference.origin_file_number) ||
      !std::binary_search(generation_numbers_.begin(),
                          generation_numbers_.end(),
                          reference.generation_file_number) ||
      reference.record_bytes < reference.stored_value_size ||
      reference.stored_value_size > std::numeric_limits<uint32_t>::max() ||
      reference.record_bytes - reference.stored_value_size >
          std::numeric_limits<uint32_t>::max()) {
    return Status::Corruption("Unexpected blob relocation reference");
  }
  BlobRelocationLiveValue value;
  value.origin_file_number = reference.origin_file_number;
  value.original_value_offset = reference.original_value_offset;
  value.stored_value_size = static_cast<uint32_t>(reference.stored_value_size);
  value.record_overhead = static_cast<uint32_t>(reference.record_bytes -
                                                reference.stored_value_size);
  value.stable_checksum = reference.stable_checksum;
  value.stored_checksum = reference.stored_checksum;
  value.compression = reference.compression;
  values_.push_back(value);
  return Status::OK();
}

Status BlobRelocationLiveSet::Finalize() {
  if (finalized_) {
    return Status::InvalidArgument("Blob relocation live set is finalized");
  }
  finalized_ = true;
  const auto sort_key = [](const BlobRelocationLiveValue& value) {
    return std::make_tuple(value.origin_file_number,
                           value.original_value_offset, value.stored_value_size,
                           value.record_overhead, value.compression,
                           value.stable_checksum, value.stored_checksum);
  };
  // Copies of one value can differ only in record_overhead (see Finalize() in
  // the header for why the smallest one is correct). The sort puts the smallest
  // record_overhead first, and the loop below keeps it. In a normal input every
  // copy is unflattened, with the same record_overhead.
  //
  // The choice of record_overhead does not change the indirect blob GC
  // decision: for a relocation input, the GC job computes the live record bytes
  // again as stored bytes plus one trailer per value.
  std::sort(values_.begin(), values_.end(),
            [&](const BlobRelocationLiveValue& lhs,
                const BlobRelocationLiveValue& rhs) {
              return sort_key(lhs) < sort_key(rhs);
            });

  size_t output = 0;
  for (size_t i = 0; i < values_.size(); ++i) {
    const BlobRelocationLiveValue& value = values_[i];
    if (output != 0 &&
        values_[output - 1].origin_file_number == value.origin_file_number &&
        values_[output - 1].original_value_offset ==
            value.original_value_offset) {
      if (ImmutableFields(values_[output - 1]) != ImmutableFields(value)) {
        return Status::Corruption(
            "Conflicting metadata for one stable blob identity");
      }
      continue;
    }
    if (value.record_bytes() >
            std::numeric_limits<uint64_t>::max() - live_record_bytes_ ||
        value.stored_value_size >
            std::numeric_limits<uint64_t>::max() - live_stored_value_bytes_) {
      return Status::Corruption("Blob relocation live byte overflow");
    }
    live_record_bytes_ += value.record_bytes();
    live_stored_value_bytes_ += value.stored_value_size;
    if (origin_runs_.empty() ||
        origin_runs_.back().origin_file_number != value.origin_file_number) {
      origin_runs_.push_back({value.origin_file_number, output, output});
    }
    values_[output++] = value;
    origin_runs_.back().end = output;
  }
  values_.resize(output);
  return UpdateReservation();
}

Status BlobRelocationLiveSet::ReleaseWorkingMemory() {
  std::vector<BlobRelocationLiveValue>().swap(values_);
  std::vector<BlobRelocationOriginRun>().swap(origin_runs_);
  return memory_tracker_->SetReservation(&charged_memory_, 0);
}

size_t BlobRelocationLiveSet::ReservedMemory() const {
  return values_.capacity() * sizeof(BlobRelocationLiveValue) +
         origin_runs_.capacity() * sizeof(BlobRelocationOriginRun);
}

Status BlobRelocationLiveSet::UpdateReservation() {
  return memory_tracker_->SetReservation(&charged_memory_, ReservedMemory());
}

Status PlanBlobRelocationPacking(
    const std::vector<BlobRelocationPackingItem>& items,
    uint64_t target_output_bytes, size_t max_outputs,
    std::vector<BlobRelocationPackingBin>* bins) {
  if (bins == nullptr || target_output_bytes == 0 || max_outputs == 0) {
    return Status::InvalidArgument("Invalid blob relocation packing limits");
  }
  bins->clear();
  std::vector<size_t> order(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    if (items[i].input_file_number == 0 || items[i].estimated_bytes == 0) {
      return Status::InvalidArgument("Invalid blob relocation packing item");
    }
    order[i] = i;
  }
  std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
    const BlobRelocationPackingItem& left = items[lhs];
    const BlobRelocationPackingItem& right = items[rhs];
    if (left.estimated_bytes != right.estimated_bytes) {
      return left.estimated_bytes > right.estimated_bytes;
    }
    return left.input_file_number < right.input_file_number;
  });

  const auto shares_origin = [&](const BlobRelocationPackingBin& bin,
                                 const BlobRelocationPackingItem& item) {
    for (size_t existing_index : bin.item_indexes) {
      const std::vector<uint64_t>& existing = items[existing_index].origins;
      auto lhs = existing.begin();
      auto rhs = item.origins.begin();
      while (lhs != existing.end() && rhs != item.origins.end()) {
        if (*lhs == *rhs) {
          return true;
        }
        if (*lhs < *rhs) {
          ++lhs;
        } else {
          ++rhs;
        }
      }
    }
    return false;
  };
  for (size_t item_index : order) {
    const BlobRelocationPackingItem& item = items[item_index];
    size_t destination = bins->size();
    for (size_t i = 0; i < bins->size(); ++i) {
      BlobRelocationPackingBin& bin = (*bins)[i];
      if (item.estimated_bytes <=
              target_output_bytes -
                  std::min(bin.estimated_bytes, target_output_bytes) &&
          !shares_origin(bin, item)) {
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
