//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "table/multiget_context.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

inline std::string* GetOutputTimestamp(OutputMetadata* output_metadata) {
  return output_metadata != nullptr && output_metadata->timestamp.has_value()
             ? &*output_metadata->timestamp
             : nullptr;
}

inline std::vector<std::string>* GetOutputTimestamps(
    MultiGetOutputMetadata* output_metadata) {
  return output_metadata != nullptr && output_metadata->timestamps.has_value()
             ? &*output_metadata->timestamps
             : nullptr;
}

inline bool* GetOutputNewerVersionPresent(OutputMetadata* output_metadata) {
  return output_metadata != nullptr &&
                 output_metadata->newer_version_present.has_value()
             ? &*output_metadata->newer_version_present
             : nullptr;
}

inline std::vector<uint8_t>* GetOutputNewerVersionPresent(
    MultiGetOutputMetadata* output_metadata) {
  return output_metadata != nullptr &&
                 output_metadata->newer_version_present.has_value()
             ? &*output_metadata->newer_version_present
             : nullptr;
}

inline ColumnFamilyHandle** MakeMutableCfHandles(
    ColumnFamilyHandle* const* column_families, size_t num_keys,
    autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>*
        stack_column_families,
    std::vector<ColumnFamilyHandle*>* heap_column_families) {
  ColumnFamilyHandle** mutable_column_families;
  if (num_keys <= MultiGetContext::MAX_BATCH_SIZE) {
    stack_column_families->resize(num_keys);
    mutable_column_families =
        num_keys == 0 ? nullptr : &(*stack_column_families)[0];
  } else {
    heap_column_families->resize(num_keys);
    mutable_column_families = heap_column_families->data();
  }
  for (size_t i = 0; i < num_keys; ++i) {
    mutable_column_families[i] = column_families[i];
  }
  return mutable_column_families;
}

// Clears the requested GetEntityWithMetadata() outputs. Entity reads do not
// return timestamps, so requesting one returns NotSupported.
inline Status InitGetEntityOutputMetadata(OutputMetadata* output_metadata) {
  bool* newer_version_present = GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    *newer_version_present = false;
  }
  std::string* timestamp = GetOutputTimestamp(output_metadata);
  if (timestamp != nullptr) {
    timestamp->clear();
    return Status::NotSupported(
        "GetEntityWithMetadata does not support timestamps");
  }
  return Status::OK();
}

// Sizes the requested MultiGetEntityWithMetadata() outputs to num_keys and
// clears the newer-version flags. Entity reads do not return timestamps, so
// requesting them returns NotSupported.
inline Status InitMultiGetEntityOutputMetadata(
    size_t num_keys, MultiGetOutputMetadata* output_metadata) {
  std::vector<uint8_t>* newer_version_present =
      GetOutputNewerVersionPresent(output_metadata);
  if (newer_version_present != nullptr) {
    newer_version_present->assign(num_keys, false);
  }
  std::vector<std::string>* timestamps = GetOutputTimestamps(output_metadata);
  if (timestamps != nullptr) {
    timestamps->assign(num_keys, std::string());
    return Status::NotSupported(
        "MultiGetEntityWithMetadata does not support timestamps");
  }
  return Status::OK();
}

// Serves MultiGetEntityWithMetadata() through MultiGetEntity(), leaving any
// requested newer-version flags false.
inline void MultiGetEntityWithoutNewerVersionTracking(
    DB* db, const ReadOptions& options, size_t num_keys,
    ColumnFamilyHandle* const* column_families, const Slice* keys,
    PinnableWideColumns* results, Status* statuses, bool sorted_input) {
  autovector<ColumnFamilyHandle*, MultiGetContext::MAX_BATCH_SIZE>
      stack_column_families;
  std::vector<ColumnFamilyHandle*> heap_column_families;
  ColumnFamilyHandle** mutable_column_families =
      column_families == nullptr
          ? nullptr
          : MakeMutableCfHandles(column_families, num_keys,
                                 &stack_column_families, &heap_column_families);
  db->MultiGetEntity(options, num_keys, mutable_column_families, keys, results,
                     statuses, sorted_input);
}

inline size_t CountAttributeGroups(size_t num_keys,
                                   const PinnableAttributeGroups* results) {
  if (results == nullptr) {
    return 0;
  }
  size_t total_count = 0;
  for (size_t i = 0; i < num_keys; ++i) {
    total_count += results[i].size();
  }
  return total_count;
}

inline void SetAttributeGroupsStatus(size_t num_keys,
                                     PinnableAttributeGroups* results,
                                     const Status& s) {
  if (results == nullptr) {
    return;
  }
  for (size_t i = 0; i < num_keys; ++i) {
    for (size_t j = 0; j < results[i].size(); ++j) {
      results[i][j].SetStatus(s);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
