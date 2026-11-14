//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <unordered_map>
#ifdef USE_FOLLY
#include <folly/container/F14Map.h>
#endif

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Helper methods to estimate memory usage by std containers.

template <class Key, class Value, class Hash>
size_t ApproximateMemoryUsage(
    const std::unordered_map<Key, Value, Hash>& umap) {
  using Map = std::unordered_map<Key, Value, Hash>;
  return sizeof(umap) +
         // Size of all items plus a next pointer for each item.
         (sizeof(typename Map::value_type) + sizeof(void*)) * umap.size() +
         // Size of hash buckets.
         umap.bucket_count() * sizeof(void*);
}

#ifdef USE_FOLLY
template <class Key, class Value, class Hash>
size_t ApproximateMemoryUsage(const folly::F14FastMap<Key, Value, Hash>& umap) {
  return sizeof(umap) + umap.getAllocatedMemorySize();
}
#endif

}  // namespace ROCKSDB_NAMESPACE
