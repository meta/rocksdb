// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <stdint.h>

#include <memory>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// PersistentCache
//
// Persistent cache interface for caching IO pages on a persistent medium. The
// cache interface is specifically designed for persistent read cache.
class PersistentCache {
 public:
  using StatsType = std::vector<std::map<std::string, double>>;

  virtual ~PersistentCache() = default;

  // Insert to page cache
  //
  // page_key   Identifier to identify a page uniquely across restarts
  // data       Page data to copy (caller retains ownership)
  // size       Size of the page
  virtual Status Insert(const Slice& key, const char* data,
                        const size_t size) = 0;

  // Lookup page cache by page identifier
  //
  // page_key   Page identifier
  // buf        Buffer where the data should be copied
  // size       Size of the page
  virtual Status Lookup(const Slice& key, std::unique_ptr<char[]>* data,
                        size_t* size) = 0;

  // True if the cache is configured to store serialized blocks, which are
  // potentially compressed and include a trailer (when SST format calls for
  // one). False if the cache stores uncompressed blocks (no trailer).
  virtual bool IsCompressed() = 0;

  // Return stats as map of {string, double} per-tier
  //
  // Persistent cache can be initialized as a tier of caches. The stats are per
  // tire top-down
  virtual StatsType Stats() = 0;

  virtual std::string GetPrintableOptions() const = 0;

  // Return a new numeric id.  May be used by multiple clients who are
  // sharding the same persistent cache to partition the key space.  Typically
  // the client will allocate a new id at startup and prepend the id to its
  // cache keys.
  virtual uint64_t NewId() = 0;
};

// Factor method to create a new persistent cache
Status NewPersistentCache(Env* const env, const std::string& path,
                          const uint64_t size,
                          const std::shared_ptr<Logger>& log,
                          const bool optimized_for_nvm,
                          std::shared_ptr<PersistentCache>* cache);
}  // namespace ROCKSDB_NAMESPACE
