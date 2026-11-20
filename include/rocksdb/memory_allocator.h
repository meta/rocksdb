// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "rocksdb/customizable.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// MemoryAllocator is an interface that a client can implement to supply custom
// memory allocation and deallocation methods. See rocksdb/cache.h for more
// information.
// All methods should be thread-safe.
class MemoryAllocator : public Customizable {
 public:
  static const char* Type() { return "MemoryAllocator"; }
  static Status CreateFromString(const ConfigOptions& options,
                                 const std::string& value,
                                 std::shared_ptr<MemoryAllocator>* result);

  // Allocate a block of at least size. Has to be thread-safe.
  virtual void* Allocate(size_t size) = 0;

  // Deallocate previously allocated block. Has to be thread-safe.
  virtual void Deallocate(void* p) = 0;

  // Returns the memory size of the block allocated at p. The default
  // implementation that just returns the original allocation_size is fine.
  virtual size_t UsableSize(void* /*p*/, size_t allocation_size) const {
    // default implementation just returns the allocation size
    return allocation_size;
  }

  std::string GetId() const override { return GenerateIndividualId(); }
};

struct JemallocAllocatorOptions {
  static const char* kName() { return "JemallocAllocatorOptions"; }
  // Jemalloc tcache cache allocations by size class. For each size class,
  // it caches between 20 (for large size classes) to 200 (for small size
  // classes). To reduce tcache memory usage in case the allocator is access
  // by large number of threads, we can control whether to cache an allocation
  // by its size.
  bool limit_tcache_size = false;

  // Lower bound of allocation size to use tcache, if limit_tcache_size=true.
  // When used with block cache, it is recommended to set it to block_size/4.
  size_t tcache_size_lower_bound = 1024;

  // Upper bound of allocation size to use tcache, if limit_tcache_size=true.
  // When used with block cache, it is recommended to set it to block_size.
  size_t tcache_size_upper_bound = 16 * 1024;

  // Number of arenas across which we spread allocation requests. Increasing
  // this setting can mitigate arena mutex contention. The value must be
  // positive.
  size_t num_arenas = 1;

  // If "" then use default arena name.
  // If not "" then use arena name with prefix, length <= 20.
  // e.g arena_name_prefix:"rocksdb_", then arena name is "rocksdb_1", 1 is index of num_arenas.
  std::string arena_name_prefix = "";
};

// Generate memory allocator which allocates through Jemalloc and utilize
// MADV_DONTDUMP through madvise to exclude cache items from core dump.
// Applications can use the allocator with block cache to exclude block cache
// usage from core dump.
//
// Implementation details:
// The JemallocNodumpAllocator creates a dedicated jemalloc arena, and all
// allocations of the JemallocNodumpAllocator are through the same arena.
// The memory allocator hooks memory allocation of the arena, and calls
// madvise() with MADV_DONTDUMP flag to exclude the piece of memory from
// core dump. Side benefit of using single arena would be reduction of jemalloc
// metadata for some workloads.
//
// To mitigate mutex contention for using one single arena (see also
// `JemallocAllocatorOptions::num_arenas` above), jemalloc tcache
// (thread-local cache) is enabled to cache unused allocations for future use.
// The tcache normally incurs 0.5M extra memory usage per-thread. The usage
// can be reduced by limiting allocation sizes to cache.
Status NewJemallocNodumpAllocator(
    const JemallocAllocatorOptions& options,
    std::shared_ptr<MemoryAllocator>* memory_allocator);

}  // namespace ROCKSDB_NAMESPACE
