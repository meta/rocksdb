//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <algorithm>

#include "rocksdb/memory_allocator.h"

namespace ROCKSDB_NAMESPACE {

struct CacheAllocationDeleter {
  explicit CacheAllocationDeleter(MemoryAllocator* a = nullptr)
      : allocator(a) {}

  void operator()(char* ptr) const {
    if (allocator) {
      allocator->Deallocate(ptr);
    } else {
      delete[] ptr;
    }
  }

  MemoryAllocator* allocator;
};

using CacheAllocationPtr = std::unique_ptr<char[], CacheAllocationDeleter>;

inline CacheAllocationPtr AllocateBlock(size_t size,
                                        MemoryAllocator* allocator) {
  if (allocator) {
    auto block = static_cast<char*>(allocator->Allocate(size));
    return CacheAllocationPtr(block, CacheAllocationDeleter(allocator));
  }
  return CacheAllocationPtr(new char[size]);
}

inline CacheAllocationPtr AllocateAndCopyBlock(const Slice& data,
                                               MemoryAllocator* allocator) {
  CacheAllocationPtr cap = AllocateBlock(data.size(), allocator);
  std::copy_n(data.data(), data.size(), cap.get());
  return cap;
}

}  // namespace ROCKSDB_NAMESPACE
