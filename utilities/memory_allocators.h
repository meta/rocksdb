//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>

#include "rocksdb/memory_allocator.h"

namespace ROCKSDB_NAMESPACE {
// A memory allocator using new/delete
class DefaultMemoryAllocator : public MemoryAllocator {
 public:
  static const char* kClassName() { return "DefaultMemoryAllocator"; }
  const char* Name() const override { return kClassName(); }
  void* Allocate(size_t size) override {
    return static_cast<void*>(new char[size]);
  }

  void Deallocate(void* p) override { delete[] static_cast<char*>(p); }
};

// Base class for a MemoryAllocator.  This implementation does nothing
// and implements the methods in failuse mode (assert if the methods are
// invoked). Implementations can extend this class and override these methods
// when they are enabled via compiler switches (e.g., the
// JeMallocMemoryAllocator can define these methods if ROCKSDB_JEMALLOC is
// defined at compile time.  If compiled in "disabled" mode, this class provides
// default/failure implementations.  If compiled in "enabled" mode, the derived
// class needs to provide the appopriate "enabled" methods for the "real"
// implementation. Failure of the "real" implementation to implement ovreride
// any of these methods will result in an assert failure.
class BaseMemoryAllocator : public MemoryAllocator {
 public:
  void* Allocate(size_t /*size*/) override {
    assert(false);
    return nullptr;
  }

  void Deallocate(void* /*p*/) override { assert(false); }
};

// A Wrapped MemoryAllocator.  Delegates the memory allcator functions to the
// wrapped one.
class MemoryAllocatorWrapper : public MemoryAllocator {
 public:
  // Initialize an MemoryAllocatorWrapper that delegates all calls to *t
  explicit MemoryAllocatorWrapper(const std::shared_ptr<MemoryAllocator>& t);
  ~MemoryAllocatorWrapper() override = default;

  // Return the target to which to forward all calls
  MemoryAllocator* target() const { return target_.get(); }
  // Allocate a block of at least size. Has to be thread-safe.
  void* Allocate(size_t size) override { return target_->Allocate(size); }

  // Deallocate previously allocated block. Has to be thread-safe.
  void Deallocate(void* p) override { return target_->Deallocate(p); }

  // Returns the memory size of the block allocated at p. The default
  // implementation that just returns the original allocation_size is fine.
  size_t UsableSize(void* p, size_t allocation_size) const override {
    return target_->UsableSize(p, allocation_size);
  }

  const Customizable* Inner() const override { return target_.get(); }

 protected:
  std::shared_ptr<MemoryAllocator> target_;
};

// A memory allocator that counts the number of allocations and deallocations
// This class is useful if the number of memory allocations/dellocations is
// important.
class CountedMemoryAllocator : public MemoryAllocatorWrapper {
 public:
  CountedMemoryAllocator()
      : MemoryAllocatorWrapper(std::make_shared<DefaultMemoryAllocator>()),
        allocations_(0),
        deallocations_(0) {}

  explicit CountedMemoryAllocator(const std::shared_ptr<MemoryAllocator>& t)
      : MemoryAllocatorWrapper(t), allocations_(0), deallocations_(0) {}
  static const char* kClassName() { return "CountedMemoryAllocator"; }
  const char* Name() const override { return kClassName(); }
  std::string GetId() const override { return std::string(Name()); }
  void* Allocate(size_t size) override {
    allocations_++;
    return MemoryAllocatorWrapper::Allocate(size);
  }

  void Deallocate(void* p) override {
    deallocations_++;
    MemoryAllocatorWrapper::Deallocate(p);
  }
  uint64_t GetNumAllocations() const { return allocations_; }
  uint64_t GetNumDeallocations() const { return deallocations_; }

 private:
  std::atomic<uint64_t> allocations_;
  std::atomic<uint64_t> deallocations_;
};
}  // namespace ROCKSDB_NAMESPACE
