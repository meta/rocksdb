//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "cache/cache_key.h"
#include "cache/typed_cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
// CacheReservationManager is an interface for reserving cache space for the
// memory used
class CacheReservationManager {
 public:
  // CacheReservationHandle is for managing the lifetime of a cache reservation
  // for an incremental amount of memory used (i.e, incremental_memory_used)
  class CacheReservationHandle {
   public:
    virtual ~CacheReservationHandle() = default;
    // Updates this owner's contribution, not the domain's aggregate usage.
    // Like MakeCacheReservation(), a failed growth remains owned until resized
    // or released. The caller must not allocate after a failed reservation.
    virtual Status Resize(std::size_t memory_used) = 0;
  };
  virtual ~CacheReservationManager() = default;
  virtual Status UpdateCacheReservation(std::size_t new_memory_used) = 0;
  // TODO(hx235): replace the usage of
  // `UpdateCacheReservation(memory_used_delta, increase)` with
  // `UpdateCacheReservation(new_memory_used)` so that we only have one
  // `UpdateCacheReservation` function
  virtual Status UpdateCacheReservation(std::size_t memory_used_delta,
                                        bool increase) = 0;
  virtual Status MakeCacheReservation(
      std::size_t incremental_memory_used,
      std::unique_ptr<CacheReservationManager::CacheReservationHandle>*
          handle) = 0;
  virtual std::size_t GetTotalReservedCacheSize() = 0;
  virtual std::size_t GetTotalMemoryUsed() = 0;
};

// CacheReservationManagerImpl implements interface CacheReservationManager
// for reserving cache space for the memory used by inserting/releasing dummy
// entries in the cache.
//
// This class is NOT thread-safe, except that GetTotalReservedCacheSize()
// can be called without external synchronization.
template <CacheEntryRole R>
class CacheReservationManagerImpl
    : public CacheReservationManager,
      public std::enable_shared_from_this<CacheReservationManagerImpl<R>> {
 public:
  class CacheReservationHandle
      : public CacheReservationManager::CacheReservationHandle {
   public:
    CacheReservationHandle(
        std::size_t incremental_memory_used,
        std::shared_ptr<CacheReservationManagerImpl> cache_res_mgr);
    ~CacheReservationHandle() override;
    Status Resize(std::size_t memory_used) override;

   private:
    std::size_t incremental_memory_used_;
    std::shared_ptr<CacheReservationManagerImpl> cache_res_mgr_;
  };

  // Construct a CacheReservationManagerImpl
  // @param cache The cache where dummy entries are inserted and released for
  // reserving cache space
  // @param delayed_decrease If set true, then dummy entries won't be released
  //                         immediately when memory usage decreases.
  //                         Instead, it will be released when the memory usage
  //                         decreases to 3/4 of what we have reserved so far.
  //                         This is for saving some future dummy entry
  //                         insertion when memory usage increases are likely to
  //                         happen in the near future.
  //
  // REQUIRED: cache is not nullptr
  explicit CacheReservationManagerImpl(std::shared_ptr<Cache> cache,
                                       bool delayed_decrease = false);

  // no copy constructor, copy assignment, move constructor, move assignment
  CacheReservationManagerImpl(const CacheReservationManagerImpl&) = delete;
  CacheReservationManagerImpl& operator=(const CacheReservationManagerImpl&) =
      delete;
  CacheReservationManagerImpl(CacheReservationManagerImpl&&) = delete;
  CacheReservationManagerImpl& operator=(CacheReservationManagerImpl&&) =
      delete;

  ~CacheReservationManagerImpl() override;

  // One of the two ways of reserving/releasing cache space,
  // see MakeCacheReservation() for the other.
  //
  // Use ONLY one of these two ways to prevent unexpected behavior.
  //
  // Insert and release dummy entries in the cache to
  // match the size of total dummy entries with the least multiple of
  // kSizeDummyEntry greater than or equal to new_mem_used
  //
  // Insert dummy entries if new_memory_used > cache_allocated_size_;
  //
  // Release dummy entries if new_memory_used < cache_allocated_size_
  // (and new_memory_used < cache_allocated_size_ * 3/4
  // when delayed_decrease is set true);
  //
  // Keey dummy entries the same if (1) new_memory_used == cache_allocated_size_
  // or (2) new_memory_used is in the interval of
  // [cache_allocated_size_ * 3/4, cache_allocated_size) when delayed_decrease
  // is set true.
  //
  // @param new_memory_used The number of bytes used by new memory
  //        The most recent new_memoy_used passed in will be returned
  //        in GetTotalMemoryUsed() even when the call return non-ok status.
  //
  //        Since the class is NOT thread-safe, external synchronization on the
  //        order of calling UpdateCacheReservation() is needed if you want
  //        GetTotalMemoryUsed() indeed returns the latest memory used.
  //
  // @return On inserting dummy entries, it returns Status::OK() if all dummy
  //         entry insertions succeed.
  //         Otherwise, it returns the first non-ok status;
  //         On releasing dummy entries, it always returns Status::OK().
  //         On keeping dummy entries the same, it always returns Status::OK().
  Status UpdateCacheReservation(std::size_t new_memory_used) override;

  Status UpdateCacheReservation(std::size_t /* memory_used_delta */,
                                bool /* increase */) override {
    return Status::NotSupported();
  }

  // One of the two ways of reserving cache space and releasing is done through
  // destruction of CacheReservationHandle.
  // See UpdateCacheReservation() for the other way.
  //
  // Use ONLY one of these two ways to prevent unexpected behavior.
  //
  // Insert dummy entries in the cache for the incremental memory usage
  // to match the size of total dummy entries with the least multiple of
  // kSizeDummyEntry greater than or equal to the total memory used.
  //
  // A CacheReservationHandle is returned as an output parameter.
  // The reserved dummy entries are automatically released on the destruction of
  // this handle, which achieves better RAII per cache reservation.
  //
  // WARNING: Deallocate all the handles of the CacheReservationManager object
  //          before deallocating the object to prevent unexpected behavior.
  //
  // @param incremental_memory_used The number of bytes increased in memory
  //        usage.
  //
  //        Calling GetTotalMemoryUsed() afterward will return the total memory
  //        increased by this number, even when calling MakeCacheReservation()
  //        returns non-ok status.
  //
  //        Since the class is NOT thread-safe, external synchronization in
  //        calling MakeCacheReservation() is needed if you want
  //        GetTotalMemoryUsed() indeed returns the latest memory used.
  //
  // @param handle An pointer to std::unique_ptr<CacheReservationHandle> that
  //        manages the lifetime of the cache reservation represented by the
  //        handle.
  //
  // @return It returns Status::OK() if all dummy
  //         entry insertions succeed.
  //         Otherwise, it returns the first non-ok status;
  //
  // REQUIRES: handle != nullptr
  Status MakeCacheReservation(
      std::size_t incremental_memory_used,
      std::unique_ptr<CacheReservationManager::CacheReservationHandle>* handle)
      override;

  // Return the size of the cache (which is a multiple of kSizeDummyEntry)
  // successfully reserved by calling UpdateCacheReservation().
  //
  // When UpdateCacheReservation() returns non-ok status,
  // calling GetTotalReservedCacheSize() after that might return a slightly
  // smaller number than the actual reserved cache size due to
  // the returned number will always be a multiple of kSizeDummyEntry
  // and cache full might happen in the middle of inserting a dummy entry.
  std::size_t GetTotalReservedCacheSize() override;

  // Return the latest total memory used indicated by the most recent call of
  // UpdateCacheReservation(std::size_t new_memory_used);
  std::size_t GetTotalMemoryUsed() override;

  static constexpr std::size_t GetDummyEntrySize() { return kSizeDummyEntry; }

  // For testing only - it is to help ensure the CacheItemHelperForRole<R>
  // accessed from CacheReservationManagerImpl and the one accessed from the
  // test are from the same translation units
  static const Cache::CacheItemHelper* TEST_GetCacheItemHelperForRole();

 private:
  static constexpr std::size_t kSizeDummyEntry = 256 * 1024;

  Slice GetNextCacheKey();

  Status ReleaseCacheReservation(std::size_t incremental_memory_used);
  Status IncreaseCacheReservation(std::size_t new_mem_used);
  Status DecreaseCacheReservation(std::size_t new_mem_used);

  using CacheInterface = PlaceholderSharedCacheInterface<R>;
  CacheInterface cache_;
  bool delayed_decrease_;
  std::atomic<std::size_t> cache_allocated_size_;
  std::size_t memory_used_;
  std::vector<Cache::Handle*> dummy_handles_;
  CacheKey cache_key_;
};

class ConcurrentCacheReservationManager
    : public CacheReservationManager,
      public std::enable_shared_from_this<ConcurrentCacheReservationManager> {
 public:
  class CacheReservationHandle
      : public CacheReservationManager::CacheReservationHandle {
   public:
    CacheReservationHandle(
        std::shared_ptr<ConcurrentCacheReservationManager> cache_res_mgr,
        std::unique_ptr<CacheReservationManager::CacheReservationHandle>
            cache_res_handle) {
      assert(cache_res_mgr && cache_res_handle);
      cache_res_mgr_ = cache_res_mgr;
      cache_res_handle_ = std::move(cache_res_handle);
    }
    // No copying allowed
    CacheReservationHandle(const CacheReservationHandle&) = delete;
    CacheReservationHandle& operator=(const CacheReservationHandle&) = delete;
    CacheReservationHandle(CacheReservationHandle&&) = delete;
    CacheReservationHandle& operator=(CacheReservationHandle&&) = delete;

    ~CacheReservationHandle() override {
      std::lock_guard<std::mutex> lock(cache_res_mgr_->cache_res_mgr_mu_);
      cache_res_handle_.reset();
    }
    Status Resize(std::size_t memory_used) override {
      std::lock_guard<std::mutex> lock(cache_res_mgr_->cache_res_mgr_mu_);
      return cache_res_handle_->Resize(memory_used);
    }

   private:
    std::shared_ptr<ConcurrentCacheReservationManager> cache_res_mgr_;
    std::unique_ptr<CacheReservationManager::CacheReservationHandle>
        cache_res_handle_;
  };

  explicit ConcurrentCacheReservationManager(
      std::shared_ptr<CacheReservationManager> cache_res_mgr) {
    cache_res_mgr_ = std::move(cache_res_mgr);
  }
  ConcurrentCacheReservationManager(const ConcurrentCacheReservationManager&) =
      delete;
  ConcurrentCacheReservationManager& operator=(
      const ConcurrentCacheReservationManager&) = delete;
  ConcurrentCacheReservationManager(ConcurrentCacheReservationManager&&) =
      delete;
  ConcurrentCacheReservationManager& operator=(
      ConcurrentCacheReservationManager&&) = delete;

  ~ConcurrentCacheReservationManager() override {}

  inline Status UpdateCacheReservation(std::size_t new_memory_used) override {
    std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
    return cache_res_mgr_->UpdateCacheReservation(new_memory_used);
  }

  inline Status UpdateCacheReservation(std::size_t memory_used_delta,
                                       bool increase) override {
    std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
    std::size_t total_mem_used = cache_res_mgr_->GetTotalMemoryUsed();
    Status s;
    if (!increase) {
      s = cache_res_mgr_->UpdateCacheReservation(
          (total_mem_used > memory_used_delta)
              ? (total_mem_used - memory_used_delta)
              : 0);
    } else {
      s = cache_res_mgr_->UpdateCacheReservation(total_mem_used +
                                                 memory_used_delta);
    }
    return s;
  }

  inline Status MakeCacheReservation(
      std::size_t incremental_memory_used,
      std::unique_ptr<CacheReservationManager::CacheReservationHandle>* handle)
      override {
    std::unique_ptr<CacheReservationManager::CacheReservationHandle>
        wrapped_handle;
    Status s;
    {
      std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
      s = cache_res_mgr_->MakeCacheReservation(incremental_memory_used,
                                               &wrapped_handle);
    }
    (*handle).reset(
        new ConcurrentCacheReservationManager::CacheReservationHandle(
            std::enable_shared_from_this<
                ConcurrentCacheReservationManager>::shared_from_this(),
            std::move(wrapped_handle)));
    return s;
  }
  inline std::size_t GetTotalReservedCacheSize() override {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  }
  inline std::size_t GetTotalMemoryUsed() override {
    std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
    return cache_res_mgr_->GetTotalMemoryUsed();
  }

 private:
  std::mutex cache_res_mgr_mu_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
};
}  // namespace ROCKSDB_NAMESPACE
