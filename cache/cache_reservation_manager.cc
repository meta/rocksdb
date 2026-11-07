//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "cache/cache_reservation_manager.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <memory>

#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/reader_common.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

template <CacheEntryRole R>
CacheReservationManagerImpl<R>::CacheReservationHandle::CacheReservationHandle(
    std::size_t incremental_memory_used,
    std::shared_ptr<CacheReservationManagerImpl> cache_res_mgr)
    : incremental_memory_used_(incremental_memory_used) {
  assert(cache_res_mgr);
  cache_res_mgr_ = cache_res_mgr;
}

template <CacheEntryRole R>
CacheReservationManagerImpl<
    R>::CacheReservationHandle::~CacheReservationHandle() {
  Status s = cache_res_mgr_->ReleaseCacheReservation(incremental_memory_used_);
  s.PermitUncheckedError();
}

template <CacheEntryRole R>
CacheReservationManagerImpl<R>::CacheReservationManagerImpl(
    std::shared_ptr<Cache> cache, bool delayed_decrease)
    : cache_(cache),
      delayed_decrease_(delayed_decrease),
      cache_allocated_size_(0),
      memory_used_(0) {
  assert(cache != nullptr);
}

template <CacheEntryRole R>
CacheReservationManagerImpl<R>::~CacheReservationManagerImpl() {
  for (auto* handle : dummy_handles_) {
    cache_.ReleaseAndEraseIfLastRef(handle);
  }
}

template <CacheEntryRole R>
Status CacheReservationManagerImpl<R>::UpdateCacheReservation(
    std::size_t new_mem_used) {
  memory_used_ = new_mem_used;
  std::size_t cur_cache_allocated_size =
      cache_allocated_size_.load(std::memory_order_relaxed);
  if (new_mem_used == cur_cache_allocated_size) {
    return Status::OK();
  } else if (new_mem_used > cur_cache_allocated_size) {
    Status s = IncreaseCacheReservation(new_mem_used);
    return s;
  } else {
    // In delayed decrease mode, we don't decrease cache reservation
    // untill the memory usage is less than 3/4 of what we reserve
    // in the cache.
    // We do this because
    // (1) Dummy entry insertion is expensive in block cache
    // (2) Delayed releasing previously inserted dummy entries can save such
    // expensive dummy entry insertion on memory increase in the near future,
    // which is likely to happen when the memory usage is greater than or equal
    // to 3/4 of what we reserve
    if (delayed_decrease_ && new_mem_used >= cur_cache_allocated_size / 4 * 3) {
      return Status::OK();
    } else {
      Status s = DecreaseCacheReservation(new_mem_used);
      return s;
    }
  }
}

template <CacheEntryRole R>
Status CacheReservationManagerImpl<R>::MakeCacheReservation(
    std::size_t incremental_memory_used,
    std::unique_ptr<CacheReservationManager::CacheReservationHandle>* handle) {
  assert(handle);
  Status s =
      UpdateCacheReservation(GetTotalMemoryUsed() + incremental_memory_used);
  (*handle).reset(new CacheReservationManagerImpl::CacheReservationHandle(
      incremental_memory_used,
      std::enable_shared_from_this<
          CacheReservationManagerImpl<R>>::shared_from_this()));
  return s;
}

template <CacheEntryRole R>
Status CacheReservationManagerImpl<R>::ReleaseCacheReservation(
    std::size_t incremental_memory_used) {
  assert(GetTotalMemoryUsed() >= incremental_memory_used);
  std::size_t updated_total_mem_used =
      GetTotalMemoryUsed() - incremental_memory_used;
  Status s = UpdateCacheReservation(updated_total_mem_used);
  return s;
}

template <CacheEntryRole R>
Status CacheReservationManagerImpl<R>::IncreaseCacheReservation(
    std::size_t new_mem_used) {
  Status return_status = Status::OK();
  while (new_mem_used > cache_allocated_size_.load(std::memory_order_relaxed)) {
    Cache::Handle* handle = nullptr;
    return_status = cache_.Insert(GetNextCacheKey(), kSizeDummyEntry, &handle);

    if (return_status != Status::OK()) {
      return return_status;
    }

    dummy_handles_.push_back(handle);
    cache_allocated_size_ += kSizeDummyEntry;
  }
  return return_status;
}

template <CacheEntryRole R>
Status CacheReservationManagerImpl<R>::DecreaseCacheReservation(
    std::size_t new_mem_used) {
  Status return_status = Status::OK();

  // Decrease to the smallest multiple of kSizeDummyEntry that is greater than
  // or equal to new_mem_used We do addition instead of new_mem_used <=
  // cache_allocated_size_.load(std::memory_order_relaxed) - kSizeDummyEntry to
  // avoid underflow of size_t when cache_allocated_size_ = 0
  while (new_mem_used + kSizeDummyEntry <=
         cache_allocated_size_.load(std::memory_order_relaxed)) {
    assert(!dummy_handles_.empty());
    auto* handle = dummy_handles_.back();
    cache_.ReleaseAndEraseIfLastRef(handle);
    dummy_handles_.pop_back();
    cache_allocated_size_ -= kSizeDummyEntry;
  }
  return return_status;
}

template <CacheEntryRole R>
std::size_t CacheReservationManagerImpl<R>::GetTotalReservedCacheSize() {
  return cache_allocated_size_.load(std::memory_order_relaxed);
}

template <CacheEntryRole R>
std::size_t CacheReservationManagerImpl<R>::GetTotalMemoryUsed() {
  return memory_used_;
}

template <CacheEntryRole R>
Slice CacheReservationManagerImpl<R>::GetNextCacheKey() {
  // Calling this function will have the side-effect of changing the
  // underlying cache_key_ that is shared among other keys generated from this
  // fucntion. Therefore please make sure the previous keys are saved/copied
  // before calling this function.
  cache_key_ = CacheKey::CreateUniqueForCacheLifetime(cache_.get());
  return cache_key_.AsSlice();
}

template <CacheEntryRole R>
const Cache::CacheItemHelper*
CacheReservationManagerImpl<R>::TEST_GetCacheItemHelperForRole() {
  return CacheInterface::GetHelper();
}

template class CacheReservationManagerImpl<
    CacheEntryRole::kBlockBasedTableReader>;
template class CacheReservationManagerImpl<
    CacheEntryRole::kCompressionDictionaryBuildingBuffer>;
template class CacheReservationManagerImpl<CacheEntryRole::kFilterConstruction>;
template class CacheReservationManagerImpl<CacheEntryRole::kMisc>;
template class CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>;
template class CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>;
template class CacheReservationManagerImpl<CacheEntryRole::kBlobCache>;
template class CacheReservationManagerImpl<CacheEntryRole::kReadaheadBuffer>;
}  // namespace ROCKSDB_NAMESPACE
