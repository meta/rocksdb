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
#include <cstdint>
#include <string>

#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/advanced_cache.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// Optional base class for classes implementing the CacheShard concept
class CacheShardBase {
 public:
  explicit CacheShardBase(CacheMetadataChargePolicy metadata_charge_policy)
      : metadata_charge_policy_(metadata_charge_policy) {}

  using DeleterFn = Cache::DeleterFn;

  // Expected by concept CacheShard (TODO with C++20 support)
  // Some Defaults
  std::string GetPrintableOptions() const { return ""; }
  using HashVal = uint64_t;
  using HashCref = uint64_t;
  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    return GetSliceNPHash64(key, seed);
  }
  static inline uint32_t HashPieceForSharding(HashCref hash) {
    return Lower32of64(hash);
  }
  void AppendPrintableOptions(std::string& /*str*/) const {}

  // Must be provided for concept CacheShard (TODO with C++20 support)
  /*
  struct HandleImpl {  // for concept HandleImpl
    HashVal hash;
    HashCref GetHash() const;
    ...
  };
  Status Insert(const Slice& key, HashCref hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                HandleImpl** handle, Cache::Priority priority,
                bool standalone) = 0;
  Handle* CreateStandalone(const Slice& key, HashCref hash, ObjectPtr obj,
                           const CacheItemHelper* helper,
                           size_t charge, bool allow_uncharged) = 0;
  HandleImpl* Lookup(const Slice& key, HashCref hash,
                        const Cache::CacheItemHelper* helper,
                        Cache::CreateContext* create_context,
                        Cache::Priority priority,
                        Statistics* stats) = 0;
  bool Release(HandleImpl* handle, bool useful, bool erase_if_last_ref) = 0;
  bool Ref(HandleImpl* handle) = 0;
  void Erase(const Slice& key, HashCref hash) = 0;
  void SetCapacity(size_t capacity) = 0;
  void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;
  size_t GetUsage() const = 0;
  size_t GetPinnedUsage() const = 0;
  size_t GetOccupancyCount() const = 0;
  size_t GetTableAddressCount() const = 0;
  // Handles iterating over roughly `average_entries_per_lock` entries, using
  // `state` to somehow record where it last ended up. Caller initially uses
  // *state == 0 and implementation sets *state = SIZE_MAX to indicate
  // completion.
  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state) = 0;
  void EraseUnRefEntries() = 0;
  */

 protected:
  const CacheMetadataChargePolicy metadata_charge_policy_;
};

// Portions of ShardedCache that do not depend on the template parameter
class ShardedCacheBase : public Cache {
 public:
  explicit ShardedCacheBase(const ShardedCacheOptions& opts);
  ~ShardedCacheBase() override = default;

  int GetNumShardBits() const;
  uint32_t GetNumShards() const;

  uint64_t NewId() override;

  bool HasStrictCapacityLimit() const override;
  size_t GetCapacity() const override;
  Status GetSecondaryCacheCapacity(size_t& size) const override;
  Status GetSecondaryCachePinnedUsage(size_t& size) const override;

  using Cache::GetUsage;
  size_t GetUsage(Handle* handle) const override;
  std::string GetPrintableOptions() const override;

  uint32_t GetHashSeed() const override { return hash_seed_; }

 protected:  // fns
  virtual void AppendPrintableOptions(std::string& str) const = 0;
  size_t GetPerShardCapacity() const;
  size_t ComputePerShardCapacity(size_t capacity) const;

 protected:                        // data
  std::atomic<uint64_t> last_id_;  // For NewId
  const uint32_t shard_mask_;
  const uint32_t hash_seed_;

  // Dynamic configuration parameters, guarded by config_mutex_
  bool strict_capacity_limit_;
  size_t capacity_;
  mutable port::Mutex config_mutex_;
};

// Generic cache interface that shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are typically sharded by the lowest num_shard_bits bits of hash value
// so that the upper bits of the hash value can keep a stable ordering of
// table entries even as the table grows (using more upper hash bits).
// See CacheShardBase above for what is expected of the CacheShard parameter.
template <class CacheShard>
class ShardedCache : public ShardedCacheBase {
 public:
  using HashVal = typename CacheShard::HashVal;
  using HashCref = typename CacheShard::HashCref;
  using HandleImpl = typename CacheShard::HandleImpl;

  explicit ShardedCache(const ShardedCacheOptions& opts)
      : ShardedCacheBase(opts),
        shards_(static_cast<CacheShard*>(port::cacheline_aligned_alloc(
            sizeof(CacheShard) * GetNumShards()))),
        destroy_shards_in_dtor_(false) {}

  // No copying allowed
  ShardedCache(const ShardedCache&) = delete;
  ShardedCache& operator=(const ShardedCache&) = delete;
  ShardedCache(ShardedCache&&) = delete;
  ShardedCache& operator=(ShardedCache&&) = delete;

  ~ShardedCache() override {
    if (destroy_shards_in_dtor_) {
      ForEachShard([](CacheShard* cs) { cs->~CacheShard(); });
    }
    port::cacheline_aligned_free(shards_);
  }

  CacheShard& GetShard(HashCref hash) {
    return shards_[CacheShard::HashPieceForSharding(hash) & shard_mask_];
  }

  const CacheShard& GetShard(HashCref hash) const {
    return shards_[CacheShard::HashPieceForSharding(hash) & shard_mask_];
  }

  void SetCapacity(size_t capacity) override {
    MutexLock l(&config_mutex_);
    capacity_ = capacity;
    auto per_shard = ComputePerShardCapacity(capacity);
    ForEachShard([=](CacheShard* cs) { cs->SetCapacity(per_shard); });
  }

  void SetStrictCapacityLimit(bool s_c_l) override {
    MutexLock l(&config_mutex_);
    strict_capacity_limit_ = s_c_l;
    ForEachShard(
        [s_c_l](CacheShard* cs) { cs->SetStrictCapacityLimit(s_c_l); });
  }

  Status Insert(
      const Slice& key, ObjectPtr obj, const CacheItemHelper* helper,
      size_t charge, Handle** handle = nullptr,
      Priority priority = Priority::LOW,
      const Slice& /*compressed_value*/ = Slice(),
      CompressionType /*type*/ = CompressionType::kNoCompression) override {
    assert(helper);
    HashVal hash = CacheShard::ComputeHash(key, hash_seed_);
    auto h_out = reinterpret_cast<HandleImpl**>(handle);
    return GetShard(hash).Insert(key, hash, obj, helper, charge, h_out,
                                 priority);
  }

  Handle* CreateStandalone(const Slice& key, ObjectPtr obj,
                           const CacheItemHelper* helper, size_t charge,
                           bool allow_uncharged) override {
    assert(helper);
    HashVal hash = CacheShard::ComputeHash(key, hash_seed_);
    HandleImpl* result = GetShard(hash).CreateStandalone(
        key, hash, obj, helper, charge, allow_uncharged);
    return static_cast<Handle*>(result);
  }

  Handle* Lookup(const Slice& key, const CacheItemHelper* helper = nullptr,
                 CreateContext* create_context = nullptr,
                 Priority priority = Priority::LOW,
                 Statistics* stats = nullptr) override {
    HashVal hash = CacheShard::ComputeHash(key, hash_seed_);
    HandleImpl* result = GetShard(hash).Lookup(key, hash, helper,
                                               create_context, priority, stats);
    return static_cast<Handle*>(result);
  }

  void Erase(const Slice& key) override {
    HashVal hash = CacheShard::ComputeHash(key, hash_seed_);
    GetShard(hash).Erase(key, hash);
  }

  bool Release(Handle* handle, bool useful,
               bool erase_if_last_ref = false) override {
    auto h = static_cast<HandleImpl*>(handle);
    return GetShard(h->GetHash()).Release(h, useful, erase_if_last_ref);
  }
  bool Ref(Handle* handle) override {
    auto h = static_cast<HandleImpl*>(handle);
    return GetShard(h->GetHash()).Ref(h);
  }
  bool Release(Handle* handle, bool erase_if_last_ref = false) override {
    return Release(handle, true /*useful*/, erase_if_last_ref);
  }
  using ShardedCacheBase::GetUsage;
  size_t GetUsage() const override {
    return SumOverShards2(&CacheShard::GetUsage);
  }
  size_t GetPinnedUsage() const override {
    return SumOverShards2(&CacheShard::GetPinnedUsage);
  }
  size_t GetOccupancyCount() const override {
    return SumOverShards2(&CacheShard::GetOccupancyCount);
  }
  size_t GetTableAddressCount() const override {
    return SumOverShards2(&CacheShard::GetTableAddressCount);
  }
  void ApplyToAllEntries(
      const std::function<void(const Slice& key, ObjectPtr value, size_t charge,
                               const CacheItemHelper* helper)>& callback,
      const ApplyToAllEntriesOptions& opts) override {
    uint32_t num_shards = GetNumShards();
    // Iterate over part of each shard, rotating between shards, to
    // minimize impact on latency of concurrent operations.
    std::unique_ptr<size_t[]> states(new size_t[num_shards]{});

    size_t aepl = opts.average_entries_per_lock;
    aepl = std::max(aepl, size_t{1});

    bool remaining_work;
    do {
      remaining_work = false;
      for (uint32_t i = 0; i < num_shards; i++) {
        if (states[i] != SIZE_MAX) {
          shards_[i].ApplyToSomeEntries(callback, aepl, &states[i]);
          remaining_work |= states[i] != SIZE_MAX;
        }
      }
    } while (remaining_work);
  }

  void EraseUnRefEntries() override {
    ForEachShard([](CacheShard* cs) { cs->EraseUnRefEntries(); });
  }

  void DisownData() override {
    // Leak data only if that won't generate an ASAN/valgrind warning.
    if (!kMustFreeHeapAllocations) {
      destroy_shards_in_dtor_ = false;
    }
  }

 protected:
  inline void ForEachShard(const std::function<void(CacheShard*)>& fn) {
    uint32_t num_shards = GetNumShards();
    for (uint32_t i = 0; i < num_shards; i++) {
      fn(shards_ + i);
    }
  }

  inline void ForEachShard(
      const std::function<void(const CacheShard*)>& fn) const {
    uint32_t num_shards = GetNumShards();
    for (uint32_t i = 0; i < num_shards; i++) {
      fn(shards_ + i);
    }
  }

  inline size_t SumOverShards(
      const std::function<size_t(CacheShard&)>& fn) const {
    uint32_t num_shards = GetNumShards();
    size_t result = 0;
    for (uint32_t i = 0; i < num_shards; i++) {
      result += fn(shards_[i]);
    }
    return result;
  }

  inline size_t SumOverShards2(size_t (CacheShard::*fn)() const) const {
    return SumOverShards([fn](CacheShard& cs) { return (cs.*fn)(); });
  }

  // Must be called exactly once by derived class constructor
  void InitShards(const std::function<void(CacheShard*)>& placement_new) {
    ForEachShard(placement_new);
    destroy_shards_in_dtor_ = true;
  }

  void AppendPrintableOptions(std::string& str) const override {
    shards_[0].AppendPrintableOptions(str);
  }

 private:
  CacheShard* const shards_;
  bool destroy_shards_in_dtor_;
};

// 512KB is traditional minimum shard size.
int GetDefaultCacheShardBits(size_t capacity,
                             size_t min_shard_size = 512U * 1024U);

}  // namespace ROCKSDB_NAMESPACE
