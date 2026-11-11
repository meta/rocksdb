//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"

#include <algorithm>
#include <string_view>

#include "db/dbformat.h"
#include "rocksdb/trace_record.h"

namespace ROCKSDB_NAMESPACE {

namespace {
constexpr std::string_view kGhostCachePrefix = "ghost_";
}  // namespace

GhostCache::GhostCache(std::shared_ptr<Cache> sim_cache)
    : sim_cache_(sim_cache) {}

bool GhostCache::Admit(const Slice& lookup_key) {
  auto handle = sim_cache_->Lookup(lookup_key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    return true;
  }
  // TODO: Should we check for errors here?
  auto s = sim_cache_->Insert(lookup_key, /*obj=*/nullptr,
                              &kNoopCacheItemHelper, lookup_key.size());
  s.PermitUncheckedError();
  return false;
}

CacheSimulator::CacheSimulator(std::unique_ptr<GhostCache>&& ghost_cache,
                               std::shared_ptr<Cache> sim_cache)
    : ghost_cache_(std::move(ghost_cache)), sim_cache_(sim_cache) {}

void CacheSimulator::Access(const BlockCacheTraceRecord& access) {
  bool admit = true;
  const bool is_user_access =
      BlockCacheTraceHelper::IsUserAccess(access.caller);
  bool is_cache_miss = true;
  if (ghost_cache_ && !access.no_insert) {
    admit = ghost_cache_->Admit(access.block_key);
  }
  auto handle = sim_cache_->Lookup(access.block_key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    is_cache_miss = false;
  } else {
    if (!access.no_insert && admit && access.block_size > 0) {
      // Ignore errors on insert
      auto s = sim_cache_->Insert(access.block_key, /*obj=*/nullptr,
                                  &kNoopCacheItemHelper, access.block_size);
      s.PermitUncheckedError();
    }
  }
  miss_ratio_stats_.UpdateMetrics(access.access_timestamp, is_user_access,
                                  is_cache_miss);
}

void MissRatioStats::UpdateMetrics(uint64_t timestamp_in_ms,
                                   bool is_user_access, bool is_cache_miss) {
  uint64_t timestamp_in_seconds = timestamp_in_ms / kMicrosInSecond;
  num_accesses_timeline_[timestamp_in_seconds] += 1;
  num_accesses_ += 1;
  if (num_misses_timeline_.find(timestamp_in_seconds) ==
      num_misses_timeline_.end()) {
    num_misses_timeline_[timestamp_in_seconds] = 0;
  }
  if (is_cache_miss) {
    num_misses_ += 1;
    num_misses_timeline_[timestamp_in_seconds] += 1;
  }
  if (is_user_access) {
    user_accesses_ += 1;
    if (is_cache_miss) {
      user_misses_ += 1;
    }
  }
}

Cache::Priority PrioritizedCacheSimulator::ComputeBlockPriority(
    const BlockCacheTraceRecord& access) const {
  if (access.block_type == TraceType::kBlockTraceFilterBlock ||
      access.block_type == TraceType::kBlockTraceIndexBlock ||
      access.block_type == TraceType::kBlockTraceUncompressionDictBlock) {
    return Cache::Priority::HIGH;
  }
  return Cache::Priority::LOW;
}

void PrioritizedCacheSimulator::AccessKVPair(
    const Slice& key, uint64_t value_size, Cache::Priority priority,
    const BlockCacheTraceRecord& access, bool no_insert, bool is_user_access,
    bool* is_cache_miss, bool* admitted, bool update_metrics) {
  assert(is_cache_miss);
  assert(admitted);
  *is_cache_miss = true;
  *admitted = true;
  if (ghost_cache_ && !no_insert) {
    *admitted = ghost_cache_->Admit(key);
  }
  auto handle = sim_cache_->Lookup(key);
  if (handle != nullptr) {
    sim_cache_->Release(handle);
    *is_cache_miss = false;
  } else if (!no_insert && *admitted && value_size > 0) {
    // TODO: Should we check for an error here?
    auto s = sim_cache_->Insert(key, /*obj=*/nullptr, &kNoopCacheItemHelper,
                                value_size,
                                /*handle=*/nullptr, priority);
    s.PermitUncheckedError();
  }
  if (update_metrics) {
    miss_ratio_stats_.UpdateMetrics(access.access_timestamp, is_user_access,
                                    *is_cache_miss);
  }
}

void PrioritizedCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  bool is_cache_miss = true;
  bool admitted = true;
  AccessKVPair(access.block_key, access.block_size,
               ComputeBlockPriority(access), access, access.no_insert,
               BlockCacheTraceHelper::IsUserAccess(access.caller),
               &is_cache_miss, &admitted, /*update_metrics=*/true);
}

void HybridRowBlockCacheSimulator::Access(const BlockCacheTraceRecord& access) {
  // TODO (haoyu): We only support Get for now. We need to extend the tracing
  // for MultiGet, i.e., non-data block accesses must log all keys in a
  // MultiGet.
  bool is_cache_miss = true;
  bool admitted = false;
  if (access.caller == TableReaderCaller::kUserGet &&
      access.get_id != BlockCacheTraceHelper::kReservedGetId) {
    // This is a Get request.
    const std::string& row_key = BlockCacheTraceHelper::ComputeRowKey(access);
    GetRequestStatus& status = getid_status_map_[access.get_id];
    if (status.is_complete) {
      // This Get request completes.
      // Skip future accesses to its index/filter/data
      // blocks. These block lookups are unnecessary if we observe a hit for the
      // referenced key-value pair already. Thus, we treat these lookups as
      // hits. This is also to ensure the total number of accesses are the same
      // when comparing to other policies.
      miss_ratio_stats_.UpdateMetrics(access.access_timestamp,
                                      /*is_user_access=*/true,
                                      /*is_cache_miss=*/false);
      return;
    }
    if (status.row_key_status.find(row_key) == status.row_key_status.end()) {
      // This is the first time that this key is accessed. Look up the key-value
      // pair first. Do not update the miss/accesses metrics here since it will
      // be updated later.
      AccessKVPair(row_key, access.referenced_data_size, Cache::Priority::HIGH,
                   access,
                   /*no_insert=*/false,
                   /*is_user_access=*/true, &is_cache_miss, &admitted,
                   /*update_metrics=*/false);
      InsertResult result = InsertResult::NO_INSERT;
      if (admitted && access.referenced_data_size > 0) {
        result = InsertResult::INSERTED;
      } else if (admitted) {
        result = InsertResult::ADMITTED;
      }
      status.row_key_status[row_key] = result;
    }
    if (!is_cache_miss) {
      // A cache hit.
      status.is_complete = true;
      miss_ratio_stats_.UpdateMetrics(access.access_timestamp,
                                      /*is_user_access=*/true,
                                      /*is_cache_miss=*/false);
      return;
    }
    // The row key-value pair observes a cache miss. We need to access its
    // index/filter/data blocks.
    InsertResult inserted = status.row_key_status[row_key];
    AccessKVPair(
        access.block_key, access.block_size, ComputeBlockPriority(access),
        access,
        /*no_insert=*/!insert_blocks_upon_row_kvpair_miss_ || access.no_insert,
        /*is_user_access=*/true, &is_cache_miss, &admitted,
        /*update_metrics=*/true);
    if (access.referenced_data_size > 0 && inserted == InsertResult::ADMITTED) {
      // TODO: Should we check for an error here?
      auto s =
          sim_cache_->Insert(row_key, /*obj=*/nullptr, &kNoopCacheItemHelper,
                             access.referenced_data_size,
                             /*handle=*/nullptr, Cache::Priority::HIGH);
      s.PermitUncheckedError();
      status.row_key_status[row_key] = InsertResult::INSERTED;
    }
    return;
  }
  AccessKVPair(access.block_key, access.block_size,
               ComputeBlockPriority(access), access, access.no_insert,
               BlockCacheTraceHelper::IsUserAccess(access.caller),
               &is_cache_miss, &admitted, /*update_metrics=*/true);
}

BlockCacheTraceSimulator::BlockCacheTraceSimulator(
    uint64_t warmup_seconds, uint32_t downsample_ratio,
    const std::vector<CacheConfiguration>& cache_configurations)
    : warmup_seconds_(warmup_seconds),
      downsample_ratio_(downsample_ratio),
      cache_configurations_(cache_configurations) {}

Status BlockCacheTraceSimulator::InitializeCaches() {
  for (auto const& config : cache_configurations_) {
    for (auto cache_capacity : config.cache_capacities) {
      // Scale down the cache capacity since the trace contains accesses on
      // 1/'downsample_ratio' blocks.
      uint64_t simulate_cache_capacity = cache_capacity / downsample_ratio_;
      std::shared_ptr<CacheSimulator> sim_cache;
      std::unique_ptr<GhostCache> ghost_cache;
      std::string cache_name = config.cache_name;
      if (cache_name.find(kGhostCachePrefix) != std::string::npos) {
        ghost_cache.reset(new GhostCache(
            NewLRUCache(config.ghost_cache_capacity, /*num_shard_bits=*/1,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0)));
        cache_name = cache_name.substr(kGhostCachePrefix.size());
      }
      if (cache_name == "lru") {
        sim_cache = std::make_shared<CacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0));
      } else if (cache_name == "lru_priority") {
        sim_cache = std::make_shared<PrioritizedCacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5));
      } else if (cache_name == "lru_hybrid") {
        sim_cache = std::make_shared<HybridRowBlockCacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5),
            /*insert_blocks_upon_row_kvpair_miss=*/true);
      } else if (cache_name == "lru_hybrid_no_insert_on_row_miss") {
        sim_cache = std::make_shared<HybridRowBlockCacheSimulator>(
            std::move(ghost_cache),
            NewLRUCache(simulate_cache_capacity, config.num_shard_bits,
                        /*strict_capacity_limit=*/false,
                        /*high_pri_pool_ratio=*/0.5),
            /*insert_blocks_upon_row_kvpair_miss=*/false);
      } else {
        // Not supported.
        return Status::InvalidArgument("Unknown cache name " +
                                       config.cache_name);
      }
      sim_caches_[config].push_back(sim_cache);
    }
  }
  return Status::OK();
}

void BlockCacheTraceSimulator::Access(const BlockCacheTraceRecord& access) {
  if (trace_start_time_ == 0) {
    trace_start_time_ = access.access_timestamp;
  }
  // access.access_timestamp is in microseconds.
  if (!warmup_complete_ &&
      trace_start_time_ + warmup_seconds_ * kMicrosInSecond <=
          access.access_timestamp) {
    for (auto& config_caches : sim_caches_) {
      for (auto& sim_cache : config_caches.second) {
        sim_cache->reset_counter();
      }
    }
    warmup_complete_ = true;
  }
  for (auto& config_caches : sim_caches_) {
    for (auto& sim_cache : config_caches.second) {
      sim_cache->Access(access);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
