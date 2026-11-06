// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Various APIs for configuring, creating, and monitoring read caches.

#pragma once

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "rocksdb/compression_type.h"
#include "rocksdb/data_structure.h"
#include "rocksdb/memory_allocator.h"

namespace ROCKSDB_NAMESPACE {

class Cache;  // defined in advanced_cache.h
struct ConfigOptions;
class SecondaryCache;

// These definitions begin source compatibility for a future change in which
// a specific class for block cache is split away from general caches, so that
// the block cache API can continue to become more specialized and
// customizeable, including in ways incompatible with a general cache. For
// example, HyperClockCache is not usable as a general cache because it expects
// only fixed-size block cache keys, but this limitation is not yet reflected
// in the API function signatures.
// * Phase 1 (done) - Make both BlockCache and RowCache aliases for Cache,
// and make a factory function for row caches. Encourage users of row_cache
// (not common) to switch to the factory function for row caches.
// * Phase 2 - Split off RowCache as its own class, removing secondary
// cache support features and more from the API to simplify it. Between Phase 1
// and Phase 2 users of row_cache will need to update their code. Any time
// after Phase 2, the block cache and row cache APIs can become more specialized
// in ways incompatible with general caches.
// * Phase 3 - Move existing RocksDB uses of Cache to BlockCache, and deprecate
// (but not yet remove) Cache as an alias for BlockCache.
using BlockCache = Cache;
using RowCache = Cache;

// Classifications of block cache entries.
//
// Developer notes: Adding a new enum to this class requires corresponding
// updates to `kCacheEntryRoleToCamelString` and
// `kCacheEntryRoleToHyphenString`. Do not add to this enum after `kMisc` since
// `kNumCacheEntryRoles` assumes `kMisc` comes last.
enum class CacheEntryRole {
  // Block-based table data block
  kDataBlock,
  // Block-based table filter block (full or partitioned)
  kFilterBlock,
  // Block-based table metadata block for partitioned filter
  kFilterMetaBlock,
  // OBSOLETE / DEPRECATED: old/removed block-based filter
  kDeprecatedFilterBlock,
  // Block-based table index block
  kIndexBlock,
  // Other kinds of block-based table block
  kOtherBlock,
  // WriteBufferManager's charge to account for its memtable usage
  kWriteBuffer,
  // Compression dictionary building buffer's charge to account for
  // its memory usage
  kCompressionDictionaryBuildingBuffer,
  // Filter's charge to account for
  // (new) bloom and ribbon filter construction's memory usage
  kFilterConstruction,
  // BlockBasedTableReader's charge to account for its memory usage
  kBlockBasedTableReader,
  // FileMetadata's charge to account for its memory usage
  kFileMetadata,
  // Blob value (when using the same cache as block cache and blob cache)
  kBlobValue,
  // Blob cache's charge to account for its memory usage (when using a
  // separate block cache and blob cache)
  kBlobCache,
  // Default bucket, for miscellaneous cache entries. Do not use for
  // entries that could potentially add up to large usage.
  kMisc,
};
constexpr uint32_t kNumCacheEntryRoles =
    static_cast<uint32_t>(CacheEntryRole::kMisc) + 1;

// Obtain a hyphen-separated, lowercase name of a `CacheEntryRole`.
const std::string& GetCacheEntryRoleName(CacheEntryRole);

// A fast bit set for CacheEntryRoles
using CacheEntryRoleSet = SmallEnumSet<CacheEntryRole, CacheEntryRole::kMisc>;

// For use with `GetMapProperty()` for property
// `DB::Properties::kBlockCacheEntryStats`. On success, the map will
// be populated with all keys that can be obtained from these functions.
struct BlockCacheEntryStatsMapKeys {
  static const std::string& CacheId();
  static const std::string& CacheCapacityBytes();
  static const std::string& LastCollectionDurationSeconds();
  static const std::string& LastCollectionAgeSeconds();

  static std::string EntryCount(CacheEntryRole);
  static std::string UsedBytes(CacheEntryRole);
  static std::string UsedPercent(CacheEntryRole);
};

extern const bool kDefaultToAdaptiveMutex;

enum CacheMetadataChargePolicy {
  // Only the `charge` of each entry inserted into a Cache counts against
  // the `capacity`
  kDontChargeCacheMetadata,
  // In addition to the `charge`, the approximate space overheads in the
  // Cache (in bytes) also count against `capacity`. These space overheads
  // are for supporting fast Lookup and managing the lifetime of entries.
  kFullChargeCacheMetadata
};
const CacheMetadataChargePolicy kDefaultCacheMetadataChargePolicy =
    kFullChargeCacheMetadata;

// Options shared between various cache implementations that
// divide the key space into shards using hashing.
struct ShardedCacheOptions {
  // Capacity of the cache, in the same units as the `charge` of each entry.
  // This is typically measured in bytes, but can be a different unit if using
  // kDontChargeCacheMetadata.
  size_t capacity = 0;

  // Cache is sharded into 2^num_shard_bits shards, by hash of key.
  // If < 0, a good default is chosen based on the capacity and the
  // implementation. (Mutex-based implementations are much more reliant
  // on many shards for parallel scalability.)
  int num_shard_bits = -1;

  // If strict_capacity_limit is set, Insert() will fail if there is not
  // enough capacity for the new entry along with all the existing referenced
  // (pinned) cache entries. (Unreferenced cache entries are evicted as
  // needed, sometimes immediately.) If strict_capacity_limit == false
  // (default), Insert() never fails.
  bool strict_capacity_limit = false;

  // If non-nullptr, RocksDB will use this allocator instead of system
  // allocator when allocating memory for cache blocks.
  //
  // Caveat: when the cache is used as block cache, the memory allocator is
  // ignored when dealing with compression libraries that allocate memory
  // internally (currently only XPRESS).
  std::shared_ptr<MemoryAllocator> memory_allocator;

  // See CacheMetadataChargePolicy
  CacheMetadataChargePolicy metadata_charge_policy =
      kDefaultCacheMetadataChargePolicy;

  // A SecondaryCache instance to use the non-volatile tier. For a RowCache
  // this option must be kept as default empty.
  std::shared_ptr<SecondaryCache> secondary_cache;

  // See hash_seed comments below
  static constexpr int32_t kQuasiRandomHashSeed = -1;
  static constexpr int32_t kHostHashSeed = -2;

  // EXPERT OPTION: Specifies how a hash seed should be determined for the
  // cache, or specifies a specific seed (only recommended for diagnostics or
  // testing).
  //
  // Background: it could be dangerous to have different cache instances
  // access the same SST files with the same hash seed, as correlated unlucky
  // hashing across hosts or restarts could cause a widespread issue, rather
  // than an isolated one. For example, with smaller block caches, it is
  // possible for large full Bloom filters in a set of SST files to be randomly
  // clustered into one cache shard, causing mutex contention or a thrashing
  // condition as there's little or no space left for other entries assigned to
  // the shard. If a set of SST files is broadcast and used on many hosts, we
  // should ensure all have an independent chance of balanced shards.
  //
  // Values >= 0 will be treated as fixed hash seeds. Values < 0 are reserved
  // for methods of dynamically choosing a seed, currently:
  // * kQuasiRandomHashSeed - Each cache created chooses a seed mostly randomly,
  //   except that within a process, no seed is repeated until all have been
  //   issued.
  // * kHostHashSeed - The seed is determined based on hashing the host name.
  //   Although this is arguably slightly worse for production reliability, it
  //   solves the essential problem of cross-host correlation while ensuring
  //   repeatable behavior on a host, for diagnostic purposes.
  int32_t hash_seed = kHostHashSeed;

  ShardedCacheOptions() {}
  ShardedCacheOptions(
      size_t _capacity, int _num_shard_bits, bool _strict_capacity_limit,
      std::shared_ptr<MemoryAllocator> _memory_allocator = nullptr,
      CacheMetadataChargePolicy _metadata_charge_policy =
          kDefaultCacheMetadataChargePolicy)
      : capacity(_capacity),
        num_shard_bits(_num_shard_bits),
        strict_capacity_limit(_strict_capacity_limit),
        memory_allocator(std::move(_memory_allocator)),
        metadata_charge_policy(_metadata_charge_policy) {}
  // Make ShardedCacheOptions polymorphic
  virtual ~ShardedCacheOptions() = default;
};

// LRUCache - A cache using LRU eviction to stay at or below a set capacity.
// The cache is sharded to 2^num_shard_bits shards, by hash of the key.
// The total capacity is divided and evenly assigned to each shard, and each
// shard has its own LRU list for evictions. Each shard also has a mutex for
// exclusive access during operations; even read operations need exclusive
// access in order to update the LRU list. Mutex contention is usually low
// with enough shards. However,
// * For a single hot block, there will be mutex contention even for reads
// regardless of the number of shards.
// * LRUCaches in the size of MBs instead of GBs can have shards small enough
// that there is a random probability of some modest number of large blocks
// (especially non-partitioned filters) thrashing a single cache shard.
//
// HYPERCLOCKCACHE IS NOW GENERALLY RECOMMENDED OVER LRUCACHE. See
// HyperClockCacheOptions below.
struct LRUCacheOptions : public ShardedCacheOptions {
  // Ratio of cache reserved for high-priority and low-priority entries,
  // respectively. (See Cache::Priority below more information on the levels.)
  // Valid values are between 0 and 1 (inclusive), and the sum of the two
  // values cannot exceed 1.
  //
  // If high_pri_pool_ratio is greater than zero, a dedicated high-priority LRU
  // list is maintained by the cache. A ratio of 0.5 means non-high-priority
  // entries will use midpoint insertion. Similarly, if low_pri_pool_ratio is
  // greater than zero, a dedicated low-priority LRU list is maintained.
  // There is also a bottom-priority LRU list, which is always enabled and not
  // explicitly configurable. Entries are spilled over to the next available
  // lower-priority pool if a certain pool's capacity is exceeded.
  //
  // Entries with cache hits are inserted into the highest priority LRU list
  // available regardless of the entry's priority. Entries without hits
  // are inserted into highest priority LRU list available whose priority
  // does not exceed the entry's priority. (For example, high-priority items
  // with no hits are placed in the high-priority pool if available;
  // otherwise, they are placed in the low-priority pool if available;
  // otherwise, they are placed in the bottom-priority pool.) This results
  // in lower-priority entries without hits getting evicted from the cache
  // sooner.
  double high_pri_pool_ratio = 0.5;
  double low_pri_pool_ratio = 0.0;

  // Whether to use adaptive mutexes for cache shards. Note that adaptive
  // mutexes need to be supported by the platform in order for this to have any
  // effect. The default value is true if RocksDB is compiled with
  // -DROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX, false otherwise.
  bool use_adaptive_mutex = kDefaultToAdaptiveMutex;

  LRUCacheOptions() {}
  LRUCacheOptions(size_t _capacity, int _num_shard_bits,
                  bool _strict_capacity_limit, double _high_pri_pool_ratio,
                  std::shared_ptr<MemoryAllocator> _memory_allocator = nullptr,
                  bool _use_adaptive_mutex = kDefaultToAdaptiveMutex,
                  CacheMetadataChargePolicy _metadata_charge_policy =
                      kDefaultCacheMetadataChargePolicy,
                  double _low_pri_pool_ratio = 0.0)
      : ShardedCacheOptions(_capacity, _num_shard_bits, _strict_capacity_limit,
                            std::move(_memory_allocator),
                            _metadata_charge_policy),
        high_pri_pool_ratio(_high_pri_pool_ratio),
        low_pri_pool_ratio(_low_pri_pool_ratio),
        use_adaptive_mutex(_use_adaptive_mutex) {}

  // Construct an instance of LRUCache using these options
  std::shared_ptr<Cache> MakeSharedCache() const;

  // Construct an instance of LRUCache for use as a row cache, typically for
  // `DBOptions::row_cache`. Some options are not relevant to row caches.
  std::shared_ptr<RowCache> MakeSharedRowCache() const;
};

// DEPRECATED wrapper function
inline std::shared_ptr<Cache> NewLRUCache(
    size_t capacity, int num_shard_bits = -1,
    bool strict_capacity_limit = false, double high_pri_pool_ratio = 0.5,
    std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
    bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
    CacheMetadataChargePolicy metadata_charge_policy =
        kDefaultCacheMetadataChargePolicy,
    double low_pri_pool_ratio = 0.0) {
  return LRUCacheOptions(capacity, num_shard_bits, strict_capacity_limit,
                         high_pri_pool_ratio, memory_allocator,
                         use_adaptive_mutex, metadata_charge_policy,
                         low_pri_pool_ratio)
      .MakeSharedCache();
}

// DEPRECATED wrapper function
inline std::shared_ptr<Cache> NewLRUCache(const LRUCacheOptions& cache_opts) {
  return cache_opts.MakeSharedCache();
}

// EXPERIMENTAL
// Options structure for configuring a SecondaryCache instance with in-memory
// compression. The implementation uses LRUCache so inherits its options,
// except LRUCacheOptions.secondary_cache is not used and should not be set.
struct CompressedSecondaryCacheOptions : LRUCacheOptions {
  // The compression method (if any) that is used to compress data.
  CompressionType compression_type = CompressionType::kLZ4Compression;

  // Options specific to the compression algorithm
  CompressionOptions compression_opts;

  // Enable the custom split and merge feature, which split the compressed value
  // into chunks so that they may better fit jemalloc bins.
  bool enable_custom_split_merge = false;

  // Kinds of entries that should not be compressed, but can be stored.
  // (Filter blocks are essentially non-compressible but others usually are.)
  CacheEntryRoleSet do_not_compress_roles = {CacheEntryRole::kFilterBlock};

  CompressedSecondaryCacheOptions() {}
  CompressedSecondaryCacheOptions(
      size_t _capacity, int _num_shard_bits, bool _strict_capacity_limit,
      double _high_pri_pool_ratio, double _low_pri_pool_ratio = 0.0,
      std::shared_ptr<MemoryAllocator> _memory_allocator = nullptr,
      bool _use_adaptive_mutex = kDefaultToAdaptiveMutex,
      CacheMetadataChargePolicy _metadata_charge_policy =
          kDefaultCacheMetadataChargePolicy,
      CompressionType _compression_type = CompressionType::kLZ4Compression,
      bool _enable_custom_split_merge = false,
      const CacheEntryRoleSet& _do_not_compress_roles =
          {CacheEntryRole::kFilterBlock})
      : LRUCacheOptions(_capacity, _num_shard_bits, _strict_capacity_limit,
                        _high_pri_pool_ratio, std::move(_memory_allocator),
                        _use_adaptive_mutex, _metadata_charge_policy,
                        _low_pri_pool_ratio),
        compression_type(_compression_type),
        enable_custom_split_merge(_enable_custom_split_merge),
        do_not_compress_roles(_do_not_compress_roles) {}

  // Construct an instance of CompressedSecondaryCache using these options
  std::shared_ptr<SecondaryCache> MakeSharedSecondaryCache() const;

  // Avoid confusion with LRUCache
  std::shared_ptr<Cache> MakeSharedCache() const = delete;
};

// DEPRECATED wrapper function
inline std::shared_ptr<SecondaryCache> NewCompressedSecondaryCache(
    size_t capacity, int num_shard_bits = -1,
    bool strict_capacity_limit = false, double high_pri_pool_ratio = 0.5,
    double low_pri_pool_ratio = 0.0,
    std::shared_ptr<MemoryAllocator> memory_allocator = nullptr,
    bool use_adaptive_mutex = kDefaultToAdaptiveMutex,
    CacheMetadataChargePolicy metadata_charge_policy =
        kDefaultCacheMetadataChargePolicy,
    CompressionType compression_type = CompressionType::kLZ4Compression,
    bool enable_custom_split_merge = false,
    const CacheEntryRoleSet& _do_not_compress_roles = {
        CacheEntryRole::kFilterBlock}) {
  return CompressedSecondaryCacheOptions(
             capacity, num_shard_bits, strict_capacity_limit,
             high_pri_pool_ratio, low_pri_pool_ratio, memory_allocator,
             use_adaptive_mutex, metadata_charge_policy, compression_type,
             enable_custom_split_merge, _do_not_compress_roles)
      .MakeSharedSecondaryCache();
}

// DEPRECATED wrapper function
inline std::shared_ptr<SecondaryCache> NewCompressedSecondaryCache(
    const CompressedSecondaryCacheOptions& opts) {
  return opts.MakeSharedSecondaryCache();
}

// HyperClockCache (also known as HCC) - A lock-free Cache alternative for
// RocksDB block cache that offers much improved CPU efficiency vs. LRUCache
// under high parallel load or high contention. Additionally, HCC only uses
// sharding for a modest performance boost, so can use much larger cache shards
// than LRUCache, dramatically reducing the risk of thrashing in configurations
// or work loads with some large blocks.
//
// HYPERCLOCKCACHE IS NOW GENERALLY RECOMMENDED OVER LRUCACHE
//
// Some caveats:
// * Not a general Cache implementation: can only be used for
// BlockBasedTableOptions::block_cache, which RocksDB uses in a way that is
// compatible with HyperClockCache.
// * Cache priorities are less aggressively enforced, which could cause
// cache dilution from long range scans (unless they use fill_cache=false).
// * In some configurations, depends on anonymous mmap support, available in
// Linux, Windows and more.
// * May have slightly lower (or slightly higher) cache hit rate vs. LRUCache,
// because of the bounded counting-CLOCK eviction algorithm.
//
// See internal cache/clock_cache.h for full description.
struct HyperClockCacheOptions : public ShardedCacheOptions {
  // OPTIONAL: The estimated average `charge` associated with cache entries.
  //
  // When not provided (== 0, recommended and default), an HCC variant with a
  // dynamically-growing table and generally good performance is used. This
  // variant depends on anonymous mmaps so might not be available on all
  // platforms.
  //
  // If the average "charge" (uncompressed block size) of block cache entries
  // is reasonably predicted and provided here, the most efficient variant of
  // HCC is used. Performance is degraded if the prediction is inaccurate.
  // Prediction could be difficult or impossible with cache-charging features
  // such as WriteBufferManager. The best parameter choice based on a cache
  // in use is roughly given by GetUsage() / GetOccupancyCount(), though it is
  // better to estimate toward the lower side than the higher side when the
  // ratio might vary.
  size_t estimated_entry_charge;

  // When estimated_entry_charge == 0, this parameter establishes a promised
  // lower bound on the average charge of all entries in the table, which is
  // roughly the average uncompressed SST block size of block cache entries,
  // typically > 4KB. The default should generally suffice with almost no cost.
  // (This option is ignored for estimated_entry_charge > 0.)
  //
  // More detail: The table for indexing cache entries will grow automatically
  // as needed, but a hard upper bound on that size is needed at creation time.
  // The reason is that a contiguous memory mapping for the maximum size is
  // created, but memory pages are only mapped to physical (RSS) memory as
  // needed. If the average charge of all entries in the table falls below
  // this value, the table will operate below its full logical capacity (total
  // memory usage) because it has reached its physical capacity for efficiently
  // indexing entries. The hash table is never allowed to exceed a certain safe
  // load factor for efficient Lookup, Insert, etc.
  size_t min_avg_entry_charge = 450;

  // A tuning parameter to cap eviction CPU usage in a "thrashing" situation
  // by allowing the memory capacity to be exceeded slightly as needed. The
  // default setting should offer balanced protection against excessive CPU
  // and memory usage under extreme stress conditions, with no effect on
  // normal operation. Such stress conditions are proportionally more likely
  // with small caches (10s of MB or less) vs. large caches (GB-scale).
  // (NOTE: With the unusual setting of strict_capacity_limit=true, this
  // parameter is ignored.)
  //
  // BACKGROUND: Without some kind of limiter, inserting into a CLOCK-based
  // cache with no evictable entries (all "pinned") requires scanning the
  // entire cache to determine that nothing can be evicted. (By contrast,
  // LRU caches can determine no entries are evictable in O(1) time, but
  // require more synchronization/coordination on that eviction metadata.)
  // This aspect of a CLOCK cache can make a stressed situation worse by
  // bogging down the CPU with repeated scans of the cache. And with
  // strict_capacity_limit=false (normal setting), finding something evictable
  // doesn't change the outcome of insertion: the entry is inserted anyway
  // and the cache is allowed to exceed its target capacity if necessary.
  //
  // SOLUTION: Eviction is aborted upon seeing some number of pinned
  // entries before evicting anything, or if the ratio of pinned to evicted
  // is too high. This setting `eviction_effort_cap` essentially controls both
  // that allowed initial number of pinned entries and the maximum allowed
  // ratio. As the pinned size approaches the target cache capacity, roughly
  // 1/eviction_effort_cap additional portion of the capacity might be kept
  // in memory and evictable in order to keep CLOCK eviction reasonably
  // performant. Under the default setting and high stress conditions, this
  // memory overhead is around 3-5%. Under normal or even moderate stress
  // conditions, the memory overhead is negligible to zero.
  //
  // A large value like 1000 offers some protection with essentially no
  // memory overhead, while the minimum value of 1 could be useful for a
  // small cache where roughly doubling in size under stress could be OK to
  // keep operations very fast.
  int eviction_effort_cap = 30;

  explicit HyperClockCacheOptions(
      size_t _capacity, size_t _estimated_entry_charge = 0,
      int _num_shard_bits = -1, bool _strict_capacity_limit = false,
      std::shared_ptr<MemoryAllocator> _memory_allocator = nullptr,
      CacheMetadataChargePolicy _metadata_charge_policy =
          kDefaultCacheMetadataChargePolicy)
      : ShardedCacheOptions(_capacity, _num_shard_bits, _strict_capacity_limit,
                            std::move(_memory_allocator),
                            _metadata_charge_policy),
        estimated_entry_charge(_estimated_entry_charge) {}

  // Construct an instance of HyperClockCache using these options
  std::shared_ptr<Cache> MakeSharedCache() const;
};

// DEPRECATED - The old Clock Cache implementation had an unresolved bug and
// has been removed. The new HyperClockCache requires an additional
// configuration parameter that is not provided by this API. This function
// simply returns a new LRUCache for functional compatibility.
std::shared_ptr<Cache> NewClockCache(
    size_t capacity, int num_shard_bits = -1,
    bool strict_capacity_limit = false,
    CacheMetadataChargePolicy metadata_charge_policy =
        kDefaultCacheMetadataChargePolicy);

enum PrimaryCacheType {
  kCacheTypeLRU,  // LRU cache type
  kCacheTypeHCC,  // Hyper Clock Cache type
  kCacheTypeMax,
};

enum TieredAdmissionPolicy {
  // Automatically select the admission policy
  kAdmPolicyAuto,
  // During promotion/demotion, first time insert a placeholder entry, second
  // time insert the full entry if the placeholder is found, i.e insert on
  // second hit
  kAdmPolicyPlaceholder,
  // Same as kAdmPolicyPlaceholder, but also if an entry in the primary cache
  // was a hit, then force insert it into the compressed secondary cache
  kAdmPolicyAllowCacheHits,
  // An admission policy for three cache tiers - primary uncompressed,
  // compressed secondary, and a compressed local flash (non-volatile) cache.
  // Each tier is managed as an independent queue.
  kAdmPolicyThreeQueue,
  // Allow all blocks evicted from the primary block cache into the secondary
  // cache. This may increase CPU overhead due to more blocks being admitted
  // and compressed, but may increase the compressed secondary cache hit rate
  // for some workloads
  kAdmPolicyAllowAll,
  kAdmPolicyMax,
};

// EXPERIMENTAL
// The following feature is experimental, and the API is subject to change
//
// A 2-tier cache with a primary block cache, and a compressed secondary
// cache. The returned cache instance will internally allocate a primary
// uncompressed cache of the specified type, and a compressed secondary
// cache. Any cache memory reservations, such as WriteBufferManager
// allocations costed to the block cache, will be distributed
// proportionally across both the primary and secondary.
struct TieredCacheOptions {
  // This should point to an instance of either LRUCacheOptions or
  // HyperClockCacheOptions, depending on the cache_type. In either
  // case, the capacity and secondary_cache fields in those options
  // should not be set. If set, they will be ignored by NewTieredCache.
  ShardedCacheOptions* cache_opts = nullptr;
  PrimaryCacheType cache_type = PrimaryCacheType::kCacheTypeLRU;
  TieredAdmissionPolicy adm_policy = TieredAdmissionPolicy::kAdmPolicyAuto;
  CompressedSecondaryCacheOptions comp_cache_opts;
  // Any capacity specified in LRUCacheOptions, HyperClockCacheOptions and
  // CompressedSecondaryCacheOptions is ignored
  // The total_capacity specified here is taken as the memory budget and
  // divided between the primary block cache and compressed secondary cache
  size_t total_capacity = 0;
  double compressed_secondary_ratio = 0.0;
  // An optional secondary cache that will serve as the persistent cache
  // tier. If present, compressed blocks will be written to this
  // secondary cache.
  std::shared_ptr<SecondaryCache> nvm_sec_cache;
};

std::shared_ptr<Cache> NewTieredCache(const TieredCacheOptions& cache_opts);

// EXPERIMENTAL
// Dynamically update some of the parameters of a TieredCache. The input
// cache shared_ptr should have been allocated using NewTieredVolatileCache.
// At the moment, there are a couple of limitations -
// 1. The total_capacity should be > the WriteBufferManager max size, if
//    using the block cache charging feature
// 2. Once the compressed secondary cache is disabled by setting the
//    compressed_secondary_ratio to 0.0, it cannot be dynamically re-enabled
//    again
Status UpdateTieredCache(
    const std::shared_ptr<Cache>& cache, int64_t total_capacity = -1,
    double compressed_secondary_ratio = std::numeric_limits<double>::max(),
    TieredAdmissionPolicy adm_policy = TieredAdmissionPolicy::kAdmPolicyMax);
}  // namespace ROCKSDB_NAMESPACE
