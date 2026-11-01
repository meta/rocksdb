//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include <cinttypes>
#include <cstddef>
#include <cstdio>
#include <limits>
#include <memory>
#include <set>
#include <sstream>

#include "cache/cache_key.h"
#include "cache/sharded_cache.h"
#include "db/db_impl/db_impl.h"
#include "monitoring/histogram.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table_properties.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/cachable_entry.h"
#include "util/coding.h"
#include "util/distributed_mutex.h"
#include "util/gflags_compat.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/stderr_logger.h"
#include "util/stop_watch.h"
#include "util/string_util.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

static constexpr uint32_t KiB = uint32_t{1} << 10;
static constexpr uint32_t MiB = KiB << 10;
static constexpr uint64_t GiB = MiB << 10;

DEFINE_uint32(threads, 16, "Number of concurrent threads to run.");
DEFINE_uint64(cache_size, 1 * GiB,
              "Number of bytes to use as a cache of uncompressed data.");
DEFINE_int32(num_shard_bits, -1,
             "ShardedCacheOptions::shard_bits. Default = auto");
DEFINE_int32(
    eviction_effort_cap,
    ROCKSDB_NAMESPACE::HyperClockCacheOptions(1, 1).eviction_effort_cap,
    "HyperClockCacheOptions::eviction_effort_cap");

DEFINE_double(resident_ratio, 0.25,
              "Ratio of keys fitting in cache to keyspace.");
DEFINE_uint64(ops_per_thread, 2000000U, "Number of operations per thread.");
DEFINE_uint32(value_bytes, 8 * KiB, "Size of each value added.");
DEFINE_uint32(value_bytes_estimate, 0,
              "If > 0, overrides estimated_entry_charge or "
              "min_avg_entry_charge depending on cache_type.");
DEFINE_double(compressible_to_ratio, 0.5,
              "Approximate size ratio that values can be compressed to.");

DEFINE_int32(
    degenerate_hash_bits, 0,
    "With HCC, fix this many hash bits to increase table hash collisions");
DEFINE_uint32(skew, 5, "Degree of skew in key selection. 0 = no skew");
DEFINE_bool(populate_cache, true, "Populate cache before operations");

DEFINE_double(pinned_ratio, 0.25,
              "Keep roughly this portion of entries pinned in cache.");
DEFINE_double(
    vary_capacity_ratio, 0.0,
    "If greater than 0.0, will periodically vary the capacity between this "
    "ratio less than full size and full size. If vary_capacity_ratio + "
    "pinned_ratio is close to or exceeds 1.0, the cache might thrash.");

DEFINE_uint32(lookup_insert_percent, 82,
              "Ratio of lookup (+ insert on not found) to total workload "
              "(expressed as a percentage)");
DEFINE_uint32(insert_percent, 2,
              "Ratio of insert to total workload (expressed as a percentage)");
DEFINE_uint32(blind_insert_percent, 5,
              "Ratio of insert without keeping handle to total workload "
              "(expressed as a percentage)");
DEFINE_uint32(lookup_percent, 10,
              "Ratio of lookup to total workload (expressed as a percentage)");
DEFINE_uint32(erase_percent, 1,
              "Ratio of erase to total workload (expressed as a percentage)");
DEFINE_bool(gather_stats, false,
            "Whether to periodically simulate gathering block cache stats, "
            "using one more thread.");
DEFINE_uint32(
    gather_stats_sleep_ms, 1000,
    "How many milliseconds to sleep between each gathering of stats.");

DEFINE_uint32(gather_stats_entries_per_lock, 256,
              "For Cache::ApplyToAllEntries");

DEFINE_uint32(usleep, 0, "Sleep up to this many microseconds after each op.");

DEFINE_bool(lean, false,
            "If true, no additional computation is performed besides cache "
            "operations.");

DEFINE_bool(early_exit, false,
            "Exit before deallocating most memory. Good for malloc stats, e.g."
            "MALLOC_CONF=\"stats_print:true\"");

DEFINE_bool(histograms, true,
            "Whether to track and print histogram statistics.");

DEFINE_bool(report_problems, true, "Whether to ReportProblems() at the end.");

DEFINE_uint32(seed, 0, "Hashing/random seed to use. 0 = choose at random");

DEFINE_string(secondary_cache_uri, "",
              "Full URI for creating a custom secondary cache object");

DEFINE_string(cache_type, "hyper_clock_cache", "Type of block cache.");

DEFINE_bool(use_jemalloc_no_dump_allocator, false,
            "Whether to use JemallocNoDumpAllocator");

DEFINE_uint32(jemalloc_no_dump_allocator_num_arenas,
              ROCKSDB_NAMESPACE::JemallocAllocatorOptions().num_arenas,
              "JemallocNodumpAllocator::num_arenas");

DEFINE_bool(jemalloc_no_dump_allocator_limit_tcache_size,
            ROCKSDB_NAMESPACE::JemallocAllocatorOptions().limit_tcache_size,
            "JemallocNodumpAllocator::limit_tcache_size");

// ## BEGIN stress_cache_key sub-tool options ##
// See class StressCacheKey below.
DEFINE_bool(stress_cache_key, false,
            "If true, run cache key stress test instead");
DEFINE_uint32(
    sck_files_per_day, 2500000,
    "(-stress_cache_key) Simulated files generated per simulated day");
// NOTE: Giving each run a specified lifetime, rather than e.g. "until
// first collision" ensures equal skew from start-up, when collisions are
// less likely.
DEFINE_uint32(sck_days_per_run, 90,
              "(-stress_cache_key) Number of days to simulate in each run");
// NOTE: The number of observed collisions directly affects the relative
// accuracy of the predicted probabilities. 15 observations should be well
// within factor-of-2 accuracy.
DEFINE_uint32(
    sck_min_collision, 15,
    "(-stress_cache_key) Keep running until this many collisions seen");
// sck_file_size_mb can be thought of as average file size. The simulation is
// not precise enough to care about the distribution of file sizes; other
// simulations (https://github.com/pdillinger/unique_id/tree/main/monte_carlo)
// indicate the distribution only makes a small difference (e.g. < 2x factor)
DEFINE_uint32(
    sck_file_size_mb, 32,
    "(-stress_cache_key) Simulated file size in MiB, for accounting purposes");
DEFINE_uint32(sck_reopen_nfiles, 100,
              "(-stress_cache_key) Simulate DB re-open average every n files");
DEFINE_uint32(sck_newdb_nreopen, 1000,
              "(-stress_cache_key) Simulate new DB average every n re-opens");
DEFINE_uint32(sck_restarts_per_day, 24,
              "(-stress_cache_key) Average simulated process restarts per day "
              "(across DBs)");
DEFINE_uint32(
    sck_db_count, 100,
    "(-stress_cache_key) Parallel DBs in simulation sharing a block cache");
DEFINE_uint32(
    sck_table_bits, 20,
    "(-stress_cache_key) Log2 number of tracked (live) files (across DBs)");
// sck_keep_bits being well below full 128 bits amplifies the collision
// probability so that the true probability can be estimated through observed
// collisions. (More explanation below.)
DEFINE_uint32(
    sck_keep_bits, 50,
    "(-stress_cache_key) Number of bits to keep from each cache key (<= 64)");
// sck_randomize is used to validate whether cache key is performing "better
// than random." Even with this setting, file offsets are not randomized.
DEFINE_bool(sck_randomize, false,
            "(-stress_cache_key) Randomize (hash) cache key");
// See https://github.com/meta/rocksdb/pull/9058
DEFINE_bool(sck_footer_unique_id, false,
            "(-stress_cache_key) Simulate using proposed footer unique id");
// ## END stress_cache_key sub-tool options ##
// ## BEGIN stress_cache_instances sub-tool options ##
DEFINE_uint32(stress_cache_instances, 0,
              "If > 0, run cache instance stress test instead");
// Uses cache_size and cache_type, maybe more
// ## END stress_cache_instance sub-tool options ##

namespace ROCKSDB_NAMESPACE {

class CacheBench;
namespace {
// State shared by all concurrent executions of the same benchmark.
class SharedState {
 public:
  explicit SharedState(CacheBench* cache_bench)
      : cv_(&mu_), cache_bench_(cache_bench) {}

  ~SharedState() = default;

  port::Mutex* GetMutex() { return &mu_; }

  port::CondVar* GetCondVar() { return &cv_; }

  CacheBench* GetCacheBench() const { return cache_bench_; }

  void IncInitialized() { num_initialized_++; }

  void IncDone() { num_done_++; }

  bool AllInitialized() const { return num_initialized_ >= FLAGS_threads; }

  bool AllDone() const { return num_done_ >= FLAGS_threads; }

  void SetStart() { start_ = true; }

  bool Started() const { return start_; }

  void AddLookupStats(uint64_t hits, uint64_t misses, size_t pinned_count) {
    MutexLock l(&mu_);
    lookup_count_ += hits + misses;
    lookup_hits_ += hits;
    pinned_count_ += pinned_count;
  }

  double GetLookupHitRatio() const {
    return 1.0 * lookup_hits_ / lookup_count_;
  }

  size_t GetPinnedCount() const { return pinned_count_; }

 private:
  port::Mutex mu_;
  port::CondVar cv_;

  CacheBench* cache_bench_;

  uint64_t num_initialized_ = 0;
  bool start_ = false;
  uint64_t num_done_ = 0;
  uint64_t lookup_count_ = 0;
  uint64_t lookup_hits_ = 0;
  size_t pinned_count_ = 0;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  uint32_t tid;
  Random64 rnd;
  SharedState* shared;
  HistogramImpl latency_ns_hist;
  uint64_t duration_us = 0;

  ThreadState(uint32_t index, SharedState* _shared)
      : tid(index), rnd(FLAGS_seed + 1 + index), shared(_shared) {}
};

struct KeyGen {
  char key_data[27];

  Slice GetRand(Random64& rnd, uint64_t max_key, uint32_t skew) {
    uint64_t raw = rnd.Next();
    // Skew according to setting
    for (uint32_t i = 0; i < skew; ++i) {
      raw = std::min(raw, rnd.Next());
    }
    uint64_t key = FastRange64(raw, max_key);
    if (FLAGS_degenerate_hash_bits) {
      uint64_t key_hash =
          Hash64(reinterpret_cast<const char*>(&key), sizeof(key));
      // HCC uses the high 64 bits and a lower bit mask for starting probe
      // location, so we fix hash bits starting at the bottom of that word.
      auto hi_hash = uint64_t{0x9e3779b97f4a7c13U} ^
                     (key_hash << 1 << (FLAGS_degenerate_hash_bits - 1));
      uint64_t un_hi;
      uint64_t un_lo;
      BijectiveUnhash2x64(hi_hash, key_hash, &un_hi, &un_lo);
      un_lo ^= BitwiseAnd(FLAGS_seed, INT32_MAX);
      EncodeFixed64(key_data, un_lo);
      EncodeFixed64(key_data + 8, un_hi);
      return Slice(key_data, kCacheKeySize);
    }
    // Variable size and alignment
    size_t off = key % 8;
    key_data[0] = char{42};
    EncodeFixed64(key_data + 1, key);
    key_data[9] = char{11};
    EncodeFixed64(key_data + 10, key);
    key_data[18] = char{4};
    EncodeFixed64(key_data + 19, key);
    assert(27 >= kCacheKeySize);
    return Slice(&key_data[off], kCacheKeySize);
  }
};

Cache::ObjectPtr createValue(Random64& rnd, MemoryAllocator* alloc) {
  char* rv = AllocateBlock(FLAGS_value_bytes, alloc).release();
  // Fill with some filler data, and take some CPU time, but add redundancy
  // as requested for compressibility.
  uint32_t random_fill_size = std::max(
      uint32_t{1}, std::min(FLAGS_value_bytes,
                            static_cast<uint32_t>(FLAGS_compressible_to_ratio *
                                                  FLAGS_value_bytes)));
  uint32_t i = 0;
  for (; i < random_fill_size; i += 8) {
    EncodeFixed64(rv + i, rnd.Next());
  }
  for (; i < FLAGS_value_bytes; i++) {
    rv[i] = rv[i % random_fill_size];
  }
  return rv;
}

// Callbacks for secondary cache
size_t SizeFn(Cache::ObjectPtr /*obj*/) { return FLAGS_value_bytes; }

Status SaveToFn(Cache::ObjectPtr from_obj, size_t /*from_offset*/,
                size_t length, char* out) {
  memcpy(out, from_obj, length);
  return Status::OK();
}

Status CreateFn(const Slice& data, CompressionType /*type*/,
                CacheTier /*source*/, Cache::CreateContext* /*context*/,
                MemoryAllocator* alloc, Cache::ObjectPtr* out_obj,
                size_t* out_charge) {
  *out_obj = AllocateBlock(data.size(), alloc).release();
  memcpy(*out_obj, data.data(), data.size());
  *out_charge = data.size();
  return Status::OK();
};

void DeleteFn(Cache::ObjectPtr value, MemoryAllocator* alloc) {
  CacheAllocationDeleter{alloc}(static_cast<char*>(value));
}

Cache::CacheItemHelper helper1_wos(CacheEntryRole::kDataBlock, DeleteFn);
Cache::CacheItemHelper helper1(CacheEntryRole::kDataBlock, DeleteFn, SizeFn,
                               SaveToFn, CreateFn, &helper1_wos);
Cache::CacheItemHelper helper2_wos(CacheEntryRole::kIndexBlock, DeleteFn);
Cache::CacheItemHelper helper2(CacheEntryRole::kIndexBlock, DeleteFn, SizeFn,
                               SaveToFn, CreateFn, &helper2_wos);
Cache::CacheItemHelper helper3_wos(CacheEntryRole::kFilterBlock, DeleteFn);
Cache::CacheItemHelper helper3(CacheEntryRole::kFilterBlock, DeleteFn, SizeFn,
                               SaveToFn, CreateFn, &helper3_wos);

void ConfigureSecondaryCache(ShardedCacheOptions& opts) {
  if (!FLAGS_secondary_cache_uri.empty()) {
    std::shared_ptr<SecondaryCache> secondary_cache;
    Status s = SecondaryCache::CreateFromString(
        ConfigOptions(), FLAGS_secondary_cache_uri, &secondary_cache);
    if (secondary_cache == nullptr) {
      fprintf(stderr,
              "No secondary cache registered matching string: %s status=%s\n",
              FLAGS_secondary_cache_uri.c_str(), s.ToString().c_str());
      exit(1);
    }
    opts.secondary_cache = secondary_cache;
  }
}

ShardedCacheBase* AsShardedCache(Cache* c) {
  if (!FLAGS_secondary_cache_uri.empty()) {
    c = static_cast_with_check<CacheWrapper>(c)->GetTarget().get();
  }
  return static_cast_with_check<ShardedCacheBase>(c);
}
}  // namespace

class CacheBench {
  static constexpr uint64_t kHundredthUint64 =
      std::numeric_limits<uint64_t>::max() / 100U;

 public:
  CacheBench()
      : max_key_(static_cast<uint64_t>(FLAGS_cache_size / FLAGS_resident_ratio /
                                       FLAGS_value_bytes)),
        lookup_insert_threshold_(kHundredthUint64 *
                                 FLAGS_lookup_insert_percent),
        insert_threshold_(lookup_insert_threshold_ +
                          kHundredthUint64 * FLAGS_insert_percent),
        blind_insert_threshold_(insert_threshold_ +
                                kHundredthUint64 * FLAGS_blind_insert_percent),
        lookup_threshold_(blind_insert_threshold_ +
                          kHundredthUint64 * FLAGS_lookup_percent),
        erase_threshold_(lookup_threshold_ +
                         kHundredthUint64 * FLAGS_erase_percent) {
    if (erase_threshold_ != 100U * kHundredthUint64) {
      fprintf(stderr, "Percentages must add to 100.\n");
      exit(1);
    }
    cache_ = MakeCache();
  }

  ~CacheBench() = default;

  static std::shared_ptr<Cache> MakeCache() {
    std::shared_ptr<MemoryAllocator> allocator;
    if (FLAGS_use_jemalloc_no_dump_allocator) {
      JemallocAllocatorOptions opts;
      opts.num_arenas = FLAGS_jemalloc_no_dump_allocator_num_arenas;
      opts.limit_tcache_size =
          FLAGS_jemalloc_no_dump_allocator_limit_tcache_size;
      Status s = NewJemallocNodumpAllocator(opts, &allocator);
      assert(s.ok());
    }
    if (FLAGS_cache_type == "clock_cache") {
      fprintf(stderr, "Old clock cache implementation has been removed.\n");
      exit(1);
    } else if (EndsWith(FLAGS_cache_type, "hyper_clock_cache")) {
      HyperClockCacheOptions opts(
          FLAGS_cache_size, /*estimated_entry_charge=*/0, FLAGS_num_shard_bits);
      opts.hash_seed = BitwiseAnd(FLAGS_seed, INT32_MAX);
      opts.memory_allocator = allocator;
      opts.eviction_effort_cap = FLAGS_eviction_effort_cap;
      if (FLAGS_cache_type == "fixed_hyper_clock_cache") {
        opts.estimated_entry_charge = FLAGS_value_bytes_estimate > 0
                                          ? FLAGS_value_bytes_estimate
                                          : FLAGS_value_bytes;
      } else if (FLAGS_cache_type == "auto_hyper_clock_cache" ||
                 FLAGS_cache_type == "hyper_clock_cache") {
        if (FLAGS_value_bytes_estimate > 0) {
          opts.min_avg_entry_charge = FLAGS_value_bytes_estimate;
        }
      } else {
        fprintf(stderr, "Cache type not supported.\n");
        exit(1);
      }
      ConfigureSecondaryCache(opts);
      return opts.MakeSharedCache();
    } else if (FLAGS_cache_type == "lru_cache") {
      LRUCacheOptions opts(FLAGS_cache_size, FLAGS_num_shard_bits,
                           false /* strict_capacity_limit */,
                           0.5 /* high_pri_pool_ratio */);
      opts.hash_seed = BitwiseAnd(FLAGS_seed, INT32_MAX);
      opts.memory_allocator = allocator;
      ConfigureSecondaryCache(opts);
      return NewLRUCache(opts);
    } else {
      fprintf(stderr, "Cache type not supported.\n");
      exit(1);
    }
  }

  void PopulateCache() {
    Random64 rnd(FLAGS_seed);
    KeyGen keygen;
    size_t max_occ = 0;
    size_t inserts_since_max_occ_increase = 0;
    size_t keys_since_last_not_found = 0;

    // Avoid redundant insertions by checking Lookup before Insert.
    // Loop until insertions consistently fail to increase max occupancy or
    // it becomes difficult to find keys not already inserted.
    while (inserts_since_max_occ_increase < 100 &&
           keys_since_last_not_found < 100) {
      Slice key = keygen.GetRand(rnd, max_key_, FLAGS_skew);

      Cache::Handle* handle = cache_->Lookup(key);
      if (handle != nullptr) {
        cache_->Release(handle);
        ++keys_since_last_not_found;
        continue;
      }
      keys_since_last_not_found = 0;

      Status s =
          cache_->Insert(key, createValue(rnd, cache_->memory_allocator()),
                         &helper1, FLAGS_value_bytes);
      assert(s.ok());

      handle = cache_->Lookup(key);
      if (!handle) {
        fprintf(stderr, "Failed to lookup key just inserted.\n");
        assert(false);
        exit(42);
      } else {
        cache_->Release(handle);
      }

      size_t occ = cache_->GetOccupancyCount();
      if (occ > max_occ) {
        max_occ = occ;
        inserts_since_max_occ_increase = 0;
      } else {
        ++inserts_since_max_occ_increase;
      }
    }
    printf("Population complete (%zu entries, %g average charge)\n", max_occ,
           1.0 * FLAGS_cache_size / max_occ);
  }

  bool Run() {
    const auto clock = SystemClock::Default().get();

    PrintEnv();
    SharedState shared(this);
    std::vector<std::unique_ptr<ThreadState>> threads(FLAGS_threads);
    for (uint32_t i = 0; i < FLAGS_threads; i++) {
      threads[i].reset(new ThreadState(i, &shared));
      std::thread(ThreadBody, threads[i].get()).detach();
    }

    HistogramImpl stats_hist;
    std::string stats_report;
    std::thread stats_thread(StatsBody, &shared, &stats_hist, &stats_report);

    uint64_t start_time;
    {
      MutexLock l(shared.GetMutex());
      while (!shared.AllInitialized()) {
        shared.GetCondVar()->Wait();
      }
      // Record start time
      start_time = clock->NowMicros();

      // Start all threads
      shared.SetStart();
      shared.GetCondVar()->SignalAll();

      // Wait threads to complete
      while (!shared.AllDone()) {
        shared.GetCondVar()->Wait();
      }
    }

    // Stats gathering is considered background work. This time measurement
    // is for foreground work, and not really ideal for that. See below.
    uint64_t end_time = clock->NowMicros();
    stats_thread.join();

    // Wall clock time - includes idle time if threads
    // finish at different times (not ideal).
    double elapsed_secs = static_cast<double>(end_time - start_time) * 1e-6;
    uint32_t ops_per_sec = static_cast<uint32_t>(
        1.0 * FLAGS_threads * FLAGS_ops_per_thread / elapsed_secs);
    printf("Complete in %.3f s; Rough parallel ops/sec = %u\n", elapsed_secs,
           ops_per_sec);

    // Total time in each thread (more accurate throughput measure)
    elapsed_secs = 0;
    for (uint32_t i = 0; i < FLAGS_threads; i++) {
      elapsed_secs += threads[i]->duration_us * 1e-6;
    }
    ops_per_sec = static_cast<uint32_t>(1.0 * FLAGS_threads *
                                        FLAGS_ops_per_thread / elapsed_secs);
    printf("Thread ops/sec = %u\n", ops_per_sec);

    printf("Lookup hit ratio: %g\n", shared.GetLookupHitRatio());

    size_t occ = cache_->GetOccupancyCount();
    size_t slot = cache_->GetTableAddressCount();
    printf("Final load factor: %g (%zu / %zu)\n", 1.0 * occ / slot, occ, slot);

    printf("Final pinned count: %zu\n", shared.GetPinnedCount());

    if (FLAGS_histograms) {
      printf("\nOperation latency (ns):\n");
      HistogramImpl combined;
      for (uint32_t i = 0; i < FLAGS_threads; i++) {
        combined.Merge(threads[i]->latency_ns_hist);
      }
      printf("%s", combined.ToString().c_str());

      if (FLAGS_gather_stats) {
        printf("\nGather stats latency (us):\n");
        printf("%s", stats_hist.ToString().c_str());
      }
    }

    if (FLAGS_report_problems) {
      printf("\n");
      std::shared_ptr<Logger> logger =
          std::make_shared<StderrLogger>(InfoLogLevel::DEBUG_LEVEL);
      cache_->ReportProblems(logger);
    }
    printf("%s", stats_report.c_str());

    return true;
  }

 private:
  std::shared_ptr<Cache> cache_;
  const uint64_t max_key_;
  // Cumulative thresholds in the space of a random uint64_t
  const uint64_t lookup_insert_threshold_;
  const uint64_t insert_threshold_;
  const uint64_t blind_insert_threshold_;
  const uint64_t lookup_threshold_;
  const uint64_t erase_threshold_;

  // A benchmark version of gathering stats on an active block cache by
  // iterating over it. The primary purpose is to measure the impact of
  // gathering stats with ApplyToAllEntries on throughput- and
  // latency-sensitive Cache users. Performance of stats gathering is
  // also reported. The last set of gathered stats is also reported, for
  // manual sanity checking for logical errors or other unexpected
  // behavior of cache_bench or the underlying Cache.
  static void StatsBody(SharedState* shared, HistogramImpl* stats_hist,
                        std::string* stats_report) {
    if (!FLAGS_gather_stats) {
      return;
    }
    const auto clock = SystemClock::Default().get();
    uint64_t total_key_size = 0;
    uint64_t total_charge = 0;
    uint64_t total_entry_count = 0;
    uint64_t table_occupancy = 0;
    uint64_t table_size = 0;
    std::set<const Cache::CacheItemHelper*> helpers;
    StopWatchNano timer(clock);

    for (;;) {
      uint64_t time;
      time = clock->NowMicros();
      uint64_t deadline = time + uint64_t{FLAGS_gather_stats_sleep_ms} * 1000;

      {
        MutexLock l(shared->GetMutex());
        for (;;) {
          if (shared->AllDone()) {
            std::ostringstream ostr;
            ostr << "\nMost recent cache entry stats:\n"
                 << "Number of entries: " << total_entry_count << "\n"
                 << "Table occupancy: " << table_occupancy << " / "
                 << table_size << " = "
                 << (100.0 * table_occupancy / table_size) << "%\n"
                 << "Total charge: " << BytesToHumanString(total_charge) << "\n"
                 << "Average key size: "
                 << (1.0 * total_key_size / total_entry_count) << "\n"
                 << "Average charge: "
                 << BytesToHumanString(static_cast<uint64_t>(
                        1.0 * total_charge / total_entry_count))
                 << "\n"
                 << "Unique helpers: " << helpers.size() << "\n";
            *stats_report = ostr.str();
            return;
          }
          if (clock->NowMicros() >= deadline) {
            break;
          }
          uint64_t diff = deadline - std::min(clock->NowMicros(), deadline);
          shared->GetCondVar()->TimedWait(diff + 1);
        }
      }

      // Now gather stats, outside of mutex
      total_key_size = 0;
      total_charge = 0;
      total_entry_count = 0;
      helpers.clear();
      auto fn = [&](const Slice& key, Cache::ObjectPtr /*value*/, size_t charge,
                    const Cache::CacheItemHelper* helper) {
        total_key_size += key.size();
        total_charge += charge;
        ++total_entry_count;
        // Something slightly more expensive as in stats by category
        helpers.insert(helper);
      };
      if (FLAGS_histograms) {
        timer.Start();
      }
      Cache::ApplyToAllEntriesOptions opts;
      opts.average_entries_per_lock = FLAGS_gather_stats_entries_per_lock;
      shared->GetCacheBench()->cache_->ApplyToAllEntries(fn, opts);
      table_occupancy = shared->GetCacheBench()->cache_->GetOccupancyCount();
      table_size = shared->GetCacheBench()->cache_->GetTableAddressCount();
      if (FLAGS_histograms) {
        stats_hist->Add(timer.ElapsedNanos() / 1000);
      }
    }
  }

  static void ThreadBody(ThreadState* thread) {
    SharedState* shared = thread->shared;

    {
      MutexLock l(shared->GetMutex());
      shared->IncInitialized();
      if (shared->AllInitialized()) {
        shared->GetCondVar()->SignalAll();
      }
      while (!shared->Started()) {
        shared->GetCondVar()->Wait();
      }
    }
    thread->shared->GetCacheBench()->OperateCache(thread);

    {
      MutexLock l(shared->GetMutex());
      shared->IncDone();
      if (shared->AllDone()) {
        shared->GetCondVar()->SignalAll();
      }
    }
  }

  void OperateCache(ThreadState* thread) {
    // To use looked-up values
    uint64_t result = 0;
    uint64_t lookup_misses = 0;
    uint64_t lookup_hits = 0;
    // To hold handles for a non-trivial amount of time
    std::deque<Cache::Handle*> pinned;
    size_t total_pin_count = static_cast<size_t>(
        (FLAGS_cache_size * FLAGS_pinned_ratio) / FLAGS_value_bytes + 0.999999);
    // For this thread. Some round up, some round down, as appropriate
    size_t pin_count = (total_pin_count + thread->tid) / FLAGS_threads;

    KeyGen gen;
    const auto clock = SystemClock::Default().get();
    uint64_t start_time = clock->NowMicros();
    StopWatchNano timer(clock);
    auto system_clock = SystemClock::Default();
    size_t steps_to_next_capacity_change = 0;

    for (uint64_t i = 0; i < FLAGS_ops_per_thread; i++) {
      Slice key = gen.GetRand(thread->rnd, max_key_, FLAGS_skew);
      uint64_t random_op = thread->rnd.Next();

      if (FLAGS_vary_capacity_ratio > 0.0 && thread->tid == 0) {
        if (steps_to_next_capacity_change == 0) {
          double cut_ratio = static_cast<double>(thread->rnd.Next()) /
                             static_cast<double>(UINT64_MAX) *
                             FLAGS_vary_capacity_ratio;
          cache_->SetCapacity(FLAGS_cache_size * (1.0 - cut_ratio));
          steps_to_next_capacity_change =
              static_cast<size_t>(FLAGS_ops_per_thread / 100);
        } else {
          --steps_to_next_capacity_change;
        }
      }

      if (FLAGS_histograms) {
        timer.Start();
      }

      if (random_op < lookup_insert_threshold_) {
        // do lookup
        auto handle = cache_->Lookup(key, &helper2, /*context*/ nullptr,
                                     Cache::Priority::LOW);
        if (handle) {
          ++lookup_hits;
          if (!FLAGS_lean) {
            // do something with the data
            result += NPHash64(static_cast<char*>(cache_->Value(handle)),
                               FLAGS_value_bytes);
          }
          pinned.push_back(handle);
        } else {
          ++lookup_misses;
          // do insert
          Status s = cache_->Insert(
              key, createValue(thread->rnd, cache_->memory_allocator()),
              &helper2, FLAGS_value_bytes, &pinned.emplace_back());
          assert(s.ok());
        }
      } else if (random_op < insert_threshold_) {
        // do insert
        Status s = cache_->Insert(
            key, createValue(thread->rnd, cache_->memory_allocator()), &helper3,
            FLAGS_value_bytes, &pinned.emplace_back());
        assert(s.ok());
      } else if (random_op < blind_insert_threshold_) {
        // insert without keeping a handle
        Status s = cache_->Insert(
            key, createValue(thread->rnd, cache_->memory_allocator()), &helper3,
            FLAGS_value_bytes);
        assert(s.ok());
      } else if (random_op < lookup_threshold_) {
        // do lookup
        auto handle = cache_->Lookup(key, &helper2, /*context*/ nullptr,
                                     Cache::Priority::LOW);
        if (handle) {
          ++lookup_hits;
          if (!FLAGS_lean) {
            // do something with the data
            result += NPHash64(static_cast<char*>(cache_->Value(handle)),
                               FLAGS_value_bytes);
          }
          pinned.push_back(handle);
        } else {
          ++lookup_misses;
        }
      } else if (random_op < erase_threshold_) {
        // do erase
        cache_->Erase(key);
      } else {
        // Should be extremely unlikely (noop)
        assert(random_op >= kHundredthUint64 * 100U);
      }
      if (FLAGS_histograms) {
        thread->latency_ns_hist.Add(timer.ElapsedNanos());
      }
      if (FLAGS_usleep > 0) {
        unsigned us =
            static_cast<unsigned>(thread->rnd.Uniform(FLAGS_usleep + 1));
        if (us > 0) {
          system_clock->SleepForMicroseconds(us);
        }
      }
      while (pinned.size() > pin_count) {
        cache_->Release(pinned.front());
        pinned.pop_front();
      }
    }
    if (FLAGS_early_exit) {
      MutexLock l(thread->shared->GetMutex());
      exit(0);
    }
    thread->shared->AddLookupStats(lookup_hits, lookup_misses, pinned.size());
    for (auto handle : pinned) {
      cache_->Release(handle);
      handle = nullptr;
    }
    // Ensure computations on `result` are not optimized away.
    if (result == 1) {
      printf("You are extremely unlucky(2). Try again.\n");
      exit(1);
    }
    thread->duration_us = clock->NowMicros() - start_time;
  }

  void PrintEnv() const {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    printf(
        "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
    printf("WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
    printf("----------------------------\n");
    printf("RocksDB version     : %d.%d\n", kMajorVersion, kMinorVersion);
    printf("Cache impl name     : %s\n", cache_->Name());
    printf("DMutex impl name    : %s\n", DMutex::kName());
    printf("Number of threads   : %u\n", FLAGS_threads);
    printf("Ops per thread      : %" PRIu64 "\n", FLAGS_ops_per_thread);
    printf("Cache size          : %s\n",
           BytesToHumanString(FLAGS_cache_size).c_str());
    printf("Num shard bits      : %d\n",
           AsShardedCache(cache_.get())->GetNumShardBits());
    printf("Max key             : %" PRIu64 "\n", max_key_);
    printf("Resident ratio      : %g\n", FLAGS_resident_ratio);
    printf("Skew degree         : %u\n", FLAGS_skew);
    printf("Populate cache      : %d\n", int{FLAGS_populate_cache});
    printf("Lookup+Insert pct   : %u%%\n", FLAGS_lookup_insert_percent);
    printf("Insert percentage   : %u%%\n", FLAGS_insert_percent);
    printf("Lookup percentage   : %u%%\n", FLAGS_lookup_percent);
    printf("Erase percentage    : %u%%\n", FLAGS_erase_percent);
    std::ostringstream stats;
    if (FLAGS_gather_stats) {
      stats << "enabled (" << FLAGS_gather_stats_sleep_ms << "ms, "
            << FLAGS_gather_stats_entries_per_lock << "/lock)";
    } else {
      stats << "disabled";
    }
    printf("Gather stats        : %s\n", stats.str().c_str());
    printf("----------------------------\n");
  }
};

// cache_bench -stress_cache_key is an independent embedded tool for
// estimating the probability of CacheKey collisions through simulation.
// At a high level, it simulates generating SST files over many months,
// keeping them in the DB and/or cache for some lifetime while staying
// under resource caps, and checking for any cache key collisions that
// arise among the set of live files. For efficient simulation, we make
// some simplifying "pessimistic" assumptions (that only increase the
// chance of the simulation reporting a collision relative to the chance
// of collision in practice):
// * Every generated file has a cache entry for every byte offset in the
// file (contiguous range of cache keys)
// * All of every file is cached for its entire lifetime. (Here "lifetime"
// is technically the union of DB and Cache lifetime, though we only
// model a generous DB lifetime, where space usage is always maximized.
// In a effective Cache, lifetime in cache can only substantially exceed
// lifetime in DB if there is little cache activity; cache activity is
// required to hit cache key collisions.)
//
// It would be possible to track an exact set of cache key ranges for the
// set of live files, but we would have no hope of observing collisions
// (overlap in live files) in our simulation. We need to employ some way
// of amplifying collision probability that allows us to predict the real
// collision probability by extrapolation from observed collisions. Our
// basic approach is to reduce each cache key range down to some smaller
// number of bits, and limiting to bits that are shared over the whole
// range.  Now we can observe collisions using a set of smaller stripped-down
// (reduced) cache keys. Let's do some case analysis to understand why this
// works:
// * No collision in reduced key - because the reduction is a pure function
// this implies no collision in the full keys
// * Collision detected between two reduced keys - either
//   * The reduction has dropped some structured uniqueness info (from one of
// session counter or file number; file offsets are never materialized here).
// This can only artificially inflate the observed and extrapolated collision
// probabilities. We only have to worry about this in designing the reduction.
//   * The reduction has preserved all the structured uniqueness in the cache
// key, which means either
//     * REJECTED: We have a uniqueness bug in generating cache keys, where
// structured uniqueness info should have been different but isn't. In such a
// case, increasing by 1 the number of bits kept after reduction would not
// reduce observed probabilities by half. (In our observations, the
// probabilities are reduced approximately by half.)
//     * ACCEPTED: The lost unstructured uniqueness in the key determines the
// probability that an observed collision would imply an overlap in ranges.
// In short, dropping n bits from key would increase collision probability by
// 2**n, assuming those n bits have full entropy in unstructured uniqueness.
//
// But we also have to account for the key ranges based on file size. If file
// sizes are roughly 2**b offsets, using XOR in 128-bit cache keys for
// "ranges", we know from other simulations (see
// https://github.com/pdillinger/unique_id/) that that's roughly equivalent to
// (less than 2x higher collision probability) using a cache key of size
// 128 - b bits for the whole file. (This is the only place we make an
// "optimistic" assumption, which is more than offset by the real
// implementation stripping off 2 lower bits from block byte offsets for cache
// keys. The simulation assumes byte offsets, which is net pessimistic.)
//
// So to accept the extrapolation as valid, we need to be confident that all
// "lost" bits, excluding those covered by file offset, are full entropy.
// Recall that we have assumed (verifiably, safely) that other structured data
// (file number and session counter) are kept, not lost. Based on the
// implementation comments for OffsetableCacheKey, the only potential hole here
// is that we only have ~103 bits of entropy in "all new" session IDs, and in
// extreme cases, there might be only 1 DB ID. However, because the upper ~39
// bits of session ID are hashed, the combination of file number and file
// offset only has to add to 25 bits (or more) to ensure full entropy in
// unstructured uniqueness lost in the reduction. Typical file size of 32MB
// suffices (at least for simulation purposes where we assume each file offset
// occupies a cache key).
//
// Example results in comments on OffsetableCacheKey.
class StressCacheKey {
 public:
  void Run() {
    if (FLAGS_sck_footer_unique_id) {
      // Proposed footer unique IDs are DB-independent and session-independent
      // (but process-dependent) which is most easily simulated here by
      // assuming 1 DB and (later below) no session resets without process
      // reset.
      FLAGS_sck_db_count = 1;
    }

    // Describe the simulated workload
    uint64_t mb_per_day =
        uint64_t{FLAGS_sck_files_per_day} * FLAGS_sck_file_size_mb;
    printf("Total cache or DBs size: %gTiB  Writing %g MiB/s or %gTiB/day\n",
           FLAGS_sck_file_size_mb / 1024.0 / 1024.0 *
               std::pow(2.0, FLAGS_sck_table_bits),
           mb_per_day / 86400.0, mb_per_day / 1024.0 / 1024.0);
    // For extrapolating probability of any collisions from a number of
    // observed collisions
    multiplier_ = std::pow(2.0, 128 - FLAGS_sck_keep_bits) /
                  (FLAGS_sck_file_size_mb * 1024.0 * 1024.0);
    printf(
        "Multiply by %g to correct for simulation losses (but still assume "
        "whole file cached)\n",
        multiplier_);
    restart_nfiles_ = FLAGS_sck_files_per_day / FLAGS_sck_restarts_per_day;
    double without_ejection =
        std::pow(1.414214, FLAGS_sck_keep_bits) / FLAGS_sck_files_per_day;
    // This should be a lower bound for -sck_randomize, usually a terribly
    // rough lower bound.
    // If observation is worse than this, then something has gone wrong.
    printf(
        "Without ejection, expect random collision after %g days (%g "
        "corrected)\n",
        without_ejection, without_ejection * multiplier_);
    double with_full_table =
        std::pow(2.0, FLAGS_sck_keep_bits - FLAGS_sck_table_bits) /
        FLAGS_sck_files_per_day;
    // This is an alternate lower bound for -sck_randomize, usually pretty
    // accurate. Our cache keys should usually perform "better than random"
    // but always no worse. (If observation is substantially worse than this,
    // then something has gone wrong.)
    printf(
        "With ejection and full table, expect random collision after %g "
        "days (%g corrected)\n",
        with_full_table, with_full_table * multiplier_);
    collisions_ = 0;

    // Run until sufficient number of observed collisions.
    for (int i = 1; collisions_ < FLAGS_sck_min_collision; i++) {
      RunOnce();
      if (collisions_ == 0) {
        printf(
            "No collisions after %d x %u days                              "
            "                   \n",
            i, FLAGS_sck_days_per_run);
      } else {
        double est = 1.0 * i * FLAGS_sck_days_per_run / collisions_;
        printf("%" PRIu64
               " collisions after %d x %u days, est %g days between (%g "
               "corrected)        \n",
               collisions_, i, FLAGS_sck_days_per_run, est, est * multiplier_);
      }
    }
  }

  void RunOnce() {
    // Re-initialized simulated state
    const size_t db_count = std::max(size_t{FLAGS_sck_db_count}, size_t{1});
    dbs_.reset(new TableProperties[db_count]{});
    const size_t table_mask = (size_t{1} << FLAGS_sck_table_bits) - 1;
    table_.reset(new uint64_t[table_mask + 1]{});
    if (FLAGS_sck_keep_bits > 64) {
      FLAGS_sck_keep_bits = 64;
    }

    // Details of which bits are dropped in reduction
    uint32_t shift_away = 64 - FLAGS_sck_keep_bits;
    // Shift away fewer potential file number bits (b) than potential
    // session counter bits (a).
    uint32_t shift_away_b = shift_away / 3;
    uint32_t shift_away_a = shift_away - shift_away_b;

    process_count_ = 0;
    session_count_ = 0;
    newdb_count_ = 0;
    ResetProcess(/*newdbs*/ true);

    Random64 r{std::random_device{}()};

    uint64_t max_file_count =
        uint64_t{FLAGS_sck_files_per_day} * FLAGS_sck_days_per_run;
    uint32_t report_count = 0;
    uint32_t collisions_this_run = 0;
    size_t db_i = 0;

    for (uint64_t file_count = 1; file_count <= max_file_count;
         ++file_count, ++db_i) {
      // Round-robin through DBs (this faster than %)
      if (db_i >= db_count) {
        db_i = 0;
      }
      // Any other periodic actions before simulating next file
      if (!FLAGS_sck_footer_unique_id && r.OneIn(FLAGS_sck_reopen_nfiles)) {
        ResetSession(db_i, /*newdb*/ r.OneIn(FLAGS_sck_newdb_nreopen));
      } else if (r.OneIn(restart_nfiles_)) {
        ResetProcess(/*newdbs*/ false);
      }
      // Simulate next file
      OffsetableCacheKey ock;
      dbs_[db_i].orig_file_number += 1;
      // skip some file numbers for other file kinds, except in footer unique
      // ID, orig_file_number here tracks process-wide generated SST file
      // count.
      if (!FLAGS_sck_footer_unique_id) {
        dbs_[db_i].orig_file_number += (r.Next() & 3);
      }
      bool is_stable;
      BlockBasedTable::SetupBaseCacheKey(&dbs_[db_i], /* ignored */ "",
                                         /* ignored */ 42, &ock, &is_stable);
      assert(is_stable);
      // Get a representative cache key, which later we analytically generalize
      // to a range.
      CacheKey ck = ock.WithOffset(0);
      uint64_t reduced_key;
      if (FLAGS_sck_randomize) {
        reduced_key = GetSliceHash64(ck.AsSlice()) >> shift_away;
      } else if (FLAGS_sck_footer_unique_id) {
        // Special case: keep only file number, not session counter
        reduced_key = DecodeFixed64(ck.AsSlice().data()) >> shift_away;
      } else {
        // Try to keep file number and session counter (shift away other bits)
        uint32_t a = DecodeFixed32(ck.AsSlice().data()) << shift_away_a;
        uint32_t b = DecodeFixed32(ck.AsSlice().data() + 4) >> shift_away_b;
        reduced_key = (uint64_t{a} << 32) + b;
      }
      if (reduced_key == 0) {
        // Unlikely, but we need to exclude tracking this value because we
        // use it to mean "empty" in table. This case is OK as long as we
        // don't hit it often.
        printf("Hit Zero!                                                  \n");
        file_count--;
        continue;
      }
      uint64_t h =
          NPHash64(reinterpret_cast<char*>(&reduced_key), sizeof(reduced_key));
      // Skew expected lifetimes, for high variance (super-Poisson) variance
      // in actual lifetimes.
      size_t pos =
          std::min(Lower32of64(h) & table_mask, Upper32of64(h) & table_mask);
      if (table_[pos] == reduced_key) {
        collisions_this_run++;
        // Our goal is to predict probability of no collisions, not expected
        // number of collisions. To make the distinction, we have to get rid
        // of observing correlated collisions, which this takes care of:
        ResetProcess(/*newdbs*/ false);
      } else {
        // Replace (end of lifetime for file that was in this slot)
        table_[pos] = reduced_key;
      }

      if (++report_count == FLAGS_sck_files_per_day) {
        report_count = 0;
        // Estimate fill %
        size_t incr = table_mask / 1000;
        size_t sampled_count = 0;
        for (size_t i = 0; i <= table_mask; i += incr) {
          if (table_[i] != 0) {
            sampled_count++;
          }
        }
        // Report
        printf(
            "%" PRIu64 " days, %" PRIu64 " proc, %" PRIu64 " sess, %" PRIu64
            " newdb, %u coll, occ %g%%, ejected %g%%      \r",
            file_count / FLAGS_sck_files_per_day, process_count_,
            session_count_, newdb_count_ - FLAGS_sck_db_count,
            collisions_this_run, 100.0 * sampled_count / 1000.0,
            100.0 * (1.0 - sampled_count / 1000.0 * table_mask / file_count));
        fflush(stdout);
      }
    }
    collisions_ += collisions_this_run;
  }

  void ResetSession(size_t i, bool newdb) {
    dbs_[i].db_session_id = DBImpl::GenerateDbSessionId(nullptr);
    if (newdb) {
      ++newdb_count_;
      if (FLAGS_sck_footer_unique_id) {
        // Simulate how footer id would behave
        dbs_[i].db_id = "none";
      } else {
        // db_id might be ignored, depending on the implementation details
        dbs_[i].db_id = std::to_string(newdb_count_);
        dbs_[i].orig_file_number = 0;
      }
    }
    session_count_++;
  }

  void ResetProcess(bool newdbs) {
    process_count_++;
    DBImpl::TEST_ResetDbSessionIdGen();
    for (size_t i = 0; i < FLAGS_sck_db_count; ++i) {
      ResetSession(i, newdbs);
    }
    if (FLAGS_sck_footer_unique_id) {
      // For footer unique ID, this tracks process-wide generated SST file
      // count.
      dbs_[0].orig_file_number = 0;
    }
  }

 private:
  // Use db_session_id and orig_file_number from TableProperties
  std::unique_ptr<TableProperties[]> dbs_;
  std::unique_ptr<uint64_t[]> table_;
  uint64_t process_count_ = 0;
  uint64_t session_count_ = 0;
  uint64_t newdb_count_ = 0;
  uint64_t collisions_ = 0;
  uint32_t restart_nfiles_ = 0;
  double multiplier_ = 0.0;
};

// cache_bench -stress_cache_instances is a partially independent embedded tool
// for evaluating the time and space required to create and destroy many cache
// instances, as this is considered important for a default cache implementation
// which could see many throw-away instances in handling of Options, or created
// in large numbers for many very small DBs with many CFs. Prefix command line
// with /usr/bin/time to see max RSS memory.
class StressCacheInstances {
 public:
  void Run() {
    const int kNumIterations = 10;
    const auto clock = SystemClock::Default().get();
    caches_.reserve(FLAGS_stress_cache_instances);

    uint64_t total_create_time_us = 0;
    uint64_t total_destroy_time_us = 0;

    for (int iter = 0; iter < kNumIterations; ++iter) {
      // Create many cache instances
      uint64_t start_create = clock->NowMicros();
      for (uint32_t i = 0; i < FLAGS_stress_cache_instances; ++i) {
        caches_.emplace_back(CacheBench::MakeCache());
      }
      uint64_t end_create = clock->NowMicros();
      uint64_t create_time = end_create - start_create;
      total_create_time_us += create_time;

      // Destroy them
      uint64_t start_destroy = clock->NowMicros();
      caches_.clear();
      uint64_t end_destroy = clock->NowMicros();
      uint64_t destroy_time = end_destroy - start_destroy;
      total_destroy_time_us += destroy_time;

      printf(
          "Iteration %d: Created %u caches in %.3f ms, destroyed in %.3f ms\n",
          iter + 1, FLAGS_stress_cache_instances, create_time / 1000.0,
          destroy_time / 1000.0);
    }

    printf("Average creation time: %.3f ms (%.1f us per cache)\n",
           static_cast<double>(total_create_time_us) / kNumIterations / 1000.0,
           static_cast<double>(total_create_time_us) / kNumIterations /
               FLAGS_stress_cache_instances);
    printf("Average destruction time: %.3f ms (%.1f us per cache)\n",
           static_cast<double>(total_destroy_time_us) / kNumIterations / 1000.0,
           static_cast<double>(total_destroy_time_us) / kNumIterations /
               FLAGS_stress_cache_instances);
  }

 private:
  std::vector<std::shared_ptr<Cache>> caches_;
};

int cache_bench_tool(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_stress_cache_key) {
    // Alternate tool
    StressCacheKey().Run();
    return 0;
  }

  if (FLAGS_stress_cache_instances > 0) {
    StressCacheInstances().Run();
    return 0;
  }

  if (FLAGS_threads <= 0) {
    fprintf(stderr, "threads number <= 0\n");
    exit(1);
  }

  if (FLAGS_seed == 0) {
    FLAGS_seed = static_cast<uint32_t>(port::GetProcessID());
    printf("Using seed = %" PRIu32 "\n", FLAGS_seed);
  }

  ROCKSDB_NAMESPACE::CacheBench bench;
  if (FLAGS_populate_cache) {
    bench.PopulateCache();
  }
  if (bench.Run()) {
    return 0;
  } else {
    return 1;
  }
}  // namespace ROCKSDB_NAMESPACE
}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
