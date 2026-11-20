//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/sim_cache.h"

#include <atomic>
#include <iomanip>

#include "file/writable_file_writer.h"
#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class CacheActivityLogger {
 public:
  CacheActivityLogger()
      : activity_logging_enabled_(false), max_logging_size_(0) {}

  // No copying allowed
  CacheActivityLogger(const CacheActivityLogger&) = delete;
  CacheActivityLogger& operator=(const CacheActivityLogger&) = delete;
  CacheActivityLogger(CacheActivityLogger&&) = delete;
  CacheActivityLogger& operator=(CacheActivityLogger&&) = delete;

  ~CacheActivityLogger() {
    MutexLock l(&mutex_);

    StopLoggingInternal();
    bg_status_.PermitUncheckedError();
  }

  Status StartLogging(const std::string& activity_log_file, Env* env,
                      uint64_t max_logging_size = 0) {
    assert(activity_log_file != "");
    assert(env != nullptr);

    Status status;
    FileOptions file_opts;

    MutexLock l(&mutex_);

    // Stop existing logging if any
    StopLoggingInternal();

    // Open log file
    status = WritableFileWriter::Create(env->GetFileSystem(), activity_log_file,
                                        file_opts, &file_writer_, nullptr);
    if (!status.ok()) {
      return status;
    }

    max_logging_size_ = max_logging_size;
    activity_logging_enabled_.store(true);

    return status;
  }

  void StopLogging() {
    MutexLock l(&mutex_);

    StopLoggingInternal();
  }

  void ReportLookup(const Slice& key) {
    if (activity_logging_enabled_.load() == false) {
      return;
    }

    std::ostringstream oss;
    // line format: "LOOKUP - <KEY>"
    oss << "LOOKUP - " << key.ToString(true) << std::endl;

    MutexLock l(&mutex_);
    Status s = file_writer_->Append(oss.str(), IOOptions());
    if (!s.ok() && bg_status_.ok()) {
      bg_status_ = s;
    }
    if (MaxLoggingSizeReached() || !bg_status_.ok()) {
      // Stop logging if we have reached the max file size or
      // encountered an error
      StopLoggingInternal();
    }
  }

  void ReportAdd(const Slice& key, size_t size) {
    if (activity_logging_enabled_.load() == false) {
      return;
    }

    std::ostringstream oss;
    // line format: "ADD - <KEY> - <KEY-SIZE>"
    oss << "ADD - " << key.ToString(true) << " - " << size << std::endl;
    MutexLock l(&mutex_);
    Status s = file_writer_->Append(oss.str(), IOOptions());
    if (!s.ok() && bg_status_.ok()) {
      bg_status_ = s;
    }

    if (MaxLoggingSizeReached() || !bg_status_.ok()) {
      // Stop logging if we have reached the max file size or
      // encountered an error
      StopLoggingInternal();
    }
  }

  Status& bg_status() {
    MutexLock l(&mutex_);
    return bg_status_;
  }

 private:
  bool MaxLoggingSizeReached() {
    mutex_.AssertHeld();

    return (max_logging_size_ > 0 &&
            file_writer_->GetFileSize() >= max_logging_size_);
  }

  void StopLoggingInternal() {
    mutex_.AssertHeld();

    if (!activity_logging_enabled_) {
      return;
    }

    activity_logging_enabled_.store(false);
    Status s = file_writer_->Close(IOOptions());
    if (!s.ok() && bg_status_.ok()) {
      bg_status_ = s;
    }
  }

  // Mutex to sync writes to file_writer, and all following
  // class data members
  port::Mutex mutex_;
  // Indicates if logging is currently enabled
  // atomic to allow reads without mutex
  std::atomic<bool> activity_logging_enabled_;
  // When reached, we will stop logging and close the file
  // Value of 0 means unlimited
  uint64_t max_logging_size_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  Status bg_status_;
};

// SimCacheImpl definition
class SimCacheImpl : public SimCache {
 public:
  // capacity for real cache (ShardedLRUCache)
  // test_capacity for key only cache
  SimCacheImpl(std::shared_ptr<Cache> sim_cache, std::shared_ptr<Cache> cache)
      : SimCache(cache),
        key_only_cache_(sim_cache),
        miss_times_(0),
        hit_times_(0),
        stats_(nullptr) {}

  ~SimCacheImpl() override = default;

  const char* Name() const override { return "SimCache"; }

  void SetCapacity(size_t capacity) override { target_->SetCapacity(capacity); }

  void SetStrictCapacityLimit(bool strict_capacity_limit) override {
    target_->SetStrictCapacityLimit(strict_capacity_limit);
  }

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const CacheItemHelper* helper, size_t charge, Handle** handle,
                Priority priority, const Slice& compressed = {},
                CompressionType type = kNoCompression) override {
    // The handle and value passed in are for real cache, so we pass nullptr
    // to key_only_cache_ for both instead. Also, the deleter function pointer
    // will be called by user to perform some external operation which should
    // be applied only once. Thus key_only_cache accepts an empty function.
    // *Lambda function without capture can be assgined to a function pointer
    Handle* h = key_only_cache_->Lookup(key);
    if (h == nullptr) {
      // TODO: Check for error here?
      auto s =
          key_only_cache_->Insert(key, nullptr, &kNoopCacheItemHelper, charge,
                                  nullptr, priority, compressed, type);
      s.PermitUncheckedError();
    } else {
      key_only_cache_->Release(h);
    }

    cache_activity_logger_.ReportAdd(key, charge);
    if (!target_) {
      return Status::OK();
    }
    return target_->Insert(key, value, helper, charge, handle, priority,
                           compressed, type);
  }

  Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                 CreateContext* create_context,
                 Priority priority = Priority::LOW,
                 Statistics* stats = nullptr) override {
    HandleLookup(key, stats);
    if (!target_) {
      return nullptr;
    }
    return target_->Lookup(key, helper, create_context, priority, stats);
  }

  void StartAsyncLookup(AsyncLookupHandle& async_handle) override {
    HandleLookup(async_handle.key, async_handle.stats);
    if (target_) {
      target_->StartAsyncLookup(async_handle);
    }
  }

  bool Ref(Handle* handle) override { return target_->Ref(handle); }

  using Cache::Release;
  bool Release(Handle* handle, bool erase_if_last_ref = false) override {
    return target_->Release(handle, erase_if_last_ref);
  }

  void Erase(const Slice& key) override {
    target_->Erase(key);
    key_only_cache_->Erase(key);
  }

  Cache::ObjectPtr Value(Handle* handle) override {
    return target_->Value(handle);
  }

  uint64_t NewId() override { return target_->NewId(); }

  size_t GetCapacity() const override { return target_->GetCapacity(); }

  bool HasStrictCapacityLimit() const override {
    return target_->HasStrictCapacityLimit();
  }

  size_t GetUsage() const override { return target_->GetUsage(); }

  size_t GetUsage(Handle* handle) const override {
    return target_->GetUsage(handle);
  }

  size_t GetCharge(Handle* handle) const override {
    return target_->GetCharge(handle);
  }

  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override {
    return target_->GetCacheItemHelper(handle);
  }

  size_t GetPinnedUsage() const override { return target_->GetPinnedUsage(); }

  void DisownData() override {
    target_->DisownData();
    key_only_cache_->DisownData();
  }

  void ApplyToAllEntries(
      const std::function<void(const Slice& key, ObjectPtr value, size_t charge,
                               const CacheItemHelper* helper)>& callback,
      const ApplyToAllEntriesOptions& opts) override {
    target_->ApplyToAllEntries(callback, opts);
  }

  void EraseUnRefEntries() override {
    target_->EraseUnRefEntries();
    key_only_cache_->EraseUnRefEntries();
  }

  size_t GetSimCapacity() const override {
    return key_only_cache_->GetCapacity();
  }
  size_t GetSimUsage() const override { return key_only_cache_->GetUsage(); }
  void SetSimCapacity(size_t capacity) override {
    key_only_cache_->SetCapacity(capacity);
  }

  uint64_t get_miss_counter() const override {
    return miss_times_.load(std::memory_order_relaxed);
  }

  uint64_t get_hit_counter() const override {
    return hit_times_.load(std::memory_order_relaxed);
  }

  void reset_counter() override {
    miss_times_.store(0, std::memory_order_relaxed);
    hit_times_.store(0, std::memory_order_relaxed);
    SetTickerCount(stats_, SIM_BLOCK_CACHE_HIT, 0);
    SetTickerCount(stats_, SIM_BLOCK_CACHE_MISS, 0);
  }

  std::string ToString() const override {
    std::ostringstream oss;
    oss << "SimCache MISSes:  " << get_miss_counter() << std::endl;
    oss << "SimCache HITs:    " << get_hit_counter() << std::endl;
    auto lookups = get_miss_counter() + get_hit_counter();
    oss << "SimCache HITRATE: " << std::fixed << std::setprecision(2)
        << (lookups == 0 ? 0 : get_hit_counter() * 100.0f / lookups)
        << std::endl;
    return oss.str();
  }

  std::string GetPrintableOptions() const override {
    std::ostringstream oss;
    oss << "    cache_options:" << std::endl;
    oss << target_->GetPrintableOptions();
    oss << "    sim_cache_options:" << std::endl;
    oss << key_only_cache_->GetPrintableOptions();
    return oss.str();
  }

  Status StartActivityLogging(const std::string& activity_log_file, Env* env,
                              uint64_t max_logging_size = 0) override {
    return cache_activity_logger_.StartLogging(activity_log_file, env,
                                               max_logging_size);
  }

  void StopActivityLogging() override { cache_activity_logger_.StopLogging(); }

  Status GetActivityLoggingStatus() override {
    return cache_activity_logger_.bg_status();
  }

 private:
  std::shared_ptr<Cache> key_only_cache_;
  std::atomic<uint64_t> miss_times_;
  std::atomic<uint64_t> hit_times_;
  Statistics* stats_;
  CacheActivityLogger cache_activity_logger_;

  void inc_miss_counter() {
    miss_times_.fetch_add(1, std::memory_order_relaxed);
  }
  void inc_hit_counter() { hit_times_.fetch_add(1, std::memory_order_relaxed); }

  void HandleLookup(const Slice& key, Statistics* stats) {
    Handle* h = key_only_cache_->Lookup(key);
    if (h != nullptr) {
      key_only_cache_->Release(h);
      inc_hit_counter();
      RecordTick(stats, SIM_BLOCK_CACHE_HIT);
    } else {
      inc_miss_counter();
      RecordTick(stats, SIM_BLOCK_CACHE_MISS);
    }
    cache_activity_logger_.ReportLookup(key);
  }
};

}  // end anonymous namespace

// For instrumentation purpose, use NewSimCache instead
std::shared_ptr<SimCache> NewSimCache(std::shared_ptr<Cache> cache,
                                      size_t sim_capacity, int num_shard_bits) {
  LRUCacheOptions co;
  co.capacity = sim_capacity;
  co.num_shard_bits = num_shard_bits;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  return NewSimCache(NewLRUCache(co), cache, num_shard_bits);
}

std::shared_ptr<SimCache> NewSimCache(std::shared_ptr<Cache> sim_cache,
                                      std::shared_ptr<Cache> cache,
                                      int num_shard_bits) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  return std::make_shared<SimCacheImpl>(sim_cache, cache);
}

}  // namespace ROCKSDB_NAMESPACE
