//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "memory/concurrent_arena.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/dynamic_bloom.h"
#include "util/random.h"

#pragma once

#ifndef NDEBUG
namespace ROCKSDB_NAMESPACE {
// A hacky allocator for single use.
// Arena depends on SyncPoint and create circular dependency.
class SingleAllocator : public Allocator {
 public:
  char* Allocate(size_t) override {
    assert(false);
    return nullptr;
  }
  char* AllocateAligned(size_t bytes, size_t, Logger*) override {
    buf_.resize(bytes);
    return const_cast<char*>(buf_.data());
  }
  size_t BlockSize() const override {
    assert(false);
    return 0;
  }

 private:
  std::string buf_;
};

struct SyncPoint::Data {
  Data() : point_filter_(&alloc_, /*total_bits=*/8192), enabled_(false) {}
  // Enable proper deletion by subclasses
  virtual ~Data() {}
  // successor/predecessor map loaded from LoadDependency
  std::unordered_map<std::string, std::vector<std::string>> successors_;
  std::unordered_map<std::string, std::vector<std::string>> predecessors_;
  std::unordered_map<std::string, std::function<void(void*)>> callbacks_;
  std::unordered_map<std::string, std::vector<std::string>> markers_;
  std::unordered_map<std::string, std::thread::id> marked_thread_id_;

  std::mutex mutex_;
  std::condition_variable cv_;
  // sync points that have been passed through
  std::unordered_set<std::string> cleared_points_;
  SingleAllocator alloc_;
  // A filter before holding mutex to speed up process.
  DynamicBloom point_filter_;
  std::atomic<bool> enabled_;
  int num_callbacks_running_ = 0;

  void LoadDependency(const std::vector<SyncPointPair>& dependencies);
  void LoadDependencyAndMarkers(const std::vector<SyncPointPair>& dependencies,
                                const std::vector<SyncPointPair>& markers);
  bool PredecessorsAllCleared(const std::string& point);
  void SetCallBack(const std::string& point,
                   const std::function<void(void*)>& callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callbacks_[point] = callback;
    point_filter_.Add(point);
  }

  void ClearCallBack(const std::string& point);
  void ClearAllCallBacks();
  void EnableProcessing() { enabled_ = true; }
  void DisableProcessing() {
    enabled_ = false;
    // Wake up any threads blocked in Process() so they can recheck enabled_
    // and return early instead of waiting forever for predecessors that will
    // never fire.
    cv_.notify_all();
  }
  void ClearTrace() {
    std::lock_guard<std::mutex> lock(mutex_);
    cleared_points_.clear();
  }
  bool DisabledByMarker(const std::string& point, std::thread::id thread_id) {
    auto marked_point_iter = marked_thread_id_.find(point);
    return marked_point_iter != marked_thread_id_.end() &&
           thread_id != marked_point_iter->second;
  }
  void Process(const Slice& point, void* cb_arg);
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // NDEBUG
