//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

/*
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */
#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

/// Unbounded thread-safe work queue.
//
// This file is an excerpt from Facebook's zstd repo at
// https://github.com/facebook/zstd/. The relevant file is
// contrib/pzstd/utils/WorkQueue.h.

template <typename T>
class WorkQueue {
  // Protects all member variable access
  std::mutex mutex_;
  std::condition_variable readerCv_;
  std::condition_variable writerCv_;
  std::condition_variable finishCv_;

  std::queue<T> queue_;
  bool done_;
  std::size_t maxSize_;

  // Must have lock to call this function
  bool full() const {
    if (maxSize_ == 0) {
      return false;
    }
    return queue_.size() >= maxSize_;
  }

 public:
  /**
   * Constructs an empty work queue with an optional max size.
   * If `maxSize == 0` the queue size is unbounded.
   *
   * @param maxSize The maximum allowed size of the work queue.
   */
  explicit WorkQueue(std::size_t maxSize = 0)
      : done_(false), maxSize_(maxSize) {}

  /**
   * Push an item onto the work queue.  Notify a single thread that work is
   * available.  If `finish()` has been called, do nothing and return false.
   * If `push()` returns false, then `item` has not been copied from.
   *
   * @param item  Item to push onto the queue.
   * @returns     True upon success, false if `finish()` has been called.  An
   *               item was pushed iff `push()` returns true.
   */
  template <typename U>
  bool push(U&& item) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      while (full() && !done_) {
        writerCv_.wait(lock);
      }
      if (done_) {
        return false;
      }
      queue_.push(std::forward<U>(item));
    }
    readerCv_.notify_one();
    return true;
  }

  /**
   * Attempts to pop an item off the work queue.  It will block until data is
   * available or `finish()` has been called.
   *
   * @param[out] item  If `pop` returns `true`, it contains the popped item.
   *                    If `pop` returns `false`, it is unmodified.
   * @returns          True upon success.  False if the queue is empty and
   *                    `finish()` has been called.
   */
  bool pop(T& item) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      while (queue_.empty() && !done_) {
        readerCv_.wait(lock);
      }
      if (queue_.empty()) {
        assert(done_);
        return false;
      }
      item = queue_.front();
      queue_.pop();
    }
    writerCv_.notify_one();
    return true;
  }

  /**
   * Sets the maximum queue size.  If `maxSize == 0` then it is unbounded.
   *
   * @param maxSize The new maximum queue size.
   */
  void setMaxSize(std::size_t maxSize) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      maxSize_ = maxSize;
    }
    writerCv_.notify_all();
  }

  /**
   * Promise that `push()` won't be called again, so once the queue is empty
   * there will never any more work.
   */
  void finish() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      assert(!done_);
      done_ = true;
    }
    readerCv_.notify_all();
    writerCv_.notify_all();
    finishCv_.notify_all();
  }

  /// Blocks until `finish()` has been called (but the queue may not be empty).
  void waitUntilFinished() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (!done_) {
      finishCv_.wait(lock);
    }
  }
};
}  // namespace ROCKSDB_NAMESPACE
