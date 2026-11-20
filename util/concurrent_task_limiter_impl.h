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
#include <memory>

#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

class TaskLimiterToken;

class ConcurrentTaskLimiterImpl : public ConcurrentTaskLimiter {
 public:
  explicit ConcurrentTaskLimiterImpl(const std::string& name,
                                     int32_t max_outstanding_task);
  // No copying allowed
  ConcurrentTaskLimiterImpl(const ConcurrentTaskLimiterImpl&) = delete;
  ConcurrentTaskLimiterImpl& operator=(const ConcurrentTaskLimiterImpl&) =
      delete;
  ConcurrentTaskLimiterImpl(ConcurrentTaskLimiterImpl&&) = delete;
  ConcurrentTaskLimiterImpl& operator=(ConcurrentTaskLimiterImpl&&) = delete;

  ~ConcurrentTaskLimiterImpl() override;

  const std::string& GetName() const override;

  void SetMaxOutstandingTask(int32_t limit) override;

  void ResetMaxOutstandingTask() override;

  int32_t GetOutstandingTask() const override;

  // Request token for adding a new task.
  // If force == true, it requests a token bypassing throttle.
  // Returns nullptr if it got throttled.
  virtual std::unique_ptr<TaskLimiterToken> GetToken(bool force);

 private:
  friend class TaskLimiterToken;

  std::string name_;
  std::atomic<int32_t> max_outstanding_tasks_;
  std::atomic<int32_t> outstanding_tasks_;
};

class TaskLimiterToken {
 public:
  explicit TaskLimiterToken(ConcurrentTaskLimiterImpl* limiter)
      : limiter_(limiter) {}
  ~TaskLimiterToken();

 private:
  ConcurrentTaskLimiterImpl* limiter_;

  // no copying allowed
  TaskLimiterToken(const TaskLimiterToken&) = delete;
  void operator=(const TaskLimiterToken&) = delete;
  TaskLimiterToken(TaskLimiterToken&&) = delete;
  void operator=(TaskLimiterToken&&) = delete;
};

}  // namespace ROCKSDB_NAMESPACE
