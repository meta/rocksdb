//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Custom callback functions to support users to plug in logic while data is
// being written to the DB. It's intended for better synchronization between
// concurrent writes. Note that these callbacks are in the write's critical path
// It's desirable to keep them fast and minimum to not affect the write's
// latency. These callbacks may be called in the context of a different thread.
class UserWriteCallback {
 public:
  virtual ~UserWriteCallback() = default;

  // This function will be called after the write is enqueued.
  virtual void OnWriteEnqueued() = 0;

  // This function will be called after wal write finishes if it applies.
  virtual void OnWalWriteFinish() = 0;
};

}  // namespace ROCKSDB_NAMESPACE
