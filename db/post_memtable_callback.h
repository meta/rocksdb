//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

// Callback invoked after finishing writing to the memtable but before
// publishing the sequence number to readers.
// Note that with write-prepared/write-unprepared transactions with
// two-write-queues, PreReleaseCallback is called before publishing the
// sequence numbers to readers.
class PostMemTableCallback {
 public:
  virtual ~PostMemTableCallback() = default;

  virtual Status operator()(SequenceNumber seq, bool disable_memtable) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
