//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// Answers whether any version of a user key exists outside the input of a
// CompactionIterator.
class AbsenceProver {
 public:
  virtual ~AbsenceProver() = default;

  // Returns true only if no entry for `user_key`, at any sequence number,
  // exists outside the iterator's input. Returning false is always safe.
  // Must not perform I/O. Calls arrive in non-decreasing user key order
  // (ignoring timestamps), and a key may be queried more than once.
  virtual bool ProveAbsent(const Slice& user_key) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
