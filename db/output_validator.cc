//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/output_validator.h"

#include "test_util/sync_point.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {
Status OutputValidator::CanAdd(const Slice& key) const {
  return CanAdd(key, false);
}

Status OutputValidator::CanAdd(const Slice& key,
                               bool require_strict_key_order) const {
  if (key.size() < kNumInternalBytes) {
    return Status::Corruption(
        "Compaction tries to write a key without internal bytes.");
  }
  // prev_key_ starts with empty.
  if (!prev_key_.empty()) {
    const int cmp = icmp_.Compare(key, prev_key_);
    if (cmp < 0) {
      return Status::Corruption("Compaction sees out-of-order keys.");
    }
    if (cmp == 0 && require_strict_key_order) {
      return Status::NotSupported(
          "Unresolved equal-sequence merge operands cannot be stored as "
          "duplicate internal keys in one SST");
    }
  }
  return Status::OK();
}

Status OutputValidator::Add(const Slice& key, const Slice& value) {
  Status s = CanAdd(key);
  if (!s.ok()) {
    return s;
  }
  AddValidated(key, value);
  return Status::OK();
}

Status OutputValidator::AddStrict(const Slice& key, const Slice& value) {
  Status s = CanAdd(key, true);
  if (!s.ok()) {
    return s;
  }
  AddValidated(key, value);
  return Status::OK();
}

void OutputValidator::AddValidated(const Slice& key, const Slice& value) {
  if (enable_hash_) {
    // Generate a rolling 64-bit hash of the key and values.
    paranoid_hash_ = NPHash64(key.data(), key.size(), paranoid_hash_);
    paranoid_hash_ = NPHash64(value.data(), value.size(), paranoid_hash_);
  }
  prev_key_.assign(key.data(), key.size());
}
}  // namespace ROCKSDB_NAMESPACE
