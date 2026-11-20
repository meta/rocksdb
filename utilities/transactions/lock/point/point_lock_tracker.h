// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "utilities/transactions/lock/lock_tracker.h"

namespace ROCKSDB_NAMESPACE {

struct TrackedKeyInfo {
  // Earliest sequence number that is relevant to this transaction for this key
  SequenceNumber seq;

  uint32_t num_writes;
  uint32_t num_reads;

  bool exclusive;

  explicit TrackedKeyInfo(SequenceNumber seq_no)
      : seq(seq_no), num_writes(0), num_reads(0), exclusive(false) {}

  void Merge(const TrackedKeyInfo& info) {
    assert(seq <= info.seq);
    num_reads += info.num_reads;
    num_writes += info.num_writes;
    exclusive = exclusive || info.exclusive;
  }
};

using TrackedKeyInfos = std::unordered_map<std::string, TrackedKeyInfo>;

using TrackedKeys = std::unordered_map<ColumnFamilyId, TrackedKeyInfos>;

// Tracks point locks on single keys.
class PointLockTracker : public LockTracker {
 public:
  PointLockTracker() = default;
  ~PointLockTracker() override = default;

  PointLockTracker(const PointLockTracker&) = delete;
  PointLockTracker& operator=(const PointLockTracker&) = delete;
  PointLockTracker(PointLockTracker&&) = delete;
  PointLockTracker& operator=(PointLockTracker&&) = delete;

  bool IsPointLockSupported() const override { return true; }

  bool IsRangeLockSupported() const override { return false; }

  void Track(const PointLockRequest& lock_request) override;

  UntrackStatus Untrack(const PointLockRequest& lock_request) override;

  void Track(const RangeLockRequest& /*lock_request*/) override {}

  UntrackStatus Untrack(const RangeLockRequest& /*lock_request*/) override {
    return UntrackStatus::NOT_TRACKED;
  }

  void Merge(const LockTracker& tracker) override;

  void Subtract(const LockTracker& tracker) override;

  void Clear() override;

  LockTracker* GetTrackedLocksSinceSavePoint(
      const LockTracker& save_point_tracker) const override;

  PointLockStatus GetPointLockStatus(ColumnFamilyId column_family_id,
                                     const std::string& key) const override;

  uint64_t GetNumPointLocks() const override;

  ColumnFamilyIterator* GetColumnFamilyIterator() const override;

  KeyIterator* GetKeyIterator(ColumnFamilyId column_family_id) const override;

 private:
  TrackedKeys tracked_keys_;
};

class PointLockTrackerFactory : public LockTrackerFactory {
 public:
  static const PointLockTrackerFactory& Get() {
    static const PointLockTrackerFactory instance;
    return instance;
  }

  LockTracker* Create() const override { return new PointLockTracker(); }

 private:
  PointLockTrackerFactory() {}
};

}  // namespace ROCKSDB_NAMESPACE
