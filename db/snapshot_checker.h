// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

enum class SnapshotCheckerResult : int {
  kInSnapshot = 0,
  kNotInSnapshot = 1,
  // In case snapshot is released and the checker has no clue whether
  // the given sequence is visible to the snapshot.
  kSnapshotReleased = 2,
};

// Callback class that control GC of duplicate keys in flush/compaction.
class SnapshotChecker {
 public:
  virtual ~SnapshotChecker() = default;
  virtual SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber sequence, SequenceNumber snapshot_sequence) const = 0;
};

class DisableGCSnapshotChecker : public SnapshotChecker {
 public:
  ~DisableGCSnapshotChecker() override = default;
  SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber /*sequence*/,
      SequenceNumber /*snapshot_sequence*/) const override {
    // By returning kNotInSnapshot, we prevent all the values from being GCed
    return SnapshotCheckerResult::kNotInSnapshot;
  }
  static DisableGCSnapshotChecker* Instance();

 protected:
  explicit DisableGCSnapshotChecker() {}
};

class WritePreparedTxnDB;

// Callback class created by WritePreparedTxnDB to check if a key
// is visible by a snapshot.
class WritePreparedSnapshotChecker : public SnapshotChecker {
 public:
  explicit WritePreparedSnapshotChecker(WritePreparedTxnDB* txn_db);
  ~WritePreparedSnapshotChecker() override = default;

  SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber sequence, SequenceNumber snapshot_sequence) const override;

 private:
  const WritePreparedTxnDB* const txn_db_;
};

bool DataIsDefinitelyInSnapshot(SequenceNumber seqno, SequenceNumber snapshot,
                                const SnapshotChecker* snapshot_checker);

bool DataIsDefinitelyNotInSnapshot(SequenceNumber seqno,
                                   SequenceNumber snapshot,
                                   const SnapshotChecker* snapshot_checker);

}  // namespace ROCKSDB_NAMESPACE
