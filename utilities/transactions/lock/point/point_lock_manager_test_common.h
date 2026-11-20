//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <sstream>

#include "rocksdb/db.h"
#include "utilities/transactions/lock/lock_manager.h"

namespace ROCKSDB_NAMESPACE {

constexpr auto kLongTxnTimeoutMs = 100000;
constexpr auto kShortTxnTimeoutMs = 100;

class MockColumnFamilyHandle : public ColumnFamilyHandle {
 public:
  explicit MockColumnFamilyHandle(ColumnFamilyId cf_id) : cf_id_(cf_id) {}

  // disable copy and assignment
  MockColumnFamilyHandle(const MockColumnFamilyHandle&) = delete;
  MockColumnFamilyHandle& operator=(const MockColumnFamilyHandle&) = delete;
  // disable move
  MockColumnFamilyHandle(MockColumnFamilyHandle&&) = delete;
  MockColumnFamilyHandle& operator=(MockColumnFamilyHandle&&) = delete;

  ~MockColumnFamilyHandle() override {}

  const std::string& GetName() const override { return name_; }

  ColumnFamilyId GetID() const override { return cf_id_; }

  Status GetDescriptor(ColumnFamilyDescriptor*) override {
    return Status::OK();
  }

  const Comparator* GetComparator() const override {
    return BytewiseComparator();
  }

 private:
  ColumnFamilyId cf_id_;
  std::string name_ = "MockCF";
};

// Verify no lock was held. Return true, if success. False, if there is. Set
// error message on False.
inline bool verifyNoLocksHeld(std::shared_ptr<LockManager>& locker,
                              std::string& errmsg) {
  // Validate no lock was held at the end of the test
  auto lock_status = locker->GetPointLockStatus();
  // print the lock status for debugging
  std::stringstream ss;
  for (auto& s : lock_status) {
    ss << "id " << s.first;
    ss << " key " << s.second.key;
    ss << " type " << (s.second.exclusive ? "exclusive" : "shared");
    ss << " txn ids [";
    for (auto& t : s.second.ids) {
      ss << t << ",";
    }
    ss << "]";
    ss << std::endl;
  }

  if (!lock_status.empty()) {
    errmsg = std::to_string(lock_status.size()) +
             " locks were held at the end. " + ss.str();
    return false;
  }

  return true;
}

}  // namespace ROCKSDB_NAMESPACE
