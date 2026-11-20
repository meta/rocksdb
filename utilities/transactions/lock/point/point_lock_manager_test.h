//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "file/file_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testharness.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/lock/point/point_lock_manager_test_common.h"
#include "utilities/transactions/pessimistic_transaction_db.h"

namespace ROCKSDB_NAMESPACE {

class PointLockManagerTest : public testing::Test {
 public:
  void init() {
    env_ = Env::Default();
    db_dir_ = test::PerThreadDBPath("point_lock_manager_test");
    ASSERT_OK(env_->CreateDir(db_dir_));

    Options opt;
    opt.create_if_missing = true;
    // Reduce the number of stripes to 4 to increase contention in test
    txndb_opt_.num_stripes = 4;
    txndb_opt_.transaction_lock_timeout = 0;

    ASSERT_OK(TransactionDB::Open(opt, txndb_opt_, db_dir_, &db_));

    wait_sync_point_name_ = "PointLockManager::AcquireWithTimeout:WaitingTxn";
  }

  void SetUp() override {
    init();
    // CAUTION: This test creates a separate lock manager object (right, NOT
    // the one that the TransactionDB is using!), and runs tests on it.
    locker_ = std::make_shared<PointLockManager>(
        static_cast<PessimisticTransactionDB*>(db_), txndb_opt_);
  }

  void TearDown() override {
    std::string errmsg;
    auto no_lock_held = verifyNoLocksHeld(locker_, errmsg);
    ASSERT_TRUE(no_lock_held) << errmsg;
    delete db_;
    EXPECT_OK(DestroyDir(env_, db_dir_));
  }

  PessimisticTransaction* NewTxn(
      TransactionOptions txn_opt = TransactionOptions()) {
    // override deadlock_timeout_us;
    txn_opt.deadlock_timeout_us = deadlock_timeout_us;
    Transaction* txn = db_->BeginTransaction(WriteOptions(), txn_opt);
    return static_cast<PessimisticTransaction*>(txn);
  }

  int64_t deadlock_timeout_us = 0;

  void UsePerKeyPointLockManager() {
    locker_ = std::make_shared<PerKeyPointLockManager>(
        static_cast<PessimisticTransactionDB*>(db_), txndb_opt_);
  }

 protected:
  Env* env_;
  TransactionDBOptions txndb_opt_;
  std::shared_ptr<LockManager> locker_;
  const char* wait_sync_point_name_;
  friend void PointLockManagerTestExternalSetup(PointLockManagerTest*);

  std::string db_dir_;
  TransactionDB* db_;
};

inline void BlockUntilWaitingTxn(const char* sync_point_name, port::Thread& t,
                                 std::function<void()> f) {
  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      sync_point_name, [&](void* /*arg*/) { reached.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  t = port::Thread(f);

  // timeout after 30 seconds, so test does not hang forever
  // 30 seconds should be enough for the test to reach the expected state
  // without causing too much flakiness
  for (int i = 0; i < 3000; i++) {
    if (reached.load()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ASSERT_TRUE(reached.load());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE
