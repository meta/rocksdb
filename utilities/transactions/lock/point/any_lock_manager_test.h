//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "utilities/transactions/lock/point/point_lock_manager_test.h"

namespace ROCKSDB_NAMESPACE {

using init_func_t = void (*)(PointLockManagerTest*);

class AnyLockManagerTest : public PointLockManagerTest,
                           public testing::WithParamInterface<init_func_t> {
 public:
  void SetUp() override {
    // If a custom setup function was provided, use it. Otherwise, use what we
    // have inherited.
    auto init_func = GetParam();
    if (init_func) {
      (*init_func)(this);
    } else {
      PointLockManagerTest::SetUp();
    }
  }
};

TEST_P(AnyLockManagerTest, ReentrantExclusiveLock) {
  // Tests that a txn can acquire exclusive lock on the same key repeatedly.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn, 1, "k", env_);

  delete txn;
}

TEST_P(AnyLockManagerTest, ReentrantSharedLock) {
  // Tests that a txn can acquire shared lock on the same key repeatedly.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));

  // Cleanup
  if (dynamic_cast<PointLockManager*>(locker_.get()) != nullptr &&
      dynamic_cast<PerKeyPointLockManager*>(locker_.get()) == nullptr) {
    // PointLockManager would create 2 entries in the lock manager, so it needs
    // to unlock it twice.
    locker_->UnLock(txn, 1, "k", env_);
  }
  locker_->UnLock(txn, 1, "k", env_);

  delete txn;
}

TEST_P(AnyLockManagerTest, LockUpgrade) {
  // Tests that a txn can upgrade from a shared lock to an exclusive lock.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn, 1, "k", env_);
  delete txn;
}

TEST_P(AnyLockManagerTest, LockDowngrade) {
  // Tests that a txn can acquire a shared lock after acquiring an exclusive
  // lock on the same key.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn = NewTxn();
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, true));
  ASSERT_OK(locker_->TryLock(txn, 1, "k", env_, false));

  // Cleanup
  locker_->UnLock(txn, 1, "k", env_);
  delete txn;
}

TEST_P(AnyLockManagerTest, LockConflict) {
  // Tests that lock conflicts lead to lock timeout.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn1 = NewTxn();
  auto txn2 = NewTxn();

  {
    // exclusive-exclusive conflict.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
    auto s = locker_->TryLock(txn2, 1, "k1", env_, true);
    ASSERT_TRUE(s.IsTimedOut());
  }

  {
    // exclusive-shared conflict.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k2", env_, true));
    auto s = locker_->TryLock(txn2, 1, "k2", env_, false);
    ASSERT_TRUE(s.IsTimedOut());
  }

  {
    // shared-exclusive conflict.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k2", env_, false));
    auto s = locker_->TryLock(txn2, 1, "k2", env_, true);
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Cleanup
  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn1, 1, "k2", env_);

  delete txn1;
  delete txn2;
}

TEST_P(AnyLockManagerTest, SharedLocks) {
  // Tests that shared locks can be concurrently held by multiple transactions.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  auto txn1 = NewTxn();
  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, false));

  // Cleanup
  locker_->UnLock(txn1, 1, "k", env_);
  locker_->UnLock(txn2, 1, "k", env_);

  delete txn1;
  delete txn2;
}

TEST_P(AnyLockManagerTest, Deadlock) {
  // Tests that deadlock can be detected.
  // Deadlock scenario:
  // txn1 exclusively locks k1, and wants to lock k2;
  // txn2 exclusively locks k2, and wants to lock k1.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  // disable dead lock timeout, so that the dead lock detection behavior is
  // consistent. This prevents the test to be flaky
  txn_opt.deadlock_timeout_us = 0;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = 1000000;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k2", env_, true));

  // txn1 tries to lock k2, will be blocked.
  port::Thread t;
  BlockUntilWaitingTxn(wait_sync_point_name_, t, [&]() {
    // block because txn2 is holding a lock on k2.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k2", env_, true));
  });

  auto s = locker_->TryLock(txn2, 1, "k1", env_, true);
  ASSERT_TRUE(s.IsBusy());
  ASSERT_EQ(s.subcode(), Status::SubCode::kDeadlock);

  std::vector<DeadlockPath> deadlock_paths = locker_->GetDeadlockInfoBuffer();
  ASSERT_EQ(deadlock_paths.size(), 1u);
  ASSERT_FALSE(deadlock_paths[0].limit_exceeded);

  std::vector<DeadlockInfo> deadlocks = deadlock_paths[0].path;
  ASSERT_EQ(deadlocks.size(), 2u);

  ASSERT_EQ(deadlocks[0].m_txn_id, txn1->GetID());
  ASSERT_EQ(deadlocks[0].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[0].m_exclusive);
  ASSERT_EQ(deadlocks[0].m_waiting_key, "k2");

  ASSERT_EQ(deadlocks[1].m_txn_id, txn2->GetID());
  ASSERT_EQ(deadlocks[1].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[1].m_exclusive);
  ASSERT_EQ(deadlocks[1].m_waiting_key, "k1");

  locker_->UnLock(txn2, 1, "k2", env_);
  t.join();

  // Cleanup
  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn1, 1, "k2", env_);
  delete txn2;
  delete txn1;
}

TEST_P(AnyLockManagerTest, GetWaitingTxnsMultipleTxns) {
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, false));

  auto txn3 = NewTxn();
  txn3->SetLockTimeout(10000);
  port::Thread t1;
  BlockUntilWaitingTxn(wait_sync_point_name_, t1, [&]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k", env_, true));
    locker_->UnLock(txn3, 1, "k", env_);
  });

  // Ok, now txn3 is waiting for lock on "k", which is owned by two
  // transactions. Check that GetWaitingTxns reports this correctly
  uint32_t wait_cf_id;
  std::string wait_key;
  auto waiters = txn3->GetWaitingTxns(&wait_cf_id, &wait_key);

  ASSERT_EQ(wait_cf_id, 1u);
  ASSERT_EQ(wait_key, "k");
  ASSERT_EQ(waiters.size(), 2);
  bool waits_correct =
      (waiters[0] == txn1->GetID() && waiters[1] == txn2->GetID()) ||
      (waiters[1] == txn1->GetID() && waiters[0] == txn2->GetID());
  ASSERT_EQ(waits_correct, true);

  // Release locks so txn3 can proceed with execution
  locker_->UnLock(txn1, 1, "k", env_);
  locker_->UnLock(txn2, 1, "k", env_);

  // Wait until txn3 finishes
  t1.join();

  delete txn1;
  delete txn2;
  delete txn3;
}

}  // namespace ROCKSDB_NAMESPACE
