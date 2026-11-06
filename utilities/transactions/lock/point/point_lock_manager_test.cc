//  Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/lock/point/point_lock_manager_test.h"

#include "utilities/transactions/lock/point/any_lock_manager_test.h"

namespace ROCKSDB_NAMESPACE {

struct SpotLockManagerTestParam {
  bool use_per_key_point_lock_manager;
  int deadlock_timeout_us;
};

// Define operator<< for SpotLockManagerTestParam to stop valgrind from
// complaining uinitialized value when printing SpotLockManagerTestParam.
std::ostream& operator<<(std::ostream& os,
                         const SpotLockManagerTestParam& param) {
  os << "use_per_key_point_lock_manager: "
     << param.use_per_key_point_lock_manager
     << ", deadlock_timeout_us: " << param.deadlock_timeout_us;
  return os;
}

// including test for both PointLockManager and PerKeyPointLockManager
class SpotLockManagerTest
    : public PointLockManagerTest,
      public testing::WithParamInterface<SpotLockManagerTestParam> {
 public:
  void SetUp() override {
    init();
    // If a custom setup function was provided, use it. Otherwise, use what we
    // have inherited.
    auto param = GetParam();
    if (param.use_per_key_point_lock_manager) {
      locker_.reset(new PerKeyPointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    } else {
      locker_.reset(new PointLockManager(
          static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    }
    deadlock_timeout_us = param.deadlock_timeout_us;
  }
};

// This test is not applicable for Range Lock manager as Range Lock Manager
// operates on Column Families, not their ids.
TEST_P(SpotLockManagerTest, LockNonExistingColumnFamily) {
  MockColumnFamilyHandle cf(1024);
  locker_->RemoveColumnFamily(&cf);
  auto txn = NewTxn();
  auto s = locker_->TryLock(txn, 1024, "k", env_, true);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_STREQ(s.getState(), "Column family id not found: 1024");
  delete txn;
}

TEST_P(SpotLockManagerTest, LockStatus) {
  MockColumnFamilyHandle cf1(1024);
  MockColumnFamilyHandle cf2(2048);
  locker_->AddColumnFamily(&cf1);
  locker_->AddColumnFamily(&cf2);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1024, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn1, 2048, "k1", env_, true));

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1024, "k2", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 2048, "k2", env_, false));

  auto s = locker_->GetPointLockStatus();
  ASSERT_EQ(s.size(), 4u);
  for (uint32_t cf_id : {1024, 2048}) {
    ASSERT_EQ(s.count(cf_id), 2u);
    auto range = s.equal_range(cf_id);
    for (auto it = range.first; it != range.second; it++) {
      ASSERT_TRUE(it->second.key == "k1" || it->second.key == "k2");
      if (it->second.key == "k1") {
        ASSERT_EQ(it->second.exclusive, true);
        ASSERT_EQ(it->second.ids.size(), 1u);
        ASSERT_EQ(it->second.ids[0], txn1->GetID());
      } else if (it->second.key == "k2") {
        ASSERT_EQ(it->second.exclusive, false);
        ASSERT_EQ(it->second.ids.size(), 1u);
        ASSERT_EQ(it->second.ids[0], txn2->GetID());
      }
    }
  }

  // Cleanup
  locker_->UnLock(txn1, 1024, "k1", env_);
  locker_->UnLock(txn1, 2048, "k1", env_);
  locker_->UnLock(txn2, 1024, "k2", env_);
  locker_->UnLock(txn2, 2048, "k2", env_);

  delete txn1;
  delete txn2;
}

TEST_P(SpotLockManagerTest, UnlockExclusive) {
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, true));
  locker_->UnLock(txn1, 1, "k", env_);

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn2, 1, "k", env_);

  delete txn1;
  delete txn2;
}

TEST_P(SpotLockManagerTest, UnlockShared) {
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);

  auto txn1 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn1, 1, "k", env_, false));
  locker_->UnLock(txn1, 1, "k", env_);

  auto txn2 = NewTxn();
  ASSERT_OK(locker_->TryLock(txn2, 1, "k", env_, true));

  // Cleanup
  locker_->UnLock(txn2, 1, "k", env_);

  delete txn1;
  delete txn2;
}

// This test doesn't work with Range Lock Manager, because Range Lock Manager
// doesn't support deadlock_detect_depth.

TEST_P(SpotLockManagerTest, DeadlockDepthExceeded) {
  // Tests that when detecting deadlock, if the detection depth is exceeded,
  // it's also viewed as deadlock.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.deadlock_detect_depth = 1;
  txn_opt.lock_timeout = kLongTxnTimeoutMs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);
  auto txn4 = NewTxn(txn_opt);
  // "a ->(k) b" means transaction a is waiting for transaction b to release
  // the held lock on key k.
  // txn4 ->(k3) -> txn3 ->(k2) txn2 ->(k1) txn1
  // txn3's deadlock detection will exceed the detection depth 1,
  // which will be viewed as a deadlock.
  // NOTE:
  // txn4 ->(k3) -> txn3 must be set up before
  // txn3 ->(k2) -> txn2, because to trigger deadlock detection for txn3,
  // it must have another txn waiting on it, which is txn4 in this case.
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  port::Thread t1;
  BlockUntilWaitingTxn(wait_sync_point_name_, t1, [&]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k2", env_, true));
    // block because txn1 is holding a lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
  });

  ASSERT_OK(locker_->TryLock(txn3, 1, "k3", env_, true));

  port::Thread t2;
  BlockUntilWaitingTxn(wait_sync_point_name_, t2, [&]() {
    // block because txn3 is holding a lock on k1.
    ASSERT_OK(locker_->TryLock(txn4, 1, "k3", env_, true));
  });

  auto s = locker_->TryLock(txn3, 1, "k2", env_, true);
  ASSERT_TRUE(s.IsBusy());
  ASSERT_EQ(s.subcode(), Status::SubCode::kDeadlock);

  std::vector<DeadlockPath> deadlock_paths = locker_->GetDeadlockInfoBuffer();
  ASSERT_EQ(deadlock_paths.size(), 1u);
  ASSERT_TRUE(deadlock_paths[0].limit_exceeded);

  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn3, 1, "k3", env_);
  t1.join();
  t2.join();

  locker_->UnLock(txn2, 1, "k2", env_);
  locker_->UnLock(txn2, 1, "k1", env_);
  locker_->UnLock(txn4, 1, "k3", env_);

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, PrioritizedLockUpgradeWithExclusiveLock) {
  // Tests that a lock upgrade request is prioritized over other lock requests.

  // txn1 acquires shared lock on k1.
  // txn2 acquires exclusive lock on k1.
  // txn1 acquires exclusive locks k1 successfully

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutMs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

  // txn2 tries to lock k1 exclusively, will be blocked.
  port::Thread t;
  BlockUntilWaitingTxn(wait_sync_point_name_, t, [this, &txn2]() {
    // block because txn1 is holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
  });

  // verify lock upgrade successfully
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // unlock txn1, so txn2 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);

  // Cleanup
  t.join();

  // Cleanup
  locker_->UnLock(txn2, 1, "k1", env_);
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest,
       PrioritizedLockUpgradeWithExclusiveLockAndSharedLock) {
  // Tests that lock upgrade is prioritized when mixed with shared and exclusive
  // locks requests

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn3 acquires exclusive lock on k1.
  // txn1 acquires exclusive locks k1 <- request granted after txn2 release the
  // lock

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutMs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));

  // txn3 tries to lock k1 exclusively, will be blocked.
  port::Thread txn3_thread;
  BlockUntilWaitingTxn(wait_sync_point_name_, txn3_thread, [this, &txn3]() {
    // block because txn1 and txn2 are holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });
  // Verify txn3 is blocked
  ASSERT_TRUE(txn3_thread.joinable());

  // txn1 tries to lock k1 exclusively, will be blocked.
  port::Thread txn1_thread;
  BlockUntilWaitingTxn(wait_sync_point_name_, txn1_thread, [this, &txn1]() {
    // block because txn1 and txn2 are holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
  });
  // Verify txn1 is blocked
  ASSERT_TRUE(txn1_thread.joinable());

  // Unlock txn2, so txn1 could proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  txn1_thread.join();

  // Unlock txn1, so txn3 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);
  txn3_thread.join();

  // Cleanup
  locker_->UnLock(txn3, 1, "k1", env_);
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, Deadlock_MultipleUpgrade) {
  // Tests that deadlock can be detected for shared locks and exclusive locks
  // mixed Deadlock scenario:

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn1 acquires exclusive locks k1
  // txn2 acquires exclusive locks k1 <- dead lock detected

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutMs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));

  // txn1 tries to lock k1 exclusively, will be blocked.
  port::Thread t;
  BlockUntilWaitingTxn(wait_sync_point_name_, t, [this, &txn1]() {
    // block because txn2 is holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
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
  ASSERT_EQ(deadlocks[0].m_waiting_key, "k1");

  ASSERT_EQ(deadlocks[1].m_txn_id, txn2->GetID());
  ASSERT_EQ(deadlocks[1].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[1].m_exclusive);
  ASSERT_EQ(deadlocks[1].m_waiting_key, "k1");

  locker_->UnLock(txn2, 1, "k1", env_);
  t.join();

  // Cleanup
  locker_->UnLock(txn1, 1, "k1", env_);
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, Deadlock_MultipleUpgradeInterleaveExclusive) {
  // Tests that deadlock can be detected for shared locks and exclusive locks
  // mixed Deadlock scenario:

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn3 acquires exclusive lock on k1.
  // txn1 acquires exclusive locks k1 <- request granted after txn2 release the
  // lock.
  // txn2 acquires exclusive locks k1 <- dead lock detected

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutMs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);
  auto txn3 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));
  ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));

  // txn3 tries to lock k1 exclusively, will be blocked.
  port::Thread txn3_thread;
  BlockUntilWaitingTxn(wait_sync_point_name_, txn3_thread, [this, &txn3]() {
    // block because txn1 and txn2 are holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });
  // Verify txn3 is blocked
  ASSERT_TRUE(txn3_thread.joinable());

  // txn1 tries to lock k1 exclusively, will be blocked.
  port::Thread txn1_thread;
  BlockUntilWaitingTxn(wait_sync_point_name_, txn1_thread, [this, &txn1]() {
    // block because txn1 and txn2 are holding a shared lock on k1.
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
  });
  // Verify txn1 is blocked
  ASSERT_TRUE(txn1_thread.joinable());

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
  ASSERT_EQ(deadlocks[0].m_waiting_key, "k1");

  ASSERT_EQ(deadlocks[1].m_txn_id, txn2->GetID());
  ASSERT_EQ(deadlocks[1].m_cf_id, 1u);
  ASSERT_TRUE(deadlocks[1].m_exclusive);
  ASSERT_EQ(deadlocks[1].m_waiting_key, "k1");

  // Unlock txn2, so txn1 could proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  txn1_thread.join();

  // Unlock txn1, so txn3 could proceed
  locker_->UnLock(txn1, 1, "k1", env_);
  txn3_thread.join();

  // Cleanup
  locker_->UnLock(txn3, 1, "k1", env_);
  delete txn3;
  delete txn2;
  delete txn1;
}

class PerKeyPointLockManagerTest : public PointLockManagerTest {
 public:
  void SetUp() override {
    init();
    cf_ = std::make_unique<MockColumnFamilyHandle>(1);
    txn_opt_.deadlock_detect = true;
    // by default use long timeout and disable expiration
    txn_opt_.lock_timeout = kLongTxnTimeoutMs;
    txn_opt_.expiration = -1;

    // CAUTION: This test creates a separate lock manager object (right, NOT
    // the one that the TransactionDB is using!), and runs tests on it.
    locker_.reset(new PerKeyPointLockManager(
        static_cast<PessimisticTransactionDB*>(db_), txndb_opt_));
    locker_->AddColumnFamily(cf_.get());
  }

  TransactionOptions txn_opt_;
  std::unique_ptr<MockColumnFamilyHandle> cf_;
};

TEST_F(PerKeyPointLockManagerTest, LockEfficiency) {
  // Create multiple transactions, each acquire exclusive lock on the same key
  std::vector<PessimisticTransaction*> txns;
  std::vector<port::Thread> blockingThreads;

  // Count the total number of wait sync point calls
  std::atomic_int wait_sync_point_times = 0;
  SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&wait_sync_point_times](void* /*arg*/) { wait_sync_point_times++; });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr auto num_of_txn = 10;
  // create 10 transactions, each of them try to acquire exclusive lock on the
  // same key
  for (int i = 0; i < num_of_txn; i++) {
    auto txn = NewTxn(txn_opt_);
    txns.push_back(txn);

    if (i == 0) {
      // txn0 acquires the lock, so the rest of the transactions could block
      ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, true));
    } else {
      blockingThreads.emplace_back([this, txn]() {
        // block because first txn is holding an exclusive lock on k1.
        ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, true));
      });
    }

    // wait for transaction i to be blocked
    while (wait_sync_point_times.load() < i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  // unlock the key, so next transaction could take the lock.
  locker_->UnLock(txns[0], 1, "k1", env_);

  auto num_of_blocking_thread = num_of_txn - 1;

  for (int i = 0; i < num_of_blocking_thread; i++) {
    // validate the thread is finished
    blockingThreads[i].join();
    auto num_of_threads_completed = i + 1;
    for (int j = 0; j < num_of_blocking_thread; j++) {
      if (j < num_of_threads_completed) {
        // validate the thread is no longer joinable
        ASSERT_FALSE(blockingThreads[j].joinable());
      } else {
        // validate the rest of the threads are still joinable
        ASSERT_TRUE(blockingThreads[j].joinable());
      }
    }
    // unlock the key, so next transaction could take the lock.
    locker_->UnLock(txns[i + 1], 1, "k1", env_);
  }

  ASSERT_EQ(wait_sync_point_times.load(), num_of_blocking_thread);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (int i = 0; i < num_of_txn; i++) {
    delete txns[num_of_txn - i - 1];
  }
}

TEST_F(PerKeyPointLockManagerTest, LockFairness) {
  // Create multiple transactions requesting locks on the same key, validate
  // that they are executed in FIFO order

  // txn0 acquires exclusive lock on k1.
  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  // txn3 acquires exclusive lock on k1.
  // txn4 acquires shared lock on k1.
  // txn5 acquires exclusive lock on k1.
  // txn6 acquires exclusive lock on k1.
  // txn7 acquires shared lock on k1.
  // txn8 acquires shared lock on k1.
  // txn9 acquires exclusive lock on k1.

  std::vector<PessimisticTransaction*> txns;
  std::vector<port::Thread> blockingThreads;

  // Count the total number of wait sync point calls
  std::atomic_int wait_sync_point_times = 0;
  SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&wait_sync_point_times](void* /*arg*/) { wait_sync_point_times++; });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr auto num_of_txn = 10;
  std::vector<bool> txn_lock_types = {true, false, false, true,  false,
                                      true, true,  false, false, true};
  // create 10 transactions, each of them try to acquire exclusive lock on the
  // same key
  for (int i = 0; i < num_of_txn; i++) {
    auto txn = NewTxn(txn_opt_);
    txns.push_back(txn);

    if (i == 0) {
      // txn0 acquires the lock, so the rest of the transactions would block
      ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, txn_lock_types[0]));
    } else {
      blockingThreads.emplace_back([this, txn, type = txn_lock_types[i]]() {
        ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, type));
      });
    }

    // wait for transaction i to be blocked
    while (wait_sync_point_times.load() < i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  auto num_of_blocking_thread = num_of_txn - 1;

  auto thread_idx = 0;
  auto txn_idx = 0;

  auto unlockTxn = [&]() {
    // unlock the key in transaction.
    locker_->UnLock(txns[txn_idx++], 1, "k1", env_);
  };

  auto validateLockTakenByNextTxn = [&]() {
    // validate the thread is finished
    blockingThreads[thread_idx++].join();
  };

  auto stillWaitingForLock = [&]() {
    // validate the thread is no longer joinable
    ASSERT_TRUE(blockingThreads[thread_idx].joinable());
  };

  // unlock the key, so next group of transactions could take the lock.
  unlockTxn();

  // txn1 acquires shared lock on k1.
  // txn2 acquires shared lock on k1.
  validateLockTakenByNextTxn();
  validateLockTakenByNextTxn();

  // txn3 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn4 acquires shared lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn5 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn6 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // txn7 acquires shared lock on k1.
  // txn8 acquires shared lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();
  validateLockTakenByNextTxn();

  // txn9 acquires exclusive lock on k1.
  stillWaitingForLock();
  unlockTxn();
  unlockTxn();
  validateLockTakenByNextTxn();

  // clean up
  unlockTxn();

  ASSERT_EQ(wait_sync_point_times.load(), num_of_blocking_thread);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (int i = 0; i < num_of_txn; i++) {
    delete txns[num_of_txn - i - 1];
  }
}

TEST_F(PerKeyPointLockManagerTest, FIFO) {
  // validate S, X, S lock order would be executed in FIFO order
  // txn1 acquires shared lock on k1.
  // txn2 acquires exclusive lock on k1.
  // txn3 acquires shared lock on k1.

  std::vector<PessimisticTransaction*> txns;
  std::vector<port::Thread> blockingThreads;

  // Count the total number of wait sync point calls
  std::atomic_int wait_sync_point_times = 0;
  SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&wait_sync_point_times](void* /*arg*/) { wait_sync_point_times++; });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr auto num_of_txn = 3;
  std::vector<bool> txn_lock_types = {false, true, false};
  // create 3 transactions, each of them try to acquire exclusive lock on the
  // same key
  for (int i = 0; i < num_of_txn; i++) {
    auto txn = NewTxn(txn_opt_);
    txns.push_back(txn);

    if (i == 0) {
      // txn0 acquires the lock, so the rest of the transactions would block
      ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, txn_lock_types[0]));
    } else {
      blockingThreads.emplace_back([this, txn, type = txn_lock_types[i]]() {
        ASSERT_OK(locker_->TryLock(txn, 1, "k1", env_, type));
      });
    }

    // wait for transaction i to be blocked
    while (wait_sync_point_times.load() < i) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  auto num_of_blocking_thread = num_of_txn - 1;

  auto thread_idx = 0;
  auto txn_idx = 0;

  auto unlockTxn = [&]() {
    // unlock the key in transaction.
    locker_->UnLock(txns[txn_idx++], 1, "k1", env_);
  };

  auto validateLockTakenByNextTxn = [&]() {
    // validate the thread is finished
    blockingThreads[thread_idx++].join();
  };

  auto stillWaitingForLock = [&]() {
    // validate the thread is no longer joinable
    ASSERT_TRUE(blockingThreads[thread_idx].joinable());
  };

  // unlock the key, so next group of transactions could take the lock.
  stillWaitingForLock();
  unlockTxn();

  // txn1 acquires exclusive lock on k1.
  validateLockTakenByNextTxn();

  // txn2 acquires shared lock on k1.
  stillWaitingForLock();
  unlockTxn();
  validateLockTakenByNextTxn();

  // clean up
  unlockTxn();

  ASSERT_EQ(wait_sync_point_times.load(), num_of_blocking_thread);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (int i = 0; i < num_of_txn; i++) {
    delete txns[num_of_txn - i - 1];
  }
}

TEST_P(SpotLockManagerTest, LockDownGradeWithOtherLockRequests) {
  // Test lock down grade always succeeds, even if there are other lock requests
  // waiting for the same lock.
  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutMs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  for (bool exclusive : {true, false}) {
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

    port::Thread t;
    BlockUntilWaitingTxn(wait_sync_point_name_, t, [this, &txn2, exclusive]() {
      // block because txn1 is holding a exclusive lock on k1.
      ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, exclusive));
    });

    // txn1 downgrades the lock to shared lock, so txn2 could proceed
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

    locker_->UnLock(txn1, 1, "k1", env_);
    t.join();
    locker_->UnLock(txn2, 1, "k1", env_);
  }

  // clean up
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, LockTimeout) {
  // Test lock timeout
  // txn1 acquires an exclusive lock on k1 successfully.
  // txn2 try to acquire a lock on k1, but timedout.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kShortTxnTimeoutMs;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  for (bool exclusive : {true, false}) {
    auto ret = locker_->TryLock(txn2, 1, "k1", env_, exclusive);
    ASSERT_TRUE(ret.IsTimedOut());
  }

  // clean up
  locker_->UnLock(txn1, 1, "k1", env_);
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, ExpiredLockStolenAfterTimeout) {
  // validate an expired lock can be stolen by another transaction that timed
  // out on the lock.
  // txn1 acquires an exclusive lock on k1 successfully with a short expiration
  // time.
  // txn2 try to acquire a shared lock on k1 with timeout that is slightly
  // longer than the txn1 expiration.
  // Validate txn2 will take the lock.

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.expiration = 1000;
  txn_opt.lock_timeout = 1000 * 2;
  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  port::Thread t1;
  BlockUntilWaitingTxn(wait_sync_point_name_, t1, [this, &txn2]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  t1.join();

  // clean up
  locker_->UnLock(txn2, 1, "k1", env_);
  locker_->UnLock(txn1, 1, "k1", env_);

  delete txn2;
  delete txn1;
}

// Try to block until transaction enters waiting state.
// However due to timing, it could fail, so return true if succeeded, false
// otherwise.
bool TryBlockUntilWaitingTxn(const char* sync_point_name, port::Thread& t,
                             std::function<void()> function) {
  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      sync_point_name, [&](void* /*arg*/) { reached.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // As the lifetime of the complete variable could go beyond the scope of this
  // function, so we wrap it in a shared_ptr, and copy it into the lambda
  std::shared_ptr<std::atomic<bool>> complete =
      std::make_shared<std::atomic<bool>>(false);
  t = port::Thread([complete, &function]() {
    function();
    complete->store(true);
  });

  auto ret = false;

  while (true) {
    if (complete->load()) {
      // function completed, before sync point was reached, return false
      t.join();
      ret = false;
      break;
    }
    if (reached.load()) {
      // sync point was reached before function completed, return true
      ret = true;
      break;
    }
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  return ret;
}

TEST_F(PerKeyPointLockManagerTest, LockStealAfterExpirationExclusive) {
  // There are multiple transactions waiting for the same lock.
  // txn1 acquires an exclusive lock on k1 successfully with a short expiration
  // time.
  // txn2 try to acquire an exclusive lock on k1, before expiration time,
  // so it is blocked and waits for txn1 lock expired.
  // txn3 try to acquire an exclusive lock on k1 after txn1 lock expires, FIFO
  // order is respected.
  // txn2 is woken up and takes the lock. unlock txn2, txn3 should proceed.

  txn_opt_.expiration = 1000;
  auto txn1 = NewTxn(txn_opt_);
  txn_opt_.expiration = -1;
  auto txn2 = NewTxn(txn_opt_);
  auto txn3 = NewTxn(txn_opt_);

  port::Thread t1;
  auto retry_times = 10;

  // Use a loop to reduce test flakiness.
  // that the test is flaky because the txn2 thread start could be delayed until
  // txn1 lock expired. In that case, txn2 will not enter into wait state, which
  // will defeat the test purpose. Use a loop to retry a few times, until it is
  // able to enter into wait state.
  while (retry_times--) {
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
    if (TryBlockUntilWaitingTxn(wait_sync_point_name_, t1, [this, &txn2]() {
          // block because txn1 is holding a shared lock on k1.
          ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
        })) {
      break;
    }
    // failed, retry again
    locker_->UnLock(txn1, 1, "k1", env_);
    locker_->UnLock(txn2, 1, "k1", env_);
  }
  // make sure txn2 is able to reach the wait state before proceed
  ASSERT_GT(retry_times, 0);

  // txn3 try to acquire an exclusive lock on k1, FIFO order is respected.
  port::Thread t2;
  BlockUntilWaitingTxn(wait_sync_point_name_, t2, [this, &txn3]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });

  // validate txn2 is woken up and takes the lock
  t1.join();

  // unlock txn2, txn3 should proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();

  // clean up
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, LockStealAfterExpirationShared) {
  // There are multiple transactions waiting for the same lock.
  // txn1 acquires a shared lock on k1 successfully with a short expiration
  // time.
  // txn2 try to acquire an exclusive lock on k1, before expiration time,
  // so it is blocked and waits for txn1 lock expired.
  // txn3 try to acquire a shared lock on k1 after txn1 lock expires, FIFO
  // order is respected.
  // txn2 is woken up and takes the lock. unlock txn2, txn3 should proceed.

  txn_opt_.expiration = 1000;
  auto txn1 = NewTxn(txn_opt_);
  txn_opt_.expiration = -1;
  auto txn2 = NewTxn(txn_opt_);
  auto txn3 = NewTxn(txn_opt_);

  port::Thread t1;
  auto retry_times = 10;

  // Use a loop to reduce test flakiness.
  // that the test is flaky because the txn2 thread start could be delayed until
  // txn1 lock expired. In that case, txn2 will not enter into wait state, which
  // will defeat the test purpose. Use a loop to retry a few times, until it is
  // able to enter into wait state.
  while (retry_times--) {
    ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));
    if (TryBlockUntilWaitingTxn(wait_sync_point_name_, t1, [this, &txn2]() {
          // block because txn1 is holding an exclusive lock on k1.
          ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
        })) {
      break;
    }
    // failed, retry again
    locker_->UnLock(txn1, 1, "k1", env_);
    locker_->UnLock(txn2, 1, "k1", env_);
  }
  // make sure txn2 is able to reach the wait state before proceed
  ASSERT_GT(retry_times, 0);

  // txn3 try to acquire an exclusive lock on k1, FIFO order is respected.
  port::Thread t2;
  BlockUntilWaitingTxn(wait_sync_point_name_, t2, [this, &txn3]() {
    // block because txn1 is holding an exclusive lock on k1.
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));
  });

  // validate txn2 is woken up and takes the lock
  t1.join();

  // unlock txn2, txn3 should proceed
  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();

  // clean up
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, DeadLockOnWaiter) {
  // Txn1 acquires exclusive lock on k1
  // Txn3 acquires shared lock on k2
  // Txn2 tries to acquire exclusive lock on k1, waiting in the waiter queue.
  // Txn3 tries to acquire exclusive lock on k1, waiting in the waiter queue.
  // Txn3 depends on both Txn1 and Txn2. Txn1 unlocks k1.
  // Txn2 takes the lock k1, and tries to acquire lock k2.
  // Now Txn2 depends on Txn3.
  // Deadlock is detected, and Txn2 is aborted.

  auto txn1 = NewTxn(txn_opt_);
  auto txn2 = NewTxn(txn_opt_);
  auto txn3 = NewTxn(txn_opt_);

  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
  ASSERT_OK(locker_->TryLock(txn3, 1, "k2", env_, false));

  port::Thread t1;
  BlockUntilWaitingTxn(wait_sync_point_name_, t1, [this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
    auto s = locker_->TryLock(txn2, 1, "k2", env_, true);
    ASSERT_TRUE(s.IsDeadlock());
  });

  port::Thread t2;
  BlockUntilWaitingTxn(wait_sync_point_name_, t2, [this, &txn3]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });

  locker_->UnLock(txn1, 1, "k1", env_);

  t1.join();

  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();

  // clean up
  locker_->UnLock(txn3, 1, "k1", env_);
  locker_->UnLock(txn3, 1, "k2", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, SharedLockRaceCondition) {
  // Verify a shared lock race condition is handled properly.
  // When there are waiters in the queue, and all of them are shared waiters,
  // and no one has taken the lock and all of them just got woken up and not
  // yet taken the lock yet. A new shared lock request should be granted
  // directly, without wait in the queue. If it did, It would not be woken up
  // until the last shared lock is released.

  // Disable deadlock detection timeout to prevent test flakyness.
  deadlock_timeout_us = 0;
  auto txn1 = NewTxn(txn_opt_);
  auto txn2 = NewTxn(txn_opt_);
  auto txn3 = NewTxn(txn_opt_);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"PerKeyPointLockManager::AcquireWithTimeout:AfterWokenUp",
        "PerKeyPointLockManagerTest::SharedLockRaceCondition:"
        "BeforeNewSharedLockRequest"},
       {"PerKeyPointLockManagerTest::SharedLockRaceCondition:"
        "AfterNewSharedLockRequest",
        "PerKeyPointLockManager::AcquireWithTimeout:BeforeTakeLock"}});

  std::atomic<bool> reached(false);
  SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&reached](void* /*arg*/) { reached.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  // txn1 acquires an exclusive lock on k1, so that the following shared lock
  // request would be blocked
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // txn2 try to acquire a shared lock on k1, and get blocked
  auto t1 = port::Thread([this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // unlock txn1, txn2 should be woken up, but txn2 stops on the sync point
  locker_->UnLock(txn1, 1, "k1", env_);

  // Use sync point to simulate the race condition.
  // txn3 tries to take the lock right after txn2 is woken up, but before it
  // takes the lock
  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::SharedLockRaceCondition:"
      "BeforeNewSharedLockRequest");

  // txn3 try to acquire a shared lock on k1, and get granted immediately
  ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));

  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::SharedLockRaceCondition:"
      "AfterNewSharedLockRequest");

  // validate txn2 is woken up and takes the lock
  t1.join();

  // cleanup
  locker_->UnLock(txn2, 1, "k1", env_);
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, UpgradeLockRaceCondition) {
  // Verify an upgrade lock race condition is handled properly.
  // When a key is locked in exlusive mode, shared lock waiters will be enqueued
  // as waiters.
  // When the exclusive lock holder release the lock. The shared lock waiters
  // are woken up to take the lock. At this point, when a new shared lock
  // requester comes in, it will take the lock directly without waiting or
  // queueing. This requester then immediately upgrade the lock to exclusive
  // lock. This request will be prioritized to the head of the queue.
  // Meantime, it should also depend on the shared lock waiters which are still
  // in the queue that are ready to take the lock. Later, when one of the reader
  // lock want to also upgrade its lock, it will detect a dead lock and abort.

  auto txn1 = NewTxn(txn_opt_);
  auto txn2 = NewTxn(txn_opt_);
  auto txn3 = NewTxn(txn_opt_);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"PerKeyPointLockManager::AcquireWithTimeout:AfterWokenUp",
        "PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
        "BeforeNewSharedLockRequest"},
       {"PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
        "AfterNewSharedLockRequest",
        "PerKeyPointLockManager::AcquireWithTimeout:BeforeTakeLock"}});

  std::atomic<bool> reached(false);
  SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_,
      [&reached](void* /*arg*/) { reached.store(true); });

  SyncPoint::GetInstance()->EnableProcessing();

  // txn1 acquires an exclusive lock on k1, so that the following shared lock
  // request would be blocked
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  auto t1 = port::Thread([this, &txn2]() {
    // txn2 try to acquire a shared lock on k1, and get blocked
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // unlock txn1, txn2 should be woken up, but txn2 stops on the sync point
  locker_->UnLock(txn1, 1, "k1", env_);

  // Use sync point to simulate the race condition.
  // txn3 tries to take the lock right after txn2 is woken up, but before it
  // takes the lock
  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
      "BeforeNewSharedLockRequest");

  // txn3 try to acquire a shared lock on k1, and get granted immediately
  ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));

  // txn3 try to upgrade its lock to exclusive lock and get blocked.
  reached = false;
  auto t2 = port::Thread([this, &txn3]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, true));
  });

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  TEST_SYNC_POINT(
      "PerKeyPointLockManagerTest::UpgradeLockRaceCondition:"
      "AfterNewSharedLockRequest");

  // validate txn2 is woken up and takes the shared lock
  t1.join();

  // validate txn2 would get deadlock when it try to upgrade its lock to
  // exclusive
  auto s = locker_->TryLock(txn2, 1, "k1", env_, true);
  ASSERT_TRUE(s.IsDeadlock());

  // cleanup
  locker_->UnLock(txn2, 1, "k1", env_);
  t2.join();
  locker_->UnLock(txn3, 1, "k1", env_);

  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_P(SpotLockManagerTest, Catch22) {
  // Benchmark the overhead of one transaction depends on another in a circle
  // repeatedly

  MockColumnFamilyHandle cf(1);
  locker_->AddColumnFamily(&cf);
  TransactionOptions txn_opt;
  txn_opt.deadlock_detect = true;
  txn_opt.lock_timeout = kLongTxnTimeoutMs;
  txn_opt.expiration = kLongTxnTimeoutMs;

  auto txn1 = NewTxn(txn_opt);
  auto txn2 = NewTxn(txn_opt);

  // use a wait count to count the number of times the lock is waited inside
  // transaction lock
  std::atomic_int wait_count(0);

  SyncPoint::GetInstance()->DisableProcessing();
  if (GetParam().use_per_key_point_lock_manager &&
      GetParam().deadlock_timeout_us != 0) {
    // Use special sync point when deadlock timeout is enabled, so the test run
    // faster
    SyncPoint::GetInstance()->SetCallBack(
        "PerKeyPointLockManager::AcquireWithTimeout:"
        "WaitingTxnBeforeDeadLockDetection",
        [&wait_count](void* /*arg*/) { wait_count++; });
  } else {
    // PointLockManager
    SyncPoint::GetInstance()->SetCallBack(
        wait_sync_point_name_, [&wait_count](void* /*arg*/) { wait_count++; });
  }
  SyncPoint::GetInstance()->EnableProcessing();

  // txn1 X lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  std::mutex coordinator_mutex;
  int iteration_count = 10000;

  // txn1 try to lock X lock in a loop
  auto t1 = port::Thread(
      [this, &txn1, &wait_count, &coordinator_mutex, &iteration_count]() {
        while (wait_count.load() < iteration_count) {
          // spin wait until the other thread enters the lock waiter queue.
          while (wait_count.load() % 2 == 0) {
          }
          // unlock the lock, so that the other thread can acquire the lock
          locker_->UnLock(txn1, 1, "k1", env_);
          {
            // Use the coordinator mutex to make sure the other thread has been
            // waked up and acquired the lock, before this thread try to acquire
            // the lock again.
            std::scoped_lock<std::mutex> lock(coordinator_mutex);
            ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));
          }
        }
        locker_->UnLock(txn1, 1, "k1", env_);
      });

  // txn2 try to lock X lock in a loop
  auto t2 = port::Thread(
      [this, &txn2, &wait_count, &coordinator_mutex, &iteration_count]() {
        while (wait_count.load() < iteration_count) {
          {
            // Use the coordinator mutex to make sure the other thread has been
            // waked up and acquired the lock, before this thread try to acquire
            // the lock again.
            std::scoped_lock<std::mutex> lock(coordinator_mutex);
            ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
          }
          // spin wait until the other thread enters the lock waiter queue.
          while (wait_count.load() % 2 == 1) {
          }
          // unlock the lock, so that the other thread can acquire the lock
          locker_->UnLock(txn2, 1, "k1", env_);
        }
      });

  // clean up
  t1.join();
  t2.join();

  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, LockUpgradeOrdering) {
  // When lock is upgraded, verify that it will only upgrade its lock after all
  // the shared lock that are before the first exclusive lock in the lock wait
  // queue.

  auto txn1 = NewTxn(txn_opt_);
  auto txn2 = NewTxn(txn_opt_);
  auto txn3 = NewTxn(txn_opt_);
  auto txn4 = NewTxn(txn_opt_);

  std::mutex txn4_mutex;
  std::unique_lock<std::mutex> txn4_lock(txn4_mutex);
  std::atomic_bool txn4_waked_up(false);
  std::atomic_int wait_count(0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->SetCallBack(
      wait_sync_point_name_, [&wait_count](void* /*arg*/) { wait_count++; });
  SyncPoint::GetInstance()->SetCallBack(
      "PerKeyPointLockManager::AcquireWithTimeout:AfterWokenUp",
      [&txn4, &txn4_mutex, &txn4_waked_up](void* arg) {
        auto transaction_id = *(static_cast<TransactionID*>(arg));
        if (transaction_id == txn4->GetID()) {
          txn4_waked_up.store(true);
          {
            // wait for txn4 mutex to be released, so that this thread will be
            // blocked.
            std::scoped_lock<std::mutex> lock(txn4_mutex);
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Txn1 X lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // Txn2,3,4 try S lock
  port::Thread t1([this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });
  port::Thread t2([this, &txn3]() {
    ASSERT_OK(locker_->TryLock(txn3, 1, "k1", env_, false));
  });
  port::Thread t3([this, &txn4]() {
    ASSERT_OK(locker_->TryLock(txn4, 1, "k1", env_, false));
  });

  // wait for all 3 transactions to enter wait state
  while (wait_count.load() < 3) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Txn1 unlock
  locker_->UnLock(txn1, 1, "k1", env_);

  // Txn2,3 take S lock
  t1.join();
  t2.join();

  // wait for txn4 to be woken up, otherwise txn2 will get deadlock
  while (!txn4_waked_up.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Txn2 try X lock
  std::atomic_bool txn2_exclusive_lock_acquired(false);
  port::Thread t4([this, &txn2, &txn2_exclusive_lock_acquired]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, true));
    txn2_exclusive_lock_acquired.store(true);
  });

  // wait for txn2 to enter wait state
  while (wait_count.load() < 4) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Txn3 release S lock
  locker_->UnLock(txn3, 1, "k1", env_);

  // Validate Txn2 has not acquired the lock yet
  ASSERT_FALSE(txn2_exclusive_lock_acquired.load());

  // Txn4 take S lock
  txn4_lock.unlock();
  t3.join();

  // Txn4 release S lock Txn2 upgraded to X lock Txn2
  locker_->UnLock(txn4, 1, "k1", env_);
  t4.join();
  ASSERT_TRUE(txn2_exclusive_lock_acquired.load());

  // release lock clean up
  locker_->UnLock(txn2, 1, "k1", env_);

  delete txn4;
  delete txn3;
  delete txn2;
  delete txn1;
}

TEST_F(PerKeyPointLockManagerTest, LockDownGradeRaceCondition) {
  // When a lock is downgraded, it should notify all the shared waiters in the
  // queue to take the lock.

  auto txn1 = NewTxn(txn_opt_);
  auto txn2 = NewTxn(txn_opt_);

  // Txn1 X lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, true));

  // Txn2 try S lock
  port::Thread t1;
  BlockUntilWaitingTxn(wait_sync_point_name_, t1, [this, &txn2]() {
    ASSERT_OK(locker_->TryLock(txn2, 1, "k1", env_, false));
  });

  // Txn1 downgrade to S lock
  ASSERT_OK(locker_->TryLock(txn1, 1, "k1", env_, false));

  // Txn2 take S lock
  t1.join();

  // clean up
  locker_->UnLock(txn1, 1, "k1", env_);
  locker_->UnLock(txn2, 1, "k1", env_);

  delete txn2;
  delete txn1;
}

// Run AnyLockManagerTest with PointLockManager
INSTANTIATE_TEST_CASE_P(PointLockManager, AnyLockManagerTest,
                        ::testing::Values(nullptr));

// Run AnyLockManagerTest with PerKeyPointLockManager
template <int64_t N>
void PerKeyPointLockManagerTestSetup(PointLockManagerTest* self) {
  self->init();
  self->deadlock_timeout_us = N;
  self->UsePerKeyPointLockManager();
}

INSTANTIATE_TEST_CASE_P(
    PerLockPointLockManager, AnyLockManagerTest,
    ::testing::Values(PerKeyPointLockManagerTestSetup<0>,
                      PerKeyPointLockManagerTestSetup<100>,
                      PerKeyPointLockManagerTestSetup<1000>));

// Run PointLockManagerTest with PerLockPointLockManager and PointLockManager
INSTANTIATE_TEST_CASE_P(
    PointLockCorrectnessCheckTestSuite, SpotLockManagerTest,
    ::testing::ValuesIn(std::vector<SpotLockManagerTestParam>{
        {true, 0}, {true, 100}, {true, 1000}, {false, 0}}));

// Regression test for a heap out-of-bounds write in
// DeadlockInfoBufferTempl::Resize. When the requested size equals the number of
// deadlock paths currently recorded, Resize used to leave buffer_idx_ equal to
// paths_buffer_.size() (one past the end). AddNewPath indexes
// paths_buffer_[buffer_idx_] before applying its own modulo, so the next
// recorded deadlock wrote past the end of the vector. Reachable in production
// via TransactionDB::SetDeadlockInfoBufferSize(current_recorded_count). Under
// ASAN the unpatched code aborts here with a heap-buffer-overflow.
TEST(DeadlockInfoBufferTest, ResizeToRecordedCountKeepsIndexInBounds) {
  DeadlockInfoBuffer buffer(1);
  // limit_exceeded = true makes the path non-empty so Normalize() counts it.
  buffer.AddNewPath(DeadlockPath(/*dl_time=*/1, /*limit=*/true));
  // Resize to exactly the one recorded path: the buffer is full, so the next
  // write must wrap to index 0, not land at index 1 of a one-element vector.
  buffer.Resize(1);
  buffer.AddNewPath(DeadlockPath(/*dl_time=*/2, /*limit=*/true));
  // Reaching here (and under ASAN, not aborting) means the write stayed in
  // bounds; the latest path overwrote the single slot.
  auto paths = buffer.PrepareBuffer();
  ASSERT_EQ(paths.size(), 1U);
  ASSERT_EQ(paths[0].deadlock_time, 2);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
