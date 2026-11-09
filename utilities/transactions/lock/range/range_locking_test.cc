//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef OS_WIN

#include <functional>
#include <iomanip>
#include <string>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testutil.h"
#include "utilities/transactions/lock/point/any_lock_manager_test.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

using std::string;

namespace ROCKSDB_NAMESPACE {

class RangeLockingTest : public ::testing::Test {
 public:
  TransactionDB* db;
  std::string dbname;
  Options options;

  std::shared_ptr<RangeLockManagerHandle> range_lock_mgr;
  TransactionDBOptions txn_db_options;

  RangeLockingTest() : db(nullptr) {
    options.create_if_missing = true;
    dbname = test::PerThreadDBPath("range_locking_testdb");

    EXPECT_OK(DestroyDB(dbname, options));

    range_lock_mgr.reset(NewRangeLockManager(nullptr));
    txn_db_options.lock_mgr_handle = range_lock_mgr;

    auto s = TransactionDB::Open(options, txn_db_options, dbname, &db);
    assert(s.ok());
  }

  ~RangeLockingTest() override {
    delete db;
    db = nullptr;
    // This is to skip the assert statement in FaultInjectionTestEnv. There
    // seems to be a bug in btrfs that the makes readdir return recently
    // unlink-ed files. By using the default fs we simply ignore errors resulted
    // from attempting to delete such files in DestroyDB.
    EXPECT_OK(DestroyDB(dbname, options));
  }

  PessimisticTransaction* NewTxn(
      TransactionOptions txn_opt = TransactionOptions()) {
    Transaction* txn = db->BeginTransaction(WriteOptions(), txn_opt);
    return static_cast<PessimisticTransaction*>(txn);
  }
};

// TODO: set a smaller lock wait timeout so that the test runs faster.
TEST_F(RangeLockingTest, BasicRangeLocking) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  std::string value;
  ReadOptions read_options;
  auto cf = db->DefaultColumnFamily();

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Get a range lock
  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c")));

  // Check that range Lock inhibits an overlapping range lock
  {
    auto s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("z"));
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Check that range Lock inhibits an overlapping point lock
  {
    auto s = txn1->GetForUpdate(read_options, cf, Slice("b"), &value);
    ASSERT_TRUE(s.IsTimedOut());
  }

  // Get a point lock, check that it inhibits range locks
  ASSERT_OK(txn0->Put(cf, Slice("n"), Slice("value")));
  {
    auto s = txn1->GetRangeLock(cf, Endpoint("m"), Endpoint("p"));
    ASSERT_TRUE(s.IsTimedOut());
  }

  ASSERT_OK(txn0->Commit());
  txn1->Rollback();

  delete txn0;
  delete txn1;
}

// A point-equality range lock passes [infimum(K), supremum(K)]: the same user
// key with the infimum endpoint as the left bound and the supremum endpoint as
// the right bound. The infimum endpoint must sort before the supremum endpoint
// even under a reverse comparator (the suffix marker is positional, not key
// content), otherwise the locktree's left <= right invariant is violated and
// try_acquire_lock aborts. Regression test for a crash on MyRocks equality
// lookups on a reverse ('rev:') column family.
TEST_F(RangeLockingTest, PointRangeLockWithReverseComparator) {
  delete db;
  db = nullptr;
  ASSERT_OK(DestroyDB(dbname, options));

  options.comparator = ReverseBytewiseComparator();
  range_lock_mgr.reset(NewRangeLockManager(nullptr));
  txn_db_options.lock_mgr_handle = range_lock_mgr;

  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname, &db));

  WriteOptions write_options;
  TransactionOptions txn_options;
  txn_options.lock_timeout = 50;
  auto cf = db->DefaultColumnFamily();

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);

  // [infimum("a"), supremum("a")] : same user key, infimum -> supremum.
  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a", false), Endpoint("a", true)));

  txn0->Rollback();
  delete txn0;
}

// CompareDbtEndpoints must use CompareWithoutTimestamp for range lock
// endpoints, which never contain user-defined timestamps.
TEST_F(RangeLockingTest, RangeLockWithTimestampComparator) {
  // Close the DB opened by the fixture (uses default comparator).
  delete db;
  db = nullptr;
  ASSERT_OK(DestroyDB(dbname, options));

  // Reopen with a timestamp-aware comparator.
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  range_lock_mgr.reset(NewRangeLockManager(nullptr));
  txn_db_options.lock_mgr_handle = range_lock_mgr;

  ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname, &db));

  WriteOptions write_options;
  TransactionOptions txn_options;
  txn_options.lock_timeout = 50;
  auto cf = db->DefaultColumnFamily();

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Acquire a range lock [a, c]. This calls CompareDbtEndpoints internally.
  // With the bug, debug builds abort here.
  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c")));

  // Overlapping range [b, z] should time out.
  auto s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("z"));
  ASSERT_TRUE(s.IsTimedOut());

  // Non-overlapping range [d, f] should succeed.
  ASSERT_OK(txn1->GetRangeLock(cf, Endpoint("d"), Endpoint("f")));

  txn0->Rollback();
  txn1->Rollback();
  delete txn0;
  delete txn1;
}

TEST_F(RangeLockingTest, MyRocksLikeUpdate) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  auto cf = db->DefaultColumnFamily();
  Status s;

  // Get a range lock for the range we are about to update
  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c")));

  bool try_range_lock_called = false;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "RangeTreeLockManager::TryRangeLock:enter",
      [&](void* /*arg*/) { try_range_lock_called = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // For performance reasons, the following must NOT call lock_mgr->TryLock():
  // We verify that by checking the value of try_range_lock_called.
  ASSERT_OK(txn0->Put(cf, Slice("b"), Slice("value"),
                      /*assume_tracked=*/true));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_FALSE(try_range_lock_called);

  txn0->Rollback();

  delete txn0;
}

TEST_F(RangeLockingTest, UpgradeLockAndGetConflict) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  auto cf = db->DefaultColumnFamily();
  Status s;
  std::string value;
  txn_options.lock_timeout = 10;

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Get the shared lock in txn0
  s = txn0->GetForUpdate(ReadOptions(), cf, Slice("a"), &value,
                         false /*exclusive*/);
  ASSERT_TRUE(s.IsNotFound());

  // Get the shared lock on the same key in txn1
  s = txn1->GetForUpdate(ReadOptions(), cf, Slice("a"), &value,
                         false /*exclusive*/);
  ASSERT_TRUE(s.IsNotFound());

  // Now, try getting an exclusive lock that overlaps with the above
  s = txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("b"));
  ASSERT_TRUE(s.IsTimedOut());

  txn0->Rollback();
  txn1->Rollback();

  delete txn0;
  delete txn1;
}

TEST_F(RangeLockingTest, ReacquireSharedLockKeepsLockMemory) {
  auto cf = db->DefaultColumnFamily();
  std::string value;
  Transaction* txn0 = db->BeginTransaction(WriteOptions());
  Transaction* txn1 = db->BeginTransaction(WriteOptions());
  const std::vector<std::pair<Transaction*, std::string>> shared_locks = {
      {txn0, "b"}, {txn1, "b"}, {txn0, "a"}, {txn0, "c"}, {txn1, "c"}};

  // "b" becomes the root of the lock tree, with "a" and "c" below it.
  // "a" has one owner, "b" and "c" have two.
  for (const auto& [txn, key] : shared_locks) {
    ASSERT_TRUE(
        txn->GetForUpdate(ReadOptions(), cf, key, &value, false /*exclusive*/)
            .IsNotFound());
  }
  const uint64_t lock_memory = range_lock_mgr->GetStatus().current_lock_memory;

  for (const auto& [txn, key] : shared_locks) {
    ASSERT_TRUE(
        txn->GetForUpdate(ReadOptions(), cf, key, &value, false /*exclusive*/)
            .IsNotFound());
    EXPECT_EQ(range_lock_mgr->GetStatus().current_lock_memory, lock_memory)
        << "after locking " << key << " again";
  }

  ASSERT_OK(txn0->Commit());
  ASSERT_OK(txn1->Commit());
  EXPECT_EQ(range_lock_mgr->GetStatus().current_lock_memory, 0);

  delete txn0;
  delete txn1;
}

TEST_F(RangeLockingTest, SnapshotValidation) {
  Status s;
  Slice key_slice = Slice("k");
  ColumnFamilyHandle* cfh = db->DefaultColumnFamily();

  auto txn0 = NewTxn();
  txn0->Put(key_slice, Slice("initial"));
  txn0->Commit();

  // txn1
  auto txn1 = NewTxn();
  txn1->SetSnapshot();
  std::string val1;
  ASSERT_OK(txn1->Get(ReadOptions(), cfh, key_slice, &val1));
  ASSERT_EQ(val1, "initial");
  val1 = val1 + std::string("-txn1");

  ASSERT_OK(txn1->Put(cfh, key_slice, Slice(val1)));

  // txn2
  auto txn2 = NewTxn();
  txn2->SetSnapshot();
  std::string val2;
  // This will see the original value as nothing is committed
  // This is also Get, so it is doesn't acquire any locks.
  ASSERT_OK(txn2->Get(ReadOptions(), cfh, key_slice, &val2));
  ASSERT_EQ(val2, "initial");

  // txn1
  ASSERT_OK(txn1->Commit());

  // txn2
  val2 = val2 + std::string("-txn2");
  // Now, this call should do Snapshot Validation and fail:
  s = txn2->Put(cfh, key_slice, Slice(val2));
  ASSERT_TRUE(s.IsBusy());

  ASSERT_OK(txn2->Commit());

  delete txn0;
  delete txn1;
  delete txn2;
}

TEST_F(RangeLockingTest, MultipleTrxLockStatusData) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  auto cf = db->DefaultColumnFamily();

  Transaction* txn0 = db->BeginTransaction(write_options, txn_options);
  Transaction* txn1 = db->BeginTransaction(write_options, txn_options);

  // Get a range lock
  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("z"), Endpoint("z")));
  ASSERT_OK(txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("e")));

  auto s = range_lock_mgr->GetRangeLockStatusData();
  ASSERT_EQ(s.size(), 2);
  for (auto it = s.begin(); it != s.end(); ++it) {
    ASSERT_EQ(it->first, cf->GetID());
    auto val = it->second;
    ASSERT_FALSE(val.start.inf_suffix);
    ASSERT_FALSE(val.end.inf_suffix);
    ASSERT_TRUE(val.exclusive);
    ASSERT_EQ(val.ids.size(), 1);
    if (val.ids[0] == txn0->GetID()) {
      ASSERT_EQ(val.start.slice, "z");
      ASSERT_EQ(val.end.slice, "z");
    } else if (val.ids[0] == txn1->GetID()) {
      ASSERT_EQ(val.start.slice, "b");
      ASSERT_EQ(val.end.slice, "e");
    } else {
      FAIL();  // Unknown transaction ID.
    }
  }

  delete txn0;
  delete txn1;
}

#if defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define SKIP_LOCK_ESCALATION_TEST 1
#endif
#else
#define SKIP_LOCK_ESCALATION_TEST 1
#endif

#ifndef SKIP_LOCK_ESCALATION_TEST
TEST_F(RangeLockingTest, BasicLockEscalation) {
  auto cf = db->DefaultColumnFamily();

  auto counters = range_lock_mgr->GetStatus();

  // Initially not using any lock memory
  ASSERT_EQ(counters.current_lock_memory, 0);
  ASSERT_EQ(counters.escalation_count, 0);

  ASSERT_EQ(0, range_lock_mgr->SetMaxLockMemory(2000));

  // Insert until we see lock escalations
  auto txn = NewTxn();

  // Get the locks until we hit an escalation
  for (int i = 0; i < 2020; i++) {
    std::ostringstream buf;
    buf << std::setw(8) << std::setfill('0') << i;
    std::string buf_str = buf.str();
    ASSERT_OK(txn->GetRangeLock(cf, Endpoint(buf_str), Endpoint(buf_str)));
  }
  counters = range_lock_mgr->GetStatus();
  ASSERT_GT(counters.escalation_count, 0);
  ASSERT_LE(counters.current_lock_memory, 2000);

  delete txn;
}

TEST_F(RangeLockingTest, LockEscalationReleasesSharedLockMemory) {
  auto cf = db->DefaultColumnFamily();
  std::string value;
  Transaction* txn0 = db->BeginTransaction(WriteOptions());
  Transaction* txn1 = db->BeginTransaction(WriteOptions());

  // Both transactions share the lock on "a". Escalation can merge the
  // exclusive locks of txn0 on "b" and "c".
  for (Transaction* txn : {txn0, txn1}) {
    ASSERT_TRUE(
        txn->GetForUpdate(ReadOptions(), cf, "a", &value, false /*exclusive*/)
            .IsNotFound());
  }
  ASSERT_OK(txn0->Put(cf, "b", "v"));
  ASSERT_OK(txn0->Put(cf, "c", "v"));

  // At the limit, the next lock request escalates first, and the merge frees
  // enough memory for the request.
  auto counters = range_lock_mgr->GetStatus();
  ASSERT_EQ(0, range_lock_mgr->SetMaxLockMemory(counters.current_lock_memory));
  EXPECT_OK(txn1->Put(cf, "d", "v"));
  EXPECT_EQ(range_lock_mgr->GetStatus().escalation_count,
            counters.escalation_count + 1);

  ASSERT_OK(txn0->Commit());
  ASSERT_OK(txn1->Commit());
  EXPECT_EQ(range_lock_mgr->GetStatus().current_lock_memory, 0);

  delete txn0;
  delete txn1;
}

// An escalation barrier function. Allow escalation iff the first two bytes are
// identical.
static bool escalation_barrier(const Endpoint& a, const Endpoint& b) {
  assert(a.slice.size() > 2);
  assert(b.slice.size() > 2);
  if (memcmp(a.slice.data(), b.slice.data(), 2)) {
    return true;  // This is a barrier
  } else {
    return false;  // No barrier
  }
}

TEST_F(RangeLockingTest, LockEscalationBarrier) {
  auto cf = db->DefaultColumnFamily();

  auto counters = range_lock_mgr->GetStatus();

  // Initially not using any lock memory
  ASSERT_EQ(counters.escalation_count, 0);

  range_lock_mgr->SetMaxLockMemory(8000);
  range_lock_mgr->SetEscalationBarrierFunc(escalation_barrier);

  // Insert enough locks to cause lock escalations to happen
  auto txn = NewTxn();
  const int N = 2000;
  for (int i = 0; i < N; i++) {
    std::ostringstream buf;
    buf << std::setw(4) << std::setfill('0') << i;
    std::string buf_str = buf.str();
    ASSERT_OK(txn->GetRangeLock(cf, Endpoint(buf_str), Endpoint(buf_str)));
  }
  counters = range_lock_mgr->GetStatus();
  ASSERT_GT(counters.escalation_count, 0);

  // Check that lock escalation was not performed across escalation barriers:
  // Use another txn to acquire locks near the barriers.
  auto txn2 = NewTxn();
  range_lock_mgr->SetMaxLockMemory(500000);
  for (int i = 100; i < N; i += 100) {
    std::ostringstream buf;
    buf << std::setw(4) << std::setfill('0') << i - 1 << "-a";
    std::string buf_str = buf.str();
    // Check that we CAN get a lock near the escalation barrier
    ASSERT_OK(txn2->GetRangeLock(cf, Endpoint(buf_str), Endpoint(buf_str)));
  }

  txn->Rollback();
  txn2->Rollback();
  delete txn;
  delete txn2;
}

#endif

TEST_F(RangeLockingTest, LockWaitCount) {
  TransactionOptions txn_options;
  auto cf = db->DefaultColumnFamily();
  txn_options.lock_timeout = 50;
  Transaction* txn0 = db->BeginTransaction(WriteOptions(), txn_options);
  Transaction* txn1 = db->BeginTransaction(WriteOptions(), txn_options);

  // Get a range lock
  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c")));

  uint64_t lock_waits1 = range_lock_mgr->GetStatus().lock_wait_count;
  // Attempt to get a conflicting lock
  auto s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("z"));
  ASSERT_TRUE(s.IsTimedOut());

  // Check that the counter was incremented
  uint64_t lock_waits2 = range_lock_mgr->GetStatus().lock_wait_count;
  ASSERT_EQ(lock_waits1 + 1, lock_waits2);

  txn0->Rollback();
  txn1->Rollback();

  delete txn0;
  delete txn1;
}

TEST_F(RangeLockingTest, LockWaiteeAccess) {
  TransactionOptions txn_options;
  auto cf = db->DefaultColumnFamily();
  txn_options.lock_timeout = 60;
  Transaction* txn0 = db->BeginTransaction(WriteOptions(), txn_options);
  Transaction* txn1 = db->BeginTransaction(WriteOptions(), txn_options);

  // Get a range lock
  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c")));

  std::atomic<bool> reached(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "RangeTreeLockManager::TryRangeLock:EnterWaitingTxn", [&](void* /*arg*/) {
        reached.store(true);
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread t([&]() {
    // Attempt to get a conflicting lock
    auto s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("z"));
    ASSERT_TRUE(s.ok());
    txn1->Rollback();
  });

  while (!reached.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Release locks and free the transaction
  txn0->Rollback();
  delete txn0;

  t.join();

  delete txn1;
}

// A killed_callback that fires makes a conflicting range-lock wait return
// Status::Aborted() (DB_LOCK_INTERRUPTED), not a timeout.
TEST_F(RangeLockingTest, KilledCallbackAbortsWait) {
  TransactionOptions txn_options;
  auto cf = db->DefaultColumnFamily();
  // Large timeout so a timeout cannot masquerade as the interrupt.
  txn_options.lock_timeout = 100000;

  std::atomic<bool> killed{false};
  range_lock_mgr->SetIsKilledCallback([&]() { return killed.load(); });

  Transaction* txn0 = db->BeginTransaction(WriteOptions(), txn_options);
  Transaction* txn1 = db->BeginTransaction(WriteOptions(), txn_options);

  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c")));

  killed.store(true);
  auto s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("z"));
  ASSERT_TRUE(s.IsAborted());
  ASSERT_FALSE(s.IsTimedOut());

  txn0->Rollback();
  txn1->Rollback();
  delete txn0;
  delete txn1;
}

// With a killed_callback installed that never fires, a conflicting wait still
// times out: the interrupt path must not disturb timeout behavior.
TEST_F(RangeLockingTest, KilledCallbackFalseStillTimesOut) {
  TransactionOptions txn_options;
  auto cf = db->DefaultColumnFamily();
  txn_options.lock_timeout = 50;

  range_lock_mgr->SetIsKilledCallback([]() { return false; });

  Transaction* txn0 = db->BeginTransaction(WriteOptions(), txn_options);
  Transaction* txn1 = db->BeginTransaction(WriteOptions(), txn_options);

  ASSERT_OK(txn0->GetRangeLock(cf, Endpoint("a"), Endpoint("c")));

  auto s = txn1->GetRangeLock(cf, Endpoint("b"), Endpoint("z"));
  ASSERT_TRUE(s.IsTimedOut());
  ASSERT_FALSE(s.IsAborted());

  txn0->Rollback();
  txn1->Rollback();
  delete txn0;
  delete txn1;
}

void PointLockManagerTestExternalSetup(PointLockManagerTest* self) {
  self->env_ = Env::Default();
  self->db_dir_ = test::PerThreadDBPath("point_lock_manager_test");
  ASSERT_OK(self->env_->CreateDir(self->db_dir_));

  Options opt;
  opt.create_if_missing = true;
  TransactionDBOptions txn_opt;
  txn_opt.transaction_lock_timeout = 0;

  auto mutex_factory = std::make_shared<TransactionDBMutexFactoryImpl>();
  self->locker_.reset(NewRangeLockManager(mutex_factory)->getLockManager());
  std::shared_ptr<RangeLockManagerHandle> range_lock_mgr =
      std::dynamic_pointer_cast<RangeLockManagerHandle>(self->locker_);
  txn_opt.lock_mgr_handle = range_lock_mgr;

  ASSERT_OK(TransactionDB::Open(opt, txn_opt, self->db_dir_, &self->db_));
  self->wait_sync_point_name_ = "RangeTreeLockManager::TryRangeLock:WaitingTxn";
}

INSTANTIATE_TEST_CASE_P(RangeLockManager, AnyLockManagerTest,
                        ::testing::Values(PointLockManagerTestExternalSetup));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // OS_WIN

#include <stdio.h>
int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "skipped as Range Locking is not supported on Windows\n");
  return 0;
}

#endif  // OS_WIN
