//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/transaction_test_util.h"
#include "util/crc32c.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class OptimisticTransactionTest
    : public testing::Test,
      public testing::WithParamInterface<OccValidationPolicy> {
 public:
  std::unique_ptr<OptimisticTransactionDB> txn_db;
  std::string dbname;
  Options options;
  OptimisticTransactionDBOptions occ_opts;

  OptimisticTransactionTest() {
    options.create_if_missing = true;
    options.max_write_buffer_number = 2;
    options.max_write_buffer_size_to_maintain = 2 * Arena::kInlineSize;
    options.merge_operator = std::make_shared<TestPutOperator>();
    occ_opts.validate_policy = GetParam();
    dbname = test::PerThreadDBPath("optimistic_transaction_testdb");

    EXPECT_OK(DestroyDB(dbname, options));
    Open();
  }
  ~OptimisticTransactionTest() override {
    EXPECT_OK(txn_db->Close());
    txn_db.reset();
    EXPECT_OK(DestroyDB(dbname, options));
  }

  void Reopen() {
    txn_db.reset();
    Open();
  }

  static void OpenImpl(const Options& options,
                       const OptimisticTransactionDBOptions& occ_opts,
                       const std::string& dbname,
                       std::unique_ptr<OptimisticTransactionDB>* txn_db) {
    ColumnFamilyOptions cf_options(options);
    std::vector<ColumnFamilyDescriptor> column_families;
    std::vector<ColumnFamilyHandle*> handles;
    column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
    OptimisticTransactionDB* raw_txn_db = nullptr;
    Status s = OptimisticTransactionDB::Open(
        options, occ_opts, dbname, column_families, &handles, &raw_txn_db);
    ASSERT_OK(s);
    ASSERT_NE(raw_txn_db, nullptr);
    txn_db->reset(raw_txn_db);
    ASSERT_EQ(handles.size(), 1);
    delete handles[0];
  }

 private:
  void Open() { OpenImpl(options, occ_opts, dbname, &txn_db); }
};

TEST_P(OptimisticTransactionTest, SuccessTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_OK(txn->Put(Slice("foo"), Slice("bar2")));

  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_P(OptimisticTransactionTest, WriteConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, "foo", "bar"));
  ASSERT_OK(txn_db->Put(write_options, "foo2", "bar"));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_OK(txn->Put("foo", "bar2"));

  // This Put outside of a transaction will conflict with the previous write
  ASSERT_OK(txn_db->Put(write_options, "foo", "barz"));

  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "barz");
  ASSERT_EQ(1, txn->GetNumKeys());

  Status s = txn->Commit();
  ASSERT_TRUE(s.IsBusy());  // Txn should not commit

  // Verify that transaction did not write anything
  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "barz");
  ASSERT_OK(txn_db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar");

  delete txn;
}

TEST_P(OptimisticTransactionTest, WriteConflictTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, "foo", "bar"));
  ASSERT_OK(txn_db->Put(write_options, "foo2", "bar"));

  txn_options.set_snapshot = true;
  Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_NE(txn, nullptr);

  // This Put outside of a transaction will conflict with a later write
  ASSERT_OK(txn_db->Put(write_options, "foo", "barz"));

  ASSERT_OK(txn->Put(
      "foo", "bar2"));  // Conflicts with write done after snapshot taken

  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "barz");

  Status s = txn->Commit();
  ASSERT_TRUE(s.IsBusy());  // Txn should not commit

  // Verify that transaction did not write anything
  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "barz");
  ASSERT_OK(txn_db->Get(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar");

  delete txn;
}

TEST_P(OptimisticTransactionTest, WriteConflictTest3) {
  ASSERT_OK(txn_db->Put(WriteOptions(), "foo", "bar"));

  Transaction* txn = txn_db->BeginTransaction(WriteOptions());
  ASSERT_NE(txn, nullptr);

  std::string value;
  ASSERT_OK(txn->GetForUpdate(ReadOptions(), "foo", &value));
  ASSERT_EQ(value, "bar");
  ASSERT_OK(txn->Merge("foo", "bar3"));

  // Merge outside of a transaction should conflict with the previous merge
  ASSERT_OK(txn_db->Merge(WriteOptions(), "foo", "bar2"));
  ASSERT_OK(txn_db->Get(ReadOptions(), "foo", &value));
  ASSERT_EQ(value, "bar2");

  ASSERT_EQ(1, txn->GetNumKeys());

  Status s = txn->Commit();
  EXPECT_TRUE(s.IsBusy());  // Txn should not commit

  // Verify that transaction did not write anything
  ASSERT_OK(txn_db->Get(ReadOptions(), "foo", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_P(OptimisticTransactionTest, WriteConflict4) {
  ASSERT_OK(txn_db->Put(WriteOptions(), "foo", "bar"));

  Transaction* txn = txn_db->BeginTransaction(WriteOptions());
  ASSERT_NE(txn, nullptr);

  std::string value;
  ASSERT_OK(txn->GetForUpdate(ReadOptions(), "foo", &value));
  ASSERT_EQ(value, "bar");
  ASSERT_OK(txn->Merge("foo", "bar3"));

  // Range delete outside of a transaction should conflict with the previous
  // merge inside txn
  auto* dbimpl = static_cast_with_check<DBImpl>(txn_db->GetRootDB());
  ColumnFamilyHandle* default_cf = dbimpl->DefaultColumnFamily();
  ASSERT_OK(dbimpl->DeleteRange(WriteOptions(), default_cf, "foo", "foo1"));
  Status s = txn_db->Get(ReadOptions(), "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_EQ(1, txn->GetNumKeys());

  s = txn->Commit();
  EXPECT_TRUE(s.IsBusy());  // Txn should not commit

  // Verify that transaction did not write anything
  s = txn_db->Get(ReadOptions(), "foo", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

TEST_P(OptimisticTransactionTest, ReadConflictTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions snapshot_read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, "foo", "bar"));
  ASSERT_OK(txn_db->Put(write_options, "foo2", "bar"));

  txn_options.set_snapshot = true;
  Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_NE(txn, nullptr);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  // This Put outside of a transaction will conflict with the previous read
  ASSERT_OK(txn_db->Put(write_options, "foo", "barz"));

  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "barz");

  Status s = txn->Commit();
  ASSERT_TRUE(s.IsBusy());  // Txn should not commit

  // Verify that transaction did not write anything
  ASSERT_OK(txn->GetForUpdate(read_options, "foo", &value));
  ASSERT_EQ(value, "barz");
  ASSERT_OK(txn->GetForUpdate(read_options, "foo2", &value));
  ASSERT_EQ(value, "bar");

  delete txn;
}

TEST_P(OptimisticTransactionTest, TxnOnlyTest) {
  // Test to make sure transactions work when there are no other writes in an
  // empty db.

  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_OK(txn->Put("x", "y"));

  ASSERT_OK(txn->Commit());

  delete txn;
}

TEST_P(OptimisticTransactionTest, FlushTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions snapshot_read_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  snapshot_read_options.snapshot = txn->GetSnapshot();

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_OK(txn->Put(Slice("foo"), Slice("bar2")));

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  // Put a random key so we have a memtable to flush
  ASSERT_OK(txn_db->Put(write_options, "dummy", "dummy"));

  // force a memtable flush
  FlushOptions flush_ops;
  ASSERT_OK(txn_db->Flush(flush_ops));

  // txn should commit since the flushed table is still in MemtableList History
  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}

namespace {
void FlushTest2PopulateTxn(Transaction* txn) {
  ReadOptions snapshot_read_options;
  std::string value;

  snapshot_read_options.snapshot = txn->GetSnapshot();

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  ASSERT_OK(txn->Put(Slice("foo"), Slice("bar2")));

  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");
}
}  // namespace

TEST_P(OptimisticTransactionTest, FlushTest2) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
  ASSERT_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  FlushTest2PopulateTxn(txn);

  // Put a random key so we have a MemTable to flush
  ASSERT_OK(txn_db->Put(write_options, "dummy", "dummy"));

  // force a memtable flush
  FlushOptions flush_ops;
  ASSERT_OK(txn_db->Flush(flush_ops));

  // Put a random key so we have a MemTable to flush
  ASSERT_OK(txn_db->Put(write_options, "dummy", "dummy2"));

  // force a memtable flush
  ASSERT_OK(txn_db->Flush(flush_ops));

  ASSERT_OK(txn_db->Put(write_options, "dummy", "dummy3"));

  // force a memtable flush
  // Since our test db has max_write_buffer_number=2, this flush will cause
  // the first memtable to get purged from the MemtableList history.
  ASSERT_OK(txn_db->Flush(flush_ops));

  Status s = txn->Commit();
  // txn should not commit since MemTableList History is not large enough
  ASSERT_TRUE(s.IsTryAgain());

  // simply trying Commit again doesn't help
  s = txn->Commit();
  ASSERT_TRUE(s.IsTryAgain());

  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");

  // But rolling back and redoing does
  ASSERT_OK(txn->Rollback());

  FlushTest2PopulateTxn(txn);

  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}

// Trigger the condition where some old memtables are skipped when doing
// TransactionUtil::CheckKey(), and make sure the result is still correct.
TEST_P(OptimisticTransactionTest, CheckKeySkipOldMemtable) {
  const int kAttemptHistoryMemtable = 0;
  const int kAttemptImmMemTable = 1;
  for (int attempt = kAttemptHistoryMemtable; attempt <= kAttemptImmMemTable;
       attempt++) {
    Reopen();

    WriteOptions write_options;
    ReadOptions read_options;
    ReadOptions snapshot_read_options;
    ReadOptions snapshot_read_options2;
    std::string value;

    ASSERT_OK(txn_db->Put(write_options, Slice("foo"), Slice("bar")));
    ASSERT_OK(txn_db->Put(write_options, Slice("foo2"), Slice("bar")));

    Transaction* txn = txn_db->BeginTransaction(write_options);
    ASSERT_TRUE(txn != nullptr);

    Transaction* txn2 = txn_db->BeginTransaction(write_options);
    ASSERT_TRUE(txn2 != nullptr);

    snapshot_read_options.snapshot = txn->GetSnapshot();
    ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "foo", &value));
    ASSERT_EQ(value, "bar");
    ASSERT_OK(txn->Put(Slice("foo"), Slice("bar2")));

    snapshot_read_options2.snapshot = txn2->GetSnapshot();
    ASSERT_OK(txn2->GetForUpdate(snapshot_read_options2, "foo2", &value));
    ASSERT_EQ(value, "bar");
    ASSERT_OK(txn2->Put(Slice("foo2"), Slice("bar2")));

    // txn updates "foo" and txn2 updates "foo2", and now a write is
    // issued for "foo", which conflicts with txn but not txn2
    ASSERT_OK(txn_db->Put(write_options, "foo", "bar"));

    if (attempt == kAttemptImmMemTable) {
      // For the second attempt, hold flush from beginning. The memtable
      // will be switched to immutable after calling TEST_SwitchMemtable()
      // while CheckKey() is called.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"OptimisticTransactionTest.CheckKeySkipOldMemtable",
            "FlushJob::Start"}});
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    }

    // force a memtable flush. The memtable should still be kept
    FlushOptions flush_ops;
    if (attempt == kAttemptHistoryMemtable) {
      ASSERT_OK(txn_db->Flush(flush_ops));
    } else {
      ASSERT_EQ(attempt, kAttemptImmMemTable);
      DBImpl* db_impl = static_cast<DBImpl*>(txn_db->GetRootDB());
      ASSERT_OK(db_impl->TEST_SwitchMemtable());
    }
    uint64_t num_imm_mems;
    ASSERT_TRUE(txn_db->GetIntProperty(DB::Properties::kNumImmutableMemTable,
                                       &num_imm_mems));
    if (attempt == kAttemptHistoryMemtable) {
      ASSERT_EQ(0, num_imm_mems);
    } else {
      ASSERT_EQ(attempt, kAttemptImmMemTable);
      ASSERT_EQ(1, num_imm_mems);
    }

    // Put something in active memtable
    ASSERT_OK(txn_db->Put(write_options, Slice("foo3"), Slice("bar")));

    // Create txn3 after flushing, when this transaction is commited,
    // only need to check the active memtable
    Transaction* txn3 = txn_db->BeginTransaction(write_options);
    ASSERT_TRUE(txn3 != nullptr);

    // Commit both of txn and txn2. txn will conflict but txn2 will
    // pass. In both ways, both memtables are queried.
    SetPerfLevel(PerfLevel::kEnableCount);

    get_perf_context()->Reset();
    Status s = txn->Commit();
    // We should have checked two memtables
    ASSERT_EQ(2, get_perf_context()->get_from_memtable_count);
    // txn should fail because of conflict, even if the memtable
    // has flushed, because it is still preserved in history.
    ASSERT_TRUE(s.IsBusy());

    get_perf_context()->Reset();
    s = txn2->Commit();
    // We should have checked two memtables
    ASSERT_EQ(2, get_perf_context()->get_from_memtable_count);
    ASSERT_TRUE(s.ok());

    ASSERT_OK(txn3->Put(Slice("foo2"), Slice("bar2")));
    get_perf_context()->Reset();
    s = txn3->Commit();
    // txn3 is created after the active memtable is created, so that is the only
    // memtable to check.
    ASSERT_EQ(1, get_perf_context()->get_from_memtable_count);
    ASSERT_TRUE(s.ok());

    TEST_SYNC_POINT("OptimisticTransactionTest.CheckKeySkipOldMemtable");
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

    SetPerfLevel(PerfLevel::kDisable);

    delete txn;
    delete txn2;
    delete txn3;
  }
}

TEST_P(OptimisticTransactionTest, NoSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, "AAA", "bar"));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  // Modify key after transaction start
  ASSERT_OK(txn_db->Put(write_options, "AAA", "bar1"));

  // Read and write without a snapshot
  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar1");
  ASSERT_OK(txn->Put("AAA", "bar2"));

  // Should commit since read/write was done after data changed
  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar2");

  delete txn;
}

TEST_P(OptimisticTransactionTest, MultipleSnapshotTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions snapshot_read_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, "AAA", "bar"));
  ASSERT_OK(txn_db->Put(write_options, "BBB", "bar"));
  ASSERT_OK(txn_db->Put(write_options, "CCC", "bar"));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_OK(txn_db->Put(write_options, "AAA", "bar1"));

  // Read and write without a snapshot
  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar1");
  ASSERT_OK(txn->Put("AAA", "bar2"));

  // Modify BBB before snapshot is taken
  ASSERT_OK(txn_db->Put(write_options, "BBB", "bar1"));

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "BBB", &value));
  ASSERT_EQ(value, "bar1");
  ASSERT_OK(txn->Put("BBB", "bar2"));

  ASSERT_OK(txn_db->Put(write_options, "CCC", "bar1"));

  // Set a new snapshot
  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  // Read and write with snapshot
  ASSERT_OK(txn->GetForUpdate(snapshot_read_options, "CCC", &value));
  ASSERT_EQ(value, "bar1");
  ASSERT_OK(txn->Put("CCC", "bar2"));

  ASSERT_OK(txn->GetForUpdate(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar2");
  ASSERT_OK(txn->GetForUpdate(read_options, "BBB", &value));
  ASSERT_EQ(value, "bar2");
  ASSERT_OK(txn->GetForUpdate(read_options, "CCC", &value));
  ASSERT_EQ(value, "bar2");

  ASSERT_OK(txn_db->Get(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar1");
  ASSERT_OK(txn_db->Get(read_options, "BBB", &value));
  ASSERT_EQ(value, "bar1");
  ASSERT_OK(txn_db->Get(read_options, "CCC", &value));
  ASSERT_EQ(value, "bar1");

  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Get(read_options, "AAA", &value));
  ASSERT_EQ(value, "bar2");
  ASSERT_OK(txn_db->Get(read_options, "BBB", &value));
  ASSERT_EQ(value, "bar2");
  ASSERT_OK(txn_db->Get(read_options, "CCC", &value));
  ASSERT_EQ(value, "bar2");

  // verify that we track multiple writes to the same key at different snapshots
  delete txn;
  txn = txn_db->BeginTransaction(write_options);

  // Potentially conflicting writes
  ASSERT_OK(txn_db->Put(write_options, "ZZZ", "zzz"));
  ASSERT_OK(txn_db->Put(write_options, "XXX", "xxx"));

  txn->SetSnapshot();

  OptimisticTransactionOptions txn_options;
  txn_options.set_snapshot = true;
  Transaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  txn2->SetSnapshot();

  // This should not conflict in txn since the snapshot is later than the
  // previous write (spoiler alert:  it will later conflict with txn2).
  ASSERT_OK(txn->Put("ZZZ", "zzzz"));
  ASSERT_OK(txn->Commit());

  delete txn;

  // This will conflict since the snapshot is earlier than another write to ZZZ
  ASSERT_OK(txn2->Put("ZZZ", "xxxxx"));

  Status s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn2;
}

TEST_P(OptimisticTransactionTest, ColumnFamiliesTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions snapshot_read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;

  ColumnFamilyHandle* cfa;
  ColumnFamilyHandle* cfb;
  ColumnFamilyOptions cf_options;

  // Create 2 new column families
  ASSERT_OK(txn_db->CreateColumnFamily(cf_options, "CFA", &cfa));
  ASSERT_OK(txn_db->CreateColumnFamily(cf_options, "CFB", &cfb));

  delete cfa;
  delete cfb;
  txn_db.reset();

  OptimisticTransactionDBOptions my_occ_opts = occ_opts;
  const size_t bucket_count = 500;
  my_occ_opts.shared_lock_buckets = MakeSharedOccLockBuckets(bucket_count);

  // open DB with three column families
  std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  // open the new column families
  column_families.emplace_back("CFA", ColumnFamilyOptions());
  column_families.emplace_back("CFB", ColumnFamilyOptions());
  std::vector<ColumnFamilyHandle*> handles;
  OptimisticTransactionDB* raw_txn_db = nullptr;
  ASSERT_OK(OptimisticTransactionDB::Open(
      options, my_occ_opts, dbname, column_families, &handles, &raw_txn_db));
  ASSERT_NE(raw_txn_db, nullptr);
  txn_db.reset(raw_txn_db);

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  txn->SetSnapshot();
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn_options.set_snapshot = true;
  Transaction* txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_TRUE(txn2);

  // Write some data to the db
  WriteBatch batch;
  ASSERT_OK(batch.Put("foo", "foo"));
  ASSERT_OK(batch.Put(handles[1], "AAA", "bar"));
  ASSERT_OK(batch.Put(handles[1], "AAAZZZ", "bar"));
  ASSERT_OK(txn_db->Write(write_options, &batch));
  ASSERT_OK(txn_db->Delete(write_options, handles[1], "AAAZZZ"));

  // These keys do no conflict with existing writes since they're in
  // different column families
  ASSERT_OK(txn->Delete("AAA"));
  Status s =
      txn->GetForUpdate(snapshot_read_options, handles[1], "foo", &value);
  ASSERT_TRUE(s.IsNotFound());
  Slice key_slice("AAAZZZ");
  Slice value_slices[2] = {Slice("bar"), Slice("bar")};
  ASSERT_OK(txn->Put(handles[2], SliceParts(&key_slice, 1),
                     SliceParts(value_slices, 2)));

  ASSERT_EQ(3, txn->GetNumKeys());

  // Txn should commit
  ASSERT_OK(txn->Commit());
  s = txn_db->Get(read_options, "AAA", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn_db->Get(read_options, handles[2], "AAAZZZ", &value);
  ASSERT_OK(s);
  ASSERT_EQ(value, "barbar");

  Slice key_slices[3] = {Slice("AAA"), Slice("ZZ"), Slice("Z")};
  Slice value_slice("barbarbar");
  // This write will cause a conflict with the earlier batch write
  ASSERT_OK(txn2->Put(handles[1], SliceParts(key_slices, 3),
                      SliceParts(&value_slice, 1)));

  ASSERT_OK(txn2->Delete(handles[2], "XXX"));
  ASSERT_OK(txn2->Delete(handles[1], "XXX"));
  s = txn2->GetForUpdate(snapshot_read_options, handles[1], "AAA", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Verify txn did not commit
  s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());
  s = txn_db->Get(read_options, handles[1], "AAAZZZ", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  delete txn2;

  // ** MultiGet **
  txn = txn_db->BeginTransaction(write_options, txn_options);
  snapshot_read_options.snapshot = txn->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_NE(txn, nullptr);

  std::vector<ColumnFamilyHandle*> multiget_cfh = {handles[1], handles[2],
                                                   handles[0], handles[2]};
  std::vector<Slice> multiget_keys = {"AAA", "AAAZZZ", "foo", "foo"};
  std::vector<std::string> values(4);

  std::vector<Status> results = txn->MultiGetForUpdate(
      snapshot_read_options, multiget_cfh, multiget_keys, &values);
  ASSERT_OK(results[0]);
  ASSERT_OK(results[1]);
  ASSERT_OK(results[2]);
  ASSERT_TRUE(results[3].IsNotFound());
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "barbar");
  ASSERT_EQ(values[2], "foo");

  ASSERT_OK(txn->Delete(handles[2], "ZZZ"));
  ASSERT_OK(txn->Put(handles[2], "ZZZ", "YYY"));
  ASSERT_OK(txn->Put(handles[2], "ZZZ", "YYYY"));
  ASSERT_OK(txn->Delete(handles[2], "ZZZ"));
  ASSERT_OK(txn->Put(handles[2], "AAAZZZ", "barbarbar"));

  ASSERT_EQ(5, txn->GetNumKeys());

  // Txn should commit
  ASSERT_OK(txn->Commit());
  s = txn_db->Get(read_options, handles[2], "ZZZ", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Put a key which will conflict with the next txn using the previous snapshot
  ASSERT_OK(txn_db->Put(write_options, handles[2], "foo", "000"));

  results = txn2->MultiGetForUpdate(snapshot_read_options, multiget_cfh,
                                    multiget_keys, &values);
  ASSERT_OK(results[0]);
  ASSERT_OK(results[1]);
  ASSERT_OK(results[2]);
  ASSERT_TRUE(results[3].IsNotFound());
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "barbar");
  ASSERT_EQ(values[2], "foo");

  // Verify Txn Did not Commit
  s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn;
  delete txn2;

  // ** Test independence and/or sharing of lock buckets across CFs and DBs **
  if (my_occ_opts.validate_policy == OccValidationPolicy::kValidateParallel) {
    struct SeenStat {
      uint64_t rolling_hash = 0;
      uintptr_t min = 0;
      uintptr_t max = 0;
    };
    SeenStat cur_seen;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "OptimisticTransaction::CommitWithParallelValidate::lock_bucket_ptr",
        [&](void* arg) {
          // Hash the pointer
          cur_seen.rolling_hash = Hash64(reinterpret_cast<char*>(&arg),
                                         sizeof(arg), cur_seen.rolling_hash);
          uintptr_t val = reinterpret_cast<uintptr_t>(arg);
          if (cur_seen.min == 0 || val < cur_seen.min) {
            cur_seen.min = val;
          }
          if (cur_seen.max == 0 || val > cur_seen.max) {
            cur_seen.max = val;
          }
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    // Another db sharing lock buckets
    auto shared_dbname =
        test::PerThreadDBPath("optimistic_transaction_testdb_shared");
    std::unique_ptr<OptimisticTransactionDB> shared_txn_db = nullptr;
    OpenImpl(options, my_occ_opts, shared_dbname, &shared_txn_db);

    // Another db not sharing lock buckets
    auto nonshared_dbname =
        test::PerThreadDBPath("optimistic_transaction_testdb_nonshared");
    std::unique_ptr<OptimisticTransactionDB> nonshared_txn_db = nullptr;
    my_occ_opts.occ_lock_buckets = bucket_count;
    my_occ_opts.shared_lock_buckets = nullptr;
    OpenImpl(options, my_occ_opts, nonshared_dbname, &nonshared_txn_db);

    // Plenty of keys to avoid randomly hitting the same hash sequence
    std::array<std::string, 30> keys;
    for (size_t i = 0; i < keys.size(); ++i) {
      keys[i] = std::to_string(i);
    }

    // Get a baseline pattern of bucket accesses
    cur_seen = {};
    txn = txn_db->BeginTransaction(write_options, txn_options);
    for (const auto& key : keys) {
      ASSERT_OK(txn->Put(handles[0], key, "blah"));
    }
    ASSERT_OK(txn->Commit());
    // Sufficiently large hash coverage of the space
    const uintptr_t min_span_bytes = sizeof(port::Mutex) * bucket_count / 2;
    ASSERT_GT(cur_seen.max - cur_seen.min, min_span_bytes);
    // Save
    SeenStat base_seen = cur_seen;

    // Verify it is repeatable
    cur_seen = {};
    txn = txn_db->BeginTransaction(write_options, txn_options, txn);
    for (const auto& key : keys) {
      ASSERT_OK(txn->Put(handles[0], key, "moo"));
    }
    ASSERT_OK(txn->Commit());
    ASSERT_EQ(cur_seen.rolling_hash, base_seen.rolling_hash);
    ASSERT_EQ(cur_seen.min, base_seen.min);
    ASSERT_EQ(cur_seen.max, base_seen.max);

    // Try another CF
    cur_seen = {};
    txn = txn_db->BeginTransaction(write_options, txn_options, txn);
    for (const auto& key : keys) {
      ASSERT_OK(txn->Put(handles[1], key, "blah"));
    }
    ASSERT_OK(txn->Commit());
    // Different access pattern (different hash seed)
    ASSERT_NE(cur_seen.rolling_hash, base_seen.rolling_hash);
    // Same pointer space
    ASSERT_LT(cur_seen.min, base_seen.max);
    ASSERT_GT(cur_seen.max, base_seen.min);
    // Sufficiently large hash coverage of the space
    ASSERT_GT(cur_seen.max - cur_seen.min, min_span_bytes);
    // Save
    SeenStat cf1_seen = cur_seen;

    // And another CF
    cur_seen = {};
    txn = txn_db->BeginTransaction(write_options, txn_options, txn);
    for (const auto& key : keys) {
      ASSERT_OK(txn->Put(handles[2], key, "blah"));
    }
    ASSERT_OK(txn->Commit());
    // Different access pattern (different hash seed)
    ASSERT_NE(cur_seen.rolling_hash, base_seen.rolling_hash);
    ASSERT_NE(cur_seen.rolling_hash, cf1_seen.rolling_hash);
    // Same pointer space
    ASSERT_LT(cur_seen.min, base_seen.max);
    ASSERT_GT(cur_seen.max, base_seen.min);
    // Sufficiently large hash coverage of the space
    ASSERT_GT(cur_seen.max - cur_seen.min, min_span_bytes);

    // And DB with shared lock buckets
    cur_seen = {};
    delete txn;
    txn = shared_txn_db->BeginTransaction(write_options, txn_options);
    for (const auto& key : keys) {
      ASSERT_OK(txn->Put(key, "blah"));
    }
    ASSERT_OK(txn->Commit());
    // Different access pattern (different hash seed)
    ASSERT_NE(cur_seen.rolling_hash, base_seen.rolling_hash);
    ASSERT_NE(cur_seen.rolling_hash, cf1_seen.rolling_hash);
    // Same pointer space
    ASSERT_LT(cur_seen.min, base_seen.max);
    ASSERT_GT(cur_seen.max, base_seen.min);
    // Sufficiently large hash coverage of the space
    ASSERT_GT(cur_seen.max - cur_seen.min, min_span_bytes);

    // And DB with distinct lock buckets
    cur_seen = {};
    delete txn;
    txn = nonshared_txn_db->BeginTransaction(write_options, txn_options);
    for (const auto& key : keys) {
      ASSERT_OK(txn->Put(key, "blah"));
    }
    ASSERT_OK(txn->Commit());
    // Different access pattern (different hash seed)
    ASSERT_NE(cur_seen.rolling_hash, base_seen.rolling_hash);
    ASSERT_NE(cur_seen.rolling_hash, cf1_seen.rolling_hash);
    // Different pointer space
    ASSERT_TRUE(cur_seen.min > base_seen.max || cur_seen.max < base_seen.min);
    // Sufficiently large hash coverage of the space
    ASSERT_GT(cur_seen.max - cur_seen.min, min_span_bytes);

    delete txn;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }

  // ** Test dropping column family before committing, or even creating txn **
  txn = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn->Delete(handles[1], "AAA"));

  s = txn_db->DropColumnFamily(handles[1]);
  ASSERT_OK(s);
  s = txn_db->DropColumnFamily(handles[2]);
  ASSERT_OK(s);

  ASSERT_NOK(txn->Commit());

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  ASSERT_OK(txn2->Delete(handles[2], "AAA"));
  ASSERT_NOK(txn2->Commit());

  delete txn;
  delete txn2;

  for (auto handle : handles) {
    delete handle;
  }
}

TEST_P(OptimisticTransactionTest, EmptyTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, "aaa", "aaa"));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn->Commit());
  delete txn;

  txn = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn->Rollback());
  delete txn;

  txn = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn->GetForUpdate(read_options, "aaa", &value));
  ASSERT_EQ(value, "aaa");

  ASSERT_OK(txn->Commit());
  delete txn;

  txn = txn_db->BeginTransaction(write_options);
  txn->SetSnapshot();
  ASSERT_OK(txn->GetForUpdate(read_options, "aaa", &value));
  ASSERT_EQ(value, "aaa");

  ASSERT_OK(txn_db->Put(write_options, "aaa", "xxx"));
  Status s = txn->Commit();
  ASSERT_TRUE(s.IsBusy());
  delete txn;
}

TEST_P(OptimisticTransactionTest, PredicateManyPreceders) {
  WriteOptions write_options;
  ReadOptions read_options1;
  ReadOptions read_options2;
  OptimisticTransactionOptions txn_options;
  std::string value;

  txn_options.set_snapshot = true;
  Transaction* txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  Transaction* txn2 = txn_db->BeginTransaction(write_options);
  txn2->SetSnapshot();
  read_options2.snapshot = txn2->GetSnapshot();

  std::vector<Slice> multiget_keys = {"1", "2", "3"};
  std::vector<std::string> multiget_values;

  std::vector<Status> results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[0].IsNotFound());
  ASSERT_TRUE(results[1].IsNotFound());
  ASSERT_TRUE(results[2].IsNotFound());

  ASSERT_OK(txn2->Put("2", "x"));

  ASSERT_OK(txn2->Commit());

  multiget_values.clear();
  results =
      txn1->MultiGetForUpdate(read_options1, multiget_keys, &multiget_values);
  ASSERT_TRUE(results[0].IsNotFound());
  ASSERT_TRUE(results[1].IsNotFound());
  ASSERT_TRUE(results[2].IsNotFound());

  // should not commit since txn2 wrote a key txn has read
  Status s = txn1->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  ASSERT_OK(txn1->Put("4", "x"));

  ASSERT_OK(txn2->Delete("4"));

  // txn1 can commit since txn2's delete hasn't happened yet (it's just batched)
  ASSERT_OK(txn1->Commit());

  s = txn2->GetForUpdate(read_options2, "4", &value);
  ASSERT_TRUE(s.IsNotFound());

  // txn2 cannot commit since txn1 changed "4"
  s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn1;
  delete txn2;
}

TEST_P(OptimisticTransactionTest, LostUpdate) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions read_options1;
  ReadOptions read_options2;
  OptimisticTransactionOptions txn_options;
  std::string value;

  // Test 2 transactions writing to the same key in multiple orders and
  // with/without snapshots

  Transaction* txn1 = txn_db->BeginTransaction(write_options);
  Transaction* txn2 = txn_db->BeginTransaction(write_options);

  ASSERT_OK(txn1->Put("1", "1"));
  ASSERT_OK(txn2->Put("1", "2"));

  ASSERT_OK(txn1->Commit());

  Status s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn1;
  delete txn2;

  txn_options.set_snapshot = true;
  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  ASSERT_OK(txn1->Put("1", "3"));
  ASSERT_OK(txn2->Put("1", "4"));

  ASSERT_OK(txn1->Commit());

  s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  ASSERT_OK(txn1->Put("1", "5"));
  ASSERT_OK(txn1->Commit());

  ASSERT_OK(txn2->Put("1", "6"));
  s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options, txn_options);
  read_options1.snapshot = txn1->GetSnapshot();

  txn2 = txn_db->BeginTransaction(write_options, txn_options);
  read_options2.snapshot = txn2->GetSnapshot();

  ASSERT_OK(txn1->Put("1", "5"));
  ASSERT_OK(txn1->Commit());

  txn2->SetSnapshot();
  ASSERT_OK(txn2->Put("1", "6"));
  ASSERT_OK(txn2->Commit());

  delete txn1;
  delete txn2;

  txn1 = txn_db->BeginTransaction(write_options);
  txn2 = txn_db->BeginTransaction(write_options);

  ASSERT_OK(txn1->Put("1", "7"));
  ASSERT_OK(txn1->Commit());

  ASSERT_OK(txn2->Put("1", "8"));
  ASSERT_OK(txn2->Commit());

  delete txn1;
  delete txn2;

  ASSERT_OK(txn_db->Get(read_options, "1", &value));
  ASSERT_EQ(value, "8");
}

TEST_P(OptimisticTransactionTest, UntrackedWrites) {
  WriteOptions write_options;
  ReadOptions read_options;
  std::string value;
  Status s;

  // Verify transaction rollback works for untracked keys.
  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn->PutUntracked("untracked", "0"));
  ASSERT_OK(txn->Rollback());
  s = txn_db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  txn = txn_db->BeginTransaction(write_options);

  const WideColumns untracked_columns{{"hello", "world"}};

  ASSERT_OK(txn->Put("tracked", "1"));
  ASSERT_OK(txn->PutUntracked("untracked", "1"));
  ASSERT_OK(txn->PutEntityUntracked(txn_db->DefaultColumnFamily(), "untracked",
                                    untracked_columns));
  ASSERT_OK(txn->MergeUntracked("untracked", "2"));
  ASSERT_OK(txn->DeleteUntracked("untracked"));

  // Write to the untracked key outside of the transaction and verify
  // it doesn't prevent the transaction from committing.
  ASSERT_OK(txn_db->Put(write_options, "untracked", "x"));

  ASSERT_OK(txn->Commit());

  s = txn_db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
  txn = txn_db->BeginTransaction(write_options);

  const WideColumns untracked_new_columns{{"foo", "bar"}};

  ASSERT_OK(txn->Put("tracked", "10"));
  ASSERT_OK(txn->PutUntracked("untracked", "A"));
  ASSERT_OK(txn->PutEntityUntracked(txn_db->DefaultColumnFamily(), "untracked",
                                    untracked_new_columns));

  // Write to tracked key outside of the transaction and verify that the
  // untracked keys are not written when the commit fails.
  ASSERT_OK(txn_db->Delete(write_options, "tracked"));

  s = txn->Commit();
  ASSERT_TRUE(s.IsBusy());

  s = txn_db->Get(read_options, "untracked", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

TEST_P(OptimisticTransactionTest, IteratorTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions snapshot_read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;

  // Write some keys to the db
  ASSERT_OK(txn_db->Put(write_options, "A", "a"));
  ASSERT_OK(txn_db->Put(write_options, "G", "g"));
  ASSERT_OK(txn_db->Put(write_options, "F", "f"));
  ASSERT_OK(txn_db->Put(write_options, "C", "c"));
  ASSERT_OK(txn_db->Put(write_options, "D", "d"));

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  // Write some keys in a txn
  ASSERT_OK(txn->Put("B", "b"));
  ASSERT_OK(txn->Put("H", "h"));
  ASSERT_OK(txn->Delete("D"));
  ASSERT_OK(txn->Put("E", "e"));

  txn->SetSnapshot();
  const Snapshot* snapshot = txn->GetSnapshot();

  // Write some keys to the db after the snapshot
  ASSERT_OK(txn_db->Put(write_options, "BB", "xx"));
  ASSERT_OK(txn_db->Put(write_options, "C", "xx"));

  read_options.snapshot = snapshot;
  Iterator* iter = txn->GetIterator(read_options);
  ASSERT_OK(iter->status());
  iter->SeekToFirst();

  // Read all keys via iter and lock them all
  std::string results[] = {"a", "b", "c", "e", "f", "g", "h"};
  for (int i = 0; i < 7; i++) {
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(results[i], iter->value().ToString());

    ASSERT_OK(
        txn->GetForUpdate(read_options, iter->key(), (std::string*)nullptr));

    iter->Next();
  }
  ASSERT_FALSE(iter->Valid());

  iter->Seek("G");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("g", iter->value().ToString());

  iter->Prev();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("f", iter->value().ToString());

  iter->Seek("D");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("e", iter->value().ToString());

  iter->Seek("C");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("c", iter->value().ToString());

  iter->Next();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("e", iter->value().ToString());

  iter->Seek("");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->value().ToString());

  iter->Seek("X");
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());

  iter->SeekToLast();
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("h", iter->value().ToString());

  // key "C" was modified in the db after txn's snapshot.  txn will not commit.
  Status s = txn->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete iter;
  delete txn;
}

TEST_P(OptimisticTransactionTest, DeleteRangeSupportTest) {
  // `OptimisticTransactionDB` does not allow range deletion in any API.
  ASSERT_TRUE(
      txn_db
          ->DeleteRange(WriteOptions(), txn_db->DefaultColumnFamily(), "a", "b")
          .IsNotSupported());
  WriteBatch wb;
  ASSERT_OK(wb.DeleteRange("a", "b"));
  ASSERT_NOK(txn_db->Write(WriteOptions(), &wb));
}

TEST_P(OptimisticTransactionTest, SavepointTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions snapshot_read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;

  Transaction* txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  Status s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());

  txn->SetSavePoint();  // 1

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to beginning of txn
  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(txn->Put("B", "b"));

  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Get(read_options, "B", &value));
  ASSERT_EQ("b", value);

  delete txn;
  txn = txn_db->BeginTransaction(write_options);
  ASSERT_NE(txn, nullptr);

  ASSERT_OK(txn->Put("A", "a"));
  ASSERT_OK(txn->Put("B", "bb"));
  ASSERT_OK(txn->Put("C", "c"));

  txn->SetSavePoint();  // 2

  ASSERT_OK(txn->Delete("B"));
  ASSERT_OK(txn->Put("C", "cc"));
  ASSERT_OK(txn->Put("D", "d"));

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to 2

  ASSERT_OK(txn->Get(read_options, "A", &value));
  ASSERT_EQ("a", value);
  ASSERT_OK(txn->Get(read_options, "B", &value));
  ASSERT_EQ("bb", value);
  ASSERT_OK(txn->Get(read_options, "C", &value));
  ASSERT_EQ("c", value);
  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(txn->Put("A", "a"));
  ASSERT_OK(txn->Put("E", "e"));

  // Rollback to beginning of txn
  s = txn->RollbackToSavePoint();
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(txn->Rollback());

  s = txn->Get(read_options, "A", &value);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_OK(txn->Get(read_options, "B", &value));
  ASSERT_EQ("b", value);
  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(read_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(txn->Put("A", "aa"));
  ASSERT_OK(txn->Put("F", "f"));

  txn->SetSavePoint();  // 3
  txn->SetSavePoint();  // 4

  ASSERT_OK(txn->Put("G", "g"));
  ASSERT_OK(txn->Delete("F"));
  ASSERT_OK(txn->Delete("B"));

  ASSERT_OK(txn->Get(read_options, "A", &value));
  ASSERT_EQ("aa", value);

  s = txn->Get(read_options, "F", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn->Get(read_options, "B", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(txn->RollbackToSavePoint());  // Rollback to 3

  ASSERT_OK(txn->Get(read_options, "F", &value));
  ASSERT_EQ("f", value);

  s = txn->Get(read_options, "G", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(txn->Commit());

  ASSERT_OK(txn_db->Get(read_options, "F", &value));
  ASSERT_EQ("f", value);

  s = txn_db->Get(read_options, "G", &value);
  ASSERT_TRUE(s.IsNotFound());

  ASSERT_OK(txn_db->Get(read_options, "A", &value));
  ASSERT_EQ("aa", value);

  ASSERT_OK(txn_db->Get(read_options, "B", &value));
  ASSERT_EQ("b", value);

  s = txn_db->Get(read_options, "C", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn_db->Get(read_options, "D", &value);
  ASSERT_TRUE(s.IsNotFound());

  s = txn_db->Get(read_options, "E", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

TEST_P(OptimisticTransactionTest, UndoGetForUpdateTest) {
  WriteOptions write_options;
  ReadOptions read_options;
  ReadOptions snapshot_read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;

  ASSERT_OK(txn_db->Put(write_options, "A", ""));

  Transaction* txn1 = txn_db->BeginTransaction(write_options);
  ASSERT_TRUE(txn1);

  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));

  txn1->UndoGetForUpdate("A");

  Transaction* txn2 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn2->Put("A", "x"));
  ASSERT_OK(txn2->Commit());
  delete txn2;

  // Verify that txn1 can commit since A isn't conflict checked
  ASSERT_OK(txn1->Commit());
  delete txn1;

  txn1 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn1->Put("A", "a"));

  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));

  txn1->UndoGetForUpdate("A");

  txn2 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn2->Put("A", "x"));
  ASSERT_OK(txn2->Commit());
  delete txn2;

  // Verify that txn1 cannot commit since A will still be conflict checked
  Status s = txn1->Commit();
  ASSERT_TRUE(s.IsBusy());
  delete txn1;

  txn1 = txn_db->BeginTransaction(write_options);

  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));
  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));

  txn1->UndoGetForUpdate("A");

  txn2 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn2->Put("A", "x"));
  ASSERT_OK(txn2->Commit());
  delete txn2;

  // Verify that txn1 cannot commit since A will still be conflict checked
  s = txn1->Commit();
  ASSERT_TRUE(s.IsBusy());
  delete txn1;

  txn1 = txn_db->BeginTransaction(write_options);

  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));
  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));

  txn1->UndoGetForUpdate("A");
  txn1->UndoGetForUpdate("A");

  txn2 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn2->Put("A", "x"));
  ASSERT_OK(txn2->Commit());
  delete txn2;

  // Verify that txn1 can commit since A isn't conflict checked
  ASSERT_OK(txn1->Commit());
  delete txn1;

  txn1 = txn_db->BeginTransaction(write_options);

  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));

  txn1->SetSavePoint();
  txn1->UndoGetForUpdate("A");

  txn2 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn2->Put("A", "x"));
  ASSERT_OK(txn2->Commit());
  delete txn2;

  // Verify that txn1 cannot commit since A will still be conflict checked
  s = txn1->Commit();
  ASSERT_TRUE(s.IsBusy());
  delete txn1;

  txn1 = txn_db->BeginTransaction(write_options);

  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));

  txn1->SetSavePoint();
  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));
  txn1->UndoGetForUpdate("A");

  txn2 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn2->Put("A", "x"));
  ASSERT_OK(txn2->Commit());
  delete txn2;

  // Verify that txn1 cannot commit since A will still be conflict checked
  s = txn1->Commit();
  ASSERT_TRUE(s.IsBusy());
  delete txn1;

  txn1 = txn_db->BeginTransaction(write_options);

  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));

  txn1->SetSavePoint();
  ASSERT_OK(txn1->GetForUpdate(read_options, "A", &value));
  txn1->UndoGetForUpdate("A");

  ASSERT_OK(txn1->RollbackToSavePoint());
  txn1->UndoGetForUpdate("A");

  txn2 = txn_db->BeginTransaction(write_options);
  ASSERT_OK(txn2->Put("A", "x"));
  ASSERT_OK(txn2->Commit());
  delete txn2;

  // Verify that txn1 can commit since A isn't conflict checked
  ASSERT_OK(txn1->Commit());
  delete txn1;
}

namespace {
Status OptimisticTransactionStressTestInserter(OptimisticTransactionDB* db,
                                               const size_t num_transactions,
                                               const size_t num_sets,
                                               const size_t num_keys_per_set) {
  size_t seed = std::hash<std::thread::id>()(std::this_thread::get_id());
  Random64 _rand(seed);
  WriteOptions write_options;
  ReadOptions read_options;
  OptimisticTransactionOptions txn_options;
  txn_options.set_snapshot = true;

  RandomTransactionInserter inserter(&_rand, write_options, read_options,
                                     num_keys_per_set,
                                     static_cast<uint16_t>(num_sets));

  for (size_t t = 0; t < num_transactions; t++) {
    bool success = inserter.OptimisticTransactionDBInsert(db, txn_options);
    if (!success) {
      // unexpected failure
      return inserter.GetLastStatus();
    }
  }

  inserter.GetLastStatus().PermitUncheckedError();

  // Make sure at least some of the transactions succeeded.  It's ok if
  // some failed due to write-conflicts.
  if (inserter.GetFailureCount() > num_transactions / 2) {
    return Status::TryAgain("Too many transactions failed! " +
                            std::to_string(inserter.GetFailureCount()) + " / " +
                            std::to_string(num_transactions));
  }

  return Status::OK();
}
}  // namespace

TEST_P(OptimisticTransactionTest, OptimisticTransactionStressTest) {
  const size_t num_threads = 4;
  const size_t num_transactions_per_thread = 10000;
  const size_t num_sets = 3;
  const size_t num_keys_per_set = 100;
  // Setting the key-space to be 100 keys should cause enough write-conflicts
  // to make this test interesting.

  std::vector<port::Thread> threads;

  std::function<void()> call_inserter = [&] {
    ASSERT_OK(OptimisticTransactionStressTestInserter(
        txn_db.get(), num_transactions_per_thread, num_sets, num_keys_per_set));
  };

  // Create N threads that use RandomTransactionInserter to write
  // many transactions.
  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(call_inserter);
  }

  // Wait for all threads to run
  for (auto& t : threads) {
    t.join();
  }

  // Verify that data is consistent
  Status s = RandomTransactionInserter::Verify(txn_db.get(), num_sets);
  ASSERT_OK(s);
}

TEST_P(OptimisticTransactionTest, SequenceNumberAfterRecoverTest) {
  WriteOptions write_options;
  OptimisticTransactionOptions transaction_options;

  Transaction* transaction(
      txn_db->BeginTransaction(write_options, transaction_options));
  Status s = transaction->Put("foo", "val");
  ASSERT_OK(s);
  s = transaction->Put("foo2", "val");
  ASSERT_OK(s);
  s = transaction->Put("foo3", "val");
  ASSERT_OK(s);
  s = transaction->Commit();
  ASSERT_OK(s);
  delete transaction;

  Reopen();
  transaction = txn_db->BeginTransaction(write_options, transaction_options);
  s = transaction->Put("bar", "val");
  ASSERT_OK(s);
  s = transaction->Put("bar2", "val");
  ASSERT_OK(s);
  s = transaction->Commit();
  ASSERT_OK(s);

  delete transaction;
}

#ifdef __SANITIZE_THREAD__
// Skip OptimisticTransactionTest.SequenceNumberAfterRecoverLargeTest under TSAN
// to avoid false positive because of TSAN lock limit of 64.
#else
TEST_P(OptimisticTransactionTest, SequenceNumberAfterRecoverLargeTest) {
  WriteOptions write_options;
  OptimisticTransactionOptions transaction_options;

  Transaction* transaction(
      txn_db->BeginTransaction(write_options, transaction_options));

  std::string value(1024 * 1024, 'X');
  const size_t n_zero = 2;
  std::string s_i;
  Status s;
  for (int i = 1; i <= 64; i++) {
    s_i = std::to_string(i);
    auto key = std::string(n_zero - std::min(n_zero, s_i.length()), '0') + s_i;
    s = transaction->Put(key, value);
    ASSERT_OK(s);
  }

  s = transaction->Commit();
  ASSERT_OK(s);
  delete transaction;

  Reopen();
  transaction = txn_db->BeginTransaction(write_options, transaction_options);
  s = transaction->Put("bar", "val");
  ASSERT_OK(s);
  s = transaction->Commit();
  if (!s.ok()) {
    std::cerr << "Failed to commit records. Error: " << s.ToString()
              << std::endl;
  }
  ASSERT_OK(s);

  delete transaction;
}
#endif  // __SANITIZE_THREAD__

TEST_P(OptimisticTransactionTest, TimestampedSnapshotMissingCommitTs) {
  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  ASSERT_OK(txn->Put("a", "v"));
  Status s = txn->CommitAndTryCreateSnapshot();
  ASSERT_TRUE(s.IsInvalidArgument());
}

TEST_P(OptimisticTransactionTest, TimestampedSnapshotSetCommitTs) {
  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  ASSERT_OK(txn->Put("a", "v"));
  std::shared_ptr<const Snapshot> snapshot;
  Status s = txn->CommitAndTryCreateSnapshot(nullptr, /*ts=*/100, &snapshot);
  ASSERT_TRUE(s.IsNotSupported());
}

TEST_P(OptimisticTransactionTest, PutEntitySuccess) {
  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};
  const WideColumns foo_new_columns{
      {kDefaultWideColumnName, "baz"}, {"colA", "valA"}, {"colB", "valB"}};

  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              foo, foo_columns));

  {
    std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));

    ASSERT_NE(txn, nullptr);
    ASSERT_EQ(txn->GetNumPutEntities(), 0);

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                               foo, &columns));
      ASSERT_EQ(columns.columns(), foo_columns);
    }

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntityForUpdate(
          ReadOptions(), txn_db->DefaultColumnFamily(), foo, &columns));
      ASSERT_EQ(columns.columns(), foo_columns);
    }

    ASSERT_OK(
        txn->PutEntity(txn_db->DefaultColumnFamily(), foo, foo_new_columns));

    ASSERT_EQ(txn->GetNumPutEntities(), 1);

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                               foo, &columns));
      ASSERT_EQ(columns.columns(), foo_new_columns);
    }

    {
      PinnableWideColumns columns;
      ASSERT_OK(txn->GetEntityForUpdate(
          ReadOptions(), txn_db->DefaultColumnFamily(), foo, &columns));
      ASSERT_EQ(columns.columns(), foo_new_columns);
    }

    ASSERT_OK(txn->Commit());
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                foo, &columns));
    ASSERT_EQ(columns.columns(), foo_new_columns);
  }
}

TEST_P(OptimisticTransactionTest, PutEntityWriteConflict) {
  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};
  constexpr char baz[] = "baz";
  const WideColumns baz_columns{
      {kDefaultWideColumnName, "quux"}, {"colA", "valA"}, {"colB", "valB"}};

  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              foo, foo_columns));
  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              baz, baz_columns));

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  ASSERT_NE(txn, nullptr);

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), baz,
                             &columns));
    ASSERT_EQ(columns.columns(), baz_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    txn->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_columns);
    ASSERT_EQ(results[1].columns(), baz_columns);
  }

  const WideColumns foo_new_columns{{kDefaultWideColumnName, "FOO"},
                                    {"hello", "world"}};
  const WideColumns baz_new_columns{{kDefaultWideColumnName, "BAZ"},
                                    {"ping", "pong"}};

  ASSERT_OK(
      txn->PutEntity(txn_db->DefaultColumnFamily(), foo, foo_new_columns));
  ASSERT_OK(
      txn->PutEntity(txn_db->DefaultColumnFamily(), baz, baz_new_columns));

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_new_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), baz,
                             &columns));
    ASSERT_EQ(columns.columns(), baz_new_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    txn->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_new_columns);
    ASSERT_EQ(results[1].columns(), baz_new_columns);
  }

  // This PutEntity outside of a transaction will conflict with the previous
  // write
  const WideColumns foo_conflict_columns{{kDefaultWideColumnName, "X"},
                                         {"conflicting", "write"}};
  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              foo, foo_conflict_columns));

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                foo, &columns));
    ASSERT_EQ(columns.columns(), foo_conflict_columns);
  }

  ASSERT_TRUE(txn->Commit().IsBusy());  // Txn should not commit

  // Verify that transaction did not write anything
  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                foo, &columns));
    ASSERT_EQ(columns.columns(), foo_conflict_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                baz, &columns));
    ASSERT_EQ(columns.columns(), baz_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn_db->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                           num_keys, keys.data(), results.data(),
                           statuses.data(), sorted_input);

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_conflict_columns);
    ASSERT_EQ(results[1].columns(), baz_columns);
  }
}

TEST_P(OptimisticTransactionTest, PutEntityWriteConflictTxnTxn) {
  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};
  constexpr char baz[] = "baz";
  const WideColumns baz_columns{
      {kDefaultWideColumnName, "quux"}, {"colA", "valA"}, {"colB", "valB"}};

  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              foo, foo_columns));
  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              baz, baz_columns));

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  ASSERT_NE(txn, nullptr);

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), baz,
                             &columns));
    ASSERT_EQ(columns.columns(), baz_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    txn->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_columns);
    ASSERT_EQ(results[1].columns(), baz_columns);
  }

  const WideColumns foo_new_columns{{kDefaultWideColumnName, "FOO"},
                                    {"hello", "world"}};
  const WideColumns baz_new_columns{{kDefaultWideColumnName, "BAZ"},
                                    {"ping", "pong"}};

  ASSERT_OK(
      txn->PutEntity(txn_db->DefaultColumnFamily(), foo, foo_new_columns));
  ASSERT_OK(
      txn->PutEntity(txn_db->DefaultColumnFamily(), baz, baz_new_columns));

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_new_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), baz,
                             &columns));
    ASSERT_EQ(columns.columns(), baz_new_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    txn->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_new_columns);
    ASSERT_EQ(results[1].columns(), baz_new_columns);
  }

  std::unique_ptr<Transaction> conflicting_txn(
      txn_db->BeginTransaction(WriteOptions()));
  ASSERT_NE(conflicting_txn, nullptr);

  const WideColumns foo_conflict_columns{{kDefaultWideColumnName, "X"},
                                         {"conflicting", "write"}};
  ASSERT_OK(conflicting_txn->PutEntity(txn_db->DefaultColumnFamily(), foo,
                                       foo_conflict_columns));
  ASSERT_OK(conflicting_txn->Commit());

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                foo, &columns));
    ASSERT_EQ(columns.columns(), foo_conflict_columns);
  }

  ASSERT_TRUE(txn->Commit().IsBusy());  // Txn should not commit

  // Verify that transaction did not write anything
  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                foo, &columns));
    ASSERT_EQ(columns.columns(), foo_conflict_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                baz, &columns));
    ASSERT_EQ(columns.columns(), baz_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{foo, baz}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn_db->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                           num_keys, keys.data(), results.data(),
                           statuses.data(), sorted_input);

    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);

    ASSERT_EQ(results[0].columns(), foo_conflict_columns);
    ASSERT_EQ(results[1].columns(), baz_columns);
  }
}

TEST_P(OptimisticTransactionTest, PutEntityReadConflict) {
  constexpr char foo[] = "foo";
  const WideColumns foo_columns{
      {kDefaultWideColumnName, "bar"}, {"col1", "val1"}, {"col2", "val2"}};

  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              foo, foo_columns));

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  ASSERT_NE(txn, nullptr);

  txn->SetSnapshot();

  ReadOptions snapshot_read_options;
  snapshot_read_options.snapshot = txn->GetSnapshot();

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntityForUpdate(
        snapshot_read_options, txn_db->DefaultColumnFamily(), foo, &columns));
    ASSERT_EQ(columns.columns(), foo_columns);
  }

  // This PutEntity outside of a transaction will conflict with the previous
  // write
  const WideColumns foo_conflict_columns{{kDefaultWideColumnName, "X"},
                                         {"conflicting", "write"}};
  ASSERT_OK(txn_db->PutEntity(WriteOptions(), txn_db->DefaultColumnFamily(),
                              foo, foo_conflict_columns));

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                foo, &columns));
    ASSERT_EQ(columns.columns(), foo_conflict_columns);
  }

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), foo,
                             &columns));
    ASSERT_EQ(columns.columns(), foo_conflict_columns);
  }

  ASSERT_TRUE(txn->Commit().IsBusy());  // Txn should not commit

  {
    PinnableWideColumns columns;
    ASSERT_OK(txn_db->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                                foo, &columns));
    ASSERT_EQ(columns.columns(), foo_conflict_columns);
  }
}

TEST_P(OptimisticTransactionTest, EntityReadSanityChecks) {
  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";
  constexpr size_t num_keys = 2;

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  ASSERT_NE(txn, nullptr);

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    PinnableWideColumns columns;
    ASSERT_TRUE(txn->GetEntity(ReadOptions(), column_family, foo, &columns)
                    .IsInvalidArgument());
  }

  {
    constexpr PinnableWideColumns* columns = nullptr;
    ASSERT_TRUE(txn->GetEntity(ReadOptions(), txn_db->DefaultColumnFamily(),
                               foo, columns)
                    .IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kGet;

    PinnableWideColumns columns;
    ASSERT_TRUE(txn->GetEntity(read_options, txn_db->DefaultColumnFamily(), foo,
                               &columns)
                    .IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(ReadOptions(), column_family, num_keys, keys.data(),
                        results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr Slice* keys = nullptr;
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), num_keys,
                        keys, results.data(), statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    std::array<Slice, num_keys> keys{{foo, bar}};
    constexpr PinnableWideColumns* results = nullptr;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(ReadOptions(), txn_db->DefaultColumnFamily(), num_keys,
                        keys.data(), results, statuses.data(), sorted_input);

    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kMultiGet;

    std::array<Slice, num_keys> keys{{foo, bar}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;
    constexpr bool sorted_input = false;

    txn->MultiGetEntity(read_options, txn_db->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data(),
                        sorted_input);
    ASSERT_TRUE(statuses[0].IsInvalidArgument());
    ASSERT_TRUE(statuses[1].IsInvalidArgument());
  }

  {
    constexpr ColumnFamilyHandle* column_family = nullptr;
    PinnableWideColumns columns;
    ASSERT_TRUE(
        txn->GetEntityForUpdate(ReadOptions(), column_family, foo, &columns)
            .IsInvalidArgument());
  }

  {
    constexpr PinnableWideColumns* columns = nullptr;
    ASSERT_TRUE(txn->GetEntityForUpdate(ReadOptions(),
                                        txn_db->DefaultColumnFamily(), foo,
                                        columns)
                    .IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kGet;

    PinnableWideColumns columns;
    ASSERT_TRUE(txn->GetEntityForUpdate(read_options,
                                        txn_db->DefaultColumnFamily(), foo,
                                        &columns)
                    .IsInvalidArgument());
  }

  {
    txn->SetSnapshot();

    ReadOptions read_options;
    read_options.snapshot = txn->GetSnapshot();

    PinnableWideColumns columns;
    constexpr bool exclusive = true;
    constexpr bool do_validate = false;

    ASSERT_TRUE(txn->GetEntityForUpdate(read_options,
                                        txn_db->DefaultColumnFamily(), foo,
                                        &columns, exclusive, do_validate)
                    .IsInvalidArgument());
  }
}

TEST_P(OptimisticTransactionTest, CoalescingIterator) {
  ColumnFamilyOptions cf_opts;
  cf_opts.enable_blob_files = true;

  ColumnFamilyHandle* cfh1 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf_opts, "cf1", &cfh1));
  std::unique_ptr<ColumnFamilyHandle> cfh1_guard(cfh1);

  ColumnFamilyHandle* cfh2 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf_opts, "cf2", &cfh2));
  std::unique_ptr<ColumnFamilyHandle> cfh2_guard(cfh2);

  // Note: "cf1" keys are present only in CF1; "cf2" keys are only present in
  // CF2; "cf12" keys are present in both CFs. "a" keys are present only in the
  // database; "b" keys are present only in the transaction; "c" keys are
  // present in both the database and the transaction. The values indicate the
  // column family as well as whether the entry came from the database or the
  // transaction.

  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf1_a", "cf1_a_db_cf1"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf1_c", "cf1_c_db_cf1"));

  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf2_a", "cf2_a_db_cf2"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf2_c", "cf2_c_db_cf2"));

  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf12_a", "cf12_a_db_cf1"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf12_a", "cf12_a_db_cf2"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf12_c", "cf12_c_db_cf1"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf12_c", "cf12_c_db_cf2"));

  ASSERT_OK(txn_db->Flush(FlushOptions(), cfh1));
  ASSERT_OK(txn_db->Flush(FlushOptions(), cfh2));

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));

  ASSERT_OK(txn->Put(cfh1, "cf1_b", "cf1_b_txn_cf1"));
  ASSERT_OK(txn->Put(cfh1, "cf1_c", "cf1_c_txn_cf1"));

  ASSERT_OK(txn->Put(cfh2, "cf2_b", "cf2_b_txn_cf2"));
  ASSERT_OK(txn->Put(cfh2, "cf2_c", "cf2_c_txn_cf2"));

  ASSERT_OK(txn->Put(cfh1, "cf12_b", "cf12_b_txn_cf1"));
  ASSERT_OK(txn->Put(cfh2, "cf12_b", "cf12_b_txn_cf2"));
  ASSERT_OK(txn->Put(cfh1, "cf12_c", "cf12_c_txn_cf1"));
  ASSERT_OK(txn->Put(cfh2, "cf12_c", "cf12_c_txn_cf2"));

  auto verify = [&](bool allow_unprepared_value, auto prepare_if_needed) {
    ReadOptions read_options;
    read_options.allow_unprepared_value = allow_unprepared_value;

    std::unique_ptr<Iterator> iter(
        txn->GetCoalescingIterator(read_options, {cfh1, cfh2}));

    {
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf12_a");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf12_a_db_cf2");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf12_b");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf12_b_txn_cf2");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf12_c");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf12_c_txn_cf2");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf1_a");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf1_a_db_cf1");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf1_b");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf1_b_txn_cf1");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf1_c");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf1_c_txn_cf1");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf2_a");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf2_a_db_cf2");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf2_b");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf2_b_txn_cf2");
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf2_c");

      prepare_if_needed(iter.get());

      ASSERT_EQ(iter->value(), "cf2_c_txn_cf2");
    }

    {
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  verify(/* allow_unprepared_value */ false, [](Iterator*) {});
  verify(/* allow_unprepared_value */ true, [](Iterator* iter) {
    ASSERT_TRUE(iter->value().empty());
    ASSERT_TRUE(iter->PrepareValue());
  });
}

TEST_P(OptimisticTransactionTest, CoalescingIteratorSanityChecks) {
  ColumnFamilyOptions cf1_opts;
  ColumnFamilyHandle* cfh1 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf1_opts, "cf1", &cfh1));
  std::unique_ptr<ColumnFamilyHandle> cfh1_guard(cfh1);

  ColumnFamilyOptions cf2_opts;
  cf2_opts.comparator = ReverseBytewiseComparator();
  ColumnFamilyHandle* cfh2 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf2_opts, "cf2", &cfh2));
  std::unique_ptr<ColumnFamilyHandle> cfh2_guard(cfh2);

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));

  {
    std::unique_ptr<Iterator> iter(
        txn->GetCoalescingIterator(ReadOptions(), {}));
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  {
    std::unique_ptr<Iterator> iter(
        txn->GetCoalescingIterator(ReadOptions(), {cfh1, cfh2}));
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kCompaction;

    std::unique_ptr<Iterator> iter(
        txn->GetCoalescingIterator(read_options, {cfh1}));
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }
}

TEST_P(OptimisticTransactionTest, AttributeGroupIterator) {
  ColumnFamilyOptions cf_opts;
  cf_opts.enable_blob_files = true;

  ColumnFamilyHandle* cfh1 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf_opts, "cf1", &cfh1));
  std::unique_ptr<ColumnFamilyHandle> cfh1_guard(cfh1);

  ColumnFamilyHandle* cfh2 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf_opts, "cf2", &cfh2));
  std::unique_ptr<ColumnFamilyHandle> cfh2_guard(cfh2);

  // Note: "cf1" keys are present only in CF1; "cf2" keys are only present in
  // CF2; "cf12" keys are present in both CFs. "a" keys are present only in the
  // database; "b" keys are present only in the transaction; "c" keys are
  // present in both the database and the transaction. The values indicate the
  // column family as well as whether the entry came from the database or the
  // transaction.

  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf1_a", "cf1_a_db_cf1"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf1_c", "cf1_c_db_cf1"));

  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf2_a", "cf2_a_db_cf2"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf2_c", "cf2_c_db_cf2"));

  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf12_a", "cf12_a_db_cf1"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf12_a", "cf12_a_db_cf2"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh1, "cf12_c", "cf12_c_db_cf1"));
  ASSERT_OK(txn_db->Put(WriteOptions(), cfh2, "cf12_c", "cf12_c_db_cf2"));

  ASSERT_OK(txn_db->Flush(FlushOptions(), cfh1));
  ASSERT_OK(txn_db->Flush(FlushOptions(), cfh2));

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));

  ASSERT_OK(txn->Put(cfh1, "cf1_b", "cf1_b_txn_cf1"));
  ASSERT_OK(txn->Put(cfh1, "cf1_c", "cf1_c_txn_cf1"));

  ASSERT_OK(txn->Put(cfh2, "cf2_b", "cf2_b_txn_cf2"));
  ASSERT_OK(txn->Put(cfh2, "cf2_c", "cf2_c_txn_cf2"));

  ASSERT_OK(txn->Put(cfh1, "cf12_b", "cf12_b_txn_cf1"));
  ASSERT_OK(txn->Put(cfh2, "cf12_b", "cf12_b_txn_cf2"));
  ASSERT_OK(txn->Put(cfh1, "cf12_c", "cf12_c_txn_cf1"));
  ASSERT_OK(txn->Put(cfh2, "cf12_c", "cf12_c_txn_cf2"));

  auto verify = [&](bool allow_unprepared_value, auto prepare_if_needed) {
    ReadOptions read_options;
    read_options.allow_unprepared_value = allow_unprepared_value;

    std::unique_ptr<AttributeGroupIterator> iter(
        txn->GetAttributeGroupIterator(read_options, {cfh1, cfh2}));

    {
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf12_a");

      prepare_if_needed(iter.get());

      WideColumns cf1_columns{{kDefaultWideColumnName, "cf12_a_db_cf1"}};
      WideColumns cf2_columns{{kDefaultWideColumnName, "cf12_a_db_cf2"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh1, &cf1_columns},
          IteratorAttributeGroup{cfh2, &cf2_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf12_b");

      prepare_if_needed(iter.get());

      WideColumns cf1_columns{{kDefaultWideColumnName, "cf12_b_txn_cf1"}};
      WideColumns cf2_columns{{kDefaultWideColumnName, "cf12_b_txn_cf2"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh1, &cf1_columns},
          IteratorAttributeGroup{cfh2, &cf2_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf12_c");

      prepare_if_needed(iter.get());

      WideColumns cf1_columns{{kDefaultWideColumnName, "cf12_c_txn_cf1"}};
      WideColumns cf2_columns{{kDefaultWideColumnName, "cf12_c_txn_cf2"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh1, &cf1_columns},
          IteratorAttributeGroup{cfh2, &cf2_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf1_a");

      prepare_if_needed(iter.get());

      WideColumns cf1_columns{{kDefaultWideColumnName, "cf1_a_db_cf1"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh1, &cf1_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf1_b");

      prepare_if_needed(iter.get());

      WideColumns cf1_columns{{kDefaultWideColumnName, "cf1_b_txn_cf1"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh1, &cf1_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf1_c");

      prepare_if_needed(iter.get());

      WideColumns cf1_columns{{kDefaultWideColumnName, "cf1_c_txn_cf1"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh1, &cf1_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf2_a");

      prepare_if_needed(iter.get());

      WideColumns cf2_columns{{kDefaultWideColumnName, "cf2_a_db_cf2"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh2, &cf2_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf2_b");

      prepare_if_needed(iter.get());

      WideColumns cf2_columns{{kDefaultWideColumnName, "cf2_b_txn_cf2"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh2, &cf2_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), "cf2_c");

      prepare_if_needed(iter.get());

      WideColumns cf2_columns{{kDefaultWideColumnName, "cf2_c_txn_cf2"}};
      IteratorAttributeGroups expected{
          IteratorAttributeGroup{cfh2, &cf2_columns}};
      ASSERT_EQ(iter->attribute_groups(), expected);
    }

    {
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  verify(/* allow_unprepared_value */ false, [](AttributeGroupIterator*) {});
  verify(/* allow_unprepared_value */ true, [](AttributeGroupIterator* iter) {
    ASSERT_TRUE(iter->attribute_groups().empty());
    ASSERT_TRUE(iter->PrepareValue());
  });
}

TEST_P(OptimisticTransactionTest, AttributeGroupIteratorSanityChecks) {
  ColumnFamilyOptions cf1_opts;
  ColumnFamilyHandle* cfh1 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf1_opts, "cf1", &cfh1));
  std::unique_ptr<ColumnFamilyHandle> cfh1_guard(cfh1);

  ColumnFamilyOptions cf2_opts;
  cf2_opts.comparator = ReverseBytewiseComparator();
  ColumnFamilyHandle* cfh2 = nullptr;
  ASSERT_OK(txn_db->CreateColumnFamily(cf2_opts, "cf2", &cfh2));
  std::unique_ptr<ColumnFamilyHandle> cfh2_guard(cfh2);

  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));

  {
    std::unique_ptr<AttributeGroupIterator> iter(
        txn->GetAttributeGroupIterator(ReadOptions(), {}));
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  {
    std::unique_ptr<AttributeGroupIterator> iter(
        txn->GetAttributeGroupIterator(ReadOptions(), {cfh1, cfh2}));
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  {
    ReadOptions read_options;
    read_options.io_activity = Env::IOActivity::kCompaction;

    std::unique_ptr<AttributeGroupIterator> iter(
        txn->GetAttributeGroupIterator(read_options, {cfh1}));
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }
}

INSTANTIATE_TEST_CASE_P(
    InstanceOccGroup, OptimisticTransactionTest,
    testing::Values(OccValidationPolicy::kValidateSerial,
                    OccValidationPolicy::kValidateParallel));

TEST(OccLockBucketsTest, CacheAligned) {
  // Typical x86_64 is 40 byte mutex, 64 byte cache line
  if (sizeof(port::Mutex) >= sizeof(CacheAlignedWrapper<port::Mutex>)) {
    ROCKSDB_GTEST_BYPASS("Test requires mutex smaller than cache line");
    return;
  }
  auto buckets_unaligned = MakeSharedOccLockBuckets(100, false);
  auto buckets_aligned = MakeSharedOccLockBuckets(100, true);
  // Save at least one byte per bucket
  ASSERT_LE(buckets_unaligned->ApproximateMemoryUsage() + 100,
            buckets_aligned->ApproximateMemoryUsage());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
