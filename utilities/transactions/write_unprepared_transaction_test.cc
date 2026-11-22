//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/transaction_test.h"
#include "utilities/transactions/write_unprepared_txn.h"
#include "utilities/transactions/write_unprepared_txn_db.h"

namespace ROCKSDB_NAMESPACE {

class WriteUnpreparedTransactionTestBase : public TransactionTestBase {
 public:
  WriteUnpreparedTransactionTestBase(bool use_stackable_db,
                                     bool two_write_queue,
                                     TxnDBWritePolicy write_policy,
                                     bool use_per_key_point_lock_mgr,
                                     int64_t deadlock_timeout_us)
      : TransactionTestBase(use_stackable_db, two_write_queue, write_policy,
                            kOrderedWrite, use_per_key_point_lock_mgr,
                            deadlock_timeout_us) {}
};

class WriteUnpreparedTransactionTest
    : public WriteUnpreparedTransactionTestBase,
      virtual public ::testing::WithParamInterface<
          std::tuple<bool, bool, TxnDBWritePolicy, bool, int64_t>> {
 public:
  WriteUnpreparedTransactionTest()
      : WriteUnpreparedTransactionTestBase(
            std::get<0>(GetParam()), std::get<1>(GetParam()),
            std::get<2>(GetParam()), std::get<3>(GetParam()),
            std::get<4>(GetParam())) {}

 protected:
  // Starts a transaction named "xid" with a flush threshold of 1 and returns
  // it in "*txn". Before each new write, such a transaction writes its current
  // write batch to the DB as an unprepared batch if that batch is not empty.
  // For example, after four writes the DB has three unprepared batches, and
  // the fourth write is only in the write batch of the transaction.
  void BeginTxnWithOneWritePerBatch(Transaction** txn) {
    TransactionOptions txn_options;
    txn_options.write_batch_flush_threshold = 1;
    Transaction* const new_txn =
        db->BeginTransaction(WriteOptions(), txn_options);
    ASSERT_NE(nullptr, new_txn);
    *txn = new_txn;
    ASSERT_OK(new_txn->SetName("xid"));
  }

  static size_t NumUnpreparedBatches(Transaction* txn) {
    return static_cast_with_check<WriteUnpreparedTxn>(txn)
        ->GetUnpreparedSequenceNumbers()
        .size();
  }

  // Deletes "txn" without rolling it back, as if the process stopped before
  // the transaction was prepared. Clearing the unprepared sequence numbers
  // keeps the destructor from rolling back the transaction, so the next DB
  // open has to do it.
  static void DeleteWithoutRollback(Transaction* txn) {
    static_cast_with_check<WriteUnpreparedTxn>(txn)->unprep_seqs_.clear();
    delete txn;
  }

  // Simulates a crash. The WAL is synced first, so everything written to the
  // DB survives, and the next DB open runs recovery on it.
  void SimulateCrash() {
    ASSERT_OK(db->FlushWAL(/*sync=*/true));
    static_cast_with_check<WriteUnpreparedTxnDB>(db)->TEST_Crash();
  }

  // Reopens the DB and counts the rollback batches that recovery writes while
  // it rolls back transactions that were not prepared.
  Status ReOpenAndCountRollbackBatches(int* rollback_batches_written) {
    *rollback_batches_written = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "WriteUnpreparedTxnDB::RollbackRecoveredTransaction:"
        "AfterWriteRollbackBatch",
        [rollback_batches_written](void*) { ++*rollback_batches_written; });
    SyncPoint::GetInstance()->EnableProcessing();
    Status s = ReOpenNoDelete();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    return s;
  }
};

INSTANTIATE_TEST_CASE_P(
    WriteUnpreparedTransactionTest, WriteUnpreparedTransactionTest,
    ::testing::Combine(::testing::Values(false), ::testing::Bool(),
                       ::testing::Values(WRITE_UNPREPARED), ::testing::Bool(),
                       ::testing::Values(0, 1000)));

enum SnapshotAction { NO_SNAPSHOT, RO_SNAPSHOT, REFRESH_SNAPSHOT };
enum VerificationOperation { VERIFY_GET, VERIFY_NEXT, VERIFY_PREV };
class WriteUnpreparedSnapshotTest
    : public WriteUnpreparedTransactionTestBase,
      virtual public ::testing::WithParamInterface<std::tuple<
          bool, SnapshotAction, VerificationOperation, bool, int64_t>> {
 public:
  WriteUnpreparedSnapshotTest()
      : WriteUnpreparedTransactionTestBase(
            false, std::get<0>(GetParam()), WRITE_UNPREPARED,
            std::get<3>(GetParam()), std::get<4>(GetParam())),
        action_(std::get<1>(GetParam())),
        verify_op_(std::get<2>(GetParam())) {}
  SnapshotAction action_;
  VerificationOperation verify_op_;
};

// Test parameters:
// Param 0): use stackable db, parameterization hard coded to be overwritten to
// false. Param 1): test mode for snapshot action Param 2): test mode for
// verification operation
INSTANTIATE_TEST_CASE_P(
    WriteUnpreparedSnapshotTest, WriteUnpreparedSnapshotTest,
    ::testing::Combine(::testing::Bool(),
                       ::testing::Values(NO_SNAPSHOT, RO_SNAPSHOT,
                                         REFRESH_SNAPSHOT),
                       ::testing::Values(VERIFY_GET, VERIFY_NEXT, VERIFY_PREV),
                       ::testing::Bool(), ::testing::Values(0, 1000)));

TEST_P(WriteUnpreparedTransactionTest, ReadYourOwnWrite) {
  // The following tests checks whether reading your own write for
  // a transaction works for write unprepared, when there are uncommitted
  // values written into DB.
  auto verify_state = [](Iterator* iter, const std::string& key,
                         const std::string& value) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(key, iter->key().ToString());
    ASSERT_EQ(value, iter->value().ToString());
  };

  // Test always reseeking vs never reseeking.
  for (uint64_t max_skip : {0, std::numeric_limits<int>::max()}) {
    options.max_sequential_skip_in_iterations = max_skip;
    options.disable_auto_compactions = true;
    ASSERT_OK(ReOpen());

    TransactionOptions txn_options;
    WriteOptions woptions;
    ReadOptions roptions;

    ASSERT_OK(db->Put(woptions, "a", ""));
    ASSERT_OK(db->Put(woptions, "b", ""));

    Transaction* txn = db->BeginTransaction(woptions, txn_options);
    WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
    txn->SetSnapshot();

    for (int i = 0; i < 5; i++) {
      std::string stored_value = "v" + std::to_string(i);
      ASSERT_OK(txn->Put("a", stored_value));
      ASSERT_OK(txn->Put("b", stored_value));
      ASSERT_OK(wup_txn->FlushWriteBatchToDB(false));

      // Test Get()
      std::string value;
      ASSERT_OK(txn->Get(roptions, "a", &value));
      ASSERT_EQ(value, stored_value);
      ASSERT_OK(txn->Get(roptions, "b", &value));
      ASSERT_EQ(value, stored_value);

      // Test Next()
      auto iter = txn->GetIterator(roptions);
      iter->Seek("a");
      verify_state(iter, "a", stored_value);

      iter->Next();
      verify_state(iter, "b", stored_value);

      iter->SeekToFirst();
      verify_state(iter, "a", stored_value);

      iter->Next();
      verify_state(iter, "b", stored_value);

      delete iter;

      // Test Prev()
      iter = txn->GetIterator(roptions);
      iter->SeekForPrev("b");
      verify_state(iter, "b", stored_value);

      iter->Prev();
      verify_state(iter, "a", stored_value);

      iter->SeekToLast();
      verify_state(iter, "b", stored_value);

      iter->Prev();
      verify_state(iter, "a", stored_value);

      delete iter;
    }

    delete txn;
  }
}

TEST_P(WriteUnpreparedSnapshotTest, ReadYourOwnWrite) {
  // This test validates a transaction can read its writes and the correctness
  // of its read with regard to a mocked snapshot functionality.
  const uint32_t kNumIter = 1000;
  const uint32_t kNumKeys = 5;

  // Test with
  // 1. no snapshots set
  // 2. snapshot set on ReadOptions
  // 3. snapshot set, and refreshing after every write.
  SnapshotAction snapshot_action = action_;
  WriteOptions write_options;
  txn_db_options.transaction_lock_timeout = -1;
  options.disable_auto_compactions = true;
  ASSERT_OK(ReOpen());

  std::vector<std::string> keys;
  keys.reserve(kNumKeys);
  for (uint32_t k = 0; k < kNumKeys; k++) {
    keys.push_back("k" + std::to_string(k));
  }

  // This counter will act as a "sequence number" to help us validate
  // visibility logic with snapshots. If we had direct access to the seqno of
  // snapshots and key/values, then we should directly compare those instead.
  std::atomic<int64_t> counter(0);

  std::function<void()> check_correctness_wrt_snapshot = [&]() {
    Transaction* txn;
    TransactionOptions txn_options;
    // batch_size of 1 causes writes to DB for every marker.
    txn_options.write_batch_flush_threshold = 1;
    ReadOptions read_options;

    for (uint32_t i = 0; i < kNumIter; i++) {
      txn = db->BeginTransaction(write_options, txn_options);
      txn->SetSnapshot();
      if (snapshot_action >= RO_SNAPSHOT) {
        read_options.snapshot = txn->GetSnapshot();
        ASSERT_TRUE(read_options.snapshot != nullptr);
      }

      uint64_t buf[1];

      // When scanning through the database, make sure that all unprepared
      // keys have value >= snapshot.
      int64_t snapshot_num = counter.fetch_add(1);

      Status s;
      for (const auto& key : keys) {
        buf[0] = counter.fetch_add(1);
        s = txn->Put(key, Slice((const char*)buf, sizeof(buf)));
        if (!s.ok()) {
          break;
        }
        if (snapshot_action == REFRESH_SNAPSHOT) {
          txn->SetSnapshot();
          read_options.snapshot = txn->GetSnapshot();
          snapshot_num = counter.fetch_add(1);
        }
      }

      ASSERT_OK(s);

      auto verify_key = [&snapshot_action,
                         &snapshot_num](const std::string& value) {
        ASSERT_EQ(value.size(), 8);

        if (snapshot_action == REFRESH_SNAPSHOT) {
          // If refresh snapshot is true, then the snapshot is refreshed
          // after every Put(), meaning that the current snapshot in
          // snapshot_num must be greater than the "seqno" of any keys
          // written by the current transaction.
          ASSERT_LT(((int64_t*)value.c_str())[0], snapshot_num);
        } else {
          // If refresh snapshot is not on, then the snapshot was taken at
          // the beginning of the transaction, meaning all writes must come
          // after snapshot_num
          ASSERT_GT(((int64_t*)value.c_str())[0], snapshot_num);
        }
      };

      // Validate one of Get()/Next()/Prev() depending on the verification
      // operation to use.
      switch (verify_op_) {
        case VERIFY_GET:  // Validate Get()
        {
          for (const auto& key : keys) {
            std::string value;
            ASSERT_OK(txn->Get(read_options, Slice(key), &value));
            verify_key(value);
          }
          break;
        }
        case VERIFY_NEXT:  // Validate Next()
        {
          Iterator* iter = txn->GetIterator(read_options);
          ASSERT_OK(iter->status());
          for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            verify_key(iter->value().ToString());
          }
          ASSERT_OK(iter->status());
          delete iter;
          break;
        }
        case VERIFY_PREV:  // Validate Prev()
        {
          Iterator* iter = txn->GetIterator(read_options);
          ASSERT_OK(iter->status());
          for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
            verify_key(iter->value().ToString());
          }
          ASSERT_OK(iter->status());
          delete iter;
          break;
        }
        default:
          FAIL();
      }

      ASSERT_OK(txn->Commit());
      delete txn;
    }
  };

  check_correctness_wrt_snapshot();
}

// This tests how write unprepared behaves during recovery when the DB crashes
// after a transaction has either been unprepared or prepared, and tests if
// the changes are correctly applied for prepared transactions if we decide to
// rollback/commit.
TEST_P(WriteUnpreparedTransactionTest, RecoveryTest) {
  WriteOptions write_options;
  write_options.disableWAL = false;
  TransactionOptions txn_options;
  std::vector<Transaction*> prepared_trans;
  options.disable_auto_compactions = true;

  enum Action { UNPREPARED, ROLLBACK, COMMIT };

  // batch_size of 1 causes writes to DB for every marker.
  for (size_t batch_size : {1, 1000000}) {
    txn_options.write_batch_flush_threshold = batch_size;
    for (bool empty : {true, false}) {
      for (Action a : {UNPREPARED, ROLLBACK, COMMIT}) {
        for (int num_batches = 1; num_batches < 10; num_batches++) {
          // Reset database.
          prepared_trans.clear();
          ASSERT_OK(ReOpen());
          if (!empty) {
            for (int i = 0; i < num_batches; i++) {
              ASSERT_OK(db->Put(WriteOptions(), "k" + std::to_string(i),
                                "before value" + std::to_string(i)));
            }
          }

          // Write num_batches unprepared batches.
          Transaction* txn = db->BeginTransaction(write_options, txn_options);
          WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
          ASSERT_OK(txn->SetName("xid"));
          for (int i = 0; i < num_batches; i++) {
            ASSERT_OK(
                txn->Put("k" + std::to_string(i), "value" + std::to_string(i)));
            if (txn_options.write_batch_flush_threshold == 1) {
              // WriteUnprepared will check write_batch_flush_threshold and
              // possibly flush before appending to the write batch. No flush
              // will happen at the first write because the batch is still
              // empty, so after k puts, there should be k-1 flushed batches.
              ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), i);
            } else {
              ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), 0);
            }
          }
          if (a == UNPREPARED) {
            DeleteWithoutRollback(txn);
          } else {
            ASSERT_OK(txn->Prepare());
            delete txn;
          }

          // Crash and run recovery code paths.
          ASSERT_NO_FATAL_FAILURE(SimulateCrash());
          ASSERT_OK(ReOpenNoDelete());
          assert(db != nullptr);

          db->GetAllPreparedTransactions(&prepared_trans);
          ASSERT_EQ(prepared_trans.size(), a == UNPREPARED ? 0 : 1);
          if (a == ROLLBACK) {
            ASSERT_OK(prepared_trans[0]->Rollback());
            delete prepared_trans[0];
          } else if (a == COMMIT) {
            ASSERT_OK(prepared_trans[0]->Commit());
            delete prepared_trans[0];
          }

          Iterator* iter = db->NewIterator(ReadOptions());
          ASSERT_OK(iter->status());
          iter->SeekToFirst();
          // Check that DB has before values.
          if (!empty || a == COMMIT) {
            for (int i = 0; i < num_batches; i++) {
              ASSERT_TRUE(iter->Valid());
              ASSERT_EQ(iter->key().ToString(), "k" + std::to_string(i));
              if (a == COMMIT) {
                ASSERT_EQ(iter->value().ToString(),
                          "value" + std::to_string(i));
              } else {
                ASSERT_EQ(iter->value().ToString(),
                          "before value" + std::to_string(i));
              }
              iter->Next();
            }
          }
          ASSERT_FALSE(iter->Valid());
          ASSERT_OK(iter->status());
          delete iter;
        }
      }
    }
  }
}

// Background: when DB open finds a transaction that has unprepared batches but
// no prepare marker, it rolls the transaction back. It writes one rollback
// batch for each unprepared batch, starting with the newest one. To build a
// rollback batch it reads the value that each key had before the transaction.
//
// This test makes that read fail after the first rollback batch is written, so
// DB open fails partway. The next DB open must still roll back every
// unprepared batch, so no value from the transaction becomes visible.
TEST_P(WriteUnpreparedTransactionTest, RecoveryRollbackWithReadError) {
  // Without a block cache, every Get() of a key in an SST file reads the file,
  // so the injected read error hits the old-value lookup.
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.disable_auto_compactions = true;
  ASSERT_OK(ReOpen());
  fault_fs->SetFileTypesExcludedFromFaultInjection({FileType::kInfoLogFile});

  // The old values live in an SST file, so the lookups read that file.
  ASSERT_OK(db->Put(WriteOptions(), "k0", "before0"));
  ASSERT_OK(db->Put(WriteOptions(), "k1", "before1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  // After the four writes, the DB has three unprepared batches: "k0" with
  // "value0", "k1", and "k0" with "value0b". "k2" is only in the write batch
  // of the transaction. "k0" is in the first and the last batch, so a rollback
  // that runs again must not read the rollback writes of the failed DB open
  // (see below).
  Transaction* txn = nullptr;
  ASSERT_NO_FATAL_FAILURE(BeginTxnWithOneWritePerBatch(&txn));
  ASSERT_OK(txn->Put("k0", "value0"));
  ASSERT_OK(txn->Put("k1", "value1"));
  ASSERT_OK(txn->Put("k0", "value0b"));
  ASSERT_OK(txn->Put("k2", "value2"));
  ASSERT_EQ(3, NumUnpreparedBatches(txn));
  DeleteWithoutRollback(txn);
  ASSERT_NO_FATAL_FAILURE(SimulateCrash());

  // Recovery first rolls back the batch with "value0b" and writes "value0" for
  // "k0". After that rollback batch is written, make all reads on this thread
  // fail, so the lookup of "k1" fails.
  int rollback_batches_written = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "WriteUnpreparedTxnDB::RollbackRecoveredTransaction:"
      "AfterWriteRollbackBatch",
      [&](void*) {
        if (++rollback_batches_written == 1) {
          fault_fs->SetThreadLocalErrorContext(FaultInjectionIOType::kRead,
                                               /*seed=*/0, /*one_in=*/1,
                                               /*retryable=*/false,
                                               /*has_data_loss=*/false);
          fault_fs->EnableThreadLocalErrorInjection(
              FaultInjectionIOType::kRead);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = ReOpenNoDelete();
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kRead);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  ASSERT_TRUE(FaultInjectionTestFS::IsInjectedError(s)) << s.ToString();
  ASSERT_EQ(1, rollback_batches_written);
  ASSERT_EQ(nullptr, db);

  // Open again without read errors. Recovery must roll back all three
  // unprepared batches again. The rollback of the oldest unprepared batch reads
  // only data older than that batch, so it restores "before0", not the "value0"
  // that the failed DB open wrote.
  ASSERT_OK(ReOpenAndCountRollbackBatches(&rollback_batches_written));
  ASSERT_EQ(3, rollback_batches_written);
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "k0", &value));
  ASSERT_EQ("before0", value);
  ASSERT_OK(db->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ("before1", value);
  ASSERT_EQ(Status::Code::kNotFound,
            db->Get(ReadOptions(), "k2", &value).code());

  // The completed rollback wrote the rollback marker, so the next DB open does
  // not find the transaction and writes no rollback batch.
  ASSERT_OK(ReOpenAndCountRollbackBatches(&rollback_batches_written));
  ASSERT_EQ(0, rollback_batches_written);
  ASSERT_OK(db->Get(ReadOptions(), "k0", &value));
  ASSERT_EQ("before0", value);
  ASSERT_OK(db->Get(ReadOptions(), "k1", &value));
  ASSERT_EQ("before1", value);
}

// With TransactionDBOptions::rollback_merge_operands set to false, recovery
// does not roll back merge operands. An unprepared batch that holds only a
// merge operand then has nothing to roll back. The test fixture sets the
// "stringappend" merge operator. The test covers two positions of the
// merge-only batch:
// - In the middle: recovery writes no rollback batch for it. Writing it would
//   break sequence number accounting: WriteImpl would allocate one sequence
//   number that the memtable inserter does not count, and a debug assert in
//   the write path catches that.
// - Oldest: its rollback batch is the last one, so recovery writes it with
//   only the rollback marker.
// In both cases the other unprepared batches must be rolled back, the merge
// operand must stay, the rollback marker must be written, and a write after
// recovery must survive a reopen.
TEST_P(WriteUnpreparedTransactionTest, RecoveryRollbackWithMergeOnlyBatch) {
  txn_db_options.rollback_merge_operands = false;

  struct TxnWrite {
    bool is_merge;
    const char* key;
    const char* value;
  };
  struct TestCase {
    const char* name;
    // Each write except the last becomes one unprepared batch. The last write
    // stays in the write batch of the transaction.
    std::vector<TxnWrite> writes;
    int expected_rollback_batches;
  };
  const std::vector<TestCase> test_cases = {
      // Rollback batches for "k2" and "k0"; none for the merge-only batch.
      {"merge-only batch in the middle",
       {{false, "k0", "value0"},
        {true, "k1", "operand1"},
        {false, "k2", "value2"},
        {false, "k3", "value3"}},
       2},
      // Rollback batches for "k2", "k0", and one with only the rollback
      // marker for the merge-only batch.
      {"merge-only batch oldest",
       {{true, "k1", "operand1"},
        {false, "k0", "value0"},
        {false, "k2", "value2"},
        {false, "k3", "value3"}},
       3},
  };

  for (const TestCase& tc : test_cases) {
    SCOPED_TRACE(tc.name);
    ASSERT_OK(ReOpen());
    ASSERT_OK(db->Put(WriteOptions(), "k0", "before0"));
    ASSERT_OK(db->Put(WriteOptions(), "k2", "before2"));

    Transaction* txn = nullptr;
    ASSERT_NO_FATAL_FAILURE(BeginTxnWithOneWritePerBatch(&txn));
    for (const TxnWrite& w : tc.writes) {
      if (w.is_merge) {
        ASSERT_OK(txn->Merge(w.key, w.value));
      } else {
        ASSERT_OK(txn->Put(w.key, w.value));
      }
    }
    ASSERT_EQ(tc.writes.size() - 1, NumUnpreparedBatches(txn));
    DeleteWithoutRollback(txn);
    ASSERT_NO_FATAL_FAILURE(SimulateCrash());

    int rollback_batches_written = 0;
    ASSERT_OK(ReOpenAndCountRollbackBatches(&rollback_batches_written));
    ASSERT_EQ(tc.expected_rollback_batches, rollback_batches_written);

    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), "k0", &value));
    ASSERT_EQ("before0", value);
    ASSERT_OK(db->Get(ReadOptions(), "k2", &value));
    ASSERT_EQ("before2", value);
    ASSERT_EQ(Status::Code::kNotFound,
              db->Get(ReadOptions(), "k3", &value).code());
    // With rollback_merge_operands set to false, the merge operand stays.
    // "k1" had no value before the transaction, so it now holds only the
    // operand.
    ASSERT_OK(db->Get(ReadOptions(), "k1", &value));
    ASSERT_EQ("operand1", value);

    // The completed rollback wrote the rollback marker, so the next DB open
    // does not find the transaction and writes no rollback batch. A write
    // after recovery survives that reopen, and the rollback holds.
    ASSERT_OK(db->Put(WriteOptions(), "k4", "after4"));
    ASSERT_OK(ReOpenAndCountRollbackBatches(&rollback_batches_written));
    ASSERT_EQ(0, rollback_batches_written);
    ASSERT_OK(db->Get(ReadOptions(), "k0", &value));
    ASSERT_EQ("before0", value);
    ASSERT_OK(db->Get(ReadOptions(), "k2", &value));
    ASSERT_EQ("before2", value);
    ASSERT_OK(db->Get(ReadOptions(), "k1", &value));
    ASSERT_EQ("operand1", value);
    ASSERT_OK(db->Get(ReadOptions(), "k4", &value));
    ASSERT_EQ("after4", value);
  }
}

// Basic test to see that unprepared batch gets written to DB when batch size
// is exceeded. It also does some basic checks to see if commit/rollback works
// as expected for write unprepared.
TEST_P(WriteUnpreparedTransactionTest, UnpreparedBatch) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  const int kNumKeys = 10;

  // batch_size of 1 causes writes to DB for every marker.
  for (size_t batch_size : {1, 1000000}) {
    txn_options.write_batch_flush_threshold = batch_size;
    for (bool prepare : {false, true}) {
      for (bool commit : {false, true}) {
        ASSERT_OK(ReOpen());
        Transaction* txn = db->BeginTransaction(write_options, txn_options);
        WriteUnpreparedTxn* wup_txn = dynamic_cast<WriteUnpreparedTxn*>(txn);
        ASSERT_OK(txn->SetName("xid"));

        for (int i = 0; i < kNumKeys; i++) {
          ASSERT_OK(txn->Put("k" + std::to_string(i), "v" + std::to_string(i)));
          if (txn_options.write_batch_flush_threshold == 1) {
            // WriteUnprepared will check write_batch_flush_threshold and
            // possibly flush before appending to the write batch. No flush will
            // happen at the first write because the batch is still empty, so
            // after k puts, there should be k-1 flushed batches.
            ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), i);
          } else {
            ASSERT_EQ(wup_txn->GetUnpreparedSequenceNumbers().size(), 0);
          }
        }

        if (prepare) {
          ASSERT_OK(txn->Prepare());
        }

        Iterator* iter = db->NewIterator(ReadOptions());
        ASSERT_OK(iter->status());
        iter->SeekToFirst();
        assert(!iter->Valid());
        ASSERT_FALSE(iter->Valid());
        ASSERT_OK(iter->status());
        delete iter;

        if (commit) {
          ASSERT_OK(txn->Commit());
        } else {
          ASSERT_OK(txn->Rollback());
        }
        delete txn;

        iter = db->NewIterator(ReadOptions());
        ASSERT_OK(iter->status());
        iter->SeekToFirst();

        for (int i = 0; i < (commit ? kNumKeys : 0); i++) {
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(iter->key().ToString(), "k" + std::to_string(i));
          ASSERT_EQ(iter->value().ToString(), "v" + std::to_string(i));
          iter->Next();
        }
        ASSERT_FALSE(iter->Valid());
        ASSERT_OK(iter->status());
        delete iter;
      }
    }
  }
}

// Test whether logs containing unprepared/prepared batches are kept even
// after memtable finishes flushing, and whether they are removed when
// transaction commits/aborts.
//
// TODO(lth): Merge with TransactionTest/TwoPhaseLogRollingTest tests.
TEST_P(WriteUnpreparedTransactionTest, MarkLogWithPrepSection) {
  WriteOptions write_options;
  TransactionOptions txn_options;
  // batch_size of 1 causes writes to DB for every marker.
  txn_options.write_batch_flush_threshold = 1;
  const int kNumKeys = 10;

  WriteOptions wopts;
  wopts.sync = true;

  for (bool prepare : {false, true}) {
    for (bool commit : {false, true}) {
      ASSERT_OK(ReOpen());
      auto wup_db = dynamic_cast<WriteUnpreparedTxnDB*>(db);
      auto db_impl = wup_db->db_impl_;

      Transaction* txn1 = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn1->SetName("xid1"));

      Transaction* txn2 = db->BeginTransaction(write_options, txn_options);
      ASSERT_OK(txn2->SetName("xid2"));

      // Spread this transaction across multiple log files.
      for (int i = 0; i < kNumKeys; i++) {
        ASSERT_OK(txn1->Put("k1" + std::to_string(i), "v" + std::to_string(i)));
        if (i >= kNumKeys / 2) {
          ASSERT_OK(
              txn2->Put("k2" + std::to_string(i), "v" + std::to_string(i)));
        }

        if (i > 0) {
          ASSERT_OK(db_impl->TEST_SwitchWAL());
        }
      }

      ASSERT_GT(txn1->GetLogNumber(), 0);
      ASSERT_GT(txn2->GetLogNumber(), 0);

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn1->GetLogNumber());
      ASSERT_GT(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());

      if (prepare) {
        ASSERT_OK(txn1->Prepare());
        ASSERT_OK(txn2->Prepare());
      }

      ASSERT_GE(db_impl->TEST_LogfileNumber(), txn1->GetLogNumber());
      ASSERT_GE(db_impl->TEST_LogfileNumber(), txn2->GetLogNumber());

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn1->GetLogNumber());
      if (commit) {
        ASSERT_OK(txn1->Commit());
      } else {
        ASSERT_OK(txn1->Rollback());
      }

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(),
                txn2->GetLogNumber());

      if (commit) {
        ASSERT_OK(txn2->Commit());
      } else {
        ASSERT_OK(txn2->Rollback());
      }

      ASSERT_EQ(db_impl->TEST_FindMinLogContainingOutstandingPrep(), 0);

      delete txn1;
      delete txn2;
    }
  }
}

TEST_P(WriteUnpreparedTransactionTest, NoSnapshotWrite) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  Transaction* txn = db->BeginTransaction(woptions, txn_options);

  // Do some writes with no snapshot
  ASSERT_OK(txn->Put("a", "a"));
  ASSERT_OK(txn->Put("b", "b"));
  ASSERT_OK(txn->Put("c", "c"));

  // Test that it is still possible to create iterators after writes with no
  // snapshot, if iterator snapshot is fresh enough.
  ReadOptions roptions;
  auto iter = txn->GetIterator(roptions);
  ASSERT_OK(iter->status());
  int keys = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev(), keys++) {
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key().ToString(), iter->value().ToString());
  }
  ASSERT_EQ(keys, 3);
  ASSERT_OK(iter->status());

  delete iter;
  delete txn;
}

// Entity reads must see the transaction's own writes after they have been
// flushed to the DB as an unprepared batch, while other transactions must not.
TEST_P(WriteUnpreparedTransactionTest, GetEntityReadYourOwnWrite) {
  WriteOptions woptions;
  ASSERT_OK(db->Put(woptions, "a", "base"));

  TransactionOptions txn_options;
  // The second Put writes the first one to the DB as an unprepared batch.
  txn_options.write_batch_flush_threshold = 1;
  std::unique_ptr<Transaction> writer(
      db->BeginTransaction(woptions, txn_options));
  ASSERT_NE(writer, nullptr);
  writer->SetSnapshot();
  ASSERT_OK(writer->Put("a", "own"));
  ASSERT_OK(writer->Put("b", "own"));

  std::unique_ptr<Transaction> reader(db->BeginTransaction(woptions));
  ASSERT_NE(reader, nullptr);

  ReadOptions snapshot_read_options;
  snapshot_read_options.snapshot = writer->GetSnapshot();
  EXPECT_NO_FATAL_FAILURE(
      VerifyTxnGetEntity(writer.get(), snapshot_read_options, "a", "own"));
  EXPECT_NO_FATAL_FAILURE(
      VerifyTxnGetEntity(writer.get(), ReadOptions(), "a", "own"));
  EXPECT_NO_FATAL_FAILURE(
      VerifyTxnGetEntity(reader.get(), ReadOptions(), "a", "base"));
  EXPECT_NO_FATAL_FAILURE(VerifyTxnGetEntityForUpdate(
      writer.get(), snapshot_read_options, "a", "own"));

  ASSERT_OK(writer->Rollback());
}

// Test whether write to a transaction while iterating is supported.
TEST_P(WriteUnpreparedTransactionTest, IterateAndWrite) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  enum Action { DO_DELETE, DO_UPDATE };

  for (Action a : {DO_DELETE, DO_UPDATE}) {
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(db->Put(woptions, std::to_string(i), std::to_string(i)));
    }

    Transaction* txn = db->BeginTransaction(woptions, txn_options);
    // write_batch_ now contains 1 key.
    ASSERT_OK(txn->Put("9", "a"));

    ReadOptions roptions;
    auto iter = txn->GetIterator(roptions);
    ASSERT_OK(iter->status());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      if (iter->key() == "9") {
        ASSERT_EQ(iter->value().ToString(), "a");
      } else {
        ASSERT_EQ(iter->key().ToString(), iter->value().ToString());
      }

      if (a == DO_DELETE) {
        ASSERT_OK(txn->Delete(iter->key()));
      } else {
        ASSERT_OK(txn->Put(iter->key(), "b"));
      }
    }
    ASSERT_OK(iter->status());

    delete iter;
    ASSERT_OK(txn->Commit());

    iter = db->NewIterator(roptions);
    ASSERT_OK(iter->status());
    if (a == DO_DELETE) {
      // Check that db is empty.
      iter->SeekToFirst();
      ASSERT_FALSE(iter->Valid());
    } else {
      int keys = 0;
      // Check that all values are updated to b.
      for (iter->SeekToFirst(); iter->Valid(); iter->Next(), keys++) {
        ASSERT_OK(iter->status());
        ASSERT_EQ(iter->value().ToString(), "b");
      }
      ASSERT_EQ(keys, 100);
    }
    ASSERT_OK(iter->status());

    delete iter;
    delete txn;
  }
}

// Test that using an iterator after transaction clear is not supported
TEST_P(WriteUnpreparedTransactionTest, IterateAfterClear) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  enum Action { kCommit, kRollback };

  for (Action a : {kCommit, kRollback}) {
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(db->Put(woptions, std::to_string(i), std::to_string(i)));
    }

    Transaction* txn = db->BeginTransaction(woptions, txn_options);
    ASSERT_OK(txn->Put("9", "a"));

    ReadOptions roptions;
    auto iter1 = txn->GetIterator(roptions);
    auto iter2 = txn->GetIterator(roptions);
    iter1->SeekToFirst();
    iter2->Seek("9");

    // Check that iterators are valid before transaction finishes.
    ASSERT_TRUE(iter1->Valid());
    ASSERT_TRUE(iter2->Valid());
    ASSERT_OK(iter1->status());
    ASSERT_OK(iter2->status());

    if (a == kCommit) {
      ASSERT_OK(txn->Commit());
    } else {
      ASSERT_OK(txn->Rollback());
    }

    // Check that iterators are invalidated after transaction finishes.
    ASSERT_FALSE(iter1->Valid());
    ASSERT_FALSE(iter2->Valid());
    ASSERT_TRUE(iter1->status().IsInvalidArgument());
    ASSERT_TRUE(iter2->status().IsInvalidArgument());

    delete iter1;
    delete iter2;
    delete txn;
  }
}

TEST_P(WriteUnpreparedTransactionTest, SavePoint) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  Transaction* txn = db->BeginTransaction(woptions, txn_options);
  txn->SetSavePoint();
  ASSERT_OK(txn->Put("a", "a"));
  ASSERT_OK(txn->Put("b", "b"));
  ASSERT_OK(txn->Commit());

  ReadOptions roptions;
  std::string value;
  ASSERT_OK(txn->Get(roptions, "a", &value));
  ASSERT_EQ(value, "a");
  ASSERT_OK(txn->Get(roptions, "b", &value));
  ASSERT_EQ(value, "b");
  delete txn;
}

TEST_P(WriteUnpreparedTransactionTest, UntrackedKeys) {
  WriteOptions woptions;
  TransactionOptions txn_options;
  txn_options.write_batch_flush_threshold = 1;

  Transaction* txn = db->BeginTransaction(woptions, txn_options);
  auto wb = txn->GetWriteBatch()->GetWriteBatch();
  ASSERT_OK(txn->Put("a", "a"));
  ASSERT_OK(wb->Put("a_untrack", "a_untrack"));
  txn->SetSavePoint();
  ASSERT_OK(txn->Put("b", "b"));
  ASSERT_OK(txn->Put("b_untrack", "b_untrack"));

  ReadOptions roptions;
  std::string value;
  ASSERT_OK(txn->Get(roptions, "a", &value));
  ASSERT_EQ(value, "a");
  ASSERT_OK(txn->Get(roptions, "a_untrack", &value));
  ASSERT_EQ(value, "a_untrack");
  ASSERT_OK(txn->Get(roptions, "b", &value));
  ASSERT_EQ(value, "b");
  ASSERT_OK(txn->Get(roptions, "b_untrack", &value));
  ASSERT_EQ(value, "b_untrack");

  // b and b_untrack should be rolled back.
  ASSERT_OK(txn->RollbackToSavePoint());
  ASSERT_OK(txn->Get(roptions, "a", &value));
  ASSERT_EQ(value, "a");
  ASSERT_OK(txn->Get(roptions, "a_untrack", &value));
  ASSERT_EQ(value, "a_untrack");
  auto s = txn->Get(roptions, "b", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "b_untrack", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Everything should be rolled back.
  ASSERT_OK(txn->Rollback());
  s = txn->Get(roptions, "a", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "a_untrack", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "b", &value);
  ASSERT_TRUE(s.IsNotFound());
  s = txn->Get(roptions, "b_untrack", &value);
  ASSERT_TRUE(s.IsNotFound());

  delete txn;
}

namespace {
std::vector<std::string> VerifyIterator(Iterator* iter, bool forward) {
  std::vector<std::string> keys;
  if (forward) {
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
  } else {
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      keys.push_back(iter->key().ToString());
    }
    std::reverse(keys.begin(), keys.end());
  }
  return keys;
}
}  // namespace

// Multiple unprepared batches write to the memtable with different seqno
// ranges tracked in unprep_seqs. Committed tombstones (c-g) exist, and the
// transaction's own Delete("h") extends the run with an uncommitted entry.
// Write-unprepared iterators widen their visible seqno to include own
// unprepared writes, so synthesizing a range tombstone for a run containing
// one of those deletes would insert it at a seqno that can cover uncommitted
// entries. Verify insertion is blocked and data remains correct after commit.
TEST_P(WriteUnpreparedTransactionTest, RangeTombstoneMultipleBatchesAndCommit) {
  // Test two scenarios:
  // 1) Txn Delete extends the END of a committed tombstone run.
  // 2) Txn Delete in the MIDDLE of a committed tombstone run.
  // Both should block insertion because the visible seqno is widened by the
  // transaction's own unprepared writes.
  for (bool middle_tombstone : {false, true}) {
    SCOPED_TRACE(middle_tombstone ? "middle tombstone" : "end tombstone");
    for (bool forward : {true, false}) {
      SCOPED_TRACE(forward ? "forward" : "reverse");
      options.min_tombstones_for_range_conversion = 4;
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      ASSERT_OK(ReOpenNoDelete());

      for (char c = 'a'; c <= 'j'; c++) {
        ASSERT_OK(db->Put(WriteOptions(), std::string(1, c), "val"));
      }
      ASSERT_OK(db->Flush(FlushOptions()));

      if (middle_tombstone) {
        // Committed deletions c, d, f, g (with gap at e).
        for (char c : {'c', 'd', 'f', 'g'}) {
          ASSERT_OK(db->Delete(WriteOptions(), std::string(1, c)));
        }
      } else {
        // Committed point deletions c-g (5 contiguous, above threshold).
        for (char c = 'c'; c <= 'g'; c++) {
          ASSERT_OK(db->Delete(WriteOptions(), std::string(1, c)));
        }
      }

      TransactionOptions txn_options;
      // flush_threshold=1 forces each write to create a separate unprepared
      // batch in the memtable, each with its own seqno range in unprep_seqs.
      txn_options.write_batch_flush_threshold = 1;
      std::unique_ptr<Transaction> txn(
          db->BeginTransaction(WriteOptions(), txn_options));
      ASSERT_NE(txn, nullptr);

      // Multiple unprepared batches.
      ASSERT_OK(txn->Put("b", "txn_b"));  // batch 1, bounds start of run
      if (middle_tombstone) {
        // Txn Delete("e") fills the gap in the middle of committed deletes
        // c, d, [e], f, g -- making a contiguous run of 5 that contains an
        // uncommitted seqno in the middle.
        ASSERT_OK(txn->Delete("e"));
      } else {
        // Txn Delete("h") extends the committed run [c, g] at the end.
        ASSERT_OK(txn->Delete("h"));
      }
      ASSERT_OK(txn->Put("i", "txn_i"));
      ASSERT_OK(txn->Put("z", "txn_z"));

      // The txn Delete (whether at "e" or "h") is an own unprepared write.
      // The iterator exposes it by widening its visible seqno, so a synthesized
      // tombstone for the whole run must be discarded.
      {
        ReadOptions ro;
        std::unique_ptr<Iterator> iter(txn->GetIterator(ro));
        if (middle_tombstone) {
          // c-g all deleted (c,d committed, e txn, f,g committed)
          ASSERT_EQ(VerifyIterator(iter.get(), forward),
                    (std::vector<std::string>{"a", "b", "h", "i", "j", "z"}));
        } else {
          // c-h all deleted (c-g committed, h txn)
          ASSERT_EQ(VerifyIterator(iter.get(), forward),
                    (std::vector<std::string>{"a", "b", "i", "j", "z"}));
        }
        ASSERT_OK(iter->status());
      }
      ASSERT_EQ(options.statistics->getTickerCount(
                    READ_PATH_RANGE_TOMBSTONES_INSERTED),
                0u);

      ASSERT_OK(txn->Commit());

      // Verify data correctness after commit.
      std::string value;
      ASSERT_OK(db->Get(ReadOptions(), "b", &value));
      ASSERT_EQ(value, "txn_b");
      char last_del = middle_tombstone ? 'g' : 'h';
      for (char c = 'c'; c <= last_del; c++) {
        ASSERT_TRUE(
            db->Get(ReadOptions(), std::string(1, c), &value).IsNotFound());
      }
    }
  }
}

// WriteUnprepared computes max_visible_seq as max(max_unprepared_seqno,
// snapshot_seq). With a snapshot taken before deletions and multiple unprepared
// batches after, the iterator's visible range extends beyond the snapshot to
// include both committed deletes and own writes. The committed deletes are
// still visible to the iterator, but a synthesized range tombstone must be
// discarded because it would be inserted at the widened visible seqno rather
// than the original snapshot seqno.
TEST_P(WriteUnpreparedTransactionTest,
       RangeTombstoneCalcMaxVisibleSeqExtendedVisibility) {
  for (bool forward : {true, false}) {
    SCOPED_TRACE(forward ? "forward" : "reverse");
    options.min_tombstones_for_range_conversion = 4;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    ASSERT_OK(ReOpenNoDelete());

    std::vector<std::pair<std::string, std::string>> inserted_ranges;
    SyncPoint::GetInstance()->SetCallBack(
        "MemTable::AddLogicallyRedundantRangeTombstone:AddRange",
        [&](void* arg) {
          auto* range = static_cast<std::pair<Slice, Slice>*>(arg);
          inserted_ranges.emplace_back(range->first.ToString(),
                                       range->second.ToString());
        });
    SyncPoint::GetInstance()->EnableProcessing();

    for (char c = 'a'; c <= 'j'; c++) {
      ASSERT_OK(db->Put(WriteOptions(), std::string(1, c), "val"));
    }
    ASSERT_OK(db->Flush(FlushOptions()));

    // Take a snapshot before the deletions.
    const Snapshot* snap = db->GetSnapshot();

    // Committed point deletions c-g (after the snapshot).
    for (char c = 'c'; c <= 'g'; c++) {
      ASSERT_OK(db->Delete(WriteOptions(), std::string(1, c)));
    }

    TransactionOptions txn_options;
    txn_options.write_batch_flush_threshold = 1;
    std::unique_ptr<Transaction> txn(
        db->BeginTransaction(WriteOptions(), txn_options));
    ASSERT_NE(txn, nullptr);
    txn->SetSnapshot();

    // Multiple unprepared writes -- these get seqnos beyond the snapshot.
    // CalcMaxVisibleSeq returns max(last_unprep_seqno, snapshot_seq), so
    // the committed deletions (seqno between snap and unprep) are visible.
    ASSERT_OK(txn->Put("x", "txn_x"));
    ASSERT_OK(txn->Put("y", "txn_y"));

    {
      ReadOptions ro;
      ro.snapshot = txn->GetSnapshot();
      std::unique_ptr<Iterator> iter(txn->GetIterator(ro));
      // c-g deleted, own writes x,y visible: a, b, h, i, j, x, y
      ASSERT_EQ(VerifyIterator(iter.get(), forward),
                (std::vector<std::string>{"a", "b", "h", "i", "j", "x", "y"}));
      ASSERT_OK(iter->status());
    }
    // The committed tombstones are visible to the iterator, but insertion is
    // discarded because the iterator's visible seqno was widened by its own
    // unprepared writes.
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        0u);
    ASSERT_EQ(options.statistics->getTickerCount(
                  READ_PATH_RANGE_TOMBSTONES_DISCARDED),
              1u);
    ASSERT_EQ(inserted_ranges.size(), 0);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(txn->Commit());
    db->ReleaseSnapshot(snap);
  }
}

// The transaction issues its own uncommitted contiguous Deletes (j-n) forming
// a tombstone run visible to its iterator, alongside committed tombstones
// (c-g). Because the iterator's visible seqno is widened to include those own
// writes, both candidate synthesized tombstones are discarded. After rollback,
// own Deletes and Puts are undone while committed deletes remain.
TEST_P(WriteUnpreparedTransactionTest, RangeTombstoneOwnDeletionsAndRollback) {
  for (bool forward : {true, false}) {
    SCOPED_TRACE(forward ? "forward" : "reverse");
    options.min_tombstones_for_range_conversion = 4;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    ASSERT_OK(ReOpenNoDelete());

    std::vector<std::pair<std::string, std::string>> inserted_ranges;
    SyncPoint::GetInstance()->SetCallBack(
        "MemTable::AddLogicallyRedundantRangeTombstone:AddRange",
        [&](void* arg) {
          auto* range = static_cast<std::pair<Slice, Slice>*>(arg);
          inserted_ranges.emplace_back(range->first.ToString(),
                                       range->second.ToString());
        });
    SyncPoint::GetInstance()->EnableProcessing();

    for (char c = 'a'; c <= 'p'; c++) {
      ASSERT_OK(db->Put(WriteOptions(), std::string(1, c), "val"));
    }
    ASSERT_OK(db->Flush(FlushOptions()));

    // Committed point deletions c-g (5 contiguous, above threshold).
    for (char c = 'c'; c <= 'g'; c++) {
      ASSERT_OK(db->Delete(WriteOptions(), std::string(1, c)));
    }

    TransactionOptions txn_options;
    txn_options.write_batch_flush_threshold = 1;
    std::unique_ptr<Transaction> txn(
        db->BeginTransaction(WriteOptions(), txn_options));
    ASSERT_NE(txn, nullptr);

    // Transaction's own contiguous deletions j-n (5 keys, above threshold).
    // Each Delete creates a separate unprepared batch (flush_threshold=1).
    // These are uncommitted but visible to the txn's iterator via
    // IsVisibleFullCheck + unprep_seqs.
    for (char c = 'j'; c <= 'n'; c++) {
      ASSERT_OK(txn->Delete(std::string(1, c)));
    }

    // Transaction also writes Puts that will be rolled back.
    ASSERT_OK(txn->Put("a", "txn_a"));
    ASSERT_OK(txn->Put("p", "txn_p"));

    // Iterator sees: committed deletes (c-g hidden), own deletes (j-n hidden),
    // own Puts (a=txn_a, p=txn_p), and remaining committed keys (b, h, i, o).
    {
      ReadOptions ro;
      std::unique_ptr<Iterator> iter(txn->GetIterator(ro));
      ASSERT_EQ(VerifyIterator(iter.get(), forward),
                (std::vector<std::string>{"a", "b", "h", "i", "o", "p"}));
      ASSERT_OK(iter->status());
    }
    // Both visible tombstone runs are discarded because the iterator's visible
    // seqno includes the transaction's own unprepared writes.
    ASSERT_EQ(
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
        0u);
    ASSERT_EQ(options.statistics->getTickerCount(
                  READ_PATH_RANGE_TOMBSTONES_DISCARDED),
              2u);
    ASSERT_EQ(inserted_ranges.size(), 0);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    // Rollback -- own Deletes and Puts are undone.
    ASSERT_OK(txn->Rollback());

    // After rollback: committed deletes c-g remain, own deletes j-n are
    // undone, own Puts a/p are undone.
    {
      ReadOptions ro;
      std::unique_ptr<Iterator> iter(db->NewIterator(ro));
      // c-g deleted (committed), j-n restored, a/p restored to originals
      ASSERT_EQ(VerifyIterator(iter.get(), forward),
                (std::vector<std::string>{"a", "b", "h", "i", "j", "k", "l",
                                          "m", "n", "o", "p"}));
      ASSERT_OK(iter->status());
    }

    // Verify rolled-back values are originals.
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), "a", &value));
    ASSERT_EQ(value, "val");
    ASSERT_OK(db->Get(ReadOptions(), "j", &value));
    ASSERT_EQ(value, "val");
    ASSERT_OK(db->Get(ReadOptions(), "p", &value));
    ASSERT_EQ(value, "val");
  }
}

TEST_P(WriteUnpreparedTransactionTest,
       RollbackPreparedAfterCommitWriteFailure) {
  // Verify that a WriteUnprepared transaction can be rolled back after a
  // commit write failure. Previously, CommitInternal() would clear
  // unprep_seqs_ even on failure, making subsequent Rollback() hit
  // assertion `unprep_seqs.size() > 0`.
  options.max_bgerror_resume_count = 0;
  ASSERT_OK(ReOpen());

  // Use fault injection to cause commit write to fail.
  fault_fs->SetFileTypesExcludedFromFaultInjection({FileType::kInfoLogFile});

  WriteOptions woptions;
  TransactionOptions txn_options;
  // Set a low flush threshold to trigger unprepared batches being written to
  // DB, populating unprep_seqs_.
  txn_options.write_batch_flush_threshold = 1;
  std::unique_ptr<Transaction> txn(db->BeginTransaction(woptions, txn_options));
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("wup_commit_fail_rollback"));
  ASSERT_OK(txn->Put(Slice("key1"), Slice("value1")));
  // The flush threshold of 1 should cause the batch to be flushed as
  // unprepared, populating unprep_seqs_.
  ASSERT_OK(txn->Put(Slice("key2"), Slice("value2")));
  ASSERT_OK(txn->Prepare());

  // Inject a write error so that Commit() fails.
  fault_fs->SetThreadLocalErrorContext(FaultInjectionIOType::kWrite,
                                       /*seed=*/0, /*one_in=*/1,
                                       /*retryable=*/true,
                                       /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
  const Status commit_s = txn->Commit();
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  ASSERT_NOK(commit_s);
  ASSERT_TRUE(FaultInjectionTestFS::IsInjectedError(commit_s))
      << commit_s.ToString();
  fault_fs->GetAndResetInjectedThreadLocalErrorCount(
      FaultInjectionIOType::kWrite);

  // Resume the DB to clear the error state.
  ASSERT_OK(db->Resume());

  // Rollback should succeed without hitting the assertion failure.
  ASSERT_OK(txn->Rollback());
  txn.reset();

  // Verify the DB is usable after rollback.
  ASSERT_OK(ReOpenNoDelete());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
