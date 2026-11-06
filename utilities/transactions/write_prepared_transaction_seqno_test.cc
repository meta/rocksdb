//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Test coverage for sequence numbers during error recovery with
// WritePrepared/WriteUnprepared TransactionDB and two_write_queues=true.
//
// The fix keeps LastSequence() caught up to LastAllocatedSequence() at recovery
// fences that create memtables/WALs. Without that sync, WAL-only writes on the
// nonmem queue can leave recovery using a stale LastSequence(), which later
// surfaces as "sequence number going backwards" corruption.

#include <atomic>
#include <cassert>
#include <memory>
#include <string>
#include <thread>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/version_set.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

namespace {

template <typename T>
T& AssertNotNull(T* ptr) {
  assert(ptr != nullptr);
  return *ptr;
}

}  // namespace

class WritePreparedTransactionSeqnoTest : public ::testing::Test {
 public:
  WritePreparedTransactionSeqnoTest()
      : db_(nullptr),
        special_env_(Env::Default()),
        fault_fs_(new FaultInjectionTestFS(FileSystem::Default())),
        env_(new CompositeEnvWrapper(&special_env_, fault_fs_)) {
    options_.create_if_missing = true;
    options_.max_write_buffer_number = 2;
    options_.write_buffer_size = 4 * 1024;
    options_.level0_file_num_compaction_trigger = 2;
    options_.env = env_.get();
    // Use two_write_queues which is typical for WritePrepared
    options_.two_write_queues = true;
    // Enable auto recovery from retryable errors
    options_.max_bgerror_resume_count = 2;
    options_.bgerror_resume_retry_interval = 100000;  // 100ms

    dbname_ = test::PerThreadDBPath("write_prepared_seqno_test");
    EXPECT_OK(DestroyDB(dbname_, options_));

    txn_db_options_.transaction_lock_timeout = 0;
    txn_db_options_.default_lock_timeout = 0;
    txn_db_options_.write_policy = TxnDBWritePolicy::WRITE_PREPARED;
  }

  ~WritePreparedTransactionSeqnoTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (db_) {
      for (auto h : handles_) {
        if (h) {
          EXPECT_OK(db_->DestroyColumnFamilyHandle(h));
        }
      }
      handles_.clear();
      delete db_;
      db_ = nullptr;
    }
  }

  Status Open() {
    return TransactionDB::Open(options_, txn_db_options_, dbname_, &db_);
  }

  void Close() {
    for (auto h : handles_) {
      if (h) {
        EXPECT_OK(db_->DestroyColumnFamilyHandle(h));
      }
    }
    handles_.clear();
    delete db_;
    db_ = nullptr;
  }

  DBImpl* dbimpl() { return static_cast_with_check<DBImpl>(db_->GetRootDB()); }

 protected:
  TransactionDB* db_;
  SpecialEnv special_env_;
  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> env_;
  std::string dbname_;
  Options options_;
  TransactionDBOptions txn_db_options_;
  std::vector<ColumnFamilyHandle*> handles_;
};

// Regression test for a WriteUnprepared sequence-number race during error
// recovery. The test pauses a prepare after it has allocated and ingested its
// sequence range but before it publishes that range with SetLastSequence().
// While the prepare is paused, a WAL-only commit on the nonmem write queue
// advances LastAllocatedSequence(). ResumeImpl() must enter both write queues
// before SyncLastSequenceWithAllocated(); otherwise recovery can publish the
// newer allocation first, and the paused prepare later trips the
// s >= last_sequence_ assertion in SetLastSequence().
TEST_F(WritePreparedTransactionSeqnoTest,
       WriteUnpreparedInFlightPrepareDoesNotRaceWithRecoverySeqnoSync) {
  // WRITE_UNPREPARED exercises the split path where Prepare writes data and
  // Commit can later write only a WAL marker on the nonmem write queue.
  txn_db_options_.write_policy = TxnDBWritePolicy::WRITE_UNPREPARED;
  ASSERT_OK(Open());
  TransactionDB& db = AssertNotNull(db_);
  DBImpl& db_impl =
      AssertNotNull(static_cast_with_check<DBImpl>(db.GetRootDB()));
  VersionSet& versions = AssertNotNull(db_impl.GetVersionSet());
  SyncPoint& sync_point = AssertNotNull(SyncPoint::GetInstance());

  WriteOptions write_opts;
  TransactionOptions txn_opts;
  // Prepare a transaction whose later WAL-only commit will advance
  // LastAllocatedSequence() while another prepare is paused.
  std::unique_ptr<Transaction> wal_only_commit_txn(
      db.BeginTransaction(write_opts, txn_opts));
  Transaction& wal_only_commit_txn_ref =
      AssertNotNull(wal_only_commit_txn.get());
  ASSERT_OK(wal_only_commit_txn_ref.SetName("wal_only_commit_txn"));
  ASSERT_OK(wal_only_commit_txn_ref.Put("commit_key", "commit_value"));
  ASSERT_OK(wal_only_commit_txn_ref.Prepare());

  // Pause the second prepare after WBWI ingestion and before it publishes its
  // allocated sequence range with SetLastSequence().
  const std::string test_prefix =
      "WriteUnpreparedInFlightPrepareDoesNotRaceWithRecoverySeqnoSync";
  sync_point.LoadDependency(
      {{"DBImpl::WriteImpl:AfterWBWIIngestBeforeSetLastSequence:pause",
        test_prefix + ":writer_paused"},
       {test_prefix + ":release_writer",
        "DBImpl::WriteImpl:AfterWBWIIngestBeforeSetLastSequence:resume"}});

  std::atomic<bool> release_writer_called{false};
  std::atomic<bool> recovery_waited_for_writer{false};
  auto release_writer = [&]() {
    if (!release_writer_called.exchange(true)) {
      sync_point.Process(test_prefix + ":release_writer");
    }
  };
  // The fixed path reaches EnterUnbatched() and releases the paused writer only
  // after recovery is waiting for in-flight writes. The AfterSyncSeq callback
  // prevents a hang on the buggy path, where recovery syncs without waiting.
  sync_point.SetCallBack("WriteThread::EnterUnbatched:Wait", [&](void*) {
    recovery_waited_for_writer.store(true);
    release_writer();
  });
  sync_point.SetCallBack("DBImpl::ResumeImpl:AfterSyncSeq",
                         [&](void*) { release_writer(); });
  sync_point.EnableProcessing();

  Status blocked_prepare_status;
  Transaction* blocked_prepare_txn = nullptr;
  // This prepare owns an allocated-but-not-yet-published sequence range while
  // it is paused at the sync point above.
  std::thread blocked_prepare([&]() {
    blocked_prepare_txn = db.BeginTransaction(write_opts, txn_opts);
    if (blocked_prepare_txn == nullptr) {
      blocked_prepare_status =
          Status::Aborted("failed to create blocked prepare transaction");
      return;
    }
    blocked_prepare_status =
        blocked_prepare_txn->SetName("blocked_prepare_txn");
    if (blocked_prepare_status.ok()) {
      blocked_prepare_status =
          blocked_prepare_txn->Put("blocked_key", "blocked_value");
    }
    if (blocked_prepare_status.ok()) {
      blocked_prepare_status = blocked_prepare_txn->Prepare();
    }
  });

  // Wait until the vulnerable window is open: allocation has advanced, but
  // LastSequence() still reflects the previous published writer.
  sync_point.Process(test_prefix + ":writer_paused");

  const SequenceNumber last_sequence_before_commit = versions.LastSequence();
  const SequenceNumber allocated_after_prepare =
      versions.LastAllocatedSequence();
  EXPECT_LT(last_sequence_before_commit, allocated_after_prepare);

  // Advance LastAllocatedSequence() again through the nonmem queue while the
  // first prepare is still paused before SetLastSequence().
  Status commit_status = wal_only_commit_txn_ref.Commit();
  const SequenceNumber allocated_after_commit =
      versions.LastAllocatedSequence();
  EXPECT_OK(commit_status);
  EXPECT_LT(allocated_after_prepare, allocated_after_commit);

  // Recovery must wait for both write queues before syncing LastSequence() to
  // LastAllocatedSequence(); otherwise it can publish past the paused writer.
  Status resume_status;
  std::thread resume_thread([&]() {
    DBRecoverContext context(FlushReason::kErrorRecoveryRetryFlush);
    resume_status = db_impl.TEST_ResumeImpl(context);
  });

  resume_thread.join();
  blocked_prepare.join();
  std::unique_ptr<Transaction> blocked_prepare_txn_guard(blocked_prepare_txn);
  sync_point.DisableProcessing();

  ASSERT_OK(commit_status);
  ASSERT_OK(resume_status);
  ASSERT_OK(blocked_prepare_status);
  Transaction& blocked_prepare_txn_ref =
      AssertNotNull(blocked_prepare_txn_guard.get());
  // The expected fixed behavior is that ResumeImpl() waited in the write queue,
  // then sequence state ended up fully published and still covers the WAL-only
  // commit allocation.
  ASSERT_TRUE(recovery_waited_for_writer.load());
  ASSERT_EQ(versions.LastSequence(), versions.LastAllocatedSequence());
  ASSERT_GE(versions.LastSequence(), allocated_after_commit);
  ASSERT_OK(blocked_prepare_txn_ref.Commit());

  blocked_prepare_txn_guard.reset();
  wal_only_commit_txn.reset();
  Close();
}

// Recovery does not wait for a main write queue whose leader is in a write
// stall. If that stall ends while recovery is joining the nonmem queue, the
// leader can allocate a sequence range before recovery syncs. Recovery must
// then retry and wait for the leader to publish, instead of publishing the
// leader's allocation for it.
TEST_F(WritePreparedTransactionSeqnoTest,
       RecoverySeqnoSyncRetriesWhenStallEndsDuringNonmemJoin) {
  txn_db_options_.write_policy = TxnDBWritePolicy::WRITE_UNPREPARED;
  ASSERT_OK(Open());
  TransactionDB& db = AssertNotNull(db_);
  DBImpl& db_impl =
      AssertNotNull(static_cast_with_check<DBImpl>(db.GetRootDB()));
  VersionSet& versions = AssertNotNull(db_impl.GetVersionSet());
  SyncPoint& sync_point = AssertNotNull(SyncPoint::GetInstance());

  const std::string test_prefix =
      "RecoverySeqnoSyncRetriesWhenStallEndsDuringNonmemJoin";
  sync_point.LoadDependency(
      {{"DBImpl::DelayWrite:Wait", test_prefix + ":writer_stalled"},
       {"DBImpl::WriteImpl:AfterWBWIIngestBeforeSetLastSequence:pause",
        test_prefix + ":writer_paused"},
       {test_prefix + ":release_writer",
        "DBImpl::WriteImpl:AfterWBWIIngestBeforeSetLastSequence:resume"}});

  std::unique_ptr<WriteControllerToken> stop_token =
      db_impl.TEST_write_controler().GetStopToken();
  std::atomic<int> non_blocking_joins{0};
  std::atomic<bool> release_writer_called{false};
  std::atomic<bool> recovery_waited_for_writer{false};
  std::atomic<bool> writer_published_before_sync{false};
  auto release_writer = [&]() {
    if (!release_writer_called.exchange(true)) {
      sync_point.Process(test_prefix + ":release_writer");
    }
  };
  // The first non-blocking join is refused by the main queue stall. The second
  // one joins the nonmem queue without the DB mutex. End the stall there and
  // let the stalled writer allocate and pause before it publishes.
  sync_point.SetCallBack("WriteThread::EnterUnbatchedNonBlocking:BeforeLock",
                         [&](void*) {
                           if (non_blocking_joins.fetch_add(1) == 1) {
                             stop_token.reset();
                             db_impl.TEST_SignalAllBgCv();
                             sync_point.Process(test_prefix + ":writer_paused");
                           }
                         });
  // On retry, recovery queues behind the paused writer in the main queue.
  sync_point.SetCallBack("WriteThread::EnterUnbatched:Wait", [&](void*) {
    recovery_waited_for_writer.store(true);
    release_writer();
  });
  // Fallback so a path that syncs without retrying fails instead of hanging.
  sync_point.SetCallBack("DBImpl::ResumeImpl:AfterSyncSeq", [&](void*) {
    writer_published_before_sync.store(release_writer_called.load());
    release_writer();
  });
  sync_point.EnableProcessing();

  WriteOptions write_opts;
  TransactionOptions txn_opts;
  Status prepare_status;
  std::unique_ptr<Transaction> stalled_txn(
      db.BeginTransaction(write_opts, txn_opts));
  Transaction& stalled_txn_ref = AssertNotNull(stalled_txn.get());
  ASSERT_OK(stalled_txn_ref.SetName("stalled_txn"));
  ASSERT_OK(stalled_txn_ref.Put("stalled_key", "stalled_value"));
  std::thread writer([&]() { prepare_status = stalled_txn_ref.Prepare(); });

  // The writer leads the main queue and waits on the write stop.
  TEST_SYNC_POINT(test_prefix + ":writer_stalled");
  DBRecoverContext context(FlushReason::kErrorRecoveryRetryFlush);
  Status resume_status = db_impl.TEST_ResumeImpl(context);
  writer.join();
  sync_point.DisableProcessing();
  sync_point.ClearAllCallBacks();
  sync_point.LoadDependency({});

  ASSERT_OK(resume_status);
  ASSERT_OK(prepare_status);
  ASSERT_GE(non_blocking_joins.load(), 3);
  ASSERT_TRUE(recovery_waited_for_writer.load());
  ASSERT_TRUE(writer_published_before_sync.load());
  ASSERT_EQ(versions.LastSequence(), versions.LastAllocatedSequence());
  ASSERT_OK(stalled_txn_ref.Commit());

  stalled_txn.reset();
  Close();
}

// Recovery does not wait for a nonmem write queue whose leader, a WAL-only
// commit, is in a write stall. The stalled commit has not allocated, so
// recovery syncs and finishes while the commit is still waiting.
TEST_F(WritePreparedTransactionSeqnoTest,
       RecoverySeqnoSyncSkipsStalledWalOnlyCommit) {
  txn_db_options_.write_policy = TxnDBWritePolicy::WRITE_UNPREPARED;
  ASSERT_OK(Open());
  TransactionDB& db = AssertNotNull(db_);
  DBImpl& db_impl =
      AssertNotNull(static_cast_with_check<DBImpl>(db.GetRootDB()));
  VersionSet& versions = AssertNotNull(db_impl.GetVersionSet());
  SyncPoint& sync_point = AssertNotNull(SyncPoint::GetInstance());

  WriteOptions write_opts;
  TransactionOptions txn_opts;
  // The data goes to the memtable at Prepare(), so Commit() writes only a WAL
  // marker on the nonmem write queue.
  std::unique_ptr<Transaction> wal_only_commit_txn(
      db.BeginTransaction(write_opts, txn_opts));
  Transaction& wal_only_commit_txn_ref =
      AssertNotNull(wal_only_commit_txn.get());
  ASSERT_OK(wal_only_commit_txn_ref.SetName("wal_only_commit_txn"));
  ASSERT_OK(wal_only_commit_txn_ref.Put("commit_key", "commit_value"));
  ASSERT_OK(wal_only_commit_txn_ref.Prepare());

  const std::string test_prefix = "RecoverySeqnoSyncSkipsStalledWalOnlyCommit";
  sync_point.LoadDependency(
      {{"DBImpl::DelayWrite:NonmemWait", test_prefix + ":commit_stalled"}});
  sync_point.EnableProcessing();

  std::unique_ptr<WriteControllerToken> stop_token =
      db_impl.TEST_write_controler().GetStopToken();
  std::atomic<bool> commit_done{false};
  Status commit_status;
  std::thread committer([&]() {
    commit_status = wal_only_commit_txn_ref.Commit();
    commit_done.store(true);
  });

  // The commit leads the nonmem queue and waits on the write stop.
  TEST_SYNC_POINT(test_prefix + ":commit_stalled");
  DBRecoverContext context(FlushReason::kErrorRecoveryRetryFlush);
  Status resume_status = db_impl.TEST_ResumeImpl(context);
  const bool commit_done_at_recovery_end = commit_done.load();
  const bool synced =
      versions.LastSequence() == versions.LastAllocatedSequence();

  stop_token.reset();
  db_impl.TEST_SignalAllBgCv();
  committer.join();
  sync_point.DisableProcessing();
  sync_point.LoadDependency({});

  ASSERT_OK(resume_status);
  ASSERT_FALSE(commit_done_at_recovery_end);
  ASSERT_TRUE(synced);
  ASSERT_OK(commit_status);
  std::string value;
  ASSERT_OK(db.Get(ReadOptions(), "commit_key", &value));
  ASSERT_EQ("commit_value", value);

  wal_only_commit_txn.reset();
  Close();
}

// Regression test for the gap left by syncing only at the start of
// ResumeImpl(). Recovery drops mutex_ in FlushAllColumnFamilies() before it
// enters AtomicFlushMemTables(), so a WriteUnprepared WAL-only commit can
// advance LastAllocatedSequence() after the early sync but before
// SwitchMemtable() reads LastSequence(). The fix syncs again at the recovery
// flush fence, after both write queues are drained and immediately before
// switching memtables.
TEST_F(WritePreparedTransactionSeqnoTest,
       WriteUnpreparedWalOnlyCommitDuringRecoveryFlushUpdatesSwitchSeq) {
  // Use WRITE_UNPREPARED because a prepared transaction's commit marker is a
  // WAL-only write on nonmem_write_thread_ when there is no commit-time data.
  txn_db_options_.write_policy = TxnDBWritePolicy::WRITE_UNPREPARED;
  ASSERT_OK(Open());
  TransactionDB& db = AssertNotNull(db_);
  DBImpl& db_impl =
      AssertNotNull(static_cast_with_check<DBImpl>(db.GetRootDB()));
  VersionSet& versions = AssertNotNull(db_impl.GetVersionSet());
  SyncPoint& sync_point = AssertNotNull(SyncPoint::GetInstance());

  WriteOptions write_opts;
  TransactionOptions txn_opts;

  // Prepare a transaction that already put its data in the memtable. Its later
  // Commit() will only write the commit marker to WAL, which advances
  // LastAllocatedSequence() without publishing LastSequence().
  std::unique_ptr<Transaction> wal_only_commit_txn(
      db.BeginTransaction(write_opts, txn_opts));
  Transaction& wal_only_commit_txn_ref =
      AssertNotNull(wal_only_commit_txn.get());
  ASSERT_OK(wal_only_commit_txn_ref.SetName("wal_only_commit_txn"));
  ASSERT_OK(wal_only_commit_txn_ref.Put("gap_key", "gap_value"));
  ASSERT_OK(wal_only_commit_txn_ref.Prepare());

  const SequenceNumber allocated_after_prepare =
      versions.LastAllocatedSequence();

  std::atomic<bool> committed_in_flush_gap{false};
  Status commit_status;
  SequenceNumber last_sequence_before_gap_commit = 0;
  SequenceNumber allocated_before_gap_commit = 0;
  SequenceNumber last_sequence_after_gap_commit = 0;
  SequenceNumber allocated_after_gap_commit = 0;

  // Force the reviewed interleaving: ResumeImpl() has completed its early sync
  // and FlushAllColumnFamilies() has released mutex_, but
  // AtomicFlushMemTables() has not yet joined/drained both write queues before
  // SwitchMemtable().
  sync_point.SetCallBack(
      "DBImpl::FlushAllColumnFamilies:BeforeAtomicFlushMemTables", [&](void*) {
        if (committed_in_flush_gap.exchange(true)) {
          return;
        }
        last_sequence_before_gap_commit = versions.LastSequence();
        allocated_before_gap_commit = versions.LastAllocatedSequence();
        commit_status = wal_only_commit_txn_ref.Commit();
        last_sequence_after_gap_commit = versions.LastSequence();
        allocated_after_gap_commit = versions.LastAllocatedSequence();
      });
  sync_point.EnableProcessing();

  // Run the recovery path that force-flushes all column families. This is the
  // path where the unlocked gap exists before the recovery memtable switch.
  DBRecoverContext context(FlushReason::kErrorRecovery);
  Status resume_status = db_impl.TEST_ResumeImpl(context);
  sync_point.DisableProcessing();

  ASSERT_TRUE(committed_in_flush_gap.load());
  ASSERT_OK(commit_status);
  ASSERT_OK(resume_status);

  // Confirm the test actually created the intended nonmem sequence gap.
  ASSERT_GE(allocated_before_gap_commit, allocated_after_prepare);
  ASSERT_EQ(last_sequence_before_gap_commit, last_sequence_after_gap_commit);
  ASSERT_LT(allocated_before_gap_commit, allocated_after_gap_commit);
  ASSERT_LT(last_sequence_after_gap_commit, allocated_after_gap_commit);

  // Without the flush-fence sync, LastSequence() remains at the stale value
  // consumed by SwitchMemtable(). The fixed path catches it up before the
  // switch creates a new memtable/WAL boundary.
  ASSERT_EQ(versions.LastSequence(), versions.LastAllocatedSequence());
  ASSERT_GE(versions.LastSequence(), allocated_after_gap_commit);

  wal_only_commit_txn.reset();
  Close();
}

// Regression test: verify that after error recovery with two_write_queues,
// the DB can be closed and reopened without sequence number corruption.
TEST_F(WritePreparedTransactionSeqnoTest,
       SeqnoGoesBackwardsDuringErrorRecovery) {
  ASSERT_OK(Open());

  // Write some initial data and flush to establish baseline
  WriteOptions write_opts;
  TransactionOptions txn_opts;
  for (int i = 0; i < 10; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Write more data - these will allocate sequence numbers
  for (int i = 10; i < 20; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Set up sync point dependency chain for deterministic recovery
  // synchronization, following the pattern from
  // ManifestWriteRetryableErrorAutoRecover in error_handler_fs_test.cc.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "SeqnoGoesBackwardsDuringErrorRecovery:0"},
       {"SeqnoGoesBackwardsDuringErrorRecovery:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "SeqnoGoesBackwardsDuringErrorRecovery:2"}});

  // Inject a retryable MANIFEST write error on the next flush
  IOStatus error_to_inject = IOStatus::IOError("Injected MANIFEST error");
  error_to_inject.SetRetryable(true);
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_to_inject); });
  SyncPoint::GetInstance()->EnableProcessing();

  // Trigger a flush that will fail due to MANIFEST write error
  Status s = db_->Flush(FlushOptions());
  ASSERT_NOK(s);

  // Wait for recovery to start, then re-enable filesystem and let it proceed.
  // Clear the callback first to prevent it from re-disabling the filesystem
  // if recovery's ResumeImpl triggers WriteManifest before we re-enable.
  TEST_SYNC_POINT("SeqnoGoesBackwardsDuringErrorRecovery:0");
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::LogAndApply:WriteManifest");
  fault_fs_->SetFilesystemActive(true);
  TEST_SYNC_POINT("SeqnoGoesBackwardsDuringErrorRecovery:1");

  // Wait for recovery to complete
  TEST_SYNC_POINT("SeqnoGoesBackwardsDuringErrorRecovery:2");
  SyncPoint::GetInstance()->DisableProcessing();

  // Write some more data after recovery
  for (int i = 20; i < 30; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn_after_" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Close and reopen - this would fail with "sequence number going backwards"
  // before the fix.
  Close();

  Status reopen_s = Open();
  ASSERT_OK(reopen_s);

  // Verify data integrity
  ReadOptions read_opts;
  for (int i = 0; i < 20; i++) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }

  Close();
}

// Test that verifies the sequence number discrepancy is resolved by checking
// that LastSequence >= LastAllocatedSequence after recovery completes.
TEST_F(WritePreparedTransactionSeqnoTest, SeqnoDiscrepancyDuringErrorRecovery) {
  ASSERT_OK(Open());

  WriteOptions write_opts;
  TransactionOptions txn_opts;

  // Write initial data and flush
  for (int i = 0; i < 5; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("init_txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Write more transactions with two_write_queues to potentially create a gap
  // between allocated and published sequence numbers. These must be written
  // before installing the error injection callback, since the small write
  // buffer (4KB) could trigger an automatic flush during these writes.
  for (int i = 5; i < 10; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Track sequence numbers at key points
  std::atomic<uint64_t> last_seq_after_recovery{0};
  std::atomic<uint64_t> last_allocated_seq_after_recovery{0};
  std::atomic<bool> captured_seqs_after{false};

  IOStatus error_to_inject = IOStatus::IOError("Injected error");
  error_to_inject.SetRetryable(true);

  // Set up sync point dependency chain for deterministic synchronization
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "SeqnoDiscrepancyDuringErrorRecovery:0"},
       {"SeqnoDiscrepancyDuringErrorRecovery:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "SeqnoDiscrepancyDuringErrorRecovery:2"}});

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_to_inject); });

  // Capture sequence numbers after recovery completes to verify the fix
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:RecoverSuccess", [&](void*) {
        DBImpl* db_impl = dbimpl();
        if (db_impl) {
          VersionSet* vs = db_impl->GetVersionSet();
          if (vs) {
            last_seq_after_recovery.store(vs->LastSequence());
            last_allocated_seq_after_recovery.store(
                vs->LastAllocatedSequence());
            captured_seqs_after.store(true);
          }
        }
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Trigger a flush that will fail
  Status flush_s = db_->Flush(FlushOptions());
  ASSERT_NOK(flush_s);

  // Wait for recovery to start, re-enable filesystem, let it proceed.
  // Clear the callback first to prevent it from re-disabling the filesystem
  // if recovery's ResumeImpl triggers WriteManifest before we re-enable.
  TEST_SYNC_POINT("SeqnoDiscrepancyDuringErrorRecovery:0");
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::LogAndApply:WriteManifest");
  fault_fs_->SetFilesystemActive(true);
  TEST_SYNC_POINT("SeqnoDiscrepancyDuringErrorRecovery:1");

  // Wait for recovery to complete
  TEST_SYNC_POINT("SeqnoDiscrepancyDuringErrorRecovery:2");
  SyncPoint::GetInstance()->DisableProcessing();

  // Verify that sequences were captured and are in sync after recovery
  ASSERT_TRUE(captured_seqs_after.load());
  ASSERT_GE(last_seq_after_recovery.load(),
            last_allocated_seq_after_recovery.load())
      << "LastSequence should be >= LastAllocatedSequence after recovery";

  // Close and reopen should succeed without corruption
  Close();
  ASSERT_OK(Open());

  // Verify data integrity
  ReadOptions read_opts;
  for (int i = 0; i < 10; i++) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }

  Close();
}

// Test that verifies SyncLastSequenceWithAllocated is called during ResumeImpl
// by checking sequence numbers before and after the sync point.
TEST_F(WritePreparedTransactionSeqnoTest, ConcurrentWritesDuringErrorRecovery) {
  ASSERT_OK(Open());

  WriteOptions write_opts;
  TransactionOptions txn_opts;

  // Write initial data and flush
  for (int i = 0; i < 5; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("init_txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Write more transactions. These must be written before installing the error
  // injection callback, since the small write buffer (4KB) could trigger an
  // automatic flush during these writes.
  for (int i = 5; i < 10; i++) {
    Transaction* txn = db_->BeginTransaction(write_opts, txn_opts);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn" + std::to_string(i)));
    ASSERT_OK(txn->Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(txn->Prepare());
    ASSERT_OK(txn->Commit());
    delete txn;
  }

  // Track sequence numbers at key points during recovery
  std::atomic<uint64_t> seq_before_resume{0};
  std::atomic<uint64_t> alloc_seq_before_resume{0};
  std::atomic<uint64_t> seq_after_resume{0};
  std::atomic<uint64_t> alloc_seq_after_resume{0};

  IOStatus error_to_inject = IOStatus::IOError("Injected error");
  error_to_inject.SetRetryable(true);

  // Set up sync point dependency chain for deterministic synchronization
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "ConcurrentWritesDuringErrorRecovery:0"},
       {"ConcurrentWritesDuringErrorRecovery:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "ConcurrentWritesDuringErrorRecovery:2"}});

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_to_inject); });

  // Capture sequences right before ResumeImpl runs the sync
  SyncPoint::GetInstance()->SetCallBack("DBImpl::ResumeImpl:Start", [&](void*) {
    DBImpl* db_impl = dbimpl();
    if (db_impl) {
      VersionSet* vs = db_impl->GetVersionSet();
      if (vs) {
        seq_before_resume.store(vs->LastSequence());
        alloc_seq_before_resume.store(vs->LastAllocatedSequence());
      }
    }
  });

  // Capture sequences right after ResumeImpl syncs them
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ResumeImpl:AfterSyncSeq", [&](void*) {
        DBImpl* db_impl = dbimpl();
        if (db_impl) {
          VersionSet* vs = db_impl->GetVersionSet();
          if (vs) {
            seq_after_resume.store(vs->LastSequence());
            alloc_seq_after_resume.store(vs->LastAllocatedSequence());
          }
        }
      });

  SyncPoint::GetInstance()->EnableProcessing();

  // Trigger a flush that will fail
  Status flush_s = db_->Flush(FlushOptions());
  ASSERT_NOK(flush_s);

  // Wait for recovery to start, re-enable filesystem, let it proceed.
  // Clear the callback first to prevent it from re-disabling the filesystem
  // if recovery's ResumeImpl triggers WriteManifest before we re-enable.
  TEST_SYNC_POINT("ConcurrentWritesDuringErrorRecovery:0");
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::LogAndApply:WriteManifest");
  fault_fs_->SetFilesystemActive(true);
  TEST_SYNC_POINT("ConcurrentWritesDuringErrorRecovery:1");

  // Wait for recovery to complete
  TEST_SYNC_POINT("ConcurrentWritesDuringErrorRecovery:2");
  SyncPoint::GetInstance()->DisableProcessing();

  // Verify that the AfterSyncSeq callback fired and sequences are in sync
  ASSERT_GT(seq_after_resume.load(), 0u)
      << "DBImpl::ResumeImpl:AfterSyncSeq callback should have fired";
  ASSERT_EQ(seq_after_resume.load(), alloc_seq_after_resume.load())
      << "Fix should have synced sequences";

  // Close and reopen
  Close();
  ASSERT_OK(Open());

  // Verify data integrity
  ReadOptions read_opts;
  for (int i = 0; i < 10; i++) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
