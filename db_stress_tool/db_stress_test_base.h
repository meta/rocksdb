//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/io_status.h"
#ifdef GFLAGS
#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "env/composite_env_wrapper.h"
#include "rocksdb/experimental.h"
#include "rocksdb/iterator.h"
#include "rocksdb/user_defined_index.h"
#include "rocksdb/utilities/checkpoint.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;
class Transaction;
class TransactionDB;
class OptimisticTransactionDB;
class DbStressFSWrapper;
class Iterator;
struct TransactionDBOptions;
using experimental::SstQueryFilterConfigsManager;

std::string FormatDiagnosticSlice(const Slice& value);
std::string SanitizeDiagnosticValue(const std::string& value);
std::string FormatReadOptionsForDiagnostics(const ReadOptions& ro);

class ScanWitnessLog {
 public:
  ScanWitnessLog(ThreadState* thread, const char* scan_type);

  bool Enabled() const;

  void Add(const std::string& event);

  void AddIteratorState(const char* action, const char* label,
                        const Iterator& iter, bool include_value);

  void AddIteratorState(const char* action, size_t label, const Iterator& iter,
                        bool include_value);

  void Flush(const char* reason) const;

 private:
  ThreadState* thread_;
  const char* scan_type_;
  std::vector<std::string> entries_;
  size_t next_entry_;
  bool wrapped_;
};

class StressTest {
 public:
  // Returns true if `error_s` should be treated as retryable rather than a real
  // failure. Covers injected fault-injection errors, and -- despite the name --
  // also non-injected IO errors, but only when a remote backend (--env_uri /
  // --fs_uri) is in use and --tolerate_non_injected_io_errors_for_remote_dbs is
  // set (infrastructure behind a remote backend can return transient IO
  // errors). Gating on a remote backend guarantees non-injected IO errors on
  // local DBs are never masked. Data-loss errors are never retryable.
  //
  // Defined out-of-line in db_stress_test_base.cc: it reads FLAGS_env_uri /
  // FLAGS_fs_uri, which db_stress_common.h declares only after it includes this
  // header, so they are not visible here.
  static bool IsErrorInjectedAndRetryable(const Status& error_s);

  // Returns true if `error_s` is a non-injected, non-data-loss IO error that
  // --tolerate_non_injected_io_errors_for_remote_dbs asks us to tolerate while
  // a remote backend (--env_uri / --fs_uri) is in use. Infrastructure behind a
  // remote backend can fail a read transiently with no RocksDB bug involved.
  //
  // Defined out-of-line in db_stress_test_base.cc for the same reason as
  // IsErrorInjectedAndRetryable.
  static bool IsTolerableNonInjectedRemoteIOError(const Status& error_s);

  // Returns true if `error_s`, surfaced by an operation during which
  // `injected_error_count` faults were injected, is retryable rather than a
  // verification failure.
  //
  // An injected-error status is only trusted when this operation actually
  // injected a fault, because otherwise the status did not come from fault
  // injection. A tolerated non-injected remote IO error has no injected fault
  // to correlate with, so `injected_error_count` must not gate it -- gating it
  // would make --tolerate_non_injected_io_errors_for_remote_dbs a no-op
  // whenever fault injection is disabled.
  static bool IsRetryableOperationError(int injected_error_count,
                                        const Status& error_s);

  static bool IsTolerableCompactionFailure(const Status& s) {
    // TOOD (hx235): allow an exact list of tolerable failures under stress
    // test
    return s.IsManualCompactionPaused() || s.IsCompactionAborted() ||
           IsErrorInjectedAndRetryable(s) || s.IsAborted() ||
           s.IsInvalidArgument() || s.IsNotSupported();
  }

  // Returns true if the status is an expected transactional error, including
  // lock conflicts (deadlock or timeout) from MaybeAddKeyToTxnForRYW writing
  // to the same key space without the stress-test-level mutex, and TryAgain
  // from optimistic transactions when conflict detection retries are exhausted.
  static bool IsExpectedTxnError(const Status& s);

  StressTest(int db_index, const std::string& db_path,
             const std::string& ev_path, const std::string& sec_path);

  virtual ~StressTest() {}

  const std::string& GetDbLabel() const;
  const std::string& GetDbPath() const;
  const std::string& GetExpectedValuesDir() const;
  const std::string& GetSecondariesBase() const;

  // See db_fault_injection_fs_ member.
  std::shared_ptr<FaultInjectionTestFS> GetDbFaultInjectionFs() const {
    return db_fault_injection_fs_;
  }

  // See db_env_ member.
  Env* GetDbEnv() const;

  static std::shared_ptr<Cache> NewCache(size_t capacity,
                                         int32_t num_shard_bits);

  static std::vector<std::string> GetBlobCompressionTags();

  bool BuildOptionsTable();

  void InitDb(SharedState*);
  // The initialization work is split into two parts to avoid a circular
  // dependency with `SharedState`.
  virtual void FinishInitDb(SharedState*);
  void TrackExpectedState(SharedState* shared);
  void OperateDb(ThreadState* thread);
  virtual void VerifyDb(ThreadState* thread) const = 0;
  virtual void ContinuouslyVerifyDb(ThreadState* /*thread*/) const = 0;
  void PrintStatistics();
  bool MightHaveUnsyncedDataLoss() {
    return FLAGS_sync_fault_injection || FLAGS_disable_wal ||
           FLAGS_manual_wal_flush_one_in > 0;
  }
  Status EnableAutoCompaction() {
    assert(options_.disable_auto_compactions);
    Status s = db_->EnableAutoCompaction(column_families_);
    return s;
  }
  Options GetOptions(int cf_id);
  void CleanUp();

 private:
  void InitializeListenersForOpen(
      SharedState* shared,
      const std::vector<ColumnFamilyDescriptor>& cf_descriptors);

 protected:
  static int GetMinInjectedErrorCount(int error_count_1, int error_count_2) {
    if (error_count_1 > 0 && error_count_2 > 0) {
      return std::min(error_count_1, error_count_2);
    } else if (error_count_1 > 0) {
      return error_count_1;
    } else if (error_count_2 > 0) {
      return error_count_2;
    } else {
      return 0;
    }
  }

  void UpdateIfInitialWriteFails(Env* env, const Status& write_s,
                                 Status* initial_write_s,
                                 bool* initial_wal_write_may_succeed,
                                 uint64_t* wait_for_recover_start_time,
                                 bool commit_bypass_memtable = false) {
    assert(env && initial_write_s && initial_wal_write_may_succeed &&
           wait_for_recover_start_time);
    if (!write_s.ok() && (*initial_write_s).ok()) {
      *initial_write_s = write_s;
      *initial_wal_write_may_succeed =
          commit_bypass_memtable ||
          !FaultInjectionTestFS::IsFailedToWriteToWALError(*initial_write_s);
      *wait_for_recover_start_time = env->NowMicros();
    }
  }

  void PrintWriteRecoveryWaitTimeIfNeeded(Env* env,
                                          const Status& initial_write_s,
                                          bool initial_wal_write_may_succeed,
                                          uint64_t wait_for_recover_start_time,
                                          const std::string& thread_name) {
    assert(env);
    bool waited_for_recovery = !initial_write_s.ok() &&
                               IsErrorInjectedAndRetryable(initial_write_s) &&
                               initial_wal_write_may_succeed;
    if (waited_for_recovery) {
      uint64_t elapsed_sec =
          (env->NowMicros() - wait_for_recover_start_time) / 1000000;
      if (elapsed_sec > 10) {
        fprintf(stdout,
                "%s thread slept to wait for write recovery for "
                "%" PRIu64 " seconds\n",
                thread_name.c_str(), elapsed_sec);
      }
    }
  }
  void GetDeleteRangeKeyLocks(
      ThreadState* thread, int rand_column_family, int64_t rand_key,
      std::vector<std::unique_ptr<MutexLock>>* range_locks) {
    for (int j = 0; j < FLAGS_range_deletion_width; ++j) {
      if (j == 0 ||
          ((rand_key + j) & ((1 << FLAGS_log2_keys_per_lock) - 1)) == 0) {
        range_locks->emplace_back(new MutexLock(
            thread->shared->GetMutexForKey(rand_column_family, rand_key + j)));
      }
    }
  }

  Status AssertSame(DB* db, ColumnFamilyHandle* cf,
                    ThreadState::SnapshotState& snap_state);

  // Currently PreloadDb has to be single-threaded.
  void PreloadDbAndReopenAsReadOnly(int64_t number_of_keys,
                                    SharedState* shared);

  Status SetOptions(ThreadState* thread);

  // For transactionsDB, there can be txns prepared but not yet committeed
  // right before previous stress run crash.
  // They will be recovered and processed through
  // ProcessRecoveredPreparedTxnsHelper on the start of current stress run.
  void ProcessRecoveredPreparedTxns(SharedState* shared);

  // Default implementation will first update ExpectedState to be
  // `SharedState::UNKNOWN` for each keys in `txn` and then randomly
  // commit or rollback `txn`.
  virtual void ProcessRecoveredPreparedTxnsHelper(Transaction* txn,
                                                  SharedState* shared);

  // ExecuteTransaction is recommended instead
  // @param commit_bypass_memtable Whether commit_bypass_memtable is set to
  // true in transaction options.
  Status NewTxn(WriteOptions& write_opts, ThreadState* thread,
                std::unique_ptr<Transaction>* out_txn,
                bool* commit_bypass_memtable = nullptr);
  Status CommitTxn(Transaction& txn, ThreadState* thread = nullptr);

  // Creates a transaction, executes `ops`, and tries to commit
  // @param commit_bypass_memtable Whether commit_bypass_memtable is set to
  // true in transaction options.
  Status ExecuteTransaction(WriteOptions& write_opts, ThreadState* thread,
                            std::function<Status(Transaction&)>&& ops,
                            bool* commit_bypass_memtable = nullptr);

  virtual void MaybeClearOneColumnFamily(ThreadState* /* thread */) {}

  virtual bool ShouldAcquireMutexOnKey() const { return false; }

  // Returns true if DB state is tracked by the stress test.
  virtual bool IsStateTracked() const = 0;

  virtual std::vector<int> GenerateColumnFamilies(
      const int /* num_column_families */, int rand_column_family) const {
    return {rand_column_family};
  }

  virtual std::vector<int64_t> GenerateKeys(int64_t rand_key) const {
    return {rand_key};
  }

  virtual void TestKeyMayExist(ThreadState*, const ReadOptions&,
                               const std::vector<int>&,
                               const std::vector<int64_t>&) {}

  virtual Status TestGet(ThreadState* thread, const ReadOptions& read_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys) = 0;

  virtual std::vector<Status> TestMultiGet(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) = 0;

  virtual void TestGetEntity(ThreadState* thread, const ReadOptions& read_opts,
                             const std::vector<int>& rand_column_families,
                             const std::vector<int64_t>& rand_keys) = 0;

  virtual void TestMultiGetEntity(ThreadState* thread,
                                  const ReadOptions& read_opts,
                                  const std::vector<int>& rand_column_families,
                                  const std::vector<int64_t>& rand_keys) = 0;

  // True if lazy wide-column read coverage (FLAGS_lazy_entity_read_one_in) is
  // enabled and applicable for this run. Cheap config-only check (no
  // randomness); used by callers that must build key data before invoking the
  // Maybe* helpers below. Requires open_files == -1 (the lazy API's
  // requirement) and, as tracked follow-ups, excludes user-defined timestamps
  // and transactions (see LazyEntityReadEnabled() in the .cc for details).
  bool LazyEntityReadEnabled() const;

  // Coverage for the lazy wide-column read API. When enabled and randomly
  // triggered (FLAGS_lazy_entity_read_one_in), always reads `key` in `cfh` via
  // GetEntityLazy and resolves a random subset of its columns (possibly none)
  // under a pinned snapshot -- this exercises the lazy read paths for crash
  // coverage. Additionally, when DB verification is enabled (!skip_verifydb),
  // checks the lazy result matches an eager reference (enumeration: column
  // count / names / known logical sizes, with no blob I/O; plus the resolved
  // subset compared byte-for-byte). If the caller already read the entity under
  // read_opts.snapshot, it should pass those columns as `eager_reference` so we
  // reuse that (already-verified) result and skip a redundant eager read;
  // otherwise (eager_reference == nullptr) we pin our own snapshot and read the
  // reference here. Reports failures via thread->shared; tolerates
  // injected/retryable read errors. No-op if not triggered. Meant to be called
  // from the modes' TestGetEntity implementations.
  //
  // By default the read targets the primary `db_`. Pass `db` (with a `cfh` from
  // that instance) to exercise a read-only or secondary instance instead; such
  // a target has a stable view for the call (and secondary intentionally
  // disallows snapshot reads), so it is read at latest with no pinned snapshot.
  void MaybeTestGetEntityLazy(ThreadState* thread, const ReadOptions& read_opts,
                              ColumnFamilyHandle* cfh, const Slice& key,
                              const WideColumns* eager_reference = nullptr,
                              DB* db = nullptr) const;

  // Per-key eager reference for the MultiGetEntityLazy differential below: the
  // caller's eager status for the key plus, when found, a pointer to its
  // columns (all read under read_opts.snapshot). Carrying the status -- rather
  // than just a columns-or-null pointer -- lets the differential tell a clean
  // NotFound apart from an injected/other eager error, which must be skipped
  // rather than compared against the lazy result.
  struct EagerEntityRef {
    Status status;
    const WideColumns* columns = nullptr;  // valid iff status.ok()
  };

  // Batch analogue of MaybeTestGetEntityLazy for TestMultiGetEntity: always
  // exercises MultiGetEntityLazy + LazyWideColumnsBatch::MultiResolve on a
  // random cross-entity column subset, and (when !skip_verifydb) verifies
  // against an eager reference. If the caller already read these keys under
  // read_opts.snapshot, it should pass `eager_references` (size == num_keys,
  // one EagerEntityRef per key) to reuse those verified results and skip a
  // redundant eager MultiGetEntity; otherwise (eager_references == nullptr) we
  // read the reference here. Single column family.
  void MaybeTestMultiGetEntityLazy(
      ThreadState* thread, const ReadOptions& read_opts,
      ColumnFamilyHandle* cfh, size_t num_keys, const Slice* keys,
      const std::vector<EagerEntityRef>* eager_references = nullptr);

  // Exercises a lazily-read entity's no-I/O enumeration accessors, and (when
  // `reference` is non-null) checks column count / names / known logical sizes
  // against the eager reference. Reports mismatches via thread->shared and
  // returns false; returns true when consistent (or when only exercising, i.e.
  // reference == nullptr).
  bool CheckLazyEntityEnumeration(ThreadState* thread, const std::string& key,
                                  const WideColumns* reference,
                                  const LazyWideColumns& lazy) const;

  // Resolves a random subset of `lazy`'s columns (possibly none) via
  // LazyWideColumns::MultiResolve to exercise the resolver; when `reference` is
  // non-null, also verifies the resolved bytes against it.
  void ResolveLazyEntity(ThreadState* thread, const std::string& key,
                         const WideColumns* reference,
                         LazyWideColumns& lazy) const;

  virtual Status TestPrefixScan(ThreadState* thread,
                                const ReadOptions& read_opts,
                                const std::vector<int>& rand_column_families,
                                const std::vector<int64_t>& rand_keys) = 0;

  virtual Status TestPut(ThreadState* thread, WriteOptions& write_opts,
                         const ReadOptions& read_opts,
                         const std::vector<int>& cf_ids,
                         const std::vector<int64_t>& keys,
                         char (&value)[100]) = 0;

  virtual Status TestDelete(ThreadState* thread, WriteOptions& write_opts,
                            const std::vector<int>& rand_column_families,
                            const std::vector<int64_t>& rand_keys) = 0;

  virtual Status TestDeleteRange(ThreadState* thread, WriteOptions& write_opts,
                                 const std::vector<int>& rand_column_families,
                                 const std::vector<int64_t>& rand_keys) = 0;

  virtual void TestIngestExternalFile(
      ThreadState* thread, const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys) = 0;

  // Issue compact range, starting with start_key, whose integer value
  // is rand_key.
  virtual void TestCompactRange(ThreadState* thread, int64_t rand_key,
                                const Slice& start_key,
                                ColumnFamilyHandle* column_family);

  virtual void TestPromoteL0(ThreadState* thread,
                             ColumnFamilyHandle* column_family);

  // Calculate a hash value for all keys in range [start_key, end_key]
  // at a certain snapshot.
  uint32_t GetRangeHash(ThreadState* thread, const Snapshot* snapshot,
                        ColumnFamilyHandle* column_family,
                        const Slice& start_key, const Slice& end_key);

  // Return a column family handle that mirrors what is pointed by
  // `column_family_id`, which will be used to validate data to be correct.
  // By default, the column family itself will be returned.
  virtual ColumnFamilyHandle* GetControlCfh(ThreadState* /* thread*/,
                                            int column_family_id) {
    return column_families_[column_family_id];
  }

  // Generated a list of keys that close to boundaries of SST keys.
  // If there isn't any SST file in the DB, return empty list.
  std::vector<std::string> GetWhiteBoxKeys(ThreadState* thread, DB* db,
                                           ColumnFamilyHandle* cfh,
                                           size_t num_keys);

  // Given a key K, this creates an iterator which scans to K and then
  // does a random sequence of Next/Prev operations.
  virtual Status TestIterate(ThreadState* thread, const ReadOptions& read_opts,
                             const std::vector<int>& rand_column_families,
                             const std::vector<int64_t>& rand_keys);

  // Given a key K, this creates an attribute group iterator which scans to K
  // and then does a random sequence of Next/Prev operations. Called only when
  // use_attribute_group=1
  virtual Status TestIterateAttributeGroups(
      ThreadState* thread, const ReadOptions& read_opts,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys);

  template <typename IterType, typename NewIterFunc, typename VerifyFunc>
  Status TestIterateImpl(ThreadState* thread, const ReadOptions& read_opts,
                         const std::vector<int>& rand_column_families,
                         const std::vector<int64_t>& rand_keys,
                         NewIterFunc new_iter_func, VerifyFunc verify_func);

  virtual Status TestIterateAgainstExpected(
      ThreadState* /* thread */, const ReadOptions& /* read_opts */,
      const std::vector<int>& /* rand_column_families */,
      const std::vector<int64_t>& /* rand_keys */) {
    return Status::NotSupported();
  }

  Status TestMultiScan(ThreadState* thread, const ReadOptions& read_opts,
                       const std::vector<int>& rand_column_families,
                       const std::vector<int64_t>& rand_keys);

  // Enum used by VerifyIterator() to identify the mode to validate.
  enum LastIterateOp {
    kLastOpSeek,
    kLastOpSeekForPrev,
    kLastOpNextOrPrev,
    kLastOpSeekToFirst,
    kLastOpSeekToLast
  };

  // Enum used to track MANIFEST verification mode during DB reopen.
  // Verification is skipped when metadata write fault injection is enabled
  // because recovery may have to abandon a partially written MANIFEST.
  enum ManifestVerifyMode {
    MANIFEST_VERIFY_NONE,
    // MANIFEST file should be reused (same file number), CURRENT should not
    // change. Used when reuse_manifest_on_open=1. Warnings are logged on
    // failure but test continues.
    MANIFEST_VERIFY_REUSE,
    // MANIFEST file should be reused AND no recovery writes should occur.
    // Used when both reuse_manifest_on_open=1 and
    // optimize_manifest_for_recovery=1. Warnings are logged on failure but test
    // continues.
    MANIFEST_VERIFY_NO_WRITE,
    // Strict verification: MANIFEST file must be reused with ZERO writes
    // (complete avoidance). Used when ALL conditions for complete avoidance
    // are met. Failures are FATAL and will terminate the test.
    // Conditions for STRICT mode:
    // - Both reuse_manifest_on_open=1 and optimize_manifest_for_recovery=1
    // - Not in best_efforts_recovery mode
    // - avoid_flush_during_recovery=true (no flush during recovery)
    // - No WriteBufferManager limit enforced during recovery
    // - write_dbid_to_manifest=0 (no DB_ID write on open)
    // - metadata_write_fault_one_in=0 (no fault injection)
    // - open_metadata_write_fault_one_in=0 (no fault injection)
    // - MANIFEST not corrupted, not at size limit
    // Note: avoid_flush_during_shutdown is NOT required. If it leaves data
    // in WAL but avoid_flush_during_recovery=true prevents flushing it,
    // MANIFEST still won't be written.
    MANIFEST_VERIFY_STRICT
  };

  // Compare the two iterator, iter and cmp_iter are in the same position,
  // unless iter might be made invalidate or undefined because of
  // upper or lower bounds, or prefix extractor.
  // Will flag failure if the verification fails.
  // diverged = true if the two iterator is already diverged.
  // Returns true only when this call reports a verification failure. A false
  // return with diverged = true means verification was intentionally skipped.
  // op_logs is the information to print when validation fails.
  // verify_key is the seek target, or for SeekToFirst() under
  // prefix_same_as_start the first key returned. Its prefix bounds the scan.
  template <typename IterType, typename VerifyFuncType>
  bool VerifyIterator(ThreadState* thread, ColumnFamilyHandle* cmp_cfh,
                      const ReadOptions& ro, IterType* iter, Iterator* cmp_iter,
                      LastIterateOp op, const Slice& verify_key,
                      const std::vector<int>& rand_column_families,
                      const std::string& op_logs, VerifyFuncType verifyFunc,
                      bool* diverged);

  void DumpIteratorDivergenceDiagnostics(
      ColumnFamilyHandle* cmp_cfh, const ReadOptions& ro,
      const Slice& verify_key,
      const std::vector<int>& rand_column_families) const;

  virtual Status TestBackupRestore(ThreadState* thread,
                                   const std::vector<int>& rand_column_families,
                                   const std::vector<int64_t>& rand_keys);

  virtual Status PrepareOptionsForRestoredDB(Options* options);

  virtual Status TestCheckpoint(ThreadState* thread,
                                const std::vector<int>& rand_column_families,
                                const std::vector<int64_t>& rand_keys);

  void TestCompactFiles(ThreadState* thread, ColumnFamilyHandle* column_family);

  void TestFlush(ThreadState* thread,
                 const std::vector<int>& rand_column_families);

  Status TestResetStats();

  Status TestPauseBackground(ThreadState* thread);

  Status TestDisableFileDeletions(ThreadState* thread);

  Status TestDisableManualCompaction(ThreadState* thread);

  bool ShouldAbortAndResumeCompactions() const;

  Status TestAbortAndResumeCompactions(ThreadState* thread);

  Status TestAbortAndResumeCfCompactions(ThreadState* thread);

  void TestAcquireSnapshot(ThreadState* thread, int rand_column_family,
                           const std::string& keystr, uint64_t i);

  Status MaybeReleaseSnapshots(ThreadState* thread, uint64_t i);

  Status TestGetLiveFiles() const;
  Status TestGetLiveFilesMetaData() const;
  Status TestGetLiveFilesStorageInfo() const;
  Status TestGetAllColumnFamilyMetaData() const;

  Status TestGetSortedWalFiles() const;
  Status TestGetCurrentWalFile() const;
  void TestGetProperty(ThreadState* thread) const;
  Status TestGetPropertiesOfAllTables() const;

  virtual Status TestApproximateSize(
      ThreadState* thread, uint64_t iteration,
      const std::vector<int>& rand_column_families,
      const std::vector<int64_t>& rand_keys);

  virtual Status TestCustomOperations(
      ThreadState* /*thread*/,
      const std::vector<int>& /*rand_column_families*/) {
    return Status::NotSupported("TestCustomOperations() must be overridden");
  }

  void ProcessStatus(SharedState* shared, std::string msg, const Status& s,
                     bool ignore_injected_error = true) const;

  void VerificationAbort(SharedState* shared, std::string msg) const;

  void VerificationAbort(SharedState* shared, std::string msg, int cf,
                         int64_t key) const;

  void VerificationAbort(SharedState* shared, std::string msg, int cf,
                         int64_t key, Slice value_from_db,
                         Slice value_from_expected) const;

  void VerificationAbort(SharedState* shared, int cf, int64_t key,
                         const Slice& value, const WideColumns& columns) const;

  // Under --verify_cpu_corruption_dir, verifies the just-run op (named by
  // `op_label`, e.g. "put"/"flush"/"compactrange") for a returned/read-back
  // corruption or a silent data corruption. A no-op when the flag is empty. See
  // the definition in db_stress_test_base.cc for the full behavior, startup
  // requirements, performance cost, and result-file contract.
  void MaybeVerifyCpuCorruption(ThreadState* thread, const char* op_label,
                                const Status& op_status);

  static std::string DebugString(const Slice& value,
                                 const WideColumns& columns);

  void PrintEnv() const;

  void Open(SharedState* shared, bool reopen = false);

  void Reopen(ThreadState* thread);

  // Periodically opens a read-only DB instance on the primary's live directory
  // while the primary keeps writing, then closes it. Exercises the concurrent
  // primary + read-only-DB-on-the-same-directory topology to guard against a
  // read-only DB's close deleting the primary's live SST files. Detection
  // relies on db_stress's existing missing-file/corruption checks.
  void MaybeOpenReadOnlyOnPrimary(ThreadState* thread);

  virtual void RegisterAdditionalListeners() {}

  virtual void PrepareTxnDbOptions(SharedState* /*shared*/,
                                   TransactionDBOptions& /*txn_db_opts*/) {}

  // Returns whether the timestamp of read_opts is updated.
  bool MaybeUseOlderTimestampForPointLookup(ThreadState* thread,
                                            std::string& ts_str,
                                            Slice& ts_slice,
                                            ReadOptions& read_opts);

  void MaybeUseOlderTimestampForRangeScan(ThreadState* thread,
                                          std::string& ts_str, Slice& ts_slice,
                                          ReadOptions& read_opts);

  void CleanUpColumnFamilies();

  Status VerifyOptionsFileStateBeforeOpen(
      const std::vector<ColumnFamilyDescriptor>& cf_descriptors,
      bool db_exists);
  Status VerifyPersistedOptionsAfterOpen();

  void RecordManifestStateBeforeReopen();
  void VerifyManifestNotRewritten();

  int db_index_;
  std::string db_label_;
  std::string db_path_;
  std::string expected_values_path_;
  std::string secondaries_path_;
  // Wraps raw_env->GetFileSystem(). See DbStressFSWrapper.
  std::shared_ptr<DbStressFSWrapper> db_stress_fs_;
  // Wraps db_stress_fs_ when NeedsFaultInjection(). Null otherwise.
  std::shared_ptr<FaultInjectionTestFS> db_fault_injection_fs_;
  // Wraps the outermost FS above. Set as options_.env for all DB I/O.
  std::unique_ptr<CompositeEnvWrapper> db_env_;
  std::shared_ptr<Cache> compressed_cache_;
  std::shared_ptr<const FilterPolicy> filter_policy_;
  std::unique_ptr<DB> db_owner_;
  DB* db_;
  TransactionDB* txn_db_;
  OptimisticTransactionDB* optimistic_txn_db_;

  // Currently only used in MultiOpsTxnsStressTest
  std::atomic<DB*> db_aptr_;

  Options options_;
  SystemClock* clock_;
  std::vector<ColumnFamilyHandle*> column_families_;
  std::vector<std::string> column_family_names_;
  std::atomic<int> new_column_family_name_;
  int num_times_reopened_;
  std::unordered_map<std::string, std::vector<std::string>> options_table_;
  std::vector<std::string> options_index_;
  std::atomic<bool> db_preload_finished_;
  std::shared_ptr<SstQueryFilterConfigsManager::Factory> sqfc_factory_;
  std::shared_ptr<IndexFactory> udi_factory_;

  std::unique_ptr<DB> secondary_db_;
  std::vector<ColumnFamilyHandle*> secondary_cfhs_;
  bool is_db_stopped_;

  std::unique_ptr<CheckpointEngine> checkpoint_engine_;

  // MANIFEST verification state for reopen
  ManifestVerifyMode manifest_verify_mode_;
  uint64_t manifest_file_number_before_reopen_;
  uint64_t manifest_file_size_before_reopen_;
  std::string current_file_content_before_reopen_;
  bool verify_options_manifest_tracking_ = false;
  // True while a reopen holds auto compactions off so that a background
  // compaction cannot append to the MANIFEST before
  // VerifyManifestNotRewritten() measures it.
  bool auto_compactions_paused_for_manifest_verify_ = false;
};

// Load options from OPTIONS file and populate `options`.
bool InitializeOptionsFromFile(Options& options);

// Initialize `options` using command line arguments.
// When this function is called, `cache`, `block_cache_compressed`,
// `filter_policy` have all been initialized. Therefore, we just pass them as
// input arguments.
void InitializeOptionsFromFlags(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    const std::shared_ptr<IndexFactory>& udi_factory, Options& options);

// Initialize `options` on which `InitializeOptionsFromFile()` and
// `InitializeOptionsFromFlags()` have both been called already.
// There are two cases.
// Case 1: OPTIONS file is not specified. Command line arguments have been used
//         to initialize `options`. InitializeOptionsGeneral() will use
//         `cache` and `filter_policy` to initialize
//         corresponding fields of `options`. InitializeOptionsGeneral() will
//         also set up other fields of `options` so that stress test can run.
//         Examples include `create_if_missing` and
//         `create_missing_column_families`, etc.
// Case 2: OPTIONS file is specified. It is possible that, after loading from
//         the given OPTIONS files, some shared object fields are still not
//         initialized because they are not set in the OPTIONS file. In this
//         case, if command line arguments indicate that the user wants to set
//         up such shared objects, e.g. block cache, compressed block cache,
//         row cache, filter policy, then InitializeOptionsGeneral() will honor
//         the user's choice, thus passing `cache`,
//         `filter_policy` as input arguments.
//
// InitializeOptionsGeneral() must not overwrite fields of `options` loaded
// from OPTIONS file.
void InitializeOptionsGeneral(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    const std::shared_ptr<SstQueryFilterConfigsManager::Factory>& sqfc_factory,
    Options& options);

// If no OPTIONS file is specified, set up `options` so that we can test
// user-defined timestamp which requires `-user_timestamp_size=8`.
// This function also checks for known (currently) incompatible features with
// user-defined timestamp.
void CheckAndSetOptionsForUserTimestamp(Options& options);

bool ShouldDisableAutoCompactionsBeforeVerifyDb();
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
