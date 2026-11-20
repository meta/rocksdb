//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

#ifdef GFLAGS
#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "db_stress_tool/db_stress_stat.h"
#include "db_stress_tool/expected_state.h"
// SyncPoint is not supported in Released Windows Mode.
#if !(defined NDEBUG) || !defined(OS_WIN)
#include "test_util/sync_point.h"
#endif  // !(defined NDEBUG) || !defined(OS_WIN)
#include "util/gflags_compat.h"

DECLARE_uint64(seed);
DECLARE_int64(max_key);
DECLARE_uint64(log2_keys_per_lock);
DECLARE_int32(threads);
DECLARE_int32(column_families);
DECLARE_int32(nooverwritepercent);
DECLARE_int32(clear_column_family_one_in);
DECLARE_bool(test_batches_snapshots);
DECLARE_int32(compaction_thread_pool_adjust_interval);
DECLARE_int32(continuous_verification_interval);
DECLARE_bool(error_recovery_with_no_fault_injection);
DECLARE_bool(sync_fault_injection);
DECLARE_uint64(liveness_check_interval_sec);
DECLARE_uint64(liveness_no_progress_timeout_sec);
DECLARE_string(stress_diagnostics_dir);
DECLARE_bool(stress_diagnostics_breadcrumbs);
DECLARE_uint64(stress_diagnostics_breadcrumb_entries);
DECLARE_bool(stress_diagnostics_scan_witness);
DECLARE_uint64(stress_diagnostics_scan_witness_entries);
DECLARE_int32(range_deletion_width);
DECLARE_bool(disable_wal);
DECLARE_int32(manual_wal_flush_one_in);
DECLARE_int32(metadata_read_fault_one_in);
DECLARE_int32(metadata_write_fault_one_in);
DECLARE_int32(read_fault_one_in);
DECLARE_int32(write_fault_one_in);
DECLARE_bool(exclude_wal_from_write_fault_injection);
DECLARE_bool(file_scope_wal_write_faults);
DECLARE_int32(open_metadata_read_fault_one_in);
DECLARE_int32(open_metadata_write_fault_one_in);
DECLARE_int32(open_write_fault_one_in);
DECLARE_int32(open_read_fault_one_in);

DECLARE_int32(inject_error_severity);
DECLARE_bool(tolerate_non_injected_io_errors_for_remote_dbs);
DECLARE_bool(disable_auto_compactions);
DECLARE_bool(enable_compaction_filter);

namespace ROCKSDB_NAMESPACE {
class MemoryMappedFileBuffer;
class StressTest;

enum class StressOperationType : uint32_t {
  kNone = 0,
  kReopen,
  kSetOptions,
  kVerifyDb,
  kManualWalFlush,
  kLockWal,
  kSyncWal,
  kCompactFiles,
  kCompactRange,
  kFlush,
  kGetLiveFiles,
  kMetadata,
  kPauseBackground,
  kDisableFileDeletions,
  kDisableManualCompaction,
  kAbortResumeCompactions,
  kVerifyChecksum,
  kVerifyFileChecksums,
  kGetProperty,
  kTableProperties,
  kIngestExternalFile,
  kBackup,
  kCheckpoint,
  kApproximateSize,
  kSnapshot,
  kKeyMayExist,
  kRead,
  kPrefixScan,
  kWrite,
  kDelete,
  kDeleteRange,
  kIterate,
  kCustom,
  kCount,
};

static constexpr size_t kStressOperationTypeCount =
    static_cast<size_t>(StressOperationType::kCount);

inline const char* StressOperationTypeName(StressOperationType type) {
  switch (type) {
    case StressOperationType::kNone:
      return "none";
    case StressOperationType::kReopen:
      return "reopen";
    case StressOperationType::kSetOptions:
      return "set_options";
    case StressOperationType::kVerifyDb:
      return "verify_db";
    case StressOperationType::kManualWalFlush:
      return "manual_wal_flush";
    case StressOperationType::kLockWal:
      return "lock_wal";
    case StressOperationType::kSyncWal:
      return "sync_wal";
    case StressOperationType::kCompactFiles:
      return "compact_files";
    case StressOperationType::kCompactRange:
      return "compact_range";
    case StressOperationType::kFlush:
      return "flush";
    case StressOperationType::kGetLiveFiles:
      return "get_live_files";
    case StressOperationType::kMetadata:
      return "metadata";
    case StressOperationType::kPauseBackground:
      return "pause_background";
    case StressOperationType::kDisableFileDeletions:
      return "disable_file_deletions";
    case StressOperationType::kDisableManualCompaction:
      return "disable_manual_compaction";
    case StressOperationType::kAbortResumeCompactions:
      return "abort_resume_compactions";
    case StressOperationType::kVerifyChecksum:
      return "verify_checksum";
    case StressOperationType::kVerifyFileChecksums:
      return "verify_file_checksums";
    case StressOperationType::kGetProperty:
      return "get_property";
    case StressOperationType::kTableProperties:
      return "table_properties";
    case StressOperationType::kIngestExternalFile:
      return "ingest_external_file";
    case StressOperationType::kBackup:
      return "backup";
    case StressOperationType::kCheckpoint:
      return "checkpoint";
    case StressOperationType::kApproximateSize:
      return "approximate_size";
    case StressOperationType::kSnapshot:
      return "snapshot";
    case StressOperationType::kKeyMayExist:
      return "key_may_exist";
    case StressOperationType::kRead:
      return "read";
    case StressOperationType::kPrefixScan:
      return "prefix_scan";
    case StressOperationType::kWrite:
      return "write";
    case StressOperationType::kDelete:
      return "delete";
    case StressOperationType::kDeleteRange:
      return "delete_range";
    case StressOperationType::kIterate:
      return "iterate";
    case StressOperationType::kCustom:
      return "custom";
    case StressOperationType::kCount:
      break;
  }
  return "unknown";
}

struct ThreadOperationSnapshot {
  StressOperationType type;
  uint64_t started_micros;
};

struct ThreadOperationState {
  ThreadOperationState()
      : active_type(static_cast<uint32_t>(StressOperationType::kNone)),
        started_micros(0) {}

  std::atomic<uint32_t> active_type;
  std::atomic<uint64_t> started_micros;
};

struct RemoteCompactionQueueItem {
  std::string job_id;
  CompactionServiceJobInfo job_info;
  std::string serialized_input;
  std::string output_directory;
  bool canceled;

  RemoteCompactionQueueItem(const std::string& id,
                            const CompactionServiceJobInfo& info,
                            const std::string& input,
                            const std::string& output_dir, bool was_canceled)
      : job_id(id),
        job_info(info),
        serialized_input(input),
        output_directory(output_dir),
        canceled(was_canceled) {}
};

// State shared by all concurrent executions of the same benchmark.
class SharedState {
 public:
  // Errors when reading filter blocks are ignored, so we use a thread
  // local variable updated via sync points to keep track of errors injected
  // while reading filter blocks in order to ignore the Get/MultiGet result
  // for those calls
  static thread_local bool ignore_read_error;

  SharedState(Env* env, StressTest* stress_test);

  // No copying allowed
  SharedState(const SharedState&) = delete;
  SharedState& operator=(const SharedState&) = delete;
  SharedState(SharedState&&) = delete;
  SharedState& operator=(SharedState&&) = delete;

  ~SharedState() {
#ifndef NDEBUG
    if (FLAGS_read_fault_one_in || FLAGS_write_fault_one_in ||
        FLAGS_metadata_write_fault_one_in) {
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->DisableProcessing();
    }
#endif
  }

  port::Mutex* GetMutex() { return &mu_; }

  port::CondVar* GetCondVar() { return &cv_; }

  StressTest* GetStressTest() const { return stress_test_; }

  int64_t GetMaxKey() const { return max_key_; }

  uint32_t GetNumThreads() const { return num_threads_; }

  void SetThreads(int num_threads) {
    num_threads_ = num_threads;
    thread_operation_states_.reset(new ThreadOperationState[num_threads]);
  }

  void IncInitialized() { num_initialized_++; }

  void IncOperated() {
    num_populated_++;
    if (num_populated_ >= num_threads_) {
      operation_finished_.store(true, std::memory_order_release);
    }
  }

  void IncDone() { num_done_++; }

  void IncVotedReopen() { vote_reopen_ = (vote_reopen_ + 1) % num_threads_; }

  bool AllInitialized() const { return num_initialized_ >= num_threads_; }

  bool AllOperated() const { return num_populated_ >= num_threads_; }

  bool AllDone() const { return num_done_ >= num_threads_; }

  bool AllVotedReopen() { return (vote_reopen_ == 0); }

  void SetStart() {
    start_ = true;
    operation_started_.store(true, std::memory_order_release);
  }

  void SetStartVerify() { start_verify_ = true; }

  bool Started() const { return start_; }

  bool VerifyStarted() const { return start_verify_; }

  bool OperationStarted() const {
    return operation_started_.load(std::memory_order_acquire);
  }

  bool OperationFinished() const {
    return operation_finished_.load(std::memory_order_acquire);
  }

  void SetVerificationFailure() { verification_failure_.store(true); }

  bool HasVerificationFailedYet() const { return verification_failure_.load(); }

  Env* GetEnv() const { return env_; }

  void IncFinishedOps() {
    finished_ops_.fetch_add(1, std::memory_order_relaxed);
  }

  uint64_t GetFinishedOps() const {
    return finished_ops_.load(std::memory_order_relaxed);
  }

  void IncCompletedOpForDiagnostics(StressOperationType type) {
    const size_t index = static_cast<size_t>(type);
    if (index == static_cast<size_t>(StressOperationType::kNone) ||
        index >= kStressOperationTypeCount) {
      return;
    }
    completed_ops_by_type_[index].fetch_add(1, std::memory_order_relaxed);
  }

  uint64_t GetCompletedOpsForDiagnostics(StressOperationType type) const {
    const size_t index = static_cast<size_t>(type);
    if (index >= kStressOperationTypeCount) {
      return 0;
    }
    return completed_ops_by_type_[index].load(std::memory_order_relaxed);
  }

  void IncSuccessfulCompactions() {
    successful_compactions_.fetch_add(1, std::memory_order_relaxed);
  }

  uint64_t GetSuccessfulCompactions() const {
    return successful_compactions_.load(std::memory_order_relaxed);
  }

  bool TryBeginAbortAndResumeCompactions(uint64_t* successful_compactions) {
    bool expected = false;
    if (!abort_resume_compactions_running_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return false;
    }

    *successful_compactions = GetSuccessfulCompactions();
    if (*successful_compactions <=
        successful_compactions_at_last_compaction_abort_.load(
            std::memory_order_relaxed)) {
      EndAbortAndResumeCompactions();
      return false;
    }
    return true;
  }

  void MarkAbortAndResumeCompactions(uint64_t successful_compactions) {
    successful_compactions_at_last_compaction_abort_.store(
        successful_compactions, std::memory_order_relaxed);
  }

  void EndAbortAndResumeCompactions() {
    abort_resume_compactions_running_.store(false, std::memory_order_release);
  }

  bool BeginOperation(uint32_t tid, StressOperationType type);

  bool EndOperation(uint32_t tid, StressOperationType type) {
    assert(tid < static_cast<uint32_t>(num_threads_));
    ThreadOperationState& state = thread_operation_states_[tid];
    const uint32_t active_type =
        state.active_type.load(std::memory_order_acquire);
    assert(active_type == static_cast<uint32_t>(type));
    if (active_type != static_cast<uint32_t>(type)) {
      return false;
    }
    state.active_type.store(static_cast<uint32_t>(StressOperationType::kNone),
                            std::memory_order_release);
    state.started_micros.store(0, std::memory_order_relaxed);
    return true;
  }

  void ClearOperation(uint32_t tid) {
    assert(tid < static_cast<uint32_t>(num_threads_));
    ThreadOperationState& state = thread_operation_states_[tid];
    state.active_type.store(static_cast<uint32_t>(StressOperationType::kNone),
                            std::memory_order_release);
    state.started_micros.store(0, std::memory_order_relaxed);
  }

  ThreadOperationSnapshot GetThreadOperationSnapshot(uint32_t tid) const {
    assert(tid < static_cast<uint32_t>(num_threads_));
    const ThreadOperationState& state = thread_operation_states_[tid];
    // This is a diagnostic snapshot, not one atomic value. A concurrent
    // begin/end/clear can produce a torn pair; timeout checks must treat
    // `kNone` and `started_micros == 0` as no active operation.
    const auto type = static_cast<StressOperationType>(
        state.active_type.load(std::memory_order_acquire));
    if (type == StressOperationType::kNone) {
      return {StressOperationType::kNone, 0};
    }
    return {type, state.started_micros.load(std::memory_order_relaxed)};
  }

  void SetShouldStopTest() { should_stop_test_.store(true); }

  bool ShouldStopTest() const { return should_stop_test_.load(); }

  // Returns a lock covering `key` in `cf`.
  port::Mutex* GetMutexForKey(int cf, int64_t key) {
    return &key_locks_[cf][key >> log2_keys_per_lock_];
  }

  // Acquires locks for all keys in `cf`.
  void LockColumnFamily(int cf) {
    for (int i = 0; i < max_key_ >> log2_keys_per_lock_; ++i) {
      key_locks_[cf][i].Lock();
    }
  }

  // Releases locks for all keys in `cf`.
  void UnlockColumnFamily(int cf) {
    for (int i = 0; i < max_key_ >> log2_keys_per_lock_; ++i) {
      key_locks_[cf][i].Unlock();
    }
  }

  // Returns a collection of mutex locks covering the key range [start, end) in
  // `cf`.
  std::vector<std::unique_ptr<MutexLock>> GetLocksForKeyRange(int cf,
                                                              int64_t start,
                                                              int64_t end) {
    std::vector<std::unique_ptr<MutexLock>> range_locks;

    if (start >= end) {
      return range_locks;
    }

    const int64_t start_idx = start >> log2_keys_per_lock_;

    int64_t end_idx = end >> log2_keys_per_lock_;
    if ((end & ((1 << log2_keys_per_lock_) - 1)) == 0) {
      --end_idx;
    }

    for (int64_t idx = start_idx; idx <= end_idx; ++idx) {
      range_locks.emplace_back(
          std::make_unique<MutexLock>(&key_locks_[cf][idx]));
    }

    return range_locks;
  }

  Status SaveAtAndAfter(DB* db) {
    return expected_state_manager_->SaveAtAndAfter(db);
  }

  bool HasHistory() { return expected_state_manager_->HasHistory(); }

  Status Restore(DB* db) { return expected_state_manager_->Restore(db); }

  // Requires external locking covering all keys in `cf`.
  void ClearColumnFamily(int cf) {
    return expected_state_manager_->ClearColumnFamily(cf);
  }

  void SetPersistedSeqno(SequenceNumber seqno) {
    MutexLock l(&persist_seqno_mu_);
    return expected_state_manager_->SetPersistedSeqno(seqno);
  }

  SequenceNumber GetPersistedSeqno() {
    MutexLock l(&persist_seqno_mu_);
    return expected_state_manager_->GetPersistedSeqno();
  }

  void EnqueueRemoteCompaction(const std::string& job_id,
                               const CompactionServiceJobInfo& job_info,
                               const std::string& serialized_input,
                               const std::string& output_directory,
                               bool canceled) {
    MutexLock l(&remote_compaction_queue_mu_);
    remote_compaction_queue_.emplace(job_id, job_info, serialized_input,
                                     output_directory, canceled);
  }

  bool DequeueRemoteCompaction(std::string* job_id,
                               CompactionServiceJobInfo* job_info,
                               std::string* serialized_input,
                               std::string* output_directory, bool* canceled) {
    assert(job_id);
    assert(job_info);
    assert(serialized_input);
    assert(output_directory);
    assert(canceled);
    MutexLock l(&remote_compaction_queue_mu_);
    if (!remote_compaction_queue_.empty()) {
      const RemoteCompactionQueueItem& item = remote_compaction_queue_.front();
      *job_id = item.job_id;
      *job_info = item.job_info;
      *serialized_input = item.serialized_input;
      *output_directory = item.output_directory;
      *canceled = item.canceled;
      remote_compaction_queue_.pop();
      return true;
    }
    return false;
  }

  void AddRemoteCompactionResult(const std::string& job_id,
                                 const Status& status,
                                 const std::string& result) {
    MutexLock l(&remote_compaction_result_map_mu_);
    remote_compaction_result_map_.emplace(
        job_id, std::pair<Status, std::string>{status, result});
  }

  std::optional<Status> GetRemoteCompactionResult(const std::string& job_id,
                                                  std::string* result) {
    MutexLock l(&remote_compaction_result_map_mu_);
    if (remote_compaction_result_map_.find(job_id) !=
        remote_compaction_result_map_.end()) {
      const auto& pair = remote_compaction_result_map_.at(job_id);
      *result = pair.second;
      return pair.first;
    }
    return std::nullopt;
  }

  void RemoveRemoteCompactionResult(const std::string& job_id) {
    MutexLock l(&remote_compaction_result_map_mu_);
    remote_compaction_result_map_.erase(job_id);
  }

  // Prepare a Put that will be started but not finish yet
  // This is useful for crash-recovery testing when the process may crash
  // before updating the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` to prevent
  // concurrent write or delete to the same `key`.
  PendingExpectedValue PreparePut(int cf, int64_t key) {
    return expected_state_manager_->PreparePut(cf, key);
  }

  // Does not requires external locking.
  ExpectedValue Get(int cf, int64_t key) {
    return expected_state_manager_->Get(cf, key);
  }

  // Prepare a Delete that will be started but not finish yet
  // This is useful for crash-recovery testing when the process may crash
  // before updating the corresponding expected value
  //
  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PrepareDelete(int cf, int64_t key) {
    return expected_state_manager_->PrepareDelete(cf, key);
  }

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // write or delete to the same `key`.
  PendingExpectedValue PrepareSingleDelete(int cf, int64_t key) {
    return expected_state_manager_->PrepareSingleDelete(cf, key);
  }

  // Requires external locking covering keys in `[begin_key, end_key)` in `cf`
  // to prevent concurrent write or delete to the same `key`.
  std::vector<PendingExpectedValue> PrepareDeleteRange(int cf,
                                                       int64_t begin_key,
                                                       int64_t end_key) {
    return expected_state_manager_->PrepareDeleteRange(cf, begin_key, end_key);
  }

  bool AllowsOverwrite(int64_t key) const {
    return no_overwrite_ids_.find(key) == no_overwrite_ids_.end();
  }

  // Requires external locking covering `key` in `cf` to prevent concurrent
  // delete to the same `key`.
  bool Exists(int cf, int64_t key) {
    return expected_state_manager_->Exists(cf, key);
  }

  // Sync the `value_base` to the corresponding expected value
  void SyncPut(int cf, int64_t key, uint32_t value_base) {
    return expected_state_manager_->SyncPut(cf, key, value_base);
  }

  // Sync the corresponding expected value to be pending Put
  void SyncPendingPut(int cf, int64_t key) {
    return expected_state_manager_->SyncPendingPut(cf, key);
  }

  // Sync the corresponding expected value to be deleted
  void SyncDelete(int cf, int64_t key) {
    return expected_state_manager_->SyncDelete(cf, key);
  }

  uint32_t GetSeed() const { return seed_; }

  void SetShouldStopBgThread() { should_stop_bg_thread_ = true; }

  bool ShouldStopBgThread() { return should_stop_bg_thread_; }

  void IncBgThreads() { ++num_bg_threads_; }

  void IncBgThreadsFinished() { ++bg_thread_finished_; }

  bool BgThreadsFinished() const {
    return bg_thread_finished_ == num_bg_threads_;
  }

  bool ShouldVerifyAtBeginning() const;

  bool PrintingVerificationResults() {
    bool tmp = false;
    return !printing_verification_results_.compare_exchange_strong(
        tmp, true, std::memory_order_relaxed);
  }

  void FinishPrintingVerificationResults() {
    printing_verification_results_.store(false, std::memory_order_relaxed);
  }

  uint64_t GetStartTimestamp() const { return start_timestamp_; }

  [[noreturn]] void SafeTerminate() {
    // Grab mutex so that we don't call terminate while another thread is
    // attempting to print a stack trace due to the first one.
    MutexLock l(&mu_);
    std::terminate();
  }

  [[noreturn]] void TerminateWithoutMutex() {
    // The liveness watchdog must still terminate when the no-progress failure
    // mode itself is holding the shared harness mutex.
    std::terminate();
  }

 private:
  static void IgnoreReadErrorCallback(void*) { ignore_read_error = true; }

  // Pick random keys in each column family that will not experience overwrite.
  std::unordered_set<int64_t> GenerateNoOverwriteIds() const {
    fprintf(stdout, "Choosing random keys with no overwrite\n");
    // Start with the identity permutation. Subsequent iterations of
    // for loop below will start with perm of previous for loop
    std::vector<int64_t> permutation(max_key_);
    for (int64_t i = 0; i < max_key_; ++i) {
      permutation[i] = i;
    }
    // Now do the Knuth shuffle
    const int64_t num_no_overwrite_keys =
        (max_key_ * FLAGS_nooverwritepercent) / 100;
    // Only need to figure out first num_no_overwrite_keys of permutation
    std::unordered_set<int64_t> ret;
    ret.reserve(num_no_overwrite_keys);
    Random64 rnd(seed_);
    for (int64_t i = 0; i < num_no_overwrite_keys; i++) {
      assert(i < max_key_);
      int64_t rand_index = i + rnd.Next() % (max_key_ - i);
      // Swap i and rand_index;
      int64_t temp = permutation[i];
      permutation[i] = permutation[rand_index];
      permutation[rand_index] = temp;
      // Fill no_overwrite_ids_ with the first num_no_overwrite_keys of
      // permutation
      ret.insert(permutation[i]);
    }
    return ret;
  }

  port::Mutex mu_;
  port::CondVar cv_;
  Env* env_;
  port::Mutex persist_seqno_mu_;
  const uint32_t seed_;
  const int64_t max_key_;
  const uint32_t log2_keys_per_lock_;
  int num_threads_;
  long num_initialized_;
  long num_populated_;
  long vote_reopen_;
  long num_done_;
  bool start_;
  bool start_verify_;
  std::atomic<bool> operation_started_;
  std::atomic<bool> operation_finished_;
  int num_bg_threads_;
  bool should_stop_bg_thread_;
  int bg_thread_finished_;
  StressTest* stress_test_;
  std::atomic<uint64_t> finished_ops_;
  std::array<std::atomic<uint64_t>, kStressOperationTypeCount>
      completed_ops_by_type_;
  std::atomic<uint64_t> successful_compactions_;
  std::atomic<uint64_t> successful_compactions_at_last_compaction_abort_;
  std::atomic<bool> abort_resume_compactions_running_;
  std::atomic<bool> verification_failure_;
  std::atomic<bool> should_stop_test_;
  std::unique_ptr<ThreadOperationState[]> thread_operation_states_;

  // Queue for the remote compaction.
  port::Mutex remote_compaction_queue_mu_;
  std::queue<RemoteCompactionQueueItem> remote_compaction_queue_;
  // Result Map for the remote compaciton. Key is the scheduled_job_id and value
  // is serialized compaction_service_result
  port::Mutex remote_compaction_result_map_mu_;
  std::unordered_map<std::string, std::pair<Status, std::string>>
      remote_compaction_result_map_;

  // Keys that should not be overwritten
  const std::unordered_set<int64_t> no_overwrite_ids_;

  std::unique_ptr<ExpectedStateManager> expected_state_manager_;
  // Cannot store `port::Mutex` directly in vector since it is not copyable
  // and storing it in the container may require copying depending on the impl.
  std::vector<std::unique_ptr<port::Mutex[]>> key_locks_;
  std::atomic<bool> printing_verification_results_;
  const uint64_t start_timestamp_;
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  uint32_t tid;  // 0..n-1
  Random rand;   // Has different seeds for different threads
  SharedState* shared;
  Stats stats;
  std::unique_ptr<MemoryMappedFileBuffer> operation_breadcrumb_mapping;
  std::string operation_breadcrumb_details;
  std::string operation_breadcrumb_record;
  size_t operation_breadcrumb_pos;
  uint64_t operation_breadcrumb_sequence;
  uint64_t operation_breadcrumb_file_id;
  bool operation_breadcrumb_setup_attempted;
  bool diagnostic_io_disabled;
  uint64_t operation_ordinal;
  uint64_t current_operation_ordinal;
  struct SnapshotState {
    const Snapshot* snapshot;
    // The cf from which we did a Get at this snapshot
    int cf_at;
    // The name of the cf at the time that we did a read
    std::string cf_at_name;
    // The key with which we did a Get at this snapshot
    std::string key;
    // The status of the Get
    Status status;
    // The value of the Get
    std::string value;
    // optional state of all keys in the db
    std::vector<bool>* key_vec;

    std::string timestamp;
  };
  std::queue<std::pair<uint64_t, SnapshotState>> snapshot_queue;

  ThreadState(uint32_t index, SharedState* _shared);
  ~ThreadState();

  bool LivenessTrackingEnabled() const {
    return FLAGS_liveness_check_interval_sec > 0 &&
           FLAGS_liveness_no_progress_timeout_sec > 0;
  }

  bool BeginOperation(StressOperationType type);

  bool EndOperation(StressOperationType type) {
    if (LivenessTrackingEnabled()) {
      return shared->EndOperation(tid, type);
    }
    return false;
  }

  void ClearOperation() {
    if (LivenessTrackingEnabled()) {
      shared->ClearOperation(tid);
    }
  }

  bool OperationBreadcrumbsEnabled() const;

  std::string* PrepareOperationEvent();

  void CommitOperationEvent(StressOperationType type);

  void RecordOperationEnd(StressOperationType type);

  void CompletedOpForDiagnostics(StressOperationType type) {
    if (LivenessTrackingEnabled()) {
      shared->IncCompletedOpForDiagnostics(type);
    }
  }

  void RecordSingleOpFinished() {
    stats.FinishedSingleOp();
    if (LivenessTrackingEnabled()) {
      shared->IncFinishedOps();
    }
  }

  void FinishedSingleOp() {
    RecordSingleOpFinished();
    ClearOperation();
  }

 private:
  bool InitializeOperationBreadcrumbs();
  void AppendOperationBreadcrumb(StressOperationType type, const char* phase,
                                 const std::string& details);
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
