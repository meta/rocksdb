//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#pragma once

#include <cinttypes>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "db_stress_tool/db_stress_compaction_service.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "file/writable_file_writer.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/unique_id.h"
#include "util/atomic.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "utilities/fault_injection_fs.h"
DECLARE_int32(compact_files_one_in);

namespace ROCKSDB_NAMESPACE {

// Verify across process executions that all seen IDs are unique
class UniqueIdVerifier {
 public:
  explicit UniqueIdVerifier(const std::string& dir);
  ~UniqueIdVerifier();

  void Verify(const std::string& id);

 private:
  void VerifyNoWrite(const std::string& id);

 private:
  std::mutex mutex_;
  // IDs persisted to a hidden file inside expected_values_dir (or DB dir)
  std::string path_;
  std::unique_ptr<WritableFileWriter> data_file_writer_;
  // Starting byte for which 8 bytes to check in memory within 24 byte ID
  size_t offset_;
  // Working copy of the set of 8 byte pieces
  std::unordered_set<uint64_t> id_set_;
};

class DbStressListener : public EventListener {
 public:
  DbStressListener(const std::string& db_name,
                   const std::vector<DbPath>& db_paths,
                   const std::vector<ColumnFamilyDescriptor>& column_families,
                   SharedState* shared);

  // No copying allowed
  DbStressListener(const DbStressListener&) = delete;
  DbStressListener& operator=(const DbStressListener&) = delete;
  DbStressListener(DbStressListener&&) = delete;
  DbStressListener& operator=(DbStressListener&&) = delete;

  const char* Name() const override { return kClassName(); }
  static const char* kClassName() { return "DBStressListener"; }

  ~DbStressListener() override { assert(num_pending_file_creations_ == 0); }

  void OnDBShutdownBegin(DB* /*db*/) override { shutting_down_.Store(true); }

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    assert(IsValidColumnFamilyName(info.cf_name));
    VerifyFilePath(info.file_path);
    // pretending doing some work here
    RandomSleep();
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
    }
    shared_->SetPersistedSeqno(info.largest_seqno);
  }

  void OnFlushBegin(DB* /*db*/,
                    const FlushJobInfo& /*flush_job_info*/) override {
    RandomSleep();
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kRead, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kRead);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kWrite, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kWrite);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataRead,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataWrite,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }
  }

  void OnTableFileDeleted(const TableFileDeletionInfo& info) override {
    // A file must not be deleted while it is still between
    // OnCompactionBegin and OnCompactionPreCommit (i.e. actively being
    // compacted with being_compacted == true). (Perhaps more realistically,
    // this is checking for failure to call OnCompactionPreCommit.)
    //
    // During shutdown, compaction notifications are skipped so tracking
    // state may be stale -- skip the check.
    if (!shutting_down_.Load()) {
      std::lock_guard<std::mutex> lock(compacting_files_mu_);
      uint64_t file_number = FileNumberFromPath(info.file_path);
      if (file_number != 0 &&
          compacting_files_.find(file_number) != compacting_files_.end()) {
        fprintf(stderr,
                "OnTableFileDeleted for file tracked as being compacted "
                "(between Begin and PreCommit): file_number=%" PRIu64 "\n",
                file_number);
        fflush(stderr);
        std::abort();
      }
    }
    RandomSleep();
  }

  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
    // Sanity check inspired by a Meta-internal check: the same input file must
    // not be in two concurrent compactions. Inserting input file numbers into a
    // shared set on Begin and removing them on PreCommit (rather than on
    // Completed) is what exercises the new OnCompactionPreCommit
    // callback. Removing on Completed would race with another picker that
    // grabs the same file as soon as ReleaseCompactionFiles flips
    // FileMetaData::being_compacted back to false; PreCommit runs
    // while being_compacted is still true and so closes that race.
    {
      std::lock_guard<std::mutex> lock(compacting_files_mu_);
      for (const auto& info : ci.input_file_infos) {
        auto [_, inserted] = compacting_files_.insert(info.file_number);
        if (!inserted) {
          fprintf(stderr,
                  "Concurrent compaction of SST file detected: cf=%s "
                  "file_number=%" PRIu64 "\n",
                  ci.cf_name.c_str(), info.file_number);
          fflush(stderr);
          std::abort();
        }
      }
    }
    RandomSleep();
  }

  void OnCompactionPreCommit(DB* /*db*/, const CompactionJobInfo& ci) override {
    // Pair with OnCompactionBegin's bookkeeping: move files from
    // compacting_files_ and record the job for Completed verification.
    {
      std::lock_guard<std::mutex> lock(compacting_files_mu_);
      std::unordered_set<uint64_t> job_files;
      for (const auto& info : ci.input_file_infos) {
        size_t erased = compacting_files_.erase(info.file_number);
        if (erased != 1) {
          fprintf(stderr,
                  "OnCompactionPreCommit for file not in tracking set: "
                  "cf=%s file_number=%" PRIu64 "\n",
                  ci.cf_name.c_str(), info.file_number);
          fflush(stderr);
          std::abort();
        }
        job_files.insert(info.file_number);
      }
      auto [_, inserted] =
          precommitted_jobs_.emplace(ci.job_id, std::move(job_files));
      if (!inserted) {
        fprintf(stderr, "OnCompactionPreCommit: duplicate job_id %d\n",
                ci.job_id);
        fflush(stderr);
        std::abort();
      }
    }
    RandomSleep();
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    assert(IsValidColumnFamilyName(ci.cf_name));
    assert(ci.input_files.size() + ci.output_files.size() > 0U);
    for (const auto& file_path : ci.input_files) {
      VerifyFilePath(file_path);
    }
    for (const auto& file_path : ci.output_files) {
      VerifyFilePath(file_path);
    }

    // Verify that OnCompactionPreCommit fired before OnCompactionCompleted
    // for the same job, with matching input files.
    {
      std::lock_guard<std::mutex> lock(compacting_files_mu_);
      auto it = precommitted_jobs_.find(ci.job_id);
      if (it == precommitted_jobs_.end()) {
        fprintf(stderr,
                "OnCompactionCompleted without prior OnCompactionPreCommit: "
                "cf=%s job_id=%d\n",
                ci.cf_name.c_str(), ci.job_id);
        fflush(stderr);
        std::abort();
      }
      // Verify input file sets match between PreCommit and Completed.
      for (const auto& info : ci.input_file_infos) {
        if (it->second.find(info.file_number) == it->second.end()) {
          fprintf(stderr,
                  "OnCompactionCompleted: input file %" PRIu64
                  " not in PreCommit set for job_id=%d\n",
                  info.file_number, ci.job_id);
          fflush(stderr);
          std::abort();
        }
      }
      precommitted_jobs_.erase(it);
    }

    if (ci.status.ok() && !ci.aborted) {
      shared_->IncSuccessfulCompactions();
    }

    // pretending doing some work here
    RandomSleep();
  }

  void OnSubcompactionBegin(const SubcompactionJobInfo& /* si */) override {
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kRead, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kRead);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kWrite, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kWrite);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataRead,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataWrite,
          static_cast<uint32_t>(FLAGS_seed), FLAGS_metadata_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }
  }

  void OnSubcompactionCompleted(const SubcompactionJobInfo& /* si */) override {
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
    }
  }

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*info*/) override {
    ++num_pending_file_creations_;
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    assert(info.db_name == db_name_);
    assert(IsValidColumnFamilyName(info.cf_name));
    assert(info.job_id > 0 || FLAGS_compact_files_one_in > 0);
    if (info.status.ok()) {
      assert(info.file_size > 0);
      VerifyFilePath(info.file_path);
      assert(info.table_properties.data_size > 0 ||
             info.table_properties.num_range_deletions > 0);
      assert(info.table_properties.raw_key_size > 0);
      assert(info.table_properties.num_entries > 0);
      // A copy of an existing table file (e.g. for a FIFO temperature change)
      // keeps the original's properties, so its unique ID is not new. It was
      // verified when the original was created.
      if (info.table_properties.orig_file_number ==
          FileNumberFromPath(info.file_path)) {
        VerifyTableFileUniqueId(info.table_properties, info.file_path);
      }
    }
    --num_pending_file_creations_;
  }

  void OnMemTableSealed(const MemTableInfo& /*info*/) override {
    RandomSleep();
  }

  void OnColumnFamilyHandleDeletionStarted(
      ColumnFamilyHandle* /*handle*/) override {
    RandomSleep();
  }

  void OnExternalFileIngested(DB* /*db*/,
                              const ExternalFileIngestionInfo& info) override {
    RandomSleep();
    // Here we assume that each generated external file is ingested
    // exactly once (or thrown away in case of crash)
    VerifyTableFileUniqueId(info.table_properties, info.internal_file_path);
  }

  void OnBackgroundError(BackgroundErrorReason /* reason */,
                         Status* /* bg_error */) override {
    RandomSleep();
  }

  void OnStallConditionsChanged(const WriteStallInfo& /*info*/) override {
    RandomSleep();
  }

  void OnBackgroundJobPressureChanged(
      DB* /*db*/, const BackgroundJobPressure& pressure) override {
    RandomSleep();
    std::lock_guard<std::mutex> lk(bg_pressure_mu_);
    last_bg_pressure_ = pressure;
  }

  void OnFileReadFinish(const FileOperationInfo& info) override {
    // Even empty callback is valuable because sometimes some locks are
    // released in order to make the callback.

    // Sleep carefully here as it is a frequent operation and we don't want
    // to slow down the tests. We always sleep when the read is large.
    // When read is small, sleep in a small chance.
    size_t length_read = info.length;
    if (length_read >= 1000000 || Random::GetTLSInstance()->OneIn(1000)) {
      RandomSleep();
    }
  }

  void OnFileWriteFinish(const FileOperationInfo& info) override {
    // Even empty callback is valuable because sometimes some locks are
    // released in order to make the callback.

    // Sleep carefully here as it is a frequent operation and we don't want
    // to slow down the tests. When the write is large, always sleep.
    // Otherwise, sleep in a relatively small chance.
    size_t length_write = info.length;
    if (length_write >= 1000000 || Random::GetTLSInstance()->OneIn(64)) {
      RandomSleep();
    }
  }

  bool ShouldBeNotifiedOnFileIO() override {
    RandomSleep();
    return static_cast<bool>(Random::GetTLSInstance()->OneIn(1));
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /* reason */,
                            Status /* bg_error */,
                            bool* /* auto_recovery */) override {
    RandomSleep();
    if (FLAGS_error_recovery_with_no_fault_injection &&
        db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
      // TODO(hx235): only exempt the flush thread during error recovery instead
      // of all the flush threads from error injection
      db_fault_injection_fs_->SetIOActivitiesExcludedFromFaultInjection(
          {Env::IOActivity::kFlush});
    }
  }

  void OnErrorRecoveryEnd(
      const BackgroundErrorRecoveryInfo& /*info*/) override {
    RandomSleep();
    if (FLAGS_error_recovery_with_no_fault_injection &&
        db_fault_injection_fs_) {
      db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
      db_fault_injection_fs_->SetIOActivitiesExcludedFromFaultInjection({});
    }
  }

 protected:
  bool IsValidColumnFamilyName(const std::string& cf_name) const {
    if (cf_name == kDefaultColumnFamilyName) {
      return true;
    }
    // The column family names in the stress tests are numbers.
    for (size_t i = 0; i < cf_name.size(); ++i) {
      if (cf_name[i] < '0' || cf_name[i] > '9') {
        return false;
      }
    }
    return true;
  }

  void VerifyFileDir(const std::string& file_dir) {
#ifndef NDEBUG
    if (db_name_ == file_dir) {
      return;
    }
    for (const auto& db_path : db_paths_) {
      if (db_path.path == file_dir) {
        return;
      }
    }
    for (auto& cf : column_families_) {
      for (const auto& cf_path : cf.options.cf_paths) {
        if (cf_path.path == file_dir) {
          return;
        }
      }
    }
    // We can't do exact matching since remote workers use dynamic temp paths.
    if (file_dir.find(DbStressCompactionService::kTempOutputDirectoryPrefix) !=
        std::string::npos) {
      return;
    }
    assert(false);
#else
    (void)file_dir;
#endif  // !NDEBUG
  }

  void VerifyFileName(const std::string& file_name) {
#ifndef NDEBUG
    uint64_t file_number;
    FileType file_type;
    bool result = ParseFileName(file_name, &file_number, &file_type);
    assert(result);
    assert(file_type == kTableFile);
#else
    (void)file_name;
#endif  // !NDEBUG
  }

  void VerifyFilePath(const std::string& file_path) {
#ifndef NDEBUG
    size_t pos = file_path.find_last_of("/");
    if (pos == std::string::npos) {
      VerifyFileName(file_path);
    } else {
      if (pos > 0) {
        VerifyFileDir(file_path.substr(0, pos));
      }
      VerifyFileName(file_path.substr(pos));
    }
#else
    (void)file_path;
#endif  // !NDEBUG
  }

  // Unique id is verified using the TableProperties. file_path is only used
  // for reporting.
  void VerifyTableFileUniqueId(const TableProperties& new_file_properties,
                               const std::string& file_path);

  void RandomSleep() {
    std::this_thread::sleep_for(
        std::chrono::microseconds(Random::GetTLSInstance()->Uniform(5000)));
  }

 private:
  std::string db_name_;
  std::vector<DbPath> db_paths_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  std::atomic<int> num_pending_file_creations_;
  UniqueIdVerifier unique_ids_;
  SharedState* shared_;
  std::shared_ptr<FaultInjectionTestFS> db_fault_injection_fs_;
  mutable std::mutex bg_pressure_mu_;
  BackgroundJobPressure last_bg_pressure_;
  // Files (by file_number) currently in flight from OnCompactionBegin to
  // OnCompactionPreCommit. Used to detect concurrent compaction of
  // the same SST file -- the bug fixed by the OnCompactionPreCommit
  // callback. Protected by compacting_files_mu_.
  std::mutex compacting_files_mu_;
  std::unordered_set<uint64_t> compacting_files_;
  // Set when DBImpl begins shutdown to suppress false positives from stale
  // tracking when compaction callbacks are skipped during shutdown.
  Atomic<bool> shutting_down_{false};
  // Jobs that have passed OnCompactionPreCommit but not yet
  // OnCompactionCompleted. Maps job_id -> input file numbers.
  // Used to verify Begin -> PreCommit -> Completed ordering per job.
  // Protected by compacting_files_mu_.
  std::unordered_map<int, std::unordered_set<uint64_t>> precommitted_jobs_;

  // Extract file number from a file path like "/path/to/000123.sst".
  // Returns 0 if the path cannot be parsed.
  static uint64_t FileNumberFromPath(const std::string& file_path) {
    size_t pos = file_path.find_last_of('/');
    // Avoid copying file_path when no '/' separator is found
    std::string file_name_buf;
    if (pos != std::string::npos) {
      file_name_buf = file_path.substr(pos + 1);
    }
    const std::string& file_name =
        (pos == std::string::npos) ? file_path : file_name_buf;
    uint64_t file_number = 0;
    FileType file_type;
    if (ParseFileName(file_name, &file_number, &file_type) &&
        file_type == kTableFile) {
      return file_number;
    }
    return 0;
  }
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
