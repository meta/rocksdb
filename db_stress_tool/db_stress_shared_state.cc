//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#ifdef GFLAGS
#include "db_stress_tool/db_stress_shared_state.h"

#include <algorithm>
#include <atomic>
#include <charconv>
#include <cstring>
#include <new>

#include "db_stress_tool/db_stress_test_base.h"
#include "port/port.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
namespace {

using BreadcrumbRecordState = std::atomic<char>;

constexpr size_t kOperationBreadcrumbRecordBytes = 512;
constexpr size_t kOperationBreadcrumbHeaderBytes =
    kOperationBreadcrumbRecordBytes;
constexpr size_t kOperationBreadcrumbRecordsPerInitializationWrite = 128;

std::string SanitizePathComponent(const std::string& value) {
  std::string result;
  result.reserve(value.size());
  for (char c : value) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.') {
      result.push_back(c);
    } else {
      result.push_back('_');
    }
  }
  return result;
}

template <typename Integer>
void AppendInteger(Integer value, std::string* output) {
  char buffer[32];
  const auto result = std::to_chars(buffer, buffer + sizeof(buffer), value);
  assert(result.ec == std::errc());
  output->append(buffer, result.ptr);
}

void AppendSanitizedRecordValue(const std::string& value, size_t length,
                                std::string* output) {
  assert(length <= value.size());
  for (size_t i = 0; i < length; ++i) {
    const char c = value[i];
    output->push_back(c == '\n' || c == '\r' || c == '\t' ? ' ' : c);
  }
}

std::string OperationBreadcrumbFilePath(const ThreadState& thread,
                                        uint64_t file_id) {
  std::string path = FLAGS_stress_diagnostics_dir;
  if (!path.empty() && path.back() != '/') {
    path.push_back('/');
  }
  const StressTest* const stress_test = thread.shared->GetStressTest();
  const std::string db_label =
      stress_test ? stress_test->GetDbLabel() : "unknown_db";
  path.append(SanitizePathComponent(db_label));
  path.append(".pid_");
  path.append(std::to_string(port::GetProcessID()));
  path.append(".thread_");
  path.append(std::to_string(file_id));
  path.append(".breadcrumbs.txt");
  return path;
}

}  // namespace

thread_local bool SharedState::ignore_read_error;

SharedState::SharedState(Env* env, StressTest* stress_test)
    : cv_(&mu_),
      env_(env != nullptr ? env : Env::Default()),
      seed_(static_cast<uint32_t>(FLAGS_seed)),
      max_key_(FLAGS_max_key),
      log2_keys_per_lock_(static_cast<uint32_t>(FLAGS_log2_keys_per_lock)),
      num_threads_(0),
      num_initialized_(0),
      num_populated_(0),
      vote_reopen_(0),
      num_done_(0),
      start_(false),
      start_verify_(false),
      operation_started_(false),
      operation_finished_(false),
      num_bg_threads_(0),
      should_stop_bg_thread_(false),
      bg_thread_finished_(0),
      stress_test_(stress_test),
      finished_ops_(0),
      successful_compactions_(0),
      successful_compactions_at_last_compaction_abort_(0),
      abort_resume_compactions_running_(false),
      verification_failure_(false),
      should_stop_test_(false),
      no_overwrite_ids_(GenerateNoOverwriteIds()),
      expected_state_manager_(nullptr),
      printing_verification_results_(false),
      start_timestamp_(env_->NowNanos()) {
  for (auto& completed_ops : completed_ops_by_type_) {
    completed_ops.store(0, std::memory_order_relaxed);
  }

  Status status;
  // TODO: We should introduce a way to explicitly disable verification
  // during shutdown. When that is disabled and FLAGS_expected_values_dir
  // is empty (disabling verification at startup), we can skip tracking
  // expected state. Only then should we permit bypassing the below feature
  // compatibility checks.
  const auto& expected_values_dir = stress_test_->GetExpectedValuesDir();
  if (!expected_values_dir.empty()) {
    if (!std::atomic<uint32_t>{}.is_lock_free() ||
        !std::atomic<uint64_t>{}.is_lock_free()) {
      std::ostringstream status_s;
      status_s << "Cannot use --expected_values_dir on platforms without "
                  "lock-free "
               << (!std::atomic<uint32_t>{}.is_lock_free()
                       ? "std::atomic<uint32_t>"
                       : "std::atomic<uint64_t>");
      status = Status::InvalidArgument(status_s.str());
    }

    if (status.ok() && FLAGS_clear_column_family_one_in > 0) {
      status = Status::InvalidArgument(
          "Cannot use --expected_values_dir on when "
          "--clear_column_family_one_in is greater than zero.");
    }
  }
  if (status.ok()) {
    if (expected_values_dir.empty()) {
      expected_state_manager_.reset(
          new AnonExpectedStateManager(FLAGS_max_key, FLAGS_column_families));
    } else {
      expected_state_manager_.reset(new FileExpectedStateManager(
          FLAGS_max_key, FLAGS_column_families, expected_values_dir));
    }
    status = expected_state_manager_->Open();
  }
  DB_STRESS_ASSERT_OK_MSG(status, "Failed setting up expected state");

  if (FLAGS_test_batches_snapshots) {
    fprintf(stdout, "No lock creation because test_batches_snapshots set\n");
    return;
  }

  long num_locks = static_cast<long>(max_key_ >> log2_keys_per_lock_);
  if (max_key_ & ((1 << log2_keys_per_lock_) - 1)) {
    num_locks++;
  }
  fprintf(stdout, "Creating %ld locks\n", num_locks * FLAGS_column_families);
  key_locks_.resize(FLAGS_column_families);

  for (int i = 0; i < FLAGS_column_families; ++i) {
    key_locks_[i].reset(new port::Mutex[num_locks]);
  }
  if (FLAGS_read_fault_one_in || FLAGS_metadata_read_fault_one_in) {
#ifdef NDEBUG
    // Unsupported in release mode because it relies on
    // `IGNORE_STATUS_IF_ERROR` to distinguish faults not expected to lead to
    // failure.
    fprintf(stderr,
            "Cannot set nonzero value for --read_fault_one_in in "
            "release mode.");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
#else         // NDEBUG
    SyncPoint::GetInstance()->SetCallBack("FaultInjectionIgnoreError",
                                          IgnoreReadErrorCallback);
    SyncPoint::GetInstance()->EnableProcessing();
#endif        // NDEBUG
  }
}

bool SharedState::BeginOperation(uint32_t tid, StressOperationType type) {
  assert(tid < static_cast<uint32_t>(num_threads_));
  ThreadOperationState& state = thread_operation_states_[tid];
  const uint32_t active_type =
      state.active_type.load(std::memory_order_acquire);
  assert(active_type == static_cast<uint32_t>(StressOperationType::kNone));
  if (active_type != static_cast<uint32_t>(StressOperationType::kNone)) {
    return false;
  }
  state.started_micros.store(env_->NowMicros(), std::memory_order_relaxed);
  state.active_type.store(static_cast<uint32_t>(type),
                          std::memory_order_release);
  return true;
}

ThreadState::ThreadState(uint32_t index, SharedState* _shared)
    : tid(index),
      rand(1000 + index + _shared->GetSeed()),
      shared(_shared),
      operation_breadcrumb_pos(0),
      operation_breadcrumb_sequence(0),
      operation_breadcrumb_file_id(0),
      operation_breadcrumb_setup_attempted(false),
      diagnostic_io_disabled(false),
      operation_ordinal(0),
      current_operation_ordinal(0) {}

ThreadState::~ThreadState() {
  if (operation_breadcrumb_mapping == nullptr) {
    return;
  }
  const size_t entry_count = (operation_breadcrumb_mapping->GetLen() -
                              kOperationBreadcrumbHeaderBytes) /
                             kOperationBreadcrumbRecordBytes;
  char* const base =
      static_cast<char*>(operation_breadcrumb_mapping->GetBase());
  for (size_t i = 0; i < entry_count; ++i) {
    char* const slot = base + kOperationBreadcrumbHeaderBytes +
                       i * kOperationBreadcrumbRecordBytes;
    BreadcrumbRecordState* const state =
        std::launder(reinterpret_cast<BreadcrumbRecordState*>(slot));
    state->~BreadcrumbRecordState();
  }
}

bool ThreadState::OperationBreadcrumbsEnabled() const {
  return FLAGS_stress_diagnostics_breadcrumbs &&
         FLAGS_stress_diagnostics_breadcrumb_entries > 0 &&
         !FLAGS_stress_diagnostics_dir.empty() && !diagnostic_io_disabled &&
         (!operation_breadcrumb_setup_attempted ||
          operation_breadcrumb_mapping != nullptr);
}

bool ThreadState::InitializeOperationBreadcrumbs() {
  if (operation_breadcrumb_mapping != nullptr) {
    return true;
  }
  if (!OperationBreadcrumbsEnabled()) {
    return false;
  }
  operation_breadcrumb_setup_attempted = true;

  if (sizeof(BreadcrumbRecordState) != sizeof(char) ||
      kOperationBreadcrumbRecordBytes % alignof(BreadcrumbRecordState) != 0 ||
      !BreadcrumbRecordState{}.is_lock_free()) {
    fprintf(stdout,
            "Operation breadcrumbs require a lock-free one-byte atomic "
            "record marker\n");
    return false;
  }

  Env* const env = Env::Default();
  Status s = env->CreateDirIfMissing(FLAGS_stress_diagnostics_dir);
  if (!s.ok()) {
    // Some Env implementations report an error when another thread creates
    // the directory between their existence check and create call.
    Status exists_status = env->FileExists(FLAGS_stress_diagnostics_dir);
    if (!exists_status.ok()) {
      fprintf(stdout, "Failed to create stress diagnostics directory %s: %s\n",
              FLAGS_stress_diagnostics_dir.c_str(), s.ToString().c_str());
      diagnostic_io_disabled = true;
      return false;
    }
  }

  static std::atomic<uint64_t> next_operation_breadcrumb_file_id{0};
  operation_breadcrumb_file_id =
      next_operation_breadcrumb_file_id.fetch_add(1, std::memory_order_relaxed);
  const std::string path =
      OperationBreadcrumbFilePath(*this, operation_breadcrumb_file_id);
  std::unique_ptr<WritableFile> file;
  s = env->NewWritableFile(path, &file, EnvOptions());
  if (!s.ok()) {
    fprintf(stdout, "Failed to create operation breadcrumb file %s: %s\n",
            path.c_str(), s.ToString().c_str());
    return false;
  }

  std::string header =
      "# mmap_breadcrumbs_v1 record_state=V(valid),I(incomplete),E(empty) "
      "order_by=seq power_loss_durable=0 pid=";
  AppendInteger(port::GetProcessID(), &header);
  header.append(" tid=");
  AppendInteger(tid, &header);
  header.append(" file_id=");
  AppendInteger(operation_breadcrumb_file_id, &header);
  header.append(" seed=");
  AppendInteger(shared->GetSeed(), &header);
  assert(header.size() < kOperationBreadcrumbHeaderBytes);
  header.resize(kOperationBreadcrumbHeaderBytes - 1, ' ');
  header.push_back('\n');

  Status write_status = file->Append(Slice(header));
  std::string empty_records(kOperationBreadcrumbRecordsPerInitializationWrite *
                                kOperationBreadcrumbRecordBytes,
                            ' ');
  for (size_t i = 0; i < kOperationBreadcrumbRecordsPerInitializationWrite;
       ++i) {
    empty_records[i * kOperationBreadcrumbRecordBytes] = 'E';
    empty_records[(i + 1) * kOperationBreadcrumbRecordBytes - 1] = '\n';
  }

  const size_t entry_count = FLAGS_stress_diagnostics_breadcrumb_entries;
  for (size_t first = 0; write_status.ok() && first < entry_count;
       first += kOperationBreadcrumbRecordsPerInitializationWrite) {
    const size_t count = std::min(
        kOperationBreadcrumbRecordsPerInitializationWrite, entry_count - first);
    write_status = file->Append(
        Slice(empty_records.data(), count * kOperationBreadcrumbRecordBytes));
  }

  Status close_status = file->Close();
  file.reset();
  if (!write_status.ok()) {
    s = write_status;
  } else if (!close_status.ok()) {
    s = close_status;
  }
  if (!s.ok()) {
    env->DeleteFile(path).PermitUncheckedError();
    fprintf(stdout, "Failed to initialize operation breadcrumb file %s: %s\n",
            path.c_str(), s.ToString().c_str());
    return false;
  }

  std::unique_ptr<MemoryMappedFileBuffer> mapping;
  s = env->NewMemoryMappedFileBuffer(path, &mapping);
  const size_t expected_size = kOperationBreadcrumbHeaderBytes +
                               entry_count * kOperationBreadcrumbRecordBytes;
  if (s.ok() && (mapping == nullptr || mapping->GetBase() == nullptr ||
                 mapping->GetLen() != expected_size)) {
    s = Status::IOError("Unexpected operation breadcrumb mapping size");
  }
  if (!s.ok()) {
    mapping.reset();
    env->DeleteFile(path).PermitUncheckedError();
    fprintf(stdout, "Failed to map operation breadcrumb file %s: %s\n",
            path.c_str(), s.ToString().c_str());
    return false;
  }

  char* const base = static_cast<char*>(mapping->GetBase());
  for (size_t i = 0; i < entry_count; ++i) {
    char* const slot = base + kOperationBreadcrumbHeaderBytes +
                       i * kOperationBreadcrumbRecordBytes;
    new (slot) BreadcrumbRecordState('E');
  }
  operation_breadcrumb_mapping = std::move(mapping);
  operation_breadcrumb_details.reserve(kOperationBreadcrumbRecordBytes);
  operation_breadcrumb_record.reserve(kOperationBreadcrumbRecordBytes);
  return true;
}

void ThreadState::AppendOperationBreadcrumb(StressOperationType type,
                                            const char* phase,
                                            const std::string& details) {
  if (!InitializeOperationBreadcrumbs()) {
    return;
  }

  const uint64_t sequence = ++operation_breadcrumb_sequence;
  operation_breadcrumb_record.clear();
  operation_breadcrumb_record.append(" seq=");
  AppendInteger(sequence, &operation_breadcrumb_record);
  operation_breadcrumb_record.append(" time_micros=");
  AppendInteger(shared->GetEnv()->NowMicros(), &operation_breadcrumb_record);
  operation_breadcrumb_record.append(" tid=");
  AppendInteger(tid, &operation_breadcrumb_record);
  operation_breadcrumb_record.append(" ordinal=");
  AppendInteger(current_operation_ordinal, &operation_breadcrumb_record);
  operation_breadcrumb_record.append(" op=");
  operation_breadcrumb_record.append(StressOperationTypeName(type));
  operation_breadcrumb_record.append(" phase=");
  operation_breadcrumb_record.append(phase);

  constexpr size_t kMaxRecordContentBytes = kOperationBreadcrumbRecordBytes - 2;
  assert(operation_breadcrumb_record.size() < kMaxRecordContentBytes);
  if (!details.empty()) {
    operation_breadcrumb_record.push_back(' ');
    const size_t available =
        kMaxRecordContentBytes - operation_breadcrumb_record.size();
    if (details.size() <= available) {
      AppendSanitizedRecordValue(details, details.size(),
                                 &operation_breadcrumb_record);
    } else {
      static constexpr char kTruncatedSuffix[] = " details_truncated=1";
      constexpr size_t kTruncatedSuffixBytes = sizeof(kTruncatedSuffix) - 1;
      assert(available >= kTruncatedSuffixBytes);
      AppendSanitizedRecordValue(details, available - kTruncatedSuffixBytes,
                                 &operation_breadcrumb_record);
      operation_breadcrumb_record.append(kTruncatedSuffix);
    }
  }

  const size_t entry_count = (operation_breadcrumb_mapping->GetLen() -
                              kOperationBreadcrumbHeaderBytes) /
                             kOperationBreadcrumbRecordBytes;
  char* const base =
      static_cast<char*>(operation_breadcrumb_mapping->GetBase());
  char* const slot = base + kOperationBreadcrumbHeaderBytes +
                     operation_breadcrumb_pos * kOperationBreadcrumbRecordBytes;
  BreadcrumbRecordState* const state =
      std::launder(reinterpret_cast<BreadcrumbRecordState*>(slot));
  // The acquire half prevents the following body stores from becoming visible
  // before the slot is invalidated on weakly ordered architectures.
  state->exchange('I', std::memory_order_acq_rel);
  std::memset(slot + 1, ' ', kOperationBreadcrumbRecordBytes - 2);
  slot[kOperationBreadcrumbRecordBytes - 1] = '\n';
  std::memcpy(slot + 1, operation_breadcrumb_record.data(),
              operation_breadcrumb_record.size());
  state->store('V', std::memory_order_release);

  operation_breadcrumb_pos = (operation_breadcrumb_pos + 1) % entry_count;
}

std::string* ThreadState::PrepareOperationEvent() {
  if (!InitializeOperationBreadcrumbs()) {
    return nullptr;
  }
  operation_breadcrumb_details.clear();
  return &operation_breadcrumb_details;
}

void ThreadState::CommitOperationEvent(StressOperationType type) {
  AppendOperationBreadcrumb(type, "event", operation_breadcrumb_details);
}

bool ThreadState::BeginOperation(StressOperationType type) {
  ++operation_ordinal;
  current_operation_ordinal = operation_ordinal;

  if (OperationBreadcrumbsEnabled()) {
    AppendOperationBreadcrumb(type, "begin", "");
  }

  if (LivenessTrackingEnabled()) {
    return shared->BeginOperation(tid, type);
  }
  return false;
}

void ThreadState::RecordOperationEnd(StressOperationType type) {
  if (!OperationBreadcrumbsEnabled()) {
    return;
  }
  const bool verification_failed = shared->HasVerificationFailedYet();
  AppendOperationBreadcrumb(
      type, "end", verification_failed ? "verification_failure=1" : "");
}

bool SharedState::ShouldVerifyAtBeginning() const {
  return !stress_test_->GetExpectedValuesDir().empty();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
