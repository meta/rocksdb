//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <initializer_list>
#include <iomanip>
#include <ios>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>
#include <unordered_set>

#include "db_stress_tool/db_stress_compression_manager.h"
#include "db_stress_tool/db_stress_listener.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "util/compression.h"
#ifdef GFLAGS
#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_compaction_filter.h"
#include "db_stress_tool/db_stress_compaction_service.h"
#include "db_stress_tool/db_stress_driver.h"
#include "db_stress_tool/db_stress_filters.h"
#include "db_stress_tool/db_stress_table_properties_collector.h"
#include "db_stress_tool/db_stress_wide_merge_operator.h"
#include "file/file_util.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/io_dispatcher.h"
#include "rocksdb/lazy_wide_columns.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/simple_external_table_factory.h"
#include "test_util/testutil.h"
#include "util/aligned_buffer.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/simple_mixed_compressor.h"
#include "utilities/backup/backup_engine_impl.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/fault_injection_secondary_cache.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr int kMaxAbortResumeCompactionsSleepMicros = 3 * 1000 * 1000;

class ScopedThreadOperation {
 public:
  enum class FinishAction { kPop, kFinishSingleOp };

  ScopedThreadOperation(ThreadState* thread, StressOperationType type,
                        FinishAction finish_action = FinishAction::kPop)
      : thread_(thread),
        type_(type),
        finish_action_(finish_action),
        tracking_(false) {
    tracking_ = thread_->BeginOperation(type);
  }

  ~ScopedThreadOperation() {
    if (tracking_) {
      thread_->CompletedOpForDiagnostics(type_);
    }
    thread_->RecordOperationEnd(type_);
    switch (finish_action_) {
      case FinishAction::kPop:
        break;
      case FinishAction::kFinishSingleOp:
        thread_->RecordSingleOpFinished();
        break;
    }
    if (tracking_) {
      thread_->EndOperation(type_);
    }
  }

 private:
  ThreadState* thread_;
  StressOperationType type_;
  FinishAction finish_action_;
  bool tracking_;
};

template <typename Integer>
void AppendOperationContextInteger(Integer value, std::string* output) {
  char buffer[32];
  const auto result = std::to_chars(buffer, buffer + sizeof(buffer), value);
  assert(result.ec == std::errc());
  output->append(buffer, result.ptr);
}

template <typename ColumnFamilies, typename Keys>
void FormatOperationContext(const ColumnFamilies& column_families,
                            const Keys& keys, std::string* output) {
  output->clear();
  output->append("cfs=[");
  bool first = true;
  for (int column_family : column_families) {
    if (!first) {
      output->push_back(',');
    }
    first = false;
    AppendOperationContextInteger(column_family, output);
  }
  output->append("] keys=[");
  first = true;
  for (int64_t key : keys) {
    if (!first) {
      output->push_back(',');
    }
    first = false;
    AppendOperationContextInteger(key, output);
  }
  output->push_back(']');
}

template <typename ColumnFamilies, typename Keys>
void MaybeRecordOperationContextImpl(ThreadState* thread,
                                     StressOperationType type,
                                     const ColumnFamilies& column_families,
                                     const Keys& keys) {
  if (!thread->OperationBreadcrumbsEnabled()) {
    return;
  }
  std::string* details = thread->PrepareOperationEvent();
  if (details == nullptr) {
    return;
  }
  FormatOperationContext(column_families, keys, details);
  thread->CommitOperationEvent(type);
}

void MaybeRecordOperationContext(ThreadState* thread, StressOperationType type,
                                 const std::vector<int>& column_families,
                                 const std::vector<int64_t>& keys) {
  MaybeRecordOperationContextImpl(thread, type, column_families, keys);
}

void MaybeRecordOperationContext(ThreadState* thread, StressOperationType type,
                                 std::initializer_list<int> column_families,
                                 std::initializer_list<int64_t> keys) {
  MaybeRecordOperationContextImpl(thread, type, column_families, keys);
}

}  // namespace

std::string SanitizeDiagnosticValue(const std::string& value) {
  std::string result;
  result.reserve(value.size());
  for (char c : value) {
    if (c == '\n' || c == '\r' || c == '\t') {
      result.push_back(' ');
    } else {
      result.push_back(c);
    }
  }
  return result;
}

namespace {

constexpr size_t kMaxScanWitnessEventBytes = 4096;

std::string SanitizeDiagnosticPathComponent(const std::string& value) {
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

}  // namespace

std::string FormatDiagnosticSlice(const Slice& value) {
  constexpr size_t kMaxFullHexBytes = 80;
  constexpr size_t kPrefixBytes = 64;
  constexpr size_t kSuffixBytes = 16;

  std::ostringstream oss;
  oss << "size=" << value.size();
  if (value.size() <= kMaxFullHexBytes) {
    oss << " hex=" << value.ToString(/*hex=*/true);
  } else {
    oss << " hex_prefix="
        << Slice(value.data(), kPrefixBytes).ToString(/*hex=*/true)
        << " hex_suffix="
        << Slice(value.data() + value.size() - kSuffixBytes, kSuffixBytes)
               .ToString(/*hex=*/true)
        << " hash64=" << GetSliceHash64(value);
  }
  return oss.str();
}

namespace {

std::string FormatOptionalDiagnosticSlice(const Slice* value) {
  if (value == nullptr) {
    return "nullptr";
  }
  return FormatDiagnosticSlice(*value);
}

}  // namespace

std::string FormatReadOptionsForDiagnostics(const ReadOptions& ro) {
  std::ostringstream oss;
  oss << "verify_checksums=" << ro.verify_checksums
      << " fill_cache=" << ro.fill_cache << " async_io=" << ro.async_io
      << " total_order_seek=" << ro.total_order_seek
      << " auto_prefix_mode=" << ro.auto_prefix_mode
      << " prefix_same_as_start=" << ro.prefix_same_as_start
      << " iterate_lower_bound={"
      << FormatOptionalDiagnosticSlice(ro.iterate_lower_bound) << "}"
      << " iterate_upper_bound={"
      << FormatOptionalDiagnosticSlice(ro.iterate_upper_bound) << "}"
      << " allow_unprepared_value=" << ro.allow_unprepared_value
      << " auto_refresh_iterator_with_snapshot="
      << ro.auto_refresh_iterator_with_snapshot << " snapshot_seq=";
  if (ro.snapshot != nullptr) {
    oss << ro.snapshot->GetSequenceNumber();
  } else {
    oss << "none";
  }
  oss << " timestamp={" << FormatOptionalDiagnosticSlice(ro.timestamp) << "}"
      << " iter_start_ts={" << FormatOptionalDiagnosticSlice(ro.iter_start_ts)
      << "}";
  return oss.str();
}

ScanWitnessLog::ScanWitnessLog(ThreadState* thread, const char* scan_type)
    : thread_(thread), scan_type_(scan_type), next_entry_(0), wrapped_(false) {
  if (Enabled()) {
    entries_.resize(
        static_cast<size_t>(FLAGS_stress_diagnostics_scan_witness_entries));
  }
}

bool ScanWitnessLog::Enabled() const {
  return thread_ != nullptr && FLAGS_stress_diagnostics_scan_witness &&
         FLAGS_stress_diagnostics_scan_witness_entries > 0 &&
         !FLAGS_stress_diagnostics_dir.empty() &&
         !thread_->diagnostic_io_disabled;
}

void ScanWitnessLog::Add(const std::string& event) {
  if (entries_.empty()) {
    return;
  }

  std::ostringstream line;
  line << "time_micros=" << thread_->shared->GetEnv()->NowMicros()
       << " tid=" << thread_->tid
       << " ordinal=" << thread_->current_operation_ordinal
       << " scan_type=" << scan_type_ << " ";
  if (event.size() <= kMaxScanWitnessEventBytes) {
    line << SanitizeDiagnosticValue(event);
  } else {
    line << "truncated_event={" << FormatDiagnosticSlice(Slice(event)) << "}";
  }
  entries_[next_entry_] = line.str();
  next_entry_ = (next_entry_ + 1) % entries_.size();
  if (next_entry_ == 0) {
    wrapped_ = true;
  }
}

void ScanWitnessLog::AddIteratorState(const char* action, const char* label,
                                      const Iterator& iter,
                                      bool include_value) {
  if (entries_.empty()) {
    return;
  }

  std::ostringstream event;
  event << "action=" << action << " iterator=" << label
        << " valid=" << iter.Valid()
        << " status=" << SanitizeDiagnosticValue(iter.status().ToString());
  if (iter.Valid()) {
    event << " key={" << FormatDiagnosticSlice(iter.key()) << "}";
    if (include_value) {
      const Slice value = iter.value();
      event << " value_size=" << value.size()
            << " value_hash64=" << GetSliceHash64(value)
            << " wide_columns=" << iter.columns().size();
    } else {
      event << " value_unprepared=1";
    }
  }
  Add(event.str());
}

void ScanWitnessLog::AddIteratorState(const char* action, size_t label,
                                      const Iterator& iter,
                                      bool include_value) {
  if (entries_.empty()) {
    return;
  }
  const std::string label_string = std::to_string(label);
  AddIteratorState(action, label_string.c_str(), iter, include_value);
}

void ScanWitnessLog::Flush(const char* reason) const {
  if (entries_.empty() || thread_->diagnostic_io_disabled) {
    return;
  }

  Env* const env = Env::Default();
  Status s = env->CreateDirIfMissing(FLAGS_stress_diagnostics_dir);
  if (!s.ok()) {
    fprintf(stderr, "Failed to create stress diagnostics directory %s: %s\n",
            FLAGS_stress_diagnostics_dir.c_str(), s.ToString().c_str());
    thread_->diagnostic_io_disabled = true;
    return;
  }

  std::string path = FLAGS_stress_diagnostics_dir;
  if (!path.empty() && path.back() != '/') {
    path.push_back('/');
  }
  const StressTest* const stress_test = thread_->shared->GetStressTest();
  const std::string db_label =
      stress_test != nullptr ? stress_test->GetDbLabel() : "unknown_db";
  path.append(SanitizeDiagnosticPathComponent(db_label));
  path.append(".pid_");
  path.append(std::to_string(port::GetProcessID()));
  path.append(".thread_");
  path.append(std::to_string(thread_->tid));
  path.push_back('.');
  path.append(SanitizeDiagnosticPathComponent(scan_type_));
  path.append(".witness.txt");
  const std::string temp_path = path + ".tmp";

  std::unique_ptr<WritableFile> file;
  s = env->NewWritableFile(temp_path, &file, EnvOptions());
  if (!s.ok()) {
    fprintf(stderr, "Failed to create scan witness file %s: %s\n",
            temp_path.c_str(), s.ToString().c_str());
    thread_->diagnostic_io_disabled = true;
    return;
  }

  std::string output;
  output.append("# reason=");
  output.append(reason != nullptr ? reason : "unknown");
  output.append(" pid=");
  output.append(std::to_string(port::GetProcessID()));
  output.append(" tid=");
  output.append(std::to_string(thread_->tid));
  output.append(" ordinal=");
  output.append(std::to_string(thread_->current_operation_ordinal));
  output.append(" scan_type=");
  output.append(scan_type_);
  output.push_back('\n');

  const size_t entry_count = wrapped_ ? entries_.size() : next_entry_;
  const size_t first_entry = wrapped_ ? next_entry_ : 0;
  for (size_t i = 0; i < entry_count; ++i) {
    const size_t index = (first_entry + i) % entries_.size();
    output.append(entries_[index]);
    output.push_back('\n');
  }

  Status write_status = file->Append(Slice(output));
  Status close_status = file->Close();
  file.reset();
  if (!write_status.ok()) {
    s = write_status;
  } else if (!close_status.ok()) {
    s = close_status;
  } else {
    s = env->RenameFile(temp_path, path);
  }
  if (!s.ok()) {
    env->DeleteFile(temp_path).PermitUncheckedError();
    fprintf(stderr, "Failed to write scan witness file %s: %s\n", path.c_str(),
            s.ToString().c_str());
    thread_->diagnostic_io_disabled = true;
  }
}

namespace {

class StressReadScopedBlockBufferProvider
    : public ReadScopedBlockBufferProvider {
 public:
  StressReadScopedBlockBufferProvider() : state_(std::make_shared<State>()) {}
  StressReadScopedBlockBufferProvider(
      const StressReadScopedBlockBufferProvider&) = delete;
  StressReadScopedBlockBufferProvider& operator=(
      const StressReadScopedBlockBufferProvider&) = delete;
  StressReadScopedBlockBufferProvider(StressReadScopedBlockBufferProvider&&) =
      delete;
  StressReadScopedBlockBufferProvider& operator=(
      StressReadScopedBlockBufferProvider&&) = delete;

  ~StressReadScopedBlockBufferProvider() override {
    state_->ValidateNoLiveAllocations();
  }

  Status Allocate(size_t size, size_t alignment, Lease* out) override {
    Check(out != nullptr, "lease output is nullptr");
    Check(size > 0, "allocation size is zero");
    Check(alignment > 0 && (alignment & (alignment - 1)) == 0,
          "alignment must be a power of two");

    auto* allocation = new Allocation();
    allocation->state = state_;
    allocation->requested_size = size;
    allocation->requested_alignment = alignment;
    allocation->storage.Alignment(alignment);
    allocation->storage.AllocateNewBuffer(size);
    allocation->data = allocation->storage.BufferStart();
    allocation->size = allocation->storage.Capacity();

    Check(allocation->data != nullptr, "provider returned null data");
    Check(allocation->size >= size, "provider returned short allocation");
    Check(AlignedBuffer::isAligned(allocation->data, alignment),
          "provider returned misaligned data");
    Check(
        alignment == 1 || AlignedBuffer::isAligned(allocation->size, alignment),
        "provider returned misaligned direct-I/O size");

    {
      std::lock_guard<std::mutex> lock(state_->mutex);
      Check(state_->live_allocations.insert(allocation).second,
            "duplicate allocation registration");
      state_->live_bytes += allocation->size;
      ++state_->total_allocations;
    }

    out->data = allocation->data;
    out->size = allocation->size;
    out->cleanup.Allocate();
    out->cleanup->RegisterCleanup(&ReleaseAllocation, allocation, nullptr);
    return Status::OK();
  }

 private:
  struct Allocation;

  struct State {
    std::mutex mutex;
    std::unordered_set<Allocation*> live_allocations;
    size_t live_bytes = 0;
    uint64_t total_allocations = 0;

    void ValidateNoLiveAllocations() {
      std::lock_guard<std::mutex> lock(mutex);
      Check(live_allocations.empty(),
            "provider destroyed with live allocations");
      Check(live_bytes == 0, "provider destroyed with live bytes");
    }
  };

  struct Allocation {
    std::shared_ptr<State> state;
    size_t requested_size = 0;
    size_t requested_alignment = 1;
    char* data = nullptr;
    size_t size = 0;
    AlignedBuffer storage;
  };

  static void Check(bool condition, const char* message) {
    if (!condition) {
      fprintf(stderr, "ReadScopedBlockBufferProvider invariant failed: %s\n",
              message);
      std::abort();
    }
  }

  static void ReleaseAllocation(void* arg1, void* /*arg2*/) {
    auto* allocation = static_cast<Allocation*>(arg1);
    Check(allocation != nullptr, "cleanup received null allocation");
    Check(allocation->state != nullptr, "cleanup received null state");
    Check(allocation->data != nullptr, "cleanup received null data");
    Check(allocation->size >= allocation->requested_size,
          "cleanup received short allocation");
    Check(AlignedBuffer::isAligned(allocation->data,
                                   allocation->requested_alignment),
          "cleanup received misaligned data");

    {
      std::lock_guard<std::mutex> lock(allocation->state->mutex);
      Check(allocation->state->live_allocations.erase(allocation) == 1,
            "cleanup for unknown or duplicate allocation");
      Check(allocation->state->live_bytes >= allocation->size,
            "cleanup underflowed live bytes");
      allocation->state->live_bytes -= allocation->size;
    }
    delete allocation;
  }

  std::shared_ptr<State> state_;
};

std::shared_ptr<const FilterPolicy> CreateFilterPolicy() {
  if (FLAGS_bloom_bits < 0) {
    return BlockBasedTableOptions().filter_policy;
  }
  const FilterPolicy* new_policy;
  if (FLAGS_bloom_before_level == INT_MAX) {
    // Use Bloom API
    new_policy = NewBloomFilterPolicy(FLAGS_bloom_bits, false);
  } else {
    new_policy =
        NewRibbonFilterPolicy(FLAGS_bloom_bits, FLAGS_bloom_before_level);
  }
  return std::shared_ptr<const FilterPolicy>(new_policy);
}

struct OpenFaultInjectionConfig {
  bool sync_fault = false;
  int metadata_read_one_in = 0;
  int metadata_write_one_in = 0;
  int read_one_in = 0;
  int write_one_in = 0;

  bool Enabled() const {
    return sync_fault || metadata_read_one_in > 0 ||
           metadata_write_one_in > 0 || read_one_in > 0 || write_one_in > 0;
  }

  void Disable() {
    sync_fault = false;
    metadata_read_one_in = 0;
    metadata_write_one_in = 0;
    read_one_in = 0;
    write_one_in = 0;
  }
};

void EnableThreadLocalOpenFault(
    const std::shared_ptr<FaultInjectionTestFS>& fault_injection_fs,
    FaultInjectionIOType io_type, int one_in) {
  assert(fault_injection_fs);
  fault_injection_fs->SetThreadLocalErrorContext(
      io_type, static_cast<uint32_t>(FLAGS_seed), one_in, false /* retryable */,
      false /* has_data_loss */);
  fault_injection_fs->EnableThreadLocalErrorInjection(io_type);
}

bool MaybeEnableOpenFaultInjection(
    const std::shared_ptr<FaultInjectionTestFS>& fault_injection_fs,
    const std::string& db_path,
    const OpenFaultInjectionConfig& open_fault_injection) {
  // Returns whether open-fault handling is active for this attempt. The caller
  // uses this to run post-open cleanup/retry handling even if CURRENT did not
  // exist and no fault was actually enabled.
  if (!open_fault_injection.Enabled()) {
    return false;
  }
  assert(fault_injection_fs);
  if (!fault_injection_fs
           ->FileExists(db_path + "/CURRENT", IOOptions(), nullptr)
           .ok()) {
    return true;
  }

  if (open_fault_injection.sync_fault ||
      open_fault_injection.write_one_in > 0) {
    fault_injection_fs->SetFilesystemDirectWritable(false);
    fault_injection_fs->SetInjectUnsyncedDataLoss(
        open_fault_injection.sync_fault);
  }
  EnableThreadLocalOpenFault(fault_injection_fs,
                             FaultInjectionIOType::kMetadataRead,
                             open_fault_injection.metadata_read_one_in);
  EnableThreadLocalOpenFault(fault_injection_fs,
                             FaultInjectionIOType::kMetadataWrite,
                             open_fault_injection.metadata_write_one_in);
  EnableThreadLocalOpenFault(fault_injection_fs, FaultInjectionIOType::kRead,
                             open_fault_injection.read_one_in);
  EnableThreadLocalOpenFault(fault_injection_fs, FaultInjectionIOType::kWrite,
                             open_fault_injection.write_one_in);
  return true;
}

bool NeedsFaultInjection() {
  return FLAGS_open_metadata_read_fault_one_in ||
         FLAGS_open_metadata_write_fault_one_in ||
         FLAGS_open_read_fault_one_in || FLAGS_open_write_fault_one_in ||
         FLAGS_metadata_read_fault_one_in ||
         FLAGS_metadata_write_fault_one_in || FLAGS_read_fault_one_in ||
         FLAGS_write_fault_one_in || FLAGS_sync_fault_injection ||
         FLAGS_test_cf_lifecycle;
}

// Resolves --read_index. The auto value (-1) reads through the trie when it is
// built as the secondary index, which kDefault reads would never use.
ReadOptions::ReadIndex EffectiveReadIndex() {
  if (FLAGS_read_index >= 0) {
    return static_cast<ReadOptions::ReadIndex>(FLAGS_read_index);
  }
  const BlockBasedTableOptions::IndexMode index_mode =
      static_cast<BlockBasedTableOptions::IndexMode>(FLAGS_index_mode);
  const bool trie_is_secondary =
      FLAGS_use_trie_index &&
      (index_mode == BlockBasedTableOptions::IndexMode::kStandardDefault ||
       index_mode == BlockBasedTableOptions::IndexMode::kStandardRequired);
  return trie_is_secondary ? ReadOptions::ReadIndex::kPreferCustom
                           : ReadOptions::ReadIndex::kDefault;
}

std::string GetFaultInjectionLogBaseDir() {
  if (!FLAGS_env_uri.empty() || !FLAGS_fs_uri.empty()) {
    return "/tmp";
  }
  // db_stress reads environment variables during single-threaded startup,
  // before worker threads are created.
  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  const char* test_tmpdir = std::getenv("TEST_TMPDIR");
  return test_tmpdir != nullptr && test_tmpdir[0] != '\0' ? test_tmpdir
                                                          : "/tmp";
}

std::string GetFaultInjectionLogPath(const std::string& db_label) {
  const std::string log_dir =
      GetFaultInjectionLogBaseDir() + "/fault_injection_logs";
  Status s = Env::Default()->CreateDirIfMissing(log_dir);
  DB_STRESS_ASSERT_OK_MSG(s, "Failed to create directory %s", log_dir.c_str());
  return log_dir + "/fault_injection_" + std::to_string(port::GetProcessID()) +
         "_" + std::to_string(Env::Default()->NowMicros()) + "_" + db_label +
         ".bin";
}

Slice ExpectedValueSlice(const ExpectedValue& expected_value, char* scratch) {
  const size_t size =
      GenerateValue(expected_value.GetValueBase(), scratch, kValueMaxLen);
  return Slice(scratch, size);
}

struct DataCorruption {
  // "lost" | "resurrected" | "wrong-value" | "detected-corruption"
  const char* kind;
  Slice value_from_db;
  Slice value_from_expected;
  std::string op_status;
};

std::optional<DataCorruption> ClassifyReadBack(
    const Status& read_status, const std::string& db_value,
    const ExpectedValue& expected_value, char* scratch) {
  if (read_status.IsCorruption()) {
    return DataCorruption{"detected-corruption", db_value, Slice(),
                          "Get: " + read_status.ToString()};
  }
  // Fault injection is off (enforced at startup), so the read-back can only be
  // OK or NotFound here. Anything else is outside this tool's contract -- stop
  // now rather than guess a classification. ImmediateExit (not exit/abort): the
  // DB is open with live background threads, so a normal exit would race their
  // static teardown (see port::ImmediateExit).
  if (!read_status.ok() && !read_status.IsNotFound()) {
    fprintf(stderr, "verify_cpu_corruption: unexpected read-back status: %s\n",
            read_status.ToString().c_str());
    port::ImmediateExit(1);
  }
  const bool present_in_db = read_status.ok();
  // Exists() asserts the key is not mid-write -- which holds under --threads=1:
  // the just-run op committed before this read-back.
  const bool expected_to_exist = expected_value.Exists();

  if (!expected_to_exist && !present_in_db) {
    return std::nullopt;  // Agree: key absent.
  }
  if (expected_to_exist && !present_in_db) {
    return DataCorruption{"lost", Slice(),
                          ExpectedValueSlice(expected_value, scratch),
                          "Get: NotFound"};
  }
  if (!expected_to_exist && present_in_db) {
    return DataCorruption{"resurrected", db_value, Slice(), "Get: OK"};
  }
  // Present in both: compare the values.
  const Slice expected = ExpectedValueSlice(expected_value, scratch);
  if (Slice(db_value) == expected) {
    return std::nullopt;  // Agree: same value.
  }
  return DataCorruption{"wrong-value", db_value, expected, "Get: OK"};
}

void WriteDataCorruption(uint32_t thread_id, int cf, int64_t key,
                         const DataCorruption& corruption) {
  std::ostringstream oss;
  oss << "{\n"
      << "  \"kind\": \"" << corruption.kind << "\",\n"
      << "  \"cf\": " << cf << ",\n"
      << "  \"key\": " << key << ",\n"
      << "  \"value_from_db\": \""
      << corruption.value_from_db.ToString(/* hex */ true) << "\",\n"
      << "  \"value_from_expected\": \""
      << corruption.value_from_expected.ToString(/* hex */ true) << "\",\n"
      << "  \"op_status\": " << std::quoted(corruption.op_status) << "\n"
      << "}\n";
  const std::string path = FLAGS_verify_cpu_corruption_dir +
                           "/data_corruption." + std::to_string(thread_id) +
                           ".json";
  FILE* f = fopen(path.c_str(), "w");
  if (f == nullptr) {
    fprintf(stderr, "Failed to open CPU corruption result file: %s\n",
            path.c_str());
    port::ImmediateExit(1);
  }
  if (fputs(oss.str().c_str(), f) == EOF) {
    fprintf(stderr, "Failed to write CPU corruption result file: %s\n",
            path.c_str());
    port::ImmediateExit(1);
  }
  if (fclose(f) != 0) {
    fprintf(stderr, "Failed to close CPU corruption result file: %s\n",
            path.c_str());
    port::ImmediateExit(1);
  }
}

std::string JoinSorted(std::vector<std::string> names) {
  std::sort(names.begin(), names.end());
  std::string out;
  for (const auto& name : names) {
    if (!out.empty()) {
      out += ", ";
    }
    out += name;
  }
  return out;
}

Status VerifyColumnFamilyDescriptors(
    const std::vector<ColumnFamilyDescriptor>& descriptors,
    const std::vector<std::string>& expected_names, const char* context) {
  std::vector<std::string> actual_names;
  actual_names.reserve(descriptors.size());
  for (const auto& descriptor : descriptors) {
    actual_names.push_back(descriptor.name);
  }
  std::sort(actual_names.begin(), actual_names.end());
  std::vector<std::string> sorted_expected_names = expected_names;
  std::sort(sorted_expected_names.begin(), sorted_expected_names.end());
  if (actual_names == sorted_expected_names) {
    return Status::OK();
  }
  return Status::Corruption(
      context, "OPTIONS={" + JoinSorted(std::move(actual_names)) +
                   "}, MANIFEST={" +
                   JoinSorted(std::move(sorted_expected_names)) + "}");
}

// Checks that a column-family-subset checkpoint holds exactly the requested
// families: the excluded ones must have been recorded as dropped in the copied
// MANIFEST, and the requested ones must have survived that rewrite.
Status VerifyCheckpointColumnFamilies(
    const std::string& checkpoint_dir, const DBOptions& db_options,
    const std::vector<ColumnFamilyDescriptor>& expected_cf_descs) {
  std::vector<std::string> actual_names;
  Status s = DB::ListColumnFamilies(db_options, checkpoint_dir, &actual_names);
  if (!s.ok()) {
    return s;
  }
  std::vector<std::string> expected_names;
  expected_names.reserve(expected_cf_descs.size());
  for (const auto& desc : expected_cf_descs) {
    expected_names.push_back(desc.name);
  }
  const std::string actual = JoinSorted(std::move(actual_names));
  const std::string expected = JoinSorted(std::move(expected_names));
  if (actual == expected) {
    return Status::OK();
  }
  std::ostringstream oss;
  oss << "Subset checkpoint " << checkpoint_dir << " has column families {"
      << actual << "} but {" << expected << "} were requested";
  return Status::Corruption(oss.str());
}

}  // namespace

bool StressTest::IsTolerableNonInjectedRemoteIOError(const Status& error_s) {
  assert(!error_s.ok());
  return FLAGS_tolerate_non_injected_io_errors_for_remote_dbs &&
         (!FLAGS_env_uri.empty() || !FLAGS_fs_uri.empty()) &&
         error_s.IsIOError() &&
         !status_to_io_status(Status(error_s)).GetDataLoss();
}

bool StressTest::IsErrorInjectedAndRetryable(const Status& error_s) {
  assert(!error_s.ok());
  const bool injected_and_retryable =
      error_s.getState() && FaultInjectionTestFS::IsInjectedError(error_s) &&
      !status_to_io_status(Status(error_s)).GetDataLoss();
  return injected_and_retryable || IsTolerableNonInjectedRemoteIOError(error_s);
}

bool StressTest::IsRetryableOperationError(int injected_error_count,
                                           const Status& error_s) {
  assert(!error_s.ok());
  if (IsTolerableNonInjectedRemoteIOError(error_s)) {
    return true;
  }
  return injected_error_count > 0 && IsErrorInjectedAndRetryable(error_s);
}

bool StressTest::LazyEntityReadEnabled() const {
  // Exercising the lazy wide-column read API requires the DB to be opened with
  // max_open_files == -1 (the API returns InvalidArgument otherwise).
  //
  // Two combinations are excluded here, both matching documented limitations of
  // the API (see DB::GetEntityLazy() in include/rocksdb/db.h) rather than being
  // silent omissions:
  //
  //   * User-defined timestamps. Wide-column entities do not support UDT at
  //   all:
  //     PutEntity() returns InvalidArgument on a UDT column family (tested by
  //     DBWideBasicTest.PutEntityTimestampError; the lazy read is at parity
  //     with GetEntity() via the shared read path -- see
  //     DBLazyEntityTest.UserTimestampParityWithGetEntity). So no entity exists
  //     to read under UDT, and this harness does not supply the read timestamp
  //     a UDT column family requires; running it there would just hit the same
  //     InvalidArgument the eager path does. Nothing entity-specific to cover.
  //
  //   * Transactions. Transaction provides GetEntity()/MultiGetEntity() but has
  //     no lazy counterpart, so there is no in-transaction lazy read to
  //     exercise or to compare against the eager txn read (which also sees the
  //     txn's own uncommitted writes). Adding a Transaction-level lazy API is
  //     the tracked follow-up; until then this is a documented API limitation.
  return FLAGS_lazy_entity_read_one_in > 0 && FLAGS_open_files == -1 &&
         FLAGS_user_timestamp_size == 0 && !FLAGS_use_txn;
}

namespace {
// Tri-state presence of the eager reference in the lazy read differential.
enum class EagerPresence { kFound, kNotFound, kUnknown };
}  // namespace

void StressTest::MaybeTestGetEntityLazy(
    ThreadState* thread, const ReadOptions& read_opts, ColumnFamilyHandle* cfh,
    const Slice& key, const WideColumns* eager_reference, DB* db) const {
  assert(thread);
  if (!LazyEntityReadEnabled() ||
      !thread->rand.OneIn(FLAGS_lazy_entity_read_one_in)) {
    return;
  }

  // Default to the primary; a caller may target a read-only or secondary
  // instance instead (see below).
  DB* target = db != nullptr ? db : db_;

  // The crash test's first goal is to exercise code (find crashes/asserts/UB),
  // which does not require data-consistency verification. So the lazy read and
  // resolution below always run; the eager-reference comparison is additionally
  // done only when DB verification is enabled.
  const bool verify = !FLAGS_skip_verifydb;

  // When the caller supplies `eager_reference`, it is the entity the caller
  // already read (and verified) under read_opts.snapshot; reuse that snapshot
  // so the lazy read observes the same view and skip issuing a redundant eager
  // read. Otherwise pin our own snapshot and read the reference here.
  assert(eager_reference == nullptr || read_opts.snapshot != nullptr);
  ReadOptions ro = read_opts;
  std::unique_ptr<ManagedSnapshot> local_snapshot;
  if (ro.snapshot == nullptr && target == db_) {
    // Pin a snapshot so the lazy and eager reads observe the same view of the
    // concurrently-mutated primary. A read-only/secondary target has a stable
    // view for this call and (secondary) disallows snapshot reads, so it is
    // read at latest without a snapshot.
    local_snapshot = std::make_unique<ManagedSnapshot>(target);
    ro.snapshot = local_snapshot->snapshot();
  }
  ro.timestamp = nullptr;  // gated on FLAGS_user_timestamp_size == 0

  LazyWideColumns lazy;
  const Status lazy_s = target->GetEntityLazy(ro, cfh, key, &lazy);
  if (!lazy_s.ok() && !lazy_s.IsNotFound()) {
    if (verify && !IsErrorInjectedAndRetryable(lazy_s)) {
      thread->shared->SetVerificationFailure();
      fprintf(stderr, "GetEntityLazy error for key %s: %s\n",
              StringToHex(key.ToString()).c_str(), lazy_s.ToString().c_str());
    }
    return;
  }

  // Establish the eager reference (caller-supplied or read here) and its
  // presence, for comparison when verifying.
  PinnableWideColumns own_eager;
  const WideColumns* reference = nullptr;
  EagerPresence presence = EagerPresence::kUnknown;
  if (verify) {
    if (eager_reference != nullptr) {
      reference = eager_reference;
      presence = EagerPresence::kFound;
    } else {
      const Status eager_s = target->GetEntity(ro, cfh, key, &own_eager);
      if (eager_s.ok()) {
        reference = &own_eager.columns();
        presence = EagerPresence::kFound;
      } else if (eager_s.IsNotFound()) {
        presence = EagerPresence::kNotFound;
      }
      // else: eager errored (e.g. injected fault) -> presence stays kUnknown.
    }
  }

  // Presence cross-check (only when the eager presence is known).
  if (presence != EagerPresence::kUnknown) {
    const bool eager_found = (presence == EagerPresence::kFound);
    const bool lazy_found = lazy_s.ok();
    if (eager_found != lazy_found) {
      thread->shared->SetVerificationFailure();
      fprintf(stderr,
              "GetEntityLazy presence mismatch for key %s: eager %s, lazy %s\n",
              StringToHex(key.ToString()).c_str(),
              eager_found ? "found" : "NotFound",
              lazy_found ? "found" : "NotFound");
      return;
    }
  }

  if (lazy_s.IsNotFound()) {
    return;  // consistent NotFound (or exercise-only): nothing to resolve
  }

  // Lazy read found the entity: exercise the resolver, comparing against the
  // eager reference when we have one.
  ResolveLazyEntity(thread, key.ToString(), reference, lazy);
}

namespace {
// Chooses a byte range for a lazy column read in db_stress fuzzing. Half the
// time (or when the size is unknown or zero) reads the whole column; otherwise
// a random sub-range, occasionally with an offset past the end or a length
// beyond the remainder to exercise the API's clamp-to-empty /
// clamp-to-remainder behavior. `known_size` is the column's logical size when
// known without I/O.
void PickLazyReadRange(Random* rand, std::optional<uint64_t> known_size,
                       uint64_t* offset, size_t* length) {
  if (!known_size.has_value() || *known_size == 0 || rand->OneIn(2)) {
    *offset = 0;
    *length = kLazyWholeColumn;
    return;
  }
  // A small over-range so ~1/16 of picks land past the end or beyond the
  // remainder, exercising the API's clamp-to-empty / clamp-to-remainder
  // behavior. db_stress column values fit comfortably in int.
  const int size_plus_some = static_cast<int>(*known_size + (*known_size >> 4));
  // Offset in [0, size_plus_some); occasionally past the end to exercise
  // clamping.
  *offset = rand->Uniform(size_plus_some);
  // Length: usually a bounded sub-range (which may exceed the remainder),
  // sometimes to the end of the column.
  if (rand->OneIn(4)) {
    *length = kLazyWholeColumn;
  } else {
    *length = rand->Uniform(size_plus_some);
  }
}

// The sub-range of `value` that a partial read with (offset, length) should
// return, applying the API's documented clamping: an offset at/past the end
// yields empty, length is clamped to the remainder, and kLazyWholeColumn reads
// to the end.
Slice ExpectedLazySlice(const Slice& value, uint64_t offset, size_t length) {
  if (offset >= value.size()) {
    return Slice();
  }
  const size_t off = static_cast<size_t>(offset);
  const size_t avail = value.size() - off;
  const size_t len = length > avail ? avail : length;
  return Slice(value.data() + off, len);
}
}  // namespace

void StressTest::MaybeTestMultiGetEntityLazy(
    ThreadState* thread, const ReadOptions& read_opts, ColumnFamilyHandle* cfh,
    size_t num_keys, const Slice* keys,
    const std::vector<EagerEntityRef>* eager_references) {
  assert(thread);
  if (num_keys == 0 || !LazyEntityReadEnabled() ||
      !thread->rand.OneIn(FLAGS_lazy_entity_read_one_in)) {
    return;
  }
  // When the caller supplies per-key references, they were read (and verified)
  // under read_opts.snapshot; each carries the eager status (and columns when
  // found) so we can distinguish a clean NotFound from an injected/other error.
  assert(eager_references == nullptr || (read_opts.snapshot != nullptr &&
                                         eager_references->size() == num_keys));

  const bool verify = !FLAGS_skip_verifydb;

  ReadOptions ro = read_opts;
  std::unique_ptr<ManagedSnapshot> local_snapshot;
  if (ro.snapshot == nullptr) {
    local_snapshot = std::make_unique<ManagedSnapshot>(db_);
    ro.snapshot = local_snapshot->snapshot();
  }
  ro.timestamp = nullptr;

  LazyWideColumnsBatch batch;
  std::vector<Status> lazy_statuses(num_keys);
  db_->MultiGetEntityLazy(ro, cfh, num_keys, keys, &batch,
                          lazy_statuses.data());

  SharedState* const shared = thread->shared;

  if (batch.size() != num_keys) {
    if (verify) {
      shared->SetVerificationFailure();
      fprintf(stderr,
              "MultiGetEntityLazy returned %zu entities, expected %zu\n",
              batch.size(), num_keys);
    }
    return;
  }

  // Reference source: caller-supplied per-key columns (already read and
  // verified under the caller's snapshot) or, absent that, an eager
  // MultiGetEntity we issue here. Only needed when verifying.
  std::vector<PinnableWideColumns> own_eager;
  std::vector<Status> own_eager_statuses;
  if (verify && eager_references == nullptr) {
    own_eager.resize(num_keys);
    own_eager_statuses.resize(num_keys);
    db_->MultiGetEntity(ro, cfh, num_keys, keys, own_eager.data(),
                        own_eager_statuses.data());
  }

  // When eager_references == nullptr the fallbacks (own_eager_statuses /
  // own_eager) are populated only in the verify path; guard against a future
  // caller reaching these lambdas outside it.
  auto eager_status = [&](size_t i) -> const Status& {
    assert(verify || eager_references != nullptr);
    return eager_references != nullptr ? (*eager_references)[i].status
                                       : own_eager_statuses[i];
  };
  auto eager_presence = [&](size_t i) -> EagerPresence {
    if (!verify) {
      return EagerPresence::kUnknown;
    }
    const Status& es = eager_status(i);
    if (es.ok()) {
      return EagerPresence::kFound;
    }
    if (es.IsNotFound()) {
      return EagerPresence::kNotFound;
    }
    return EagerPresence::kUnknown;  // e.g. injected error
  };
  auto eager_columns = [&](size_t i) -> const WideColumns* {
    assert(verify || eager_references != nullptr);
    if (!eager_status(i).ok()) {
      return nullptr;
    }
    return eager_references != nullptr ? (*eager_references)[i].columns
                                       : &own_eager[i].columns();
  };

  // Per entity: reconcile presence vs the eager reference, verify enumeration,
  // then collect a random cross-entity subset of columns to resolve together.
  // entity_ref[i] is non-null only when entity i has a usable reference to
  // verify against.
  std::vector<const WideColumns*> entity_ref(num_keys, nullptr);
  std::vector<std::pair<size_t, size_t>>
      chosen;  // (entity_index, column_index)
  for (size_t i = 0; i < num_keys; ++i) {
    const Status& ls = lazy_statuses[i];
    if (!ls.ok() && !ls.IsNotFound()) {
      if (verify && !IsErrorInjectedAndRetryable(ls)) {
        shared->SetVerificationFailure();
        fprintf(stderr, "MultiGetEntityLazy error for key %s: %s\n",
                StringToHex(keys[i].ToString()).c_str(), ls.ToString().c_str());
      }
      continue;
    }

    const EagerPresence presence = eager_presence(i);
    if (presence != EagerPresence::kUnknown) {
      const bool eager_found = (presence == EagerPresence::kFound);
      const bool lazy_found = ls.ok();
      if (eager_found != lazy_found) {
        shared->SetVerificationFailure();
        fprintf(stderr,
                "MultiGetEntityLazy presence mismatch for key %s: eager %s, "
                "lazy %s\n",
                StringToHex(keys[i].ToString()).c_str(),
                eager_found ? "found" : "NotFound",
                lazy_found ? "found" : "NotFound");
        continue;
      }
    }

    if (ls.IsNotFound()) {
      continue;
    }

    // Lazy read found this entity.
    const LazyWideColumns& entity = batch[i];
    const WideColumns* reference =
        (presence == EagerPresence::kFound) ? eager_columns(i) : nullptr;
    if (!CheckLazyEntityEnumeration(thread, keys[i].ToString(), reference,
                                    entity)) {
      continue;
    }
    entity_ref[i] = reference;
    const size_t num_columns = entity.size();
    for (size_t c = 0; c < num_columns; ++c) {
      if (thread->rand.OneIn(2)) {
        chosen.emplace_back(i, c);
      }
    }
  }

  if (chosen.empty()) {
    return;  // resolved "none" this time; enumeration was still exercised above
  }

  // Reserve up front so the out-param pointers stored in `reads` stay valid.
  std::vector<PinnableSlice> results(chosen.size());
  std::vector<Status> statuses(chosen.size());
  std::vector<LazyColumnReadRequest> reads(chosen.size());
  for (size_t j = 0; j < chosen.size(); ++j) {
    const size_t i = chosen[j].first;
    const size_t c = chosen[j].second;
    // Randomly fuzz a partial (byte-range) read vs a whole-column read, sized
    // against the reference when available, else the known logical size; and
    // independently fuzz force_verify (which forces a whole, checksum-verified
    // read even for a sub-range) -- the resolved bytes are unchanged either
    // way.
    const WideColumns* reference = entity_ref[i];
    const std::optional<uint64_t> size_hint =
        reference ? std::optional<uint64_t>((*reference)[c].value().size())
                  : batch[i][c].logical_size();
    reads[j].column = &batch[i][c];
    PickLazyReadRange(&thread->rand, size_hint, &reads[j].offset,
                      &reads[j].length);
    reads[j].force_verify = thread->rand.OneIn(4);
    reads[j].result = &results[j];
    reads[j].status = &statuses[j];
  }

  const Status ms = batch.MultiResolve(reads);
  if (!ms.ok()) {
    if (verify && !IsErrorInjectedAndRetryable(ms)) {
      shared->SetVerificationFailure();
      fprintf(stderr, "LazyWideColumnsBatch::MultiResolve error: %s\n",
              ms.ToString().c_str());
    }
    return;
  }

  for (size_t j = 0; j < chosen.size(); ++j) {
    const WideColumns* reference = entity_ref[chosen[j].first];
    if (reference == nullptr) {
      continue;  // exercise-only entity: resolved for crash coverage, no
                 // compare
    }
    if (!statuses[j].ok()) {
      if (!IsErrorInjectedAndRetryable(statuses[j])) {
        shared->SetVerificationFailure();
        fprintf(stderr,
                "MultiGetEntityLazy column resolve error for key %s: %s\n",
                StringToHex(keys[chosen[j].first].ToString()).c_str(),
                statuses[j].ToString().c_str());
      }
      continue;
    }
    const Slice expected =
        ExpectedLazySlice((*reference)[chosen[j].second].value(),
                          reads[j].offset, reads[j].length);
    if (Slice(results[j]) != expected) {
      shared->SetVerificationFailure();
      fprintf(stderr,
              "MultiGetEntityLazy value mismatch for key %s column %zu "
              "[offset %" PRIu64 " len %zu]: lazy %s, reference %s\n",
              StringToHex(keys[chosen[j].first].ToString()).c_str(),
              chosen[j].second, reads[j].offset, reads[j].length,
              Slice(results[j]).ToString(true).c_str(),
              expected.ToString(true).c_str());
    }
  }
}

bool StressTest::CheckLazyEntityEnumeration(ThreadState* thread,
                                            const std::string& key,
                                            const WideColumns* reference,
                                            const LazyWideColumns& lazy) const {
  const size_t num_columns = lazy.size();
  if (reference && num_columns != reference->size()) {
    thread->shared->SetVerificationFailure();
    fprintf(stderr,
            "GetEntityLazy column count mismatch for key %s: lazy %zu, "
            "reference %zu\n",
            StringToHex(key).c_str(), num_columns, reference->size());
    return false;
  }
  for (size_t c = 0; c < num_columns; ++c) {
    if (reference == nullptr) {
      // Exercise the no-I/O enumeration accessors for crash coverage even when
      // there is nothing to compare against.
      const LazyWideColumn& col = lazy[c];
      col.name();
      col.logical_size();
      continue;
    }
    if (lazy[c].name() != (*reference)[c].name()) {
      thread->shared->SetVerificationFailure();
      fprintf(stderr, "GetEntityLazy column name mismatch for key %s at %zu\n",
              StringToHex(key).c_str(), c);
      return false;
    }
    // Logical size is known (and must be exact) for inline and uncompressed
    // blob columns; unknown for compressed blob columns (skip those).
    if (lazy[c].logical_size().has_value() &&
        *lazy[c].logical_size() != (*reference)[c].value().size()) {
      thread->shared->SetVerificationFailure();
      fprintf(stderr,
              "GetEntityLazy logical size mismatch for key %s column %zu: lazy "
              "%" PRIu64 ", reference %zu\n",
              StringToHex(key).c_str(), c, *lazy[c].logical_size(),
              (*reference)[c].value().size());
      return false;
    }
  }
  return true;
}

void StressTest::ResolveLazyEntity(ThreadState* thread, const std::string& key,
                                   const WideColumns* reference,
                                   LazyWideColumns& lazy) const {
  if (!CheckLazyEntityEnumeration(thread, key, reference, lazy)) {
    return;
  }

  // Resolve a random subset of columns (possibly none, possibly all) in one
  // MultiResolve call. This exercises the resolver for crash coverage; when a
  // reference is available it also verifies the resolved bytes.
  std::vector<size_t> chosen;
  const size_t num_columns = lazy.size();
  for (size_t c = 0; c < num_columns; ++c) {
    if (thread->rand.OneIn(2)) {
      chosen.push_back(c);
    }
  }
  if (chosen.empty()) {
    return;
  }

  std::vector<PinnableSlice> results(chosen.size());
  std::vector<Status> statuses(chosen.size());
  std::vector<LazyColumnReadRequest> reads(chosen.size());
  for (size_t j = 0; j < chosen.size(); ++j) {
    const size_t c = chosen[j];
    // Randomly fuzz a partial (byte-range) read vs a whole-column read, sized
    // against the reference when available, else the known logical size; and
    // independently fuzz force_verify (which forces a whole, checksum-verified
    // read even for a sub-range) -- the resolved bytes are unchanged either
    // way.
    const std::optional<uint64_t> size_hint =
        reference ? std::optional<uint64_t>((*reference)[c].value().size())
                  : lazy[c].logical_size();
    reads[j].column = &lazy[c];
    PickLazyReadRange(&thread->rand, size_hint, &reads[j].offset,
                      &reads[j].length);
    reads[j].force_verify = thread->rand.OneIn(4);
    reads[j].result = &results[j];
    reads[j].status = &statuses[j];
  }

  SharedState* const shared = thread->shared;
  const Status ms = lazy.MultiResolve(reads);
  if (!ms.ok()) {
    if (reference && !IsErrorInjectedAndRetryable(ms)) {
      shared->SetVerificationFailure();
      fprintf(stderr, "LazyWideColumns::MultiResolve error for key %s: %s\n",
              StringToHex(key).c_str(), ms.ToString().c_str());
    }
    return;
  }
  if (reference == nullptr) {
    return;  // exercise-only (e.g. skip_verifydb): nothing to compare
  }
  for (size_t j = 0; j < chosen.size(); ++j) {
    if (!statuses[j].ok()) {
      if (!IsErrorInjectedAndRetryable(statuses[j])) {
        shared->SetVerificationFailure();
        fprintf(
            stderr,
            "GetEntityLazy column resolve error for key %s column %zu: %s\n",
            StringToHex(key).c_str(), chosen[j],
            statuses[j].ToString().c_str());
      }
      continue;
    }
    const Slice expected = ExpectedLazySlice((*reference)[chosen[j]].value(),
                                             reads[j].offset, reads[j].length);
    if (Slice(results[j]) != expected) {
      shared->SetVerificationFailure();
      fprintf(stderr,
              "GetEntityLazy value mismatch for key %s column %zu "
              "[offset %" PRIu64 " len %zu]: lazy %s, reference %s\n",
              StringToHex(key).c_str(), chosen[j], reads[j].offset,
              reads[j].length, Slice(results[j]).ToString(true).c_str(),
              expected.ToString(true).c_str());
    }
  }
}

const std::string& StressTest::GetDbLabel() const { return db_label_; }

const std::string& StressTest::GetDbPath() const { return db_path_; }

const std::string& StressTest::GetExpectedValuesDir() const {
  return expected_values_path_;
}

const std::string& StressTest::GetSecondariesBase() const {
  return secondaries_path_;
}

Env* StressTest::GetDbEnv() const { return db_env_.get(); }

StressTest::StressTest(int db_index, const std::string& db_path,
                       const std::string& ev_path, const std::string& sec_path)
    : db_index_(db_index),
      db_label_("db_" + std::to_string(db_index)),
      db_path_(db_path),
      expected_values_path_(ev_path),
      secondaries_path_(sec_path),
      db_stress_fs_(
          std::make_shared<DbStressFSWrapper>(raw_env->GetFileSystem())),
      db_fault_injection_fs_(
          NeedsFaultInjection()
              ? std::make_shared<FaultInjectionTestFS>(
                    db_stress_fs_, GetFaultInjectionLogPath(db_label_))
              : nullptr),
      db_env_(std::make_unique<CompositeEnvWrapper>(
          raw_env, db_fault_injection_fs_
                       ? std::shared_ptr<FileSystem>(db_fault_injection_fs_)
                       : std::shared_ptr<FileSystem>(db_stress_fs_))),
      filter_policy_(CreateFilterPolicy()),
      db_(nullptr),
      txn_db_(nullptr),
      optimistic_txn_db_(nullptr),
      db_aptr_(nullptr),
      clock_(raw_env->GetSystemClock().get()),
      new_column_family_name_(1),
      num_times_reopened_(0),
      db_preload_finished_(false),
      is_db_stopped_(false),
      manifest_verify_mode_(MANIFEST_VERIFY_NONE),
      manifest_file_number_before_reopen_(0),
      manifest_file_size_before_reopen_(0) {
  if (db_fault_injection_fs_) {
    // Info logs are debugging artifacts, so exclude them from fault injection
    // and keep error accounting focused on DB data and metadata.
    db_fault_injection_fs_->SetFileTypesExcludedFromFaultInjection(
        {FileType::kInfoLogFile});
    db_fault_injection_fs_->SetInjectFileScopeOnWALWriteError(
        FLAGS_file_scope_wal_write_faults);
    db_fault_injection_fs_->SetInjectWriteFaultsOnlyOnWAL(
        FLAGS_file_scope_wal_write_faults);
    // Normally bypass filesystem fault injection during DB open. Lifecycle
    // testing keeps the fresh MANIFEST behind the wrapper while injection
    // remains disabled until the target setup after open. Other modes
    // overwrite this in StressTest::Open() for open fault injection and in
    // RunStressTestImpl() for normal fault injection setup.
    db_fault_injection_fs_->SetFilesystemDirectWritable(
        !FLAGS_test_cf_lifecycle);
  }

  if (FLAGS_destroy_db_initially) {
    const Status s = DbStressDestroyDb(GetDbPath());
    DB_STRESS_ASSERT_OK_MSG(s, "Cannot destroy original db");
  }

  Status s = DbStressSqfcManager().MakeSharedFactory(
      FLAGS_sqfc_name, FLAGS_sqfc_version, &sqfc_factory_);
  DB_STRESS_ASSERT_OK_MSG(s, "Error initializing SstQueryFilterConfig");

  CheckpointEngineOptions engine_options;
  engine_options.max_background_operations =
      FLAGS_checkpoint_engine_max_background_operations;
  engine_options.use_link_file_when_available =
      FLAGS_checkpoint_engine_use_link_file_when_available;
  s = CheckpointEngine::Open(engine_options, &checkpoint_engine_);
  DB_STRESS_ASSERT_OK_MSG(s, "Error opening CheckpointEngine");
}

void StressTest::CleanUp() {
  // Prevent any new VerifyPkSkFast entries from background threads before
  // we close and destroy the DB.
  db_aptr_.store(nullptr, std::memory_order_release);
  CleanUpColumnFamilies();
  if (db_) {
    db_->Close();
  }
  db_owner_.reset();
  db_ = nullptr;

  secondary_db_.reset();
}

void StressTest::InitializeListenersForOpen(
    SharedState* shared,
    const std::vector<ColumnFamilyDescriptor>& cf_descriptors) {
  options_.listeners.clear();
  options_.listeners.emplace_back(new DbStressListener(
      GetDbPath(), options_.db_paths, cf_descriptors, shared));
  RegisterAdditionalListeners();

  if (!FLAGS_listener_uri.empty()) {
    std::shared_ptr<EventListener> listener;
    Status listener_status =
        ObjectRegistry::Default()->NewSharedObject<EventListener>(
            FLAGS_listener_uri, &listener);
    DB_STRESS_ASSERT_OK_MSG(listener_status,
                            "Failed to create listener from URI '%s'",
                            FLAGS_listener_uri.c_str());
    options_.listeners.emplace_back(std::move(listener));
  }
}

void StressTest::CleanUpColumnFamilies() {
  for (auto cf : column_families_) {
    delete cf;
  }
  column_families_.clear();
  for (auto* cf : secondary_cfhs_) {
    delete cf;
  }
  secondary_cfhs_.clear();
}

std::shared_ptr<Cache> StressTest::NewCache(size_t capacity,
                                            int32_t num_shard_bits) {
  ConfigOptions config_options;
  if (capacity <= 0) {
    return nullptr;
  }

  std::shared_ptr<SecondaryCache> secondary_cache;
  if (!FLAGS_secondary_cache_uri.empty()) {
    assert(!strstr(FLAGS_secondary_cache_uri.c_str(),
                   "compressed_secondary_cache") ||
           (FLAGS_compressed_secondary_cache_size == 0 &&
            FLAGS_compressed_secondary_cache_ratio == 0.0 &&
            !StartsWith(FLAGS_cache_type, "tiered_")));
    Status s = SecondaryCache::CreateFromString(
        config_options, FLAGS_secondary_cache_uri, &secondary_cache);
    if (secondary_cache == nullptr) {
      fprintf(stderr,
              "No secondary cache registered matching string: %s status=%s\n",
              FLAGS_secondary_cache_uri.c_str(), s.ToString().c_str());
      exit(1);
    }
    if (FLAGS_secondary_cache_fault_one_in > 0) {
      secondary_cache = std::make_shared<FaultInjectionSecondaryCache>(
          secondary_cache, static_cast<uint32_t>(FLAGS_seed),
          FLAGS_secondary_cache_fault_one_in);
    }
  } else if (FLAGS_compressed_secondary_cache_size > 0) {
    if (StartsWith(FLAGS_cache_type, "tiered_")) {
      fprintf(stderr,
              "Cannot specify both compressed_secondary_cache_size and %s\n",
              FLAGS_cache_type.c_str());
      exit(1);
    }
    CompressedSecondaryCacheOptions opts;
    opts.capacity = FLAGS_compressed_secondary_cache_size;
    if (FLAGS_enable_do_not_compress_roles) {
      opts.do_not_compress_roles = {CacheEntryRoleSet::All()};
    }
    opts.enable_custom_split_merge = FLAGS_enable_custom_split_merge;
    secondary_cache = NewCompressedSecondaryCache(opts);
    if (secondary_cache == nullptr) {
      fprintf(stderr, "Failed to allocate compressed secondary cache\n");
      exit(1);
    }
    compressed_secondary_cache = secondary_cache;
  }

  std::string cache_type = FLAGS_cache_type;
  size_t cache_size = FLAGS_cache_size;
  bool tiered = false;
  if (StartsWith(cache_type, "tiered_")) {
    tiered = true;
    cache_type.erase(0, strlen("tiered_"));
  }
  if (FLAGS_use_write_buffer_manager) {
    cache_size += FLAGS_db_write_buffer_size;
  }
  if (cache_type == "clock_cache") {
    fprintf(stderr, "Old clock cache implementation has been removed.\n");
    exit(1);
  } else if (EndsWith(cache_type, "hyper_clock_cache")) {
    size_t estimated_entry_charge;
    if (cache_type == "fixed_hyper_clock_cache") {
      estimated_entry_charge = FLAGS_block_size;
    } else if (cache_type == "auto_hyper_clock_cache" ||
               cache_type == "hyper_clock_cache") {
      estimated_entry_charge = 0;
    } else {
      fprintf(stderr, "Cache type not supported.");
      exit(1);
    }
    HyperClockCacheOptions opts(cache_size, estimated_entry_charge,
                                num_shard_bits);
    opts.hash_seed = BitwiseAnd(FLAGS_seed, INT32_MAX);
    if (tiered) {
      TieredCacheOptions tiered_opts;
      tiered_opts.cache_opts = &opts;
      tiered_opts.cache_type = PrimaryCacheType::kCacheTypeHCC;
      tiered_opts.total_capacity = cache_size;
      tiered_opts.compressed_secondary_ratio = 0.5;
      tiered_opts.adm_policy =
          static_cast<TieredAdmissionPolicy>(FLAGS_adm_policy);
      if (tiered_opts.adm_policy ==
          TieredAdmissionPolicy::kAdmPolicyThreeQueue) {
        CompressedSecondaryCacheOptions nvm_sec_cache_opts;
        nvm_sec_cache_opts.capacity = cache_size;
        tiered_opts.nvm_sec_cache =
            NewCompressedSecondaryCache(nvm_sec_cache_opts);
      }
      block_cache = NewTieredCache(tiered_opts);
    } else {
      opts.secondary_cache = std::move(secondary_cache);
      block_cache = opts.MakeSharedCache();
    }
  } else if (EndsWith(cache_type, "lru_cache")) {
    LRUCacheOptions opts;
    opts.capacity = capacity;
    opts.num_shard_bits = num_shard_bits;
    opts.metadata_charge_policy =
        static_cast<CacheMetadataChargePolicy>(FLAGS_metadata_charge_policy);
    opts.use_adaptive_mutex = FLAGS_use_adaptive_mutex_lru;
    opts.high_pri_pool_ratio = FLAGS_high_pri_pool_ratio;
    opts.low_pri_pool_ratio = FLAGS_low_pri_pool_ratio;
    if (tiered) {
      TieredCacheOptions tiered_opts;
      tiered_opts.cache_opts = &opts;
      tiered_opts.cache_type = PrimaryCacheType::kCacheTypeLRU;
      tiered_opts.total_capacity = cache_size;
      tiered_opts.compressed_secondary_ratio = 0.5;
      tiered_opts.adm_policy =
          static_cast<TieredAdmissionPolicy>(FLAGS_adm_policy);
      if (tiered_opts.adm_policy ==
          TieredAdmissionPolicy::kAdmPolicyThreeQueue) {
        CompressedSecondaryCacheOptions nvm_sec_cache_opts;
        nvm_sec_cache_opts.capacity = cache_size;
        tiered_opts.nvm_sec_cache =
            NewCompressedSecondaryCache(nvm_sec_cache_opts);
      }
      block_cache = NewTieredCache(tiered_opts);
    } else {
      opts.secondary_cache = std::move(secondary_cache);
      block_cache = NewLRUCache(opts);
    }
  } else {
    fprintf(stderr, "Cache type not supported.");
    exit(1);
  }
  return block_cache;
}

std::vector<std::string> StressTest::GetBlobCompressionTags() {
  std::vector<std::string> compression_tags{"kNoCompression"};

  if (Snappy_Supported()) {
    compression_tags.emplace_back("kSnappyCompression");
  }
  if (LZ4_Supported()) {
    compression_tags.emplace_back("kLZ4Compression");
  }
  if (ZSTD_Supported()) {
    compression_tags.emplace_back("kZSTD");
  }

  return compression_tags;
}

bool StressTest::BuildOptionsTable() {
  if (FLAGS_set_options_one_in <= 0) {
    return true;
  }

  bool keepRibbonFilterPolicyOnly = FLAGS_bloom_before_level != INT_MAX;

  std::unordered_map<std::string, std::vector<std::string>> options_tbl = {
      {"write_buffer_size",
       {std::to_string(options_.write_buffer_size),
        std::to_string(options_.write_buffer_size * 2),
        std::to_string(options_.write_buffer_size * 4)}},
      {"max_write_buffer_number",
       {std::to_string(options_.max_write_buffer_number),
        std::to_string(options_.max_write_buffer_number * 2),
        std::to_string(options_.max_write_buffer_number * 4)}},
      {"arena_block_size",
       {
           std::to_string(options_.arena_block_size),
           std::to_string(options_.write_buffer_size / 4),
           std::to_string(options_.write_buffer_size / 8),
       }},
      {"memtable_huge_page_size", {"0", std::to_string(2 * 1024 * 1024)}},
      {"strict_max_successive_merges", {"false", "true"}},
      {"inplace_update_num_locks", {"100", "200", "300"}},
      // TODO: re-enable once internal task T124324915 is fixed.
      // {"experimental_mempurge_threshold", {"0.0", "1.0"}},
      // TODO(ljin): enable test for this option
      // {"disable_auto_compactions", {"100", "200", "300"}},
      {"level0_slowdown_writes_trigger",
       {
           std::to_string(options_.level0_slowdown_writes_trigger),
           std::to_string(options_.level0_slowdown_writes_trigger + 2),
           std::to_string(options_.level0_slowdown_writes_trigger + 4),
       }},
      {"level0_stop_writes_trigger",
       {
           std::to_string(options_.level0_stop_writes_trigger),
           std::to_string(options_.level0_stop_writes_trigger + 2),
           std::to_string(options_.level0_stop_writes_trigger + 4),
       }},
      {"max_compaction_bytes",
       {
           std::to_string(options_.target_file_size_base * 5),
           std::to_string(options_.target_file_size_base * 15),
           std::to_string(options_.target_file_size_base * 100),
       }},
      {"target_file_size_base",
       {
           std::to_string(options_.target_file_size_base),
           std::to_string(options_.target_file_size_base * 2),
           std::to_string(options_.target_file_size_base * 4),
       }},
      {"target_file_size_multiplier",
       {
           std::to_string(options_.target_file_size_multiplier),
           "1",
           "2",
       }},
      {"max_bytes_for_level_base",
       {
           std::to_string(options_.max_bytes_for_level_base / 2),
           std::to_string(options_.max_bytes_for_level_base),
           std::to_string(options_.max_bytes_for_level_base * 2),
       }},
      {"max_bytes_for_level_multiplier",
       {
           std::to_string(options_.max_bytes_for_level_multiplier),
           "1",
           "2",
       }},
      {"block_based_table_factory",
       {
           keepRibbonFilterPolicyOnly ? "{filter_policy=ribbonfilter:2.35}"
                                      : "{filter_policy=bloomfilter:2.34}",
           "{filter_policy=ribbonfilter:5.67:-1}",
           keepRibbonFilterPolicyOnly ? "{filter_policy=ribbonfilter:8.9:3}"
                                      : "{filter_policy=nullptr}",
           "{block_size=" + std::to_string(FLAGS_block_size) + "}",
           "{block_size=" +
               std::to_string(FLAGS_block_size + (FLAGS_seed & 0xFFFU)) + "}",
       }},
  };
  if (FLAGS_use_multiscan == 0) {
    // TODO: this can fail MultiScan when consecutive data blocks share the
    // same user at boundary. MultiScan uses user key to locate the block to
    // reach which can move the scan earlier than its current block.
    options_tbl.emplace("max_sequential_skip_in_iterations",
                        std::vector<std::string>{"4", "8", "12"});
  }
  if (FLAGS_compaction_style == kCompactionStyleUniversal &&
      FLAGS_universal_max_read_amp > 0) {
    // level0_file_num_compaction_trigger needs to be at most max_read_amp
    options_tbl.emplace(
        "level0_file_num_compaction_trigger",
        std::vector<std::string>{
            std::to_string(options_.level0_file_num_compaction_trigger),
            std::to_string(
                std::min(options_.level0_file_num_compaction_trigger + 2,
                         FLAGS_universal_max_read_amp)),
            std::to_string(
                std::min(options_.level0_file_num_compaction_trigger + 4,
                         FLAGS_universal_max_read_amp)),
        });
  } else {
    options_tbl.emplace(
        "level0_file_num_compaction_trigger",
        std::vector<std::string>{
            std::to_string(options_.level0_file_num_compaction_trigger),
            std::to_string(options_.level0_file_num_compaction_trigger + 2),
            std::to_string(options_.level0_file_num_compaction_trigger + 4),
        });
  }
  if (FLAGS_unordered_write) {
    options_tbl.emplace("max_successive_merges", std::vector<std::string>{"0"});
  } else {
    options_tbl.emplace("max_successive_merges",
                        std::vector<std::string>{"0", "2", "4"});
  }

  if (FLAGS_allow_setting_blob_options_dynamically) {
    options_tbl.emplace("enable_blob_files",
                        std::vector<std::string>{"false", "true"});
    options_tbl.emplace("min_blob_size",
                        std::vector<std::string>{"0", "8", "16"});
    options_tbl.emplace("blob_file_size",
                        std::vector<std::string>{"1M", "16M", "256M", "1G"});
    options_tbl.emplace("blob_compression_type", GetBlobCompressionTags());
    options_tbl.emplace("enable_blob_garbage_collection",
                        std::vector<std::string>{"false", "true"});
    options_tbl.emplace(
        "blob_garbage_collection_age_cutoff",
        std::vector<std::string>{"0.0", "0.25", "0.5", "0.75", "1.0"});
    options_tbl.emplace("blob_garbage_collection_force_threshold",
                        std::vector<std::string>{"0.5", "0.75", "1.0"});
    options_tbl.emplace("blob_compaction_readahead_size",
                        std::vector<std::string>{"0", "1M", "4M"});
    options_tbl.emplace("blob_file_starting_level",
                        std::vector<std::string>{"0", "1", "2"});
    options_tbl.emplace("prepopulate_blob_cache",
                        std::vector<std::string>{"kDisable", "kFlushOnly"});
  }

  if (keepRibbonFilterPolicyOnly) {
    // Can modify RibbonFilterPolicy field
    options_tbl.emplace("table_factory.filter_policy.bloom_before_level",
                        std::vector<std::string>{"-1", "0", "1", "2",
                                                 "2147483646", "2147483647"});
  }

  if (!FLAGS_file_temperature_age_thresholds.empty()) {
    // Modify file_temperature_age_thresholds only if it is set initially
    // (FIFO tiered storage setup)
    options_tbl.emplace(
        "file_temperature_age_thresholds",
        std::vector<std::string>{
            "{{temperature=kWarm;age=10}:{temperature=kCool;age=30}:{"
            "temperature=kCold;age=100}:{"
            "temperature=kIce;age=300}}",
            "{{temperature=kWarm;age=30}:{temperature=kCold;age=300}}",
            "{{temperature=kCold;age=100}}", "{}"});
    options_tbl.emplace(
        "allow_trivial_copy_when_change_temperature",
        std::vector<std::string>{
            FLAGS_allow_trivial_copy_when_change_temperature ? "true"
                                                             : "false"});
  }

  // NOTE: allow -1 to mean starting disabled but dynamically changing
  // But 0 means tiering is disabled for the entire run.
  if (FLAGS_preclude_last_level_data_seconds != 0) {
    options_tbl.emplace("preclude_last_level_data_seconds",
                        std::vector<std::string>{"0", "5", "30", "5000"});
  }
  options_tbl.emplace("preserve_internal_time_seconds",
                      std::vector<std::string>{"0", "5", "30", "5000"});

  options_table_ = std::move(options_tbl);

  for (const auto& iter : options_table_) {
    options_index_.push_back(iter.first);
  }
  return true;
}

void StressTest::InitDb(SharedState* shared) {
  uint64_t now = clock_->NowMicros();
  fprintf(stdout, "%s Initializing db_stress\n",
          clock_->TimeToString(now / 1000000).c_str());
  PrintEnv();
  Open(shared);
  BuildOptionsTable();
}

void StressTest::FinishInitDb(SharedState* shared) {
  if (FLAGS_read_only) {
    uint64_t now = clock_->NowMicros();
    fprintf(stdout, "%s Preloading db with %" PRIu64 " KVs\n",
            clock_->TimeToString(now / 1000000).c_str(), FLAGS_max_key);
    PreloadDbAndReopenAsReadOnly(FLAGS_max_key, shared);
  }

  if (shared->HasHistory()) {
    // The way it works right now is, if there's any history, that means the
    // previous run mutating the DB had all its operations traced, in which case
    // we should always be able to `Restore()` the expected values to match the
    // `db_`'s current seqno.
    Status s = shared->Restore(db_);
    if (!s.ok()) {
      fprintf(stderr, "Error restoring historical expected values: %s\n",
              s.ToString().c_str());
      port::ImmediateExit(1);
    }
  }
  if (FLAGS_use_txn && !FLAGS_use_optimistic_txn) {
    // It's OK here without sync because unsynced data cannot be lost at this
    // point
    // - even with sync_fault_injection=1 as the
    // file is still directly writable until after FinishInitDb()
    ProcessRecoveredPreparedTxns(shared);
  }

  if (FLAGS_enable_compaction_filter) {
    auto* compaction_filter_factory =
        static_cast<DbStressCompactionFilterFactory*>(
            options_.compaction_filter_factory.get());
    assert(compaction_filter_factory);
    // This must be called only after any potential `SharedState::Restore()` has
    // completed in order for the `compaction_filter_factory` to operate on the
    // correct latest values file.
    compaction_filter_factory->SetSharedState(shared);
    fprintf(stdout, "Compaction filter factory: %s\n",
            compaction_filter_factory->Name());
  }
}

void StressTest::TrackExpectedState(SharedState* shared) {
  // When data loss is simulated, recovery from potential data loss is a prefix
  // recovery that requires tracing
  if (MightHaveUnsyncedDataLoss() && IsStateTracked()) {
    Status s = shared->SaveAtAndAfter(db_);
    if (!s.ok()) {
      fprintf(stderr, "Error enabling history tracing: %s\n",
              s.ToString().c_str());
      port::ImmediateExit(1);
    }
  }
}

Status StressTest::AssertSame(DB* db, ColumnFamilyHandle* cf,
                              ThreadState::SnapshotState& snap_state) {
  Status s;
  if (cf->GetName() != snap_state.cf_at_name) {
    return s;
  }
  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ropt;
  ropt.snapshot = snap_state.snapshot;
  ropt.auto_refresh_iterator_with_snapshot =
      FLAGS_auto_refresh_iterator_with_snapshot;
  Slice ts;
  std::string validation_ts_str;
  if (!snap_state.timestamp.empty()) {
    if (!FLAGS_persist_user_defined_timestamps) {
      // Timestamp-enabled reads require a read timestamp. In memtable-only UDT
      // mode, the saved timestamp is not part of the long-running snapshot
      // contract because full_history_ts_low can advance while the snapshot is
      // held. Use a fresh timestamp to validate the pinned sequence-number
      // view.
      validation_ts_str = GetReadTimestamp();
      ts = validation_ts_str;
    } else {
      ts = snap_state.timestamp;
    }
    ropt.timestamp = &ts;
  }
  PinnableSlice exp_v(&snap_state.value);
  exp_v.PinSelf();
  PinnableSlice v;
  s = DbStressGet(db, ropt, cf, snap_state.key, &v);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (snap_state.status != s) {
    return Status::Corruption(
        "The snapshot gave inconsistent results for key " +
        std::to_string(Hash(snap_state.key.c_str(), snap_state.key.size(), 0)) +
        " in cf " + cf->GetName() + ": (" + snap_state.status.ToString() +
        ") vs. (" + s.ToString() + ")");
  }
  if (s.ok()) {
    if (exp_v != v) {
      return Status::Corruption("The snapshot gave inconsistent values: (" +
                                exp_v.ToString() + ") vs. (" + v.ToString() +
                                ")");
    }
  }
  if (snap_state.key_vec != nullptr) {
    // When `prefix_extractor` is set, seeking to beginning and scanning
    // across prefixes are only supported with `total_order_seek` set.
    ropt.total_order_seek = true;
    std::unique_ptr<Iterator> iterator(db->NewIterator(ropt));
    std::unique_ptr<std::vector<bool>> tmp_bitvec(
        new std::vector<bool>(FLAGS_max_key));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      uint64_t key_val;
      if (GetIntVal(iterator->key().ToString(), &key_val)) {
        (*tmp_bitvec.get())[key_val] = true;
      }
    }
    if (!std::equal(snap_state.key_vec->begin(), snap_state.key_vec->end(),
                    tmp_bitvec.get()->begin())) {
      return Status::Corruption("Found inconsistent keys at this snapshot");
    }
  }
  return Status::OK();
}

void StressTest::ProcessStatus(SharedState* shared, std::string opname,
                               const Status& s,
                               bool ignore_injected_error) const {
  if (s.ok()) {
    return;
  }
  if (!ignore_injected_error || !IsErrorInjectedAndRetryable(s)) {
    std::ostringstream oss;
    oss << opname << " failed: " << s.ToString();
    VerificationAbort(shared, oss.str());
    assert(false);
  }
}

void StressTest::VerificationAbort(SharedState* shared, std::string msg) const {
  fprintf(stderr, "Verification failed: %s\n", msg.c_str());
  shared->SetVerificationFailure();
}

void StressTest::VerificationAbort(SharedState* shared, std::string msg, int cf,
                                   int64_t key) const {
  auto key_str = Key(key);
  Slice key_slice = key_str;
  fprintf(stderr,
          "Verification failed for column family %d key %s (%" PRIi64 "): %s\n",
          cf, key_slice.ToString(true).c_str(), key, msg.c_str());
  shared->SetVerificationFailure();
}

void StressTest::VerificationAbort(SharedState* shared, std::string msg, int cf,
                                   int64_t key, Slice value_from_db,
                                   Slice value_from_expected) const {
  auto key_str = Key(key);
  fprintf(stderr,
          "Verification failed for column family %d key %s (%" PRIi64
          "): value_from_db: %s, value_from_expected: %s, msg: %s\n",
          cf, Slice(key_str).ToString(true).c_str(), key,
          value_from_db.ToString(true).c_str(),
          value_from_expected.ToString(true).c_str(), msg.c_str());
  shared->SetVerificationFailure();
}

void StressTest::VerificationAbort(SharedState* shared, int cf, int64_t key,
                                   const Slice& value,
                                   const WideColumns& columns) const {
  assert(shared);

  auto key_str = Key(key);

  fprintf(stderr,
          "Verification failed for column family %d key %s (%" PRIi64
          "): Value and columns inconsistent: value: %s, columns: %s\n",
          cf, Slice(key_str).ToString(/* hex */ true).c_str(), key,
          value.ToString(/* hex */ true).c_str(),
          WideColumnsToHex(columns).c_str());

  shared->SetVerificationFailure();
}

// Under --verify_cpu_corruption_dir, right after each write op
// (put/delete/deleterange), flush, and compaction (compactrange/compactfiles),
// verifies that op for corruption and, on the first one found, writes a result
// file into the directory and marks the run as a verification failure. It
// catches: (a) a corruption returned by the op itself or by the read-back
// (Status::Corruption); or (b) a silent data corruption (SDC) -- a read-back
// that SUCCEEDS but returns the wrong result vs the committed expected state: a
// lost key (NotFound), a resurrected key (deleted but present), or a wrong
// value (bytes differ). Surfacing these with an immediate read right after the
// op (instead of waiting for a later read or the end-of-run db verification)
// gives the most immediate and accurate verification when an external CPU-fault
// injector (e.g. gdb) flips a register inside that op.
//
// REQUIRES (enforced at startup) --threads=1 and all fault injection off (every
// *_fault_one_in = 0 and sync_fault_injection = false): the full-keyspace
// read-back is only well-defined with a single writer, and injected I/O faults
// would otherwise taint it.
//
// OUTPUT CONTRACT (one file per worker thread):
// <dir>/data_corruption.<thread_id>.json -- a JSON object with fields: kind
// (one of lost|resurrected|wrong-value|detected-corruption), cf (int), key
// (int), value_from_db (hex), value_from_expected (hex), op_status (string).
//
// PERFORMANCE: the read-back scans the entire keyspace after every op
// (O(max_key) Get calls per op), so this is meant for single-op
// CPU-fault-injection debugging, not general stress runs.
void StressTest::MaybeVerifyCpuCorruption(ThreadState* thread,
                                          const char* op_label,
                                          const Status& op_status) {
  if (FLAGS_verify_cpu_corruption_dir.empty()) {
    return;
  }
  auto shared = thread->shared;

  if (shared->HasVerificationFailedYet()) {
    return;
  }

  // The op itself returned Corruption: an integrity check caught the
  // corruption.
  if (op_status.IsCorruption()) {
    WriteDataCorruption(thread->tid, /* cf */ -1, /* key */ -1,
                        {"detected-corruption", Slice(), Slice(),
                         std::string(op_label) + ": " + op_status.ToString()});
    VerificationAbort(shared,
                      std::string(op_label) +
                          " detected data corruption: " + op_status.ToString());
    return;
  }

  // Single writer (the run is pinned to --threads=1): read every key back and
  // compare it against the committed expected state, ending the run on the
  // first data corruption found.
  // TODO: this full-keyspace read-back duplicates the canonical verifier
  // VerifyDb (no_batched_ops_stress.cc). Refactor that key/value compare into a
  // shared per-key helper both can call, so this stays in sync with the model.
  ReadOptions read_opts;
  read_opts.verify_checksums = true;
  // A user-timestamp DB needs a read timestamp; supply one like the canonical
  // verifier (no_batched_ops_stress.cc).
  std::string read_ts_str;
  Slice read_ts;
  if (FLAGS_user_timestamp_size > 0) {
    read_ts_str = GetReadTimestamp();
    read_ts = read_ts_str;
    read_opts.timestamp = &read_ts;
  }
  const int64_t max_key = shared->GetMaxKey();
  std::string db_value;
  char expected_scratch[kValueMaxLen];
  for (int cf = 0; cf < static_cast<int>(column_families_.size()); ++cf) {
    for (int64_t key = 0; key < max_key; ++key) {
      const ExpectedValue expected = shared->Get(cf, key);
      db_value.clear();
      const Status s = DbStressGet(db_, read_opts, column_families_[cf],
                                   Key(key), &db_value);
      const std::optional<DataCorruption> corruption =
          ClassifyReadBack(s, db_value, expected, expected_scratch);
      if (corruption.has_value()) {
        WriteDataCorruption(thread->tid, cf, key, *corruption);
        VerificationAbort(
            shared,
            std::string(corruption->kind) + " (" + corruption->op_status + ")",
            cf, key, corruption->value_from_db,
            corruption->value_from_expected);
        return;
      }
    }
  }
}

std::string StressTest::DebugString(const Slice& value,
                                    const WideColumns& columns) {
  std::ostringstream oss;

  oss << "value: " << value.ToString(/* hex */ true)
      << ", columns: " << WideColumnsToHex(columns);

  return oss.str();
}

void StressTest::PrintStatistics() {
  // Print statistics from the DB instance instead of global dbstats
  if (db_) {
    auto stats = db_->GetOptions().statistics;
    if (stats) {
      fprintf(stdout, "STATISTICS:\n%s\n", stats->ToString().c_str());
    }
  }
  // Print statistics from secondary DB instance if it exists
  if (secondary_db_) {
    auto stats = secondary_db_->GetOptions().statistics;
    if (stats) {
      fprintf(stdout, "Secondary instance STATISTICS:\n%s\n",
              stats->ToString().c_str());
    }
  }
}

// Currently PreloadDb has to be single-threaded.
void StressTest::PreloadDbAndReopenAsReadOnly(int64_t number_of_keys,
                                              SharedState* shared) {
  WriteOptions write_opts;
  write_opts.disableWAL = FLAGS_disable_wal;
  if (FLAGS_sync) {
    write_opts.sync = true;
  }
  if (FLAGS_rate_limit_auto_wal_flush) {
    write_opts.rate_limiter_priority = Env::IO_USER;
  }
  char value[100];
  int cf_idx = 0;
  Status s;
  for (auto cfh : column_families_) {
    for (int64_t k = 0; k != number_of_keys; ++k) {
      const std::string key = Key(k);
      PendingExpectedValue pending_expected_value =
          shared->PreparePut(cf_idx, k);
      const uint32_t value_base = pending_expected_value.GetFinalValueBase();
      const size_t sz = GenerateValue(value_base, value, sizeof(value));

      const Slice v(value, sz);

      std::string ts;
      if (FLAGS_user_timestamp_size > 0) {
        ts = GetNowNanos();
      }

      if (FLAGS_use_put_entity_one_in > 0 &&
          (value_base % FLAGS_use_put_entity_one_in) == 0) {
        if (!FLAGS_use_txn) {
          if (FLAGS_use_attribute_group) {
            s = db_->PutEntity(write_opts, key,
                               GenerateAttributeGroups({cfh}, value_base, v));
          } else {
            s = db_->PutEntity(write_opts, cfh, key,
                               GenerateWideColumns(value_base, v));
          }
        } else {
          s = ExecuteTransaction(
              write_opts, /*thread=*/nullptr, [&](Transaction& txn) {
                return txn.PutEntity(cfh, key,
                                     GenerateWideColumns(value_base, v));
              });
        }
      } else if (FLAGS_use_merge) {
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size > 0) {
            s = db_->Merge(write_opts, cfh, key, ts, v);
          } else {
            s = db_->Merge(write_opts, cfh, key, v);
          }
        } else {
          s = ExecuteTransaction(
              write_opts, /*thread=*/nullptr,
              [&](Transaction& txn) { return txn.Merge(cfh, key, v); });
        }
      } else {
        if (!FLAGS_use_txn) {
          if (FLAGS_user_timestamp_size > 0) {
            s = db_->Put(write_opts, cfh, key, ts, v);
          } else {
            s = db_->Put(write_opts, cfh, key, v);
          }
        } else {
          s = ExecuteTransaction(
              write_opts, /*thread=*/nullptr,
              [&](Transaction& txn) { return txn.Put(cfh, key, v); });
        }
      }

      if (!s.ok()) {
        pending_expected_value.Rollback();
        break;
      }
      pending_expected_value.Commit();
    }
    if (!s.ok()) {
      break;
    }
    ++cf_idx;
  }
  if (s.ok()) {
    s = db_->Flush(FlushOptions(), column_families_);
  }
  if (s.ok()) {
    CleanUpColumnFamilies();
    db_owner_.reset();
    db_ = nullptr;
    txn_db_ = nullptr;
    optimistic_txn_db_ = nullptr;
    secondary_db_.reset();

    db_preload_finished_.store(true);
    auto now = clock_->NowMicros();
    fprintf(stdout, "%s Reopening database in read-only\n",
            clock_->TimeToString(now / 1000000).c_str());
    // Reopen as read-only, can ignore all options related to updates
    Open(shared);
  } else {
    DB_STRESS_ASSERT_OK_MSG(s, "Failed to preload db");
  }
}

Status StressTest::SetOptions(ThreadState* thread) {
  assert(FLAGS_set_options_one_in > 0);
  std::unordered_map<std::string, std::string> opts;
  std::string name =
      options_index_[thread->rand.Next() % options_index_.size()];
  int value_idx = thread->rand.Next() % options_table_[name].size();
  if (name == "level0_file_num_compaction_trigger" ||
      name == "level0_slowdown_writes_trigger" ||
      name == "level0_stop_writes_trigger") {
    opts["level0_file_num_compaction_trigger"] =
        options_table_["level0_file_num_compaction_trigger"][value_idx];
    opts["level0_slowdown_writes_trigger"] =
        options_table_["level0_slowdown_writes_trigger"][value_idx];
    opts["level0_stop_writes_trigger"] =
        options_table_["level0_stop_writes_trigger"][value_idx];
  } else {
    opts[name] = options_table_[name][value_idx];
  }

  int rand_cf_idx = thread->rand.Next() % FLAGS_column_families;
  auto cfh = column_families_[rand_cf_idx];
  return db_->SetOptions(cfh, opts);
}

Options StressTest::GetOptions(int cf_id) {
  auto cfh = column_families_[cf_id];
  assert(cfh);
  return db_->GetOptions(cfh);
}

void StressTest::ProcessRecoveredPreparedTxns(SharedState* shared) {
  assert(txn_db_);
  std::vector<Transaction*> recovered_prepared_trans;
  txn_db_->GetAllPreparedTransactions(&recovered_prepared_trans);
  for (Transaction* txn : recovered_prepared_trans) {
    ProcessRecoveredPreparedTxnsHelper(txn, shared);
    delete txn;
  }
  recovered_prepared_trans.clear();
  txn_db_->GetAllPreparedTransactions(&recovered_prepared_trans);
  assert(recovered_prepared_trans.size() == 0);
}

void StressTest::ProcessRecoveredPreparedTxnsHelper(Transaction* txn,
                                                    SharedState* shared) {
  thread_local Random rand(static_cast<uint32_t>(FLAGS_seed));
  for (size_t i = 0; i < column_families_.size(); ++i) {
    std::unique_ptr<WBWIIterator> wbwi_iter(
        txn->GetWriteBatch()->NewIterator(column_families_[i]));
    for (wbwi_iter->SeekToFirst(); wbwi_iter->Valid(); wbwi_iter->Next()) {
      uint64_t key_val;
      if (GetIntVal(wbwi_iter->Entry().key.ToString(), &key_val)) {
        shared->SyncPendingPut(static_cast<int>(i) /* cf_idx */, key_val);
      }
    }
  }
  if (rand.OneIn(2)) {
    Status s = txn->Commit();
    DB_STRESS_ASSERT_OK(s);
  } else {
    Status s = txn->Rollback();
    DB_STRESS_ASSERT_OK(s);
  }
}

Status StressTest::NewTxn(WriteOptions& write_opts, ThreadState* thread,
                          std::unique_ptr<Transaction>* out_txn,
                          bool* commit_bypass_memtable) {
  if (!FLAGS_use_txn) {
    return Status::InvalidArgument("NewTxn when FLAGS_use_txn is not set");
  }
  write_opts.disableWAL = FLAGS_disable_wal;
  static std::atomic<uint64_t> txn_id = {0};
  if (FLAGS_use_optimistic_txn) {
    out_txn->reset(optimistic_txn_db_->BeginTransaction(write_opts));
    return Status::OK();
  } else {
    TransactionOptions txn_options;
    txn_options.use_only_the_last_commit_time_batch_for_recovery =
        FLAGS_use_only_the_last_commit_time_batch_for_recovery;
    txn_options.lock_timeout = 600000;  // 10 min
    txn_options.deadlock_detect = true;
    if (FLAGS_commit_bypass_memtable_one_in > 0 &&
        thread->rand.OneIn(FLAGS_commit_bypass_memtable_one_in)) {
      assert(FLAGS_txn_write_policy == 0);
      assert(FLAGS_user_timestamp_size == 0);
      if (thread->rand.OneIn(2)) {
        txn_options.commit_bypass_memtable = true;
      }
      if (thread->rand.OneIn(2)) {
        txn_options.large_txn_commit_optimize_threshold = 1;
      }
      if (thread->rand.OneIn(2) ||
          (!txn_options.commit_bypass_memtable &&
           txn_options.large_txn_commit_optimize_threshold != 1)) {
        txn_options.large_txn_commit_optimize_byte_threshold = 1;
      }
      if (commit_bypass_memtable) {
        *commit_bypass_memtable = txn_options.commit_bypass_memtable;
      }
    }
    out_txn->reset(txn_db_->BeginTransaction(write_opts, txn_options));
    auto istr = std::to_string(txn_id.fetch_add(1));
    Status s = (*out_txn)->SetName("xid" + istr);
    return s;
  }
}

Status StressTest::CommitTxn(Transaction& txn, ThreadState* thread) {
  if (!FLAGS_use_txn) {
    return Status::InvalidArgument("CommitTxn when FLAGS_use_txn is not set");
  }
  Status s = Status::OK();
  // We don't issue write to transaction's underlying WriteBatch in stress test
  assert(txn.GetWriteBatch()->GetWriteBatch()->Count());
  assert(txn.GetWriteBatch()->GetWBWIOpCount() ==
         txn.GetWriteBatch()->GetWriteBatch()->Count());
  if (FLAGS_use_optimistic_txn) {
    assert(optimistic_txn_db_);
    s = txn.Commit();
  } else {
    assert(txn_db_);
    s = txn.Prepare();
    const bool prepared = s.ok();
    std::shared_ptr<const Snapshot> timestamped_snapshot;
    if (s.ok()) {
      if (thread && FLAGS_create_timestamped_snapshot_one_in &&
          thread->rand.OneIn(FLAGS_create_timestamped_snapshot_one_in)) {
        uint64_t ts = raw_env->NowNanos();
        s = txn.CommitAndTryCreateSnapshot(/*notifier=*/nullptr, ts,
                                           &timestamped_snapshot);

        std::pair<Status, std::shared_ptr<const Snapshot>> res;
        if (thread->tid == 0) {
          uint64_t now = raw_env->NowNanos();
          res = txn_db_->CreateTimestampedSnapshot(now);
          if (res.first.ok()) {
            assert(res.second);
            assert(res.second->GetTimestamp() == now);
            if (timestamped_snapshot) {
              assert(res.second->GetTimestamp() >
                     timestamped_snapshot->GetTimestamp());
            }
          } else {
            assert(!res.second);
          }
        }
      } else {
        s = txn.Commit();
      }
    }
    if (prepared && !s.ok()) {
      Status rollback_s = txn.Rollback();
      int rollback_recovery_retries = 0;
      int resume_busy_count = 0;
      std::string last_resume_status;
      if (!rollback_s.ok() && IsErrorInjectedAndRetryable(rollback_s) &&
          db_ != nullptr) {
        // Retry budget: count only actual rollback attempts (after Resume
        // succeeds), not time spent waiting for recovery to finish on another
        // thread. With many concurrent threads hitting write errors, recovery
        // can take much longer than individual threads' retry budgets.
        constexpr int kMaxRollbackRetries = 100;
        constexpr int kMaxResumeBusyWaits = 3000;
        constexpr int kRetryIntervalMicros = 10 * 1000;
        for (; rollback_recovery_retries < kMaxRollbackRetries &&
               resume_busy_count < kMaxResumeBusyWaits && !rollback_s.ok();) {
          const Status resume_s = db_->Resume();
          if (resume_s.ok()) {
            ++rollback_recovery_retries;
            last_resume_status = resume_s.ToString();
            rollback_s = txn.Rollback();
            if (rollback_s.ok() || !IsErrorInjectedAndRetryable(rollback_s)) {
              break;
            }
          } else if (!resume_s.IsBusy()) {
            last_resume_status = resume_s.ToString();
            break;
          } else {
            ++resume_busy_count;
            last_resume_status = resume_s.ToString();
          }
          clock_->SleepForMicroseconds(kRetryIntervalMicros);
        }
      }
      if (!rollback_s.ok()) {
        fprintf(stderr,
                "Rollback after failed prepared transaction commit failed: "
                "txn=%s, commit_status=%s, rollback_status=%s, "
                "recovery_retries=%d, resume_busy_count=%d, "
                "last_resume_status=%s\n",
                txn.GetName().c_str(), s.ToString().c_str(),
                rollback_s.ToString().c_str(), rollback_recovery_retries,
                resume_busy_count, last_resume_status.c_str());
      }
    }
    if (thread && FLAGS_create_timestamped_snapshot_one_in > 0 &&
        thread->rand.OneInOpt(50000)) {
      uint64_t now = raw_env->NowNanos();
      constexpr uint64_t time_diff = static_cast<uint64_t>(1000) * 1000 * 1000;
      txn_db_->ReleaseTimestampedSnapshotsOlderThan(now - time_diff);
    }
  }
  return s;
}

bool StressTest::IsExpectedTxnError(const Status& s) {
  if ((s.IsDeadlock() || s.IsTimedOut()) &&
      (FLAGS_use_multiget || FLAGS_use_multi_get_entity)) {
    return true;
  }
  // Optimistic transaction may return TryAgain when memtable history is
  // insufficient for conflict detection (controlled by
  // max_write_buffer_size_to_maintain). ExecuteTransaction retries up to 10
  // times, and if all retries fail, it returns TryAgain. This is an expected
  // condition and should not crash the stress test.
  if (s.IsTryAgain() && FLAGS_use_optimistic_txn) {
    return true;
  }
  return false;
}

Status StressTest::ExecuteTransaction(WriteOptions& write_opts,
                                      ThreadState* thread,
                                      std::function<Status(Transaction&)>&& ops,
                                      bool* commit_bypass_memtable) {
  std::unique_ptr<Transaction> txn;
  Status s = NewTxn(write_opts, thread, &txn, commit_bypass_memtable);
  std::string try_again_messages;
  if (s.ok()) {
    for (int tries = 1;; ++tries) {
      s = ops(*txn);
      if (s.ok()) {
        s = CommitTxn(*txn, thread);
        if (s.ok()) {
          break;
        }
      }
      // Optimistic txn might return TryAgain, in which case rollback
      // and try again.
      if (!s.IsTryAgain() || !FLAGS_use_optimistic_txn) {
        break;
      }
      // Record and report historical TryAgain messages for debugging
      try_again_messages +=
          std::to_string(SystemClock::Default()->NowMicros() / 1000);
      try_again_messages += "ms ";
      try_again_messages += s.getState();
      try_again_messages += "\n";
      // In theory, each Rollback after TryAgain should have an independent
      // chance of success, so too many retries could indicate something is
      // not working properly.
      if (tries >= 10) {
        s = Status::TryAgain(try_again_messages);
        break;
      }
      s = txn->Rollback();
      if (!s.ok()) {
        break;
      }
    }
  }
  return s;
}

void StressTest::OperateDb(ThreadState* thread) {
  ReadOptions read_opts(FLAGS_verify_checksum, true);
  read_opts.rate_limiter_priority =
      FLAGS_rate_limit_user_ops ? Env::IO_USER : Env::IO_TOTAL;
  read_opts.async_io = FLAGS_async_io;
  read_opts.adaptive_readahead = FLAGS_adaptive_readahead;
  read_opts.readahead_size = FLAGS_readahead_size;
  read_opts.auto_readahead_size = FLAGS_auto_readahead_size;
  read_opts.fill_cache = FLAGS_fill_cache;
  read_opts.optimize_multiget_for_io = FLAGS_optimize_multiget_for_io;
  read_opts.allow_unprepared_value = FLAGS_allow_unprepared_value;
  read_opts.auto_refresh_iterator_with_snapshot =
      FLAGS_auto_refresh_iterator_with_snapshot;
  if (udi_factory_) {
    read_opts.read_index = EffectiveReadIndex();
  }
  std::unique_ptr<StressReadScopedBlockBufferProvider>
      read_scoped_block_buffer_provider;
  if (FLAGS_read_scoped_block_buffer_provider) {
    read_scoped_block_buffer_provider =
        std::make_unique<StressReadScopedBlockBufferProvider>();
    read_opts.read_scoped_block_buffer_provider =
        read_scoped_block_buffer_provider.get();
  }
  WriteOptions write_opts;
  if (FLAGS_rate_limit_auto_wal_flush) {
    write_opts.rate_limiter_priority = Env::IO_USER;
  }
  write_opts.memtable_insert_hint_per_batch =
      FLAGS_memtable_insert_hint_per_batch;
  auto shared = thread->shared;
  char value[100];
  std::string from_db;
  if (FLAGS_sync) {
    write_opts.sync = true;
  }
  write_opts.disableWAL = FLAGS_disable_wal;
  write_opts.protection_bytes_per_key = FLAGS_batch_protection_bytes_per_key;
  const int prefix_bound = static_cast<int>(FLAGS_readpercent) +
                           static_cast<int>(FLAGS_prefixpercent);
  const int write_bound = prefix_bound + static_cast<int>(FLAGS_writepercent);
  const int del_bound = write_bound + static_cast<int>(FLAGS_delpercent);
  const int delrange_bound =
      del_bound + static_cast<int>(FLAGS_delrangepercent);
  const int iterate_bound =
      delrange_bound + static_cast<int>(FLAGS_iterpercent);

  const uint64_t ops_per_open = FLAGS_ops_per_thread / (FLAGS_reopen + 1);

  thread->stats.Start();
  for (int open_cnt = 0; open_cnt <= FLAGS_reopen; ++open_cnt) {
    if (thread->shared->HasVerificationFailedYet() ||
        thread->shared->ShouldStopTest()) {
      break;
    }
    if (open_cnt != 0) {
      ScopedThreadOperation op(
          thread, StressOperationType::kReopen,
          ScopedThreadOperation::FinishAction::kFinishSingleOp);
      MutexLock l(thread->shared->GetMutex());
      while (!thread->snapshot_queue.empty()) {
        db_->ReleaseSnapshot(thread->snapshot_queue.front().second.snapshot);
        delete thread->snapshot_queue.front().second.key_vec;
        thread->snapshot_queue.pop();
      }
      thread->shared->IncVotedReopen();
      if (thread->shared->AllVotedReopen()) {
        thread->shared->GetStressTest()->Reopen(thread);
        thread->shared->GetCondVar()->SignalAll();
      } else {
        thread->shared->GetCondVar()->Wait();
      }
      // Commenting this out as we don't want to reset stats on each open.
      // thread->stats.Start();
      if (udi_factory_) {
        read_opts.read_index = EffectiveReadIndex();
      }
    }

#ifndef NDEBUG
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kRead, thread->shared->GetSeed(),
          FLAGS_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kRead);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kWrite, thread->shared->GetSeed(),
          FLAGS_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kWrite);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataRead, thread->shared->GetSeed(),
          FLAGS_metadata_read_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);

      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kMetadataWrite, thread->shared->GetSeed(),
          FLAGS_metadata_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataWrite);
    }
#else
    // Most fault injection is debug-only. Keep the dedicated file-scoped WAL
    // recovery mode available in optimized and sanitizer stress binaries so
    // the recovery state machine gets the same matrix coverage as normal
    // db_stress operation.
    if (db_fault_injection_fs_ && FLAGS_file_scope_wal_write_faults) {
      db_fault_injection_fs_->SetThreadLocalErrorContext(
          FaultInjectionIOType::kWrite, thread->shared->GetSeed(),
          FLAGS_write_fault_one_in,
          FLAGS_inject_error_severity == 1 /* retryable */,
          FLAGS_inject_error_severity == 2 /* has_data_loss*/);
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kWrite);
    }
#endif  // NDEBUG

    for (uint64_t i = 0; i < ops_per_open; i++) {
      if (thread->shared->HasVerificationFailedYet()) {
        break;
      }
      // Change Options
      if (thread->rand.OneInOpt(FLAGS_set_options_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kSetOptions);
        Status s = SetOptions(thread);
        ProcessStatus(shared, "SetOptions", s);
      }

      if (thread->rand.OneInOpt(FLAGS_set_in_place_one_in)) {
        options_.inplace_update_support ^= options_.inplace_update_support;
      }

      if (thread->tid == 0 && FLAGS_verify_db_one_in > 0 &&
          thread->rand.OneIn(FLAGS_verify_db_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kVerifyDb);
        //  Temporarily disable error injection for verification
        if (db_fault_injection_fs_) {
          db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
        }
        ContinuouslyVerifyDb(thread);
        //  Enable back error injection disabled for verification
        if (db_fault_injection_fs_) {
          db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
        }
        if (thread->shared->ShouldStopTest()) {
          break;
        }
      }

      MaybeOpenReadOnlyOnPrimary(thread);

      MaybeClearOneColumnFamily(thread);

      if (thread->rand.OneInOpt(FLAGS_manual_wal_flush_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kManualWalFlush);
        bool sync = thread->rand.OneIn(2) ? true : false;
        Status s = db_->FlushWAL(sync);
        if (!s.ok() && !IsErrorInjectedAndRetryable(s) &&
            !(sync && s.IsNotSupported())) {
          fprintf(stderr, "FlushWAL(sync=%s) failed: %s\n",
                  (sync ? "true" : "false"), s.ToString().c_str());
        }
      }

      if (thread->rand.OneInOpt(FLAGS_lock_wal_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kLockWal);
        Status s = db_->LockWAL();
        if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
          fprintf(stderr, "LockWAL() failed: %s\n", s.ToString().c_str());
        } else if (s.ok()) {
          //  Temporarily disable error injection for verification
          if (db_fault_injection_fs_) {
            db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
          }

          // Verify no writes during LockWAL
          auto old_seqno = db_->GetLatestSequenceNumber();
          // And also that WAL is not changed during LockWAL()
          std::unique_ptr<WalFile> old_wal;
          s = db_->GetCurrentWalFile(&old_wal);
          if (!s.ok()) {
            fprintf(stderr, "GetCurrentWalFile() failed: %s\n",
                    s.ToString().c_str());
          } else {
            // Yield for a while
            do {
              std::this_thread::yield();
            } while (thread->rand.OneIn(2));
            // Current WAL and size should not have changed
            std::unique_ptr<WalFile> new_wal;
            s = db_->GetCurrentWalFile(&new_wal);
            if (!s.ok()) {
              fprintf(stderr, "GetCurrentWalFile() failed: %s\n",
                      s.ToString().c_str());
            } else {
              if (old_wal->LogNumber() != new_wal->LogNumber()) {
                fprintf(stderr,
                        "Failed: WAL number changed during LockWAL(): %" PRIu64
                        " to %" PRIu64 "\n",
                        old_wal->LogNumber(), new_wal->LogNumber());
              }
              if (old_wal->SizeFileBytes() != new_wal->SizeFileBytes()) {
                fprintf(stderr,
                        "Failed: WAL %" PRIu64
                        " size changed during LockWAL(): %" PRIu64
                        " to %" PRIu64 "\n",
                        old_wal->LogNumber(), old_wal->SizeFileBytes(),
                        new_wal->SizeFileBytes());
              }
            }
          }
          // Verify no writes during LockWAL
          auto new_seqno = db_->GetLatestSequenceNumber();
          if (old_seqno != new_seqno) {
            fprintf(
                stderr,
                "Failure: latest seqno changed from %u to %u with WAL locked\n",
                (unsigned)old_seqno, (unsigned)new_seqno);
          }
          // Verification done. Now unlock WAL
          s = db_->UnlockWAL();
          if (!s.ok()) {
            fprintf(stderr, "UnlockWAL() failed: %s\n", s.ToString().c_str());
          }

          //  Enable back error injection disabled for verification
          if (db_fault_injection_fs_) {
            db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
          }
        }
      }

      if (thread->rand.OneInOpt(FLAGS_sync_wal_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kSyncWal);
        Status s = db_->SyncWAL();
        if (!s.ok() && !s.IsNotSupported() && !IsErrorInjectedAndRetryable(s)) {
          fprintf(stderr, "SyncWAL() failed: %s\n", s.ToString().c_str());
        }
      }

      int rand_column_family = thread->rand.Next() % FLAGS_column_families;
      ColumnFamilyHandle* column_family = column_families_[rand_column_family];

      if (thread->rand.OneInOpt(FLAGS_compact_files_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kCompactFiles);
        MaybeRecordOperationContext(thread, StressOperationType::kCompactFiles,
                                    {rand_column_family}, {});
        TestCompactFiles(thread, column_family);
      }

      int64_t rand_key = GenerateOneKey(thread, i);
      std::string keystr = Key(rand_key);
      Slice key = keystr;

      if (thread->rand.OneInOpt(FLAGS_compact_range_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kCompactRange);
        MaybeRecordOperationContext(thread, StressOperationType::kCompactRange,
                                    {rand_column_family}, {rand_key});
        TestCompactRange(thread, rand_key, key, column_family);
        if (thread->shared->HasVerificationFailedYet()) {
          break;
        }
      }

      if (thread->rand.OneInOpt(FLAGS_promote_l0_one_in)) {
        TestPromoteL0(thread, column_family);
      }

      std::vector<int> rand_column_families =
          GenerateColumnFamilies(FLAGS_column_families, rand_column_family);

      if (thread->rand.OneInOpt(FLAGS_flush_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kFlush);
        MaybeRecordOperationContext(thread, StressOperationType::kFlush,
                                    rand_column_families, {});
        TestFlush(thread, rand_column_families);
      }

      if (thread->rand.OneInOpt(FLAGS_get_live_files_apis_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kGetLiveFiles);
        Status s_1 = TestGetLiveFiles();
        ProcessStatus(shared, "GetLiveFiles", s_1);
        Status s_2 = TestGetLiveFilesMetaData();
        ProcessStatus(shared, "GetLiveFilesMetaData", s_2);
        // TODO: enable again after making `GetLiveFilesStorageInfo()`
        // compatible with `Options::recycle_log_file_num`
        if (FLAGS_recycle_log_file_num == 0) {
          Status s_3 = TestGetLiveFilesStorageInfo();
          ProcessStatus(shared, "GetLiveFilesStorageInfo", s_3);
        }
      }

      if (thread->rand.OneInOpt(FLAGS_get_all_column_family_metadata_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kMetadata);
        Status status = TestGetAllColumnFamilyMetaData();
        ProcessStatus(shared, "GetAllColumnFamilyMetaData", status);
      }

      if (thread->rand.OneInOpt(FLAGS_get_sorted_wal_files_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kMetadata);
        Status status = TestGetSortedWalFiles();
        ProcessStatus(shared, "GetSortedWalFiles", status);
      }

      if (thread->rand.OneInOpt(FLAGS_get_current_wal_file_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kMetadata);
        Status status = TestGetCurrentWalFile();
        ProcessStatus(shared, "GetCurrentWalFile", status);
      }

      if (thread->rand.OneInOpt(FLAGS_reset_stats_one_in)) {
        Status status = TestResetStats();
        ProcessStatus(shared, "ResetStats", status);
      }

      if (thread->rand.OneInOpt(FLAGS_pause_background_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kPauseBackground);
        Status status = TestPauseBackground(thread);
        ProcessStatus(shared, "Pause/ContinueBackgroundWork", status);
      }

      if (thread->rand.OneInOpt(FLAGS_disable_file_deletions_one_in)) {
        ScopedThreadOperation op(thread,
                                 StressOperationType::kDisableFileDeletions);
        Status status = TestDisableFileDeletions(thread);
        ProcessStatus(shared, "TestDisableFileDeletions", status);
      }

      if (thread->rand.OneInOpt(FLAGS_disable_manual_compaction_one_in)) {
        ScopedThreadOperation op(thread,
                                 StressOperationType::kDisableManualCompaction);
        Status status = TestDisableManualCompaction(thread);
        ProcessStatus(shared, "TestDisableManualCompaction", status);
      }

      if (thread->rand.OneInOpt(FLAGS_abort_and_resume_compactions_one_in)) {
        Status status = TestAbortAndResumeCompactions(thread);
        ProcessStatus(shared, "TestAbortAndResumeCompactions", status);
      }

      if (thread->rand.OneInOpt(FLAGS_abort_and_resume_cf_compactions_one_in)) {
        Status status = TestAbortAndResumeCfCompactions(thread);
        ProcessStatus(shared, "TestAbortAndResumeCfCompactions", status);
      }

      if (thread->rand.OneInOpt(FLAGS_verify_checksum_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kVerifyChecksum);
        ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
        ThreadStatusUtil::SetThreadOperation(
            ThreadStatus::OperationType::OP_VERIFY_DB_CHECKSUM);
        Status status = db_->VerifyChecksum();
        ThreadStatusUtil::ResetThreadStatus();
        ProcessStatus(shared, "VerifyChecksum", status);
      }

      if (thread->rand.OneInOpt(FLAGS_verify_file_checksums_one_in)) {
        ScopedThreadOperation op(thread,
                                 StressOperationType::kVerifyFileChecksums);
        ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
        ThreadStatusUtil::SetThreadOperation(
            ThreadStatus::OperationType::OP_VERIFY_FILE_CHECKSUMS);
        Status status = db_->VerifyFileChecksums(read_opts);
        ThreadStatusUtil::ResetThreadStatus();
        ProcessStatus(shared, "VerifyFileChecksums", status);
      }

      if (thread->rand.OneInOpt(FLAGS_get_property_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kGetProperty);
        // TestGetProperty doesn't return status for us to tell whether it has
        // failed due to injected error. So we disable fault injection to avoid
        // false positive
        if (db_fault_injection_fs_) {
          db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
        }

        TestGetProperty(thread);

        if (db_fault_injection_fs_) {
          db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
        }
      }

      if (thread->rand.OneInOpt(FLAGS_get_properties_of_all_tables_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kTableProperties);
        Status status = TestGetPropertiesOfAllTables();
        ProcessStatus(shared, "TestGetPropertiesOfAllTables", status);
      }

      std::vector<int64_t> rand_keys = GenerateKeys(rand_key);

      if (thread->rand.OneInOpt(FLAGS_ingest_external_file_one_in)) {
        ScopedThreadOperation op(thread,
                                 StressOperationType::kIngestExternalFile);
        MaybeRecordOperationContext(thread,
                                    StressOperationType::kIngestExternalFile,
                                    rand_column_families, rand_keys);
        TestIngestExternalFile(thread, rand_column_families, rand_keys);
      }

      if (thread->rand.OneInOpt(FLAGS_backup_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kBackup);
        MaybeRecordOperationContext(thread, StressOperationType::kBackup,
                                    rand_column_families, rand_keys);
        // Beyond a certain DB size threshold, this test becomes heavier than
        // it's worth.
        uint64_t total_size = 0;
        if (FLAGS_backup_max_size > 0) {
          std::vector<FileAttributes> files;
          GetDbEnv()->GetChildrenFileAttributes(GetDbPath(), &files);
          for (auto& file : files) {
            total_size += file.size_bytes;
          }
        }

        if (total_size <= FLAGS_backup_max_size) {
          // TODO(hx235): enable error injection with
          // backup/restore after fixing the various issues it surfaces
          if (db_fault_injection_fs_) {
            db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
          }
          Status s = TestBackupRestore(thread, rand_column_families, rand_keys);
          if (db_fault_injection_fs_) {
            db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
          }
          ProcessStatus(shared, "Backup/restore", s);
        }
      }

      if (thread->rand.OneInOpt(FLAGS_checkpoint_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kCheckpoint);
        MaybeRecordOperationContext(thread, StressOperationType::kCheckpoint,
                                    rand_column_families, rand_keys);
        Status s = TestCheckpoint(thread, rand_column_families, rand_keys);
        ProcessStatus(shared, "Checkpoint", s);
      }

      if (thread->rand.OneInOpt(FLAGS_approximate_size_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kApproximateSize);
        MaybeRecordOperationContext(thread,
                                    StressOperationType::kApproximateSize,
                                    rand_column_families, rand_keys);
        Status s =
            TestApproximateSize(thread, i, rand_column_families, rand_keys);
        ProcessStatus(shared, "ApproximateSize", s);
      }
      if (thread->rand.OneInOpt(FLAGS_acquire_snapshot_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kSnapshot);
        MaybeRecordOperationContext(thread, StressOperationType::kSnapshot,
                                    {rand_column_family}, {rand_key});
        TestAcquireSnapshot(thread, rand_column_family, keystr, i);
      }

      /*always*/ {
        ScopedThreadOperation op(thread, StressOperationType::kSnapshot);
        Status s = MaybeReleaseSnapshots(thread, i);
        ProcessStatus(shared, "Snapshot", s);
      }

      // Assign timestamps if necessary.
      std::string read_ts_str;
      Slice read_ts;
      if (FLAGS_user_timestamp_size > 0) {
        read_ts_str = GetReadTimestamp();
        read_ts = read_ts_str;
        read_opts.timestamp = &read_ts;
      }

      if (thread->rand.OneInOpt(FLAGS_key_may_exist_one_in)) {
        ScopedThreadOperation op(thread, StressOperationType::kKeyMayExist);
        MaybeRecordOperationContext(thread, StressOperationType::kKeyMayExist,
                                    rand_column_families, rand_keys);
        TestKeyMayExist(thread, read_opts, rand_column_families, rand_keys);
      }
      // Historical expected-state restore replays exactly
      // `db->GetLatestSequenceNumber() - saved_seqno_` write ops from the
      // trace. Missing trace entries are fatal, while extra suffix entries are
      // tolerated. Keep fault injection disabled during user writes until each
      // injected failure mode is audited against that contract.
      bool disable_fault_injection_during_user_write =
          db_fault_injection_fs_ && MightHaveUnsyncedDataLoss();
      int prob_op = thread->rand.Uniform(100);
      if (prob_op >= 0 && prob_op < static_cast<int>(FLAGS_readpercent)) {
        assert(0 <= prob_op);
        // OPERATION read
        ScopedThreadOperation op(
            thread, StressOperationType::kRead,
            ScopedThreadOperation::FinishAction::kFinishSingleOp);
        ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
        if (FLAGS_use_multi_get_entity) {
          constexpr uint64_t max_batch_size = 64;
          const uint64_t batch_size = std::min(
              static_cast<uint64_t>(thread->rand.Uniform(max_batch_size)) + 1,
              ops_per_open - i);
          assert(batch_size >= 1);
          assert(batch_size <= max_batch_size);
          assert(i + batch_size <= ops_per_open);

          rand_keys = GenerateNKeys(thread, static_cast<int>(batch_size), i);
          MaybeRecordOperationContext(thread, StressOperationType::kRead,
                                      rand_column_families, rand_keys);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_MULTIGETENTITY);
          TestMultiGetEntity(thread, read_opts, rand_column_families,
                             rand_keys);
          i += batch_size - 1;
        } else if (FLAGS_use_get_entity) {
          MaybeRecordOperationContext(thread, StressOperationType::kRead,
                                      rand_column_families, rand_keys);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_GETENTITY);
          TestGetEntity(thread, read_opts, rand_column_families, rand_keys);
        } else if (FLAGS_use_multiget) {
          // Leave room for one more iteration of the loop with a single key
          // batch. This is to ensure that each thread does exactly the same
          // number of ops
          int multiget_batch_size = static_cast<int>(
              std::min(static_cast<uint64_t>(thread->rand.Uniform(64)),
                       FLAGS_ops_per_thread - i - 1));
          // If its the last iteration, ensure that multiget_batch_size is 1
          multiget_batch_size = std::max(multiget_batch_size, 1);
          rand_keys = GenerateNKeys(thread, multiget_batch_size, i);
          MaybeRecordOperationContext(thread, StressOperationType::kRead,
                                      rand_column_families, rand_keys);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_MULTIGET);
          TestMultiGet(thread, read_opts, rand_column_families, rand_keys);
          i += multiget_batch_size - 1;
        } else {
          MaybeRecordOperationContext(thread, StressOperationType::kRead,
                                      rand_column_families, rand_keys);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_GET);
          TestGet(thread, read_opts, rand_column_families, rand_keys);
        }
        ThreadStatusUtil::ResetThreadStatus();
      } else if (prob_op < prefix_bound) {
        assert(static_cast<int>(FLAGS_readpercent) <= prob_op);
        // OPERATION prefix scan
        ScopedThreadOperation op(
            thread, StressOperationType::kPrefixScan,
            ScopedThreadOperation::FinishAction::kFinishSingleOp);
        MaybeRecordOperationContext(thread, StressOperationType::kPrefixScan,
                                    rand_column_families, rand_keys);
        // keys are 8 bytes long, prefix size is FLAGS_prefix_size. There are
        // (8 - FLAGS_prefix_size) bytes besides the prefix. So there will
        // be 2 ^ ((8 - FLAGS_prefix_size) * 8) possible keys with the same
        // prefix
        TestPrefixScan(thread, read_opts, rand_column_families, rand_keys);
      } else if (prob_op < write_bound) {
        assert(prefix_bound <= prob_op);
        // OPERATION write
        ScopedThreadOperation op(
            thread, StressOperationType::kWrite,
            ScopedThreadOperation::FinishAction::kFinishSingleOp);
        MaybeRecordOperationContext(thread, StressOperationType::kWrite,
                                    rand_column_families, rand_keys);
        if (disable_fault_injection_during_user_write) {
          db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
        }
        TestPut(thread, write_opts, read_opts, rand_column_families, rand_keys,
                value);
        if (disable_fault_injection_during_user_write) {
          db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
        }
      } else if (prob_op < del_bound) {
        assert(write_bound <= prob_op);
        // OPERATION delete
        ScopedThreadOperation op(
            thread, StressOperationType::kDelete,
            ScopedThreadOperation::FinishAction::kFinishSingleOp);
        MaybeRecordOperationContext(thread, StressOperationType::kDelete,
                                    rand_column_families, rand_keys);
        if (disable_fault_injection_during_user_write) {
          db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
        }
        TestDelete(thread, write_opts, rand_column_families, rand_keys);
        if (disable_fault_injection_during_user_write) {
          db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
        }
      } else if (prob_op < delrange_bound) {
        assert(del_bound <= prob_op);
        // OPERATION delete range
        ScopedThreadOperation op(
            thread, StressOperationType::kDeleteRange,
            ScopedThreadOperation::FinishAction::kFinishSingleOp);
        MaybeRecordOperationContext(thread, StressOperationType::kDeleteRange,
                                    rand_column_families, rand_keys);
        if (disable_fault_injection_during_user_write) {
          db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
        }
        TestDeleteRange(thread, write_opts, rand_column_families, rand_keys);
        if (disable_fault_injection_during_user_write) {
          db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
        }
      } else if (prob_op < iterate_bound) {
        assert(delrange_bound <= prob_op);
        // OPERATION iterate
        ScopedThreadOperation op(
            thread, StressOperationType::kIterate,
            ScopedThreadOperation::FinishAction::kFinishSingleOp);
        if (FLAGS_use_multiscan) {
          int num_seeks = static_cast<int>(
              std::min(static_cast<uint64_t>(thread->rand.Uniform(64)),
                       static_cast<uint64_t>(FLAGS_ops_per_thread - i - 1)));
          // Generate 2x num_seeks random keys, as each scan has a start key
          // and an upper bound
          rand_keys = GenerateNKeys(thread, num_seeks * 2, i);
          i += num_seeks - 1;
          MaybeRecordOperationContext(thread, StressOperationType::kIterate,
                                      rand_column_families, rand_keys);
          ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_DBITERATOR);
          Status s =
              TestMultiScan(thread, read_opts, rand_column_families, rand_keys);
          ProcessStatus(shared, "MultiScan", s);
          ThreadStatusUtil::ResetThreadStatus();
        } else if (!FLAGS_skip_verifydb &&
                   thread->rand.OneInOpt(
                       FLAGS_verify_iterator_with_expected_state_one_in)) {
          ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
          MaybeRecordOperationContext(thread, StressOperationType::kIterate,
                                      rand_column_families, rand_keys);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_DBITERATOR);
          TestIterateAgainstExpected(thread, read_opts, rand_column_families,
                                     rand_keys);
          ThreadStatusUtil::ResetThreadStatus();
        } else {
          int num_seeks = static_cast<int>(std::min(
              std::max(static_cast<uint64_t>(thread->rand.Uniform(4)),
                       static_cast<uint64_t>(1)),
              std::max(static_cast<uint64_t>(FLAGS_ops_per_thread - i - 1),
                       static_cast<uint64_t>(1))));
          rand_keys = GenerateNKeys(thread, num_seeks, i);
          i += num_seeks - 1;
          MaybeRecordOperationContext(thread, StressOperationType::kIterate,
                                      rand_column_families, rand_keys);
          ThreadStatusUtil::SetEnableTracking(FLAGS_enable_thread_tracking);
          ThreadStatusUtil::SetThreadOperation(
              ThreadStatus::OperationType::OP_DBITERATOR);
          Status s;
          if (FLAGS_use_multi_cf_iterator && FLAGS_use_attribute_group) {
            s = TestIterateAttributeGroups(thread, read_opts,
                                           rand_column_families, rand_keys);
            ProcessStatus(shared, "IterateAttributeGroups", s);
          } else {
            s = TestIterate(thread, read_opts, rand_column_families, rand_keys);
            ProcessStatus(shared, "Iterate", s);
          }
          ThreadStatusUtil::ResetThreadStatus();
        }
      } else {
        assert(iterate_bound <= prob_op);
        ScopedThreadOperation op(
            thread, StressOperationType::kCustom,
            ScopedThreadOperation::FinishAction::kFinishSingleOp);
        MaybeRecordOperationContext(thread, StressOperationType::kCustom,
                                    rand_column_families, rand_keys);
        TestCustomOperations(thread, rand_column_families);
      }
    }

#ifndef NDEBUG
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
    }
#endif  // NDEBUG
  }
  while (!thread->snapshot_queue.empty()) {
    db_->ReleaseSnapshot(thread->snapshot_queue.front().second.snapshot);
    delete thread->snapshot_queue.front().second.key_vec;
    thread->snapshot_queue.pop();
  }

  thread->stats.Stop();
}

// Generated a list of keys that close to boundaries of SST keys.
// If there isn't any SST file in the DB, return empty list.
std::vector<std::string> StressTest::GetWhiteBoxKeys(ThreadState* thread,
                                                     DB* db,
                                                     ColumnFamilyHandle* cfh,
                                                     size_t num_keys) {
  ColumnFamilyMetaData cfmd;
  db->GetColumnFamilyMetaData(cfh, &cfmd);
  std::vector<std::string> boundaries;
  for (const LevelMetaData& lmd : cfmd.levels) {
    for (const SstFileMetaData& sfmd : lmd.files) {
      // If FLAGS_user_timestamp_size > 0, then both smallestkey and largestkey
      // have timestamps.
      const auto& skey = sfmd.smallestkey;
      const auto& lkey = sfmd.largestkey;
      assert(skey.size() >= FLAGS_user_timestamp_size);
      assert(lkey.size() >= FLAGS_user_timestamp_size);
      boundaries.push_back(
          skey.substr(0, skey.size() - FLAGS_user_timestamp_size));
      boundaries.push_back(
          lkey.substr(0, lkey.size() - FLAGS_user_timestamp_size));
    }
  }
  if (boundaries.empty()) {
    return {};
  }

  std::vector<std::string> ret;
  for (size_t j = 0; j < num_keys; j++) {
    std::string k =
        boundaries[thread->rand.Uniform(static_cast<int>(boundaries.size()))];
    if (thread->rand.OneIn(3)) {
      // Reduce one byte from the string
      for (int i = static_cast<int>(k.length()) - 1; i >= 0; i--) {
        uint8_t cur = k[i];
        if (cur > 0) {
          k[i] = static_cast<char>(cur - 1);
          break;
        } else if (i > 0) {
          k[i] = 0xFFu;
        }
      }
    } else if (thread->rand.OneIn(2)) {
      // Add one byte to the string
      for (int i = static_cast<int>(k.length()) - 1; i >= 0; i--) {
        uint8_t cur = k[i];
        if (cur < 255) {
          k[i] = static_cast<char>(cur + 1);
          break;
        } else if (i > 0) {
          k[i] = 0x00;
        }
      }
    }
    ret.push_back(k);
  }
  return ret;
}

// Given a key K, this creates an iterator which scans to K and then
// does a random sequence of Next/Prev operations.
Status StressTest::TestIterate(ThreadState* thread,
                               const ReadOptions& read_opts,
                               const std::vector<int>& rand_column_families,
                               const std::vector<int64_t>& rand_keys) {
  auto new_iter_func = [&rand_column_families, this](const ReadOptions& ro) {
    if (FLAGS_use_multi_cf_iterator) {
      std::vector<ColumnFamilyHandle*> cfhs;
      cfhs.reserve(rand_column_families.size());
      for (auto cf_index : rand_column_families) {
        cfhs.emplace_back(column_families_[cf_index]);
      }
      assert(!cfhs.empty());
      return db_->NewCoalescingIterator(ro, cfhs);
    } else {
      ColumnFamilyHandle* const cfh = column_families_[rand_column_families[0]];
      assert(cfh);
      return std::unique_ptr<Iterator>(db_->NewIterator(ro, cfh));
    }
  };

  auto verify_func = [](Iterator* iter) {
    if (!VerifyWideColumns(iter->value(), iter->columns())) {
      fprintf(stderr,
              "Value and columns inconsistent for iterator: value: %s, "
              "columns: %s\n",
              iter->value().ToString(/* hex */ true).c_str(),
              WideColumnsToHex(iter->columns()).c_str());
      return false;
    }
    return true;
  };

  return TestIterateImpl<Iterator>(thread, read_opts, rand_column_families,
                                   rand_keys, new_iter_func, verify_func);
}

Status StressTest::TestIterateAttributeGroups(
    ThreadState* thread, const ReadOptions& read_opts,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  auto new_iter_func = [&rand_column_families, this](const ReadOptions& ro) {
    assert(FLAGS_use_multi_cf_iterator);
    std::vector<ColumnFamilyHandle*> cfhs;
    cfhs.reserve(rand_column_families.size());
    for (auto cf_index : rand_column_families) {
      cfhs.emplace_back(column_families_[cf_index]);
    }
    assert(!cfhs.empty());
    return db_->NewAttributeGroupIterator(ro, cfhs);
  };
  auto verify_func = [](AttributeGroupIterator* iter) {
    if (!VerifyIteratorAttributeGroups(iter->attribute_groups())) {
      // TODO - print out attribute group values
      fprintf(stderr,
              "one of the columns in the attribute groups inconsistent for "
              "iterator\n");
      return false;
    }
    return true;
  };

  return TestIterateImpl<AttributeGroupIterator>(
      thread, read_opts, rand_column_families, rand_keys, new_iter_func,
      verify_func);
}

Status StressTest::TestMultiScan(ThreadState* thread,
                                 const ReadOptions& read_opts,
                                 const std::vector<int>& rand_column_families,
                                 const std::vector<int64_t>& rand_keys) {
  size_t num_scans = rand_keys.size() / 2;
  assert(!rand_column_families.empty());
  assert(!rand_keys.empty());

  ThreadStatus::OperationType cur_op_type =
      ThreadStatusUtil::GetThreadOperation();
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);
  ManagedSnapshot snapshot_guard(db_);
  ThreadStatusUtil::SetThreadOperation(cur_op_type);

  ReadOptions ro = read_opts;
  ro.snapshot = snapshot_guard.snapshot();

  std::string read_ts_str;
  Slice read_ts_slice;
  MaybeUseOlderTimestampForRangeScan(thread, read_ts_str, read_ts_slice, ro);

  std::vector<std::string> start_key_strs;
  std::vector<std::string> end_key_strs;
  // TODO support reverse BytewiseComparator in the stress test
  MultiScanArgs scan_opts(options_.comparator);
  const bool reverse_multiscan = FLAGS_multiscan_reverse;
  scan_opts.reverse = reverse_multiscan;
  scan_opts.use_async_io =
      FLAGS_multiscan_use_async_io &&
      CheckFSFeatureSupport(options_.env->GetFileSystem().get(),
                            FSSupportedOps::kAsyncIO);
  std::shared_ptr<IODispatcher> io_dispatcher;
  if (FLAGS_multiscan_max_prefetch_memory_bytes > 0) {
    IODispatcherOptions io_opts;
    io_opts.max_prefetch_memory_bytes =
        FLAGS_multiscan_max_prefetch_memory_bytes;
    io_dispatcher.reset(NewIODispatcher(io_opts));
    scan_opts.io_dispatcher = io_dispatcher;
  }
  start_key_strs.reserve(num_scans);
  end_key_strs.reserve(num_scans);

  // Will be initialized before Seek() below.
  Slice lb;
  Slice ub;
  if (reverse_multiscan) {
    ro.iterate_lower_bound = &lb;
  }
  ro.iterate_upper_bound = &ub;
  for (size_t i = 0; i < num_scans * 2; i += 2) {
    assert(rand_keys[i] <= rand_keys[i + 1]);
    start_key_strs.emplace_back(Key(rand_keys[i]));
    end_key_strs.emplace_back(Key(rand_keys[i + 1]));
    scan_opts.insert(Slice(start_key_strs.back()), Slice(end_key_strs.back()));
  }

  ScanWitnessLog witness(thread, "multi_scan");
  if (witness.Enabled()) {
    std::ostringstream start_oss;
    start_oss << "action=start cf=" << rand_column_families[0]
              << " range_count=" << num_scans
              << " reverse=" << reverse_multiscan
              << " multiscan_async_io=" << scan_opts.use_async_io
              << " prefetch_memory_limit="
              << FLAGS_multiscan_max_prefetch_memory_bytes << " read_options={"
              << FormatReadOptionsForDiagnostics(ro) << "}";
    witness.Add(start_oss.str());
    for (size_t scan_idx = 0; scan_idx < scan_opts.GetScanRanges().size();
         ++scan_idx) {
      const ScanOptions& scan_opt = scan_opts.GetScanRanges()[scan_idx];
      std::ostringstream range_oss;
      range_oss << "action=range_input scan_range_index=" << scan_idx
                << " lower={"
                << FormatDiagnosticSlice(scan_opt.range.start.value()) << "}"
                << " upper={"
                << FormatDiagnosticSlice(scan_opt.range.limit.value()) << "}";
      witness.Add(range_oss.str());
    }
  }

  std::string op_logs;
  ro.pin_data = thread->rand.OneIn(2);
  ro.background_purge_on_iterator_cleanup = thread->rand.OneIn(2);
  if (witness.Enabled()) {
    witness.Add("action=read_options_after_randomization read_options={" +
                FormatReadOptionsForDiagnostics(ro) + "}");
  }

  if (options_.prefix_extractor.get() != nullptr) {
    witness.Add("failure=multiscan_with_prefix_extractor");
    witness.Flush("multiscan_with_prefix_extractor");
  }
  assert(options_.prefix_extractor.get() == nullptr);

  std::unique_ptr<Iterator> iter;
  iter.reset(db_->NewIterator(ro, column_families_[rand_column_families[0]]));
  iter->Prepare(scan_opts);
  witness.AddIteratorState("Prepare", "multi_scan", *iter,
                           !ro.allow_unprepared_value);

  constexpr size_t kOpLogsLimit = 50000;

  auto verify_func = [](Iterator* iterator) {
    if (!VerifyWideColumns(iterator->value(), iterator->columns())) {
      fprintf(stderr,
              "Value and columns inconsistent for iterator: value: %s, "
              "columns: %s\n",
              iterator->value().ToString(/* hex */ true).c_str(),
              WideColumnsToHex(iterator->columns()).c_str());
      return false;
    }
    return true;
  };

  // Sometimes stop before draining all prefetched blocks, matching applications
  // that stop after a bounded number of results. In one mode the whole prepared
  // MultiScan is abandoned. In the other mode only the current range is
  // stopped, so the next range's Seek() exercises releasing skipped blocks on
  // the same prepared iterator.
  const bool early_exit = thread->rand.OneIn(2);
  const bool abandon_scan_after_early_exit =
      early_exit && thread->rand.OneIn(2);
  bool abandon_prepared_scan = false;

  const std::vector<ScanOptions>& scan_ranges = scan_opts.GetScanRanges();
  for (size_t scan_idx = 0; scan_idx < scan_ranges.size(); ++scan_idx) {
    const ScanOptions& scan_opt = scan_ranges[scan_idx];
    if (op_logs.size() > kOpLogsLimit) {
      // Shouldn't take too much memory for the history log. Clear it.
      op_logs = "(cleared...)\n";
    }

    // Set up an iterator, perform the same operations without bounds and with
    // total order seek, and compare the results. This is to identify bugs
    // related to bounds, prefix extractor, or reseeking. Sometimes we are
    // comparing iterators with the same set-up, and it doesn't hurt to check
    // them to be equal.
    //
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions cmp_ro;
    cmp_ro.timestamp = ro.timestamp;
    cmp_ro.iter_start_ts = ro.iter_start_ts;
    cmp_ro.snapshot = snapshot_guard.snapshot();
    cmp_ro.auto_refresh_iterator_with_snapshot =
        ro.auto_refresh_iterator_with_snapshot;
    cmp_ro.total_order_seek = true;

    ColumnFamilyHandle* const cmp_cfh =
        GetControlCfh(thread, rand_column_families[0]);
    assert(cmp_cfh);

    std::unique_ptr<Iterator> cmp_iter(db_->NewIterator(cmp_ro, cmp_cfh));

    bool diverged = false;
    bool divergence_witness_flushed = false;
    bool iter_value_prepared = !ro.allow_unprepared_value;
    auto flush_divergence_witness = [&]() {
      if (divergence_witness_flushed) {
        return;
      }
      witness.Add("failure=multiscan_diverged scan_range_index=" +
                  std::to_string(scan_idx) + " op_logs={" +
                  FormatDiagnosticSlice(Slice(op_logs)) + "}");
      witness.AddIteratorState("Divergence", "multi_scan", *iter,
                               iter_value_prepared);
      witness.AddIteratorState("Divergence", "control", *cmp_iter, true);
      witness.Flush("multiscan_diverged");
      divergence_witness_flushed = true;
    };

    assert(scan_opt.range.start);
    assert(scan_opt.range.limit);
    lb = scan_opt.range.start.value();
    ub = scan_opt.range.limit.value();
    Slice key = reverse_multiscan ? ub : lb;

    LastIterateOp last_op;
    if (reverse_multiscan) {
      iter->SeekForPrev(key);
      cmp_iter->SeekForPrev(key);
      while (cmp_iter->Valid() && options_.comparator->CompareWithoutTimestamp(
                                      cmp_iter->key(), /*a_has_ts=*/false, ub,
                                      /*b_has_ts=*/false) >= 0) {
        cmp_iter->Prev();
      }
      last_op = kLastOpSeekForPrev;
      op_logs += "SFP " + key.ToString(true) + " ";
    } else {
      iter->Seek(key);
      cmp_iter->Seek(key);
      last_op = kLastOpSeek;
      op_logs += "S " + key.ToString(true) + " ";
    }
    iter_value_prepared = !ro.allow_unprepared_value;
    if (witness.Enabled()) {
      witness.Add(
          "action=range_seek scan_range_index=" + std::to_string(scan_idx) +
          " lower={" + FormatDiagnosticSlice(lb) + "} upper={" +
          FormatDiagnosticSlice(ub) + "} seek_key={" +
          FormatDiagnosticSlice(key) + "}");
    }
    witness.AddIteratorState("Seek", "multi_scan", *iter,
                             !ro.allow_unprepared_value);
    witness.AddIteratorState("Seek", "control", *cmp_iter, true);

    if (iter->Valid() && ro.allow_unprepared_value) {
      op_logs += "*";
      witness.AddIteratorState("PrepareValueBefore", "multi_scan", *iter,
                               false);

      if (!iter->PrepareValue()) {
        assert(!iter->Valid());
        assert(!iter->status().ok());
        witness.AddIteratorState("PrepareValueFailed", "multi_scan", *iter,
                                 false);
      } else {
        iter_value_prepared = true;
        witness.AddIteratorState("PrepareValue", "multi_scan", *iter, true);
      }
    }

    if (!iter->status().ok() && IsErrorInjectedAndRetryable(iter->status())) {
      return iter->status();
    } else if (!cmp_iter->status().ok() &&
               IsErrorInjectedAndRetryable(cmp_iter->status())) {
      return cmp_iter->status();
    }

    auto verify_reverse_multiscan = [&]() {
      if (diverged || ro.iter_start_ts != nullptr) {
        return;
      }

      if (!iter->status().ok()) {
        witness.Add("failure=reverse_multiscan_status status=" +
                    SanitizeDiagnosticValue(iter->status().ToString()));
        witness.Flush("reverse_multiscan_status");
        fprintf(stderr, "Reverse MultiScan error: %s\n",
                iter->status().ToString().c_str());
        diverged = true;
        thread->stats.AddErrors(1);
        thread->shared->SetVerificationFailure();
        return;
      }

      const bool cmp_valid =
          cmp_iter->Valid() && options_.comparator->CompareWithoutTimestamp(
                                   cmp_iter->key(), /*a_has_ts=*/false, lb,
                                   /*b_has_ts=*/false) >= 0;
      if (iter->Valid() != cmp_valid ||
          (iter->Valid() && iter->key() != cmp_iter->key())) {
        std::ostringstream failure_oss;
        failure_oss << "failure=reverse_multiscan_divergence scan_range_index="
                    << scan_idx << " op_logs={"
                    << FormatDiagnosticSlice(Slice(op_logs)) << "}";
        witness.Add(failure_oss.str());
        witness.AddIteratorState("Divergence", "multi_scan", *iter,
                                 iter_value_prepared);
        witness.AddIteratorState("Divergence", "control", *cmp_iter, true);
        witness.Flush("reverse_multiscan_divergence");
        fprintf(stderr,
                "Reverse MultiScan diverged from control iterator %s under "
                "range [%s, %s)\n",
                op_logs.c_str(), lb.ToString(true).c_str(),
                ub.ToString(true).c_str());
        if (iter->Valid()) {
          fprintf(stderr, "iterator has key %s\n",
                  iter->key().ToString(true).c_str());
        } else {
          fprintf(stderr, "iterator is not valid with status: %s\n",
                  iter->status().ToString().c_str());
        }
        if (cmp_valid) {
          fprintf(stderr, "control iterator has key %s\n",
                  cmp_iter->key().ToString(true).c_str());
        } else {
          fprintf(stderr, "control iterator is outside range or invalid\n");
        }
        diverged = true;
        thread->stats.AddErrors(1);
        thread->shared->SetVerificationFailure();
        return;
      }

      if (iter->Valid() && !verify_func(iter.get())) {
        witness.Add("failure=reverse_multiscan_value_verification");
        witness.AddIteratorState("ValueVerification", "multi_scan", *iter,
                                 iter_value_prepared);
        witness.Flush("reverse_multiscan_value_verification");
        diverged = true;
        thread->stats.AddErrors(1);
        thread->shared->SetVerificationFailure();
      }
    };

    if (reverse_multiscan) {
      verify_reverse_multiscan();
    } else {
      if (VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(),
                         last_op, key, rand_column_families, op_logs,
                         verify_func, &diverged)) {
        flush_divergence_witness();
      }
    }

    uint64_t range_iterations = 0;
    while (iter->Valid()) {
      if (early_exit && range_iterations >= FLAGS_num_iterations) {
        if (abandon_scan_after_early_exit) {
          op_logs += "E";
          abandon_prepared_scan = true;
        } else {
          op_logs += "R";
        }
        break;
      }

      if (reverse_multiscan) {
        iter->Prev();
        if (!diverged) {
          assert(cmp_iter->Valid());
          cmp_iter->Prev();
        }
        op_logs += "P";
      } else {
        iter->Next();
        if (!diverged) {
          assert(cmp_iter->Valid());
          cmp_iter->Next();
        }
        op_logs += "N";
      }
      ++range_iterations;
      last_op = kLastOpNextOrPrev;
      iter_value_prepared = !ro.allow_unprepared_value;
      witness.AddIteratorState(reverse_multiscan ? "Prev" : "Next",
                               "multi_scan", *iter, !ro.allow_unprepared_value);
      witness.AddIteratorState(reverse_multiscan ? "Prev" : "Next", "control",
                               *cmp_iter, true);

      if (iter->Valid() && ro.allow_unprepared_value) {
        op_logs += "*";
        witness.AddIteratorState("PrepareValueBefore", "multi_scan", *iter,
                                 false);

        if (!iter->PrepareValue()) {
          assert(!iter->Valid());
          assert(!iter->status().ok());
          witness.AddIteratorState("PrepareValueFailed", "multi_scan", *iter,
                                   false);
        } else {
          iter_value_prepared = true;
          witness.AddIteratorState("PrepareValue", "multi_scan", *iter, true);
        }
      }

      if (!iter->status().ok() && IsErrorInjectedAndRetryable(iter->status())) {
        return iter->status();
      } else if (!cmp_iter->status().ok() &&
                 IsErrorInjectedAndRetryable(cmp_iter->status())) {
        return cmp_iter->status();
      }

      if (reverse_multiscan) {
        verify_reverse_multiscan();
      } else {
        if (VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(),
                           last_op, key, rand_column_families, op_logs,
                           verify_func, &diverged)) {
          flush_divergence_witness();
        }
      }

      if (diverged) {
        if (thread->shared->HasVerificationFailedYet()) {
          const std::vector<ScanOptions>& scanoptions =
              scan_opts.GetScanRanges();
          for (const auto& t : scanoptions) {
            fprintf(stdout, "Multiscan options: %s to %s \n",
                    t.range.start.value().ToString(true).c_str(),
                    t.range.limit.value().ToString(true).c_str());
          }
        }
        break;
      }
    }

    thread->stats.AddIterations(1);

    op_logs += "; ";
    if (diverged || abandon_prepared_scan) {
      break;
    }
  }

  return Status::OK();
}

template <typename IterType, typename NewIterFunc, typename VerifyFunc>
Status StressTest::TestIterateImpl(ThreadState* thread,
                                   const ReadOptions& read_opts,
                                   const std::vector<int>& rand_column_families,
                                   const std::vector<int64_t>& rand_keys,
                                   NewIterFunc new_iter_func,
                                   VerifyFunc verify_func) {
  assert(!rand_column_families.empty());
  assert(!rand_keys.empty());

  ManagedSnapshot snapshot_guard(db_);

  ReadOptions ro = read_opts;
  ro.snapshot = snapshot_guard.snapshot();

  std::string read_ts_str;
  Slice read_ts_slice;
  MaybeUseOlderTimestampForRangeScan(thread, read_ts_str, read_ts_slice, ro);

  std::string op_logs;
  ro.pin_data = thread->rand.OneIn(2);
  ro.background_purge_on_iterator_cleanup = thread->rand.OneIn(2);

  bool expect_total_order = false;
  if (thread->rand.OneIn(16)) {
    // When prefix extractor is used, it's useful to cover total order seek.
    ro.total_order_seek = true;
    expect_total_order = true;
  } else if (thread->rand.OneIn(4)) {
    ro.total_order_seek = thread->rand.OneIn(2);
    ro.auto_prefix_mode = true;
    expect_total_order = true;
  } else if (options_.prefix_extractor.get() == nullptr) {
    expect_total_order = true;
  } else if (thread->rand.OneIn(2)) {
    ro.prefix_same_as_start = true;
  }
  // Run SeekToFirst() after Seek() on the same table iterator under
  // prefix_same_as_start, without bounds. DBIter converts SeekToFirst() with a
  // lower bound to Seek(lower_bound). An upper bound inside the first prefix
  // ends the scan before the verifier can tell a truncated scan from a complete
  // one. prefix_hash returns nothing from a prefix SeekToFirst().
  const bool cover_seek_to_first_after_seek =
      ro.prefix_same_as_start && FLAGS_memtablerep != "prefix_hash" &&
      thread->rand.OneIn(8);

  std::string upper_bound_str;
  Slice upper_bound;
  // Prefer no bound with no range query filtering; prefer bound with it
  if (!cover_seek_to_first_after_seek &&
      (FLAGS_use_sqfc_for_range_queries ^ thread->rand.OneIn(16))) {
    // Note: upper_bound can be smaller than the seek key.
    const int64_t rand_upper_key = GenerateOneKey(thread, FLAGS_ops_per_thread);
    upper_bound_str = Key(rand_upper_key);
    upper_bound = Slice(upper_bound_str);
    ro.iterate_upper_bound = &upper_bound;
  }

  std::string lower_bound_str;
  Slice lower_bound;
  if (!cover_seek_to_first_after_seek &&
      (FLAGS_use_sqfc_for_range_queries ^ thread->rand.OneIn(16))) {
    // Note: lower_bound can be greater than the seek key.
    const int64_t rand_lower_key = GenerateOneKey(thread, FLAGS_ops_per_thread);
    lower_bound_str = Key(rand_lower_key);
    lower_bound = Slice(lower_bound_str);
    ro.iterate_lower_bound = &lower_bound;
  }

  std::function<bool(const TableProperties&)> table_filter;
  if (FLAGS_use_sqfc_for_range_queries && ro.iterate_upper_bound &&
      ro.iterate_lower_bound) {
    table_filter = sqfc_factory_->GetTableFilterForRangeQuery(
        *ro.iterate_lower_bound, *ro.iterate_upper_bound);
    ro.table_filter = &table_filter;
  }

  std::unique_ptr<IterType> iter = new_iter_func(ro);

  std::vector<std::string> key_strs;
  if (thread->rand.OneIn(16)) {
    // Generate keys close to lower or upper bound of SST files.
    key_strs =
        GetWhiteBoxKeys(thread, db_, column_families_[rand_column_families[0]],
                        rand_keys.size());
  }
  if (key_strs.empty()) {
    // Use the random keys passed in.
    for (int64_t rkey : rand_keys) {
      key_strs.push_back(Key(rkey));
    }
  }

  constexpr size_t kOpLogsLimit = 10000;

  for (const std::string& key_str : key_strs) {
    if (op_logs.size() > kOpLogsLimit) {
      // Shouldn't take too much memory for the history log. Clear it.
      op_logs = "(cleared...)\n";
    }

    if (!FLAGS_use_sqfc_for_range_queries &&
        ro.iterate_upper_bound != nullptr && thread->rand.OneIn(2)) {
      // With a 1/2 chance, change the upper bound.
      // Not compatible with sqfc range filter.
      // It is possible that it is changed before first use, but there is no
      // problem with that.
      const int64_t rand_upper_key =
          GenerateOneKey(thread, FLAGS_ops_per_thread);
      upper_bound_str = Key(rand_upper_key);
      upper_bound = Slice(upper_bound_str);
    }
    if (!FLAGS_use_sqfc_for_range_queries &&
        ro.iterate_lower_bound != nullptr && thread->rand.OneIn(4)) {
      // With a 1/4 chance, change the lower bound.
      // Not compatible with sqfc range filter.
      // It is possible that it is changed before first use, but there is no
      // problem with that.
      const int64_t rand_lower_key =
          GenerateOneKey(thread, FLAGS_ops_per_thread);
      lower_bound_str = Key(rand_lower_key);
      lower_bound = Slice(lower_bound_str);
    }

    // Set up an iterator, perform the same operations without bounds and with
    // total order seek, and compare the results. This is to identify bugs
    // related to bounds, prefix extractor, or reseeking. Sometimes we are
    // comparing iterators with the same set-up, and it doesn't hurt to check
    // them to be equal.
    //
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions cmp_ro;
    cmp_ro.timestamp = ro.timestamp;
    cmp_ro.iter_start_ts = ro.iter_start_ts;
    cmp_ro.snapshot = snapshot_guard.snapshot();
    cmp_ro.auto_refresh_iterator_with_snapshot =
        ro.auto_refresh_iterator_with_snapshot;
    cmp_ro.total_order_seek = true;

    ColumnFamilyHandle* const cmp_cfh =
        GetControlCfh(thread, rand_column_families[0]);
    assert(cmp_cfh);

    std::unique_ptr<Iterator> cmp_iter(db_->NewIterator(cmp_ro, cmp_cfh));

    bool diverged = false;

    Slice key(key_str);

    // DBIter converts SeekToFirst() with a lower bound to Seek(lower_bound),
    // which VerifyIterator() does not handle. SeekToLast() is only verified
    // under total order.
    const bool support_seek_to_first =
        (expect_total_order && FLAGS_test_backward_scan) ||
        (ro.prefix_same_as_start && ro.iterate_lower_bound == nullptr &&
         FLAGS_memtablerep != "prefix_hash");
    const bool support_seek_to_last =
        expect_total_order && FLAGS_test_backward_scan;
    const bool support_seek_for_prev = FLAGS_test_backward_scan;

    // Write-prepared and Write-unprepared and multi-cf-iterator do not support
    // Refresh() yet.
    if (!(FLAGS_use_txn && FLAGS_txn_write_policy != 0 /* write committed */) &&
        !FLAGS_use_multi_cf_iterator && thread->rand.OneIn(4)) {
      Status s = iter->Refresh(snapshot_guard.snapshot());
      if (!s.ok() && IsErrorInjectedAndRetryable(s)) {
        return s;
      }
      DB_STRESS_ASSERT_OK(s);
      op_logs += "Refresh ";
    }

    LastIterateOp last_op;
    // Under prefix_same_as_start a SeekToFirst() scan covers the prefix of the
    // first key.
    std::string verify_key_str;
    Slice verify_key = key;
    if (support_seek_to_first &&
        thread->rand.OneIn(cover_seek_to_first_after_seek ? 4 : 100)) {
      if (cover_seek_to_first_after_seek) {
        // If this Seek() read the first data block, the block would be cached
        // and SeekToFirst() would not read the file.
        const std::string pre_seek_key =
            Key(GenerateOneKey(thread, FLAGS_ops_per_thread));
        iter->Seek(pre_seek_key);
        cmp_iter->Seek(pre_seek_key);
        op_logs += "PreS " + Slice(pre_seek_key).ToString(true) + " ";
        thread->stats.AddSeekToFirstAfterSeek(1);
      }
      iter->SeekToFirst();
      cmp_iter->SeekToFirst();
      last_op = kLastOpSeekToFirst;
      op_logs += "STF ";
      if (ro.prefix_same_as_start && cmp_iter->Valid()) {
        verify_key_str = cmp_iter->key().ToString();
        verify_key = verify_key_str;
      }
    } else if (support_seek_to_last && thread->rand.OneIn(100)) {
      iter->SeekToLast();
      cmp_iter->SeekToLast();
      last_op = kLastOpSeekToLast;
      op_logs += "STL ";
    } else if (support_seek_for_prev && thread->rand.OneIn(8)) {
      iter->SeekForPrev(key);
      cmp_iter->SeekForPrev(key);
      last_op = kLastOpSeekForPrev;
      op_logs += "SFP " + key.ToString(true) + " ";
    } else {
      iter->Seek(key);
      cmp_iter->Seek(key);
      last_op = kLastOpSeek;
      op_logs += "S " + key.ToString(true) + " ";
    }

    if (iter->Valid() && ro.allow_unprepared_value) {
      op_logs += "*";

      if (!iter->PrepareValue()) {
        assert(!iter->Valid());
        assert(!iter->status().ok());
      }
    }

    if (!iter->status().ok() && IsErrorInjectedAndRetryable(iter->status())) {
      return iter->status();
    } else if (!cmp_iter->status().ok() &&
               IsErrorInjectedAndRetryable(cmp_iter->status())) {
      return cmp_iter->status();
    }

    VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(), last_op,
                   verify_key, rand_column_families, op_logs, verify_func,
                   &diverged);

    // Prev() resets the table iterator's readahead lookup state.
    const bool no_reverse =
        (FLAGS_memtablerep == "prefix_hash" && !expect_total_order) ||
        !FLAGS_test_backward_scan ||
        (cover_seek_to_first_after_seek && last_op == kLastOpSeekToFirst);
    for (uint64_t i = 0; i < FLAGS_num_iterations && iter->Valid(); ++i) {
      if (no_reverse || thread->rand.OneIn(2)) {
        iter->Next();
        if (!diverged) {
          assert(cmp_iter->Valid());
          cmp_iter->Next();
        }
        op_logs += "N";
      } else {
        iter->Prev();
        if (!diverged) {
          assert(cmp_iter->Valid());
          cmp_iter->Prev();
        }
        op_logs += "P";
      }

      last_op = kLastOpNextOrPrev;

      if (iter->Valid() && ro.allow_unprepared_value) {
        op_logs += "*";

        if (!iter->PrepareValue()) {
          assert(!iter->Valid());
          assert(!iter->status().ok());
        }
      }

      if (!iter->status().ok() && IsErrorInjectedAndRetryable(iter->status())) {
        return iter->status();
      } else if (!cmp_iter->status().ok() &&
                 IsErrorInjectedAndRetryable(cmp_iter->status())) {
        return cmp_iter->status();
      }

      VerifyIterator(thread, cmp_cfh, ro, iter.get(), cmp_iter.get(), last_op,
                     verify_key, rand_column_families, op_logs, verify_func,
                     &diverged);
    }

    thread->stats.AddIterations(1);

    op_logs += "; ";
  }

  return Status::OK();
}

Status StressTest::TestGetLiveFiles() const {
  std::vector<std::string> live_file;
  uint64_t manifest_size = 0;
  return db_->GetLiveFiles(live_file, &manifest_size);
}

Status StressTest::TestGetLiveFilesMetaData() const {
  std::vector<LiveFileMetaData> live_file_metadata;
  db_->GetLiveFilesMetaData(&live_file_metadata);
  return Status::OK();
}

Status StressTest::TestGetLiveFilesStorageInfo() const {
  std::vector<LiveFileStorageInfo> live_file_storage_info;
  LiveFilesStorageInfoOptions live_opts;
  live_opts.atomic_flush = FLAGS_checkpoint_atomic_flush;
  return db_->GetLiveFilesStorageInfo(live_opts, &live_file_storage_info);
}

Status StressTest::TestGetAllColumnFamilyMetaData() const {
  std::vector<ColumnFamilyMetaData> all_cf_metadata;
  db_->GetAllColumnFamilyMetaData(&all_cf_metadata);
  return Status::OK();
}

Status StressTest::TestGetSortedWalFiles() const {
  VectorWalPtr log_ptr;
  return db_->GetSortedWalFiles(log_ptr);
}

Status StressTest::TestGetCurrentWalFile() const {
  std::unique_ptr<WalFile> cur_wal_file;
  return db_->GetCurrentWalFile(&cur_wal_file);
}

void StressTest::DumpIteratorDivergenceDiagnostics(
    ColumnFamilyHandle* cmp_cfh, const ReadOptions& ro, const Slice& verify_key,
    const std::vector<int>& rand_column_families) const {
  fprintf(stderr,
          "Iterator divergence diagnostics: verify_key=%s, cmp_cf=%s, "
          "prefix_extractor=%d, using_udi=%d, use_multi_cf_iterator=%d, "
          "selected_cf_count=%zu\n",
          verify_key.ToString(/*hex=*/true).c_str(), cmp_cfh->GetName().c_str(),
          static_cast<int>(options_.prefix_extractor != nullptr),
          static_cast<int>(ro.read_index != ReadOptions::ReadIndex::kDefault),
          static_cast<int>(FLAGS_use_multi_cf_iterator),
          rand_column_families.size());

  auto make_debug_iter =
      [&](const ReadOptions& debug_ro,
          bool use_multi_cf_iter) -> std::unique_ptr<Iterator> {
    if (use_multi_cf_iter) {
      std::vector<ColumnFamilyHandle*> cfhs;
      cfhs.reserve(rand_column_families.size());
      for (int cf_index : rand_column_families) {
        cfhs.emplace_back(column_families_[cf_index]);
      }
      return db_->NewCoalescingIterator(debug_ro, cfhs);
    }
    return std::unique_ptr<Iterator>(db_->NewIterator(debug_ro, cmp_cfh));
  };

  auto dump_debug_iter = [&](const char* label, const ReadOptions& debug_ro,
                             bool use_multi_cf_iter) {
    auto debug_iter = make_debug_iter(debug_ro, use_multi_cf_iter);
    debug_iter->Seek(verify_key);

    std::string sv_number;
    const Status prop_s = debug_iter->GetProperty(
        "rocksdb.iterator.super-version-number", &sv_number);
    fprintf(stderr,
            "%s before_prepare: valid=%d, status=%s, sv=%s, key=%s, "
            "value_size=%zu\n",
            label, static_cast<int>(debug_iter->Valid()),
            debug_iter->status().ToString().c_str(),
            prop_s.ok() ? sv_number.c_str() : prop_s.ToString().c_str(),
            debug_iter->Valid() ? debug_iter->key().ToString(true).c_str()
                                : "(invalid)",
            debug_iter->Valid() ? debug_iter->value().size() : 0U);

    if (!debug_iter->Valid()) {
      return;
    }

    bool prepared = true;
    if (debug_ro.allow_unprepared_value) {
      prepared = debug_iter->PrepareValue();
    }

    fprintf(stderr,
            "%s after_prepare: prepared=%d, valid=%d, status=%s, key=%s, "
            "value_size=%zu, columns=%zu, wide_columns_ok=%d\n",
            label, static_cast<int>(prepared),
            static_cast<int>(debug_iter->Valid()),
            debug_iter->status().ToString().c_str(),
            debug_iter->Valid() ? debug_iter->key().ToString(true).c_str()
                                : "(invalid)",
            debug_iter->Valid() ? debug_iter->value().size() : 0U,
            debug_iter->Valid() ? debug_iter->columns().size() : 0U,
            static_cast<int>(
                debug_iter->Valid() &&
                VerifyWideColumns(debug_iter->value(), debug_iter->columns())));
  };

  ReadOptions standard_ro = ro;
  standard_ro.read_index = ReadOptions::ReadIndex::kBuiltin;
  dump_debug_iter("Debug standard direct", standard_ro,
                  /*use_multi_cf_iter=*/false);

  if (udi_factory_) {
    ReadOptions trie_ro = ro;
    trie_ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
    dump_debug_iter("Debug trie direct", trie_ro,
                    /*use_multi_cf_iter=*/false);
  }

  if (FLAGS_use_multi_cf_iterator) {
    dump_debug_iter("Debug standard coalescing", standard_ro,
                    /*use_multi_cf_iter=*/true);
    if (udi_factory_) {
      ReadOptions trie_ro = ro;
      trie_ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
      dump_debug_iter("Debug trie coalescing", trie_ro,
                      /*use_multi_cf_iter=*/true);
    }
  }
}

// Compare the two iterator, iter and cmp_iter are in the same position,
// unless iter might be made invalidate or undefined because of
// upper or lower bounds, or prefix extractor.
// Will flag failure if the verification fails.
// diverged = true if the two iterator is already diverged.
// Returns true only when this call reports a verification failure. A false
// return with diverged = true means verification was intentionally skipped.
template <typename IterType, typename VerifyFuncType>
bool StressTest::VerifyIterator(
    ThreadState* thread, ColumnFamilyHandle* cmp_cfh, const ReadOptions& ro,
    IterType* iter, Iterator* cmp_iter, LastIterateOp op,
    const Slice& verify_key, const std::vector<int>& rand_column_families,
    const std::string& op_logs, VerifyFuncType verify_func, bool* diverged) {
  assert(diverged);

  if (*diverged) {
    return false;
  }

  if (ro.iter_start_ts != nullptr) {
    assert(FLAGS_user_timestamp_size > 0);
    // We currently do not verify iterator when dumping history of internal
    // keys.
    *diverged = true;
    return false;
  }

  if (op == kLastOpSeekToFirst && ro.iterate_lower_bound != nullptr) {
    // SeekToFirst() with lower bound is not well-defined.
    *diverged = true;
    return false;
  } else if (op == kLastOpSeekToLast && ro.iterate_upper_bound != nullptr) {
    // SeekToLast() with higher bound is not well-defined.
    *diverged = true;
    return false;
  } else if (op == kLastOpSeek && ro.iterate_lower_bound != nullptr &&
             (options_.comparator->CompareWithoutTimestamp(
                  *ro.iterate_lower_bound, /*a_has_ts=*/false, verify_key,
                  /*b_has_ts=*/false) >= 0 ||
              (ro.iterate_upper_bound != nullptr &&
               options_.comparator->CompareWithoutTimestamp(
                   *ro.iterate_lower_bound, /*a_has_ts=*/false,
                   *ro.iterate_upper_bound, /*b_has_ts*/ false) >= 0))) {
    // Lower bound behavior is not well-defined if it is larger than
    // seek key or upper bound. Disable the check for now.
    *diverged = true;
    return false;
  } else if (op == kLastOpSeekForPrev && ro.iterate_upper_bound != nullptr &&
             (options_.comparator->CompareWithoutTimestamp(
                  *ro.iterate_upper_bound, /*a_has_ts=*/false, verify_key,
                  /*b_has_ts=*/false) <= 0 ||
              (ro.iterate_lower_bound != nullptr &&
               options_.comparator->CompareWithoutTimestamp(
                   *ro.iterate_lower_bound, /*a_has_ts=*/false,
                   *ro.iterate_upper_bound, /*b_has_ts=*/false) >= 0))) {
    // Upper bound behavior is not well-defined if it is smaller than
    // seek key or lower bound. Disable the check for now.
    *diverged = true;
    return false;
  }

  if (!ro.total_order_seek && options_.prefix_extractor != nullptr &&
      ro.iterate_lower_bound != nullptr) {
    const SliceTransform* prefix_extractor = options_.prefix_extractor.get();
    if (!prefix_extractor->InDomain(verify_key) ||
        !prefix_extractor->InDomain(*ro.iterate_lower_bound) ||
        prefix_extractor->Transform(verify_key) !=
            prefix_extractor->Transform(*ro.iterate_lower_bound)) {
      // ReadOptions requires the seek target and iterate_lower_bound to share
      // a prefix when prefix iteration is enabled. Skip verification for this
      // undefined configuration.
      *diverged = true;
      return false;
    }
  }

  const SliceTransform* pe = (ro.total_order_seek || ro.auto_prefix_mode)
                                 ? nullptr
                                 : options_.prefix_extractor.get();
  const Comparator* cmp = options_.comparator;
  std::ostringstream read_opt_oss;
  read_opt_oss << "pin_data: " << ro.pin_data
               << ", background_purge_on_iterator_cleanup: "
               << ro.background_purge_on_iterator_cleanup
               << ", total_order_seek: " << ro.total_order_seek
               << ", auto_prefix_mode: " << ro.auto_prefix_mode
               << ", prefix_same_as_start: " << ro.prefix_same_as_start
               << ", iterate_upper_bound: "
               << (ro.iterate_upper_bound
                       ? ro.iterate_upper_bound->ToString(true).c_str()
                       : "")
               << ", iterate_lower_bound: "
               << (ro.iterate_lower_bound
                       ? ro.iterate_lower_bound->ToString(true).c_str()
                       : "")
               << ", allow_unprepared_value: " << ro.allow_unprepared_value
               << ", auto_refresh_iterator_with_snapshot: "
               << ro.auto_refresh_iterator_with_snapshot
               << ", snapshot: " << (ro.snapshot ? "non-nullptr" : "nullptr")
               << ", timestamp: "
               << (ro.timestamp ? ro.timestamp->ToString(true).c_str() : "")
               << ", iter_start_ts: "
               << (ro.iter_start_ts ? ro.iter_start_ts->ToString(true).c_str()
                                    : "");

  if (iter->Valid() && !cmp_iter->Valid()) {
    if (pe != nullptr) {
      if (!pe->InDomain(verify_key)) {
        // Prefix seek a non-in-domain key is undefined. Skip checking for
        // this scenario.
        *diverged = true;
        return false;
      } else if (!pe->InDomain(iter->key())) {
        // out of range is iterator key is not in domain anymore.
        *diverged = true;
        return false;
      } else if (pe->Transform(iter->key()) != pe->Transform(verify_key)) {
        *diverged = true;
        return false;
      }
    }
    fprintf(stderr,
            "Control iterator is invalid but iterator has key %s "
            "%s under specified iterator ReadOptions: %s (Empty string or "
            "missing field indicates default option or value is used)\n",
            iter->key().ToString(true).c_str(), op_logs.c_str(),
            read_opt_oss.str().c_str());

    *diverged = true;
  } else if (cmp_iter->Valid()) {
    // Iterator is not valid. It can be legitimate if it has already been
    // out of upper or lower bound, or filtered out by prefix iterator.
    const Slice& total_order_key = cmp_iter->key();

    if (pe != nullptr) {
      if (!pe->InDomain(verify_key)) {
        // Prefix seek a non-in-domain key is undefined. Skip checking for
        // this scenario.
        *diverged = true;
        return false;
      }

      if (!pe->InDomain(total_order_key) ||
          pe->Transform(total_order_key) != pe->Transform(verify_key)) {
        // If the prefix is exhausted, the only thing needs to check
        // is the iterator isn't return a position in prefix.
        // Either way, checking can stop from here.
        *diverged = true;
        if (!iter->Valid() || !pe->InDomain(iter->key()) ||
            pe->Transform(iter->key()) != pe->Transform(verify_key)) {
          return false;
        }
        fprintf(stderr,
                "Iterator stays in prefix but control doesn't"
                " iterator key %s control iterator key %s %s under specified "
                "iterator ReadOptions: %s (Empty string or "
                "missing field indicates default option or value is used)\n",
                iter->key().ToString(true).c_str(),
                cmp_iter->key().ToString(true).c_str(), op_logs.c_str(),
                read_opt_oss.str().c_str());
      }
    }
    // Check upper or lower bounds.
    if (!*diverged) {
      if ((iter->Valid() && iter->key() != cmp_iter->key()) ||
          (!iter->Valid() &&
           (ro.iterate_upper_bound == nullptr ||
            cmp->CompareWithoutTimestamp(total_order_key, /*a_has_ts=*/false,
                                         *ro.iterate_upper_bound,
                                         /*b_has_ts=*/false) < 0) &&
           (ro.iterate_lower_bound == nullptr ||
            cmp->CompareWithoutTimestamp(total_order_key, /*a_has_ts=*/false,
                                         *ro.iterate_lower_bound,
                                         /*b_has_ts=*/false) > 0))) {
        fprintf(stderr,
                "Iterator diverged from control iterator which"
                " has value %s %s  under specified iterator ReadOptions: %s "
                "(Empty string or "
                "missing field indicates default option or value is used)\n",
                total_order_key.ToString(true).c_str(), op_logs.c_str(),
                read_opt_oss.str().c_str());
        if (iter->Valid()) {
          fprintf(stderr, "iterator has value %s\n",
                  iter->key().ToString(true).c_str());
        } else {
          fprintf(stderr, "iterator is not valid with status: %s\n",
                  iter->status().ToString().c_str());
        }
        *diverged = true;
      }
    }
  }

  if (!*diverged && iter->Valid()) {
    if (!verify_func(iter)) {
      *diverged = true;
    }
  }

  if (*diverged) {
    DumpIteratorDivergenceDiagnostics(cmp_cfh, ro, verify_key,
                                      rand_column_families);
    fprintf(stderr, "VerifyIterator failed. Control CF %s\n",
            cmp_cfh->GetName().c_str());
    thread->stats.AddErrors(1);
    // Fail fast to preserve the DB state.
    thread->shared->SetVerificationFailure();
    return true;
  }
  return false;
}

Status StressTest::TestBackupRestore(
    ThreadState* thread, const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  std::vector<std::unique_ptr<MutexLock>> locks;
  if (ShouldAcquireMutexOnKey()) {
    for (int rand_column_family : rand_column_families) {
      // `rand_keys[0]` on each chosen CF will be verified.
      locks.emplace_back(new MutexLock(
          thread->shared->GetMutexForKey(rand_column_family, rand_keys[0])));
    }
  }

  const std::string backup_dir =
      GetDbPath() + "/.backup" + std::to_string(thread->tid);
  const std::string restore_dir =
      GetDbPath() + "/.restore" + std::to_string(thread->tid);
  BackupEngineOptions backup_opts(backup_dir);
  // For debugging, get info_log from live options
  backup_opts.info_log = db_->GetDBOptions().info_log.get();
  if (thread->rand.OneIn(10)) {
    backup_opts.share_table_files = false;
  } else {
    backup_opts.share_table_files = true;
    if (thread->rand.OneIn(5)) {
      backup_opts.share_files_with_checksum = false;
    } else {
      backup_opts.share_files_with_checksum = true;
      if (thread->rand.OneIn(2)) {
        // old
        backup_opts.share_files_with_checksum_naming =
            BackupEngineOptions::kLegacyCrc32cAndFileSize;
      } else {
        // new
        backup_opts.share_files_with_checksum_naming =
            BackupEngineOptions::kUseDbSessionId;
      }
      if (thread->rand.OneIn(2)) {
        backup_opts.share_files_with_checksum_naming =
            backup_opts.share_files_with_checksum_naming |
            BackupEngineOptions::kFlagIncludeFileSize;
      }
    }
  }
  if (thread->rand.OneIn(2)) {
    backup_opts.schema_version = 1;
  } else {
    backup_opts.schema_version = 2;
  }
  if (thread->rand.OneIn(3)) {
    backup_opts.max_background_operations = 16;
  } else {
    backup_opts.max_background_operations = 1;
  }
  if (thread->rand.OneIn(2)) {
    backup_opts.backup_rate_limiter.reset(NewGenericRateLimiter(
        FLAGS_backup_max_size * 1000000 /* rate_bytes_per_sec */,
        1 /* refill_period_us */));
  }
  if (thread->rand.OneIn(2)) {
    backup_opts.restore_rate_limiter.reset(NewGenericRateLimiter(
        FLAGS_backup_max_size * 1000000 /* rate_bytes_per_sec */,
        1 /* refill_period_us */));
  }
  backup_opts.current_temperatures_override_manifest = thread->rand.OneIn(2);
  std::ostringstream backup_opt_oss;
  backup_opt_oss << "share_table_files: " << backup_opts.share_table_files
                 << ", share_files_with_checksum: "
                 << backup_opts.share_files_with_checksum
                 << ", share_files_with_checksum_naming: "
                 << backup_opts.share_files_with_checksum_naming
                 << ", schema_version: " << backup_opts.schema_version
                 << ", max_background_operations: "
                 << backup_opts.max_background_operations
                 << ", backup_rate_limiter: "
                 << backup_opts.backup_rate_limiter.get()
                 << ", restore_rate_limiter: "
                 << backup_opts.restore_rate_limiter.get()
                 << ", current_temperatures_override_manifest: "
                 << backup_opts.current_temperatures_override_manifest;

  std::ostringstream create_backup_opt_oss;
  std::ostringstream restore_opts_oss;
  BackupEngine* backup_engine = nullptr;
  std::string from = "a backup/restore operation";
  Status s = BackupEngine::Open(GetDbEnv(), backup_opts, &backup_engine);
  if (!s.ok()) {
    from = "BackupEngine::Open";
  }

  if (s.ok() && FLAGS_manual_wal_flush_one_in > 0) {
    // To avoid missing buffered WAL data during backup and cause false-positive
    // inconsistent values between original DB and restored DB
    s = db_->FlushWAL(/*sync=*/false);
    if (!s.ok()) {
      from = "FlushWAL";
    }
  }

  if (s.ok()) {
    if (backup_opts.schema_version >= 2 && thread->rand.OneIn(2)) {
      TEST_BackupMetaSchemaOptions test_opts;
      test_opts.crc32c_checksums = thread->rand.OneIn(2) == 0;
      test_opts.file_sizes = thread->rand.OneIn(2) == 0;
      TEST_SetBackupMetaSchemaOptions(backup_engine, test_opts);
    }
    CreateBackupOptions create_opts;
    if (FLAGS_disable_wal) {
      // The verification can only work when latest value of `key` is backed up,
      // which requires flushing in case of WAL disabled.
      //
      // Note this triggers a flush with a key lock held. Meanwhile, operations
      // like flush/compaction may attempt to grab key locks like in
      // `DbStressCompactionFilter`. The philosophy around preventing deadlock
      // is the background operation key lock acquisition only tries but does
      // not wait for the lock. So here in the foreground it is OK to hold the
      // lock and wait on a background operation (flush).
      create_opts.flush_before_backup = true;
    }
    if (FLAGS_atomic_flush) {
      // When atomic flush is enabled for the DB, use it for backup too.
      // This ensures cross-CF consistency without needing WAL files.
      // flush_before_backup must be true for atomic_flush to take effect.
      create_opts.flush_before_backup = true;
      create_opts.atomic_flush = true;
    }
    create_opts.decrease_background_thread_cpu_priority = thread->rand.OneIn(2);
    create_opts.background_thread_cpu_priority = static_cast<CpuPriority>(
        thread->rand.Next() % (static_cast<int>(CpuPriority::kHigh) + 1));
    create_backup_opt_oss << "flush_before_backup: "
                          << create_opts.flush_before_backup
                          << ", atomic_flush: " << create_opts.atomic_flush
                          << ", decrease_background_thread_cpu_priority: "
                          << create_opts.decrease_background_thread_cpu_priority
                          << ", background_thread_cpu_priority: "
                          << static_cast<int>(
                                 create_opts.background_thread_cpu_priority);
    s = backup_engine->CreateNewBackup(create_opts, db_);
    if (!s.ok()) {
      from = "BackupEngine::CreateNewBackup";
    }
  }
  if (s.ok()) {
    delete backup_engine;
    backup_engine = nullptr;
    s = BackupEngine::Open(GetDbEnv(), backup_opts, &backup_engine);
    if (!s.ok()) {
      from = "BackupEngine::Open (again)";
    }
  }
  std::vector<BackupInfo> backup_info;
  // If inplace_not_restore, we verify the backup by opening it as a
  // read-only DB. If !inplace_not_restore, we restore it to a temporary
  // directory for verification.
  bool inplace_not_restore = thread->rand.OneIn(3);
  if (s.ok()) {
    backup_engine->GetBackupInfo(&backup_info,
                                 /*include_file_details*/ inplace_not_restore);
    if (backup_info.empty()) {
      s = Status::NotFound("no backups found");
      from = "BackupEngine::GetBackupInfo";
    }
  }
  if (s.ok() && thread->rand.OneIn(2)) {
    s = backup_engine->VerifyBackup(
        backup_info.front().backup_id,
        thread->rand.OneIn(2) /* verify_with_checksum */);
    if (!s.ok()) {
      from = "BackupEngine::VerifyBackup";
    }
  }
  const bool allow_persistent = thread->tid == 0;  // not too many
  bool from_latest = false;
  int count = static_cast<int>(backup_info.size());
  RestoreOptions restore_options;
  restore_options.keep_log_files = thread->rand.OneIn(2);
  restore_opts_oss << "keep_log_files: " << restore_options.keep_log_files;
  if (s.ok() && !inplace_not_restore) {
    if (count > 1) {
      s = backup_engine->RestoreDBFromBackup(
          restore_options, backup_info[thread->rand.Uniform(count)].backup_id,
          restore_dir /* db_dir */, restore_dir /* wal_dir */);
      if (!s.ok()) {
        from = "BackupEngine::RestoreDBFromBackup";
      }
    } else {
      from_latest = true;
      s = backup_engine->RestoreDBFromLatestBackup(
          restore_options, restore_dir /* db_dir */, restore_dir /* wal_dir */);
      if (!s.ok()) {
        from = "BackupEngine::RestoreDBFromLatestBackup";
      }
    }
  }
  if (s.ok() && !inplace_not_restore) {
    // Purge early if restoring, to ensure the restored directory doesn't
    // have some secret dependency on the backup directory.
    uint32_t to_keep = 0;
    if (allow_persistent) {
      // allow one thread to keep up to 2 backups
      to_keep = thread->rand.Uniform(3);
    }
    s = backup_engine->PurgeOldBackups(to_keep);
    if (!s.ok()) {
      from = "BackupEngine::PurgeOldBackups";
    }
  }
  std::unique_ptr<DB> restored_db;
  std::vector<ColumnFamilyHandle*> restored_cf_handles;

  // Not yet implemented: opening restored BlobDB or TransactionDB
  Options db_opt;
  if (s.ok() && !FLAGS_use_txn && !FLAGS_use_blob_db) {
    s = PrepareOptionsForRestoredDB(&db_opt);
    if (!s.ok()) {
      from = "PrepareRestoredDBOptions in backup/restore";
    }
  }
  if (s.ok() && !FLAGS_use_txn && !FLAGS_use_blob_db) {
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    // TODO(ajkr): `column_family_names_` is not safe to access here when
    // `clear_column_family_one_in != 0`. But we can't easily switch to
    // `ListColumnFamilies` to get names because it won't necessarily give
    // the same order as `column_family_names_`.
    assert(FLAGS_clear_column_family_one_in == 0);
    for (const auto& name : column_family_names_) {
      cf_descriptors.emplace_back(name, ColumnFamilyOptions(db_opt));
    }
    if (inplace_not_restore) {
      BackupInfo& info = backup_info[thread->rand.Uniform(count)];
      db_opt.env = info.env_for_open.get();
      s = DB::OpenForReadOnly(DBOptions(db_opt), info.name_for_open,
                              cf_descriptors, &restored_cf_handles,
                              &restored_db);
      if (!s.ok()) {
        from = "DB::OpenForReadOnly in backup/restore";
      }
    } else {
      s = DB::Open(DBOptions(db_opt), restore_dir, cf_descriptors,
                   &restored_cf_handles, &restored_db);
      if (!s.ok()) {
        from = "DB::Open in backup/restore";
      }
    }
  }
  // Note the column families chosen by `rand_column_families` cannot be
  // dropped while the locks for `rand_keys` are held. So we should not have
  // to worry about accessing those column families throughout this function.
  //
  // For simplicity, currently only verifies existence/non-existence of a
  // single key
  for (size_t i = 0; restored_db && s.ok() && i < rand_column_families.size();
       ++i) {
    std::string key_str = Key(rand_keys[0]);
    Slice key = key_str;
    std::string restored_value;
    // This `ReadOptions` is for validation purposes. Ignore
    // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
    ReadOptions read_opts;
    std::string ts_str;
    Slice ts;
    if (FLAGS_user_timestamp_size > 0) {
      ts_str = GetReadTimestamp();
      ts = ts_str;
      read_opts.timestamp = &ts;
    }
    Status get_status = DbStressGet(
        restored_db.get(), read_opts,
        restored_cf_handles[rand_column_families[i]], key, &restored_value);
    bool exists = thread->shared->Exists(rand_column_families[i], rand_keys[0]);
    if (get_status.ok()) {
      if (!exists && from_latest && ShouldAcquireMutexOnKey()) {
        std::ostringstream oss;
        oss << "0x" << key.ToString(true)
            << " exists in restore but not in original db";
        s = Status::Corruption(oss.str());
      }
    } else if (get_status.IsNotFound()) {
      if (exists && from_latest && ShouldAcquireMutexOnKey()) {
        std::ostringstream oss;
        oss << "0x" << key.ToString(true)
            << " exists in original db but not in restore";
        s = Status::Corruption(oss.str());
      }
    } else {
      s = get_status;
      if (!s.ok()) {
        from = "DB::Get in backup/restore";
      }
    }
  }
  if (restored_db != nullptr) {
    for (auto* cf_handle : restored_cf_handles) {
      restored_db->DestroyColumnFamilyHandle(cf_handle);
    }
    restored_db.reset();
  }
  if (s.ok() && inplace_not_restore) {
    // Purge late if inplace open read-only
    uint32_t to_keep = 0;
    if (allow_persistent) {
      // allow one thread to keep up to 2 backups
      to_keep = thread->rand.Uniform(3);
    }
    s = backup_engine->PurgeOldBackups(to_keep);
    if (!s.ok()) {
      from = "BackupEngine::PurgeOldBackups";
    }
  }
  if (backup_engine != nullptr) {
    delete backup_engine;
    backup_engine = nullptr;
  }

  // Temporarily disable error injection for clean up
  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
  }

  if (s.ok() || IsErrorInjectedAndRetryable(s)) {
    // Preserve directories on failure, or allowed persistent backup
    if (!allow_persistent) {
      s = DestroyDir(GetDbEnv(), backup_dir);
      if (!s.ok()) {
        from = "Destroy backup dir";
      }
    }
  }

  if (s.ok() || IsErrorInjectedAndRetryable(s)) {
    s = DestroyDir(GetDbEnv(), restore_dir);
    if (!s.ok()) {
      from = "Destroy restore dir";
    }
  }

  // Enable back error injection disabled for clean up
  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
  }

  if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
    fprintf(stderr,
            "Failure in %s with: %s under specified BackupEngineOptions: %s, "
            "CreateBackupOptions: %s, RestoreOptions: %s (Empty string or "
            "missing field indicates default option or value is used)\n",
            from.c_str(), s.ToString().c_str(), backup_opt_oss.str().c_str(),
            create_backup_opt_oss.str().c_str(),
            restore_opts_oss.str().c_str());
  }
  return s;
}

void InitializeMergeOperator(Options& options) {
  if (FLAGS_use_full_merge_v1) {
    options.merge_operator = MergeOperators::CreateDeprecatedPutOperator();
  } else {
    if (FLAGS_use_put_entity_one_in > 0) {
      options.merge_operator = std::make_shared<DBStressWideMergeOperator>();
    } else {
      options.merge_operator = MergeOperators::CreatePutOperator();
    }
  }
}

Status StressTest::PrepareOptionsForRestoredDB(Options* options) {
  assert(options);
  // To avoid race with other threads' operations (e.g, SetOptions())
  // on the same pointer sub-option (e.g, `std::shared_ptr<const FilterPolicy>
  // filter_policy`) while having the same settings as `options_`, we create a
  // new Options object from `options_`'s string to deep copy these pointer
  // sub-options
  Status s;
  ConfigOptions config_opts;

  std::string db_options_str;
  s = GetStringFromDBOptions(config_opts, options_, &db_options_str);
  if (!s.ok()) {
    return s;
  }
  DBOptions db_options;
  s = GetDBOptionsFromString(config_opts, Options(), db_options_str,
                             &db_options);
  if (!s.ok()) {
    return s;
  }

  std::string cf_options_str;
  s = GetStringFromColumnFamilyOptions(config_opts, options_, &cf_options_str);
  if (!s.ok()) {
    return s;
  }
  ColumnFamilyOptions cf_options;
  s = GetColumnFamilyOptionsFromString(config_opts, Options(), cf_options_str,
                                       &cf_options);
  if (!s.ok()) {
    return s;
  }

  *options = Options(db_options, cf_options);
  options->best_efforts_recovery = false;
  options->listeners.clear();
  // Avoid dangling/shared file descriptors, for reliable destroy
  options->sst_file_manager = nullptr;
  // GetColumnFamilyOptionsFromString does not create customized merge operator.
  InitializeMergeOperator(*options);
  if (FLAGS_user_timestamp_size > 0) {
    // Check OPTIONS string loading can bootstrap the correct user comparator
    // from object registry.
    assert(options->comparator);
    assert(options->comparator == test::BytewiseComparatorWithU64TsWrapper());
  }

  return Status::OK();
}
Status StressTest::TestApproximateSize(
    ThreadState* thread, uint64_t iteration,
    const std::vector<int>& rand_column_families,
    const std::vector<int64_t>& rand_keys) {
  // rand_keys likely only has one key. Just use the first one.
  assert(!rand_keys.empty());
  assert(!rand_column_families.empty());
  int64_t key1 = rand_keys[0];
  int64_t key2;
  if (thread->rand.OneIn(2)) {
    // Two totally random keys. This tends to cover large ranges.
    key2 = GenerateOneKey(thread, iteration);
    if (key2 < key1) {
      std::swap(key1, key2);
    }
  } else {
    // Unless users pass a very large FLAGS_max_key, it we should not worry
    // about overflow. It is for testing, so we skip the overflow checking
    // for simplicity.
    key2 = key1 + static_cast<int64_t>(thread->rand.Uniform(1000));
  }
  std::string key1_str = Key(key1);
  std::string key2_str = Key(key2);
  Range range{Slice(key1_str), Slice(key2_str)};
  if (thread->rand.OneIn(3)) {
    // Call GetApproximateMemTableStats instead
    uint64_t count;
    uint64_t size;
    db_->GetApproximateMemTableStats(column_families_[rand_column_families[0]],
                                     range, &count, &size);
    return Status::OK();
  } else {
    // Call GetApproximateSizes
    SizeApproximationOptions sao;
    sao.include_memtables = thread->rand.OneIn(2);
    sao.include_blob_files = thread->rand.OneIn(2);
    if (sao.include_memtables || sao.include_blob_files) {
      sao.include_files = thread->rand.OneIn(2);
    }
    if (thread->rand.OneIn(2)) {
      if (thread->rand.OneIn(2)) {
        sao.files_size_error_margin = 0.0;
      } else {
        sao.files_size_error_margin =
            static_cast<double>(thread->rand.Uniform(3));
      }
    }
    uint64_t result;
    return db_->GetApproximateSizes(
        sao, column_families_[rand_column_families[0]], &range, 1, &result);
  }
}

Status StressTest::TestCheckpoint(ThreadState* thread,
                                  const std::vector<int>& rand_column_families,
                                  const std::vector<int64_t>& rand_keys) {
  std::vector<std::unique_ptr<MutexLock>> locks;
  if (ShouldAcquireMutexOnKey()) {
    for (int rand_column_family : rand_column_families) {
      // `rand_keys[0]` on each chosen CF will be verified.
      locks.emplace_back(new MutexLock(
          thread->shared->GetMutexForKey(rand_column_family, rand_keys[0])));
    }
  }

  std::string checkpoint_dir =
      GetDbPath() + "/.checkpoint" + std::to_string(thread->tid);
  Options tmp_opts(options_);
  tmp_opts.listeners.clear();
  tmp_opts.env = GetDbEnv();
  // Avoid delayed deletion so whole directory can be deleted
  tmp_opts.sst_file_manager.reset();
  //  Temporarily disable error injection for clean-up
  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
  }
  DestroyDB(checkpoint_dir, tmp_opts);
  // Enable back error injection disabled for clean-up
  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
  }
  const size_t num_cfs = column_families_.size();
  const bool use_cf_subset =
      num_cfs > 1 && thread->rand.OneInOpt(FLAGS_subset_cf_checkpoint_one_in);
  const bool use_parallel_engine =
      thread->rand.OneInOpt(FLAGS_parallel_checkpoint_one_in);

  // `in_checkpoint[i]` is whether column family `i` is expected to exist in the
  // checkpoint; all of them unless subset mode picked otherwise.
  std::vector<bool> in_checkpoint(num_cfs, true);

  // Picks a random non-empty proper subset of the column families, marking the
  // excluded ones in `in_checkpoint` and returning the handles to pass to the
  // Checkpoint API.
  auto pick_cf_subset = [&]() {
    // Index 0 is the default column family, which is in every checkpoint
    // whether or not it is requested (it cannot be dropped), so only the
    // non-default families at indices [1, num_cfs) can be excluded.
    for (size_t i = 1; i < num_cfs; ++i) {
      in_checkpoint[i] = thread->rand.OneIn(2);
    }
    // Force one exclusion so every subset checkpoint exercises the MANIFEST
    // column-family-drop rewrite instead of degenerating into a whole-DB
    // checkpoint.
    in_checkpoint[1 + thread->rand.Uniform(static_cast<int>(num_cfs) - 1)] =
        false;

    std::vector<ColumnFamilyHandle*> cfhs;
    cfhs.reserve(num_cfs);
    for (size_t i = 1; i < num_cfs; ++i) {
      if (in_checkpoint[i]) {
        cfhs.push_back(column_families_[i]);
      }
    }
    // Pass the default column family only half the time, to cover both sides
    // of the "included whether or not you pass it" contract. An empty vector
    // means "all column families" to the API, so it has to go in when nothing
    // else did.
    if (cfhs.empty() || thread->rand.OneIn(2)) {
      cfhs.push_back(column_families_[0]);
    }
    return cfhs;
  };

  std::vector<ColumnFamilyHandle*> subset_cfhs;
  if (use_cf_subset) {
    subset_cfhs = pick_cf_subset();
  }

  Checkpoint* checkpoint = nullptr;
  Status s;
  if (use_parallel_engine) {
    CreateCheckpointOptions create_options;
    create_options.column_families = subset_cfhs;
    const IOStatus io_s = checkpoint_engine_->CreateCheckpoint(
        db_, checkpoint_dir, /*sequence_number_ptr=*/nullptr, create_options);
    s = io_s;
  } else {
    s = Checkpoint::Create(db_, &checkpoint);
    if (s.ok()) {
      s = checkpoint->CreateCheckpoint(checkpoint_dir, subset_cfhs);
    }
  }
  if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
    fprintf(stderr, "Fail to create checkpoint to %s\n",
            checkpoint_dir.c_str());
    std::vector<std::string> files;

    // Temporarily disable error injection to print debugging information
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);
    }

    Status my_s = GetDbEnv()->GetChildren(checkpoint_dir, &files);

    // Enable back disable error injection disabled for printing debugging
    // information
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->EnableThreadLocalErrorInjection(
          FaultInjectionIOType::kMetadataRead);
    }
    if (!my_s.ok()) {
      fprintf(stderr, "Fail to GetChildren under %s due to %s\n",
              checkpoint_dir.c_str(), my_s.ToString().c_str());
    } else {
      for (const auto& f : files) {
        fprintf(stderr, " %s\n", f.c_str());
      }
    }
  }
  delete checkpoint;
  checkpoint = nullptr;
  std::vector<ColumnFamilyHandle*> cf_handles;
  std::unique_ptr<DB> checkpoint_db;
  // Maps a column family index in `column_families_` to its index in
  // `cf_handles`, or -1 when it was left out of a subset checkpoint.
  std::vector<int> cf_handle_index(num_cfs, -1);
  if (s.ok()) {
    Options options(options_);
    options.best_efforts_recovery = false;
    options.listeners.clear();
    // Avoid race condition in trash handling after delete checkpoint_db
    options.sst_file_manager.reset();
    std::vector<ColumnFamilyDescriptor> cf_descs;
    // TODO(ajkr): `column_family_names_` is not safe to access here when
    // `clear_column_family_one_in != 0`. But we can't easily switch to
    // `ListColumnFamilies` to get names because it won't necessarily give
    // the same order as `column_family_names_`.
    assert(FLAGS_clear_column_family_one_in == 0);
    if (FLAGS_clear_column_family_one_in == 0) {
      for (size_t i = 0; i < num_cfs; ++i) {
        if (!in_checkpoint[i]) {
          continue;
        }
        cf_handle_index[i] = static_cast<int>(cf_descs.size());
        cf_descs.emplace_back(column_family_names_[i],
                              ColumnFamilyOptions(options));
      }
      if (use_cf_subset) {
        s = VerifyCheckpointColumnFamilies(checkpoint_dir, DBOptions(options),
                                           cf_descs);
      }
      if (s.ok()) {
        s = DB::OpenForReadOnly(DBOptions(options), checkpoint_dir, cf_descs,
                                &cf_handles, &checkpoint_db);
      }
    }
  }
  if (checkpoint_db != nullptr) {
    // Note the column families chosen by `rand_column_families` cannot be
    // dropped while the locks for `rand_keys` are held. So we should not have
    // to worry about accessing those column families throughout this function.
    for (size_t i = 0; s.ok() && i < rand_column_families.size(); ++i) {
      const int handle_index = cf_handle_index[rand_column_families[i]];
      if (handle_index < 0) {
        // Left out of this subset checkpoint, so there is nothing to compare.
        continue;
      }
      std::string key_str = Key(rand_keys[0]);
      Slice key = key_str;
      std::string ts_str;
      Slice ts;
      ReadOptions read_opts;
      if (FLAGS_user_timestamp_size > 0) {
        ts_str = GetReadTimestamp();
        ts = ts_str;
        read_opts.timestamp = &ts;
      }
      std::string value;
      Status get_status = DbStressGet(checkpoint_db.get(), read_opts,
                                      cf_handles[handle_index], key, &value);
      bool exists =
          thread->shared->Exists(rand_column_families[i], rand_keys[0]);
      if (get_status.ok()) {
        if (!exists && ShouldAcquireMutexOnKey()) {
          std::ostringstream oss;
          oss << "0x" << key.ToString(true) << " exists in checkpoint "
              << checkpoint_dir << " but not in original db";
          s = Status::Corruption(oss.str());
        }
      } else if (get_status.IsNotFound()) {
        if (exists && ShouldAcquireMutexOnKey()) {
          std::ostringstream oss;
          oss << "0x" << key.ToString(true)
              << " exists in original db but not in checkpoint "
              << checkpoint_dir;
          s = Status::Corruption(oss.str());
        }
      } else {
        s = get_status;
      }
    }
    for (auto cfh : cf_handles) {
      delete cfh;
    }
    cf_handles.clear();
    checkpoint_db.reset();
  }

  //  Temporarily disable error injection for clean-up
  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
  }

  if (!s.ok() && !IsErrorInjectedAndRetryable(s)) {
    fprintf(stderr, "A checkpoint operation failed with: %s\n",
            s.ToString().c_str());
  } else {
    DestroyDB(checkpoint_dir, tmp_opts);
  }

  // Enable back error injection disabled for clean-up
  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
  }
  return s;
}

void StressTest::TestGetProperty(ThreadState* thread) const {
  std::unordered_set<std::string> levelPropertyNames = {
      DB::Properties::kAggregatedTablePropertiesAtLevel,
      DB::Properties::kCompressionRatioAtLevelPrefix,
      DB::Properties::kNumFilesAtLevelPrefix,
  };
  std::unordered_set<std::string> unknownPropertyNames = {
      DB::Properties::kEstimateOldestKeyTime,
      DB::Properties::kOptionsStatistics,
      DB::Properties::
          kLiveSstFilesSizeAtTemperature,  // similar to levelPropertyNames, it
                                           // requires a number suffix
  };
  unknownPropertyNames.insert(levelPropertyNames.begin(),
                              levelPropertyNames.end());

  std::unordered_set<std::string> blockCachePropertyNames = {
      DB::Properties::kBlockCacheCapacity,
      DB::Properties::kBlockCacheEntryStats,
      DB::Properties::kFastBlockCacheEntryStats,
      DB::Properties::kBlockCacheUsage,
      DB::Properties::kBlockCachePinnedUsage,
  };
  const Options options = db_->GetOptions();
  if (options.table_factory->GetOptions<Cache>(
          TableFactory::kBlockCacheOpts()) == nullptr) {
    unknownPropertyNames.insert(blockCachePropertyNames.begin(),
                                blockCachePropertyNames.end());
  }

  std::unordered_set<std::string> blobCachePropertyNames = {
      DB::Properties::kBlobCacheCapacity,
      DB::Properties::kBlobCacheUsage,
      DB::Properties::kBlobCachePinnedUsage,
  };
  if (options.blob_cache == nullptr) {
    unknownPropertyNames.insert(blobCachePropertyNames.begin(),
                                blobCachePropertyNames.end());
  }

  std::string prop;
  for (const auto& ppt_name_and_info : InternalStats::ppt_name_to_info) {
    bool res = db_->GetProperty(ppt_name_and_info.first, &prop);
    if (unknownPropertyNames.find(ppt_name_and_info.first) ==
        unknownPropertyNames.end()) {
      if (!res) {
        fprintf(stderr, "Failed to get DB property: %s\n",
                ppt_name_and_info.first.c_str());
        thread->shared->SetVerificationFailure();
      }
      if (ppt_name_and_info.second.handle_int != nullptr) {
        uint64_t prop_int;
        if (!db_->GetIntProperty(ppt_name_and_info.first, &prop_int)) {
          fprintf(stderr, "Failed to get Int property: %s\n",
                  ppt_name_and_info.first.c_str());
          thread->shared->SetVerificationFailure();
        }
      }
      if (ppt_name_and_info.second.handle_map != nullptr) {
        std::map<std::string, std::string> prop_map;
        if (!db_->GetMapProperty(ppt_name_and_info.first, &prop_map)) {
          fprintf(stderr, "Failed to get Map property: %s\n",
                  ppt_name_and_info.first.c_str());
          thread->shared->SetVerificationFailure();
        }
      }
    }
  }

  ROCKSDB_NAMESPACE::ColumnFamilyMetaData cf_meta_data;
  db_->GetColumnFamilyMetaData(&cf_meta_data);
  int level_size = static_cast<int>(cf_meta_data.levels.size());
  for (int level = 0; level < level_size; level++) {
    for (const auto& ppt_name : levelPropertyNames) {
      bool res = db_->GetProperty(ppt_name + std::to_string(level), &prop);
      if (!res) {
        fprintf(stderr, "Failed to get DB property: %s\n",
                (ppt_name + std::to_string(level)).c_str());
        thread->shared->SetVerificationFailure();
      }
    }
  }

  // Test for an invalid property name
  if (thread->rand.OneIn(100)) {
    if (db_->GetProperty("rocksdb.invalid_property_name", &prop)) {
      fprintf(stderr, "Failed to return false for invalid property name\n");
      thread->shared->SetVerificationFailure();
    }
  }
}

Status StressTest::TestGetPropertiesOfAllTables() const {
  TablePropertiesCollection props;
  return db_->GetPropertiesOfAllTables(&props);
}

void StressTest::TestCompactFiles(ThreadState* thread,
                                  ColumnFamilyHandle* column_family) {
  ROCKSDB_NAMESPACE::ColumnFamilyMetaData cf_meta_data;
  db_->GetColumnFamilyMetaData(column_family, &cf_meta_data);

  if (cf_meta_data.levels.empty()) {
    return;
  }

  // Randomly compact up to three consecutive files from a level
  const int kMaxRetry = 3;
  for (int attempt = 0; attempt < kMaxRetry; ++attempt) {
    size_t random_level =
        thread->rand.Uniform(static_cast<int>(cf_meta_data.levels.size()));

    const auto& files = cf_meta_data.levels[random_level].files;
    if (files.size() > 0) {
      size_t random_file_index =
          thread->rand.Uniform(static_cast<int>(files.size()));
      if (files[random_file_index].being_compacted) {
        // Retry as the selected file is currently being compacted
        continue;
      }

      std::vector<std::string> input_files;
      input_files.push_back(files[random_file_index].name);
      if (random_file_index > 0 &&
          !files[random_file_index - 1].being_compacted) {
        input_files.push_back(files[random_file_index - 1].name);
      }
      if (random_file_index + 1 < files.size() &&
          !files[random_file_index + 1].being_compacted) {
        input_files.push_back(files[random_file_index + 1].name);
      }

      size_t output_level =
          std::min(random_level + 1, cf_meta_data.levels.size() - 1);
      CompactionOptions compact_options;
      if (thread->rand.OneIn(2)) {
        compact_options.output_file_size_limit = FLAGS_target_file_size_base;
      }
      std::ostringstream compact_opt_oss;
      compact_opt_oss << "output_file_size_limit: "
                      << compact_options.output_file_size_limit;
      auto s = db_->CompactFiles(compact_options, column_family, input_files,
                                 static_cast<int>(output_level));
      if (!s.ok()) {
        thread->stats.AddNumCompactFilesFailed(1);
      } else {
        thread->stats.AddNumCompactFilesSucceed(1);
      }
      // Verify before the fail-fast: a corruption that CompactFiles itself
      // returns must be recorded (CORRUPTION) before we terminate, or it
      // mis-buckets as CRASH. No-op unless CPU-corruption verification is on.
      MaybeVerifyCpuCorruption(thread, "compactfiles", s);
      if (!s.ok() && !IsTolerableCompactionFailure(s)) {
        fprintf(stderr,
                "Unable to perform CompactFiles(): %s under specified "
                "CompactionOptions: %s (Empty string or missing field "
                "indicates default option or value is used)\n",
                s.ToString().c_str(), compact_opt_oss.str().c_str());
        thread->shared->SafeTerminate();
      }
      break;
    }
  }
}

void StressTest::TestPromoteL0(ThreadState* thread,
                               ColumnFamilyHandle* column_family) {
  int target_level = thread->rand.Next() % options_.num_levels;
  Status s = db_->PromoteL0(column_family, target_level);
  if (!s.ok()) {
    // The second error occurs when another concurrent PromoteL0() moving the
    // same files finishes first which is an allowed behavior
    bool non_ok_status_allowed =
        s.IsInvalidArgument() ||
        (s.IsCorruption() &&
         s.ToString().find("VersionBuilder: Cannot delete table file") !=
             std::string::npos &&
         s.ToString().find("since it is on level") != std::string::npos);

    if (!non_ok_status_allowed) {
      fprintf(stderr,
              "Unable to perform PromoteL0(): %s under specified "
              "target_level: %d.\n",
              s.ToString().c_str(), target_level);
      thread->shared->SafeTerminate();
    }
  }
}

void StressTest::TestFlush(ThreadState* thread,
                           const std::vector<int>& rand_column_families) {
  FlushOptions flush_opts;
  assert(flush_opts.wait);
  // Occasionally exercise the stronger guarantee that Flush() does not return
  // until the OnFlushCompleted listener callbacks have finished.
  if (thread->rand.OneIn(4)) {
    flush_opts.listener_wait = true;
  }
  Status status;
  if (FLAGS_atomic_flush) {
    status = db_->Flush(flush_opts, column_families_);
  } else {
    std::vector<ColumnFamilyHandle*> cfhs;
    std::for_each(
        rand_column_families.begin(), rand_column_families.end(),
        [this, &cfhs](int k) { cfhs.push_back(column_families_[k]); });
    status = db_->Flush(flush_opts, cfhs);
  }
  // Verify before the fail-fast (a no-op unless CPU-corruption verification is
  // on), then route the status through the standard flush error handling.
  MaybeVerifyCpuCorruption(thread, "flush", status);
  ProcessStatus(thread->shared, "Flush", status);
}

Status StressTest::TestResetStats() { return db_->ResetStats(); }

Status StressTest::TestPauseBackground(ThreadState* thread) {
  Status status = db_->PauseBackgroundWork();
  if (!status.ok()) {
    return status;
  }
  // To avoid stalling/deadlocking ourself in this thread, just
  // sleep here during pause and let other threads do db operations.
  // Sleep up to ~16 seconds (2**24 microseconds), but very skewed
  // toward short pause. (1 chance in 25 of pausing >= 1s;
  // 1 chance in 625 of pausing full 16s.)
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(1 << pwr2_micros);
  return db_->ContinueBackgroundWork();
}

Status StressTest::TestDisableFileDeletions(ThreadState* thread) {
  Status status = db_->DisableFileDeletions();
  if (!status.ok()) {
    return status;
  }
  // Similar to TestPauseBackground()
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(1 << pwr2_micros);
  return db_->EnableFileDeletions();
}

Status StressTest::TestDisableManualCompaction(ThreadState* thread) {
  db_->DisableManualCompaction();
  // Similar to TestPauseBackground()
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(1 << pwr2_micros);
  db_->EnableManualCompaction();
  return Status::OK();
}

bool StressTest::ShouldAbortAndResumeCompactions() const {
  uint64_t is_write_stopped = 0;
  if (db_->GetIntProperty(DB::Properties::kIsWriteStopped, &is_write_stopped) &&
      is_write_stopped != 0) {
    return false;
  }

  uint64_t delayed_write_rate = 0;
  if (db_->GetIntProperty(DB::Properties::kActualDelayedWriteRate,
                          &delayed_write_rate) &&
      delayed_write_rate != 0) {
    return false;
  }

  uint64_t running_compactions = 0;
  const bool has_running_compactions =
      db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                          &running_compactions) &&
      running_compactions != 0;

  uint64_t pending_compactions = 0;
  uint64_t pending_compaction_bytes = 0;
  for (auto* column_family : column_families_) {
    uint64_t cf_pending_compactions = 0;
    if (db_->GetIntProperty(column_family, DB::Properties::kCompactionPending,
                            &cf_pending_compactions)) {
      pending_compactions += cf_pending_compactions;
    }

    uint64_t cf_pending_compaction_bytes = 0;
    if (db_->GetIntProperty(column_family,
                            DB::Properties::kEstimatePendingCompactionBytes,
                            &cf_pending_compaction_bytes)) {
      pending_compaction_bytes += cf_pending_compaction_bytes;
    }
  }

  return has_running_compactions || pending_compactions != 0 ||
         pending_compaction_bytes != 0;
}

Status StressTest::TestAbortAndResumeCompactions(ThreadState* thread) {
  uint64_t successful_compactions = 0;
  if (!thread->shared->TryBeginAbortAndResumeCompactions(
          &successful_compactions)) {
    return Status::OK();
  }

  if (!ShouldAbortAndResumeCompactions()) {
    thread->shared->EndAbortAndResumeCompactions();
    return Status::OK();
  }

  // Each abort must be paid for by at least one completed non-aborted
  // compaction, and concurrent abort/resume sections are serialized. This keeps
  // aggressive random sampling from starving compactions and causing unrelated
  // write stalls.
  thread->shared->MarkAbortAndResumeCompactions(successful_compactions);
  ScopedThreadOperation op(thread,
                           StressOperationType::kAbortResumeCompactions);

  // Abort all running compactions and prevent new ones from starting
  db_->AbortAllCompactions();
  // Sleep to allow other threads to attempt operations while aborted
  // Uses the same short-skewed pattern as TestPauseBackground, capped to avoid
  // creating long compaction starvation windows.
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(
      std::min(1 << pwr2_micros, kMaxAbortResumeCompactionsSleepMicros));
  // Resume compactions
  db_->ResumeAllCompactions();
  thread->shared->EndAbortAndResumeCompactions();
  return Status::OK();
}

Status StressTest::TestAbortAndResumeCfCompactions(ThreadState* thread) {
  int rand_cf = thread->rand.Uniform(static_cast<int>(column_families_.size()));
  auto* cfh = column_families_[rand_cf];
  db_->AbortCompactions(cfh);
  int pwr2_micros =
      std::min(thread->rand.Uniform(25), thread->rand.Uniform(25));
  clock_->SleepForMicroseconds(1 << pwr2_micros);
  db_->ResumeCompactions(cfh);
  return Status::OK();
}

void StressTest::TestAcquireSnapshot(ThreadState* thread,
                                     int rand_column_family,
                                     const std::string& keystr, uint64_t i) {
  Slice key = keystr;
  ColumnFamilyHandle* column_family = column_families_[rand_column_family];
  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ropt;
  auto db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  const bool ww_snapshot = thread->rand.OneIn(10);
  const Snapshot* snapshot =
      ww_snapshot ? db_impl->GetSnapshotForWriteConflictBoundary()
                  : db_->GetSnapshot();
  ropt.snapshot = snapshot;
  ropt.auto_refresh_iterator_with_snapshot =
      FLAGS_auto_refresh_iterator_with_snapshot;

  // Ideally, we want snapshot taking and timestamp generation to be atomic
  // here, so that the snapshot corresponds to the timestamp. However, it is
  // not possible with current GetSnapshot() API.
  std::string ts_str;
  Slice ts;
  if (FLAGS_user_timestamp_size > 0) {
    ts_str = GetReadTimestamp();
    ts = ts_str;
    ropt.timestamp = &ts;
  }

  std::string value_at;
  // When taking a snapshot, we also read a key from that snapshot. We
  // will later read the same key before releasing the snapshot and
  // verify that the results are the same.
  Status status_at = DbStressGet(db_, ropt, column_family, key, &value_at);
  if (!status_at.ok() && IsErrorInjectedAndRetryable(status_at)) {
    db_->ReleaseSnapshot(snapshot);
    return;
  }
  std::vector<bool>* key_vec = nullptr;

  if (FLAGS_compare_full_db_state_snapshot && (thread->tid == 0)) {
    key_vec = new std::vector<bool>(FLAGS_max_key);
    // When `prefix_extractor` is set, seeking to beginning and scanning
    // across prefixes are only supported with `total_order_seek` set.
    ropt.total_order_seek = true;
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ropt));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      uint64_t key_val;
      if (GetIntVal(iterator->key().ToString(), &key_val)) {
        (*key_vec)[key_val] = true;
      }
    }
  }

  ThreadState::SnapshotState snap_state = {snapshot,
                                           rand_column_family,
                                           column_family->GetName(),
                                           keystr,
                                           status_at,
                                           value_at,
                                           key_vec,
                                           ts_str};
  uint64_t hold_for = FLAGS_snapshot_hold_ops;
  if (FLAGS_long_running_snapshots) {
    // Hold 10% of snapshots for 10x more
    if (thread->rand.OneIn(10)) {
      assert(hold_for < std::numeric_limits<uint64_t>::max() / 10);
      hold_for *= 10;
      // Hold 1% of snapshots for 100x more
      if (thread->rand.OneIn(10)) {
        assert(hold_for < std::numeric_limits<uint64_t>::max() / 10);
        hold_for *= 10;
      }
    }
  }
  uint64_t release_at = std::min(FLAGS_ops_per_thread - 1, i + hold_for);
  thread->snapshot_queue.emplace(release_at, snap_state);
}

Status StressTest::MaybeReleaseSnapshots(ThreadState* thread, uint64_t i) {
  while (!thread->snapshot_queue.empty() &&
         i >= thread->snapshot_queue.front().first) {
    auto snap_state = thread->snapshot_queue.front().second;
    assert(snap_state.snapshot);
    // Note: this is unsafe as the cf might be dropped concurrently. But
    // it is ok since unclean cf drop is cunnrently not supported by write
    // prepared transactions.
    Status s = AssertSame(db_, column_families_[snap_state.cf_at], snap_state);
    db_->ReleaseSnapshot(snap_state.snapshot);
    delete snap_state.key_vec;
    thread->snapshot_queue.pop();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

void StressTest::TestCompactRange(ThreadState* thread, int64_t rand_key,
                                  const Slice& start_key,
                                  ColumnFamilyHandle* column_family) {
  int64_t end_key_num;
  if (std::numeric_limits<int64_t>::max() - rand_key <
      FLAGS_compact_range_width) {
    end_key_num = std::numeric_limits<int64_t>::max();
  } else {
    end_key_num = FLAGS_compact_range_width + rand_key;
  }
  std::string end_key_buf = Key(end_key_num);
  Slice end_key(end_key_buf);

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = static_cast<bool>(thread->rand.Next() % 2);
  if (static_cast<ROCKSDB_NAMESPACE::CompactionStyle>(FLAGS_compaction_style) !=
      ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO) {
    cro.change_level = static_cast<bool>(thread->rand.Next() % 2);
  }
  if (thread->rand.OneIn(2)) {
    cro.target_level = thread->rand.Next() % options_.num_levels;
  }
  std::vector<BottommostLevelCompaction> bottom_level_styles = {
      BottommostLevelCompaction::kSkip,
      BottommostLevelCompaction::kIfHaveCompactionFilter,
      BottommostLevelCompaction::kForce,
      BottommostLevelCompaction::kForceOptimized};
  cro.bottommost_level_compaction =
      bottom_level_styles[thread->rand.Next() %
                          static_cast<uint32_t>(bottom_level_styles.size())];
  cro.allow_write_stall = static_cast<bool>(thread->rand.Next() % 2);
  cro.max_subcompactions = static_cast<uint32_t>(thread->rand.Next() % 4);
  std::vector<BlobGarbageCollectionPolicy> blob_gc_policies = {
      BlobGarbageCollectionPolicy::kForce,
      BlobGarbageCollectionPolicy::kDisable,
      BlobGarbageCollectionPolicy::kUseDefault};
  cro.blob_garbage_collection_policy =
      blob_gc_policies[thread->rand.Next() %
                       static_cast<uint32_t>(blob_gc_policies.size())];
  cro.blob_garbage_collection_age_cutoff =
      static_cast<double>(thread->rand.Next() % 100) / 100.0;

  const Snapshot* pre_snapshot = nullptr;
  uint32_t pre_hash = 0;
  if (thread->rand.OneIn(2)) {
    // Temporarily disable error injection to for validation
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
    }

    // Declare a snapshot and compare the data before and after the compaction
    pre_snapshot = db_->GetSnapshot();
    pre_hash =
        GetRangeHash(thread, pre_snapshot, column_family, start_key, end_key);

    // Enable back error injection disabled for validation
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
    }
  }
  std::ostringstream compact_range_opt_oss;
  compact_range_opt_oss << "exclusive_manual_compaction: "
                        << cro.exclusive_manual_compaction
                        << ", change_level: " << cro.change_level
                        << ", target_level: " << cro.target_level
                        << ", bottommost_level_compaction: "
                        << static_cast<int>(cro.bottommost_level_compaction)
                        << ", allow_write_stall: " << cro.allow_write_stall
                        << ", max_subcompactions: " << cro.max_subcompactions
                        << ", blob_garbage_collection_policy: "
                        << static_cast<int>(cro.blob_garbage_collection_policy)
                        << ", blob_garbage_collection_age_cutoff: "
                        << cro.blob_garbage_collection_age_cutoff;
  Status status = db_->CompactRange(cro, column_family, &start_key, &end_key);

  // Verify before the fail-fast: a corruption that CompactRange itself returns
  // must be recorded (CORRUPTION) before we flag failure, or it mis-buckets.
  // No-op unless CPU-corruption verification is on.
  MaybeVerifyCpuCorruption(thread, "compactrange", status);
  if (!status.ok() && !IsTolerableCompactionFailure(status)) {
    fprintf(stderr,
            "Unable to perform CompactRange(): %s under specified "
            "CompactRangeOptions: %s (Empty string or missing field "
            "indicates default option or value is used)\n",
            status.ToString().c_str(), compact_range_opt_oss.str().c_str());
    // Fail fast to preserve the DB state.
    thread->shared->SetVerificationFailure();
  }

  if (pre_snapshot != nullptr) {
    // Temporarily disable error injection for validation
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
    }
    uint32_t post_hash =
        GetRangeHash(thread, pre_snapshot, column_family, start_key, end_key);
    if (pre_hash != post_hash) {
      fprintf(stderr,
              "Data hash different before and after compact range "
              "start_key %s end_key %s under specified CompactRangeOptions: %s "
              "(Empty string or "
              "missing field indicates default option or value is used)\n",
              start_key.ToString(true).c_str(), end_key.ToString(true).c_str(),
              compact_range_opt_oss.str().c_str());
      thread->stats.AddErrors(1);
      // Fail fast to preserve the DB state.
      thread->shared->SetVerificationFailure();
    }
    db_->ReleaseSnapshot(pre_snapshot);
    if (db_fault_injection_fs_) {
      // Enable back error injection disabled for validation
      db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
    }
  }
}

uint32_t StressTest::GetRangeHash(ThreadState* thread, const Snapshot* snapshot,
                                  ColumnFamilyHandle* column_family,
                                  const Slice& start_key,
                                  const Slice& end_key) {
  // This `ReadOptions` is for validation purposes. Ignore
  // `FLAGS_rate_limit_user_ops` to avoid slowing any validation.
  ReadOptions ro;
  ro.snapshot = snapshot;
  ro.total_order_seek = true;
  ro.auto_refresh_iterator_with_snapshot =
      FLAGS_auto_refresh_iterator_with_snapshot;
  std::string ts_str;
  Slice ts;
  if (FLAGS_user_timestamp_size > 0) {
    ts_str = GetReadTimestamp();
    ts = ts_str;
    ro.timestamp = &ts;
  }

  std::unique_ptr<Iterator> it(db_->NewIterator(ro, column_family));

  constexpr char kCrcCalculatorSepearator = ';';

  uint32_t crc = 0;

  for (it->Seek(start_key);
       it->Valid() && options_.comparator->Compare(it->key(), end_key) <= 0;
       it->Next()) {
    crc = crc32c::Extend(crc, it->key().data(), it->key().size());
    crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));
    crc = crc32c::Extend(crc, it->value().data(), it->value().size());
    crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));

    for (const auto& column : it->columns()) {
      crc = crc32c::Extend(crc, column.name().data(), column.name().size());
      crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));
      crc = crc32c::Extend(crc, column.value().data(), column.value().size());
      crc = crc32c::Extend(crc, &kCrcCalculatorSepearator, sizeof(char));
    }
  }

  if (!it->status().ok()) {
    fprintf(stderr, "Iterator non-OK when calculating range CRC: %s\n",
            it->status().ToString().c_str());
    thread->stats.AddErrors(1);
    // Fail fast to preserve the DB state.
    thread->shared->SetVerificationFailure();
  }

  return crc;
}

void StressTest::PrintEnv() const {
  fprintf(stdout, "RocksDB version           : %d.%d\n", kMajorVersion,
          kMinorVersion);
  fprintf(stdout, "Format version            : %d\n", FLAGS_format_version);
  fprintf(stdout, "TransactionDB             : %s\n",
          FLAGS_use_txn ? "true" : "false");
  if (FLAGS_use_txn) {
    fprintf(stdout, "TransactionDB Type        : %s\n",
            FLAGS_use_optimistic_txn ? "Optimistic" : "Pessimistic");
    if (FLAGS_use_optimistic_txn) {
      fprintf(stdout, "OCC Validation Type       : %d\n",
              static_cast<int>(FLAGS_occ_validation_policy));
      if (static_cast<uint64_t>(OccValidationPolicy::kValidateParallel) ==
          FLAGS_occ_validation_policy) {
        fprintf(stdout, "Share Lock Buckets        : %s\n",
                FLAGS_share_occ_lock_buckets ? "true" : "false");
        if (FLAGS_share_occ_lock_buckets) {
          fprintf(stdout, "Lock Bucket Count         : %d\n",
                  static_cast<int>(FLAGS_occ_lock_bucket_count));
        }
      }
    } else {
      fprintf(stdout, "Two write queues:         : %s\n",
              FLAGS_two_write_queues ? "true" : "false");
      fprintf(stdout, "Write policy              : %d\n",
              static_cast<int>(FLAGS_txn_write_policy));
      if (static_cast<uint64_t>(TxnDBWritePolicy::WRITE_PREPARED) ==
              FLAGS_txn_write_policy ||
          static_cast<uint64_t>(TxnDBWritePolicy::WRITE_UNPREPARED) ==
              FLAGS_txn_write_policy) {
        fprintf(stdout, "Snapshot cache bits       : %d\n",
                static_cast<int>(FLAGS_wp_snapshot_cache_bits));
        fprintf(stdout, "Commit cache bits         : %d\n",
                static_cast<int>(FLAGS_wp_commit_cache_bits));
      }
      fprintf(stdout, "last cwb for recovery    : %s\n",
              FLAGS_use_only_the_last_commit_time_batch_for_recovery ? "true"
                                                                     : "false");
    }
  }

  fprintf(stdout, "Stacked BlobDB            : %s\n",
          FLAGS_use_blob_db ? "true" : "false");
  fprintf(stdout, "Read only mode            : %s\n",
          FLAGS_read_only ? "true" : "false");
  fprintf(stdout, "Atomic flush              : %s\n",
          FLAGS_atomic_flush ? "true" : "false");
  fprintf(stdout, "Manual WAL flush          : %s\n",
          FLAGS_manual_wal_flush_one_in > 0 ? "true" : "false");
  fprintf(stdout, "Column families           : %d\n", FLAGS_column_families);
  if (!FLAGS_test_batches_snapshots) {
    fprintf(stdout, "Clear CFs one in          : %d\n",
            FLAGS_clear_column_family_one_in);
  }
  fprintf(stdout, "Number of threads         : %d\n", FLAGS_threads);
  fprintf(stdout, "Ops per thread            : %lu\n",
          (unsigned long)FLAGS_ops_per_thread);
  std::string ttl_state("unused");
  if (FLAGS_ttl > 0) {
    ttl_state = std::to_string(FLAGS_ttl);
  }
  fprintf(stdout, "Time to live(sec)         : %s\n", ttl_state.c_str());
  fprintf(stdout, "Read percentage           : %d%%\n", FLAGS_readpercent);
  fprintf(stdout, "Prefix percentage         : %d%%\n", FLAGS_prefixpercent);
  fprintf(stdout, "Write percentage          : %d%%\n", FLAGS_writepercent);
  fprintf(stdout, "Delete percentage         : %d%%\n", FLAGS_delpercent);
  fprintf(stdout, "Delete range percentage   : %d%%\n", FLAGS_delrangepercent);
  fprintf(stdout, "No overwrite percentage   : %d%%\n",
          FLAGS_nooverwritepercent);
  fprintf(stdout, "Iterate percentage        : %d%%\n", FLAGS_iterpercent);
  fprintf(stdout, "Custom ops percentage     : %d%%\n", FLAGS_customopspercent);
  fprintf(stdout, "DB-write-buffer-size      : %" PRIu64 "\n",
          FLAGS_db_write_buffer_size);
  fprintf(stdout, "Write-buffer-size         : %d\n", FLAGS_write_buffer_size);
  fprintf(stdout, "Iterations                : %lu\n",
          (unsigned long)FLAGS_num_iterations);
  fprintf(stdout, "Max key                   : %lu\n",
          (unsigned long)FLAGS_max_key);
  fprintf(stdout, "Ratio #ops/#keys          : %f\n",
          (1.0 * FLAGS_ops_per_thread * FLAGS_threads) / FLAGS_max_key);
  fprintf(stdout, "Num times DB reopens      : %d\n", FLAGS_reopen);
  fprintf(stdout, "Batches/snapshots         : %d\n",
          FLAGS_test_batches_snapshots);
  fprintf(stdout, "Do update in place        : %d\n",
          FLAGS_inplace_update_support);
  fprintf(stdout, "Num keys per lock         : %d\n",
          1 << FLAGS_log2_keys_per_lock);
  std::string compression = CompressionTypeToString(compression_type_e);
  fprintf(stdout, "Compression               : %s\n", compression.c_str());
  std::string bottommost_compression =
      CompressionTypeToString(bottommost_compression_type_e);
  fprintf(stdout, "Bottommost Compression    : %s\n",
          bottommost_compression.c_str());
  std::string checksum = ChecksumTypeToString(checksum_type_e);
  fprintf(stdout, "Checksum type             : %s\n", checksum.c_str());
  fprintf(stdout, "File checksum impl        : %s\n",
          FLAGS_file_checksum_impl.c_str());
  fprintf(stdout, "Bloom bits / key          : %s\n",
          FormatDoubleParam(FLAGS_bloom_bits).c_str());
  fprintf(stdout, "Max subcompactions        : %" PRIu64 "\n",
          FLAGS_subcompactions);
  fprintf(stdout, "Use MultiGet              : %s\n",
          FLAGS_use_multiget ? "true" : "false");
  fprintf(stdout, "Use full external table   : %s\n",
          FLAGS_use_full_external_table ? "true" : "false");
  fprintf(stdout, "Use async DB API          : %s\n",
          FLAGS_use_async_db_api ? "true" : "false");
  fprintf(stdout, "Use GetEntity             : %s\n",
          FLAGS_use_get_entity ? "true" : "false");
  fprintf(stdout, "Use MultiGetEntity        : %s\n",
          FLAGS_use_multi_get_entity ? "true" : "false");
  fprintf(stdout, "Verification only         : %s\n",
          FLAGS_verification_only ? "true" : "false");

  const char* memtablerep;
  switch (FLAGS_rep_factory) {
    default:
    case kSkipList:
      memtablerep = "skip_list";
      break;
    case kHashSkipList:
      memtablerep = "prefix_hash";
      break;
    case kVectorRep:
      memtablerep = "vector";
      break;
  }

  fprintf(stdout, "Memtablerep               : %s\n", memtablerep);

#ifndef NDEBUG
  KillPoint* kp = KillPoint::GetInstance();
  fprintf(stdout, "Test kill odd             : %d\n", kp->rocksdb_kill_odds);
  if (!kp->rocksdb_kill_exclude_prefixes.empty()) {
    fprintf(stdout, "Skipping kill points prefixes:\n");
    for (auto& p : kp->rocksdb_kill_exclude_prefixes) {
      fprintf(stdout, "  %s\n", p.c_str());
    }
  }
#endif
  fprintf(stdout, "Periodic Compaction Secs  : %" PRIu64 "\n",
          FLAGS_periodic_compaction_seconds);
  fprintf(stdout, "Daily Offpeak UTC         : %s\n",
          FLAGS_daily_offpeak_time_utc.c_str());
  fprintf(stdout, "Compaction TTL            : %" PRIu64 "\n",
          FLAGS_compaction_ttl);
  const char* compaction_pri = "";
  switch (FLAGS_compaction_pri) {
    case kByCompensatedSize:
      compaction_pri = "kByCompensatedSize";
      break;
    case kOldestLargestSeqFirst:
      compaction_pri = "kOldestLargestSeqFirst";
      break;
    case kOldestSmallestSeqFirst:
      compaction_pri = "kOldestSmallestSeqFirst";
      break;
    case kMinOverlappingRatio:
      compaction_pri = "kMinOverlappingRatio";
      break;
    case kRoundRobin:
      compaction_pri = "kRoundRobin";
      break;
  }
  fprintf(stdout, "Compaction Pri            : %s\n", compaction_pri);
  fprintf(stdout, "Background Purge          : %d\n",
          static_cast<int>(FLAGS_avoid_unnecessary_blocking_io));
  fprintf(stdout, "Write DB ID to manifest   : %d\n",
          static_cast<int>(FLAGS_write_dbid_to_manifest));
  fprintf(stdout, "Track OPTIONS in manifest : %d\n",
          static_cast<int>(FLAGS_track_options_file_number_in_manifest));
  fprintf(stdout, "Optimize manifest recovery: %d\n",
          static_cast<int>(FLAGS_optimize_manifest_for_recovery));
  fprintf(stdout, "Reuse manifest on open    : %d\n",
          static_cast<int>(FLAGS_reuse_manifest_on_open));
  fprintf(stdout, "Max Write Batch Group Size: %" PRIu64 "\n",
          FLAGS_max_write_batch_group_size_bytes);
  fprintf(stdout, "Use dynamic level         : %d\n",
          static_cast<int>(FLAGS_level_compaction_dynamic_level_bytes));
  fprintf(stdout, "Metadata read fault one in         : %d\n",
          FLAGS_metadata_read_fault_one_in);
  fprintf(stdout, "Metadata write fault one in        : %d\n",
          FLAGS_metadata_write_fault_one_in);
  fprintf(stdout, "Read fault one in         : %d\n", FLAGS_read_fault_one_in);
  fprintf(stdout, "Write fault one in        : %d\n", FLAGS_write_fault_one_in);
  fprintf(stdout, "File-scoped WAL write faults: %d\n",
          static_cast<int>(FLAGS_file_scope_wal_write_faults));
  fprintf(stdout, "Open metadata read fault one in:\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_metadata_read_fault_one_in);
  fprintf(stdout, "Open metadata write fault one in:\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_metadata_write_fault_one_in);
  fprintf(stdout, "Open read fault one in          :\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_read_fault_one_in);
  fprintf(stdout, "Open write fault one in         :\n");
  fprintf(stdout, "                            %d\n",
          FLAGS_open_write_fault_one_in);
  fprintf(stdout, "Sync fault injection      : %d\n",
          FLAGS_sync_fault_injection);
  fprintf(stdout, "Best efforts recovery     : %d\n",
          static_cast<int>(FLAGS_best_efforts_recovery));
  fprintf(stdout, "User timestamp size bytes : %d\n",
          static_cast<int>(FLAGS_user_timestamp_size));
  fprintf(stdout, "Persist user defined timestamps : %d\n",
          FLAGS_persist_user_defined_timestamps);
  fprintf(stdout, "WAL compression           : %s\n",
          FLAGS_wal_compression.c_str());
  fprintf(stdout, "Try verify sst unique id  : %d\n",
          static_cast<int>(FLAGS_verify_sst_unique_id_in_manifest));
  fprintf(stdout, "Fast SST open             : %d\n",
          static_cast<int>(FLAGS_fast_sst_open));

  fprintf(stdout, "------------------------------------------------\n");
}

void StressTest::Open(SharedState* shared, bool reopen) {
  assert(db_ == nullptr);
  assert(txn_db_ == nullptr);
  assert(optimistic_txn_db_ == nullptr);
  if (FLAGS_use_trie_index) {
    udi_factory_ = std::make_shared<trie_index::TrieIndexFactory>(
        FLAGS_trie_index_format_version);
  }
  if (!InitializeOptionsFromFile(options_)) {
    InitializeOptionsFromFlags(block_cache, filter_policy_, udi_factory_,
                               options_);
  }
  InitializeOptionsGeneral(block_cache, filter_policy_, sqfc_factory_,
                           options_);
  // Auto compactions scheduled at the end of DB::Open() can append to the
  // MANIFEST before VerifyManifestNotRewritten() runs, so keep them off until
  // Reopen() has verified the MANIFEST.
  if (reopen && manifest_verify_mode_ != MANIFEST_VERIFY_NONE &&
      !options_.disable_auto_compactions) {
    options_.disable_auto_compactions = true;
    auto_compactions_paused_for_manifest_verify_ = true;
  }
  // InitializeOptions* helpers set options_.env = raw_env (no fault injection).
  // Override with per-StressTest env that includes FaultInjectionTestFS +
  // DbStressFSWrapper for all DB I/O.
  options_.env = GetDbEnv();
  // InitializeOptionsGeneral creates SstFileManager with raw_env. Override
  // with per-StressTest env so file deletions go through DbStressFSWrapper.
  if (options_.sst_file_manager) {
    Status status;
    options_.sst_file_manager.reset(NewSstFileManager(
        GetDbEnv(), options_.info_log, "" /* trash_dir */,
        static_cast<int64_t>(FLAGS_sst_file_manager_bytes_per_sec),
        true /* delete_existing_trash */, &status,
        0.25 /* max_trash_db_ratio */,
        FLAGS_sst_file_manager_bytes_per_truncate));
    DB_STRESS_ASSERT_OK_MSG(status, "SstFileManager creation failed");
  }
  DbStressCustomCompressionManager::Register();

  if (!strcasecmp(FLAGS_compression_manager.c_str(), "custom")) {
    options_.compression_manager =
        std::make_shared<DbStressCustomCompressionManager>();
  } else if (!strcasecmp(FLAGS_compression_manager.c_str(), "mixed")) {
    options_.compression_manager =
        std::make_shared<RoundRobinManager>(GetBuiltinV2CompressionManager());
  } else if (!strcasecmp(FLAGS_compression_manager.c_str(), "randommixed")) {
    options_.compression_manager =
        std::make_shared<RandomMixedCompressionManager>(
            GetBuiltinV2CompressionManager());
  } else if (!strcasecmp(FLAGS_compression_manager.c_str(), "none")) {
    // Nothing to do using default compression manager
  } else {
    fprintf(stderr, "Unknown compression manager: %s\n",
            FLAGS_compression_manager.c_str());
    exit(1);
  }
  if (FLAGS_use_full_external_table) {
    options_.table_factory = NewExternalTableFactory(
        std::make_shared<SimpleFullExternalTableFactory>());
  }
  if (FLAGS_prefix_size == 0 && FLAGS_rep_factory == kHashSkipList) {
    fprintf(stderr,
            "prefeix_size cannot be zero if memtablerep == prefix_hash\n");
    exit(1);
  }
  if (FLAGS_prefix_size != 0 && FLAGS_rep_factory != kHashSkipList) {
    fprintf(stdout,
            "WARNING: prefix_size is non-zero but "
            "memtablerep != prefix_hash\n");
  }

  // Remote Compaction
  if (FLAGS_remote_compaction_worker_threads > 0) {
    // Each DB open/reopen gets a fresh compaction service instance with a clean
    // aborted_ state
    auto compaction_service = std::make_shared<DbStressCompactionService>(
        shared, FLAGS_remote_compaction_failure_fall_back_to_local);

    options_.compaction_service = compaction_service;
    options_.use_separate_remote_compaction_pool =
        FLAGS_use_separate_remote_compaction_pool;
  }

  if (FLAGS_allow_resumption_one_in > 0) {
    if (FLAGS_remote_compaction_worker_threads == 0) {
      fprintf(stderr,
              "allow_resumption or randomize_allow_resumption requires "
              "remote_compaction_worker_threads > 0\n");
      exit(1);
    }
  }

  if ((options_.enable_blob_files || options_.enable_blob_garbage_collection ||
       FLAGS_allow_setting_blob_options_dynamically) &&
      FLAGS_best_efforts_recovery) {
    fprintf(stderr,
            "Integrated BlobDB is currently incompatible with best-effort "
            "recovery\n");
    exit(1);
  }

  fprintf(stdout,
          "Integrated BlobDB: blob files enabled %d, min blob size %" PRIu64
          ", direct write enabled %d, direct write partitions %" PRIu32
          ", blob file size %" PRIu64
          ", blob file writable file max buffer size %" PRIu64
          ", blob compression type %s, blob GC enabled %d, cutoff %f, force "
          "threshold %f, blob compaction readahead size %" PRIu64
          ", blob file starting level %d\n",
          options_.enable_blob_files, options_.min_blob_size,
          options_.enable_blob_direct_write,
          options_.blob_direct_write_partitions, options_.blob_file_size,
          options_.blob_file_writable_file_max_buffer_size,
          CompressionTypeToString(options_.blob_compression_type).c_str(),
          options_.enable_blob_garbage_collection,
          options_.blob_garbage_collection_age_cutoff,
          options_.blob_garbage_collection_force_threshold,
          options_.blob_compaction_readahead_size,
          options_.blob_file_starting_level);

  if (FLAGS_use_blob_cache) {
    fprintf(stdout,
            "Integrated BlobDB: blob cache enabled"
            ", block and blob caches shared: %d",
            FLAGS_use_shared_block_and_blob_cache);
    if (!FLAGS_use_shared_block_and_blob_cache) {
      fprintf(stdout,
              ", blob cache size %" PRIu64 ", blob cache num shard bits: %d",
              FLAGS_blob_cache_size, FLAGS_blob_cache_numshardbits);
    }
    fprintf(stdout, ", blob cache prepopulated: %d\n",
            FLAGS_prepopulate_blob_cache);
  } else {
    fprintf(stdout, "Integrated BlobDB: blob cache disabled\n");
  }

  fprintf(stdout, "DB path: [%s]\n", GetDbPath().c_str());

  Status s;

  if (FLAGS_ttl == -1) {
    std::vector<std::string> existing_column_families;
    s = DB::ListColumnFamilies(DBOptions(options_), GetDbPath(),
                               &existing_column_families);  // ignore errors
    const bool db_exists = s.ok();
    if (!s.ok()) {
      // DB doesn't exist
      assert(existing_column_families.empty());
      assert(column_family_names_.empty());
      column_family_names_.push_back(kDefaultColumnFamilyName);
    } else if (column_family_names_.empty()) {
      // this is the first call to the function Open()
      column_family_names_ = existing_column_families;
    } else {
      // this is a reopen. just assert that existing column_family_names are
      // equivalent to what we remember
      auto sorted_cfn = column_family_names_;
      std::sort(sorted_cfn.begin(), sorted_cfn.end());
      std::sort(existing_column_families.begin(),
                existing_column_families.end());
      if (sorted_cfn != existing_column_families) {
        fprintf(stderr, "Expected column families differ from the existing:\n");
        fprintf(stderr, "Expected: {");
        for (const auto& cf : sorted_cfn) {
          fprintf(stderr, "%s ", cf.c_str());
        }
        fprintf(stderr, "}\n");
        fprintf(stderr, "Existing: {");
        for (const auto& cf : existing_column_families) {
          fprintf(stderr, "%s ", cf.c_str());
        }
        fprintf(stderr, "}\n");
      }
      assert(sorted_cfn == existing_column_families);
    }
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    for (const auto& name : column_family_names_) {
      if (name != kDefaultColumnFamilyName) {
        new_column_family_name_ =
            std::max(new_column_family_name_.load(), std::stoi(name) + 1);
      }
      cf_descriptors.emplace_back(name, ColumnFamilyOptions(options_));
    }

    // If this is for DB reopen,  error injection may have been enabled.
    // Disable it here in case there is no open fault injection.
    if (db_fault_injection_fs_) {
      db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
    }
    // Verify before appending missing CFs because read-only open cannot create
    // them.
    DB_STRESS_ASSERT_OK_MSG(
        VerifyOptionsFileStateBeforeOpen(cf_descriptors, db_exists),
        "Persisted OPTIONS verification before open failed");
    while (cf_descriptors.size() < (size_t)FLAGS_column_families) {
      std::string name = std::to_string(new_column_family_name_.load());
      new_column_family_name_++;
      cf_descriptors.emplace_back(name, ColumnFamilyOptions(options_));
      column_family_names_.push_back(name);
    }
    // TODO; test transaction DB Open with fault injection
    if (!FLAGS_use_txn) {
      OpenFaultInjectionConfig open_fault_injection;
      open_fault_injection.sync_fault = FLAGS_sync_fault_injection;
      open_fault_injection.metadata_read_one_in =
          FLAGS_open_metadata_read_fault_one_in;
      open_fault_injection.metadata_write_one_in =
          FLAGS_open_metadata_write_fault_one_in;
      open_fault_injection.read_one_in = FLAGS_open_read_fault_one_in;
      open_fault_injection.write_one_in = FLAGS_open_write_fault_one_in;
      while (true) {
        InitializeListenersForOpen(shared, cf_descriptors);

        const bool open_fault_injection_enabled = MaybeEnableOpenFaultInjection(
            db_fault_injection_fs_, GetDbPath(), open_fault_injection);
        // StackableDB-based BlobDB
        if (FLAGS_use_blob_db) {
          blob_db::BlobDBOptions blob_db_options;
          blob_db_options.blob_file_size = FLAGS_blob_db_file_size;
          blob_db_options.enable_garbage_collection = FLAGS_blob_db_enable_gc;

          blob_db::BlobDB* blob_db = nullptr;
          s = blob_db::BlobDB::Open(options_, blob_db_options, GetDbPath(),
                                    cf_descriptors, &column_families_,
                                    &blob_db);
          if (s.ok()) {
            db_owner_.reset(blob_db);
            db_ = blob_db;
          }
        } else {
          if (db_preload_finished_.load() && FLAGS_read_only) {
            s = DB::OpenForReadOnly(DBOptions(options_), GetDbPath(),
                                    cf_descriptors, &column_families_,
                                    &db_owner_);
          } else {
            s = DB::Open(DBOptions(options_), GetDbPath(), cf_descriptors,
                         &column_families_, &db_owner_);
          }
          if (s.ok()) {
            db_ = db_owner_.get();
          }
        }

        if (open_fault_injection_enabled) {
          db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();

          if (s.ok()) {
            // Injected errors might happen in background compactions. We
            // wait for all compactions to finish to make sure DB is in
            // clean state before executing queries.
            s = db_->GetRootDB()->WaitForCompact(WaitForCompactOptions());
            if (!s.ok()) {
              CleanUpColumnFamilies();
              db_owner_.reset();
              db_ = nullptr;
              secondary_db_.reset();
            }
          }
          if (!s.ok()) {
            // After failure to opening a DB due to IO error or unsynced data
            // loss, retry should successfully open the DB with correct data if
            // no IO error shows up.
            open_fault_injection.Disable();

            // TODO: Unsynced data loss during DB reopen is not supported yet in
            //  stress test. Will need to recreate expected state if we decide
            //  to support unsynced data loss during DB reopen.
            if (!reopen) {
              Random rand(static_cast<uint32_t>(FLAGS_seed));
              if (rand.OneIn(2)) {
                db_fault_injection_fs_->DeleteFilesCreatedAfterLastDirSync(
                    IOOptions(), nullptr);
              }
              if (rand.OneIn(3)) {
                db_fault_injection_fs_->DropUnsyncedFileData();
              } else if (rand.OneIn(2)) {
                db_fault_injection_fs_->DropRandomUnsyncedFileData(&rand);
              }
            }
            continue;
          }
        }
        break;
      }
    } else {
      InitializeListenersForOpen(shared, cf_descriptors);

      if (FLAGS_use_optimistic_txn) {
        OptimisticTransactionDBOptions optimistic_txn_db_options;
        optimistic_txn_db_options.validate_policy =
            static_cast<OccValidationPolicy>(FLAGS_occ_validation_policy);

        if (FLAGS_share_occ_lock_buckets) {
          optimistic_txn_db_options.shared_lock_buckets =
              MakeSharedOccLockBuckets(FLAGS_occ_lock_bucket_count);
        } else {
          optimistic_txn_db_options.occ_lock_buckets =
              FLAGS_occ_lock_bucket_count;
          optimistic_txn_db_options.shared_lock_buckets = nullptr;
        }
        s = OptimisticTransactionDB::Open(
            options_, optimistic_txn_db_options, GetDbPath(), cf_descriptors,
            &column_families_, &optimistic_txn_db_);
        DB_STRESS_ASSERT_OK_MSG(s,
                                "Error in opening the OptimisticTransactionDB");
        {
          db_owner_.reset(optimistic_txn_db_);
          db_ = optimistic_txn_db_;
          db_aptr_.store(optimistic_txn_db_, std::memory_order_release);
        }
      } else {
        TransactionDBOptions txn_db_options;
        // Match the 10-minute lock_timeout used for explicit transactions
        // in NewTxn(), rather than the 1-second default which is too short
        // for stress tests under heavy contention (see T228932399).
        txn_db_options.default_lock_timeout = 600000;
        assert(FLAGS_txn_write_policy <= TxnDBWritePolicy::WRITE_UNPREPARED);
        txn_db_options.write_policy =
            static_cast<TxnDBWritePolicy>(FLAGS_txn_write_policy);
        if (FLAGS_unordered_write) {
          assert(txn_db_options.write_policy ==
                 TxnDBWritePolicy::WRITE_PREPARED);
          options_.unordered_write = true;
          options_.two_write_queues = true;
          txn_db_options.skip_concurrency_control = true;
        } else {
          options_.two_write_queues = FLAGS_two_write_queues;
        }
        txn_db_options.wp_snapshot_cache_bits =
            static_cast<size_t>(FLAGS_wp_snapshot_cache_bits);
        txn_db_options.wp_commit_cache_bits =
            static_cast<size_t>(FLAGS_wp_commit_cache_bits);
        txn_db_options.use_per_key_point_lock_mgr =
            FLAGS_use_per_key_point_lock_mgr;
        PrepareTxnDbOptions(shared, txn_db_options);
        s = TransactionDB::Open(options_, txn_db_options, GetDbPath(),
                                cf_descriptors, &column_families_, &txn_db_);
        DB_STRESS_ASSERT_OK_MSG(s, "Error in opening the TransactionDB");

        // Do not swap the order of the following.
        {
          db_owner_.reset(txn_db_);
          db_ = txn_db_;
          db_aptr_.store(txn_db_, std::memory_order_release);
        }
      }
    }
    DB_STRESS_ASSERT_OK_MSG(s, "Error in opening the DB");
    assert(column_families_.size() ==
           static_cast<size_t>(FLAGS_column_families));
    // Clear statistics reference from options_ to intentionally shorten the
    // statistics object lifetime to be same as the db object (which is the
    // common case in practice) and detect if RocksDB access the statistics
    // beyond its lifetime.
    if (FLAGS_statistics) {
      options_.statistics.reset();
    }

    // Secondary instance does not support write-prepared/write-unprepared
    // transactions, thus just disable secondary instance if we use
    // transaction.
    if (s.ok() && FLAGS_test_secondary && !FLAGS_use_txn) {
      Options tmp_opts;
      // TODO(yanqin) support max_open_files != -1 for secondary instance.
      tmp_opts.max_open_files = -1;
      tmp_opts.env = GetDbEnv();
      const std::string& secondary_path = GetSecondariesBase();
      s = DB::OpenAsSecondary(tmp_opts, GetDbPath(), secondary_path,
                              cf_descriptors, &secondary_cfhs_, &secondary_db_);
      DB_STRESS_ASSERT_OK(s);
      assert(secondary_cfhs_.size() ==
             static_cast<size_t>(FLAGS_column_families));
    }
  } else {
    DBWithTTL* db_with_ttl;
    s = DBWithTTL::Open(options_, GetDbPath(), &db_with_ttl, FLAGS_ttl);
    db_owner_.reset(db_with_ttl);
    db_ = db_with_ttl;
  }

  if (FLAGS_preserve_unverified_changes) {
    // Up until now, no live file should have become obsolete due to these
    // options. After `DisableFileDeletions()` we can reenable auto compactions
    // since, even if live files become obsolete, they won't be deleted.
    assert(options_.avoid_flush_during_recovery);
    assert(options_.disable_auto_compactions);
    if (s.ok()) {
      s = db_->DisableFileDeletions();
    }
    if (s.ok()) {
      s = db_->EnableAutoCompaction(column_families_);
    }
  }

  if (!s.ok()) {
    fprintf(stderr, "open error: %s\n", s.ToString().c_str());
    port::ImmediateExit(1);
  }

  if (db_->GetLatestSequenceNumber() < shared->GetPersistedSeqno()) {
    fprintf(stderr,
            "DB of latest sequence number %" PRIu64
            "did not recover to the persisted "
            "sequence number %" PRIu64 " from last DB session\n",
            db_->GetLatestSequenceNumber(), shared->GetPersistedSeqno());
    port::ImmediateExit(1);
  }
  DB_STRESS_ASSERT_OK_MSG(VerifyPersistedOptionsAfterOpen(),
                          "Persisted OPTIONS verification after open failed");
}

Status StressTest::VerifyOptionsFileStateBeforeOpen(
    const std::vector<ColumnFamilyDescriptor>& cf_descriptors, bool db_exists) {
  verify_options_manifest_tracking_ =
      options_.track_options_file_number_in_manifest;
  if (!db_exists || FLAGS_best_efforts_recovery || FLAGS_use_blob_db) {
    return Status::OK();
  }

  OptionsFileManifestState manifest_state;
  Status s = VersionSet::GetOptionsFileManifestState(
      GetDbPath(), GetDbEnv()->GetFileSystem().get(), &manifest_state);
  if (!s.ok()) {
    return s;
  }
  verify_options_manifest_tracking_ |=
      manifest_state.has_committed_options_file_number;
  if (!verify_options_manifest_tracking_ || FLAGS_use_txn) {
    return Status::OK();
  }

  std::string selected_options_file;
  s = GetLatestOptionsFileName(GetDbPath(), GetDbEnv(), &selected_options_file);
  uint64_t expected_options_file_number = 0;
  if (s.IsNotFound() && !manifest_state.has_committed_options_file_number) {
    // OPTIONS files are optional. A crash can leave a recoverable DB before
    // its first snapshot is finalized, in which case recovery selects zero.
  } else if (!s.ok()) {
    return s;
  } else {
    FileType type;
    if (!ParseFileName(selected_options_file, &expected_options_file_number,
                       &type) ||
        type != kOptionsFile) {
      return Status::Corruption("Invalid selected OPTIONS file",
                                selected_options_file);
    }
  }

  if (FLAGS_test_cf_lifecycle && expected_options_file_number != 0) {
    ConfigOptions config_options;
    config_options.env = GetDbEnv();
    config_options.ignore_unknown_options = true;
    DBOptions loaded_db_options;
    std::vector<ColumnFamilyDescriptor> loaded_cf_descriptors;
    s = LoadOptionsFromFile(config_options,
                            GetDbPath() + "/" + selected_options_file,
                            &loaded_db_options, &loaded_cf_descriptors);
    if (!s.ok()) {
      return s;
    }
    s = VerifyColumnFamilyDescriptors(
        loaded_cf_descriptors, column_family_names_,
        "Persisted OPTIONS do not match live MANIFEST column families");
    if (!s.ok()) {
      return s;
    }
  }

  DBOptions read_only_options(options_);
  read_only_options.create_if_missing = false;
  read_only_options.create_missing_column_families = false;
  read_only_options.compaction_service = nullptr;
  read_only_options.listeners.clear();
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> read_only_db;
  s = DB::OpenForReadOnly(read_only_options, GetDbPath(), cf_descriptors,
                          &handles, &read_only_db);
  if (!s.ok()) {
    return s;
  }
  auto* db_impl = static_cast_with_check<DBImpl>(read_only_db->GetRootDB());
  const uint64_t recovered_options_file_number =
      db_impl->GetVersionSet()->options_file_number();
  for (auto* handle : handles) {
    delete handle;
  }
  read_only_db.reset();
  if (recovered_options_file_number != expected_options_file_number) {
    return Status::Corruption(
        "Read-only recovery selected a different OPTIONS file",
        "utility=" + std::to_string(expected_options_file_number) +
            ", recovery=" + std::to_string(recovered_options_file_number));
  }
  return Status::OK();
}

Status StressTest::VerifyPersistedOptionsAfterOpen() {
  if (!verify_options_manifest_tracking_ || FLAGS_best_efforts_recovery ||
      FLAGS_ttl != -1 || FLAGS_use_blob_db) {
    return Status::OK();
  }

  ConfigOptions config_options;
  config_options.env = GetDbEnv();
  config_options.ignore_unknown_options = true;
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descriptors;
  Status s;
  if (FLAGS_test_cf_lifecycle) {
    std::string selected_options_file;
    s = GetLatestOptionsFileName(GetDbPath(), GetDbEnv(),
                                 &selected_options_file);
    if (s.ok()) {
      s = LoadOptionsFromFile(config_options,
                              GetDbPath() + "/" + selected_options_file,
                              &loaded_db_options, &loaded_cf_descriptors);
    }
  } else {
    s = LoadLatestOptions(config_options, GetDbPath(), &loaded_db_options,
                          &loaded_cf_descriptors);
  }
  if (!s.ok()) {
    return s;
  }
  if (!FLAGS_read_only &&
      loaded_db_options.track_options_file_number_in_manifest !=
          options_.track_options_file_number_in_manifest) {
    return Status::Corruption(
        "Latest OPTIONS file has an unexpected tracking marker");
  }

  return VerifyColumnFamilyDescriptors(
      loaded_cf_descriptors, column_family_names_,
      "Latest OPTIONS file does not match live column families");
}

void StressTest::MaybeOpenReadOnlyOnPrimary(ThreadState* thread) {
  assert(thread);
  if (FLAGS_open_read_only_one_in <= 0 || thread->tid != 0 ||
      !thread->rand.OneIn(FLAGS_open_read_only_one_in)) {
    return;
  }

  // Snapshot the current column family names for the read-only open. Other
  // worker threads can rename families via MaybeClearOneColumnFamily(), which
  // writes column_family_names_[cf] while holding ALL key locks for that CF.
  // We only need a single key lock per CF to synchronize with that writer --
  // using LockColumnFamily would acquire all key locks (potentially hundreds of
  // thousands) simultaneously, exceeding the number of locks TSAN's
  // deadlock detector can track as held per thread (128), which aborts with
  // "sanitizer_deadlock_detector.h ... n_all_locks_ < ... (0x80, 0x80)".
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  cf_descriptors.reserve(column_family_names_.size());
  for (int cf = 0; cf < static_cast<int>(column_family_names_.size()); ++cf) {
    MutexLock l(thread->shared->GetMutexForKey(cf, 0));
    cf_descriptors.emplace_back(column_family_names_[cf],
                                ColumnFamilyOptions(options_));
  }

  // A read-only instance opened concurrently with the primary can trip over
  // injected faults or files the primary mutates underneath it, so disable
  // error injection while we open and close the reader. The primary's
  // own subsequent operations run with injection re-enabled, so a genuinely
  // deleted live file still surfaces on the normal verification path.
  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->DisableAllThreadLocalErrorInjection();
  }

  std::vector<ColumnFamilyHandle*> ro_cfhs;
  std::unique_ptr<DB> ro_db;
  // A read-only reader never compacts, so it must not share the primary's
  // background-coordination services. Sharing the compaction service in
  // particular would let the reader's open/close abort the primary's in-flight
  // remote compactions, and sharing listeners would fire primary callbacks for
  // the reader instance.
  DBOptions read_only_db_options(options_);
  read_only_db_options.compaction_service = nullptr;
  read_only_db_options.listeners.clear();
  Status s = DB::OpenForReadOnly(read_only_db_options, GetDbPath(),
                                 cf_descriptors, &ro_cfhs, &ro_db);
  // Opening read-only while the primary mutates the same directory is
  // best-effort; a transient failure here is expected and not a bug.
  if (s.ok()) {
    // Create new SST files in the primary that are absent from the reader's
    // frozen live-file snapshot, so that a read-only close wrongly running
    // obsolete-file cleanup would delete these live files. Flush all column
    // families; error injection is disabled here, so the flush is expected to
    // succeed and any real failure is surfaced via ProcessStatus().
    Status flush_s = db_->Flush(FlushOptions(), column_families_);
    ProcessStatus(thread->shared, "Flush read-only-primary", flush_s);

    // Closing the reader runs the read-only close path. If it wrongly purges
    // obsolete files based on its stale snapshot, the primary's live files are
    // deleted and later detected by db_stress's read/verification paths.
    for (auto* handle : ro_cfhs) {
      delete handle;
    }
    ro_cfhs.clear();
    ro_db.reset();
  }

  if (db_fault_injection_fs_) {
    db_fault_injection_fs_->EnableAllThreadLocalErrorInjection();
  }
}

void StressTest::RecordManifestStateBeforeReopen() {
  // Determine verification mode based on flags
  if (FLAGS_best_efforts_recovery) {
    // Options are disabled in BER mode
    manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
    return;
  }

  const bool optimize_manifest = FLAGS_optimize_manifest_for_recovery;
  const bool reuse_manifest = FLAGS_reuse_manifest_on_open;

  if (!optimize_manifest && !reuse_manifest) {
    manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
    return;
  }

  if (FLAGS_metadata_write_fault_one_in != 0 ||
      FLAGS_open_metadata_write_fault_one_in != 0) {
    manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
    return;
  }

  if (reuse_manifest && optimize_manifest) {
    const bool options_manifest_tracking_active =
        options_.track_options_file_number_in_manifest;
    // Check if ALL conditions for complete avoidance are met.
    // If so, use STRICT mode where failures are fatal.
    // A WriteBufferManager limit enforced during recovery can force a flush
    // even with avoid_flush_during_recovery.
    const bool wbm_flush_possible_during_recovery =
        options_.enforce_write_buffer_manager_during_recovery &&
        (options_.db_write_buffer_size > 0 ||
         options_.write_buffer_manager != nullptr);
    const bool no_manifest_writes_expected =
        FLAGS_avoid_flush_during_recovery &&  // No flush during recovery
        !wbm_flush_possible_during_recovery &&
        !FLAGS_write_dbid_to_manifest &&    // No DB_ID write on open
        !options_manifest_tracking_active;  // No OPTIONS MANIFEST commit
    // Note: avoid_flush_during_shutdown is NOT required for STRICT mode.
    // If avoid_flush_during_shutdown=true leaves data in WAL, but
    // avoid_flush_during_recovery=true prevents flushing it, so MANIFEST
    // still won't be written. Complete avoidance is still achieved.

    if (no_manifest_writes_expected) {
      // All conditions met for complete MANIFEST write avoidance.
      // Any MANIFEST write is a bug - make it fatal.
      manifest_verify_mode_ = MANIFEST_VERIFY_STRICT;
    } else {
      // Both options enabled but some conditions prevent complete avoidance.
      // Use NO_WRITE mode with warnings.
      manifest_verify_mode_ = MANIFEST_VERIFY_NO_WRITE;
    }
  } else if (reuse_manifest) {
    manifest_verify_mode_ = MANIFEST_VERIFY_REUSE;
  } else {
    // Only optimize_manifest_for_recovery is enabled without
    // reuse_manifest_on_open. The optimize_manifest_for_recovery option reduces
    // MANIFEST writes during recovery by skipping no-op edits (edits that don't
    // change the VersionSet state). However, without reuse_manifest_on_open,
    // the MANIFEST file may still be recreated on DB open as part of the normal
    // open process. Verifying that the optimization worked (i.e., fewer edits
    // were written during recovery) would require intrusive sync points or
    // internal counters to track how many edits were skipped. Since we can't
    // easily observe this from the outside without such instrumentation, we
    // skip verification in this configuration.
    manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
    return;
  }

  // Record MANIFEST file info
  std::vector<std::string> files;
  const std::string& db_path = GetDbPath();
  Status s = Env::Default()->GetChildren(db_path, &files);
  if (!s.ok()) {
    fprintf(
        stderr,
        "Warning: Failed to list DB directory for MANIFEST verification: %s\n",
        s.ToString().c_str());
    manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
    return;
  }

  manifest_file_number_before_reopen_ = 0;
  manifest_file_size_before_reopen_ = 0;

  for (const auto& f : files) {
    if (f.find("MANIFEST-") == 0) {
      // Extract file number from MANIFEST-xxxxxx
      uint64_t number;
      FileType type;
      if (ParseFileName(f, &number, &type) && type == kDescriptorFile) {
        manifest_file_number_before_reopen_ = number;
        std::string manifest_path = db_path;
        manifest_path.append("/").append(f);
        Status file_s = Env::Default()->GetFileSize(
            manifest_path, &manifest_file_size_before_reopen_);
        if (!file_s.ok()) {
          fprintf(stderr, "Warning: Failed to get MANIFEST file size: %s\n",
                  file_s.ToString().c_str());
          manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
          return;
        }
        break;
      }
    }
  }

  // Record CURRENT file content for both REUSE_ONLY and REUSE_AND_CURRENT
  // modes. If the MANIFEST file is reused (same file number), CURRENT should
  // not be updated since it already points to the correct MANIFEST.
  std::string current_path = db_path;
  current_path.append("/CURRENT");
  s = ReadFileToString(Env::Default(), current_path,
                       &current_file_content_before_reopen_);
  if (!s.ok()) {
    fprintf(stderr, "Warning: Failed to read CURRENT file: %s\n",
            s.ToString().c_str());
    // Clear the content so we skip CURRENT verification later
    current_file_content_before_reopen_.clear();
  }
}

void StressTest::VerifyManifestNotRewritten() {
  if (manifest_verify_mode_ == MANIFEST_VERIFY_NONE) {
    return;
  }

  const bool strict_mode = (manifest_verify_mode_ == MANIFEST_VERIFY_STRICT);

  // Get current MANIFEST file info
  const std::string& db_path = GetDbPath();
  std::vector<std::string> files;
  Status s = Env::Default()->GetChildren(db_path, &files);
  if (!s.ok()) {
    fprintf(
        stderr,
        "Warning: Failed to list DB directory for MANIFEST verification: %s\n",
        s.ToString().c_str());
    manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
    return;
  }

  uint64_t current_manifest_number = 0;
  uint64_t current_manifest_size = 0;

  for (const auto& f : files) {
    if (f.find("MANIFEST-") == 0) {
      uint64_t number;
      FileType type;
      if (ParseFileName(f, &number, &type) && type == kDescriptorFile) {
        current_manifest_number = number;
        std::string manifest_path = db_path;
        manifest_path.append("/").append(f);
        Env::Default()->GetFileSize(manifest_path, &current_manifest_size);
        break;
      }
    }
  }

  // Check MANIFEST reuse
  if (current_manifest_number != manifest_file_number_before_reopen_) {
    if (strict_mode) {
      fprintf(stderr,
              "FATAL: MANIFEST file was recreated despite "
              "reuse_manifest_on_open=1 and all conditions for complete "
              "avoidance being met. "
              "Before: MANIFEST-%06" PRIu64 ", After: MANIFEST-%06" PRIu64 "\n",
              manifest_file_number_before_reopen_, current_manifest_number);
      fprintf(stderr, "This indicates a bug in the MANIFEST optimization.\n");
      exit(1);
    } else {
      fprintf(stdout,
              "WARNING: MANIFEST file was recreated despite "
              "reuse_manifest_on_open=1. "
              "Before: MANIFEST-%06" PRIu64 ", After: MANIFEST-%06" PRIu64 "\n",
              manifest_file_number_before_reopen_, current_manifest_number);
    }
  } else {
    // MANIFEST was reused, check size growth
    const uint64_t size_growth =
        (current_manifest_size > manifest_file_size_before_reopen_)
            ? (current_manifest_size - manifest_file_size_before_reopen_)
            : 0;
    if (manifest_verify_mode_ == MANIFEST_VERIFY_NO_WRITE ||
        manifest_verify_mode_ == MANIFEST_VERIFY_STRICT) {
      // In NO_WRITE/STRICT mode, we expect complete avoidance - zero growth.
      // This is the D103568447 guarantee: when both options are enabled
      // and no recovery is needed, MANIFEST should not be written at all.
      if (size_growth > 0) {
        if (strict_mode) {
          fprintf(stderr,
                  "FATAL: MANIFEST file grew by %" PRIu64
                  " bytes after reopen despite ALL conditions for complete "
                  "avoidance being met:\n"
                  "  - optimize_manifest_for_recovery=1\n"
                  "  - reuse_manifest_on_open=1\n"
                  "  - avoid_flush_during_recovery=true\n"
                  "  - write_dbid_to_manifest=0\n"
                  "  - No fault injection\n"
                  "  - Not in BER mode\n"
                  "Expected complete avoidance (0 bytes growth). "
                  "This indicates a bug in the MANIFEST optimization.\n",
                  size_growth);
          exit(1);
        } else {
          fprintf(stdout,
                  "WARNING: MANIFEST file grew by %" PRIu64
                  " bytes after reopen despite both "
                  "optimize_manifest_for_recovery=1 and "
                  "reuse_manifest_on_open=1. "
                  "Expected complete avoidance (0 bytes growth).\n",
                  size_growth);
        }
      }
    } else {
      // In REUSE mode, allow up to 10KB growth for recovery edits
      if (size_growth > 10240) {
        fprintf(stdout,
                "WARNING: MANIFEST file grew by %" PRIu64
                " bytes after reopen "
                "(may indicate unexpected writes)\n",
                size_growth);
      }
    }
  }

  // Check CURRENT file in all verification modes.
  // If the MANIFEST file was reused (same file number), CURRENT should not
  // have been modified since it already points to the correct MANIFEST.
  if (!current_file_content_before_reopen_.empty()) {
    std::string current_path = db_path;
    current_path.append("/CURRENT");
    std::string current_content;
    s = ReadFileToString(Env::Default(), current_path, &current_content);
    if (s.ok()) {
      if (current_content != current_file_content_before_reopen_) {
        if (strict_mode) {
          fprintf(
              stderr,
              "FATAL: CURRENT file was updated despite ALL conditions for "
              "complete avoidance being met. MANIFEST was reused but CURRENT "
              "was modified. This indicates a bug.\n");
          exit(1);
        } else if (manifest_verify_mode_ == MANIFEST_VERIFY_NO_WRITE) {
          fprintf(stdout,
                  "WARNING: CURRENT file was updated despite both "
                  "optimize_manifest_for_recovery=1 and "
                  "reuse_manifest_on_open=1\n");
        } else {
          fprintf(stdout,
                  "WARNING: CURRENT file was updated despite "
                  "reuse_manifest_on_open=1 and MANIFEST being reused\n");
        }
      }
    }
  }

  // Reset verification mode
  manifest_verify_mode_ = MANIFEST_VERIFY_NONE;
}

void StressTest::Reopen(ThreadState* thread) {
  // BG jobs in WritePrepared must be canceled first because i) they can
  // access the db via a callbac ii) they hold on to a snapshot and the
  // upcoming
  // ::Close would complain about it.
  const bool write_prepared = FLAGS_use_txn && FLAGS_txn_write_policy != 0;
  bool bg_canceled __attribute__((unused)) = false;
  if (write_prepared || thread->rand.OneIn(2)) {
    const bool wait =
        write_prepared || static_cast<bool>(thread->rand.OneIn(2));
    CancelAllBackgroundWork(db_, wait);
    bg_canceled = wait;
  }
  assert(!write_prepared || bg_canceled);

  CleanUpColumnFamilies();

  // Currently reopen does not restore expected state
  // with potential data loss in mind like the first open before
  // crash-recovery verification does. Therefore it always expects no data
  // loss and we should ensure no data loss in testing.
  // TODO(hx235): eliminate the FlushWAL(true /* sync */)/SyncWAL() below
  if (!FLAGS_disable_wal) {
    Status s;
    if (FLAGS_manual_wal_flush_one_in > 0) {
      s = db_->FlushWAL(/*sync=*/true);
    } else {
      s = db_->SyncWAL();
    }
    DB_STRESS_ASSERT_OK_MSG(
        s, "Error persisting WAL data which is needed before reopening the DB");
  }

  if (thread->rand.OneIn(2)) {
    Status s = db_->Close();
    DB_STRESS_ASSERT_OK_MSG(s, "Non-ok close status");
  }
  assert((txn_db_ == nullptr && optimistic_txn_db_ == nullptr) ||
         (db_ == txn_db_ || db_ == optimistic_txn_db_));
  db_owner_.reset();
  db_ = nullptr;
  txn_db_ = nullptr;
  optimistic_txn_db_ = nullptr;
  secondary_db_.reset();

  num_times_reopened_++;
  auto now = clock_->NowMicros();
  fprintf(stdout, "%s Reopening database for the %dth time\n",
          clock_->TimeToString(now / 1000000).c_str(), num_times_reopened_);

  // Record MANIFEST state before reopen for verification
  RecordManifestStateBeforeReopen();

  Open(thread->shared, /*reopen=*/true);

  // Verify MANIFEST was not unnecessarily rewritten
  VerifyManifestNotRewritten();

  if (auto_compactions_paused_for_manifest_verify_) {
    auto_compactions_paused_for_manifest_verify_ = false;
    options_.disable_auto_compactions = false;
    Status s = db_->EnableAutoCompaction(column_families_);
    DB_STRESS_ASSERT_OK_MSG(
        s, "Error re-enabling auto compactions after MANIFEST verification");
  }

  if (thread->shared->GetStressTest()->MightHaveUnsyncedDataLoss() &&
      IsStateTracked()) {
    Status s = thread->shared->SaveAtAndAfter(db_);
    DB_STRESS_ASSERT_OK_MSG(s, "Error enabling history tracing");
  }
}

bool StressTest::MaybeUseOlderTimestampForPointLookup(ThreadState* thread,
                                                      std::string& ts_str,
                                                      Slice& ts_slice,
                                                      ReadOptions& read_opts) {
  if (FLAGS_user_timestamp_size == 0) {
    return false;
  }

  if (!FLAGS_persist_user_defined_timestamps) {
    // Not read with older timestamps to avoid get InvalidArgument.
    return false;
  }

  assert(thread);
  if (!thread->rand.OneInOpt(3)) {
    return false;
  }

  const SharedState* const shared = thread->shared;
  assert(shared);
  const uint64_t start_ts = shared->GetStartTimestamp();

  uint64_t now = raw_env->NowNanos();

  assert(now > start_ts);
  uint64_t time_diff = now - start_ts;
  uint64_t ts = start_ts + (thread->rand.Next64() % time_diff);
  ts_str.clear();
  PutFixed64(&ts_str, ts);
  ts_slice = ts_str;
  read_opts.timestamp = &ts_slice;
  return true;
}

void StressTest::MaybeUseOlderTimestampForRangeScan(ThreadState* thread,
                                                    std::string& ts_str,
                                                    Slice& ts_slice,
                                                    ReadOptions& read_opts) {
  if (FLAGS_user_timestamp_size == 0) {
    return;
  }

  if (!FLAGS_persist_user_defined_timestamps) {
    // Not read with older timestamps to avoid get InvalidArgument.
    return;
  }

  assert(thread);
  if (!thread->rand.OneInOpt(3)) {
    return;
  }

  const Slice* const saved_ts = read_opts.timestamp;
  assert(saved_ts != nullptr);

  const SharedState* const shared = thread->shared;
  assert(shared);
  const uint64_t start_ts = shared->GetStartTimestamp();

  uint64_t now = raw_env->NowNanos();

  assert(now > start_ts);
  uint64_t time_diff = now - start_ts;
  uint64_t ts = start_ts + (thread->rand.Next64() % time_diff);
  ts_str.clear();
  PutFixed64(&ts_str, ts);
  ts_slice = ts_str;
  read_opts.timestamp = &ts_slice;

  // TODO (yanqin): support Merge with iter_start_ts
  if (!thread->rand.OneInOpt(3) || FLAGS_use_merge || FLAGS_use_full_merge_v1) {
    return;
  }

  ts_str.clear();
  PutFixed64(&ts_str, start_ts);
  ts_slice = ts_str;
  read_opts.iter_start_ts = &ts_slice;
  read_opts.timestamp = saved_ts;
}

void CheckAndSetOptionsForUserTimestamp(Options& options) {
  assert(FLAGS_user_timestamp_size > 0);
  const Comparator* const cmp = test::BytewiseComparatorWithU64TsWrapper();
  assert(cmp);
  if (FLAGS_user_timestamp_size != cmp->timestamp_size()) {
    fprintf(stderr,
            "Only -user_timestamp_size=%d is supported in stress test.\n",
            static_cast<int>(cmp->timestamp_size()));
    exit(1);
  }
  if (FLAGS_use_txn) {
    fprintf(stderr, "TransactionDB does not support timestamp yet.\n");
    exit(1);
  }
  if (FLAGS_test_cf_consistency || FLAGS_test_batches_snapshots) {
    fprintf(stderr,
            "Due to per-key ts-seq ordering constraint, only the (default) "
            "non-batched test is supported with timestamp.\n");
    exit(1);
  }
  if (FLAGS_ingest_external_file_one_in > 0) {
    fprintf(stderr, "Bulk loading may not support timestamp yet.\n");
    exit(1);
  }
  options.comparator = cmp;
  options.persist_user_defined_timestamps =
      FLAGS_persist_user_defined_timestamps;
}

bool ShouldDisableAutoCompactionsBeforeVerifyDb() {
  return !FLAGS_disable_auto_compactions && FLAGS_enable_compaction_filter;
}

bool InitializeOptionsFromFile(Options& options) {
  DBOptions db_options;
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.input_strings_escaped = true;
  config_options.env = raw_env;
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  if (!FLAGS_options_file.empty()) {
    Status s = LoadOptionsFromFile(config_options, FLAGS_options_file,
                                   &db_options, &cf_descriptors);
    DB_STRESS_ASSERT_OK_MSG(s, "Unable to load options file %s",
                            FLAGS_options_file.c_str());
    db_options.env = new CompositeEnvWrapper(raw_env);
    options = Options(db_options, cf_descriptors[0].options);
    return true;
  }
  return false;
}

void InitializeOptionsFromFlags(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    const std::shared_ptr<IndexFactory>& udi_factory, Options& options) {
  BlockBasedTableOptions block_based_options;
  block_based_options.decouple_partitioned_filters =
      FLAGS_decouple_partitioned_filters;
  block_based_options.block_cache = cache;
  block_based_options.cache_index_and_filter_blocks =
      FLAGS_cache_index_and_filter_blocks;
  block_based_options.metadata_cache_options.top_level_index_pinning =
      static_cast<PinningTier>(FLAGS_top_level_index_pinning);
  block_based_options.metadata_cache_options.partition_pinning =
      static_cast<PinningTier>(FLAGS_partition_pinning);
  block_based_options.metadata_cache_options.unpartitioned_pinning =
      static_cast<PinningTier>(FLAGS_unpartitioned_pinning);
  block_based_options.checksum = checksum_type_e;
  block_based_options.block_size = FLAGS_block_size;
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kCompressionDictionaryBuildingBuffer,
       {/*.charged = */ FLAGS_charge_compression_dictionary_building_buffer
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kFilterConstruction,
       {/*.charged = */ FLAGS_charge_filter_construction
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kBlockBasedTableReader,
       {/*.charged = */ FLAGS_charge_table_reader
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kFileMetadata,
       {/*.charged = */ FLAGS_charge_file_metadata
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kBlobCache,
       {/*.charged = */ FLAGS_charge_blob_cache
            ? CacheEntryRoleOptions::Decision::kEnabled
            : CacheEntryRoleOptions::Decision::kDisabled}});
  block_based_options.format_version =
      static_cast<uint32_t>(FLAGS_format_version);
  block_based_options.separate_key_value_in_data_block =
      FLAGS_separate_key_value_in_data_block;
  block_based_options.index_block_restart_interval =
      static_cast<int32_t>(FLAGS_index_block_restart_interval);
  block_based_options.filter_policy = filter_policy;
  block_based_options.partition_filters = FLAGS_partition_filters;
  block_based_options.optimize_filters_for_memory =
      FLAGS_optimize_filters_for_memory;
  block_based_options.detect_filter_construct_corruption =
      FLAGS_detect_filter_construct_corruption;
  block_based_options.index_type =
      static_cast<BlockBasedTableOptions::IndexType>(FLAGS_index_type);
  block_based_options.data_block_index_type =
      static_cast<BlockBasedTableOptions::DataBlockIndexType>(
          FLAGS_data_block_index_type);
  block_based_options.optimize_key_common_prefix =
      static_cast<BlockBasedTableOptions::OptimizeKeyCommonPrefix>(
          FLAGS_optimize_key_common_prefix);
  block_based_options.index_block_search_type =
      static_cast<BlockBasedTableOptions::BlockSearchType>(
          FLAGS_index_block_search_type);
  block_based_options.uniform_cv_threshold = FLAGS_uniform_cv_threshold;
  block_based_options.prepopulate_block_cache =
      static_cast<BlockBasedTableOptions::PrepopulateBlockCache>(
          FLAGS_prepopulate_block_cache);
  block_based_options.initial_auto_readahead_size =
      FLAGS_initial_auto_readahead_size;
  block_based_options.max_auto_readahead_size = FLAGS_max_auto_readahead_size;
  block_based_options.num_file_reads_for_auto_readahead =
      FLAGS_num_file_reads_for_auto_readahead;
  block_based_options.cache_index_and_filter_blocks_with_high_priority =
      FLAGS_cache_index_and_filter_blocks_with_high_priority;
  block_based_options.use_delta_encoding = FLAGS_use_delta_encoding;
  block_based_options.verify_compression = FLAGS_verify_compression;
  block_based_options.read_amp_bytes_per_bit = FLAGS_read_amp_bytes_per_bit;
  block_based_options.enable_index_compression = FLAGS_enable_index_compression;
  block_based_options.index_shortening =
      static_cast<BlockBasedTableOptions::IndexShorteningMode>(
          FLAGS_index_shortening);
  block_based_options.block_align = FLAGS_block_align;
  block_based_options.super_block_alignment_size =
      fLU64::FLAGS_super_block_alignment_size;
  block_based_options.super_block_alignment_space_overhead_ratio =
      fLU64::FLAGS_super_block_alignment_space_overhead_ratio;
  if (FLAGS_index_mode < 0 || FLAGS_index_mode > 4) {
    fprintf(stderr, "Invalid --index_mode=%d (must be 0..4)\n",
            FLAGS_index_mode);
    exit(1);
  }
  const BlockBasedTableOptions::IndexMode index_mode =
      static_cast<BlockBasedTableOptions::IndexMode>(FLAGS_index_mode);
  if (!udi_factory &&
      index_mode != BlockBasedTableOptions::IndexMode::kStandardOnly &&
      index_mode != BlockBasedTableOptions::IndexMode::kStandardDefault) {
    fprintf(stderr,
            "--index_mode=%d requires --use_trie_index=1; only "
            "kStandardOnly/kStandardDefault are valid without a UDI factory\n",
            FLAGS_index_mode);
    exit(1);
  }
  if (FLAGS_read_index < -1 || FLAGS_read_index > 2) {
    fprintf(stderr, "Invalid --read_index=%d (must be -1..2)\n",
            FLAGS_read_index);
    exit(1);
  }
  if (!udi_factory && FLAGS_read_index > 0) {
    // No custom index reader is ever installed without a factory, so a
    // non-default read_index would silently do nothing.
    fprintf(stderr,
            "--read_index=%d requires --use_trie_index=1; without a UDI "
            "factory only auto (-1) or kDefault (0) has any effect\n",
            FLAGS_read_index);
    exit(1);
  }
  if (FLAGS_read_index == static_cast<int>(ReadOptions::ReadIndex::kBuiltin) &&
      index_mode == BlockBasedTableOptions::IndexMode::kCustomOnly) {
    // kCustomOnly SSTs carry only a standard-index stub, so kBuiltin reads
    // fail loudly by design rather than returning an empty result.
    fprintf(stderr,
            "--read_index=1 (kBuiltin) is incompatible with --index_mode=3 "
            "(kCustomOnly); those SSTs have only a standard-index stub\n");
    exit(1);
  }
  block_based_options.index_mode = index_mode;
  if (udi_factory) {
    block_based_options.user_defined_index_factory = udi_factory;
    // Write fault injection can corrupt the UDI meta block during SST
    // creation. When reads default to the UDI (kCustomDefault, kCustomOnly), a
    // corrupted UDI block causes the reader to fail, making compaction read
    // zero keys from the affected SST and triggering a false positive in
    // record count verification. Modes that read through the standard index
    // by default are not affected.
    if ((index_mode == BlockBasedTableOptions::IndexMode::kCustomDefault ||
         index_mode == BlockBasedTableOptions::IndexMode::kCustomOnly) &&
        (FLAGS_write_fault_one_in > 0 ||
         FLAGS_metadata_write_fault_one_in > 0)) {
      options.compaction_verify_record_count = false;
    }
  }
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));
  options.db_write_buffer_size = FLAGS_db_write_buffer_size;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.max_write_buffer_number = FLAGS_max_write_buffer_number;
  options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;
  options.max_write_buffer_size_to_maintain =
      FLAGS_max_write_buffer_size_to_maintain;
  options.memtable_prefix_bloom_size_ratio =
      FLAGS_memtable_prefix_bloom_size_ratio;
  if (FLAGS_use_write_buffer_manager) {
    options.write_buffer_manager = wbm;
  }
  options.memtable_whole_key_filtering = FLAGS_memtable_whole_key_filtering;
  if (ShouldDisableAutoCompactionsBeforeVerifyDb()) {
    options.disable_auto_compactions = true;
  } else {
    options.disable_auto_compactions = FLAGS_disable_auto_compactions;
  }
  options.max_background_compactions = FLAGS_max_background_compactions;
  options.max_background_flushes = FLAGS_max_background_flushes;
  options.compaction_style =
      static_cast<ROCKSDB_NAMESPACE::CompactionStyle>(FLAGS_compaction_style);
  if (options.compaction_style ==
      ROCKSDB_NAMESPACE::CompactionStyle::kCompactionStyleFIFO) {
    options.compaction_options_fifo.allow_compaction =
        FLAGS_fifo_allow_compaction;
    if (FLAGS_fifo_compaction_max_data_files_size_mb > 0) {
      options.compaction_options_fifo.max_data_files_size =
          FLAGS_fifo_compaction_max_data_files_size_mb * 1024 * 1024;
    }
    if (FLAGS_fifo_compaction_max_table_files_size_mb > 0) {
      options.compaction_options_fifo.max_table_files_size =
          FLAGS_fifo_compaction_max_table_files_size_mb * 1024 * 1024;
    }
    options.compaction_options_fifo.use_kv_ratio_compaction =
        FLAGS_fifo_compaction_use_kv_ratio_compaction;
  }
  options.compaction_pri =
      static_cast<ROCKSDB_NAMESPACE::CompactionPri>(FLAGS_compaction_pri);
  options.num_levels = FLAGS_num_levels;
  if (FLAGS_prefix_size >= 0) {
    options.prefix_extractor.reset(NewFixedPrefixTransform(FLAGS_prefix_size));
    if (FLAGS_enable_memtable_insert_with_hint_prefix_extractor) {
      options.memtable_insert_with_hint_prefix_extractor =
          options.prefix_extractor;
    }
  }
  options.max_open_files = FLAGS_open_files;
  options.open_files_async = FLAGS_open_files_async;
  options.async_wal_precreate = FLAGS_async_wal_precreate;
  if (FLAGS_open_files_async && !FLAGS_skip_stats_update_on_db_open) {
    FLAGS_skip_stats_update_on_db_open = true;
    fprintf(stderr,
            "open_files_async requires skip_stats_update_on_db_open, "
            "enabling it automatically\n");
  }
  if (FLAGS_statistics) {
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  }
  options.env = raw_env;
  options.use_fsync = FLAGS_use_fsync;
  options.compaction_readahead_size = FLAGS_compaction_readahead_size;
  options.allow_mmap_reads = FLAGS_mmap_read;
  options.allow_mmap_writes = FLAGS_mmap_write;
  options.use_direct_reads = FLAGS_use_direct_reads;
  options.use_direct_io_for_compaction_reads =
      FLAGS_use_direct_io_for_compaction_reads;
  options.use_direct_io_for_flush_and_compaction =
      FLAGS_use_direct_io_for_flush_and_compaction;
  options.recycle_log_file_num =
      static_cast<size_t>(FLAGS_recycle_log_file_num);
  options.target_file_size_base = FLAGS_target_file_size_base;
  options.target_file_size_multiplier = FLAGS_target_file_size_multiplier;
  options.max_bytes_for_level_base = FLAGS_max_bytes_for_level_base;
  options.max_bytes_for_level_multiplier = FLAGS_max_bytes_for_level_multiplier;
  options.level0_stop_writes_trigger = FLAGS_level0_stop_writes_trigger;
  options.level0_slowdown_writes_trigger = FLAGS_level0_slowdown_writes_trigger;
  options.level0_file_num_compaction_trigger =
      FLAGS_level0_file_num_compaction_trigger;
  options.compression = compression_type_e;
  options.bottommost_compression = bottommost_compression_type_e;
  options.compression_opts.max_dict_bytes = FLAGS_compression_max_dict_bytes;
  options.compression_opts.zstd_max_train_bytes =
      FLAGS_compression_zstd_max_train_bytes;
  options.compression_opts.parallel_threads =
      FLAGS_compression_parallel_threads;
  options.compression_opts.max_dict_buffer_bytes =
      FLAGS_compression_max_dict_buffer_bytes;
  options.compression_opts.auto_skip = FLAGS_compression_auto_skip;
  options.compression_opts.auto_skip_min_sample_every =
      FLAGS_compression_auto_skip_min_sample_every;
  if (ZSTD_FinalizeDictionarySupported()) {
    options.compression_opts.use_zstd_dict_trainer =
        FLAGS_compression_use_zstd_dict_trainer;
  } else if (!FLAGS_compression_use_zstd_dict_trainer) {
    fprintf(
        stdout,
        "WARNING: use_zstd_dict_trainer is false but zstd finalizeDictionary "
        "cannot be used because ZSTD 1.4.5+ is not linked with the binary."
        " zstd dictionary trainer will be used.\n");
  }
  if (FLAGS_compression_checksum) {
    options.compression_opts.checksum = true;
  }
  options.max_manifest_file_size = FLAGS_max_manifest_file_size;
  options.max_manifest_space_amp_pct = FLAGS_max_manifest_space_amp_pct;
  options.verify_manifest_content_on_close =
      FLAGS_verify_manifest_content_on_close;
  options.max_subcompactions = static_cast<uint32_t>(FLAGS_subcompactions);
  options.allow_concurrent_memtable_write =
      FLAGS_allow_concurrent_memtable_write;
  options.experimental_mempurge_threshold =
      FLAGS_experimental_mempurge_threshold;
  options.periodic_compaction_seconds = FLAGS_periodic_compaction_seconds;
  options.daily_offpeak_time_utc = FLAGS_daily_offpeak_time_utc;
  options.stats_dump_period_sec =
      static_cast<unsigned int>(FLAGS_stats_dump_period_sec);
  options.mutex_hold_log_threshold_us = FLAGS_mutex_hold_log_threshold_us;
  options.max_compaction_trigger_wakeup_seconds =
      FLAGS_max_compaction_trigger_wakeup_seconds;
  options.periodic_compaction_phase_recovery_percent =
      FLAGS_periodic_compaction_phase_recovery_percent;
  options.ttl = FLAGS_compaction_ttl;
  options.enable_pipelined_write = FLAGS_enable_pipelined_write;
  options.enable_write_thread_adaptive_yield =
      FLAGS_enable_write_thread_adaptive_yield;
  options.compaction_options_universal.size_ratio = FLAGS_universal_size_ratio;
  options.compaction_options_universal.min_merge_width =
      FLAGS_universal_min_merge_width;
  options.compaction_options_universal.max_merge_width =
      FLAGS_universal_max_merge_width;
  options.compaction_options_universal.max_size_amplification_percent =
      FLAGS_universal_max_size_amplification_percent;
  options.compaction_options_universal.max_read_amp =
      FLAGS_universal_max_read_amp;
  options.atomic_flush = FLAGS_atomic_flush;
  options.manual_wal_flush = FLAGS_manual_wal_flush_one_in > 0 ? true : false;
  options.avoid_unnecessary_blocking_io = FLAGS_avoid_unnecessary_blocking_io;
  options.write_dbid_to_manifest = FLAGS_write_dbid_to_manifest;
  options.track_options_file_number_in_manifest =
      FLAGS_track_options_file_number_in_manifest;
  options.optimize_manifest_for_recovery = FLAGS_optimize_manifest_for_recovery;
  options.write_identity_file = FLAGS_write_identity_file;
  options.reuse_manifest_on_open = FLAGS_reuse_manifest_on_open;
  options.avoid_flush_during_recovery = FLAGS_avoid_flush_during_recovery;
  options.enforce_write_buffer_manager_during_recovery =
      FLAGS_enforce_write_buffer_manager_during_recovery;
  options.max_write_batch_group_size_bytes =
      FLAGS_max_write_batch_group_size_bytes;
  options.level_compaction_dynamic_level_bytes =
      FLAGS_level_compaction_dynamic_level_bytes;
  // File-scoped WAL recovery deliberately rejects either WAL-tracking mode.
  // Disable the normally-on stress setting only for its dedicated fault mode.
  options.track_and_verify_wals_in_manifest =
      !FLAGS_file_scope_wal_write_faults;
  options.track_and_verify_wals = FLAGS_track_and_verify_wals;
  options.verify_sst_unique_id_in_manifest =
      FLAGS_verify_sst_unique_id_in_manifest;
  options.fast_sst_open = FLAGS_fast_sst_open;
  options.memtable_protection_bytes_per_key =
      FLAGS_memtable_protection_bytes_per_key;
  options.block_protection_bytes_per_key = FLAGS_block_protection_bytes_per_key;
  options.verify_output_flags =
      static_cast<VerifyOutputFlags>(FLAGS_verify_output_flags);
  options.paranoid_memory_checks = FLAGS_paranoid_memory_checks;
  options.memtable_verify_per_key_checksum_on_seek =
      FLAGS_memtable_verify_per_key_checksum_on_seek;
  options.memtable_batch_lookup_optimization =
      FLAGS_memtable_batch_lookup_optimization;

  // Integrated BlobDB
  options.enable_blob_files = FLAGS_enable_blob_files;
  options.enable_blob_direct_write = FLAGS_enable_blob_direct_write;
  options.blob_direct_write_partitions =
      static_cast<uint32_t>(FLAGS_blob_direct_write_partitions);
  options.min_blob_size = FLAGS_min_blob_size;
  options.blob_file_size = FLAGS_blob_file_size;
  options.blob_file_writable_file_max_buffer_size =
      FLAGS_blob_file_writable_file_max_buffer_size;
  options.blob_compression_type =
      StringToCompressionType(FLAGS_blob_compression_type.c_str());
  options.enable_blob_garbage_collection = FLAGS_enable_blob_garbage_collection;
  options.blob_garbage_collection_age_cutoff =
      FLAGS_blob_garbage_collection_age_cutoff;
  options.blob_garbage_collection_force_threshold =
      FLAGS_blob_garbage_collection_force_threshold;
  options.blob_compaction_readahead_size = FLAGS_blob_compaction_readahead_size;
  options.blob_file_starting_level = FLAGS_blob_file_starting_level;
  options.read_triggered_compaction_threshold =
      FLAGS_read_triggered_compaction_threshold;

  if (FLAGS_use_blob_cache) {
    if (FLAGS_use_shared_block_and_blob_cache) {
      options.blob_cache = cache;
    } else {
      if (FLAGS_blob_cache_size > 0) {
        LRUCacheOptions co;
        co.capacity = FLAGS_blob_cache_size;
        co.num_shard_bits = FLAGS_blob_cache_numshardbits;
        options.blob_cache = NewLRUCache(co);
      } else {
        fprintf(stderr,
                "Unable to create a standalone blob cache if blob_cache_size "
                "<= 0.\n");
        exit(1);
      }
    }
    switch (FLAGS_prepopulate_blob_cache) {
      case 0:
        options.prepopulate_blob_cache = PrepopulateBlobCache::kDisable;
        break;
      case 1:
        options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
        break;
      default:
        fprintf(stderr, "Unknown prepopulate blob cache mode\n");
        exit(1);
    }
  }

  options.wal_compression =
      StringToCompressionType(FLAGS_wal_compression.c_str());

  options.last_level_temperature =
      StringToTemperature(FLAGS_last_level_temperature.c_str());
  options.default_write_temperature =
      StringToTemperature(FLAGS_default_write_temperature.c_str());
  options.default_temperature =
      StringToTemperature(FLAGS_default_temperature.c_str());

  if (!FLAGS_file_temperature_age_thresholds.empty()) {
    const std::string allowTrivialCopyBoolStr =
        FLAGS_allow_trivial_copy_when_change_temperature ? "true" : "false";
    Status s = GetColumnFamilyOptionsFromString(
        {}, options,
        "compaction_options_fifo={file_temperature_age_thresholds=" +
            FLAGS_file_temperature_age_thresholds +
            ";allow_trivial_copy_when_change_temperature=" +
            allowTrivialCopyBoolStr + "}",
        &options);
    DB_STRESS_ASSERT_OK_MSG(s, "While setting file_temperature_age_thresholds");
  }
  // NOTE: allow -1 to mean starting disabled but dynamically changing
  options.preclude_last_level_data_seconds =
      std::max(FLAGS_preclude_last_level_data_seconds, int64_t{0});
  options.preserve_internal_time_seconds = FLAGS_preserve_internal_time_seconds;

  switch (FLAGS_rep_factory) {
    case kSkipList:
      // no need to do anything
      break;
    case kHashSkipList:
      options.memtable_factory.reset(NewHashSkipListRepFactory(10000));
      break;
    case kVectorRep:
      options.memtable_factory.reset(new VectorRepFactory());
      break;
  }

  InitializeMergeOperator(options);

  if (FLAGS_enable_compaction_filter) {
    options.compaction_filter_factory =
        std::make_shared<DbStressCompactionFilterFactory>();
  }

  options.best_efforts_recovery = FLAGS_best_efforts_recovery;
  options.paranoid_file_checks = FLAGS_paranoid_file_checks;

  if (FLAGS_user_timestamp_size > 0) {
    CheckAndSetOptionsForUserTimestamp(options);
  }

  options.allow_data_in_errors = FLAGS_allow_data_in_errors;

  options.enable_thread_tracking = FLAGS_enable_thread_tracking;

  options.memtable_max_range_deletions = FLAGS_memtable_max_range_deletions;

  options.bottommost_file_compaction_delay =
      FLAGS_bottommost_file_compaction_delay;

  options.allow_fallocate = FLAGS_allow_fallocate;
  options.table_cache_numshardbits = FLAGS_table_cache_numshardbits;
  options.log_readahead_size = FLAGS_log_readahead_size;
  options.bgerror_resume_retry_interval = FLAGS_bgerror_resume_retry_interval;
  options.delete_obsolete_files_period_micros =
      FLAGS_delete_obsolete_files_period_micros;
  options.max_log_file_size = FLAGS_max_log_file_size;
  options.log_file_time_to_roll = FLAGS_log_file_time_to_roll;
  options.use_adaptive_mutex = FLAGS_use_adaptive_mutex;
  options.advise_random_on_open = FLAGS_advise_random_on_open;
  // TODO (hx235): test the functionality of `WAL_ttl_seconds`,
  // `WAL_size_limit_MB` i.e, `GetUpdatesSince()`
  options.WAL_ttl_seconds = FLAGS_WAL_ttl_seconds;
  options.WAL_size_limit_MB = FLAGS_WAL_size_limit_MB;
  options.wal_bytes_per_sync = FLAGS_wal_bytes_per_sync;
  options.strict_bytes_per_sync = FLAGS_strict_bytes_per_sync;
  options.avoid_flush_during_shutdown = FLAGS_avoid_flush_during_shutdown;
  options.dump_malloc_stats = FLAGS_dump_malloc_stats;
  options.stats_history_buffer_size = FLAGS_stats_history_buffer_size;
  options.skip_stats_update_on_db_open = FLAGS_skip_stats_update_on_db_open;
  options.optimize_filters_for_hits = FLAGS_optimize_filters_for_hits;
  options.sample_for_compression = FLAGS_sample_for_compression;
  options.report_bg_io_stats = FLAGS_report_bg_io_stats;
  options.manifest_preallocation_size = FLAGS_manifest_preallocation_size;
  if (FLAGS_enable_checksum_handoff) {
    options.checksum_handoff_file_types = {FileTypeSet::All()};
  } else {
    options.checksum_handoff_file_types = {};
  }
  options.max_total_wal_size = FLAGS_max_total_wal_size;
  options.soft_pending_compaction_bytes_limit =
      FLAGS_soft_pending_compaction_bytes_limit;
  options.hard_pending_compaction_bytes_limit =
      FLAGS_hard_pending_compaction_bytes_limit;
  options.max_sequential_skip_in_iterations =
      FLAGS_max_sequential_skip_in_iterations;
  if (FLAGS_enable_sst_partitioner_factory) {
    options.sst_partitioner_factory = std::shared_ptr<SstPartitionerFactory>(
        NewSstPartitionerFixedPrefixFactory(1));
  }
  options.lowest_used_cache_tier =
      static_cast<CacheTier>(FLAGS_lowest_used_cache_tier);
  options.inplace_update_support = FLAGS_inplace_update_support;
  options.uncache_aggressiveness = FLAGS_uncache_aggressiveness;

  options.memtable_op_scan_flush_trigger = FLAGS_memtable_op_scan_flush_trigger;
  options.min_tombstones_for_range_conversion =
      FLAGS_min_tombstones_for_range_conversion;
  options.compaction_options_universal.reduce_file_locking =
      FLAGS_universal_reduce_file_locking;
}

void InitializeOptionsGeneral(
    const std::shared_ptr<Cache>& cache,
    const std::shared_ptr<const FilterPolicy>& filter_policy,
    const std::shared_ptr<SstQueryFilterConfigsManager::Factory>& sqfc_factory,
    Options& options) {
  options.create_missing_column_families = true;
  options.create_if_missing = true;
#if USE_COROUTINES
  if (FLAGS_use_async_db_api) {
    options.read_io_executor_threads = 8;
  }
#endif  // USE_COROUTINES

  if (FLAGS_statistics) {
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  }

  if (options.env == Options().env) {
    options.env = raw_env;
  }

  assert(options.table_factory);
  auto table_options =
      options.table_factory->GetOptions<BlockBasedTableOptions>();
  if (table_options) {
    if (FLAGS_cache_size > 0) {
      table_options->block_cache = cache;
    }
    if (!table_options->filter_policy) {
      table_options->filter_policy = filter_policy;
    }
  }

  // TODO: row_cache, thread-pool IO priority, CPU priority.

  if (!options.rate_limiter && rate_limiter) {
    options.rate_limiter = rate_limiter;
  }

  if (!options.file_checksum_gen_factory) {
    options.file_checksum_gen_factory =
        GetFileChecksumImpl(FLAGS_file_checksum_impl);
  }

  if (FLAGS_preserve_unverified_changes) {
    if (!options.avoid_flush_during_recovery) {
      fprintf(stderr,
              "WARNING: flipping `avoid_flush_during_recovery` to true for "
              "`preserve_unverified_changes` to keep all files\n");
      options.avoid_flush_during_recovery = true;
    }
    if (options.enforce_write_buffer_manager_during_recovery) {
      fprintf(
          stderr,
          "WARNING: flipping `enforce_write_buffer_manager_during_recovery` "
          "to false for `preserve_unverified_changes` to keep all files\n");
      options.enforce_write_buffer_manager_during_recovery = false;
    }
    // Together with `avoid_flush_during_recovery == true` and
    // `enforce_write_buffer_manager_during_recovery == false`, this will
    // prevent live files from becoming obsolete and deleted between
    // `DB::Open()` and `DisableFileDeletions()` due to flush or compaction.
    // We do not need to warn the user since we will reenable compaction soon.
    options.disable_auto_compactions = true;
  }

  if (FLAGS_sst_file_manager_bytes_per_sec > 0 ||
      FLAGS_sst_file_manager_bytes_per_truncate > 0) {
    Status status;
    options.sst_file_manager.reset(NewSstFileManager(
        raw_env, options.info_log, "" /* trash_dir */,
        static_cast<int64_t>(FLAGS_sst_file_manager_bytes_per_sec),
        true /* delete_existing_trash */, &status,
        0.25 /* max_trash_db_ratio */,
        FLAGS_sst_file_manager_bytes_per_truncate));
    DB_STRESS_ASSERT_OK_MSG(status, "SstFileManager creation failed");
  }

  options.table_properties_collector_factories.clear();
  options.table_properties_collector_factories.emplace_back(
      std::make_shared<DbStressTablePropertiesCollectorFactory>());

  if (sqfc_factory && !sqfc_factory->GetConfigs().IsEmptyNotFound()) {
    options.table_properties_collector_factories.emplace_back(sqfc_factory);
  }

  // Add CompactOnDeletionCollectorFactory if enabled
  if (FLAGS_enable_compaction_on_deletion_trigger) {
    options.table_properties_collector_factories.emplace_back(
        ROCKSDB_NAMESPACE::NewCompactOnDeletionCollectorFactory(
            FLAGS_compaction_on_deletion_window_size,
            FLAGS_compaction_on_deletion_trigger_count,
            FLAGS_compaction_on_deletion_ratio,
            FLAGS_compaction_on_deletion_min_file_size));
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
