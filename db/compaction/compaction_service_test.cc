//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "db/blob/blob_log_sequential_reader.h"
#include "db/compaction/compaction_outputs.h"
#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "port/stack_trace.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_util.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/unique_id_impl.h"
#include "test_util/mock_time_env.h"
#include "util/cast_util.h"
#include "util/defer.h"
#include "utilities/merge_operators/string_append/stringappend.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Append ":extra_value" to every "counts=...;" field in a serialized
// CompactionServiceResult string to simulate a worker with more
// CompactionReason enums than the reader. Loops because the serialized
// result contains multiple CompactionStats structs (output_level_stats,
// proximal_level_stats), each with its own "counts=" field.
void AppendToCountsFields(std::string* s, const std::string& extra_value) {
  const std::string prefix = "counts=";
  size_t pos = s->find(prefix);
  while (pos != std::string::npos) {
    // Find the semicolon that terminates this field's value.
    size_t semi = s->find(';', pos + prefix.size());
    assert(semi != std::string::npos);
    // Insert ":extra_value" just before the semicolon.
    s->insert(semi, ":" + extra_value);
    // Advance past the inserted text to find the next occurrence.
    pos = s->find(prefix, semi + extra_value.size() + 2);
  }
}

// Remove the last colon-separated element from every "counts=...;" field
// to simulate a worker with fewer CompactionReason enums than the reader.
// Loops for the same reason as AppendToCountsFields (multiple counts fields).
void RemoveLastFromCountsFields(std::string* s) {
  const std::string prefix = "counts=";
  size_t pos = s->find(prefix);
  while (pos != std::string::npos) {
    size_t val_begin = pos + prefix.size();
    size_t semi = s->find(';', val_begin);
    assert(semi != std::string::npos);
    // Find the last ':' separator within the value.
    size_t last_sep = s->rfind(':', semi - 1);
    assert(last_sep != std::string::npos && last_sep >= val_begin);
    // Erase from the last ':' up to (not including) the ';'.
    // e.g. "counts=0:3:5;" -> erase ":5" -> "counts=0:3;"
    s->erase(last_sep, semi - last_sep);
    pos = s->find(prefix, last_sep);
  }
}

// Count the colon-separated elements in the first "counts=...;" field of a
// serialized CompactionServiceResult. Used to assert the serialized width
// (how many CompactionReason counts this binary writes).
size_t CountFirstCountsFieldElements(const std::string& s) {
  const std::string prefix = "counts=";
  size_t pos = s.find(prefix);
  assert(pos != std::string::npos);
  size_t val_begin = pos + prefix.size();
  size_t semi = s.find(';', val_begin);
  assert(semi != std::string::npos);
  if (semi == val_begin) {
    return 0;
  }
  size_t count = 1;
  for (size_t i = val_begin; i < semi; ++i) {
    if (s[i] == ':') {
      ++count;
    }
  }
  return count;
}

// Simulates a filesystem without POSIX read-after-unlink semantics (a remote
// or object store): once the primary renames a freshly written CURRENT over
// the old one, a reader that already opened the previous CURRENT object can no
// longer read it and the filesystem reports the path as gone rather than
// serving the unlinked object. Fails the first `fail_times` reads of a CURRENT
// file that way; later reads succeed, as they do once the reader picks up the
// replacement.
class VanishingCurrentFileFS : public FileSystemWrapper {
 public:
  explicit VanishingCurrentFileFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "VanishingCurrentFileFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewSequentialFile(const std::string& fname, const FileOptions& opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override {
    IOStatus io_s = target()->NewSequentialFile(fname, opts, result, dbg);
    if (io_s.ok() && IsCurrentFileName(fname)) {
      *result = std::make_unique<MaybeVanishedFile>(std::move(*result), this);
    }
    return io_s;
  }

  void SetFailuresToInject(int failures) {
    failures_to_inject_.store(failures);
  }

  int injected_failures() const { return injected_failures_.load(); }

 private:
  static bool IsCurrentFileName(const std::string& fname) {
    const std::string suffix = "/" + kCurrentFileName;
    return fname.size() > suffix.size() &&
           fname.compare(fname.size() - suffix.size(), suffix.size(), suffix) ==
               0;
  }

  // Returns true (consuming one injection) while failures remain.
  bool ConsumeFailure() {
    int remaining = failures_to_inject_.load();
    while (remaining > 0) {
      if (failures_to_inject_.compare_exchange_weak(remaining, remaining - 1)) {
        injected_failures_.fetch_add(1);
        return true;
      }
    }
    return false;
  }

  class MaybeVanishedFile : public FSSequentialFileOwnerWrapper {
   public:
    MaybeVanishedFile(std::unique_ptr<FSSequentialFile>&& target,
                      VanishingCurrentFileFS* fs)
        : FSSequentialFileOwnerWrapper(std::move(target)), fs_(fs) {}

    IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                  char* scratch, IODebugContext* dbg) override {
      if (fs_->ConsumeFailure()) {
        return IOStatus::PathNotFound(
            "Failed to read CURRENT: the object was replaced and is no longer "
            "readable");
      }
      return FSSequentialFileOwnerWrapper::Read(n, options, result, scratch,
                                                dbg);
    }

   private:
    VanishingCurrentFileFS* fs_;
  };

  std::atomic<int> failures_to_inject_{0};
  std::atomic<int> injected_failures_{0};
};

struct RetryCloseTableFactoryState {
  std::atomic<bool> enabled{false};
  std::atomic<int> cut_requests{0};
};

class RetryCloseTableBuilder : public TableBuilder {
 public:
  RetryCloseTableBuilder(std::unique_ptr<TableBuilder> base_builder,
                         std::string cut_before_key,
                         std::shared_ptr<RetryCloseTableFactoryState> state)
      : base_builder_(std::move(base_builder)),
        cut_before_key_(std::move(cut_before_key)),
        state_(std::move(state)) {}

  void Add(const Slice& key, const Slice& value) override {
    base_builder_->Add(key, value);
  }

  TableBuilderAddResult TryAdd(const Slice& key, const Slice& value,
                               TableBuilderAddContext* context) override {
    int expected_cut_requests = 0;
    if (state_->enabled.load() && !base_builder_->IsEmpty() &&
        ExtractUserKey(key).compare(Slice(cut_before_key_)) == 0 &&
        state_->cut_requests.compare_exchange_strong(expected_cut_requests,
                                                     1)) {
      context->new_table_reason = TableBuilderNewTableReason::kBuilderPolicy;
      return TableBuilderAddResult::kRequiresNewTable;
    }
    return base_builder_->TryAdd(key, value, context);
  }

  Status status() const override { return base_builder_->status(); }
  IOStatus io_status() const override { return base_builder_->io_status(); }
  Status Finish() override { return base_builder_->Finish(); }
  void Abandon() override { base_builder_->Abandon(); }
  uint64_t NumEntries() const override { return base_builder_->NumEntries(); }
  bool IsEmpty() const override { return base_builder_->IsEmpty(); }
  uint64_t PreCompressionSize() const override {
    return base_builder_->PreCompressionSize();
  }
  uint64_t FileSize() const override { return base_builder_->FileSize(); }
  uint64_t EstimatedFileSize() const override {
    return base_builder_->EstimatedFileSize();
  }
  uint64_t EstimatedTailSize() const override {
    return base_builder_->EstimatedTailSize();
  }
  uint64_t GetTailSize() const override { return base_builder_->GetTailSize(); }
  bool NeedCompact() const override { return base_builder_->NeedCompact(); }
  TableProperties GetTableProperties() const override {
    return base_builder_->GetTableProperties();
  }
  std::string GetFileChecksum() const override {
    return base_builder_->GetFileChecksum();
  }
  const char* GetFileChecksumFuncName() const override {
    return base_builder_->GetFileChecksumFuncName();
  }
  void SetSeqnoTimeTableProperties(const SeqnoToTimeMapping& mapping,
                                   uint64_t oldest_ancestor_time) override {
    base_builder_->SetSeqnoTimeTableProperties(mapping, oldest_ancestor_time);
  }
  uint64_t GetWorkerCPUMicros() const override {
    return base_builder_->GetWorkerCPUMicros();
  }

 private:
  std::unique_ptr<TableBuilder> base_builder_;
  std::string cut_before_key_;
  std::shared_ptr<RetryCloseTableFactoryState> state_;
};

class RetryCloseTableFactory : public TableFactory {
 public:
  RetryCloseTableFactory(std::string cut_before_key,
                         std::shared_ptr<RetryCloseTableFactoryState> state)
      : cut_before_key_(std::move(cut_before_key)), state_(std::move(state)) {}

  const char* Name() const override { return "RetryCloseTableFactory"; }
  using TableFactory::NewTableReader;

  Status NewTableReader(
      const ReadOptions& read_options,
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache) const override {
    return base_factory_.NewTableReader(
        read_options, table_reader_options, std::move(file), file_size,
        table_reader, prefetch_index_and_filter_in_cache);
  }

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override {
    return new RetryCloseTableBuilder(
        std::unique_ptr<TableBuilder>(
            base_factory_.NewTableBuilder(table_builder_options, file)),
        cut_before_key_, state_);
  }

  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<RetryCloseTableFactory>(cut_before_key_, state_);
  }
  bool IsDeleteRangeSupported() const override {
    return base_factory_.IsDeleteRangeSupported();
  }
  std::string GetPrintableOptions() const override {
    return base_factory_.GetPrintableOptions();
  }

 private:
  std::string cut_before_key_;
  std::shared_ptr<RetryCloseTableFactoryState> state_;
  BlockBasedTableFactory base_factory_;
};

}  // namespace

class MyTestCompactionService : public CompactionService {
 public:
  MyTestCompactionService(
      std::string db_path, Options& options,
      std::shared_ptr<Statistics>& statistics,
      std::vector<std::shared_ptr<EventListener>> listeners,
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
          table_properties_collector_factories)
      : db_path_(std::move(db_path)),
        statistics_(statistics),
        options_(options),
        start_info_("na", "na", "na", 0, "na", 0, Env::TOTAL,
                    CompactionReason::kUnknown, false, false, false, -1, -1),
        wait_info_("na", "na", "na", 0, "na", 0, Env::TOTAL,
                   CompactionReason::kUnknown, false, false, false, -1, -1),
        listeners_(std::move(listeners)),
        table_properties_collector_factories_(
            std::move(table_properties_collector_factories)) {}

  static const char* kClassName() { return "MyTestCompactionService"; }

  const char* Name() const override { return kClassName(); }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info,
      const std::string& compaction_service_input) override {
    InstrumentedMutexLock l(&mutex_);
    start_info_ = info;
    last_input_ = compaction_service_input;
    inputs_.push_back(compaction_service_input);
    assert(info.db_name == db_path_);
    std::string unique_id = Env::Default()->GenerateUniqueId();
    last_output_directory_name_ = unique_id;
    jobs_.emplace(unique_id, compaction_service_input);
    infos_.emplace(unique_id, info);
    CompactionServiceScheduleResponse response(
        unique_id, is_override_start_status_
                       ? override_start_status_
                       : CompactionServiceJobStatus::kSuccess);
    return response;
  }

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    std::string compaction_input;
    {
      InstrumentedMutexLock l(&mutex_);
      auto job_index = jobs_.find(scheduled_job_id);
      if (job_index == jobs_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      compaction_input = std::move(job_index->second);
      jobs_.erase(job_index);

      auto info_index = infos_.find(scheduled_job_id);
      if (info_index == infos_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      wait_info_ = std::move(info_index->second);
      infos_.erase(info_index);
    }
    if (is_override_wait_status_) {
      return override_wait_status_;
    }

    CompactionServiceOptionsOverride options_override = GetOptionsOverride();

    OpenAndCompactOptions options;
    options.canceled = &canceled_;
    options.max_secondary_open_retries = max_secondary_open_retries_;

    TEST_SYNC_POINT("MyTestCompactionService::Wait:BeforeOpenAndCompact");
    TEST_SYNC_POINT("MyTestCompactionService::Wait:Continue");

    Status s = DB::OpenAndCompact(options, db_path_, scheduled_job_id,
                                  compaction_input, result, options_override);
    if (wait_result_mutator_) {
      wait_result_mutator_(result);
    }
    {
      InstrumentedMutexLock l(&mutex_);
      if (is_override_wait_result_) {
        *result = override_wait_result_;
      }
      result_ = *result;
      results_.push_back(*result);
    }
    compaction_num_.fetch_add(1);
    if (s.ok()) {
      return CompactionServiceJobStatus::kSuccess;
    } else if (s.IsIncomplete() && !s.IsManualCompactionPaused()) {
      // The remote worker declined the job (e.g. the MANIFEST floor check
      // rejected an older/stale filesystem view). Mapping that decline to
      // kUseLocal (retry locally) vs. kFailure (surface it) is the integrator's
      // choice; this example picks kUseLocal to exercise the fallback path,
      // while DbStressCompactionService deliberately picks kFailure. Tests can
      // select the latter behavior here as well. Real cancellation
      // (kManualCompactionPaused) is handled as a failure.
      return return_incomplete_as_failure_
                 ? CompactionServiceJobStatus::kFailure
                 : CompactionServiceJobStatus::kUseLocal;
    } else {
      return CompactionServiceJobStatus::kFailure;
    }
  }

  CompactionServiceOptionsOverride GetOptionsOverride() {
    CompactionServiceOptionsOverride options_override;
    options_override.env = options_.env;
    options_override.file_checksum_gen_factory =
        options_.file_checksum_gen_factory;
    options_override.comparator = options_.comparator;
    options_override.merge_operator = options_.merge_operator;
    options_override.compaction_filter = options_.compaction_filter;
    options_override.compaction_filter_factory =
        options_.compaction_filter_factory;
    options_override.prefix_extractor = options_.prefix_extractor;
    options_override.table_factory = options_.table_factory;
    options_override.sst_partitioner_factory = options_.sst_partitioner_factory;
    options_override.statistics = statistics_;
    options_override.info_log = options_.info_log;
    options_override.options_map = worker_options_override_;
    if (!listeners_.empty()) {
      options_override.listeners = listeners_;
    }

    if (!table_properties_collector_factories_.empty()) {
      options_override.table_properties_collector_factories =
          table_properties_collector_factories_;
    }
    return options_override;
  }

  void CancelAwaitingJobs() override { canceled_ = true; }

  void OnInstallation(const std::string& /*scheduled_job_id*/,
                      CompactionServiceJobStatus status) override {
    installation_callback_count_.fetch_add(1);
    if (status == CompactionServiceJobStatus::kUseLocal) {
      use_local_installation_callback_count_.fetch_add(1);
    }
    final_updated_status_ = status;
  }

  int GetCompactionNum() { return compaction_num_.load(); }

  std::string GetLastOutputDirectoryName() {
    InstrumentedMutexLock l(&mutex_);
    return last_output_directory_name_;
  }

  CompactionServiceJobInfo GetCompactionInfoForStart() { return start_info_; }
  CompactionServiceJobInfo GetCompactionInfoForWait() { return wait_info_; }

  void OverrideStartStatus(CompactionServiceJobStatus s) {
    is_override_start_status_ = true;
    override_start_status_ = s;
  }

  void OverrideWaitStatus(CompactionServiceJobStatus s) {
    is_override_wait_status_ = true;
    override_wait_status_ = s;
  }

  void OverrideWaitResult(std::string str) {
    is_override_wait_result_ = true;
    override_wait_result_ = std::move(str);
  }

  void SetWaitResultMutator(std::function<void(std::string*)> mutator) {
    wait_result_mutator_ = std::move(mutator);
  }

  void ResetOverride() {
    is_override_wait_result_ = false;
    is_override_start_status_ = false;
    is_override_wait_status_ = false;
  }

  void SetCanceled(bool canceled) { canceled_ = canceled; }
  bool GetCanceled() { return canceled_; }

  void SetMaxSecondaryOpenRetries(uint32_t retries) {
    max_secondary_open_retries_ = retries;
  }

  void ReturnIncompleteAsFailure(bool value) {
    return_incomplete_as_failure_ = value;
  }

  void SetWorkerOptionOverride(const std::string& name,
                               const std::string& value) {
    worker_options_override_[name] = value;
  }

  void GetResult(CompactionServiceResult* deserialized) {
    CompactionServiceResult::Read(result_, deserialized).PermitUncheckedError();
  }

  void GetInput(CompactionServiceInput* deserialized) {
    InstrumentedMutexLock l(&mutex_);
    CompactionServiceInput::Read(last_input_, deserialized)
        .PermitUncheckedError();
  }

  std::vector<CompactionServiceInput> GetInputs() {
    InstrumentedMutexLock l(&mutex_);
    std::vector<CompactionServiceInput> inputs;
    inputs.reserve(inputs_.size());
    for (const auto& serialized : inputs_) {
      inputs.emplace_back();
      CompactionServiceInput::Read(serialized, &inputs.back())
          .PermitUncheckedError();
    }
    return inputs;
  }

  std::vector<CompactionServiceResult> GetResults() {
    InstrumentedMutexLock l(&mutex_);
    std::vector<CompactionServiceResult> results;
    results.reserve(results_.size());
    for (const auto& serialized : results_) {
      results.emplace_back();
      CompactionServiceResult::Read(serialized, &results.back())
          .PermitUncheckedError();
      results.back().status.PermitUncheckedError();
    }
    return results;
  }

  CompactionServiceJobStatus GetFinalCompactionServiceJobStatus() {
    return final_updated_status_.load();
  }

  int GetOnInstallationCount() { return installation_callback_count_.load(); }

  int GetUseLocalInstallationCount() {
    return use_local_installation_callback_count_.load();
  }

 protected:
  InstrumentedMutex mutex_;
  const std::string db_path_;
  std::shared_ptr<Statistics> statistics_;
  std::map<std::string, std::string> jobs_;
  std::map<std::string, CompactionServiceJobInfo> infos_;
  std::string result_;
  std::vector<std::string> results_;
  std::string last_input_;
  std::vector<std::string> inputs_;
  std::string last_output_directory_name_;

  std::string GetOutputPath(const std::string& scheduled_job_id) {
    return RemoteCompactionJobDir(
        db_path_, options_.use_session_tmp_dir_for_remote_compaction,
        scheduled_job_id);
  }

 private:
  std::atomic_int compaction_num_{0};
  Options options_;
  CompactionServiceJobInfo start_info_;
  CompactionServiceJobInfo wait_info_;
  bool is_override_start_status_ = false;
  CompactionServiceJobStatus override_start_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_status_ = false;
  CompactionServiceJobStatus override_wait_status_ =
      CompactionServiceJobStatus::kFailure;
  bool is_override_wait_result_ = false;
  std::string override_wait_result_;
  std::function<void(std::string*)> wait_result_mutator_;
  bool return_incomplete_as_failure_ = false;
  std::unordered_map<std::string, std::string> worker_options_override_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      table_properties_collector_factories_;
  std::atomic_int installation_callback_count_{0};
  std::atomic_int use_local_installation_callback_count_{0};
  std::atomic<CompactionServiceJobStatus> final_updated_status_{
      CompactionServiceJobStatus::kUseLocal};
  uint32_t max_secondary_open_retries_ =
      OpenAndCompactOptions().max_secondary_open_retries;

 protected:
  std::atomic_bool canceled_{false};
};

class CompactionServiceTest : public DBTestBase {
 public:
  explicit CompactionServiceTest()
      : DBTestBase("compaction_service_test", true) {}

 protected:
  void ReopenWithCompactionService(Options* options) {
    options->env = env_;
    primary_statistics_ = CreateDBStatistics();
    options->statistics = primary_statistics_;
    compactor_statistics_ = CreateDBStatistics();

    auto my_cs = std::make_shared<MyTestCompactionService>(
        dbname_, *options, compactor_statistics_, remote_listeners,
        remote_table_properties_collector_factories);

    compaction_service_ = my_cs;
    options->compaction_service = compaction_service_;
    DestroyAndReopen(*options);
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, *options);
    my_cs->SetCanceled(false);
  }

  Statistics* GetCompactorStatistics() { return compactor_statistics_.get(); }

  Statistics* GetPrimaryStatistics() { return primary_statistics_.get(); }

  MyTestCompactionService* GetCompactionService() {
    CompactionService* cs = compaction_service_.get();
    return static_cast_with_check<MyTestCompactionService>(cs);
  }

  void GenerateTestData(bool move_files_manually = false) {
    // Generate 20 files @ L2 Per CF
    for (int cf_id = 0; cf_id < static_cast<int>(handles_.size()); cf_id++) {
      for (int i = 0; i < 20; i++) {
        for (int j = 0; j < 10; j++) {
          int key_id = i * 10 + j;
          ASSERT_OK(Put(cf_id, Key(key_id), "value" + std::to_string(key_id)));
        }
        ASSERT_OK(Flush(cf_id));
      }
      if (move_files_manually) {
        MoveFilesToLevel(2, cf_id);
      }

      // Generate 10 files @ L1 overlap with all 20 files @ L2
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
          int key_id = i * 20 + j * 2;
          ASSERT_OK(
              Put(cf_id, Key(key_id), "value_new" + std::to_string(key_id)));
        }
        ASSERT_OK(Flush(cf_id));
      }
      if (move_files_manually) {
        MoveFilesToLevel(1, cf_id);
        ASSERT_EQ(FilesPerLevel(cf_id), "0,10,20");
      }
    }
  }

  void VerifyTestData() {
    for (int cf_id = 0; cf_id < static_cast<int>(handles_.size()); cf_id++) {
      for (int i = 0; i < 200; i++) {
        auto result = Get(cf_id, Key(i));
        if (i % 2) {
          ASSERT_EQ(result, "value" + std::to_string(i));
        } else {
          ASSERT_EQ(result, "value_new" + std::to_string(i));
        }
      }
    }
  }

  Status ReadBlobFileRecords(uint64_t blob_file_number, size_t num_records,
                             std::vector<BlobLogRecord>* records) {
    assert(records != nullptr);
    records->clear();

    std::unique_ptr<FSRandomAccessFile> file;
    FileOptions file_options;
    constexpr IODebugContext* dbg = nullptr;
    const std::string blob_file_path = BlobFileName(dbname_, blob_file_number);
    FileSystem* const fs = env_->GetFileSystem().get();
    SystemClock* const clock = env_->GetSystemClock().get();
    Status s =
        fs->NewRandomAccessFile(blob_file_path, file_options, &file, dbg);
    if (!s.ok()) {
      return s;
    }

    auto file_reader = std::make_unique<RandomAccessFileReader>(
        std::move(file), blob_file_path, clock);
    BlobLogSequentialReader reader(std::move(file_reader), clock,
                                   /*statistics=*/nullptr);
    BlobLogHeader header;
    s = reader.ReadHeader(&header);
    if (!s.ok()) {
      return s;
    }
    records->reserve(num_records);
    for (size_t i = 0; i < num_records; ++i) {
      BlobLogRecord record;
      s = reader.ReadRecord(&record,
                            BlobLogSequentialReader::kReadHeaderKeyBlob);
      if (!s.ok()) {
        return s;
      }
      records->push_back(std::move(record));
    }
    return Status::OK();
  }

  std::vector<std::shared_ptr<EventListener>> remote_listeners;
  std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>
      remote_table_properties_collector_factories;

 private:
  std::shared_ptr<Statistics> compactor_statistics_;
  std::shared_ptr<Statistics> primary_statistics_;
  std::shared_ptr<CompactionService> compaction_service_;
};

TEST_F(CompactionServiceTest, RemotePoolPropertyDoesNotCreatePool) {
  Options options = CurrentOptions();
  options.compaction_service.reset();
  DestroyAndReopen(options);
  ASSERT_EQ(RemoteCompactionThreadPool::GetShared(false), nullptr);

  int pools_created = 0;
  SyncPoint::GetInstance()->SetCallBack("RemoteCompactionThreadPool::Created",
                                        [&](void*) { ++pools_created; });
  SyncPoint::GetInstance()->EnableProcessing();
  uint64_t threads = 0;
  for (int i = 0; i < 2; ++i) {
    EXPECT_TRUE(db_->GetIntProperty(
        DB::Properties::kNumRemoteCompactionPoolThreads, &threads));
    EXPECT_EQ(threads, 0U);
  }
  EXPECT_EQ(pools_created, 0);
  {
    auto pool = RemoteCompactionThreadPool::GetShared();
    auto* pool_ptr = pool.get();
    pool->SubmitJob([pool_ptr] { pool_ptr->CompleteJob(); }, this, 3);
    EXPECT_TRUE(db_->GetIntProperty(
        DB::Properties::kNumRemoteCompactionPoolThreads, &threads));
    EXPECT_EQ(threads, 3U);
  }
  EXPECT_TRUE(db_->GetIntProperty(
      DB::Properties::kNumRemoteCompactionPoolThreads, &threads));
  EXPECT_EQ(threads, 0U);
  EXPECT_EQ(pools_created, 1);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CompactionServiceTest, BasicCompactions) {
  Options options = CurrentOptions();
  options.max_open_files = -1;
  ReopenWithCompactionService(&options);

  Statistics* primary_statistics = GetPrimaryStatistics();
  Statistics* compactor_statistics = GetCompactorStatistics();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::PrefetchTail::TaiSizeNotRecorded",
      [&](void* /* arg */) {
        // Trigger assertion to verify precise tail prefetch size calculation
        assert(false);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->DisableProcessing();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  ASSERT_EQ(CompactionServiceJobStatus::kSuccess,
            my_cs->GetFinalCompactionServiceJobStatus());

  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES), 1);
  ASSERT_GE(compactor_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES), 0);
  // even with remote compaction, primary host still needs to read SST files to
  // `verify_table()`.
  ASSERT_GE(primary_statistics->getTickerCount(COMPACT_READ_BYTES), 1);
  // all the compaction write happens on the remote side
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_GE(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 1);
  // The remote worker reads the full compaction inputs on demand, while the
  // primary only re-reads the (smaller) output files for verify_table(), so
  // remote read bytes dominate. (The worker no longer eagerly opens every file
  // during secondary recovery, so its input reads are attributed accurately.)
  ASSERT_GT(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES),
            primary_statistics->getTickerCount(COMPACT_READ_BYTES));
  // compactor is already the remote side, which doesn't have remote
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(compactor_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            0);

  // Test failed compaction
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CompactWithoutInstallation::End", [&](void* status) {
        // override job status
        auto s = static_cast<Status*>(status);
        *s = Status::Aborted("MyTestCompactionService failed to compact!");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s;
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      s = Put(Key(key_id), "value_new" + std::to_string(key_id));
      if (s.IsAborted()) {
        break;
      }
    }
    if (s.IsAborted()) {
      break;
    }
    s = Flush();
    if (s.IsAborted()) {
      break;
    }
    s = dbfull()->TEST_WaitForCompact();
    if (s.IsAborted()) {
      break;
    }
  }
  ASSERT_TRUE(s.IsAborted());

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_NOK(result.status);

  // The aborted job's output files were never installed, so they are still in
  // the job's staging directory. Size them before reopening: the reopen starts
  // a new DB session, and DB::Open() deletes staging directories belonging to
  // any other session.
  uint64_t total_size = 0;
  uint64_t total_entries = 0;
  Env* const env = options.env;
  ASSERT_TRUE(options.env != nullptr);
  for (const auto& output_file : result.output_files) {
    std::string file_name = result.output_path + "/" + output_file.file_name;

    uint64_t file_size = 0;
    ASSERT_OK(env->GetFileSize(file_name, &file_size));
    ASSERT_GT(file_size, 0);
    total_size += file_size;
    total_entries += output_file.table_properties.num_entries;
  }
  ASSERT_EQ(total_size, result.internal_stats.TotalBytesWritten());

  // Test re-open and successful unique id verification
  std::atomic_int verify_passed{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::PassedVerifyUniqueId", [&](void* arg) {
        // override job status
        auto id = static_cast<UniqueId64x2*>(arg);
        assert(*id != kNullUniqueId64x2);
        verify_passed++;
      });
  Close();
  my_cs->SetCanceled(false);
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "cf_1", "cf_2", "cf_3"},
                           options);
  ASSERT_GT(verify_passed, 0);
  ASSERT_GE(result.internal_stats.output_level_stats.micros, 1);
  ASSERT_GE(result.internal_stats.output_level_stats.cpu_micros, 1);

  ASSERT_GT(total_entries, 0);
  ASSERT_EQ(total_entries,
            result.internal_stats.output_level_stats.num_output_records);
  ASSERT_EQ(result.output_files.size(),
            result.internal_stats.output_level_stats.num_output_files);

  ASSERT_TRUE(result.stats.is_remote_compaction);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_FALSE(result.stats.is_full_compaction);

  Close();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(CompactionServiceTest, SuccessfulWaitWithFailedResultInstallsNoOutput) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  GetCompactionService()->SetWaitResultMutator([](std::string* serialized) {
    CompactionServiceResult result;
    ASSERT_OK(CompactionServiceResult::Read(*serialized, &result));
    result.status = Status::Corruption("injected non-OK worker result");
    serialized->clear();
    ASSERT_OK(result.Write(serialized));
  });

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(status.ToString().find("injected non-OK worker result"),
            std::string::npos);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kFailure);
  EXPECT_EQ(GetCompactionService()->GetOnInstallationCount(), 1);
  EXPECT_EQ(NumTableFilesAtLevel(0), 1);
  EXPECT_EQ(NumTableFilesAtLevel(1), 0);
  EXPECT_EQ(Get("key"), "value");
}

namespace {

// A CompressionManager that records, per GetCompressorForSST() call, whether
// the FilterBuildingContext reports the SST as being built by a remote
// (offloaded) compaction worker. Observations go into process-global counters
// because the remote worker reconstructs its own manager instance from the
// persisted OPTIONS file (same process in this test).
struct RemoteAwareObservations {
  std::atomic<int> num_remote{0};
  std::atomic<int> num_non_remote{0};
  void Reset() {
    num_remote.store(0);
    num_non_remote.store(0);
  }
};

RemoteAwareObservations& GetRemoteAwareObservations() {
  static RemoteAwareObservations obs;
  return obs;
}

class RemoteAwareCompressionManager : public CompressionManagerWrapper {
 public:
  RemoteAwareCompressionManager()
      : CompressionManagerWrapper(GetBuiltinV2CompressionManager()) {}

  static const char* kClassName() { return "RemoteAwareCompressionManager"; }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<Compressor> GetCompressorForSST(
      const FilterBuildingContext& context, const CompressionOptions& opts,
      CompressionType preferred) override {
    if (context.is_remote_compaction) {
      GetRemoteAwareObservations().num_remote.fetch_add(1);
    } else {
      GetRemoteAwareObservations().num_non_remote.fetch_add(1);
    }
    return CompressionManagerWrapper::GetCompressorForSST(context, opts,
                                                          preferred);
  }
};

}  // namespace

TEST_F(CompactionServiceTest, IsRemoteCompactionInCompressorContext) {
  // Register so the remote worker's DB::OpenAndCompact can reconstruct the
  // custom CompressionManager from the persisted OPTIONS file
  // (CompactionServiceOptionsOverride does not carry a compression_manager).
  ObjectLibrary::Default()->AddFactory<CompressionManager>(
      RemoteAwareCompressionManager::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<CompressionManager>* guard,
         std::string* /*errmsg*/) {
        *guard = std::make_unique<RemoteAwareCompressionManager>();
        return guard->get();
      });

  auto& obs = GetRemoteAwareObservations();
  obs.Reset();

  Options options = CurrentOptions();
  options.compression_manager =
      std::make_shared<RemoteAwareCompressionManager>();
  ReopenWithCompactionService(&options);

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  VerifyTestData();

  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);

  // The offloaded worker built the compaction output SSTs and therefore must
  // have observed is_remote_compaction == true in GetCompressorForSST().
  ASSERT_GT(obs.num_remote.load(), 0);
  // The primary's own flushes, seen by the same manager, must report false
  ASSERT_GT(obs.num_non_remote.load(), 0);

  Close();
}

TEST_F(CompactionServiceTest, IsRemoteCompactionFalseForLocalCompaction) {
  // Same custom manager, but no compaction_service: everything runs locally,
  // so GetCompressorForSST() must never see is_remote_compaction == true.
  auto& obs = GetRemoteAwareObservations();
  obs.Reset();

  Options options = CurrentOptions();
  options.compression_manager =
      std::make_shared<RemoteAwareCompressionManager>();
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 100 + j), rnd.RandomString(64)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Local flushes and compactions must all report not-remote.
  ASSERT_GT(obs.num_non_remote.load(), 0);
  ASSERT_EQ(obs.num_remote.load(), 0);

  Close();
}

TEST_F(CompactionServiceTest, SkipWALRecoveryInOpenAndCompact) {
  // Verify that OpenAndCompact skips WAL recovery when opening the secondary
  // instance. WAL replay is unnecessary for remote compaction since it only
  // needs the LSM state from MANIFEST.
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  // Track whether FindAndRecoverLogFiles is called during compaction
  std::atomic_bool wal_recovery_called{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::FindAndRecoverLogFiles:Begin",
      [&](void* /* arg */) { wal_recovery_called.store(true); });
  SyncPoint::GetInstance()->EnableProcessing();

  // Generate data and trigger compaction (which uses OpenAndCompact)
  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // WAL recovery should NOT have been called during OpenAndCompact
  ASSERT_FALSE(wal_recovery_called.load());

  // Data should still be correct (compaction worked without WAL recovery)
  VerifyTestData();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CompactionServiceTest, ManualCompaction) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  end_str = Key(92);
  end = end_str;
  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);

  auto info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(0, info.cf_id);
  ASSERT_EQ(kDefaultColumnFamilyName, info.cf_name);

  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(0, info.cf_id);
  ASSERT_EQ(kDefaultColumnFamilyName, info.cf_name);

  // Test non-default CF
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);

  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(handles_[1]->GetID(), info.cf_id);
  ASSERT_EQ(handles_[1]->GetName(), info.cf_name);

  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(handles_[1]->GetID(), info.cf_id);
  ASSERT_EQ(handles_[1]->GetName(), info.cf_name);
}

TEST_F(CompactionServiceTest, RemoteCompactionPreservesBlobDependencies) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  ReopenWithCompactionService(&options);

  const std::vector<std::pair<std::string, std::string>> old_values{
      {"key1", std::string(16 * 1024, 'a')},
      {"key2", std::string(16 * 1024, 'b')}};
  for (const auto& [key, value] : old_values) {
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  const std::vector<std::pair<std::string, std::string>> expected_values{
      {"key1", std::string(16 * 1024, 'c')},
      {"key2", std::string(16 * 1024, 'd')}};
  for (const auto& [key, value] : expected_values) {
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions({{"enable_blob_files", "false"}}));

  auto* const compaction_service = GetCompactionService();
  const uint64_t compaction_num = compaction_service->GetCompactionNum();
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_GE(compaction_service->GetCompactionNum(), compaction_num + 1);

  CompactionServiceInput input;
  compaction_service->GetInput(&input);
  ASSERT_FALSE(input.input_blob_files.empty());
  ASSERT_EQ(input.blob_file_number_start, kInvalidBlobFileNumber);
  ASSERT_EQ(input.blob_file_number_limit, kInvalidBlobFileNumber);

  CompactionServiceResult result;
  compaction_service->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_FALSE(result.output_files.empty());
  for (const auto& output_file : result.output_files) {
    EXPECT_NE(output_file.oldest_blob_file_number, kInvalidBlobFileNumber);
  }

  for (const auto& [key, expected_value] : expected_values) {
    std::string actual_value;
    ASSERT_OK(db_->Get(ReadOptions(), key, &actual_value));
    EXPECT_EQ(actual_value, expected_value);
  }
}

class ChangeBlobValueCompactionFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /* level */, const Slice& key,
                    ValueType /* value_type */, const Slice& existing_value,
                    std::string* new_value,
                    std::string* /* skip_until */) const override {
    if (key == "key1") {
      *new_value = existing_value.ToString();
      (*new_value)[0] = 'z';
      return Decision::kChangeValue;
    }
    if (key == "key2") {
      return Decision::kRemove;
    }
    return Decision::kKeep;
  }

  const char* Name() const override {
    return "ChangeBlobValueCompactionFilter";
  }
};

class RemoveAllBlobValuesCompactionFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /* level */, const Slice& /* key */,
                    ValueType /* value_type */,
                    const Slice& /* existing_value */,
                    std::string* /* new_value */,
                    std::string* /* skip_until */) const override {
    return Decision::kRemove;
  }

  const char* Name() const override {
    return "RemoveAllBlobValuesCompactionFilter";
  }
};

class ChangeAllBlobValuesCompactionFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /* level */, const Slice& /* key */,
                    ValueType /* value_type */, const Slice& existing_value,
                    std::string* new_value,
                    std::string* /* skip_until */) const override {
    *new_value = existing_value.ToString();
    (*new_value)[0] = 'z';
    return Decision::kChangeValue;
  }

  const char* Name() const override {
    return "ChangeAllBlobValuesCompactionFilter";
  }
};

class RecordingBlobCreationListener : public EventListener {
 public:
  void OnBlobFileCreated(const BlobFileCreationInfo& info) override {
    if (info.reason == BlobFileCreationReason::kCompaction) {
      created_files.push_back(info.file_path);
    }
  }

  std::vector<std::string> created_files;
};

enum class BoundaryFilterDecision {
  kChangeValue,
  kRemove,
  kChangeWideColumnEntity,
};

class BoundaryBlobValueCompactionFilter : public CompactionFilter {
 public:
  explicit BoundaryBlobValueCompactionFilter(BoundaryFilterDecision decision)
      : decision_(decision) {}

  Decision FilterV4(
      int /* level */, const Slice& key, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* new_value,
      std::vector<std::pair<std::string, std::string>>* new_columns,
      std::string* /* skip_until */,
      WideColumnBlobResolver* /* blob_resolver */) const override {
    if (key != "key3") {
      return Decision::kKeep;
    }
    switch (decision_) {
      case BoundaryFilterDecision::kChangeValue:
        assert(value_type == ValueType::kValue);
        assert(existing_value != nullptr);
        assert(new_value != nullptr);
        *new_value = existing_value->ToString();
        (*new_value)[0] = 'z';
        return Decision::kChangeValue;
      case BoundaryFilterDecision::kRemove:
        return Decision::kRemove;
      case BoundaryFilterDecision::kChangeWideColumnEntity:
        assert(value_type == ValueType::kWideColumnEntity);
        assert(existing_columns != nullptr);
        assert(new_columns != nullptr);
        new_columns->emplace_back(kDefaultWideColumnName.ToString(),
                                  "changed_default");
        new_columns->emplace_back("attribute", "changed_attribute");
        return Decision::kChangeWideColumnEntity;
    }
    assert(false);
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }

  const char* Name() const override {
    return "BoundaryBlobValueCompactionFilter";
  }

 private:
  BoundaryFilterDecision decision_;
};

TEST_F(CompactionServiceTest,
       RemoteCompactionIncludesFullBlobFileRangeAndSizesReservation) {
  constexpr uint64_t kBlobFileSize = 4096;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = kBlobFileSize;
  options.max_subcompactions = 1;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::vector<std::pair<std::string, std::string>> values{
      {"key1", std::string(kBlobFileSize, 'a')},
      {"key2", std::string(kBlobFileSize, 'b')},
      {"key3", std::string(kBlobFileSize, 'c')}};
  for (const auto& [key, value] : values) {
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_GE(original_blob_files.size(), 3);
  std::vector<std::string> expected_input_blob_files;
  expected_input_blob_files.reserve(original_blob_files.size());
  for (const uint64_t file_number : original_blob_files) {
    expected_input_blob_files.emplace_back(BlobFileName(file_number));
  }

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {input_sst.db_path + "/" + input_sst.name}, 1));

  CompactionServiceInput input;
  GetCompactionService()->GetInput(&input);
  EXPECT_EQ(input.input_blob_files, expected_input_blob_files);
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_INPUT_BLOB_FILE_COUNT),
            expected_input_blob_files.size());
  EXPECT_GE(input.blob_file_number_limit - input.blob_file_number_start,
            original_blob_files.size() + 2);

  CompactionServiceResult result;
  GetCompactionService()->GetResult(&result);
  ASSERT_OK(result.status);
  EXPECT_EQ(result.blob_compaction_version, kCompactionServiceBlobVersion);
  for (const auto& [key, value] : values) {
    std::string expected = value;
    expected[0] = 'z';
    EXPECT_EQ(Get(key), expected);
  }
}

TEST_F(CompactionServiceTest, OlderBlobWorkerIsDeclinedRecoverably) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [](void* arg) {
        CompactionServiceResult** const result_ptr =
            static_cast<CompactionServiceResult**>(arg);
        ASSERT_NE(result_ptr, nullptr);
        ASSERT_NE(*result_ptr, nullptr);
        (*result_ptr)->blob_compaction_version = 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kUseLocal);
  EXPECT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 1);
  std::string expected = value;
  expected[0] = 'z';
  EXPECT_EQ(Get("key"), expected);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest,
       OlderBlobGarbageCollectionWorkerIsDeclinedRecoverably) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [](void* arg) {
        CompactionServiceResult** const result_ptr =
            static_cast<CompactionServiceResult**>(arg);
        ASSERT_NE(result_ptr, nullptr);
        ASSERT_NE(*result_ptr, nullptr);
        (*result_ptr)->blob_gc_cutoff_version = 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kUseLocal);
  EXPECT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 1);
  EXPECT_EQ(Get("key"), value);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest,
       OlderBlobWorkerCreatingBlobsIsDeclinedRecoverably) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = false;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ASSERT_TRUE(GetBlobFileNumbers().empty());
  ASSERT_OK(db_->SetOptions({{"enable_blob_files", "true"}}));

  MyTestCompactionService* const compaction_service = GetCompactionService();
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [](void* arg) {
        CompactionServiceResult** const result_ptr =
            static_cast<CompactionServiceResult**>(arg);
        ASSERT_NE(result_ptr, nullptr);
        ASSERT_NE(*result_ptr, nullptr);
        CompactionServiceResult* const result = *result_ptr;
        ASSERT_FALSE(result->blob_file_additions.empty());

        // Model a pre-feature worker, which can create blob files but cannot
        // advertise their metadata or its support for transporting them.
        result->blob_compaction_version = 0;
        result->blob_file_additions.clear();
        result->blob_file_garbages.clear();
        for (auto& output_file : result->output_files) {
          output_file.oldest_blob_file_number = kInvalidBlobFileNumber;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  EXPECT_EQ(compaction_service->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kUseLocal);
  EXPECT_EQ(compaction_service->GetUseLocalInstallationCount(), 1);
  ASSERT_FALSE(GetBlobFileNumbers().empty());
  std::string expected = value;
  expected[0] = 'z';
  EXPECT_EQ(Get("key"), expected);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest, BlobValidationDeclinesFallBackLocally) {
  using ResultMutator = std::function<void(CompactionServiceResult*)>;
  struct ValidationCase {
    const char* name;
    ResultMutator mutate;
  };
  const std::vector<ValidationCase> cases{
      {"missing capability",
       [](CompactionServiceResult* result) {
         result->blob_compaction_version = 0;
       }},
      {"non-contiguous file number",
       [](CompactionServiceResult* result) {
         ASSERT_FALSE(result->blob_file_additions.empty());
         const BlobFileAddition& addition = result->blob_file_additions.front();
         result->blob_file_additions.front() = BlobFileAddition(
             addition.GetBlobFileNumber() + 1, addition.GetTotalBlobCount(),
             addition.GetTotalBlobBytes(), addition.GetChecksumMethod(),
             addition.GetChecksumValue());
       }},
      {"invalid addition metadata",
       [](CompactionServiceResult* result) {
         ASSERT_FALSE(result->blob_file_additions.empty());
         const BlobFileAddition& addition = result->blob_file_additions.front();
         result->blob_file_additions.front() = BlobFileAddition(
             addition.GetBlobFileNumber(), /*total_blob_count=*/0,
             addition.GetTotalBlobBytes(), addition.GetChecksumMethod(),
             addition.GetChecksumValue());
       }},
      {"unknown oldest blob file",
       [](CompactionServiceResult* result) {
         ASSERT_FALSE(result->output_files.empty());
         result->output_files.front().oldest_blob_file_number =
             std::numeric_limits<uint64_t>::max() - 1;
       }},
  };

  for (const ValidationCase& test_case : cases) {
    SCOPED_TRACE(test_case.name);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    options.blob_file_size = 1;
    options.max_subcompactions = 1;
    options.paranoid_checks = true;
    std::unique_ptr<CompactionFilter> compaction_filter(
        new ChangeAllBlobValuesCompactionFilter());
    options.compaction_filter = compaction_filter.get();
    ReopenWithCompactionService(&options);
    ASSERT_EQ(GetPrimaryStatistics()->getTickerCount(
                  REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT),
              0);

    const std::string value(4096, 'a');
    ASSERT_OK(Put("key", value));
    ASSERT_OK(Flush());
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), 1);
    const SstFileMetaData& input_sst = metadata.levels[0].files.front();

    SyncPoint::GetInstance()->SetCallBack(
        "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
          auto** result_ptr = static_cast<CompactionServiceResult**>(arg);
          ASSERT_NE(result_ptr, nullptr);
          ASSERT_NE(*result_ptr, nullptr);
          test_case.mutate(*result_ptr);
        });
    SyncPoint::GetInstance()->EnableProcessing();

    const Status status = db_->CompactFiles(
        CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_OK(status);
    EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
              CompactionServiceJobStatus::kUseLocal);
    EXPECT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 1);
    EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                  REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT),
              1);
    ASSERT_OK(dbfull()->TEST_GetBGError());
    std::string expected = value;
    expected[0] = 'z';
    EXPECT_EQ(Get("key"), expected);
    ASSERT_OK(Put("post-fallback", "writable"));
  }
}

TEST_F(CompactionServiceTest, ServiceFailurePreservesWorkerIncompleteStatus) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = false;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  MyTestCompactionService* const compaction_service = GetCompactionService();
  compaction_service->ReturnIncompleteAsFailure(true);
  compaction_service->SetWorkerOptionOverride("enable_blob_files", "true");
  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  ASSERT_TRUE(status.IsIncomplete()) << status.ToString();
  EXPECT_EQ(compaction_service->GetUseLocalInstallationCount(), 0);
  EXPECT_EQ(compaction_service->GetOnInstallationCount(), 0);
  CompactionServiceResult result;
  compaction_service->GetResult(&result);
  ASSERT_TRUE(result.status.IsIncomplete()) << result.status.ToString();
  EXPECT_TRUE(result.blob_file_additions.empty());
  EXPECT_EQ(Get("key"), value);
  EXPECT_TRUE(GetBlobFileNumbers().empty());
}

TEST_F(CompactionServiceTest, RemoteBlobWorkerDisablesBlobOutputSafely) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  MyTestCompactionService* const compaction_service = GetCompactionService();
  compaction_service->SetWorkerOptionOverride("enable_blob_files", "false");
  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {input_sst.db_path + "/" + input_sst.name}, 1));
  EXPECT_EQ(compaction_service->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kSuccess);
  EXPECT_EQ(compaction_service->GetUseLocalInstallationCount(), 0);
  CompactionServiceResult result;
  compaction_service->GetResult(&result);
  EXPECT_OK(result.status);
  EXPECT_EQ(result.blob_compaction_version, kCompactionServiceBlobVersion);
  EXPECT_TRUE(result.blob_file_additions.empty());
  EXPECT_FALSE(result.blob_file_garbages.empty());
  std::string expected = value;
  expected[0] = 'z';
  EXPECT_EQ(Get("key"), expected);
}

TEST_F(CompactionServiceTest, RemoteCompactionFiltersAndCreatesBlobFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  auto* const sfm =
      static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  auto blob_listener = std::make_shared<RecordingBlobCreationListener>();
  options.listeners.push_back(blob_listener);
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeBlobValueCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::vector<std::pair<std::string, std::string>> first_values{
      {"key1", std::string(4096, 'a')},
      {"key2", std::string(4096, 'b')},
      {"key3", std::string(4096, 'c')}};
  for (const auto& [key, value] : first_values) {
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());

  const std::string updated_value(4096, 'd');
  ASSERT_OK(Put("key1", updated_value));
  ASSERT_OK(Flush());

  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_GE(original_blob_files.size(), 2);

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  const Status compact_status =
      db_->CompactRange(compact_options, nullptr, nullptr);

  ASSERT_OK(compact_status);

  CompactionServiceInput input;
  GetCompactionService()->GetInput(&input);
  ASSERT_FALSE(input.input_blob_files.empty());
  ASSERT_GT(input.blob_file_number_start, 0);
  ASSERT_GT(input.blob_file_number_limit, input.blob_file_number_start);
  for (const auto& blob_file : input.input_blob_files) {
    ASSERT_EQ(blob_file.substr(blob_file.size() - 5), ".blob");
  }

  const std::vector<CompactionServiceInput> inputs =
      GetCompactionService()->GetInputs();
  const std::vector<CompactionServiceResult> results =
      GetCompactionService()->GetResults();
  ASSERT_EQ(results.size(), inputs.size());
  const auto input_it = std::find_if(
      inputs.begin(), inputs.end(), [&](const CompactionServiceInput& item) {
        return item.blob_file_number_start == input.blob_file_number_start;
      });
  ASSERT_NE(input_it, inputs.end());
  const size_t result_index =
      static_cast<size_t>(std::distance(inputs.begin(), input_it));
  const CompactionServiceResult& result = results[result_index];
  ASSERT_OK(result.status);
  ASSERT_GE(result.blob_file_additions.size(), 2);
  ASSERT_FALSE(result.blob_file_garbages.empty());
  const std::unordered_map<std::string, uint64_t> tracked_files =
      sfm->GetTrackedFiles();

  std::vector<uint64_t> added_blob_files;
  added_blob_files.reserve(result.blob_file_additions.size());
  for (size_t i = 0; i < result.blob_file_additions.size(); ++i) {
    const auto& addition = result.blob_file_additions[i];
    ASSERT_EQ(addition.GetBlobFileNumber(), input.blob_file_number_start + i);
    ASSERT_LT(addition.GetBlobFileNumber(), input.blob_file_number_limit);
    const std::string installed_path =
        BlobFileName(dbname_, addition.GetBlobFileNumber());
    const auto tracked = tracked_files.find(installed_path);
    ASSERT_NE(tracked, tracked_files.end());
    EXPECT_EQ(std::count(blob_listener->created_files.begin(),
                         blob_listener->created_files.end(), installed_path),
              1);
    uint64_t physical_size = 0;
    ASSERT_OK(env_->GetFileSize(installed_path, &physical_size));
    EXPECT_EQ(tracked->second, physical_size);
    added_blob_files.push_back(addition.GetBlobFileNumber());
  }
  ASSERT_EQ(GetBlobFileNumbers(), added_blob_files);
  ASSERT_GE(dbfull()->TEST_Current_Next_FileNo(), input.blob_file_number_limit);

  std::vector<uint64_t> garbage_blob_files;
  garbage_blob_files.reserve(result.blob_file_garbages.size());
  for (const auto& garbage : result.blob_file_garbages) {
    garbage_blob_files.push_back(garbage.GetBlobFileNumber());
    EXPECT_GT(garbage.GetGarbageBlobCount(), 0);
    EXPECT_GT(garbage.GetGarbageBlobBytes(), 0);
  }
  std::sort(garbage_blob_files.begin(), garbage_blob_files.end());
  ASSERT_EQ(
      std::adjacent_find(garbage_blob_files.begin(), garbage_blob_files.end()),
      garbage_blob_files.end());

  std::string filtered_value = updated_value;
  filtered_value[0] = 'z';
  ASSERT_EQ(Get("key1"), filtered_value);
  ASSERT_EQ(Get("key2"), "NOT_FOUND");
  ASSERT_EQ(Get("key3"), first_values[2].second);
}

TEST_F(CompactionServiceTest,
       RemoteCompactionUsesPrimaryBlobGarbageCollectionCutoff) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.75;
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key1", value));
  ASSERT_OK(Put("key2", value));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> primary_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(primary_blob_files.size(), 2);

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  uint64_t scheduled_cutoff = 0;
  uint64_t worker_cutoff = 0;
  std::vector<uint64_t> worker_blob_files;
  bool worker_view_skewed = false;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void* arg) {
        const auto* input = static_cast<CompactionServiceInput*>(arg);
        ASSERT_TRUE(input->has_blob_gc_cutoff_file_number);
        scheduled_cutoff = input->blob_gc_cutoff_file_number;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "MyTestCompactionService::Wait:BeforeOpenAndCompact", [&](void*) {
        ASSERT_OK(Put("key3", value));
        ASSERT_OK(Flush());
        worker_blob_files = GetBlobFileNumbers();
        worker_view_skewed = true;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::BlobGarbageCollectionCutoffFileNumber",
      [&](void* arg) {
        if (worker_view_skewed) {
          worker_cutoff = *static_cast<uint64_t*>(arg);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  ASSERT_EQ(worker_blob_files.size(), 3);
  const uint64_t recomputed_worker_cutoff = worker_blob_files[2];
  ASSERT_EQ(scheduled_cutoff, primary_blob_files[1]);
  ASSERT_NE(scheduled_cutoff, recomputed_worker_cutoff);
  EXPECT_EQ(worker_cutoff, scheduled_cutoff);
  EXPECT_EQ(Get("key1"), value);
  EXPECT_EQ(Get("key2"), value);
  EXPECT_EQ(Get("key3"), value);
}

TEST_F(CompactionServiceTest,
       RemoteCompactionWithoutBlobGarbageCollectionCutoffDisablesGC) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  uint64_t worker_cutoff = kInvalidBlobFileNumber;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [](void* arg) {
        auto* input = static_cast<CompactionServiceInput*>(arg);
        input->has_blob_gc_cutoff_file_number = false;
        input->blob_gc_cutoff_file_number = 0;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::BlobGarbageCollectionCutoffFileNumber",
      [&](void* arg) { worker_cutoff = *static_cast<uint64_t*>(arg); });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  EXPECT_EQ(worker_cutoff, 0);
  CompactionServiceInput input;
  GetCompactionService()->GetInput(&input);
  EXPECT_FALSE(input.has_blob_gc_cutoff_file_number);
  CompactionServiceResult result;
  GetCompactionService()->GetResult(&result);
  ASSERT_OK(result.status);
  EXPECT_TRUE(result.blob_file_garbages.empty());
  EXPECT_EQ(Get("key"), value);
}

TEST_F(CompactionServiceTest, RemoteBlobGarbageIsInstalledExactlyOnce) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.max_subcompactions = 1;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeBlobValueCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key1", value));
  ASSERT_OK(Put("key2", value));
  ASSERT_OK(Put("key3", value));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 1);
  const uint64_t original_blob_file = original_blob_files.front();
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {input_sst.db_path + "/" + input_sst.name}, 1));

  CompactionServiceResult result;
  GetCompactionService()->GetResult(&result);
  ASSERT_OK(result.status);
  const auto garbage_it = std::find_if(
      result.blob_file_garbages.begin(), result.blob_file_garbages.end(),
      [original_blob_file](const BlobFileGarbage& garbage) {
        return garbage.GetBlobFileNumber() == original_blob_file;
      });
  ASSERT_NE(garbage_it, result.blob_file_garbages.end());

  VersionSet* const versions = dbfull()->GetVersionSet();
  ASSERT_NE(versions, nullptr);
  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);
  const std::shared_ptr<BlobFileMetaData> installed_blob =
      cfd->current()->storage_info()->GetBlobFileMetaData(original_blob_file);
  ASSERT_NE(installed_blob, nullptr);
  EXPECT_EQ(installed_blob->GetGarbageBlobCount(),
            garbage_it->GetGarbageBlobCount());
  EXPECT_EQ(installed_blob->GetGarbageBlobBytes(),
            garbage_it->GetGarbageBlobBytes());
  EXPECT_EQ(Get("key2"), "NOT_FOUND");
  EXPECT_EQ(Get("key3"), value);
}

TEST_F(CompactionServiceTest, PartialBlobRenameFailureRollsBackFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  auto* const sfm =
      static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put("key1", std::string(4096, 'a')));
  ASSERT_OK(Put("key2", std::string(4096, 'b')));
  ASSERT_OK(Flush());
  const size_t tracked_file_count_before_import = sfm->GetTrackedFiles().size();
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  size_t rename_count = 0;
  bool rollback_observed = false;
  bool garbage_prevalidation_observed = false;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  if (sync_point == nullptr) {
    FAIL() << "SyncPoint is unavailable";
  }
  sync_point->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "AfterRemoteBlobGarbagePrevalidation",
      [&](void* arg) {
        if (arg == nullptr) {
          ADD_FAILURE() << "Missing aggregate state";
          return;
        }
        const bool* const aggregate_is_empty = static_cast<const bool*>(arg);
        EXPECT_TRUE(*aggregate_is_empty);
        garbage_prevalidation_observed = true;
      });
  sync_point->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "AfterBlobFileRename",
      [&](void* arg) {
        Status* const status = static_cast<Status*>(arg);
        if (++rename_count == 2) {
          *status = Status::IOError("injected blob rename failure");
        }
      });
  sync_point->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "AfterBlobFileRollback",
      [&](void* arg) {
        CompactionOutputs* const outputs = static_cast<CompactionOutputs*>(arg);
        EXPECT_TRUE(outputs->GetBlobFileAdditions().empty());
        EXPECT_TRUE(outputs->GetOutputFilePaths().empty());
        rollback_observed = true;
      });
  sync_point->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  sync_point->DisableProcessing();
  sync_point->ClearAllCallBacks();

  ASSERT_TRUE(status.IsIOError()) << status.ToString();
  ASSERT_GE(rename_count, 2);
  ASSERT_TRUE(rollback_observed);
  ASSERT_TRUE(garbage_prevalidation_observed);
  EXPECT_EQ(sfm->GetTrackedFiles().size(), tracked_file_count_before_import);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kFailure);
  CompactionServiceResult result;
  GetCompactionService()->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_FALSE(result.blob_file_garbages.empty());
  ASSERT_GE(result.blob_file_additions.size(), 2);
  for (const BlobFileAddition& addition : result.blob_file_additions) {
    const Status exists =
        env_->FileExists(BlobFileName(dbname_, addition.GetBlobFileNumber()));
    EXPECT_TRUE(exists.IsNotFound()) << exists.ToString();
  }
}

TEST_F(CompactionServiceTest, RemoteBlobStructuralValidationIOFailsInstall) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.max_subcompactions = 1;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  size_t injected_errors = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ValidateRemoteBlobFile:AfterGetFileSize", [&](void* arg) {
        auto* const status = static_cast<IOStatus*>(arg);
        if (injected_errors++ == 0) {
          *status = IOStatus::IOError("injected structural validation error");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(status.IsIOError()) << status.ToString();
  ASSERT_NE(status.ToString().find("injected structural validation error"),
            std::string::npos);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kFailure);
  EXPECT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 0);
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT),
            0);
  EXPECT_EQ(GetBlobFileNumbers().size(), 1);
  EXPECT_EQ(Get("key"), value);
}

TEST_F(CompactionServiceTest, BlobRollbackDeleteFailureRemainsTracked) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  auto* const sfm =
      static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put("key1", std::string(4096, 'a')));
  ASSERT_OK(Put("key2", std::string(4096, 'b')));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  size_t rename_count = 0;
  size_t delete_count = 0;
  bool failed_delete_was_still_tracked = false;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "AfterBlobFileRename",
      [&](void* arg) {
        if (++rename_count == 2) {
          *static_cast<Status*>(arg) =
              Status::IOError("injected blob rename failure");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "BeforeBlobFileDelete",
      [&](void* arg) {
        if (delete_count++ == 0) {
          *static_cast<IOStatus*>(arg) =
              IOStatus::IOError("injected blob delete failure");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "AfterBlobFileRollback",
      [&](void*) {
        CompactionServiceResult result;
        GetCompactionService()->GetResult(&result);
        result.status.PermitUncheckedError();
        const auto tracked_files = sfm->GetTrackedFiles();
        for (const BlobFileAddition& addition : result.blob_file_additions) {
          const std::string path =
              BlobFileName(dbname_, addition.GetBlobFileNumber());
          if (env_->FileExists(path).ok() &&
              tracked_files.find(path) != tracked_files.end()) {
            failed_delete_was_still_tracked = true;
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(status.IsIOError()) << status.ToString();
  ASSERT_NE(status.ToString().find("injected blob delete failure"),
            std::string::npos);
  ASSERT_GE(rename_count, 2);
  ASSERT_GE(delete_count, 1);
  ASSERT_TRUE(failed_delete_was_still_tracked);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kFailure);
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT),
            0);
}

TEST_F(CompactionServiceTest, RemoteBlobImportSpaceLimitFallsBackLocally) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  auto* const sfm =
      static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();

  const uint64_t max_allowed_space = sfm->GetTotalSize() + 1;
  size_t max_space_hits = 0;
  size_t registration_attempts = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "BeforeBlobFileRegistration",
      [&](void*) {
        if (++registration_attempts == 1) {
          sfm->SetMaxAllowedSpaceUsage(max_allowed_space);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileCompletionCallback::CallBack::MaxAllowedSpaceReached",
      [&](void*) {
        ++max_space_hits;
        sfm->SetMaxAllowedSpaceUsage(0);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status = db_->CompactFiles(
      CompactionOptions(), {input_sst.db_path + "/" + input_sst.name}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  ASSERT_EQ(max_space_hits, 1);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kUseLocal);
  EXPECT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 1);
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT),
            1);
  std::string expected = value;
  expected[0] = 'z';
  EXPECT_EQ(Get("key"), expected);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest,
       CorruptedRemoteBlobChecksumFailsBeforeSstInstallation) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  options.verify_output_flags = VerifyOutputFlags::kEnableForRemoteCompaction |
                                VerifyOutputFlags::kVerifyFileChecksum;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string first_value(4096, 'a');
  const std::string second_value(4096, 'b');
  ASSERT_OK(Put("key1", first_value));
  ASSERT_OK(Put("key2", second_value));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        auto** result_ptr = static_cast<CompactionServiceResult**>(arg);
        ASSERT_NE(result_ptr, nullptr);
        ASSERT_NE(*result_ptr, nullptr);
        ASSERT_FALSE((*result_ptr)->blob_file_additions.empty());
        const uint64_t file_number =
            (*result_ptr)->blob_file_additions.front().GetBlobFileNumber();
        ASSERT_OK(test::CorruptFile(
            env_, BlobFileName((*result_ptr)->output_path, file_number), 0, 1,
            false /* verifyChecksum */));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status status =
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kUseLocal);
  EXPECT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 1);
  std::string expected_first = first_value;
  std::string expected_second = second_value;
  expected_first[0] = 'z';
  expected_second[0] = 'z';
  EXPECT_EQ(Get("key1"), expected_first);
  EXPECT_EQ(Get("key2"), expected_second);
  ASSERT_OK(Put("post-fallback", "writable"));
  CompactionServiceResult result;
  GetCompactionService()->GetResult(&result);
  ASSERT_OK(result.status);
  for (const BlobFileAddition& addition : result.blob_file_additions) {
    const Status exists =
        env_->FileExists(BlobFileName(dbname_, addition.GetBlobFileNumber()));
    EXPECT_TRUE(exists.IsNotFound()) << exists.ToString();
  }
}

TEST_F(CompactionServiceTest,
       RemoteBlobChecksumCapabilityIsDeclinedRecoverably) {
  enum class ChecksumMetadata {
    kMissing,
    kUnsupportedMethod,
  };

  for (const ChecksumMetadata checksum_metadata :
       {ChecksumMetadata::kMissing, ChecksumMetadata::kUnsupportedMethod}) {
    SCOPED_TRACE(static_cast<int>(checksum_metadata));
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    options.blob_file_size = 1;
    options.max_subcompactions = 1;
    options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
    options.verify_output_flags =
        VerifyOutputFlags::kEnableForRemoteCompaction |
        VerifyOutputFlags::kVerifyFileChecksum;
    std::unique_ptr<CompactionFilter> compaction_filter(
        new ChangeAllBlobValuesCompactionFilter());
    options.compaction_filter = compaction_filter.get();
    ReopenWithCompactionService(&options);

    const std::string value(4096, 'a');
    ASSERT_OK(Put("key", value));
    ASSERT_OK(Flush());
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
          auto** result_ptr = static_cast<CompactionServiceResult**>(arg);
          ASSERT_NE(result_ptr, nullptr);
          ASSERT_NE(*result_ptr, nullptr);
          ASSERT_FALSE((*result_ptr)->blob_file_additions.empty());
          const BlobFileAddition& addition =
              (*result_ptr)->blob_file_additions.front();
          const std::string checksum_method =
              checksum_metadata == ChecksumMetadata::kMissing
                  ? ""
                  : "UnsupportedChecksum";
          const std::string checksum_value =
              checksum_metadata == ChecksumMetadata::kMissing
                  ? ""
                  : addition.GetChecksumValue();
          (*result_ptr)->blob_file_additions.front() = BlobFileAddition(
              addition.GetBlobFileNumber(), addition.GetTotalBlobCount(),
              addition.GetTotalBlobBytes(), checksum_method, checksum_value);
        });
    SyncPoint::GetInstance()->EnableProcessing();

    const Status status =
        db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_OK(status);
    EXPECT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
              CompactionServiceJobStatus::kUseLocal);
    EXPECT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 1);
    std::string expected = value;
    expected[0] = 'z';
    EXPECT_EQ(Get("key"), expected);
    ASSERT_OK(Put("post-fallback", "writable"));
    Close();
  }
}

TEST_F(CompactionServiceTest,
       RemoteBlobFileRangeDoesNotRaceWithPrimaryAllocation) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeBlobValueCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key1", value));
  ASSERT_OK(Put("key3", value));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->LoadDependency(
      {{"MyTestCompactionService::Wait:BeforeOpenAndCompact",
        "CompactionServiceTest::RemoteRangeReserved"},
       {"CompactionServiceTest::PrimaryFlushFinished",
        "MyTestCompactionService::Wait:Continue"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Status compact_status;
  std::thread compaction_thread([&] {
    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    compact_options.max_subcompactions = 1;
    compact_status = db_->CompactRange(compact_options, nullptr, nullptr);
  });

  TEST_SYNC_POINT("CompactionServiceTest::RemoteRangeReserved");
  CompactionServiceInput input;
  GetCompactionService()->GetInput(&input);
  const uint64_t next_file_number_before_flush =
      dbfull()->TEST_Current_Next_FileNo();
  const Status put_status = Put("concurrent", std::string(4096, 'c'));
  const Status flush_status = Flush();
  const uint64_t next_file_number_after_flush =
      dbfull()->TEST_Current_Next_FileNo();
  TEST_SYNC_POINT("CompactionServiceTest::PrimaryFlushFinished");
  compaction_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(put_status);
  ASSERT_OK(flush_status);
  ASSERT_OK(compact_status);
  ASSERT_GT(input.blob_file_number_start, 0);
  ASSERT_GT(input.blob_file_number_limit, input.blob_file_number_start);
  ASSERT_GE(next_file_number_before_flush, input.blob_file_number_limit);
  ASSERT_GT(next_file_number_after_flush, next_file_number_before_flush);

  const std::vector<CompactionServiceInput> inputs =
      GetCompactionService()->GetInputs();
  const std::vector<CompactionServiceResult> results =
      GetCompactionService()->GetResults();
  ASSERT_EQ(results.size(), inputs.size());
  const auto input_it = std::find_if(
      inputs.begin(), inputs.end(), [&](const CompactionServiceInput& item) {
        return item.blob_file_number_start == input.blob_file_number_start;
      });
  ASSERT_NE(input_it, inputs.end());
  const size_t result_index =
      static_cast<size_t>(std::distance(inputs.begin(), input_it));
  const CompactionServiceResult& result = results[result_index];
  ASSERT_OK(result.status);
  ASSERT_FALSE(result.blob_file_additions.empty());
  for (const auto& addition : result.blob_file_additions) {
    EXPECT_GE(addition.GetBlobFileNumber(), input.blob_file_number_start);
    EXPECT_LT(addition.GetBlobFileNumber(), input.blob_file_number_limit);
    EXPECT_LT(addition.GetBlobFileNumber(), next_file_number_before_flush);
  }
  std::string filtered_value = value;
  filtered_value[0] = 'z';
  ASSERT_EQ(Get("key1"), filtered_value);
  ASSERT_EQ(Get("concurrent"), std::string(4096, 'c'));
}

TEST_F(CompactionServiceTest, RemoteBlobRangeExhaustionFallsBackLocally) {
  constexpr size_t kValueSize = 1 << 20;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = kValueSize;
  options.max_subcompactions = 1;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  auto* const compaction_service = GetCompactionService();
  ASSERT_OK(Put("key1", std::string(kValueSize, 'a')));
  ASSERT_OK(Put("key2", std::string(kValueSize, 'b')));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [](void* arg) {
        auto* input = static_cast<CompactionServiceInput*>(arg);
        ASSERT_GT(input->blob_file_number_start, 0);
        ASSERT_GT(input->blob_file_number_limit - input->blob_file_number_start,
                  2);
        input->blob_file_number_limit = input->blob_file_number_start + 1;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.max_subcompactions = 1;
  const Status compact_status =
      db_->CompactRange(compact_options, nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(compact_status);
  ASSERT_EQ(compaction_service->GetUseLocalInstallationCount(), 0);
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT),
            0);
  const std::vector<CompactionServiceResult> results =
      compaction_service->GetResults();
  ASSERT_TRUE(std::any_of(results.begin(), results.end(), [](const auto& item) {
    return item.status.IsIncomplete();
  }));
  std::string expected1(kValueSize, 'a');
  std::string expected2(kValueSize, 'b');
  expected1[0] = 'z';
  expected2[0] = 'z';
  ASSERT_EQ(Get("key1"), expected1);
  ASSERT_EQ(Get("key2"), expected2);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest, RemoteBlobOptionSkewFallsBackLocally) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.max_subcompactions = 1;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  auto* const compaction_service = GetCompactionService();
  compaction_service->SetWorkerOptionOverride("enable_blob_files", "true");
  ASSERT_OK(Put("key1", std::string(4096, 'a')));
  ASSERT_OK(Put("key2", std::string(4096, 'b')));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions({{"enable_blob_files", "false"}}));

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.max_subcompactions = 1;
  const Status compact_status =
      db_->CompactRange(compact_options, nullptr, nullptr);

  ASSERT_OK(compact_status);
  ASSERT_EQ(compaction_service->GetUseLocalInstallationCount(), 0);
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_BLOB_RESULT_DECLINED_COUNT),
            0);
  const std::vector<CompactionServiceInput> inputs =
      compaction_service->GetInputs();
  ASSERT_TRUE(std::any_of(inputs.begin(), inputs.end(), [](const auto& item) {
    return !item.input_blob_files.empty() &&
           item.blob_file_number_start == kInvalidBlobFileNumber &&
           item.blob_file_number_limit == kInvalidBlobFileNumber;
  }));
  const std::vector<CompactionServiceResult> results =
      compaction_service->GetResults();
  ASSERT_TRUE(std::any_of(results.begin(), results.end(), [](const auto& item) {
    return item.status.IsIncomplete();
  }));
  std::string expected1(4096, 'a');
  std::string expected2(4096, 'b');
  expected1[0] = 'z';
  expected2[0] = 'z';
  ASSERT_EQ(Get("key1"), expected1);
  ASSERT_EQ(Get("key2"), expected2);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest, RejectsMalformedRemoteBlobMetadata) {
  enum class InvalidMetadata {
    kOutOfRangeAddition,
    kDuplicateAddition,
    kInvalidAddition,
    kWrongAdditionCount,
    kWrongAdditionBytes,
    kUnknownGarbage,
    kInvalidGarbage,
    kDuplicateGarbage,
    kExcessiveGarbage,
    kSameResultGarbage,
    kDanglingOldestBlobFile,
  };

  for (const InvalidMetadata invalid_metadata :
       {InvalidMetadata::kOutOfRangeAddition,
        InvalidMetadata::kDuplicateAddition, InvalidMetadata::kInvalidAddition,
        InvalidMetadata::kWrongAdditionCount,
        InvalidMetadata::kWrongAdditionBytes, InvalidMetadata::kUnknownGarbage,
        InvalidMetadata::kInvalidGarbage, InvalidMetadata::kDuplicateGarbage,
        InvalidMetadata::kExcessiveGarbage, InvalidMetadata::kSameResultGarbage,
        InvalidMetadata::kDanglingOldestBlobFile}) {
    SCOPED_TRACE(static_cast<int>(invalid_metadata));
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    options.blob_file_size = 1;
    std::unique_ptr<CompactionFilter> compaction_filter(
        new ChangeBlobValueCompactionFilter());
    options.compaction_filter = compaction_filter.get();
    ReopenWithCompactionService(&options);

    ASSERT_OK(Put("key1", std::string(4096, 'a')));
    ASSERT_OK(Put("key3", std::string(4096, 'c')));
    ASSERT_OK(Flush());

    SyncPoint::GetInstance()->SetCallBack(
        "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
          auto** result_ptr = static_cast<CompactionServiceResult**>(arg);
          ASSERT_NE(result_ptr, nullptr);
          CompactionServiceResult* const result = *result_ptr;
          ASSERT_NE(result, nullptr);
          ASSERT_FALSE(result->blob_file_additions.empty());

          CompactionServiceInput input;
          GetCompactionService()->GetInput(&input);
          const BlobFileAddition& addition =
              result->blob_file_additions.front();
          ASSERT_FALSE(result->blob_file_garbages.empty());
          const uint64_t existing_blob_file_number =
              result->blob_file_garbages.front().GetBlobFileNumber();
          switch (invalid_metadata) {
            case InvalidMetadata::kOutOfRangeAddition:
              result->blob_file_additions.front() = BlobFileAddition(
                  input.blob_file_number_limit, addition.GetTotalBlobCount(),
                  addition.GetTotalBlobBytes(), addition.GetChecksumMethod(),
                  addition.GetChecksumValue());
              break;
            case InvalidMetadata::kDuplicateAddition:
              result->blob_file_additions.push_back(addition);
              break;
            case InvalidMetadata::kInvalidAddition:
              result->blob_file_additions.front() = BlobFileAddition(
                  addition.GetBlobFileNumber(), 0, addition.GetTotalBlobBytes(),
                  addition.GetChecksumMethod(), addition.GetChecksumValue());
              break;
            case InvalidMetadata::kWrongAdditionCount:
              result->blob_file_additions.front() = BlobFileAddition(
                  addition.GetBlobFileNumber(),
                  addition.GetTotalBlobCount() + 1,
                  addition.GetTotalBlobBytes(), addition.GetChecksumMethod(),
                  addition.GetChecksumValue());
              break;
            case InvalidMetadata::kWrongAdditionBytes:
              result->blob_file_additions.front() = BlobFileAddition(
                  addition.GetBlobFileNumber(), addition.GetTotalBlobCount(),
                  addition.GetTotalBlobBytes() + 1,
                  addition.GetChecksumMethod(), addition.GetChecksumValue());
              break;
            case InvalidMetadata::kUnknownGarbage:
              result->blob_file_garbages.emplace_back(
                  input.blob_file_number_limit, 1, 1);
              break;
            case InvalidMetadata::kInvalidGarbage:
              result->blob_file_garbages.emplace_back(existing_blob_file_number,
                                                      0, 1);
              break;
            case InvalidMetadata::kDuplicateGarbage:
              result->blob_file_garbages.emplace_back(existing_blob_file_number,
                                                      1, 1);
              result->blob_file_garbages.emplace_back(existing_blob_file_number,
                                                      1, 1);
              break;
            case InvalidMetadata::kExcessiveGarbage:
              result->blob_file_garbages.emplace_back(
                  existing_blob_file_number,
                  std::numeric_limits<uint64_t>::max(),
                  std::numeric_limits<uint64_t>::max());
              break;
            case InvalidMetadata::kSameResultGarbage:
              result->blob_file_garbages.emplace_back(
                  addition.GetBlobFileNumber(), 1, 1);
              break;
            case InvalidMetadata::kDanglingOldestBlobFile:
              ASSERT_FALSE(result->output_files.empty());
              result->output_files.front().oldest_blob_file_number =
                  input.blob_file_number_limit;
              break;
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    const Status status = db_->CompactRange(compact_options, nullptr, nullptr);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_OK(status);
    ASSERT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
              CompactionServiceJobStatus::kUseLocal);
    ASSERT_GE(GetCompactionService()->GetUseLocalInstallationCount(), 1);
    std::string expected_key1(4096, 'a');
    expected_key1[0] = 'z';
    ASSERT_EQ(Get("key1"), expected_key1);
    ASSERT_EQ(Get("key3"), std::string(4096, 'c'));
    ASSERT_OK(Put("post-fallback", "writable"));
    Close();
  }
}

TEST_F(CompactionServiceTest, RejectsGarbageForUntransportedBlobFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeBlobValueCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put("a", std::string(4096, 'a')));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  const std::vector<uint64_t> existing_blob_files = GetBlobFileNumbers();
  ASSERT_FALSE(existing_blob_files.empty());
  const uint64_t unrelated_blob_file = existing_blob_files.front();

  ASSERT_OK(Put("z", std::string(4096, 'z')));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        auto** result_ptr = static_cast<CompactionServiceResult**>(arg);
        ASSERT_NE(result_ptr, nullptr);
        CompactionServiceResult* const result = *result_ptr;
        ASSERT_NE(result, nullptr);
        CompactionServiceInput input;
        GetCompactionService()->GetInput(&input);
        EXPECT_EQ(std::find(input.input_blob_files.begin(),
                            input.input_blob_files.end(),
                            BlobFileName(unrelated_blob_file)),
                  input.input_blob_files.end());
        result->blob_file_garbages.emplace_back(unrelated_blob_file, 1, 1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Slice begin("z");
  const Status status =
      db_->CompactRange(CompactRangeOptions(), &begin, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(status);
  ASSERT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kUseLocal);
  ASSERT_GE(GetCompactionService()->GetUseLocalInstallationCount(), 1);
  ASSERT_EQ(Get("a"), std::string(4096, 'a'));
  std::string expected_z(4096, 'z');
  expected_z[0] = 'z';
  ASSERT_EQ(Get("z"), expected_z);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest, BlobFileCollisionIsDeclinedRecoverably) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new ChangeBlobValueCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  const std::string value(4096, 'a');
  ASSERT_OK(Put("key1", value));
  ASSERT_OK(Flush());

  std::string collision_path;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        auto** result_ptr = static_cast<CompactionServiceResult**>(arg);
        ASSERT_NE(result_ptr, nullptr);
        ASSERT_NE(*result_ptr, nullptr);
        ASSERT_FALSE((*result_ptr)->blob_file_additions.empty());
        collision_path = BlobFileName(
            dbname_,
            (*result_ptr)->blob_file_additions.front().GetBlobFileNumber());
        std::unique_ptr<WritableFile> file;
        ASSERT_OK(env_->NewWritableFile(collision_path, &file, EnvOptions()));
        ASSERT_OK(file->Append("collision"));
        ASSERT_OK(file->Close());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  const Status status = db_->CompactRange(compact_options, nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  ASSERT_EQ(GetCompactionService()->GetFinalCompactionServiceJobStatus(),
            CompactionServiceJobStatus::kUseLocal);
  ASSERT_GE(GetCompactionService()->GetUseLocalInstallationCount(), 1);
  std::string expected = value;
  expected[0] = 'z';
  ASSERT_EQ(Get("key1"), expected);
  ASSERT_OK(Put("post-fallback", "writable"));
  if (!collision_path.empty()) {
    env_->DeleteFile(collision_path).PermitUncheckedError();
  }
}

TEST_F(CompactionServiceTest, RemoteBlobCompactionInstallsEmptyResult) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::unique_ptr<CompactionFilter> compaction_filter(
      new RemoveAllBlobValuesCompactionFilter());
  options.compaction_filter = compaction_filter.get();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put("key", std::string(4096, 'a')));
  ASSERT_OK(Flush());

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  CompactionServiceInput input;
  GetCompactionService()->GetInput(&input);
  ASSERT_GT(input.blob_file_number_start, 0);
  ASSERT_GT(input.blob_file_number_limit, input.blob_file_number_start);

  CompactionServiceResult result;
  GetCompactionService()->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.output_files.empty());
  ASSERT_TRUE(result.blob_file_additions.empty());
  ASSERT_FALSE(result.blob_file_garbages.empty());
  ASSERT_EQ(Get("key"), "NOT_FOUND");
}

TEST_F(CompactionServiceTest, StandaloneDeleteRangeTombstoneOptimization) {
  Options options = CurrentOptions();

  size_t num_files_after_filtered = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::MakeInputIterator:NewCompactionMergingIterator",
      [&](void* arg) {
        num_files_after_filtered = *static_cast<size_t*>(arg);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (auto compaction_style : {CompactionStyle::kCompactionStyleLevel,
                                CompactionStyle::kCompactionStyleUniversal}) {
    SCOPED_TRACE("Style: " + std::to_string(compaction_style));
    options.compaction_style = compaction_style;
    ReopenWithCompactionService(&options);

    num_files_after_filtered = 0;

    std::vector<std::string> files;
    {
      // Writes first version of data in range partitioned files.
      SstFileWriter sst_file_writer(EnvOptions(), options);
      std::string file1 = dbname_ + "file1.sst";
      ASSERT_OK(sst_file_writer.Open(file1));
      ASSERT_OK(sst_file_writer.Put("a", "a1"));
      ASSERT_OK(sst_file_writer.Put("b", "b1"));
      ExternalSstFileInfo file1_info;
      ASSERT_OK(sst_file_writer.Finish(&file1_info));
      files.push_back(std::move(file1));

      std::string file2 = dbname_ + "file2.sst";
      ASSERT_OK(sst_file_writer.Open(file2));
      ASSERT_OK(sst_file_writer.Put("x", "x1"));
      ASSERT_OK(sst_file_writer.Put("y", "y1"));
      ExternalSstFileInfo file2_info;
      ASSERT_OK(sst_file_writer.Finish(&file2_info));
      files.push_back(std::move(file2));
    }

    IngestExternalFileOptions ifo;
    ASSERT_OK(db_->IngestExternalFile(files, ifo));
    ASSERT_EQ(Get("a"), "a1");
    ASSERT_EQ(Get("b"), "b1");
    ASSERT_EQ(Get("x"), "x1");
    ASSERT_EQ(Get("y"), "y1");
    ASSERT_EQ(2, NumTableFilesAtLevel(6));

    auto my_cs = GetCompactionService();
    uint64_t comp_num = my_cs->GetCompactionNum();

    {
      // Atomically delete old version of data with one range delete file.
      // And a new batch of range partitioned files with new version of data.
      files.clear();
      SstFileWriter sst_file_writer(EnvOptions(), options);
      std::string file2 = dbname_ + "file2.sst";
      ASSERT_OK(sst_file_writer.Open(file2));
      ASSERT_OK(sst_file_writer.DeleteRange("a", "z"));
      ExternalSstFileInfo file2_info;
      ASSERT_OK(sst_file_writer.Finish(&file2_info));
      files.push_back(std::move(file2));

      std::string file3 = dbname_ + "file3.sst";
      ASSERT_OK(sst_file_writer.Open(file3));
      ASSERT_OK(sst_file_writer.Put("a", "a2"));
      ASSERT_OK(sst_file_writer.Put("b", "b2"));
      ExternalSstFileInfo file3_info;
      ASSERT_OK(sst_file_writer.Finish(&file3_info));
      files.push_back(std::move(file3));

      std::string file4 = dbname_ + "file4.sst";
      ASSERT_OK(sst_file_writer.Open(file4));
      ASSERT_OK(sst_file_writer.Put("x", "x2"));
      ASSERT_OK(sst_file_writer.Put("y", "y2"));
      ExternalSstFileInfo file4_info;
      ASSERT_OK(sst_file_writer.Finish(&file4_info));
      files.push_back(std::move(file4));
    }

    ASSERT_OK(db_->IngestExternalFile(files, ifo));
    ASSERT_OK(db_->WaitForCompact(WaitForCompactOptions()));
    ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

    CompactionServiceResult result;
    my_cs->GetResult(&result);
    ASSERT_OK(result.status);
    ASSERT_TRUE(result.stats.is_manual_compaction);
    ASSERT_TRUE(result.stats.is_remote_compaction);

    if (compaction_style == kCompactionStyleUniversal) {
      ASSERT_EQ(num_files_after_filtered, 1);
    } else {
      // Not filtered
      ASSERT_EQ(num_files_after_filtered, 3);
    }

    Close();
  }

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(CompactionServiceTest,
       StandaloneDeleteRangeTombstoneMakesInputCountInaccurate) {
  Options options = CurrentOptions();
  options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  options.compaction_verify_record_count = true;
  ReopenWithCompactionService(&options);

  std::vector<std::string> files;
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    std::string file1 = dbname_ + "file1.sst";
    ASSERT_OK(sst_file_writer.Open(file1));
    ASSERT_OK(sst_file_writer.Put("a", "a1"));
    ASSERT_OK(sst_file_writer.Put("b", "b1"));
    ExternalSstFileInfo file1_info;
    ASSERT_OK(sst_file_writer.Finish(&file1_info));
    files.push_back(std::move(file1));

    std::string file2 = dbname_ + "file2.sst";
    ASSERT_OK(sst_file_writer.Open(file2));
    ASSERT_OK(sst_file_writer.Put("x", "x1"));
    ASSERT_OK(sst_file_writer.Put("y", "y1"));
    ExternalSstFileInfo file2_info;
    ASSERT_OK(sst_file_writer.Finish(&file2_info));
    files.push_back(std::move(file2));
  }

  IngestExternalFileOptions ifo;
  ASSERT_OK(db_->IngestExternalFile(files, ifo));
  ASSERT_EQ(Get("a"), "a1");
  ASSERT_EQ(Get("b"), "b1");
  ASSERT_EQ(Get("x"), "x1");
  ASSERT_EQ(Get("y"), "y1");
  ASSERT_EQ(2, NumTableFilesAtLevel(6));

  auto my_cs = GetCompactionService();
  uint64_t comp_num = my_cs->GetCompactionNum();

  size_t num_files_after_filtered = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::MakeInputIterator:NewCompactionMergingIterator",
      [&](void* arg) {
        num_files_after_filtered = *static_cast<size_t*>(arg);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  {
    // The standalone range tombstone can fully cover the old versioned files,
    // so universal compaction may filter those files before iteration.
    files.clear();
    SstFileWriter sst_file_writer(EnvOptions(), options);
    std::string file2 = dbname_ + "file2.sst";
    ASSERT_OK(sst_file_writer.Open(file2));
    ASSERT_OK(sst_file_writer.DeleteRange("a", "z"));
    ExternalSstFileInfo file2_info;
    ASSERT_OK(sst_file_writer.Finish(&file2_info));
    files.push_back(std::move(file2));

    std::string file3 = dbname_ + "file3.sst";
    ASSERT_OK(sst_file_writer.Open(file3));
    ASSERT_OK(sst_file_writer.Put("a", "a2"));
    ASSERT_OK(sst_file_writer.Put("b", "b2"));
    ExternalSstFileInfo file3_info;
    ASSERT_OK(sst_file_writer.Finish(&file3_info));
    files.push_back(std::move(file3));

    std::string file4 = dbname_ + "file4.sst";
    ASSERT_OK(sst_file_writer.Open(file4));
    ASSERT_OK(sst_file_writer.Put("x", "x2"));
    ASSERT_OK(sst_file_writer.Put("y", "y2"));
    ExternalSstFileInfo file4_info;
    ASSERT_OK(sst_file_writer.Finish(&file4_info));
    files.push_back(std::move(file4));
  }

  ASSERT_OK(db_->IngestExternalFile(files, ifo));
  ASSERT_OK(db_->WaitForCompact(WaitForCompactOptions()));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_remote_compaction);
  ASSERT_FALSE(result.stats.has_accurate_num_input_records);
  ASSERT_EQ(1, num_files_after_filtered);

  ASSERT_EQ(Get("a"), "a2");
  ASSERT_EQ(Get("b"), "b2");
  ASSERT_EQ(Get("x"), "x2");
  ASSERT_EQ(Get("y"), "y2");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CompactionServiceTest, CompactionOutputFileIOError) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::FinishCompactionOutputFile()::AfterFinish",
      [&](void* status) {
        // override status
        auto s = static_cast<Status*>(status);
        *s = Status::IOError("Injected IOError!");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_NOK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_NOK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);
}

TEST_F(CompactionServiceTest, PreservedOptionsLocalCompaction) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  Random rnd(301);
  for (auto i = 0; i < 2; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("foo" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::Processing", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);
        std::string options_file_name = OptionsFileName(
            dbname_,
            compaction->input_version()->version_set()->options_file_number());

        // Change option twice to make sure the very first OPTIONS file gets
        // purged
        ASSERT_OK(dbfull()->SetOptions(
            {{"level0_file_num_compaction_trigger", "4"}}));
        ASSERT_EQ(4, dbfull()->GetOptions().level0_file_num_compaction_trigger);
        ASSERT_OK(dbfull()->SetOptions(
            {{"level0_file_num_compaction_trigger", "6"}}));
        ASSERT_EQ(6, dbfull()->GetOptions().level0_file_num_compaction_trigger);
        dbfull()->TEST_DeleteObsoleteFiles();

        // For non-remote compactions, OPTIONS file can be deleted while
        // using option at the start of the compaction
        Status s = env_->FileExists(options_file_name);
        ASSERT_NOK(s);
        ASSERT_TRUE(s.IsNotFound());
        // Should be old value
        ASSERT_EQ(2, compaction->mutable_cf_options()
                         .level0_file_num_compaction_trigger);
        ASSERT_TRUE(dbfull()->min_options_file_numbers_.empty());
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Status s = dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.ok());
}

TEST_F(CompactionServiceTest, PreservedOptionsRemoteCompaction) {
  // For non-remote compaction do not preserve options file
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  Random rnd(301);
  for (auto i = 0; i < 2; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("foo" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"CompactionServiceTest::OptionsFileChanged",
        "DBImplSecondary::OpenAndCompact::BeforeLoadingOptions:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::OpenAndCompact::BeforeLoadingOptions:0",
      [&](void* arg) {
        auto options_file_number = static_cast<uint64_t*>(arg);
        // Change the option twice before the compaction run
        ASSERT_OK(dbfull()->SetOptions(
            {{"level0_file_num_compaction_trigger", "4"}}));
        ASSERT_EQ(4, dbfull()->GetOptions().level0_file_num_compaction_trigger);
        ASSERT_TRUE(dbfull()->versions_->options_file_number() >
                    *options_file_number);

        // Change the option twice before the compaction run
        ASSERT_OK(dbfull()->SetOptions(
            {{"level0_file_num_compaction_trigger", "5"}}));
        ASSERT_EQ(5, dbfull()->GetOptions().level0_file_num_compaction_trigger);
        ASSERT_TRUE(dbfull()->versions_->options_file_number() >
                    *options_file_number);

        TEST_SYNC_POINT("CompactionServiceTest::OptionsFileChanged");
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void* arg) {
        auto input = static_cast<CompactionServiceInput*>(arg);
        std::string options_file_name =
            OptionsFileName(dbname_, input->options_file_number);

        ASSERT_OK(env_->FileExists(options_file_name));
        ASSERT_FALSE(dbfull()->min_options_file_numbers_.empty());
        ASSERT_EQ(dbfull()->min_options_file_numbers_.front(),
                  input->options_file_number);

        DBOptions db_options;
        ConfigOptions config_options;
        std::vector<ColumnFamilyDescriptor> all_column_families;
        config_options.env = env_;
        ASSERT_OK(LoadOptionsFromFile(config_options, options_file_name,
                                      &db_options, &all_column_families));
        bool has_cf = false;
        for (auto& cf : all_column_families) {
          if (cf.name == input->cf_name) {
            // Should be old value
            ASSERT_EQ(2, cf.options.level0_file_num_compaction_trigger);
            has_cf = true;
          }
        }
        ASSERT_TRUE(has_cf);
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::Processing", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);
        ASSERT_EQ(2, compaction->mutable_cf_options()
                         .level0_file_num_compaction_trigger);
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Status s = dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.ok());

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);
}

class EventVerifier : public EventListener {
 public:
  explicit EventVerifier(uint64_t expected_num_input_records,
                         size_t expected_num_input_files,
                         uint64_t expected_num_output_records,
                         size_t expected_num_output_files,
                         const std::string& expected_smallest_output_key_prefix,
                         const std::string& expected_largest_output_key_prefix,
                         bool expected_is_remote_compaction_on_begin,
                         bool expected_is_remote_compaction_on_complete)
      : expected_num_input_records_(expected_num_input_records),
        expected_num_input_files_(expected_num_input_files),
        expected_num_output_records_(expected_num_output_records),
        expected_num_output_files_(expected_num_output_files),
        expected_smallest_output_key_prefix_(
            expected_smallest_output_key_prefix),
        expected_largest_output_key_prefix_(expected_largest_output_key_prefix),
        expected_is_remote_compaction_on_begin_(
            expected_is_remote_compaction_on_begin),
        expected_is_remote_compaction_on_complete_(
            expected_is_remote_compaction_on_complete) {}
  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_EQ(expected_num_input_files_, ci.input_files.size());
    ASSERT_EQ(expected_num_input_files_, ci.input_file_infos.size());
    ASSERT_EQ(expected_is_remote_compaction_on_begin_,
              ci.stats.is_remote_compaction);
    ASSERT_TRUE(ci.stats.is_manual_compaction);
    ASSERT_FALSE(ci.stats.is_full_compaction);
  }
  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_GT(ci.stats.elapsed_micros, 0);
    ASSERT_GT(ci.stats.cpu_micros, 0);
    ASSERT_EQ(expected_num_input_records_, ci.stats.num_input_records);
    ASSERT_EQ(expected_num_input_files_, ci.stats.num_input_files);
    ASSERT_EQ(expected_num_output_records_, ci.stats.num_output_records);
    ASSERT_EQ(expected_num_output_files_, ci.stats.num_output_files);
    ASSERT_EQ(expected_smallest_output_key_prefix_,
              ci.stats.smallest_output_key_prefix);
    ASSERT_EQ(expected_largest_output_key_prefix_,
              ci.stats.largest_output_key_prefix);
    ASSERT_GT(ci.stats.total_input_bytes, 0);
    ASSERT_GT(ci.stats.total_output_bytes, 0);
    ASSERT_EQ(ci.stats.num_input_records,
              ci.stats.num_output_records + ci.stats.num_records_replaced);
    ASSERT_EQ(expected_is_remote_compaction_on_complete_,
              ci.stats.is_remote_compaction);
    ASSERT_TRUE(ci.stats.is_manual_compaction);
    ASSERT_FALSE(ci.stats.is_full_compaction);
  }

 private:
  uint64_t expected_num_input_records_;
  size_t expected_num_input_files_;
  uint64_t expected_num_output_records_;
  size_t expected_num_output_files_;
  std::string expected_smallest_output_key_prefix_;
  std::string expected_largest_output_key_prefix_;
  bool expected_is_remote_compaction_on_begin_;
  bool expected_is_remote_compaction_on_complete_;
};

TEST_F(CompactionServiceTest, VerifyStats) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  auto event_verifier = std::make_shared<EventVerifier>(
      30 /* expected_num_input_records */, 3 /* expected_num_input_files */,
      20 /* expected_num_output_records */, 1 /* expected_num_output_files */,
      "key00000" /* expected_smallest_output_key_prefix */,
      "key00001" /* expected_largest_output_key_prefix */,
      true /* expected_is_remote_compaction_on_begin */,
      true /* expected_is_remote_compaction_on_complete */);
  options.listeners.push_back(event_verifier);
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(0);
  std::string end_str = Key(1);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  VerifyTestData();

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);
}

TEST_F(CompactionServiceTest, VerifyStatsLocalFallback) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  auto event_verifier = std::make_shared<EventVerifier>(
      30 /* expected_num_input_records */, 3 /* expected_num_input_files */,
      20 /* expected_num_output_records */, 1 /* expected_num_output_files */,
      "key00000" /* expected_smallest_output_key_prefix */,
      "key00001" /* expected_largest_output_key_prefix */,
      true /* expected_is_remote_compaction_on_begin */,
      false /* expected_is_remote_compaction_on_complete */);
  options.listeners.push_back(event_verifier);
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kUseLocal);

  std::string start_str = Key(0);
  std::string end_str = Key(1);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  // Remote Compaction did not happen
  ASSERT_EQ(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();
}

TEST_F(CompactionServiceTest, VerifyInputRecordCount) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  // Only iterator through 10 keys and force compaction to finish.
  int num_iter = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::stop", [&](void* stop_ptr) {
        num_iter++;
        if (num_iter == 10) {
          *(bool*)stop_ptr = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // CompactRange() should fail
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsCorruption());
  const char* expected_message =
      "Compaction number of input keys does not match number of keys "
      "processed.";
  ASSERT_TRUE(std::strstr(s.getState(), expected_message));

  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CompactionServiceTest, InaccurateInputRecordCount) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        CompactionServiceResult* compaction_result =
            *(static_cast<CompactionServiceResult**>(arg));
        ASSERT_TRUE(compaction_result != nullptr);
        compaction_result->stats.has_accurate_num_input_records = false;
        compaction_result->stats.num_input_records = 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CompactionServiceTest, EmptyResult) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  // Delete range to cover entire range
  ASSERT_OK(db_->DeleteRange(WriteOptions(), "key", "keyz"));
  ASSERT_OK(Flush());

  // In this unit test, both remote compaction and primary db instance are
  // running in the same process, so NewFileNumber will never have a collision.
  // In the real-world remote compactions, when the compaction is indeed running
  // in another process, this is not going to be the case.
  // To simulate the SST file with the same name created in the tmp directory,
  // override the file number in remote compaction to re-use old SST file
  // number.
  bool need_to_override_file_number = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::OpenAndCompact::BeforeLoadingOptions:0",
      [&](void*) { need_to_override_file_number = true; });

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::OpenCompactionOutputFile::NewFileNumber",
      [&](void* file_number) {
        if (need_to_override_file_number) {
          auto n = static_cast<uint64_t*>(file_number);
          ColumnFamilyMetaData cf_meta;
          db_->GetColumnFamilyMetaData(&cf_meta);
          for (const auto& level : cf_meta.levels) {
            if (!level.files.empty()) {
              // Use one of the existing file name
              *n = test::GetFileNumber(level.files.front().name);
              need_to_override_file_number = false;
              return;
            }
          }
        }
      });

  // Inject failure, so that the remote compaction fails after
  // ProcessKeyValueCompaction()
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CompactWithoutInstallation::End", [&](void* status) {
        // override job status
        auto s = static_cast<Status*>(status);
        *s = Status::Aborted("MyTestCompactionService failed to compact!");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Compaction should fail and SST files in the primary db should exist
  {
    ASSERT_NOK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ColumnFamilyMetaData meta;
    db_->GetColumnFamilyMetaData(&meta);
    for (const auto& level : meta.levels) {
      for (const auto& file : level.files) {
        std::string fname = file.db_path + "/" + file.name;
        ASSERT_OK(db_->GetEnv()->FileExists(fname));
      }
    }
  }
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CompactionServiceTest, CorruptedOutput) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        CompactionServiceResult* compaction_result =
            *(static_cast<CompactionServiceResult**>(arg));
        ASSERT_TRUE(compaction_result != nullptr &&
                    !compaction_result->output_files.empty());
        // Corrupt files here
        for (const auto& output_file : compaction_result->output_files) {
          std::string file_name =
              compaction_result->output_path + "/" + output_file.file_name;

          uint64_t file_size = 0;
          Status s = options.env->GetFileSize(file_name, &file_size);
          ASSERT_OK(s);
          ASSERT_GT(file_size, 0);

          ASSERT_OK(test::CorruptFile(env_, file_name, 0,
                                      static_cast<int>(file_size),
                                      true /* verifyChecksum */));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // CompactRange() should fail
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsCorruption());

  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // On the worker side, the compaction is considered success
  // Verification is done on the primary side
  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);
}

TEST_F(CompactionServiceTest, CorruptedOutputParanoidFileCheck) {
  for (bool paranoid_file_check_enabled : {false, true}) {
    SCOPED_TRACE("paranoid_file_check_enabled=" +
                 std::to_string(paranoid_file_check_enabled));

    Options options = CurrentOptions();
    Destroy(options);
    options.disable_auto_compactions = true;
    options.paranoid_file_checks = paranoid_file_check_enabled;
    options.verify_output_flags = VerifyOutputFlags::kVerifyNone;
    ReopenWithCompactionService(&options);
    GenerateTestData();

    auto my_cs = GetCompactionService();

    std::string start_str = Key(15);
    std::string end_str = Key(45);
    Slice start(start_str);
    Slice end(end_str);
    uint64_t comp_num = my_cs->GetCompactionNum();

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
          CompactionServiceResult* compaction_result =
              *(static_cast<CompactionServiceResult**>(arg));
          ASSERT_TRUE(compaction_result != nullptr &&
                      !compaction_result->output_files.empty());
          // Corrupt files here
          for (const auto& output_file : compaction_result->output_files) {
            std::string file_name =
                compaction_result->output_path + "/" + output_file.file_name;

            // Corrupt very small range of bytes. This corruption is so small
            // that this isn't caught by default light-weight check
            ASSERT_OK(test::CorruptFile(env_, file_name, 0, 1,
                                        false /* verifyChecksum */));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
    if (paranoid_file_check_enabled) {
      ASSERT_NOK(s);
      ASSERT_EQ(Status::Corruption("Paranoid checksums do not match"), s);
    } else {
      // CompactRange() goes through if paranoid file check is not enabled
      ASSERT_OK(s);
    }

    ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    // On the worker side, the compaction is considered success
    // Verification is done on the primary side
    CompactionServiceResult result;
    my_cs->GetResult(&result);
    ASSERT_OK(result.status);
    ASSERT_TRUE(result.stats.is_manual_compaction);
    ASSERT_TRUE(result.stats.is_remote_compaction);
  }
}

TEST_F(CompactionServiceTest, CorruptedOutputVerifyOutputFlags) {
  for (VerifyOutputFlags verify_output_flags :
       {VerifyOutputFlags::kVerifyNone,
        VerifyOutputFlags::kEnableForLocalCompaction |
            VerifyOutputFlags::kVerifyBlockChecksum,
        VerifyOutputFlags::kEnableForRemoteCompaction |
            VerifyOutputFlags::kVerifyBlockChecksum,
        VerifyOutputFlags::kEnableForRemoteCompaction |
            VerifyOutputFlags::kVerifyIteration,
        VerifyOutputFlags::kEnableForRemoteCompaction |
            VerifyOutputFlags::kVerifyFileChecksum,
        VerifyOutputFlags::kVerifyAll}) {
    SCOPED_TRACE(
        "verify_output_flags=" +
        std::to_string(static_cast<std::underlying_type_t<VerifyOutputFlags>>(
            verify_output_flags)));

    Options options = CurrentOptions();
    Destroy(options);
    options.disable_auto_compactions = true;
    options.paranoid_file_checks = false;
    options.verify_output_flags = verify_output_flags;
    options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
    ReopenWithCompactionService(&options);
    GenerateTestData();

    auto my_cs = GetCompactionService();

    std::string start_str = Key(15);
    std::string end_str = Key(45);
    Slice start(start_str);
    Slice end(end_str);
    uint64_t comp_num = my_cs->GetCompactionNum();

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
          CompactionServiceResult* compaction_result =
              *(static_cast<CompactionServiceResult**>(arg));
          ASSERT_TRUE(compaction_result != nullptr &&
                      !compaction_result->output_files.empty());
          // Corrupt files here
          for (const auto& output_file : compaction_result->output_files) {
            std::string file_name =
                compaction_result->output_path + "/" + output_file.file_name;

            // Corrupt very small range of bytes. This corruption is so small
            // that this isn't caught by default light-weight check
            ASSERT_OK(test::CorruptFile(env_, file_name, 0, 1,
                                        false /* verifyChecksum */));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    const bool is_enabled_for_remote_compaction =
        !!(verify_output_flags & VerifyOutputFlags::kEnableForRemoteCompaction);
    const bool should_verify_block_checksum =
        !!(verify_output_flags & VerifyOutputFlags::kVerifyBlockChecksum);
    const bool should_verify_iteration =
        !!(verify_output_flags & VerifyOutputFlags::kVerifyIteration);
    const bool should_verify_file_checksum =
        !!(verify_output_flags & VerifyOutputFlags::kVerifyFileChecksum);

    Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
    if (is_enabled_for_remote_compaction &&
        (should_verify_block_checksum || should_verify_iteration ||
         should_verify_file_checksum)) {
      ASSERT_NOK(s);
      ASSERT_TRUE(s.IsCorruption());
    } else {
      // CompactRange() goes through if block checksum wasn't verified
      ASSERT_OK(s);
    }

    ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    // On the worker side, the compaction is considered success
    // Verification is done on the primary side
    CompactionServiceResult result;
    my_cs->GetResult(&result);
    ASSERT_OK(result.status);
    ASSERT_TRUE(result.stats.is_manual_compaction);
    ASSERT_TRUE(result.stats.is_remote_compaction);
  }
}

TEST_F(CompactionServiceTest, TruncatedOutput) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  // Skip calculating tail size to avoid crashing due to truncated file size
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FileMetaData::CalculateTailSize", [&](void* arg) {
        bool* skip = static_cast<bool*>(arg);
        *skip = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        CompactionServiceResult* compaction_result =
            *(static_cast<CompactionServiceResult**>(arg));
        ASSERT_TRUE(compaction_result != nullptr &&
                    !compaction_result->output_files.empty());
        // Truncate files here
        for (const auto& output_file : compaction_result->output_files) {
          std::string file_name =
              compaction_result->output_path + "/" + output_file.file_name;

          uint64_t file_size = 0;
          Status s = options.env->GetFileSize(file_name, &file_size);
          ASSERT_OK(s);
          ASSERT_GT(file_size, 0);

          ASSERT_OK(test::TruncateFile(env_, file_name, file_size / 4));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // CompactRange() should fail
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsCorruption());

  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // On the worker side, the compaction is considered success
  // Verification is done on the primary side
  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);
}

TEST_F(CompactionServiceTest, CustomFileChecksum) {
  Options options = CurrentOptions();
  // Pin compression so the auto-compacted LSM shape (and thus whether the
  // manual CompactRange below schedules a remote compaction) doesn't depend on
  // the default compression type. kNoCompression is always available.
  options.compression = kNoCompression;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        CompactionServiceResult* compaction_result =
            *(static_cast<CompactionServiceResult**>(arg));
        ASSERT_TRUE(compaction_result != nullptr &&
                    !compaction_result->output_files.empty());
        // Validate Checksum files here
        for (const auto& output_file : compaction_result->output_files) {
          std::string file_name =
              compaction_result->output_path + "/" + output_file.file_name;

          FileChecksumGenContext gen_context;
          gen_context.file_name = file_name;
          std::unique_ptr<FileChecksumGenerator> file_checksum_gen =
              options.file_checksum_gen_factory->CreateFileChecksumGenerator(
                  gen_context);

          std::unique_ptr<SequentialFile> file_reader;
          uint64_t file_size = 0;
          Status s = options.env->GetFileSize(file_name, &file_size);
          ASSERT_OK(s);
          ASSERT_GT(file_size, 0);

          s = options.env->NewSequentialFile(file_name, &file_reader,
                                             EnvOptions());
          ASSERT_OK(s);

          Slice result;
          std::unique_ptr<char[]> scratch(new char[file_size]);
          s = file_reader->Read(file_size, &result, scratch.get());
          ASSERT_OK(s);

          file_checksum_gen->Update(scratch.get(), result.size());
          file_checksum_gen->Finalize();

          // Verify actual checksum and the func name
          ASSERT_EQ(file_checksum_gen->Name(),
                    output_file.file_checksum_func_name);
          ASSERT_EQ(file_checksum_gen->GetChecksum(),
                    output_file.file_checksum);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_TRUE(result.stats.is_manual_compaction);
  ASSERT_TRUE(result.stats.is_remote_compaction);
}

TEST_F(CompactionServiceTest, CancelCompactionOnRemoteSide) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  // Test cancel compaction at the beginning
  my_cs->SetCanceled(true);
  auto s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();

  // Test cancel compaction in progress
  ReopenWithCompactionService(&options);
  GenerateTestData();
  my_cs = GetCompactionService();
  my_cs->SetCanceled(false);

  std::atomic_bool cancel_issued{false};
  SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Inprogress",
                                        [&](void* /*arg*/) {
                                          cancel_issued = true;
                                          my_cs->SetCanceled(true);
                                        });

  SyncPoint::GetInstance()->EnableProcessing();

  s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
  ASSERT_TRUE(cancel_issued);
  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
  VerifyTestData();
}

TEST_F(CompactionServiceTest, CancelCompactionOnPrimarySide) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  auto my_cs = GetCompactionService();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  ReopenWithCompactionService(&options);
  GenerateTestData();
  my_cs = GetCompactionService();

  // Primary DB calls CancelAllBackgroundWork() while the compaction is running
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Inprogress", [&](void* /*arg*/) {
        CancelAllBackgroundWork(db_.get(), false /*wait*/);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();

  // Check canceled_ was set to true by CancelAwaitingJobs()
  ASSERT_TRUE(my_cs->GetCanceled());

  // compaction number is not increased
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num);
}

TEST_F(CompactionServiceTest, FailedToStart) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kFailure);

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
}

TEST_F(CompactionServiceTest, InvalidResultIsDeclinedRecoverably) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();

  auto my_cs = GetCompactionService();
  ASSERT_EQ(0, my_cs->GetOnInstallationCount());
  my_cs->OverrideWaitResult("Invalid Str");
  const int compaction_num_before = my_cs->GetCompactionNum();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  // The remote result is malformed before any installation starts, so RocksDB
  // reruns the compaction locally.
  const Status s = db_->CompactRange(CompactRangeOptions(), &start, &end);
  ASSERT_OK(s);
  ASSERT_GE(my_cs->GetCompactionNum(), compaction_num_before + 1);
  VerifyTestData();
  ASSERT_EQ(1, my_cs->GetOnInstallationCount());
  ASSERT_EQ(CompactionServiceJobStatus::kUseLocal,
            my_cs->GetFinalCompactionServiceJobStatus());
  ASSERT_EQ(my_cs->GetUseLocalInstallationCount(), 1);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest, CompatCheckCountsWidthTolerance) {
  // Number of per-CompactionReason counts this binary serializes and expects.
  constexpr size_t kExpected =
      static_cast<size_t>(CompactionReason::kNumOfReasons);
  constexpr size_t kLastIndex = kExpected - 1;
  constexpr int kFirstCount = 3;
  constexpr int kLastCount = 5;

  CompactionServiceResult result;
  result.status = Status::OK();
  result.output_level = 2;
  result.output_path = "/tmp/output";
  result.internal_stats.output_level_stats.counts[0] = kFirstCount;
  result.internal_stats.output_level_stats.counts[kLastIndex] = kLastCount;
  std::string serialized;
  ASSERT_OK(result.Write(&serialized));

  // Guard the produced format: this binary writes exactly kExpected counts.
  // That element count is what a remote worker hands to the primary to read,
  // so pinning it catches an accidental change to what is written.
  ASSERT_EQ(CountFirstCountsFieldElements(serialized), kExpected);

  // Peer agrees on kNumOfReasons: every written count is read back unchanged.
  {
    CompactionServiceResult parsed;
    ASSERT_OK(CompactionServiceResult::Read(serialized, &parsed));
    ASSERT_OK(parsed.status);
    const auto& counts = parsed.internal_stats.output_level_stats.counts;
    ASSERT_EQ(counts[0], kFirstCount);
    ASSERT_EQ(counts[kLastIndex], kLastCount);
  }

  // Peer serialized one more count than this binary knows (a worker with an
  // extra CompactionReason): the surplus trailing count is ignored, and the
  // counts this binary understands are read unchanged.
  {
    std::string from_wider_peer = serialized;
    AppendToCountsFields(&from_wider_peer, "7");
    ASSERT_EQ(CountFirstCountsFieldElements(from_wider_peer), kExpected + 1);
    CompactionServiceResult parsed;
    ASSERT_OK(CompactionServiceResult::Read(from_wider_peer, &parsed));
    ASSERT_OK(parsed.status);
    const auto& counts = parsed.internal_stats.output_level_stats.counts;
    ASSERT_EQ(counts[0], kFirstCount);
    ASSERT_EQ(counts[kLastIndex], kLastCount);
  }

  // Peer serialized one fewer count than this binary knows (a worker missing
  // the newest CompactionReason): the absent trailing count reads as zero.
  {
    std::string from_narrower_peer = serialized;
    RemoveLastFromCountsFields(&from_narrower_peer);
    ASSERT_EQ(CountFirstCountsFieldElements(from_narrower_peer), kExpected - 1);
    CompactionServiceResult parsed;
    ASSERT_OK(CompactionServiceResult::Read(from_narrower_peer, &parsed));
    ASSERT_OK(parsed.status);
    const auto& counts = parsed.internal_stats.output_level_stats.counts;
    ASSERT_EQ(counts[0], kFirstCount);
    ASSERT_EQ(counts[kLastIndex], 0);
  }
}

TEST_F(CompactionServiceTest, SubCompaction) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;  // 1KB
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  int compaction_num_before = my_cs->GetCompactionNum();

  auto cro = CompactRangeOptions();
  cro.max_subcompactions = 10;
  Status s = db_->CompactRange(cro, nullptr, nullptr);
  ASSERT_OK(s);
  VerifyTestData();
  int compaction_num = my_cs->GetCompactionNum() - compaction_num_before;
  // make sure there's sub-compaction by checking the compaction number
  ASSERT_GE(compaction_num, 2);

  std::vector<CompactionServiceInput> inputs = my_cs->GetInputs();
  ASSERT_EQ(inputs.size(), static_cast<size_t>(compaction_num));
  std::sort(
      inputs.begin(), inputs.end(),
      [](const CompactionServiceInput& lhs, const CompactionServiceInput& rhs) {
        return lhs.blob_file_number_start < rhs.blob_file_number_start;
      });
  std::set<std::vector<std::string>> distinct_blob_manifests;
  std::set<std::string> distinct_blob_files;
  for (size_t i = 0; i < inputs.size(); ++i) {
    EXPECT_GT(inputs[i].blob_file_number_start, 0);
    EXPECT_GT(inputs[i].blob_file_number_limit,
              inputs[i].blob_file_number_start);
    EXPECT_FALSE(inputs[i].input_blob_files.empty());
    distinct_blob_manifests.insert(inputs[i].input_blob_files);
    distinct_blob_files.insert(inputs[i].input_blob_files.begin(),
                               inputs[i].input_blob_files.end());
    if (i > 0) {
      EXPECT_LE(inputs[i - 1].blob_file_number_limit,
                inputs[i].blob_file_number_start);
    }
  }
  EXPECT_GT(distinct_blob_manifests.size(), 1);
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_INPUT_BLOB_FILE_COUNT),
            distinct_blob_files.size());
}

TEST_F(CompactionServiceTest, BlobManifestTickerIgnoresReservationFallback) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put("key", std::string(4096, 'a')));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompactionWithCompactionService:"
      "BlobFileNumberRangeSize",
      [](void* arg) {
        *static_cast<uint64_t*>(arg) = std::numeric_limits<uint64_t>::max();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_EQ(GetPrimaryStatistics()->getTickerCount(
                REMOTE_COMPACT_INPUT_BLOB_FILE_COUNT),
            0);
  EXPECT_EQ(GetCompactionService()->GetCompactionNum(), 0);
  ASSERT_OK(Put("post-fallback", "writable"));
}

TEST_F(CompactionServiceTest, RejectsExcessiveCrossSubcompactionBlobGarbage) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 30;
  ReopenWithCompactionService(&options);

  GenerateTestData(/*move_files_manually=*/true);
  VerifyTestData();

  constexpr int kBlobKeys = 3;
  const std::string blob_value(4096, 'b');
  const std::array<std::string, kBlobKeys> blob_keys = {
      "0000_blob", "5000_blob", "9999_blob"};
  for (const std::string& key : blob_keys) {
    ASSERT_OK(Put(key, blob_value));
  }
  ASSERT_OK(Flush());

  const std::vector<uint64_t> blob_file_numbers = GetBlobFileNumbers();
  ASSERT_FALSE(blob_file_numbers.empty());
  const uint64_t blob_file_number = blob_file_numbers.back();
  VersionSet* const versions = dbfull()->GetVersionSet();
  ASSERT_NE(versions, nullptr);
  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);
  const std::shared_ptr<BlobFileMetaData> blob_file =
      cfd->current()->storage_info()->GetBlobFileMetaData(blob_file_number);
  ASSERT_NE(blob_file, nullptr);
  ASSERT_EQ(blob_file->GetTotalBlobCount(), kBlobKeys);
  ASSERT_EQ(blob_file->GetGarbageBlobCount(), 0);
  ASSERT_EQ(blob_file->GetGarbageBlobBytes(), 0);

  port::Mutex callback_mutex;
  port::CondVar callback_cv(&callback_mutex);
  int callbacks_ready = 0;
  std::atomic<size_t> modified_results{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::Run:0", [&](void* arg) {
        int callback_index = 0;
        {
          MutexLock lock(&callback_mutex);
          callback_index = callbacks_ready++;
          if (callbacks_ready < 2) {
            while (callbacks_ready < 2) {
              callback_cv.Wait();
            }
          } else {
            callback_cv.SignalAll();
          }
        }
        if (callback_index >= 2) {
          return;
        }
        auto** result_ptr = static_cast<CompactionServiceResult**>(arg);
        ASSERT_NE(result_ptr, nullptr);
        CompactionServiceResult* const result = *result_ptr;
        ASSERT_NE(result, nullptr);
        result->blob_file_garbages.clear();
        result->blob_file_garbages.emplace_back(
            blob_file_number, blob_file->GetTotalBlobCount() - 1, 1);
        ++modified_results;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  CompactRangeOptions compact_options;
  compact_options.max_subcompactions = 10;
  const Status status = db_->CompactRange(compact_options, nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  ASSERT_EQ(modified_results.load(), 2);
  ASSERT_EQ(GetCompactionService()->GetUseLocalInstallationCount(), 1);
  VerifyTestData();
  for (const std::string& key : blob_keys) {
    ASSERT_EQ(Get(key), blob_value);
  }
  ASSERT_OK(Put("post-fallback", "writable"));
}

enum class LocalRouting { kRemote, kSchedule, kWait, kInvalidResult };

class PoolTestCompactionService : public MyTestCompactionService {
 public:
  using MyTestCompactionService::MyTestCompactionService;

  void SetRouting(LocalRouting routing) { routing_ = routing; }
  void SetScheduleObserver(
      std::function<void(const CompactionServiceJobInfo&)> observer) {
    schedule_observer_ = std::move(observer);
  }
  void SetLocalCF(const std::string& cf) { local_cf_ = cf; }
  void SetLocalSubcompactions(int count) { local_subcompactions_ = count; }
  void SetSubcompactionException(uint32_t sub_job_id, bool unknown) {
    throwing_subcompaction_ = sub_job_id;
    throw_unknown_ = unknown;
  }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& info, const std::string& input) override {
    if (schedule_observer_) {
      schedule_observer_(info);
    }
    if ((info.job_id & UINT32_MAX) == throwing_subcompaction_) {
      if (throw_unknown_) {
        throw 42;
      }
      throw std::runtime_error("Injected subcompaction exception");
    }
    if (routing_ == LocalRouting::kSchedule ||
        (info.cf_name == local_cf_ &&
         local_decisions_.fetch_add(1) < local_subcompactions_)) {
      return CompactionServiceScheduleResponse(
          CompactionServiceJobStatus::kUseLocal);
    }
    return MyTestCompactionService::Schedule(info, input);
  }

  CompactionServiceJobStatus Wait(const std::string& id,
                                  std::string* result) override {
    {
      std::unique_lock<std::mutex> lock(wait_mutex_);
      ++waiting_;
      wait_cv_.notify_all();
      wait_cv_.wait(lock, [&] { return released_; });
    }
    if (routing_ == LocalRouting::kWait) {
      return CompactionServiceJobStatus::kUseLocal;
    }
    if (routing_ == LocalRouting::kInvalidResult) {
      *result = "invalid result";
      return CompactionServiceJobStatus::kSuccess;
    }
    return MyTestCompactionService::Wait(id, result);
  }

  bool WaitForRemote(int count) {
    std::unique_lock<std::mutex> lock(wait_mutex_);
    bool ready = wait_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return waiting_ >= count; });
    EXPECT_GE(waiting_, count);
    return ready;
  }

  void Release() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    released_ = true;
    wait_cv_.notify_all();
  }

  void ResetWait() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    waiting_ = 0;
    released_ = false;
  }

 private:
  std::function<void(const CompactionServiceJobInfo&)> schedule_observer_;
  LocalRouting routing_ = LocalRouting::kRemote;
  uint64_t throwing_subcompaction_ = UINT64_MAX;
  bool throw_unknown_ = false;
  std::string local_cf_;
  int local_subcompactions_ = 100;
  std::atomic<int> local_decisions_{0};
  std::mutex wait_mutex_;
  std::condition_variable wait_cv_;
  int waiting_ = 0;
  bool released_ = false;
};

class PoolPressureListener : public EventListener {
 public:
  void OnBackgroundJobPressureChanged(
      DB*, const BackgroundJobPressure& pressure) override {
    std::lock_guard<std::mutex> lock(mutex_);
    pressure_ = pressure;
    cv_.notify_all();
  }

  BackgroundJobPressure WaitForCompactionPressure() {
    std::unique_lock<std::mutex> lock(mutex_);
    EXPECT_TRUE(cv_.wait_for(lock, std::chrono::seconds(10), [&] {
      return pressure_.compaction_scheduled > 0;
    }));
    return pressure_;
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    pressure_ = BackgroundJobPressure();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  BackgroundJobPressure pressure_;
};

class ShortcutCompactionListener : public EventListener {
 public:
  explicit ShortcutCompactionListener(bool gate) : gate_(gate) {}

  void OnCompactionBegin(DB*, const CompactionJobInfo& info) override {
    EXPECT_EQ(info.use_separate_remote_compaction_pool, gate_);
    moved_files = info.stats.num_input_files_trivially_moved;
    ++begun;
  }

  void OnCompactionPreCommit(DB*, const CompactionJobInfo& info) override {
    EXPECT_EQ(info.use_separate_remote_compaction_pool, gate_);
    ++precommitted;
  }

  void OnCompactionCompleted(DB*, const CompactionJobInfo& info) override {
    EXPECT_EQ(info.use_separate_remote_compaction_pool, gate_);
    ++completed;
  }

  void Verify(int count) {
    EXPECT_EQ(begun.load(), count);
    EXPECT_EQ(precommitted.load(), count);
    EXPECT_EQ(completed.load(), count);
  }

  std::atomic<int> begun{0};
  std::atomic<int> precommitted{0};
  std::atomic<int> completed{0};
  std::atomic<uint64_t> moved_files{0};

 private:
  const bool gate_;
};

class CompactionContextListener : public EventListener {
 public:
  void OnCompactionBegin(DB*, const CompactionJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    EXPECT_TRUE(jobs_
                    .emplace(std::make_pair(info.cf_id, info.job_id),
                             Counts{info.use_separate_remote_compaction_pool})
                    .second);
    ++begun_;
    gated_jobs_ += info.use_separate_remote_compaction_pool ? 1 : 0;
  }

  void OnSchedule(const CompactionServiceJobInfo& info) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = jobs_.find({info.cf_id, static_cast<int>(info.job_id >> 32)});
    ASSERT_NE(it, jobs_.end());
    EXPECT_EQ(info.use_separate_remote_compaction_pool,
              it->second.separate_pool);
    ++scheduled_;
  }

  void OnSubcompactionBegin(const SubcompactionJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = jobs_.find({info.cf_id, info.job_id});
    ASSERT_NE(it, jobs_.end());
    EXPECT_EQ(info.use_separate_remote_compaction_pool,
              it->second.separate_pool);
    ++it->second.begun;
  }

  void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = jobs_.find({info.cf_id, info.job_id});
    ASSERT_NE(it, jobs_.end());
    EXPECT_EQ(info.use_separate_remote_compaction_pool,
              it->second.separate_pool);
    ++it->second.completed;
  }

  void OnCompactionPreCommit(DB*, const CompactionJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = jobs_.find({info.cf_id, info.job_id});
    ASSERT_NE(it, jobs_.end());
    EXPECT_EQ(info.use_separate_remote_compaction_pool,
              it->second.separate_pool);
    EXPECT_EQ(it->second.begun, 3);
    EXPECT_EQ(it->second.completed, 3);
    it->second.precommitted = true;
  }

  void OnCompactionCompleted(DB*, const CompactionJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = jobs_.find({info.cf_id, info.job_id});
    ASSERT_NE(it, jobs_.end());
    EXPECT_EQ(info.use_separate_remote_compaction_pool,
              it->second.separate_pool);
    EXPECT_TRUE(it->second.precommitted);
    jobs_.erase(it);
    ++completed_;
  }

  void Verify() {
    std::lock_guard<std::mutex> lock(mutex_);
    EXPECT_TRUE(jobs_.empty());
    EXPECT_EQ(begun_, 2);
    EXPECT_EQ(gated_jobs_, 1);
    EXPECT_EQ(completed_, 2);
    EXPECT_EQ(scheduled_, 6);
  }

 private:
  struct Counts {
    bool separate_pool;
    int begun = 0;
    int completed = 0;
    bool precommitted = false;
  };
  std::mutex mutex_;
  std::map<std::pair<uint32_t, int>, Counts> jobs_;
  int gated_jobs_ = 0;
  int begun_ = 0;
  int completed_ = 0;
  int scheduled_ = 0;
};

class CompactFilesPoolGateListener : public EventListener {
 public:
  explicit CompactFilesPoolGateListener(bool gate) : gate_(gate) {}
  void OnCompactionBegin(DB*, const CompactionJobInfo&) override { ++parents; }
  void OnSchedule(const CompactionServiceJobInfo& info) {
    EXPECT_EQ(info.use_separate_remote_compaction_pool, gate_);
    ++scheduled;
  }
  void OnSubcompactionBegin(const SubcompactionJobInfo& info) override {
    EXPECT_EQ(info.use_separate_remote_compaction_pool, gate_);
    ++begun;
  }
  void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
    EXPECT_EQ(info.use_separate_remote_compaction_pool, gate_);
    ++completed;
  }
  std::atomic<int> parents{0};
  std::atomic<int> scheduled{0};
  std::atomic<int> begun{0};
  std::atomic<int> completed{0};

 private:
  const bool gate_;
};

class DedicatedCompactionPoolTest : public CompactionServiceTest {
 protected:
  Options SetUpCompactions(
      LocalRouting routing, int subcompactions = 1, int local_threads = 1,
      int local_limit = 1, int num_levels = 7, bool separate_pool = true,
      bool thread_tracking = false,
      const CompactionFilter* compaction_filter = nullptr) {
    Options options = CurrentOptions();
    options.env = env_;
    options.disable_auto_compactions = true;
    options.enable_thread_tracking = thread_tracking;
    options.compaction_filter = compaction_filter;
    if (extra_listener_) {
      options.listeners.push_back(extra_listener_);
    }
    if (pressure_listener_) {
      options.listeners.push_back(pressure_listener_);
    }
    options.level0_file_num_compaction_trigger = 4;
    options.max_background_compactions = local_limit;
    if (local_limit > 1) {
      options.level0_slowdown_writes_trigger = 4;
    }
    options.max_background_flushes = 1;
    options.max_subcompactions = subcompactions;
    options.target_file_size_base = 4096;
    options.compression = kNoCompression;
    options.num_levels = num_levels;
    env_->SetBackgroundThreads(local_threads, Env::Priority::LOW);
    env_->SetBackgroundThreads(1, Env::Priority::BOTTOM);
    std::shared_ptr<Statistics> statistics = CreateDBStatistics();
    service_ = std::make_shared<PoolTestCompactionService>(
        dbname_, options, statistics,
        std::vector<std::shared_ptr<EventListener>>{},
        std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
    service_->SetRouting(routing);
    options.compaction_service = service_;
    options.use_separate_remote_compaction_pool = separate_pool;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"cf_1", "cf_2", "cf_3"}, options);
    service_->SetCanceled(false);
    env_->SetBackgroundThreads(local_threads, Env::Priority::LOW);
    for (int cf = 0; cf < 4; ++cf) {
      WriteCompactionInputs(cf);
    }
    return options;
  }

  void WriteCompactionInputs(int cf) {
    for (int file = 0; file < 4; ++file) {
      for (int key = 0; key < 200; ++key) {
        ASSERT_OK(Put(cf, Key(key + file * 17),
                      std::string(1024, static_cast<char>('a' + file))));
      }
      ASSERT_OK(Flush(cf));
    }
  }

  Options SetUpShortcutCompaction(int kind, bool gate,
                                  std::shared_ptr<EventListener> listener) {
    Options options = CurrentOptions();
    options.env = env_;
    options.disable_auto_compactions = true;
    options.compaction_style = kCompactionStyleLevel;
    options.level0_file_num_compaction_trigger = 1;
    options.max_background_compactions = 1;
    options.max_background_flushes = 1;
    options.max_open_files = -1;
    options.use_separate_remote_compaction_pool = gate;
    options.listeners.push_back(std::move(listener));
    if (kind != 0) {
      options.compaction_style = kCompactionStyleFIFO;
      options.num_levels = 1;
      options.compaction_options_fifo.max_table_files_size =
          kind == 1 ? 1 : 1024 * 1024 * 1024;
      if (kind == 2) {
        options.compaction_options_fifo.file_temperature_age_thresholds = {
            {Temperature::kCold, 30}};
        options.compaction_options_fifo
            .allow_trivial_copy_when_change_temperature = true;
        options.default_write_temperature = Temperature::kWarm;
        env_->SetMockSleep();
      }
    }
    env_->SetBackgroundThreads(1, Env::Priority::LOW);
    env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
    ReopenWithCompactionService(&options);
    return options;
  }

  void BlockLocalWork() {
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BGWorkLocalCompaction:Start", [this](void*) {
          std::unique_lock<std::mutex> lock(local_mutex_);
          ++local_started_;
          ++local_active_;
          peak_local_active_ = std::max(peak_local_active_, local_active_);
          local_cv_.notify_all();
          local_cv_.wait(lock, [&] { return release_local_; });
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BGWorkLocalCompaction:Finished", [this](void*) {
          std::lock_guard<std::mutex> lock(local_mutex_);
          --local_active_;
          ++local_finished_;
          local_cv_.notify_all();
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RunLocalCompaction:WaitForCredit", [this](void*) {
          std::lock_guard<std::mutex> lock(local_mutex_);
          ++credit_waits_;
          local_cv_.notify_all();
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  bool WaitForLocal(int started, int credit_waits, int finished = 0) {
    std::unique_lock<std::mutex> lock(local_mutex_);
    bool ready = local_cv_.wait_for(lock, std::chrono::seconds(10), [&] {
      return local_started_ >= started && credit_waits_ >= credit_waits &&
             local_finished_ >= finished;
    });
    EXPECT_GE(local_started_, started);
    EXPECT_GE(credit_waits_, credit_waits);
    EXPECT_GE(local_finished_, finished);
    return ready;
  }

  void ReleaseLocal() {
    std::lock_guard<std::mutex> lock(local_mutex_);
    release_local_ = true;
    local_cv_.notify_all();
  }

  int RemotePoolThreads() {
    uint64_t threads = 0;
    if (db_) {
      EXPECT_TRUE(db_->GetIntProperty(
          DB::Properties::kNumRemoteCompactionPoolThreads, &threads));
      EXPECT_EQ(
          threads,
          static_cast<uint64_t>(
              RemoteCompactionThreadPool::GetShared()->GetBackgroundThreads()));
    } else {
      threads = RemoteCompactionThreadPool::GetShared()->GetBackgroundThreads();
    }
    return static_cast<int>(threads);
  }

  bool SharesRemotePool(DBImpl* other) {
    return dbfull()->remote_compaction_pool_ == other->remote_compaction_pool_;
  }

  int LocalScheduled() {
    InstrumentedMutexLock lock(&dbfull()->mutex_);
    return dbfull()->LocalCompactionsScheduled();
  }

  int ManualCompactionPauseCount() {
    return dbfull()->manual_compaction_paused_.load();
  }

  void WaitForRemotePoolIdle() {
    InstrumentedMutexLock lock(&dbfull()->mutex_);
    while (dbfull()->remote_compaction_pool_jobs_ != 0) {
      dbfull()->bg_cv_.Wait();
    }
    EXPECT_EQ(dbfull()->remote_coordinators_scheduled_, 0);
  }

  void CompleteLocalTaskAfterStatusCopyFailure(Env::Priority priority,
                                               bool queued) {
    SetUpCompactions(LocalRouting::kRemote);
    if (queued) {
      BlockPool(priority);
    }
    std::atomic<bool> fail_copy{false};
    std::atomic<int> failures{0};
    std::atomic<int> work_runs{0};
    const auto set_error = [&] {
      dbfull()->mutex_.AssertHeld();
      dbfull()->error_handler_.SetBGErrorNoAutoRecovery(
          Status::IOError("Injected background error"),
          BackgroundErrorReason::kCompaction);
      EXPECT_TRUE(dbfull()->error_handler_.IsBGWorkStopped());
    };
    if (queued) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::RunLocalCompaction:Scheduled", [&](void*) { set_error(); });
      SyncPoint::GetInstance()->SetCallBack(
          "ThreadPoolImpl::UnSchedule:BeforeRemove",
          [&](void*) { fail_copy = true; });
    } else {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::BGWorkLocalCompaction:BeforeStatus", [&](void*) {
            set_error();
            fail_copy = true;
          });
    }
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::LocalCompactionStatus:BeforeGetBGError", [&](void* arg) {
          if (fail_copy.exchange(false)) {
            *static_cast<std::function<void()>*>(arg) = [&] {
              ++failures;
              throw std::bad_alloc();
            };
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    std::atomic<bool> canceled{false};
    Status result;
    bool completed = false;
    std::thread coordinator([&] {
      result = dbfull()->RunLocalCompaction(
          [&] { ++work_runs; }, priority,
          static_cast<ColumnFamilyHandleImpl*>(handles_[0])->cfd(), canceled,
          false);
      std::lock_guard<std::mutex> lock(local_mutex_);
      completed = true;
      local_cv_.notify_all();
    });
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                     [&] { return completed; }));
    }
    coordinator.join();
    EXPECT_TRUE(result.IsAborted());
    EXPECT_EQ(failures.load(), 1);
    EXPECT_EQ(work_runs.load(), 0);
    EXPECT_EQ(LocalScheduled(), 0);
    {
      InstrumentedMutexLock lock(&dbfull()->mutex_);
      EXPECT_EQ(dbfull()->local_compactions_running_by_priority_[0], 0);
      EXPECT_EQ(dbfull()->local_compactions_running_by_priority_[1], 0);
    }
    EXPECT_EQ(env_->GetThreadPoolQueueLen(priority), 0U);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ReleasePools();
    Close();
  }

  void RunPoolMaintenance(uint64_t idle_seconds, DBImpl* db = nullptr) {
    SyncPoint::GetInstance()->SetCallBack(
        "RemoteCompactionThreadPool::Shrink:IdleNanos",
        [idle_seconds](void* arg) {
          *static_cast<uint64_t*>(arg) = idle_seconds * 1000 * 1000 * 1000;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    (db ? db : dbfull())
        ->periodic_task_functions_.at(PeriodicTaskType::kFlushInfoLog)();
    SyncPoint::GetInstance()->ClearCallBack(
        "RemoteCompactionThreadPool::Shrink:IdleNanos");
  }

  void AbortQueuedFallback(Env::Priority priority, int subcompactions,
                           bool from_worker, bool reject_first_cancel = false) {
    SetUpCompactions(LocalRouting::kSchedule, subcompactions, 1, 4,
                     priority == Env::Priority::BOTTOM ? 2 : 7);
    if (priority == Env::Priority::LOW) {
      env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
    }
    BlockLocalWork();
    std::atomic<int> cancellation_attempts{0};
    if (reject_first_cancel) {
      SyncPoint::GetInstance()->SetCallBack(
          "ThreadPoolImpl::UnSchedule:BeforeRemove", [&](void* arg) {
            if (cancellation_attempts.fetch_add(1) == 0) {
              *static_cast<std::function<void()>*>(arg) = [] {
                throw std::bad_alloc();
              };
            }
          });
    }
    bool occupied = false;
    bool abort_requested = false;
    bool aborted = false;
    bool release_worker = false;
    bool worker_done = false;
    int queued = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RunLocalCompaction:Scheduled", [&](void*) {
          std::lock_guard<std::mutex> lock(local_mutex_);
          ++queued;
          local_cv_.notify_all();
        });
    std::function<void()> occupy = [&] {
      {
        std::unique_lock<std::mutex> lock(local_mutex_);
        occupied = true;
        local_cv_.notify_all();
        local_cv_.wait(lock, [&] { return abort_requested; });
      }
      if (from_worker) {
        db_->AbortCompactions(handles_[0]);
        std::lock_guard<std::mutex> lock(local_mutex_);
        aborted = true;
        local_cv_.notify_all();
      }
      std::unique_lock<std::mutex> lock(local_mutex_);
      local_cv_.wait(lock, [&] { return release_worker; });
      worker_done = true;
      local_cv_.notify_all();
    };
    env_->Schedule(
        [](void* arg) { (*static_cast<std::function<void()>*>(arg))(); },
        &occupy, priority);
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                     [&] { return occupied; }));
    }
    EXPECT_OK(dbfull()->EnableAutoCompaction({handles_[0], handles_[1]}));
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10), [&] {
        return queued >= std::min(4, 2 * subcompactions);
      }));
      abort_requested = true;
      local_cv_.notify_all();
    }
    std::thread abort_thread;
    if (!from_worker) {
      abort_thread = std::thread([&] {
        db_->AbortCompactions(handles_[0]);
        std::lock_guard<std::mutex> lock(local_mutex_);
        aborted = true;
        local_cv_.notify_all();
      });
    }
    bool abort_finished;
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      abort_finished = local_cv_.wait_for(lock, std::chrono::seconds(10),
                                          [&] { return aborted; });
      EXPECT_TRUE(abort_finished);
      if (abort_finished) {
        EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10), [&] {
          return env_->GetThreadPoolQueueLen(priority) ==
                 static_cast<unsigned int>(subcompactions);
        }));
        EXPECT_EQ(local_started_, 0);
      }
    }
    if (abort_finished) {
      EXPECT_EQ(LocalScheduled(), subcompactions);
      EXPECT_EQ(NumTableFilesAtLevel(0), 4);
      EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
    }
    ReleaseLocal();
    if (!abort_finished) {
      env_->SetBackgroundThreads(2, priority);
    }
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      release_worker = true;
      local_cv_.notify_all();
      local_cv_.wait(lock, [&] { return worker_done; });
    }
    if (abort_thread.joinable()) {
      abort_thread.join();
    }
    db_->ResumeCompactions(handles_[0]);
    EXPECT_OK(dbfull()->TEST_WaitForCompact());
    EXPECT_EQ(LocalScheduled(), 0);
    EXPECT_EQ(NumTableFilesAtLevel(0, 1), 0);
    EXPECT_EQ(Get(1, Key(199)), std::string(1024, 'd'));
    if (reject_first_cancel) {
      EXPECT_GE(cancellation_attempts.load(), 2);
    }
    Close();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  void VerifyCompacted() {
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    for (int cf = 0; cf < 4; ++cf) {
      ASSERT_EQ(NumTableFilesAtLevel(0, cf), 0);
      ASSERT_EQ(Get(cf, Key(0)), std::string(1024, 'a'));
      ASSERT_EQ(Get(cf, Key(199)), std::string(1024, 'd'));
    }
  }

  void BlockPool(Env::Priority priority) {
    auto task = std::make_unique<test::SleepingBackgroundTask>();
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, task.get(),
                   priority);
    task->WaitUntilSleeping();
    blockers_.push_back(std::move(task));
  }

  void ReleasePools() {
    for (auto& blocker : blockers_) {
      blocker->WakeUp();
      blocker->WaitUntilDone();
    }
    blockers_.clear();
  }

  void CancelQueuedManualCompaction(LocalRouting routing,
                                    Env::Priority priority, bool close,
                                    bool caller_cancellation = false) {
    SetUpCompactions(routing, 3, 1, 1,
                     priority == Env::Priority::BOTTOM ? 2 : 7);
    if (priority == Env::Priority::LOW) {
      env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
    }
    BlockPool(priority);
    BlockLocalWork();
    service_->Release();
    int queued = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RunLocalCompaction:Scheduled", [&](void*) {
          std::lock_guard<std::mutex> lock(local_mutex_);
          ++queued;
          local_cv_.notify_all();
        });
    Status manual_status;
    std::atomic<bool> caller_canceled{false};
    bool manual_done = false;
    if (close) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::CloseHelper:CFHandleCleanupUnlocked", [&](void*) {
            std::unique_lock<std::mutex> lock(local_mutex_);
            local_cv_.wait(lock, [&] { return manual_done; });
          });
    }
    std::thread manual([&] {
      CompactRangeOptions options;
      options.canceled = &caller_canceled;
      manual_status = db_->CompactRange(options, nullptr, nullptr);
      std::lock_guard<std::mutex> lock(local_mutex_);
      manual_done = true;
      local_cv_.notify_all();
    });
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10), [&] {
        return queued == 1 && credit_waits_ >= 2;
      }));
    }
    bool canceled = false;
    std::thread cancel([&] {
      if (caller_cancellation) {
        caller_canceled.store(true, std::memory_order_release);
        std::unique_lock<std::mutex> lock(local_mutex_);
        local_cv_.wait(lock, [&] { return manual_done; });
      } else if (close) {
        Close();
      } else {
        db_->DisableManualCompaction();
      }
      std::lock_guard<std::mutex> lock(local_mutex_);
      canceled = true;
      local_cv_.notify_all();
    });
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                     [&] { return canceled; }));
    }
    ReleaseLocal();
    ReleasePools();
    cancel.join();
    manual.join();
    EXPECT_TRUE(manual_status.IsManualCompactionPaused() ||
                (close && manual_status.IsShutdownInProgress()));
    EXPECT_EQ(local_started_, 0);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  void CancelManualWaitingForCredit(LocalRouting routing, int subcompactions) {
    SetUpCompactions(routing, subcompactions, 2);
    env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
    BlockLocalWork();
    service_->Release();
    ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
    ASSERT_TRUE(WaitForLocal(1, subcompactions - 1));

    std::atomic<bool> canceled{false};
    Status status;
    bool done = false;
    std::thread manual([&] {
      CompactRangeOptions options;
      options.exclusive_manual_compaction = false;
      options.canceled = &canceled;
      status = db_->CompactRange(options, handles_[1], nullptr, nullptr);
      std::lock_guard<std::mutex> lock(local_mutex_);
      done = true;
      local_cv_.notify_all();
    });
    EXPECT_TRUE(WaitForLocal(1, 2 * subcompactions - 1));
    canceled.store(true, std::memory_order_release);
    {
      std::unique_lock<std::mutex> lock(local_mutex_);
      EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                     [&] { return done; }));
      EXPECT_EQ(local_started_, 1);
      EXPECT_EQ(local_finished_, 0);
    }
    ReleaseLocal();
    manual.join();
    ASSERT_TRUE(status.IsManualCompactionPaused());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    EXPECT_EQ(NumTableFilesAtLevel(0, 1), 4);
  }

  void TearDown() override {
    ReleaseLocal();
    ReleasePools();
    if (service_) {
      service_->Release();
    }
    // Drain callbacks before destroying the state they capture.
    Close();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    CompactionServiceTest::TearDown();
  }

  std::shared_ptr<EventListener> extra_listener_;
  std::shared_ptr<PoolTestCompactionService> service_;
  std::shared_ptr<PoolPressureListener> pressure_listener_;
  std::vector<std::unique_ptr<test::SleepingBackgroundTask>> blockers_;
  std::mutex local_mutex_;
  std::condition_variable local_cv_;
  bool release_local_ = false;
  int local_started_ = 0;
  int local_finished_ = 0;
  int local_active_ = 0;
  int peak_local_active_ = 0;
  int credit_waits_ = 0;
};

class RemoteCompactionPoolGateTest : public DedicatedCompactionPoolTest,
                                     public testing::WithParamInterface<bool> {
};

TEST_P(RemoteCompactionPoolGateTest, RemoteSchedulingRespectsPoolGate) {
  ASSERT_FALSE(Options().use_separate_remote_compaction_pool);
  SetUpCompactions(LocalRouting::kRemote, 3, 1, 1, 7, GetParam());
  ASSERT_EQ(db_->GetDBOptions().use_separate_remote_compaction_pool,
            GetParam());
  BlockPool(Env::Priority::LOW);
  BlockPool(Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  if (GetParam()) {
    ASSERT_TRUE(service_->WaitForRemote(3));
    EXPECT_EQ(env_->GetThreadPoolQueueLen(Env::Priority::LOW), 0U);
  } else {
    EXPECT_EQ(env_->GetThreadPoolQueueLen(Env::Priority::LOW), 1U);
    uint64_t running = 0;
    ASSERT_TRUE(db_->GetIntProperty(
        DB::Properties::kNumRunningRemoteCompactions, &running));
    EXPECT_EQ(running, 0U);
  }
  ReleasePools();
  service_->Release();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(NumTableFilesAtLevel(0), 0);
  EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
}

TEST_P(RemoteCompactionPoolGateTest, RuntimeSwitchPreservesScheduledJobs) {
  auto listener = std::make_shared<CompactionContextListener>();
  extra_listener_ = listener;
  SetUpCompactions(LocalRouting::kWait, 3, 1, 1, 7, GetParam());
  service_->SetScheduleObserver(
      [listener](const CompactionServiceJobInfo& info) {
        listener->OnSchedule(info);
      });
  BlockPool(Env::Priority::LOW);
  BlockPool(Env::Priority::BOTTOM);
  BlockLocalWork();
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  if (GetParam()) {
    ASSERT_TRUE(service_->WaitForRemote(3));
  } else {
    ASSERT_EQ(env_->GetThreadPoolQueueLen(Env::Priority::LOW), 1U);
  }
  ASSERT_OK(db_->SetDBOptions({{"use_separate_remote_compaction_pool",
                                GetParam() ? "false" : "true"}}));
  ASSERT_EQ(db_->GetDBOptions().use_separate_remote_compaction_pool,
            !GetParam());
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[1]}));
  ASSERT_TRUE(service_->WaitForRemote(3));
  EXPECT_EQ(env_->GetThreadPoolQueueLen(Env::Priority::LOW), 1U);
  service_->Release();
  ASSERT_TRUE(WaitForLocal(0, 3));
  {
    std::lock_guard<std::mutex> lock(local_mutex_);
    EXPECT_EQ(local_started_, 0);
  }
  ReleasePools();
  ASSERT_TRUE(WaitForLocal(1, 3));
  ReleaseLocal();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int cf : {0, 1}) {
    EXPECT_EQ(NumTableFilesAtLevel(0, cf), 0);
    EXPECT_EQ(Get(cf, Key(199)), std::string(1024, 'd'));
  }
  listener->Verify();
  EXPECT_EQ(local_finished_, 3);
  EXPECT_EQ(peak_local_active_, 1);
}

TEST_P(RemoteCompactionPoolGateTest, CompactFilesCarriesSubcompactionGate) {
  auto listener = std::make_shared<CompactFilesPoolGateListener>(GetParam());
  extra_listener_ = listener;
  SetUpCompactions(LocalRouting::kSchedule, 3, 1, 1, 7, GetParam());
  service_->SetScheduleObserver(
      [listener](const CompactionServiceJobInfo& info) {
        listener->OnSchedule(info);
      });
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  std::vector<std::string> inputs;
  for (const auto& file : metadata.levels[0].files) {
    inputs.push_back(file.db_path + "/" + file.name);
  }
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, 1));
  EXPECT_EQ(listener->parents.load(), 0);
  EXPECT_EQ(listener->scheduled.load(), 3);
  EXPECT_EQ(listener->begun.load(), 3);
  EXPECT_EQ(listener->completed.load(), 3);
  EXPECT_EQ(NumTableFilesAtLevel(0), 0);
}

INSTANTIATE_TEST_CASE_P(PoolGate, RemoteCompactionPoolGateTest,
                        testing::Bool());

enum class RemoteLifecycle { kPause, kAbort, kExclusiveManual, kWait };

class RemoteCompactionLifecycleTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<RemoteLifecycle, int>> {};

TEST_P(RemoteCompactionLifecycleTest, WaitsForRemoteCoordinators) {
  const auto operation = std::get<0>(GetParam());
  const int subcompactions = std::get<1>(GetParam());
  SetUpCompactions(LocalRouting::kRemote, subcompactions);
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  ASSERT_TRUE(service_->WaitForRemote(subcompactions));
  const char* wait_point = nullptr;
  switch (operation) {
    case RemoteLifecycle::kPause:
      wait_point = "DBImpl::PauseBackgroundWork:Wait";
      break;
    case RemoteLifecycle::kAbort:
      wait_point = "DBImpl::AbortAllCompactions:Wait";
      break;
    case RemoteLifecycle::kExclusiveManual:
      wait_point = "DBImpl::RunManualCompaction:WaitScheduled";
      break;
    case RemoteLifecycle::kWait:
      wait_point = "DBImpl::WaitForCompact:InsideLoop";
      break;
  }
  bool waiting = false;
  bool done = false;
  SyncPoint::GetInstance()->SetCallBack(wait_point, [&](void*) {
    std::lock_guard<std::mutex> lock(local_mutex_);
    waiting = true;
    local_cv_.notify_all();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Status status;
  std::thread waiter([&] {
    switch (operation) {
      case RemoteLifecycle::kPause:
        status = db_->PauseBackgroundWork();
        break;
      case RemoteLifecycle::kAbort:
        db_->AbortAllCompactions();
        break;
      case RemoteLifecycle::kExclusiveManual: {
        CompactRangeOptions options;
        options.exclusive_manual_compaction = true;
        status = db_->CompactRange(options, handles_[1], nullptr, nullptr);
        break;
      }
      case RemoteLifecycle::kWait:
        status = db_->WaitForCompact(WaitForCompactOptions());
        break;
    }
    std::lock_guard<std::mutex> lock(local_mutex_);
    done = true;
    local_cv_.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return waiting || done; }));
    EXPECT_TRUE(waiting);
    EXPECT_FALSE(done);
  }
  service_->Release();
  waiter.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(status);
  if (operation == RemoteLifecycle::kPause) {
    ASSERT_OK(db_->ContinueBackgroundWork());
  } else if (operation == RemoteLifecycle::kAbort) {
    db_->ResumeAllCompactions();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
}

INSTANTIATE_TEST_CASE_P(
    RemoteWork, RemoteCompactionLifecycleTest,
    testing::Combine(testing::Values(RemoteLifecycle::kPause,
                                     RemoteLifecycle::kAbort,
                                     RemoteLifecycle::kExclusiveManual,
                                     RemoteLifecycle::kWait),
                     testing::Values(1, 3)));

class RemoteCompactFilesLifecycleTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<int> {};

TEST_P(RemoteCompactFilesLifecycleTest, WaitsForCallerThreadCompactFiles) {
  SetUpCompactions(LocalRouting::kRemote, GetParam());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  std::vector<std::string> inputs;
  for (const auto& file : metadata.levels[0].files) {
    inputs.push_back(file.db_path + "/" + file.name);
  }
  Status status;
  std::thread compact(
      [&] { status = db_->CompactFiles(CompactionOptions(), inputs, 1); });
  EXPECT_TRUE(service_->WaitForRemote(GetParam()));
  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::milliseconds(10);
  EXPECT_TRUE(db_->WaitForCompact(wait_options).IsTimedOut());
  service_->Release();
  compact.join();
  ASSERT_OK(status);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(NumTableFilesAtLevel(0), 0);
}

INSTANTIATE_TEST_CASE_P(RemoteWork, RemoteCompactFilesLifecycleTest,
                        testing::Values(1, 3));

class RemoteCompactionAccountingTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<Env::Priority> {};

TEST_P(RemoteCompactionAccountingTest, CountsOnlyAdmittedLocalWorkInPools) {
  const bool bottom = GetParam() == Env::Priority::BOTTOM;
  pressure_listener_ = std::make_shared<PoolPressureListener>();
  SetUpCompactions(LocalRouting::kWait, 3, 1, 1, bottom ? 2 : 7);
  BlockLocalWork();
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  ASSERT_TRUE(service_->WaitForRemote(3));
  ASSERT_OK(Put(1, "remote-pressure", "value"));
  ASSERT_OK(Flush(1));
  auto pressure = pressure_listener_->WaitForCompactionPressure();
  EXPECT_EQ(pressure.compaction_scheduled, 1);
  EXPECT_EQ(pressure.compaction_low_scheduled, 0);
  EXPECT_EQ(pressure.compaction_bottom_scheduled, 0);
  EXPECT_EQ(pressure.compaction_low_running, 0);
  EXPECT_EQ(pressure.compaction_bottom_running, 0);

  service_->Release();
  ASSERT_TRUE(WaitForLocal(1, 2));
  pressure_listener_->Reset();
  ASSERT_OK(Put(1, "local-pressure", "value"));
  ASSERT_OK(Flush(1));
  pressure = pressure_listener_->WaitForCompactionPressure();
  EXPECT_EQ(pressure.compaction_scheduled, 2);
  EXPECT_EQ(pressure.compaction_low_scheduled, bottom ? 0 : 1);
  EXPECT_EQ(pressure.compaction_bottom_scheduled, bottom ? 1 : 0);
  EXPECT_EQ(pressure.compaction_low_running, bottom ? 0 : 1);
  EXPECT_EQ(pressure.compaction_bottom_running, bottom ? 1 : 0);
  ReleaseLocal();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(local_finished_, 3);
  EXPECT_EQ(NumTableFilesAtLevel(0), 0);
}

INSTANTIATE_TEST_CASE_P(LocalPools, RemoteCompactionAccountingTest,
                        testing::Values(Env::Priority::LOW,
                                        Env::Priority::BOTTOM));

#ifndef NROCKSDB_THREAD_STATUS
class DedicatedCompactionThreadTrackingTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<
          std::tuple<LocalRouting, Env::Priority, int, bool>> {
 protected:
  void CheckThreadTracking(LocalRouting routing, Env::Priority priority,
                           int subcompactions, bool separate_pool,
                           bool throw_error) {
    std::mutex ids_mutex;
    std::vector<uint64_t> worker_ids;
    std::set<uint64_t> armed_workers;
    class ThrowingFilter : public CompactionFilter {
     public:
      explicit ThrowingFilter(std::function<bool()> should_throw)
          : should_throw_(std::move(should_throw)) {}
      const char* Name() const override {
        return "ThreadTrackingThrowingFilter";
      }
      bool Filter(int, const Slice&, const Slice&, std::string*,
                  bool*) const override {
        if (should_throw_()) {
          throw std::runtime_error("Injected thread tracking exception");
        }
        return false;
      }

     private:
      const std::function<bool()> should_throw_;
    } filter([&] {
      std::lock_guard<std::mutex> lock(ids_mutex);
      return armed_workers.erase(env_->GetThreadID()) != 0;
    });
    SetUpCompactions(routing, subcompactions, 1, 1,
                     priority == Env::Priority::BOTTOM ? 2 : 7, separate_pool,
                     true, throw_error ? &filter : nullptr);
    if (priority == Env::Priority::LOW) {
      env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
    }
    service_->Release();
    auto* primary_cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(handles_[0])->cfd();
    std::atomic<int> started{0};
    std::atomic<int> finished{0};
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionJob::ProcessKeyValueCompaction()::Processing",
        [&](void* arg) {
          auto* compaction = static_cast<Compaction*>(arg);
          if (compaction->column_family_data() != primary_cfd) {
            return;
          }
          const uint64_t thread_id = env_->GetThreadID();
          {
            std::lock_guard<std::mutex> lock(ids_mutex);
            worker_ids.push_back(thread_id);
            if (throw_error) {
              armed_workers.insert(thread_id);
            }
          }
          std::vector<ThreadStatus> threads;
          EXPECT_OK(env_->GetThreadList(&threads));
          bool found = false;
          for (const ThreadStatus& thread : threads) {
            if (thread.thread_id != thread_id) {
              continue;
            }
            found = true;
            EXPECT_EQ(thread.db_name, dbname_);
            EXPECT_EQ(thread.cf_name, "default");
            EXPECT_EQ(thread.operation_type, ThreadStatus::OP_COMPACTION);
            EXPECT_EQ(thread.operation_stage,
                      ThreadStatus::STAGE_COMPACTION_PROCESS_KV);
            EXPECT_EQ(thread.thread_type, priority == Env::Priority::BOTTOM
                                              ? ThreadStatus::BOTTOM_PRIORITY
                                              : ThreadStatus::LOW_PRIORITY);
            EXPECT_GT(thread.op_properties[ThreadStatus::COMPACTION_JOB_ID],
                      0U);
            EXPECT_EQ(
                thread
                    .op_properties[ThreadStatus::COMPACTION_INPUT_OUTPUT_LEVEL],
                (static_cast<uint64_t>(compaction->start_level()) << 32) +
                    compaction->output_level());
            EXPECT_GT(
                thread
                    .op_properties[ThreadStatus::COMPACTION_TOTAL_INPUT_BYTES],
                0U);
          }
          EXPECT_TRUE(found);
          ++started;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BGWorkLocalCompaction:Finished", [&](void*) {
          std::vector<ThreadStatus> threads;
          EXPECT_OK(env_->GetThreadList(&threads));
          bool found = false;
          for (const ThreadStatus& thread : threads) {
            if (thread.thread_id != env_->GetThreadID()) {
              continue;
            }
            found = true;
            EXPECT_TRUE(thread.db_name.empty());
            EXPECT_TRUE(thread.cf_name.empty());
            EXPECT_EQ(thread.operation_type, ThreadStatus::OP_UNKNOWN);
            EXPECT_EQ(thread.operation_stage, ThreadStatus::STAGE_UNKNOWN);
            for (uint64_t property : thread.op_properties) {
              EXPECT_EQ(property, 0U);
            }
          }
          EXPECT_TRUE(found);
          ++finished;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    EXPECT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
    Status status = dbfull()->TEST_WaitForCompact();
    if (throw_error) {
      EXPECT_TRUE(status.IsAborted());
    } else {
      EXPECT_OK(status);
      EXPECT_EQ(NumTableFilesAtLevel(0), 0);
      EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
    }
    EXPECT_EQ(started.load(), subcompactions);
    EXPECT_EQ(finished.load(), separate_pool ? subcompactions : 0);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    std::vector<ThreadStatus> threads;
    ASSERT_OK(env_->GetThreadList(&threads));
    for (const ThreadStatus& thread : threads) {
      for (uint64_t worker_id : worker_ids) {
        if (thread.thread_id == worker_id) {
          EXPECT_EQ(thread.operation_type, ThreadStatus::OP_UNKNOWN);
          EXPECT_TRUE(thread.cf_name.empty());
        }
      }
    }
    Close();
  }
};

TEST_P(DedicatedCompactionThreadTrackingTest, LocalWorkersTrackCompaction) {
  CheckThreadTracking(std::get<0>(GetParam()), std::get<1>(GetParam()),
                      std::get<2>(GetParam()), true, std::get<3>(GetParam()));
}

TEST_F(DedicatedCompactionThreadTrackingTest, LegacyWorkerTracksCompaction) {
  CheckThreadTracking(LocalRouting::kSchedule, Env::Priority::LOW, 1, false,
                      false);
}

INSTANTIATE_TEST_CASE_P(
    LocalExecution, DedicatedCompactionThreadTrackingTest,
    testing::Combine(testing::Values(LocalRouting::kSchedule,
                                     LocalRouting::kWait,
                                     LocalRouting::kInvalidResult),
                     testing::Values(Env::Priority::LOW, Env::Priority::BOTTOM),
                     testing::Values(1, 3), testing::Bool()));
#endif  // NROCKSDB_THREAD_STATUS

class RemoteCompactionRetryTest : public DedicatedCompactionPoolTest,
                                  public testing::WithParamInterface<bool> {
 protected:
  void CheckBackoff(bool error) {
    auto limiter = std::shared_ptr<ConcurrentTaskLimiter>(
        NewConcurrentTaskLimiter("retry-limiter", error ? -1 : 0));
    Options options = SetUpCompactions(error ? LocalRouting::kSchedule
                                             : LocalRouting::kRemote);
    options.compaction_thread_limiter = limiter;
    options.use_separate_remote_compaction_pool = GetParam();
    options.paranoid_checks = !error;
    ReopenWithColumnFamilies({"default", "cf_1", "cf_2", "cf_3"}, options);
    service_->SetCanceled(false);
    env_->non_writeable_rate_ = error ? 100 : 0;

    std::mutex mutex;
    std::condition_variable cv;
    int backoffs = 0;
    bool released = false;
    const std::vector<ColumnFamilyHandle*> families = {handles_[0], handles_[1],
                                                       handles_[2]};
    const int expected_coordinators =
        GetParam() ? static_cast<int>(families.size()) : 1;
    {
      Defer drain([&] {
        limiter->ResetMaxOutstandingTask();
        env_->non_writeable_rate_ = 0;
        service_->Release();
        {
          std::lock_guard<std::mutex> lock(mutex);
          released = true;
          cv.notify_all();
        }
        EXPECT_OK(dbfull()->TEST_WaitForCompact());
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
      });
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::BackgroundCallCompaction:BeforeBackoff", [&](void* arg) {
            auto* status = static_cast<Status*>(arg);
            EXPECT_TRUE(error ? status->IsIOError() : status->IsBusy());
            std::unique_lock<std::mutex> lock(mutex);
            ++backoffs;
            cv.notify_all();
            cv.wait(lock, [&] { return released; });
          });
      SyncPoint::GetInstance()->EnableProcessing();
      const auto schedule = [&] {
        const Status status = dbfull()->EnableAutoCompaction(families);
        if (error) {
          EXPECT_TRUE(status.IsIOError());
        } else {
          EXPECT_OK(status);
        }
      };
      schedule();
      {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] {
          return backoffs >= expected_coordinators;
        }));
      }
      uint64_t unscheduled = 0;
      ASSERT_TRUE(db_->GetIntProperty(
          DB::Properties::kNumUnscheduledCompactions, &unscheduled));
      EXPECT_EQ(unscheduled, GetParam() ? 0U : families.size());
      for (int i = 0; i < 3; ++i) {
        schedule();
        EXPECT_EQ(RemotePoolThreads(), GetParam() ? expected_coordinators : 0);
      }
      EXPECT_EQ(LocalScheduled(), GetParam() ? 0 : 1);
    }
    for (int cf = 0; cf < 3; ++cf) {
      EXPECT_EQ(NumTableFilesAtLevel(0, cf), 0);
      EXPECT_EQ(Get(cf, Key(199)), std::string(1024, 'd'));
    }
    EXPECT_EQ(LocalScheduled(), 0);
    WaitForRemotePoolIdle();
    Close();
  }
};

TEST_P(RemoteCompactionRetryTest, ThrottledPickRetainsCreditDuringBackoff) {
  CheckBackoff(false);
}

TEST_P(RemoteCompactionRetryTest, RetryableErrorRetainsCreditDuringBackoff) {
  CheckBackoff(true);
}

INSTANTIATE_TEST_CASE_P(PoolGate, RemoteCompactionRetryTest, testing::Bool());

TEST_F(DedicatedCompactionPoolTest, DisableManualCompactionPausesFutureWork) {
  SetUpCompactions(LocalRouting::kRemote);
  service_->Release();
  EXPECT_EQ(ManualCompactionPauseCount(), 0);
  db_->DisableManualCompaction();
  EXPECT_EQ(ManualCompactionPauseCount(), 1);
  EXPECT_TRUE(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsManualCompactionPaused());
  db_->EnableManualCompaction();
  EXPECT_EQ(ManualCompactionPauseCount(), 0);
  EXPECT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
}

TEST_F(DedicatedCompactionPoolTest, CloseWithoutManualWorkPreservesPauseCount) {
  for (bool paused : {false, true}) {
    SetUpCompactions(LocalRouting::kRemote);
    if (paused) {
      db_->DisableManualCompaction();
    }
    const int pause_count = ManualCompactionPauseCount();
    for (auto handle : handles_) {
      EXPECT_OK(db_->DestroyColumnFamilyHandle(handle));
    }
    handles_.clear();
    ASSERT_OK(db_->Close());
    EXPECT_EQ(ManualCompactionPauseCount(), pause_count);
    ASSERT_OK(db_->Close());
    EXPECT_EQ(ManualCompactionPauseCount(), pause_count);
  }
}

TEST_F(DedicatedCompactionPoolTest, RemoteProgressWithBothLocalPoolsOccupied) {
  SetUpCompactions(LocalRouting::kRemote);
  BlockPool(Env::Priority::LOW);
  BlockPool(Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(service_->WaitForRemote(4));
  ReleasePools();
  service_->Release();
  VerifyCompacted();
}

TEST_F(DedicatedCompactionPoolTest, RemotePoolStartsAtCombinedLocalPoolSize) {
  SetUpCompactions(LocalRouting::kRemote, 1, 2);
  env_->SetBackgroundThreads(3, Env::Priority::BOTTOM);
  EXPECT_EQ(RemotePoolThreads(), 0);
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  ASSERT_TRUE(service_->WaitForRemote(1));
  EXPECT_EQ(RemotePoolThreads(), 5);
  service_->Release();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(DedicatedCompactionPoolTest, MultipleDBsShareRemotePoolAndLifetime) {
  SetUpCompactions(LocalRouting::kRemote, 3, 2);
  env_->SetBackgroundThreads(3, Env::Priority::BOTTOM);
  EnvWrapper other_env(env_);
  Options options = CurrentOptions();
  options.env = &other_env;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 3;
  options.target_file_size_base = 4096;
  options.compression = kNoCompression;
  options.use_separate_remote_compaction_pool = true;
  const std::string other_name = dbname_ + "_other";
  auto statistics = CreateDBStatistics();
  auto other_service = std::make_shared<PoolTestCompactionService>(
      other_name, options, statistics,
      std::vector<std::shared_ptr<EventListener>>{},
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
  options.compaction_service = other_service;
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_name, &other_db));
  auto* other_impl = static_cast<DBImpl*>(other_db->GetRootDB());
  for (int file = 0; file < 4; ++file) {
    for (int key = 0; key < 200; ++key) {
      ASSERT_OK(
          other_db->Put(WriteOptions(), Key(key + file * 17),
                        std::string(1024, static_cast<char>('a' + file))));
    }
    ASSERT_OK(other_db->Flush(FlushOptions()));
  }
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  std::atomic<int> named_threads{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void*) {
        char name[16];
        EXPECT_EQ(pthread_getname_np(pthread_self(), name, sizeof(name)), 0);
        EXPECT_STREQ(name, "rocksdb:remote");
        ++named_threads;
      });
  SyncPoint::GetInstance()->EnableProcessing();
#endif
#endif
  EXPECT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  EXPECT_TRUE(service_->WaitForRemote(3));
  EXPECT_EQ(RemotePoolThreads(), 5);
  EXPECT_OK(
      other_impl->EnableAutoCompaction({other_db->DefaultColumnFamily()}));
  EXPECT_TRUE(other_service->WaitForRemote(3));
  const int peak = RemotePoolThreads();
  EXPECT_GE(peak, 6);
  EXPECT_TRUE(SharesRemotePool(other_impl));
  EXPECT_OK(
      db_->SetDBOptions({{"use_separate_remote_compaction_pool", "false"}}));
  RunPoolMaintenance(60);
  EXPECT_EQ(RemotePoolThreads(), peak);
  service_->Release();
  EXPECT_OK(dbfull()->TEST_WaitForCompact());
  RunPoolMaintenance(60);
  EXPECT_EQ(RemotePoolThreads(), peak);
  std::mutex mutex;
  std::condition_variable cv;
  bool closed = false;
  std::thread close([&] {
    Close();
    std::lock_guard<std::mutex> lock(mutex);
    closed = true;
    cv.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return closed; }));
  }
  other_service->Release();
  close.join();
  EXPECT_OK(other_impl->TEST_WaitForCompact());
  RunPoolMaintenance(30, other_impl);
  EXPECT_EQ(RemotePoolThreads(), 5);
  EXPECT_OK(other_db->SetDBOptions(
      {{"use_separate_remote_compaction_pool", "false"}}));
  RunPoolMaintenance(30, other_impl);
  EXPECT_EQ(RemotePoolThreads(), 0);
  EXPECT_OK(other_db->SetDBOptions(
      {{"use_separate_remote_compaction_pool", "true"}}));
  EXPECT_OK(other_db->SetOptions({{"disable_auto_compactions", "true"}}));
  for (int file = 0; file < 4; ++file) {
    EXPECT_OK(other_db->Put(WriteOptions(), Key(file), std::string(1024, 'x')));
    EXPECT_OK(other_db->Flush(FlushOptions()));
  }
  EXPECT_OK(
      other_impl->EnableAutoCompaction({other_db->DefaultColumnFamily()}));
  EXPECT_OK(other_impl->TEST_WaitForCompact());
  EXPECT_EQ(RemotePoolThreads(), 5);
  other_db.reset();
  EXPECT_EQ(RemotePoolThreads(), 0);
  EXPECT_OK(DestroyDB(other_name, options));
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
  EXPECT_GE(named_threads.load(), 6);
#endif
#endif
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DedicatedCompactionPoolTest, RemotePoolShrinksAfterQuietPeriod) {
  SetUpCompactions(LocalRouting::kRemote, 3);
  BlockPool(Env::Priority::LOW);
  BlockPool(Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(service_->WaitForRemote(12));
  const int peak = RemotePoolThreads();
  EXPECT_GE(peak, 12);
  RunPoolMaintenance(60);
  EXPECT_EQ(RemotePoolThreads(), peak);
  ReleasePools();
  service_->Release();
  VerifyCompacted();
  RunPoolMaintenance(29);
  EXPECT_EQ(RemotePoolThreads(), peak);
  RunPoolMaintenance(30);
  EXPECT_EQ(RemotePoolThreads(), 2);
  ASSERT_OK(
      db_->SetDBOptions({{"use_separate_remote_compaction_pool", "false"}}));
  RunPoolMaintenance(30);
  EXPECT_EQ(RemotePoolThreads(), 0);
  ASSERT_OK(
      db_->SetDBOptions({{"use_separate_remote_compaction_pool", "true"}}));
  ASSERT_OK(
      db_->SetOptions(handles_[1], {{"disable_auto_compactions", "true"}}));
  WriteCompactionInputs(1);
  service_->ResetWait();
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[1]}));
  ASSERT_TRUE(service_->WaitForRemote(3));
  EXPECT_GE(RemotePoolThreads(), 3);
  RunPoolMaintenance(60);
  EXPECT_GE(RemotePoolThreads(), 3);
  service_->Release();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  RunPoolMaintenance(30);
  EXPECT_EQ(RemotePoolThreads(), 2);
}

TEST_F(DedicatedCompactionPoolTest, RemotePoolBaselineTracksLocalPoolSizes) {
  SetUpCompactions(LocalRouting::kRemote, 3);
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(service_->WaitForRemote(12));
  service_->Release();
  VerifyCompacted();
  env_->SetBackgroundThreads(2, Env::Priority::LOW);
  env_->SetBackgroundThreads(3, Env::Priority::BOTTOM);
  RunPoolMaintenance(30);
  EXPECT_EQ(RemotePoolThreads(), 5);
  env_->SetBackgroundThreads(0, Env::Priority::LOW);
  env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
  RunPoolMaintenance(30);
  EXPECT_EQ(RemotePoolThreads(), 1);
}

class DedicatedQueuedCompactionAbortTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<Env::Priority, int, bool>> {
};

TEST_P(DedicatedQueuedCompactionAbortTest,
       TargetedAbortCancelsOnlyItsFallbacks) {
  AbortQueuedFallback(std::get<0>(GetParam()), std::get<1>(GetParam()),
                      std::get<2>(GetParam()));
}

INSTANTIATE_TEST_CASE_P(
    QueuedWork, DedicatedQueuedCompactionAbortTest,
    testing::Combine(testing::Values(Env::Priority::LOW, Env::Priority::BOTTOM),
                     testing::Values(1, 3), testing::Bool()));

TEST_F(DedicatedCompactionPoolTest,
       RemoteSubcompactionsDoNotReserveLocalSlots) {
  SetUpCompactions(LocalRouting::kRemote, 3);
  BlockPool(Env::Priority::LOW);
  BlockPool(Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(service_->WaitForRemote(12));
  uint64_t running = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningRemoteCompactions,
                                  &running));
  ASSERT_EQ(running, 12U);
  ReleasePools();
  service_->Release();
  VerifyCompacted();
}

TEST_F(DedicatedCompactionPoolTest, LocalCompletionDoesNotBlockRemoteSiblings) {
  SetUpCompactions(LocalRouting::kRemote, 3);
  service_->SetLocalCF("cf_1");
  service_->SetLocalSubcompactions(1);
  BlockLocalWork();
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(WaitForLocal(1, 0));
  ASSERT_TRUE(service_->WaitForRemote(11));
  ReleaseLocal();
  ASSERT_TRUE(WaitForLocal(1, 0, 1));
  // Local subcompactions have finished while other jobs remain in Wait().
  uint64_t running = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningRemoteCompactions,
                                  &running));
  ASSERT_EQ(running, 11U);
  service_->Release();
  VerifyCompacted();
  ASSERT_EQ(peak_local_active_, 1);
}

class DedicatedCompactionExceptionTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {};

TEST_P(DedicatedCompactionExceptionTest, ServiceExceptionFailsCompaction) {
  SetUpCompactions(LocalRouting::kSchedule, 3);
  service_->SetSubcompactionException(std::get<0>(GetParam()),
                                      std::get<1>(GetParam()));
  Status status = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  EXPECT_TRUE(status.IsAborted());
  EXPECT_EQ(NumTableFilesAtLevel(0), 4);
  EXPECT_EQ(Get(Key(0)), std::string(1024, 'a'));
  EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
  Close();
}

INSTANTIATE_TEST_CASE_P(Subcompaction, DedicatedCompactionExceptionTest,
                        testing::Combine(testing::Values(0U, 1U),
                                         testing::Bool()));

class LocalCompactionCancellationFailureTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<Env::Priority, int>> {};

TEST_P(LocalCompactionCancellationFailureTest, RetriesWithoutUnwindingTask) {
  AbortQueuedFallback(std::get<0>(GetParam()), std::get<1>(GetParam()), true,
                      true);
}

INSTANTIATE_TEST_CASE_P(LocalPools, LocalCompactionCancellationFailureTest,
                        testing::Combine(testing::Values(Env::Priority::LOW,
                                                         Env::Priority::BOTTOM),
                                         testing::Values(1, 3)));

class LocalCompactionStatusCopyFailureTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<Env::Priority, bool>> {};

TEST_P(LocalCompactionStatusCopyFailureTest, CompletesAcceptedTask) {
  CompleteLocalTaskAfterStatusCopyFailure(std::get<0>(GetParam()),
                                          std::get<1>(GetParam()));
}

INSTANTIATE_TEST_CASE_P(LocalPools, LocalCompactionStatusCopyFailureTest,
                        testing::Combine(testing::Values(Env::Priority::LOW,
                                                         Env::Priority::BOTTOM),
                                         testing::Bool()));

TEST_F(DedicatedCompactionPoolTest,
       IncreasedCompactionLimitWakesAutomaticWaiters) {
  SetUpCompactions(LocalRouting::kWait, 3, 1, 1);
  ASSERT_OK(db_->SetOptions({{"level0_slowdown_writes_trigger", "4"}}));
  BlockLocalWork();
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  ASSERT_TRUE(service_->WaitForRemote(3));
  service_->Release();
  ASSERT_TRUE(WaitForLocal(1, 2));
  ASSERT_OK(db_->SetDBOptions({{"max_background_compactions", "3"}}));
  EXPECT_TRUE(WaitForLocal(3, 2));
  EXPECT_EQ(local_finished_, 0);
  ReleaseLocal();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  WaitForRemotePoolIdle();
  EXPECT_EQ(LocalScheduled(), 0);
  EXPECT_EQ(local_finished_, 3);
}

class CoordinatorSubmissionFailureTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<bool, int>> {};

TEST_P(CoordinatorSubmissionFailureTest,
       BackgroundSubmissionPreservesPendingWork) {
  const bool separate_pool = std::get<0>(GetParam());
  const int failed_submission = std::get<1>(GetParam());
  SetUpCompactions(LocalRouting::kRemote, 1, 1, 1, 7, separate_pool);
  int submissions = 0;
  SyncPoint::GetInstance()->EnableProcessing();
  bool finished = false;
  std::function<void()> schedule = [&] {
    SyncPoint::GetInstance()->SetCallBack(
        separate_pool ? "ThreadPoolImpl::StartBGThreads:BeforeCreate"
                      : "ThreadPoolImpl::Submit:BeforeEnqueue",
        [&](void* arg) {
          if (++submissions == failed_submission) {
            *static_cast<std::function<void()>*>(arg) = [separate_pool] {
              if (separate_pool) {
                throw std::system_error(std::make_error_code(
                    std::errc::resource_unavailable_try_again));
              }
              throw std::bad_alloc();
            };
          }
        });
    EXPECT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
    std::lock_guard<std::mutex> lock(local_mutex_);
    finished = true;
    local_cv_.notify_all();
  };
  env_->Schedule(
      [](void* arg) { (*static_cast<std::function<void()>*>(arg))(); },
      &schedule, Env::Priority::HIGH);
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return finished; }));
  }
  WaitForCompactOptions wait;
  wait.timeout = std::chrono::seconds(10);
  EXPECT_TRUE(db_->WaitForCompact(wait).IsAborted());
  WaitForRemotePoolIdle();
  EXPECT_EQ(LocalScheduled(), 0);
  EXPECT_EQ(submissions, failed_submission);
  if (separate_pool) {
    EXPECT_EQ(RemotePoolThreads(), failed_submission - 1);
  }
  uint64_t pending = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumUnscheduledCompactions,
                                  &pending));
  EXPECT_EQ(pending, 1U);
  EXPECT_EQ(NumTableFilesAtLevel(0), 4);
  EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

INSTANTIATE_TEST_CASE_P(Submission, CoordinatorSubmissionFailureTest,
                        testing::Values(std::make_tuple(true, 1),
                                        std::make_tuple(true, 2),
                                        std::make_tuple(false, 1)));

class CoordinatorSubmissionModeTest : public DedicatedCompactionPoolTest,
                                      public testing::WithParamInterface<bool> {
};

TEST_P(CoordinatorSubmissionModeTest, RejectedManualCoordinatorReleasesInputs) {
  SetUpCompactions(LocalRouting::kRemote, 1, 1, 1, 7, GetParam());
  SyncPoint::GetInstance()->SetCallBack(
      GetParam() ? "DBImpl::ScheduleRemoteCompaction:BeforeSubmit"
                 : "ThreadPoolImpl::Submit:BeforeEnqueue",
      [](void* arg) {
        *static_cast<std::function<void()>*>(arg) = [] {
          throw std::bad_alloc();
        };
      });
  SyncPoint::GetInstance()->EnableProcessing();
  EXPECT_TRUE(
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr).IsAborted());
  WaitForRemotePoolIdle();
  EXPECT_EQ(LocalScheduled(), 0);
  EXPECT_EQ(NumTableFilesAtLevel(0), 4);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  service_->Release();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ(NumTableFilesAtLevel(0), 0);
  EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
}

TEST_P(CoordinatorSubmissionModeTest,
       RejectedBottomCoordinatorDrainsPickingTask) {
  SetUpCompactions(LocalRouting::kRemote, 1, 1, 1, 2, GetParam());
  int submissions = 0;
  SyncPoint::GetInstance()->SetCallBack(
      GetParam() ? "DBImpl::ScheduleRemoteCompaction:BeforeSubmit"
                 : "ThreadPoolImpl::Submit:BeforeEnqueue",
      [&](void* arg) {
        if (++submissions == 2) {
          *static_cast<std::function<void()>*>(arg) = [] {
            throw std::runtime_error("Rejected BOTTOM coordinator");
          };
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  WaitForCompactOptions wait;
  wait.timeout = std::chrono::seconds(10);
  EXPECT_TRUE(db_->WaitForCompact(wait).IsAborted());
  ASSERT_OK(db_->PauseBackgroundWork());
  WaitForRemotePoolIdle();
  ASSERT_OK(db_->ContinueBackgroundWork());
  EXPECT_EQ(submissions, 2);
  EXPECT_EQ(LocalScheduled(), 0);
  EXPECT_EQ(NumTableFilesAtLevel(0), 4);
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  for (const auto& file : metadata.levels[0].files) {
    EXPECT_FALSE(file.being_compacted);
  }
}

INSTANTIATE_TEST_CASE_P(PoolMode, CoordinatorSubmissionModeTest,
                        testing::Bool());

class ShortcutCompactionGateTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<int, bool>> {};

TEST_P(ShortcutCompactionGateTest, CallbacksPreserveSubmittedMode) {
  const int kind = std::get<0>(GetParam());
  const bool gate = std::get<1>(GetParam());
  auto listener = std::make_shared<ShortcutCompactionListener>(gate);
  SetUpShortcutCompaction(kind, gate, listener);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  if (kind == 2) {
    env_->MockSleepForSeconds(60);
  }
  bool started = false;
  bool released = false;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::BGWorkCompaction", [&](void*) {
    std::unique_lock<std::mutex> lock(local_mutex_);
    started = true;
    local_cv_.notify_all();
    local_cv_.wait(lock, [&] { return released; });
  });
  SyncPoint::GetInstance()->EnableProcessing();
  EXPECT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return started; }));
  }
  EXPECT_OK(db_->SetDBOptions(
      {{"use_separate_remote_compaction_pool", gate ? "false" : "true"}}));
  {
    std::lock_guard<std::mutex> lock(local_mutex_);
    released = true;
    local_cv_.notify_all();
  }
  EXPECT_OK(dbfull()->TEST_WaitForCompact());
  listener->Verify(1);
  if (kind == 0) {
    EXPECT_EQ(listener->moved_files.load(), 1U);
  } else if (kind == 1) {
    EXPECT_EQ(NumTableFilesAtLevel(0), 0);
  } else {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), 1U);
    EXPECT_EQ(metadata.levels[0].files[0].temperature, Temperature::kCold);
  }
}

INSTANTIATE_TEST_CASE_P(Shortcut, ShortcutCompactionGateTest,
                        testing::Combine(testing::Values(0, 1, 2),
                                         testing::Bool()));

TEST_F(DedicatedCompactionPoolTest, FifoCopiesShareLocalWorkersAcrossDBs) {
  auto listener = std::make_shared<ShortcutCompactionListener>(true);
  Options options = SetUpShortcutCompaction(2, true, listener);
  const std::string second_path = dbname_ + "_fifo_copy";
  std::unique_ptr<DB> second;
  ASSERT_OK(DB::Open(options, second_path, &second));
  ASSERT_OK(Put("key", "first"));
  ASSERT_OK(Flush());
  ASSERT_OK(second->Put(WriteOptions(), "key", "second"));
  ASSERT_OK(second->Flush(FlushOptions()));
  env_->MockSleepForSeconds(60);
  int scheduled = 0;
  int copies_started = 0;
  bool released = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunLocalCompaction:Scheduled", [&](void*) {
        std::lock_guard<std::mutex> lock(local_mutex_);
        ++scheduled;
        local_cv_.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "NewWritableFile::FileOptions.temperature", [&](void* arg) {
        if (*static_cast<Temperature*>(arg) != Temperature::kCold) {
          return;
        }
        std::unique_lock<std::mutex> lock(local_mutex_);
        ++copies_started;
        local_cv_.notify_all();
        local_cv_.wait(lock, [&] { return released; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  EXPECT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  EXPECT_OK(second->EnableAutoCompaction({second->DefaultColumnFamily()}));
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10), [&] {
      return scheduled == 2 && copies_started == 1;
    }));
    EXPECT_EQ(copies_started, 1);
  }
  EXPECT_OK(
      db_->SetDBOptions({{"use_separate_remote_compaction_pool", "false"}}));
  {
    std::lock_guard<std::mutex> lock(local_mutex_);
    released = true;
    local_cv_.notify_all();
  }
  EXPECT_OK(dbfull()->TEST_WaitForCompact());
  WaitForCompactOptions wait;
  wait.timeout = std::chrono::seconds(10);
  EXPECT_OK(second->WaitForCompact(wait));
  EXPECT_EQ(copies_started, 2);
  listener->Verify(2);
  EXPECT_EQ(Get("key"), "first");
  std::string value;
  EXPECT_OK(second->Get(ReadOptions(), "key", &value));
  EXPECT_EQ(value, "second");
  EXPECT_OK(second->Close());
  second.reset();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_OK(DestroyDB(second_path, options));
}

TEST_F(DedicatedCompactionPoolTest, AbortQueuedFifoCopyReleasesLocalCredit) {
  auto listener = std::make_shared<ShortcutCompactionListener>(true);
  SetUpShortcutCompaction(2, true, listener);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  env_->MockSleepForSeconds(60);
  BlockPool(Env::Priority::LOW);
  bool scheduled = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunLocalCompaction:Scheduled", [&](void*) {
        std::lock_guard<std::mutex> lock(local_mutex_);
        scheduled = true;
        local_cv_.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  EXPECT_OK(dbfull()->EnableAutoCompaction({handles_[0]}));
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return scheduled; }));
  }
  db_->AbortCompactions(handles_[0]);
  WaitForRemotePoolIdle();
  EXPECT_EQ(LocalScheduled(), 0);
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1U);
  EXPECT_EQ(metadata.levels[0].files[0].temperature, Temperature::kWarm);
  EXPECT_EQ(Get("key"), "value");
  listener->Verify(1);
  ReleasePools();
  db_->ResumeCompactions(handles_[0]);
  EXPECT_OK(dbfull()->TEST_WaitForCompact());
}

class DedicatedCompactionSubmissionExceptionTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<std::tuple<int, int>> {};

TEST_P(DedicatedCompactionSubmissionExceptionTest,
       DrainsAcceptedSubcompactionsBeforeReturning) {
  SetUpCompactions(LocalRouting::kRemote, 3);
  const int failed_sibling = std::get<0>(GetParam());
  const int exception_kind = std::get<1>(GetParam());
  int submissions = 0;
  bool draining = false;
  bool manual_done = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ScheduleRemoteCompaction:BeforeSubmit", [&](void* arg) {
        if (++submissions == failed_sibling + 1) {
          *static_cast<std::function<void()>*>(arg) = [exception_kind] {
            if (exception_kind == 0) {
              throw std::runtime_error("Injected submission exception");
            }
            if (exception_kind == 1) {
              throw std::bad_alloc();
            }
            throw 42;
          };
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::RunSubcompactions:Wait", [&](void*) {
        std::lock_guard<std::mutex> lock(local_mutex_);
        draining = true;
        local_cv_.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status status;
  std::thread manual([&] {
    status = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    std::lock_guard<std::mutex> lock(local_mutex_);
    manual_done = true;
    local_cv_.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return draining; }));
    if (failed_sibling > 1) {
      EXPECT_FALSE(manual_done);
    }
  }
  if (failed_sibling > 1) {
    EXPECT_TRUE(service_->WaitForRemote(failed_sibling - 1));
  }
  service_->Release();
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return manual_done; }));
  }
  manual.join();
  EXPECT_TRUE(status.IsAborted());
  EXPECT_EQ(submissions, failed_sibling + 1);
  WaitForRemotePoolIdle();
  EXPECT_EQ(LocalScheduled(), 0);
  EXPECT_EQ(NumTableFilesAtLevel(0), 4);
  EXPECT_EQ(NumTableFilesAtLevel(1), 0);
  EXPECT_EQ(Get(Key(0)), std::string(1024, 'a'));
  EXPECT_EQ(Get(Key(199)), std::string(1024, 'd'));
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

INSTANTIATE_TEST_CASE_P(Submission, DedicatedCompactionSubmissionExceptionTest,
                        testing::Combine(testing::Values(1, 2),
                                         testing::Values(0, 1, 2)));

TEST_F(CompactionServiceTest, LocalFilterExceptionFailsCompaction) {
  class ThrowingFilterFactory : public CompactionFilterFactory {
   public:
    ThrowingFilterFactory(std::string key, bool unknown)
        : key_(std::move(key)), unknown_(unknown) {}

    const char* Name() const override { return "ThrowingFilterFactory"; }

    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context&) override {
      class ThrowingFilter : public CompactionFilter {
       public:
        ThrowingFilter(std::string key, bool unknown)
            : key_(std::move(key)), unknown_(unknown) {}
        const char* Name() const override { return "ThrowingFilter"; }
        bool Filter(int, const Slice& key, const Slice&, std::string*,
                    bool*) const override {
          if (key == Slice(key_)) {
            if (unknown_) {
              throw 42;
            }
            throw std::runtime_error("Injected local filter exception");
          }
          return false;
        }

       private:
        const std::string key_;
        const bool unknown_;
      };
      return std::make_unique<ThrowingFilter>(key_, unknown_);
    }

   private:
    const std::string key_;
    const bool unknown_;
  };
  class CompletionListener : public EventListener {
   public:
    void OnSubcompactionBegin(const SubcompactionJobInfo&) override {
      ++started;
    }
    void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
      ++completed;
      if (!info.status.ok()) {
        ++failed;
      }
    }
    std::atomic<int> started{0};
    std::atomic<int> completed{0};
    std::atomic<int> failed{0};
  };

  for (bool unknown : {false, true}) {
    SCOPED_TRACE(unknown);
    Options options = CurrentOptions();
    options.use_separate_remote_compaction_pool = true;
    options.disable_auto_compactions = true;
    options.max_subcompactions = 3;
    options.max_background_compactions = 1;
    options.target_file_size_base = 4096;
    options.compression = kNoCompression;
    options.compaction_filter_factory =
        std::make_shared<ThrowingFilterFactory>(Key(100), unknown);
    auto listener = std::make_shared<CompletionListener>();
    options.listeners.push_back(listener);
    ReopenWithCompactionService(&options);
    GetCompactionService()->OverrideStartStatus(
        CompactionServiceJobStatus::kUseLocal);
    GenerateTestData();

    Status status = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    EXPECT_TRUE(status.IsAborted());
    EXPECT_NE(
        status.ToString().find(unknown ? "Unknown local subcompaction exception"
                                       : "Injected local filter exception"),
        std::string::npos);
    EXPECT_GT(listener->started.load(), 1);
    EXPECT_EQ(listener->started.load(), listener->completed.load());
    EXPECT_EQ(listener->failed.load(), 1);
    VerifyTestData();
    Close();
  }
}

class DedicatedCompactionFallbackTest
    : public DedicatedCompactionPoolTest,
      public testing::WithParamInterface<LocalRouting> {};

TEST_P(DedicatedCompactionFallbackTest, CompactFilesFromLowWorker) {
  SetUpCompactions(GetParam(), 3);
  service_->Release();
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  std::vector<std::string> inputs;
  for (const SstFileMetaData& file : metadata.levels[0].files) {
    inputs.push_back(file.db_path + "/" + file.name);
  }
  ASSERT_EQ(inputs.size(), 4);
  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;
  Status status;
  std::function<void()> compact = [&] {
    status = db_->CompactFiles(CompactionOptions(), inputs, 1);
    std::lock_guard<std::mutex> lock(mutex);
    done = true;
    cv.notify_one();
  };
  env_->Schedule(
      [](void* arg) { (*static_cast<std::function<void()>*>(arg))(); },
      &compact, Env::Priority::LOW);
  {
    std::unique_lock<std::mutex> lock(mutex);
    EXPECT_TRUE(
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return done; }));
  }
  // Allow cleanup to complete if a regression queues work behind the caller.
  env_->SetBackgroundThreads(2, Env::Priority::LOW);
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return done; });
  }
  ASSERT_OK(status);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(Get(Key(0)), std::string(1024, 'a'));
  ASSERT_EQ(Get(Key(199)), std::string(1024, 'd'));
}

TEST_P(DedicatedCompactionFallbackTest, DisableQueuedManualLowSubcompactions) {
  CancelQueuedManualCompaction(GetParam(), Env::Priority::LOW, false);
}

TEST_P(DedicatedCompactionFallbackTest, CallerCancellationWhileQueued) {
  CancelQueuedManualCompaction(GetParam(), Env::Priority::LOW, false, true);
}

TEST_P(DedicatedCompactionFallbackTest,
       DisableQueuedManualBottomSubcompactions) {
  CancelQueuedManualCompaction(GetParam(), Env::Priority::BOTTOM, false);
}

TEST_P(DedicatedCompactionFallbackTest, CloseQueuedManualLowSubcompactions) {
  CancelQueuedManualCompaction(GetParam(), Env::Priority::LOW, true);
}

TEST_P(DedicatedCompactionFallbackTest, CloseQueuedManualBottomSubcompactions) {
  CancelQueuedManualCompaction(GetParam(), Env::Priority::BOTTOM, true);
}

TEST_P(DedicatedCompactionFallbackTest,
       CallerCancellationWhileWaitingForCredit) {
  CancelManualWaitingForCredit(GetParam(), 1);
}

TEST_P(DedicatedCompactionFallbackTest,
       CallerCancellationWhileSubcompactionsWaitForCredit) {
  CancelManualWaitingForCredit(GetParam(), 3);
}

TEST_P(DedicatedCompactionFallbackTest, LocalSubcompactionsRespectDBCredits) {
  SetUpCompactions(GetParam(), 3, 4);
  BlockLocalWork();
  service_->Release();
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(WaitForLocal(1, 11));
  {
    std::lock_guard<std::mutex> lock(local_mutex_);
    ASSERT_EQ(local_started_, 1);
  }
  ReleaseLocal();
  VerifyCompacted();
  ASSERT_EQ(peak_local_active_, 1);
  ASSERT_EQ(local_finished_, 12);
}

TEST_P(DedicatedCompactionFallbackTest,
       LowFlushProgressWhileBottomSubcompactionsRun) {
  SetUpCompactions(GetParam(), 3, 1, 1, 2);
  BlockLocalWork();
  service_->Release();
  ASSERT_OK(Put(1, "flush-key", "flush-value"));
  const int high_threads = env_->GetBackgroundThreads(Env::Priority::HIGH);
  env_->SetBackgroundThreads(0, Env::Priority::HIGH);

  Status compaction_status;
  std::thread compaction([&] {
    compaction_status =
        db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  });
  EXPECT_TRUE(WaitForLocal(1, 2));
  Status flush_status;
  bool flushed = false;
  std::thread flush([&] {
    flush_status = Flush(1);
    std::lock_guard<std::mutex> lock(local_mutex_);
    flushed = true;
    local_cv_.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(local_mutex_);
    EXPECT_TRUE(local_cv_.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return flushed; }));
    EXPECT_EQ(local_finished_, 0);
  }
  env_->SetBackgroundThreads(high_threads, Env::Priority::HIGH);
  ReleaseLocal();
  compaction.join();
  flush.join();
  ASSERT_OK(compaction_status);
  ASSERT_OK(flush_status);
  EXPECT_EQ(Get(1, "flush-key"), "flush-value");
  EXPECT_EQ(NumTableFilesAtLevel(0, 1), 5);
}

TEST_P(DedicatedCompactionFallbackTest, BottomSubcompactionsUseBottomPool) {
  SetUpCompactions(GetParam(), 3, 1, 1, 2);
  BlockPool(Env::Priority::LOW);
  BlockLocalWork();
  service_->Release();
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(WaitForLocal(1, 11));
  ReleaseLocal();
  VerifyCompacted();
  ASSERT_EQ(peak_local_active_, 1);
  ASSERT_EQ(local_finished_, 12);
}

TEST_F(DedicatedCompactionPoolTest, LocalWorkRespectsPoolCapacity) {
  SetUpCompactions(LocalRouting::kSchedule, 3, 1, 4);
  BlockLocalWork();
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(WaitForLocal(1, 8));
  ReleaseLocal();
  VerifyCompacted();
  ASSERT_EQ(peak_local_active_, 1);
  ASSERT_EQ(local_finished_, 12);
}

TEST_F(DedicatedCompactionPoolTest, ShutdownCancelsQueuedLocalSubcompactions) {
  SetUpCompactions(LocalRouting::kSchedule, 3);
  BlockPool(Env::Priority::LOW);
  BlockLocalWork();
  ASSERT_OK(dbfull()->EnableAutoCompaction(handles_));
  ASSERT_TRUE(WaitForLocal(0, 11));
  // The LOW worker stays occupied throughout close. Shutdown must unschedule
  // its queued local task and wake the remaining credit waiters.
  Close();
  ASSERT_EQ(local_started_, 0);
  ReleasePools();
}

INSTANTIATE_TEST_CASE_P(LocalRouting, DedicatedCompactionFallbackTest,
                        testing::Values(LocalRouting::kSchedule,
                                        LocalRouting::kWait,
                                        LocalRouting::kInvalidResult));

class PropertySamplingCompactionService : public MyTestCompactionService {
 public:
  using MyTestCompactionService::MyTestCompactionService;

  void SetPrimaryDB(DB* db) { db_ = db; }

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    EXPECT_TRUE(
        db_->GetIntProperty(DB::Properties::kNumRunningRemoteCompactions,
                            &num_running_while_waiting_));
    return MyTestCompactionService::Wait(scheduled_job_id, result);
  }

  uint64_t GetNumRunningWhileWaiting() const {
    return num_running_while_waiting_;
  }

 private:
  DB* db_ = nullptr;
  uint64_t num_running_while_waiting_ = 0;
};

TEST_F(CompactionServiceTest, NumRunningRemoteCompactionsProperty) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env_;
  auto statistics = CreateDBStatistics();
  auto compaction_service = std::make_shared<PropertySamplingCompactionService>(
      dbname_, options, statistics,
      std::vector<std::shared_ptr<EventListener>>{},
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
  options.compaction_service = compaction_service;
  DestroyAndReopen(options);
  compaction_service->SetPrimaryDB(db_.get());

  // Overlapping key ranges across files so the compaction is not a trivial
  // move, which would bypass the compaction service entirely.
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(i * 10 + j)));
    }
    ASSERT_OK(Flush());
  }

  // No remote compaction is in flight yet.
  uint64_t num_running = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningRemoteCompactions,
                                  &num_running));
  ASSERT_EQ(num_running, 0U);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GE(compaction_service->GetCompactionNum(), 1);
  ASSERT_GE(compaction_service->GetNumRunningWhileWaiting(), 1U);

  // The counter is back to 0 once the remote jobs are installed.
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningRemoteCompactions,
                                  &num_running));
  ASSERT_EQ(num_running, 0U);
}

TEST_F(CompactionServiceTest, NumRunningRemoteCompactionsPropertyLocalOnly) {
  // Without a compaction_service configured, the property stays 0.
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Overlapping key ranges across files so the compaction is not a trivial
  // move, which would bypass the compaction service entirely.
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(i * 10 + j)));
    }
    ASSERT_OK(Flush());
  }

  std::atomic<int> property_samples{0};
  std::atomic_bool observed_non_zero{false};
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::Processing",
      [&](void* /*arg*/) {
        uint64_t num_running = 0;
        EXPECT_TRUE(db_->GetIntProperty(
            DB::Properties::kNumRunningRemoteCompactions, &num_running));
        property_samples.fetch_add(1);
        if (num_running != 0) {
          observed_non_zero.store(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GT(property_samples.load(), 0);
  ASSERT_FALSE(observed_non_zero.load());

  uint64_t num_running = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningRemoteCompactions,
                                  &num_running));
  ASSERT_EQ(num_running, 0U);
}

class PartialDeleteCompactionFilter : public CompactionFilter {
 public:
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& key, ValueType /*value_type*/,
      const Slice& /*existing_value*/, std::string* /*new_value*/,
      std::string* /*skip_until*/) const override {
    int i = std::stoi(key.ToString().substr(3));
    if (i > 5 && i <= 105) {
      return CompactionFilter::Decision::kRemove;
    }
    return CompactionFilter::Decision::kKeep;
  }

  const char* Name() const override { return "PartialDeleteCompactionFilter"; }
};

TEST_F(CompactionServiceTest, CompactionFilter) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  std::unique_ptr<CompactionFilter> delete_comp_filter(
      new PartialDeleteCompactionFilter());
  options.compaction_filter = delete_comp_filter.get();
  ReopenWithCompactionService(&options);
  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i > 5 && i <= 105) {
      ASSERT_EQ(result, "NOT_FOUND");
    } else if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
}

TEST_F(CompactionServiceTest, MergeOperator) {
  Options options = CurrentOptions();
  options.merge_operator.reset(new StringAppendOperator(','));
  ReopenWithCompactionService(&options);
  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(db_->Merge(WriteOptions(), Key(i),
                         "merge_op_append_" + std::to_string(i)));
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i) + ",merge_op_append_" +
                            std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i) + ",merge_op_append_" +
                            std::to_string(i));
    }
  }
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
}

TEST_F(CompactionServiceTest, Snapshot) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put(Key(1), "value1"));
  ASSERT_OK(Put(Key(2), "value1"));
  const Snapshot* s1 = db_->GetSnapshot();
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(1), "value2"));
  ASSERT_OK(Put(Key(3), "value2"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  auto my_cs = GetCompactionService();
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  ASSERT_EQ("value1", Get(Key(1), s1));
  ASSERT_EQ("value2", Get(Key(1)));
  db_->ReleaseSnapshot(s1);
}

// Later compactions take write times only from their input files, so every
// output file needs its seqno->time mapping.
static void AssertFilesHaveSeqnoToTimeMapping(DB* db) {
  TablePropertiesCollection props;
  ASSERT_OK(db->GetPropertiesOfAllTables(&props));
  ASSERT_FALSE(props.empty());
  for (const auto& [file_name, file_props] : props) {
    ASSERT_FALSE(file_props->seqno_to_time_mapping.empty()) << file_name;
  }
}

// With the real preclude_last_level_data_seconds option, recently written data
// stays out of the last level, in a remote compaction as in a local one.
TEST_F(CompactionServiceTest, PrecludeLastLevelKeepsRecentData) {
  const int kNumTrigger = 4;
  const int kNumKeys = 100;
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.last_level_temperature = Temperature::kCold;
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = 7;
  options.preclude_last_level_data_seconds = 10000;

  auto write_and_compact = [&]() {
    for (int i = 0; i < kNumTrigger; i++) {
      for (int j = 0; j < kNumKeys; j++) {
        ASSERT_OK(Put(Key(j * kNumTrigger + i), "v" + std::to_string(i)));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  };

  DestroyAndReopen(options);
  write_and_compact();
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  AssertFilesHaveSeqnoToTimeMapping(db_.get());

  ReopenWithCompactionService(&options);
  write_and_compact();
  ASSERT_GE(GetCompactionService()->GetCompactionNum(), 1);
  // The worker's per-key placement matched the primary's, so the remote
  // result was installed rather than redone locally.
  ASSERT_EQ(CompactionServiceJobStatus::kSuccess,
            GetCompactionService()->GetFinalCompactionServiceJobStatus());
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  AssertFilesHaveSeqnoToTimeMapping(db_.get());
}

// If the worker's per-key placement differs from the primary's, the worker
// declines the job and the compaction runs locally.
TEST_F(CompactionServiceTest, PerKeyPlacementMismatchFallsBackToLocal) {
  const int kNumTrigger = 4;
  const int kNumKeys = 100;
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.last_level_temperature = Temperature::kCold;
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = 7;
  options.preclude_last_level_data_seconds = 10000;
  ReopenWithCompactionService(&options);

  std::atomic<int> altered{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void* arg) {
        auto* input = static_cast<CompactionServiceInput*>(arg);
        if (input->proximal_level > 0) {
          input->keep_in_last_level_through_seqno += 1;
          altered++;
        }
      });
  std::atomic<int> mismatches{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CompactWithoutInstallation:PerKeyPlacementCheck",
      [&](void* arg) {
        if (*static_cast<bool*>(arg)) {
          mismatches++;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(j * kNumTrigger + i), "v" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GE(altered.load(), 1);
  ASSERT_EQ(mismatches.load(), altered.load());
  // Only the jobs that were not altered were installed from the worker.
  auto* my_cs = GetCompactionService();
  ASSERT_EQ(my_cs->GetOnInstallationCount(),
            my_cs->GetCompactionNum() - altered.load());
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
}

// The primary never installs a proximal-level output for a compaction without
// a proximal level. It redoes the compaction locally instead.
TEST_F(CompactionServiceTest, UnexpectedProximalOutputFallsBackToLocal) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  CompactionServiceResult result;
  result.status = Status::OK();
  CompactionServiceOutputFile file;
  file.file_name = "000123.sst";
  file.file_size = 1;
  file.marked_for_compaction = false;
  file.is_proximal_level_output = true;
  result.output_files.push_back(file);
  std::string result_str;
  ASSERT_OK(result.Write(&result_str));
  auto my_cs = GetCompactionService();
  my_cs->OverrideWaitResult(result_str);

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  VerifyTestData();
  ASSERT_EQ(1, my_cs->GetOnInstallationCount());
  ASSERT_EQ(CompactionServiceJobStatus::kUseLocal,
            my_cs->GetFinalCompactionServiceJobStatus());
}

// With preserve_internal_time_seconds, a remote compaction keeps the sequence
// numbers of recent writes in the bottommost level, as a local one does.
TEST_F(CompactionServiceTest, PreserveInternalTimeKeepsRecentSeqnos) {
  Options options = CurrentOptions();
  options.preserve_internal_time_seconds = 10000;

  auto write_and_compact = [&](SequenceNumber* largest_seqno) {
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(Put(Key(i), "value"));
    }
    ASSERT_OK(Flush());
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    *largest_seqno = 0;
    for (const auto& f : files) {
      if (f.column_family_name == kDefaultColumnFamilyName) {
        *largest_seqno = std::max(*largest_seqno, f.largest_seqno);
      }
    }
  };

  DestroyAndReopen(options);
  SequenceNumber local_largest_seqno = 0;
  write_and_compact(&local_largest_seqno);
  ASSERT_GT(local_largest_seqno, 0U);
  AssertFilesHaveSeqnoToTimeMapping(db_.get());

  ReopenWithCompactionService(&options);
  SequenceNumber remote_largest_seqno = 0;
  write_and_compact(&remote_largest_seqno);
  ASSERT_GE(GetCompactionService()->GetCompactionNum(), 1);
  ASSERT_GT(remote_largest_seqno, 0U);
  AssertFilesHaveSeqnoToTimeMapping(db_.get());
}

TEST_F(CompactionServiceTest, PrecludeLastLevel) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.last_level_temperature = Temperature::kCold;
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 10;
  options.num_levels = kNumLevels;

  ReopenWithCompactionService(&options);
  // Alternate for comparison: DestroyAndReopen(options);

  // This is simpler than setting up mock time to make the user option work,
  // but is not as direct as testing with preclude option itself.
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled",
      [&](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [&](void* arg) { *static_cast<SequenceNumber*>(arg) = 100; });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(j * kNumTrigger + i), "v" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Data split between proximal (kUnknown) and last (kCold) levels
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // TODO: Check FileSystem temperatures with FileTemperatureTestFS

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_EQ(Get(Key(j * kNumTrigger + i)), "v" + std::to_string(i));
    }
  }

  // Verify Output Stats
  auto my_cs = GetCompactionService();
  {
    CompactionServiceResult result;
    my_cs->GetResult(&result);
    ASSERT_OK(result.status);
    ASSERT_GT(result.internal_stats.output_level_stats.cpu_micros, 0);
    ASSERT_GT(result.internal_stats.output_level_stats.micros, 0);
    ASSERT_EQ(result.internal_stats.output_level_stats.num_output_records +
                  result.internal_stats.proximal_level_stats.num_output_records,
              kNumTrigger * kNumKeys);
    ASSERT_EQ(result.internal_stats.output_level_stats.num_output_files +
                  result.internal_stats.proximal_level_stats.num_output_files,
              2);

    CompactionServiceJobInfo info = my_cs->GetCompactionInfoForStart();
    ASSERT_EQ(0, info.base_input_level);
    ASSERT_EQ(kNumLevels - 1, info.output_level);
  }
  SyncPoint::GetInstance()->DisableProcessing();
  // Disable Preclude feature and run full compaction to the bottommost level
  {
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    CompactionServiceJobInfo info = my_cs->GetCompactionInfoForStart();
    ASSERT_EQ(kNumLevels - 2, info.base_input_level);
    ASSERT_EQ(kNumLevels - 1, info.output_level);
  }
}

TEST_F(CompactionServiceTest, ConcurrentCompaction) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.max_background_jobs = 20;
  ReopenWithCompactionService(&options);
  GenerateTestData(true);

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);

  std::vector<std::thread> threads;
  for (const auto& file : meta.levels[1].files) {
    threads.emplace_back([&]() {
      std::string fname = file.db_path + "/" + file.name;
      ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 2));
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify result
  VerifyTestData();
  auto my_cs = GetCompactionService();
  ASSERT_EQ(my_cs->GetCompactionNum(), 10);
  ASSERT_EQ(FilesPerLevel(), "0,0,10");
}

TEST_F(CompactionServiceTest, RemoteCompactionMissingLowerFileTrustsManifest) {
  // A remote compaction reconstructs its view of the LSM (which files sit below
  // the output level) from the secondary's recovered MANIFEST version. That
  // view drives correctness-critical decisions like whether a deletion
  // tombstone can be dropped (Compaction::KeyNotExistsBeyondOutputLevel /
  // IsBottommostLevel, which read input_version_'s VersionStorageInfo).
  //
  // If a non-input file that is still referenced by the MANIFEST is momentarily
  // missing (as on a live primary), the OLD point-in-time recovery rolls back
  // to an earlier version. Here that rollback drops the (later, whole-DB)
  // manifest record entirely, so the remote worker can no longer find the
  // compaction input and the job fails ("cannot find matched SST files") --
  // i.e. it fails safe rather than mis-compacting. The fix makes recovery TRUST
  // the manifest: it keeps the current version, the job completes, and the
  // deleted key correctly stays deleted (the below-output tombstone is
  // retained).
  Options options = CurrentOptions();
  options.num_levels = 7;
  // -1 keeps the primary's descriptor for the (unlinked) lower file valid, so
  // the final read below is served from the primary regardless of the deletion.
  options.max_open_files = -1;
  options.level0_file_num_compaction_trigger = 100;  // no auto compaction
  ReopenWithCompactionService(&options);

  // Old value for "k", pushed down to L2 (below the compaction output level).
  ASSERT_OK(Put("k", "OLD"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);  // L2 file contains k=OLD

  // Newer tombstone for "k" at L0 (the compaction input).
  ASSERT_OK(Delete("k"));
  ASSERT_OK(Flush());  // L0 file contains Delete(k)

  // Reopen so the MANIFEST is rewritten as current state before we perturb it.
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "cf_1", "cf_2", "cf_3"},
                           options);
  // Closing on reopen cancels pending remote jobs; re-arm the service.
  GetCompactionService()->SetCanceled(false);

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);  // default CF
  ASSERT_EQ(meta.levels[0].files.size(), 1);
  ASSERT_EQ(meta.levels[2].files.size(), 1);
  const auto& f_del = meta.levels[0].files[0];
  const auto& f_old = meta.levels[2].files[0];

  // Make the lower (L2) file momentarily unavailable to a fresh open, as a live
  // primary would if it had compacted it away. It is NOT a compaction input.
  ASSERT_OK(env_->DeleteFile(f_old.db_path + "/" + f_old.name));

  // Remote-compact the L0 tombstone into L1. L2 (with k=OLD) is below the
  // output level, so a correct compaction must retain the tombstone. Without
  // the fix, recovery rolls back and this job fails; with the fix it completes.
  std::string fname = f_del.db_path + "/" + f_del.name;
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 1));

  // Deleted key stays deleted (tombstone retained above the still-present L2
  // value, which the primary can still read via its open descriptor).
  ASSERT_EQ("NOT_FOUND", Get("k"));
}

TEST_F(CompactionServiceTest, RemoteCompactionManifestFloorSendsPosition) {
  // By default (remote_compaction_manifest_floor=true), the primary attaches a
  // non-zero MANIFEST floor to every remote compaction request, and the worker
  // runs its floor check. Here the worker recovers the same-or-later MANIFEST
  // position the primary scheduled from, so the check accepts (the
  // later-manifest positive case).
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  ASSERT_TRUE(db_->GetDBOptions().remote_compaction_manifest_floor);
  GenerateTestData();

  uint64_t floor_number = 0;
  uint64_t floor_size = 0;
  int floor_check_count = 0;
  bool floor_check_rejected = false;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void* arg) {
        auto* input = static_cast<CompactionServiceInput*>(arg);
        floor_number = input->min_manifest_file_number;
        floor_size = input->min_manifest_file_size;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::OpenAndCompact::ManifestFloorCheck", [&](void* arg) {
        floor_check_count++;
        if (*static_cast<bool*>(arg)) {
          floor_check_rejected = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // A floor was populated and the worker accepted its (later-or-equal) view.
  ASSERT_GT(floor_number, 0U);
  ASSERT_GT(floor_size, 0U);
  ASSERT_GE(floor_check_count, 1);
  ASSERT_FALSE(floor_check_rejected);
  VerifyTestData();
}

TEST_F(CompactionServiceTest, RemoteCompactionManifestFloorRejectsStaleView) {
  // If the worker recovers an OLDER MANIFEST view than the primary scheduled
  // from (simulated here by inflating the floor above the real position), the
  // floor check rejects the job and the primary falls back to a correct local
  // compaction rather than compacting against a stale/older LSM shape.
  Options options = CurrentOptions();
  options.num_levels = 7;
  options.level0_file_num_compaction_trigger = 100;  // no auto compaction
  ReopenWithCompactionService(&options);

  // Old value for "k", pushed down to L2 (below the compaction output level).
  ASSERT_OK(Put("k", "OLD"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);  // L2: k=OLD

  // Newer tombstone for "k" at L0 (the compaction input).
  ASSERT_OK(Delete("k"));
  ASSERT_OK(Flush());  // L0: Delete(k)

  int floor_check_rejected = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void* arg) {
        auto* input = static_cast<CompactionServiceInput*>(arg);
        // Simulate the worker seeing an older MANIFEST than the primary
        // scheduled from by raising the floor above the real position.
        input->min_manifest_file_number += 1000000;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::OpenAndCompact::ManifestFloorCheck", [&](void* arg) {
        if (*static_cast<bool*>(arg)) {
          floor_check_rejected++;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);  // default CF
  ASSERT_EQ(meta.levels[0].files.size(), 1);
  const auto& f_del = meta.levels[0].files[0];
  std::string fname = f_del.db_path + "/" + f_del.name;

  // The worker rejects the stale view -> local fallback -> the compaction still
  // completes correctly (CompactFiles returns OK).
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), {fname}, 1));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GE(floor_check_rejected, 1);
  // Deleted key stays deleted: the local fallback retained the below-output
  // tombstone. A silent mis-compaction against the older view would resurrect
  // "OLD".
  ASSERT_EQ("NOT_FOUND", Get("k"));
}

TEST_F(CompactionServiceTest, RemoteCompactionManifestFloorKillSwitchOff) {
  // With the kill switch off, the primary sends no floor, so the worker uses
  // the original point-in-time recovery (no trust-manifest, no floor check). A
  // transiently-missing non-input file then rolls recovery back and fails the
  // job -- confirming the escape hatch reverts to the pre-fix behavior.
  Options options = CurrentOptions();
  options.num_levels = 7;
  options.max_open_files = -1;
  options.open_files_async = true;
  options.skip_stats_update_on_db_open = true;
  options.level0_file_num_compaction_trigger = 100;  // no auto compaction
  options.remote_compaction_manifest_floor = false;  // kill switch OFF
  ReopenWithCompactionService(&options);

  ASSERT_OK(Put("k", "OLD"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);  // L2: k=OLD

  ASSERT_OK(Delete("k"));
  ASSERT_OK(Flush());  // L0: Delete(k)

  // Reopen so the MANIFEST is rewritten as current state before we perturb it.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkAsyncFileOpen:Done",
        "CompactionServiceTest::ManifestFloorKillSwitchOff:OpenDone"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "cf_1", "cf_2", "cf_3"},
                           options);
  TEST_SYNC_POINT("CompactionServiceTest::ManifestFloorKillSwitchOff:OpenDone");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  GetCompactionService()->SetCanceled(false);

  bool floor_provided_seen = true;
  int floor_check_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void* arg) {
        auto* input = static_cast<CompactionServiceInput*>(arg);
        floor_provided_seen = (input->min_manifest_file_number != 0);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::OpenAndCompact::ManifestFloorCheck",
      [&](void* /*arg*/) { floor_check_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[0].files.size(), 1);
  ASSERT_EQ(meta.levels[2].files.size(), 1);
  const auto& f_del = meta.levels[0].files[0];
  const auto& f_old = meta.levels[2].files[0];

  // Make the lower (L2) non-input file momentarily unavailable, as a live
  // primary would if it had compacted it away.
  ASSERT_OK(env_->DeleteFile(f_old.db_path + "/" + f_old.name));

  std::string fname = f_del.db_path + "/" + f_del.name;
  Status s = db_->CompactFiles(CompactionOptions(), {fname}, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // No floor was sent and the worker skipped the floor check; the original
  // recovery rolled back and failed the job (reverted behavior).
  ASSERT_FALSE(floor_provided_seen);
  ASSERT_EQ(0, floor_check_count);
  ASSERT_NOK(s);
}

TEST_F(CompactionServiceTest, RemoteCompactionManifestFloorMutable) {
  // remote_compaction_manifest_floor is a mutable DB option: flipping it via
  // SetDBOptions changes whether the primary attaches a floor to subsequent
  // remote compaction requests (and thus whether the worker runs the floor
  // check).
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);
  GenerateTestData();

  bool floor_provided = false;
  int floor_check_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceJob::ProcessKeyValueCompactionWithCompactionService",
      [&](void* arg) {
        auto* input = static_cast<CompactionServiceInput*>(arg);
        floor_provided = (input->min_manifest_file_number != 0);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::OpenAndCompact::ManifestFloorCheck",
      [&](void* /*arg*/) { floor_check_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);

  // Default on: floor attached, floor check runs.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_TRUE(floor_provided);
  ASSERT_GE(floor_check_count, 1);

  // Turn the kill switch off dynamically.
  ASSERT_OK(db_->SetDBOptions({{"remote_compaction_manifest_floor", "false"}}));
  ASSERT_FALSE(db_->GetDBOptions().remote_compaction_manifest_floor);

  floor_provided = true;
  floor_check_count = 0;

  // Create a fresh compaction target overlapping the range and re-compact.
  ASSERT_OK(Put(Key(20), "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Now no floor is attached and the worker skips the floor check.
  ASSERT_FALSE(floor_provided);
  ASSERT_EQ(0, floor_check_count);
}

namespace {
// A remote compaction service whose output directory already contains an SST
// left behind by a previously interrupted (canceled) fresh attempt, then runs
// OpenAndCompact with allow_resumption=true. Used to verify that when
// resumption is internally disabled, the output directory is cleaned before the
// fresh compaction starts.
class LeftoverOutputCompactionService : public MyTestCompactionService {
 public:
  using MyTestCompactionService::MyTestCompactionService;

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    std::string compaction_input;
    {
      InstrumentedMutexLock l(&mutex_);
      auto it = jobs_.find(scheduled_job_id);
      if (it == jobs_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      compaction_input = it->second;
      jobs_.erase(it);
    }

    CompactionServiceOptionsOverride options_override = GetOptionsOverride();
    Env* env = options_override.env;
    const std::string output_dir = GetOutputPath(scheduled_job_id);
    EXPECT_OK(env->CreateDirIfMissing(output_dir));

    // An output SST left behind by a previously interrupted fresh attempt. Uses
    // a high file number the fresh compaction will not reuse, so whether it is
    // cleaned up is directly observable.
    leftover_file_ = MakeTableFileName(output_dir, 999999);
    {
      std::unique_ptr<WritableFile> f;
      EXPECT_OK(env->NewWritableFile(leftover_file_, &f, EnvOptions()));
      EXPECT_OK(f->Append("leftover"));
      EXPECT_OK(f->Close());
    }

    OpenAndCompactOptions open_options;
    open_options.allow_resumption = true;
    Status s = DB::OpenAndCompact(open_options, db_path_, scheduled_job_id,
                                  compaction_input, result, options_override);
    {
      InstrumentedMutexLock l(&mutex_);
      result_ = *result;
    }
    if (!s.ok()) {
      return CompactionServiceJobStatus::kFailure;
    }
    leftover_cleaned_ = env->FileExists(leftover_file_).IsNotFound();
    return CompactionServiceJobStatus::kSuccess;
  }

  bool leftover_cleaned() const { return leftover_cleaned_; }

 private:
  std::string leftover_file_;
  bool leftover_cleaned_ = false;
};
}  // namespace

TEST_F(CompactionServiceTest, CleansOutputDirWhenResumptionDisabled) {
  // Regression test: allow_resumption=true is requested, but resumption is
  // internally disabled because paranoid_file_checks=true is incompatible with
  // the resumption path's output-hash verification. OpenAndCompact then starts
  // a fresh compaction, which must clean the output directory first (honoring
  // the documented OpenAndCompactOptions::allow_resumption fallback contract),
  // so an output file left by a previously interrupted attempt does not collide
  // with the file numbers the fresh compaction reuses. Before the fix the
  // leftover file was kept, which under a no-reopen filesystem (as in the crash
  // test) fails the fresh compaction's NewWritableFile with a
  // "no-reopen-for-write contract" violation.
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.paranoid_file_checks = true;  // disables resumption on the worker
  options.use_session_tmp_dir_for_remote_compaction = true;
  options.env = env_;
  auto statistics = CreateDBStatistics();

  auto cs = std::make_shared<LeftoverOutputCompactionService>(
      dbname_, options, statistics,
      std::vector<std::shared_ptr<EventListener>>{},
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
  options.compaction_service = cs;
  DestroyAndReopen(options);

  // Two overlapping flushed files so the manual compaction has real work.
  ASSERT_OK(Put(Key(1), "v1"));
  ASSERT_OK(Put(Key(3), "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(2), "v2"));
  ASSERT_OK(Put(Key(3), "v3new"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // The fresh compaction cleaned the leftover output before starting.
  ASSERT_TRUE(cs->leftover_cleaned());
  ASSERT_EQ(Get(Key(1)), "v1");
  ASSERT_EQ(Get(Key(2)), "v2");
  ASSERT_EQ(Get(Key(3)), "v3new");
}

TEST_F(CompactionServiceTest, RetriesSecondaryOpenWhenCurrentFileVanishes) {
  // The remote compaction worker opens the live primary's DB directory
  // read-only inside OpenAndCompact. The primary replaces CURRENT on every
  // MANIFEST rotation, and on a filesystem without POSIX read-after-unlink
  // semantics the CURRENT object the worker already opened becomes unreadable.
  // That is a transient race, so the secondary open must be retried; otherwise
  // a CompactionService that does not fall back to a local compaction
  // (db_stress with remote_compaction_failure_fall_back_to_local=false) turns
  // it into a primary background error that fails every later flush.
  auto fs = std::make_shared<VanishingCurrentFileFS>(env_->GetFileSystem());
  std::unique_ptr<Env> vanishing_env(NewCompositeEnv(fs));

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = vanishing_env.get();
  auto statistics = CreateDBStatistics();

  auto cs = std::make_shared<MyTestCompactionService>(
      dbname_, options, statistics,
      std::vector<std::shared_ptr<EventListener>>{},
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
  cs->SetMaxSecondaryOpenRetries(3);
  options.compaction_service = cs;
  DestroyAndReopen(options);

  // Two overlapping flushed files so the manual compaction has real work.
  ASSERT_OK(Put(Key(1), "v1"));
  ASSERT_OK(Put(Key(3), "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(2), "v2"));
  ASSERT_OK(Put(Key(3), "v3new"));
  ASSERT_OK(Flush());

  // Arm the injection only now: the primary is done opening, so the failed
  // CURRENT reads below happen in the worker's secondary open. Three failures
  // exercise a non-default retry limit and require the fourth attempt.
  fs->SetFailuresToInject(3);

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  const Status compact_s = db_->CompactRange(cro, nullptr, nullptr);

  // Collect everything before closing: the DB must be closed while the
  // injecting Env is still alive, so no assertion may return early above it.
  const int injected_failures = fs->injected_failures();
  const int compactions = cs->GetCompactionNum();
  const std::string v1 = Get(Key(1));
  const std::string v2 = Get(Key(2));
  const std::string v3 = Get(Key(3));
  Close();

  ASSERT_OK(compact_s);
  ASSERT_EQ(3, injected_failures);
  ASSERT_GE(compactions, 1);
  ASSERT_EQ(v1, "v1");
  ASSERT_EQ(v2, "v2");
  ASSERT_EQ(v3, "v3new");
}

TEST_F(CompactionServiceTest, CanDisableSecondaryOpenRetry) {
  auto fs = std::make_shared<VanishingCurrentFileFS>(env_->GetFileSystem());
  std::unique_ptr<Env> vanishing_env(NewCompositeEnv(fs));

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = vanishing_env.get();
  auto statistics = CreateDBStatistics();

  auto cs = std::make_shared<MyTestCompactionService>(
      dbname_, options, statistics,
      std::vector<std::shared_ptr<EventListener>>{},
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
  cs->SetMaxSecondaryOpenRetries(0);
  options.compaction_service = cs;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(1), "v1"));
  ASSERT_OK(Put(Key(3), "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(2), "v2"));
  ASSERT_OK(Put(Key(3), "v3new"));
  ASSERT_OK(Flush());

  fs->SetFailuresToInject(1);

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  const Status compact_s = db_->CompactRange(cro, nullptr, nullptr);
  const int injected_failures = fs->injected_failures();
  const int compactions = cs->GetCompactionNum();
  Close();

  ASSERT_TRUE(compact_s.IsIncomplete()) << compact_s.ToString();
  ASSERT_EQ(1, injected_failures);
  ASSERT_EQ(1, compactions);
}

TEST_F(CompactionServiceTest, CompactionInfo) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  auto my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());
  uint64_t comp_num = my_cs->GetCompactionNum();
  ASSERT_GE(comp_num, 1);

  CompactionServiceJobInfo info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(dbname_, info.db_name);
  std::string db_id;
  std::string db_session_id;
  ASSERT_OK(db_->GetDbIdentity(db_id));
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_OK(db_->GetDbSessionId(db_session_id));
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(dbname_, info.db_name);
  ASSERT_EQ(db_id, info.db_id);
  ASSERT_EQ(db_session_id, info.db_session_id);
  ASSERT_EQ(Env::LOW, info.priority);

  // Test priority USER
  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  SstFileMetaData file = meta.levels[1].files[0];
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {file.db_path + "/" + file.name}, 2));
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(Env::USER, info.priority);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(1, info.base_input_level);
  ASSERT_EQ(2, info.output_level);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::USER, info.priority);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(1, info.base_input_level);
  ASSERT_EQ(2, info.output_level);
  ASSERT_EQ(kDefaultColumnFamilyName, info.cf_name);

  // Test priority BOTTOM
  env_->SetBackgroundThreads(1, Env::BOTTOM);
  // This will set bottommost_level = true but is_full_compaction = false
  options.num_levels = 2;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum, info.compaction_reason);
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(Env::BOTTOM, info.priority);
  ASSERT_EQ(0, info.base_input_level);
  ASSERT_EQ(db_->NumberLevels() - 1, info.output_level);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(Env::BOTTOM, info.priority);
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum, info.compaction_reason);
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(0, info.base_input_level);
  ASSERT_EQ(db_->NumberLevels() - 1, info.output_level);

  // Test Non-Bottommost Level
  options.num_levels = 4;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());
  int compaction_num = my_cs->GetCompactionNum();
  ASSERT_EQ(0, compaction_num);

  for (int i = 0; i < options.level0_file_num_compaction_trigger; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value_new_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // This is trivial move. Done locally.
  ASSERT_EQ(0, my_cs->GetCompactionNum());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(false, info.bottommost_level);
  ASSERT_EQ(-1, info.base_input_level);
  ASSERT_EQ(-1, info.output_level);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(false, info.is_manual_compaction);
  ASSERT_EQ(false, info.is_full_compaction);
  ASSERT_EQ(false, info.bottommost_level);
  ASSERT_EQ(-1, info.base_input_level);
  ASSERT_EQ(-1, info.output_level);

  // Test Full Compaction + Bottommost Level
  options.num_levels = 6;
  ReopenWithCompactionService(&options);
  my_cs =
      static_cast_with_check<MyTestCompactionService>(GetCompactionService());

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value_new_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(options.num_levels - 1);

  // Force final level compaction
  // base_input_level == output_level == last_level
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  info = my_cs->GetCompactionInfoForStart();
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(true, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
  info = my_cs->GetCompactionInfoForWait();
  ASSERT_EQ(options.num_levels - 1, info.base_input_level);
  ASSERT_EQ(options.num_levels - 1, info.output_level);
  ASSERT_EQ(true, info.is_manual_compaction);
  ASSERT_EQ(true, info.is_full_compaction);
  ASSERT_EQ(true, info.bottommost_level);
  ASSERT_EQ(CompactionReason::kManualCompaction, info.compaction_reason);
  ASSERT_EQ(options.num_levels - 1, info.base_input_level);
  ASSERT_EQ(options.num_levels - 1, info.output_level);
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
}

TEST_F(CompactionServiceTest, FallbackLocalAuto) {
  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  my_cs->OverrideStartStatus(CompactionServiceJobStatus::kUseLocal);

  GenerateTestData();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  VerifyTestData();

  ASSERT_EQ(my_cs->GetCompactionNum(), 0);

  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_READ_BYTES), 0);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES), 0);
}

TEST_F(CompactionServiceTest, FallbackLocalManual) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();
  uint64_t compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  uint64_t primary_write_bytes =
      primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  // re-enable remote compaction
  my_cs->ResetOverride();
  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);
  uint64_t comp_num = my_cs->GetCompactionNum();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  ASSERT_GE(my_cs->GetCompactionNum(), comp_num + 1);
  // make sure the compaction statistics is only recorded on the remote side
  ASSERT_GT(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES));
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);

  // return run local again with API WaitForComplete
  my_cs->OverrideWaitStatus(CompactionServiceJobStatus::kUseLocal);
  start_str = Key(120);
  start = start_str;
  comp_num = my_cs->GetCompactionNum();
  compactor_write_bytes =
      compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES);
  primary_write_bytes = primary_statistics->getTickerCount(COMPACT_WRITE_BYTES);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, nullptr));
  ASSERT_EQ(my_cs->GetCompactionNum(),
            comp_num);  // no remote compaction is run
  // make sure the compaction statistics is only recorded on the local side
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            compactor_write_bytes);
  ASSERT_GT(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES),
            primary_write_bytes);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES),
            compactor_write_bytes);

  // verify result after 2 manual compactions
  VerifyTestData();
}

TEST_F(CompactionServiceTest, AbortedWhileWait) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  ReopenWithCompactionService(&options);

  GenerateTestData();
  VerifyTestData();

  auto my_cs = GetCompactionService();
  Statistics* compactor_statistics = GetCompactorStatistics();
  Statistics* primary_statistics = GetPrimaryStatistics();

  my_cs->ResetOverride();
  std::string start_str = Key(15);
  std::string end_str = Key(45);
  Slice start(start_str);
  Slice end(end_str);

  // Override Wait() result with kAborted
  my_cs->OverrideWaitStatus(CompactionServiceJobStatus::kAborted);
  start_str = Key(120);
  start = start_str;

  Status s = db_->CompactRange(CompactRangeOptions(), &start, nullptr);
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsAborted());
  // no remote compaction is run
  ASSERT_EQ(my_cs->GetCompactionNum(), 0);
  // make sure the compaction statistics is not recorded any side
  ASSERT_EQ(primary_statistics->getTickerCount(COMPACT_WRITE_BYTES), 0);
  ASSERT_EQ(primary_statistics->getTickerCount(REMOTE_COMPACT_WRITE_BYTES), 0);
  ASSERT_EQ(compactor_statistics->getTickerCount(COMPACT_WRITE_BYTES), 0);
}

TEST_F(CompactionServiceTest, RemoteEventListener) {
  class RemoteEventListenerTest : public EventListener {
   public:
    const char* Name() const override { return "RemoteEventListenerTest"; }

    void OnSubcompactionBegin(const SubcompactionJobInfo& info) override {
      auto result = on_going_compactions.emplace(info.job_id);
      ASSERT_TRUE(result.second);  // make sure there's no duplication
      compaction_num++;
      EventListener::OnSubcompactionBegin(info);
    }
    void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
      auto num = on_going_compactions.erase(info.job_id);
      ASSERT_TRUE(num == 1);  // make sure the compaction id exists
      EventListener::OnSubcompactionCompleted(info);
    }
    void OnTableFileCreated(const TableFileCreationInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_created++;
      EventListener::OnTableFileCreated(info);
    }
    void OnTableFileCreationStarted(
        const TableFileCreationBriefInfo& info) override {
      ASSERT_EQ(on_going_compactions.count(info.job_id), 1);
      file_creation_started++;
      EventListener::OnTableFileCreationStarted(info);
    }

    bool ShouldBeNotifiedOnFileIO() override {
      file_io_notified++;
      return EventListener::ShouldBeNotifiedOnFileIO();
    }

    std::atomic_uint64_t file_io_notified{0};
    std::atomic_uint64_t file_creation_started{0};
    std::atomic_uint64_t file_created{0};

    std::set<int> on_going_compactions;  // store the job_id
    std::atomic_uint64_t compaction_num{0};
  };

  auto listener = new RemoteEventListenerTest();
  remote_listeners.emplace_back(listener);

  Options options = CurrentOptions();
  ReopenWithCompactionService(&options);

  for (int i = 0; i < 20; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }

  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value_new" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // check the events are triggered
  ASSERT_TRUE(listener->file_io_notified > 0);
  ASSERT_TRUE(listener->file_creation_started > 0);
  ASSERT_TRUE(listener->file_created > 0);
  ASSERT_TRUE(listener->compaction_num > 0);
  ASSERT_TRUE(listener->on_going_compactions.empty());

  // verify result
  for (int i = 0; i < 200; i++) {
    auto result = Get(Key(i));
    if (i % 2) {
      ASSERT_EQ(result, "value" + std::to_string(i));
    } else {
      ASSERT_EQ(result, "value_new" + std::to_string(i));
    }
  }
}

namespace {
// Runs each remote compaction in two attempts. The first attempt stops after
// it saves progress. The second attempt resumes after the saved output files
// are deleted, so the resume fails and the primary compacts locally.
class FailedResumeCompactionService : public MyTestCompactionService {
 public:
  using MyTestCompactionService::MyTestCompactionService;

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    std::string compaction_input;
    {
      InstrumentedMutexLock l(&mutex_);
      auto it = jobs_.find(scheduled_job_id);
      if (it == jobs_.end()) {
        return CompactionServiceJobStatus::kFailure;
      }
      compaction_input = it->second;
      jobs_.erase(it);
    }

    CompactionServiceOptionsOverride options_override = GetOptionsOverride();
    std::atomic<bool> canceled{false};
    OpenAndCompactOptions open_options;
    open_options.allow_resumption = true;
    open_options.canceled = &canceled;

    // Put each key in its own output file, and stop at the first saved
    // progress.
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionOutputs::ShouldStopBefore::manual_decision", [](void* p) {
          *static_cast<std::pair<bool*, const Slice>*>(p)->first = true;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionJob::PersistSubcompactionProgress:Complete",
        [&](void*) { canceled.store(true); });
    SyncPoint::GetInstance()->EnableProcessing();
    std::string first_result;
    Status s =
        DB::OpenAndCompact(open_options, db_path_, scheduled_job_id,
                           compaction_input, &first_result, options_override);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_TRUE(s.IsManualCompactionPaused()) << s.ToString();

    Env* env = options_override.env;
    const std::string output_dir = GetOutputPath(scheduled_job_id);
    std::vector<std::string> children;
    EXPECT_OK(env->GetChildren(output_dir, &children));
    for (const auto& child : children) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(child, &number, &type) && type == kTableFile) {
        EXPECT_OK(env->DeleteFile(MakeTableFileName(output_dir, number)));
      }
    }

    canceled.store(false);
    s = DB::OpenAndCompact(open_options, db_path_, scheduled_job_id,
                           compaction_input, result, options_override);
    EXPECT_TRUE(s.IsCorruption()) << s.ToString();
    return CompactionServiceJobStatus::kUseLocal;
  }
};
}  // namespace

TEST_F(CompactionServiceTest, FailedResumeReportsSubcompactionCompleted) {
  class SubcompactionEventCounter : public EventListener {
   public:
    const char* Name() const override { return "SubcompactionEventCounter"; }

    void OnSubcompactionBegin(const SubcompactionJobInfo& /*info*/) override {
      ++begin_count;
    }
    void OnSubcompactionCompleted(const SubcompactionJobInfo& info) override {
      ++completed_count;
      if (info.status.IsCorruption()) {
        ++corruption_count;
      }
    }

    std::atomic<int> begin_count{0};
    std::atomic<int> completed_count{0};
    std::atomic<int> corruption_count{0};
  };

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env_;
  auto listener = std::make_shared<SubcompactionEventCounter>();
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();
  auto cs = std::make_shared<FailedResumeCompactionService>(
      dbname_, options, statistics,
      std::vector<std::shared_ptr<EventListener>>{listener},
      std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
  options.compaction_service = cs;
  DestroyAndReopen(options);

  for (int i = 0; i < 2; ++i) {
    for (int key = 0; key < 4; ++key) {
      ASSERT_OK(Put(Key(key), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Each attempt that began a subcompaction also reported its completion, and
  // the failed resume reported its status.
  ASSERT_GT(listener->begin_count, 0);
  ASSERT_EQ(listener->completed_count, listener->begin_count);
  ASSERT_GT(listener->corruption_count, 0);
  for (int key = 0; key < 4; ++key) {
    ASSERT_EQ(Get(Key(key)), "value1");
  }
}

TEST_F(CompactionServiceTest, TablePropertiesCollector) {
  const static std::string kUserPropertyName = "TestCount";

  class TablePropertiesCollectorTest : public TablePropertiesCollector {
   public:
    Status Finish(UserCollectedProperties* properties) override {
      *properties = UserCollectedProperties{
          {kUserPropertyName, std::to_string(count_)},
      };
      return Status::OK();
    }

    UserCollectedProperties GetReadableProperties() const override {
      return UserCollectedProperties();
    }

    const char* Name() const override { return "TablePropertiesCollectorTest"; }

    Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                      EntryType /*type*/, SequenceNumber /*seq*/,
                      uint64_t /*file_size*/) override {
      count_++;
      return Status::OK();
    }

   private:
    uint32_t count_ = 0;
  };

  class TablePropertiesCollectorFactoryTest
      : public TablePropertiesCollectorFactory {
   public:
    TablePropertiesCollector* CreateTablePropertiesCollector(
        TablePropertiesCollectorFactory::Context /*context*/) override {
      return new TablePropertiesCollectorTest();
    }

    const char* Name() const override {
      return "TablePropertiesCollectorFactoryTest";
    }
  };

  auto factory = new TablePropertiesCollectorFactoryTest();
  remote_table_properties_collector_factories.emplace_back(factory);

  const int kNumSst = 3;
  const int kLevel0Trigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  ReopenWithCompactionService(&options);

  // generate a few SSTs locally which should not have user property
  for (int i = 0; i < kNumSst; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }

  TablePropertiesCollection fname_to_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    ASSERT_EQ(it, properties.end());
  }

  // trigger compaction
  for (int i = kNumSst; i < kLevel0Trigger; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(db_->GetPropertiesOfAllTables(&fname_to_props));

  bool has_user_property = false;
  for (const auto& file_props : fname_to_props) {
    auto properties = file_props.second->user_collected_properties;
    auto it = properties.find(kUserPropertyName);
    if (it != properties.end()) {
      has_user_property = true;
      ASSERT_GT(std::stoi(it->second), 0);
    }
  }
  ASSERT_TRUE(has_user_property);
}

class ResumableCompactionService : public MyTestCompactionService {
 public:
  enum class TestScenario {
    // Test scenario 1: Two-phase compaction with resumption
    // - Phase 1: Cancel the compaction running with resumption enabled (saves
    // progress)
    // - Phase 2: Resume from saved progress and complete
    // Validates: Resumption reduces redundant work
    kCancelThenResume,

    // Test scenario 2: Two-phase compaction without resumption
    // - Phase 1: Cancel the compaction running with resumption enabled (saves
    // progress)
    // - Phase 2: Start fresh without resumption (ignores saved progress) and
    // complete
    // Validates: Disabling resumption causes full reprocessing
    kCancelThenFreshStart,

    // Test scenario 3: Three-phase compaction toggling resumption on/off/on
    // - Phase 1: Cancel the compaction running with resumption enabled (saves
    // progress)
    // - Phase 2: Start fresh wtihout resumption (ignores saved progress) and
    // cancel agains
    // - Phase 3: Resume with resumption support (loads Phase 1's progress) and
    // complete
    // Validates: Resumption state can be toggled;
    kMultipleCancelToggleResumption,

    // Test scenario 4: Resume with the pre-allocated blob range exhausted.
    // Validates: The worker returns Incomplete and the primary falls back to
    // local compaction without installing partial remote output.
    kResumeRangeExhaustion
  };

  ResumableCompactionService(const std::string& db_path, Options& options,
                             std::shared_ptr<Statistics> statistics,
                             TestScenario scenario,
                             size_t output_file_key_interval = 1,
                             bool merge_resumed_output = false)
      : MyTestCompactionService(db_path, options, statistics,
                                {} /* listeners */,
                                {} /* table_properties_collector_factories */),
        scenario_(scenario),
        output_file_key_interval_(output_file_key_interval),
        merge_resumed_output_(merge_resumed_output) {}

  // Set the user key where cancellation should happen.
  void SetCancelAtKey(const std::string& key, SequenceNumber seqno) {
    cancel_at_key_ = key;
    cancel_at_seqno_ = seqno;
  }

  // Run an extra resumed-then-canceled phase between the first canceled phase
  // and the final phase, canceling at `key`.
  void SetSecondCancelAtKey(const std::string& key) {
    second_cancel_at_key_ = key;
  }

  void CaptureVerifiedInputRecords(std::atomic<uint64_t>* records) {
    verified_input_records_ = records;
  }

  CompactionServiceJobStatus Wait(const std::string& scheduled_job_id,
                                  std::string* result) override {
    std::string compaction_input = ExtractCompactionInput(scheduled_job_id);
    EXPECT_FALSE(compaction_input.empty());

    OpenAndCompactOptions open_and_compaction_options;
    auto override_options = GetOptionsOverride();

    // Force creation of one key per output file for test simplicity.
    // ASSUMPTION: This makes stats.count directly proportional to keys
    // processed.
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionOutputs::ShouldStopBefore::manual_decision",
        [this](void* p) {
          auto* pair = static_cast<std::pair<bool*, const Slice>*>(p);
          if (output_file_key_interval_ > 0 &&
              !(merge_resumed_output_ && already_canceled_)) {
            *(pair->first) = output_key_count_ % output_file_key_interval_ == 0;
            ++output_key_count_;
          }

          // If cancel_at_key_ is set, cancel when we encounter that key
          if (!cancel_at_key_.empty() && !already_canceled_) {
            ParsedInternalKey parsed_key;
            if (ParseInternalKey(pair->second, &parsed_key, true).ok()) {
              if (parsed_key.user_key.ToString() == cancel_at_key_) {
                // Check sequence number if specified
                if (cancel_at_seqno_ == kMaxSequenceNumber ||
                    parsed_key.sequence == cancel_at_seqno_) {
                  canceled_ = true;
                  already_canceled_ = true;
                }
              }
            }
          }
        });

    // If no cancel_at_key_ is set, use the original behavior:
    // Simulate cancelled compaction by overriding status at completion. So
    // compaction processes all keys before this point to make stats.count
    // comparison straightforward.
    if (cancel_at_key_.empty()) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImplSecondary::CompactWithoutInstallation::End",
          [&](void* status) {
            auto s = static_cast<Status*>(status);
            *s = Status::Incomplete(Status::SubCode::kManualCompactionPaused);
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();

    // Phase 1: Run compaction with resumption enabled and cancel it
    // - Processes input keys until cancellation point
    // - Creates output files and saves progress
    // - Status overridden to "paused"
    open_and_compaction_options.allow_resumption = true;
    open_and_compaction_options.canceled = &canceled_;
    already_canceled_ = false;
    canceled_ = false;
    output_key_count_ = 0;

    auto phase1_stats =
        RunCancelledCompaction(open_and_compaction_options, scheduled_job_id,
                               compaction_input, override_options);
    std::string output_path = GetOutputPath(scheduled_job_id);
    TEST_SYNC_POINT_CALLBACK(
        "ResumableCompactionService::AfterCancelledCompaction", &output_path);

    if (!second_cancel_at_key_.empty()) {
      open_and_compaction_options.allow_resumption = true;
      cancel_at_key_ = second_cancel_at_key_;
      already_canceled_ = false;
      canceled_ = false;
      output_key_count_ = 0;
      RunCancelledCompaction(open_and_compaction_options, scheduled_job_id,
                             compaction_input, override_options);
    }

    HistogramData phase2_stats;

    if (scenario_ == TestScenario::kMultipleCancelToggleResumption) {
      // Phase 2: Run compaction WITHOUT resumption (fresh start) and cancel it
      // - Delete all files left behind Phase 1 before calling OpenAndCompact()
      // - Processes all input keys again from scratch
      // - Creates output files but does NOT save progress
      // - Status overridden to "paused"
      open_and_compaction_options.allow_resumption = false;

      // Clean up output folder for fresh start
      std::string output_dir = GetOutputPath(scheduled_job_id);
      Status cleanup_status = DestroyDir(override_options.env, output_dir);
      EXPECT_TRUE(cleanup_status.ok());
      EXPECT_OK(override_options.env->CreateDir(output_dir));

      already_canceled_ = false;
      canceled_ = false;
      output_key_count_ = 0;

      phase2_stats =
          RunCancelledCompaction(open_and_compaction_options, scheduled_job_id,
                                 compaction_input, override_options);

      // Validation: Phase 2 starts from scratch, so it processes the same
      // input keys as Phase 1.
      // ASSUMPTION: With fixed input (10 keys) and deterministic cancellation
      // (after processing), both phases create the same number of output files.
      EXPECT_EQ(phase2_stats.count, phase1_stats.count);
    }

    // Final phase: Run compaction to completion (no cancellation)
    if (scenario_ == TestScenario::kMultipleCancelToggleResumption) {
      // Attempt to resume but it ends up starting fresh
      open_and_compaction_options.allow_resumption = true;
    } else if (scenario_ == TestScenario::kCancelThenResume ||
               scenario_ == TestScenario::kResumeRangeExhaustion) {
      // Resume from Phase 1's saved progress
      open_and_compaction_options.allow_resumption = true;
    } else {  // kCancelThenFreshStart
      // Start fresh without resumption
      open_and_compaction_options.allow_resumption = false;

      // Clean up output folder for fresh start
      std::string output_dir = GetOutputPath(scheduled_job_id);
      Status cleanup_status = DestroyDir(override_options.env, output_dir);
      EXPECT_TRUE(cleanup_status.ok());
      EXPECT_OK(override_options.env->CreateDir(output_dir));
    }

    // Prevent triggering of cancellation
    SyncPoint::GetInstance()->ClearCallBack(
        "DBImplSecondary::CompactWithoutInstallation::End");
    already_canceled_ = true;
    canceled_ = false;
    output_key_count_ = 0;

    TEST_SYNC_POINT_CALLBACK(
        "ResumableCompactionService::BeforeFinalCompaction", &compaction_input);

    if (scenario_ == TestScenario::kResumeRangeExhaustion) {
      Status s = DB::OpenAndCompact(open_and_compaction_options, db_path_,
                                    scheduled_job_id, compaction_input, result,
                                    override_options);
      StoreResult(*result);
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      EXPECT_TRUE(s.IsIncomplete());
      EXPECT_NE(s.ToString().find("Blob file number range exhausted"),
                std::string::npos)
          << s.ToString();
      return CompactionServiceJobStatus::kUseLocal;
    }

    auto final_phase_stats =
        RunCompaction(open_and_compaction_options, scheduled_job_id,
                      compaction_input, override_options, result);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (verified_input_records_ != nullptr) {
      auto* const records = verified_input_records_;
      SyncPoint::GetInstance()->SetCallBack(
          "CompactionJob::VerifyInputRecordCount:Actual",
          [records](void* arg) { *records = *static_cast<uint64_t*>(arg); });
      SyncPoint::GetInstance()->EnableProcessing();
    }

    // Validate statistics based on scenario (only when cancelling at end)
    if (cancel_at_key_.empty()) {
      if (scenario_ == TestScenario::kMultipleCancelToggleResumption) {
        // ASSUMPTION: Phase 1 processes all keys before cancellation
        EXPECT_GT(phase1_stats.count, 0);

        // ASSUMPTION: Phase 2 runs with allow_resumption=false and an empty
        // folder. Phase 2 then creates its own output files (but doesn't save
        // progress). When Phase 3 starts with allow_resumption=true, it finds
        // no progress file exists, so it cannot resume and must start from
        // scratch, processing all input keys again. Result: Phase 3 does the
        // same amount of work as Phase 1.
        EXPECT_EQ(final_phase_stats.count, phase1_stats.count);

      } else if (scenario_ == TestScenario::kCancelThenResume) {
        // ASSUMPTION: Phase 1 processes all keys before cancellation
        EXPECT_GT(phase1_stats.count, 0);

        // ASSUMPTION: Phase 1 processes all keys and saves progress before
        // cancellation. Final phase resumes from Phase 1's saved progress.
        // Since Phase 1 completed all processing before being cancelled, the
        // final phase should do less work than Phase 1.
        EXPECT_LT(final_phase_stats.count, phase1_stats.count);

      } else {  // kCancelThenFreshStart
                // ASSUMPTION: Phase 1 processes all keys before cancellation
        EXPECT_GT(phase1_stats.count, 0);

        // ASSUMPTION: Final phase starts fresh without resumption, so it
        // processes all input keys again and creates the same number of files
        EXPECT_EQ(final_phase_stats.count, phase1_stats.count);
      }
    }

    StoreResult(*result);

    return CompactionServiceJobStatus::kSuccess;
  }

 private:
  std::string ExtractCompactionInput(const std::string& scheduled_job_id) {
    InstrumentedMutexLock l(&mutex_);

    auto job_index = jobs_.find(scheduled_job_id);
    if (job_index == jobs_.end()) {
      return "";
    }
    std::string compaction_input = std::move(job_index->second);
    jobs_.erase(job_index);

    auto info_index = infos_.find(scheduled_job_id);
    if (info_index == infos_.end()) {
      return "";
    }
    infos_.erase(info_index);

    return compaction_input;
  }

  HistogramData RunCancelledCompaction(
      const OpenAndCompactOptions& options, const std::string& scheduled_job_id,
      const std::string& compaction_input,
      const CompactionServiceOptionsOverride& override_options) {
    std::string temp_result;
    EXPECT_OK(statistics_->Reset());

    Status s =
        DB::OpenAndCompact(options, db_path_, scheduled_job_id,
                           compaction_input, &temp_result, override_options);

    EXPECT_TRUE(s.IsManualCompactionPaused());

    HistogramData stats;
    statistics_->histogramData(FILE_WRITE_COMPACTION_MICROS, &stats);
    return stats;
  }

  HistogramData RunCompaction(
      const OpenAndCompactOptions& options, const std::string& scheduled_job_id,
      const std::string& compaction_input,
      const CompactionServiceOptionsOverride& override_options,
      std::string* result) {
    EXPECT_OK(statistics_->Reset());

    Status s = DB::OpenAndCompact(options, db_path_, scheduled_job_id,
                                  compaction_input, result, override_options);

    EXPECT_TRUE(s.ok()) << s.ToString();

    HistogramData stats;
    statistics_->histogramData(FILE_WRITE_COMPACTION_MICROS, &stats);
    return stats;
  }

  void StoreResult(const std::string& result) {
    InstrumentedMutexLock l(&mutex_);
    result_ = result;
  }

  TestScenario scenario_;
  const size_t output_file_key_interval_;
  const bool merge_resumed_output_;
  size_t output_key_count_ = 0;
  std::string cancel_at_key_;
  std::string second_cancel_at_key_;
  SequenceNumber cancel_at_seqno_ = kMaxSequenceNumber;
  std::atomic<bool> already_canceled_{false};
  std::atomic<uint64_t>* verified_input_records_ = nullptr;
};

class ResumableCompactionServiceTest : public CompactionServiceTest {
 public:
  explicit ResumableCompactionServiceTest() : CompactionServiceTest() {}

  uint64_t CountPrimaryPointInputRecords() {
    TablePropertiesCollection properties;
    EXPECT_OK(db_->GetPropertiesOfAllTables(&properties));
    uint64_t input_records = 0;
    for (const auto& entry : properties) {
      input_records +=
          entry.second->num_entries - entry.second->num_range_deletions;
    }
    return input_records;
  }

  void AssertDroppedNewerVersionScenarioPointInputTotal() {
    // Before compaction, the three input files contain four point entries:
    // j@1 and k@2 in the first file, k@3 and l@4 in the second, plus a range
    // tombstone [a, kk) in the third. This only verifies that the fixture built
    // the intended scenario and is independent of option A.
    EXPECT_EQ(CountPrimaryPointInputRecords(), 4);
  }

  void AssertDroppedNewerVersionRecordCounts(
      const CompactionServiceResult& result, uint64_t verified_input_records) {
    // The range tombstone drops k@3 while j@1, k@2, and l@4 remain as point
    // outputs. Both VerifyInputRecordCount's installed-worker value and
    // result.stats cover the resumed scan of k@3, k@2, and l@4. The result's
    // output count covers all point outputs returned for installation.
    constexpr uint64_t kExpectedInputRecordsAfterResume = 3;
    EXPECT_EQ(verified_input_records, kExpectedInputRecordsAfterResume);
    EXPECT_EQ(result.stats.num_input_records, kExpectedInputRecordsAfterResume);
    EXPECT_EQ(result.stats.num_output_records, 3);
  }

  void RunBlobCompactionResumeAfterDroppedNewerVersion(
      bool verify_record_count, bool merge_resumed_output) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    options.compaction_verify_record_count = verify_record_count;
    std::shared_ptr<Statistics> statistics = CreateDBStatistics();

    auto resume_cs = std::make_shared<ResumableCompactionService>(
        dbname_, options, statistics,
        ResumableCompactionService::TestScenario::kCancelThenResume,
        /*output_file_key_interval=*/1, merge_resumed_output);
    resume_cs->SetCancelAtKey("k", kMaxSequenceNumber);
    resume_cs->SetWorkerOptionOverride("compaction_verify_record_count",
                                       verify_record_count ? "true" : "false");
    options.compaction_service = resume_cs;

    DestroyAndReopen(options);
    const std::string value(100, 'v');
    ASSERT_OK(Put("j", value));
    ASSERT_OK(Put("k", value));
    ASSERT_OK(Flush());
    ASSERT_EQ(GetBlobFileNumbers().size(), 1);

    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(Put("k", value));
    ASSERT_OK(Put("l", value));
    ASSERT_OK(Flush());
    const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_EQ(blob_files.size(), 2);
    const uint64_t second_blob_file = blob_files.back();

    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a",
                               "kk"));
    ASSERT_OK(Flush());
    AssertDroppedNewerVersionScenarioPointInputTotal();

    std::atomic<size_t> persisted_progress{0};
    std::atomic<uint64_t> verified_input_records{0};
    resume_cs->CaptureVerifiedInputRecords(&verified_input_records);
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionJob::PersistSubcompactionProgress:Complete",
        [&](void*) { ++persisted_progress; });

    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    const Status status = db_->CompactRange(compact_options, nullptr, nullptr);
    db_->ReleaseSnapshot(snapshot);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    EXPECT_OK(status);
    CompactionServiceResult result;
    resume_cs->GetResult(&result);
    AssertDroppedNewerVersionRecordCounts(result,
                                          verified_input_records.load());
    EXPECT_TRUE(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES) > 0 ||
                persisted_progress.load() > 0);
    ASSERT_OK(result.status);

    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    const auto blob_meta =
        std::find_if(metadata.blob_files.begin(), metadata.blob_files.end(),
                     [second_blob_file](const BlobMetaData& item) {
                       return item.blob_file_number == second_blob_file;
                     });
    ASSERT_NE(blob_meta, metadata.blob_files.end());
    EXPECT_EQ(blob_meta->total_blob_count, 2);
    EXPECT_EQ(blob_meta->garbage_blob_count, 1);
    EXPECT_EQ(Get("l"), value);
  }

  void RunCompactionResumeAfterDroppedNewerVersion() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = false;
    options.compaction_verify_record_count = true;
    std::shared_ptr<Statistics> statistics = CreateDBStatistics();

    auto resume_cs = std::make_shared<ResumableCompactionService>(
        dbname_, options, statistics,
        ResumableCompactionService::TestScenario::kCancelThenResume,
        /*output_file_key_interval=*/1, /*merge_resumed_output=*/true);
    resume_cs->SetCancelAtKey("k", kMaxSequenceNumber);
    resume_cs->SetWorkerOptionOverride("compaction_verify_record_count",
                                       "true");
    options.compaction_service = resume_cs;

    DestroyAndReopen(options);
    const std::string value(100, 'v');
    ASSERT_OK(Put("j", value));
    ASSERT_OK(Put("k", value));
    ASSERT_OK(Flush());

    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(Put("k", value));
    ASSERT_OK(Put("l", value));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a",
                               "kk"));
    ASSERT_OK(Flush());
    AssertDroppedNewerVersionScenarioPointInputTotal();

    std::atomic<size_t> persisted_progress{0};
    std::atomic<uint64_t> verified_input_records{0};
    resume_cs->CaptureVerifiedInputRecords(&verified_input_records);
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionJob::PersistSubcompactionProgress:Complete",
        [&](void*) { ++persisted_progress; });

    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    const Status status = db_->CompactRange(compact_options, nullptr, nullptr);
    db_->ReleaseSnapshot(snapshot);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    EXPECT_OK(status);
    CompactionServiceResult result;
    resume_cs->GetResult(&result);
    AssertDroppedNewerVersionRecordCounts(result,
                                          verified_input_records.load());
    EXPECT_EQ(Get("l"), value);
    EXPECT_TRUE(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES) > 0 ||
                persisted_progress.load() > 0);
    ASSERT_OK(result.status);
  }

  void RunCompactionCancelTest(
      ResumableCompactionService::TestScenario scenario) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    std::shared_ptr<Statistics> statistics = CreateDBStatistics();

    options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
    BlockBasedTableOptions table_options;
    table_options.verify_compression = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    auto resume_cs = std::make_shared<ResumableCompactionService>(
        dbname_, options, statistics, scenario);
    options.compaction_service = resume_cs;

    DestroyAndReopen(options);

    GenerateTestData();

    ASSERT_OK(statistics->Reset());

    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    Status s = db_->CompactRange(cro, nullptr, nullptr);
    ASSERT_OK(s);

    VerifyTestData();

    s = db_->VerifyChecksum();
    ASSERT_OK(s);

    s = db_->VerifyFileChecksums(ReadOptions());
    ASSERT_OK(s);

    CompactionServiceResult result;
    resume_cs->GetResult(&result);
    ASSERT_OK(result.status);
    ASSERT_TRUE(result.stats.is_manual_compaction);
    ASSERT_TRUE(result.stats.is_remote_compaction);
    ASSERT_GT(result.output_files.size(), 0);

    uint64_t resumed_bytes =
        statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES);
    if (scenario ==
        ResumableCompactionService::TestScenario::kCancelThenResume) {
      // When resuming compaction, some bytes should be resumed from previous
      // progress
      ASSERT_GT(resumed_bytes, 0);
    } else if (scenario == ResumableCompactionService::TestScenario::
                               kCancelThenFreshStart) {
      // When starting fresh (ignoring existing progress), no bytes should be
      // resumed
      ASSERT_EQ(resumed_bytes, 0);
    } else {  // kMultipleCancelToggleResumption
      // Phase 2 ran without resumption (fresh start), so Phase 3 has no
      // progress to resume from. It processes all keys again from scratch.
      ASSERT_EQ(resumed_bytes, 0);
    }
  }

  void GenerateTestData() {
    for (int i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(Put(Key(i), "value"));
      ASSERT_OK(Flush());
      if (i % 2 == 0) {
        ASSERT_OK(Delete(Key(i)));
        ASSERT_OK(Flush());
      }
    }
  }

  void VerifyTestData() {
    for (int i = 0; i < kNumKeys; ++i) {
      if (i % 2 == 0) {
        ASSERT_EQ("NOT_FOUND", Get((Key(i))));
      } else {
        ASSERT_EQ("value", Get((Key(i))));
      }
    }
  }

 private:
  static constexpr int kNumKeys = 10;
};

TEST_F(ResumableCompactionServiceTest, CompactionCancelThenResume) {
  RunCompactionCancelTest(
      ResumableCompactionService::TestScenario::kCancelThenResume);
}

TEST_F(ResumableCompactionServiceTest, CompactionCancelThenFreshStart) {
  RunCompactionCancelTest(
      ResumableCompactionService::TestScenario::kCancelThenFreshStart);
}

TEST_F(ResumableCompactionServiceTest,
       CompactionMultipleCancelToggleResumption) {
  RunCompactionCancelTest(ResumableCompactionService::TestScenario::
                              kMultipleCancelToggleResumption);
}

TEST_F(ResumableCompactionServiceTest, BlobCompactionCancelThenResume) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_starting_level = 1;
  // Keep a blob file open across the forced SST boundary. The resumable path
  // must close it before checkpointing the SST that references it.
  options.blob_file_size = 1024;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  auto resume_cs = std::make_shared<ResumableCompactionService>(
      dbname_, options, statistics,
      ResumableCompactionService::TestScenario::kCancelThenResume);
  options.compaction_service = resume_cs;

  std::vector<uint64_t> allocated_blob_numbers;
  std::set<uint64_t> deleted_blob_numbers;
  size_t phase1_allocation_count = 0;
  constexpr uint64_t orphan_blob_number = 999999999;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::NewBlobFileNumber", [&](void* arg) {
        allocated_blob_numbers.push_back(*static_cast<uint64_t*>(arg));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::CleanupPhysicalCompactionOutputFiles::BlobFile",
      [&](void* arg) {
        deleted_blob_numbers.insert(*static_cast<uint64_t*>(arg));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "ResumableCompactionService::AfterCancelledCompaction", [&](void* arg) {
        phase1_allocation_count = allocated_blob_numbers.size();
        const auto* output_path = static_cast<std::string*>(arg);
        std::unique_ptr<WritableFile> orphan;
        ASSERT_OK(env_->NewWritableFile(
            BlobFileName(*output_path, orphan_blob_number), &orphan,
            EnvOptions()));
        ASSERT_OK(orphan->Append("uncheckpointed"));
        ASSERT_OK(orphan->Close());
      });

  DestroyAndReopen(options);
  GenerateTestData();
  ASSERT_OK(statistics->Reset());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  VerifyTestData();
  ASSERT_OK(db_->VerifyChecksum());
  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));

  CompactionServiceInput input;
  resume_cs->GetInput(&input);
  CompactionServiceResult result;
  resume_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_FALSE(result.blob_file_additions.empty());
  ASSERT_GT(phase1_allocation_count, 0);
  ASSERT_LT(phase1_allocation_count, allocated_blob_numbers.size());
  ASSERT_EQ(deleted_blob_numbers.count(orphan_blob_number), 1);

  std::set<uint64_t> checkpointed_phase1_numbers(
      allocated_blob_numbers.begin(),
      allocated_blob_numbers.begin() + phase1_allocation_count);
  for (const uint64_t deleted : deleted_blob_numbers) {
    checkpointed_phase1_numbers.erase(deleted);
  }
  for (size_t i = phase1_allocation_count; i < allocated_blob_numbers.size();
       ++i) {
    EXPECT_EQ(checkpointed_phase1_numbers.count(allocated_blob_numbers[i]), 0);
  }

  for (size_t i = 0; i < result.blob_file_additions.size(); ++i) {
    const uint64_t file_number =
        result.blob_file_additions[i].GetBlobFileNumber();
    EXPECT_EQ(file_number, input.blob_file_number_start + i);
    EXPECT_LT(file_number, input.blob_file_number_limit);
  }
  ASSERT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);
}

TEST_F(ResumableCompactionServiceTest,
       BuilderRequestedRotationCrashResumeDoesNotDuplicateBlob) {
  const std::string boundary_key = "key4";
  auto factory_state = std::make_shared<RetryCloseTableFactoryState>();
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_starting_level = 1;
  options.blob_file_size = 1 << 20;
  options.table_factory =
      std::make_shared<RetryCloseTableFactory>(boundary_key, factory_state);
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  auto resume_cs = std::make_shared<ResumableCompactionService>(
      dbname_, options, statistics,
      ResumableCompactionService::TestScenario::kCancelThenResume,
      /*output_file_key_interval=*/2);
  options.compaction_service = resume_cs;

  size_t suppressed_progress_updates = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::FinishCompactionOutputFile:"
      "PostPrepareRetryShouldUpdateProgress",
      [&](void* arg) {
        EXPECT_FALSE(*static_cast<bool*>(arg));
        ++suppressed_progress_updates;
        resume_cs->SetCanceled(true);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);
  const std::string value(4096, 'a');
  for (const char* const key : {"key1", "key2", "key3", "key4", "key5"}) {
    ASSERT_OK(Put(key, value));
  }
  ASSERT_OK(Flush());
  ASSERT_TRUE(GetBlobFileNumbers().empty());
  factory_state->enabled = true;

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {input_sst.db_path + "/" + input_sst.name}, 1));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(factory_state->cut_requests.load(), 1);
  ASSERT_EQ(suppressed_progress_updates, 1);
  EXPECT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);
  for (const char* const key : {"key1", "key2", "key3", "key4", "key5"}) {
    EXPECT_EQ(Get(key), value);
  }

  db_->GetColumnFamilyMetaData(&metadata);
  size_t physical_record_count = 0;
  size_t boundary_record_count = 0;
  uint64_t garbage_blob_count = 0;
  for (const BlobMetaData& blob_meta : metadata.blob_files) {
    std::vector<BlobLogRecord> records;
    ASSERT_OK(ReadBlobFileRecords(blob_meta.blob_file_number,
                                  blob_meta.total_blob_count, &records));
    physical_record_count += records.size();
    boundary_record_count += static_cast<size_t>(std::count_if(
        records.begin(), records.end(), [&](const BlobLogRecord& record) {
          return record.key == boundary_key;
        }));
    garbage_blob_count += blob_meta.garbage_blob_count;
  }
  EXPECT_EQ(physical_record_count, 5);
  EXPECT_EQ(boundary_record_count, 1);
  EXPECT_EQ(garbage_blob_count, 0);
}

TEST_F(ResumableCompactionServiceTest,
       BlobCompactionResumeDoesNotDoubleCountBoundaryKey) {
  for (const bool inline_boundary_key : {false, true}) {
    SCOPED_TRACE(inline_boundary_key);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.min_blob_size = 1024;
    options.blob_file_starting_level = 0;
    options.blob_file_size = 1 << 20;
    std::unique_ptr<CompactionFilter> compaction_filter(
        new ChangeBlobValueCompactionFilter());
    options.compaction_filter = compaction_filter.get();
    std::shared_ptr<Statistics> statistics = CreateDBStatistics();

    auto resume_cs = std::make_shared<ResumableCompactionService>(
        dbname_, options, statistics,
        ResumableCompactionService::TestScenario::kCancelThenResume);
    resume_cs->SetCancelAtKey(inline_boundary_key ? "key4" : "key3",
                              kMaxSequenceNumber);
    options.compaction_service = resume_cs;

    DestroyAndReopen(options);
    const std::string value(4096, 'a');
    for (const char* const key : {"key1", "key2", "key3"}) {
      ASSERT_OK(Put(key, value));
    }
    const std::string inline_value("inline");
    if (inline_boundary_key) {
      ASSERT_OK(Put("key4", inline_value));
    }
    ASSERT_OK(Flush());
    const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
    ASSERT_EQ(original_blob_files.size(), 1);
    const uint64_t original_blob_file = original_blob_files.front();
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), 1);
    const SstFileMetaData& input_sst = metadata.levels[0].files.front();
    ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                                {input_sst.db_path + "/" + input_sst.name}, 1));
    EXPECT_EQ(resume_cs->GetUseLocalInstallationCount(), 0);
    EXPECT_EQ(resume_cs->GetFinalCompactionServiceJobStatus(),
              CompactionServiceJobStatus::kSuccess);
    CompactionServiceInput input;
    resume_cs->GetInput(&input);
    ASSERT_FALSE(input.input_blob_files.empty());

    CompactionServiceResult result;
    resume_cs->GetResult(&result);
    ASSERT_OK(result.status);
    const std::vector<BlobFileGarbage>::const_iterator garbage = std::find_if(
        result.blob_file_garbages.begin(), result.blob_file_garbages.end(),
        [original_blob_file](const BlobFileGarbage& item) {
          return item.GetBlobFileNumber() == original_blob_file;
        });
    ASSERT_NE(garbage, result.blob_file_garbages.end());
    EXPECT_EQ(garbage->GetGarbageBlobCount(), 2);
    std::string changed_value = value;
    changed_value[0] = 'z';
    EXPECT_EQ(Get("key1"), changed_value);
    EXPECT_EQ(Get("key2"), "NOT_FOUND");
    EXPECT_EQ(Get("key3"), value);
    if (inline_boundary_key) {
      EXPECT_EQ(Get("key4"), inline_value);
    }
    EXPECT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);
    Close();
  }
}

TEST_F(ResumableCompactionServiceTest,
       BlobCompactionResumeAfterDroppedNewerVersionWithRecordVerification) {
  RunBlobCompactionResumeAfterDroppedNewerVersion(
      /*verify_record_count=*/true, /*merge_resumed_output=*/true);
}

TEST_F(ResumableCompactionServiceTest,
       BlobCompactionResumeAfterDroppedNewerVersionWithLinkedBlobFile) {
  RunBlobCompactionResumeAfterDroppedNewerVersion(
      /*verify_record_count=*/false, /*merge_resumed_output=*/false);
}

TEST_F(ResumableCompactionServiceTest,
       BlobCompactionResumeAfterDroppedNewerVersionWithMergedOutput) {
  RunBlobCompactionResumeAfterDroppedNewerVersion(
      /*verify_record_count=*/false, /*merge_resumed_output=*/true);
}

TEST_F(ResumableCompactionServiceTest,
       CompactionResumeAfterDroppedNewerVersionWithRecordVerification) {
  RunCompactionResumeAfterDroppedNewerVersion();
}

TEST_F(ResumableCompactionServiceTest,
       MultiVersionOverwritePersistsProgressAtMultipleBoundaries) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  auto resume_cs = std::make_shared<ResumableCompactionService>(
      dbname_, options, statistics,
      ResumableCompactionService::TestScenario::kCancelThenResume,
      /*output_file_key_interval=*/1);
  options.compaction_service = resume_cs;

  DestroyAndReopen(options);
  for (const char* const key : {"a", "b", "c", "d", "e"}) {
    ASSERT_OK(Put(key, "old"));
  }
  ASSERT_OK(Flush());
  for (const char* const key : {"a", "b", "c", "d", "e"}) {
    ASSERT_OK(Put(key, "new"));
  }
  ASSERT_OK(Flush());

  std::atomic<size_t> persisted_progress{0};
  std::atomic<size_t> phase_one_persisted_progress{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PersistSubcompactionProgress:Complete",
      [&](void*) { ++persisted_progress; });
  SyncPoint::GetInstance()->SetCallBack(
      "ResumableCompactionService::AfterCancelledCompaction",
      [&](void*) { phase_one_persisted_progress = persisted_progress.load(); });

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  EXPECT_GE(phase_one_persisted_progress.load(), 2);
  EXPECT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);
  for (const char* const key : {"a", "b", "c", "d", "e"}) {
    EXPECT_EQ(Get(key), "new");
  }
}

TEST_F(ResumableCompactionServiceTest,
       RepeatedCancelKeepsOutputRecordCountConsistent) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compaction_verify_record_count = true;
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  auto resume_cs = std::make_shared<ResumableCompactionService>(
      dbname_, options, statistics,
      ResumableCompactionService::TestScenario::kCancelThenResume,
      /*output_file_key_interval=*/3);
  resume_cs->SetCancelAtKey(Key(20), kMaxSequenceNumber);
  resume_cs->SetSecondCancelAtKey(Key(40));
  resume_cs->SetWorkerOptionOverride("compaction_verify_record_count", "true");
  options.compaction_service = resume_cs;

  DestroyAndReopen(options);
  constexpr int kTestNumKeys = 60;
  for (int i = 0; i < kTestNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "old"));
  }
  ASSERT_OK(Flush());
  const Snapshot* snapshot = db_->GetSnapshot();
  for (int i = 0; i < kTestNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "new"));
  }
  ASSERT_OK(Flush());

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  const Status s = db_->CompactRange(compact_options, nullptr, nullptr);
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(s);

  CompactionServiceResult result;
  resume_cs->GetResult(&result);
  ASSERT_OK(result.status);
  uint64_t entries_in_output_files = 0;
  for (const auto& file : result.output_files) {
    entries_in_output_files += file.table_properties.num_entries -
                               file.table_properties.num_range_deletions;
  }
  // The snapshot keeps both versions of every key.
  EXPECT_EQ(entries_in_output_files, 2 * kTestNumKeys);
  EXPECT_EQ(result.stats.num_output_records, entries_in_output_files);
  EXPECT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);
  for (int i = 0; i < kTestNumKeys; ++i) {
    EXPECT_EQ(Get(Key(i)), "new");
  }
}

TEST_F(ResumableCompactionServiceTest,
       BlobPerKeyPlacementDoesNotPersistProgress) {
  const auto run_compaction = [&](bool supports_per_key_placement,
                                  size_t* persisted_progress_count) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    options.blob_file_starting_level = 0;
    std::shared_ptr<Statistics> statistics = CreateDBStatistics();

    auto resume_cs = std::make_shared<ResumableCompactionService>(
        dbname_, options, statistics,
        ResumableCompactionService::TestScenario::kCancelThenResume,
        /*output_file_key_interval=*/1);
    resume_cs->SetCancelAtKey("c", kMaxSequenceNumber);
    options.compaction_service = resume_cs;

    DestroyAndReopen(options);
    for (const char* const key : {"a", "b", "c", "d"}) {
      ASSERT_OK(Put(key, std::string(100, key[0])));
    }
    ASSERT_OK(Flush());

    std::atomic<size_t> persisted_progress{0};
    if (supports_per_key_placement) {
      SyncPoint::GetInstance()->SetCallBack(
          "CompactionJob::ShouldUpdateSubcompactionProgress:"
          "SupportsPerKeyPlacement",
          [&](void* arg) { *static_cast<bool*>(arg) = true; });
    }
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionJob::PersistSubcompactionProgress:Complete",
        [&](void*) { ++persisted_progress; });
    SyncPoint::GetInstance()->EnableProcessing();

    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

    for (const char* const key : {"a", "b", "c", "d"}) {
      EXPECT_EQ(Get(key), std::string(100, key[0]));
    }
    // Wait clears callbacks before the resumed phase, so this counter measures
    // phase-one checkpointing. The control proves that phase one reaches an
    // otherwise eligible checkpoint boundary.
    *persisted_progress_count = persisted_progress.load();
  };

  size_t control_persisted_progress = 0;
  run_compaction(/*supports_per_key_placement=*/false,
                 &control_persisted_progress);
  EXPECT_GT(control_persisted_progress, 0);

  size_t per_key_placement_persisted_progress = 0;
  run_compaction(/*supports_per_key_placement=*/true,
                 &per_key_placement_persisted_progress);
  EXPECT_EQ(per_key_placement_persisted_progress, 0);
}

TEST_F(ResumableCompactionServiceTest,
       BlobCompactionResumePreservesFilteredBoundaryInflow) {
  for (const BoundaryFilterDecision decision :
       {BoundaryFilterDecision::kChangeValue, BoundaryFilterDecision::kRemove,
        BoundaryFilterDecision::kChangeWideColumnEntity}) {
    SCOPED_TRACE(static_cast<int>(decision));
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.min_blob_size = 1024;
    options.blob_file_starting_level = 0;
    options.blob_file_size = 1 << 20;
    std::unique_ptr<CompactionFilter> compaction_filter(
        new BoundaryBlobValueCompactionFilter(decision));
    options.compaction_filter = compaction_filter.get();
    std::shared_ptr<Statistics> statistics = CreateDBStatistics();

    auto resume_cs = std::make_shared<ResumableCompactionService>(
        dbname_, options, statistics,
        ResumableCompactionService::TestScenario::kCancelThenResume);
    resume_cs->SetCancelAtKey(
        decision == BoundaryFilterDecision::kRemove ? "key4" : "key3",
        kMaxSequenceNumber);
    options.compaction_service = resume_cs;

    DestroyAndReopen(options);
    const std::string value(4096, 'a');
    ASSERT_OK(Put("key1", value));
    ASSERT_OK(Put("key2", value));
    if (decision == BoundaryFilterDecision::kChangeWideColumnEntity) {
      const std::string attribute_value(4096, 'b');
      const WideColumns columns{{kDefaultWideColumnName, value},
                                {"attribute", attribute_value}};
      ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                               "key3", columns));
    } else {
      ASSERT_OK(Put("key3", value));
    }
    ASSERT_OK(Put("key4", value));
    ASSERT_OK(Flush());
    const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
    ASSERT_EQ(original_blob_files.size(), 1);
    const uint64_t original_blob_file = original_blob_files.front();

    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), 1);
    const SstFileMetaData& input_sst = metadata.levels[0].files.front();
    ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                                {input_sst.db_path + "/" + input_sst.name}, 1));

    CompactionServiceResult result;
    resume_cs->GetResult(&result);
    ASSERT_OK(result.status);
    const std::vector<BlobFileGarbage>::const_iterator garbage = std::find_if(
        result.blob_file_garbages.begin(), result.blob_file_garbages.end(),
        [original_blob_file](const BlobFileGarbage& item) {
          return item.GetBlobFileNumber() == original_blob_file;
        });
    ASSERT_NE(garbage, result.blob_file_garbages.end());
    EXPECT_EQ(
        garbage->GetGarbageBlobCount(),
        decision == BoundaryFilterDecision::kChangeWideColumnEntity ? 2 : 1);
    EXPECT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);

    if (decision == BoundaryFilterDecision::kChangeValue) {
      std::string changed_value = value;
      changed_value[0] = 'z';
      EXPECT_EQ(Get("key3"), changed_value);
    } else if (decision == BoundaryFilterDecision::kRemove) {
      EXPECT_EQ(Get("key3"), "NOT_FOUND");
    } else {
      PinnableWideColumns columns;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               "key3", &columns));
      const WideColumns expected{{kDefaultWideColumnName, "changed_default"},
                                 {"attribute", "changed_attribute"}};
      EXPECT_EQ(columns.columns(), expected);
    }
    Close();
  }
}

TEST_F(ResumableCompactionServiceTest,
       BlobReservationCoversNaturalOutputCheckpoints) {
  constexpr int kTestNumKeys = 1024;
  constexpr size_t kValueSize = 4096;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = false;
  options.min_blob_size = 0;
  options.blob_file_starting_level = 1;
  options.blob_file_size = 64 << 20;
  options.target_file_size_base = 512;
  options.target_file_size_multiplier = 1;
  options.compression = kNoCompression;
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  auto resume_cs = std::make_shared<ResumableCompactionService>(
      dbname_, options, statistics,
      ResumableCompactionService::TestScenario::kCancelThenResume,
      /*output_file_key_interval=*/0);
  resume_cs->SetWorkerOptionOverride("enable_blob_files", "true");
  options.compaction_service = resume_cs;

  DestroyAndReopen(options);
  for (int i = 0; i < kTestNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), std::string(kValueSize, 'a' + i % 26)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions({{"enable_blob_files", "true"}}));

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();
  CompactionOptions compaction_options;
  compaction_options.output_file_size_limit = options.target_file_size_base;
  ASSERT_OK(db_->CompactFiles(compaction_options,
                              {input_sst.db_path + "/" + input_sst.name}, 1));

  CompactionServiceInput input;
  resume_cs->GetInput(&input);
  CompactionServiceResult result;
  resume_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_GE(result.output_files.size(), 3);
  ASSERT_EQ(result.blob_file_additions.size(), result.output_files.size());
  uint64_t output_blob_count = 0;
  for (const BlobFileAddition& addition : result.blob_file_additions) {
    output_blob_count += addition.GetTotalBlobCount();
  }
  EXPECT_EQ(output_blob_count, kTestNumKeys);
  EXPECT_GE(input.blob_file_number_limit - input.blob_file_number_start,
            result.blob_file_additions.size());
  EXPECT_EQ(resume_cs->GetUseLocalInstallationCount(), 0);
  EXPECT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);
  for (int i = 0; i < kTestNumKeys; ++i) {
    ASSERT_EQ(Get(Key(i)), std::string(kValueSize, 'a' + i % 26));
  }
}

TEST_F(ResumableCompactionServiceTest,
       BlobReservationAddsSizeRotationsAndCheckpointTails) {
  constexpr int kTestNumKeys = 20;
  constexpr size_t kBlobFileSize = 64 << 10;
  constexpr size_t kSmallValueSize = 1024;
  constexpr size_t kKeysPerOutputFile = 2;
  constexpr uint64_t kExpectedOutputFileCount =
      kTestNumKeys / kKeysPerOutputFile;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = false;
  options.min_blob_size = 0;
  options.blob_file_starting_level = 1;
  options.blob_file_size = kBlobFileSize;
  options.compression = kNoCompression;
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  auto resume_cs = std::make_shared<ResumableCompactionService>(
      dbname_, options, statistics,
      ResumableCompactionService::TestScenario::kCancelThenResume,
      kKeysPerOutputFile);
  resume_cs->SetWorkerOptionOverride("enable_blob_files", "true");
  options.compaction_service = resume_cs;

  DestroyAndReopen(options);
  uint64_t raw_value_bytes = 0;
  for (int i = 0; i < kTestNumKeys; ++i) {
    const size_t value_size = i % 2 == 0 ? kBlobFileSize : kSmallValueSize;
    raw_value_bytes += value_size;
    ASSERT_OK(Put(Key(i), std::string(value_size, static_cast<char>('a' + i))));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions({{"enable_blob_files", "true"}}));

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const SstFileMetaData& input_sst = metadata.levels[0].files.front();
  CompactionOptions compaction_options;
  compaction_options.output_file_size_limit =
      input_sst.size / kExpectedOutputFileCount + 1;
  ASSERT_OK(db_->CompactFiles(compaction_options,
                              {input_sst.db_path + "/" + input_sst.name}, 1));

  CompactionServiceInput input;
  resume_cs->GetInput(&input);
  CompactionServiceResult result;
  resume_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_EQ(result.output_files.size(), kExpectedOutputFileCount);
  const uint64_t expected_data_files =
      raw_value_bytes / kBlobFileSize +
      (raw_value_bytes % kBlobFileSize != 0 ? 1 : 0);
  EXPECT_GT(result.blob_file_additions.size(),
            std::max(expected_data_files, kExpectedOutputFileCount));
  EXPECT_GE(input.blob_file_number_limit - input.blob_file_number_start,
            expected_data_files + kExpectedOutputFileCount + 2);
  EXPECT_EQ(resume_cs->GetUseLocalInstallationCount(), 0);
  EXPECT_GT(statistics->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES), 0);
  for (int i = 0; i < kTestNumKeys; ++i) {
    const size_t value_size = i % 2 == 0 ? kBlobFileSize : kSmallValueSize;
    ASSERT_EQ(Get(Key(i)), std::string(value_size, static_cast<char>('a' + i)));
  }
}

TEST_F(ResumableCompactionServiceTest,
       BlobCompactionResumeRangeExhaustionFallsBackLocally) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_starting_level = 1;
  options.blob_file_size = 1024;
  std::shared_ptr<Statistics> statistics = CreateDBStatistics();

  auto resume_cs = std::make_shared<ResumableCompactionService>(
      dbname_, options, statistics,
      ResumableCompactionService::TestScenario::kResumeRangeExhaustion);
  options.compaction_service = resume_cs;

  size_t phase1_allocation_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionServiceCompactionJob::NewBlobFileNumber",
      [&](void*) { ++phase1_allocation_count; });
  SyncPoint::GetInstance()->SetCallBack(
      "ResumableCompactionService::BeforeFinalCompaction", [&](void* arg) {
        ASSERT_GT(phase1_allocation_count, 1);
        auto* serialized_input = static_cast<std::string*>(arg);
        CompactionServiceInput input;
        ASSERT_OK(CompactionServiceInput::Read(*serialized_input, &input));
        input.blob_file_number_limit =
            input.blob_file_number_start + phase1_allocation_count - 1;
        serialized_input->clear();
        ASSERT_OK(input.Write(serialized_input));
      });

  DestroyAndReopen(options);
  GenerateTestData();

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  const Status status = db_->CompactRange(cro, nullptr, nullptr);

  ASSERT_OK(status);
  ASSERT_EQ(resume_cs->GetUseLocalInstallationCount(), 0);
  VerifyTestData();
}

class ResumableCompactionKeyTypeTest : public CompactionServiceTest {
 public:
  explicit ResumableCompactionKeyTypeTest() : CompactionServiceTest() {}

 protected:
  void SetupResumableCompactionService(
      Options& options, const std::string& cancel_at_key = "",
      SequenceNumber cancel_at_seqno = kMaxSequenceNumber) {
    options.disable_auto_compactions = true;
    statistics_ = CreateDBStatistics();

    resume_cs_ = std::make_shared<ResumableCompactionService>(
        dbname_, options, statistics_,
        ResumableCompactionService::TestScenario::kCancelThenResume);

    if (!cancel_at_key.empty()) {
      resume_cs_->SetCancelAtKey(cancel_at_key, cancel_at_seqno);
    }

    options.compaction_service = resume_cs_;
    DestroyAndReopen(options);
  }

  void ResetStatistics() { ASSERT_OK(statistics_->Reset()); }

  void VerifyResumeBytes() {
    uint64_t resumed_bytes =
        statistics_->getTickerCount(REMOTE_COMPACT_RESUMED_BYTES);
    ASSERT_GT(resumed_bytes, 0);
  }

 private:
  std::shared_ptr<ResumableCompactionService> resume_cs_;
  std::shared_ptr<Statistics> statistics_;
};

// Cancel compaction right before processing key "c" to test resumption at a
//  deletion at the non-bottom level. When resumed, compaction will continue
//  from this deletion.
TEST_F(ResumableCompactionKeyTypeTest,
       CancelAndResumeWithDeleteAtNonBottomLevel) {
  Options options = CurrentOptions();

  SetupResumableCompactionService(options, "c");

  ASSERT_OK(Put("c", "old_value"));
  ASSERT_OK(Put("c_placeholder", "placeholder"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(options.num_levels - 1);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Delete("c"));
  ASSERT_OK(Flush());

  std::vector<std::string> input_files;
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  for (const auto& file : cf_meta.levels[0].files) {
    input_files.push_back(file.name);
  }

  ASSERT_EQ(input_files.size(), 2);

  ResetStatistics();

  CompactionOptions compact_options;
  ASSERT_OK(
      db_->CompactFiles(compact_options, input_files, 1 /* output_level*/));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");
  ASSERT_EQ(Get("c"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "val4");

  VerifyResumeBytes();
}

// Cancel compaction right before processing key "c" to test resumption at a
//  deletion at the ottom level. When resumed, compaction will continue from
//  the last saved progress point before the delete.
TEST_F(ResumableCompactionKeyTypeTest, CancelAndResumeWithDeleteAtBottomLevel) {
  Options options = CurrentOptions();

  SetupResumableCompactionService(options, "c");

  ASSERT_OK(Put("c", "old_value"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(options.num_levels - 1);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  ResetStatistics();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");
  ASSERT_EQ(Get("c"), "NOT_FOUND");
  ASSERT_EQ(Get("c", snapshot), "old_value");
  ASSERT_EQ(Get("d"), "val4");
  db_->ReleaseSnapshot(snapshot);

  VerifyResumeBytes();
}

// Cancel compaction right before processing key "c" to test resumption at a
// merge operand. When resumed, compaction will continue from the last saved
// progress point before the merge operand.
TEST_F(ResumableCompactionKeyTypeTest, CancelAndResumeWithMerge) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();

  SetupResumableCompactionService(options, "c");

  ASSERT_OK(Put("c", "old_value"));
  ASSERT_OK(Put("c_placeholder", "placeholder"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(options.num_levels - 1);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Merge("c", "new_value"));
  ASSERT_OK(Flush());

  std::vector<std::string> input_files;
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  for (const auto& file : cf_meta.levels[0].files) {
    input_files.push_back(file.name);
  }

  ASSERT_EQ(input_files.size(), 2);

  ResetStatistics();

  CompactionOptions compact_options;
  ASSERT_OK(
      db_->CompactFiles(compact_options, input_files, 1 /* output_level*/));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");
  ASSERT_EQ(Get("c"), "old_value,new_value");
  ASSERT_EQ(Get("d"), "val4");

  VerifyResumeBytes();
}

// Cancel compaction right before processing key "c" to test resumption at a
// single delete. When resumed, compaction will continue from the last saved
// progress point before the single delete.
TEST_F(ResumableCompactionKeyTypeTest, CancelAndResumeWithSingleDelete) {
  Options options = CurrentOptions();

  SetupResumableCompactionService(options, "c");

  ASSERT_OK(Put("c", "old_value"));
  ASSERT_OK(Put("c_placeholder", "placeholder"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(options.num_levels - 1);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  ASSERT_OK(SingleDelete("c"));
  ASSERT_OK(Flush());

  std::vector<std::string> input_files;
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  for (const auto& file : cf_meta.levels[0].files) {
    input_files.push_back(file.name);
  }

  ASSERT_EQ(input_files.size(), 2);

  ResetStatistics();

  CompactionOptions compact_options;
  ASSERT_OK(
      db_->CompactFiles(compact_options, input_files, 1 /* output_level*/));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");
  ASSERT_EQ(Get("c"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "val4");

  VerifyResumeBytes();
}

// Cancel compaction right before processing key "c" to test resumption at a
// range delete. When resumed, compaction will continue from the last saved
// progress point before the range delete.
TEST_F(ResumableCompactionKeyTypeTest, CancelAndResumeWithRangeDelete) {
  Options options = CurrentOptions();

  SetupResumableCompactionService(options, "c");

  ASSERT_OK(Put("c", "old_value"));
  ASSERT_OK(Put("c_placeholder", "placeholder"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(options.num_levels - 1);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "c", "c_"));
  ASSERT_OK(Flush());

  std::vector<std::string> input_files;
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  for (const auto& file : cf_meta.levels[0].files) {
    input_files.push_back(file.name);
  }

  ASSERT_EQ(input_files.size(), 2);

  ResetStatistics();

  CompactionOptions compact_options;
  ASSERT_OK(
      db_->CompactFiles(compact_options, input_files, 1 /* output_level*/));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");
  ASSERT_EQ(Get("c"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "val4");

  VerifyResumeBytes();
}

// Test resumption when a key has multiple versions spanning across file
// boundaries (i.e., the same key exists in multiple SST files).
//
// Scenario:
//   File 1 largest key: key "b"
//   File 2 smallest key: key "c" with seqno=4 (older version)
//   File 3 largest key: key "c" with seqno=5 (newer version)
//
// Cancel compaction right before processing the older version of key "c".
// Upon resumption, compaction continues from the saved progress point "b" and
// correctly processes both versions
TEST_F(ResumableCompactionKeyTypeTest,
       CancelAndResumeWithKeySpanningFileBoundaries) {
  Options options = CurrentOptions();

  // Set up cancellation at the older version of the key which will have
  // sequence number zero-ed out
  SetupResumableCompactionService(options, "c" /*cancel_at_key*/, 0 /*seqno*/);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("c", "old_value"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("c", "new_value"));
  ASSERT_OK(Flush());

  ResetStatistics();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");
  ASSERT_EQ(Get("c"), "new_value");
  ASSERT_EQ(Get("c", snapshot), "old_value");
  ASSERT_EQ(Get("d"), "val4");
  db_->ReleaseSnapshot(snapshot);

  VerifyResumeBytes();
}

// Cancel compaction right before processing key "c" to test resumption at a
// wide column. When resumed, compaction will continue
// from the wide column.
TEST_F(ResumableCompactionKeyTypeTest, CancelAndResumeWithWideColumn) {
  Options options = CurrentOptions();

  SetupResumableCompactionService(options, "c" /*cancel_at_key*/);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  WideColumns columns{{"col1", "value1"}, {"col2", "value2"}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "c", columns));
  ASSERT_OK(Flush());

  ResetStatistics();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");

  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "c", &result));
  WideColumns expected{{"col1", "value1"}, {"col2", "value2"}};
  ASSERT_EQ(result.columns(), expected);

  ASSERT_EQ(Get("d"), "val4");

  VerifyResumeBytes();
}

// Cancel compaction right before processing key "c" to test resumption at a
// timed put. When resumed, compaction will continue
// from the timed put.
TEST_F(ResumableCompactionKeyTypeTest, CancelAndResumeWithTimedPut) {
  Options options = CurrentOptions();
  options.preclude_last_level_data_seconds = 86400;  // Enable TimedPut feature
  options.preserve_internal_time_seconds = 86400;    // Preserve write time

  SetupResumableCompactionService(options, "c" /*cancel_at_key*/);

  ASSERT_OK(Put("c", "old_value"));
  ASSERT_OK(Put("c_placeholder", "placeholder"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(options.num_levels - 1);

  ASSERT_OK(Put("a", "val1"));
  ASSERT_OK(Put("b", "val2"));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  // Use TimedPut for key "c" with current write time
  uint64_t write_time = env_->NowMicros() / 1000000;
  ASSERT_OK(TimedPut("c", "val3", write_time /*write_unix_time*/));
  ASSERT_OK(Put("d", "val4"));
  ASSERT_OK(Flush());

  std::vector<std::string> input_files;
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  for (const auto& file : cf_meta.levels[0].files) {
    input_files.push_back(file.name);
  }

  ASSERT_EQ(input_files.size(), 2);

  ResetStatistics();

  CompactionOptions compact_options;
  ASSERT_OK(
      db_->CompactFiles(compact_options, input_files, 1 /* output_level*/));

  ASSERT_EQ(Get("a"), "val1");
  ASSERT_EQ(Get("b"), "val2");
  ASSERT_EQ(Get("c"), "val3");
  ASSERT_EQ(Get("d"), "val4");

  VerifyResumeBytes();
}

// Covers the DB session temporary directory lifecycle when remote compaction
// opts in to using it.
class SessionTmpDirTest : public DBTestBase {
 public:
  SessionTmpDirTest()
      : DBTestBase("session_tmp_dir_test",
                   /*env_do_fsync=*/true) {}

 protected:
  // Opens with a CompactionService opted in to the session temporary directory.
  Options OptionsWithCompactionService() {
    Options options = CurrentOptions();
    options.env = env_;
    options.use_session_tmp_dir_for_remote_compaction = true;
    worker_statistics_ = CreateDBStatistics();
    compaction_service_ = std::make_shared<MyTestCompactionService>(
        dbname_, options, worker_statistics_,
        std::vector<std::shared_ptr<EventListener>>{},
        std::vector<std::shared_ptr<TablePropertiesCollectorFactory>>{});
    options.compaction_service = compaction_service_;
    return options;
  }

  std::string SessionTmpPath() { return SessionTmpDir(dbname_); }

  // Creates and closes a DB session before planting temporary output for the
  // next session to delete.
  void CreateDbAndQuiesce(const Options& options) {
    DestroyAndReopen(options);
    ASSERT_TRUE(Exists(SessionTmpPath()));
    Close();
  }

  // Creates `parent/name/` holding one file, mimicking a leaked job directory.
  void PlantDir(const std::string& parent, const std::string& name) {
    SpecialEnv* const env = env_;
    if (env == nullptr) {
      ADD_FAILURE() << "Test environment is not initialized";
      return;
    }
    ASSERT_OK(env->CreateDirIfMissing(parent));
    const std::string dir = parent + "/" + name;
    ASSERT_OK(env->CreateDirIfMissing(dir));
    ASSERT_OK(WriteStringToFile(env, "leftover", dir + "/000123.sst"));
  }

  bool Exists(const std::string& path) {
    return env_ != nullptr && env_->FileExists(path).ok();
  }

  // Reopens after planting stale output. Cleanup completes before DB::Open()
  // schedules compactions or returns.
  void ReopenAndDeleteStaleOutput(const Options& options) { Reopen(options); }

  std::shared_ptr<Statistics> worker_statistics_;
  std::shared_ptr<CompactionService> compaction_service_;
};

TEST_F(SessionTmpDirTest, ResolvesJobDirectory) {
  const std::string job_dir_name = "SESSION01_4294967296";
  const std::string legacy_output_path = dbname_ + "/" + job_dir_name;

  ASSERT_EQ(RemoteCompactionJobDir(dbname_, false, job_dir_name),
            legacy_output_path);
  ASSERT_EQ(RemoteCompactionJobDir(dbname_, false, legacy_output_path),
            legacy_output_path);
  ASSERT_EQ(RemoteCompactionJobDir(dbname_, true, job_dir_name),
            SessionTmpPath() + "/" + job_dir_name);
}

TEST_F(SessionTmpDirTest, DeletesDirsFromPreviousSessions) {
  Options options = OptionsWithCompactionService();
  CreateDbAndQuiesce(options);

  // Two leaked job directories, one of them with a nested subdirectory to prove
  // the removal is recursive.
  PlantDir(SessionTmpPath(), "SESSIONFROMDEADHOST01_4294967296");
  PlantDir(SessionTmpPath() + "/SESSIONFROMDEADHOST01_4294967296", "nested");
  PlantDir(SessionTmpPath(), "ANOTHERDEADSESSION02_8589934592");

  ReopenAndDeleteStaleOutput(options);

  ASSERT_TRUE(Exists(SessionTmpPath()));
  ASSERT_FALSE(Exists(SessionTmpPath() + "/SESSIONFROMDEADHOST01_4294967296"));
  ASSERT_FALSE(Exists(SessionTmpPath() + "/ANOTHERDEADSESSION02_8589934592"));
}

TEST_F(SessionTmpDirTest, IgnoresDirsOutsideSessionTmpDir) {
  Options options = OptionsWithCompactionService();
  CreateDbAndQuiesce(options);

  // A CompactionService that stages output outside the configured root owns
  // cleaning it up; the DB must not touch it.
  const std::string elsewhere = "service_owned_staging_dir";
  PlantDir(dbname_, elsewhere);
  PlantDir(SessionTmpPath(), "SESSIONFROMDEADHOST01_4294967296");

  ReopenAndDeleteStaleOutput(options);

  ASSERT_TRUE(Exists(dbname_ + "/" + elsewhere));
  ASSERT_FALSE(Exists(SessionTmpPath() + "/SESSIONFROMDEADHOST01_4294967296"));

  // Nothing in RocksDB owns this directory, so nothing else will remove it --
  // including the DestroyDB that the next test's setup runs against the shared
  // per-thread DB path.
  ASSERT_OK(DestroyDir(env_, dbname_ + "/" + elsewhere));
}

TEST_F(SessionTmpDirTest, DisabledOptionSkipsCleanup) {
  if (Exists(SessionTmpPath())) {
    ASSERT_OK(DestroyDir(env_, SessionTmpPath()));
  }

  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  ASSERT_FALSE(Exists(SessionTmpPath()));

  PlantDir(SessionTmpPath(), "SESSIONFROMDEADHOST01_4294967296");

  Reopen(options);
  Close();

  ASSERT_TRUE(Exists(SessionTmpPath() + "/SESSIONFROMDEADHOST01_4294967296"));
  ASSERT_OK(DestroyDir(env_, SessionTmpPath()));
}

TEST_F(SessionTmpDirTest, EnabledSessionTmpDirIsCreated) {
  Options options = CurrentOptions();
  options.use_session_tmp_dir_for_remote_compaction = true;
  DestroyAndReopen(options);
  ASSERT_TRUE(Exists(SessionTmpPath()));
}

TEST_F(SessionTmpDirTest, DestroyDBRemovesSessionTmpDir) {
  Options options = OptionsWithCompactionService();
  CreateDbAndQuiesce(options);
  PlantDir(SessionTmpPath(), "SESSIONFROMDEADHOST01_4294967296");

  // DestroyDB's final DeleteDir(dbname) is non-recursive, so it can only remove
  // the DB directory if the session temporary directory is gone first.
  ASSERT_OK(DestroyDB(dbname_, options));
  ASSERT_FALSE(Exists(dbname_));
}

TEST_F(SessionTmpDirTest, CompactionUsesSessionTmpDir) {
  Options options = OptionsWithCompactionService();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 20; i += 2) {
    ASSERT_OK(Put(Key(i), "value_new" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  auto* my_cs = static_cast_with_check<MyTestCompactionService>(
      compaction_service_.get());
  ASSERT_GE(my_cs->GetCompactionNum(), 1);
  CompactionServiceResult result;
  my_cs->GetResult(&result);
  ASSERT_OK(result.status);
  ASSERT_EQ(result.output_path,
            RemoteCompactionJobDir(dbname_, true,
                                   my_cs->GetLastOutputDirectoryName()));

  for (int i = 0; i < 20; i++) {
    ASSERT_EQ(Get(Key(i)), (i % 2 ? "value" : "value_new") + std::to_string(i));
  }
}

// A bottommost file written before preserve_internal_time_seconds was enabled
// has an empty seqno->time mapping. Once the DB-wide mapping ages it out of the
// preserve window, the file is marked for a kBottommostFiles compaction. The
// remote compaction of it must zero its sequence numbers as a local one does,
// or the file is marked again after every rewrite.
TEST_F(CompactionServiceTest, BottommostFileWithEmptySeqnoTimeMapping) {
  auto mock_clock = std::make_shared<MockSystemClock>(env_->GetSystemClock());
  mock_clock->SetCurrentTime(10000000);
  mock_clock->InstallTimedWaitFixCallback();
  std::unique_ptr<Env> mock_env(new CompositeEnvWrapper(env_, mock_clock));
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::StartPeriodicTaskScheduler:Init", [&](void* arg) {
        static_cast<PeriodicTaskScheduler*>(arg)->TEST_OverrideTimer(
            mock_clock.get());
      });

  // Counts executed compactions: with a BOTTOM thread pool, the picker runs
  // twice for one forwarded bottommost compaction.
  class BottommostCompactionCounter : public EventListener {
   public:
    void OnCompactionCompleted(DB* /*db*/,
                               const CompactionJobInfo& ci) override {
      if (ci.compaction_reason == CompactionReason::kBottommostFiles) {
        count.fetch_add(1);
      }
    }
    std::atomic<int> count{0};
  };
  auto bottommost_compactions = std::make_shared<BottommostCompactionCounter>();

  const int kNumLevels = 7;
  Options options = CurrentOptions();
  options.env = mock_env.get();
  options.num_levels = kNumLevels;
  options.level0_file_num_compaction_trigger = 4;
  options.listeners.push_back(bottommost_compactions);
  std::shared_ptr<Statistics> compactor_statistics = CreateDBStatistics();
  auto compaction_service = std::make_shared<MyTestCompactionService>(
      dbname_, options, compactor_statistics, remote_listeners,
      remote_table_properties_collector_factories);
  options.compaction_service = compaction_service;
  // The DB uses mock_env and mock_clock, so close it before they go out of
  // scope, also when an assertion fails.
  Defer close_db([this]() {
    Close();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  DestroyAndReopen(options);

  ASSERT_OK(Put("keeplow", "v"));
  const Snapshot* protect = db_->GetSnapshot();
  ASSERT_OK(Put("aged", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(kNumLevels - 1);
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 1));

  ASSERT_OK(db_->SetOptions({{"preserve_internal_time_seconds", "10000"}}));
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put("hot", "v" + std::to_string(i)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock->MockSleepForSeconds(200); });
  }

  ASSERT_EQ(bottommost_compactions->count.load(), 0);
  const int compactions_before = compaction_service->GetCompactionNum();
  db_->ReleaseSnapshot(protect);

  // A looping file would keep this wait from returning.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(bottommost_compactions->count.load(), 1);
  ASSERT_EQ(compaction_service->GetCompactionNum(), compactions_before + 1);
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  for (const auto& f : files) {
    if (f.level == kNumLevels - 1) {
      ASSERT_EQ(f.largest_seqno, 0U);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
