//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/lsm_edit.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "file/file_io_listener.h"
#include "port/stack_trace.h"
#include "rocksdb/file_system.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/wide_columns.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/table_reader.h"
#include "test_util/testharness.h"
#include "util/defer.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

namespace {
struct DirectorySyncState {
  std::string cf_path;
  std::atomic<uint64_t> step{0};
  std::atomic<uint64_t> created{0};
  std::atomic<uint64_t> synced{0};
};

class TrackingDirectory : public FSDirectoryWrapper {
 public:
  TrackingDirectory(std::unique_ptr<FSDirectory> target,
                    std::shared_ptr<DirectorySyncState> state)
      : FSDirectoryWrapper(std::move(target)), state_(std::move(state)) {}
  IOStatus Fsync(const IOOptions& options, IODebugContext* debug) override {
    IOStatus status = FSDirectoryWrapper::Fsync(options, debug);
    if (status.ok()) {
      state_->synced = ++state_->step;
    }
    return status;
  }
  IOStatus FsyncWithDirOptions(
      const IOOptions& options, IODebugContext* debug,
      const DirFsyncOptions& directory_options) override {
    IOStatus status = FSDirectoryWrapper::FsyncWithDirOptions(
        options, debug, directory_options);
    if (status.ok()) {
      state_->synced = ++state_->step;
    }
    return status;
  }

 private:
  std::shared_ptr<DirectorySyncState> state_;
};

class DirectorySyncFileSystem : public FileSystemWrapper {
 public:
  const char* Name() const override { return "DirectorySyncFileSystem"; }
  DirectorySyncFileSystem(std::shared_ptr<FileSystem> target,
                          std::shared_ptr<DirectorySyncState> state)
      : FileSystemWrapper(std::move(target)), state_(std::move(state)) {}
  IOStatus NewWritableFile(const std::string& path, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* debug) override {
    IOStatus status = target()->NewWritableFile(path, options, result, debug);
    if (status.ok() &&
        path.compare(0, state_->cf_path.size(), state_->cf_path) == 0 &&
        path.size() >= 4 && path.compare(path.size() - 4, 4, ".sst") == 0) {
      state_->created = ++state_->step;
    }
    return status;
  }
  IOStatus NewDirectory(const std::string& path, const IOOptions& options,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* debug) override {
    IOStatus status = target()->NewDirectory(path, options, result, debug);
    std::string normalized = path;
    while (normalized.size() > 1 && normalized.back() == '/') {
      normalized.pop_back();
    }
    if (status.ok() && normalized == state_->cf_path) {
      *result = std::make_unique<TrackingDirectory>(std::move(*result), state_);
    }
    return status;
  }

 private:
  std::shared_ptr<DirectorySyncState> state_;
};

class FreshPropertiesTableReader : public TableReader {
 public:
  explicit FreshPropertiesTableReader(std::unique_ptr<TableReader> reader)
      : reader_(std::move(reader)) {}

  InternalIterator* NewIterator(const ReadOptions& ro,
                                const SliceTransform* prefix, Arena* arena,
                                bool skip_filters, TableReaderCaller caller,
                                size_t readahead,
                                bool allow_unprepared_value) override {
    return reader_->NewIterator(ro, prefix, arena, skip_filters, caller,
                                readahead, allow_unprepared_value);
  }
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& ro) override {
    return reader_->NewRangeTombstoneIterator(ro);
  }
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      SequenceNumber seq, const Slice* timestamp) override {
    return reader_->NewRangeTombstoneIterator(seq, timestamp);
  }
  Status Get(const ReadOptions& ro, const Slice& key, GetContext* context,
             const SliceTransform* prefix, bool skip_filters) override {
    return reader_->Get(ro, key, context, prefix, skip_filters);
  }
  uint64_t ApproximateOffsetOf(const ReadOptions& ro, const Slice& key,
                               TableReaderCaller caller) override {
    return reader_->ApproximateOffsetOf(ro, key, caller);
  }
  uint64_t ApproximateSize(const ReadOptions& ro, const Slice& start,
                           const Slice& end,
                           TableReaderCaller caller) override {
    return reader_->ApproximateSize(ro, start, end, caller);
  }
  size_t ApproximateMemoryUsage() const override {
    return reader_->ApproximateMemoryUsage();
  }
  void SetupForCompaction() override { reader_->SetupForCompaction(); }
  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    std::shared_ptr<TableProperties> properties =
        std::make_shared<TableProperties>(*reader_->GetTableProperties());
    properties->newest_key_time = 12345;
    properties->oldest_key_time = 12300;
    properties->oldest_ancestor_time = 12299;
    return properties;
  }

 private:
  std::unique_ptr<TableReader> reader_;
};

class FreshPropertiesTableFactory : public BlockBasedTableFactory {
 public:
  using BlockBasedTableFactory::NewTableReader;
  Status NewTableReader(const ReadOptions& ro,
                        const TableReaderOptions& options,
                        std::unique_ptr<RandomAccessFileReader>&& file,
                        uint64_t file_size,
                        std::unique_ptr<TableReader>* reader,
                        bool prefetch) const override {
    Status status = BlockBasedTableFactory::NewTableReader(
        ro, options, std::move(file), file_size, reader, prefetch);
    if (status.ok()) {
      *reader =
          std::make_unique<FreshPropertiesTableReader>(std::move(*reader));
    }
    return status;
  }
  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<FreshPropertiesTableFactory>(*this);
  }
};

class NotificationAllocationListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileWriteFinish(const FileOperationInfo& info) override {
    Check(info);
  }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    Check(info);
  }
  void OnFileSyncFinish(const FileOperationInfo& info) override { Check(info); }
  void OnIOError(const IOErrorInfo& info) override {
    if (info.io_status.IsMemoryLimit()) {
      EXPECT_TRUE(info.file_path.empty());
      ++memory_errors;
      CheckSnapshot();
    } else if (observe &&
               info.file_path.find("MANIFEST") != std::string::npos) {
      CheckSnapshot();
    }
  }
  void OnExternalFileIngested(DB*, const ExternalFileIngestionInfo&) override {
    if (observe) {
      EXPECT_EQ(1, memory_errors);
    }
  }
  DB* db = nullptr;
  bool observe = false;
  int memory_errors = 0;
  int snapshots = 0;
  int manifest_callbacks = 0;

 private:
  void Check(const FileOperationInfo& info) {
    if (observe && info.path.find("MANIFEST") != std::string::npos) {
      ++manifest_callbacks;
      CheckSnapshot();
    }
  }
  void CheckSnapshot() {
    ASSERT_NE(nullptr, db);
    const Snapshot* snapshot = db->GetSnapshot();
    ASSERT_NE(nullptr, snapshot);
    ++snapshots;
    db->ReleaseSnapshot(snapshot);
  }
};

class SnapshotOnFileIOListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if (kind == 0 && info.path.find("MANIFEST") != std::string::npos) {
      Capture(info.path, info.start_ts.time_since_epoch().count());
    }
  }
  void OnFileReadFinish(const FileOperationInfo& info) override {
    if (kind == 1 && info.path.find(".sst") != std::string::npos) {
      Capture(info.path, info.start_ts.time_since_epoch().count());
    }
  }
  void OnIOError(const IOErrorInfo& info) override {
    if (kind == 2 && info.file_path.find("MANIFEST") != std::string::npos) {
      Capture(info.file_path, 1);
    }
  }
  void OnExternalFileIngested(DB*, const ExternalFileIngestionInfo&) override {
    if (kind != 2) {
      EXPECT_EQ(1, calls.load());
    }
  }

  std::atomic<DB*> db{nullptr};
  std::atomic<bool> armed{false};
  std::atomic<int> calls{0};
  int kind = 0;
  bool snapshot_only = false;
  bool retain_snapshot = false;
  const Snapshot* retained_snapshot = nullptr;
  std::string observed;
  std::string path;
  std::thread::id callback_thread;

 private:
  void Capture(const std::string& event_path, int64_t event_time) {
    DB* source = db.load();
    if (source == nullptr || !armed.exchange(false)) {
      return;
    }
    EXPECT_GT(event_time, 0);
    path = event_path;
    callback_thread = std::this_thread::get_id();
    const Snapshot* snapshot = source->GetSnapshot();
    EXPECT_NE(snapshot, nullptr);
    if (snapshot) {
      if (!snapshot_only) {
        ReadOptions read;
        read.snapshot = snapshot;
        Status status = source->Get(read, "m1", &observed);
        if (status.IsNotFound()) {
          observed = "NOT_FOUND";
        } else {
          EXPECT_OK(status);
        }
      }
      if (retain_snapshot) {
        retained_snapshot = snapshot;
      } else {
        source->ReleaseSnapshot(snapshot);
      }
    }
    ++calls;
  }
};

class PrepareOnFileIOListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if (info.path.find("MANIFEST") != std::string::npos &&
        armed.exchange(false)) {
      action();
    }
  }
  void OnExternalFileIngested(DB*, const ExternalFileIngestionInfo&) override {
    ++completed;
  }

  std::atomic<bool> armed{false};
  std::atomic<int> completed{0};
  std::function<void()> action;
};

class SnapshotOnBackgroundErrorListener : public EventListener {
 public:
  void OnBackgroundError(BackgroundErrorReason reason, Status* error) override {
    if (!armed.exchange(false)) {
      return;
    }
    EXPECT_EQ(BackgroundErrorReason::kManifestWrite, reason);
    EXPECT_EQ(Status::kIOError, error->code());
    ReadOptions read;
    std::shared_ptr<const Snapshot> timestamped_snapshot;
    if (timestamped) {
      auto result = db->CreateTimestampedSnapshot(kMaxSequenceNumber, 1);
      EXPECT_OK(result.first);
      timestamped_snapshot = std::move(result.second);
      read.snapshot = timestamped_snapshot.get();
    } else {
      read.snapshot = db->GetSnapshot();
    }
    EXPECT_NE(nullptr, read.snapshot);
    if (read.snapshot) {
      EXPECT_OK(db->Get(read, "m1", &observed));
      if (!timestamped) {
        db->ReleaseSnapshot(read.snapshot);
      }
    }
    ++calls;
  }

  DBImpl* db = nullptr;
  bool timestamped = false;
  std::atomic<bool> armed{false};
  std::atomic<int> calls{0};
  std::string observed;
};

class OrderedFileIOListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if (!armed.load() || info.path.find("MANIFEST") == std::string::npos ||
        !info.status.ok() ||
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            info.start_ts.time_since_epoch())
                .count() < arm_time.load()) {
      return;
    }
    const Snapshot* snapshot = db->GetSnapshot();
    EXPECT_NE(nullptr, snapshot);
    if (snapshot) {
      db->ReleaseSnapshot(snapshot);
    }
    std::lock_guard<std::mutex> lock(mutex);
    if (events.empty() || events.back() != "io") {
      events.push_back("io");
    }
  }
  void OnIOError(const IOErrorInfo& info) override {
    if (armed.load() && info.file_path.find("MANIFEST") != std::string::npos) {
      ++errors;
    }
  }
  void OnExternalFileIngested(DB*,
                              const ExternalFileIngestionInfo& info) override {
    if (armed.load()) {
      std::lock_guard<std::mutex> lock(mutex);
      events.push_back(info.external_file_path);
    }
  }
  std::vector<std::string> Events() {
    std::lock_guard<std::mutex> lock(mutex);
    return events;
  }

  DB* db = nullptr;
  std::atomic<bool> armed{false};
  std::atomic<int64_t> arm_time{0};
  std::atomic<int> errors{0};

 private:
  std::mutex mutex;
  std::vector<std::string> events;
};

class EditOnFileIOListener : public EventListener {
 public:
  bool ShouldBeNotifiedOnFileIO() override { return true; }
  void OnFileReadFinish(const FileOperationInfo& info) override {
    if (kind == 0 && info.path.find(".sst") != std::string::npos) {
      Apply();
    }
  }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if ((kind == 1 || kind == 3) &&
        info.path.find("MANIFEST") != std::string::npos) {
      Apply();
    }
  }
  void OnIOError(const IOErrorInfo& info) override {
    if (kind == 2 && info.file_path.find("MANIFEST") != std::string::npos) {
      Apply();
    }
  }

  DB* db = nullptr;
  LsmEdit edit;
  Status result;
  std::atomic<bool> armed{false};
  std::atomic<int> calls{0};
  int kind = 0;

 private:
  void Apply() {
    if (armed.exchange(false)) {
      result = db->ApplyLsmEdit(LsmEditOptions(), {edit});
      ++calls;
    }
  }
};

class NestedLeaseEditListener : public EventListener {
 public:
  enum class CallbackKind { kManifest, kStall, kFlushBegin, kIngested };
  bool ShouldBeNotifiedOnFileIO() override {
    return callback_kind != CallbackKind::kStall && notify_file_io;
  }
  void OnFileFlushFinish(const FileOperationInfo& info) override {
    if (callback_kind == CallbackKind::kManifest &&
        info.path.find("MANIFEST") != std::string::npos) {
      Apply();
    }
  }
  void OnStallConditionsChanged(const WriteStallInfo& info) override {
    if (callback_kind == CallbackKind::kStall &&
        info.condition.prev == WriteStallCondition::kNormal &&
        info.condition.cur == WriteStallCondition::kDelayed) {
      Apply();
    }
  }
  void OnFlushBegin(DB* db, const FlushJobInfo&) override {
    if (callback_kind == CallbackKind::kFlushBegin && db == source) {
      Apply();
    }
  }
  void OnExternalFileIngested(DB* db,
                              const ExternalFileIngestionInfo&) override {
    ++ingestions;
    if (callback_kind == CallbackKind::kIngested && db == source) {
      Apply();
    }
  }

  DB* source = nullptr;
  DB* target = nullptr;
  LsmEdit edit;
  LsmEditOptions edit_options;
  CallbackKind callback_kind = CallbackKind::kManifest;
  bool notify_file_io = true;
  std::atomic<bool> armed{false};
  std::atomic<int> calls{0};
  std::atomic<int> ingestions{0};
  std::optional<Status> result;
  std::function<void()> before_apply;
  std::function<void()> after_apply;

 private:
  void Apply() {
    if (armed.exchange(false)) {
      if (before_apply) {
        before_apply();
      }
      result = target->ApplyLsmEdit(edit_options, {edit});
      ++calls;
      if (after_apply) {
        after_apply();
      }
    }
  }
};
}  // namespace

class LsmEditTest : public DBTestBase {
 public:
  LsmEditTest() : DBTestBase("lsm_edit_test", /*env_do_fsync=*/true) {
    sst_files_dir_ = dbname_ + "_sst_files/";
    EXPECT_OK(DestroyDir(env_, sst_files_dir_));
    EXPECT_OK(env_->CreateDir(sst_files_dir_));
  }

  ~LsmEditTest() override {
    EXPECT_OK(DestroyDir(env_, sst_files_dir_));
    for (const auto& name : source_dbnames_) {
      EXPECT_OK(DestroyDB(name, CurrentOptions()));
    }
  }

  // Writes `kvs` to a standalone SST file and returns an LsmEditFile that
  // installs it at `level`.
  LsmEditFile MakeFile(const std::string& name, int level,
                       const std::map<std::string, std::string>& kvs) {
    LsmEditFile file;
    file.path = sst_files_dir_ + name;
    file.level = level;
    SstFileWriter writer(EnvOptions(), CurrentOptions());
    EXPECT_OK(writer.Open(file.path));
    for (const auto& [key, value] : kvs) {
      EXPECT_OK(writer.Put(key, value));
    }
    EXPECT_OK(writer.Finish());
    return file;
  }

  LsmEditFile MakeRangeDeletionFile(const std::string& name, int level,
                                    const Slice& start, const Slice& limit) {
    LsmEditFile file;
    file.path = sst_files_dir_ + name;
    file.level = level;
    SstFileWriter writer(EnvOptions(), CurrentOptions());
    EXPECT_OK(writer.Open(file.path));
    EXPECT_OK(writer.DeleteRange(start, limit));
    EXPECT_OK(writer.Finish());
    return file;
  }

  LsmEditFile MakePointFile(const std::string& name, int level, ValueType type,
                            const std::string& key, const std::string& value) {
    LsmEditFile file;
    file.path = sst_files_dir_ + name;
    file.level = level;
    SstFileWriter writer(EnvOptions(), CurrentOptions());
    EXPECT_OK(writer.Open(file.path));
    if (type == kTypeMerge) {
      EXPECT_OK(writer.Merge(key, value));
    } else if (type == kTypeDeletion) {
      EXPECT_OK(writer.Delete(key));
    } else {
      EXPECT_OK(writer.Put(key, value));
    }
    EXPECT_OK(writer.Finish());
    return file;
  }

  void ExpectVisibleEntries(
      const std::vector<std::pair<std::string, std::string>>& expected) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    std::vector<std::pair<std::string, std::string>> actual;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      actual.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(expected, actual);
    actual.clear();
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      actual.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ((std::vector<std::pair<std::string, std::string>>(
                  expected.rbegin(), expected.rend())),
              actual);
    for (size_t i = 0; i < expected.size(); ++i) {
      EXPECT_EQ(expected[i].second, Get(expected[i].first));
      iter->Seek(expected[i].first);
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(expected[i].second, iter->value().ToString());
      iter->Next();
      if (i + 1 < expected.size()) {
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i + 1].first, iter->key().ToString());
        iter->Prev();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i].first, iter->key().ToString());
      } else {
        EXPECT_FALSE(iter->Valid());
      }
      iter->SeekForPrev(expected[i].first);
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(expected[i].second, iter->value().ToString());
      iter->Prev();
      if (i > 0) {
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i - 1].first, iter->key().ToString());
        iter->Next();
        ASSERT_TRUE(iter->Valid());
        EXPECT_EQ(expected[i].first, iter->key().ToString());
      } else {
        EXPECT_FALSE(iter->Valid());
      }
      ASSERT_OK(iter->status());
    }
  }

  void CheckPointTypesDirectionChanges(int range_boundary);

  LsmEditFile MakeDbFile(const std::string& name, int level,
                         const std::map<std::string, std::string>& kvs,
                         bool compact = false) {
    const std::string source_name = dbname_ + "_source_" + name;
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    EXPECT_OK(DestroyDB(source_name, options));
    source_dbnames_.push_back(source_name);
    std::unique_ptr<DB> source;
    EXPECT_OK(DB::Open(options, source_name, &source));
    if (!source) {
      return {};
    }
    for (const auto& [key, value] : kvs) {
      EXPECT_OK(source->Put(WriteOptions(), key, value));
    }
    EXPECT_OK(source->Flush(FlushOptions()));
    if (compact) {
      CompactRangeOptions compact_options;
      compact_options.change_level = true;
      compact_options.target_level = options.num_levels - 1;
      compact_options.bottommost_level_compaction =
          BottommostLevelCompaction::kForce;
      EXPECT_OK(source->CompactRange(compact_options, nullptr, nullptr));
      TablePropertiesCollection properties;
      EXPECT_OK(source->GetPropertiesOfAllTables(&properties));
      for (const auto& [path, props] : properties) {
        EXPECT_EQ(0, props->key_largest_seqno);
      }
    }
    ColumnFamilyMetaData metadata;
    source->GetColumnFamilyMetaData(&metadata);
    LsmEditFile result;
    result.level = level;
    for (const auto& source_level : metadata.levels) {
      for (const auto& file : source_level.files) {
        EXPECT_TRUE(result.path.empty());
        result.path = file.db_path + file.name;
        if (!compact) {
          EXPECT_GT(file.largest_seqno, 0);
        }
      }
    }
    EXPECT_OK(source->Close());
    return result;
  }

  static LsmEdit MakeEdit(ColumnFamilyHandle* cfh, const Slice& start,
                          const Slice& limit,
                          std::vector<LsmEditFile> add_files) {
    LsmEdit edit;
    edit.column_family = cfh;
    edit.range = RangeOpt(start, limit);
    edit.add_files = std::move(add_files);
    return edit;
  }

  // "<level>:<file count>" for each non-empty level of the default column
  // family, e.g. "0:2 1:1".
  static std::string ShapeOf(DB* db) {
    ColumnFamilyMetaData cf_meta;
    db->GetColumnFamilyMetaData(&cf_meta);
    std::string shape;
    for (const auto& level : cf_meta.levels) {
      if (level.files.empty()) {
        continue;
      }
      if (!shape.empty()) {
        shape += " ";
      }
      shape += std::to_string(level.level) + ":" +
               std::to_string(level.files.size());
    }
    return shape;
  }

  // File numbers currently in the default column family, per level. Used to
  // show that an edit left existing files alone rather than rewriting them.
  std::vector<std::vector<uint64_t>> FileNumbersPerLevel() {
    ColumnFamilyMetaData cf_meta;
    db_->GetColumnFamilyMetaData(&cf_meta);
    std::vector<std::vector<uint64_t>> result;
    for (const auto& level : cf_meta.levels) {
      result.emplace_back();
      for (const auto& file : level.files) {
        result.back().push_back(file.file_number);
      }
    }
    return result;
  }

  static Status RunBounded(const std::function<Status()>& operation) {
    std::future<Status> result = std::async(std::launch::async, operation);
    return AwaitBounded(result);
  }

  static Status AwaitBounded(std::future<Status>& result) {
    if (result.wait_for(std::chrono::seconds(20)) !=
        std::future_status::ready) {
      ADD_FAILURE() << "LSM edit did not complete within the deadline";
      std::_Exit(1);
    }
    return result.get();
  }

  void ExerciseLiveDeletionReservation(int level, int files,
                                       bool manifest_failure);

  static void WaitBounded(std::condition_variable& cv,
                          std::unique_lock<std::mutex>& lock,
                          const std::function<bool()>& ready) {
    if (!cv.wait_for(lock, std::chrono::seconds(10), ready)) {
      ADD_FAILURE() << "LSM edit synchronization did not complete";
      std::_Exit(1);
    }
  }

 protected:
  std::string sst_files_dir_;
  std::vector<std::string> source_dbnames_;
};

// The MyRocks index-rebuild shape: install files into a key range that holds
// no data but is not aligned to the boundaries of the file covering it.
TEST_F(LsmEditTest, GraftIntoEmptyRangeUnderStraddlingFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // One bottom-level file spanning the whole key space, with a hole in the
  // middle where the edit will go.
  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
  ASSERT_EQ(1, NumTableFilesAtLevel(6));

  // The straddling file sits at L6, so the graft declares L5. Two levels of
  // the source shape are preserved as two levels here.
  std::vector<LsmEditFile> files;
  files.push_back(MakeDbFile("graft_l5", 5, {{"m1", "new_m1"}}));
  files.push_back(MakeDbFile("graft_l4", 4, {{"m2", "new_m2"}}));
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))}));

  EXPECT_EQ("old_a", Get("a1"));
  EXPECT_EQ("old_z", Get("z1"));
  EXPECT_EQ("new_m1", Get("m1"));
  EXPECT_EQ("new_m2", Get("m2"));
  EXPECT_EQ("0,0,0,0,1,1,1", FilesPerLevel());

  // The straddling file was retained untouched: no rewrite of live data was
  // needed to make the range file-aligned.
  const std::vector<std::vector<uint64_t>> after = FileNumbersPerLevel();
  EXPECT_EQ(before[6], after[6]);
}

TEST_F(LsmEditTest, RejectsRangeThatStillHoldsKeys) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("m5", "old_m5"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeDbFile("graft", 5, {{"m1", "new_m1"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
  EXPECT_EQ("old_m5", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, RejectsRangeTombstoneReachingIntoRange) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  // Covers the edit range without leaving any point key in it. Grafted data
  // carries its own (lower) sequence numbers, so this tombstone would silently
  // delete it.
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "y"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeDbFile("graft", 5, {{"m1", "new_m1"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
}

TEST_F(LsmEditTest, RejectsStraddlingFileWhenProbeDisabled) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  LsmEditOptions edit_options;
  edit_options.probe_straddling_files = false;
  const Status s = db_->ApplyLsmEdit(
      edit_options, {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              {MakeDbFile("graft", 5, {{"m1", "v"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("partially overlaps"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementDeletesStraddlingKeys) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  const SequenceNumber last_seqno = db_->GetLatestSequenceNumber();

  LsmEditOptions edit_options;
  edit_options.link_files = true;
  edit_options.probe_straddling_files = false;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 4, {{"m1", "new"}})})}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));

  std::vector<std::string> values;
  const auto statuses = db_->MultiGet(ReadOptions(), {"m1", "m5"}, &values);
  ASSERT_EQ(2, statuses.size());
  EXPECT_OK(statuses[0]);
  EXPECT_TRUE(statuses[1].IsNotFound());
  EXPECT_EQ("new", values[0]);
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    std::vector<std::string> entries;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      entries.push_back(iter->key().ToString() + ":" +
                        iter->value().ToString());
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ((std::vector<std::string>{"a1:left", "m1:new", "z1:right"}),
              entries);
  }

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.levels[4].files.size());
  ASSERT_EQ(1, metadata.levels[5].files.size());
  EXPECT_EQ(last_seqno + 1, metadata.levels[5].files[0].largest_seqno);
  EXPECT_EQ(last_seqno + 2, metadata.levels[4].files[0].largest_seqno);
  EXPECT_EQ(last_seqno + 2, db_->GetLatestSequenceNumber());

  Reopen(options);
  EXPECT_EQ(last_seqno + 2, db_->GetLatestSequenceNumber());
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
  ASSERT_OK(Put("m1", "later"));
  EXPECT_EQ("later", Get("m1"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementRelocatesAboveTombstone) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  LsmEditOptions edit_options;
  edit_options.allow_replacement_file_relocation = true;
  edit_options.probe_straddling_files = false;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.levels[4].files.size());
  ASSERT_EQ(1, metadata.levels[5].files.size());
  EXPECT_GT(metadata.levels[4].files[0].largest_seqno,
            metadata.levels[5].files[0].largest_seqno);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementRequiresRoomWithoutRelocation) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("allow_replacement_file_relocation"));
  EXPECT_EQ("old", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementRelocatesAboveFlushedStraddler) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  LsmEditOptions edit_options;
  edit_options.allow_replacement_file_relocation = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})}));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
  EXPECT_EQ(3, NumTableFilesAtLevel(0));
  Reopen(options);
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementProbesWhenTombstoneCannotFit) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 5, {{"m1", "new"}})})}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ(0, NumTableFilesAtLevel(0));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_EQ("new", Get("m1"));
}

TEST_F(LsmEditTest, ZeroSequenceRelocationPreservesRunOrderInLevelZero) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  LsmEditOptions edit_options;
  edit_options.allow_replacement_file_relocation = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options, {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              {MakeFile("upper.sst", 5, {{"m1", "upper"}}),
                               MakeFile("lower.sst", 6, {{"m1", "lower"}})})}));
  EXPECT_EQ("upper", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("upper", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m5"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementOverridesStraddlingTombstone) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "y"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("replacement.sst", 4, {{"m1", "new"}})})}));
  EXPECT_EQ("new", Get("m1"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("right", Get("z1"));
}

TEST_F(LsmEditTest, DbGeneratedZeroSequenceReplacementStillProbes) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeDbFile("compacted", 5, {{"m1", "new"}}, true)})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
  EXPECT_EQ("old", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MixedSequenceReplacementStillProbes) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "left"));
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("zero.sst", 5, {{"m1", "new"}}),
                 MakeDbFile("nonzero", 4, {{"m2", "new"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("range is not empty"));
  EXPECT_EQ("old", Get("m5"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m2"));
}

TEST_F(LsmEditTest, ZeroSequenceReplacementOrdersDeclaredLevels) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("upper.sst", 5, {{"m1", "upper"}}),
                 MakeFile("lower.sst", 6, {{"m1", "lower"}}),
                 MakeFile("l0_old.sst", 0, {{"m1", "l0_old"}}),
                 MakeFile("l0_new.sst", 0, {{"m1", "l0_new"}})})}));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  EXPECT_GT(metadata.levels[5].files[0].largest_seqno,
            metadata.levels[6].files[0].largest_seqno);
  EXPECT_EQ("l0_new", Get("m1"));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ("l0_new", Get("m1"));
}

TEST_F(LsmEditTest, RejectsDeclaredLevelOccupiedByRetainedFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Put("z1", "old_z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  // The retained file's key range still covers L6 across the edit range, so
  // the grafted file cannot share that level even though the range is empty.
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeDbFile("graft", 6, {{"m1", "v"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("declares level 6"));
}

// A file the edit removes must not block a declared level.
TEST_F(LsmEditTest, DeclaredLevelReusesSpaceOfRemovedFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.add_files = {
      MakeFile("left.sst", 6, {{"a1", "left"}}),
      MakeFile("old.sst", 6, {{"m1", "old_m1"}, {"m9", "old_m9"}}),
      MakeFile("right.sst", 6, {{"z1", "right"}}),
  };
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(initial)}));
  ASSERT_EQ(3, NumTableFilesAtLevel(6));

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("graft.sst", 6, {{"m5", "new_m5"}})})}));

  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m9"));
  EXPECT_EQ("new_m5", Get("m5"));
  EXPECT_EQ("right", Get("z1"));
  EXPECT_EQ("0,0,0,0,0,0,3", FilesPerLevel());
}

TEST_F(LsmEditTest, ExplicitEditPreservesUnlistedAndStraddlingFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.range = RangeOpt("a", "zz");
  initial.add_files = {
      MakeFile("straddling.sst", 6, {{"a1", "left"}, {"z1", "right"}}),
      MakeFile("old_middle.sst", 5, {{"m1", "old_m1"}, {"m9", "old_m9"}}),
  };
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(initial)}));
  const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
  ASSERT_EQ(1, before[5].size());
  ASSERT_EQ(1, before[6].size());

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.range = RangeOpt("m", "n");
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({5, before[5][0]});
  edit.add_files.push_back(MakeFile("new_middle.sst", 5, {{"m5", "new_m5"}}));
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)}));

  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("NOT_FOUND", Get("m9"));
  EXPECT_EQ("new_m5", Get("m5"));
  EXPECT_EQ("right", Get("z1"));
  const std::vector<std::vector<uint64_t>> after = FileNumbersPerLevel();
  EXPECT_EQ(before[6], after[6]);
  ASSERT_EQ(1, after[5].size());
  EXPECT_NE(before[5][0], after[5][0]);
}

TEST_F(LsmEditTest, ExplicitDeletionOnly) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeFile("old.sst", 5, {{"m1", "old"}})})}));
  const uint64_t file_number = FileNumbersPerLevel()[5][0];

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.range = RangeOpt("m", "n");
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({5, file_number});
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)}));

  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ(0, NumTableFilesAtLevel(5));
}

TEST_F(LsmEditTest, RemoveOnlyReservationsProtectConcurrentPreparedFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.max_open_files = 20;
  options.max_manifest_file_size = uint64_t{1} << 30;
  options.avoid_unnecessary_blocking_io = false;
  for (bool rollback : {false, true}) {
    SCOPED_TRACE(rollback);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"incoming"}, options);
    ASSERT_OK(Put("a", "old"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    const auto before = FileNumbersPerLevel();
    LsmEdit removal;
    removal.column_family = handles_[0];
    removal.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    removal.delete_files.push_back({0, before[0].front()});
    if (rollback) {
      removal.delete_files.push_back({0, uint64_t{1} << 60});
    }
    const LsmEditFile incoming =
        MakeFile("concurrent_prepared.sst", 0, {{"m1", "new"}});
    const uint64_t removal_number = dbfull()->TEST_Current_Next_FileNo();
    std::mutex mutex;
    std::condition_variable cv;
    std::thread::id removal_thread;
    bool reserved = false;
    bool release = false;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLastJobPrepare:0", [&](void*) {
          std::unique_lock<std::mutex> lock(mutex);
          if (std::this_thread::get_id() == removal_thread) {
            reserved = true;
            cv.notify_all();
            WaitBounded(cv, lock, [&]() { return release; });
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    std::future<Status> result = std::async(std::launch::async, [&]() {
      {
        std::lock_guard<std::mutex> lock(mutex);
        removal_thread = std::this_thread::get_id();
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {removal});
    });
    {
      std::unique_lock<std::mutex> lock(mutex);
      WaitBounded(cv, lock, [&]() { return reserved; });
    }
    auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    cfd->table_cache()->get_cache().get()->SetCapacity(0);
    IngestExternalFileOptions ingest;
    ingest.move_files = true;
    std::unique_ptr<FileIngestionHandle> prepared;
    const Status preparation = db_->PrepareFileIngestion(
        handles_[1], {incoming.path}, ingest, &prepared);
    std::string staged;
    if (preparation.ok()) {
      const auto files = prepared->GetPreparedFiles();
      EXPECT_EQ(1, files.size());
      if (!files.empty()) {
        EXPECT_NE(removal_number, files.front().file_number);
        staged =
            files.front().directory + "/" + files.front().relative_filename;
        dbfull()->TEST_DeleteObsoleteFiles();
        EXPECT_OK(env_->FileExists(staged));
      }
    }
    {
      std::lock_guard<std::mutex> lock(mutex);
      release = true;
      cv.notify_all();
    }
    const Status removed = AwaitBounded(result);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(preparation);
    if (rollback) {
      EXPECT_TRUE(removed.IsInvalidArgument()) << removed.ToString();
    } else {
      ASSERT_OK(removed);
    }
    dbfull()->TEST_DeleteObsoleteFiles();
    ASSERT_OK(env_->FileExists(staged));
    ASSERT_OK(db_->CommitFileIngestionHandle(std::move(prepared)));
    EXPECT_EQ("new", Get(1, "m1"));
    EXPECT_EQ(rollback ? "old" : "NOT_FOUND", Get("a"));
    EXPECT_TRUE(env_->FileExists(incoming.path).IsNotFound());
    ReopenWithColumnFamilies({kDefaultColumnFamilyName, "incoming"}, options);
    EXPECT_EQ("new", Get(1, "m1"));
    EXPECT_EQ(rollback ? "old" : "NOT_FOUND", Get("a"));
  }
}

TEST_F(LsmEditTest, RejectsExplicitDeletionOutsideRangeAtomically) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "a", "b",
                {MakeFile("outside.sst", 5, {{"a1", "old"}})})}));
  const uint64_t file_number = FileNumbersPerLevel()[5][0];

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.range = RangeOpt("m", "n");
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({5, file_number});
  edit.add_files.push_back(MakeFile("new.sst", 6, {{"m1", "new"}}));
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("does not contain explicitly deleted file"));
  EXPECT_EQ("old", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, ReplaceWholeColumnFamily) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a1", "old_a"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b1", "old_b"));
  ASSERT_OK(Flush());
  ASSERT_EQ(2, NumTableFilesAtLevel(0));

  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.add_files.push_back(MakeFile("all.sst", 6, {{"c1", "new_c"}}));
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));

  EXPECT_EQ("NOT_FOUND", Get("a1"));
  EXPECT_EQ("NOT_FOUND", Get("b1"));
  EXPECT_EQ("new_c", Get("c1"));
  EXPECT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
}

TEST_F(LsmEditTest, AtomicAcrossColumnFamilies) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"other"}, options);

  std::vector<LsmEdit> edits;
  edits.push_back(MakeEdit(handles_[0], "m", "n",
                           {MakeFile("cf0.sst", 6, {{"m1", "v0"}})}));
  edits.push_back(MakeEdit(handles_[1], "m", "n",
                           {MakeFile("cf1.sst", 6, {{"m1", "v1"}})}));
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), edits));

  EXPECT_EQ("v0", Get(0, "m1"));
  EXPECT_EQ("v1", Get(1, "m1"));
}

// One bad edit leaves every column family untouched.
TEST_F(LsmEditTest, RollsBackAllColumnFamiliesOnFailure) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"other"}, options);

  // A file straddling the edit range with a key inside it, so this column
  // family's edit cannot be satisfied.
  ASSERT_OK(Put(1, "a1", "old_a"));
  ASSERT_OK(Put(1, "m5", "old_m5"));
  ASSERT_OK(Put(1, "z1", "old_z"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(6, 1);

  std::vector<LsmEdit> edits;
  edits.push_back(
      MakeEdit(handles_[0], "m", "n", {MakeFile("ok.sst", 6, {{"m1", "v0"}})}));
  edits.push_back(MakeEdit(handles_[1], "m", "n",
                           {MakeFile("bad.sst", 6, {{"m1", "v1"}})}));
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), edits);
  ASSERT_NOK(s);

  EXPECT_EQ("NOT_FOUND", Get(0, "m1"));
  EXPECT_EQ("NOT_FOUND", Get(1, "m1"));
  EXPECT_EQ("old_m5", Get(1, "m5"));
}

TEST_F(LsmEditTest, PreservesLevelZeroOrdering) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Two overlapping level 0 files: a shape file ingestion cannot express,
  // because it would have to assign them sequence numbers to disambiguate.
  std::vector<LsmEditFile> files;
  files.push_back(MakeFile("older.sst", 0, {{"m2", "older"}}));
  files.push_back(MakeFile("newer.sst", 0, {{"m1", "newer"}, {"m2", "newer"}}));
  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))}));

  EXPECT_EQ(2, NumTableFilesAtLevel(0));
  EXPECT_EQ("newer", Get("m2"));
}

TEST_F(LsmEditTest, RejectsOverlappingFilesAtSameLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  std::vector<LsmEditFile> files;
  files.push_back(MakeFile("one.sst", 6, {{"m1", "v"}, {"m5", "v"}}));
  files.push_back(MakeFile("two.sst", 6, {{"m3", "v"}}));
  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("overlap each other"));
}

TEST_F(LsmEditTest, RejectsOlderSequenceNumberInHigherLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::string older_file_path;
  std::string newer_file_path;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));
    assert(src != nullptr);
    DB& source = *src;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "older"));
    ASSERT_OK(source.Flush(FlushOptions()));
    ColumnFamilyMetaData src_meta;
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(1, src_meta.levels[0].files.size());
    older_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "newer"));
    ASSERT_OK(source.Flush(FlushOptions()));
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(2, src_meta.levels[0].files.size());
    newer_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;
    ASSERT_OK(source.Close());
  }

  LsmEditFile older_file;
  older_file.path = std::move(older_file_path);
  older_file.level = 5;
  LsmEditFile newer_file;
  newer_file.path = std::move(newer_file_path);
  newer_file.level = 6;
  LsmEditOptions edit_options;
  edit_options.link_files = true;
  const Status s = db_->ApplyLsmEdit(
      edit_options, {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              {std::move(older_file), std::move(newer_file)})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
}

TEST_F(LsmEditTest, RefreshRechecksZeroSequenceRunsAfterLsmEdit) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit initial = MakeEdit(
      db_->DefaultColumnFamily(), "m", "n",
      {MakeFile("refresh_old.sst", 6, {{"m1", "old"}, {"m2", "retained"}})});
  initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
  ReadOptions read_options;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ("old", iter->value().ToString());

  LsmEdit newer = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                           {MakeFile("refresh_new.sst", 5, {{"m1", "new"}})});
  newer.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {newer}));

  auto read_all = [&]() {
    std::vector<std::pair<std::string, std::string>> entries;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      entries.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    return entries;
  };
  const std::vector<std::pair<std::string, std::string>> old_entries = {
      {"m1", "old"}, {"m2", "retained"}};
  EXPECT_EQ(old_entries, read_all());

  const std::vector<std::pair<std::string, std::string>> new_entries = {
      {"m1", "new"}, {"m2", "retained"}};
  ASSERT_OK(iter->Refresh());
  EXPECT_EQ(new_entries, read_all());
  ASSERT_OK(iter->Refresh());
  EXPECT_EQ(new_entries, read_all());
}

TEST_F(LsmEditTest, RejectsOlderAddedFileAboveSurvivingFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("m1", "survivor"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeFile("older.sst", 5, {{"m1", "older"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), {std::move(edit)});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
  EXPECT_EQ("survivor", Get("m1"));
}

TEST_F(LsmEditTest, AllowsInterleavedSequenceRangesForDistinctKeys) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::string wide_file_path;
  std::string middle_file_path;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));
    assert(src != nullptr);
    DB& source = *src;

    ASSERT_OK(source.Put(WriteOptions(), "a1", "left"));
    ASSERT_OK(source.Put(WriteOptions(), "z1", "right"));
    ASSERT_OK(source.Flush(FlushOptions()));
    ColumnFamilyMetaData src_meta;
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(1, src_meta.levels[0].files.size());
    wide_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "middle"));
    ASSERT_OK(source.Flush(FlushOptions()));
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(2, src_meta.levels[0].files.size());
    middle_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;
    ASSERT_OK(source.Close());
  }

  LsmEditFile wide_file;
  wide_file.path = std::move(wide_file_path);
  wide_file.level = 5;
  LsmEditFile middle_file;
  middle_file.path = std::move(middle_file_path);
  middle_file.level = 6;
  LsmEditOptions edit_options;
  edit_options.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "a", "zz",
                {std::move(wide_file), std::move(middle_file)})}));
  EXPECT_EQ("left", Get("a1"));
  EXPECT_EQ("middle", Get("m1"));
  EXPECT_EQ("right", Get("z1"));
}

TEST_F(LsmEditTest, RejectsOlderRangeTombstoneInHigherLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::string tombstone_file_path;
  std::string point_file_path;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));
    assert(src != nullptr);
    DB& source = *src;

    ASSERT_OK(source.DeleteRange(WriteOptions(), source.DefaultColumnFamily(),
                                 "m", "n"));
    ASSERT_OK(source.Flush(FlushOptions()));
    ColumnFamilyMetaData src_meta;
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(1, src_meta.levels[0].files.size());
    tombstone_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;

    ASSERT_OK(source.Put(WriteOptions(), "m1", "newer"));
    ASSERT_OK(source.Flush(FlushOptions()));
    source.GetColumnFamilyMetaData(&src_meta);
    ASSERT_EQ(2, src_meta.levels[0].files.size());
    point_file_path =
        src_meta.levels[0].files[0].db_path + src_meta.levels[0].files[0].name;
    ASSERT_OK(source.Close());
  }

  LsmEditFile tombstone_file;
  tombstone_file.path = std::move(tombstone_file_path);
  tombstone_file.level = 5;
  LsmEditFile point_file;
  point_file.path = std::move(point_file_path);
  point_file.level = 6;
  LsmEditOptions edit_options;
  edit_options.link_files = true;
  const Status s = db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {std::move(tombstone_file), std::move(point_file)})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
}

TEST_F(LsmEditTest, AllowsZeroSequenceNumberAcrossLevels) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeFile("upper.sst", 5, {{"m1", "upper"}}),
                           MakeFile("lower.sst", 6, {{"m1", "lower"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_EQ("upper", Get("m1"));
}

TEST_F(LsmEditTest, LiveTablePropertiesAvoidEqualZeroSequenceScan) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.max_open_files = 10;
  DestroyAndReopen(options);
  LsmEdit old = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                         {MakeFile("properties_old.sst", 6, {{"m", "old"}})});
  old.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {old}));
  Reopen(options);
  int skipped = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::SurvivingFileSequenceOrder:SkipScan",
      [&](void* arg) {
        if (*static_cast<bool*>(arg)) {
          ++skipped;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  LsmEdit newer = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                           {MakeFile("properties_new.sst", 5, {{"m", "new"}})});
  newer.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {newer}));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_EQ(1, skipped);
  ExpectVisibleEntries({{"m", "new"}});
  Reopen(options);
  ExpectVisibleEntries({{"m", "new"}});
}

TEST_F(LsmEditTest, SurvivorSequenceValidationAllowsConcurrentWrites) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  LsmEdit old =
      MakeEdit(db_->DefaultColumnFamily(), "a", "zz",
               {MakeDbFile("scan_live", 6, {{"a", "a"}, {"z", "z"}})});
  old.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {old}));
  LsmEdit edit =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n",
               {MakeDbFile("scan_added", 5, {{"m1", "new"}, {"m2", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  std::mutex mutex;
  std::condition_variable cv;
  bool preparing = false;
  bool release = false;
  int scans = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::SurvivingFileSequenceOrder:Prepare",
      [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        preparing = true;
        cv.notify_all();
        WaitBounded(cv, lock, [&]() { return release; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::SurvivingFileSequenceOrder:SkipScan",
      [&](void* arg) {
        if (!*static_cast<bool*>(arg)) {
          ++scans;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::future<Status> result = std::async(std::launch::async, [&]() {
    return db_->ApplyLsmEdit(LsmEditOptions(), {edit});
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return preparing; });
  }
  const Status write = RunBounded([&]() { return Put("p", "concurrent"); });
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    cv.notify_all();
  }
  const Status status = AwaitBounded(result);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(write);
  ASSERT_OK(status);
  EXPECT_GT(scans, 0);
  ExpectVisibleEntries({{"a", "a"},
                        {"m1", "new"},
                        {"m2", "new"},
                        {"p", "concurrent"},
                        {"z", "z"}});
}

TEST_F(LsmEditTest, SurvivorSequenceValidationRetriesOnlyRelevantChanges) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool overlaps : {false, true}) {
    SCOPED_TRACE(overlaps);
    DestroyAndReopen(options);
    ASSERT_OK(Put("z", "seed"));
    LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                            {MakeDbFile("retry_added", 5, {{"m1", "new"}})});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    bool changed = false;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
          if (!changed) {
            changed = true;
            EXPECT_OK(Put(overlaps ? "m1" : "z", "concurrent"));
            EXPECT_OK(Flush());
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    const Status status = RunBounded(
        [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {edit}); });
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_TRUE(changed);
    if (overlaps) {
      ASSERT_TRUE(status.IsTryAgain()) << status.ToString();
      EXPECT_EQ(0, NumTableFilesAtLevel(5));
      EXPECT_EQ("concurrent", Get("m1"));
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
      EXPECT_EQ("concurrent", Get("m1"));
    } else {
      ASSERT_OK(status);
      EXPECT_EQ("new", Get("m1"));
      EXPECT_EQ("concurrent", Get("z"));
    }
    EXPECT_EQ(1, NumTableFilesAtLevel(5));
  }
}

TEST_F(LsmEditTest, EqualZeroPointRunsHaveStableReadAndCompactionPriority) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  for (int scenario = 0; scenario < 6; ++scenario) {
    SCOPED_TRACE(scenario);
    DestroyAndReopen(options);
    const ValueType upper_type = scenario == 2   ? kTypeDeletion
                                 : scenario == 3 ? kTypeMerge
                                                 : kTypeValue;
    const ValueType lower_type = scenario == 4 ? kTypeMerge : kTypeValue;
    LsmEditFile upper =
        MakePointFile("scan_upper.sst", 4, upper_type, "m", "new");
    LsmEditFile lower =
        MakePointFile("scan_lower.sst", 6, lower_type, "m", "old");
    std::vector<LsmEditFile> files{upper, lower};
    if (scenario != 1) {
      files.push_back(MakeFile("scan_prefix.sst", 5, {{"a", "prefix"}}));
    }
    if (scenario == 5) {
      for (LsmEditFile& file : files) {
        file.level = 0;
      }
      std::swap(files[0], files[1]);
    }
    LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "a", "n", files);
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
    std::vector<std::pair<std::string, std::string>> expected;
    if (scenario != 1) {
      expected.emplace_back("a", "prefix");
    }
    if (scenario != 2) {
      expected.emplace_back("m", scenario == 3 ? "old,new" : "new");
    }
    ExpectVisibleEntries(expected);
    Reopen(options);
    ExpectVisibleEntries(expected);
    CompactRangeOptions compact;
    compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
    ExpectVisibleEntries(expected);
  }
}

TEST_F(LsmEditTest, WidePointAndRangeStartKeepPriorityAfterCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 4;
  options.disable_auto_compactions = true;
  for (bool level_zero : {false, true}) {
    for (bool delete_winner : {false, true}) {
      SCOPED_TRACE(level_zero);
      SCOPED_TRACE(delete_winner);
      DestroyAndReopen(options);
      LsmEditFile old;
      old.path = sst_files_dir_ + "wide_old.sst";
      old.level = level_zero ? 0 : 3;
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(old.path));
      ASSERT_OK(writer.PutEntity("a", {{kDefaultWideColumnName, "prefix"}}));
      ASSERT_OK(writer.PutEntity("m", {{kDefaultWideColumnName, "old"}}));
      ASSERT_OK(writer.DeleteRange("m", "z"));
      ASSERT_OK(writer.Finish());
      const LsmEditFile middle =
          MakeFile("wide_middle.sst", level_zero ? 0 : 2, {{"m", "middle"}});
      const LsmEditFile newest =
          MakePointFile("wide_new.sst", level_zero ? 0 : 1,
                        delete_winner ? kTypeDeletion : kTypeValue, "m", "new");
      LsmEdit edit;
      edit.column_family = db_->DefaultColumnFamily();
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      edit.add_files = level_zero
                           ? std::vector<LsmEditFile>{old, middle, newest}
                           : std::vector<LsmEditFile>{newest, middle, old};
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
      std::vector<std::pair<std::string, std::string>> expected{
          {"a", "prefix"}};
      if (!delete_winner) {
        expected.emplace_back("m", "new");
      }
      const auto& verify = [&]() {
        ExpectVisibleEntries(expected);
        EXPECT_EQ(delete_winner ? "NOT_FOUND" : "new", Get("m"));
      };
      verify();
      ColumnFamilyMetaData metadata;
      db_->GetColumnFamilyMetaData(&metadata);
      std::vector<std::string> names;
      for (const auto& level : metadata.levels) {
        for (const auto& file : level.files) {
          names.push_back(file.name);
        }
      }
      ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                                  db_->DefaultColumnFamily(), names, 3));
      verify();
      Reopen(options);
      verify();
    }
  }
}

void LsmEditTest::CheckPointTypesDirectionChanges(int range_boundary) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  for (bool level_zero : {false, true}) {
    for (bool upper_entity : {false, true}) {
      SCOPED_TRACE(level_zero);
      SCOPED_TRACE(upper_entity);
      SCOPED_TRACE(range_boundary);
      DestroyAndReopen(options);
      LsmEditFile upper;
      upper.path = sst_files_dir_ + "direction_upper.sst";
      upper.level = level_zero ? 0 : 5;
      SstFileWriter upper_writer(EnvOptions(), options);
      ASSERT_OK(upper_writer.Open(upper.path));
      if (upper_entity) {
        ASSERT_OK(
            upper_writer.PutEntity("a", {{kDefaultWideColumnName, "new"}}));
      } else {
        ASSERT_OK(upper_writer.Put("a", "new"));
      }
      ASSERT_OK(upper_writer.Put("b", "middle"));
      ASSERT_OK(upper_writer.Put("c", "last"));
      ASSERT_OK(upper_writer.Finish());
      LsmEditFile lower;
      lower.path = sst_files_dir_ + "direction_lower.sst";
      lower.level = level_zero ? 0 : 6;
      SstFileWriter lower_writer(EnvOptions(), options);
      ASSERT_OK(lower_writer.Open(lower.path));
      if (upper_entity) {
        ASSERT_OK(lower_writer.Put("a", "old"));
      } else {
        ASSERT_OK(
            lower_writer.PutEntity("a", {{kDefaultWideColumnName, "old"}}));
      }
      if (range_boundary == 1) {
        ASSERT_OK(lower_writer.DeleteRange("a", "d"));
      } else if (range_boundary == 2) {
        ASSERT_OK(lower_writer.DeleteRange("0", "a"));
      }
      ASSERT_OK(lower_writer.Finish());
      LsmEdit edit;
      edit.column_family = db_->DefaultColumnFamily();
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      edit.add_files = level_zero ? std::vector<LsmEditFile>{lower, upper}
                                  : std::vector<LsmEditFile>{upper, lower};
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
      for (bool async_io : {true, false}) {
        ReadOptions read;
        read.total_order_seek = true;
        read.async_io = async_io;
        read.fill_cache = false;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read));
        iter->Seek("c");
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("c", iter->key().ToString());
        iter->Prev();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("b", iter->key().ToString());
        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("c", iter->key().ToString());
        ASSERT_OK(iter->status());
      }
      ExpectVisibleEntries({{"a", "new"}, {"b", "middle"}, {"c", "last"}});
    }
  }
}

TEST_F(LsmEditTest, DifferentPointTypesKeepPriorityAcrossDirectionChanges) {
  CheckPointTypesDirectionChanges(0);
}

TEST_F(LsmEditTest, DifferentPointTypesKeepPriorityAtRangeStart) {
  CheckPointTypesDirectionChanges(1);
}

TEST_F(LsmEditTest, DifferentPointTypesKeepPriorityAtRangeEnd) {
  CheckPointTypesDirectionChanges(2);
}

TEST_F(LsmEditTest, TailingRenewalRechecksZeroSequenceDuplicates) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool renew_with_next : {false, true}) {
    SCOPED_TRACE(renew_with_next);
    DestroyAndReopen(options);
    if (renew_with_next) {
      LsmEdit initial;
      initial.column_family = db_->DefaultColumnFamily();
      initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      initial.add_files = {
          MakeFile("tail_initial.sst", 5, {{"a", "first"}, {"z", "last"}})};
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
    }
    ReadOptions read;
    read.tailing = true;
    std::unique_ptr<Iterator> tail(db_->NewIterator(read));
    tail->SeekToFirst();
    ASSERT_EQ(renew_with_next, tail->Valid());
    if (renew_with_next) {
      EXPECT_EQ("a", tail->key().ToString());
    }
    ASSERT_OK(tail->status());
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.add_files = {MakeFile("tail_new.sst", 4, {{"m", "new"}}),
                      MakeFile("tail_old.sst", 6, {{"m", "old"}})};
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
    if (renew_with_next) {
      tail->Next();
    } else {
      tail->SeekToFirst();
    }
    ASSERT_TRUE(tail->Valid());
    EXPECT_EQ("m", tail->key().ToString());
    EXPECT_EQ("new", tail->value().ToString());
    tail->Next();
    if (renew_with_next) {
      ASSERT_TRUE(tail->Valid());
      EXPECT_EQ("z", tail->key().ToString());
      tail->Next();
    }
    EXPECT_FALSE(tail->Valid());
    ASSERT_OK(tail->status());
    tail->Seek("m");
    ASSERT_TRUE(tail->Valid());
    EXPECT_EQ("new", tail->value().ToString());
    tail->Next();
    if (renew_with_next) {
      ASSERT_TRUE(tail->Valid());
      EXPECT_EQ("z", tail->key().ToString());
      tail->Next();
    }
    EXPECT_FALSE(tail->Valid());
    ASSERT_OK(tail->status());
  }
}

TEST_F(LsmEditTest, BulkTailingRenewalAcrossColumnFamilies) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"other"}, options);
  LsmEdit initial;
  initial.column_family = handles_[1];
  initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  initial.add_files = {
      MakeFile("bulk_tail_edges.sst", 5, {{"a", "first"}, {"z", "last"}})};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
  ReadOptions read;
  read.tailing = true;
  std::vector<Iterator*> raw;
  ASSERT_OK(db_->NewIterators(read, {handles_[0], handles_[1]}, &raw));
  std::vector<std::unique_ptr<Iterator>> tails;
  for (Iterator* iter : raw) {
    tails.emplace_back(iter);
  }
  ASSERT_EQ(2, tails.size());
  tails[0]->SeekToFirst();
  EXPECT_FALSE(tails[0]->Valid());
  ASSERT_OK(tails[0]->status());
  tails[1]->SeekToFirst();
  ASSERT_TRUE(tails[1]->Valid());
  EXPECT_EQ("a", tails[1]->key().ToString());

  std::vector<LsmEdit> edits;
  for (size_t i = 0; i < tails.size(); ++i) {
    LsmEdit edit;
    edit.column_family = handles_[i];
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.add_files = {MakeFile("bulk_tail_new_" + std::to_string(i) + ".sst", 4,
                               {{"m", "new"}}),
                      MakeFile("bulk_tail_old_" + std::to_string(i) + ".sst", 6,
                               {{"m", "old"}})};
    edits.push_back(std::move(edit));
  }
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), edits));
  tails[0]->SeekToFirst();
  tails[1]->Next();
  for (size_t i = 0; i < tails.size(); ++i) {
    SCOPED_TRACE(i);
    ASSERT_TRUE(tails[i]->Valid());
    EXPECT_EQ("m", tails[i]->key().ToString());
    EXPECT_EQ("new", tails[i]->value().ToString());
    tails[i]->Next();
    if (i == 1) {
      ASSERT_TRUE(tails[i]->Valid());
      EXPECT_EQ("z", tails[i]->key().ToString());
      tails[i]->Next();
    }
    EXPECT_FALSE(tails[i]->Valid());
    ASSERT_OK(tails[i]->status());
  }

  const auto expect_tail = [&](size_t i, const std::string& value) {
    tails[i]->Seek("m");
    ASSERT_TRUE(tails[i]->Valid());
    EXPECT_EQ("m", tails[i]->key().ToString());
    EXPECT_EQ(value, tails[i]->value().ToString());
    tails[i]->Next();
    if (i == 1) {
      ASSERT_TRUE(tails[i]->Valid());
      EXPECT_EQ("z", tails[i]->key().ToString());
      tails[i]->Next();
    }
    EXPECT_FALSE(tails[i]->Valid());
    ASSERT_OK(tails[i]->status());
    EXPECT_EQ(value, Get(static_cast<int>(i), "m"));
  };
  CompactRangeOptions compact;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  for (size_t i = 0; i < tails.size(); ++i) {
    ASSERT_OK(db_->CompactRange(compact, handles_[i], nullptr, nullptr));
    expect_tail(i, "new");
    edits[i].add_files = {
        MakeFile("bulk_tail_latest_" + std::to_string(i) + ".sst", 0,
                 {{"m", "latest"}})};
  }
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), edits));
  for (size_t i = 0; i < tails.size(); ++i) {
    expect_tail(i, "latest");
  }
}

TEST_F(LsmEditTest, TailingReadsHonorEqualSequenceSourcePriority) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool level_zero : {false, true}) {
    for (bool newer_delete : {true, false}) {
      SCOPED_TRACE(level_zero);
      SCOPED_TRACE(newer_delete);
      DestroyAndReopen(options);
      const LsmEditFile upper =
          MakePointFile("tail_upper.sst", level_zero ? 0 : 4,
                        newer_delete ? kTypeDeletion : kTypeValue, "m", "new");
      LsmEditFile lower;
      lower.path = sst_files_dir_ + "tail_lower.sst";
      lower.level = level_zero ? 0 : 6;
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(lower.path));
      if (newer_delete) {
        ASSERT_OK(writer.Put("m", "old"));
      } else {
        ASSERT_OK(writer.PutEntity("m", {{kDefaultWideColumnName, "old"}}));
      }
      ASSERT_OK(writer.Finish());
      LsmEdit edit;
      edit.column_family = db_->DefaultColumnFamily();
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      edit.add_files = level_zero ? std::vector<LsmEditFile>{lower, upper}
                                  : std::vector<LsmEditFile>{upper, lower};
      edit.add_files.push_back(MakeFile("tail_edges.sst", level_zero ? 0 : 5,
                                        {{"a", "first"}, {"z", "last"}}));
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
      std::vector<std::pair<std::string, std::string>> expected{{"a", "first"}};
      if (!newer_delete) {
        expected.emplace_back("m", "new");
      }
      expected.emplace_back("z", "last");
      ExpectVisibleEntries(expected);
      ReadOptions read;
      read.tailing = true;
      std::unique_ptr<Iterator> tail(db_->NewIterator(read));
      const auto expect_tail = [&]() {
        std::vector<std::pair<std::string, std::string>> actual;
        for (tail->SeekToFirst(); tail->Valid(); tail->Next()) {
          actual.emplace_back(tail->key().ToString(), tail->value().ToString());
        }
        ASSERT_OK(tail->status());
        EXPECT_EQ(expected, actual);
      };
      expect_tail();
      LsmEdit renewal;
      renewal.column_family = db_->DefaultColumnFamily();
      renewal.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      renewal.add_files = {MakeFile("tail_latest.sst", 0, {{"m", "latest"}})};
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {renewal}));
      expected = {{"a", "first"}, {"m", "latest"}, {"z", "last"}};
      ExpectVisibleEntries(expected);
      expect_tail();
      ASSERT_OK(Put("zz", "renewed"));
      ASSERT_OK(Flush());
      expected.emplace_back("zz", "renewed");
      expect_tail();
      CompactRangeOptions compact;
      compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
      EXPECT_EQ(1, CountLiveFiles());
      expect_tail();
    }
  }
}

TEST_F(LsmEditTest, UnresolvedEqualSequenceOperandsFailWithoutLosingInputs) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  for (bool with_base : {false, true}) {
    SCOPED_TRACE(with_base);
    DestroyAndReopen(options);
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    if (with_base) {
      edit.add_files.push_back(
          MakeFile("operand_base.sst", 5, {{"m1", "base"}}));
    }
    edit.add_files.push_back(
        MakePointFile("operand_old.sst", 5, kTypeMerge, "m1", "old"));
    edit.add_files.push_back(
        MakePointFile("operand_new.sst", 5, kTypeMerge, "m1", "new"));
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.move_files = true;
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    const Status status = db_->ApplyLsmEdit(edit_options, {edit});
    if (with_base) {
      ASSERT_OK(status);
      ExpectVisibleEntries({{"m1", "base,old,new"}});
      Reopen(options);
      ExpectVisibleEntries({{"m1", "base,old,new"}});
    } else {
      ASSERT_TRUE(status.IsNotSupported()) << status.ToString();
      EXPECT_EQ(before, FileNumbersPerLevel());
      EXPECT_EQ("NOT_FOUND", Get("m1"));
      for (const LsmEditFile& file : edit.add_files) {
        ASSERT_OK(env_->FileExists(file.path));
      }
      ASSERT_OK(Put("m1", "fresh"));
      EXPECT_EQ("fresh", Get("m1"));
    }
  }
}

TEST_F(LsmEditTest, EqualSequenceMergeOperandsCannotRemainInSeparateRuns) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  for (bool survivor : {false, true}) {
    for (bool merged_outputs : {false, true}) {
      SCOPED_TRACE(survivor);
      SCOPED_TRACE(merged_outputs);
      DestroyAndReopen(options);
      const LsmEditFile old =
          MakePointFile("separate_old.sst", 6, kTypeMerge, "m1", "old");
      if (survivor) {
        LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {old});
        first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
        ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
        EXPECT_EQ("old", Get("m1"));
      }
      const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
      const LsmEditFile newest =
          MakePointFile("separate_new.sst", 5, kTypeMerge, "m1", "new");
      LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                              survivor ? std::vector<LsmEditFile>{newest}
                                       : std::vector<LsmEditFile>{newest, old});
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      LsmEditOptions edit_options;
      edit_options.merge_incoming_files = merged_outputs;
      const Status status = db_->ApplyLsmEdit(edit_options, {edit});
      ASSERT_TRUE(status.IsNotSupported()) << status.ToString();
      EXPECT_EQ(before, FileNumbersPerLevel());
      EXPECT_EQ(survivor ? "old" : "NOT_FOUND", Get("m1"));
    }
  }
}

TEST_F(LsmEditTest, BackgroundErrorSnapshotsAfterFailedAtomicEdit) {
  for (bool timestamped : {false, true}) {
    for (bool two_write_queues : {false, true}) {
      SCOPED_TRACE(timestamped);
      SCOPED_TRACE(two_write_queues);
      Options options = CurrentOptions();
      options.disable_auto_compactions = true;
      options.two_write_queues = two_write_queues;
      const std::shared_ptr<SnapshotOnBackgroundErrorListener> listener =
          std::make_shared<SnapshotOnBackgroundErrorListener>();
      listener->timestamped = timestamped;
      options.listeners.push_back(listener);
      DestroyAndReopen(options);
      CreateAndReopenWithCF({"other"}, options);
      std::vector<LsmEdit> edits;
      std::vector<std::string> original_files;
      for (int i = 0; i < 2; ++i) {
        ASSERT_OK(Put(i, "m1", "old"));
        ASSERT_OK(Flush(i));
        ColumnFamilyMetaData metadata;
        db_->GetColumnFamilyMetaData(handles_[i], &metadata);
        ASSERT_EQ(1, metadata.levels[0].files.size());
        LsmEdit edit;
        edit.column_family = handles_[i];
        edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
        const SstFileMetaData& old = metadata.levels[0].files.front();
        edit.delete_files.push_back({0, old.file_number});
        edit.add_files = {
            MakeFile("error_snapshot_" + std::to_string(i) + ".sst", 0,
                     {{"m1", "new"}})};
        original_files.push_back(old.db_path + old.name);
        edits.push_back(std::move(edit));
      }
      listener->db = dbfull();
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
            listener->armed.store(true);
            env_->manifest_write_error_.store(true);
          });
      SyncPoint::GetInstance()->EnableProcessing();
      const Status status = RunBounded(
          [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), edits); });
      env_->manifest_write_error_.store(false);
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      ASSERT_EQ(Status::kIOError, status.code());
      EXPECT_EQ(1, listener->calls.load());
      EXPECT_EQ("old", listener->observed);
      for (int i = 0; i < 2; ++i) {
        EXPECT_EQ("old", Get(i, "m1"));
        ASSERT_OK(env_->FileExists(original_files[i]));
        ASSERT_OK(env_->FileExists(edits[i].add_files.front().path));
      }
      const Snapshot* snapshot = db_->GetSnapshot();
      ASSERT_NE(nullptr, snapshot);
      db_->ReleaseSnapshot(snapshot);
      listener->db = nullptr;
      ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other"}, options);
      listener->db = dbfull();
      for (int i = 0; i < 2; ++i) {
        EXPECT_EQ("old", Get(i, "m1"));
        edits[i].column_family = handles_[i];
      }
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), edits));
      for (int i = 0; i < 2; ++i) {
        EXPECT_EQ("new", Get(i, "m1"));
      }
    }
  }
}

TEST_F(LsmEditTest, ListenerRegistrationFailureDoesNotBlockRetry) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.listeners = {std::make_shared<EventListener>(),
                       std::make_shared<EventListener>(),
                       std::make_shared<EventListener>()};
  for (size_t fail_at = 0; fail_at < options.listeners.size(); ++fail_at) {
    SCOPED_TRACE(fail_at);
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "old"));
    ASSERT_OK(Flush());
    const auto before = FileNumbersPerLevel();
    const LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("registration_failure.sst", 5, {{"m1", "new"}})});
    size_t attempts = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "ScopedDeferredFileIOListeners::BeforeRegisterLease", [&](void* arg) {
          if (attempts++ == fail_at) {
            *static_cast<std::function<void()>*>(arg) = []() {
              throw std::bad_alloc();
            };
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    const Status failed = RunBounded(
        [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {edit}); });
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_TRUE(failed.IsMemoryLimit()) << failed.ToString();
    EXPECT_EQ(fail_at + 1, attempts);
    EXPECT_EQ(before, FileNumbersPerLevel());
    ExpectVisibleEntries({{"a", "old"}});
    ASSERT_OK(env_->FileExists(edit.add_files.front().path));
    ASSERT_OK(RunBounded([&]() { return Put("z", "post_failure"); }));
    ASSERT_OK(RunBounded(
        [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {edit}); }));
    ExpectVisibleEntries({{"a", "old"}, {"m1", "new"}, {"z", "post_failure"}});
  }
}

TEST_F(LsmEditTest, NotificationAllocationFailureReleasesCommitResources) {
  const std::vector<std::string> hooks{
      "ScopedDeferredFileIOListeners::BeforeOwnedEvent",
      "ScopedDeferredFileIOListeners::BeforeCallbackInsertion",
      "ScopedDeferredFileIOListeners::BeforeOwnedErrorEvent",
      "ScopedDeferredFileIOListeners::BeforeErrorCallbackInsertion"};
  for (bool two_write_queues : {false, true}) {
    for (size_t hook = 0; hook < hooks.size(); ++hook) {
      SCOPED_TRACE(two_write_queues);
      SCOPED_TRACE(hooks[hook]);
      Options options = CurrentOptions();
      options.disable_auto_compactions = true;
      options.two_write_queues = two_write_queues;
      const auto listener = std::make_shared<NotificationAllocationListener>();
      options.listeners = {listener};
      DestroyAndReopen(options);
      ASSERT_OK(Put("m1", "old"));
      ASSERT_OK(Flush());
      MoveFilesToLevel(6);
      const auto before = FileNumbersPerLevel();
      const auto file = MakeFile("allocation_notify.sst", 6, {{"m1", "new"}});
      LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {file});
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      edit.delete_files = {{6, before[6].front()}};
      listener->db = db_.get();
      const bool manifest_failure = hook >= 2;
      bool armed = false;
      bool injected = false;
      size_t notifications = 0;
      SyncPoint::GetInstance()->SetCallBack(
          "VersionSet::LogAndApply:WriteManifestStart", [&](void*) {
            if (!ScopedDeferredFileIOListeners::CaptureContext()) {
              return;
            }
            armed = true;
            listener->observe = true;
            if (manifest_failure) {
              env_->manifest_write_error_.store(true);
            }
          });
      SyncPoint::GetInstance()->SetCallBack(hooks[hook], [&](void* arg) {
        if (armed && !injected &&
            notifications++ == (manifest_failure ? 0 : 1)) {
          injected = true;
          *static_cast<std::function<void()>*>(arg) = []() {
            throw std::bad_alloc();
          };
        }
      });
      SyncPoint::GetInstance()->EnableProcessing();
      const Status status = RunBounded(
          [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {edit}); });
      env_->manifest_write_error_.store(false);
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      listener->observe = false;
      EXPECT_TRUE(injected);
      EXPECT_EQ(1, listener->memory_errors);
      EXPECT_GT(listener->snapshots, 0);
      EXPECT_GT(listener->manifest_callbacks, 0);
      if (manifest_failure) {
        ASSERT_NOK(status);
        EXPECT_EQ(before, FileNumbersPerLevel());
        EXPECT_EQ("old", Get("m1"));
        listener->db = nullptr;
        Reopen(options);
        listener->db = db_.get();
      } else {
        ASSERT_OK(status);
        EXPECT_EQ("new", Get("m1"));
      }
      ASSERT_OK(env_->FileExists(file.path));
      ASSERT_OK(RunBounded([&]() { return Put("after", "write"); }));
      ASSERT_OK(RunBounded([&]() {
        const Snapshot* snapshot = db_->GetSnapshot();
        if (snapshot == nullptr) {
          return Status::NotSupported("snapshot unavailable");
        }
        db_->ReleaseSnapshot(snapshot);
        return Status::OK();
      }));
      LsmEdit next = MakeEdit(
          db_->DefaultColumnFamily(), "p", "q",
          {MakeFile("allocation_notify_next.sst", 5, {{"p1", "next"}})});
      next.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      ASSERT_OK(RunBounded(
          [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {next}); }));
      std::vector<LiveFileMetaData> live_files;
      db_->GetLiveFilesMetaData(&live_files);
      for (const auto& live : live_files) {
        EXPECT_FALSE(live.being_compacted);
      }
      listener->db = nullptr;
      ASSERT_OK(RunBounded([&]() { return db_->Close(); }));
    }
  }
}

TEST_F(LsmEditTest, FileIOCallbacksCanSnapshotAfterInstallationAndFailure) {
  for (int kind = 0; kind < 3; ++kind) {
    SCOPED_TRACE(kind);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.max_open_files = -1;
    const std::shared_ptr<SnapshotOnFileIOListener> listener =
        std::make_shared<SnapshotOnFileIOListener>();
    listener->kind = kind;
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    const LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("callback_one.sst", 5, {{"m1", "new"}}),
                  MakeFile("callback_two.sst", 5, {{"m2", "other"}})});
    listener->db.store(db_.get());
    std::thread::id committing_thread;
    std::mutex state_mutex;
    std::condition_variable state_cv;
    int worker_opens = 0;
    auto* cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
          listener->armed.store(true);
          if (kind == 2) {
            env_->manifest_write_error_.store(true);
          }
        });
    if (kind == 1) {
      SyncPoint::GetInstance()->SetCallBack(
          "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable",
          [&](void* arg) {
            std::unique_lock<std::mutex> lock(state_mutex);
            if (std::this_thread::get_id() == committing_thread) {
              WaitBounded(state_cv, lock, [&]() { return worker_opens > 0; });
            } else {
              ++worker_opens;
              state_cv.notify_all();
            }
            auto* file = static_cast<FileMetaData*>(arg);
            TableCache::Evict(cfd->table_cache()->get_cache().get(),
                              file->fd.GetNumber());
          });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    const Status status = RunBounded([&]() {
      committing_thread = std::this_thread::get_id();
      LsmEditOptions edit_options;
      edit_options.file_opening_threads = 4;
      return db_->ApplyLsmEdit(edit_options, {edit});
    });
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(1, listener->calls.load());
    EXPECT_FALSE(listener->path.empty());
    if (kind == 2) {
      ASSERT_NOK(status);
      EXPECT_EQ("NOT_FOUND", listener->observed);
      EXPECT_EQ("NOT_FOUND", Get("m1"));
      listener->db.store(nullptr);
      Reopen(options);
      listener->db.store(db_.get());
    } else {
      ASSERT_OK(status);
      EXPECT_EQ("new", listener->observed);
      EXPECT_EQ("new", Get("m1"));
    }
    if (kind == 1) {
      EXPECT_GT(worker_opens, 0);
    }
    ASSERT_OK(Put("post", "write"));
    EXPECT_EQ("write", Get("post"));
    listener->db.store(nullptr);
  }
}

TEST_F(LsmEditTest, EarlierManifestSnapshotCallbackDoesNotBlockLsmEdit) {
  for (bool retain_snapshot : {false, true}) {
    SCOPED_TRACE(retain_snapshot);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    const auto listener = std::make_shared<SnapshotOnFileIOListener>();
    listener->retain_snapshot = retain_snapshot;
    options.listeners = {listener};
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"other"}, options);
    ASSERT_OK(Put(1, "z", "background"));
    const LsmEdit edit =
        MakeEdit(handles_[0], "m", "n",
                 {MakeFile("queued_manifest.sst", 5, {{"m1", "new"}})});
    listener->db = db_.get();
    std::mutex mutex;
    std::condition_variable cv;
    std::thread::id editing_thread;
    bool edit_paused = false;
    bool release_edit = false;
    bool manifest_paused = false;
    bool release_manifest = false;
    bool edit_waiting = false;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
          std::unique_lock<std::mutex> lock(mutex);
          edit_paused = true;
          cv.notify_all();
          WaitBounded(cv, lock, [&]() { return release_edit; });
        });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifestStart", [&](void*) {
          std::unique_lock<std::mutex> lock(mutex);
          if (std::this_thread::get_id() != editing_thread) {
            manifest_paused = true;
            cv.notify_all();
            WaitBounded(cv, lock, [&]() { return release_manifest; });
          }
        });
    const auto signal_wait = [&](void*) {
      std::lock_guard<std::mutex> lock(mutex);
      if (std::this_thread::get_id() == editing_thread && manifest_paused) {
        edit_waiting = true;
        cv.notify_all();
      }
    };
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:BeforeWriterWaiting", signal_wait);
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::WaitForManifestWrites:Waiting", signal_wait);
    SyncPoint::GetInstance()->EnableProcessing();
    std::future<Status> result = std::async(std::launch::async, [&]() {
      {
        std::lock_guard<std::mutex> lock(mutex);
        editing_thread = std::this_thread::get_id();
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    });
    {
      std::unique_lock<std::mutex> lock(mutex);
      WaitBounded(cv, lock, [&]() { return edit_paused; });
    }
    std::future<Status> flush = std::async(std::launch::async, [&]() {
      return db_->Flush(FlushOptions(), handles_[1]);
    });
    {
      std::unique_lock<std::mutex> lock(mutex);
      WaitBounded(cv, lock, [&]() { return manifest_paused; });
      release_edit = true;
      cv.notify_all();
      WaitBounded(cv, lock, [&]() { return edit_waiting; });
      listener->armed = true;
      release_manifest = true;
      cv.notify_all();
    }
    const Status status = AwaitBounded(result);
    const Status flushed = AwaitBounded(flush);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (listener->retained_snapshot) {
      db_->ReleaseSnapshot(listener->retained_snapshot);
      listener->retained_snapshot = nullptr;
    }
    ASSERT_OK(flushed);
    EXPECT_EQ(1, listener->calls.load());
    EXPECT_EQ("NOT_FOUND", listener->observed);
    EXPECT_EQ("background", Get(1, "z"));
    if (retain_snapshot) {
      ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
      EXPECT_EQ("NOT_FOUND", Get("m1"));
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
    } else {
      ASSERT_OK(status);
    }
    EXPECT_EQ("new", Get("m1"));
    ASSERT_OK(db_->WaitForCompact(WaitForCompactOptions()));
    listener->db = nullptr;
  }
}

TEST_F(LsmEditTest, ForeignListenerContextsUseSeparateManifestBatches) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  const auto first_listener = std::make_shared<PrepareOnFileIOListener>();
  const auto second_listener = std::make_shared<PrepareOnFileIOListener>();
  options.listeners = {first_listener};
  DestroyAndReopen(options);
  Options second_options = options;
  second_options.create_if_missing = true;
  second_options.listeners = {second_listener};
  Options destination_options = second_options;
  destination_options.listeners.clear();
  const std::string second_name = dbname_ + "_context_source";
  const std::string destination_name = dbname_ + "_context_destination";
  source_dbnames_.push_back(second_name);
  source_dbnames_.push_back(destination_name);
  ASSERT_OK(DestroyDB(second_name, second_options));
  ASSERT_OK(DestroyDB(destination_name, destination_options));
  std::unique_ptr<DB> second;
  std::unique_ptr<DB> destination;
  ASSERT_OK(DB::Open(second_options, second_name, &second));
  ASSERT_OK(DB::Open(destination_options, destination_name, &destination));
  ASSERT_OK(destination->Put(WriteOptions(), "z", "old"));
  const LsmEdit first_edit =
      MakeEdit(db_->DefaultColumnFamily(), "a", "b",
               {MakeFile("context_first.sst", 5, {{"a1", "first"}})});
  const LsmEdit second_edit =
      MakeEdit(second->DefaultColumnFamily(), "g", "h",
               {MakeFile("context_second.sst", 5, {{"g1", "second"}})});
  const auto first_file =
      MakeFile("context_prepare_first.sst", 0, {{"p", "p"}});
  const auto second_file =
      MakeFile("context_prepare_second.sst", 0, {{"t", "t"}});
  std::unique_ptr<FileIngestionHandle> first_handle;
  std::unique_ptr<FileIngestionHandle> second_handle;
  std::optional<Status> first_prepared;
  std::optional<Status> second_prepared;
  std::mutex mutex;
  std::condition_variable cv;
  std::thread::id first_thread;
  std::thread::id second_thread;
  bool leader_paused = false;
  bool release_leader = false;
  bool first_preparing = false;
  bool second_preparing = false;
  bool first_queued = false;
  bool second_queued = false;
  first_listener->action = [&]() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      first_preparing = true;
    }
    first_prepared = destination->PrepareFileIngestion(
        destination->DefaultColumnFamily(), {first_file.path},
        IngestExternalFileOptions(), &first_handle);
  };
  second_listener->action = [&]() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      second_preparing = true;
    }
    second_prepared = destination->PrepareFileIngestion(
        destination->DefaultColumnFamily(), {second_file.path},
        IngestExternalFileOptions(), &second_handle);
  };
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifestStart", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        if (!leader_paused) {
          leader_paused = true;
          cv.notify_all();
          WaitBounded(cv, lock, [&]() { return release_leader; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        if (std::this_thread::get_id() == first_thread) {
          first_listener->armed = true;
        } else if (std::this_thread::get_id() == second_thread) {
          second_listener->armed = true;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        if (first_preparing && std::this_thread::get_id() == first_thread) {
          first_queued = true;
        }
        if (second_preparing && std::this_thread::get_id() == second_thread) {
          second_queued = true;
        }
        cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::future<Status> flush = std::async(
      std::launch::async, [&]() { return destination->Flush(FlushOptions()); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return leader_paused; });
  }
  std::future<Status> first_result = std::async(std::launch::async, [&]() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      first_thread = std::this_thread::get_id();
    }
    return db_->ApplyLsmEdit(LsmEditOptions(), {first_edit});
  });
  std::future<Status> second_result = std::async(std::launch::async, [&]() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      second_thread = std::this_thread::get_id();
    }
    return second->ApplyLsmEdit(LsmEditOptions(), {second_edit});
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return first_queued && second_queued; });
    release_leader = true;
    cv.notify_all();
  }
  const Status first_status = AwaitBounded(first_result);
  const Status second_status = AwaitBounded(second_result);
  const Status flushed = AwaitBounded(flush);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(first_status);
  ASSERT_OK(second_status);
  ASSERT_OK(flushed);
  ASSERT_TRUE(first_prepared.has_value());
  ASSERT_TRUE(second_prepared.has_value());
  ASSERT_OK(*first_prepared);
  ASSERT_OK(*second_prepared);
  EXPECT_EQ(1, first_listener->completed.load());
  EXPECT_EQ(1, second_listener->completed.load());
  std::vector<std::unique_ptr<FileIngestionHandle>> handles;
  handles.push_back(std::move(first_handle));
  handles.push_back(std::move(second_handle));
  ASSERT_OK(destination->CommitFileIngestionHandles(std::move(handles)));
  std::string value;
  ASSERT_OK(destination->Get(ReadOptions(), "p", &value));
  EXPECT_EQ("p", value);
  ASSERT_OK(destination->Get(ReadOptions(), "t", &value));
  EXPECT_EQ("t", value);
  ASSERT_OK(destination->Get(ReadOptions(), "z", &value));
  EXPECT_EQ("old", value);
  EXPECT_EQ("first", Get("a1"));
  ASSERT_OK(second->Get(ReadOptions(), "g1", &value));
  EXPECT_EQ("second", value);
  ASSERT_OK(destination->Close());
  ASSERT_OK(second->Close());
}

TEST_F(LsmEditTest, MultiCfPreparationReleasesEveryVersionOnAllocationFailure) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other"}, options);
  const auto first = MakeFile("pin_first.sst", 5, {{"a", "first"}});
  const auto second = MakeFile("pin_second.sst", 5, {{"g", "second"}});
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  std::vector<IngestExternalFileArg> args(2);
  args[0].column_family = handles_[0];
  args[0].external_files = {first.path};
  args[1].column_family = handles_[1];
  args[1].external_files = {second.path};
  for (int fail_at : {-1, 0, 1}) {
    SCOPED_TRACE(fail_at);
    const uint64_t releases =
        options.statistics->getTickerCount(NUMBER_SUPERVERSION_RELEASES);
    int jobs = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::PrepareFileIngestionImpl:BeforeOptionsCopy", [&](void* arg) {
          if (fail_at == -1) {
            *static_cast<std::function<void()>*>(arg) = []() {
              throw std::bad_alloc();
            };
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::PrepareFileIngestionImpl:BeforeCreateJob", [&](void* arg) {
          if (jobs++ == fail_at) {
            *static_cast<std::function<void()>*>(arg) = []() {
              throw std::bad_alloc();
            };
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    std::unique_ptr<FileIngestionHandle> prepared;
    EXPECT_THROW(RunBounded([&]() {
                   return db_->PrepareFileIngestion(args, &prepared);
                 }).PermitUncheckedError(),
                 std::bad_alloc);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(nullptr, prepared);
    EXPECT_EQ(args.size(),
              options.statistics->getTickerCount(NUMBER_SUPERVERSION_RELEASES) -
                  releases);
    ASSERT_OK(db_->PrepareFileIngestion(args, &prepared));
    ASSERT_OK(prepared->Abort());
  }
}

TEST_F(LsmEditTest, ConcurrentEditsDeliverTheirOwnFileIOBeforeCompletion) {
  for (bool manifest_failure : {false, true}) {
    SCOPED_TRACE(manifest_failure);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    const std::shared_ptr<OrderedFileIOListener> listener =
        std::make_shared<OrderedFileIOListener>();
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    listener->db = db_.get();
    LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                             {MakeFile("first_io.sst", 5, {{"m1", "first"}})});
    LsmEdit second =
        MakeEdit(db_->DefaultColumnFamily(), "p", "q",
                 {MakeFile("second_io.sst", 5, {{"p1", "second"}})});
    std::mutex state_mutex;
    std::condition_variable state_cv;
    std::thread::id first_thread;
    std::thread::id second_thread;
    bool first_paused = false;
    bool second_attempted = false;
    bool first_finished = false;
    SyncPoint::GetInstance()->SetCallBack(
        "ScopedDeferredFileIOListeners::BeforeWaitForScopes", [&](void*) {
          std::lock_guard<std::mutex> lock(state_mutex);
          if (std::this_thread::get_id() == second_thread) {
            second_attempted = true;
            state_cv.notify_all();
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          if (std::this_thread::get_id() == first_thread) {
            first_paused = true;
            state_cv.notify_all();
            WaitBounded(state_cv, lock, [&]() { return second_attempted; });
          } else if (std::this_thread::get_id() == second_thread) {
            WaitBounded(state_cv, lock, [&]() { return first_finished; });
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
          if (std::this_thread::get_id() == first_thread) {
            listener->arm_time.store(
                std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
            listener->armed.store(true);
          }
          if (manifest_failure && std::this_thread::get_id() == second_thread) {
            env_->manifest_write_error_.store(true);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    std::future<Status> first_result = std::async(std::launch::async, [&]() {
      {
        std::lock_guard<std::mutex> lock(state_mutex);
        first_thread = std::this_thread::get_id();
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {first});
    });
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return first_paused; });
    }
    std::future<Status> second_result = std::async(std::launch::async, [&]() {
      {
        std::lock_guard<std::mutex> lock(state_mutex);
        second_thread = std::this_thread::get_id();
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {second});
    });
    const Status first_status = AwaitBounded(first_result);
    EXPECT_EQ((std::vector<std::string>{"io", first.add_files[0].path}),
              listener->Events());
    {
      std::lock_guard<std::mutex> lock(state_mutex);
      first_finished = true;
      state_cv.notify_all();
    }
    const Status second_status = AwaitBounded(second_result);
    env_->manifest_write_error_.store(false);
    listener->armed.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(first_status);
    if (manifest_failure) {
      ASSERT_NOK(second_status);
      EXPECT_GT(listener->errors.load(), 0);
      EXPECT_EQ((std::vector<std::string>{"io", first.add_files[0].path}),
                listener->Events());
      ASSERT_OK(env_->FileExists(second.add_files[0].path));
      Reopen(options);
    } else {
      ASSERT_OK(second_status);
      EXPECT_EQ((std::vector<std::string>{"io", first.add_files[0].path, "io",
                                          second.add_files[0].path}),
                listener->Events());
    }
    EXPECT_EQ("first", Get("m1"));
    EXPECT_EQ(manifest_failure ? "NOT_FOUND" : "second", Get("p1"));
    ASSERT_OK(Put("post", "write"));
  }
}

TEST_F(LsmEditTest, DifferentListenersDoNotSerializeUnrelatedEdits) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.listeners.push_back(std::make_shared<OrderedFileIOListener>());
  DestroyAndReopen(options);
  Options other_options = options;
  other_options.listeners = {std::make_shared<OrderedFileIOListener>()};
  other_options.create_if_missing = true;
  const std::string other_name = dbname_ + "_independent_listener";
  source_dbnames_.push_back(other_name);
  ASSERT_OK(DestroyDB(other_name, other_options));
  std::unique_ptr<DB> other;
  ASSERT_OK(DB::Open(other_options, other_name, &other));
  const LsmEdit first =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n",
               {MakeFile("independent_first.sst", 5, {{"m1", "first"}})});
  const LsmEdit second =
      MakeEdit(other->DefaultColumnFamily(), "p", "q",
               {MakeFile("independent_second.sst", 5, {{"p1", "second"}})});
  std::mutex state_mutex;
  std::condition_variable state_cv;
  std::thread::id first_thread;
  bool first_paused = false;
  bool release = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
        std::unique_lock<std::mutex> lock(state_mutex);
        if (std::this_thread::get_id() == first_thread) {
          first_paused = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return release; });
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::future<Status> first_result = std::async(std::launch::async, [&]() {
    {
      std::lock_guard<std::mutex> lock(state_mutex);
      first_thread = std::this_thread::get_id();
    }
    return db_->ApplyLsmEdit(LsmEditOptions(), {first});
  });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    WaitBounded(state_cv, lock, [&]() { return first_paused; });
  }
  const Status second_status = RunBounded(
      [&]() { return other->ApplyLsmEdit(LsmEditOptions(), {second}); });
  std::string value;
  EXPECT_OK(other->Get(ReadOptions(), "p1", &value));
  EXPECT_EQ("second", value);
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  {
    std::lock_guard<std::mutex> lock(state_mutex);
    release = true;
    state_cv.notify_all();
  }
  const Status first_status = AwaitBounded(first_result);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(first_status);
  ASSERT_OK(second_status);
  EXPECT_EQ("first", Get("m1"));
  ASSERT_OK(other->Close());
}

TEST_F(LsmEditTest, UnrelatedFileIOWithSharedListenerRemainsSynchronous) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.max_open_files = 10;
  const auto listener = std::make_shared<SnapshotOnFileIOListener>();
  listener->kind = 1;
  listener->snapshot_only = true;
  options.listeners = {listener};
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", std::string(16384, 'v')));
  ASSERT_OK(Flush());
  Reopen(options);
  const LsmEdit edit =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n",
               {MakeFile("shared_io_edit.sst", 5, {{"m1", "new"}})});
  std::mutex mutex;
  std::condition_variable cv;
  bool paused = false;
  bool release = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        paused = true;
        cv.notify_all();
        WaitBounded(cv, lock, [&]() { return release; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::future<Status> result = std::async(std::launch::async, [&]() {
    return db_->ApplyLsmEdit(LsmEditOptions(), {edit});
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return paused; });
  }
  listener->db = db_.get();
  listener->armed = true;
  std::thread::id read_thread;
  const Status read = RunBounded([&]() {
    read_thread = std::this_thread::get_id();
    std::string value;
    return db_->Get(ReadOptions(), "a", &value);
  });
  EXPECT_EQ(1, listener->calls.load());
  EXPECT_EQ(read_thread, listener->callback_thread);
  EXPECT_TRUE(listener->observed.empty());
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    cv.notify_all();
  }
  const Status status = AwaitBounded(result);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(read);
  ASSERT_OK(status);
  EXPECT_EQ("new", Get("m1"));
  listener->db = nullptr;
}

TEST_F(LsmEditTest, DisjointListenerCallbacksCannotWaitCyclically) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  const auto first_listener = std::make_shared<NestedLeaseEditListener>();
  const auto second_listener = std::make_shared<NestedLeaseEditListener>();
  options.listeners = {first_listener};
  DestroyAndReopen(options);
  Options other_options = options;
  other_options.create_if_missing = true;
  other_options.listeners = {second_listener};
  const std::string other_name = dbname_ + "_cyclic_listener";
  source_dbnames_.push_back(other_name);
  ASSERT_OK(DestroyDB(other_name, other_options));
  std::unique_ptr<DB> other;
  ASSERT_OK(DB::Open(other_options, other_name, &other));
  LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "a", "b",
                           {MakeFile("cycle_first.sst", 5, {{"a1", "first"}})});
  LsmEdit second =
      MakeEdit(other->DefaultColumnFamily(), "g", "h",
               {MakeFile("cycle_second.sst", 5, {{"g1", "second"}})});
  first.deletion_mode = second.deletion_mode =
      LsmEditDeletionMode::kExplicitFiles;
  first_listener->source = db_.get();
  first_listener->target = other.get();
  first_listener->edit = MakeEdit(
      other->DefaultColumnFamily(), "p", "q",
      {MakeFile("cycle_inner_second.sst", 6, {{"p1", "inner-second"}})});
  second_listener->source = other.get();
  second_listener->target = db_.get();
  second_listener->edit =
      MakeEdit(db_->DefaultColumnFamily(), "t", "u",
               {MakeFile("cycle_inner_first.sst", 6, {{"t1", "inner-first"}})});
  first_listener->edit.deletion_mode = second_listener->edit.deletion_mode =
      LsmEditDeletionMode::kExplicitFiles;
  std::mutex mutex;
  std::condition_variable cv;
  int entered = 0;
  int finished = 0;
  const auto before_nested = [&]() {
    std::unique_lock<std::mutex> lock(mutex);
    ++entered;
    cv.notify_all();
    WaitBounded(cv, lock, [&]() { return entered == 2; });
  };
  const auto after_nested = [&]() {
    std::unique_lock<std::mutex> lock(mutex);
    ++finished;
    cv.notify_all();
    WaitBounded(cv, lock, [&]() { return finished == 2; });
  };
  first_listener->before_apply = second_listener->before_apply = before_nested;
  first_listener->after_apply = second_listener->after_apply = after_nested;
  std::thread::id first_thread;
  std::thread::id second_thread;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        if (std::this_thread::get_id() == first_thread) {
          first_listener->armed = true;
        } else if (std::this_thread::get_id() == second_thread) {
          second_listener->armed = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::future<Status> first_result = std::async(std::launch::async, [&]() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      first_thread = std::this_thread::get_id();
    }
    return db_->ApplyLsmEdit(LsmEditOptions(), {first});
  });
  std::future<Status> second_result = std::async(std::launch::async, [&]() {
    {
      std::lock_guard<std::mutex> lock(mutex);
      second_thread = std::this_thread::get_id();
    }
    return other->ApplyLsmEdit(LsmEditOptions(), {second});
  });
  const Status first_status = AwaitBounded(first_result);
  const Status second_status = AwaitBounded(second_result);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(first_status);
  ASSERT_OK(second_status);
  ASSERT_TRUE(first_listener->result.has_value());
  ASSERT_TRUE(second_listener->result.has_value());
  EXPECT_TRUE(first_listener->result->IsNotSupported());
  EXPECT_TRUE(second_listener->result->IsNotSupported());
  EXPECT_EQ(1, first_listener->ingestions.load());
  EXPECT_EQ(1, second_listener->ingestions.load());
  EXPECT_EQ("first", Get("a1"));
  std::string value;
  ASSERT_OK(other->Get(ReadOptions(), "g1", &value));
  EXPECT_EQ("second", value);
  EXPECT_EQ("NOT_FOUND", Get("t1"));
  EXPECT_TRUE(other->Get(ReadOptions(), "p1", &value).IsNotFound());
  ASSERT_OK(other->ApplyLsmEdit(LsmEditOptions(), {first_listener->edit}));
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {second_listener->edit}));
  EXPECT_EQ("inner-first", Get("t1"));
  ASSERT_OK(other->Get(ReadOptions(), "p1", &value));
  EXPECT_EQ("inner-second", value);
  ASSERT_OK(other->Close());
}

TEST_F(LsmEditTest, PartiallySharedListenerLeaseRejectsNestedEdit) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  const std::shared_ptr<NestedLeaseEditListener> listener =
      std::make_shared<NestedLeaseEditListener>();
  const std::shared_ptr<OrderedFileIOListener> shared =
      std::make_shared<OrderedFileIOListener>();
  options.listeners = {listener, shared};
  DestroyAndReopen(options);
  Options other_options = options;
  other_options.create_if_missing = true;
  other_options.listeners = {shared};
  const std::string other_name = dbname_ + "_partial_lease";
  source_dbnames_.push_back(other_name);
  ASSERT_OK(DestroyDB(other_name, other_options));
  std::unique_ptr<DB> other;
  ASSERT_OK(DB::Open(other_options, other_name, &other));
  listener->target = other.get();
  listener->edit =
      MakeEdit(other->DefaultColumnFamily(), "p", "q",
               {MakeFile("partial_inner.sst", 6, {{"p1", "inner"}})});
  listener->edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEdit outer =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n",
               {MakeFile("partial_outer.sst", 5, {{"m1", "outer"}})});
  outer.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply",
      [&](void*) { listener->armed = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  const Status outer_status = RunBounded(
      [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {outer}); });
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(outer_status);
  ASSERT_EQ(1, listener->calls.load());
  ASSERT_TRUE(listener->result.has_value());
  EXPECT_EQ(Status::Code::kNotSupported, listener->result->code());
  EXPECT_EQ(1, listener->ingestions.load());
  EXPECT_EQ("outer", Get("m1"));
  std::string value;
  EXPECT_TRUE(other->Get(ReadOptions(), "p1", &value).IsNotFound());
  ASSERT_OK(env_->FileExists(listener->edit.add_files[0].path));
  ASSERT_OK(other->ApplyLsmEdit(LsmEditOptions(), {listener->edit}));
  ASSERT_OK(other->Get(ReadOptions(), "p1", &value));
  EXPECT_EQ("inner", value);
  ASSERT_OK(other->Put(WriteOptions(), "post", "write"));
  ASSERT_OK(other->Close());
}

TEST_F(LsmEditTest, StallCallbackRejectsItsOwnListenerLease) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 10;
  options.level_compaction_dynamic_level_bytes = false;
  const std::shared_ptr<NestedLeaseEditListener> listener =
      std::make_shared<NestedLeaseEditListener>();
  listener->callback_kind = NestedLeaseEditListener::CallbackKind::kStall;
  options.listeners = {listener};
  DestroyAndReopen(options);
  Options other_options = options;
  other_options.create_if_missing = true;
  other_options.disable_auto_compactions = true;
  const std::string other_name = dbname_ + "_stall_lease";
  source_dbnames_.push_back(other_name);
  ASSERT_OK(DestroyDB(other_name, other_options));
  std::unique_ptr<DB> other;
  ASSERT_OK(DB::Open(other_options, other_name, &other));
  listener->target = other.get();
  listener->edit =
      MakeEdit(other->DefaultColumnFamily(), "g", "h",
               {MakeFile("stall_inner.sst", 6, {{"g1", "inner"}})});
  listener->edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  listener->edit_options.allow_blocking_flush = false;
  LsmEdit outer =
      MakeEdit(db_->DefaultColumnFamily(), "a", "z",
               {MakeFile("stall_outer_m.sst", 0, {{"m1", "outer-m"}}),
                MakeFile("stall_outer_p.sst", 0, {{"p1", "outer-p"}})});
  outer.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  SyncPoint::GetInstance()->LoadDependency({
      {"LsmEditTest::ReleaseStallCompaction", "BackgroundCallCompaction:0"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  listener->armed = true;
  LsmEditOptions edit_options;
  edit_options.allow_blocking_flush = false;
  const Status outer_status =
      RunBounded([&]() { return db_->ApplyLsmEdit(edit_options, {outer}); });
  TEST_SYNC_POINT("LsmEditTest::ReleaseStallCompaction");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(outer_status);
  ASSERT_EQ(1, listener->calls.load());
  ASSERT_TRUE(listener->result.has_value());
  EXPECT_EQ(Status::Code::kNotSupported, listener->result->code());
  EXPECT_EQ(2, listener->ingestions.load());
  EXPECT_EQ("outer-m", Get("m1"));
  EXPECT_EQ("outer-p", Get("p1"));
  ASSERT_OK(env_->FileExists(listener->edit.add_files[0].path));
  ASSERT_OK(other->ApplyLsmEdit(listener->edit_options, {listener->edit}));
  std::string value;
  ASSERT_OK(other->Get(ReadOptions(), "g1", &value));
  EXPECT_EQ("inner", value);
  EXPECT_EQ(3, listener->ingestions.load());
  ASSERT_OK(other->Put(WriteOptions(), "post", "write"));
  ASSERT_OK(other->Close());
}

TEST_F(LsmEditTest, FlushCallbackRejectsLeaseDependentNestedEdit) {
  for (bool notify_file_io : {false, true}) {
    SCOPED_TRACE(notify_file_io);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    const std::shared_ptr<NestedLeaseEditListener> listener =
        std::make_shared<NestedLeaseEditListener>();
    listener->callback_kind =
        NestedLeaseEditListener::CallbackKind::kFlushBegin;
    listener->notify_file_io = notify_file_io;
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    const std::string other_name = dbname_ + "_flush_callback_target";
    ASSERT_OK(DestroyDB(other_name, options));
    source_dbnames_.push_back(other_name);
    std::unique_ptr<DB> other;
    ASSERT_OK(DB::Open(options, other_name, &other));
    listener->source = db_.get();
    listener->target = other.get();
    listener->edit =
        MakeEdit(other->DefaultColumnFamily(), "x", "y",
                 {MakeFile("flush_nested.sst", 0, {{"x1", "inner"}})});
    listener->edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    const LsmEdit outer =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("flush_outer.sst", 0, {{"m1", "new"}})});
    ASSERT_OK(Put("m1", "old"));
    listener->armed = true;
    ASSERT_OK(RunBounded(
        [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {outer}); }));
    ASSERT_EQ(1, listener->calls.load());
    ASSERT_TRUE(listener->result.has_value());
    EXPECT_EQ(Status::kNotSupported, listener->result->code());
    EXPECT_EQ("new", Get("m1"));
    std::string value;
    EXPECT_EQ(Status::kNotFound,
              other->Get(ReadOptions(), "x1", &value).code());
    ASSERT_OK(env_->FileExists(listener->edit.add_files.front().path));
    ASSERT_OK(other->ApplyLsmEdit(listener->edit_options, {listener->edit}));
    ASSERT_OK(other->Get(ReadOptions(), "x1", &value));
    EXPECT_EQ("inner", value);
    ASSERT_OK(Put("post", "write"));
    ASSERT_OK(other->Put(WriteOptions(), "post", "write"));
    ASSERT_OK(other->Close());
  }
}

TEST_F(LsmEditTest, LifecycleCallbackWithoutLeaseAllowsNestedEdit) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  const std::shared_ptr<NestedLeaseEditListener> listener =
      std::make_shared<NestedLeaseEditListener>();
  listener->callback_kind = NestedLeaseEditListener::CallbackKind::kIngested;
  listener->notify_file_io = false;
  options.listeners.push_back(listener);
  DestroyAndReopen(options);
  const std::string other_name = dbname_ + "_unleased_callback_target";
  ASSERT_OK(DestroyDB(other_name, options));
  source_dbnames_.push_back(other_name);
  std::unique_ptr<DB> other;
  ASSERT_OK(DB::Open(options, other_name, &other));
  listener->source = db_.get();
  listener->target = other.get();
  listener->edit =
      MakeEdit(other->DefaultColumnFamily(), "x", "y",
               {MakeFile("unleased_nested.sst", 6, {{"x1", "inner"}})});
  listener->edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEdit outer =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n",
               {MakeFile("unleased_outer.sst", 5, {{"m1", "outer"}})});
  outer.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  listener->armed = true;
  ASSERT_OK(RunBounded(
      [&]() { return db_->ApplyLsmEdit(LsmEditOptions(), {outer}); }));
  ASSERT_EQ(1, listener->calls.load());
  ASSERT_TRUE(listener->result.has_value());
  ASSERT_OK(*listener->result);
  EXPECT_EQ("outer", Get("m1"));
  std::string value;
  ASSERT_OK(other->Get(ReadOptions(), "x1", &value));
  EXPECT_EQ("inner", value);
  ASSERT_OK(other->Close());
}

TEST_F(LsmEditTest, FileIOCallbacksRejectReentrantEditsWithoutHanging) {
  for (int kind = 0; kind < 4; ++kind) {
    SCOPED_TRACE(kind);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    const std::shared_ptr<EditOnFileIOListener> listener =
        std::make_shared<EditOnFileIOListener>();
    listener->kind = kind;
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "seed"));
    ASSERT_OK(Flush());
    listener->db = db_.get();
    listener->edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("reentrant_io.sst", 5, {{"m1", "imported"}})});
    listener->edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    const LsmEdit outer =
        MakeEdit(db_->DefaultColumnFamily(), "p", "q",
                 {MakeFile("outer_io.sst", 5, {{"p1", "outer"}})});
    if (kind == 1 || kind == 2) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
            listener->armed.store(true);
            if (kind == 2) {
              env_->manifest_write_error_.store(true);
            }
          });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    listener->armed.store(kind == 0 || kind == 3);
    std::string value;
    const Status status = RunBounded([&]() {
      if (kind == 0) {
        ReadOptions read;
        read.fill_cache = false;
        return db_->Get(read, "a", &value);
      }
      return db_->ApplyLsmEdit(LsmEditOptions(), {outer});
    });
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(1, listener->calls.load());
    EXPECT_TRUE(listener->result.IsNotSupported())
        << listener->result.ToString();
    EXPECT_NE(std::string::npos, listener->result.ToString().find("callback"));
    ASSERT_OK(env_->FileExists(listener->edit.add_files[0].path));
    EXPECT_EQ("NOT_FOUND", Get("m1"));
    if (kind == 2) {
      ASSERT_NOK(status);
      EXPECT_EQ(before, FileNumbersPerLevel());
      Reopen(options);
      listener->db = db_.get();
      listener->edit.column_family = db_->DefaultColumnFamily();
    } else {
      ASSERT_OK(status);
      if (kind == 0) {
        EXPECT_EQ("seed", value);
        EXPECT_EQ(before, FileNumbersPerLevel());
      } else {
        EXPECT_EQ("outer", Get("p1"));
      }
    }
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {listener->edit}));
    EXPECT_EQ("imported", Get("m1"));
    ASSERT_OK(Put("post", "write"));
  }
}

TEST_F(LsmEditTest, DeletionJobDestructionDoesNotRaceSetOptions) {
  for (bool manifest_failure : {false, true}) {
    SCOPED_TRACE(manifest_failure);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.periodic_compaction_seconds = 0;
    DestroyAndReopen(options);
    ASSERT_OK(Put("m1", "old"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(2);
    ASSERT_OK(Put("z1", "keep"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(2);
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(2, metadata.levels[2].files.size());
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.delete_files = {{2, metadata.levels[2].files.front().file_number}};
    std::mutex state_mutex;
    std::condition_variable state_cv;
    bool destructor_paused = false;
    bool updater_ready = false;
    bool release = false;
    SyncPoint::GetInstance()->SetCallBack(
        "FileIngestionHandleImpl::~FileIngestionHandleImpl:"
        "BeforeJobDestruction",
        [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          destructor_paused = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return release; });
        });
    if (manifest_failure) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeLogAndApply",
          [&](void*) { env_->manifest_write_error_.store(true); });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    std::future<Status> edit_result = std::async(std::launch::async, [&]() {
      return db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    });
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return destructor_paused; });
    }
    std::future<Status> options_result = std::async(std::launch::async, [&]() {
      {
        std::unique_lock<std::mutex> lock(state_mutex);
        updater_ready = true;
        state_cv.notify_all();
        WaitBounded(state_cv, lock, [&]() { return release; });
      }
      for (int i = 0; i < 16; ++i) {
        const Status status = db_->SetOptions(
            {{"periodic_compaction_seconds", std::to_string(100 + i)}});
        if (!status.ok()) {
          return status;
        }
      }
      return Status::OK();
    });
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return updater_ready; });
      release = true;
      state_cv.notify_all();
    }
    const Status edit_status = AwaitBounded(edit_result);
    const Status options_status = AwaitBounded(options_result);
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(options_status);
    if (manifest_failure) {
      ASSERT_NOK(edit_status);
      Reopen(options);
    } else {
      ASSERT_OK(edit_status);
    }
    EXPECT_EQ(manifest_failure ? "old" : "NOT_FOUND", Get("m1"));
    EXPECT_EQ("keep", Get("z1"));
    ASSERT_OK(Put("post", "write"));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    EXPECT_EQ("keep", Get("z1"));
  }
}

void LsmEditTest::ExerciseLiveDeletionReservation(int level, int files,
                                                  bool manifest_failure) {
  struct Scenario {
    int level;
    int files;
    bool manifest_failure;
  };
  const Scenario scenario{level, files, manifest_failure};
  {
    SCOPED_TRACE(scenario.level);
    SCOPED_TRACE(scenario.files);
    SCOPED_TRACE(scenario.manifest_failure);
    Options options = CurrentOptions();
    options.num_levels = 4;
    options.disable_auto_compactions = true;
    options.max_background_compactions = 1;
    options.compression = kNoCompression;
    options.compression_per_level.clear();
    options.level_compaction_dynamic_level_bytes = false;
    options.level0_file_num_compaction_trigger = scenario.files == 1 ? 1 : 4;
    options.max_bytes_for_level_base = 1000000000;
    options.periodic_compaction_seconds = 0;
    env_->SetMockSleep(true);
    DestroyAndReopen(options);
    if (scenario.level == 0) {
      ASSERT_OK(Put("a", "base"));
      ASSERT_OK(Flush());
      MoveFilesToLevel(1);
    }
    for (int i = 0; i < scenario.files; ++i) {
      ASSERT_OK(Put("m1", "v" + std::to_string(i)));
      ASSERT_OK(Flush());
    }
    if (scenario.level > 0) {
      MoveFilesToLevel(scenario.level);
      env_->MockSleepForMicroseconds(20000000);
      ASSERT_OK(db_->SetOptions({{"periodic_compaction_seconds", "10"}}));
    }
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    const uint64_t target =
        metadata.levels[scenario.level].files.back().file_number;
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.delete_files = {{scenario.level, target}};
    std::mutex state_mutex;
    std::condition_variable state_cv;
    bool worker_queued = false;
    bool allow_picker = false;
    bool picker_finished = false;
    std::atomic<bool> installing{false};
    std::thread::id committing_thread;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BGWorkCompaction", [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          worker_queued = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return allow_picker; });
        });
    const auto& picker_done = [&](void*) {
      std::lock_guard<std::mutex> lock(state_mutex);
      picker_finished = true;
      state_cv.notify_all();
    };
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundCompaction():AfterPickCompaction", picker_done);
    SyncPoint::GetInstance()->SetCallBack("BackgroundCallCompaction:1",
                                          picker_done);
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply",
        [&](void*) { installing.store(true); });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifest", [&](void*) {
          if (!installing.load() ||
              std::this_thread::get_id() != committing_thread) {
            return;
          }
          std::unique_lock<std::mutex> lock(state_mutex);
          allow_picker = true;
          state_cv.notify_all();
          WaitBounded(state_cv, lock, [&]() { return picker_finished; });
          if (scenario.manifest_failure) {
            env_->manifest_write_error_.store(true);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
    {
      std::unique_lock<std::mutex> lock(state_mutex);
      WaitBounded(state_cv, lock, [&]() { return worker_queued; });
    }
    const Status status = RunBounded([&]() {
      committing_thread = std::this_thread::get_id();
      return db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    });
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (scenario.manifest_failure) {
      ASSERT_NOK(status);
      uint64_t pending = 0;
      ASSERT_TRUE(db_->GetIntProperty("rocksdb.compaction-pending", &pending));
      EXPECT_EQ(1, pending);
      EXPECT_EQ("v0", Get("m1"));
      Reopen(options);
    } else {
      ASSERT_OK(status);
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    if (scenario.files > 1) {
      EXPECT_EQ("v5", Get("m1"));
    } else if (scenario.manifest_failure) {
      EXPECT_EQ("v0", Get("m1"));
    } else {
      EXPECT_EQ("NOT_FOUND", Get("m1"));
    }
    ASSERT_OK(Put("post", "write"));
    EXPECT_EQ("write", Get("post"));
  }
}

TEST_F(LsmEditTest, L0DeletionReservationsExcludeQueuedAutomaticPickers) {
  ExerciseLiveDeletionReservation(0, 1, false);
  ExerciseLiveDeletionReservation(0, 6, false);
}

TEST_F(LsmEditTest, PeriodicDeletionReservationsRefreshCandidatesAndRollback) {
  ExerciseLiveDeletionReservation(2, 1, false);
  ExerciseLiveDeletionReservation(2, 1, true);
}

TEST_F(LsmEditTest, EqualL0RangeTombstoneCannotHideAnOlderPoint) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool survivor : {false, true}) {
    DestroyAndReopen(options);
    LsmEditFile point = MakeFile("l0_point.sst", 0, {{"m1", "point"}});
    if (survivor) {
      LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {point});
      first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
    }
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    LsmEditFile tombstone = MakeRangeDeletionFile("l0_range.sst", 0, "m", "n");
    LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 survivor ? std::vector<LsmEditFile>{tombstone}
                          : std::vector<LsmEditFile>{point, tombstone});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    const Status status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ(survivor ? "point" : "NOT_FOUND", Get("m1"));
  }
}

TEST_F(LsmEditTest, UnknownSurvivorStatisticsCannotBypassTombstoneValidation) {
  for (int max_open_files : {20, -1}) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.max_open_files = max_open_files;
    DestroyAndReopen(options);
    std::vector<LsmEditFile> files;
    for (int i = 0; i < 21; ++i) {
      const std::string key = "m" + std::to_string(100 + i);
      files.push_back(MakeFile(key + ".sst", 6, {{key, "point"}}));
    }
    LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "m", "n", files);
    first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
    std::vector<std::vector<FileMetaData>> metadata;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &metadata);
    std::string key = "m120";
    bool unknown = false;
    for (const FileMetaData& file : metadata[6]) {
      if (!file.init_stats_from_file) {
        key = file.smallest.user_key().ToString();
        unknown = true;
        break;
      }
    }
    if (max_open_files != -1) {
      ASSERT_TRUE(unknown);
    }
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    LsmEdit edit = MakeEdit(
        db_->DefaultColumnFamily(), "m", "n",
        {MakeRangeDeletionFile("unknown_stats.sst", 5, key, key + "~")});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    const Status status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ("point", Get(key));
  }
}

TEST_F(LsmEditTest, DeclaredLevelAllowsExclusiveRangeEndAdjacency) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool range_first : {false, true}) {
    DestroyAndReopen(options);
    const LsmEditFile point =
        MakeFile("adjacent_point.sst", 5, {{"m", "point"}});
    const LsmEditFile range =
        MakeRangeDeletionFile("adjacent_range.sst", 5, "a", "m");
    LsmEdit first = MakeEdit(db_->DefaultColumnFamily(), "a", "n",
                             {range_first ? range : point});
    first.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {first}));
    LsmEdit second = MakeEdit(db_->DefaultColumnFamily(), "a", "n",
                              {range_first ? point : range});
    second.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {second}));
    EXPECT_EQ(2, NumTableFilesAtLevel(5));
    EXPECT_EQ("point", Get("m"));
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    LsmEdit overlap = MakeEdit(db_->DefaultColumnFamily(), "a", "n",
                               {MakeFile("interior.sst", 5, {{"b", "bad"}})});
    overlap.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    EXPECT_TRUE(
        db_->ApplyLsmEdit(LsmEditOptions(), {overlap}).IsInvalidArgument());
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ("NOT_FOUND", Get("b"));
  }
}

TEST_F(LsmEditTest,
       WritePreparedBlockingFlushDoesNotWaitOnItsOwnSnapshotFence) {
  Close();
  for (bool two_queues : {false, true}) {
    for (bool atomic_flush : {false, true}) {
      for (bool active_user_snapshot : {false, true}) {
        SCOPED_TRACE(two_queues);
        SCOPED_TRACE(atomic_flush);
        SCOPED_TRACE(active_user_snapshot);
        Options options = CurrentOptions();
        options.disable_auto_compactions = true;
        options.two_write_queues = two_queues;
        options.atomic_flush = atomic_flush;
        ASSERT_OK(DestroyDB(dbname_, options));
        options.create_if_missing = true;
        TransactionDBOptions transaction_options;
        transaction_options.write_policy = WRITE_PREPARED;
        TransactionDB* raw_db = nullptr;
        ASSERT_OK(TransactionDB::Open(options, transaction_options, dbname_,
                                      &raw_db));
        std::unique_ptr<TransactionDB> transaction_db(raw_db);
        ASSERT_OK(transaction_db->Put(WriteOptions(), "m0", "old"));
        const Snapshot* user_snapshot =
            active_user_snapshot ? transaction_db->GetSnapshot() : nullptr;
        if (active_user_snapshot) {
          ASSERT_NE(nullptr, user_snapshot);
        }
        LsmEdit edit =
            MakeEdit(transaction_db->DefaultColumnFamily(), "m", "n",
                     {MakeFile("blocking_flush.sst", 0, {{"m1", "new"}})});
        std::mutex state_mutex;
        std::condition_variable state_cv;
        bool cleanup_paused = false;
        bool release_cleanup = false;
        bool cleanup_wait_seen = false;
        bool edit_finished = false;
        const auto& wait_for_installed_flush = [&](void*) {
          std::unique_lock<std::mutex> lock(state_mutex);
          WaitBounded(state_cv, lock, [&]() { return cleanup_paused; });
        };
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::FlushMemTable:BeforeWaitForBgFlush",
            wait_for_installed_flush);
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush",
            wait_for_installed_flush);
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCallFlush:FilesFound", [&](void*) {
              std::unique_lock<std::mutex> lock(state_mutex);
              cleanup_paused = true;
              state_cv.notify_all();
              WaitBounded(state_cv, lock, [&]() { return release_cleanup; });
            });
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::IngestExternalFile:WaitForFlushSnapshotCleanup",
            [&](void*) {
              std::lock_guard<std::mutex> lock(state_mutex);
              cleanup_wait_seen = true;
              state_cv.notify_all();
            });
        SyncPoint::GetInstance()->EnableProcessing();
        std::future<Status> result = std::async(std::launch::async, [&]() {
          const Status status =
              transaction_db->ApplyLsmEdit(LsmEditOptions(), {edit});
          std::lock_guard<std::mutex> lock(state_mutex);
          edit_finished = true;
          state_cv.notify_all();
          return status;
        });
        {
          std::unique_lock<std::mutex> lock(state_mutex);
          WaitBounded(state_cv, lock, [&]() { return cleanup_paused; });
          WaitBounded(state_cv, lock,
                      [&]() { return cleanup_wait_seen || edit_finished; });
          EXPECT_TRUE(cleanup_wait_seen);
          EXPECT_FALSE(edit_finished);
          release_cleanup = true;
          state_cv.notify_all();
        }
        if (result.wait_for(std::chrono::seconds(10)) !=
            std::future_status::ready) {
          ADD_FAILURE() << "LSM edit blocked its required WritePrepared flush";
          std::_Exit(1);
        }
        const Status status = result.get();
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();
        std::string value;
        if (active_user_snapshot) {
          EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
          EXPECT_NE(std::string::npos,
                    status.ToString().find("snapshots are active"));
          EXPECT_TRUE(
              transaction_db->Get(ReadOptions(), "m1", &value).IsNotFound());
          ASSERT_OK(transaction_db->Get(ReadOptions(), "m0", &value));
          EXPECT_EQ("old", value);
          transaction_db->ReleaseSnapshot(user_snapshot);
        } else {
          ASSERT_OK(status);
          ASSERT_OK(transaction_db->Get(ReadOptions(), "m1", &value));
          EXPECT_EQ("new", value);
        }
        ASSERT_OK(transaction_db->Put(WriteOptions(), "post", "write"));
        ASSERT_OK(transaction_db->Get(ReadOptions(), "post", &value));
        EXPECT_EQ("write", value);
        ASSERT_OK(transaction_db->Close());
      }
    }
  }
}

TEST_F(LsmEditTest, DroppedColumnFamilyStaysAliveAcrossJobSnapshotWait) {
  Close();
  for (TxnDBWritePolicy policy : {WRITE_PREPARED, WRITE_UNPREPARED}) {
    SCOPED_TRACE(policy);
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.listeners = {std::make_shared<EventListener>()};
    ASSERT_OK(DestroyDB(dbname_, options));
    TransactionDBOptions transaction_options;
    transaction_options.write_policy = policy;
    TransactionDB* raw = nullptr;
    ASSERT_OK(TransactionDB::Open(options, transaction_options, dbname_, &raw));
    std::unique_ptr<TransactionDB> db(raw);
    ColumnFamilyOptions cf_options(options);
    cf_options.disable_auto_compactions = false;
    cf_options.compaction_style = kCompactionStyleUniversal;
    cf_options.level0_file_num_compaction_trigger = 2;
    ColumnFamilyHandle* other = nullptr;
    ASSERT_OK(db->CreateColumnFamily(cf_options, "snapshot_wait", &other));
    const LsmEdit edit =
        MakeEdit(db->DefaultColumnFamily(), "m", "n",
                 {MakeFile("drop_snapshot_import.sst", 5, {{"m1", "new"}})});
    std::mutex mutex;
    std::condition_variable cv;
    std::thread::id compaction_thread;
    std::thread::id edit_thread;
    bool compaction_paused = false;
    bool release_compaction = false;
    bool edit_paused = false;
    bool release_edit = false;
    bool snapshot_waiting = false;
    SyncPoint::GetInstance()->SetCallBack(
        "BackgroundCallCompaction:0", [&](void*) {
          std::unique_lock<std::mutex> lock(mutex);
          compaction_thread = std::this_thread::get_id();
          compaction_paused = true;
          cv.notify_all();
          WaitBounded(cv, lock, [&]() { return release_compaction; });
        });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifestStart", [&](void*) {
          std::unique_lock<std::mutex> lock(mutex);
          if (std::this_thread::get_id() == edit_thread &&
              ScopedDeferredFileIOListeners::CaptureContext()) {
            edit_paused = true;
            cv.notify_all();
            WaitBounded(cv, lock, [&]() { return release_edit; });
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::GetSnapshotImpl:WaitForLsmEdit", [&](void*) {
          std::lock_guard<std::mutex> lock(mutex);
          if (std::this_thread::get_id() == compaction_thread) {
            snapshot_waiting = true;
            cv.notify_all();
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db->Put(WriteOptions(), other, "a", "first"));
    ASSERT_OK(db->Flush(FlushOptions(), other));
    ASSERT_OK(db->Put(WriteOptions(), other, "b", "second"));
    ASSERT_OK(db->Flush(FlushOptions(), other));
    {
      std::unique_lock<std::mutex> lock(mutex);
      SCOPED_TRACE("waiting for compaction entry");
      WaitBounded(cv, lock, [&]() { return compaction_paused; });
    }
    std::future<Status> applied = std::async(std::launch::async, [&]() {
      {
        std::lock_guard<std::mutex> lock(mutex);
        edit_thread = std::this_thread::get_id();
      }
      return db->ApplyLsmEdit(LsmEditOptions(), {edit});
    });
    {
      std::unique_lock<std::mutex> lock(mutex);
      SCOPED_TRACE("waiting for fenced MANIFEST write");
      WaitBounded(cv, lock, [&]() { return edit_paused; });
      release_compaction = true;
      cv.notify_all();
      SCOPED_TRACE("waiting for internal snapshot fence");
      WaitBounded(cv, lock, [&]() { return snapshot_waiting; });
    }
    std::future<Status> dropped = std::async(std::launch::async, [&]() {
      Status status = db->DropColumnFamily(other);
      if (status.ok()) {
        status = db->DestroyColumnFamilyHandle(other);
      }
      return status;
    });
    {
      std::lock_guard<std::mutex> lock(mutex);
      release_edit = true;
      cv.notify_all();
    }
    const Status applied_status = AwaitBounded(applied);
    const Status dropped_status = AwaitBounded(dropped);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(applied_status);
    ASSERT_OK(dropped_status);
    WaitForCompactOptions wait;
    ASSERT_OK(db->WaitForCompact(wait));
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), "m1", &value));
    EXPECT_EQ("new", value);
    ASSERT_OK(db->Close());
  }
}

TEST_F(LsmEditTest, BackgroundJobSnapshotsDoNotRejectUnrelatedImports) {
  Close();
  for (TxnDBWritePolicy policy : {WRITE_PREPARED, WRITE_UNPREPARED}) {
    for (bool user_snapshot : {false, true}) {
      SCOPED_TRACE(policy);
      SCOPED_TRACE(user_snapshot);
      Options options = CurrentOptions();
      options.disable_auto_compactions = true;
      options.create_if_missing = true;
      ASSERT_OK(DestroyDB(dbname_, options));
      TransactionDBOptions transaction_options;
      transaction_options.write_policy = policy;
      TransactionDB* raw = nullptr;
      ASSERT_OK(
          TransactionDB::Open(options, transaction_options, dbname_, &raw));
      std::unique_ptr<TransactionDB> db(raw);
      ColumnFamilyHandle* other = nullptr;
      ASSERT_OK(db->CreateColumnFamily(ColumnFamilyOptions(options), "other",
                                       &other));
      ASSERT_OK(db->Put(WriteOptions(), other, "z", "background"));
      const LsmEdit edit =
          MakeEdit(db->DefaultColumnFamily(), "m", "n",
                   {MakeFile("job_snapshot_import.sst", 5, {{"m1", "new"}})});
      const Snapshot* snapshot = user_snapshot ? db->GetSnapshot() : nullptr;
      std::mutex mutex;
      std::condition_variable cv;
      bool paused = false;
      bool release = false;
      bool cleanup_paused = false;
      bool release_cleanup = false;
      bool cleanup_wait_seen = false;
      SyncPoint::GetInstance()->SetCallBack(
          "FlushJob::WriteLevel0Table", [&](void*) {
            std::unique_lock<std::mutex> lock(mutex);
            paused = true;
            cv.notify_all();
            WaitBounded(cv, lock, [&]() { return release; });
          });
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::BackgroundCallFlush:FilesFound", [&](void*) {
            std::unique_lock<std::mutex> lock(mutex);
            cleanup_paused = true;
            cv.notify_all();
            WaitBounded(cv, lock, [&]() { return release_cleanup; });
          });
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::WaitForCompact:InsideLoop", [&](void*) {
            std::lock_guard<std::mutex> lock(mutex);
            cleanup_wait_seen = true;
            cv.notify_all();
          });
      SyncPoint::GetInstance()->EnableProcessing();
      FlushOptions background_flush;
      background_flush.wait = false;
      ASSERT_OK(db->Flush(background_flush, other));
      {
        std::unique_lock<std::mutex> lock(mutex);
        WaitBounded(cv, lock, [&]() { return paused; });
      }
      uint64_t snapshots = 0;
      EXPECT_TRUE(db->GetIntProperty("rocksdb.num-snapshots", &snapshots));
      EXPECT_EQ(user_snapshot ? 2 : 1, snapshots);
      const Status status = RunBounded(
          [&]() { return db->ApplyLsmEdit(LsmEditOptions(), {edit}); });
      EXPECT_TRUE(db->GetIntProperty("rocksdb.num-snapshots", &snapshots));
      EXPECT_EQ(user_snapshot ? 2 : 1, snapshots);
      {
        std::lock_guard<std::mutex> lock(mutex);
        release = true;
        cv.notify_all();
      }
      {
        std::unique_lock<std::mutex> lock(mutex);
        WaitBounded(cv, lock, [&]() { return cleanup_paused; });
      }
      const Status flushed =
          RunBounded([&]() { return db->Flush(FlushOptions(), other); });
      EXPECT_TRUE(db->GetIntProperty("rocksdb.num-snapshots", &snapshots));
      EXPECT_EQ(user_snapshot ? 2 : 1, snapshots);
      std::future<Status> drain = std::async(std::launch::async, [&]() {
        return db->WaitForCompact(WaitForCompactOptions());
      });
      {
        std::unique_lock<std::mutex> lock(mutex);
        WaitBounded(cv, lock, [&]() { return cleanup_wait_seen; });
        release_cleanup = true;
        cv.notify_all();
      }
      const Status drained = AwaitBounded(drain);
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      ASSERT_OK(flushed);
      ASSERT_OK(drained);
      EXPECT_TRUE(db->GetIntProperty("rocksdb.num-snapshots", &snapshots));
      EXPECT_EQ(user_snapshot ? 1 : 0, snapshots);
      std::string value;
      if (user_snapshot) {
        EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
        EXPECT_TRUE(db->Get(ReadOptions(), "m1", &value).IsNotFound());
        db->ReleaseSnapshot(snapshot);
        ASSERT_OK(db->ApplyLsmEdit(LsmEditOptions(), {edit}));
      } else {
        ASSERT_OK(status);
      }
      ASSERT_OK(db->Get(ReadOptions(), "m1", &value));
      EXPECT_EQ("new", value);
      ASSERT_OK(db->Get(ReadOptions(), other, "z", &value));
      EXPECT_EQ("background", value);
      ASSERT_OK(db->DestroyColumnFamilyHandle(other));
      ASSERT_OK(db->Close());
    }
  }
}

TEST_F(LsmEditTest, BlockingFlushWaitDoesNotIncludeLaterCleanup) {
  Close();
  const int original_workers = env_->GetBackgroundThreads(Env::HIGH);
  env_->SetBackgroundThreads(2, Env::HIGH);
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  ASSERT_OK(DestroyDB(dbname_, options));
  options.create_if_missing = true;
  TransactionDBOptions transaction_options;
  transaction_options.write_policy = WRITE_PREPARED;
  TransactionDB* raw = nullptr;
  ASSERT_OK(TransactionDB::Open(options, transaction_options, dbname_, &raw));
  std::unique_ptr<TransactionDB> db(raw);
  ColumnFamilyHandle* later_cf = nullptr;
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(options), "later", &later_cf));
  ASSERT_OK(db->Put(WriteOptions(), later_cf, "z", "later"));
  ASSERT_OK(db->Put(WriteOptions(), "m0", "old"));
  LsmEdit edit = MakeEdit(db->DefaultColumnFamily(), "m", "n",
                          {MakeFile("cleanup_limit.sst", 0, {{"m1", "new"}})});

  std::mutex mutex;
  std::condition_variable cv;
  std::thread::id later_thread;
  bool later_started = false;
  bool release_main_start = false;
  bool release_later_start = false;
  bool main_paused = false;
  bool release_main = false;
  bool later_cleaned = false;
  bool release_later_cleanup = false;
  bool wait_seen = false;
  int starts = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        if (++starts == 1) {
          SCOPED_TRACE("release main start");
          WaitBounded(cv, lock, [&]() { return release_main_start; });
        } else if (starts == 2) {
          later_thread = std::this_thread::get_id();
          later_started = true;
          cv.notify_all();
          SCOPED_TRACE("release later start");
          WaitBounded(cv, lock, [&]() { return release_later_start; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:FilesFound", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        if (std::this_thread::get_id() != later_thread) {
          main_paused = true;
          cv.notify_all();
          SCOPED_TRACE("release main cleanup");
          WaitBounded(cv, lock, [&]() { return release_main; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:SnapshotCleanedUp", [&](void*) {
        std::unique_lock<std::mutex> lock(mutex);
        if (std::this_thread::get_id() == later_thread) {
          later_cleaned = true;
          cv.notify_all();
          SCOPED_TRACE("release later cleanup");
          WaitBounded(cv, lock, [&]() { return release_later_cleanup; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFile:WaitForFlushSnapshotCleanup", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        wait_seen = true;
        cv.notify_all();
      });
  const auto& await_main_flush = [&](void*) {
    SCOPED_TRACE("main flush installation");
    std::unique_lock<std::mutex> lock(mutex);
    release_main_start = true;
    cv.notify_all();
    WaitBounded(cv, lock, [&]() { return main_paused; });
  };
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions flush;
  flush.wait = false;
  ASSERT_OK(db->Flush(flush));
  ASSERT_OK(db->Flush(flush, later_cf));
  {
    SCOPED_TRACE("setup later worker");
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return later_started; });
  }
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTable:BeforeWaitForBgFlush", await_main_flush);
  std::future<Status> result = std::async(std::launch::async, [&]() {
    return db->ApplyLsmEdit(LsmEditOptions(), {edit});
  });
  {
    SCOPED_TRACE("edit cleanup wait and later cleanup");
    std::unique_lock<std::mutex> lock(mutex);
    WaitBounded(cv, lock, [&]() { return wait_seen; });
    release_later_start = true;
    cv.notify_all();
    WaitBounded(cv, lock, [&]() { return later_cleaned; });
    release_main = true;
    cv.notify_all();
  }
  const bool completed =
      result.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  EXPECT_TRUE(completed)
      << "Edit waited for a flush started after its cleanup fence";
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_later_cleanup = true;
    cv.notify_all();
  }
  ASSERT_OK(AwaitBounded(result));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "m1", &value));
  EXPECT_EQ("new", value);
  ASSERT_OK(db->Get(ReadOptions(), later_cf, "z", &value));
  EXPECT_EQ("later", value);
  ASSERT_OK(db->DestroyColumnFamilyHandle(later_cf));
  ASSERT_OK(db->Close());
  env_->SetBackgroundThreads(original_workers, Env::HIGH);
}

TEST_F(LsmEditTest, RejectsEqualSequenceTombstoneAbovePoint) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeRangeDeletionFile("tombstone.sst", 5, "m", "n"),
                           MakeFile("point.sst", 6, {{"m1", "point"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  const Status s = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos,
            s.ToString().find("invalid sequence number ordering"));
}

TEST_F(LsmEditTest, RejectsActiveSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_NE(nullptr, snapshot);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("snapshot.sst", 6, {{"m1", "value"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  EXPECT_NE(std::string::npos, s.ToString().find("snapshots are active"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, SnapshotsWaitForManifestInstallation) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool timestamped : {false, true}) {
    for (bool manifest_failure : {false, true}) {
      SCOPED_TRACE(timestamped);
      SCOPED_TRACE(manifest_failure);
      DestroyAndReopen(options);
      const LsmEdit edit =
          MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                   {MakeFile("snapshot_install.sst", 6, {{"m1", "value"}})});

      SyncPoint::GetInstance()->LoadDependency({
          {"DBImpl::IngestExternalFiles:BeforeLogAndApply",
           "LsmEditTest::BeforeSnapshot"},
      });
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
            env_->manifest_write_error_.store(manifest_failure);
            SyncPoint::GetInstance()->LoadDependency({
                {"VersionSet::LogAndApply:WriteManifestStart",
                 "LsmEditTest::BeforeSnapshot"},
                {"DBImpl::GetSnapshotImpl:WaitForLsmEdit",
                 "VersionSet::LogAndApply:WriteManifest"},
            });
          });
      SyncPoint::GetInstance()->EnableProcessing();
      std::thread snapshot_thread([&]() {
        TEST_SYNC_POINT("LsmEditTest::BeforeSnapshot");
        std::shared_ptr<const Snapshot> timestamped_snapshot;
        ReadOptions read_options;
        if (timestamped) {
          auto result =
              dbfull()->CreateTimestampedSnapshot(kMaxSequenceNumber, 1);
          EXPECT_OK(result.first);
          timestamped_snapshot = std::move(result.second);
          read_options.snapshot = timestamped_snapshot.get();
        } else {
          read_options.snapshot = db_->GetSnapshot();
        }
        EXPECT_NE(nullptr, read_options.snapshot);
        std::string value;
        const Status read_status = db_->Get(read_options, "m1", &value);
        if (manifest_failure) {
          EXPECT_EQ(Status::kNotFound, read_status.code());
        } else {
          EXPECT_OK(read_status);
          EXPECT_EQ("value", value);
        }
        if (!timestamped) {
          db_->ReleaseSnapshot(read_options.snapshot);
        }
      });
      const Status status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
      snapshot_thread.join();
      env_->manifest_write_error_.store(false);
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->ClearTrace();
      if (manifest_failure) {
        ASSERT_EQ(Status::kIOError, status.code());
      } else {
        ASSERT_OK(status);
      }
    }
  }
}

TEST_F(LsmEditTest, RejectsFileOutsideRange) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const Status s = db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeFile("out.sst", 6, {{"z1", "v"}})})});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(LsmEditTest, RejectsBadArguments) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  DestroyAndReopen(options);

  EXPECT_TRUE(db_->ApplyLsmEdit(LsmEditOptions(), {}).IsInvalidArgument());

  LsmEditOptions conflicting_file_actions;
  conflicting_file_actions.move_files = true;
  conflicting_file_actions.link_files = true;
  const Status conflicting_actions = db_->ApplyLsmEdit(
      conflicting_file_actions,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("conflicting_actions.sst", 6, {{"m1", "v"}})})});
  ASSERT_TRUE(conflicting_actions.IsInvalidArgument())
      << conflicting_actions.ToString();
  EXPECT_NE(std::string::npos,
            conflicting_actions.ToString().find(
                "move_files` and `link_files` can not both be true"));

  LsmEdit no_files;
  no_files.column_family = db_->DefaultColumnFamily();
  no_files.range = RangeOpt(Slice("m"), Slice("n"));
  EXPECT_TRUE(
      db_->ApplyLsmEdit(LsmEditOptions(), {no_files}).IsInvalidArgument());

  LsmEdit half_range;
  half_range.column_family = db_->DefaultColumnFamily();
  half_range.range.start = Slice("m");
  half_range.add_files.push_back(MakeFile("half.sst", 6, {{"m1", "v"}}));
  EXPECT_TRUE(
      db_->ApplyLsmEdit(LsmEditOptions(), {half_range}).IsNotSupported());

  const Status bad_level = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("deep.sst", options.num_levels, {{"m1", "v"}})})});
  ASSERT_TRUE(bad_level.IsInvalidArgument()) << bad_level.ToString();
  EXPECT_NE(std::string::npos, bad_level.ToString().find("outside [0, 7)"));
}

TEST_F(LsmEditTest, FifoCompactionRequiresLevelZero) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.disable_auto_compactions = true;
  options.max_open_files = -1;
  DestroyAndReopen(options);

  const Status nonzero_level = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("fifo_l1.sst", 1, {{"m1", "bad"}})})});
  ASSERT_TRUE(nonzero_level.IsInvalidArgument()) << nonzero_level.ToString();
  EXPECT_NE(std::string::npos, nonzero_level.ToString().find("FIFO"));

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                {MakeFile("fifo_l0.sst", 0, {{"m1", "good"}})})}));
  EXPECT_EQ("good", Get("m1"));
  EXPECT_EQ("1", FilesPerLevel());
}

TEST_F(LsmEditTest, RejectsIncompleteChecksums) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  LsmEditFile checksum_only = MakeFile("checksum_only.sst", 6, {{"m1", "v"}});
  checksum_only.checksum = "checksum";
  const Status missing_func = db_->ApplyLsmEdit(
      LsmEditOptions(),
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", {checksum_only})});
  ASSERT_TRUE(missing_func.IsInvalidArgument()) << missing_func.ToString();
  EXPECT_NE(std::string::npos,
            missing_func.ToString().find("must both be set or both be empty"));

  LsmEditFile checksum_and_func = checksum_only;
  checksum_and_func.checksum_func_name = "checksum_func";
  LsmEditFile no_checksum = MakeFile("no_checksum.sst", 5, {{"m2", "v"}});
  const Status partial = db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {checksum_and_func, no_checksum})});
  ASSERT_TRUE(partial.IsInvalidArgument()) << partial.ToString();
  EXPECT_NE(std::string::npos,
            partial.ToString().find("checksums for all files or none"));
}

// Transplant a whole multi-level LSM, overlapping level 0 and all, from a
// separate DB. This is what lets a bulk loader skip flattening its scratch
// instance into a single sorted run before handing the files over.
TEST_F(LsmEditTest, GraftLsmBuiltInAnotherDb) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string src_dbname = dbname_ + "_source";
  ASSERT_OK(DestroyDB(src_dbname, options));
  std::vector<LsmEditFile> files;
  std::string src_shape;
  {
    Options src_options = options;
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src;
    ASSERT_OK(DB::Open(src_options, src_dbname, &src));

    // Bottom level: the bulk of the data.
    for (int i = 0; i < 8; i++) {
      ASSERT_OK(src->Put(WriteOptions(), "m" + std::to_string(i), "base"));
    }
    ASSERT_OK(src->Flush(FlushOptions()));
    ASSERT_OK(src->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Two overlapping level 0 runs on top of it, oldest written first.
    ASSERT_OK(src->Put(WriteOptions(), "m3", "middle"));
    ASSERT_OK(src->Flush(FlushOptions()));
    ASSERT_OK(src->Put(WriteOptions(), "m3", "newest"));
    ASSERT_OK(src->Put(WriteOptions(), "m5", "newest"));
    ASSERT_OK(src->Flush(FlushOptions()));

    ColumnFamilyMetaData src_meta;
    src->GetColumnFamilyMetaData(&src_meta);
    for (const auto& level : src_meta.levels) {
      std::vector<LsmEditFile> level_files;
      for (const auto& file : level.files) {
        LsmEditFile edit_file;
        edit_file.path = file.db_path + file.name;
        edit_file.level = level.level;
        level_files.push_back(std::move(edit_file));
      }
      // Level 0 metadata is newest first, but ApplyLsmEdit() treats later
      // entries as newer, so put the oldest run first.
      if (level.level == 0) {
        std::reverse(level_files.begin(), level_files.end());
      }
      for (auto& edit_file : level_files) {
        files.push_back(std::move(edit_file));
      }
    }
    ASSERT_EQ(3, files.size());
    src_shape = ShapeOf(src.get());
    ASSERT_OK(src->Close());
  }

  LsmEditOptions edit_options;
  edit_options.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(
      edit_options,
      {MakeEdit(db_->DefaultColumnFamily(), "m", "n", std::move(files))}));

  EXPECT_EQ("base", Get("m0"));
  EXPECT_EQ("newest", Get("m3"));
  EXPECT_EQ("newest", Get("m5"));
  // The scratch instance's shape came across as-is: same files, same levels,
  // no compaction on either side.
  EXPECT_EQ(src_shape, ShapeOf(db_.get()));
}

TEST_F(LsmEditTest, SurvivesReopen) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(db_->ApplyLsmEdit(
      LsmEditOptions(), {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                                  {MakeFile("graft.sst", 4, {{"m1", "v"}})})}));
  Reopen(options);

  EXPECT_EQ("v", Get("m1"));
  EXPECT_EQ("0,0,0,0,1", FilesPerLevel());
}

TEST_F(LsmEditTest, SurvivingSingleDeleteCannotBypassTieValidation) {
  Options options = CurrentOptions();
  options.num_levels = 4;
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  for (bool merge : {false, true}) {
    SCOPED_TRACE(merge);
    DestroyAndReopen(options);
    ASSERT_OK(Put("k", "old"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(3);
    ASSERT_OK(db_->SingleDelete(WriteOptions(), "k"));
    ASSERT_OK(Flush());
    const auto before = FileNumbersPerLevel();
    const SequenceNumber sequence = db_->GetLatestSequenceNumber();
    const std::string name = dbname_ + "_survivor_delete_source";
    ASSERT_OK(DestroyDB(name, options));
    source_dbnames_.push_back(name);
    Options source_options = options;
    source_options.create_if_missing = true;
    std::unique_ptr<DB> source;
    ASSERT_OK(DB::Open(source_options, name, &source));
    ASSERT_OK(source->Put(WriteOptions(), "j", "padding"));
    ASSERT_OK(source->Flush(FlushOptions()));
    ASSERT_OK(source->Put(WriteOptions(), "k", "new"));
    ASSERT_OK(source->Flush(FlushOptions()));
    ColumnFamilyMetaData metadata;
    source->GetColumnFamilyMetaData(&metadata);
    const auto& input = metadata.levels[0].files.front();
    LsmEditFile file;
    file.path = input.db_path + input.name;
    file.level = 0;
    ASSERT_OK(source->Close());
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.add_files = {file};
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = merge;
    const Status status = db_->ApplyLsmEdit(edit_options, {edit});
    ASSERT_TRUE(status.IsNotSupported()) << status.ToString();
    EXPECT_NE(std::string::npos, status.ToString().find("SingleDelete"));
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ(sequence, db_->GetLatestSequenceNumber());
    EXPECT_EQ("NOT_FOUND", Get("k"));
    std::vector<std::string> compact_inputs;
    db_->GetColumnFamilyMetaData(&metadata);
    for (const auto& live : metadata.levels[0].files) {
      compact_inputs.push_back(live.name);
    }
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), db_->DefaultColumnFamily(),
                                compact_inputs, 1));
    EXPECT_EQ("NOT_FOUND", Get("k"));
  }
}

TEST_F(LsmEditTest, SourcePriorityRequirementSurvivesManifestRotation) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  options.max_manifest_file_size = 1;
  DestroyAndReopen(options);
  const auto required = [&]() {
    dbfull()->TEST_LockMutex();
    const bool value =
        static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
            ->cfd()
            ->current()
            ->storage_info()
            ->RequiresSourcePriorityForEqualSequences();
    dbfull()->TEST_UnlockMutex();
    return value;
  };
  LsmEdit edit = MakeEdit(
      db_->DefaultColumnFamily(), "m", "n",
      {MakePointFile("priority_delete.sst", 4, kTypeDeletion, "m1", ""),
       MakeFile("priority_old.sst", 6, {{"m1", "old"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_TRUE(required());
  for (int round = 0; round < 3; ++round) {
    ASSERT_OK(Put("x", "rotate"));
    ASSERT_OK(Flush());
    Reopen(options);
    EXPECT_TRUE(required());
    EXPECT_EQ("NOT_FOUND", Get("m1"));
  }
  CompactRangeOptions compact;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  EXPECT_FALSE(required());
  ASSERT_OK(Put("z", "rotate"));
  ASSERT_OK(Flush());
  Reopen(options);
  EXPECT_FALSE(required());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, GeneratedFilesSyncTheirColumnFamilyDirectory) {
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    const auto state = std::make_shared<DirectorySyncState>();
    state->cf_path = dbname_ + "_separate_cf";
    const auto fs =
        std::make_shared<DirectorySyncFileSystem>(env_->GetFileSystem(), state);
    CompositeEnvWrapper composite(env_, fs);
    Options options = CurrentOptions();
    options.env = &composite;
    options.cf_paths = {DbPath(state->cf_path, 0)};
    options.disable_auto_compactions = true;
    Defer close_composite_db([this]() {
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      Close();
      last_options_.env = env_;
      last_options_.cf_paths.clear();
    });
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "left"));
    ASSERT_OK(Put("z", "right"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(6);
    state->created = 0;
    state->synced = 0;
    bool checked = false;
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifestStart", [&](void*) {
          const uint64_t created = state->created.load();
          if (created != 0) {
            checked = true;
            EXPECT_GT(state->synced.load(), created);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    LsmEditOptions edit_options;
    if (mode == 2) {
      edit.level_splits = {{6, {"m", "n"}}};
    } else {
      edit.range = RangeOpt("m", "n");
      edit.add_files = {
          MakeFile("directory_input.sst", mode == 0 ? 5 : 6, {{"m1", "new"}})};
      edit_options.merge_incoming_files = true;
      edit_options.split_existing_files = mode == 1;
    }
    const Status status = db_->ApplyLsmEdit(edit_options, {edit});
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(status);
    EXPECT_TRUE(checked);
    EXPECT_EQ("left", Get("a"));
    EXPECT_EQ("right", Get("z"));
    if (mode != 2) {
      EXPECT_EQ("new", Get("m1"));
    }
    Close();
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_OK(DestroyDir(env_, state->cf_path));
  }
}

TEST_F(LsmEditTest, SingleDeleteTiesAreRejectedBeforeHistoryIsLost) {
  Options options = CurrentOptions();
  options.num_levels = 4;
  options.level_compaction_dynamic_level_bytes = false;
  options.disable_auto_compactions = true;
  for (bool merge : {false, true}) {
    for (bool tied : {true, false}) {
      SCOPED_TRACE(merge);
      SCOPED_TRACE(tied);
      DestroyAndReopen(options);
      LsmEdit seed;
      seed.column_family = db_->DefaultColumnFamily();
      seed.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      seed.add_files = {MakeDbFile("single_delete_old", 3, {{"k", "old"}})};
      ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {seed}));
      const auto before = FileNumbersPerLevel();
      const SequenceNumber before_sequence = db_->GetLatestSequenceNumber();
      const auto make_input = [&](const std::string& name, bool single_delete,
                                  SequenceNumber sequence) {
        const std::string source_name = dbname_ + "_" + name;
        EXPECT_OK(DestroyDB(source_name, options));
        source_dbnames_.push_back(source_name);
        Options source_options = options;
        source_options.create_if_missing = true;
        std::unique_ptr<DB> source;
        EXPECT_OK(DB::Open(source_options, source_name, &source));
        if (!source) {
          return LsmEditFile();
        }
        EXPECT_OK(source->Put(WriteOptions(), "k", "old"));
        while (source->GetLatestSequenceNumber() + 1 < sequence) {
          EXPECT_OK(source->Put(WriteOptions(), "j", "padding"));
        }
        EXPECT_OK(source->Flush(FlushOptions()));
        EXPECT_OK(single_delete ? source->SingleDelete(WriteOptions(), "k")
                                : source->Put(WriteOptions(), "k", "new"));
        EXPECT_OK(source->Flush(FlushOptions()));
        ColumnFamilyMetaData metadata;
        source->GetColumnFamilyMetaData(&metadata);
        const auto& file = metadata.levels[0].files.front();
        LsmEditFile result;
        result.path = file.db_path + file.name;
        result.level = 0;
        EXPECT_OK(source->Close());
        return result;
      };
      LsmEdit edit;
      edit.column_family = db_->DefaultColumnFamily();
      edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
      edit.add_files = {make_input("single_delete_barrier", true, 2),
                        make_input("single_delete_new", false, tied ? 2 : 3)};
      LsmEditOptions edit_options;
      edit_options.merge_incoming_files = merge;
      const Status status = db_->ApplyLsmEdit(edit_options, {edit});
      if (tied) {
        ASSERT_TRUE(status.IsNotSupported()) << status.ToString();
        EXPECT_NE(std::string::npos, status.ToString().find("SingleDelete"));
        EXPECT_EQ(before, FileNumbersPerLevel());
        EXPECT_EQ(before_sequence, db_->GetLatestSequenceNumber());
        EXPECT_EQ("old", Get("k"));
      } else {
        ASSERT_OK(status);
        EXPECT_EQ("new", Get("k"));
        ASSERT_OK(db_->SingleDelete(WriteOptions(), "k"));
        ASSERT_OK(Flush());
        ColumnFamilyMetaData metadata;
        db_->GetColumnFamilyMetaData(&metadata);
        std::vector<std::string> inputs;
        for (const auto& file : metadata.levels[0].files) {
          inputs.push_back(file.name);
        }
        ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                                    db_->DefaultColumnFamily(), inputs, 1));
        EXPECT_EQ("NOT_FOUND", Get("k"));
        EXPECT_EQ(before[3], FileNumbersPerLevel()[3]);
        Reopen(options);
        EXPECT_EQ("NOT_FOUND", Get("k"));
      }
      for (const auto& file : edit.add_files) {
        ASSERT_OK(env_->FileExists(file.path));
      }
    }
  }
}

TEST_F(LsmEditTest, RewritesRetainFreshTablePropertiesOwners) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.table_factory = std::make_shared<FreshPropertiesTableFactory>();
  for (bool merge_existing : {false, true}) {
    SCOPED_TRACE(merge_existing);
    DestroyAndReopen(options);
    if (merge_existing) {
      ASSERT_OK(Put("a", "left"));
      ASSERT_OK(Put("z", "right"));
      ASSERT_OK(Flush());
      MoveFilesToLevel(6);
    }
    LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("fresh_properties.sst", 6, {{"m1", "new"}})});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.merge_existing_files = merge_existing;
    ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
    EXPECT_EQ("new", Get("m1"));
    if (merge_existing) {
      EXPECT_EQ("left", Get("a"));
      EXPECT_EQ("right", Get("z"));
    }
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    ASSERT_EQ(1, metadata.size());
    EXPECT_EQ(12299, metadata.front().oldest_ancestor_time);
    Reopen(options);
    EXPECT_EQ("new", Get("m1"));
  }
}

TEST_F(LsmEditTest, MergedInputWatermarkSurvivesReopen) {
  for (bool atomic_group : {false, true}) {
    for (bool survivor : {false, true}) {
      for (int prior_writes : {0, 8}) {
        SCOPED_TRACE(survivor);
        SCOPED_TRACE(prior_writes);
        SCOPED_TRACE(atomic_group);
        Options options = CurrentOptions();
        options.disable_auto_compactions = true;
        options.optimize_manifest_for_recovery = false;
        DestroyAndReopen(options);
        if (atomic_group) {
          CreateAndReopenWithCF({"other"}, options);
        }
        for (int i = 0; i < prior_writes; ++i) {
          ASSERT_OK(Put("d" + std::to_string(i), "destination"));
        }
        if (prior_writes > 0) {
          ASSERT_OK(Flush());
        }
        const uint64_t original_files = CountLiveFiles();
        const std::string source_name = dbname_ + "_merged_sequence_source";
        ASSERT_OK(DestroyDB(source_name, options));
        source_dbnames_.push_back(source_name);
        Options source_options = options;
        source_options.create_if_missing = true;
        std::unique_ptr<DB> source;
        ASSERT_OK(DB::Open(source_options, source_name, &source));
        if (survivor) {
          ASSERT_OK(source->Put(WriteOptions(), "a", "survivor"));
        }
        ASSERT_OK(source->Put(WriteOptions(), "k", "value"));
        ASSERT_OK(source->Flush(FlushOptions()));
        ASSERT_OK(source->SingleDelete(WriteOptions(), "k"));
        ASSERT_OK(source->Flush(FlushOptions()));
        const SequenceNumber source_max = survivor ? 3 : 2;
        ASSERT_EQ(source_max, source->GetLatestSequenceNumber());
        ColumnFamilyMetaData metadata;
        source->GetColumnFamilyMetaData(&metadata);
        ASSERT_EQ(2, metadata.levels[0].files.size());
        LsmEdit edit;
        edit.column_family = db_->DefaultColumnFamily();
        edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
        for (auto it = metadata.levels[0].files.rbegin();
             it != metadata.levels[0].files.rend(); ++it) {
          LsmEditFile file;
          file.path = it->db_path + it->name;
          file.level = 0;
          edit.add_files.push_back(std::move(file));
        }
        ASSERT_OK(source->Close());
        LsmEditOptions edit_options;
        edit_options.merge_incoming_files = true;
        std::vector<LsmEdit> edits{edit};
        if (atomic_group) {
          LsmEdit other = MakeEdit(
              handles_[1], "x", "y",
              {MakeFile("sequence_group_other.sst", 6, {{"x1", "other"}})});
          other.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
          edits.push_back(std::move(other));
          if (survivor) {
            std::reverse(edits.begin(), edits.end());
          }
        }
        const SequenceNumber expected =
            std::max(source_max, static_cast<SequenceNumber>(prior_writes));
        ASSERT_OK(db_->ApplyLsmEdit(edit_options, edits));
        EXPECT_EQ("NOT_FOUND", Get("k"));
        EXPECT_EQ(survivor ? "survivor" : "NOT_FOUND", Get("a"));
        EXPECT_EQ(original_files + (survivor ? 1 : 0) + (atomic_group ? 1 : 0),
                  CountLiveFiles());
        EXPECT_EQ(expected, db_->GetLatestSequenceNumber());
        for (const LsmEditFile& file : edit.add_files) {
          ASSERT_OK(env_->FileExists(file.path));
        }
        if (atomic_group) {
          ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other"},
                                   options);
          EXPECT_EQ("other", Get(1, "x1"));
        } else {
          Reopen(options);
        }
        EXPECT_EQ(expected, db_->GetLatestSequenceNumber());
        EXPECT_EQ("NOT_FOUND", Get("k"));
        EXPECT_EQ(survivor ? "survivor" : "NOT_FOUND", Get("a"));
        if (prior_writes > 0) {
          EXPECT_EQ("destination", Get("d0"));
        }
        ASSERT_OK(Put("post", "write"));
        EXPECT_EQ(expected + 1, db_->GetLatestSequenceNumber());
        EXPECT_EQ("write", Get("post"));
      }
    }
  }
}

TEST_F(LsmEditTest, MergesOnlyIncomingFilesAtDeclaredLevel) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "outside"));
  ASSERT_OK(Put("z", "outside"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  const auto older =
      MakeFile("merge_older.sst", 5, {{"m1", "old"}, {"m2", "keep"}});
  const auto newer =
      MakeFile("merge_newer.sst", 5, {{"m1", "new"}, {"m3", "add"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {older, newer});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_EQ("keep", Get("m2"));
  EXPECT_EQ("add", Get("m3"));
  EXPECT_EQ("outside", Get("a"));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_OK(env_->FileExists(older.path));
  EXPECT_OK(env_->FileExists(newer.path));
  Reopen(options);
  EXPECT_EQ("new", Get("m1"));
}

TEST_F(LsmEditTest, MergeDoesNotResolveDestinationOverlap) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "outside"));
  ASSERT_OK(Put("z", "outside"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);
  const auto before = FileNumbersPerLevel();
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("overlap.sst", 5, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  EXPECT_EQ(before, FileNumbersPerLevel());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, SplitLevelSupportsThreeFourAndFiveCuts) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (size_t count : {3, 4, 5}) {
    DestroyAndReopen(options);
    std::vector<std::pair<std::string, std::string>> expected;
    for (int index = 100; index < 110; ++index) {
      const auto key = std::to_string(index) + "/k";
      ASSERT_OK(Put(key, key));
      expected.emplace_back(key, key);
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(6);
    const auto before = FileNumbersPerLevel();
    std::vector<std::string> cuts{"102/", "104/", "106/", "108/", "109/"};
    cuts.resize(count);
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.level_splits.push_back({6, cuts});
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
    ASSERT_EQ(static_cast<int>(count + 1), NumTableFilesAtLevel(6));
    const auto after = FileNumbersPerLevel();
    EXPECT_EQ(after[6].end(),
              std::find(after[6].begin(), after[6].end(), before[6].front()));
    ExpectVisibleEntries(expected);
    Reopen(options);
    ExpectVisibleEntries(expected);
  }
}

TEST_F(LsmEditTest, SplitLevelAppliesCutsAcrossAllSstsAndAddsIndex102) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  initial.add_files = {
      MakeFile("level_101_105.sst", 6, {{"101/a", "101"}, {"105/a", "105"}}),
      MakeFile("level_106_107.sst", 6, {{"106/a", "106"}, {"107/a", "107"}}),
      MakeFile("level_110.sst", 6, {{"110/a", "110"}})};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
  const auto before = FileNumbersPerLevel();
  auto incoming = MakeFile("level_102.sst", 6, {{"102/a", "102"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "102/", "103/", {incoming});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits.push_back({6, {"101/", "105/", "106/", "107/"}});
  LsmEditOptions split;
  split.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  ASSERT_EQ(6, NumTableFilesAtLevel(6));
  const auto after = FileNumbersPerLevel();
  EXPECT_EQ(after[6].end(),
            std::find(after[6].begin(), after[6].end(), before[6][0]));
  EXPECT_EQ(after[6].end(),
            std::find(after[6].begin(), after[6].end(), before[6][1]));
  EXPECT_NE(after[6].end(),
            std::find(after[6].begin(), after[6].end(), before[6][2]));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  bool found = false;
  for (const auto& file : metadata.levels[6].files) {
    if (file.smallestkey == "102/a") {
      bool same = false;
      ASSERT_OK(
          env_->AreFilesSame(incoming.path, file.db_path + file.name, &same));
      EXPECT_TRUE(same);
      found = true;
    }
  }
  EXPECT_TRUE(found);
  ExpectVisibleEntries({{"101/a", "101"},
                        {"102/a", "102"},
                        {"105/a", "105"},
                        {"106/a", "106"},
                        {"107/a", "107"},
                        {"110/a", "110"}});
}

TEST_F(LsmEditTest, SplitLevelMinimalCutsKeepOtherIndexesTogether) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  for (const auto& key : {"101/a", "105/a", "106/a", "107/a"}) {
    ASSERT_OK(Put(key, key));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "100/", "108/",
                       {MakeFile("minimal_102.sst", 6, {{"102/a", "102"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits.push_back({6, {"101/", "105/"}});
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  ASSERT_EQ(3, NumTableFilesAtLevel(6));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  EXPECT_EQ("105/a", metadata.levels[6].files.back().smallestkey);
  EXPECT_EQ("107/a", metadata.levels[6].files.back().largestkey);
  ExpectVisibleEntries({{"101/a", "101/a"},
                        {"102/a", "102"},
                        {"105/a", "105/a"},
                        {"106/a", "106/a"},
                        {"107/a", "107/a"}});
}

TEST_F(LsmEditTest,
       SplitLevelPreservesCrossingTombstonesSnapshotsAndMemtableWrites) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  for (const auto& key : {"a", "b", "d", "f", "h", "z"}) {
    ASSERT_OK(Put(key, key));
  }
  ASSERT_OK(Flush());
  ManagedSnapshot snapshot(db_.get());
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "h"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact;
  compact.change_level = true;
  compact.target_level = 6;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  ASSERT_OK(Put("a", "new"));
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits.push_back({6, {"c", "e", "g"}});
  LsmEditOptions split;
  split.allow_blocking_flush = false;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  EXPECT_EQ(4, NumTableFilesAtLevel(6));
  EXPECT_EQ(0, NumTableFilesAtLevel(0));
  ExpectVisibleEntries({{"a", "new"}, {"h", "h"}, {"z", "z"}});
  ReadOptions old;
  old.snapshot = snapshot.snapshot();
  for (const auto& key : {"a", "b", "d", "f", "h", "z"}) {
    std::string value;
    ASSERT_OK(db_->Get(old, key, &value));
    EXPECT_EQ(key, value);
  }
}

TEST_F(LsmEditTest, SplitLevelRejectsInvalidCutsAndModes) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "a"));
  ASSERT_OK(Put("z", "z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  const std::vector<std::vector<LsmEditLevelSplit>> invalid{
      {{0, {"m"}}},      {{options.num_levels, {"m"}}},
      {{6, {}}},         {{6, {"n", "m"}}},
      {{6, {"m", "m"}}}, {{6, {"m"}}, {6, {"n"}}}};
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  for (const auto& cuts : invalid) {
    edit.level_splits = cuts;
    const auto status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(before, FileNumbersPerLevel());
  }
  edit.level_splits = {{6, {"m"}}};
  edit.deletion_mode = LsmEditDeletionMode::kFilesInRange;
  EXPECT_TRUE(db_->ApplyLsmEdit(LsmEditOptions(), {edit}).IsInvalidArgument());
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions mixed;
  mixed.merge_incoming_files = true;
  mixed.merge_existing_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(mixed, {edit}).IsInvalidArgument());
  mixed.merge_existing_files = false;
  mixed.split_existing_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(mixed, {edit}).IsInvalidArgument());
  ExpectVisibleEntries({{"a", "a"}, {"z", "z"}});
}

TEST_F(LsmEditTest, SplitLevelRejectsStaleOriginalFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "a"));
  ASSERT_OK(Put("z", "z"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits = {{6, {"m"}}};
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::SplitLevels:Selected", [&](void*) {
        CompactRangeOptions compact;
        compact.change_level = true;
        compact.target_level = 6;
        compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
        ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const auto status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(status.IsTryAgain()) << status.ToString();
  ExpectVisibleEntries({{"a", "a"}, {"z", "z"}});
}

TEST_F(LsmEditTest, SplitLevelRejectsNewlyStraddlingFile) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a0", "a0"));
  ASSERT_OK(Put("a9", "a9"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits = {{6, {"a5", "m"}}};
  const auto late =
      MakeFile("late_level_file.sst", 6, {{"l", "l"}, {"z", "z"}});
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::SplitLevels:Selected", [&](void*) {
        IngestExternalFileOptions ingestion;
        ingestion.fail_if_not_bottommost_level = true;
        ASSERT_OK(db_->IngestExternalFile({late.path}, ingestion));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const auto status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(status.IsTryAgain()) << status.ToString();
  ExpectVisibleEntries({{"a0", "a0"}, {"a9", "a9"}, {"l", "l"}, {"z", "z"}});
}

TEST_F(LsmEditTest, SplitLevelAlignedFilesStayUntouchedAndLastKeyGoesRight) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  initial.add_files = {
      MakeFile("already_left.sst", 6, {{"a", "a"}, {"b", "b"}}),
      MakeFile("already_right.sst", 6, {{"d", "d"}, {"e", "e"}})};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
  const auto before = FileNumbersPerLevel();
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits = {{6, {"a", "c", "d", "z"}}};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_EQ(before, FileNumbersPerLevel());
  edit.level_splits = {{6, {"b"}}};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_EQ(3, NumTableFilesAtLevel(6));
  ExpectVisibleEntries({{"a", "a"}, {"b", "b"}, {"d", "d"}, {"e", "e"}});
}

TEST_F(LsmEditTest, SplitLevelKeepsTombstoneOnlyPiecesCoveringDeeperData) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("d", "old-d"));
  ASSERT_OK(Put("n", "old-n"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "y"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits = {{5, {"c", "m", "p"}}};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_EQ(4, NumTableFilesAtLevel(5));
  EXPECT_EQ(1, NumTableFilesAtLevel(6));
  EXPECT_EQ("NOT_FOUND", Get("d"));
  EXPECT_EQ("NOT_FOUND", Get("n"));
  ASSERT_OK(Put("d", "new-d"));
  EXPECT_EQ("new-d", Get("d"));
}

TEST_F(LsmEditTest, SplitLevelReservesCutsWhileManifestMutexIsReleased) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (bool rewrite : {false, true}) {
    DestroyAndReopen(options);
    LsmEdit initial;
    initial.column_family = db_->DefaultColumnFamily();
    initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    if (rewrite) {
      initial.add_files.push_back(
          MakeFile("reserve_ad.sst", 6, {{"a", "a"}, {"d", "d"}}));
    }
    initial.add_files.push_back(MakeFile("reserve_m.sst", 6, {{"m", "m"}}));
    initial.add_files.push_back(MakeFile("reserve_z.sst", 6, {{"z", "z"}}));
    ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    std::vector<std::string> compact_inputs;
    for (const auto& file : metadata.levels[6].files) {
      if (file.smallestkey == "m" || file.smallestkey == "z") {
        compact_inputs.push_back(file.name);
      }
    }
    LsmEdit edit;
    edit.column_family = db_->DefaultColumnFamily();
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edit.level_splits = {{6, rewrite ? std::vector<std::string>{"c", "t"}
                                     : std::vector<std::string>{"t"}}};
    std::atomic<bool> installing{false};
    std::atomic<int> attempts{0};
    const auto committing_thread = std::this_thread::get_id();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::IngestExternalFiles:BeforeLogAndApply",
        [&](void*) { installing.store(true); });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifest", [&](void*) {
          if (!installing.load() ||
              std::this_thread::get_id() != committing_thread) {
            return;
          }
          ++attempts;
          const auto status =
              db_->CompactFiles(CompactionOptions(), db_->DefaultColumnFamily(),
                                compact_inputs, 6);
          EXPECT_TRUE(status.IsAborted()) << status.ToString();
        });
    SyncPoint::GetInstance()->EnableProcessing();
    const auto status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(status);
    EXPECT_EQ(1, attempts.load());
    EXPECT_EQ(rewrite ? 4 : 2, NumTableFilesAtLevel(6));
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), db_->DefaultColumnFamily(),
                                compact_inputs, 6));
    if (rewrite) {
      ExpectVisibleEntries({{"a", "a"}, {"d", "d"}, {"m", "m"}, {"z", "z"}});
    } else {
      ExpectVisibleEntries({{"m", "m"}, {"z", "z"}});
    }
  }
}

TEST_F(LsmEditTest, SplitLevelInsufficientCutsDoNotOverwriteExistingData) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  for (const auto& key : {"101/a", "103/a", "105/a"}) {
    ASSERT_OK(Put(key, key));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  auto edit =
      MakeEdit(db_->DefaultColumnFamily(), "102/", "103/",
               {MakeFile("insufficient_102.sst", 6, {{"102/a", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits = {{6, {"105/"}}};
  const auto status = db_->ApplyLsmEdit(LsmEditOptions(), {edit});
  ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
  EXPECT_EQ(before, FileNumbersPerLevel());
  ExpectVisibleEntries(
      {{"101/a", "101/a"}, {"103/a", "103/a"}, {"105/a", "105/a"}});
}

TEST_F(LsmEditTest, SplitLevelDoesNotResurrectExplicitlyDeletedFiles) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  initial.add_files = {
      MakeFile("delete_101_105.sst", 6, {{"101/a", "101"}, {"105/a", "105"}}),
      MakeFile("keep_106_107.sst", 6, {{"106/a", "106"}, {"107/a", "107"}})};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files = {{6, FileNumbersPerLevel()[6][0]}};
  edit.level_splits = {{6, {"105/", "106/", "107/"}}};
  edit.add_files = {MakeFile("delete_add_102.sst", 6, {{"102/a", "102"}})};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_EQ(3, NumTableFilesAtLevel(6));
  ExpectVisibleEntries({{"102/a", "102"}, {"106/a", "106"}, {"107/a", "107"}});
}

TEST_F(LsmEditTest, SplitLevelUsesComparatorOrderedCutPoints) {
  Options options = CurrentOptions();
  options.comparator = ReverseBytewiseComparator();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  for (const auto& key : {"z", "n1", "m1", "l1", "g", "a"}) {
    ASSERT_OK(Put(key, key));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  LsmEdit edit;
  edit.column_family = db_->DefaultColumnFamily();
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.level_splits = {{6, {"n", "m", "h"}}};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {edit}));
  EXPECT_EQ(4, NumTableFilesAtLevel(6));
  ExpectVisibleEntries({{"z", "z"},
                        {"n1", "n1"},
                        {"m1", "m1"},
                        {"l1", "l1"},
                        {"g", "g"},
                        {"a", "a"}});
}

TEST_F(LsmEditTest, SplitLevelAndAdditionsCommitAcrossColumnFamilies) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"other"}, options);
  for (int cf : {0, 1}) {
    ASSERT_OK(Put(cf, "101/a", "101"));
    ASSERT_OK(Put(cf, "105/a", "105"));
    ASSERT_OK(Flush(cf));
    MoveFilesToLevel(6, cf);
  }
  std::vector<LsmEdit> edits(2);
  for (int cf : {0, 1}) {
    edits[cf].column_family = handles_[cf];
    edits[cf].deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    edits[cf].level_splits = {{6, {"105/"}}};
  }
  edits[0].add_files = {MakeFile("two_cf_102.sst", 6, {{"102/a", "102"}})};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), edits));
  EXPECT_EQ(3, NumTableFilesAtLevel(6, 0));
  EXPECT_EQ(2, NumTableFilesAtLevel(6, 1));
  for (int cf : {0, 1}) {
    EXPECT_EQ("101", Get(cf, "101/a"));
    EXPECT_EQ("105", Get(cf, "105/a"));
  }
  EXPECT_EQ("102", Get(0, "102/a"));
  EXPECT_EQ("NOT_FOUND", Get(1, "102/a"));
}

TEST_F(LsmEditTest, SplitExistingL6KeepsIncomingSstUnchanged) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("100/a", "index-100"));
  ASSERT_OK(Put("105/a", "index-105"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  const auto incoming =
      MakeFile("split_102.sst", 6, {{"102/a", "first"}, {"102/z", "last"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "102/", "103/", {incoming});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions split;
  split.split_existing_files = true;
  split.link_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  ASSERT_EQ(3, NumTableFilesAtLevel(6));
  const auto after = FileNumbersPerLevel();
  EXPECT_EQ(after[6].end(),
            std::find(after[6].begin(), after[6].end(), before[6].front()));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  bool found_incoming = false;
  for (const auto& file : metadata.levels[6].files) {
    if (file.smallestkey == "102/a") {
      bool same = false;
      ASSERT_OK(
          env_->AreFilesSame(incoming.path, file.db_path + file.name, &same));
      EXPECT_TRUE(same);
      found_incoming = true;
    }
  }
  EXPECT_TRUE(found_incoming);
  const std::vector<std::pair<std::string, std::string>> expected{
      {"100/a", "index-100"},
      {"102/a", "first"},
      {"102/z", "last"},
      {"105/a", "index-105"}};
  ExpectVisibleEntries(expected);
  Reopen(options);
  ExpectVisibleEntries(expected);
}

TEST_F(LsmEditTest, SplitLeavesDisjointDestinationFilesUntouched) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  LsmEdit initial;
  initial.column_family = db_->DefaultColumnFamily();
  initial.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  initial.add_files = {MakeFile("split_left.sst", 6, {{"a", "left"}}),
                       MakeFile("split_middle.sst", 6, {{"j", "middle"}}),
                       MakeFile("split_right.sst", 6, {{"z", "right"}})};
  ASSERT_OK(db_->ApplyLsmEdit(LsmEditOptions(), {initial}));
  const auto before = FileNumbersPerLevel();
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("split_gap.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions split;
  split.split_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  ASSERT_EQ(4, NumTableFilesAtLevel(6));
  const auto after = FileNumbersPerLevel();
  for (auto number : before[6]) {
    EXPECT_NE(after[6].end(),
              std::find(after[6].begin(), after[6].end(), number));
  }
  ExpectVisibleEntries(
      {{"a", "left"}, {"j", "middle"}, {"m1", "new"}, {"z", "right"}});
}

TEST_F(LsmEditTest, SplitCanMergeIncomingSnapshotAndEventsSeparately) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  auto snapshot = MakeDbFile("split_snapshot", 6,
                             {{"m1", "snapshot"}, {"m2", "unchanged"}});
  snapshot.zero_sequence_numbers = true;
  auto edit =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n",
               {snapshot, MakeDbFile("split_events", 6, {{"m1", "online"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions split;
  split.split_existing_files = true;
  split.merge_incoming_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  EXPECT_EQ(3, NumTableFilesAtLevel(6));
  ExpectVisibleEntries(
      {{"a", "left"}, {"m1", "online"}, {"m2", "unchanged"}, {"z", "right"}});
}

TEST_F(LsmEditTest, SplitRespectsReverseComparator) {
  Options options = CurrentOptions();
  options.comparator = ReverseBytewiseComparator();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("z", "first"));
  ASSERT_OK(Put("a", "last"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  LsmEditFile incoming;
  incoming.path = sst_files_dir_ + "split_reverse.sst";
  incoming.level = 6;
  {
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(incoming.path));
    ASSERT_OK(writer.Put("m5", "new"));
    ASSERT_OK(writer.Finish());
  }
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "n", "m", {incoming});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions split;
  split.split_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  EXPECT_EQ(3, NumTableFilesAtLevel(6));
  ExpectVisibleEntries({{"z", "first"}, {"m5", "new"}, {"a", "last"}});
}

TEST_F(LsmEditTest, SplitRejectsEntriesAndCoveringTombstones) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (int kind : {0, 1, 2}) {
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "left"));
    ASSERT_OK(Put("z", "right"));
    if (kind == 1) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "b", "y"));
    } else {
      ASSERT_OK(Put("m5", "existing"));
      if (kind == 2) {
        ASSERT_OK(Delete("m5"));
      }
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(6);
    const auto before = FileNumbersPerLevel();
    auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                         {MakeFile("split_conflict.sst", 6, {{"m1", "new"}})});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    LsmEditOptions split;
    split.split_existing_files = true;
    const auto status = db_->ApplyLsmEdit(split, {edit});
    ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ("NOT_FOUND", Get("m1"));
    EXPECT_EQ("left", Get("a"));
    EXPECT_EQ("right", Get("z"));
  }
}

TEST_F(LsmEditTest, SplitPreservesOutsideTombstonesAndSnapshotHistory) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a1", "old"));
  ASSERT_OK(Put("b1", "before-left-delete"));
  ASSERT_OK(Put("x1", "before-right-delete"));
  ASSERT_OK(Put("z1", "right"));
  ASSERT_OK(Flush());
  ManagedSnapshot snapshot(db_.get());
  ASSERT_OK(Put("a1", "current"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "c"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "x", "y"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact;
  compact.change_level = true;
  compact.target_level = 6;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("split_history.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions split;
  split.split_existing_files = true;
  split.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  EXPECT_EQ(3, NumTableFilesAtLevel(6));
  ExpectVisibleEntries({{"a1", "current"}, {"m1", "new"}, {"z1", "right"}});
  ReadOptions old;
  old.snapshot = snapshot.snapshot();
  for (const auto& [key, expected] :
       std::map<std::string, std::string>{{"a1", "old"},
                                          {"b1", "before-left-delete"},
                                          {"x1", "before-right-delete"},
                                          {"z1", "right"}}) {
    std::string value;
    ASSERT_OK(db_->Get(old, key, &value));
    EXPECT_EQ(expected, value);
  }
}

TEST_F(LsmEditTest, SplitPreservesManifestSequenceOverrides) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("zz", "unrelated"));
  ASSERT_OK(Flush());
  ManagedSnapshot snapshot(db_.get());
  const auto external =
      MakeFile("split_global.sst", 6, {{"a", "left"}, {"z", "right"}});
  ASSERT_OK(
      db_->IngestExternalFile({external.path}, IngestExternalFileOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("split_global_new.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions split;
  split.split_existing_files = true;
  split.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(split, {edit}));
  EXPECT_EQ(3, NumTableFilesAtLevel(6));
  ReadOptions old;
  old.snapshot = snapshot.snapshot();
  std::string value;
  EXPECT_TRUE(db_->Get(old, "a", &value).IsNotFound());
  EXPECT_TRUE(db_->Get(old, "z", &value).IsNotFound());
  ExpectVisibleEntries(
      {{"a", "left"}, {"m1", "new"}, {"z", "right"}, {"zz", "unrelated"}});
}

TEST_F(LsmEditTest, SplitRejectsStaleDestination) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("split_stale.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions split;
  split.split_existing_files = true;
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::SplitExisting:Selected", [&](void*) {
        CompactRangeOptions compact;
        compact.change_level = true;
        compact.target_level = 6;
        compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
        ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const auto status = db_->ApplyLsmEdit(split, {edit});
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(status.IsTryAgain()) << status.ToString();
  ExpectVisibleEntries({{"a", "left"}, {"z", "right"}});
}

TEST_F(LsmEditTest, SplitCommitsOrRollsBackEveryColumnFamily) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"other"}, options);
  for (int cf : {0, 1}) {
    ASSERT_OK(Put(cf, "a", "left"));
    ASSERT_OK(Put(cf, "z", "right"));
    if (cf == 1) {
      ASSERT_OK(Put(cf, "m5", "conflict"));
    }
    ASSERT_OK(Flush(cf));
    MoveFilesToLevel(6, cf);
  }
  std::vector<LsmEdit> edits{
      MakeEdit(handles_[0], "m", "n",
               {MakeFile("split_cf0.sst", 6, {{"m1", "new0"}})}),
      MakeEdit(handles_[1], "m", "n",
               {MakeFile("split_cf1.sst", 6, {{"m1", "new1"}})})};
  for (auto& edit : edits) {
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  }
  LsmEditOptions split;
  split.split_existing_files = true;
  const auto status = db_->ApplyLsmEdit(split, edits);
  ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
  for (int cf : {0, 1}) {
    EXPECT_EQ(1, NumTableFilesAtLevel(6, cf));
    EXPECT_EQ("NOT_FOUND", Get(cf, "m1"));
    EXPECT_EQ("left", Get(cf, "a"));
    EXPECT_EQ("right", Get(cf, "z"));
  }
  EXPECT_EQ("conflict", Get(1, "m5"));
  for (int cf : {0, 1}) {
    edits[cf].range = RangeOpt("n", "o");
    edits[cf].add_files = {
        MakeFile("split_success_cf" + std::to_string(cf) + ".sst", 6,
                 {{"n1", "installed"}})};
  }
  ASSERT_OK(db_->ApplyLsmEdit(split, edits));
  for (int cf : {0, 1}) {
    EXPECT_EQ(3, NumTableFilesAtLevel(6, cf));
    EXPECT_EQ("installed", Get(cf, "n1"));
    EXPECT_EQ("NOT_FOUND", Get(cf, "m1"));
    EXPECT_EQ("left", Get(cf, "a"));
    EXPECT_EQ("right", Get(cf, "z"));
  }
  EXPECT_EQ("conflict", Get(1, "m5"));
}

TEST_F(LsmEditTest, SplitRequiresBoundedAddOnlyExplicitEdit) {
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("split_options.sst", 6, {{"m1", "new"}})});
  LsmEditOptions split;
  split.split_existing_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(split, {edit}).IsInvalidArgument());
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.range = RangeOpt();
  EXPECT_TRUE(db_->ApplyLsmEdit(split, {edit}).IsInvalidArgument());
  edit.range = RangeOpt("m", "n");
  edit.delete_files.push_back({6, 999});
  EXPECT_TRUE(db_->ApplyLsmEdit(split, {edit}).IsInvalidArgument());
  edit.delete_files.clear();
  split.merge_existing_files = true;
  split.merge_incoming_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(split, {edit}).IsInvalidArgument());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MergeExistingReplacesStraddlingL6File) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "outside-left"));
  ASSERT_OK(Put("m5", "retained-middle"));
  ASSERT_OK(Put("z", "outside-right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const auto before = FileNumbersPerLevel();
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("existing_merge_1.sst", 6, {{"m1", "one"}}),
                        MakeFile("existing_merge_2.sst", 6, {{"m2", "two"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("outside-left", Get("a"));
  EXPECT_EQ("retained-middle", Get("m5"));
  EXPECT_EQ("outside-right", Get("z"));
  EXPECT_EQ("one", Get("m1"));
  EXPECT_EQ("two", Get("m2"));
  EXPECT_EQ(1, NumTableFilesAtLevel(6));
  EXPECT_NE(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  Reopen(options);
  EXPECT_EQ("outside-left", Get("a"));
  EXPECT_EQ("retained-middle", Get("m5"));
  EXPECT_EQ("two", Get("m2"));
}

TEST_F(LsmEditTest, MergedFifoFilesKeepExpiryAges) {
  env_->SetMockSleep();
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  options.max_open_files = -1;
  options.disable_auto_compactions = true;
  options.ttl = 10;
  for (bool merge_existing : {false, true}) {
    DestroyAndReopen(options);
    if (merge_existing) {
      ASSERT_OK(Put("a", "left"));
      ASSERT_OK(Put("z", "right"));
      ASSERT_OK(Flush());
    }
    env_->MockSleepForSeconds(1);
    LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeDbFile("fifo_native", 0, {{"m1", "native"}}),
                  MakeFile("fifo_external.sst", 0, {{"m2", "external"}})});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.merge_existing_files = merge_existing;
    ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
    EXPECT_EQ("native", Get("m1"));
    EXPECT_EQ("external", Get("m2"));
    std::vector<std::vector<FileMetaData>> metadata;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &metadata);
    ASSERT_EQ(1, metadata[0].size());
    TablePropertiesCollection tables;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&tables));
    ASSERT_EQ(1, tables.size());
    const TableProperties& properties = *tables.begin()->second;
    EXPECT_GT(properties.newest_key_time, 0);
    EXPECT_GT(properties.file_creation_time, 0);
    EXPECT_GT(properties.oldest_ancestor_time, 0);
    EXPECT_EQ(properties.file_creation_time, metadata[0][0].file_creation_time);
    EXPECT_EQ(properties.oldest_ancestor_time,
              metadata[0][0].oldest_ancestor_time);
    Reopen(options);
    env_->MockSleepForSeconds(20);
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    EXPECT_EQ(0, NumTableFilesAtLevel(0));
    EXPECT_EQ("NOT_FOUND", Get("m1"));
    EXPECT_EQ("NOT_FOUND", Get("m2"));
    if (merge_existing) {
      EXPECT_EQ("NOT_FOUND", Get("a"));
      EXPECT_EQ("NOT_FOUND", Get("z"));
    }
  }
}

TEST_F(LsmEditTest, DestinationMergeRegistersGeneratedFileSize) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  ASSERT_NE(nullptr, options.sst_file_manager);
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  const std::unordered_map<std::string, uint64_t> original_files =
      options.sst_file_manager->GetTrackedFiles();
  ASSERT_EQ(1, original_files.size());
  std::unique_ptr<Iterator> original_iter(db_->NewIterator(ReadOptions()));
  original_iter->SeekToFirst();
  ASSERT_TRUE(original_iter->Valid());
  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                          {MakeFile("accounted.sst", 6, {{"m1", "incoming"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  const std::unordered_map<std::string, uint64_t> tracked =
      options.sst_file_manager->GetTrackedFiles();
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  uint64_t live_size = 0;
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      const std::string path = file.db_path + file.name;
      ASSERT_NE(tracked.end(), tracked.find(path));
      EXPECT_EQ(file.size, tracked.at(path));
      live_size += file.size;
    }
  }
  EXPECT_EQ(live_size + original_files.begin()->second,
            options.sst_file_manager->GetTotalSize());
  ASSERT_NE(tracked.end(), tracked.find(original_files.begin()->first));
  EXPECT_EQ(original_files.begin()->second,
            tracked.at(original_files.begin()->first));
  EXPECT_OK(env_->FileExists(original_files.begin()->first));
  original_iter->Seek("m1");
  ASSERT_TRUE(original_iter->Valid());
  EXPECT_EQ("z", original_iter->key().ToString());
  ASSERT_OK(original_iter->status());
  original_iter.reset();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:FilesFound",
        "DBImpl::FlushMemTable:BeforeWaitForBgFlush"},
       {"DBImpl::WaitForCompact:StartWaiting",
        "DBImpl::PurgeObsoleteFiles:Begin"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("zz", "trigger obsolete-file deletion"));
  ASSERT_OK(Flush());
  // Flush() waits for installation, not obsolete-file cleanup.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  db_->GetColumnFamilyMetaData(&metadata);
  live_size = 0;
  std::unordered_map<std::string, uint64_t> expected_tracked;
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      expected_tracked.emplace(file.db_path + file.name, file.size);
      live_size += file.size;
    }
  }
  EXPECT_EQ(expected_tracked, options.sst_file_manager->GetTrackedFiles());
  EXPECT_EQ(live_size, options.sst_file_manager->GetTotalSize());
  EXPECT_EQ(Status::kNotFound,
            env_->FileExists(original_files.begin()->first).code());
  EXPECT_EQ("incoming", Get("m1"));
  EXPECT_EQ("left", Get("a"));
  EXPECT_EQ("right", Get("z"));
}

TEST_F(LsmEditTest, FailedDestinationMergeUntracksGeneratedOutput) {
  for (int failure = 0; failure < 4; ++failure) {
    SCOPED_TRACE(failure);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.sst_file_manager.reset(NewSstFileManager(env_));
    ASSERT_NE(nullptr, options.sst_file_manager);
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "left"));
    ASSERT_OK(Put("z", "right"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(6);
    const std::unordered_map<std::string, uint64_t> before =
        options.sst_file_manager->GetTrackedFiles();
    if (failure == 0) {
      options.sst_file_manager->SetMaxAllowedSpaceUsage(
          options.sst_file_manager->GetTotalSize() + 1);
    } else if (failure == 1) {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::BuildMergedFile:BeforeOutputFileInfo",
          [](void* arg) {
            *static_cast<Status*>(arg) =
                Status::IOError("output metadata read");
          });
      SyncPoint::GetInstance()->EnableProcessing();
    } else if (failure == 2) {
      env_->manifest_write_error_.store(true);
    } else {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::IngestExternalFiles:BeforeJobsRun:0", [&](void*) {
            options.sst_file_manager->SetMaxAllowedSpaceUsage(
                options.sst_file_manager->GetTotalSize() - 1);
          });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    LsmEdit edit =
        MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                 {MakeFile("failed_accounted.sst", 6, {{"m1", "incoming"}})});
    edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.merge_existing_files = true;
    const Status status = db_->ApplyLsmEdit(edit_options, {edit});
    env_->manifest_write_error_.store(false);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_TRUE(failure == 0 || failure == 3
                    ? status.subcode() == Status::kSpaceLimit
                    : status.IsIOError())
        << status.ToString();
    EXPECT_EQ(before, options.sst_file_manager->GetTrackedFiles());
    EXPECT_EQ("left", Get("a"));
    EXPECT_EQ("right", Get("z"));
    EXPECT_EQ("NOT_FOUND", Get("m1"));
  }
}

TEST_F(LsmEditTest, MergeExistingIncludesGapBetweenIncomingRanges) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("m", "gap-data"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "a", "z",
                       {MakeFile("existing_gap_a.sst", 6, {{"b", "left"}}),
                        MakeFile("existing_gap_z.sst", 6, {{"x", "right"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("left", Get("b"));
  EXPECT_EQ("gap-data", Get("m"));
  EXPECT_EQ("right", Get("x"));
  EXPECT_EQ(1, NumTableFilesAtLevel(6));
}

TEST_F(LsmEditTest, MergeExistingRejectsStaleDestination) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("stale_existing.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::MergeExisting:Selected", [&](void*) {
        CompactRangeOptions compact;
        compact.change_level = true;
        compact.target_level = 6;
        compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
        ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const auto status = db_->ApplyLsmEdit(edit_options, {edit});
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(status.IsTryAgain()) << status.ToString();
  EXPECT_EQ("left", Get("a"));
  EXPECT_EQ("right", Get("z"));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MergeExistingRetainsSnapshotHistory) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("m5", "old"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("m5", "current"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact;
  compact.change_level = true;
  compact.target_level = 6;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("snapshot_existing.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  ReadOptions read;
  read.snapshot = snapshot;
  std::string value;
  EXPECT_OK(db_->Get(read, "m5", &value));
  EXPECT_EQ("old", value);
  EXPECT_EQ("current", Get("m5"));
  EXPECT_EQ("new", Get("m1"));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, MergeExistingPreservesManifestGlobalSequenceForSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("zz", "unrelated"));
  ASSERT_OK(Flush());
  ManagedSnapshot snapshot(db_.get());
  const auto external =
      MakeFile("manifest_global.sst", 6, {{"a", "left"}, {"z", "right"}});
  IngestExternalFileOptions ingestion;
  ASSERT_OK(db_->IngestExternalFile({external.path}, ingestion));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));
  ReadOptions old_read;
  old_read.snapshot = snapshot.snapshot();
  std::string value;
  EXPECT_TRUE(db_->Get(old_read, "a", &value).IsNotFound());
  EXPECT_TRUE(db_->Get(old_read, "z", &value).IsNotFound());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("manifest_merge.sst", 6, {{"m1", "new"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.merge_existing_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("left", Get("a"));
  EXPECT_EQ("right", Get("z"));
  EXPECT_EQ("new", Get("m1"));
  EXPECT_TRUE(db_->Get(old_read, "a", &value).IsNotFound());
  EXPECT_TRUE(db_->Get(old_read, "z", &value).IsNotFound());
}

TEST_F(LsmEditTest, MergeExistingRequiresAddOnlyMergeEdit) {
  LsmEditOptions options;
  options.merge_existing_files = true;
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("existing_options.sst", 6, {{"m1", "v"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  EXPECT_TRUE(db_->ApplyLsmEdit(options, {edit}).IsInvalidArgument());
  options.merge_incoming_files = true;
  edit.deletion_mode = LsmEditDeletionMode::kFilesInRange;
  EXPECT_TRUE(db_->ApplyLsmEdit(options, {edit}).IsInvalidArgument());
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({6, 999});
  EXPECT_TRUE(db_->ApplyLsmEdit(options, {edit}).IsInvalidArgument());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
}

TEST_F(LsmEditTest, MergeResetsScratchSequencesButPreservesOnlineUpdates) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  auto snapshot = MakeDbFile("snapshot", 5,
                             {{"m0", "seed"},
                              {"m1", "snapshot"},
                              {"m2", "unchanged"},
                              {"m3", "snapshot"}});
  snapshot.zero_sequence_numbers = true;
  const auto events = MakeDbFile("events", 5, {{"m1", "online"}});
  auto edit =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n", {snapshot, events});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("online", Get("m1"));
  EXPECT_EQ("unchanged", Get("m2"));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_EQ(1, db_->GetLatestSequenceNumber());
  ASSERT_OK(Put("m1", "after"));
  EXPECT_EQ("after", Get("m1"));
  ASSERT_OK(Flush());
  Reopen(options);
  EXPECT_EQ("after", Get("m1"));
}

TEST_F(LsmEditTest, MergeHonorsEqualSequenceDeletionAndMergeOperands) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);
  const auto base = MakeFile("base.sst", 5, {{"m1", "old"}, {"m2", "a"}});
  LsmEditFile delta;
  delta.path = sst_files_dir_ + "delta.sst";
  delta.level = 5;
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(delta.path));
  ASSERT_OK(writer.Delete("m1"));
  ASSERT_OK(writer.Merge("m2", "b"));
  ASSERT_OK(writer.Finish());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, delta});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.move_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("a,b", Get("m2"));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_TRUE(env_->FileExists(base.path).IsNotFound());
  EXPECT_TRUE(env_->FileExists(delta.path).IsNotFound());
  Reopen(options);
  EXPECT_EQ("a,b", Get("m2"));
}

TEST_F(LsmEditTest, MergeRetainsIncomingRangeTombstones) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  const std::string source_name = dbname_ + "_range_source";
  source_dbnames_.push_back(source_name);
  ASSERT_OK(DestroyDB(source_name, options));
  std::unique_ptr<DB> source;
  ASSERT_OK(DB::Open(options, source_name, &source));
  ASSERT_OK(source->Put(WriteOptions(), "m1", "old"));
  ASSERT_OK(source->Put(WriteOptions(), "m2", "keep"));
  ASSERT_OK(source->Flush(FlushOptions()));
  ASSERT_OK(source->DeleteRange(WriteOptions(), source->DefaultColumnFamily(),
                                "m1", "m2"));
  ASSERT_OK(source->Flush(FlushOptions()));
  ColumnFamilyMetaData metadata;
  source->GetColumnFamilyMetaData(&metadata);
  std::vector<LsmEditFile> files;
  for (auto file = metadata.levels[0].files.rbegin();
       file != metadata.levels[0].files.rend(); ++file) {
    files.push_back({.path = file->db_path + file->name, .level = 5});
  }
  ASSERT_OK(source->Close());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", files);
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("keep", Get("m2"));
  EXPECT_EQ(3, db_->GetLatestSequenceNumber());
  Reopen(options);
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("keep", Get("m2"));
}

TEST_F(LsmEditTest, MergeResetRequiresSnapshotPointInputs) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  auto tombstone = MakeRangeDeletionFile("reset_range.sst", 5, "m", "n");
  tombstone.zero_sequence_numbers = true;
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {tombstone});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  EXPECT_TRUE(db_->ApplyLsmEdit(LsmEditOptions(), {edit}).IsInvalidArgument());
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  EXPECT_OK(env_->FileExists(tombstone.path));
  EXPECT_EQ("", ShapeOf(db_.get()));
}

TEST_F(LsmEditTest, AddOnlyMayOptOutOfSnapshotConsistency) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "old"));
  ASSERT_OK(Flush());
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("a", "new"));
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                       {MakeFile("snapshot_optout.sst", 5, {{"m1", "add"}})});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("old", Get("a", snapshot));
  EXPECT_EQ("new", Get("a"));
  EXPECT_EQ("add", Get("m1"));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, MergeRetainsVersionsNeededByKnownSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "seed"));
  const Snapshot* snapshot = db_->GetSnapshot();
  auto base = MakeFile("history_base.sst", 5, {{"m1", "before"}});
  auto events =
      MakeDbFile("history_events", 5, {{"m0", "seed"}, {"m1", "after"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, events});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ("before", Get("m1", snapshot));
  EXPECT_EQ("after", Get("m1"));
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(LsmEditTest, SnapshotOptOutCannotDeleteFiles) {
  const auto file = MakeFile("cannot_delete.sst", 5, {{"m1", "add"}});
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {file});
  LsmEditOptions edit_options;
  edit_options.snapshot_consistency = false;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  edit.delete_files.push_back({.level = 0, .file_number = 1});
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
}

TEST_F(LsmEditTest, ProbedEmptyRangeMergeMayKeepActiveSnapshots) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "old-left"));
  ASSERT_OK(Put("z", "right"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("a", "new-left"));

  LsmEditFile base = MakeDbFile("probed_range_base", 5, {{"m1", "before"}});
  base.zero_sequence_numbers = true;
  const LsmEditFile events =
      MakeDbFile("probed_range_events", 5,
                 {{"m0", "seed"}, {"m01", "seed"}, {"m1", "after"}});
  const LsmEdit edit =
      MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, events});
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.snapshot_consistency = false;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ(before[6], FileNumbersPerLevel()[6]);
  EXPECT_EQ(1, NumTableFilesAtLevel(5));
  EXPECT_EQ("old-left", Get("a", snapshot));
  EXPECT_EQ("new-left", Get("a"));
  EXPECT_EQ("before", Get("m1", snapshot));
  EXPECT_EQ("after", Get("m1"));
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(Put("m1", "post-install"));
  EXPECT_EQ("post-install", Get("m1"));
  Reopen(options);
  EXPECT_EQ("post-install", Get("m1"));
  EXPECT_EQ("right", Get("z"));
}

TEST_F(LsmEditTest, ProbedRangeSnapshotOptOutRejectsDestinationData) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  for (int scenario = 0; scenario < 4; ++scenario) {
    SCOPED_TRACE(scenario);
    DestroyAndReopen(options);
    if (scenario == 1 || scenario == 2) {
      ASSERT_OK(Put("a", "left"));
      ASSERT_OK(Put("z", "right"));
    }
    if (scenario == 2) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "b", "y"));
    } else {
      ASSERT_OK(Put("m5", "existing"));
    }
    if (scenario != 3) {
      ASSERT_OK(Flush());
      MoveFilesToLevel(6);
    }
    const std::vector<std::vector<uint64_t>> before = FileNumbersPerLevel();
    const Snapshot* snapshot = db_->GetSnapshot();
    LsmEditOptions edit_options;
    edit_options.merge_incoming_files = true;
    edit_options.snapshot_consistency = false;
    edit_options.allow_blocking_flush = false;
    const Status s = db_->ApplyLsmEdit(
        edit_options,
        {MakeEdit(db_->DefaultColumnFamily(), "m", "n",
                  {MakeFile("nonempty_range.sst", 5, {{"m1", "incoming"}})})});
    EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
    EXPECT_NE(std::string::npos,
              s.ToString().find(scenario == 3 ? "requires flush"
                                              : "range is not empty"));
    EXPECT_EQ(before, FileNumbersPerLevel());
    EXPECT_EQ("NOT_FOUND", Get("m1"));
    EXPECT_EQ(scenario == 2 ? "NOT_FOUND" : "existing", Get("m5", snapshot));
    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(LsmEditTest, RangeSnapshotOptOutRequiresBoundsAndProbing) {
  const LsmEditFile file =
      MakeFile("range_optout.sst", 5, {{"m1", "incoming"}});
  LsmEdit edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {file});
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  edit_options.snapshot_consistency = false;
  edit_options.probe_straddling_files = false;
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  edit_options.probe_straddling_files = true;
  edit.range = RangeOpt();
  EXPECT_TRUE(db_->ApplyLsmEdit(edit_options, {edit}).IsInvalidArgument());
  EXPECT_EQ("NOT_FOUND", Get("m1"));
  EXPECT_EQ("", ShapeOf(db_.get()));
}

TEST_F(LsmEditTest, MergeResolvesEmbeddedBlobsAndWideColumns) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compaction_readahead_size = 64 * 1024;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);
  const std::string scalar(1024, 'x');
  const std::string wide_default(1024, 'y');
  const std::string wide_other(1024, 'z');
  const WideColumns columns{{kDefaultWideColumnName, wide_default},
                            {"big", wide_other}};
  LsmEditFile base;
  base.path = sst_files_dir_ + "embedded_base.sst";
  base.level = 5;
  base.zero_sequence_numbers = true;
  SstFileWriterEmbeddedBlobOptions embedded_options;
  embedded_options.min_blob_size = 8;
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(base.path, embedded_options));
  ASSERT_OK(writer.Put("m1", scalar));
  ASSERT_OK(writer.PutEntity("m2", columns));
  ASSERT_OK(writer.Finish());
  LsmEditFile delta;
  delta.path = sst_files_dir_ + "embedded_delta.sst";
  delta.level = 5;
  SstFileWriter delta_writer(EnvOptions(), options);
  ASSERT_OK(delta_writer.Open(delta.path));
  ASSERT_OK(delta_writer.Merge("m1", "tail"));
  ASSERT_OK(delta_writer.Merge("m2", "tail"));
  ASSERT_OK(delta_writer.Finish());
  auto edit = MakeEdit(db_->DefaultColumnFamily(), "m", "n", {base, delta});
  edit.deletion_mode = LsmEditDeletionMode::kExplicitFiles;
  LsmEditOptions edit_options;
  edit_options.merge_incoming_files = true;
  ASSERT_OK(db_->ApplyLsmEdit(edit_options, {edit}));
  EXPECT_EQ(scalar + ",tail", Get("m1"));
  const std::string merged_default = wide_default + ",tail";
  const WideColumns expected{{kDefaultWideColumnName, merged_default},
                             {"big", wide_other}};
  auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
  iter->Seek("m2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(expected, iter->columns());
  iter.reset();
  Reopen(options);
  EXPECT_EQ(scalar + ",tail", Get("m1"));
  EXPECT_EQ(merged_default, Get("m2"));
}

TEST_F(LsmEditTest, RejectsReadOnlyAndSecondaryInstances) {
  Options options = CurrentOptions();
  ASSERT_OK(Put("seed", "value"));
  ASSERT_OK(Flush());

  Options read_only_options = options;
  read_only_options.max_open_files = -1;
  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(read_only_options, dbname_, &read_only_db));
  assert(read_only_db != nullptr);
  DB& read_only = *read_only_db;
  EXPECT_TRUE(read_only.ApplyLsmEdit(LsmEditOptions(), {}).IsNotSupported());
  ASSERT_OK(read_only.Close());

  const std::string secondary_path = dbname_ + "_secondary";
  ASSERT_OK(DestroyDB(secondary_path, options));
  std::unique_ptr<DB> secondary_db;
  ASSERT_OK(
      DB::OpenAsSecondary(options, dbname_, secondary_path, &secondary_db));
  assert(secondary_db != nullptr);
  DB& secondary = *secondary_db;
  EXPECT_TRUE(secondary.ApplyLsmEdit(LsmEditOptions(), {}).IsNotSupported());
  ASSERT_OK(secondary.Close());
  ASSERT_OK(DestroyDB(secondary_path, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
