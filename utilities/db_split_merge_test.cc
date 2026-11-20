//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/db_split_merge.h"

#include <atomic>
#include <cstdarg>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "port/lang.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb/wal_filter.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/blob_db/blob_db.h"
#include "utilities/trie_index/trie_index_factory.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class TestDB {
 public:
  TestDB() = default;
  TestDB(const TestDB&) = delete;
  TestDB& operator=(const TestDB&) = delete;
  TestDB(TestDB&&) = delete;
  TestDB& operator=(TestDB&&) = delete;

  ~TestDB() { Close(); }

  Status Open(const std::string& path, const std::vector<std::string>& cf_names,
              const Options& options) {
    Close();
    std::vector<ColumnFamilyDescriptor> descriptors;
    descriptors.reserve(cf_names.size() + 1);
    descriptors.emplace_back(kDefaultColumnFamilyName, options);
    for (const std::string& name : cf_names) {
      descriptors.emplace_back(name, options);
    }
    return DB::Open(options, path, descriptors, &handles_, &db_);
  }

  void Close() {
    for (ColumnFamilyHandle* handle : handles_) {
      delete handle;
    }
    handles_.clear();
    db_.reset();
  }

  DB* db() const { return db_.get(); }
  ColumnFamilyHandle* cf(size_t index) const { return handles_[index]; }

 private:
  std::unique_ptr<DB> db_;
  std::vector<ColumnFamilyHandle*> handles_;
};

class FailNewDirectoryOnceFS : public FileSystemWrapper {
 public:
  explicit FailNewDirectoryOnceFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "FailNewDirectoryOnceFS"; }
  const char* Name() const override { return kClassName(); }

  void FailNextNewDirectory(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    fail_path_ = path;
  }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!fail_path_.empty() && name == fail_path_) {
        fail_path_.clear();
        return IOStatus::IOError("injected NewDirectory failure");
      }
    }
    return FileSystemWrapper::NewDirectory(name, io_opts, result, dbg);
  }

 private:
  std::mutex mutex_;
  std::string fail_path_;
};

class FailDeleteDirFS : public FileSystemWrapper {
 public:
  explicit FailDeleteDirFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "FailDeleteDirFS"; }
  const char* Name() const override { return kClassName(); }

  // Fails every DeleteDir() of path until ClearDeleteDirFailure().
  void FailDeleteDir(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    fail_path_ = path;
  }

  void ClearDeleteDirFailure() {
    std::lock_guard<std::mutex> lock(mutex_);
    fail_path_.clear();
  }

  IOStatus DeleteDir(const std::string& name, const IOOptions& io_opts,
                     IODebugContext* dbg) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!fail_path_.empty() && name == fail_path_) {
        return IOStatus::IOError("injected DeleteDir failure");
      }
    }
    return FileSystemWrapper::DeleteDir(name, io_opts, dbg);
  }

 private:
  std::mutex mutex_;
  std::string fail_path_;
};

class CapturingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char buffer[1024];
    va_list copy;
    va_copy(copy, ap);
    vsnprintf(buffer, sizeof(buffer), format, copy);
    va_end(copy);
    std::lock_guard<std::mutex> lock(mutex_);
    messages_.emplace_back(buffer);
  }

  bool Contains(const std::string& text) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const std::string& message : messages_) {
      if (message.find(text) != std::string::npos) {
        return true;
      }
    }
    return false;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::string> messages_;
};

class DropKeyCFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& key, const Slice& /*existing_value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return key == "c";
  }
  const char* Name() const override { return "DropKeyCFilter"; }
};

class DropKeyCFilterFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<DropKeyCFilter>();
  }
  const char* Name() const override { return "DropKeyCFilterFactory"; }
};

class IgnoreAllWalFilter : public WalFilter {
 public:
  WalProcessingOption LogRecordFound(unsigned long long /*log_number*/,
                                     const std::string& /*log_file_name*/,
                                     const WriteBatch& /*batch*/,
                                     WriteBatch* /*new_batch*/,
                                     bool* /*batch_changed*/) override {
    return WalProcessingOption::kIgnoreCurrentRecord;
  }
  const char* Name() const override { return "IgnoreAllWalFilter"; }
};

const std::unordered_map<std::string, OptionTypeInfo>* WatchedPathTypeInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> type_info = {
      {"watched_path",
       {0, OptionType::kString, OptionVerificationType::kNormal,
        OptionTypeFlags::kNone}},
  };
  return &type_info;
}

// Records whether any compaction it serves reads files under watched_path.
// watched_path is a registered option, so it is serialized into OPTIONS.
class WatchedPathDropKeyCFilterFactory : public CompactionFilterFactory {
 public:
  explicit WatchedPathDropKeyCFilterFactory(std::string watched_path)
      : watched_path_(std::move(watched_path)) {
    RegisterOptions("WatchedPathOptions", &watched_path_,
                    WatchedPathTypeInfo());
  }
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    for (const auto& file : context.input_table_properties) {
      if (file.first.rfind(watched_path_, 0) == 0) {
        used_on_watched_path_.store(true);
      }
    }
    return std::make_unique<DropKeyCFilter>();
  }
  const char* Name() const override {
    return "WatchedPathDropKeyCFilterFactory";
  }
  bool used_on_watched_path() const { return used_on_watched_path_.load(); }

 private:
  std::string watched_path_;
  std::atomic<bool> used_on_watched_path_{false};
};

class CompactionCountingListener : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& /*info*/) override {
    compactions_.fetch_add(1);
  }
  int compactions() const { return compactions_.load(); }

 private:
  std::atomic<int> compactions_{0};
};

class DBSplitMergeTest : public testing::Test {
 protected:
  void SetUp() override {
    options_.create_if_missing = true;
    options_.create_missing_column_families = true;
    source_path_ = test::PerThreadDBPath("db_split_merge_source");
    destination_path_ = test::PerThreadDBPath("db_split_merge_destination");
    checkpoint_path_ = test::PerThreadDBPath("db_split_merge_checkpoint");
    ASSERT_OK(DestroyTestDB(source_path_));
    ASSERT_OK(DestroyTestDB(destination_path_));
    ASSERT_OK(DestroyTestDB(checkpoint_path_));
  }

  void TearDown() override {
    source_.Close();
    destination_.Close();
    EXPECT_OK(DestroyTestDB(source_path_));
    EXPECT_OK(DestroyTestDB(destination_path_));
    EXPECT_OK(DestroyTestDB(checkpoint_path_));
  }

  // Also removes the session_tmp directory that DB::Open() creates when
  // use_session_tmp_dir_for_remote_compaction is set, so the reused paths are
  // gone before the next test in the same process.
  static Status DestroyTestDB(const std::string& path) {
    Options options;
    options.use_session_tmp_dir_for_remote_compaction = true;
    return DestroyDB(path, options);
  }

  void ResetDBsWithEmptySourceKey() {
    source_.Close();
    destination_.Close();
    ASSERT_OK(DestroyDB(source_path_, options_));
    ASSERT_OK(DestroyDB(destination_path_, options_));
    ASSERT_OK(source_.Open(source_path_, {}, options_));
    ASSERT_OK(source_.db()->Put(WriteOptions(), "", "empty-key-value"));
    ASSERT_OK(source_.db()->Flush(FlushOptions()));
  }

  void PutFiveKeys(DB* db, ColumnFamilyHandle* cf,
                   const std::string& value_prefix) {
    ASSERT_NE(db, nullptr);
    ASSERT_NE(cf, nullptr);
    if (db == nullptr || cf == nullptr) {
      return;
    }
    for (const char* key : {"a", "b", "c", "d", "e"}) {
      ASSERT_OK(db->Put(WriteOptions(), cf, key, value_prefix + key));
    }
  }

  void ExpectValue(DB* db, ColumnFamilyHandle* cf, const std::string& key,
                   const std::string& expected) {
    ASSERT_NE(db, nullptr);
    ASSERT_NE(cf, nullptr);
    if (db == nullptr || cf == nullptr) {
      return;
    }
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), cf, key, &value));
    ASSERT_EQ(expected, value);
  }

  void ExpectNotFound(DB* db, ColumnFamilyHandle* cf, const std::string& key) {
    ASSERT_NE(db, nullptr);
    ASSERT_NE(cf, nullptr);
    if (db == nullptr || cf == nullptr) {
      return;
    }
    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions(), cf, key, &value).IsNotFound());
  }

  // Declared before the DBs so it outlives any DB opened with it.
  std::unique_ptr<Env> test_env_;
  Options options_;
  TestDB source_;
  TestDB destination_;
  std::string source_path_;
  std::string destination_path_;
  std::string checkpoint_path_;

  void ExpectPathNotFound(const std::string& path) {
    ASSERT_NE(options_.env, nullptr);
    if (options_.env != nullptr) {
      ASSERT_TRUE(options_.env->FileExists(path).IsNotFound());
    }
  }

  // Uses db as the split source, the merge source, and the merge destination,
  // pairing it with destination_ for merges.
  void ExpectSplitAndMergeRejected(DB* db, ColumnFamilyHandle* cf) {
    ASSERT_NE(db, nullptr);
    if (db == nullptr) {
      return;
    }
    SplitDBOptions split_options;
    split_options.destination_db_path = checkpoint_path_;
    split_options.column_family_splits = {{"b", "d", cf}};
    SplitMergeResult result;
    ASSERT_TRUE(SplitDB(db, split_options, &result).IsNotSupported());
    ASSERT_TRUE(DeleteSplitRanges(db, split_options).IsNotSupported());

    MergeDBOptions merge_options;
    merge_options.checkpoint_directory = checkpoint_path_;
    merge_options.column_family_merges = {{"b", "d", cf, destination_.cf(0)}};
    ASSERT_TRUE(MergeDB(db, destination_.db(), merge_options, &result)
                    .IsNotSupported());
    merge_options.column_family_merges = {{"b", "d", destination_.cf(0), cf}};
    ASSERT_TRUE(MergeDB(destination_.db(), db, merge_options, &result)
                    .IsNotSupported());
    ExpectPathNotFound(checkpoint_path_);
  }

  // Also checks that the rejected calls left "b" in cf.
  void ExpectSplitAndMergeNotSupported(DB* db, ColumnFamilyHandle* cf) {
    ExpectSplitAndMergeRejected(db, cf);
    ASSERT_NE(db, nullptr);
    if (db == nullptr) {
      return;
    }
    std::string value;
    ASSERT_OK(db->Get(ReadOptions(), cf, "b", &value));
  }
};

TEST_F(DBSplitMergeTest, SplitMultipleColumnFamilies) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two", "unlisted"}, options_));
  // One default-CF key below L0 and one still in the memtable, which the
  // checkpoint flushes to L0.
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(0), "default-key",
                              "default-value"));
  ASSERT_OK(source_.db()->CompactRange(CompactRangeOptions(), source_.cf(0),
                                       nullptr, nullptr));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(0),
                              "default-memtable-key",
                              "default-memtable-value"));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  PutFiveKeys(source_.db(), source_.cf(2), "two-");
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(3), "unlisted-key",
                              "unlisted-value"));

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {
      {"b", "d", source_.cf(1)},
      {"c", "e", source_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  ASSERT_GT(result.checkpoint_sequence, 0);
  ASSERT_GT(result.transferred_files, 0);
  ExpectValue(source_.db(), source_.cf(1), "b", "one-b");
  ExpectValue(source_.db(), source_.cf(2), "c", "two-c");

  // Must run before anything reopens the split DB and rewrites its OPTIONS.
  ConfigOptions config_options;
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_descriptors;
  ASSERT_OK(LoadLatestOptions(config_options, destination_path_,
                              &loaded_db_options, &loaded_descriptors));
  ASSERT_EQ(3, loaded_descriptors.size());
  for (const ColumnFamilyDescriptor& descriptor : loaded_descriptors) {
    ASSERT_FALSE(descriptor.options.disable_auto_compactions)
        << descriptor.name;
  }

  ASSERT_OK(DeleteSplitRanges(source_.db(), split_options));
  ASSERT_OK(DeleteSplitRanges(source_.db(), split_options));

  ExpectValue(source_.db(), source_.cf(1), "a", "one-a");
  ExpectNotFound(source_.db(), source_.cf(1), "b");
  ExpectNotFound(source_.db(), source_.cf(1), "c");
  ExpectValue(source_.db(), source_.cf(1), "d", "one-d");
  ExpectNotFound(source_.db(), source_.cf(2), "c");
  ExpectNotFound(source_.db(), source_.cf(2), "d");
  ExpectValue(source_.db(), source_.cf(2), "e", "two-e");
  ExpectValue(source_.db(), source_.cf(3), "unlisted-key", "unlisted-value");
  ExpectValue(source_.db(), source_.cf(0), "default-key", "default-value");
  ExpectValue(source_.db(), source_.cf(0), "default-memtable-key",
              "default-memtable-value");

  TestDB split_destination;
  Options destination_options = options_;
  destination_options.create_if_missing = false;
  destination_options.create_missing_column_families = false;
  ASSERT_OK(split_destination.Open(destination_path_, {"one", "two"},
                                   destination_options));
  ExpectNotFound(split_destination.db(), split_destination.cf(0),
                 "default-key");
  ExpectNotFound(split_destination.db(), split_destination.cf(0),
                 "default-memtable-key");
  ExpectNotFound(split_destination.db(), split_destination.cf(1), "a");
  ExpectValue(split_destination.db(), split_destination.cf(1), "b", "one-b");
  ExpectValue(split_destination.db(), split_destination.cf(1), "c", "one-c");
  ExpectNotFound(split_destination.db(), split_destination.cf(1), "d");
  ExpectNotFound(split_destination.db(), split_destination.cf(2), "b");
  ExpectValue(split_destination.db(), split_destination.cf(2), "c", "two-c");
  ExpectValue(split_destination.db(), split_destination.cf(2), "d", "two-d");
  ExpectNotFound(split_destination.db(), split_destination.cf(2), "e");
}

TEST_F(DBSplitMergeTest, SplitSelectedDefaultColumnFamily) {
  ASSERT_OK(source_.Open(source_path_, {}, options_));
  PutFiveKeys(source_.db(), source_.cf(0), "default-");

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(0)}};
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));

  TestDB split_destination;
  ASSERT_OK(split_destination.Open(destination_path_, {}, options_));
  ExpectNotFound(split_destination.db(), split_destination.cf(0), "a");
  ExpectValue(split_destination.db(), split_destination.cf(0), "b",
              "default-b");
  ExpectValue(split_destination.db(), split_destination.cf(0), "c",
              "default-c");
  ExpectNotFound(split_destination.db(), split_destination.cf(0), "d");
}

TEST_F(DBSplitMergeTest, SplitSkipsSourceCompactionFilterAndListeners) {
  const std::shared_ptr<CompactionCountingListener> listener =
      std::make_shared<CompactionCountingListener>();
  options_.listeners.push_back(listener);
  options_.compaction_filter_factory =
      std::make_shared<DropKeyCFilterFactory>();
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  ASSERT_EQ(0, listener->compactions());

  std::string options_file;
  ASSERT_OK(
      GetLatestOptionsFileName(destination_path_, options_.env, &options_file));
  std::string options_text;
  ASSERT_OK(ReadFileToString(
      options_.env, destination_path_ + "/" + options_file, &options_text));
  ASSERT_NE(std::string::npos, options_text.find("DropKeyCFilterFactory"));

  TestDB split_destination;
  ASSERT_OK(split_destination.Open(destination_path_, {"one"}, options_));
  ExpectValue(split_destination.db(), split_destination.cf(1), "b", "one-b");
  ExpectValue(split_destination.db(), split_destination.cf(1), "c", "one-c");
}

TEST_F(DBSplitMergeTest, SplitPersistsOptionsWithoutRunningSourceFilter) {
  // Every compaction advances the clock past periodic_compaction_seconds, so
  // the clipped files are due for periodic compaction if anything opens the
  // split DB with the source options.
  const std::shared_ptr<MockSystemClock> clock =
      std::make_shared<MockSystemClock>(SystemClock::Default());
  clock->SetCurrentTime(1000);
  test_env_ = std::make_unique<CompositeEnvWrapper>(Env::Default(), clock);
  options_.env = test_env_.get();
  options_.periodic_compaction_seconds = 100;
  const std::shared_ptr<WatchedPathDropKeyCFilterFactory> filter_factory =
      std::make_shared<WatchedPathDropKeyCFilterFactory>(destination_path_);
  options_.compaction_filter_factory = filter_factory;
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:AfterCompaction",
      [&](void* /*arg*/) { clock->MockSleepForSeconds(1000); });
  SyncPoint::GetInstance()->EnableProcessing();
  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_FALSE(filter_factory->used_on_watched_path());

  std::string options_file;
  ASSERT_OK(
      GetLatestOptionsFileName(destination_path_, options_.env, &options_file));
  std::string options_text;
  ASSERT_OK(ReadFileToString(
      options_.env, destination_path_ + "/" + options_file, &options_text));
  ASSERT_NE(std::string::npos,
            options_text.find("WatchedPathDropKeyCFilterFactory"));
  ASSERT_NE(std::string::npos,
            options_text.find("watched_path=" + destination_path_));

  Options read_options = options_;
  read_options.compaction_filter_factory.reset();
  std::vector<ColumnFamilyDescriptor> descriptors = {
      {kDefaultColumnFamilyName, read_options}, {"one", read_options}};
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> split_db;
  ASSERT_OK(DB::OpenForReadOnly(read_options, destination_path_, descriptors,
                                &handles, &split_db));
  std::string value;
  ASSERT_OK(split_db->Get(ReadOptions(), handles[1], "c", &value));
  ASSERT_EQ("one-c", value);
  for (ColumnFamilyHandle* handle : handles) {
    delete handle;
  }
}

TEST_F(DBSplitMergeTest, SplitCopiesWalOnlyKeysWithSourceWalFilter) {
  IgnoreAllWalFilter wal_filter;
  Options source_options = options_;
  source_options.wal_filter = &wal_filter;
  ASSERT_OK(source_.Open(source_path_, {}, source_options));
  PutFiveKeys(source_.db(), source_.cf(0), "default-");

  // With the WAL locked, the checkpoint copies the keys only through its WAL.
  ASSERT_OK(source_.db()->LockWAL());
  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(0)}};
  SplitMergeResult result;
  const Status status = SplitDB(source_.db(), split_options, &result);
  ASSERT_OK(source_.db()->UnlockWAL());
  ASSERT_OK(status);

  TestDB split_destination;
  ASSERT_OK(split_destination.Open(destination_path_, {}, options_));
  ExpectValue(split_destination.db(), split_destination.cf(0), "b",
              "default-b");
  ExpectValue(split_destination.db(), split_destination.cf(0), "c",
              "default-c");
}

TEST_F(DBSplitMergeTest, MergeCopiesWalOnlyKeysWithSourceWalFilter) {
  IgnoreAllWalFilter wal_filter;
  Options source_options = options_;
  source_options.wal_filter = &wal_filter;
  ASSERT_OK(source_.Open(source_path_, {}, source_options));
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  PutFiveKeys(source_.db(), source_.cf(0), "source-");

  // With the WAL locked, the checkpoint copies the keys only through its WAL.
  ASSERT_OK(source_.db()->LockWAL());
  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "d", source_.cf(0), destination_.cf(0)}};
  SplitMergeResult result;
  const Status status =
      MergeDB(source_.db(), destination_.db(), merge_options, &result);
  ASSERT_OK(source_.db()->UnlockWAL());
  ASSERT_OK(status);

  ExpectValue(destination_.db(), destination_.cf(0), "b", "source-b");
  ExpectValue(destination_.db(), destination_.cf(0), "c", "source-c");
}

TEST_F(DBSplitMergeTest, SplitWithSessionTmpDirLeavesNoSessionTmpDir) {
  options_.use_session_tmp_dir_for_remote_compaction = true;
  ASSERT_OK(source_.Open(source_path_, {}, options_));
  PutFiveKeys(source_.db(), source_.cf(0), "default-");

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(0)}};
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  ExpectPathNotFound(SessionTmpDir(destination_path_));
}

TEST_F(DBSplitMergeTest, MergeWithSessionTmpDirRemovesCheckpointDirectory) {
  options_.use_session_tmp_dir_for_remote_compaction = true;
  ASSERT_OK(source_.Open(source_path_, {}, options_));
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  PutFiveKeys(source_.db(), source_.cf(0), "source-");

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "d", source_.cf(0), destination_.cf(0)}};
  SplitMergeResult result;
  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ExpectPathNotFound(checkpoint_path_);
  ExpectValue(destination_.db(), destination_.cf(0), "b", "source-b");
}

TEST_F(DBSplitMergeTest, SplitRejectsDuplicateSourceColumnFamily) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {
      {"a", "c", source_.cf(1)},
      {"c", "e", source_.cf(1)},
  };
  SplitMergeResult result;
  ASSERT_TRUE(
      SplitDB(source_.db(), split_options, &result).IsInvalidArgument());
  ExpectPathNotFound(destination_path_);
}

TEST_F(DBSplitMergeTest, SplitRejectsSourceAtStagingPath) {
  // Checkpoint deletes files already in its own "<path>.tmp" staging path.
  for (const char* suffix : {".staging", ".staging.tmp"}) {
    SCOPED_TRACE(suffix);
    source_.Close();
    source_path_ = destination_path_ + suffix;
    ASSERT_OK(DestroyDB(source_path_, Options()));
    ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
    PutFiveKeys(source_.db(), source_.cf(1), "one-");
    ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

    SplitDBOptions split_options;
    split_options.destination_db_path = destination_path_;
    split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
    SplitMergeResult result;
    ASSERT_TRUE(
        SplitDB(source_.db(), split_options, &result).IsInvalidArgument());
    ExpectPathNotFound(destination_path_);
    ExpectValue(source_.db(), source_.cf(1), "b", "one-b");

    source_.Close();
    ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
    ExpectValue(source_.db(), source_.cf(1), "b", "one-b");
    source_.Close();
    ASSERT_OK(DestroyDB(source_path_, Options()));
  }
}

TEST_F(DBSplitMergeTest, SplitCleansUpStagedDirectoriesOnFailure) {
  std::shared_ptr<FailNewDirectoryOnceFS> fault_fs =
      std::make_shared<FailNewDirectoryOnceFS>(FileSystem::Default());
  test_env_ = NewCompositeEnv(fault_fs);
  Options options = options_;
  options.env = test_env_.get();
  ASSERT_OK(source_.Open(source_path_, {"one"}, options));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  SplitMergeResult result;
  // Checkpoint opens the staging directory to fsync it after its rename, and
  // SplitDB() opens the destination to fsync it after the final rename.
  for (const std::string& failed_directory :
       {destination_path_ + ".staging", destination_path_}) {
    SCOPED_TRACE(failed_directory);
    fault_fs->FailNextNewDirectory(failed_directory);
    ASSERT_TRUE(SplitDB(source_.db(), split_options, &result).IsIOError());
    ExpectPathNotFound(destination_path_);
    ExpectPathNotFound(destination_path_ + ".staging");
    ExpectPathNotFound(destination_path_ + ".staging.tmp");
  }

  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  ASSERT_GT(result.transferred_files, 0);
}

TEST_F(DBSplitMergeTest, MergeWarnsWhenCheckpointDirectoryRemains) {
  std::shared_ptr<FailDeleteDirFS> fault_fs =
      std::make_shared<FailDeleteDirFS>(FileSystem::Default());
  test_env_ = NewCompositeEnv(fault_fs);
  Options options = options_;
  options.env = test_env_.get();
  ASSERT_OK(source_.Open(source_path_, {"one"}, options));
  const std::shared_ptr<CapturingLogger> destination_log =
      std::make_shared<CapturingLogger>();
  Options destination_options = options;
  destination_options.info_log = destination_log;
  ASSERT_OK(destination_.Open(destination_path_, {"dst"}, destination_options));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  // The merge commits, but its checkpoint directory cannot be removed.
  fault_fs->FailDeleteDir(checkpoint_path_);
  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "d", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ExpectValue(destination_.db(), destination_.cf(1), "b", "one-b");
  ExpectValue(destination_.db(), destination_.cf(1), "c", "one-c");
  EXPECT_TRUE(
      destination_log->Contains("failed to remove checkpoint directory"));

  // The leftover directory rejects a later merge that reuses the path.
  merge_options.column_family_merges = {
      {"d", "f", source_.cf(1), destination_.cf(1)}};
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());
  ExpectNotFound(destination_.db(), destination_.cf(1), "d");

  fault_fs->ClearDeleteDirFailure();
  ASSERT_OK(test_env_->DeleteDir(checkpoint_path_));
  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ExpectValue(destination_.db(), destination_.cf(1), "d", "one-d");
  ExpectValue(destination_.db(), destination_.cf(1), "e", "one-e");
  ExpectPathNotFound(checkpoint_path_);
}

TEST_F(DBSplitMergeTest, SplitPublishesDestinationOnlyWhenComplete) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  std::atomic<bool> destination_seen_while_clipping{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:AfterCompaction", [&](void* /*arg*/) {
        if (options_.env->FileExists(destination_path_).ok()) {
          destination_seen_while_clipping.store(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  SplitDBOptions split_options;
  // The trailing slash must not change the derived staging path.
  split_options.destination_db_path = destination_path_ + "/";
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  SplitMergeResult result;
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_FALSE(destination_seen_while_clipping.load());
  ExpectPathNotFound(destination_path_ + ".staging");
  TestDB split_destination;
  ASSERT_OK(split_destination.Open(destination_path_, {"one"}, options_));
  ExpectValue(split_destination.db(), split_destination.cf(1), "c", "one-c");
}

TEST_F(DBSplitMergeTest, MergeMultipleColumnFamilies) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two"}, options_));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one", "dst-two"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-old-");
  PutFiveKeys(source_.db(), source_.cf(2), "two-");
  ASSERT_OK(
      source_.db()->Flush(FlushOptions(), {source_.cf(1), source_.cf(2)}));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "c", "one-new-c"));
  ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(1), "a",
                                   "destination-a"));
  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(2), "z",
                                   "destination-z"));
  ASSERT_OK(destination_.db()->Flush(FlushOptions(),
                                     {destination_.cf(1), destination_.cf(2)}));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)},
      {"c", "f", source_.cf(2), destination_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ASSERT_GT(result.checkpoint_sequence, 0);
  ASSERT_GT(result.transferred_files, 0);
  ExpectPathNotFound(checkpoint_path_);

  ExpectValue(destination_.db(), destination_.cf(1), "a", "destination-a");
  ExpectValue(destination_.db(), destination_.cf(1), "b", "one-old-b");
  ExpectValue(destination_.db(), destination_.cf(1), "c", "one-new-c");
  ExpectValue(destination_.db(), destination_.cf(1), "d", "one-old-d");
  ExpectNotFound(destination_.db(), destination_.cf(1), "e");
  ExpectNotFound(destination_.db(), destination_.cf(2), "b");
  ExpectValue(destination_.db(), destination_.cf(2), "c", "two-c");
  ExpectValue(destination_.db(), destination_.cf(2), "d", "two-d");
  ExpectValue(destination_.db(), destination_.cf(2), "e", "two-e");
  ExpectValue(destination_.db(), destination_.cf(2), "z", "destination-z");

  ExpectValue(source_.db(), source_.cf(1), "b", "one-old-b");
  ExpectValue(source_.db(), source_.cf(1), "c", "one-new-c");
  ExpectValue(source_.db(), source_.cf(2), "e", "two-e");
}

TEST_F(DBSplitMergeTest, MergeRejectsRepeatedSourceOrDestination) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two"}, options_));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one", "dst-two"}, options_));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"a", "c", source_.cf(1), destination_.cf(1)},
      {"c", "e", source_.cf(1), destination_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());

  merge_options.column_family_merges = {
      {"a", "c", source_.cf(1), destination_.cf(1)},
      {"c", "e", source_.cf(2), destination_.cf(1)},
  };
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());
  ExpectPathNotFound(checkpoint_path_);
}

TEST_F(DBSplitMergeTest, MergeRejectsExistingStagingPath) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ASSERT_OK(destination_.Open(destination_path_, {"dst-one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  Env* env = options_.env;
  ASSERT_NE(env, nullptr);
  if (env == nullptr) {
    return;
  }
  const std::string staging_path = checkpoint_path_ + ".tmp";
  const std::string sentinel_path = staging_path + "/sentinel";
  ASSERT_OK(env->CreateDirIfMissing(staging_path));
  ASSERT_OK(WriteStringToFile(env, "sentinel", sentinel_path));

  MergeDBOptions merge_options;
  // The trailing slash must not change the derived staging path.
  merge_options.checkpoint_directory = checkpoint_path_ + "/";
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  const Status status =
      MergeDB(source_.db(), destination_.db(), merge_options, &result);
  ASSERT_OK(env->FileExists(sentinel_path));
  ASSERT_OK(env->DeleteFile(sentinel_path));
  ASSERT_OK(env->DeleteDir(staging_path));
  ASSERT_TRUE(status.IsInvalidArgument()) << status.ToString();
  ExpectPathNotFound(checkpoint_path_);
  ExpectNotFound(destination_.db(), destination_.cf(1), "b");
}

TEST_F(DBSplitMergeTest, MergeRejectsDestinationSnapshot) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ASSERT_OK(destination_.Open(destination_path_, {"dst-one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  // Advance destination sequence numbers past the source keys so a snapshot
  // would cover them if they kept their source sequence numbers.
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(0),
                                     "advance-" + std::to_string(i), "v"));
  }

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  {
    ManagedSnapshot snapshot(destination_.db());
    ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                    .IsInvalidArgument());
    ReadOptions snapshot_read;
    snapshot_read.snapshot = snapshot.snapshot();
    std::string value;
    ASSERT_TRUE(destination_.db()
                    ->Get(snapshot_read, destination_.cf(1), "b", &value)
                    .IsNotFound());
    ExpectNotFound(destination_.db(), destination_.cf(1), "b");
    ExpectPathNotFound(checkpoint_path_);
  }

  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ExpectValue(destination_.db(), destination_.cf(1), "b", "one-b");
}

TEST_F(DBSplitMergeTest, MergeRejectsOverlapWithoutPartialIngestion) {
  ASSERT_OK(source_.Open(source_path_, {"one", "two"}, options_));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one", "dst-two"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  PutFiveKeys(source_.db(), source_.cf(2), "two-");
  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(2), "d",
                                   "existing-d"));
  ASSERT_OK(destination_.db()->Flush(FlushOptions(), destination_.cf(2)));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)},
      {"c", "f", source_.cf(2), destination_.cf(2)},
  };
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());
  ExpectNotFound(destination_.db(), destination_.cf(1), "b");
  ExpectNotFound(destination_.db(), destination_.cf(1), "c");
  ExpectValue(destination_.db(), destination_.cf(2), "d", "existing-d");
  ExpectPathNotFound(checkpoint_path_);
}

TEST_F(DBSplitMergeTest, RejectsTransactionDB) {
  TransactionDBOptions txn_db_options;
  txn_db_options.write_policy = WRITE_PREPARED;
  TransactionDB* txn_db_ptr = nullptr;
  ASSERT_OK(
      TransactionDB::Open(options_, txn_db_options, source_path_, &txn_db_ptr));
  std::unique_ptr<TransactionDB> txn_db(txn_db_ptr);
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  ASSERT_OK(txn_db->Put(WriteOptions(), "b", "committed"));
  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  ASSERT_OK(txn->SetName("txn"));
  ASSERT_OK(txn->Put("b", "prepared"));
  ASSERT_OK(txn->Prepare());

  // The base DB has allow_2pc set and holds the prepared value.
  for (DB* db : std::vector<DB*>{txn_db.get(), txn_db->GetRootDB()}) {
    ExpectSplitAndMergeNotSupported(db, db->DefaultColumnFamily());
  }
  ASSERT_OK(txn->Rollback());
}

TEST_F(DBSplitMergeTest, RejectsDBWithTTL) {
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  // DBWithTTL wraps a raw compaction_filter in TtlCompactionFilter and
  // otherwise installs TtlCompactionFilterFactory.
  DropKeyCFilter raw_filter;
  std::vector<const CompactionFilter*> filters = {nullptr};
#ifndef MUST_FREE_HEAP_ALLOCATIONS
  // Verifying a written OPTIONS file creates a TtlCompactionFilter from its
  // name, and raw compaction filters created that way are never freed.
  filters.push_back(&raw_filter);
#endif  // MUST_FREE_HEAP_ALLOCATIONS
  for (const CompactionFilter* filter : filters) {
    SCOPED_TRACE(filter == nullptr ? "factory" : "raw filter");
    ASSERT_OK(DestroyDB(source_path_, Options()));
    Options ttl_options = options_;
    ttl_options.compaction_filter = filter;
    DBWithTTL* ttl_db_ptr = nullptr;
    ASSERT_OK(
        DBWithTTL::Open(ttl_options, source_path_, &ttl_db_ptr, /*ttl=*/3600));
    std::unique_ptr<DBWithTTL> ttl_db(ttl_db_ptr);
    ASSERT_OK(ttl_db->Put(WriteOptions(), "b", "value"));

    // The base DB stores a timestamp suffix in every value.
    for (DB* db : std::vector<DB*>{ttl_db.get(), ttl_db->GetRootDB()}) {
      ExpectSplitAndMergeNotSupported(db, db->DefaultColumnFamily());
    }
  }
}

TEST_F(DBSplitMergeTest, RejectsIngestBehind) {
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  Options cf_ingest_behind = options_;
  cf_ingest_behind.compaction_style = kCompactionStyleUniversal;
  cf_ingest_behind.cf_allow_ingest_behind = true;
  Options db_ingest_behind = options_;
  db_ingest_behind.compaction_style = kCompactionStyleUniversal;
  db_ingest_behind.allow_ingest_behind = true;
  for (const Options& ingest_behind_options :
       {cf_ingest_behind, db_ingest_behind}) {
    SCOPED_TRACE(ingest_behind_options.allow_ingest_behind
                     ? "allow_ingest_behind"
                     : "cf_allow_ingest_behind");
    source_.Close();
    ASSERT_OK(DestroyDB(source_path_, Options()));
    ASSERT_OK(source_.Open(source_path_, {"one"}, ingest_behind_options));
    ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "b", "value"));
    ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

    ExpectSplitAndMergeNotSupported(source_.db(), source_.cf(1));
  }
}

TEST_F(DBSplitMergeTest, RejectsPlainTable) {
  Options plain_table_options = options_;
  plain_table_options.table_factory.reset(NewPlainTableFactory());
  plain_table_options.prefix_extractor.reset(NewNoopTransform());
  plain_table_options.allow_mmap_reads = true;
  ASSERT_OK(source_.Open(source_path_, {"one"}, plain_table_options));
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "b", "value"));
  ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

  ExpectSplitAndMergeNotSupported(source_.db(), source_.cf(1));
}

TEST_F(DBSplitMergeTest, MergeOpensFilesWithDestinationTableOptions) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  // Files written without a user-defined index cannot be opened by the
  // destination.
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory =
      std::make_shared<trie_index::TrieIndexFactory>();
  table_options.fail_if_no_udi_on_open = true;
  Options destination_options = options_;
  destination_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one"}, destination_options));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "d", source_.cf(1), destination_.cf(1)}};
  merge_options.verify_checksums = false;
  SplitMergeResult result;
  ASSERT_NOK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ExpectNotFound(destination_.db(), destination_.cf(1), "b");
  ExpectPathNotFound(checkpoint_path_);
}

TEST_F(DBSplitMergeTest, RejectsUserDefinedTimestamps) {
  Options timestamp_options = options_;
  timestamp_options.comparator = BytewiseComparatorWithU64Ts();
  ASSERT_OK(source_.Open(source_path_, {"one"}, timestamp_options));
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));

  // The bounds are too short to hold a timestamp.
  ExpectSplitAndMergeRejected(source_.db(), source_.cf(1));
}

TEST_F(DBSplitMergeTest, SplitRejectsUnsupportedDefaultColumnFamily) {
  Options timestamp_options = options_;
  timestamp_options.comparator = BytewiseComparatorWithU64Ts();
  std::vector<ColumnFamilyDescriptor> descriptors = {
      {kDefaultColumnFamilyName, timestamp_options}, {"one", options_}};
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, source_path_, descriptors, &handles, &db));
  ASSERT_OK(db->Put(WriteOptions(), handles[1], "b", "value"));

  // The default column family is copied and emptied even when not selected.
  SplitDBOptions split_options;
  split_options.destination_db_path = destination_path_;
  split_options.column_family_splits = {{"b", "d", handles[1]}};
  SplitMergeResult result;
  ASSERT_TRUE(SplitDB(db.get(), split_options, &result).IsNotSupported());
  ExpectPathNotFound(destination_path_);
  for (ColumnFamilyHandle* handle : handles) {
    delete handle;
  }
}

TEST_F(DBSplitMergeTest, RejectsLegacyBlobDB) {
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  blob_db::BlobDB* blob_db_ptr = nullptr;
  ASSERT_OK(blob_db::BlobDB::Open(options_, blob_db::BlobDBOptions(),
                                  source_path_, &blob_db_ptr));
  std::unique_ptr<blob_db::BlobDB> blob_db(blob_db_ptr);
  ASSERT_OK(blob_db->Put(WriteOptions(), "b", "value"));

  // The base DB stores blob indexes instead of values.
  for (DB* db : std::vector<DB*>{blob_db.get(), blob_db->GetRootDB()}) {
    ExpectSplitAndMergeRejected(db, db->DefaultColumnFamily());
  }
}

TEST_F(DBSplitMergeTest, RejectsUnsupportedDBOptions) {
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  const std::string other_path = source_path_ + "_other";
  Options db_paths_options = options_;
  db_paths_options.db_paths = {{other_path, 1 << 30}};
  Options cf_paths_options = options_;
  cf_paths_options.cf_paths = {{other_path, 1 << 30}};
  // Files in the second path keep path ID 1 in the MANIFEST.
  Options colocated_db_paths_options = options_;
  colocated_db_paths_options.db_paths = {{source_path_, 1 << 20},
                                         {source_path_, 1 << 30}};
  Options row_cache_options = options_;
  row_cache_options.row_cache = NewLRUCache(1 << 20);
  const std::vector<std::pair<std::string, Options>> cases = {
      {"db_paths", db_paths_options},
      {"colocated db_paths", colocated_db_paths_options},
      {"cf_paths", cf_paths_options},
      {"row_cache", row_cache_options},
  };
  for (const std::pair<std::string, Options>& test_case : cases) {
    SCOPED_TRACE(test_case.first);
    source_.Close();
    ASSERT_OK(DestroyDB(source_path_, test_case.second));
    ASSERT_OK(source_.Open(source_path_, {"one"}, test_case.second));
    ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "b", "value"));
    ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

    ExpectSplitAndMergeNotSupported(source_.db(), source_.cf(1));
    source_.Close();
    ASSERT_OK(DestroyDB(source_path_, test_case.second));
  }
}

TEST_F(DBSplitMergeTest, RejectsDisallowMemtableWrites) {
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  Options ingestion_only_options = options_;
  ingestion_only_options.disallow_memtable_writes = true;
  std::vector<ColumnFamilyDescriptor> descriptors = {
      {kDefaultColumnFamilyName, options_}, {"one", ingestion_only_options}};
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, source_path_, descriptors, &handles, &db));

  // Clipping and DeleteSplitRanges() write range deletions.
  ExpectSplitAndMergeRejected(db.get(), handles[1]);
  for (ColumnFamilyHandle* handle : handles) {
    delete handle;
  }
}

TEST_F(DBSplitMergeTest, MergeRejectsSequenceTimeTracking) {
  Options time_options = options_;
  time_options.preserve_internal_time_seconds = 3600;
  ASSERT_OK(source_.Open(source_path_, {"one"}, time_options));
  ASSERT_OK(destination_.Open(destination_path_, {"dst-one"}, options_));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "d", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsNotSupported());
  merge_options.column_family_merges = {
      {"b", "d", destination_.cf(1), source_.cf(1)}};
  ASSERT_TRUE(MergeDB(destination_.db(), source_.db(), merge_options, &result)
                  .IsNotSupported());
  ExpectPathNotFound(checkpoint_path_);

  // A split DB shares the source's sequence numbers.
  SplitDBOptions split_options;
  split_options.destination_db_path = checkpoint_path_;
  split_options.column_family_splits = {{"b", "d", source_.cf(1)}};
  ASSERT_OK(SplitDB(source_.db(), split_options, &result));
}

TEST_F(DBSplitMergeTest, RejectsResidualBlobFiles) {
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  Options blob_options = options_;
  blob_options.enable_blob_files = true;
  ASSERT_OK(source_.Open(source_path_, {"one"}, blob_options));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "b", "value"));
  ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

  // Blob files written earlier stay live after blob files are disabled.
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ExpectSplitAndMergeNotSupported(source_.db(), source_.cf(1));
}

TEST_F(DBSplitMergeTest, SplitSingleEmptyUserKey) {
  for (bool keep_empty_key : {false, true}) {
    SCOPED_TRACE(keep_empty_key);
    ASSERT_NO_FATAL_FAILURE(ResetDBsWithEmptySourceKey());

    SplitDBOptions split_options;
    split_options.destination_db_path = destination_path_;
    const char* begin_key = keep_empty_key ? "" : "b";
    split_options.column_family_splits = {{begin_key, "d", source_.cf(0)}};
    SplitMergeResult result;
    ASSERT_OK(SplitDB(source_.db(), split_options, &result));
    ExpectValue(source_.db(), source_.cf(0), "", "empty-key-value");
    ASSERT_OK(destination_.Open(destination_path_, {}, options_));
    if (keep_empty_key) {
      ExpectValue(destination_.db(), destination_.cf(0), "", "empty-key-value");
    } else {
      ExpectNotFound(destination_.db(), destination_.cf(0), "");
    }
    ASSERT_OK(DeleteSplitRanges(source_.db(), split_options));
    ASSERT_OK(source_.Open(source_path_, {}, options_));
    if (keep_empty_key) {
      ExpectNotFound(source_.db(), source_.cf(0), "");
    } else {
      ExpectValue(source_.db(), source_.cf(0), "", "empty-key-value");
    }
  }
}

TEST_F(DBSplitMergeTest, MergeSingleEmptyUserKey) {
  for (bool keep_empty_key : {false, true}) {
    SCOPED_TRACE(keep_empty_key);
    ASSERT_NO_FATAL_FAILURE(ResetDBsWithEmptySourceKey());
    ASSERT_OK(destination_.Open(destination_path_, {}, options_));

    MergeDBOptions merge_options;
    merge_options.checkpoint_directory = checkpoint_path_;
    const char* begin_key = keep_empty_key ? "" : "b";
    merge_options.column_family_merges = {
        {begin_key, "d", source_.cf(0), destination_.cf(0)}};
    SplitMergeResult result;
    ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
    ExpectValue(source_.db(), source_.cf(0), "", "empty-key-value");
    ASSERT_OK(destination_.Open(destination_path_, {}, options_));
    if (keep_empty_key) {
      ExpectValue(destination_.db(), destination_.cf(0), "", "empty-key-value");
    } else {
      ExpectNotFound(destination_.db(), destination_.cf(0), "");
    }
    ExpectPathNotFound(checkpoint_path_);
  }
}

TEST_F(DBSplitMergeTest, MergeRejectsNonEmptyDestinationRange) {
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ASSERT_OK(destination_.Open(destination_path_, {"dst-one"}, options_));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "c", "source-c"));
  // Outside the source data but inside the requested range.
  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(1), "y",
                                   "stale-y"));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "z", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());
  ExpectNotFound(destination_.db(), destination_.cf(1), "c");
  ExpectPathNotFound(checkpoint_path_);
}

TEST_F(DBSplitMergeTest, MergeChecksDestinationRangeAcrossPrefixes) {
  // A prefix seek for "bbb" would skip the file holding "ccc1".
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  table_options.whole_key_filtering = false;
  Options prefix_options = options_;
  prefix_options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  prefix_options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_OK(source_.Open(source_path_, {"one"}, prefix_options));
  ASSERT_OK(destination_.Open(destination_path_, {"dst-one"}, prefix_options));
  ASSERT_OK(source_.db()->Put(WriteOptions(), source_.cf(1), "bbb2", "source"));
  ASSERT_OK(destination_.db()->Put(WriteOptions(), destination_.cf(1), "ccc1",
                                   "stale"));
  ASSERT_OK(destination_.db()->Flush(FlushOptions(), destination_.cf(1)));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"bbb1", "ddd0", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsInvalidArgument());
  ExpectNotFound(destination_.db(), destination_.cf(1), "bbb2");
  ExpectPathNotFound(checkpoint_path_);
}

TEST_F(DBSplitMergeTest, MergeRejectsDifferentFileSystems) {
  test_env_.reset(NewMemEnv(Env::Default()));
  Options destination_options = options_;
  destination_options.env = test_env_.get();
  ASSERT_OK(source_.Open(source_path_, {"one"}, options_));
  ASSERT_OK(
      destination_.Open(destination_path_, {"dst-one"}, destination_options));
  PutFiveKeys(source_.db(), source_.cf(1), "one-");
  ASSERT_OK(source_.db()->Flush(FlushOptions(), source_.cf(1)));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(1), destination_.cf(1)}};
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsNotSupported());
  ExpectPathNotFound(checkpoint_path_);
  ExpectNotFound(destination_.db(), destination_.cf(1), "b");
}

TEST_F(DBSplitMergeTest, MergeRejectsDestinationReaderTombstoneConversion) {
  Options conversion_options = options_;
  conversion_options.min_tombstones_for_range_conversion = 2;
  ASSERT_OK(source_.Open(source_path_, {}, conversion_options));
  ASSERT_OK(destination_.Open(destination_path_, {}, conversion_options));
  ASSERT_OK(source_.db()->Put(WriteOptions(), "c", "copied"));
  ASSERT_OK(destination_.db()->Delete(WriteOptions(), "a"));
  ASSERT_OK(destination_.db()->Delete(WriteOptions(), "f"));
  ASSERT_OK(destination_.db()->Put(WriteOptions(), "z", "existing"));
  std::unique_ptr<Iterator> reader(
      destination_.db()->NewIterator(ReadOptions()));

  MergeDBOptions merge_options;
  merge_options.checkpoint_directory = checkpoint_path_;
  merge_options.column_family_merges = {
      {"b", "e", source_.cf(0), destination_.cf(0)}};
  SplitMergeResult result;
  ASSERT_TRUE(MergeDB(source_.db(), destination_.db(), merge_options, &result)
                  .IsNotSupported());
  ExpectPathNotFound(checkpoint_path_);
  ExpectPathNotFound(checkpoint_path_ + ".tmp");
  ExpectValue(source_.db(), source_.cf(0), "c", "copied");
  ExpectNotFound(destination_.db(), destination_.cf(0), "c");
  reader.reset();

  ASSERT_OK(destination_.db()->SetOptions(
      {{"min_tombstones_for_range_conversion", "0"}}));
  ASSERT_OK(MergeDB(source_.db(), destination_.db(), merge_options, &result));
  ExpectValue(destination_.db(), destination_.cf(0), "c", "copied");
  ExpectValue(destination_.db(), destination_.cf(0), "z", "existing");
  ASSERT_OK(destination_.Open(destination_path_, {}, options_));
  ExpectValue(destination_.db(), destination_.cf(0), "c", "copied");
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
