//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/listener.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/defer.h"
#include "util/string_util.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
std::string EncodeAsUint64(uint64_t number) {
  std::string result;
  PutFixed64(&result, number);
  return result;
}
}  // namespace

static const int kValueSize = 1000;

// counts how many operations were performed
class EnvCounter : public SpecialEnv {
 public:
  explicit EnvCounter(Env* base)
      : SpecialEnv(base), num_new_writable_file_(0) {}
  int GetNumberOfNewWritableFileCalls() { return num_new_writable_file_; }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& soptions) override {
    ++num_new_writable_file_;
    return EnvWrapper::NewWritableFile(f, r, soptions);
  }

 private:
  std::atomic<int> num_new_writable_file_;
};

class ColumnFamilyTestBase : public testing::Test {
 public:
  explicit ColumnFamilyTestBase(uint32_t format) : rnd_(139), format_(format) {
    Env* base_env = Env::Default();
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &base_env, &env_guard_));
    EXPECT_NE(nullptr, base_env);
    env_ = new EnvCounter(base_env);
    env_->skip_fsync_ = true;
    dbname_ = test::PerThreadDBPath("column_family_test");
    db_options_.create_if_missing = true;
    db_options_.env = env_;
  }

  void SetUp() override {
    EXPECT_OK(DestroyDB(dbname_, Options(db_options_, column_family_options_)));
  }

  ~ColumnFamilyTestBase() override {
    std::vector<ColumnFamilyDescriptor> column_families;
    for (auto h : handles_) {
      ColumnFamilyDescriptor cfdescriptor;
      Status s = h->GetDescriptor(&cfdescriptor);
      EXPECT_OK(s);
      column_families.push_back(cfdescriptor);
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    Destroy(column_families);
    delete env_;
  }

  BlockBasedTableOptions GetBlockBasedTableOptions() {
    BlockBasedTableOptions options;
    options.format_version = format_;
    return options;
  }

  // Return the value to associate with the specified key
  Slice Value(int k, std::string* storage) {
    if (k == 0) {
      // Ugh.  Random seed of 0 used to produce no entropy.  This code
      // preserves the implementation that was in place when all of the
      // magic values in this file were picked.
      *storage = std::string(kValueSize, ' ');
    } else {
      Random r(k);
      *storage = r.RandomString(kValueSize);
    }
    return Slice(*storage);
  }

  void Build(int base, int n, int flush_every = 0) {
    std::string key_space;
    std::string value_space;
    WriteBatch batch;

    for (int i = 0; i < n; i++) {
      if (flush_every != 0 && i != 0 && i % flush_every == 0) {
        dbfull()->TEST_FlushMemTable();
      }

      int keyi = base + i;
      Slice key(DBTestBase::Key(keyi));

      batch.Clear();
      batch.Put(handles_[0], key, Value(keyi, &value_space));
      batch.Put(handles_[1], key, Value(keyi, &value_space));
      batch.Put(handles_[2], key, Value(keyi, &value_space));
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
    }
  }

  void CheckMissed() {
    uint64_t next_expected = 0;
    uint64_t missed = 0;
    int bad_keys = 0;
    int bad_values = 0;
    int correct = 0;
    std::string value_space;
    for (int cf = 0; cf < 3; cf++) {
      next_expected = 0;
      Iterator* iter = db_->NewIterator(ReadOptions(false, true), handles_[cf]);
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        uint64_t key;
        Slice in(iter->key());
        in.remove_prefix(3);
        if (!ConsumeDecimalNumber(&in, &key) || !in.empty() ||
            key < next_expected) {
          bad_keys++;
          continue;
        }
        missed += (key - next_expected);
        next_expected = key + 1;
        if (iter->value() != Value(static_cast<int>(key), &value_space)) {
          bad_values++;
        } else {
          correct++;
        }
      }
      delete iter;
    }

    ASSERT_EQ(0, bad_keys);
    ASSERT_EQ(0, bad_values);
    ASSERT_EQ(0, missed);
    (void)correct;
  }

  void Close() {
    for (auto h : handles_) {
      if (h) {
        ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
      }
    }
    handles_.clear();
    names_.clear();
    db_.reset();
  }

  void AssertColumnFamilyNames(const std::vector<std::string>& expected) {
    std::vector<std::string> column_families;
    ASSERT_OK(DB::ListColumnFamilies(db_options_, dbname_, &column_families));
    ASSERT_EQ(expected, column_families);
  }

  void LoadLatestColumnFamilyDescriptors(
      std::vector<ColumnFamilyDescriptor>* descriptors) {
    ConfigOptions config_options;
    config_options.env = db_options_.env;
    DBOptions loaded_db_options;
    ASSERT_OK(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                                descriptors));
  }

  size_t CountOptionsFiles() {
    std::vector<std::string> children;
    EXPECT_OK(db_options_.env->GetChildren(dbname_, &children));
    size_t count = 0;
    for (const auto& child : children) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(child, &number, &type) && type == kOptionsFile) {
        ++count;
      }
    }
    return count;
  }

  Status TryOpen(std::vector<std::string> cf,
                 std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    names_.clear();
    for (size_t i = 0; i < cf.size(); ++i) {
      column_families.emplace_back(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]);
      names_.push_back(cf[i]);
    }
    return DB::Open(db_options_, dbname_, column_families, &handles_, &db_);
  }

  Status OpenReadOnly(std::vector<std::string> cf,
                      std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    names_.clear();
    for (size_t i = 0; i < cf.size(); ++i) {
      column_families.emplace_back(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]);
      names_.push_back(cf[i]);
    }
    return DB::OpenForReadOnly(db_options_, dbname_, column_families, &handles_,
                               &db_);
  }

  void AssertOpenReadOnly(std::vector<std::string> cf,
                          std::vector<ColumnFamilyOptions> options = {}) {
    ASSERT_OK(OpenReadOnly(cf, options));
  }

  void Open(std::vector<std::string> cf,
            std::vector<ColumnFamilyOptions> options = {}) {
    ASSERT_OK(TryOpen(cf, options));
  }

  void Open() { Open({"default"}); }

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_.get()); }

  int GetProperty(int cf, std::string property) {
    std::string value;
    EXPECT_TRUE(dbfull()->GetProperty(handles_[cf], property, &value));
#ifndef CYGWIN
    return std::stoi(value);
#else
    return std::strtol(value.c_str(), 0 /* off */, 10 /* base */);
#endif
  }

  bool IsDbWriteStopped() {
    uint64_t v;
    EXPECT_TRUE(dbfull()->GetIntProperty("rocksdb.is-write-stopped", &v));
    return (v == 1);
  }

  uint64_t GetDbDelayedWriteRate() {
    uint64_t v;
    EXPECT_TRUE(
        dbfull()->GetIntProperty("rocksdb.actual-delayed-write-rate", &v));
    return v;
  }

  void Destroy(const std::vector<ColumnFamilyDescriptor>& column_families =
                   std::vector<ColumnFamilyDescriptor>()) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, Options(db_options_, column_family_options_),
                        column_families));
  }

  void CreateColumnFamilies(
      const std::vector<std::string>& cfs,
      const std::vector<ColumnFamilyOptions> options = {}) {
    int cfi = static_cast<int>(handles_.size());
    handles_.resize(cfi + cfs.size());
    names_.resize(cfi + cfs.size());
    for (size_t i = 0; i < cfs.size(); ++i) {
      const auto& current_cf_opt =
          options.size() == 0 ? column_family_options_ : options[i];
      ASSERT_OK(
          db_->CreateColumnFamily(current_cf_opt, cfs[i], &handles_[cfi]));
      names_[cfi] = cfs[i];

      // Verify the CF options of the returned CF handle.
      ColumnFamilyDescriptor desc;
      ASSERT_OK(handles_[cfi]->GetDescriptor(&desc));
      // Need to sanitize the default column family options before comparing
      // them.
      ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
          ConfigOptions(), desc.options,
          SanitizeCfOptions(dbfull()->immutable_db_options(),
                            /*read_only*/ false, current_cf_opt)));
      cfi++;
    }
  }

  void Reopen(const std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<std::string> names;
    for (const auto& name : names_) {
      if (name != "") {
        names.push_back(name);
      }
    }
    Close();
    assert(options.size() == 0 || names.size() == options.size());
    Open(names, options);
  }

  void CreateColumnFamiliesAndReopen(const std::vector<std::string>& cfs) {
    CreateColumnFamilies(cfs);
    Reopen();
  }

  void DropColumnFamilies(const std::vector<int>& cfs) {
    for (auto cf : cfs) {
      ASSERT_OK(db_->DropColumnFamily(handles_[cf]));
      ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[cf]));
      handles_[cf] = nullptr;
      names_[cf] = "";
    }
  }

  void PutRandomData(int cf, int num, int key_value_size, bool save = false) {
    if (cf >= static_cast<int>(keys_.size())) {
      keys_.resize(cf + 1);
    }
    for (int i = 0; i < num; ++i) {
      // 10 bytes for key, rest is value
      if (!save) {
        ASSERT_OK(Put(cf, test::RandomKey(&rnd_, 11),
                      rnd_.RandomString(key_value_size - 10)));
      } else {
        std::string key = test::RandomKey(&rnd_, 11);
        keys_[cf].insert(key);
        ASSERT_OK(Put(cf, key, rnd_.RandomString(key_value_size - 10)));
      }
    }
    ASSERT_OK(db_->FlushWAL(/*sync=*/false));
  }

  void WaitForFlush(int cf) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
  }

  void WaitForCompaction() { ASSERT_OK(dbfull()->TEST_WaitForCompact()); }

  uint64_t MaxTotalInMemoryState() {
    return dbfull()->TEST_MaxTotalInMemoryState();
  }

  void AssertMaxTotalInMemoryState(uint64_t value) {
    ASSERT_EQ(value, MaxTotalInMemoryState());
  }

  Status Put(int cf, const std::string& key, const std::string& value) {
    return db_->Put(WriteOptions(), handles_[cf], Slice(key), Slice(value));
  }
  Status Merge(int cf, const std::string& key, const std::string& value) {
    return db_->Merge(WriteOptions(), handles_[cf], Slice(key), Slice(value));
  }
  Status Flush(int cf) { return db_->Flush(FlushOptions(), handles_[cf]); }

  std::string Get(int cf, const std::string& key) {
    ReadOptions options;
    options.verify_checksums = true;
    std::string result;
    Status s = db_->Get(options, handles_[cf], Slice(key), &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  void CompactAll(int cf) {
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[cf], nullptr,
                                nullptr));
  }

  void Compact(int cf, const Slice& start, const Slice& limit) {
    ASSERT_OK(
        db_->CompactRange(CompactRangeOptions(), handles_[cf], &start, &limit));
  }

  int NumTableFilesAtLevel(int level, int cf) {
    return GetProperty(cf,
                       "rocksdb.num-files-at-level" + std::to_string(level));
  }

  // Return spread of files per level
  std::string FilesPerLevel(int cf) {
    std::string result;
    int last_non_zero_offset = 0;
    for (int level = 0; level < dbfull()->NumberLevels(handles_[cf]); level++) {
      int f = NumTableFilesAtLevel(level, cf);
      char buf[100];
      snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
      result += buf;
      if (f > 0) {
        last_non_zero_offset = static_cast<int>(result.size());
      }
    }
    result.resize(last_non_zero_offset);
    return result;
  }

  void AssertFilesPerLevel(const std::string& value, int cf) {
    ASSERT_EQ(value, FilesPerLevel(cf));
  }

  int CountLiveFiles() {
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    return static_cast<int>(metadata.size());
  }

  void AssertCountLiveFiles(int expected_value) {
    ASSERT_EQ(expected_value, CountLiveFiles());
  }

  // Do n memtable flushes, each of which produces an sstable
  // covering the range [small,large].
  void MakeTables(int cf, int n, const std::string& small,
                  const std::string& large) {
    for (int i = 0; i < n; i++) {
      ASSERT_OK(Put(cf, small, "begin"));
      ASSERT_OK(Put(cf, large, "end"));
      ASSERT_OK(db_->Flush(FlushOptions(), handles_[cf]));
    }
  }

  int CountLiveLogFiles() {
    int micros_wait_for_log_deletion = 20000;
    env_->SleepForMicroseconds(micros_wait_for_log_deletion);
    int ret = 0;
    VectorLogPtr wal_files;
    Status s;
    // GetSortedWalFiles is a flakey function -- it gets all the wal_dir
    // children files and then later checks for their existence. if some of the
    // log files doesn't exist anymore, it reports an error. it does all of this
    // without DB mutex held, so if a background process deletes the log file
    // while the function is being executed, it returns an error. We retry the
    // function 10 times to avoid the error failing the test
    for (int retries = 0; retries < 10; ++retries) {
      wal_files.clear();
      s = db_->GetSortedWalFiles(wal_files);
      if (s.ok()) {
        break;
      }
    }
    EXPECT_OK(s);
    for (const auto& wal : wal_files) {
      if (wal->Type() == kAliveLogFile) {
        ++ret;
      }
    }
    return ret;
  }

  void AssertCountLiveLogFiles(int value) {
    ASSERT_EQ(value, CountLiveLogFiles());
  }

  void AssertNumberOfImmutableMemtables(std::vector<int> num_per_cf) {
    assert(num_per_cf.size() == handles_.size());

    for (size_t i = 0; i < num_per_cf.size(); ++i) {
      ASSERT_EQ(num_per_cf[i], GetProperty(static_cast<int>(i),
                                           "rocksdb.num-immutable-mem-table"));
    }
  }

  void CopyFile(const std::string& source, const std::string& destination,
                uint64_t size = 0) {
    const EnvOptions soptions;
    std::unique_ptr<SequentialFile> srcfile;
    ASSERT_OK(env_->NewSequentialFile(source, &srcfile, soptions));
    std::unique_ptr<WritableFile> destfile;
    ASSERT_OK(env_->NewWritableFile(destination, &destfile, soptions));

    if (size == 0) {
      // default argument means copy everything
      ASSERT_OK(env_->GetFileSize(source, &size));
    }

    char buffer[4096];
    Slice slice;
    while (size > 0) {
      uint64_t one = std::min(uint64_t(sizeof(buffer)), size);
      ASSERT_OK(srcfile->Read(one, &slice, buffer));
      ASSERT_OK(destfile->Append(slice));
      size -= slice.size();
    }
    ASSERT_OK(destfile->Close());
  }

  int GetSstFileCount(std::string path) {
    std::vector<std::string> files;
    DBTestBase::GetSstFiles(env_, path, &files);
    return static_cast<int>(files.size());
  }

  void RecalculateWriteStallConditions(
      ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options) {
    // add lock to avoid race condition between
    // `RecalculateWriteStallConditions` which writes to CFStats and
    // background `DBImpl::DumpStats()` threads which read CFStats
    dbfull()->TEST_LockMutex();
    cfd->RecalculateWriteStallConditions(mutable_cf_options);
    dbfull()->TEST_UnlockMutex();
  }

  std::vector<ColumnFamilyHandle*> handles_;
  std::vector<std::string> names_;
  std::vector<std::set<std::string>> keys_;
  ColumnFamilyOptions column_family_options_;
  DBOptions db_options_;
  std::string dbname_;
  std::unique_ptr<DB> db_;
  EnvCounter* env_;
  std::shared_ptr<Env> env_guard_;
  Random rnd_;
  uint32_t format_;
};

class ColumnFamilyTest
    : public ColumnFamilyTestBase,
      virtual public ::testing::WithParamInterface<uint32_t> {
 public:
  ColumnFamilyTest() : ColumnFamilyTestBase(GetParam()) {}
};

INSTANTIATE_TEST_CASE_P(FormatDef, ColumnFamilyTest,
                        testing::Values(test::kDefaultFormatVersion));
INSTANTIATE_TEST_CASE_P(
    FormatLatest, ColumnFamilyTest,
    testing::Values(BlockBasedTableOptions::kLatestSupportedFormatVersion));

TEST_P(ColumnFamilyTest, DontReuseColumnFamilyID) {
  for (int iter = 0; iter < 3; ++iter) {
    Open();
    CreateColumnFamilies({"one", "two", "three"});
    for (size_t i = 0; i < handles_.size(); ++i) {
      auto cfh = static_cast_with_check<ColumnFamilyHandleImpl>(handles_[i]);
      ASSERT_EQ(i, cfh->GetID());
    }
    if (iter == 1) {
      Reopen();
    }
    DropColumnFamilies({3});
    Reopen();
    if (iter == 2) {
      // this tests if max_column_family is correctly persisted with
      // WriteSnapshot()
      Reopen();
    }
    CreateColumnFamilies({"three2"});
    // ID 3 that was used for dropped column family "three" should not be
    // reused
    auto cfh3 = static_cast_with_check<ColumnFamilyHandleImpl>(handles_[3]);
    ASSERT_EQ(4U, cfh3->GetID());
    Close();
    Destroy();
  }
}

TEST_P(ColumnFamilyTest, CreateCFRaceWithGetAggProperty) {
  Open();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WriteOptionsFile:1",
        "ColumnFamilyTest.CreateCFRaceWithGetAggProperty:1"},
       {"ColumnFamilyTest.CreateCFRaceWithGetAggProperty:2",
        "DBImpl::WriteOptionsFile:2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread thread(
      [&] { CreateColumnFamilies({"one"}); });

  TEST_SYNC_POINT("ColumnFamilyTest.CreateCFRaceWithGetAggProperty:1");
  uint64_t pv;
  db_->GetAggregatedIntProperty(DB::Properties::kEstimateTableReadersMem, &pv);
  TEST_SYNC_POINT("ColumnFamilyTest.CreateCFRaceWithGetAggProperty:2");

  thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

class FlushEmptyCFTestWithParam
    : public ColumnFamilyTestBase,
      virtual public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  FlushEmptyCFTestWithParam()
      : ColumnFamilyTestBase(std::get<0>(GetParam())),
        allow_2pc_(std::get<1>(GetParam())) {}

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  bool allow_2pc_;
};

TEST_P(FlushEmptyCFTestWithParam, FlushEmptyCFTest) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  db_options_.allow_2pc = allow_2pc_;
  Open();
  CreateColumnFamilies({"one", "two"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1

  Reopen();
  // Log file A is not dropped after reopening because default column family's
  // min log number is 0.
  // It flushes to SST file X
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 2
  ASSERT_OK(Put(1, "bar", "v2"));  // seqID 3
  // Current log file is file B now. While flushing, a new log file C is created
  // and is set to current. Boths' min log number is set to file C in memory, so
  // after flushing file B is deleted. At the same time, the min log number of
  // default CF is not written to manifest. Log file A still remains.
  // Flushed to SST file Y.
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(1, "bar", "v3"));  // seqID 4
  ASSERT_OK(Put(1, "foo", "v4"));  // seqID 5
  ASSERT_OK(db_->FlushWAL(/*sync=*/false));

  // Preserve file system state up to here to simulate a crash condition.
  fault_env->SetFilesystemActive(false);
  std::vector<std::string> names;
  for (const auto& name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }

  Close();
  fault_env->ResetState();

  // Before opening, there are four files:
  //   Log file A contains seqID 1
  //   Log file C contains seqID 4, 5
  //   SST file X contains seqID 1
  //   SST file Y contains seqID 2, 3
  // Min log number:
  //   default CF: 0
  //   CF one, two: C
  // When opening the DB, all the seqID should be preserved.
  Open(names, {});
  ASSERT_EQ("v4", Get(1, "foo"));
  ASSERT_EQ("v3", Get(1, "bar"));
  Close();

  db_options_.env = env_;
}

TEST_P(FlushEmptyCFTestWithParam, FlushEmptyCFTest2) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  db_options_.allow_2pc = allow_2pc_;
  Open();
  CreateColumnFamilies({"one", "two"});
  // Generate log file A.
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 1

  Reopen();
  // Log file A is not dropped after reopening because default column family's
  // min log number is 0.
  // It flushes to SST file X
  ASSERT_OK(Put(1, "foo", "v1"));  // seqID 2
  ASSERT_OK(Put(1, "bar", "v2"));  // seqID 3
  // Current log file is file B now. While flushing, a new log file C is created
  // and is set to current. Both CFs' min log number is set to file C so after
  // flushing file B is deleted. Log file A still remains.
  // Flushed to SST file Y.
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(0, "bar", "v2"));  // seqID 4
  ASSERT_OK(Put(2, "bar", "v2"));  // seqID 5
  ASSERT_OK(Put(1, "bar", "v3"));  // seqID 6
  // Flushing all column families. This forces all CFs' min log to current. This
  // is written to the manifest file. Log file C is cleared.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));
  // Write to log file D
  ASSERT_OK(Put(1, "bar", "v4"));  // seqID 7
  ASSERT_OK(Put(1, "bar", "v5"));  // seqID 8
  ASSERT_OK(db_->FlushWAL(/*sync=*/false));
  // Preserve file system state up to here to simulate a crash condition.
  fault_env->SetFilesystemActive(false);
  std::vector<std::string> names;
  for (const auto& name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }

  Close();
  fault_env->ResetState();
  // Before opening, there are two logfiles:
  //   Log file A contains seqID 1
  //   Log file D contains seqID 7, 8
  // Min log number:
  //   default CF: D
  //   CF one, two: D
  // When opening the DB, log file D should be replayed using the seqID
  // specified in the file.
  Open(names, {});
  ASSERT_EQ("v1", Get(1, "foo"));
  ASSERT_EQ("v5", Get(1, "bar"));
  Close();

  db_options_.env = env_;
}

INSTANTIATE_TEST_CASE_P(
    FormatDef, FlushEmptyCFTestWithParam,
    testing::Values(std::make_tuple(test::kDefaultFormatVersion, true),
                    std::make_tuple(test::kDefaultFormatVersion, false)));
INSTANTIATE_TEST_CASE_P(
    FormatLatest, FlushEmptyCFTestWithParam,
    testing::Values(
        std::make_tuple(BlockBasedTableOptions::kLatestSupportedFormatVersion,
                        true),
        std::make_tuple(BlockBasedTableOptions::kLatestSupportedFormatVersion,
                        false)));

TEST_P(ColumnFamilyTest, AddDrop) {
  Open();
  CreateColumnFamilies({"one", "two", "three"});
  ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
  ASSERT_EQ("NOT_FOUND", Get(2, "fodor"));
  DropColumnFamilies({2});
  ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
  CreateColumnFamilies({"four"});
  ASSERT_EQ("NOT_FOUND", Get(3, "fodor"));
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  ASSERT_EQ("NOT_FOUND", Get(3, "fodor"));
  Close();
  ASSERT_TRUE(TryOpen({"default"}).IsInvalidArgument());
  Open({"default", "one", "three", "four"});
  DropColumnFamilies({1});
  Reopen();
  Close();

  std::vector<std::string> families;
  ASSERT_OK(DB::ListColumnFamilies(db_options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  ASSERT_TRUE(families ==
              std::vector<std::string>({"default", "four", "three"}));
}

TEST_P(ColumnFamilyTest, EmptyNameRejected) {
  // Creating a column family with an empty-string name should be rejected with
  // InvalidArgument. Empty strings are reserved by various RocksDB APIs and
  // serialization formats to mean "unknown/unspecified column family" (e.g.
  // TablePropertiesCollectorFactory::Context::kUnknownColumnFamily, table
  // properties).
  //
  // Prior behavior was silently broken: CreateColumnFamily("") would return
  // OK and a usable handle, but the CF was not persisted in the manifest --
  // data written to it was lost on DB reopen.
  Open();
  ColumnFamilyHandle* handle = nullptr;
  Status s =
      db_->CreateColumnFamily(column_family_options_, std::string(), &handle);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ(handle, nullptr);

  // Same via the bulk-by-names API. The first valid name should still get
  // created; the failure should come on the empty one.
  std::vector<ColumnFamilyHandle*> handles;
  s = db_->CreateColumnFamilies(column_family_options_,
                                {"ok_name", "", "another"}, &handles);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  // Clean up the partially-created handle ("ok_name") before the failure.
  ASSERT_EQ(handles.size(), 1U);
  ASSERT_OK(db_->DropColumnFamily(handles[0]));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handles[0]));
  handles.clear();

  // Same via the bulk-by-descriptor API.
  s = db_->CreateColumnFamilies(
      {ColumnFamilyDescriptor("", column_family_options_)}, &handles);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_TRUE(handles.empty());
}

TEST_P(ColumnFamilyTest, BulkAddDrop) {
  constexpr int kNumCF = 1000;
  ColumnFamilyOptions cf_options;
  WriteOptions write_options;
  db_options_.write_buffer_manager = std::make_shared<WriteBufferManager>(
      1 << 30, nullptr, false, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  Open();
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyHandle*> cf_handles;
  for (int i = 1; i <= kNumCF; i++) {
    cf_names.push_back("cf1-" + std::to_string(i));
  }
  ASSERT_OK(db_->CreateColumnFamilies(cf_options, cf_names, &cf_handles));
  for (int i = 1; i <= kNumCF; i++) {
    ASSERT_OK(db_->Put(write_options, cf_handles[i - 1], "foo", "bar"));
  }
  std::atomic<int> per_removal_refreshes{0};
  std::atomic<int> bulk_refreshes{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ColumnFamilySet::RemoveColumnFamily:Refresh",
      [&](void*) { ++per_removal_refreshes; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ColumnFamilySet::EndBulkFlushableMemAccountingUpdate:Refresh",
      [&](void*) { ++bulk_refreshes; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });
  ASSERT_OK(db_->DropColumnFamilies(cf_handles));
  EXPECT_EQ(0, per_removal_refreshes.load());
  EXPECT_EQ(1, bulk_refreshes.load());
  std::vector<ColumnFamilyDescriptor> cf_descriptors;
  for (auto* handle : cf_handles) {
    delete handle;
  }
  cf_handles.clear();
  for (int i = 1; i <= kNumCF; i++) {
    cf_descriptors.emplace_back("cf2-" + std::to_string(i),
                                ColumnFamilyOptions());
  }
  ASSERT_OK(db_->CreateColumnFamilies(cf_descriptors, &cf_handles));
  for (int i = 1; i <= kNumCF; i++) {
    ASSERT_OK(db_->Put(write_options, cf_handles[i - 1], "foo", "bar"));
  }
  bulk_refreshes.store(0);
  ASSERT_OK(db_->DropColumnFamilies(cf_handles));
  EXPECT_EQ(0, per_removal_refreshes.load());
  EXPECT_EQ(1, bulk_refreshes.load());
  for (auto* handle : cf_handles) {
    delete handle;
  }
  Close();
  std::vector<std::string> families;
  ASSERT_OK(DB::ListColumnFamilies(db_options_, dbname_, &families));
  std::sort(families.begin(), families.end());
  ASSERT_TRUE(families == std::vector<std::string>({"default"}));
}

TEST_P(ColumnFamilyTest, DropTest) {
  // first iteration - don't reopen DB before dropping
  // second iteration - reopen DB before dropping
  for (int iter = 0; iter < 2; ++iter) {
    Open({"default"});
    CreateColumnFamiliesAndReopen({"pikachu"});
    for (int i = 0; i < 100; ++i) {
      ASSERT_OK(Put(1, std::to_string(i), "bar" + std::to_string(i)));
    }
    ASSERT_OK(Flush(1));

    if (iter == 1) {
      Reopen();
    }
    ASSERT_EQ("bar1", Get(1, "1"));

    AssertCountLiveFiles(1);
    DropColumnFamilies({1});
    // make sure that all files are deleted when we drop the column family
    AssertCountLiveFiles(0);
    Destroy();
  }
}

TEST_P(ColumnFamilyTest, WriteBatchFailure) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  WriteBatch batch;
  ASSERT_OK(batch.Put(handles_[0], Slice("existing"), Slice("column-family")));
  ASSERT_OK(
      batch.Put(handles_[1], Slice("non-existing"), Slice("column-family")));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  DropColumnFamilies({1});
  WriteOptions woptions_ignore_missing_cf;
  woptions_ignore_missing_cf.ignore_missing_column_families = true;
  ASSERT_OK(
      batch.Put(handles_[0], Slice("still here"), Slice("column-family")));
  ASSERT_OK(db_->Write(woptions_ignore_missing_cf, &batch));
  ASSERT_EQ("column-family", Get(0, "still here"));
  Status s = db_->Write(WriteOptions(), &batch);
  ASSERT_TRUE(s.IsInvalidArgument());
  Close();
}

TEST_P(ColumnFamilyTest, ReadWrite) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_OK(Put(2, "fodor", "v5"));

  for (int iter = 0; iter <= 3; ++iter) {
    ASSERT_EQ("v2", Get(0, "foo"));
    ASSERT_EQ("v2", Get(0, "bar"));
    ASSERT_EQ("v3", Get(1, "mirko"));
    ASSERT_EQ("v5", Get(2, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(0, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(2, "foo"));
    if (iter <= 1) {
      Reopen();
    }
  }
  Close();
}

TEST_P(ColumnFamilyTest, IgnoreRecoveredLog) {
  std::string backup_logs = dbname_ + "/backup_logs";

  // delete old files in backup_logs directory
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  ASSERT_OK(env_->CreateDirIfMissing(backup_logs));
  std::vector<std::string> old_files;
  ASSERT_OK(env_->GetChildren(backup_logs, &old_files));
  for (auto& file : old_files) {
    ASSERT_OK(env_->DeleteFile(backup_logs + "/" + file));
  }

  column_family_options_.merge_operator =
      MergeOperators::CreateUInt64AddOperator();
  db_options_.wal_dir = dbname_ + "/logs";
  Destroy();
  Open();
  CreateColumnFamilies({"cf1", "cf2"});

  // fill up the DB
  std::string one;
  std::string two;
  std::string three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);
  ASSERT_OK(Merge(0, "foo", one));
  ASSERT_OK(Merge(1, "mirko", one));
  ASSERT_OK(Merge(0, "foo", one));
  ASSERT_OK(Merge(2, "bla", one));
  ASSERT_OK(Merge(2, "fodor", one));
  ASSERT_OK(Merge(0, "bar", one));
  ASSERT_OK(Merge(2, "bla", one));
  ASSERT_OK(Merge(1, "mirko", two));
  ASSERT_OK(Merge(1, "franjo", one));

  // copy the logs to backup
  std::vector<std::string> logs;
  ASSERT_OK(env_->GetChildren(db_options_.wal_dir, &logs));
  for (auto& log : logs) {
    CopyFile(db_options_.wal_dir + "/" + log, backup_logs + "/" + log);
  }

  // recover the DB
  Close();

  // 1. check consistency
  // 2. copy the logs from backup back to WAL dir. if the recovery happens
  // again on the same log files, this should lead to incorrect results
  // due to applying merge operator twice
  // 3. check consistency
  for (int iter = 0; iter < 2; ++iter) {
    // assert consistency
    Open({"default", "cf1", "cf2"});
    ASSERT_EQ(two, Get(0, "foo"));
    ASSERT_EQ(one, Get(0, "bar"));
    ASSERT_EQ(three, Get(1, "mirko"));
    ASSERT_EQ(one, Get(1, "franjo"));
    ASSERT_EQ(one, Get(2, "fodor"));
    ASSERT_EQ(two, Get(2, "bla"));
    Close();

    if (iter == 0) {
      // copy the logs from backup back to wal dir
      for (auto& log : logs) {
        CopyFile(backup_logs + "/" + log, db_options_.wal_dir + "/" + log);
      }
    }
  }
}

TEST_P(ColumnFamilyTest, FlushTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(0, "bar", "v2"));
  ASSERT_OK(Put(1, "mirko", "v3"));
  ASSERT_OK(Put(0, "foo", "v2"));
  ASSERT_OK(Put(2, "fodor", "v5"));

  for (int j = 0; j < 2; j++) {
    ReadOptions ro;
    std::vector<Iterator*> iterators;
    // Hold super version.
    if (j == 0) {
      ASSERT_OK(db_->NewIterators(ro, handles_, &iterators));
    }

    for (int i = 0; i < 3; ++i) {
      uint64_t max_total_in_memory_state = MaxTotalInMemoryState();
      ASSERT_OK(Flush(i));
      AssertMaxTotalInMemoryState(max_total_in_memory_state);
    }
    ASSERT_OK(Put(1, "foofoo", "bar"));
    ASSERT_OK(Put(0, "foofoo", "bar"));

    for (auto* it : iterators) {
      ASSERT_OK(it->status());
      delete it;
    }
  }
  Reopen();

  for (int iter = 0; iter <= 2; ++iter) {
    ASSERT_EQ("v2", Get(0, "foo"));
    ASSERT_EQ("v2", Get(0, "bar"));
    ASSERT_EQ("v3", Get(1, "mirko"));
    ASSERT_EQ("v5", Get(2, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(0, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(1, "fodor"));
    ASSERT_EQ("NOT_FOUND", Get(2, "foo"));
    if (iter <= 1) {
      Reopen();
    }
  }
  Close();
}

// Makes sure that obsolete log files get deleted
TEST_P(ColumnFamilyTest, LogDeletionTest) {
  db_options_.max_total_wal_size = std::numeric_limits<uint64_t>::max();
  column_family_options_.arena_block_size = 4 * 1024;
  column_family_options_.write_buffer_size = 128000;  // 128KB
  Open();
  CreateColumnFamilies({"one", "two", "three", "four"});
  // Each bracket is one log file. if number is in (), it means
  // we don't need it anymore (it's been flushed)
  // []
  AssertCountLiveLogFiles(0);
  PutRandomData(0, 1, 128);
  // [0]
  PutRandomData(1, 1, 128);
  // [0, 1]
  PutRandomData(1, 1000, 128);
  WaitForFlush(1);
  // [0, (1)] [1]
  AssertCountLiveLogFiles(2);
  PutRandomData(0, 1, 128);
  // [0, (1)] [0, 1]
  AssertCountLiveLogFiles(2);
  PutRandomData(2, 1, 128);
  // [0, (1)] [0, 1, 2]
  PutRandomData(2, 1000, 128);
  WaitForFlush(2);
  // [0, (1)] [0, 1, (2)] [2]
  AssertCountLiveLogFiles(3);
  PutRandomData(2, 1000, 128);
  WaitForFlush(2);
  // [0, (1)] [0, 1, (2)] [(2)] [2]
  AssertCountLiveLogFiles(4);
  PutRandomData(3, 1, 128);
  // [0, (1)] [0, 1, (2)] [(2)] [2, 3]
  PutRandomData(1, 1, 128);
  // [0, (1)] [0, 1, (2)] [(2)] [1, 2, 3]
  AssertCountLiveLogFiles(4);
  PutRandomData(1, 1000, 128);
  WaitForFlush(1);
  // [0, (1)] [0, (1), (2)] [(2)] [(1), 2, 3] [1]
  AssertCountLiveLogFiles(5);
  PutRandomData(0, 1000, 128);
  WaitForFlush(0);
  // [(0), (1)] [(0), (1), (2)] [(2)] [(1), 2, 3] [1, (0)] [0]
  // delete obsolete logs -->
  // [(1), 2, 3] [1, (0)] [0]
  AssertCountLiveLogFiles(3);
  PutRandomData(0, 1000, 128);
  WaitForFlush(0);
  // [(1), 2, 3] [1, (0)], [(0)] [0]
  AssertCountLiveLogFiles(4);
  PutRandomData(1, 1000, 128);
  WaitForFlush(1);
  // [(1), 2, 3] [(1), (0)] [(0)] [0, (1)] [1]
  AssertCountLiveLogFiles(5);
  PutRandomData(2, 1000, 128);
  WaitForFlush(2);
  // [(1), (2), 3] [(1), (0)] [(0)] [0, (1)] [1, (2)], [2]
  AssertCountLiveLogFiles(6);
  PutRandomData(3, 1000, 128);
  WaitForFlush(3);
  // [(1), (2), (3)] [(1), (0)] [(0)] [0, (1)] [1, (2)], [2, (3)] [3]
  // delete obsolete logs -->
  // [0, (1)] [1, (2)], [2, (3)] [3]
  AssertCountLiveLogFiles(4);
  Close();
}

TEST_P(ColumnFamilyTest, CrashAfterFlush) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  db_options_.env = fault_env.get();
  Open();
  CreateColumnFamilies({"one"});

  WriteBatch batch;
  ASSERT_OK(batch.Put(handles_[0], Slice("foo"), Slice("bar")));
  ASSERT_OK(batch.Put(handles_[1], Slice("foo"), Slice("bar")));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(Flush(0));
  fault_env->SetFilesystemActive(false);

  std::vector<std::string> names;
  for (const auto& name : names_) {
    if (name != "") {
      names.push_back(name);
    }
  }
  Close();
  ASSERT_OK(fault_env->DropUnsyncedFileData());
  fault_env->ResetState();
  Open(names, {});

  // Write batch should be atomic.
  ASSERT_EQ(Get(0, "foo"), Get(1, "foo"));

  Close();
  db_options_.env = env_;
}

TEST_P(ColumnFamilyTest, OpenNonexistentColumnFamily) {
  ASSERT_OK(TryOpen({"default"}));
  Close();
  ASSERT_TRUE(TryOpen({"default", "dne"}).IsInvalidArgument());
}

// Makes sure that obsolete log files get deleted
TEST_P(ColumnFamilyTest, DifferentWriteBufferSizes) {
  // disable flushing stale column families
  db_options_.max_total_wal_size = std::numeric_limits<uint64_t>::max();
  Open();
  CreateColumnFamilies({"one", "two", "three"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  ColumnFamilyOptions two;
  ColumnFamilyOptions three;
  // setup options. all column families have max_write_buffer_number setup to 10
  // "default" -> 100KB memtable, start flushing immediately
  // "one" -> 200KB memtable, start flushing with two immutable memtables
  // "two" -> 1MB memtable, start flushing with three immutable memtables
  // "three" -> 90KB memtable, start flushing with four immutable memtables
  default_cf.write_buffer_size = 100000;
  default_cf.arena_block_size = 4 * 4096;
  default_cf.max_write_buffer_number = 10;
  default_cf.min_write_buffer_number_to_merge = 1;
  default_cf.max_write_buffer_size_to_maintain = 0;
  one.write_buffer_size = 200000;
  one.arena_block_size = 4 * 4096;
  one.max_write_buffer_number = 10;
  one.min_write_buffer_number_to_merge = 2;
  one.max_write_buffer_size_to_maintain =
      static_cast<int>(one.write_buffer_size);
  two.write_buffer_size = 1000000;
  two.arena_block_size = 4 * 4096;
  two.max_write_buffer_number = 10;
  two.min_write_buffer_number_to_merge = 3;
  two.max_write_buffer_size_to_maintain =
      static_cast<int>(two.write_buffer_size);
  three.write_buffer_size = 4096 * 22;
  three.arena_block_size = 4096;
  three.max_write_buffer_number = 10;
  three.min_write_buffer_number_to_merge = 4;
  three.max_write_buffer_size_to_maintain =
      static_cast<int>(three.write_buffer_size);

  Reopen({default_cf, one, two, three});

  int micros_wait_for_flush = 10000;
  PutRandomData(0, 100, 1000);
  WaitForFlush(0);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(1);
  PutRandomData(1, 200, 1000);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 0, 0});
  AssertCountLiveLogFiles(2);
  PutRandomData(2, 1000, 1000);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 1, 0});
  AssertCountLiveLogFiles(3);
  PutRandomData(2, 1000, 1000);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 0});
  AssertCountLiveLogFiles(4);
  PutRandomData(3, 93, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 1});
  AssertCountLiveLogFiles(5);
  PutRandomData(3, 88, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 2});
  AssertCountLiveLogFiles(6);
  PutRandomData(3, 88, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 2, 3});
  AssertCountLiveLogFiles(7);
  PutRandomData(0, 100, 1000);
  WaitForFlush(0);
  AssertNumberOfImmutableMemtables({0, 1, 2, 3});
  AssertCountLiveLogFiles(8);
  PutRandomData(2, 100, 10000);
  WaitForFlush(2);
  AssertNumberOfImmutableMemtables({0, 1, 0, 3});
  AssertCountLiveLogFiles(9);
  PutRandomData(3, 88, 990);
  WaitForFlush(3);
  AssertNumberOfImmutableMemtables({0, 1, 0, 0});
  AssertCountLiveLogFiles(10);
  PutRandomData(3, 88, 990);
  env_->SleepForMicroseconds(micros_wait_for_flush);
  AssertNumberOfImmutableMemtables({0, 1, 0, 1});
  AssertCountLiveLogFiles(11);
  PutRandomData(1, 200, 1000);
  WaitForFlush(1);
  AssertNumberOfImmutableMemtables({0, 0, 0, 1});
  AssertCountLiveLogFiles(5);
  PutRandomData(3, 88 * 3, 990);
  WaitForFlush(3);
  PutRandomData(3, 88 * 4, 990);
  WaitForFlush(3);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(12);
  PutRandomData(0, 100, 1000);
  WaitForFlush(0);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(12);
  PutRandomData(2, 3 * 1000, 1000);
  WaitForFlush(2);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(12);
  PutRandomData(1, 2 * 200, 1000);
  WaitForFlush(1);
  AssertNumberOfImmutableMemtables({0, 0, 0, 0});
  AssertCountLiveLogFiles(7);
  Close();
}

// The test is commented out because we want to test that snapshot is
// not created for memtables not supported it, but There isn't a memtable
// that doesn't support snapshot right now. If we have one later, we can
// re-enable the test.
//
//   TEST_P(ColumnFamilyTest, MemtableNotSupportSnapshot) {
//   db_options_.allow_concurrent_memtable_write = false;
//   Open();
//   auto* s1 = dbfull()->GetSnapshot();
//   ASSERT_TRUE(s1 != nullptr);
//   dbfull()->ReleaseSnapshot(s1);

//   // Add a column family that doesn't support snapshot
//   ColumnFamilyOptions first;
//   first.memtable_factory.reset(new DummyMemtableNotSupportingSnapshot());
//   CreateColumnFamilies({"first"}, {first});
//   auto* s2 = dbfull()->GetSnapshot();
//   ASSERT_TRUE(s2 == nullptr);

//   // Add a column family that supports snapshot. Snapshot stays not
//   supported. ColumnFamilyOptions second; CreateColumnFamilies({"second"},
//   {second}); auto* s3 = dbfull()->GetSnapshot(); ASSERT_TRUE(s3 == nullptr);
//   Close();
// }

class TestComparator : public Comparator {
  int Compare(const ROCKSDB_NAMESPACE::Slice& /*a*/,
              const ROCKSDB_NAMESPACE::Slice& /*b*/) const override {
    return 0;
  }
  const char* Name() const override { return "Test"; }
  void FindShortestSeparator(
      std::string* /*start*/,
      const ROCKSDB_NAMESPACE::Slice& /*limit*/) const override {}
  void FindShortSuccessor(std::string* /*key*/) const override {}
};

static TestComparator third_comparator;
static TestComparator fourth_comparator;

// Test that we can retrieve the comparator from a created CF
TEST_P(ColumnFamilyTest, GetComparator) {
  Open();
  // Add a column family with no comparator specified
  CreateColumnFamilies({"first"});
  const Comparator* comp = handles_[0]->GetComparator();
  ASSERT_EQ(comp, BytewiseComparator());

  // Add three column families - one with no comparator and two
  // with comparators specified
  ColumnFamilyOptions second;
  ColumnFamilyOptions third;
  ColumnFamilyOptions fourth;
  second.comparator = &third_comparator;
  third.comparator = &fourth_comparator;
  CreateColumnFamilies({"second", "third", "fourth"}, {second, third, fourth});
  ASSERT_EQ(handles_[1]->GetComparator(), BytewiseComparator());
  ASSERT_EQ(handles_[2]->GetComparator(), &third_comparator);
  ASSERT_EQ(handles_[3]->GetComparator(), &fourth_comparator);
  Close();
}

TEST_P(ColumnFamilyTest, DifferentMergeOperators) {
  Open();
  CreateColumnFamilies({"first", "second"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions first;
  ColumnFamilyOptions second;
  first.merge_operator = MergeOperators::CreateUInt64AddOperator();
  second.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen({default_cf, first, second});

  std::string one;
  std::string two;
  std::string three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);

  ASSERT_OK(Put(0, "foo", two));
  ASSERT_OK(Put(0, "foo", one));
  ASSERT_TRUE(Merge(0, "foo", two).IsNotSupported());
  ASSERT_EQ(Get(0, "foo"), one);

  ASSERT_OK(Put(1, "foo", two));
  ASSERT_OK(Put(1, "foo", one));
  ASSERT_OK(Merge(1, "foo", two));
  ASSERT_EQ(Get(1, "foo"), three);

  ASSERT_OK(Put(2, "foo", two));
  ASSERT_OK(Put(2, "foo", one));
  ASSERT_OK(Merge(2, "foo", two));
  ASSERT_EQ(Get(2, "foo"), one + "," + two);
  Close();
}

TEST_P(ColumnFamilyTest, DifferentCompactionStyles) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  ColumnFamilyOptions two;
  db_options_.max_open_files = 20;  // only 10 files in file cache

  default_cf.level_compaction_dynamic_level_bytes = false;
  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = static_cast<uint64_t>(1) << 60;

  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.level_compaction_dynamic_level_bytes = false;
  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 1; ++i) {
    PutRandomData(1, 10, 12000);
    PutRandomData(1, 1, 10);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger - 1; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(std::to_string(i + 1), 2);
  }

  // TRIGGER compaction "one"
  PutRandomData(1, 10, 12000);
  PutRandomData(1, 1, 10);

  // TRIGGER compaction "two"
  PutRandomData(2, 10, 12000);
  PutRandomData(2, 1, 10);

  // WAIT for compactions
  WaitForCompaction();

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);

  Close();
}

// Sync points not supported in RocksDB Lite

TEST_P(ColumnFamilyTest, MultipleManualCompactions) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  ColumnFamilyOptions two;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.level_compaction_dynamic_level_bytes = false;
  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.level_compaction_dynamic_level_bytes = false;
  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }
  std::atomic_bool cf_1_1{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::MultiManual:4", "ColumnFamilyTest::MultiManual:1"},
       {"ColumnFamilyTest::MultiManual:2", "ColumnFamilyTest::MultiManual:5"},
       {"ColumnFamilyTest::MultiManual:2", "ColumnFamilyTest::MultiManual:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:4");
          TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:3");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  std::vector<port::Thread> threads;
  threads.emplace_back([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(std::to_string(i + 1), 2);
  }
  threads.emplace_back([&] {
    TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:1");
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[2], nullptr, nullptr));
    TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:2");
  });

  TEST_SYNC_POINT("ColumnFamilyTest::MultiManual:5");
  for (auto& t : threads) {
    t.join();
  }

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

TEST_P(ColumnFamilyTest, AutomaticAndManualCompactions) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  ColumnFamilyOptions two;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.level_compaction_dynamic_level_bytes = false;
  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();

  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.level_compaction_dynamic_level_bytes = false;
  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  std::atomic_bool cf_1_1{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::AutoManual:4", "ColumnFamilyTest::AutoManual:1"},
       {"ColumnFamilyTest::AutoManual:2", "ColumnFamilyTest::AutoManual:5"},
       {"ColumnFamilyTest::AutoManual:2", "ColumnFamilyTest::AutoManual:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:4");
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:3");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:1");

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(std::to_string(i + 1), 2);
  }
  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[2], nullptr, nullptr));
    TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:2");
  });

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:5");
  threads.join();

  // WAIT for compactions
  WaitForCompaction();

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, ManualAndAutomaticCompactions) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  ColumnFamilyOptions two;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.level_compaction_dynamic_level_bytes = false;
  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();

  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  two.level_compaction_dynamic_level_bytes = false;
  two.compaction_style = kCompactionStyleLevel;
  two.num_levels = 4;
  two.level0_file_num_compaction_trigger = 3;
  two.write_buffer_size = 100000;

  Reopen({default_cf, one, two});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }
  std::atomic_bool cf_1_1{true};
  std::atomic_bool cf_1_2{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:1"},
       {"ColumnFamilyTest::ManualAuto:5", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:2", "ColumnFamilyTest::ManualAuto:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:4");
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:3");
        } else if (cf_1_2.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:2");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:1");

  // SETUP column family "two" -- level style with 4 levels
  for (int i = 0; i < two.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(2, 10, 12000);
    PutRandomData(2, 1, 10);
    WaitForFlush(2);
    AssertFilesPerLevel(std::to_string(i + 1), 2);
  }
  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:5");
  threads.join();

  // WAIT for compactions
  WaitForCompaction();

  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);

  // VERIFY compaction "two"
  AssertFilesPerLevel("0,1", 2);
  CompactAll(2);
  AssertFilesPerLevel("0,1", 2);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, SameCFManualManualCompactions) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }
  std::atomic_bool cf_1_1{true};
  std::atomic_bool cf_1_2{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualManual:4", "ColumnFamilyTest::ManualManual:2"},
       {"ColumnFamilyTest::ManualManual:4", "ColumnFamilyTest::ManualManual:5"},
       {"ColumnFamilyTest::ManualManual:1", "ColumnFamilyTest::ManualManual:2"},
       {"ColumnFamilyTest::ManualManual:1",
        "ColumnFamilyTest::ManualManual:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:4");
          TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:3");
        } else if (cf_1_2.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:2");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = true;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:5");

  WaitForFlush(1);

  // Add more L0 files and force another manual compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(
        std::to_string(one.level0_file_num_compaction_trigger + i), 1);
  }

  ROCKSDB_NAMESPACE::port::Thread threads1([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualManual:1");

  threads.join();
  threads1.join();
  WaitForCompaction();
  // VERIFY compaction "one"
  ASSERT_LE(NumTableFilesAtLevel(0, 1), 2);

  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, SameCFManualAutomaticCompactions) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }
  std::atomic_bool cf_1_1{true};
  std::atomic_bool cf_1_2{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:5"},
       {"ColumnFamilyTest::ManualAuto:1", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:1", "ColumnFamilyTest::ManualAuto:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:4");
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:3");
        } else if (cf_1_2.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:2");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:5");

  WaitForFlush(1);

  // Add more L0 files and force automatic compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(
        std::to_string(one.level0_file_num_compaction_trigger + i), 1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:1");

  threads.join();
  WaitForCompaction();
  // VERIFY compaction "one"
  ASSERT_LE(NumTableFilesAtLevel(0, 1), 2);

  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, SameCFManualAutomaticCompactionsLevel) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleLevel;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 3;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // SETUP column family "one" -- level style
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }
  std::atomic_bool cf_1_1{true};
  std::atomic_bool cf_1_2{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:2"},
       {"ColumnFamilyTest::ManualAuto:4", "ColumnFamilyTest::ManualAuto:5"},
       {"ColumnFamilyTest::ManualAuto:3", "ColumnFamilyTest::ManualAuto:2"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "ColumnFamilyTest::ManualAuto:3"},
       {"ColumnFamilyTest::ManualAuto:1", "ColumnFamilyTest::ManualAuto:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:4");
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:3");
        } else if (cf_1_2.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:2");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  });

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:5");

  // Add more L0 files and force automatic compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(
        std::to_string(one.level0_file_num_compaction_trigger + i), 1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::ManualAuto:1");

  threads.join();
  WaitForCompaction();
  // VERIFY compaction "one"
  AssertFilesPerLevel("0,1", 1);

  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

// In this test, we generate enough files to trigger automatic compactions.
// The automatic compaction waits in NonTrivial:AfterRun
// We generate more files and then trigger an automatic compaction
// This will wait because the automatic compaction has files it needs.
// Once the conflict is hit, the automatic compaction starts and ends
// Then the manual will run and end.
TEST_P(ColumnFamilyTest, SameCFAutomaticManualCompactions) {
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  db_options_.max_open_files = 20;  // only 10 files in file cache
  db_options_.max_background_compactions = 3;

  default_cf.compaction_style = kCompactionStyleLevel;
  default_cf.num_levels = 3;
  default_cf.write_buffer_size = 64 << 10;  // 64KB
  default_cf.target_file_size_base = 30 << 10;
  default_cf.max_compaction_bytes = default_cf.target_file_size_base * 1100;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  ;
  table_options.no_block_cache = true;
  default_cf.table_factory.reset(NewBlockBasedTableFactory(table_options));

  one.compaction_style = kCompactionStyleUniversal;

  one.num_levels = 1;
  // trigger compaction if there are >= 4 files
  one.level0_file_num_compaction_trigger = 4;
  one.write_buffer_size = 120000;

  Reopen({default_cf, one});
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  std::atomic_bool cf_1_1{true};
  std::atomic_bool cf_1_2{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyTest::AutoManual:4", "ColumnFamilyTest::AutoManual:2"},
       {"ColumnFamilyTest::AutoManual:4", "ColumnFamilyTest::AutoManual:5"},
       {"CompactionPicker::CompactRange:Conflict",
        "ColumnFamilyTest::AutoManual:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (cf_1_1.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:4");
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:3");
        } else if (cf_1_2.exchange(false)) {
          TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:2");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // SETUP column family "one" -- universal style
  for (int i = 0; i < one.level0_file_num_compaction_trigger; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
    AssertFilesPerLevel(std::to_string(i + 1), 1);
  }

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:5");

  // Add another L0 file and force automatic compaction
  for (int i = 0; i < one.level0_file_num_compaction_trigger - 2; ++i) {
    PutRandomData(1, 10, 12000, true);
    PutRandomData(1, 1, 10, true);
    WaitForFlush(1);
  }

  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = false;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));

  TEST_SYNC_POINT("ColumnFamilyTest::AutoManual:1");

  WaitForCompaction();
  // VERIFY compaction "one"
  AssertFilesPerLevel("1", 1);
  // Compare against saved keys
  std::set<std::string>::iterator key_iter = keys_[1].begin();
  while (key_iter != keys_[1].end()) {
    ASSERT_NE("NOT_FOUND", Get(1, *key_iter));
    key_iter++;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

namespace {
std::string IterStatus(Iterator* iter) {
  std::string result;
  if (iter->Valid()) {
    result = iter->key().ToString() + "->" + iter->value().ToString();
  } else {
    EXPECT_OK(iter->status());
    result = "(invalid)";
  }
  return result;
}
}  // anonymous namespace

TEST_P(ColumnFamilyTest, NewIteratorsTest) {
  // iter == 0 -- no tailing
  // iter == 2 -- tailing
  for (int iter = 0; iter < 2; ++iter) {
    Open();
    CreateColumnFamiliesAndReopen({"one", "two"});
    ASSERT_OK(Put(0, "a", "b"));
    ASSERT_OK(Put(1, "b", "a"));
    ASSERT_OK(Put(2, "c", "m"));
    ASSERT_OK(Put(2, "v", "t"));
    std::vector<Iterator*> iterators;
    ReadOptions options;
    options.tailing = (iter == 1);
    ASSERT_OK(db_->NewIterators(options, handles_, &iterators));

    for (auto it : iterators) {
      it->SeekToFirst();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "a->b");
    ASSERT_EQ(IterStatus(iterators[1]), "b->a");
    ASSERT_EQ(IterStatus(iterators[2]), "c->m");

    ASSERT_OK(Put(1, "x", "x"));

    for (auto it : iterators) {
      it->Next();
    }

    ASSERT_EQ(IterStatus(iterators[0]), "(invalid)");
    if (iter == 0) {
      // no tailing
      ASSERT_EQ(IterStatus(iterators[1]), "(invalid)");
    } else {
      // tailing
      ASSERT_EQ(IterStatus(iterators[1]), "x->x");
    }
    ASSERT_EQ(IterStatus(iterators[2]), "v->t");

    for (auto it : iterators) {
      delete it;
    }
    Destroy();
  }
}

TEST_P(ColumnFamilyTest, ReadOnlyDBTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two", "three", "four"});
  ASSERT_OK(Put(0, "a", "b"));
  ASSERT_OK(Put(1, "foo", "bla"));
  ASSERT_OK(Put(2, "foo", "blabla"));
  ASSERT_OK(Put(3, "foo", "blablabla"));
  ASSERT_OK(Put(4, "foo", "blablablabla"));

  DropColumnFamilies({2});
  Close();
  // open only a subset of column families
  AssertOpenReadOnly({"default", "one", "four"});
  ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
  ASSERT_EQ("bla", Get(1, "foo"));
  ASSERT_EQ("blablablabla", Get(2, "foo"));

  // test newiterators
  {
    std::vector<Iterator*> iterators;
    ASSERT_OK(db_->NewIterators(ReadOptions(), handles_, &iterators));
    for (auto it : iterators) {
      it->SeekToFirst();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "a->b");
    ASSERT_EQ(IterStatus(iterators[1]), "foo->bla");
    ASSERT_EQ(IterStatus(iterators[2]), "foo->blablablabla");
    for (auto it : iterators) {
      it->Next();
    }
    ASSERT_EQ(IterStatus(iterators[0]), "(invalid)");
    ASSERT_EQ(IterStatus(iterators[1]), "(invalid)");
    ASSERT_EQ(IterStatus(iterators[2]), "(invalid)");

    for (auto it : iterators) {
      delete it;
    }
  }

  Close();
  // can't open dropped column family
  Status s = OpenReadOnly({"default", "one", "two"});
  ASSERT_TRUE(!s.ok());

  // Can't open without specifying default column family
  s = OpenReadOnly({"one", "four"});
  ASSERT_TRUE(!s.ok());
}

TEST_P(ColumnFamilyTest, DontRollEmptyLogs) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two", "three", "four"});

  for (size_t i = 0; i < handles_.size(); ++i) {
    PutRandomData(static_cast<int>(i), 10, 100);
  }
  int num_writable_file_start = env_->GetNumberOfNewWritableFileCalls();
  // this will trigger the flushes
  for (int i = 0; i <= 4; ++i) {
    ASSERT_OK(Flush(i));
  }

  for (int i = 0; i < 4; ++i) {
    WaitForFlush(i);
  }
  int total_new_writable_files =
      env_->GetNumberOfNewWritableFileCalls() - num_writable_file_start;
  ASSERT_EQ(static_cast<size_t>(total_new_writable_files), handles_.size() + 1);
  Close();
}

TEST_P(ColumnFamilyTest, FlushStaleColumnFamilies) {
  Open();
  CreateColumnFamilies({"one", "two"});
  ColumnFamilyOptions default_cf;
  ColumnFamilyOptions one;
  ColumnFamilyOptions two;
  default_cf.write_buffer_size = 100000;  // small write buffer size
  default_cf.arena_block_size = 4096;
  default_cf.disable_auto_compactions = true;
  one.disable_auto_compactions = true;
  two.disable_auto_compactions = true;
  db_options_.max_total_wal_size = 210000;

  Reopen({default_cf, one, two});

  PutRandomData(2, 1, 10);  // 10 bytes
  for (int i = 0; i < 2; ++i) {
    PutRandomData(0, 100, 1000);  // flush
    WaitForFlush(0);

    AssertCountLiveFiles(i + 1);
  }
  // third flush. now, CF [two] should be detected as stale and flushed
  // column family 1 should not be flushed since it's empty
  PutRandomData(0, 100, 1000);  // flush
  WaitForFlush(0);
  WaitForFlush(2);
  // at least 3 files for default column families, 1 file for column family
  // [two], zero files for column family [one], because it's empty
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_GE(metadata.size(), 4);
  bool has_cf1_sst = false;
  bool has_cf2_sst = false;
  for (const auto& file : metadata) {
    if (file.column_family_name == "one") {
      has_cf1_sst = true;
    } else if (file.column_family_name == "two") {
      has_cf2_sst = true;
    }
  }
  ASSERT_FALSE(has_cf1_sst);
  ASSERT_TRUE(has_cf2_sst);

  ASSERT_OK(Flush(0));
  ASSERT_EQ(0, dbfull()->TEST_wals_total_size());
  Close();
}

namespace {
struct CountOptionsFilesFs : public FileSystemWrapper {
  explicit CountOptionsFilesFs(const std::shared_ptr<FileSystem>& t)
      : FileSystemWrapper(t) {}
  const char* Name() const override { return "CountOptionsFilesFs"; }

  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override {
    if (f.find("OPTIONS-") != std::string::npos) {
      options_files_created.fetch_add(1, std::memory_order_relaxed);
    }
    return FileSystemWrapper::NewWritableFile(f, file_opts, r, dbg);
  }

  std::atomic<int> options_files_created{};
};

struct FailOptionsFilesFs : public FileSystemWrapper {
  explicit FailOptionsFilesFs(const std::shared_ptr<FileSystem>& t)
      : FileSystemWrapper(t) {}
  const char* Name() const override { return "FailOptionsFilesFs"; }

  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override {
    if (fail_options_writes.load(std::memory_order_relaxed) &&
        f.find("OPTIONS-") != std::string::npos) {
      return IOStatus::IOError("injected OPTIONS-file creation failure");
    }
    return FileSystemWrapper::NewWritableFile(f, file_opts, r, dbg);
  }

  std::atomic<bool> fail_options_writes{false};
};
}  // namespace

TEST_P(ColumnFamilyTest, TrackedOptionsWriteFailureDoesNotChangeManifest) {
  db_options_.track_options_file_number_in_manifest = true;
  db_options_.max_manifest_file_size = 1;
  db_options_.max_manifest_space_amp_pct = 0;
  auto fail_fs =
      std::make_shared<FailOptionsFilesFs>(db_options_.env->GetFileSystem());
  auto fail_env =
      std::make_unique<CompositeEnvWrapper>(db_options_.env, fail_fs);
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, fail_env.get());

  Open();
  OptionsFileManifestState before_failure;
  ASSERT_OK(VersionSet::GetOptionsFileManifestState(
      dbname_, db_options_.env->GetFileSystem().get(), &before_failure));
  fail_fs->fail_options_writes.store(true, std::memory_order_relaxed);
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_NOK(db_->CreateColumnFamily(ColumnFamilyOptions(), "one", &handle));
  ASSERT_EQ(nullptr, handle);
  fail_fs->fail_options_writes.store(false, std::memory_order_relaxed);

  OptionsFileManifestState after_failure;
  ASSERT_OK(VersionSet::GetOptionsFileManifestState(
      dbname_, db_options_.env->GetFileSystem().get(), &after_failure));
  ASSERT_EQ(before_failure.has_committed_options_file_number,
            after_failure.has_committed_options_file_number);
  ASSERT_EQ(before_failure.committed_options_file_number,
            after_failure.committed_options_file_number);

  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  LoadLatestColumnFamilyDescriptors(&loaded_cfs);
  ASSERT_EQ(1U, loaded_cfs.size());
  ASSERT_EQ(kDefaultColumnFamilyName, loaded_cfs.front().name);

  const uint64_t old_manifest_number =
      dbfull()->GetVersionSet()->manifest_file_number();
  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_NE(old_manifest_number,
            dbfull()->GetVersionSet()->manifest_file_number());

  OptionsFileManifestState after_rotation;
  ASSERT_OK(VersionSet::GetOptionsFileManifestState(
      dbname_, db_options_.env->GetFileSystem().get(), &after_rotation));
  ASSERT_EQ(before_failure.has_committed_options_file_number,
            after_rotation.has_committed_options_file_number);
  ASSERT_EQ(before_failure.committed_options_file_number,
            after_rotation.committed_options_file_number);
  Close();

  AssertColumnFamilyNames({kDefaultColumnFamilyName});
}

TEST_P(ColumnFamilyTest, TrackedDropFailsBeforeManifestOnOptionsError) {
  db_options_.track_options_file_number_in_manifest = true;
  auto fail_fs =
      std::make_shared<FailOptionsFilesFs>(db_options_.env->GetFileSystem());
  auto fail_env =
      std::make_unique<CompositeEnvWrapper>(db_options_.env, fail_fs);
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, fail_env.get());

  Open();
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "one", &handle));
  ASSERT_NE(nullptr, handle);

  fail_fs->fail_options_writes.store(true, std::memory_order_relaxed);
  ASSERT_NOK(db_->DropColumnFamily(handle));
  fail_fs->fail_options_writes.store(false, std::memory_order_relaxed);
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
  Close();

  AssertColumnFamilyNames({kDefaultColumnFamilyName, "one"});
}

TEST_P(ColumnFamilyTest, TrackedManifestFailureKeepsPreviousOptions) {
  db_options_.track_options_file_number_in_manifest = true;
  auto fault_fs =
      std::make_shared<FaultInjectionTestFS>(db_options_.env->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, fault_env.get());

  Open();
  std::string selected_options_before;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, db_options_.env,
                                     &selected_options_before));

  std::atomic<bool> inject_error{true};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (inject_error.exchange(false)) {
          fault_fs->SetThreadLocalErrorContext(
              FaultInjectionIOType::kWrite, /*seed=*/0, /*one_in=*/1,
              /*retryable=*/false, /*has_data_loss=*/false);
          fault_fs->EnableThreadLocalErrorInjection(
              FaultInjectionIOType::kWrite);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ColumnFamilyHandle* handle = nullptr;
  ASSERT_NOK(db_->CreateColumnFamily(ColumnFamilyOptions(), "one", &handle));
  ASSERT_EQ(nullptr, handle);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  std::string selected_options_after;
  ASSERT_OK(GetLatestOptionsFileName(dbname_, db_options_.env,
                                     &selected_options_after));
  ASSERT_EQ(selected_options_before, selected_options_after);
  Close();

  AssertColumnFamilyNames({kDefaultColumnFamilyName});
}

TEST_P(ColumnFamilyTest, FirstTrackedCreateFailureIgnoresUncommittedOptions) {
  // Create a legacy DB without a committed number, then enable tracking while
  // Open also
  // needs to create a missing CF. The current topology must be bound before
  // the proposed topology OPTIONS file can become a durable orphan.
  Open();
  Close();
  db_options_.track_options_file_number_in_manifest = true;
  db_options_.create_missing_column_families = true;

  auto fault_fs =
      std::make_shared<FaultInjectionTestFS>(db_options_.env->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, fault_env.get());

  bool injected = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CreateColumnFamilyImpl:AfterPersistOptions", [&](void*) {
        if (!injected) {
          injected = true;
          fault_fs->SetThreadLocalErrorContext(
              FaultInjectionIOType::kWrite, /*seed=*/0, /*one_in=*/1,
              /*retryable=*/false, /*has_data_loss=*/false);
          fault_fs->EnableThreadLocalErrorInjection(
              FaultInjectionIOType::kWrite);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_NOK(TryOpen({kDefaultColumnFamilyName, "one"}));
  ASSERT_TRUE(injected);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  LoadLatestColumnFamilyDescriptors(&loaded_cfs);
  ASSERT_EQ(1U, loaded_cfs.size());
  ASSERT_EQ(kDefaultColumnFamilyName, loaded_cfs.front().name);

  OptionsFileManifestState protocol_state;
  ASSERT_OK(VersionSet::GetOptionsFileManifestState(
      dbname_, db_options_.env->GetFileSystem().get(), &protocol_state));
  ASSERT_TRUE(protocol_state.has_committed_options_file_number);
  std::string selected_options;
  ASSERT_OK(
      GetLatestOptionsFileName(dbname_, db_options_.env, &selected_options));
  ASSERT_EQ(OptionsFileName(protocol_state.committed_options_file_number),
            selected_options);

  AssertColumnFamilyNames({kDefaultColumnFamilyName});
}

TEST_P(ColumnFamilyTest, TrackedCreatePersistsCommittedOptions) {
  db_options_.track_options_file_number_in_manifest = true;
  Open();

  ColumnFamilyOptions requested;
  requested.arena_block_size = 0;
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(requested, "one", &handle));
  const ColumnFamilyOptions effective = db_->GetOptions(handle);
  ASSERT_GT(effective.arena_block_size, 0U);

  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  LoadLatestColumnFamilyDescriptors(&loaded_cfs);
  ASSERT_EQ(2U, loaded_cfs.size());
  ASSERT_EQ("one", loaded_cfs[1].name);
  ASSERT_EQ(effective.arena_block_size, loaded_cfs[1].options.arena_block_size);

  ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
}

TEST_P(ColumnFamilyTest, TrackedBatchTopologyWritesOneOptionsSnapshot) {
  db_options_.track_options_file_number_in_manifest = true;
  auto count_fs =
      std::make_shared<CountOptionsFilesFs>(db_options_.env->GetFileSystem());
  auto count_env =
      std::make_unique<CompositeEnvWrapper>(db_options_.env, count_fs);
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, count_env.get());

  Open();
  count_fs->options_files_created.store(0, std::memory_order_relaxed);
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(db_->CreateColumnFamilies(ColumnFamilyOptions(),
                                      {"one", "two", "three"}, &handles));
  ASSERT_EQ(3U, handles.size());
  ASSERT_EQ(1, count_fs->options_files_created.load(std::memory_order_relaxed));

  count_fs->options_files_created.store(0, std::memory_order_relaxed);
  ASSERT_OK(db_->DropColumnFamilies(handles));
  ASSERT_EQ(1, count_fs->options_files_created.load(std::memory_order_relaxed));
  for (auto* handle : handles) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
  }

  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  LoadLatestColumnFamilyDescriptors(&loaded_cfs);
  ASSERT_EQ(1U, loaded_cfs.size());
  ASSERT_EQ(kDefaultColumnFamilyName, loaded_cfs.front().name);
  Close();
}

TEST_P(ColumnFamilyTest, TrackedPartialBatchFiltersOptionsSuperset) {
  db_options_.track_options_file_number_in_manifest = true;
  auto fault_fs =
      std::make_shared<FaultInjectionTestFS>(db_options_.env->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, fault_env.get());

  Open();
  std::atomic<int> manifest_writes{0};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (manifest_writes.fetch_add(1, std::memory_order_relaxed) == 1) {
          fault_fs->SetThreadLocalErrorContext(
              FaultInjectionIOType::kWrite, /*seed=*/0, /*one_in=*/1,
              /*retryable=*/false, /*has_data_loss=*/false);
          fault_fs->EnableThreadLocalErrorInjection(
              FaultInjectionIOType::kWrite);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_NOK(db_->CreateColumnFamilies(ColumnFamilyOptions(), {"one", "two"},
                                       &handles));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
  ASSERT_EQ(1U, handles.size());

  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  LoadLatestColumnFamilyDescriptors(&loaded_cfs);
  ASSERT_EQ(2U, loaded_cfs.size());
  ASSERT_EQ(kDefaultColumnFamilyName, loaded_cfs[0].name);
  ASSERT_EQ("one", loaded_cfs[1].name);

  ConfigOptions config_options;
  config_options.env = db_options_.env;
  DBOptions loaded_db_options;
  ASSERT_OK(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                              &loaded_cfs));
  ASSERT_OK(CheckOptionsCompatibility(config_options, dbname_,
                                      loaded_db_options, loaded_cfs));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handles.front()));
  Close();
}

TEST_P(ColumnFamilyTest, TrackedPartialBatchDropFiltersOptionsSuperset) {
  db_options_.track_options_file_number_in_manifest = true;
  auto fault_fs =
      std::make_shared<FaultInjectionTestFS>(db_options_.env->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, fault_env.get());

  Open();
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(db_->CreateColumnFamilies(ColumnFamilyOptions(), {"one", "two"},
                                      &handles));
  ASSERT_EQ(2U, handles.size());

  std::atomic<int> manifest_writes{0};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (manifest_writes.fetch_add(1, std::memory_order_relaxed) == 1) {
          fault_fs->SetThreadLocalErrorContext(
              FaultInjectionIOType::kWrite, /*seed=*/0, /*one_in=*/1,
              /*retryable=*/false, /*has_data_loss=*/false);
          fault_fs->EnableThreadLocalErrorInjection(
              FaultInjectionIOType::kWrite);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_NOK(db_->DropColumnFamilies(handles));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  LoadLatestColumnFamilyDescriptors(&loaded_cfs);
  ASSERT_EQ(2U, loaded_cfs.size());
  ASSERT_EQ(kDefaultColumnFamilyName, loaded_cfs[0].name);
  ASSERT_EQ("two", loaded_cfs[1].name);
  for (auto* handle : handles) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
  }
  Close();
}

TEST_P(ColumnFamilyTest, InvalidTrackedTopologyChangeCreatesNoOptionsOrphan) {
  db_options_.track_options_file_number_in_manifest = true;
  Open();

  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "one", &handle));
  const size_t before_duplicate_create = CountOptionsFiles();
  ColumnFamilyHandle* duplicate = nullptr;
  ASSERT_TRUE(db_->CreateColumnFamily(ColumnFamilyOptions(), "one", &duplicate)
                  .IsInvalidArgument());
  ASSERT_EQ(nullptr, duplicate);
  ASSERT_EQ(before_duplicate_create, CountOptionsFiles());

  ASSERT_OK(db_->DropColumnFamily(handle));
  const size_t before_duplicate_drop = CountOptionsFiles();
  ASSERT_TRUE(db_->DropColumnFamily(handle).IsInvalidArgument());
  ASSERT_EQ(before_duplicate_drop, CountOptionsFiles());
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
}

TEST_P(ColumnFamilyTest,
       PartialMultiDropConsumesLegacyOptionsPersistenceFailure) {
  auto fail_fs =
      std::make_shared<FailOptionsFilesFs>(db_options_.env->GetFileSystem());
  auto fail_env =
      std::make_unique<CompositeEnvWrapper>(db_options_.env, fail_fs);
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, fail_env.get());

  Open();
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "one", &handle));

  fail_fs->fail_options_writes.store(true, std::memory_order_relaxed);
  Status s = db_->DropColumnFamilies({handle, db_->DefaultColumnFamily()});
  ASSERT_TRUE(s.IsInvalidArgument());
  fail_fs->fail_options_writes.store(false, std::memory_order_relaxed);

  ASSERT_OK(db_->DestroyColumnFamilyHandle(handle));
  Close();
  AssertColumnFamilyNames({kDefaultColumnFamilyName});
}

TEST_P(ColumnFamilyTest, CreateMissingColumnFamilies) {
  // Can't accidentally add CFs to an existing DB
  Open();
  Close();
  ASSERT_FALSE(db_options_.create_missing_column_families);
  ASSERT_NOK(TryOpen({"one", "two"}));

  // Nor accidentally create in a new DB
  Destroy();
  db_options_.create_if_missing = true;
  ASSERT_NOK(TryOpen({"one", "two"}));

  // Only with the option (new DB case)
  db_options_.create_missing_column_families = true;
  // Also setup to count number of options files created (see check below)
  auto my_fs =
      std::make_shared<CountOptionsFilesFs>(db_options_.env->GetFileSystem());
  auto my_env = std::make_unique<CompositeEnvWrapper>(db_options_.env, my_fs);
  SaveAndRestore<Env*> save_restore_env(&db_options_.env, my_env.get());

  ASSERT_OK(TryOpen({"default", "one", "two"}));
  Close();

  // An older version would write an updated options file for each column
  // family created under create_missing_column_families, which would be
  // quadratic I/O in the number of column families.
  ASSERT_EQ(my_fs->options_files_created.load(), 1);

  // Add to existing DB case
  ASSERT_OK(TryOpen({"default", "one", "two", "three", "four"}));
  Close();
  ASSERT_EQ(my_fs->options_files_created.load(), 2);

  Destroy();
  db_options_.track_options_file_number_in_manifest = true;
  my_fs->options_files_created.store(0, std::memory_order_relaxed);
  ASSERT_OK(TryOpen({"default", "one", "two", "three", "four"}));
  Close();
  // A new tracked DB writes its initial snapshot, one proposed batch snapshot,
  // and the normal final DB::Open snapshot, independent of the number of
  // missing CFs.
  ASSERT_EQ(my_fs->options_files_created.load(), 3);
}

TEST_P(ColumnFamilyTest, SanitizeCfOptions) {
  DBOptions db_options;
  for (int s = kCompactionStyleLevel; s <= kCompactionStyleUniversal; ++s) {
    for (int l = 0; l <= 2; l++) {
      for (int i = 1; i <= 3; i++) {
        for (int j = 1; j <= 3; j++) {
          for (int k = 1; k <= 3; k++) {
            ColumnFamilyOptions original;
            original.compaction_style = static_cast<CompactionStyle>(s);
            original.num_levels = l;
            original.level0_stop_writes_trigger = i;
            original.level0_slowdown_writes_trigger = j;
            original.level0_file_num_compaction_trigger = k;
            original.write_buffer_size =
                l * 4 * 1024 * 1024 + i * 1024 * 1024 + j * 1024 + k;

            ColumnFamilyOptions result = SanitizeCfOptions(
                ImmutableDBOptions(db_options), /*read_only*/ false, original);
            ASSERT_TRUE(result.level0_stop_writes_trigger >=
                        result.level0_slowdown_writes_trigger);
            ASSERT_TRUE(result.level0_slowdown_writes_trigger >=
                        result.level0_file_num_compaction_trigger);
            ASSERT_TRUE(result.level0_file_num_compaction_trigger ==
                        original.level0_file_num_compaction_trigger);
            if (s == kCompactionStyleLevel) {
              ASSERT_GE(result.num_levels, 2);
            } else {
              ASSERT_GE(result.num_levels, 1);
              if (original.num_levels >= 1) {
                ASSERT_EQ(result.num_levels, original.num_levels);
              }
            }

            // Make sure Sanitize options sets arena_block_size to 1/8 of
            // the write_buffer_size, rounded up to a multiple of 4k.
            size_t expected_arena_block_size =
                l * 4 * 1024 * 1024 / 8 + i * 1024 * 1024 / 8;
            if (j + k != 0) {
              // not a multiple of 4k, round up 4k
              expected_arena_block_size += 4 * 1024;
            }
            expected_arena_block_size =
                std::min(size_t{1024 * 1024}, expected_arena_block_size);
            ASSERT_EQ(expected_arena_block_size, result.arena_block_size);
          }
        }
      }
    }
  }
}

TEST_P(ColumnFamilyTest, ReadDroppedColumnFamily) {
  // iter 0 -- drop CF, don't reopen
  // iter 1 -- delete CF, reopen
  for (int iter = 0; iter < 2; ++iter) {
    db_options_.create_missing_column_families = true;
    db_options_.max_open_files = 20;
    // delete obsolete files always
    db_options_.delete_obsolete_files_period_micros = 0;
    Open({"default", "one", "two"});
    ColumnFamilyOptions options;
    options.level0_file_num_compaction_trigger = 100;
    options.level0_slowdown_writes_trigger = 200;
    options.level0_stop_writes_trigger = 200;
    options.write_buffer_size = 100000;  // small write buffer size
    Reopen({options, options, options});

    // 1MB should create ~10 files for each CF
    int kKeysNum = 10000;
    PutRandomData(0, kKeysNum, 100);
    PutRandomData(1, kKeysNum, 100);
    PutRandomData(2, kKeysNum, 100);

    {
      std::unique_ptr<Iterator> iterator(
          db_->NewIterator(ReadOptions(), handles_[2]));
      iterator->SeekToFirst();

      if (iter == 0) {
        // Drop CF two
        ASSERT_OK(db_->DropColumnFamily(handles_[2]));
      } else {
        // delete CF two
        ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[2]));
        handles_[2] = nullptr;
      }
      // Make sure iterator created can still be used.
      int count = 0;
      for (; iterator->Valid(); iterator->Next()) {
        ASSERT_OK(iterator->status());
        ++count;
      }
      ASSERT_OK(iterator->status());
      ASSERT_EQ(count, kKeysNum);
    }

    // Add bunch more data to other CFs
    PutRandomData(0, kKeysNum, 100);
    PutRandomData(1, kKeysNum, 100);

    if (iter == 1) {
      Reopen();
    }

    // Since we didn't delete CF handle, RocksDB's contract guarantees that
    // we're still able to read dropped CF
    for (int i = 0; i < 3; ++i) {
      std::unique_ptr<Iterator> iterator(
          db_->NewIterator(ReadOptions(), handles_[i]));
      int count = 0;
      for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        ASSERT_OK(iterator->status());
        ++count;
      }
      ASSERT_OK(iterator->status());
      ASSERT_EQ(count, kKeysNum * ((i == 2) ? 1 : 2));
    }

    Close();
    Destroy();
  }
}

TEST_P(ColumnFamilyTest, LiveIteratorWithDroppedColumnFamily) {
  db_options_.create_missing_column_families = true;
  db_options_.max_open_files = 20;
  // delete obsolete files always
  db_options_.delete_obsolete_files_period_micros = 0;
  Open({"default", "one", "two"});
  ColumnFamilyOptions options;
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 200;
  options.level0_stop_writes_trigger = 200;
  options.write_buffer_size = 100000;  // small write buffer size
  Reopen({options, options, options});

  // 1MB should create ~10 files for each CF
  int kKeysNum = 10000;
  PutRandomData(1, kKeysNum, 100);
  {
    std::unique_ptr<Iterator> iterator(
        db_->NewIterator(ReadOptions(), handles_[1]));
    iterator->SeekToFirst();

    DropColumnFamilies({1});

    // Make sure iterator created can still be used.
    int count = 0;
    for (; iterator->Valid(); iterator->Next()) {
      ASSERT_OK(iterator->status());
      ++count;
    }
    ASSERT_OK(iterator->status());
    ASSERT_EQ(count, kKeysNum);
  }

  Reopen();
  Close();
  Destroy();
}

TEST_P(ColumnFamilyTest, FlushAndDropRaceCondition) {
  db_options_.create_missing_column_families = true;
  Open({"default", "one"});
  ColumnFamilyOptions options;
  options.level0_file_num_compaction_trigger = 100;
  options.level0_slowdown_writes_trigger = 200;
  options.level0_stop_writes_trigger = 200;
  options.max_write_buffer_number = 20;
  options.write_buffer_size = 100000;  // small write buffer size
  Reopen({options, options});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply::ColumnFamilyDrop:0",
        "FlushJob::WriteLevel0Table"},
       {"VersionSet::LogAndApply::ColumnFamilyDrop:1",
        "FlushJob::InstallResults"},
       {"FlushJob::InstallResults",
        "VersionSet::LogAndApply::ColumnFamilyDrop:2"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  test::SleepingBackgroundTask sleeping_task;

  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);
  // Make sure the task is sleeping. Otherwise, it might start to execute
  // after sleeping_task.WaitUntilDone() and cause TSAN warning.
  sleeping_task.WaitUntilSleeping();

  // 1MB should create ~10 files for each CF
  int kKeysNum = 10000;
  PutRandomData(1, kKeysNum, 100);

  std::vector<port::Thread> threads;
  threads.emplace_back([&] { ASSERT_OK(db_->DropColumnFamily(handles_[1])); });

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
  sleeping_task.Reset();
  // now we sleep again. this is just so we're certain that flush job finished
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);
  // Make sure the task is sleeping. Otherwise, it might start to execute
  // after sleeping_task.WaitUntilDone() and cause TSAN warning.
  sleeping_task.WaitUntilSleeping();
  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();

  {
    // Since we didn't delete CF handle, RocksDB's contract guarantees that
    // we're still able to read dropped CF
    std::unique_ptr<Iterator> iterator(
        db_->NewIterator(ReadOptions(), handles_[1]));
    int count = 0;
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      ASSERT_OK(iterator->status());
      ++count;
    }
    ASSERT_OK(iterator->status());
    ASSERT_EQ(count, kKeysNum);
  }
  for (auto& t : threads) {
    t.join();
  }

  Close();
  Destroy();
}

namespace {
std::atomic<int> test_stage(0);
std::atomic<bool> ordered_by_writethread(false);
const int kMainThreadStartPersistingOptionsFile = 1;
const int kChildThreadFinishDroppingColumnFamily = 2;
void DropSingleColumnFamily(ColumnFamilyTest* cf_test, int cf_id,
                            std::vector<Comparator*>* comparators) {
  while (test_stage < kMainThreadStartPersistingOptionsFile &&
         !ordered_by_writethread) {
    Env::Default()->SleepForMicroseconds(100);
  }
  cf_test->DropColumnFamilies({cf_id});
  if ((*comparators)[cf_id]) {
    delete (*comparators)[cf_id];
    (*comparators)[cf_id] = nullptr;
  }
  test_stage = kChildThreadFinishDroppingColumnFamily;
}
}  // anonymous namespace

// This test attempts to set up a race condition in a way that is no longer
// possible, causing the test to hang. If DBImpl::options_mutex_ is removed
// in the future, this test might become relevant again.
TEST_P(ColumnFamilyTest, DISABLED_CreateAndDropRace) {
  const int kCfCount = 5;
  std::vector<ColumnFamilyOptions> cf_opts;
  std::vector<Comparator*> comparators;
  for (int i = 0; i < kCfCount; ++i) {
    cf_opts.emplace_back();
    comparators.push_back(new test::SimpleSuffixReverseComparator());
    cf_opts.back().comparator = comparators.back();
  }
  db_options_.create_if_missing = true;
  db_options_.create_missing_column_families = true;

  auto main_thread_id = std::this_thread::get_id();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PersistRocksDBOptions:start", [&](void* /*arg*/) {
        auto current_thread_id = std::this_thread::get_id();
        // If it's the main thread hitting this sync-point, then it
        // will be blocked until some other thread update the test_stage.
        if (main_thread_id == current_thread_id) {
          test_stage = kMainThreadStartPersistingOptionsFile;
          while (test_stage < kChildThreadFinishDroppingColumnFamily &&
                 !ordered_by_writethread) {
            Env::Default()->SleepForMicroseconds(100);
          }
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::EnterUnbatched:Wait", [&](void* /*arg*/) {
        // This means a thread doing DropColumnFamily() is waiting for
        // other thread to finish persisting options.
        // In such case, we update the test_stage to unblock the main thread.
        ordered_by_writethread = true;
      });

  // Create a database with four column families
  Open({"default", "one", "two", "three"},
       {cf_opts[0], cf_opts[1], cf_opts[2], cf_opts[3]});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Start a thread that will drop the first column family
  // and its comparator
  ROCKSDB_NAMESPACE::port::Thread drop_cf_thread(DropSingleColumnFamily, this,
                                                 1, &comparators);

  DropColumnFamilies({2});

  drop_cf_thread.join();
  Close();
  Destroy();
  for (auto* comparator : comparators) {
    if (comparator) {
      delete comparator;
    }
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, CreateAndDropPeriodicRace) {
  // This is a mini-stress test looking for inconsistency between the set of
  // CFs in the DB, particularly whether any use preserve_internal_time_seconds,
  // and whether that is accurately reflected in the periodic task setup.
  constexpr size_t kNumThreads = 12;
  std::vector<std::thread> threads;
  bool last_cf_on = Random::GetTLSInstance()->OneIn(2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RegisterRecordSeqnoTimeWorker:BeforePeriodicTaskType",
      [&](void* /*arg*/) { std::this_thread::yield(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_EQ(column_family_options_.preserve_internal_time_seconds, 0U);
  ColumnFamilyOptions other_opts = column_family_options_;
  ColumnFamilyOptions last_opts = column_family_options_;
  (last_cf_on ? last_opts : other_opts).preserve_internal_time_seconds =
      1000000;
  Open();

  for (size_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back([this, &other_opts, i]() {
      ColumnFamilyHandle* cfh;
      ASSERT_OK(db_->CreateColumnFamily(other_opts, std::to_string(i), &cfh));
      ASSERT_OK(db_->DropColumnFamily(cfh));
      ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
    });
  }

  ColumnFamilyHandle* last_cfh;
  ASSERT_OK(db_->CreateColumnFamily(last_opts, "last", &last_cfh));

  for (auto& t : threads) {
    t.join();
  }

  bool task_enabled = dbfull()->TEST_GetPeriodicTaskScheduler().TEST_HasTask(
      PeriodicTaskType::kRecordSeqnoTime);
  ASSERT_EQ(last_cf_on, task_enabled);

  ASSERT_OK(db_->DropColumnFamily(last_cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(last_cfh));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, WriteStallSingleColumnFamily) {
  const uint64_t kBaseRate = 800000u;
  db_options_.delayed_write_rate = kBaseRate;
  db_options_.max_background_compactions = 6;

  Open({"default"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();

  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);

  mutable_cf_options.level0_slowdown_writes_trigger = 20;
  mutable_cf_options.level0_stop_writes_trigger = 10000;
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;
  mutable_cf_options.disable_auto_compactions = false;

  auto dbmu = dbfull()->TEST_Mutex();

  vstorage->TEST_set_estimated_compaction_needed_bytes(50, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->TEST_set_estimated_compaction_needed_bytes(201, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(400, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(500, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(450, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(205, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(202, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(201, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(198, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->TEST_set_estimated_compaction_needed_bytes(399, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(599, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(2001, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(3001, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->TEST_set_estimated_compaction_needed_bytes(390, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(100, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->set_l0_delay_trigger_count(100);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(101);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(0);
  vstorage->TEST_set_estimated_compaction_needed_bytes(300, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(101);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(200, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(0);
  vstorage->TEST_set_estimated_compaction_needed_bytes(0, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  mutable_cf_options.disable_auto_compactions = true;
  dbfull()->TEST_write_controler().set_delayed_write_rate(kBaseRate);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage->set_l0_delay_trigger_count(50);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(0, GetDbDelayedWriteRate());
  ASSERT_EQ(kBaseRate, dbfull()->TEST_write_controler().delayed_write_rate());

  vstorage->set_l0_delay_trigger_count(60);
  vstorage->TEST_set_estimated_compaction_needed_bytes(300, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(0, GetDbDelayedWriteRate());
  ASSERT_EQ(kBaseRate, dbfull()->TEST_write_controler().delayed_write_rate());

  mutable_cf_options.disable_auto_compactions = false;
  vstorage->set_l0_delay_trigger_count(70);
  vstorage->TEST_set_estimated_compaction_needed_bytes(500, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->set_l0_delay_trigger_count(71);
  vstorage->TEST_set_estimated_compaction_needed_bytes(501, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());
}

TEST_P(ColumnFamilyTest, CompactionSpeedupSingleColumnFamily) {
  db_options_.max_background_compactions = 6;
  Open({"default"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();

  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);

  // Speed up threshold = min(4 * 2, 4 + (36 - 4)/4) = 8
  mutable_cf_options.level0_file_num_compaction_trigger = 4;
  mutable_cf_options.level0_slowdown_writes_trigger = 36;
  mutable_cf_options.level0_stop_writes_trigger = 50;
  // Speedup threshold = 200 / 4 = 50
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;

  auto dbmu = dbfull()->TEST_Mutex();

  vstorage->TEST_set_estimated_compaction_needed_bytes(40, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(50, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(300, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(45, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(7);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(9);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(6);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  // Speed up threshold = min(4 * 2, 4 + (12 - 4)/4) = 6
  mutable_cf_options.level0_file_num_compaction_trigger = 4;
  mutable_cf_options.level0_slowdown_writes_trigger = 16;
  mutable_cf_options.level0_stop_writes_trigger = 30;

  vstorage->set_l0_delay_trigger_count(5);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(7);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(3);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
}

TEST_P(ColumnFamilyTest, WriteStallTwoColumnFamilies) {
  const uint64_t kBaseRate = 810000u;
  db_options_.delayed_write_rate = kBaseRate;
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  ColumnFamilyData* cfd1 =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  VersionStorageInfo* vstorage1 = cfd1->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);
  mutable_cf_options.level0_slowdown_writes_trigger = 20;
  mutable_cf_options.level0_stop_writes_trigger = 10000;
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;

  MutableCFOptions mutable_cf_options1 = mutable_cf_options;
  mutable_cf_options1.soft_pending_compaction_bytes_limit = 500;

  auto dbmu = dbfull()->TEST_Mutex();

  vstorage->TEST_set_estimated_compaction_needed_bytes(50, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(201, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(600, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(70, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate, GetDbDelayedWriteRate());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(800, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(300, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(700, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());

  vstorage->TEST_set_estimated_compaction_needed_bytes(500, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25 / 1.25, GetDbDelayedWriteRate());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(600, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_TRUE(!IsDbWriteStopped());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_EQ(kBaseRate / 1.25, GetDbDelayedWriteRate());
}

TEST_P(ColumnFamilyTest, CompactionSpeedupTwoColumnFamilies) {
  db_options_.max_background_compactions = 6;
  column_family_options_.soft_pending_compaction_bytes_limit = 200;
  column_family_options_.hard_pending_compaction_bytes_limit = 2000;
  Open();
  CreateColumnFamilies({"one"});
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();

  ColumnFamilyData* cfd1 =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  VersionStorageInfo* vstorage1 = cfd1->current()->storage_info();

  MutableCFOptions mutable_cf_options(column_family_options_);
  // Speed up threshold = min(4 * 2, 4 + (36 - 4)/4) = 8
  mutable_cf_options.level0_file_num_compaction_trigger = 4;
  mutable_cf_options.level0_slowdown_writes_trigger = 36;
  mutable_cf_options.level0_stop_writes_trigger = 30;
  // Speedup threshold = 200 / 4 = 50
  mutable_cf_options.soft_pending_compaction_bytes_limit = 200;
  mutable_cf_options.hard_pending_compaction_bytes_limit = 2000;

  MutableCFOptions mutable_cf_options1 = mutable_cf_options;
  mutable_cf_options1.level0_slowdown_writes_trigger = 16;

  auto dbmu = dbfull()->TEST_Mutex();

  vstorage->TEST_set_estimated_compaction_needed_bytes(40, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(60, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(30, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(70, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->TEST_set_estimated_compaction_needed_bytes(20, dbmu);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->TEST_set_estimated_compaction_needed_bytes(3, dbmu);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(9);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage1->set_l0_delay_trigger_count(2);
  RecalculateWriteStallConditions(cfd1, mutable_cf_options);
  ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());

  vstorage->set_l0_delay_trigger_count(0);
  RecalculateWriteStallConditions(cfd, mutable_cf_options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
}

TEST_P(ColumnFamilyTest, CompactionSpeedupForCompactionDebt) {
  db_options_.max_background_compactions = 6;
  Open();
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  MutableCFOptions mutable_cf_options(column_family_options_);
  mutable_cf_options.soft_pending_compaction_bytes_limit =
      std::numeric_limits<uint64_t>::max();

  auto dbmu = dbfull()->TEST_Mutex();

  {
    // No bottommost data, so debt ratio cannot trigger speedup.
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    vstorage->TEST_set_estimated_compaction_needed_bytes(1048576 /* 1MB */,
                                                         dbmu);
    RecalculateWriteStallConditions(cfd, mutable_cf_options);
    ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  }

  // Add a tiny amount of bottommost data.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  {
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    // Eight bytes is way smaller than bottommost data so definitely does not
    // trigger speedup.
    vstorage->TEST_set_estimated_compaction_needed_bytes(8, dbmu);
    RecalculateWriteStallConditions(cfd, mutable_cf_options);
    ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

    // 1MB is much larger than bottommost level size. However, since it's too
    // small in terms of absolute size, it does not trigger parallel compaction
    // in this case (see GetPendingCompactionBytesForCompactionSpeedup()).
    vstorage->TEST_set_estimated_compaction_needed_bytes(1048576 /* 1MB */,
                                                         dbmu);
    RecalculateWriteStallConditions(cfd, mutable_cf_options);
    ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

    vstorage->TEST_set_estimated_compaction_needed_bytes(
        2 * mutable_cf_options.max_bytes_for_level_base, dbmu);
    RecalculateWriteStallConditions(cfd, mutable_cf_options);
    ASSERT_EQ(6, dbfull()->TEST_BGCompactionsAllowed());
  }
}

TEST_P(ColumnFamilyTest, CompactionSpeedupForMarkedFiles) {
  const int kParallelismLimit = 3;
  class AlwaysCompactTpc : public TablePropertiesCollector {
   public:
    Status Finish(UserCollectedProperties* /* properties */) override {
      return Status::OK();
    }

    UserCollectedProperties GetReadableProperties() const override {
      return UserCollectedProperties{};
    }

    const char* Name() const override { return "AlwaysCompactTpc"; }

    bool NeedCompact() const override { return true; }
  };

  class AlwaysCompactTpcf : public TablePropertiesCollectorFactory {
   public:
    TablePropertiesCollector* CreateTablePropertiesCollector(
        TablePropertiesCollectorFactory::Context /* context */) override {
      return new AlwaysCompactTpc();
    }

    const char* Name() const override { return "AlwaysCompactTpcf"; }
  };

  column_family_options_.num_levels = 2;
  column_family_options_.table_properties_collector_factories.emplace_back(
      std::make_shared<AlwaysCompactTpcf>());
  db_options_.max_background_compactions = kParallelismLimit;
  Open();

  // Make a nonempty last level. Only marked files in upper levels count.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  WaitForCompaction();
  AssertFilesPerLevel("0,1", 0 /* cf */);

  // We should calculate the limit by obtaining the number of env background
  // threads, because the current test case will share the same env
  // with another case that may have already increased the number of
  // background threads which is larger than kParallelismLimit
  const auto limit = env_->GetBackgroundThreads(Env::Priority::LOW);

  // Block the compaction thread pool so marked files accumulate in L0.
  std::vector<std::shared_ptr<test::SleepingBackgroundTask>> sleeping_tasks;
  for (int i = 0; i < limit; i++) {
    sleeping_tasks.emplace_back(
        std::make_shared<test::SleepingBackgroundTask>());
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   sleeping_tasks[i].get(), Env::Priority::LOW);
    sleeping_tasks[i]->WaitUntilSleeping();
  }

  // Zero marked upper-level files. No speedup.
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  AssertFilesPerLevel("0,1", 0 /* cf */);

  // One marked upper-level file. No speedup.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  AssertFilesPerLevel("1,1", 0 /* cf */);

  // Two marked upper-level files. Speedup.
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(kParallelismLimit, dbfull()->TEST_BGCompactionsAllowed());
  AssertFilesPerLevel("2,1", 0 /* cf */);

  for (int i = 0; i < limit; i++) {
    sleeping_tasks[i]->WakeUp();
    sleeping_tasks[i]->WaitUntilDone();
  }
}

TEST_P(ColumnFamilyTest, CreateAndDestroyOptions) {
  std::unique_ptr<ColumnFamilyOptions> cfo(new ColumnFamilyOptions());
  ColumnFamilyHandle* cfh;
  Open();
  ASSERT_OK(db_->CreateColumnFamily(*(cfo.get()), "yoyo", &cfh));
  cfo.reset();
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->DropColumnFamily(cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
}

TEST_P(ColumnFamilyTest, CreateDropAndDestroy) {
  ColumnFamilyHandle* cfh;
  Open();
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "yoyo", &cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->DropColumnFamily(cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
}

TEST_P(ColumnFamilyTest, DropWaitsForUnorderedMemtableWrite) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WriteImpl:UnorderedWriteAfterWriteWAL",
        "ColumnFamilyTest::DropWaitsForUnorderedMemtableWrite:AfterWAL"},
       {"DBImpl::WaitForPendingWrites:BeforeBlock",
        "DBImpl::WriteImpl:BeforeUnorderedWriteMemtable"}});

  db_options_.unordered_write = true;
  Open();
  ColumnFamilyHandle* cfh;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "yoyo", &cfh));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer(
      [&] { ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "bar")); });

  TEST_SYNC_POINT(
      "ColumnFamilyTest::DropWaitsForUnorderedMemtableWrite:AfterWAL");
  ASSERT_OK(db_->DropColumnFamily(cfh));
  writer.join();
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(ColumnFamilyTest, CreateDropAndDestroyWithoutFileDeletion) {
  ColumnFamilyHandle* cfh;
  Open();
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "yoyo", &cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "foo", "bar"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->DisableFileDeletions());
  ASSERT_OK(db_->DropColumnFamily(cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cfh));
}

TEST_P(ColumnFamilyTest, FlushCloseWALFiles) {
  SpecialEnv env(Env::Default());
  db_options_.env = &env;
  db_options_.max_background_flushes = 1;
  column_family_options_.memtable_factory.reset(
      test::NewSpecialSkipListFactory(2));
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkFlush:done", "FlushCloseWALFiles:0"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Block flush jobs from running
  test::SleepingBackgroundTask sleeping_task;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);
  // Make sure the task is sleeping. Otherwise, it might start to execute
  // after sleeping_task.WaitUntilDone() and cause TSAN warning.
  sleeping_task.WaitUntilSleeping();

  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  ASSERT_EQ(2, env.num_open_wal_file_.load());

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
  TEST_SYNC_POINT("FlushCloseWALFiles:0");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(1, env.num_open_wal_file_.load());

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  db_options_.env = env_;
  Close();
}

TEST_P(ColumnFamilyTest, IteratorCloseWALFile1) {
  SpecialEnv env(Env::Default());
  db_options_.env = &env;
  // The DB keeps its Env pointer until Close(). Close it before the local Env
  // is destroyed, including when an ASSERT below exits the test early.
  Defer close_db_on_exit([this] {
    Close();
    db_options_.env = env_;
  });
  db_options_.max_background_flushes = 1;
  // When this option is removed, the test will need re-engineering
  db_options_.background_close_inactive_wals = true;
  column_family_options_.memtable_factory.reset(
      test::NewSpecialSkipListFactory(2));
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  // Create an iterator holding the current super version.
  std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions(), handles_[1]));
  ASSERT_OK(it->status());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkFlush:done",
       "ColumnFamilyTest::IteratorCloseWALFile1:SetupFlushDone"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer disable_sync_points(
      [] { ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing(); });

  // A flush will make `it` hold the last reference of its super version.
  ASSERT_OK(Flush(1));
  // Flush() can return before the background function releases all temporary
  // references to the old super version.
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile1:SetupFlushDone");

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"ColumnFamilyTest::IteratorCloseWALFile1:ReleaseFlush",
       "DBImpl::BackgroundCallFlush:start"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  // Flush jobs close previous WAL files after finishing. Keep the next flush
  // from starting so the iterator destructor is responsible for closing the
  // WAL. Occupying one Env worker is insufficient because the shared Env can
  // have multiple HIGH-priority workers.
  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  ASSERT_EQ(2, env.num_open_wal_file_.load());
  // Deleting the iterator will clear its super version, triggering
  // closing all files
  it.reset();
  ASSERT_EQ(1, env.num_open_wal_file_.load());

  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile1:ReleaseFlush");
  WaitForFlush(1);

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
}

TEST_P(ColumnFamilyTest, IteratorCloseWALFile2) {
  SpecialEnv env(Env::Default());
  // Allow both of flush and purge job to schedule.
  env.SetBackgroundThreads(2, Env::HIGH);
  db_options_.env = &env;
  db_options_.max_background_flushes = 1;
  // When this option is removed, the test will need re-engineering
  db_options_.background_close_inactive_wals = true;
  column_family_options_.memtable_factory.reset(
      test::NewSpecialSkipListFactory(2));
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  // Create an iterator holding the current super version.
  ReadOptions ro;
  ro.background_purge_on_iterator_cleanup = true;
  Iterator* it = db_->NewIterator(ro, handles_[1]);
  ASSERT_OK(it->status());
  // A flush will make `it` hold the last reference of its super version.
  ASSERT_OK(Flush(1));

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"ColumnFamilyTest::IteratorCloseWALFile2:0",
       "DBImpl::BGWorkPurge:start"},
      {"ColumnFamilyTest::IteratorCloseWALFile2:2",
       "DBImpl::BackgroundCallFlush:start"},
      {"DBImpl::BGWorkPurge:end", "ColumnFamilyTest::IteratorCloseWALFile2:1"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  ASSERT_EQ(2, env.num_open_wal_file_.load());
  // Deleting the iterator will clear its super version, triggering
  // closing all files
  delete it;
  ASSERT_EQ(2, env.num_open_wal_file_.load());

  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:0");
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:1");
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:2");
  WaitForFlush(1);
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  db_options_.env = env_;
  Close();
}

TEST_P(ColumnFamilyTest, ForwardIteratorCloseWALFile) {
  SpecialEnv env(Env::Default());
  // Allow both of flush and purge job to schedule.
  env.SetBackgroundThreads(2, Env::HIGH);
  db_options_.env = &env;
  db_options_.max_background_flushes = 1;
  // When this option is removed, the test will need re-engineering
  db_options_.background_close_inactive_wals = true;
  column_family_options_.memtable_factory.reset(
      test::NewSpecialSkipListFactory(3));
  column_family_options_.level0_file_num_compaction_trigger = 2;
  Open();
  CreateColumnFamilies({"one"});
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodar2", "mirko"));
  ASSERT_OK(Flush(1));

  // Create an iterator holding the current super version, as well as
  // the SST file just flushed.
  ReadOptions ro;
  ro.tailing = true;
  ro.background_purge_on_iterator_cleanup = true;
  Iterator* it = db_->NewIterator(ro, handles_[1]);
  // A flush will make `it` hold the last reference of its super version.

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodar2", "mirko"));
  ASSERT_OK(Flush(1));

  WaitForCompaction();

  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));
  ASSERT_OK(Put(0, "fodor", "mirko"));
  ASSERT_OK(Put(1, "fodor", "mirko"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"ColumnFamilyTest::IteratorCloseWALFile2:0",
       "DBImpl::BGWorkPurge:start"},
      {"ColumnFamilyTest::IteratorCloseWALFile2:2",
       "DBImpl::BackgroundCallFlush:start"},
      {"DBImpl::BGWorkPurge:end", "ColumnFamilyTest::IteratorCloseWALFile2:1"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = true;
  ASSERT_OK(db_->Put(wo, handles_[1], "fodor", "mirko"));

  env.delete_count_.store(0);
  ASSERT_EQ(2, env.num_open_wal_file_.load());
  // Deleting the iterator will clear its super version, triggering
  // closing all files
  it->Seek("");
  ASSERT_OK(it->status());

  ASSERT_EQ(2, env.num_open_wal_file_.load());
  ASSERT_EQ(0, env.delete_count_.load());

  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:0");
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:1");
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  ASSERT_EQ(1, env.delete_count_.load());
  TEST_SYNC_POINT("ColumnFamilyTest::IteratorCloseWALFile2:2");
  WaitForFlush(1);
  ASSERT_EQ(1, env.num_open_wal_file_.load());
  ASSERT_EQ(1, env.delete_count_.load());

  delete it;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  Reopen();
  ASSERT_EQ("mirko", Get(0, "fodor"));
  ASSERT_EQ("mirko", Get(1, "fodor"));
  db_options_.env = env_;
  Close();
}

// Disable on windows because SyncWAL requires env->IsSyncThreadSafe()
// to return true which is not so in unbuffered mode.
#ifndef OS_WIN
TEST_P(ColumnFamilyTest, LogSyncConflictFlush) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});

  ASSERT_OK(Put(0, "", ""));
  ASSERT_OK(Put(1, "foo", "bar"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::SyncWAL:BeforeMarkLogsSynced:1",
        "ColumnFamilyTest::LogSyncConflictFlush:1"},
       {"ColumnFamilyTest::LogSyncConflictFlush:2",
        "DBImpl::SyncWAL:BeforeMarkLogsSynced:2"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread thread([&] { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT("ColumnFamilyTest::LogSyncConflictFlush:1");
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Flush(1));

  TEST_SYNC_POINT("ColumnFamilyTest::LogSyncConflictFlush:2");

  thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  Close();
}
#endif

// this test is placed here, because the infrastructure for Column Family
// test is being used to ensure a roll of wal files.
// Basic idea is to test that WAL truncation is being detected and not
// ignored
TEST_P(ColumnFamilyTest, DISABLED_LogTruncationTest) {
  Open();
  CreateColumnFamiliesAndReopen({"one", "two"});

  Build(0, 100);

  // Flush the 0th column family to force a roll of the wal log
  ASSERT_OK(Flush(0));

  // Add some more entries
  Build(100, 100);

  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));

  // collect wal files
  std::vector<std::string> logfs;
  for (size_t i = 0; i < filenames.size(); i++) {
    uint64_t number;
    FileType type;
    if (!(ParseFileName(filenames[i], &number, &type))) {
      continue;
    }

    if (type != kWalFile) {
      continue;
    }

    logfs.push_back(filenames[i]);
  }

  std::sort(logfs.begin(), logfs.end());
  ASSERT_GE(logfs.size(), 2);

  // Take the last but one file, and truncate it
  std::string fpath = dbname_ + "/" + logfs[logfs.size() - 2];
  std::vector<std::string> names_save = names_;

  uint64_t fsize;
  ASSERT_OK(env_->GetFileSize(fpath, &fsize));
  ASSERT_GT(fsize, 0);

  Close();

  std::string backup_logs = dbname_ + "/backup_logs";
  std::string t_fpath = backup_logs + "/" + logfs[logfs.size() - 2];

  ASSERT_OK(env_->CreateDirIfMissing(backup_logs));
  // Not sure how easy it is to make this data driven.
  // need to read back the WAL file and truncate last 10
  // entries
  CopyFile(fpath, t_fpath, fsize - 9180);

  ASSERT_OK(env_->DeleteFile(fpath));
  ASSERT_OK(env_->RenameFile(t_fpath, fpath));

  db_options_.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  OpenReadOnly(names_save);

  CheckMissed();

  Close();

  Open(names_save);

  CheckMissed();

  Close();

  // cleanup
  ASSERT_OK(env_->DeleteDir(backup_logs));
}

TEST_P(ColumnFamilyTest, DefaultCfPathsTest) {
  Open();
  // Leave cf_paths for one column families to be empty.
  // Files should be generated according to db_paths for that
  // column family.
  ColumnFamilyOptions cf_opt1;
  ColumnFamilyOptions cf_opt2;
  cf_opt1.cf_paths.emplace_back(dbname_ + "_one_1",
                                std::numeric_limits<uint64_t>::max());
  CreateColumnFamilies({"one", "two"}, {cf_opt1, cf_opt2});
  Reopen({ColumnFamilyOptions(), cf_opt1, cf_opt2});

  // Fill Column family 1.
  PutRandomData(1, 100, 100);
  ASSERT_OK(Flush(1));

  ASSERT_EQ(1, GetSstFileCount(cf_opt1.cf_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // Fill column family 2
  PutRandomData(2, 100, 100);
  ASSERT_OK(Flush(2));

  // SST from Column family 2 should be generated in
  // db_paths which is dbname_ in this case.
  ASSERT_EQ(1, GetSstFileCount(dbname_));
}

TEST_P(ColumnFamilyTest, MultipleCFPathsTest) {
  Open();
  // Configure Column family specific paths.
  ColumnFamilyOptions cf_opt1;
  ColumnFamilyOptions cf_opt2;
  cf_opt1.cf_paths.emplace_back(dbname_ + "_one_1",
                                std::numeric_limits<uint64_t>::max());
  cf_opt2.cf_paths.emplace_back(dbname_ + "_two_1",
                                std::numeric_limits<uint64_t>::max());
  CreateColumnFamilies({"one", "two"}, {cf_opt1, cf_opt2});
  Reopen({ColumnFamilyOptions(), cf_opt1, cf_opt2});

  PutRandomData(1, 100, 100, true /* save */);
  ASSERT_OK(Flush(1));

  // Check that files are generated in appropriate paths.
  ASSERT_EQ(1, GetSstFileCount(cf_opt1.cf_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  PutRandomData(2, 100, 100, true /* save */);
  ASSERT_OK(Flush(2));

  ASSERT_EQ(1, GetSstFileCount(cf_opt2.cf_paths[0].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // Re-open and verify the keys.
  Reopen({ColumnFamilyOptions(), cf_opt1, cf_opt2});
  for (int cf = 1; cf != 3; ++cf) {
    ReadOptions read_options;
    read_options.readahead_size = 0;
    auto it = db_->NewIterator(read_options, handles_[cf]);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ASSERT_OK(it->status());
      Slice key(it->key());
      ASSERT_NE(keys_[cf].end(), keys_[cf].find(key.ToString()));
    }
    ASSERT_OK(it->status());
    delete it;

    for (const auto& key : keys_[cf]) {
      ASSERT_NE("NOT_FOUND", Get(cf, key));
    }
  }
}

TEST(ColumnFamilyTest, ValidateBlobGCCutoff) {
  DBOptions db_options;

  ColumnFamilyOptions cf_options;
  cf_options.enable_blob_garbage_collection = true;

  cf_options.blob_garbage_collection_age_cutoff = -0.5;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, cf_options)
                  .IsInvalidArgument());

  cf_options.blob_garbage_collection_age_cutoff = 0.0;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.blob_garbage_collection_age_cutoff = 0.5;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.blob_garbage_collection_age_cutoff = 1.0;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.blob_garbage_collection_age_cutoff = 1.5;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, cf_options)
                  .IsInvalidArgument());
}

TEST(ColumnFamilyTest, ValidateBlobGCForceThreshold) {
  DBOptions db_options;

  ColumnFamilyOptions cf_options;
  cf_options.enable_blob_garbage_collection = true;

  cf_options.blob_garbage_collection_force_threshold = -0.5;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, cf_options)
                  .IsInvalidArgument());

  cf_options.blob_garbage_collection_force_threshold = 0.0;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.blob_garbage_collection_force_threshold = 0.5;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.blob_garbage_collection_force_threshold = 1.0;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.blob_garbage_collection_force_threshold = 1.5;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, cf_options)
                  .IsInvalidArgument());
}

TEST(ColumnFamilyTest, ValidateMemtableKVChecksumOption) {
  DBOptions db_options;

  ColumnFamilyOptions cf_options;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.memtable_protection_bytes_per_key = 5;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, cf_options)
                  .IsNotSupported());

  cf_options.memtable_protection_bytes_per_key = 1;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));

  cf_options.memtable_protection_bytes_per_key = 16;
  ASSERT_TRUE(ColumnFamilyData::ValidateOptions(db_options, cf_options)
                  .IsNotSupported());

  cf_options.memtable_protection_bytes_per_key = 0;
  ASSERT_OK(ColumnFamilyData::ValidateOptions(db_options, cf_options));
}

// Tests the flushing behavior of a column family to retain user-defined
// timestamp when `persist_user_defined_timestamp` is false. The behavior of
// auto flush is it makes some effort to retain user-defined timestamps while
// the behavior of manual flush is that it skips retaining UDTs.
class ColumnFamilyRetainUDTTest : public ColumnFamilyTestBase {
 public:
  ColumnFamilyRetainUDTTest()
      : ColumnFamilyTestBase(
            BlockBasedTableOptions::kLatestSupportedFormatVersion) {}

  void SetUp() override {
    db_options_.allow_concurrent_memtable_write = false;
    column_family_options_.comparator =
        test::BytewiseComparatorWithU64TsWrapper();
    column_family_options_.persist_user_defined_timestamps = false;
    ColumnFamilyTestBase::SetUp();
  }

  Status Put(int cf, const std::string& key, const std::string& ts,
             const std::string& value) {
    return db_->Put(WriteOptions(), handles_[cf], Slice(key), Slice(ts),
                    Slice(value));
  }

  std::string Get(int cf, const std::string& key, const std::string& read_ts) {
    ReadOptions ropts;
    Slice timestamp = read_ts;
    ropts.timestamp = &timestamp;
    std::string value;
    Status s = db_->Get(ropts, handles_[cf], Slice(key), &value);
    if (s.IsNotFound()) {
      return "NOT_FOUND";
    } else if (s.ok()) {
      return value;
    }
    return "";
  }

  void CheckEffectiveCutoffTime(uint64_t expected_cutoff) {
    std::string effective_full_history_ts_low;
    EXPECT_OK(
        db_->GetFullHistoryTsLow(handles_[0], &effective_full_history_ts_low));
    EXPECT_EQ(EncodeAsUint64(expected_cutoff), effective_full_history_ts_low);
  }
};

class TestTsComparator : public Comparator {
 public:
  TestTsComparator() : Comparator(8 /*ts_sz*/) {}

  int Compare(const ROCKSDB_NAMESPACE::Slice& /*a*/,
              const ROCKSDB_NAMESPACE::Slice& /*b*/) const override {
    return 0;
  }
  const char* Name() const override { return "TestTs"; }
  void FindShortestSeparator(
      std::string* /*start*/,
      const ROCKSDB_NAMESPACE::Slice& /*limit*/) const override {}
  void FindShortSuccessor(std::string* /*key*/) const override {}
};

TEST_F(ColumnFamilyRetainUDTTest, SanityCheck) {
  Open();
  ColumnFamilyOptions cf_options;
  cf_options.persist_user_defined_timestamps = false;
  TestTsComparator test_comparator;
  cf_options.comparator = &test_comparator;
  ColumnFamilyHandle* handle;
  // Not persisting user-defined timestamps feature only supports user-defined
  // timestamps formatted as uint64_t.
  ASSERT_TRUE(
      db_->CreateColumnFamily(cf_options, "pikachu", &handle).IsNotSupported());

  Destroy();
  // Not persisting user-defined timestamps feature doesn't work in combination
  // with atomic flush.
  db_options_.atomic_flush = true;
  ASSERT_TRUE(TryOpen({"default"}).IsNotSupported());

  // Not persisting user-defined timestamps feature doesn't work in combination
  // with concurrent memtable write.
  db_options_.atomic_flush = false;
  db_options_.allow_concurrent_memtable_write = true;
  ASSERT_TRUE(TryOpen({"default"}).IsNotSupported());
  Close();
}

class AutoFlushRetainUDTTest : public ColumnFamilyRetainUDTTest {};

TEST_F(AutoFlushRetainUDTTest, FullHistoryTsLowNotSet) {
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundFlush:CheckFlushRequest:cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto reschedule_count = *static_cast<int*>(arg);
        ASSERT_EQ(1, reschedule_count);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Open();
  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v1"));
  // No `full_history_ts_low` explicitly set by user, auto flush is continued
  // without checking if its UDTs expired.
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // After flush, `full_history_ts_low` should be automatically advanced to
  // the effective cutoff timestamp: write_ts + 1
  CheckEffectiveCutoffTime(2);
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(AutoFlushRetainUDTTest, AllKeysExpired) {
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundFlush:CheckFlushRequest:cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto reschedule_count = *static_cast<int*>(arg);
        ASSERT_EQ(1, reschedule_count);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Open();
  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v1"));
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(3)));
  // All keys expired w.r.t the configured `full_history_ts_low`, auto flush
  // continue without the need for a re-schedule.
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // `full_history_ts_low` stays unchanged after flush.
  CheckEffectiveCutoffTime(3);
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(AutoFlushRetainUDTTest, NotAllKeysExpiredFlushToAvoidWriteStall) {
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundFlush:CheckFlushRequest:cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto reschedule_count = *static_cast<int*>(arg);
        ASSERT_EQ(1, reschedule_count);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Open();
  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v1"));
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(1)));
  ASSERT_OK(db_->SetOptions(handles_[0], {{"max_write_buffer_number", "1"}}));
  // Not all keys expired, but auto flush is continued without a re-schedule
  // because of risk of write stall.
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // After flush, `full_history_ts_low` should be automatically advanced to
  // the effective cutoff timestamp: write_ts + 1
  CheckEffectiveCutoffTime(2);
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(AutoFlushRetainUDTTest, NotAllKeysExpiredFlushRescheduled) {
  std::atomic<int> local_counter{1};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AfterRetainUDTReschedule:cb", [&](void* /*arg*/) {
        // Increasing full_history_ts_low so all keys expired after the initial
        // FlushRequest is rescheduled
        ASSERT_OK(
            db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(3)));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundFlush:CheckFlushRequest:cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto reschedule_count = *static_cast<int*>(arg);
        ASSERT_EQ(2, reschedule_count);
        local_counter.fetch_add(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Open();
  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v1"));
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(1)));
  // Not all keys expired, and there is no risk of write stall. Flush is
  // rescheduled. The actual flush happens after `full_history_ts_low` is
  // increased to mark all keys expired.
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  // Make sure callback is not skipped.
  ASSERT_EQ(2, local_counter);

  CheckEffectiveCutoffTime(3);
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class ManualFlushSkipRetainUDTTest : public ColumnFamilyRetainUDTTest {
 public:
  // Write an entry with timestamp that is not expired w.r.t cutoff timestamp,
  // and make sure automatic flush would be rescheduled to retain UDT.
  void CheckAutomaticFlushRetainUDT(uint64_t write_ts) {
    std::atomic<int> local_counter{1};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::AfterRetainUDTReschedule:cb", [&](void* /*arg*/) {
          // Increasing full_history_ts_low so all keys expired after the
          // initial FlushRequest is rescheduled
          ASSERT_OK(db_->IncreaseFullHistoryTsLow(
              handles_[0], EncodeAsUint64(write_ts + 1)));
        });
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundFlush:CheckFlushRequest:cb", [&](void* arg) {
          ASSERT_NE(nullptr, arg);
          auto reschedule_count = *static_cast<int*>(arg);
          ASSERT_EQ(2, reschedule_count);
          local_counter.fetch_add(1);
        });
    SyncPoint::GetInstance()->EnableProcessing();
    EXPECT_OK(Put(0, "foo", EncodeAsUint64(write_ts),
                  "foo" + std::to_string(write_ts)));
    EXPECT_OK(dbfull()->TEST_SwitchWAL());
    EXPECT_OK(dbfull()->TEST_WaitForFlushMemTable());
    // Make sure callback is not skipped.
    EXPECT_EQ(2, local_counter);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
};

TEST_F(ManualFlushSkipRetainUDTTest, ManualFlush) {
  Open();
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(0)));

  // Manual flush proceeds without trying to retain UDT.
  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v1"));
  ASSERT_OK(Flush(0));
  CheckEffectiveCutoffTime(2);
  CheckAutomaticFlushRetainUDT(3);

  Close();
}

TEST_F(ManualFlushSkipRetainUDTTest, FlushRemovesStaleEntries) {
  column_family_options_.max_write_buffer_number = 4;
  Open();
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(0)));

  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  ColumnFamilyData* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
  for (int version = 0; version < 100; version++) {
    if (version == 50) {
      ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfd));
    }
    ASSERT_OK(
        Put(0, "foo", EncodeAsUint64(version), "v" + std::to_string(version)));
  }

  ASSERT_OK(Flush(0));
  TablePropertiesCollection tables_properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&tables_properties));
  ASSERT_EQ(1, tables_properties.size());
  std::shared_ptr<const TableProperties> table_properties =
      tables_properties.begin()->second;
  ASSERT_EQ(1, table_properties->num_entries);
  ASSERT_EQ(0, table_properties->num_deletions);
  ASSERT_EQ(0, table_properties->num_range_deletions);
  CheckEffectiveCutoffTime(100);
  CheckAutomaticFlushRetainUDT(101);

  Close();
}

TEST_F(ManualFlushSkipRetainUDTTest, RangeDeletionFlushRemovesStaleEntries) {
  column_family_options_.max_write_buffer_number = 4;
  Open();
  // TODO(yuzhangyu): a non 0 full history ts low is needed for this garbage
  // collection to kick in. This doesn't work well for the very first flush of
  // the column family. Not a big issue, but would be nice to improve this.
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(9)));

  for (int i = 10; i < 100; i++) {
    ASSERT_OK(Put(0, "foo" + std::to_string(i), EncodeAsUint64(i),
                  "val" + std::to_string(i)));
    if (i % 2 == 1) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), "foo" + std::to_string(i - 1),
                                 "foo" + std::to_string(i), EncodeAsUint64(i)));
    }
  }

  ASSERT_OK(Flush(0));
  CheckEffectiveCutoffTime(100);
  std::string read_ts = EncodeAsUint64(100);
  std::string min_ts = EncodeAsUint64(0);
  ReadOptions ropts;
  Slice read_ts_slice = read_ts;
  std::string value;
  ropts.timestamp = &read_ts_slice;
  {
    Iterator* iter = db_->NewIterator(ropts);
    iter->SeekToFirst();
    int i = 11;
    while (iter->Valid()) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("foo" + std::to_string(i), iter->key());
      ASSERT_EQ("val" + std::to_string(i), iter->value());
      ASSERT_EQ(min_ts, iter->timestamp());
      iter->Next();
      i += 2;
    }
    ASSERT_OK(iter->status());
    delete iter;
  }
  TablePropertiesCollection tables_properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&tables_properties));
  ASSERT_EQ(1, tables_properties.size());
  std::shared_ptr<const TableProperties> table_properties =
      tables_properties.begin()->second;
  // 45 point data + 45 range deletions. 45 obsolete point data are garbage
  // collected.
  ASSERT_EQ(90, table_properties->num_entries);
  ASSERT_EQ(45, table_properties->num_deletions);
  ASSERT_EQ(45, table_properties->num_range_deletions);

  Close();
}

TEST_F(ManualFlushSkipRetainUDTTest, ManualCompaction) {
  Open();
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(0)));

  // Manual compaction proceeds without trying to retain UDT.
  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v2"));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[0], nullptr, nullptr));
  CheckEffectiveCutoffTime(2);
  CheckAutomaticFlushRetainUDT(3);

  Close();
}

TEST_F(ManualFlushSkipRetainUDTTest, BulkLoading) {
  Open();
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(0)));
  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v1"));

  // Test flush behavior in bulk loading scenarios.
  Options options(db_options_, column_family_options_);
  std::string sst_files_dir = dbname_ + "/sst_files/";
  ASSERT_OK(DestroyDir(env_, sst_files_dir));
  ASSERT_OK(env_->CreateDir(sst_files_dir));
  SstFileWriter sst_file_writer(EnvOptions(), options);
  std::string file1 = sst_files_dir + "file1.sst";
  ASSERT_OK(sst_file_writer.Open(file1));
  ASSERT_OK(sst_file_writer.Put("foo", EncodeAsUint64(0), "v2"));
  ExternalSstFileInfo file1_info;
  ASSERT_OK(sst_file_writer.Finish(&file1_info));

  // Bulk loading in UDT mode doesn't support external file key range overlap
  // with DB key range.
  ASSERT_TRUE(db_->IngestExternalFile({file1}, IngestExternalFileOptions())
                  .IsInvalidArgument());

  std::string file2 = sst_files_dir + "file2.sst";
  ASSERT_OK(sst_file_writer.Open(file2));
  ASSERT_OK(sst_file_writer.Put("bar", EncodeAsUint64(0), "val"));
  ExternalSstFileInfo file2_info;
  ASSERT_OK(sst_file_writer.Finish(&file2_info));
  // A successful bulk loading, and it doesn't trigger any flush. As a result
  // the effective cutoff timestamp is also unchanged.
  ASSERT_OK(db_->IngestExternalFile({file2}, IngestExternalFileOptions()));

  ASSERT_EQ(Get(0, "foo", EncodeAsUint64(1)), "v1");
  ASSERT_EQ(Get(0, "bar", EncodeAsUint64(0)), "val");
  CheckEffectiveCutoffTime(0);
  CheckAutomaticFlushRetainUDT(1);

  Close();
}

TEST_F(ManualFlushSkipRetainUDTTest, AutomaticFlushQueued) {
  Open();
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(0)));

  ASSERT_OK(Put(0, "foo", EncodeAsUint64(1), "v1"));
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  CheckEffectiveCutoffTime(0);

  // Default `max_write_buffer_number=2` used, writing another memtable can get
  // automatic flush to proceed because of memory pressure. Not doing that so
  // we can test automatic flush gets to proceed because of an ongoing manual
  // flush attempt.
  ASSERT_OK(Flush(0));
  CheckEffectiveCutoffTime(2);
  CheckAutomaticFlushRetainUDT(3);

  Close();
}

TEST_F(ManualFlushSkipRetainUDTTest, ConcurrentManualFlushes) {
  Open();
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(handles_[0], EncodeAsUint64(0)));

  std::vector<ROCKSDB_NAMESPACE::port::Thread> manual_flush_tds;
  std::atomic<int> next_ts{0};
  std::mutex mtx;
  std::condition_variable cv;

  auto manual_flush = [&](int write_ts) {
    {
      std::unique_lock<std::mutex> lock(mtx);
      cv.wait(lock,
              [&write_ts, &next_ts] { return write_ts == next_ts.load(); });
      ASSERT_OK(Put(0, "foo" + std::to_string(write_ts),
                    EncodeAsUint64(write_ts),
                    "val_" + std::to_string(write_ts)));
      next_ts.fetch_add(1);
      cv.notify_all();
    }
    if (write_ts % 2 == 0) {
      ASSERT_OK(Flush(0));
    } else {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[0], nullptr,
                                  nullptr));
    }
  };

  for (int write_ts = 0; write_ts < 10; write_ts++) {
    manual_flush_tds.emplace_back(manual_flush, write_ts);
  }

  for (auto& td : manual_flush_tds) {
    td.join();
  }

  CheckEffectiveCutoffTime(10);
  CheckAutomaticFlushRetainUDT(11);
  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
