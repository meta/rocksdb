//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/blob_db/blob_db.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "port/port.h"
#include "rocksdb/utilities/debug.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/blob_db/blob_db_impl.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE::blob_db {

OutputMetadata NewerVersionOutputMetadata() {
  OutputMetadata output_metadata;
  output_metadata.WantNewerVersionPresent();
  return output_metadata;
}

MultiGetOutputMetadata NewerVersionMultiGetOutputMetadata() {
  MultiGetOutputMetadata output_metadata;
  output_metadata.WantNewerVersionPresent();
  return output_metadata;
}

class BlobDBTest : public testing::Test {
 public:
  const int kMaxBlobSize = 1 << 14;

  struct BlobIndexVersion {
    BlobIndexVersion() = default;
    BlobIndexVersion(std::string _user_key, uint64_t _file_number,
                     uint64_t _expiration, SequenceNumber _sequence,
                     ValueType _type)
        : user_key(std::move(_user_key)),
          file_number(_file_number),
          expiration(_expiration),
          sequence(_sequence),
          type(_type) {}

    std::string user_key;
    uint64_t file_number = kInvalidBlobFileNumber;
    uint64_t expiration = kNoExpiration;
    SequenceNumber sequence = 0;
    ValueType type = kTypeValue;
  };

  BlobDBTest()
      : dbname_(test::PerThreadDBPath("blob_db_test")), blob_db_(nullptr) {
    mock_clock_ = std::make_shared<MockSystemClock>(SystemClock::Default());
    mock_env_.reset(new CompositeEnvWrapper(Env::Default(), mock_clock_));
    fault_injection_env_.reset(new FaultInjectionTestEnv(Env::Default()));

    Status s = DestroyBlobDB(dbname_, Options(), BlobDBOptions());
    assert(s.ok());
  }

  ~BlobDBTest() override {
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Destroy();
  }

  Status TryOpen(BlobDBOptions bdb_options = BlobDBOptions(),
                 Options options = Options()) {
    options.create_if_missing = true;
    if (options.env == mock_env_.get()) {
      // Need to disable stats dumping and persisting which also use
      // RepeatableThread, which uses InstrumentedCondVar::TimedWaitInternal.
      // With mocked time, this can hang on some platforms (MacOS)
      // because (a) on some platforms, pthread_cond_timedwait does not appear
      // to release the lock for other threads to operate if the deadline time
      // is already passed, and (b) TimedWait calls are currently a bad
      // abstraction because the deadline parameter is usually computed from
      // Env time, but is interpreted in real clock time.
      options.stats_dump_period_sec = 0;
      options.stats_persist_period_sec = 0;
    }
    bdb_options_ = bdb_options;
    return BlobDB::Open(options, bdb_options, dbname_, &blob_db_);
  }

  void Open(BlobDBOptions bdb_options = BlobDBOptions(),
            Options options = Options()) {
    ASSERT_OK(TryOpen(bdb_options, options));
  }

  void Reopen(BlobDBOptions bdb_options = BlobDBOptions(),
              Options options = Options()) {
    assert(blob_db_ != nullptr);
    delete blob_db_;
    blob_db_ = nullptr;
    Open(bdb_options, options);
  }

  void Close() {
    assert(blob_db_ != nullptr);
    delete blob_db_;
    blob_db_ = nullptr;
  }

  void Destroy() {
    if (blob_db_) {
      Options options = blob_db_->GetOptions();
      delete blob_db_;
      blob_db_ = nullptr;
      ASSERT_OK(DestroyBlobDB(dbname_, options, bdb_options_));
    }
  }

  BlobDBImpl* blob_db_impl() { return static_cast<BlobDBImpl*>(blob_db_); }

  Status Put(const Slice& key, const Slice& value,
             std::map<std::string, std::string>* data = nullptr) {
    Status s = blob_db_->Put(WriteOptions(), key, value);
    if (data != nullptr) {
      (*data)[key.ToString()] = value.ToString();
    }
    return s;
  }

  void Delete(const std::string& key,
              std::map<std::string, std::string>* data = nullptr) {
    ASSERT_OK(blob_db_->Delete(WriteOptions(), key));
    if (data != nullptr) {
      data->erase(key);
    }
  }

  Status PutWithTTL(const Slice& key, const Slice& value, uint64_t ttl,
                    std::map<std::string, std::string>* data = nullptr) {
    Status s = blob_db_->PutWithTTL(WriteOptions(), key, value, ttl);
    if (data != nullptr) {
      (*data)[key.ToString()] = value.ToString();
    }
    return s;
  }

  void PutRandomWithTTL(const std::string& key, uint64_t ttl, Random* rnd,
                        std::map<std::string, std::string>* data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = rnd->HumanReadableString(len);
    ASSERT_OK(
        blob_db_->PutWithTTL(WriteOptions(), Slice(key), Slice(value), ttl));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandom(const std::string& key, Random* rnd,
                 std::map<std::string, std::string>* data = nullptr) {
    PutRandom(blob_db_, key, rnd, data);
  }

  void PutRandom(DB* db, const std::string& key, Random* rnd,
                 std::map<std::string, std::string>* data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = rnd->HumanReadableString(len);
    ASSERT_OK(db->Put(WriteOptions(), Slice(key), Slice(value)));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  void PutRandomToWriteBatch(
      const std::string& key, Random* rnd, WriteBatch* batch,
      std::map<std::string, std::string>* data = nullptr) {
    int len = rnd->Next() % kMaxBlobSize + 1;
    std::string value = rnd->HumanReadableString(len);
    ASSERT_OK(batch->Put(key, value));
    if (data != nullptr) {
      (*data)[key] = value;
    }
  }

  // Verify blob db contain expected data and nothing more.
  void VerifyDB(const std::map<std::string, std::string>& data) {
    VerifyDB(blob_db_, data);
  }

  void VerifyDB(DB* db, const std::map<std::string, std::string>& data) {
    // Verify normal Get
    auto* cfh = db->DefaultColumnFamily();
    for (auto& p : data) {
      PinnableSlice value_slice;
      ASSERT_OK(db->Get(ReadOptions(), cfh, p.first, &value_slice));
      ASSERT_EQ(p.second, value_slice.ToString());
      std::string value;
      ASSERT_OK(db->Get(ReadOptions(), cfh, p.first, &value));
      ASSERT_EQ(p.second, value);
    }

    // Verify iterators
    Iterator* iter = db->NewIterator(ReadOptions());
    iter->SeekToFirst();
    for (auto& p : data) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(p.first, iter->key().ToString());
      ASSERT_EQ(p.second, iter->value().ToString());
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    delete iter;
  }

  void VerifyBaseDB(
      const std::map<std::string, KeyVersion>& expected_versions) {
    auto* bdb_impl = static_cast<BlobDBImpl*>(blob_db_);
    DB* db = blob_db_->GetRootDB();
    const size_t kMaxKeys = 10000;
    std::vector<KeyVersion> versions;
    ASSERT_OK(GetAllKeyVersions(db, {}, {}, kMaxKeys, &versions));
    ASSERT_EQ(expected_versions.size(), versions.size());
    size_t i = 0;
    for (auto& key_version : expected_versions) {
      const KeyVersion& expected_version = key_version.second;
      ASSERT_EQ(expected_version.user_key, versions[i].user_key);
      ASSERT_EQ(expected_version.sequence, versions[i].sequence);
      ASSERT_EQ(expected_version.type, versions[i].type);
      if (versions[i].type == kTypeValue) {
        ASSERT_EQ(expected_version.value, versions[i].value);
      } else {
        ASSERT_EQ(kTypeBlobIndex, versions[i].type);
        PinnableSlice value;
        ASSERT_OK(bdb_impl->TEST_GetBlobValue(versions[i].user_key,
                                              versions[i].value, &value));
        ASSERT_EQ(expected_version.value, value.ToString());
      }
      i++;
    }
  }

  void VerifyBaseDBBlobIndex(
      const std::map<std::string, BlobIndexVersion>& expected_versions) {
    const size_t kMaxKeys = 10000;
    std::vector<KeyVersion> versions;
    ASSERT_OK(
        GetAllKeyVersions(blob_db_->GetRootDB(), {}, {}, kMaxKeys, &versions));
    ASSERT_EQ(versions.size(), expected_versions.size());

    size_t i = 0;
    for (const auto& expected_pair : expected_versions) {
      const BlobIndexVersion& expected_version = expected_pair.second;

      ASSERT_EQ(versions[i].user_key, expected_version.user_key);
      ASSERT_EQ(versions[i].sequence, expected_version.sequence);
      ASSERT_EQ(versions[i].type, expected_version.type);
      if (versions[i].type != kTypeBlobIndex) {
        ASSERT_EQ(kInvalidBlobFileNumber, expected_version.file_number);
        ASSERT_EQ(kNoExpiration, expected_version.expiration);

        ++i;
        continue;
      }

      BlobIndex blob_index;
      ASSERT_OK(blob_index.DecodeFrom(versions[i].value));

      ASSERT_EQ(blob_index.file_number(), expected_version.file_number);

      const uint64_t expiration =
          blob_index.HasTTL() ? blob_index.expiration() : kNoExpiration;
      ASSERT_EQ(expiration, expected_version.expiration);

      ++i;
    }
  }

  void InsertBlobs() {
    WriteOptions wo;
    std::string value;

    Random rnd(301);
    for (size_t i = 0; i < 100000; i++) {
      uint64_t ttl = rnd.Next() % 86400;
      PutRandomWithTTL("key" + std::to_string(i % 500), ttl, &rnd, nullptr);
    }

    for (size_t i = 0; i < 10; i++) {
      Delete("key" + std::to_string(i % 500));
    }
  }

  const std::string dbname_;
  std::shared_ptr<MockSystemClock> mock_clock_;
  std::unique_ptr<Env> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  BlobDB* blob_db_;
  BlobDBOptions bdb_options_;
};  // class BlobDBTest

TEST_F(BlobDBTest, Put) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, PutWithTTL) {
  Random rnd(301);
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 1000;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  std::map<std::string, std::string> data;
  mock_clock_->SetCurrentTime(50);
  for (size_t i = 0; i < 100; i++) {
    uint64_t ttl = rnd.Next() % 100;
    PutRandomWithTTL("key" + std::to_string(i), ttl, &rnd,
                     (ttl <= 50 ? nullptr : &data));
  }
  mock_clock_->SetCurrentTime(100);
  auto* bdb_impl = static_cast<BlobDBImpl*>(blob_db_);
  auto blob_files = bdb_impl->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_TRUE(blob_files[0]->HasTTL());
  ASSERT_OK(bdb_impl->TEST_CloseBlobFile(blob_files[0]));
  VerifyDB(data);
}

TEST_F(BlobDBTest, StackableDBGet) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i++) {
    StackableDB* db = blob_db_;
    ColumnFamilyHandle* column_family = db->DefaultColumnFamily();
    std::string key = "key" + std::to_string(i);
    PinnableSlice pinnable_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &pinnable_value));
    std::string string_value;
    ASSERT_OK(db->Get(ReadOptions(), column_family, key, &string_value));
    ASSERT_EQ(string_value, pinnable_value.ToString());
    ASSERT_EQ(string_value, data[key]);
  }
}

TEST_F(BlobDBTest, GetWithMetadataResolvesBlobValue) {
  // BlobDB reports newer blob values while returning the snapshot-visible blob.
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  assert(blob_db_ != nullptr);

  ASSERT_OK(Put("key", "old_blob_value"));
  const Snapshot* snapshot = blob_db_->GetSnapshot();
  ASSERT_OK(Put("key", "new_blob_value"));

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::string value;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(
      blob_db_->GetWithMetadata(read_options, "key", &value, &output_metadata));
  ASSERT_EQ("old_blob_value", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  blob_db_->ReleaseSnapshot(snapshot);
}

TEST_F(BlobDBTest, MultiGetWithMetadataResolvesBlobValues) {
  // BlobDB MultiGet preserves per-key newer-version metadata while resolving
  // blob indexes to values.
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  assert(blob_db_ != nullptr);

  ASSERT_OK(Put("key", "old_blob_value"));
  const Snapshot* snapshot = blob_db_->GetSnapshot();
  ASSERT_OK(Put("key", "new_blob_value"));

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::vector<Slice> keys{"key", "missing"};
  std::vector<std::string> values;
  MultiGetOutputMetadata output_metadata = NewerVersionMultiGetOutputMetadata();
  std::vector<Status> statuses = blob_db_->MultiGetWithMetadata(
      read_options, keys, &values, &output_metadata);

  ASSERT_EQ(keys.size(), statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old_blob_value", values[0]);
  ASSERT_TRUE((*output_metadata.newer_version_present)[0]);
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_FALSE((*output_metadata.newer_version_present)[1]);

  blob_db_->ReleaseSnapshot(snapshot);
}

TEST_F(BlobDBTest, GetExpiration) {
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  mock_clock_->SetCurrentTime(100);
  Open(bdb_options, options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(PutWithTTL("key2", "value2", 200));
  PinnableSlice value;
  uint64_t expiration;
  ASSERT_OK(blob_db_->Get(ReadOptions(), "key1", &value, &expiration));
  ASSERT_EQ("value1", value.ToString());
  ASSERT_EQ(kNoExpiration, expiration);
  ASSERT_OK(blob_db_->Get(ReadOptions(), "key2", &value, &expiration));
  ASSERT_EQ("value2", value.ToString());
  ASSERT_EQ(300 /* = 100 + 200 */, expiration);
}

TEST_F(BlobDBTest, GetIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  ColumnFamilyHandle* column_family = blob_db_->DefaultColumnFamily();
  PinnableSlice value;
  ASSERT_OK(Put("foo", "bar"));
  fault_injection_env_->SetFilesystemActive(false, Status::IOError());
  Status s = blob_db_->Get(ReadOptions(), column_family, "foo", &value);
  ASSERT_TRUE(s.IsIOError());
  // Reactivate file system to allow test to close DB.
  fault_injection_env_->SetFilesystemActive(true);
}

TEST_F(BlobDBTest, PutIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options, options);
  fault_injection_env_->SetFilesystemActive(false, Status::IOError());
  ASSERT_TRUE(Put("foo", "v1").IsIOError());
  fault_injection_env_->SetFilesystemActive(true, Status::IOError());
  ASSERT_OK(Put("bar", "v1"));
}

TEST_F(BlobDBTest, WriteBatch) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    WriteBatch batch;
    for (size_t j = 0; j < 10; j++) {
      PutRandomToWriteBatch("key" + std::to_string(j * 100 + i), &rnd, &batch,
                            &data);
    }

    ASSERT_OK(blob_db_->Write(WriteOptions(), &batch));
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, Delete) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  for (size_t i = 0; i < 100; i += 5) {
    Delete("key" + std::to_string(i), &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, DeleteBatch) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd);
  }
  WriteBatch batch;
  for (size_t i = 0; i < 100; i++) {
    ASSERT_OK(batch.Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(blob_db_->Write(WriteOptions(), &batch));
  // DB should be empty.
  VerifyDB({});
}

TEST_F(BlobDBTest, Override) {
  Random rnd(301);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + std::to_string(i), &rnd, nullptr);
  }
  // override all the keys
  for (int i = 0; i < 10000; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, MultipleWriters) {
  Open(BlobDBOptions());

  std::vector<port::Thread> workers;
  std::vector<std::map<std::string, std::string>> data_set(10);
  for (uint32_t i = 0; i < 10; i++) {
    workers.emplace_back(
        [&](uint32_t id) {
          Random rnd(301 + id);
          for (int j = 0; j < 100; j++) {
            std::string key =
                "key" + std::to_string(id) + "_" + std::to_string(j);
            if (id < 5) {
              PutRandom(key, &rnd, &data_set[id]);
            } else {
              WriteBatch batch;
              PutRandomToWriteBatch(key, &rnd, &batch, &data_set[id]);
              ASSERT_OK(blob_db_->Write(WriteOptions(), &batch));
            }
          }
        },
        i);
  }
  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 10; i++) {
    workers[i].join();
    data.insert(data_set[i].begin(), data_set[i].end());
  }
  VerifyDB(data);
}

TEST_F(BlobDBTest, SstFileManager) {
  // run the same test for Get(), MultiGet() and Iterator each.
  std::shared_ptr<SstFileManager> sst_file_manager(
      NewSstFileManager(mock_env_.get()));
  sst_file_manager->SetDeleteRateBytesPerSecond(1024 * 1024);
  SstFileManagerImpl* sfm =
      static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  BlobDBOptions bdb_options;
  bdb_options.enable_garbage_collection = true;
  Options db_options;

  int files_scheduled_to_delete = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          ++files_scheduled_to_delete;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  db_options.sst_file_manager = sst_file_manager;

  Open(bdb_options, db_options);

  // Create 4 blob files. With GC cutoff of 0.25, the oldest file (file 1)
  // will be in the GC zone: floor(0.25 * 4) = 1.
  ASSERT_OK(blob_db_->Put(WriteOptions(), "foo", "bar"));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  std::shared_ptr<BlobFile> bfile = blob_files[0];
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(bfile));

  // Create 3 more blob files (files 2-4, outside GC zone).
  for (int i = 1; i < 4; i++) {
    ASSERT_OK(blob_db_->Put(WriteOptions(), "key" + std::to_string(i), "val"));
    blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(static_cast<size_t>(i + 1), blob_files.size());
    ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[i]));
  }

  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  blob_db_impl()->TEST_DeleteObsoleteFiles();

  // Even if SSTFileManager is not set, DB is creating a dummy one.
  ASSERT_EQ(1, files_scheduled_to_delete);
  Destroy();
  // Make sure that DestroyBlobDB() also goes through delete scheduler.
  // Remaining files: 3 original (files 2-4) + 1 GC output file = 4 files.
  ASSERT_EQ(5, files_scheduled_to_delete);
  SyncPoint::GetInstance()->DisableProcessing();
  sfm->WaitForEmptyTrash();
}

TEST_F(BlobDBTest, SstFileManagerRestart) {
  int files_scheduled_to_delete = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          ++files_scheduled_to_delete;
        }
      });

  // run the same test for Get(), MultiGet() and Iterator each.
  std::shared_ptr<SstFileManager> sst_file_manager(
      NewSstFileManager(mock_env_.get()));
  sst_file_manager->SetDeleteRateBytesPerSecond(1024 * 1024);
  SstFileManagerImpl* sfm =
      static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  BlobDBOptions bdb_options;
  Options db_options;

  SyncPoint::GetInstance()->EnableProcessing();
  db_options.sst_file_manager = sst_file_manager;

  Open(bdb_options, db_options);
  std::string blob_dir = blob_db_impl()->TEST_blob_dir();
  ASSERT_OK(blob_db_->Put(WriteOptions(), "foo", "bar"));
  Close();

  // Create 3 dummy trash files under the blob_dir
  const auto& fs = db_options.env->GetFileSystem();
  ASSERT_OK(CreateFile(fs, blob_dir + "/000666.blob.trash", "", false));
  ASSERT_OK(CreateFile(fs, blob_dir + "/000888.blob.trash", "", true));
  ASSERT_OK(CreateFile(fs, blob_dir + "/something_not_match.trash", "", false));

  // Make sure that reopening the DB rescan the existing trash files
  Open(bdb_options, db_options);
  ASSERT_EQ(files_scheduled_to_delete, 2);

  sfm->WaitForEmptyTrash();

  // There should be exact one file under the blob dir now.
  std::vector<std::string> all_files;
  ASSERT_OK(db_options.env->GetChildren(blob_dir, &all_files));
  int nfiles = 0;
  for (const auto& f : all_files) {
    assert(!f.empty());
    if (f[0] == '.') {
      continue;
    }
    nfiles++;
  }
  ASSERT_EQ(nfiles, 1);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(BlobDBTest, SnapshotAndGarbageCollection) {
  BlobDBOptions bdb_options;
  bdb_options.enable_garbage_collection = true;
  bdb_options.disable_background_tasks = true;

  Options options;
  options.disable_auto_compactions = true;

  // This test verifies that snapshots protect blob files from deletion during
  // garbage collection. With fixed GC cutoff of 0.25 and 8 immutable files,
  // floor(0.25 * 8) = 2 files are in the GC zone (files 1 and 2).
  //
  // We run 4 iterations with different snapshot timing:
  //   i=0: snapshot after key1 (before key2) - protects file 1
  //   i=1: snapshot after key2 (before key3) - protects files 1 and 2
  //   i=2: snapshot after key9 (after all keys) - no protection needed
  //   i=3: snapshot after Delete(key2) - no protection needed
  for (int i = 0; i < 4; i++) {
    Destroy();
    Open(bdb_options, options);

    const Snapshot* snapshot = nullptr;

    // Create first blob file (will be in GC zone).
    ASSERT_OK(Put("key1", "value"));
    if (i == 0) {
      snapshot = blob_db_->GetSnapshot();
    }

    auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(1, blob_files.size());
    ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));

    // Create second blob file (will be in GC zone). We track this file
    // to verify it becomes obsolete after GC relocates its blob.
    ASSERT_OK(Put("key2", "value"));
    if (i == 1) {
      snapshot = blob_db_->GetSnapshot();
    }

    blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(2, blob_files.size());
    auto gc_target_file = blob_files[1];
    ASSERT_FALSE(gc_target_file->Immutable());
    ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(gc_target_file));

    // Create files 3-8, all closed (these are outside GC zone).
    for (int j = 3; j <= 8; j++) {
      ASSERT_OK(Put("key" + std::to_string(j), "value"));
      blob_files = blob_db_impl()->TEST_GetBlobFiles();
      ASSERT_EQ(static_cast<size_t>(j), blob_files.size());
      ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[j - 1]));
    }

    // Create file 9 but leave it open (mutable). Only immutable files are
    // counted for GC cutoff calculation.
    ASSERT_OK(Put("key9", "value"));
    if (i == 2) {
      snapshot = blob_db_->GetSnapshot();
    }

    // Verify we have 9 total files (8 immutable + 1 mutable).
    blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(9, blob_files.size());

    // Trigger GC via compaction. Blobs in files 1 and 2 will be relocated
    // to a new GC output file.
    ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Verify gc_target_file (file 2) is now obsolete.
    ASSERT_TRUE(gc_target_file->Obsolete());
    // Verify the obsolete sequence matches the latest sequence number.
    ASSERT_EQ(blob_db_->GetLatestSequenceNumber(),
              gc_target_file->GetObsoleteSequence());

    Delete("key2");
    if (i == 3) {
      snapshot = blob_db_->GetSnapshot();
    }

    // Verify we now have 10 files (9 original + 1 GC output file).
    // Files 1 and 2 are obsolete but not yet deleted.
    ASSERT_EQ(10, blob_db_impl()->TEST_GetBlobFiles().size());
    blob_db_impl()->TEST_DeleteObsoleteFiles();

    if (i >= 2) {
      // Snapshot was taken after all keys were written, so it sees the
      // post-compaction state where blob indexes point to the GC output file.
      // Obsolete files 1 and 2 can be deleted immediately.
      // Verify 8 files remain (10 - 2 obsolete files deleted).
      ASSERT_EQ(8, blob_db_impl()->TEST_GetBlobFiles().size());
      blob_db_->ReleaseSnapshot(snapshot);
    } else {
      // Snapshot was taken before compaction completed, so it may still
      // reference blobs in the obsolete files. Files cannot be deleted.
      // Verify all 10 files still exist.
      ASSERT_EQ(10, blob_db_impl()->TEST_GetBlobFiles().size());
      blob_db_->ReleaseSnapshot(snapshot);
      blob_db_impl()->TEST_DeleteObsoleteFiles();
      // After releasing the snapshot, obsolete files can be deleted.
      // Verify 8 files remain.
      ASSERT_EQ(8, blob_db_impl()->TEST_GetBlobFiles().size());
    }
  }
}

TEST_F(BlobDBTest, ColumnFamilyNotSupported) {
  Options options;
  options.env = mock_env_.get();
  mock_clock_->SetCurrentTime(0);
  Open(BlobDBOptions(), options);
  ColumnFamilyHandle* default_handle = blob_db_->DefaultColumnFamily();
  ColumnFamilyHandle* handle = nullptr;
  std::string value;
  std::vector<std::string> values;
  // The call simply pass through to base db. It should succeed.
  ASSERT_OK(
      blob_db_->CreateColumnFamily(ColumnFamilyOptions(), "foo", &handle));
  ASSERT_TRUE(blob_db_->Put(WriteOptions(), handle, "k", "v").IsNotSupported());
  ASSERT_TRUE(blob_db_->PutWithTTL(WriteOptions(), handle, "k", "v", 60)
                  .IsNotSupported());
  WriteBatch batch;
  ASSERT_OK(batch.Put("k1", "v1"));
  ASSERT_OK(batch.Put(handle, "k2", "v2"));
  ASSERT_TRUE(blob_db_->Write(WriteOptions(), &batch).IsNotSupported());
  ASSERT_TRUE(blob_db_->Get(ReadOptions(), "k1", &value).IsNotFound());
  ASSERT_TRUE(
      blob_db_->Get(ReadOptions(), handle, "k", &value).IsNotSupported());
  auto statuses = blob_db_->MultiGet(ReadOptions(), {default_handle, handle},
                                     {"k1", "k2"}, &values);
  ASSERT_EQ(2, statuses.size());
  ASSERT_TRUE(statuses[0].IsNotSupported());
  ASSERT_TRUE(statuses[1].IsNotSupported());
  ASSERT_EQ(nullptr, blob_db_->NewIterator(ReadOptions(), handle));
  delete handle;
}

TEST_F(BlobDBTest, GetLiveFilesMetaData) {
  Random rnd(301);

  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 10;
  bdb_options.disable_background_tasks = true;

  Options options;
  options.env = mock_env_.get();

  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  for (size_t i = 0; i < 100; i++) {
    PutRandom("key" + std::to_string(i), &rnd, &data);
  }

  // At time 0, the stored expiration equals TTL
  constexpr uint64_t ttl = 1000ULL;
  PutRandomWithTTL("key100", ttl, &rnd, &data);

  std::vector<LiveFileMetaData> metadata;
  blob_db_->GetLiveFilesMetaData(&metadata);

  ASSERT_EQ(2U, metadata.size());
  // Path should be relative to db_name, but begin with slash.
  const std::string filename1("/blob_dir/000001.blob");
  ASSERT_EQ(filename1, metadata[0].name);
  ASSERT_EQ(1, metadata[0].file_number);
  ASSERT_EQ(0, metadata[0].oldest_ancestor_time);
  ASSERT_EQ(kDefaultColumnFamilyName, metadata[0].column_family_name);

  const std::string filename2("/blob_dir/000002.blob");
  ASSERT_EQ(filename2, metadata[1].name);
  ASSERT_EQ(2, metadata[1].file_number);
  ASSERT_EQ(ttl, metadata[1].oldest_ancestor_time);
  ASSERT_EQ(kDefaultColumnFamilyName, metadata[1].column_family_name);

  std::vector<std::string> livefile;
  uint64_t mfs;
  ASSERT_OK(blob_db_->GetLiveFiles(livefile, &mfs, false));
  ASSERT_EQ(5U, livefile.size());
  ASSERT_EQ(filename1, livefile[3]);
  ASSERT_EQ(filename2, livefile[4]);

  std::vector<LiveFileStorageInfo> all_files;
  std::vector<LiveFileStorageInfo> blob_files;
  ASSERT_OK(blob_db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(),
                                              &all_files));
  for (size_t i = 0; i < all_files.size(); i++) {
    if (all_files[i].file_type == kBlobFile) {
      blob_files.push_back(all_files[i]);
    }
  }

  ASSERT_EQ(2U, blob_files.size());
  ASSERT_GT(all_files.size(), blob_files.size());

  ASSERT_EQ("000001.blob", blob_files[0].relative_filename);
  ASSERT_EQ(blob_db_impl()->TEST_blob_dir(), blob_files[0].directory);
  ASSERT_GT(blob_files[0].size, 0);

  ASSERT_EQ("000002.blob", blob_files[1].relative_filename);
  ASSERT_EQ(blob_db_impl()->TEST_blob_dir(), blob_files[1].directory);
  ASSERT_GT(blob_files[1].size, 0);

  VerifyDB(data);
}

TEST_F(BlobDBTest, MigrateFromPlainRocksDB) {
  constexpr size_t kNumKey = 20;
  constexpr size_t kNumIteration = 10;
  Random rnd(301);
  std::map<std::string, std::string> data;
  std::vector<bool> is_blob(kNumKey, false);

  // Write to plain rocksdb.
  Options options;
  options.create_if_missing = true;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname_, &db));
  for (size_t i = 0; i < kNumIteration; i++) {
    auto key_index = rnd.Next() % kNumKey;
    std::string key = "key" + std::to_string(key_index);
    PutRandom(db.get(), key, &rnd, &data);
  }
  VerifyDB(db.get(), data);
  db.reset();

  // Open as blob db. Verify it can read existing data.
  Open();
  VerifyDB(blob_db_, data);
  for (size_t i = 0; i < kNumIteration; i++) {
    auto key_index = rnd.Next() % kNumKey;
    std::string key = "key" + std::to_string(key_index);
    is_blob[key_index] = true;
    PutRandom(blob_db_, key, &rnd, &data);
  }
  VerifyDB(blob_db_, data);
  delete blob_db_;
  blob_db_ = nullptr;

  // Verify plain db return error for keys written by blob db.
  ASSERT_OK(DB::Open(options, dbname_, &db));
  std::string value;
  for (size_t i = 0; i < kNumKey; i++) {
    std::string key = "key" + std::to_string(i);
    Status s = db->Get(ReadOptions(), key, &value);
    if (data.count(key) == 0) {
      ASSERT_TRUE(s.IsNotFound());
    } else if (is_blob[i]) {
      ASSERT_TRUE(s.IsCorruption());
    } else {
      ASSERT_OK(s);
      ASSERT_EQ(data[key], value);
    }
  }
}

// Test to verify that a NoSpace IOError Status is returned on reaching
// max_db_size limit.
TEST_F(BlobDBTest, OutOfSpace) {
  // Use mock env to stop wall clock.
  Options options;
  options.env = mock_env_.get();
  BlobDBOptions bdb_options;
  bdb_options.max_db_size = 200;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  // Each stored blob has an overhead of about 42 bytes currently.
  // So a small key + a 100 byte blob should take up ~150 bytes in the db.
  std::string value(100, 'v');
  ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), "key1", value, 60));

  // Putting another blob should fail as ading it would exceed the max_db_size
  // limit.
  Status s = blob_db_->PutWithTTL(WriteOptions(), "key2", value, 60);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_TRUE(s.IsNoSpace());
}

namespace {
class BlobDBMergeOrderFilter : public CompactionFilter {
 public:
  explicit BlobDBMergeOrderFilter(MergeOrder order) : order_(order) {}
  MergeOrder GetMergeOrder() const override { return order_; }
  const char* Name() const override { return "BlobDBMergeOrderFilter"; }
  Decision FilterV2(int /*level*/, const Slice& key, ValueType /*type*/,
                    const Slice& /*value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    calls.fetch_add(1);
    return key == "drop" ? Decision::kPurge : Decision::kKeep;
  }
  mutable std::atomic<uint64_t> calls{0};

 private:
  const MergeOrder order_;
};

class BlobDBMergeOrderFactory : public CompactionFilterFactory {
 public:
  explicit BlobDBMergeOrderFactory(CompactionFilter::MergeOrder order)
      : instance_order(order), order_(order) {}
  CompactionFilter::MergeOrder GetMergeOrder() const override { return order_; }
  const char* Name() const override { return "BlobDBMergeOrderFactory"; }
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    creations.fetch_add(1);
    return std::make_unique<BlobDBMergeOrderFilter>(instance_order);
  }
  CompactionFilter::MergeOrder instance_order;
  std::atomic<uint64_t> creations{0};

 private:
  const CompactionFilter::MergeOrder order_;
};
}  // namespace

TEST_F(BlobDBTest, MergeBeforeFilterRealStackedOpen) {
  using Order = CompactionFilter::MergeOrder;
  const Order legacy = Order::kFilterBeforeMerge;
  const Order post = Order::kMergeBeforeFilter;
  struct Case {
    Order direct_order;
    Order factory_order;
    bool direct;
    bool factory;
    bool not_supported;
  };
  const Case cases[] = {
      {legacy, legacy, false, false, false},
      {legacy, legacy, true, false, false},
      {legacy, legacy, false, true, false},
      {post, legacy, true, false, true},
      {legacy, post, false, true, true},
      {post, legacy, true, true, true},
      {legacy, post, true, true, false},
  };
  for (bool gc : {false, true}) {
    for (const Case& c : cases) {
      SCOPED_TRACE(gc);
      SCOPED_TRACE(testing::PrintToString(c.direct_order));
      SCOPED_TRACE(testing::PrintToString(c.factory_order));
      BlobDBMergeOrderFilter direct(c.direct_order);
      auto factory = std::make_shared<BlobDBMergeOrderFactory>(c.factory_order);
      BlobDBOptions bdb_options;
      bdb_options.disable_background_tasks = true;
      bdb_options.enable_garbage_collection = gc;
      Options options;
      options.disable_auto_compactions = true;
      options.compaction_filter = c.direct ? &direct : nullptr;
      options.compaction_filter_factory = c.factory ? factory : nullptr;
      Status s = TryOpen(bdb_options, options);
      if (c.not_supported) {
        ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
        ASSERT_EQ(nullptr, blob_db_);
        ASSERT_TRUE(Env::Default()->FileExists(dbname_).IsNotFound());
        ASSERT_EQ(0, direct.calls.load());
        ASSERT_EQ(0, factory->creations.load());
        continue;
      }
      ASSERT_OK(s);
      ASSERT_OK(Put("a", "older"));
      ASSERT_OK(Put("keep", "older"));
      ASSERT_OK(blob_db_->Flush(FlushOptions()));
      CompactRangeOptions move;
      move.change_level = true;
      move.target_level = 2;
      ASSERT_OK(blob_db_->CompactRange(move, nullptr, nullptr));
      ASSERT_OK(Put("drop", "value"));
      ASSERT_OK(Put("keep", "value"));
      ASSERT_OK(blob_db_->Flush(FlushOptions()));
      std::vector<LiveFileMetaData> files;
      blob_db_->GetRootDB()->GetLiveFilesMetaData(&files);
      std::vector<std::string> inputs;
      for (const auto& file : files) {
        if (file.level == 0) {
          inputs.push_back(file.relative_filename);
        }
      }
      ASSERT_FALSE(inputs.empty());
      ASSERT_OK(
          blob_db_->GetRootDB()->CompactFiles(CompactionOptions(), inputs, 1));
      if (c.direct || c.factory) {
        VerifyDB({{"a", "older"}, {"keep", "value"}});
        std::vector<KeyVersion> versions;
        ASSERT_OK(GetAllKeyVersions(blob_db_->GetRootDB(), "drop", "droq", 10,
                                    &versions));
        ASSERT_EQ(1, versions.size());
        ASSERT_EQ(kTypeSingleDeletion, versions.front().type);
      } else {
        VerifyDB({{"a", "older"}, {"drop", "value"}, {"keep", "value"}});
      }
      if (c.direct) {
        ASSERT_GT(direct.calls.load(), 0);
      }
      Destroy();
    }
  }
}

TEST_F(BlobDBTest, MergeBeforeFilterRealStackedRuntimeMismatch) {
  for (bool gc : {false, true}) {
    SCOPED_TRACE(gc);
    auto factory = std::make_shared<BlobDBMergeOrderFactory>(
        CompactionFilter::MergeOrder::kFilterBeforeMerge);
    factory->instance_order = CompactionFilter::MergeOrder::kMergeBeforeFilter;
    BlobDBOptions bdb_options;
    bdb_options.disable_background_tasks = true;
    bdb_options.enable_garbage_collection = gc;
    Options options;
    options.disable_auto_compactions = true;
    options.compaction_filter_factory = factory;
    Open(bdb_options, options);
    ASSERT_OK(Put("drop", "value"));
    ASSERT_OK(blob_db_->Flush(FlushOptions()));
    std::vector<LiveFileMetaData> before;
    blob_db_->GetRootDB()->GetLiveFilesMetaData(&before);
    ASSERT_TRUE(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                    .IsInvalidArgument());
    std::vector<LiveFileMetaData> after;
    blob_db_->GetRootDB()->GetLiveFilesMetaData(&after);
    ASSERT_EQ(before.size(), after.size());
    for (size_t i = 0; i < before.size(); ++i) {
      ASSERT_EQ(before[i].file_number, after[i].file_number);
    }
    VerifyDB({{"drop", "value"}});
    Destroy();
  }
}

TEST_F(BlobDBTest, UserCompactionFilter) {
  class CustomerFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice& /*key*/, const Slice& value,
                std::string* new_value, bool* value_changed) const override {
      *value_changed = false;
      // Test compaction filter modifying blob values
      if (value.size() % 4 == 1) {
        *new_value = value.ToString();
        // double size by duplicating value
        *new_value += *new_value;
        *value_changed = true;
        return false;
      } else if (value.size() % 3 == 1) {
        *new_value = value.ToString();
        // trancate value size by half
        *new_value = new_value->substr(0, new_value->size() / 2);
        *value_changed = true;
        return false;
      } else if (value.size() % 2 == 1) {
        return true;
      }
      return false;
    }
    bool IgnoreSnapshots() const override { return true; }
    const char* Name() const override { return "CustomerFilter"; }
  };
  class CustomerFilterFactory : public CompactionFilterFactory {
    const char* Name() const override { return "CustomerFilterFactory"; }
    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context& /*context*/) override {
      return std::unique_ptr<CompactionFilter>(new CustomerFilter());
    }
  };

  constexpr size_t kNumPuts = 1 << 10;
  constexpr uint64_t kMinValueSize = 1 << 6;
  constexpr uint64_t kMaxValueSize = 1 << 8;

  BlobDBOptions bdb_options;
  bdb_options.blob_file_size = kMaxValueSize * 10;
  bdb_options.disable_background_tasks = true;
  // case_num == 0: Test user defined compaction filter
  // case_num == 1: Test user defined compaction filter factory
  for (int case_num = 0; case_num < 2; case_num++) {
    Options options;
    if (case_num == 0) {
      options.compaction_filter = new CustomerFilter();
    } else {
      options.compaction_filter_factory.reset(new CustomerFilterFactory());
    }
    options.disable_auto_compactions = true;
    options.env = mock_env_.get();
    options.statistics = CreateDBStatistics();
    Open(bdb_options, options);

    std::map<std::string, std::string> data;
    std::map<std::string, std::string> data_after_compact;
    Random rnd(301);
    uint64_t value_size = kMinValueSize;
    int drop_record = 0;
    for (size_t i = 0; i < kNumPuts; ++i) {
      std::ostringstream oss;
      oss << "key" << std::setw(4) << std::setfill('0') << i;

      const std::string key(oss.str());
      const std::string value = rnd.HumanReadableString((int)value_size);
      const SequenceNumber sequence = blob_db_->GetLatestSequenceNumber() + 1;

      ASSERT_OK(Put(key, value));
      ASSERT_EQ(blob_db_->GetLatestSequenceNumber(), sequence);

      data[key] = value;
      if (value.length() % 4 == 1) {
        data_after_compact[key] = value + value;
      } else if (value.length() % 3 == 1) {
        data_after_compact[key] = value.substr(0, value.size() / 2);
      } else if (value.length() % 2 == 1) {
        ++drop_record;
      } else {
        data_after_compact[key] = value;
      }

      if (++value_size > kMaxValueSize) {
        value_size = kMinValueSize;
      }
    }
    // Verify full data set
    VerifyDB(data);
    // Applying compaction filter for records
    ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    // Verify data after compaction, only value with even length left.
    VerifyDB(data_after_compact);
    ASSERT_EQ(drop_record,
              options.statistics->getTickerCount(COMPACTION_KEY_DROP_USER));
    delete options.compaction_filter;
    Destroy();
  }
}

// Test user comapction filter when there is IO error on blob data.
TEST_F(BlobDBTest, UserCompactionFilter_BlobIOError) {
  class CustomerFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice& /*key*/, const Slice& value,
                std::string* new_value, bool* value_changed) const override {
      *new_value = value.ToString() + "_new";
      *value_changed = true;
      return false;
    }
    bool IgnoreSnapshots() const override { return true; }
    const char* Name() const override { return "CustomerFilter"; }
  };

  constexpr size_t kNumPuts = 100;
  constexpr int kValueSize = 100;

  BlobDBOptions bdb_options;
  bdb_options.blob_file_size = kValueSize * 10;
  bdb_options.disable_background_tasks = true;

  std::vector<std::string> io_failure_cases = {
      "BlobDBImpl::CreateBlobFileAndWriter",
      "BlobIndexCompactionFilterBase::WriteBlobToNewFile",
      "BlobDBImpl::CloseBlobFile"};

  for (size_t case_num = 0; case_num < io_failure_cases.size(); case_num++) {
    Options options;
    options.compaction_filter = new CustomerFilter();
    options.disable_auto_compactions = true;
    options.env = fault_injection_env_.get();
    options.statistics = CreateDBStatistics();
    Open(bdb_options, options);

    std::map<std::string, std::string> data;
    Random rnd(301);
    for (size_t i = 0; i < kNumPuts; ++i) {
      std::ostringstream oss;
      oss << "key" << std::setw(4) << std::setfill('0') << i;

      const std::string key(oss.str());
      const std::string value = rnd.HumanReadableString(kValueSize);
      const SequenceNumber sequence = blob_db_->GetLatestSequenceNumber() + 1;

      ASSERT_OK(Put(key, value));
      ASSERT_EQ(blob_db_->GetLatestSequenceNumber(), sequence);
      data[key] = value;
    }

    // Verify full data set
    VerifyDB(data);

    SyncPoint::GetInstance()->SetCallBack(
        io_failure_cases[case_num], [&](void* /*arg*/) {
          fault_injection_env_->SetFilesystemActive(false, Status::IOError());
        });
    SyncPoint::GetInstance()->EnableProcessing();
    auto s = blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    ASSERT_TRUE(s.IsIOError());

    // Reactivate file system to allow test to verify and close DB.
    fault_injection_env_->SetFilesystemActive(true);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    // Verify full data set after compaction failure
    VerifyDB(data);

    delete options.compaction_filter;
    Destroy();
  }
}

// Test comapction filter should remove any expired blob index.
TEST_F(BlobDBTest, FilterExpiredBlobIndex) {
  constexpr size_t kNumKeys = 100;
  constexpr size_t kNumPuts = 1000;
  constexpr uint64_t kMaxTTL = 1000;
  constexpr uint64_t kCompactTime = 500;
  Random rnd(301);
  mock_clock_->SetCurrentTime(0);
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  std::map<std::string, std::string> data_after_compact;
  for (size_t i = 0; i < kNumPuts; i++) {
    bool has_ttl = rnd.Next() % 2;
    // At time 0, stored expiration equals TTL
    uint64_t ttl = rnd.Next() % kMaxTTL;
    int len = rnd.Next() % 200 + 10;
    std::string key = "key" + std::to_string(rnd.Next() % kNumKeys);
    std::string value = rnd.HumanReadableString(len);
    if (!has_ttl) {
      ASSERT_OK(Put(key, value));
      data_after_compact[key] = value;
    } else {
      ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), key, value, ttl));
      if (ttl <= kCompactTime) {
        data_after_compact.erase(key);
      } else {
        data_after_compact[key] = value;
      }
    }
    data[key] = value;
  }
  VerifyDB(data);

  mock_clock_->SetCurrentTime(kCompactTime);
  // Take a snapshot before compaction. Make sure expired blob indexes is
  // filtered regardless of snapshot.
  const Snapshot* snapshot = blob_db_->GetSnapshot();
  // Issue manual compaction to trigger compaction filter.
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  blob_db_->ReleaseSnapshot(snapshot);
  // Verify expired blob index are filtered.
  std::vector<KeyVersion> versions;
  const size_t kMaxKeys = 10000;
  ASSERT_OK(GetAllKeyVersions(blob_db_, {}, {}, kMaxKeys, &versions));
  ASSERT_EQ(data_after_compact.size(), versions.size());
  for (auto& version : versions) {
    ASSERT_TRUE(data_after_compact.count(version.user_key) > 0);
  }
  VerifyDB(data_after_compact);
}

// Test compaction filter should remove any blob index where corresponding
// blob file has been removed.
TEST_F(BlobDBTest, FilterFileNotAvailable) {
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.disable_auto_compactions = true;
  Open(bdb_options, options);

  ASSERT_OK(Put("foo", "v1"));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  ASSERT_EQ(1, blob_files[0]->BlobFileNumber());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));

  ASSERT_OK(Put("bar", "v2"));
  blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(2, blob_files.size());
  ASSERT_EQ(2, blob_files[1]->BlobFileNumber());
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[1]));

  const size_t kMaxKeys = 10000;

  DB* base_db = blob_db_->GetRootDB();
  std::vector<KeyVersion> versions;
  ASSERT_OK(GetAllKeyVersions(base_db, {}, {}, kMaxKeys, &versions));
  ASSERT_EQ(2, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  ASSERT_EQ("foo", versions[1].user_key);
  VerifyDB({{"bar", "v2"}, {"foo", "v1"}});

  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, {}, {}, kMaxKeys, &versions));
  ASSERT_EQ(2, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  ASSERT_EQ("foo", versions[1].user_key);
  VerifyDB({{"bar", "v2"}, {"foo", "v1"}});

  // Remove the first blob file and compact. foo should be remove from base db.
  blob_db_impl()->TEST_ObsoleteBlobFile(blob_files[0]);
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, {}, {}, kMaxKeys, &versions));
  ASSERT_EQ(1, versions.size());
  ASSERT_EQ("bar", versions[0].user_key);
  VerifyDB({{"bar", "v2"}});

  // Remove the second blob file and compact. bar should be remove from base db.
  blob_db_impl()->TEST_ObsoleteBlobFile(blob_files[1]);
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(GetAllKeyVersions(base_db, {}, {}, kMaxKeys, &versions));
  ASSERT_EQ(0, versions.size());
  VerifyDB({});
}

TEST_F(BlobDBTest, GarbageCollection) {
  constexpr size_t kNumPuts = 1 << 10;

  // At time 0, stored expiration equals TTL
  constexpr uint64_t kTTL = 1000;
  constexpr uint64_t kCompactTime = 500;

  constexpr uint64_t kKeySize = 7;  // "key" + 4 digits
  constexpr uint64_t kValueSize = 1 << 8;

  constexpr size_t kBlobsPerFile = 8;
  constexpr size_t kNumBlobFiles = kNumPuts / kBlobsPerFile;
  constexpr uint64_t kBlobFileSize =
      BlobLogHeader::kSize +
      (BlobLogRecord::kHeaderSize + kKeySize + kValueSize) * kBlobsPerFile;

  BlobDBOptions bdb_options;
  bdb_options.blob_file_size = kBlobFileSize;
  bdb_options.enable_garbage_collection = true;
  bdb_options.disable_background_tasks = true;

  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();

  Open(bdb_options, options);

  std::map<std::string, std::string> data;
  std::map<std::string, KeyVersion> blob_value_versions;
  std::map<std::string, BlobIndexVersion> blob_index_versions;

  Random rnd(301);

  // Add a bunch of non-TTL values. These will be written to non-TTL
  // blob files and will be subject to GC.
  for (size_t i = 0; i < kNumPuts; ++i) {
    std::ostringstream oss;
    oss << "key" << std::setw(4) << std::setfill('0') << i;

    const std::string key(oss.str());
    const std::string value = rnd.HumanReadableString(kValueSize);
    const SequenceNumber sequence = blob_db_->GetLatestSequenceNumber() + 1;

    ASSERT_OK(Put(key, value));
    ASSERT_EQ(blob_db_->GetLatestSequenceNumber(), sequence);

    data[key] = value;
    blob_value_versions[key] = KeyVersion(key, value, sequence, kTypeBlobIndex);
    blob_index_versions[key] =
        BlobIndexVersion(key, /* file_number */ (i >> 3) + 1, kNoExpiration,
                         sequence, kTypeBlobIndex);
  }

  // Add a TTL value that will be written to its own TTL blob file (ignored
  // during GC).
  {
    const std::string key("key2000");
    const std::string value = rnd.HumanReadableString(kValueSize);
    const SequenceNumber sequence = blob_db_->GetLatestSequenceNumber() + 1;

    ASSERT_OK(blob_db_->PutWithTTL(WriteOptions(), key, value, kTTL));
    ASSERT_EQ(blob_db_->GetLatestSequenceNumber(), sequence);

    data[key] = value;
    blob_value_versions[key] = KeyVersion(key, value, sequence, kTypeBlobIndex);
    blob_index_versions[key] =
        BlobIndexVersion(key, /* file_number */ kNumBlobFiles + 1, kTTL,
                         sequence, kTypeBlobIndex);
  }

  VerifyDB(data);
  VerifyBaseDB(blob_value_versions);
  VerifyBaseDBBlobIndex(blob_index_versions);

  // At this point, we should have 128 immutable non-TTL files with file numbers
  // 1..128.
  {
    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), kNumBlobFiles);
    for (size_t i = 0; i < kNumBlobFiles; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 1);
      ASSERT_EQ(live_imm_files[i]->GetFileSize(),
                kBlobFileSize + BlobLogFooter::kSize);
    }
  }

  mock_clock_->SetCurrentTime(kCompactTime);

  ASSERT_OK(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // We expect the data to remain the same and the blobs from the oldest N files
  // to be moved to new files. Sequence numbers get zeroed out during the
  // compaction.
  VerifyDB(data);

  for (auto& pair : blob_value_versions) {
    KeyVersion& version = pair.second;
    version.sequence = 0;
  }

  VerifyBaseDB(blob_value_versions);

  // GC cutoff is fixed at 0.25
  constexpr double kGCCutoff = 0.25;
  const uint64_t cutoff = static_cast<uint64_t>(kGCCutoff * kNumBlobFiles);
  for (auto& pair : blob_index_versions) {
    BlobIndexVersion& version = pair.second;

    version.sequence = 0;

    if (version.file_number == kInvalidBlobFileNumber) {
      continue;
    }

    if (version.file_number > cutoff) {
      continue;
    }

    version.file_number += kNumBlobFiles + 1;
  }

  VerifyBaseDBBlobIndex(blob_index_versions);

  const Statistics* const statistics = options.statistics.get();
  assert(statistics);

  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_NUM_FILES), cutoff);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_NUM_NEW_FILES), cutoff);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_FAILURES), 0);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_NUM_KEYS_RELOCATED),
            cutoff * kBlobsPerFile);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_BYTES_RELOCATED),
            cutoff * kBlobsPerFile * kValueSize);

  // At this point, we should have 128 immutable non-TTL files with file numbers
  // 33..128 and 130..161. (129 was taken by the TTL blob file.)
  {
    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), kNumBlobFiles);
    for (size_t i = 0; i < kNumBlobFiles; ++i) {
      uint64_t expected_file_number = i + cutoff + 1;
      if (expected_file_number > kNumBlobFiles) {
        ++expected_file_number;
      }

      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), expected_file_number);
      ASSERT_EQ(live_imm_files[i]->GetFileSize(),
                kBlobFileSize + BlobLogFooter::kSize);
    }
  }
}

TEST_F(BlobDBTest, GarbageCollectionFailure) {
  BlobDBOptions bdb_options;
  bdb_options.enable_garbage_collection = true;
  bdb_options.disable_background_tasks = true;

  Options db_options;
  db_options.statistics = CreateDBStatistics();

  Open(bdb_options, db_options);

  // Create 4 blob files. With fixed GC cutoff of 0.25, the oldest file
  // (floor(0.25 * 4) = 1) will be in the GC zone.
  // The first file contains valid blobs for "foo" and "dead".
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("dead", "beef"));

  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);
  auto first_file = blob_files[0];
  uint64_t first_file_number = first_file->BlobFileNumber();
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(first_file));

  // Create 3 more blob files (files 2-4, outside GC zone).
  for (int i = 1; i < 4; i++) {
    ASSERT_OK(Put("key" + std::to_string(i), "value"));
    blob_files = blob_db_impl()->TEST_GetBlobFiles();
    ASSERT_EQ(static_cast<size_t>(i + 1), blob_files.size());
    ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[i]));
  }

  // Write a fake blob index that points to the first file (in GC zone)
  // but with an invalid offset beyond the file size. This will cause
  // GC to fail when it tries to read this blob.
  std::string blob_index;
  BlobIndex::EncodeBlob(&blob_index, first_file_number, /* offset */ 999999,
                        /* size */ 5678, kNoCompression);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(
      &batch, blob_db_->DefaultColumnFamily()->GetID(), "key", blob_index));
  ASSERT_OK(blob_db_->GetRootDB()->Write(WriteOptions(), &batch));

  // Verify compaction fails with IO error due to invalid blob offset.
  ASSERT_TRUE(blob_db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsIOError());

  const Statistics* const statistics = db_options.statistics.get();
  assert(statistics);

  // Verify GC statistics:
  // - Relocated 2 keys ("foo" and "dead") with 7 bytes ("bar" + "beef")
  // - Failed on "key" which has invalid blob offset
  // - Created 1 new GC output file before failing
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_NUM_FILES), 0);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_NUM_NEW_FILES), 1);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_FAILURES), 1);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_NUM_KEYS_RELOCATED), 2);
  ASSERT_EQ(statistics->getTickerCount(BLOB_DB_GC_BYTES_RELOCATED), 7);
}

// File should be evicted after expiration.
TEST_F(BlobDBTest, EvictExpiredFile) {
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 100;
  bdb_options.disable_background_tasks = true;
  Options options;
  options.env = mock_env_.get();
  Open(bdb_options, options);
  mock_clock_->SetCurrentTime(50);
  std::map<std::string, std::string> data;
  ASSERT_OK(PutWithTTL("foo", "bar", 100, &data));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files[0];
  ASSERT_FALSE(blob_file->Immutable());
  ASSERT_FALSE(blob_file->Obsolete());
  VerifyDB(data);
  mock_clock_->SetCurrentTime(250);
  // The key should expired now.
  blob_db_impl()->TEST_EvictExpiredFiles();
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
  ASSERT_TRUE(blob_file->Immutable());
  ASSERT_TRUE(blob_file->Obsolete());
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(0, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(0, blob_db_impl()->TEST_GetObsoleteFiles().size());
  // Make sure we don't return garbage value after blob file being evicted,
  // but the blob index still exists in the LSM tree.
  std::string val;
  ASSERT_TRUE(blob_db_->Get(ReadOptions(), "foo", &val).IsNotFound());
  ASSERT_EQ("", val);
}

TEST_F(BlobDBTest, DisableFileDeletions) {
  BlobDBOptions bdb_options;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);
  std::map<std::string, std::string> data;
  ASSERT_OK(Put("foo", "v", &data));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files[0];
  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_file));
  blob_db_impl()->TEST_ObsoleteBlobFile(blob_file);
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
  // Call DisableFileDeletions twice.
  ASSERT_OK(blob_db_->DisableFileDeletions());
  ASSERT_OK(blob_db_->DisableFileDeletions());
  // File deletions should be disabled.
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
  VerifyDB(data);
  // Enable file deletions once. File deletion will later get enabled when
  // `EnableFileDeletions` called for a second time.
  ASSERT_OK(blob_db_->EnableFileDeletions());
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(1, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(1, blob_db_impl()->TEST_GetObsoleteFiles().size());
  VerifyDB(data);
  // Call EnableFileDeletions a second time.
  ASSERT_OK(blob_db_->EnableFileDeletions());
  blob_db_impl()->TEST_DeleteObsoleteFiles();
  // File should be deleted by now.
  ASSERT_EQ(0, blob_db_impl()->TEST_GetBlobFiles().size());
  ASSERT_EQ(0, blob_db_impl()->TEST_GetObsoleteFiles().size());
  VerifyDB({});
}

TEST_F(BlobDBTest, MaintainBlobFileToSstMapping) {
  BlobDBOptions bdb_options;
  bdb_options.enable_garbage_collection = true;
  bdb_options.disable_background_tasks = true;
  Open(bdb_options);

  // Register some dummy blob files.
  blob_db_impl()->TEST_AddDummyBlobFile(1, /* immutable_sequence */ 200);
  blob_db_impl()->TEST_AddDummyBlobFile(2, /* immutable_sequence */ 300);
  blob_db_impl()->TEST_AddDummyBlobFile(3, /* immutable_sequence */ 400);
  blob_db_impl()->TEST_AddDummyBlobFile(4, /* immutable_sequence */ 500);
  blob_db_impl()->TEST_AddDummyBlobFile(5, /* immutable_sequence */ 600);

  // Initialize the blob <-> SST file mapping. First, add some SST files with
  // blob file references, then some without.
  std::vector<LiveFileMetaData> live_files;

  for (uint64_t i = 1; i <= 10; ++i) {
    LiveFileMetaData live_file;
    live_file.file_number = i;
    live_file.oldest_blob_file_number = ((i - 1) % 5) + 1;

    live_files.emplace_back(live_file);
  }

  for (uint64_t i = 11; i <= 20; ++i) {
    LiveFileMetaData live_file;
    live_file.file_number = i;

    live_files.emplace_back(live_file);
  }

  blob_db_impl()->TEST_InitializeBlobFileToSstMapping(live_files);

  // Check that the blob <-> SST mappings have been correctly initialized.
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();

  ASSERT_EQ(blob_files.size(), 5);

  {
    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 1);
    }

    ASSERT_TRUE(blob_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  {
    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{false, false, false, false,
                                              false};
    for (size_t i = 0; i < 5; ++i) {
      const auto& blob_file = blob_files[i];
      ASSERT_EQ(blob_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(blob_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 1);
    }

    ASSERT_TRUE(blob_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  // Simulate a flush where the SST does not reference any blob files.
  {
    FlushJobInfo info{};
    info.file_number = 21;
    info.smallest_seqno = 1;
    info.largest_seqno = 100;

    blob_db_impl()->TEST_ProcessFlushJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{false, false, false, false,
                                              false};
    for (size_t i = 0; i < 5; ++i) {
      const auto& blob_file = blob_files[i];
      ASSERT_EQ(blob_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(blob_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 1);
    }

    ASSERT_TRUE(blob_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  // Simulate a flush where the SST references a blob file.
  {
    FlushJobInfo info{};
    info.file_number = 22;
    info.oldest_blob_file_number = 5;
    info.smallest_seqno = 101;
    info.largest_seqno = 200;

    blob_db_impl()->TEST_ProcessFlushJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {1, 6}, {2, 7}, {3, 8}, {4, 9}, {5, 10, 22}};
    const std::vector<bool> expected_obsolete{false, false, false, false,
                                              false};
    for (size_t i = 0; i < 5; ++i) {
      const auto& blob_file = blob_files[i];
      ASSERT_EQ(blob_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(blob_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 5);
    for (size_t i = 0; i < 5; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 1);
    }

    ASSERT_TRUE(blob_db_impl()->TEST_GetObsoleteFiles().empty());
  }

  // Simulate a compaction. Some inputs and outputs have blob file references,
  // some don't. There is also a trivial move (which means the SST appears on
  // both the input and the output list). Blob file 1 loses all its linked SSTs,
  // and since it got marked immutable at sequence number 200 which has already
  // been flushed, it can be marked obsolete.
  {
    CompactionJobInfo info{};
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 1, 1});
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 2, 2});
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 6, 1});
    info.input_file_infos.emplace_back(
        CompactionFileInfo{1, 11, kInvalidBlobFileNumber});
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 23, 3});
    info.output_file_infos.emplace_back(
        CompactionFileInfo{2, 24, kInvalidBlobFileNumber});

    blob_db_impl()->TEST_ProcessCompactionJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {7}, {3, 8, 23}, {4, 9}, {5, 10, 22}};
    const std::vector<bool> expected_obsolete{true, false, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto& blob_file = blob_files[i];
      ASSERT_EQ(blob_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(blob_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 2);
    }

    auto obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 1);
    ASSERT_EQ(obsolete_files[0]->BlobFileNumber(), 1);
  }

  // Simulate a failed compaction. No mappings should be updated.
  {
    CompactionJobInfo info{};
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 7, 2});
    info.input_file_infos.emplace_back(CompactionFileInfo{2, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 25, 3});
    info.status = Status::Corruption();

    blob_db_impl()->TEST_ProcessCompactionJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {7}, {3, 8, 23}, {4, 9}, {5, 10, 22}};
    const std::vector<bool> expected_obsolete{true, false, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto& blob_file = blob_files[i];
      ASSERT_EQ(blob_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(blob_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 2);
    }

    auto obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 1);
    ASSERT_EQ(obsolete_files[0]->BlobFileNumber(), 1);
  }

  // Simulate another compaction. Blob file 2 loses all its linked SSTs
  // but since it got marked immutable at sequence number 300 which hasn't
  // been flushed yet, it cannot be marked obsolete at this point.
  {
    CompactionJobInfo info{};
    info.input_file_infos.emplace_back(CompactionFileInfo{1, 7, 2});
    info.input_file_infos.emplace_back(CompactionFileInfo{2, 22, 5});
    info.output_file_infos.emplace_back(CompactionFileInfo{2, 25, 3});

    blob_db_impl()->TEST_ProcessCompactionJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {}, {3, 8, 23, 25}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{true, false, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto& blob_file = blob_files[i];
      ASSERT_EQ(blob_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(blob_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 4);
    for (size_t i = 0; i < 4; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 2);
    }

    auto obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 1);
    ASSERT_EQ(obsolete_files[0]->BlobFileNumber(), 1);
  }

  // Simulate a flush with largest sequence number 300. This will make it
  // possible to mark blob file 2 obsolete.
  {
    FlushJobInfo info{};
    info.file_number = 26;
    info.smallest_seqno = 201;
    info.largest_seqno = 300;

    blob_db_impl()->TEST_ProcessFlushJobInfo(info);

    const std::vector<std::unordered_set<uint64_t>> expected_sst_files{
        {}, {}, {3, 8, 23, 25}, {4, 9}, {5, 10}};
    const std::vector<bool> expected_obsolete{true, true, false, false, false};
    for (size_t i = 0; i < 5; ++i) {
      const auto& blob_file = blob_files[i];
      ASSERT_EQ(blob_file->GetLinkedSstFiles(), expected_sst_files[i]);
      ASSERT_EQ(blob_file->Obsolete(), expected_obsolete[i]);
    }

    auto live_imm_files = blob_db_impl()->TEST_GetLiveImmNonTTLFiles();
    ASSERT_EQ(live_imm_files.size(), 3);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_EQ(live_imm_files[i]->BlobFileNumber(), i + 3);
    }

    auto obsolete_files = blob_db_impl()->TEST_GetObsoleteFiles();
    ASSERT_EQ(obsolete_files.size(), 2);
    ASSERT_EQ(obsolete_files[0]->BlobFileNumber(), 1);
    ASSERT_EQ(obsolete_files[1]->BlobFileNumber(), 2);
  }
}

TEST_F(BlobDBTest, ShutdownWait) {
  BlobDBOptions bdb_options;
  bdb_options.ttl_range_secs = 100;
  bdb_options.disable_background_tasks = false;
  Options options;
  options.env = mock_env_.get();

  SyncPoint::GetInstance()->LoadDependency({
      {"BlobDBImpl::EvictExpiredFiles:0", "BlobDBTest.ShutdownWait:0"},
      {"BlobDBTest.ShutdownWait:1", "BlobDBImpl::EvictExpiredFiles:1"},
      {"BlobDBImpl::EvictExpiredFiles:2", "BlobDBTest.ShutdownWait:2"},
      {"BlobDBTest.ShutdownWait:3", "BlobDBImpl::EvictExpiredFiles:3"},
  });
  // Force all tasks to be scheduled immediately.
  SyncPoint::GetInstance()->SetCallBack(
      "TimeQueue::Add:item.end", [&](void* arg) {
        std::chrono::steady_clock::time_point* tp =
            static_cast<std::chrono::steady_clock::time_point*>(arg);
        *tp =
            std::chrono::steady_clock::now() - std::chrono::milliseconds(10000);
      });

  SyncPoint::GetInstance()->SetCallBack(
      "BlobDBImpl::EvictExpiredFiles:cb", [&](void* /*arg*/) {
        // Sleep 3 ms to increase the chance of data race.
        // We've synced up the code so that EvictExpiredFiles()
        // is called concurrently with ~BlobDBImpl().
        // ~BlobDBImpl() is supposed to wait for all background
        // task to shutdown before doing anything else. In order
        // to use the same test to reproduce a bug of the waiting
        // logic, we wait a little bit here, so that TSAN can
        // catch the data race.
        // We should improve the test if we find a better way.
        Env::Default()->SleepForMicroseconds(3000);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  Open(bdb_options, options);
  mock_clock_->SetCurrentTime(50);
  std::map<std::string, std::string> data;
  ASSERT_OK(PutWithTTL("foo", "bar", 100, &data));
  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(1, blob_files.size());
  auto blob_file = blob_files[0];
  ASSERT_FALSE(blob_file->Immutable());
  ASSERT_FALSE(blob_file->Obsolete());
  VerifyDB(data);

  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:0");
  mock_clock_->SetCurrentTime(250);
  // The key should expired now.
  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:1");

  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:2");
  TEST_SYNC_POINT("BlobDBTest.ShutdownWait:3");
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(BlobDBTest, SyncBlobFileBeforeClose) {
  Options options;
  options.statistics = CreateDBStatistics();

  BlobDBOptions blob_options;
  blob_options.disable_background_tasks = true;

  Open(blob_options, options);

  ASSERT_OK(Put("foo", "bar"));

  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  ASSERT_OK(blob_db_impl()->TEST_CloseBlobFile(blob_files[0]));
  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_SYNCED), 1);
}

TEST_F(BlobDBTest, SyncBlobFileBeforeCloseIOError) {
  Options options;
  options.env = fault_injection_env_.get();

  BlobDBOptions blob_options;
  blob_options.disable_background_tasks = true;

  Open(blob_options, options);

  ASSERT_OK(Put("foo", "bar"));

  auto blob_files = blob_db_impl()->TEST_GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  SyncPoint::GetInstance()->SetCallBack(
      "BlobLogWriter::Sync", [this](void* /* arg */) {
        fault_injection_env_->SetFilesystemActive(false, Status::IOError());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status s = blob_db_impl()->TEST_CloseBlobFile(blob_files[0]);

  fault_injection_env_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(s.IsIOError());
}

}  // namespace ROCKSDB_NAMESPACE::blob_db

// A black-box test for the ttl wrapper around rocksdb
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
