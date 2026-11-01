//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Syncpoint prevents us building and running tests in release
#include "rocksdb/utilities/checkpoint.h"

#ifndef OS_WIN
#include <unistd.h>
#endif
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <thread>
#include <utility>

#include "db/db_impl/db_impl.h"
#include "db/log_writer.h"
#include "db/manifest_ops.h"
#include "db/version_edit.h"
#include "file/file_util.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/metadata.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {
class CheckpointTest : public testing::Test {
 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault = 0,
  };
  int option_config_;

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  Env* env_;
  std::unique_ptr<DB> db_;
  Options last_options_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::string snapshot_name_;
  std::string export_path_;
  ColumnFamilyHandle* cfh_reverse_comp_;
  ExportImportFilesMetaData* metadata_;

  CheckpointTest() : env_(Env::Default()) {
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::PerThreadDBPath(env_, "checkpoint_test");
    alternative_wal_dir_ = dbname_ + "/wal";
    auto options = CurrentOptions();
    auto delete_options = options;
    delete_options.wal_dir = alternative_wal_dir_;
    EXPECT_OK(DestroyDB(dbname_, delete_options));
    // Destroy it for not alternative WAL dir is used.
    EXPECT_OK(DestroyDB(dbname_, options));
    db_.reset();
    snapshot_name_ = test::PerThreadDBPath(env_, "snapshot");
    std::string snapshot_tmp_name = snapshot_name_ + ".tmp";
    EXPECT_OK(DestroyDB(snapshot_name_, options));
    test::DeleteDir(env_, snapshot_name_);
    EXPECT_OK(DestroyDB(snapshot_tmp_name, options));
    test::DeleteDir(env_, snapshot_tmp_name);
    Reopen(options);
    export_path_ = test::PerThreadDBPath("/export");
    DestroyDir(env_, export_path_).PermitUncheckedError();
    cfh_reverse_comp_ = nullptr;
    metadata_ = nullptr;
  }

  ~CheckpointTest() override {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({});
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    if (cfh_reverse_comp_) {
      EXPECT_OK(db_->DestroyColumnFamilyHandle(cfh_reverse_comp_));
      cfh_reverse_comp_ = nullptr;
    }
    if (metadata_) {
      delete metadata_;
      metadata_ = nullptr;
    }
    Close();
    Options options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    EXPECT_OK(DestroyDB(dbname_, options));
    EXPECT_OK(DestroyDB(snapshot_name_, options));
    DestroyDir(env_, export_path_).PermitUncheckedError();
  }

  DBImpl* dbfull() { return static_cast_with_check<DBImpl>(db_.get()); }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    return options;
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const Options& options) {
    ColumnFamilyOptions cf_opts(options);
    size_t cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (const auto& cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
    }
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options& options) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<Options>& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const std::vector<Options>& options) {
    Close();
    EXPECT_EQ(cfs.size(), options.size());
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.emplace_back(cfs[i], options[i]);
    }
    DBOptions db_opts = DBOptions(options[0]);
    return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options& options) {
    Close();
    std::vector<Options> v_opts(cfs.size(), options);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  void Reopen(const Options& options) { ASSERT_OK(TryReopen(options)); }

  void CompactAll() {
    for (auto h : handles_) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), h, nullptr, nullptr));
    }
  }

  void Close() {
    for (auto h : handles_) {
      delete h;
    }
    handles_.clear();
    db_.reset();
  }

  void DestroyAndReopen(const Options& options) {
    // Destroy using last options
    Destroy(last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(const Options& options) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, options));
  }

  Status ReadOnlyReopen(const Options& options) {
    return DB::OpenForReadOnly(options, dbname_, &db_);
  }

  Status ReadOnlyReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const Options& options) {
    std::vector<ColumnFamilyDescriptor> column_families;
    for (const auto& cf : cfs) {
      column_families.emplace_back(cf, options);
    }
    return DB::OpenForReadOnly(options, dbname_, column_families, &handles_,
                               &db_);
  }

  Status TryReopen(const Options& options) {
    Close();
    last_options_ = options;
    return DB::Open(options, dbname_, &db_);
  }

  Status Flush(int cf = 0) {
    if (cf == 0) {
      return db_->Flush(FlushOptions());
    } else {
      return db_->Flush(FlushOptions(), handles_[cf]);
    }
  }

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, k, v);
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, handles_[cf], k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  Status Delete(int cf, const std::string& k) {
    return db_->Delete(WriteOptions(), handles_[cf], k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, handles_[cf], k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  int NumTableFilesAtLevel(int level) {
    std::string property;
    EXPECT_TRUE(db_->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(level), &property));
    return atoi(property.c_str());
  }
};

TEST_F(CheckpointTest, GetSnapshotLink) {
  for (uint64_t log_size_for_flush : {0, 1000000}) {
    Options options;
    ReadOptions roptions;
    std::string result;
    Checkpoint* checkpoint;

    options = CurrentOptions();
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options));

    // Create a database
    options.create_if_missing = true;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    std::string key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));
    // Take a snapshot
    ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, log_size_for_flush));
    ASSERT_OK(Put(key, "v2"));
    ASSERT_EQ("v2", Get(key));
    ASSERT_OK(Flush());
    ASSERT_EQ("v2", Get(key));
    // Open snapshot and verify contents while DB is running
    options.create_if_missing = false;
    std::unique_ptr<DB> snapshotDB;
    ASSERT_OK(DB::Open(options, snapshot_name_, &snapshotDB));
    ASSERT_OK(snapshotDB->Get(roptions, key, &result));
    ASSERT_EQ("v1", result);
    snapshotDB.reset();
    db_.reset();

    // Destroy original DB
    ASSERT_OK(DestroyDB(dbname_, options));

    // Open snapshot and verify contents
    options.create_if_missing = false;
    dbname_ = snapshot_name_;
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("v1", Get(key));
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options));
    delete checkpoint;

    // Restore DB name
    dbname_ = test::PerThreadDBPath(env_, "db_test");
  }
}

TEST_F(CheckpointTest, CheckpointWithBlob) {
  // Create a database with a blob file
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  ASSERT_OK(Put(key, blob));
  ASSERT_OK(Flush());

  // Create a checkpoint
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));

  std::unique_ptr<Checkpoint> checkpoint_guard(checkpoint);

  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));

  // Make sure it contains the blob file
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(snapshot_name_, &files));

  bool blob_file_found = false;
  for (const auto& file : files) {
    uint64_t number = 0;
    FileType type = kWalFile;

    if (ParseFileName(file, &number, &type) && type == kBlobFile) {
      blob_file_found = true;
      break;
    }
  }

  ASSERT_TRUE(blob_file_found);

  // Make sure the checkpoint can be opened and the blob value read
  options.create_if_missing = false;
  std::unique_ptr<DB> checkpoint_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &checkpoint_db));

  PinnableSlice value;
  ASSERT_OK(checkpoint_db->Get(
      ReadOptions(), checkpoint_db->DefaultColumnFamily(), key, &value));

  ASSERT_EQ(value, blob);
}

TEST_F(CheckpointTest, ExportColumnFamilyWithLinks) {
  // Create a database
  auto options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({}, options);

  // Helper to verify the number of files in metadata and export dir
  auto verify_files_exported = [&](const ExportImportFilesMetaData& metadata,
                                   int num_files_expected) {
    ASSERT_EQ(metadata.files.size(), num_files_expected);
    std::vector<std::string> subchildren;
    ASSERT_OK(env_->GetChildren(export_path_, &subchildren));
    ASSERT_EQ(subchildren.size(), num_files_expected);
  };

  // Test DefaultColumnFamily
  {
    const auto key = std::string("foo");
    ASSERT_OK(Put(key, "v1"));

    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));

    // Export the Tables and verify
    ASSERT_OK(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                             export_path_, &metadata_));
    verify_files_exported(*metadata_, 1);
    ASSERT_EQ(metadata_->db_comparator_name, options.comparator->Name());
    ASSERT_OK(DestroyDir(env_, export_path_));
    delete metadata_;
    metadata_ = nullptr;

    // Check again after compaction
    CompactAll();
    ASSERT_OK(Put(key, "v2"));
    ASSERT_OK(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                             export_path_, &metadata_));
    verify_files_exported(*metadata_, 2);
    ASSERT_EQ(metadata_->db_comparator_name, options.comparator->Name());
    ASSERT_OK(DestroyDir(env_, export_path_));
    delete metadata_;
    metadata_ = nullptr;
    delete checkpoint;
  }

  // Test non default column family with non default comparator
  {
    auto cf_options = CurrentOptions();
    cf_options.comparator = ReverseBytewiseComparator();
    ASSERT_OK(db_->CreateColumnFamily(cf_options, "yoyo", &cfh_reverse_comp_));

    const auto key = std::string("foo");
    ASSERT_OK(db_->Put(WriteOptions(), cfh_reverse_comp_, key, "v1"));

    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));

    // Export the Tables and verify
    ASSERT_OK(checkpoint->ExportColumnFamily(cfh_reverse_comp_, export_path_,
                                             &metadata_));
    verify_files_exported(*metadata_, 1);
    ASSERT_EQ(metadata_->db_comparator_name,
              ReverseBytewiseComparator()->Name());
    delete checkpoint;
  }
}

TEST_F(CheckpointTest, ExportEmptyColumnFamily) {
  // Verify that exporting a column family with no levels (empty CF) does not
  // leak the allocated ExportImportFilesMetaData and correctly sets *metadata.
  auto options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({}, options);

  // Do NOT put any data -- the default CF has no levels.

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));

  ASSERT_OK(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                           export_path_, &metadata_));
  // metadata_ must be set even when the CF has no files.
  ASSERT_NE(metadata_, nullptr);
  ASSERT_EQ(metadata_->files.size(), 0);
  ASSERT_EQ(metadata_->db_comparator_name, options.comparator->Name());

  delete checkpoint;
}

TEST_F(CheckpointTest, ExportColumnFamilyNegativeTest) {
  // Create a database
  auto options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({}, options);

  const auto key = std::string("foo");
  ASSERT_OK(Put(key, "v1"));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));

  // Export onto existing directory
  ASSERT_OK(env_->CreateDirIfMissing(export_path_));
  ASSERT_EQ(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                           export_path_, &metadata_),
            Status::InvalidArgument("Specified export_dir exists"));
  ASSERT_OK(DestroyDir(env_, export_path_));

  // Export with invalid directory specification
  export_path_ = "";
  ASSERT_EQ(checkpoint->ExportColumnFamily(db_->DefaultColumnFamily(),
                                           export_path_, &metadata_),
            Status::InvalidArgument("Specified export_dir invalid"));
  delete checkpoint;
}

TEST_F(CheckpointTest, CheckpointCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointTest::CheckpointCF:2", "DBImpl::FlushAllColumnFamilies:2"},
       {"DBImpl::FlushAllColumnFamilies:1", "CheckpointTest::CheckpointCF:1"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Put(2, "two", "two"));
  ASSERT_OK(Put(3, "three", "three"));
  ASSERT_OK(Put(4, "four", "four"));
  ASSERT_OK(Put(5, "five", "five"));

  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  // Take a snapshot
  ROCKSDB_NAMESPACE::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:1");
  ASSERT_OK(Put(0, "Default", "Default1"));
  ASSERT_OK(Put(1, "one", "eleven"));
  ASSERT_OK(Put(2, "two", "twelve"));
  ASSERT_OK(Put(3, "three", "thirteen"));
  ASSERT_OK(Put(4, "four", "fourteen"));
  ASSERT_OK(Put(5, "five", "fifteen"));
  TEST_SYNC_POINT("CheckpointTest::CheckpointCF:2");
  t.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(Put(1, "one", "twentyone"));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  ASSERT_OK(Put(3, "three", "twentythree"));
  ASSERT_OK(Put(4, "four", "twentyfour"));
  ASSERT_OK(Put(5, "five", "twentyfive"));
  ASSERT_OK(Flush());

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.emplace_back(cfs[i], options);
  }
  std::unique_ptr<DB> snapshotDB;
  ASSERT_OK(DB::Open(options, snapshot_name_, column_families, &cphandles,
                     &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default1", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("eleven", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  for (auto h : cphandles) {
    delete h;
  }
  cphandles.clear();
  snapshotDB.reset();
}

namespace {
// The subset-checkpoint tests all share a {default, "one", "two", "three"}
// layout, with handles in that order.
constexpr size_t kNumSubsetCheckpointCfs = 4;

void AppendIncompleteAtomicGroupToManifest(Env* env,
                                           const std::string& manifest_path) {
  uint64_t file_size;
  ASSERT_OK(env->GetFileSize(manifest_path, &file_size));
  const std::shared_ptr<FileSystem>& fs = env->GetFileSystem();
  std::unique_ptr<FSWritableFile> fs_file;
  ASSERT_OK(fs->ReopenWritableFile(manifest_path, FileOptions(), &fs_file,
                                   /*dbg=*/nullptr));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(fs_file), manifest_path, FileOptions()));
  log::Writer log_writer(std::move(file_writer), /*log_number=*/0,
                         /*recycle_log_files=*/false,
                         /*manual_flush=*/false, kNoCompression,
                         /*track_and_verify_wals=*/false,
                         file_size % log::kBlockSize);

  VersionEdit first_edit;
  first_edit.SetLogNumber(0);
  first_edit.SetNextFile(100);
  first_edit.SetLastSequence(100);
  first_edit.MarkAtomicGroup(2);
  std::string first_record;
  ASSERT_TRUE(first_edit.EncodeTo(&first_record, 0));
  ASSERT_OK(log_writer.AddRecord(WriteOptions(), first_record));

  VersionEdit second_edit;
  second_edit.SetLogNumber(0);
  second_edit.SetNextFile(100);
  second_edit.SetLastSequence(100);
  second_edit.MarkAtomicGroup(1);
  std::string second_record;
  ASSERT_TRUE(second_edit.EncodeTo(&second_record, 0));
  ASSERT_OK(log_writer.AddRecord(WriteOptions(), second_record));
  // Omit the final remaining_entries=0 record, leaving the group incomplete.
}

// Writes one key per column family and flushes, so each family owns exactly one
// SST file and the families excluded from a subset checkpoint are visible as
// missing SSTs.
void PopulateFourCfsForSubsetCheckpoint(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles) {
  ASSERT_EQ(kNumSubsetCheckpointCfs, handles.size());
  ASSERT_OK(db->Put(WriteOptions(), handles[0], "d_key", "d_val"));
  ASSERT_OK(db->Put(WriteOptions(), handles[1], "one_key", "one_val"));
  ASSERT_OK(db->Put(WriteOptions(), handles[2], "two_key", "two_val"));
  ASSERT_OK(db->Put(WriteOptions(), handles[3], "three_key", "three_val"));
  for (auto* handle : handles) {
    ASSERT_OK(db->Flush(FlushOptions(), handle));
  }
}

// Guards against a subset checkpoint dropping column families from the live DB
// while post-processing the checkpoint's MANIFEST.
void VerifyFourCfsIntact(DB* db,
                         const std::vector<ColumnFamilyHandle*>& handles) {
  ASSERT_EQ(kNumSubsetCheckpointCfs, handles.size());
  std::string val;
  ASSERT_OK(db->Get(ReadOptions(), handles[0], "d_key", &val));
  ASSERT_EQ("d_val", val);
  ASSERT_OK(db->Get(ReadOptions(), handles[1], "one_key", &val));
  ASSERT_EQ("one_val", val);
  ASSERT_OK(db->Get(ReadOptions(), handles[2], "two_key", &val));
  ASSERT_EQ("two_val", val);
  ASSERT_OK(db->Get(ReadOptions(), handles[3], "three_key", &val));
  ASSERT_EQ("three_val", val);
}

int CountSstFiles(Env* env, const std::string& dir) {
  std::vector<std::string> children;
  EXPECT_OK(env->GetChildren(dir, &children));
  int n = 0;
  for (const auto& c : children) {
    uint64_t number;
    FileType type;
    if (ParseFileName(c, &number, &type) && type == kTableFile) {
      ++n;
    }
  }
  return n;
}

// Verifies a checkpoint taken from the {default, "one", "two", "three"} layout
// built by the subset tests, when only {default, "two"} were requested: the
// other families' SSTs were never captured, and the families themselves are
// gone from the checkpoint's MANIFEST.
void VerifyDefaultAndTwoSubsetCheckpoint(Env* env, const std::string& dir,
                                         Options options) {
  // 4 CFs x 1 SST each, of which only default's and two's are kept.
  ASSERT_EQ(2, CountSstFiles(env, dir));

  ConfigOptions config_options;
  config_options.env = env;
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_cfs;
  ASSERT_OK(
      LoadLatestOptions(config_options, dir, &loaded_db_options, &loaded_cfs));
  ASSERT_EQ(options.track_options_file_number_in_manifest,
            loaded_db_options.track_options_file_number_in_manifest);
  ASSERT_EQ(2U, loaded_cfs.size());
  ASSERT_EQ(kDefaultColumnFamilyName, loaded_cfs[0].name);
  ASSERT_EQ("two", loaded_cfs[1].name);

  options.create_if_missing = false;

  // Opening with exactly {default, two} succeeds and returns the values.
  {
    std::vector<ColumnFamilyDescriptor> cfds;
    cfds.emplace_back(kDefaultColumnFamilyName, options);
    cfds.emplace_back("two", options);
    std::vector<ColumnFamilyHandle*> cph;
    std::unique_ptr<DB> snapshotDB;
    ASSERT_OK(DB::Open(options, dir, cfds, &cph, &snapshotDB));
    std::string result;
    ASSERT_OK(snapshotDB->Get(ReadOptions(), cph[0], "d_key", &result));
    ASSERT_EQ("d_val", result);
    ASSERT_OK(snapshotDB->Get(ReadOptions(), cph[1], "two_key", &result));
    ASSERT_EQ("two_val", result);
    for (auto h : cph) {
      delete h;
    }
    snapshotDB.reset();
  }

  // The excluded column families no longer exist in the checkpoint: opening
  // with the full CF list fails.
  {
    std::vector<ColumnFamilyDescriptor> cfds;
    for (const std::string& cf : std::vector<std::string>{
             kDefaultColumnFamilyName, "one", "two", "three"}) {
      cfds.emplace_back(cf, options);
    }
    std::vector<ColumnFamilyHandle*> cph;
    std::unique_ptr<DB> snapshotDB;
    Status s = DB::Open(options, dir, cfds, &cph, &snapshotDB);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    for (auto h : cph) {
      delete h;
    }
  }
}
}  // namespace

TEST_F(CheckpointTest, CheckpointSubsetOfCF) {
  Options options = CurrentOptions();
  options.track_options_file_number_in_manifest = true;
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);

  // Checkpoint only {default, two}.
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  ASSERT_OK(uchk->CreateCheckpoint(snapshot_name_, {handles_[0], handles_[2]}));

  VerifyFourCfsIntact(db_.get(), handles_);

  // Close and reopen the source DB with the full CF list to prove the live
  // MANIFEST was not mutated by the checkpoint post-processing.
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "one", "two", "three"},
                           options);
  VerifyFourCfsIntact(db_.get(), handles_);

  VerifyDefaultAndTwoSubsetCheckpoint(env_, snapshot_name_, options);
}

TEST_F(CheckpointTest, CheckpointSubsetOfCFLegacyOptionsSelection) {
  Options options = CurrentOptions();
  options.track_options_file_number_in_manifest = false;
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  ASSERT_OK(uchk->CreateCheckpoint(snapshot_name_, {handles_[0], handles_[2]}));

  VerifyDefaultAndTwoSubsetCheckpoint(env_, snapshot_name_, options);
}

TEST_F(CheckpointTest, SubsetOptionsAndManifestUseSameMembershipSnapshot) {
  Options options = CurrentOptions();
  options.track_options_file_number_in_manifest = true;
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);

  bool dropped_after_capture = false;
  Status drop_status;
  SyncPoint::GetInstance()->SetCallBack(
      "CheckpointImpl::CreateCustomCheckpoint:AfterGetLive1", [&](void*) {
        if (!dropped_after_capture) {
          dropped_after_capture = true;
          drop_status = db_->DropColumnFamily(handles_[2]);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer sync_point_cleanup([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  ASSERT_OK(uchk->CreateCheckpoint(snapshot_name_, {handles_[0], handles_[2]}));
  ASSERT_TRUE(dropped_after_capture);
  ASSERT_OK(drop_status);

  // The included CF was dropped in the source after live-file capture. The
  // checkpoint must still use the earlier membership for both MANIFEST and
  // OPTIONS rather than re-reading the now-dropped live CF set.
  VerifyDefaultAndTwoSubsetCheckpoint(env_, snapshot_name_, options);
}

TEST_F(CheckpointTest, SubsetOptionsFileRacesWithMutableFilterPolicy) {
  // RibbonFilterPolicy::bloom_before_level is mutated in place by SetOptions,
  // and the subset checkpoint's options snapshot shares the live policy
  // object. Persisting that snapshot must not interleave with SetOptions.
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(
      NewRibbonFilterPolicy(10, /*bloom_before_level=*/1));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);

  const std::thread::id checkpoint_thread_id = std::this_thread::get_id();
  std::atomic<bool> injected{false};
  std::atomic<bool> set_options_done{false};
  Status set_options_status;
  std::thread set_options_thread;
  SyncPoint::GetInstance()->SetCallBack(
      "PersistRocksDBOptions:written", [&](void*) {
        if (std::this_thread::get_id() != checkpoint_thread_id ||
            injected.exchange(true)) {
          return;
        }
        set_options_thread = std::thread([&]() {
          set_options_status = db_->SetOptions(
              handles_[0],
              {{"table_factory.filter_policy.bloom_before_level", "2"}});
          set_options_done.store(true);
        });
        // Give an unserialized SetOptions the chance to land between writing
        // the checkpoint OPTIONS file and verifying it.
        for (int i = 0; i < 100 && !set_options_done.load(); ++i) {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer sync_point_cleanup([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  Status checkpoint_status =
      uchk->CreateCheckpoint(snapshot_name_, {handles_[0], handles_[2]});
  if (set_options_thread.joinable()) {
    set_options_thread.join();
  }
  ASSERT_TRUE(injected.load());
  ASSERT_OK(set_options_status);
  ASSERT_OK(checkpoint_status);
}

TEST_F(CheckpointTest,
       CheckpointSubsetIncompleteAtomicGroupAfterWritableRecovery) {
  Options options = CurrentOptions();
  options.optimize_manifest_for_recovery = true;
  options.reuse_manifest_on_open = false;
  options.track_and_verify_wals_in_manifest = true;
  options.avoid_flush_during_recovery = true;
  options.write_dbid_to_manifest = false;
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);
  Close();

  std::string manifest_path;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, env_->GetFileSystem().get(),
                                   /*is_retry=*/false, &manifest_path,
                                   &manifest_file_number));
  AppendIncompleteAtomicGroupToManifest(env_, manifest_path);

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "one", "two", "three"},
                           options);
  std::string manifest_path_after_reopen;
  ASSERT_OK(GetCurrentManifestPath(
      dbname_, env_->GetFileSystem().get(), /*is_retry=*/false,
      &manifest_path_after_reopen, &manifest_file_number));
  ASSERT_EQ(manifest_path, manifest_path_after_reopen);

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  ASSERT_OK(uchk->CreateCheckpoint(snapshot_name_, {handles_[0], handles_[2]},
                                   std::numeric_limits<uint64_t>::max()));

  VerifyDefaultAndTwoSubsetCheckpoint(env_, snapshot_name_, options);
}

TEST_F(CheckpointTest,
       CheckpointSubsetIncompleteAtomicGroupAfterReadOnlyRecovery) {
  Options options = CurrentOptions();
  options.optimize_manifest_for_recovery = true;
  options.reuse_manifest_on_open = false;
  options.track_and_verify_wals_in_manifest = true;
  options.avoid_flush_during_recovery = true;
  options.write_dbid_to_manifest = false;
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);
  Close();

  std::string manifest_path;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, env_->GetFileSystem().get(),
                                   /*is_retry=*/false, &manifest_path,
                                   &manifest_file_number));
  AppendIncompleteAtomicGroupToManifest(env_, manifest_path);

  ASSERT_OK(ReadOnlyReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, "one", "two", "three"}, options));
  std::string manifest_path_after_reopen;
  ASSERT_OK(GetCurrentManifestPath(
      dbname_, env_->GetFileSystem().get(), /*is_retry=*/false,
      &manifest_path_after_reopen, &manifest_file_number));
  ASSERT_EQ(manifest_path, manifest_path_after_reopen);

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  ASSERT_OK(uchk->CreateCheckpoint(snapshot_name_, {handles_[0], handles_[2]},
                                   std::numeric_limits<uint64_t>::max()));

  VerifyDefaultAndTwoSubsetCheckpoint(env_, snapshot_name_, options);
}

TEST_F(CheckpointTest, CheckpointSubsetEmptyListEqualsAll) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two"}, options);
  ASSERT_OK(Put(0, "d", "0"));
  ASSERT_OK(Put(1, "a", "1"));
  ASSERT_OK(Put(2, "b", "2"));
  for (int cf = 0; cf <= 2; ++cf) {
    ASSERT_OK(Flush(cf));
  }

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  // Empty list is equivalent to a whole-DB checkpoint.
  ASSERT_OK(uchk->CreateCheckpoint(snapshot_name_,
                                   std::vector<ColumnFamilyHandle*>{}));

  // All three CFs' SSTs are present.
  ASSERT_EQ(3, CountSstFiles(env_, snapshot_name_));

  options.create_if_missing = false;
  std::vector<ColumnFamilyDescriptor> cfds;
  for (const std::string& cf :
       std::vector<std::string>{kDefaultColumnFamilyName, "one", "two"}) {
    cfds.emplace_back(cf, options);
  }
  std::vector<ColumnFamilyHandle*> cph;
  std::unique_ptr<DB> snapshotDB;
  ASSERT_OK(DB::Open(options, snapshot_name_, cfds, &cph, &snapshotDB));
  std::string result;
  ASSERT_OK(snapshotDB->Get(ReadOptions(), cph[1], "a", &result));
  ASSERT_EQ("1", result);
  ASSERT_OK(snapshotDB->Get(ReadOptions(), cph[2], "b", &result));
  ASSERT_EQ("2", result);
  for (auto h : cph) {
    delete h;
  }
  snapshotDB.reset();
}

TEST_F(CheckpointTest, CheckpointSubsetNullHandleRejected) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one"}, options);
  ASSERT_OK(Put(1, "a", "1"));
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  ASSERT_TRUE(uchk->CreateCheckpoint(snapshot_name_, {handles_[1], nullptr})
                  .IsInvalidArgument());
}

TEST_F(CheckpointTest, CheckpointSubsetForeignHandleRejected) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one"}, options);
  ASSERT_OK(Put(1, "a", "1"));

  // Open a second, unrelated DB with its own CF whose id will collide with
  // "one" from this DB (both are the first non-default CF -> id 1).
  const std::string other_dbname = test::PerThreadDBPath("other_db_for_ckpt");
  ASSERT_OK(DestroyDB(other_dbname, options));
  Options open_opts = options;
  open_opts.create_if_missing = true;
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(open_opts, other_dbname, &other_db));
  ColumnFamilyHandle* other_cf = nullptr;
  ASSERT_OK(
      other_db->CreateColumnFamily(ColumnFamilyOptions(), "one", &other_cf));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  Status s = uchk->CreateCheckpoint(snapshot_name_, {other_cf});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  ASSERT_OK(other_db->DestroyColumnFamilyHandle(other_cf));
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

TEST_F(CheckpointTest, CheckpointSubsetDroppedHandleRejected) {
  // A CF handle for a CF that was already dropped must not silently succeed;
  // GetLiveFilesStorageInfoForSubsetCheckpoint would skip it, producing a
  // checkpoint missing what the caller asked for.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one"}, options);
  ASSERT_OK(Put(1, "a", "1"));
  ColumnFamilyHandle* dropped_handle = handles_[1];
  ASSERT_OK(db_->DropColumnFamily(dropped_handle));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> uchk(checkpoint);
  Status s = uchk->CreateCheckpoint(snapshot_name_, {dropped_handle});
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(CheckpointTest, CheckpointCFNoFlush) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three", "four", "five"}, options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "one", "one"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(2, "two", "two"));

  ReadOptions roptions;
  std::string result;
  std::vector<ColumnFamilyHandle*> cphandles;

  // Take a snapshot
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void* /*arg*/) {
        // Flush should never trigger.
        FAIL();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, 1000000));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  delete checkpoint;
  ASSERT_OK(Put(1, "one", "two"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "two", "twentytwo"));
  Close();
  EXPECT_OK(DestroyDB(dbname_, options));

  // Open snapshot and verify contents while DB is running
  options.create_if_missing = false;
  std::vector<std::string> cfs;
  cfs = {kDefaultColumnFamilyName, "one", "two", "three", "four", "five"};
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.emplace_back(cfs[i], options);
  }
  std::unique_ptr<DB> snapshotDB;
  ASSERT_OK(DB::Open(options, snapshot_name_, column_families, &cphandles,
                     &snapshotDB));
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "Default", &result));
  ASSERT_EQ("Default", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "one", &result));
  ASSERT_EQ("one", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "two", &result));
  ASSERT_EQ("two", result);
  for (auto h : cphandles) {
    delete h;
  }
  cphandles.clear();
  snapshotDB.reset();
}

TEST_F(CheckpointTest, CurrentFileModifiedWhileCheckpointing) {
  Options options = CurrentOptions();
  options.max_manifest_file_size = 0;  // always rollover manifest for file add
  options.max_manifest_space_amp_pct = 0;
  Reopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {// Get past the flush in the checkpoint thread before adding any keys to
       // the db so the checkpoint thread won't hit the WriteManifest
       // syncpoints.
       {"CheckpointImpl::CreateCheckpoint:FlushDone",
        "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut"},
       // Roll the manifest during checkpointing right after live files are
       // snapshotted.
       {"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "VersionSet::LogAndApply:WriteManifest"},
       {"VersionSet::LogAndApply:WriteManifestDone",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing:PrePut");
  ASSERT_OK(Put("Default", "Default1"));
  ASSERT_OK(Flush());
  t.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Successful Open() implies that CURRENT pointed to the manifest in the
  // checkpoint.
  std::unique_ptr<DB> snapshotDB;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshotDB));
}

TEST_F(CheckpointTest, CurrentFileModifiedWhileCheckpointing2PC) {
  Close();
  const std::string dbname = test::PerThreadDBPath("transaction_testdb");
  ASSERT_OK(DestroyDB(dbname, CurrentOptions()));
  test::DeleteDir(env_, dbname);

  Options options = CurrentOptions();
  options.allow_2pc = true;
  // allow_2pc is implicitly set with tx prepare
  // options.allow_2pc = true;
  TransactionDBOptions txn_db_options;
  TransactionDB* txdb;
  Status s = TransactionDB::Open(options, txn_db_options, dbname, &txdb);
  ASSERT_OK(s);
  ColumnFamilyHandle* cfa;
  ColumnFamilyHandle* cfb;
  ColumnFamilyOptions cf_options;
  ASSERT_OK(txdb->CreateColumnFamily(cf_options, "CFA", &cfa));

  WriteOptions write_options;
  // Insert something into CFB so lots of log files will be kept
  // before creating the checkpoint.
  ASSERT_OK(txdb->CreateColumnFamily(cf_options, "CFB", &cfb));
  ASSERT_OK(txdb->Put(write_options, cfb, "", ""));

  ReadOptions read_options;
  std::string value;
  TransactionOptions txn_options;
  Transaction* txn = txdb->BeginTransaction(write_options, txn_options);
  s = txn->SetName("xid");
  ASSERT_OK(s);
  ASSERT_EQ(txdb->GetTransactionByName("xid"), txn);

  s = txn->Put(Slice("foo"), Slice("bar"));
  ASSERT_OK(s);
  s = txn->Put(cfa, Slice("foocfa"), Slice("barcfa"));
  ASSERT_OK(s);
  // Writing prepare into middle of first WAL, then flush WALs many times
  for (int i = 1; i <= 100000; i++) {
    Transaction* tx = txdb->BeginTransaction(write_options, txn_options);
    ASSERT_OK(tx->SetName("x"));
    ASSERT_OK(tx->Put(Slice(std::to_string(i)), Slice("val")));
    ASSERT_OK(tx->Put(cfa, Slice("aaa"), Slice("111")));
    ASSERT_OK(tx->Prepare());
    ASSERT_OK(tx->Commit());
    if (i % 10000 == 0) {
      ASSERT_OK(txdb->Flush(FlushOptions()));
    }
    if (i == 88888) {
      ASSERT_OK(txn->Prepare());
    }
    delete tx;
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"CheckpointImpl::CreateCheckpoint:SavedLiveFiles1",
        "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PreCommit"},
       {"CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PostCommit",
        "CheckpointImpl::CreateCheckpoint:SavedLiveFiles2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ROCKSDB_NAMESPACE::port::Thread t([&]() {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(txdb, &checkpoint));
    ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
    delete checkpoint;
  });
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PreCommit");
  ASSERT_OK(txn->Commit());
  delete txn;
  TEST_SYNC_POINT(
      "CheckpointTest::CurrentFileModifiedWhileCheckpointing2PC:PostCommit");
  t.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // No more than two logs files should exist.
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(snapshot_name_, &files));
  int num_log_files = 0;
  for (auto& file : files) {
    uint64_t num;
    FileType type;
    WalFileType log_type;
    if (ParseFileName(file, &num, &type, &log_type) && type == kWalFile) {
      num_log_files++;
    }
  }
  // One flush after preapare + one outstanding file before checkpoint + one log
  // file generated after checkpoint.
  ASSERT_LE(num_log_files, 3);

  TransactionDB* snapshotDB;
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  column_families.emplace_back("CFA", ColumnFamilyOptions());
  column_families.emplace_back("CFB", ColumnFamilyOptions());
  std::vector<ROCKSDB_NAMESPACE::ColumnFamilyHandle*> cf_handles;
  ASSERT_OK(TransactionDB::Open(options, txn_db_options, snapshot_name_,
                                column_families, &cf_handles, &snapshotDB));
  ASSERT_OK(snapshotDB->Get(read_options, "foo", &value));
  ASSERT_EQ(value, "bar");
  ASSERT_OK(snapshotDB->Get(read_options, cf_handles[1], "foocfa", &value));
  ASSERT_EQ(value, "barcfa");

  delete cfa;
  delete cfb;
  delete cf_handles[0];
  delete cf_handles[1];
  delete cf_handles[2];
  delete snapshotDB;
  snapshotDB = nullptr;
  delete txdb;
}

TEST_F(CheckpointTest, CheckpointInvalidDirectoryName) {
  for (std::string checkpoint_dir : {"", "/", "////"}) {
    Checkpoint* checkpoint;
    ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
    ASSERT_TRUE(
        checkpoint->CreateCheckpoint(checkpoint_dir).IsInvalidArgument());
    delete checkpoint;
  }
}

TEST_F(CheckpointTest, CheckpointWithParallelWrites) {
  // When run with TSAN, this exposes the data race fixed in
  // https://github.com/meta/rocksdb/pull/3603
  ASSERT_OK(Put("key1", "val1"));
  port::Thread thread([this]() { ASSERT_OK(Put("key2", "val2")); });
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  thread.join();
}

class CheckpointTestWithWalParams
    : public CheckpointTest,
      public testing::WithParamInterface<
          std::tuple<uint64_t, bool, bool, bool>> {
 public:
  uint64_t GetLogSizeForFlush() { return std::get<0>(GetParam()); }
  bool GetWalsInManifest() { return std::get<1>(GetParam()); }
  bool GetManualWalFlush() { return std::get<2>(GetParam()); }
  bool GetBackgroundCloseInactiveWals() { return std::get<3>(GetParam()); }
};

INSTANTIATE_TEST_CASE_P(NormalWalParams, CheckpointTestWithWalParams,
                        ::testing::Combine(::testing::Values(0U, 100000000U),
                                           ::testing::Bool(), ::testing::Bool(),
                                           ::testing::Values(false)));

INSTANTIATE_TEST_CASE_P(DeprecatedWalParams, CheckpointTestWithWalParams,
                        ::testing::Values(std::make_tuple(100000000U, true,
                                                          false, true)));

TEST_P(CheckpointTestWithWalParams, CheckpointWithUnsyncedDataDropped) {
  Options options = CurrentOptions();
  options.max_write_buffer_number = 4;
  options.track_and_verify_wals_in_manifest = GetWalsInManifest();
  options.manual_wal_flush = GetManualWalFlush();
  options.background_close_inactive_wals = GetBackgroundCloseInactiveWals();
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));

  if (options.background_close_inactive_wals) {
    // Disable this hygiene check when the fix is disabled
    fault_fs->SetAllowLinkOpenFile();
  }

  options.env = fault_fs_env.get();
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  if (GetLogSizeForFlush() > 0) {
    // When not flushing memtable for checkpoint, this is the simplest way
    // to get
    // * one inactive WAL, synced
    // * one inactive WAL, not synced, and
    // * one active WAL, not synced
    // with a single thread, so that we have at least one that can be hard
    // linked, etc.
    ASSERT_OK(dbfull()->PauseBackgroundWork());
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    ASSERT_OK(db_->SyncWAL());
  }
  ASSERT_OK(Put("key2", "val2"));
  if (GetLogSizeForFlush() > 0) {
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  }
  ASSERT_OK(Put("key3", "val3"));
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, GetLogSizeForFlush()));
  delete checkpoint;
  ASSERT_OK(fault_fs->DropUnsyncedFileData());
  // make sure it's openable even though whatever data that wasn't synced got
  // dropped.
  options.env = env_;
  std::unique_ptr<DB> snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "key1", &get_result));
  ASSERT_EQ("val1", get_result);
  ASSERT_OK(snapshot_db->Get(read_opts, "key2", &get_result));
  ASSERT_EQ("val2", get_result);
  ASSERT_OK(snapshot_db->Get(read_opts, "key3", &get_result));
  ASSERT_EQ("val3", get_result);
  snapshot_db.reset();
  db_.reset();
}

TEST_F(CheckpointTest, CheckpointReadOnlyDB) {
  ASSERT_OK(Put("foo", "foo_value"));
  ASSERT_OK(Flush());
  Close();
  Options options = CurrentOptions();
  ASSERT_OK(ReadOnlyReopen(options));
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;
  Close();
  std::unique_ptr<DB> snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("foo_value", get_result);
}

TEST_F(CheckpointTest, CheckpointWithLockWAL) {
  Options options = CurrentOptions();
  ASSERT_OK(Put("foo", "foo_value"));

  ASSERT_OK(db_->LockWAL());

  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;

  ASSERT_OK(db_->UnlockWAL());
  Close();

  std::unique_ptr<DB> snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("foo_value", get_result);
}

TEST_F(CheckpointTest, CheckpointWithLockWALNoWALSameThreadAborted) {
  WriteOptions write_options;
  write_options.disableWAL = true;
  ASSERT_OK(db_->Put(write_options, "foo", "foo_value"));
  ASSERT_OK(db_->LockWAL());

  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> checkpoint_holder(checkpoint);

  Status s = checkpoint->CreateCheckpoint(snapshot_name_);
  ASSERT_TRUE(s.IsAborted()) << s.ToString();
  ASSERT_NE(s.ToString().find("Likely deadlock"), std::string::npos)
      << s.ToString();

  ASSERT_OK(db_->UnlockWAL());
}

TEST_F(CheckpointTest, CheckpointWithLockWALNoWALCrossThreadFlushes) {
  Options options = CurrentOptions();
  WriteOptions write_options;
  write_options.disableWAL = true;
  ASSERT_OK(db_->Put(write_options, "foo", "foo_value"));
  ASSERT_OK(db_->LockWAL());

  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> checkpoint_holder(checkpoint);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WaitForPendingWrites:BeforeBlock",
        "CheckpointTest::CheckpointWithLockWALNoWALCrossThreadFlushes:"
        "BeforeUnlock"}});
  SyncPoint::GetInstance()->EnableProcessing();

  std::atomic<bool> checkpoint_done{false};
  Status checkpoint_status;
  port::Thread checkpoint_thread([&]() {
    checkpoint_status = checkpoint->CreateCheckpoint(snapshot_name_);
    checkpoint_done.store(true);
  });

  TEST_SYNC_POINT(
      "CheckpointTest::CheckpointWithLockWALNoWALCrossThreadFlushes:"
      "BeforeUnlock");
  const bool finished_early = checkpoint_done.load();
  Status unlock_status = db_->UnlockWAL();
  checkpoint_thread.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});

  ASSERT_OK(unlock_status);
  ASSERT_FALSE(finished_early) << checkpoint_status.ToString();
  ASSERT_OK(checkpoint_status);
  Close();

  std::unique_ptr<DB> snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("foo_value", get_result);
}

TEST_F(CheckpointTest, CheckpointWithLockWALRejectsBlobDirectWrite) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.min_blob_size = 32;
  Reopen(options);

  ASSERT_OK(Put("foo", std::string(128, 'b')));
  ASSERT_OK(db_->LockWAL());

  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> checkpoint_holder(checkpoint);

  Status s = checkpoint->CreateCheckpoint(snapshot_name_);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_NE(s.ToString().find("Blob direct write"), std::string::npos)
      << s.ToString();

  ASSERT_OK(db_->UnlockWAL());
}

TEST_F(CheckpointTest, CheckpointReadOnlyDBWithMultipleColumnFamilies) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  for (int i = 0; i != 3; ++i) {
    ASSERT_OK(Put(i, "foo", "foo_value"));
    ASSERT_OK(Flush(i));
  }
  Close();
  Status s = ReadOnlyReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, "pikachu", "eevee"}, options);
  ASSERT_OK(s);
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;
  checkpoint = nullptr;
  Close();

  std::vector<ColumnFamilyDescriptor> column_families{
      {kDefaultColumnFamilyName, options},
      {"pikachu", options},
      {"eevee", options}};
  std::unique_ptr<DB> snapshot_db;
  std::vector<ColumnFamilyHandle*> snapshot_handles;
  s = DB::Open(options, snapshot_name_, column_families, &snapshot_handles,
               &snapshot_db);
  ASSERT_OK(s);
  ReadOptions read_opts;
  for (int i = 0; i != 3; ++i) {
    std::string get_result;
    s = snapshot_db->Get(read_opts, snapshot_handles[i], "foo", &get_result);
    ASSERT_OK(s);
    ASSERT_EQ("foo_value", get_result);
  }

  for (auto snapshot_h : snapshot_handles) {
    delete snapshot_h;
  }
  snapshot_handles.clear();
}

TEST_F(CheckpointTest, CheckpointWithDbPath) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_ + "_2", 0);
  Reopen(options);
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Flush());
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  // Currently not supported
  ASSERT_TRUE(checkpoint->CreateCheckpoint(snapshot_name_).IsNotSupported());
  delete checkpoint;
}

TEST_F(CheckpointTest, CheckpointWithArchievedLog) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"WalManager::ArchiveWALFile",
        "CheckpointTest:CheckpointWithArchievedLog"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Options options = CurrentOptions();
  options.WAL_ttl_seconds = 3600;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", std::string(1024 * 1024, 'a')));
  // flush and archive the first log
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", std::string(1024, 'a')));

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  TEST_SYNC_POINT("CheckpointTest:CheckpointWithArchievedLog");
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_, 1024 * 1024));
  // unflushed log size < 1024 * 1024 < total file size including archived log,
  // so flush shouldn't occur, there is only one file at level 0
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  delete checkpoint;
  checkpoint = nullptr;

  std::unique_ptr<DB> snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "key1", &get_result));
  ASSERT_EQ(std::string(1024 * 1024, 'a'), get_result);
  get_result.clear();
  ASSERT_OK(snapshot_db->Get(read_opts, "key2", &get_result));
  ASSERT_EQ(std::string(1024, 'a'), get_result);
}

class CheckpointDestroyTest : public CheckpointTest,
                              public testing::WithParamInterface<bool> {};

TEST_P(CheckpointDestroyTest, DisableEnableSlowDeletion) {
  bool slow_deletion = GetParam();
  Options options = CurrentOptions();
  options.num_levels = 2;
  options.disable_auto_compactions = true;
  Status s;
  options.sst_file_manager.reset(NewSstFileManager(
      options.env, options.info_log, "", slow_deletion ? 1024 * 1024 : 0,
      false /* delete_existing_trash */, &s, 1));
  ASSERT_OK(s);
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "a"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("bar", "b"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("bar", "val" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);
  ASSERT_EQ(NumTableFilesAtLevel(1), 2);

  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));

  delete checkpoint;
  checkpoint = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 2);

  std::unique_ptr<DB> snapshot_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &snapshot_db));
  ReadOptions read_opts;
  std::string get_result;
  ASSERT_OK(snapshot_db->Get(read_opts, "foo", &get_result));
  ASSERT_EQ("a", get_result);
  ASSERT_OK(snapshot_db->Get(read_opts, "bar", &get_result));
  ASSERT_EQ("val9", get_result);
  snapshot_db.reset();

  // Make sure original obsolete files for hard linked files are all deleted.
  dbfull()->TEST_DeleteObsoleteFiles();
  auto sfm = static_cast_with_check<SstFileManagerImpl>(
      options.sst_file_manager.get());
  ASSERT_NE(nullptr, sfm);
  sfm->WaitForEmptyTrash();
  // SST file 2-12 for "bar" will be compacted into one file on L1 during the
  // compaction  after checkpoint is created. SST file 1 on L1: foo, seq:
  // 1 (hard links is 1 after checkpoint destroy)
  std::atomic<int> bg_delete_sst{0};
  std::atomic<int> fg_delete_sst{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile::cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto file_name = *static_cast<std::string*>(arg);
        if (file_name.size() >= 4 &&
            file_name.compare(file_name.size() - 4, 4, ".sst") == 0) {
          fg_delete_sst.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile::cb", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto file_name = *static_cast<std::string*>(arg);
        if (file_name.size() >= 10 &&
            file_name.compare(file_name.size() - 10, 10, ".sst.trash") == 0) {
          bg_delete_sst.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(DestroyDB(snapshot_name_, options));
  if (slow_deletion) {
    ASSERT_EQ(fg_delete_sst, 1);
    ASSERT_EQ(bg_delete_sst, 11);
  } else {
    ASSERT_EQ(fg_delete_sst, 12);
  }

  ASSERT_EQ("a", Get("foo"));
  ASSERT_EQ("val9", Get("bar"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}
INSTANTIATE_TEST_CASE_P(CheckpointDestroyTest, CheckpointDestroyTest,
                        ::testing::Values(true, false));

TEST_F(CheckpointTest, CheckpointCFAtomicFlushOverride) {
  // Test that LiveFilesStorageInfoOptions::atomic_flush forces atomic flush
  // even when DBOptions::atomic_flush is false. Verify the checkpoint is
  // valid by creating it, opening it, and checking data consistency.
  Options options = CurrentOptions();
  options.atomic_flush = false;
  CreateAndReopenWithCF({"one", "two", "three"}, options);

  // Write data to multiple CFs, flush some to SST, leave some in memtable.
  ASSERT_OK(Put(0, "key0", "val0_old"));
  ASSERT_OK(Put(1, "key1", "val1_old"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(0, "key0", "val0"));
  ASSERT_OK(Put(1, "key1", "val1"));
  ASSERT_OK(Put(2, "key2", "val2"));
  ASSERT_OK(Put(3, "key3", "val3"));

  // Verify that atomic flush path is taken via sync point.
  bool atomic_flush_called = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
      [&](void* /*arg*/) { atomic_flush_called = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Create checkpoint using GetLiveFilesStorageInfo with atomic_flush=true.
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  // CreateCheckpoint internally calls GetLiveFilesStorageInfo.
  // To exercise the atomic_flush flag, use GetLiveFilesStorageInfo directly
  // and verify the path, then also create a full checkpoint to verify
  // end-to-end correctness.
  LiveFilesStorageInfoOptions live_opts;
  live_opts.atomic_flush = true;
  std::vector<LiveFileStorageInfo> files;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(live_opts, &files));
  ASSERT_TRUE(atomic_flush_called);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Now create a standard checkpoint (after the atomic flush has happened,
  // data is already on disk so checkpoint will capture it).
  ASSERT_OK(checkpoint->CreateCheckpoint(snapshot_name_));
  delete checkpoint;

  // Open checkpoint and verify all data across CFs is present.
  options.create_if_missing = false;
  std::vector<std::string> cfs = {kDefaultColumnFamilyName, "one", "two",
                                  "three"};
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.reserve(cfs.size());
  for (const auto& cf : cfs) {
    column_families.emplace_back(cf, options);
  }
  std::vector<ColumnFamilyHandle*> cphandles;
  std::unique_ptr<DB> snapshotDB;
  ASSERT_OK(DB::Open(options, snapshot_name_, column_families, &cphandles,
                     &snapshotDB));

  ReadOptions roptions;
  std::string result;
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[0], "key0", &result));
  ASSERT_EQ("val0", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[1], "key1", &result));
  ASSERT_EQ("val1", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[2], "key2", &result));
  ASSERT_EQ("val2", result);
  ASSERT_OK(snapshotDB->Get(roptions, cphandles[3], "key3", &result));
  ASSERT_EQ("val3", result);

  for (auto h : cphandles) {
    delete h;
  }
  cphandles.clear();
  snapshotDB.reset();
}

TEST_F(CheckpointTest, CheckpointCFDefaultNoAtomicFlush) {
  // Negative test: verify that when LiveFilesStorageInfoOptions::atomic_flush
  // is false (default), the non-atomic flush path is taken.
  Options options = CurrentOptions();
  options.atomic_flush = false;
  CreateAndReopenWithCF({"one", "two"}, options);

  ASSERT_OK(Put(0, "key0", "val0"));
  ASSERT_OK(Put(1, "key1", "val1"));
  ASSERT_OK(Put(2, "key2", "val2"));

  bool atomic_flush_called = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
      [&](void* /*arg*/) { atomic_flush_called = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Default LiveFilesStorageInfoOptions has atomic_flush=false.
  LiveFilesStorageInfoOptions live_opts;
  std::vector<LiveFileStorageInfo> files;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(live_opts, &files));
  ASSERT_FALSE(atomic_flush_called);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(CheckpointTest, CheckpointAtomicFlushOverrideSingleCF) {
  // Edge case: forced atomic flush with only the default CF.
  Options options = CurrentOptions();
  options.atomic_flush = false;
  Reopen(options);

  ASSERT_OK(Put("key", "val"));

  bool atomic_flush_called = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
      [&](void* /*arg*/) { atomic_flush_called = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  LiveFilesStorageInfoOptions live_opts;
  live_opts.atomic_flush = true;
  std::vector<LiveFileStorageInfo> files;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(live_opts, &files));
  ASSERT_TRUE(atomic_flush_called);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ("val", Get("key"));
}

TEST_F(CheckpointTest, CheckpointAtomicFlushOverrideWithDBAtomicFlush) {
  // When DBOptions::atomic_flush is already true, the override should be
  // harmless (atomic flush already used).
  Options options = CurrentOptions();
  options.atomic_flush = true;
  CreateAndReopenWithCF({"one", "two"}, options);

  ASSERT_OK(Put(0, "key0", "val0"));
  ASSERT_OK(Put(1, "key1", "val1"));
  ASSERT_OK(Put(2, "key2", "val2"));

  bool atomic_flush_called = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
      [&](void* /*arg*/) { atomic_flush_called = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  LiveFilesStorageInfoOptions live_opts;
  live_opts.atomic_flush = true;
  std::vector<LiveFileStorageInfo> files;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(live_opts, &files));
  ASSERT_TRUE(atomic_flush_called);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ("val0", Get(0, "key0"));
  ASSERT_EQ("val1", Get(1, "key1"));
  ASSERT_EQ("val2", Get(2, "key2"));
}

TEST_F(CheckpointTest, MixedAtomicAndNonAtomicFlushInQueue) {
  // Test that both atomic and non-atomic flush requests can coexist in the
  // flush queue. First enqueue a non-atomic flush, then an atomic flush.
  Options options = CurrentOptions();
  options.atomic_flush = false;
  // Use multiple CFs so atomic flush is meaningful.
  CreateAndReopenWithCF({"one", "two"}, options);

  // Pause background flushes so requests queue up.
  ASSERT_OK(dbfull()->PauseBackgroundWork());

  // Write data to all CFs.
  ASSERT_OK(Put(0, "k0", "v0"));
  ASSERT_OK(Put(1, "k1", "v1"));
  ASSERT_OK(Put(2, "k2", "v2"));

  // Enqueue a non-atomic flush (default).
  FlushOptions non_atomic_fo;
  non_atomic_fo.wait = false;
  non_atomic_fo.allow_write_stall = true;
  ASSERT_OK(db_->Flush(non_atomic_fo, handles_[0]));

  // Write more data so there's something new to flush atomically.
  ASSERT_OK(Put(0, "k0_2", "v0_2"));
  ASSERT_OK(Put(1, "k1_2", "v1_2"));
  ASSERT_OK(Put(2, "k2_2", "v2_2"));

  // Enqueue an atomic flush via force_atomic_flush.
  FlushOptions atomic_fo;
  atomic_fo.wait = false;
  atomic_fo.allow_write_stall = true;
  atomic_fo.force_atomic_flush = true;
  ASSERT_OK(db_->Flush(atomic_fo, handles_));

  // Resume and let both flushes execute.
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Verify all data is readable.
  ASSERT_EQ("v0_2", Get(0, "k0_2"));
  ASSERT_EQ("v1_2", Get(1, "k1_2"));
  ASSERT_EQ("v2_2", Get(2, "k2_2"));
}

TEST_F(CheckpointTest, MixedAtomicThenNonAtomicFlushInQueue) {
  // Reverse order: first enqueue an atomic flush, then a non-atomic flush.
  Options options = CurrentOptions();
  options.atomic_flush = false;
  CreateAndReopenWithCF({"one", "two"}, options);

  // Pause background flushes so requests queue up.
  ASSERT_OK(dbfull()->PauseBackgroundWork());

  // Write data to all CFs.
  ASSERT_OK(Put(0, "k0", "v0"));
  ASSERT_OK(Put(1, "k1", "v1"));
  ASSERT_OK(Put(2, "k2", "v2"));

  // Enqueue an atomic flush first.
  FlushOptions atomic_fo;
  atomic_fo.wait = false;
  atomic_fo.allow_write_stall = true;
  atomic_fo.force_atomic_flush = true;
  ASSERT_OK(db_->Flush(atomic_fo, handles_));

  // Write more data.
  ASSERT_OK(Put(0, "k0_2", "v0_2"));
  ASSERT_OK(Put(1, "k1_2", "v1_2"));

  // Enqueue a non-atomic flush on a single CF.
  FlushOptions non_atomic_fo;
  non_atomic_fo.wait = false;
  non_atomic_fo.allow_write_stall = true;
  ASSERT_OK(db_->Flush(non_atomic_fo, handles_[1]));

  // Resume and let both flushes execute.
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Verify all data is readable.
  ASSERT_EQ("v0", Get(0, "k0"));
  ASSERT_EQ("v0_2", Get(0, "k0_2"));
  ASSERT_EQ("v1_2", Get(1, "k1_2"));
  ASSERT_EQ("v2", Get(2, "k2"));
}
TEST_F(CheckpointTest, BackupWithAtomicFlushSkipsWAL) {
  // Test that BackupEngine with atomic_flush=true and backup_log_files=false
  // creates a valid, restorable backup for a multi-CF database.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  // Write data to all column families (default, cf1, cf2)
  ASSERT_OK(Put(0, "key0", "val0"));
  ASSERT_OK(Put(1, "key1", "val1"));
  ASSERT_OK(Put(2, "key2", "val2"));

  // Set up backup engine with backup_log_files=false
  std::string backup_dir = test::PerThreadDBPath(env_, "backup_atomic_flush");
  std::string restore_dir = test::PerThreadDBPath(env_, "restore_atomic_flush");
  ASSERT_OK(DestroyDB(restore_dir, options));
  test::DeleteDir(env_, backup_dir);

  BackupEngineOptions backup_options(backup_dir);
  backup_options.backup_log_files = false;
  backup_options.destroy_old_data = true;

  BackupEngine* backup_engine_ptr;
  ASSERT_OK(BackupEngine::Open(env_, backup_options, &backup_engine_ptr));
  std::unique_ptr<BackupEngine> backup_engine(backup_engine_ptr);

  // Verify that the atomic flush sync point is triggered
  bool atomic_flush_triggered = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
      [&](void* /*arg*/) { atomic_flush_triggered = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Create backup with atomic_flush=true, flush_before_backup=true
  CreateBackupOptions create_options;
  create_options.flush_before_backup = true;
  create_options.atomic_flush = true;
  ASSERT_OK(backup_engine->CreateNewBackup(create_options, db_.get()));

  ASSERT_TRUE(atomic_flush_triggered);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify no WAL files in the backup
  BackupInfo backup_info;
  ASSERT_OK(backup_engine->GetLatestBackupInfo(&backup_info,
                                               /*include_file_details=*/true));
  for (const auto& file : backup_info.file_details) {
    // WAL files have names like 000001.log
    ASSERT_TRUE(file.relative_filename.find(".log") == std::string::npos)
        << "Found WAL file in backup: " << file.relative_filename;
  }

  // Close the original DB and restore from backup
  Close();

  ASSERT_OK(backup_engine->RestoreDBFromLatestBackup(restore_dir, restore_dir));

  // Open the restored DB and verify data
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions(options));
  cf_descs.emplace_back("cf1", ColumnFamilyOptions(options));
  cf_descs.emplace_back("cf2", ColumnFamilyOptions(options));

  std::unique_ptr<DB> restored_db;
  std::vector<ColumnFamilyHandle*> restored_handles;
  ASSERT_OK(DB::Open(DBOptions(options), restore_dir, cf_descs,
                     &restored_handles, &restored_db));

  ReadOptions ropts;
  std::string value;
  ASSERT_OK(restored_db->Get(ropts, restored_handles[0], "key0", &value));
  ASSERT_EQ("val0", value);
  ASSERT_OK(restored_db->Get(ropts, restored_handles[1], "key1", &value));
  ASSERT_EQ("val1", value);
  ASSERT_OK(restored_db->Get(ropts, restored_handles[2], "key2", &value));
  ASSERT_EQ("val2", value);

  for (auto* h : restored_handles) {
    delete h;
  }
  restored_db.reset();
  ASSERT_OK(DestroyDB(restore_dir, options));
  test::DeleteDir(env_, backup_dir);
}

TEST_F(CheckpointTest, BackupWithoutFlushRejectsBlobDirectWrite) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.min_blob_size = 32;
  Reopen(options);

  ASSERT_OK(Put("foo", std::string(128, 'x')));

  std::string backup_dir =
      test::PerThreadDBPath(env_, "backup_blob_direct_write_requires_flush");
  test::DeleteDir(env_, backup_dir);

  BackupEngineOptions backup_options(backup_dir);
  backup_options.destroy_old_data = true;

  BackupEngine* backup_engine_ptr = nullptr;
  ASSERT_OK(BackupEngine::Open(env_, backup_options, &backup_engine_ptr));
  std::unique_ptr<BackupEngine> backup_engine(backup_engine_ptr);

  CreateBackupOptions create_options;
  create_options.flush_before_backup = false;
  Status s = backup_engine->CreateNewBackup(create_options, db_.get());
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_NE(s.ToString().find("Blob direct write"), std::string::npos)
      << s.ToString();

  test::DeleteDir(env_, backup_dir);
}

namespace {
// FileSystem wrapper that fails all hard-links, forcing the copy path.
class NoLinkFileSystem : public FileSystemWrapper {
 public:
  explicit NoLinkFileSystem(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "NoLinkFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus LinkFile(const std::string& /*src*/, const std::string& /*dst*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::NotSupported("Hard links not supported");
  }
};

}  // namespace

TEST_F(CheckpointTest, CheckpointEngineParallelLink) {
  // Default FileSystem supports hard links, so files are linked across threads.
  constexpr int kNumFiles = 8;
  constexpr int kKeysPerFile = 50;
  for (int f = 0; f < kNumFiles; ++f) {
    for (int k = 0; k < kKeysPerFile; ++k) {
      int id = f * kKeysPerFile + k;
      ASSERT_OK(Put("key" + std::to_string(id), "value" + std::to_string(id)));
    }
    ASSERT_OK(Flush());
  }

  CheckpointEngineOptions engine_options;
  engine_options.max_background_operations = 4;
  std::unique_ptr<CheckpointEngine> engine;
  ASSERT_OK(CheckpointEngine::Open(engine_options, &engine));
  if (engine == nullptr) {
    FAIL() << "CheckpointEngine::Open returned a null engine";
  }
  ASSERT_OK(engine->CreateCheckpoint(db_.get(), snapshot_name_));

  Options options = CurrentOptions();
  options.create_if_missing = false;
  std::unique_ptr<DB> checkpoint_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &checkpoint_db));
  if (checkpoint_db == nullptr) {
    FAIL() << "DB::Open returned a null db";
  }
  for (int id = 0; id < kNumFiles * kKeysPerFile; ++id) {
    std::string value;
    ASSERT_OK(
        checkpoint_db->Get(ReadOptions(), "key" + std::to_string(id), &value));
    ASSERT_EQ("value" + std::to_string(id), value);
  }
}

TEST_F(CheckpointTest, CheckpointEngineParallelCopy) {
  // Links fail NotSupported and fall back to parallel copies.
  auto no_link_fs = std::make_shared<NoLinkFileSystem>(FileSystem::Default());
  std::unique_ptr<Env> no_link_env(NewCompositeEnv(no_link_fs));

  Options options = CurrentOptions();
  options.env = no_link_env.get();
  Reopen(options);

  constexpr int kNumFiles = 8;
  constexpr int kKeysPerFile = 50;
  for (int f = 0; f < kNumFiles; ++f) {
    for (int k = 0; k < kKeysPerFile; ++k) {
      int id = f * kKeysPerFile + k;
      ASSERT_OK(Put("key" + std::to_string(id), "value" + std::to_string(id)));
    }
    ASSERT_OK(Flush());
  }

  CheckpointEngineOptions engine_options;
  engine_options.max_background_operations = 4;
  std::unique_ptr<CheckpointEngine> engine;
  ASSERT_OK(CheckpointEngine::Open(engine_options, &engine));
  if (engine == nullptr) {
    FAIL() << "CheckpointEngine::Open returned a null engine";
  }
  ASSERT_OK(engine->CreateCheckpoint(db_.get(), snapshot_name_));

  Close();  // before tearing down the env the DB was opened with

  options.env = env_;
  options.create_if_missing = false;
  std::unique_ptr<DB> checkpoint_db;
  ASSERT_OK(DB::Open(options, snapshot_name_, &checkpoint_db));
  if (checkpoint_db == nullptr) {
    FAIL() << "DB::Open returned a null db";
  }
  for (int id = 0; id < kNumFiles * kKeysPerFile; ++id) {
    std::string value;
    ASSERT_OK(
        checkpoint_db->Get(ReadOptions(), "key" + std::to_string(id), &value));
    ASSERT_EQ("value" + std::to_string(id), value);
  }
}

TEST_F(CheckpointTest, CheckpointEngineParallelCopyRecordsCheckpointBytes) {
  // Force copies (NoLinkFileSystem) so the parallel path actually moves bytes;
  // hard links move no data and record nothing.
  auto no_link_fs = std::make_shared<NoLinkFileSystem>(FileSystem::Default());
  std::unique_ptr<Env> no_link_env(NewCompositeEnv(no_link_fs));

  Options options = CurrentOptions();
  options.env = no_link_env.get();
  options.statistics = CreateDBStatistics();
  Reopen(options);

  constexpr int kNumFiles = 4;
  constexpr int kKeysPerFile = 50;
  for (int f = 0; f < kNumFiles; ++f) {
    for (int k = 0; k < kKeysPerFile; ++k) {
      int id = f * kKeysPerFile + k;
      ASSERT_OK(Put("key" + std::to_string(id), "value" + std::to_string(id)));
    }
    ASSERT_OK(Flush());
  }

  CheckpointEngineOptions engine_options;
  engine_options.max_background_operations = 4;
  std::unique_ptr<CheckpointEngine> engine;
  ASSERT_OK(CheckpointEngine::Open(engine_options, &engine));
  if (engine == nullptr) {
    FAIL() << "CheckpointEngine::Open returned a null engine";
  }
  ASSERT_OK(engine->CreateCheckpoint(db_.get(), snapshot_name_));

  // Copied bytes are attributed to the checkpoint tickers, and never to the
  // backup tickers -- this verifies both the ticker wiring and that a live
  // Statistics* reaches the CopyEngine WorkItems.
  EXPECT_GT(options.statistics->getTickerCount(CHECKPOINT_READ_BYTES), 0U);
  EXPECT_GT(options.statistics->getTickerCount(CHECKPOINT_WRITE_BYTES), 0U);
  EXPECT_EQ(options.statistics->getTickerCount(BACKUP_READ_BYTES), 0U);
  EXPECT_EQ(options.statistics->getTickerCount(BACKUP_WRITE_BYTES), 0U);

  Close();  // before tearing down the env the DB was opened with
}

TEST_F(CheckpointTest, CheckpointEngineSubsetOfCF) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);

  CheckpointEngineOptions engine_options;
  engine_options.max_background_operations = 4;
  std::unique_ptr<CheckpointEngine> engine;
  ASSERT_OK(CheckpointEngine::Open(engine_options, &engine));
  if (engine == nullptr) {
    FAIL() << "CheckpointEngine::Open returned a null engine";
  }

  // Checkpoint only {default, two} through the parallel hard-link path.
  CreateCheckpointOptions create_options;
  create_options.column_families = {handles_[0], handles_[2]};
  ASSERT_OK(engine->CreateCheckpoint(db_.get(), snapshot_name_,
                                     /*sequence_number_ptr=*/nullptr,
                                     create_options));

  // The source DB must be untouched by the checkpoint MANIFEST rewrite.
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "one", "two", "three"},
                           options);
  VerifyFourCfsIntact(db_.get(), handles_);

  VerifyDefaultAndTwoSubsetCheckpoint(env_, snapshot_name_, options);
}

TEST_F(CheckpointTest, CheckpointEngineSubsetOfCFParallelCopy) {
  // Links fail NotSupported, so the subset's files go through the parallel
  // copy path while the MANIFEST drop records are appended afterwards.
  auto no_link_fs = std::make_shared<NoLinkFileSystem>(FileSystem::Default());
  std::unique_ptr<Env> no_link_env(NewCompositeEnv(no_link_fs));

  Options options = CurrentOptions();
  options.env = no_link_env.get();
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);

  CheckpointEngineOptions engine_options;
  engine_options.max_background_operations = 4;
  std::unique_ptr<CheckpointEngine> engine;
  ASSERT_OK(CheckpointEngine::Open(engine_options, &engine));
  if (engine == nullptr) {
    FAIL() << "CheckpointEngine::Open returned a null engine";
  }

  CreateCheckpointOptions create_options;
  create_options.column_families = {handles_[2]};
  ASSERT_OK(engine->CreateCheckpoint(db_.get(), snapshot_name_,
                                     /*sequence_number_ptr=*/nullptr,
                                     create_options));

  Close();  // before tearing down the env the DB was opened with

  options.env = env_;
  VerifyDefaultAndTwoSubsetCheckpoint(env_, snapshot_name_, options);
}

TEST_F(CheckpointTest, CheckpointEngineSubsetEmptyListEqualsAll) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two", "three"}, options);
  PopulateFourCfsForSubsetCheckpoint(db_.get(), handles_);

  CheckpointEngineOptions engine_options;
  engine_options.max_background_operations = 4;
  std::unique_ptr<CheckpointEngine> engine;
  ASSERT_OK(CheckpointEngine::Open(engine_options, &engine));
  if (engine == nullptr) {
    FAIL() << "CheckpointEngine::Open returned a null engine";
  }

  // Default-constructed options leave column_families empty, which must remain
  // equivalent to a whole-DB checkpoint.
  ASSERT_OK(engine->CreateCheckpoint(db_.get(), snapshot_name_));

  ASSERT_EQ(4, CountSstFiles(env_, snapshot_name_));

  options.create_if_missing = false;
  std::vector<ColumnFamilyDescriptor> cfds;
  for (const std::string& cf : std::vector<std::string>{
           kDefaultColumnFamilyName, "one", "two", "three"}) {
    cfds.emplace_back(cf, options);
  }
  std::vector<ColumnFamilyHandle*> cph;
  std::unique_ptr<DB> snapshotDB;
  ASSERT_OK(DB::Open(options, snapshot_name_, cfds, &cph, &snapshotDB));
  std::string result;
  ASSERT_OK(snapshotDB->Get(ReadOptions(), cph[1], "one_key", &result));
  ASSERT_EQ("one_val", result);
  ASSERT_OK(snapshotDB->Get(ReadOptions(), cph[3], "three_key", &result));
  ASSERT_EQ("three_val", result);
  for (auto h : cph) {
    delete h;
  }
}

TEST_F(CheckpointTest, CheckpointEngineSubsetInvalidHandlesRejected) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one"}, options);
  ASSERT_OK(Put(1, "a", "1"));

  // Second, unrelated DB whose CF id collides with "one" from this DB (both
  // are the first non-default CF -> id 1).
  const std::string other_dbname = test::PerThreadDBPath("other_db_for_engine");
  ASSERT_OK(DestroyDB(other_dbname, options));
  Options open_opts = options;
  open_opts.create_if_missing = true;
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(open_opts, other_dbname, &other_db));
  ColumnFamilyHandle* other_cf = nullptr;
  ASSERT_OK(
      other_db->CreateColumnFamily(ColumnFamilyOptions(), "one", &other_cf));

  CheckpointEngineOptions engine_options;
  std::unique_ptr<CheckpointEngine> engine;
  ASSERT_OK(CheckpointEngine::Open(engine_options, &engine));
  if (engine == nullptr) {
    FAIL() << "CheckpointEngine::Open returned a null engine";
  }

  // Validation must run before any staging directory is created, so a rejected
  // request leaves nothing behind and can be retried at the same path.
  auto expect_rejected = [&](const std::vector<ColumnFamilyHandle*>& cfs) {
    CreateCheckpointOptions create_options;
    create_options.column_families = cfs;
    IOStatus io_s = engine->CreateCheckpoint(db_.get(), snapshot_name_,
                                             /*sequence_number_ptr=*/nullptr,
                                             create_options);
    ASSERT_TRUE(io_s.IsInvalidArgument()) << io_s.ToString();
    ASSERT_TRUE(env_->FileExists(snapshot_name_).IsNotFound());
  };
  expect_rejected({handles_[1], nullptr});
  expect_rejected({other_cf});

  ColumnFamilyHandle* dropped_handle = handles_[1];
  ASSERT_OK(db_->DropColumnFamily(dropped_handle));
  expect_rejected({dropped_handle});

  ASSERT_OK(other_db->DestroyColumnFamilyHandle(other_cf));
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
