//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "file/sst_file_manager_impl.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/table.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class DBSSTTest : public DBTestBase {
 public:
  DBSSTTest(const std::string& test_name = "db_sst_test")
      : DBTestBase(test_name, /*env_do_fsync=*/true) {}
};

// A class which remembers the name of each flushed file.
class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() = default;
  ~FlushedFileCollector() override = default;

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (const auto& fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }
  void ClearFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.clear();
  }

 private:
  std::vector<std::string> flushed_files_;
  std::mutex mutex_;
};

TEST_F(DBSSTTest, DontDeletePendingOutputs) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  // Every time we write to a table file, call FOF/POF with full DB scan. This
  // will make sure our pending_outputs_ protection work correctly
  std::function<void()> purge_obsolete_files_function = [&]() {
    JobContext job_context(0);
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&job_context, true /*force*/);
    dbfull()->TEST_UnlockMutex();
    dbfull()->PurgeObsoleteFiles(job_context);
    job_context.Clean();
  };

  env_->table_write_callback_ = &purge_obsolete_files_function;

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Put("a", "begin"));
    ASSERT_OK(Put("z", "end"));
    ASSERT_OK(Flush());
  }

  // If pending output guard does not work correctly, PurgeObsoleteFiles() will
  // delete the file that Compaction is trying to create, causing this: error
  // db/db_test.cc:975: IO error:
  // /tmp/rocksdbtest-1552237650/db_test/000009.sst: No such file or directory
  Compact("a", "b");
}

// 1 Create some SST files by inserting K-V pairs into DB
// 2 Close DB and change suffix from ".sst" to ".ldb" for every other SST file
// 3 Open DB and check if all key can be read
TEST_F(DBSSTTest, SSTsWithLdbSuffixHandling) {
  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.num_levels = 4;
  DestroyAndReopen(options);

  Random rnd(301);
  int key_id = 0;
  for (int i = 0; i < 10; ++i) {
    GenerateNewFile(&rnd, &key_id, false);
  }
  ASSERT_OK(Flush());
  Close();
  int const num_files = GetSstFileCount(dbname_);
  ASSERT_GT(num_files, 0);

  Reopen(options);
  std::vector<std::string> values;
  values.reserve(key_id);
  for (int k = 0; k < key_id; ++k) {
    values.push_back(Get(Key(k)));
  }
  Close();

  std::vector<std::string> filenames;
  GetSstFiles(env_, dbname_, &filenames);
  int num_ldb_files = 0;
  for (size_t i = 0; i < filenames.size(); ++i) {
    if (i & 1) {
      continue;
    }
    std::string const rdb_name = dbname_ + "/" + filenames[i];
    std::string const ldb_name = Rocks2LevelTableFileName(rdb_name);
    ASSERT_TRUE(env_->RenameFile(rdb_name, ldb_name).ok());
    ++num_ldb_files;
  }
  ASSERT_GT(num_ldb_files, 0);
  ASSERT_EQ(num_files, GetSstFileCount(dbname_));

  Reopen(options);
  for (int k = 0; k < key_id; ++k) {
    ASSERT_EQ(values[k], Get(Key(k)));
  }
  Destroy(options);
}

TEST_F(DBSSTTest, DontDeleteMovedFile) {
  // This test triggers move compaction and verifies that the file is not
  // deleted when it's part of move compaction
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  DestroyAndReopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), rnd.RandomString(10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  // If the moved file is actually deleted (the move-safeguard in
  // ~Version::Version() is not there), we get this failure:
  // Corruption: Can't access /000009.sst
  Reopen(options);
}

// This reproduces a bug where we don't delete a file because when it was
// supposed to be deleted, it was blocked by pending_outputs
// Consider:
// 1. current file_number is 13
// 2. compaction (1) starts, blocks deletion of all files starting with 13
// (pending outputs)
// 3. file 13 is created by compaction (2)
// 4. file 13 is consumed by compaction (3) and file 15 was created. Since file
// 13 has no references, it is put into VersionSet::obsolete_files_
// 5. FindObsoleteFiles() gets file 13 from VersionSet::obsolete_files_. File 13
// is deleted from obsolete_files_ set.
// 6. PurgeObsoleteFiles() tries to delete file 13, but this file is blocked by
// pending outputs since compaction (1) is still running. It is not deleted and
// it is not present in obsolete_files_ anymore. Therefore, we never delete it.
TEST_F(DBSSTTest, DeleteObsoleteFilesPendingOutputs) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 2 * 1024 * 1024;     // 2 MB
  options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
  options.level0_file_num_compaction_trigger =
      2;  // trigger compaction when we have 2 files
  options.max_background_flushes = 2;
  options.max_background_compactions = 2;

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

  Reopen(options);

  Random rnd(301);
  // Create two 1MB sst files
  for (int i = 0; i < 2; ++i) {
    // Create 1MB sst file
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(i * 50 + j), rnd.RandomString(10 * 1024)));
    }
    ASSERT_OK(Flush());
  }
  // this should execute both L0->L1 and L1->(move)->L2 compactions
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  test::SleepingBackgroundTask blocking_thread;
  port::Mutex mutex_;
  bool already_blocked(false);

  // block the flush
  std::function<void()> block_first_time = [&]() {
    bool blocking = false;
    {
      MutexLock l(&mutex_);
      if (!already_blocked) {
        blocking = true;
        already_blocked = true;
      }
    }
    if (blocking) {
      blocking_thread.DoSleep();
    }
  };
  env_->table_write_callback_ = &block_first_time;
  // Insert 2.5MB data, which should trigger a flush because we exceed
  // write_buffer_size. The flush will be blocked with block_first_time
  // pending_file is protecting all the files created after
  for (int j = 0; j < 256; ++j) {
    ASSERT_OK(Put(Key(j), rnd.RandomString(10 * 1024)));
  }
  blocking_thread.WaitUntilSleeping();

  ASSERT_OK(dbfull()->TEST_CompactRange(2, nullptr, nullptr));

  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  auto file_on_L2 = metadata[0].name;
  listener->SetExpectedFileName(dbname_ + file_on_L2);

  ASSERT_OK(dbfull()->TEST_CompactRange(3, nullptr, nullptr, nullptr,
                                        true /* disallow trivial move */));
  ASSERT_EQ("0,0,0,0,1", FilesPerLevel(0));

  // finish the flush!
  blocking_thread.WakeUp();
  blocking_thread.WaitUntilDone();
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  // File just flushed is too big for L0 and L1 so gets moved to L2.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,1,0,1", FilesPerLevel(0));

  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 2U);

  // This file should have been deleted during last compaction
  ASSERT_EQ(Status::NotFound(), env_->FileExists(dbname_ + file_on_L2));
  listener->VerifyMatchedCount(1);
}

// Test that producing an empty .sst file does not write it out to
// disk, and that the DeleteFile() env method is not called for
// removing the non-existing file later.
TEST_F(DBSSTTest, DeleteFileNotCalledForNotCreatedSSTFile) {
  Options options = CurrentOptions();
  options.env = env_;

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

  Reopen(options);

  // Flush the empty database.
  ASSERT_OK(Flush());
  ASSERT_EQ("", FilesPerLevel(0));

  // We expect no .sst files.
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 0U);

  // We expect no file deletions.
  listener->VerifyMatchedCount(0);
}

// Test that producing a non-empty .sst file does write it out to
// disk, and that the DeleteFile() env method is not called for removing
// the file later.
TEST_F(DBSSTTest, DeleteFileNotCalledForCreatedSSTFile) {
  Options options = CurrentOptions();
  options.env = env_;

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

  Reopen(options);

  ASSERT_OK(Put("pika", "choo"));

  // Flush the non-empty database.
  ASSERT_OK(Flush());
  ASSERT_EQ("1", FilesPerLevel(0));

  // We expect 1 .sst files.
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);

  // We expect no file deletions.
  listener->VerifyMatchedCount(0);
}

TEST_F(DBSSTTest, DBWithSstFileManager) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  int files_added = 0;
  int files_deleted = 0;
  int files_moved = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnAddFile", [&](void* /*arg*/) { files_added++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnDeleteFile",
      [&](void* /*arg*/) { files_deleted++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnMoveFile", [&](void* /*arg*/) { files_moved++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 25; i++) {
    GenerateNewRandomFile(&rnd);
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // Verify that we are tracking all sst files in dbname_
    std::unordered_map<std::string, uint64_t> files_in_db;
    ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db));
    ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  std::unordered_map<std::string, uint64_t> files_in_db;
  ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db));
  // Verify that we are tracking all sst files in dbname_
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  // Verify the total files size
  uint64_t total_files_size = 0;
  for (auto& file_to_size : files_in_db) {
    total_files_size += file_to_size.second;
  }
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);
  // We flushed at least 25 files
  ASSERT_GE(files_added, 25);
  // Compaction must have deleted some files
  ASSERT_GT(files_deleted, 0);
  // No files were moved
  ASSERT_EQ(files_moved, 0);

  Close();
  ASSERT_EQ(sfm->GetTrackedFiles().size(), 0) << "sfm should be empty";
  ASSERT_EQ(sfm->GetTotalSize(), 0) << "sfm should be empty";
  Reopen(options);
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  // Verify that we track all the files again after the DB is closed and opened
  Close();
  ASSERT_EQ(sfm->GetTrackedFiles().size(), 0) << "sfm should be empty";
  ASSERT_EQ(sfm->GetTotalSize(), 0) << "sfm should be empty";
  sst_file_manager.reset(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Reopen(options);
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, DBWithSstFileManagerForBlobFiles) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  int files_added = 0;
  int files_deleted = 0;
  int files_moved = 0;
  int files_scheduled_to_delete = 0;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnAddFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          files_added++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnDeleteFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          files_deleted++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          ++files_scheduled_to_delete;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnMoveFile", [&](void* /*arg*/) { files_moved++; });

  int64_t untracked_files = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnUntrackFile",
      [&](void* /*arg*/) { ++untracked_files; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.enable_blob_files = true;
  options.blob_file_size = 32;  // create one blob per file
  DestroyAndReopen(options);
  Random rnd(301);

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("Key_" + std::to_string(i), "Value_" + std::to_string(i)));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    // Verify that we are tracking all sst and blob files in dbname_
    std::unordered_map<std::string, uint64_t> files_in_db;
    ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db));
    ASSERT_OK(GetAllDataFiles(kBlobFile, &files_in_db));
    ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  }

  std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(files_added, blob_files.size());
  // No blob file is obsoleted.
  ASSERT_EQ(files_deleted, 0);
  ASSERT_EQ(files_scheduled_to_delete, 0);
  // No files were moved.
  ASSERT_EQ(files_moved, 0);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  std::unordered_map<std::string, uint64_t> files_in_db;
  ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db));
  ASSERT_OK(GetAllDataFiles(kBlobFile, &files_in_db));

  // Verify that we are tracking all sst and blob files in dbname_
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  // Verify the total files size
  uint64_t total_files_size = 0;
  for (auto& file_to_size : files_in_db) {
    total_files_size += file_to_size.second;
  }
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);
  Close();
  ASSERT_EQ(untracked_files, files_in_db.size());
  untracked_files = 0;
  ASSERT_EQ(sfm->GetTrackedFiles().size(), 0) << "sfm should be empty";
  ASSERT_EQ(sfm->GetTotalSize(), 0) << "sfm should be empty";

  Reopen(options);
  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  // Verify that we track all the files again after the DB is closed and opened.
  Close();
  ASSERT_EQ(untracked_files, files_in_db.size());
  untracked_files = 0;
  ASSERT_EQ(sfm->GetTrackedFiles().size(), 0) << "sfm should be empty";
  ASSERT_EQ(sfm->GetTotalSize(), 0) << "sfm should be empty";

  sst_file_manager.reset(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Reopen(options);

  ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  // Destroy DB and it will remove all the blob files from sst file manager and
  // blob files deletion will go through ScheduleFileDeletion.
  ASSERT_EQ(files_deleted, 0);
  ASSERT_EQ(files_scheduled_to_delete, 0);
  Close();
  ASSERT_EQ(untracked_files, files_in_db.size());
  untracked_files = 0;
  ASSERT_EQ(sfm->GetTrackedFiles().size(), 0) << "sfm should be empty";
  ASSERT_EQ(sfm->GetTotalSize(), 0) << "sfm should be empty";
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleUnaccountedFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          ++files_scheduled_to_delete;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::OnDeleteFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          files_deleted++;
        }
      });
  ASSERT_OK(DestroyDB(dbname_, options));
  ASSERT_EQ(files_deleted, blob_files.size());
  ASSERT_EQ(files_scheduled_to_delete, blob_files.size());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBSSTTest, DBWithSstFileManagerForBlobFilesWithGC) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());
  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.enable_blob_files = true;
  options.blob_file_size = 32;  // create one blob per file
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.5;

  int files_added = 0;
  int files_deleted = 0;
  int files_moved = 0;
  int files_scheduled_to_delete = 0;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnAddFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          files_added++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnDeleteFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          files_deleted++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (file_path->find(".blob") != std::string::npos) {
          ++files_scheduled_to_delete;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnMoveFile", [&](void* /*arg*/) { files_moved++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);
  Random rnd(301);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  constexpr char fourth_key[] = "fourth_key";
  constexpr char fourth_value[] = "fourth_value";
  constexpr char fifth_key[] = "fifth_key";
  constexpr char fifth_value[] = "fifth_value";

  ASSERT_OK(Put(third_key, third_value));
  ASSERT_OK(Put(fourth_key, fourth_value));
  ASSERT_OK(Put(fifth_key, fifth_value));
  ASSERT_OK(Flush());

  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();

  ASSERT_EQ(original_blob_files.size(), 5);
  ASSERT_EQ(files_added, 5);
  ASSERT_EQ(files_deleted, 0);
  ASSERT_EQ(files_scheduled_to_delete, 0);
  ASSERT_EQ(files_moved, 0);
  {
    // Verify that we are tracking all sst and blob files in dbname_
    std::unordered_map<std::string, uint64_t> files_in_db;
    ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db));
    ASSERT_OK(GetAllDataFiles(kBlobFile, &files_in_db));
    ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  }

  const size_t cutoff_index = static_cast<size_t>(
      options.blob_garbage_collection_age_cutoff * original_blob_files.size());

  size_t expected_number_of_files = original_blob_files.size();
  // Note: turning off enable_blob_files before the compaction results in
  // garbage collected values getting inlined.
  ASSERT_OK(db_->SetOptions({{"enable_blob_files", "false"}}));
  expected_number_of_files -= cutoff_index;
  files_added = 0;

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  sfm->WaitForEmptyTrash();

  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);
  ASSERT_EQ(Get(fourth_key), fourth_value);
  ASSERT_EQ(Get(fifth_key), fifth_value);

  const std::vector<uint64_t> new_blob_files = GetBlobFileNumbers();

  ASSERT_EQ(new_blob_files.size(), expected_number_of_files);
  // No new file is added.
  ASSERT_EQ(files_added, 0);
  ASSERT_EQ(files_deleted, cutoff_index);
  ASSERT_EQ(files_scheduled_to_delete, cutoff_index);
  ASSERT_EQ(files_moved, 0);

  // Original blob files below the cutoff should be gone, original blob files at
  // or above the cutoff should be still there
  for (size_t i = cutoff_index; i < original_blob_files.size(); ++i) {
    ASSERT_EQ(new_blob_files[i - cutoff_index], original_blob_files[i]);
  }

  {
    // Verify that we are tracking all sst and blob files in dbname_
    std::unordered_map<std::string, uint64_t> files_in_db;
    ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db));
    ASSERT_OK(GetAllDataFiles(kBlobFile, &files_in_db));
    ASSERT_EQ(sfm->GetTrackedFiles(), files_in_db);
  }

  Close();
  ASSERT_EQ(sfm->GetTrackedFiles().size(), 0) << "sfm should be empty";
  ASSERT_EQ(sfm->GetTotalSize(), 0) << "sfm should be empty";

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleUnaccountedFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          ++files_scheduled_to_delete;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::OnDeleteFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          files_deleted++;
        }
      });
  ASSERT_OK(DestroyDB(dbname_, options));
  sfm->WaitForEmptyTrash();
  ASSERT_EQ(files_deleted, 5);
  ASSERT_EQ(files_scheduled_to_delete, 5);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBSSTTest, SstFileManagerTracksMultipleCompactionBlobOutputs) {
  constexpr uint64_t kBlobFileSize = 1024;
  constexpr size_t kValueSize = 600;

  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto* sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.blob_file_size = kBlobFileSize;
  options.blob_compression_type = kNoCompression;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.0;
  DestroyAndReopen(options);

  const std::string value(kValueSize, 'a');
  ASSERT_OK(Put("key1", value));
  ASSERT_OK(Put("key3", value));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", value));
  ASSERT_OK(Put("key4", value));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  sfm->WaitForEmptyTrash();
  ASSERT_EQ(2U, GetBlobFileNumbers().size());

  std::unordered_map<std::string, uint64_t> files_in_db;
  ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db));
  ASSERT_OK(GetAllDataFiles(kBlobFile, &files_in_db));
  ASSERT_EQ(files_in_db, sfm->GetTrackedFiles());
}

class DBSSTTestRateLimit : public DBSSTTest,
                           public ::testing::WithParamInterface<bool> {
 public:
  DBSSTTestRateLimit() : DBSSTTest() {}
  ~DBSSTTestRateLimit() override = default;
};

TEST_P(DBSSTTestRateLimit, RateLimitedDelete) {
  Destroy(last_options_);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBSSTTest::RateLimitedDelete:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });

  std::vector<uint64_t> penalties;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<uint64_t*>(arg))); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "InstrumentedCondVar::TimedWaitInternal", [&](void* arg) {
        // Turn timed wait into a simulated sleep
        uint64_t* abs_time_us = static_cast<uint64_t*>(arg);
        uint64_t cur_time = env_->NowMicros();
        if (*abs_time_us > cur_time) {
          env_->MockSleepForMicroseconds(*abs_time_us - cur_time);
        }

        // Plus an additional short, random amount
        env_->MockSleepForMicroseconds(Random::GetTLSInstance()->Uniform(10));

        // Set wait until time to before (actual) current time to force not
        // to sleep
        *abs_time_us = Env::Default()->NowMicros();
      });

  // Disable PeriodicTaskScheduler as it also has TimedWait, which could update
  // the simulated sleep time
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::StartPeriodicTaskScheduler:DisableScheduler", [&](void* arg) {
        bool* disable_scheduler = static_cast<bool*>(arg);
        *disable_scheduler = true;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool different_wal_dir = GetParam();
  Options options = CurrentOptions();
  SetTimeElapseOnlySleepOnReopen(&options);
  options.disable_auto_compactions = true;
  options.env = env_;
  options.statistics = CreateDBStatistics();
  if (different_wal_dir) {
    options.wal_dir = alternative_wal_dir_;
  }

  int64_t rate_bytes_per_sec = 1024 * 10;  // 10 Kbs / Sec
  Status s;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 0, false, &s, 0));
  ASSERT_OK(s);
  options.sst_file_manager->SetDeleteRateBytesPerSecond(rate_bytes_per_sec);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  sfm->delete_scheduler()->SetMaxTrashDBRatio(1.1);

  WriteOptions wo;
  if (!different_wal_dir) {
    wo.disableWAL = true;
  }
  Reopen(options);
  // Create 4 files in L0
  for (char v = 'a'; v <= 'd'; v++) {
    ASSERT_OK(Put("Key2", DummyString(1024, v), wo));
    ASSERT_OK(Put("Key3", DummyString(1024, v), wo));
    ASSERT_OK(Put("Key4", DummyString(1024, v), wo));
    ASSERT_OK(Put("Key1", DummyString(1024, v), wo));
    ASSERT_OK(Put("Key4", DummyString(1024, v), wo));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);

  // Compaction will move the 4 files in L0 to trash and create 1 L1 file
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1", FilesPerLevel(0));

  uint64_t delete_start_time = env_->NowMicros();
  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DBSSTTest::RateLimitedDelete:1");
  sfm->WaitForEmptyTrash();
  uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

  uint64_t total_files_size = 0;
  uint64_t expected_penlty = 0;
  ASSERT_EQ(penalties.size(), metadata.size());
  for (size_t i = 0; i < metadata.size(); i++) {
    total_files_size += metadata[i].size;
    expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec);
    ASSERT_EQ(expected_penlty, penalties[i]);
  }
  ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);
  ASSERT_LT(time_spent_deleting, expected_penlty * 1.1);
  ASSERT_EQ(4, options.statistics->getAndResetTickerCount(FILES_MARKED_TRASH));
  ASSERT_EQ(
      0, options.statistics->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(RateLimitedDelete, DBSSTTestRateLimit,
                        ::testing::Bool());

TEST_F(DBSSTTest, RateLimitedWALDelete) {
  Destroy(last_options_);

  std::vector<uint64_t> penalties;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<uint64_t*>(arg))); });

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.env = env_;

  int64_t rate_bytes_per_sec = 1024 * 10;  // 10 Kbs / Sec
  Status s;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 0, false, &s, 0));
  ASSERT_OK(s);
  options.sst_file_manager->SetDeleteRateBytesPerSecond(rate_bytes_per_sec);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  sfm->delete_scheduler()->SetMaxTrashDBRatio(3.1);
  SetTimeElapseOnlySleepOnReopen(&options);

  ASSERT_OK(TryReopen(options));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Create 4 files in L0
  for (char v = 'a'; v <= 'd'; v++) {
    ASSERT_OK(Put("Key2", DummyString(1024, v)));
    ASSERT_OK(Put("Key3", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Put("Key1", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  // Compaction will move the 4 files in L0 to trash and create 1 L1 file.
  // Use kForceOptimized to not rewrite the new L1 file.
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1", FilesPerLevel(0));

  sfm->WaitForEmptyTrash();
  ASSERT_EQ(penalties.size(), 8);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

class DBWALTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<std::string, bool>> {
 public:
  explicit DBWALTestWithParam()
      : DBTestBase("db_wal_test_with_params", /*env_do_fsync=*/true) {
    wal_dir_ = std::get<0>(GetParam());
    wal_dir_same_as_dbname_ = std::get<1>(GetParam());
  }

  std::string wal_dir_;
  bool wal_dir_same_as_dbname_;
};

TEST_P(DBWALTestWithParam, WALTrashCleanupOnOpen) {
  class MyEnv : public EnvWrapper {
   public:
    MyEnv(Env* t) : EnvWrapper(t), fake_log_delete(false) {}
    const char* Name() const override { return "MyEnv"; }
    Status DeleteFile(const std::string& fname) override {
      if (fname.find(".log.trash") != std::string::npos && fake_log_delete) {
        return Status::OK();
      }

      return target()->DeleteFile(fname);
    }

    void set_fake_log_delete(bool fake) { fake_log_delete = fake; }

   private:
    bool fake_log_delete;
  };

  std::unique_ptr<MyEnv> env(new MyEnv(env_));
  Destroy(last_options_);

  env->set_fake_log_delete(true);

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.env = env.get();
  options.wal_dir = dbname_ + wal_dir_;

  int64_t rate_bytes_per_sec = 1024 * 10;  // 10 Kbs / Sec
  Status s;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 0, false, &s, 0));
  ASSERT_OK(s);
  options.sst_file_manager->SetDeleteRateBytesPerSecond(rate_bytes_per_sec);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  sfm->delete_scheduler()->SetMaxTrashDBRatio(3.1);

  Reopen(options);

  // Create 4 files in L0
  for (char v = 'a'; v <= 'd'; v++) {
    if (v == 'c') {
      // Maximize the chance that the last log file will be preserved in trash
      // before restarting the DB. (Enable slow deletion but at a very slow
      // deletion rate)
      // We have to set this on the 2nd to last file for it to delay deletion
      // on the last file. (Quirk of DeleteScheduler::BackgroundEmptyTrash())
      options.sst_file_manager->SetDeleteRateBytesPerSecond(1);
    }
    ASSERT_OK(Put("Key2", DummyString(1024, v)));
    ASSERT_OK(Put("Key3", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Put("Key1", DummyString(1024, v)));
    ASSERT_OK(Put("Key4", DummyString(1024, v)));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  Close();

  options.sst_file_manager.reset();
  std::vector<std::string> filenames;
  int trash_log_count = 0;
  if (!wal_dir_same_as_dbname_) {
    // Forcibly create some trash log files
    std::unique_ptr<WritableFile> result;
    ASSERT_OK(env->NewWritableFile(options.wal_dir + "/1000.log.trash", &result,
                                   EnvOptions()));
    result.reset();
  }
  ASSERT_OK(env->GetChildren(options.wal_dir, &filenames));
  for (const std::string& fname : filenames) {
    if (fname.find(".log.trash") != std::string::npos) {
      trash_log_count++;
    }
  }
  ASSERT_GE(trash_log_count, 1);

  env->set_fake_log_delete(false);
  Reopen(options);

  filenames.clear();
  trash_log_count = 0;
  ASSERT_OK(env->GetChildren(options.wal_dir, &filenames));
  for (const std::string& fname : filenames) {
    if (fname.find(".log.trash") != std::string::npos) {
      trash_log_count++;
    }
  }
  ASSERT_EQ(trash_log_count, 0);
  Close();
}

INSTANTIATE_TEST_CASE_P(DBWALTestWithParam, DBWALTestWithParam,
                        ::testing::Values(std::make_tuple("", true),
                                          std::make_tuple("_wal_dir", false)));

// Test param: max_trash_db_ratio for DeleteScheduler
class DBObsoleteFileDeletionOnOpenTest
    : public DBSSTTest,
      public ::testing::WithParamInterface<double> {
 public:
  explicit DBObsoleteFileDeletionOnOpenTest()
      : DBSSTTest("db_sst_deletion_on_open_test") {}
};

TEST_P(DBObsoleteFileDeletionOnOpenTest, Basic) {
  Options options = CurrentOptions();
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 1024 * 1024 /* 1 MB/sec */));
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  sfm->SetDeleteRateBytesPerSecond(1024 * 1024);
  double max_trash_db_ratio = GetParam();
  sfm->delete_scheduler()->SetMaxTrashDBRatio(max_trash_db_ratio);

  int bg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Destroy(last_options_);

  // Add some trash files to the db directory so the DB can clean them up
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  ASSERT_OK(
      WriteStringToFile(env_, "abc", dbname_ + "/" + "001.sst.trash", false));
  ASSERT_OK(
      WriteStringToFile(env_, "abc", dbname_ + "/" + "002.sst.trash", false));
  ASSERT_OK(
      WriteStringToFile(env_, "abc", dbname_ + "/" + "003.sst.trash", false));
  // Manually add an obsolete sst file. Obsolete SST files are discovered and
  // deleted upon recovery.
  uint64_t sst_file_number = 100;
  const std::string kObsoleteSstFileOne =
      MakeTableFileName(dbname_, sst_file_number);
  ASSERT_OK(WriteStringToFile(env_, "abc", kObsoleteSstFileOne, false));
  // The slow deletion on recovery had a bug before where a file's size is not
  // first tracked in `total_size_` in SstFileManager before passed to
  // DeleteScheduler. The first obsolete file is still slow deleted because
  // 0 (total_trash_size_) > 0 (total_size_) * 1000 (max_trash_db_ratio)
  // is always false.
  // Here we explicitly create a second obsolete file to verify this bug's fix
  const std::string kObsoleteSstFileTwo =
      MakeTableFileName(dbname_, sst_file_number - 1);
  ASSERT_OK(WriteStringToFile(env_, "abc", kObsoleteSstFileTwo, false));

  // Reopen the DB and verify that it deletes existing trash files and obsolete
  // SST files with rate limiting.
  Reopen(options);
  sfm->WaitForEmptyTrash();
  ASSERT_NOK(env_->FileExists(dbname_ + "/" + "001.sst.trash"));
  ASSERT_NOK(env_->FileExists(dbname_ + "/" + "002.sst.trash"));
  ASSERT_NOK(env_->FileExists(dbname_ + "/" + "003.sst.trash"));
  ASSERT_NOK(env_->FileExists(kObsoleteSstFileOne));
  ASSERT_NOK(env_->FileExists(kObsoleteSstFileTwo));
  // The files in the DB's directory are all either trash or obsolete sst files.
  // So the trash/db ratio is 1. A ratio equal to or higher than 1 should
  // schedule all files' deletion in background. A ratio lower than 1 may
  // send some files to be deleted immediately.
  if (max_trash_db_ratio < 1) {
    ASSERT_LE(bg_delete_file, 5);
  } else {
    ASSERT_EQ(bg_delete_file, 5);
  }

  ASSERT_EQ(sfm->GetTotalSize(), 0);
  ASSERT_EQ(sfm->delete_scheduler()->GetTotalTrashSize(), 0);
}

INSTANTIATE_TEST_CASE_P(DBObsoleteFileDeletionOnOpenTest,
                        DBObsoleteFileDeletionOnOpenTest,
                        ::testing::Values(0, 0.5, 1, 1.2));

// Create a DB with 2 db_paths, and generate multiple files in the 2
// db_paths using CompactRangeOptions, make sure that files that were
// deleted from first db_path were deleted using DeleteScheduler and
// files in the second path were not.
TEST_F(DBSSTTest, DeleteSchedulerMultipleDBPaths) {
  std::atomic<int> bg_delete_file(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  // The deletion scheduler sometimes skips marking file as trash according to
  // a heuristic. In that case the deletion will go through the below SyncPoint.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile", [&](void* /*arg*/) { bg_delete_file++; });

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.db_paths.emplace_back(dbname_, 1024 * 100);
  options.db_paths.emplace_back(dbname_ + "_2", 1024 * 100);
  options.env = env_;

  int64_t rate_bytes_per_sec = 1024 * 1024;  // 1 Mb / Sec
  Status s;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", rate_bytes_per_sec, false, &s,
                        /* max_trash_db_ratio= */ 1.1));

  ASSERT_OK(s);
  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());

  DestroyAndReopen(options);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.disableWAL = true;

  // Create 4 files in L0
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("Key" + std::to_string(i), DummyString(1024, 'A'), wo));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));
  // Compaction will delete files from L0 in first db path and generate a new
  // file in L1 in second db path
  CompactRangeOptions compact_options;
  compact_options.target_path_id = 1;
  Slice begin("Key0");
  Slice end("Key3");
  ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  // Create 4 files in L0
  for (int i = 4; i < 8; i++) {
    ASSERT_OK(Put("Key" + std::to_string(i), DummyString(1024, 'B'), wo));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("4,1", FilesPerLevel(0));

  // Compaction will delete files from L0 in first db path and generate a new
  // file in L1 in second db path
  begin = "Key4";
  end = "Key7";
  ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  ASSERT_EQ("0,2", FilesPerLevel(0));

  sfm->WaitForEmptyTrash();
  ASSERT_EQ(bg_delete_file, 8);

  // Compaction will delete both files and regenerate a file in L1 in second
  // db path. The deleted files should still be cleaned up via delete scheduler.
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  ASSERT_EQ("0,1", FilesPerLevel(0));

  sfm->WaitForEmptyTrash();
  ASSERT_EQ(bg_delete_file, 10);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, DestroyDBWithRateLimitedDelete) {
  Destroy(last_options_);

  int bg_delete_file = 0;
  int preexisting_trash_files_scheduled = 0;
  uint64_t preexisting_trash_penalty = 0;
  std::string file_being_deleted;
  std::string preexisting_trash_file;
  std::vector<uint64_t> penalties;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile::cb",
      [&](void* arg) { file_being_deleted = *static_cast<std::string*>(arg); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait", [&](void* arg) {
        uint64_t penalty = *(static_cast<uint64_t*>(arg));
        penalties.push_back(penalty);
        if (file_being_deleted == preexisting_trash_file) {
          preexisting_trash_penalty = penalty;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleExistingTrashFileDeletion", [&](void* arg) {
        const std::string& file_path = *static_cast<std::string*>(arg);
        if (EndsWith(file_path, ".trash")) {
          preexisting_trash_files_scheduled++;
        }
      });
  std::atomic<bool> fail_preexisting_trash_delete{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:BeforeDeleteFile", [&](void* arg) {
        if (file_being_deleted == preexisting_trash_file &&
            fail_preexisting_trash_delete.exchange(false)) {
          *static_cast<Status*>(arg) = Status::IOError("injected delete error");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Status s;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.env = env_;
  options.sst_file_manager.reset(
      NewSstFileManager(env_, nullptr, "", 0, false, &s, 0));
  ASSERT_OK(s);
  Reopen(options);

  // Create 4 files in L0
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put("Key" + std::to_string(i), DummyString(1024, 'A')));
    ASSERT_OK(Flush());
  }
  // We created 4 sst files in L0
  ASSERT_EQ("4", FilesPerLevel(0));

  // Close DB and destroy it using DeleteScheduler
  Close();

  int num_sst_files = 0;
  int num_wal_files = 0;
  std::string sst_file_to_mark_trash;
  std::vector<std::string> db_files;
  ASSERT_OK(env_->GetChildren(dbname_, &db_files));
  for (const std::string& f : db_files) {
    if (f.substr(f.find_last_of('.') + 1) == "sst") {
      num_sst_files++;
      if (sst_file_to_mark_trash.empty()) {
        sst_file_to_mark_trash = dbname_;
        sst_file_to_mark_trash.append("/").append(f);
      }
    } else if (f.substr(f.find_last_of('.') + 1) == "log") {
      num_wal_files++;
    } else {
      continue;
    }
  }
  ASSERT_GT(num_sst_files, 0);
  ASSERT_GT(num_wal_files, 0);
  ASSERT_FALSE(sst_file_to_mark_trash.empty());
  uint64_t preexisting_trash_file_size = 0;
  ASSERT_OK(
      env_->GetFileSize(sst_file_to_mark_trash, &preexisting_trash_file_size));
  preexisting_trash_file = sst_file_to_mark_trash;
  preexisting_trash_file.append(".trash");

  auto sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());

  const int64_t rate_bytes_per_sec = 1024 * 1024;
  sfm->SetDeleteRateBytesPerSecond(rate_bytes_per_sec);
  // Set an extra high trash ratio to prevent immediate/non-rate limited
  // deletions
  sfm->delete_scheduler()->SetMaxTrashDBRatio(1000.0);
  ASSERT_OK(
      sfm->OnAddFile(sst_file_to_mark_trash, preexisting_trash_file_size));
  ASSERT_OK(sfm->ScheduleFileDeletion(sst_file_to_mark_trash, dbname_,
                                      /*force_bg=*/true));
  sfm->WaitForEmptyTrashInDirectory(dbname_);
  ASSERT_FALSE(fail_preexisting_trash_delete);
  ASSERT_OK(env_->FileExists(preexisting_trash_file));
  ASSERT_EQ(sfm->GetTotalSize(), preexisting_trash_file_size);
  ASSERT_EQ(sfm->GetTotalTrashSize(), preexisting_trash_file_size);

  const std::string unaccounted_trash_file = dbname_ + "/unaccounted.sst.trash";
  ASSERT_OK(WriteStringToFile(env_, "trash", unaccounted_trash_file,
                              /*should_sync=*/false));

  std::string dbname_alias = dbname_;
  dbname_alias.insert(dbname_alias.find_last_of('/'), "/");
  ASSERT_OK(DestroyDB(dbname_alias, options));
  sfm->WaitForEmptyTrash();
  ASSERT_EQ(bg_delete_file, num_sst_files + num_wal_files + 2);
  ASSERT_EQ(preexisting_trash_files_scheduled, 2);
  ASSERT_GE(preexisting_trash_penalty,
            preexisting_trash_file_size * 1000000 / rate_bytes_per_sec);
  ASSERT_EQ(penalties.size(), num_sst_files + num_wal_files + 2);
  ASSERT_TRUE(sfm->GetTrackedFiles().empty());
  ASSERT_EQ(sfm->GetTotalSize(), 0);
  ASSERT_EQ(sfm->GetTotalTrashSize(), 0);
  auto bg_errors = sfm->delete_scheduler()->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 1);
  for (auto& error : bg_errors) {
    ASSERT_NOK(error.second);
  }
  ASSERT_TRUE(env_->FileExists(dbname_).IsNotFound());
}

TEST_F(DBSSTTest, DBWithMaxSpaceAllowed) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  Random rnd(301);

  // Generate a file containing 100 keys.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());

  uint64_t first_file_size = 0;
  std::unordered_map<std::string, uint64_t> files_in_db;
  ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db, &first_file_size));
  ASSERT_EQ(sfm->GetTotalSize(), first_file_size);

  // Set the maximum allowed space usage to the current total size
  sfm->SetMaxAllowedSpaceUsage(first_file_size + 1);

  ASSERT_OK(Put("key1", "val1"));
  // This flush will cause bg_error_ and will fail
  ASSERT_NOK(Flush());
}

TEST_F(DBSSTTest, DBWithMaxSpaceAllowedWithBlobFiles) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  DestroyAndReopen(options);

  Random rnd(301);

  // Generate a file containing keys.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());

  uint64_t files_size = 0;
  uint64_t total_files_size = 0;
  std::unordered_map<std::string, uint64_t> files_in_db;

  ASSERT_OK(GetAllDataFiles(kBlobFile, &files_in_db, &files_size));
  // Make sure blob files are considered by SSTFileManage in size limits.
  ASSERT_GT(files_size, 0);
  total_files_size = files_size;
  ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db, &files_size));
  total_files_size += files_size;
  ASSERT_EQ(sfm->GetTotalSize(), total_files_size);

  // Set the maximum allowed space usage to the current total size.
  sfm->SetMaxAllowedSpaceUsage(total_files_size + 1);

  bool max_allowed_space_reached = false;
  bool delete_blob_file = false;
  // Sync point called after blob file is closed and max allowed space is
  // checked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlobFileCompletionCallback::CallBack::MaxAllowedSpaceReached",
      [&](void* /*arg*/) { max_allowed_space_reached = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BuildTable::AfterDeleteFile",
      [&](void* /*arg*/) { delete_blob_file = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {
          "BuildTable::AfterDeleteFile",
          "DBSSTTest::DBWithMaxSpaceAllowedWithBlobFiles:1",
      },
  });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key1", "val1"));
  // This flush will fail
  ASSERT_NOK(Flush());
  ASSERT_TRUE(max_allowed_space_reached);

  TEST_SYNC_POINT("DBSSTTest::DBWithMaxSpaceAllowedWithBlobFiles:1");
  ASSERT_TRUE(delete_blob_file);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, CancellingCompactionsWorks) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.level0_file_num_compaction_trigger = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  int completed_compactions = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction():CancelledCompaction", [&](void* /*arg*/) {
        sfm->SetMaxAllowedSpaceUsage(0);
        ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun",
      [&](void* /*arg*/) { completed_compactions++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);

  // Generate a file containing 10 keys.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());
  uint64_t total_file_size = 0;
  std::unordered_map<std::string, uint64_t> files_in_db;
  ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db, &total_file_size));
  // Set the maximum allowed space usage to the current total size
  sfm->SetMaxAllowedSpaceUsage(2 * total_file_size + 1);

  // Generate another file to trigger compaction.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Because we set a callback in CancelledCompaction, we actually
  // let the compaction run
  ASSERT_GT(completed_compactions, 0);
  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
  // Make sure the stat is bumped
  ASSERT_GT(dbfull()->immutable_db_options().statistics.get()->getTickerCount(
                COMPACTION_CANCELLED),
            0);
  ASSERT_EQ(0,
            dbfull()->immutable_db_options().statistics.get()->getTickerCount(
                FILES_MARKED_TRASH));
  ASSERT_EQ(4,
            dbfull()->immutable_db_options().statistics.get()->getTickerCount(
                FILES_DELETED_IMMEDIATELY));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, CancellingManualCompactionsWorks) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.statistics = CreateDBStatistics();

  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  DestroyAndReopen(options);

  Random rnd(301);

  // Generate a file containing 10 keys.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());
  uint64_t total_file_size = 0;
  std::unordered_map<std::string, uint64_t> files_in_db;
  ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db, &total_file_size));
  // Set the maximum allowed space usage to the current total size
  sfm->SetMaxAllowedSpaceUsage(2 * total_file_size + 1);

  // Generate another file to trigger compaction.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());

  // OK, now trigger a manual compaction
  ASSERT_TRUE(dbfull()
                  ->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsCompactionTooLarge());

  // Wait for manual compaction to get scheduled and finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
  // Make sure the stat is bumped
  ASSERT_EQ(dbfull()->immutable_db_options().statistics.get()->getTickerCount(
                COMPACTION_CANCELLED),
            1);

  // Now make sure CompactFiles also gets cancelled
  auto l0_files = collector->GetFlushedFiles();
  ASSERT_TRUE(
      dbfull()
          ->CompactFiles(ROCKSDB_NAMESPACE::CompactionOptions(), l0_files, 0)
          .IsCompactionTooLarge());

  // Wait for manual compaction to get scheduled and finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(dbfull()->immutable_db_options().statistics.get()->getTickerCount(
                COMPACTION_CANCELLED),
            2);
  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);

  // Now let the flush through and make sure GetCompactionsReservedSize
  // returns to normal
  sfm->SetMaxAllowedSpaceUsage(0);
  int completed_compactions = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactFilesImpl:End", [&](void* /*arg*/) { completed_compactions++; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->CompactFiles(ROCKSDB_NAMESPACE::CompactionOptions(),
                                   l0_files, 0));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(sfm->GetCompactionsReservedSize(), 0);
  ASSERT_GT(completed_compactions, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, CompactFilesTrivialMoveReleasesReservedSpace) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  options.compression = kNoCompression;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  auto* sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  ASSERT_GT(sfm->GetTotalSize(), 0);
  sfm->SetMaxAllowedSpaceUsage(2 * sfm->GetTotalSize());

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.levels[0].files.size());
  const std::string file_name = metadata.levels[0].files[0].relative_filename;

  CompactionOptions compact_options;
  compact_options.allow_trivial_move = true;
  for (int level = 1; level <= 2; ++level) {
    ASSERT_OK(db_->CompactFiles(compact_options, {file_name}, level));
    ASSERT_EQ(0, sfm->GetCompactionsReservedSize());
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(1, metadata.levels[level].files.size());
    // A trivial move preserves the SST instead of rewriting it.
    ASSERT_EQ(file_name, metadata.levels[level].files[0].relative_filename);
  }
  ASSERT_EQ("value", Get("key"));
}

TEST_F(DBSSTTest, CompactFilesTrivialMoveErrorReleasesReservedSpace) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = false;
  options.compression = kNoCompression;
  options.sst_file_manager.reset(NewSstFileManager(env_));
  auto* sfm = static_cast<SstFileManagerImpl*>(options.sst_file_manager.get());
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.levels[0].files.size());

  CompactionOptions compact_options;
  compact_options.allow_trivial_move = true;
  env_->manifest_write_error_ = true;
  Status s = db_->CompactFiles(
      compact_options, {metadata.levels[0].files[0].relative_filename}, 1);
  env_->manifest_write_error_ = false;
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(0, sfm->GetCompactionsReservedSize());
  ASSERT_EQ("1", FilesPerLevel());
  ASSERT_EQ("value", Get("key"));
}

TEST_F(DBSSTTest, DBWithMaxSpaceAllowedRandomized) {
  // This test will set a maximum allowed space for the DB, then it will
  // keep filling the DB until the limit is reached and bg_error_ is set.
  // When bg_error_ is set we will verify that the DB size is greater
  // than the limit.

  std::vector<int> max_space_limits_mbs = {1, 10};
  std::atomic<bool> bg_error_set(false);

  std::atomic<int> reached_max_space_on_flush(0);
  std::atomic<int> reached_max_space_on_compaction(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:MaxAllowedSpaceReached",
      [&](void* arg) {
        Status* bg_error = static_cast<Status*>(arg);
        bg_error_set = true;
        reached_max_space_on_flush++;
        // clear error to ensure compaction callback is called
        *bg_error = Status::OK();
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction():CancelledCompaction", [&](void* arg) {
        bool* enough_room = static_cast<bool*>(arg);
        *enough_room = true;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::FinishCompactionOutputFile:MaxAllowedSpaceReached",
      [&](void* /*arg*/) {
        bg_error_set = true;
        reached_max_space_on_compaction++;
      });

  for (auto limit_mb : max_space_limits_mbs) {
    bg_error_set = false;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
    auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());

    Options options = CurrentOptions();
    options.sst_file_manager = sst_file_manager;
    options.write_buffer_size = 1024 * 512;  // 512 Kb
    DestroyAndReopen(options);
    Random rnd(301);

    sfm->SetMaxAllowedSpaceUsage(limit_mb * 1024 * 1024);

    // It is easy to detect if the test is stuck in a loop. No need for
    // complex termination logic.
    while (true) {
      auto s = Put(rnd.RandomString(10), rnd.RandomString(50));
      if (!s.ok()) {
        break;
      }
    }
    ASSERT_TRUE(bg_error_set);
    uint64_t total_sst_files_size = 0;
    std::unordered_map<std::string, uint64_t> files_in_db;
    ASSERT_OK(GetAllDataFiles(kTableFile, &files_in_db, &total_sst_files_size));
    ASSERT_GE(total_sst_files_size, limit_mb * 1024 * 1024);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }

  ASSERT_GT(reached_max_space_on_flush, 0);
  ASSERT_GT(reached_max_space_on_compaction, 0);
}

TEST_F(DBSSTTest, OpenDBWithInfiniteMaxOpenFiles) {
  // Open DB with infinite max open files
  //  - First iteration use 1 thread to open files
  //  - Second iteration use 5 threads to open files
  for (int iter = 0; iter < 2; iter++) {
    Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 100000;
    options.disable_auto_compactions = true;
    options.max_open_files = -1;
    if (iter == 0) {
      options.max_file_opening_threads = 1;
    } else {
      options.max_file_opening_threads = 5;
    }
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    // Create 12 Files in L0 (then move then to L2)
    for (int i = 0; i < 12; i++) {
      std::string k = "L2_" + Key(i);
      ASSERT_OK(Put(k, k + std::string(1000, 'a')));
      ASSERT_OK(Flush());
    }
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 2;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

    // Create 12 Files in L0
    for (int i = 0; i < 12; i++) {
      std::string k = "L0_" + Key(i);
      ASSERT_OK(Put(k, k + std::string(1000, 'a')));
      ASSERT_OK(Flush());
    }
    Close();

    // Reopening the DB will load all existing files
    Reopen(options);
    ASSERT_EQ("12,0,12", FilesPerLevel(0));
    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);

    for (const auto& level : files) {
      for (const auto& file : level) {
        ASSERT_TRUE(file.fd.pinned_reader.Get() != nullptr);
      }
    }

    for (int i = 0; i < 12; i++) {
      ASSERT_EQ(Get("L0_" + Key(i)), "L0_" + Key(i) + std::string(1000, 'a'));
      ASSERT_EQ(Get("L2_" + Key(i)), "L2_" + Key(i) + std::string(1000, 'a'));
    }
  }
}

TEST_F(DBSSTTest, OpenDBWithInfiniteMaxOpenFilesSubjectToMemoryLimit) {
  for (CacheEntryRoleOptions::Decision charge_table_reader :
       {CacheEntryRoleOptions::Decision::kEnabled,
        CacheEntryRoleOptions::Decision::kDisabled}) {
    // Open DB with infinite max open files
    //  - First iteration use 1 thread to open files
    //  - Second iteration use 5 threads to open files
    for (int iter = 0; iter < 2; iter++) {
      Options options;
      options.create_if_missing = true;
      options.write_buffer_size = 100000;
      options.disable_auto_compactions = true;
      options.max_open_files = -1;

      BlockBasedTableOptions table_options;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      if (iter == 0) {
        options.max_file_opening_threads = 1;
      } else {
        options.max_file_opening_threads = 5;
      }

      DestroyAndReopen(options);

      // Create 5 Files in L0 (then move then to L2)
      for (int i = 0; i < 5; i++) {
        std::string k = "L2_" + Key(i);
        ASSERT_OK(Put(k, k + std::string(1000, 'a')));
        ASSERT_OK(Flush()) << i;
      }
      CompactRangeOptions compact_options;
      compact_options.change_level = true;
      compact_options.target_level = 2;
      ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

      // Create 5 Files in L0
      for (int i = 0; i < 5; i++) {
        std::string k = "L0_" + Key(i);
        ASSERT_OK(Put(k, k + std::string(1000, 'a')));
        ASSERT_OK(Flush());
      }
      Close();

      table_options.cache_usage_options.options_overrides.insert(
          {CacheEntryRole::kBlockBasedTableReader,
           {/*.charged = */ charge_table_reader}});
      table_options.block_cache =
          NewLRUCache(1024 /* capacity */, 0 /* num_shard_bits */,
                      true /* strict_capacity_limit */);
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));

      // Reopening the DB will try to load all existing files, conditionally
      // subject to memory limit
      Status s = TryReopen(options);

      if (charge_table_reader == CacheEntryRoleOptions::Decision::kEnabled) {
        EXPECT_TRUE(s.IsMemoryLimit());
        EXPECT_TRUE(s.ToString().find(
                        kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
                            CacheEntryRole::kBlockBasedTableReader)]) !=
                    std::string::npos);
        EXPECT_TRUE(s.ToString().find("memory limit based on cache capacity") !=
                    std::string::npos);

      } else {
        EXPECT_TRUE(s.ok());
        ASSERT_EQ("5,0,5", FilesPerLevel(0));
      }
    }
  }
}

TEST_F(DBSSTTest, GetTotalSstFilesSize) {
  // FIXME: L0 file and L1+ file also differ in size of `oldest_key_time`.
  //  L0 file has non-zero `oldest_key_time` while L1+ files have 0.
  //  The test passes since L1+ file uses current time instead of 0
  //  as oldest_ancestor_time.
  //
  // We don't propagate oldest-key-time table property on compaction and
  // just write 0 as default value. This affect the exact table size, since
  // we encode table properties as varint64. Force time to be 0 to work around
  // it. Should remove the workaround after we propagate the property on
  // compaction.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:oldest_ancestor_time", [&](void* arg) {
        uint64_t* current_time = static_cast<uint64_t*>(arg);
        *current_time = 0;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  DestroyAndReopen(options);
  // Generate 5 files in L0
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j < 10; j++) {
      std::string val = "val_file_" + std::to_string(i);
      ASSERT_OK(Put(Key(j), val));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("5", FilesPerLevel(0));

  std::vector<LiveFileMetaData> live_files_meta;
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);
  uint64_t single_file_size = live_files_meta[0].size;

  uint64_t live_sst_files_size = 0;
  uint64_t total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 5
  // Total SST files = 5
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));
  ASSERT_OK(iter1->status());

  // Compact 5 files into 1 file in L0
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 1);

  live_sst_files_size = 0;
  total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 1 (compacted file)
  // Total SST files = 6 (5 original files + compacted file)
  ASSERT_EQ(live_sst_files_size, 1 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 6 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));
  ASSERT_OK(iter2->status());

  // Delete all keys and compact, this will delete all live files
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 0);

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 6 (5 original files + compacted file)
  ASSERT_EQ(total_sst_files_size, 6 * single_file_size);

  ASSERT_OK(iter1->status());
  iter1.reset();
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 1 (compacted file)
  ASSERT_EQ(total_sst_files_size, 1 * single_file_size);

  ASSERT_OK(iter2->status());
  iter2.reset();
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 0
  ASSERT_EQ(total_sst_files_size, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBSSTTest, GetTotalSstFilesSizeVersionsFilesShared) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  DestroyAndReopen(options);
  // Generate 5 files in L0
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Put(Key(i), "val"));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("5", FilesPerLevel(0));

  std::vector<LiveFileMetaData> live_files_meta;
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);
  uint64_t single_file_size = live_files_meta[0].size;

  uint64_t live_sst_files_size = 0;
  uint64_t total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));

  // Live SST files = 5
  // Total SST files = 5
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));
  ASSERT_OK(iter1->status());

  // Compaction will do trivial move from L0 to L1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,5", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 5);

  live_sst_files_size = 0;
  total_sst_files_size = 0;
  for (const auto& file_meta : live_files_meta) {
    live_sst_files_size += file_meta.size;
  }
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 5
  // Total SST files = 5 (used in 2 version)
  ASSERT_EQ(live_sst_files_size, 5 * single_file_size);
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  // hold current version
  std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));
  ASSERT_OK(iter2->status());

  // Delete all keys and compact, this will delete all live files
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("", FilesPerLevel(0));

  live_files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&live_files_meta);
  ASSERT_EQ(live_files_meta.size(), 0);

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 5 (used in 2 version)
  ASSERT_EQ(total_sst_files_size, 5 * single_file_size);

  ASSERT_OK(iter1->status());
  iter1.reset();
  ASSERT_OK(iter2->status());
  iter2.reset();

  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.total-sst-files-size",
                                       &total_sst_files_size));
  // Live SST files = 0
  // Total SST files = 0
  ASSERT_EQ(total_sst_files_size, 0);
}

// This test if blob files are recorded by SST File Manager when Compaction job
// creates/delete them and in case of AtomicFlush.
TEST_F(DBSSTTest, DBWithSFMForBlobFilesAtomicFlush) {
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  auto sfm = static_cast<SstFileManagerImpl*>(sst_file_manager.get());
  Options options = CurrentOptions();
  options.sst_file_manager = sst_file_manager;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.5;
  options.atomic_flush = true;

  int files_added = 0;
  int files_deleted = 0;
  int files_scheduled_to_delete = 0;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnAddFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          files_added++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::OnDeleteFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          files_deleted++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          ++files_scheduled_to_delete;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);
  Random rnd(301);

  ASSERT_OK(Put("key_1", "value_1"));
  ASSERT_OK(Put("key_2", "value_2"));
  ASSERT_OK(Put("key_3", "value_3"));
  ASSERT_OK(Put("key_4", "value_4"));
  ASSERT_OK(Flush());

  // Overwrite will create the garbage data.
  ASSERT_OK(Put("key_3", "new_value_3"));
  ASSERT_OK(Put("key_4", "new_value_4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key5", "blob_value5"));
  ASSERT_OK(Put("Key6", "blob_value6"));
  ASSERT_OK(Flush());

  ASSERT_EQ(files_added, 3);
  ASSERT_EQ(files_deleted, 0);
  ASSERT_EQ(files_scheduled_to_delete, 0);
  files_added = 0;

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;
  // Compaction job will create a new file and delete the older files.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(files_added, 1);
  ASSERT_EQ(files_scheduled_to_delete, 1);

  sfm->WaitForEmptyTrash();

  ASSERT_EQ(files_deleted, 1);

  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SstFileManagerImpl::ScheduleUnaccountedFileDeletion", [&](void* arg) {
        assert(arg);
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          ++files_scheduled_to_delete;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::OnDeleteFile", [&](void* arg) {
        const std::string* const file_path =
            static_cast<const std::string*>(arg);
        if (EndsWith(*file_path, ".blob")) {
          files_deleted++;
        }
      });
  ASSERT_OK(DestroyDB(dbname_, options));

  ASSERT_EQ(files_scheduled_to_delete, 4);

  sfm->WaitForEmptyTrash();

  ASSERT_EQ(files_deleted, 4);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBSSTTest, SstGetFileSizeFails) {
  // Build an SST file
  ASSERT_OK(Put("x", "zaphod"));
  ASSERT_OK(Flush());
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(1U, metadata.size());
  std::string filename = dbname_ + metadata[0].name;

  // Prepare for fault injection
  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(
          CurrentOptions().env->GetFileSystem());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.env = fault_fs_env.get();
  options.paranoid_checks = false;  // don't check file sizes on open

  for (int i = 0; i < 4; i++) {
    SCOPED_TRACE("Iteration = " + std::to_string(i));
    fault_fs->SetFailRandomAccessGetFileSizeSst(false);
    fault_fs->SetFailFilesystemGetFileSizeSst(false);
    Close();

    if (i == 1) {
      // Just FSRandomAccessFile::GetFileSize fails, which should be worked
      // around
      fault_fs->SetFailRandomAccessGetFileSizeSst(true);
    } else if (i == 2) {
      // FileSystem::GetFileSize fails, which should be worked around if
      // FSRandomAccessFile::GetFileSize is supported
      fault_fs->SetFailFilesystemGetFileSizeSst(true);
    } else if (i == 3) {
      // Both GetFileSize APIs fail with an IOError
      fault_fs->SetFailRandomAccessGetFileSizeSst(true);
      fault_fs->SetFailFilesystemGetFileSizeSst(true);
    }

    ASSERT_OK(TryReopen(options));
    std::string value;
    Status get_status = db_->Get({}, "x", &value);
    if (i < 2) {
      ASSERT_OK(get_status);
    } else if (i == 2) {
      if (encrypted_env_) {
        // Can't recover because RandomAccessFile::GetFileSize is not supported
        // on EncryptedEnv
        // Fail with propagated IOError. (Not Corruption nor NotSupported!)
        ASSERT_EQ(get_status.code(), Status::Code::kIOError);
        ASSERT_STREQ(get_status.getState(), "FileSystem::GetFileSize failed");
      } else {
        // Never sees the FileSystem::GetFileSize failure
        ASSERT_OK(get_status);
      }
    } else {
      ASSERT_EQ(i, 3);
      // Fail with propagated IOError. (Not Corruption nor NotSupported!)
      ASSERT_EQ(get_status.code(), Status::Code::kIOError);
      ASSERT_STREQ(get_status.getState(), "FileSystem::GetFileSize failed");
    }
  }
  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
