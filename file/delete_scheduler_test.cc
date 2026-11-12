//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/delete_scheduler.h"

#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class DeleteSchedulerTest : public testing::Test {
 public:
  DeleteSchedulerTest() : env_(Env::Default()) {
    const int kNumDataDirs = 3;
    dummy_files_dirs_.reserve(kNumDataDirs);
    for (size_t i = 0; i < kNumDataDirs; ++i) {
      dummy_files_dirs_.emplace_back(
          test::PerThreadDBPath(env_, "delete_scheduler_dummy_data_dir") +
          std::to_string(i));
      DestroyAndCreateDir(dummy_files_dirs_.back());
    }
    stats_ = ROCKSDB_NAMESPACE::CreateDBStatistics();
  }

  ~DeleteSchedulerTest() override {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({});
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    for (const auto& dummy_files_dir : dummy_files_dirs_) {
      EXPECT_OK(DestroyDir(env_, dummy_files_dir));
    }
  }

  void DestroyAndCreateDir(const std::string& dir) {
    ASSERT_OK(DestroyDir(env_, dir));
    EXPECT_OK(env_->CreateDir(dir));
  }

  int CountNormalFiles(size_t dummy_files_dirs_idx = 0) {
    std::vector<std::string> files_in_dir;
    EXPECT_OK(env_->GetChildren(dummy_files_dirs_[dummy_files_dirs_idx],
                                &files_in_dir));

    int normal_cnt = 0;
    for (auto& f : files_in_dir) {
      if (!DeleteScheduler::IsTrashFile(f)) {
        normal_cnt++;
      }
    }
    return normal_cnt;
  }

  int CountTrashFiles(size_t dummy_files_dirs_idx = 0) {
    std::vector<std::string> files_in_dir;
    EXPECT_OK(env_->GetChildren(dummy_files_dirs_[dummy_files_dirs_idx],
                                &files_in_dir));

    int trash_cnt = 0;
    for (auto& f : files_in_dir) {
      if (DeleteScheduler::IsTrashFile(f)) {
        trash_cnt++;
      }
    }
    return trash_cnt;
  }

  std::string NewDummyFile(const std::string& file_name, uint64_t size = 1024,
                           size_t dummy_files_dirs_idx = 0, bool track = true) {
    std::string file_path =
        dummy_files_dirs_[dummy_files_dirs_idx] + "/" + file_name;
    std::unique_ptr<WritableFile> f;
    EXPECT_OK(env_->NewWritableFile(file_path, &f, EnvOptions()));
    std::string data(size, 'A');
    EXPECT_OK(f->Append(data));
    EXPECT_OK(f->Close());
    if (track) {
      EXPECT_OK(sst_file_mgr_->OnAddFile(file_path));
    }
    return file_path;
  }

  void NewDeleteScheduler() {
    // Tests in this file are for DeleteScheduler component and don't create any
    // DBs, so we need to set max_trash_db_ratio to 100% (instead of default
    // 25%)
    sst_file_mgr_.reset(
        new SstFileManagerImpl(env_->GetSystemClock(), env_->GetFileSystem(),
                               nullptr, rate_bytes_per_sec_,
                               /* max_trash_db_ratio= */ 1.1, 128 * 1024));
    delete_scheduler_ = sst_file_mgr_->delete_scheduler();
    sst_file_mgr_->SetStatisticsPtr(stats_);
  }

  Env* env_;
  std::vector<std::string> dummy_files_dirs_;
  int64_t rate_bytes_per_sec_;
  DeleteScheduler* delete_scheduler_;
  std::unique_ptr<SstFileManagerImpl> sst_file_mgr_;
  std::shared_ptr<Statistics> stats_;
};

// Test the basic functionality of DeleteScheduler (Rate Limiting).
// 1- Create 100 dummy files
// 2- Delete the 100 dummy files using DeleteScheduler
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 3- Wait for DeleteScheduler to delete all files in trash
// 4- Verify that BackgroundEmptyTrash used to correct penlties for the files
// 5- Make sure that all created files were completely deleted
TEST_F(DeleteSchedulerTest, BasicRateLimiting) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::BasicRateLimiting:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });

  std::vector<uint64_t> penalties;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<uint64_t*>(arg))); });
  int dir_synced = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile::AfterSyncDir", [&](void* arg) {
        dir_synced++;
        std::string* dir = static_cast<std::string*>(arg);
        EXPECT_EQ(dummy_files_dirs_[0], *dir);
      });

  int num_files = 100;        // 100 files
  uint64_t file_size = 1024;  // every file is 1 kb
  std::vector<uint64_t> delete_kbs_per_sec = {512, 200, 100, 50, 25};

  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    penalties.clear();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndCreateDir(dummy_files_dirs_[0]);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    NewDeleteScheduler();

    dir_synced = 0;
    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    for (int i = 0; i < num_files; i++) {
      std::string file_name = "file" + std::to_string(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
    }

    // Delete dummy files and measure time spent to empty trash
    for (int i = 0; i < num_files; i++) {
      ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i],
                                              dummy_files_dirs_[0]));
    }
    ASSERT_EQ(CountNormalFiles(), 0);

    uint64_t delete_start_time = env_->NowMicros();
    TEST_SYNC_POINT("DeleteSchedulerTest::BasicRateLimiting:1");
    delete_scheduler_->WaitForEmptyTrash();
    uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);

    uint64_t total_files_size = 0;
    uint64_t expected_penlty = 0;
    ASSERT_EQ(penalties.size(), num_files);
    for (int i = 0; i < num_files; i++) {
      total_files_size += file_size;
      expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec_);
      ASSERT_EQ(expected_penlty, penalties[i]);
    }
    ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);

    ASSERT_EQ(num_files, dir_synced);

    ASSERT_EQ(CountTrashFiles(), 0);
    ASSERT_EQ(num_files, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
    ASSERT_EQ(num_files,
              stats_->getAndResetTickerCount(FILES_DELETED_FROM_TRASH_QUEUE));
    ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DeleteSchedulerTest, MultiDirectoryDeletionsScheduled) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::MultiDbPathDeletionsScheduled:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  rate_bytes_per_sec_ = 1 << 20;  // 1MB
  NewDeleteScheduler();

  // Generate dummy files in multiple directories
  const size_t kNumFiles = dummy_files_dirs_.size();
  const size_t kFileSize = 1 << 10;  // 1KB
  std::vector<std::string> generated_files;
  for (size_t i = 0; i < kNumFiles; i++) {
    generated_files.push_back(NewDummyFile("file", kFileSize, i));
    ASSERT_EQ(1, CountNormalFiles(i));
  }

  // Mark dummy files as trash
  for (size_t i = 0; i < kNumFiles; i++) {
    ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i], ""));
    ASSERT_EQ(0, CountNormalFiles(i));
    ASSERT_EQ(1, CountTrashFiles(i));
  }
  TEST_SYNC_POINT("DeleteSchedulerTest::MultiDbPathDeletionsScheduled:1");
  delete_scheduler_->WaitForEmptyTrash();

  // Verify dummy files eventually got deleted
  for (size_t i = 0; i < kNumFiles; i++) {
    ASSERT_EQ(0, CountNormalFiles(i));
    ASSERT_EQ(0, CountTrashFiles(i));
  }

  ASSERT_EQ(kNumFiles, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
  ASSERT_EQ(kNumFiles,
            stats_->getAndResetTickerCount(FILES_DELETED_FROM_TRASH_QUEUE));
  ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Same as the BasicRateLimiting test but delete files in multiple threads.
// 1- Create 100 dummy files
// 2- Delete the 100 dummy files using DeleteScheduler using 10 threads
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 3- Wait for DeleteScheduler to delete all files in queue
// 4- Verify that BackgroundEmptyTrash used to correct penlties for the files
// 5- Make sure that all created files were completely deleted
TEST_F(DeleteSchedulerTest, RateLimitingMultiThreaded) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::RateLimitingMultiThreaded:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });

  std::vector<uint64_t> penalties;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<uint64_t*>(arg))); });

  int thread_cnt = 10;
  int num_files = 10;         // 10 files per thread
  uint64_t file_size = 1024;  // every file is 1 kb

  std::vector<uint64_t> delete_kbs_per_sec = {512, 200, 100, 50, 25};
  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    penalties.clear();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndCreateDir(dummy_files_dirs_[0]);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    NewDeleteScheduler();

    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    for (int i = 0; i < num_files * thread_cnt; i++) {
      std::string file_name = "file" + std::to_string(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
    }

    // Delete dummy files using 10 threads and measure time spent to empty trash
    std::atomic<int> thread_num(0);
    std::vector<port::Thread> threads;
    std::function<void()> delete_thread = [&]() {
      int idx = thread_num.fetch_add(1);
      int range_start = idx * num_files;
      int range_end = range_start + num_files;
      for (int j = range_start; j < range_end; j++) {
        ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[j], ""));
      }
    };

    threads.reserve(thread_cnt);
    for (int i = 0; i < thread_cnt; i++) {
      threads.emplace_back(delete_thread);
    }

    for (size_t i = 0; i < threads.size(); i++) {
      threads[i].join();
    }

    uint64_t delete_start_time = env_->NowMicros();
    TEST_SYNC_POINT("DeleteSchedulerTest::RateLimitingMultiThreaded:1");
    delete_scheduler_->WaitForEmptyTrash();
    uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);

    uint64_t total_files_size = 0;
    uint64_t expected_penlty = 0;
    ASSERT_EQ(penalties.size(), num_files * thread_cnt);
    for (int i = 0; i < num_files * thread_cnt; i++) {
      total_files_size += file_size;
      expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec_);
      ASSERT_EQ(expected_penlty, penalties[i]);
    }
    ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);

    ASSERT_EQ(CountNormalFiles(), 0);
    ASSERT_EQ(CountTrashFiles(), 0);
    int total_num_files = num_files * thread_cnt;
    ASSERT_EQ(total_num_files,
              stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
    ASSERT_EQ(total_num_files,
              stats_->getAndResetTickerCount(FILES_DELETED_FROM_TRASH_QUEUE));
    ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

// Disable rate limiting by setting rate_bytes_per_sec_ to 0 and make sure
// that when DeleteScheduler delete a file it delete it immediately and don't
// move it to trash
TEST_F(DeleteSchedulerTest, DisableRateLimiting) {
  int bg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 0;
  NewDeleteScheduler();
  constexpr int num_files = 10;

  for (int i = 0; i < num_files; i++) {
    // Every file we delete will be deleted immediately
    std::string dummy_file = NewDummyFile("dummy.data");
    ASSERT_OK(delete_scheduler_->DeleteFile(dummy_file, ""));
    ASSERT_TRUE(env_->FileExists(dummy_file).IsNotFound());
    ASSERT_EQ(CountNormalFiles(), 0);
    ASSERT_EQ(CountTrashFiles(), 0);
  }

  ASSERT_EQ(bg_delete_file, 0);
  ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
  ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_DELETED_FROM_TRASH_QUEUE));
  ASSERT_EQ(num_files,
            stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));

  ASSERT_FALSE(delete_scheduler_->NewTrashBucket().has_value());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Testing that moving files to trash with the same name is not a problem
// 1- Create 10 files with the same name "conflict.data"
// 2- Delete the 10 files using DeleteScheduler
// 3- Make sure that trash directory contain 10 files ("conflict.data" x 10)
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 4- Make sure that files are deleted from trash
TEST_F(DeleteSchedulerTest, ConflictNames) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::ConflictNames:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 Mb/sec
  NewDeleteScheduler();

  // Create "conflict.data" and move it to trash 10 times
  for (int i = 0; i < 10; i++) {
    std::string dummy_file = NewDummyFile("conflict.data");
    ASSERT_OK(delete_scheduler_->DeleteFile(dummy_file, ""));
  }
  ASSERT_EQ(CountNormalFiles(), 0);
  // 10 files ("conflict.data" x 10) in trash
  ASSERT_EQ(CountTrashFiles(), 10);

  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DeleteSchedulerTest::ConflictNames:1");
  delete_scheduler_->WaitForEmptyTrash();
  ASSERT_EQ(CountTrashFiles(), 0);

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);
  ASSERT_EQ(10, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
  ASSERT_EQ(10, stats_->getAndResetTickerCount(FILES_DELETED_FROM_TRASH_QUEUE));
  ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// 1- Create 10 dummy files
// 2- Delete the 10 files using DeleteScheduler (move them to trsah)
// 3- Delete the 10 files directly (using env_->DeleteFile)
// --- Hold DeleteScheduler::BackgroundEmptyTrash ---
// 4- Make sure that DeleteScheduler failed to delete the 10 files and
//    reported 10 background errors
// 5- Make sure the vanished files no longer count as trash or tracked size
TEST_F(DeleteSchedulerTest, BackgroundError) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::BackgroundError:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 Mb/sec
  NewDeleteScheduler();

  // Generate 10 dummy files and move them to trash
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + std::to_string(i) + ".data";
    ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name), ""));
  }
  ASSERT_EQ(CountNormalFiles(), 0);
  ASSERT_EQ(CountTrashFiles(), 10);

  // Delete 10 files from trash, this will cause background errors in
  // BackgroundEmptyTrash since we already deleted the files it was
  // goind to delete
  for (int i = 0; i < 10; i++) {
    std::string file_name = "data_" + std::to_string(i) + ".data.trash";
    ASSERT_OK(env_->DeleteFile(dummy_files_dirs_[0] + "/" + file_name));
  }

  // Hold BackgroundEmptyTrash
  TEST_SYNC_POINT("DeleteSchedulerTest::BackgroundError:1");
  delete_scheduler_->WaitForEmptyTrash();
  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 10);
  for (const auto& it : bg_errors) {
    ASSERT_TRUE(it.second.IsPathNotFound());
  }
  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(0, sst_file_mgr_->GetTotalSize());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// 1- Create kTestFileNum dummy files
// 2- Delete kTestFileNum dummy files using DeleteScheduler
// 3- Wait for DeleteScheduler to delete all files in queue
// 4- Make sure all files in trash directory were deleted
// 5- Repeat previous steps 5 times
TEST_F(DeleteSchedulerTest, StartBGEmptyTrashMultipleTimes) {
  constexpr int kTestFileNum = 10;
  std::atomic_int bg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  NewDeleteScheduler();

  // If trash file is generated faster than deleting, delete_scheduler will
  // delete it directly instead of waiting for background trash empty thread to
  // clean it. Set the ratio higher to avoid that.
  sst_file_mgr_->SetMaxTrashDBRatio(kTestFileNum + 1);

  // Move files to trash, wait for empty trash, start again
  for (int run = 1; run <= 5; run++) {
    // Generate kTestFileNum dummy files and move them to trash
    for (int i = 0; i < kTestFileNum; i++) {
      std::string file_name = "data_" + std::to_string(i) + ".data";
      ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name), ""));
    }
    ASSERT_EQ(CountNormalFiles(), 0);
    delete_scheduler_->WaitForEmptyTrash();
    ASSERT_EQ(bg_delete_file, kTestFileNum * run);
    ASSERT_EQ(CountTrashFiles(), 0);

    auto bg_errors = delete_scheduler_->GetBackgroundErrors();
    ASSERT_EQ(bg_errors.size(), 0);
    ASSERT_EQ(kTestFileNum, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
    ASSERT_EQ(kTestFileNum,
              stats_->getAndResetTickerCount(FILES_DELETED_FROM_TRASH_QUEUE));
    ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));
  }

  ASSERT_EQ(bg_delete_file, 5 * kTestFileNum);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
}

TEST_F(DeleteSchedulerTest, DeletePartialFile) {
  int bg_delete_file = 0;
  int bg_fsync = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void*) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:Fsync", [&](void*) { bg_fsync++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  NewDeleteScheduler();

  // Should delete in 4 batch
  ASSERT_OK(
      delete_scheduler_->DeleteFile(NewDummyFile("data_1", 500 * 1024), ""));
  ASSERT_OK(
      delete_scheduler_->DeleteFile(NewDummyFile("data_2", 100 * 1024), ""));
  // Should delete in 2 batch
  ASSERT_OK(
      delete_scheduler_->DeleteFile(NewDummyFile("data_2", 200 * 1024), ""));

  delete_scheduler_->WaitForEmptyTrash();

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);
  ASSERT_EQ(7, bg_delete_file);
  ASSERT_EQ(4, bg_fsync);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
}

#ifdef OS_LINUX
TEST_F(DeleteSchedulerTest, NoPartialDeleteWithLink) {
  int bg_delete_file = 0;
  int bg_fsync = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void*) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:Fsync", [&](void*) { bg_fsync++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  NewDeleteScheduler();

  std::string file1 = NewDummyFile("data_1", 500 * 1024);
  std::string file2 = NewDummyFile("data_2", 100 * 1024);

  ASSERT_OK(env_->LinkFile(file1, dummy_files_dirs_[0] + "/data_1b"));
  ASSERT_OK(env_->LinkFile(file2, dummy_files_dirs_[0] + "/data_2b"));

  // Should delete in 4 batch if there is no hardlink
  ASSERT_OK(delete_scheduler_->DeleteFile(file1, ""));
  ASSERT_OK(delete_scheduler_->DeleteFile(file2, ""));

  delete_scheduler_->WaitForEmptyTrash();

  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(bg_errors.size(), 0);
  ASSERT_EQ(2, bg_delete_file);
  ASSERT_EQ(0, bg_fsync);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
}
#endif

// 1- Create a DeleteScheduler with very slow rate limit (1 Byte / sec)
// 2- Delete 100 files using DeleteScheduler
// 3- Delete the DeleteScheduler (call the destructor while queue is not empty)
// 4- Make sure that not all files were deleted from trash and that
//    DeleteScheduler background thread did not delete all files
TEST_F(DeleteSchedulerTest, DestructorWithNonEmptyQueue) {
  int bg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1;  // 1 Byte / sec
  NewDeleteScheduler();

  for (int i = 0; i < 100; i++) {
    std::string file_name = "data_" + std::to_string(i) + ".data";
    ASSERT_OK(delete_scheduler_->DeleteFile(NewDummyFile(file_name), ""));
  }

  // Deleting 100 files will need >28 hours to delete
  // we will delete the DeleteScheduler while delete queue is not empty
  sst_file_mgr_.reset();

  ASSERT_LT(bg_delete_file, 100);
  ASSERT_GT(CountTrashFiles(), 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DeleteSchedulerTest, DISABLED_DynamicRateLimiting1) {
  std::vector<uint64_t> penalties;
  int bg_delete_file = 0;
  int fg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile", [&](void* /*arg*/) { fg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::BackgroundEmptyTrash:Wait",
      [&](void* arg) { penalties.push_back(*(static_cast<int*>(arg))); });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DeleteSchedulerTest::DynamicRateLimiting1:1",
       "DeleteScheduler::BackgroundEmptyTrash"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 0;  // Disable rate limiting initially
  NewDeleteScheduler();

  int num_files = 10;         // 10 files
  uint64_t file_size = 1024;  // every file is 1 kb

  std::vector<int64_t> delete_kbs_per_sec = {512, 200, 0, 100, 50, -2, 25};
  for (size_t t = 0; t < delete_kbs_per_sec.size(); t++) {
    penalties.clear();
    bg_delete_file = 0;
    fg_delete_file = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    DestroyAndCreateDir(dummy_files_dirs_[0]);
    rate_bytes_per_sec_ = delete_kbs_per_sec[t] * 1024;
    delete_scheduler_->SetRateBytesPerSecond(rate_bytes_per_sec_);

    // Create 100 dummy files, every file is 1 Kb
    std::vector<std::string> generated_files;
    for (int i = 0; i < num_files; i++) {
      std::string file_name = "file" + std::to_string(i) + ".data";
      generated_files.push_back(NewDummyFile(file_name, file_size));
    }

    // Delete dummy files and measure time spent to empty trash
    for (int i = 0; i < num_files; i++) {
      ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i], ""));
    }
    ASSERT_EQ(CountNormalFiles(), 0);

    if (rate_bytes_per_sec_ > 0) {
      uint64_t delete_start_time = env_->NowMicros();
      TEST_SYNC_POINT("DeleteSchedulerTest::DynamicRateLimiting1:1");
      delete_scheduler_->WaitForEmptyTrash();
      uint64_t time_spent_deleting = env_->NowMicros() - delete_start_time;

      auto bg_errors = delete_scheduler_->GetBackgroundErrors();
      ASSERT_EQ(bg_errors.size(), 0);

      uint64_t total_files_size = 0;
      uint64_t expected_penlty = 0;
      ASSERT_EQ(penalties.size(), num_files);
      for (int i = 0; i < num_files; i++) {
        total_files_size += file_size;
        expected_penlty = ((total_files_size * 1000000) / rate_bytes_per_sec_);
        ASSERT_EQ(expected_penlty, penalties[i]);
      }
      ASSERT_GT(time_spent_deleting, expected_penlty * 0.9);
      ASSERT_EQ(bg_delete_file, num_files);
      ASSERT_EQ(fg_delete_file, 0);
    } else {
      ASSERT_EQ(penalties.size(), 0);
      ASSERT_EQ(bg_delete_file, 0);
      ASSERT_EQ(fg_delete_file, num_files);
    }

    ASSERT_EQ(CountTrashFiles(), 0);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DeleteSchedulerTest, ImmediateDeleteOn25PercDBSize) {
  int bg_delete_file = 0;
  int fg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile", [&](void* /*arg*/) { fg_delete_file++; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  int num_files = 100;             // 100 files
  uint64_t file_size = 1024 * 10;  // 100 KB as a file size
  rate_bytes_per_sec_ = 1;         // 1 byte per sec (very slow trash delete)

  NewDeleteScheduler();
  delete_scheduler_->SetMaxTrashDBRatio(0.25);

  std::vector<std::string> generated_files;
  for (int i = 0; i < num_files; i++) {
    std::string file_name = "file" + std::to_string(i) + ".data";
    generated_files.push_back(NewDummyFile(file_name, file_size));
  }

  for (std::string& file_name : generated_files) {
    ASSERT_OK(delete_scheduler_->DeleteFile(file_name, ""));
  }

  // When we end up with 26 files in trash we will start
  // deleting new files immediately
  ASSERT_EQ(fg_delete_file, 74);
  ASSERT_EQ(26, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
  ASSERT_EQ(74, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DeleteSchedulerTest, IsTrashCheck) {
  // Trash files
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("x.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile(".trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("abc.sst.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("/a/b/c/abc..sst.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("log.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("^^^^^.log.trash"));
  ASSERT_TRUE(DeleteScheduler::IsTrashFile("abc.t.trash"));

  // Not trash files
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.sst"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.txt"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("/a/b/c/abc.sst"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("/a/b/c/abc.sstrash"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("^^^^^.trashh"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.ttrash"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile(".ttrash"));
  ASSERT_FALSE(DeleteScheduler::IsTrashFile("abc.trashx"));
}

TEST_F(DeleteSchedulerTest, DeleteAccountedAndUnaccountedFiles) {
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / s
  NewDeleteScheduler();

  // Create 100 files, every file is 1 KB
  int num_files = 100;        // 100 files
  uint64_t file_size = 1024;  // 1 KB as a file size
  std::vector<std::string> generated_files;
  for (int i = 0; i < num_files; i++) {
    std::string file_name = "file" + std::to_string(i) + ".data";
    generated_files.push_back(NewDummyFile(file_name, file_size,
                                           /*dummy_files_dirs_idx*/ 0,
                                           /*track=*/false));
  }

  for (int i = 0; i < num_files; i++) {
    if (i % 2) {
      ASSERT_OK(sst_file_mgr_->OnAddFile(generated_files[i], file_size));
      ASSERT_OK(delete_scheduler_->DeleteFile(generated_files[i], ""));
    } else {
      ASSERT_OK(
          delete_scheduler_->DeleteUnaccountedFile(generated_files[i], ""));
    }
  }

  delete_scheduler_->WaitForEmptyTrash();
  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(0, sst_file_mgr_->GetTotalSize());
}

TEST_F(DeleteSchedulerTest, WaitForEmptyTrashInDirectory) {
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / s
  NewDeleteScheduler();

  const std::string first_file =
      NewDummyFile("first.data", 1024, /*dummy_files_dirs_idx=*/0);
  const std::string second_file =
      NewDummyFile("second.data", 1024, /*dummy_files_dirs_idx=*/1);
  std::string first_dir_alias = dummy_files_dirs_[0];
  first_dir_alias.insert(first_dir_alias.find_last_of('/'), "/");
  std::string second_dir_alias = dummy_files_dirs_[1];
  second_dir_alias.insert(second_dir_alias.find_last_of('/'), "/");
  std::string second_trash_file = second_file;
  second_trash_file.append(".trash");

  std::mutex mutex;
  std::condition_variable cv;
  bool second_delete_started = false;
  bool allow_second_delete = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile::cb", [&](void* arg) {
        const std::string& path = *static_cast<std::string*>(arg);
        if (path != second_trash_file) {
          return;
        }
        std::unique_lock<std::mutex> lock(mutex);
        second_delete_started = true;
        cv.notify_one();
        cv.wait(lock, [&] { return allow_second_delete; });
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(delete_scheduler_->DeleteFile(first_file, first_dir_alias));
  ASSERT_OK(delete_scheduler_->DeleteFile(second_file, dummy_files_dirs_[1]));

  delete_scheduler_->WaitForEmptyTrashInDirectory(dummy_files_dirs_[0]);

  Status second_file_status;
  {
    std::unique_lock<std::mutex> lock(mutex);
    cv.wait(lock, [&] { return second_delete_started; });
    second_file_status = env_->FileExists(second_trash_file);
    allow_second_delete = true;
  }
  cv.notify_one();

  delete_scheduler_->WaitForEmptyTrashInDirectory(second_dir_alias);
  ASSERT_OK(second_file_status);
  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(0, sst_file_mgr_->GetTotalSize());
}

TEST_F(DeleteSchedulerTest, RetryFailedAccountedTrashDeletion) {
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / s
  NewDeleteScheduler();

  constexpr uint64_t file_size = 1024;
  const std::string file = NewDummyFile("retry.data", file_size);
  const std::string trash_file = file + DeleteScheduler::kTrashExtension;
  std::atomic<bool> fail_next_delete{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:BeforeDeleteFile", [&](void* arg) {
        if (fail_next_delete.exchange(false)) {
          *static_cast<Status*>(arg) = Status::IOError("injected delete error");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(delete_scheduler_->DeleteFile(file, dummy_files_dirs_[0],
                                          /*force_bg=*/true));
  delete_scheduler_->WaitForEmptyTrashInDirectory(dummy_files_dirs_[0]);
  ASSERT_FALSE(fail_next_delete);
  ASSERT_OK(env_->FileExists(trash_file));
  ASSERT_EQ(file_size, sst_file_mgr_->GetTotalSize());
  ASSERT_EQ(file_size, delete_scheduler_->GetTotalTrashSize());
  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(1, bg_errors.size());
  for (auto& error : bg_errors) {
    ASSERT_NOK(error.second);
  }

  std::optional<int32_t> bucket = delete_scheduler_->NewTrashBucket();
  ASSERT_TRUE(bucket.has_value());
  ASSERT_OK(sst_file_mgr_->ScheduleExistingTrashFileDeletion(
      trash_file, dummy_files_dirs_[0], /*force_bg=*/true, bucket));
  delete_scheduler_->WaitForEmptyTrashBucket(bucket.value());

  ASSERT_TRUE(env_->FileExists(trash_file).IsNotFound());
  ASSERT_EQ(0, sst_file_mgr_->GetTotalSize());
  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(1, bg_errors.size());
  for (auto& error : bg_errors) {
    ASSERT_NOK(error.second);
  }
}

TEST_F(DeleteSchedulerTest, ImmediateRetryOfVanishedFailedTrashDeletion) {
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / s
  NewDeleteScheduler();

  constexpr uint64_t kept_file_size = 2048;
  NewDummyFile("kept.data", kept_file_size);
  const std::string file = NewDummyFile("retry.data", 1024);
  const std::string trash_file = file + DeleteScheduler::kTrashExtension;
  std::atomic<bool> fail_next_delete{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:BeforeDeleteFile", [&](void* arg) {
        if (fail_next_delete.exchange(false)) {
          *static_cast<Status*>(arg) = Status::IOError("injected delete error");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(delete_scheduler_->DeleteFile(file, dummy_files_dirs_[0],
                                          /*force_bg=*/true));
  delete_scheduler_->WaitForEmptyTrashInDirectory(dummy_files_dirs_[0]);
  ASSERT_OK(env_->FileExists(trash_file));

  ASSERT_OK(env_->DeleteFile(trash_file));
  delete_scheduler_->SetRateBytesPerSecond(0);
  ASSERT_TRUE(sst_file_mgr_
                  ->ScheduleExistingTrashFileDeletion(
                      trash_file, dummy_files_dirs_[0], /*force_bg=*/false)
                  .IsPathNotFound());

  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(kept_file_size, sst_file_mgr_->GetTotalSize());
}

TEST_F(DeleteSchedulerTest, DirSyncFailureAfterTrashFileDeleted) {
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / s
  NewDeleteScheduler();

  constexpr uint64_t kept_file_size = 2048;
  NewDummyFile("kept.data", kept_file_size);
  const std::string file = NewDummyFile("synced.data", 1024);
  const std::string trash_file = file + DeleteScheduler::kTrashExtension;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:BeforeSyncDir", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("injected sync error");
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(delete_scheduler_->DeleteFile(file, dummy_files_dirs_[0],
                                          /*force_bg=*/true));
  delete_scheduler_->WaitForEmptyTrashInDirectory(dummy_files_dirs_[0]);

  ASSERT_TRUE(env_->FileExists(trash_file).IsNotFound());
  auto bg_errors = delete_scheduler_->GetBackgroundErrors();
  ASSERT_EQ(1, bg_errors.size());
  for (auto& error : bg_errors) {
    ASSERT_NOK(error.second);
  }
  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(kept_file_size, sst_file_mgr_->GetTotalSize());
}

TEST_F(DeleteSchedulerTest, ConcurrentDirectoryWaitStress) {
  rate_bytes_per_sec_ = 64 * 1024 * 1024;
  NewDeleteScheduler();

  constexpr int num_rounds = 10;
  constexpr int files_per_directory = 20;
  for (int round = 0; round < num_rounds; ++round) {
    std::vector<std::string> aliases;
    aliases.reserve(dummy_files_dirs_.size());
    for (const std::string& dir : dummy_files_dirs_) {
      std::string alias = dir;
      alias.insert(alias.find_last_of('/'), "/");
      aliases.push_back(std::move(alias));
    }

    for (size_t dir = 0; dir < dummy_files_dirs_.size(); ++dir) {
      for (int file = 0; file < files_per_directory; ++file) {
        const std::string name = "stress_" + std::to_string(round) + "_" +
                                 std::to_string(file) + ".data";
        const std::string path = NewDummyFile(name, 1024, dir);
        const std::string& directory_key =
            (round + static_cast<int>(dir)) % 2 == 0 ? dummy_files_dirs_[dir]
                                                     : aliases[dir];
        ASSERT_OK(delete_scheduler_->DeleteFile(path, directory_key,
                                                /*force_bg=*/true));
      }
    }

    std::vector<port::Thread> waiters;
    waiters.reserve(dummy_files_dirs_.size());
    for (size_t dir = 0; dir < dummy_files_dirs_.size(); ++dir) {
      waiters.emplace_back([&, dir] {
        const std::string& directory_key =
            (round + static_cast<int>(dir)) % 2 == 0 ? aliases[dir]
                                                     : dummy_files_dirs_[dir];
        delete_scheduler_->WaitForEmptyTrashInDirectory(directory_key);
      });
    }
    for (auto& waiter : waiters) {
      waiter.join();
    }
    for (size_t dir = 0; dir < dummy_files_dirs_.size(); ++dir) {
      ASSERT_EQ(0, CountTrashFiles(dir));
    }
  }

  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(0, sst_file_mgr_->GetTotalSize());
}

TEST_F(DeleteSchedulerTest, ConcurrentlyDeleteUnaccountedFilesInBuckets) {
  int bg_delete_file = 0;
  int fg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile", [&](void* /*arg*/) { fg_delete_file++; });
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / s
  NewDeleteScheduler();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Create 1000 files, every file is 1 KB
  int num_files = 1000;
  uint64_t file_size = 1024;  // 1 KB as a file size
  std::vector<std::string> generated_files;
  for (int i = 0; i < num_files; i++) {
    std::string file_name = "file" + std::to_string(i) + ".data";
    generated_files.push_back(NewDummyFile(file_name, file_size,
                                           /*dummy_files_dirs_idx*/ 0,
                                           /*track=*/false));
  }
  // Concurrently delete files in different buckets and check all the buckets
  // are empty.
  int thread_cnt = 10;
  int files_per_thread = 100;
  std::atomic<int> thread_num(0);
  std::vector<port::Thread> threads;
  std::function<void()> delete_thread = [&]() {
    std::optional<int32_t> bucket = delete_scheduler_->NewTrashBucket();
    ASSERT_TRUE(bucket.has_value());
    int idx = thread_num.fetch_add(1);
    int range_start = idx * files_per_thread;
    int range_end = range_start + files_per_thread;
    for (int j = range_start; j < range_end; j++) {
      ASSERT_OK(delete_scheduler_->DeleteUnaccountedFile(
          generated_files[j], "", /*false_bg=*/false, bucket));
    }
    delete_scheduler_->WaitForEmptyTrashBucket(bucket.value());
  };

  threads.reserve(thread_cnt);
  for (int i = 0; i < thread_cnt; i++) {
    threads.emplace_back(delete_thread);
  }

  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }

  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));
  ASSERT_EQ(1000, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
  ASSERT_EQ(0, fg_delete_file);
  ASSERT_EQ(1000, bg_delete_file);

  // OK to re check an already empty bucket
  delete_scheduler_->WaitForEmptyTrashBucket(9);
  // Invalid bucket return too.
  delete_scheduler_->WaitForEmptyTrashBucket(100);
  std::optional<int32_t> next_bucket = delete_scheduler_->NewTrashBucket();
  ASSERT_TRUE(next_bucket.has_value());
  ASSERT_EQ(10, next_bucket.value());
  delete_scheduler_->WaitForEmptyTrashBucket(10);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DeleteSchedulerTest,
       ImmediatelyDeleteUnaccountedFilesWithRemainingLinks) {
  int bg_delete_file = 0;
  int fg_delete_file = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile",
      [&](void* /*arg*/) { bg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteFile", [&](void* /*arg*/) { fg_delete_file++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / sec
  NewDeleteScheduler();

  std::string file1 = NewDummyFile("data_1", 500 * 1024,
                                   /*dummy_files_dirs_idx*/ 0, /*track=*/false);
  std::string file2 = NewDummyFile("data_2", 100 * 1024,
                                   /*dummy_files_dirs_idx*/ 0, /*track=*/false);

  ASSERT_OK(env_->LinkFile(file1, dummy_files_dirs_[0] + "/data_1b"));
  ASSERT_OK(env_->LinkFile(file2, dummy_files_dirs_[0] + "/data_2b"));

  // Should delete in 4 batch if there is no hardlink
  ASSERT_OK(
      delete_scheduler_->DeleteUnaccountedFile(file1, "", /*force_bg=*/false));
  ASSERT_OK(
      delete_scheduler_->DeleteUnaccountedFile(file2, "", /*force_bg=*/false));

  delete_scheduler_->WaitForEmptyTrash();

  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_EQ(0, bg_delete_file);
  ASSERT_EQ(2, fg_delete_file);
  ASSERT_EQ(0, stats_->getAndResetTickerCount(FILES_MARKED_TRASH));
  ASSERT_EQ(2, stats_->getAndResetTickerCount(FILES_DELETED_IMMEDIATELY));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
}

TEST_F(DeleteSchedulerTest, BucketSignalOnSingleFileCompletion) {
  // Test that WaitForEmptyTrashBucket wakes up correctly when a single file
  // in a bucket is deleted. This requires the pre-decrement fix: with
  // post-decrement, the pending count is checked before it reaches zero,
  // so the signal is never fired for the bucket, causing a hang.
  //
  // Strategy:
  // 1. Block the background thread inside DeleteTrashFile for file 0 (mu_ is
  //    NOT held at this point, since it's unlocked before calling
  //    DeleteTrashFile).
  // 2. Start WaitForEmptyTrashBucket(bucket0) on another thread. Since mu_ is
  //    free, it acquires mu_, sees bucket0's pending count == 1, enters
  //    cv_.Wait() (which releases mu_).
  // 3. Unblock file 0's deletion. The background thread completes
  //    DeleteTrashFile, re-acquires mu_, decrements the bucket counter. With
  //    the post-decrement bug, pending_files_in_bucket gets 1 (old value),
  //    so cv_.SignalAll() is NOT called. The wait thread stays stuck.
  // 4. Also block file 1 inside DeleteTrashFile to keep pending_files_ > 0,
  //    preventing the global pending_files_ == 0 check from triggering a
  //    signal.
  // 5. Assert the wait thread times out (bug) or completes (fix).
  rate_bytes_per_sec_ = 1024 * 1024;  // 1 MB / s
  NewDeleteScheduler();

  std::atomic<int> delete_calls{0};
  std::atomic<bool> unblock_file0{false};
  std::atomic<bool> unblock_file1{false};

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DeleteScheduler::DeleteTrashFile:DeleteFile", [&](void* /*arg*/) {
        int n = delete_calls.fetch_add(1) + 1;
        if (n == 1) {
          // Block file 0's deletion until we've set up the wait thread
          while (!unblock_file0.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
        } else if (n == 2) {
          // Block file 1's deletion to keep pending_files_ > 0
          while (!unblock_file1.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Bucket 0: single file
  std::optional<int32_t> bucket0 = delete_scheduler_->NewTrashBucket();
  ASSERT_TRUE(bucket0.has_value());
  std::string file0 =
      NewDummyFile("bucket_signal_file0.data", 1024, 0, /*track=*/false);
  ASSERT_OK(delete_scheduler_->DeleteUnaccountedFile(file0, "",
                                                     /*force_bg=*/false,
                                                     bucket0));

  // Bucket 1: single file (keeps pending_files_ > 0 when bucket 0 empties)
  std::optional<int32_t> bucket1 = delete_scheduler_->NewTrashBucket();
  ASSERT_TRUE(bucket1.has_value());
  std::string file1 =
      NewDummyFile("bucket_signal_file1.data", 1024, 0, /*track=*/false);
  ASSERT_OK(delete_scheduler_->DeleteUnaccountedFile(file1, "",
                                                     /*force_bg=*/false,
                                                     bucket1));

  // Wait for the background thread to reach the DeleteFile syncpoint for
  // file 0. At this point mu_ is NOT held by the background thread.
  while (delete_calls.load() < 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Start waiting for bucket 0 on another thread. Since the background thread
  // doesn't hold mu_, this thread acquires mu_, sees iter->second == 1,
  // and enters cv_.Wait() (releasing mu_).
  std::atomic<bool> wait_done{false};
  auto wait_future = std::async(std::launch::async, [&]() {
    delete_scheduler_->WaitForEmptyTrashBucket(bucket0.value());
    wait_done.store(true);
  });

  // Give the wait thread time to enter cv_.Wait(). We need it to be blocked
  // in cv_.Wait() before unblocking the background thread.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Unblock file 0's deletion. The background thread will:
  // - Complete DeleteTrashFile, re-acquire mu_
  // - Decrement pending_files_ (2 -> 1) and bucket0 counter (1 -> 0)
  // - With bug (post-decrement): pending_files_in_bucket = 1, no signal fired
  // - With fix (pre-decrement): pending_files_in_bucket = 0, signal fired
  // Then it moves to file 1 and blocks at the second DeleteFile syncpoint.
  unblock_file0.store(true);

  // Check if WaitForEmptyTrashBucket returns within 5 seconds.
  // With the bug: no signal fired, wait thread stays stuck -> timeout
  // With the fix: signal fired, wait thread wakes up -> completes quickly
  auto status = wait_future.wait_for(std::chrono::seconds(5));
  ASSERT_EQ(status, std::future_status::ready)
      << "WaitForEmptyTrashBucket(bucket0) timed out - bucket signal not fired";

  // Unblock file 1 and wait for bucket 1
  unblock_file1.store(true);
  delete_scheduler_->WaitForEmptyTrashBucket(bucket1.value());

  ASSERT_EQ(0, delete_scheduler_->GetTotalTrashSize());
  ASSERT_GE(delete_calls.load(), 2);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
