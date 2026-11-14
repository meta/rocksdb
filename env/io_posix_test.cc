// Copyright (c) 2020-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"

#ifdef ROCKSDB_LIB_IO_POSIX
#include "env/io_posix.h"
#include "rocksdb/file_system.h"

#if USE_COROUTINES && FOLLY_HAS_LIBURING
#include <pthread.h>
#include <sched.h>

#include <thread>
#include <vector>

#include "folly/executors/IOThreadPoolExecutor.h"
#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING

namespace ROCKSDB_NAMESPACE {

#if USE_COROUTINES && FOLLY_HAS_LIBURING
class PosixFileSystemTest : public testing::Test {
 protected:
  // Takes `reads` event bases from a thread lowered to SCHED_IDLE, as
  // background reads do, and returns the scheduling policy of each one's
  // thread.
  static std::vector<int> PoliciesAfterIdleFirstReads(
      folly::IOExecutor* read_executor, size_t reads) {
    std::vector<folly::EventBase*> first_reads;
    std::thread idle_reader([&] {
      sched_param param{};
      ASSERT_EQ(0, pthread_setschedparam(pthread_self(), SCHED_IDLE, &param));
      for (size_t i = 0; i < reads; ++i) {
        first_reads.push_back(read_executor->getEventBase());
      }
    });
    idle_reader.join();

    std::vector<int> policies;
    for (auto* event_base : first_reads) {
      int policy = -1;
      event_base->runInEventBaseThreadAndWait(
          [&] { policy = sched_getscheduler(0); });
      policies.push_back(policy);
    }
    return policies;
  }
};

TEST_F(PosixFileSystemTest, OnlyIncreasesReadIOExecutor) {
  auto fs = FileSystem::Default();
  fs->SetReadIOExecutorThreads(1);
  auto* read_executor = fs->GetReadExecutor();
  if (read_executor == nullptr) {
    return;
  }
  auto* thread_pool = dynamic_cast<folly::IOThreadPoolExecutor*>(read_executor);
  ASSERT_NE(thread_pool, nullptr);
  const size_t original_threads = thread_pool->numThreads();

  fs->SetReadIOExecutorThreads(static_cast<int>(original_threads + 1));
  EXPECT_EQ(thread_pool->numThreads(), original_threads + 1);

  fs->SetReadIOExecutorThreads(static_cast<int>(original_threads));
  EXPECT_EQ(thread_pool->numThreads(), original_threads + 1);
}

TEST_F(PosixFileSystemTest, IdleFirstReadKeepsReadIOExecutorThreadsNormal) {
  auto fs = FileSystem::Default();
  fs->SetReadIOExecutorThreads(1);
  auto* read_executor = fs->GetReadExecutor();
  if (read_executor == nullptr) {
    return;
  }
  auto* thread_pool = dynamic_cast<folly::IOThreadPoolExecutor*>(read_executor);
  ASSERT_NE(thread_pool, nullptr);
  const size_t threads = thread_pool->numThreads() + 1;
  fs->SetReadIOExecutorThreads(static_cast<int>(threads));

  EXPECT_EQ(PoliciesAfterIdleFirstReads(read_executor, threads),
            std::vector<int>(threads, SCHED_OTHER));
}

// Run alone, this test's sizing is the one that creates the pool, as a
// process's first DB::Open() is.
TEST_F(PosixFileSystemTest, FirstSizingStartsReadIOExecutorThreads) {
  auto fs = FileSystem::Default();
  fs->SetReadIOExecutorThreads(1);
  auto* read_executor = fs->GetReadExecutor();
  if (read_executor == nullptr) {
    return;
  }

  EXPECT_EQ(PoliciesAfterIdleFirstReads(read_executor, 1),
            std::vector<int>{SCHED_OTHER});
}

#ifndef NDEBUG
TEST_F(PosixFileSystemTest, LogsReadIOExecutorThreadStartFailure) {
  SyncPoint::GetInstance()->SetCallBack(
      "ReadIOExecutorState::StartThreads:Fail",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  auto fs = FileSystem::Default();
  testing::internal::CaptureStderr();
  fs->SetReadIOExecutorThreads(1);
  const std::string logged = testing::internal::GetCapturedStderr();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  auto* read_executor = fs->GetReadExecutor();
  if (read_executor == nullptr) {
    return;
  }

  EXPECT_NE(logged.find("Failed to start async read threads"),
            std::string::npos)
      << logged;
  bool ran = false;
  read_executor->getEventBase()->runInEventBaseThreadAndWait(
      [&] { ran = true; });
  EXPECT_TRUE(ran);
}
#endif  // NDEBUG

#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING

#ifdef OS_LINUX
class LogicalBlockSizeCacheTest : public testing::Test {};

// Tests the caching behavior.
TEST_F(LogicalBlockSizeCacheTest, Cache) {
  int ncall = 0;
  auto get_fd_block_size = [&](int fd) {
    ncall++;
    return fd;
  };
  std::map<std::string, int> dir_fds{
      {"/", 0},
      {"/db", 1},
      {"/db1", 2},
      {"/db2", 3},
  };
  auto get_dir_block_size = [&](const std::string& dir, size_t* size) {
    ncall++;
    *size = dir_fds[dir];
    return Status::OK();
  };
  LogicalBlockSizeCache cache(get_fd_block_size, get_dir_block_size);
  ASSERT_EQ(0, ncall);
  ASSERT_EQ(0, cache.Size());

  ASSERT_EQ(6, cache.GetLogicalBlockSize("/sst", 6));
  ASSERT_EQ(1, ncall);
  ASSERT_EQ(7, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(2, ncall);
  ASSERT_EQ(8, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(3, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/", "/db1/", "/db2"}));
  ASSERT_EQ(3, cache.Size());
  ASSERT_TRUE(cache.Contains("/"));
  ASSERT_TRUE(cache.Contains("/db1"));
  ASSERT_TRUE(cache.Contains("/db2"));
  ASSERT_EQ(6, ncall);
  // Block size for / is cached.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/sst", 6));
  ASSERT_EQ(6, ncall);
  // No cached size for /db.
  ASSERT_EQ(7, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(7, ncall);
  ASSERT_EQ(8, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(8, ncall);
  // Block size for /db1 is cached.
  ASSERT_EQ(2, cache.GetLogicalBlockSize("/db1/sst1", 4));
  ASSERT_EQ(8, ncall);
  ASSERT_EQ(2, cache.GetLogicalBlockSize("/db1/sst2", 5));
  ASSERT_EQ(8, ncall);
  // Block size for /db2 is cached.
  ASSERT_EQ(3, cache.GetLogicalBlockSize("/db2/sst1", 6));
  ASSERT_EQ(8, ncall);
  ASSERT_EQ(3, cache.GetLogicalBlockSize("/db2/sst2", 7));
  ASSERT_EQ(8, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(4, cache.Size());
  ASSERT_TRUE(cache.Contains("/"));
  ASSERT_TRUE(cache.Contains("/db1"));
  ASSERT_TRUE(cache.Contains("/db2"));
  ASSERT_TRUE(cache.Contains("/db"));

  ASSERT_EQ(9, ncall);
  // Block size for /db is cached.
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst1", 7));
  ASSERT_EQ(9, ncall);
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst2", 8));
  ASSERT_EQ(9, ncall);
}

// Tests the reference counting behavior.
TEST_F(LogicalBlockSizeCacheTest, Ref) {
  int ncall = 0;
  auto get_fd_block_size = [&](int fd) {
    ncall++;
    return fd;
  };
  std::map<std::string, int> dir_fds{
      {"/db", 0},
  };
  auto get_dir_block_size = [&](const std::string& dir, size_t* size) {
    ncall++;
    *size = dir_fds[dir];
    return Status::OK();
  };
  LogicalBlockSizeCache cache(get_fd_block_size, get_dir_block_size);

  ASSERT_EQ(0, ncall);

  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst0", 1));
  ASSERT_EQ(1, ncall);

  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(2, ncall);
  ASSERT_EQ(1, cache.GetRefCount("/db"));
  // Block size for /db is cached. Ref count = 1.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/db/sst1", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 2, but won't recompute the cached buffer size.
  ASSERT_OK(cache.RefAndCacheLogicalBlockSize({"/db"}));
  ASSERT_EQ(2, cache.GetRefCount("/db"));
  ASSERT_EQ(2, ncall);

  // Ref count = 1.
  cache.UnrefAndTryRemoveCachedLogicalBlockSize({"/db"});
  ASSERT_EQ(1, cache.GetRefCount("/db"));
  // Block size for /db is still cached.
  ASSERT_EQ(0, cache.GetLogicalBlockSize("/db/sst2", 1));
  ASSERT_EQ(2, ncall);

  // Ref count = 0 and cached buffer size for /db is removed.
  cache.UnrefAndTryRemoveCachedLogicalBlockSize({"/db"});
  ASSERT_EQ(0, cache.Size());
  ASSERT_EQ(1, cache.GetLogicalBlockSize("/db/sst0", 1));
  ASSERT_EQ(3, ncall);
}
#endif

class PosixWritableFileTest : public testing::Test {};

TEST_F(PosixWritableFileTest, SeekAfterTruncate) {
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  std::string path =
      test::PerThreadDBPath("PosixWritableFileTest_SeekAfterTruncate");
  Random rnd(300);
  std::unique_ptr<FSWritableFile> wfile;

  ASSERT_OK(fs->NewWritableFile(path, FileOptions(), &wfile, nullptr));
  ASSERT_OK(wfile->Append(rnd.RandomString(16384), IOOptions(), nullptr));
  ASSERT_OK(wfile->Truncate(4096, IOOptions(), nullptr));
  ASSERT_OK(wfile->Append(rnd.RandomString(4096), IOOptions(), nullptr));
  ASSERT_OK(wfile->Close(IOOptions(), nullptr));
  wfile.reset();

  uint64_t size = 0;
  ASSERT_OK(fs->GetFileSize(path, IOOptions(), &size, nullptr));
  ASSERT_EQ(size, 8192);
  ASSERT_OK(fs->DeleteFile(path, IOOptions(), nullptr));
}

TEST_F(PosixWritableFileTest, SeekAfterExtend) {
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  std::string path =
      test::PerThreadDBPath("PosixWritableFileTest_SeekAfterTruncate");
  Random rnd(300);
  std::unique_ptr<FSWritableFile> wfile;

  ASSERT_OK(fs->NewWritableFile(path, FileOptions(), &wfile, nullptr));
  ASSERT_OK(wfile->Append(rnd.RandomString(4096), IOOptions(), nullptr));
  ASSERT_OK(wfile->Truncate(8192, IOOptions(), nullptr));
  ASSERT_OK(wfile->Append(rnd.RandomString(8192), IOOptions(), nullptr));
  ASSERT_OK(wfile->Close(IOOptions(), nullptr));
  wfile.reset();

  uint64_t size = 0;
  ASSERT_OK(fs->GetFileSize(path, IOOptions(), &size, nullptr));
  ASSERT_EQ(size, 16384);
  ASSERT_OK(fs->DeleteFile(path, IOOptions(), nullptr));
}

#ifdef OS_LINUX
class PosixDirectoryTest : public testing::Test {};

TEST_F(PosixDirectoryTest, BtrfsFsyncFailedOpenDoesNotCloseInvalidFd) {
  // When FsyncWithDirOptions is called with kFileRenamed on a btrfs filesystem
  // and open() fails, close(-1) should not be called. Without the fix,
  // close(-1) is called which is POSIX undefined behavior and overwrites the
  // meaningful open error with a misleading "While closing file after fsync".
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  std::string dir_path =
      test::PerThreadDBPath("PosixDirectoryTest_BtrfsFsyncFailedOpen");
  ASSERT_OK(fs->CreateDirIfMissing(dir_path, IOOptions(), nullptr));

  std::unique_ptr<FSDirectory> dir;
  ASSERT_OK(fs->NewDirectory(dir_path, IOOptions(), &dir, nullptr));

  // Force the btrfs code path via sync point
  SyncPoint::GetInstance()->SetCallBack(
      "PosixDirectory::FsyncWithDirOptions:ForceBtrfs",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Call FsyncWithDirOptions with a non-existent file for rename sync.
  // open() will fail since the file doesn't exist.
  DirFsyncOptions opts(std::string(dir_path + "/nonexistent_file"));
  IOStatus s = dir->FsyncWithDirOptions(IOOptions(), nullptr, opts);

  // Should get an error about open failing, NOT about closing
  ASSERT_TRUE(s.IsIOError());
  // The error message should mention "open", not "closing"
  ASSERT_TRUE(s.ToString().find("open") != std::string::npos);
  ASSERT_TRUE(s.ToString().find("closing") == std::string::npos);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(dir->Close(IOOptions(), nullptr));
  ASSERT_OK(fs->DeleteDir(dir_path, IOOptions(), nullptr));
}
#endif

}  // namespace ROCKSDB_NAMESPACE
#endif

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
