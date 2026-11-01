//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <fcntl.h>

#include "rocksdb/env.h"
#include "rocksdb/status.h"
#ifdef __FreeBSD__
#include <sys/types.h>
#include <sys/wait.h>
#endif
#ifdef __OpenBSD__
#include <sys/wait.h>
#endif
#include <vector>

#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class LockTest : public testing::Test {
 public:
  static LockTest* current_;
  std::string file_;
  ROCKSDB_NAMESPACE::Env* env_;

  LockTest()
      : file_(test::PerThreadDBPath("db_testlock_file")),
        env_(ROCKSDB_NAMESPACE::Env::Default()) {
    current_ = this;
  }

  ~LockTest() override = default;

  Status LockFile(FileLock** db_lock) { return env_->LockFile(file_, db_lock); }

  Status UnlockFile(FileLock* db_lock) { return env_->UnlockFile(db_lock); }

  bool AssertFileIsLocked() {
    return CheckFileLock(/* lock_expected = */ true);
  }

  bool AssertFileIsNotLocked() {
    return CheckFileLock(/* lock_expected = */ false);
  }

  bool CheckFileLock(bool lock_expected) {
    // We need to fork to check the fcntl lock as we need
    // to open and close the file from a different process
    // to avoid either releasing the lock on close, or not
    // contending for it when requesting a lock.

#ifdef OS_WIN

    // WaitForSingleObject and GetExitCodeProcess can do what waitpid does.
    // TODO - implement on Windows
    return true;

#else

    pid_t pid = fork();
    if (0 == pid) {
      // child process
      int exit_val = EXIT_FAILURE;
      int fd = open(file_.c_str(), O_RDWR | O_CREAT, 0644);
      if (fd < 0) {
        // could not open file, could not check if it was locked
        fprintf(stderr, "Open on on file %s failed.\n", file_.c_str());
        exit(exit_val);
      }

      struct flock f;
      memset(&f, 0, sizeof(f));
      f.l_type = (F_WRLCK);
      f.l_whence = SEEK_SET;
      f.l_start = 0;
      f.l_len = 0;  // Lock/unlock entire file
      int value = fcntl(fd, F_SETLK, &f);
      if (value == -1) {
        if (lock_expected) {
          exit_val = EXIT_SUCCESS;
        }
      } else {
        if (!lock_expected) {
          exit_val = EXIT_SUCCESS;
        }
      }
      close(fd);  // lock is released for child process
      exit(exit_val);
    } else if (pid > 0) {
      // parent process
      int status;
      while (-1 == waitpid(pid, &status, 0)) {
      }
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        // child process exited with non success status
        return false;
      } else {
        return true;
      }
    } else {
      fprintf(stderr, "Fork failed\n");
      return false;
    }

#endif
  }
};
LockTest* LockTest::current_;

TEST_F(LockTest, LockBySameThread) {
  FileLock* lock1;
  FileLock* lock2;

  // acquire a lock on a file
  ASSERT_OK(LockFile(&lock1));

  // check the file is locked
  ASSERT_TRUE(AssertFileIsLocked());

  // re-acquire the lock on the same file. This should fail.
  Status s = LockFile(&lock2);
  ASSERT_TRUE(s.IsIOError());
#ifndef OS_WIN
  // Validate that error message contains current thread ID.
  ASSERT_TRUE(s.ToString().find(std::to_string(
                  Env::Default()->GetThreadID())) != std::string::npos);
#endif

  // check the file is locked
  ASSERT_TRUE(AssertFileIsLocked());

  // release the lock
  ASSERT_OK(UnlockFile(lock1));

  // check the file is not locked
  ASSERT_TRUE(AssertFileIsNotLocked());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
