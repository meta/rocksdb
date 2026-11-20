// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the rocksdb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#pragma once
#include <stdint.h>
#include <windows.h>

#include <mutex>
#include <string>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"
#include "util/threadpool_imp.h"

#undef GetCurrentTime
#undef DeleteFile
#undef LoadLibrary

namespace ROCKSDB_NAMESPACE {
namespace port {

// Currently not designed for inheritance but rather a replacement
class WinEnvThreads {
 public:
  explicit WinEnvThreads(Env* hosted_env);

  ~WinEnvThreads();

  WinEnvThreads(const WinEnvThreads&) = delete;
  WinEnvThreads& operator=(const WinEnvThreads&) = delete;

  void Schedule(void (*function)(void*), void* arg, Env::Priority pri,
                void* tag, void (*unschedFunction)(void* arg));

  int UnSchedule(void* arg, Env::Priority pri);

  void StartThread(void (*function)(void* arg), void* arg);

  void WaitForJoin();

  unsigned int GetThreadPoolQueueLen(Env::Priority pri) const;

  int ReserveThreads(int threads_to_be_reserved, Env::Priority pri);

  int ReleaseThreads(int threads_to_be_released, Env::Priority pri);

  static uint64_t gettid();

  uint64_t GetThreadID() const;

  // Allow increasing the number of worker threads.
  void SetBackgroundThreads(int num, Env::Priority pri);
  int GetBackgroundThreads(Env::Priority pri);

  void IncBackgroundThreadsIfNeeded(int num, Env::Priority pri);

 private:
  Env* hosted_env_;
  mutable std::mutex mu_;
  std::vector<ThreadPoolImpl> thread_pools_;
  std::vector<Thread> threads_to_join_;
};

class WinClock : public SystemClock {
 public:
  WinClock();
  virtual ~WinClock() {}

  static const char* kClassName() { return "WindowsClock"; }
  const char* Name() const override { return kDefaultName(); }
  const char* NickName() const override { return kClassName(); }

  uint64_t NowMicros() override;

  uint64_t NowNanos() override;

  // 0 indicates not supported
  uint64_t CPUMicros() override { return 0; }
  void SleepForMicroseconds(int micros) override;

  Status GetCurrentTime(int64_t* unix_time) override;
  // Converts seconds-since-Jan-01-1970 to a printable string
  std::string TimeToString(uint64_t time) override;

  uint64_t GetPerfCounterFrequency() const { return perf_counter_frequency_; }

 private:
  using FnGetSystemTimePreciseAsFileTime = VOID(WINAPI*)(LPFILETIME);

  uint64_t perf_counter_frequency_;
  uint64_t nano_seconds_per_period_;
  FnGetSystemTimePreciseAsFileTime GetSystemTimePreciseAsFileTime_;
};

class WinFileSystem : public FileSystem {
 public:
  static const std::shared_ptr<WinFileSystem>& Default();
  WinFileSystem(const std::shared_ptr<SystemClock>& clock);
  ~WinFileSystem() {}
  static const char* kClassName() { return "WinFS"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kDefaultName(); }

  static size_t GetSectorSize(const std::string& fname);
  size_t GetPageSize() const { return page_size_; }
  size_t GetAllocationGranularity() const { return allocation_granularity_; }

  IOStatus DeleteFile(const std::string& fname, const IOOptions& options,
                      IODebugContext* dbg) override;

  // Truncate the named file to the specified size.
  IOStatus Truncate(const std::string& /*fname*/, size_t /*size*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override;
  IOStatus NewSequentialFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSSequentialFile>* result,
                             IODebugContext* dbg) override;

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* /*dbg*/) override;
  IOStatus NewWritableFile(const std::string& f, const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* r,
                           IODebugContext* dbg) override;
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& options,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override;

  IOStatus NewRandomRWFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSRandomRWFile>* result,
                           IODebugContext* dbg) override;
  IOStatus NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override;

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override;
  IOStatus FileExists(const std::string& f, const IOOptions& io_opts,
                      IODebugContext* dbg) override;
  IOStatus GetChildren(const std::string& dir, const IOOptions& io_opts,
                       std::vector<std::string>* r,
                       IODebugContext* dbg) override;
  IOStatus CreateDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  IOStatus CreateDirIfMissing(const std::string& dirname,
                              const IOOptions& options,
                              IODebugContext* dbg) override;

  // Delete the specified directory.
  IOStatus DeleteDir(const std::string& dirname, const IOOptions& options,
                     IODebugContext* dbg) override;
  // Store the size of fname in *file_size.
  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override;
  // Store the last modification time of fname in *file_mtime.
  IOStatus GetFileModificationTime(const std::string& fname,
                                   const IOOptions& options,
                                   uint64_t* file_mtime,
                                   IODebugContext* dbg) override;
  // Rename file src to target.
  IOStatus RenameFile(const std::string& src, const std::string& target,
                      const IOOptions& options, IODebugContext* dbg) override;

  // Hard Link file src to target.
  IOStatus LinkFile(const std::string& /*src*/, const std::string& /*target*/,
                    const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override;
  IOStatus NumFileLinks(const std::string& /*fname*/,
                        const IOOptions& /*options*/, uint64_t* /*count*/,
                        IODebugContext* /*dbg*/) override;
  IOStatus AreFilesSame(const std::string& /*first*/,
                        const std::string& /*second*/,
                        const IOOptions& /*options*/, bool* /*res*/,
                        IODebugContext* /*dbg*/) override;
  IOStatus LockFile(const std::string& fname, const IOOptions& options,
                    FileLock** lock, IODebugContext* dbg) override;
  IOStatus UnlockFile(FileLock* lock, const IOOptions& options,
                      IODebugContext* dbg) override;
  IOStatus GetTestDirectory(const IOOptions& options, std::string* path,
                            IODebugContext* dbg) override;

  // Create and returns a default logger (an instance of EnvLogger) for storing
  // informational messages. Derived classes can override to provide custom
  // logger.
  IOStatus NewLogger(const std::string& fname, const IOOptions& io_opts,
                     std::shared_ptr<Logger>* result,
                     IODebugContext* dbg) override;
  // Get full directory name for this db.
  IOStatus GetAbsolutePath(const std::string& db_path, const IOOptions& options,
                           std::string* output_path,
                           IODebugContext* dbg) override;
  IOStatus IsDirectory(const std::string& /*path*/, const IOOptions& options,
                       bool* is_dir, IODebugContext* /*dbg*/) override;
  // This seems to clash with a macro on Windows, so #undef it here
#undef GetFreeSpace
  IOStatus GetFreeSpace(const std::string& /*path*/,
                        const IOOptions& /*options*/, uint64_t* /*diskfree*/,
                        IODebugContext* /*dbg*/) override;
  FileOptions OptimizeForLogWrite(const FileOptions& file_options,
                                  const DBOptions& db_options) const override;
  FileOptions OptimizeForManifestRead(
      const FileOptions& file_options) const override;
  FileOptions OptimizeForManifestWrite(
      const FileOptions& file_options) const override;
  void SupportedOps(int64_t& supported_ops) override { supported_ops = 0; }

 protected:
  static uint64_t FileTimeToUnixTime(const FILETIME& ftTime);
  // Returns true iff the named directory exists and is a directory.

  virtual bool DirExists(const std::string& dname);
  // Helper for NewWritable and ReopenWritableFile
  virtual IOStatus OpenWritableFile(const std::string& fname,
                                    const FileOptions& options,
                                    std::unique_ptr<FSWritableFile>* result,
                                    bool reopen);

 private:
  std::shared_ptr<SystemClock> clock_;
  size_t page_size_;
  size_t allocation_granularity_;
};

// Designed for inheritance so can be re-used
// but certain parts replaced
class WinEnvIO {
 public:
  explicit WinEnvIO(Env* hosted_env);

  virtual ~WinEnvIO();

  virtual Status GetHostName(char* name, uint64_t len);

 private:
  Env* hosted_env_;
};

class WinEnv : public CompositeEnv {
 public:
  WinEnv();

  ~WinEnv();
  static const char* kClassName() { return "WinEnv"; }
  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kDefaultName(); }

  Status GetHostName(char* name, uint64_t len) override;

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override;

  void Schedule(void (*function)(void*), void* arg, Env::Priority pri,
                void* tag, void (*unschedFunction)(void* arg)) override;

  int UnSchedule(void* arg, Env::Priority pri) override;

  void StartThread(void (*function)(void* arg), void* arg) override;

  void WaitForJoin() override;

  unsigned int GetThreadPoolQueueLen(Env::Priority pri) const override;

  int ReserveThreads(int threads_to_be_reserved, Env::Priority pri) override;

  int ReleaseThreads(int threads_to_be_released, Env::Priority pri) override;

  uint64_t GetThreadID() const override;

  // Allow increasing the number of worker threads.
  void SetBackgroundThreads(int num, Env::Priority pri) override;
  int GetBackgroundThreads(Env::Priority pri) override;

  void IncBackgroundThreadsIfNeeded(int num, Env::Priority pri) override;

 private:
  WinEnvIO winenv_io_;
  WinEnvThreads winenv_threads_;
};

}  // namespace port
}  // namespace ROCKSDB_NAMESPACE
