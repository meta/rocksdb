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

#include <cstdarg>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/functor_wrapper.h"
#include "rocksdb/port_defs.h"
#include "rocksdb/status.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/types.h"

#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#undef GetCurrentTime
#undef LoadLibrary
#endif

#if defined(__GNUC__) || defined(__clang__)  // ODR-SAFE (essentially)
#define ROCKSDB_PRINTF_FORMAT_ATTR(format_param, dots_param) \
  __attribute__((__format__(__printf__, format_param, dots_param)))
#else
#define ROCKSDB_PRINTF_FORMAT_ATTR(format_param, dots_param)
#endif

namespace ROCKSDB_NAMESPACE {

class DynamicLibrary;
class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class Slice;
struct DataVerificationInfo;
class WritableFile;
class RandomRWFile;
class MemoryMappedFileBuffer;
class Directory;
struct DBOptions;
struct ImmutableDBOptions;
struct MutableDBOptions;
class RateLimiter;
class ThreadStatusUpdater;
struct ThreadStatus;
class FileSystem;
class SystemClock;
struct ConfigOptions;
struct IOOptions;

const size_t kDefaultPageSize = 4 * 1024;

// Options while opening a file to read/write
struct EnvOptions {
  // Construct with default Options
  EnvOptions();

  // Construct from Options
  explicit EnvOptions(const DBOptions& options);

  // If true, then use mmap to read data.
  // Not recommended for 32-bit OS.
  bool use_mmap_reads = false;

  // If true, then use mmap to write data
  bool use_mmap_writes = true;

  // If true, then use O_DIRECT for reading data
  bool use_direct_reads = false;

  // If true, then use O_DIRECT for writing data
  bool use_direct_writes = false;

  // If false, fallocate() calls are bypassed
  bool allow_fallocate = true;

  // If true, set the FD_CLOEXEC on open fd.
  bool set_fd_cloexec = true;

  // Allows OS to incrementally sync files to disk while they are being
  // written, in the background. Issue one request for every bytes_per_sync
  // written. 0 turns it off.
  // Default: 0
  uint64_t bytes_per_sync = 0;

  // When true, guarantees the file has at most `bytes_per_sync` bytes submitted
  // for writeback at any given time.
  //
  //  - If `sync_file_range` is supported it achieves this by waiting for any
  //    prior `sync_file_range`s to finish before proceeding. In this way,
  //    processing (compression, etc.) can proceed uninhibited in the gap
  //    between `sync_file_range`s, and we block only when I/O falls behind.
  //  - Otherwise the `WritableFile::Sync` method is used. Note this mechanism
  //    always blocks, thus preventing the interleaving of I/O and processing.
  //
  // Note: Enabling this option does not provide any additional persistence
  // guarantees, as it may use `sync_file_range`, which does not write out
  // metadata.
  //
  // Default: false
  bool strict_bytes_per_sync = false;

  // If true, we will preallocate the file with FALLOC_FL_KEEP_SIZE flag, which
  // means that file size won't change as part of preallocation.
  // If false, preallocation will also change the file size. This option will
  // improve the performance in workloads where you sync the data on every
  // write. By default, we set it to true for MANIFEST writes and false for
  // WAL writes
  bool fallocate_with_keep_size = true;

  // See DBOptions doc
  size_t compaction_readahead_size = 0;

  // See DBOptions doc
  size_t writable_file_max_buffer_size = 1024 * 1024;

  // If not nullptr, write rate limiting is enabled for flush and compaction
  RateLimiter* rate_limiter = nullptr;
};

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
// An interface that abstracts RocksDB's interactions with the operating system
// environment. There are three main types of APIs:
// 1) File system operations, like creating a file, writing to a file, etc.
// 2) Thread management.
// 3) Misc functions, like getting the current time.
class Env : public Customizable {
 public:
  static const char* kDefaultName() { return "DefaultEnv"; }
  struct FileAttributes {
    // File name
    std::string name;

    // Size of file in bytes
    uint64_t size_bytes;

    // EXPERIMENTAL - only provided by some implementations
    Temperature temperature = Temperature::kUnknown;
  };

  Env();
  // Construct an Env with a separate FileSystem and/or SystemClock
  // implementation
  explicit Env(const std::shared_ptr<FileSystem>& fs);
  Env(const std::shared_ptr<FileSystem>& fs,
      const std::shared_ptr<SystemClock>& clock);
  // No copying allowed
  Env(const Env&) = delete;
  void operator=(const Env&) = delete;
  Env(Env&&) = delete;
  void operator=(Env&&) = delete;

  ~Env() override;

  static const char* Type() { return "Environment"; }

  // Deprecated. Will be removed in a major release. Derived classes
  // should implement this method.
  const char* Name() const override { return ""; }

  // Loads the environment specified by the input value into the result
  // @see Customizable for a more detailed description of the parameters and
  // return codes
  //
  // @param config_options Controls how the environment is loaded.
  // @param value the name and associated properties for the environment.
  // @param result On success, the environment that was loaded.
  // @param guard If specified and the loaded environment is not static,
  //      this value will contain the loaded environment (guard.get() ==
  //      result).
  // @return OK If the environment was successfully loaded (and optionally
  // prepared)
  // @return not-OK if the load failed.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value, Env** result);
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value, Env** result,
                                 std::shared_ptr<Env>* guard);

  // Loads the environment specified by the env and fs uri.
  // If both are specified, an error is returned.
  // Otherwise, the environment is created by loading (via CreateFromString)
  // the appropriate env/fs from the corresponding values.
  static Status CreateFromUri(const ConfigOptions& options,
                              const std::string& env_uri,
                              const std::string& fs_uri, Env** result,
                              std::shared_ptr<Env>* guard);

  // Return a default environment suitable for the current operating
  // system.  Sophisticated users may wish to provide their own Env
  // implementation instead of relying on this default environment.
  //
  // The result of Default() belongs to rocksdb and must never be deleted.
  static Env* Default();

  // See FileSystem::RegisterDbPaths.
  virtual Status RegisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }
  // See FileSystem::UnregisterDbPaths.
  virtual Status UnregisterDbPaths(const std::vector<std::string>& /*paths*/) {
    return Status::OK();
  }

  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) = 0;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) = 0;
  // These values match Linux definition
  // https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/fcntl.h#n56
  enum WriteLifeTimeHint {
    WLTH_NOT_SET = 0,  // No hint information set
    WLTH_NONE,         // No hints about write life time
    WLTH_SHORT,        // Data written has a short life time
    WLTH_MEDIUM,       // Data written has a medium life time
    WLTH_LONG,         // Data written has a long life time
    WLTH_EXTREME,      // Data written has an extremely long life time
  };

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) = 0;

  // Create an object that writes to a file with the specified name.
  // `WritableFile::Append()`s will append after any existing content.  If the
  // file does not already exist, creates it.
  //
  // On success, stores a pointer to the file in *result and returns OK.  On
  // failure stores nullptr in *result and returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status ReopenWritableFile(const std::string& /*fname*/,
                                    std::unique_ptr<WritableFile>* /*result*/,
                                    const EnvOptions& /*options*/) {
    return Status::NotSupported("Env::ReopenWritableFile() not supported.");
  }

  // Reuse an existing file by renaming it and opening it as writable.
  virtual Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options);

  // Open `fname` for random read and write, if file doesn't exist the file
  // will not be created.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewRandomRWFile(const std::string& /*fname*/,
                                 std::unique_ptr<RandomRWFile>* /*result*/,
                                 const EnvOptions& /*options*/) {
    return Status::NotSupported("RandomRWFile is not implemented in this Env");
  }

  // Opens `fname` as a memory-mapped file for read and write (in-place updates
  // only, i.e., no appends). On success, stores a raw buffer covering the whole
  // file in `*result`. The file must exist prior to this call.
  virtual Status NewMemoryMappedFileBuffer(
      const std::string& /*fname*/,
      std::unique_ptr<MemoryMappedFileBuffer>* /*result*/) {
    return Status::NotSupported(
        "MemoryMappedFileBuffer is not implemented in this Env");
  }

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  virtual Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) = 0;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  virtual Status FileExists(const std::string& fname) = 0;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir", and shall never include the
  // names `.` or `..`.
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) = 0;

  // Store in *result the attributes of the children of the specified directory.
  // In case the implementation lists the directory prior to iterating the files
  // and files are concurrently deleted, the deleted files will be omitted from
  // result.
  // The name attributes are relative to "dir", and shall never include the
  // names `.` or `..`.
  // Original contents of *results are dropped.
  // Returns OK if "dir" exists and "*result" contains its children.
  //         NotFound if "dir" does not exist, the calling process does not have
  //                  permission to access "dir", or if "dir" is invalid.
  //         IOError if an IO Error was encountered
  virtual Status GetChildrenFileAttributes(const std::string& dir,
                                           std::vector<FileAttributes>* result);

  // Delete the named file.
  virtual Status DeleteFile(const std::string& fname) = 0;

  // Truncate the named file to the specified size.
  virtual Status Truncate(const std::string& /*fname*/, size_t /*size*/) {
    return Status::NotSupported("Truncate is not supported for this Env");
  }

  // Create the specified directory. Returns error if directory exists.
  virtual Status CreateDir(const std::string& dirname) = 0;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  virtual Status CreateDirIfMissing(const std::string& dirname) = 0;

  // Delete the specified directory.
  // Many implementations of this function will only delete a directory if it is
  // empty.
  virtual Status DeleteDir(const std::string& dirname) = 0;

  // Store the size of fname in *file_size.
  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

  // Store the last modification time of fname in *file_mtime.
  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) = 0;
  // Rename file src to target.
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) = 0;

  // Hard Link file src to target.
  virtual Status LinkFile(const std::string& /*src*/,
                          const std::string& /*target*/) {
    return Status::NotSupported("LinkFile is not supported for this Env");
  }

  // Syncs file data and, when requested, file metadata for `fname`.
  //
  // See FileSystem::SyncFile() for the corresponding path-level sync contract.
  // The default implementation reopens the file as writable, calls Sync() or
  // Fsync(), then closes it.
  virtual Status SyncFile(const std::string& fname, const EnvOptions& options,
                          bool use_fsync);

  virtual Status NumFileLinks(const std::string& /*fname*/,
                              uint64_t* /*count*/) {
    return Status::NotSupported(
        "Getting number of file links is not supported for this Env");
  }

  virtual Status AreFilesSame(const std::string& /*first*/,
                              const std::string& /*second*/, bool* /*res*/) {
    return Status::NotSupported("AreFilesSame is not supported for this Env");
  }

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  virtual Status UnlockFile(FileLock* lock) = 0;

  // Opens `lib_name` as a dynamic library.
  // If the 'search_path' is specified, breaks the path into its components
  // based on the appropriate platform separator (";" or ";") and looks for the
  // library in those directories.  If 'search path is not specified, uses the
  // default library path search mechanism (such as LD_LIBRARY_PATH). On
  // success, stores a dynamic library in `*result`.
  virtual Status LoadLibrary(const std::string& /*lib_name*/,
                             const std::string& /*search_path */,
                             std::shared_ptr<DynamicLibrary>* /*result*/) {
    return Status::NotSupported("LoadLibrary is not implemented in this Env");
  }

  // BOTTOM through READ identify Env-managed thread pools. The READ pool is
  // for work done on behalf of foreground reads; it has no threads until
  // configured and runs at normal CPU and I/O priority unless lowered. USER is
  // not a thread-pool priority; it is used to attribute work performed by user
  // threads in internal statistics. TOTAL is a sentinel.
  enum Priority { BOTTOM, LOW, HIGH, READ, USER, TOTAL };

  static std::string PriorityToString(Priority priority);

  // Priority for requesting bytes in rate limiter scheduler
  enum IOPriority {
    IO_LOW = 0,
    IO_MID = 1,
    IO_HIGH = 2,
    IO_USER = 3,
    IO_TOTAL = 4
  };

  // EXPERIMENTAL
  enum class IOActivity : uint8_t {
    kFlush = 0,
    kCompaction = 1,
    kDBOpen = 2,
    kGet = 3,
    kMultiGet = 4,
    kDBIterator = 5,
    kVerifyDBChecksum = 6,
    kVerifyFileChecksums = 7,
    kGetEntity = 8,
    kMultiGetEntity = 9,
    kGetFileChecksumsFromCurrentManifest = 10,
    // Deferred blob-byte reads performed while resolving a lazy wide-column
    // result (DB::GetEntityLazy / DB::MultiGetEntityLazy), i.e. reads issued by
    // LazyWideColumns::MultiResolve after the originating call returned.
    // Distinct from kGetEntity/kMultiGetEntity, which cover the initial entity
    // read.
    kLazyResolve = 11,
    // Enums after this, up to 0x7F, are reserved for future use for the public
    // RocksDB API (i.e. they should be "non-custom" IO activities). Make sure
    // to also update IOActivityToString when adding new values.

    kCustomIOActivity80 = 0x80,
    kFirstCustomIOActivity = kCustomIOActivity80,
    kCustomIOActivity81 = 0x81,
    kCustomIOActivity82 = 0x82,
    kCustomIOActivity83 = 0x83,
    kCustomIOActivity84 = 0x84,
    kCustomIOActivity85 = 0x85,
    kCustomIOActivity86 = 0x86,
    kCustomIOActivity87 = 0x87,
    kCustomIOActivity88 = 0x88,
    kCustomIOActivity89 = 0x89,
    kCustomIOActivity8A = 0x8A,
    kCustomIOActivity8B = 0x8B,
    kCustomIOActivity8C = 0x8C,
    kCustomIOActivity8D = 0x8D,
    kCustomIOActivity8E = 0x8E,
    kCustomIOActivity8F = 0x8F,
    kCustomIOActivity90 = 0x90,
    kCustomIOActivity91 = 0x91,
    kCustomIOActivity92 = 0x92,
    kCustomIOActivity93 = 0x93,
    kCustomIOActivity94 = 0x94,
    kCustomIOActivity95 = 0x95,
    kCustomIOActivity96 = 0x96,
    kCustomIOActivity97 = 0x97,
    kCustomIOActivity98 = 0x98,
    kCustomIOActivity99 = 0x99,
    kCustomIOActivity9A = 0x9A,
    kCustomIOActivity9B = 0x9B,
    kCustomIOActivity9C = 0x9C,
    kCustomIOActivity9D = 0x9D,
    kCustomIOActivity9E = 0x9E,
    kCustomIOActivity9F = 0x9F,
    kCustomIOActivityA0 = 0xA0,
    kCustomIOActivityA1 = 0xA1,
    kCustomIOActivityA2 = 0xA2,
    kCustomIOActivityA3 = 0xA3,
    kCustomIOActivityA4 = 0xA4,
    kCustomIOActivityA5 = 0xA5,
    kCustomIOActivityA6 = 0xA6,
    kCustomIOActivityA7 = 0xA7,
    kCustomIOActivityA8 = 0xA8,
    kCustomIOActivityA9 = 0xA9,
    kCustomIOActivityAA = 0xAA,
    kCustomIOActivityAB = 0xAB,
    kCustomIOActivityAC = 0xAC,
    kCustomIOActivityAD = 0xAD,
    kCustomIOActivityAE = 0xAE,
    kCustomIOActivityAF = 0xAF,
    kCustomIOActivityB0 = 0xB0,
    kCustomIOActivityB1 = 0xB1,
    kCustomIOActivityB2 = 0xB2,
    kCustomIOActivityB3 = 0xB3,
    kCustomIOActivityB4 = 0xB4,
    kCustomIOActivityB5 = 0xB5,
    kCustomIOActivityB6 = 0xB6,
    kCustomIOActivityB7 = 0xB7,
    kCustomIOActivityB8 = 0xB8,
    kCustomIOActivityB9 = 0xB9,
    kCustomIOActivityBA = 0xBA,
    kCustomIOActivityBB = 0xBB,
    kCustomIOActivityBC = 0xBC,
    kCustomIOActivityBD = 0xBD,
    kCustomIOActivityBE = 0xBE,
    kCustomIOActivityBF = 0xBF,
    kCustomIOActivityC0 = 0xC0,
    kCustomIOActivityC1 = 0xC1,
    kCustomIOActivityC2 = 0xC2,
    kCustomIOActivityC3 = 0xC3,
    kCustomIOActivityC4 = 0xC4,
    kCustomIOActivityC5 = 0xC5,
    kCustomIOActivityC6 = 0xC6,
    kCustomIOActivityC7 = 0xC7,
    kCustomIOActivityC8 = 0xC8,
    kCustomIOActivityC9 = 0xC9,
    kCustomIOActivityCA = 0xCA,
    kCustomIOActivityCB = 0xCB,
    kCustomIOActivityCC = 0xCC,
    kCustomIOActivityCD = 0xCD,
    kCustomIOActivityCE = 0xCE,
    kCustomIOActivityCF = 0xCF,
    kCustomIOActivityD0 = 0xD0,
    kCustomIOActivityD1 = 0xD1,
    kCustomIOActivityD2 = 0xD2,
    kCustomIOActivityD3 = 0xD3,
    kCustomIOActivityD4 = 0xD4,
    kCustomIOActivityD5 = 0xD5,
    kCustomIOActivityD6 = 0xD6,
    kCustomIOActivityD7 = 0xD7,
    kCustomIOActivityD8 = 0xD8,
    kCustomIOActivityD9 = 0xD9,
    kCustomIOActivityDA = 0xDA,
    kCustomIOActivityDB = 0xDB,
    kCustomIOActivityDC = 0xDC,
    kCustomIOActivityDD = 0xDD,
    kCustomIOActivityDE = 0xDE,
    kCustomIOActivityDF = 0xDF,
    kCustomIOActivityE0 = 0xE0,
    kCustomIOActivityE1 = 0xE1,
    kCustomIOActivityE2 = 0xE2,
    kCustomIOActivityE3 = 0xE3,
    kCustomIOActivityE4 = 0xE4,
    kCustomIOActivityE5 = 0xE5,
    kCustomIOActivityE6 = 0xE6,
    kCustomIOActivityE7 = 0xE7,
    kCustomIOActivityE8 = 0xE8,
    kCustomIOActivityE9 = 0xE9,
    kCustomIOActivityEA = 0xEA,
    kCustomIOActivityEB = 0xEB,
    kCustomIOActivityEC = 0xEC,
    kCustomIOActivityED = 0xED,
    kCustomIOActivityEE = 0xEE,
    kCustomIOActivityEF = 0xEF,
    kCustomIOActivityF0 = 0xF0,
    kCustomIOActivityF1 = 0xF1,
    kCustomIOActivityF2 = 0xF2,
    kCustomIOActivityF3 = 0xF3,
    kCustomIOActivityF4 = 0xF4,
    kCustomIOActivityF5 = 0xF5,
    kCustomIOActivityF6 = 0xF6,
    kCustomIOActivityF7 = 0xF7,
    kCustomIOActivityF8 = 0xF8,
    kCustomIOActivityF9 = 0xF9,
    kCustomIOActivityFA = 0xFA,
    kCustomIOActivityFB = 0xFB,
    kCustomIOActivityFC = 0xFC,
    kCustomIOActivityFD = 0xFD,
    kCustomIOActivityFE = 0xFE,
    kLastCustomIOActivity = kCustomIOActivityFE,

    kUnknown = 0xFF,  // Keep last as unknown
  };

  static std::string IOActivityToString(IOActivity activity);

  // Arrange to run "(*function)(arg)" once in a background thread, in
  // the thread pool specified by pri. By default, jobs go to the 'LOW'
  // priority thread pool.

  // "function" may run in an unspecified thread.  Multiple functions
  // added to the same Env may run concurrently in different threads.
  // I.e., the caller may not assume that background work items are
  // serialized.
  // When the UnSchedule function is called, the unschedFunction
  // registered at the time of Schedule is invoked with arg as a parameter.
  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr,
                        void (*unschedFunction)(void* arg) = nullptr) = 0;

  // Arrange to remove jobs for given arg from the queue_ if they are not
  // already scheduled. Caller is expected to have exclusive lock on arg.
  virtual int UnSchedule(void* /*arg*/, Priority /*pri*/) { return 0; }

  // Start a new thread, invoking "function(arg)" within the new thread.
  // When "function(arg)" returns, the thread will be destroyed.
  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  // Start a new thread, invoking "function(args...)" within the new thread.
  // When "function(args...)" returns, the thread will be destroyed.
  template <typename FunctionT, typename... Args>
  void StartThreadTyped(FunctionT function, Args&&... args) {
    using FWType = FunctorWrapper<Args...>;
    StartThread(
        [](void* arg) {
          auto* functor = static_cast<FWType*>(arg);
          functor->invoke();
          delete functor;
        },
        new FWType(std::function<void(Args...)>(function),
                   std::forward<Args>(args)...));
  }

  // Wait for all threads started by StartThread to terminate.
  virtual void WaitForJoin() {}

  // Reserve available background threads in the specified thread pool.
  virtual int ReserveThreads(int /*threads_to_be_reserved*/, Priority /*pri*/) {
    return 0;
  }

  // Release a specific number of reserved threads from the specified thread
  // pool
  virtual int ReleaseThreads(int /*threads_to_be_released*/, Priority /*pri*/) {
    return 0;
  }

  // Get thread pool queue length for specific thread pool.
  virtual unsigned int GetThreadPoolQueueLen(Priority /*pri*/ = LOW) const {
    return 0;
  }

  // *path is set to a temporary directory that can be used for testing. It may
  // or many not have just been created. The directory may or may not differ
  // between runs of the same process, but subsequent calls will return the
  // same directory.
  virtual Status GetTestDirectory(std::string* path) = 0;

  // Create and returns a default logger (an instance of EnvLogger) for storing
  // informational messages. Derived classes can override to provide custom
  // logger.
  virtual Status NewLogger(const std::string& fname,
                           std::shared_ptr<Logger>* result);

  // Returns the number of micro-seconds since some fixed point in time.
  // It is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  virtual uint64_t NowMicros() = 0;

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros.
  // In platform-specific implementations, NowNanos() should return time points
  // that are MONOTONIC.
  virtual uint64_t NowNanos() { return NowMicros() * 1000; }

  // 0 indicates not supported.
  virtual uint64_t NowCPUNanos() { return 0; }

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) = 0;

  // Get the current host name as a null terminated string iff the string
  // length is < len. The hostname should otherwise be truncated to len.
  virtual Status GetHostName(char* name, uint64_t len) = 0;

  // Get the current hostname from the given env as a std::string in result.
  // The result may be truncated if the hostname is too
  // long
  virtual Status GetHostNameString(std::string* result);

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  // Only overwrites *unix_time on success.
  virtual Status GetCurrentTime(int64_t* unix_time) = 0;

  // Get full directory name for this db.
  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) = 0;

  // The number of background worker threads of a specific thread pool
  // for this environment. 'LOW' is the default pool.
  // default number: 1
  virtual void SetBackgroundThreads(int number, Priority pri = LOW) = 0;
  virtual int GetBackgroundThreads(Priority pri = LOW) = 0;

  virtual Status SetAllowNonOwnerAccess(bool /*allow_non_owner_access*/) {
    return Status::NotSupported("Env::SetAllowNonOwnerAccess() not supported.");
  }

  // Enlarge number of background worker threads of a specific thread pool
  // for this environment if it is smaller than specified. 'LOW' is the default
  // pool.
  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) = 0;

  // Lower IO priority for threads from the specified pool.
  virtual void LowerThreadPoolIOPriority(Priority /*pool*/ = LOW) {}

  // Lower CPU priority for threads from the specified pool.
  virtual Status LowerThreadPoolCPUPriority(Priority /*pool*/,
                                            CpuPriority /*pri*/) {
    return Status::NotSupported(
        "Env::LowerThreadPoolCPUPriority(Priority, CpuPriority) not supported");
  }

  // Lower CPU priority for threads from the specified pool.
  virtual void LowerThreadPoolCPUPriority(Priority /*pool*/ = LOW) {}

  // Converts seconds-since-Jan-01-1970 to a printable string
  virtual std::string TimeToString(uint64_t time) = 0;

  // Generates a human-readable unique ID that can be used to identify a DB.
  // In built-in implementations, this is an RFC-4122 UUID string, but might
  // not be in all implementations. Overriding is not recommended.
  // NOTE: this has not be validated for use in cryptography
  virtual std::string GenerateUniqueId();

  // OptimizeForLogWrite will create a new EnvOptions object that is a copy of
  // the EnvOptions in the parameters, but is optimized for reading log files.
  virtual EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const;

  // OptimizeForManifestRead will create a new EnvOptions object that is a copy
  // of the EnvOptions in the parameters, but is optimized for reading manifest
  // files.
  virtual EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const;

  // OptimizeForLogWrite will create a new EnvOptions object that is a copy of
  // the EnvOptions in the parameters, but is optimized for writing log files.
  // Default implementation returns the copy of the same object.
  virtual EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                         const DBOptions& db_options) const;
  // OptimizeForManifestWrite will create a new EnvOptions object that is a copy
  // of the EnvOptions in the parameters, but is optimized for writing manifest
  // files. Default implementation returns the copy of the same object.
  virtual EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const;

  // OptimizeForCompactionTableWrite will create a new EnvOptions object that is
  // a copy of the EnvOptions in the parameters, but is optimized for writing
  // table files.
  virtual EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const;

  // OptimizeForCompactionTableRead will create a new EnvOptions object that
  // is a copy of the EnvOptions in the parameters, but is optimized for reading
  // table files.
  virtual EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const;

  // OptimizeForBlobFileRead will create a new EnvOptions object that
  // is a copy of the EnvOptions in the parameters, but is optimized for reading
  // blob files.
  virtual EnvOptions OptimizeForBlobFileRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const;

  // Returns the status of all threads that belong to the current Env.
  virtual Status GetThreadList(std::vector<ThreadStatus>* /*thread_list*/) {
    return Status::NotSupported("Env::GetThreadList() not supported.");
  }

  // Returns the pointer to ThreadStatusUpdater.  This function will be
  // used in RocksDB internally to update thread status and supports
  // GetThreadList().
  virtual ThreadStatusUpdater* GetThreadStatusUpdater() const {
    return thread_status_updater_;
  }

  // Returns the ID of the current thread.
  virtual uint64_t GetThreadID() const;

// This seems to clash with a macro on Windows, so #undef it here
#undef GetFreeSpace

  // Get the amount of free disk space
  virtual Status GetFreeSpace(const std::string& /*path*/,
                              uint64_t* /*diskfree*/) {
    return Status::NotSupported("Env::GetFreeSpace() not supported.");
  }

  // Check whether the specified path is a directory
  virtual Status IsDirectory(const std::string& /*path*/, bool* /*is_dir*/) {
    return Status::NotSupported("Env::IsDirectory() not supported.");
  }

  virtual void SanitizeEnvOptions(EnvOptions* /*env_opts*/) const {}

  // Get the FileSystem implementation this Env was constructed with. It
  // could be a fully implemented one, or a wrapper class around the Env
  const std::shared_ptr<FileSystem>& GetFileSystem() const;

  // Get the SystemClock implementation this Env was constructed with. It
  // could be a fully implemented one, or a wrapper class around the Env
  const std::shared_ptr<SystemClock>& GetSystemClock() const;

  // If you're adding methods here, remember to add them to EnvWrapper too.

 protected:
  // The pointer to an internal structure that will update the
  // status of each thread.
  ThreadStatusUpdater* thread_status_updater_;

  // Pointer to the underlying FileSystem implementation
  std::shared_ptr<FileSystem> file_system_;

  // Pointer to the underlying SystemClock implementation
  std::shared_ptr<SystemClock> system_clock_;

 private:
  static const size_t kMaxHostNameLen = 256;
};

// The factory function to construct a ThreadStatusUpdater.  Any Env
// that supports GetThreadList() feature should call this function in its
// constructor to initialize thread_status_updater_.
ThreadStatusUpdater* CreateThreadStatusUpdater();

// A file abstraction for reading sequentially through a file
class SequentialFile {
 public:
  SequentialFile() {}
  virtual ~SequentialFile();

  // Read up to "n" bytes from the file.  "scratch[0..n-1]" may be
  // written by this routine.  Sets "*result" to the data that was
  // read (including if fewer than "n" bytes were successfully read).
  // May set "*result" to point at data in "scratch[0..n-1]", so
  // "scratch[0..n-1]" must be live when "*result" is used.
  // If an error was encountered, returns a non-OK status.
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // REQUIRES: External synchronization
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  // Skip "n" bytes from the file. This is guaranteed to be no
  // slower that reading the same data, but may be faster.
  //
  // If end of file is reached, skipping will stop at the end of the
  // file, and Skip will return OK.
  //
  // REQUIRES: External synchronization
  virtual Status Skip(uint64_t n) = 0;

  // Indicates the upper layers if the current SequentialFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual Status InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return Status::NotSupported(
        "SequentialFile::InvalidateCache not supported.");
  }

  // Positioned Read for direct I/O
  // If Direct I/O enabled, offset, n, and scratch should be properly aligned
  virtual Status PositionedRead(uint64_t /*offset*/, size_t /*n*/,
                                Slice* /*result*/, char* /*scratch*/) {
    return Status::NotSupported(
        "SequentialFile::PositionedRead() not supported.");
  }

  // If you're adding methods here, remember to add them to
  // SequentialFileWrapper too.
};

// A read IO request structure for use in MultiRead
struct ReadRequest {
  // File offset in bytes
  uint64_t offset;

  // Length to read in bytes. `result` only returns fewer bytes if end of file
  // is hit (or `status` is not OK).
  size_t len;

  // A buffer that MultiRead()  can optionally place data in. It can
  // ignore this and allocate its own buffer
  char* scratch;

  // Output parameter set by MultiRead() to point to the data buffer, and
  // the number of valid bytes
  Slice result;

  // Status of read
  Status status;
};

// A file abstraction for randomly reading the contents of a file.
class RandomAccessFile {
 public:
  RandomAccessFile() {}
  virtual ~RandomAccessFile();

  // Read up to "n" bytes from the file starting at "offset".
  // "scratch[0..n-1]" may be written by this routine.  Sets "*result"
  // to the data that was read (including if fewer than "n" bytes were
  // successfully read).  May set "*result" to point at data in
  // "scratch[0..n-1]", so "scratch[0..n-1]" must be live when
  // "*result" is used.  If an error was encountered, returns a non-OK
  // status.
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // Safe for concurrent use by multiple threads.
  // If Direct I/O enabled, offset, n, and scratch should be aligned properly.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;

  // Readahead the file starting from offset by n bytes for caching.
  virtual Status Prefetch(uint64_t /*offset*/, size_t /*n*/) {
    return Status::OK();
  }

  // Read a bunch of blocks as described by reqs. The blocks can
  // optionally be read in parallel. This is a synchronous call, i.e it
  // should return after all reads have completed. The reads will be
  // non-overlapping. If the function return Status is not ok, status of
  // individual requests will be ignored and return status will be assumed
  // for all read requests. The function return status is only meant for
  // any errors that occur before even processing specific read requests
  virtual Status MultiRead(ReadRequest* reqs, size_t num_reqs) {
    assert(reqs != nullptr);
    for (size_t i = 0; i < num_reqs; ++i) {
      ReadRequest& req = reqs[i];
      req.status = Read(req.offset, req.len, &req.result, req.scratch);
    }
    return Status::OK();
  }

  // Tries to get an unique ID for this file that will be the same each time
  // the file is opened (and will stay the same while the file is open).
  // Furthermore, it tries to make this ID at most "max_size" bytes. If such an
  // ID can be created this function returns the length of the ID and places it
  // in "id"; otherwise, this function returns 0, in which case "id"
  // may not have been modified.
  //
  // This function guarantees, for IDs from a given environment, two unique ids
  // cannot be made equal to each other by adding arbitrary bytes to one of
  // them. That is, no unique ID is the prefix of another.
  //
  // This function guarantees that the returned ID will not be interpretable as
  // a single varint.
  //
  // Note: these IDs are only valid for the duration of the process.
  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;  // Default implementation to prevent issues with backwards
               // compatibility.
  }

  enum AccessPattern { NORMAL, RANDOM, SEQUENTIAL, WILLNEED, DONTNEED };

  virtual void Hint(AccessPattern /*pattern*/) {}

  // Indicates the upper layers if the current RandomAccessFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  virtual Status InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return Status::NotSupported(
        "RandomAccessFile::InvalidateCache not supported.");
  }

  // The default implementation returns "not supported" so that user
  // implementations of FSRandomAccessFile do not need to immediately implement
  // this function.
  virtual Status GetFileSize(uint64_t* /*result*/) {
    return Status::NotSupported("RandomAccessFile::GetFileSize not supported.");
  }

  // If you're adding methods here, remember to add them to
  // RandomAccessFileWrapper too.
};

// A file abstraction for sequential writing.  The implementation
// must provide buffering since callers may append small fragments
// at a time to the file.
class WritableFile {
 public:
  WritableFile()
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(false) {}

  explicit WritableFile(const EnvOptions& options)
      : last_preallocated_block_(0),
        preallocation_block_size_(0),
        io_priority_(Env::IO_TOTAL),
        write_hint_(Env::WLTH_NOT_SET),
        strict_bytes_per_sync_(options.strict_bytes_per_sync) {}
  // No copying allowed
  WritableFile(const WritableFile&) = delete;
  void operator=(const WritableFile&) = delete;
  WritableFile(WritableFile&&) = delete;
  void operator=(WritableFile&&) = delete;

  // For cases when Close() hasn't been called, many derived classes of
  // WritableFile will need to call Close() non-virtually in their destructor,
  // and ignore the result, to ensure resources are released.
  virtual ~WritableFile();

  // Append data to the end of the file
  // Note: A WritableFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  virtual Status Append(const Slice& data) = 0;

  // Append data with verification information.
  // Note that this API change is experimental and it might be changed in
  // the future. Currently, RocksDB only generates crc32c based checksum for
  // the file writes when the checksum handoff option is set.
  // Expected behavior: if currently ChecksumType::kCRC32C is not supported by
  // WritableFile, the information in DataVerificationInfo can be ignored
  // (i.e. does not perform checksum verification).
  virtual Status Append(const Slice& data,
                        const DataVerificationInfo& /* verification_info */) {
    return Append(data);
  }

  // PositionedAppend data to the specified offset. The new EOF after append
  // must be larger than the previous EOF. This is to be used when writes are
  // not backed by OS buffers and hence has to always start from the start of
  // the sector. The implementation thus needs to also rewrite the last
  // partial sector.
  // Note: PositionAppend does not guarantee moving the file offset after the
  // write. A WritableFile object must support either Append or
  // PositionedAppend, so the users cannot mix the two.
  //
  // PositionedAppend() can only happen on the page/sector boundaries. For that
  // reason, if the last write was an incomplete sector we still need to rewind
  // back to the nearest sector/page and rewrite the portion of it with whatever
  // we need to add. We need to keep where we stop writing.
  //
  // PositionedAppend() can only write whole sectors. For that reason we have to
  // pad with zeros for the last write and trim the file when closing according
  // to the position we keep in the previous step.
  //
  // PositionedAppend() requires aligned buffer to be passed in. The alignment
  // required is queried via GetRequiredBufferAlignment()
  virtual Status PositionedAppend(const Slice& /* data */,
                                  uint64_t /* offset */) {
    return Status::NotSupported(
        "WritableFile::PositionedAppend() not supported.");
  }

  // PositionedAppend data with verification information.
  // Note that this API change is experimental and it might be changed in
  // the future. Currently, RocksDB only generates crc32c based checksum for
  // the file writes when the checksum handoff option is set.
  // Expected behavior: if currently ChecksumType::kCRC32C is not supported by
  // WritableFile, the information in DataVerificationInfo can be ignored
  // (i.e. does not perform checksum verification).
  virtual Status PositionedAppend(
      const Slice& /* data */, uint64_t /* offset */,
      const DataVerificationInfo& /* verification_info */) {
    return Status::NotSupported("PositionedAppend");
  }

  // Truncate is necessary to trim the file to the correct size
  // before closing. It is not always possible to keep track of the file
  // size due to whole pages writes. The behavior is undefined if called
  // with other writes to follow.
  virtual Status Truncate(uint64_t /*size*/) { return Status::OK(); }

  // The caller should call Close() before destroying the WritableFile to
  // surface any errors associated with finishing writes to the file.
  // The file is considered closed regardless of return status.
  // (However, implementations must also clean up properly in the destructor
  // even if Close() is not called.)
  virtual Status Close() = 0;

  // Flush any internally buffered data to the underlying storage, so that
  // the data is no longer dependent on this process's memory. After this
  // call, the data should survive a process crash but is not necessarily
  // persisted to stable storage. Use Sync() for that guarantee.
  // All flushed data must be readable through file access APIs (e.g.
  // RandomAccessFile), though path-level metadata queries such as
  // Env::GetFileSize() might lag on some implementations.
  // Not thread-safe; see IsSyncThreadSafe().
  virtual Status Flush() = 0;

  // Persist data to stable storage. After this call, the data should
  // survive power failures. Does not necessarily persist file metadata
  // (e.g. file size); see Fsync().
  // Sync() implies Flush(): implementations must ensure all internally
  // buffered data is also flushed.
  // Not safe to call concurrently with Append() or Flush() unless
  // IsSyncThreadSafe() returns true.
  virtual Status Sync() = 0;

  /*
   * Persist data and metadata to stable storage.
   * By default, sync only data.
   * Override this method for environments where we need to sync
   * metadata as well.
   */
  virtual Status Fsync() { return Sync(); }

  // true if Sync() and Fsync() are safe to call concurrently with Append()
  // and Flush().
  virtual bool IsSyncThreadSafe() const { return false; }

  // Indicates the upper layers if the current WritableFile implementation
  // uses direct IO.
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  /*
   * If rate limiting is enabled, change the file-granularity priority used in
   * rate-limiting writes.
   *
   * In the presence of finer-granularity priority such as
   * `WriteOptions::rate_limiter_priority`, this file-granularity priority may
   * be overridden by a non-Env::IO_TOTAL finer-granularity priority and used as
   * a fallback for Env::IO_TOTAL finer-granularity priority.
   *
   * If rate limiting is not enabled, this call has no effect.
   */
  virtual void SetIOPriority(Env::IOPriority pri) { io_priority_ = pri; }

  virtual Env::IOPriority GetIOPriority() { return io_priority_; }

  virtual void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
    write_hint_ = hint;
  }

  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return write_hint_; }
  /*
   * Get the size of valid data in the file.
   */
  virtual uint64_t GetFileSize() = 0;

  /*
   * Get and set the default pre-allocation block size for writes to
   * this file.  If non-zero, then Allocate will be used to extend the
   * underlying storage of a file (generally via fallocate) if the Env
   * instance supports it.
   */
  virtual void SetPreallocationBlockSize(size_t size) {
    preallocation_block_size_ = size;
  }

  virtual void GetPreallocationStatus(size_t* block_size,
                                      size_t* last_allocated_block) {
    *last_allocated_block = last_preallocated_block_;
    *block_size = preallocation_block_size_;
  }

  // For documentation, refer to RandomAccessFile::GetUniqueId()
  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;  // Default implementation to prevent issues with backwards
  }

  // Remove any kind of caching of data from the offset to offset+length
  // of this file. If the length is 0, then it refers to the end of file.
  // If the system is not caching the file contents, then this is a noop.
  // This call has no effect on dirty pages in the cache.
  virtual Status InvalidateCache(size_t /*offset*/, size_t /*length*/) {
    return Status::NotSupported("WritableFile::InvalidateCache not supported.");
  }

  // Sync a file range with disk.
  // offset is the starting byte of the file range to be synchronized.
  // nbytes specifies the length of the range to be synchronized.
  // This asks the OS to initiate flushing the cached data to disk,
  // without waiting for completion.
  // Default implementation does nothing.
  virtual Status RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/) {
    if (strict_bytes_per_sync_) {
      return Sync();
    }
    return Status::OK();
  }

  // PrepareWrite performs any necessary preparation for a write
  // before the write actually occurs.  This allows for pre-allocation
  // of space on devices where it can result in less file
  // fragmentation and/or less waste from over-zealous filesystem
  // pre-allocation.
  virtual void PrepareWrite(size_t offset, size_t len) {
    if (preallocation_block_size_ == 0) {
      return;
    }
    // If this write would cross one or more preallocation blocks,
    // determine what the last preallocation block necessary to
    // cover this write would be and Allocate to that point.
    const auto block_size = preallocation_block_size_;
    size_t new_last_preallocated_block =
        (offset + len + block_size - 1) / block_size;
    if (new_last_preallocated_block > last_preallocated_block_) {
      size_t num_spanned_blocks =
          new_last_preallocated_block - last_preallocated_block_;
      // TODO: Don't ignore errors from allocate
      Allocate(block_size * last_preallocated_block_,
               block_size * num_spanned_blocks)
          .PermitUncheckedError();
      last_preallocated_block_ = new_last_preallocated_block;
    }
  }

  // Pre-allocates space for a file.
  virtual Status Allocate(uint64_t /*offset*/, uint64_t /*len*/) {
    return Status::OK();
  }

  enum AccessPattern { kNormal, kSequential, kRandom, kWillNeed, kDontNeed };

  virtual void Hint(AccessPattern /*pattern*/) {}

  // If you're adding methods here, remember to add them to
  // WritableFileWrapper too.

 protected:
  size_t preallocation_block_size() { return preallocation_block_size_; }

 private:
  size_t last_preallocated_block_;
  size_t preallocation_block_size_;

 protected:
  Env::IOPriority io_priority_;
  Env::WriteLifeTimeHint write_hint_;
  const bool strict_bytes_per_sync_;
};

// A file abstraction for random reading and writing.
class RandomRWFile {
 public:
  RandomRWFile() {}
  // No copying allowed
  RandomRWFile(const RandomRWFile&) = delete;
  RandomRWFile& operator=(const RandomRWFile&) = delete;
  RandomRWFile(RandomRWFile&&) = delete;
  RandomRWFile& operator=(RandomRWFile&&) = delete;

  // For cases when Close() hasn't been called, many derived classes of
  // RandomRWFile will need to call Close() non-virtually in their destructor,
  // and ignore the result, to ensure resources are released.
  virtual ~RandomRWFile() {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  virtual bool use_direct_io() const { return false; }

  // Use the returned alignment value to allocate
  // aligned buffer for Direct I/O
  virtual size_t GetRequiredBufferAlignment() const { return kDefaultPageSize; }

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  virtual Status Write(uint64_t offset, const Slice& data) = 0;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  //
  // After call, result->size() < n only if end of file has been
  // reached (or non-OK status). Read might fail if called again after
  // first result->size() < n.
  //
  // Returns Status::OK() on success.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;

  virtual Status Flush() = 0;

  virtual Status Sync() = 0;

  virtual Status Fsync() { return Sync(); }

  // The caller should call Close() before destroying the RandomRWFile to
  // surface any errors associated with finishing writes to the file.
  // The file is considered closed regardless of return status.
  // (However, implementations must also clean up properly in the destructor
  // even if Close() is not called.)
  virtual Status Close() = 0;

  // If you're adding methods here, remember to add them to
  // RandomRWFileWrapper too.
};

// MemoryMappedFileBuffer object represents a memory-mapped file's raw buffer.
// Subclasses should release the mapping upon destruction.
class MemoryMappedFileBuffer {
 public:
  MemoryMappedFileBuffer(void* _base, size_t _length)
      : base_(_base), length_(_length) {}

  virtual ~MemoryMappedFileBuffer() = 0;

  // We do not want to unmap this twice. We can make this class
  // movable if desired, however, since
  MemoryMappedFileBuffer(const MemoryMappedFileBuffer&) = delete;
  MemoryMappedFileBuffer& operator=(const MemoryMappedFileBuffer&) = delete;
  MemoryMappedFileBuffer(MemoryMappedFileBuffer&&) = delete;
  MemoryMappedFileBuffer& operator=(MemoryMappedFileBuffer&&) = delete;

  void* GetBase() const { return base_; }
  size_t GetLen() const { return length_; }

 protected:
  void* base_;
  const size_t length_;
};

// Directory object represents collection of files and implements
// filesystem operations that can be executed on directories.
class Directory {
 public:
  // Many derived classes of Directory will need to call Close() in their
  // destructor, when not called already, to ensure resources are released.
  virtual ~Directory() = default;
  // Fsync directory. Can be called concurrently from multiple threads.
  virtual Status Fsync() = 0;
  // Calling Close() before destroying a Directory is recommended to surface
  // any errors associated with finishing writes (in case of future features).
  // The directory is considered closed regardless of return status.
  virtual Status Close() { return Status::NotSupported("Close"); }

  virtual size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const {
    return 0;
  }

  // If you're adding methods here, remember to add them to
  // DirectoryWrapper too.
};

enum InfoLogLevel : unsigned char {
  DEBUG_LEVEL = 0,
  INFO_LEVEL,
  WARN_LEVEL,
  ERROR_LEVEL,
  FATAL_LEVEL,
  HEADER_LEVEL,
  NUM_INFO_LOG_LEVELS,
};

// An interface for writing log messages.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class Logger {
 public:
  static constexpr size_t kDoNotSupportGetLogFileSize = SIZE_MAX;

  // Set to INFO_LEVEL when RocksDB is compiled in release mode, and
  // DEBUG_LEVEL when compiled in debug mode. See DBOptions::info_log_level.
  static const InfoLogLevel kDefaultLogLevel;

  explicit Logger(const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL)
      : closed_(false), log_level_(log_level) {}
  // No copying allowed
  Logger(const Logger&) = delete;
  void operator=(const Logger&) = delete;
  Logger(Logger&&) = delete;
  void operator=(Logger&&) = delete;

  virtual ~Logger();

  // Because Logger is typically a shared object, Close() may or may not be
  // called before the object is destroyed, but is recommended to reveal any
  // final errors in finishing outstanding writes. No other functions are
  // supported after calling Close(), and the Logger is considered closed
  // regardless of return status.
  virtual Status Close();

  // Write a header to the log file with the specified format
  // It is recommended that you log all header information at the start of the
  // application. But it is not enforced.
  virtual void LogHeader(const char* format, va_list ap) {
    // Default implementation does a simple INFO level log write.
    // Please override as per the logger class requirement.
    Logv(InfoLogLevel::INFO_LEVEL, format, ap);
  }

  // Write an entry to the log file with the specified format.
  //
  // Users who override the `Logv()` overload taking `InfoLogLevel` do not need
  // to implement this, unless they explicitly invoke it in
  // `Logv(InfoLogLevel, ...)`.
  virtual void Logv(const char* /* format */, va_list /* ap */) {
    assert(false);
  }

  // Write an entry to the log file with the specified log level
  // and format.  Any log with level under the internal log level
  // of *this (see @SetInfoLogLevel and @GetInfoLogLevel) will not be
  // printed.
  virtual void Logv(const InfoLogLevel log_level, const char* format,
                    va_list ap);

  virtual size_t GetLogFileSize() const { return kDoNotSupportGetLogFileSize; }
  // Flush to the OS buffers
  virtual void Flush() {}
  virtual InfoLogLevel GetInfoLogLevel() const { return log_level_; }
  virtual void SetInfoLogLevel(const InfoLogLevel log_level) {
    log_level_ = log_level;
  }

  // If you're adding methods here, remember to add them to LoggerWrapper too.

 protected:
  virtual Status CloseImpl();
  bool closed_;

 private:
  InfoLogLevel log_level_;
};

// Identifies a locked file. Except in custom Env/Filesystem implementations,
// the lifetime of a FileLock object should be managed only by LockFile() and
// UnlockFile().
class FileLock {
 public:
  FileLock() {}
  virtual ~FileLock();

 private:
  // No copying allowed
  FileLock(const FileLock&) = delete;
  void operator=(const FileLock&) = delete;
  FileLock(FileLock&&) = delete;
  void operator=(FileLock&&) = delete;
};

class DynamicLibrary {
 public:
  virtual ~DynamicLibrary() = default;

  // Returns the name of the dynamic library.
  virtual const char* Name() const = 0;

  // Loads the symbol for sym_name from the library and updates the input
  // function. Returns the loaded symbol.
  template <typename T>
  Status LoadFunction(const std::string& sym_name, std::function<T>* function) {
    assert(nullptr != function);
    void* ptr = nullptr;
    Status s = LoadSymbol(sym_name, &ptr);
    *function = reinterpret_cast<T*>(ptr);
    return s;
  }
  // Loads and returns the symbol for sym_name from the library.
  virtual Status LoadSymbol(const std::string& sym_name, void** func) = 0;
};

void LogFlush(const std::shared_ptr<Logger>& info_log);

void Log(const InfoLogLevel log_level, const std::shared_ptr<Logger>& info_log,
         const char* format, ...) ROCKSDB_PRINTF_FORMAT_ATTR(3, 4);

// a set of log functions with different log levels.
void Header(const std::shared_ptr<Logger>& info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Debug(const std::shared_ptr<Logger>& info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Info(const std::shared_ptr<Logger>& info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Warn(const std::shared_ptr<Logger>& info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Error(const std::shared_ptr<Logger>& info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Fatal(const std::shared_ptr<Logger>& info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

// Log the specified data to *info_log if info_log is non-nullptr.
// The default info log level is InfoLogLevel::INFO_LEVEL.
void Log(const std::shared_ptr<Logger>& info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

void LogFlush(Logger* info_log);

void Log(const InfoLogLevel log_level, Logger* info_log, const char* format,
         ...) ROCKSDB_PRINTF_FORMAT_ATTR(3, 4);

// The default info log level is InfoLogLevel::INFO_LEVEL.
void Log(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

// a set of log functions with different log levels.
void Header(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Debug(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Info(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Warn(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Error(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);
void Fatal(Logger* info_log, const char* format, ...)
    ROCKSDB_PRINTF_FORMAT_ATTR(2, 3);

// A utility routine: write "data" to the named file.
Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname,
                         bool should_sync = false,
                         const IOOptions* io_options = nullptr);

// A utility routine: read contents of named file into *data
Status ReadFileToString(Env* env, const std::string& fname, std::string* data);

// Below are helpers for wrapping most of the classes in this file.
// They forward all calls to another instance of the class.
// Useful when wrapping the default implementations.
// Typical usage is to inherit your wrapper from *Wrapper, e.g.:
//
// class MySequentialFileWrapper : public
// ROCKSDB_NAMESPACE::SequentialFileWrapper {
//  public:
//   MySequentialFileWrapper(ROCKSDB_NAMESPACE::SequentialFile* target):
//     ROCKSDB_NAMESPACE::SequentialFileWrapper(target) {}
//   Status Read(size_t n, Slice* result, char* scratch) override {
//     cout << "Doing a read of size " << n << "!" << endl;
//     return ROCKSDB_NAMESPACE::SequentialFileWrapper::Read(n, result,
//     scratch);
//   }
//   // All other methods are forwarded to target_ automatically.
// };
//
// This is often more convenient than inheriting the class directly because
// (a) Don't have to override and forward all methods - the Wrapper will
//     forward everything you're not explicitly overriding.
// (b) Don't need to update the wrapper when more methods are added to the
//     rocksdb class. Unless you actually want to override the behavior.
//     (And unless rocksdb people forgot to update the *Wrapper class.)

// An implementation of Env that forwards all calls to another Env.
// May be useful to clients who wish to override just part of the
// functionality of another Env.
class EnvWrapper : public Env {
 public:
  // The Target struct allows an Env to be stored as a raw (Env*) or
  // std::shared_ptr<Env>.  By using this struct, the wrapping/calling
  // class does not need to worry about the ownership/lifetime of the
  // wrapped target env.  If the guard is set, then the Env will point
  // to the guard.get().
  struct Target {
    Env* env;                    // The raw Env
    std::shared_ptr<Env> guard;  // The guarded Env

    // Creates a Target without assuming ownership of the target Env
    explicit Target(Env* t) : env(t) {}

    // Creates a Target from the guarded env, assuming ownership
    explicit Target(std::unique_ptr<Env>&& t) : guard(t.release()) {
      env = guard.get();
    }

    // Creates a Target from the guarded env, assuming ownership
    explicit Target(const std::shared_ptr<Env>& t) : guard(t) {
      env = guard.get();
    }

    // Makes sure the raw Env is not nullptr
    void Prepare() {
      if (guard.get() != nullptr) {
        env = guard.get();
      } else if (env == nullptr) {
        env = Env::Default();
      }
    }
  };

  // Initialize an EnvWrapper that delegates all calls to *t
  explicit EnvWrapper(Env* t);
  explicit EnvWrapper(std::unique_ptr<Env>&& t);
  explicit EnvWrapper(const std::shared_ptr<Env>& t);
  ~EnvWrapper() override;

  // Return the target to which this Env forwards all calls
  Env* target() const { return target_.env; }

  // Deprecated. Will be removed in a major release. Derived classes
  // should implement this method.
  const char* Name() const override { return target_.env->Name(); }

  // The following text is boilerplate that forwards all methods to target()
  Status RegisterDbPaths(const std::vector<std::string>& paths) override {
    return target_.env->RegisterDbPaths(paths);
  }

  Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
    return target_.env->UnregisterDbPaths(paths);
  }

  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& options) override {
    return target_.env->NewSequentialFile(f, r, options);
  }
  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& options) override {
    return target_.env->NewRandomAccessFile(f, r, options);
  }
  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    return target_.env->NewWritableFile(f, r, options);
  }
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override {
    return target_.env->ReopenWritableFile(fname, result, options);
  }
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* r,
                           const EnvOptions& options) override {
    return target_.env->ReuseWritableFile(fname, old_fname, r, options);
  }
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override {
    return target_.env->NewRandomRWFile(fname, result, options);
  }
  Status NewMemoryMappedFileBuffer(
      const std::string& fname,
      std::unique_ptr<MemoryMappedFileBuffer>* result) override {
    return target_.env->NewMemoryMappedFileBuffer(fname, result);
  }
  Status NewDirectory(const std::string& name,
                      std::unique_ptr<Directory>* result) override {
    return target_.env->NewDirectory(name, result);
  }
  Status FileExists(const std::string& f) override {
    return target_.env->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_.env->GetChildren(dir, r);
  }
  Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    return target_.env->GetChildrenFileAttributes(dir, result);
  }
  Status DeleteFile(const std::string& f) override {
    return target_.env->DeleteFile(f);
  }
  Status Truncate(const std::string& fname, size_t size) override {
    return target_.env->Truncate(fname, size);
  }
  Status CreateDir(const std::string& d) override {
    return target_.env->CreateDir(d);
  }
  Status CreateDirIfMissing(const std::string& d) override {
    return target_.env->CreateDirIfMissing(d);
  }
  Status DeleteDir(const std::string& d) override {
    return target_.env->DeleteDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_.env->GetFileSize(f, s);
  }

  Status GetFileModificationTime(const std::string& fname,
                                 uint64_t* file_mtime) override {
    return target_.env->GetFileModificationTime(fname, file_mtime);
  }

  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_.env->RenameFile(s, t);
  }

  Status LinkFile(const std::string& s, const std::string& t) override {
    return target_.env->LinkFile(s, t);
  }

  Status SyncFile(const std::string& fname, const EnvOptions& options,
                  bool use_fsync) override {
    return target_.env->SyncFile(fname, options, use_fsync);
  }

  Status NumFileLinks(const std::string& fname, uint64_t* count) override {
    return target_.env->NumFileLinks(fname, count);
  }

  Status AreFilesSame(const std::string& first, const std::string& second,
                      bool* res) override {
    return target_.env->AreFilesSame(first, second, res);
  }

  Status LockFile(const std::string& f, FileLock** l) override {
    return target_.env->LockFile(f, l);
  }

  Status UnlockFile(FileLock* l) override { return target_.env->UnlockFile(l); }

  Status IsDirectory(const std::string& path, bool* is_dir) override {
    return target_.env->IsDirectory(path, is_dir);
  }

  Status LoadLibrary(const std::string& lib_name,
                     const std::string& search_path,
                     std::shared_ptr<DynamicLibrary>* result) override {
    return target_.env->LoadLibrary(lib_name, search_path, result);
  }

  void Schedule(void (*f)(void* arg), void* a, Priority pri,
                void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
    return target_.env->Schedule(f, a, pri, tag, u);
  }

  int UnSchedule(void* tag, Priority pri) override {
    return target_.env->UnSchedule(tag, pri);
  }

  void StartThread(void (*f)(void*), void* a) override {
    return target_.env->StartThread(f, a);
  }
  void WaitForJoin() override { return target_.env->WaitForJoin(); }
  unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
    return target_.env->GetThreadPoolQueueLen(pri);
  }

  int ReserveThreads(int threads_to_be_reserved, Priority pri) override {
    return target_.env->ReserveThreads(threads_to_be_reserved, pri);
  }

  int ReleaseThreads(int threads_to_be_released, Priority pri) override {
    return target_.env->ReleaseThreads(threads_to_be_released, pri);
  }

  Status GetTestDirectory(std::string* path) override {
    return target_.env->GetTestDirectory(path);
  }
  Status NewLogger(const std::string& fname,
                   std::shared_ptr<Logger>* result) override {
    return target_.env->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return target_.env->NowMicros(); }
  uint64_t NowNanos() override { return target_.env->NowNanos(); }
  uint64_t NowCPUNanos() override { return target_.env->NowCPUNanos(); }

  void SleepForMicroseconds(int micros) override {
    target_.env->SleepForMicroseconds(micros);
  }
  Status GetHostName(char* name, uint64_t len) override {
    return target_.env->GetHostName(name, len);
  }
  Status GetCurrentTime(int64_t* unix_time) override {
    return target_.env->GetCurrentTime(unix_time);
  }
  Status GetAbsolutePath(const std::string& db_path,
                         std::string* output_path) override {
    return target_.env->GetAbsolutePath(db_path, output_path);
  }
  void SetBackgroundThreads(int num, Priority pri) override {
    return target_.env->SetBackgroundThreads(num, pri);
  }
  int GetBackgroundThreads(Priority pri) override {
    return target_.env->GetBackgroundThreads(pri);
  }

  Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override {
    return target_.env->SetAllowNonOwnerAccess(allow_non_owner_access);
  }

  void IncBackgroundThreadsIfNeeded(int num, Priority pri) override {
    return target_.env->IncBackgroundThreadsIfNeeded(num, pri);
  }

  void LowerThreadPoolIOPriority(Priority pool) override {
    target_.env->LowerThreadPoolIOPriority(pool);
  }

  void LowerThreadPoolCPUPriority(Priority pool) override {
    target_.env->LowerThreadPoolCPUPriority(pool);
  }

  Status LowerThreadPoolCPUPriority(Priority pool, CpuPriority pri) override {
    return target_.env->LowerThreadPoolCPUPriority(pool, pri);
  }

  std::string TimeToString(uint64_t time) override {
    return target_.env->TimeToString(time);
  }

  Status GetThreadList(std::vector<ThreadStatus>* thread_list) override {
    return target_.env->GetThreadList(thread_list);
  }

  ThreadStatusUpdater* GetThreadStatusUpdater() const override {
    return target_.env->GetThreadStatusUpdater();
  }

  uint64_t GetThreadID() const override { return target_.env->GetThreadID(); }

  std::string GenerateUniqueId() override {
    return target_.env->GenerateUniqueId();
  }

  EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override {
    return target_.env->OptimizeForLogRead(env_options);
  }
  EnvOptions OptimizeForManifestRead(
      const EnvOptions& env_options) const override {
    return target_.env->OptimizeForManifestRead(env_options);
  }
  EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                 const DBOptions& db_options) const override {
    return target_.env->OptimizeForLogWrite(env_options, db_options);
  }
  EnvOptions OptimizeForManifestWrite(
      const EnvOptions& env_options) const override {
    return target_.env->OptimizeForManifestWrite(env_options);
  }
  EnvOptions OptimizeForCompactionTableWrite(
      const EnvOptions& env_options,
      const ImmutableDBOptions& immutable_ops) const override {
    return target_.env->OptimizeForCompactionTableWrite(env_options,
                                                        immutable_ops);
  }
  EnvOptions OptimizeForCompactionTableRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return target_.env->OptimizeForCompactionTableRead(env_options, db_options);
  }
  EnvOptions OptimizeForBlobFileRead(
      const EnvOptions& env_options,
      const ImmutableDBOptions& db_options) const override {
    return target_.env->OptimizeForBlobFileRead(env_options, db_options);
  }
  Status GetFreeSpace(const std::string& path, uint64_t* diskfree) override {
    return target_.env->GetFreeSpace(path, diskfree);
  }
  void SanitizeEnvOptions(EnvOptions* env_opts) const override {
    target_.env->SanitizeEnvOptions(env_opts);
  }
  Status PrepareOptions(const ConfigOptions& options) override;
  std::string SerializeOptions(const ConfigOptions& config_options,
                               const std::string& header) const override;

 private:
  Target target_;
};

class SequentialFileWrapper : public SequentialFile {
 public:
  explicit SequentialFileWrapper(SequentialFile* target) : target_(target) {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    return target_->Read(n, result, scratch);
  }
  Status Skip(uint64_t n) override { return target_->Skip(n); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    return target_->PositionedRead(offset, n, result, scratch);
  }

 private:
  SequentialFile* target_;
};

class RandomAccessFileWrapper : public RandomAccessFile {
 public:
  explicit RandomAccessFileWrapper(RandomAccessFile* target)
      : target_(target) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    return target_->Read(offset, n, result, scratch);
  }
  Status MultiRead(ReadRequest* reqs, size_t num_reqs) override {
    return target_->MultiRead(reqs, num_reqs);
  }
  Status Prefetch(uint64_t offset, size_t n) override {
    return target_->Prefetch(offset, n);
  }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }
  void Hint(AccessPattern pattern) override { target_->Hint(pattern); }
  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }
  Status GetFileSize(uint64_t* file_size) override {
    return target_->GetFileSize(file_size);
  }

 private:
  RandomAccessFile* target_;
};

class WritableFileWrapper : public WritableFile {
 public:
  explicit WritableFileWrapper(WritableFile* t) : target_(t) {}

  Status Append(const Slice& data) override { return target_->Append(data); }
  Status Append(const Slice& data,
                const DataVerificationInfo& verification_info) override {
    return target_->Append(data, verification_info);
  }
  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return target_->PositionedAppend(data, offset);
  }
  Status PositionedAppend(
      const Slice& data, uint64_t offset,
      const DataVerificationInfo& verification_info) override {
    return target_->PositionedAppend(data, offset, verification_info);
  }
  Status Truncate(uint64_t size) override { return target_->Truncate(size); }
  Status Close() override { return target_->Close(); }
  Status Flush() override { return target_->Flush(); }
  Status Sync() override { return target_->Sync(); }
  Status Fsync() override { return target_->Fsync(); }
  bool IsSyncThreadSafe() const override { return target_->IsSyncThreadSafe(); }

  bool use_direct_io() const override { return target_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }

  void SetIOPriority(Env::IOPriority pri) override {
    target_->SetIOPriority(pri);
  }

  Env::IOPriority GetIOPriority() override { return target_->GetIOPriority(); }

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override {
    target_->SetWriteLifeTimeHint(hint);
  }

  Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return target_->GetWriteLifeTimeHint();
  }

  uint64_t GetFileSize() override { return target_->GetFileSize(); }

  void SetPreallocationBlockSize(size_t size) override {
    target_->SetPreallocationBlockSize(size);
  }

  void GetPreallocationStatus(size_t* block_size,
                              size_t* last_allocated_block) override {
    target_->GetPreallocationStatus(block_size, last_allocated_block);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

  Status InvalidateCache(size_t offset, size_t length) override {
    return target_->InvalidateCache(offset, length);
  }

  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    return target_->RangeSync(offset, nbytes);
  }

  void PrepareWrite(size_t offset, size_t len) override {
    target_->PrepareWrite(offset, len);
  }

  Status Allocate(uint64_t offset, uint64_t len) override {
    return target_->Allocate(offset, len);
  }

  void Hint(WritableFile::AccessPattern pattern) override {
    target_->Hint(pattern);
  }

 private:
  WritableFile* target_;
};

class RandomRWFileWrapper : public RandomRWFile {
 public:
  explicit RandomRWFileWrapper(RandomRWFile* target) : target_(target) {}

  bool use_direct_io() const override { return target_->use_direct_io(); }
  size_t GetRequiredBufferAlignment() const override {
    return target_->GetRequiredBufferAlignment();
  }
  Status Write(uint64_t offset, const Slice& data) override {
    return target_->Write(offset, data);
  }
  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    return target_->Read(offset, n, result, scratch);
  }
  Status Flush() override { return target_->Flush(); }
  Status Sync() override { return target_->Sync(); }
  Status Fsync() override { return target_->Fsync(); }
  Status Close() override { return target_->Close(); }

 private:
  RandomRWFile* target_;
};

class DirectoryWrapper : public Directory {
 public:
  explicit DirectoryWrapper(Directory* target) : target_(target) {}

  Status Fsync() override { return target_->Fsync(); }
  Status Close() override { return target_->Close(); }
  size_t GetUniqueId(char* id, size_t max_size) const override {
    return target_->GetUniqueId(id, max_size);
  }

 private:
  Directory* target_;
};

class LoggerWrapper : public Logger {
 public:
  explicit LoggerWrapper(Logger* target) : target_(target) {}

  Status Close() override { return target_->Close(); }
  void LogHeader(const char* format, va_list ap) override {
    return target_->LogHeader(format, ap);
  }
  void Logv(const char* format, va_list ap) override {
    return target_->Logv(format, ap);
  }
  void Logv(const InfoLogLevel log_level, const char* format,
            va_list ap) override {
    return target_->Logv(log_level, format, ap);
  }
  size_t GetLogFileSize() const override { return target_->GetLogFileSize(); }
  void Flush() override { return target_->Flush(); }
  InfoLogLevel GetInfoLogLevel() const override {
    return target_->GetInfoLogLevel();
  }
  void SetInfoLogLevel(const InfoLogLevel log_level) override {
    return target_->SetInfoLogLevel(log_level);
  }

 private:
  Logger* target_;
};

// Returns a new environment that stores its data in memory and delegates
// all non-file-storage tasks to base_env. The caller must delete the result
// when it is no longer needed.
// *base_env must remain live while the result is in use.
Env* NewMemEnv(Env* base_env);

// Returns a new environment that measures function call times for filesystem
// operations, reporting results to variables in PerfContext.
// This is a factory method for TimedEnv defined in utilities/env_timed.cc.
Env* NewTimedEnv(Env* base_env);

// Returns an instance of logger that can be used for storing informational
// messages.
// This is a factory method for EnvLogger declared in logging/env_logging.h
Status NewEnvLogger(const std::string& fname, Env* env,
                    std::shared_ptr<Logger>* result);

// Creates a new Env based on Env::Default() but modified to use the specified
// FileSystem.
std::unique_ptr<Env> NewCompositeEnv(const std::shared_ptr<FileSystem>& fs);

}  // namespace ROCKSDB_NAMESPACE
