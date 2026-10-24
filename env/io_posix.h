//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <errno.h>
#if defined(ROCKSDB_IOURING_PRESENT)
#include <liburing.h>
#include <pthread.h>
#include <sys/uio.h>

#include <cstdio>

#include "util/string_util.h"

// Compatibility defines for io_uring flags that may not be present in older
// kernel headers. These values are fixed and won't change, so it's safe to
// define them even if the running kernel doesn't support them.
#ifndef IORING_SETUP_SINGLE_ISSUER
#define IORING_SETUP_SINGLE_ISSUER (1U << 12)
#endif
#ifndef IORING_SETUP_DEFER_TASKRUN
#define IORING_SETUP_DEFER_TASKRUN (1U << 13)
#endif
#endif
#if !defined(OS_WIN)
#include <unistd.h>
#endif

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>

#include "port/lang.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"

#if USE_COROUTINES
#include "folly/io/async/Liburing.h"

namespace folly {
class EventBaseManager;
}  // namespace folly
#endif  // USE_COROUTINES

// For non linux platform, the following macros are used only as place
// holder.
#if !(defined OS_LINUX) && !(defined OS_FREEBSD) && !(defined CYGWIN) && \
    !(defined OS_AIX) && !(defined OS_ANDROID)
#define POSIX_FADV_NORMAL 0     /* [MC1] no further special treatment */
#define POSIX_FADV_RANDOM 1     /* [MC1] expect random page refs */
#define POSIX_FADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define POSIX_FADV_WILLNEED 3   /* [MC1] will need these pages */
#define POSIX_FADV_DONTNEED 4   /* [MC1] don't need these pages */

#define POSIX_MADV_NORMAL 0     /* [MC1] no further special treatment */
#define POSIX_MADV_RANDOM 1     /* [MC1] expect random page refs */
#define POSIX_MADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define POSIX_MADV_WILLNEED 3   /* [MC1] will need these pages */
#define POSIX_MADV_DONTNEED 4   /* [MC1] don't need these pages */
#endif

namespace ROCKSDB_NAMESPACE {
std::string IOErrorMsg(const std::string& context,
                       const std::string& file_name);
// file_name can be left empty if it is not unkown.
IOStatus IOError(const std::string& context, const std::string& file_name,
                 int err_number);
#if USE_COROUTINES && FOLLY_HAS_LIBURING
void SetCurrentThreadReadIOUringBackendAvailable();
bool IsCurrentThreadReadIOUringBackendAvailable();
#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING
#if !defined(OS_WIN)
bool PosixPositionedWrite(int fd, const char* buf, size_t nbyte, off_t offset);
#endif

// SyncPoint payload used by deterministic TSAN regression tests to observe
// which virtual address range a freshly created mapping occupies.
struct TsanMappedMemoryInfo {
  const void* addr;
  size_t size;
};

#ifdef __SANITIZE_THREAD__
extern "C" void AnnotateNewMemory(const char* file, int line,
                                  const volatile void* mem, long size);
#endif  // __SANITIZE_THREAD__

inline void TsanAnnotateMappedMemory(const volatile void* mem, size_t size) {
  TsanMappedMemoryInfo info{const_cast<const void*>(mem), size};
  TEST_SYNC_POINT_CALLBACK("TsanAnnotateMappedMemory", &info);
  (void)info;
#ifdef __SANITIZE_THREAD__
  if (mem != nullptr && size != 0) {
    // TSAN does not understand that a new mmap or io_uring setup can legally
    // reuse a virtual address from an unrelated, previously unmapped region.
    // Reset the shadow state as soon as the new mapping exists so later
    // accesses are not reported against stale accesses from the old mapping.
    AnnotateNewMemory(__FILE__, __LINE__, mem, static_cast<long>(size));
  }
#endif  // __SANITIZE_THREAD__
}

class PosixHelper {
 public:
  static const std::string& GetLogicalBlockSizeFileName() {
    static const std::string kLogicalBlockSizeFileName = "logical_block_size";
    return kLogicalBlockSizeFileName;
  }
  static const std::string& GetMaxSectorsKBFileName() {
    static const std::string kMaxSectorsKBFileName = "max_sectors_kb";
    return kMaxSectorsKBFileName;
  }
  static size_t GetUniqueIdFromFile(int fd, char* id, size_t max_size);
  static size_t GetLogicalBlockSizeOfFd(int fd);
  static Status GetLogicalBlockSizeOfDirectory(const std::string& directory,
                                               size_t* size);

  static Status GetMaxSectorsKBOfDirectory(const std::string& directory,
                                           size_t* kb);

 private:
  static const size_t kDefaultMaxSectorsKB = 2 * 1024;

  static size_t GetMaxSectorsKBOfFd(int fd);

  // Return the value in the specified `file_name` under
  // `/sys/block/xxx/queue/` for the device where the file of `fd` is on.
  // If not found, then return the specified `default_return_value`
  static size_t GetQueueSysfsFileValueOfFd(int fd, const std::string& file_name,
                                           size_t default_return_value);

  /// Return the value in the specified `file_name` under
  // `/sys/block/xxx/queue/` for the device where `directory` is on.
  // If not found, then return the specified `default_return_value`
  static Status GetQueueSysfsFileValueofDirectory(const std::string& directory,
                                                  const std::string& file_name,
                                                  size_t* value);
};

/*
 * DirectIOHelper
 */
inline bool IsSectorAligned(const size_t off, size_t sector_size) {
  assert((sector_size & (sector_size - 1)) == 0);
  return (off & (sector_size - 1)) == 0;
}

#ifndef NDEBUG
inline bool IsSectorAligned(const void* ptr, size_t sector_size) {
  return uintptr_t(ptr) % sector_size == 0;
}
#endif

#if defined(ROCKSDB_IOURING_PRESENT)
struct Posix_IOHandle {
  Posix_IOHandle(struct io_uring* _iu,
                 std::function<void(FSReadRequest&, void*)> _cb, void* _cb_arg,
                 uint64_t _offset, size_t _len, char* _scratch,
                 bool _use_direct_io, size_t _alignment)
      : iu(_iu),
        cb(_cb),
        cb_arg(_cb_arg),
        offset(_offset),
        len(_len),
        scratch(_scratch),
        use_direct_io(_use_direct_io),
        alignment(_alignment),
        is_finished(false),
        is_being_aborted(false),
        req_count(0) {}

  struct iovec iov;
  struct io_uring* iu;
  std::function<void(FSReadRequest&, void*)> cb;
  void* cb_arg;
  uint64_t offset;
  size_t len;
  char* scratch;
  bool use_direct_io;
  size_t alignment;
  bool is_finished;
  // is_being_aborted is set by AbortIO when a cancel request is submitted.
  // Used to distinguish between aborted handles (expect 2 completions) and
  // non-aborted handles (expect 1 completion) when processing completions.
  bool is_being_aborted;
  // req_count is used by AbortIO API to keep track of number of requests.
  uint32_t req_count;
};

inline void UpdateResult(struct io_uring_cqe* cqe, const std::string& file_name,
                         size_t len, size_t iov_len, bool async_read,
                         bool use_direct_io, size_t alignment,
                         size_t& finished_len, FSReadRequest* req,
                         size_t& bytes_read, bool& read_again) {
  read_again = false;
  if (cqe->res < 0) {
    req->result = Slice(req->scratch, 0);
    req->status = IOError("Req failed", file_name, cqe->res);
  } else {
    bytes_read = static_cast<size_t>(cqe->res);
    TEST_SYNC_POINT_CALLBACK("UpdateResults::io_uring_result", &bytes_read);
    if (bytes_read == iov_len) {
      req->result = Slice(req->scratch, req->len);
      req->status = IOStatus::OK();
    } else if (bytes_read == 0) {
      /// cqe->res == 0 can means EOF, or can mean partial results. See
      // comment
      // https://github.com/facebook/rocksdb/pull/6441#issuecomment-589843435
      // Fall back to pread in this case.
      if (use_direct_io && !IsSectorAligned(finished_len, alignment)) {
        // Bytes reads don't fill sectors. Should only happen at the end
        // of the file.
        req->result = Slice(req->scratch, finished_len);
        req->status = IOStatus::OK();
      } else {
        if (async_read) {
          // No  bytes read. It can means EOF. In case of partial results, it's
          // caller responsibility to call read/readasync again.
          req->result = Slice(req->scratch, 0);
          req->status = IOStatus::OK();
        } else {
          read_again = true;
        }
      }
    } else if (bytes_read < iov_len) {
      assert(bytes_read > 0);
      if (async_read) {
        req->result = Slice(req->scratch, bytes_read);
        req->status = IOStatus::OK();
      } else {
        assert(bytes_read + finished_len < len);
        finished_len += bytes_read;
      }
    } else {
      req->result = Slice(req->scratch, 0);
      req->status = IOError("Req returned more bytes than requested", file_name,
                            cqe->res);
    }
  }
#ifdef NDEBUG
  (void)len;
#endif
}

// Finalize a completed async read request.
// Processes the CQE result, marks the handle as finished, and invokes the
// callback. This is shared between Poll and AbortIO (for non-aborted handles).
inline void FinalizeAsyncRead(struct io_uring* iu, struct io_uring_cqe* cqe,
                              Posix_IOHandle* posix_handle) {
  FSReadRequest req;
  req.scratch = posix_handle->scratch;
  req.offset = posix_handle->offset;
  req.len = posix_handle->len;

  size_t finished_len = 0;
  size_t bytes_read = 0;
  bool read_again = false;
  UpdateResult(cqe, "", req.len, posix_handle->iov.iov_len, true /*async_read*/,
               posix_handle->use_direct_io, posix_handle->alignment,
               finished_len, &req, bytes_read, read_again);
  posix_handle->is_finished = true;
  io_uring_cqe_seen(iu, cqe);
  posix_handle->cb(req, posix_handle->cb_arg);
}
#endif

#ifdef OS_LINUX
// Files under a specific directory have the same logical block size.
// This class caches the logical block size for the specified directories to
// save the CPU cost of computing the size.
// Safe for concurrent access from multiple threads without any external
// synchronization.
class LogicalBlockSizeCache {
 public:
  LogicalBlockSizeCache(
      std::function<size_t(int)> get_logical_block_size_of_fd =
          PosixHelper::GetLogicalBlockSizeOfFd,
      std::function<Status(const std::string&, size_t*)>
          get_logical_block_size_of_directory =
              PosixHelper::GetLogicalBlockSizeOfDirectory)
      : get_logical_block_size_of_fd_(get_logical_block_size_of_fd),
        get_logical_block_size_of_directory_(
            get_logical_block_size_of_directory) {}

  // Takes the following actions:
  // 1. Increases reference count of the directories;
  // 2. If the directory's logical block size is not cached,
  //    compute the buffer size and cache the result.
  Status RefAndCacheLogicalBlockSize(
      const std::vector<std::string>& directories);

  // Takes the following actions:
  // 1. Decreases reference count of the directories;
  // 2. If the reference count of a directory reaches 0, remove the directory
  //    from the cache.
  void UnrefAndTryRemoveCachedLogicalBlockSize(
      const std::vector<std::string>& directories);

  // Returns the logical block size for the file.
  //
  // If the file is under a cached directory, return the cached size.
  // Otherwise, the size is computed.
  size_t GetLogicalBlockSize(const std::string& fname, int fd);

  int GetRefCount(const std::string& dir) {
    ReadLock lock(&cache_mutex_);
    auto it = cache_.find(dir);
    if (it == cache_.end()) {
      return 0;
    }
    return it->second.ref;
  }

  size_t Size() const { return cache_.size(); }

  bool Contains(const std::string& dir) {
    ReadLock lock(&cache_mutex_);
    return cache_.find(dir) != cache_.end();
  }

 private:
  struct CacheValue {
    CacheValue() : size(0), ref(0) {}

    // Logical block size of the directory.
    size_t size;
    // Reference count of the directory.
    int ref;
  };

  std::function<size_t(int)> get_logical_block_size_of_fd_;
  std::function<Status(const std::string&, size_t*)>
      get_logical_block_size_of_directory_;

  std::map<std::string, CacheValue> cache_;
  port::RWMutex cache_mutex_;
};
#endif

class PosixSequentialFile : public FSSequentialFile {
 private:
  std::string filename_;
  FILE* file_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* file, int fd,
                      size_t logical_block_size, const EnvOptions& options);
  ~PosixSequentialFile() override;

  IOStatus Read(size_t n, const IOOptions& opts, Slice* result, char* scratch,
                IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& opts,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  IOStatus Skip(uint64_t n) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  bool use_direct_io() const override { return use_direct_io_; }
  size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

#if defined(ROCKSDB_IOURING_PRESENT)
// io_uring instance queue depth
const unsigned int kIoUringDepth = 256;

inline void TsanAnnotateIOUringMemory(struct io_uring* iu) {
  TsanAnnotateMappedMemory(iu->sq.ring_ptr, iu->sq.ring_sz);
  // CQ and SQ can share the same mmap region.
  if (iu->cq.ring_ptr != iu->sq.ring_ptr) {
    TsanAnnotateMappedMemory(iu->cq.ring_ptr, iu->cq.ring_sz);
  }
  TsanAnnotateMappedMemory(
      iu->sq.sqes, static_cast<size_t>(kIoUringDepth) * sizeof(io_uring_sqe));
}

inline void DeleteIOUring(void* p) {
  struct io_uring* iu = static_cast<struct io_uring*>(p);
  io_uring_queue_exit(iu);
  delete iu;
}

// Creates a new io_uring instance for the calling thread.
//
// On failure, returns nullptr and, if err_out is non-null, sets *err_out to
// the positive errno value returned by io_uring_queue_init.  Callers can use
// this to distinguish resource-exhaustion failures (ENOMEM — memlock limit
// hit) from "not supported by kernel" failures (ENOSYS/EPERM).
//
// Common failure reasons:
//   ENOMEM  — the process has exhausted its locked-memory quota
//             (RLIMIT_MEMLOCK).  Raise the limit or reduce the number of
//             concurrent io_uring rings.
//   ENOSYS  — the kernel does not support io_uring.
//   EPERM   — the process lacks the required capability.
inline struct io_uring* CreateIOUring(int* err_out = nullptr) {
  struct io_uring* new_io_uring = new struct io_uring;
  unsigned int flags = 0;
  flags |= IORING_SETUP_SINGLE_ISSUER;
  flags |= IORING_SETUP_DEFER_TASKRUN;
  int ret = io_uring_queue_init(kIoUringDepth, new_io_uring, flags);
  if (ret) {
    int err = -ret;  // liburing returns negative errno
    if (err_out) {
      *err_out = err;
    }
    if (err == ENOMEM) {
      fprintf(stderr,
              "CreateIOUring failed: %s (errno=%d) — memlock limit may be too"
              " low; consider raising RLIMIT_MEMLOCK. thread=%lu\n",
              errnoStr(err).c_str(), err,
              static_cast<unsigned long>(pthread_self()));
    } else {
      fprintf(stderr, "CreateIOUring failed: %s (errno=%d), thread=%lu\n",
              errnoStr(err).c_str(), err,
              static_cast<unsigned long>(pthread_self()));
    }
    delete new_io_uring;
    new_io_uring = nullptr;
  } else {
    TsanAnnotateIOUringMemory(new_io_uring);
  }
  return new_io_uring;
}
#endif  // defined(ROCKSDB_IOURING_PRESENT)

class PosixRandomAccessFile : public FSRandomAccessFile {
 protected:
  std::string filename_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;
#if USE_COROUTINES && FOLLY_HAS_LIBURING
  folly::EventBaseManager* read_event_base_manager_;
#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING
#if defined(ROCKSDB_IOURING_PRESENT)
  ThreadLocalPtr* thread_local_async_read_io_urings_;
  ThreadLocalPtr* thread_local_multi_read_io_urings_;
#endif

 public:
  PosixRandomAccessFile(const std::string& fname, int fd,
                        size_t logical_block_size, const FileOptions& options
#if USE_COROUTINES && FOLLY_HAS_LIBURING
                        ,
                        folly::EventBaseManager* read_event_base_manager
#endif
#if defined(ROCKSDB_IOURING_PRESENT)
                        ,
                        ThreadLocalPtr* thread_local_async_read_io_urings,
                        ThreadLocalPtr* thread_local_multi_read_io_urings
#endif
  );
  ~PosixRandomAccessFile() override;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts, Slice* result,
                char* scratch, IODebugContext* dbg) const override;

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& opts,
                    IODebugContext* dbg) override;

#if defined(OS_LINUX) || defined(OS_MACOSX) || defined(OS_AIX)
  size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
  void Hint(AccessPattern pattern) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  bool use_direct_io() const override { return use_direct_io_; }
  size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;

#if USE_COROUTINES && FOLLY_HAS_LIBURING
  bool SubmitReadAsync(FSReadRequest& req, const IOOptions& opts,
                       std::function<void(FSReadRequest&)> cb,
                       IODebugContext* dbg) override;
#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING

  IOStatus GetFileSize(uint64_t* result) override;
};

class PosixWritableFile : public FSWritableFile {
 protected:
  const std::string filename_;
  const bool use_direct_io_;
  int fd_;
  uint64_t filesize_;
  size_t logical_sector_size_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;
  bool fallocate_with_keep_size_;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
  // Even if the syscall is present, the filesystem may still not properly
  // support it, so we need to do a dynamic check too.
  bool sync_file_range_supported_;
#endif  // ROCKSDB_RANGESYNC_PRESENT

 public:
  explicit PosixWritableFile(const std::string& fname, int fd,
                             size_t logical_block_size,
                             const EnvOptions& options,
                             uint64_t initial_file_size);
  ~PosixWritableFile() override;

  // Need to implement this so the file is truncated correctly
  // with direct I/O
  IOStatus Truncate(uint64_t size, const IOOptions& opts,
                    IODebugContext* dbg) override;
  IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  IODebugContext* dbg) override;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  const DataVerificationInfo& /* verification_info */,
                  IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& opts,
                            IODebugContext* dbg) override;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& opts,
                            const DataVerificationInfo& /* verification_info */,
                            IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }
  IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  bool IsSyncThreadSafe() const override;
  bool use_direct_io() const override { return use_direct_io_; }
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;
  uint64_t GetFileSize(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
#ifdef ROCKSDB_FALLOCATE_PRESENT
  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& opts,
                    IODebugContext* dbg) override;
#endif
  IOStatus RangeSync(uint64_t offset, uint64_t nbytes, const IOOptions& opts,
                     IODebugContext* dbg) override;
#ifdef OS_LINUX
  size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
};

// mmap() based random-access
class PosixMmapReadableFile : public FSRandomAccessFile {
 private:
  int fd_;
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  PosixMmapReadableFile(const int fd, const std::string& fname, void* base,
                        size_t length, const EnvOptions& options);
  ~PosixMmapReadableFile() override;
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts, Slice* result,
                char* scratch, IODebugContext* dbg) const override;
  void Hint(AccessPattern pattern) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
  IOStatus GetFileSize(uint64_t* result) override;
};

class PosixMmapFile : public FSWritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;  // If false, fallocate calls are bypassed
  bool fallocate_with_keep_size_;
#endif

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  IOStatus MapNewRegion();
  IOStatus UnmapCurrentRegion();
  IOStatus Msync();

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                const EnvOptions& options, uint64_t initial_file_size);
  ~PosixMmapFile() override;

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }
  IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  IODebugContext* dbg) override;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  const DataVerificationInfo& /* verification_info */,
                  IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  uint64_t GetFileSize(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus InvalidateCache(size_t offset, size_t length) override;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& opts,
                    IODebugContext* dbg) override;
#endif
};

class PosixRandomRWFile : public FSRandomRWFile {
 public:
  explicit PosixRandomRWFile(const std::string& fname, int fd,
                             const EnvOptions& options);
  ~PosixRandomRWFile() override;

  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& opts,
                 IODebugContext* dbg) override;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts, Slice* result,
                char* scratch, IODebugContext* dbg) const override;

  IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;

 private:
  const std::string filename_;
  int fd_;
};

struct PosixMemoryMappedFileBuffer : public MemoryMappedFileBuffer {
  PosixMemoryMappedFileBuffer(void* _base, size_t _length)
      : MemoryMappedFileBuffer(_base, _length) {}
  ~PosixMemoryMappedFileBuffer() override;
};

class PosixDirectory : public FSDirectory {
 public:
  explicit PosixDirectory(int fd, const std::string& directory_name);
  ~PosixDirectory() override;
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;

  IOStatus FsyncWithDirOptions(
      const IOOptions&, IODebugContext*,
      const DirFsyncOptions& dir_fsync_options) override;

 private:
  int fd_;
  bool is_btrfs_;
  const std::string directory_name_;
};

}  // namespace ROCKSDB_NAMESPACE
