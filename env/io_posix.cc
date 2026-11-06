//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef ROCKSDB_LIB_IO_POSIX
#include "env/io_posix.h"

#include <fcntl.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#if defined(OS_LINUX)
#include <linux/fs.h>
#ifndef FALLOC_FL_KEEP_SIZE
#include <linux/falloc.h>
#endif
#endif
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#if defined(OS_LINUX) || defined(OS_ANDROID)
#include <sys/statfs.h>
#include <sys/sysmacros.h>
#endif
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/slice.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#include "util/autovector.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/string_util.h"

#if USE_COROUTINES && FOLLY_HAS_LIBURING
#include "folly/Executor.h"
#include "folly/io/async/EventBase.h"
#include "folly/io/async/EventBaseManager.h"
#include "folly/io/async/IoUringBackend.h"
#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING

#if defined(OS_LINUX) && !defined(F_SET_RW_HINT)
#define F_LINUX_SPECIFIC_BASE 1024
#define F_SET_RW_HINT (F_LINUX_SPECIFIC_BASE + 12)
#endif

// Suppress known TSAN false positives in third-party liburing code.
// liburing's io_uring_mmap() uses __sys_mmap (raw syscall) which bypasses
// TSAN's mmap interceptor, then immediately reads from the mapped memory in
// io_uring_setup_ring_pointers().  When the kernel reuses a virtual address
// that was previously mmap'd through libc (and thus tracked by TSAN), TSAN
// flags a data race between the old mapping's write and the new io_uring
// read.  The TsanAnnotateIOUringMemory() call in CreateIOUring() resets
// shadow state, but only AFTER io_uring_queue_init() returns — too late for
// reads that happen inside io_uring_mmap() during init.
//
// The suppression is safe because io_uring_setup_ring_pointers() is pure
// pointer arithmetic + reads from kernel-managed shared memory; there are
// no real data races in that code path.
#if defined(__SANITIZE_THREAD__) && defined(ROCKSDB_IOURING_PRESENT)
extern "C" const char* __tsan_default_suppressions() {
  return "race:io_uring_mmap\n"
         "race:io_uring_setup_ring_pointers\n";
}
#endif  // __SANITIZE_THREAD__ && ROCKSDB_IOURING_PRESENT

namespace ROCKSDB_NAMESPACE {

std::string IOErrorMsg(const std::string& context,
                       const std::string& file_name) {
  if (file_name.empty()) {
    return context;
  }
  return context + ": " + file_name;
}

// file_name can be left empty if it is not unkown.
IOStatus IOError(const std::string& context, const std::string& file_name,
                 int err_number) {
  switch (err_number) {
    case ENOSPC: {
      IOStatus s = IOStatus::NoSpace(IOErrorMsg(context, file_name),
                                     errnoStr(err_number).c_str());
      s.SetRetryable(true);
      return s;
    }
    case ESTALE:
      return IOStatus::IOError(IOStatus::kStaleFile);
    case ENOENT:
      return IOStatus::PathNotFound(IOErrorMsg(context, file_name),
                                    errnoStr(err_number).c_str());
    default:
      return IOStatus::IOError(IOErrorMsg(context, file_name),
                               errnoStr(err_number).c_str());
  }
}

#if USE_COROUTINES && FOLLY_HAS_LIBURING
namespace {
thread_local bool current_thread_read_io_uring_backend_available = false;
}  // namespace

void SetCurrentThreadReadIOUringBackendAvailable() {
  current_thread_read_io_uring_backend_available = true;
}

bool IsCurrentThreadReadIOUringBackendAvailable() {
  return current_thread_read_io_uring_backend_available;
}
#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING

// A wrapper for fadvise, if the platform doesn't support fadvise,
// it will simply return 0.
int Fadvise(int fd, off_t offset, size_t len, int advice) {
#ifdef OS_LINUX
  return posix_fadvise(fd, offset, len, advice);
#else
  (void)fd;
  (void)offset;
  (void)len;
  (void)advice;
  return 0;  // simply do nothing.
#endif
}

// A wrapper for fadvise, if the platform doesn't support fadvise,
// it will simply return 0.
int Madvise(void* addr, size_t len, int advice) {
#ifdef OS_LINUX
  return posix_madvise(addr, len, advice);
#else
  (void)addr;
  (void)len;
  (void)advice;
  return 0;  // simply do nothing.
#endif
}

namespace {

// On MacOS (and probably *BSD), the posix write and pwrite calls do not support
// buffers larger than 2^31-1 bytes. These two wrappers fix this issue by
// cutting the buffer in 1GB chunks. We use this chunk size to be sure to keep
// the writes aligned.

bool PosixWrite(int fd, const char* buf, size_t nbyte) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = write(fd, src, bytes_to_write);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    left -= done;
    src += done;
  }
  return true;
}

bool PosixPositionedWriteInternal(int fd, const char* buf, size_t nbyte,
                                  off_t offset) {
  const size_t kLimit1Gb = 1UL << 30;

  const char* src = buf;
  size_t left = nbyte;

  while (left != 0) {
    size_t bytes_to_write = std::min(left, kLimit1Gb);

    ssize_t done = pwrite(fd, src, bytes_to_write, offset);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (done == 0) {
      return false;
    }
    left -= done;
    offset += done;
    src += done;
  }

  return true;
}

#ifdef ROCKSDB_RANGESYNC_PRESENT

#if !defined(ZFS_SUPER_MAGIC)
// The magic number for ZFS was not exposed until recently. It should be fixed
// forever so we can just copy the magic number here.
#define ZFS_SUPER_MAGIC 0x2fc12fc1
#endif

bool IsSyncFileRangeSupported(int fd) {
  // This function tracks and checks for cases where we know `sync_file_range`
  // definitely will not work properly despite passing the compile-time check
  // (`ROCKSDB_RANGESYNC_PRESENT`). If we are unsure, or if any of the checks
  // fail in unexpected ways, we allow `sync_file_range` to be used. This way
  // should minimize risk of impacting existing use cases.
  struct statfs buf;
  int ret = fstatfs(fd, &buf);
  assert(ret == 0);
  if (ret == 0 && buf.f_type == ZFS_SUPER_MAGIC) {
    // Testing on ZFS showed the writeback did not happen asynchronously when
    // `sync_file_range` was called, even though it returned success. Avoid it
    // and use `fdatasync` instead to preserve the contract of `bytes_per_sync`,
    // even though this'll incur extra I/O for metadata.
    return false;
  }

  ret = sync_file_range(fd, 0 /* offset */, 0 /* nbytes */, 0 /* flags */);
  assert(!(ret == -1 && errno != ENOSYS));
  if (ret == -1 && errno == ENOSYS) {
    // `sync_file_range` is not implemented on all platforms even if
    // compile-time checks pass and a supported filesystem is in-use. For
    // example, using ext4 on WSL (Windows Subsystem for Linux),
    // `sync_file_range()` returns `ENOSYS`
    // ("Function not implemented").
    return false;
  }
  // None of the known cases matched, so allow `sync_file_range` use.
  return true;
}

#undef ZFS_SUPER_MAGIC

#endif  // ROCKSDB_RANGESYNC_PRESENT

}  // anonymous namespace

bool PosixPositionedWrite(int fd, const char* buf, size_t nbyte, off_t offset) {
  return PosixPositionedWriteInternal(fd, buf, nbyte, offset);
}

/*
 * PosixSequentialFile
 */
PosixSequentialFile::PosixSequentialFile(const std::string& fname, FILE* file,
                                         int fd, size_t logical_block_size,
                                         const EnvOptions& options)
    : filename_(fname),
      file_(file),
      fd_(fd),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(logical_block_size) {
  assert(!options.use_direct_reads || !options.use_mmap_reads);
}

PosixSequentialFile::~PosixSequentialFile() {
  if (!PosixSequentialFile::use_direct_io()) {
    assert(file_);
    fclose(file_);
  } else {
    assert(fd_);
    close(fd_);
  }
}

IOStatus PosixSequentialFile::Read(size_t n, const IOOptions& /*opts*/,
                                   Slice* result, char* scratch,
                                   IODebugContext* /*dbg*/) {
  assert(result != nullptr && !use_direct_io());
  IOStatus s;
  size_t r = 0;
  do {
    clearerr(file_);
    r = fread_unlocked(scratch, 1, n, file_);
  } while (r == 0 && ferror(file_) && errno == EINTR);
  *result = Slice(scratch, r);
  if (r < n) {
    if (feof(file_)) {
      // We leave status as ok if we hit the end of the file
      // We also clear the error so that the reads can continue
      // if a new data is written to the file
      clearerr(file_);
    } else {
      // A partial read with an error: return a non-ok status
      s = IOError("While reading file sequentially", filename_, errno);
    }
  }
  return s;
}

IOStatus PosixSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                             const IOOptions& /*opts*/,
                                             Slice* result, char* scratch,
                                             IODebugContext* /*dbg*/) {
  assert(use_direct_io());
  assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
  assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
  assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));

  IOStatus s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
    if (!IsSectorAligned(r, GetRequiredBufferAlignment())) {
      // Bytes reads don't fill sectors. Should only happen at the end
      // of the file.
      break;
    }
  }
  if (r < 0) {
    // An error: return a non-ok status
    s = IOError("While pread " + std::to_string(n) + " bytes from offset " +
                    std::to_string(offset),
                filename_, errno);
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

IOStatus PosixSequentialFile::Skip(uint64_t n) {
  if (fseek(file_, static_cast<long int>(n), SEEK_CUR)) {
    return IOError("While fseek to skip " + std::to_string(n) + " bytes",
                   filename_, errno);
  }
  return IOStatus::OK();
}

IOStatus PosixSequentialFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  if (!use_direct_io()) {
    // free OS pages
    int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
    if (ret != 0) {
      return IOError("While fadvise NotNeeded offset " +
                         std::to_string(offset) + " len " +
                         std::to_string(length),
                     filename_, errno);
    }
  }
  return IOStatus::OK();
#endif
}

/*
 * PosixRandomAccessFile
 */
#if defined(OS_LINUX)
size_t PosixHelper::GetUniqueIdFromFile(int fd, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  long version = 0;
  result = ioctl(fd, FS_IOC_GETVERSION, &version);
  TEST_SYNC_POINT_CALLBACK("GetUniqueIdFromFile:FS_IOC_GETVERSION", &result);
  if (result == -1) {
    return 0;
  }
  uint64_t uversion = (uint64_t)version;

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, uversion);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}
#endif

#if defined(OS_MACOSX) || defined(OS_AIX)
size_t PosixHelper::GetUniqueIdFromFile(int fd, char* id, size_t max_size) {
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, buf.st_gen);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);
}
#endif

#ifdef OS_LINUX
std::string RemoveTrailingSlash(const std::string& path) {
  std::string p = path;
  if (p.size() > 1 && p.back() == '/') {
    p.pop_back();
  }
  return p;
}

Status LogicalBlockSizeCache::RefAndCacheLogicalBlockSize(
    const std::vector<std::string>& directories) {
  std::vector<std::string> dirs;
  dirs.reserve(directories.size());
  for (auto& d : directories) {
    dirs.emplace_back(RemoveTrailingSlash(d));
  }

  std::map<std::string, size_t> dir_sizes;
  {
    ReadLock lock(&cache_mutex_);
    for (const auto& dir : dirs) {
      if (cache_.find(dir) == cache_.end()) {
        dir_sizes.emplace(dir, 0);
      }
    }
  }

  Status s;
  for (auto& dir_size : dir_sizes) {
    s = get_logical_block_size_of_directory_(dir_size.first, &dir_size.second);
    if (!s.ok()) {
      return s;
    }
  }

  WriteLock lock(&cache_mutex_);
  for (const auto& dir : dirs) {
    auto& v = cache_[dir];
    v.ref++;
    auto dir_size = dir_sizes.find(dir);
    if (dir_size != dir_sizes.end()) {
      v.size = dir_size->second;
    }
  }
  return s;
}

void LogicalBlockSizeCache::UnrefAndTryRemoveCachedLogicalBlockSize(
    const std::vector<std::string>& directories) {
  std::vector<std::string> dirs;
  dirs.reserve(directories.size());
  for (auto& dir : directories) {
    dirs.emplace_back(RemoveTrailingSlash(dir));
  }

  WriteLock lock(&cache_mutex_);
  for (const auto& dir : dirs) {
    auto it = cache_.find(dir);
    if (it != cache_.end() && !(--(it->second.ref))) {
      cache_.erase(it);
    }
  }
}

size_t LogicalBlockSizeCache::GetLogicalBlockSize(const std::string& fname,
                                                  int fd) {
  std::string dir = fname.substr(0, fname.find_last_of('/'));
  if (dir.empty()) {
    dir = "/";
  }
  {
    ReadLock lock(&cache_mutex_);
    auto it = cache_.find(dir);
    if (it != cache_.end()) {
      return it->second.size;
    }
  }
  return get_logical_block_size_of_fd_(fd);
}
#endif

Status PosixHelper::GetLogicalBlockSizeOfDirectory(const std::string& directory,
                                                   size_t* size) {
  return GetQueueSysfsFileValueofDirectory(directory,
                                           GetLogicalBlockSizeFileName(), size);
}

Status PosixHelper::GetMaxSectorsKBOfDirectory(const std::string& directory,
                                               size_t* kb) {
  return GetQueueSysfsFileValueofDirectory(directory, GetMaxSectorsKBFileName(),
                                           kb);
}

Status PosixHelper::GetQueueSysfsFileValueofDirectory(
    const std::string& directory, const std::string& file_name, size_t* value) {
  int fd = open(directory.c_str(), O_DIRECTORY | O_RDONLY);
  if (fd == -1) {
    return Status::IOError("Cannot open directory " + directory);
  }
  if (file_name == PosixHelper::GetLogicalBlockSizeFileName()) {
    *value = PosixHelper::GetLogicalBlockSizeOfFd(fd);
  } else if (file_name == PosixHelper::GetMaxSectorsKBFileName()) {
    *value = PosixHelper::GetMaxSectorsKBOfFd(fd);
  } else {
    assert(false);
  }
  close(fd);
  return Status::OK();
}

size_t PosixHelper::GetLogicalBlockSizeOfFd(int fd) {
  return GetQueueSysfsFileValueOfFd(fd, GetLogicalBlockSizeFileName(),
                                    kDefaultPageSize);
}

size_t PosixHelper::GetMaxSectorsKBOfFd(int fd) {
  return GetQueueSysfsFileValueOfFd(fd, GetMaxSectorsKBFileName(),
                                    kDefaultMaxSectorsKB);
}

size_t PosixHelper::GetQueueSysfsFileValueOfFd(
    int fd, const std::string& file_name, const size_t default_return_value) {
#ifdef OS_LINUX
  struct stat buf;
  int result = fstat(fd, &buf);
  if (result == -1) {
    return default_return_value;
  }

  // Get device number
  if (major(buf.st_dev) == 0) {
    // Unnamed devices (e.g. non-device mounts), reserved as null device number.
    // These don't have an entry in /sys/dev/block/. Return a sensible default.
    return default_return_value;
  }

  // Get device path
  const int kBufferSize = 100;
  char path[kBufferSize];
  char real_path[PATH_MAX + 1];
  snprintf(path, kBufferSize, "/sys/dev/block/%u:%u", major(buf.st_dev),
           minor(buf.st_dev));
  if (realpath(path, real_path) == nullptr) {
    return default_return_value;
  }
  std::string device_dir(real_path);

  // Get the queue sysfs file path
  if (!device_dir.empty() && device_dir.back() == '/') {
    device_dir.pop_back();
  }
  // NOTE: sda3 and nvme0n1p1 do not have a `queue/` subdir, only the parent sda
  // and nvme0n1 have it.
  // $ ls -al '/sys/dev/block/8:3'
  // lrwxrwxrwx. 1 root root 0 Jun 26 01:38 /sys/dev/block/8:3 ->
  // ../../block/sda/sda3
  // $ ls -al '/sys/dev/block/259:4'
  // lrwxrwxrwx 1 root root 0 Jan 31 16:04 /sys/dev/block/259:4 ->
  // ../../devices/pci0000:17/0000:17:00.0/0000:18:00.0/nvme/nvme0/nvme0n1/nvme0n1p1
  size_t parent_end = device_dir.rfind('/', device_dir.length() - 1);
  if (parent_end == std::string::npos) {
    return default_return_value;
  }
  size_t parent_begin = device_dir.rfind('/', parent_end - 1);
  if (parent_begin == std::string::npos) {
    return default_return_value;
  }
  std::string parent =
      device_dir.substr(parent_begin + 1, parent_end - parent_begin - 1);
  std::string child = device_dir.substr(parent_end + 1, std::string::npos);
  if (parent != "block" &&
      (child.compare(0, 4, "nvme") || child.find('p') != std::string::npos)) {
    device_dir = device_dir.substr(0, parent_end);
  }
  std::string fname = device_dir + "/queue/" + file_name;

  // Get value in the queue sysfs file
  FILE* fp;
  size_t value = 0;
  fp = fopen(fname.c_str(), "r");
  if (fp != nullptr) {
    char* line = nullptr;
    size_t len = 0;
    if (getline(&line, &len, fp) != -1) {
      sscanf(line, "%zu", &value);
    }
    free(line);
    fclose(fp);
  }

  if (file_name == GetLogicalBlockSizeFileName()) {
    if (value != 0 && (value & (value - 1)) == 0) {
      return value;
    }
  } else if (file_name == GetMaxSectorsKBFileName()) {
    if (value != 0) {
      return value;
    }
  } else {
    assert(false);
  }
#endif
  (void)fd;
  (void)file_name;
  return default_return_value;
}

/*
 * PosixRandomAccessFile
 *
 * pread() based random-access
 */
PosixRandomAccessFile::PosixRandomAccessFile(
    const std::string& fname, int fd, size_t logical_block_size,
    const FileOptions& options
#if USE_COROUTINES && FOLLY_HAS_LIBURING
    ,
    folly::EventBaseManager* read_event_base_manager
#endif
#if defined(ROCKSDB_IOURING_PRESENT)
    ,
    ThreadLocalPtr* thread_local_async_read_io_urings,
    ThreadLocalPtr* thread_local_multi_read_io_urings
#endif
    )
    : filename_(fname),
      fd_(fd),
      use_direct_io_(options.use_direct_reads),
      logical_sector_size_(logical_block_size)
#if USE_COROUTINES && FOLLY_HAS_LIBURING
      ,
      read_event_base_manager_(read_event_base_manager)
#endif
#if defined(ROCKSDB_IOURING_PRESENT)
      ,
      thread_local_async_read_io_urings_(thread_local_async_read_io_urings),
      thread_local_multi_read_io_urings_(thread_local_multi_read_io_urings)
#endif
{
  assert(!options.use_direct_reads || !options.use_mmap_reads);
  assert(!options.use_mmap_reads);
}

PosixRandomAccessFile::~PosixRandomAccessFile() { close(fd_); }

#if USE_COROUTINES && FOLLY_HAS_LIBURING
namespace {

struct PosixSubmitReadAsyncState {
  int fd;
  FSReadRequest* req;
  char* cur;
  size_t remaining;
  uint64_t off;
  size_t total = 0;
  std::function<void(FSReadRequest&)> cb;
  folly::Executor::KeepAlive<folly::EventBase> event_base;
  size_t alignment = 0;

  void Complete(IOStatus status) {
    req->status = std::move(status);
    req->result = req->status.ok() ? Slice(req->scratch, total) : Slice();
    cb(*req);
    delete this;
  }

  void Queue() {
    auto* backend =
        static_cast_with_check<folly::IoUringBackend>(event_base->getBackend());
    backend->queueRead(
        fd, cur, remaining, static_cast<off_t>(off), [this](int res) {
          if (res == -EINTR || res == -EAGAIN) {
            event_base->add([this] { Queue(); });
            return;
          }
          IOStatus status;
          if (res < 0) {
            status = IOStatus::IOError("io_uring read failed",
                                       errnoStr(-res).c_str());
          } else if (res == 0) {
            status = IOStatus::OK();
          } else {
            const size_t bytes_read = static_cast<size_t>(res);
            if (bytes_read > remaining) {
              status = IOStatus::IOError(
                  "io_uring read returned " + std::to_string(bytes_read) +
                  " bytes for a " + std::to_string(remaining) +
                  " byte request");
            } else {
              total += bytes_read;
              if (bytes_read == remaining ||
                  // If direct io enabled, then it is possible to have a short
                  // read, which would indicate EOF.
                  (alignment != 0 && bytes_read % alignment != 0)) {
                status = IOStatus::OK();
              } else {
                remaining -= bytes_read;
                cur += bytes_read;
                off += bytes_read;
                event_base->add([this] { Queue(); });
                return;
              }
            }
          }
          Complete(std::move(status));
        });
  }
};

}  // namespace

bool PosixRandomAccessFile::SubmitReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&)> cb, IODebugContext* dbg) {
  if (use_direct_io()) {
    assert(IsSectorAligned(req.offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(req.len, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(req.scratch, GetRequiredBufferAlignment()));
  }
  assert(req.len == 0 || req.scratch != nullptr);

  // Workers using the default EventBase backend read synchronously.
  if (!IsCurrentThreadReadIOUringBackendAvailable()) {
    req.status =
        Read(req.offset, req.len, opts, &(req.result), req.scratch, dbg);
    cb(req);
    return false;
  }

  assert(read_event_base_manager_);
  folly::EventBase* event_base =
      read_event_base_manager_->getExistingEventBase();
  assert(event_base != nullptr && event_base->inRunningEventBaseThread());

  // Keep the event base alive so that cb is guaranteed to be run on it
  auto event_base_keep_alive = folly::Executor::getKeepAliveToken(event_base);

  auto* state = new PosixSubmitReadAsyncState{
      fd_,
      &req,
      req.scratch,
      req.len,
      req.offset,
      0,
      std::move(cb),
      std::move(event_base_keep_alive),
      use_direct_io() ? GetRequiredBufferAlignment() : 0};

  if (req.len == 0) {
    state->Complete(IOStatus::OK());
  } else {
    state->Queue();
  }
  return true;
}
#endif  // USE_COROUTINES && FOLLY_HAS_LIBURING

IOStatus PosixRandomAccessFile::GetFileSize(uint64_t* result) {
  struct stat sbuf{};
  if (fstat(fd_, &sbuf) != 0) {
    *result = 0;
    return IOError("While fstat with fd " + std::to_string(fd_), filename_,
                   errno);
  }
  *result = sbuf.st_size;
  return IOStatus::OK();
}

IOStatus PosixRandomAccessFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*opts*/, Slice* result,
                                     char* scratch,
                                     IODebugContext* /*dbg*/) const {
  if (use_direct_io()) {
    assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(n, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(scratch, GetRequiredBufferAlignment()));
  }
  IOStatus s;
  ssize_t r = -1;
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    r = pread(fd_, ptr, left, static_cast<off_t>(offset));
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ptr += r;
    offset += r;
    left -= r;
    if (use_direct_io() &&
        r % static_cast<ssize_t>(GetRequiredBufferAlignment()) != 0) {
      // Bytes reads don't fill sectors. Should only happen at the end
      // of the file.
      break;
    }
  }
  if (r < 0) {
    // An error: return a non-ok status
    s = IOError("While pread offset " + std::to_string(offset) + " len " +
                    std::to_string(n),
                filename_, errno);
  }
  *result = Slice(scratch, (r < 0) ? 0 : n - left);
  return s;
}

// MultiRead: Perform multiple concurrent read requests using io_uring.
//
// OVERVIEW:
// This function batches multiple read requests and submits them concurrently
// to io_uring for improved I/O performance. It operates synchronously from the
// caller's perspective (blocks until all reads complete) but uses io_uring's
// async capabilities internally for parallel I/O execution.
//
// IO_URING LIFECYCLE:
// 1. Preparation Phase:
//    - Allocate SQEs (Submission Queue Entries) for read requests
//    - Limited by: min(pending_work, io_uring_sq_space_left(), kIoUringDepth -
//    inflight)
//    - Uses io_uring_sq_space_left() to query available SQ slots
//    - Each SQE is tracked in wrap_cache for completion matching
//
// 2. Submission Phase:
//    - Loop: while io_uring_sq_ready() > 0 (SQEs pending submission)
//    - Call io_uring_submit_and_wait() to submit SQEs and wait for CQEs
//    - Handles retryable errors (EINTR, EAGAIN) by continuing
//    - Breaks on terminal errors (logs error, sets err variable)
//
// 3. Completion Phase:
//    - Non-blocking CQE reaping via io_uring_for_each_cqe()
//    - Matches CQEs to requests using user_data pointer
//    - Processes results: updates bytes read, handles partial reads
//    - Removes completed requests from wrap_cache
//
// 4. Loop Iteration:
//    - Repeats until: all requests submitted AND all completions reaped
//    - Termination condition: (num_reqs == reqs_off) &&
//    resubmit_rq_list.empty() && wrap_cache.empty()
//
// ERROR HANDLING STRATEGY:
// - Retryable submission errors (-EINTR, -EAGAIN): Retry submission
// - Memory pressure (-ENOMEM): Mark memory_pressure_on_submission, attempt
// recovery
// - Terminal submission errors: Break, enter teardown path
// - Retryable CQE errors (-EINTR, -EAGAIN): Add to resubmit_rq_list for retry
// - Terminal CQE errors: Set ios to IOError, continue processing other CQEs
// - Teardown path: If SQEs remain unsubmitted after error, reap submitted CQEs,
//   destroy io_uring instance, return error
//
// PARTIAL READ HANDLING:
// - Short reads (bytes_read < requested): Request added to resubmit_rq_list
// - finished_len tracks cumulative bytes read across resubmissions
// - iov.iov_base/iov_len adjusted on each resubmission attempt
// - UpdateResult() determines if read should be retried based on:
//   * Direct I/O alignment requirements
//   * EOF detection
//   * Error conditions
//
// RESUBMISSION LOGIC:
// - resubmit_rq_list: Requests needing retry (short reads, EINTR/EAGAIN errors)
// - Prioritized in SQE allocation loop: resubmits before new requests
// - List cleared after SQE preparation
// - Requests remain in wrap_cache across resubmissions until fully complete
//
// CONCURRENCY CONTROL:
// - wrap_cache.size(): Tracks total inflight requests (SQ + CQ)
// - io_uring_sq_ready(): Queries SQEs prepared but not yet submitted
// - io_uring_sq_space_left(): Queries available SQ slots
// - Max concurrency: kIoUringDepth (256)
//
// ACCOUNTING CORRECTNESS:
// - Uses io_uring native APIs (io_uring_sq_ready, io_uring_sq_space_left)
//   instead of manual counters for robustness
// - wrap_cache is the authoritative source for inflight request tracking
// - Re-query io_uring_sq_ready() after submission loop to detect
//   unsubmitted SQEs (indicates submission errors)
//
// THREAD SAFETY:
// - Uses thread-local io_uring instance (thread_local_multi_read_io_urings_)
// - IORING_SETUP_SINGLE_ISSUER: Only one thread submits to this ring
// - IORING_SETUP_DEFER_TASKRUN: Task work runs in submitting thread
// - No cross-thread coordination required
//
IOStatus PosixRandomAccessFile::MultiRead(FSReadRequest* reqs, size_t num_reqs,
                                          const IOOptions& options,
                                          IODebugContext* dbg) {
  if (use_direct_io()) {
    for (size_t i = 0; i < num_reqs; i++) {
      assert(IsSectorAligned(reqs[i].offset, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(reqs[i].len, GetRequiredBufferAlignment()));
      assert(IsSectorAligned(reqs[i].scratch, GetRequiredBufferAlignment()));
    }
  }

#if defined(ROCKSDB_IOURING_PRESENT)
  struct io_uring* iu = nullptr;
  if (thread_local_multi_read_io_urings_) {
    iu = static_cast<struct io_uring*>(
        thread_local_multi_read_io_urings_->Get());
    if (iu == nullptr) {
      iu = CreateIOUring();
      if (iu != nullptr) {
        thread_local_multi_read_io_urings_->Reset(iu);
      }
    }
  }

  // Init failed, platform doesn't support io_uring. Fall back to
  // serialized reads
  if (iu == nullptr) {
    return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
  }

  struct WrappedReadRequest {
    FSReadRequest* req;
    struct iovec iov;
    size_t finished_len;
    explicit WrappedReadRequest(FSReadRequest* r) : req(r), finished_len(0) {}
  };

  autovector<WrappedReadRequest, 32> req_wraps;
  autovector<WrappedReadRequest*, 4> resubmit_rq_list;
  std::unordered_set<WrappedReadRequest*> wrap_cache;

  for (size_t i = 0; i < num_reqs; i++) {
    req_wraps.emplace_back(&reqs[i]);
  }

  IOStatus ios = IOStatus::OK();
  size_t reqs_off = 0;
  while ((num_reqs > reqs_off) || !resubmit_rq_list.empty() ||
         !wrap_cache.empty()) {
    assert(resubmit_rq_list.size() + wrap_cache.size() <= kIoUringDepth);
    // Total number of requests that still need to be submitted, includes:
    //
    //  1) requests NOT yet submitted (num_reqs - reqs_off)
    //  2) requests on resubmission list (resubmit_rq_list)
    //
    // capped by min of the # of remaining entries in IO ring submission queue
    // and the max IO ring depth less the inflight requests.
    size_t new_sqe_reqs_count = std::min({
        num_reqs - reqs_off + resubmit_rq_list.size(),
        static_cast<size_t>(io_uring_sq_space_left(iu)),
        kIoUringDepth - wrap_cache.size()  // queue depth less inflight requests
    });
    for (size_t i = 0; i < new_sqe_reqs_count; i++) {
      WrappedReadRequest* req;
      if (i < resubmit_rq_list.size()) {
        req = resubmit_rq_list[i];
      } else {
        req = &req_wraps[reqs_off++];
      }
      assert(req->req->len > req->finished_len);
      req->iov.iov_base = req->req->scratch + req->finished_len;
      req->iov.iov_len = req->req->len - req->finished_len;

      struct io_uring_sqe* sqe;
      sqe = io_uring_get_sqe(iu);
      // NULL is unexpected as we do maintain proper ring accounting.
      assert(sqe);
      io_uring_prep_readv(sqe, fd_, &req->iov, 1,
                          req->req->offset + req->finished_len);
      io_uring_sqe_set_data(sqe, req);
      wrap_cache.emplace(req);
    }
    resubmit_rq_list.clear();

    struct io_uring_cqe* cqe = nullptr;
    unsigned head;
    ssize_t err = 0;
    bool memory_pressure_on_submission = false;
    unsigned reqs_pending_submission;
    unsigned reqs_submitted = 0;
    while ((reqs_pending_submission = io_uring_sq_ready(iu))) {
      // MultiRead is synchronous in nature. io_uring_submit_and_wait provides
      // batching semantics (submit + best effort wait in one syscall), while
      // io_uring_submit enables async producer/consumer semantics (submit
      // only, requires separate reaping). We chose batching approach to
      // reduce the volume of syscalls and context switches.
      ssize_t ret = io_uring_submit_and_wait(iu, reqs_pending_submission);
      if (ret < 0) {
        if (-EINTR == ret || -EAGAIN == ret) {
          // Submission failed due to rare, retryable syscall error. Try again.
          continue;
        }
        if (-ENOMEM == ret) {
          fprintf(stderr,
                  "PosixRandomAccessFile::MultiRead: io_uring_submit_and_wait "
                  "experienced terse memory condition.\n");
          // Best effort to reclaim resources in terse condition.
          memory_pressure_on_submission = true;
        } else {
          fprintf(stderr,
                  "PosixRandomAccessFile::MultiRead: "
                  "io_uring_submit_and_wait returned terminal error: %zd.\n",
                  ret);
          err = ret;
        }
        break;
      }
      if (0 == ret) {
        // This scenario is unexpected for any modern kernel!
        // We deliberately error out to avoid bugs around infinite loops.
        fprintf(stderr,
                "PosixRandomAccessFile::MultiRead: "
                "io_uring_submit_and_wait returned 0 submissions!\n");
        break;
      }
      reqs_submitted += static_cast<unsigned int>(ret);
    };
    reqs_pending_submission = io_uring_sq_ready(iu);

    TEST_SYNC_POINT_CALLBACK(
        "PosixRandomAccessFile::MultiRead:io_uring_sq_ready:return1",
        &reqs_pending_submission);

    // Error occurred or IO uring stopped submitting outstanding requests.
    if (reqs_pending_submission && !memory_pressure_on_submission) {
      // IO ring is initialized once in thread-local variable and then reused
      // to handle the consecutive MultiRead API calls. Therefore, it's crucial
      // to reap all the submitted requests.
      //
      // NOTE: Loop will run indefinitely until we reap all the completions!!!
      size_t nr = 0;
      assert(reqs_pending_submission <= wrap_cache.size());
      size_t nr_await_cqe = wrap_cache.size() - reqs_pending_submission;
      while (nr < nr_await_cqe) {
        // blocking
        io_uring_wait_cqes(iu, &cqe,
                           static_cast<unsigned int>(nr_await_cqe - nr),
                           nullptr, nullptr);
        size_t reaped_cqe_count = 0;
        io_uring_for_each_cqe(iu, head, cqe) { reaped_cqe_count++; }
        if (reaped_cqe_count > 0) {
          io_uring_cq_advance(iu, static_cast<unsigned int>(reaped_cqe_count));
          nr += reaped_cqe_count;
        }
      }

      TEST_SYNC_POINT_CALLBACK(
          "PosixRandomAccessFile::MultiRead:io_uring_submit_and_wait:return2",
          iu);

      // While all the submitted completions have been reaped successfully,
      // IO ring submission queue still contains at least one non-submitted
      // request. Destroy io_uring (discards unsubmitted SQEs).
      //
      // NOTE: This is a rare scenario and should not happen in normal cases.
      //       Hence, this should NOT materially impact the performance metrics.
      io_uring_queue_exit(iu);
      delete iu;
      thread_local_multi_read_io_urings_->Reset(nullptr);

      if (err < 0) {
        return IOStatus::IOError(
            "io_uring_submit_and_wait() failed with an error " +
            std::to_string(err));
      }
      return IOStatus::IOError(
          "io_uring_submit_and_wait() requested " +
          std::to_string(reqs_submitted + reqs_pending_submission) +
          " but returned " + std::to_string(reqs_submitted));
    }

    if ((0 == reqs_submitted) && wrap_cache.size() > reqs_pending_submission) {
      // If no requests have been submitted and there is at least one request
      // pending completion, wait for at least one completion to arrive.
      // This is a guardrail to prevent the busy CPU loops.
      //
      // NOTE: it's not really a tight CPU-burning loop in the traditional sense
      // as it's naturally throttled by the io_uring_submit_and_wait() syscall.
      io_uring_wait_cqe(iu, &cqe);
    }

    unsigned int nr = 0;
    io_uring_for_each_cqe(iu, head, cqe) {  // non-blocking
      if (cqe->user_data) {  // non-discarded, valid user data only!
        nr++;
        WrappedReadRequest* req_wrap =
            static_cast<WrappedReadRequest*>(io_uring_cqe_get_data(cqe));
        // Reset cqe data to catch any stray reuse of it
        static_cast<struct io_uring_cqe*>(cqe)->user_data = 0xd5d5d5d5d5d5d5d5;
        // Check that we got a valid unique cqe data
        auto wrap_check = wrap_cache.find(req_wrap);
        if (wrap_check == wrap_cache.end()) {
          fprintf(stderr,
                  "PosixRandomAccessFile::MultiRead: "
                  "Bad cqe data from IO uring - %p\n",
                  req_wrap);
          port::PrintStack();
          ios = IOStatus::IOError("io_uring_cqe_get_data() returned " +
                                  std::to_string((uint64_t)req_wrap));
          continue;
        }
        wrap_cache.erase(wrap_check);
        if (cqe->res < 0) {
          if (-EINTR == cqe->res || -EAGAIN == cqe->res) {
            resubmit_rq_list.push_back(req_wrap);
          } else {
            ios = IOStatus::IOError("io_uring_for_each_cqe() returns " +
                                    std::to_string(cqe->res));
          }
          continue;
        }
        // cqe->res >= 0
        FSReadRequest* req = req_wrap->req;
        size_t bytes_read = 0;
        bool read_again = false;
        UpdateResult(cqe, filename_, req->len, req_wrap->iov.iov_len,
                     false /*async_read*/, use_direct_io(),
                     GetRequiredBufferAlignment(), req_wrap->finished_len, req,
                     bytes_read, read_again);

        if (0 == bytes_read) {
          if (read_again) {
            Slice tmp_slice;
            req->status =
                Read(req->offset + req_wrap->finished_len,
                     req->len - req_wrap->finished_len, options, &tmp_slice,
                     req->scratch + req_wrap->finished_len, dbg);
            req->result =
                Slice(req->scratch, req_wrap->finished_len + tmp_slice.size());
          }
          // else it means EOF so no need to do anything.
        } else if (bytes_read < req_wrap->iov.iov_len) {
          resubmit_rq_list.push_back(req_wrap);
        }
      }
    }
    if (nr > 0) {
      io_uring_cq_advance(iu, nr);
    }
  }
  return ios;
#else
  return FSRandomAccessFile::MultiRead(reqs, num_reqs, options, dbg);
#endif
}

IOStatus PosixRandomAccessFile::Prefetch(uint64_t offset, size_t n,
                                         const IOOptions& /*opts*/,
                                         IODebugContext* /*dbg*/) {
  IOStatus s;
  if (!use_direct_io()) {
    ssize_t r = 0;
#ifdef OS_LINUX
    r = readahead(fd_, offset, n);
#endif
#ifdef OS_MACOSX
    radvisory advice;
    advice.ra_offset = static_cast<off_t>(offset);
    advice.ra_count = static_cast<int>(n);
    r = fcntl(fd_, F_RDADVISE, &advice);
#endif
    if (r == -1) {
      s = IOError("While prefetching offset " + std::to_string(offset) +
                      " len " + std::to_string(n),
                  filename_, errno);
    }
  }
  return s;
}

#if defined(OS_LINUX) || defined(OS_MACOSX) || defined(OS_AIX)
size_t PosixRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return PosixHelper::GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

void PosixRandomAccessFile::Hint(AccessPattern pattern) {
  if (use_direct_io()) {
    return;
  }
  switch (pattern) {
    case kNormal:
      Fadvise(fd_, 0, 0, POSIX_FADV_NORMAL);
      break;
    case kRandom:
      Fadvise(fd_, 0, 0, POSIX_FADV_RANDOM);
      break;
    case kSequential:
      Fadvise(fd_, 0, 0, POSIX_FADV_SEQUENTIAL);
      break;
    case kWillNeed:
      Fadvise(fd_, 0, 0, POSIX_FADV_WILLNEED);
      break;
    case kWontNeed:
      Fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
      break;
    default:
      assert(false);
      break;
  }
}

IOStatus PosixRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  if (use_direct_io()) {
    return IOStatus::OK();
  }
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded offset " + std::to_string(offset) +
                     " len " + std::to_string(length),
                 filename_, errno);
#endif
}

IOStatus PosixRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& /*opts*/,
    std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(req.offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(req.len, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(req.scratch, GetRequiredBufferAlignment()));
  }

#if defined(ROCKSDB_IOURING_PRESENT)
  // io_uring_queue_init.
  struct io_uring* iu = nullptr;
  if (thread_local_async_read_io_urings_) {
    iu = static_cast<struct io_uring*>(
        thread_local_async_read_io_urings_->Get());
    if (iu == nullptr) {
      iu = CreateIOUring();
      if (iu != nullptr) {
        thread_local_async_read_io_urings_->Reset(iu);
      }
    }
  }

  // Init failed, platform doesn't support io_uring.
  if (iu == nullptr) {
    return IOStatus::NotSupported("ReadAsync: failed to init io_uring");
  }

  *io_handle = nullptr;
  *del_fn = nullptr;

  // Allocate io_handle.
  IOHandleDeleter deletefn = [](void* args) -> void {
    delete (static_cast<Posix_IOHandle*>(args));
    args = nullptr;
  };

  // Initialize Posix_IOHandle.
  Posix_IOHandle* posix_handle =
      new Posix_IOHandle(iu, cb, cb_arg, req.offset, req.len, req.scratch,
                         use_direct_io(), GetRequiredBufferAlignment());
  posix_handle->iov.iov_base = req.scratch;
  posix_handle->iov.iov_len = req.len;

  // Step 3: io_uring_sqe_set_data
  bool skip_get_sqe = false;
  TEST_SYNC_POINT_CALLBACK(
      "PosixRandomAccessFile::ReadAsync:skip_io_uring_get_sqe", &skip_get_sqe);
  struct io_uring_sqe* sqe = nullptr;
  if (!skip_get_sqe) {
    sqe = io_uring_get_sqe(iu);
    TEST_SYNC_POINT_CALLBACK(
        "PosixRandomAccessFile::ReadAsync:io_uring_get_sqe", &sqe);
  }
  if (sqe == nullptr) {
    // Submission queue is full, so outstanding completions have not been
    // reaped yet. Submission never succeeded, so clean up the local handle and
    // return Busy without publishing io_handle/del_fn to the caller.
    delete posix_handle;
    return IOStatus::Busy(
        "PosixRandomAccessFile::ReadAsync: io_uring submission queue is full");
  }

  io_uring_prep_readv(sqe, fd_, /*sqe->addr=*/&posix_handle->iov,
                      /*sqe->len=*/1, /*sqe->offset=*/posix_handle->offset);

  // Sets sqe->user_data to posix_handle.
  io_uring_sqe_set_data(sqe, posix_handle);

  // Step 4: io_uring_submit
  ssize_t ret;
  do {
    ret = io_uring_submit(iu);
    if (ret < 0) {
      if (-EINTR == ret || -EAGAIN == ret) {
        // Submission failed due to transient error. Try again.
        continue;
      }
      fprintf(stderr,
              "PosixRandomAccessFile::ReadAsync: "
              "io_uring_submit returned terminal error = %zd\n",
              ret);
      break;
    }
    if (0 == ret) {
      // Unexpected. Will be reported as error.
      break;
    }
  } while (ret < 1);
  if (ret <= 0) {
    delete posix_handle;
    return IOStatus::IOError(
        "PosixRandomAccessFile::ReadAsync: io_uring_submit() returned " +
        std::to_string(ret));
  }
  if (ret > 1) {
    fprintf(stderr,
            "PosixRandomAccessFile::ReadAsync: "
            "io_uring_submit() returned = %zd\n",
            ret);
  }
  *io_handle = static_cast<void*>(posix_handle);
  *del_fn = deletefn;
  return IOStatus::OK();
#else
  (void)req;
  (void)cb;
  (void)cb_arg;
  (void)io_handle;
  (void)del_fn;
  return IOStatus::NotSupported(
      "ReadAsync: ROCKSDB_IOURING_PRESENT is not set");
#endif
}

/*
 * PosixMmapReadableFile
 *
 * mmap() based random-access
 */
// base[0,length-1] contains the mmapped contents of the file.
PosixMmapReadableFile::PosixMmapReadableFile(const int fd,
                                             const std::string& fname,
                                             void* base, size_t length,
                                             const EnvOptions& options)
    : fd_(fd), filename_(fname), mmapped_region_(base), length_(length) {
#ifdef NDEBUG
  (void)options;
#endif
  fd_ = fd_ + 0;  // suppress the warning for used variables
  assert(options.use_mmap_reads);
  assert(!options.use_direct_reads);
}

PosixMmapReadableFile::~PosixMmapReadableFile() {
  int ret = munmap(mmapped_region_, length_);
  if (ret != 0) {
    fprintf(stdout, "failed to munmap %p length %" ROCKSDB_PRIszt " \n",
            mmapped_region_, length_);
  }
  close(fd_);
}

IOStatus PosixMmapReadableFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*opts*/, Slice* result,
                                     char* /*scratch*/,
                                     IODebugContext* /*dbg*/) const {
  IOStatus s;
  if (offset > length_) {
    *result = Slice();
    return IOError("While mmap read offset " + std::to_string(offset) +
                       " larger than file length " + std::to_string(length_),
                   filename_, EINVAL);
  } else if (offset + n > length_) {
    n = static_cast<size_t>(length_ - offset);
  }
  *result = Slice(static_cast<char*>(mmapped_region_) + offset, n);
  return s;
}

void PosixMmapReadableFile::Hint(AccessPattern pattern) {
  switch (pattern) {
    case kNormal:
      Madvise(mmapped_region_, length_, POSIX_MADV_NORMAL);
      break;
    case kRandom:
      Madvise(mmapped_region_, length_, POSIX_MADV_RANDOM);
      break;
    case kSequential:
      Madvise(mmapped_region_, length_, POSIX_MADV_SEQUENTIAL);
      break;
    case kWillNeed:
      Madvise(mmapped_region_, length_, POSIX_MADV_WILLNEED);
      break;
    case kWontNeed:
      Madvise(mmapped_region_, length_, POSIX_MADV_DONTNEED);
      break;
    default:
      assert(false);
      break;
  }
}

IOStatus PosixMmapReadableFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise not needed. Offset " + std::to_string(offset) +
                     " len" + std::to_string(length),
                 filename_, errno);
#endif
}

IOStatus PosixMmapReadableFile::GetFileSize(uint64_t* result) {
  *result = length_;
  return IOStatus::OK();
}

/*
 * PosixMmapFile
 *
 * We preallocate up to an extra megabyte and use memcpy to append new
 * data to the file.  This is safe since we either properly close the
 * file before reading from it, or for log files, the reading code
 * knows enough to skip zero suffixes.
 */
IOStatus PosixMmapFile::UnmapCurrentRegion() {
  TEST_KILL_RANDOM("PosixMmapFile::UnmapCurrentRegion:0");
  if (base_ != nullptr) {
    int munmap_status = munmap(base_, limit_ - base_);
    if (munmap_status != 0) {
      return IOError("While munmap", filename_, munmap_status);
    }
    file_offset_ += limit_ - base_;
    base_ = nullptr;
    limit_ = nullptr;
    last_sync_ = nullptr;
    dst_ = nullptr;

    // Increase the amount we map the next time, but capped at 1MB
    if (map_size_ < (1 << 20)) {
      map_size_ *= 2;
    }
  }
  return IOStatus::OK();
}

IOStatus PosixMmapFile::MapNewRegion() {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  assert(base_ == nullptr);
  TEST_KILL_RANDOM("PosixMmapFile::UnmapCurrentRegion:0");
  // we can't fallocate with FALLOC_FL_KEEP_SIZE here
  if (allow_fallocate_) {
    IOSTATS_TIMER_GUARD(allocate_nanos);
    int alloc_status = fallocate(fd_, 0, file_offset_, map_size_);
    if (alloc_status != 0) {
      // fallback to posix_fallocate
      alloc_status = posix_fallocate(fd_, file_offset_, map_size_);
    }
    if (alloc_status != 0) {
      return IOStatus::IOError("Error allocating space to file : " + filename_ +
                               "Error : " + errnoStr(alloc_status).c_str());
    }
  }

  TEST_KILL_RANDOM("PosixMmapFile::Append:1");
  void* ptr = mmap(nullptr, map_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_,
                   file_offset_);
  if (ptr == MAP_FAILED) {
    return IOStatus::IOError("MMap failed on " + filename_);
  }
  TsanAnnotateMappedMemory(ptr, map_size_);
  TEST_KILL_RANDOM("PosixMmapFile::Append:2");

  base_ = static_cast<char*>(ptr);
  limit_ = base_ + map_size_;
  dst_ = base_;
  last_sync_ = base_;
  return IOStatus::OK();
#else
  return IOStatus::NotSupported("This platform doesn't support fallocate()");
#endif
}

IOStatus PosixMmapFile::Msync() {
  if (dst_ == last_sync_) {
    return IOStatus::OK();
  }
  // Find the beginnings of the pages that contain the first and last
  // bytes to be synced.
  size_t p1 = TruncateToPageBoundary(last_sync_ - base_);
  size_t p2 = TruncateToPageBoundary(dst_ - base_ - 1);
  last_sync_ = dst_;
  TEST_KILL_RANDOM("PosixMmapFile::Msync:0");
  if (msync(base_ + p1, p2 - p1 + page_size_, MS_SYNC) < 0) {
    return IOError("While msync", filename_, errno);
  }
  return IOStatus::OK();
}

PosixMmapFile::PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                             const EnvOptions& options,
                             uint64_t initial_file_size)
    : filename_(fname),
      fd_(fd),
      page_size_(page_size),
      map_size_(Roundup(65536, page_size)),
      base_(nullptr),
      limit_(nullptr),
      dst_(nullptr),
      last_sync_(nullptr),
      file_offset_(initial_file_size) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#else
  (void)options;
#endif
  assert((page_size & (page_size - 1)) == 0);
  assert(options.use_mmap_writes);
  assert(!options.use_direct_writes);
}

PosixMmapFile::~PosixMmapFile() {
  if (fd_ >= 0) {
    IOStatus s = PosixMmapFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixMmapFile::Append(const Slice& data, const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
  const char* src = data.data();
  size_t left = data.size();
  while (left > 0) {
    assert(base_ <= dst_);
    assert(dst_ <= limit_);
    size_t avail = limit_ - dst_;
    if (avail == 0) {
      IOStatus s = UnmapCurrentRegion();
      if (!s.ok()) {
        return s;
      }
      s = MapNewRegion();
      if (!s.ok()) {
        return s;
      }
      TEST_KILL_RANDOM("PosixMmapFile::Append:0");
    }

    size_t n = (left <= avail) ? left : avail;
    assert(dst_);
    memcpy(dst_, src, n);
    dst_ += n;
    src += n;
    left -= n;
  }
  return IOStatus::OK();
}

IOStatus PosixMmapFile::Close(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  IOStatus s;
  size_t unused = limit_ - dst_;

  s = UnmapCurrentRegion();
  if (!s.ok()) {
    s = IOError("While closing mmapped file", filename_, errno);
  } else if (unused > 0) {
    // Trim the extra space at the end of the file
    if (ftruncate(fd_, file_offset_ - unused) < 0) {
      s = IOError("While ftruncating mmaped file", filename_, errno);
    }
  }

  if (close(fd_) < 0) {
    if (s.ok()) {
      s = IOError("While closing mmapped file", filename_, errno);
    }
  }

  fd_ = -1;
  base_ = nullptr;
  limit_ = nullptr;
  return s;
}

IOStatus PosixMmapFile::Flush(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixMmapFile::Sync(const IOOptions& /*opts*/,
                             IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLSYNC) mmapped file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync mmapped file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC

  return Msync();
}

/**
 * Flush data as well as metadata to stable storage.
 */
IOStatus PosixMmapFile::Fsync(const IOOptions& /*opts*/,
                              IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("While fcntl(F_FULLSYNC) on mmaped file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fsync(fd_) < 0) {
    return IOError("While fsync mmaped file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC

  return Msync();
}

/**
 * Get the size of valid data in the file. This will not match the
 * size that is returned from the filesystem because we use mmap
 * to extend file by map_size every time.
 */
uint64_t PosixMmapFile::GetFileSize(const IOOptions& /*opts*/,
                                    IODebugContext* /*dbg*/) {
  size_t used = dst_ - base_;
  return file_offset_ + used;
}

IOStatus PosixMmapFile::InvalidateCache(size_t offset, size_t length) {
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded mmapped file", filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
IOStatus PosixMmapFile::Allocate(uint64_t offset, uint64_t len,
                                 const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(len <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  TEST_KILL_RANDOM("PosixMmapFile::Allocate:0");
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status =
        fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0,
                  static_cast<off_t>(offset), static_cast<off_t>(len));
  }
  if (alloc_status == 0) {
    return IOStatus::OK();
  } else {
    return IOError("While fallocate offset " + std::to_string(offset) +
                       " len " + std::to_string(len),
                   filename_, errno);
  }
}
#endif

/*
 * PosixWritableFile
 *
 * Use posix write to write data to a file.
 */
PosixWritableFile::PosixWritableFile(const std::string& fname, int fd,
                                     size_t logical_block_size,
                                     const EnvOptions& options,
                                     uint64_t initial_file_size)
    : FSWritableFile(options),
      filename_(fname),
      use_direct_io_(options.use_direct_writes),
      fd_(fd),
      filesize_(initial_file_size),
      logical_sector_size_(logical_block_size) {
#ifdef ROCKSDB_FALLOCATE_PRESENT
  allow_fallocate_ = options.allow_fallocate;
  fallocate_with_keep_size_ = options.fallocate_with_keep_size;
#endif
#ifdef ROCKSDB_RANGESYNC_PRESENT
  sync_file_range_supported_ = IsSyncFileRangeSupported(fd_);
#endif  // ROCKSDB_RANGESYNC_PRESENT
  assert(!options.use_mmap_writes);
}

PosixWritableFile::~PosixWritableFile() {
  if (fd_ >= 0) {
    IOStatus s = PosixWritableFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixWritableFile::Append(const Slice& data, const IOOptions& /*opts*/,
                                   IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
  }
  const char* src = data.data();
  size_t nbytes = data.size();

  if (!PosixWrite(fd_, src, nbytes)) {
    return IOError("While appending to file", filename_, errno);
  }

  filesize_ += nbytes;
  return IOStatus::OK();
}

IOStatus PosixWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                             const IOOptions& /*opts*/,
                                             IODebugContext* /*dbg*/) {
  if (use_direct_io()) {
    assert(IsSectorAligned(offset, GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.size(), GetRequiredBufferAlignment()));
    assert(IsSectorAligned(data.data(), GetRequiredBufferAlignment()));
  }
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixPositionedWrite(fd_, src, nbytes, static_cast<off_t>(offset))) {
    return IOError("While pwrite to file at offset " + std::to_string(offset),
                   filename_, errno);
  }
  filesize_ = offset + nbytes;
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Truncate(uint64_t size, const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  IOStatus s;
  int r = ftruncate(fd_, size);
  if (r < 0) {
    s = IOError("While ftruncate file to size " + std::to_string(size),
                filename_, errno);
  } else {
    filesize_ = size;
    lseek(fd_, filesize_, SEEK_SET);
  }
  return s;
}

IOStatus PosixWritableFile::Close(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  IOStatus s;

  size_t block_size;
  size_t last_allocated_block;
  GetPreallocationStatus(&block_size, &last_allocated_block);
  TEST_SYNC_POINT_CALLBACK("PosixWritableFile::Close", &last_allocated_block);
  if (last_allocated_block > 0) {
    // trim the extra space preallocated at the end of the file
    // NOTE(ljin): we probably don't want to surface failure as an IOError,
    // but it will be nice to log these errors.
    int dummy __attribute__((__unused__));
    dummy = ftruncate(fd_, filesize_);
#if defined(ROCKSDB_FALLOCATE_PRESENT) && defined(FALLOC_FL_PUNCH_HOLE)
    // in some file systems, ftruncate only trims trailing space if the
    // new file size is smaller than the current size. Calling fallocate
    // with FALLOC_FL_PUNCH_HOLE flag to explicitly release these unused
    // blocks. FALLOC_FL_PUNCH_HOLE is supported on at least the following
    // filesystems:
    //   XFS (since Linux 2.6.38)
    //   ext4 (since Linux 3.0)
    //   Btrfs (since Linux 3.7)
    //   tmpfs (since Linux 3.5)
    // We ignore error since failure of this operation does not affect
    // correctness.
    struct stat file_stats;
    int result = fstat(fd_, &file_stats);
    // After ftruncate, we check whether ftruncate has the correct behavior.
    // If not, we should hack it with FALLOC_FL_PUNCH_HOLE
    if (result == 0 &&
        static_cast<size_t>((file_stats.st_size + file_stats.st_blksize - 1) /
                            file_stats.st_blksize) !=
            static_cast<size_t>(file_stats.st_blocks /
                                (file_stats.st_blksize / 512))) {
      IOSTATS_TIMER_GUARD(allocate_nanos);
      if (allow_fallocate_) {
        fallocate(fd_, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, filesize_,
                  block_size * last_allocated_block - filesize_);
      }
    }
#endif
  }

  if (close(fd_) < 0) {
    s = IOError("While closing file after writing", filename_, errno);
  }
  fd_ = -1;
  return s;
}

// write out the cached data to the OS cache
IOStatus PosixWritableFile::Flush(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Sync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC)", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

IOStatus PosixWritableFile::Fsync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC)", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fsync(fd_) < 0) {
    return IOError("While fsync", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

bool PosixWritableFile::IsSyncThreadSafe() const { return true; }

uint64_t PosixWritableFile::GetFileSize(const IOOptions& /*opts*/,
                                        IODebugContext* /*dbg*/) {
  return filesize_;
}

void PosixWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
#ifdef OS_LINUX
// Suppress Valgrind "Unimplemented functionality" error.
#ifndef ROCKSDB_VALGRIND_RUN
  uint64_t fcntl_hint = hint;

  if (hint == write_hint_) {
    return;
  }
  if (fcntl(fd_, F_SET_RW_HINT, &fcntl_hint) == 0) {
    write_hint_ = hint;
  }
#else
  (void)hint;
#endif  // ROCKSDB_VALGRIND_RUN
#else
  (void)hint;
#endif  // OS_LINUX
}

IOStatus PosixWritableFile::InvalidateCache(size_t offset, size_t length) {
  if (use_direct_io()) {
    return IOStatus::OK();
  }
#ifndef OS_LINUX
  (void)offset;
  (void)length;
  return IOStatus::OK();
#else
  // free OS pages
  int ret = Fadvise(fd_, offset, length, POSIX_FADV_DONTNEED);
  if (ret == 0) {
    return IOStatus::OK();
  }
  return IOError("While fadvise NotNeeded", filename_, errno);
#endif
}

#ifdef ROCKSDB_FALLOCATE_PRESENT
IOStatus PosixWritableFile::Allocate(uint64_t offset, uint64_t len,
                                     const IOOptions& /*opts*/,
                                     IODebugContext* /*dbg*/) {
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(len <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  TEST_KILL_RANDOM("PosixWritableFile::Allocate:0");
  IOSTATS_TIMER_GUARD(allocate_nanos);
  int alloc_status = 0;
  if (allow_fallocate_) {
    alloc_status =
        fallocate(fd_, fallocate_with_keep_size_ ? FALLOC_FL_KEEP_SIZE : 0,
                  static_cast<off_t>(offset), static_cast<off_t>(len));
  }
  if (alloc_status == 0) {
    return IOStatus::OK();
  } else {
    return IOError("While fallocate offset " + std::to_string(offset) +
                       " len " + std::to_string(len),
                   filename_, errno);
  }
}
#endif

IOStatus PosixWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                      const IOOptions& opts,
                                      IODebugContext* dbg) {
#ifdef ROCKSDB_RANGESYNC_PRESENT
  assert(offset <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  assert(nbytes <= static_cast<uint64_t>(std::numeric_limits<off_t>::max()));
  if (sync_file_range_supported_) {
    int ret;
    if (strict_bytes_per_sync_) {
      // Specifying `SYNC_FILE_RANGE_WAIT_BEFORE` together with an offset/length
      // that spans all bytes written so far tells `sync_file_range` to wait for
      // any outstanding writeback requests to finish before issuing a new one.
      ret =
          sync_file_range(fd_, 0, static_cast<off_t>(offset + nbytes),
                          SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE);
    } else {
      ret = sync_file_range(fd_, static_cast<off_t>(offset),
                            static_cast<off_t>(nbytes), SYNC_FILE_RANGE_WRITE);
    }
    if (ret != 0) {
      return IOError("While sync_file_range returned " + std::to_string(ret),
                     filename_, errno);
    }
    return IOStatus::OK();
  }
#endif  // ROCKSDB_RANGESYNC_PRESENT
  return FSWritableFile::RangeSync(offset, nbytes, opts, dbg);
}

#ifdef OS_LINUX
size_t PosixWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return PosixHelper::GetUniqueIdFromFile(fd_, id, max_size);
}
#endif

/*
 * PosixRandomRWFile
 */

PosixRandomRWFile::PosixRandomRWFile(const std::string& fname, int fd,
                                     const EnvOptions& /*options*/)
    : filename_(fname), fd_(fd) {}

PosixRandomRWFile::~PosixRandomRWFile() {
  if (fd_ >= 0) {
    IOStatus s = PosixRandomRWFile::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixRandomRWFile::Write(uint64_t offset, const Slice& data,
                                  const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  const char* src = data.data();
  size_t nbytes = data.size();
  if (!PosixPositionedWrite(fd_, src, nbytes, static_cast<off_t>(offset))) {
    return IOError("While write random read/write file at offset " +
                       std::to_string(offset),
                   filename_, errno);
  }

  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Read(uint64_t offset, size_t n,
                                 const IOOptions& /*opts*/, Slice* result,
                                 char* scratch, IODebugContext* /*dbg*/) const {
  size_t left = n;
  char* ptr = scratch;
  while (left > 0) {
    ssize_t done = pread(fd_, ptr, left, offset);
    if (done < 0) {
      // error while reading from file
      if (errno == EINTR) {
        // read was interrupted, try again.
        continue;
      }
      return IOError("While reading random read/write file offset " +
                         std::to_string(offset) + " len " + std::to_string(n),
                     filename_, errno);
    } else if (done == 0) {
      // Nothing more to read
      break;
    }

    // Read `done` bytes
    ptr += done;
    offset += done;
    left -= done;
  }

  *result = Slice(scratch, n - left);
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Flush(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Sync(const IOOptions& /*opts*/,
                                 IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC) random rw file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fdatasync(fd_) < 0) {
    return IOError("While fdatasync random read/write file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Fsync(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
#ifdef HAVE_FULLFSYNC
  if (::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("While fcntl(F_FULLSYNC) random rw file", filename_, errno);
  }
#else   // HAVE_FULLFSYNC
  if (fsync(fd_) < 0) {
    return IOError("While fsync random read/write file", filename_, errno);
  }
#endif  // HAVE_FULLFSYNC
  return IOStatus::OK();
}

IOStatus PosixRandomRWFile::Close(const IOOptions& /*opts*/,
                                  IODebugContext* /*dbg*/) {
  if (close(fd_) < 0) {
    return IOError("While close random read/write file", filename_, errno);
  }
  fd_ = -1;
  return IOStatus::OK();
}

PosixMemoryMappedFileBuffer::~PosixMemoryMappedFileBuffer() {
  // TODO should have error handling though not much we can do...
  munmap(this->base_, length_);
}

/*
 * PosixDirectory
 */
#if !defined(BTRFS_SUPER_MAGIC)
// The magic number for BTRFS is fixed, if it's not defined, define it here
#define BTRFS_SUPER_MAGIC 0x9123683E
#endif
PosixDirectory::PosixDirectory(int fd, const std::string& directory_name)
    : fd_(fd), directory_name_(directory_name) {
  is_btrfs_ = false;
#ifdef OS_LINUX
  struct statfs buf;
  int ret = fstatfs(fd, &buf);
  is_btrfs_ = (ret == 0 && buf.f_type == static_cast<decltype(buf.f_type)>(
                                             BTRFS_SUPER_MAGIC));
#endif
}

PosixDirectory::~PosixDirectory() {
  if (fd_ >= 0) {
    IOStatus s = PosixDirectory::Close(IOOptions(), nullptr);
    s.PermitUncheckedError();
  }
}

IOStatus PosixDirectory::Fsync(const IOOptions& opts, IODebugContext* dbg) {
  return FsyncWithDirOptions(opts, dbg, DirFsyncOptions());
}

// Users who want the file entries synced in Directory project must call a
// Fsync or FsyncWithDirOptions function before Close
IOStatus PosixDirectory::Close(const IOOptions& /*opts*/,
                               IODebugContext* /*dbg*/) {
  IOStatus s = IOStatus::OK();
  if (close(fd_) < 0) {
    s = IOError("While closing directory ", directory_name_, errno);
  } else {
    fd_ = -1;
  }
  return s;
}

IOStatus PosixDirectory::FsyncWithDirOptions(
    const IOOptions& /*opts*/, IODebugContext* /*dbg*/,
    const DirFsyncOptions& dir_fsync_options) {
  assert(fd_ >= 0);  // Check use after close
  IOStatus s = IOStatus::OK();
#ifndef OS_AIX
  bool test_is_btrfs = is_btrfs_;
  TEST_SYNC_POINT_CALLBACK("PosixDirectory::FsyncWithDirOptions:ForceBtrfs",
                           &test_is_btrfs);
  if (test_is_btrfs) {
    // skip dir fsync for new file creation, which is not needed for btrfs
    if (dir_fsync_options.reason == DirFsyncOptions::kNewFileSynced) {
      return s;
    }
    // skip dir fsync for renaming file, only need to sync new file
    if (dir_fsync_options.reason == DirFsyncOptions::kFileRenamed) {
      std::string new_name = dir_fsync_options.renamed_new_name;
      assert(!new_name.empty());
      int fd;
      do {
        IOSTATS_TIMER_GUARD(open_nanos);
        fd = open(new_name.c_str(), O_RDONLY);
      } while (fd < 0 && errno == EINTR);
      if (fd < 0) {
        s = IOError("While open renaming file", new_name, errno);
      } else if (fsync(fd) < 0) {
        s = IOError("While fsync renaming file", new_name, errno);
      }
      if (fd >= 0 && close(fd) < 0) {
        s = IOError("While closing file after fsync", new_name, errno);
      }
      return s;
    }
    // fallback to dir-fsync for kDefault, kDirRenamed and kFileDeleted
  }

  // skip fsync/fcntl when fd_ == -1 since this file descriptor has been closed
  // in either the de-construction or the close function, data must have been
  // fsync-ed before de-construction and close is called
#ifdef HAVE_FULLFSYNC
  // btrfs is a Linux file system, while currently F_FULLFSYNC is available on
  // Mac OS.
  assert(!is_btrfs_);
  if (fd_ != -1 && ::fcntl(fd_, F_FULLFSYNC) < 0) {
    return IOError("while fcntl(F_FULLFSYNC)", "a directory", errno);
  }
#else   // HAVE_FULLFSYNC
  if (fd_ != -1 && fsync(fd_) == -1) {
    s = IOError("While fsync", "a directory", errno);
  }
#endif  // HAVE_FULLFSYNC
#endif  // OS_AIX
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
