//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <stdint.h>
#include <windows.h>

#include <mutex>
#include <string>

#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
#include "util/aligned_buffer.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace port {

std::string GetWindowsErrSz(DWORD err);

inline IOStatus IOErrorFromWindowsError(const std::string& context, DWORD err) {
  return ((err == ERROR_HANDLE_DISK_FULL) || (err == ERROR_DISK_FULL))
             ? IOStatus::NoSpace(context, GetWindowsErrSz(err))
         : ((err == ERROR_FILE_NOT_FOUND) || (err == ERROR_PATH_NOT_FOUND))
             ? IOStatus::PathNotFound(context, GetWindowsErrSz(err))
             : IOStatus::IOError(context, GetWindowsErrSz(err));
}

inline IOStatus IOErrorFromLastWindowsError(const std::string& context) {
  return IOErrorFromWindowsError(context, GetLastError());
}

inline IOStatus IOError(const std::string& context, int err_number) {
  return (err_number == ENOSPC)
             ? IOStatus::NoSpace(context, errnoStr(err_number).c_str())
         : (err_number == ENOENT)
             ? IOStatus::PathNotFound(context, errnoStr(err_number).c_str())
             : IOStatus::IOError(context, errnoStr(err_number).c_str());
}

struct Win_IOHandle;
class WinFileData;

IOStatus pwrite(const WinFileData* file_data, const Slice& data,
                uint64_t offset, size_t& bytes_written);

IOStatus pread(const WinFileData* file_data, char* src, size_t num_bytes,
               uint64_t offset, size_t& bytes_read);

IOStatus fallocate(const std::string& filename, HANDLE hFile, uint64_t to_size);

IOStatus ftruncate(const std::string& filename, HANDLE hFile, uint64_t toSize);

size_t GetUniqueIdFromFile(HANDLE hFile, char* id, size_t max_size);

class WinFileData {
 protected:
  const std::string filename_;
  HANDLE hFile_;
  // If true, the I/O issued would be direct I/O which the buffer
  // will need to be aligned (not sure there is a guarantee that the buffer
  // passed in is aligned).
  const bool use_direct_io_;
  const size_t sector_size_;

 public:
  // We want this class be usable both for inheritance (prive
  // or protected) and for containment so __ctor and __dtor public
  WinFileData(const std::string& filename, HANDLE hFile, bool direct_io);

  virtual ~WinFileData() { this->CloseFile(); }

  bool CloseFile() {
    bool result = true;

    if (hFile_ != NULL && hFile_ != INVALID_HANDLE_VALUE) {
      result = ::CloseHandle(hFile_);
      assert(result);
      hFile_ = NULL;
    }
    return result;
  }

  const std::string& GetName() const { return filename_; }

  HANDLE GetFileHandle() const { return hFile_; }

  bool use_direct_io() const { return use_direct_io_; }

  size_t GetSectorSize() const { return sector_size_; }

  bool IsSectorAligned(const size_t off) const;

  WinFileData(const WinFileData&) = delete;
  WinFileData& operator=(const WinFileData&) = delete;
};

class WinSequentialFile : protected WinFileData, public FSSequentialFile {
  // Override for behavior change when creating a custom env
  virtual IOStatus PositionedReadInternal(char* src, size_t numBytes,
                                          uint64_t offset,
                                          size_t& bytes_read) const;

 public:
  WinSequentialFile(const std::string& fname, HANDLE f,
                    const FileOptions& options);

  ~WinSequentialFile();

  WinSequentialFile(const WinSequentialFile&) = delete;
  WinSequentialFile& operator=(const WinSequentialFile&) = delete;

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;

  IOStatus Skip(uint64_t n) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  bool use_direct_io() const override { return WinFileData::use_direct_io(); }
};

// mmap() based random-access
class WinMmapReadableFile : private WinFileData, public FSRandomAccessFile {
  HANDLE hMap_;

  const void* mapped_region_;
  const size_t length_;

 public:
  // mapped_region_[0,length-1] contains the mmapped contents of the file.
  WinMmapReadableFile(const std::string& fileName, HANDLE hFile, HANDLE hMap,
                      const void* mapped_region, size_t length);

  ~WinMmapReadableFile();

  WinMmapReadableFile(const WinMmapReadableFile&) = delete;
  WinMmapReadableFile& operator=(const WinMmapReadableFile&) = delete;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  size_t GetUniqueId(char* id, size_t max_size) const override;

  IOStatus GetFileSize(uint64_t* file_size) override;
};

// We preallocate and use memcpy to append new
// data to the file.  This is safe since we either properly close the
// file before reading from it, or for log files, the reading code
// knows enough to skip zero suffixes.
class WinMmapFile : private WinFileData, public FSWritableFile {
 private:
  HANDLE hMap_;

  const size_t page_size_;  // We flush the mapping view in page_size
  // increments. We may decide if this is a memory
  // page size or SSD page size
  const size_t
      allocation_granularity_;  // View must start at such a granularity

  size_t reserved_size_;  // Preallocated size

  size_t mapping_size_;  // The max size of the mapping object
  // we want to guess the final file size to minimize the remapping
  size_t view_size_;  // How much memory to map into a view at a time

  char* mapped_begin_;  // Must begin at the file offset that is aligned with
  // allocation_granularity_
  char* mapped_end_;
  char* dst_;  // Where to write next  (in range [mapped_begin_,mapped_end_])
  char* last_sync_;  // Where have we synced up to

  uint64_t file_offset_;  // Offset of mapped_begin_ in file

  // Do we have unsynced writes?
  bool pending_sync_;

  // Can only truncate or reserve to a sector size aligned if
  // used on files that are opened with Unbuffered I/O
  IOStatus TruncateFile(uint64_t toSize);

  IOStatus UnmapCurrentRegion();

  IOStatus MapNewRegion(const IOOptions& options, IODebugContext* dbg);

  virtual IOStatus PreallocateInternal(uint64_t spaceToReserve);

 public:
  WinMmapFile(const std::string& fname, HANDLE hFile, size_t page_size,
              size_t allocation_granularity, const FileOptions& options);

  ~WinMmapFile();

  WinMmapFile(const WinMmapFile&) = delete;
  WinMmapFile& operator=(const WinMmapFile&) = delete;

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  const DataVerificationInfo& /* verification_info */,
                  IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  // Flush only data
  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  /**
   * Flush data as well as metadata to stable storage.
   */
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  /**
   * Get the size of valid data in the file. This will not match the
   * size that is returned from the filesystem because we use mmap
   * to extend file by map_size every time.
   */
  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override;

  size_t GetUniqueId(char* id, size_t max_size) const override;
};

class WinRandomAccessImpl {
 protected:
  WinFileData* file_base_;
  size_t alignment_;

  // Override for behavior change when creating a custom env
  virtual IOStatus PositionedReadInternal(char* src, size_t num_bytes,
                                          uint64_t offset,
                                          size_t& bytes_read) const;

  WinRandomAccessImpl(WinFileData* file_base, size_t alignment,
                      const FileOptions& options);

  virtual ~WinRandomAccessImpl() {}

  IOStatus ReadImpl(uint64_t offset, size_t n, Slice* result,
                    char* scratch) const;

  size_t GetAlignment() const { return alignment_; }

 public:
  WinRandomAccessImpl(const WinRandomAccessImpl&) = delete;
  WinRandomAccessImpl& operator=(const WinRandomAccessImpl&) = delete;
};

// pread() based random-access
class WinRandomAccessFile
    : private WinFileData,
      protected WinRandomAccessImpl,  // Want to be able to override
                                      // PositionedReadInternal
      public FSRandomAccessFile {
 public:
  WinRandomAccessFile(const std::string& fname, HANDLE hFile, size_t alignment,
                      const FileOptions& options);

  virtual ~WinRandomAccessFile() = default;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  size_t GetUniqueId(char* id, size_t max_size) const override;

  bool use_direct_io() const override { return WinFileData::use_direct_io(); }

  IOStatus InvalidateCache(size_t offset, size_t length) override;

  size_t GetRequiredBufferAlignment() const override;

  IOStatus GetFileSize(uint64_t* file_size) override;
};

// hFile opened with FILE_FLAG_OVERLAPPED
class WinRandomAccessFileAsyncIo : public WinRandomAccessFile {
 public:
  static constexpr size_t kMaxConcurrentRequests = 32;
  static constexpr DWORD kErrorCustomized = (DWORD)-1;

  WinRandomAccessFileAsyncIo(const std::string& fname, HANDLE hFile,
                             size_t alignment, const FileOptions& options);

  virtual ~WinRandomAccessFileAsyncIo() = default;

  virtual IOStatus PositionedReadInternal(char* src, size_t num_bytes,
                                          uint64_t offset,
                                          size_t& bytes_read) const override;

  virtual IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                             const IOOptions& options,
                             IODebugContext* dbg) override;
  // EXPERIMENTAL
  virtual IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                             std::function<void(FSReadRequest&, void*)> cb,
                             void* cb_arg, void** io_handle,
                             IOHandleDeleter* del_fn,
                             IODebugContext* dbg) override;
};

// This is a sequential write class. It has been mimicked (as others) after
// the original Posix class. We add support for unbuffered I/O on windows as
// well
// we utilize the original buffer as an alignment buffer to write directly to
// file with no buffering.
// No buffering requires that the provided buffer is aligned to the physical
// sector size (SSD page size) and
// that all SetFilePointer() operations to occur with such an alignment.
// We thus always write in sector/page size increments to the drive and leave
// the tail for the next write OR for Close() at which point we pad with zeros.
// No padding is required for
// buffered access.
class WinWritableImpl {
 protected:
  WinFileData* file_data_;
  const uint64_t alignment_;
  uint64_t
      next_write_offset_;  // Needed because Windows does not support O_APPEND
  uint64_t reservedsize_;  // how far we have reserved space

  virtual IOStatus PreallocateInternal(uint64_t spaceToReserve);

  WinWritableImpl(WinFileData* file_data, size_t alignment);

  ~WinWritableImpl() {}

  uint64_t GetAlignment() const { return alignment_; }

  IOStatus AppendImpl(const Slice& data);

  // Requires that the data is aligned as specified by
  // GetRequiredBufferAlignment()
  IOStatus PositionedAppendImpl(const Slice& data, uint64_t offset);

  IOStatus TruncateImpl(uint64_t size);

  IOStatus CloseImpl();

  IOStatus SyncImpl(const IOOptions& options, IODebugContext* dbg);

  uint64_t GetFileNextWriteOffset() {
    // Double accounting now here with WritableFileWriter
    // and this size will be wrong when unbuffered access is used
    // but tests implement their own writable files and do not use
    // WritableFileWrapper
    // so we need to squeeze a square peg through
    // a round hole here.
    return next_write_offset_;
  }

  IOStatus AllocateImpl(uint64_t offset, uint64_t len);

 public:
  WinWritableImpl(const WinWritableImpl&) = delete;
  WinWritableImpl& operator=(const WinWritableImpl&) = delete;
};

class WinWritableFile : private WinFileData,
                        protected WinWritableImpl,
                        public FSWritableFile {
 public:
  WinWritableFile(const std::string& fname, HANDLE hFile, size_t alignment,
                  size_t capacity, const FileOptions& options);

  ~WinWritableFile();

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  const DataVerificationInfo& /* verification_info */,
                  IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }

  // Requires that the data is aligned as specified by
  // GetRequiredBufferAlignment()
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& options,
                            IODebugContext* dbg) override;
  IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                            const IOOptions& opts,
                            const DataVerificationInfo& /* verification_info */,
                            IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }

  // Need to implement this so the file is truncated correctly
  // when buffered and unbuffered mode
  IOStatus Truncate(uint64_t size, const IOOptions& options,
                    IODebugContext* dbg) override;

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  // write out the cached data to the OS cache
  // This is now taken care of the WritableFileWriter
  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;

  bool IsSyncThreadSafe() const override;

  // Indicates if the class makes use of direct I/O
  // Use PositionedAppend
  bool use_direct_io() const override;

  size_t GetRequiredBufferAlignment() const override;

  uint64_t GetFileSize(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Allocate(uint64_t offset, uint64_t len, const IOOptions& options,
                    IODebugContext* dbg) override;

  size_t GetUniqueId(char* id, size_t max_size) const override;
};

class WinRandomRWFile : private WinFileData,
                        protected WinRandomAccessImpl,
                        protected WinWritableImpl,
                        public FSRandomRWFile {
 public:
  WinRandomRWFile(const std::string& fname, HANDLE hFile, size_t alignment,
                  const FileOptions& options);

  ~WinRandomRWFile() {}

  // Indicates if the class makes use of direct I/O
  // If false you must pass aligned buffer to Write()
  bool use_direct_io() const override;

  // Use the returned alignment value to allocate aligned
  // buffer for Write() when use_direct_io() returns true
  size_t GetRequiredBufferAlignment() const override;

  // Write bytes in `data` at  offset `offset`, Returns Status::OK() on success.
  // Pass aligned buffer when use_direct_io() returns true.
  IOStatus Write(uint64_t offset, const Slice& data, const IOOptions& options,
                 IODebugContext* dbg) override;

  // Read up to `n` bytes starting from offset `offset` and store them in
  // result, provided `scratch` size should be at least `n`.
  // Returns Status::OK() on success.
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Flush(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;

  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
    return Sync(options, dbg);
  }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;
};

class WinMemoryMappedBuffer : public MemoryMappedFileBuffer {
 private:
  HANDLE file_handle_;
  HANDLE map_handle_;

 public:
  WinMemoryMappedBuffer(HANDLE file_handle, HANDLE map_handle, void* base,
                        size_t size)
      : MemoryMappedFileBuffer(base, size),
        file_handle_(file_handle),
        map_handle_(map_handle) {}
  ~WinMemoryMappedBuffer() override;
};

class WinDirectory : public FSDirectory {
  const std::string filename_;
  HANDLE handle_;

 public:
  explicit WinDirectory(const std::string& filename, HANDLE h) noexcept
      : filename_(filename), handle_(h) {
    assert(handle_ != INVALID_HANDLE_VALUE);
  }
  ~WinDirectory() {
    if (handle_ != NULL) {
      IOStatus s = WinDirectory::Close(IOOptions(), nullptr);
      s.PermitUncheckedError();
    }
  }
  const std::string& GetName() const { return filename_; }
  IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override;
  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override;

  size_t GetUniqueId(char* id, size_t max_size) const override;
};

class WinFileLock : public FileLock {
 public:
  explicit WinFileLock(HANDLE hFile) : hFile_(hFile) {
    assert(hFile != NULL);
    assert(hFile != INVALID_HANDLE_VALUE);
  }

  ~WinFileLock();

 private:
  HANDLE hFile_;
};

// This class auto closes overlapped.hEvent during destruction if it is not NULL
struct Win_IOHandle {
  std::function<void(FSReadRequest&, void*)> cb;
  void* cb_arg;
  uint64_t offset;
  size_t len;
  char* scratch;
  OVERLAPPED overlapped;
  bool is_finished;
  WinFileData* file_data;

  Win_IOHandle(std::function<void(FSReadRequest&, void*)> _cb, void* _cb_arg,
               uint64_t _offset, size_t _len, char* _scratch,
               WinFileData* _file_data);
  ~Win_IOHandle();
};

}  // namespace port
}  // namespace ROCKSDB_NAMESPACE
