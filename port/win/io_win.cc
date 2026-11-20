//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#if defined(OS_WIN)

#include "port/win/io_win.h"

#include "env_win.h"
#include "monitoring/iostats_context_imp.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace port {

/*
 * DirectIOHelper
 */
namespace {

const size_t kSectorSize = 512;

inline bool IsPowerOfTwo(const size_t alignment) {
  return ((alignment) & (alignment - 1)) == 0;
}

inline bool IsAligned(size_t alignment, const void* ptr) {
  return ((uintptr_t(ptr)) & (alignment - 1)) == 0;
}
}  // namespace

std::string GetWindowsErrSz(DWORD err) {
  std::string Err;
  LPSTR lpMsgBuf = nullptr;
  FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
                     FORMAT_MESSAGE_IGNORE_INSERTS,
                 NULL, err,
                 0,  // Default language
                 reinterpret_cast<LPSTR>(&lpMsgBuf), 0, NULL);

  if (lpMsgBuf) {
    Err = lpMsgBuf;
    LocalFree(lpMsgBuf);
  }
  return Err;
}

// We preserve the original name of this interface to denote the original idea
// behind it.
// All reads happen by a specified offset and pwrite interface does not change
// the position of the file pointer. Judging from the man page and errno it does
// execute
// lseek atomically to return the position of the file back where it was.
// WriteFile() does not
// have this capability. Therefore, for both pread and pwrite the pointer is
// advanced to the next position
// which is fine for writes because they are (should be) sequential.
// Because all the reads/writes happen by the specified offset, the caller in
// theory should not
// rely on the current file offset.
IOStatus pwrite(const WinFileData* file_data, const Slice& data,
                uint64_t offset, size_t& bytes_written) {
  IOStatus s;
  bytes_written = 0;

  size_t num_bytes = data.size();
  if (num_bytes > std::numeric_limits<DWORD>::max()) {
    // May happen in 64-bit builds where size_t is 64-bits but
    // long is still 32-bit, but that's the API here at the moment
    return IOStatus::InvalidArgument(
        "num_bytes is too large for a single write: " + file_data->GetName());
  }

  OVERLAPPED overlapped = {0};
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  DWORD bytesWritten = 0;

  if (FALSE == WriteFile(file_data->GetFileHandle(), data.data(),
                         static_cast<DWORD>(num_bytes), &bytesWritten,
                         &overlapped)) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("WriteFile failed: " + file_data->GetName(),
                                lastError);
  } else {
    bytes_written = bytesWritten;
  }

  return s;
}

// See comments for pwrite above
IOStatus pread(const WinFileData* file_data, char* src, size_t num_bytes,
               uint64_t offset, size_t& bytes_read) {
  IOStatus s;
  bytes_read = 0;

  if (num_bytes > std::numeric_limits<DWORD>::max()) {
    return IOStatus::InvalidArgument(
        "num_bytes is too large for a single read: " + file_data->GetName());
  }

  OVERLAPPED overlapped = {0};
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  DWORD bytesRead = 0;

  if (FALSE == ReadFile(file_data->GetFileHandle(), src,
                        static_cast<DWORD>(num_bytes), &bytesRead,
                        &overlapped)) {
    auto lastError = GetLastError();
    // EOF is OK with zero bytes read
    if (lastError != ERROR_HANDLE_EOF) {
      s = IOErrorFromWindowsError("ReadFile failed: " + file_data->GetName(),
                                  lastError);
    }
  } else {
    bytes_read = bytesRead;
  }

  return s;
}

// SetFileInformationByHandle() is capable of fast pre-allocates.
// However, this does not change the file end position unless the file is
// truncated and the pre-allocated space is not considered filled with zeros.
IOStatus fallocate(const std::string& filename, HANDLE hFile,
                   uint64_t to_size) {
  IOStatus status;

  FILE_ALLOCATION_INFO alloc_info;
  alloc_info.AllocationSize.QuadPart = to_size;

  if (!SetFileInformationByHandle(hFile, FileAllocationInfo, &alloc_info,
                                  sizeof(FILE_ALLOCATION_INFO))) {
    auto lastError = GetLastError();
    status = IOErrorFromWindowsError(
        "Failed to pre-allocate space: " + filename, lastError);
  }

  return status;
}

IOStatus ftruncate(const std::string& filename, HANDLE hFile, uint64_t toSize) {
  IOStatus status;

  FILE_END_OF_FILE_INFO end_of_file;
  end_of_file.EndOfFile.QuadPart = toSize;

  if (!SetFileInformationByHandle(hFile, FileEndOfFileInfo, &end_of_file,
                                  sizeof(FILE_END_OF_FILE_INFO))) {
    auto lastError = GetLastError();
    status = IOErrorFromWindowsError("Failed to Set end of file: " + filename,
                                     lastError);
  }

  return status;
}

size_t GetUniqueIdFromFile(HANDLE /*hFile*/, char* /*id*/,
                           size_t /*max_size*/) {
  // Returning 0 is safe as it causes the table reader to generate a unique ID.
  // This is suboptimal for performance as it prevents multiple table readers
  // for the same file from sharing cached blocks. For example, if users have
  // a low value for `max_open_files`, there can be many table readers opened
  // for the same file.
  //
  // TODO: this is a temporarily solution as it is safe but not optimal for
  // performance. For more details see discussion in
  // https://github.com/meta/rocksdb/pull/5844.
  return 0;
}

WinFileData::WinFileData(const std::string& filename, HANDLE hFile,
                         bool direct_io)
    : filename_(filename),
      hFile_(hFile),
      use_direct_io_(direct_io),
      sector_size_(WinFileSystem::GetSectorSize(filename)) {}

bool WinFileData::IsSectorAligned(const size_t off) const {
  return (off & (sector_size_ - 1)) == 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// WinMmapReadableFile

WinMmapReadableFile::WinMmapReadableFile(const std::string& fileName,
                                         HANDLE hFile, HANDLE hMap,
                                         const void* mapped_region,
                                         size_t length)
    : WinFileData(fileName, hFile, false /* use_direct_io */),
      hMap_(hMap),
      mapped_region_(mapped_region),
      length_(length) {}

WinMmapReadableFile::~WinMmapReadableFile() {
  BOOL ret __attribute__((__unused__));
  ret = ::UnmapViewOfFile(mapped_region_);
  assert(ret);

  ret = ::CloseHandle(hMap_);
  assert(ret);
}

IOStatus WinMmapReadableFile::Read(uint64_t offset, size_t n,
                                   const IOOptions& /*options*/, Slice* result,
                                   char* scratch,
                                   IODebugContext* /*dbg*/) const {
  IOStatus s;

  if (offset > length_) {
    *result = Slice();
    return IOError(filename_, EINVAL);
  } else if (offset + n > length_) {
    n = length_ - static_cast<size_t>(offset);
  }
  *result = Slice(static_cast<const char*>(mapped_region_) + offset, n);
  return s;
}

IOStatus WinMmapReadableFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::OK();
}

size_t WinMmapReadableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

IOStatus WinMmapReadableFile::GetFileSize(uint64_t* size) {
  LARGE_INTEGER fileSize;
  if (GetFileSizeEx(hFile_, &fileSize)) {
    *size = fileSize.QuadPart;
    return IOStatus::OK();
  } else {
    return IOStatus::IOError("Failed to get file size", filename_);
  }
}

///////////////////////////////////////////////////////////////////////////////
/// WinMmapFile

// Can only truncate or reserve to a sector size aligned if
// used on files that are opened with Unbuffered I/O
IOStatus WinMmapFile::TruncateFile(uint64_t toSize) {
  return ftruncate(filename_, hFile_, toSize);
}

IOStatus WinMmapFile::UnmapCurrentRegion() {
  IOStatus status;

  if (mapped_begin_ != nullptr) {
    if (!::UnmapViewOfFile(mapped_begin_)) {
      status = IOErrorFromWindowsError(
          "Failed to unmap file view: " + filename_, GetLastError());
    }

    // Move on to the next portion of the file
    file_offset_ += view_size_;

    // UnmapView automatically sends data to disk but not the metadata
    // which is good and provides some equivalent of fdatasync() on Linux
    // therefore, we donot need separate flag for metadata
    mapped_begin_ = nullptr;
    mapped_end_ = nullptr;
    dst_ = nullptr;

    last_sync_ = nullptr;
    pending_sync_ = false;
  }

  return status;
}

IOStatus WinMmapFile::MapNewRegion(const IOOptions& options,
                                   IODebugContext* dbg) {
  IOStatus status;

  assert(mapped_begin_ == nullptr);

  size_t minDiskSize = static_cast<size_t>(file_offset_) + view_size_;

  if (minDiskSize > reserved_size_) {
    status = Allocate(file_offset_, view_size_, options, dbg);
    if (!status.ok()) {
      return status;
    }
  }

  // Need to remap
  if (hMap_ == NULL || reserved_size_ > mapping_size_) {
    if (hMap_ != NULL) {
      // Unmap the previous one
      BOOL ret __attribute__((__unused__));
      ret = ::CloseHandle(hMap_);
      assert(ret);
      hMap_ = NULL;
    }

    ULARGE_INTEGER mappingSize;
    mappingSize.QuadPart = reserved_size_;

    hMap_ = CreateFileMappingA(
        hFile_,
        NULL,                  // Security attributes
        PAGE_READWRITE,        // There is not a write only mode for mapping
        mappingSize.HighPart,  // Enable mapping the whole file but the actual
        // amount mapped is determined by MapViewOfFile
        mappingSize.LowPart,
        NULL);  // Mapping name

    if (NULL == hMap_) {
      return IOErrorFromWindowsError(
          "WindowsMmapFile failed to create file mapping for: " + filename_,
          GetLastError());
    }

    mapping_size_ = reserved_size_;
  }

  ULARGE_INTEGER offset;
  offset.QuadPart = file_offset_;

  // View must begin at the granularity aligned offset
  mapped_begin_ =
      static_cast<char*>(MapViewOfFileEx(hMap_, FILE_MAP_WRITE, offset.HighPart,
                                         offset.LowPart, view_size_, NULL));

  if (!mapped_begin_) {
    status = IOErrorFromWindowsError(
        "WindowsMmapFile failed to map file view: " + filename_,
        GetLastError());
  } else {
    mapped_end_ = mapped_begin_ + view_size_;
    dst_ = mapped_begin_;
    last_sync_ = mapped_begin_;
    pending_sync_ = false;
  }
  return status;
}

IOStatus WinMmapFile::PreallocateInternal(uint64_t spaceToReserve) {
  return fallocate(filename_, hFile_, spaceToReserve);
}

WinMmapFile::WinMmapFile(const std::string& fname, HANDLE hFile,
                         size_t page_size, size_t allocation_granularity,
                         const FileOptions& options)
    : WinFileData(fname, hFile, false),
      FSWritableFile(options),
      hMap_(NULL),
      page_size_(page_size),
      allocation_granularity_(allocation_granularity),
      reserved_size_(0),
      mapping_size_(0),
      view_size_(0),
      mapped_begin_(nullptr),
      mapped_end_(nullptr),
      dst_(nullptr),
      last_sync_(nullptr),
      file_offset_(0),
      pending_sync_(false) {
  // Allocation granularity must be obtained from GetSystemInfo() and must be
  // a power of two.
  assert(allocation_granularity > 0);
  assert((allocation_granularity & (allocation_granularity - 1)) == 0);

  assert(page_size > 0);
  assert((page_size & (page_size - 1)) == 0);

  // Only for memory mapped writes
  assert(options.use_mmap_writes);

  // View size must be both the multiple of allocation_granularity AND the
  // page size and the granularity is usually a multiple of a page size.
  const size_t viewSize =
      32 * 1024;  // 32Kb similar to the Windows File Cache in buffered mode
  view_size_ = Roundup(viewSize, allocation_granularity_);
}

WinMmapFile::~WinMmapFile() {
  if (hFile_) {
    this->Close(IOOptions(), nullptr);
  }
}

IOStatus WinMmapFile::Append(const Slice& data, const IOOptions& options,
                             IODebugContext* dbg) {
  const char* src = data.data();
  size_t left = data.size();

  while (left > 0) {
    assert(mapped_begin_ <= dst_);
    size_t avail = mapped_end_ - dst_;

    if (avail == 0) {
      IOStatus s = UnmapCurrentRegion();
      if (s.ok()) {
        s = MapNewRegion(options, dbg);
      }

      if (!s.ok()) {
        return s;
      }
    } else {
      size_t n = std::min(left, avail);
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
      pending_sync_ = true;
    }
  }

  // Now make sure that the last partial page is padded with zeros if needed
  size_t bytesToPad = Roundup(size_t(dst_), page_size_) - size_t(dst_);
  if (bytesToPad > 0) {
    memset(dst_, 0, bytesToPad);
  }

  return IOStatus::OK();
}

// Means Close() will properly take care of truncate
// and it does not need any additional information
IOStatus WinMmapFile::Truncate(uint64_t size, const IOOptions& /*options*/,
                               IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus WinMmapFile::Close(const IOOptions& options, IODebugContext* dbg) {
  IOStatus s;

  assert(NULL != hFile_);

  // We truncate to the precise size so no
  // uninitialized data at the end. SetEndOfFile
  // which we use does not write zeros and it is good.
  uint64_t targetSize = GetFileSize(options, dbg);

  if (mapped_begin_ != nullptr) {
    // Sync before unmapping to make sure everything
    // is on disk and there is not a lazy writing
    // so we are deterministic with the tests
    Sync(options, dbg);
    s = UnmapCurrentRegion();
  }

  if (NULL != hMap_) {
    BOOL ret = ::CloseHandle(hMap_);
    if (!ret && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
          "Failed to Close mapping for file: " + filename_, lastError);
    }

    hMap_ = NULL;
  }

  if (hFile_ != NULL) {
    TruncateFile(targetSize);

    BOOL ret = ::CloseHandle(hFile_);
    hFile_ = NULL;

    if (!ret && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
          "Failed to close file map handle: " + filename_, lastError);
    }
  }

  return s;
}

IOStatus WinMmapFile::Flush(const IOOptions& /*options*/,
                            IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

// Flush only data
IOStatus WinMmapFile::Sync(const IOOptions& /*options*/,
                           IODebugContext* /*dbg*/) {
  IOStatus s;

  // Some writes occurred since last sync
  if (dst_ > last_sync_) {
    assert(mapped_begin_);
    assert(dst_);
    assert(dst_ > mapped_begin_);
    assert(dst_ < mapped_end_);

    size_t page_begin =
        TruncateToPageBoundary(page_size_, last_sync_ - mapped_begin_);
    size_t page_end =
        TruncateToPageBoundary(page_size_, dst_ - mapped_begin_ - 1);

    // Flush only the amount of that is a multiple of pages
    if (!::FlushViewOfFile(mapped_begin_ + page_begin,
                           (page_end - page_begin) + page_size_)) {
      s = IOErrorFromWindowsError("Failed to FlushViewOfFile: " + filename_,
                                  GetLastError());
    } else {
      last_sync_ = dst_;
    }
  }

  return s;
}

/**
 * Flush data as well as metadata to stable storage.
 */
IOStatus WinMmapFile::Fsync(const IOOptions& options, IODebugContext* dbg) {
  IOStatus s = Sync(options, dbg);

  // Flush metadata
  if (s.ok() && pending_sync_) {
    if (!::FlushFileBuffers(hFile_)) {
      s = IOErrorFromWindowsError("Failed to FlushFileBuffers: " + filename_,
                                  GetLastError());
    }
    pending_sync_ = false;
  }

  return s;
}

/**
 * Get the size of valid data in the file. This will not match the
 * size that is returned from the filesystem because we use mmap
 * to extend file by map_size every time.
 */
uint64_t WinMmapFile::GetFileSize(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  size_t used = dst_ - mapped_begin_;
  return file_offset_ + used;
}

IOStatus WinMmapFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::OK();
}

IOStatus WinMmapFile::Allocate(uint64_t offset, uint64_t len,
                               const IOOptions& /*options*/,
                               IODebugContext* /*dbg*/) {
  IOStatus status;
  TEST_KILL_RANDOM("WinMmapFile::Allocate");

  // Make sure that we reserve an aligned amount of space
  // since the reservation block size is driven outside so we want
  // to check if we are ok with reservation here
  size_t spaceToReserve =
      Roundup(static_cast<size_t>(offset + len), view_size_);
  // Nothing to do
  if (spaceToReserve <= reserved_size_) {
    return status;
  }

  IOSTATS_TIMER_GUARD(allocate_nanos);
  status = PreallocateInternal(spaceToReserve);
  if (status.ok()) {
    reserved_size_ = spaceToReserve;
  }
  return status;
}

size_t WinMmapFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

//////////////////////////////////////////////////////////////////////////////////
// WinSequentialFile

WinSequentialFile::WinSequentialFile(const std::string& fname, HANDLE f,
                                     const FileOptions& options)
    : WinFileData(fname, f, options.use_direct_reads) {}

WinSequentialFile::~WinSequentialFile() {
  assert(hFile_ != INVALID_HANDLE_VALUE);
}

IOStatus WinSequentialFile::Read(size_t n, const IOOptions& /*opts*/,
                                 Slice* result, char* scratch,
                                 IODebugContext* /*dbg*/) {
  IOStatus s;
  size_t r = 0;

  assert(result != nullptr);
  if (WinFileData::use_direct_io()) {
    return IOStatus::NotSupported("Read() does not support direct_io");
  }

  // Windows ReadFile API accepts a DWORD.
  // While it is possible to read in a loop if n is too big
  // it is an unlikely case.
  if (n > std::numeric_limits<DWORD>::max()) {
    return IOStatus::InvalidArgument("n is too big for a single ReadFile: " +
                                     filename_);
  }

  DWORD bytesToRead =
      static_cast<DWORD>(n);  // cast is safe due to the check above
  DWORD bytesRead = 0;
  BOOL ret = ReadFile(hFile_, scratch, bytesToRead, &bytesRead, NULL);
  if (ret != FALSE) {
    r = bytesRead;
  } else {
    auto lastError = GetLastError();
    if (lastError != ERROR_HANDLE_EOF) {
      s = IOErrorFromWindowsError("ReadFile failed: " + filename_, lastError);
    }
  }

  *result = Slice(scratch, r);
  return s;
}

IOStatus WinSequentialFile::PositionedReadInternal(char* src, size_t numBytes,
                                                   uint64_t offset,
                                                   size_t& bytes_read) const {
  return pread(this, src, numBytes, offset, bytes_read);
}

IOStatus WinSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                           const IOOptions& /*opts*/,
                                           Slice* result, char* scratch,
                                           IODebugContext* /*dbg*/) {
  if (!WinFileData::use_direct_io()) {
    return IOStatus::NotSupported("This function is only used for direct_io");
  }

  assert(IsSectorAligned(static_cast<size_t>(offset)));
  assert(IsSectorAligned(static_cast<size_t>(n)));

  size_t bytes_read = 0;  // out param
  IOStatus s = PositionedReadInternal(scratch, static_cast<size_t>(n), offset,
                                      bytes_read);
  *result = Slice(scratch, bytes_read);
  return s;
}

IOStatus WinSequentialFile::Skip(uint64_t n) {
  // Can't handle more than signed max as SetFilePointerEx accepts a signed
  // 64-bit integer. As such it is a highly unlikley case to have n so large.
  if (n > static_cast<uint64_t>(std::numeric_limits<LONGLONG>::max())) {
    return IOStatus::InvalidArgument(
        "n is too large for a single SetFilePointerEx() call" + filename_);
  }

  LARGE_INTEGER li;
  li.QuadPart = static_cast<LONGLONG>(n);  // cast is safe due to the check
                                           // above
  BOOL ret = SetFilePointerEx(hFile_, li, NULL, FILE_CURRENT);
  if (ret == FALSE) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError("Skip SetFilePointerEx():" + filename_,
                                   lastError);
  }
  return IOStatus::OK();
}

IOStatus WinSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::OK();
}

//////////////////////////////////////////////////////////////////////////////////////////////////
/// WinRandomAccessBase

inline IOStatus WinRandomAccessImpl::PositionedReadInternal(
    char* src, size_t numBytes, uint64_t offset, size_t& bytes_read) const {
  return pread(file_base_, src, numBytes, offset, bytes_read);
}

inline WinRandomAccessImpl::WinRandomAccessImpl(WinFileData* file_base,
                                                size_t alignment,
                                                const FileOptions& options)
    : file_base_(file_base),
      alignment_(std::max(alignment, file_base->GetSectorSize())) {
  assert(!options.use_mmap_reads);
}

inline IOStatus WinRandomAccessImpl::ReadImpl(uint64_t offset, size_t n,
                                              Slice* result,
                                              char* scratch) const {
  // Check buffer alignment
  if (file_base_->use_direct_io()) {
    assert(file_base_->IsSectorAligned(static_cast<size_t>(offset)));
    assert(IsAligned(alignment_, scratch));
  }

  if (n == 0) {
    *result = Slice(scratch, 0);
    return IOStatus::OK();
  }

  size_t bytes_read = 0;
  IOStatus s = PositionedReadInternal(scratch, n, offset, bytes_read);
  *result = Slice(scratch, bytes_read);
  return s;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// WinRandomAccessFile

WinRandomAccessFile::WinRandomAccessFile(const std::string& fname, HANDLE hFile,
                                         size_t alignment,
                                         const FileOptions& options)
    : WinFileData(fname, hFile, options.use_direct_reads),
      WinRandomAccessImpl(this, alignment, options) {}

WinRandomAccessFile::~WinRandomAccessFile() {}

IOStatus WinRandomAccessFile::Read(uint64_t offset, size_t n,
                                   const IOOptions& /*options*/, Slice* result,
                                   char* scratch,
                                   IODebugContext* /*dbg*/) const {
  return ReadImpl(offset, n, result, scratch);
}

IOStatus WinRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return IOStatus::OK();
}

size_t WinRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(GetFileHandle(), id, max_size);
}

size_t WinRandomAccessFile::GetRequiredBufferAlignment() const {
  return GetAlignment();
}

IOStatus WinRandomAccessFile::GetFileSize(uint64_t* size) {
  LARGE_INTEGER fileSize;
  if (GetFileSizeEx(hFile_, &fileSize)) {
    *size = fileSize.QuadPart;
    return IOStatus::OK();
  } else {
    return IOStatus::IOError("Failed to get file size", filename_);
  }
}

/////////////////////////////////////////////////////////////////////////////
// WinWritableImpl
//

inline IOStatus WinWritableImpl::PreallocateInternal(uint64_t spaceToReserve) {
  return fallocate(file_data_->GetName(), file_data_->GetFileHandle(),
                   spaceToReserve);
}

inline WinWritableImpl::WinWritableImpl(WinFileData* file_data,
                                        size_t alignment)
    : file_data_(file_data),
      alignment_(std::max(alignment, file_data->GetSectorSize())),
      next_write_offset_(0),
      reservedsize_(0) {
  // Query current position in case ReopenWritableFile is called
  // This position is only important for buffered writes
  // for unbuffered writes we explicitely specify the position.
  LARGE_INTEGER zero_move;
  zero_move.QuadPart = 0;  // Do not move
  LARGE_INTEGER pos;
  pos.QuadPart = 0;
  BOOL ret = SetFilePointerEx(file_data_->GetFileHandle(), zero_move, &pos,
                              FILE_CURRENT);
  // Querying no supped to fail
  if (ret != 0) {
    next_write_offset_ = pos.QuadPart;
  } else {
    assert(false);
  }
}

inline IOStatus WinWritableImpl::AppendImpl(const Slice& data) {
  IOStatus s;

  if (data.size() > std::numeric_limits<DWORD>::max()) {
    return IOStatus::InvalidArgument("data is too long for a single write" +
                                     file_data_->GetName());
  }

  size_t bytes_written = 0;  // out param

  if (file_data_->use_direct_io()) {
    // With no offset specified we are appending
    // to the end of the file
    assert(file_data_->IsSectorAligned(next_write_offset_));
    assert(file_data_->IsSectorAligned(data.size()));
    assert(IsAligned(static_cast<size_t>(GetAlignment()), data.data()));
    s = pwrite(file_data_, data, next_write_offset_, bytes_written);
  } else {
    DWORD bytesWritten = 0;
    if (!WriteFile(file_data_->GetFileHandle(), data.data(),
                   static_cast<DWORD>(data.size()), &bytesWritten, NULL)) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
          "Failed to WriteFile: " + file_data_->GetName(), lastError);
    } else {
      bytes_written = bytesWritten;
    }
  }

  if (s.ok()) {
    if (bytes_written == data.size()) {
      // This matters for direct_io cases where
      // we rely on the fact that next_write_offset_
      // is sector aligned
      next_write_offset_ += bytes_written;
    } else {
      s = IOStatus::IOError("Failed to write all bytes: " +
                            file_data_->GetName());
    }
  }

  return s;
}

inline IOStatus WinWritableImpl::PositionedAppendImpl(const Slice& data,
                                                      uint64_t offset) {
  if (file_data_->use_direct_io()) {
    assert(file_data_->IsSectorAligned(static_cast<size_t>(offset)));
    assert(file_data_->IsSectorAligned(data.size()));
    assert(IsAligned(static_cast<size_t>(GetAlignment()), data.data()));
  }

  size_t bytes_written = 0;
  IOStatus s = pwrite(file_data_, data, offset, bytes_written);

  if (s.ok()) {
    if (bytes_written == data.size()) {
      // For sequential write this would be simple
      // size extension by data.size()
      uint64_t write_end = offset + bytes_written;
      if (write_end >= next_write_offset_) {
        next_write_offset_ = write_end;
      }
    } else {
      s = IOStatus::IOError("Failed to write all of the requested data: " +
                            file_data_->GetName());
    }
  }
  return s;
}

inline IOStatus WinWritableImpl::TruncateImpl(uint64_t size) {
  // It is tempting to check for the size for sector alignment
  // but truncation may come at the end and there is not a requirement
  // for this to be sector aligned so long as we do not attempt to write
  // after that. The interface docs state that the behavior is undefined
  // in that case.
  IOStatus s =
      ftruncate(file_data_->GetName(), file_data_->GetFileHandle(), size);

  if (s.ok()) {
    next_write_offset_ = size;
  }
  return s;
}

inline IOStatus WinWritableImpl::CloseImpl() {
  IOStatus s;

  auto hFile = file_data_->GetFileHandle();
  assert(INVALID_HANDLE_VALUE != hFile);

  if (!::FlushFileBuffers(hFile)) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
        "FlushFileBuffers failed at Close() for: " + file_data_->GetName(),
        lastError);
  }

  if (!file_data_->CloseFile() && s.ok()) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
        "CloseHandle failed for: " + file_data_->GetName(), lastError);
  }
  return s;
}

inline IOStatus WinWritableImpl::SyncImpl(const IOOptions& /*options*/,
                                          IODebugContext* /*dbg*/) {
  IOStatus s;
  if (!::FlushFileBuffers(file_data_->GetFileHandle())) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
        "FlushFileBuffers failed at Sync() for: " + file_data_->GetName(),
        lastError);
  }
  return s;
}

inline IOStatus WinWritableImpl::AllocateImpl(uint64_t offset, uint64_t len) {
  IOStatus status;
  TEST_KILL_RANDOM("WinWritableFile::Allocate");

  // Make sure that we reserve an aligned amount of space
  // since the reservation block size is driven outside so we want
  // to check if we are ok with reservation here
  size_t spaceToReserve = Roundup(static_cast<size_t>(offset + len),
                                  static_cast<size_t>(alignment_));
  // Nothing to do
  if (spaceToReserve <= reservedsize_) {
    return status;
  }

  IOSTATS_TIMER_GUARD(allocate_nanos);
  status = PreallocateInternal(spaceToReserve);
  if (status.ok()) {
    reservedsize_ = spaceToReserve;
  }
  return status;
}

////////////////////////////////////////////////////////////////////////////////
/// WinWritableFile

WinWritableFile::WinWritableFile(const std::string& fname, HANDLE hFile,
                                 size_t alignment, size_t /* capacity */,
                                 const FileOptions& options)
    : WinFileData(fname, hFile, options.use_direct_writes),
      WinWritableImpl(this, alignment),
      FSWritableFile(options) {
  assert(!options.use_mmap_writes);
}

WinWritableFile::~WinWritableFile() {}

// Indicates if the class makes use of direct I/O
bool WinWritableFile::use_direct_io() const {
  return WinFileData::use_direct_io();
}

size_t WinWritableFile::GetRequiredBufferAlignment() const {
  return static_cast<size_t>(GetAlignment());
}

IOStatus WinWritableFile::Append(const Slice& data,
                                 const IOOptions& /*options*/,
                                 IODebugContext* /*dbg*/) {
  return AppendImpl(data);
}

IOStatus WinWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                           const IOOptions& /*options*/,
                                           IODebugContext* /*dbg*/) {
  return PositionedAppendImpl(data, offset);
}

// Need to implement this so the file is truncated correctly
// when buffered and unbuffered mode
IOStatus WinWritableFile::Truncate(uint64_t size, const IOOptions& /*options*/,
                                   IODebugContext* /*dbg*/) {
  return TruncateImpl(size);
}

IOStatus WinWritableFile::Close(const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  return CloseImpl();
}

// write out the cached data to the OS cache
// This is now taken care of the WritableFileWriter
IOStatus WinWritableFile::Flush(const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus WinWritableFile::Sync(const IOOptions& options, IODebugContext* dbg) {
  return SyncImpl(options, dbg);
}

IOStatus WinWritableFile::Fsync(const IOOptions& options, IODebugContext* dbg) {
  return SyncImpl(options, dbg);
}

bool WinWritableFile::IsSyncThreadSafe() const { return true; }

uint64_t WinWritableFile::GetFileSize(const IOOptions& /*options*/,
                                      IODebugContext* /*dbg*/) {
  return GetFileNextWriteOffset();
}

IOStatus WinWritableFile::Allocate(uint64_t offset, uint64_t len,
                                   const IOOptions& /*options*/,
                                   IODebugContext* /*dbg*/) {
  return AllocateImpl(offset, len);
}

size_t WinWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(GetFileHandle(), id, max_size);
}

/////////////////////////////////////////////////////////////////////////
/// WinRandomRWFile

WinRandomRWFile::WinRandomRWFile(const std::string& fname, HANDLE hFile,
                                 size_t alignment, const FileOptions& options)
    : WinFileData(fname, hFile,
                  options.use_direct_reads && options.use_direct_writes),
      WinRandomAccessImpl(this, alignment, options),
      WinWritableImpl(this, alignment) {}

bool WinRandomRWFile::use_direct_io() const {
  return WinFileData::use_direct_io();
}

size_t WinRandomRWFile::GetRequiredBufferAlignment() const {
  assert(WinRandomAccessImpl::GetAlignment() ==
         WinWritableImpl::GetAlignment());
  return static_cast<size_t>(WinRandomAccessImpl::GetAlignment());
}

IOStatus WinRandomRWFile::Write(uint64_t offset, const Slice& data,
                                const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  return PositionedAppendImpl(data, offset);
}

IOStatus WinRandomRWFile::Read(uint64_t offset, size_t n,
                               const IOOptions& /*options*/, Slice* result,
                               char* scratch, IODebugContext* /*dbg*/) const {
  return ReadImpl(offset, n, result, scratch);
}

IOStatus WinRandomRWFile::Flush(const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus WinRandomRWFile::Sync(const IOOptions& options, IODebugContext* dbg) {
  return SyncImpl(options, dbg);
}

IOStatus WinRandomRWFile::Close(const IOOptions& /*options*/,
                                IODebugContext* /*dbg*/) {
  return CloseImpl();
}

//////////////////////////////////////////////////////////////////////////
/// WinMemoryMappedBufer
WinMemoryMappedBuffer::~WinMemoryMappedBuffer() {
  BOOL ret
#if defined(_MSC_VER)
      = FALSE;
#else
      __attribute__((__unused__));
#endif
  if (base_ != nullptr) {
    ret = ::UnmapViewOfFile(base_);
    assert(ret);
    base_ = nullptr;
  }
  if (map_handle_ != NULL && map_handle_ != INVALID_HANDLE_VALUE) {
    ret = ::CloseHandle(map_handle_);
    assert(ret);
    map_handle_ = NULL;
  }
  if (file_handle_ != NULL && file_handle_ != INVALID_HANDLE_VALUE) {
    ret = ::CloseHandle(file_handle_);
    assert(ret);
    file_handle_ = NULL;
  }
}

//////////////////////////////////////////////////////////////////////////
/// WinDirectory

IOStatus WinDirectory::Fsync(const IOOptions& /*options*/,
                             IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus WinDirectory::Close(const IOOptions& /*options*/,
                             IODebugContext* /*dbg*/) {
  IOStatus s = IOStatus::OK();
  BOOL ret __attribute__((__unused__));
  if (handle_ != INVALID_HANDLE_VALUE) {
    ret = ::CloseHandle(handle_);
    if (!ret) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("Directory closes failed for : " + GetName(),
                                  lastError);
    }
    handle_ = NULL;
  }
  return s;
}

size_t WinDirectory::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(handle_, id, max_size);
}
//////////////////////////////////////////////////////////////////////////
/// WinFileLock

WinFileLock::~WinFileLock() {
  BOOL ret __attribute__((__unused__));
  ret = ::CloseHandle(hFile_);
  assert(ret);
}

}  // namespace port
}  // namespace ROCKSDB_NAMESPACE

#endif
