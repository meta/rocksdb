//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/readahead_raf.h"

#include <algorithm>
#include <mutex>

#include "file/read_write_util.h"
#include "rocksdb/file_system.h"
#include "util/aligned_buffer.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class ReadaheadRandomAccessFile : public FSRandomAccessFile {
 public:
  ReadaheadRandomAccessFile(std::unique_ptr<FSRandomAccessFile>&& file,
                            size_t readahead_size)
      : file_(std::move(file)),
        alignment_(file_->GetRequiredBufferAlignment()),
        readahead_size_(Roundup(readahead_size, alignment_)),
        buffer_(),
        buffer_offset_(0) {
    buffer_.Alignment(alignment_);
    buffer_.AllocateNewBuffer(readahead_size_);
  }

  ReadaheadRandomAccessFile(const ReadaheadRandomAccessFile&) = delete;

  ReadaheadRandomAccessFile& operator=(const ReadaheadRandomAccessFile&) =
      delete;
  ReadaheadRandomAccessFile(ReadaheadRandomAccessFile&&) = delete;
  ReadaheadRandomAccessFile& operator=(ReadaheadRandomAccessFile&&) = delete;
  ~ReadaheadRandomAccessFile() override = default;

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override {
    // Read-ahead only make sense if we have some slack left after reading
    if (n + alignment_ >= readahead_size_) {
      return file_->Read(offset, n, options, result, scratch, dbg);
    }

    std::unique_lock<std::mutex> lk(lock_);

    size_t cached_len = 0;
    // Check if there is a cache hit, meaning that [offset, offset + n) is
    // either completely or partially in the buffer. If it's completely cached,
    // including end of file case when offset + n is greater than EOF, then
    // return.
    if (TryReadFromCache(offset, n, &cached_len, scratch) &&
        (cached_len == n || buffer_.CurrentSize() < readahead_size_)) {
      // We read exactly what we needed, or we hit end of file - return.
      *result = Slice(scratch, cached_len);
      return IOStatus::OK();
    }
    size_t advanced_offset = static_cast<size_t>(offset + cached_len);
    // In the case of cache hit advanced_offset is already aligned, means that
    // chunk_offset equals to advanced_offset
    size_t chunk_offset = TruncateToPageBoundary(alignment_, advanced_offset);

    IOStatus s = ReadIntoBuffer(chunk_offset, readahead_size_, options, dbg);
    if (s.ok()) {
      // The data we need is now in cache, so we can safely read it
      size_t remaining_len;
      TryReadFromCache(advanced_offset, n - cached_len, &remaining_len,
                       scratch + cached_len);
      *result = Slice(scratch, cached_len + remaining_len);
    }
    return s;
  }

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override {
    if (n < readahead_size_) {
      // Don't allow smaller prefetches than the configured `readahead_size_`.
      // `Read()` assumes a smaller prefetch buffer indicates EOF was reached.
      return IOStatus::OK();
    }

    std::unique_lock<std::mutex> lk(lock_);

    size_t offset_ = static_cast<size_t>(offset);
    size_t prefetch_offset = TruncateToPageBoundary(alignment_, offset_);
    if (prefetch_offset == buffer_offset_) {
      return IOStatus::OK();
    }
    return ReadIntoBuffer(prefetch_offset,
                          Roundup(offset_ + n, alignment_) - prefetch_offset,
                          options, dbg);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return file_->GetUniqueId(id, max_size);
  }

  void Hint(AccessPattern pattern) override { file_->Hint(pattern); }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    std::unique_lock<std::mutex> lk(lock_);
    buffer_.Clear();
    return file_->InvalidateCache(offset, length);
  }

  bool use_direct_io() const override { return file_->use_direct_io(); }

  IOStatus GetFileSize(uint64_t* result) override {
    return file_->GetFileSize(result);
  }

 private:
  // Tries to read from buffer_ n bytes starting at offset. If anything was read
  // from the cache, it sets cached_len to the number of bytes actually read,
  // copies these number of bytes to scratch and returns true.
  // If nothing was read sets cached_len to 0 and returns false.
  bool TryReadFromCache(uint64_t offset, size_t n, size_t* cached_len,
                        char* scratch) const {
    if (offset < buffer_offset_ ||
        offset >= buffer_offset_ + buffer_.CurrentSize()) {
      *cached_len = 0;
      return false;
    }
    uint64_t offset_in_buffer = offset - buffer_offset_;
    *cached_len = std::min(
        buffer_.CurrentSize() - static_cast<size_t>(offset_in_buffer), n);
    memcpy(scratch, buffer_.BufferStart() + offset_in_buffer, *cached_len);
    return true;
  }

  // Reads into buffer_ the next n bytes from file_ starting at offset.
  // Can actually read less if EOF was reached.
  // Returns the status of the read operastion on the file.
  IOStatus ReadIntoBuffer(uint64_t offset, size_t n, const IOOptions& options,
                          IODebugContext* dbg) const {
    if (n > buffer_.Capacity()) {
      n = buffer_.Capacity();
    }
    assert(IsFileSectorAligned(offset, alignment_));
    assert(IsFileSectorAligned(n, alignment_));
    Slice result;
    IOStatus s =
        file_->Read(offset, n, options, &result, buffer_.BufferStart(), dbg);
    if (s.ok()) {
      buffer_offset_ = offset;
      buffer_.Size(result.size());
      assert(result.size() == 0 || buffer_.BufferStart() == result.data());
    }
    return s;
  }

  const std::unique_ptr<FSRandomAccessFile> file_;
  const size_t alignment_;
  const size_t readahead_size_;

  mutable std::mutex lock_;
  // The buffer storing the prefetched data
  mutable AlignedBuffer buffer_;
  // The offset in file_, corresponding to data stored in buffer_
  mutable uint64_t buffer_offset_;
};
}  // namespace

std::unique_ptr<FSRandomAccessFile> NewReadaheadRandomAccessFile(
    std::unique_ptr<FSRandomAccessFile>&& file, size_t readahead_size) {
  std::unique_ptr<FSRandomAccessFile> result(
      new ReadaheadRandomAccessFile(std::move(file), readahead_size));
  return result;
}
}  // namespace ROCKSDB_NAMESPACE
