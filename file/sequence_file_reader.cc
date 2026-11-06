//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/sequence_file_reader.h"

#include <algorithm>
#include <mutex>

#include "file/read_write_util.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#include "util/random.h"
#include "util/rate_limiter_impl.h"

namespace ROCKSDB_NAMESPACE {
IOStatus SequentialFileReader::Create(
    const std::shared_ptr<FileSystem>& fs, const std::string& fname,
    const FileOptions& file_opts, std::unique_ptr<SequentialFileReader>* reader,
    IODebugContext* dbg, RateLimiter* rate_limiter) {
  std::unique_ptr<FSSequentialFile> file;
  IOStatus io_s = fs->NewSequentialFile(fname, file_opts, &file, dbg);
  if (io_s.ok()) {
    reader->reset(new SequentialFileReader(std::move(file), fname, nullptr, {},
                                           rate_limiter));
  }
  return io_s;
}

IOStatus SequentialFileReader::Read(size_t n, Slice* result, char* scratch,
                                    Env::IOPriority rate_limiter_priority) {
  IOStatus io_s;
  IOOptions io_opts;
  io_opts.rate_limiter_priority = rate_limiter_priority;
  io_opts.verify_and_reconstruct_read = verify_and_reconstruct_read_;
  if (use_direct_io()) {
    //
    //    |-offset_advance-|---bytes returned--|
    //    |----------------------buf size-------------------------|
    //    |                |                   |                  |
    // aligned           offset          offset + n  Roundup(offset + n,
    // offset                                             alignment)
    //
    size_t offset = offset_.fetch_add(n);
    size_t alignment = file_->GetRequiredBufferAlignment();
    size_t aligned_offset = TruncateToPageBoundary(alignment, offset);
    size_t offset_advance = offset - aligned_offset;
    size_t size = Roundup(offset + n, alignment) - aligned_offset;
    size_t r = 0;
    AlignedBuffer buf;
    buf.Alignment(alignment);
    buf.AllocateNewBuffer(size);

    while (buf.CurrentSize() < size) {
      size_t allowed;
      if (rate_limiter_priority != Env::IO_TOTAL && rate_limiter_ != nullptr) {
        allowed = rate_limiter_->RequestToken(
            buf.Capacity() - buf.CurrentSize(), buf.Alignment(),
            rate_limiter_priority, nullptr /* stats */,
            RateLimiter::OpType::kRead);
      } else {
        assert(buf.CurrentSize() == 0);
        allowed = size;
      }

      Slice tmp;
      uint64_t orig_offset = 0;
      FileOperationInfo::StartTimePoint start_ts;
      if (ShouldNotifyListeners()) {
        orig_offset = aligned_offset + buf.CurrentSize();
        start_ts = FileOperationInfo::StartNow();
      }
      io_s = file_->PositionedRead(aligned_offset + buf.CurrentSize(), allowed,
                                   io_opts, &tmp, buf.Destination(),
                                   nullptr /* dbg */);
      if (ShouldNotifyListeners()) {
        auto finish_ts = FileOperationInfo::FinishNow();
        NotifyOnFileReadFinish(orig_offset, tmp.size(), start_ts, finish_ts,
                               io_s);
      }
      buf.Size(buf.CurrentSize() + tmp.size());
      if (!io_s.ok() || tmp.size() < allowed) {
        break;
      }
    }

    if (io_s.ok() && offset_advance < buf.CurrentSize()) {
      r = buf.Read(scratch, offset_advance,
                   std::min(buf.CurrentSize() - offset_advance, n));
    }
    *result = Slice(scratch, r);
  } else {
    // To be paranoid, modify scratch a little bit, so in case underlying
    // FileSystem doesn't fill the buffer but return success and `scratch`
    // returns contains a previous block, returned value will not pass
    // checksum.
    // It's hard to find useful byte for direct I/O case, so we skip it.
    if (n > 0 && scratch != nullptr) {
      scratch[0]++;
    }

    size_t read = 0;
    while (read < n) {
      size_t allowed;
      if (rate_limiter_priority != Env::IO_TOTAL && rate_limiter_ != nullptr) {
        allowed = rate_limiter_->RequestToken(
            n - read, 0 /* alignment */, rate_limiter_priority,
            nullptr /* stats */, RateLimiter::OpType::kRead);
      } else {
        allowed = n;
      }
      FileOperationInfo::StartTimePoint start_ts;
      if (ShouldNotifyListeners()) {
        start_ts = FileOperationInfo::StartNow();
      }
      Slice tmp;
      io_s = file_->Read(allowed, io_opts, &tmp, scratch + read,
                         nullptr /* dbg */);
      if (ShouldNotifyListeners()) {
        auto finish_ts = FileOperationInfo::FinishNow();
        size_t offset = offset_.fetch_add(tmp.size());
        NotifyOnFileReadFinish(offset, tmp.size(), start_ts, finish_ts, io_s);
      }
      read += tmp.size();
      if (!io_s.ok() || tmp.size() < allowed) {
        break;
      }
    }
    *result = Slice(scratch, read);
  }
  IOSTATS_ADD(bytes_read, result->size());
  return io_s;
}

IOStatus SequentialFileReader::Skip(uint64_t n) {
  if (use_direct_io()) {
    offset_ += static_cast<size_t>(n);
    return IOStatus::OK();
  }
  return file_->Skip(n);
}

void SequentialFileReader::SetReopenOptions(
    const std::shared_ptr<FileSystem>& fs, const FileOptions& file_options,
    size_t readahead_size, const std::shared_ptr<IOTracer>& io_tracer) {
  reopen_options_.reset(
      new ReopenOptions{fs, file_options, readahead_size, io_tracer});
}

IOStatus SequentialFileReader::ReopenAt(
    uint64_t offset, std::unique_ptr<SequentialFileReader>* reader) const {
  if (!CanReopen()) {
    return IOStatus::NotSupported("Sequential reader cannot reopen its input");
  }
  const ReopenOptions& options = *reopen_options_;
  std::unique_ptr<FSSequentialFile> file;
  IOStatus s = options.fs->NewSequentialFile(file_name_, options.file_options,
                                             &file, nullptr);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<SequentialFileReader> fresh(new SequentialFileReader(
      std::move(file), file_name_, options.readahead_size, options.io_tracer,
      listeners_, rate_limiter_, verify_and_reconstruct_read_));
  fresh->SetReopenOptions(options.fs, options.file_options,
                          options.readahead_size, options.io_tracer);
  s = fresh->Skip(offset);
  if (s.ok()) {
    *reader = std::move(fresh);
  }
  return s;
}

namespace {
// This class wraps a SequentialFile, exposing same API, with the differenece
// of being able to prefetch up to readahead_size bytes and then serve them
// from memory, avoiding the entire round-trip if, for example, the data for the
// file is actually remote.
class ReadaheadSequentialFile : public FSSequentialFile {
 public:
  ReadaheadSequentialFile(std::unique_ptr<FSSequentialFile>&& file,
                          size_t readahead_size)
      : file_(std::move(file)),
        alignment_(file_->GetRequiredBufferAlignment()),
        readahead_size_(Roundup(readahead_size, alignment_)),
        buffer_(),
        buffer_offset_(0),
        read_offset_(0) {
    buffer_.Alignment(alignment_);
    buffer_.AllocateNewBuffer(readahead_size_);
  }

  ~ReadaheadSequentialFile() override = default;

  ReadaheadSequentialFile(const ReadaheadSequentialFile&) = delete;

  ReadaheadSequentialFile& operator=(const ReadaheadSequentialFile&) = delete;

  ReadaheadSequentialFile(ReadaheadSequentialFile&&) = delete;

  ReadaheadSequentialFile& operator=(ReadaheadSequentialFile&&) = delete;

  IOStatus Read(size_t n, const IOOptions& opts, Slice* result, char* scratch,
                IODebugContext* dbg) override {
    std::unique_lock<std::mutex> lk(lock_);

    size_t cached_len = 0;
    // Check if there is a cache hit, meaning that [offset, offset + n) is
    // either completely or partially in the buffer. If it's completely cached,
    // including end of file case when offset + n is greater than EOF, then
    // return.
    if (TryReadFromCache(n, &cached_len, scratch) &&
        (cached_len == n || buffer_.CurrentSize() < readahead_size_)) {
      // We read exactly what we needed, or we hit end of file - return.
      *result = Slice(scratch, cached_len);
      return IOStatus::OK();
    }
    n -= cached_len;

    IOStatus s;
    // Read-ahead only make sense if we have some slack left after reading
    if (n + alignment_ >= readahead_size_) {
      s = file_->Read(n, opts, result, scratch + cached_len, dbg);
      if (s.ok()) {
        read_offset_ += result->size();
        *result = Slice(scratch, cached_len + result->size());
      }
      buffer_.Clear();
      return s;
    }

    s = ReadIntoBuffer(readahead_size_, opts, dbg);
    if (s.ok()) {
      // The data we need is now in cache, so we can safely read it
      size_t remaining_len;
      TryReadFromCache(n, &remaining_len, scratch + cached_len);
      *result = Slice(scratch, cached_len + remaining_len);
    }
    return s;
  }

  IOStatus Skip(uint64_t n) override {
    std::unique_lock<std::mutex> lk(lock_);
    IOStatus s = IOStatus::OK();
    // First check if we need to skip already cached data
    if (buffer_.CurrentSize() > 0) {
      // Do we need to skip beyond cached data?
      if (read_offset_ + n >= buffer_offset_ + buffer_.CurrentSize()) {
        // Yes. Skip whaterver is in memory and adjust offset accordingly
        n -= buffer_offset_ + buffer_.CurrentSize() - read_offset_;
        read_offset_ = buffer_offset_ + buffer_.CurrentSize();
      } else {
        // No. The entire section to be skipped is entirely i cache.
        read_offset_ += n;
        n = 0;
      }
    }
    if (n > 0) {
      // We still need to skip more, so call the file API for skipping
      s = file_->Skip(n);
      if (s.ok()) {
        read_offset_ += n;
      }
      buffer_.Clear();
    }
    return s;
  }

  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& opts,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override {
    return file_->PositionedRead(offset, n, opts, result, scratch, dbg);
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    std::unique_lock<std::mutex> lk(lock_);
    buffer_.Clear();
    return file_->InvalidateCache(offset, length);
  }

  bool use_direct_io() const override { return file_->use_direct_io(); }

 private:
  // Tries to read from buffer_ n bytes. If anything was read from the cache, it
  // sets cached_len to the number of bytes actually read, copies these number
  // of bytes to scratch and returns true.
  // If nothing was read sets cached_len to 0 and returns false.
  bool TryReadFromCache(size_t n, size_t* cached_len, char* scratch) {
    if (read_offset_ < buffer_offset_ ||
        read_offset_ >= buffer_offset_ + buffer_.CurrentSize()) {
      *cached_len = 0;
      return false;
    }
    uint64_t offset_in_buffer = read_offset_ - buffer_offset_;
    *cached_len = std::min(
        buffer_.CurrentSize() - static_cast<size_t>(offset_in_buffer), n);
    memcpy(scratch, buffer_.BufferStart() + offset_in_buffer, *cached_len);
    read_offset_ += *cached_len;
    return true;
  }

  // Reads into buffer_ the next n bytes from file_.
  // Can actually read less if EOF was reached.
  // Returns the status of the read operastion on the file.
  IOStatus ReadIntoBuffer(size_t n, const IOOptions& opts,
                          IODebugContext* dbg) {
    if (n > buffer_.Capacity()) {
      n = buffer_.Capacity();
    }
    assert(IsFileSectorAligned(n, alignment_));
    Slice result;
    IOStatus s = file_->Read(n, opts, &result, buffer_.BufferStart(), dbg);
    if (s.ok()) {
      buffer_offset_ = read_offset_;
      buffer_.Size(result.size());
      assert(result.size() == 0 || buffer_.BufferStart() == result.data());
    }
    return s;
  }

  const std::unique_ptr<FSSequentialFile> file_;
  const size_t alignment_;
  const size_t readahead_size_;

  std::mutex lock_;
  // The buffer storing the prefetched data
  AlignedBuffer buffer_;
  // The offset in file_, corresponding to data stored in buffer_
  uint64_t buffer_offset_;
  // The offset up to which data was read from file_. In fact, it can be larger
  // than the actual file size, since the file_->Skip(n) call doesn't return the
  // actual number of bytes that were skipped, which can be less than n.
  // This is not a problemm since read_offset_ is monotonically increasing and
  // its only use is to figure out if next piece of data should be read from
  // buffer_ or file_ directly.
  uint64_t read_offset_;
};
}  // namespace

std::unique_ptr<FSSequentialFile>
SequentialFileReader::NewReadaheadSequentialFile(
    std::unique_ptr<FSSequentialFile>&& file, size_t readahead_size) {
  if (file->GetRequiredBufferAlignment() >= readahead_size) {
    // Short-circuit and return the original file if readahead_size is
    // too small and hence doesn't make sense to be used for prefetching.
    return std::move(file);
  }
  std::unique_ptr<FSSequentialFile> result(
      new ReadaheadSequentialFile(std::move(file), readahead_size));
  return result;
}
}  // namespace ROCKSDB_NAMESPACE
