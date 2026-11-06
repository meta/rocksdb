//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <algorithm>
#include <atomic>
#include <deque>
#include <sstream>
#include <string>

#include "file/random_access_file_reader.h"
#include "file/readahead_file_info.h"
#include "file_util.h"
#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "util/aligned_buffer.h"
#include "util/autovector.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

#define DEFAULT_DECREMENT 8 * 1024

struct IOOptions;
class RandomAccessFileReader;

enum class FilePrefetchBufferUsage {
  kTableOpenPrefetchTail,
  kUserScanPrefetch,
  kCompactionPrefetch,
  kUnknown,
};

enum class PrefetchDirection {
  kForward,
  kBackward,
};

struct ReadaheadParams {
  ReadaheadParams() {}

  // The initial readahead size.
  size_t initial_readahead_size = 0;

  // The maximum readahead size.
  // If max_readahead_size > readahead_size, then readahead size will be doubled
  // on every IO until max_readahead_size is hit. Typically this is set as a
  // multiple of initial_readahead_size. initial_readahead_size should be
  // greater than equal to initial_readahead_size.
  size_t max_readahead_size = 0;

  // If true, Readahead is enabled implicitly by rocksdb
  // after doing sequential scans for num_file_reads_for_auto_readahead.
  bool implicit_auto_readahead = false;

  // TODO akanksha - Remove num_file_reads when BlockPrefetcher is refactored.
  uint64_t num_file_reads = 0;
  uint64_t num_file_reads_for_auto_readahead = 0;

  // Number of buffers to maintain that contains prefetched data. If num_buffers
  // > 1 then buffers will be filled asynchronously whenever they get emptied.
  size_t num_buffers = 1;

  // Direction of prefetch for sequential scans. Forward is default for backward
  // compatibility. Backward is used for reverse iteration.
  PrefetchDirection direction = PrefetchDirection::kForward;
};

struct BufferInfo {
  void ClearBuffer() {
    buffer_.Clear();
    initial_end_offset_ = 0;
    async_req_len_ = 0;
  }

  AlignedBuffer buffer_;

  uint64_t offset_ = 0;

  // Below parameters are used in case of async read flow.
  // Length requested for in ReadAsync.
  size_t async_req_len_ = 0;

  // async_read_in_progress can be used as mutex. Callback can update the buffer
  // and its size but async_read_in_progress is only set by main thread.
  bool async_read_in_progress_ = false;

  // io_handle is allocated and used by underlying file system in case of
  // asynchronous reads.
  void* io_handle_ = nullptr;

  IOHandleDeleter del_fn_ = nullptr;

  // initial_end_offset is used to keep track of the end offset of the buffer
  // that was originally called. It's helpful in case of autotuning of readahead
  // size when callback is made to BlockBasedTableIterator.
  // initial end offset of this buffer which will be the starting
  // offset of next prefetch.
  //
  // For example - if end offset of previous buffer was 100 and because of
  // readahead_size optimization, end_offset was trimmed to 60. Then for next
  // prefetch call, start_offset should be initialized to 100 i.e  start_offset
  // = buf->initial_end_offset_.
  uint64_t initial_end_offset_ = 0;

  bool IsDataBlockInBuffer(uint64_t offset, size_t length) {
    assert(async_read_in_progress_ == false);
    return (offset >= offset_ &&
            offset + length <= offset_ + buffer_.CurrentSize());
  }

  bool IsOffsetInBuffer(uint64_t offset) {
    assert(async_read_in_progress_ == false);
    return (offset >= offset_ && offset < offset_ + buffer_.CurrentSize());
  }

  bool DoesBufferContainData() {
    assert(async_read_in_progress_ == false);
    return buffer_.CurrentSize() > 0;
  }

  bool IsBufferOutdated(uint64_t offset) {
    return (!async_read_in_progress_ && DoesBufferContainData() &&
            offset >= offset_ + buffer_.CurrentSize());
  }

  bool IsBufferOutdatedBackward(uint64_t offset, size_t length) {
    return (!async_read_in_progress_ && DoesBufferContainData() &&
            offset + length <= offset_);
  }

  bool IsBufferOutdatedWithAsyncProgress(uint64_t offset) {
    return (async_read_in_progress_ && io_handle_ != nullptr &&
            offset >= offset_ + async_req_len_);
  }

  bool IsBufferOutdatedBackwardWithAsyncProgress(uint64_t offset,
                                                 size_t length) {
    return (async_read_in_progress_ && io_handle_ != nullptr &&
            offset + length <= offset_);
  }

  bool IsOffsetInBufferWithAsyncProgress(uint64_t offset) {
    return (async_read_in_progress_ && offset >= offset_ &&
            offset < offset_ + async_req_len_);
  }

  size_t CurrentSize() { return buffer_.CurrentSize(); }
};

// Implementation:
// FilePrefetchBuffer maintains a dequeu of free buffers (free_bufs_) with no
// data and bufs_ which contains the prefetched data. Whenever a buffer is
// consumed or is outdated (w.r.t. to requested offset), that buffer is cleared
// and returned to free_bufs_.
//
// If a buffer is available in free_bufs_, it's moved to bufs_ and is sent for
// prefetching.
// num_buffers_ defines how many buffers FilePrefetchBuffer can maintain at a
// time that contains prefetched data with num_buffers_ == bufs_.size() +
// free_bufs_.size().
//
// If num_buffers_ == 1, it's a sequential read flow. Read API will be called on
// that one buffer whenever the data is requested and is not in the buffer.
// When reusing the file system allocated buffer, overlap_buf_ is used if the
// main buffer only contains part of the requested data. It is returned to
// the caller after the remaining data is fetched.
// If num_buffers_ > 1, then the data is prefetched asynchronously in the
// buffers whenever the data is consumed from the buffers and that buffer is
// freed.
// If num_buffers > 1, then requested data can be overlapping between 2 buffers.
// To return the continuous buffer, overlap_buf_ is used. The requested data is
// copied from 2 buffers to the overlap_buf_ and overlap_buf_ is returned to
// the caller.

// FilePrefetchBuffer is a smart buffer to store and read data from a file.
class FilePrefetchBuffer {
 public:
  // Constructor.
  //
  // All arguments are optional.
  // ReadaheadParams  : Parameters to control the readahead behavior.
  // enable           : controls whether reading from the buffer is enabled.
  //                    If false, TryReadFromCache() always return false, and we
  //                    only take stats for the minimum offset if
  //                    track_min_offset = true.
  //                    See below NOTE about mmap reads.
  // track_min_offset : Track the minimum offset ever read and collect stats on
  //                    it. Used for adaptable readahead of the file
  //                    footer/metadata.
  //
  // A user can construct a FilePrefetchBuffer without any arguments, but use
  // `Prefetch` to load data into the buffer.
  // NOTE: FilePrefetchBuffer is incompatible with prefetching from
  // RandomAccessFileReaders using mmap reads, so it is common to use
  // `!use_mmap_reads` for the `enable` parameter.
  FilePrefetchBuffer(
      const ReadaheadParams& readahead_params = {}, bool enable = true,
      bool track_min_offset = false, FileSystem* fs = nullptr,
      SystemClock* clock = nullptr, Statistics* stats = nullptr,
      const std::function<void(bool, uint64_t&, uint64_t&)>& cb = nullptr,
      FilePrefetchBufferUsage usage = FilePrefetchBufferUsage::kUnknown)
      : readahead_size_(readahead_params.initial_readahead_size),
        initial_auto_readahead_size_(readahead_params.initial_readahead_size),
        max_readahead_size_(readahead_params.max_readahead_size),
        min_offset_read_(std::numeric_limits<size_t>::max()),
        enable_(enable),
        track_min_offset_(track_min_offset),
        implicit_auto_readahead_(readahead_params.implicit_auto_readahead),
        prev_offset_(0),
        prev_len_(0),
        num_file_reads_for_auto_readahead_(
            readahead_params.num_file_reads_for_auto_readahead),
        num_file_reads_(readahead_params.num_file_reads),
        explicit_prefetch_submitted_(false),
        fs_(fs),
        clock_(clock),
        stats_(stats),
        usage_(usage),
        readaheadsize_cb_(cb),
        num_buffers_(readahead_params.num_buffers),
        direction_(readahead_params.direction) {
    assert((num_file_reads_ >= num_file_reads_for_auto_readahead_ + 1) ||
           (num_file_reads_ == 0));

    // overlap_buf_ is used whenever the main buffer only has part of the
    // requested data. The relevant data is copied into overlap_buf_ and the
    // remaining data is copied in later to satisfy the user's request. This is
    // used in both the synchronous (num_buffers_ = 1) and asynchronous
    // (num_buffers_ > 1) cases. In the asynchronous case, the requested data
    // may be spread out over 2 buffers.
    if (num_buffers_ > 1 ||
        (fs_ != nullptr &&
         CheckFSFeatureSupport(fs_, FSSupportedOps::kFSBuffer))) {
      overlap_buf_ = new BufferInfo();
    }

    free_bufs_.resize(num_buffers_);
    for (uint32_t i = 0; i < num_buffers_; i++) {
      free_bufs_[i] = new BufferInfo();
    }
  }

  // No copying allowed
  FilePrefetchBuffer(const FilePrefetchBuffer&) = delete;
  FilePrefetchBuffer& operator=(const FilePrefetchBuffer&) = delete;
  FilePrefetchBuffer(FilePrefetchBuffer&&) = delete;
  FilePrefetchBuffer& operator=(FilePrefetchBuffer&&) = delete;

  ~FilePrefetchBuffer() {
    // Abort any pending async read request before destroying the class object.
    if (fs_ != nullptr) {
      std::vector<void*> handles;
      for (auto& buf : bufs_) {
        if (buf->async_read_in_progress_ && buf->io_handle_ != nullptr) {
          handles.emplace_back(buf->io_handle_);
        }
      }
      if (!handles.empty()) {
        StopWatch sw(clock_, stats_, ASYNC_PREFETCH_ABORT_MICROS);
        Status s = fs_->AbortIO(handles);
        assert(s.ok());
      }

      for (auto& buf : bufs_) {
        if (buf->io_handle_ != nullptr) {
          DestroyAndClearIOHandle(buf);
          buf->ClearBuffer();
        }
        buf->async_read_in_progress_ = false;
      }
    }

    // Prefetch buffer bytes discarded.
    uint64_t bytes_discarded = 0;
    // Iterated over buffers.
    for (auto& buf : bufs_) {
      if (buf->DoesBufferContainData()) {
        // If last read was from this block and some bytes are still unconsumed.
        if (prev_offset_ >= buf->offset_ &&
            prev_offset_ + prev_len_ < buf->offset_ + buf->CurrentSize()) {
          bytes_discarded +=
              buf->CurrentSize() - (prev_offset_ + prev_len_ - buf->offset_);
        }
        // If last read was from previous blocks and this block is unconsumed.
        else if (prev_offset_ < buf->offset_ &&
                 prev_offset_ + prev_len_ <= buf->offset_) {
          bytes_discarded += buf->CurrentSize();
        }
      }
    }

    RecordInHistogram(stats_, PREFETCHED_BYTES_DISCARDED, bytes_discarded);

    for (auto& buf : bufs_) {
      delete buf;
      buf = nullptr;
    }

    for (auto& buf : free_bufs_) {
      delete buf;
      buf = nullptr;
    }

    if (overlap_buf_ != nullptr) {
      delete overlap_buf_;
      overlap_buf_ = nullptr;
    }
  }

  bool Enabled() const { return enable_; }

  // Called externally by user to only load data into the buffer from a file
  // with num_buffers_ should be set to default(1).
  //
  // opts                  : the IO options to use.
  // reader                : the file reader.
  // offset                : the file offset to start reading from.
  // n                     : the number of bytes to read.
  //
  // Note: Why do we pass in the RandomAccessFileReader* for every single call
  // to Prefetch/PrefetchAsync/TryReadFromCache? Why can't we just pass it in at
  // construction time?
  // Although the RandomAccessFileReader* is often available when creating
  // the FilePrefetchBuffer, this is not true for BlobDB (see
  // BlobSource::GetBlob). The file reader gets retrieved or created inside
  // BlobFileCache::GetBlobFileReader, after we have already allocated a new
  // FilePrefetchBuffer.
  Status Prefetch(const IOOptions& opts, RandomAccessFileReader* reader,
                  uint64_t offset, size_t n);

  // Request for reading the data from a file asynchronously.
  // If data already exists in the buffer, result will be updated.
  // reader                : the file reader.
  // offset                : the file offset to start reading from.
  // n                     : the number of bytes to read.
  // result                : if data already exists in the buffer, result will
  //                         be updated with the data.
  //
  // If data already exist in the buffer, it will return Status::OK, otherwise
  // it will send asynchronous request and return Status::TryAgain.
  Status PrefetchAsync(const IOOptions& opts, RandomAccessFileReader* reader,
                       uint64_t offset, size_t n, Slice* result);

  // Tries returning the data for a file read from this buffer if that data is
  // in the buffer.
  // It handles tracking the minimum read offset if track_min_offset = true.
  // It also does the exponential readahead when readahead_size is set as part
  // of the constructor.
  //
  // opts                  : the IO options to use.
  // reader                : the file reader.
  // offset                : the file offset.
  // n                     : the number of bytes.
  // result                : output buffer to put the data into.
  // s                     : output status.
  // for_compaction        : true if cache read is done for compaction read.
  bool TryReadFromCache(const IOOptions& opts, RandomAccessFileReader* reader,
                        uint64_t offset, size_t n, Slice* result, Status* s,
                        bool for_compaction = false);

  // The minimum `offset` ever passed to TryReadFromCache(). This will nly be
  // tracked if track_min_offset = true.
  size_t min_offset_read() const { return min_offset_read_; }

  size_t GetPrefetchOffset() const { return bufs_.front()->offset_; }

  // Called in case of implicit auto prefetching.
  void UpdateReadPattern(const uint64_t& offset, const size_t& len,
                         bool decrease_readaheadsize) {
    if (decrease_readaheadsize) {
      DecreaseReadAheadIfEligible(offset, len);
    }
    prev_offset_ = offset;
    prev_len_ = len;
    explicit_prefetch_submitted_ = false;
  }

  void GetReadaheadState(ReadaheadFileInfo::ReadaheadInfo* readahead_info) {
    readahead_info->readahead_size = readahead_size_;
    readahead_info->num_file_reads = num_file_reads_;
  }

  void DecreaseReadAheadIfEligible(uint64_t offset, size_t size,
                                   size_t value = DEFAULT_DECREMENT) {
    if (bufs_.empty()) {
      return;
    }

    // Decrease the readahead_size if
    // - its enabled internally by RocksDB (implicit_auto_readahead_) and,
    // - readahead_size is greater than 0 and,
    // - this block would have called prefetch API if not found in cache for
    //   which conditions are:
    //   - few/no bytes are in buffer and,
    //   - block is sequential with the previous read and,
    //   - num_file_reads_ + 1 (including this read) >
    //   num_file_reads_for_auto_readahead_

    size_t curr_size = bufs_.front()->async_read_in_progress_
                           ? bufs_.front()->async_req_len_
                           : bufs_.front()->CurrentSize();
    if (implicit_auto_readahead_ && readahead_size_ > 0) {
      if ((offset + size > bufs_.front()->offset_ + curr_size) &&
          IsBlockSequential(offset) &&
          (num_file_reads_ + 1 > num_file_reads_for_auto_readahead_)) {
        readahead_size_ =
            std::max(initial_auto_readahead_size_,
                     (readahead_size_ >= value ? readahead_size_ - value : 0));
      }
    }
  }

  // Callback function passed to underlying FS in case of asynchronous reads.
  void PrefetchAsyncCallback(FSReadRequest& req, void* cb_arg);

  void TEST_GetBufferOffsetandSize(
      std::vector<std::tuple<uint64_t, size_t, bool>>& buffer_info) {
    for (size_t i = 0; i < bufs_.size(); i++) {
      std::get<0>(buffer_info[i]) = bufs_[i]->offset_;
      std::get<1>(buffer_info[i]) = bufs_[i]->async_read_in_progress_
                                        ? bufs_[i]->async_req_len_
                                        : bufs_[i]->CurrentSize();
      std::get<2>(buffer_info[i]) = bufs_[i]->async_read_in_progress_;
    }
  }

  void TEST_GetOverlapBufferOffsetandSize(
      std::pair<uint64_t, size_t>& buffer_info) {
    if (overlap_buf_ != nullptr) {
      buffer_info.first = overlap_buf_->offset_;
      buffer_info.second = overlap_buf_->CurrentSize();
    }
  }

 private:
  // Calculates roundoff offset and length to be prefetched based on alignment
  // and data present in buffer_. It also allocates new buffer or refit tail if
  // required.
  void PrepareBufferForRead(BufferInfo* buf, size_t alignment, uint64_t offset,
                            size_t roundup_len, bool refit_tail,
                            bool use_fs_buffer, uint64_t& aligned_useful_len);

  void AbortOutdatedIO(uint64_t offset, size_t length);

  void AbortAllIOs();

  void ClearOutdatedData(uint64_t offset, size_t len);

  // It calls Poll API to check for any pending asynchronous request.
  Status PollIfNeeded(uint64_t offset, size_t len);

  Status PrefetchInternal(const IOOptions& opts, RandomAccessFileReader* reader,
                          uint64_t offset, size_t length, size_t readahead_size,
                          bool& copy_to_third_buffer);

  Status Read(BufferInfo* buf, const IOOptions& opts,
              RandomAccessFileReader* reader, uint64_t read_len,
              uint64_t aligned_useful_len, uint64_t start_offset,
              bool use_fs_buffer);

  Status ReadAsync(BufferInfo* buf, const IOOptions& opts,
                   RandomAccessFileReader* reader, uint64_t read_len,
                   uint64_t start_offset);

  // Copy the data from src to overlap_buf_.
  void CopyDataToOverlapBuffer(BufferInfo* src, uint64_t& offset,
                               size_t& length);

  bool IsBlockSequential(const size_t& offset) {
    return (prev_len_ == 0 || (prev_offset_ + prev_len_ == offset));
  }

  // Called in case of implicit auto prefetching.
  void ResetValues() {
    num_file_reads_ = 1;
    readahead_size_ = initial_auto_readahead_size_;
  }

  // Called in case of implicit auto prefetching.
  bool IsEligibleForPrefetch(uint64_t offset, size_t n) {
    // Prefetch only if this read is sequential otherwise reset readahead_size_
    // to initial value.
    if (!IsBlockSequential(offset)) {
      UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);
      ResetValues();
      return false;
    }
    num_file_reads_++;

    // Since async request was submitted in last call directly by calling
    // PrefetchAsync, it skips num_file_reads_ check as this call is to poll the
    // data submitted in previous call.
    if (explicit_prefetch_submitted_) {
      return true;
    }
    if (num_file_reads_ <= num_file_reads_for_auto_readahead_) {
      UpdateReadPattern(offset, n, false /*decrease_readaheadsize*/);
      return false;
    }
    return true;
  }

  bool IsEligibleForFurtherPrefetching() {
    if (free_bufs_.empty()) {
      return false;
    }
    // Readahead size can be 0 because of trimming.
    if (readahead_size_ == 0) {
      return false;
    }
    return true;
  }

  // Whether we reuse the file system provided buffer
  // Until we also handle the async read case, only enable this optimization
  // for the synchronous case when num_buffers_ = 1.
  // Note: Although it would be more convenient if we could determine
  // whether we want to reuse the file system buffer at construction time,
  // this would not work in all cases, because not all clients (BlobDB in
  // particular) have a RandomAccessFileReader* available at construction time.
  bool UseFSBuffer(RandomAccessFileReader* reader) {
    return reader->file() != nullptr && !reader->use_direct_io() &&
           fs_ != nullptr &&
           CheckFSFeatureSupport(fs_, FSSupportedOps::kFSBuffer) &&
           num_buffers_ == 1;
  }

  // When we are reusing the file system provided buffer, we are not concerned
  // with alignment. However, quite a bit of prefetch code incorporates
  // alignment, so we can put in 1 to keep the code simpler.
  size_t GetRequiredBufferAlignment(RandomAccessFileReader* reader) {
    if (UseFSBuffer(reader)) {
      return 1;
    }
    return reader->file()->GetRequiredBufferAlignment();
  }

  // Reuses the file system allocated buffer to avoid an extra copy
  IOStatus FSBufferDirectRead(RandomAccessFileReader* reader, BufferInfo* buf,
                              const IOOptions& opts, uint64_t offset, size_t n,
                              Slice& result) {
    FSReadRequest read_req;
    read_req.offset = offset;
    read_req.len = n;
    read_req.scratch = nullptr;
    AlignedBuffer direct_io_buffer;
    AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
    IOStatus s = reader->MultiRead(opts, &read_req, 1, &direct_io_context);
    if (!s.ok()) {
      return s;
    }
    s = read_req.status;
    if (!s.ok()) {
      return s;
    }
    buf->buffer_.SetBuffer(read_req.result, std::move(read_req.fs_scratch));
    buf->offset_ = offset;
    buf->initial_end_offset_ = offset + read_req.result.size();
    result = read_req.result;
    return s;
  }

  void DestroyAndClearIOHandle(BufferInfo* buf) {
    if (buf->io_handle_ != nullptr && buf->del_fn_ != nullptr) {
      buf->del_fn_(buf->io_handle_);
      buf->io_handle_ = nullptr;
      buf->del_fn_ = nullptr;
    }
    buf->async_read_in_progress_ = false;
  }

  void HandleOverlappingSyncData(uint64_t offset, size_t length,
                                 uint64_t& tmp_offset, size_t& tmp_length,
                                 bool& use_overlap_buffer);

  Status HandleOverlappingAsyncData(const IOOptions& opts,
                                    RandomAccessFileReader* reader,
                                    uint64_t offset, size_t length,
                                    size_t readahead_size,
                                    bool& copy_to_third_buffer,
                                    uint64_t& tmp_offset, size_t& tmp_length);

  bool TryReadFromCacheUntracked(const IOOptions& opts,
                                 RandomAccessFileReader* reader,
                                 uint64_t offset, size_t n, Slice* result,
                                 Status* s, bool for_compaction = false);

  void ReadAheadSizeTuning(BufferInfo* buf, bool read_curr_block,
                           bool refit_tail, bool use_fs_buffer,
                           uint64_t prev_buf_end_offset, size_t alignment,
                           size_t length, size_t readahead_size,
                           uint64_t& offset, uint64_t& end_offset,
                           size_t& read_len, uint64_t& aligned_useful_len);

  void UpdateStats(bool found_in_buffer, size_t length_found) {
    if (usage_ != FilePrefetchBufferUsage::kUserScanPrefetch) {
      return;
    }
    if (found_in_buffer) {
      RecordTick(stats_, PREFETCH_HITS);
    }
    if (length_found > 0) {
      RecordTick(stats_, PREFETCH_BYTES_USEFUL, length_found);
    }
  }

  void UpdateReadAheadTrimmedStat(size_t initial_length,
                                  size_t updated_length) {
    if (initial_length != updated_length) {
      RecordTick(stats_, READAHEAD_TRIMMED);
    }
  }

  Status PrefetchRemBuffers(const IOOptions& opts,
                            RandomAccessFileReader* reader,
                            uint64_t end_offset1, size_t alignment,
                            size_t readahead_size);

  // *** BEGIN APIs related to allocating and freeing buffers ***
  bool IsBufferQueueEmpty() { return bufs_.empty(); }

  BufferInfo* GetFirstBuffer() { return bufs_.front(); }

  BufferInfo* GetLastBuffer() { return bufs_.back(); }

  size_t NumBuffersAllocated() { return bufs_.size(); }

  void AllocateBuffer() {
    assert(!free_bufs_.empty());
    BufferInfo* buf = free_bufs_.front();
    free_bufs_.pop_front();
    bufs_.emplace_back(buf);
  }

  void AllocateBufferIfEmpty() {
    if (bufs_.empty()) {
      AllocateBuffer();
    }
  }

  void FreeFrontBuffer() {
    BufferInfo* buf = bufs_.front();
    buf->ClearBuffer();
    bufs_.pop_front();
    free_bufs_.emplace_back(buf);
  }

  void FreeLastBuffer() {
    BufferInfo* buf = bufs_.back();
    buf->ClearBuffer();
    bufs_.pop_back();
    free_bufs_.emplace_back(buf);
  }

  void FreeAllBuffers() {
    while (!bufs_.empty()) {
      BufferInfo* buf = bufs_.front();
      buf->ClearBuffer();
      bufs_.pop_front();
      free_bufs_.emplace_back(buf);
    }
  }

  void FreeEmptyBuffers() {
    if (bufs_.empty()) {
      return;
    }

    std::deque<BufferInfo*> tmp_buf;
    while (!bufs_.empty()) {
      BufferInfo* buf = bufs_.front();
      bufs_.pop_front();
      if (buf->async_read_in_progress_ || buf->DoesBufferContainData()) {
        tmp_buf.emplace_back(buf);
      } else {
        free_bufs_.emplace_back(buf);
      }
    }
    bufs_ = tmp_buf;
  }

  // *** END APIs related to allocating and freeing buffers ***

  std::deque<BufferInfo*> bufs_;
  std::deque<BufferInfo*> free_bufs_;
  BufferInfo* overlap_buf_ = nullptr;

  size_t readahead_size_;
  size_t initial_auto_readahead_size_;
  // FilePrefetchBuffer object won't be created from Iterator flow if
  // max_readahead_size_ = 0.
  size_t max_readahead_size_;

  // The minimum `offset` ever passed to TryReadFromCache().
  size_t min_offset_read_;
  // if false, TryReadFromCache() always return false, and we only take stats
  // for track_min_offset_ if track_min_offset_ = true
  bool enable_;
  // If true, track minimum `offset` ever passed to TryReadFromCache(), which
  // can be fetched from min_offset_read().
  bool track_min_offset_;

  // implicit_auto_readahead is enabled by rocksdb internally after 2
  // sequential IOs.
  bool implicit_auto_readahead_;
  uint64_t prev_offset_;
  size_t prev_len_;
  // num_file_reads_ and num_file_reads_for_auto_readahead_ is only used when
  // implicit_auto_readahead_ is set.
  uint64_t num_file_reads_for_auto_readahead_;
  uint64_t num_file_reads_;

  // If explicit_prefetch_submitted_ is set then it indicates RocksDB called
  // PrefetchAsync to submit request. It needs to call TryReadFromCache to
  // poll the submitted request without checking if data is sequential and
  // num_file_reads_.
  bool explicit_prefetch_submitted_;

  FileSystem* fs_;
  SystemClock* clock_;
  Statistics* stats_;

  FilePrefetchBufferUsage usage_;

  std::function<void(bool, uint64_t&, uint64_t&)> readaheadsize_cb_;

  // num_buffers_ is the number of buffers maintained by FilePrefetchBuffer to
  // prefetch the data at a time.
  size_t num_buffers_;

  // Direction of prefetch for sequential scans.
  PrefetchDirection direction_;
};
}  // namespace ROCKSDB_NAMESPACE
