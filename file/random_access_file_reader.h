//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <atomic>
#include <functional>
#include <sstream>
#include <string>

#include "env/file_system_tracer.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/rate_limiter.h"
#include "util/aligned_buffer.h"
#include "util/coro_utils.h"

namespace ROCKSDB_NAMESPACE {
class Statistics;
class HistogramImpl;
class SystemClock;

using AlignedBuf = FSAllocationPtr;

struct AlignedBufferAllocationContext {
  // `allocator` is intentionally not owned. RandomAccessFileReader invokes it
  // only while allocating `buffer`, before Read/ReadAsync/MultiRead returns.
  // For ReadAsync, `buffer` is still referenced by the completion callback and
  // must outlive the async operation.
  AlignedBuffer* buffer = nullptr;
  const AlignedBuffer::Allocator* allocator = nullptr;
};

// Align the request r according to alignment and return the aligned result.
FSReadRequest Align(const FSReadRequest& r, size_t alignment);

// Try to merge src to dest if they have overlap.
//
// Each request represents an inclusive interval [offset, offset + len].
// If the intervals have overlap, update offset and len to represent the
// merged interval, and return true.
// Otherwise, do nothing and return false.
bool TryMerge(FSReadRequest* dest, const FSReadRequest& src);

// RandomAccessFileReader is a wrapper on top of FSRandomAccessFile. It is
// responsible for:
// - Handling Buffered and Direct reads appropriately.
// - Rate limiting compaction reads.
// - Notifying any interested listeners on the completion of a read.
// - Updating IO stats.
class RandomAccessFileReader final : public FSRandomAccessFile {
 private:
  void NotifyOnFileReadFinish(
      uint64_t offset, size_t length,
      const FileOperationInfo::StartTimePoint& start_ts,
      const FileOperationInfo::FinishTimePoint& finish_ts,
      const Status& status) const {
    FileOperationInfo info(FileOperationType::kRead, file_name_, start_ts,
                           finish_ts, status, file_temperature_);
    info.offset = offset;
    info.length = length;

    for (auto& listener : listeners_) {
      listener->OnFileReadFinish(info);
    }
    info.status.PermitUncheckedError();
  }

  void NotifyOnIOError(const IOStatus& io_status, FileOperationType operation,
                       const std::string& file_path, size_t length,
                       uint64_t offset) const {
    if (listeners_.empty()) {
      return;
    }
    IOErrorInfo io_error_info(io_status, operation, file_path, length, offset);

    for (auto& listener : listeners_) {
      listener->OnIOError(io_error_info);
    }
    io_status.PermitUncheckedError();
  }

  bool ShouldNotifyListeners() const { return !listeners_.empty(); }

  // Charges `bytes` of reads to the rate limiter at
  // opts.rate_limiter_priority. No-op without a rate limiter or with IO_TOTAL.
  void ChargeRateLimiterForRead(const IOOptions& opts, size_t bytes) const;

  FileSystem* file_system_;
  FSRandomAccessFilePtr file_;
  std::string file_name_;
  SystemClock* clock_;
  Statistics* stats_;
  uint32_t hist_type_;
  HistogramImpl* file_read_hist_;
  RateLimiter* rate_limiter_;
  std::vector<std::shared_ptr<EventListener>> listeners_;
  const Temperature file_temperature_;
  const bool is_last_level_;

  struct ReadAsyncInfo {
    ReadAsyncInfo(std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
                  uint64_t start_time)
        : cb_(cb),
          cb_arg_(cb_arg),
          start_time_(start_time),
          user_scratch_(nullptr),
          user_aligned_buf_(nullptr),
          direct_io_buffer_(nullptr),
          user_offset_(0),
          user_len_(0),
          is_aligned_(false) {}

    std::function<void(FSReadRequest&, void*)> cb_;
    void* cb_arg_;
    uint64_t start_time_;
    FileOperationInfo::StartTimePoint fs_start_ts_;
    // Below fields stores the parameters passed by caller in case of direct_io.
    char* user_scratch_;
    AlignedBuf* user_aligned_buf_;
    // Raw pointer to caller-owned direct-I/O storage used when forming the
    // user-visible result in ReadAsyncCallback. The caller must keep it alive
    // until the async operation completes or is cancelled.
    AlignedBuffer* direct_io_buffer_;
    uint64_t user_offset_;
    size_t user_len_;
    Slice user_result_;
    // Used in case of direct_io
    AlignedBuffer buf_;
    bool is_aligned_;
  };

 public:
  explicit RandomAccessFileReader(
      std::unique_ptr<FSRandomAccessFile>&& raf, const std::string& _file_name,
      SystemClock* clock = nullptr,
      const std::shared_ptr<IOTracer>& io_tracer = nullptr,
      Statistics* stats = nullptr,
      uint32_t hist_type = Histograms::HISTOGRAM_ENUM_MAX,
      HistogramImpl* file_read_hist = nullptr,
      RateLimiter* rate_limiter = nullptr,
      const std::vector<std::shared_ptr<EventListener>>& listeners = {},
      Temperature file_temperature = Temperature::kUnknown,
      bool is_last_level = false, FileSystem* file_system = nullptr)
      : file_system_(file_system),
        file_(std::move(raf), io_tracer, _file_name),
        file_name_(std::move(_file_name)),
        clock_(clock),
        stats_(stats),
        hist_type_(hist_type),
        file_read_hist_(file_read_hist),
        rate_limiter_(rate_limiter),
        listeners_(),
        file_temperature_(file_temperature),
        is_last_level_(is_last_level) {
    (void)file_system_;
    std::for_each(listeners.begin(), listeners.end(),
                  [this](const std::shared_ptr<EventListener>& e) {
                    if (e->ShouldBeNotifiedOnFileIO()) {
                      listeners_.emplace_back(e);
                    }
                  });
  }

  static IOStatus Create(const std::shared_ptr<FileSystem>& fs,
                         const std::string& fname, const FileOptions& file_opts,
                         std::unique_ptr<RandomAccessFileReader>* reader,
                         IODebugContext* dbg);
  RandomAccessFileReader(const RandomAccessFileReader&) = delete;
  RandomAccessFileReader& operator=(const RandomAccessFileReader&) = delete;
  RandomAccessFileReader(RandomAccessFileReader&&) = delete;
  RandomAccessFileReader& operator=(RandomAccessFileReader&&) = delete;
  ~RandomAccessFileReader() override = default;

  // In non-direct IO mode, mmap reads can return a buffer other than scratch.
  // The FSReadRequest overload can also return a filesystem-provided buffer
  // through result and fs_scratch when scratch is null. The read status is
  // returned in req->status.
  //
  // In direct IO mode, if direct_io_buffer_context is provided then it
  // allocates the aligned buffer and the result refers to a region in
  // direct_io_buffer_context->buffer. Otherwise, results are returned in
  // scratch; unaligned reads use an internal aligned buffer and copy the
  // requested subrange to scratch.
  DECLARE_SYNC_AND_ASYNC_CONST(
      void, Read, const IOOptions& opts, FSReadRequest* req,
      AlignedBufferAllocationContext* direct_io_buffer_context = nullptr,
      IODebugContext* dbg = nullptr);

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts, Slice* result,
                char* scratch, IODebugContext* dbg) const override;

  // REQUIRES: num_reqs > 0 and reqs do not overlap.
  // MultiRead uses direct_io_buffer_context to allocate the aligned buffer in
  // direct IO mode. The result Slices in reqs refer to
  // direct_io_buffer_context->buffer, so callers must keep it alive while those
  // Slices are used. Callers should pass a default-constructed AlignedBuffer
  // when default heap-backed allocation is sufficient. direct_io_buffer_context
  // is ignored in non-direct IO mode.
  DECLARE_SYNC_AND_ASYNC_CONST(
      IOStatus, MultiRead, const IOOptions& opts, FSReadRequest* reqs,
      size_t num_reqs, AlignedBufferAllocationContext* direct_io_buffer_context,
      IODebugContext* dbg = nullptr);

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& opts, IODebugContext* dbg) override {
    return MultiRead(opts, reqs, num_reqs,
                     /*direct_io_buffer_context=*/nullptr, dbg);
  }

  IOStatus Prefetch(const IOOptions& opts, uint64_t offset, size_t n,
                    IODebugContext* dbg = nullptr) const {
    return file_->Prefetch(offset, n, opts, dbg);
  }

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& opts,
                    IODebugContext* dbg) override {
    return Prefetch(opts, offset, n, dbg);
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return file_->GetUniqueId(id, max_size);
  }

  void Hint(AccessPattern pattern) override { file_->Hint(pattern); }

  FSRandomAccessFile* file() { return file_.get(); }

  const std::string& file_name() const { return file_name_; }

  bool use_direct_io() const override { return file_->use_direct_io(); }

  size_t GetRequiredBufferAlignment() const override {
    return file_->GetRequiredBufferAlignment();
  }

  IOStatus InvalidateCache(size_t offset, size_t length) override {
    return file_->InvalidateCache(offset, length);
  }

  IOStatus PrepareIOOptions(const ReadOptions& ro, IOOptions& opts,
                            IODebugContext* dbg = nullptr) const;

  IOStatus ReadAsync(
      FSReadRequest& req, const IOOptions& opts,
      std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
      void** io_handle, IOHandleDeleter* del_fn, AlignedBuf* aligned_buf,
      IODebugContext* dbg = nullptr,
      AlignedBufferAllocationContext* direct_io_buffer_context = nullptr);

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override {
    return ReadAsync(req, opts, std::move(cb), cb_arg, io_handle, del_fn,
                     /*aligned_buf=*/nullptr, dbg);
  }

  bool SubmitReadAsync(FSReadRequest& req, const IOOptions& opts,
                       std::function<void(FSReadRequest&)> cb,
                       IODebugContext* dbg) override;

  Temperature GetTemperature() const override {
    return file_->GetTemperature();
  }

  IOStatus GetFileSize(uint64_t* result) override {
    return file_->GetFileSize(result);
  }

  IOStatus GetFileOpenMetadata(std::string* metadata) override {
    return file_->GetFileOpenMetadata(metadata);
  }

  void ReadAsyncCallback(FSReadRequest& req, void* cb_arg);
};
}  // namespace ROCKSDB_NAMESPACE
