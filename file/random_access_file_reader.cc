//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "file/random_access_file_reader.h"

#include <algorithm>
#include <mutex>

#include "file/file_util.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "port/port.h"
#include "rocksdb/io_status.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/rate_limiter_impl.h"

#if USE_COROUTINES
#include "folly/io/async/EventBase.h"
#endif

namespace ROCKSDB_NAMESPACE {
inline Histograms GetFileReadHistograms(Statistics* stats,
                                        Env::IOActivity io_activity) {
  switch (io_activity) {
    case Env::IOActivity::kFlush:
      return Histograms::FILE_READ_FLUSH_MICROS;
    case Env::IOActivity::kCompaction:
      return Histograms::FILE_READ_COMPACTION_MICROS;
    case Env::IOActivity::kDBOpen:
      return Histograms::FILE_READ_DB_OPEN_MICROS;
    default:
      break;
  }

  if (stats && stats->get_stats_level() > StatsLevel::kExceptDetailedTimers) {
    switch (io_activity) {
      case Env::IOActivity::kGet:
        return Histograms::FILE_READ_GET_MICROS;
      case Env::IOActivity::kMultiGet:
        return Histograms::FILE_READ_MULTIGET_MICROS;
      case Env::IOActivity::kDBIterator:
        return Histograms::FILE_READ_DB_ITERATOR_MICROS;
      case Env::IOActivity::kVerifyDBChecksum:
        return Histograms::FILE_READ_VERIFY_DB_CHECKSUM_MICROS;
      case Env::IOActivity::kVerifyFileChecksums:
        return Histograms::FILE_READ_VERIFY_FILE_CHECKSUMS_MICROS;
      default:
        break;
    }
  }
  return Histograms::HISTOGRAM_ENUM_MAX;
}
inline void RecordIOStats(Statistics* stats, Temperature file_temperature,
                          bool is_last_level, size_t size) {
  IOSTATS_ADD(bytes_read, size);
  // record for last/non-last level
  if (is_last_level) {
    RecordTick(stats, LAST_LEVEL_READ_BYTES, size);
    RecordTick(stats, LAST_LEVEL_READ_COUNT, 1);
  } else {
    RecordTick(stats, NON_LAST_LEVEL_READ_BYTES, size);
    RecordTick(stats, NON_LAST_LEVEL_READ_COUNT, 1);
  }

  // record for temperature file
  switch (file_temperature) {
    case Temperature::kHot:
      IOSTATS_ADD(file_io_stats_by_temperature.hot_file_bytes_read, size);
      IOSTATS_ADD(file_io_stats_by_temperature.hot_file_read_count, 1);
      RecordTick(stats, HOT_FILE_READ_BYTES, size);
      RecordTick(stats, HOT_FILE_READ_COUNT, 1);
      break;
    case Temperature::kWarm:
      IOSTATS_ADD(file_io_stats_by_temperature.warm_file_bytes_read, size);
      IOSTATS_ADD(file_io_stats_by_temperature.warm_file_read_count, 1);
      RecordTick(stats, WARM_FILE_READ_BYTES, size);
      RecordTick(stats, WARM_FILE_READ_COUNT, 1);
      break;
    case Temperature::kCool:
      IOSTATS_ADD(file_io_stats_by_temperature.cool_file_bytes_read, size);
      IOSTATS_ADD(file_io_stats_by_temperature.cool_file_read_count, 1);
      RecordTick(stats, COOL_FILE_READ_BYTES, size);
      RecordTick(stats, COOL_FILE_READ_COUNT, 1);
      break;
    case Temperature::kCold:
      IOSTATS_ADD(file_io_stats_by_temperature.cold_file_bytes_read, size);
      IOSTATS_ADD(file_io_stats_by_temperature.cold_file_read_count, 1);
      RecordTick(stats, COLD_FILE_READ_BYTES, size);
      RecordTick(stats, COLD_FILE_READ_COUNT, 1);
      break;
    case Temperature::kIce:
      IOSTATS_ADD(file_io_stats_by_temperature.ice_file_bytes_read, size);
      IOSTATS_ADD(file_io_stats_by_temperature.ice_file_read_count, 1);
      RecordTick(stats, ICE_FILE_READ_BYTES, size);
      RecordTick(stats, ICE_FILE_READ_COUNT, 1);
      break;
    case Temperature::kUnknown:
      if (is_last_level) {
        IOSTATS_ADD(file_io_stats_by_temperature.unknown_last_level_bytes_read,
                    size);
        IOSTATS_ADD(file_io_stats_by_temperature.unknown_last_level_read_count,
                    1);
      } else {
        IOSTATS_ADD(
            file_io_stats_by_temperature.unknown_non_last_level_bytes_read,
            size);
        IOSTATS_ADD(
            file_io_stats_by_temperature.unknown_non_last_level_read_count, 1);
      }
      break;
    default:
      break;
  }
}

IOStatus RandomAccessFileReader::Create(
    const std::shared_ptr<FileSystem>& fs, const std::string& fname,
    const FileOptions& file_opts,
    std::unique_ptr<RandomAccessFileReader>* reader, IODebugContext* dbg) {
  std::unique_ptr<FSRandomAccessFile> file;
  IOStatus io_s = fs->NewRandomAccessFile(fname, file_opts, &file, dbg);
  if (io_s.ok()) {
    reader->reset(new RandomAccessFileReader(std::move(file), fname));
  }
  return io_s;
}

size_t End(const FSReadRequest& r) {
  return static_cast<size_t>(r.offset) + r.len;
}

FSReadRequest Align(const FSReadRequest& r, size_t alignment) {
  FSReadRequest req;
  req.offset = static_cast<uint64_t>(
      TruncateToPageBoundary(alignment, static_cast<size_t>(r.offset)));
  req.len = Roundup(End(r), alignment) - req.offset;
  req.scratch = nullptr;
  return req;
}

bool TryMerge(FSReadRequest* dest, const FSReadRequest& src) {
  size_t dest_offset = static_cast<size_t>(dest->offset);
  size_t src_offset = static_cast<size_t>(src.offset);
  size_t dest_end = End(*dest);
  size_t src_end = End(src);
  if (std::max(dest_offset, src_offset) > std::min(dest_end, src_end)) {
    return false;
  }
  dest->offset = static_cast<uint64_t>(std::min(dest_offset, src_offset));
  dest->len = std::max(dest_end, src_end) - dest->offset;
  return true;
}

}  // namespace ROCKSDB_NAMESPACE

// clang-format off
#define WITHOUT_COROUTINES
#include "file/random_access_file_reader_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "file/random_access_file_reader_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {

IOStatus RandomAccessFileReader::Read(uint64_t offset, size_t n,
                                      const IOOptions& opts, Slice* result,
                                      char* scratch,
                                      IODebugContext* dbg) const {
  FSReadRequest req;
  req.offset = offset;
  req.len = n;
  req.scratch = scratch;
  Read(opts, &req, /*direct_io_buffer_context=*/nullptr, dbg);
  if (req.fs_scratch != nullptr) {
    *result = Slice();
    if (!req.status.ok()) {
      return std::move(req.status);
    }
    return IOStatus::InvalidArgument(
        "Use the FSReadRequest overload for filesystem-owned buffers");
  }
  *result = req.result;
  return std::move(req.status);
}

IOStatus RandomAccessFileReader::PrepareIOOptions(const ReadOptions& ro,
                                                  IOOptions& opts,
                                                  IODebugContext* dbg) const {
  if (clock_ != nullptr) {
    return PrepareIOFromReadOptions(ro, clock_, opts, dbg);
  } else {
    return PrepareIOFromReadOptions(ro, SystemClock::Default().get(), opts,
                                    dbg);
  }
}

void RandomAccessFileReader::ChargeRateLimiterForRead(const IOOptions& opts,
                                                      size_t bytes) const {
  if (opts.rate_limiter_priority == Env::IO_TOTAL || rate_limiter_ == nullptr) {
    return;
  }
  // TODO: ideally we should call `RateLimiter::RequestToken()` for allowed
  // bytes and then consume those bytes by satisfying as many requests as
  // possible, instead of what we do here, which can cause burst when `bytes`
  // is big.
  size_t remaining_bytes = bytes;
  while (remaining_bytes > 0) {
    const size_t request_bytes =
        std::min(static_cast<size_t>(rate_limiter_->GetSingleBurstBytes()),
                 remaining_bytes);
    rate_limiter_->Request(request_bytes, opts.rate_limiter_priority,
                           nullptr /* stats */, RateLimiter::OpType::kRead);
    remaining_bytes -= request_bytes;
  }
}

// Notes for when direct_io is enabled:
// Unless req.offset, req.len, req.scratch are all already aligned,
// RandomAccessFileReader creates an aligned request and aligned buffer for the
// request. If direct_io_buffer_context is provided, its buffer owns the aligned
// backing storage and its optional allocator is used only for this allocation.
// Otherwise, callers should provide either req.scratch or aligned_buf. If only
// req.scratch is provided, the result is copied from the allocated aligned
// buffer to req.scratch. If only aligned_buf is provided, it is set to the
// aligned buffer allocated by RandomAccessFileReader and saves a copy.
IOStatus RandomAccessFileReader::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, AlignedBuf* aligned_buf,
    IODebugContext* dbg,
    AlignedBufferAllocationContext* direct_io_buffer_context) {
  AlignedBuffer* direct_io_buffer = direct_io_buffer_context != nullptr
                                        ? direct_io_buffer_context->buffer
                                        : nullptr;
  const AlignedBuffer::Allocator* direct_io_allocator =
      direct_io_buffer_context != nullptr ? direct_io_buffer_context->allocator
                                          : nullptr;
  assert(direct_io_buffer_context == nullptr || direct_io_buffer != nullptr);
  IOStatus s;
  TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::ReadAsync:InjectStatus",
                           &s);
  if (!s.ok()) {
    return s;
  }
  // Create a callback and populate info.
  auto read_async_callback =
      std::bind(&RandomAccessFileReader::ReadAsyncCallback, this,
                std::placeholders::_1, std::placeholders::_2);

  ReadAsyncInfo* read_async_info = new ReadAsyncInfo(
      cb, cb_arg, (clock_ != nullptr ? clock_->NowMicros() : 0));

  if (ShouldNotifyListeners()) {
    read_async_info->fs_start_ts_ = FileOperationInfo::StartNow();
  }

  size_t alignment = file_->GetRequiredBufferAlignment();
  // The "already aligned" fast path forwards the caller's request (including
  // its scratch pointer) straight to the FileSystem. It is only safe when the
  // caller actually supplied a scratch buffer to read into. A null scratch
  // combined with an `aligned_buf` out-parameter means the caller expects this
  // reader to allocate the backing buffer (returned via `aligned_buf`), so we
  // must take the allocating path below. Otherwise a null buffer would be
  // submitted to the async read (e.g. a null iovec base to io_uring, which
  // fails with EFAULT).
  const bool caller_needs_allocation =
      req.scratch == nullptr && aligned_buf != nullptr;
  bool is_aligned = direct_io_buffer == nullptr && !caller_needs_allocation &&
                    (req.offset & (alignment - 1)) == 0 &&
                    (req.len & (alignment - 1)) == 0 &&
                    (uintptr_t(req.scratch) & (alignment - 1)) == 0;
  read_async_info->is_aligned_ = is_aligned;

  uint64_t elapsed = 0;
  size_t read_bytes = req.len;
  if (use_direct_io() && is_aligned == false) {
    FSReadRequest aligned_req = Align(req, alignment);
    aligned_req.status.PermitUncheckedError();
    read_bytes = aligned_req.len;

    AlignedBuffer* aligned_read_buffer =
        direct_io_buffer != nullptr ? direct_io_buffer : &read_async_info->buf_;
    aligned_read_buffer->Alignment(alignment);
    Status allocate_status = aligned_read_buffer->AllocateNewBuffer(
        aligned_req.len, direct_io_allocator);
    if (!allocate_status.ok()) {
      delete read_async_info;
      return status_to_io_status(std::move(allocate_status));
    }
    aligned_req.scratch = aligned_read_buffer->BufferStart();

    // Set user provided fields to populate back in callback.
    read_async_info->user_scratch_ = req.scratch;
    read_async_info->user_aligned_buf_ = aligned_buf;
    read_async_info->direct_io_buffer_ = direct_io_buffer;
    read_async_info->user_len_ = req.len;
    read_async_info->user_offset_ = req.offset;
    read_async_info->user_result_ = req.result;

    assert(direct_io_buffer != nullptr ||
           read_async_info->buf_.CurrentSize() == 0);

    StopWatch sw(clock_, stats_, hist_type_,
                 GetFileReadHistograms(stats_, opts.io_activity),
                 (stats_ != nullptr) ? &elapsed : nullptr, true /*overwrite*/,
                 true /*delay_enabled*/);
    s = file_->ReadAsync(aligned_req, opts, read_async_callback,
                         read_async_info, io_handle, del_fn, dbg);
  } else {
    StopWatch sw(clock_, stats_, hist_type_,
                 GetFileReadHistograms(stats_, opts.io_activity),
                 (stats_ != nullptr) ? &elapsed : nullptr, true /*overwrite*/,
                 true /*delay_enabled*/);
    s = file_->ReadAsync(req, opts, read_async_callback, read_async_info,
                         io_handle, del_fn, dbg);
  }
  RecordTick(stats_, READ_ASYNC_MICROS, elapsed);
  // Charged only once the read is submitted: callers fall back to Read() on
  // NotSupported, and Read() charges for itself.
  if (s.ok()) {
    ChargeRateLimiterForRead(opts, read_bytes);
  }

// Suppress false positive clang analyzer warnings.
// Memory is not released if file_->ReadAsync returns !s.ok(), because
// ReadAsyncCallback is never called in that case. If ReadAsyncCallback is
// called then ReadAsync should always return IOStatus::OK().
#ifndef __clang_analyzer__
  if (!s.ok()) {
    delete read_async_info;
  }
#endif  // __clang_analyzer__

  return s;
}

bool RandomAccessFileReader::SubmitReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&)> cb, IODebugContext* dbg) {
#if USE_COROUTINES
  if (file_system_ != nullptr) {
    auto* read_executor = file_system_->GetReadExecutor();
    if (read_executor != nullptr) {
      auto* read_event_base = read_executor->getEventBase();
      assert(read_event_base != nullptr);
      auto task = [](RandomAccessFileReader* reader, FSReadRequest* task_req,
                     IOOptions task_opts,
                     std::function<void(FSReadRequest&)> task_cb,
                     IODebugContext* task_dbg) -> folly::coro::Task<void> {
        co_await folly::coro::co_nothrow(reader->ReadCoroutine(
            task_opts, task_req,
            /*direct_io_buffer_context=*/nullptr, task_dbg));
        task_cb(*task_req);
      }(this, &req, opts, std::move(cb), dbg);
      auto task_with_executor = folly::coro::co_withExecutor(
          folly::Executor::getKeepAliveToken(read_event_base), std::move(task));
      if (read_event_base->inRunningEventBaseThread()) {
        std::move(task_with_executor).startInlineUnsafe();
      } else {
        std::move(task_with_executor).start();
      }
      return true;
    }
  }
#endif  // USE_COROUTINES
  return FSRandomAccessFile::SubmitReadAsync(req, opts, std::move(cb), dbg);
}

void RandomAccessFileReader::ReadAsyncCallback(FSReadRequest& req,
                                               void* cb_arg) {
  ReadAsyncInfo* read_async_info = static_cast<ReadAsyncInfo*>(cb_arg);
  assert(read_async_info);
  assert(read_async_info->cb_);

  if (use_direct_io() && read_async_info->is_aligned_ == false) {
    // Create FSReadRequest with user provided fields.
    FSReadRequest user_req;
    user_req.scratch = read_async_info->user_scratch_;
    user_req.offset = read_async_info->user_offset_;
    user_req.len = read_async_info->user_len_;

    // Update results in user_req.
    user_req.result = req.result;
    user_req.status = req.status;

    size_t current_size = req.result.size();
    if (read_async_info->direct_io_buffer_ == nullptr) {
      read_async_info->buf_.Size(read_async_info->buf_.CurrentSize() +
                                 req.result.size());
      current_size = read_async_info->buf_.CurrentSize();
    }

    size_t offset_advance_len = static_cast<size_t>(
        /*offset_passed_by_user=*/read_async_info->user_offset_ -
        /*aligned_offset=*/req.offset);

    size_t res_len = 0;
    if (req.status.ok() && offset_advance_len < current_size) {
      res_len = std::min(current_size - offset_advance_len,
                         read_async_info->user_len_);
      if (read_async_info->direct_io_buffer_ != nullptr) {
        user_req.scratch = read_async_info->direct_io_buffer_->BufferStart() +
                           offset_advance_len;
      } else if (read_async_info->user_aligned_buf_ == nullptr) {
        // Copy the data into user's scratch.
// Clang analyzer assumes that it will take use_direct_io() == false in
// ReadAsync and use_direct_io() == true in Callback which cannot be true.
#ifndef __clang_analyzer__
        read_async_info->buf_.Read(user_req.scratch, offset_advance_len,
                                   res_len);
#endif  // __clang_analyzer__
      } else {
        // Set aligned_buf provided by user without additional copy.
        user_req.scratch =
            read_async_info->buf_.BufferStart() + offset_advance_len;
        *read_async_info->user_aligned_buf_ = read_async_info->buf_.Release();
      }
      user_req.result = Slice(user_req.scratch, res_len);
    } else {
      // Either req.status is not ok or data was not read.
      user_req.result = Slice();
    }
    read_async_info->cb_(user_req, read_async_info->cb_arg_);
  } else {
    read_async_info->cb_(req, read_async_info->cb_arg_);
  }

  // Update stats and notify listeners.
  if (stats_ != nullptr && file_read_hist_ != nullptr) {
    // elapsed doesn't take into account delay and overwrite as StopWatch does
    // in Read.
    uint64_t elapsed = clock_->NowMicros() - read_async_info->start_time_;
    file_read_hist_->Add(elapsed);
  }
  if (req.status.ok()) {
    RecordInHistogram(stats_, ASYNC_READ_BYTES, req.result.size());
  } else if (!req.status.IsAborted()) {
    RecordTick(stats_, ASYNC_READ_ERROR_COUNT, 1);
  }
  if (ShouldNotifyListeners()) {
    auto finish_ts = FileOperationInfo::FinishNow();
    NotifyOnFileReadFinish(req.offset, req.result.size(),
                           read_async_info->fs_start_ts_, finish_ts,
                           req.status);
  }
  if (!req.status.ok()) {
    NotifyOnIOError(req.status, FileOperationType::kRead, file_name(),
                    req.result.size(), req.offset);
  }
  RecordIOStats(stats_, file_temperature_, is_last_level_, req.result.size());
  delete read_async_info;
}
}  // namespace ROCKSDB_NAMESPACE
