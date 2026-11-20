//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstring>

#include "util/coro_utils.h"
#include "util/defer.h"

#if defined(USE_COROUTINES)
#include <atomic>
#include <memory>

#include "folly/coro/Baton.h"
#include "folly/coro/Coroutine.h"
#include "folly/io/async/Request.h"
#endif

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

#if defined(WITH_COROUTINES)
inline folly::coro::Task<void> SubmitMultiReadAsync(
    FSRandomAccessFile* file, FSReadRequest* fs_reqs, size_t num_fs_reqs,
    const IOOptions& opts, Statistics* stats, IODebugContext* dbg) {
  if (num_fs_reqs == 0) {
    co_return;
  }
  folly::coro::Baton baton;
  std::atomic<size_t> pending{num_fs_reqs};
#ifndef NDEBUG
  // DB async reads are internally driven to completion; exiting while
  // filesystem callbacks are outstanding would invalidate this stack state.
  Defer assert_no_pending_reads(
      [&] { assert(pending.load(std::memory_order_acquire) == 0); });
#endif  // NDEBUG
  for (size_t i = 0; i < num_fs_reqs; ++i) {
    if (!file->SubmitReadAsync(
            fs_reqs[i], opts,
            [&baton, &pending](FSReadRequest& /*req*/) {
              if (pending.fetch_sub(1) == 1) {
                // If ViaIfAsync is already waiting, post from null so its
                // executor callback does not also capture the read's stats as
                // the outer context restored after continuation completes.
                folly::RequestContextScopeGuard null_context{
                    std::shared_ptr<folly::RequestContext>{}};
                baton.post();
              }
            },
            dbg)) {
      RecordTick(stats, FILE_SUBMIT_ASYNC_READ_FALLBACK, 1);
    }
  }

  co_await baton;
}
#endif  // WITH_COROUTINES

DEFINE_SYNC_AND_ASYNC(void, RandomAccessFileReader::Read)
(const IOOptions& opts, FSReadRequest* read_req,
 AlignedBufferAllocationContext* direct_io_buffer_context, IODebugContext* dbg)
    const {
  assert(read_req != nullptr);
  const uint64_t offset = read_req->offset;
  const size_t n = read_req->len;
  Slice* result = &read_req->result;
  char* scratch = read_req->scratch;
  AlignedBuffer* direct_io_buffer = direct_io_buffer_context != nullptr
                                        ? direct_io_buffer_context->buffer
                                        : nullptr;
  const AlignedBuffer::Allocator* direct_io_allocator =
      direct_io_buffer_context != nullptr ? direct_io_buffer_context->allocator
                                          : nullptr;
  assert(direct_io_buffer_context == nullptr || direct_io_buffer != nullptr);
  const Env::IOPriority rate_limiter_priority = opts.rate_limiter_priority;

  TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::Read", nullptr);
  TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::Read:IODebugContext",
                           const_cast<void*>(static_cast<void*>(dbg)));

  // To be paranoid: modify scratch a little bit, so in case underlying
  // FileSystem doesn't fill the buffer but return success and `scratch` returns
  // contains a previous block, returned value will not pass checksum.
  if (n > 0 && scratch != nullptr) {
    // This byte might not change anything for direct I/O case, but it's OK.
    scratch[0]++;
  }

  IOStatus io_s;
  uint64_t elapsed = 0;
  size_t alignment = file_->GetRequiredBufferAlignment();
  bool is_aligned = false;
  if (direct_io_buffer == nullptr && scratch != nullptr) {
    // Check if offset, length and buffer are aligned.
    is_aligned = (offset & (alignment - 1)) == 0 &&
                 (n & (alignment - 1)) == 0 &&
                 (uintptr_t(scratch) & (alignment - 1)) == 0;
  }

  {
    StopWatch sw(clock_, stats_, hist_type_,
                 GetFileReadHistograms(stats_, opts.io_activity),
                 (stats_ != nullptr) ? &elapsed : nullptr, true /*overwrite*/,
                 true /*delay_enabled*/);
    IOSTATS_TIMER_GUARD(read_nanos);
    if (use_direct_io() && is_aligned == false) {
      size_t aligned_offset =
          TruncateToPageBoundary(alignment, static_cast<size_t>(offset));
      size_t offset_advance = static_cast<size_t>(offset) - aligned_offset;
      size_t read_size =
          Roundup(static_cast<size_t>(offset + n), alignment) - aligned_offset;
      AlignedBuffer buf;
      AlignedBuffer* aligned_read_buffer =
          direct_io_buffer != nullptr ? direct_io_buffer : &buf;
      aligned_read_buffer->Alignment(alignment);
      Status allocate_status = aligned_read_buffer->AllocateNewBuffer(
          read_size, direct_io_allocator);
      if (!allocate_status.ok()) {
        io_s = status_to_io_status(std::move(allocate_status));
      }
      char* aligned_scratch = aligned_read_buffer->BufferStart();
      size_t current_size = 0;
      while (io_s.ok() && current_size < read_size) {
        size_t allowed;
        if (rate_limiter_priority != Env::IO_TOTAL &&
            rate_limiter_ != nullptr) {
          allowed = rate_limiter_->RequestToken(
              read_size - current_size, alignment, rate_limiter_priority,
              stats_, RateLimiter::OpType::kRead);
        } else {
          assert(current_size == 0);
          allowed = read_size;
        }
        Slice tmp;

        FileOperationInfo::StartTimePoint start_ts;
        uint64_t orig_offset = 0;
        if (ShouldNotifyListeners()) {
          start_ts = FileOperationInfo::StartNow();
          orig_offset = aligned_offset + current_size;
        }

        // Only user reads are expected to specify a timeout. And user reads
        // are not subjected to rate_limiter and should go through only
        // one iteration of this loop, so we don't need to check and adjust
        // the opts.timeout before issuing the read.
        assert(!opts.timeout.count() || allowed == read_size);
#if defined(USE_COROUTINES) && defined(WITH_COROUTINES)
        FSReadRequest fs_req;
        fs_req.offset = aligned_offset + current_size;
        fs_req.len = allowed;
        fs_req.scratch = aligned_scratch + current_size;
        fs_req.status.PermitUncheckedError();
        TEST_SYNC_POINT_CALLBACK(
            "RandomAccessFileReader::ReadCoroutine:SubmitReadAsync", &fs_req);
        co_await SubmitMultiReadAsync(file_.get(), &fs_req, 1, opts, stats_,
                                      dbg);
        io_s = fs_req.status;
        tmp = fs_req.result;
#else
        {
          IOSTATS_CPU_TIMER_GUARD(cpu_read_nanos, clock_);
          io_s = file_->Read(aligned_offset + current_size, allowed, opts, &tmp,
                             aligned_scratch + current_size, dbg);
        }
#endif
        if (ShouldNotifyListeners()) {
          auto finish_ts = FileOperationInfo::FinishNow();
          NotifyOnFileReadFinish(orig_offset, tmp.size(), start_ts, finish_ts,
                                 io_s);
          if (!io_s.ok()) {
            NotifyOnIOError(io_s, FileOperationType::kRead, file_name(),
                            tmp.size(), orig_offset);
          }
        }

        current_size += tmp.size();
        if (direct_io_buffer == nullptr) {
          buf.Size(current_size);
        }
        if (!io_s.ok() || tmp.size() < allowed) {
          break;
        }
      }
      size_t res_len = 0;
      if (io_s.ok() && offset_advance < current_size) {
        res_len = std::min(current_size - offset_advance, n);
        if (direct_io_buffer != nullptr) {
          scratch = aligned_scratch + offset_advance;
        } else {
          assert(scratch != nullptr);
          buf.Read(scratch, offset_advance, res_len);
        }
      }
      *result = Slice(scratch, res_len);
    } else {
      size_t pos = 0;
      const char* res_scratch = nullptr;
      while (pos < n) {
        size_t allowed;
        if (rate_limiter_priority != Env::IO_TOTAL &&
            rate_limiter_ != nullptr) {
          if (rate_limiter_->IsRateLimited(RateLimiter::OpType::kRead)) {
            sw.DelayStart();
          }
          const size_t remaining_bytes = n - pos;
          allowed = rate_limiter_->RequestToken(
              remaining_bytes, (use_direct_io() ? alignment : 0),
              rate_limiter_priority, stats_, RateLimiter::OpType::kRead);
          // A null scratch lets the FileSystem provide the result buffer.
          // Acquire all tokens before issuing one read since independently
          // allocated chunks cannot share one result and fs_scratch owner.
          while (scratch == nullptr && allowed < remaining_bytes) {
            allowed += rate_limiter_->RequestToken(
                remaining_bytes - allowed, (use_direct_io() ? alignment : 0),
                rate_limiter_priority, stats_, RateLimiter::OpType::kRead);
          }
          if (rate_limiter_->IsRateLimited(RateLimiter::OpType::kRead)) {
            sw.DelayStop();
          }
        } else {
          allowed = n;
        }
        Slice tmp_result;

        FileOperationInfo::StartTimePoint start_ts;
        if (ShouldNotifyListeners()) {
          start_ts = FileOperationInfo::StartNow();
        }

        // Only user reads are expected to specify a timeout. And user reads
        // are not subjected to rate_limiter and should go through only
        // one iteration of this loop, so we don't need to check and adjust
        // the opts.timeout before issuing the read.
        assert(!opts.timeout.count() || allowed == n);
#if defined(USE_COROUTINES) && defined(WITH_COROUTINES)
        FSReadRequest fs_req;
        fs_req.offset = offset + pos;
        fs_req.len = allowed;
        fs_req.scratch = scratch != nullptr ? scratch + pos : nullptr;
        fs_req.status.PermitUncheckedError();
        TEST_SYNC_POINT_CALLBACK(
            "RandomAccessFileReader::ReadCoroutine:SubmitReadAsync", &fs_req);
        co_await SubmitMultiReadAsync(file_.get(), &fs_req, 1, opts, stats_,
                                      dbg);
        io_s = fs_req.status;
        tmp_result = fs_req.result;
        if (scratch == nullptr) {
          read_req->fs_scratch = std::move(fs_req.fs_scratch);
        }
#else
        {
          IOSTATS_CPU_TIMER_GUARD(cpu_read_nanos, clock_);
          if (scratch == nullptr) {
            // MultiRead lets the file system allocate the buffer when scratch
            // is null.
            FSReadRequest fs_req;
            fs_req.offset = offset + pos;
            fs_req.len = allowed;
            fs_req.scratch = nullptr;
            fs_req.status.PermitUncheckedError();
            IOStatus multi_read_status =
                file_->MultiRead(&fs_req, /*num_reqs=*/1, opts, dbg);
            if (!multi_read_status.ok()) {
              fs_req.status = std::move(multi_read_status);
            }
            io_s = fs_req.status;
            tmp_result = fs_req.result;
            read_req->fs_scratch = std::move(fs_req.fs_scratch);
          } else {
            io_s = file_->Read(offset + pos, allowed, opts, &tmp_result,
                               scratch + pos, dbg);
          }
        }
#endif
        if (ShouldNotifyListeners()) {
          auto finish_ts = FileOperationInfo::FinishNow();
          NotifyOnFileReadFinish(offset + pos, tmp_result.size(), start_ts,
                                 finish_ts, io_s);

          if (!io_s.ok()) {
            NotifyOnIOError(io_s, FileOperationType::kRead, file_name(),
                            tmp_result.size(), offset + pos);
          }
        }
        if (res_scratch == nullptr) {
          // we can't simply use `scratch` because reads of mmap'd files return
          // data in a different buffer.
          res_scratch = tmp_result.data();
        } else {
          // make sure chunks are inserted contiguously into `res_scratch`.
          assert(tmp_result.data() == res_scratch + pos);
        }
        pos += tmp_result.size();
        if (!io_s.ok() || tmp_result.size() < allowed) {
          break;
        }
      }
      *result = Slice(res_scratch, io_s.ok() ? pos : 0);
    }
    RecordIOStats(stats_, file_temperature_, is_last_level_, result->size());
  }
  if (stats_ != nullptr && file_read_hist_ != nullptr) {
    file_read_hist_->Add(elapsed);
  }

#ifndef NDEBUG
  auto pair = std::make_pair(&file_name_, &io_s);
  if (offset == 0) {
    TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::Read::BeforeReturn",
                             &pair);
  }
  TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::Read::AnyOffset", &pair);
#endif
  read_req->status = std::move(io_s);
  CO_RETURN;
}

DEFINE_SYNC_AND_ASYNC(IOStatus, RandomAccessFileReader::MultiRead)
(const IOOptions& opts, FSReadRequest* read_reqs, size_t num_reqs,
 AlignedBufferAllocationContext* direct_io_buffer_context, IODebugContext* dbg)
    const {
  assert(num_reqs > 0);
  AlignedBuffer local_direct_io_buffer;
  AlignedBuffer* direct_io_buffer =
      use_direct_io() ? (direct_io_buffer_context != nullptr
                             ? direct_io_buffer_context->buffer
                             : &local_direct_io_buffer)
                      : nullptr;
  const AlignedBuffer::Allocator* direct_io_allocator =
      direct_io_buffer_context != nullptr ? direct_io_buffer_context->allocator
                                          : nullptr;
  assert(!use_direct_io() || direct_io_buffer != nullptr);

#ifndef NDEBUG
  if (use_direct_io() && direct_io_buffer_context == nullptr) {
    for (size_t i = 0; i < num_reqs; ++i) {
      assert(read_reqs[i].len == 0 || read_reqs[i].scratch != nullptr);
    }
  }
#endif

  const Env::IOPriority rate_limiter_priority = opts.rate_limiter_priority;

  // To be paranoid modify scratch a little bit, so in case underlying
  // FileSystem doesn't fill the buffer but return success and `scratch` returns
  // contains a previous block, returned value will not pass checksum.
  // This byte might not change anything for direct I/O case, but it's OK.
  for (size_t i = 0; i < num_reqs; i++) {
    FSReadRequest& r = read_reqs[i];
    if (r.len > 0 && r.scratch != nullptr) {
      r.scratch[0]++;
    }
  }

  IOStatus io_s;
  uint64_t elapsed = 0;
  {
    StopWatch sw(clock_, stats_, hist_type_,
                 GetFileReadHistograms(stats_, opts.io_activity),
                 (stats_ != nullptr) ? &elapsed : nullptr, true /*overwrite*/,
                 true /*delay_enabled*/);
    IOSTATS_TIMER_GUARD(read_nanos);

    FSReadRequest* fs_reqs = read_reqs;
    size_t num_fs_reqs = num_reqs;
    std::vector<FSReadRequest> aligned_reqs;
    std::vector<size_t> read_req_order;
    bool use_caller_direct_io_buffers = false;
    bool read_reqs_are_sorted = true;
    if (use_direct_io()) {
      const size_t alignment = file_->GetRequiredBufferAlignment();
      use_caller_direct_io_buffers = direct_io_buffer_context == nullptr;
      for (size_t i = 0; i < num_reqs; ++i) {
        const FSReadRequest& req = read_reqs[i];
        if (i > 0 && read_reqs[i - 1].offset > req.offset) {
          read_reqs_are_sorted = false;
        }
        if (use_caller_direct_io_buffers &&
            (!AlignedBuffer::isAligned(static_cast<size_t>(req.offset),
                                       alignment) ||
             !AlignedBuffer::isAligned(req.len, alignment) ||
             (req.len > 0 &&
              (req.scratch == nullptr ||
               !AlignedBuffer::isAligned(req.scratch, alignment))))) {
          use_caller_direct_io_buffers = false;
        }
      }
    }
    if (use_direct_io() && !use_caller_direct_io_buffers) {
      if (!read_reqs_are_sorted) {
        // Alignment merges each request only with aligned_reqs.back(), so
        // process requests in increasing offset order.
        read_req_order.resize(num_reqs);
        for (size_t i = 0; i < num_reqs; ++i) {
          read_req_order[i] = i;
        }
        std::sort(read_req_order.begin(), read_req_order.end(),
                  [read_reqs](size_t lhs, size_t rhs) {
                    return read_reqs[lhs].offset < read_reqs[rhs].offset;
                  });
      }
      // num_reqs is the max possible size,
      // this can reduce std::vector's internal resize operations.
      aligned_reqs.reserve(num_reqs);
      // Align and merge the read requests.
      size_t alignment = file_->GetRequiredBufferAlignment();
      for (size_t i = 0; i < num_reqs; i++) {
        const size_t read_req_i =
            read_req_order.empty() ? i : read_req_order[i];
        FSReadRequest r = Align(read_reqs[read_req_i], alignment);
        if (i == 0) {
          // head
          aligned_reqs.push_back(std::move(r));

        } else if (!TryMerge(&aligned_reqs.back(), r)) {
          // head + n
          aligned_reqs.push_back(std::move(r));

        } else {
          // unused
          r.status.PermitUncheckedError();
        }
      }
      TEST_SYNC_POINT_CALLBACK("RandomAccessFileReader::MultiRead:AlignedReqs",
                               &aligned_reqs);

      // Allocate aligned buffer and let scratch buffers point to it.
      size_t total_len = 0;
      for (const auto& r : aligned_reqs) {
        total_len += r.len;
      }
      direct_io_buffer->Alignment(alignment);
      Status allocate_status =
          direct_io_buffer->AllocateNewBuffer(total_len, direct_io_allocator);
      if (!allocate_status.ok()) {
        io_s = status_to_io_status(std::move(allocate_status));
      }
      if (io_s.ok()) {
        char* scratch = direct_io_buffer->BufferStart();
        for (auto& r : aligned_reqs) {
          r.scratch = scratch;
          scratch += r.len;
        }
      }

      fs_reqs = aligned_reqs.data();
      num_fs_reqs = aligned_reqs.size();
    }

    FileOperationInfo::StartTimePoint start_ts;
    if (ShouldNotifyListeners()) {
      start_ts = FileOperationInfo::StartNow();
    }

    if (io_s.ok()) {
#if defined(WITHOUT_COROUTINES)
      IOSTATS_CPU_TIMER_GUARD(cpu_read_nanos, clock_);
#endif
      if (rate_limiter_priority != Env::IO_TOTAL && rate_limiter_ != nullptr) {
        size_t total_multi_read_size = 0;
        assert(fs_reqs != nullptr);
        for (size_t i = 0; i < num_fs_reqs; ++i) {
          FSReadRequest& req = fs_reqs[i];
          total_multi_read_size += req.len;
        }
        ChargeRateLimiterForRead(opts, total_multi_read_size);
      }
      TEST_SYNC_POINT_CALLBACK(
          "RandomAccessFileReader::MultiRead:IODebugContext",
          const_cast<void*>(static_cast<void*>(dbg)));
#if defined(USE_COROUTINES) && defined(WITH_COROUTINES)
      co_await SubmitMultiReadAsync(file_.get(), fs_reqs, num_fs_reqs, opts,
                                    stats_, dbg);
#else
      io_s = file_->MultiRead(fs_reqs, num_fs_reqs, opts, dbg);
#endif
      RecordInHistogram(stats_, MULTIGET_IO_BATCH_SIZE, num_fs_reqs);
    }

    if (use_direct_io() && !use_caller_direct_io_buffers && io_s.ok()) {
      // Populate results in the unaligned read requests.
      size_t aligned_i = 0;
      for (size_t i = 0; i < num_reqs; i++) {
        const size_t read_req_i =
            read_req_order.empty() ? i : read_req_order[i];
        auto& r = read_reqs[read_req_i];
        if (static_cast<size_t>(r.offset) > End(aligned_reqs[aligned_i])) {
          aligned_i++;
        }
        const auto& fs_r = fs_reqs[aligned_i];
        r.status = fs_r.status;
        if (r.status.ok()) {
          uint64_t offset = r.offset - fs_r.offset;
          if (fs_r.result.size() <= offset) {
            // No byte in the read range is returned.
            r.result = Slice();
          } else {
            size_t len = std::min(
                r.len, static_cast<size_t>(fs_r.result.size() - offset));
            const char* result_data = fs_r.scratch + offset;
            if (direct_io_buffer_context == nullptr) {
              if (len > 0) {
                std::memcpy(r.scratch, result_data, len);
              }
              r.result = Slice(r.scratch, len);
            } else {
              r.result = Slice(result_data, len);
            }
          }
        } else {
          r.result = Slice();
        }
      }
    }

    const bool overall_io_ok = io_s.ok();
    for (size_t i = 0; i < num_reqs; ++i) {
      const IOStatus& req_status = overall_io_ok ? read_reqs[i].status : io_s;
      const size_t result_size = overall_io_ok ? read_reqs[i].result.size() : 0;
      if (ShouldNotifyListeners()) {
        auto finish_ts = FileOperationInfo::FinishNow();
        NotifyOnFileReadFinish(read_reqs[i].offset, result_size, start_ts,
                               finish_ts, req_status);
      }
      if (!req_status.ok()) {
        NotifyOnIOError(req_status, FileOperationType::kRead, file_name(),
                        result_size, read_reqs[i].offset);
      }
      RecordIOStats(stats_, file_temperature_, is_last_level_, result_size);
    }
  }
  if (stats_ != nullptr && file_read_hist_ != nullptr) {
    file_read_hist_->Add(elapsed);
  }

  CO_RETURN io_s;
}
}  // namespace ROCKSDB_NAMESPACE
#endif
