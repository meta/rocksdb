//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#if USE_COROUTINES
#include "util/async_file_reader.h"

#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
bool AsyncFileReader::MultiReadAsyncImpl(ReadAwaiter* awaiter) {
  if (tail_) {
    tail_->next_ = awaiter;
  }
  tail_ = awaiter;
  if (!head_) {
    head_ = awaiter;
  }
  num_reqs_ += awaiter->num_reqs_;
  awaiter->io_handle_.resize(awaiter->num_reqs_);
  awaiter->del_fn_.resize(awaiter->num_reqs_);
  const bool use_direct_io = awaiter->file_->use_direct_io();
  for (size_t i = 0; i < awaiter->num_reqs_; ++i) {
    FSReadRequest& req = awaiter->read_reqs_[i];
    // A direct-IO request without a scratch buffer gets an aligned buffer from
    // ReadAsync, returned through fs_scratch. The caller must keep the request
    // until it has consumed the result.
    AlignedBuf* const aligned_buf =
        use_direct_io && req.scratch == nullptr ? &req.fs_scratch : nullptr;
    IOStatus s = awaiter->file_->ReadAsync(
        req, awaiter->opts_,
        [](FSReadRequest& completed_req, void* cb_arg) {
          FSReadRequest* read_req = static_cast<FSReadRequest*>(cb_arg);
          read_req->status = completed_req.status;
          read_req->result = completed_req.result;
          if (completed_req.fs_scratch != nullptr) {
            read_req->fs_scratch = std::move(completed_req.fs_scratch);
          }
        },
        &req, &awaiter->io_handle_[i], &awaiter->del_fn_[i], aligned_buf,
        awaiter->dbg_);
    if (s.IsNotSupported()) {
      // SupportedOps is advisory, so ReadAsync can still report NotSupported
      // when the request is submitted. Read synchronously instead so the
      // MultiGet caller doesn't see a spurious error. FilePrefetchBuffer and
      // IODispatcher use the same fallback.
      TEST_SYNC_POINT("AsyncFileReader::MultiReadAsyncImpl:SyncFallback");
      awaiter->io_handle_[i] = nullptr;
      awaiter->del_fn_[i] = nullptr;
      if (aligned_buf != nullptr) {
        AlignedBuffer buffer;
        AlignedBufferAllocationContext context{&buffer};
        awaiter->file_->Read(awaiter->opts_, &req, &context, awaiter->dbg_);
        *aligned_buf = buffer.Release();
      } else {
        awaiter->file_->Read(awaiter->opts_, &req,
                             /*direct_io_buffer_context=*/nullptr,
                             awaiter->dbg_);
      }
    } else if (!s.ok()) {
      // For any non-ok status, the FileSystem will not call the callback
      // So let's update the status ourselves
      req.status = s;
    }
  }
  return true;
}

void AsyncFileReader::Wait() {
  if (!head_) {
    return;
  }
  ReadAwaiter* waiter;
  std::vector<void*> io_handles;
  IOStatus s;
  io_handles.reserve(num_reqs_);
  waiter = head_;
  do {
    for (size_t i = 0; i < waiter->num_reqs_; ++i) {
      if (waiter->io_handle_[i]) {
        io_handles.push_back(waiter->io_handle_[i]);
      }
    }
  } while (waiter != tail_ && (waiter = waiter->next_));
  if (io_handles.size() > 0) {
    StopWatch sw(SystemClock::Default().get(), stats_, POLL_WAIT_MICROS);
    s = fs_->Poll(io_handles, io_handles.size());
  }
  TEST_SYNC_POINT_CALLBACK("AsyncFileReader::Wait:InjectPollStatus", &s);
  do {
    waiter = head_;
    head_ = waiter->next_;

    for (size_t i = 0; i < waiter->num_reqs_; ++i) {
      if (waiter->io_handle_[i] && waiter->del_fn_[i]) {
        waiter->del_fn_[i](waiter->io_handle_[i]);
      }
      if (waiter->io_handle_[i] && waiter->read_reqs_[i].status.ok() &&
          !s.ok()) {
        // Override the request status with the Poll error
        waiter->read_reqs_[i].status = s;
      }
    }
    waiter->awaiting_coro_.resume();
  } while (waiter != tail_);
  head_ = tail_ = nullptr;
  RecordInHistogram(stats_, MULTIGET_IO_BATCH_SIZE, num_reqs_);
  num_reqs_ = 0;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // USE_COROUTINES
