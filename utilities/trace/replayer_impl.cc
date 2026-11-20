//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trace/replayer_impl.h"

#include <cmath>
#include <thread>

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/system_clock.h"
#include "util/threadpool_imp.h"

namespace ROCKSDB_NAMESPACE {

ReplayerImpl::ReplayerImpl(DB* db,
                           const std::vector<ColumnFamilyHandle*>& handles,
                           std::unique_ptr<TraceReader>&& reader)
    : Replayer(),
      trace_reader_(std::move(reader)),
      prepared_(false),
      trace_end_(false),
      header_ts_(0),
      exec_handler_(TraceRecord::NewExecutionHandler(db, handles)),
      env_(db->GetEnv()),
      trace_file_version_(-1) {}

ReplayerImpl::~ReplayerImpl() {
  exec_handler_.reset();
  trace_reader_.reset();
}

Status ReplayerImpl::Prepare() {
  Trace header;
  int db_version;
  Status s = ReadHeader(&header);
  if (!s.ok()) {
    return s;
  }
  s = TracerHelper::ParseTraceHeader(header, &trace_file_version_, &db_version);
  if (!s.ok()) {
    return s;
  }
  header_ts_ = header.ts;
  prepared_ = true;
  trace_end_ = false;
  return Status::OK();
}

Status ReplayerImpl::Next(std::unique_ptr<TraceRecord>* record) {
  if (!prepared_) {
    return Status::Incomplete("Not prepared!");
  }
  if (trace_end_) {
    return Status::Incomplete("Trace end.");
  }

  Trace trace;
  Status s = ReadTrace(&trace);  // ReadTrace is atomic
  // Reached the trace end.
  if (s.ok() && trace.type == kTraceEnd) {
    trace_end_ = true;
    return Status::Incomplete("Trace end.");
  }
  if (!s.ok() || record == nullptr) {
    return s;
  }

  return TracerHelper::DecodeTraceRecord(&trace, trace_file_version_, record);
}

Status ReplayerImpl::Execute(const std::unique_ptr<TraceRecord>& record,
                             std::unique_ptr<TraceRecordResult>* result) {
  return record->Accept(exec_handler_.get(), result);
}

Status ReplayerImpl::Replay(
    const ReplayOptions& options,
    const std::function<void(Status, std::unique_ptr<TraceRecordResult>&&)>&
        result_callback) {
  if (options.fast_forward <= 0.0) {
    return Status::InvalidArgument("Wrong fast forward speed!");
  }

  if (!prepared_) {
    return Status::Incomplete("Not prepared!");
  }
  if (trace_end_) {
    return Status::Incomplete("Trace end.");
  }

  Status s = Status::OK();
  // Set when a record's execution (or decoding) fails, so that its status is
  // returned even when it is Incomplete, which otherwise means trace end.
  bool record_failed = false;

  if (options.num_threads <= 1) {
    // num_threads == 0 or num_threads == 1 uses single thread.
    std::chrono::system_clock::time_point replay_epoch =
        std::chrono::system_clock::now();

    while (s.ok()) {
      Trace trace;
      s = ReadTrace(&trace);
      // If already at trace end, ReadTrace should return Status::Incomplete().
      if (!s.ok()) {
        break;
      }

      // No need to sleep before breaking the loop if at the trace end.
      if (trace.type == kTraceEnd) {
        trace_end_ = true;
        s = Status::Incomplete("Trace end.");
        break;
      }

      // In single-threaded replay, decode first then sleep.
      std::unique_ptr<TraceRecord> record;
      s = TracerHelper::DecodeTraceRecord(&trace, trace_file_version_, &record);
      if (!s.ok() && !s.IsNotSupported()) {
        record_failed = true;
        break;
      }

      std::chrono::system_clock::time_point sleep_to =
          replay_epoch +
          std::chrono::microseconds(static_cast<uint64_t>(std::llround(
              1.0 * (trace.ts - header_ts_) / options.fast_forward)));
      if (sleep_to > std::chrono::system_clock::now()) {
        std::this_thread::sleep_until(sleep_to);
      }

      // Skip unsupported traces, stop for other errors.
      if (s.IsNotSupported()) {
        if (result_callback != nullptr) {
          result_callback(s, nullptr);
        }
        s = Status::OK();
        continue;
      }

      if (result_callback == nullptr) {
        s = Execute(record, nullptr);
      } else {
        std::unique_ptr<TraceRecordResult> res;
        s = Execute(record, &res);
        result_callback(s, std::move(res));
      }
      record_failed = !s.ok();
    }
  } else {
    // Multi-threaded replay.
    ThreadPoolImpl thread_pool;
    thread_pool.SetHostEnv(env_);
    thread_pool.SetBackgroundThreads(static_cast<int>(options.num_threads));

    std::mutex mtx;
    // Background decoding and execution status. Guarded by mtx; read without
    // it only after the thread pool is joined.
    Status bg_s = Status::OK();
    uint64_t last_err_ts = static_cast<uint64_t>(-1);
    // Set with bg_s so the scheduling loop can stop without taking mtx.
    std::atomic<bool> bg_failed{false};
    // Callback function used in background work to update bg_s for the ealiest
    // TraceRecord which has execution error. This is different from the
    // timestamp of the first execution error (either start or end timestamp).
    //
    // Suppose TraceRecord R1, R2, with timestamps T1 < T2. Their execution
    // timestamps are T1_start, T1_end, T2_start, T2_end.
    // Single-thread: there must be T1_start < T1_end < T2_start < T2_end.
    // Multi-thread: T1_start < T2_start may not be enforced. Orders of them are
    // totally unknown.
    // In order to report the same `first` error in both single-thread and
    // multi-thread replay, we can only rely on the TraceRecords' timestamps,
    // rather than their executin timestamps. Although in single-thread replay,
    // the first error is also the last error, while in multi-thread replay, the
    // first error may not be the first error in execution, and it may not be
    // the last error in exeution as well.
    auto error_cb = [&mtx, &bg_s, &last_err_ts, &bg_failed](const Status& err,
                                                            uint64_t err_ts) {
      std::lock_guard<std::mutex> gd(mtx);
      // Only record the first error.
      if (!err.ok() && err_ts < last_err_ts) {
        bg_s = err;
        last_err_ts = err_ts;
        bg_failed.store(true, std::memory_order_relaxed);
      }
    };

    std::chrono::system_clock::time_point replay_epoch =
        std::chrono::system_clock::now();

    while (!bg_failed.load(std::memory_order_relaxed) && s.ok()) {
      Trace trace;
      s = ReadTrace(&trace);
      // If already at trace end, ReadTrace should return Status::Incomplete().
      if (!s.ok()) {
        break;
      }

      TraceType trace_type = trace.type;

      // No need to sleep before breaking the loop if at the trace end.
      if (trace_type == kTraceEnd) {
        trace_end_ = true;
        s = Status::Incomplete("Trace end.");
        break;
      }

      // In multi-threaded replay, sleep first then start decoding and
      // execution in a thread.
      std::chrono::system_clock::time_point sleep_to =
          replay_epoch +
          std::chrono::microseconds(static_cast<uint64_t>(std::llround(
              1.0 * (trace.ts - header_ts_) / options.fast_forward)));
      if (sleep_to > std::chrono::system_clock::now()) {
        std::this_thread::sleep_until(sleep_to);
      }

      if (trace_type == kTraceWrite || trace_type == kTraceGet ||
          trace_type == kTraceIteratorSeek ||
          trace_type == kTraceIteratorSeekForPrev ||
          trace_type == kTraceMultiGet) {
        std::unique_ptr<ReplayerWorkerArg> ra(new ReplayerWorkerArg);
        ra->trace_entry = std::move(trace);
        ra->handler = exec_handler_.get();
        ra->trace_file_version = trace_file_version_;
        ra->error_cb = error_cb;
        ra->result_cb = result_callback;
        thread_pool.Schedule(&ReplayerImpl::BackgroundWork, ra.release(),
                             nullptr, nullptr);
      } else {
        // Skip unsupported traces.
        if (result_callback != nullptr) {
          result_callback(Status::NotSupported("Unsupported trace type."),
                          nullptr);
        }
      }
    }

    thread_pool.WaitForJobsAndJoinAllThreads();
    if (!bg_s.ok()) {
      s = bg_s;
      record_failed = true;
    }
  }

  if (s.IsIncomplete() && !record_failed) {
    // Reaching eof returns Incomplete status at the moment.
    // Could happen when killing a process without calling EndTrace() API.
    // TODO: Add better error handling.
    trace_end_ = true;
    return Status::OK();
  }
  return s;
}

uint64_t ReplayerImpl::GetHeaderTimestamp() const { return header_ts_; }

Status ReplayerImpl::ReadHeader(Trace* header) {
  assert(header != nullptr);
  Status s = trace_reader_->Reset();
  if (!s.ok()) {
    return s;
  }
  std::string encoded_trace;
  // Read the trace head
  s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  return TracerHelper::DecodeHeader(encoded_trace, header);
}

Status ReplayerImpl::ReadTrace(Trace* trace) {
  assert(trace != nullptr);
  std::string encoded_trace;
  // We don't know if TraceReader is implemented thread-safe, so we protect the
  // reading trace part with a mutex. The decoding part does not need to be
  // protected since it's local.
  {
    std::lock_guard<std::mutex> guard(mutex_);
    Status s = trace_reader_->Read(&encoded_trace);
    if (!s.ok()) {
      return s;
    }
  }
  return TracerHelper::DecodeTrace(encoded_trace, trace);
}

void ReplayerImpl::BackgroundWork(void* arg) {
  std::unique_ptr<ReplayerWorkerArg> ra(static_cast<ReplayerWorkerArg*>(arg));
  assert(ra != nullptr);

  std::unique_ptr<TraceRecord> record;
  Status s = TracerHelper::DecodeTraceRecord(&(ra->trace_entry),
                                             ra->trace_file_version, &record);
  if (!s.ok()) {
    // Stop the replay, except for an unsupported trace type, which is skipped
    // as in single-threaded replay. Replay() schedules only supported types,
    // so this is a safeguard.
    if (!s.IsNotSupported() && ra->error_cb != nullptr) {
      ra->error_cb(s, ra->trace_entry.ts);
    }
    // Report the result
    if (ra->result_cb != nullptr) {
      ra->result_cb(s, nullptr);
    }
    return;
  }

  if (ra->result_cb == nullptr) {
    s = record->Accept(ra->handler, nullptr);
  } else {
    std::unique_ptr<TraceRecordResult> res;
    s = record->Accept(ra->handler, &res);
    ra->result_cb(s, std::move(res));
  }
  record.reset();
  // Stop the replay on any execution error, including NotSupported, as
  // single-threaded replay does.
  if (!s.ok() && ra->error_cb != nullptr) {
    ra->error_cb(s, ra->trace_entry.ts);
  }
}

}  // namespace ROCKSDB_NAMESPACE
