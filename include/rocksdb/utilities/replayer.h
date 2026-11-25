//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <functional>
#include <memory>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class TraceRecord;
class TraceRecordResult;

struct ReplayOptions {
  // Number of threads used for replaying. If 0 or 1, replay using
  // single thread.
  uint32_t num_threads;

  // Enables fast forwarding a replay by increasing/reducing the delay between
  // the ingested traces.
  //   If > 0.0 and < 1.0, slow down the replay by this amount.
  //   If 1.0, replay the operations at the same rate as in the trace stream.
  //   If > 1, speed up the replay by this amount.
  double fast_forward;

  ReplayOptions() : num_threads(1), fast_forward(1.0) {}

  ReplayOptions(uint32_t num_of_threads, double fast_forward_ratio)
      : num_threads(num_of_threads), fast_forward(fast_forward_ratio) {}
};

// Replayer helps to replay the captured RocksDB query level operations.
// The Replayer can either be created from DB::NewReplayer method, or be
// instantiated via db_bench today, on using "replay" benchmark.
class Replayer {
 public:
  virtual ~Replayer() = default;

  // Make some preparation before replaying the trace. This will also reset the
  // replayer in order to restart replaying.
  virtual Status Prepare() = 0;

  // Return the timestamp when the trace recording was started.
  virtual uint64_t GetHeaderTimestamp() const = 0;

  // Atomically read one trace into a TraceRecord (excluding the header and
  // footer traces).
  // Return Status::OK() on success;
  // Status::Incomplete() if Prepare() was not called or no more available
  // trace;
  // Status::NotSupported() if the read trace type is not supported.
  virtual Status Next(std::unique_ptr<TraceRecord>* record) = 0;

  // Execute one TraceRecord.
  // Return Status::OK() if the execution was successful. Get/MultiGet traces
  // will still return Status::OK() even if they got Status::NotFound()
  // from DB::Get() or DB::MultiGet();
  // Status::Incomplete() if Prepare() was not called or no more available
  // trace;
  // Status::NotSupported() if the operation is not supported;
  // Otherwise, return the corresponding error status.
  //
  // The actual operation execution status and result(s) will be saved in
  // result. For example, a GetQueryTraceRecord will have its DB::Get() status
  // and the returned value saved in a SingleValueTraceExecutionResult.
  virtual Status Execute(const std::unique_ptr<TraceRecord>& record,
                         std::unique_ptr<TraceRecordResult>* result) = 0;

  // Replay all the traces from the provided trace stream, taking the delay
  // between the traces into consideration.
  //
  // result_callback reports the status of executing a trace record, and the
  // actual operation execution result (See the description for Execute()).
  // With multiple threads, it is called concurrently and must be thread-safe.
  //
  // Replay stops at the first trace record that fails to decode or execute and
  // returns that status. Trace types the replayer does not support are
  // skipped, but a supported record whose execution returns
  // Status::NotSupported() stops the replay. With multiple threads, records
  // already scheduled still run, and the status returned is that of the failed
  // record with the earliest trace timestamp.
  virtual Status Replay(
      const ReplayOptions& options,
      const std::function<void(Status, std::unique_ptr<TraceRecordResult>&&)>&
          result_callback) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
