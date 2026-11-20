//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_record.h"

namespace ROCKSDB_NAMESPACE {

class IteratorTraceExecutionResult;
class MultiValuesTraceExecutionResult;
class SingleValueTraceExecutionResult;
class StatusOnlyTraceExecutionResult;

// Base class for the results of all types of trace records.
// Theses classes can be used to report the execution result of
// TraceRecord::Handler::Handle() or TraceRecord::Accept().
class TraceRecordResult {
 public:
  explicit TraceRecordResult(TraceType trace_type);

  virtual ~TraceRecordResult() = default;

  // Trace type of the corresponding TraceRecord.
  virtual TraceType GetTraceType() const;

  class Handler {
   public:
    virtual ~Handler() = default;

    virtual Status Handle(const StatusOnlyTraceExecutionResult& result) = 0;

    virtual Status Handle(const SingleValueTraceExecutionResult& result) = 0;

    virtual Status Handle(const MultiValuesTraceExecutionResult& result) = 0;

    virtual Status Handle(const IteratorTraceExecutionResult& result) = 0;
  };

  // Accept the handler.
  virtual Status Accept(Handler* handler) = 0;

 private:
  TraceType trace_type_;
};

// Base class for the results from the trace record execution handler (created
// by TraceRecord::NewExecutionHandler()).
//
// The actual execution status or returned values may be hidden from
// TraceRecord::Handler::Handle and TraceRecord::Accept. For example, a
// GetQueryTraceRecord's execution calls DB::Get() internally. DB::Get() may
// return Status::NotFound() but TraceRecord::Handler::Handle() or
// TraceRecord::Accept() will still return Status::OK(). The actual status from
// DB::Get() and the returned value string may be saved in a
// SingleValueTraceExecutionResult.
class TraceExecutionResult : public TraceRecordResult {
 public:
  TraceExecutionResult(uint64_t start_timestamp, uint64_t end_timestamp,
                       TraceType trace_type);

  // Execution start/end timestamps and request latency in microseconds.
  virtual uint64_t GetStartTimestamp() const;
  virtual uint64_t GetEndTimestamp() const;
  inline uint64_t GetLatency() const {
    return GetEndTimestamp() - GetStartTimestamp();
  }

 private:
  uint64_t ts_start_;
  uint64_t ts_end_;
};

// Result for operations that only return a single Status.
// Example operation: DB::Write()
class StatusOnlyTraceExecutionResult : public TraceExecutionResult {
 public:
  StatusOnlyTraceExecutionResult(Status status, uint64_t start_timestamp,
                                 uint64_t end_timestamp, TraceType trace_type);

  ~StatusOnlyTraceExecutionResult() override = default;

  // Return value of DB::Write(), etc.
  virtual const Status& GetStatus() const;

  Status Accept(Handler* handler) override;

 private:
  Status status_;
};

// Result for operations that return a Status and a value.
// Example operation: DB::Get()
class SingleValueTraceExecutionResult : public TraceExecutionResult {
 public:
  SingleValueTraceExecutionResult(Status status, const std::string& value,
                                  uint64_t start_timestamp,
                                  uint64_t end_timestamp, TraceType trace_type);

  SingleValueTraceExecutionResult(Status status, std::string&& value,
                                  uint64_t start_timestamp,
                                  uint64_t end_timestamp, TraceType trace_type);

  SingleValueTraceExecutionResult(const SingleValueTraceExecutionResult&) =
      default;
  SingleValueTraceExecutionResult& operator=(
      const SingleValueTraceExecutionResult&) = default;
  SingleValueTraceExecutionResult(SingleValueTraceExecutionResult&&) = default;
  SingleValueTraceExecutionResult& operator=(
      SingleValueTraceExecutionResult&&) = default;

  ~SingleValueTraceExecutionResult() override;

  // Return status of DB::Get().
  virtual const Status& GetStatus() const;

  // Value for the searched key.
  virtual const std::string& GetValue() const;

  Status Accept(Handler* handler) override;

 private:
  Status status_;
  std::string value_;
};

// Result for operations that return multiple Status(es) and values as vectors.
// Example operation: DB::MultiGet()
class MultiValuesTraceExecutionResult : public TraceExecutionResult {
 public:
  MultiValuesTraceExecutionResult(std::vector<Status> multi_status,
                                  std::vector<std::string> values,
                                  uint64_t start_timestamp,
                                  uint64_t end_timestamp, TraceType trace_type);

  MultiValuesTraceExecutionResult(const MultiValuesTraceExecutionResult&) =
      default;
  MultiValuesTraceExecutionResult& operator=(
      const MultiValuesTraceExecutionResult&) = default;
  MultiValuesTraceExecutionResult(MultiValuesTraceExecutionResult&&) = default;
  MultiValuesTraceExecutionResult& operator=(
      MultiValuesTraceExecutionResult&&) = default;

  ~MultiValuesTraceExecutionResult() override;

  // Returned Status(es) of DB::MultiGet().
  virtual const std::vector<Status>& GetMultiStatus() const;

  // Returned values for the searched keys.
  virtual const std::vector<std::string>& GetValues() const;

  Status Accept(Handler* handler) override;

 private:
  std::vector<Status> multi_status_;
  std::vector<std::string> values_;
};

// Result for Iterator operations.
// Example operations: Iterator::Seek(), Iterator::SeekForPrev()
class IteratorTraceExecutionResult : public TraceExecutionResult {
 public:
  IteratorTraceExecutionResult(bool valid, Status status, PinnableSlice&& key,
                               PinnableSlice&& value, uint64_t start_timestamp,
                               uint64_t end_timestamp, TraceType trace_type);

  IteratorTraceExecutionResult(bool valid, Status status,
                               const std::string& key, const std::string& value,
                               uint64_t start_timestamp, uint64_t end_timestamp,
                               TraceType trace_type);

  // No copying allowed
  IteratorTraceExecutionResult(const IteratorTraceExecutionResult&) = delete;
  IteratorTraceExecutionResult& operator=(const IteratorTraceExecutionResult&) =
      delete;
  IteratorTraceExecutionResult(IteratorTraceExecutionResult&&) = delete;
  IteratorTraceExecutionResult& operator=(IteratorTraceExecutionResult&&) =
      delete;

  ~IteratorTraceExecutionResult() override;

  // Return if the Iterator is valid.
  virtual bool GetValid() const;

  // Return the status of the Iterator.
  virtual const Status& GetStatus() const;

  // Key of the current iterating entry, empty if GetValid() is false.
  virtual Slice GetKey() const;

  // Value of the current iterating entry, empty if GetValid() is false.
  virtual Slice GetValue() const;

  Status Accept(Handler* handler) override;

 private:
  bool valid_;
  Status status_;
  PinnableSlice key_;
  PinnableSlice value_;
};

}  // namespace ROCKSDB_NAMESPACE
