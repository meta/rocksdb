//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/trace_record.h"

#include <utility>

#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_record_result.h"
#include "trace_replay/trace_record_handler.h"

namespace ROCKSDB_NAMESPACE {

// TraceRecord
TraceRecord::TraceRecord(uint64_t timestamp) : timestamp_(timestamp) {}

uint64_t TraceRecord::GetTimestamp() const { return timestamp_; }

TraceRecord::Handler* TraceRecord::NewExecutionHandler(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles) {
  return new TraceExecutionHandler(db, handles);
}

// QueryTraceRecord
QueryTraceRecord::QueryTraceRecord(uint64_t timestamp)
    : TraceRecord(timestamp) {}

// WriteQueryTraceRecord
WriteQueryTraceRecord::WriteQueryTraceRecord(PinnableSlice&& write_batch_rep,
                                             uint64_t timestamp)
    : QueryTraceRecord(timestamp), rep_(std::move(write_batch_rep)) {}

WriteQueryTraceRecord::WriteQueryTraceRecord(const std::string& write_batch_rep,
                                             uint64_t timestamp)
    : QueryTraceRecord(timestamp) {
  rep_.PinSelf(write_batch_rep);
}

WriteQueryTraceRecord::~WriteQueryTraceRecord() { rep_.clear(); }

Slice WriteQueryTraceRecord::GetWriteBatchRep() const { return Slice(rep_); }

const WriteOptions* WriteQueryTraceRecord::GetWriteOptions() const {
  return write_options_.get();
}

void WriteQueryTraceRecord::SetWriteOptions(const WriteOptions& write_options) {
  write_options_ = std::make_unique<WriteOptions>(write_options);
}

Status WriteQueryTraceRecord::Accept(
    Handler* handler, std::unique_ptr<TraceRecordResult>* result) {
  assert(handler != nullptr);
  return handler->Handle(*this, result);
}

// GetQueryTraceRecord
GetQueryTraceRecord::GetQueryTraceRecord(uint32_t column_family_id,
                                         PinnableSlice&& key,
                                         uint64_t timestamp)
    : QueryTraceRecord(timestamp),
      cf_id_(column_family_id),
      key_(std::move(key)) {}

GetQueryTraceRecord::GetQueryTraceRecord(uint32_t column_family_id,
                                         const std::string& key,
                                         uint64_t timestamp)
    : QueryTraceRecord(timestamp), cf_id_(column_family_id) {
  key_.PinSelf(key);
}

GetQueryTraceRecord::~GetQueryTraceRecord() { key_.clear(); }

uint32_t GetQueryTraceRecord::GetColumnFamilyID() const { return cf_id_; }

Slice GetQueryTraceRecord::GetKey() const { return Slice(key_); }

const ReadOptions* GetQueryTraceRecord::GetReadOptions() const {
  return read_options_.get();
}

void GetQueryTraceRecord::SetReadOptions(const ReadOptions& read_options) {
  read_options_ = std::make_unique<ReadOptions>(read_options);
}

Status GetQueryTraceRecord::Accept(Handler* handler,
                                   std::unique_ptr<TraceRecordResult>* result) {
  assert(handler != nullptr);
  return handler->Handle(*this, result);
}

// IteratorQueryTraceRecord
IteratorQueryTraceRecord::IteratorQueryTraceRecord(uint64_t timestamp)
    : QueryTraceRecord(timestamp) {}

IteratorQueryTraceRecord::IteratorQueryTraceRecord(PinnableSlice&& lower_bound,
                                                   PinnableSlice&& upper_bound,
                                                   uint64_t timestamp)
    : QueryTraceRecord(timestamp),
      lower_(std::move(lower_bound)),
      upper_(std::move(upper_bound)) {}

IteratorQueryTraceRecord::IteratorQueryTraceRecord(
    const std::string& lower_bound, const std::string& upper_bound,
    uint64_t timestamp)
    : QueryTraceRecord(timestamp) {
  lower_.PinSelf(lower_bound);
  upper_.PinSelf(upper_bound);
}

IteratorQueryTraceRecord::~IteratorQueryTraceRecord() = default;

Slice IteratorQueryTraceRecord::GetLowerBound() const { return Slice(lower_); }

Slice IteratorQueryTraceRecord::GetUpperBound() const { return Slice(upper_); }

const ReadOptions* IteratorQueryTraceRecord::GetReadOptions() const {
  return read_options_.get();
}

void IteratorQueryTraceRecord::SetReadOptions(const ReadOptions& read_options) {
  read_options_ = std::make_unique<ReadOptions>(read_options);
}

// IteratorSeekQueryTraceRecord
IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType seek_type, uint32_t column_family_id, PinnableSlice&& key,
    uint64_t timestamp)
    : IteratorQueryTraceRecord(timestamp),
      type_(seek_type),
      cf_id_(column_family_id),
      key_(std::move(key)) {}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType seek_type, uint32_t column_family_id, const std::string& key,
    uint64_t timestamp)
    : IteratorQueryTraceRecord(timestamp),
      type_(seek_type),
      cf_id_(column_family_id) {
  key_.PinSelf(key);
}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType seek_type, uint32_t column_family_id, PinnableSlice&& key,
    PinnableSlice&& lower_bound, PinnableSlice&& upper_bound,
    uint64_t timestamp)
    : IteratorQueryTraceRecord(std::move(lower_bound), std::move(upper_bound),
                               timestamp),
      type_(seek_type),
      cf_id_(column_family_id),
      key_(std::move(key)) {}

IteratorSeekQueryTraceRecord::IteratorSeekQueryTraceRecord(
    SeekType seek_type, uint32_t column_family_id, const std::string& key,
    const std::string& lower_bound, const std::string& upper_bound,
    uint64_t timestamp)
    : IteratorQueryTraceRecord(lower_bound, upper_bound, timestamp),
      type_(seek_type),
      cf_id_(column_family_id) {
  key_.PinSelf(key);
}

IteratorSeekQueryTraceRecord::~IteratorSeekQueryTraceRecord() { key_.clear(); }

TraceType IteratorSeekQueryTraceRecord::GetTraceType() const {
  return static_cast<TraceType>(type_);
}

IteratorSeekQueryTraceRecord::SeekType
IteratorSeekQueryTraceRecord::GetSeekType() const {
  return type_;
}

uint32_t IteratorSeekQueryTraceRecord::GetColumnFamilyID() const {
  return cf_id_;
}

Slice IteratorSeekQueryTraceRecord::GetKey() const { return Slice(key_); }

Status IteratorSeekQueryTraceRecord::Accept(
    Handler* handler, std::unique_ptr<TraceRecordResult>* result) {
  assert(handler != nullptr);
  return handler->Handle(*this, result);
}

// MultiGetQueryTraceRecord
MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t> column_family_ids, std::vector<PinnableSlice>&& keys,
    uint64_t timestamp)
    : QueryTraceRecord(timestamp),
      cf_ids_(std::move(column_family_ids)),
      keys_(std::move(keys)) {}

MultiGetQueryTraceRecord::MultiGetQueryTraceRecord(
    std::vector<uint32_t> column_family_ids,
    const std::vector<std::string>& keys, uint64_t timestamp)
    : QueryTraceRecord(timestamp), cf_ids_(std::move(column_family_ids)) {
  keys_.reserve(keys.size());
  for (const std::string& key : keys) {
    PinnableSlice ps;
    ps.PinSelf(key);
    keys_.push_back(std::move(ps));
  }
}

MultiGetQueryTraceRecord::~MultiGetQueryTraceRecord() {
  cf_ids_.clear();
  keys_.clear();
}

std::vector<uint32_t> MultiGetQueryTraceRecord::GetColumnFamilyIDs() const {
  return cf_ids_;
}

std::vector<Slice> MultiGetQueryTraceRecord::GetKeys() const {
  return std::vector<Slice>(keys_.begin(), keys_.end());
}

const ReadOptions* MultiGetQueryTraceRecord::GetReadOptions() const {
  return read_options_.get();
}

void MultiGetQueryTraceRecord::SetReadOptions(const ReadOptions& read_options) {
  read_options_ = std::make_unique<ReadOptions>(read_options);
}

Status MultiGetQueryTraceRecord::Accept(
    Handler* handler, std::unique_ptr<TraceRecordResult>* result) {
  assert(handler != nullptr);
  return handler->Handle(*this, result);
}

}  // namespace ROCKSDB_NAMESPACE
