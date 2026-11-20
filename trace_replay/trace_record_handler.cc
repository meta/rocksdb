//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/trace_record_handler.h"

#include <limits>
#include <memory>

#include "rocksdb/iterator.h"
#include "rocksdb/trace_record_result.h"
#include "rocksdb/write_batch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// The options recorded in the trace can make a replayed call fail the way the
// live call could. That status is the call's result, reported like NotFound,
// not a replay error. Other failures, such as a column family or an option the
// replay DB lacks, still stop the replay.
bool IsOutcomeOf(const WriteOptions* recorded, const Status& s) {
  return recorded != nullptr &&
         ((s.IsIncomplete() && recorded->no_slowdown) ||
          // WriteImpl() rejects this combination after tracing the write.
          (s.IsInvalidArgument() && recorded->sync && recorded->disableWAL));
}

bool IsOutcomeOf(const ReadOptions* recorded, const Status& s) {
  return recorded != nullptr &&
         ((s.IsIncomplete() && recorded->read_tier != kReadAllTier) ||
          (s.IsAborted() && recorded->value_size_soft_limit !=
                                std::numeric_limits<uint64_t>::max()) ||
          (s.IsNotSupported() && recorded->read_tier == kPersistedTier));
}

// An iterator also stops with Incomplete after max_skippable_internal_keys,
// and a tailing iterator fails SeekForPrev() and seeks over range tombstones
// with NotSupported.
bool IsIteratorOutcomeOf(const ReadOptions* recorded, const Status& s) {
  return IsOutcomeOf(recorded, s) ||
         (recorded != nullptr &&
          ((s.IsIncomplete() && recorded->max_skippable_internal_keys > 0) ||
           (s.IsNotSupported() && recorded->tailing)));
}

// A DB that cannot create tailing iterators, such as a secondary, fails every
// tailing seek with NotSupported. That is a replay error.
bool RefusesTailingIterators(DB* db, ColumnFamilyHandle* cf) {
  ReadOptions probe;
  probe.tailing = true;
  // Otherwise range tombstones also fail the new iterator.
  probe.ignore_range_deletions = true;
  std::unique_ptr<Iterator> iter(db->NewIterator(probe, cf));
  return iter->status().IsNotSupported();
}

}  // namespace

// TraceExecutionHandler
TraceExecutionHandler::TraceExecutionHandler(
    DB* db, const std::vector<ColumnFamilyHandle*>& handles)
    : TraceRecord::Handler(),
      db_(db),
      write_opts_(WriteOptions()),
      read_opts_(ReadOptions()) {
  assert(db != nullptr);
  cf_map_.reserve(handles.size() + 1);
  for (ColumnFamilyHandle* handle : handles) {
    assert(handle != nullptr);
    cf_map_.insert({handle->GetID(), handle});
  }
  // Every DB has a default column family, so records on it can be replayed
  // even when the caller did not pass its handle.
  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
  if (default_cf != nullptr) {
    cf_map_.insert({default_cf->GetID(), default_cf});
  }
  clock_ = db_->GetEnv()->GetSystemClock().get();
}

TraceExecutionHandler::~TraceExecutionHandler() { cf_map_.clear(); }

Status TraceExecutionHandler::Handle(
    const WriteQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  uint64_t start = clock_->NowMicros();

  WriteBatch batch(record.GetWriteBatchRep().ToString());
  const WriteOptions* recorded = record.GetWriteOptions();
  Status s = db_->Write(recorded != nullptr ? *recorded : write_opts_, &batch);

  uint64_t end = clock_->NowMicros();

  const bool outcome = IsOutcomeOf(recorded, s);
  if ((s.ok() || outcome) && result != nullptr) {
    result->reset(new StatusOnlyTraceExecutionResult(s, start, end,
                                                     record.GetTraceType()));
  }

  return outcome ? Status::OK() : s;
}

Status TraceExecutionHandler::Handle(
    const GetQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  auto it = cf_map_.find(record.GetColumnFamilyID());
  if (it == cf_map_.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }

  uint64_t start = clock_->NowMicros();

  std::string value;
  const ReadOptions* recorded = record.GetReadOptions();
  Status s = db_->Get(recorded != nullptr ? *recorded : read_opts_, it->second,
                      record.GetKey(), &value);

  uint64_t end = clock_->NowMicros();

  // Treat not found as ok, return other errors.
  if (!s.ok() && !s.IsNotFound() && !IsOutcomeOf(recorded, s)) {
    return s;
  }

  if (result != nullptr) {
    // Report the actual opetation status in TraceExecutionResult
    result->reset(new SingleValueTraceExecutionResult(
        std::move(s), std::move(value), start, end, record.GetTraceType()));
  }
  return Status::OK();
}

Status TraceExecutionHandler::Handle(
    const IteratorSeekQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  auto it = cf_map_.find(record.GetColumnFamilyID());
  if (it == cf_map_.end()) {
    return Status::Corruption("Invalid Column Family ID.");
  }

  const ReadOptions* recorded = record.GetReadOptions();
  ReadOptions r_opts = recorded != nullptr ? *recorded : read_opts_;
  Slice lower = record.GetLowerBound();
  if (!lower.empty()) {
    r_opts.iterate_lower_bound = &lower;
  }
  Slice upper = record.GetUpperBound();
  if (!upper.empty()) {
    r_opts.iterate_upper_bound = &upper;
  }
  Iterator* single_iter = db_->NewIterator(r_opts, it->second);

  uint64_t start = clock_->NowMicros();

  switch (record.GetSeekType()) {
    case IteratorSeekQueryTraceRecord::kSeekForPrev: {
      single_iter->SeekForPrev(record.GetKey());
      break;
    }
    case IteratorSeekQueryTraceRecord::kSeek:
    default: {
      single_iter->Seek(record.GetKey());
      break;
    }
  }

  uint64_t end = clock_->NowMicros();

  // A captured result holds the value, so with a recorded
  // allow_unprepared_value it is loaded first; a load error becomes the seek's
  // status. Without a result the value stays unprepared, as in the live call.
  if (result != nullptr && single_iter->Valid()) {
    single_iter->PrepareValue();
  }
  Status s = single_iter->status();
  if (IsIteratorOutcomeOf(recorded, s) &&
      !(s.IsNotSupported() && recorded->tailing &&
        RefusesTailingIterators(db_, it->second))) {
    if (result != nullptr) {
      result->reset(new IteratorTraceExecutionResult(
          false, s, "", "", start, end, record.GetTraceType()));
    }
    s = Status::OK();
  } else if (s.ok() && result != nullptr) {
    if (single_iter->Valid()) {
      PinnableSlice ps_key;
      ps_key.PinSelf(single_iter->key());
      PinnableSlice ps_value;
      ps_value.PinSelf(single_iter->value());
      result->reset(new IteratorTraceExecutionResult(
          true, s, std::move(ps_key), std::move(ps_value), start, end,
          record.GetTraceType()));
    } else {
      result->reset(new IteratorTraceExecutionResult(
          false, s, "", "", start, end, record.GetTraceType()));
    }
  }
  delete single_iter;

  return s;
}

Status TraceExecutionHandler::Handle(
    const MultiGetQueryTraceRecord& record,
    std::unique_ptr<TraceRecordResult>* result) {
  if (result != nullptr) {
    result->reset(nullptr);
  }
  std::vector<ColumnFamilyHandle*> handles;
  handles.reserve(record.GetColumnFamilyIDs().size());
  for (uint32_t cf_id : record.GetColumnFamilyIDs()) {
    auto it = cf_map_.find(cf_id);
    if (it == cf_map_.end()) {
      return Status::Corruption("Invalid Column Family ID.");
    }
    handles.push_back(it->second);
  }

  std::vector<Slice> keys = record.GetKeys();

  if (handles.empty() || keys.empty()) {
    return Status::InvalidArgument("Empty MultiGet cf_ids or keys.");
  }
  if (handles.size() != keys.size()) {
    return Status::InvalidArgument("MultiGet cf_ids and keys size mismatch.");
  }

  uint64_t start = clock_->NowMicros();

  std::vector<std::string> values;
  const ReadOptions* recorded = record.GetReadOptions();
  std::vector<Status> ss = db_->MultiGet(
      recorded != nullptr ? *recorded : read_opts_, handles, keys, &values);

  uint64_t end = clock_->NowMicros();

  // Treat not found as ok, return other errors.
  for (const Status& s : ss) {
    if (!s.ok() && !s.IsNotFound() && !IsOutcomeOf(recorded, s)) {
      return s;
    }
  }

  if (result != nullptr) {
    // Report the actual opetation status in TraceExecutionResult
    result->reset(new MultiValuesTraceExecutionResult(
        std::move(ss), std::move(values), start, end, record.GetTraceType()));
  }

  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
