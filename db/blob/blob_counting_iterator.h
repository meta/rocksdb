//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <optional>

#include "db/blob/blob_garbage_meter.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "table/internal_iterator.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

// An internal iterator that passes each key-value encountered to
// BlobGarbageMeter as inflow in order to measure the total number and size of
// blobs in the compaction input on a per-blob file basis.
class BlobCountingIterator : public InternalIterator {
 public:
  BlobCountingIterator(InternalIterator* iter,
                       BlobGarbageMeter* blob_garbage_meter,
                       bool track_current_key_inflow)
      : iter_(iter), blob_garbage_meter_(blob_garbage_meter) {
    assert(iter_);
    assert(blob_garbage_meter_);
    if (track_current_key_inflow) {
      current_key_inflow_.emplace();
    }

    UpdateAndCountBlobIfNeeded();
  }

  bool Valid() const override { return iter_->Valid() && status_.ok(); }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    UpdateAndCountBlobIfNeeded();
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    UpdateAndCountBlobIfNeeded();
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    UpdateAndCountBlobIfNeeded();
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    UpdateAndCountBlobIfNeeded();
  }

  void Next() override {
    assert(Valid());

    iter_->Next();
    UpdateAndCountBlobIfNeeded();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(Valid());

    const bool res = iter_->NextAndGetResult(result);
    UpdateAndCountBlobIfNeeded();
    return res;
  }

  void Prev() override {
    assert(Valid());

    iter_->Prev();
    UpdateAndCountBlobIfNeeded();
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice user_key() const override {
    assert(Valid());
    return iter_->user_key();
  }

  Slice value() const override {
    assert(Valid());
    return iter_->value();
  }

  Status status() const override { return status_; }

  bool PrepareValue() override {
    assert(Valid());
    return iter_->PrepareValue();
  }

  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return iter_->MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return iter_->UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  bool IsKeyPinned() const override {
    assert(Valid());
    return iter_->IsKeyPinned();
  }

  bool IsValuePinned() const override {
    assert(Valid());
    return iter_->IsValuePinned();
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(prop_name, prop);
  }

  bool IsDeleteRangeSentinelKey() const override {
    return iter_->IsDeleteRangeSentinelKey();
  }

  const BlobGarbageMeter& GetCurrentKeyInflow() const {
    assert(current_key_inflow_.has_value());
    return *current_key_inflow_;
  }

  bool TracksCurrentKeyInflow() const {
    return current_key_inflow_.has_value();
  }

 private:
  void UpdateAndCountBlobIfNeeded() {
    assert(!iter_->Valid() || iter_->status().ok());

    if (!iter_->Valid()) {
      status_ = iter_->status();
      if (current_key_inflow_.has_value()) {
        current_key_inflow_->Clear();
      }
      return;
    }

    TEST_SYNC_POINT(
        "BlobCountingIterator::UpdateAndCountBlobIfNeeded:ProcessInFlow");

    if (current_key_inflow_.has_value()) {
      current_key_inflow_->Clear();
      TEST_SYNC_POINT(
          "BlobCountingIterator::UpdateAndCountBlobIfNeeded:CurrentKeyInflow");
      status_ = current_key_inflow_->ProcessInFlow(key(), value());
      if (!status_.ok()) {
        return;
      }
    }
    TEST_SYNC_POINT(
        "BlobCountingIterator::UpdateAndCountBlobIfNeeded:AggregateInflow");
    status_ = blob_garbage_meter_->ProcessInFlow(key(), value());
  }

  InternalIterator* iter_;
  BlobGarbageMeter* blob_garbage_meter_;
  std::optional<BlobGarbageMeter> current_key_inflow_;
  Status status_;
};

}  // namespace ROCKSDB_NAMESPACE
