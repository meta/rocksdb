//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db_stress_tool/db_stress_common.h"
#include "db_stress_tool/db_stress_shared_state.h"
#include "rocksdb/compaction_filter.h"

namespace ROCKSDB_NAMESPACE {

// DbStressCompactionFilter is safe to use with db_stress as it does not perform
// any mutation. It only makes `kRemove` decisions for keys that are already
// non-existent according to the `SharedState`.
// TODO: add follow-up stress coverage for wide-column blob entities through
// both FilterV3 compatibility and FilterV4 lazy-resolution paths, including
// blob direct write configurations. The current stress filter only exercises
// FilterV2 behavior.
class DbStressCompactionFilter : public CompactionFilter {
 public:
  DbStressCompactionFilter(SharedState* state, int cf_id)
      : state_(state),
        cf_id_(cf_id),
        merge_order_(FLAGS_merge_before_compaction_filter
                         ? MergeOrder::kMergeBeforeFilter
                         : MergeOrder::kFilterBeforeMerge) {}

  MergeOrder GetMergeOrder() const override { return merge_order_; }

  Decision FilterV2(int /*level*/, const Slice& key, ValueType /*value_type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    if (merge_order_ == MergeOrder::kMergeBeforeFilter || state_ == nullptr) {
      return Decision::kKeep;
    }
    if (key.empty() || ('0' <= key[0] && key[0] <= '9')) {
      // It is likely leftover from a test_batches_snapshots run. Below this
      // conditional, the test_batches_snapshots key format is not handled
      // properly. Just keep it to be safe.
      return Decision::kKeep;
    }
    uint64_t key_num = 0;
    {
      Slice ukey_without_ts = key;
      assert(ukey_without_ts.size() >= FLAGS_user_timestamp_size);
      ukey_without_ts.remove_suffix(FLAGS_user_timestamp_size);
      [[maybe_unused]] bool ok =
          GetIntVal(ukey_without_ts.ToString(), &key_num);
      assert(ok);
    }
    port::Mutex* key_mutex = state_->GetMutexForKey(cf_id_, key_num);
    if (!key_mutex->TryLock()) {
      return Decision::kKeep;
    }
    // Reaching here means we acquired the lock.
    key_mutex->AssertHeld();
    bool key_exists = state_->Exists(cf_id_, key_num);
    const bool allow_overwrite = state_->AllowsOverwrite(key_num);

    key_mutex->Unlock();

    if (!key_exists) {
      return allow_overwrite ? Decision::kRemove : Decision::kPurge;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "DbStressCompactionFilter"; }

 private:
  SharedState* const state_;
  const int cf_id_;
  const MergeOrder merge_order_;
};

class DbStressCompactionFilterFactory : public CompactionFilterFactory {
 public:
  DbStressCompactionFilterFactory()
      : state_(nullptr),
        merge_order_(FLAGS_merge_before_compaction_filter
                         ? CompactionFilter::MergeOrder::kMergeBeforeFilter
                         : CompactionFilter::MergeOrder::kFilterBeforeMerge) {}

  CompactionFilter::MergeOrder GetMergeOrder() const override {
    return merge_order_;
  }

  void SetSharedState(SharedState* state) {
    MutexLock state_mutex_guard(&state_mutex_);
    state_ = state;
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    MutexLock state_mutex_guard(&state_mutex_);
    return std::unique_ptr<CompactionFilter>(
        new DbStressCompactionFilter(state_, context.column_family_id));
  }

  const char* Name() const override {
    return "DbStressCompactionFilterFactory";
  }

 private:
  port::Mutex state_mutex_;
  SharedState* state_;
  const CompactionFilter::MergeOrder merge_order_;
};

}  // namespace ROCKSDB_NAMESPACE
