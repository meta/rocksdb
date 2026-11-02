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

// Counts merge-before-filter decisions by value type, so a run's log shows
// which paths were exercised.
struct DbStressMergeFilterCounters {
  enum Kind { kKeep, kChange, kRemove, kSkip, kNumKinds };
  static constexpr const char* kKindNames[kNumKinds] = {"keep", "change",
                                                        "remove", "skip"};
  // Index 0 is kValue (including full merge results), 1 is kMergeOperand
  // (partial merge results and uncombined operands).
  std::atomic<uint64_t> counts[2][kNumKinds] = {};

  void Record(CompactionFilter::ValueType value_type, Kind kind) {
    const int type =
        value_type == CompactionFilter::ValueType::kMergeOperand ? 1 : 0;
    counts[type][kind].fetch_add(1, std::memory_order_relaxed);
  }

  void Print() const {
    for (int type = 0; type < 2; ++type) {
      fprintf(stdout, "Merge-before-filter %s decisions:",
              type == 0 ? "value" : "operand");
      for (int kind = 0; kind < kNumKinds; ++kind) {
        fprintf(stdout, " %s=%" PRIu64, kKindNames[kind],
                counts[type][kind].load(std::memory_order_relaxed));
      }
      fprintf(stdout, "\n");
    }
  }
};

inline DbStressMergeFilterCounters& GetDbStressMergeFilterCounters() {
  static DbStressMergeFilterCounters counters;
  return counters;
}

// DbStressCompactionFilter is safe to use with db_stress as it does not perform
// any mutation. It only makes `kRemove` decisions for keys that are already
// non-existent according to the `SharedState`.
//
// With merge-before-filter, every decision leaves the expected state valid:
// `kChangeValue` rewrites the same bytes, and `kRemove` /
// `kRemoveAndSkipUntil` only apply to deleted keys when
// merge_before_filter_remove_deleted is set (which requires no snapshots).
// Skips cover only the current key and only in full compactions, so no older
// version outside the compaction can reappear. The filter also checks that
// every value it sees is a well-formed stress value for its key.
// TODO: add follow-up stress coverage for wide-column blob entities through
// both FilterV3 compatibility and FilterV4 lazy-resolution paths, including
// blob direct write configurations. The current stress filter only exercises
// FilterV2 behavior.
class DbStressCompactionFilter : public CompactionFilter {
 public:
  DbStressCompactionFilter(SharedState* state, int cf_id,
                           bool is_full_compaction)
      : state_(state),
        cf_id_(cf_id),
        is_full_compaction_(is_full_compaction),
        merge_order_(FLAGS_merge_before_compaction_filter
                         ? MergeOrder::kMergeBeforeFilter
                         : MergeOrder::kFilterBeforeMerge) {}

  MergeOrder GetMergeOrder() const override { return merge_order_; }

  Decision FilterV2(int /*level*/, const Slice& key, ValueType value_type,
                    const Slice& existing_value, std::string* new_value,
                    std::string* skip_until) const override {
    if (merge_order_ == MergeOrder::kMergeBeforeFilter) {
      return FilterMergeBeforeFilter(key, value_type, existing_value, new_value,
                                     skip_until);
    }
    if (state_ == nullptr) {
      return Decision::kKeep;
    }
    uint64_t key_num = 0;
    if (!ParseKey(key, &key_num)) {
      return Decision::kKeep;
    }
    bool key_exists = false;
    bool allow_overwrite = false;
    if (!CheckKey(key_num, &key_exists, &allow_overwrite)) {
      return Decision::kKeep;
    }
    if (!key_exists) {
      return allow_overwrite ? Decision::kRemove : Decision::kPurge;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "DbStressCompactionFilter"; }

 private:
  static bool ParseKey(const Slice& key, uint64_t* key_num) {
    if (key.empty() || ('0' <= key[0] && key[0] <= '9')) {
      // It is likely leftover from a test_batches_snapshots run. Below this
      // conditional, the test_batches_snapshots key format is not handled
      // properly. Just keep it to be safe.
      return false;
    }
    Slice ukey_without_ts = key;
    assert(ukey_without_ts.size() >= FLAGS_user_timestamp_size);
    ukey_without_ts.remove_suffix(FLAGS_user_timestamp_size);
    [[maybe_unused]] bool ok = GetIntVal(ukey_without_ts.ToString(), key_num);
    assert(ok);
    return true;
  }

  // Returns false if the key lock is busy, in which case the caller keeps the
  // record.
  bool CheckKey(uint64_t key_num, bool* key_exists,
                bool* allow_overwrite) const {
    port::Mutex* key_mutex = state_->GetMutexForKey(cf_id_, key_num);
    if (!key_mutex->TryLock()) {
      return false;
    }
    // Reaching here means we acquired the lock.
    key_mutex->AssertHeld();
    *key_exists = state_->Exists(cf_id_, key_num);
    *allow_overwrite = state_->AllowsOverwrite(key_num);
    key_mutex->Unlock();
    return true;
  }

  // Values are GenerateValue() output: a 4-byte value base followed by bytes
  // derived from it. Merge results and partial merge results keep this format
  // because the stress merge operators return the newest operand.
  static bool IsWellFormedValue(const Slice& value) {
    if (value.empty()) {
      // test_ingest_standalone_range_deletion ingests empty values that a
      // range deletion in the same ingestion always covers.
      return true;
    }
    if (value.size() < sizeof(uint32_t)) {
      return false;
    }
    const uint32_t value_base = GetValueBase(value);
    const size_t expected_size =
        ((value_base % kRandomValueMaxFactor) + 1) * FLAGS_value_size_mult;
    if (value.size() != expected_size) {
      return false;
    }
    for (size_t i = sizeof(uint32_t); i < value.size(); ++i) {
      if (value[i] != static_cast<char>(value_base ^ i)) {
        return false;
      }
    }
    return true;
  }

  Decision FilterMergeBeforeFilter(const Slice& key, ValueType value_type,
                                   const Slice& existing_value,
                                   std::string* new_value,
                                   std::string* skip_until) const {
    DbStressMergeFilterCounters& counters = GetDbStressMergeFilterCounters();
    uint64_t key_num = 0;
    if (!ParseKey(key, &key_num)) {
      counters.Record(value_type, DbStressMergeFilterCounters::kKeep);
      return Decision::kKeep;
    }
    if (!IsWellFormedValue(existing_value)) {
      fprintf(stderr, "Merge-before-filter got a malformed %s for key %s: %s\n",
              value_type == ValueType::kMergeOperand ? "operand" : "value",
              key.ToString(true).c_str(),
              existing_value.ToString(true).c_str());
      std::abort();
    }
    if (FLAGS_merge_before_filter_remove_deleted && state_ != nullptr) {
      bool key_exists = false;
      bool allow_overwrite = false;
      if (CheckKey(key_num, &key_exists, &allow_overwrite) && !key_exists) {
        if (is_full_compaction_) {
          // Every version of this key is in the compaction, so dropping all of
          // them leaves the key deleted.
          *skip_until = key.ToString();
          skip_until->push_back('\0');
          counters.Record(value_type, DbStressMergeFilterCounters::kSkip);
          return Decision::kRemoveAndSkipUntil;
        }
        counters.Record(value_type, DbStressMergeFilterCounters::kRemove);
        return Decision::kRemove;
      }
    }
    if (FLAGS_merge_before_filter_change_one_in > 0 &&
        !existing_value.empty() &&
        (GetValueBase(existing_value) + key_num) %
                FLAGS_merge_before_filter_change_one_in ==
            0) {
      new_value->assign(existing_value.data(), existing_value.size());
      counters.Record(value_type, DbStressMergeFilterCounters::kChange);
      return Decision::kChangeValue;
    }
    counters.Record(value_type, DbStressMergeFilterCounters::kKeep);
    return Decision::kKeep;
  }

  SharedState* const state_;
  const int cf_id_;
  const bool is_full_compaction_;
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
    if (merge_order_ == CompactionFilter::MergeOrder::kMergeBeforeFilter &&
        context.reason != TableFileCreationReason::kCompaction) {
      fprintf(stderr,
              "Merge-before-filter factory called for a non-compaction "
              "table file creation\n");
      std::abort();
    }
    return std::unique_ptr<CompactionFilter>(new DbStressCompactionFilter(
        state_, context.column_family_id, context.is_full_compaction));
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
