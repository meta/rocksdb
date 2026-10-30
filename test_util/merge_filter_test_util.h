// Copyright (c) Meta Platforms, Inc. and affiliates.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <string>
#include <vector>

#include "rocksdb/compaction_filter.h"
#include "rocksdb/merge_operator.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace test {

class MergeFilterTrace {
 public:
  void Add(const std::string& event) {
    MutexLock lock(&mutex_);
    events_.push_back(event);
  }

  std::vector<std::string> Events() const {
    MutexLock lock(&mutex_);
    return events_;
  }

  void Clear() {
    MutexLock lock(&mutex_);
    events_.clear();
  }

 private:
  mutable port::Mutex mutex_;
  std::vector<std::string> events_;
};

class RecordingMergeFilter : public CompactionFilter {
 public:
  RecordingMergeFilter(MergeOrder order, MergeFilterTrace* trace)
      : order_(order), trace_(trace) {}

  MergeOrder GetMergeOrder() const override { return order_; }
  const char* Name() const override { return "RecordingMergeFilter"; }

  Decision FilterV4(
      int /*level*/, const Slice& key, ValueType type, const Slice* value,
      const WideColumns* columns, std::string* new_value,
      std::vector<std::pair<std::string, std::string>>* new_columns,
      std::string* skip_until,
      WideColumnBlobResolver* resolver) const override {
    std::string payload = value ? value->ToString() : "";
    std::string kind = "value";
    if (type == ValueType::kMergeOperand) {
      kind = "operand";
    } else if (type == ValueType::kWideColumnEntity) {
      kind = resolver ? "lazy_entity" : "entity";
      for (const auto& column : *columns) {
        if (column.name().empty()) {
          payload = column.value().ToString();
        }
      }
    }
    trace_->Add(kind + ":" + key.ToString() + ":" + payload);
    if (!action_key.empty() && key != action_key) {
      return Decision::kKeep;
    }
    Decision decision =
        type == ValueType::kMergeOperand ? operand_decision : value_decision;
    if (decision == Decision::kChangeValue) {
      *new_value = replacement;
    }
    if (decision == Decision::kChangeWideColumnEntity && new_columns) {
      *new_columns = replacement_columns;
    }
    *skip_until = skip_target;
    return decision;
  }

  bool SupportsFilterV4() const override { return true; }

  Decision FilterBlobByKey(int /*level*/, const Slice& key,
                           std::string* /*new_value*/,
                           std::string* /*skip_until*/) const override {
    blob_key_calls.fetch_add(1);
    return action_key.empty() || key == action_key ? blob_decision
                                                   : Decision::kUndetermined;
  }

  // Configure actions only between completed jobs; the order is immutable.
  Decision value_decision = Decision::kKeep;
  Decision operand_decision = Decision::kKeep;
  Decision blob_decision = Decision::kUndetermined;
  std::string action_key;
  std::string replacement;
  std::string skip_target;
  std::vector<std::pair<std::string, std::string>> replacement_columns;
  mutable std::atomic<uint64_t> blob_key_calls{0};

 private:
  const MergeOrder order_;
  MergeFilterTrace* const trace_;
};

class RecordingMergeOperator : public MergeOperator {
 public:
  explicit RecordingMergeOperator(MergeFilterTrace* trace) : trace_(trace) {}
  const char* Name() const override { return "RecordingMergeOperator"; }

  bool FullMergeV2(const MergeOperationInput& input,
                   MergeOperationOutput* output) const override {
    std::string result =
        input.existing_value ? input.existing_value->ToString() : "";
    for (const Slice& operand : input.operand_list) {
      result += operand.ToString();
    }
    trace_->Add("full:" + input.key.ToString() + ":" + result);
    if (fail_full) {
      output->op_failure_scope = failure_scope;
      return false;
    }
    output->new_value = result;
    return true;
  }

  bool PartialMergeMulti(const Slice& key, const std::deque<Slice>& operands,
                         std::string* result,
                         Logger* /*logger*/) const override {
    std::string combined;
    for (const Slice& operand : operands) {
      combined += operand.ToString();
    }
    trace_->Add("partial:" + key.ToString() + ":" + combined);
    if (!partial_success) {
      return false;
    }
    *result = combined;
    return true;
  }

  bool AllowSingleOperand() const override { return allow_single; }

  bool partial_success = true;
  bool allow_single = false;
  bool fail_full = false;
  OpFailureScope failure_scope = OpFailureScope::kTryMerge;

 private:
  MergeFilterTrace* const trace_;
};

class RecordingMergeFilterFactory : public CompactionFilterFactory {
 public:
  RecordingMergeFilterFactory(CompactionFilter::MergeOrder order,
                              MergeFilterTrace* trace)
      : instance_order(order), order_(order), trace_(trace) {}

  const char* Name() const override { return "RecordingMergeFilterFactory"; }
  CompactionFilter::MergeOrder GetMergeOrder() const override { return order_; }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == TableFileCreationReason::kCompaction ||
           (filter_other_reason && reason == other_reason);
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    creations.fetch_add(1);
    if (return_null) {
      return nullptr;
    }
    auto filter =
        std::make_unique<RecordingMergeFilter>(instance_order, trace_);
    filter->operand_decision = operand_decision;
    filter->value_decision = value_decision;
    return filter;
  }

  CompactionFilter::MergeOrder instance_order;
  bool filter_other_reason = false;
  TableFileCreationReason other_reason = TableFileCreationReason::kFlush;
  bool return_null = false;
  CompactionFilter::Decision operand_decision =
      CompactionFilter::Decision::kKeep;
  CompactionFilter::Decision value_decision = CompactionFilter::Decision::kKeep;
  std::atomic<uint64_t> creations{0};

 private:
  const CompactionFilter::MergeOrder order_;
  MergeFilterTrace* const trace_;
};

}  // namespace test
}  // namespace ROCKSDB_NAMESPACE
