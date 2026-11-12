//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "merge_operator.h"

#include <cassert>
#include <memory>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_type.h"
#include "utilities/cassandra/format.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE::cassandra {
static std::unordered_map<std::string, OptionTypeInfo>
    merge_operator_options_info = {
        {"gc_grace_period_in_seconds",
         {offsetof(struct CassandraOptions, gc_grace_period_in_seconds),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"operands_limit",
         {offsetof(struct CassandraOptions, operands_limit), OptionType::kSizeT,
          OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
};

CassandraValueMergeOperator::CassandraValueMergeOperator(
    int32_t gc_grace_period_in_seconds, size_t operands_limit)
    : options_(gc_grace_period_in_seconds, operands_limit) {
  RegisterOptions(&options_, &merge_operator_options_info);
}

// Implementation for the merge operation (merges two Cassandra values)
bool CassandraValueMergeOperator::FullMergeV2(
    const MergeOperationInput& merge_in,
    MergeOperationOutput* merge_out) const {
  // Clear the *new_value for writing.
  merge_out->new_value.clear();
  std::vector<RowValue> row_values;
  if (merge_in.existing_value) {
    row_values.push_back(RowValue::Deserialize(
        merge_in.existing_value->data(), merge_in.existing_value->size()));
  }

  for (auto& operand : merge_in.operand_list) {
    row_values.push_back(RowValue::Deserialize(operand.data(), operand.size()));
  }

  RowValue merged = RowValue::Merge(std::move(row_values));
  merged = merged.RemoveTombstones(options_.gc_grace_period_in_seconds);
  merge_out->new_value.reserve(merged.Size());
  merged.Serialize(&(merge_out->new_value));

  return true;
}

bool CassandraValueMergeOperator::PartialMergeMulti(
    const Slice& /*key*/, const std::deque<Slice>& operand_list,
    std::string* new_value, Logger* /*logger*/) const {
  // Clear the *new_value for writing.
  assert(new_value);
  new_value->clear();

  std::vector<RowValue> row_values;
  row_values.reserve(operand_list.size());
  for (auto& operand : operand_list) {
    row_values.push_back(RowValue::Deserialize(operand.data(), operand.size()));
  }
  RowValue merged = RowValue::Merge(std::move(row_values));
  new_value->reserve(merged.Size());
  merged.Serialize(new_value);
  return true;
}

}  // namespace ROCKSDB_NAMESPACE::cassandra
