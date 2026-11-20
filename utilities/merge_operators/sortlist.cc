//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "utilities/merge_operators/sortlist.h"

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

bool SortList::FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const {
  std::vector<int> left;
  for (Slice slice : merge_in.operand_list) {
    std::vector<int> right;
    MakeVector(right, slice);
    left = Merge(left, right);
  }
  for (int i = 0; i < static_cast<int>(left.size()) - 1; i++) {
    merge_out->new_value.append(std::to_string(left[i])).append(",");
  }
  merge_out->new_value.append(std::to_string(left.back()));
  return true;
}

bool SortList::PartialMerge(const Slice& /*key*/, const Slice& left_operand,
                            const Slice& right_operand, std::string* new_value,
                            Logger* /*logger*/) const {
  std::vector<int> left;
  std::vector<int> right;
  MakeVector(left, left_operand);
  MakeVector(right, right_operand);
  left = Merge(left, right);
  for (int i = 0; i < static_cast<int>(left.size()) - 1; i++) {
    new_value->append(std::to_string(left[i])).append(",");
  }
  new_value->append(std::to_string(left.back()));
  return true;
}

bool SortList::PartialMergeMulti(const Slice& /*key*/,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value,
                                 Logger* /*logger*/) const {
  (void)operand_list;
  (void)new_value;
  return true;
}

void SortList::MakeVector(std::vector<int>& operand, Slice slice) const {
  do {
    const char* begin = slice.data_;
    while (*slice.data_ != ',' && *slice.data_) {
      slice.data_++;
    }
    operand.push_back(std::stoi(std::string(begin, slice.data_)));
  } while (0 != *slice.data_++);
}

std::vector<int> SortList::Merge(std::vector<int>& left,
                                 std::vector<int>& right) const {
  // Fill the resultant vector with sorted results from both vectors
  std::vector<int> result;
  unsigned left_it = 0;
  unsigned right_it = 0;

  while (left_it < left.size() && right_it < right.size()) {
    // If the left value is smaller than the right it goes next
    // into the resultant vector
    if (left[left_it] < right[right_it]) {
      result.push_back(left[left_it]);
      left_it++;
    } else {
      result.push_back(right[right_it]);
      right_it++;
    }
  }

  // Push the remaining data from both vectors onto the resultant
  while (left_it < left.size()) {
    result.push_back(left[left_it]);
    left_it++;
  }

  while (right_it < right.size()) {
    result.push_back(right[right_it]);
    right_it++;
  }

  return result;
}

std::shared_ptr<MergeOperator> MergeOperators::CreateSortOperator() {
  return std::make_shared<SortList>();
}
}  // namespace ROCKSDB_NAMESPACE
