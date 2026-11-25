//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <cstdio>
#include <memory>
#include <string>

#include "rocksdb/merge_operator.h"

namespace ROCKSDB_NAMESPACE {

class MergeOperators {
 public:
  static std::shared_ptr<MergeOperator> CreatePutOperator();
  static std::shared_ptr<MergeOperator> CreateDeprecatedPutOperator();
  static std::shared_ptr<MergeOperator> CreateUInt64AddOperator();
  static std::shared_ptr<MergeOperator> CreateStringAppendOperator();
  static std::shared_ptr<MergeOperator> CreateStringAppendOperator(
      char delim_char);
  static std::shared_ptr<MergeOperator> CreateStringAppendOperator(
      const std::string& delim);
  static std::shared_ptr<MergeOperator> CreateStringAppendTESTOperator();
  static std::shared_ptr<MergeOperator> CreateMaxOperator();
  static std::shared_ptr<MergeOperator> CreateBytesXOROperator();
  static std::shared_ptr<MergeOperator> CreateSortOperator();

  // Will return a different merge operator depending on the string.
  static std::shared_ptr<MergeOperator> CreateFromStringId(
      const std::string& name);
};

}  // namespace ROCKSDB_NAMESPACE
