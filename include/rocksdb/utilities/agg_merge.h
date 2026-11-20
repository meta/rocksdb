// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {
// The feature is still in development so the encoding format is subject
// to change.
//
// Aggregation Merge Operator is a merge operator that allows users to
// aggregate merge operands of different keys with different registered
// aggregation functions. The aggregation can also change for the same
// key if the functions store the data in the same format.
// The target application highly overlaps with merge operator in general
// but we try to provide a better interface so that users are more likely
// to use pre-implemented plug-in functions and connect with existing
// third-party aggregation functions (such as those from SQL engines).
// In this case, the need for users to write customized C++ plug-in code
// is reduced.
// If the idea proves to useful, we might consider to move it to be
// a core functionality of RocksDB, and reduce the support of merge
// operators.
//
// Users can implement aggregation functions by implementing abstract
// class Aggregator, and register it using AddAggregator().
// The merge operator can be retrieved from GetAggMergeOperator() and
// it is a singleton.
//
// Users can push values to be updated with a merge operand encoded with
// registered function name and payload using EncodeAggFuncAndPayload(),
// and the merge operator will invoke the aggregation function.
// An example:
//
//    // Assume class ExampleSumAggregator is implemented to do simple sum.
//    AddAggregator("sum", std::make_unique<ExampleSumAggregator>());
//    std::shared_ptr<MergeOperator> mp_guard = CreateAggMergeOperator();
//    options.merge_operator = mp_guard.get();
//    ...... // Creating DB
//
//
//    std::string encoded_value;
//    s = EncodeAggFuncAndPayload(kUnamedFuncName, "200", encoded_value);
//    assert(s.ok());
//    db->Put(WriteOptions(), "foo", encoded_value);
//    s = EncodeAggFuncAndPayload("sum", "200", encoded_value);
//    assert(s.ok());
//    db->Merge(WriteOptions(), "foo", encoded_value);
//    s = EncodeAggFuncAndPayload("sum", "200", encoded_value);
//    assert(s.ok());
//    db->Merge(WriteOptions(), "foo", encoded_value);
//
//    std::string value;
//    Status s = db->Get(ReadOptions, "foo", &value);
//    assert(s.ok());
//    Slice func, aggregated_value;
//    assert(ExtractAggFuncAndValue(value, func, aggregated_value));
//    assert(func == "sum");
//    assert(aggregated_value == "600");
//
//
// DB::Put() can also be used to add a payloadin the same way as Merge().
//
// kUnamedFuncName can be used as a placeholder function name. This will
// be aggregated with merge operands inserted later based on function
// name given there.
//
// If the aggregation function is not registered or there is an error
// returned by aggregation function, the result will be encoded with a fake
// aggregation function kErrorFuncName, with each merge operands to be encoded
// into a list that can be extracted using ExtractList();
//
// If users add a merge operand using a different aggregation function from
// the previous one, the merge operands for the previous one is aggregated
// and the payload part of the result is treated as the first payload of
// the items for the new aggregation function. For example, users can
// Merge("plus, 1"), merge("plus 2"), merge("minus 3") and the aggregation
// result would be "minus 0".
//

// A class used to aggregate data per key/value. The plug-in function is
// implemented and registered using AddAggregator(). And then use it
// with merge operator created using CreateAggMergeOperator().
class Aggregator {
 public:
  virtual ~Aggregator() = default;
  // The input list is in reverse insertion order, with values[0] to be
  // the one inserted last and values.back() to be the one inserted first.
  // The oldest one might be from Get().
  // Return whether aggregation succeeded. False for aggregation error.
  virtual bool Aggregate(const std::vector<Slice>& values,
                         std::string& result) const = 0;

  // True if a partial aggregation should be invoked. Some aggregators
  // might opt to skip partial aggregation if possible.
  virtual bool DoPartialAggregate() const { return true; }
};

// The function adds aggregation plugin by function name. It is used
// by all the aggregation operator created using CreateAggMergeOperator().
// It's currently not thread safe to run concurrently with the aggregation
// merge operator. It is recommended that all the aggregation function
// is added before calling CreateAggMergeOperator().
Status AddAggregator(const std::string& function_name,
                     std::unique_ptr<Aggregator>&& agg);

// Get the singleton instance of merge operator for aggregation.
// Always the same one is returned with a shared_ptr is hold as a
// static variable by the function.
// This is done so because options.merge_operator is shared_ptr.
std::shared_ptr<MergeOperator> GetAggMergeOperator();

// Encode aggregation function and payload that can be consumed by aggregation
// merge operator.
Status EncodeAggFuncAndPayload(const Slice& function_name, const Slice& payload,
                               std::string& output);
// Helper function to extract aggregation function name and payload.
// Return false if it fails to decode.
bool ExtractAggFuncAndValue(const Slice& op, Slice& func, Slice& value);

// Extract encoded list. This can be used to extract error merge operands when
// the returned function name is kErrorFuncName.
bool ExtractList(const Slice& encoded_list, std::vector<Slice>& decoded_list);

// Special function name that allows it to be merged to subsequent type.
extern const std::string kUnnamedFuncName;

// Special error function name reserved for merging or aggregation error.
extern const std::string kErrorFuncName;

}  // namespace ROCKSDB_NAMESPACE
