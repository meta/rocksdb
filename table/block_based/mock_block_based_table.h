//  Copyright (c) 2019-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <memory>

#include "rocksdb/filter_policy.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/filter_policy_internal.h"

namespace ROCKSDB_NAMESPACE {
namespace mock {

class MockBlockBasedTable : public BlockBasedTable {
 public:
  explicit MockBlockBasedTable(Rep* rep)
      : BlockBasedTable(rep, nullptr /* block_cache_tracer */) {}
};

class MockBlockBasedTableTester {
  static constexpr int kMockLevel = 0;

 public:
  Options options_;
  ImmutableOptions ioptions_;
  EnvOptions env_options_;
  BlockBasedTableOptions table_options_;
  InternalKeyComparator icomp_;
  std::unique_ptr<BlockBasedTable> table_;

  explicit MockBlockBasedTableTester(const FilterPolicy* filter_policy)
      : MockBlockBasedTableTester(
            std::shared_ptr<const FilterPolicy>(filter_policy)) {}

  explicit MockBlockBasedTableTester(
      std::shared_ptr<const FilterPolicy> filter_policy)
      : ioptions_(options_),
        env_options_(options_),
        icomp_(options_.comparator) {
    table_options_.filter_policy = std::move(filter_policy);

    constexpr bool skip_filters = false;
    constexpr bool immortal_table = false;
    table_.reset(new MockBlockBasedTable(new BlockBasedTable::Rep(
        ioptions_, env_options_, table_options_, icomp_, skip_filters,
        12345 /*file_size*/, kMockLevel, immortal_table)));
  }

  FilterBitsBuilder* GetBuilder() const {
    FilterBuildingContext context(table_options_);
    context.column_family_name = "mock_cf";
    context.compaction_style = ioptions_.compaction_style;
    context.level_at_creation = kMockLevel;
    context.info_log = ioptions_.logger;
    return BloomFilterPolicy::GetBuilderFromContext(context);
  }
};

}  // namespace mock
}  // namespace ROCKSDB_NAMESPACE
