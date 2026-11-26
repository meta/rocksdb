//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/table_properties_collectors/compact_for_tiering_collector.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <iostream>
#include <vector>

#include "db/seqno_to_time_mapping.h"
#include "port/stack_trace.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

TEST(CompactForTieringCollector, NotEnabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;

  // Set compaction trigger ratio to 0 to disable it. No collector created.
  auto factory = NewCompactForTieringCollectorFactory(0);
  std::unique_ptr<TablePropertiesCollector> collector(
      factory->CreateTablePropertiesCollector(context));
  ASSERT_EQ(nullptr, collector);
}

TEST(CompactForTieringCollector, TieringDisabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = kMaxSequenceNumber;

  // Tiering is disabled on the column family. No collector created.
  {
    for (double compaction_trigger_ratio : {0.0, 0.1, 1.0, 1.5}) {
      auto factory =
          NewCompactForTieringCollectorFactory(compaction_trigger_ratio);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      ASSERT_EQ(nullptr, collector);
    }
  }
}

TEST(CompactForTieringCollector, LastLevelFile) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 5;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;

  // No collector created for a file that is already on the last level.
  {
    for (double compaction_trigger_ratio : {0.0, 0.1, 1.0, 1.5}) {
      auto factory =
          NewCompactForTieringCollectorFactory(compaction_trigger_ratio);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      ASSERT_EQ(nullptr, collector);
    }
  }
}

TEST(CompactForTieringCollector, ProximalLevelOutputFile) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  // Proximal level: the level above the last level (num_levels - 2).
  context.level_at_creation = 4;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  context.is_proximal_level_output = true;

  // No collector is created for an output routed to the proximal level by
  // per-key placement, so such files are never marked for tiering
  // compaction.
  for (double compaction_trigger_ratio : {0.1, 0.4, 1.0, 1.5}) {
    auto factory =
        NewCompactForTieringCollectorFactory(compaction_trigger_ratio);
    std::unique_ptr<TablePropertiesCollector> collector(
        factory->CreateTablePropertiesCollector(context));
    ASSERT_EQ(nullptr, collector);
  }

  // Same when the proximal level is L0 in a 2-level DB: routing, not the
  // level number, decides.
  context.level_at_creation = 0;
  context.num_levels = 2;
  auto factory = NewCompactForTieringCollectorFactory(0.4);
  std::unique_ptr<TablePropertiesCollector> collector(
      factory->CreateTablePropertiesCollector(context));
  ASSERT_EQ(nullptr, collector);
}

TEST(CompactForTieringCollector, OrdinaryFileOnProximalLevel) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  // Same level as the proximal level, but this is an ordinary output that
  // was not routed there by per-key placement.
  context.level_at_creation = 4;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  context.is_proximal_level_output = false;

  // A collector is still created, so ordinary files on this level remain
  // eligible to be marked for tiering compaction.
  for (double compaction_trigger_ratio : {0.1, 0.4, 1.0, 1.5}) {
    auto factory =
        NewCompactForTieringCollectorFactory(compaction_trigger_ratio);
    std::unique_ptr<TablePropertiesCollector> collector(
        factory->CreateTablePropertiesCollector(context));
    ASSERT_NE(nullptr, collector);
  }
}

TEST(CompactForTieringCollector, CollectorEnabled) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  {
    for (double compaction_trigger_ratio : {0.1, 0.33333333, 0.5, 1.0, 1.5}) {
      auto factory =
          NewCompactForTieringCollectorFactory(compaction_trigger_ratio);
      std::unique_ptr<TablePropertiesCollector> collector(
          factory->CreateTablePropertiesCollector(context));
      for (size_t i = 0; i < kTotalEntries; i++) {
        ASSERT_OK(collector->AddUserKey("hello", "rocksdb", kEntryPut, i, 0));
        ASSERT_FALSE(collector->NeedCompact());
      }
      UserCollectedProperties user_properties;
      ASSERT_OK(collector->Finish(&user_properties));
      ASSERT_EQ(user_properties[CompactForTieringCollector::
                                    kNumEligibleLastLevelEntriesPropertyName],
                std::to_string(50));
      if (compaction_trigger_ratio > 0.5) {
        ASSERT_FALSE(collector->NeedCompact());
      } else {
        ASSERT_TRUE(collector->NeedCompact());
      }
    }
  }
}

TEST(CompactForTieringCollector, TimedPutEntries) {
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 1;
  context.level_at_creation = 1;
  context.num_levels = 6;
  context.last_level_inclusive_max_seqno_threshold = 50;
  const size_t kTotalEntries = 100;

  auto factory = NewCompactForTieringCollectorFactory(0.1);
  std::unique_ptr<TablePropertiesCollector> collector(
      factory->CreateTablePropertiesCollector(context));
  for (size_t i = 0; i < kTotalEntries; i++) {
    std::string value;
    PackValueAndSeqno("rocksdb", i, &value);
    ASSERT_OK(collector->AddUserKey("hello", value, kEntryTimedPut, 0, 0));
    ASSERT_FALSE(collector->NeedCompact());
  }
  UserCollectedProperties user_properties;
  ASSERT_OK(collector->Finish(&user_properties));
  ASSERT_EQ(user_properties[CompactForTieringCollector::
                                kNumEligibleLastLevelEntriesPropertyName],
            std::to_string(50));
  ASSERT_TRUE(collector->NeedCompact());
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}