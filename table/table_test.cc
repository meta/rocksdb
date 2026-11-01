//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/table.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdio>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "cache/lru_cache.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "memtable/stl_wrappers.h"
#include "monitoring/statistics_impl.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/c.h"
#include "rocksdb/cache.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/external_table.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/file_system.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/iterator.h"
#include "rocksdb/listener.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/unique_id.h"
#include "rocksdb/user_defined_index.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_iterator.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/filter_policy_internal.h"
#include "table/block_based/flush_block_policy_impl.h"
#include "table/block_fetcher.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/plain/plain_table_factory.h"
#include "table/sst_file_writer_collectors.h"
#include "table/unique_id_impl.h"
#include "test_util/simple_external_table_factory.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/memory_allocators.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

namespace {

static_assert(
    std::is_same<IndexFactoryIterator, UserDefinedIndexIterator>::value);
static_assert(std::is_same<IndexFactoryReader, UserDefinedIndexReader>::value);

TEST(IndexFactoryCompatibilityTest, NewNamesPreserveOldSubclassShape) {
  class LegacyOnlyBuilder : public IndexFactoryBuilder {
   public:
    Slice AddIndexEntry(const Slice& last_key_in_current_block,
                        const Slice* /*first_key_in_next_block*/,
                        const BlockHandle& /*block_handle*/,
                        std::string* /*separator_scratch*/,
                        const IndexEntryContext& /*context*/) override {
      return last_key_in_current_block;
    }

    Status Finish(Slice* index_contents) override {
      *index_contents = Slice();
      return Status::OK();
    }

    uint64_t EstimatedSize() const override { return 0; }
  };

  class LegacyOnlyFactory : public IndexFactory {
   public:
    using IndexFactory::NewBuilder;
    using IndexFactory::NewReader;

    const char* Name() const override { return "legacy_index"; }

    IndexFactoryBuilder* NewBuilder() const override {
      return new LegacyOnlyBuilder();
    }

    std::unique_ptr<IndexFactoryReader> NewReader(
        Slice& /*index_block*/) const override {
      return nullptr;
    }
  };

  LegacyOnlyFactory factory;
  const IndexFactory* index_factory = &factory;
  IndexFactoryOptions options;
  std::unique_ptr<IndexFactoryBuilder> builder;
  ASSERT_OK(index_factory->NewBuilder(options, builder));
  ASSERT_NE(builder, nullptr);

  Slice index_contents;
  ASSERT_OK(builder->Finish(&index_contents));
  ASSERT_TRUE(index_contents.empty());
  ASSERT_EQ(UserDefinedIndexBuilder::kValue,
            UserDefinedIndexBuilder::ValueType::kValue);
  ASSERT_STREQ(kIndexFactoryMetaPrefix, kUserDefinedIndexPrefix);
}

TEST(IndexFactoryCompatibilityTest, LegacyBlockBasedTableOptionsFields) {
  BlockBasedTableOptions table_options;
  table_options.use_udi_as_primary_index = true;
  BlockBasedTableFactory primary_factory(table_options);
  ASSERT_NE(primary_factory.GetPrintableOptions().find("index_mode: 2"),
            std::string::npos);

  table_options.use_udi_as_primary_index = false;
  table_options.fail_if_no_udi_on_open = true;
  BlockBasedTableFactory required_factory(table_options);
  ASSERT_NE(required_factory.GetPrintableOptions().find("index_mode: 4"),
            std::string::npos);
}

TEST(IndexFactoryCompatibilityTest, LegacyIndexBooleansCanBeCleared) {
  using IndexMode = BlockBasedTableOptions::IndexMode;
  ConfigOptions cfg;
  cfg.ignore_unknown_options = false;
  for (bool use_map : {false, true}) {
    SCOPED_TRACE(testing::Message() << "use_map=" << use_map);
    BlockBasedTableFactory factory{BlockBasedTableOptions()};
    auto set_bool = [&](const std::string& name, bool value,
                        IndexMode expected) {
      SCOPED_TRACE(name + "=" + (value ? "true" : "false"));
      if (use_map) {
        ASSERT_OK(
            factory.ConfigureFromMap(cfg, {{name, value ? "true" : "false"}}));
      } else {
        ASSERT_OK(factory.ConfigureOption(cfg, name, value ? "true" : "false"));
      }
      EXPECT_EQ(
          factory.GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
          expected);
    };
    set_bool("fail_if_no_udi_on_open", true, IndexMode::kStandardRequired);
    set_bool("use_udi_as_primary_index", true, IndexMode::kCustomDefault);
    set_bool("skip_standard_index", true, IndexMode::kCustomOnly);
    set_bool("skip_standard_index", false, IndexMode::kCustomDefault);
    set_bool("use_udi_as_primary_index", false, IndexMode::kStandardRequired);
    set_bool("fail_if_no_udi_on_open", false, IndexMode::kStandardDefault);
  }

  for (bool primary : {false, true}) {
    BlockBasedTableOptions options;
    options.use_udi_as_primary_index = primary;
    options.fail_if_no_udi_on_open = !primary;
    BlockBasedTableFactory factory(options);
    ASSERT_OK(factory.ConfigureOption(
        cfg, primary ? "use_udi_as_primary_index" : "fail_if_no_udi_on_open",
        "false"));
    EXPECT_EQ(
        factory.GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
        IndexMode::kStandardDefault);
  }
}

TEST(IndexFactoryCompatibilityTest, CopiedLegacyOptionsCanRollBack) {
  using IndexMode = BlockBasedTableOptions::IndexMode;
  for (bool primary : {false, true}) {
    SCOPED_TRACE(primary);
    BlockBasedTableOptions options;
    options.use_udi_as_primary_index = primary;
    options.fail_if_no_udi_on_open = !primary;
    BlockBasedTableFactory factory(options);
    BlockBasedTableOptions copied =
        *factory.GetOptions<BlockBasedTableOptions>();
    EXPECT_EQ(copied.index_mode, IndexMode::kStandardDefault);
    EXPECT_EQ(
        copied.GetEffectiveIndexMode(),
        primary ? IndexMode::kCustomDefault : IndexMode::kStandardRequired);
    copied.use_udi_as_primary_index = false;
    copied.fail_if_no_udi_on_open = false;
    BlockBasedTableFactory rollback(copied);
    EXPECT_EQ(
        rollback.GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
        IndexMode::kStandardDefault);
    EXPECT_NE(rollback.GetPrintableOptions().find("index_mode: 1"),
              std::string::npos);
  }
}

TEST(IndexFactoryCompatibilityTest, CApiLegacyFactoryCanRollBack) {
  for (bool explicit_default : {false, true}) {
    SCOPED_TRACE(explicit_default);
    rocksdb_block_based_table_options_t* table_options =
        rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_use_udi_as_primary_index(table_options, 1);
    if (explicit_default) {
      rocksdb_block_based_options_set_index_mode(
          table_options,
          static_cast<int>(
              BlockBasedTableOptions::IndexMode::kStandardDefault));
      rocksdb_block_based_options_set_use_udi_as_primary_index(table_options,
                                                               1);
    }
    rocksdb_options_t* base = rocksdb_options_create();
    rocksdb_options_set_block_based_table_factory(base, table_options);
    rocksdb_block_based_options_destroy(table_options);
    rocksdb_options_t* rollback = rocksdb_options_create();
    char* error = nullptr;
    rocksdb_get_options_from_string(
        base,
        explicit_default
            ? "block_based_table_factory.use_udi_as_primary_index=true"
            : "block_based_table_factory.use_udi_as_primary_index=false",
        rollback, &error);
    ASSERT_EQ(error, nullptr) << error;
    rocksdb_options_destroy(base);
    rocksdb_options_set_create_if_missing(rollback, 1);
    const std::string path = test::PerThreadDBPath("c_api_udi_rollback");
    rocksdb_t* db = rocksdb_open(rollback, path.c_str(), &error);
    ASSERT_EQ(error, nullptr) << error;
    ASSERT_NE(db, nullptr);
    rocksdb_close(db);
    rocksdb_destroy_db(rollback, path.c_str(), &error);
    ASSERT_EQ(error, nullptr) << error;
    rocksdb_options_destroy(rollback);
  }
}

TEST(IndexFactoryCompatibilityTest, ConfiguredIndexModeBeatsLegacyBooleans) {
  using IndexMode = BlockBasedTableOptions::IndexMode;
  const std::pair<const char*, IndexMode> modes[] = {
      {"kStandardOnly", IndexMode::kStandardOnly},
      {"kStandardDefault", IndexMode::kStandardDefault},
      {"kStandardRequired", IndexMode::kStandardRequired},
      {"kCustomDefault", IndexMode::kCustomDefault},
      {"kCustomOnly", IndexMode::kCustomOnly}};
  ConfigOptions cfg;
  cfg.ignore_unknown_options = false;
  for (const auto& [name, mode] : modes) {
    for (bool use_map : {false, true}) {
      SCOPED_TRACE(testing::Message() << name << " use_map=" << use_map);
      BlockBasedTableFactory factory{BlockBasedTableOptions()};
      auto set_option = [&](const std::string& key, const std::string& value) {
        if (use_map) {
          ASSERT_OK(factory.ConfigureFromMap(cfg, {{key, value}}));
        } else {
          ASSERT_OK(factory.ConfigureOption(cfg, key, value));
        }
      };
      set_option("use_udi_as_primary_index", "true");
      set_option("index_mode", name);
      for (const char* alias :
           {"use_udi_as_primary_index", "fail_if_no_udi_on_open",
            "skip_standard_index"}) {
        for (const char* value : {"false", "true"}) {
          set_option(alias, value);
          EXPECT_EQ(factory.GetOptions<BlockBasedTableOptions>()
                        ->GetEffectiveIndexMode(),
                    mode);
        }
      }
      std::unique_ptr<TableFactory> clone = factory.Clone();
      ASSERT_OK(
          clone->ConfigureOption(cfg, "use_udi_as_primary_index", "true"));
      EXPECT_EQ(
          clone->GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
          mode);

      BlockBasedTableFactory reconstructed(
          *factory.GetOptions<BlockBasedTableOptions>());
      EXPECT_EQ(reconstructed.GetOptions<BlockBasedTableOptions>()
                    ->GetEffectiveIndexMode(),
                mode);

      BlockBasedTableFactory combined{BlockBasedTableOptions()};
      ASSERT_OK(
          combined.ConfigureFromMap(cfg, {{"index_mode", name},
                                          {"index_mode_explicit", "true"},
                                          {"use_udi_as_primary_index", "true"},
                                          {"fail_if_no_udi_on_open", "true"},
                                          {"skip_standard_index", "true"}}));
      EXPECT_EQ(combined.GetOptions<BlockBasedTableOptions>()
                    ->GetEffectiveIndexMode(),
                mode);
      BlockBasedTableOptions parsed;
      ASSERT_OK(GetBlockBasedTableOptionsFromString(
          cfg, BlockBasedTableOptions(),
          std::string("index_mode=") + name +
              ";index_mode_explicit=true;use_udi_as_primary_index=true;"
              "fail_if_no_udi_on_open=true;"
              "skip_standard_index=true",
          &parsed));
      BlockBasedTableFactory from_parsed(parsed);
      EXPECT_EQ(from_parsed.GetOptions<BlockBasedTableOptions>()
                    ->GetEffectiveIndexMode(),
                mode);
    }
  }
}

TEST(IndexFactoryCompatibilityTest, LegacyIndexStateSurvivesFailedMapAndClone) {
  using IndexMode = BlockBasedTableOptions::IndexMode;
  ConfigOptions cfg;
  cfg.ignore_unknown_options = false;
  BlockBasedTableFactory factory{BlockBasedTableOptions()};
  ASSERT_OK(factory.ConfigureOption(cfg, "use_udi_as_primary_index", "true"));
  for (const char* invalid_name :
       {"index_mode", "use_udi_as_primary_index", "fail_if_no_udi_on_open",
        "skip_standard_index"}) {
    SCOPED_TRACE(invalid_name);
    std::unordered_map<std::string, std::string> updates = {
        {"skip_standard_index", "true"}};
    updates[invalid_name] = "invalid";
    Status s = factory.ConfigureFromMap(cfg, updates);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(
        factory.GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
        IndexMode::kCustomDefault);
    std::unique_ptr<TableFactory> clone = factory.Clone();
    ASSERT_OK(clone->ConfigureOption(cfg, "use_udi_as_primary_index", "false"));
    EXPECT_EQ(
        clone->GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
        IndexMode::kStandardDefault);
  }
  ASSERT_OK(factory.ConfigureOption(cfg, "skip_standard_index", "true"));
  std::unique_ptr<TableFactory> clone = factory.Clone();
  ASSERT_OK(clone->ConfigureOption(cfg, "skip_standard_index", "false"));
  EXPECT_EQ(
      clone->GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
      IndexMode::kCustomDefault);
  EXPECT_EQ(
      factory.GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
      IndexMode::kCustomOnly);

  cfg.mutable_options_only = true;
  for (const char* alias : {"use_udi_as_primary_index",
                            "fail_if_no_udi_on_open", "skip_standard_index"}) {
    Status s = factory.ConfigureOption(cfg, alias, "false");
    EXPECT_TRUE(s.IsInvalidArgument());
    EXPECT_EQ(
        factory.GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
        IndexMode::kCustomOnly);
  }
}

TEST(IndexFactoryCompatibilityTest, IndexModeOptionsFileRoundTrip) {
  using IndexMode = BlockBasedTableOptions::IndexMode;
  const struct {
    const char* option;
    const char* value;
    IndexMode expected;
  } cases[] = {
      {"index_mode", "kStandardOnly", IndexMode::kStandardOnly},
      {"index_mode", "kStandardDefault", IndexMode::kStandardDefault},
      {"index_mode", "kStandardRequired", IndexMode::kStandardRequired},
      {"index_mode", "kCustomDefault", IndexMode::kCustomDefault},
      {"index_mode", "kCustomOnly", IndexMode::kCustomOnly},
      {"use_udi_as_primary_index", "true", IndexMode::kCustomDefault},
      {"fail_if_no_udi_on_open", "true", IndexMode::kStandardRequired}};
  ConfigOptions cfg;
  cfg.ignore_unknown_options = false;
  DBOptions db_options;
  const std::string options_path = test::PerThreadDBPath("udi_mode_round_trip");
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.option);
    SCOPED_TRACE(test_case.value);
    ColumnFamilyOptions cf_options;
    ASSERT_OK(cf_options.table_factory->ConfigureOption(cfg, test_case.option,
                                                        test_case.value));
    ASSERT_OK(PersistRocksDBOptions(WriteOptions(), cfg, db_options,
                                    {"default"}, {cf_options}, options_path,
                                    db_options.env->GetFileSystem().get()));
    std::string contents;
    ASSERT_OK(ReadFileToString(db_options.env, options_path, &contents));
    ASSERT_NE(contents.find("index_mode_explicit=true"), std::string::npos);

    // 11.10/11.11 route by these booleans even when they parse index_mode.
    // Check the persisted policy, including explicit modes with cleared
    // aliases.
    ASSERT_NE(contents.find(std::string("use_udi_as_primary_index=") +
                            (test_case.expected == IndexMode::kCustomDefault
                                 ? "true"
                                 : "false")),
              std::string::npos);
    ASSERT_NE(contents.find(std::string("fail_if_no_udi_on_open=") +
                            (test_case.expected == IndexMode::kStandardRequired
                                 ? "true"
                                 : "false")),
              std::string::npos);
    if (test_case.expected == IndexMode::kStandardDefault ||
        test_case.expected == IndexMode::kStandardRequired ||
        test_case.expected == IndexMode::kCustomDefault) {
      // Exercise the policy an older reader sees after ignoring the mode and
      // its origin marker. Other persisted settings remain in the file.
      std::istringstream input(contents);
      std::string legacy_contents;
      std::string line;
      while (std::getline(input, line)) {
        if (line.find("index_mode=") == std::string::npos &&
            line.find("index_mode_explicit=") == std::string::npos) {
          legacy_contents += line + "\n";
        }
      }
      ASSERT_OK(
          WriteStringToFile(db_options.env, legacy_contents, options_path));
      std::vector<ColumnFamilyDescriptor> legacy_descriptors;
      ASSERT_OK(LoadOptionsFromFile(cfg, options_path, &db_options,
                                    &legacy_descriptors));
      ASSERT_EQ(legacy_descriptors.size(), 1U);
      EXPECT_EQ(legacy_descriptors[0]
                    .options.table_factory->GetOptions<BlockBasedTableOptions>()
                    ->GetEffectiveIndexMode(),
                test_case.expected);
    }

    // Stale flags must not override a newly persisted explicit mode, including
    // kStandardDefault, whose value also appeared in the old encoding.
    contents +=
        "use_udi_as_primary_index=true\nfail_if_no_udi_on_open=true\n"
        "skip_standard_index=true\n";
    ASSERT_OK(WriteStringToFile(db_options.env, contents, options_path));
    std::vector<ColumnFamilyDescriptor> descriptors;
    ASSERT_OK(
        LoadOptionsFromFile(cfg, options_path, &db_options, &descriptors));
    ASSERT_EQ(descriptors.size(), 1U);
    std::shared_ptr<TableFactory> loaded = descriptors[0].options.table_factory;
    ASSERT_EQ(
        loaded->GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
        test_case.expected);
    ASSERT_OK(
        loaded->ConfigureOption(cfg, "use_udi_as_primary_index", "false"));
    ASSERT_EQ(
        loaded->GetOptions<BlockBasedTableOptions>()->GetEffectiveIndexMode(),
        test_case.expected);
  }
  ASSERT_OK(db_options.env->DeleteFile(options_path));
}

// An index_mode the caller set explicitly must survive a deprecated bool they
// forgot to clear, otherwise the documented rollback (drop index_mode back to
// kStandardOnly) is silently escalated back to the custom index.
TEST(IndexFactoryCompatibilityTest, ExplicitIndexModeBeatsLegacyBools) {
  auto expect_mode = [](const BlockBasedTableOptions& opts,
                        const std::string& expected) {
    SCOPED_TRACE(expected);
    BlockBasedTableFactory factory(opts);
    EXPECT_NE(factory.GetPrintableOptions().find(expected), std::string::npos);
  };

  BlockBasedTableOptions table_options;
  table_options.use_udi_as_primary_index = true;
  table_options.fail_if_no_udi_on_open = true;

  table_options.index_mode = BlockBasedTableOptions::IndexMode::kStandardOnly;
  expect_mode(table_options, "index_mode: 0");

  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardRequired;
  expect_mode(table_options, "index_mode: 4");

  table_options.index_mode = BlockBasedTableOptions::IndexMode::kCustomOnly;
  expect_mode(table_options, "index_mode: 3");

  // Left at the default, the bools still apply for old callers.
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  expect_mode(table_options, "index_mode: 2");
}

// Unsupported underlying values can take incidental comparison branches in
// index selection. Reject them instead of silently choosing unintended modes.
TEST(IndexFactoryCompatibilityTest, UnrecognizedIndexModeIsRejected) {
  ColumnFamilyOptions cf_opts;
  DBOptions db_opts;

  for (int mode : {-1, 5, 99}) {
    SCOPED_TRACE("index_mode=" + std::to_string(mode));
    BlockBasedTableOptions table_options;
    table_options.index_mode =
        static_cast<BlockBasedTableOptions::IndexMode>(mode);
    BlockBasedTableFactory factory(table_options);
    Status s = factory.ValidateOptions(db_opts, cf_opts);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_NE(s.ToString().find("Unrecognized index_mode"), std::string::npos)
        << s.ToString();
  }

  for (auto mode : {BlockBasedTableOptions::IndexMode::kStandardOnly,
                    BlockBasedTableOptions::IndexMode::kStandardDefault,
                    BlockBasedTableOptions::IndexMode::kStandardRequired}) {
    SCOPED_TRACE("index_mode=" + std::to_string(static_cast<int>(mode)));
    BlockBasedTableOptions table_options;
    table_options.index_mode = mode;
    BlockBasedTableFactory factory(table_options);
    ASSERT_OK(factory.ValidateOptions(db_opts, cf_opts));
  }
}

const std::string kDummyValue(10000, 'o');
constexpr auto kVerbose = false;

// DummyPropertiesCollector used to test BlockBasedTableProperties
class DummyPropertiesCollector : public TablePropertiesCollector {
 public:
  const char* Name() const override { return "DummyPropertiesCollector"; }

  Status Finish(UserCollectedProperties* /*properties*/) override {
    return Status::OK();
  }

  Status Add(const Slice& /*user_key*/, const Slice& /*value*/) override {
    return Status::OK();
  }

  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties{};
  }
};

class DummyPropertiesCollectorFactory1
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return new DummyPropertiesCollector();
  }
  const char* Name() const override {
    return "DummyPropertiesCollectorFactory1";
  }
};

class DummyPropertiesCollectorFactory2
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return new DummyPropertiesCollector();
  }
  const char* Name() const override {
    return "DummyPropertiesCollectorFactory2";
  }
};

// Return reverse of "key".
// Used to test non-lexicographic comparators.
std::string Reverse(const Slice& key) {
  auto rev = key.ToString();
  std::reverse(rev.begin(), rev.end());
  return rev;
}

class ReverseKeyComparator : public Comparator {
 public:
  const char* Name() const override {
    return "rocksdb.ReverseBytewiseComparator";
  }

  int Compare(const Slice& a, const Slice& b) const override {
    return BytewiseComparator()->Compare(Reverse(a), Reverse(b));
  }

  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    std::string s = Reverse(*start);
    std::string l = Reverse(limit);
    BytewiseComparator()->FindShortestSeparator(&s, l);
    *start = Reverse(s);
  }

  void FindShortSuccessor(std::string* key) const override {
    std::string s = Reverse(*key);
    BytewiseComparator()->FindShortSuccessor(&s);
    *key = Reverse(s);
  }
};

ReverseKeyComparator reverse_key_comparator;

void Increment(const Comparator* cmp, std::string* key) {
  if (cmp == BytewiseComparator()) {
    key->push_back('\0');
  } else {
    assert(cmp == &reverse_key_comparator);
    std::string rev = Reverse(*key);
    rev.push_back('\0');
    *key = Reverse(rev);
  }
}

const auto kUnknownColumnFamily =
    TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;

}  // namespace

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator* cmp)
      : data_(stl_wrappers::LessOfComparator(cmp)) {}
  virtual ~Constructor() = default;

  void Add(const std::string& key, const Slice& value) {
    data_[key] = value.ToString();
  }

  // Finish constructing the data structure with all the keys that have
  // been added so far.  Returns the keys in sorted order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options& options, const ImmutableOptions& ioptions,
              const MutableCFOptions& moptions,
              const BlockBasedTableOptions& table_options,
              const InternalKeyComparator& internal_comparator,
              std::vector<std::string>* keys, stl_wrappers::KVMap* kvmap) {
    last_internal_comparator_ = &internal_comparator;
    *kvmap = data_;
    keys->clear();
    for (const auto& kv : data_) {
      keys->push_back(kv.first);
    }
    data_.clear();
    Status s = FinishImpl(options, ioptions, moptions, table_options,
                          internal_comparator, *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options& options,
                            const ImmutableOptions& ioptions,
                            const MutableCFOptions& moptions,
                            const BlockBasedTableOptions& table_options,
                            const InternalKeyComparator& internal_comparator,
                            const stl_wrappers::KVMap& data) = 0;

  virtual InternalIterator* NewIterator(
      const SliceTransform* prefix_extractor = nullptr) const = 0;

  virtual const stl_wrappers::KVMap& data() { return data_; }

  virtual bool IsArenaMode() const { return false; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor

  virtual bool AnywayDeleteIterator() const { return false; }

 protected:
  const InternalKeyComparator* last_internal_comparator_;

 private:
  stl_wrappers::KVMap data_;
};

// A helper class that converts internal format keys into user keys
class KeyConvertingIterator : public InternalIterator {
 public:
  explicit KeyConvertingIterator(InternalIterator* iter,
                                 bool arena_mode = false)
      : iter_(iter), arena_mode_(arena_mode) {}
  ~KeyConvertingIterator() override {
    if (arena_mode_) {
      iter_->~InternalIterator();
    } else {
      delete iter_;
    }
  }
  bool Valid() const override { return iter_->Valid() && status_.ok(); }
  void Seek(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->Seek(encoded);
  }
  void SeekForPrev(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->SeekForPrev(encoded);
  }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }
  IterBoundCheck UpperBoundCheckResult() override {
    return iter_->UpperBoundCheckResult();
  }

  Slice key() const override {
    assert(Valid());
    ParsedInternalKey parsed_key;
    Status pik_status =
        ParseInternalKey(iter_->key(), &parsed_key, true /* log_err_key */);
    if (!pik_status.ok()) {
      status_ = pik_status;
      return Slice(status_.getState());
    }
    return parsed_key.user_key;
  }

  Slice value() const override { return iter_->value(); }
  Status status() const override {
    return status_.ok() ? iter_->status() : status_;
  }

 private:
  mutable Status status_;
  InternalIterator* iter_;
  bool arena_mode_;

  // No copying allowed
  KeyConvertingIterator(const KeyConvertingIterator&) = delete;
  void operator=(const KeyConvertingIterator&) = delete;
};

// `BlockConstructor` APIs always accept/return user keys.
class BlockConstructor : public Constructor {
 public:
  explicit BlockConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp), block_(nullptr) {}
  ~BlockConstructor() override { delete block_; }
  Status FinishImpl(const Options& /*options*/,
                    const ImmutableOptions& /*ioptions*/,
                    const MutableCFOptions& /*moptions*/,
                    const BlockBasedTableOptions& table_options,
                    const InternalKeyComparator& /*internal_comparator*/,
                    const stl_wrappers::KVMap& kv_map) override {
    delete block_;
    block_ = nullptr;
    BlockBuilder builder(BlockBuilder::ForMetaBlock{},
                         table_options.block_restart_interval);

    for (const auto& kv : kv_map) {
      // `DataBlockIter` assumes it reads only internal keys. `BlockConstructor`
      // clients provide user keys, so we need to convert to internal key format
      // before writing the data block.
      ParsedInternalKey ikey(kv.first, kMaxSequenceNumber, kTypeValue);
      std::string encoded;
      AppendInternalKey(&encoded, ikey);
      builder.Add(encoded, kv.second);
    }
    // Open the block
    data_ = builder.Finish().ToString();
    BlockContents contents;
    contents.data = data_;
    block_ = new Block(std::move(contents));
    return Status::OK();
  }
  InternalIterator* NewIterator(
      const SliceTransform* /*prefix_extractor*/) const override {
    // `DataBlockIter` returns the internal keys it reads.
    // `KeyConvertingIterator` converts them to user keys before they are
    // exposed to the `BlockConstructor` clients.
    return new KeyConvertingIterator(
        block_->NewDataIterator(comparator_, kDisableGlobalSequenceNumber));
  }

 private:
  const Comparator* comparator_;
  std::string data_;
  Block* block_;

  BlockConstructor() = delete;
};

class TableConstructor : public Constructor {
 public:
  explicit TableConstructor(const Comparator* cmp,
                            bool convert_to_internal_key = false,
                            int level = -1, SequenceNumber largest_seqno = 0)
      : Constructor(cmp),
        largest_seqno_(largest_seqno),
        convert_to_internal_key_(convert_to_internal_key),
        level_(level) {
    env_ = ROCKSDB_NAMESPACE::Env::Default();
  }
  ~TableConstructor() override { Reset(); }

  Status FinishImpl(const Options& options, const ImmutableOptions& ioptions,
                    const MutableCFOptions& moptions,
                    const BlockBasedTableOptions& /*table_options*/,
                    const InternalKeyComparator& internal_comparator,
                    const stl_wrappers::KVMap& kv_map) override {
    Reset();
    soptions.use_mmap_reads = ioptions.allow_mmap_reads;
    std::unique_ptr<FSWritableFile> sink(new test::StringSink());
    file_writer_.reset(new WritableFileWriter(
        std::move(sink), "" /* don't care */, FileOptions()));
    std::unique_ptr<TableBuilder> builder;
    InternalTblPropCollFactories internal_tbl_prop_coll_factories;

    if (largest_seqno_ != 0) {
      // Pretend that it's an external file written by SstFileWriter.
      internal_tbl_prop_coll_factories.emplace_back(
          new SstFileWriterPropertiesCollectorFactory(2 /* version */));
    }

    std::string column_family_name;
    const ReadOptions read_options;
    const WriteOptions write_options;
    builder.reset(moptions.table_factory->NewTableBuilder(
        TableBuilderOptions(
            ioptions, moptions, read_options, write_options,
            internal_comparator, &internal_tbl_prop_coll_factories,
            options.compression, options.compression_opts, kUnknownColumnFamily,
            column_family_name, level_, kUnknownNewestKeyTime),
        file_writer_.get()));

    for (const auto& kv : kv_map) {
      if (convert_to_internal_key_) {
        ParsedInternalKey ikey(kv.first, kMaxSequenceNumber, kTypeValue);
        std::string encoded;
        AppendInternalKey(&encoded, ikey);
        builder->Add(encoded, kv.second);
      } else {
        builder->Add(kv.first, kv.second);
      }
      EXPECT_OK(builder->status());
    }
    Status s = builder->Finish();
    EXPECT_OK(file_writer_->Flush(IOOptions()));
    EXPECT_TRUE(s.ok()) << s.ToString();

    EXPECT_EQ(TEST_GetSink()->contents().size(), builder->FileSize());

    // Open the table
    file_num_ = cur_file_num_++;

    return Reopen(ioptions, moptions);
  }

  InternalIterator* NewIterator(
      const SliceTransform* prefix_extractor) const override {
    InternalIterator* iter = table_reader_->NewIterator(
        read_options_, prefix_extractor, /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized);
    if (convert_to_internal_key_) {
      return new KeyConvertingIterator(iter);
    } else {
      return iter;
    }
  }

  uint64_t ApproximateOffsetOf(const Slice& key) const {
    const ReadOptions read_options;
    if (convert_to_internal_key_) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      const Slice skey = ikey.Encode();
      return table_reader_->ApproximateOffsetOf(
          read_options, skey, TableReaderCaller::kUncategorized);
    }
    return table_reader_->ApproximateOffsetOf(
        read_options, key, TableReaderCaller::kUncategorized);
  }

  virtual Status Reopen(const ImmutableOptions& ioptions,
                        const MutableCFOptions& moptions) {
    std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
        TEST_GetSink()->contents(), file_num_, ioptions.allow_mmap_reads));

    file_reader_.reset(new RandomAccessFileReader(std::move(source), "test"));
    return moptions.table_factory->NewTableReader(
        TableReaderOptions(ioptions, moptions.prefix_extractor,
                           moptions.compression_manager.get(), soptions,
                           *last_internal_comparator_,
                           0 /* block_protection_bytes_per_key */,
                           /*skip_filters*/ false,
                           /*immortal*/ false, false, level_,
                           &block_cache_tracer_, moptions.write_buffer_size, "",
                           file_num_, kNullUniqueId64x2, largest_seqno_),
        std::move(file_reader_), TEST_GetSink()->contents().size(),
        &table_reader_);
  }

  virtual TableReader* GetTableReader() { return table_reader_.get(); }

  bool AnywayDeleteIterator() const override {
    return convert_to_internal_key_;
  }

  void ResetTableReader() { table_reader_.reset(); }

  bool ConvertToInternalKey() { return convert_to_internal_key_; }

  test::StringSink* TEST_GetSink() {
    return static_cast<test::StringSink*>(file_writer_->writable_file());
  }

  BlockCacheTracer block_cache_tracer_;
  Env* env_;

 private:
  void Reset() {
    file_num_ = 0;
    table_reader_.reset();
    file_writer_.reset();
    file_reader_.reset();
  }

  const ReadOptions read_options_;
  uint64_t file_num_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<TableReader> table_reader_;
  SequenceNumber largest_seqno_;
  bool convert_to_internal_key_;
  int level_;

  TableConstructor() = delete;

  static uint64_t cur_file_num_;
  EnvOptions soptions;
};
uint64_t TableConstructor::cur_file_num_ = 1;

class MemTableConstructor : public Constructor {
 public:
  explicit MemTableConstructor(const Comparator* cmp, WriteBufferManager* wb)
      : Constructor(cmp),
        internal_comparator_(cmp),
        write_buffer_manager_(wb),
        table_factory_(new SkipListFactory) {
    options_.memtable_factory = table_factory_;
    ImmutableOptions ioptions(options_);
    memtable_ =
        new MemTable(internal_comparator_, ioptions, MutableCFOptions(options_),
                     wb, kMaxSequenceNumber, 0 /* column_family_id */,
                     nullptr /* flush_initiator */);
    memtable_->Ref();
  }
  ~MemTableConstructor() override { delete memtable_->Unref(); }
  Status FinishImpl(const Options&, const ImmutableOptions& ioptions,
                    const MutableCFOptions& /*moptions*/,
                    const BlockBasedTableOptions& /*table_options*/,
                    const InternalKeyComparator& /*internal_comparator*/,
                    const stl_wrappers::KVMap& kv_map) override {
    delete memtable_->Unref();
    ImmutableOptions mem_ioptions(ioptions);
    memtable_ = new MemTable(internal_comparator_, mem_ioptions,
                             MutableCFOptions(options_), write_buffer_manager_,
                             kMaxSequenceNumber, 0 /* column_family_id */,
                             nullptr /* flush_initiator */);
    memtable_->Ref();
    int seq = 1;
    for (const auto& kv : kv_map) {
      Status s = memtable_->Add(seq, kTypeValue, kv.first, kv.second,
                                nullptr /* kv_prot_info */);
      if (!s.ok()) {
        return s;
      }
      seq++;
    }
    return Status::OK();
  }
  InternalIterator* NewIterator(
      const SliceTransform* /*prefix_extractor*/) const override {
    return new KeyConvertingIterator(
        memtable_->NewIterator(ReadOptions(), /*seqno_to_time_mapping=*/nullptr,
                               &arena_, /*prefix_extractor=*/nullptr,
                               /*for_flush=*/false),
        true);
  }

  bool AnywayDeleteIterator() const override { return true; }

  bool IsArenaMode() const override { return true; }

 private:
  mutable Arena arena_;
  InternalKeyComparator internal_comparator_;
  Options options_;
  WriteBufferManager* write_buffer_manager_;
  MemTable* memtable_;
  std::shared_ptr<SkipListFactory> table_factory_;
};

class InternalIteratorFromIterator : public InternalIterator {
 public:
  explicit InternalIteratorFromIterator(Iterator* it) : it_(it) {}
  bool Valid() const override { return it_->Valid(); }
  void Seek(const Slice& target) override { it_->Seek(target); }
  void SeekForPrev(const Slice& target) override { it_->SeekForPrev(target); }
  void SeekToFirst() override { it_->SeekToFirst(); }
  void SeekToLast() override { it_->SeekToLast(); }
  void Next() override { it_->Next(); }
  void Prev() override { it_->Prev(); }
  Slice key() const override { return it_->key(); }
  Slice value() const override { return it_->value(); }
  Status status() const override { return it_->status(); }

 private:
  std::unique_ptr<Iterator> it_;
};

class DBConstructor : public Constructor {
 public:
  explicit DBConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp) {
    NewDB();
  }
  ~DBConstructor() override {}
  Status FinishImpl(const Options& /*options*/,
                    const ImmutableOptions& /*ioptions*/,
                    const MutableCFOptions& /*moptions*/,
                    const BlockBasedTableOptions& /*table_options*/,
                    const InternalKeyComparator& /*internal_comparator*/,
                    const stl_wrappers::KVMap& kv_map) override {
    db_.reset();
    NewDB();
    for (const auto& kv : kv_map) {
      WriteBatch batch;
      EXPECT_OK(batch.Put(kv.first, kv.second));
      EXPECT_TRUE(db_->Write(WriteOptions(), &batch).ok());
    }
    return Status::OK();
  }

  InternalIterator* NewIterator(
      const SliceTransform* /*prefix_extractor*/) const override {
    return new InternalIteratorFromIterator(db_->NewIterator(ReadOptions()));
  }

  DB* db() const override { return db_.get(); }

 private:
  void NewDB() {
    std::string name = test::PerThreadDBPath("table_testdb");

    Options options;
    options.comparator = comparator_;
    Status status = DestroyDB(name, options);
    ASSERT_TRUE(status.ok()) << status.ToString();

    options.create_if_missing = true;
    options.error_if_exists = true;
    options.write_buffer_size = 10000;  // Something small to force merging
    status = DB::Open(options, name, &db_);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  const Comparator* comparator_;
  std::unique_ptr<DB> db_;
};

enum TestType {
  BLOCK_BASED_TABLE_TEST,
  PLAIN_TABLE_SEMI_FIXED_PREFIX,
  PLAIN_TABLE_FULL_STR_PREFIX,
  PLAIN_TABLE_TOTAL_ORDER,
  BLOCK_TEST,
  MEMTABLE_TEST,
  DB_TEST
};

struct TestArgs {
  TestType type;
  bool reverse_compare;
  int restart_interval;
  CompressionType compression;
  uint32_t compression_parallel_threads;
  uint32_t format_version;
  bool use_mmap;
};

std::ostream& operator<<(std::ostream& os, const TestArgs& args) {
  os << "type: " << args.type << " reverse_compare: " << args.reverse_compare
     << " restart_interval: " << args.restart_interval
     << " compression: " << args.compression
     << " compression_parallel_threads: " << args.compression_parallel_threads
     << " format_version: " << args.format_version
     << " use_mmap: " << args.use_mmap;

  return os;
}

static std::vector<TestArgs> GenerateArgList() {
  std::vector<TestArgs> test_args;
  std::vector<TestType> test_types = {BLOCK_BASED_TABLE_TEST,
                                      PLAIN_TABLE_SEMI_FIXED_PREFIX,
                                      PLAIN_TABLE_FULL_STR_PREFIX,
                                      PLAIN_TABLE_TOTAL_ORDER,
                                      BLOCK_TEST,
                                      MEMTABLE_TEST,
                                      DB_TEST};
  std::vector<bool> reverse_compare_types = {false, true};
  std::vector<int> restart_intervals = {16, 1, 1024};
  std::vector<uint32_t> compression_parallel_threads = {1, 4};

  for (auto test_type : test_types) {
    for (auto reverse_compare : reverse_compare_types) {
      if (test_type == PLAIN_TABLE_SEMI_FIXED_PREFIX ||
          test_type == PLAIN_TABLE_FULL_STR_PREFIX ||
          test_type == PLAIN_TABLE_TOTAL_ORDER) {
        // Plain table doesn't use restart index or compression.
        TestArgs one_arg;
        one_arg.type = test_type;
        one_arg.reverse_compare = reverse_compare;
        one_arg.restart_interval = restart_intervals[0];
        one_arg.compression = kNoCompression;
        one_arg.compression_parallel_threads = 1;
        one_arg.format_version = 0;  // Plain tables use their own versioning
        one_arg.use_mmap = true;
        test_args.push_back(one_arg);
        one_arg.use_mmap = false;
        test_args.push_back(one_arg);
        continue;
      }

      for (auto restart_interval : restart_intervals) {
        for (auto compression_type : GetSupportedCompressions()) {
          for (auto num_threads : compression_parallel_threads) {
            // format_version = 7 changes some compression handling; 8 adds the
            // index value-delta escape and the common-key-prefix feature.
            for (uint32_t fv :
                 {kMinSupportedBbtFormatVersionForRead, 7U,
                  BlockBasedTableOptions::kLatestSupportedFormatVersion}) {
              TestArgs one_arg;
              one_arg.type = test_type;
              one_arg.reverse_compare = reverse_compare;
              one_arg.restart_interval = restart_interval;
              one_arg.compression = compression_type;
              one_arg.compression_parallel_threads = num_threads;
              one_arg.format_version = fv;
              one_arg.use_mmap = false;
              test_args.push_back(one_arg);
            }
          }
        }
      }
    }
  }
  return test_args;
}

// In order to make all tests run for plain table format, including
// those operating on empty keys, create a new prefix transformer which
// return fixed prefix if the slice is not shorter than the prefix length,
// and the full slice if it is shorter.
class FixedOrLessPrefixTransform : public SliceTransform {
 private:
  const size_t prefix_len_;

 public:
  explicit FixedOrLessPrefixTransform(size_t prefix_len)
      : prefix_len_(prefix_len) {}

  const char* Name() const override { return "rocksdb.FixedPrefix"; }

  Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    if (src.size() < prefix_len_) {
      return src;
    }
    return Slice(src.data(), prefix_len_);
  }

  bool InDomain(const Slice& /*src*/) const override { return true; }

  bool FullLengthEnabled(size_t* /*len*/) const override { return false; }
};

class HarnessTest : public testing::Test {
 public:
  explicit HarnessTest(const TestArgs& args)
      : args_(args),
        ioptions_(options_),
        moptions_(options_),
        write_buffer_(options_.db_write_buffer_size),
        support_prev_(true),
        only_support_prefix_seek_(false) {
    options_.compression = args_.compression;
    options_.compression_opts.parallel_threads =
        args_.compression_parallel_threads;
    // Use shorter block size for tests to exercise block boundary
    // conditions more.
    if (args_.reverse_compare) {
      options_.comparator = &reverse_key_comparator;
    }

    internal_comparator_.reset(
        new test::PlainInternalKeyComparator(options_.comparator));

    options_.allow_mmap_reads = args_.use_mmap;
    switch (args_.type) {
      case BLOCK_BASED_TABLE_TEST:
        table_options_.flush_block_policy_factory.reset(
            new FlushBlockBySizePolicyFactory());
        table_options_.block_size = 256;
        table_options_.block_restart_interval = args_.restart_interval;
        table_options_.index_block_restart_interval = args_.restart_interval;
        table_options_.format_version = args_.format_version;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;

      case PLAIN_TABLE_SEMI_FIXED_PREFIX:
        support_prev_ = false;
        only_support_prefix_seek_ = true;
        options_.prefix_extractor.reset(new FixedOrLessPrefixTransform(2));
        options_.table_factory.reset(NewPlainTableFactory());
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
      case PLAIN_TABLE_FULL_STR_PREFIX:
        support_prev_ = false;
        only_support_prefix_seek_ = true;
        options_.prefix_extractor.reset(NewNoopTransform());
        options_.table_factory.reset(NewPlainTableFactory());
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
      case PLAIN_TABLE_TOTAL_ORDER:
        support_prev_ = false;
        only_support_prefix_seek_ = false;
        options_.prefix_extractor = nullptr;

        {
          PlainTableOptions plain_table_options;
          plain_table_options.user_key_len = kPlainTableVariableLength;
          plain_table_options.bloom_bits_per_key = 0;
          plain_table_options.hash_table_ratio = 0;

          options_.table_factory.reset(
              NewPlainTableFactory(plain_table_options));
        }
        constructor_.reset(new TableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */));
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
      case BLOCK_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(new BlockConstructor(options_.comparator));
        break;
      case MEMTABLE_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(
            new MemTableConstructor(options_.comparator, &write_buffer_));
        break;
      case DB_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new BlockBasedTableFactory(table_options_));
        constructor_.reset(new DBConstructor(options_.comparator));
        break;
    }
    ioptions_ = ImmutableOptions(options_);
    moptions_ = MutableCFOptions(options_);
  }

  void Add(const std::string& key, const std::string& value) {
    constructor_->Add(key, value);
  }

  void Test(Random* rnd) {
    std::vector<std::string> keys;
    stl_wrappers::KVMap data;
    constructor_->Finish(options_, ioptions_, moptions_, table_options_,
                         *internal_comparator_, &keys, &data);

    TestForwardScan(keys, data);
    if (support_prev_) {
      TestBackwardScan(keys, data);
    }
    TestRandomAccess(rnd, keys, data);
  }

  void TestForwardScan(const std::vector<std::string>& /*keys*/,
                       const stl_wrappers::KVMap& data) {
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    for (stl_wrappers::KVMap::const_iterator model_iter = data.begin();
         model_iter != data.end(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Next();
      ASSERT_OK(iter->status());
    }
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  void TestBackwardScan(const std::vector<std::string>& /*keys*/,
                        const stl_wrappers::KVMap& data) {
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToLast();
    ASSERT_OK(iter->status());
    for (stl_wrappers::KVMap::const_reverse_iterator model_iter = data.rbegin();
         model_iter != data.rend(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Prev();
      ASSERT_OK(iter->status());
    }
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  void TestRandomAccess(Random* rnd, const std::vector<std::string>& keys,
                        const stl_wrappers::KVMap& data) {
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    stl_wrappers::KVMap::const_iterator model_iter = data.begin();
    if (kVerbose) {
      fprintf(stderr, "---\n");
    }
    for (int i = 0; i < 200; i++) {
      const int toss = rnd->Uniform(support_prev_ ? 5 : 3);
      switch (toss) {
        case 0: {
          if (iter->Valid()) {
            if (kVerbose) {
              fprintf(stderr, "Next\n");
            }
            iter->Next();
            ASSERT_OK(iter->status());
            ++model_iter;
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 1: {
          if (kVerbose) {
            fprintf(stderr, "SeekToFirst\n");
          }
          iter->SeekToFirst();
          ASSERT_OK(iter->status());
          model_iter = data.begin();
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 2: {
          std::string key = PickRandomKey(rnd, keys);
          model_iter = data.lower_bound(key);
          if (kVerbose) {
            fprintf(stderr, "Seek '%s'\n", EscapeString(key).c_str());
          }
          iter->Seek(Slice(key));
          ASSERT_OK(iter->status());
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 3: {
          if (iter->Valid()) {
            if (kVerbose) {
              fprintf(stderr, "Prev\n");
            }
            iter->Prev();
            ASSERT_OK(iter->status());
            if (model_iter == data.begin()) {
              model_iter = data.end();  // Wrap around to invalid value
            } else {
              --model_iter;
            }
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 4: {
          if (kVerbose) {
            fprintf(stderr, "SeekToLast\n");
          }
          iter->SeekToLast();
          ASSERT_OK(iter->status());
          if (keys.empty()) {
            model_iter = data.end();
          } else {
            std::string last = data.rbegin()->first;
            model_iter = data.lower_bound(last);
          }
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
      }
    }
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  std::string ToString(const stl_wrappers::KVMap& data,
                       const stl_wrappers::KVMap::const_iterator& it) {
    if (it == data.end()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const stl_wrappers::KVMap& data,
                       const stl_wrappers::KVMap::const_reverse_iterator& it) {
    if (it == data.rend()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const InternalIterator* it) {
    if (!it->Valid()) {
      return "END";
    } else {
      return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
    }
  }

  std::string PickRandomKey(Random* rnd, const std::vector<std::string>& keys) {
    if (keys.empty()) {
      return "foo";
    } else {
      const int index = rnd->Uniform(static_cast<int>(keys.size()));
      std::string result = keys[index];
      switch (rnd->Uniform(support_prev_ ? 3 : 1)) {
        case 0:
          // Return an existing key
          break;
        case 1: {
          // Attempt to return something smaller than an existing key
          if (result.size() > 0 && result[result.size() - 1] > '\0' &&
              (!only_support_prefix_seek_ ||
               options_.prefix_extractor->Transform(result).size() <
                   result.size())) {
            result[result.size() - 1]--;
          }
          break;
        }
        case 2: {
          // Return something larger than an existing key
          Increment(options_.comparator, &result);
          break;
        }
      }
      return result;
    }
  }

  // Returns nullptr if not running against a DB
  DB* db() const { return constructor_->db(); }

 private:
  TestArgs args_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions moptions_;
  BlockBasedTableOptions table_options_;
  std::unique_ptr<Constructor> constructor_;
  WriteBufferManager write_buffer_;
  bool support_prev_;
  bool only_support_prefix_seek_;
  std::shared_ptr<InternalKeyComparator> internal_comparator_;
};

class ParameterizedHarnessTest : public testing::WithParamInterface<TestArgs>,
                                 public HarnessTest {
 public:
  ParameterizedHarnessTest() : HarnessTest(GetParam()) {}
};

INSTANTIATE_TEST_CASE_P(TableTest, ParameterizedHarnessTest,
                        ::testing::ValuesIn(GenerateArgList()));

class DBHarnessTest : public HarnessTest {
 public:
  DBHarnessTest()
      : HarnessTest(TestArgs{DB_TEST, /* reverse_compare */ false,
                             /* restart_interval */ 16, kNoCompression,
                             /* compression_parallel_threads */ 1,
                             /* format_version */ 0, /* use_mmap */ false}) {}
};

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
            (unsigned long long)(val), (unsigned long long)(low),
            (unsigned long long)(high));
  }
  return result;
}

// Tests against all kinds of tables
class TableTest : public testing::Test {
 public:
  const InternalKeyComparator& GetPlainInternalComparator(
      const Comparator* comp) {
    if (!plain_internal_comparator) {
      plain_internal_comparator.reset(
          new test::PlainInternalKeyComparator(comp));
    }
    return *plain_internal_comparator;
  }
  void IndexTest(BlockBasedTableOptions table_options);

 private:
  std::unique_ptr<InternalKeyComparator> plain_internal_comparator;
};

class GeneralTableTest : public TableTest {};
class BlockBasedTableTestBase : public TableTest {};
class BlockBasedTableTest : public BlockBasedTableTestBase,
                            virtual public ::testing::WithParamInterface<
                                std::tuple<uint32_t, size_t, size_t, bool>> {
 public:
  BlockBasedTableTest() : format_(std::get<0>(GetParam())) {
    env_ = Env::Default();
  }

  BlockBasedTableOptions GetBlockBasedTableOptions() {
    BlockBasedTableOptions options;
    options.format_version = format_;
    auto param = GetParam();
    options.super_block_alignment_size = std::get<1>(param);
    options.super_block_alignment_space_overhead_ratio = std::get<2>(param);
    // separate_key_value_in_data_block
    options.separate_key_value_in_data_block = std::get<3>(param);
    return options;
  }

  void SetupTracingTest(TableConstructor* c) {
    test_path_ = test::PerThreadDBPath("block_based_table_tracing_test");
    EXPECT_OK(env_->CreateDir(test_path_));
    trace_file_path_ = test_path_ + "/block_cache_trace_file";

    BlockCacheTraceWriterOptions trace_writer_opt;
    BlockCacheTraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    EXPECT_OK(NewFileTraceWriter(env_, EnvOptions(), trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    // Always return Status::OK().
    ASSERT_OK(c->block_cache_tracer_.StartTrace(
        trace_opt, std::move(block_cache_trace_writer)));

    {
      InternalKey internal_key(auto_add_key1, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      c->Add(encoded_key, kDummyValue);
    }
    {
      InternalKey internal_key(auto_add_key2, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      c->Add(encoded_key, kDummyValue);
    }
  }

  void VerifyBlockAccessTrace(
      TableConstructor* c,
      const std::vector<BlockCacheTraceRecord>& expected_records) {
    c->block_cache_tracer_.EndTrace();

    {
      std::unique_ptr<TraceReader> trace_reader;
      Status s = NewFileTraceReader(env_, EnvOptions(), trace_file_path_,
                                    &trace_reader);
      EXPECT_OK(s);
      BlockCacheTraceReader reader(std::move(trace_reader));
      BlockCacheTraceHeader header;
      EXPECT_OK(reader.ReadHeader(&header));
      uint32_t index = 0;
      while (s.ok()) {
        SCOPED_TRACE("expected_records[" + std::to_string(index) + "]");
        BlockCacheTraceRecord access;
        s = reader.ReadAccess(&access);
        if (!s.ok()) {
          break;
        }
        ASSERT_LT(index, expected_records.size());
        EXPECT_NE("", access.block_key);
        EXPECT_EQ(access.block_type, expected_records[index].block_type);
        EXPECT_GT(access.block_size, 0);
        EXPECT_EQ(access.caller, expected_records[index].caller);
        EXPECT_EQ(access.no_insert, expected_records[index].no_insert);
        EXPECT_EQ(access.is_cache_hit, expected_records[index].is_cache_hit);
        EXPECT_EQ(access.get_id, expected_records[index].get_id);
        // The well-populated cases
        if (access.caller == TableReaderCaller::kUserGet ||
            (access.caller == TableReaderCaller::kUserMultiGet &&
             access.block_type == TraceType::kBlockTraceDataBlock)) {
          EXPECT_EQ(access.referenced_key,
                    expected_records[index].referenced_key);
          EXPECT_EQ(access.get_from_user_specified_snapshot,
                    expected_records[index].get_from_user_specified_snapshot);
          if (access.block_type == TraceType::kBlockTraceDataBlock) {
            EXPECT_GT(access.referenced_data_size, 0);
            EXPECT_GT(access.num_keys_in_block, 0);
            if (access.caller == TableReaderCaller::kUserMultiGet) {
              // Test num_keys_in_block estimate, assuming default restart
              // interval of 16 and just one interval.
              // Rounding depends on get_id.
              if (access.get_id & 1) {
                EXPECT_EQ(access.num_keys_in_block, 9);
              } else {
                EXPECT_EQ(access.num_keys_in_block, 8);
              }
            }
            EXPECT_EQ(access.referenced_key_exist_in_block,
                      expected_records[index].referenced_key_exist_in_block);
          }
        } else {
          EXPECT_EQ(access.referenced_key, "");
          EXPECT_FALSE(access.get_from_user_specified_snapshot);
          EXPECT_EQ(access.referenced_data_size, 0);
          EXPECT_EQ(access.num_keys_in_block, 0);
          EXPECT_FALSE(access.referenced_key_exist_in_block);
        }
        index++;
      }
      EXPECT_EQ(index, expected_records.size());
    }
    EXPECT_OK(env_->DeleteFile(trace_file_path_));
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

 protected:
  uint64_t IndexUncompressedHelper(bool indexCompress);
  const std::string auto_add_key1 = "aak01";
  const std::string auto_add_key2 = "aak02";

 private:
  uint32_t format_;
  Env* env_;
  std::string trace_file_path_;
  std::string test_path_;
};
class PlainTableTest : public TableTest {};
class TablePropertyTest : public testing::Test {};
class BBTTailPrefetchTest : public TableTest {};

// The helper class to test the file checksum
class FileChecksumTestHelper {
 public:
  FileChecksumTestHelper(bool convert_to_internal_key = false)
      : convert_to_internal_key_(convert_to_internal_key) {}
  ~FileChecksumTestHelper() = default;

  void CreateWritableFile() {
    sink_ = new test::StringSink();
    std::unique_ptr<FSWritableFile> holder(sink_);
    file_writer_.reset(new WritableFileWriter(
        std::move(holder), "" /* don't care */, FileOptions()));
  }

  void SetFileChecksumGenerator(FileChecksumGenerator* checksum_generator) {
    if (file_writer_ != nullptr) {
      file_writer_->TEST_SetFileChecksumGenerator(checksum_generator);
    } else {
      delete checksum_generator;
    }
  }

  WritableFileWriter* GetFileWriter() { return file_writer_.get(); }

  Status ResetTableBuilder(std::unique_ptr<TableBuilder>&& builder) {
    assert(builder != nullptr);
    table_builder_ = std::move(builder);
    return Status::OK();
  }

  void AddKVtoKVMap(int num_entries) {
    Random rnd(test::RandomSeed());
    for (int i = 0; i < num_entries; i++) {
      std::string v = rnd.RandomString(100);
      kv_map_[test::RandomKey(&rnd, 20)] = v;
    }
  }

  Status WriteKVAndFlushTable() {
    for (const auto& kv : kv_map_) {
      if (convert_to_internal_key_) {
        ParsedInternalKey ikey(kv.first, kMaxSequenceNumber, kTypeValue);
        std::string encoded;
        AppendInternalKey(&encoded, ikey);
        table_builder_->Add(encoded, kv.second);
      } else {
        table_builder_->Add(kv.first, kv.second);
      }
      EXPECT_TRUE(table_builder_->status().ok());
    }
    Status s = table_builder_->Finish();
    EXPECT_OK(file_writer_->Flush(IOOptions()));
    EXPECT_OK(s);

    EXPECT_EQ(sink_->contents().size(), table_builder_->FileSize());
    return s;
  }

  std::string GetFileChecksum() {
    EXPECT_OK(file_writer_->Close(IOOptions()));
    return table_builder_->GetFileChecksum();
  }

  const char* GetFileChecksumFuncName() {
    return table_builder_->GetFileChecksumFuncName();
  }

  Status CalculateFileChecksum(FileChecksumGenerator* file_checksum_generator,
                               std::string* checksum) {
    assert(file_checksum_generator != nullptr);
    cur_file_num_ = checksum_file_num_++;
    test::StringSink* ss_rw =
        static_cast<test::StringSink*>(file_writer_->writable_file());
    std::unique_ptr<FSRandomAccessFile> source(
        new test::StringSource(ss_rw->contents()));
    file_reader_.reset(new RandomAccessFileReader(std::move(source), "test"));

    std::unique_ptr<char[]> scratch(new char[2048]);
    Slice result;
    uint64_t offset = 0;
    Status s;
    s = file_reader_->Read(offset, 2048, IOOptions(), &result, scratch.get(),
                           nullptr);
    if (!s.ok()) {
      return s;
    }
    while (result.size() != 0) {
      file_checksum_generator->Update(scratch.get(), result.size());
      offset += static_cast<uint64_t>(result.size());
      s = file_reader_->Read(offset, 2048, IOOptions(), &result, scratch.get(),
                             nullptr);
      if (!s.ok()) {
        return s;
      }
    }
    EXPECT_EQ(offset, static_cast<uint64_t>(table_builder_->FileSize()));
    file_checksum_generator->Finalize();
    *checksum = file_checksum_generator->GetChecksum();
    return Status::OK();
  }

 private:
  bool convert_to_internal_key_;
  uint64_t cur_file_num_ = 0;
  std::unique_ptr<WritableFileWriter> file_writer_;
  std::unique_ptr<RandomAccessFileReader> file_reader_;
  std::unique_ptr<TableBuilder> table_builder_;
  stl_wrappers::KVMap kv_map_;
  test::StringSink* sink_ = nullptr;

  static uint64_t checksum_file_num_;
};

uint64_t FileChecksumTestHelper::checksum_file_num_ = 1;

INSTANTIATE_TEST_CASE_P(
    FormatVersions, BlockBasedTableTest,
    testing::Combine(testing::ValuesIn(test::kFooterFormatVersionsToTest),
                     testing::Values(0, 128 * 1024, 512 * 1024,
                                     2 * 1024 * 1024),
                     testing::Values(2048, 32, 128), testing::Bool()));

// This test serves as the living tutorial for the prefix scan of user collected
// properties.
TEST_F(TablePropertyTest, PrefixScanTest) {
  UserCollectedProperties props{
      {"num.111.1", "1"}, {"num.111.2", "2"}, {"num.111.3", "3"},
      {"num.333.1", "1"}, {"num.333.2", "2"}, {"num.333.3", "3"},
      {"num.555.1", "1"}, {"num.555.2", "2"}, {"num.555.3", "3"},
  };

  // prefixes that exist
  for (const std::string prefix : {"num.111", "num.333", "num.555"}) {
    int num = 0;
    for (auto pos = props.lower_bound(prefix);
         pos != props.end() &&
         pos->first.compare(0, prefix.size(), prefix) == 0;
         ++pos) {
      ++num;
      auto key = prefix + "." + std::to_string(num);
      ASSERT_EQ(key, pos->first);
      ASSERT_EQ(std::to_string(num), pos->second);
    }
    ASSERT_EQ(3, num);
  }

  // prefixes that don't exist
  for (const std::string prefix :
       {"num.000", "num.222", "num.444", "num.666"}) {
    auto pos = props.lower_bound(prefix);
    ASSERT_TRUE(pos == props.end() ||
                pos->first.compare(0, prefix.size(), prefix) != 0);
  }
}

namespace {
struct TestIds {
  UniqueId64x3 internal_id;
  UniqueId64x3 external_id;
};

inline bool operator==(const TestIds& lhs, const TestIds& rhs) {
  return lhs.internal_id == rhs.internal_id &&
         lhs.external_id == rhs.external_id;
}

std::ostream& operator<<(std::ostream& os, const TestIds& ids) {
  return os << std::hex << "{{{ 0x" << ids.internal_id[0] << "U, 0x"
            << ids.internal_id[1] << "U, 0x" << ids.internal_id[2]
            << "U }}, {{ 0x" << ids.external_id[0] << "U, 0x"
            << ids.external_id[1] << "U, 0x" << ids.external_id[2] << "U }}}";
}

TestIds GetUniqueId(TableProperties* tp, std::unordered_set<uint64_t>* seen,
                    const std::string& db_id, const std::string& db_session_id,
                    uint64_t file_number) {
  // First test session id logic
  if (db_session_id.size() == 20) {
    uint64_t upper;
    uint64_t lower;
    EXPECT_OK(DecodeSessionId(db_session_id, &upper, &lower));
    EXPECT_EQ(EncodeSessionId(upper, lower), db_session_id);
  }

  // Get external using public API
  tp->db_id = db_id;
  tp->db_session_id = db_session_id;
  tp->orig_file_number = file_number;
  TestIds t;
  {
    std::string euid;
    EXPECT_OK(GetExtendedUniqueIdFromTableProperties(*tp, &euid));
    EXPECT_EQ(euid.size(), 24U);
    t.external_id[0] = DecodeFixed64(euid.data());
    t.external_id[1] = DecodeFixed64(&euid[8]);
    t.external_id[2] = DecodeFixed64(&euid[16]);

    std::string uid;
    EXPECT_OK(GetUniqueIdFromTableProperties(*tp, &uid));
    EXPECT_EQ(uid.size(), 16U);
    EXPECT_EQ(uid, euid.substr(0, 16));
    EXPECT_EQ(t.external_id[0], DecodeFixed64(uid.data()));
    EXPECT_EQ(t.external_id[1], DecodeFixed64(&uid[8]));
  }
  // All these should be effectively random
  EXPECT_TRUE(seen->insert(t.external_id[0]).second);
  EXPECT_TRUE(seen->insert(t.external_id[1]).second);
  EXPECT_TRUE(seen->insert(t.external_id[2]).second);

  // Get internal with internal API
  EXPECT_OK(GetSstInternalUniqueId(db_id, db_session_id, file_number,
                                   &t.internal_id));
  EXPECT_NE(t.internal_id, kNullUniqueId64x3);

  // Verify relationship
  UniqueId64x3 tmp = t.internal_id;
  InternalUniqueIdToExternal(&tmp);
  EXPECT_EQ(tmp, t.external_id);
  ExternalUniqueIdToInternal(&tmp);
  EXPECT_EQ(tmp, t.internal_id);

  // And 128-bit internal version
  UniqueId64x2 tmp2{};
  EXPECT_OK(GetSstInternalUniqueId(db_id, db_session_id, file_number, &tmp2));
  EXPECT_NE(tmp2, kNullUniqueId64x2);

  EXPECT_EQ(tmp2[0], t.internal_id[0]);
  EXPECT_EQ(tmp2[1], t.internal_id[1]);
  InternalUniqueIdToExternal(&tmp2);
  EXPECT_EQ(tmp2[0], t.external_id[0]);
  EXPECT_EQ(tmp2[1], t.external_id[1]);
  ExternalUniqueIdToInternal(&tmp2);
  EXPECT_EQ(tmp2[0], t.internal_id[0]);
  EXPECT_EQ(tmp2[1], t.internal_id[1]);

  return t;
}
}  // namespace

TEST_F(TablePropertyTest, UniqueIdsSchemaAndQuality) {
  // To ensure the computation only depends on the expected entries, we set
  // the rest randomly
  TableProperties tp;
  TEST_SetRandomTableProperties(&tp);

  // DB id is normally RFC-4122
  const std::string db_id1 = "7265b6eb-4e42-4aec-86a4-0dc5e73a228d";
  // Allow other forms of DB id
  const std::string db_id2 = "1728000184588763620";
  const std::string db_id3 = "x";

  // DB session id is normally 20 chars in base-36, but 13 to 24 chars
  // is ok, roughly 64 to 128 bits.
  const std::string ses_id1 = "ABCDEFGHIJ0123456789";
  // Same trailing 13 digits
  const std::string ses_id2 = "HIJ0123456789";
  const std::string ses_id3 = "0123ABCDEFGHIJ0123456789";
  // Different trailing 12 digits
  const std::string ses_id4 = "ABCDEFGH888888888888";
  // And change length
  const std::string ses_id5 = "ABCDEFGHIJ012";
  const std::string ses_id6 = "ABCDEFGHIJ0123456789ABCD";

  using T = TestIds;
  std::unordered_set<uint64_t> seen;
  // Establish a stable schema for the unique IDs. These values must not
  // change for existing table files.
  // (Note: parens needed for macro parsing, extra braces needed for some
  // compilers.)
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id1, 1),
      T({{{0x61d7dcf415d9cf19U, 0x160d77aae90757fdU, 0x907f41dfd90724ffU}},
         {{0xf0bd230365df7464U, 0xca089303f3648eb4U, 0x4b44f7e7324b2817U}}}));
  // Only change internal_id[1] with file number
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id1, 2),
      T({{{0x61d7dcf415d9cf19U, 0x160d77aae90757feU, 0x907f41dfd90724ffU}},
         {{0xf13fdf7adcfebb6dU, 0x97cd2226cc033ea2U, 0x198c438182091f0eU}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id1, 123456789),
      T({{{0x61d7dcf415d9cf19U, 0x160d77aaee5c9ae9U, 0x907f41dfd90724ffU}},
         {{0x81fbcebe1ac6c4f0U, 0x6b14a64cfdc0f1c4U, 0x7d8fb6eaf18edbb3U}}}));
  // Change internal_id[1] and internal_id[2] with db_id
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id2, ses_id1, 1),
      T({{{0x61d7dcf415d9cf19U, 0xf89c471f572f0d25U, 0x1f0f2a5eb0e6257eU}},
         {{0x7f1d01d453616991U, 0x32ddf2afec804ab2U, 0xd10a1ee2f0c7d9c1U}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id3, ses_id1, 1),
      T({{{0x61d7dcf415d9cf19U, 0xfed297a8154a57d0U, 0x8b931b9cdebd9e8U}},
         {{0x62b2f43183f6894bU, 0x897ff2b460eefad1U, 0xf4ec189fb2d15e04U}}}));
  // Keeping same last 13 digits of ses_id keeps same internal_id[0]
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id2, 1),
      T({{{0x61d7dcf415d9cf19U, 0x5f6cc4fa2d528c8U, 0x7b70845d5bfb5446U}},
         {{0x96d1c83ffcc94266U, 0x82663eac0ec6e14aU, 0x94a88b49678b77f6U}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id3, 1),
      T({{{0x61d7dcf415d9cf19U, 0xfc7232879db37ea2U, 0xc0378d74ea4c89cdU}},
         {{0xdf2ef57e98776905U, 0xda5b31c987da833bU, 0x79c1b4bd0a9e760dU}}}));
  // Changing last 12 digits of ses_id only changes internal_id[0]
  // (vs. db_id1, ses_id1, 1)
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id4, 1),
      T({{{0x4f07cc0d003a83a8U, 0x160d77aae90757fdU, 0x907f41dfd90724ffU}},
         {{0xbcf85336a9f71f04U, 0x4f2949e2f3adb60dU, 0x9ca0def976abfa10U}}}));
  // ses_id can change everything.
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id5, 1),
      T({{{0x94b8768e43f87ce6U, 0xc2559653ac4e7c93U, 0xde6dff6bbb1223U}},
         {{0x5a9537af681817fbU, 0x1afcd1fecaead5eaU, 0x767077ad9ebe0008U}}}));
  EXPECT_EQ(
      GetUniqueId(&tp, &seen, db_id1, ses_id6, 1),
      T({{{0x43cfb0ffa3b710edU, 0x263c580426406a1bU, 0xfacc91379a80d29dU}},
         {{0xfa90547d84cb1cdbU, 0x2afe99c641992d4aU, 0x205b7f7b60e51cc2U}}}));

  // Now verify more thoroughly that any small change in inputs completely
  // changes external unique id.
  // (Relying on 'seen' checks etc. in GetUniqueId)
  std::string db_id = "00000000-0000-0000-0000-000000000000";
  std::string ses_id = "000000000000000000000000";
  uint64_t file_num = 1;
  // change db_id
  for (size_t i = 0; i < db_id.size(); ++i) {
    if (db_id[i] == '-') {
      continue;
    }
    for (char alt : std::string("123456789abcdef")) {
      db_id[i] = alt;
      GetUniqueId(&tp, &seen, db_id, ses_id, file_num);
    }
    db_id[i] = '0';
  }
  // change ses_id
  for (size_t i = 0; i < ses_id.size(); ++i) {
    for (char alt : std::string("123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")) {
      ses_id[i] = alt;
      GetUniqueId(&tp, &seen, db_id, ses_id, file_num);
    }
    ses_id[i] = '0';
  }
  // change file_num
  for (int i = 1; i < 64; ++i) {
    GetUniqueId(&tp, &seen, db_id, ses_id, file_num << i);
  }

  // Verify that "all zeros" in first 128 bits is equivalent for internal and
  // external IDs. This way, as long as we avoid "all zeros" in internal IDs,
  // we avoid it in external IDs.
  {
    UniqueId64x3 id1{{0, 0, Random::GetTLSInstance()->Next64()}};
    UniqueId64x3 id2 = id1;
    InternalUniqueIdToExternal(&id1);
    EXPECT_EQ(id1, id2);
    ExternalUniqueIdToInternal(&id2);
    EXPECT_EQ(id1, id2);
  }
}

namespace {
void SetGoodTableProperties(TableProperties* tp) {
  // To ensure the computation only depends on the expected entries, we set
  // the rest randomly
  TEST_SetRandomTableProperties(tp);
  tp->db_id = "7265b6eb-4e42-4aec-86a4-0dc5e73a228d";
  tp->db_session_id = "ABCDEFGHIJ0123456789";
  tp->orig_file_number = 1;
}
}  // namespace

TEST_F(TablePropertyTest, UniqueIdHumanStrings) {
  TableProperties tp;
  SetGoodTableProperties(&tp);

  std::string tmp;
  EXPECT_OK(GetExtendedUniqueIdFromTableProperties(tp, &tmp));
  EXPECT_EQ(tmp,
            (std::string{{'\x64', '\x74', '\xdf', '\x65', '\x03', '\x23',
                          '\xbd', '\xf0', '\xb4', '\x8e', '\x64', '\xf3',
                          '\x03', '\x93', '\x08', '\xca', '\x17', '\x28',
                          '\x4b', '\x32', '\xe7', '\xf7', '\x44', '\x4b'}}));
  EXPECT_EQ(UniqueIdToHumanString(tmp),
            "6474DF650323BDF0-B48E64F3039308CA-17284B32E7F7444B");

  EXPECT_OK(GetUniqueIdFromTableProperties(tp, &tmp));
  EXPECT_EQ(UniqueIdToHumanString(tmp), "6474DF650323BDF0-B48E64F3039308CA");

  // including zero padding
  tmp = std::string(24U, '\0');
  tmp[15] = '\x12';
  tmp[23] = '\xAB';
  EXPECT_EQ(UniqueIdToHumanString(tmp),
            "0000000000000000-0000000000000012-00000000000000AB");

  // And shortened
  tmp = std::string(20U, '\0');
  tmp[5] = '\x12';
  tmp[10] = '\xAB';
  tmp[17] = '\xEF';
  EXPECT_EQ(UniqueIdToHumanString(tmp),
            "0000000000120000-0000AB0000000000-00EF0000");

  tmp.resize(16);
  EXPECT_EQ(UniqueIdToHumanString(tmp), "0000000000120000-0000AB0000000000");

  tmp.resize(11);
  EXPECT_EQ(UniqueIdToHumanString(tmp), "0000000000120000-0000AB");

  tmp.resize(6);
  EXPECT_EQ(UniqueIdToHumanString(tmp), "000000000012");

  // Also internal IDs to human string
  UniqueId64x3 euid = {12345, 678, 9};
  EXPECT_EQ(InternalUniqueIdToHumanString(&euid), "{12345,678,9}");

  UniqueId64x2 uid = {1234, 567890};
  EXPECT_EQ(InternalUniqueIdToHumanString(&uid), "{1234,567890}");
}

TEST_F(TablePropertyTest, UniqueIdsFailure) {
  TableProperties tp;
  std::string tmp;

  // Missing DB id
  SetGoodTableProperties(&tp);
  tp.db_id = "";
  EXPECT_TRUE(GetUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
  EXPECT_TRUE(
      GetExtendedUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());

  // Missing session id
  SetGoodTableProperties(&tp);
  tp.db_session_id = "";
  EXPECT_TRUE(GetUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
  EXPECT_TRUE(
      GetExtendedUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());

  // Missing file number
  SetGoodTableProperties(&tp);
  tp.orig_file_number = 0;
  EXPECT_TRUE(GetUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
  EXPECT_TRUE(
      GetExtendedUniqueIdFromTableProperties(tp, &tmp).IsNotSupported());
}

// This test include all the basic checks except those for index size and block
// size, which will be conducted in separated unit tests.
TEST_P(BlockBasedTableTest, BasicBlockBasedTableProperties) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);

  c.Add("a1", "val1");
  c.Add("b2", "val2");
  c.Add("c3", "val3");
  c.Add("d4", "val4");
  c.Add("e5", "val5");
  c.Add("f6", "val6");
  c.Add("g7", "val7");
  c.Add("h8", "val8");
  c.Add("j9", "val9");
  uint64_t diff_internal_user_bytes = 9 * 8;  // 8 is seq size, 9 k-v totally

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  ASSERT_EQ(
      options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSION_REJECTED), 0);

  auto& props = *c.GetTableReader()->GetTableProperties();
  ASSERT_EQ(kvmap.size(), props.num_entries);

  auto raw_key_size = kvmap.size() * 2ul;
  auto raw_value_size = kvmap.size() * 4ul;

  ASSERT_EQ(raw_key_size + diff_internal_user_bytes, props.raw_key_size);
  ASSERT_EQ(raw_value_size, props.raw_value_size);
  ASSERT_EQ(1ul, props.num_data_blocks);
  ASSERT_EQ("", props.filter_policy_name);  // no filter policy is used

  // Verify data size.
  BlockBuilder block_builder(
      1 /* block_restart_interval */, true /* use_delta_encoding */,
      false /* use_value_delta_encoding */,
      BlockBasedTableOptions::kDataBlockBinarySearch /* index_type */,
      0.75 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
      true /* persist_user_defined_timestamps */, false /* is_user_key */,
      table_options.separate_key_value_in_data_block, nullptr /* statistics */,
      -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);
  for (const auto& item : kvmap) {
    block_builder.Add(item.first, item.second);
  }
  Slice content = block_builder.Finish();
  ASSERT_EQ(content.size() + BlockBasedTable::kBlockTrailerSize +
                diff_internal_user_bytes,
            props.data_size);
  c.ResetTableReader();
}

namespace {
// A total-order comparator that behaves exactly like the built-in bytewise
// comparator but is a distinct object (distinct Name). The common-prefix
// feature treats it as "non-bytewise" (pointer identity), so the writer still
// strips (keys are byte-clustered) while the reader must use the full-key
// reconstruction seek path.
class ForwardingBytewiseComparator : public Comparator {
 public:
  const char* Name() const override { return "test.ForwardingBytewise"; }
  int Compare(const Slice& a, const Slice& b) const override {
    return BytewiseComparator()->Compare(a, b);
  }
  void FindShortestSeparator(std::string* start,
                             const Slice& limit) const override {
    BytewiseComparator()->FindShortestSeparator(start, limit);
  }
  void FindShortSuccessor(std::string* key) const override {
    BytewiseComparator()->FindShortSuccessor(key);
  }
};
const Comparator* ForwardingBytewise() {
  static ForwardingBytewiseComparator cmp;
  return &cmp;
}

Options MakeCommonPrefixOptions(
    const Comparator* ucmp,
    BlockBasedTableOptions::OptimizeKeyCommonPrefix mode,
    BlockBasedTableOptions* table_options_out) {
  Options options;
  options.comparator = ucmp;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.format_version = 8;
  table_options.optimize_key_common_prefix = mode;
  table_options.block_restart_interval = 16;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  *table_options_out = table_options;
  return options;
}

// Adds prefix-heavy keys and finishes `c`, returning its data_size. All the
// referenced options must outlive later reader use, because the table reader
// holds references to `ioptions`/`moptions`.
uint64_t FinishCommonPrefixTable(TableConstructor* c, const std::string& prefix,
                                 int num_keys, const Options& options,
                                 const ImmutableOptions& ioptions,
                                 const MutableCFOptions& moptions,
                                 const BlockBasedTableOptions& table_options,
                                 const InternalKeyComparator& icmp,
                                 stl_wrappers::KVMap* kvmap) {
  for (int i = 0; i < num_keys; i++) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%06d", i);
    c->Add(prefix + buf, "value_" + std::to_string(i));
  }
  std::vector<std::string> keys;
  c->Finish(options, ioptions, moptions, table_options, icmp, &keys, kvmap);
  return c->GetTableReader()->GetTableProperties()->data_size;
}
}  // namespace

// Focused test for optimize_key_common_prefix (format_version 8): prefix-heavy
// keys should make data blocks smaller when the optimization is on, and reads
// must stay correct (including Seek targets that straddle the common prefix).
TEST_F(GeneralTableTest, OptimizeKeyCommonPrefix) {
  const std::string kPrefix = "this_is_a_long_shared_user_key_prefix_";
  constexpr int kNumKeys = 2000;
  const Comparator* ucmp = BytewiseComparator();
  const InternalKeyComparator& icmp = GetPlainInternalComparator(ucmp);

  BlockBasedTableOptions tbo_on;
  BlockBasedTableOptions tbo_off;
  Options options_on = MakeCommonPrefixOptions(
      ucmp, BlockBasedTableOptions::OptimizeKeyCommonPrefix::kIfFastSeek,
      &tbo_on);
  Options options_off = MakeCommonPrefixOptions(
      ucmp, BlockBasedTableOptions::OptimizeKeyCommonPrefix::kDisabled,
      &tbo_off);
  ImmutableOptions ioptions_on(options_on);
  MutableCFOptions moptions_on(options_on);
  ImmutableOptions ioptions_off(options_off);
  MutableCFOptions moptions_off(options_off);

  TableConstructor c_on(ucmp, true /* convert_to_internal_key */);
  TableConstructor c_off(ucmp, true /* convert_to_internal_key */);
  stl_wrappers::KVMap kv_on;
  stl_wrappers::KVMap kv_off;
  uint64_t size_on =
      FinishCommonPrefixTable(&c_on, kPrefix, kNumKeys, options_on, ioptions_on,
                              moptions_on, tbo_on, icmp, &kv_on);
  uint64_t size_off = FinishCommonPrefixTable(
      &c_off, kPrefix, kNumKeys, options_off, ioptions_off, moptions_off,
      tbo_off, icmp, &kv_off);

  // The optimization engaged and shrank the (prefix-heavy) data blocks.
  ASSERT_LT(size_on, size_off);

  auto* reader = c_on.GetTableReader();
  ReadOptions ro;
  ro.total_order_seek = true;
  std::unique_ptr<InternalIterator> iter(reader->NewIterator(
      ro, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // Full forward iteration matches every stored key/value.
  auto expect = kv_on.begin();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ASSERT_TRUE(expect != kv_on.end());
    ASSERT_EQ(expect->first, ExtractUserKey(iter->key()).ToString());
    ASSERT_EQ(expect->second, iter->value().ToString());
    ++expect;
  }
  ASSERT_OK(iter->status());
  ASSERT_TRUE(expect == kv_on.end());

  auto user_key = [&](const Slice& internal_key) {
    return ExtractUserKey(internal_key).ToString();
  };

  // Seek to an existing key.
  iter->Seek(
      InternalKey(kPrefix + "001234", kMaxSequenceNumber, kTypeValue).Encode());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kPrefix + "001234", user_key(iter->key()));

  // Missing key that still shares the common prefix lands on the next key.
  iter->Seek(InternalKey(kPrefix + "001234x", kMaxSequenceNumber, kTypeValue)
                 .Encode());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kPrefix + "001235", user_key(iter->key()));

  // Target diverging below the common prefix sorts before all keys.
  iter->Seek(InternalKey("aaaa", kMaxSequenceNumber, kTypeValue).Encode());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kPrefix + "000000", user_key(iter->key()));

  // Target diverging above the common prefix sorts after all keys.
  iter->Seek(InternalKey("zzzz", kMaxSequenceNumber, kTypeValue).Encode());
  ASSERT_OK(iter->status());
  ASSERT_FALSE(iter->Valid());

  // SeekForPrev to a missing key lands on the previous key.
  iter->SeekForPrev(
      InternalKey(kPrefix + "001234x", kMaxSequenceNumber, kTypeValue)
          .Encode());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kPrefix + "001234", user_key(iter->key()));

  c_on.ResetTableReader();
  c_off.ResetTableReader();
}

// optimize_key_common_prefix=kEnabled strips data blocks even for a
// non-bytewise comparator; the reader reconstructs full keys (no suffix-seek
// shortcut).
TEST_F(GeneralTableTest, OptimizeKeyCommonPrefixNonBytewise) {
  const std::string kPrefix = "this_is_a_long_shared_user_key_prefix_";
  constexpr int kNumKeys = 2000;
  const Comparator* ucmp = ForwardingBytewise();
  const InternalKeyComparator& icmp = GetPlainInternalComparator(ucmp);

  BlockBasedTableOptions tbo_on;
  BlockBasedTableOptions tbo_off;
  Options options_on = MakeCommonPrefixOptions(
      ucmp, BlockBasedTableOptions::OptimizeKeyCommonPrefix::kEnabled, &tbo_on);
  Options options_off = MakeCommonPrefixOptions(
      ucmp, BlockBasedTableOptions::OptimizeKeyCommonPrefix::kDisabled,
      &tbo_off);
  ImmutableOptions ioptions_on(options_on);
  MutableCFOptions moptions_on(options_on);
  ImmutableOptions ioptions_off(options_off);
  MutableCFOptions moptions_off(options_off);

  TableConstructor c_on(ucmp, true /* convert_to_internal_key */);
  TableConstructor c_off(ucmp, true /* convert_to_internal_key */);
  stl_wrappers::KVMap kv_on;
  stl_wrappers::KVMap kv_off;
  uint64_t size_on =
      FinishCommonPrefixTable(&c_on, kPrefix, kNumKeys, options_on, ioptions_on,
                              moptions_on, tbo_on, icmp, &kv_on);
  uint64_t size_off = FinishCommonPrefixTable(
      &c_off, kPrefix, kNumKeys, options_off, ioptions_off, moptions_off,
      tbo_off, icmp, &kv_off);

  // kEnabled strips even for a non-bytewise comparator.
  ASSERT_LT(size_on, size_off);

  auto* reader = c_on.GetTableReader();
  ReadOptions ro;
  ro.total_order_seek = true;
  std::unique_ptr<InternalIterator> iter(reader->NewIterator(
      ro, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // Full forward iteration reconstructs every key/value correctly.
  auto expect = kv_on.begin();
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ASSERT_TRUE(expect != kv_on.end());
    ASSERT_EQ(expect->first, ExtractUserKey(iter->key()).ToString());
    ASSERT_EQ(expect->second, iter->value().ToString());
    ++expect;
  }
  ASSERT_OK(iter->status());
  ASSERT_TRUE(expect == kv_on.end());

  auto user_key = [&](const Slice& internal_key) {
    return ExtractUserKey(internal_key).ToString();
  };

  // Seek (reconstruction path): existing key, missing-with-shared-prefix key,
  // and SeekForPrev.
  iter->Seek(
      InternalKey(kPrefix + "001234", kMaxSequenceNumber, kTypeValue).Encode());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kPrefix + "001234", user_key(iter->key()));

  iter->Seek(InternalKey(kPrefix + "001234x", kMaxSequenceNumber, kTypeValue)
                 .Encode());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kPrefix + "001235", user_key(iter->key()));

  iter->SeekForPrev(
      InternalKey(kPrefix + "001234x", kMaxSequenceNumber, kTypeValue)
          .Encode());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(kPrefix + "001234", user_key(iter->key()));

  c_on.ResetTableReader();
  c_off.ResetTableReader();
}

#ifdef SNAPPY
uint64_t BlockBasedTableTest::IndexUncompressedHelper(bool compressed) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  constexpr size_t kNumKeys = 10000;

  for (size_t k = 0; k < kNumKeys; ++k) {
    c.Add("key" + std::to_string(k), "val" + std::to_string(k));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kSnappyCompression;
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  table_options.enable_index_compression = compressed;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  c.ResetTableReader();
  return options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
}
TEST_P(BlockBasedTableTest, IndexUncompressed) {
  uint64_t tbl1_compressed_cnt = IndexUncompressedHelper(true);
  uint64_t tbl2_compressed_cnt = IndexUncompressedHelper(false);
  // Normally, enabling index compression compresses exactly one more block (the
  // index block). At format_version >= 8 (without super block alignment) the
  // index block uses the common user-key prefix section, which strips shared
  // prefixes from the index separators. The resulting index block can be small
  // enough that compression no longer clears the storage threshold, so it may
  // be stored uncompressed even when index compression is enabled. Allow that
  // (0 or 1 extra compressed block) in that case; keep the exact invariant for
  // every other format.
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  const bool index_uses_common_prefix =
      table_options.format_version >= 8 &&
      table_options.super_block_alignment_size == 0;
  if (index_uses_common_prefix) {
    EXPECT_GE(tbl1_compressed_cnt, tbl2_compressed_cnt);
    EXPECT_LE(tbl1_compressed_cnt, tbl2_compressed_cnt + 1);
  } else {
    // tbl1_compressed_cnt should include 1 index block
    EXPECT_EQ(tbl2_compressed_cnt + 1, tbl1_compressed_cnt);
  }
}
#endif  // SNAPPY

TEST_P(BlockBasedTableTest, BlockBasedTableProperties2) {
  TableConstructor c(&reverse_key_comparator,
                     true /* convert_to_internal_key_ */);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  for (CompressionType ct : {kNoCompression, kSnappyCompression}) {
    if (!Snappy_Supported() && ct == kSnappyCompression) {
      continue;
    }
    Options options;
    options.compression = ct;
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Add("blah", std::string(200, 'x'));  // something to compress
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);

    auto& props = *c.GetTableReader()->GetTableProperties();

    // Default comparator
    ASSERT_EQ("leveldb.BytewiseComparator", props.comparator_name);
    // No merge operator
    ASSERT_EQ("nullptr", props.merge_operator_name);
    // No prefix extractor
    ASSERT_EQ("nullptr", props.prefix_extractor_name);
    // No property collectors
    ASSERT_EQ("[]", props.property_collectors_names);
    // No filter policy is used
    ASSERT_EQ("", props.filter_policy_name);
    // Compression type == that set:
    if (FormatVersionUsesCompressionManagerName(table_options.format_version)) {
      ASSERT_EQ(ct == kNoCompression ? ";;" : "BuiltinV2;01;",
                props.compression_name);
    } else {
      ASSERT_EQ(ct == kNoCompression ? "NoCompression" : "Snappy",
                props.compression_name);
    }
    c.ResetTableReader();
  }

  {
    Options options;
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.comparator = &reverse_key_comparator;
    options.merge_operator = MergeOperators::CreateUInt64AddOperator();
    options.prefix_extractor.reset(NewNoopTransform());
    options.table_properties_collector_factories.emplace_back(
        new DummyPropertiesCollectorFactory1());
    options.table_properties_collector_factories.emplace_back(
        new DummyPropertiesCollectorFactory2());

    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);

    auto& props = *c.GetTableReader()->GetTableProperties();

    ASSERT_EQ("rocksdb.ReverseBytewiseComparator", props.comparator_name);
    ASSERT_EQ("UInt64AddOperator", props.merge_operator_name);
    ASSERT_EQ("rocksdb.Noop", props.prefix_extractor_name);
    ASSERT_EQ(
        "[DummyPropertiesCollectorFactory1,DummyPropertiesCollectorFactory2]",
        props.property_collectors_names);
    ASSERT_EQ("", props.filter_policy_name);  // no filter policy is used
    c.ResetTableReader();
  }
}

TEST_P(BlockBasedTableTest, RangeDelBlock) {
  TableConstructor c(BytewiseComparator());
  std::vector<std::string> keys = {"1pika", "2chu"};
  std::vector<std::string> vals = {"p", "c"};

  std::vector<RangeTombstone> expected_tombstones = {
      {"1pika", "2chu", 0},
      {"2chu", "c", 1},
      {"2chu", "c", 0},
      {"c", "p", 0},
  };

  for (int i = 0; i < 2; i++) {
    RangeTombstone t(keys[i], vals[i], i);
    std::pair<InternalKey, Slice> p = t.Serialize();
    c.Add(p.first.Encode().ToString(), p.second);
  }

  std::vector<std::string> sorted_keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  std::unique_ptr<InternalKeyComparator> internal_cmp(
      new InternalKeyComparator(options.comparator));
  c.Finish(options, ioptions, moptions, table_options, *internal_cmp,
           &sorted_keys, &kvmap);

  for (int j = 0; j < 2; ++j) {
    std::unique_ptr<InternalIterator> iter(
        c.GetTableReader()->NewRangeTombstoneIterator(ReadOptions()));
    if (j > 0) {
      // For second iteration, delete the table reader object and verify the
      // iterator can still access its metablock's range tombstones.
      c.ResetTableReader();
    }
    ASSERT_FALSE(iter->Valid());
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    for (size_t i = 0; i < expected_tombstones.size(); i++) {
      ASSERT_TRUE(iter->Valid());
      ParsedInternalKey parsed_key;
      ASSERT_OK(
          ParseInternalKey(iter->key(), &parsed_key, true /* log_err_key */));
      RangeTombstone t(parsed_key, iter->value());
      const auto& expected_t = expected_tombstones[i];
      ASSERT_EQ(t.start_key_, expected_t.start_key_);
      ASSERT_EQ(t.end_key_, expected_t.end_key_);
      ASSERT_EQ(t.seq_, expected_t.seq_);
      iter->Next();
    }
    ASSERT_TRUE(!iter->Valid());
  }
}

TEST_P(BlockBasedTableTest, FilterPolicyNameProperties) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("a1", "val1");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  auto& props = *c.GetTableReader()->GetTableProperties();
  ASSERT_EQ(table_options.filter_policy->Name(), props.filter_policy_name);
  c.ResetTableReader();
}

//
// BlockBasedTableTest::PrefetchTest
//
void AssertKeysInCache(BlockBasedTable* table_reader,
                       const std::vector<std::string>& keys_in_cache,
                       const std::vector<std::string>& keys_not_in_cache,
                       bool convert = false) {
  if (convert) {
    for (const auto& key : keys_in_cache) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
    }
    for (const auto& key : keys_not_in_cache) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
    }
  } else {
    for (const auto& key : keys_in_cache) {
      ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), key));
    }
    for (const auto& key : keys_not_in_cache) {
      ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), key));
    }
  }
}

void PrefetchRange(TableConstructor* c, Options* opt,
                   BlockBasedTableOptions* table_options, const char* key_begin,
                   const char* key_end,
                   const std::vector<std::string>& keys_in_cache,
                   const std::vector<std::string>& keys_not_in_cache,
                   const Status expected_status = Status::OK()) {
  // reset the cache and reopen the table
  table_options->block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt->table_factory.reset(NewBlockBasedTableFactory(*table_options));
  const ImmutableOptions ioptions2(*opt);
  const MutableCFOptions moptions(*opt);
  ASSERT_OK(c->Reopen(ioptions2, moptions));

  // prefetch
  auto* table_reader = dynamic_cast<BlockBasedTable*>(c->GetTableReader());
  Status s;
  std::unique_ptr<Slice> begin;
  std::unique_ptr<Slice> end;
  std::unique_ptr<InternalKey> i_begin;
  std::unique_ptr<InternalKey> i_end;
  if (key_begin != nullptr) {
    if (c->ConvertToInternalKey()) {
      i_begin.reset(new InternalKey(key_begin, kMaxSequenceNumber, kTypeValue));
      begin.reset(new Slice(i_begin->Encode()));
    } else {
      begin.reset(new Slice(key_begin));
    }
  }
  if (key_end != nullptr) {
    if (c->ConvertToInternalKey()) {
      i_end.reset(new InternalKey(key_end, kMaxSequenceNumber, kTypeValue));
      end.reset(new Slice(i_end->Encode()));
    } else {
      end.reset(new Slice(key_end));
    }
  }
  const ReadOptions read_options;
  s = table_reader->Prefetch(read_options, begin.get(), end.get());

  ASSERT_TRUE(s.code() == expected_status.code());

  // assert our expectation in cache warmup
  AssertKeysInCache(table_reader, keys_in_cache, keys_not_in_cache,
                    c->ConvertToInternalKey());
  c->ResetTableReader();
}

TEST_P(BlockBasedTableTest, PrefetchTest) {
  // The purpose of this test is to test the prefetching operation built into
  // BlockBasedTable.
  Options opt;
  std::unique_ptr<InternalKeyComparator> ikc;
  ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
  opt.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_size = 1024;
  // big enough so we don't ever lose cached values.
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewBlockBasedTableFactory(table_options));

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(opt);
  const MutableCFOptions moptions(opt);
  c.Finish(opt, ioptions, moptions, table_options, *ikc, &keys, &kvmap);
  c.ResetTableReader();

  // We get the following data spread :
  //
  // Data block         Index
  // ========================
  // [ k01 k02 k03 ]    k03
  // [ k04         ]    k04
  // [ k05         ]    k05
  // [ k06 k07     ]    k07

  // Simple
  PrefetchRange(&c, &opt, &table_options,
                /*key_begin=*/"k01", /*key_end=*/"k05",
                /*keys_in_cache=*/{"k01", "k02", "k03", "k04", "k05"},
                /*keys_not_in_cache=*/{"k06", "k07"});
  PrefetchRange(&c, &opt, &table_options, "k01", "k01", {"k01", "k02", "k03"},
                {"k04", "k05", "k06", "k07"});
  // odd
  PrefetchRange(&c, &opt, &table_options, "a", "z",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k00", "k00", {"k01", "k02", "k03"},
                {"k04", "k05", "k06", "k07"});
  // Edge cases
  PrefetchRange(&c, &opt, &table_options, "k00", "k06",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k00", "zzz",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  // null keys
  PrefetchRange(&c, &opt, &table_options, nullptr, nullptr,
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k04", nullptr,
                {"k04", "k05", "k06", "k07"}, {"k01", "k02", "k03"});
  PrefetchRange(&c, &opt, &table_options, nullptr, "k05",
                {"k01", "k02", "k03", "k04", "k05"}, {"k06", "k07"});
  // invalid
  PrefetchRange(&c, &opt, &table_options, "k06", "k00", {}, {},
                Status::InvalidArgument(Slice("k06 "), Slice("k07")));
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TotalOrderSeekOnHashIndex) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  for (int i = 0; i <= 4; ++i) {
    Options options;
    // Make each key/value an individual block
    table_options.block_size = 64;
    switch (i) {
      case 0:
        // Binary search index
        table_options.index_type = BlockBasedTableOptions::kBinarySearch;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        break;
      case 1:
        // Hash search index
        table_options.index_type = BlockBasedTableOptions::kHashSearch;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        options.prefix_extractor.reset(NewFixedPrefixTransform(4));
        break;
      case 2:
        // Hash search index with filter policy
        table_options.index_type = BlockBasedTableOptions::kHashSearch;
        table_options.filter_policy.reset(NewBloomFilterPolicy(10));
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        options.prefix_extractor.reset(NewFixedPrefixTransform(4));
        break;
      case 3:
        // Two-level index
        table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        break;
      case 4:
        // Binary search with first key
        table_options.index_type =
            BlockBasedTableOptions::kBinarySearchWithFirstKey;
        options.table_factory.reset(new BlockBasedTableFactory(table_options));
        break;
    }

    TableConstructor c(BytewiseComparator(),
                       true /* convert_to_internal_key_ */);
    c.Add("aaaa1", std::string('a', 56));
    c.Add("bbaa1", std::string('a', 56));
    c.Add("cccc1", std::string('a', 56));
    c.Add("bbbb1", std::string('a', 56));
    c.Add("baaa1", std::string('a', 56));
    c.Add("abbb1", std::string('a', 56));
    c.Add("cccc2", std::string('a', 56));
    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;
    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);
    auto props = c.GetTableReader()->GetTableProperties();
    ASSERT_EQ(7u, props->num_data_blocks);
    auto* reader = c.GetTableReader();
    ReadOptions ro;
    ro.total_order_seek = true;
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        ro, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    iter->Seek(InternalKey("b", 0, kTypeValue).Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("baaa1", ExtractUserKey(iter->key()).ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbaa1", ExtractUserKey(iter->key()).ToString());

    iter->Seek(InternalKey("bb", 0, kTypeValue).Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbaa1", ExtractUserKey(iter->key()).ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbbb1", ExtractUserKey(iter->key()).ToString());

    iter->Seek(InternalKey("bbb", 0, kTypeValue).Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bbbb1", ExtractUserKey(iter->key()).ToString());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("cccc1", ExtractUserKey(iter->key()).ToString());
  }
}

TEST_P(BlockBasedTableTest, NoopTransformSeek) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewNoopTransform());

  TableConstructor c(options.comparator);
  // To tickle the PrefixMayMatch bug it is important that the
  // user-key is a single byte so that the index key exactly matches
  // the user-key.
  InternalKey key("a", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "b");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  auto* reader = c.GetTableReader();
  for (int i = 0; i < 2; ++i) {
    ReadOptions ro;
    ro.total_order_seek = (i == 0);
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        ro, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    iter->Seek(key.Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", ExtractUserKey(iter->key()).ToString());
  }
}

TEST_P(BlockBasedTableTest, SkipPrefixBloomFilter) {
  // if DB is opened with a prefix extractor of a different name,
  // prefix bloom is skipped when read the file
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(2));
  table_options.whole_key_filtering = false;

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  TableConstructor c(options.comparator);
  InternalKey key("abcdefghijk", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "test");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);
  // TODO(Zhongyi): update test to use MutableCFOptions
  options.prefix_extractor.reset(NewFixedPrefixTransform(9));
  const ImmutableOptions new_ioptions(options);
  const MutableCFOptions new_moptions(options);
  ASSERT_OK(c.Reopen(new_ioptions, new_moptions));
  auto reader = c.GetTableReader();
  ReadOptions read_options;
  std::unique_ptr<InternalIterator> db_iter(reader->NewIterator(
      read_options, new_moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // Test point lookup
  // only one kv
  for (auto& kv : kvmap) {
    db_iter->Seek(kv.first);
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(db_iter->key(), kv.first);
    ASSERT_EQ(db_iter->value(), kv.second);
  }
}

TEST_P(BlockBasedTableTest, BadChecksumType) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));

  TableConstructor c(options.comparator);
  InternalKey key("abc", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "test");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  // Corrupt checksum type (123 is invalid)
  auto& sink = *c.TEST_GetSink();
  size_t len = sink.contents_.size();
  ASSERT_EQ(sink.contents_[len - Footer::kNewVersionsEncodedLength],
            table_options.checksum);
  sink.contents_[len - Footer::kNewVersionsEncodedLength] = char{123};

  // (Re-)Open table file with bad checksum type
  const ImmutableOptions new_ioptions(options);
  const MutableCFOptions new_moptions(options);
  Status s = c.Reopen(new_ioptions, new_moptions);
  ASSERT_NOK(s);
  // "test" is file name
  ASSERT_EQ(s.ToString(),
            "Corruption: Corrupt or unsupported checksum type: 123 in test");
}

TEST_P(BlockBasedTableTest, ReservedBitInDataBlockFooter) {
  // Bit 30 of the data block footer is the reserved "extended metadata present"
  // escape (format_version >= 8). No feature defines it yet, so setting it must
  // be detected as corruption rather than silently misread.
  //
  // We construct a block directly rather than going through the full table
  // iterator path to avoid issues with iterator error handling.

  // Build a simple data block.
  BlockBuilder builder(BlockBuilder::ForMetaBlock{}, 16 /* restart_interval */);
  InternalKey key("abc", 1, kTypeValue);
  builder.Add(key.Encode(), "test_value");
  Slice block_contents = builder.Finish();
  std::string block_data = block_contents.ToString();

  // The footer is the last 4 bytes - set the reserved extended-metadata bit
  // (bit 30).
  ASSERT_GE(block_data.size(), sizeof(uint32_t));
  size_t footer_offset = block_data.size() - sizeof(uint32_t);
  uint32_t footer = DecodeFixed32(block_data.data() + footer_offset);
  footer |= (1u << 30);  // Set the reserved extended-metadata bit
  EncodeFixed32(&block_data[footer_offset], footer);

  // Try to construct a Block from the data with the reserved bit set. The
  // footer decode must reject it.
  BlockContents contents(std::move(block_data));
  Block block(std::move(contents), 0 /* read_amp_bytes_per_bit */);

  // Block should have size() == 0 indicating error
  ASSERT_EQ(block.size(), 0u);

  // Try to get an iterator - it should be invalid with corruption status
  DataBlockIter iter;
  block.NewDataIterator(BytewiseComparator(), kMaxSequenceNumber, &iter,
                        /*stats=*/nullptr, /*block_contents_pinned=*/false);
  ASSERT_FALSE(iter.Valid());
  ASSERT_EQ(iter.status().code(), Status::kCorruption)
      << iter.status().ToString();
}

class BuiltinChecksumTest : public testing::Test,
                            public testing::WithParamInterface<ChecksumType> {};

INSTANTIATE_TEST_CASE_P(SupportedChecksums, BuiltinChecksumTest,
                        testing::ValuesIn(GetSupportedChecksums()));

namespace {
std::string ChecksumAsString(const std::string& data,
                             ChecksumType checksum_type) {
  uint32_t v = ComputeBuiltinChecksum(checksum_type, data.data(), data.size());

  // Verify consistency with other function
  if (data.size() >= 1) {
    EXPECT_EQ(v, ComputeBuiltinChecksumWithLastByte(
                     checksum_type, data.data(), data.size() - 1, data.back()));
  }
  // Little endian as in file
  std::array<char, 4> raw_bytes;
  EncodeFixed32(raw_bytes.data(), v);
  return Slice(raw_bytes.data(), raw_bytes.size()).ToString(/*hex*/ true);
}

std::string ChecksumAsString(std::string* data, char new_last_byte,
                             ChecksumType checksum_type) {
  data->back() = new_last_byte;
  return ChecksumAsString(*data, checksum_type);
}
}  // namespace

// Make sure that checksum values don't change in later versions, even if
// consistent within current version.
TEST_P(BuiltinChecksumTest, ChecksumSchemas) {
  // Trailing 'x' chars will be replaced by compression type. Specifically,
  // the first byte of a block trailer is compression type, which is part of
  // the checksum input. This test does not deal with storing or parsing
  // checksums from the trailer (next 4 bytes of trailer).
  std::string b0 = "x";
  std::string b1 = "This is a short block!x";
  std::string b2;
  for (int i = 0; i < 100; ++i) {
    b2.append("This is a long block!");
  }
  b2.append("x");

  std::string empty;

  char ct1 = kNoCompression;
  char ct2 = kSnappyCompression;
  char ct3 = kZSTD;

  ChecksumType t = GetParam();
  switch (t) {
    case kNoChecksum:
      EXPECT_EQ(ChecksumAsString(empty, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "00000000");
      break;
    case kCRC32c:
      EXPECT_EQ(ChecksumAsString(empty, t), "D8EA82A2");
      EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "D28F2549");
      EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "052B2843");
      EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "46F8F711");
      EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "583F0355");
      EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "2F9B0A57");
      EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "ECE7DA1D");
      EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "943EF0AB");
      EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "43A2EDB1");
      EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "00E53D63");
      break;
    case kxxHash:
      EXPECT_EQ(ChecksumAsString(empty, t), "055DCC02");
      EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "3EB065CF");
      EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "31F79238");
      EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "320D2E00");
      EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "4A2E5FB0");
      EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "0BD9F652");
      EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "B4107E50");
      EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "20F4D4BA");
      EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "8F1A1F99");
      EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "A191A338");
      break;
    case kxxHash64:
      EXPECT_EQ(ChecksumAsString(empty, t), "99E9D851");
      EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "682705DB");
      EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "30E7211B");
      EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "B7BB58E8");
      EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "B74655EF");
      EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "B6C8BBBE");
      EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "AED9E3B4");
      EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "0D4999FE");
      EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "F5932423");
      EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "6B31BAB1");
      break;
    case kXXH3:
      EXPECT_EQ(ChecksumAsString(empty, t), "00000000");
      EXPECT_EQ(ChecksumAsString(&b0, ct1, t), "C294D338");
      EXPECT_EQ(ChecksumAsString(&b0, ct2, t), "1B174353");
      EXPECT_EQ(ChecksumAsString(&b0, ct3, t), "2D0E20C8");
      EXPECT_EQ(ChecksumAsString(&b1, ct1, t), "B37FB5E6");
      EXPECT_EQ(ChecksumAsString(&b1, ct2, t), "6AFC258D");
      EXPECT_EQ(ChecksumAsString(&b1, ct3, t), "5CE54616");
      EXPECT_EQ(ChecksumAsString(&b2, ct1, t), "FA2D482E");
      EXPECT_EQ(ChecksumAsString(&b2, ct2, t), "23AED845");
      EXPECT_EQ(ChecksumAsString(&b2, ct3, t), "15B7BBDE");
      break;
    default:
      // Force this test to be updated on new ChecksumTypes
      assert(false);
      break;
  }
}

TEST_P(BuiltinChecksumTest, ChecksumZeroInputs) {
  // Verify that no reasonably sized "all zeros" inputs produce "all zeros"
  // output. Otherwise, "wiped" data could appear to be well-formed.
  // Assuming essentially random assignment of output values, the likelihood
  // of encountering checksum == 0 for an input not specifically crafted is
  // 1 in 4 billion.
  if (GetParam() == kNoChecksum) {
    return;
  }
  // "Thorough" case is too slow for continouous testing
  bool thorough = getenv("ROCKSDB_THOROUGH_CHECKSUM_TEST") != nullptr;
  // Verified through 10M
  size_t kMaxZerosLen = thorough ? 10000000 : 20000;
  std::string zeros(kMaxZerosLen, '\0');

  for (size_t len = 0; len < kMaxZerosLen; ++len) {
    if (thorough && (len & 0xffffU) == 0) {
      fprintf(stderr, "t=%u len=%u\n", (unsigned)GetParam(), (unsigned)len);
    }
    uint32_t v = ComputeBuiltinChecksum(GetParam(), zeros.data(), len);
    if (v == 0U) {
      // One exception case:
      if (GetParam() == kXXH3 && len == 0) {
        // This is not a big deal because assuming the block length is known
        // from the block handle, which comes from a checksum-verified block,
        // there is nothing to corrupt in a zero-length block. And when there
        // is a block trailer with compression byte (as in block-based table),
        // zero length checksummed data never arises.
        continue;
      }
      // Only compute this on failure
      SCOPED_TRACE("len=" + std::to_string(len));
      ASSERT_NE(v, 0U);
    }
  }
}

void AddInternalKey(TableConstructor* c, const std::string& prefix,
                    std::string value = "v", int /*suffix_len*/ = 800) {
  static Random rnd(1023);
  InternalKey k(prefix + rnd.RandomString(800), 0, kTypeValue);
  c->Add(k.Encode().ToString(), value);
}

void TableTest::IndexTest(BlockBasedTableOptions table_options) {
  TableConstructor c(BytewiseComparator());

  // keys with prefix length 3, make sure the key/value is big enough to fill
  // one block
  AddInternalKey(&c, "0015");
  AddInternalKey(&c, "0035");

  AddInternalKey(&c, "0054");
  AddInternalKey(&c, "0055");

  AddInternalKey(&c, "0056");
  AddInternalKey(&c, "0057");

  AddInternalKey(&c, "0058");
  AddInternalKey(&c, "0075");

  AddInternalKey(&c, "0076");
  AddInternalKey(&c, "0095");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  table_options.block_size = 1700;
  table_options.block_cache = NewLRUCache(1024, 4);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  auto reader = c.GetTableReader();

  auto props = reader->GetTableProperties();
  ASSERT_EQ(5u, props->num_data_blocks);

  // TODO(Zhongyi): update test to use MutableCFOptions
  ReadOptions read_options;
  std::unique_ptr<InternalIterator> index_iter(reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // -- Find keys do not exist, but have common prefix.
  std::vector<std::string> prefixes = {"001", "003", "005", "007", "009"};
  std::vector<std::string> lower_bound = {
      keys[0], keys[1], keys[2], keys[7], keys[9],
  };

  // find the lower bound of the prefix
  for (size_t i = 0; i < prefixes.size(); ++i) {
    index_iter->Seek(InternalKey(prefixes[i], 0, kTypeValue).Encode());
    ASSERT_OK(index_iter->status());
    ASSERT_TRUE(index_iter->Valid());

    // seek the first element in the block
    ASSERT_EQ(lower_bound[i], index_iter->key().ToString());
    ASSERT_EQ("v", index_iter->value().ToString());
  }

  // find the upper bound of prefixes
  std::vector<std::string> upper_bound = {
      keys[1],
      keys[2],
      keys[7],
      keys[9],
  };

  // find existing keys
  for (const auto& item : kvmap) {
    auto ukey = ExtractUserKey(item.first).ToString();
    index_iter->Seek(ukey);

    // ASSERT_OK(regular_iter->status());
    ASSERT_OK(index_iter->status());

    // ASSERT_TRUE(regular_iter->Valid());
    ASSERT_TRUE(index_iter->Valid());

    ASSERT_EQ(item.first, index_iter->key().ToString());
    ASSERT_EQ(item.second, index_iter->value().ToString());
  }

  for (size_t i = 0; i < prefixes.size(); ++i) {
    // the key is greater than any existing keys.
    auto key = prefixes[i] + "9";
    index_iter->Seek(InternalKey(key, 0, kTypeValue).Encode());

    ASSERT_TRUE(index_iter->status().ok() || index_iter->status().IsNotFound());
    ASSERT_TRUE(!index_iter->status().IsNotFound() || !index_iter->Valid());
    if (i == prefixes.size() - 1) {
      // last key
      ASSERT_TRUE(!index_iter->Valid());
    } else {
      ASSERT_TRUE(index_iter->Valid());
      // seek the first element in the block
      ASSERT_EQ(upper_bound[i], index_iter->key().ToString());
      ASSERT_EQ("v", index_iter->value().ToString());
    }
  }

  // find keys with prefix that don't match any of the existing prefixes.
  std::vector<std::string> non_exist_prefixes = {"002", "004", "006", "008"};
  for (const auto& prefix : non_exist_prefixes) {
    index_iter->Seek(InternalKey(prefix, 0, kTypeValue).Encode());
    // regular_iter->Seek(prefix);

    ASSERT_OK(index_iter->status());
    // Seek to non-existing prefixes should yield either invalid, or a
    // key with prefix greater than the target.
    if (index_iter->Valid()) {
      Slice ukey = ExtractUserKey(index_iter->key());
      Slice ukey_prefix = options.prefix_extractor->Transform(ukey);
      ASSERT_TRUE(BytewiseComparator()->Compare(prefix, ukey_prefix) < 0);
    }
  }
  for (const auto& prefix : non_exist_prefixes) {
    index_iter->SeekForPrev(InternalKey(prefix, 0, kTypeValue).Encode());
    // regular_iter->Seek(prefix);

    ASSERT_OK(index_iter->status());
    // Seek to non-existing prefixes should yield either invalid, or a
    // key with prefix greater than the target.
    if (index_iter->Valid()) {
      Slice ukey = ExtractUserKey(index_iter->key());
      Slice ukey_prefix = options.prefix_extractor->Transform(ukey);
      ASSERT_TRUE(BytewiseComparator()->Compare(prefix, ukey_prefix) > 0);
    }
  }

  {
    // Test reseek case. It should impact partitioned index more.
    ReadOptions ro;
    ro.total_order_seek = true;
    std::unique_ptr<InternalIterator> index_iter2(reader->NewIterator(
        ro, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    // Things to cover in partitioned index:
    // 1. Both of Seek() and SeekToLast() has optimization to prevent
    //    rereek leaf index block if it remains to the same one, and
    //    they reuse the same variable.
    // 2. When Next() or Prev() is called, the block moves, so the
    //    optimization should kick in only with the current one.
    index_iter2->Seek(InternalKey("0055", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0055", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0055", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0055", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0095", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0095", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Prev();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Seek(InternalKey("0075", 0, kTypeValue).Encode());
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0075", index_iter2->key().ToString().substr(0, 4));

    index_iter2->Next();
    ASSERT_TRUE(index_iter2->Valid());
    index_iter2->Next();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));

    index_iter2->SeekToLast();
    ASSERT_TRUE(index_iter2->Valid());
    ASSERT_EQ("0095", index_iter2->key().ToString().substr(0, 4));
  }

  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, BinaryIndexTestBinarySearch) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  table_options.index_block_search_type = BlockBasedTableOptions::kBinary;
  IndexTest(table_options);
}

TEST_P(BlockBasedTableTest, BinaryIndexTestInterpolationSearch) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  table_options.index_block_search_type =
      BlockBasedTableOptions::kInterpolation;
  IndexTest(table_options);
}

TEST_P(BlockBasedTableTest, HashIndexTest) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  IndexTest(table_options);
}

TEST_P(BlockBasedTableTest, PartitionIndexTest) {
  const int max_index_keys = 5;
  const int est_max_index_key_value_size = 32;
  const int est_max_index_size = max_index_keys * est_max_index_key_value_size;
  for (int i = 1; i <= est_max_index_size + 1; i++) {
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
    table_options.metadata_block_size = i;
    IndexTest(table_options);
  }
}

// The index of index partitions and the index of filter partitions are written
// by builders separate from the per-partition index builder, and their values
// are only delta encoded when index_block_restart_interval > 1 (with the
// default of 1 every entry is a restart and carries a full handle, so the
// value-delta codec is never exercised). Cover that path here -- including at
// the unpublished draft format_version 8, where the value-delta codec reserves
// an in-value escape codepoint -- so that a codec mismatch between either
// top-level index writer and its reader is caught.
TEST_P(BlockBasedTableTest, PartitionedIndexAndFilterValueDelta) {
  // BlockBasedTableTest is parameterized over kFooterFormatVersionsToTest,
  // which includes the unpublished draft format_version 8. (table_test opts in
  // globally via TEST_AllowUnsupportedFormatVersion in main().)
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  table_options.partition_filters = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  // Small blocks and partitions so there are many index and filter partitions,
  // hence many top-level index entries.
  table_options.block_size = 512;
  table_options.metadata_block_size = 128;
  // > 1 so top-level index values are actually delta encoded.
  table_options.index_block_restart_interval = 3;
  table_options.block_cache = NewLRUCache(1 << 20);
  table_options.cache_index_and_filter_blocks = true;

  TableConstructor c(BytewiseComparator());
  // Vary value sizes so that consecutive partition handles have both zero and
  // non-zero size deltas. Zero is the interesting one: it is the delta whose
  // legacy encoding collides with the format_version 8 escape codepoint.
  Random rnd(302);
  std::vector<std::string> user_keys;
  for (int i = 0; i < 1000; i++) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key%06d", i);
    std::string user_key(buf);
    InternalKey ikey(user_key, 0, kTypeValue);
    c.Add(ikey.Encode().ToString(),
          rnd.RandomString(i % 3 == 0 ? 40 : 8) /* value */);
    user_keys.push_back(std::move(user_key));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator ikc(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, ikc, &keys, &kvmap);

  auto* reader = static_cast<BlockBasedTable*>(c.GetTableReader());
  auto props = reader->GetTableProperties();
  // Both top-level indexes must have enough entries that non-restart (i.e.
  // delta encoded) entries exist, otherwise this test proves nothing.
  ASSERT_GT(props->index_partitions,
            static_cast<uint64_t>(table_options.index_block_restart_interval));
  ASSERT_EQ(props->index_value_is_delta_encoded, 1);

  // Full scan: drives the top-level index of index partitions.
  ReadOptions read_options;
  {
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));
    size_t i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_LT(i, keys.size());
      ASSERT_EQ(keys[i], iter->key().ToString());
      ASSERT_EQ(kvmap[keys[i]], iter->value().ToString());
      i++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(i, keys.size());
  }

  // Point lookups: drive the index of filter partitions as well.
  for (size_t i = 0; i < user_keys.size(); i += 7) {
    SCOPED_TRACE("user_key=" + user_keys[i]);
    InternalKey ikey(user_keys[i], 0, kTypeValue);
    PinnableSlice value;
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, user_keys[i], &value, nullptr,
                           nullptr, nullptr, true, nullptr, nullptr);
    ASSERT_OK(reader->Get(read_options, ikey.Encode(), &get_context,
                          moptions.prefix_extractor.get()));
    ASSERT_EQ(kvmap[ikey.Encode().ToString()], value.ToString());
  }

  c.ResetTableReader();
}

// format_version 8 extends the data-block common user-key prefix feature to
// index blocks (leaf, partitioned top-level) and the partitioned-filter
// top-level index. This test verifies that, for prefix-heavy keys, an fv8 table
// reads *identically* to an fv7 table (the trusted baseline) across index
// types, partitioned filters, and (reverse-)bytewise comparators: full forward
// scan, random Seek positioning, and point Get all match. It also asserts the
// index actually shrank at fv8, so the feature is genuinely engaged (not a
// silently-disabled no-op).
TEST_F(GeneralTableTest, IndexBlockCommonPrefixEquivalence) {
  struct Config {
    const char* name;
    BlockBasedTableOptions::IndexType index_type;
    bool partition_filters;
    bool reverse;
    bool hash;        // needs a prefix extractor for the hash index
    bool custom_cmp;  // non-(reverse-)bytewise comparator; forces kEnabled and
                      // the reader's reconstruction (non-suffix-seek) path
  };
  const std::vector<Config> configs = {
      {"leaf_binary", BlockBasedTableOptions::kBinarySearch, false, false,
       false},
      {"leaf_binary_reverse", BlockBasedTableOptions::kBinarySearch, false,
       true, false},
      {"leaf_first_key", BlockBasedTableOptions::kBinarySearchWithFirstKey,
       false, false, false},
      {"leaf_hash", BlockBasedTableOptions::kHashSearch, false, false, true},
      {"partitioned", BlockBasedTableOptions::kTwoLevelIndexSearch, false,
       false, false},
      {"partitioned_reverse", BlockBasedTableOptions::kTwoLevelIndexSearch,
       false, true, false},
      {"partitioned_filters", BlockBasedTableOptions::kTwoLevelIndexSearch,
       true, false, false},
      // Non-(reverse-)bytewise comparator with kEnabled: exercises the reader's
      // reconstruction path for stripped index blocks (leaf + partitioned).
      {"leaf_binary_custom_cmp", BlockBasedTableOptions::kBinarySearch, false,
       false, false, true},
      {"partitioned_custom_cmp", BlockBasedTableOptions::kTwoLevelIndexSearch,
       false, false, false, true},
  };

  // Long user-key prefix shared by every key, so data-block, index-separator,
  // and filter-separator keys are all prefix-heavy.
  const std::string shared = "abcdefghij_common_index_prefix_v1_";
  const int kNum = 2000;

  std::vector<std::string> user_keys;
  std::vector<std::string> values;
  user_keys.reserve(kNum);
  values.reserve(kNum);
  for (int i = 0; i < kNum; i++) {
    char kbuf[16];
    snprintf(kbuf, sizeof(kbuf), "%08d", i);
    user_keys.push_back(shared + kbuf);
    char vbuf[16];
    snprintf(vbuf, sizeof(vbuf), "V%06d", i);
    values.emplace_back(vbuf);
  }

  // Probe targets. All of these stay within the shared prefix so they are valid
  // for the hash index's prefix extractor. Diverging (out-of-prefix) targets
  // are added only for the non-hash configs (see below), where they exercise
  // the StripSeekTargetPrefix before-all / after-all paths.
  std::vector<std::string> base_probes;
  for (int i : {0, 1, 2, 137, 500, 999, 1000, 1998, 1999}) {
    base_probes.push_back(user_keys[i]);
  }
  base_probes.push_back(shared + "00000005x");  // gap between existing keys
  base_probes.push_back(shared + "!!!!!!!!");   // before all keys, in-prefix
  base_probes.push_back(shared + "99999999");   // after all keys, in-prefix
  const std::vector<std::string> diverge_probes = {
      "AAAA_before_all",  // diverges before the shared prefix
      "zzzz_after_all",   // diverges after the shared prefix
  };

  struct ProbeResult {
    std::vector<std::pair<std::string, std::string>> scan;
    std::vector<std::string> seek_key;  // "" when Seek lands invalid
    std::vector<int> get_found;
    std::vector<std::string> get_val;
    uint64_t index_size = 0;
  };

  auto run = [&](const Config& cfg, uint32_t fv,
                 const std::vector<std::string>& probes) -> ProbeResult {
    const Comparator* ucmp = cfg.custom_cmp
                                 ? ForwardingBytewise()
                                 : (cfg.reverse ? ReverseBytewiseComparator()
                                                : BytewiseComparator());
    InternalKeyComparator icmp(ucmp);

    Options options;
    options.comparator = ucmp;
    if (cfg.hash) {
      options.prefix_extractor.reset(
          NewFixedPrefixTransform(shared.size() + 2));
    }
    BlockBasedTableOptions table_options;
    table_options.format_version = fv;
    table_options.index_type = cfg.index_type;
    if (cfg.custom_cmp) {
      // A custom comparator only strips under kEnabled (kIfFastSeek is
      // (reverse-)bytewise only); at fv7 the option is inert.
      table_options.optimize_key_common_prefix =
          BlockBasedTableOptions::OptimizeKeyCommonPrefix::kEnabled;
    }
    // Exercise the last-separator successor path (and its suppression under
    // common-prefix): with this mode fv7 emits a short successor as the file's
    // last separator, while fv8 keeps the full last key so the top-level /
    // non-partitioned index still strips. Read results must match regardless.
    table_options.index_shortening = BlockBasedTableOptions::
        IndexShorteningMode::kShortenSeparatorsAndSuccessor;
    table_options.block_size = 100;
    table_options.index_block_restart_interval =
        cfg.index_type == BlockBasedTableOptions::kHashSearch ? 1 : 4;
    table_options.metadata_block_size = 256;  // force many index partitions
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    table_options.whole_key_filtering = true;
    if (cfg.partition_filters) {
      table_options.partition_filters = true;
      table_options.cache_index_and_filter_blocks = true;
    }
    table_options.block_cache = NewLRUCache(16 * 1024 * 1024);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    TableConstructor c(ucmp, /*convert_to_internal_key=*/true);
    for (int i = 0; i < kNum; i++) {
      c.Add(user_keys[i], values[i]);
    }

    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;
    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options, icmp, &keys, &kvmap);

    ProbeResult r;
    auto reader = c.GetTableReader();
    r.index_size = reader->GetTableProperties()->index_size;

    ReadOptions ro;
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        ro, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized));

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      r.scan.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());

    for (const auto& uk : probes) {
      InternalKey target(uk, kMaxSequenceNumber, kValueTypeForSeek);
      iter->Seek(target.Encode());
      EXPECT_OK(iter->status());
      r.seek_key.push_back(iter->Valid() ? iter->key().ToString() : "");
    }

    for (const auto& uk : probes) {
      PinnableSlice value;
      GetContext get_context(ucmp, nullptr, nullptr, nullptr,
                             GetContext::kNotFound, uk, &value, nullptr,
                             nullptr, nullptr, true, nullptr, nullptr);
      InternalKey lkey(uk, kMaxSequenceNumber, kValueTypeForSeek);
      Status s = reader->Get(ro, lkey.Encode(), &get_context,
                             moptions.prefix_extractor.get());
      EXPECT_OK(s);
      if (get_context.State() == GetContext::kFound) {
        r.get_found.push_back(1);
        r.get_val.push_back(value.ToString());
      } else {
        r.get_found.push_back(0);
        r.get_val.emplace_back("");
      }
    }
    return r;
  };

  for (const auto& cfg : configs) {
    SCOPED_TRACE(cfg.name);
    std::vector<std::string> probes = base_probes;
    if (!cfg.hash) {
      // Out-of-prefix targets are only valid without a prefix extractor.
      probes.insert(probes.end(), diverge_probes.begin(), diverge_probes.end());
    }
    ProbeResult r7 = run(cfg, 7, probes);
    ProbeResult r8 = run(cfg, 8, probes);
    EXPECT_EQ(r7.scan, r8.scan);
    EXPECT_EQ(r7.seek_key, r8.seek_key);
    EXPECT_EQ(r7.get_found, r8.get_found);
    EXPECT_EQ(r7.get_val, r8.get_val);
    // Sanity: every key is found, and the scan reproduces the input.
    EXPECT_EQ(r8.scan.size(), static_cast<size_t>(kNum));
    // Feature is genuinely engaged: the prefix-heavy index shrinks at fv8.
    EXPECT_LT(r8.index_size, r7.index_size);
  }
}
TEST_P(BlockBasedTableTest, IndexSeekOptimizationIncomplete) {
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);

  TableConstructor c(BytewiseComparator());
  AddInternalKey(&c, "pika");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  ASSERT_EQ(1, keys.size());

  auto reader = c.GetTableReader();
  ReadOptions ropt;
  ropt.read_tier = ReadTier::kBlockCacheTier;
  std::unique_ptr<InternalIterator> iter(reader->NewIterator(
      ropt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  auto ikey = [](Slice user_key) {
    return InternalKey(user_key, 0, kTypeValue).Encode().ToString();
  };

  iter->Seek(ikey("pika"));
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsIncomplete());

  // This used to crash at some point.
  iter->Seek(ikey("pika"));
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsIncomplete());
}

TEST_P(BlockBasedTableTest, BinaryIndexWithFirstKey1) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kBinarySearchWithFirstKey;
  IndexTest(table_options);
}

class CustomFlushBlockPolicy : public FlushBlockPolicyFactory,
                               public FlushBlockPolicy {
 public:
  explicit CustomFlushBlockPolicy(std::vector<int> keys_per_block)
      : keys_per_block_(keys_per_block) {}

  const char* Name() const override { return "CustomFlushBlockPolicy"; }

  FlushBlockPolicy* NewFlushBlockPolicy(const BlockBasedTableOptions&,
                                        const BlockBuilder&) const override {
    return new CustomFlushBlockPolicy(keys_per_block_);
  }

  bool Update(const Slice&, const Slice&) override {
    if (keys_in_current_block_ >= keys_per_block_.at(current_block_idx_)) {
      ++current_block_idx_;
      keys_in_current_block_ = 1;
      return true;
    }

    ++keys_in_current_block_;
    return false;
  }

  std::vector<int> keys_per_block_;

  int current_block_idx_ = 0;
  int keys_in_current_block_ = 0;
};

TEST_P(BlockBasedTableTest, BinaryIndexWithFirstKey2) {
  for (int use_first_key = 0; use_first_key < 2; ++use_first_key) {
    SCOPED_TRACE("use_first_key = " + std::to_string(use_first_key));
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    table_options.index_type =
        use_first_key ? BlockBasedTableOptions::kBinarySearchWithFirstKey
                      : BlockBasedTableOptions::kBinarySearch;
    table_options.block_cache = NewLRUCache(10000);  // fits all blocks
    table_options.index_shortening =
        BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
    table_options.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicy>(std::vector<int>{2, 1, 3, 2});
    Options options;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.statistics = CreateDBStatistics();
    Statistics* stats = options.statistics.get();
    std::unique_ptr<InternalKeyComparator> comparator(
        new InternalKeyComparator(BytewiseComparator()));
    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);

    TableConstructor c(BytewiseComparator());

    // Block 0.
    AddInternalKey(&c, "aaaa", "v0");
    AddInternalKey(&c, "aaac", "v1");

    // Block 1.
    AddInternalKey(&c, "aaca", "v2");

    // Block 2.
    AddInternalKey(&c, "caaa", "v3");
    AddInternalKey(&c, "caac", "v4");
    AddInternalKey(&c, "caae", "v5");

    // Block 3.
    AddInternalKey(&c, "ccaa", "v6");
    AddInternalKey(&c, "ccac", "v7");

    // Write the file.
    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;
    c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
             &kvmap);
    ASSERT_EQ(8, keys.size());

    auto reader = c.GetTableReader();
    auto props = reader->GetTableProperties();
    ASSERT_EQ(4u, props->num_data_blocks);
    ReadOptions read_options;
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(
        read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUncategorized,
        /*compaction_readahead_size=*/0, /*allow_unprepared_value=*/true));

    // Shouldn't have read data blocks before iterator is seeked.
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    auto ikey = [](Slice user_key) {
      return InternalKey(user_key, 0, kTypeValue).Encode().ToString();
    };

    // Seek to a key between blocks. If index contains first key, we shouldn't
    // read any data blocks until value is requested.
    iter->Seek(ikey("aaba"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 0 : 1,
              stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v2", iter->value().ToString());
    EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to the middle of a block. The block should be read right away.
    iter->Seek(ikey("caab"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[4], iter->key().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v4", iter->value().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to just before the same block and don't access value.
    // The iterator should keep pinning the block contents.
    iter->Seek(ikey("baaa"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[3], iter->key().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to the same block again to check that the block is still pinned.
    iter->Seek(ikey("caae"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[5], iter->key().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v5", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Step forward and fall through to the next block. Don't access value.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[6], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 3,
              stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Step forward again. Block should be read.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[7], iter->key().ToString());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v7", iter->value().ToString());
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Step forward and reach the end.
    iter->Next();
    EXPECT_FALSE(iter->Valid());
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to a single-key block and step forward without accessing value.
    iter->Seek(ikey("aaca"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 0 : 1,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[3], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 1 : 2,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v3", iter->value().ToString());
    EXPECT_EQ(2, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    // Seek between blocks and step back without accessing value.
    iter->Seek(ikey("aaca"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 3,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    EXPECT_EQ(3, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[1], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 3,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    // All blocks are in cache now, there'll be no more misses ever.
    EXPECT_EQ(4, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v1", iter->value().ToString());

    // Next into the next block again.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[2], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 4,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Seek to first and step back without accessing value.
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[0], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 2 : 5,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->Prev();
    EXPECT_FALSE(iter->Valid());
    EXPECT_EQ(use_first_key ? 2 : 5,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    // Do some SeekForPrev() and SeekToLast() just to cover all methods.
    iter->SeekForPrev(ikey("caad"));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[4], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 3 : 6,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v4", iter->value().ToString());
    EXPECT_EQ(use_first_key ? 3 : 6,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(keys[7], iter->key().ToString());
    EXPECT_EQ(use_first_key ? 4 : 7,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ("v7", iter->value().ToString());
    EXPECT_EQ(use_first_key ? 4 : 7,
              stats->getTickerCount(BLOCK_CACHE_DATA_HIT));

    EXPECT_EQ(4, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

    c.ResetTableReader();
  }
}

TEST_P(BlockBasedTableTest, BinaryIndexWithFirstKeyGlobalSeqno) {
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kBinarySearchWithFirstKey;
  table_options.block_cache = NewLRUCache(10000);
  Options options;
  options.statistics = CreateDBStatistics();
  Statistics* stats = options.statistics.get();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);

  TableConstructor c(BytewiseComparator(), /* convert_to_internal_key */ false,
                     /* level */ -1, /* largest_seqno */ 42);

  c.Add(InternalKey("b", 0, kTypeValue).Encode().ToString(), "x");
  c.Add(InternalKey("c", 0, kTypeValue).Encode().ToString(), "y");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  ASSERT_EQ(2, keys.size());

  auto reader = c.GetTableReader();
  auto props = reader->GetTableProperties();
  ASSERT_EQ(1u, props->num_data_blocks);
  ReadOptions read_options;
  std::unique_ptr<InternalIterator> iter(reader->NewIterator(
      read_options, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized,
      /*compaction_readahead_size=*/0, /*allow_unprepared_value=*/true));

  iter->Seek(InternalKey("a", 0, kTypeValue).Encode().ToString());
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(InternalKey("b", 42, kTypeValue).Encode().ToString(),
            iter->key().ToString());
  EXPECT_NE(keys[0], iter->key().ToString());
  // Key should have been served from index, without reading data blocks.
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));

  ASSERT_TRUE(iter->PrepareValue());
  EXPECT_EQ("x", iter->value().ToString());
  EXPECT_EQ(1, stats->getTickerCount(BLOCK_CACHE_DATA_MISS));
  EXPECT_EQ(0, stats->getTickerCount(BLOCK_CACHE_DATA_HIT));
  EXPECT_EQ(InternalKey("b", 42, kTypeValue).Encode().ToString(),
            iter->key().ToString());

  c.ResetTableReader();
}

// It's very hard to figure out the index block size of a block accurately.
// To make sure we get the index size, we just make sure as key number
// grows, the filter block size also grows.
TEST_P(BlockBasedTableTest, IndexSizeStat) {
  uint64_t last_index_size = 0;

  // we need to use random keys since the pure human readable texts
  // may be well compressed, resulting insignifcant change of index
  // block size.
  Random rnd(test::RandomSeed());
  std::vector<std::string> keys;

  for (int i = 0; i < 100; ++i) {
    keys.push_back(rnd.RandomString(10000));
  }

  // Each time we load one more key to the table. the table index block
  // size is expected to be larger than last time's.
  for (size_t i = 1; i < keys.size(); ++i) {
    TableConstructor c(BytewiseComparator(),
                       true /* convert_to_internal_key_ */);
    for (size_t j = 0; j < i; ++j) {
      c.Add(keys[j], "val");
    }

    std::vector<std::string> ks;
    stl_wrappers::KVMap kvmap;
    Options options;
    options.compression = kNoCompression;
    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    table_options.block_restart_interval = 1;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    const ImmutableOptions ioptions(options);
    const MutableCFOptions moptions(options);
    c.Finish(options, ioptions, moptions, table_options,
             GetPlainInternalComparator(options.comparator), &ks, &kvmap);
    auto index_size = c.GetTableReader()->GetTableProperties()->index_size;
    ASSERT_GT(index_size, last_index_size);
    last_index_size = index_size;
    c.ResetTableReader();
  }
}

TEST_P(BlockBasedTableTest, NumBlockStat) {
  Random rnd(test::RandomSeed());
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_restart_interval = 1;
  table_options.block_size = 1000;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  for (int i = 0; i < 10; ++i) {
    // the key/val are slightly smaller than block size, so that each block
    // holds roughly one key/value pair.
    c.Add(rnd.RandomString(900), "val");
  }

  std::vector<std::string> ks;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &ks, &kvmap);
  ASSERT_EQ(kvmap.size(),
            c.GetTableReader()->GetTableProperties()->num_data_blocks);
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TracingGetTest) {
  TableConstructor c(BytewiseComparator());
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  SetupTracingTest(&c);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  InternalKey internal_key(auto_add_key1, 0, kTypeValue);
  std::string encoded_key = internal_key.Encode().ToString();
  for (uint32_t i = 1; i <= 2; i++) {
    PinnableSlice value;
    GetContext get_context(
        options.comparator, nullptr, nullptr, nullptr, GetContext::kNotFound,
        auto_add_key1, &value, nullptr, nullptr, nullptr, true, nullptr,
        nullptr, nullptr, nullptr, nullptr, nullptr, /*tracing_get_id=*/i);
    get_perf_context()->Reset();
    ASSERT_OK(c.GetTableReader()->Get(ReadOptions(), encoded_key, &get_context,
                                      moptions.prefix_extractor.get()));
    ASSERT_EQ(get_context.State(), GetContext::kFound);
    ASSERT_EQ(value.ToString(), kDummyValue);
  }

  // Verify traces.
  std::vector<BlockCacheTraceRecord> expected_records;
  // The first two records should be prefetching index and filter blocks.
  BlockCacheTraceRecord record;
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kPrefetch;
  record.is_cache_hit = false;
  record.no_insert = false;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceFilterBlock;
  expected_records.push_back(record);
  // Then we should have three records for one index, one filter, and one data
  // block access.
  record.get_id = 1;
  record.block_type = TraceType::kBlockTraceFilterBlock;
  record.caller = TableReaderCaller::kUserGet;
  record.get_from_user_specified_snapshot = false;
  record.referenced_key = encoded_key;
  record.referenced_key_exist_in_block = true;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceIndexBlock;
  expected_records.push_back(record);
  record.is_cache_hit = false;
  record.block_type = TraceType::kBlockTraceDataBlock;
  expected_records.push_back(record);
  // The second get should all observe cache hits.
  record.is_cache_hit = true;
  record.get_id = 2;
  record.block_type = TraceType::kBlockTraceFilterBlock;
  record.caller = TableReaderCaller::kUserGet;
  record.get_from_user_specified_snapshot = false;
  record.referenced_key = encoded_key;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceIndexBlock;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceDataBlock;
  expected_records.push_back(record);
  VerifyBlockAccessTrace(&c, expected_records);
  c.ResetTableReader();
}

void GenerateKVMap(TableConstructor* c) {
  int num_block = 100;
  Random rnd(101);
  uint32_t key = 0;
  for (int block = 0; block < num_block; block++) {
    for (int i = 0; i < 16; i++) {
      char k[9] = {0};
      // Internal key is constructed directly from this key,
      // and internal key size is required to be >= 8 bytes,
      // so use %08u as the format string.
      snprintf(k, sizeof(k), "%08u", key);
      std::string v = rnd.RandomString(256);
      InternalKey ikey(std::string(k), 0, kTypeValue);
      c->Add(ikey.Encode().ToString(), rnd.RandomString(256));
      key++;
    }
  }
}

void WarmUpCache(TableConstructor* c, const MutableCFOptions& moptions,
                 const std::vector<std::string>& warm_keys) {
  ReadOptions ro;
  std::unique_ptr<InternalIterator> iter(c->GetTableReader()->NewIterator(
      ro, moptions.prefix_extractor.get(), nullptr, false,
      TableReaderCaller::kUncategorized));
  size_t i = 0;
  while (i < warm_keys.size()) {
    InternalKey ikey(warm_keys[i], 0, kTypeValue);
    iter->Seek(ikey.Encode().ToString());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    i++;
  }
}

TEST_P(BlockBasedTableTest, BlockCacheLookupSeqScans) {
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  table_options.block_align = true;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  ASSERT_OK(options.table_factory->ValidateOptions(
      DBOptions(options), ColumnFamilyOptions(options)));

  TableConstructor c(BytewiseComparator());
  GenerateKVMap(&c);

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);

  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  BlockBasedTable* bbt = static_cast<BlockBasedTable*>(c.GetTableReader());
  BlockHandle block_handle;

  ReadOptions read_options;
  read_options.auto_readahead_size = true;
  Slice ub = Slice("00000805");
  Slice* ub_ptr = &ub;
  read_options.iterate_upper_bound = ub_ptr;
  read_options.readahead_size = 16384;

  // Test various functionalities -
  // 5 blocks prefetched - Current + 4 additional (readahead_size).
  {
    // Check the behavior when it's -
    // Miss(200), Hit(210), Hit(225), Hit(240), Hit(255).
    // It should only prefetch current block (200).
    {
      std::vector<std::string> warm_keys{"00000210", "00000225", "00000240",
                                         "00000255"};
      WarmUpCache(&c, moptions, warm_keys);

      ASSERT_OK(options.statistics->Reset());

      std::unique_ptr<InternalIterator> iter(c.GetTableReader()->NewIterator(
          read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));

      // Seek key -
      InternalKey ikey("00000200", 0, kTypeValue);
      auto kv_iter = kvmap.find(ikey.Encode().ToString());

      iter->Seek(kv_iter->first);
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), kv_iter->first);
      ASSERT_EQ(iter->value().ToString(), kv_iter->second);

      FilePrefetchBuffer* prefetch_buffer =
          (static_cast<BlockBasedTableIterator*>(iter.get()))
              ->prefetch_buffer();
      std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(1);
      prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);

      bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first, block_handle);
      // It won't prefetch the data of cache hit.
      // One block data.
      ASSERT_EQ(std::get<1>(buffer_info[0]), 4096);
      ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());

      ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                1);
    }

    {
      // Check the behavior when it's -
      // First Prefetch - Miss(315), Miss(330), Miss(345), Hit(360), Hit(375),
      // Second Prefetch - Miss(390), Miss(405) ...
      // First prefetch should only prefetch from 315 to 345.
      std::vector<std::string> warm_keys{"00000360", "00000375"};
      WarmUpCache(&c, moptions, warm_keys);

      std::unique_ptr<InternalIterator> iter(c.GetTableReader()->NewIterator(
          read_options, moptions.prefix_extractor.get(), nullptr, false,
          TableReaderCaller::kUncategorized));

      // Seek key -
      InternalKey ikey("00000315", 0, kTypeValue);
      auto kv_iter = kvmap.find(ikey.Encode().ToString());

      iter->Seek(kv_iter->first);
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), kv_iter->first);
      ASSERT_EQ(iter->value().ToString(), kv_iter->second);

      FilePrefetchBuffer* prefetch_buffer =
          (static_cast<BlockBasedTableIterator*>(iter.get()))
              ->prefetch_buffer();
      std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(1);
      prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);
      bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first, block_handle);

      // It won't prefetch the data of cache hit.
      // 3 blocks data.
      ASSERT_EQ(std::get<1>(buffer_info[0]), 12288);
      ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());

      for (; kv_iter != kvmap.end() && iter->Valid(); kv_iter++) {
        ASSERT_EQ(iter->key(), kv_iter->first);
        ASSERT_EQ(iter->value().ToString(), kv_iter->second);
        iter->Next();
        ASSERT_OK(iter->status());

        if (iter->user_key().ToString() == "00000400") {
          break;
        }
      }

      // Second Prefetch.
      prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);
      bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first, block_handle);
      ASSERT_EQ(std::get<1>(buffer_info[0]), 20480);
      ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());

      ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                1);
    }
  }
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, BlockCacheLookupAsyncScansSeek) {
  Options options;
  TableConstructor c(BytewiseComparator());
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(c.env_, FileSystem::Default()));
  options.env = env.get();
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  c.env_ = env.get();

  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  table_options.block_align = true;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  ASSERT_OK(options.table_factory->ValidateOptions(
      DBOptions(options), ColumnFamilyOptions(options)));

  GenerateKVMap(&c);

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);

  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  BlockBasedTable* bbt = static_cast<BlockBasedTable*>(c.GetTableReader());
  BlockHandle block_handle;

  ReadOptions read_options;
  read_options.auto_readahead_size = true;
  Slice ub = Slice("00000805");
  Slice* ub_ptr = &ub;
  read_options.iterate_upper_bound = ub_ptr;
  read_options.readahead_size = 16384;
  read_options.async_io = true;

  // Test Various functionalities -
  // 3 blocks prefetched - Current + 2 additional (readahead_size/2).
  {
    // Check the behavior when it's -
    // 1st Prefetch - Miss(200), Hit(210), Hit(225),
    // 2nd Prefetch - Hit(240), Hit(255)
    // First Prefetch will be for 200 offset.
    // Second prefetch will be 0.
    {
      std::vector<std::string> warm_keys{"00000210", "00000225", "00000240",
                                         "00000255"};
      WarmUpCache(&c, moptions, warm_keys);

      ASSERT_OK(options.statistics->Reset());

      std::unique_ptr<InternalIterator> iter(c.GetTableReader()->NewIterator(
          read_options, moptions.prefix_extractor.get(), nullptr, false,
          TableReaderCaller::kUncategorized));

      // Seek key -
      InternalKey ikey("00000200", 0, kTypeValue);
      auto kv_iter = kvmap.find(ikey.Encode().ToString());

      iter->Seek(kv_iter->first);
      ASSERT_TRUE(iter->status().IsTryAgain());
      iter->Seek(kv_iter->first);
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), kv_iter->first);
      ASSERT_EQ(iter->value().ToString(), kv_iter->second);

      FilePrefetchBuffer* prefetch_buffer =
          (static_cast<BlockBasedTableIterator*>(iter.get()))
              ->prefetch_buffer();
      std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(2);
      prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);

      bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first, block_handle);
      ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());
      ASSERT_EQ(std::get<1>(buffer_info[0]), 4096);
      ASSERT_EQ(std::get<1>(buffer_info[1]), 0);

      ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                2);
    }
    {
      // Check the behavior when it's -
      // First Prefetch - Miss(315), Miss(330), Hit(345),
      // Second Prefetch - Miss(360), Miss(375), ...
      // First prefetch should only prefetch from 315 to 330.
      // Second prefetch should start from 360.
      std::vector<std::string> warm_keys{"00000345"};
      WarmUpCache(&c, moptions, warm_keys);

      std::unique_ptr<InternalIterator> iter(c.GetTableReader()->NewIterator(
          read_options, moptions.prefix_extractor.get(), nullptr, false,
          TableReaderCaller::kUncategorized));

      // Seek key -
      InternalKey ikey("00000315", 0, kTypeValue);
      auto kv_iter = kvmap.find(ikey.Encode().ToString());

      iter->Seek(kv_iter->first);
      ASSERT_TRUE(iter->status().IsTryAgain());
      iter->Seek(kv_iter->first);
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), kv_iter->first);
      ASSERT_EQ(iter->value().ToString(), kv_iter->second);

      FilePrefetchBuffer* prefetch_buffer =
          (static_cast<BlockBasedTableIterator*>(iter.get()))
              ->prefetch_buffer();
      std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(2);
      prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);
      {
        // 1st Buffer Verification.
        bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first,
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[0]), 8192);

        // 2nd Buffer Verification.
        InternalKey ikey_tmp("00000360", 0, kTypeValue);
        bbt->TEST_GetDataBlockHandle(read_options, ikey_tmp.Encode().ToString(),
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[1]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[1]), 8192);

        ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                  1);
      }
    }

    {
      // Check the behavior when it's -
      // First Prefetch - Miss(495), Miss(510), Hit(525), prefetch len- 8192
      // Second Prefetch async - Miss(540), Miss(555),   - 8192
      // Third Prefetch Async - Hit(570), Miss(585),  - 4096
      // 4th Prefetch Async - Hit(600), Miss(615), - 4096
      // 5th Prefetch Async - Miss(630), Miss(645) - 8192
      std::vector<std::string> warm_keys{"00000525", "00000570", "00000600"};
      WarmUpCache(&c, moptions, warm_keys);

      std::unique_ptr<InternalIterator> iter(c.GetTableReader()->NewIterator(
          read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
          /*skip_filters=*/false, TableReaderCaller::kUncategorized));

      // Seek key -
      InternalKey ikey("00000495", 0, kTypeValue);
      auto kv_iter = kvmap.find(ikey.Encode().ToString());

      // First and Second Prefetch.
      iter->Seek(kv_iter->first);
      ASSERT_TRUE(iter->status().IsTryAgain());
      iter->Seek(kv_iter->first);
      ASSERT_OK(iter->status());
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), kv_iter->first);
      ASSERT_EQ(iter->value().ToString(), kv_iter->second);

      FilePrefetchBuffer* prefetch_buffer =
          (static_cast<BlockBasedTableIterator*>(iter.get()))
              ->prefetch_buffer();

      {
        std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(2);
        prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);

        // 1st Buffer Verification.
        bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first,
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[0]), 8192);

        // 2nd Buffer Verification.
        InternalKey ikey_tmp("00000540", 0, kTypeValue);
        bbt->TEST_GetDataBlockHandle(read_options, ikey_tmp.Encode().ToString(),
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[1]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[1]), 8192);

        ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                  1);
      }

      // Third prefetch ReadAsync (buffers will swap).
      for (; kv_iter != kvmap.end() && iter->Valid(); kv_iter++) {
        ASSERT_EQ(iter->key(), kv_iter->first);
        ASSERT_EQ(iter->value().ToString(), kv_iter->second);

        if (iter->user_key() == "00000540") {
          break;
        }

        iter->Next();
        ASSERT_OK(iter->status());
      }

      {
        std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(2);
        prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);

        // 1st Buffer Verification.
        bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first,
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[0]), 8192);

        // 2nd Buffer Verification.
        InternalKey ikey_tmp("00000585", 0, kTypeValue);
        bbt->TEST_GetDataBlockHandle(read_options, ikey_tmp.Encode().ToString(),
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[1]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[1]), 4096);

        ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                  1);
      }

      // 4th Prefetch ReadAsync (buffers will swap).
      for (; kv_iter != kvmap.end() && iter->Valid(); kv_iter++) {
        ASSERT_EQ(iter->key(), kv_iter->first);
        ASSERT_EQ(iter->value().ToString(), kv_iter->second);

        if (iter->user_key() == "00000585") {
          break;
        }

        iter->Next();
        ASSERT_OK(iter->status());
      }

      {
        std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(2);
        prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);

        // 1st Buffer Verification.
        bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first,
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[0]), 4096);

        // 2nd Buffer Verification.
        InternalKey ikey_tmp("00000615", 0, kTypeValue);
        bbt->TEST_GetDataBlockHandle(read_options, ikey_tmp.Encode().ToString(),
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[1]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[1]), 4096);

        ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                  1);
      }

      // 5th Prefetch ReadAsync.
      for (; kv_iter != kvmap.end() && iter->Valid(); kv_iter++) {
        ASSERT_EQ(iter->key(), kv_iter->first);
        ASSERT_EQ(iter->value().ToString(), kv_iter->second);

        if (iter->user_key() == "00000615") {
          break;
        }

        iter->Next();
        ASSERT_OK(iter->status());
      }

      {
        std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(2);
        prefetch_buffer->TEST_GetBufferOffsetandSize(buffer_info);

        // 1st Buffer Verification.
        bbt->TEST_GetDataBlockHandle(read_options, kv_iter->first,
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[0]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[0]), 4096);

        // 2nd Buffer Verification.
        InternalKey ikey_tmp("00000630", 0, kTypeValue);
        bbt->TEST_GetDataBlockHandle(read_options, ikey_tmp.Encode().ToString(),
                                     block_handle);
        ASSERT_EQ(std::get<0>(buffer_info[1]), block_handle.offset());
        ASSERT_EQ(std::get<1>(buffer_info[1]), 8192);

        ASSERT_EQ(options.statistics->getAndResetTickerCount(READAHEAD_TRIMMED),
                  0);
      }
    }
  }
  c.ResetTableReader();
}

struct HitMissCountingCache : public CacheWrapper {
  using CacheWrapper::CacheWrapper;
  const char* Name() const override { return "HitMissCountingCache"; }

  uint64_t hit_count_ = 0;
  uint64_t miss_count_ = 0;

  void Reset() {
    hit_count_ = 0;
    miss_count_ = 0;
  }

  Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                 CreateContext* create_context,
                 Priority priority = Priority::LOW,
                 Statistics* stats = nullptr) override {
    // ASSUMES no blocking async lookups
    Handle* h = target_->Lookup(key, helper, create_context, priority, stats);
    if (h) {
      hit_count_++;
    } else {
      miss_count_++;
    }
    return h;
  }

  void StartAsyncLookup(AsyncLookupHandle& async_handle) override {
    target_->StartAsyncLookup(async_handle);
    // If not pending, caller might not call WaitAll, so have to account here.
    if (!async_handle.IsPending()) {
      if (async_handle.Result()) {
        hit_count_++;
      } else {
        miss_count_++;
      }
    }
  }

  void WaitAll(AsyncLookupHandle* async_handles, size_t count) override {
    // If !pending, then we already accounted for it in StartAsyncLookup.
    // Assume the pending status does not change asynchronously (since
    // StartAsyncLookup) and remember which still need accounting.
    std::vector<AsyncLookupHandle*> needs_accounting;
    for (size_t i = 0; i < count; ++i) {
      if (async_handles[i].IsPending()) {
        needs_accounting.push_back(async_handles + i);
      }
    }
    target_->WaitAll(async_handles, count);
    for (auto ah : needs_accounting) {
      if (ah->Result()) {
        hit_count_++;
      } else {
        miss_count_++;
      }
    }
  }

  void VerifyExpectedHitMissCounts(
      const std::vector<BlockCacheTraceRecord>& expected_records) {
    uint64_t expected_hits = 0;
    uint64_t expected_misses = 0;
    for (const auto& r : expected_records) {
      if (r.is_cache_hit) {
        expected_hits++;
      } else {
        expected_misses++;
      }
    }
    EXPECT_EQ(expected_hits, hit_count_);
    EXPECT_EQ(expected_misses, miss_count_);
    Reset();
  }
};

TEST_P(BlockBasedTableTest, TracingMultiGetTest) {
  TableConstructor c(BytewiseComparator());
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  auto cache =
      std::make_shared<HitMissCountingCache>(NewLRUCache(1024 * 1024, 0));
  table_options.block_cache = cache;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  // Put auto_add_key1 and auto_add_key2 in the same data block
  table_options.block_size = kDummyValue.size() * 2 + 100;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  SetupTracingTest(&c);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  std::vector<BlockCacheTraceRecord> expected_records;

  for (bool first_pass : {true, false}) {
    uint64_t get_id_offset = first_pass ? 2 : 5;
    ReadOptions ro;
    std::array<Slice, 2> ukeys{{auto_add_key1, auto_add_key2}};
    std::array<PinnableSlice, 2> values;
    std::vector<GetContext> get_contexts;
    get_contexts.emplace_back(
        options.comparator, nullptr, nullptr, nullptr, GetContext::kNotFound,
        ukeys[0], values.data(), nullptr, nullptr, nullptr, true, nullptr,
        nullptr, nullptr, nullptr, nullptr, nullptr, get_id_offset);
    get_contexts.emplace_back(
        options.comparator, nullptr, nullptr, nullptr, GetContext::kNotFound,
        ukeys[1], &values[1], nullptr, nullptr, nullptr, true, nullptr, nullptr,
        nullptr, nullptr, nullptr, nullptr, get_id_offset + 1);
    std::array<std::string, 2> encoded_keys;
    encoded_keys[0] = InternalKey(ukeys[0], 0, kTypeValue).Encode().ToString();
    encoded_keys[1] = InternalKey(ukeys[1], 0, kTypeValue).Encode().ToString();
    std::array<Status, 2> statuses;
    autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_context;
    key_context.emplace_back(/*ColumnFamilyHandle omitted*/ nullptr, ukeys[0],
                             values.data(),
                             /*PinnableWideColumns omitted*/ nullptr,
                             /*timestamp omitted*/ nullptr, statuses.data());
    key_context[0].ukey_without_ts = ukeys[0];
    key_context[0].ikey = encoded_keys[0];
    key_context[0].get_context = get_contexts.data();
    key_context.emplace_back(/*ColumnFamilyHandle omitted*/ nullptr, ukeys[1],
                             &values[1],
                             /*PinnableWideColumns omitted*/ nullptr,
                             /*timestamp omitted*/ nullptr, &statuses[1]);
    key_context[1].ukey_without_ts = ukeys[1];
    key_context[1].ikey = encoded_keys[1];
    key_context[1].get_context = &get_contexts[1];
    autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
    sorted_keys.push_back(&key_context[0]);
    sorted_keys.push_back(&key_context[1]);
    MultiGetContext m_context(
        &sorted_keys, 0, sorted_keys.size(), /*SequenceNumber*/ 42, ro,
        options.env->GetFileSystem().get(), options.statistics.get());
    MultiGetRange range = m_context.GetMultiGetRange();

    get_perf_context()->Reset();
    c.GetTableReader()->MultiGet(ro, &range, /*prefix_extractor*/ nullptr);

    // Verify read op result
    for (uint32_t i = 0; i <= 1; i++) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(get_contexts[i].State(), GetContext::kFound);
      ASSERT_EQ(values[i].ToString(), kDummyValue);
    }

    // Verify traces.
    BlockCacheTraceRecord record;
    if (first_pass) {
      // The first two records should be prefetching index and filter blocks.
      record.get_id = 0;
      record.block_type = TraceType::kBlockTraceIndexBlock;
      record.caller = TableReaderCaller::kPrefetch;
      record.is_cache_hit = false;
      record.no_insert = false;
      expected_records.push_back(record);
      record.block_type = TraceType::kBlockTraceFilterBlock;
      expected_records.push_back(record);
    }
    // Then we should have three records for one index, one filter, and one
    // data block access. (The two keys share a data block.)
    record.get_id = get_id_offset;
    record.block_type = TraceType::kBlockTraceFilterBlock;
    record.caller = TableReaderCaller::kUserMultiGet;
    record.get_from_user_specified_snapshot = false;
    record.referenced_key = encoded_keys[0];
    record.referenced_key_exist_in_block = true;
    record.is_cache_hit = true;
    expected_records.push_back(record);
    record.block_type = TraceType::kBlockTraceIndexBlock;
    expected_records.push_back(record);
    record.is_cache_hit = !first_pass;
    record.block_type = TraceType::kBlockTraceDataBlock;
    expected_records.push_back(record);
  }
  VerifyBlockAccessTrace(&c, expected_records);
  cache->VerifyExpectedHitMissCounts(expected_records);
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TracingApproximateOffsetOfTest) {
  TableConstructor c(BytewiseComparator());
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  SetupTracingTest(&c);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  const ReadOptions read_options;
  for (uint32_t i = 1; i <= 2; i++) {
    InternalKey internal_key(auto_add_key1, 0, kTypeValue);
    std::string encoded_key = internal_key.Encode().ToString();
    c.GetTableReader()->ApproximateOffsetOf(
        read_options, encoded_key, TableReaderCaller::kUserApproximateSize);
  }
  // Verify traces.
  std::vector<BlockCacheTraceRecord> expected_records;
  // The first two records should be prefetching index and filter blocks.
  BlockCacheTraceRecord record;
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kPrefetch;
  record.is_cache_hit = false;
  record.no_insert = false;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceFilterBlock;
  expected_records.push_back(record);
  // Then we should have two records for only index blocks.
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kUserApproximateSize;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  expected_records.push_back(record);
  VerifyBlockAccessTrace(&c, expected_records);
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, TracingIterator) {
  TableConstructor c(BytewiseComparator());
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  options.create_if_missing = true;
  table_options.block_cache = NewLRUCache(1024 * 1024, 0);
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  SetupTracingTest(&c);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  for (uint32_t i = 1; i <= 2; i++) {
    ReadOptions read_options;
    std::unique_ptr<InternalIterator> iter(c.GetTableReader()->NewIterator(
        read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
        /*skip_filters=*/false, TableReaderCaller::kUserIterator));
    iter->SeekToFirst();
    while (iter->Valid()) {
      iter->key();
      iter->value();
      iter->Next();
    }
    ASSERT_OK(iter->status());
    iter.reset();
  }

  // Verify traces.
  std::vector<BlockCacheTraceRecord> expected_records;
  // The first two records should be prefetching index and filter blocks.
  BlockCacheTraceRecord record;
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kPrefetch;
  record.is_cache_hit = false;
  record.no_insert = false;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceFilterBlock;
  expected_records.push_back(record);
  // Then we should have three records for index and two data block access.
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.caller = TableReaderCaller::kUserIterator;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceDataBlock;
  record.is_cache_hit = false;
  expected_records.push_back(record);
  expected_records.push_back(record);
  // When we iterate this file for the second time, we should observe all
  // cache hits.
  record.block_type = TraceType::kBlockTraceIndexBlock;
  record.is_cache_hit = true;
  expected_records.push_back(record);
  record.block_type = TraceType::kBlockTraceDataBlock;
  expected_records.push_back(record);
  expected_records.push_back(record);
  VerifyBlockAccessTrace(&c, expected_records);
  c.ResetTableReader();
}

// A simple tool that takes the snapshot of block cache statistics.
class BlockCachePropertiesSnapshot {
 public:
  explicit BlockCachePropertiesSnapshot(Statistics* statistics) {
    block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_MISS);
    block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_HIT);
    index_block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_INDEX_MISS);
    index_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_INDEX_HIT);
    data_block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_DATA_MISS);
    data_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_DATA_HIT);
    filter_block_cache_miss =
        statistics->getTickerCount(BLOCK_CACHE_FILTER_MISS);
    filter_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_FILTER_HIT);
    block_cache_bytes_read = statistics->getTickerCount(BLOCK_CACHE_BYTES_READ);
    block_cache_bytes_write =
        statistics->getTickerCount(BLOCK_CACHE_BYTES_WRITE);
  }

  void AssertIndexBlockStat(int64_t expected_index_block_cache_miss,
                            int64_t expected_index_block_cache_hit) {
    ASSERT_EQ(expected_index_block_cache_miss, index_block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit, index_block_cache_hit);
  }

  void AssertFilterBlockStat(int64_t expected_filter_block_cache_miss,
                             int64_t expected_filter_block_cache_hit) {
    ASSERT_EQ(expected_filter_block_cache_miss, filter_block_cache_miss);
    ASSERT_EQ(expected_filter_block_cache_hit, filter_block_cache_hit);
  }

  // Check if the fetched props matches the expected ones.
  // TODO(kailiu) Use this only when you disabled filter policy!
  void AssertEqual(int64_t expected_index_block_cache_miss,
                   int64_t expected_index_block_cache_hit,
                   int64_t expected_data_block_cache_miss,
                   int64_t expected_data_block_cache_hit) const {
    ASSERT_EQ(expected_index_block_cache_miss, index_block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit, index_block_cache_hit);
    ASSERT_EQ(expected_data_block_cache_miss, data_block_cache_miss);
    ASSERT_EQ(expected_data_block_cache_hit, data_block_cache_hit);
    ASSERT_EQ(expected_index_block_cache_miss + expected_data_block_cache_miss,
              block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit + expected_data_block_cache_hit,
              block_cache_hit);
  }

  int64_t GetCacheBytesRead() { return block_cache_bytes_read; }

  int64_t GetCacheBytesWrite() { return block_cache_bytes_write; }

 private:
  int64_t block_cache_miss = 0;
  int64_t block_cache_hit = 0;
  int64_t index_block_cache_miss = 0;
  int64_t index_block_cache_hit = 0;
  int64_t data_block_cache_miss = 0;
  int64_t data_block_cache_hit = 0;
  int64_t filter_block_cache_miss = 0;
  int64_t filter_block_cache_hit = 0;
  int64_t block_cache_bytes_read = 0;
  int64_t block_cache_bytes_write = 0;
};

// Make sure, by default, index/filter blocks were pre-loaded (meaning we
// won't use block cache to store them).
TEST_P(BlockBasedTableTest, BlockCacheDisabledTest) {
  Options options;
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_cache = NewLRUCache(1024, 4);
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("key", "value");
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  // preloading filter/index blocks is enabled.
  auto reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  ASSERT_FALSE(reader->TEST_FilterBlockInCache());
  ASSERT_FALSE(reader->TEST_IndexBlockInCache());

  {
    // nothing happens in the beginning
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertIndexBlockStat(0, 0);
    props.AssertFilterBlockStat(0, 0);
  }

  {
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, Slice(), nullptr, nullptr,
                           nullptr, nullptr, true, nullptr, nullptr);
    // a hack that just to trigger BlockBasedTable::GetFilter.
    ASSERT_OK(reader->Get(ReadOptions(), "non-exist-key", &get_context,
                          moptions.prefix_extractor.get()));
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertIndexBlockStat(0, 0);
    props.AssertFilterBlockStat(0, 0);
  }
}

// Due to the difficulities of the intersaction between statistics, this test
// only tests the case when "index block is put to block cache"
TEST_P(BlockBasedTableTest, FilterBlockInBlockCache) {
  // -- Table construction
  Options options;
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();

  // Enable the cache for index/filter blocks
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  LRUCacheOptions co;
  co.capacity = 2048;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  table_options.block_cache = NewLRUCache(co);
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("key", "value");
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  // preloading filter/index blocks is prohibited.
  auto* reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  ASSERT_FALSE(reader->TEST_FilterBlockInCache());
  ASSERT_TRUE(reader->TEST_IndexBlockInCache());

  // -- PART 1: Open with regular block cache.
  // Since block_cache is disabled, no cache activities will be involved.
  std::unique_ptr<InternalIterator> iter;

  int64_t last_cache_bytes_read = 0;
  // At first, no block will be accessed.
  {
    BlockCachePropertiesSnapshot props(options.statistics.get());
    // index will be added to block cache.
    props.AssertEqual(1,  // index block miss
                      0, 0, 0);
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
    last_cache_bytes_read = props.GetCacheBytesRead();
  }

  // Only index block will be accessed
  {
    iter.reset(c.NewIterator(moptions.prefix_extractor.get()));
    BlockCachePropertiesSnapshot props(options.statistics.get());
    // NOTE: to help better highlight the "detla" of each ticker, I use
    // <last_value> + <added_value> to indicate the increment of changed
    // value; other numbers remain the same.
    props.AssertEqual(1, 0 + 1,  // index block hit
                      0, 0);
    // Cache hit, bytes read from cache should increase
    ASSERT_GT(props.GetCacheBytesRead(), last_cache_bytes_read);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
    last_cache_bytes_read = props.GetCacheBytesRead();
  }

  // Only data block will be accessed
  {
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1, 1, 0 + 1,  // data block miss
                      0);
    // Cache miss, Bytes read from cache should not change
    ASSERT_EQ(props.GetCacheBytesRead(), last_cache_bytes_read);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
    last_cache_bytes_read = props.GetCacheBytesRead();
  }

  // Data block will be in cache
  {
    iter.reset(c.NewIterator(moptions.prefix_extractor.get()));
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1, 1 + 1, /* index block hit */
                      1, 0 + 1 /* data block hit */);
    // Cache hit, bytes read from cache should increase
    ASSERT_GT(props.GetCacheBytesRead(), last_cache_bytes_read);
    ASSERT_EQ(props.GetCacheBytesWrite(),
              static_cast<int64_t>(table_options.block_cache->GetUsage()));
  }
  // release the iterator so that the block cache can reset correctly.
  iter.reset();

  c.ResetTableReader();

  // -- PART 2: Open with very small block cache
  // In this test, no block will ever get hit since the block cache is
  // too small to fit even one entry.
  table_options.block_cache = NewLRUCache(1, 4);
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions2(options);
  const MutableCFOptions moptions2(options);
  ASSERT_OK(c.Reopen(ioptions2, moptions2));
  {
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1,  // index block miss
                      0, 0, 0);
    // Cache miss, Bytes read from cache should not change
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
  }

  {
    // Both index and data block get accessed.
    // It first cache index block then data block. But since the cache size
    // is only 1, index block will be purged after data block is inserted.
    iter.reset(c.NewIterator(moptions2.prefix_extractor.get()));
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(1 + 1,  // index block miss
                      0, 0,   // data block miss
                      0);
    // Cache hit, bytes read from cache should increase
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
  }

  {
    // SeekToFirst() accesses data block. With similar reason, we expect data
    // block's cache miss.
    iter->SeekToFirst();
    ASSERT_OK(iter->status());
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertEqual(2, 0, 0 + 1,  // data block miss
                      0);
    // Cache miss, Bytes read from cache should not change
    ASSERT_EQ(props.GetCacheBytesRead(), 0);
  }
  iter.reset();
  c.ResetTableReader();

  // -- PART 3: Open table with bloom filter enabled but not in SST file
  table_options.block_cache = NewLRUCache(4096, 4);
  table_options.cache_index_and_filter_blocks = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  TableConstructor c3(BytewiseComparator());
  std::string user_key = "k01";
  InternalKey internal_key(user_key, 0, kTypeValue);
  c3.Add(internal_key.Encode().ToString(), "hello");
  ImmutableOptions ioptions3(options);
  MutableCFOptions moptions3(options);
  // Generate table without filter policy
  c3.Finish(options, ioptions3, moptions3, table_options,
            GetPlainInternalComparator(options.comparator), &keys, &kvmap);
  c3.ResetTableReader();

  // Open table with filter policy
  table_options.filter_policy.reset(NewBloomFilterPolicy(1));
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  options.statistics = CreateDBStatistics();
  ImmutableOptions ioptions4(options);
  MutableCFOptions moptions4(options);
  ASSERT_OK(c3.Reopen(ioptions4, moptions4));
  reader = dynamic_cast<BlockBasedTable*>(c3.GetTableReader());
  ASSERT_FALSE(reader->TEST_FilterBlockInCache());
  PinnableSlice value;
  GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, user_key, &value, nullptr,
                         nullptr, nullptr, true, nullptr, nullptr);
  ASSERT_OK(reader->Get(ReadOptions(), internal_key.Encode(), &get_context,
                        moptions4.prefix_extractor.get()));
  // Length-aware compare: with format_version 8 the data-block common-prefix
  // section leaves a non-NUL byte after the value, so strcmp (ASSERT_STREQ)
  // would read past the value. The value bytes themselves are unchanged.
  ASSERT_EQ(value.ToString(), "hello");
  BlockCachePropertiesSnapshot props(options.statistics.get());
  props.AssertFilterBlockStat(0, 0);
  c3.ResetTableReader();
}

void ValidateBlockSizeDeviation(int value, int expected) {
  BlockBasedTableOptions table_options;
  table_options.block_size_deviation = value;
  BlockBasedTableFactory* factory = new BlockBasedTableFactory(table_options);

  const BlockBasedTableOptions* normalized_table_options =
      factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(normalized_table_options->block_size_deviation, expected);

  delete factory;
}

void ValidateBlockRestartInterval(int value, int expected) {
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = value;
  BlockBasedTableFactory* factory = new BlockBasedTableFactory(table_options);

  const BlockBasedTableOptions* normalized_table_options =
      factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(normalized_table_options->block_restart_interval, expected);

  delete factory;
}

TEST_P(BlockBasedTableTest, InvalidOptions) {
  // invalid values for block_size_deviation (<0 or >100) are silently set to
  // 0
  ValidateBlockSizeDeviation(-10, 0);
  ValidateBlockSizeDeviation(-1, 0);
  ValidateBlockSizeDeviation(0, 0);
  ValidateBlockSizeDeviation(1, 1);
  ValidateBlockSizeDeviation(99, 99);
  ValidateBlockSizeDeviation(100, 100);
  ValidateBlockSizeDeviation(101, 0);
  ValidateBlockSizeDeviation(1000, 0);

  // invalid values for block_restart_interval (<1) are silently set to 1
  ValidateBlockRestartInterval(-10, 1);
  ValidateBlockRestartInterval(-1, 1);
  ValidateBlockRestartInterval(0, 1);
  ValidateBlockRestartInterval(1, 1);
  ValidateBlockRestartInterval(2, 2);
  ValidateBlockRestartInterval(1000, 1000);
}

TEST_P(BlockBasedTableTest, BlockReadCountTest) {
  // bloom_filter_type = 1 -- full filter using use_block_based_builder=false
  // bloom_filter_type = 2 -- full filter using use_block_based_builder=true
  //                          because of API change to hide block-based filter
  for (int bloom_filter_type = 1; bloom_filter_type <= 2; ++bloom_filter_type) {
    for (int index_and_filter_in_cache = 0; index_and_filter_in_cache < 2;
         ++index_and_filter_in_cache) {
      Options options;
      options.create_if_missing = true;

      BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
      table_options.block_cache = NewLRUCache(1, 0);
      table_options.cache_index_and_filter_blocks = index_and_filter_in_cache;
      table_options.filter_policy.reset(
          NewBloomFilterPolicy(10, bloom_filter_type == 2));
      options.table_factory.reset(new BlockBasedTableFactory(table_options));
      std::vector<std::string> keys;
      stl_wrappers::KVMap kvmap;

      TableConstructor c(BytewiseComparator());
      std::string user_key = "k04";
      InternalKey internal_key(user_key, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      c.Add(encoded_key, "hello");
      ImmutableOptions ioptions(options);
      MutableCFOptions moptions(options);
      // Generate table with filter policy
      c.Finish(options, ioptions, moptions, table_options,
               GetPlainInternalComparator(options.comparator), &keys, &kvmap);
      auto reader = c.GetTableReader();
      PinnableSlice value;
      {
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        get_perf_context()->Reset();
        ASSERT_OK(reader->Get(ReadOptions(), encoded_key, &get_context,
                              moptions.prefix_extractor.get()));
        const uint64_t total_classified_bytes =
            get_perf_context()->data_block_read_byte +
            get_perf_context()->index_block_read_byte +
            get_perf_context()->filter_block_read_byte +
            get_perf_context()->compression_dict_block_read_byte +
            get_perf_context()->metadata_block_read_byte;
        ASSERT_EQ(get_perf_context()->block_read_byte, total_classified_bytes);
        if (index_and_filter_in_cache) {
          // data, index and filter block
          ASSERT_EQ(get_perf_context()->block_read_count, 3);
          ASSERT_EQ(get_perf_context()->index_block_read_count, 1);
          ASSERT_EQ(get_perf_context()->filter_block_read_count, 1);
          ASSERT_GT(get_perf_context()->data_block_read_byte, 0);
          ASSERT_GT(get_perf_context()->index_block_read_byte, 0);
          ASSERT_GT(get_perf_context()->filter_block_read_byte, 0);
        } else {
          // just the data block
          ASSERT_EQ(get_perf_context()->block_read_count, 1);
          ASSERT_EQ(get_perf_context()->block_read_byte,
                    get_perf_context()->data_block_read_byte);
        }
        ASSERT_EQ(get_context.State(), GetContext::kFound);
        // Length-aware compare: format_version 8's common-prefix section leaves
        // a non-NUL byte after the value; strcmp would over-read. Value bytes
        // are unchanged.
        ASSERT_EQ(value.ToString(), "hello");
      }

      // Get non-existing key
      user_key = "does-not-exist";
      internal_key = InternalKey(user_key, 0, kTypeValue);
      encoded_key = internal_key.Encode().ToString();

      value.Reset();
      {
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        get_perf_context()->Reset();
        ASSERT_OK(reader->Get(ReadOptions(), encoded_key, &get_context,
                              moptions.prefix_extractor.get()));
        const uint64_t total_classified_bytes =
            get_perf_context()->data_block_read_byte +
            get_perf_context()->index_block_read_byte +
            get_perf_context()->filter_block_read_byte +
            get_perf_context()->compression_dict_block_read_byte +
            get_perf_context()->metadata_block_read_byte;
        ASSERT_EQ(get_perf_context()->block_read_byte, total_classified_bytes);
        ASSERT_EQ(get_context.State(), GetContext::kNotFound);
      }

      if (index_and_filter_in_cache) {
        if (bloom_filter_type == 0) {
          // with block-based, we read index and then the filter
          ASSERT_EQ(get_perf_context()->block_read_count, 2);
          ASSERT_EQ(get_perf_context()->index_block_read_count, 1);
          ASSERT_EQ(get_perf_context()->filter_block_read_count, 1);
        } else {
          // with full-filter, we read filter first and then we stop
          ASSERT_EQ(get_perf_context()->block_read_count, 1);
          ASSERT_EQ(get_perf_context()->filter_block_read_count, 1);
          ASSERT_EQ(get_perf_context()->block_read_byte,
                    get_perf_context()->filter_block_read_byte);
        }
      } else {
        // filter is already in memory and it figures out that the key doesn't
        // exist
        ASSERT_EQ(get_perf_context()->block_read_count, 0);
      }
    }
  }
}

TEST_P(BlockBasedTableTest, BlockCacheLeak) {
  // Check that when we reopen a table we don't lose access to blocks already
  // in the cache. This test checks whether the Table actually makes use of
  // the unique ID from the file.

  Options opt;
  std::unique_ptr<InternalKeyComparator> ikc;
  ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
  opt.compression = kNoCompression;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.block_size = 1024;
  // big enough so we don't ever lose cached values.
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewBlockBasedTableFactory(table_options));

  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(opt);
  const MutableCFOptions moptions(opt);
  c.Finish(opt, ioptions, moptions, table_options, *ikc, &keys, &kvmap);

  std::unique_ptr<InternalIterator> iter(
      c.NewIterator(moptions.prefix_extractor.get()));
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->key();
    iter->value();
    iter->Next();
  }
  ASSERT_OK(iter->status());
  iter.reset();

  const ImmutableOptions ioptions1(opt);
  const MutableCFOptions moptions1(opt);
  ASSERT_OK(c.Reopen(ioptions1, moptions1));
  auto table_reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  for (const std::string& key : keys) {
    InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
    ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
  }
  c.ResetTableReader();

  // rerun with different block cache
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions2(opt);
  const MutableCFOptions moptions2(opt);
  ASSERT_OK(c.Reopen(ioptions2, moptions2));
  table_reader = dynamic_cast<BlockBasedTable*>(c.GetTableReader());
  for (const std::string& key : keys) {
    InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
    ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
  }
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, MemoryAllocator) {
  auto default_memory_allocator = std::make_shared<DefaultMemoryAllocator>();
  auto custom_memory_allocator =
      std::make_shared<CountedMemoryAllocator>(default_memory_allocator);
  {
    Options opt;
    std::unique_ptr<InternalKeyComparator> ikc;
    ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
    opt.compression = kNoCompression;
    BlockBasedTableOptions table_options;
    table_options.block_size = 1024;
    LRUCacheOptions lruOptions;
    lruOptions.memory_allocator = custom_memory_allocator;
    lruOptions.capacity = 16 * 1024 * 1024;
    lruOptions.num_shard_bits = 4;
    table_options.block_cache = NewLRUCache(std::move(lruOptions));
    opt.table_factory.reset(NewBlockBasedTableFactory(table_options));

    TableConstructor c(BytewiseComparator(),
                       true /* convert_to_internal_key_ */);
    c.Add("k01", "hello");
    c.Add("k02", "hello2");
    c.Add("k03", std::string(10000, 'x'));
    c.Add("k04", std::string(200000, 'x'));
    c.Add("k05", std::string(300000, 'x'));
    c.Add("k06", "hello3");
    c.Add("k07", std::string(100000, 'x'));
    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;
    const ImmutableOptions ioptions(opt);
    const MutableCFOptions moptions(opt);
    c.Finish(opt, ioptions, moptions, table_options, *ikc, &keys, &kvmap);

    std::unique_ptr<InternalIterator> iter(
        c.NewIterator(moptions.prefix_extractor.get()));
    iter->SeekToFirst();
    while (iter->Valid()) {
      iter->key();
      iter->value();
      iter->Next();
    }
    ASSERT_OK(iter->status());
  }

  // out of scope, block cache should have been deleted, all allocations
  // deallocated
  EXPECT_EQ(custom_memory_allocator->GetNumAllocations(),
            custom_memory_allocator->GetNumDeallocations());
  // make sure that allocations actually happened through the cache allocator
  EXPECT_GT(custom_memory_allocator->GetNumAllocations(), 0);
}

// Test the file checksum of block based table
TEST_P(BlockBasedTableTest, NoFileChecksum) {
  Options options;
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  int level = 0;
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  std::string column_family_name;

  FileChecksumTestHelper f(true);
  f.CreateWritableFile();
  std::unique_ptr<TableBuilder> builder;
  const ReadOptions read_options;
  const WriteOptions write_options;
  builder.reset(moptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options,
                          *comparator, &internal_tbl_prop_coll_factories,
                          options.compression, options.compression_opts,
                          kUnknownColumnFamily, column_family_name, level,
                          kUnknownNewestKeyTime),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), kUnknownFileChecksumFuncName);
  ASSERT_STREQ(f.GetFileChecksum().c_str(), kUnknownFileChecksum);
}

TEST_P(BlockBasedTableTest, Crc32cFileChecksum) {
  FileChecksumGenCrc32cFactory* file_checksum_gen_factory =
      new FileChecksumGenCrc32cFactory();
  Options options;
  options.file_checksum_gen_factory.reset(file_checksum_gen_factory);
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  int level = 0;
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  std::string column_family_name;

  FileChecksumGenContext gen_context;
  gen_context.file_name = "db/tmp";
  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen1 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  FileChecksumTestHelper f(true);
  f.CreateWritableFile();
  f.SetFileChecksumGenerator(checksum_crc32c_gen1.release());
  std::unique_ptr<TableBuilder> builder;
  const ReadOptions read_options;
  const WriteOptions write_options;
  builder.reset(moptions.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options,
                          *comparator, &internal_tbl_prop_coll_factories,
                          options.compression, options.compression_opts,
                          kUnknownColumnFamily, column_family_name, level,
                          kUnknownNewestKeyTime),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), "FileChecksumCrc32c");

  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen2 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  std::string checksum;
  ASSERT_OK(f.CalculateFileChecksum(checksum_crc32c_gen2.get(), &checksum));
  ASSERT_STREQ(f.GetFileChecksum().c_str(), checksum.c_str());

  // Unit test the generator itself for schema stability
  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen3 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  const char data[] = "here is some data";
  checksum_crc32c_gen3->Update(data, sizeof(data));
  checksum_crc32c_gen3->Finalize();
  checksum = checksum_crc32c_gen3->GetChecksum();
  ASSERT_STREQ(checksum.c_str(), "\345\245\277\110");
}

TEST_F(PlainTableTest, BasicPlainTableProperties) {
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 8;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;

  PlainTableFactory factory(plain_table_options);
  std::unique_ptr<FSWritableFile> sink(new test::StringSink());
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(sink), "" /* don't care */, FileOptions()));
  Options options;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  std::string column_family_name;
  int unknown_level = -1;
  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, unknown_level,
                          kUnknownNewestKeyTime),
      file_writer.get()));

  for (char c = 'a'; c <= 'z'; ++c) {
    std::string key(8, c);
    key.append("\1       ");  // PlainTable expects internal key structure
    std::string value(28, c + 42);
    builder->Add(key, value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush(IOOptions()));

  test::StringSink* ss =
      static_cast<test::StringSink*>(file_writer->writable_file());
  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(ss->contents(), 72242, true));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test"));

  std::unique_ptr<TableProperties> props;
  auto s = ReadTableProperties(file_reader.get(), ss->contents().size(),
                               kPlainTableMagicNumber, ioptions, read_options,
                               &props);
  ASSERT_OK(s);

  ASSERT_EQ(0ul, props->index_size);
  ASSERT_EQ(0ul, props->filter_size);
  ASSERT_EQ(16ul * 26, props->raw_key_size);
  ASSERT_EQ(28ul * 26, props->raw_value_size);
  ASSERT_EQ(26ul, props->num_entries);
  ASSERT_EQ(1ul, props->num_data_blocks);
}

TEST_F(PlainTableTest, NoFileChecksum) {
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 20;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;
  PlainTableFactory factory(plain_table_options);

  Options options;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  std::string column_family_name;
  int unknown_level = -1;
  FileChecksumTestHelper f(true);
  f.CreateWritableFile();
  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, unknown_level,
                          kUnknownNewestKeyTime),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), kUnknownFileChecksumFuncName);
  EXPECT_EQ(f.GetFileChecksum(), kUnknownFileChecksum);
}

TEST_F(PlainTableTest, Crc32cFileChecksum) {
  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 20;
  plain_table_options.bloom_bits_per_key = 8;
  plain_table_options.hash_table_ratio = 0;
  PlainTableFactory factory(plain_table_options);

  FileChecksumGenCrc32cFactory* file_checksum_gen_factory =
      new FileChecksumGenCrc32cFactory();
  Options options;
  options.file_checksum_gen_factory.reset(file_checksum_gen_factory);
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  std::string column_family_name;
  int unknown_level = -1;

  FileChecksumGenContext gen_context;
  gen_context.file_name = "db/tmp";
  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen1 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  FileChecksumTestHelper f(true);
  f.CreateWritableFile();
  f.SetFileChecksumGenerator(checksum_crc32c_gen1.release());
  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, unknown_level,
                          kUnknownNewestKeyTime),
      f.GetFileWriter()));
  ASSERT_OK(f.ResetTableBuilder(std::move(builder)));
  f.AddKVtoKVMap(1000);
  ASSERT_OK(f.WriteKVAndFlushTable());
  ASSERT_STREQ(f.GetFileChecksumFuncName(), "FileChecksumCrc32c");

  std::unique_ptr<FileChecksumGenerator> checksum_crc32c_gen2 =
      options.file_checksum_gen_factory->CreateFileChecksumGenerator(
          gen_context);
  std::string checksum;
  ASSERT_OK(f.CalculateFileChecksum(checksum_crc32c_gen2.get(), &checksum));
  EXPECT_STREQ(f.GetFileChecksum().c_str(), checksum.c_str());
}

TEST_F(GeneralTableTest, ApproximateOffsetOfPlain) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.db_host_id = "";
  test::PlainInternalKeyComparator internal_comparator(options.comparator);
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01a"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 10000, 11000));
  // k04 and k05 will be in two consecutive blocks, the index is
  // an arbitrary slice between k04 and k05, either before or after k04a
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04a"), 10000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k05"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k06"), 510000, 512000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k07"), 510000, 512000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 610000, 612000));
  c.ResetTableReader();
}

static void DoCompressionTest(CompressionType comp) {
  SCOPED_TRACE("CompressionType = " + CompressionTypeToString(comp));
  Random rnd(301);
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  std::string tmp;
  c.Add("k01", "hello");
  c.Add("k02", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  c.Add("k03", "hello3");
  c.Add("k04", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  test::PlainInternalKeyComparator ikc(options.comparator);
  options.compression = comp;
  options.db_host_id = "";
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  options.table_factory.reset(new BlockBasedTableFactory(table_options));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, ikc, &keys, &kvmap);
  size_t file_size = c.TEST_GetSink()->contents().size();
  EXPECT_EQ(c.ApproximateOffsetOf("abc"), 0);
  EXPECT_EQ(c.ApproximateOffsetOf("k01"), 0);
  EXPECT_EQ(c.ApproximateOffsetOf("k02"), 0);
  EXPECT_NEAR2(c.ApproximateOffsetOf("k03"), file_size / 2, file_size / 10);
  EXPECT_NEAR2(c.ApproximateOffsetOf("k04"), file_size / 2, file_size / 10);
  EXPECT_NEAR2(c.ApproximateOffsetOf("xyz"), file_size, file_size / 10);

  size_t data_blocks_size = c.GetTableReader()->GetTableProperties()->data_size;
  // Near expected compressed size ~= (0.25 + 0.25) * 10000
  EXPECT_NEAR2(data_blocks_size, 5000, 1500);

  c.ResetTableReader();
}

TEST_F(GeneralTableTest, ApproximateOffsetOfCompressed) {
  std::vector<CompressionType> compression_state;
  if (!Snappy_Supported()) {
    fprintf(stderr, "skipping snappy compression tests\n");
  } else {
    compression_state.push_back(kSnappyCompression);
  }

  if (!Zlib_Supported()) {
    fprintf(stderr, "skipping zlib compression tests\n");
  } else {
    compression_state.push_back(kZlibCompression);
  }

  // TODO(kailiu) DoCompressionTest() doesn't work with BZip2.
  /*
  if (!BZip2_Supported()) {
    fprintf(stderr, "skipping bzip2 compression tests\n");
  } else {
    compression_state.push_back(kBZip2Compression);
  }
  */

  if (!LZ4_Supported()) {
    fprintf(stderr, "skipping lz4 and lz4hc compression tests\n");
  } else {
    compression_state.push_back(kLZ4Compression);
    compression_state.push_back(kLZ4HCCompression);
  }

  if (!XPRESS_Supported()) {
    fprintf(stderr, "skipping xpress and xpress compression tests\n");
  } else {
    compression_state.push_back(kXpressCompression);
  }

  for (auto state : compression_state) {
    DoCompressionTest(state);
  }
}

TEST_F(GeneralTableTest, ApproximateKeyAnchors) {
  Random rnd(301);
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  std::string tmp;
  for (int i = 1000; i < 9000; i++) {
    c.Add(std::to_string(i), rnd.RandomString(2000));
  }
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  InternalKeyComparator ikc(options.comparator);
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 4096;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, ikc, &keys, &kvmap);

  std::vector<TableReader::Anchor> anchors;
  ASSERT_OK(c.GetTableReader()->ApproximateKeyAnchors(ReadOptions(), anchors));
  // The target is 128 anchors. But in reality it can be slightly more or
  // fewer.
  ASSERT_GT(anchors.size(), 120);
  ASSERT_LT(anchors.size(), 140);

  // We have around 8000 keys. With 128 anchors, in average 62.5 keys per
  // anchor. Here we take a rough range and estimate the distance between
  // anchors is between 50 and 100.
  // Total data size is about 18,000,000, so each anchor range is about
  // 140,625. We also take a rough range.
  int prev_num = 1000;
  // Non-last anchor
  for (size_t i = 0; i + 1 < anchors.size(); i++) {
    auto& anchor = anchors[i];
    ASSERT_GT(anchor.range_size, 100000);
    ASSERT_LT(anchor.range_size, 200000);

    // Key might be shortened, so fill 0 in the end if it is the case.
    std::string key_cpy = anchor.user_key;
    key_cpy.append(4 - key_cpy.size(), '0');
    int num = std::stoi(key_cpy);
    ASSERT_GT(num - prev_num, 50);
    ASSERT_LT(num - prev_num, 100);
    prev_num = num;
  }

  ASSERT_EQ("8999", anchors.back().user_key);
  ASSERT_LT(anchors.back().range_size, 200000);

  c.ResetTableReader();
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_P(ParameterizedHarnessTest, RandomizedHarnessTest) {
  Random rnd(test::RandomSeed() + 5);
  for (int num_entries = 0; num_entries < 2000;
       num_entries += (num_entries < 50 ? 1 : 200)) {
    for (int e = 0; e < num_entries; e++) {
      Add(test::RandomKey(&rnd, rnd.Skewed(4)),
          rnd.RandomString(rnd.Skewed(5)));
    }
    Test(&rnd);
  }
}

TEST_F(DBHarnessTest, RandomizedLongDB) {
  Random rnd(test::RandomSeed());
  int num_entries = 100000;
  for (int e = 0; e < num_entries; e++) {
    std::string v;
    Add(test::RandomKey(&rnd, rnd.Skewed(4)), rnd.RandomString(rnd.Skewed(5)));
  }
  Test(&rnd);

  // We must have created enough data to force merging
  int files = 0;
  for (int level = 0; level < db()->NumberLevels(); level++) {
    std::string value;
    char name[100];
    snprintf(name, sizeof(name), "rocksdb.num-files-at-level%d", level);
    ASSERT_TRUE(db()->GetProperty(name, &value));
    files += atoi(value.c_str());
  }
  ASSERT_GT(files, 0);
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

class MemTableTest : public testing::Test {
 public:
  MemTableTest() {
    InternalKeyComparator cmp(BytewiseComparator());
    auto table_factory = std::make_shared<SkipListFactory>();
    options_.memtable_factory = table_factory;
    ImmutableOptions ioptions(options_);
    wb_ = new WriteBufferManager(options_.db_write_buffer_size);
    memtable_ = new MemTable(cmp, ioptions, MutableCFOptions(options_), wb_,
                             kMaxSequenceNumber, 0 /* column_family_id */,
                             nullptr /* flush_initiator */);
    memtable_->Ref();
  }

  ~MemTableTest() override {
    delete memtable_->Unref();
    delete wb_;
  }

  MemTable* GetMemTable() { return memtable_; }

 private:
  MemTable* memtable_;
  Options options_;
  WriteBufferManager* wb_;
};

TEST_F(MemTableTest, Simple) {
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  ASSERT_OK(batch.Put(std::string("k1"), std::string("v1")));
  ASSERT_OK(batch.Put(std::string("k2"), std::string("v2")));
  ASSERT_OK(batch.Put(std::string("k3"), std::string("v3")));
  ASSERT_OK(batch.Put(std::string("largekey"), std::string("vlarge")));
  ASSERT_OK(batch.DeleteRange(std::string("chi"), std::string("xigua")));
  ASSERT_OK(batch.DeleteRange(std::string("begin"), std::string("end")));
  ColumnFamilyMemTablesDefault cf_mems_default(GetMemTable());
  ASSERT_TRUE(
      WriteBatchInternal::InsertInto(&batch, &cf_mems_default, nullptr, nullptr)
          .ok());

  for (int i = 0; i < 2; ++i) {
    Arena arena;
    ScopedArenaPtr<InternalIterator> arena_iter_guard;
    std::unique_ptr<InternalIterator> iter_guard;
    InternalIterator* iter;
    if (i == 0) {
      iter = GetMemTable()->NewIterator(ReadOptions(),
                                        /*seqno_to_time_mapping=*/nullptr,
                                        &arena, /*prefix_extractor=*/nullptr,
                                        /*for_flush=*/false);
      arena_iter_guard.reset(iter);
    } else {
      iter = GetMemTable()->NewRangeTombstoneIterator(
          ReadOptions(), kMaxSequenceNumber /* read_seq */,
          false /* immutable_memtable */);
      iter_guard.reset(iter);
    }
    if (iter == nullptr) {
      continue;
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
      fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
              iter->value().ToString().c_str());
      iter->Next();
    }
  }
}

// Test the empty key
TEST_P(ParameterizedHarnessTest, SimpleEmptyKey) {
  Random rnd(test::RandomSeed() + 1);
  Add("", "v");
  Test(&rnd);
}

TEST_P(ParameterizedHarnessTest, SimpleSingle) {
  Random rnd(test::RandomSeed() + 2);
  Add("abc", "v");
  Test(&rnd);
}

TEST_P(ParameterizedHarnessTest, SimpleMulti) {
  Random rnd(test::RandomSeed() + 3);
  Add("abc", "v");
  Add("abcd", "v");
  Add("ac", "v2");
  Test(&rnd);
}

TEST_P(ParameterizedHarnessTest, SimpleSpecialKey) {
  Random rnd(test::RandomSeed() + 4);
  Add("\xff\xff", "v3");
  Test(&rnd);
}

TEST(TableTest, FooterTests) {
  Random* r = Random::GetTLSInstance();
  uint64_t data_size = (uint64_t{1} << r->Uniform(40)) + r->Uniform(100);
  uint64_t index_size = r->Uniform(1000000000);
  uint64_t metaindex_size = r->Uniform(1000000);
  // 5 == block trailer size
  BlockHandle index(data_size + 5, index_size);
  uint32_t base_context_checksum = 123456789;
  // block based, various checksums, various versions (format_version >= 2)
  for (auto t : GetSupportedChecksums()) {
    for (uint32_t fv = kMinSupportedBbtFormatVersionForWrite;
         IsSupportedFormatVersionForWrite(kBlockBasedTableMagicNumber, fv);
         ++fv) {
      uint32_t maybe_bcc =
          FormatVersionUsesContextChecksum(fv) ? base_context_checksum : 0U;
      // format_version >= 8 stores the metaindex block size in only the low 16
      // bits of the footer (the high 16 bits hold the metaindex-to-footer gap),
      // so cap the size for those versions; metaindex blocks are small in
      // practice.
      uint64_t this_metaindex_size = FormatVersionUsesMetaindexGap(fv)
                                         ? (metaindex_size & 0xFFFFU)
                                         : metaindex_size;
      BlockHandle meta_index(data_size + index_size + 2 * 5,
                             this_metaindex_size);
      uint64_t footer_offset =
          data_size + this_metaindex_size + index_size + 3 * 5;
      FooterBuilder footer;
      ASSERT_OK(footer.Build(kBlockBasedTableMagicNumber, fv, footer_offset, t,
                             meta_index, index, maybe_bcc));
      Footer decoded_footer;
      ASSERT_OK(decoded_footer.DecodeFrom(footer.GetSlice(), footer_offset));
      ASSERT_EQ(decoded_footer.table_magic_number(),
                kBlockBasedTableMagicNumber);
      ASSERT_EQ(decoded_footer.checksum_type(), t);
      ASSERT_EQ(decoded_footer.metaindex_handle().offset(),
                meta_index.offset());
      ASSERT_EQ(decoded_footer.metaindex_handle().size(), meta_index.size());
      if (FormatVersionUsesIndexHandleInFooter(fv)) {
        ASSERT_EQ(decoded_footer.index_handle().offset(), index.offset());
        ASSERT_EQ(decoded_footer.index_handle().size(), index.size());
      }
      ASSERT_EQ(decoded_footer.format_version(), fv);
      ASSERT_EQ(decoded_footer.GetBlockTrailerSize(), 5U);

      if (FormatVersionUsesContextChecksum(fv)) {
        ASSERT_EQ(decoded_footer.base_context_checksum(),
                  base_context_checksum);

        // Bad offset should fail footer checksum
        decoded_footer = Footer();
        ASSERT_NOK(
            decoded_footer.DecodeFrom(footer.GetSlice(), footer_offset - 1));
      } else {
        ASSERT_EQ(decoded_footer.base_context_checksum(), 0U);
      }

      // Too big metaindex size should also fail encoding only in new footer
      uint64_t big_metaindex_size = 0x100000007U;
      uint64_t big_footer_offset =
          data_size + big_metaindex_size + index_size + 3 * 5;
      BlockHandle big_metaindex =
          BlockHandle(data_size + index_size + 2 * 5, big_metaindex_size);
      ASSERT_NE(footer
                    .Build(kBlockBasedTableMagicNumber, fv, big_footer_offset,
                           t, big_metaindex, index, maybe_bcc)
                    .ok(),
                FormatVersionUsesContextChecksum(fv));
    }
  }

  // plain table, various checksums, various versions (format_version >= 2)
  // Plain tables have no block trailer (size 0), so set up separate handles
  // Note: format_version >= 6 has complex footer checksum requirements,
  // so we only test format_version 2-5 for plain tables here
  {
    uint64_t plain_metaindex_size = r->Uniform(1000000);
    // For plain tables: metaindex is at offset 0, footer immediately follows
    BlockHandle plain_meta_index(0, plain_metaindex_size);
    uint64_t plain_footer_offset = plain_metaindex_size;
    for (auto t : GetSupportedChecksums()) {
      for (uint32_t fv = kMinSupportedBbtFormatVersionForWrite; fv < 6; ++fv) {
        FooterBuilder footer;
        ASSERT_OK(footer.Build(kPlainTableMagicNumber, fv, plain_footer_offset,
                               t, plain_meta_index));
        Footer decoded_footer;
        ASSERT_OK(
            decoded_footer.DecodeFrom(footer.GetSlice(), plain_footer_offset));
        ASSERT_EQ(decoded_footer.table_magic_number(), kPlainTableMagicNumber);
        ASSERT_EQ(decoded_footer.checksum_type(), t);
        ASSERT_EQ(decoded_footer.metaindex_handle().offset(),
                  plain_meta_index.offset());
        ASSERT_EQ(decoded_footer.metaindex_handle().size(),
                  plain_meta_index.size());
        ASSERT_EQ(decoded_footer.format_version(), fv);
        ASSERT_EQ(decoded_footer.GetBlockTrailerSize(), 0U);
      }
    }
  }
}

// Test that legacy SST formats (format_version < 2) are properly rejected
TEST(TableTest, LegacyFormatRejectionTests) {
  // Temporarily disable unsupported format version allowance for this test
  bool& allow = TEST_AllowUnsupportedFormatVersion();
  SaveAndRestore<bool> saved_allow(&allow, false);

  // Test legacy block-based magic number from LevelDB should be rejected
  {
    // Construct a fake footer with legacy block-based magic number
    std::array<char, Footer::kVersion0EncodedLength> fake_footer;
    std::fill(fake_footer.begin(), fake_footer.end(), 0);
    // Put legacy magic number at the end
    EncodeFixed64(fake_footer.data() + fake_footer.size() - 8,
                  0xdb4775248b80fb57ull /*legacy magic number*/);

    Footer decoded_footer;
    Status s = decoded_footer.DecodeFrom(
        Slice(fake_footer.data(), fake_footer.size()), 0);
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_TRUE(s.ToString().find("nsupported legacy magic number") !=
                std::string::npos)
        << s.ToString();
    ASSERT_TRUE(s.ToString().find("full compaction") != std::string::npos)
        << s.ToString();
  }

  // Test format_version=1 with new magic number should be rejected
  {
    std::array<char, Footer::kNewVersionsEncodedLength> fake_footer;
    std::fill(fake_footer.begin(), fake_footer.end(), 0);
    // Part 1: checksum type
    fake_footer[0] = kCRC32c;
    // Part 3: format_version=1 and new magic number
    char* part3 = fake_footer.data() + fake_footer.size() - 12;
    EncodeFixed32(part3, 1);  // format_version = 1
    EncodeFixed64(part3 + 4, kBlockBasedTableMagicNumber);

    Footer decoded_footer;
    Status s = decoded_footer.DecodeFrom(
        Slice(fake_footer.data(), fake_footer.size()), 0);
    // format_version=1 is not supported for read, should return Corruption
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_TRUE(s.ToString().find("format_version") != std::string::npos)
        << s.ToString();
  }

  // Test format_version=0 with new magic number should be rejected
  {
    std::array<char, Footer::kNewVersionsEncodedLength> fake_footer;
    std::fill(fake_footer.begin(), fake_footer.end(), 0);
    // Part 1: checksum type
    fake_footer[0] = kCRC32c;
    // Part 3: format_version=0 and new magic number
    char* part3 = fake_footer.data() + fake_footer.size() - 12;
    EncodeFixed32(part3, 0);  // format_version = 0
    EncodeFixed64(part3 + 4, kBlockBasedTableMagicNumber);

    Footer decoded_footer;
    Status s = decoded_footer.DecodeFrom(
        Slice(fake_footer.data(), fake_footer.size()), 0);
    // format_version=0 is not supported for read, should return Corruption
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_TRUE(s.ToString().find("format_version") != std::string::npos)
        << s.ToString();
  }
}

// Test that configuring unsupported format_version for writing is sanitized
// or rejected as appropriate
TEST(TableTest, UnsupportedFormatVersionConfigTest) {
  // Temporarily disable unsupported format version allowance for this test
  bool& allow = TEST_AllowUnsupportedFormatVersion();
  SaveAndRestore<bool> saved_allow(&allow, false);

  // Test that format_version < kMinSupportedBbtFormatVersionForWrite is
  // sanitized to kMinSupportedBbtFormatVersionForWrite during initialization
  for (uint32_t fv = 0; fv < kMinSupportedBbtFormatVersionForWrite; ++fv) {
    BlockBasedTableOptions table_options;
    table_options.format_version = fv;
    BlockBasedTableFactory factory(table_options);

    // After construction, format_version should be sanitized
    auto* opts = factory.GetOptions<BlockBasedTableOptions>();
    ASSERT_EQ(opts->format_version, kMinSupportedBbtFormatVersionForWrite)
        << "format_version=" << fv << " should be sanitized to "
        << kMinSupportedBbtFormatVersionForWrite;
  }

  // Test that supported format versions are not changed
  for (uint32_t fv = kMinSupportedBbtFormatVersionForWrite;
       IsSupportedFormatVersionForWrite(kBlockBasedTableMagicNumber, fv);
       ++fv) {
    BlockBasedTableOptions table_options;
    table_options.format_version = fv;
    BlockBasedTableFactory factory(table_options);

    auto* opts = factory.GetOptions<BlockBasedTableOptions>();
    ASSERT_EQ(opts->format_version, fv)
        << "format_version=" << fv << " should not be changed";

    ColumnFamilyOptions cf_opts;
    DBOptions db_opts;
    Status s = factory.ValidateOptions(db_opts, cf_opts);
    ASSERT_OK(s) << "format_version=" << fv << ": " << s.ToString();
  }

  // Test that format_version > kLatestSupportedFormatVersion is rejected by
  // ValidateOptions (not sanitized, since it could be a future version that
  // requires newer code)
  {
    BlockBasedTableOptions table_options;
    table_options.format_version =
        BlockBasedTableOptions::kLatestSupportedFormatVersion + 1;
    BlockBasedTableFactory factory(table_options);

    ColumnFamilyOptions cf_opts;
    DBOptions db_opts;
    Status s = factory.ValidateOptions(db_opts, cf_opts);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  }
}

class IndexBlockRestartIntervalTest
    : public TableTest,
      public ::testing::WithParamInterface<std::pair<int, bool>> {
 public:
  static std::vector<std::pair<int, bool>> GetRestartValues() {
    return {{-1, false}, {0, false},  {1, false}, {8, false},
            {16, false}, {32, false}, {-1, true}, {0, true},
            {1, true},   {8, true},   {16, true}, {32, true}};
  }
};

INSTANTIATE_TEST_CASE_P(
    IndexBlockRestartIntervalTest, IndexBlockRestartIntervalTest,
    ::testing::ValuesIn(IndexBlockRestartIntervalTest::GetRestartValues()));

TEST_P(IndexBlockRestartIntervalTest, IndexBlockRestartInterval) {
  const int kKeysInTable = 10000;
  const int kKeySize = 100;
  const int kValSize = 500;

  const int index_block_restart_interval = std::get<0>(GetParam());
  const bool value_delta_encoding = std::get<1>(GetParam());

  Options options;
  BlockBasedTableOptions table_options;
  table_options.block_size = 64;  // small block size to get big index block
  table_options.index_block_restart_interval = index_block_restart_interval;
  if (value_delta_encoding) {
    table_options.format_version = 4;
  } else {
    table_options.format_version = 3;
  }
  options.table_factory.reset(new BlockBasedTableFactory(table_options));

  TableConstructor c(BytewiseComparator());
  static Random rnd(301);
  for (int i = 0; i < kKeysInTable; i++) {
    InternalKey k(rnd.RandomString(kKeySize), 0, kTypeValue);
    c.Add(k.Encode().ToString(), rnd.RandomString(kValSize));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options, *comparator, &keys,
           &kvmap);
  auto reader = c.GetTableReader();

  ReadOptions read_options;
  std::unique_ptr<InternalIterator> db_iter(reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  // Test point lookup
  for (auto& kv : kvmap) {
    db_iter->Seek(kv.first);

    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(db_iter->key(), kv.first);
    ASSERT_EQ(db_iter->value(), kv.second);
  }

  // Test iterating
  auto kv_iter = kvmap.begin();
  for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
    ASSERT_EQ(db_iter->key(), kv_iter->first);
    ASSERT_EQ(db_iter->value(), kv_iter->second);
    kv_iter++;
  }
  ASSERT_EQ(kv_iter, kvmap.end());
  c.ResetTableReader();
}

class PrefixTest : public testing::Test {
 public:
  PrefixTest() : testing::Test() {}
  ~PrefixTest() override = default;
};

namespace {
// A simple PrefixExtractor that only works for test PrefixAndWholeKeyTest
class TestPrefixExtractor : public ROCKSDB_NAMESPACE::SliceTransform {
 public:
  ~TestPrefixExtractor() override = default;
  const char* Name() const override { return "TestPrefixExtractor"; }

  ROCKSDB_NAMESPACE::Slice Transform(
      const ROCKSDB_NAMESPACE::Slice& src) const override {
    assert(IsValid(src));
    return ROCKSDB_NAMESPACE::Slice(src.data(), 3);
  }

  bool InDomain(const ROCKSDB_NAMESPACE::Slice& src) const override {
    return IsValid(src);
  }

  bool IsValid(const ROCKSDB_NAMESPACE::Slice& src) const {
    if (src.size() != 4) {
      return false;
    }
    if (src[0] != '[') {
      return false;
    }
    if (src[1] < '0' || src[1] > '9') {
      return false;
    }
    if (src[2] != ']') {
      return false;
    }
    if (src[3] < '0' || src[3] > '9') {
      return false;
    }
    return true;
  }
};
}  // namespace

TEST_F(PrefixTest, PrefixAndWholeKeyTest) {
  ROCKSDB_NAMESPACE::Options options;
  options.compaction_style = ROCKSDB_NAMESPACE::kCompactionStyleUniversal;
  options.num_levels = 20;
  options.create_if_missing = true;
  options.optimize_filters_for_hits = false;
  options.target_file_size_base = 268435456;
  options.prefix_extractor = std::make_shared<TestPrefixExtractor>();
  ROCKSDB_NAMESPACE::BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(ROCKSDB_NAMESPACE::NewBloomFilterPolicy(10));
  bbto.block_size = 262144;
  bbto.whole_key_filtering = true;

  const std::string kDBPath = test::PerThreadDBPath("table_prefix_test");
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(DestroyDB(kDBPath, options));
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> db;
  ASSERT_OK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));

  // Create a bunch of keys with 10 filters.
  for (int i = 0; i < 10; i++) {
    std::string prefix = "[" + std::to_string(i) + "]";
    for (int j = 0; j < 10; j++) {
      std::string key = prefix + std::to_string(j);
      ASSERT_OK(db->Put(ROCKSDB_NAMESPACE::WriteOptions(), key, "1"));
    }
  }

  // Trigger compaction.
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  db.reset();
  // In the second round, turn whole_key_filtering off and expect
  // rocksdb still works.
}

TEST_P(BlockBasedTableTest, LegacyGlobalSeqnoChecksumRecovery) {
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "" /* don't care */, FileOptions()));
  Options options;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  internal_tbl_prop_coll_factories.emplace_back(
      new SstFileWriterPropertiesCollectorFactory(2 /* version */));
  std::string column_family_name;
  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, -1, kUnknownNewestKeyTime),
      file_writer.get()));

  for (char c = 'a'; c <= 'z'; ++c) {
    std::string key(8, c);
    std::string value = key;
    InternalKey ik(key, 0, kTypeValue);

    builder->Add(ik.Encode(), value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush(IOOptions()));

  test::RandomRWStringSink ss_rw(sink);
  uint32_t version;
  uint64_t global_seqno;
  uint64_t global_seqno_offset = 0;

  // Locate the field without exposing its offset through TableProperties.
  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(ss_rw.contents(), 73342, true));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), ""));
  Footer footer;
  ASSERT_OK(
      ReadFooterFromFile(IOOptions(), file_reader.get(), *FileSystem::Default(),
                         nullptr /* prefetch_buffer */, ss_rw.contents().size(),
                         &footer, kBlockBasedTableMagicNumber));
  BlockHandle properties_handle;
  ASSERT_OK(FindMetaBlockInFile(
      file_reader.get(), ss_rw.contents().size(), kBlockBasedTableMagicNumber,
      ioptions, read_options, kPropertiesBlockName, &properties_handle));
  ReadOptions no_checksum_read_options;
  no_checksum_read_options.verify_checksums = false;
  BlockContents properties_contents;
  BlockFetcher block_fetcher(file_reader.get(), nullptr /* prefetch_buffer */,
                             footer, no_checksum_read_options,
                             properties_handle, &properties_contents, ioptions,
                             false /* decompress */, false /*maybe_compressed*/,
                             BlockType::kProperties, nullptr /*decompressor*/,
                             PersistentCacheOptions::kEmpty);
  ASSERT_OK(block_fetcher.ReadBlockContents());
  Block properties_block(std::move(properties_contents));
  std::unique_ptr<MetaBlockIter> properties_iter(
      properties_block.NewMetaIterator());
  for (properties_iter->SeekToFirst(); properties_iter->Valid();
       properties_iter->Next()) {
    if (properties_iter->key() == ExternalSstFilePropertyNames::kGlobalSeqno) {
      global_seqno_offset =
          properties_handle.offset() + properties_iter->ValueOffset();
      break;
    }
  }
  ASSERT_NE(global_seqno_offset, 0);

  // Helper function to get version and global_seqno.
  std::function<Status(const ReadOptions&)> GetVersionAndGlobalSeqno =
      [&](const ReadOptions& properties_read_options) {
        std::unique_ptr<FSRandomAccessFile> properties_source(
            new test::StringSource(ss_rw.contents(), 73342, true));
        std::unique_ptr<RandomAccessFileReader> properties_file_reader(
            new RandomAccessFileReader(std::move(properties_source), ""));

        std::unique_ptr<TableProperties> props;
        Status s = ReadTableProperties(properties_file_reader.get(),
                                       ss_rw.contents().size(),
                                       kBlockBasedTableMagicNumber, ioptions,
                                       properties_read_options, &props);
        if (!s.ok()) {
          return s;
        }

        UserCollectedProperties user_props = props->user_collected_properties;
        version = DecodeFixed32(
            user_props[ExternalSstFilePropertyNames::kVersion].c_str());
        global_seqno = DecodeFixed64(
            user_props[ExternalSstFilePropertyNames::kGlobalSeqno].c_str());
        return Status::OK();
      };

  std::unique_ptr<FSRandomAccessFile> iterator_source(
      new test::StringSource(ss_rw.contents(), 73342, true));
  std::unique_ptr<RandomAccessFileReader> iterator_file_reader(
      new RandomAccessFileReader(std::move(iterator_source), ""));
  std::unique_ptr<TableReader> table_reader;
  ASSERT_OK(options.table_factory->NewTableReader(
      TableReaderOptions(ioptions, moptions.prefix_extractor,
                         moptions.compression_manager.get(), EnvOptions(), ikc,
                         0 /* block_protection_bytes_per_key */),
      std::move(iterator_file_reader), ss_rw.contents().size(), &table_reader));

  ASSERT_OK(GetVersionAndGlobalSeqno(read_options));
  ASSERT_EQ(2u, version);
  ASSERT_EQ(0u, global_seqno);

  InternalIterator* iter = table_reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized);
  char current_c = 'a';
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey pik;
    ASSERT_OK(ParseInternalKey(iter->key(), &pik, true /* log_err_key */));

    ASSERT_EQ(pik.type, ValueType::kTypeValue);
    ASSERT_EQ(pik.sequence, 0);
    ASSERT_EQ(pik.user_key, iter->value());
    ASSERT_EQ(pik.user_key.ToString(), std::string(8, current_c));
    current_c++;
  }
  ASSERT_EQ(current_c, 'z' + 1);
  delete iter;

  std::string new_global_seqno;
  PutFixed64(&new_global_seqno, 10);
  ASSERT_OK(
      ss_rw.Write(global_seqno_offset, new_global_seqno, IOOptions(), nullptr));

  ASSERT_OK(GetVersionAndGlobalSeqno(read_options));
  ASSERT_EQ(2u, version);
  ASSERT_EQ(10u, global_seqno);

  const uint64_t checksum_offset =
      properties_handle.offset() + properties_handle.size() + 1;
  std::string bad_checksum_byte(
      1, static_cast<char>(ss_rw.contents()[checksum_offset] ^ 0xff));
  ASSERT_OK(
      ss_rw.Write(checksum_offset, bad_checksum_byte, IOOptions(), nullptr));
  ASSERT_TRUE(GetVersionAndGlobalSeqno(read_options).IsCorruption());
  ReadOptions skip_checksum_read_options;
  skip_checksum_read_options.verify_checksums = false;
  ASSERT_OK(GetVersionAndGlobalSeqno(skip_checksum_read_options));
  ASSERT_EQ(2u, version);
  ASSERT_EQ(10u, global_seqno);
}

TEST_P(BlockBasedTableTest, BlockAlignTest) {
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  bbto.block_align = true;
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "" /* don't care */, FileOptions()));
  Options options;
  options.compression = kNoCompression;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(options.table_factory->ValidateOptions(
      DBOptions(options), ColumnFamilyOptions(options)));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  std::string column_family_name;
  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, -1, kUnknownNewestKeyTime),
      file_writer.get()));

  for (int i = 1; i <= 10000; ++i) {
    std::ostringstream ostr;
    ostr << std::setfill('0') << std::setw(5) << i;
    std::string key = ostr.str();
    std::string value = "val";
    InternalKey ik(key, 0, kTypeValue);

    builder->Add(ik.Encode(), value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush(IOOptions()));

  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(sink->contents(), 73342, false));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test"));
  // Verify that data blocks are aligned.
  std::function<void()> VerifyBlockAlignment = [&]() {
    std::unique_ptr<TableProperties> props;
    ASSERT_OK(ReadTableProperties(file_reader.get(), sink->contents().size(),
                                  kBlockBasedTableMagicNumber, ioptions,
                                  read_options, &props));

    uint64_t data_block_size = props->data_size / props->num_data_blocks;
    ASSERT_EQ(data_block_size, 4096);
    ASSERT_EQ(props->data_size, data_block_size * props->num_data_blocks);
  };

  VerifyBlockAlignment();

  // The below block of code verifies that we can read back the keys. Set
  // block_align to false when creating the reader to ensure we can flip between
  // the two modes without any issues
  std::unique_ptr<TableReader> table_reader;
  bbto.block_align = false;
  Options options2;
  options2.compression = kNoCompression;
  options2.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(options2.table_factory->ValidateOptions(
      DBOptions(options2), ColumnFamilyOptions(options2)));
  ImmutableOptions ioptions2(options2);
  const MutableCFOptions moptions2(options2);

  ASSERT_OK(moptions.table_factory->NewTableReader(
      TableReaderOptions(ioptions2, moptions2.prefix_extractor,
                         moptions2.compression_manager.get(), EnvOptions(),
                         GetPlainInternalComparator(options2.comparator),
                         0 /* block_protection_bytes_per_key */),
      std::move(file_reader), sink->contents().size(), &table_reader));

  std::unique_ptr<InternalIterator> db_iter(table_reader->NewIterator(
      read_options, moptions2.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  int expected_key = 1;
  for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
    std::ostringstream ostr;
    ostr << std::setfill('0') << std::setw(5) << expected_key++;
    std::string key = ostr.str();
    std::string value = "val";

    ASSERT_OK(db_iter->status());
    ASSERT_EQ(ExtractUserKey(db_iter->key()).ToString(), key);
    ASSERT_EQ(db_iter->value().ToString(), value);
  }
  expected_key--;
  ASSERT_EQ(expected_key, 10000);
  table_reader.reset();
}

TEST_P(BlockBasedTableTest, FixBlockAlignMismatchedFileChecksums) {
  Options options;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  BlockBasedTableOptions bbto;
  bbto.block_align = true;
  bbto.block_size = 1024;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(options.table_factory->ValidateOptions(
      DBOptions(options), ColumnFamilyOptions(options)));
  const std::string kDBPath =
      test::PerThreadDBPath("block_align_padded_bytes_verify_file_checksums");
  ASSERT_OK(DestroyDB(kDBPath, options));
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, kDBPath, &db));
  ASSERT_OK(db->Put(WriteOptions(), "k1", "v1"));
  ASSERT_OK(db->Flush(FlushOptions()));
  // Before the fix, VerifyFileChecksums() will fail as padded bytes from
  // aligning blocks are used to generate the checksum to compare against the
  // one not generated by padded bytes
  ASSERT_OK(db->VerifyFileChecksums(ReadOptions()));
  db.reset();
}

class NoBufferAlignmenttWritableFile : public FSWritableFileOwnerWrapper {
 public:
  explicit NoBufferAlignmenttWritableFile(
      std::unique_ptr<FSWritableFile>&& file)
      : FSWritableFileOwnerWrapper(std::move(file)) {}
  size_t GetRequiredBufferAlignment() const override { return 1; }
};

class NoBufferAlignmenttWritableFileFileSystem : public FileSystemWrapper {
 public:
  explicit NoBufferAlignmenttWritableFileFileSystem(
      const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() {
    return "NoBufferAlignmenttWritableFileFileSystem";
  }
  const char* Name() const override { return kClassName(); }

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    IOStatus s = target()->NewWritableFile(fname, file_opts, result, dbg);
    EXPECT_OK(s);
    result->reset(new NoBufferAlignmenttWritableFile(std::move(*result)));
    return s;
  }
};

TEST_P(BlockBasedTableTest,
       FixBlockAlignFlushDuringPadMismatchedFileChecksums) {
  Options options;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  // To force flush during pad by enforcing a small buffer size
  options.writable_file_max_buffer_size = 1;
  // To help enforce a small buffer size by removing buffer alignment
  Env* raw_env = Env::Default();
  std::shared_ptr<NoBufferAlignmenttWritableFileFileSystem> fs =
      std::make_shared<NoBufferAlignmenttWritableFileFileSystem>(
          raw_env->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(raw_env, fs));
  options.env = env.get();

  BlockBasedTableOptions bbto;
  bbto.block_align = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  const std::string kDBPath = test::PerThreadDBPath(
      "block_align_flush_during_flush_verify_file_checksums");
  ASSERT_OK(DestroyDB(kDBPath, options));
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, kDBPath, &db));

  ASSERT_OK(db->Put(WriteOptions(), "k1", "k2"));
  ASSERT_OK(db->Flush(FlushOptions()));

  // Before the fix, VerifyFileChecksums() will fail as incorrect padded bytes
  // were used to generate checksum upon file creation
  ASSERT_OK(db->VerifyFileChecksums(ReadOptions()));
  db.reset();
}

TEST_P(BlockBasedTableTest, PropertiesBlockRestartPointTest) {
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  bbto.block_align = true;
  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "" /* don't care */, FileOptions()));

  Options options;
  options.compression = kNoCompression;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(options.table_factory->ValidateOptions(
      DBOptions(options), ColumnFamilyOptions(options)));

  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;
  std::string column_family_name;
  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kNoCompression,
                          CompressionOptions(), kUnknownColumnFamily,
                          column_family_name, -1, kUnknownNewestKeyTime),
      file_writer.get()));

  for (int i = 1; i <= 10000; ++i) {
    std::ostringstream ostr;
    ostr << std::setfill('0') << std::setw(5) << i;
    std::string key = ostr.str();
    std::string value = "val";
    InternalKey ik(key, 0, kTypeValue);

    builder->Add(ik.Encode(), value);
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(file_writer->Flush(IOOptions()));

  std::unique_ptr<FSRandomAccessFile> source(
      new test::StringSource(sink->contents(), 73342, true));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test"));

  {
    RandomAccessFileReader* file = file_reader.get();
    uint64_t file_size = sink->contents().size();

    Footer footer;
    ASSERT_OK(ReadFooterFromFile(IOOptions(), file, *FileSystem::Default(),
                                 nullptr /* prefetch_buffer */, file_size,
                                 &footer, kBlockBasedTableMagicNumber));

    auto BlockFetchHelper = [&](const BlockHandle& handle, BlockType block_type,
                                BlockContents* contents) {
      ReadOptions read_options_for_helper;
      read_options_for_helper.verify_checksums = false;
      PersistentCacheOptions cache_options;

      auto mgr = GetBuiltinV2CompressionManager();
      BlockFetcher block_fetcher(file, nullptr /* prefetch_buffer */, footer,
                                 read_options_for_helper, handle, contents,
                                 ioptions, false /* decompress */,
                                 false /*maybe_compressed*/, block_type,
                                 mgr->GetDecompressor().get(), cache_options);

      ASSERT_OK(block_fetcher.ReadBlockContents());
    };

    // -- Read metaindex block
    auto metaindex_handle = footer.metaindex_handle();
    BlockContents metaindex_contents;

    get_perf_context()->Reset();
    BlockFetchHelper(metaindex_handle, BlockType::kMetaIndex,
                     &metaindex_contents);
    ASSERT_GT(get_perf_context()->metadata_block_read_byte, 0);
    ASSERT_EQ(get_perf_context()->block_read_byte,
              get_perf_context()->metadata_block_read_byte);
    Block metaindex_block(std::move(metaindex_contents));

    std::unique_ptr<InternalIterator> meta_iter(metaindex_block.NewDataIterator(
        BytewiseComparator(), kDisableGlobalSequenceNumber));

    // -- Read properties block
    BlockHandle properties_handle;
    ASSERT_OK(FindOptionalMetaBlock(meta_iter.get(), kPropertiesBlockName,
                                    &properties_handle));
    ASSERT_FALSE(properties_handle.IsNull());
    BlockContents properties_contents;
    get_perf_context()->Reset();
    BlockFetchHelper(properties_handle, BlockType::kProperties,
                     &properties_contents);
    ASSERT_GT(get_perf_context()->metadata_block_read_byte, 0);
    ASSERT_EQ(get_perf_context()->block_read_byte,
              get_perf_context()->metadata_block_read_byte);
    Block properties_block(std::move(properties_contents));

    ASSERT_EQ(properties_block.NumRestarts(), 1u);
  }
}

TEST_P(BlockBasedTableTest, CompressionRatioThreshold) {
  for (CompressionType type : GetSupportedCompressions()) {
    if (type == kNoCompression) {
      continue;
    }
    if (type == kBZip2Compression) {
      // Weird behavior in this test
      continue;
    }
    SCOPED_TRACE("Compression type: " + std::to_string(type));

    Options options;
    options.compression = type;

    BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
    int len = 10000;
    Random rnd(301);
    std::vector<std::string> keys;
    stl_wrappers::KVMap kvmap;

    // Test the max_compressed_bytes_per_kb option
    for (int threshold : {0, 1, 100, 400, 600, 900, 1024}) {
      SCOPED_TRACE("threshold=" + std::to_string(threshold));
      options.compression_opts.max_compressed_bytes_per_kb = threshold;
      ImmutableOptions ioptions(options);
      MutableCFOptions moptions(options);

      for (double compressible_to : {0.25, 0.75}) {
        SCOPED_TRACE("compressible_to=" + std::to_string(compressible_to));
        TableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
        std::string buf;
        c.Add("x", test::CompressibleString(&rnd, compressible_to, len, &buf));

        // write an SST file
        c.Finish(options, ioptions, moptions, table_options,
                 GetPlainInternalComparator(options.comparator), &keys, &kvmap);

        // Verify the compression *decision* via the persisted data-block
        // sizes rather than the total file size, so this check is resilient to
        // unrelated changes in per-file overhead (e.g. added table properties)
        // and to algorithm-specific size variance.
        const auto& props = *c.GetTableReader()->GetTableProperties();
        if (compressible_to < threshold / 1024.0) {
          // Blocks should be stored compressed: on-disk data is smaller than
          // the uncompressed block payload.
          EXPECT_LT(props.data_size, props.uncompressed_data_size);
        } else {
          // Blocks should be stored uncompressed: on-disk data is the
          // uncompressed payload plus per-block trailers, never smaller.
          EXPECT_GE(props.data_size, props.uncompressed_data_size);
        }
        c.ResetTableReader();
      }
    }
  }
}

namespace {

constexpr char kCrc32cHandoffDestinationBufferMismatch[] =
    "Checksum handoff detected data corruption after copying data into "
    "WritableFileWriter destination buffer";

std::string ChecksumTypeToString(ChecksumType checksum_type) {
  switch (checksum_type) {
    case kNoChecksum:
      return "no_checksum";
    case kCRC32c:
      return "crc32c";
    case kxxHash:
      return "xxhash";
    case kxxHash64:
      return "xxhash64";
    case kXXH3:
      return "xxh3";
    default:
      return "unsupported";
  }
}

std::vector<ChecksumType> GetSupportedChecksumHandoffTestChecksumTypes() {
  std::vector<ChecksumType> checksum_types;
  for (ChecksumType checksum_type : GetSupportedChecksums()) {
    if (checksum_type != kNoChecksum) {
      checksum_types.push_back(checksum_type);
    }
  }
  return checksum_types;
}

std::string ChecksumHandoffTestParamName(
    const testing::TestParamInfo<ChecksumType>& info) {
  return ChecksumTypeToString(info.param);
}

CompressionType GetSupportedNonNoCompressionForChecksumHandoffTest() {
  for (CompressionType type : GetSupportedCompressions()) {
    if (type != kNoCompression) {
      return type;
    }
  }
  return kNoCompression;
}

std::string ChecksumHandoffTestValue(size_t i) {
  return std::string(10000, static_cast<char>('a' + i));
}

std::vector<std::pair<std::string, std::string>> ChecksumHandoffTestKvs() {
  std::vector<std::pair<std::string, std::string>> kvs;
  kvs.reserve(5);
  for (size_t i = 0; i < 5; ++i) {
    kvs.emplace_back("key" + std::to_string(i), ChecksumHandoffTestValue(i));
  }
  return kvs;
}

BlockBasedTableOptions GetChecksumHandoffTableOptions(
    ChecksumType checksum_type) {
  BlockBasedTableOptions table_options;
  table_options.checksum = checksum_type;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  return table_options;
}

struct RecordedChecksumHandoffAppend {
  uint64_t offset = 0;
  std::string data;
  bool has_crc32c = false;
  uint32_t crc32c = 0;
};

using BlockBasedTableBuilderAppendCallbackArg =
    std::pair<Slice, const Crc32cChecksum*>;

class NullSink : public FSWritableFile {
 public:
  IOStatus Truncate(uint64_t size, const IOOptions& /*opts*/,
                    IODebugContext* /*dbg*/) override {
    size_ = size;
    return IOStatus::OK();
  }

  IOStatus Close(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Flush(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus Sync(const IOOptions& /*opts*/, IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  using FSWritableFile::Append;
  IOStatus Append(const Slice& data, const IOOptions& /*opts*/,
                  IODebugContext* /*dbg*/) override {
    size_ += data.size();
    return IOStatus::OK();
  }

  uint64_t GetFileSize(const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
    return size_;
  }

 private:
  uint64_t size_ = 0;
};

struct ChecksumHandoffTable {
  std::string contents;
  std::vector<std::string> internal_keys;
  std::vector<BlockHandle> data_blocks;
  std::vector<RecordedChecksumHandoffAppend> recorded_appends;
};

enum class ChecksumHandoffBuildCorruption {
  kNone,
  kCompressionType,
  kBlockChecksum,
};

Status BuildChecksumHandoffTable(ChecksumType checksum_type,
                                 CompressionType compression_type,
                                 ChecksumHandoffTable* table,
                                 ChecksumHandoffBuildCorruption corruption =
                                     ChecksumHandoffBuildCorruption::kNone) {
  assert(table != nullptr);
  *table = ChecksumHandoffTable();

  BlockBasedTableOptions table_options =
      GetChecksumHandoffTableOptions(checksum_type);

  Options options;
  options.env = Env::Default();
  options.compression = compression_type;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  uint64_t next_append_offset = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::Append:Before", [&](void* arg) {
        auto* append_info =
            static_cast<BlockBasedTableBuilderAppendCallbackArg*>(arg);
        RecordedChecksumHandoffAppend recorded_append;
        recorded_append.offset = next_append_offset;
        recorded_append.data.assign(append_info->first.data(),
                                    append_info->first.size());
        if (append_info->second != nullptr) {
          recorded_append.has_crc32c = true;
          recorded_append.crc32c = append_info->second->value;
        }
        table->contents.append(recorded_append.data);
        next_append_offset += recorded_append.data.size();
        table->recorded_appends.push_back(std::move(recorded_append));
      });
  bool corrupted_trailer = false;
  if (checksum_type == kCRC32c &&
      corruption != ChecksumHandoffBuildCorruption::kNone) {
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::WriteMaybeCompressedBlock:"
        "TamperWithChecksum",
        [&](void* arg) {
          if (corrupted_trailer) {
            return;
          }
          auto* trailer = static_cast<char*>(arg);
          if (corruption == ChecksumHandoffBuildCorruption::kCompressionType) {
            trailer[0] = static_cast<char>(
                static_cast<unsigned char>(trailer[0]) ^ 0x80U);
          } else {
            trailer[1] = static_cast<char>(
                static_cast<unsigned char>(trailer[1]) ^ 0x80U);
          }
          corrupted_trailer = true;
        });
  }
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  std::unique_ptr<FSWritableFile> holder(new NullSink());
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(holder), "test_file_name", FileOptions(),
                             SystemClock::Default().get(), nullptr, nullptr,
                             Histograms::HISTOGRAM_ENUM_MAX, {}, nullptr,
                             /*perform_data_verification=*/true,
                             /*buffered_data_with_checksum=*/true));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;

  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, compression_type,
                          options.compression_opts, kUnknownColumnFamily,
                          "test_cf", -1 /* level */, kUnknownNewestKeyTime),
      file_writer.get()));

  for (const auto& kv : ChecksumHandoffTestKvs()) {
    InternalKey key(kv.first, 1 /* sequence number */, kTypeValue);
    const std::string encoded_key = key.Encode().ToString();
    table->internal_keys.push_back(encoded_key);
    builder->Add(encoded_key, kv.second);
    if (!builder->status().ok()) {
      Status s = builder->status();
      builder->Abandon();
      return s;
    }
  }
  Status s = builder->Finish();
  if (s.ok()) {
    s = file_writer->Flush(IOOptions());
  }
  if (s.ok() && table->contents.size() != file_writer->GetFileSize()) {
    s = Status::Corruption("Incomplete checksum handoff append recording");
  }
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<TableReader> reader;
  BlockBasedTableOptions reader_table_options =
      GetChecksumHandoffTableOptions(checksum_type);
  Options reader_options;
  reader_options.env = Env::Default();
  reader_options.compression = compression_type;
  reader_options.table_factory.reset(
      NewBlockBasedTableFactory(reader_table_options));
  ImmutableOptions reader_ioptions(reader_options);
  MutableCFOptions reader_moptions(reader_options);
  InternalKeyComparator reader_ikc(reader_options.comparator);
  std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
      table->contents, 1 /* uniq_id */, reader_ioptions.allow_mmap_reads));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test_file_name"));
  s = reader_options.table_factory->NewTableReader(
      TableReaderOptions(
          reader_ioptions, reader_moptions.prefix_extractor,
          reader_moptions.compression_manager.get(), EnvOptions(), reader_ikc,
          0 /* block_protection_bytes_per_key */,
          /*skip_filters*/ false, /*immortal*/ false,
          false /* force_direct_prefetch */, -1 /* level */,
          nullptr /* block_cache_tracer */, reader_moptions.write_buffer_size,
          "" /* cur_db_session_id */, 1 /* cur_file_num */, kNullUniqueId64x2),
      std::move(file_reader), table->contents.size(), &reader);
  if (!s.ok()) {
    return s;
  }

  BlockBasedTable* bbt = static_cast<BlockBasedTable*>(reader.get());
  for (const std::string& key : table->internal_keys) {
    BlockHandle block_handle;
    bbt->TEST_GetDataBlockHandle(read_options, key, block_handle);
    if (table->data_blocks.empty() ||
        table->data_blocks.back() != block_handle) {
      table->data_blocks.push_back(block_handle);
    }
  }
  return Status::OK();
}

Status VerifyChecksumHandoffTableContents(ChecksumType checksum_type,
                                          const std::string& contents,
                                          CompressionType compression_type) {
  BlockBasedTableOptions table_options =
      GetChecksumHandoffTableOptions(checksum_type);
  Options options;
  options.env = Env::Default();
  options.compression = compression_type;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
      contents, 1 /* uniq_id */, ioptions.allow_mmap_reads));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(source), "test_file_name"));
  std::unique_ptr<TableReader> table_reader;
  Status s = options.table_factory->NewTableReader(
      TableReaderOptions(ioptions, moptions.prefix_extractor,
                         moptions.compression_manager.get(), EnvOptions(), ikc,
                         0 /* block_protection_bytes_per_key */,
                         /*skip_filters*/ false, /*immortal*/ false,
                         false /* force_direct_prefetch */, -1 /* level */,
                         nullptr /* block_cache_tracer */,
                         moptions.write_buffer_size, "" /* cur_db_session_id */,
                         1 /* cur_file_num */, kNullUniqueId64x2),
      std::move(file_reader), contents.size(), &table_reader);
  if (!s.ok()) {
    return s;
  }

  ReadOptions read_options;
  read_options.verify_checksums = true;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      read_options, nullptr /* prefix_extractor */, nullptr /* arena */,
      /*skip_filters*/ false, TableReaderCaller::kUncategorized));

  iter->SeekToFirst();
  for (const auto& kv : ChecksumHandoffTestKvs()) {
    if (!iter->status().ok()) {
      return iter->status();
    }
    if (!iter->Valid()) {
      return Status::Corruption("Expected another key in checksum handoff SST");
    }
    ParsedInternalKey parsed_key;
    s = ParseInternalKey(iter->key(), &parsed_key, true /* log_err_key */);
    if (!s.ok()) {
      return s;
    }
    if (parsed_key.user_key.ToString() != kv.first) {
      return Status::Corruption("Unexpected key in checksum handoff SST");
    }
    if (iter->value().ToString() != kv.second) {
      return Status::Corruption("Unexpected value in checksum handoff SST");
    }
    iter->Next();
  }
  if (!iter->status().ok()) {
    return iter->status();
  }
  if (iter->Valid()) {
    return Status::Corruption("Unexpected extra key in checksum handoff SST");
  }
  return Status::OK();
}

enum class ChecksumHandoffReplayCorruption {
  kNone,
  kBlockPayload,
  kCompressionType,
  kBlockChecksum,
};

struct ChecksumHandoffReplayOptions {
  ChecksumHandoffReplayCorruption corruption =
      ChecksumHandoffReplayCorruption::kNone;
  size_t block_index = 0;
  size_t byte_offset = 0;
};

void FlipByte(std::string* data, size_t offset) {
  assert(data != nullptr);
  assert(offset < data->size());
  (*data)[offset] =
      static_cast<char>(static_cast<unsigned char>((*data)[offset]) ^ 0x80U);
}

void MaybeCorruptRecordedAppend(
    const RecordedChecksumHandoffAppend& recorded_append,
    const BlockHandle& target_handle,
    const ChecksumHandoffReplayOptions& options, std::string* data) {
  assert(data != nullptr);
  if (options.corruption == ChecksumHandoffReplayCorruption::kNone) {
    return;
  }

  const uint64_t block_offset = target_handle.offset();
  const size_t block_size = static_cast<size_t>(target_handle.size());
  const uint64_t trailer_offset = block_offset + block_size;

  if (options.corruption == ChecksumHandoffReplayCorruption::kBlockPayload &&
      recorded_append.offset == block_offset &&
      recorded_append.data.size() == block_size) {
    assert(block_size > 0);
    FlipByte(data, options.byte_offset % data->size());
  } else if (options.corruption ==
                 ChecksumHandoffReplayCorruption::kCompressionType &&
             recorded_append.offset == trailer_offset &&
             recorded_append.data.size() ==
                 BlockBasedTable::kBlockTrailerSize) {
    FlipByte(data, 0);
  } else if (options.corruption ==
                 ChecksumHandoffReplayCorruption::kBlockChecksum &&
             recorded_append.offset == trailer_offset &&
             recorded_append.data.size() ==
                 BlockBasedTable::kBlockTrailerSize) {
    FlipByte(data, 1 + options.byte_offset %
                           (BlockBasedTable::kBlockTrailerSize - 1));
  }
}

Status ReplayChecksumHandoffTable(const ChecksumHandoffTable& table,
                                  const ChecksumHandoffReplayOptions& options,
                                  std::string* contents) {
  assert(contents != nullptr);
  assert(options.corruption == ChecksumHandoffReplayCorruption::kNone ||
         options.block_index < table.data_blocks.size());
  contents->clear();

  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(holder), "test_file_name", FileOptions(),
                             SystemClock::Default().get(), nullptr, nullptr,
                             Histograms::HISTOGRAM_ENUM_MAX, {}, nullptr,
                             /*perform_data_verification=*/true,
                             /*buffered_data_with_checksum=*/true));

  for (const RecordedChecksumHandoffAppend& recorded_append :
       table.recorded_appends) {
    std::string data = recorded_append.data;
    if (options.corruption != ChecksumHandoffReplayCorruption::kNone) {
      MaybeCorruptRecordedAppend(recorded_append,
                                 table.data_blocks[options.block_index],
                                 options, &data);
    }
    Status s;
    if (recorded_append.has_crc32c) {
      s = file_writer->Append(Slice(data), IOOptions(),
                              Crc32cChecksum(recorded_append.crc32c));
    } else {
      s = file_writer->Append(Slice(data), IOOptions());
    }
    if (!s.ok()) {
      return s;
    }
  }

  Status s = file_writer->Flush(IOOptions());
  if (s.ok()) {
    *contents = sink->contents();
  }
  return s;
}

}  // namespace

class BlockBasedTableChecksumHandoffTest
    : public BlockBasedTableTestBase,
      public testing::WithParamInterface<ChecksumType> {};

INSTANTIATE_TEST_CASE_P(
    SupportedChecksums, BlockBasedTableChecksumHandoffTest,
    testing::ValuesIn(GetSupportedChecksumHandoffTestChecksumTypes()),
    ChecksumHandoffTestParamName);

TEST_P(BlockBasedTableChecksumHandoffTest,
       Crc32cHandoffDetectsCompressedBlockCorruption) {
  const ChecksumType checksum_type = GetParam();
  SCOPED_TRACE(ChecksumTypeToString(checksum_type));
  const CompressionType compression_type =
      GetSupportedNonNoCompressionForChecksumHandoffTest();
  if (compression_type == kNoCompression) {
    ROCKSDB_GTEST_SKIP("No supported compression");
    return;
  }

  ChecksumHandoffTable table;
  ASSERT_OK(BuildChecksumHandoffTable(checksum_type, compression_type, &table));
  ASSERT_FALSE(table.data_blocks.empty());

  std::string contents;
  const Status s = ReplayChecksumHandoffTable(
      table,
      ChecksumHandoffReplayOptions{
          ChecksumHandoffReplayCorruption::kBlockPayload,
          /*block_index=*/0,
          /*byte_offset=*/0},
      &contents);
  if (checksum_type == kCRC32c) {
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_NE(s.ToString().find(kCrc32cHandoffDestinationBufferMismatch),
              std::string::npos)
        << s.ToString();
  } else {
    ASSERT_OK(s);
    ASSERT_TRUE(VerifyChecksumHandoffTableContents(checksum_type, contents,
                                                   compression_type)
                    .IsCorruption());
  }
}

TEST_P(BlockBasedTableChecksumHandoffTest, Crc32cHandoffReadsBackBlocks) {
  const ChecksumType checksum_type = GetParam();
  SCOPED_TRACE(ChecksumTypeToString(checksum_type));
  ChecksumHandoffTable table;
  ASSERT_OK(BuildChecksumHandoffTable(checksum_type, kNoCompression, &table));

  std::string contents;
  ASSERT_OK(ReplayChecksumHandoffTable(table, ChecksumHandoffReplayOptions(),
                                       &contents));
  ASSERT_OK(VerifyChecksumHandoffTableContents(checksum_type, contents,
                                               kNoCompression));
}

TEST_P(BlockBasedTableChecksumHandoffTest,
       Crc32cHandoffDetectsCompressionTypeCorruption) {
  const ChecksumType checksum_type = GetParam();
  SCOPED_TRACE(ChecksumTypeToString(checksum_type));
  ChecksumHandoffTable table;
  ASSERT_OK(BuildChecksumHandoffTable(checksum_type, kNoCompression, &table));
  ASSERT_FALSE(table.data_blocks.empty());

  std::string contents;
  const Status s = ReplayChecksumHandoffTable(
      table,
      ChecksumHandoffReplayOptions{
          ChecksumHandoffReplayCorruption::kCompressionType,
          /*block_index=*/0,
          /*byte_offset=*/0},
      &contents);
  if (checksum_type == kCRC32c) {
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_NE(s.ToString().find(kCrc32cHandoffDestinationBufferMismatch),
              std::string::npos)
        << s.ToString();
  } else {
    ASSERT_OK(s);
    ASSERT_TRUE(VerifyChecksumHandoffTableContents(checksum_type, contents,
                                                   kNoCompression)
                    .IsCorruption());
  }
}

TEST_P(BlockBasedTableChecksumHandoffTest,
       Crc32cHandoffDetectsBlockChecksumTrailerCorruption) {
  const ChecksumType checksum_type = GetParam();
  SCOPED_TRACE(ChecksumTypeToString(checksum_type));
  ChecksumHandoffTable table;
  ASSERT_OK(BuildChecksumHandoffTable(checksum_type, kNoCompression, &table));
  ASSERT_FALSE(table.data_blocks.empty());

  std::string contents;
  const Status s = ReplayChecksumHandoffTable(
      table,
      ChecksumHandoffReplayOptions{
          ChecksumHandoffReplayCorruption::kBlockChecksum,
          /*block_index=*/0,
          /*byte_offset=*/1},
      &contents);

  if (checksum_type == kCRC32c) {
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_NE(s.ToString().find(kCrc32cHandoffDestinationBufferMismatch),
              std::string::npos)
        << s.ToString();
  } else {
    ASSERT_OK(s);
    ASSERT_TRUE(VerifyChecksumHandoffTableContents(checksum_type, contents,
                                                   kNoCompression)
                    .IsCorruption());
  }
}

TEST_F(BlockBasedTableTestBase,
       Crc32cHandoffDetectsTrailerBufferCorruptionDuringBuild) {
  for (ChecksumHandoffBuildCorruption corruption :
       {ChecksumHandoffBuildCorruption::kCompressionType,
        ChecksumHandoffBuildCorruption::kBlockChecksum}) {
    ChecksumHandoffTable table;
    const Status s =
        BuildChecksumHandoffTable(kCRC32c, kNoCompression, &table, corruption);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_NE(s.ToString().find(kCrc32cHandoffDestinationBufferMismatch),
              std::string::npos)
        << s.ToString();
  }
}

TEST_P(BlockBasedTableChecksumHandoffTest, Crc32cHandoffRandomizedReplay) {
  const ChecksumType checksum_type = GetParam();
  const uint32_t seed = static_cast<uint32_t>(test::RandomSeed() + 6707);
  Random rnd(seed);
  SCOPED_TRACE("seed=" + std::to_string(seed) +
               " checksum_type=" + ChecksumTypeToString(checksum_type));

  const CompressionType non_no_compression_type =
      GetSupportedNonNoCompressionForChecksumHandoffTest();
  const std::array<CompressionType, 2> compression_types = {
      {kNoCompression, non_no_compression_type}};
  for (int i = 0; i < 20; ++i) {
    const CompressionType compression_type = compression_types[rnd.Uniform(
        static_cast<int>(non_no_compression_type == kNoCompression ? 1 : 2))];
    ChecksumHandoffTable table;
    ASSERT_OK(
        BuildChecksumHandoffTable(checksum_type, compression_type, &table));
    ASSERT_FALSE(table.data_blocks.empty());

    const auto corruption =
        static_cast<ChecksumHandoffReplayCorruption>(1 + rnd.Uniform(3));
    const size_t block_index =
        rnd.Uniform(static_cast<int>(table.data_blocks.size()));
    const size_t block_size =
        static_cast<size_t>(table.data_blocks[block_index].size());
    const size_t byte_offset =
        block_size == 0 ? 0 : rnd.Uniform(static_cast<int>(block_size));

    std::string contents;
    const Status s = ReplayChecksumHandoffTable(
        table,
        ChecksumHandoffReplayOptions{corruption, block_index, byte_offset},
        &contents);

    if (checksum_type == kCRC32c) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
      ASSERT_NE(s.ToString().find(kCrc32cHandoffDestinationBufferMismatch),
                std::string::npos)
          << s.ToString();
    } else {
      ASSERT_OK(s);
      ASSERT_TRUE(VerifyChecksumHandoffTableContents(checksum_type, contents,
                                                     compression_type)
                      .IsCorruption());
    }
  }
}

TEST_P(BlockBasedTableTest, PropertiesMetaBlockLast) {
  // The properties meta-block should come at the end since we always need to
  // read it when opening a file, unlike index/filter/other meta-blocks, which
  // are sometimes read depending on the user's configuration. This ordering
  // allows us to do a small readahead on the end of the file to read properties
  // and meta-index blocks with one I/O.
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("a1", "val1");
  c.Add("b2", "val2");
  c.Add("c3", "val3");
  c.Add("d4", "val4");
  c.Add("e5", "val5");
  c.Add("f6", "val6");
  c.Add("g7", "val7");
  c.Add("h8", "val8");
  c.Add("j9", "val9");

  // write an SST file
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.filter_policy.reset(NewBloomFilterPolicy(
      8 /* bits_per_key */, false /* use_block_based_filter */));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  // get file reader
  test::StringSink* table_sink = c.TEST_GetSink();
  std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
      table_sink->contents(), 0 /* unique_id */, false /* allow_mmap_reads */));

  std::unique_ptr<RandomAccessFileReader> table_reader(
      new RandomAccessFileReader(std::move(source), "test"));
  size_t table_size = table_sink->contents().size();

  // read footer
  Footer footer;
  IOOptions opts;
  ASSERT_OK(ReadFooterFromFile(opts, table_reader.get(), *FileSystem::Default(),
                               nullptr /* prefetch_buffer */, table_size,
                               &footer, kBlockBasedTableMagicNumber));

  // read metaindex
  auto metaindex_handle = footer.metaindex_handle();
  BlockContents metaindex_contents;
  PersistentCacheOptions pcache_opts;
  auto mgr = GetBuiltinV2CompressionManager();
  BlockFetcher block_fetcher(
      table_reader.get(), nullptr /* prefetch_buffer */, footer, ReadOptions(),
      metaindex_handle, &metaindex_contents, ioptions, false /* decompress */,
      false /*maybe_compressed*/, BlockType::kMetaIndex,
      mgr->GetDecompressor().get(), pcache_opts, nullptr /*memory_allocator*/);
  ASSERT_OK(block_fetcher.ReadBlockContents());
  Block metaindex_block(std::move(metaindex_contents));

  // verify properties block comes last
  std::unique_ptr<InternalIterator> metaindex_iter{
      metaindex_block.NewMetaIterator()};
  uint64_t max_offset = 0;
  std::string key_at_max_offset;
  for (metaindex_iter->SeekToFirst(); metaindex_iter->Valid();
       metaindex_iter->Next()) {
    BlockHandle handle;
    Slice value = metaindex_iter->value();
    ASSERT_OK(handle.DecodeFrom(&value));
    if (handle.offset() > max_offset) {
      max_offset = handle.offset();
      key_at_max_offset = metaindex_iter->key().ToString();
    }
  }
  ASSERT_EQ(kPropertiesBlockName, key_at_max_offset);
  if (FormatVersionUsesIndexHandleInFooter(footer.format_version())) {
    // If index handle is stored in footer rather than metaindex block,
    // need separate logic to verify it comes before properties block.
    ASSERT_GT(max_offset, footer.index_handle().offset());
  } else {
    ASSERT_TRUE(footer.index_handle().IsNull());
  }
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, SeekMetaBlocks) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  c.Add("foo_a1", "val1");
  c.Add("foo_b2", "val2");
  c.Add("foo_c3", "val3");
  c.Add("foo_d4", "val4");
  c.Add("foo_e5", "val5");
  c.Add("foo_f6", "val6");
  c.Add("foo_g7", "val7");
  c.Add("foo_h8", "val8");
  c.Add("foo_j9", "val9");

  // write an SST file
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  table_options.filter_policy.reset(NewBloomFilterPolicy(
      8 /* bits_per_key */, false /* use_block_based_filter */));
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  // get file reader
  test::StringSink* table_sink = c.TEST_GetSink();
  std::unique_ptr<FSRandomAccessFile> source(new test::StringSource(
      table_sink->contents(), 0 /* unique_id */, false /* allow_mmap_reads */));

  std::unique_ptr<RandomAccessFileReader> table_reader(
      new RandomAccessFileReader(std::move(source), "test"));
  size_t table_size = table_sink->contents().size();

  // read footer
  Footer footer;
  IOOptions opts;
  ASSERT_OK(ReadFooterFromFile(opts, table_reader.get(), *FileSystem::Default(),
                               nullptr /* prefetch_buffer */, table_size,
                               &footer, kBlockBasedTableMagicNumber));

  // read metaindex
  auto metaindex_handle = footer.metaindex_handle();
  BlockContents metaindex_contents;
  PersistentCacheOptions pcache_opts;
  auto mgr = GetBuiltinV2CompressionManager();
  BlockFetcher block_fetcher(
      table_reader.get(), nullptr /* prefetch_buffer */, footer, ReadOptions(),
      metaindex_handle, &metaindex_contents, ioptions, false /* decompress */,
      false /*maybe_compressed*/, BlockType::kMetaIndex,
      mgr->GetDecompressor().get(), pcache_opts, nullptr /*memory_allocator*/);
  ASSERT_OK(block_fetcher.ReadBlockContents());
  Block metaindex_block(std::move(metaindex_contents));

  // verify properties block comes last
  std::unique_ptr<MetaBlockIter> metaindex_iter(
      metaindex_block.NewMetaIterator());
  bool has_hash_prefixes = false;
  bool has_hash_metadata = false;
  for (metaindex_iter->SeekToFirst(); metaindex_iter->Valid();
       metaindex_iter->Next()) {
    if (metaindex_iter->key().ToString() == kHashIndexPrefixesBlock) {
      has_hash_prefixes = true;
    } else if (metaindex_iter->key().ToString() ==
               kHashIndexPrefixesMetadataBlock) {
      has_hash_metadata = true;
    }
  }
  if (has_hash_metadata) {
    metaindex_iter->Seek(kHashIndexPrefixesMetadataBlock);
    ASSERT_TRUE(metaindex_iter->Valid());
    ASSERT_EQ(kHashIndexPrefixesMetadataBlock,
              metaindex_iter->key().ToString());
  }
  if (has_hash_prefixes) {
    metaindex_iter->Seek(kHashIndexPrefixesBlock);
    ASSERT_TRUE(metaindex_iter->Valid());
    ASSERT_EQ(kHashIndexPrefixesBlock, metaindex_iter->key().ToString());
  }
  c.ResetTableReader();
}

TEST_P(BlockBasedTableTest, BadOptions) {
  ROCKSDB_NAMESPACE::Options options;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  BlockBasedTableOptions bbto = GetBlockBasedTableOptions();
  bbto.block_size = 4000;
  bbto.block_align = true;

  const std::string kDBPath =
      test::PerThreadDBPath("block_based_table_bad_options_test");
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(DestroyDB(kDBPath, options));

  {
    std::unique_ptr<ROCKSDB_NAMESPACE::DB> db;
    ASSERT_NOK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));

    bbto.block_size = 4096;
    options.compression = kSnappyCompression;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    ASSERT_NOK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));

    options.compression = kNoCompression;
    options.bottommost_compression = kSnappyCompression;
    ASSERT_NOK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));

    options.bottommost_compression = kNoCompression;
    options.compression_per_level.emplace_back(kSnappyCompression);
    ASSERT_NOK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));

    options.compression_per_level.clear();
    ASSERT_OK(ROCKSDB_NAMESPACE::DB::Open(options, kDBPath, &db));
  }
}

TEST_F(BBTTailPrefetchTest, TestTailPrefetchStats) {
  TailPrefetchStats tpstats;
  ASSERT_EQ(0, tpstats.GetSuggestedPrefetchSize());
  tpstats.RecordEffectiveSize(size_t{1000});
  tpstats.RecordEffectiveSize(size_t{1005});
  tpstats.RecordEffectiveSize(size_t{1002});
  ASSERT_EQ(1005, tpstats.GetSuggestedPrefetchSize());

  // One single super large value shouldn't influence much
  tpstats.RecordEffectiveSize(size_t{1002000});
  tpstats.RecordEffectiveSize(size_t{999});
  ASSERT_LE(1005, tpstats.GetSuggestedPrefetchSize());
  ASSERT_GT(1200, tpstats.GetSuggestedPrefetchSize());

  // Only history of 32 is kept
  for (int i = 0; i < 32; i++) {
    tpstats.RecordEffectiveSize(size_t{100});
  }
  ASSERT_EQ(100, tpstats.GetSuggestedPrefetchSize());

  // 16 large values and 16 small values. The result should be closer
  // to the small value as the algorithm.
  for (int i = 0; i < 16; i++) {
    tpstats.RecordEffectiveSize(size_t{1000});
  }
  tpstats.RecordEffectiveSize(size_t{10});
  tpstats.RecordEffectiveSize(size_t{20});
  for (int i = 0; i < 6; i++) {
    tpstats.RecordEffectiveSize(size_t{100});
  }
  ASSERT_LE(80, tpstats.GetSuggestedPrefetchSize());
  ASSERT_GT(200, tpstats.GetSuggestedPrefetchSize());
}

TEST_F(BBTTailPrefetchTest, FilePrefetchBufferMinOffset) {
  TailPrefetchStats tpstats;
  FilePrefetchBuffer buffer(ReadaheadParams(), false /* enable */,
                            true /* track_min_offset */);
  IOOptions opts;
  buffer.TryReadFromCache(opts, nullptr /* reader */, 500 /* offset */,
                          10 /* n */, nullptr /* result */,
                          nullptr /* status */);
  buffer.TryReadFromCache(opts, nullptr /* reader */, 480 /* offset */,
                          10 /* n */, nullptr /* result */,
                          nullptr /* status */);
  buffer.TryReadFromCache(opts, nullptr /* reader */, 490 /* offset */,
                          10 /* n */, nullptr /* result */,
                          nullptr /* status */);
  ASSERT_EQ(480, buffer.min_offset_read());
}

TEST_P(BlockBasedTableTest, DataBlockHashIndex) {
  const int kNumKeys = 500;
  const int kKeySize = 8;
  const int kValSize = 40;

  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();
  table_options.data_block_index_type =
      BlockBasedTableOptions::kDataBlockBinaryAndHash;

  Options options;
  options.comparator = BytewiseComparator();

  options.table_factory.reset(new BlockBasedTableFactory(table_options));

  TableConstructor c(options.comparator);

  static Random rnd(1048);
  for (int i = 0; i < kNumKeys; i++) {
    // padding one "0" to mark existent keys.
    std::string random_key(rnd.RandomString(kKeySize - 1) + "1");
    InternalKey k(random_key, 0, kTypeValue);
    c.Add(k.Encode().ToString(), rnd.RandomString(kValSize));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, moptions, table_options, internal_comparator,
           &keys, &kvmap);

  auto reader = c.GetTableReader();

  std::unique_ptr<InternalIterator> seek_iter;
  ReadOptions read_options;
  seek_iter.reset(reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));
  for (int i = 0; i < 2; ++i) {
    ReadOptions ro;
    // for every kv, we seek using two method: Get() and Seek()
    // Get() will use the SuffixIndexHash in Block. For non-existent key it
    //      will invalidate the iterator
    // Seek() will use the default BinarySeek() in Block. So for non-existent
    //      key it will land at the closest key that is large than target.

    // Search for existent keys
    for (auto& kv : kvmap) {
      if (i == 0) {
        // Search using Seek()
        seek_iter->Seek(kv.first);
        ASSERT_OK(seek_iter->status());
        ASSERT_TRUE(seek_iter->Valid());
        ASSERT_EQ(seek_iter->key(), kv.first);
        ASSERT_EQ(seek_iter->value(), kv.second);
      } else {
        // Search using Get()
        PinnableSlice value;
        std::string user_key = ExtractUserKey(kv.first).ToString();
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        ASSERT_OK(reader->Get(ro, kv.first, &get_context,
                              moptions.prefix_extractor.get()));
        ASSERT_EQ(get_context.State(), GetContext::kFound);
        ASSERT_EQ(value, Slice(kv.second));
        value.Reset();
      }
    }

    // Search for non-existent keys
    for (auto& kv : kvmap) {
      std::string user_key = ExtractUserKey(kv.first).ToString();
      user_key.back() = '0';  // make it non-existent key
      InternalKey internal_key(user_key, 0, kTypeValue);
      std::string encoded_key = internal_key.Encode().ToString();
      if (i == 0) {  // Search using Seek()
        seek_iter->Seek(encoded_key);
        ASSERT_OK(seek_iter->status());
        if (seek_iter->Valid()) {
          ASSERT_TRUE(BytewiseComparator()->Compare(
                          user_key, ExtractUserKey(seek_iter->key())) < 0);
        }
      } else {  // Search using Get()
        PinnableSlice value;
        GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                               GetContext::kNotFound, user_key, &value, nullptr,
                               nullptr, nullptr, true, nullptr, nullptr);
        ASSERT_OK(reader->Get(ro, encoded_key, &get_context,
                              moptions.prefix_extractor.get()));
        ASSERT_EQ(get_context.State(), GetContext::kNotFound);
        value.Reset();
      }
    }
  }
}

// BlockBasedTableIterator should invalidate itself and return
// OutOfBound()=true immediately after Seek(), to allow LevelIterator
// filter out corresponding level.
TEST_P(BlockBasedTableTest, OutOfBoundOnSeek) {
  TableConstructor c(BytewiseComparator(), true /*convert_to_internal_key*/);
  c.Add("foo", "v1");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  BlockBasedTableOptions table_opt(GetBlockBasedTableOptions());
  options.table_factory.reset(NewBlockBasedTableFactory(table_opt));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_opt,
           GetPlainInternalComparator(BytewiseComparator()), &keys, &kvmap);
  auto* reader = c.GetTableReader();
  ReadOptions read_opt;
  std::string upper_bound = "bar";
  Slice upper_bound_slice(upper_bound);
  read_opt.iterate_upper_bound = &upper_bound_slice;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->Seek("foo");
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
}

// BlockBasedTableIterator should invalidate itself and return
// OutOfBound()=true after Next(), if it finds current index key is no smaller
// than upper bound, unless it is pointing to the last data block.
TEST_P(BlockBasedTableTest, OutOfBoundOnNext) {
  TableConstructor c(BytewiseComparator(), true /*convert_to_internal_key*/);
  c.Add("bar", "v");
  c.Add("foo", "v");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  BlockBasedTableOptions table_opt(GetBlockBasedTableOptions());
  table_opt.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();
  options.table_factory.reset(NewBlockBasedTableFactory(table_opt));
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_opt,
           GetPlainInternalComparator(BytewiseComparator()), &keys, &kvmap);
  auto* reader = c.GetTableReader();
  ReadOptions read_opt;
  std::string ub1 = "bar_after";
  Slice ub_slice1(ub1);
  read_opt.iterate_upper_bound = &ub_slice1;
  std::unique_ptr<InternalIterator> iter;
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
  std::string ub2 = "foo_after";
  Slice ub_slice2(ub2);
  read_opt.iterate_upper_bound = &ub_slice2;
  iter.reset(new KeyConvertingIterator(reader->NewIterator(
      read_opt, /*prefix_extractor=*/nullptr, /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized)));
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_FALSE(iter->UpperBoundCheckResult() == IterBoundCheck::kOutOfBound);
}

// Test that a single large entry with value larger than block size works
TEST_P(BlockBasedTableTest, SingleLargeEntry) {
  TableConstructor c(BytewiseComparator(), true /* convert_to_internal_key_ */);
  Options options;
  BlockBasedTableOptions table_options = GetBlockBasedTableOptions();

  // Set a small block size
  constexpr size_t kBlockSize = 1024;
  table_options.block_size = kBlockSize;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.compression = kNoCompression;

  // Create a value that is larger than the block size
  const size_t kLargeValueSize = kBlockSize * 4;
  std::string large_value(kLargeValueSize, 'x');
  c.Add("key1", large_value);

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableOptions ioptions(options);
  const MutableCFOptions moptions(options);
  c.Finish(options, ioptions, moptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  auto* reader = c.GetTableReader();
  ReadOptions read_options;
  std::unique_ptr<InternalIterator> iter(reader->NewIterator(
      read_options, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized));

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(large_value, iter->value().ToString());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

class ChargeCompressionDictionaryBuildingBufferTest
    : public BlockBasedTableTestBase {};
TEST_F(ChargeCompressionDictionaryBuildingBufferTest, Basic) {
  if (GetSupportedDictCompressions().empty()) {
    ROCKSDB_GTEST_SKIP("No supported dict compression");
    return;
  }
  const auto kCompression = GetSupportedDictCompressions()[0];

  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  constexpr std::size_t kCacheCapacity = 8 * 1024 * 1024;
  constexpr std::size_t kMaxDictBytes = 1024;
  constexpr std::size_t kMaxDictBufferBytes = 1024;

  for (CacheEntryRoleOptions::Decision
           charge_compression_dictionary_building_buffer :
       {CacheEntryRoleOptions::Decision::kEnabled,
        CacheEntryRoleOptions::Decision::kDisabled}) {
    BlockBasedTableOptions table_options;
    LRUCacheOptions lo;
    lo.capacity = kCacheCapacity;
    lo.num_shard_bits = 0;  // 2^0 shard
    lo.strict_capacity_limit = true;
    std::shared_ptr<Cache> cache(NewLRUCache(lo));
    table_options.block_cache = cache;
    table_options.flush_block_policy_factory =
        std::make_shared<FlushBlockEveryKeyPolicyFactory>();
    table_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kCompressionDictionaryBuildingBuffer,
         {/*.charged = */ charge_compression_dictionary_building_buffer}});
    Options options;
    options.compression = kCompression;
    options.compression_opts.max_dict_bytes = kMaxDictBytes;
    options.compression_opts.max_dict_buffer_bytes = kMaxDictBufferBytes;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    test::StringSink* sink = new test::StringSink();
    std::unique_ptr<FSWritableFile> holder(sink);
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(holder), "test_file_name", FileOptions()));

    ImmutableOptions ioptions(options);
    MutableCFOptions moptions(options);
    InternalKeyComparator ikc(options.comparator);
    InternalTblPropCollFactories internal_tbl_prop_coll_factories;

    const ReadOptions read_options;
    const WriteOptions write_options;
    std::unique_ptr<TableBuilder> builder(
        options.table_factory->NewTableBuilder(
            TableBuilderOptions(ioptions, moptions, read_options, write_options,
                                ikc, &internal_tbl_prop_coll_factories,
                                kCompression, options.compression_opts,
                                kUnknownColumnFamily, "test_cf", -1 /* level */,
                                kUnknownNewestKeyTime),
            file_writer.get()));

    std::string key1 = "key1";
    std::string value1 = "val1";
    InternalKey ik1(key1, 0 /* sequnce number */, kTypeValue);
    // Adding the first key won't trigger a flush by FlushBlockEveryKeyPolicy
    // therefore won't trigger any data block's buffering
    builder->Add(ik1.Encode(), value1);
    ASSERT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

    std::string key2 = "key2";
    std::string value2 = "val2";
    InternalKey ik2(key2, 1 /* sequnce number */, kTypeValue);
    // Adding the second key will trigger a flush of the last data block (the
    // one containing key1 and value1) by FlushBlockEveryKeyPolicy and hence
    // trigger buffering of that data block.
    builder->Add(ik2.Encode(), value2);
    // Cache charging will increase for last buffered data block (the one
    // containing key1 and value1) since the buffer limit is not exceeded after
    // that buffering and the cache will not be full after this reservation
    if (charge_compression_dictionary_building_buffer ==
        CacheEntryRoleOptions::Decision::kEnabled) {
      EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
      EXPECT_LT(cache->GetPinnedUsage(),
                1 * kSizeDummyEntry + kMetaDataChargeOverhead);
    } else {
      EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
    }

    ASSERT_OK(builder->Finish());
    EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
  }
}

TEST_F(ChargeCompressionDictionaryBuildingBufferTest,
       BasicWithBufferLimitExceed) {
  if (GetSupportedDictCompressions().empty()) {
    ROCKSDB_GTEST_SKIP("No supported dict compression");
    return;
  }
  const auto kCompression = GetSupportedDictCompressions()[0];

  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  constexpr std::size_t kCacheCapacity = 8 * 1024 * 1024;
  constexpr std::size_t kMaxDictBytes = 1024;
  constexpr std::size_t kMaxDictBufferBytes = 2 * kSizeDummyEntry;

  // `CacheEntryRoleOptions::charged` is enabled by default for
  // CacheEntryRole::kCompressionDictionaryBuildingBuffer
  BlockBasedTableOptions table_options;
  LRUCacheOptions lo;
  lo.capacity = kCacheCapacity;
  lo.num_shard_bits = 0;  // 2^0 shard
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache(NewLRUCache(lo));
  table_options.block_cache = cache;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();

  Options options;
  options.compression = kCompression;
  options.compression_opts.max_dict_bytes = kMaxDictBytes;
  options.compression_opts.max_dict_buffer_bytes = kMaxDictBufferBytes;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "test_file_name", FileOptions()));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;

  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kCompression,
                          options.compression_opts, kUnknownColumnFamily,
                          "test_cf", -1 /* level */, kUnknownNewestKeyTime),
      file_writer.get()));

  std::string key1 = "key1";
  std::string value1(kSizeDummyEntry, '0');
  InternalKey ik1(key1, 0 /* sequnce number */, kTypeValue);
  // Adding the first key won't trigger a flush by FlushBlockEveryKeyPolicy
  // therefore won't trigger any data block's buffering
  builder->Add(ik1.Encode(), value1);
  ASSERT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  std::string key2 = "key2";
  std::string value2(kSizeDummyEntry, '0');
  InternalKey ik2(key2, 1 /* sequnce number */, kTypeValue);
  // Adding the second key will trigger a flush of the last data block (the one
  // containing key1 and value1) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik2.Encode(), value2);
  // Cache charging will increase for last buffered data block (the one
  // containing key1 and value1) since the buffer limit is not exceeded after
  // the buffering and the cache will not be full after this reservation
  EXPECT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);
  EXPECT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead);

  std::string key3 = "key3";
  std::string value3 = "val3";
  InternalKey ik3(key3, 2 /* sequnce number */, kTypeValue);
  // Adding the third key will trigger a flush of the last data block (the one
  // containing key2 and value2) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik3.Encode(), value3);
  // Cache charging will decrease since the buffer limit is now exceeded
  // after the last buffering and EnterUnbuffered() is triggered
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  ASSERT_OK(builder->Finish());
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
}

TEST_F(ChargeCompressionDictionaryBuildingBufferTest, BasicWithCacheFull) {
  if (GetSupportedDictCompressions().empty()) {
    ROCKSDB_GTEST_SKIP("No supported dict compression");
    return;
  }
  const auto kCompression = GetSupportedDictCompressions()[0];

  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  // A small kCacheCapacity is chosen so that increase cache charging for
  // buffering two data blocks, each containing key1/value1, key2/a big
  // value2, will cause cache full
  constexpr std::size_t kCacheCapacity =
      1 * kSizeDummyEntry + kSizeDummyEntry / 2;
  constexpr std::size_t kMaxDictBytes = 1024;
  // A big kMaxDictBufferBytes is chosen so that adding a big key value pair
  // (key2, value2) won't exceed the buffer limit
  constexpr std::size_t kMaxDictBufferBytes = 1024 * 1024 * 1024;

  // `CacheEntryRoleOptions::charged` is enabled by default for
  // CacheEntryRole::kCompressionDictionaryBuildingBuffer
  BlockBasedTableOptions table_options;
  LRUCacheOptions lo;
  lo.capacity = kCacheCapacity;
  lo.num_shard_bits = 0;  // 2^0 shard
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache(NewLRUCache(lo));
  table_options.block_cache = cache;
  table_options.flush_block_policy_factory =
      std::make_shared<FlushBlockEveryKeyPolicyFactory>();

  Options options;
  options.compression = kCompression;
  options.compression_opts.max_dict_bytes = kMaxDictBytes;
  options.compression_opts.max_dict_buffer_bytes = kMaxDictBufferBytes;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  test::StringSink* sink = new test::StringSink();
  std::unique_ptr<FSWritableFile> holder(sink);
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(holder), "test_file_name", FileOptions()));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator ikc(options.comparator);
  InternalTblPropCollFactories internal_tbl_prop_coll_factories;

  const ReadOptions read_options;
  const WriteOptions write_options;
  std::unique_ptr<TableBuilder> builder(options.table_factory->NewTableBuilder(
      TableBuilderOptions(ioptions, moptions, read_options, write_options, ikc,
                          &internal_tbl_prop_coll_factories, kCompression,
                          options.compression_opts, kUnknownColumnFamily,
                          "test_cf", -1 /* level */, kUnknownNewestKeyTime),
      file_writer.get()));

  std::string key1 = "key1";
  std::string value1 = "val1";
  InternalKey ik1(key1, 0 /* sequnce number */, kTypeValue);
  // Adding the first key won't trigger a flush by FlushBlockEveryKeyPolicy
  // therefore won't trigger any data block's buffering
  builder->Add(ik1.Encode(), value1);
  ASSERT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  std::string key2 = "key2";
  std::string value2(kSizeDummyEntry, '0');
  InternalKey ik2(key2, 1 /* sequnce number */, kTypeValue);
  // Adding the second key will trigger a flush of the last data block (the one
  // containing key1 and value1) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik2.Encode(), value2);
  // Cache charging will increase for the last buffered data block (the one
  // containing key1 and value1) since the buffer limit is not exceeded after
  // the buffering and the cache will not be full after this reservation
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
  EXPECT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead);

  std::string key3 = "key3";
  std::string value3 = "value3";
  InternalKey ik3(key3, 2 /* sequnce number */, kTypeValue);
  // Adding the third key will trigger a flush of the last data block (the one
  // containing key2 and value2) by FlushBlockEveryKeyPolicy and hence trigger
  // buffering of the last data block.
  builder->Add(ik3.Encode(), value3);
  // Cache charging will decrease since the cache is now full after
  // increasing reservation for the last buffered block and EnterUnbuffered() is
  // triggered
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);

  ASSERT_OK(builder->Finish());
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry);
}

class CacheUsageOptionsOverridesTest : public DBTestBase {
 public:
  CacheUsageOptionsOverridesTest()
      : DBTestBase("cache_usage_options_overrides_test",
                   /*env_do_fsync=*/false) {}
};

TEST_F(CacheUsageOptionsOverridesTest, SanitizeAndValidateOptions) {
  // To test `cache_usage_options.options_overrides` is sanitized
  // where `cache_usage_options.options` is used when there is no entry in
  // `cache_usage_options.options_overrides`
  Options options;
  options.create_if_missing = true;
  BlockBasedTableOptions table_options = BlockBasedTableOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Destroy(options);
  Status s = TryReopen(options);
  EXPECT_TRUE(s.ok());
  const auto* sanitized_table_options =
      options.table_factory->GetOptions<BlockBasedTableOptions>();
  const auto sanitized_options_overrides =
      sanitized_table_options->cache_usage_options.options_overrides;
  EXPECT_EQ(sanitized_options_overrides.size(), kNumCacheEntryRoles);
  for (auto options_overrides_iter = sanitized_options_overrides.cbegin();
       options_overrides_iter != sanitized_options_overrides.cend();
       ++options_overrides_iter) {
    CacheEntryRoleOptions role_options = options_overrides_iter->second;
    CacheEntryRoleOptions default_options =
        sanitized_table_options->cache_usage_options.options;
    EXPECT_TRUE(role_options == default_options);
  }
  Destroy(options);

  // To test option validation on unsupported CacheEntryRole
  table_options = BlockBasedTableOptions();
  table_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kDataBlock,
       {/*.charged = */ CacheEntryRoleOptions::Decision::kDisabled}});
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Destroy(options);
  s = TryReopen(options);
  EXPECT_TRUE(s.IsNotSupported());
  EXPECT_TRUE(
      s.ToString().find("Enable/Disable CacheEntryRoleOptions::charged") !=
      std::string::npos);
  EXPECT_TRUE(
      s.ToString().find(kCacheEntryRoleToCamelString[static_cast<uint32_t>(
          CacheEntryRole::kDataBlock)]) != std::string::npos);
  Destroy(options);

  // To test option validation on existence of block cache
  table_options = BlockBasedTableOptions();
  table_options.no_block_cache = true;
  table_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kFilterConstruction,
       {/*.charged = */ CacheEntryRoleOptions::Decision::kEnabled}});
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Destroy(options);
  s = TryReopen(options);
  EXPECT_TRUE(s.IsInvalidArgument());
  EXPECT_TRUE(s.ToString().find("Enable CacheEntryRoleOptions::charged") !=
              std::string::npos);
  EXPECT_TRUE(
      s.ToString().find(kCacheEntryRoleToCamelString[static_cast<std::size_t>(
          CacheEntryRole::kFilterConstruction)]) != std::string::npos);
  EXPECT_TRUE(s.ToString().find("block cache is disabled") !=
              std::string::npos);
  Destroy(options);
}

class ExternalTableTest : public DBTestBase {
 public:
  ExternalTableTest()
      : DBTestBase("external_table_test", /*env_do_fsync=*/false) {}

 protected:
  // A reader that pins values from its internal buffer, exercising the
  // zero-copy path in ExternalTableReaderAdapter::Get().
  class PinnedSimpleExternalTableReader
      : public SimpleExternalTableReader<
            ExternalTableMode::kOnlyZeroSeqnoAndPuts> {
   public:
    using SimpleExternalTableReader<
        ExternalTableMode::kOnlyZeroSeqnoAndPuts>::SimpleExternalTableReader;

    Status Get(const ReadOptions& /*read_options*/, const Slice& key,
               const SliceTransform* /*prefix_extractor*/,
               PinnableSlice* value) override {
      auto it = pinned_data_.find(key.ToString());
      if (it != pinned_data_.end()) {
        Slice s(it->second);
        value->PinSlice(s, &PinCleanup, &pin_cleanup_count_, nullptr);
        return Status::OK();
      }
      return Status::NotFound();
    }

    void SetPinnedData(const std::map<std::string, std::string>& data) {
      pinned_data_ = data;
    }

    int pin_cleanup_count() const { return pin_cleanup_count_; }

   private:
    static void PinCleanup(void* arg1, void* /*arg2*/) {
      (*static_cast<int*>(arg1))++;
    }
    std::map<std::string, std::string> pinned_data_;
    int pin_cleanup_count_ = 0;
  };

  class ConfigurableSimpleExternalTableFactory
      : public SimpleExternalTableFactory {
   public:
    static const char* kClassName() {
      return "ConfigurableSimpleExternalTableFactory";
    }

    static const char* kValidConfig() { return "mode=fast;limit=7"; }

    static const char* kAlternateValidConfig() {
      return "mode=compact;limit=11";
    }

    static std::string ValidFactoryConfig() {
      return "{id=" + std::string(kClassName()) + ";external_table_config={" +
             kValidConfig() + "}}";
    }

    static std::string SerializedValidConfig() {
      return SerializeConfig(kValidConfig());
    }

    static std::string SerializedAlternateValidConfig() {
      return SerializeConfig(kAlternateValidConfig());
    }

    const char* Name() const override { return kClassName(); }

    Status Configure(const std::string& config) override {
      if (config.empty() || config == kValidConfig() ||
          config == kAlternateValidConfig()) {
        config_ = config;
        return Status::OK();
      }
      return Status::InvalidArgument("Invalid simple external table config");
    }

    const std::string& config() const { return config_; }

   private:
    static std::string SerializeConfig(const std::string& config) {
      return "{" + config + "}";
    }

    std::string config_;
  };

  static void RegisterConfigurableSimpleExternalTableFactory() {
    static FactoryFunc<TableFactory> registration =
        ObjectLibrary::Default()->AddFactory<TableFactory>(
            ConfigurableSimpleExternalTableFactory::kClassName(),
            [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
               std::string* /*errmsg*/) {
              std::shared_ptr<ExternalTableFactory> inner =
                  std::make_shared<ConfigurableSimpleExternalTableFactory>();
              std::unique_ptr<TableFactory> factory =
                  NewExternalTableFactory(std::move(inner));
              guard->reset(factory.release());
              return guard->get();
            });
    (void)registration;
  }

  class CountingFileReadListener : public EventListener {
   public:
    bool ShouldBeNotifiedOnFileIO() override { return true; }

    void OnFileReadFinish(const FileOperationInfo& info) override {
      if (!info.status.ok()) {
        return;
      }
      read_count_.fetch_add(1);
      read_bytes_.fetch_add(info.length);
    }

    uint64_t read_count() const { return read_count_.load(); }
    uint64_t read_bytes() const { return read_bytes_.load(); }

   private:
    std::atomic<uint64_t> read_count_{0};
    std::atomic<uint64_t> read_bytes_{0};
  };

  class PinnedSimpleExternalTableFactory : public ExternalTableFactory {
   public:
    const char* Name() const override {
      return "PinnedSimpleExternalTableFactory";
    }

    Status NewTableReader(
        const ReadOptions& read_options, const std::string& /*file_path*/,
        const ExternalTableOptions& table_options,
        std::unique_ptr<FSRandomAccessFile>&& file, uint64_t file_size,
        std::unique_ptr<ExternalTableReader>* table_reader) const override {
      auto* reader = new PinnedSimpleExternalTableReader(
          read_options, table_options, std::move(file), file_size);
      if (!reader->status().ok()) {
        Status status = reader->status();
        delete reader;
        return status;
      }
      last_reader_ = reader;
      table_reader->reset(reader);
      return Status::OK();
    }

    ExternalTableBuilder* NewTableBuilder(
        const ExternalTableBuilderOptions& options,
        const std::string& file_path, FSWritableFile* file) const override {
      return simple_factory_.NewTableBuilder(options, file_path, file);
    }

    PinnedSimpleExternalTableReader* last_reader() const {
      return last_reader_;
    }

   private:
    SimpleExternalTableFactory simple_factory_;
    mutable PinnedSimpleExternalTableReader* last_reader_ = nullptr;
  };
};

class ExternalTablePointReadTest : public ExternalTableTest,
                                   public testing::WithParamInterface<bool> {
 public:
  ExternalTablePointReadTest() : use_coroutine_(GetParam()) {}

 protected:
  const bool use_coroutine_;
};

TEST_F(ExternalTableTest, BootstrapConfig) {
  RegisterConfigurableSimpleExternalTableFactory();

  ConfigOptions config_options;
  std::shared_ptr<TableFactory> table_factory;
  ASSERT_OK(TableFactory::CreateFromString(
      config_options,
      ConfigurableSimpleExternalTableFactory::ValidFactoryConfig(),
      &table_factory));
  ASSERT_NE(table_factory, nullptr);

  std::string serialized_config;
  ASSERT_OK(table_factory->GetOption(config_options, "external_table_config",
                                     &serialized_config));
  ASSERT_EQ(serialized_config,
            ConfigurableSimpleExternalTableFactory::SerializedValidConfig());

  std::shared_ptr<TableFactory> invalid_factory;
  const std::string invalid_config =
      "{id=" +
      std::string(ConfigurableSimpleExternalTableFactory::kClassName()) +
      ";external_table_config={mode=invalid}}";
  ASSERT_NOK(TableFactory::CreateFromString(config_options, invalid_config,
                                            &invalid_factory));
}

TEST_F(ExternalTableTest, GetWrappedExternalTableFactory) {
  std::shared_ptr<ExternalTableFactory> inner =
      std::make_shared<SimpleExternalTableFactory>();
  std::unique_ptr<TableFactory> table_factory = NewExternalTableFactory(inner);
  std::unique_ptr<TableFactory> block_based_factory(
      NewBlockBasedTableFactory());

  ASSERT_EQ(GetWrappedExternalTableFactory(table_factory.get()), inner);
  ASSERT_EQ(GetWrappedExternalTableFactory(block_based_factory.get()), nullptr);
  ASSERT_EQ(GetWrappedExternalTableFactory(nullptr), nullptr);
}

TEST_F(ExternalTableTest, BootstrapConfigOptionsFileRoundTrip) {
  RegisterConfigurableSimpleExternalTableFactory();

  DBOptions db_options;
  db_options.env = env_;
  ConfigOptions config_options(db_options);
  std::shared_ptr<TableFactory> table_factory;
  ASSERT_OK(TableFactory::CreateFromString(
      config_options,
      ConfigurableSimpleExternalTableFactory::ValidFactoryConfig(),
      &table_factory));

  ColumnFamilyOptions cf_options;
  cf_options.table_factory = table_factory;
  const std::vector<std::string> cf_names{"default"};
  const std::vector<ColumnFamilyOptions> all_cf_options{cf_options};
  const std::string options_file =
      test::PerThreadDBPath("external_table_config_options");
  std::shared_ptr<FileSystem> fs = db_options.env->GetFileSystem();
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), config_options, db_options,
                                  cf_names, all_cf_options, options_file,
                                  fs.get()));

  std::string contents;
  ASSERT_OK(ReadFileToString(env_, options_file, &contents));
  ASSERT_NE(contents.find("external_table_config={mode=fast;limit=7}"),
            std::string::npos);

  RocksDBOptionsParser parser;
  ASSERT_OK(parser.Parse(config_options, options_file, fs.get()));
  ASSERT_EQ(parser.cf_opts()->size(), 1U);
  ASSERT_NE(parser.cf_opts()->front().table_factory, nullptr);
  std::string loaded_config;
  ASSERT_OK(parser.cf_opts()->front().table_factory->GetOption(
      config_options, "external_table_config", &loaded_config));
  ASSERT_EQ(loaded_config,
            ConfigurableSimpleExternalTableFactory::SerializedValidConfig());

  ASSERT_OK(env_->DeleteFile(options_file));
}

TEST_F(ExternalTableTest, BootstrapConfigIsImmutable) {
  RegisterConfigurableSimpleExternalTableFactory();

  Options options = GetDefaultOptions();
  ConfigOptions config_options(options);
  ASSERT_OK(TableFactory::CreateFromString(
      config_options,
      ConfigurableSimpleExternalTableFactory::ValidFactoryConfig(),
      &options.table_factory));
  options.create_if_missing = true;

  const std::string dbname =
      test::PerThreadDBPath("external_table_config_immutable");
  ASSERT_OK(DestroyDB(dbname, options));
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname, &db));
  Status update_status =
      db->SetOptions({{"table_factory.external_table_config", "{mode=slow}"}});
  ASSERT_TRUE(update_status.IsInvalidArgument()) << update_status.ToString();

  Options current_options = db->GetOptions();
  std::string current_config;
  ASSERT_OK(current_options.table_factory->GetOption(
      config_options, "external_table_config", &current_config));
  ASSERT_EQ(current_config,
            ConfigurableSimpleExternalTableFactory::SerializedValidConfig());

  ASSERT_OK(db->Close());
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options));
}

TEST_F(ExternalTableTest, BootstrapConfigCanChangeBetweenDBOpens) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;

  std::shared_ptr<ConfigurableSimpleExternalTableFactory> first_factory =
      std::make_shared<ConfigurableSimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(first_factory);
  ConfigOptions config_options(options);
  ASSERT_OK(options.table_factory->ConfigureFromString(
      config_options,
      "external_table_config=" +
          ConfigurableSimpleExternalTableFactory::SerializedValidConfig()));
  ASSERT_EQ(first_factory->config(),
            ConfigurableSimpleExternalTableFactory::kValidConfig());

  const std::string dbname =
      test::PerThreadDBPath("external_table_config_reopen");
  ASSERT_OK(DestroyDB(dbname, options));
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, dbname, &db));
  ASSERT_EQ(first_factory->config(),
            ConfigurableSimpleExternalTableFactory::kValidConfig());
  ASSERT_OK(db->Close());
  db.reset();

  std::shared_ptr<ConfigurableSimpleExternalTableFactory> second_factory =
      std::make_shared<ConfigurableSimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(second_factory);
  ConfigOptions reopen_config_options(options);
  ASSERT_OK(options.table_factory->ConfigureFromString(
      reopen_config_options, "external_table_config=" +
                                 ConfigurableSimpleExternalTableFactory::
                                     SerializedAlternateValidConfig()));
  ASSERT_EQ(second_factory->config(),
            ConfigurableSimpleExternalTableFactory::kAlternateValidConfig());

  ASSERT_OK(DB::Open(options, dbname, &db));
  ASSERT_EQ(second_factory->config(),
            ConfigurableSimpleExternalTableFactory::kAlternateValidConfig());

  std::string reopened_config;
  ASSERT_OK(db->GetOptions().table_factory->GetOption(
      reopen_config_options, "external_table_config", &reopened_config));
  ASSERT_EQ(
      reopened_config,
      ConfigurableSimpleExternalTableFactory::SerializedAlternateValidConfig());

  ASSERT_OK(db->Close());
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options));
}

TEST_F(ExternalTableTest, BasicTest) {
  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();

  std::string file_path = test::PerThreadDBPath("external_table");
  const auto& fs = env_->GetFileSystem();
  const FileOptions file_options;
  std::unique_ptr<FSWritableFile> writable_file;
  ASSERT_OK(
      fs->NewWritableFile(file_path, file_options, &writable_file, nullptr));
  {
    std::unique_ptr<ExternalTableBuilder> builder;
    builder.reset(factory->NewTableBuilder(
        ExternalTableBuilderOptions(ReadOptions(), WriteOptions(),
                                    std::shared_ptr<const SliceTransform>(),
                                    BytewiseComparator(), "default",
                                    TableFileCreationReason::kMisc, fs),
        file_path, writable_file.get()));
    builder->Add("foo", "bar");
    ASSERT_OK(builder->Finish());
  }
  ASSERT_OK(writable_file->Close(IOOptions(), nullptr));

  std::unique_ptr<ExternalTableReader> reader;
  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(fs->NewRandomAccessFile(file_path, file_options, &file, nullptr));
  uint64_t file_size = 0;
  ASSERT_OK(fs->GetFileSize(file_path, IOOptions(), &file_size, nullptr));
  std::shared_ptr<SliceTransform> prefix_extractor;
  ASSERT_OK(factory->NewTableReader(
      {}, file_path,
      ExternalTableOptions(prefix_extractor, BytewiseComparator(), fs,
                           file_options),
      std::move(file), file_size, &reader));

  ReadOptions ro;
  std::unique_ptr<ExternalTableIterator> iter(reader->NewIterator(ro, nullptr));
  ASSERT_NE(iter, nullptr);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->value(), "bar");
  iter->Next();
  ASSERT_FALSE(iter->Valid());

  PinnableSlice val;
  ASSERT_OK(reader->Get({}, "foo", nullptr, &val));
  ASSERT_EQ(val, "bar");

  std::vector<PinnableSlice> vals;
  std::vector<Status> statuses;
  reader->MultiGet({}, {"foo", "bar"}, nullptr, &vals, &statuses);
  ASSERT_EQ(vals.size(), 2);
  ASSERT_EQ(statuses.size(), 2);
  ASSERT_EQ(vals[0], "bar");
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::NotFound());
}

TEST_F(ExternalTableTest, BasicModeGlobalSeqnoAcrossLevels) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }

  Options options = GetDefaultOptions();
  options.disable_auto_compactions = true;
  options.table_factory =
      NewExternalTableFactory(std::make_shared<SimpleExternalTableFactory>());
  ASSERT_FALSE(options.table_factory->IsDeleteRangeSupported());
  Reopen(options);

  const std::string old_file = dbname_ + "/external_old.immutable";
  SstFileWriter writer(EnvOptions(options), options);
  ASSERT_OK(writer.Open(old_file));
  ASSERT_OK(writer.Put("a", "old-a"));
  ASSERT_OK(writer.Put("c", "old-c"));
  ASSERT_OK(writer.Finish());

  IngestExternalFileOptions ingest_options;
  ingest_options.allow_global_seqno = true;
  ASSERT_OK(db_->IngestExternalFile({old_file}, ingest_options));
  ASSERT_EQ(NumTableFilesAtLevel(6), 1);

  const Snapshot* snapshot = db_->GetSnapshot();

  const std::string new_file = dbname_ + "/external_new.immutable";
  ASSERT_OK(writer.Open(new_file));
  ASSERT_OK(writer.Put("a", "new-a"));
  ASSERT_OK(writer.Put("b", "new-b"));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({new_file}, ingest_options));
  ASSERT_EQ(NumTableFilesAtLevel(5), 1);
  ASSERT_EQ(NumTableFilesAtLevel(6), 1);

  auto* cfhi = static_cast_with_check<ColumnFamilyHandleImpl>(
      dbfull()->DefaultColumnFamily());
  SuperVersion* super_version = cfhi->cfd()->GetSuperVersion();
  SequenceNumber latest_sequence = kMaxSequenceNumber;
  bool found_record = false;
  bool is_blob_index = false;
  ASSERT_OK(dbfull()->GetLatestSequenceForKey(
      super_version, "a", /*cache_only=*/false, /*lower_bound_seq=*/0,
      &latest_sequence, /*timestamp=*/nullptr, &found_record, &is_blob_index));
  ASSERT_TRUE(found_record);
  ASSERT_EQ(latest_sequence, dbfull()->GetLatestSequenceNumber());
  ASSERT_FALSE(is_blob_index);

  latest_sequence = kMaxSequenceNumber;
  found_record = false;
  ASSERT_OK(dbfull()->GetLatestSequenceForKey(
      super_version, "c", /*cache_only=*/false, /*lower_bound_seq=*/0,
      &latest_sequence, /*timestamp=*/nullptr, &found_record, &is_blob_index));
  ASSERT_TRUE(found_record);
  ASSERT_EQ(latest_sequence, 0);
  ASSERT_FALSE(is_blob_index);

  ASSERT_EQ(Get("a"), "new-a");
  ASSERT_EQ(Get("b"), "new-b");
  ASSERT_EQ(Get("c"), "old-c");

  ReadOptions snapshot_read_options;
  snapshot_read_options.snapshot = snapshot;
  std::string value;
  ASSERT_OK(db_->Get(snapshot_read_options, "a", &value));
  ASSERT_EQ(value, "old-a");
  ASSERT_TRUE(db_->Get(snapshot_read_options, "b", &value).IsNotFound());

  std::array<Slice, 3> keys = {Slice("a"), Slice("b"), Slice("c")};
  std::array<PinnableSlice, 3> values;
  std::array<Status, 3> statuses;
  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[0], "new-a");
  ASSERT_EQ(values[1], "new-b");
  ASSERT_EQ(values[2], "old-c");

  db_->MultiGet(snapshot_read_options, db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[0], "old-a");
  ASSERT_EQ(values[2], "old-c");

  std::vector<std::pair<std::string, std::string>> actual;
  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    actual.emplace_back(iterator->key().ToString(),
                        iterator->value().ToString());
  }
  ASSERT_OK(iterator->status());
  const std::vector<std::pair<std::string, std::string>> expected = {
      {"a", "new-a"}, {"b", "new-b"}, {"c", "old-c"}};
  ASSERT_EQ(actual, expected);

  actual.clear();
  iterator.reset(db_->NewIterator(snapshot_read_options));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    actual.emplace_back(iterator->key().ToString(),
                        iterator->value().ToString());
  }
  ASSERT_OK(iterator->status());
  const std::vector<std::pair<std::string, std::string>> snapshot_expected = {
      {"a", "old-a"}, {"c", "old-c"}};
  ASSERT_EQ(actual, snapshot_expected);

  iterator.reset();
  const std::string range_file = dbname_ + "/external_range.immutable";
  ASSERT_OK(writer.Open(range_file));
  ASSERT_OK(writer.DeleteRange("a", "b"));
  ASSERT_OK(writer.Finish());

  Options unsupported_options = options;
  unsupported_options.table_factory = NewExternalTableFactory(
      std::make_shared<PinnedSimpleExternalTableFactory>());
  SstFileReader unsupported_reader(unsupported_options);
  ASSERT_TRUE(unsupported_reader.Open(range_file).IsCorruption());

  ASSERT_OK(db_->IngestExternalFile({range_file}, ingest_options));
  ASSERT_EQ(Get("a"), "NOT_FOUND");
  ASSERT_EQ(Get("b"), "new-b");
  ASSERT_EQ(Get("c"), "old-c");
  ASSERT_OK(db_->Get(snapshot_read_options, "a", &value));
  ASSERT_EQ(value, "old-a");

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());
  ASSERT_TRUE(statuses[0].IsNotFound());
  ASSERT_OK(statuses[1]);
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[1], "new-b");
  ASSERT_EQ(values[2], "old-c");

  actual.clear();
  iterator.reset(db_->NewIterator(ReadOptions()));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    actual.emplace_back(iterator->key().ToString(),
                        iterator->value().ToString());
  }
  ASSERT_OK(iterator->status());
  const std::vector<std::pair<std::string, std::string>> range_expected = {
      {"b", "new-b"}, {"c", "old-c"}};
  ASSERT_EQ(actual, range_expected);

  iterator.reset();
  Status compaction_status =
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(compaction_status.IsNotSupported())
      << compaction_status.ToString();
  db_->ReleaseSnapshot(snapshot);
  Close();
  Reopen(options);
  ASSERT_EQ(Get("a"), "NOT_FOUND");
  ASSERT_EQ(Get("b"), "new-b");
  ASSERT_EQ(Get("c"), "old-c");
}

TEST_P(ExternalTablePointReadTest, FullModeLiveWritesAndCompaction) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }

  Options options = GetDefaultOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.table_factory = NewExternalTableFactory(
      std::make_shared<SimpleFullExternalTableFactory>());
  Reopen(options);

  ASSERT_OK(Put("deleted", "old"));
  ASSERT_OK(Put("merged", "base"));
  ASSERT_OK(Put("range-deleted-a", "old-a"));
  ASSERT_OK(Put("range-deleted-b", "old-b"));
  ASSERT_OK(Put("stable", "v1"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 1);

  {
    ManagedSnapshot snapshot(db_.get());

    ASSERT_OK(Merge("merged", "m1"));
    ASSERT_OK(Delete("deleted"));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "range-deleted-a", "range-deleted-c"));
    ASSERT_OK(Put("stable", "v2"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(1);
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_EQ(NumTableFilesAtLevel(1), 1);
    ASSERT_EQ(NumTableFilesAtLevel(2), 1);

    ASSERT_OK(Merge("merged", "m2"));
    ASSERT_OK(Put("live", "memtable"));

    ASSERT_EQ(
        Get("merged", static_cast<const Snapshot*>(nullptr), use_coroutine_),
        "base,m1,m2");
    ASSERT_EQ(
        Get("deleted", static_cast<const Snapshot*>(nullptr), use_coroutine_),
        "NOT_FOUND");
    ASSERT_EQ(
        Get("stable", static_cast<const Snapshot*>(nullptr), use_coroutine_),
        "v2");
    ASSERT_EQ(Get("range-deleted-a", static_cast<const Snapshot*>(nullptr),
                  use_coroutine_),
              "NOT_FOUND");
    ASSERT_EQ(Get("range-deleted-b", static_cast<const Snapshot*>(nullptr),
                  use_coroutine_),
              "NOT_FOUND");

    ASSERT_EQ(Get("merged", snapshot.snapshot(), use_coroutine_), "base");
    ASSERT_EQ(Get("deleted", snapshot.snapshot(), use_coroutine_), "old");
    ASSERT_EQ(Get("range-deleted-a", snapshot.snapshot(), use_coroutine_),
              "old-a");
    ASSERT_EQ(Get("range-deleted-b", snapshot.snapshot(), use_coroutine_),
              "old-b");
    ASSERT_EQ(Get("stable", snapshot.snapshot(), use_coroutine_), "v1");

    ASSERT_OK(Flush());
    ASSERT_EQ(NumTableFilesAtLevel(0), 1);
    ASSERT_EQ(NumTableFilesAtLevel(1), 1);
    ASSERT_EQ(NumTableFilesAtLevel(2), 1);

    MoveFilesToLevel(2);
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_EQ(NumTableFilesAtLevel(1), 0);
    ASSERT_EQ(NumTableFilesAtLevel(2), 1);

    const std::vector<std::string> expected_values = {
        "NOT_FOUND", "memtable", "base,m1,m2", "NOT_FOUND", "NOT_FOUND", "v2"};
    ASSERT_EQ(MultiGet({"deleted", "live", "merged", "range-deleted-a",
                        "range-deleted-b", "stable"},
                       /*snapshot=*/nullptr, /*async=*/false,
                       /*optimize_multiget_for_io=*/true, use_coroutine_),
              expected_values);

    std::vector<std::pair<std::string, std::string>> actual;
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      actual.emplace_back(iterator->key().ToString(),
                          iterator->value().ToString());
    }
    ASSERT_OK(iterator->status());
    const std::vector<std::pair<std::string, std::string>> expected = {
        {"live", "memtable"}, {"merged", "base,m1,m2"}, {"stable", "v2"}};
    ASSERT_EQ(actual, expected);

    ASSERT_EQ(Get("merged", snapshot.snapshot(), use_coroutine_), "base");
    ASSERT_EQ(Get("deleted", snapshot.snapshot(), use_coroutine_), "old");
    ASSERT_EQ(Get("range-deleted-a", snapshot.snapshot(), use_coroutine_),
              "old-a");
    ASSERT_EQ(Get("range-deleted-b", snapshot.snapshot(), use_coroutine_),
              "old-b");
    ASSERT_EQ(Get("stable", snapshot.snapshot(), use_coroutine_), "v1");
  }

  Close();
  Reopen(options);
  ASSERT_EQ(
      Get("merged", static_cast<const Snapshot*>(nullptr), use_coroutine_),
      "base,m1,m2");
  ASSERT_EQ(
      Get("deleted", static_cast<const Snapshot*>(nullptr), use_coroutine_),
      "NOT_FOUND");
  ASSERT_EQ(Get("live", static_cast<const Snapshot*>(nullptr), use_coroutine_),
            "memtable");
  ASSERT_EQ(Get("range-deleted-a", static_cast<const Snapshot*>(nullptr),
                use_coroutine_),
            "NOT_FOUND");
  ASSERT_EQ(Get("range-deleted-b", static_cast<const Snapshot*>(nullptr),
                use_coroutine_),
            "NOT_FOUND");
  ASSERT_EQ(
      Get("stable", static_cast<const Snapshot*>(nullptr), use_coroutine_),
      "v2");
}

TEST_F(ExternalTableTest, FullModeDBMultiScanWithGlobalSeqno) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }

  Options options = GetDefaultOptions();
  options.disable_auto_compactions = true;
  options.table_factory = NewExternalTableFactory(
      std::make_shared<SimpleFullExternalTableFactory>());
  Reopen(options);

  ASSERT_OK(Put("c", "old-c"));
  ASSERT_OK(Flush());

  const std::string file_path = dbname_ + "/multiscan.sst";
  SstFileWriter writer(EnvOptions(options), options);
  ASSERT_OK(writer.Open(file_path));
  for (const std::string key : {"a", "b", "c", "d", "e"}) {
    ASSERT_OK(writer.Put(key, "new-" + key));
  }
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({file_path}, IngestExternalFileOptions()));
  ASSERT_EQ(Get("c"), "new-c");

  const auto scan = [&](const MultiScanArgs& scan_options) {
    std::vector<std::pair<std::string, std::string>> entries;
    std::unique_ptr<MultiScan> iter = db_->NewMultiScan(
        ReadOptions(), db_->DefaultColumnFamily(), scan_options);
    for (auto range : *iter) {
      for (auto entry : range) {
        entries.emplace_back(entry.first.ToString(), entry.second.ToString());
      }
    }
    return entries;
  };

  MultiScanArgs one_range(BytewiseComparator());
  one_range.insert("b", "d");
  const std::vector<std::pair<std::string, std::string>> one_range_expected = {
      {"b", "new-b"}, {"c", "new-c"}};
  ASSERT_EQ(scan(one_range), one_range_expected);

  MultiScanArgs multiple_ranges(BytewiseComparator());
  multiple_ranges.insert("a", "c");
  multiple_ranges.insert("d", "f");
  const std::vector<std::pair<std::string, std::string>>
      multiple_ranges_expected = {
          {"a", "new-a"}, {"b", "new-b"}, {"d", "new-d"}, {"e", "new-e"}};
  ASSERT_EQ(scan(multiple_ranges), multiple_ranges_expected);
}

TEST_F(ExternalTableTest, FullModePropagatesCollectorCompactionMark) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }

  Options options = GetDefaultOptions();
  options.disable_auto_compactions = true;
  options.table_properties_collector_factories.emplace_back(
      NewCompactOnDeletionCollectorFactory(
          /*sliding_window_size=*/0, /*deletion_trigger=*/0,
          /*deletion_ratio=*/1.0));
  options.table_factory = NewExternalTableFactory(
      std::make_shared<SimpleFullExternalTableFactory>());
  Reopen(options);

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Delete("key"));
  ASSERT_OK(Flush());

  std::vector<std::vector<FileMetaData>> metadata;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &metadata);
  ASSERT_EQ(metadata[0].size(), 1);
  ASSERT_TRUE(metadata[0][0].marked_for_compaction);
}

TEST_F(ExternalTableTest, FullModePreservesUniqueIdAcrossReopen) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }

  Options options = GetDefaultOptions();
  options.table_factory = NewExternalTableFactory(
      std::make_shared<SimpleFullExternalTableFactory>());
  Reopen(options);

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  TablePropertiesCollection properties;
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1);
  std::string unique_id;
  ASSERT_OK(GetExtendedUniqueIdFromTableProperties(*properties.begin()->second,
                                                   &unique_id));

  Close();
  Reopen(options);
  properties.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1);
  std::string reopened_unique_id;
  ASSERT_OK(GetExtendedUniqueIdFromTableProperties(*properties.begin()->second,
                                                   &reopened_unique_id));
  ASSERT_EQ(reopened_unique_id, unique_id);
}

TEST_F(ExternalTableTest, FullModeVerifyChecksum) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }

  Options options = GetDefaultOptions();
  options.compression = kNoCompression;
  options.table_factory = NewExternalTableFactory(
      std::make_shared<SimpleFullExternalTableFactory>());
  Reopen(options);

  const std::string file_path = dbname_ + "/corrupt_ingest.sst";
  SstFileWriter writer(EnvOptions(options), options);
  ASSERT_OK(writer.Open(file_path));
  ASSERT_OK(writer.Put("key", std::string(1024, 'v')));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(test::CorruptFile(env_, file_path, /*offset=*/16,
                              /*bytes_to_corrupt=*/3));

  {
    SstFileReader reader(options);
    ASSERT_OK(reader.Open(file_path));
    ASSERT_TRUE(reader.VerifyChecksum().IsCorruption());
  }

  const std::string range_file_path = dbname_ + "/corrupt_range_ingest.sst";
  SstFileWriter range_writer(EnvOptions(options), options);
  ASSERT_OK(range_writer.Open(range_file_path));
  ASSERT_OK(range_writer.Put("key", std::string(1024, 'v')));
  ASSERT_OK(range_writer.DeleteRange("a", "z"));
  ASSERT_OK(range_writer.Finish());
  ASSERT_OK(test::CorruptFile(env_, range_file_path, /*offset=*/16,
                              /*bytes_to_corrupt=*/3));
  SstFileReader range_reader(options);
  ASSERT_TRUE(range_reader.Open(range_file_path).IsCorruption());

  const std::string missing_range_file_path =
      dbname_ + "/missing_range_ingest.sst";
  SstFileWriter missing_range_writer(EnvOptions(options), options);
  ASSERT_OK(missing_range_writer.Open(missing_range_file_path));
  ASSERT_OK(missing_range_writer.Put("key", "value"));
  ASSERT_OK(missing_range_writer.DeleteRange("a", "z"));
  ASSERT_OK(missing_range_writer.Finish());

  std::string missing_range_contents;
  ASSERT_OK(
      ReadFileToString(env_, missing_range_file_path, &missing_range_contents));
  constexpr size_t kSimpleExternalTableFooterSize =
      3 * sizeof(uint64_t) + sizeof(uint32_t);
  ASSERT_GT(missing_range_contents.size(), kSimpleExternalTableFooterSize);
  const size_t footer_offset =
      missing_range_contents.size() - kSimpleExternalTableFooterSize;
  const uint64_t range_deletion_size =
      DecodeFixed64(missing_range_contents.data() + footer_offset);
  const uint64_t properties_size = DecodeFixed64(
      missing_range_contents.data() + footer_offset + sizeof(uint64_t));
  ASSERT_GT(range_deletion_size, 0);
  ASSERT_LE(properties_size, footer_offset);
  ASSERT_LE(range_deletion_size, footer_offset - properties_size);
  missing_range_contents.erase(
      footer_offset - properties_size - range_deletion_size,
      range_deletion_size);
  const size_t new_footer_offset =
      missing_range_contents.size() - kSimpleExternalTableFooterSize;
  EncodeFixed64(&missing_range_contents[new_footer_offset], 0);
  const size_t checksummed_size =
      missing_range_contents.size() - sizeof(uint32_t);
  EncodeFixed32(&missing_range_contents[checksummed_size],
                crc32c::Value(missing_range_contents.data(), checksummed_size));
  ASSERT_OK(
      WriteStringToFile(env_, missing_range_contents, missing_range_file_path));

  SstFileReader missing_range_reader(options);
  Status missing_range_status =
      missing_range_reader.Open(missing_range_file_path);
  ASSERT_TRUE(missing_range_status.IsCorruption())
      << missing_range_status.ToString();

  IngestExternalFileOptions ingest_options;
  ingest_options.verify_checksums_before_ingest = true;
  ASSERT_TRUE(
      db_->IngestExternalFile({file_path}, ingest_options).IsCorruption());

  ASSERT_OK(Put("db-key", std::string(1024, 'v')));
  ASSERT_OK(Flush());
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1);
  ASSERT_OK(test::CorruptFile(
      env_, metadata.front().directory + "/" + metadata.front().name,
      /*offset=*/0, /*bytes_to_corrupt=*/3));
  ASSERT_TRUE(db_->VerifyChecksum().IsCorruption());
}

TEST_F(ExternalTableTest, SstReaderTest) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("external_table_test");
  std::string ingest_file = dbname + "test.immutabledb";
  dbname += "_db";

  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("a", "val_a"));
  ASSERT_OK(writer->Put("b", "val_b"));
  ASSERT_OK(writer->Put("c", "val_c"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<SstFileReader> reader(new SstFileReader(options));
  ASSERT_OK(reader->Open(ingest_file));

  // Test iterator
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(reader->NewIterator(ro));
  ASSERT_NE(iter, nullptr);
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->value(), "val_a");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), "val_b");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value(), "val_c");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().ok());

  // Verify external table Get() goes through the simple SaveValue entry point
  std::atomic<int> simple_save_value_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "GetContext::SaveValue::Simple",
      [&](void* /*arg*/) { simple_save_value_count.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  // Test MultiGet
  std::vector<Slice> keys = {"a", "b", "missing", "c"};
  std::vector<std::string> values;
  std::vector<Status> statuses = reader->MultiGet(ReadOptions(), keys, &values);
  ASSERT_EQ(simple_save_value_count, 3);
  ASSERT_EQ(values.size(), keys.size());
  ASSERT_EQ(statuses.size(), keys.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "val_a");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], "val_b");
  ASSERT_TRUE(statuses[2].IsNotFound());
  ASSERT_OK(statuses[3]);
  ASSERT_EQ(values[3], "val_c");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(ExternalTableTest, PropertiesBlockErrorPropagates) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }

  Options options = GetDefaultOptions();
  std::string file_path = test::PerThreadDBPath("external_table_error");
  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(file_path));
  ASSERT_OK(writer.Put("key", "value"));
  ASSERT_OK(writer.Finish());

  SyncPoint::GetInstance()->SetCallBack(
      "SimpleExternalTableReader::GetPropertiesBlock", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("injected error");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  SstFileReader reader(options);
  Status status = reader.Open(file_path);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(status.IsIOError()) << status.ToString();
}

TEST_F(ExternalTableTest, ReaderFileReadsUpdateStatistics) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  std::shared_ptr<CountingFileReadListener> listener =
      std::make_shared<CountingFileReadListener>();
  options.listeners.emplace_back(listener);
  std::string dbname = test::PerThreadDBPath("external_table_test");
  std::string ingest_file = dbname + "test.immutabledb";

  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("a", "val_a"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  const auto read_count_before =
      options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);
  const auto read_bytes_before =
      options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES);
  HistogramData sst_read_micros_before;
  options.statistics->histogramData(SST_READ_MICROS, &sst_read_micros_before);
  const auto listener_read_count_before = listener->read_count();
  const auto listener_read_bytes_before = listener->read_bytes();

  SyncPoint::GetInstance()->SetCallBack(
      "SimpleExternalTableFactory::NewTableReader:File", [&](void* arg) {
        auto* file = static_cast<FSRandomAccessFile*>(arg);
        ASSERT_NE(file, nullptr);
        char scratch = '\0';
        Slice result;
        ASSERT_OK(file->Read(0, 1, IOOptions(), &result, &scratch,
                             /*dbg=*/nullptr));
        ASSERT_EQ(result.size(), 1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<SstFileReader> reader(new SstFileReader(options));
  Status status = reader->Open(ingest_file);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(status);

  EXPECT_GT(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT),
            read_count_before);
  EXPECT_GT(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES),
            read_bytes_before);
  HistogramData sst_read_micros_after;
  options.statistics->histogramData(SST_READ_MICROS, &sst_read_micros_after);
  EXPECT_GT(sst_read_micros_after.count, sst_read_micros_before.count);
  EXPECT_GT(listener->read_count(), listener_read_count_before);
  EXPECT_GT(listener->read_bytes(), listener_read_bytes_before);
}

TEST_P(ExternalTablePointReadTest, PinnedGetTest) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  auto factory = std::make_shared<PinnedSimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);
  Reopen(options);

  std::string ingest_file = dbname_ + "/test.immutabledb";

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("key1", "val1"));
  ASSERT_OK(writer->Put("key2", "val2"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  IngestExternalFileOptions ifo;
  ASSERT_OK(db_->IngestExternalFile({ingest_file}, ifo));
  ASSERT_NE(factory->last_reader(), nullptr);

  factory->last_reader()->SetPinnedData(
      {{"key1", "pinned_val1"}, {"key2", "pinned_val2"}});

  // Verify external table Get() goes through the simple SaveValue entry point
  // (the no-ParsedInternalKey overload) rather than the complex one.
  std::atomic<int> simple_save_value_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "GetContext::SaveValue::Simple",
      [&](void* /*arg*/) { simple_save_value_count.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice pinnable;
  ASSERT_OK(Get("key1", &pinnable, use_coroutine_));
  ASSERT_EQ(pinnable.ToString(), "pinned_val1");
  ASSERT_TRUE(pinnable.IsPinned());
  pinnable.Reset();

  ASSERT_OK(Get("key2", &pinnable, use_coroutine_));
  ASSERT_EQ(pinnable.ToString(), "pinned_val2");
  ASSERT_TRUE(pinnable.IsPinned());
  pinnable.Reset();

  // Two found Gets => simple SaveValue invoked twice.
  ASSERT_EQ(simple_save_value_count.load(), 2);

  // Verify cleanup ran for both Gets
  ASSERT_EQ(factory->last_reader()->pin_cleanup_count(), 2);

  // Verify NotFound still works (does not invoke SaveValue)
  Status s = Get("missing", &pinnable, use_coroutine_);
  ASSERT_TRUE(s.IsNotFound());
  ASSERT_EQ(simple_save_value_count.load(), 2);

  // Test MultiGet with PinnableSlice to exercise the batched pin path
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;
  MultiGet({"key1", "missing", "key2"}, /*snapshot=*/nullptr,
           /*async=*/false, /*optimize_multiget_for_io=*/true, use_coroutine_,
           values, statuses);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "pinned_val1");
  ASSERT_TRUE(values[0].IsPinned());

  ASSERT_TRUE(statuses[1].IsNotFound());

  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2].ToString(), "pinned_val2");
  ASSERT_TRUE(values[2].IsPinned());

  for (PinnableSlice& value : values) {
    value.Reset();
  }
  ASSERT_EQ(factory->last_reader()->pin_cleanup_count(), 4);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

#if USE_COROUTINES
INSTANTIATE_TEST_CASE_P(SyncAndCoroutine, ExternalTablePointReadTest,
                        testing::Bool());
#else
INSTANTIATE_TEST_CASE_P(SyncAndCoroutine, ExternalTablePointReadTest,
                        testing::Values(false));
#endif  // USE_COROUTINES

TEST_F(ExternalTableTest, SstReaderPinnableMultiGetTest) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  std::string dbname =
      test::PerThreadDBPath("sst_reader_pinnable_multiget_test");
  std::string sst_file = dbname + "/test.sst";
  ASSERT_OK(options.env->CreateDirIfMissing(dbname));

  std::unique_ptr<SstFileWriter> writer(
      new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(sst_file));
  ASSERT_OK(writer->Put("a", "val_a"));
  ASSERT_OK(writer->Put("b", "val_b"));
  ASSERT_OK(writer->Put("c", "val_c"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<SstFileReader> reader(new SstFileReader(options));
  ASSERT_OK(reader->Open(sst_file));

  // Test PinnableSlice MultiGet
  std::vector<Slice> keys = {"a", "b", "missing", "c"};
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses = reader->MultiGet(ReadOptions(), keys, &values);
  ASSERT_EQ(values.size(), keys.size());
  ASSERT_EQ(statuses.size(), keys.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "val_a");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "val_b");
  ASSERT_TRUE(statuses[2].IsNotFound());
  ASSERT_OK(statuses[3]);
  ASSERT_EQ(values[3].ToString(), "val_c");

  // Verify std::string MultiGet wrapper still works
  std::vector<std::string> str_values;
  statuses = reader->MultiGet(ReadOptions(), keys, &str_values);
  ASSERT_EQ(str_values.size(), keys.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(str_values[0], "val_a");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(str_values[1], "val_b");
  ASSERT_TRUE(statuses[2].IsNotFound());
  ASSERT_OK(statuses[3]);
  ASSERT_EQ(str_values[3], "val_c");
}

TEST_F(ExternalTableTest, ExternalFileChecksumTest) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("external_table_test");
  std::string ingest_file = dbname + "test.immutable";
  dbname += "_db";
  ASSERT_OK(DestroyDB(dbname, options));

  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  // Create a file
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("foo", "bar"));
  ASSERT_OK(writer->Put("foo2", "bar2"));
  ExternalSstFileInfo info;
  ASSERT_OK(writer->Finish(&info));
  writer.reset();

  FileChecksumGenContext cksum_ctx;
  FileChecksumGenCrc32c cksum_gen(cksum_ctx);
  std::string file_data;
  ASSERT_OK(ReadFileToString(options.env, ingest_file, &file_data));
  cksum_gen.Update(file_data.data(), file_data.size());
  cksum_gen.Finalize();
  ASSERT_EQ(info.file_checksum, cksum_gen.GetChecksum());
}

TEST_F(ExternalTableTest, DBIterTest) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("external_table_test");
  std::string ingest_file = dbname + "test.immutable";
  dbname += "_db";
  ASSERT_OK(DestroyDB(dbname, options));

  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  // Create a file
  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("foo", "bar"));
  ASSERT_OK(writer->Put("foo2", "bar2"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<DB> db;
  options.create_if_missing = true;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  ifo.allow_db_generated_files = true;
  ifo.fill_cache = false;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  std::unique_ptr<Iterator> iter(db->NewIterator({}, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->value(), "bar");
  iter->Next();
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->key(), "foo2");
  ASSERT_EQ(iter->value(), "bar2");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
}

TEST_F(ExternalTableTest, ReadOptionsCustomContext) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("external_table_test");
  std::string ingest_file = dbname + "test.immutable";
  dbname += "_db";
  ASSERT_OK(DestroyDB(dbname, options));

  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  std::unique_ptr<SstFileWriter> writer(
      new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("foo", "bar"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<DB> db;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname, &db));
  ASSERT_NE(db, nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  ifo.allow_db_generated_files = true;
  ifo.fill_cache = false;
  ASSERT_OK(db->IngestExternalFile(cfh, {ingest_file}, ifo));

  int get_context = 0;
  int iterator_context = 0;
  int get_call_count = 0;
  int new_iterator_call_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "SimpleExternalTableReader::Get", [&](void* arg) {
        ReadOptions* read_options = static_cast<ReadOptions*>(arg);
        EXPECT_EQ(read_options->custom_context, &get_context);
        ++get_call_count;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "SimpleExternalTableReader::NewIterator", [&](void* arg) {
        ReadOptions* read_options = static_cast<ReadOptions*>(arg);
        EXPECT_EQ(read_options->custom_context, &iterator_context);
        ++new_iterator_call_count;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  ReadOptions get_options;
  ASSERT_EQ(get_options.custom_context, nullptr);
  get_options.custom_context = &get_context;
  std::string value;
  ASSERT_OK(db->Get(get_options, cfh, "foo", &value));
  ASSERT_EQ(value, "bar");
  ASSERT_EQ(get_call_count, 1);

  ReadOptions iterator_options;
  iterator_options.custom_context = &iterator_context;
  std::unique_ptr<Iterator> iter(db->NewIterator(iterator_options, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(iter->value(), "bar");
  ASSERT_EQ(new_iterator_call_count, 1);
  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
}

TEST_F(ExternalTableTest, DBMultiScanTest) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("external_table_test");
  std::string ingest_file = dbname + "test.immutable";
  dbname += "_db";
  ASSERT_OK(DestroyDB(dbname, options));

  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  // Create a file
  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  for (int i = 0; i < 100; ++i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    ASSERT_OK(writer->Put("k" + ss.str(), "val" + ss.str()));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<DB> db;
  options.create_if_missing = true;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  ifo.allow_db_generated_files = true;
  ifo.fill_cache = false;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  std::vector<std::string> key_ranges({"k03", "k10", "k25", "k50"});
  ReadOptions ro;
  MultiScanArgs scan_options(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);
  std::unique_ptr<MultiScan> iter = db->NewMultiScan(ro, cfh, scan_options);
  try {
    int idx = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString().compare(key_ranges[idx]), 0);
        ASSERT_LT(it.first.ToString().compare(key_ranges[idx + 1]), 0);
        count++;
      }
      idx += 2;
    }
    ASSERT_EQ(count, 32);
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
    std::cerr << "Iterator returned status " << ex.what();
    abort();
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();

  // Test the overlapping scan case
  key_ranges[1] = "k30";
  scan_options = MultiScanArgs(BytewiseComparator());
  scan_options.insert(key_ranges[0], key_ranges[1]);
  scan_options.insert(key_ranges[2], key_ranges[3]);

  iter = db->NewMultiScan(ro, cfh, scan_options);
  try {
    int idx = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString().compare(key_ranges[idx]), 0);
        ASSERT_LT(it.first.ToString().compare(key_ranges[idx + 1]), 0);
        count++;
      }
      idx += 2;
    }
    ASSERT_EQ(count, 52);
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();

  // Test the no limit scan case
  scan_options = MultiScanArgs(BytewiseComparator());
  scan_options.insert(key_ranges[0]);
  scan_options.insert(key_ranges[2]);
  iter = db->NewMultiScan(ro, cfh, scan_options);
  try {
    int idx = 0;
    int count = 0;
    for (auto range : *iter) {
      for (auto it : range) {
        ASSERT_GE(it.first.ToString().compare(key_ranges[idx]), 0);
        if (it.first.ToString().compare(key_ranges[idx + 1]) == 0) {
          break;
        }
        count++;
      }
      idx += 2;
    }
    ASSERT_EQ(count, 52);
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();

  SyncPoint::GetInstance()->SetCallBack(
      "SimpleExternalTableIterator::Constructor", [](void* arg) {
        Status* status = static_cast<Status*>(arg);
        *status = Status::IOError();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  iter = db->NewMultiScan(ro, cfh, scan_options);
  try {
    for (auto range : *iter) {
      // Should not get here. Iterator should throw an exception
      assert(false);
      for (auto it : range) {
        (void)it;
        assert(false);
      }
    }
  } catch (MultiScanException& ex) {
    // Make sure exception contains the status
    ASSERT_NOK(ex.status());
  } catch (std::logic_error& ex) {
    std::cerr << "Iterator returned logic error " << ex.what();
    abort();
  }
  iter.reset();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
}

TEST_F(ExternalTableTest, IngestionTest) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-encrypted environment");
    return;
  }
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("external_table_test");
  std::string ingest_file = dbname + "test.immutable";
  dbname += "_db";
  ASSERT_OK(DestroyDB(dbname, options));

  std::shared_ptr<ExternalTableFactory> factory =
      std::make_shared<SimpleExternalTableFactory>();
  options.table_factory = NewExternalTableFactory(factory);

  // Create a file
  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("foo", "bar"));
  ASSERT_OK(writer->Put("foo2", "bar2"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<DB> db;
  options.create_if_missing = true;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  ifo.allow_db_generated_files = false;
  ifo.fill_cache = false;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  std::unique_ptr<Iterator> iter(db->NewIterator({}, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->value(), "bar");
  iter->Next();
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->key(), "foo2");
  ASSERT_EQ(iter->value(), "bar2");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();

  // Create an overlapping file to ingest with atomic_replace_range option
  ingest_file += "2";
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("foo", "val"));
  ASSERT_OK(writer->Put("foo2", "val2"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  ifo.snapshot_consistency = false;
  s = db->IngestExternalFiles({{cfh,
                                {ingest_file},
                                ifo,
                                {},
                                {},
                                Temperature::kUnknown,
                                {{nullptr, nullptr}}}});
  ASSERT_OK(s);

  iter.reset(db->NewIterator({}, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->value(), "val");
  iter->Next();
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->key(), "foo2");
  ASSERT_EQ(iter->value(), "val2");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();

  // Create an overlapping file to ingest without atomic_replace_range. The
  // assigned global sequence number makes its values the newest.
  ingest_file += "3";
  writer.reset(new SstFileWriter(EnvOptions(), options));
  ASSERT_OK(writer->Open(ingest_file));
  ASSERT_OK(writer->Put("foo", "newval"));
  ASSERT_OK(writer->Put("foo2", "newval2"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  s = db->IngestExternalFiles(
      {{cfh, {ingest_file}, ifo, {}, {}, Temperature::kUnknown, {}}});
  ASSERT_OK(s);

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), cfh, "foo", &value));
  ASSERT_EQ(value, "newval");

  std::array<Slice, 2> keys = {Slice("foo"), Slice("foo2")};
  std::array<PinnableSlice, 2> values;
  std::array<Status, 2> statuses;
  db->MultiGet(ReadOptions(), cfh, keys.size(), keys.data(), values.data(),
               statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[0], "newval");
  ASSERT_EQ(values[1], "newval2");

  iter.reset(db->NewIterator({}, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->value(), "newval");
  iter->Next();
  ASSERT_TRUE(iter->Valid() && iter->status().ok());
  ASSERT_EQ(iter->key(), "foo2");
  ASSERT_EQ(iter->value(), "newval2");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
}

class UserDefinedIndexTestBase : public BlockBasedTableTestBase {
 public:
  class CustomFlushBlockPolicy : public FlushBlockPolicy {
   public:
    explicit CustomFlushBlockPolicy(int keys_per_block)
        : keys_in_current_block_(0), keys_per_block_(keys_per_block) {}

    bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
      if (keys_in_current_block_ >= keys_per_block_) {
        keys_in_current_block_ = 1;
        return true;
      }
      keys_in_current_block_++;
      return false;
    }

   private:
    int keys_in_current_block_;
    int keys_per_block_;
  };

  class CustomFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
   public:
    CustomFlushBlockPolicyFactory(int keys_per_block = 3)
        : keys_per_block_(keys_per_block) {}
    const char* Name() const override { return "CustomFlushBlockPolicy"; }
    FlushBlockPolicy* NewFlushBlockPolicy(const BlockBasedTableOptions&,
                                          const BlockBuilder&) const override {
      return new CustomFlushBlockPolicy(keys_per_block_);
    }
    int keys_per_block_;
  };

 public:
  class TestIndexFactory : public IndexFactory {
   public:
    using IndexFactory::NewBuilder;
    using IndexFactory::NewReader;

    const char* Name() const override { return "test_index"; }

    Status NewBuilder(
        const IndexFactoryOptions& /*option*/,
        std::unique_ptr<IndexFactoryBuilder>& builder) const override {
      auto b = std::make_unique<TestIndexFactoryBuilder>();
      b->skip_key_size_check_ = skip_key_size_check_;
      // Share the factory's key_type_log so tests can inspect after flush.
      b->shared_key_type_log_ = &key_type_log_;
      builder = std::move(b);
      return Status::OK();
    }

    // When true, builders skip key-size assertions (for variable-length keys).
    bool skip_key_size_check_ = false;

    // Accumulated log of (key, ValueType) pairs from all builders created
    // by this factory. Tests can inspect this after flush/compaction.
    mutable std::vector<std::pair<std::string, IndexFactoryBuilder::ValueType>>
        key_type_log_;

    struct CustomizedMapComparator {
      CustomizedMapComparator(const Comparator* _comparator)
          : comparator(_comparator) {}
      const Comparator* comparator;
      bool operator()(const std::string& lhs, const std::string& rhs) const {
        return comparator->Compare(lhs, rhs) < 0;
      }
    };

    Status NewReader(
        const IndexFactoryOptions& option, Slice& index_block,
        std::unique_ptr<IndexFactoryReader>& reader) const override {
      reader = std::make_unique<TestIndexFactoryReader>(
          index_block, option.comparator, this);
      return Status::OK();
    }

    uint64_t seek_error_count_ = 0;
    uint64_t next_error_count_ = 0;

   private:
    class TestIndexFactoryBuilder : public IndexFactoryBuilder {
     public:
      TestIndexFactoryBuilder() : entries_added_(0), keys_added_(0) {}

      Slice AddIndexEntry(const Slice& last_key_in_current_block,
                          const Slice* first_key_in_next_block,
                          const BlockHandle& block_handle,
                          std::string* separator_scratch,
                          const IndexEntryContext& /*context*/) override {
        if (keys_added_ == 0) {
          return last_key_in_current_block;
        }
        if (!skip_key_size_check_) {
          EXPECT_EQ(last_key_in_current_block.size(), 5);
          if (first_key_in_next_block) {
            EXPECT_EQ(first_key_in_next_block->size(), 5);
          }
        }
        // Unused parameters
        (void)separator_scratch;
        entries_added_++;
        index_data_[last_key_in_current_block.ToString()].clear();
        // Store the block handle for each key
        PutFixed64(&index_data_[last_key_in_current_block.ToString()],
                   block_handle.offset);
        PutFixed64(&index_data_[last_key_in_current_block.ToString()],
                   block_handle.size);
        PutFixed32(&index_data_[last_key_in_current_block.ToString()],
                   keys_added_);
        keys_added_ = 0;
        return last_key_in_current_block;
      }

      void OnKeyAdded(const Slice& key, ValueType type,
                      const Slice& /*value*/) override {
        if (key.starts_with("dummy")) {
          return;
        }
        if (!skip_key_size_check_) {
          EXPECT_EQ(key.size(), 5);
        }
        // Record the ValueType for each key so tests can verify the mapping.
        if (shared_key_type_log_) {
          shared_key_type_log_->emplace_back(key.ToString(), type);
        }
        // Track keys added to the current block (used by AddIndexEntry).
        keys_added_++;
        if (!skip_key_size_check_) {
          // For fixed-size key tests, add a dummy per-key entry that the
          // TestIndexFactoryReader can parse alongside block-level entries.
          PutFixed64(&index_data_[key.ToString()], 0);
          PutFixed64(&index_data_[key.ToString()], 0);
          PutFixed32(&index_data_[key.ToString()], 0);
        }
      }

      Status Finish(Slice* index_contents) override {
        if (entries_added_ == 0) {
          *index_contents = Slice();
          return Status::OK();
        }
        // Serialize the index data
        std::string result;
        for (const auto& entry : index_data_) {
          PutLengthPrefixedSlice(&result, entry.first);
          result.append(entry.second);
        }
        index_contents_data_ = result;
        *index_contents = index_contents_data_;
        return Status::OK();
      }

      int GetEntriesAdded() const { return entries_added_; }

      uint64_t EstimatedSize() const override { return 0; }

      // When true, skip the EXPECT_EQ(key.size(), 5) checks, allowing
      // variable-length keys (e.g., from DB flush/compaction).
      bool skip_key_size_check_ = false;

      // Points to the factory's shared log vector. Set by the factory.
      mutable std::vector<std::pair<std::string, ValueType>>*
          shared_key_type_log_ = nullptr;

     private:
      int entries_added_;
      std::map<std::string, std::string> index_data_;
      uint32_t keys_added_;
      std::string index_contents_data_;
    };

    class TestIndexFactoryReader : public IndexFactoryReader {
     public:
      explicit TestIndexFactoryReader(Slice& index_block,
                                      const Comparator* comparator,
                                      const TestIndexFactory* factory)
          : factory_(factory),
            comparator_(comparator),
            index_data_(CustomizedMapComparator(comparator)) {
        Slice block = index_block;
        while (!block.empty()) {
          Slice key;
          uint64_t offset = 0;
          uint64_t size = 0;
          uint32_t num_keys = 0;
          EXPECT_TRUE(GetLengthPrefixedSlice(&block, &key));
          EXPECT_TRUE(GetFixed64(&block, &offset));
          EXPECT_TRUE(GetFixed64(&block, &size));
          EXPECT_TRUE(GetFixed32(&block, &num_keys));

          IndexFactoryBuilder::BlockHandle handle{0, 0};
          handle.offset = offset;
          handle.size = size;
          index_data_[key.ToString()] =
              std::make_pair<IndexFactoryBuilder::BlockHandle, uint32_t>(
                  std::move(handle), std::move(num_keys));
        }
      }

      std::unique_ptr<IndexFactoryIterator> NewIterator(
          const ReadOptions& /*ro*/) override {
        return std::make_unique<TestIndexFactoryIterator>(index_data_, factory_,
                                                          comparator_);
      }

      size_t ApproximateMemoryUsage() const override { return 0; }

     private:
      class TestIndexFactoryIterator : public IndexFactoryIterator {
       public:
        TestIndexFactoryIterator(
            std::map<std::string,
                     std::pair<IndexFactoryBuilder::BlockHandle, uint32_t>,
                     CustomizedMapComparator>& index,
            const TestIndexFactory* factory, const Comparator* comparator)
            : index_(index),
              iter_(index_.end()),
              scan_opts_(nullptr),
              num_opts_(0),
              target_num_keys_(0),
              seek_error_count_(factory->seek_error_count_),
              next_error_count_(factory->next_error_count_),
              comparator_(comparator) {}

        Status SeekAndGetResult(const Slice& key, IterateResult* result,
                                const SeekContext& /*context*/) override {
          Status s;
          if (seek_error_count_) {
            seek_error_count_--;
            s = Status::IOError();
          }
          if (!s.ok()) {
            return s;
          }
          if (scan_opts_) {
            // Seeks should be in order specified in scan_opts_
            EXPECT_EQ(comparator_->Compare(
                          scan_opts_[scan_idx_].range.start.value(), key),
                      0);
            EXPECT_TRUE(scan_opts_[scan_idx_].property_bag.has_value());
            target_num_keys_ = std::stoi(scan_opts_[scan_idx_]
                                             .property_bag.value()
                                             .find("count")
                                             ->second);
            scan_idx_++;
          }
          iter_ = index_.lower_bound(key.ToString());
          if ((iter_ != index_.end()) && IsInbound()) {
            AdvanceToNextIndexEntry();
            if (iter_ != index_.end()) {
              result->bound_check_result = IterBoundCheck::kInbound;
              result->key = Slice(iter_->first);
              if (scan_opts_ && target_num_keys_ > 0 &&
                  comparator_->Compare(key, iter_->first) == 0) {
                target_num_keys_--;
              }
            } else {
              result->bound_check_result = IterBoundCheck::kUnknown;
              result->key = Slice();
            }
          } else {
            result->bound_check_result = IterBoundCheck::kOutOfBound;
            result->key = Slice();
          }
          return Status::OK();
        }

        // Override so SeekToFirst works under any comparator. The base
        // default seeks empty Slice, which lower_bound("") interprets as
        // "past end" for ReverseBytewiseComparator. Use map::begin().
        Status SeekToFirstAndGetResult(IterateResult* result) override {
          iter_ = index_.begin();
          AdvanceToNextIndexEntry();
          if (iter_ != index_.end()) {
            result->bound_check_result = IterBoundCheck::kInbound;
            result->key = Slice(iter_->first);
          } else {
            result->bound_check_result = IterBoundCheck::kUnknown;
            result->key = Slice();
          }
          return Status::OK();
        }

        Status NextAndGetResult(IterateResult* result) override {
          Status s;
          if (next_error_count_) {
            next_error_count_--;
            s = Status::IOError();
          }
          if (!s.ok()) {
            return s;
          }
          if (scan_opts_ && scan_opts_[scan_idx_ - 1].range.limit.has_value()) {
            if (comparator_->Compare(
                    iter_->first,
                    scan_opts_[scan_idx_ - 1].range.limit.value()) >= 0) {
              result->bound_check_result = IterBoundCheck::kOutOfBound;
              result->key = Slice();
              return Status::OK();
            }
          }
          if (scan_opts_ && target_num_keys_ == 0) {
            result->key = Slice();
            result->bound_check_result = IterBoundCheck::kOutOfBound;
            return Status::OK();
          }
          iter_++;
          if ((iter_ != index_.end()) && IsInbound()) {
            AdvanceToNextIndexEntry();
            if (iter_ != index_.end()) {
              result->bound_check_result = IterBoundCheck::kInbound;
              result->key = Slice(iter_->first);
              target_num_keys_ -=
                  std::min(target_num_keys_, iter_->second.second);
            } else {
              // AdvanceToNextIndexEntry reached end of map.
              result->bound_check_result = IterBoundCheck::kUnknown;
              result->key = Slice();
            }
          } else {
            // EOF
            result->bound_check_result = IterBoundCheck::kUnknown;
            result->key = Slice();
          }
          return Status::OK();
        }

        Status SeekToLastAndGetResult(IterateResult* result) override {
          if (index_.empty()) {
            result->bound_check_result = IterBoundCheck::kUnknown;
            result->key = Slice();
            return Status::OK();
          }

          iter_ = index_.end();
          --iter_;
          SetPreviousValidResult(result);
          return Status::OK();
        }

        Status PrevAndGetResult(IterateResult* result) override {
          if (iter_ == index_.end() || iter_ == index_.begin()) {
            iter_ = index_.end();
            result->bound_check_result = IterBoundCheck::kUnknown;
            result->key = Slice();
            return Status::OK();
          }

          --iter_;
          SetPreviousValidResult(result);
          return Status::OK();
        }

        void AdvanceToNextIndexEntry() {
          while (iter_ != index_.end() && iter_->second.second == 0) {
            iter_++;
          }
        }

        void SetPreviousValidResult(IterateResult* result) {
          while (iter_ != index_.end() && iter_->second.second == 0) {
            if (iter_ == index_.begin()) {
              iter_ = index_.end();
              break;
            }
            --iter_;
          }
          if (iter_ != index_.end()) {
            result->bound_check_result = IterBoundCheck::kInbound;
            result->key = Slice(iter_->first);
          } else {
            result->bound_check_result = IterBoundCheck::kUnknown;
            result->key = Slice();
          }
        }

        bool IsInbound() {
          if (!scan_opts_) {
            return true;
          }
          if (scan_opts_[scan_idx_ - 1].range.limit.has_value() &&
              comparator_->Compare(
                  scan_opts_[scan_idx_ - 1].range.limit.value(),
                  iter_->first) <= 0) {
            return false;
          }
          return true;
        }

        IndexFactoryBuilder::BlockHandle value() override {
          IndexFactoryBuilder::BlockHandle handle{0, 0};
          handle.offset = iter_->second.first.offset;
          handle.size = iter_->second.first.size;
          return handle;
        }

        void Prepare(const ScanOptions scan_opts[], size_t num_opts) override {
          // Prepare should only be called once
          EXPECT_EQ(scan_opts_, nullptr);
          scan_opts_ = scan_opts;
          num_opts_ = num_opts;
          scan_idx_ = 0;
        }

       private:
        std::map<std::string,
                 std::pair<IndexFactoryBuilder::BlockHandle, uint32_t>,
                 CustomizedMapComparator>& index_;
        std::map<std::string, std::pair<IndexFactoryBuilder::BlockHandle,
                                        uint32_t>>::iterator iter_;
        const ScanOptions* scan_opts_;
        size_t num_opts_{};
        size_t scan_idx_{};
        uint32_t target_num_keys_;
        uint64_t seek_error_count_;
        uint64_t next_error_count_;
        const Comparator* comparator_;
      };

      const TestIndexFactory* factory_;
      const Comparator* comparator_;
      std::map<std::string,
               std::pair<IndexFactoryBuilder::BlockHandle, uint32_t>,
               CustomizedMapComparator>
          index_data_;
    };
  };

  // Minimal IndexFactory whose builder opts into the parallel-compression
  // protocol (SupportsParallelAddEntry == true). Used to verify that the
  // table builder's parallel pipeline correctly invokes PrepareAddEntry +
  // FinishAddEntry on custom builders. Counters live on the factory so the
  // test can assert which path was taken.
  //
  // The factory's NewReader returns a stub reader so SST open succeeds.
  // Tests using this factory should set ReadOptions::read_index =
  // kBuiltin so reads go through the standard index -- the stub iterator
  // has no usable behavior.
  class ParallelAwareTestFactory : public IndexFactory {
   public:
    using IndexFactory::NewBuilder;
    using IndexFactory::NewReader;

    mutable std::atomic<int> prepare_calls{0};
    mutable std::atomic<int> finish_calls{0};
    mutable std::atomic<int> add_index_entry_calls{0};

    const char* Name() const override { return "parallel_aware_test_index"; }

    Status NewBuilder(
        const IndexFactoryOptions& /*opts*/,
        std::unique_ptr<IndexFactoryBuilder>& builder) const override {
      builder = std::make_unique<Builder>(this);
      return Status::OK();
    }
    Status NewReader(
        const IndexFactoryOptions&, Slice&,
        std::unique_ptr<IndexFactoryReader>& reader) const override {
      reader = std::make_unique<StubReader>();
      return Status::OK();
    }

   private:
    struct Prepared : public IndexFactoryBuilder::PreparedAddEntry {
      // Captures what the emit thread saw, then committed by the worker.
      std::string last_user_key;
      std::string next_user_key;
      bool has_next = false;
    };

    // Stub reader: SST open requires a non-null reader from NewReader.
    // Tests using this factory route reads via read_index = kBuiltin so
    // the iterator produced here is never actually invoked.
    class StubReader : public IndexFactoryReader {
     public:
      std::unique_ptr<IndexFactoryIterator> NewIterator(
          const ReadOptions&) override {
        return nullptr;
      }
      size_t ApproximateMemoryUsage() const override { return 0; }
    };

    class Builder : public IndexFactoryBuilder {
     public:
      explicit Builder(const ParallelAwareTestFactory* f) : factory_(f) {}

      bool SupportsParallelAddEntry() const override { return true; }

      std::unique_ptr<PreparedAddEntry> CreatePreparedAddEntry() override {
        return std::make_unique<Prepared>();
      }

      void PrepareAddEntry(const Slice& last_key, const Slice* next_key,
                           const IndexEntryContext& /*ctx*/,
                           PreparedAddEntry* out) override {
        auto* p = static_cast<Prepared*>(out);
        p->last_user_key = last_key.ToString();
        p->has_next = next_key != nullptr;
        if (p->has_next) {
          p->next_user_key = next_key->ToString();
        }
        factory_->prepare_calls.fetch_add(1, std::memory_order_relaxed);
      }

      void FinishAddEntry(const BlockHandle& handle, PreparedAddEntry* entry,
                          std::string* /*scratch*/,
                          bool /*skip_delta_encoding*/) override {
        auto* p = static_cast<Prepared*>(entry);
        // Append a fixed-size record per entry so Finish() returns a
        // non-empty block (the meta block is only written when contents
        // are non-empty).
        PutFixed64(&serialized_, handle.offset);
        PutFixed64(&serialized_, handle.size);
        PutFixed32(&serialized_,
                   static_cast<uint32_t>(p->last_user_key.size()));
        factory_->finish_calls.fetch_add(1, std::memory_order_relaxed);
      }

      Slice AddIndexEntry(const Slice& last_key, const Slice* /*next_key*/,
                          const BlockHandle& handle, std::string* /*scratch*/,
                          const IndexEntryContext& /*ctx*/) override {
        // Synchronous path: only used when the table builder didn't take
        // the parallel pipeline. Append a record so Finish() output is
        // still consistent.
        PutFixed64(&serialized_, handle.offset);
        PutFixed64(&serialized_, handle.size);
        PutFixed32(&serialized_, static_cast<uint32_t>(last_key.size()));
        factory_->add_index_entry_calls.fetch_add(1, std::memory_order_relaxed);
        return last_key;
      }

      void OnKeyAdded(const Slice&, ValueType, const Slice&) override {}

      Status Finish(Slice* out) override {
        *out = Slice(serialized_);
        return Status::OK();
      }

      uint64_t EstimatedSize() const override { return serialized_.size(); }

     private:
      const ParallelAwareTestFactory* factory_;
      std::string serialized_;
    };
  };

 protected:
  std::vector<std::pair<std::string, std::string>> generateKVWithValue(
      int key_count, const std::string& value) {
    std::vector<std::pair<std::string, std::string>> kvs(key_count);
    for (int i = 0; i < key_count; i++) {
      std::stringstream ss;
      ss << std::setw(2) << std::setfill('0') << i;
      std::string key = "key" + ss.str();
      kvs[i] = std::make_pair(key, value);
    }
    if (is_reverse_comparator_) {
      std::reverse(kvs.begin(), kvs.end());
    }
    return kvs;
  }

  std::vector<std::pair<std::string, std::string>> generateKVs(
      int key_count, int value_size = 0) {
    std::vector<std::pair<std::string, std::string>> kvs(key_count);
    for (int i = 0; i < key_count; i++) {
      std::stringstream ss;
      ss << std::setw(2) << std::setfill('0') << i;
      std::string key = "key" + ss.str();
      std::string value;
      if (value_size != 0) {
        value = rnd.RandomString(1024);
      } else {
        value = "value" + ss.str();
      }
      kvs[i] = std::make_pair(key, value);
    }
    if (is_reverse_comparator_) {
      std::reverse(kvs.begin(), kvs.end());
    }
    return kvs;
  }

  void BasicTest(bool use_partitioned_index);

  void ValidateMultiScan(
      std::vector<std::tuple<std::vector<std::string>, int, int>>
          scan_opt_validation_arg,
      std::unordered_map<std::string, std::string> property_bag,
      const ReadOptions& ro, MultiScanArgs& scan_opts,
      std::vector<int>& key_counts, std::unique_ptr<DB>& db,
      ColumnFamilyHandle* cfh) {
    key_counts.clear();
    (*scan_opts).clear();

    if (is_reverse_comparator_) {
      for (auto& scan_opt_validation_range : scan_opt_validation_arg) {
        // reverse each range
        std::reverse(std::get<0>(scan_opt_validation_range).begin(),
                     std::get<0>(scan_opt_validation_range).end());
      }
      // reverse all the ranges
      std::reverse(scan_opt_validation_arg.begin(),
                   scan_opt_validation_arg.end());
    }

    for (auto& scan_opt_validation_range : scan_opt_validation_arg) {
      scan_opts.insert(std::get<0>(scan_opt_validation_range)[0],
                       std::get<0>(scan_opt_validation_range)[1],
                       std::optional(property_bag));
      if (is_reverse_comparator_) {
        key_counts.push_back(std::get<2>(scan_opt_validation_range));
      } else {
        key_counts.push_back(std::get<1>(scan_opt_validation_range));
      }
    }

    Slice ub;
    ReadOptions read_opts = ro;
    int key_count = 0;
    int index = 0;
    auto opts = scan_opts.GetScanRanges();
    read_opts.iterate_upper_bound = &ub;
    std::unique_ptr<Iterator> iter(db->NewIterator(read_opts, cfh));
    iter->Prepare(scan_opts);
    for (auto opt : opts) {
      ub = opt.range.limit.value();
      iter->Seek(opt.range.start.value());
      if (kVerbose) {
        printf("range start key %s, end key %s\n",
               opt.range.start.value().ToString().c_str(),
               opt.range.limit.value().ToString().c_str());
      }
      EXPECT_OK(iter->status());
      while (iter->Valid()) {
        if (kVerbose) {
          printf("found key %s\n", iter->key().ToString().c_str());
        }
        key_count++;
        iter->Next();
      }
      EXPECT_EQ(key_count, key_counts[index]);
      key_count = 0;
      index++;
    }
    EXPECT_OK(iter->status());
  }
  Options options_;
  const Comparator* comparator_;
  bool is_reverse_comparator_;
  Random rnd{301};
};

class UserDefinedIndexTest
    : public UserDefinedIndexTestBase,
      public testing::WithParamInterface<const Comparator*> {
  void SetUp() override {
    comparator_ = GetParam();
    options_.comparator = comparator_;
    is_reverse_comparator_ = comparator_ == ReverseBytewiseComparator();
  }
};

void UserDefinedIndexTestBase::BasicTest(bool use_partitioned_index) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  if (use_partitioned_index) {
    table_options.partition_filters = true;
    table_options.decouple_partitioned_filters = true;
    table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  }
  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));

  auto kvs = generateKVs(/*key_count*/ 100);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  ImmutableOptions ioptions(options_);
  MutableCFOptions moptions((ColumnFamilyOptions(options_)));
  EnvOptions eoptions(options_);
  TableReaderOptions toptions(
      ioptions, moptions.prefix_extractor,
      /*_compression_manager=*/nullptr, eoptions, ioptions.internal_comparator,
      moptions.block_protection_bytes_per_key,
      /*skip_filters*/ false, /*immortal*/ false,
      /*force_direct_prefetch*/ false, /*level*/ -1,
      /*block_cache_tracer*/ nullptr,
      /*max_file_size_for_l0_meta_pin*/ 0, /*cur_db_session_id*/ "",
      /*cur_file_num*/ 0,
      /* unique_id */ {}, /* largest_seqno */ 0,
      /* tail_size */ 0, ioptions.persist_user_defined_timestamps);
  // Verify that the user-defined index was created
  std::string meta_block_name =
      std::string(kIndexFactoryMetaPrefix) + "test_index";
  BlockHandle block_handle;
  uint64_t file_size = 0;
  std::unique_ptr<FSRandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  const auto& fs = options_.env->GetFileSystem();
  ASSERT_OK(fs->GetFileSize(ingest_file, IOOptions(), &file_size, nullptr));
  ASSERT_OK(fs->NewRandomAccessFile(ingest_file, eoptions, &file, nullptr));
  file_reader.reset(new RandomAccessFileReader(std::move(file), ingest_file));
  ASSERT_OK(FindMetaBlockInFile(file_reader.get(), file_size,
                                kBlockBasedTableMagicNumber, ioptions,
                                ReadOptions(), meta_block_name, &block_handle));
  file_reader.reset();
  // With our custom flush policy that flushes every 3 keys,
  // we expect around 34 data blocks (100/3 rounded up)
  // Verify the number of entries in the user-defined index
  // Each data block should have an entry in the index
  // With our flush policy of 3 keys per block, we expect around 34 entries
  int expected_entries = (100 + 2) / 3;  // Ceiling of 100/3
  ASSERT_GE(block_handle.size(),
            expected_entries);  // At least this many entries

  std::unique_ptr<SstFileReader> reader(new SstFileReader(options_));
  ASSERT_OK(reader->Open(ingest_file));

  ReadOptions ro;
  std::unique_ptr<Iterator> iter(reader->NewIterator(ro));
  ASSERT_NE(iter, nullptr);

  // Test that we can read all the keys
  int key_count = 0;
  for (iter->SeekToFirst(); iter->Valid() && iter->status().ok();
       iter->Next()) {
    key_count++;
  }
  ASSERT_EQ(key_count, 100);  // We added 100 keys
  ASSERT_OK(iter->status());
  iter.reset();

  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  iter.reset(reader->NewIterator(ro));
  ASSERT_NE(iter, nullptr);

  // Test seek specific key
  key_count = 0;
  for (iter->Seek("key40"); iter->Valid(); iter->Next()) {
    key_count++;
  }
  ASSERT_EQ(key_count, is_reverse_comparator_ ? 41 : 60);
  ASSERT_OK(iter->status());

  // Test upper bound
  Slice ub(is_reverse_comparator_ ? "key25" : "key75");
  ro.iterate_upper_bound = &ub;
  iter.reset(reader->NewIterator(ro));
  ASSERT_NE(iter, nullptr);

  // Test seek specific key with upper bound
  key_count = 0;
  for (iter->Seek("key40"); iter->Valid(); iter->Next()) {
    key_count++;
  }
  ASSERT_EQ(key_count, is_reverse_comparator_ ? 15 : 35);
  ASSERT_OK(iter->status());

  user_defined_index_factory->seek_error_count_ = 1;
  iter.reset(reader->NewIterator(ro));
  ASSERT_NE(iter, nullptr);
  iter->Seek("key40");
  ASSERT_NOK(iter->status());

  user_defined_index_factory->seek_error_count_ = 0;
  user_defined_index_factory->next_error_count_ = 1;
  iter.reset(reader->NewIterator(ro));
  ASSERT_NE(iter, nullptr);
  iter->Seek(is_reverse_comparator_ ? "key92" : "key09");
  ASSERT_OK(iter->status());
  iter->Next();
  ASSERT_OK(iter->status());
  iter->Next();
  if (!is_reverse_comparator_) {
    ASSERT_OK(iter->status());
    iter->Next();
  }
  ASSERT_NOK(iter->status());
  user_defined_index_factory->next_error_count_ = 0;

  ro.iterate_upper_bound = &ub;
  iter.reset(reader->NewIterator(ro));
  ASSERT_NE(iter, nullptr);
  MultiScanArgs scan_opts(comparator_);

  std::unordered_map<std::string, std::string> property_bag;
  property_bag["count"] = std::to_string(25);
  std::vector<std::string> boundaries = {"key10", "key50"};
  if (is_reverse_comparator_) {
    std::reverse(boundaries.begin(), boundaries.end());
  }

  scan_opts.insert(boundaries[0], boundaries[1], std::optional(property_bag));
  iter->Prepare(scan_opts);
  // Test that UDI is used to help fetch the number of keys
  key_count = 0;
  ub = boundaries[1];
  for (iter->Seek(scan_opts.GetScanRanges()[0].range.start.value());
       iter->Valid(); iter->Next()) {
    key_count++;
  }
  // The index may undercount by 2 blocks
  ASSERT_EQ(key_count, 29);
  ASSERT_OK(iter->status());
}

TEST_P(UserDefinedIndexTest, BasicTestWithPartitionedIndex) {
  BasicTest(/*use_partitioned_index=*/true);
}

TEST_P(UserDefinedIndexTest, BasicTestWithoutPartitionedIndex) {
  BasicTest(/*use_partitioned_index=*/false);
}

TEST_P(UserDefinedIndexTest, NewBuilderOkWithNullBuilderFailsSstBuild) {
  class NullBuilderFactory : public IndexFactory {
   public:
    using IndexFactory::NewBuilder;
    using IndexFactory::NewReader;

    const char* Name() const override { return "null_builder_test_index"; }

    Status NewBuilder(
        const IndexFactoryOptions&,
        std::unique_ptr<IndexFactoryBuilder>& builder) const override {
      builder.reset();
      return Status::OK();
    }

    Status NewReader(
        const IndexFactoryOptions&, Slice&,
        std::unique_ptr<IndexFactoryReader>& reader) const override {
      reader.reset();
      return Status::OK();
    }
  };

  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory =
      std::make_shared<NullBuilderFactory>();
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::string dbname = test::PerThreadDBPath("udi_null_builder_test");
  std::string ingest_file = dbname + ".sst";
  SstFileWriter writer(EnvOptions(), options_);
  ASSERT_OK(writer.Open(ingest_file));
  Status s = writer.Put("key", "value");
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_NE(s.ToString().find("null_builder_test_index"), std::string::npos)
      << s.ToString();
}

TEST_P(UserDefinedIndexTest, EmptyCustomIndexUsesStandardFallback) {
  class EmptyIndexFactory : public IndexFactory {
   public:
    using IndexFactory::NewBuilder;
    using IndexFactory::NewReader;

    const char* Name() const override { return "empty_test_index"; }

    Status NewBuilder(
        const IndexFactoryOptions&,
        std::unique_ptr<IndexFactoryBuilder>& builder) const override {
      builder = std::make_unique<Builder>();
      return Status::OK();
    }

    Status NewReader(
        const IndexFactoryOptions&, Slice&,
        std::unique_ptr<IndexFactoryReader>& reader) const override {
      reader.reset();
      return Status::OK();
    }

   private:
    class Builder : public IndexFactoryBuilder {
     public:
      Slice AddIndexEntry(const Slice& last_key, const Slice*,
                          const BlockHandle&, std::string*,
                          const IndexEntryContext&) override {
        return last_key;
      }

      void OnKeyAdded(const Slice&, ValueType, const Slice&) override {}

      Status Finish(Slice* index_contents) override {
        *index_contents = Slice();
        return Status::OK();
      }

      uint64_t EstimatedSize() const override { return 0; }
    };
  };

  for (auto mode : {BlockBasedTableOptions::IndexMode::kStandardDefault,
                    BlockBasedTableOptions::IndexMode::kStandardRequired,
                    BlockBasedTableOptions::IndexMode::kCustomDefault,
                    BlockBasedTableOptions::IndexMode::kCustomOnly}) {
    SCOPED_TRACE(static_cast<int>(mode));
    BlockBasedTableOptions table_options;
    table_options.user_defined_index_factory =
        std::make_shared<EmptyIndexFactory>();
    table_options.index_mode = mode;
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

    std::string ingest_file = test::PerThreadDBPath("udi_empty_output_test") +
                              std::to_string(static_cast<int>(mode)) + ".sst";
    SstFileWriter writer(EnvOptions(), options_);
    ASSERT_OK(writer.Open(ingest_file));
    ASSERT_OK(writer.Put("key", "value"));
    Status s = writer.Finish();
    if (mode == BlockBasedTableOptions::IndexMode::kCustomOnly) {
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      ASSERT_NE(s.ToString().find("returned an empty index"), std::string::npos)
          << s.ToString();
      continue;
    }
    ASSERT_OK(s);
    {
      SstFileReader reader(options_);
      ASSERT_OK(reader.Open(ingest_file));
      for (auto selection : {ReadOptions::ReadIndex::kDefault,
                             ReadOptions::ReadIndex::kPreferCustom}) {
        ReadOptions ro;
        ro.read_index = selection;
        std::string value;
        ASSERT_OK(reader.Get(ro, "key", &value));
        ASSERT_EQ(value, "value");
        std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key(), "key");
        iter->Next();
        ASSERT_FALSE(iter->Valid());
        ASSERT_OK(iter->status());
      }
    }
    ASSERT_OK(options_.env->DeleteFile(ingest_file));

    // Flush a memtable containing only a range tombstone, then compact it
    // with point data. Both operations must preserve the standard fallback.
    Options db_options = options_;
    db_options.create_if_missing = true;
    db_options.disable_auto_compactions = true;
    const std::string db_path = ingest_file + ".db";
    ASSERT_OK(DestroyDB(db_path, db_options));
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(db_options, db_path, &db));
    ASSERT_OK(db->Put(WriteOptions(), "key09", "deleted"));
    ASSERT_OK(db->Put(WriteOptions(), "outside", "survives"));
    FlushOptions flush;
    flush.wait = true;
    ASSERT_OK(db->Flush(flush));
    ASSERT_OK(db->DeleteRange(WriteOptions(), db->DefaultColumnFamily(),
                              is_reverse_comparator_ ? "keyz" : "key",
                              is_reverse_comparator_ ? "key" : "keyz"));
    ASSERT_OK(db->Flush(flush));
    for (int stage : {0, 1, 2}) {
      db.reset();
      ASSERT_OK(DB::Open(db_options, db_path, &db));
      if (stage == 1) {
        ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      }
      std::string value;
      ASSERT_TRUE(db->Get(ReadOptions(), "key09", &value).IsNotFound());
      ASSERT_OK(db->Get(ReadOptions(), "outside", &value));
      ASSERT_EQ(value, "survives");
      std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "outside");
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
    db.reset();
    ASSERT_OK(DestroyDB(db_path, db_options));
  }
}

TEST_P(UserDefinedIndexTest, CustomOnlyRequiresCheckedFooterFormat) {
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory =
      std::make_shared<TestIndexFactory>();
  table_options.index_mode = BlockBasedTableOptions::IndexMode::kCustomOnly;
  table_options.format_version = 5;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = options_.table_factory->ValidateOptions(
      DBOptions(options_), ColumnFamilyOptions(options_));
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_NE(s.ToString().find("format_version >= 6"), std::string::npos)
      << s.ToString();
}

TEST_P(UserDefinedIndexTest, FooterRequirementForcesCustomReader) {
  BlockBasedTableOptions table_options;
  table_options.index_mode = BlockBasedTableOptions::IndexMode::kStandardOnly;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteFooter:IncompatibleFeatures",
      [](void* arg) {
        *static_cast<uint64_t*>(arg) |= kFooterFeatureRequireUserDefinedIndex;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string ingest_file =
      test::PerThreadDBPath("udi_footer_requirement_test") + ".sst";
  Status write_status;
  {
    SstFileWriter writer(EnvOptions(), options_);
    write_status = writer.Open(ingest_file);
    if (write_status.ok()) {
      write_status = writer.Put("key", "value");
    }
    if (write_status.ok()) {
      write_status = writer.Finish();
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(write_status);

  std::unique_ptr<SstFileReader> reader(new SstFileReader(options_));
  Status s = reader->Open(ingest_file);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.user_defined_index_factory =
      std::make_shared<TestIndexFactory>();
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  reader = std::make_unique<SstFileReader>(options_);
  s = reader->Open(ingest_file);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_P(UserDefinedIndexTest, ReopenReusesCachedCustomIndex) {
  class CustomIndexReadTrackingFileSystem : public FileSystemWrapper {
   public:
    explicit CustomIndexReadTrackingFileSystem(
        const std::shared_ptr<FileSystem>& base)
        : FileSystemWrapper(base) {}

    const char* Name() const override {
      return "CustomIndexReadTrackingFileSystem";
    }

    uint64_t custom_offset = UINT64_MAX;
    std::atomic<uint64_t> custom_reads{0};

    IOStatus NewRandomAccessFile(const std::string& path,
                                 const FileOptions& opts,
                                 std::unique_ptr<FSRandomAccessFile>* result,
                                 IODebugContext* dbg) override {
      IOStatus s = target()->NewRandomAccessFile(path, opts, result, dbg);
      if (s.ok()) {
        result->reset(new TrackingFile(std::move(*result), this));
      }
      return s;
    }

   private:
    class TrackingFile : public FSRandomAccessFileOwnerWrapper {
     public:
      TrackingFile(std::unique_ptr<FSRandomAccessFile>&& file,
                   CustomIndexReadTrackingFileSystem* fs)
          : FSRandomAccessFileOwnerWrapper(std::move(file)), fs_(fs) {}

      IOStatus Read(uint64_t offset, size_t n, const IOOptions& opts,
                    Slice* result, char* scratch,
                    IODebugContext* dbg) const override {
        if (offset == fs_->custom_offset) {
          fs_->custom_reads.fetch_add(1);
        }
        return target()->Read(offset, n, opts, result, scratch, dbg);
      }

      IOStatus Prefetch(uint64_t, size_t, const IOOptions&,
                        IODebugContext*) override {
        // Prefetch is advisory. Accept it without reading so tail prefetching
        // cannot hide which metadata block the SST reader actually requests.
        return IOStatus::OK();
      }

     private:
      CustomIndexReadTrackingFileSystem* fs_;
    };
  };

  using IndexMode = BlockBasedTableOptions::IndexMode;
  const std::string sst_path =
      test::PerThreadDBPath("udi_reopen_cache") + ".sst";
  const auto& kvs = generateKVs(/*key_count=*/30);
  for (IndexMode mode : {IndexMode::kStandardDefault, IndexMode::kCustomDefault,
                         IndexMode::kCustomOnly}) {
    for (bool cache_indexes : {false, true}) {
      SCOPED_TRACE(testing::Message() << "mode=" << static_cast<int>(mode)
                                      << " cache_indexes=" << cache_indexes);
      BlockBasedTableOptions table_options;
      table_options.block_cache = NewLRUCache(1 << 20);
      table_options.cache_index_and_filter_blocks = cache_indexes;
      table_options.user_defined_index_factory =
          std::make_shared<TestIndexFactory>();
      table_options.index_mode = mode;
      table_options.flush_block_policy_factory =
          std::make_shared<CustomFlushBlockPolicyFactory>();
      auto fs = std::make_shared<CustomIndexReadTrackingFileSystem>(
          options_.env->GetFileSystem());
      std::unique_ptr<Env> env = NewCompositeEnv(fs);
      Options options = options_;
      options.env = env.get();
      options.compression = kNoCompression;
      options.statistics = CreateDBStatistics();
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      {
        SstFileWriter writer(EnvOptions(), options);
        ASSERT_OK(writer.Open(sst_path));
        for (const auto& [key, value] : kvs) {
          ASSERT_OK(writer.Put(key, value));
        }
        ASSERT_OK(writer.Finish());
      }

      BlockHandle custom_handle;
      uint64_t file_size;
      ASSERT_OK(fs->GetFileSize(sst_path, IOOptions(), &file_size, nullptr));
      std::unique_ptr<FSRandomAccessFile> file;
      ASSERT_OK(fs->NewRandomAccessFile(sst_path, EnvOptions(options), &file,
                                        nullptr));
      {
        RandomAccessFileReader file_reader(std::move(file), sst_path);
        ASSERT_OK(FindMetaBlockInFile(
            &file_reader, file_size, kBlockBasedTableMagicNumber,
            ImmutableOptions(options), ReadOptions(),
            "rocksdb.user_defined_index.test_index", &custom_handle));
      }
      ASSERT_GT(custom_handle.size(), 0U);
      fs->custom_offset = custom_handle.offset();

      auto open_and_scan = [&](bool expect_cached) {
        fs->custom_reads.store(0);
        ASSERT_OK(options.statistics->Reset());
        get_perf_context()->Reset();
        SstFileReader reader(options);
        ASSERT_OK(reader.Open(sst_path));
        // UDI reads used to count as metadata reads. Measure the block itself
        // so this test also covers branches with the older PerfContext
        // grouping.
        EXPECT_EQ(fs->custom_reads.load(), expect_cached ? 0U : 1U);
        if (expect_cached) {
          EXPECT_EQ(get_perf_context()->index_block_read_count, 0);
          EXPECT_EQ(get_perf_context()->index_block_read_byte, 0);
          EXPECT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_INDEX_ADD),
                    0);
          EXPECT_EQ(options.statistics->getTickerCount(
                        BLOCK_CACHE_INDEX_ADD_REDUNDANT),
                    0);
          EXPECT_GE(options.statistics->getTickerCount(BLOCK_CACHE_INDEX_HIT),
                    1);
        } else {
          EXPECT_GT(get_perf_context()->index_block_read_count, 0);
        }
        ReadOptions ro;
        ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
        std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
        iter->SeekToFirst();
        for (const auto& [key, value] : kvs) {
          ASSERT_TRUE(iter->Valid());
          EXPECT_EQ(iter->key(), key);
          EXPECT_EQ(iter->value(), value);
          iter->Next();
        }
        EXPECT_FALSE(iter->Valid());
        ASSERT_OK(iter->status());
      };
      open_and_scan(/*expect_cached=*/false);
      open_and_scan(/*expect_cached=*/cache_indexes);
      open_and_scan(/*expect_cached=*/cache_indexes);
      table_options.block_cache->EraseUnRefEntries();
      open_and_scan(/*expect_cached=*/false);
      ASSERT_OK(options.env->DeleteFile(sst_path));
    }
  }
}

TEST_P(UserDefinedIndexTest, LegacyOptionsFilePreservesIndexMode) {
  using IndexMode = BlockBasedTableOptions::IndexMode;
  const struct {
    const char* legacy_options;
    IndexMode expected;
  } cases[] = {
      {"use_udi_as_primary_index=true\n", IndexMode::kCustomDefault},
      {"fail_if_no_udi_on_open=true\n", IndexMode::kStandardRequired},
      {"use_udi_as_primary_index=true\nfail_if_no_udi_on_open=true\n",
       IndexMode::kCustomDefault},
      {"skip_standard_index=true\n", IndexMode::kCustomOnly},
      {"use_udi_as_primary_index=false\nfail_if_no_udi_on_open=false\n",
       IndexMode::kStandardDefault}};
  Env* env = options_.env;
  const std::string options_path = test::PerThreadDBPath("udi_legacy_options");
  const std::string sst_path = options_path + ".sst";
  {
    SstFileWriter writer(EnvOptions(), options_);
    ASSERT_OK(writer.Open(sst_path));
    ASSERT_OK(writer.Put("key00", "value"));
    ASSERT_OK(writer.Finish());
  }

  ConfigOptions cfg;
  cfg.env = env;
  cfg.ignore_unknown_options = false;
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.legacy_options);
    for (bool mode_first : {false, true}) {
      SCOPED_TRACE(testing::Message() << "mode_first=" << mode_first);
      const std::string mode = "index_mode=kStandardDefault\n";
      const std::string legacy = test_case.legacy_options;
      const std::string contents =
          "[Version]\nrocksdb_version=11.0.0\noptions_file_version=1.1\n"
          "[DBOptions]\n[CFOptions \"default\"]\n"
          "table_factory=BlockBasedTable\n"
          "[TableOptions/BlockBasedTable \"default\"]\n" +
          (mode_first ? mode + legacy : legacy + mode);
      ASSERT_OK(WriteStringToFile(env, contents, options_path));
      DBOptions db_options;
      std::vector<ColumnFamilyDescriptor> descriptors;
      ASSERT_OK(
          LoadOptionsFromFile(cfg, options_path, &db_options, &descriptors));
      ASSERT_EQ(descriptors.size(), 1U);
      BlockBasedTableOptions loaded =
          *descriptors[0]
               .options.table_factory->GetOptions<BlockBasedTableOptions>();
      EXPECT_EQ(loaded.GetEffectiveIndexMode(), test_case.expected);

      // The restored mode must retain the missing-custom-index guard, rather
      // than silently serving this older, standard-only SST.
      loaded.user_defined_index_factory = std::make_shared<TestIndexFactory>();
      Options read_options = options_;
      read_options.table_factory.reset(NewBlockBasedTableFactory(loaded));
      SstFileReader reader(read_options);
      Status s = reader.Open(sst_path);
      if (test_case.expected == IndexMode::kStandardDefault) {
        ASSERT_OK(s);
        std::unique_ptr<Iterator> iter(reader.NewIterator(ReadOptions()));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key(), "key00");
        ASSERT_EQ(iter->value(), "value");
        ASSERT_OK(iter->status());
      } else {
        EXPECT_TRUE(s.IsCorruption()) << s.ToString();
      }
    }
  }
  ASSERT_OK(env->DeleteFile(options_path));
  ASSERT_OK(env->DeleteFile(sst_path));
}

TEST_P(UserDefinedIndexTest, StandaloneSstRejectsUnrecognizedIndexMode) {
  const std::string sst_path =
      test::PerThreadDBPath("udi_invalid_index_mode.sst");
  Options options = options_;
  options.compression = kNoCompression;
  {
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(sst_path));
    ASSERT_OK(writer.Put("key00", "value"));
    ASSERT_OK(writer.Finish());
  }

  for (bool with_factory : {false, true}) {
    for (int mode : {-1, 5, 99}) {
      SCOPED_TRACE(testing::Message()
                   << "factory=" << with_factory << " mode=" << mode);
      BlockBasedTableOptions table_options;
      table_options.index_mode =
          static_cast<BlockBasedTableOptions::IndexMode>(mode);
      if (with_factory) {
        table_options.user_defined_index_factory =
            std::make_shared<TestIndexFactory>();
      }
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      Status s = options.table_factory->ValidateOptions(options, options);
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();

      const std::string invalid_path = sst_path + ".invalid";
      {
        SstFileWriter writer(EnvOptions(), options);
        s = writer.Open(invalid_path);
        if (s.ok()) {
          s = writer.Put("key00", "value");
        }
        if (s.ok()) {
          s = writer.Finish();
        }
        EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
        EXPECT_NE(s.ToString().find("Unrecognized index_mode"),
                  std::string::npos);
      }
      ASSERT_OK(options.env->DeleteFile(invalid_path));

      SstFileReader reader(options);
      s = reader.Open(sst_path);
      EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
      EXPECT_NE(s.ToString().find("Unrecognized index_mode"),
                std::string::npos);
    }
  }
  ASSERT_OK(options.env->DeleteFile(sst_path));
}

namespace {

constexpr int kIndexFactoryTestKeyCount = 1000;

std::string IndexFactoryTestKey(int i) {
  return "key" + std::to_string(1000000 + i);
}

Options IndexFactoryTestOptions(std::shared_ptr<IndexFactory> factory,
                                BlockBasedTableOptions::IndexMode mode,
                                bool cache_index) {
  BlockBasedTableOptions table_options;
  table_options.block_size = 512;
  table_options.block_cache = NewLRUCache(16 << 20);
  table_options.cache_index_and_filter_blocks = cache_index;
  table_options.index_mode = mode;
  table_options.user_defined_index_factory = std::move(factory);
  Options options;
  options.compression = kNoCompression;
  options.statistics = CreateDBStatistics();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  return options;
}

void WriteIndexFactoryTestSst(const Options& options, const std::string& path) {
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.Open(path));
  for (int i = 0; i < kIndexFactoryTestKeyCount; ++i) {
    ASSERT_OK(writer.Put(IndexFactoryTestKey(i), std::string(64, 'v')));
  }
  ASSERT_OK(writer.Finish());
}

// Seeks and scans every key through the custom index.
void VerifyIndexFactoryTestSst(SstFileReader* reader) {
  ReadOptions read_options;
  read_options.read_index = ReadOptions::ReadIndex::kPreferCustom;
  std::unique_ptr<Iterator> iter(reader->NewIterator(read_options));
  iter->Seek(IndexFactoryTestKey(kIndexFactoryTestKeyCount / 2));
  ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
  ASSERT_EQ(iter->key(), IndexFactoryTestKey(kIndexFactoryTestKeyCount / 2));
  iter->SeekToFirst();
  for (int i = 0; i < kIndexFactoryTestKeyCount; ++i) {
    ASSERT_TRUE(iter->Valid()) << iter->status().ToString();
    ASSERT_EQ(iter->key(), IndexFactoryTestKey(i));
    ASSERT_EQ(iter->value(), std::string(64, 'v'));
    iter->Next();
  }
  EXPECT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

void OpenAndVerifyIndexFactoryTestSst(const Options& options,
                                      const std::string& path) {
  SstFileReader reader(options);
  ASSERT_OK(reader.Open(path));
  VerifyIndexFactoryTestSst(&reader);
}

// Overwrites the stack below the caller's frame, where a finished table open
// kept its locals.
void OverwriteStack() {
  volatile char buffer[64 << 10];
  for (size_t i = 0; i < sizeof(buffer); ++i) {
    buffer[i] = static_cast<char>(0xa5);
  }
}

// A reader may keep a pointer to the Slice passed to NewReader() and use it
// after table open returns.
class SliceRetainingIndexFactory : public IndexFactory {
 public:
  explicit SliceRetainingIndexFactory(std::shared_ptr<IndexFactory> delegate)
      : delegate_(std::move(delegate)) {}
  const char* Name() const override { return delegate_->Name(); }
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;
  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override {
    return delegate_->NewBuilder(options, builder);
  }
  Status NewReader(const IndexFactoryOptions& options, Slice& contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override {
    Status s = delegate_->NewReader(options, contents, reader);
    if (s.ok()) {
      reader = std::make_unique<Reader>(std::move(reader), &contents);
    }
    return s;
  }

 private:
  class Reader : public IndexFactoryReader {
   public:
    Reader(std::unique_ptr<IndexFactoryReader> delegate, const Slice* contents)
        : delegate_(std::move(delegate)),
          contents_(contents),
          data_(contents->data()),
          size_(contents->size()) {}
    std::unique_ptr<IndexFactoryIterator> NewIterator(
        const ReadOptions& options) override {
      // Compare the fields without reading the bytes; an expired Slice holds
      // arbitrary values.
      if (contents_->data() != data_ || contents_->size() != size_) {
        ADD_FAILURE() << "NewReader() input Slice changed after table open";
        return nullptr;
      }
      return delegate_->NewIterator(options);
    }
    size_t ApproximateMemoryUsage() const override {
      return delegate_->ApproximateMemoryUsage();
    }

   private:
    std::unique_ptr<IndexFactoryReader> delegate_;
    const Slice* const contents_;
    const char* const data_;
    const size_t size_;
  };
  std::shared_ptr<IndexFactory> delegate_;
};

}  // namespace

TEST(IndexFactoryCompatibilityTest, RetainedReaderSliceOutlivesTableOpen) {
  std::shared_ptr<IndexFactory> trie_factory;
  ASSERT_OK(IndexFactory::CreateFromString(ConfigOptions(), "trie_index",
                                           &trie_factory));
  auto factory = std::make_shared<SliceRetainingIndexFactory>(trie_factory);
  const std::string path = test::PerThreadDBPath("udi_retaining_reader.sst");
  // Called through a volatile pointer so it gets its own stack frame.
  void (*volatile overwrite_stack)() = OverwriteStack;
  for (bool cache_index : {false, true}) {
    for (auto mode : {BlockBasedTableOptions::IndexMode::kStandardDefault,
                      BlockBasedTableOptions::IndexMode::kCustomDefault,
                      BlockBasedTableOptions::IndexMode::kCustomOnly}) {
      SCOPED_TRACE(testing::Message()
                   << cache_index << " " << static_cast<int>(mode));
      const Options options =
          IndexFactoryTestOptions(factory, mode, cache_index);
      ASSERT_NO_FATAL_FAILURE(WriteIndexFactoryTestSst(options, path));
      // The second open reuses the cached block when caching is enabled.
      for (int open = 0; open < 2; ++open) {
        SstFileReader reader(options);
        ASSERT_OK(reader.Open(path));
        overwrite_stack();
        ASSERT_NO_FATAL_FAILURE(VerifyIndexFactoryTestSst(&reader));
      }
      if (cache_index) {
        EXPECT_GT(options.statistics->getTickerCount(BLOCK_CACHE_INDEX_HIT),
                  0U);
      }
      ASSERT_OK(options.env->DeleteFile(path));
    }
  }
}

// A legacy reader may consume its Slice, while retaining the backing bytes.
class ConsumingIndexFactory : public IndexFactory {
 public:
  explicit ConsumingIndexFactory(std::shared_ptr<IndexFactory> delegate)
      : delegate_(std::move(delegate)) {}
  const char* Name() const override { return delegate_->Name(); }
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;
  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override {
    return delegate_->NewBuilder(options, builder);
  }
  std::unique_ptr<IndexFactoryReader> NewReader(
      Slice& contents) const override {
    EXPECT_GT(contents.size(), 0U);
    std::unique_ptr<IndexFactoryReader> reader;
    IndexFactoryOptions options;
    options.comparator = BytewiseComparator();
    EXPECT_OK(delegate_->NewReader(options, contents, reader));
    contents.remove_prefix(contents.size());
    if (reject_reader.load()) {
      reader.reset();
    }
    return reader;
  }
  std::atomic<bool> reject_reader{false};

 private:
  std::shared_ptr<IndexFactory> delegate_;
};

TEST(IndexFactoryCompatibilityTest, ConsumingReaderPreservesCachedContents) {
  std::shared_ptr<IndexFactory> trie_factory;
  ASSERT_OK(IndexFactory::CreateFromString(ConfigOptions(), "trie_index",
                                           &trie_factory));
  auto factory = std::make_shared<ConsumingIndexFactory>(trie_factory);
  const std::string path = test::PerThreadDBPath("udi_consuming_reader.sst");
  for (bool cache_index : {false, true}) {
    for (auto mode : {BlockBasedTableOptions::IndexMode::kStandardDefault,
                      BlockBasedTableOptions::IndexMode::kCustomDefault,
                      BlockBasedTableOptions::IndexMode::kCustomOnly}) {
      SCOPED_TRACE(testing::Message()
                   << cache_index << " " << static_cast<int>(mode));
      const Options options =
          IndexFactoryTestOptions(factory, mode, cache_index);
      ASSERT_NO_FATAL_FAILURE(WriteIndexFactoryTestSst(options, path));
      auto open_and_scan = [&] {
        OpenAndVerifyIndexFactoryTestSst(options, path);
      };
      open_and_scan();
      open_and_scan();
      if (cache_index) {
        EXPECT_GT(options.statistics->getTickerCount(BLOCK_CACHE_INDEX_HIT),
                  0U);
      }
      // Each table reader must receive an independent Slice view, including
      // simultaneous opens using the same cached BlockContents.
      std::vector<std::thread> threads;
      for (int i = 0; i < 4; ++i) {
        threads.emplace_back(open_and_scan);
      }
      for (auto& thread : threads) {
        thread.join();
      }
      // Consuming the input before rejecting a reader must not poison the
      // cache for a later successful open, even when standard fallback works.
      factory->reject_reader.store(true);
      {
        SstFileReader reader(options);
        Status s = reader.Open(path);
        if (mode == BlockBasedTableOptions::IndexMode::kStandardDefault) {
          ASSERT_OK(s);
        } else {
          ASSERT_TRUE(s.IsCorruption()) << s.ToString();
        }
      }
      factory->reject_reader.store(false);
      open_and_scan();
      ASSERT_OK(options.env->DeleteFile(path));
    }
  }
}

// Exercise readers that report only their auxiliary allocations, as documented,
// and readers that opt in to including the serialized block in their estimate.
class MemoryReportingIndexFactory : public IndexFactory {
 public:
  MemoryReportingIndexFactory(std::shared_ptr<IndexFactory> delegate,
                              bool includes_index_block)
      : delegate_(std::move(delegate)),
        includes_index_block_(includes_index_block) {}

  const char* Name() const override { return delegate_->Name(); }
  using IndexFactory::NewBuilder;
  using IndexFactory::NewReader;
  Status NewBuilder(
      const IndexFactoryOptions& options,
      std::unique_ptr<IndexFactoryBuilder>& builder) const override {
    return delegate_->NewBuilder(options, builder);
  }
  Status NewReader(const IndexFactoryOptions& options, Slice& contents,
                   std::unique_ptr<IndexFactoryReader>& reader) const override {
    const size_t block_size = contents.size();
    index_block_size.store(block_size);
    Status s = delegate_->NewReader(options, contents, reader);
    if (s.ok()) {
      const size_t extra = extra_bytes.load();
      if (includes_index_block_) {
        reader = std::make_unique<InclusiveReader>(std::move(reader),
                                                   block_size + extra);
      } else {
        reader = std::make_unique<Reader>(std::move(reader), extra);
      }
    }
    return s;
  }

  // Added to the estimate of readers created afterwards.
  std::atomic<size_t> extra_bytes{0};
  // Size of the serialized block most recently passed to NewReader().
  mutable std::atomic<size_t> index_block_size{0};

 private:
  // Implements only the reader methods that predate
  // MemoryUsageIncludesIndexBlock().
  class Reader : public IndexFactoryReader {
   public:
    Reader(std::unique_ptr<IndexFactoryReader> delegate, size_t extra)
        : delegate_(std::move(delegate)), extra_(extra) {}
    std::unique_ptr<IndexFactoryIterator> NewIterator(
        const ReadOptions& options) override {
      return delegate_->NewIterator(options);
    }
    size_t ApproximateMemoryUsage() const override {
      return delegate_->ApproximateMemoryUsage() + extra_;
    }

   private:
    std::unique_ptr<IndexFactoryReader> delegate_;
    const size_t extra_;
  };
  class InclusiveReader : public Reader {
   public:
    using Reader::Reader;
    bool MemoryUsageIncludesIndexBlock() const override { return true; }
  };
  std::shared_ptr<IndexFactory> delegate_;
  const bool includes_index_block_;
};

namespace {

// Separate allocations can have different usable sizes (e.g. with Folly).
// This allowance is much smaller than the serialized index in these tests, so
// missing or duplicating a charge still fails their assertions.
constexpr size_t kIndexMemoryAllocatorAllowance = 1024;

size_t RoundUpToTableReaderReservation(size_t bytes) {
  const size_t unit = CacheReservationManagerImpl<
      CacheEntryRole::kBlockBasedTableReader>::GetDummyEntrySize();
  return (bytes + unit - 1) / unit * unit;
}

std::shared_ptr<Cache> NewStrictTableReaderCache(size_t capacity) {
  LRUCacheOptions cache_options;
  cache_options.capacity = capacity;
  cache_options.num_shard_bits = 0;
  cache_options.strict_capacity_limit = true;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;
  return cache_options.MakeSharedCache();
}

// Builds one SST with a trie custom index, then reopens it under modified
// table_options to compare memory estimates.
class CustomIndexMemoryTable {
 public:
  explicit CustomIndexMemoryTable(std::shared_ptr<IndexFactory> factory)
      : internal_comparator_(BytewiseComparator()),
        table_(BytewiseComparator(), /*convert_to_internal_key=*/true) {
    table_options.cache_index_and_filter_blocks = false;
    table_options.block_size = 512;
    table_options.user_defined_index_factory = std::move(factory);
    options_.compression = kNoCompression;
    UpdateOptions();
    Random random(301);
    for (int i = 0; i < 16384; ++i) {
      table_.Add(test::RandomKey(&random, 512), "value");
    }
    stl_wrappers::KVMap kvmap;
    table_.Finish(options_, *immutable_options_, *mutable_options_,
                  table_options, internal_comparator_, &keys_, &kvmap);
  }

  // Closes the current reader before replacing the options it references.
  Status Reopen() {
    Close();
    UpdateOptions();
    return table_.Reopen(*immutable_options_, *mutable_options_);
  }

  void Close() { table_.ResetTableReader(); }

  size_t MemoryUsage() {
    return table_.GetTableReader()->ApproximateMemoryUsage();
  }

  void VerifyScan() {
    ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.read_index = ReadOptions::ReadIndex::kPreferCustom;
    std::unique_ptr<InternalIterator> iter(
        new KeyConvertingIterator(table_.GetTableReader()->NewIterator(
            read_options, nullptr, nullptr, false,
            TableReaderCaller::kUncategorized)));
    size_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_LT(count, keys_.size());
      EXPECT_EQ(iter->key(), keys_[count]);
      EXPECT_EQ(iter->value(), "value");
      ++count;
    }
    ASSERT_OK(iter->status());
    EXPECT_EQ(count, keys_.size());
  }

  BlockBasedTableOptions table_options;

 private:
  void UpdateOptions() {
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    immutable_options_ = std::make_unique<ImmutableOptions>(options_);
    mutable_options_ = std::make_unique<MutableCFOptions>(options_);
  }

  Options options_;
  const InternalKeyComparator internal_comparator_;
  std::unique_ptr<ImmutableOptions> immutable_options_;
  std::unique_ptr<MutableCFOptions> mutable_options_;
  // Declared after the options so its reader is destroyed first.
  TableConstructor table_;
  std::vector<std::string> keys_;
};

}  // namespace

TEST(IndexFactoryCompatibilityTest, LegacyReaderMemoryWithStrictCache) {
  std::shared_ptr<IndexFactory> trie_factory;
  ASSERT_OK(IndexFactory::CreateFromString(ConfigOptions(), "trie_index",
                                           &trie_factory));
  CustomIndexMemoryTable table(
      std::make_shared<MemoryReportingIndexFactory>(trie_factory, false));
  const size_t memory = table.MemoryUsage();
  table.Close();

  table.table_options.block_cache = NewStrictTableReaderCache(
      RoundUpToTableReaderReservation(memory + kIndexMemoryAllocatorAllowance));
  table.table_options.cache_usage_options
      .options_overrides[CacheEntryRole::kBlockBasedTableReader]
      .charged = CacheEntryRoleOptions::Decision::kEnabled;
  for (bool includes_index_block : {false, true}) {
    SCOPED_TRACE(includes_index_block);
    table.table_options.user_defined_index_factory =
        std::make_shared<MemoryReportingIndexFactory>(trie_factory,
                                                      includes_index_block);
    ASSERT_OK(table.Reopen());
    const size_t usage = table.MemoryUsage();
    EXPECT_LE(usage, memory + kIndexMemoryAllocatorAllowance);
    EXPECT_LE(memory, usage + kIndexMemoryAllocatorAllowance);
    // An SST that fits the cache must be readable under either convention.
    ASSERT_NO_FATAL_FAILURE(table.VerifyScan());
    table.Close();
    EXPECT_EQ(table.table_options.block_cache->GetUsage(), 0U);
  }

  // Cached raw bytes are charged to the block cache rather than the reader.
  table.table_options.cache_index_and_filter_blocks = true;
  table.table_options.block_cache = NewLRUCache(16 << 20);
  table.table_options.cache_usage_options.options_overrides.clear();
  size_t cached_memory = 0;
  for (bool includes_index_block : {false, true}) {
    SCOPED_TRACE(includes_index_block);
    table.table_options.user_defined_index_factory =
        std::make_shared<MemoryReportingIndexFactory>(trie_factory,
                                                      includes_index_block);
    ASSERT_OK(table.Reopen());
    const size_t usage = table.MemoryUsage();
    if (!includes_index_block) {
      cached_memory = usage;
    }
    EXPECT_LE(usage, cached_memory + kIndexMemoryAllocatorAllowance);
    EXPECT_LE(cached_memory, usage + kIndexMemoryAllocatorAllowance);
    EXPECT_LT(usage + kIndexMemoryAllocatorAllowance, memory);
    table.Close();
  }
}

// A reader that only implements ApproximateMemoryUsage() reports auxiliary
// allocations, as documented. All of that allocation must reach the table
// reader's estimate and its cache reservation, whether it is smaller or larger
// than the serialized block.
TEST(IndexFactoryCompatibilityTest, AuxiliaryReaderMemoryIsFullyCharged) {
  std::shared_ptr<IndexFactory> trie_factory;
  ASSERT_OK(IndexFactory::CreateFromString(ConfigOptions(), "trie_index",
                                           &trie_factory));
  auto factory = std::make_shared<MemoryReportingIndexFactory>(
      trie_factory, /*includes_index_block=*/false);
  CustomIndexMemoryTable table(factory);
  const size_t index_block_size = factory->index_block_size.load();
  ASSERT_GT(index_block_size, 8 * kIndexMemoryAllocatorAllowance);
  table.Close();
  const size_t auxiliary_sizes[] = {index_block_size / 2, index_block_size * 2};

  for (bool cache_index : {false, true}) {
    SCOPED_TRACE(cache_index);
    table.table_options.cache_index_and_filter_blocks = cache_index;
    table.table_options.block_cache = NewLRUCache(64 << 20);
    factory->extra_bytes.store(0);
    ASSERT_OK(table.Reopen());
    const size_t base = table.MemoryUsage();
    for (size_t auxiliary : auxiliary_sizes) {
      SCOPED_TRACE(auxiliary);
      factory->extra_bytes.store(auxiliary);
      ASSERT_OK(table.Reopen());
      const size_t usage = table.MemoryUsage();
      EXPECT_LE(usage, base + auxiliary + kIndexMemoryAllocatorAllowance);
      EXPECT_LE(base + auxiliary, usage + kIndexMemoryAllocatorAllowance);
      ASSERT_NO_FATAL_FAILURE(table.VerifyScan());
    }
    table.Close();
  }

  // Pad the estimate to end just below a reservation boundary, and size a
  // strict cache to that boundary. Any further charged allocation larger than
  // the allocator allowance must then fail the open.
  table.table_options.cache_index_and_filter_blocks = false;
  factory->extra_bytes.store(0);
  ASSERT_OK(table.Reopen());
  const size_t base = table.MemoryUsage();
  table.Close();
  const size_t capacity = RoundUpToTableReaderReservation(
      base + 2 * kIndexMemoryAllocatorAllowance);
  const size_t padding = capacity - kIndexMemoryAllocatorAllowance - base;
  table.table_options.block_cache = NewStrictTableReaderCache(capacity);
  table.table_options.cache_usage_options
      .options_overrides[CacheEntryRole::kBlockBasedTableReader]
      .charged = CacheEntryRoleOptions::Decision::kEnabled;
  factory->extra_bytes.store(padding);
  ASSERT_OK(table.Reopen());
  table.Close();
  for (size_t auxiliary : auxiliary_sizes) {
    SCOPED_TRACE(auxiliary);
    factory->extra_bytes.store(padding + auxiliary);
    Status s = table.Reopen();
    EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
    table.Close();
    EXPECT_EQ(table.table_options.block_cache->GetUsage(), 0U);
  }
}

TEST_P(UserDefinedIndexTest, RawIndexMemoryIsChargedExactlyOnce) {
  LRUCacheOptions cache_options;
  cache_options.capacity = 1 << 20;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;
  auto block_cache = cache_options.MakeSharedCache();
  auto factory = std::make_shared<TestIndexFactory>();

  BlockBasedTableOptions table_options;
  table_options.block_cache = block_cache;
  table_options.cache_index_and_filter_blocks = false;
  table_options.user_defined_index_factory = factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  Options options = options_;
  options.compression = kNoCompression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::unique_ptr<ImmutableOptions> immutable_options =
      std::make_unique<ImmutableOptions>(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator internal_comparator(options.comparator);

  TableConstructor table(options.comparator,
                         true /* convert_to_internal_key */);
  for (const auto& [key, value] : generateKVs(/*key_count=*/30)) {
    table.Add(key, value);
  }
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  table.Finish(options, *immutable_options, moptions, table_options,
               internal_comparator, &keys, &kvmap);
  const size_t owned_memory = table.GetTableReader()->ApproximateMemoryUsage();
  ASSERT_EQ(block_cache->GetUsage(), 0);

  table.ResetTableReader();
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  immutable_options = std::make_unique<ImmutableOptions>(options);
  MutableCFOptions cached_moptions(options);
  const size_t cache_usage_before = block_cache->GetUsage();
  ASSERT_OK(table.Reopen(*immutable_options, cached_moptions));
  const size_t cached_memory = table.GetTableReader()->ApproximateMemoryUsage();
  const size_t index_cache_charge =
      block_cache->GetUsage() - cache_usage_before;

  ASSERT_GT(index_cache_charge, 0);
  // Block::ApproximateMemoryUsage() adds malloc_usable_size() of the Block
  // object, so the owned and cached allocations can land in different
  // allocator size classes and differ by a few bytes. Compare with a
  // tolerance well under the block size: charging the block twice or not at
  // all would shift the total by roughly index_cache_charge.
  const size_t expected_total = cached_memory + index_cache_charge;
  const size_t accounting_diff = owned_memory > expected_total
                                     ? owned_memory - expected_total
                                     : expected_total - owned_memory;
  EXPECT_LT(accounting_diff, index_cache_charge / 4);
}

// Verifies that kStandardOnly truly ignores user_defined_index_factory:
// even if a factory pointer is set in the table options, the reader must
// not probe for a UDI block. This matches the kStandardOnly contract
// ("only the standard index is built/used") and avoids spurious load
// failures on SSTs that have no UDI block.
TEST_P(UserDefinedIndexTest, StandardOnlyIgnoresUdiFactory) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("udi_standard_only_test");
  std::string ingest_file = dbname + "test.sst";

  // Step 1: write an SST in kStandardOnly mode -> no UDI block on disk.
  table_options.index_mode = BlockBasedTableOptions::IndexMode::kStandardOnly;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer(
      new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));
  auto kvs = generateKVs(/*key_count=*/30);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  // Step 2: re-open the SST with kStandardOnly *and* a UDI factory
  // configured. The factory is irrelevant -- kStandardOnly should ignore it.
  // Without this fix, the reader would probe the (absent) UDI block and
  // tick SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT.
  BlockBasedTableOptions read_table_options;
  read_table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardOnly;
  read_table_options.user_defined_index_factory =
      std::make_shared<TestIndexFactory>();
  options_.table_factory.reset(NewBlockBasedTableFactory(read_table_options));
  options_.statistics = CreateDBStatistics();

  std::unique_ptr<SstFileReader> reader(new SstFileReader(options_));
  ASSERT_OK(reader->Open(ingest_file));

  // No UDI load attempt should have been recorded.
  ASSERT_EQ(options_.statistics->getTickerCount(
                SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT),
            0u);

  // Reads still work via the standard index.
  std::unique_ptr<Iterator> iter(reader->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
}

// A custom index cannot be correct under user-defined timestamps, so both
// writers must refuse the combination. SstFileWriter never reaches
// TableFactory::ValidateOptions, so the table builder has to catch it too.
TEST_P(UserDefinedIndexTest, UserDefinedTimestampsRejectCustomIndex) {
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory =
      std::make_shared<TestIndexFactory>();
  table_options.index_mode = BlockBasedTableOptions::IndexMode::kCustomDefault;
  std::unique_ptr<TableFactory> factory(
      NewBlockBasedTableFactory(table_options));

  ColumnFamilyOptions cf_opts;
  cf_opts.comparator = BytewiseComparatorWithU64Ts();
  Status s = factory->ValidateOptions(DBOptions(), cf_opts);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_NE(s.ToString().find("user-defined timestamps"), std::string::npos)
      << s.ToString();

  // The same table options are fine without timestamps.
  cf_opts.comparator = BytewiseComparator();
  ASSERT_OK(factory->ValidateOptions(DBOptions(), cf_opts));

  // kStandardOnly never builds the custom index, so timestamps stay allowed.
  BlockBasedTableOptions unused_factory_options = table_options;
  unused_factory_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardOnly;
  std::unique_ptr<TableFactory> unused_factory(
      NewBlockBasedTableFactory(unused_factory_options));
  cf_opts.comparator = BytewiseComparatorWithU64Ts();
  ASSERT_OK(unused_factory->ValidateOptions(DBOptions(), cf_opts));

  // SstFileWriter skips ValidateOptions entirely. The error can surface at any
  // of these calls, but the file must never be written successfully.
  Options udt_options = options_;
  udt_options.comparator = BytewiseComparatorWithU64Ts();
  udt_options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  SstFileWriter writer(EnvOptions(), udt_options);
  std::string ts_buf;
  Slice ts = EncodeU64Ts(1, &ts_buf);
  Status writer_status =
      writer.Open(test::PerThreadDBPath("udi_udt_reject") + "test.sst");
  if (writer_status.ok()) {
    writer_status = writer.Put("key", ts, "value");
  }
  if (writer_status.ok()) {
    writer_status = writer.Finish();
  }
  ASSERT_TRUE(writer_status.IsInvalidArgument()) << writer_status.ToString();
  ASSERT_NE(writer_status.ToString().find("user-defined timestamps"),
            std::string::npos)
      << writer_status.ToString();
}

TEST_P(UserDefinedIndexTest, CustomOnlyRejectsPartitionedFiltersInSstWriter) {
  BlockBasedTableOptions table_options;
  table_options.user_defined_index_factory =
      std::make_shared<TestIndexFactory>();
  table_options.index_mode = BlockBasedTableOptions::IndexMode::kCustomOnly;
  table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  table_options.partition_filters = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  Options options = options_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileWriter writer(EnvOptions(), options);
  Status s = writer.Open(
      test::PerThreadDBPath("udi_custom_only_partitioned_filter") + ".sst");
  if (s.ok()) {
    s = writer.Put("key", "value");
  }
  if (s.ok()) {
    s = writer.Finish();
  }
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

// Backward-compatibility regression: before index_mode existed, setting only
// user_defined_index_factory built a secondary UDI while reads still used the
// standard index by default. That source still compiles, so it must not
// silently stop writing UDI blocks.
TEST_P(UserDefinedIndexTest, FactoryOnlyDefaultBuildsSecondaryUdi) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("udi_factory_only_default_test");
  std::string ingest_file = dbname + "test.sst";

  auto factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = factory;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  ASSERT_EQ(table_options.index_mode,
            BlockBasedTableOptions::IndexMode::kStandardDefault);
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer(
      new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));
  auto kvs = generateKVs(/*key_count=*/30);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<SstFileReader> reader(new SstFileReader(options_));
  ASSERT_OK(reader->Open(ingest_file));

  // Default reads use the standard index and should not see UDI errors.
  std::unique_ptr<Iterator> iter(reader->NewIterator(ReadOptions()));
  iter->Seek("key09");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());

  // Explicitly selecting the custom index must route through the UDI that was
  // built by the factory-only default path.
  factory->seek_error_count_ = 1;
  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  iter.reset(reader->NewIterator(ro));
  iter->Seek("key09");
  ASSERT_NOK(iter->status());
}

// A custom index block that is present but unreadable must not take the whole
// SST offline when the standard index is fully populated. kStandardDefault
// treats the custom index as optional, so it falls back and counts the failure.
// The modes that promise or route through the custom index still fail loudly.
TEST_P(UserDefinedIndexTest, UnreadableSecondaryUdiFallsBackToStandardIndex) {
  // Keeps TestIndexFactory's name so the meta block is still found on reopen,
  // then fails to build a reader the way a corrupt block would.
  class FailingReaderFactory : public TestIndexFactory {
   public:
    using TestIndexFactory::NewReader;

    Status NewReader(
        const IndexFactoryOptions& /*option*/, Slice& /*index_block*/,
        std::unique_ptr<IndexFactoryReader>& /*reader*/) const override {
      return Status::Corruption("simulated unreadable UDI block");
    }
  };

  std::string dbname = test::PerThreadDBPath("udi_unreadable_secondary_test");
  std::string ingest_file = dbname + "test.sst";

  BlockBasedTableOptions write_table_options;
  write_table_options.user_defined_index_factory =
      std::make_shared<TestIndexFactory>();
  write_table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();
  ASSERT_EQ(write_table_options.index_mode,
            BlockBasedTableOptions::IndexMode::kStandardDefault);
  options_.table_factory.reset(NewBlockBasedTableFactory(write_table_options));

  std::unique_ptr<SstFileWriter> writer(
      new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));
  auto kvs = generateKVs(/*key_count=*/30);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<SstFileReader> reader;
  auto open_with_mode = [&](BlockBasedTableOptions::IndexMode mode) {
    BlockBasedTableOptions read_table_options;
    read_table_options.index_mode = mode;
    read_table_options.user_defined_index_factory =
        std::make_shared<FailingReaderFactory>();
    options_.table_factory.reset(NewBlockBasedTableFactory(read_table_options));
    options_.statistics = CreateDBStatistics();
    reader.reset(new SstFileReader(options_));
    return reader->Open(ingest_file);
  };

  ASSERT_OK(
      open_with_mode(BlockBasedTableOptions::IndexMode::kStandardDefault));
  ASSERT_EQ(options_.statistics->getTickerCount(
                SST_USER_DEFINED_INDEX_LOAD_FAIL_COUNT),
            1);
  {
    std::unique_ptr<Iterator> iter(reader->NewIterator(ReadOptions()));
    iter->Seek("key09");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key().ToString(), "key09");
    ASSERT_EQ(iter->value().ToString(), "value09");
  }

  ASSERT_NOK(
      open_with_mode(BlockBasedTableOptions::IndexMode::kStandardRequired));
  ASSERT_NOK(open_with_mode(BlockBasedTableOptions::IndexMode::kCustomDefault));
}

TEST_P(UserDefinedIndexTest, LegacyReadOptionFactoryPrecedence) {
  class OtherNameTestIndexFactory : public TestIndexFactory {
   public:
    const char* Name() const override { return "other_test_index"; }
  };
  using IndexMode = BlockBasedTableOptions::IndexMode;
  for (IndexMode mode : {IndexMode::kStandardDefault, IndexMode::kCustomDefault,
                         IndexMode::kCustomOnly}) {
    SCOPED_TRACE(static_cast<int>(mode));
    BlockBasedTableOptions table_options;
    const std::string path =
        test::PerThreadDBPath("udi_read_factory_precedence") + ".sst";
    auto factory = std::make_shared<TestIndexFactory>();
    table_options.user_defined_index_factory = factory;
    table_options.index_mode = mode;
    table_options.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicyFactory>();
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
    {
      SstFileWriter writer(EnvOptions(), options_);
      ASSERT_OK(writer.Open(path));
      for (const auto& [key, value] : generateKVs(/*key_count=*/30)) {
        ASSERT_OK(writer.Put(key, value));
      }
      ASSERT_OK(writer.Finish());
    }
    {
      SstFileReader reader(options_);
      ASSERT_OK(reader.Open(path));
      OtherNameTestIndexFactory other_factory;
      for (auto selection :
           {ReadOptions::ReadIndex::kDefault, ReadOptions::ReadIndex::kBuiltin,
            ReadOptions::ReadIndex::kPreferCustom}) {
        SCOPED_TRACE(static_cast<int>(selection));
        ReadOptions ro;
        ro.read_index = selection;
        ro.table_index_factory = &other_factory;
        const bool expect_error =
            selection == ReadOptions::ReadIndex::kPreferCustom ||
            (selection == ReadOptions::ReadIndex::kDefault &&
             mode == IndexMode::kStandardDefault) ||
            (selection == ReadOptions::ReadIndex::kBuiltin &&
             mode == IndexMode::kCustomOnly);
        std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
        iter->Seek("key09");
        std::string value;
        Status get_status = reader.Get(ro, "key09", &value);
        if (expect_error) {
          ASSERT_TRUE(iter->status().IsInvalidArgument())
              << iter->status().ToString();
          ASSERT_TRUE(get_status.IsInvalidArgument()) << get_status.ToString();
        } else {
          ASSERT_OK(get_status);
          ASSERT_EQ(value, "value09");
          ASSERT_OK(iter->status());
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(iter->key(), "key09");
          ASSERT_EQ(iter->value(), "value09");
        }
      }
      ReadOptions ro;
      ro.table_index_factory = factory.get();
      std::string value;
      ASSERT_OK(reader.Get(ro, "key09", &value));
      ASSERT_EQ(value, "value09");
      std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
      iter->Seek("key09");
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
    }
    ASSERT_OK(options_.env->DeleteFile(path));
  }
}

TEST_P(UserDefinedIndexTest, MetaBlockPrefixOnDiskBackwardCompat) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("udi_meta_prefix_test");
  std::string ingest_file = dbname + "test.sst";

  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer(
      new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));
  auto kvs = generateKVs(/*key_count=*/30);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  // Confirm the meta block exists at exactly the legacy prefix path.
  ImmutableOptions ioptions(options_);
  EnvOptions eoptions(options_);
  uint64_t file_size = 0;
  std::unique_ptr<FSRandomAccessFile> file;
  std::unique_ptr<RandomAccessFileReader> file_reader;
  const auto& fs = options_.env->GetFileSystem();
  ASSERT_OK(fs->GetFileSize(ingest_file, IOOptions(), &file_size, nullptr));
  ASSERT_OK(fs->NewRandomAccessFile(ingest_file, eoptions, &file, nullptr));
  file_reader.reset(new RandomAccessFileReader(std::move(file), ingest_file));

  // Hardcoded literal -- do NOT replace with kIndexFactoryMetaPrefix.
  const std::string legacy_meta_block_name =
      "rocksdb.user_defined_index.test_index";
  BlockHandle block_handle;
  ASSERT_OK(FindMetaBlockInFile(
      file_reader.get(), file_size, kBlockBasedTableMagicNumber, ioptions,
      ReadOptions(), legacy_meta_block_name, &block_handle));
  // Sanity: a non-empty index block was found.
  ASSERT_GT(block_handle.size(), 0u);

  // And the SST opens / serves reads correctly through the standard path.
  std::unique_ptr<SstFileReader> reader(new SstFileReader(options_));
  ASSERT_OK(reader->Open(ingest_file));
  std::unique_ptr<Iterator> iter(reader->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
}

// On-disk format regression test: the meta block written for a user-defined
// index must be keyed by the literal prefix "rocksdb.user_defined_index."
// followed by the factory name. This exact string is used by SSTs written
// by previous RocksDB versions; changing it would silently break readability
// of those existing files.
//
// This test deliberately uses a hardcoded string literal rather than the
// kIndexFactoryMetaPrefix / kUserDefinedIndexPrefix constants -- that way an
// accidental edit to the constant's value is caught here, not in production
// after data is already lost.
//
// Verifies that the two on-disk SST shapes a DB can have are readable
// under every supported open-time read configuration:
//   - sst_secondary : full standard index + UDI (kStandardDefault /
//                     kCustomDefault produce this shape).
//   - sst_primary   : stub standard index + UDI only (kCustomOnly).
// For each shape every (open_mode, read_index) combination that should
// be able to serve the SST must return all 50 KVs.
TEST_P(UserDefinedIndexTest, OldFormatUdiSstReadableInAllModes) {
  auto make_sst = [&](BlockBasedTableOptions::IndexMode write_mode,
                      const std::string& path) {
    BlockBasedTableOptions write_opts;
    write_opts.user_defined_index_factory =
        std::make_shared<TestIndexFactory>();
    write_opts.index_mode = write_mode;
    write_opts.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicyFactory>();

    Options write_options = options_;
    write_options.table_factory.reset(NewBlockBasedTableFactory(write_opts));
    std::unique_ptr<SstFileWriter> w(
        new SstFileWriter(EnvOptions(), write_options));
    ASSERT_OK(w->Open(path));
    auto kvs = generateKVs(/*key_count=*/50);
    for (const auto& kv : kvs) {
      ASSERT_OK(w->Put(kv.first, kv.second));
    }
    ASSERT_OK(w->Finish());
  };

  std::string dbname = test::PerThreadDBPath("udi_old_format_compat");
  std::string sst_secondary = dbname + "secondary.sst";
  std::string sst_primary = dbname + "primary.sst";
  make_sst(BlockBasedTableOptions::IndexMode::kStandardDefault, sst_secondary);
  make_sst(BlockBasedTableOptions::IndexMode::kCustomOnly, sst_primary);

  // Confirm both SSTs have a UDI meta block at the legacy literal prefix.
  // The on-disk constant must remain "rocksdb.user_defined_index.".
  auto assert_has_legacy_prefix = [&](const std::string& path,
                                      bool requires_custom_reader) {
    ImmutableOptions ioptions(options_);
    EnvOptions eoptions(options_);
    uint64_t file_size = 0;
    std::unique_ptr<FSRandomAccessFile> file;
    std::unique_ptr<RandomAccessFileReader> file_reader;
    const auto& fs = options_.env->GetFileSystem();
    ASSERT_OK(fs->GetFileSize(path, IOOptions(), &file_size, nullptr));
    ASSERT_OK(fs->NewRandomAccessFile(path, eoptions, &file, nullptr));
    file_reader.reset(new RandomAccessFileReader(std::move(file), path));
    BlockHandle handle;
    // Hardcoded literal -- do NOT replace with kIndexFactoryMetaPrefix.
    ASSERT_OK(FindMetaBlockInFile(
        file_reader.get(), file_size, kBlockBasedTableMagicNumber, ioptions,
        ReadOptions(), "rocksdb.user_defined_index.test_index", &handle));
    ASSERT_GT(handle.size(), 0u);

    Footer footer;
    ASSERT_OK(ReadFooterFromFile(IOOptions(), file_reader.get(), *fs,
                                 nullptr /* prefetch_buffer */, file_size,
                                 &footer, kBlockBasedTableMagicNumber));
    EXPECT_EQ(
        footer.HasIncompatibleFeature(kFooterFeatureRequireUserDefinedIndex),
        requires_custom_reader);
  };
  assert_has_legacy_prefix(sst_secondary, false);
  assert_has_legacy_prefix(sst_primary, true);

  // Each (open_mode, sst_shape, read_index) combination below must return
  // all 50 KVs. Only an explicit read_index=kBuiltin can land on a
  // kCustomOnly SST's footer-satisfying standard-index stub, and that must
  // fail loudly rather than returning an OK empty scan.
  auto verify_full_scan = [&](BlockBasedTableOptions::IndexMode open_mode,
                              bool factory_set, const std::string& path,
                              ReadOptions::ReadIndex rprobe,
                              const std::string& label) {
    SCOPED_TRACE(label + " probe=" + std::to_string(static_cast<int>(rprobe)));
    BlockBasedTableOptions ro_opts;
    ro_opts.index_mode = open_mode;
    if (factory_set) {
      ro_opts.user_defined_index_factory = std::make_shared<TestIndexFactory>();
    }
    Options read_options = options_;
    read_options.table_factory.reset(NewBlockBasedTableFactory(ro_opts));

    std::unique_ptr<SstFileReader> reader(new SstFileReader(read_options));
    ASSERT_OK(reader->Open(path));
    ReadOptions ro;
    ro.read_index = rprobe;
    std::unique_ptr<Iterator> iter(reader->NewIterator(ro));
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 50);
  };
  auto verify_iter_invalid = [&](BlockBasedTableOptions::IndexMode open_mode,
                                 const std::string& path,
                                 ReadOptions::ReadIndex rprobe,
                                 const std::string& label) {
    SCOPED_TRACE(label + " probe=" + std::to_string(static_cast<int>(rprobe)));
    BlockBasedTableOptions ro_opts;
    ro_opts.index_mode = open_mode;
    ro_opts.user_defined_index_factory = std::make_shared<TestIndexFactory>();
    Options read_options = options_;
    read_options.table_factory.reset(NewBlockBasedTableFactory(ro_opts));

    std::unique_ptr<SstFileReader> reader(new SstFileReader(read_options));
    ASSERT_OK(reader->Open(path));
    ReadOptions ro;
    ro.read_index = rprobe;
    std::unique_ptr<Iterator> iter(reader->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->status().IsInvalidArgument())
        << iter->status().ToString();
  };
  auto verify_open_invalid = [&](BlockBasedTableOptions::IndexMode open_mode,
                                 const std::string& path,
                                 const std::string& label) {
    SCOPED_TRACE(label);
    BlockBasedTableOptions ro_opts;
    ro_opts.index_mode = open_mode;
    Options read_options = options_;
    read_options.table_factory.reset(NewBlockBasedTableFactory(ro_opts));

    std::unique_ptr<SstFileReader> reader(new SstFileReader(read_options));
    Status s = reader->Open(path);
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  };

  // sst_secondary has both indexes; every (mode, probe) returns all rows.
  for (auto mode : {BlockBasedTableOptions::IndexMode::kStandardOnly,
                    BlockBasedTableOptions::IndexMode::kStandardDefault,
                    BlockBasedTableOptions::IndexMode::kStandardRequired,
                    BlockBasedTableOptions::IndexMode::kCustomDefault,
                    BlockBasedTableOptions::IndexMode::kCustomOnly}) {
    bool factory_needed =
        mode != BlockBasedTableOptions::IndexMode::kStandardOnly;
    for (auto probe :
         {ReadOptions::ReadIndex::kDefault, ReadOptions::ReadIndex::kBuiltin,
          ReadOptions::ReadIndex::kPreferCustom}) {
      verify_full_scan(
          mode, factory_needed, sst_secondary, probe,
          "sst_secondary mode=" + std::to_string(static_cast<int>(mode)));
    }
  }

  // sst_primary's standard index is a stub, so reads must route to the
  // custom index. kDefault does this on its own even when the current
  // index_mode makes the standard index primary, so lowering index_mode
  // does not brick SSTs already written in kCustomOnly.
  verify_full_scan(BlockBasedTableOptions::IndexMode::kStandardDefault, true,
                   sst_primary, ReadOptions::ReadIndex::kDefault,
                   "sst_primary kStandardDefault");
  verify_full_scan(BlockBasedTableOptions::IndexMode::kStandardDefault, true,
                   sst_primary, ReadOptions::ReadIndex::kPreferCustom,
                   "sst_primary kStandardDefault");
  verify_full_scan(BlockBasedTableOptions::IndexMode::kStandardRequired, true,
                   sst_primary, ReadOptions::ReadIndex::kDefault,
                   "sst_primary kStandardRequired");
  verify_full_scan(BlockBasedTableOptions::IndexMode::kStandardRequired, true,
                   sst_primary, ReadOptions::ReadIndex::kPreferCustom,
                   "sst_primary kStandardRequired");
  verify_full_scan(BlockBasedTableOptions::IndexMode::kCustomDefault, true,
                   sst_primary, ReadOptions::ReadIndex::kDefault,
                   "sst_primary kCustomDefault");
  verify_full_scan(BlockBasedTableOptions::IndexMode::kCustomDefault, true,
                   sst_primary, ReadOptions::ReadIndex::kPreferCustom,
                   "sst_primary kCustomDefault");
  verify_full_scan(BlockBasedTableOptions::IndexMode::kCustomOnly, true,
                   sst_primary, ReadOptions::ReadIndex::kDefault,
                   "sst_primary kCustomOnly");
  verify_full_scan(BlockBasedTableOptions::IndexMode::kCustomOnly, true,
                   sst_primary, ReadOptions::ReadIndex::kPreferCustom,
                   "sst_primary kCustomOnly");

  // Only an explicit kBuiltin request reaches the stub, in any open mode.
  verify_iter_invalid(BlockBasedTableOptions::IndexMode::kStandardDefault,
                      sst_primary, ReadOptions::ReadIndex::kBuiltin,
                      "sst_primary kStandardDefault builtin");
  verify_iter_invalid(BlockBasedTableOptions::IndexMode::kStandardRequired,
                      sst_primary, ReadOptions::ReadIndex::kBuiltin,
                      "sst_primary kStandardRequired builtin");
  verify_iter_invalid(BlockBasedTableOptions::IndexMode::kCustomDefault,
                      sst_primary, ReadOptions::ReadIndex::kBuiltin,
                      "sst_primary kCustomDefault builtin");
  verify_iter_invalid(BlockBasedTableOptions::IndexMode::kCustomOnly,
                      sst_primary, ReadOptions::ReadIndex::kBuiltin,
                      "sst_primary kCustomOnly builtin");
  verify_open_invalid(BlockBasedTableOptions::IndexMode::kStandardOnly,
                      sst_primary, "sst_primary kStandardOnly without factory");
  verify_open_invalid(BlockBasedTableOptions::IndexMode::kStandardDefault,
                      sst_primary,
                      "sst_primary kStandardDefault without factory");
}

// Verifies that the deprecated UDI boolean aliases
// (use_udi_as_primary_index, skip_standard_index, fail_if_no_udi_on_open)
// translate into the equivalent `index_mode` at OPTIONS parse time, so
// that an upgrade from a DB with these booleans in its OPTIONS file
// produces the same SST shape and read routing as before.
TEST_P(UserDefinedIndexTest, DeprecatedUdiBooleansTranslateToIndexMode) {
  ConfigOptions cfg;
  cfg.ignore_unknown_options = false;

  auto translates_to = [&](const std::string& boolean_assignments,
                           BlockBasedTableOptions::IndexMode expected) {
    SCOPED_TRACE(boolean_assignments);
    BlockBasedTableOptions out;
    ASSERT_OK(GetBlockBasedTableOptionsFromString(cfg, BlockBasedTableOptions(),
                                                  boolean_assignments, &out));
    EXPECT_EQ(static_cast<int>(out.GetEffectiveIndexMode()),
              static_cast<int>(expected));
  };

  // All booleans false leaves the default kStandardDefault, which preserves the
  // old factory-only secondary-UDI behavior while acting like standard-only
  // when no factory is configured.
  translates_to(
      "fail_if_no_udi_on_open=false;use_udi_as_primary_index=false;skip_"
      "standard_index=false",
      BlockBasedTableOptions::IndexMode::kStandardDefault);

  // Each boolean upgrades to its mapped mode.
  translates_to("fail_if_no_udi_on_open=true",
                BlockBasedTableOptions::IndexMode::kStandardRequired);
  translates_to("use_udi_as_primary_index=true",
                BlockBasedTableOptions::IndexMode::kCustomDefault);
  translates_to("skip_standard_index=true",
                BlockBasedTableOptions::IndexMode::kCustomOnly);

  // Monotonic upgrade: highest mode requested wins regardless of order.
  translates_to("use_udi_as_primary_index=true;skip_standard_index=true",
                BlockBasedTableOptions::IndexMode::kCustomOnly);
  translates_to("skip_standard_index=true;use_udi_as_primary_index=true",
                BlockBasedTableOptions::IndexMode::kCustomOnly);
  translates_to(
      "fail_if_no_udi_on_open=true;use_udi_as_primary_index=true;skip_standard_"
      "index=true",
      BlockBasedTableOptions::IndexMode::kCustomOnly);

  // Non-default modes are explicit even in the old encoding. A default mode
  // needs the origin marker to override legacy aliases in mixed input.
  translates_to("index_mode=kCustomOnly;fail_if_no_udi_on_open=true",
                BlockBasedTableOptions::IndexMode::kCustomOnly);
  translates_to("index_mode=kStandardOnly;skip_standard_index=true",
                BlockBasedTableOptions::IndexMode::kStandardOnly);
  translates_to(
      "index_mode=kStandardDefault;use_udi_as_primary_index=true;skip_standard_"
      "index=true",
      BlockBasedTableOptions::IndexMode::kCustomOnly);
  translates_to(
      "index_mode=kStandardDefault;index_mode_explicit=true;"
      "use_udi_as_primary_index=true;skip_standard_index=true",
      BlockBasedTableOptions::IndexMode::kStandardDefault);

  // SetOptions() applies dotted "block_based_table_factory.<name>" keys one at
  // a time through Configurable::ConfigureOption(), which never reaches
  // ConfigureOptions()'s whole-map canonicalization. kStandardOnly is the
  // rollback switch, so a stale legacy alias arriving through that path must
  // not escalate it back into a custom-index mode.
  auto single_key_from = [&](BlockBasedTableOptions::IndexMode start,
                             const std::string& name, const std::string& value,
                             BlockBasedTableOptions::IndexMode expected) {
    SCOPED_TRACE("start=" + std::to_string(static_cast<int>(start)) + " " +
                 name + "=" + value);
    BlockBasedTableOptions in;
    in.index_mode = start;
    std::shared_ptr<TableFactory> factory(NewBlockBasedTableFactory(in));
    ASSERT_OK(factory->ConfigureOption(cfg, name, value));
    const auto* out = factory->GetOptions<BlockBasedTableOptions>();
    ASSERT_NE(out, nullptr);
    EXPECT_EQ(static_cast<int>(out->GetEffectiveIndexMode()),
              static_cast<int>(expected));
  };

  for (const std::string alias :
       {"fail_if_no_udi_on_open", "use_udi_as_primary_index",
        "skip_standard_index"}) {
    single_key_from(BlockBasedTableOptions::IndexMode::kStandardOnly, alias,
                    "true", BlockBasedTableOptions::IndexMode::kStandardOnly);
  }

  // From the unset default the aliases still escalate, so legacy-only callers
  // that never touch index_mode keep working on this path too.
  single_key_from(BlockBasedTableOptions::IndexMode::kStandardDefault,
                  "fail_if_no_udi_on_open", "true",
                  BlockBasedTableOptions::IndexMode::kStandardRequired);
  single_key_from(BlockBasedTableOptions::IndexMode::kStandardDefault,
                  "use_udi_as_primary_index", "true",
                  BlockBasedTableOptions::IndexMode::kCustomDefault);
  single_key_from(BlockBasedTableOptions::IndexMode::kStandardDefault,
                  "skip_standard_index", "true",
                  BlockBasedTableOptions::IndexMode::kCustomOnly);

  // Non-default modes supplied in the C++ options are explicit too.
  single_key_from(BlockBasedTableOptions::IndexMode::kStandardRequired,
                  "use_udi_as_primary_index", "true",
                  BlockBasedTableOptions::IndexMode::kStandardRequired);
  single_key_from(BlockBasedTableOptions::IndexMode::kCustomDefault,
                  "skip_standard_index", "true",
                  BlockBasedTableOptions::IndexMode::kCustomDefault);

  // Strict secondary mode must preserve the old fail_if_no_udi_on_open=true
  // behavior without switching default reads to the custom index.
  std::string strict_dbname = test::PerThreadDBPath("udi_strict_compat");
  std::string strict_sst = strict_dbname + "missing_udi.sst";
  {
    Options write_options = options_;
    write_options.table_factory.reset(
        NewBlockBasedTableFactory(BlockBasedTableOptions()));
    std::unique_ptr<SstFileWriter> w(
        new SstFileWriter(EnvOptions(), write_options));
    ASSERT_OK(w->Open(strict_sst));
    ASSERT_OK(w->Put("key00", "value00"));
    ASSERT_OK(w->Finish());
  }
  BlockBasedTableOptions strict_opts;
  ASSERT_OK(GetBlockBasedTableOptionsFromString(cfg, BlockBasedTableOptions(),
                                                "fail_if_no_udi_on_open=true",
                                                &strict_opts));
  ASSERT_EQ(strict_opts.GetEffectiveIndexMode(),
            BlockBasedTableOptions::IndexMode::kStandardRequired);
  Options strict_no_factory_options = options_;
  strict_no_factory_options.table_factory.reset(
      NewBlockBasedTableFactory(strict_opts));
  std::unique_ptr<SstFileReader> strict_no_factory_reader(
      new SstFileReader(strict_no_factory_options));
  ASSERT_OK(strict_no_factory_reader->Open(strict_sst));
  ASSERT_OK(strict_no_factory_reader->VerifyChecksum(ReadOptions()));
  std::unique_ptr<Iterator> strict_no_factory_iter(
      strict_no_factory_reader->NewIterator(ReadOptions()));
  strict_no_factory_iter->Seek("key00");
  ASSERT_TRUE(strict_no_factory_iter->Valid());
  ASSERT_EQ(strict_no_factory_iter->key(), "key00");
  ASSERT_EQ(strict_no_factory_iter->value(), "value00");
  ASSERT_OK(strict_no_factory_iter->status());

  strict_opts.user_defined_index_factory = std::make_shared<TestIndexFactory>();
  Options strict_read_options = options_;
  strict_read_options.table_factory.reset(
      NewBlockBasedTableFactory(strict_opts));
  std::unique_ptr<SstFileReader> strict_reader(
      new SstFileReader(strict_read_options));
  Status strict_s = strict_reader->Open(strict_sst);
  ASSERT_TRUE(strict_s.IsCorruption()) << strict_s.ToString();

  // End-to-end check: an OPTIONS string containing only
  // `use_udi_as_primary_index=true` must produce a config that reads a
  // kCustomOnly-shape SST in full.
  std::string dbname = test::PerThreadDBPath("udi_legacy_options_compat");
  std::string sst_path = dbname + "primary.sst";
  {
    BlockBasedTableOptions write_opts;
    write_opts.user_defined_index_factory =
        std::make_shared<TestIndexFactory>();
    write_opts.index_mode = BlockBasedTableOptions::IndexMode::kCustomOnly;
    write_opts.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicyFactory>();
    Options write_options = options_;
    write_options.table_factory.reset(NewBlockBasedTableFactory(write_opts));
    std::unique_ptr<SstFileWriter> w(
        new SstFileWriter(EnvOptions(), write_options));
    ASSERT_OK(w->Open(sst_path));
    auto kvs = generateKVs(/*key_count=*/50);
    for (const auto& kv : kvs) {
      ASSERT_OK(w->Put(kv.first, kv.second));
    }
    ASSERT_OK(w->Finish());
  }

  BlockBasedTableOptions parsed_opts;
  ASSERT_OK(GetBlockBasedTableOptionsFromString(cfg, BlockBasedTableOptions(),
                                                "use_udi_as_primary_index=true",
                                                &parsed_opts));
  ASSERT_EQ(parsed_opts.GetEffectiveIndexMode(),
            BlockBasedTableOptions::IndexMode::kCustomDefault);
  parsed_opts.user_defined_index_factory = std::make_shared<TestIndexFactory>();

  Options read_options = options_;
  read_options.table_factory.reset(NewBlockBasedTableFactory(parsed_opts));
  std::unique_ptr<SstFileReader> reader(new SstFileReader(read_options));
  ASSERT_OK(reader->Open(sst_path));
  std::unique_ptr<Iterator> iter(reader->NewIterator(ReadOptions()));
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    count++;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 50)
      << "kCustomOnly SST was unreadable through the upgraded config: legacy "
         "use_udi_as_primary_index=true must translate to kCustomDefault";
}

// Verifies that a UDI whose builder reports SupportsParallelAddEntry()==true
// is wired into the parallel compression pipeline correctly:
// PrepareAddEntry runs on the emit thread, FinishAddEntry on the BG worker,
// and the synchronous AddIndexEntry path is NOT taken. ParallelAwareTestFactory
// counts each callback, letting us assert which path was used.
//
// This test uses the DB Put + Flush path (rather than SstFileWriter)
// because that's the canonical caller of MaybeStartParallelCompression
// when parallel_threads > 1.
TEST_P(UserDefinedIndexTest, ParallelCompressionWithSupportingUdi) {
  BlockBasedTableOptions table_options;
  auto factory = std::make_shared<ParallelAwareTestFactory>();
  table_options.user_defined_index_factory = factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.create_if_missing = true;
  // Compression must be on for the parallel pipeline to actually run; with
  // no compressor the table builder silently falls back to serial.
  // Use Zlib so the parallel pipeline activates regardless of whether
  // Snappy/LZ4/ZSTD are linked into this test binary.
  if (!Zlib_Supported()) {
    fprintf(stderr, "Skipping: Zlib not linked into this build\n");
    return;
  }
  options_.compression = kZlibCompression;
  options_.compression_opts.parallel_threads = 4;
  options_.write_buffer_size = 1 << 20;  // 1 MiB

  std::string dbname = test::PerThreadDBPath("udi_parallel_supporting_test");
  ASSERT_OK(DestroyDB(dbname, options_));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, dbname, &db));

  // Enough keys with the 3-per-block flush policy that we get many data
  // blocks -> many index entries through the parallel pipeline.
  auto kvs = generateKVs(/*key_count=*/300);
  for (const auto& kv : kvs) {
    ASSERT_OK(db->Put(WriteOptions(), kv.first, kv.second));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // Parallel pipeline ran for the custom index: prepare and finish were
  // invoked, AddIndexEntry (the serial fallback) was NOT.
  EXPECT_GT(factory->prepare_calls.load(), 0);
  EXPECT_GT(factory->finish_calls.load(), 0);
  EXPECT_EQ(factory->prepare_calls.load(), factory->finish_calls.load());
  EXPECT_EQ(factory->add_index_entry_calls.load(), 0);

  // Reads work via the standard index (the DB still has the UDI factory
  // configured, but ParallelAwareTestFactory's NewReader returns
  // NotSupported, so set read_index = kBuiltin to bypass it).
  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kBuiltin;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options_));
}

// when EmitBlockForParallel doesn't run its custom-index
// PrepareAddEntry path for a given block (defensive ParseInternalKey
// failure path), BGWorker must NOT commit a stale prepared entry left over
// from a previous use of the same BlockRep slot.
//
// Reproduction: a sync-point callback flips skip_custom_prepare = true on
// every 3rd block emitted. Without the flag-reset fix in
// EmitBlockForParallel, the BlockRep ring buffer's custom_entries_prepared
// flag from a previous iteration would leak through, and BGWorker would
// invoke FinishAddEntry against stale staged data -- observable as
// finish_calls > prepare_calls. With the fix, the flags are reset at the
// top of EmitBlockForParallel before any conditional staging runs, so
// FinishAddEntry only fires for blocks whose Prepare actually ran.
TEST_P(UserDefinedIndexTest,
       ParallelCompressionStaleFlagResetWhenPrepareSkipped) {
  BlockBasedTableOptions table_options;
  auto factory = std::make_shared<ParallelAwareTestFactory>();
  table_options.user_defined_index_factory = factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.create_if_missing = true;
  if (!Zlib_Supported()) {
    fprintf(stderr, "Skipping: Zlib not linked into this build\n");
    return;
  }
  options_.compression = kZlibCompression;
  options_.compression_opts.parallel_threads = 4;
  options_.write_buffer_size = 1 << 20;

  std::string dbname = test::PerThreadDBPath("udi_parallel_stale_flag_test");
  ASSERT_OK(DestroyDB(dbname, options_));

  // Inject a controlled "skip" on every 3rd EmitBlockForParallel call
  // that has a non-empty custom_indexes vector. The atomic counter is
  // process-global which is fine for this single-DB test.
  std::atomic<int> emit_call_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::EmitBlockForParallel:SkipCustomPrepare",
      [&](void* arg) {
        bool* skip = static_cast<bool*>(arg);
        const int n = emit_call_count.fetch_add(1) + 1;
        if (n % 3 == 0) {
          *skip = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, dbname, &db));

  // Use enough keys to comfortably cycle the parallel ring buffer
  // multiple times so the stale-flag scenario becomes observable.
  auto kvs = generateKVs(/*key_count=*/300);
  for (const auto& kv : kvs) {
    ASSERT_OK(db->Put(WriteOptions(), kv.first, kv.second));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // The fix's guarantee: FinishAddEntry only ever runs for blocks whose
  // Prepare actually populated the slot. With the sync-point skipping
  // ~1 in 3 prepares, we expect strictly fewer finishes than the total
  // emitted blocks but exactly equal to the prepares that ran.
  EXPECT_GT(factory->prepare_calls.load(), 0);
  EXPECT_GT(factory->finish_calls.load(), 0);
  EXPECT_EQ(factory->prepare_calls.load(), factory->finish_calls.load())
      << "Stale prepared-entry slot leaked from a previous ring-buffer "
         "iteration: BGWorker called FinishAddEntry for a block whose "
         "PrepareAddEntry was skipped this iteration.";
  // Sanity: the sync-point did skip something.
  EXPECT_GT(emit_call_count.load(), 0);

  // SST opens cleanly (read via standard index since
  // ParallelAwareTestFactory has only a stub reader).
  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kBuiltin;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options_));
}

// Whether parallel compression is usable is a per-implementation property
// (IndexFactoryBuilder::SupportsParallelAddEntry). When the configured
// custom factory's builder doesn't support the parallel protocol, the
// table builder silently falls back to single-threaded compression instead
// of erroring out -- same convention as the partition_filters fallback.
//
// TestIndexFactoryBuilder leaves SupportsParallelAddEntry() at its default
// (false), so this test exercises the fallback path: parallel_threads=10
// is requested, the SST still builds and verifies cleanly.
TEST_P(UserDefinedIndexTest, ParallelCompressionFallbackWithUnsupportedUdi) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.compression_opts.parallel_threads = 10;

  std::unique_ptr<SstFileWriter> writer(
      new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));
  auto kvs = generateKVs(/*key_count=*/30);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  // Round-trip read: the SST opens cleanly via the standard index path
  // and the UDI block (built single-threaded) is consultable.
  std::unique_ptr<SstFileReader> reader(new SstFileReader(options_));
  ASSERT_OK(reader->Open(ingest_file));
  std::unique_ptr<Iterator> iter(reader->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
}

// A builder can claim parallel support and still leave the rest of the
// protocol unimplemented: CreatePreparedAddEntry() defaults to nullptr. The
// table builder probes for that before committing to the parallel pipeline and
// treats it as declining support, so the SST is built through the synchronous
// AddIndexEntry path instead of dereferencing the null prepared entry.
TEST_P(UserDefinedIndexTest, ParallelClaimWithoutPreparedEntryFallsBack) {
  class ClaimsParallelFactory : public IndexFactory {
   public:
    using IndexFactory::NewBuilder;
    using IndexFactory::NewReader;

    mutable std::atomic<int> add_index_entry_calls{0};

    const char* Name() const override { return "claims_parallel_test_index"; }

    Status NewBuilder(
        const IndexFactoryOptions& /*opts*/,
        std::unique_ptr<IndexFactoryBuilder>& builder) const override {
      builder = std::make_unique<Builder>(this);
      return Status::OK();
    }

    Status NewReader(
        const IndexFactoryOptions&, Slice&,
        std::unique_ptr<IndexFactoryReader>& reader) const override {
      reader = std::make_unique<StubReader>();
      return Status::OK();
    }

   private:
    // SST open requires a non-null reader. Tests route reads through
    // read_index = kBuiltin, so this iterator is never invoked.
    class StubReader : public IndexFactoryReader {
     public:
      std::unique_ptr<IndexFactoryIterator> NewIterator(
          const ReadOptions&) override {
        return nullptr;
      }
      size_t ApproximateMemoryUsage() const override { return 0; }
    };

    class Builder : public IndexFactoryBuilder {
     public:
      explicit Builder(const ClaimsParallelFactory* f) : factory_(f) {}

      // Claims the protocol, but never overrides CreatePreparedAddEntry, so
      // it inherits the nullptr default -- the contract violation under test.
      bool SupportsParallelAddEntry() const override { return true; }

      Slice AddIndexEntry(const Slice& last_key_in_current_block,
                          const Slice* /*first_key_in_next_block*/,
                          const BlockHandle& /*block_handle*/,
                          std::string* separator_scratch,
                          const IndexEntryContext& /*context*/) override {
        factory_->add_index_entry_calls.fetch_add(1);
        separator_scratch->assign(last_key_in_current_block.data(),
                                  last_key_in_current_block.size());
        return Slice(*separator_scratch);
      }

      Status Finish(Slice* index_contents) override {
        contents_ = "claims_parallel";
        *index_contents = Slice(contents_);
        return Status::OK();
      }

      uint64_t EstimatedSize() const override { return 64; }

     private:
      const ClaimsParallelFactory* factory_;
      std::string contents_;
    };
  };

  if (!Zlib_Supported()) {
    fprintf(stderr, "Skipping: Zlib not linked into this build\n");
    return;
  }

  BlockBasedTableOptions table_options;
  auto factory = std::make_shared<ClaimsParallelFactory>();
  table_options.user_defined_index_factory = factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.create_if_missing = true;
  // Compression must be on, otherwise the table builder skips the parallel
  // pipeline for unrelated reasons and the probe never runs.
  options_.compression = kZlibCompression;
  options_.compression_opts.parallel_threads = 4;
  options_.write_buffer_size = 1 << 20;  // 1 MiB

  std::string dbname = test::PerThreadDBPath("udi_claims_parallel_test");
  ASSERT_OK(DestroyDB(dbname, options_));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, dbname, &db));

  auto kvs = generateKVs(/*key_count=*/300);
  for (const auto& kv : kvs) {
    ASSERT_OK(db->Put(WriteOptions(), kv.first, kv.second));
  }
  ASSERT_OK(db->Flush(FlushOptions()));

  // The serial path built the index, which is only reachable if the parallel
  // probe rejected the builder instead of trusting its claim.
  EXPECT_GT(factory->add_index_entry_calls.load(), 0);

  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kBuiltin;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro));
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(iter->value().ToString(),
              "value" + iter->key().ToString().substr(3));
    ++count;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(count, 300);
  iter.reset();
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, MergeWithUDI) {
  // Verify that Merge operations work correctly with user-defined index.
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));

  // Use 5-byte keys to match TestIndexFactoryBuilder expectations.
  ASSERT_OK(writer->Merge("key_a", "val_a"));
  ASSERT_OK(writer->Finish());
  writer.reset();

  // Read back and verify the merge entry is present in the SST.
  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(ingest_file));
  ReadOptions ro;
  std::unique_ptr<Iterator> iter(reader.NewIterator(ro));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "key_a");
  ASSERT_EQ(iter->value().ToString(), "val_a");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_P(UserDefinedIndexTest, DBFlushWithMixedOpsAndUDI) {
  // Verify that Put, Delete, Merge, and SingleDelete all flow correctly
  // through the UDI builder when flushed via DB::Flush.
  std::string dbname = test::PerThreadDBPath("udi_db_flush_test");
  ASSERT_OK(DestroyDB(dbname, options_));

  BlockBasedTableOptions table_options;
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  user_defined_index_factory->skip_key_size_check_ = true;
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.create_if_missing = true;

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, dbname, &db));

  // Write mixed operations.
  ASSERT_OK(db->Put(WriteOptions(), "key_aa", "val_put"));
  ASSERT_OK(db->Merge(WriteOptions(), "key_bb", "val_merge"));
  ASSERT_OK(db->Delete(WriteOptions(), "key_cc"));
  ASSERT_OK(db->Put(WriteOptions(), "key_dd", "val_put2"));
  ASSERT_OK(db->SingleDelete(WriteOptions(), "key_dd"));
  ASSERT_OK(db->Put(WriteOptions(), "key_ee", "val_put3"));

  // Flush to produce an SST with UDI.
  ASSERT_OK(db->Flush(FlushOptions()));

  // Verify data is readable via the native index (which always works with
  // SeekToFirst). key_aa (put), key_bb (merge), key_ee (put) should be
  // visible. key_cc was deleted, key_dd was single-deleted.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db->NewIterator(ro));
    iter->SeekToFirst();
    std::vector<std::string> visible;
    for (; iter->Valid(); iter->Next()) {
      visible.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(visible.size(), 3u);
    // With reverse comparator, keys are in reverse order.
    if (is_reverse_comparator_) {
      std::vector<std::string> expected = {"key_ee", "key_bb", "key_aa"};
      ASSERT_EQ(visible, expected);
    } else {
      std::vector<std::string> expected = {"key_aa", "key_bb", "key_ee"};
      ASSERT_EQ(visible, expected);
    }
  }

  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, ValueTypeMappingViaDBFlush) {
  // Verify that MapToUDIValueType correctly maps internal ValueTypes to UDI
  // ValueTypes by writing various operation types via the DB API, flushing,
  // and inspecting what the TestIndexFactoryBuilder received.
  if (is_reverse_comparator_) {
    // Skip for reverse comparator -- the key ordering makes this test
    // unnecessarily complex and the mapping logic is comparator-independent.
    ROCKSDB_GTEST_BYPASS("Skipped for reverse comparator");
    return;
  }
  std::string dbname = test::PerThreadDBPath("udi_valuetype_mapping_test");
  ASSERT_OK(DestroyDB(dbname, options_));

  BlockBasedTableOptions table_options;
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  user_defined_index_factory->skip_key_size_check_ = true;
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.merge_operator = MergeOperators::CreateStringAppendOperator();
  options_.create_if_missing = true;
  options_.disable_auto_compactions = true;

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, dbname, &db));

  // Write one entry of each type that goes through the flush path.
  // kTypeValue:
  ASSERT_OK(db->Put(WriteOptions(), "key_01_put", "v1"));
  // kTypeMerge:
  ASSERT_OK(db->Merge(WriteOptions(), "key_02_merge", "m1"));
  // kTypeDeletion:
  ASSERT_OK(db->Delete(WriteOptions(), "key_03_del"));
  // kTypeSingleDeletion:
  ASSERT_OK(db->SingleDelete(WriteOptions(), "key_04_sdel"));
  // kTypeWideColumnEntity:
  ASSERT_OK(db->PutEntity(WriteOptions(), db->DefaultColumnFamily(),
                          "key_05_entity", WideColumns{{"col1", "val1"}}));

  ASSERT_OK(db->Flush(FlushOptions()));

  // The builder recorded all (key, ValueType) pairs via the shared log.
  const auto& log = user_defined_index_factory->key_type_log_;
  ASSERT_FALSE(log.empty());

  // Build a map from key to the ValueType received by OnKeyAdded.
  std::map<std::string, IndexFactoryBuilder::ValueType> type_map;
  for (const auto& entry : log) {
    type_map[entry.first] = entry.second;
  }

  // Verify each mapping.
  ASSERT_EQ(type_map.count("key_01_put"), 1u);
  EXPECT_EQ(type_map["key_01_put"], IndexFactoryBuilder::ValueType::kValue);

  ASSERT_EQ(type_map.count("key_02_merge"), 1u);
  EXPECT_EQ(type_map["key_02_merge"], IndexFactoryBuilder::ValueType::kMerge);

  ASSERT_EQ(type_map.count("key_03_del"), 1u);
  EXPECT_EQ(type_map["key_03_del"], IndexFactoryBuilder::ValueType::kDelete);

  ASSERT_EQ(type_map.count("key_04_sdel"), 1u);
  EXPECT_EQ(type_map["key_04_sdel"], IndexFactoryBuilder::ValueType::kDelete);

  ASSERT_EQ(type_map.count("key_05_entity"), 1u);
  EXPECT_EQ(type_map["key_05_entity"], IndexFactoryBuilder::ValueType::kOther);

  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, CompactionWithSnapshotsAndUDI) {
  // Verify that compaction with snapshots (producing multiple versions of the
  // same user key) works correctly with UDI.
  if (is_reverse_comparator_) {
    ROCKSDB_GTEST_BYPASS("Skipped for reverse comparator");
    return;
  }
  std::string dbname = test::PerThreadDBPath("udi_compaction_snapshot_test");
  ASSERT_OK(DestroyDB(dbname, options_));

  BlockBasedTableOptions table_options;
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  user_defined_index_factory->skip_key_size_check_ = true;
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options_.create_if_missing = true;
  // Disable auto-compaction so we control when compaction runs.
  options_.disable_auto_compactions = true;

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options_, dbname, &db));

  // Write version 1 and flush.
  ASSERT_OK(db->Put(WriteOptions(), "key_aa", "v1"));
  ASSERT_OK(db->Put(WriteOptions(), "key_bb", "v1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  // Take a snapshot to force compaction to keep both versions.
  const Snapshot* snap = db->GetSnapshot();

  // Write version 2 and flush (creates a second L0 file).
  ASSERT_OK(db->Put(WriteOptions(), "key_aa", "v2"));
  ASSERT_OK(db->Delete(WriteOptions(), "key_bb"));
  ASSERT_OK(db->Flush(FlushOptions()));

  // Compact L0 -> L1. With the snapshot held, both versions of key_aa
  // and the delete tombstone for key_bb must be preserved in the compaction
  // output. The UDI builder receives multiple entries for key_aa.
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the UDI builder saw entries during compaction. The key_type_log
  // accumulates from all builders (two flushes + one compaction). The
  // compaction output must contain multiple versions of key_aa (v2 and v1,
  // due to the snapshot) and both the delete tombstone and old value of key_bb.
  const auto& log = user_defined_index_factory->key_type_log_;
  ASSERT_FALSE(log.empty());

  // Count total occurrences of key_aa across all builders -- at least 4:
  // flush1 (v1) + flush2 (v2) + compaction (v2, v1).
  int key_aa_count = 0;
  int key_bb_count = 0;
  for (const auto& entry : log) {
    if (entry.first == "key_aa") {
      key_aa_count++;
    } else if (entry.first == "key_bb") {
      key_bb_count++;
    }
  }
  // flush1 (1) + flush2 (1) + compaction (2 versions due to snapshot) = 4.
  ASSERT_GE(key_aa_count, 4) << "Expected key_aa from flush1 + flush2 + "
                                "compaction (2 versions due to snapshot)";
  // flush1 (1) + flush2 (1) + compaction (tombstone + old value) = 4.
  ASSERT_GE(key_bb_count, 4) << "Expected key_bb from flush1 + flush2 + "
                                "compaction (tombstone + old value)";

  // Verify current view via native index: key_aa=v2, key_bb deleted.
  {
    ReadOptions ro;
    std::unique_ptr<Iterator> iter(db->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_aa");
    ASSERT_EQ(iter->value().ToString(), "v2");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Verify snapshot view via native index: key_aa=v1, key_bb=v1.
  {
    ReadOptions ro;
    ro.snapshot = snap;
    std::unique_ptr<Iterator> iter(db->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_aa");
    ASSERT_EQ(iter->value().ToString(), "v1");
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "key_bb");
    ASSERT_EQ(iter->value().ToString(), "v1");
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  db->ReleaseSnapshot(snap);
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, IngestTest) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;

  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));

  auto kvs = generateKVs(/*key_count*/ 100);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }

  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<DB> db;
  options_.create_if_missing = true;
  Status s = DB::Open(options_, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options_, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  ReadOptions ro;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  ASSERT_OK(iter->status());

  // Test that we can read all the keys
  int key_count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key_count++;
  }
  ASSERT_EQ(key_count, 100);  // We added 100 keys
  ASSERT_OK(iter->status());
  iter.reset();

  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  iter.reset(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);

  // Test seek specific key
  key_count = 0;
  for (iter->Seek("key40"); iter->Valid(); iter->Next()) {
    key_count++;
  }
  ASSERT_EQ(key_count, is_reverse_comparator_ ? 41 : 60);
  ASSERT_OK(iter->status());

  // Test upper bound
  Slice ub(is_reverse_comparator_ ? "key25" : "key75");
  ro.iterate_upper_bound = &ub;
  iter.reset(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);

  // Test seek specific key with upper bound
  key_count = 0;
  for (iter->Seek("key40"); iter->Valid(); iter->Next()) {
    key_count++;
  }
  ASSERT_EQ(key_count, is_reverse_comparator_ ? 15 : 35);
  ASSERT_OK(iter->status());
  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, EmptyRangeTest) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;

  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));

  // Generate key range key0 ~ key19, key40 ~ key59, key80 ~ key99
  std::vector<std::pair<std::string, std::string>> kvs;
  bool skip = false;
  for (int i = 0; i < 100; i++) {
    if (i > 0 && i % 20 == 0) {
      skip = !skip;
    }
    if (skip) {
      continue;
    }
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    std::string key = "key" + ss.str();
    std::string value = "value" + ss.str();
    kvs.emplace_back(key, value);
  }

  if (is_reverse_comparator_) {
    std::reverse(kvs.begin(), kvs.end());
  }

  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<DB> db;
  options_.create_if_missing = true;
  Status s = DB::Open(options_, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options_, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  ReadOptions ro;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  ASSERT_OK(iter->status());

  // Test that we can read all the keys
  int key_count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key_count++;
  }
  ASSERT_EQ(key_count, 60);
  ASSERT_OK(iter->status());
  iter.reset();

  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  std::vector<int> key_counts;
  MultiScanArgs scan_opts(options_.comparator);
  std::unordered_map<std::string, std::string> property_bag;
  property_bag["count"] = std::to_string(5);

  ValidateMultiScan({{{"key25", "key30"}, 0, 0},
                     {{"key33", "key37"}, 0, 0},
                     // Non-empty scan with range greater than count
                     // In the key42:key56 range, we might read an additional
                     // block worth of keys due to the boundaries (5 + 3)
                     {{"key42", "key56"}, 8, 7},
                     // Empty scan succeeding a non-empty one
                     {{"key65", "key70"}, 0, 0},
                     // A non-empty scan with range smaller than count
                     {{"key85", "key87"}, 2, 2},
                     // Scan range completely outside the DB
                     {{"key991", "key999"}, 0, 0}},
                    property_bag, ro, scan_opts, key_counts, db, cfh);

  // Scans that overlap with part of key range, with overlap less than count
  ValidateMultiScan({{{"key18", "key25"}, 2, 1}, {{"key38", "key43"}, 3, 4}},
                    property_bag, ro, scan_opts, key_counts, db, cfh);

  // Scans that overlap with part of key range, with overlap same as count
  ValidateMultiScan({{{"key15", "key26"}, 5, 4}, {{"key38", "key46"}, 6, 7}},
                    property_bag, ro, scan_opts, key_counts, db, cfh);

  // Scans that overlap with part of key range, with overlap greater than count
  ValidateMultiScan({{{"key10", "key26"}, 8, 8},
                     // Cross block boundary
                     {{"key38", "key49"}, 7, 9}},
                    property_bag, ro, scan_opts, key_counts, db, cfh);

  // Scan bigger than one contiguous range of keys, with overlap greater than
  // count
  ValidateMultiScan({{{"key75", "key991"}, 8, 9}}, property_bag, ro, scan_opts,
                    key_counts, db, cfh);

  // Scan bigger than one contiguous range of keys, with overlap less than count
  property_bag["count"] = std::to_string(25);
  ValidateMultiScan({{{"key75", "key991"}, 20, 20}}, property_bag, ro,
                    scan_opts, key_counts, db, cfh);

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

// Verify that external file ingestion fails if we try to ingest an SST file
// without the UDI block when index_mode is kCustomDefault or kCustomOnly (which
// require all SSTs to have the custom index).
TEST_P(UserDefinedIndexTest, IngestFailTest) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));

  auto kvs = generateKVs(/*key_count*/ 100);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode = BlockBasedTableOptions::IndexMode::kCustomDefault;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<DB> db;
  options_.create_if_missing = true;
  Status s = DB::Open(options_, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options_, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_NOK(s);

  // Downgrade to kStandardOnly to allow ingesting files without UDI.
  ASSERT_OK(db->SetOptions(
      cfh, {{"block_based_table_factory", "{index_mode=kStandardOnly;}"}}));
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, IngestEmptyUDI) {
  auto run_ingest = [&](BlockBasedTableOptions::IndexMode read_mode,
                        const std::string& suffix) {
    SCOPED_TRACE(suffix);
    BlockBasedTableOptions table_options;
    std::string dbname =
        test::PerThreadDBPath("user_defined_index_test_" + suffix);
    std::string ingest_file = dbname + "test.sst";
    std::string ingest_file2 = dbname + "dummy.sst";

    auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
    table_options.user_defined_index_factory = user_defined_index_factory;
    table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kStandardDefault;
    table_options.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicyFactory>();

    Options write_options = options_;
    write_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    std::unique_ptr<SstFileWriter> writer;
    writer.reset(new SstFileWriter(EnvOptions(), write_options));
    ASSERT_OK(writer->Open(ingest_file));

    auto kvs = generateKVs(/*key_count*/ 100);
    for (const auto& kv : kvs) {
      ASSERT_OK(writer->Put(kv.first, kv.second));
    }
    ASSERT_OK(writer->Finish());
    writer.reset();
    writer.reset(new SstFileWriter(EnvOptions(), write_options));
    ASSERT_OK(writer->Open(ingest_file2));
    ASSERT_OK(writer->Put("dummy", "val"));
    ASSERT_OK(writer->Finish());
    writer.reset();

    table_options.index_mode = read_mode;
    Options read_options = options_;
    read_options.create_if_missing = true;
    read_options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    std::unique_ptr<DB> db;
    Status s = DB::Open(read_options, dbname, &db);
    ASSERT_OK(s);
    ASSERT_TRUE(db != nullptr);
    ColumnFamilyHandle* cfh = nullptr;
    ASSERT_OK(db->CreateColumnFamily(read_options, "new_cf", &cfh));

    std::vector<IngestExternalFileArg> ifa;
    ifa.emplace_back();
    ifa[0].column_family = cfh;
    ifa[0].external_files.emplace_back(ingest_file);
    ifa[0].external_files.emplace_back(ingest_file2);
    s = db->IngestExternalFiles(ifa);
    ASSERT_OK(s);

    ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
    ASSERT_OK(db->Close());
    ASSERT_OK(DestroyDB(dbname, read_options));
  };

  // The one-key file produces a present but zero-size UDI block. Legacy code
  // treated that as "no UDI" and read through the populated standard index,
  // even with fail_if_no_udi_on_open/use_udi_as_primary_index semantics.
  run_ingest(BlockBasedTableOptions::IndexMode::kStandardRequired,
             "standard_required");

  {
    SCOPED_TRACE("custom_default");
    BlockBasedTableOptions table_options;
    std::string dbname =
        test::PerThreadDBPath("user_defined_index_test_custom_default");
    std::string ingest_file = dbname + "dummy.sst";

    auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
    table_options.user_defined_index_factory = user_defined_index_factory;
    table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kStandardDefault;
    table_options.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicyFactory>();

    Options write_options = options_;
    write_options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    std::unique_ptr<SstFileWriter> writer(
        new SstFileWriter(EnvOptions(), write_options));
    ASSERT_OK(writer->Open(ingest_file));
    ASSERT_OK(writer->Put("dummy", "val"));
    ASSERT_OK(writer->Finish());
    writer.reset();

    table_options.index_mode =
        BlockBasedTableOptions::IndexMode::kCustomDefault;
    Options read_options = options_;
    read_options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    std::unique_ptr<SstFileReader> reader(new SstFileReader(read_options));
    ASSERT_OK(reader->Open(ingest_file));
    std::unique_ptr<Iterator> iter(reader->NewIterator(ReadOptions()));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "dummy");
    ASSERT_EQ(iter->value(), "val");
    ASSERT_OK(iter->status());
  }
}

TEST_P(UserDefinedIndexTest, MultiScanFailureTest) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;

  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));

  // Use bigger value, so that prefetch size limit will be effective
  auto kvs = generateKVs(/*key_count*/ 100, /* value_size */ 1024);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  std::unique_ptr<DB> db;
  options_.create_if_missing = true;
  Status s = DB::Open(options_, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options_, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  std::vector<std::string> key_ranges({"key03", "key05", "key12", "key14"});
  ReadOptions ro;
  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  Slice ub;
  ro.iterate_upper_bound = &ub;
  std::unordered_map<std::string, std::string> property_bag;
  property_bag["count"] = std::to_string(5);
  MultiScanArgs scan_options(comparator_);
  if (is_reverse_comparator_) {
    std::reverse(key_ranges.begin(), key_ranges.end());
  }
  scan_options.insert(key_ranges[0], key_ranges[1], property_bag);
  scan_options.insert(key_ranges[2], key_ranges[3], property_bag);
  scan_options.max_prefetch_size = 3500;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  iter->Prepare(scan_options);
  int count = 0;
  ub = key_ranges[1];
  iter->Seek(key_ranges[0]);
  while (iter->status().ok() && iter->Valid()) {
    ASSERT_GE(comparator_->Compare(iter->key(), key_ranges[0]), 0);
    ASSERT_LT(comparator_->Compare(iter->key(), key_ranges[1]), 0);
    count++;
    iter->Next();
  }
  ASSERT_OK(iter->status()) << iter->status().ToString();
  ASSERT_EQ(count, 2);

  ub = key_ranges[3];
  iter->Seek(key_ranges[2]);
  // This should fail due to reaching max_prefetch_size limit
  ASSERT_EQ(iter->status(), Status::Incomplete());
  iter.reset();

  // Empty range multiscan error
  iter.reset(db->NewIterator(ro, cfh));
  scan_options = MultiScanArgs(comparator_);
  iter->Prepare(scan_options);
  ASSERT_EQ(iter->status(), Status::InvalidArgument("Empty MultiScanArgs"));

  // Check no seek key error
  iter.reset(db->NewIterator(ro, cfh));
  scan_options = MultiScanArgs(comparator_);
  scan_options.insert(key_ranges[0], key_ranges[2], property_bag);
  iter->Prepare(scan_options);
  iter->SeekToFirst();
  ASSERT_EQ(iter->status(),
            Status::InvalidArgument("No seek key for MultiScan"));

  // Seek is not allowed to seen a key that is not following the prepare order
  iter.reset(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  scan_options.max_prefetch_size = 0;
  iter->Prepare(scan_options);
  ub = key_ranges[3];
  iter->Seek(key_ranges[2]);
  ASSERT_EQ(
      iter->status(),
      Status::InvalidArgument(
          "Seek target does not match the start of the next prepared range at "
          "index 0"));
  ASSERT_FALSE(iter->Valid());
  iter.reset();

  // limit is equal to start error
  iter.reset(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  (*scan_options).clear();
  scan_options.insert(key_ranges[0], key_ranges[0], property_bag);
  iter->Prepare(scan_options);
  ASSERT_EQ(iter->status(),
            Status::InvalidArgument(
                "Scan start key is large or equal than limit at index 0"));
  iter.reset();

  // overlapping ranges error
  iter.reset(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  (*scan_options).clear();
  scan_options.insert(key_ranges[0], key_ranges[2], property_bag);
  scan_options.insert(key_ranges[1], key_ranges[3], property_bag);
  iter->Prepare(scan_options);
  ASSERT_EQ(iter->status(),
            Status::InvalidArgument("Overlapping ranges at index 1"));
  iter.reset();

  // Validate an error is returned if upper bound is not set to the same value
  // as limit
  iter.reset(db->NewIterator(ro, cfh));
  scan_options = MultiScanArgs(comparator_);
  scan_options.insert(key_ranges[0], key_ranges[1], property_bag);
  iter->Prepare(scan_options);
  ub = "";
  iter->Seek(key_ranges[0]);
  ASSERT_EQ(iter->status(),
            Status::InvalidArgument(
                "Upper bound is not set to the same limit value of the next "
                "prepared range at index 0"));
  ASSERT_FALSE(iter->Valid());

  // Validate an error is returned when seek more keys than prepared
  iter.reset(db->NewIterator(ro, cfh));
  scan_options = MultiScanArgs(comparator_);
  scan_options.insert(key_ranges[0], key_ranges[1], property_bag);
  iter->Prepare(scan_options);
  ub = key_ranges[1];
  iter->Seek(key_ranges[0]);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  iter->Seek(key_ranges[2]);
  ASSERT_EQ(iter->status(),
            Status::InvalidArgument(
                "Seek called after exhausting all of the scan ranges"));
  ASSERT_FALSE(iter->Valid());
  iter.reset();

  // Check error is returned if upper bound is not set and limit is set
  ro.iterate_upper_bound = nullptr;
  iter.reset(db->NewIterator(ro, cfh));
  scan_options = MultiScanArgs(comparator_);
  scan_options.insert(key_ranges[0], key_ranges[1], property_bag);
  iter->Prepare(scan_options);
  iter->Seek(key_ranges[0]);
  ASSERT_EQ(iter->status(),
            Status::InvalidArgument(
                "Upper bound is not set to the same limit value of the next "
                "prepared range at index 0"));
  ASSERT_FALSE(iter->Valid());
  iter.reset();

  // Upper bound is allowed to be empty, if limit is not set
  ro.iterate_upper_bound = nullptr;
  iter.reset(db->NewIterator(ro, cfh));
  scan_options = MultiScanArgs(comparator_);
  scan_options.insert(key_ranges[0], property_bag);
  iter->Prepare(scan_options);
  iter->Seek(key_ranges[0]);
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, ConfigTest) {
  BlockBasedTableOptions table_options;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;

  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::unique_ptr<SstFileWriter> writer;
  writer.reset(new SstFileWriter(EnvOptions(), options_));
  ASSERT_OK(writer->Open(ingest_file));

  auto kvs = generateKVs(/*key_count*/ 100);
  for (const auto& kv : kvs) {
    ASSERT_OK(writer->Put(kv.first, kv.second));
  }
  ASSERT_OK(writer->Finish());
  writer.reset();

  table_options.user_defined_index_factory.reset();
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  // Set up the user-defined index factory
  ObjectLibrary::Default().get()->AddFactory<IndexFactory>(
      "test_index",
      [](const std::string& /* uri */, std::unique_ptr<IndexFactory>* guard,
         std::string* /* errmsg */) {
        auto factory = new TestIndexFactory();
        guard->reset(factory);
        return guard->get();
      });
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      ConfigOptions(), options_,
      "block_based_table_factory={user_defined_index_factory=test_index;}",
      &options_));

  std::unique_ptr<DB> db;
  options_.create_if_missing = true;
  Status s = DB::Open(options_, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options_, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  s = db->IngestExternalFile(cfh, {ingest_file}, ifo);
  ASSERT_OK(s);

  ReadOptions ro;
  Slice ub;
  ro.iterate_upper_bound = &ub;
  ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  MultiScanArgs scan_opts(options_.comparator);
  std::unordered_map<std::string, std::string> property_bag;
  property_bag["count"] = std::to_string(25);

  std::vector<std::string> boundaries = {"key10", "key50"};
  if (is_reverse_comparator_) {
    std::reverse(boundaries.begin(), boundaries.end());
  }

  scan_opts.insert(boundaries[0], boundaries[1], std::optional(property_bag));
  iter->Prepare(scan_opts);
  // Test that UDI is used to help fetch the number of keys
  ub = boundaries[1];
  int key_count = 0;
  for (iter->Seek(scan_opts.GetScanRanges()[0].range.start.value());
       iter->Valid(); iter->Next()) {
    key_count++;
  }
  // Number of blocks prepared is based on UDI, it would be slightly higher than
  // the limit
  // The index may undercount by 2 blocks
  ASSERT_EQ(key_count, 29);
  ASSERT_OK(iter->status());
  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, RangeDelete) {
  BlockBasedTableOptions table_options;
  options_.num_levels = 50;
  options_.compaction_style = kCompactionStyleUniversal;
  options_.disable_auto_compactions = true;
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;

  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  auto create_ingestion_data_file = [&](const std::string& filename) {
    std::unique_ptr<SstFileWriter> writer;
    writer.reset(new SstFileWriter(EnvOptions(), options_));
    ASSERT_OK(writer->Open(filename));
    auto kvs = generateKVs(100);

    for (const auto& kv : kvs) {
      ASSERT_OK(writer->Put(kv.first, kv.second));
    }
    ASSERT_OK(writer->Finish());
    writer.reset();
  };

  // Create first ingestion file with data
  create_ingestion_data_file(ingest_file + "_0");

  // Create second ingestion file with range delete only that covers the first
  // file to delete all of its keys.
  {
    std::unique_ptr<SstFileWriter> writer;
    writer.reset(new SstFileWriter(EnvOptions(), options_));
    ASSERT_OK(writer->Open(ingest_file + "_1"));
    if (is_reverse_comparator_) {
      ASSERT_OK(writer->DeleteRange("keyz", "key"));
    } else {
      ASSERT_OK(writer->DeleteRange("key", "keyz"));
    }
    ASSERT_OK(writer->Finish());
    writer.reset();
  }

  // Create the second ingestion file with data
  create_ingestion_data_file(ingest_file + "_2");

  std::unique_ptr<DB> db;
  options_.create_if_missing = true;
  Status s = DB::Open(options_, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options_, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  // ingest first data file key00~key99
  s = db->IngestExternalFile(cfh, {ingest_file + "_0"}, ifo);
  ASSERT_OK(s);
  // ingest delete range (key-keyz) and new data file (key00-key99) together
  s = db->IngestExternalFile(cfh, {ingest_file + "_1", ingest_file + "_2"},
                             ifo);
  ASSERT_OK(s);

  std::vector<Slice> range = {
      Slice("key10"),
      Slice("key25"),
      Slice("key80"),
      Slice("key95"),
  };

  if (is_reverse_comparator_) {
    std::reverse(range.begin(), range.end());
  }

  Slice ub("");
  ReadOptions ro;
  ro.iterate_upper_bound = &ub;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);

  MultiScanArgs scan_opts(options_.comparator);
  std::unordered_map<std::string, std::string> property_bag;
  property_bag["count"] = std::to_string(9);

  std::vector<std::vector<char>> decoded_ranges;
  for (size_t i = 0; i < range.size() / 2; i++) {
    scan_opts.insert(range[i * 2], range[i * 2 + 1],
                     std::optional(property_bag));
  }
  iter->Prepare(scan_opts);

  for (size_t i = 0; i < range.size() / 2; i++) {
    // Update upper bound before each seek
    ub = range[2 * i + 1];
    auto key_count = 0;
    for (iter->Seek(range[i * 2]); iter->Valid(); iter->Next()) {
      key_count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(key_count, 15);
  }

  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

TEST_P(UserDefinedIndexTest, QueryCrossTwoFiles) {
  BlockBasedTableOptions table_options;
  options_.num_levels = 50;
  options_.compaction_style = kCompactionStyleUniversal;
  options_.disable_auto_compactions = true;
  options_.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(4);
  std::string dbname = test::PerThreadDBPath("user_defined_index_test");
  std::string ingest_file = dbname + "test.sst";

  // Set up the user-defined index factory
  auto user_defined_index_factory = std::make_shared<TestIndexFactory>();
  table_options.user_defined_index_factory = user_defined_index_factory;
  table_options.index_mode =
      BlockBasedTableOptions::IndexMode::kStandardDefault;

  // Set up custom flush block policy that flushes every 3 keys
  table_options.flush_block_policy_factory =
      std::make_shared<CustomFlushBlockPolicyFactory>();

  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

  auto create_ingestion_data_file = [&](const std::string& filename,
                                        const std::string& value) {
    std::unique_ptr<SstFileWriter> writer;
    writer.reset(new SstFileWriter(EnvOptions(), options_));
    ASSERT_OK(writer->Open(filename));
    auto kvs = generateKVWithValue(100, value);

    for (const auto& kv : kvs) {
      ASSERT_OK(writer->Put(kv.first, kv.second));
    }
    ASSERT_OK(writer->Finish());
    writer.reset();
  };

  // Create first ingestion file with data
  create_ingestion_data_file(ingest_file + "_0", "old");

  std::unique_ptr<DB> db;
  options_.create_if_missing = true;
  Status s = DB::Open(options_, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db->CreateColumnFamily(options_, "new_cf", &cfh));

  IngestExternalFileOptions ifo;
  // ingest data file key00~key99
  s = db->IngestExternalFile(cfh, {ingest_file + "_0"}, ifo);
  ASSERT_OK(s);

  // Compact the file with SST partitioner, so that files are split into
  // multiple ones
  s = db->CompactRange(
      {.exclusive_manual_compaction = true,
       .bottommost_level_compaction = BottommostLevelCompaction::kForce},
      cfh, nullptr, nullptr);
  ASSERT_OK(s);

  std::vector<Slice> range = {
      // Each range span across 2 files
      Slice("key16"),
      Slice("key24"),
      Slice("key26"),
      Slice("key34"),
  };

  if (is_reverse_comparator_) {
    std::reverse(range.begin(), range.end());
  }

  Slice ub("");
  ReadOptions ro;
  ro.iterate_upper_bound = &ub;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);

  MultiScanArgs scan_opts(options_.comparator);
  std::unordered_map<std::string, std::string> property_bag;
  auto read_key_per_range_limit = 2;
  property_bag["count"] = std::to_string(read_key_per_range_limit);

  for (size_t i = 0; i < range.size() / 2; i++) {
    scan_opts.insert(range[i * 2], range[i * 2 + 1],
                     std::optional(property_bag));
  }
  iter->Prepare(scan_opts);

  for (size_t i = 0; i < range.size() / 2; i++) {
    // Update upper bound before each seek
    ub = range[2 * i + 1];
    auto key_count = 0;
    for (iter->Seek(range[i * 2]); iter->Valid(); iter->Next()) {
      key_count++;
      ASSERT_EQ(iter->value(), "old");
      if (key_count >= read_key_per_range_limit) {
        break;
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(key_count, read_key_per_range_limit);
  }

  // Create another ingestion file with range delete only that covers the first
  // file to delete all of its keys.
  {
    std::unique_ptr<SstFileWriter> writer;
    writer.reset(new SstFileWriter(EnvOptions(), options_));
    ASSERT_OK(writer->Open(ingest_file + "_1"));
    if (is_reverse_comparator_) {
      ASSERT_OK(writer->DeleteRange("keyz", "key"));
    } else {
      ASSERT_OK(writer->DeleteRange("key", "keyz"));
    }
    ASSERT_OK(writer->Finish());
    writer.reset();
  }
  s = db->IngestExternalFile(cfh, {ingest_file + "_1"}, ifo);
  ASSERT_OK(s);

  // ingest new data
  create_ingestion_data_file(ingest_file + "_2", "new");
  s = db->IngestExternalFile(cfh, {ingest_file + "_2"}, ifo);
  ASSERT_OK(s);

  iter.reset(db->NewIterator(ro, cfh));
  ASSERT_NE(iter, nullptr);
  ASSERT_OK(iter->status());

  iter->Prepare(scan_opts);

  for (size_t i = 0; i < range.size() / 2; i++) {
    // Update upper bound before each seek
    ub = range[2 * i + 1];
    auto key_count = 0;
    for (iter->Seek(range[i * 2]); iter->Valid(); iter->Next()) {
      key_count++;
      ASSERT_EQ(iter->value(), "new");
      if (key_count >= read_key_per_range_limit) {
        break;
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(key_count, read_key_per_range_limit);
  }

  iter.reset();

  ASSERT_OK(db->DestroyColumnFamilyHandle(cfh));
  ASSERT_OK(db->Close());
  ASSERT_OK(DestroyDB(dbname, options_));
}

INSTANTIATE_TEST_CASE_P(UserDefinedIndexTest, UserDefinedIndexTest,
                        ::testing::Values(BytewiseComparator(),
                                          ReverseBytewiseComparator()));

struct UserDefinedIndexStressTestParam {
  const Comparator* comparator;
  bool enable_udi;
  bool enable_compaction_with_sst_partitioner;
  bool reverse_scan;

  using UserDefinedIndexStressTestTuple =
      std::tuple<const Comparator*, bool, bool, bool>;

  UserDefinedIndexStressTestParam(const UserDefinedIndexStressTestTuple& tuple)
      : comparator(std::get<0>(tuple)),
        enable_udi(std::get<1>(tuple)),
        enable_compaction_with_sst_partitioner(std::get<2>(tuple)),
        reverse_scan(std::get<3>(tuple)) {}
};

std::ostream& operator<<(std::ostream& os,
                         const UserDefinedIndexStressTestParam& param) {
  return os << "UserDefinedIndexStressTestParam{comparator="
            << (param.comparator ? param.comparator->Name() : "nullptr")
            << ", enable_udi=" << param.enable_udi
            << ", enable_compaction_with_sst_partitioner="
            << param.enable_compaction_with_sst_partitioner
            << ", reverse_scan=" << param.reverse_scan << "}";
}

struct DataRange {
  size_t start;  // inclusive
  size_t end;    // exclusive
  std::string value;
  bool is_range_delete;
  bool skipped;
  size_t scan_key_count_limit;
  std::string start_key;
  std::string end_key;

  // print the range in human readable format
  std::string ToString() const {
    std::ostringstream oss;
    oss << "[" << start << ", " << end << "), value: " << value
        << ", is_range_delete: " << is_range_delete << ", skipped: " << skipped
        << ", scan_key_count_limit: " << scan_key_count_limit
        << ", start_key: " << start_key << ", end_key: " << end_key;
    return oss.str();
  }
};
class UserDefinedIndexStressTest
    : public UserDefinedIndexTestBase,
      public testing::WithParamInterface<
          UserDefinedIndexStressTestParam::UserDefinedIndexStressTestTuple> {
 public:
  void SetUp() override {
    rand_seed_ = static_cast<uint32_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());

    std::cout << "Random seed: " << rand_seed_ << std::endl;

    rnd = Random(rand_seed_);
    UserDefinedIndexStressTestParam param = GetParam();
    comparator_ = param.comparator;
    enable_udi_ = param.enable_udi;
    enable_compaction_with_sst_partitioner_ =
        param.enable_compaction_with_sst_partitioner;
    reverse_scan_ = param.reverse_scan;
    options_.comparator = comparator_;
    is_reverse_comparator_ = comparator_ == ReverseBytewiseComparator();
    options_.compaction_style = kCompactionStyleUniversal;

    // Set up custom flush block policy that flushes every 3 keys
    table_options_.flush_block_policy_factory =
        std::make_shared<CustomFlushBlockPolicyFactory>();

    options_.table_factory.reset(NewBlockBasedTableFactory(table_options_));
  }

  void TearDown() override {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(ingest_cfh_));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(regular_cfh_));

    ASSERT_OK(db_->Close());
    ASSERT_OK(DestroyDB(dbname_, options_));
  }

 protected:
  static constexpr auto kKeyRange = 100;
  bool enable_udi_{};
  bool enable_compaction_with_sst_partitioner_{};
  bool reverse_scan_{};
  uint32_t rand_seed_{};
  std::shared_ptr<IndexFactory> user_defined_index_factory_;
  BlockBasedTableOptions table_options_;
  const Comparator* comparator_{};
  bool is_reverse_comparator_{};
  Random rnd{0};
  ColumnFamilyHandle* ingest_cfh_ = nullptr;
  ColumnFamilyHandle* regular_cfh_ = nullptr;
  std::unique_ptr<DB> db_;
  std::vector<std::vector<DataRange>> ranges_in_levels_;
  std::string dbname_;

  void SetupDB(const std::string& dbname) {
    options_.create_if_missing = true;
    options_.disable_auto_compactions = true;
    Status s = DB::Open(options_, dbname, &db_);
    ASSERT_OK(s);
    ASSERT_TRUE(db_ != nullptr);
    if (enable_compaction_with_sst_partitioner_) {
      // Use a SST partitioner to create multiple files, use the first 4 bytes
      // of key to partition the file, The key is formatted with 2 digit
      // following "key" string, e.g. key01, key99
      options_.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(4);
    }

    ASSERT_OK(db_->CreateColumnFamily(options_, "regular_cf", &regular_cfh_));

    if (enable_udi_) {
      // Set up the user-defined index factory
      user_defined_index_factory_ = std::make_shared<TestIndexFactory>();
      table_options_.user_defined_index_factory = user_defined_index_factory_;
      table_options_.index_mode =
          BlockBasedTableOptions::IndexMode::kStandardDefault;
    }

    options_.table_factory.reset(NewBlockBasedTableFactory(table_options_));
    ASSERT_OK(db_->CreateColumnFamily(options_, "ingest_cf", &ingest_cfh_));
  }

  template <typename T>
  std::string FormatKey(T i) {
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << i;
    return "key" + ss.str();
  }

  std::vector<DataRange> GenerateKeyRanges(size_t range_count,
                                           int skip_range_count,
                                           const std::string& value) {
    std::set<size_t> boundaries;
    // generate n + 1 number of unique boundaries to form n contiguoes ranges
    while (boundaries.size() < range_count + 1) {
      boundaries.insert(rnd.Uniform(kKeyRange));
    }
    std::vector<size_t> sorted_boundaries(boundaries.begin(), boundaries.end());
    if (is_reverse_comparator_) {
      std::reverse(sorted_boundaries.begin(), sorted_boundaries.end());
    }
    auto ranges = std::vector<DataRange>();
    std::optional<size_t> prev_bound;
    for (auto it = sorted_boundaries.begin(); it != sorted_boundaries.end();
         it++) {
      if (prev_bound.has_value()) {
        ranges.push_back({.start = prev_bound.value(),
                          .end = *it,
                          .value = value,
                          .is_range_delete = rnd.OneIn(6),
                          .skipped = false,
                          .scan_key_count_limit = rnd.Uniform(10) + 1,
                          .start_key = FormatKey(prev_bound.value()),
                          .end_key = FormatKey(*it)});
      }
      prev_bound = *it;
    }
    // skipped some of them
    for (int j = 0; j < skip_range_count; j++) {
      ranges[rnd.Uniform(static_cast<uint32_t>(range_count))].skipped = true;
    }

    if (kVerbose) {
      for (auto const& range : ranges) {
        std::cout << range.ToString() << std::endl;
      }
    }

    return ranges;
  }

  void CreateSstFileWithRanges(const std::string& ingest_file,
                               const std::vector<DataRange>& ranges,
                               bool& data_added) {
    std::unique_ptr<SstFileWriter> writer;

    data_added = false;

    std::vector<DataRange> ranges_in_file;

    for (auto const& range : ranges) {
      assert(range.start != range.end);
      if (range.skipped) {
        continue;
      }

      if (writer == nullptr) {
        // lazy create writer until there is data to be written to avoid
        // unchecked status error
        writer = std::make_unique<SstFileWriter>(EnvOptions(), options_);
        ASSERT_OK(writer->Open(ingest_file));
      }

      ranges_in_file.push_back(range);

      data_added = true;

      if (range.is_range_delete) {
        ASSERT_OK(writer->DeleteRange(range.start_key, range.end_key));
      } else {
        for (size_t i = range.start; i != range.end;) {
          auto key = FormatKey(i);
          range.start < range.end ? i++ : i--;
          ASSERT_OK(writer->Put(key, range.value));
        }
      }
    }
    if (kVerbose) {
      std::cout << "Ingested file: " + ingest_file + "; Range: {" << std::endl;
      for (const auto& range : ranges_in_file) {
        std::cout << "    " << range.ToString() << "," << std::endl;
      }
      std::cout << "}" << std::endl;
    }
    if (data_added) {
      ASSERT_OK(writer->Finish());
    }
  }

  void RangeScan(std::unique_ptr<Iterator>& iter,
                 const std::vector<DataRange>& ranges, Slice& lower_bound,
                 Slice& upper_bound,
                 std::vector<std::pair<std::string, std::string>>& result,
                 bool use_multi_scan) {
    ASSERT_NE(iter, nullptr);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(!ranges.empty());

    MultiScanArgs scan_opts(options_.comparator);
    scan_opts.reverse = reverse_scan_;
    std::unordered_map<std::string, std::string> property_bag;
    if (use_multi_scan) {
      for (auto const& range : ranges) {
        if (range.skipped) {
          continue;
        }
        property_bag["count"] = std::to_string(range.scan_key_count_limit);
        scan_opts.insert(range.start_key, range.end_key, property_bag);
        // print range start end key
        if (kVerbose) {
          std::cout << "range start " << range.start_key << " end "
                    << range.end_key << std::endl;
        }
      }
      iter->Prepare(scan_opts);
      ASSERT_OK(iter->status());
    }

    for (auto const& range : ranges) {
      if (range.skipped) {
        continue;
      }
      size_t scan_key_count = 0;
      lower_bound = range.start_key;
      upper_bound = range.end_key;
      if (reverse_scan_) {
        if (kVerbose) {
          std::cout << "seek for prev key " << range.end_key << std::endl;
        }
        for (iter->SeekForPrev(range.end_key);
             iter->Valid() && scan_key_count < range.scan_key_count_limit;
             iter->Prev()) {
          if (kVerbose) {
            std::cout << "key " << iter->key().ToString() << " value "
                      << iter->value().ToString() << std::endl;
          }
          result.emplace_back(iter->key().ToString(), iter->value().ToString());
          scan_key_count++;
        }
      } else {
        if (kVerbose) {
          std::cout << "seek key " << range.start_key << std::endl;
        }
        for (iter->Seek(range.start_key);
             iter->Valid() && scan_key_count < range.scan_key_count_limit;
             iter->Next()) {
          if (kVerbose) {
            std::cout << "key " << iter->key().ToString() << " value "
                      << iter->value().ToString() << std::endl;
          }
          result.emplace_back(iter->key().ToString(), iter->value().ToString());
          scan_key_count++;
        }
      }
      ASSERT_OK(iter->status());
    }
  }

  void AddDataToRegularCF() {
    for (auto const& ranges_in_level : ranges_in_levels_) {
      for (auto const& range : ranges_in_level) {
        if (!range.skipped) {
          for (auto i = range.start; i != range.end;
               range.start < range.end ? i++ : i--) {
            if (range.is_range_delete) {
              ASSERT_OK(
                  db_->Delete(WriteOptions(), regular_cfh_, FormatKey(i)));
            } else {
              ASSERT_OK(db_->Put(WriteOptions(), regular_cfh_, FormatKey(i),
                                 range.value));
            }
          }
        }
      }
    }
    ASSERT_OK(db_->Flush(FlushOptions(), regular_cfh_));
  }

  void ValidateQueryResult() {
    // Query both CF with same range scan and validate result are same
    for (auto i = 0; i < 200; i++) {
      if (kVerbose) {
        std::cout << "iteration " << i << std::endl;
      }
      SCOPED_TRACE("Iteration " + std::to_string(i));
      // randomly generate 1 to 3 ranges
      auto ranges = GenerateKeyRanges(rnd.Uniform(3) + 4, 2, "");

      // Query regular CF
      std::vector<std::pair<std::string, std::string>> expected_result;
      Slice lower_bound("");
      Slice upper_bound("");
      ReadOptions ro;
      if (reverse_scan_) {
        ro.iterate_lower_bound = &lower_bound;
      }
      ro.iterate_upper_bound = &upper_bound;

      std::unique_ptr<Iterator> iter(db_->NewIterator(ro, regular_cfh_));
      ASSERT_NO_FATAL_FAILURE(RangeScan(iter, ranges, lower_bound, upper_bound,
                                        expected_result, false));
      ASSERT_OK(iter->status());

      // Query ingest CF
      iter.reset(db_->NewIterator(ro, ingest_cfh_));
      std::vector<std::pair<std::string, std::string>> ingest_cf_result;
      ASSERT_NO_FATAL_FAILURE(RangeScan(iter, ranges, lower_bound, upper_bound,
                                        ingest_cf_result, false));

      ASSERT_EQ(expected_result, ingest_cf_result);
      ASSERT_OK(iter->status());

      // Query ingest CF with UDI if it is enabled.
      if (enable_udi_ && reverse_scan_) {
        ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
        iter.reset(db_->NewIterator(ro, ingest_cfh_));
        std::vector<std::pair<std::string, std::string>> ingest_cf_udi_result;
        ASSERT_NO_FATAL_FAILURE(RangeScan(iter, ranges, lower_bound,
                                          upper_bound, ingest_cf_udi_result,
                                          false));
        ASSERT_EQ(expected_result, ingest_cf_udi_result);
        ASSERT_OK(iter->status());
      }

      if (!reverse_scan_) {
        if (enable_udi_) {
          ro.read_index = ReadOptions::ReadIndex::kPreferCustom;
        }
        iter.reset(db_->NewIterator(ro, ingest_cfh_));
        std::vector<std::pair<std::string, std::string>>
            ingest_cf_multi_scan_result;
        ASSERT_NO_FATAL_FAILURE(RangeScan(iter, ranges, lower_bound,
                                          upper_bound,
                                          ingest_cf_multi_scan_result, true));
        ASSERT_EQ(expected_result, ingest_cf_multi_scan_result);
        ASSERT_OK(iter->status());
      }
    }
  }

  void IngestFilesInOneLevel(const std::vector<DataRange>& ranges_in_level,
                             const std::string& ingest_file_name_prefix,
                             size_t& ingest_file_count,
                             const IngestExternalFileOptions& ifo,
                             bool combine_ranges = false) {
    // Generate SST file and bulk load them one level at a time
    std::vector<std::string> ingest_files;
    if (combine_ranges) {
      size_t i = 0;
      while (i < ranges_in_level.size()) {
        // if combine ranges, generate 1 SST file that combines muliple ranges
        // together
        // Randomly combine ranges to SST file.
        size_t batch_end_idx =
            std::min(i + rnd.Uniform(3) + 2, ranges_in_level.size());
        bool data_added = false;
        ASSERT_NO_FATAL_FAILURE(CreateSstFileWithRanges(
            ingest_file_name_prefix + std::to_string(ingest_file_count),
            {ranges_in_level.begin() + i,
             ranges_in_level.begin() + batch_end_idx},
            data_added));
        if (data_added) {
          ingest_files.push_back(ingest_file_name_prefix +
                                 std::to_string(ingest_file_count));
          ingest_file_count++;
        }
        i = batch_end_idx;
      }
    } else {
      for (auto const& range : ranges_in_level) {
        if (!range.skipped) {
          bool data_added = false;
          ASSERT_NO_FATAL_FAILURE(CreateSstFileWithRanges(
              ingest_file_name_prefix + std::to_string(ingest_file_count),
              {range}, data_added));
          ASSERT_TRUE(data_added);
          ingest_files.push_back(ingest_file_name_prefix +
                                 std::to_string(ingest_file_count));
          ingest_file_count++;
        }
      }
    }

    ASSERT_OK(db_->IngestExternalFile(ingest_cfh_, ingest_files, ifo));
  }

  void IngestDataToCF() {
    IngestExternalFileOptions ifo;
    ifo.snapshot_consistency = false;
    auto ingest_file_name_prefix = dbname_ + "ingest_file_";
    size_t ingest_file_count = 0;
    for (auto const& ranges_in_level : ranges_in_levels_) {
      ASSERT_NO_FATAL_FAILURE(IngestFilesInOneLevel(
          ranges_in_level, ingest_file_name_prefix, ingest_file_count, ifo));
    }

    ASSERT_GE(ingest_file_count, 0);
  }

  void CompactIngestedCF() {
    auto s = db_->CompactRange(
        {.exclusive_manual_compaction = true,
         .bottommost_level_compaction = BottommostLevelCompaction::kForce},
        ingest_cfh_, nullptr, nullptr);
    ASSERT_OK(s);
  }
};

TEST_P(UserDefinedIndexStressTest, PartialDeleteRange) {
  // Create 2 column families. One use normal put/del, the other uses sst
  // ingest Randomly generate multiple non overlapping range for multiple
  // levels Range scan same range between the 2 CF and validate the result is
  // same
  SCOPED_TRACE("Start with random seed: " + std::to_string(rand_seed_));
  dbname_ =
      test::PerThreadDBPath("UserDefinedIndexStressTest_PartialDeleteRange");
  SCOPED_TRACE("dbname: " + dbname_);
  ASSERT_NO_FATAL_FAILURE(SetupDB(dbname_));

  if (enable_udi_) {
    // Skip UDI for now.
    // The issue is that with UDI enabled, prepare might not prepare enough keys
    // at lower level due to range delete from upper level.
    // E.g. consider a LSM tree:
    // L1: Data         [0-1]
    // L2: Delete Range [0-6]
    // L3: Data         [0-9]
    // When multiscan queries range [0-9) with UDI count as 3, the L3 file
    // will only prepare range [0-3). However, this range is masked out by upper
    // layer delete range from [0-6] from L2. This causes query to only return
    // [0,1], while [0,1,7] is the right result. Until prepare is able to
    // preparing additional block supported, UDI is skipped.
    return;
  }

  for (int i = 0; i < 5; i++) {
    ranges_in_levels_.push_back(
        GenerateKeyRanges(rnd.Uniform(3) + 4, 2,
                          "L" + std::to_string(options_.num_levels - 1 - i)));
  }

  ASSERT_NO_FATAL_FAILURE(IngestDataToCF());

  if (enable_compaction_with_sst_partitioner_) {
    ASSERT_NO_FATAL_FAILURE(CompactIngestedCF());
  }

  ASSERT_NO_FATAL_FAILURE(AddDataToRegularCF());

  ASSERT_NO_FATAL_FAILURE(ValidateQueryResult());
}

TEST_P(UserDefinedIndexStressTest, DeleteRangeMixedWithDataFile) {
  // Create 2 column families. One use normal put/del, the other uses sst
  // ingest.
  // Test the case where there are 3 levels, the middle level is a delete
  // range file that span across the entire key space. The top and bottom level
  // file have multiple files and each one has both data and delete range. Scan
  // same range between the 2 CF and validate the result is same
  SCOPED_TRACE("Start with random seed: " + std::to_string(rand_seed_));
  dbname_ = test::PerThreadDBPath(
      "UserDefinedIndexStressTest_DeleteRangeMixedWithDataFile");
  SCOPED_TRACE("dbname: " + dbname_);
  ASSERT_NO_FATAL_FAILURE(SetupDB(dbname_));

  // Test 3 levels.
  // Bottom level is mixed data with delete range.
  ranges_in_levels_.push_back(GenerateKeyRanges(rnd.Uniform(3) + 6, 2, "L6"));
  // Middle level delete range across entire key space.
  if (is_reverse_comparator_) {
    ranges_in_levels_.push_back({{.start = 100,
                                  .end = 0,
                                  .is_range_delete = true,
                                  .skipped = false,
                                  .start_key = "keyz",
                                  .end_key = "key"}});
  } else {
    ranges_in_levels_.push_back({{.start = 0,
                                  .end = 100,
                                  .is_range_delete = true,
                                  .skipped = false,
                                  .start_key = "key",
                                  .end_key = "keyz"}});
  }

  // Top level is mixed data with delete range.
  ranges_in_levels_.push_back(GenerateKeyRanges(rnd.Uniform(3) + 6, 2, "L4"));

  IngestExternalFileOptions ifo;
  ifo.snapshot_consistency = false;
  auto ingest_file_name_prefix = dbname_ + "ingest_file_";
  size_t ingest_file_count = 0;
  auto first_level = true;
  for (auto const& ranges_in_level : ranges_in_levels_) {
    ASSERT_NO_FATAL_FAILURE(
        IngestFilesInOneLevel(ranges_in_level, ingest_file_name_prefix,
                              ingest_file_count, ifo, /*combine_ranges=*/true));
    if (first_level) {
      first_level = false;
      if (enable_compaction_with_sst_partitioner_) {
        // When compaction is enabled, do a compaction at the first level
        ASSERT_NO_FATAL_FAILURE(CompactIngestedCF());
      }
    }
  }

  ASSERT_NO_FATAL_FAILURE(AddDataToRegularCF());

  ASSERT_NO_FATAL_FAILURE(ValidateQueryResult());
}

TEST_P(UserDefinedIndexStressTest, DeleteRange) {
  // Create 2 column families. One use normal put/del, the other uses sst
  // ingest.
  // Test the case where there are 3 levels, the middle level is a delete
  // range file that span across the entire key space. Range scan same range
  // between the 2 CF and validate the result is same
  SCOPED_TRACE("Start with random seed: " + std::to_string(rand_seed_));
  dbname_ = test::PerThreadDBPath("UserDefinedIndexStressTest_DeleteRange");
  SCOPED_TRACE("dbname: " + dbname_);
  ASSERT_NO_FATAL_FAILURE(SetupDB(dbname_));

  // Test 3 levels.
  // bottom level constains multiple files, each could have data or delete
  // ranges or both.
  ranges_in_levels_.push_back(GenerateKeyRanges(rnd.Uniform(3) + 4, 2, "L6"));
  // middle level delete range across entire key space
  if (is_reverse_comparator_) {
    ranges_in_levels_.push_back({{.start = 100,
                                  .end = 0,
                                  .is_range_delete = true,
                                  .skipped = false,
                                  .start_key = "keyz",
                                  .end_key = "key"}});
  } else {
    ranges_in_levels_.push_back({{.start = 0,
                                  .end = 100,
                                  .is_range_delete = true,
                                  .skipped = false,
                                  .start_key = "key",
                                  .end_key = "keyz"}});
  }
  // Top level constains multiple files, each could have data or delete
  // ranges or both.
  ranges_in_levels_.push_back(GenerateKeyRanges(rnd.Uniform(3) + 4, 2, "L4"));

  IngestExternalFileOptions ifo;
  ifo.snapshot_consistency = false;
  auto ingest_file_name_prefix = dbname_ + "ingest_file_";
  size_t ingest_file_count = 0;
  auto first_level = true;
  for (auto const& ranges_in_level : ranges_in_levels_) {
    ASSERT_NO_FATAL_FAILURE(IngestFilesInOneLevel(
        ranges_in_level, ingest_file_name_prefix, ingest_file_count, ifo));
    if (first_level) {
      first_level = false;
      if (enable_compaction_with_sst_partitioner_) {
        // When compaction is enabled, do a compaction at the first level
        ASSERT_NO_FATAL_FAILURE(CompactIngestedCF());
      }
    }
  }

  ASSERT_NO_FATAL_FAILURE(AddDataToRegularCF());

  ASSERT_NO_FATAL_FAILURE(ValidateQueryResult());
}

TEST_P(UserDefinedIndexStressTest, AtomicReplaceBulkLoad) {
  // Create 2 column families. One use normal put/del, the other uses SST
  // ingest. The SST ingest uses atomic range replace.
  SCOPED_TRACE("Start with random seed: " + std::to_string(rand_seed_));
  dbname_ =
      test::PerThreadDBPath("UserDefinedIndexStressTest_AtomicReplaceBulkLoad");
  SCOPED_TRACE("dbname: " + dbname_);
  ASSERT_NO_FATAL_FAILURE(SetupDB(dbname_));

  // Test 3 levels.
  // bottom level constains multiple files, each could have data or delete
  // ranges or both.
  ranges_in_levels_.push_back(GenerateKeyRanges(rnd.Uniform(3) + 4, 2, "L6"));
  // middle level delete range across entire key space
  if (is_reverse_comparator_) {
    ranges_in_levels_.push_back({{.start = 100,
                                  .end = 0,
                                  .is_range_delete = true,
                                  .skipped = false,
                                  .start_key = "keyz",
                                  .end_key = "key"}});
  } else {
    ranges_in_levels_.push_back({{.start = 0,
                                  .end = 100,
                                  .is_range_delete = true,
                                  .skipped = false,
                                  .start_key = "key",
                                  .end_key = "keyz"}});
  }
  // Top level constains multiple files, each could have data or delete
  // ranges or both.
  ranges_in_levels_.push_back(GenerateKeyRanges(rnd.Uniform(3) + 4, 2, "L4"));

  IngestExternalFileOptions ifo;
  ifo.snapshot_consistency = false;
  auto ingest_file_name_prefix = dbname_ + "ingest_file_";
  size_t ingest_file_count = 0;
  auto first_level = true;
  for (auto const& ranges_in_level : ranges_in_levels_) {
    ASSERT_NO_FATAL_FAILURE(IngestFilesInOneLevel(
        ranges_in_level, ingest_file_name_prefix, ingest_file_count, ifo));
    if (first_level) {
      first_level = false;
      if (enable_compaction_with_sst_partitioner_) {
        // When compaction is enabled, do a compaction at the first level
        ASSERT_NO_FATAL_FAILURE(CompactIngestedCF());
      }
    }
  }

  // Ingest the a new file with atomic replace with full key space, this layer
  // is exactly same as the one at the top level
  bool data_added;
  ASSERT_NO_FATAL_FAILURE(CreateSstFileWithRanges(
      ingest_file_name_prefix + std::to_string(++ingest_file_count),
      ranges_in_levels_[2], data_added));

  IngestExternalFileArg ingest_arg;
  ingest_arg.column_family = ingest_cfh_;
  ingest_arg.options = ifo;
  ingest_arg.external_files.push_back(ingest_file_name_prefix +
                                      std::to_string(ingest_file_count));
  ingest_arg.atomic_replace_range = RangeOpt(nullptr, nullptr);

  ASSERT_OK(db_->IngestExternalFiles(
      std::vector<IngestExternalFileArg>({ingest_arg})));

  ASSERT_NO_FATAL_FAILURE(AddDataToRegularCF());

  ASSERT_NO_FATAL_FAILURE(ValidateQueryResult());
}

INSTANTIATE_TEST_CASE_P(
    UserDefinedIndexStressTest, UserDefinedIndexStressTest,
    testing::Combine(testing::Values(BytewiseComparator(),
                                     ReverseBytewiseComparator()),
                     testing::Bool(), testing::Bool(), testing::Bool()));
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  // Opt-in this whole test file
  ROCKSDB_NAMESPACE::TEST_AllowUnsupportedFormatVersion() = true;

  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
