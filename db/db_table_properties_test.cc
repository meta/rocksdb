//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/db.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_properties_internal.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/atomic.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// A helper function that ensures the table properties returned in
// `GetPropertiesOfAllTablesTest` is correct.
// This test assumes entries size is different for each of the tables.
namespace {

void VerifyTableProperties(DB* db, uint64_t expected_entries_size) {
  TablePropertiesCollection props;
  ASSERT_OK(db->GetPropertiesOfAllTables(&props));

  ASSERT_EQ(4U, props.size());
  std::unordered_set<uint64_t> unique_entries;

  // Indirect test
  uint64_t sum = 0;
  for (const auto& item : props) {
    unique_entries.insert(item.second->num_entries);
    sum += item.second->num_entries;
  }

  ASSERT_EQ(props.size(), unique_entries.size());
  ASSERT_EQ(expected_entries_size, sum);

  VerifySstUniqueIds(props);
}

class ParseCompressionDisplayNameManager : public CompressionManagerWrapper {
 public:
  static constexpr const char* kCompatibilityName =
      "ParseCompressionDisplayNameManager";

  ParseCompressionDisplayNameManager()
      : CompressionManagerWrapper(GetBuiltinV2CompressionManager()) {}

  const char* Name() const override { return kCompatibilityName; }
  const char* CompatibilityName() const override { return kCompatibilityName; }

  std::string CompressionTypeToString(CompressionType type) const override {
    if (type == kCustomCompression8A) {
      return "CustomAlpha";
    }
    if (type == kCustomCompression8B) {
      return "CustomBeta";
    }
    return CompressionManagerWrapper::CompressionTypeToString(type);
  }

  static void Register() {
    static std::once_flag loaded;
    std::call_once(loaded, []() {
      auto& library = *ObjectLibrary::Default();
      library.AddFactory<CompressionManager>(
          kCompatibilityName, [](const std::string& /*uri*/,
                                 std::unique_ptr<CompressionManager>* guard,
                                 std::string* /*errmsg*/) {
            *guard = std::make_unique<ParseCompressionDisplayNameManager>();
            return guard->get();
          });
    });
  }
};
}  // anonymous namespace

class DBTablePropertiesTest : public DBTestBase,
                              public testing::WithParamInterface<std::string> {
 public:
  DBTablePropertiesTest()
      : DBTestBase("db_table_properties_test", /*env_do_fsync=*/false) {}
};

TEST_F(DBTablePropertiesTest, GetPropertiesOfAllTablesTest) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 8;
  // Part of strategy to prevent pinning table files
  options.max_open_files = 42;
  Reopen(options);

  // Create 4 tables
  for (int table = 0; table < 4; ++table) {
    // Build file
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(
          db_->Put(WriteOptions(), std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  std::string original_session_id;
  ASSERT_OK(db_->GetDbSessionId(original_session_id));

  // Part of strategy to prevent pinning table files
  SyncPoint::GetInstance()->SetCallBack(
      "VersionEditHandler::LoadTables:skip_load_table_files",
      [&](void* skip_load) { *static_cast<bool*>(skip_load) = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // 1. Read table properties directly from file
  Reopen(options);
  // Clear out auto-opened files
  dbfull()->TEST_table_cache()->EraseUnRefEntries();
  ASSERT_EQ(dbfull()->TEST_table_cache()->GetUsage(), 0U);
  VerifyTableProperties(db_.get(), 10 + 11 + 12 + 13);

  // 2. Put two tables to table cache and
  Reopen(options);
  // Clear out auto-opened files
  dbfull()->TEST_table_cache()->EraseUnRefEntries();
  ASSERT_EQ(dbfull()->TEST_table_cache()->GetUsage(), 0U);
  // fetch key from 1st and 2nd table, which will internally place that table to
  // the table cache.
  for (int i = 0; i < 2; ++i) {
    Get(std::to_string(i * 100 + 0));
  }

  VerifyTableProperties(db_.get(), 10 + 11 + 12 + 13);

  // 3. Put all tables to table cache
  Reopen(options);
  // fetch key from all tables, which will place them in table cache.
  for (int i = 0; i < 4; ++i) {
    Get(std::to_string(i * 100 + 0));
  }
  VerifyTableProperties(db_.get(), 10 + 11 + 12 + 13);

  // 4. Try to read CORRUPT properties (a) directly from file, and (b)
  // through reader on Get

  // It's not practical to prevent table file read on Open, so we
  // corrupt after open and after purging table cache.
  for (bool direct : {true, false}) {
    Reopen(options);
    // Clear out auto-opened files
    dbfull()->TEST_table_cache()->EraseUnRefEntries();
    ASSERT_EQ(dbfull()->TEST_table_cache()->GetUsage(), 0U);

    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    std::string sst_file = props.begin()->first;

    // Corrupt the file's TableProperties using session id
    std::string contents;
    ASSERT_OK(
        ReadFileToString(env_->GetFileSystem().get(), sst_file, &contents));
    size_t pos = contents.find(original_session_id);
    ASSERT_NE(pos, std::string::npos);
    ASSERT_OK(test::CorruptFile(env_, sst_file, static_cast<int>(pos), 1,
                                /*verify checksum fails*/ false));

    // Try to read CORRUPT properties
    if (direct) {
      ASSERT_TRUE(db_->GetPropertiesOfAllTables(&props).IsCorruption());
    } else {
      bool found_corruption = false;
      for (int i = 0; i < 4; ++i) {
        std::string result = Get(std::to_string(i * 100 + 0));
        if (result.find_first_of("Corruption: block checksum mismatch") !=
            std::string::npos) {
          found_corruption = true;
        }
      }
      ASSERT_TRUE(found_corruption);
    }

    // UN-corrupt file for next iteration
    ASSERT_OK(test::CorruptFile(env_, sst_file, static_cast<int>(pos), 1,
                                /*verify checksum fails*/ false));
  }

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTablePropertiesTest, InvalidReportedAsCorruption) {
  // Inject properties block data that Block considers invalid
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WritePropertiesBlock:BlockData",
      [&](void* block_data) { *static_cast<Slice*>(block_data) = Slice("X"); });
  SyncPoint::GetInstance()->EnableProcessing();

  // Corrupting the table properties corrupts the unique id.
  // Ignore the unique id recorded in the manifest.
  auto options = CurrentOptions();
  options.verify_sst_unique_id_in_manifest = false;
  Reopen(options);

  // Build file
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), std::to_string(i), "val"));
  }
  // Corrupted properties block should be detected and reported as corruption
  ASSERT_TRUE(db_->Flush(FlushOptions()).IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTablePropertiesTest, CreateOnDeletionCollectorFactory) {
  ConfigOptions options;
  options.ignore_unsupported_options = false;

  std::shared_ptr<TablePropertiesCollectorFactory> factory;
  std::string id = CompactOnDeletionCollectorFactory::kClassName();
  ASSERT_OK(
      TablePropertiesCollectorFactory::CreateFromString(options, id, &factory));
  auto del_factory = factory->CheckedCast<CompactOnDeletionCollectorFactory>();
  ASSERT_NE(del_factory, nullptr);
  ASSERT_EQ(0U, del_factory->GetWindowSize());
  ASSERT_EQ(0U, del_factory->GetDeletionTrigger());
  ASSERT_EQ(0.0, del_factory->GetDeletionRatio());
  ASSERT_OK(TablePropertiesCollectorFactory::CreateFromString(
      options, "window_size=100; deletion_trigger=90; id=" + id, &factory));
  del_factory = factory->CheckedCast<CompactOnDeletionCollectorFactory>();
  ASSERT_NE(del_factory, nullptr);
  ASSERT_EQ(100U, del_factory->GetWindowSize());
  ASSERT_EQ(90U, del_factory->GetDeletionTrigger());
  ASSERT_EQ(0.0, del_factory->GetDeletionRatio());
  ASSERT_OK(TablePropertiesCollectorFactory::CreateFromString(
      options,
      "window_size=100; deletion_trigger=90; deletion_ratio=0.5; id=" + id,
      &factory));
  del_factory = factory->CheckedCast<CompactOnDeletionCollectorFactory>();
  ASSERT_NE(del_factory, nullptr);
  ASSERT_EQ(100U, del_factory->GetWindowSize());
  ASSERT_EQ(90U, del_factory->GetDeletionTrigger());
  ASSERT_EQ(0.5, del_factory->GetDeletionRatio());
}

TEST_F(DBTablePropertiesTest, GetPropertiesOfTablesByLevelTest) {
  Random rnd(202);
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  options.write_buffer_size = 4096;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.target_file_size_base = 2048;
  options.max_bytes_for_level_base = 40960;
  options.max_bytes_for_level_multiplier = 4;
  options.hard_pending_compaction_bytes_limit = 16 * 1024;
  options.num_levels = 8;
  options.env = env_;

  DestroyAndReopen(options);

  // build a decent LSM
  for (int i = 0; i < 10000; i++) {
    EXPECT_OK(Put(test::RandomKey(&rnd, 5), rnd.RandomString(102)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  if (NumTableFilesAtLevel(0) == 0) {
    EXPECT_OK(Put(test::RandomKey(&rnd, 5), rnd.RandomString(102)));
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_->PauseBackgroundWork());

  // Ensure that we have at least L0, L1 and L2
  ASSERT_GT(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1), 0);
  ASSERT_GT(NumTableFilesAtLevel(2), 0);
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  std::vector<std::unique_ptr<TablePropertiesCollection>> levels_props;
  ASSERT_OK(db_->GetPropertiesOfTablesByLevel(db_->DefaultColumnFamily(),
                                              &levels_props));
  for (int i = 0; i < 8; i++) {
    const std::unique_ptr<TablePropertiesCollection>& level_props =
        levels_props[i];
    ASSERT_EQ(level_props->size(), cf_meta.levels[i].files.size());
  }

  Close();
}

// Test params:
// 1) whether to enable user-defined timestamps
class DBTablePropertiesInRangeTest : public DBTestBase,
                                     public testing::WithParamInterface<bool> {
 public:
  DBTablePropertiesInRangeTest()
      : DBTestBase("db_table_properties_in_range_test",
                   /*env_do_fsync=*/false) {}

  void SetUp() override { enable_udt_ = GetParam(); }

 protected:
  void PutKeyValue(const Slice& key, const Slice& value) {
    if (enable_udt_) {
      EXPECT_OK(db_->Put(WriteOptions(), key, min_ts_, value));
    } else {
      EXPECT_OK(Put(key, value));
    }
  }

  std::string GetValue(const std::string& key) {
    ReadOptions roptions;
    std::string result;
    if (enable_udt_) {
      roptions.timestamp = &min_ts_;
    }
    Status s = db_->Get(roptions, key, &result);
    EXPECT_TRUE(s.ok());
    return result;
  }

  Status MaybeGetValue(const std::string& key, std::string* result) {
    ReadOptions roptions;
    if (enable_udt_) {
      roptions.timestamp = &min_ts_;
    }
    Status s = db_->Get(roptions, key, result);
    EXPECT_TRUE(s.IsNotFound() || s.ok());
    return s;
  }

  TablePropertiesCollection TestGetPropertiesOfTablesInRange(
      std::vector<Range> ranges, std::size_t* num_properties = nullptr,
      std::size_t* num_files = nullptr) {
    // Since we deref zero element in the vector it can not be empty
    // otherwise we pass an address to some random memory
    EXPECT_GT(ranges.size(), 0U);
    // run the query
    TablePropertiesCollection props;
    ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
    EXPECT_OK(db_->GetPropertiesOfTablesInRange(default_cf, ranges.data(),
                                                ranges.size(), &props));

    const Comparator* ucmp = default_cf->GetComparator();
    EXPECT_NE(ucmp, nullptr);
    const size_t ts_sz = ucmp->timestamp_size();
    const size_t range_size = ranges.size();
    autovector<UserKeyRange> ukey_ranges;
    std::vector<std::string> keys;
    ukey_ranges.reserve(range_size);
    keys.reserve(range_size * 2);
    for (auto& r : ranges) {
      auto [start, limit] = MaybeAddTimestampsToRange(
          r.start, r.limit, ts_sz, &keys.emplace_back(), &keys.emplace_back(),
          /*exclusive_end=*/false);
      EXPECT_TRUE(start.has_value());
      EXPECT_TRUE(limit.has_value());
      ukey_ranges.emplace_back(start.value(), limit.value());
    }
    // Make sure that we've received properties for those and for those files
    // only which fall within requested ranges
    std::vector<LiveFileMetaData> vmd;
    db_->GetLiveFilesMetaData(&vmd);
    for (auto& md : vmd) {
      std::string fn = md.db_path + md.name;
      bool in_range = false;
      for (auto& r : ukey_ranges) {
        if (ucmp->Compare(r.start, md.largestkey) <= 0 &&
            ucmp->Compare(r.limit, md.smallestkey) >= 0) {
          in_range = true;
          EXPECT_GT(props.count(fn), 0);
        }
      }
      if (!in_range) {
        EXPECT_EQ(props.count(fn), 0);
      }
    }

    if (num_properties) {
      *num_properties = props.size();
    }

    if (num_files) {
      *num_files = vmd.size();
    }
    return props;
  }

  bool enable_udt_ = false;
  Slice min_ts_ = MinU64Ts();
};

TEST_P(DBTablePropertiesInRangeTest, GetPropertiesOfTablesInRange) {
  // Fixed random sead
  Random rnd(301);

  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  options.write_buffer_size = 4096;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.target_file_size_base = 2048;
  options.max_bytes_for_level_base = 40960;
  options.max_bytes_for_level_multiplier = 4;
  options.hard_pending_compaction_bytes_limit = 16 * 1024;
  options.num_levels = 8;
  options.env = env_;
  bool udt_enabled = GetParam();
  if (udt_enabled) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }

  DestroyAndReopen(options);

  // build a decent LSM
  for (int i = 0; i < 10000; i++) {
    PutKeyValue(test::RandomKey(&rnd, 5), rnd.RandomString(102));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  if (NumTableFilesAtLevel(0) == 0) {
    PutKeyValue(test::RandomKey(&rnd, 5), rnd.RandomString(102));
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_->PauseBackgroundWork());

  // Ensure that we have at least L0, L1 and L2
  ASSERT_GT(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1), 0);
  ASSERT_GT(NumTableFilesAtLevel(2), 0);

  // Query the largest range
  std::size_t num_properties;
  std::size_t num_files;
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::SMALLEST),
             test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST))},
      &num_properties, &num_files);
  ASSERT_EQ(num_properties, num_files);

  // Query the empty range
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST),
             test::RandomKey(&rnd, 5, test::RandomKeyType::SMALLEST))},
      &num_properties, &num_files);
  ASSERT_GT(num_files, 0);
  ASSERT_EQ(num_properties, 0);

  // Query the middle rangee
  TestGetPropertiesOfTablesInRange(
      {Range(test::RandomKey(&rnd, 5, test::RandomKeyType::MIDDLE),
             test::RandomKey(&rnd, 5, test::RandomKeyType::LARGEST))},
      &num_properties, &num_files);
  ASSERT_GT(num_files, 0);
  ASSERT_GT(num_files, num_properties);
  ASSERT_GT(num_properties, 0);

  // Query a bunch of random ranges
  for (int j = 0; j < 100; j++) {
    // create a bunch of ranges
    std::vector<std::string> random_keys;
    // Random returns numbers with zero included
    // when we pass empty ranges TestGetPropertiesOfTablesInRange()
    // derefs random memory in the empty ranges[0]
    // so want to be greater than zero and even since
    // the below loop requires that random_keys.size() to be even.
    auto n = 2 * (rnd.Uniform(50) + 1);

    for (uint32_t i = 0; i < n; ++i) {
      random_keys.push_back(test::RandomKey(&rnd, 5));
    }

    ASSERT_GT(random_keys.size(), 0U);
    ASSERT_EQ((random_keys.size() % 2), 0U);

    std::vector<Range> ranges;
    auto it = random_keys.begin();
    while (it != random_keys.end()) {
      ranges.emplace_back(*it, *(it + 1));
      it += 2;
    }

    TestGetPropertiesOfTablesInRange(std::move(ranges));
  }
}

INSTANTIATE_TEST_CASE_P(DBTablePropertiesInRangeTest,
                        DBTablePropertiesInRangeTest,
                        ::testing::Values(true, false));

TEST_F(DBTablePropertiesTest, GetColumnFamilyNameProperty) {
  std::string kExtraCfName = "pikachu";
  CreateAndReopenWithCF({kExtraCfName}, CurrentOptions());

  // Create one table per CF, then verify it was created with the column family
  // name property.
  for (uint32_t cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Flush(cf));

    TablePropertiesCollection fname_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[cf], &fname_to_props));
    ASSERT_EQ(1U, fname_to_props.size());

    std::string expected_cf_name;
    if (cf > 0) {
      expected_cf_name = kExtraCfName;
    } else {
      expected_cf_name = kDefaultColumnFamilyName;
    }
    ASSERT_EQ(expected_cf_name,
              fname_to_props.begin()->second->column_family_name);
    ASSERT_EQ(cf, static_cast<uint32_t>(
                      fname_to_props.begin()->second->column_family_id));
  }
}

TEST_F(DBTablePropertiesTest, GetDbIdentifiersProperty) {
  CreateAndReopenWithCF({"goku"}, CurrentOptions());

  for (uint32_t cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Put(cf, "foo", "bar"));
    ASSERT_OK(Flush(cf));

    TablePropertiesCollection fname_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[cf], &fname_to_props));
    ASSERT_EQ(1U, fname_to_props.size());

    std::string id;
    std::string sid;
    ASSERT_OK(db_->GetDbIdentity(id));
    ASSERT_OK(db_->GetDbSessionId(sid));
    ASSERT_EQ(id, fname_to_props.begin()->second->db_id);
    ASSERT_EQ(sid, fname_to_props.begin()->second->db_session_id);
  }
}

TEST_F(DBTablePropertiesTest, FactoryReturnsNull) {
  struct JunkTablePropertiesCollector : public TablePropertiesCollector {
    const char* Name() const override { return "JunkTablePropertiesCollector"; }
    Status Finish(UserCollectedProperties* properties) override {
      properties->insert({"Junk", "Junk"});
      return Status::OK();
    }
    UserCollectedProperties GetReadableProperties() const override {
      return {};
    }
  };

  // Alternates between putting a "Junk" property and using `nullptr` to
  // opt out.
  static RelaxedAtomic<int> count{0};
  struct SometimesTablePropertiesCollectorFactory
      : public TablePropertiesCollectorFactory {
    const char* Name() const override {
      return "SometimesTablePropertiesCollectorFactory";
    }
    TablePropertiesCollector* CreateTablePropertiesCollector(
        TablePropertiesCollectorFactory::Context /*context*/) override {
      if (count.FetchAddRelaxed(1) & 1) {
        return nullptr;
      } else {
        return new JunkTablePropertiesCollector();
      }
    }
  };

  Options options = CurrentOptions();
  options.table_properties_collector_factories.emplace_back(
      std::make_shared<SometimesTablePropertiesCollectorFactory>());
  // For plain table
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  for (const std::shared_ptr<TableFactory>& tf :
       {options.table_factory,
        std::shared_ptr<TableFactory>(NewPlainTableFactory({}))}) {
    SCOPED_TRACE("Table factory = " + std::string(tf->Name()));
    options.table_factory = tf;

    DestroyAndReopen(options);

    ASSERT_OK(Put("key0", "value1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("key0", "value2"));
    ASSERT_OK(Flush());

    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    int no_junk_count = 0;
    int junk_count = 0;
    for (const auto& item : props) {
      if (item.second->user_collected_properties.find("Junk") !=
          item.second->user_collected_properties.end()) {
        junk_count++;
      } else {
        no_junk_count++;
      }
    }
    EXPECT_EQ(1, no_junk_count);
    EXPECT_EQ(1, junk_count);
  }
}

class DBTableHostnamePropertyTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<int, std::string>> {
 public:
  DBTableHostnamePropertyTest()
      : DBTestBase("db_table_hostname_property_test",
                   /*env_do_fsync=*/false) {}
};

TEST_P(DBTableHostnamePropertyTest, DbHostLocationProperty) {
  option_config_ = std::get<0>(GetParam());
  Options opts = CurrentOptions();
  std::string expected_host_id = std::get<1>(GetParam());
  ;
  if (expected_host_id == kHostnameForDbHostId) {
    ASSERT_OK(env_->GetHostNameString(&expected_host_id));
  } else {
    opts.db_host_id = expected_host_id;
  }
  CreateAndReopenWithCF({"goku"}, opts);

  for (uint32_t cf = 0; cf < 2; ++cf) {
    ASSERT_OK(Put(cf, "key", "val"));
    ASSERT_OK(Put(cf, "foo", "bar"));
    ASSERT_OK(Flush(cf));

    TablePropertiesCollection fname_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[cf], &fname_to_props));
    ASSERT_EQ(1U, fname_to_props.size());

    ASSERT_EQ(fname_to_props.begin()->second->db_host_id, expected_host_id);
  }
}

INSTANTIATE_TEST_CASE_P(
    DBTableHostnamePropertyTest, DBTableHostnamePropertyTest,
    ::testing::Values(
        // OptionConfig, override db_host_location
        std::make_tuple(DBTestBase::OptionConfig::kDefault,
                        kHostnameForDbHostId),
        std::make_tuple(DBTestBase::OptionConfig::kDefault, "foobar"),
        std::make_tuple(DBTestBase::OptionConfig::kDefault, ""),
        std::make_tuple(DBTestBase::OptionConfig::kPlainTableFirstBytePrefix,
                        kHostnameForDbHostId),
        std::make_tuple(DBTestBase::OptionConfig::kPlainTableFirstBytePrefix,
                        "foobar"),
        std::make_tuple(DBTestBase::OptionConfig::kPlainTableFirstBytePrefix,
                        "")));

class DeletionTriggeredCompactionTestListener : public EventListener {
 public:
  void OnCompactionBegin(DB*, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.compaction_reason,
              CompactionReason::kFilesMarkedForCompaction);
  }

  void OnCompactionCompleted(DB*, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.compaction_reason,
              CompactionReason::kFilesMarkedForCompaction);
  }
};

TEST_P(DBTablePropertiesTest, DeletionTriggeredCompactionMarking) {
  int kNumKeys = 1000;
  int kWindowSize = 100;
  int kNumDelsTrigger = 90;
  std::shared_ptr<TablePropertiesCollectorFactory> compact_on_del =
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger);

  Options opts = CurrentOptions();
  opts.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  opts.table_properties_collector_factories.emplace_back(compact_on_del);

  if (GetParam() == "kCompactionStyleUniversal") {
    opts.compaction_style = kCompactionStyleUniversal;
  }
  Reopen(opts);

  // add an L1 file to prevent tombstones from dropping due to obsolescence
  // during flush
  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  DeletionTriggeredCompactionTestListener* listener =
      new DeletionTriggeredCompactionTestListener();
  opts.listeners.emplace_back(listener);
  Reopen(opts);

  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      ASSERT_OK(Delete(Key(i)));
    } else {
      ASSERT_OK(Put(Key(i), "val"));
    }
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  // Change the window size and deletion trigger and ensure new values take
  // effect
  kWindowSize = 50;
  kNumDelsTrigger = 40;
  static_cast<CompactOnDeletionCollectorFactory*>(compact_on_del.get())
      ->SetWindowSize(kWindowSize);
  static_cast<CompactOnDeletionCollectorFactory*>(compact_on_del.get())
      ->SetDeletionTrigger(kNumDelsTrigger);
  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      ASSERT_OK(Delete(Key(i)));
    } else {
      ASSERT_OK(Put(Key(i), "val"));
    }
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  // Change the window size to disable delete triggered compaction
  kWindowSize = 0;
  static_cast<CompactOnDeletionCollectorFactory*>(compact_on_del.get())
      ->SetWindowSize(kWindowSize);
  static_cast<CompactOnDeletionCollectorFactory*>(compact_on_del.get())
      ->SetDeletionTrigger(kNumDelsTrigger);
  for (int i = 0; i < kNumKeys; ++i) {
    if (i >= kNumKeys - kWindowSize &&
        i < kNumKeys - kWindowSize + kNumDelsTrigger) {
      ASSERT_OK(Delete(Key(i)));
    } else {
      ASSERT_OK(Put(Key(i), "val"));
    }
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_LT(0, opts.statistics->getTickerCount(COMPACT_WRITE_BYTES_MARKED));
  ASSERT_LT(0, opts.statistics->getTickerCount(COMPACT_READ_BYTES_MARKED));
}

TEST_P(DBTablePropertiesTest, RatioBasedDeletionTriggeredCompactionMarking) {
  constexpr int kNumKeys = 1000;
  constexpr int kWindowSize = 0;
  constexpr int kNumDelsTrigger = 0;
  constexpr double kDeletionRatio = 0.1;
  std::shared_ptr<TablePropertiesCollectorFactory> compact_on_del =
      NewCompactOnDeletionCollectorFactory(kWindowSize, kNumDelsTrigger,
                                           kDeletionRatio);

  Options opts = CurrentOptions();
  opts.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  opts.table_properties_collector_factories.emplace_back(compact_on_del);

  Reopen(opts);

  // Add an L2 file to prevent tombstones from dropping due to obsolescence
  // during flush
  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  auto* listener = new DeletionTriggeredCompactionTestListener();
  opts.listeners.emplace_back(listener);
  Reopen(opts);

  // Generate one L0 with kNumKeys Put.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "not important"));
  }
  ASSERT_OK(Flush());

  // Generate another L0 with kNumKeys Delete.
  // This file, due to deletion ratio, will trigger compaction: 2@0 files to L1.
  // The resulting L1 file has only one tombstone for user key 'Key(0)'.
  // Again, due to deletion ratio, a compaction will be triggered: 1@1 + 1@2
  // files to L2. However, the resulting file is empty because the tombstone
  // and value are both dropped.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(0, NumTableFilesAtLevel(i));
  }
}

TEST_F(DBTablePropertiesTest, KeyLargestSmallestSeqno) {
  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db_->Put(WriteOptions(), "key3", "value3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  {
    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_EQ(1U, props.size());

    auto table_props = props.begin()->second;

    ASSERT_TRUE(table_props->HasKeyLargestSeqno());
    ASSERT_TRUE(table_props->HasKeySmallestSeqno());

    ASSERT_EQ(table_props->key_largest_seqno,
              table_props->key_smallest_seqno + 2);
    ASSERT_GT(table_props->key_largest_seqno, 0U);
    ASSERT_GT(table_props->key_smallest_seqno, 0U);
  }

  // Becomes zero after compaction
  {
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_EQ(1U, props.size());

    auto table_props = props.begin()->second;
    ASSERT_TRUE(table_props->HasKeyLargestSeqno());
    ASSERT_TRUE(table_props->HasKeySmallestSeqno());

    ASSERT_EQ(table_props->key_largest_seqno, table_props->key_smallest_seqno);
    ASSERT_EQ(table_props->key_largest_seqno, 0U);
  }
}

TEST_F(DBTablePropertiesTest, LsmInfoAtCreationPacking) {
  using Applicability = LsmInfoAtCreation::Applicability;
  auto round_trip = [](const LsmInfoAtCreation& info) {
    return LsmInfoAtCreation::DecodeFrom(info.Encode());
  };

  // Default is "unknown", which encodes to 0 and is distinct from every other
  // state.
  LsmInfoAtCreation unknown;
  EXPECT_EQ(unknown.applicability, Applicability::kUnknown);
  EXPECT_EQ(unknown.Encode(), 0U);
  EXPECT_EQ(round_trip(unknown), unknown);

  // "not applicable" is non-zero (so it persists) and distinct from "unknown".
  LsmInfoAtCreation na = LsmInfoAtCreation::NotApplicable();
  EXPECT_EQ(na.applicability, Applicability::kNotApplicable);
  EXPECT_NE(na.Encode(), 0U);
  EXPECT_NE(na.Encode(), unknown.Encode());
  EXPECT_EQ(round_trip(na), na);

  // "applicable" round-trips natural level values and bottommost. Level 0 / not
  // bottommost is still non-zero and thus distinct from "unknown"/"N/A".
  for (int level : {0, 1, 5, 63, 200}) {
    for (bool bottommost : {false, true}) {
      LsmInfoAtCreation info = LsmInfoAtCreation::Applicable(level, bottommost);
      EXPECT_NE(info.Encode(), 0U);
      LsmInfoAtCreation decoded = round_trip(info);
      EXPECT_EQ(decoded.applicability, Applicability::kApplicable);
      EXPECT_EQ(decoded.level_at_creation, level);
      EXPECT_EQ(decoded.is_bottommost, bottommost);
      EXPECT_EQ(decoded, info);
    }
  }

  // Applicable but with an unknown level (e.g. the DB repairer): still
  // applicable, bottommost preserved, level decodes back to kUnknownLevel.
  LsmInfoAtCreation unknown_level = LsmInfoAtCreation::Applicable(
      LsmInfoAtCreation::kUnknownLevel, /*is_bottommost=*/true);
  LsmInfoAtCreation decoded = round_trip(unknown_level);
  EXPECT_EQ(decoded.applicability, Applicability::kApplicable);
  EXPECT_EQ(decoded.level_at_creation, LsmInfoAtCreation::kUnknownLevel);
  EXPECT_TRUE(decoded.is_bottommost);
}

TEST_F(DBTablePropertiesTest, CreationTimeAliasesOldestAncestorTime) {
  TableProperties props;
  ASSERT_EQ(&props.creation_time, &props.oldest_ancestor_time);
  props.creation_time = 12345;
  ASSERT_EQ(props.oldest_ancestor_time, 12345U);
  ASSERT_EQ(&TablePropertiesNames::kCreationTime,
            &TablePropertiesNames::kOldestAncestorTime);
}

TEST_F(DBTablePropertiesTest, SstFileMetaDataOldestAncesterTimeAlias) {
  // Intentionally uses the deprecated misspelled alias
  LiveFileMetaData meta;
  ASSERT_EQ(&meta.oldest_ancester_time, &meta.oldest_ancestor_time);
  meta.oldest_ancester_time = 12345;
  ASSERT_EQ(meta.oldest_ancestor_time, 12345U);
}

TEST_F(DBTablePropertiesTest, UncompressedDataSizeAndLsmInfoPersisted) {
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  DestroyAndReopen(options);

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db_->Put(WriteOptions(), "key3", "value3"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Freshly flushed L0 file. uncompressed_data_size is now persisted (it used
  // to always read back as 0), and LSM-at-creation info is populated.
  {
    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_EQ(1U, props.size());
    auto table_props = props.begin()->second;

    EXPECT_GT(table_props->uncompressed_data_size, 0U);
    LsmInfoAtCreation info =
        LsmInfoAtCreation::DecodeFrom(table_props->lsm_info_at_creation);
    EXPECT_EQ(info.applicability,
              LsmInfoAtCreation::Applicability::kApplicable);
    EXPECT_EQ(info.level_at_creation, 0);
    EXPECT_FALSE(info.is_bottommost);
  }

  // After forcing a bottommost compaction, the (re)created file records
  // is_bottommost = true and a deeper creation level.
  {
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_EQ(1U, props.size());
    auto table_props = props.begin()->second;

    EXPECT_GT(table_props->uncompressed_data_size, 0U);
    LsmInfoAtCreation info =
        LsmInfoAtCreation::DecodeFrom(table_props->lsm_info_at_creation);
    EXPECT_EQ(info.applicability,
              LsmInfoAtCreation::Applicability::kApplicable);
    EXPECT_TRUE(info.is_bottommost);
    EXPECT_GT(info.level_at_creation, 0);
  }
}

TEST_F(DBTablePropertiesTest, LsmInfoNotApplicableForSstFileWriter) {
  // A file written by SstFileWriter has no LSM position, so its
  // lsm_info_at_creation records "not applicable" (distinct from "unknown").
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  std::string file = dbname_ + "/for_ingestion.sst";
  {
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(file));
    ASSERT_OK(writer.Put("a", "1"));
    ASSERT_OK(writer.Put("b", "2"));
    ASSERT_OK(writer.Finish());
  }

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(file));
  ASSERT_OK(reader.VerifyChecksum());
  std::shared_ptr<const TableProperties> table_props =
      reader.GetTableProperties();
  ASSERT_NE(table_props, nullptr);

  LsmInfoAtCreation info =
      LsmInfoAtCreation::DecodeFrom(table_props->lsm_info_at_creation);
  EXPECT_EQ(info.applicability,
            LsmInfoAtCreation::Applicability::kNotApplicable);
  EXPECT_EQ(info.level_at_creation, LsmInfoAtCreation::kUnknownLevel);
  EXPECT_FALSE(info.is_bottommost);
  // uncompressed_data_size is persisted for SstFileWriter files too.
  EXPECT_GT(table_props->uncompressed_data_size, 0U);
}

TEST_F(DBTablePropertiesTest, FileCreationTimeAlwaysSet) {
  // file_creation_time is recorded in both table properties and the MANIFEST
  // for SstFileWriter files, WAL recovery flushes, and after DB repair.
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  int64_t start_time = 0;
  ASSERT_OK(env_->GetCurrentTime(&start_time));

  std::string file = dbname_ + "/for_ingestion.sst";
  {
    // With a column family, so that repair keeps the file
    SstFileWriter writer(EnvOptions(), options, db_->DefaultColumnFamily());
    ASSERT_OK(writer.Open(file));
    ASSERT_OK(writer.Put("a", "1"));
    ASSERT_OK(writer.Finish());
  }
  IngestExternalFileOptions ifo;
  ifo.move_files = true;
  ASSERT_OK(db_->IngestExternalFile({file}, ifo));

  // Left in the WAL to be flushed during recovery
  ASSERT_OK(Put("b", "2"));
  Reopen(options);

  auto verify = [&]() {
    int64_t end_time = 0;
    ASSERT_OK(env_->GetCurrentTime(&end_time));
    const uint64_t lo = static_cast<uint64_t>(start_time);
    const uint64_t hi = static_cast<uint64_t>(end_time);

    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_EQ(props.size(), 2U);
    for (const auto& p : props) {
      EXPECT_GE(p.second->file_creation_time, lo) << p.first;
      EXPECT_LE(p.second->file_creation_time, hi) << p.first;
    }

    std::vector<std::vector<FileMetaData>> metadata;
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &metadata);
    size_t num_files = 0;
    for (const auto& level : metadata) {
      for (const auto& f : level) {
        ++num_files;
        EXPECT_GE(f.file_creation_time, lo) << f.fd.GetNumber();
        EXPECT_LE(f.file_creation_time, hi) << f.fd.GetNumber();
      }
    }
    ASSERT_EQ(num_files, 2U);
  };
  {
    SCOPED_TRACE("before repair");
    verify();
  }

  Close();
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  {
    SCOPED_TRACE("after repair");
    verify();
  }
}

TEST_F(DBTablePropertiesTest, CompressionManagerFullDetailsPersisted) {
  // With an explicit CompressionManager, compression_options records the
  // manager's full details via ToString() (id plus any configured options),
  // not just GetId(). This manager has no options, so ToString() is its id.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.compression_manager =
      std::make_shared<ParseCompressionDisplayNameManager>();
  DestroyAndReopen(options);

  ASSERT_OK(db_->Put(WriteOptions(), "k", "v"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_EQ(1U, props.size());
  auto table_props = props.begin()->second;

  EXPECT_NE(
      table_props->compression_options.find(
          "_compression_manager=" +
          std::string(ParseCompressionDisplayNameManager::kCompatibilityName)),
      std::string::npos)
      << table_props->compression_options;
}

TEST_F(DBTablePropertiesTest, ParseCompressionNameForDisplay) {
  auto custom_mgr = std::make_shared<ParseCompressionDisplayNameManager>();

  // Test empty string
  EXPECT_EQ("NoCompression", ParseCompressionNameForDisplay(""));

  // Test old format (no semicolon)
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("ZSTD"));
  EXPECT_EQ("Snappy", ParseCompressionNameForDisplay("Snappy"));
  EXPECT_EQ("LZ4", ParseCompressionNameForDisplay("LZ4"));
  EXPECT_EQ("kZSTD", ParseCompressionNameForDisplay("kZSTD"));
  EXPECT_EQ("kSnappyCompression",
            ParseCompressionNameForDisplay("kSnappyCompression"));

  // Test new format version 7 with ZSTD
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("zstd;07;"));
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("BuiltinV2;07;"));
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("builtin_v2;07;"));
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay(";07;"));
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("custom_mgr;07;extra;"));

  // Test new format with LZ4
  EXPECT_EQ("LZ4", ParseCompressionNameForDisplay("zstd;04;"));
  EXPECT_EQ("LZ4", ParseCompressionNameForDisplay("lz4;04;"));

  // Test lowercase hex for reserved/custom values
  EXPECT_EQ("Reserved7F", ParseCompressionNameForDisplay("test;7f;"));
  EXPECT_EQ("Custom8A", ParseCompressionNameForDisplay("test;8a;"));

  // Test multiple compression types
  EXPECT_EQ("LZ4,ZSTD", ParseCompressionNameForDisplay("test;0407;"));
  EXPECT_EQ(
      "ZSTD,LZ4",
      ParseCompressionNameForDisplay("test;0704;"));  // sorted by appearance
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("test;0007;"));

  // Test all standard compression types (01-07)
  EXPECT_EQ("NoCompression", ParseCompressionNameForDisplay("test;00;"));
  EXPECT_EQ("Snappy", ParseCompressionNameForDisplay("test;01;"));
  EXPECT_EQ("Zlib", ParseCompressionNameForDisplay("test;02;"));
  EXPECT_EQ("BZip2", ParseCompressionNameForDisplay("test;03;"));
  EXPECT_EQ("LZ4", ParseCompressionNameForDisplay("test;04;"));
  EXPECT_EQ("LZ4HC", ParseCompressionNameForDisplay("test;05;"));
  EXPECT_EQ("Xpress", ParseCompressionNameForDisplay("test;06;"));
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("test;07;"));

  // Test custom/reserved types (>= 0x08)
  // 0x08-0x7F are Reserved, 0x80-0xFE are Custom
  EXPECT_EQ("Reserved08", ParseCompressionNameForDisplay("test;08;"));
  EXPECT_EQ("Custom80", ParseCompressionNameForDisplay("test;80;"));
  EXPECT_EQ("Reserved7F", ParseCompressionNameForDisplay("test;7F;"));
  EXPECT_EQ("CustomFE", ParseCompressionNameForDisplay("test;FE;"));
  EXPECT_EQ("CustomAlpha,CustomBeta",
            ParseCompressionNameForDisplay(
                "ParseCompressionDisplayNameManager;8A8B;", custom_mgr));
  EXPECT_EQ("CustomAlpha,LZ4,CustomBeta",
            ParseCompressionNameForDisplay(
                "ParseCompressionDisplayNameManager;8A048B;", custom_mgr));

  ParseCompressionDisplayNameManager::Register();
  EXPECT_EQ("CustomAlpha", ParseCompressionNameForDisplay(
                               "ParseCompressionDisplayNameManager;8A;"));

  // Test DisableOption (0xFF) - filtered out
  EXPECT_EQ("NoCompression", ParseCompressionNameForDisplay("test;FF;"));

  // Test NoCompression (empty hex field would be caught by validation, but test
  // "00")
  EXPECT_EQ("NoCompression", ParseCompressionNameForDisplay("test;00;"));
  EXPECT_EQ("NoCompression", ParseCompressionNameForDisplay("BuiltinV2;;"));
  EXPECT_EQ("NoCompression", ParseCompressionNameForDisplay("test;;"));

  // Test three+ semicolons (future fields ignored)
  EXPECT_EQ("ZSTD", ParseCompressionNameForDisplay("test;07;extra;fields;"));

  // Test malformed inputs -> "Unknown"
  EXPECT_EQ("Unknown", ParseCompressionNameForDisplay("only_one;"));
  EXPECT_EQ("Unknown", ParseCompressionNameForDisplay("bad;0;"));  // odd length
  EXPECT_EQ("Unknown",
            ParseCompressionNameForDisplay("bad;0G;"));  // invalid hex
  EXPECT_EQ("Unknown",
            ParseCompressionNameForDisplay("bad;GG;"));  // invalid hex
}

INSTANTIATE_TEST_CASE_P(DBTablePropertiesTest, DBTablePropertiesTest,
                        ::testing::Values("kCompactionStyleLevel",
                                          "kCompactionStyleUniversal"));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
