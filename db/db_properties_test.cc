// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <cstdio>
#include <string>

#include "db/db_test_util.h"
#include "db/write_stall_stats.h"
#include "options/cf_options.h"
#include "port/stack_trace.h"
#include "rocksdb/listener.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/table.h"
#include "table/block_based/block.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "test_util/mock_time_env.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class DBPropertiesTest : public DBTestBase {
 public:
  DBPropertiesTest()
      : DBTestBase("db_properties_test", /*env_do_fsync=*/false) {}

  void AssertDbStats(const std::map<std::string, std::string>& db_stats,
                     double expected_uptime, int expected_user_bytes_written,
                     int expected_wal_bytes_written,
                     int expected_user_writes_by_self,
                     int expected_user_writes_with_wal) {
    ASSERT_EQ(std::to_string(expected_uptime), db_stats.at("db.uptime"));
    ASSERT_EQ(std::to_string(expected_wal_bytes_written),
              db_stats.at("db.wal_bytes_written"));
    ASSERT_EQ("0", db_stats.at("db.wal_syncs"));
    ASSERT_EQ(std::to_string(expected_user_bytes_written),
              db_stats.at("db.user_bytes_written"));
    ASSERT_EQ("0", db_stats.at("db.user_writes_by_other"));
    ASSERT_EQ(std::to_string(expected_user_writes_by_self),
              db_stats.at("db.user_writes_by_self"));
    ASSERT_EQ(std::to_string(expected_user_writes_with_wal),
              db_stats.at("db.user_writes_with_wal"));
    ASSERT_EQ("0", db_stats.at("db.user_write_stall_micros"));
  }
};

TEST_F(DBPropertiesTest, Empty) {
  do {
    Options options;
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    options.allow_concurrent_memtable_write = false;
    options = CurrentOptions(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    std::string num;
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("1", num);

    // Block sync calls
    env_->delay_sstable_sync_.store(true, std::memory_order_release);
    ASSERT_OK(Put(1, "k1", std::string(100000, 'x')));  // Fill memtable
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("2", num);

    ASSERT_OK(Put(1, "k2", std::string(100000, 'y')));  // Trigger compaction
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ("1", num);

    ASSERT_EQ("v1", Get(1, "foo"));
    // Release sync calls
    env_->delay_sstable_sync_.store(false, std::memory_order_release);

    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(db_->DisableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(db_->EnableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("0", num);

    ASSERT_OK(db_->EnableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("0", num);
    // File deletion enabled after `EnableFileDeletions` called as many times
    // as `DisableFileDeletions`.
    ASSERT_OK(db_->EnableFileDeletions());
    ASSERT_TRUE(
        dbfull()->GetProperty("rocksdb.is-file-deletions-enabled", &num));
    ASSERT_EQ("1", num);
  } while (ChangeOptions());
}

TEST_F(DBPropertiesTest, CurrentVersionNumber) {
  uint64_t v1;
  uint64_t v2;
  uint64_t v3;
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.current-super-version-number", &v1));
  ASSERT_OK(Put("12345678", ""));
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.current-super-version-number", &v2));
  ASSERT_OK(Flush());
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.current-super-version-number", &v3));

  ASSERT_EQ(v1, v2);
  ASSERT_GT(v3, v2);
}

TEST_F(DBPropertiesTest, GetAggregatedIntPropertyTest) {
  const int kKeySize = 100;
  const int kValueSize = 500;
  const int kKeyNum = 100;

  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.write_buffer_size = (kKeySize + kValueSize) * kKeyNum / 10;
  // Make them never flush
  options.min_write_buffer_number_to_merge = 1000;
  options.max_write_buffer_number = 1000;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"one", "two", "three", "four"}, options);

  Random rnd(301);
  for (auto* handle : handles_) {
    for (int i = 0; i < kKeyNum; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), handle, rnd.RandomString(kKeySize),
                         rnd.RandomString(kValueSize)));
    }
  }

  uint64_t manual_sum = 0;
  uint64_t api_sum = 0;
  uint64_t value = 0;
  for (auto* handle : handles_) {
    ASSERT_TRUE(
        db_->GetIntProperty(handle, DB::Properties::kSizeAllMemTables, &value));
    manual_sum += value;
  }
  ASSERT_TRUE(db_->GetAggregatedIntProperty(DB::Properties::kSizeAllMemTables,
                                            &api_sum));
  ASSERT_GT(manual_sum, 0);
  ASSERT_EQ(manual_sum, api_sum);

  ASSERT_FALSE(db_->GetAggregatedIntProperty(DB::Properties::kDBStats, &value));

  uint64_t before_flush_trm;
  uint64_t after_flush_trm;
  for (auto* handle : handles_) {
    ASSERT_TRUE(db_->GetAggregatedIntProperty(
        DB::Properties::kEstimateTableReadersMem, &before_flush_trm));

    // Issue flush and expect larger memory usage of table readers.
    ASSERT_OK(db_->Flush(FlushOptions(), handle));

    ASSERT_TRUE(db_->GetAggregatedIntProperty(
        DB::Properties::kEstimateTableReadersMem, &after_flush_trm));
    ASSERT_GT(after_flush_trm, before_flush_trm);
  }
}

TEST_F(DBPropertiesTest, AggregateBlockCacheProperty) {
  constexpr size_t kCapacity = 1000;
  LRUCacheOptions co;
  co.capacity = kCapacity;
  co.num_shard_bits = 0;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto block_cache = NewLRUCache(co);

  // All columns families share the same block cache.
  Options options = CurrentOptions();
  BlockBasedTableOptions table_opt;
  table_opt.no_block_cache = false;
  table_opt.block_cache = block_cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_opt));

  CreateAndReopenWithCF({"one", "two", "three", "four"}, options);

  // Insert unpinned block to the cache
  constexpr size_t kSize1 = 100;
  ASSERT_OK(block_cache->Insert("block1", nullptr /*value*/,
                                &kNoopCacheItemHelper, kSize1));
  // Insert pinned block to the cache
  constexpr size_t kSize2 = 200;
  Cache::Handle* block2 = nullptr;
  ASSERT_OK(block_cache->Insert("block2", nullptr /*value*/,
                                &kNoopCacheItemHelper, kSize2, &block2));

  uint64_t value;
  ASSERT_TRUE(db_->GetAggregatedIntProperty(DB::Properties::kBlockCacheCapacity,
                                            &value));
  ASSERT_EQ(value, kCapacity);

  ASSERT_TRUE(
      db_->GetAggregatedIntProperty(DB::Properties::kBlockCacheUsage, &value));
  ASSERT_EQ(value, kSize1 + kSize2);

  ASSERT_TRUE(db_->GetAggregatedIntProperty(
      DB::Properties::kBlockCachePinnedUsage, &value));
  ASSERT_EQ(value, kSize2);

  block_cache->Release(block2);
}

namespace {
void VerifySimilar(uint64_t a, uint64_t b, double bias) {
  ASSERT_EQ(a == 0U, b == 0U);
  if (a == 0) {
    return;
  }
  double dbl_a = static_cast<double>(a);
  double dbl_b = static_cast<double>(b);
  if (dbl_a > dbl_b) {
    ASSERT_LT(static_cast<double>(dbl_a - dbl_b) / (dbl_a + dbl_b), bias);
  } else {
    ASSERT_LT(static_cast<double>(dbl_b - dbl_a) / (dbl_a + dbl_b), bias);
  }
}

void VerifyTableProperties(
    const TableProperties& base_tp, const TableProperties& new_tp,
    double filter_size_bias = CACHE_LINE_SIZE >= 256 ? 0.18 : 0.1,
    double index_size_bias = 0.1, double data_size_bias = 0.1,
    double num_data_blocks_bias = 0.05) {
  VerifySimilar(base_tp.data_size, new_tp.data_size, data_size_bias);
  VerifySimilar(base_tp.index_size, new_tp.index_size, index_size_bias);
  VerifySimilar(base_tp.filter_size, new_tp.filter_size, filter_size_bias);
  VerifySimilar(base_tp.num_data_blocks, new_tp.num_data_blocks,
                num_data_blocks_bias);

  ASSERT_EQ(base_tp.raw_key_size, new_tp.raw_key_size);
  ASSERT_EQ(base_tp.raw_value_size, new_tp.raw_value_size);
  ASSERT_EQ(base_tp.num_entries, new_tp.num_entries);
  ASSERT_EQ(base_tp.num_deletions, new_tp.num_deletions);
  ASSERT_EQ(base_tp.num_range_deletions, new_tp.num_range_deletions);

  // Merge operands may become Puts, so we only have an upper bound the exact
  // number of merge operands.
  ASSERT_GE(base_tp.num_merge_operands, new_tp.num_merge_operands);
}

void GetExpectedTableProperties(
    TableProperties* expected_tp, const int kKeySize, const int kValueSize,
    const int kPutsPerTable, const int kDeletionsPerTable,
    const int kMergeOperandsPerTable, const int kRangeDeletionsPerTable,
    const int kTableCount, const int kBloomBitsPerKey, const size_t kBlockSize,
    const bool index_key_is_user_key, const bool value_delta_encoding) {
  const int kKeysPerTable =
      kPutsPerTable + kDeletionsPerTable + kMergeOperandsPerTable;
  const int kPutCount = kTableCount * kPutsPerTable;
  const int kDeletionCount = kTableCount * kDeletionsPerTable;
  const int kMergeCount = kTableCount * kMergeOperandsPerTable;
  const int kRangeDeletionCount = kTableCount * kRangeDeletionsPerTable;
  const int kKeyCount =
      kPutCount + kDeletionCount + kMergeCount + kRangeDeletionCount;
  const int kAvgSuccessorSize = kKeySize / 5;
  const int kEncodingSavePerKey = kKeySize / 4;
  expected_tp->raw_key_size = kKeyCount * (kKeySize + 8);
  expected_tp->raw_value_size =
      (kPutCount + kMergeCount + kRangeDeletionCount) * kValueSize;
  expected_tp->num_entries = kKeyCount;
  expected_tp->num_deletions = kDeletionCount + kRangeDeletionCount;
  expected_tp->num_merge_operands = kMergeCount;
  expected_tp->num_range_deletions = kRangeDeletionCount;
  expected_tp->num_data_blocks =
      kTableCount *
      (kKeysPerTable * (kKeySize - kEncodingSavePerKey + kValueSize)) /
      kBlockSize;
  expected_tp->data_size =
      kTableCount * (kKeysPerTable * (kKeySize + 8 + kValueSize));
  expected_tp->index_size =
      expected_tp->num_data_blocks *
      (kAvgSuccessorSize + (index_key_is_user_key ? 0 : 8) -
       // discount 1 byte as value size is not encoded in value delta encoding
       (value_delta_encoding ? 1 : 0));
  expected_tp->filter_size =
      kTableCount * ((kKeysPerTable * kBloomBitsPerKey + 7) / 8 +
                     /*average-ish overhead*/ CACHE_LINE_SIZE / 2);
}
}  // anonymous namespace

TEST_F(DBPropertiesTest, ValidatePropertyInfo) {
  for (const auto& ppt_name_and_info : InternalStats::ppt_name_to_info) {
    // If C++ gets a std::string_literal, this would be better to check at
    // compile-time using static_assert.
    ASSERT_TRUE(ppt_name_and_info.first.empty() ||
                !isdigit(ppt_name_and_info.first.back()));

    int count = 0;
    count += (ppt_name_and_info.second.handle_string == nullptr) ? 0 : 1;
    count += (ppt_name_and_info.second.handle_int == nullptr) ? 0 : 1;
    count += (ppt_name_and_info.second.handle_string_dbimpl == nullptr) ? 0 : 1;
    ASSERT_TRUE(count == 1);
  }
}

TEST_F(DBPropertiesTest, ValidateSampleNumber) {
  // When "max_open_files" is -1, we read all the files for
  // "rocksdb.estimate-num-keys" computation, which is the ground truth.
  // Otherwise, we sample 20 newest files to make an estimation.
  // Formula: lastest_20_files_active_key_ratio * total_files
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_stop_writes_trigger = 1000;
  DestroyAndReopen(options);
  int key = 0;
  for (int files = 20; files >= 10; files -= 10) {
    for (int i = 0; i < files; i++) {
      int rows = files / 10;
      for (int j = 0; j < rows; j++) {
        ASSERT_OK(db_->Put(WriteOptions(), std::to_string(++key), "foo"));
      }
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
  }
  std::string num;
  Reopen(options);
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ("45", num);
  options.max_open_files = -1;
  Reopen(options);
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ("50", num);
}

TEST_F(DBPropertiesTest, AggregatedTableProperties) {
  for (int kTableCount = 40; kTableCount <= 100; kTableCount += 30) {
    const int kDeletionsPerTable = 0;
    const int kMergeOperandsPerTable = 15;
    const int kRangeDeletionsPerTable = 5;
    const int kPutsPerTable = 100;
    const int kKeySize = 80;
    const int kValueSize = 200;
    const int kBloomBitsPerKey = 20;

    Options options = CurrentOptions();
    options.level0_file_num_compaction_trigger = 8;
    options.compression = kNoCompression;
    options.create_if_missing = true;
    options.merge_operator.reset(new TestPutOperator());

    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(
        NewBloomFilterPolicy(kBloomBitsPerKey, false));
    table_options.block_size = 1024;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    // The checks assume kTableCount number of files
    options.disable_auto_compactions = true;

    DestroyAndReopen(options);

    // Hold open a snapshot to prevent range tombstones from being compacted
    // away.
    ManagedSnapshot snapshot(db_.get());

    Random rnd(5632);
    for (int table = 1; table <= kTableCount; ++table) {
      for (int i = 0; i < kPutsPerTable; ++i) {
        ASSERT_OK(db_->Put(WriteOptions(), rnd.RandomString(kKeySize),
                           rnd.RandomString(kValueSize)));
      }
      for (int i = 0; i < kDeletionsPerTable; i++) {
        ASSERT_OK(db_->Delete(WriteOptions(), rnd.RandomString(kKeySize)));
      }
      for (int i = 0; i < kMergeOperandsPerTable; i++) {
        ASSERT_OK(db_->Merge(WriteOptions(), rnd.RandomString(kKeySize),
                             rnd.RandomString(kValueSize)));
      }
      for (int i = 0; i < kRangeDeletionsPerTable; i++) {
        std::string start = rnd.RandomString(kKeySize);
        std::string end = start;
        end.resize(kValueSize);
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   start, end));
      }
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
    std::string property;
    db_->GetProperty(DB::Properties::kAggregatedTableProperties, &property);
    TableProperties output_tp;
    ParseTablePropertiesString(property, &output_tp);
    bool index_key_is_user_key = output_tp.index_key_is_user_key > 0;
    bool value_is_delta_encoded = output_tp.index_value_is_delta_encoded > 0;

    TableProperties expected_tp;
    GetExpectedTableProperties(
        &expected_tp, kKeySize, kValueSize, kPutsPerTable, kDeletionsPerTable,
        kMergeOperandsPerTable, kRangeDeletionsPerTable, kTableCount,
        kBloomBitsPerKey, table_options.block_size, index_key_is_user_key,
        value_is_delta_encoded);

    VerifyTableProperties(expected_tp, output_tp);
  }
}

TEST_F(DBPropertiesTest, ReadLatencyHistogramByLevel) {
  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;
  options.level0_file_num_compaction_trigger = 6;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 4500 << 10;
  options.target_file_size_base = 98 << 10;
  options.max_write_buffer_number = 2;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.max_open_files = 11;  // Make sure no proloading of table readers

  // RocksDB sanitize max open files to at least 20. Modify it back.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = static_cast<int*>(arg);
        *max_open_files = 11;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;

  CreateAndReopenWithCF({"pikachu"}, options);
  int key_index = 0;
  Random rnd(301);
  for (int num = 0; num < 8; num++) {
    ASSERT_OK(Put("foo", "bar"));
    GenerateNewFile(&rnd, &key_index);
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  std::string prop;
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.dbstats", &prop));

  // Get() after flushes, See latency histogram tracked.
  for (int key = 0; key < key_index; key++) {
    Get(Key(key));
  }
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.cfstats", &prop));
  ASSERT_NE(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_NE(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));

  // Reopen and issue Get(). See thee latency tracked
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int key = 0; key < key_index; key++) {
    Get(Key(key));
  }

  // Test for getting immutable_db_options_.statistics
  ASSERT_TRUE(dbfull()->GetProperty(dbfull()->DefaultColumnFamily(),
                                    "rocksdb.options-statistics", &prop));
  ASSERT_NE(std::string::npos, prop.find("rocksdb.block.cache.miss"));
  ASSERT_EQ(std::string::npos, prop.find("rocksdb.db.f.micros"));

  ASSERT_TRUE(dbfull()->GetProperty(dbfull()->DefaultColumnFamily(),
                                    "rocksdb.cf-file-histogram", &prop));
  ASSERT_NE(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_NE(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));

  // Reopen and issue iterating. See thee latency tracked
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.cf-file-histogram", &prop));
  ASSERT_EQ(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    for (iter->Seek(Key(0)); iter->Valid(); iter->Next()) {
    }
    ASSERT_OK(iter->status());
  }
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.cf-file-histogram", &prop));
  ASSERT_NE(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_NE(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));

  // CF 1 should show no histogram.
  ASSERT_TRUE(
      dbfull()->GetProperty(handles_[1], "rocksdb.cf-file-histogram", &prop));
  ASSERT_EQ(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));
  // put something and read it back , CF 1 should show histogram.
  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("bar", Get(1, "foo"));

  ASSERT_TRUE(
      dbfull()->GetProperty(handles_[1], "rocksdb.cf-file-histogram", &prop));
  ASSERT_NE(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));

  // options.max_open_files preloads table readers.
  options.max_open_files = -1;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(dbfull()->GetProperty(dbfull()->DefaultColumnFamily(),
                                    "rocksdb.cf-file-histogram", &prop));
  ASSERT_NE(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_NE(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));
  for (int key = 0; key < key_index; key++) {
    Get(Key(key));
  }
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.cfstats", &prop));
  ASSERT_NE(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_NE(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));

  // Clear internal stats
  ASSERT_OK(dbfull()->ResetStats());
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.cfstats", &prop));
  ASSERT_EQ(std::string::npos, prop.find("** Level 0 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 1 read latency histogram"));
  ASSERT_EQ(std::string::npos, prop.find("** Level 2 read latency histogram"));
}

TEST_F(DBPropertiesTest, AggregatedTablePropertiesAtLevel) {
  const int kTableCount = 100;
  const int kDeletionsPerTable = 0;
  const int kMergeOperandsPerTable = 2;
  const int kRangeDeletionsPerTable = 2;
  const int kPutsPerTable = 10;
  const int kKeySize = 50;
  const int kValueSize = 400;
  const int kMaxLevel = 7;
  const int kBloomBitsPerKey = 20;
  Random rnd(301);
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 8;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.target_file_size_base = 8192;
  options.max_bytes_for_level_base = 10000;
  options.max_bytes_for_level_multiplier = 2;
  // The checks assume kTableCount number of files
  options.disable_auto_compactions = true;
  options.merge_operator.reset(new TestPutOperator());

  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(
      NewBloomFilterPolicy(kBloomBitsPerKey, false));
  table_options.block_size = 1024;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);

  // Hold open a snapshot to prevent range tombstones from being compacted away.
  ManagedSnapshot snapshot(db_.get());

  std::string level_tp_strings[kMaxLevel];
  std::string tp_string;
  TableProperties level_tps[kMaxLevel];
  TableProperties tp;
  TableProperties sum_tp;
  TableProperties expected_tp;
  for (int table = 1; table <= kTableCount; ++table) {
    for (int i = 0; i < kPutsPerTable; ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), rnd.RandomString(kKeySize),
                         rnd.RandomString(kValueSize)));
    }
    for (int i = 0; i < kDeletionsPerTable; i++) {
      ASSERT_OK(db_->Delete(WriteOptions(), rnd.RandomString(kKeySize)));
    }
    for (int i = 0; i < kMergeOperandsPerTable; i++) {
      ASSERT_OK(db_->Merge(WriteOptions(), rnd.RandomString(kKeySize),
                           rnd.RandomString(kValueSize)));
    }
    for (int i = 0; i < kRangeDeletionsPerTable; i++) {
      std::string start = rnd.RandomString(kKeySize);
      std::string end = start;
      end.resize(kValueSize);
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 start, end));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ResetTableProperties(&sum_tp);
    for (int level = 0; level < kMaxLevel; ++level) {
      db_->GetProperty(DB::Properties::kAggregatedTablePropertiesAtLevel +
                           std::to_string(level),
                       &level_tp_strings[level]);
      ParseTablePropertiesString(level_tp_strings[level], &level_tps[level]);
      sum_tp.data_size += level_tps[level].data_size;
      sum_tp.index_size += level_tps[level].index_size;
      sum_tp.filter_size += level_tps[level].filter_size;
      sum_tp.raw_key_size += level_tps[level].raw_key_size;
      sum_tp.raw_value_size += level_tps[level].raw_value_size;
      sum_tp.num_data_blocks += level_tps[level].num_data_blocks;
      sum_tp.num_entries += level_tps[level].num_entries;
      sum_tp.num_deletions += level_tps[level].num_deletions;
      sum_tp.num_merge_operands += level_tps[level].num_merge_operands;
      sum_tp.num_range_deletions += level_tps[level].num_range_deletions;
      sum_tp.standard_index_is_stub += level_tps[level].standard_index_is_stub;
    }
    db_->GetProperty(DB::Properties::kAggregatedTableProperties, &tp_string);
    ParseTablePropertiesString(tp_string, &tp);
    bool index_key_is_user_key = tp.index_key_is_user_key > 0;
    bool value_is_delta_encoded = tp.index_value_is_delta_encoded > 0;
    ASSERT_EQ(sum_tp.data_size, tp.data_size);
    ASSERT_EQ(sum_tp.index_size, tp.index_size);
    ASSERT_EQ(sum_tp.filter_size, tp.filter_size);
    ASSERT_EQ(sum_tp.raw_key_size, tp.raw_key_size);
    ASSERT_EQ(sum_tp.raw_value_size, tp.raw_value_size);
    ASSERT_EQ(sum_tp.num_data_blocks, tp.num_data_blocks);
    ASSERT_EQ(sum_tp.num_entries, tp.num_entries);
    ASSERT_EQ(sum_tp.num_deletions, tp.num_deletions);
    ASSERT_EQ(sum_tp.num_merge_operands, tp.num_merge_operands);
    ASSERT_EQ(sum_tp.num_range_deletions, tp.num_range_deletions);
    ASSERT_EQ(sum_tp.standard_index_is_stub, tp.standard_index_is_stub);
    if (table > 3) {
      GetExpectedTableProperties(
          &expected_tp, kKeySize, kValueSize, kPutsPerTable, kDeletionsPerTable,
          kMergeOperandsPerTable, kRangeDeletionsPerTable, table,
          kBloomBitsPerKey, table_options.block_size, index_key_is_user_key,
          value_is_delta_encoded);
      // Gives larger bias here as index block size, filter block size,
      // and data block size become much harder to estimate in this test.
      VerifyTableProperties(expected_tp, tp, CACHE_LINE_SIZE >= 256 ? 0.6 : 0.5,
                            0.5, 0.5, 0.25);
    }
  }
}

TEST_F(DBPropertiesTest, NumImmutableMemTable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    options.write_buffer_size = 1000000;
    options.max_write_buffer_size_to_maintain =
        5 * static_cast<int64_t>(options.write_buffer_size);
    CreateAndReopenWithCF({"pikachu"}, options);

    std::string big_value(1000000 * 2, 'x');
    std::string num;
    uint64_t value;
    SetPerfLevel(kEnableTime);
    ASSERT_TRUE(GetPerfLevel() == kEnableTime);

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k1", big_value));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "0");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], DB::Properties::kNumImmutableMemTableFlushed, &num));
    ASSERT_EQ(num, "0");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ(num, "1");
    get_perf_context()->Reset();
    Get(1, "k1");
    ASSERT_EQ(1, static_cast<int>(get_perf_context()->get_from_memtable_count));

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k2", big_value));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "1");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ(num, "1");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-imm-mem-tables", &num));
    ASSERT_EQ(num, "1");

    get_perf_context()->Reset();
    Get(1, "k1");
    ASSERT_EQ(2, static_cast<int>(get_perf_context()->get_from_memtable_count));
    get_perf_context()->Reset();
    Get(1, "k2");
    ASSERT_EQ(1, static_cast<int>(get_perf_context()->get_from_memtable_count));

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k3", big_value));
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.cur-size-active-mem-table", &num));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "2");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &num));
    ASSERT_EQ(num, "1");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], "rocksdb.num-entries-imm-mem-tables", &num));
    ASSERT_EQ(num, "2");
    get_perf_context()->Reset();
    Get(1, "k2");
    ASSERT_EQ(2, static_cast<int>(get_perf_context()->get_from_memtable_count));
    get_perf_context()->Reset();
    Get(1, "k3");
    ASSERT_EQ(1, static_cast<int>(get_perf_context()->get_from_memtable_count));
    get_perf_context()->Reset();
    Get(1, "k1");
    ASSERT_EQ(3, static_cast<int>(get_perf_context()->get_from_memtable_count));

    ASSERT_OK(Flush(1));
    ASSERT_TRUE(dbfull()->GetProperty(handles_[1],
                                      "rocksdb.num-immutable-mem-table", &num));
    ASSERT_EQ(num, "0");
    ASSERT_TRUE(dbfull()->GetProperty(
        handles_[1], DB::Properties::kNumImmutableMemTableFlushed, &num));
    ASSERT_EQ(num, "3");
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.cur-size-active-mem-table", &value));
    // "192" is the size of the metadata of two empty skiplists, this would
    // break if we change the default skiplist implementation
    ASSERT_GE(value, 192);

    uint64_t int_num;
    uint64_t base_total_size;
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.estimate-num-keys", &base_total_size));

    ASSERT_OK(dbfull()->Delete(writeOpt, handles_[1], "k2"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k3", ""));
    ASSERT_OK(dbfull()->Delete(writeOpt, handles_[1], "k3"));
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-deletes-active-mem-table", &int_num));
    ASSERT_EQ(int_num, 2U);
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-entries-active-mem-table", &int_num));
    ASSERT_EQ(int_num, 3U);

    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k2", big_value));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "k2", big_value));
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-entries-imm-mem-tables", &int_num));
    ASSERT_EQ(int_num, 4U);
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.num-deletes-imm-mem-tables", &int_num));
    ASSERT_EQ(int_num, 2U);

    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.estimate-num-keys", &int_num));
    ASSERT_EQ(int_num, base_total_size + 1);

    SetPerfLevel(kDisable);
    ASSERT_TRUE(GetPerfLevel() == kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBPropertiesTest, ConcurrentWriteMemTableProperties) {
  Options options = CurrentOptions();
  options.allow_concurrent_memtable_write = true;
  options.memtable_factory = std::make_shared<SkipListFactory>();
  options.flush_verify_memtable_count = true;
  DestroyAndReopen(options);

  // Multi-threaded writes that go through the concurrent Add() +
  // BatchPostProcess path.
  const int kNumThreads = 4;
  const int kEntriesPerThread = 100;
  std::vector<port::Thread> threads;
  threads.reserve(kNumThreads);
  for (int t = 0; t < kNumThreads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kEntriesPerThread; i++) {
        std::string key = "k_" + std::to_string(t) + "_" + std::to_string(i);
        ASSERT_OK(Put(key, "val"));
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  uint64_t num_entries;
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.num-entries-active-mem-table",
                                       &num_entries));
  ASSERT_EQ(num_entries,
            static_cast<uint64_t>(kNumThreads * kEntriesPerThread));

  // Write deletes and single deletes via concurrent path
  threads.clear();
  for (int t = 0; t < kNumThreads; t++) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kEntriesPerThread; i++) {
        std::string key = "d_" + std::to_string(t) + "_" + std::to_string(i);
        if (i % 2 == 0) {
          ASSERT_OK(Delete(key));
        } else {
          ASSERT_OK(SingleDelete(key));
        }
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }

  uint64_t total_entries;
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.num-entries-active-mem-table",
                                       &total_entries));
  ASSERT_EQ(total_entries,
            static_cast<uint64_t>(kNumThreads * kEntriesPerThread * 2));

  uint64_t num_deletes;
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.num-deletes-active-mem-table",
                                       &num_deletes));
  ASSERT_EQ(num_deletes,
            static_cast<uint64_t>(kNumThreads * kEntriesPerThread));

  // Flush exercises the integrity check (flush_verify_memtable_count = true).
  // If stat counters are wrong, this would return Corruption.
  ASSERT_OK(Flush());
}

// TODO(techdept) : Disabled flaky test #12863555
TEST_F(DBPropertiesTest, DISABLED_GetProperty) {
  // Set sizes to both background thread pool to be 1 and block them.
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  Options options = CurrentOptions();
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = 1;
  options.compaction_options_universal.size_ratio = 50;
  options.max_background_compactions = 1;
  options.max_background_flushes = 1;
  options.max_write_buffer_number = 10;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_size_to_maintain = 0;
  options.write_buffer_size = 1000000;
  Reopen(options);

  std::string big_value(1000000 * 2, 'x');
  std::string num;
  uint64_t int_num;
  SetPerfLevel(kEnableTime);

  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-live-data-size", &int_num));
  ASSERT_EQ(int_num, 0U);

  ASSERT_OK(dbfull()->Put(writeOpt, "k1", big_value));
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.mem-table-flush-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.compaction-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ(num, "1");
  get_perf_context()->Reset();

  ASSERT_OK(dbfull()->Put(writeOpt, "k2", big_value));
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
  ASSERT_EQ(num, "1");
  ASSERT_OK(dbfull()->Delete(writeOpt, "k-non-existing"));
  ASSERT_OK(dbfull()->Put(writeOpt, "k3", big_value));
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.num-immutable-mem-table", &num));
  ASSERT_EQ(num, "2");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.mem-table-flush-pending", &num));
  ASSERT_EQ(num, "1");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.compaction-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ(num, "2");
  // Verify the same set of properties through GetIntProperty
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.num-immutable-mem-table", &int_num));
  ASSERT_EQ(int_num, 2U);
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.mem-table-flush-pending", &int_num));
  ASSERT_EQ(int_num, 1U);
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.compaction-pending", &int_num));
  ASSERT_EQ(int_num, 0U);
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.estimate-num-keys", &int_num));
  ASSERT_EQ(int_num, 2U);

  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
  dbfull()->TEST_WaitForFlushMemTable();

  ASSERT_OK(dbfull()->Put(writeOpt, "k4", big_value));
  ASSERT_OK(dbfull()->Put(writeOpt, "k5", big_value));
  dbfull()->TEST_WaitForFlushMemTable();
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.mem-table-flush-pending", &num));
  ASSERT_EQ(num, "0");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.compaction-pending", &num));
  ASSERT_EQ(num, "1");
  ASSERT_TRUE(dbfull()->GetProperty("rocksdb.estimate-num-keys", &num));
  ASSERT_EQ(num, "4");

  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_GT(int_num, 0U);

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  // Wait for compaction to be done. This is important because otherwise RocksDB
  // might schedule a compaction when reopening the database, failing assertion
  // (A) as a result.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  options.max_open_files = 10;
  Reopen(options);
  // After reopening, no table reader is loaded, so no memory for table readers
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_EQ(int_num, 0U);  // (A)
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.estimate-num-keys", &int_num));
  ASSERT_GT(int_num, 0U);

  // After reading a key, at least one table reader is loaded.
  Get("k5");
  ASSERT_TRUE(
      dbfull()->GetIntProperty("rocksdb.estimate-table-readers-mem", &int_num));
  ASSERT_GT(int_num, 0U);

  // Test rocksdb.num-live-versions
  {
    options.level0_file_num_compaction_trigger = 20;
    Reopen(options);
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 1U);

    // Use an iterator to hold current version
    std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));

    ASSERT_OK(dbfull()->Put(writeOpt, "k6", big_value));
    ASSERT_OK(Flush());
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 2U);

    // Use an iterator to hold current version
    std::unique_ptr<Iterator> iter2(dbfull()->NewIterator(ReadOptions()));

    ASSERT_OK(dbfull()->Put(writeOpt, "k7", big_value));
    ASSERT_OK(Flush());
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 3U);

    iter2.reset();
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 2U);

    iter1.reset();
    ASSERT_TRUE(
        dbfull()->GetIntProperty("rocksdb.num-live-versions", &int_num));
    ASSERT_EQ(int_num, 1U);
  }
}

TEST_F(DBPropertiesTest, ApproximateMemoryUsage) {
  const int kNumRounds = 10;
  // TODO(noetzli) kFlushesPerRound does not really correlate with how many
  // flushes happen.
  const int kFlushesPerRound = 10;
  const int kWritesPerFlush = 10;
  const int kKeySize = 100;
  const int kValueSize = 1000;
  Options options;
  options.write_buffer_size = 1000;  // small write buffer
  options.min_write_buffer_number_to_merge = 4;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  Random rnd(301);

  std::vector<Iterator*> iters;

  uint64_t active_mem;
  uint64_t unflushed_mem;
  uint64_t all_mem;
  uint64_t prev_all_mem;

  // Phase 0. The verify the initial value of all these properties are the same
  // as we have no mem-tables.
  dbfull()->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);
  dbfull()->GetIntProperty("rocksdb.cur-size-all-mem-tables", &unflushed_mem);
  dbfull()->GetIntProperty("rocksdb.size-all-mem-tables", &all_mem);
  ASSERT_EQ(all_mem, active_mem);
  ASSERT_EQ(all_mem, unflushed_mem);

  // Phase 1. Simply issue Put() and expect "cur-size-all-mem-tables" equals to
  // "size-all-mem-tables"
  for (int r = 0; r < kNumRounds; ++r) {
    for (int f = 0; f < kFlushesPerRound; ++f) {
      for (int w = 0; w < kWritesPerFlush; ++w) {
        ASSERT_OK(
            Put(rnd.RandomString(kKeySize), rnd.RandomString(kValueSize)));
      }
    }
    // Make sure that there is no flush between getting the two properties.
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    dbfull()->GetIntProperty("rocksdb.cur-size-all-mem-tables", &unflushed_mem);
    dbfull()->GetIntProperty("rocksdb.size-all-mem-tables", &all_mem);
    // in no iterator case, these two number should be the same.
    ASSERT_EQ(unflushed_mem, all_mem);
  }
  prev_all_mem = all_mem;

  // Phase 2. Keep issuing Put() but also create new iterators. This time we
  // expect "size-all-mem-tables" > "cur-size-all-mem-tables".
  for (int r = 0; r < kNumRounds; ++r) {
    iters.push_back(db_->NewIterator(ReadOptions()));
    for (int f = 0; f < kFlushesPerRound; ++f) {
      for (int w = 0; w < kWritesPerFlush; ++w) {
        ASSERT_OK(
            Put(rnd.RandomString(kKeySize), rnd.RandomString(kValueSize)));
      }
    }
    // Force flush to prevent flush from happening between getting the
    // properties or after getting the properties and before the new round.
    ASSERT_OK(Flush());

    // In the second round, add iterators.
    dbfull()->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);
    dbfull()->GetIntProperty("rocksdb.cur-size-all-mem-tables", &unflushed_mem);
    dbfull()->GetIntProperty("rocksdb.size-all-mem-tables", &all_mem);
    ASSERT_GT(all_mem, active_mem);
    ASSERT_GT(all_mem, unflushed_mem);
    ASSERT_GT(all_mem, prev_all_mem);
    prev_all_mem = all_mem;
  }

  // Phase 3. Delete iterators and expect "size-all-mem-tables" shrinks
  // whenever we release an iterator.
  for (auto* iter : iters) {
    ASSERT_OK(iter->status());
    delete iter;
    dbfull()->GetIntProperty("rocksdb.size-all-mem-tables", &all_mem);
    // Expect the size shrinking
    ASSERT_LT(all_mem, prev_all_mem);
    prev_all_mem = all_mem;
  }

  // Expect all these three counters to be the same.
  dbfull()->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);
  dbfull()->GetIntProperty("rocksdb.cur-size-all-mem-tables", &unflushed_mem);
  dbfull()->GetIntProperty("rocksdb.size-all-mem-tables", &all_mem);
  ASSERT_EQ(active_mem, unflushed_mem);
  ASSERT_EQ(unflushed_mem, all_mem);

  // Phase 5. Reopen, and expect all these three counters to be the same again.
  Reopen(options);
  dbfull()->GetIntProperty("rocksdb.cur-size-active-mem-table", &active_mem);
  dbfull()->GetIntProperty("rocksdb.cur-size-all-mem-tables", &unflushed_mem);
  dbfull()->GetIntProperty("rocksdb.size-all-mem-tables", &all_mem);
  ASSERT_EQ(active_mem, unflushed_mem);
  ASSERT_EQ(unflushed_mem, all_mem);
}

TEST_F(DBPropertiesTest, EstimatePendingCompBytes) {
  // Set sizes to both background thread pool to be 1 and block them.
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  Options options = CurrentOptions();
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 2;
  options.max_background_compactions = 1;
  options.max_background_flushes = 1;
  options.max_write_buffer_number = 10;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_size_to_maintain = 0;
  options.write_buffer_size = 1000000;
  Reopen(options);

  std::string big_value(1000000 * 2, 'x');
  std::string num;
  uint64_t int_num;

  ASSERT_OK(dbfull()->Put(writeOpt, "k1", big_value));
  ASSERT_OK(Flush());
  ASSERT_TRUE(dbfull()->GetIntProperty(
      "rocksdb.estimate-pending-compaction-bytes", &int_num));
  ASSERT_EQ(int_num, 0U);

  ASSERT_OK(dbfull()->Put(writeOpt, "k2", big_value));
  ASSERT_OK(Flush());
  ASSERT_TRUE(dbfull()->GetIntProperty(
      "rocksdb.estimate-pending-compaction-bytes", &int_num));
  ASSERT_GT(int_num, 0U);

  ASSERT_OK(dbfull()->Put(writeOpt, "k3", big_value));
  ASSERT_OK(Flush());
  ASSERT_TRUE(dbfull()->GetIntProperty(
      "rocksdb.estimate-pending-compaction-bytes", &int_num));
  ASSERT_GT(int_num, 0U);

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(dbfull()->GetIntProperty(
      "rocksdb.estimate-pending-compaction-bytes", &int_num));
  ASSERT_EQ(int_num, 0U);
}

TEST_F(DBPropertiesTest, NumUnscheduledCompactions) {
  if (env_ == nullptr) {
    ADD_FAILURE() << "Missing test environment";
    return;
  }
  auto& env = *env_;
  env.SetBackgroundThreads(1, Env::HIGH);
  env.SetBackgroundThreads(1, Env::LOW);

  Options options = CurrentOptions();
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 2;
  options.max_background_compactions = 1;
  options.max_background_flushes = 1;
  options.write_buffer_size = 1000000;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"one"}, options);

  uint64_t int_num;

  ASSERT_TRUE(dbfull()->GetIntProperty(
      DB::Properties::kNumUnscheduledCompactions, &int_num));
  ASSERT_EQ(int_num, 0U);

  // Keep one scheduled compaction from completing.
  test::SleepingBackgroundTask sleeping_task_low;
  env.Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
               Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();

  std::string big_value(1000000 * 2, 'x');
  for (int cf = 0; cf < 2; cf++) {
    for (int i = 0; i < 4; i++) {
      ASSERT_OK(dbfull()->Put(writeOpt, handles_[cf], "k" + std::to_string(i),
                              big_value));
      ASSERT_OK(Flush(cf));
    }
  }

  ASSERT_TRUE(dbfull()->GetIntProperty(
      DB::Properties::kNumUnscheduledCompactions, &int_num));
  ASSERT_GT(int_num, 0U);

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_TRUE(dbfull()->GetIntProperty(
      DB::Properties::kNumUnscheduledCompactions, &int_num));
  ASSERT_EQ(int_num, 0U);
}

TEST_F(DBPropertiesTest, EstimateCompressionRatio) {
  if (!Snappy_Supported()) {
    return;
  }
  const int kNumL0Files = 3;
  const int kNumEntriesPerFile = 1000;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 3;
  Reopen(options);

  ASSERT_OK(db_->SetOptions(
      {{"compression_per_level", "kNoCompression:kSnappyCompression"}}));
  auto opts = db_->GetOptions();
  ASSERT_EQ(opts.compression_per_level.size(), 2);
  ASSERT_EQ(opts.compression_per_level[0], kNoCompression);
  ASSERT_EQ(opts.compression_per_level[1], kSnappyCompression);

  // compression ratio is -1.0 when no open files at level
  ASSERT_EQ(CompressionRatioAtLevel(0), -1.0);

  const std::string kVal(100, 'a');
  for (int i = 0; i < kNumL0Files; ++i) {
    for (int j = 0; j < kNumEntriesPerFile; ++j) {
      // Put common data ("key") at end to prevent delta encoding from
      // compressing the key effectively
      std::string key = std::to_string(i) + std::to_string(j) + "key";
      ASSERT_OK(dbfull()->Put(WriteOptions(), key, kVal));
    }
    ASSERT_OK(Flush());
  }

  // no compression at L0, so ratio is less than one
  ASSERT_LT(CompressionRatioAtLevel(0), 1.0);
  ASSERT_GT(CompressionRatioAtLevel(0), 0.0);
  ASSERT_EQ(CompressionRatioAtLevel(1), -1.0);

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));

  ASSERT_EQ(CompressionRatioAtLevel(0), -1.0);
  // Data at L1 should be highly compressed thanks to Snappy and redundant data
  // in values (ratio is 12.846 as of 4/19/2016).
  ASSERT_GT(CompressionRatioAtLevel(1), 10.0);
}

class CountingUserTblPropCollector : public TablePropertiesCollector {
 public:
  const char* Name() const override { return "CountingUserTblPropCollector"; }

  Status Finish(UserCollectedProperties* properties) override {
    assert(!finish_called_);
    std::string encoded;
    PutVarint32(&encoded, count_);
    *properties = UserCollectedProperties{
        {"CountingUserTblPropCollector", message_},
        {"Count", encoded},
    };
    finish_called_ = true;
    return Status::OK();
  }

  Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                    EntryType /*type*/, SequenceNumber /*seq*/,
                    uint64_t /*file_size*/) override {
    ++count_;
    return Status::OK();
  }

  UserCollectedProperties GetReadableProperties() const override {
    assert(finish_called_);
    return UserCollectedProperties{};
  }

 private:
  std::string message_ = "Rocksdb";
  uint32_t count_ = 0;
  bool finish_called_ = false;
};

class CountingUserTblPropCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  explicit CountingUserTblPropCollectorFactory(
      uint32_t expected_column_family_id)
      : expected_column_family_id_(expected_column_family_id),
        num_created_(0) {}
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override {
    EXPECT_EQ(expected_column_family_id_, context.column_family_id);
    num_created_++;
    return new CountingUserTblPropCollector();
  }
  const char* Name() const override {
    return "CountingUserTblPropCollectorFactory";
  }
  void set_expected_column_family_id(uint32_t v) {
    expected_column_family_id_ = v;
  }
  uint32_t expected_column_family_id_;
  uint32_t num_created_;
};

class CountingDeleteTabPropCollector : public TablePropertiesCollector {
 public:
  const char* Name() const override { return "CountingDeleteTabPropCollector"; }

  Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                    EntryType type, SequenceNumber /*seq*/,
                    uint64_t /*file_size*/) override {
    if (type == kEntryDelete) {
      num_deletes_++;
    }
    return Status::OK();
  }

  bool NeedCompact() const override { return num_deletes_ > 10; }

  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties{};
  }

  Status Finish(UserCollectedProperties* properties) override {
    *properties =
        UserCollectedProperties{{"num_delete", std::to_string(num_deletes_)}};
    return Status::OK();
  }

 private:
  uint32_t num_deletes_ = 0;
};

class CountingDeleteTabPropCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return new CountingDeleteTabPropCollector();
  }
  const char* Name() const override {
    return "CountingDeleteTabPropCollectorFactory";
  }
};

class BlockCountingTablePropertiesCollector : public TablePropertiesCollector {
 public:
  static const std::string kNumSampledBlocksPropertyName;

  const char* Name() const override {
    return "BlockCountingTablePropertiesCollector";
  }

  Status Finish(UserCollectedProperties* properties) override {
    (*properties)[kNumSampledBlocksPropertyName] =
        std::to_string(num_sampled_blocks_);
    return Status::OK();
  }

  Status AddUserKey(const Slice& /*user_key*/, const Slice& /*value*/,
                    EntryType /*type*/, SequenceNumber /*seq*/,
                    uint64_t /*file_size*/) override {
    return Status::OK();
  }

  void BlockAdd(uint64_t /* block_uncomp_bytes */,
                uint64_t block_compressed_bytes_fast,
                uint64_t block_compressed_bytes_slow) override {
    if (block_compressed_bytes_fast > 0 || block_compressed_bytes_slow > 0) {
      num_sampled_blocks_++;
    }
  }

  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties{
        {kNumSampledBlocksPropertyName, std::to_string(num_sampled_blocks_)},
    };
  }

 private:
  uint32_t num_sampled_blocks_ = 0;
};

const std::string
    BlockCountingTablePropertiesCollector::kNumSampledBlocksPropertyName =
        "NumSampledBlocks";

class BlockCountingTablePropertiesCollectorFactory
    : public TablePropertiesCollectorFactory {
 public:
  const char* Name() const override {
    return "BlockCountingTablePropertiesCollectorFactory";
  }

  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /* context */) override {
    return new BlockCountingTablePropertiesCollector();
  }
};

TEST_F(DBPropertiesTest, GetUserDefinedTableProperties) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = (1 << 30);
  options.table_properties_collector_factories.resize(1);
  std::shared_ptr<CountingUserTblPropCollectorFactory> collector_factory =
      std::make_shared<CountingUserTblPropCollectorFactory>(0);
  options.table_properties_collector_factories[0] = collector_factory;
  Reopen(options);
  // Create 4 tables
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(
          db_->Put(WriteOptions(), std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }

  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_EQ(4U, props.size());
  uint32_t sum = 0;
  for (const auto& item : props) {
    auto& user_collected = item.second->user_collected_properties;
    ASSERT_TRUE(user_collected.find("CountingUserTblPropCollector") !=
                user_collected.end());
    ASSERT_EQ(user_collected.at("CountingUserTblPropCollector"), "Rocksdb");
    ASSERT_TRUE(user_collected.find("Count") != user_collected.end());
    Slice key(user_collected.at("Count"));
    uint32_t count;
    ASSERT_TRUE(GetVarint32(&key, &count));
    sum += count;
  }
  ASSERT_EQ(10u + 11u + 12u + 13u, sum);

  ASSERT_GT(collector_factory->num_created_, 0U);
  collector_factory->num_created_ = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_GT(collector_factory->num_created_, 0U);
}

TEST_F(DBPropertiesTest, UserDefinedTablePropertiesContext) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 3;
  options.table_properties_collector_factories.resize(1);
  std::shared_ptr<CountingUserTblPropCollectorFactory> collector_factory =
      std::make_shared<CountingUserTblPropCollectorFactory>(1);
  options.table_properties_collector_factories[0] = collector_factory,
  CreateAndReopenWithCF({"pikachu"}, options);
  // Create 2 files
  for (int table = 0; table < 2; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(Put(1, std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(Flush(1));
  }
  ASSERT_GT(collector_factory->num_created_, 0U);

  collector_factory->num_created_ = 0;
  // Trigger automatic compactions.
  for (int table = 0; table < 3; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(Put(1, std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_GT(collector_factory->num_created_, 0U);

  collector_factory->num_created_ = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
  ASSERT_GT(collector_factory->num_created_, 0U);

  // Come back to write to default column family
  collector_factory->num_created_ = 0;
  collector_factory->set_expected_column_family_id(0);  // default CF
  // Create 4 tables in default column family
  for (int table = 0; table < 2; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(Put(std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_GT(collector_factory->num_created_, 0U);

  collector_factory->num_created_ = 0;
  // Trigger automatic compactions.
  for (int table = 0; table < 3; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(Put(std::to_string(table * 100 + i), "val"));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_GT(collector_factory->num_created_, 0U);

  collector_factory->num_created_ = 0;
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_GT(collector_factory->num_created_, 0U);
}

TEST_F(DBPropertiesTest, TablePropertiesNeedCompactTest) {
  Random rnd(301);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 4096;
  options.max_write_buffer_number = 8;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 4;
  options.target_file_size_base = 2048;
  options.max_bytes_for_level_base = 10240;
  options.max_bytes_for_level_multiplier = 4;
  options.soft_pending_compaction_bytes_limit = 1024 * 1024;
  options.num_levels = 8;
  options.env = env_;

  std::shared_ptr<TablePropertiesCollectorFactory> collector_factory =
      std::make_shared<CountingDeleteTabPropCollectorFactory>();
  options.table_properties_collector_factories.resize(1);
  options.table_properties_collector_factories[0] = collector_factory;

  DestroyAndReopen(options);

  const int kMaxKey = 1000;
  for (int i = 0; i < kMaxKey; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(102)));
    ASSERT_OK(Put(Key(kMaxKey + i), rnd.RandomString(102)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  if (NumTableFilesAtLevel(0) == 1) {
    // Clear Level 0 so that when later flush a file with deletions,
    // we don't trigger an organic compaction.
    ASSERT_OK(Put(Key(0), ""));
    ASSERT_OK(Put(Key(kMaxKey * 2), ""));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  {
    int c = 0;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek(Key(kMaxKey - 100));
    while (iter->Valid() && iter->key().compare(Key(kMaxKey + 100)) < 0) {
      iter->Next();
      ++c;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(c, 200);
  }

  ASSERT_OK(Delete(Key(0)));
  for (int i = kMaxKey - 100; i < kMaxKey + 100; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Delete(Key(kMaxKey * 2)));

  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  {
    SetPerfLevel(kEnableCount);
    get_perf_context()->Reset();
    int c = 0;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek(Key(kMaxKey - 100));
    while (iter->Valid() && iter->key().compare(Key(kMaxKey + 100)) < 0) {
      iter->Next();
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(c, 0);
    ASSERT_LT(get_perf_context()->internal_delete_skipped_count, 30u);
    ASSERT_LT(get_perf_context()->internal_key_skipped_count, 30u);
    SetPerfLevel(kDisable);
  }
}

TEST_F(DBPropertiesTest, NeedCompactHintPersistentTest) {
  Random rnd(301);

  Options options;
  options.create_if_missing = true;
  options.max_write_buffer_number = 8;
  options.level0_file_num_compaction_trigger = 10;
  options.level0_slowdown_writes_trigger = 10;
  options.level0_stop_writes_trigger = 10;
  options.disable_auto_compactions = true;
  options.env = env_;

  std::shared_ptr<TablePropertiesCollectorFactory> collector_factory =
      std::make_shared<CountingDeleteTabPropCollectorFactory>();
  options.table_properties_collector_factories.resize(1);
  options.table_properties_collector_factories[0] = collector_factory;

  DestroyAndReopen(options);

  const int kMaxKey = 100;
  for (int i = 0; i < kMaxKey; i++) {
    ASSERT_OK(Put(Key(i), ""));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  for (int i = 1; i < kMaxKey - 1; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);

  // Restart the DB. Although number of files didn't reach
  // options.level0_file_num_compaction_trigger, compaction should
  // still be triggered because of the need-compaction hint.
  options.disable_auto_compactions = false;
  Reopen(options);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  {
    SetPerfLevel(kEnableCount);
    get_perf_context()->Reset();
    int c = 0;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    for (iter->Seek(Key(0)); iter->Valid(); iter->Next()) {
      c++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(c, 2);
    ASSERT_EQ(get_perf_context()->internal_delete_skipped_count, 0);
    // We iterate every key twice. Is it a bug?
    ASSERT_LE(get_perf_context()->internal_key_skipped_count, 2);
    SetPerfLevel(kDisable);
  }
}

// Excluded from RocksDB lite tests due to `GetPropertiesOfAllTables()` usage.
TEST_F(DBPropertiesTest, BlockAddForCompressionSampling) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.table_properties_collector_factories.emplace_back(
      std::make_shared<BlockCountingTablePropertiesCollectorFactory>());
  options.compression = kNoCompression;

  bool fast_sampling_supported = Snappy_Supported() || LZ4_Supported();
  bool slow_sampling_supported = ZSTD_Supported() || Zlib_Supported();

  for (bool sample_for_compression : {false, true}) {
    // For simplicity/determinism, sample 100% when enabled, or 0% when disabled
    options.sample_for_compression = sample_for_compression ? 1 : 0;

    DestroyAndReopen(options);

    // Setup the following LSM:
    //
    // L0_0 ["a", "b"]
    // L1_0 ["a", "b"]
    //
    // L0_0 was created by flush. L1_0 was created by compaction. Each file
    // contains one data block with enough data to be compressible.
    for (int i = 0; i < 3; ++i) {
      for (int j = 0; j < 50; ++j) {
        ASSERT_OK(Put(std::to_string(j), "thisismyvalue"));
      }
      ASSERT_OK(Flush());
      if (i == 1) {
        ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      }
    }

    // A `BlockAdd()` should have been seen for files generated by flush or
    // compaction when `sample_for_compression` is enabled.
    TablePropertiesCollection file_to_props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&file_to_props));
    ASSERT_EQ(2, file_to_props.size());
    for (const auto& file_and_props : file_to_props) {
      auto& props = *file_and_props.second;
      auto& user_props = props.user_collected_properties;
      ASSERT_TRUE(user_props.find(BlockCountingTablePropertiesCollector::
                                      kNumSampledBlocksPropertyName) !=
                  user_props.end());
      ASSERT_EQ(user_props.at(BlockCountingTablePropertiesCollector::
                                  kNumSampledBlocksPropertyName),
                std::to_string(sample_for_compression ? 1 : 0));
      if (sample_for_compression) {
        EXPECT_GT(props.fast_compression_estimated_data_size, 0);
        EXPECT_GT(props.slow_compression_estimated_data_size, 0);
        if (fast_sampling_supported) {
          EXPECT_LT(props.fast_compression_estimated_data_size,
                    props.data_size);
          if (slow_sampling_supported) {
            EXPECT_LT(props.slow_compression_estimated_data_size,
                      props.fast_compression_estimated_data_size);
          }
        }
        if (slow_sampling_supported) {
          EXPECT_LT(props.slow_compression_estimated_data_size,
                    props.data_size);
        }
      } else {
        EXPECT_EQ(props.fast_compression_estimated_data_size, 0);
        EXPECT_EQ(props.slow_compression_estimated_data_size, 0);
      }
    }
  }
}

class CompressionSamplingDBPropertiesTest
    : public DBPropertiesTest,
      public ::testing::WithParamInterface<bool> {
 public:
  CompressionSamplingDBPropertiesTest() : fast_(GetParam()) {}

 protected:
  const bool fast_;
};

INSTANTIATE_TEST_CASE_P(CompressionSamplingDBPropertiesTest,
                        CompressionSamplingDBPropertiesTest, ::testing::Bool());

// Excluded from RocksDB lite tests due to `GetPropertiesOfAllTables()` usage.
TEST_P(CompressionSamplingDBPropertiesTest,
       EstimateDataSizeWithCompressionSampling) {
  Options options = CurrentOptions();
  if (fast_) {
    // One of the following light compression libraries must be present.
    if (LZ4_Supported()) {
      options.compression = kLZ4Compression;
    } else if (Snappy_Supported()) {
      options.compression = kSnappyCompression;
    } else {
      return;
    }
  } else {
    // One of the following heavy compression libraries must be present.
    if (ZSTD_Supported()) {
      options.compression = kZSTD;
    } else if (Zlib_Supported()) {
      options.compression = kZlibCompression;
    } else {
      return;
    }
  }
  options.disable_auto_compactions = true;
  // For simplicity/determinism, sample 100%.
  options.sample_for_compression = 1;
  Reopen(options);

  // Setup the following LSM:
  //
  // L0_0 ["a", "b"]
  // L1_0 ["a", "b"]
  //
  // L0_0 was created by flush. L1_0 was created by compaction. Each file
  // contains one data block. The value consists of compressible data so the
  // data block should be stored compressed.
  std::string val(1024, 'a');
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("a", val));
    ASSERT_OK(Put("b", val));
    ASSERT_OK(Flush());
    if (i == 1) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }
  }

  TablePropertiesCollection file_to_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&file_to_props));
  ASSERT_EQ(2, file_to_props.size());
  for (const auto& file_and_props : file_to_props) {
    ASSERT_GT(file_and_props.second->data_size, 0);
    if (fast_) {
      ASSERT_EQ(file_and_props.second->data_size,
                file_and_props.second->fast_compression_estimated_data_size);
    } else {
      ASSERT_EQ(file_and_props.second->data_size,
                file_and_props.second->slow_compression_estimated_data_size);
    }
  }
}

TEST_F(DBPropertiesTest, EstimateNumKeysUnderflow) {
  Options options = CurrentOptions();
  Reopen(options);
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Delete("foo"));
  ASSERT_OK(Delete("foo"));
  uint64_t num_keys = 0;
  ASSERT_TRUE(dbfull()->GetIntProperty("rocksdb.estimate-num-keys", &num_keys));
  ASSERT_EQ(0, num_keys);
}

TEST_F(DBPropertiesTest, EstimateOldestKeyTime) {
  uint64_t oldest_key_time = 0;
  Options options = CurrentOptions();
  SetTimeElapseOnlySleepOnReopen(&options);

  // "rocksdb.estimate-oldest-key-time" only available to fifo compaction.
  for (auto compaction : {kCompactionStyleLevel, kCompactionStyleUniversal,
                          kCompactionStyleNone}) {
    options.compaction_style = compaction;
    options.create_if_missing = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_FALSE(dbfull()->GetIntProperty(
        DB::Properties::kEstimateOldestKeyTime, &oldest_key_time));
  }

  int64_t mock_start_time;
  ASSERT_OK(env_->GetCurrentTime(&mock_start_time));

  options.compaction_style = kCompactionStyleFIFO;
  options.ttl = 300;
  options.max_open_files = -1;
  options.compaction_options_fifo.allow_compaction = false;
  DestroyAndReopen(options);

  env_->MockSleepForSeconds(100);
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_TRUE(dbfull()->GetIntProperty(DB::Properties::kEstimateOldestKeyTime,
                                       &oldest_key_time));
  ASSERT_EQ(100, oldest_key_time - mock_start_time);
  ASSERT_OK(Flush());
  ASSERT_EQ("1", FilesPerLevel());
  ASSERT_TRUE(dbfull()->GetIntProperty(DB::Properties::kEstimateOldestKeyTime,
                                       &oldest_key_time));
  ASSERT_EQ(100, oldest_key_time - mock_start_time);

  env_->MockSleepForSeconds(100);  // -> 200
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());
  ASSERT_EQ("2", FilesPerLevel());
  ASSERT_TRUE(dbfull()->GetIntProperty(DB::Properties::kEstimateOldestKeyTime,
                                       &oldest_key_time));
  ASSERT_EQ(100, oldest_key_time - mock_start_time);

  env_->MockSleepForSeconds(100);  // -> 300
  ASSERT_OK(Put("k3", "v3"));
  ASSERT_OK(Flush());
  ASSERT_EQ("3", FilesPerLevel());
  ASSERT_TRUE(dbfull()->GetIntProperty(DB::Properties::kEstimateOldestKeyTime,
                                       &oldest_key_time));
  ASSERT_EQ(100, oldest_key_time - mock_start_time);

  env_->MockSleepForSeconds(150);  // -> 450
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("2", FilesPerLevel());
  ASSERT_TRUE(dbfull()->GetIntProperty(DB::Properties::kEstimateOldestKeyTime,
                                       &oldest_key_time));
  ASSERT_EQ(200, oldest_key_time - mock_start_time);

  env_->MockSleepForSeconds(100);  // -> 550
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("1", FilesPerLevel());
  ASSERT_TRUE(dbfull()->GetIntProperty(DB::Properties::kEstimateOldestKeyTime,
                                       &oldest_key_time));
  ASSERT_EQ(300, oldest_key_time - mock_start_time);

  env_->MockSleepForSeconds(100);  // -> 650
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("", FilesPerLevel());
  ASSERT_FALSE(dbfull()->GetIntProperty(DB::Properties::kEstimateOldestKeyTime,
                                        &oldest_key_time));
}

TEST_F(DBPropertiesTest, SstFilesSize) {
  struct TestListener : public EventListener {
    void OnCompactionCompleted(DB* db,
                               const CompactionJobInfo& /*info*/) override {
      assert(callback_triggered == false);
      assert(size_before_compaction > 0);
      callback_triggered = true;
      uint64_t total_sst_size = 0;
      uint64_t live_sst_size = 0;
      bool ok = db->GetIntProperty(DB::Properties::kTotalSstFilesSize,
                                   &total_sst_size);
      ASSERT_TRUE(ok);
      // total_sst_size include files before and after compaction.
      ASSERT_GT(total_sst_size, size_before_compaction);
      ok =
          db->GetIntProperty(DB::Properties::kLiveSstFilesSize, &live_sst_size);
      ASSERT_TRUE(ok);
      // live_sst_size only include files after compaction.
      ASSERT_GT(live_sst_size, 0);
      ASSERT_LT(live_sst_size, size_before_compaction);
    }

    uint64_t size_before_compaction = 0;
    bool callback_triggered = false;
  };
  std::shared_ptr<TestListener> listener = std::make_shared<TestListener>();

  Options options;
  options.env = CurrentOptions().env;
  options.disable_auto_compactions = true;
  options.listeners.push_back(listener);
  Reopen(options);

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("key" + std::to_string(i), std::string(1000, 'v')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  uint64_t sst_size;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kTotalSstFilesSize, &sst_size));
  ASSERT_GT(sst_size, 0);
  listener->size_before_compaction = sst_size;

  uint64_t obsolete_sst_size;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kObsoleteSstFilesSize,
                                  &obsolete_sst_size));
  ASSERT_EQ(obsolete_sst_size, 0);

  // Hold files from being deleted so we can test property for size of obsolete
  // SST files.
  ASSERT_OK(db_->DisableFileDeletions());

  // Compact to clean all keys and trigger listener.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(listener->callback_triggered);

  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kObsoleteSstFilesSize,
                                  &obsolete_sst_size));
  ASSERT_EQ(obsolete_sst_size, sst_size);

  // Let the obsolete files be deleted.
  ASSERT_OK(db_->EnableFileDeletions());
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kObsoleteSstFilesSize,
                                  &obsolete_sst_size));
  ASSERT_EQ(obsolete_sst_size, 0);
}

TEST_F(DBPropertiesTest, MinObsoleteSstNumberToKeep) {
  class TestListener : public EventListener {
   public:
    void OnTableFileCreated(const TableFileCreationInfo& info) override {
      if (info.reason == TableFileCreationReason::kCompaction) {
        // Verify the property indicates that SSTs created by a running
        // compaction cannot be deleted.
        uint64_t created_file_num;
        FileType created_file_type;
        std::string filename =
            info.file_path.substr(info.file_path.rfind('/') + 1);
        ASSERT_TRUE(
            ParseFileName(filename, &created_file_num, &created_file_type));
        ASSERT_EQ(kTableFile, created_file_type);

        uint64_t keep_sst_lower_bound;
        ASSERT_TRUE(
            db_->GetIntProperty(DB::Properties::kMinObsoleteSstNumberToKeep,
                                &keep_sst_lower_bound));

        ASSERT_LE(keep_sst_lower_bound, created_file_num);
        validated_ = true;
      }
    }

    void SetDB(DB* db) { db_ = db; }

    // True if we've verified the property for at least one output file
    bool Validated() { return validated_; }

   private:
    bool validated_ = false;
    DB* db_ = nullptr;
  };

  const int kNumL0Files = 4;

  std::shared_ptr<TestListener> listener = std::make_shared<TestListener>();

  Options options = CurrentOptions();
  options.listeners.push_back(listener);
  options.level0_file_num_compaction_trigger = kNumL0Files;
  DestroyAndReopen(options);
  listener->SetDB(db_.get());

  for (int i = 0; i < kNumL0Files; ++i) {
    // Make sure they overlap in keyspace to prevent trivial move
    ASSERT_OK(Put("key1", "val"));
    ASSERT_OK(Put("key2", "val"));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(listener->Validated());
}

TEST_F(DBPropertiesTest, BlobCacheProperties) {
  Options options;
  uint64_t value;

  options.env = CurrentOptions().env;

  // Test with empty blob cache.
  constexpr size_t kCapacity = 100;
  LRUCacheOptions co;
  co.capacity = kCapacity;
  co.num_shard_bits = 0;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto blob_cache = NewLRUCache(co);
  options.blob_cache = blob_cache;

  Reopen(options);

  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheUsage, &value));
  ASSERT_EQ(0, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlobCachePinnedUsage, &value));
  ASSERT_EQ(0, value);

  // Insert unpinned blob to the cache and check size.
  constexpr size_t kSize1 = 70;
  ASSERT_OK(blob_cache->Insert("blob1", nullptr /*value*/,
                               &kNoopCacheItemHelper, kSize1));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheUsage, &value));
  ASSERT_EQ(kSize1, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlobCachePinnedUsage, &value));
  ASSERT_EQ(0, value);

  // Insert pinned blob to the cache and check size.
  constexpr size_t kSize2 = 60;
  Cache::Handle* blob2 = nullptr;
  ASSERT_OK(blob_cache->Insert("blob2", nullptr /*value*/,
                               &kNoopCacheItemHelper, kSize2, &blob2));
  ASSERT_NE(nullptr, blob2);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheUsage, &value));
  // blob1 is evicted.
  ASSERT_EQ(kSize2, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlobCachePinnedUsage, &value));
  ASSERT_EQ(kSize2, value);

  // Insert another pinned blob to make the cache over-sized.
  constexpr size_t kSize3 = 80;
  Cache::Handle* blob3 = nullptr;
  ASSERT_OK(blob_cache->Insert("blob3", nullptr /*value*/,
                               &kNoopCacheItemHelper, kSize3, &blob3));
  ASSERT_NE(nullptr, blob3);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheUsage, &value));
  ASSERT_EQ(kSize2 + kSize3, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlobCachePinnedUsage, &value));
  ASSERT_EQ(kSize2 + kSize3, value);

  // Check size after release.
  blob_cache->Release(blob2);
  blob_cache->Release(blob3);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlobCacheUsage, &value));
  // blob2 will be evicted, while blob3 remain in cache after release.
  ASSERT_EQ(kSize3, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlobCachePinnedUsage, &value));
  ASSERT_EQ(0, value);
}

TEST_F(DBPropertiesTest, BlockCacheProperties) {
  Options options;
  uint64_t value;

  options.env = CurrentOptions().env;

  // Block cache properties are not available for tables other than
  // block-based table.
  options.table_factory.reset(NewPlainTableFactory());
  Reopen(options);
  ASSERT_FALSE(
      db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_FALSE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  ASSERT_FALSE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));

  options.table_factory.reset(NewCuckooTableFactory());
  Reopen(options);
  ASSERT_FALSE(
      db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_FALSE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  ASSERT_FALSE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));

  // Block cache properties are not available if block cache is not used.
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_FALSE(
      db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_FALSE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  ASSERT_FALSE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));

  // Test with empty block cache.
  constexpr size_t kCapacity = 100;
  LRUCacheOptions co;
  co.capacity = kCapacity;
  co.num_shard_bits = 0;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto block_cache = NewLRUCache(co);
  table_options.block_cache = block_cache;
  table_options.no_block_cache = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  ASSERT_EQ(0, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));
  ASSERT_EQ(0, value);

  // Insert unpinned item to the cache and check size.
  constexpr size_t kSize1 = 50;
  ASSERT_OK(block_cache->Insert("item1", nullptr /*value*/,
                                &kNoopCacheItemHelper, kSize1));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  ASSERT_EQ(kSize1, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));
  ASSERT_EQ(0, value);

  // Insert pinned item to the cache and check size.
  constexpr size_t kSize2 = 30;
  Cache::Handle* item2 = nullptr;
  ASSERT_OK(block_cache->Insert("item2", nullptr /*value*/,
                                &kNoopCacheItemHelper, kSize2, &item2));
  ASSERT_NE(nullptr, item2);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  ASSERT_EQ(kSize1 + kSize2, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));
  ASSERT_EQ(kSize2, value);

  // Insert another pinned item to make the cache over-sized.
  constexpr size_t kSize3 = 80;
  Cache::Handle* item3 = nullptr;
  ASSERT_OK(block_cache->Insert("item3", nullptr /*value*/,
                                &kNoopCacheItemHelper, kSize3, &item3));
  ASSERT_NE(nullptr, item2);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  // Item 1 is evicted.
  ASSERT_EQ(kSize2 + kSize3, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));
  ASSERT_EQ(kSize2 + kSize3, value);

  // Check size after release.
  block_cache->Release(item2);
  block_cache->Release(item3);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheCapacity, &value));
  ASSERT_EQ(kCapacity, value);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBlockCacheUsage, &value));
  // item2 will be evicted, while item3 remain in cache after release.
  ASSERT_EQ(kSize3, value);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBlockCachePinnedUsage, &value));
  ASSERT_EQ(0, value);
}

TEST_F(DBPropertiesTest, GetMapPropertyDbStats) {
  auto mock_clock = std::make_shared<MockSystemClock>(env_->GetSystemClock());
  CompositeEnvWrapper env(env_, mock_clock);

  Options opts = CurrentOptions();
  opts.env = &env;
  Reopen(opts);

  {
    std::map<std::string, std::string> db_stats;
    ASSERT_TRUE(db_->GetMapProperty(DB::Properties::kDBStats, &db_stats));
    AssertDbStats(db_stats, 0.0 /* expected_uptime */,
                  0 /* expected_user_bytes_written */,
                  0 /* expected_wal_bytes_written */,
                  0 /* expected_user_writes_by_self */,
                  0 /* expected_user_writes_with_wal */);
  }

  {
    mock_clock->SleepForMicroseconds(1500000);

    std::map<std::string, std::string> db_stats;
    ASSERT_TRUE(db_->GetMapProperty(DB::Properties::kDBStats, &db_stats));
    AssertDbStats(db_stats, 1.5 /* expected_uptime */,
                  0 /* expected_user_bytes_written */,
                  0 /* expected_wal_bytes_written */,
                  0 /* expected_user_writes_by_self */,
                  0 /* expected_user_writes_with_wal */);
  }

  int expected_user_bytes_written = 0;
  {
    // Write with WAL disabled.
    WriteOptions write_opts;
    write_opts.disableWAL = true;

    WriteBatch batch;
    ASSERT_OK(batch.Put("key", "val"));
    expected_user_bytes_written += static_cast<int>(batch.GetDataSize());

    ASSERT_OK(db_->Write(write_opts, &batch));

    std::map<std::string, std::string> db_stats;
    ASSERT_TRUE(db_->GetMapProperty(DB::Properties::kDBStats, &db_stats));
    AssertDbStats(db_stats, 1.5 /* expected_uptime */,
                  expected_user_bytes_written,
                  0 /* expected_wal_bytes_written */,
                  1 /* expected_user_writes_by_self */,
                  0 /* expected_user_writes_with_wal */);
  }

  int expected_wal_bytes_written = 0;
  {
    // Write with WAL enabled.
    WriteBatch batch;
    ASSERT_OK(batch.Delete("key"));
    expected_user_bytes_written += static_cast<int>(batch.GetDataSize());
    expected_wal_bytes_written += static_cast<int>(batch.GetDataSize());

    ASSERT_OK(db_->Write(WriteOptions(), &batch));

    std::map<std::string, std::string> db_stats;
    ASSERT_TRUE(db_->GetMapProperty(DB::Properties::kDBStats, &db_stats));
    AssertDbStats(db_stats, 1.5 /* expected_uptime */,
                  expected_user_bytes_written, expected_wal_bytes_written,
                  2 /* expected_user_writes_by_self */,
                  1 /* expected_user_writes_with_wal */);
  }

  Close();
}

TEST_F(DBPropertiesTest, GetMapPropertyBlockCacheEntryStats) {
  // Currently only verifies the expected properties are present
  std::map<std::string, std::string> values;
  ASSERT_TRUE(
      db_->GetMapProperty(DB::Properties::kBlockCacheEntryStats, &values));

  ASSERT_TRUE(values.find(BlockCacheEntryStatsMapKeys::CacheId()) !=
              values.end());
  ASSERT_TRUE(values.find(BlockCacheEntryStatsMapKeys::CacheCapacityBytes()) !=
              values.end());
  ASSERT_TRUE(
      values.find(
          BlockCacheEntryStatsMapKeys::LastCollectionDurationSeconds()) !=
      values.end());
  ASSERT_TRUE(
      values.find(BlockCacheEntryStatsMapKeys::LastCollectionAgeSeconds()) !=
      values.end());
  for (size_t i = 0; i < kNumCacheEntryRoles; ++i) {
    CacheEntryRole role = static_cast<CacheEntryRole>(i);
    ASSERT_TRUE(values.find(BlockCacheEntryStatsMapKeys::EntryCount(role)) !=
                values.end());
    ASSERT_TRUE(values.find(BlockCacheEntryStatsMapKeys::UsedBytes(role)) !=
                values.end());
    ASSERT_TRUE(values.find(BlockCacheEntryStatsMapKeys::UsedPercent(role)) !=
                values.end());
  }

  // There should be no extra values in the map.
  ASSERT_EQ(3 * kNumCacheEntryRoles + 4, values.size());
}

TEST_F(DBPropertiesTest, WriteStallStatsSanityCheck) {
  for (uint32_t i = 0; i < static_cast<uint32_t>(WriteStallCause::kNone); ++i) {
    WriteStallCause cause = static_cast<WriteStallCause>(i);
    const std::string& str = WriteStallCauseToHyphenString(cause);
    ASSERT_TRUE(!str.empty())
        << "Please ensure mapping from `WriteStallCause` to "
           "`WriteStallCauseToHyphenString` is complete";
    if (cause == WriteStallCause::kCFScopeWriteStallCauseEnumMax ||
        cause == WriteStallCause::kDBScopeWriteStallCauseEnumMax) {
      ASSERT_EQ(str, InvalidWriteStallHyphenString())
          << "Please ensure order in `WriteStallCauseToHyphenString` is "
             "consistent with `WriteStallCause`";
    }
  }

  for (uint32_t i = 0; i < static_cast<uint32_t>(WriteStallCondition::kNormal);
       ++i) {
    WriteStallCondition condition = static_cast<WriteStallCondition>(i);
    const std::string& str = WriteStallConditionToHyphenString(condition);
    ASSERT_TRUE(!str.empty())
        << "Please ensure mapping from `WriteStallCondition` to "
           "`WriteStallConditionToHyphenString` is complete";
  }

  for (uint32_t i = 0; i < static_cast<uint32_t>(WriteStallCause::kNone); ++i) {
    for (uint32_t j = 0;
         j < static_cast<uint32_t>(WriteStallCondition::kNormal); ++j) {
      WriteStallCause cause = static_cast<WriteStallCause>(i);
      WriteStallCondition condition = static_cast<WriteStallCondition>(j);

      if (isCFScopeWriteStallCause(cause)) {
        ASSERT_TRUE(InternalCFStat(cause, condition) !=
                    InternalStats::INTERNAL_CF_STATS_ENUM_MAX)
            << "Please ensure the combination of WriteStallCause(" +
                   std::to_string(static_cast<uint32_t>(cause)) +
                   ") + WriteStallCondition(" +
                   std::to_string(static_cast<uint32_t>(condition)) +
                   ") is correctly mapped to a valid `InternalStats` or bypass "
                   "its check in this test";
      } else if (isDBScopeWriteStallCause(cause)) {
        InternalStats::InternalDBStatsType internal_db_stat =
            InternalDBStat(cause, condition);
        if (internal_db_stat == InternalStats::kIntStatsNumMax) {
          ASSERT_TRUE(cause == WriteStallCause::kWriteBufferManagerLimit &&
                      condition == WriteStallCondition::kDelayed)
              << "Please ensure the combination of WriteStallCause(" +
                     std::to_string(static_cast<uint32_t>(cause)) +
                     ") + WriteStallCondition(" +
                     std::to_string(static_cast<uint32_t>(condition)) +
                     ") is correctly mapped to a valid `InternalStats` or "
                     "bypass its check in this test";
        }
      } else if (cause != WriteStallCause::kCFScopeWriteStallCauseEnumMax &&
                 cause != WriteStallCause::kDBScopeWriteStallCauseEnumMax) {
        ASSERT_TRUE(false) << "Please ensure the WriteStallCause(" +
                                  std::to_string(static_cast<uint32_t>(cause)) +
                                  ") is either CF-scope or DB-scope write "
                                  "stall cause in enum `WriteStallCause`";
      }
    }
  }
}
TEST_F(DBPropertiesTest, GetMapPropertyWriteStallStats) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"heavy_write_cf"}, options);

  for (auto test_cause : {WriteStallCause::kWriteBufferManagerLimit,
                          WriteStallCause::kMemtableLimit}) {
    if (test_cause == WriteStallCause::kWriteBufferManagerLimit) {
      options.write_buffer_manager.reset(
          new WriteBufferManager(100000, nullptr, true));
    } else if (test_cause == WriteStallCause::kMemtableLimit) {
      options.max_write_buffer_number = 2;
      options.disable_auto_compactions = true;
    }
    ReopenWithColumnFamilies({"default", "heavy_write_cf"}, options);

    // Assert initial write stall stats are all 0
    std::map<std::string, std::string> db_values;
    ASSERT_TRUE(dbfull()->GetMapProperty(DB::Properties::kDBWriteStallStats,
                                         &db_values));
    ASSERT_EQ(std::stoi(db_values[WriteStallStatsMapKeys::CauseConditionCount(
                  WriteStallCause::kWriteBufferManagerLimit,
                  WriteStallCondition::kStopped)]),
              0);

    for (int cf = 0; cf <= 1; ++cf) {
      std::map<std::string, std::string> cf_values;
      ASSERT_TRUE(dbfull()->GetMapProperty(
          handles_[cf], DB::Properties::kCFWriteStallStats, &cf_values));
      ASSERT_EQ(std::stoi(cf_values[WriteStallStatsMapKeys::TotalStops()]), 0);
      ASSERT_EQ(std::stoi(cf_values[WriteStallStatsMapKeys::TotalDelays()]), 0);
    }

    // Pause flush thread to help coerce write stall
    std::unique_ptr<test::SleepingBackgroundTask> sleeping_task(
        new test::SleepingBackgroundTask());
    env_->SetBackgroundThreads(1, Env::HIGH);
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   sleeping_task.get(), Env::Priority::HIGH);
    sleeping_task->WaitUntilSleeping();

    // Coerce write stall
    if (test_cause == WriteStallCause::kWriteBufferManagerLimit) {
      ASSERT_OK(dbfull()->Put(
          WriteOptions(), handles_[1], Key(1),
          DummyString(options.write_buffer_manager->buffer_size())));

      WriteOptions wo;
      wo.no_slowdown = true;
      Status s = dbfull()->Put(
          wo, handles_[1], Key(2),
          DummyString(options.write_buffer_manager->buffer_size()));
      ASSERT_TRUE(s.IsIncomplete());
      ASSERT_TRUE(s.ToString().find("Write stall") != std::string::npos);
    } else if (test_cause == WriteStallCause::kMemtableLimit) {
      FlushOptions fo;
      fo.allow_write_stall = true;
      fo.wait = false;

      ASSERT_OK(
          dbfull()->Put(WriteOptions(), handles_[1], Key(1), DummyString(1)));
      ASSERT_OK(dbfull()->Flush(fo, handles_[1]));

      ASSERT_OK(
          dbfull()->Put(WriteOptions(), handles_[1], Key(2), DummyString(1)));
      ASSERT_OK(dbfull()->Flush(fo, handles_[1]));
    }

    if (test_cause == WriteStallCause::kWriteBufferManagerLimit) {
      db_values.clear();
      EXPECT_TRUE(dbfull()->GetMapProperty(DB::Properties::kDBWriteStallStats,
                                           &db_values));
      EXPECT_EQ(std::stoi(db_values[WriteStallStatsMapKeys::CauseConditionCount(
                    WriteStallCause::kWriteBufferManagerLimit,
                    WriteStallCondition::kStopped)]),
                1);
      // `WriteStallCause::kWriteBufferManagerLimit` should not result in any
      // CF-scope write stall stats changes
      for (int cf = 0; cf <= 1; ++cf) {
        std::map<std::string, std::string> cf_values;
        EXPECT_TRUE(dbfull()->GetMapProperty(
            handles_[cf], DB::Properties::kCFWriteStallStats, &cf_values));
        EXPECT_EQ(std::stoi(cf_values[WriteStallStatsMapKeys::TotalStops()]),
                  0);
        EXPECT_EQ(std::stoi(cf_values[WriteStallStatsMapKeys::TotalDelays()]),
                  0);
      }
    } else if (test_cause == WriteStallCause::kMemtableLimit) {
      for (int cf = 0; cf <= 1; ++cf) {
        std::map<std::string, std::string> cf_values;
        EXPECT_TRUE(dbfull()->GetMapProperty(
            handles_[cf], DB::Properties::kCFWriteStallStats, &cf_values));
        EXPECT_EQ(std::stoi(cf_values[WriteStallStatsMapKeys::TotalStops()]),
                  cf == 1 ? 1 : 0);
        EXPECT_EQ(
            std::stoi(cf_values[WriteStallStatsMapKeys::CauseConditionCount(
                WriteStallCause::kMemtableLimit,
                WriteStallCondition::kStopped)]),
            cf == 1 ? 1 : 0);
        EXPECT_EQ(std::stoi(cf_values[WriteStallStatsMapKeys::TotalDelays()]),
                  0);
        EXPECT_EQ(
            std::stoi(cf_values[WriteStallStatsMapKeys::CauseConditionCount(
                WriteStallCause::kMemtableLimit,
                WriteStallCondition::kDelayed)]),
            0);
      }
    }

    sleeping_task->WakeUp();
    sleeping_task->WaitUntilDone();
  }
}

namespace {
std::string PopMetaIndexKey(InternalIterator* meta_iter) {
  Status s = meta_iter->status();
  if (!s.ok()) {
    return s.ToString();
  } else if (meta_iter->Valid()) {
    std::string rv = meta_iter->key().ToString();
    meta_iter->Next();
    return rv;
  } else {
    return "NOT_FOUND";
  }
}

}  // anonymous namespace

TEST_F(DBPropertiesTest, TableMetaIndexKeys) {
  // This is to detect unexpected churn in metaindex block keys. This is more
  // of a "table test" but table_test.cc doesn't depend on db_test_util.h and
  // we need ChangeOptions() for broad coverage.
  constexpr int kKeyCount = 100;
  do {
    Options options;
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    // Create an SST file
    for (int key = 0; key < kKeyCount; key++) {
      ASSERT_OK(Put(Key(key), "val"));
    }
    ASSERT_OK(Flush());

    // Find its file number
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    // 1 SST file
    ASSERT_EQ(1, files.size());

    // Open it for inspection
    std::string sst_file =
        files[0].directory + "/" + files[0].relative_filename;
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
        sst_file, FileOptions(), &f, nullptr));
    std::unique_ptr<RandomAccessFileReader> r;
    r.reset(new RandomAccessFileReader(std::move(f), sst_file));
    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(sst_file, &file_size));

    // Read metaindex
    BlockContents bc;
    const ReadOptions read_options;
    ASSERT_OK(ReadMetaIndexBlockInFile(
        r.get(), file_size, 0U, ImmutableOptions(options), read_options, &bc));
    Block metaindex_block(std::move(bc));
    std::unique_ptr<InternalIterator> meta_iter;
    meta_iter.reset(metaindex_block.NewMetaIterator());
    meta_iter->SeekToFirst();

    if (strcmp(options.table_factory->Name(),
               TableFactory::kBlockBasedTableName()) == 0) {
      auto bbto = options.table_factory->GetOptions<BlockBasedTableOptions>();
      if (bbto->filter_policy) {
        if (bbto->partition_filters) {
          // The key names are intentionally hard-coded here to detect
          // accidental regression on compatibility.
          EXPECT_EQ("partitionedfilter.rocksdb.BuiltinBloomFilter",
                    PopMetaIndexKey(meta_iter.get()));
        } else {
          EXPECT_EQ("fullfilter.rocksdb.BuiltinBloomFilter",
                    PopMetaIndexKey(meta_iter.get()));
        }
      }
      if (bbto->index_type == BlockBasedTableOptions::kHashSearch) {
        EXPECT_EQ("rocksdb.hashindex.metadata",
                  PopMetaIndexKey(meta_iter.get()));
        EXPECT_EQ("rocksdb.hashindex.prefixes",
                  PopMetaIndexKey(meta_iter.get()));
      }
      if (bbto->format_version >= 6) {
        EXPECT_EQ("rocksdb.index", PopMetaIndexKey(meta_iter.get()));
      }
    }
    EXPECT_EQ("rocksdb.properties", PopMetaIndexKey(meta_iter.get()));
    EXPECT_EQ("NOT_FOUND", PopMetaIndexKey(meta_iter.get()));
  } while (ChangeOptions());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
