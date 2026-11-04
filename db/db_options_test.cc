//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <algorithm>
#include <atomic>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/stats_history.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/defer.h"
#include "util/random.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class SlowRecordCallbackLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list /*ap*/) override {
    if (std::strstr(format, "SLOW_GLOBAL_MUTEX") != nullptr &&
        !seen.exchange(true)) {
      on_slow_record();
    }
  }
  std::atomic<bool> seen{false};
  std::function<void()> on_slow_record;
};

class DBOptionsTest : public DBTestBase {
 public:
  DBOptionsTest() : DBTestBase("db_options_test", /*env_do_fsync=*/true) {}

  std::unordered_map<std::string, std::string> GetMutableDBOptionsMap(
      const DBOptions& options) {
    std::string options_str;
    std::unordered_map<std::string, std::string> mutable_map;
    ConfigOptions config_options(options);
    config_options.delimiter = "; ";

    EXPECT_OK(GetStringFromMutableDBOptions(
        config_options, MutableDBOptions(options), &options_str));
    EXPECT_OK(StringToMap(options_str, &mutable_map));

    return mutable_map;
  }

  std::unordered_map<std::string, std::string> GetMutableCFOptionsMap(
      const ColumnFamilyOptions& options) {
    std::string options_str;
    ConfigOptions config_options;
    config_options.delimiter = "; ";

    std::unordered_map<std::string, std::string> mutable_map;
    EXPECT_OK(GetStringFromMutableCFOptions(
        config_options, MutableCFOptions(options), &options_str));
    EXPECT_OK(StringToMap(options_str, &mutable_map));
    for (auto& opt : TEST_GetImmutableInMutableCFOptions()) {
      // Not yet mutable but migrated to MutableCFOptions in preparation for
      // being mutable
      mutable_map.erase(opt);
    }
    return mutable_map;
  }

  std::unordered_map<std::string, std::string> GetRandomizedMutableCFOptionsMap(
      Random* rnd) {
    Options options = CurrentOptions();
    options.env = env_;
    ImmutableDBOptions db_options(options);
    test::RandomInitCFOptions(&options, options, rnd);
    auto sanitized_options =
        SanitizeCfOptions(db_options, /*read_only*/ false, options);
    auto opt_map = GetMutableCFOptionsMap(sanitized_options);
    delete options.compaction_filter;
    return opt_map;
  }

  std::unordered_map<std::string, std::string> GetRandomizedMutableDBOptionsMap(
      Random* rnd) {
    DBOptions db_options;
    test::RandomInitDBOptions(&db_options, rnd);
    auto sanitized_options = SanitizeOptions(dbname_, db_options);
    return GetMutableDBOptionsMap(sanitized_options);
  }

  std::string LatestOptionsFileName() {
    std::string file_name;
    EXPECT_OK(GetLatestOptionsFileName(dbname_, env_, &file_name));
    return file_name;
  }

  // Returns the options of column family cf_name in the latest OPTIONS file.
  ColumnFamilyOptions PersistedCFOptions(const std::string& cf_name) {
    DBOptions db_options;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    ConfigOptions config_options;
    config_options.env = env_;
    EXPECT_OK(
        LoadLatestOptions(config_options, dbname_, &db_options, &cf_descs));
    for (const auto& cf_desc : cf_descs) {
      if (cf_desc.name == cf_name) {
        return cf_desc.options;
      }
    }
    ADD_FAILURE() << "Column family " << cf_name
                  << " is not in the OPTIONS file";
    return ColumnFamilyOptions();
  }

  // Returns the sorted column family names in the latest OPTIONS file. Unlike
  // LoadLatestOptions(), this keeps the names that MANIFEST does not list.
  std::vector<std::string> PersistedCFNames() {
    DBOptions db_options;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    ConfigOptions config_options;
    config_options.env = env_;
    EXPECT_OK(LoadOptionsFromFile(config_options,
                                  dbname_ + "/" + LatestOptionsFileName(),
                                  &db_options, &cf_descs));
    std::vector<std::string> names;
    names.reserve(cf_descs.size());
    for (const auto& cf_desc : cf_descs) {
      names.push_back(cf_desc.name);
    }
    std::sort(names.begin(), names.end());
    return names;
  }

  // Returns true if fn() succeeds and writes a new OPTIONS file.
  bool WritesOptionsFile(const std::function<Status()>& fn) {
    const std::string file_before = LatestOptionsFileName();
    const Status s = fn();
    EXPECT_OK(s);
    return s.ok() && LatestOptionsFileName() != file_before;
  }

  // SetOptions() writes a new OPTIONS file exactly when it applies the request
  // instead of skipping it.
  bool SetOptionsWritesOptionsFile(
      ColumnFamilyHandle* cfh,
      const std::unordered_map<std::string, std::string>& opts) {
    return WritesOptionsFile([&]() { return db_->SetOptions(cfh, opts); });
  }

  // Runs fn() while OPTIONS file writes on fs fail, and returns its status.
  static Status FailOptionsFileWrites(FaultInjectionTestFS* fs,
                                      const std::function<Status()>& fn) {
    SyncPoint::GetInstance()->SetCallBack(
        "PersistRocksDBOptions:create",
        [fs](void* /*arg*/) { fs->SetFilesystemActive(false); });
    SyncPoint::GetInstance()->SetCallBack(
        "PersistRocksDBOptions:written",
        [fs](void* /*arg*/) { fs->SetFilesystemActive(true); });
    SyncPoint::GetInstance()->EnableProcessing();
    const Status s = fn();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    return s;
  }

  // Runs fn() while the second and later MANIFEST writes on this thread fail,
  // and returns its status.
  static Status FailSecondManifestWrite(FaultInjectionTestFS* fs,
                                        const std::function<Status()>& fn) {
    std::atomic<int> manifest_writes{0};
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifest", [&](void* /*arg*/) {
          if (manifest_writes.fetch_add(1, std::memory_order_relaxed) == 1) {
            fs->SetThreadLocalErrorContext(
                FaultInjectionIOType::kWrite, /*seed=*/0, /*one_in=*/1,
                /*retryable=*/false, /*has_data_loss=*/false);
            fs->EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    const Status s = fn();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
    return s;
  }
};

TEST_F(DBOptionsTest, MutableMutexHoldLogThreshold) {
  Options options = CurrentOptions();
  ASSERT_EQ(options.mutex_hold_log_threshold_us, 0);
  options.mutex_hold_log_threshold_us = 25000;
  Reopen(options);
  ASSERT_EQ(dbfull()->GetDBOptions().mutex_hold_log_threshold_us, 25000);
  ASSERT_EQ(dbfull()->GetMutexHoldLogThreshold()->load(), 25000);
  for (uint64_t threshold : {uint64_t{500000}, uint64_t{0}, uint64_t{100000}}) {
    ASSERT_OK(dbfull()->SetDBOptions(
        {{"mutex_hold_log_threshold_us", std::to_string(threshold)}}));
    ASSERT_EQ(dbfull()->GetDBOptions().mutex_hold_log_threshold_us, threshold);
    ASSERT_EQ(dbfull()->GetMutexHoldLogThreshold()->load(), threshold);
  }
  ASSERT_FALSE(
      dbfull()
          ->SetDBOptions({{"mutex_hold_log_threshold_us", "not-an-integer"}})
          .ok());
  ASSERT_EQ(dbfull()->GetMutexHoldLogThreshold()->load(), 100000);
  DBOptions parsed;
  ASSERT_OK(GetDBOptionsFromString(ConfigOptions(), DBOptions(),
                                   "mutex_hold_log_threshold_us=50000",
                                   &parsed));
  ASSERT_EQ(parsed.mutex_hold_log_threshold_us, 50000);
}

TEST_F(DBOptionsTest, SlowSetOptionsRecordCanReenterDBOptions) {
  Options options = CurrentOptions();
  options.mutex_hold_log_threshold_us = 100000;
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
  std::shared_ptr<SlowRecordCallbackLogger> logger =
      std::make_shared<SlowRecordCallbackLogger>();
  options.info_log = logger;
  Reopen(options);
  env_->SetMockSleep(true);
  options.statistics->set_stats_level(StatsLevel::kAll);
  std::shared_ptr<Status> nested_status = std::make_shared<Status>();
  logger->on_slow_record = [&, nested_status] {
    *nested_status =
        db_->SetDBOptions({{"mutex_hold_log_threshold_us", "500000"}});
  };
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SetOptions:dummy_edit",
      [&](void*) { env_->MockSleepForMicroseconds(200000); });
  SyncPoint::GetInstance()->EnableProcessing();
  Status status = db_->SetOptions({{"write_buffer_size", "1048576"}});
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(status);
  ASSERT_TRUE(logger->seen.load());
  ASSERT_OK(*nested_status);
  ASSERT_EQ(db_->GetDBOptions().mutex_hold_log_threshold_us, 500000);
  options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
  logger->on_slow_record = [] {};
}

TEST_F(DBOptionsTest, CloseDropsDeferredOwnedLoggerRecord) {
  Options options = CurrentOptions();
  options.mutex_hold_log_threshold_us = 100000;
  options.info_log.reset();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
  Reopen(options);
  env_->SetMockSleep(true);
  options.statistics->set_stats_level(StatsLevel::kAll);
  std::shared_ptr<SlowMutexLoggingContext> context =
      dbfull()->GetSlowMutexLogging();
  ASSERT_NE(context, nullptr);
  {
    SlowMutexLogScope defer_logs(true);
    dbfull()->mutex()->Lock();
    env_->MockSleepForMicroseconds(200000);
    dbfull()->mutex()->Unlock();
    ASSERT_OK(db_->Close());
    ASSERT_TRUE(context->stopped.load());
  }
  ASSERT_EQ(options.statistics->get_stats_level(), StatsLevel::kAll);
  ASSERT_EQ(context->threshold_us->load(), 100000);
}

TEST_F(DBOptionsTest, ImmutableTrackAndVerifyWalsInManifest) {
  Options options;
  options.env = env_;
  options.track_and_verify_wals_in_manifest = true;

  ImmutableDBOptions db_options(options);
  ASSERT_TRUE(db_options.track_and_verify_wals_in_manifest);

  Reopen(options);
  ASSERT_TRUE(dbfull()->GetDBOptions().track_and_verify_wals_in_manifest);

  Status s =
      dbfull()->SetDBOptions({{"track_and_verify_wals_in_manifest", "false"}});
  ASSERT_FALSE(s.ok());
}

TEST_F(DBOptionsTest, ImmutableAsyncWalPrecreate) {
  Options options;
  options.env = env_;
  options.async_wal_precreate = true;

  ImmutableDBOptions db_options(options);
  ASSERT_TRUE(db_options.async_wal_precreate);

  Reopen(options);
  ASSERT_TRUE(dbfull()->GetDBOptions().async_wal_precreate);

  Status s = dbfull()->SetDBOptions({{"async_wal_precreate", "false"}});
  ASSERT_FALSE(s.ok());
}

TEST_F(DBOptionsTest, SanitizeAsyncWalPrecreateWithWalRecycle) {
  // This checks the option-level contract for the incompatible WAL paths:
  // SanitizeOptions() preserves WAL recycling and disables only the
  // opportunistic async precreation path, so the immutable options reflect
  // effective behavior.
  Options options;
  options.env = env_;
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 1;

  Options sanitized_options = SanitizeOptions(dbname_, options);
  ASSERT_FALSE(sanitized_options.async_wal_precreate);
  ASSERT_EQ(1U, sanitized_options.recycle_log_file_num);

  options.recycle_log_file_num = 0;
  sanitized_options = SanitizeOptions(dbname_, options);
  ASSERT_TRUE(sanitized_options.async_wal_precreate);
}

TEST_F(DBOptionsTest, ImmutableVerifySstUniqueIdInManifest) {
  Options options;
  options.env = env_;
  options.verify_sst_unique_id_in_manifest = true;

  ImmutableDBOptions db_options(options);
  ASSERT_TRUE(db_options.verify_sst_unique_id_in_manifest);

  Reopen(options);
  ASSERT_TRUE(dbfull()->GetDBOptions().verify_sst_unique_id_in_manifest);

  Status s =
      dbfull()->SetDBOptions({{"verify_sst_unique_id_in_manifest", "false"}});
  ASSERT_FALSE(s.ok());
}

// RocksDB lite don't support dynamic options.

TEST_F(DBOptionsTest, AvoidUpdatingOptions) {
  Options options;
  options.env = env_;
  options.max_background_jobs = 4;
  options.delayed_write_rate = 1024;

  Reopen(options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  bool is_changed_stats = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteOptionsFile:PersistOptions", [&](void* /*arg*/) {
        ASSERT_FALSE(is_changed_stats);  // should only save options file once
        is_changed_stats = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // helper function to check the status and reset after each check
  auto is_changed = [&] {
    bool ret = is_changed_stats;
    is_changed_stats = false;
    return ret;
  };

  // without changing the value, but it's sanitized to a different value
  ASSERT_OK(dbfull()->SetDBOptions({{"bytes_per_sync", "0"}}));
  ASSERT_TRUE(is_changed());

  // without changing the value
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_jobs", "4"}}));
  ASSERT_FALSE(is_changed());

  // changing the value
  ASSERT_OK(dbfull()->SetDBOptions({{"bytes_per_sync", "123"}}));
  ASSERT_TRUE(is_changed());

  // update again
  ASSERT_OK(dbfull()->SetDBOptions({{"bytes_per_sync", "123"}}));
  ASSERT_FALSE(is_changed());

  // without changing a default value
  ASSERT_OK(dbfull()->SetDBOptions({{"strict_bytes_per_sync", "false"}}));
  ASSERT_FALSE(is_changed());

  // now change
  ASSERT_OK(dbfull()->SetDBOptions({{"strict_bytes_per_sync", "true"}}));
  ASSERT_TRUE(is_changed());

  // multiple values without change
  ASSERT_OK(dbfull()->SetDBOptions(
      {{"max_total_wal_size", "0"}, {"stats_dump_period_sec", "600"}}));
  ASSERT_FALSE(is_changed());

  // multiple values with change
  ASSERT_OK(dbfull()->SetDBOptions(
      {{"max_open_files", "100"}, {"stats_dump_period_sec", "600"}}));
  ASSERT_TRUE(is_changed());
}

TEST_F(DBOptionsTest, GetLatestDBOptions) {
  // GetOptions should be able to get latest option changed by SetOptions.
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  Random rnd(228);
  Reopen(options);
  auto new_options = GetRandomizedMutableDBOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetDBOptions(new_options));
  ASSERT_EQ(new_options, GetMutableDBOptionsMap(dbfull()->GetDBOptions()));
}

TEST_F(DBOptionsTest, GetLatestCFOptions) {
  // GetOptions should be able to get latest option changed by SetOptions.
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  Random rnd(228);
  Reopen(options);
  CreateColumnFamilies({"foo"}, options);
  ReopenWithColumnFamilies({"default", "foo"}, options);
  auto options_default = GetRandomizedMutableCFOptionsMap(&rnd);
  auto options_foo = GetRandomizedMutableCFOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetOptions(handles_[0], options_default));
  ASSERT_OK(dbfull()->SetOptions(handles_[1], options_foo));
  ASSERT_EQ(options_default,
            GetMutableCFOptionsMap(dbfull()->GetOptions(handles_[0])));
  ASSERT_EQ(options_foo,
            GetMutableCFOptionsMap(dbfull()->GetOptions(handles_[1])));
}

TEST_F(DBOptionsTest, SetMutableTableOptions) {
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.blob_file_size = 16384;
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;
  bbto.block_size = 8192;
  bbto.block_restart_interval = 7;

  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  Options c_opts = dbfull()->GetOptions(cfh);

  const auto* c_bbto =
      c_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(c_bbto, nullptr);
  ASSERT_EQ(c_opts.blob_file_size, 16384);
  ASSERT_EQ(c_bbto->no_block_cache, true);
  ASSERT_EQ(c_bbto->block_size, 8192);
  ASSERT_EQ(c_bbto->block_restart_interval, 7);
  ASSERT_OK(dbfull()->SetOptions(
      cfh, {{"table_factory.block_size", "16384"},
            {"table_factory.block_restart_interval", "11"}}));
  // Old c_bbto
  ASSERT_EQ(c_bbto->block_size, 8192);
  ASSERT_EQ(c_bbto->block_restart_interval, 7);
  // New c_bbto
  c_opts = dbfull()->GetOptions(cfh);
  c_bbto = c_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(c_bbto->block_size, 16384);
  ASSERT_EQ(c_bbto->block_restart_interval, 11);

  // Now set an option that is not mutable - options should not change
  // FIXME: find a way to make this fail again
  // ASSERT_NOK(
  //    dbfull()->SetOptions(cfh, {{"table_factory.no_block_cache", "false"}}));
  c_opts = dbfull()->GetOptions(cfh);
  ASSERT_EQ(c_bbto, c_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(c_bbto->no_block_cache, true);
  ASSERT_EQ(c_bbto->block_size, 16384);
  ASSERT_EQ(c_bbto->block_restart_interval, 11);

  // Set some that are mutable and some that are not - options should not change
  // FIXME: find a way to make this fail again
  // ASSERT_NOK(dbfull()->SetOptions(
  //     cfh, {{"table_factory.no_block_cache", "false"},
  //           {"table_factory.block_size", "8192"},
  //           {"table_factory.block_restart_interval", "7"}}));
  c_opts = dbfull()->GetOptions(cfh);
  ASSERT_EQ(c_bbto, c_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(c_bbto->no_block_cache, true);
  ASSERT_EQ(c_bbto->block_size, 16384);
  ASSERT_EQ(c_bbto->block_restart_interval, 11);

  // Set some that are mutable and some that do not exist - options should not
  // change
  ASSERT_NOK(dbfull()->SetOptions(
      cfh, {{"table_factory.block_size", "8192"},
            {"table_factory.does_not_exist", "true"},
            {"table_factory.block_restart_interval", "7"}}));
  c_opts = dbfull()->GetOptions(cfh);
  ASSERT_EQ(c_bbto, c_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(c_bbto->no_block_cache, true);
  ASSERT_EQ(c_bbto->block_size, 16384);
  ASSERT_EQ(c_bbto->block_restart_interval, 11);

  // Trying to change the table factory fails
  ASSERT_NOK(dbfull()->SetOptions(
      cfh, {{"table_factory", TableFactory::kPlainTableName()}}));

  // Set some on the table and some on the Column Family
  ASSERT_OK(dbfull()->SetOptions(
      cfh, {{"table_factory.block_size", "16384"},
            {"blob_file_size", "32768"},
            {"table_factory.block_restart_interval", "13"}}));
  c_opts = dbfull()->GetOptions(cfh);
  ASSERT_EQ(c_opts.blob_file_size, 32768);
  c_bbto = c_opts.table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_EQ(c_bbto->block_size, 16384);
  ASSERT_EQ(c_bbto->block_restart_interval, 13);
  // Set some on the table and a bad one on the ColumnFamily - options should
  // not change
  ASSERT_NOK(dbfull()->SetOptions(
      cfh, {{"table_factory.block_size", "1024"},
            {"no_such_option", "32768"},
            {"table_factory.block_restart_interval", "7"}}));
  ASSERT_EQ(c_bbto, c_opts.table_factory->GetOptions<BlockBasedTableOptions>());
  ASSERT_EQ(c_bbto->block_size, 16384);
  ASSERT_EQ(c_bbto->block_restart_interval, 13);
}

// SetOptions() used to skip the table factory checks of DB::Open, so it could
// install options that DB::Open rejects. Clearing the prefix extractor of a
// hash index made the next flush crash.
TEST_F(DBOptionsTest, SetOptionsRejectsHashIndexWithoutPrefixExtractor) {
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  BlockBasedTableOptions bbto;
  bbto.index_type = BlockBasedTableOptions::kHashSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  Reopen(options);
  ASSERT_OK(Put("a1", "v1"));

  ASSERT_EQ(dbfull()->SetOptions({{"prefix_extractor", "nullptr"}}).code(),
            Status::Code::kInvalidArgument);
  ASSERT_NE(dbfull()->GetOptions().prefix_extractor, nullptr);

  // Dropping both is valid, but then the hash index cannot come back alone.
  ASSERT_OK(dbfull()->SetOptions({{"table_factory.index_type", "kBinarySearch"},
                                  {"prefix_extractor", "nullptr"}}));
  ASSERT_EQ(dbfull()
                ->SetOptions({{"table_factory.index_type", "kHashSearch"}})
                .code(),
            Status::Code::kInvalidArgument);
  ASSERT_EQ(dbfull()
                ->GetOptions()
                .table_factory->GetOptions<BlockBasedTableOptions>()
                ->index_type,
            BlockBasedTableOptions::kBinarySearch);

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("a1"), "v1");
  // The options that SetOptions() kept are ones DB::Open accepts.
  bbto.index_type = BlockBasedTableOptions::kBinarySearch;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  options.prefix_extractor.reset();
  Reopen(options);
  ASSERT_EQ(Get("a1"), "v1");
}

// The table builder fails every compressed build when block_align is set, so a
// flush after such a SetOptions() used to stop writes with a background error.
TEST_F(DBOptionsTest, SetOptionsRejectsCompressionWithBlockAlign) {
  CompressionType compression = kNoCompression;
  for (CompressionType t : GetSupportedCompressions()) {
    if (t != kNoCompression) {
      compression = t;
      break;
    }
  }
  if (compression == kNoCompression) {
    ROCKSDB_GTEST_BYPASS("No compression supported");
    return;
  }
  std::string compression_name;
  ASSERT_OK(GetStringFromCompressionType(&compression_name, compression));

  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.compression = kNoCompression;
  BlockBasedTableOptions bbto;
  bbto.block_align = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  ASSERT_EQ(dbfull()->SetOptions({{"compression", compression_name}}).code(),
            Status::Code::kInvalidArgument);
  ASSERT_EQ(dbfull()->GetOptions().compression, kNoCompression);
  ASSERT_OK(Put("a1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a2", "v2"));
  ASSERT_EQ(Get("a1"), "v1");
}

TEST_F(DBOptionsTest, SetWithCustomMemTableFactory) {
  class DummySkipListFactory : public SkipListFactory {
   public:
    static const char* kClassName() { return "DummySkipListFactory"; }
    const char* Name() const override { return kClassName(); }
    explicit DummySkipListFactory() : SkipListFactory(2) {}
  };
  {
    // Verify the DummySkipList cannot be created
    ConfigOptions config_options;
    config_options.ignore_unsupported_options = false;
    std::unique_ptr<MemTableRepFactory> factory;
    ASSERT_NOK(MemTableRepFactory::CreateFromString(
        config_options, DummySkipListFactory::kClassName(), &factory));
  }
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.disable_auto_compactions = false;

  options.memtable_factory.reset(new DummySkipListFactory());
  Reopen(options);

  ColumnFamilyHandle* cfh = dbfull()->DefaultColumnFamily();
  ASSERT_OK(dbfull()->SetOptions(cfh, {{"disable_auto_compactions", "true"}}));
  ColumnFamilyDescriptor cfd;
  ASSERT_OK(cfh->GetDescriptor(&cfd));
  ASSERT_STREQ(cfd.options.memtable_factory->Name(),
               DummySkipListFactory::kClassName());
  ColumnFamilyHandle* test = nullptr;
  ASSERT_OK(dbfull()->CreateColumnFamily(options, "test", &test));
  ASSERT_OK(test->GetDescriptor(&cfd));
  ASSERT_STREQ(cfd.options.memtable_factory->Name(),
               DummySkipListFactory::kClassName());

  ASSERT_OK(dbfull()->DropColumnFamily(test));
  delete test;
}

TEST_F(DBOptionsTest, SetBytesPerSync) {
  const size_t kValueSize = 1024 * 1024;  // 1MB
  Options options;
  options.create_if_missing = true;
  options.bytes_per_sync = 1024 * 1024;
  options.use_direct_reads = false;
  options.write_buffer_size = 400 * kValueSize;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.env = env_;
  Reopen(options);
  int counter = 0;
  int low_bytes_per_sync = 0;
  int i = 0;
  const std::string kValue(kValueSize, 'v');
  ASSERT_EQ(options.bytes_per_sync, dbfull()->GetDBOptions().bytes_per_sync);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::RangeSync:0", [&](void* /*arg*/) { counter++; });

  WriteOptions write_opts;
  // should sync approximately 40MB/1MB ~= 40 times.
  for (i = 0; i < 40; i++) {
    ASSERT_OK(Put(Key(i), kValue, write_opts));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  low_bytes_per_sync = counter;
  ASSERT_GT(low_bytes_per_sync, 35);
  ASSERT_LT(low_bytes_per_sync, 45);

  counter = 0;
  // 8388608 = 8 * 1024 * 1024
  ASSERT_OK(dbfull()->SetDBOptions({{"bytes_per_sync", "8388608"}}));
  ASSERT_EQ(8388608, dbfull()->GetDBOptions().bytes_per_sync);
  // should sync approximately 40MB*2/8MB ~= 10 times.
  // data will be 40*2MB because of previous Puts too.
  for (i = 0; i < 40; i++) {
    ASSERT_OK(Put(Key(i), kValue, write_opts));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_GT(counter, 5);
  ASSERT_LT(counter, 15);

  // Redundant assert. But leaving it here just to get the point across that
  // low_bytes_per_sync > counter.
  ASSERT_GT(low_bytes_per_sync, counter);
}

TEST_F(DBOptionsTest, SetWalBytesPerSync) {
  const size_t kValueSize = 1024 * 1024 * 3;
  Options options;
  options.create_if_missing = true;
  options.wal_bytes_per_sync = 512;
  options.write_buffer_size = 100 * kValueSize;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(512, dbfull()->GetDBOptions().wal_bytes_per_sync);
  std::atomic_int counter{0};
  int low_bytes_per_sync = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::RangeSync:0",
      [&](void* /*arg*/) { counter.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  const std::string kValue(kValueSize, 'v');
  int i = 0;
  for (; i < 10; i++) {
    ASSERT_OK(Put(Key(i), kValue));
  }
  // Do not flush. If we flush here, SwitchWAL will reuse old WAL file since its
  // empty and will not get the new wal_bytes_per_sync value.
  low_bytes_per_sync = counter;
  // 5242880 = 1024 * 1024 * 5
  ASSERT_OK(dbfull()->SetDBOptions({{"wal_bytes_per_sync", "5242880"}}));
  ASSERT_EQ(5242880, dbfull()->GetDBOptions().wal_bytes_per_sync);
  counter = 0;
  i = 0;
  for (; i < 10; i++) {
    ASSERT_OK(Put(Key(i), kValue));
  }
  ASSERT_GT(counter, 0);
  ASSERT_GT(low_bytes_per_sync, 0);
  ASSERT_GT(low_bytes_per_sync, counter);
}

TEST_F(DBOptionsTest, MutableManifestOptions) {
  // These aren't end-to-end tests, but sufficient to ensure the VersionSet
  // receives the updates with SetDBOptions
  for (int64_t i : {0, 1, 100, 100000, 10000000}) {
    ASSERT_OK(
        db_->SetDBOptions({{"max_manifest_file_size", std::to_string(i)}}));
    ASSERT_EQ(i,
              static_cast<int64_t>(db_->GetDBOptions().max_manifest_file_size));
    ASSERT_EQ(i,
              static_cast<int64_t>(
                  dbfull()->GetVersionSet()->TEST_GetMinMaxManifestFileSize()));
    if (i > 1) {
      ++i;
    }
    ASSERT_OK(
        db_->SetDBOptions({{"max_manifest_space_amp_pct", std::to_string(i)}}));
    ASSERT_EQ(i, static_cast<int64_t>(
                     db_->GetDBOptions().max_manifest_space_amp_pct));
    ASSERT_EQ(i,
              static_cast<int64_t>(
                  dbfull()->GetVersionSet()->TEST_GetMaxManifestSpaceAmpPct()));
    if (i > 1) {
      ++i;
    }
    ASSERT_OK(db_->SetDBOptions(
        {{"manifest_preallocation_size", std::to_string(i)}}));
    ASSERT_EQ(i, static_cast<int64_t>(
                     db_->GetDBOptions().manifest_preallocation_size));
    ASSERT_EQ(
        i, static_cast<int64_t>(
               dbfull()->GetVersionSet()->TEST_GetManifestPreallocationSize()));
  }
}

TEST_F(DBOptionsTest, WritableFileMaxBufferSize) {
  Options options;
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 1024 * 1024;
  options.level0_file_num_compaction_trigger = 3;
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  options.env = env_;
  int buffer_size = 1024 * 1024;
  Reopen(options);
  ASSERT_EQ(buffer_size,
            dbfull()->GetDBOptions().writable_file_max_buffer_size);

  std::atomic<int> match_cnt(0);
  std::atomic<int> unmatch_cnt(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::WritableFileWriter:0", [&](void* arg) {
        int value = static_cast<int>(reinterpret_cast<uintptr_t>(arg));
        if (value == buffer_size) {
          match_cnt++;
        } else {
          unmatch_cnt++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  int i = 0;
  for (; i < 3; i++) {
    ASSERT_OK(Put("foo", std::to_string(i)));
    ASSERT_OK(Put("bar", std::to_string(i)));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(unmatch_cnt, 0);
  ASSERT_GE(match_cnt, 11);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"writable_file_max_buffer_size", "524288"}}));
  buffer_size = 512 * 1024;
  match_cnt = 0;
  unmatch_cnt = 0;  // SetDBOptions() will create a WritableFileWriter

  ASSERT_EQ(buffer_size,
            dbfull()->GetDBOptions().writable_file_max_buffer_size);
  i = 0;
  for (; i < 3; i++) {
    ASSERT_OK(Put("foo", std::to_string(i)));
    ASSERT_OK(Put("bar", std::to_string(i)));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(unmatch_cnt, 0);
  ASSERT_GE(match_cnt, 11);
}

TEST_F(DBOptionsTest, SetOptionsAndReopen) {
  Random rnd(1044);
  auto rand_opts = GetRandomizedMutableCFOptionsMap(&rnd);
  ASSERT_OK(dbfull()->SetOptions(rand_opts));
  // Verify if DB can be reopen after setting options.
  Options options;
  options.env = env_;
  ASSERT_OK(TryReopen(options));
}

TEST_F(DBOptionsTest, SetBlobFileWritableFileMaxBufferSize) {
  constexpr uint64_t kBlobWriterBufferSize = 128 * 1024;

  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  Reopen(options);

  ASSERT_EQ(db_->GetOptions().blob_file_writable_file_max_buffer_size, 0U);

  ASSERT_OK(db_->SetOptions({{"blob_file_writable_file_max_buffer_size",
                              std::to_string(kBlobWriterBufferSize)}}));

  ASSERT_EQ(db_->GetOptions().blob_file_writable_file_max_buffer_size,
            kBlobWriterBufferSize);
}

TEST_F(DBOptionsTest, EnableAutoCompactionAndTriggerStall) {
  const std::string kValue(1024, 'v');
  for (int method_type = 0; method_type < 2; method_type++) {
    for (int option_type = 0; option_type < 4; option_type++) {
      Options options;
      options.create_if_missing = true;
      options.disable_auto_compactions = true;
      options.write_buffer_size = 1024 * 1024 * 10;
      options.compression = CompressionType::kNoCompression;
      options.level0_file_num_compaction_trigger = 1;
      options.level0_stop_writes_trigger = std::numeric_limits<int>::max();
      options.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
      options.hard_pending_compaction_bytes_limit =
          std::numeric_limits<uint64_t>::max();
      options.soft_pending_compaction_bytes_limit =
          std::numeric_limits<uint64_t>::max();
      options.env = env_;

      DestroyAndReopen(options);
      int i = 0;
      for (; i < 1024; i++) {
        ASSERT_OK(Put(Key(i), kValue));
      }
      ASSERT_OK(Flush());
      for (; i < 1024 * 2; i++) {
        ASSERT_OK(Put(Key(i), kValue));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      ASSERT_EQ(2, NumTableFilesAtLevel(0));
      uint64_t l0_size = SizeAtLevel(0);

      switch (option_type) {
        case 0:
          // test with level0_stop_writes_trigger
          options.level0_stop_writes_trigger = 2;
          options.level0_slowdown_writes_trigger = 2;
          break;
        case 1:
          options.level0_slowdown_writes_trigger = 2;
          break;
        case 2:
          options.hard_pending_compaction_bytes_limit = l0_size;
          options.soft_pending_compaction_bytes_limit = l0_size;
          break;
        case 3:
          options.soft_pending_compaction_bytes_limit = l0_size;
          break;
      }
      Reopen(options);
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
      ASSERT_FALSE(dbfull()->TEST_write_controler().NeedsDelay());

      SyncPoint::GetInstance()->LoadDependency(
          {{"DBOptionsTest::EnableAutoCompactionAndTriggerStall:1",
            "BackgroundCallCompaction:0"},
           {"DBImpl::BackgroundCompaction():BeforePickCompaction",
            "DBOptionsTest::EnableAutoCompactionAndTriggerStall:2"},
           {"DBOptionsTest::EnableAutoCompactionAndTriggerStall:3",
            "DBImpl::BackgroundCompaction():AfterPickCompaction"}});
      // Block background compaction.
      SyncPoint::GetInstance()->EnableProcessing();

      switch (method_type) {
        case 0:
          ASSERT_OK(
              dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
          break;
        case 1:
          ASSERT_OK(dbfull()->EnableAutoCompaction(
              {dbfull()->DefaultColumnFamily()}));
          break;
      }
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:1");
      // Wait for stall condition recalculate.
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:2");

      switch (option_type) {
        case 0:
          ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
          break;
        case 1:
          ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
          ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
          break;
        case 2:
          ASSERT_TRUE(dbfull()->TEST_write_controler().IsStopped());
          break;
        case 3:
          ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
          ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
          break;
      }
      TEST_SYNC_POINT("DBOptionsTest::EnableAutoCompactionAndTriggerStall:3");

      // Background compaction executed.
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_FALSE(dbfull()->TEST_write_controler().IsStopped());
      ASSERT_FALSE(dbfull()->TEST_write_controler().NeedsDelay());
    }
  }
}

TEST_F(DBOptionsTest, SetOptionsMayTriggerCompaction) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 1000;
  options.env = env_;
  Reopen(options);
  for (int i = 0; i < 3; i++) {
    // Need to insert two keys to avoid trivial move.
    ASSERT_OK(Put("foo", std::to_string(i)));
    ASSERT_OK(Put("bar", std::to_string(i)));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("3", FilesPerLevel());
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "3"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1", FilesPerLevel());
}

TEST_F(DBOptionsTest, SetBackgroundCompactionThreads) {
  Options options;
  options.create_if_missing = true;
  options.max_background_compactions = 1;  // default value
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_compactions", "3"}}));
  ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());
  auto stop_token = dbfull()->TEST_write_controler().GetStopToken();
  ASSERT_EQ(3, dbfull()->TEST_BGCompactionsAllowed());
}

TEST_F(DBOptionsTest, SetBackgroundFlushThreads) {
  Options options;
  options.create_if_missing = true;
  options.max_background_flushes = 1;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(1, dbfull()->TEST_BGFlushesAllowed());
  ASSERT_EQ(1, env_->GetBackgroundThreads(Env::Priority::HIGH));
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_flushes", "3"}}));
  ASSERT_EQ(3, env_->GetBackgroundThreads(Env::Priority::HIGH));
  ASSERT_EQ(3, dbfull()->TEST_BGFlushesAllowed());
}

TEST_F(DBOptionsTest, SetBackgroundJobs) {
  Options options;
  options.create_if_missing = true;
  options.max_background_jobs = 8;
  options.env = env_;
  Reopen(options);

  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      options.max_background_jobs = 12;
      ASSERT_OK(dbfull()->SetDBOptions(
          {{"max_background_jobs",
            std::to_string(options.max_background_jobs)}}));
    }

    const int expected_max_flushes = options.max_background_jobs / 4;

    ASSERT_EQ(expected_max_flushes, dbfull()->TEST_BGFlushesAllowed());
    ASSERT_EQ(1, dbfull()->TEST_BGCompactionsAllowed());

    auto stop_token = dbfull()->TEST_write_controler().GetStopToken();

    const int expected_max_compactions = 3 * expected_max_flushes;

    ASSERT_EQ(expected_max_flushes, dbfull()->TEST_BGFlushesAllowed());
    ASSERT_EQ(expected_max_compactions, dbfull()->TEST_BGCompactionsAllowed());

    ASSERT_EQ(expected_max_flushes,
              env_->GetBackgroundThreads(Env::Priority::HIGH));
    ASSERT_EQ(expected_max_compactions,
              env_->GetBackgroundThreads(Env::Priority::LOW));
  }
}

TEST_F(DBOptionsTest, AvoidFlushDuringShutdown) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  WriteOptions write_without_wal;
  write_without_wal.disableWAL = true;

  ASSERT_FALSE(options.avoid_flush_during_shutdown);
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1", write_without_wal));
  Reopen(options);
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_EQ("1", FilesPerLevel());

  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v2", write_without_wal));
  ASSERT_OK(dbfull()->SetDBOptions({{"avoid_flush_during_shutdown", "true"}}));
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ("", FilesPerLevel());
}

TEST_F(DBOptionsTest, SetDelayedWriteRateOption) {
  Options options;
  options.create_if_missing = true;
  options.delayed_write_rate = 2 * 1024U * 1024U;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(2 * 1024U * 1024U,
            dbfull()->TEST_write_controler().max_delayed_write_rate());

  ASSERT_OK(dbfull()->SetDBOptions({{"delayed_write_rate", "20000"}}));
  ASSERT_EQ(20000, dbfull()->TEST_write_controler().max_delayed_write_rate());
}

TEST_F(DBOptionsTest, MaxTotalWalSizeChange) {
  Random rnd(1044);
  const auto value_size = size_t(1024);
  std::string value = rnd.RandomString(value_size);

  Options options;
  options.create_if_missing = true;
  options.env = env_;
  CreateColumnFamilies({"1", "2", "3"}, options);
  ReopenWithColumnFamilies({"default", "1", "2", "3"}, options);

  WriteOptions write_options;

  const int key_count = 100;
  for (int i = 0; i < key_count; ++i) {
    for (size_t cf = 0; cf < handles_.size(); ++cf) {
      ASSERT_OK(Put(static_cast<int>(cf), Key(i), value));
    }
  }
  ASSERT_OK(dbfull()->SetDBOptions({{"max_total_wal_size", "10"}}));

  for (size_t cf = 0; cf < handles_.size(); ++cf) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
    ASSERT_EQ("1", FilesPerLevel(static_cast<int>(cf)));
  }
}

TEST_F(DBOptionsTest, SetStatsDumpPeriodSec) {
  Options options;
  options.create_if_missing = true;
  options.stats_dump_period_sec = 5;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_dump_period_sec);

  for (int i = 0; i < 20; i++) {
    unsigned int num = rand() % 5000 + 1;
    ASSERT_OK(dbfull()->SetDBOptions(
        {{"stats_dump_period_sec", std::to_string(num)}}));
    ASSERT_EQ(num, dbfull()->GetDBOptions().stats_dump_period_sec);
  }
  Close();
}

TEST_F(DBOptionsTest, SetStatsDumpPeriodSecRace) {
  // This is a mini-stress test looking for inconsistency between the reported
  // state of the option and the behavior in effect for the DB, after the last
  // modification to that option (indefinite inconsistency).
  std::vector<std::thread> threads;
  for (int i = 0; i < 12; i++) {
    threads.emplace_back([this, i]() {
      ASSERT_OK(dbfull()->SetDBOptions(
          {{"stats_dump_period_sec", i % 2 ? "100" : "0"}}));
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  bool stats_dump_set = dbfull()->GetDBOptions().stats_dump_period_sec > 0;
  bool task_enabled = dbfull()->TEST_GetPeriodicTaskScheduler().TEST_HasTask(
      PeriodicTaskType::kDumpStats);

  ASSERT_EQ(stats_dump_set, task_enabled);
}

TEST_F(DBOptionsTest, SetOptionsAndFileRace) {
  // This is a mini-stress test looking for inconsistency between the reported
  // state of the option and what is persisted in the options file, after the
  // last modification to that option (indefinite inconsistency).
  std::vector<std::thread> threads;
  for (int i = 0; i < 12; i++) {
    threads.emplace_back([this, i]() {
      ASSERT_OK(dbfull()->SetOptions({{"ttl", std::to_string(i * 100)}}));
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto setting_in_mem = dbfull()->GetOptions().ttl;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  DBOptions db_options;
  ConfigOptions cfg;
  cfg.env = env_;
  ASSERT_OK(LoadLatestOptions(cfg, dbname_, &db_options, &cf_descs, nullptr));
  ASSERT_EQ(cf_descs.size(), 1);
  ASSERT_EQ(setting_in_mem, cf_descs[0].options.ttl);
}

TEST_F(DBOptionsTest, SetOptionsStatsPersistPeriodSec) {
  Options options;
  options.create_if_missing = true;
  options.stats_persist_period_sec = 5;
  options.env = env_;
  Reopen(options);
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_persist_period_sec);

  ASSERT_OK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "12345"}}));
  ASSERT_EQ(12345u, dbfull()->GetDBOptions().stats_persist_period_sec);
  ASSERT_NOK(dbfull()->SetDBOptions({{"stats_persist_period_sec", "abcde"}}));
  ASSERT_EQ(12345u, dbfull()->GetDBOptions().stats_persist_period_sec);
}

static void assert_candidate_files_empty(DBImpl* dbfull, const bool empty) {
  dbfull->TEST_LockMutex();
  JobContext job_context(0);
  dbfull->FindObsoleteFiles(&job_context, false);
  ASSERT_EQ(empty, job_context.full_scan_candidate_files.empty());
  dbfull->TEST_UnlockMutex();
  if (job_context.HaveSomethingToDelete()) {
    // fulfill the contract of FindObsoleteFiles by calling PurgeObsoleteFiles
    // afterwards; otherwise the test may hang on shutdown
    dbfull->PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
}

TEST_F(DBOptionsTest, DeleteObsoleteFilesPeriodChange) {
  Options options;
  options.env = env_;
  SetTimeElapseOnlySleepOnReopen(&options);
  options.create_if_missing = true;
  ASSERT_OK(TryReopen(options));

  // Verify that candidate files set is empty when no full scan requested.
  assert_candidate_files_empty(dbfull(), true);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"delete_obsolete_files_period_micros", "0"}}));

  // After delete_obsolete_files_period_micros updated to 0, the next call
  // to FindObsoleteFiles should make a full scan
  assert_candidate_files_empty(dbfull(), false);

  ASSERT_OK(
      dbfull()->SetDBOptions({{"delete_obsolete_files_period_micros", "20"}}));

  assert_candidate_files_empty(dbfull(), true);

  env_->MockSleepForMicroseconds(20);
  assert_candidate_files_empty(dbfull(), true);

  env_->MockSleepForMicroseconds(1);
  assert_candidate_files_empty(dbfull(), false);

  Close();
}

TEST_F(DBOptionsTest, MaxOpenFilesChange) {
  SpecialEnv env(env_);
  Options options;
  options.env = CurrentOptions().env;
  options.max_open_files = -1;

  Reopen(options);

  Cache* tc = dbfull()->TEST_table_cache();

  ASSERT_EQ(-1, dbfull()->GetDBOptions().max_open_files);
  ASSERT_LT(2000, tc->GetCapacity());
  ASSERT_OK(dbfull()->SetDBOptions({{"max_open_files", "1024"}}));
  ASSERT_EQ(1024, dbfull()->GetDBOptions().max_open_files);
  // examine the table cache (actual size should be 1014)
  ASSERT_GT(1500, tc->GetCapacity());
  Close();
}

TEST_F(DBOptionsTest, SanitizeDelayedWriteRate) {
  Options options;
  options.env = CurrentOptions().env;
  options.delayed_write_rate = 0;
  Reopen(options);
  ASSERT_EQ(16 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);

  options.rate_limiter.reset(NewGenericRateLimiter(31 * 1024 * 1024));
  Reopen(options);
  ASSERT_EQ(31 * 1024 * 1024, dbfull()->GetDBOptions().delayed_write_rate);
}

TEST_F(DBOptionsTest, SanitizeUniversalTTLCompaction) {
  Options options;
  options.env = CurrentOptions().env;
  options.compaction_style = kCompactionStyleUniversal;

  options.ttl = 0;
  options.periodic_compaction_seconds = 0;
  Reopen(options);
  ASSERT_EQ(0, dbfull()->GetOptions().ttl);
  ASSERT_EQ(0, dbfull()->GetOptions().periodic_compaction_seconds);

  options.ttl = 0;
  options.periodic_compaction_seconds = 100;
  Reopen(options);
  ASSERT_EQ(0, dbfull()->GetOptions().ttl);
  ASSERT_EQ(100, dbfull()->GetOptions().periodic_compaction_seconds);

  options.ttl = 100;
  options.periodic_compaction_seconds = 0;
  Reopen(options);
  ASSERT_EQ(100, dbfull()->GetOptions().ttl);
  ASSERT_EQ(100, dbfull()->GetOptions().periodic_compaction_seconds);

  options.ttl = 100;
  options.periodic_compaction_seconds = 500;
  Reopen(options);
  ASSERT_EQ(100, dbfull()->GetOptions().ttl);
  ASSERT_EQ(100, dbfull()->GetOptions().periodic_compaction_seconds);
}

TEST_F(DBOptionsTest, SanitizeTtlDefault) {
  Options options;
  options.env = CurrentOptions().env;
  Reopen(options);
  ASSERT_EQ(30 * 24 * 60 * 60, dbfull()->GetOptions().ttl);

  options.compaction_style = kCompactionStyleLevel;
  options.ttl = 0;
  Reopen(options);
  ASSERT_EQ(0, dbfull()->GetOptions().ttl);

  options.ttl = 100;
  Reopen(options);
  ASSERT_EQ(100, dbfull()->GetOptions().ttl);
}

TEST_F(DBOptionsTest, SanitizeFIFOPeriodicCompaction) {
  Options options;
  options.compaction_style = kCompactionStyleFIFO;
  options.env = CurrentOptions().env;
  // Default value allows RocksDB to set ttl to 30 days.
  ASSERT_EQ(30 * 24 * 60 * 60, dbfull()->GetOptions().ttl);

  // Disable
  options.ttl = 0;
  Reopen(options);
  ASSERT_EQ(0, dbfull()->GetOptions().ttl);

  options.ttl = 100;
  Reopen(options);
  ASSERT_EQ(100, dbfull()->GetOptions().ttl);

  options.ttl = 100 * 24 * 60 * 60;
  Reopen(options);
  ASSERT_EQ(100 * 24 * 60 * 60, dbfull()->GetOptions().ttl);

  // periodic_compaction_seconds should have no effect
  // on FIFO compaction.
  options.ttl = 500;
  options.periodic_compaction_seconds = 300;
  Reopen(options);
  ASSERT_EQ(500, dbfull()->GetOptions().ttl);
}

TEST_F(DBOptionsTest, SetFIFOCompactionOptions) {
  Options options;
  options.env = CurrentOptions().env;
  options.compaction_style = kCompactionStyleFIFO;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.write_buffer_size = 10 << 10;  // 10KB
  options.arena_block_size = 4096;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options.compaction_options_fifo.allow_compaction = false;
  options.num_levels = 1;
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  // Test dynamically changing ttl.
  options.ttl = 1 * 60 * 60;  // 1 hour
  ASSERT_OK(TryReopen(options));

  Random rnd(301);
  for (int i = 0; i < 10; i++) {
    // Generate and flush a file about 10KB.
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  env_->MockSleepForSeconds(61);

  // No files should be compacted as ttl is set to 1 hour.
  ASSERT_EQ(dbfull()->GetOptions().ttl, 3600);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  ASSERT_EQ(options.statistics->getTickerCount(FIFO_TTL_COMPACTIONS), 0);
  ASSERT_EQ(options.statistics->getTickerCount(FIFO_MAX_SIZE_COMPACTIONS), 0);

  // Set ttl to 1 minute. So all files should get deleted.
  ASSERT_OK(dbfull()->SetOptions({{"ttl", "60"}}));
  ASSERT_EQ(dbfull()->GetOptions().ttl, 60);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  ASSERT_GT(options.statistics->getTickerCount(FIFO_TTL_COMPACTIONS), 0);
  ASSERT_EQ(options.statistics->getTickerCount(FIFO_MAX_SIZE_COMPACTIONS), 0);
  ASSERT_OK(options.statistics->Reset());

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  // Test dynamically changing compaction_options_fifo.max_table_files_size
  options.compaction_options_fifo.max_table_files_size = 500 << 10;  // 00KB
  options.ttl = 0;
  DestroyAndReopen(options);

  for (int i = 0; i < 10; i++) {
    // Generate and flush a file about 10KB.
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // No files should be compacted as max_table_files_size is set to 500 KB.
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            500 << 10);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  ASSERT_EQ(options.statistics->getTickerCount(FIFO_MAX_SIZE_COMPACTIONS), 0);
  ASSERT_EQ(options.statistics->getTickerCount(FIFO_TTL_COMPACTIONS), 0);

  // Set max_table_files_size to 12 KB. So only 1 file should remain now.
  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo", "{max_table_files_size=12288;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            12 << 10);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  ASSERT_GT(options.statistics->getTickerCount(FIFO_MAX_SIZE_COMPACTIONS), 0);
  ASSERT_EQ(options.statistics->getTickerCount(FIFO_TTL_COMPACTIONS), 0);
  ASSERT_OK(options.statistics->Reset());

  // Test dynamically changing compaction_options_fifo.allow_compaction
  options.compaction_options_fifo.max_table_files_size = 500 << 10;  // 500KB
  options.ttl = 0;
  options.compaction_options_fifo.allow_compaction = false;
  options.level0_file_num_compaction_trigger = 6;
  DestroyAndReopen(options);

  for (int i = 0; i < 10; i++) {
    // Generate and flush a file about 10KB.
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // No files should be compacted as max_table_files_size is set to 500 KB and
  // allow_compaction is false
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            false);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // Set allow_compaction to true. So number of files should be between 1 and 5.
  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo", "{allow_compaction=true;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            true);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GE(NumTableFilesAtLevel(0), 1);
  ASSERT_LE(NumTableFilesAtLevel(0), 5);

  // Test dynamically setting `file_temperature_age_thresholds`
  ASSERT_TRUE(
      dbfull()
          ->GetOptions()
          .compaction_options_fifo.file_temperature_age_thresholds.empty());
  ASSERT_OK(dbfull()->SetOptions({{"compaction_options_fifo",
                                   "{file_temperature_age_thresholds={{age=10;"
                                   "temperature=kWarm}:{age=30000;"
                                   "temperature=kCold}}}"}}));
  auto opts = dbfull()->GetOptions();
  const auto& fifo_temp_opt =
      opts.compaction_options_fifo.file_temperature_age_thresholds;
  ASSERT_EQ(fifo_temp_opt.size(), 2);
  ASSERT_EQ(fifo_temp_opt[0].temperature, Temperature::kWarm);
  ASSERT_EQ(fifo_temp_opt[0].age, 10);
  ASSERT_EQ(fifo_temp_opt[1].temperature, Temperature::kCold);
  ASSERT_EQ(fifo_temp_opt[1].age, 30000);
}

TEST_F(DBOptionsTest, OffpeakTimes) {
  Options options;
  options.create_if_missing = true;
  Random rnd(test::RandomSeed());

  auto verify_invalid = [&]() {
    Status s = DBImpl::TEST_ValidateOptions(options);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsInvalidArgument());
  };

  auto verify_valid = [&]() {
    Status s = DBImpl::TEST_ValidateOptions(options);
    ASSERT_OK(s);
    ASSERT_FALSE(s.IsInvalidArgument());
  };
  std::vector<std::string> invalid_cases = {
      "06:30-",
      "-23:30",  // Both need to be set
      "00:00-00:00",
      "06:30-06:30"  //  Start time cannot be the same as end time
      "12:30 PM-23:30",
      "12:01AM-11:00PM",  // Invalid format
      "01:99-22:00",      // Invalid value for minutes
      "00:00-24:00",      // 24:00 is an invalid value
      "6-7",
      "6:-7",
      "06:31.42-7:00",
      "6.31:42-7:00",
      "6:0-7:",
      "15:0.2-3:.7",
      ":00-00:02",
      "02:00-:00",
      "random-value",
      "No:No-Hi:Hi",
  };

  std::vector<std::string> valid_cases = {
      "",  // Not enabled. Valid case
      "06:30-11:30",
      "06:30-23:30",
      "13:30-14:30",
      "00:00-23:59",  // Entire Day
      "23:30-01:15",  // From 11:30PM to 1:15AM next day. Valid case.
      "1:0000000000000-2:000000000042",  // Weird, but we can parse the int.
  };

  for (const std::string& invalid_case : invalid_cases) {
    options.daily_offpeak_time_utc = invalid_case;
    verify_invalid();
  }
  for (const std::string& valid_case : valid_cases) {
    options.daily_offpeak_time_utc = valid_case;
    verify_valid();
  }

  auto verify_offpeak_info = [&](bool expected_is_now_off_peak,
                                 int expected_seconds_till_next_offpeak_start,
                                 int now_utc_hour, int now_utc_minute,
                                 int now_utc_second = 0) {
    auto mock_clock = std::make_shared<MockSystemClock>(env_->GetSystemClock());
    // Add some extra random days to current time
    int days = rnd.Uniform(100);
    mock_clock->SetCurrentTime(
        days * OffpeakTimeOption::kSecondsPerDay +
        now_utc_hour * OffpeakTimeOption::kSecondsPerHour +
        now_utc_minute * OffpeakTimeOption::kSecondsPerMinute + now_utc_second);
    Status s = DBImpl::TEST_ValidateOptions(options);
    ASSERT_OK(s);
    auto offpeak_option = OffpeakTimeOption(options.daily_offpeak_time_utc);
    int64_t now;
    ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
    auto offpeak_info = offpeak_option.GetOffpeakTimeInfo(now);
    ASSERT_EQ(expected_is_now_off_peak, offpeak_info.is_now_offpeak);
    ASSERT_EQ(expected_seconds_till_next_offpeak_start,
              offpeak_info.seconds_till_next_offpeak_start);
  };

  options.daily_offpeak_time_utc = "";
  verify_offpeak_info(false, 0, 12, 30);

  options.daily_offpeak_time_utc = "06:30-11:30";
  verify_offpeak_info(false, 1 * OffpeakTimeOption::kSecondsPerHour, 5, 30);
  verify_offpeak_info(true, 24 * OffpeakTimeOption::kSecondsPerHour, 6, 30);
  verify_offpeak_info(true, 20 * OffpeakTimeOption::kSecondsPerHour, 10, 30);
  verify_offpeak_info(true, 19 * OffpeakTimeOption::kSecondsPerHour, 11, 30);
  verify_offpeak_info(false, 17 * OffpeakTimeOption::kSecondsPerHour, 13, 30);

  options.daily_offpeak_time_utc = "23:30-04:30";
  verify_offpeak_info(false, 17 * OffpeakTimeOption::kSecondsPerHour, 6, 30);
  verify_offpeak_info(true, 24 * OffpeakTimeOption::kSecondsPerHour, 23, 30);
  verify_offpeak_info(true,
                      23 * OffpeakTimeOption::kSecondsPerHour +
                          30 * OffpeakTimeOption::kSecondsPerMinute,
                      0, 0);
  verify_offpeak_info(true,
                      22 * OffpeakTimeOption::kSecondsPerHour +
                          30 * OffpeakTimeOption::kSecondsPerMinute,
                      1, 0);
  verify_offpeak_info(true, 19 * OffpeakTimeOption::kSecondsPerHour, 4, 30);
  verify_offpeak_info(false,
                      18 * OffpeakTimeOption::kSecondsPerHour +
                          59 * OffpeakTimeOption::kSecondsPerMinute,
                      4, 31);

  // Entire day offpeak
  options.daily_offpeak_time_utc = "00:00-23:59";
  verify_offpeak_info(true, 24 * OffpeakTimeOption::kSecondsPerHour, 0, 0);
  verify_offpeak_info(true, 12 * OffpeakTimeOption::kSecondsPerHour, 12, 00);
  verify_offpeak_info(true, 1 * OffpeakTimeOption::kSecondsPerMinute, 23, 59);
  verify_offpeak_info(true, 59, 23, 59, 1);
  verify_offpeak_info(true, 1, 23, 59, 59);

  // Start with a valid option
  options.daily_offpeak_time_utc = "01:30-04:15";
  DestroyAndReopen(options);
  ASSERT_EQ("01:30-04:15", dbfull()->GetDBOptions().daily_offpeak_time_utc);

  int may_schedule_compaction_called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MaybeScheduleFlushOrCompaction:Start",
      [&](void*) { may_schedule_compaction_called++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Make sure calling SetDBOptions with invalid option does not change the
  // value nor call MaybeScheduleFlushOrCompaction()
  for (std::string invalid_case : invalid_cases) {
    ASSERT_NOK(
        dbfull()->SetDBOptions({{"daily_offpeak_time_utc", invalid_case}}));
    ASSERT_EQ("01:30-04:15", dbfull()
                                 ->GetVersionSet()
                                 ->offpeak_time_option()
                                 .daily_offpeak_time_utc);
    ASSERT_EQ(1 * kSecondInHour + 30 * kSecondInMinute,
              dbfull()
                  ->GetVersionSet()
                  ->offpeak_time_option()
                  .daily_offpeak_start_time_utc);
    ASSERT_EQ(4 * kSecondInHour + 15 * kSecondInMinute,
              dbfull()
                  ->GetVersionSet()
                  ->offpeak_time_option()
                  .daily_offpeak_end_time_utc);
  }
  ASSERT_EQ(0, may_schedule_compaction_called);

  // Changing to new valid values should call MaybeScheduleFlushOrCompaction()
  // and sets the offpeak_time_option in VersionSet
  int expected_count = 0;
  for (std::string valid_case : valid_cases) {
    if (dbfull()
            ->GetVersionSet()
            ->offpeak_time_option()
            .daily_offpeak_time_utc != valid_case) {
      expected_count++;
    }
    ASSERT_OK(dbfull()->SetDBOptions({{"daily_offpeak_time_utc", valid_case}}));
    ASSERT_EQ(valid_case, dbfull()->GetDBOptions().daily_offpeak_time_utc);
    ASSERT_EQ(valid_case, dbfull()
                              ->GetVersionSet()
                              ->offpeak_time_option()
                              .daily_offpeak_time_utc);
  }
  ASSERT_EQ(expected_count, may_schedule_compaction_called);

  // Changing to the same value should not call MaybeScheduleFlushOrCompaction()
  ASSERT_OK(
      dbfull()->SetDBOptions({{"daily_offpeak_time_utc", "06:30-11:30"}}));
  may_schedule_compaction_called = 0;
  ASSERT_OK(
      dbfull()->SetDBOptions({{"daily_offpeak_time_utc", "06:30-11:30"}}));
  ASSERT_EQ(0, may_schedule_compaction_called);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

TEST_F(DBOptionsTest, CompactionReadaheadSizeChange) {
  for (bool use_direct_reads : {true, false}) {
    SpecialEnv env(env_);
    Options options;
    options.env = &env;

    options.use_direct_reads = use_direct_reads;
    options.level0_file_num_compaction_trigger = 2;
    const std::string kValue(1024, 'v');
    Status s = TryReopen(options);
    if (use_direct_reads && (s.IsNotSupported() || s.IsInvalidArgument())) {
      continue;
    } else {
      ASSERT_OK(s);
    }

    ASSERT_EQ(1024 * 1024 * 2,
              dbfull()->GetDBOptions().compaction_readahead_size);
    ASSERT_OK(dbfull()->SetDBOptions({{"compaction_readahead_size", "256"}}));
    ASSERT_EQ(256, dbfull()->GetDBOptions().compaction_readahead_size);
    for (int i = 0; i < 1024; i++) {
      ASSERT_OK(Put(Key(i), kValue));
    }
    ASSERT_OK(Flush());
    for (int i = 0; i < 1024 * 2; i++) {
      ASSERT_OK(Put(Key(i), kValue));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(256, env_->compaction_readahead_size_);
    Close();
  }
}

TEST_F(DBOptionsTest, FIFOTtlBackwardCompatible) {
  Options options;
  options.compaction_style = kCompactionStyleFIFO;
  options.write_buffer_size = 10 << 10;  // 10KB
  options.create_if_missing = true;
  options.env = CurrentOptions().env;
  options.num_levels = 1;

  ASSERT_OK(TryReopen(options));

  Random rnd(301);
  for (int i = 0; i < 10; i++) {
    // Generate and flush a file about 10KB.
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  // In release 6.0, ttl was promoted from a secondary level option under
  // compaction_options_fifo to a top level option under ColumnFamilyOptions.
  // We still need to handle old SetOptions calls but should ignore
  // ttl under compaction_options_fifo.
  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo",
        "{allow_compaction=true;max_table_files_size=1024;ttl=731;file_"
        "temperature_age_thresholds={temperature=kCold;age=12345}}"},
       {"ttl", "60"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            true);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            1024);
  auto opts = dbfull()->GetOptions();
  const auto& file_temp_age =
      opts.compaction_options_fifo.file_temperature_age_thresholds;
  ASSERT_EQ(file_temp_age.size(), 1);
  ASSERT_EQ(file_temp_age[0].temperature, Temperature::kCold);
  ASSERT_EQ(file_temp_age[0].age, 12345);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 60);

  // Put ttl as the first option inside compaction_options_fifo. That works as
  // it doesn't overwrite any other option.
  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo",
        "{ttl=985;allow_compaction=true;max_table_files_size=1024;}"},
       {"ttl", "191"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            true);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            1024);
  ASSERT_EQ(file_temp_age.size(), 1);
  ASSERT_EQ(file_temp_age[0].temperature, Temperature::kCold);
  ASSERT_EQ(file_temp_age[0].age, 12345);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 191);
}

TEST_F(DBOptionsTest, ChangeCompression) {
  if (!Snappy_Supported() || !LZ4_Supported()) {
    return;
  }
  Options options;
  options.write_buffer_size = 10 << 10;  // 10KB
  options.level0_file_num_compaction_trigger = 2;
  options.create_if_missing = true;
  options.compression = CompressionType::kLZ4Compression;
  options.bottommost_compression = CompressionType::kNoCompression;
  options.bottommost_compression_opts.level = 2;
  options.bottommost_compression_opts.parallel_threads = 1;
  options.env = CurrentOptions().env;

  ASSERT_OK(TryReopen(options));

  CompressionType compression_used = CompressionType::kLZ4Compression;
  CompressionOptions compression_opt_used;
  bool compacted = false;
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* c = static_cast<Compaction*>(arg);
        compression_used = c->output_compression();
        compression_opt_used = c->output_compression_opts();
        compacted = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("foo", "foofoofoo"));
  ASSERT_OK(Put("bar", "foofoofoo"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "foofoofoo"));
  ASSERT_OK(Put("bar", "foofoofoo"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(compacted);
  ASSERT_EQ(CompressionType::kNoCompression, compression_used);
  ASSERT_EQ(options.compression_opts.level, compression_opt_used.level);
  ASSERT_EQ(options.compression_opts.parallel_threads,
            compression_opt_used.parallel_threads);

  compression_used = CompressionType::kLZ4Compression;
  compacted = false;
  ASSERT_OK(dbfull()->SetOptions(
      {{"bottommost_compression", "kSnappyCompression"},
       {"bottommost_compression_opts", "0:6:0:0:4:true"}}));
  ASSERT_OK(Put("foo", "foofoofoo"));
  ASSERT_OK(Put("bar", "foofoofoo"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "foofoofoo"));
  ASSERT_OK(Put("bar", "foofoofoo"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(compacted);
  ASSERT_EQ(CompressionType::kSnappyCompression, compression_used);
  ASSERT_EQ(6, compression_opt_used.level);
  // Right now parallel_level is not yet allowed to be changed.

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBOptionsTest, BottommostCompressionOptsWithFallbackType) {
  // Verify the bottommost compression options still take effect even when the
  // bottommost compression type is left at its default value. Verify for both
  // automatic and manual compaction.
  if (!Snappy_Supported() || !LZ4_Supported()) {
    return;
  }

  constexpr int kUpperCompressionLevel = 1;
  constexpr int kBottommostCompressionLevel = 2;
  constexpr int kNumL0Files = 2;

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.compression = CompressionType::kLZ4Compression;
  options.compression_opts.level = kUpperCompressionLevel;
  options.bottommost_compression_opts.level = kBottommostCompressionLevel;
  options.bottommost_compression_opts.enabled = true;
  Reopen(options);

  CompressionType compression_used = CompressionType::kDisableCompressionOption;
  CompressionOptions compression_opt_used;
  bool compacted = false;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionPicker::RegisterCompaction:Registered", [&](void* arg) {
        Compaction* c = static_cast<Compaction*>(arg);
        compression_used = c->output_compression();
        compression_opt_used = c->output_compression_opts();
        compacted = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // First, verify for automatic compaction.
  for (int i = 0; i < kNumL0Files; ++i) {
    ASSERT_OK(Put("foo", "foofoofoo"));
    ASSERT_OK(Put("bar", "foofoofoo"));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_TRUE(compacted);
  ASSERT_EQ(CompressionType::kLZ4Compression, compression_used);
  ASSERT_EQ(kBottommostCompressionLevel, compression_opt_used.level);

  // Second, verify for manual compaction.
  compacted = false;
  compression_used = CompressionType::kDisableCompressionOption;
  compression_opt_used = CompressionOptions();
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(compacted);
  ASSERT_EQ(CompressionType::kLZ4Compression, compression_used);
  ASSERT_EQ(kBottommostCompressionLevel, compression_opt_used.level);
}

TEST_F(DBOptionsTest, FIFOTemperatureAgeThresholdValidation) {
  Options options = CurrentOptions();
  Destroy(options);

  options.num_levels = 1;
  options.compaction_style = kCompactionStyleFIFO;
  options.max_open_files = -1;
  // elements are not sorted
  // During DB open
  options.compaction_options_fifo.file_temperature_age_thresholds.push_back(
      {Temperature::kCold, 1000});
  options.compaction_options_fifo.file_temperature_age_thresholds.push_back(
      {Temperature::kWarm, 500});
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(std::strstr(
      s.getState(),
      "Option file_temperature_age_thresholds requires elements to be sorted "
      "in increasing order with respect to `age` field."));
  // Dynamically set option
  options.compaction_options_fifo.file_temperature_age_thresholds.pop_back();
  ASSERT_OK(TryReopen(options));
  s = db_->SetOptions({{"compaction_options_fifo",
                        "{file_temperature_age_thresholds={{temperature=kCold;"
                        "age=1000000}:{temperature=kWarm;age=1}}}"}});
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(std::strstr(
      s.getState(),
      "Option file_temperature_age_thresholds requires elements to be sorted "
      "in increasing order with respect to `age` field."));

  // not single level
  // During DB open
  options.num_levels = 2;
  s = TryReopen(options);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Option file_temperature_age_thresholds is only "
                          "supported when num_levels = 1."));
  // Dynamically set option
  options.compaction_options_fifo.file_temperature_age_thresholds.clear();
  DestroyAndReopen(options);
  s = db_->SetOptions(
      {{"compaction_options_fifo",
        "{file_temperature_age_thresholds={temperature=kCold;age=1000}}"}});
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Option file_temperature_age_thresholds is only "
                          "supported when num_levels = 1."));
}

TEST_F(DBOptionsTest, TempOptionsFailTest) {
  std::shared_ptr<FaultInjectionTestFS> fs;
  std::unique_ptr<Env> env;

  fs.reset(new FaultInjectionTestFS(env_->GetFileSystem()));
  env = NewCompositeEnv(fs);
  Options options = CurrentOptions();
  options.env = env.get();

  SyncPoint::GetInstance()->SetCallBack(
      "PersistRocksDBOptions:create",
      [&](void* /*arg*/) { fs->SetFilesystemActive(false); });
  SyncPoint::GetInstance()->SetCallBack(
      "PersistRocksDBOptions:written",
      [&](void* /*arg*/) { fs->SetFilesystemActive(true); });

  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NOK(TryReopen(options));
  SyncPoint::GetInstance()->DisableProcessing();

  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  uint64_t number;
  FileType type;
  bool found_temp_file = false;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type) && type == kTempFile) {
      found_temp_file = true;
    }
  }
  ASSERT_FALSE(found_temp_file);
}

TEST_F(DBOptionsTest, SetOptionsNoManifestWrite) {
  ASSERT_OK(Put("x", "x"));
  ASSERT_OK(Flush());

  // In addition to checking manifest file, we want to ensure that SetOptions
  // is essentially atomic, without releasing the DB mutex between applying
  // the options to the cfd and installing new Version and SuperVersion. We
  // probabilistically verify that by attempting to catch an inconsistency.
  auto* const cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  std::optional<std::thread> t;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WakeUpAndNotDone", [&](void* arg) {
        auto* mu = static_cast<InstrumentedMutex*>(arg);
        // Option not yet modified
        ASSERT_FALSE(cfd->GetLatestMutableCFOptions().disable_auto_compactions);
        ASSERT_FALSE(
            cfd->current()->GetMutableCFOptions().disable_auto_compactions);
        ASSERT_FALSE(
            cfd->GetCurrentMutableCFOptions().disable_auto_compactions);
        t = std::thread([mu, cfd]() {
          InstrumentedMutexLock l(mu);
          // Assuming above correctness, we can only acquire the mutex after
          // options fully installed.
          ASSERT_TRUE(
              cfd->GetLatestMutableCFOptions().disable_auto_compactions);
          ASSERT_TRUE(
              cfd->current()->GetMutableCFOptions().disable_auto_compactions);
          ASSERT_TRUE(
              cfd->GetCurrentMutableCFOptions().disable_auto_compactions);
        });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Baseline manifest file info
  std::vector<std::string> live_files;
  uint64_t orig_manifest_file_size;
  ASSERT_OK(dbfull()->GetLiveFiles(live_files, &orig_manifest_file_size));
  uint64_t orig_manifest_file_num = dbfull()->TEST_Current_Manifest_FileNo();

  // Although this test mostly concerns SetOptions, we also include SetDBOptions
  // just for the added scope
  ASSERT_OK(db_->SetDBOptions({{"max_open_files", "100"}}));
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));

  // Verify that our above check was activated and completed
  ASSERT_TRUE(t.has_value());
  t->join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify manifest was not written to
  uint64_t new_manifest_file_size;
  ASSERT_OK(dbfull()->GetLiveFiles(live_files, &new_manifest_file_size));
  uint64_t new_manifest_file_num = dbfull()->TEST_Current_Manifest_FileNo();
  ASSERT_EQ(orig_manifest_file_num, new_manifest_file_num);
  ASSERT_EQ(orig_manifest_file_size, new_manifest_file_size);

  ASSERT_EQ(Get("x"), "x");
}

TEST_F(DBOptionsTest, SetOptionsMultipleColumnFamilies) {
  Options options;
  options.create_if_missing = true;
  options.env = CurrentOptions().env;
  options.disable_auto_compactions = true;
  Reopen(options);

  // Create two additional column families
  CreateColumnFamilies({"cf1", "cf2"}, options);
  ReopenWithColumnFamilies({"default", "cf1", "cf2"}, options);

  // Verify initial state - auto compaction should be disabled
  ASSERT_TRUE(dbfull()->GetOptions(handles_[0]).disable_auto_compactions);
  ASSERT_TRUE(dbfull()->GetOptions(handles_[1]).disable_auto_compactions);
  ASSERT_TRUE(dbfull()->GetOptions(handles_[2]).disable_auto_compactions);

  // Set options on multiple column families at once
  ASSERT_OK(dbfull()->SetOptions({handles_[1], handles_[2]},
                                 {{"disable_auto_compactions", "false"}}));

  ASSERT_TRUE(
      dbfull()->GetOptions(handles_[0]).disable_auto_compactions);  // unchanged
  ASSERT_FALSE(
      dbfull()->GetOptions(handles_[1]).disable_auto_compactions);  // changed
  ASSERT_FALSE(
      dbfull()->GetOptions(handles_[2]).disable_auto_compactions);  // changed

  std::unordered_map<ColumnFamilyHandle*,
                     std::unordered_map<std::string, std::string>>
      options_map;
  options_map[handles_[0]] = {{"disable_auto_compactions", "false"}};
  options_map[handles_[1]] = {{"disable_auto_compactions", "true"}};
  options_map[handles_[2]] = {{"disable_auto_compactions", "true"}};
  ASSERT_OK(dbfull()->SetOptions(options_map));

  ASSERT_FALSE(dbfull()->GetOptions(handles_[0]).disable_auto_compactions);
  ASSERT_TRUE(dbfull()->GetOptions(handles_[1]).disable_auto_compactions);
  ASSERT_TRUE(dbfull()->GetOptions(handles_[2]).disable_auto_compactions);
}

TEST_F(DBOptionsTest, SetOptionsSkipsUnchangedValues) {
  ColumnFamilyHandle* const cfh = db_->DefaultColumnFamily();
  const std::unordered_map<std::string, std::string> opts = {
      {"periodic_compaction_seconds", "4800"},
      {"memtable_prefix_bloom_size_ratio", "0.1"},
      {"compaction_options_universal", "{min_merge_width=4}"}};
  ASSERT_TRUE(SetOptionsWritesOptionsFile(cfh, opts));
  ASSERT_FALSE(SetOptionsWritesOptionsFile(cfh, opts));

  // A request that does not parse changes nothing, so the next request that
  // changes nothing is still skipped.
  ASSERT_EQ(db_->SetOptions(cfh, {{"no_such_option", "1"}}).code(),
            Status::Code::kInvalidArgument);
  ASSERT_FALSE(SetOptionsWritesOptionsFile(cfh, opts));

  ASSERT_TRUE(SetOptionsWritesOptionsFile(
      cfh, {{"periodic_compaction_seconds", "9600"}}));
  ASSERT_EQ(db_->GetOptions(cfh).periodic_compaction_seconds, 9600);

  // A very small change to a double value is still a change.
  ASSERT_TRUE(SetOptionsWritesOptionsFile(
      cfh, {{"memtable_prefix_bloom_size_ratio", "0"}}));
  ASSERT_TRUE(SetOptionsWritesOptionsFile(
      cfh, {{"memtable_prefix_bloom_size_ratio", "0.0000001"}}));
}

TEST_F(DBOptionsTest, SetOptionsAlwaysAppliesTableFactoryFields) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(
      NewRibbonFilterPolicy(10, /*bloom_before_level=*/1));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  // SetOptions() changes this field in place on the live filter policy, so the
  // MutableCFOptions from before and after compare equal.
  const std::unordered_map<std::string, std::string> opts = {
      {"table_factory.filter_policy.bloom_before_level", "2"}};
  ASSERT_TRUE(SetOptionsWritesOptionsFile(db_->DefaultColumnFamily(), opts));
  ASSERT_TRUE(SetOptionsWritesOptionsFile(db_->DefaultColumnFamily(), opts));
}

TEST_F(DBOptionsTest, SetOptionsRewritesOptionsFileAfterWriteFailure) {
  auto fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env = NewCompositeEnv(fs);
  // Close the DB before env is destroyed, also when an assertion fails.
  Defer close_db([this]() { Close(); });
  Options options = CurrentOptions();
  options.env = env.get();
  Reopen(options);

  const std::unordered_map<std::string, std::string> opts = {
      {"write_buffer_size", "1048576"}};
  ASSERT_EQ(
      FailOptionsFileWrites(fs.get(), [&]() { return db_->SetOptions(opts); })
          .code(),
      Status::Code::kIOError);
  ASSERT_EQ(db_->GetOptions().write_buffer_size, 1048576);

  // Only the OPTIONS file has the old value, so the same request is applied
  // again before it is skipped.
  ASSERT_TRUE(SetOptionsWritesOptionsFile(db_->DefaultColumnFamily(), opts));
  ASSERT_EQ(PersistedCFOptions(kDefaultColumnFamilyName).write_buffer_size,
            1048576);
  ASSERT_FALSE(SetOptionsWritesOptionsFile(db_->DefaultColumnFamily(), opts));
}

TEST_F(DBOptionsTest, SetDBOptionsRewritesStaleOptionsFile) {
  auto fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env = NewCompositeEnv(fs);
  // Close the DB before env is destroyed, also when an assertion fails.
  Defer close_db([this]() { Close(); });
  Options options = CurrentOptions();
  options.env = env.get();
  // SetDBOptions() changes a bytes_per_sync of 0 to 1MB. Start with 1MB, so a
  // request with the current values changes nothing.
  options.bytes_per_sync = 1024 * 1024;
  Reopen(options);

  const std::unordered_map<std::string, std::string> db_opts = {
      {"max_background_jobs",
       std::to_string(db_->GetDBOptions().max_background_jobs)}};
  ASSERT_FALSE(WritesOptionsFile([&]() { return db_->SetDBOptions(db_opts); }));

  const std::unordered_map<std::string, std::string> cf_opts = {
      {"write_buffer_size", "1048576"}};
  ASSERT_EQ(FailOptionsFileWrites(fs.get(),
                                  [&]() { return db_->SetOptions(cf_opts); })
                .code(),
            Status::Code::kIOError);

  // The same SetDBOptions() request now writes the stale OPTIONS file again,
  // and then it is skipped.
  ASSERT_TRUE(WritesOptionsFile([&]() { return db_->SetDBOptions(db_opts); }));
  ASSERT_EQ(PersistedCFOptions(kDefaultColumnFamilyName).write_buffer_size,
            1048576);
  ASSERT_FALSE(WritesOptionsFile([&]() { return db_->SetDBOptions(db_opts); }));

  // The OPTIONS file is up to date, so SetOptions() skips the same request.
  ASSERT_FALSE(
      SetOptionsWritesOptionsFile(db_->DefaultColumnFamily(), cf_opts));
}

TEST_F(DBOptionsTest, SetOptionsFinishesUpdateLeftIncompleteByFailure) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two"}, options);

  // DB::SetOptions() changes the column families in the iteration order of
  // the map and stops at the first error.
  std::unordered_map<ColumnFamilyHandle*,
                     std::unordered_map<std::string, std::string>>
      opts_map = {{handles_[1], {}}, {handles_[2], {}}};
  auto it = opts_map.begin();
  ColumnFamilyHandle* const changed_cfh = it->first;
  it->second = {{"preserve_internal_time_seconds", "3600"}};
  (++it)->second = {{"no_such_option", "1"}};
  ASSERT_EQ(db_->SetOptions(opts_map).code(), Status::Code::kInvalidArgument);

  // The first column family has the new value, but its SuperVersion, the
  // OPTIONS file and the seqno-to-time worker do not.
  ColumnFamilyData* const changed_cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(changed_cfh)->cfd();
  ASSERT_EQ(db_->GetOptions(changed_cfh).preserve_internal_time_seconds, 3600);
  {
    InstrumentedMutexLock l(dbfull()->mutex());
    ASSERT_EQ(changed_cfd->GetCurrentMutableCFOptions()
                  .preserve_internal_time_seconds,
              0);
  }
  ASSERT_EQ(
      PersistedCFOptions(changed_cfh->GetName()).preserve_internal_time_seconds,
      0);
  const PeriodicTaskScheduler& scheduler =
      dbfull()->TEST_GetPeriodicTaskScheduler();
  ASSERT_FALSE(scheduler.TEST_HasTask(PeriodicTaskType::kRecordSeqnoTime));

  // A request on another column family that changes nothing is applied, and
  // it updates the OPTIONS file and the worker.
  ASSERT_TRUE(SetOptionsWritesOptionsFile(
      handles_[0],
      {{"write_buffer_size", std::to_string(options.write_buffer_size)}}));
  ASSERT_EQ(
      PersistedCFOptions(changed_cfh->GetName()).preserve_internal_time_seconds,
      3600);
  ASSERT_TRUE(scheduler.TEST_HasTask(PeriodicTaskType::kRecordSeqnoTime));

  // The changed column family still needs a new SuperVersion, so the same
  // request on it is applied once more before it is skipped.
  const std::unordered_map<std::string, std::string> opts = {
      {"preserve_internal_time_seconds", "3600"}};
  ASSERT_TRUE(SetOptionsWritesOptionsFile(changed_cfh, opts));
  {
    InstrumentedMutexLock l(dbfull()->mutex());
    ASSERT_EQ(changed_cfd->GetCurrentMutableCFOptions()
                  .preserve_internal_time_seconds,
              3600);
  }
  ASSERT_FALSE(SetOptionsWritesOptionsFile(changed_cfh, opts));
}

TEST_F(DBOptionsTest, SetOptionsReportsDroppedColumnFamily) {
  Options options = CurrentOptions();
  CreateColumnFamilies({"one"}, options);
  ASSERT_OK(db_->DropColumnFamily(handles_[0]));

  // A request that changes nothing still fails on a dropped column family.
  ASSERT_EQ(db_->SetOptions(handles_[0],
                            {{"write_buffer_size",
                              std::to_string(options.write_buffer_size)}})
                .code(),
            Status::Code::kColumnFamilyDropped);
}

TEST_F(DBOptionsTest, SetOptionsRewritesSnapshotAfterPartialTrackedBatch) {
  auto fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env = NewCompositeEnv(fs);
  // Close the DB before env is destroyed, also when an assertion fails.
  Defer close_db([this]() { Close(); });
  Options options = CurrentOptions();
  options.env = env.get();
  options.track_options_file_number_in_manifest = true;
  Reopen(options);
  ColumnFamilyHandle* const default_cfh = db_->DefaultColumnFamily();
  const std::unordered_map<std::string, std::string> opts = {
      {"write_buffer_size",
       std::to_string(db_->GetOptions().write_buffer_size)}};

  // The first create commits a snapshot that lists both new column families,
  // and then the second create fails.
  std::vector<ColumnFamilyHandle*> created;
  ASSERT_NOK(FailSecondManifestWrite(fs.get(), [&]() {
    return db_->CreateColumnFamilies(ColumnFamilyOptions(), {"one", "two"},
                                     &created);
  }));
  handles_.insert(handles_.end(), created.begin(), created.end());
  ASSERT_EQ(created.size(), 1U);
  ASSERT_EQ(PersistedCFNames(),
            (std::vector<std::string>{kDefaultColumnFamilyName, "one", "two"}));

  // So a request that changes no option writes a current snapshot, and then
  // the same request is skipped.
  ASSERT_TRUE(SetOptionsWritesOptionsFile(default_cfh, opts));
  ASSERT_EQ(PersistedCFNames(),
            (std::vector<std::string>{kDefaultColumnFamilyName, "one"}));
  ASSERT_FALSE(SetOptionsWritesOptionsFile(default_cfh, opts));

  // Only the last drop of a batch commits the new snapshot. Here the second
  // drop fails, so the committed snapshot still lists the dropped column
  // family.
  ColumnFamilyHandle* two = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "two", &two));
  handles_.push_back(two);
  ASSERT_NOK(FailSecondManifestWrite(
      fs.get(), [&]() { return db_->DropColumnFamilies({created[0], two}); }));
  ASSERT_EQ(PersistedCFNames(),
            (std::vector<std::string>{kDefaultColumnFamilyName, "one", "two"}));

  ASSERT_TRUE(SetOptionsWritesOptionsFile(default_cfh, opts));
  ASSERT_EQ(PersistedCFNames(),
            (std::vector<std::string>{kDefaultColumnFamilyName, "two"}));
  ASSERT_FALSE(SetOptionsWritesOptionsFile(default_cfh, opts));
}

TEST_F(DBOptionsTest, TrackedColumnFamilyChangeCommitsCurrentSnapshot) {
  auto fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env = NewCompositeEnv(fs);
  // Close the DB before env is destroyed, also when an assertion fails.
  Defer close_db([this]() { Close(); });
  Options options = CurrentOptions();
  options.env = env.get();
  options.track_options_file_number_in_manifest = true;
  Reopen(options);
  ColumnFamilyHandle* const default_cfh = db_->DefaultColumnFamily();

  // After a failed OPTIONS file write, each tracked column family change
  // commits a snapshot of the options in effect. So the request that failed is
  // then skipped.
  std::vector<ColumnFamilyHandle*> created;
  const std::vector<std::pair<size_t, std::function<Status()>>> changes = {
      {1048576,
       [&]() {
         const Status s = db_->CreateColumnFamilies(ColumnFamilyOptions(),
                                                    {"one", "two"}, &created);
         handles_.insert(handles_.end(), created.begin(), created.end());
         return s;
       }},
      {2097152, [&]() { return db_->DropColumnFamily(created[0]); }},
      {3145728, [&]() { return db_->DropColumnFamilies({created[1]}); }},
  };
  for (const auto& [write_buffer_size, change] : changes) {
    SCOPED_TRACE("write_buffer_size=" + std::to_string(write_buffer_size));
    const std::unordered_map<std::string, std::string> opts = {
        {"write_buffer_size", std::to_string(write_buffer_size)}};
    ASSERT_EQ(
        FailOptionsFileWrites(fs.get(), [&]() { return db_->SetOptions(opts); })
            .code(),
        Status::Code::kIOError);
    ASSERT_OK(change());
    ASSERT_EQ(PersistedCFOptions(kDefaultColumnFamilyName).write_buffer_size,
              write_buffer_size);
    ASSERT_FALSE(SetOptionsWritesOptionsFile(default_cfh, opts));
  }
}

// Confirms the default value and serialization/parse round-trip of the new
// option. No DB open required; exercises only the options-metadata layer.
TEST_F(DBOptionsTest, UseDirectIoForCompactionReadsRoundTrip) {
  // Default value must remain false to preserve existing semantics.
  ASSERT_FALSE(DBOptions().use_direct_io_for_compaction_reads);

  DBOptions parsed;
  ConfigOptions config_options;
  ASSERT_OK(GetDBOptionsFromString(config_options, DBOptions(),
                                   "use_direct_io_for_compaction_reads=true",
                                   &parsed));
  ASSERT_TRUE(parsed.use_direct_io_for_compaction_reads);
  ASSERT_OK(GetDBOptionsFromString(config_options, DBOptions(),
                                   "use_direct_io_for_compaction_reads=false",
                                   &parsed));
  ASSERT_FALSE(parsed.use_direct_io_for_compaction_reads);
}

// Validates that Open rejects the documented incompatible combination.
TEST_F(DBOptionsTest, UseDirectIoForCompactionReadsValidation) {
  // mmap_reads + use_direct_io_for_compaction_reads is rejected at Open
  // time, the same way mmap_reads + use_direct_reads has always been
  // rejected.
  Options bad_options = CurrentOptions();
  bad_options.create_if_missing = true;
  bad_options.allow_mmap_reads = true;
  bad_options.use_direct_io_for_compaction_reads = true;
  Status bad_status = TryReopen(bad_options);
  ASSERT_TRUE(bad_status.IsNotSupported()) << bad_status.ToString();
}

// Confirms the option is plumbed all the way to the live DB's options API:
// after opening with the flag set, GetDBOptions() reports it back. Only runs
// when the environment supports direct I/O.
TEST_F(DBOptionsTest, UseDirectIoForCompactionReadsLiveReopen) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.use_direct_io_for_compaction_reads = true;
  // Use a buffered user-read setup so the new flag is the one doing the work.
  options.use_direct_reads = false;
  options.use_direct_io_for_flush_and_compaction = false;
  Status s = TryReopen(options);
  if (s.IsNotSupported() || s.IsInvalidArgument()) {
    ROCKSDB_GTEST_BYPASS(
        "Direct I/O not supported in this test environment; live reopen "
        "cannot be exercised.");
    return;
  }
  ASSERT_OK(s);
  ASSERT_TRUE(dbfull()->GetDBOptions().use_direct_io_for_compaction_reads);
  Close();
}

TEST_F(DBOptionsTest, UseDirectIoForCompactionReadsUnsupportedFileSystem) {
  auto fs = std::make_shared<MockFileSystem>(Env::Default()->GetSystemClock(),
                                             /*supports_direct_io=*/false);
  std::unique_ptr<Env> mock_env = NewCompositeEnv(fs);

  Options options = CurrentOptions();
  options.env = mock_env.get();
  options.create_if_missing = true;
  options.use_direct_reads = false;
  options.use_direct_io_for_flush_and_compaction = false;
  options.use_direct_io_for_compaction_reads = true;

  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_NE(s.ToString().find("Direct I/O is not supported"), std::string::npos)
      << s.ToString();
}

TEST_F(
    DBOptionsTest,
    UseDirectIoForCompactionReadsSetDBOptionsKeepsVerificationReadsBuffered) {
  auto fs = std::make_shared<MockFileSystem>(Env::Default()->GetSystemClock(),
                                             /*supports_direct_io=*/true);
  std::unique_ptr<Env> mock_env = NewCompositeEnv(fs);

  Options options = CurrentOptions();
  options.env = mock_env.get();
  options.create_if_missing = true;
  options.use_direct_reads = false;
  options.use_direct_io_for_flush_and_compaction = false;
  options.use_direct_io_for_compaction_reads = true;

  ASSERT_OK(TryReopen(options));

  bool observed = false;
  FileOptions observed_file_options;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SetDBOptions:FileOptionsForCompaction", [&](void* arg) {
        observed = true;
        observed_file_options = *static_cast<FileOptions*>(arg);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->SetDBOptions({{"bytes_per_sync", "1024"}}));
  ASSERT_TRUE(observed);
  ASSERT_FALSE(observed_file_options.use_direct_reads);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// Exercises the FileSystem::OptimizeForCompactionTableRead and
// OptimizeForBlobFileRead helpers directly, asserting that the compaction-only
// flag does not affect shared FileSystem hooks.
TEST_F(DBOptionsTest, OptimizeForCompactionTableReadUsesGlobalDirectReadsOnly) {
  FileOptions in_opts;
  in_opts.use_direct_reads = false;

  {
    Options check_options;
    check_options.use_direct_reads = false;
    check_options.use_direct_io_for_compaction_reads = true;
    check_options.use_direct_io_for_flush_and_compaction = false;
    ImmutableDBOptions immutable(check_options);
    FileOptions sst_read =
        env_->GetFileSystem()->OptimizeForCompactionTableRead(in_opts,
                                                              immutable);
    FileOptions blob_read =
        env_->GetFileSystem()->OptimizeForBlobFileRead(in_opts, immutable);
    EXPECT_FALSE(sst_read.use_direct_reads);
    EXPECT_FALSE(blob_read.use_direct_reads);
    EXPECT_FALSE(sst_read.use_direct_writes);
  }

  {
    Options off_options;
    off_options.use_direct_reads = false;
    off_options.use_direct_io_for_compaction_reads = false;
    off_options.use_direct_io_for_flush_and_compaction = false;
    ImmutableDBOptions immutable_off(off_options);
    FileOptions sst_read_off =
        env_->GetFileSystem()->OptimizeForCompactionTableRead(in_opts,
                                                              immutable_off);
    EXPECT_FALSE(sst_read_off.use_direct_reads);
  }

  {
    Options global_on_options;
    global_on_options.use_direct_reads = true;
    global_on_options.use_direct_io_for_compaction_reads = false;
    global_on_options.use_direct_io_for_flush_and_compaction = false;
    ImmutableDBOptions immutable_global(global_on_options);
    FileOptions sst_read_global =
        env_->GetFileSystem()->OptimizeForCompactionTableRead(in_opts,
                                                              immutable_global);
    EXPECT_TRUE(sst_read_global.use_direct_reads);
  }

  {
    Options both_on;
    both_on.use_direct_reads = true;
    both_on.use_direct_io_for_compaction_reads = true;
    both_on.use_direct_io_for_flush_and_compaction = false;
    ImmutableDBOptions immutable_both(both_on);
    FileOptions sst_read_both =
        env_->GetFileSystem()->OptimizeForCompactionTableRead(in_opts,
                                                              immutable_both);
    EXPECT_TRUE(sst_read_both.use_direct_reads);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
