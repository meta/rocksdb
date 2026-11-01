// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"

#include "cache/cache_reservation_manager.h"
#include "db/forward_iterator.h"
#include "env/fs_readonly.h"
#include "env/mock_env.h"
#include "file/file_util.h"
#include "port/lang.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env_encryption.h"
#include "rocksdb/unique_id.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/format.h"
#include "util/random.h"

#if USE_COROUTINES
#include "folly/coro/BlockingWait.h"
#include "rocksdb/coro_db.h"
#endif  // USE_COROUTINES

namespace ROCKSDB_NAMESPACE {

namespace {
int64_t MaybeCurrentTime(Env* env) {
  int64_t time = 1337346000;  // arbitrary fallback default
  env->GetCurrentTime(&time).PermitUncheckedError();
  return time;
}

}  // anonymous namespace

// Special Env used to delay background operations

SpecialEnv::SpecialEnv(Env* base, bool time_elapse_only_sleep)
    : EnvWrapper(base),
      maybe_starting_time_(MaybeCurrentTime(base)),
      rnd_(301),
      sleep_counter_(this),
      time_elapse_only_sleep_(time_elapse_only_sleep),
      no_slowdown_(time_elapse_only_sleep) {
  delay_sstable_sync_.store(false, std::memory_order_release);
  drop_writes_.store(false, std::memory_order_release);
  no_space_.store(false, std::memory_order_release);
  non_writable_.store(false, std::memory_order_release);
  count_random_reads_ = false;
  count_sequential_reads_ = false;
  manifest_sync_error_.store(false, std::memory_order_release);
  manifest_write_error_.store(false, std::memory_order_release);
  log_write_error_.store(false, std::memory_order_release);
  no_file_overwrite_.store(false, std::memory_order_release);
  random_file_open_counter_.store(0, std::memory_order_relaxed);
  delete_count_.store(0, std::memory_order_relaxed);
  num_open_wal_file_.store(0);
  log_write_slowdown_ = 0;
  bytes_written_ = 0;
  sync_counter_ = 0;
  non_writeable_rate_ = 0;
  new_writable_count_ = 0;
  non_writable_count_ = 0;
  table_write_callback_ = nullptr;
}
DBTestBase::DBTestBase(const std::string path, bool env_do_fsync)
    : mem_env_(nullptr), encrypted_env_(nullptr), option_config_(kDefault) {
  Env* base_env = Env::Default();
  ConfigOptions config_options;
  EXPECT_OK(test::CreateEnvFromSystem(config_options, &base_env, &env_guard_));
  EXPECT_NE(nullptr, base_env);
  if (getenv("MEM_ENV")) {
    mem_env_ = MockEnv::Create(base_env, base_env->GetSystemClock());
  }
  if (auto ee = getenv("ENCRYPTED_ENV")) {
    std::shared_ptr<EncryptionProvider> provider;
    std::string provider_id = ee;
    if (provider_id.find('=') == std::string::npos &&
        !EndsWith(provider_id, "://test")) {
      provider_id = provider_id + "://test";
    }
    EXPECT_OK(EncryptionProvider::CreateFromString(ConfigOptions(), provider_id,
                                                   &provider));
    encrypted_env_ = NewEncryptedEnv(mem_env_ ? mem_env_ : base_env, provider);
  }
  env_ = new SpecialEnv(encrypted_env_ ? encrypted_env_
                                       : (mem_env_ ? mem_env_ : base_env));
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->skip_fsync_ = !env_do_fsync;
  dbname_ = test::PerThreadDBPath(env_, path);
  alternative_wal_dir_ = dbname_ + "/wal";
  alternative_db_log_dir_ = dbname_ + "/db_log_dir";
  EXPECT_OK(DestroyDir(env_, alternative_wal_dir_));
  EXPECT_OK(DestroyDir(env_, alternative_db_log_dir_));
  auto options = CurrentOptions();
  options.env = env_;
  auto delete_options = options;
  delete_options.wal_dir = alternative_wal_dir_;
  EXPECT_OK(DestroyDB(dbname_, delete_options));
  // Destroy it for not alternative WAL dir is used.
  EXPECT_OK(DestroyDB(dbname_, options));
  db_.reset();
  Reopen(options);
  Random::GetTLSInstance()->Reset(0xdeadbeef);
}

DBTestBase::~DBTestBase() {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
  Options options;
  options.db_paths.emplace_back(dbname_, 0);
  options.db_paths.emplace_back(dbname_ + "_2", 0);
  options.db_paths.emplace_back(dbname_ + "_3", 0);
  options.db_paths.emplace_back(dbname_ + "_4", 0);
  options.env = env_;

  if (getenv("KEEP_DB")) {
    printf("DB is still at %s\n", dbname_.c_str());
  } else {
    EXPECT_OK(DestroyDir(env_, alternative_wal_dir_));
    EXPECT_OK(DestroyDir(env_, alternative_db_log_dir_));
    EXPECT_OK(DestroyDB(dbname_, options));
  }
  // Ensure SstFileManager (and its DeleteScheduler background thread) is
  // destroyed before env_. The bg thread calls env_ virtual methods
  // (e.g. NewDirectory), so env_ must outlive it.
  last_options_.sst_file_manager.reset();
  delete env_;
}

bool DBTestBase::ShouldSkipOptions(int option_config, int skip_mask) {
  if ((skip_mask & kSkipUniversalCompaction) &&
      (option_config == kUniversalCompaction ||
       option_config == kUniversalCompactionMultiLevel ||
       option_config == kUniversalSubcompactions)) {
    return true;
  }
  if ((skip_mask & kSkipMergePut) && option_config == kMergePut) {
    return true;
  }
  if ((skip_mask & kSkipNoSeekToLast) &&
      (option_config == kHashLinkList || option_config == kHashSkipList)) {
    return true;
  }
  if ((skip_mask & kSkipPlainTable) &&
      (option_config == kPlainTableAllBytesPrefix ||
       option_config == kPlainTableFirstBytePrefix ||
       option_config == kPlainTableCappedPrefix ||
       option_config == kPlainTableCappedPrefixNonMmap)) {
    return true;
  }
  if ((skip_mask & kSkipHashIndex) &&
      (option_config == kBlockBasedTableWithPrefixHashIndex ||
       option_config == kBlockBasedTableWithWholeKeyHashIndex)) {
    return true;
  }
  if ((skip_mask & kSkipFIFOCompaction) && option_config == kFIFOCompaction) {
    return true;
  }
  if ((skip_mask & kSkipMmapReads) && option_config == kWalDirAndMmapReads) {
    return true;
  }
  if ((skip_mask & kSkipRowCache) && option_config == kRowCache) {
    return true;
  }
  return false;
}

// Switch to a fresh database with the next option configuration to
// test.  Return false if there are no more configurations to test.
bool DBTestBase::ChangeOptions(int skip_mask) {
  for (option_config_++; option_config_ < kEnd; option_config_++) {
    if (ShouldSkipOptions(option_config_, skip_mask)) {
      continue;
    }
    break;
  }

  if (option_config_ >= kEnd) {
    Destroy(last_options_);
    return false;
  } else {
    auto options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);
    return true;
  }
}

// Switch between different compaction styles.
bool DBTestBase::ChangeCompactOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kUniversalCompaction;
    Destroy(last_options_);
    auto options = CurrentOptions();
    options.create_if_missing = true;
    Reopen(options);
    return true;
  } else if (option_config_ == kUniversalCompaction) {
    option_config_ = kUniversalCompactionMultiLevel;
    Destroy(last_options_);
    auto options = CurrentOptions();
    options.create_if_missing = true;
    Reopen(options);
    return true;
  } else if (option_config_ == kUniversalCompactionMultiLevel) {
    option_config_ = kLevelSubcompactions;
    Destroy(last_options_);
    auto options = CurrentOptions();
    assert(options.max_subcompactions > 1);
    Reopen(options);
    return true;
  } else if (option_config_ == kLevelSubcompactions) {
    option_config_ = kUniversalSubcompactions;
    Destroy(last_options_);
    auto options = CurrentOptions();
    assert(options.max_subcompactions > 1);
    Reopen(options);
    return true;
  } else {
    return false;
  }
}

// Switch between different WAL settings
bool DBTestBase::ChangeWalOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kDBLogDir;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    options.create_if_missing = true;
    Reopen(options);
    return true;
  } else if (option_config_ == kDBLogDir) {
    option_config_ = kWalDirAndMmapReads;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    options.create_if_missing = true;
    Reopen(options);
    return true;
  } else if (option_config_ == kWalDirAndMmapReads) {
    option_config_ = kRecycleLogFiles;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    Reopen(options);
    return true;
  } else {
    return false;
  }
}

// Switch between different filter policy
// Jump from kDefault to kFilter to kFullFilter
bool DBTestBase::ChangeFilterOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kFilter;
  } else if (option_config_ == kFilter) {
    option_config_ = kFullFilterWithNewTableReaderForCompactions;
  } else if (option_config_ == kFullFilterWithNewTableReaderForCompactions) {
    option_config_ = kPartitionedFilterWithNewTableReaderForCompactions;
  } else {
    return false;
  }
  Destroy(last_options_);

  auto options = CurrentOptions();
  options.create_if_missing = true;
  EXPECT_OK(TryReopen(options));
  return true;
}

// Switch between different DB options for file ingestion tests.
bool DBTestBase::ChangeOptionsForFileIngestionTest() {
  if (option_config_ == kDefault) {
    option_config_ = kUniversalCompaction;
    Destroy(last_options_);
    auto options = CurrentOptions();
    options.create_if_missing = true;
    EXPECT_OK(TryReopen(options));
    return true;
  } else if (option_config_ == kUniversalCompaction) {
    option_config_ = kUniversalCompactionMultiLevel;
    Destroy(last_options_);
    auto options = CurrentOptions();
    options.create_if_missing = true;
    EXPECT_OK(TryReopen(options));
    return true;
  } else if (option_config_ == kUniversalCompactionMultiLevel) {
    option_config_ = kLevelSubcompactions;
    Destroy(last_options_);
    auto options = CurrentOptions();
    assert(options.max_subcompactions > 1);
    EXPECT_OK(TryReopen(options));
    return true;
  } else if (option_config_ == kLevelSubcompactions) {
    option_config_ = kUniversalSubcompactions;
    Destroy(last_options_);
    auto options = CurrentOptions();
    assert(options.max_subcompactions > 1);
    EXPECT_OK(TryReopen(options));
    return true;
  } else if (option_config_ == kUniversalSubcompactions) {
    option_config_ = kDirectIO;
    Destroy(last_options_);
    auto options = CurrentOptions();
    EXPECT_OK(TryReopen(options));
    return true;
  } else {
    return false;
  }
}

// Return the current option configuration.
Options DBTestBase::CurrentOptions(
    const anon::OptionsOverride& options_override) const {
  return GetOptions(option_config_, GetDefaultOptions(), options_override);
}

Options DBTestBase::CurrentOptions(
    const Options& default_options,
    const anon::OptionsOverride& options_override) const {
  return GetOptions(option_config_, default_options, options_override);
}

Options DBTestBase::GetDefaultOptions() const {
  Options options;
  options.write_buffer_size = 4090 * 4096;
  options.target_file_size_base = 2 * 1024 * 1024;
  options.max_bytes_for_level_base = 10 * 1024 * 1024;
  options.max_open_files = 5000;
  options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  options.compaction_pri = CompactionPri::kByCompensatedSize;
  // The original default value for this option is false,
  // and many unit tests assume this value. It also makes
  // it easier to create desired LSM shape in unit tests.
  // Unit tests for this option sets level_compaction_dynamic_level_bytes=true
  // explicitly.
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_;
  if (!env_->skip_fsync_) {
    options.track_and_verify_wals_in_manifest = true;
  }
  return options;
}

Options DBTestBase::GetOptions(
    int option_config, const Options& default_options,
    const anon::OptionsOverride& options_override) const {
  // this redundant copy is to minimize code change w/o having lint error.
  Options options = default_options;
  BlockBasedTableOptions table_options;
  bool set_block_based_table_factory = true;
#if !defined(OS_MACOSX) && !defined(OS_WIN) && !defined(OS_AIX)
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "NewRandomAccessFile:O_DIRECT");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "NewWritableFile:O_DIRECT");
#endif
  // kMustFreeHeapAllocations -> indicates ASAN build
  if (kMustFreeHeapAllocations && !options_override.full_block_cache) {
    // Detecting block cache use-after-free is normally difficult in unit
    // tests, because as a cache, it tends to keep unreferenced entries in
    // memory, and we normally want unit tests to take advantage of block
    // cache for speed. However, we also want a strong chance of detecting
    // block cache use-after-free in unit tests in ASAN builds, so for ASAN
    // builds we use a trivially small block cache to which entries can be
    // added but are immediately freed on no more references.
    table_options.block_cache = NewLRUCache(/* too small */ 1);
  }

  bool can_allow_mmap = IsMemoryMappedAccessSupported();
  switch (option_config) {
    case kHashSkipList:
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      options.memtable_factory.reset(NewHashSkipListRepFactory(16));
      options.allow_concurrent_memtable_write = false;
      options.unordered_write = false;
      break;
    case kPlainTableFirstBytePrefix:
      options.table_factory.reset(NewPlainTableFactory());
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      options.allow_mmap_reads = can_allow_mmap;
      options.max_sequential_skip_in_iterations = 999999;
      set_block_based_table_factory = false;
      break;
    case kPlainTableCappedPrefix:
      options.table_factory.reset(NewPlainTableFactory());
      options.prefix_extractor.reset(NewCappedPrefixTransform(8));
      options.allow_mmap_reads = can_allow_mmap;
      options.max_sequential_skip_in_iterations = 999999;
      set_block_based_table_factory = false;
      break;
    case kPlainTableCappedPrefixNonMmap:
      options.table_factory.reset(NewPlainTableFactory());
      options.prefix_extractor.reset(NewCappedPrefixTransform(8));
      options.allow_mmap_reads = false;
      options.max_sequential_skip_in_iterations = 999999;
      set_block_based_table_factory = false;
      break;
    case kPlainTableAllBytesPrefix:
      options.table_factory.reset(NewPlainTableFactory());
      options.prefix_extractor.reset(NewNoopTransform());
      options.allow_mmap_reads = can_allow_mmap;
      options.max_sequential_skip_in_iterations = 999999;
      set_block_based_table_factory = false;
      break;
    case kVectorRep:
      options.memtable_factory.reset(new VectorRepFactory(100));
      options.allow_concurrent_memtable_write = false;
      options.unordered_write = false;
      break;
    case kHashLinkList:
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      options.memtable_factory.reset(
          NewHashLinkListRepFactory(4, 0, 3, true, 4));
      options.allow_concurrent_memtable_write = false;
      options.unordered_write = false;
      break;
    case kDirectIO: {
      options.use_direct_reads = true;
      options.use_direct_io_for_flush_and_compaction = true;
      options.compaction_readahead_size = 2 * 1024 * 1024;
      SetupSyncPointsToMockDirectIO();
      break;
    }
    case kMergePut:
      options.merge_operator = MergeOperators::CreatePutOperator();
      break;
    case kFilter:
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
      break;
    case kFullFilterWithNewTableReaderForCompactions:
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
      options.compaction_readahead_size = 10 * 1024 * 1024;
      break;
    case kPartitionedFilterWithNewTableReaderForCompactions:
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
      table_options.partition_filters = true;
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      options.compaction_readahead_size = 10 * 1024 * 1024;
      break;
    case kUncompressed:
      options.compression = kNoCompression;
      break;
    case kNumLevel_3:
      options.num_levels = 3;
      break;
    case kDBLogDir:
      options.db_log_dir = alternative_db_log_dir_;
      break;
    case kWalDirAndMmapReads:
      options.wal_dir = alternative_wal_dir_;
      // mmap reads should be orthogonal to WalDir setting, so we piggyback to
      // this option config to test mmap reads as well
      options.allow_mmap_reads = can_allow_mmap;
      break;
    case kManifestFileSize:
      options.max_manifest_file_size = 50;     // 50 bytes
      options.max_manifest_space_amp_pct = 0;  // old behavior
      break;
    case kPerfOptions:
      options.delayed_write_rate = 8 * 1024 * 1024;
      options.report_bg_io_stats = true;
      // TODO(3.13) -- test more options
      break;
    case kUniversalCompaction:
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
      break;
    case kUniversalCompactionMultiLevel:
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 8;
      break;
    case kInfiniteMaxOpenFiles:
      options.max_open_files = -1;
      break;
    case kCRC32cChecksum: {
      // Old default was CRC32c, but XXH3 (new default) is faster on common
      // hardware
      table_options.checksum = kCRC32c;
      // Thrown in here for basic coverage:
      options.DisableExtraChecks();
      break;
    }
    case kFIFOCompaction: {
      options.compaction_style = kCompactionStyleFIFO;
      options.max_open_files = -1;
      break;
    }
    case kBlockBasedTableWithPrefixHashIndex: {
      table_options.index_type = BlockBasedTableOptions::kHashSearch;
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      break;
    }
    case kBlockBasedTableWithWholeKeyHashIndex: {
      table_options.index_type = BlockBasedTableOptions::kHashSearch;
      options.prefix_extractor.reset(NewNoopTransform());
      break;
    }
    case kBlockBasedTableWithPartitionedIndex: {
      table_options.format_version = 3;
      table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      options.prefix_extractor.reset(NewNoopTransform());
      break;
    }
    case kBlockBasedTableWithPartitionedIndexFormat4: {
      table_options.format_version = 4;
      // Format 4 changes the binary index format. Since partitioned index is a
      // super-set of simple indexes, we are also using kTwoLevelIndexSearch to
      // test this format.
      table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      // The top-level index in partition filters are also affected by format 4.
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
      table_options.partition_filters = true;
      table_options.index_block_restart_interval = 8;
      break;
    }
    case kBlockBasedTableWithIndexRestartInterval: {
      table_options.index_block_restart_interval = 8;
      break;
    }
    case kBlockBasedTableWithLatestFormat: {
      // In case different from default
      table_options.format_version =
          BlockBasedTableOptions::kLatestSupportedFormatVersion;
      break;
    }
    case kOptimizeFiltersForHits: {
      options.optimize_filters_for_hits = true;
      set_block_based_table_factory = true;
      break;
    }
    case kRowCache: {
      options.row_cache = NewLRUCache(1024 * 1024);
      break;
    }
    case kRecycleLogFiles: {
      options.recycle_log_file_num = 2;
      break;
    }
    case kLevelSubcompactions: {
      options.max_subcompactions = 4;
      break;
    }
    case kUniversalSubcompactions: {
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 8;
      options.max_subcompactions = 4;
      break;
    }
    case kConcurrentSkipList: {
      options.allow_concurrent_memtable_write = true;
      options.enable_write_thread_adaptive_yield = true;
      break;
    }
    case kPipelinedWrite: {
      options.enable_pipelined_write = true;
      break;
    }
    case kConcurrentWALWrites: {
      // This options optimize 2PC commit path
      options.two_write_queues = true;
      options.manual_wal_flush = true;
      break;
    }
    case kUnorderedWrite: {
      options.allow_concurrent_memtable_write = false;
      options.unordered_write = false;
      break;
    }
    case kBlockBasedTableWithBinarySearchWithFirstKeyIndex: {
      table_options.index_type =
          BlockBasedTableOptions::kBinarySearchWithFirstKey;
      break;
    }

    default:
      break;
  }

  if (options_override.filter_policy) {
    table_options.filter_policy = options_override.filter_policy;
    table_options.partition_filters = options_override.partition_filters;
    table_options.metadata_block_size = options_override.metadata_block_size;
  }
  if (set_block_based_table_factory) {
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }
  options.level_compaction_dynamic_level_bytes =
      options_override.level_compaction_dynamic_level_bytes;
  options.env = env_;
  options.create_if_missing = true;
  return options;
}

void DBTestBase::CreateColumnFamilies(const std::vector<std::string>& cfs,
                                      const Options& options) {
  ColumnFamilyOptions cf_opts(options);
  size_t cfi = handles_.size();
  handles_.resize(cfi + cfs.size());
  for (const auto& cf : cfs) {
    Status s = db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]);
    ASSERT_OK(s);
  }
}

void DBTestBase::CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                                       const Options& options) {
  CreateColumnFamilies(cfs, options);
  std::vector<std::string> cfs_plus_default = cfs;
  cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
  ReopenWithColumnFamilies(cfs_plus_default, options);
}

void DBTestBase::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const std::vector<Options>& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
}

void DBTestBase::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const Options& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
}

void DBTestBase::SetTimeElapseOnlySleepOnReopen(DBOptions* options) {
  time_elapse_only_sleep_on_reopen_ = true;

  // Need to disable stats dumping and persisting which also use
  // RepeatableThread, which uses InstrumentedCondVar::TimedWaitInternal.
  // With time_elapse_only_sleep_, this can hang on some platforms (MacOS)
  // because (a) on some platforms, pthread_cond_timedwait does not appear
  // to release the lock for other threads to operate if the deadline time
  // is already passed, and (b) TimedWait calls are currently a bad abstraction
  // because the deadline parameter is usually computed from Env time,
  // but is interpreted in real clock time.
  options->stats_dump_period_sec = 0;
  options->stats_persist_period_sec = 0;
}

void DBTestBase::MaybeInstallTimeElapseOnlySleep(const DBOptions& options) {
  if (time_elapse_only_sleep_on_reopen_) {
    assert(options.env == env_ ||
           static_cast_with_check<CompositeEnvWrapper>(options.env)
                   ->env_target() == env_);
    assert(options.stats_dump_period_sec == 0);
    assert(options.stats_persist_period_sec == 0);
    // We cannot set these before destroying the last DB because they might
    // cause a deadlock or similar without the appropriate options set in
    // the DB.
    env_->time_elapse_only_sleep_ = true;
    env_->no_slowdown_ = true;
  } else {
    // Going back in same test run is not yet supported, so no
    // reset in this case.
  }
}

Status DBTestBase::TryReopenWithColumnFamilies(
    const std::vector<std::string>& cfs, const std::vector<Options>& options) {
  Close();
  EXPECT_EQ(cfs.size(), options.size());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.emplace_back(cfs[i], options[i]);
  }
  DBOptions db_opts = DBOptions(options[0]);
  last_options_ = options[0];
  MaybeInstallTimeElapseOnlySleep(db_opts);
  Status s = DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  return s;
}

Status DBTestBase::TryReopenWithColumnFamilies(
    const std::vector<std::string>& cfs, const Options& options) {
  Close();
  std::vector<Options> v_opts(cfs.size(), options);
  return TryReopenWithColumnFamilies(cfs, v_opts);
}

Status DBTestBase::TryReopenReadOnlyWithColumnFamilies(
    const std::vector<std::string>& cfs, const std::vector<Options>& options) {
  Close();
  EXPECT_EQ(cfs.size(), options.size());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.emplace_back(cfs[i], options[i]);
  }
  DBOptions db_opts = DBOptions(options[0]);
  last_options_ = options[0];
  MaybeInstallTimeElapseOnlySleep(db_opts);
  return DB::OpenForReadOnly(db_opts, dbname_, column_families, &handles_,
                             &db_);
}

Status DBTestBase::TryReopenReadOnlyWithColumnFamilies(
    const std::vector<std::string>& cfs, const Options& options) {
  Close();
  std::vector<Options> v_opts(cfs.size(), options);
  return TryReopenReadOnlyWithColumnFamilies(cfs, v_opts);
}

void DBTestBase::Reopen(const Options& options) {
  ASSERT_OK(TryReopen(options));
}

void DBTestBase::Close() {
  for (auto h : handles_) {
    EXPECT_OK(db_->DestroyColumnFamilyHandle(h));
  }
  handles_.clear();
  db_.reset();
}

void DBTestBase::DestroyAndReopen(const Options& options) {
  // Destroy using last options
  Destroy(last_options_);
  Reopen(options);
}

void DBTestBase::Destroy(const Options& options, bool delete_cf_paths) {
  std::vector<ColumnFamilyDescriptor> column_families;
  if (delete_cf_paths) {
    for (size_t i = 0; i < handles_.size(); ++i) {
      ColumnFamilyDescriptor cfdescriptor;
      handles_[i]->GetDescriptor(&cfdescriptor).PermitUncheckedError();
      column_families.push_back(cfdescriptor);
    }
  }
  Close();
  ASSERT_OK(DestroyDB(dbname_, options, column_families));
}

Status DBTestBase::ReadOnlyReopen(const Options& options) {
  Close();
  MaybeInstallTimeElapseOnlySleep(options);
  Status s = DB::OpenForReadOnly(options, dbname_, &db_);
  return s;
}

Status DBTestBase::EnforcedReadOnlyReopen(const Options& options) {
  Close();
  Options options_copy = options;
  MaybeInstallTimeElapseOnlySleep(options_copy);
  auto fs_read_only =
      std::make_shared<ReadOnlyFileSystem>(env_->GetFileSystem());
  env_read_only_ = std::make_shared<CompositeEnvWrapper>(env_, fs_read_only);
  options_copy.env = env_read_only_.get();
  Status s = DB::OpenForReadOnly(options_copy, dbname_, &db_);
  return s;
}

Status DBTestBase::TryReopen(const Options& options) {
  Close();
  last_options_.table_factory.reset();
  // Note: operator= is an unsafe approach here since it destructs
  // std::shared_ptr in the same order of their creation, in contrast to
  // destructors which destructs them in the opposite order of creation. One
  // particular problem is that the cache destructor might invoke callback
  // functions that use Option members such as statistics. To work around this
  // problem, we manually call destructor of table_factory which eventually
  // clears the block cache.
  last_options_ = options;
  MaybeInstallTimeElapseOnlySleep(options);
  Status s = DB::Open(options, dbname_, &db_);
  return s;
}

bool DBTestBase::IsDirectIOSupported() {
  return test::IsDirectIOSupported(env_, dbname_);
}

bool DBTestBase::IsMemoryMappedAccessSupported() const {
  return (!encrypted_env_);
}

Status DBTestBase::Flush(int cf) {
  if (cf == 0) {
    return db_->Flush(FlushOptions());
  } else {
    return db_->Flush(FlushOptions(), handles_[cf]);
  }
}

Status DBTestBase::Flush(const std::vector<int>& cf_ids) {
  std::vector<ColumnFamilyHandle*> cfhs;
  std::for_each(cf_ids.begin(), cf_ids.end(),
                [&cfhs, this](int id) { cfhs.emplace_back(handles_[id]); });
  return db_->Flush(FlushOptions(), cfhs);
}

Status DBTestBase::Put(const Slice& k, const Slice& v, WriteOptions wo) {
  if (kMergePut == option_config_) {
    return db_->Merge(wo, k, v);
  } else {
    return db_->Put(wo, k, v);
  }
}

Status DBTestBase::Put(int cf, const Slice& k, const Slice& v,
                       WriteOptions wo) {
  if (kMergePut == option_config_) {
    return db_->Merge(wo, handles_[cf], k, v);
  } else {
    return db_->Put(wo, handles_[cf], k, v);
  }
}

Status DBTestBase::TimedPut(const Slice& k, const Slice& v,
                            uint64_t write_unix_time, WriteOptions wo) {
  return TimedPut(0, k, v, write_unix_time, wo);
}

Status DBTestBase::TimedPut(int cf, const Slice& k, const Slice& v,
                            uint64_t write_unix_time, WriteOptions wo) {
  WriteBatch wb(/*reserved_bytes=*/0, /*max_bytes=*/0,
                wo.protection_bytes_per_key,
                /*default_cf_ts_sz=*/0);
  ColumnFamilyHandle* cfh;
  if (cf != 0) {
    cfh = handles_[cf];
  } else {
    cfh = db_->DefaultColumnFamily();
  }
  EXPECT_OK(wb.TimedPut(cfh, k, v, write_unix_time));
  return db_->Write(wo, &wb);
}

Status DBTestBase::Merge(const Slice& k, const Slice& v, WriteOptions wo) {
  return db_->Merge(wo, k, v);
}

Status DBTestBase::Merge(int cf, const Slice& k, const Slice& v,
                         WriteOptions wo) {
  return db_->Merge(wo, handles_[cf], k, v);
}

Status DBTestBase::Delete(const std::string& k) {
  return db_->Delete(WriteOptions(), k);
}

Status DBTestBase::Delete(int cf, const std::string& k) {
  return db_->Delete(WriteOptions(), handles_[cf], k);
}

Status DBTestBase::SingleDelete(const std::string& k) {
  return db_->SingleDelete(WriteOptions(), k);
}

Status DBTestBase::SingleDelete(int cf, const std::string& k) {
  return db_->SingleDelete(WriteOptions(), handles_[cf], k);
}

std::string DBTestBase::Get(const std::string& k, const Snapshot* snapshot,
                            bool use_coroutine) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  std::string result;
  Status s;
#if USE_COROUTINES
  if (use_coroutine) {
    PinnableSlice pinnable_value(&result);
    s = folly::coro::blockingWait(CoroDB::CoGet(
        dbfull(), options, dbfull()->DefaultColumnFamily(), k, &pinnable_value,
        /*timestamp=*/nullptr));
    if (s.ok() && pinnable_value.IsPinned()) {
      result.assign(pinnable_value.data(), pinnable_value.size());
    }
  } else
#else
  (void)use_coroutine;
#endif  // USE_COROUTINES
  {
    s = db_->Get(options, k, &result);
  }
  if (s.IsNotFound()) {
    result = "NOT_FOUND";
  } else if (!s.ok()) {
    result = s.ToString();
  }
  return result;
}

std::string DBTestBase::Get(int cf, const std::string& k,
                            const Snapshot* snapshot, bool use_coroutine) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  std::string result;
  Status s;
#if USE_COROUTINES
  if (use_coroutine) {
    PinnableSlice pinnable_value(&result);
    s = folly::coro::blockingWait(CoroDB::CoGet(dbfull(), options, handles_[cf],
                                                k, &pinnable_value,
                                                /*timestamp=*/nullptr));
    if (s.ok() && pinnable_value.IsPinned()) {
      result.assign(pinnable_value.data(), pinnable_value.size());
    }
  } else
#else
  (void)use_coroutine;
#endif  // USE_COROUTINES
  {
    s = db_->Get(options, handles_[cf], k, &result);
  }
  if (s.IsNotFound()) {
    result = "NOT_FOUND";
  } else if (!s.ok()) {
    result = s.ToString();
  }
  return result;
}

std::vector<std::string> DBTestBase::MultiGet(std::vector<int> cfs,
                                              const std::vector<std::string>& k,
                                              const Snapshot* snapshot,
                                              bool batched, bool async,
                                              bool use_coroutine) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  options.async_io = async;
  std::vector<ColumnFamilyHandle*> handles;
  std::vector<Slice> keys;
  std::vector<std::string> result;

  for (unsigned int i = 0; i < cfs.size(); ++i) {
    handles.push_back(handles_[cfs[i]]);
    keys.emplace_back(k[i]);
  }
  std::vector<Status> s;
  if (!batched) {
    s = db_->MultiGet(options, handles, keys, &result);
    for (size_t i = 0; i < s.size(); ++i) {
      if (s[i].IsNotFound()) {
        result[i] = "NOT_FOUND";
      } else if (!s[i].ok()) {
        result[i] = s[i].ToString();
      }
    }
  } else {
    std::vector<PinnableSlice> pin_values(cfs.size());
    result.resize(cfs.size());
    s.resize(cfs.size());
#if USE_COROUTINES
    if (use_coroutine) {
      folly::coro::blockingWait(CoroDB::CoMultiGet(
          dbfull(), options, cfs.size(), handles.data(), keys.data(),
          pin_values.data(), /*timestamps=*/nullptr, s.data(),
          /*sorted_input=*/false));
    } else
#else
    (void)use_coroutine;
#endif  // USE_COROUTINES
    {
      db_->MultiGet(options, cfs.size(), handles.data(), keys.data(),
                    pin_values.data(), s.data());
    }
    for (size_t i = 0; i < s.size(); ++i) {
      if (s[i].IsNotFound()) {
        result[i] = "NOT_FOUND";
      } else if (!s[i].ok()) {
        result[i] = s[i].ToString();
      } else {
        result[i].assign(pin_values[i].data(), pin_values[i].size());
        // Increase likelihood of detecting potential use-after-free bugs with
        // PinnableSlices tracking the same resource
        pin_values[i].Reset();
      }
    }
  }
  return result;
}

std::vector<std::string> DBTestBase::MultiGet(const std::vector<std::string>& k,
                                              const Snapshot* snapshot,
                                              bool async,
                                              bool optimize_multiget_for_io,
                                              bool use_coroutine) {
  std::vector<Status> statuses;
  std::vector<PinnableSlice> pin_values;
  MultiGet(k, snapshot, async, optimize_multiget_for_io, use_coroutine,
           pin_values, statuses);

  std::vector<std::string> result(k.size());
  for (size_t i = 0; i < statuses.size(); ++i) {
    if (statuses[i].IsNotFound()) {
      result[i] = "NOT_FOUND";
    } else if (!statuses[i].ok()) {
      result[i] = statuses[i].ToString();
    } else {
      result[i].assign(pin_values[i].data(), pin_values[i].size());
      // Increase likelihood of detecting potential use-after-free bugs with
      // PinnableSlices tracking the same resource
      pin_values[i].Reset();
    }
  }
  return result;
}

void DBTestBase::MultiGet(const std::vector<std::string>& k,
                          const Snapshot* snapshot, bool async,
                          bool optimize_multiget_for_io, bool use_coroutine,
                          std::vector<PinnableSlice>& values,
                          std::vector<Status>& statuses) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  options.async_io = async;
  options.optimize_multiget_for_io = optimize_multiget_for_io;
  std::vector<Slice> keys;
  values.clear();
  statuses.resize(k.size());
  values.resize(k.size());

  for (size_t i = 0; i < k.size(); ++i) {
    keys.emplace_back(k[i]);
  }
#if USE_COROUTINES
  if (use_coroutine) {
    std::vector<ColumnFamilyHandle*> cfs(keys.size(),
                                         dbfull()->DefaultColumnFamily());
    folly::coro::blockingWait(CoroDB::CoMultiGet(
        dbfull(), options, keys.size(), cfs.data(), keys.data(), values.data(),
        /*timestamps=*/nullptr, statuses.data(),
        /*sorted_input=*/false));
  } else
#else
  (void)use_coroutine;
#endif  // USE_COROUTINES
  {
    db_->MultiGet(options, dbfull()->DefaultColumnFamily(), keys.size(),
                  keys.data(), values.data(), statuses.data());
  }
}

Status DBTestBase::Get(const std::string& k, PinnableSlice* v,
                       bool use_coroutine) {
  ReadOptions options;
  options.verify_checksums = true;
#if USE_COROUTINES
  if (use_coroutine) {
    return folly::coro::blockingWait(
        CoroDB::CoGet(dbfull(), options, dbfull()->DefaultColumnFamily(), k, v,
                      /*timestamp=*/nullptr));
  }
#else
  (void)use_coroutine;
#endif  // USE_COROUTINES
  return dbfull()->Get(options, dbfull()->DefaultColumnFamily(), k, v);
}

Status DBTestBase::CompactRange(const CompactRangeOptions& options,
                                std::optional<Slice> begin,
                                std::optional<Slice> end) {
  return db_->CompactRange(options, begin ? &begin.value() : nullptr,
                           end ? &end.value() : nullptr);
}

uint64_t DBTestBase::GetNumSnapshots() {
  uint64_t int_num;
  EXPECT_TRUE(dbfull()->GetIntProperty("rocksdb.num-snapshots", &int_num));
  return int_num;
}

uint64_t DBTestBase::GetTimeOldestSnapshots() {
  uint64_t int_num;
  EXPECT_TRUE(
      dbfull()->GetIntProperty("rocksdb.oldest-snapshot-time", &int_num));
  return int_num;
}

uint64_t DBTestBase::GetSequenceOldestSnapshots() {
  uint64_t int_num;
  EXPECT_TRUE(
      dbfull()->GetIntProperty("rocksdb.oldest-snapshot-sequence", &int_num));
  return int_num;
}

// Return a string that contains all key,value pairs in order,
// formatted like "(k1->v1)(k2->v2)".
std::string DBTestBase::Contents(int cf) {
  std::vector<std::string> forward;
  std::string result;
  Iterator* iter = (cf == 0) ? db_->NewIterator(ReadOptions())
                             : db_->NewIterator(ReadOptions(), handles_[cf]);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string s = IterStatus(iter);
    result.push_back('(');
    result.append(s);
    result.push_back(')');
    forward.push_back(s);
  }

  // Check reverse iteration results are the reverse of forward results
  unsigned int matched = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    EXPECT_LT(matched, forward.size());
    EXPECT_EQ(IterStatus(iter), forward[forward.size() - matched - 1]);
    matched++;
  }
  EXPECT_OK(iter->status());
  EXPECT_EQ(matched, forward.size());

  delete iter;
  return result;
}

void DBTestBase::CheckAllEntriesWithFifoReopen(
    const std::string& expected_value, const Slice& user_key, int cf,
    const std::vector<std::string>& cfs, const Options& options) {
  ASSERT_EQ(AllEntriesFor(user_key, cf), expected_value);

  std::vector<std::string> cfs_plus_default = cfs;
  cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);

  Options fifo_options(options);
  fifo_options.compaction_style = kCompactionStyleFIFO;
  fifo_options.max_open_files = -1;
  fifo_options.disable_auto_compactions = true;
  ASSERT_OK(TryReopenWithColumnFamilies(cfs_plus_default, fifo_options));
  ASSERT_EQ(AllEntriesFor(user_key, cf), expected_value);

  ASSERT_OK(TryReopenWithColumnFamilies(cfs_plus_default, options));
  ASSERT_EQ(AllEntriesFor(user_key, cf), expected_value);
}

std::string DBTestBase::AllEntriesFor(const Slice& user_key, int cf) {
  Arena arena;
  auto options = CurrentOptions();
  InternalKeyComparator icmp(options.comparator);
  ReadOptions read_options;
  ScopedArenaPtr<InternalIterator> iter;
  if (cf == 0) {
    iter.reset(dbfull()->NewInternalIterator(read_options, &arena,
                                             kMaxSequenceNumber));
  } else {
    iter.reset(dbfull()->NewInternalIterator(read_options, &arena,
                                             kMaxSequenceNumber, handles_[cf]));
  }
  InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
  iter->Seek(target.Encode());
  std::string result;
  if (!iter->status().ok()) {
    result = iter->status().ToString();
  } else {
    result = "[ ";
    bool first = true;
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      if (ParseInternalKey(iter->key(), &ikey, true /* log_err_key */) !=
          Status::OK()) {
        result += "CORRUPTED";
      } else {
        if (!last_options_.comparator->Equal(ikey.user_key, user_key)) {
          break;
        }
        if (!first) {
          result += ", ";
        }
        first = false;
        switch (ikey.type) {
          case kTypeValue:
            result += iter->value().ToString();
            break;
          case kTypeMerge:
            // keep it the same as kTypeValue for testing kMergePut
            result += iter->value().ToString();
            break;
          case kTypeDeletion:
            result += "DEL";
            break;
          case kTypeSingleDeletion:
            result += "SDEL";
            break;
          default:
            assert(false);
            break;
        }
      }
      iter->Next();
    }
    if (!first) {
      result += " ";
    }
    result += "]";
  }
  return result;
}

int DBTestBase::NumSortedRuns(int cf) {
  ColumnFamilyMetaData cf_meta;
  if (cf == 0) {
    db_->GetColumnFamilyMetaData(&cf_meta);
  } else {
    db_->GetColumnFamilyMetaData(handles_[cf], &cf_meta);
  }
  int num_sr = static_cast<int>(cf_meta.levels[0].files.size());
  for (size_t i = 1U; i < cf_meta.levels.size(); i++) {
    if (cf_meta.levels[i].files.size() > 0) {
      num_sr++;
    }
  }
  return num_sr;
}

uint64_t DBTestBase::TotalSize(int cf) {
  ColumnFamilyMetaData cf_meta;
  if (cf == 0) {
    db_->GetColumnFamilyMetaData(&cf_meta);
  } else {
    db_->GetColumnFamilyMetaData(handles_[cf], &cf_meta);
  }
  return cf_meta.size;
}

uint64_t DBTestBase::SizeAtLevel(int level) {
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  uint64_t sum = 0;
  for (const auto& m : metadata) {
    if (m.level == level) {
      sum += m.size;
    }
  }
  return sum;
}

size_t DBTestBase::TotalLiveFiles(int cf) {
  ColumnFamilyMetaData cf_meta;
  if (cf == 0) {
    db_->GetColumnFamilyMetaData(&cf_meta);
  } else {
    db_->GetColumnFamilyMetaData(handles_[cf], &cf_meta);
  }
  size_t num_files = 0;
  for (auto& level : cf_meta.levels) {
    num_files += level.files.size();
  }
  return num_files;
}

size_t DBTestBase::TotalLiveFilesAtPath(int cf, const std::string& path) {
  ColumnFamilyMetaData cf_meta;
  if (cf == 0) {
    db_->GetColumnFamilyMetaData(&cf_meta);
  } else {
    db_->GetColumnFamilyMetaData(handles_[cf], &cf_meta);
  }
  size_t num_files = 0;
  for (auto& level : cf_meta.levels) {
    for (auto& f : level.files) {
      if (f.directory == path) {
        num_files++;
      }
    }
  }
  return num_files;
}

size_t DBTestBase::CountLiveFiles() {
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  return metadata.size();
}

int DBTestBase::NumTableFilesAtLevel(int level, int cf) {
  return NumTableFilesAtLevel(level,
                              cf ? handles_[cf] : db_->DefaultColumnFamily());
}

int DBTestBase::NumTableFilesAtLevel(int level, ColumnFamilyHandle* cfh,
                                     DB* db) {
  if (!db) {
    db = db_.get();
  }
  std::string property;
  EXPECT_TRUE(db->GetProperty(
      cfh, "rocksdb.num-files-at-level" + std::to_string(level), &property));
  return atoi(property.c_str());
}

double DBTestBase::CompressionRatioAtLevel(int level, int cf) {
  std::string property;
  if (cf == 0) {
    // default cfd
    EXPECT_TRUE(db_->GetProperty(
        "rocksdb.compression-ratio-at-level" + std::to_string(level),
        &property));
  } else {
    EXPECT_TRUE(db_->GetProperty(
        handles_[cf],
        "rocksdb.compression-ratio-at-level" + std::to_string(level),
        &property));
  }
  return std::stod(property);
}

int DBTestBase::TotalTableFiles(int cf, int levels) {
  if (levels == -1) {
    levels = (cf == 0) ? db_->NumberLevels() : db_->NumberLevels(handles_[1]);
  }
  int result = 0;
  for (int level = 0; level < levels; level++) {
    result += NumTableFilesAtLevel(level, cf);
  }
  return result;
}

// Return spread of files per level
std::string DBTestBase::FilesPerLevel(int cf) {
  if (cf == 0) {
    return FilesPerLevel(db_->DefaultColumnFamily());
  } else {
    return FilesPerLevel(handles_[cf]);
  }
}

std::string DBTestBase::FilesPerLevel(ColumnFamilyHandle* cfh, DB* db) {
  if (!db) {
    db = db_.get();
  }
  int num_levels = db->NumberLevels(cfh);
  std::string result;
  size_t last_non_zero_offset = 0;
  for (int level = 0; level < num_levels; level++) {
    int f = NumTableFilesAtLevel(level, cfh, db);
    char buf[100];
    snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
    result += buf;
    if (f > 0) {
      last_non_zero_offset = result.size();
    }
  }
  result.resize(last_non_zero_offset);
  return result;
}

std::vector<uint64_t> DBTestBase::GetBlobFileNumbers() {
  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const current = cfd->current();
  assert(current);

  const VersionStorageInfo* const storage_info = current->storage_info();
  assert(storage_info);

  const auto& blob_files = storage_info->GetBlobFiles();

  std::vector<uint64_t> result;
  result.reserve(blob_files.size());

  for (const auto& blob_file : blob_files) {
    assert(blob_file);
    result.emplace_back(blob_file->GetBlobFileNumber());
  }

  return result;
}

size_t DBTestBase::CountFiles() {
  size_t count = 0;
  std::vector<std::string> files;
  if (env_->GetChildren(dbname_, &files).ok()) {
    count += files.size();
  }

  if (dbname_ != last_options_.wal_dir) {
    if (env_->GetChildren(last_options_.wal_dir, &files).ok()) {
      count += files.size();
    }
  }

  return count;
};

Status DBTestBase::CountFiles(size_t* count) {
  std::vector<std::string> files;
  Status s = env_->GetChildren(dbname_, &files);
  if (!s.ok()) {
    return s;
  }
  size_t files_count = files.size();

  if (dbname_ != last_options_.wal_dir) {
    s = env_->GetChildren(last_options_.wal_dir, &files);
    if (!s.ok()) {
      return s;
    }
    *count = files_count + files.size();
  }

  return Status::OK();
}

std::vector<FileMetaData*> DBTestBase::GetLevelFileMetadatas(int level,
                                                             int cf) {
  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);
  ColumnFamilyData* const cfd =
      versions->GetColumnFamilySet()->GetColumnFamily(cf);
  assert(cfd);
  Version* const current = cfd->current();
  assert(current);
  VersionStorageInfo* const storage_info = current->storage_info();
  assert(storage_info);
  return storage_info->LevelFiles(level);
}

Status DBTestBase::Size(const Slice& start, const Slice& limit, int cf,
                        uint64_t* size) {
  Range r(start, limit);
  if (cf == 0) {
    return db_->GetApproximateSizes(&r, 1, size);
  } else {
    return db_->GetApproximateSizes(handles_[1], &r, 1, size);
  }
}

void DBTestBase::Compact(int cf, const Slice& start, const Slice& limit,
                         uint32_t target_path_id) {
  CompactRangeOptions compact_options;
  compact_options.target_path_id = target_path_id;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[cf], &start, &limit));
}

void DBTestBase::Compact(int cf, const Slice& start, const Slice& limit) {
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[cf], &start, &limit));
}

void DBTestBase::Compact(const Slice& start, const Slice& limit) {
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &limit));
}

// Do n memtable compactions, each of which produces an sstable
// covering the range [small,large].
void DBTestBase::MakeTables(int n, const std::string& small,
                            const std::string& large, int cf) {
  for (int i = 0; i < n; i++) {
    ASSERT_OK(Put(cf, small, "begin"));
    ASSERT_OK(Put(cf, large, "end"));
    ASSERT_OK(Flush(cf));
    MoveFilesToLevel(n - i - 1, cf);
  }
}

// Prevent pushing of new sstables into deeper levels by adding
// tables that cover a specified range to all levels.
void DBTestBase::FillLevels(const std::string& smallest,
                            const std::string& largest, int cf) {
  MakeTables(db_->NumberLevels(handles_[cf]), smallest, largest, cf);
}

void DBTestBase::MoveFilesToLevel(int level, int cf) {
  MoveFilesToLevel(level, cf ? handles_[cf] : db_->DefaultColumnFamily());
}

void DBTestBase::MoveFilesToLevel(int level, ColumnFamilyHandle* column_family,
                                  DB* db) {
  DBImpl* db_impl = db ? static_cast<DBImpl*>(db) : dbfull();
  for (int l = 0; l < level; ++l) {
    EXPECT_OK(db_impl->TEST_CompactRange(l, nullptr, nullptr, column_family));
  }
}

void DBTestBase::DumpFileCounts(const char* label) {
  fprintf(stderr, "---\n%s:\n", label);
  fprintf(stderr, "maxoverlap: %" PRIu64 "\n",
          dbfull()->TEST_MaxNextLevelOverlappingBytes());
  for (int level = 0; level < db_->NumberLevels(); level++) {
    int num = NumTableFilesAtLevel(level);
    if (num > 0) {
      fprintf(stderr, "  level %3d : %d files\n", level, num);
    }
  }
}

std::string DBTestBase::DumpSSTableList() {
  std::string property;
  db_->GetProperty("rocksdb.sstables", &property);
  return property;
}

void DBTestBase::GetSstFiles(Env* env, std::string path,
                             std::vector<std::string>* files) {
  EXPECT_OK(env->GetChildren(path, files));

  files->erase(std::remove_if(files->begin(), files->end(),
                              [](std::string name) {
                                uint64_t number;
                                FileType type;
                                return !(ParseFileName(name, &number, &type) &&
                                         type == kTableFile);
                              }),
               files->end());
}

int DBTestBase::GetSstFileCount(std::string path) {
  std::vector<std::string> files;
  DBTestBase::GetSstFiles(env_, path, &files);
  return static_cast<int>(files.size());
}

// this will generate non-overlapping files since it keeps increasing key_idx
void DBTestBase::GenerateNewFile(int cf, Random* rnd, int* key_idx,
                                 bool nowait) {
  for (int i = 0; i < KNumKeysByGenerateNewFile; i++) {
    ASSERT_OK(Put(cf, Key(*key_idx), rnd->RandomString((i == 99) ? 1 : 990)));
    (*key_idx)++;
  }
  if (!nowait) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
}

// this will generate non-overlapping files since it keeps increasing key_idx
void DBTestBase::GenerateNewFile(Random* rnd, int* key_idx, bool nowait) {
  for (int i = 0; i < KNumKeysByGenerateNewFile; i++) {
    ASSERT_OK(Put(Key(*key_idx), rnd->RandomString((i == 99) ? 1 : 990)));
    (*key_idx)++;
  }
  if (!nowait) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
}

const int DBTestBase::kNumKeysByGenerateNewRandomFile = 51;

void DBTestBase::GenerateNewRandomFile(Random* rnd, bool nowait) {
  for (int i = 0; i < kNumKeysByGenerateNewRandomFile; i++) {
    ASSERT_OK(Put("key" + rnd->RandomString(7), rnd->RandomString(2000)));
  }
  ASSERT_OK(Put("key" + rnd->RandomString(7), rnd->RandomString(200)));
  if (!nowait) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
}

std::string DBTestBase::IterStatus(Iterator* iter) {
  std::string result;
  if (iter->Valid()) {
    result = iter->key().ToString() + "->" + iter->value().ToString();
  } else {
    EXPECT_OK(iter->status());
    result = "(invalid)";
  }
  return result;
}

Options DBTestBase::OptionsForLogIterTest() {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.WAL_ttl_seconds = 1000;
  return options;
}

std::string DBTestBase::DummyString(size_t len, char c) {
  return std::string(len, c);
}

void DBTestBase::VerifyIterLast(std::string expected_key, int cf) {
  Iterator* iter;
  ReadOptions ro;
  if (cf == 0) {
    iter = db_->NewIterator(ro);
  } else {
    iter = db_->NewIterator(ro, handles_[cf]);
  }
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), expected_key);
  delete iter;
}

// Used to test InplaceUpdate

// If previous value is nullptr or delta is > than previous value,
//   sets newValue with delta
// If previous value is not empty,
//   updates previous value with 'b' string of previous value size - 1.
UpdateStatus DBTestBase::updateInPlaceSmallerSize(char* prevValue,
                                                  uint32_t* prevSize,
                                                  Slice delta,
                                                  std::string* newValue) {
  if (prevValue == nullptr) {
    *newValue = std::string(delta.size(), 'c');
    return UpdateStatus::UPDATED;
  } else {
    *prevSize = *prevSize - 1;
    std::string str_b = std::string(*prevSize, 'b');
    memcpy(prevValue, str_b.c_str(), str_b.size());
    return UpdateStatus::UPDATED_INPLACE;
  }
}

UpdateStatus DBTestBase::updateInPlaceSmallerVarintSize(char* prevValue,
                                                        uint32_t* prevSize,
                                                        Slice delta,
                                                        std::string* newValue) {
  if (prevValue == nullptr) {
    *newValue = std::string(delta.size(), 'c');
    return UpdateStatus::UPDATED;
  } else {
    *prevSize = 1;
    std::string str_b = std::string(*prevSize, 'b');
    memcpy(prevValue, str_b.c_str(), str_b.size());
    return UpdateStatus::UPDATED_INPLACE;
  }
}

UpdateStatus DBTestBase::updateInPlaceLargerSize(char* /*prevValue*/,
                                                 uint32_t* /*prevSize*/,
                                                 Slice delta,
                                                 std::string* newValue) {
  *newValue = std::string(delta.size(), 'c');
  return UpdateStatus::UPDATED;
}

UpdateStatus DBTestBase::updateInPlaceNoAction(char* /*prevValue*/,
                                               uint32_t* /*prevSize*/,
                                               Slice /*delta*/,
                                               std::string* /*newValue*/) {
  return UpdateStatus::UPDATE_FAILED;
}

// Utility method to test InplaceUpdate
void DBTestBase::validateNumberOfEntries(int numValues, int cf) {
  Arena arena;
  auto options = CurrentOptions();
  InternalKeyComparator icmp(options.comparator);
  ReadOptions read_options;
  ScopedArenaPtr<InternalIterator> iter;
  if (cf != 0) {
    iter.reset(dbfull()->NewInternalIterator(read_options, &arena,
                                             kMaxSequenceNumber, handles_[cf]));
  } else {
    iter.reset(dbfull()->NewInternalIterator(read_options, &arena,
                                             kMaxSequenceNumber));
  }
  iter->SeekToFirst();
  ASSERT_OK(iter->status());
  int seq = numValues;
  while (iter->Valid()) {
    ParsedInternalKey ikey;
    ikey.clear();
    ASSERT_OK(ParseInternalKey(iter->key(), &ikey, true /* log_err_key */));

    // checks sequence number for updates
    ASSERT_EQ(ikey.sequence, (unsigned)seq--);
    iter->Next();
  }
  ASSERT_EQ(0, seq);
}

void DBTestBase::CopyFile(const std::string& source,
                          const std::string& destination, uint64_t size) {
  const EnvOptions soptions;
  std::unique_ptr<SequentialFile> srcfile;
  ASSERT_OK(env_->NewSequentialFile(source, &srcfile, soptions));
  std::unique_ptr<WritableFile> destfile;
  ASSERT_OK(env_->NewWritableFile(destination, &destfile, soptions));

  if (size == 0) {
    // default argument means copy everything
    ASSERT_OK(env_->GetFileSize(source, &size));
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    uint64_t one = std::min(uint64_t(sizeof(buffer)), size);
    ASSERT_OK(srcfile->Read(one, &slice, buffer));
    ASSERT_OK(destfile->Append(slice));
    size -= slice.size();
  }
  ASSERT_OK(destfile->Close());
}

Status DBTestBase::GetAllDataFiles(
    const FileType file_type, std::unordered_map<std::string, uint64_t>* files,
    uint64_t* total_size /* = nullptr */) {
  if (total_size) {
    *total_size = 0;
  }
  std::vector<std::string> children;
  Status s = env_->GetChildren(dbname_, &children);
  if (s.ok()) {
    for (auto& file_name : children) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file_name, &number, &type) && type == file_type) {
        std::string file_path = dbname_ + "/" + file_name;
        uint64_t file_size = 0;
        s = env_->GetFileSize(file_path, &file_size);
        if (!s.ok()) {
          break;
        }
        (*files)[file_path] = file_size;
        if (total_size) {
          *total_size += file_size;
        }
      }
    }
  }
  return s;
}

std::vector<std::uint64_t> DBTestBase::ListTableFiles(Env* env,
                                                      const std::string& path) {
  std::vector<std::string> files;
  std::vector<uint64_t> file_numbers;
  EXPECT_OK(env->GetChildren(path, &files));
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < files.size(); ++i) {
    if (ParseFileName(files[i], &number, &type)) {
      if (type == kTableFile) {
        file_numbers.push_back(number);
      }
    }
  }
  return file_numbers;
}

void DBTestBase::VerifyDBFromMap(
    std::map<std::string, std::string> true_data, size_t* total_reads_res,
    bool tailing_iter, ReadOptions* ro, ColumnFamilyHandle* cf,
    std::unordered_set<std::string>* not_found) const {
  ReadOptions temp_ro;
  if (!ro) {
    ro = &temp_ro;
    ro->verify_checksums = true;
  }
  if (!cf) {
    cf = db_->DefaultColumnFamily();
  }

  // Get
  size_t total_reads = 0;
  std::string result;
  for (auto& [k, v] : true_data) {
    ASSERT_OK(db_->Get(*ro, cf, k, &result)) << "key is " << k;
    ASSERT_EQ(v, result);
    total_reads++;
  }
  if (not_found) {
    for (const auto& k : *not_found) {
      ASSERT_TRUE(db_->Get(*ro, cf, k, &result).IsNotFound())
          << "key is " << k << " val is " << result;
    }
  }

  // MultiGet
  std::vector<Slice> key_slice;
  for (const auto& [k, _] : true_data) {
    key_slice.emplace_back(k);
  }
  std::vector<std::string> values;
  std::vector<ColumnFamilyHandle*> cfs(key_slice.size(), cf);
  std::vector<Status> status = db_->MultiGet(*ro, cfs, key_slice, &values);
  total_reads += key_slice.size();
  auto data_iter = true_data.begin();
  for (size_t i = 0; i < key_slice.size(); ++i, ++data_iter) {
    ASSERT_OK(status[i]);
    ASSERT_EQ(values[i], data_iter->second);
  }
  // MultiGet - not found
  if (not_found) {
    key_slice.clear();
    for (const auto& k : *not_found) {
      key_slice.emplace_back(k);
    }
    cfs = std::vector<ColumnFamilyHandle*>(key_slice.size(), cf);
    values.clear();
    status = db_->MultiGet(*ro, cfs, key_slice, &values);
    for (const auto& s : status) {
      ASSERT_TRUE(s.IsNotFound());
    }
  }

  // Normal Iterator
  {
    int iter_cnt = 0;
    ReadOptions ro_ = *ro;
    ro_.total_order_seek = true;
    Iterator* iter = db_->NewIterator(ro_, cf);
    // Verify Iterator::Next()
    iter_cnt = 0;
    data_iter = true_data.begin();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++data_iter) {
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);
      iter_cnt++;
      total_reads++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(data_iter, true_data.end())
        << iter_cnt << " / " << true_data.size();
    delete iter;

    // Verify Iterator::Prev()
    // Use a new iterator to make sure its status is clean.
    iter = db_->NewIterator(ro_, cf);
    iter_cnt = 0;
    auto data_rev = true_data.rbegin();
    for (iter->SeekToLast(); iter->Valid(); iter->Prev(), data_rev++) {
      ASSERT_EQ(iter->key().ToString(), data_rev->first);
      ASSERT_EQ(iter->value().ToString(), data_rev->second);
      iter_cnt++;
      total_reads++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(data_rev, true_data.rend())
        << iter_cnt << " / " << true_data.size();

    // Verify Iterator::Seek() and SeekForPrev()
    for (const auto& [k, v] : true_data) {
      for (bool prev : {false, true}) {
        if (prev) {
          iter->SeekForPrev(k);
        } else {
          iter->Seek(k);
        }
        ASSERT_TRUE(iter->Valid());
        ASSERT_OK(iter->status());
        ASSERT_EQ(iter->key(), k);
        ASSERT_EQ(iter->value(), v);
        ++total_reads;
      }
    }
    delete iter;
  }

  if (tailing_iter) {
    // Tailing iterator
    int iter_cnt = 0;
    ReadOptions ro_ = *ro;
    ro_.tailing = true;
    ro_.total_order_seek = true;
    Iterator* iter = db_->NewIterator(ro_, cf);

    // Verify ForwardIterator::Next()
    iter_cnt = 0;
    data_iter = true_data.begin();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), data_iter++) {
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);
      iter_cnt++;
      total_reads++;
    }
    ASSERT_EQ(data_iter, true_data.end())
        << iter_cnt << " / " << true_data.size();

    // Verify ForwardIterator::Seek()
    for (const auto& kv : true_data) {
      iter->Seek(kv.first);
      ASSERT_EQ(kv.first, iter->key().ToString());
      ASSERT_EQ(kv.second, iter->value().ToString());
      total_reads++;
    }

    delete iter;
  }

  if (total_reads_res) {
    *total_reads_res = total_reads;
  }
}

void DBTestBase::VerifyDBInternal(
    std::vector<std::pair<std::string, std::string>> true_data) {
  Arena arena;
  InternalKeyComparator icmp(last_options_.comparator);
  ReadOptions read_options;
  auto iter =
      dbfull()->NewInternalIterator(read_options, &arena, kMaxSequenceNumber);
  iter->SeekToFirst();
  for (const auto& p : true_data) {
    ASSERT_TRUE(iter->Valid());
    ParsedInternalKey ikey;
    ASSERT_OK(ParseInternalKey(iter->key(), &ikey, true /* log_err_key */));
    ASSERT_EQ(p.first, ikey.user_key);
    ASSERT_EQ(p.second, iter->value());
    iter->Next();
  };
  ASSERT_FALSE(iter->Valid());
  iter->~InternalIterator();
}

uint64_t DBTestBase::GetNumberOfSstFilesForColumnFamily(
    DB* db, std::string column_family_name) {
  std::vector<LiveFileMetaData> metadata;
  db->GetLiveFilesMetaData(&metadata);
  uint64_t result = 0;
  for (auto& fileMetadata : metadata) {
    result += (fileMetadata.column_family_name == column_family_name);
  }
  return result;
}

uint64_t DBTestBase::GetSstSizeHelper(Temperature temperature) {
  std::string prop;
  EXPECT_TRUE(dbfull()->GetProperty(
      DB::Properties::kLiveSstFilesSizeAtTemperature +
          std::to_string(static_cast<uint8_t>(temperature)),
      &prop));
  return static_cast<uint64_t>(std::atoi(prop.c_str()));
}

void VerifySstUniqueIds(const TablePropertiesCollection& props) {
  ASSERT_FALSE(props.empty());  // suspicious test if empty
  std::unordered_set<std::string> seen;
  for (auto& pair : props) {
    std::string id;
    ASSERT_OK(GetUniqueIdFromTableProperties(*pair.second, &id));
    ASSERT_TRUE(seen.insert(id).second);
  }
}

template <CacheEntryRole R>
TargetCacheChargeTrackingCache<R>::TargetCacheChargeTrackingCache(
    std::shared_ptr<Cache> target)
    : CacheWrapper(std::move(target)),
      cur_cache_charge_(0),
      cache_charge_peak_(0),
      cache_charge_increment_(0),
      last_peak_tracked_(false),
      cache_charge_increments_sum_(0) {}

template <CacheEntryRole R>
Status TargetCacheChargeTrackingCache<R>::Insert(
    const Slice& key, ObjectPtr value, const CacheItemHelper* helper,
    size_t charge, Handle** handle, Priority priority, const Slice& compressed,
    CompressionType type) {
  Status s = target_->Insert(key, value, helper, charge, handle, priority,
                             compressed, type);
  if (helper == kCrmHelper) {
    if (last_peak_tracked_) {
      cache_charge_peak_ = 0;
      cache_charge_increment_ = 0;
      last_peak_tracked_ = false;
    }
    if (s.ok()) {
      cur_cache_charge_ += charge;
    }
    cache_charge_peak_ = std::max(cache_charge_peak_, cur_cache_charge_);
    cache_charge_increment_ += charge;
  }

  return s;
}

template <CacheEntryRole R>
bool TargetCacheChargeTrackingCache<R>::Release(Handle* handle,
                                                bool erase_if_last_ref) {
  auto helper = GetCacheItemHelper(handle);
  if (helper == kCrmHelper) {
    if (!last_peak_tracked_) {
      cache_charge_peaks_.push_back(cache_charge_peak_);
      cache_charge_increments_sum_ += cache_charge_increment_;
      last_peak_tracked_ = true;
    }
    cur_cache_charge_ -= GetCharge(handle);
  }
  bool is_successful = target_->Release(handle, erase_if_last_ref);
  return is_successful;
}

template <CacheEntryRole R>
const Cache::CacheItemHelper* TargetCacheChargeTrackingCache<R>::kCrmHelper =
    CacheReservationManagerImpl<R>::TEST_GetCacheItemHelperForRole();

template class TargetCacheChargeTrackingCache<
    CacheEntryRole::kFilterConstruction>;
template class TargetCacheChargeTrackingCache<
    CacheEntryRole::kBlockBasedTableReader>;
template class TargetCacheChargeTrackingCache<CacheEntryRole::kFileMetadata>;

const std::vector<Temperature> kKnownTemperatures = {
    Temperature::kHot, Temperature::kWarm, Temperature::kCool,
    Temperature::kCold, Temperature::kIce};

Temperature RandomKnownTemperature() {
  return kKnownTemperatures[Random::GetTLSInstance()->Uniform(
      static_cast<int>(kKnownTemperatures.size()))];
}

}  // namespace ROCKSDB_NAMESPACE
