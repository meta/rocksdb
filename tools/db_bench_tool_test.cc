//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db_bench_tool.h"

#include "db/db_impl/db_impl.h"
#include "options/options_parser.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"

#ifdef GFLAGS
#include "util/gflags_compat.h"

namespace ROCKSDB_NAMESPACE {
namespace {
static const int kMaxArgCount = 100;
static const size_t kArgBufferSize = 100000;
}  // namespace

class DBBenchTest : public testing::Test {
 public:
  DBBenchTest() : rnd_(0xFB) {
    test_path_ = test::PerThreadDBPath("db_bench_test");
    Env::Default()->CreateDir(test_path_);
    db_path_ = test_path_ + "/db";
    wal_path_ = test_path_ + "/wal";
  }

  ~DBBenchTest() {
    //  DestroyDB(db_path_, Options());
  }

  void ResetArgs() {
    argc_ = 0;
    cursor_ = 0;
    memset(arg_buffer_, 0, kArgBufferSize);
  }

  void AppendArgs(const std::vector<std::string>& args) {
    for (const auto& arg : args) {
      ASSERT_LE(cursor_ + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc_ + 1, kMaxArgCount);
      snprintf(arg_buffer_ + cursor_, arg.size() + 1, "%s", arg.c_str());

      argv_[argc_++] = arg_buffer_ + cursor_;
      cursor_ += arg.size() + 1;
    }
  }

  // Gets the default options for this test/db_bench.
  // Note that db_bench may change some of the default option values and that
  // the database might as well.  The options changed by db_bench are
  // specified here; the ones by the DB are set via SanitizeOptions
  Options GetDefaultOptions(CompactionStyle style = kCompactionStyleLevel,
                            int levels = 7) const {
    Options opt;

    opt.create_if_missing = true;
    opt.max_open_files = 256;
    opt.max_background_compactions = 10;
    opt.dump_malloc_stats = true;  // db_bench uses a different default
    opt.compaction_style = style;
    opt.num_levels = levels;
    opt.compression = kNoCompression;
    opt.arena_block_size = 8388608;

    return SanitizeOptions(db_path_, opt);
  }

  void RunDbBench(const std::string& options_file_name,
                  const std::vector<std::string>& extra_args = {}) {
    AppendArgs({"./db_bench", "--benchmarks=fillseq", "--use_existing_db=0",
                "--num=1000", "--compression_type=none",
                std::string(std::string("--db=") + db_path_).c_str(),
                std::string(std::string("--wal_dir=") + wal_path_).c_str(),
                std::string(std::string("--options_file=") + options_file_name)
                    .c_str()});
    AppendArgs(extra_args);
    ASSERT_EQ(0, db_bench_tool(argc(), argv()));
  }

  // Every flag is passed explicitly because gflags state persists across
  // db_bench_tool() calls within the same test process.
  void RunIngestBench(int batch_size, int num_batches, int file_opening_threads,
                      bool use_file_info, bool fill_cache) {
    ResetArgs();
    AppendArgs(
        {"./db_bench", "--benchmarks=ingestexternalfile", "--use_existing_db=0",
         "--num=2000", "--compression_type=none", "--db=" + db_path_,
         "--wal_dir=" + wal_path_,
         "--ingest_external_file_batch_size=" + std::to_string(batch_size),
         "--ingest_external_file_num_batches=" + std::to_string(num_batches),
         "--ingest_external_file_file_opening_threads=" +
             std::to_string(file_opening_threads),
         "--ingest_external_file_use_file_info=" +
             std::string(use_file_info ? "true" : "false"),
         "--ingest_external_file_fill_cache=" +
             std::string(fill_cache ? "true" : "false")});
    ASSERT_EQ(0, db_bench_tool(argc(), argv()));
  }

  void LoadPersistedOptions(
      DBOptions* loaded_db_opts,
      std::vector<ColumnFamilyDescriptor>* cf_descs) const {
    ConfigOptions config_opts;
    config_opts.ignore_unknown_options = false;
    config_opts.input_strings_escaped = true;
    config_opts.env = Env::Default();
    ASSERT_OK(
        LoadLatestOptions(config_opts, db_path_, loaded_db_opts, cf_descs));
  }

  void VerifyOptions(const Options& opt) {
    DBOptions loaded_db_opts;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    LoadPersistedOptions(&loaded_db_opts, &cf_descs);

    ConfigOptions exact;
    exact.input_strings_escaped = false;
    exact.sanity_level = ConfigOptions::kSanityLevelExactMatch;
    ASSERT_OK(RocksDBOptionsParser::VerifyDBOptions(exact, DBOptions(opt),
                                                    loaded_db_opts));
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(
        exact, ColumnFamilyOptions(opt), cf_descs[0].options));

    // check with the default rocksdb options and expect failure
    ASSERT_NOK(RocksDBOptionsParser::VerifyDBOptions(exact, DBOptions(),
                                                     loaded_db_opts));
    ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(
        exact, ColumnFamilyOptions(), cf_descs[0].options));
  }

  char** argv() { return argv_; }

  int argc() { return argc_; }

  std::string db_path_;
  std::string test_path_;
  std::string wal_path_;

  char arg_buffer_[kArgBufferSize];
  char* argv_[kMaxArgCount];
  int argc_ = 0;
  int cursor_ = 0;
  Random rnd_;
};

namespace {}  // namespace

TEST_F(DBBenchTest, OptionsFile) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";
  Options opt = GetDefaultOptions();
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  opt.env->GetFileSystem().get()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);
  opt.delayed_write_rate = 16 * 1024 * 1024;  // Set by SanitizeOptions

  VerifyOptions(opt);
}

TEST_F(DBBenchTest, OptionsFileDisableAutoCompactionsOverride) {
  GFLAGS_NAMESPACE::FlagSaver flag_saver;
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";
  Options opt = GetDefaultOptions();
  opt.disable_auto_compactions = false;
  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  opt.env->GetFileSystem().get()));

  opt.wal_dir = wal_path_;
  RunDbBench(kOptionsFileName, {"--disable_auto_compactions=true"});

  DBOptions loaded_db_opts;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  LoadPersistedOptions(&loaded_db_opts, &cf_descs);
  ASSERT_TRUE(cf_descs[0].options.disable_auto_compactions);
}

TEST_F(DBBenchTest, CompactAllParallelMultiDb) {
  GFLAGS_NAMESPACE::FlagSaver flag_saver;
  ResetArgs();
  AppendArgs({"./db_bench", "--benchmarks=fillseq,compactall",
              "--use_existing_db=0", "--num=1000", "--threads=4",
              "--num_multi_db=4", "--compact_all_parallelism=2",
              "--subcompactions=2", "--disable_auto_compactions=true",
              "--compression_type=none", "--db=" + db_path_,
              "--wal_dir=" + wal_path_});

  ASSERT_EQ(0, db_bench_tool(argc(), argv()));
}

TEST_F(DBBenchTest, IngestExternalFile) {
  // Exercise the ingestexternalfile benchmark with both serial and parallel
  // (file_opening_threads > 1) commit-time table-reader opening.
  for (int file_opening_threads : {1, 4}) {
    RunIngestBench(/*batch_size=*/3, /*num_batches=*/2, file_opening_threads,
                   /*use_file_info=*/false, /*fill_cache=*/true);
  }
}

TEST_F(DBBenchTest, IngestExternalFileWithFileInfo) {
  RunIngestBench(/*batch_size=*/3, /*num_batches=*/2,
                 /*file_opening_threads=*/4,
                 /*use_file_info=*/true, /*fill_cache=*/true);
}

TEST_F(DBBenchTest, IngestExternalFileWithoutFillCache) {
  RunIngestBench(/*batch_size=*/3, /*num_batches=*/2,
                 /*file_opening_threads=*/1,
                 /*use_file_info=*/false, /*fill_cache=*/false);
}

TEST_F(DBBenchTest, ReportFileKeepsAllBenchmarkRuns) {
  // Each run lasts 2s, so secs_elapsed reaches 3 only if both runs share
  // the report.
  GFLAGS_NAMESPACE::FlagSaver flag_saver;
  const std::string report_file = test_path_ + "/report.csv";
  ResetArgs();
  AppendArgs({"./db_bench", "--benchmarks=fillrandom,readrandom",
              "--use_existing_db=0", "--num=100000000", "--value_size=10",
              "--duration=2", "--report_interval_seconds=1",
              "--report_file=" + report_file, "--compression_type=none",
              "--db=" + db_path_, "--wal_dir=" + wal_path_});
  ASSERT_EQ(0, db_bench_tool(argc(), argv()));

  std::string contents;
  ASSERT_OK(ReadFileToString(Env::Default(), report_file, &contents));
  SCOPED_TRACE("report:\n" + contents);
  std::vector<std::string> lines = StringSplit(contents, '\n');
  ASSERT_GE(lines.size(), 2U);
  ASSERT_EQ("secs_elapsed,interval_qps", lines[0]);
  std::vector<uint64_t> secs_elapsed;
  for (size_t i = 1; i < lines.size(); ++i) {
    std::vector<std::string> fields = StringSplit(lines[i], ',');
    ASSERT_EQ(2U, fields.size());
    // Also rejects a repeated header
    ASSERT_FALSE(fields[0].empty());
    ASSERT_EQ(std::string::npos, fields[0].find_first_not_of("0123456789"));
    secs_elapsed.push_back(ParseUint64(fields[0]));
  }
  ASSERT_EQ(1U, secs_elapsed.front());
  for (size_t i = 1; i < secs_elapsed.size(); ++i) {
    ASSERT_LT(secs_elapsed[i - 1], secs_elapsed[i]);
  }
  ASSERT_GE(secs_elapsed.back(), 3U);
}

TEST_F(DBBenchTest, OptionsFileUniversal) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";

  Options opt = GetDefaultOptions(kCompactionStyleUniversal, 1);

  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  opt.env->GetFileSystem().get()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;
  RunDbBench(kOptionsFileName);

  VerifyOptions(opt);
}

TEST_F(DBBenchTest, OptionsFileMultiLevelUniversal) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";

  Options opt = GetDefaultOptions(kCompactionStyleUniversal, 12);

  ASSERT_OK(PersistRocksDBOptions(WriteOptions(), DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  opt.env->GetFileSystem().get()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);
  VerifyOptions(opt);
}

const std::string options_file_content = R"OPTIONS_FILE(
[Version]
  rocksdb_version=4.3.1
  options_file_version=1.1

[DBOptions]
  wal_bytes_per_sync=1048576
  delete_obsolete_files_period_micros=0
  WAL_ttl_seconds=0
  WAL_size_limit_MB=0
  db_write_buffer_size=0
  max_subcompactions=1
  table_cache_numshardbits=4
  max_open_files=-1
  max_file_opening_threads=10
  max_background_compactions=5
  use_fsync=false
  use_adaptive_mutex=false
  max_total_wal_size=18446744073709551615
  compaction_readahead_size=0
  keep_log_file_num=10
  skip_stats_update_on_db_open=false
  max_manifest_file_size=18446744073709551615
  db_log_dir=
  writable_file_max_buffer_size=1048576
  paranoid_checks=true
  is_fd_close_on_exec=true
  bytes_per_sync=1048576
  enable_thread_tracking=true
  recycle_log_file_num=0
  create_missing_column_families=false
  log_file_time_to_roll=0
  max_background_flushes=1
  create_if_missing=true
  error_if_exists=false
  delayed_write_rate=1048576
  manifest_preallocation_size=4194304
  allow_mmap_reads=false
  allow_mmap_writes=false
  use_direct_reads=false
  use_direct_io_for_flush_and_compaction=false
  stats_dump_period_sec=600
  allow_fallocate=true
  max_log_file_size=83886080
  advise_random_on_open=true
  dump_malloc_stats=true

[CFOptions "default"]
  compaction_filter_factory=nullptr
  table_factory=BlockBasedTable
  prefix_extractor=nullptr
  comparator=leveldb.BytewiseComparator
  compression_per_level=
  max_bytes_for_level_base=104857600
  bloom_locality=0
  target_file_size_base=10485760
  memtable_huge_page_size=0
  max_successive_merges=1000
  max_sequential_skip_in_iterations=8
  arena_block_size=52428800
  target_file_size_multiplier=1
  source_compaction_factor=1
  min_write_buffer_number_to_merge=1
  max_write_buffer_number=2
  write_buffer_size=419430400
  max_grandparent_overlap_factor=10
  max_bytes_for_level_multiplier=10
  memtable_factory=SkipListFactory
  compression=kNoCompression
  min_partial_merge_operands=2
  level0_stop_writes_trigger=100
  num_levels=1
  level0_slowdown_writes_trigger=50
  level0_file_num_compaction_trigger=10
  expanded_compaction_factor=25
  max_write_buffer_size_to_maintain=0
  verify_checksums_in_compaction=true
  merge_operator=nullptr
  memtable_prefix_bloom_bits=0
  memtable_whole_key_filtering=true
  paranoid_file_checks=false
  inplace_update_num_locks=10000
  optimize_filters_for_hits=false
  level_compaction_dynamic_level_bytes=false
  inplace_update_support=false
  compaction_style=kCompactionStyleUniversal
  memtable_prefix_bloom_probes=6
  filter_deletes=false
  hard_pending_compaction_bytes_limit=0
  disable_auto_compactions=false
  compaction_measure_io_stats=false
  enable_blob_files=true
  min_blob_size=16
  blob_file_size=10485760
  blob_compression_type=kNoCompression
  enable_blob_garbage_collection=true
  blob_garbage_collection_age_cutoff=0.5
  blob_garbage_collection_force_threshold=0.75
  blob_compaction_readahead_size=262144
  blob_file_starting_level=0
  prepopulate_blob_cache=kDisable;

[TableOptions/BlockBasedTable "default"]
  format_version=0
  skip_table_builder_flush=false
  cache_index_and_filter_blocks=false
  flush_block_policy_factory=FlushBlockBySizePolicyFactory
  index_type=kBinarySearch
  whole_key_filtering=true
  checksum=kCRC32c
  no_block_cache=false
  block_size=32768
  block_size_deviation=10
  block_restart_interval=16
  filter_policy=rocksdb.BuiltinBloomFilter
)OPTIONS_FILE";

TEST_F(DBBenchTest, OptionsFileFromFile) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_flash";
  std::unique_ptr<WritableFile> writable;
  ASSERT_OK(Env::Default()->NewWritableFile(kOptionsFileName, &writable,
                                            EnvOptions()));
  ASSERT_OK(writable->Append(options_file_content));
  ASSERT_OK(writable->Close());

  DBOptions db_opt;
  ConfigOptions config_opt;
  config_opt.ignore_unknown_options = false;
  config_opt.input_strings_escaped = true;
  config_opt.env = Env::Default();
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ASSERT_OK(
      LoadOptionsFromFile(config_opt, kOptionsFileName, &db_opt, &cf_descs));
  Options opt(db_opt, cf_descs[0].options);
  opt.create_if_missing = true;

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);

  VerifyOptions(SanitizeOptions(db_path_, opt));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

#else

int main(int argc, char** argv) {
  printf("Skip db_bench_tool_test as the required library GFLAG is missing.");
}
#endif  // #ifdef GFLAGS
