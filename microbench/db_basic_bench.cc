//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef OS_WIN
#include <unistd.h>
#endif  // ! OS_WIN

#include "benchmark/benchmark.h"
#include "db/db_impl/db_impl.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "table/block_based/block.h"
#include "table/block_based/block_builder.h"
#include "util/random.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class KeyGenerator {
 public:
  // Generate next key
  // buff: the caller needs to make sure there's enough space for generated key
  // offset: to control the group of the key, 0 means normal key, 1 means
  // non-existing key, 2 is reserved prefix_only: only return a prefix
  Slice Next(char* buff, int8_t offset = 0, bool prefix_only = false) {
    assert(max_key_ < std::numeric_limits<uint32_t>::max() /
                          MULTIPLIER);  // TODO: add large key support

    uint32_t k;
    if (is_sequential_) {
      assert(next_sequential_key_ < max_key_);
      k = (next_sequential_key_ % max_key_) * MULTIPLIER + offset;
      if (next_sequential_key_ + 1 == max_key_) {
        next_sequential_key_ = 0;
      } else {
        next_sequential_key_++;
      }
    } else {
      k = (rnd_->Next() % max_key_) * MULTIPLIER + offset;
    }
    // TODO: make sure the buff is large enough
    memset(buff, 0, key_size_);
    if (prefix_num_ > 0) {
      uint32_t prefix = (k % prefix_num_) * MULTIPLIER + offset;
      Encode(buff, prefix);
      if (prefix_only) {
        return {buff, prefix_size_};
      }
    }
    Encode(buff + prefix_size_, k);
    return {buff, key_size_};
  }

  // use internal buffer for generated key, make sure there's only one caller in
  // single thread
  Slice Next() { return Next(buff_); }

  // user internal buffer for generated prefix
  Slice NextPrefix() {
    assert(prefix_num_ > 0);
    return Next(buff_, 0, true);
  }

  // helper function to get non exist key
  Slice NextNonExist() { return Next(buff_, 1); }

  Slice MaxKey(char* buff) const {
    memset(buff, 0xff, key_size_);
    return {buff, key_size_};
  }

  Slice MinKey(char* buff) const {
    memset(buff, 0, key_size_);
    return {buff, key_size_};
  }

  // max_key: the max key that it could generate
  // prefix_num: the max prefix number
  // key_size: in bytes
  explicit KeyGenerator(Random* rnd, uint64_t max_key = 100 * 1024 * 1024,
                        size_t prefix_num = 0, size_t key_size = 10) {
    prefix_num_ = prefix_num;
    key_size_ = key_size;
    max_key_ = max_key;
    rnd_ = rnd;
    if (prefix_num > 0) {
      prefix_size_ = 4;  // TODO: support different prefix_size
    }
  }

  // generate sequential keys
  explicit KeyGenerator(uint64_t max_key = 100 * 1024 * 1024,
                        size_t key_size = 10) {
    key_size_ = key_size;
    max_key_ = max_key;
    rnd_ = nullptr;
    is_sequential_ = true;
  }

 private:
  Random* rnd_;
  size_t prefix_num_ = 0;
  size_t prefix_size_ = 0;
  size_t key_size_;
  uint64_t max_key_;
  bool is_sequential_ = false;
  uint32_t next_sequential_key_ = 0;
  char buff_[256] = {0};
  const int MULTIPLIER = 3;

  void static Encode(char* buf, uint32_t value) {
    if (port::kLittleEndian) {
      buf[0] = static_cast<char>((value >> 24) & 0xff);
      buf[1] = static_cast<char>((value >> 16) & 0xff);
      buf[2] = static_cast<char>((value >> 8) & 0xff);
      buf[3] = static_cast<char>(value & 0xff);
    } else {
      memcpy(buf, &value, sizeof(value));
    }
  }
};

static void SetupDB(benchmark::State& state, Options& options,
                    std::unique_ptr<DB>* db,
                    const std::string& test_name = "") {
  options.create_if_missing = true;
  auto env = Env::Default();
  std::string db_path;
  Status s = env->GetTestDirectory(&db_path);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }
  std::string db_name =
      db_path + kFilePathSeparator + test_name + std::to_string(getpid());
  DestroyDB(db_name, options);

  s = DB::Open(options, db_name, db);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }
}

static void TeardownDB(benchmark::State& state, const std::unique_ptr<DB>& db,
                       const Options& options, KeyGenerator& kg) {
  char min_buff[256];
  char max_buff[256];
  const Range r(kg.MinKey(min_buff), kg.MaxKey(max_buff));
  uint64_t size;
  Status s = db->GetApproximateSizes(&r, 1, &size);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
  }
  state.counters["db_size"] = static_cast<double>(size);

  std::string db_name = db->GetName();
  s = db->Close();
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
  }
  DestroyDB(db_name, options);
}

static void DBOpen(benchmark::State& state) {
  // create DB
  std::unique_ptr<DB> db;
  Options options;
  SetupDB(state, options, &db, "DBOpen");

  std::string db_name = db->GetName();
  db->Close();

  options.create_if_missing = false;

  auto rnd = Random(123);

  for ([[maybe_unused]] auto _ : state) {
    {
      Status s = DB::Open(options, db_name, &db);
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }
    state.PauseTiming();
    auto wo = WriteOptions();
    Status s;
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 100; j++) {
        s = db->Put(wo, rnd.RandomString(10), rnd.RandomString(100));
        if (!s.ok()) {
          state.SkipWithError(s.ToString().c_str());
        }
      }
      s = db->Flush(FlushOptions());
    }
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    s = db->Close();
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    state.ResumeTiming();
  }
  DestroyDB(db_name, options);
}

BENCHMARK(DBOpen)->Iterations(200);  // specify iteration number as the db size
                                     // is impacted by iteration number

static void DBClose(benchmark::State& state) {
  // create DB
  std::unique_ptr<DB> db;
  Options options;
  SetupDB(state, options, &db, "DBClose");

  std::string db_name = db->GetName();
  db->Close();

  options.create_if_missing = false;

  auto rnd = Random(12345);

  for ([[maybe_unused]] auto _ : state) {
    state.PauseTiming();
    {
      Status s = DB::Open(options, db_name, &db);
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }
    auto wo = WriteOptions();
    Status s;
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 100; j++) {
        s = db->Put(wo, rnd.RandomString(10), rnd.RandomString(100));
        if (!s.ok()) {
          state.SkipWithError(s.ToString().c_str());
        }
      }
      s = db->Flush(FlushOptions());
    }
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    state.ResumeTiming();
    s = db->Close();
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }
  DestroyDB(db_name, options);
}

BENCHMARK(DBClose)->Iterations(200);  // specify iteration number as the db size
                                      // is impacted by iteration number

static void DBPut(benchmark::State& state) {
  auto compaction_style = static_cast<CompactionStyle>(state.range(0));
  uint64_t max_data = state.range(1);
  uint64_t per_key_size = state.range(2);
  bool enable_statistics = state.range(3);
  bool enable_wal = state.range(4);
  uint64_t key_num = max_data / per_key_size;

  // setup DB
  static std::unique_ptr<DB> db = nullptr;
  Options options;
  if (enable_statistics) {
    options.statistics = CreateDBStatistics();
  }
  options.compaction_style = compaction_style;

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, key_num);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "DBPut");
  }

  auto wo = WriteOptions();
  wo.disableWAL = !enable_wal;

  for ([[maybe_unused]] auto _ : state) {
    state.PauseTiming();
    Slice key = kg.Next();
    std::string val = rnd.RandomString(static_cast<int>(per_key_size));
    state.ResumeTiming();
    Status s = db->Put(wo, key, val);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  if (state.thread_index() == 0) {
    auto db_full = static_cast_with_check<DBImpl>(db.get());
    Status s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
    if (enable_statistics) {
      HistogramData histogram_data;
      options.statistics->histogramData(DB_WRITE, &histogram_data);
      state.counters["put_mean"] = histogram_data.average * std::milli::den;
      state.counters["put_p95"] = histogram_data.percentile95 * std::milli::den;
      state.counters["put_p99"] = histogram_data.percentile99 * std::milli::den;
    }

    TeardownDB(state, db, options, kg);
  }
}

static void DBPutArguments(benchmark::internal::Benchmark* b) {
  for (int comp_style : {kCompactionStyleLevel, kCompactionStyleUniversal,
                         kCompactionStyleFIFO}) {
    for (int64_t max_data : {100l << 30}) {
      for (int64_t per_key_size : {256, 1024}) {
        for (bool enable_statistics : {false, true}) {
          for (bool wal : {false, true}) {
            b->Args(
                {comp_style, max_data, per_key_size, enable_statistics, wal});
          }
        }
      }
    }
  }
  b->ArgNames(
      {"comp_style", "max_data", "per_key_size", "enable_statistics", "wal"});
}

static const uint64_t DBPutNum = 409600l;
BENCHMARK(DBPut)->Threads(1)->Iterations(DBPutNum)->Apply(DBPutArguments);
BENCHMARK(DBPut)->Threads(8)->Iterations(DBPutNum / 8)->Apply(DBPutArguments);

static void ManualCompaction(benchmark::State& state) {
  auto compaction_style = static_cast<CompactionStyle>(state.range(0));
  uint64_t max_data = state.range(1);
  uint64_t per_key_size = state.range(2);
  bool enable_statistics = state.range(3);
  uint64_t key_num = max_data / per_key_size;

  // setup DB
  static std::unique_ptr<DB> db;
  Options options;
  if (enable_statistics) {
    options.statistics = CreateDBStatistics();
  }
  options.compaction_style = compaction_style;
  // No auto compaction
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = (1 << 30);
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.soft_pending_compaction_bytes_limit = 0;
  options.hard_pending_compaction_bytes_limit = 0;

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, key_num);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "ManualCompaction");
  }

  auto wo = WriteOptions();
  wo.disableWAL = true;
  uint64_t flush_mod = key_num / 4;  // at least generate 4 files for compaction
  for (uint64_t i = 0; i < key_num; i++) {
    Status s = db->Put(wo, kg.Next(),
                       rnd.RandomString(static_cast<int>(per_key_size)));
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    if (i + 1 % flush_mod == 0) {
      s = db->Flush(FlushOptions());
    }
  }
  FlushOptions fo;
  Status s = db->Flush(fo);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
  }
  std::vector<LiveFileMetaData> files_meta;
  db->GetLiveFilesMetaData(&files_meta);
  std::vector<std::string> files_before_compact;
  files_before_compact.reserve(files_meta.size());
  for (const LiveFileMetaData& file : files_meta) {
    files_before_compact.emplace_back(file.name);
  }

  SetPerfLevel(kEnableTime);
  get_perf_context()->EnablePerLevelPerfContext();
  get_perf_context()->Reset();
  CompactionOptions co;
  for ([[maybe_unused]] auto _ : state) {
    s = db->CompactFiles(co, files_before_compact, 1);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  if (state.thread_index() == 0) {
    auto db_full = static_cast_with_check<DBImpl>(db.get());
    s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
    if (enable_statistics) {
      HistogramData histogram_data;
      options.statistics->histogramData(COMPACTION_TIME, &histogram_data);
      state.counters["comp_time"] = histogram_data.average;
      options.statistics->histogramData(COMPACTION_CPU_TIME, &histogram_data);
      state.counters["comp_cpu_time"] = histogram_data.average;
      options.statistics->histogramData(COMPACTION_OUTFILE_SYNC_MICROS,
                                        &histogram_data);
      state.counters["comp_outfile_sync"] = histogram_data.average;

      state.counters["comp_read"] = static_cast<double>(
          options.statistics->getTickerCount(COMPACT_READ_BYTES));
      state.counters["comp_write"] = static_cast<double>(
          options.statistics->getTickerCount(COMPACT_WRITE_BYTES));

      state.counters["user_key_comparison_count"] =
          static_cast<double>(get_perf_context()->user_key_comparison_count);
      state.counters["block_read_count"] =
          static_cast<double>(get_perf_context()->block_read_count);
      state.counters["block_read_time"] =
          static_cast<double>(get_perf_context()->block_read_time);
      state.counters["block_read_cpu_time"] =
          static_cast<double>(get_perf_context()->block_read_cpu_time);
      state.counters["block_checksum_time"] =
          static_cast<double>(get_perf_context()->block_checksum_time);
      state.counters["new_table_block_iter_nanos"] =
          static_cast<double>(get_perf_context()->new_table_block_iter_nanos);
      state.counters["new_table_iterator_nanos"] =
          static_cast<double>(get_perf_context()->new_table_iterator_nanos);
      state.counters["find_table_nanos"] =
          static_cast<double>(get_perf_context()->find_table_nanos);
    }

    TeardownDB(state, db, options, kg);
  }
}

static void ManualCompactionArguments(benchmark::internal::Benchmark* b) {
  for (int comp_style : {kCompactionStyleLevel, kCompactionStyleUniversal}) {
    for (int64_t max_data : {32l << 20, 128l << 20}) {
      for (int64_t per_key_size : {256, 1024}) {
        for (bool enable_statistics : {false, true}) {
          b->Args({comp_style, max_data, per_key_size, enable_statistics});
        }
      }
    }
  }
  b->ArgNames({"comp_style", "max_data", "per_key_size", "enable_statistics"});
}

BENCHMARK(ManualCompaction)->Iterations(1)->Apply(ManualCompactionArguments);

static void ManualFlush(benchmark::State& state) {
  uint64_t key_num = state.range(0);
  uint64_t per_key_size = state.range(1);
  bool enable_statistics = true;

  // setup DB
  static std::unique_ptr<DB> db;
  Options options;
  if (enable_statistics) {
    options.statistics = CreateDBStatistics();
  }
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = (1 << 30);
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.soft_pending_compaction_bytes_limit = 0;
  options.hard_pending_compaction_bytes_limit = 0;
  options.write_buffer_size = 2l << 30;  // 2G to avoid auto flush

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, key_num);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "ManualFlush");
  }

  auto wo = WriteOptions();
  for ([[maybe_unused]] auto _ : state) {
    state.PauseTiming();
    for (uint64_t i = 0; i < key_num; i++) {
      Status s = db->Put(wo, kg.Next(),
                         rnd.RandomString(static_cast<int>(per_key_size)));
    }
    FlushOptions fo;
    state.ResumeTiming();
    Status s = db->Flush(fo);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  if (state.thread_index() == 0) {
    auto db_full = static_cast_with_check<DBImpl>(db.get());
    Status s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
    if (enable_statistics) {
      HistogramData histogram_data;
      options.statistics->histogramData(FLUSH_TIME, &histogram_data);
      state.counters["flush_time"] = histogram_data.average;
      state.counters["flush_write_bytes"] = static_cast<double>(
          options.statistics->getTickerCount(FLUSH_WRITE_BYTES));
    }

    TeardownDB(state, db, options, kg);
  }
}

static void ManualFlushArguments(benchmark::internal::Benchmark* b) {
  for (int64_t key_num : {1l << 10, 8l << 10, 64l << 10}) {
    for (int64_t per_key_size : {256, 1024}) {
      b->Args({key_num, per_key_size});
    }
  }
  b->ArgNames({"key_num", "per_key_size"});
}

BENCHMARK(ManualFlush)->Iterations(1)->Apply(ManualFlushArguments);

// Copied from test_util.cc to not depend on rocksdb_test_lib
// when building microbench binaries.
static Slice CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst) {
  int raw = static_cast<int>(len * compressed_fraction);
  if (raw < 1) {
    raw = 1;
  }
  std::string raw_data = rnd->RandomBinaryString(raw);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < (unsigned int)len) {
    dst->append(raw_data);
  }
  dst->resize(len);
  return Slice(*dst);
}

static void DBGet(benchmark::State& state) {
  auto compaction_style = static_cast<CompactionStyle>(state.range(0));
  uint64_t max_data = state.range(1);
  uint64_t per_key_size = state.range(2);
  bool enable_statistics = state.range(3);
  bool negative_query = state.range(4);
  bool enable_filter = state.range(5);
  bool mmap = state.range(6);
  auto compression_type = static_cast<CompressionType>(state.range(7));
  bool compression_checksum = static_cast<bool>(state.range(8));
  bool no_blockcache = state.range(9);
  uint64_t key_num = max_data / per_key_size;

  // setup DB
  static std::unique_ptr<DB> db;
  Options options;
  if (enable_statistics) {
    options.statistics = CreateDBStatistics();
  }
  if (mmap) {
    options.allow_mmap_reads = true;
    options.compression = kNoCompression;
  }
  options.compaction_style = compaction_style;

  BlockBasedTableOptions table_options;
  if (enable_filter) {
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  }
  if (mmap) {
    table_options.no_block_cache = true;
    table_options.block_restart_interval = 1;
  }
  options.compression = compression_type;
  options.compression_opts.checksum = compression_checksum;
  if (no_blockcache) {
    table_options.no_block_cache = true;
  } else {
    table_options.block_cache = NewLRUCache(100 << 20);
  }
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  auto rnd = Random(301 + state.thread_index());

  if (state.thread_index() == 0) {
    KeyGenerator kg_seq(key_num /* max_key */);
    SetupDB(state, options, &db, "DBGet");

    // Load all valid keys into DB. That way, iterations in `!negative_query`
    // runs can always find the key even though it is generated from a random
    // number.
    auto wo = WriteOptions();
    wo.disableWAL = true;
    std::string val;
    for (uint64_t i = 0; i < key_num; i++) {
      CompressibleString(&rnd, 0.5, static_cast<int>(per_key_size), &val);
      Status s = db->Put(wo, kg_seq.Next(), val);
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }

    // Compact whole DB into one level, so each iteration will consider the same
    // number of files (one).
    Status s = db->CompactRange(CompactRangeOptions(), nullptr /* begin */,
                                nullptr /* end */);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  KeyGenerator kg_rnd(&rnd, key_num /* max_key */);
  auto ro = ReadOptions();
  if (mmap) {
    ro.verify_checksums = false;
  }
  size_t not_found = 0;
  if (negative_query) {
    for ([[maybe_unused]] auto _ : state) {
      std::string val;
      Status s = db->Get(ro, kg_rnd.NextNonExist(), &val);
      if (s.IsNotFound()) {
        not_found++;
      }
    }
  } else {
    for ([[maybe_unused]] auto _ : state) {
      std::string val;
      Status s = db->Get(ro, kg_rnd.Next(), &val);
      if (s.IsNotFound()) {
        not_found++;
      }
    }
  }

  state.counters["neg_qu_pct"] = benchmark::Counter(
      static_cast<double>(not_found * 100), benchmark::Counter::kAvgIterations);

  if (state.thread_index() == 0) {
    if (enable_statistics) {
      HistogramData histogram_data;
      options.statistics->histogramData(DB_GET, &histogram_data);
      state.counters["get_mean"] = histogram_data.average * std::milli::den;
      state.counters["get_p95"] = histogram_data.percentile95 * std::milli::den;
      state.counters["get_p99"] = histogram_data.percentile99 * std::milli::den;
    }

    TeardownDB(state, db, options, kg_rnd);
  }
}

static void DBGetArguments(benchmark::internal::Benchmark* b) {
  for (int comp_style : {kCompactionStyleLevel, kCompactionStyleUniversal,
                         kCompactionStyleFIFO}) {
    for (int64_t max_data : {1l << 20, 128l << 20, 512l << 20}) {
      for (int64_t per_key_size : {256, 1024}) {
        for (bool enable_statistics : {false, true}) {
          for (bool negative_query : {false, true}) {
            for (bool enable_filter : {false, true}) {
              for (bool mmap : {false, true}) {
                for (int compression_type :
                     {kNoCompression /* 0x0 */, kZSTD /* 0x7 */}) {
                  for (bool compression_checksum : {false, true}) {
                    for (bool no_blockcache : {false, true}) {
                      b->Args({comp_style, max_data, per_key_size,
                               enable_statistics, negative_query, enable_filter,
                               mmap, compression_type, compression_checksum,
                               no_blockcache});
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  b->ArgNames({"comp_style", "max_data", "per_key_size", "enable_statistics",
               "negative_query", "enable_filter", "mmap", "compression_type",
               "compression_checksum", "no_blockcache"});
}

static const uint64_t DBGetNum = 10000l;
BENCHMARK(DBGet)->Threads(1)->Iterations(DBGetNum)->Apply(DBGetArguments);
BENCHMARK(DBGet)->Threads(8)->Iterations(DBGetNum / 8)->Apply(DBGetArguments);

static void SimpleGetWithPerfContext(benchmark::State& state) {
  // setup DB
  static std::unique_ptr<DB> db;
  std::string db_name;
  Options options;
  options.create_if_missing = true;
  options.arena_block_size = 8 << 20;

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, 1024);

  if (state.thread_index() == 0) {
    auto env = Env::Default();
    std::string db_path;
    Status s = env->GetTestDirectory(&db_path);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
    db_name = db_path + "/simple_get_" + std::to_string(getpid());
    DestroyDB(db_name, options);

    {
      s = DB::Open(options, db_name, &db);
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
        return;
      }
    }
    // load db
    auto wo = WriteOptions();
    wo.disableWAL = true;
    for (uint64_t i = 0; i < 1024; i++) {
      s = db->Put(wo, kg.Next(), rnd.RandomString(1024));
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }
    auto db_full = static_cast_with_check<DBImpl>(db.get());
    s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
    FlushOptions fo;
    s = db->Flush(fo);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  auto ro = ReadOptions();
  size_t not_found = 0;
  uint64_t user_key_comparison_count = 0;
  uint64_t block_read_time = 0;
  uint64_t block_read_cpu_time = 0;
  uint64_t block_checksum_time = 0;
  uint64_t get_snapshot_time = 0;
  uint64_t get_post_process_time = 0;
  uint64_t get_from_output_files_time = 0;
  uint64_t new_table_block_iter_nanos = 0;
  uint64_t block_seek_nanos = 0;
  uint64_t get_cpu_nanos = 0;
  uint64_t get_from_table_nanos = 0;
  SetPerfLevel(kEnableTime);
  get_perf_context()->EnablePerLevelPerfContext();
  for ([[maybe_unused]] auto _ : state) {
    std::string val;
    get_perf_context()->Reset();
    Status s = db->Get(ro, kg.NextNonExist(), &val);
    if (s.IsNotFound()) {
      not_found++;
    }
    user_key_comparison_count += get_perf_context()->user_key_comparison_count;
    block_read_time += get_perf_context()->block_read_time;
    block_read_cpu_time += get_perf_context()->block_read_cpu_time;
    block_checksum_time += get_perf_context()->block_checksum_time;
    get_snapshot_time += get_perf_context()->get_snapshot_time;
    get_post_process_time += get_perf_context()->get_post_process_time;
    get_from_output_files_time +=
        get_perf_context()->get_from_output_files_time;
    new_table_block_iter_nanos +=
        get_perf_context()->new_table_block_iter_nanos;
    block_seek_nanos += get_perf_context()->block_seek_nanos;
    get_cpu_nanos += get_perf_context()->get_cpu_nanos;
    get_from_table_nanos +=
        (*(get_perf_context()->level_to_perf_context))[0].get_from_table_nanos;
  }

  state.counters["neg_qu_pct"] = benchmark::Counter(
      static_cast<double>(not_found * 100), benchmark::Counter::kAvgIterations);
  state.counters["user_key_comparison_count"] =
      benchmark::Counter(static_cast<double>(user_key_comparison_count),
                         benchmark::Counter::kAvgIterations);
  state.counters["block_read_time"] = benchmark::Counter(
      static_cast<double>(block_read_time), benchmark::Counter::kAvgIterations);
  state.counters["block_read_cpu_time"] =
      benchmark::Counter(static_cast<double>(block_read_cpu_time),
                         benchmark::Counter::kAvgIterations);
  state.counters["block_checksum_time"] =
      benchmark::Counter(static_cast<double>(block_checksum_time),
                         benchmark::Counter::kAvgIterations);
  state.counters["get_snapshot_time"] =
      benchmark::Counter(static_cast<double>(get_snapshot_time),
                         benchmark::Counter::kAvgIterations);
  state.counters["get_post_process_time"] =
      benchmark::Counter(static_cast<double>(get_post_process_time),
                         benchmark::Counter::kAvgIterations);
  state.counters["get_from_output_files_time"] =
      benchmark::Counter(static_cast<double>(get_from_output_files_time),
                         benchmark::Counter::kAvgIterations);
  state.counters["new_table_block_iter_nanos"] =
      benchmark::Counter(static_cast<double>(new_table_block_iter_nanos),
                         benchmark::Counter::kAvgIterations);
  state.counters["block_seek_nanos"] =
      benchmark::Counter(static_cast<double>(block_seek_nanos),
                         benchmark::Counter::kAvgIterations);
  state.counters["get_cpu_nanos"] = benchmark::Counter(
      static_cast<double>(get_cpu_nanos), benchmark::Counter::kAvgIterations);
  state.counters["get_from_table_nanos"] =
      benchmark::Counter(static_cast<double>(get_from_table_nanos),
                         benchmark::Counter::kAvgIterations);

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, kg);
  }
}

BENCHMARK(SimpleGetWithPerfContext)->Iterations(1000000);

static void DBGetMergeOperandsInMemtable(benchmark::State& state) {
  const uint64_t kDataLen = 16 << 20;  // 16MB
  const uint64_t kValueLen = 64;
  const uint64_t kNumEntries = kDataLen / kValueLen;
  const uint64_t kNumEntriesPerKey = state.range(0);
  const uint64_t kNumKeys = kNumEntries / kNumEntriesPerKey;

  // setup DB
  static std::unique_ptr<DB> db;

  Options options;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  // Make memtable large enough that automatic flush will not be triggered.
  options.write_buffer_size = 2 * kDataLen;

  KeyGenerator sequential_key_gen(kNumKeys);
  auto rnd = Random(301 + state.thread_index());

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "DBGetMergeOperandsInMemtable");

    // load db
    auto write_opts = WriteOptions();
    write_opts.disableWAL = true;
    for (uint64_t i = 0; i < kNumEntries; i++) {
      Status s = db->Merge(write_opts, sequential_key_gen.Next(),
                           rnd.RandomString(static_cast<int>(kValueLen)));
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }
  }

  KeyGenerator random_key_gen(kNumKeys);
  std::vector<PinnableSlice> value_operands;
  value_operands.resize(kNumEntriesPerKey);
  GetMergeOperandsOptions get_merge_ops_opts;
  get_merge_ops_opts.expected_max_number_of_operands =
      static_cast<int>(kNumEntriesPerKey);
  for ([[maybe_unused]] auto _ : state) {
    int num_value_operands = 0;
    Status s = db->GetMergeOperands(
        ReadOptions(), db->DefaultColumnFamily(), random_key_gen.Next(),
        value_operands.data(), &get_merge_ops_opts, &num_value_operands);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    if (num_value_operands != static_cast<int>(kNumEntriesPerKey)) {
      state.SkipWithError("Unexpected number of merge operands found for key");
    }
    for (auto& value_operand : value_operands) {
      value_operand.Reset();
    }
  }

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, random_key_gen);
  }
}

static void DBGetMergeOperandsInSstFile(benchmark::State& state) {
  const uint64_t kDataLen = 16 << 20;  // 16MB
  const uint64_t kValueLen = 64;
  const uint64_t kNumEntries = kDataLen / kValueLen;
  const uint64_t kNumEntriesPerKey = state.range(0);
  const uint64_t kNumKeys = kNumEntries / kNumEntriesPerKey;
  const bool kMmap = state.range(1);

  // setup DB
  static std::unique_ptr<DB> db;

  BlockBasedTableOptions table_options;
  if (kMmap) {
    table_options.no_block_cache = true;
  } else {
    // Make block cache large enough that eviction will not be triggered.
    table_options.block_cache = NewLRUCache(2 * kDataLen);
  }

  Options options;
  if (kMmap) {
    options.allow_mmap_reads = true;
  }
  options.compression = kNoCompression;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  // Make memtable large enough that automatic flush will not be triggered.
  options.write_buffer_size = 2 * kDataLen;

  KeyGenerator sequential_key_gen(kNumKeys);
  auto rnd = Random(301 + state.thread_index());

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "DBGetMergeOperandsInBlockCache");

    // load db
    //
    // Take a snapshot after each cycle of merges to ensure flush cannot
    // merge any entries.
    std::vector<const Snapshot*> snapshots;
    snapshots.resize(kNumEntriesPerKey);
    auto write_opts = WriteOptions();
    write_opts.disableWAL = true;
    for (uint64_t i = 0; i < kNumEntriesPerKey; i++) {
      for (uint64_t j = 0; j < kNumKeys; j++) {
        Status s = db->Merge(write_opts, sequential_key_gen.Next(),
                             rnd.RandomString(static_cast<int>(kValueLen)));
        if (!s.ok()) {
          state.SkipWithError(s.ToString().c_str());
        }
      }
      snapshots[i] = db->GetSnapshot();
    }

    // Flush to an L0 file; read back to prime the cache/mapped memory.
    db->Flush(FlushOptions());
    for (uint64_t i = 0; i < kNumKeys; ++i) {
      std::string value;
      Status s = db->Get(ReadOptions(), sequential_key_gen.Next(), &value);
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }

    if (state.thread_index() == 0) {
      for (uint64_t i = 0; i < kNumEntriesPerKey; ++i) {
        db->ReleaseSnapshot(snapshots[i]);
      }
    }
  }

  KeyGenerator random_key_gen(kNumKeys);
  std::vector<PinnableSlice> value_operands;
  value_operands.resize(kNumEntriesPerKey);
  GetMergeOperandsOptions get_merge_ops_opts;
  get_merge_ops_opts.expected_max_number_of_operands =
      static_cast<int>(kNumEntriesPerKey);
  for ([[maybe_unused]] auto _ : state) {
    int num_value_operands = 0;
    ReadOptions read_opts;
    read_opts.verify_checksums = false;
    Status s = db->GetMergeOperands(
        read_opts, db->DefaultColumnFamily(), random_key_gen.Next(),
        value_operands.data(), &get_merge_ops_opts, &num_value_operands);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
    if (num_value_operands != static_cast<int>(kNumEntriesPerKey)) {
      state.SkipWithError("Unexpected number of merge operands found for key");
    }
    for (auto& value_operand : value_operands) {
      value_operand.Reset();
    }
  }

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, random_key_gen);
  }
}

static void DBGetMergeOperandsInMemtableArguments(
    benchmark::internal::Benchmark* b) {
  for (int entries_per_key : {1, 32, 1024}) {
    b->Args({entries_per_key});
  }
  b->ArgNames({"entries_per_key"});
}

static void DBGetMergeOperandsInSstFileArguments(
    benchmark::internal::Benchmark* b) {
  for (int entries_per_key : {1, 32, 1024}) {
    for (bool mmap : {false, true}) {
      b->Args({entries_per_key, mmap});
    }
  }
  b->ArgNames({"entries_per_key", "mmap"});
}

BENCHMARK(DBGetMergeOperandsInMemtable)
    ->Threads(1)
    ->Apply(DBGetMergeOperandsInMemtableArguments);
BENCHMARK(DBGetMergeOperandsInMemtable)
    ->Threads(8)
    ->Apply(DBGetMergeOperandsInMemtableArguments);
BENCHMARK(DBGetMergeOperandsInSstFile)
    ->Threads(1)
    ->Apply(DBGetMergeOperandsInSstFileArguments);
BENCHMARK(DBGetMergeOperandsInSstFile)
    ->Threads(8)
    ->Apply(DBGetMergeOperandsInSstFileArguments);

std::string GenerateKey(int primary_key, int secondary_key, int padding_size,
                        Random* rnd) {
  char buf[50];
  char* p = &buf[0];
  snprintf(buf, sizeof(buf), "%6d%4d", primary_key, secondary_key);
  std::string k(p);
  if (padding_size) {
    k += rnd->RandomString(padding_size);
  }

  return k;
}

void GenerateRandomKVs(std::vector<std::string>* keys,
                       std::vector<std::string>* values, const int from,
                       const int len, const int step = 1,
                       const int padding_size = 0,
                       const int keys_share_prefix = 1) {
  Random rnd(302);

  // generate different prefix
  for (int i = from; i < from + len; i += step) {
    // generating keys that share the prefix
    for (int j = 0; j < keys_share_prefix; ++j) {
      keys->emplace_back(GenerateKey(i, j, padding_size, &rnd));
      // 100 bytes values
      values->emplace_back(rnd.RandomString(100));
    }
  }
}

// TODO: move it to different files, as it's testing an internal API
static void DataBlockSeek(benchmark::State& state) {
  Random rnd(301);
  Options options = Options();

  BlockBuilder builder(
      16, true, false, BlockBasedTableOptions::kDataBlockBinarySearch,
      0.75 /* data_block_hash_table_util_ratio */, 0 /* ts_sz */,
      true /* persist_user_defined_timestamps */, false /* is_user_key */,
      false /* use_separated_kv_storage */, nullptr /* statistics */,
      -1.0 /* uniform_cv_threshold */, false /* use_common_prefix */);

  int num_records = 500;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  GenerateRandomKVs(&keys, &values, 0, num_records);

  for (int i = 0; i < num_records; i++) {
    std::string ukey(keys[i] + "1");
    InternalKey ikey(ukey, 0, kTypeValue);
    builder.Add(ikey.Encode().ToString(), values[i]);
  }

  Slice rawblock = builder.Finish();

  BlockContents contents;
  contents.data = rawblock;
  Block reader(std::move(contents));

  SetPerfLevel(kEnableTime);
  uint64_t total = 0;
  for ([[maybe_unused]] auto _ : state) {
    DataBlockIter* iter = reader.NewDataIterator(options.comparator,
                                                 kDisableGlobalSequenceNumber);
    uint32_t index = rnd.Uniform(static_cast<int>(num_records));
    std::string ukey(keys[index] + "1");
    InternalKey ikey(ukey, 0, kTypeValue);
    get_perf_context()->Reset();
    bool may_exist = iter->SeekForGet(ikey.Encode().ToString());
    if (!may_exist) {
      state.SkipWithError("key not found");
    }
    total += get_perf_context()->block_seek_nanos;
    delete iter;
  }
  state.counters["seek_ns"] = benchmark::Counter(
      static_cast<double>(total), benchmark::Counter::kAvgIterations);
}

BENCHMARK(DataBlockSeek)->Iterations(1000000);

static void IteratorSeek(benchmark::State& state) {
  auto compaction_style = static_cast<CompactionStyle>(state.range(0));
  uint64_t max_data = state.range(1);
  uint64_t per_key_size = state.range(2);
  bool enable_statistics = state.range(3);
  bool negative_query = state.range(4);
  bool enable_filter = state.range(5);
  uint64_t key_num = max_data / per_key_size;

  // setup DB
  static std::unique_ptr<DB> db;
  Options options;
  if (enable_statistics) {
    options.statistics = CreateDBStatistics();
  }
  options.compaction_style = compaction_style;

  if (enable_filter) {
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, key_num);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "IteratorSeek");

    // load db
    auto wo = WriteOptions();
    wo.disableWAL = true;
    for (uint64_t i = 0; i < key_num; i++) {
      Status s = db->Put(wo, kg.Next(),
                         rnd.RandomString(static_cast<int>(per_key_size)));
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }

    FlushOptions fo;
    Status s = db->Flush(fo);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }

    auto db_full = static_cast_with_check<DBImpl>(db.get());
    s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
  }

  for ([[maybe_unused]] auto _ : state) {
    std::unique_ptr<Iterator> iter{nullptr};
    state.PauseTiming();
    if (!iter) {
      iter.reset(db->NewIterator(ReadOptions()));
    }
    Slice key = negative_query ? kg.NextNonExist() : kg.Next();
    if (!iter->status().ok()) {
      state.SkipWithError(iter->status().ToString().c_str());
      return;
    }
    state.ResumeTiming();
    iter->Seek(key);
  }

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, kg);
  }
}

static void IteratorSeekArguments(benchmark::internal::Benchmark* b) {
  for (int comp_style : {kCompactionStyleLevel, kCompactionStyleUniversal,
                         kCompactionStyleFIFO}) {
    for (int64_t max_data : {128l << 20, 512l << 20}) {
      for (int64_t per_key_size : {256, 1024}) {
        for (bool enable_statistics : {false, true}) {
          for (bool negative_query : {false, true}) {
            for (bool enable_filter : {false, true}) {
              b->Args({comp_style, max_data, per_key_size, enable_statistics,
                       negative_query, enable_filter});
            }
          }
        }
      }
    }
  }
  b->ArgNames({"comp_style", "max_data", "per_key_size", "enable_statistics",
               "negative_query", "enable_filter"});
}

static constexpr uint64_t kDBSeekNum = 10l << 10;
BENCHMARK(IteratorSeek)
    ->Threads(1)
    ->Iterations(kDBSeekNum)
    ->Apply(IteratorSeekArguments);
BENCHMARK(IteratorSeek)
    ->Threads(8)
    ->Iterations(kDBSeekNum / 8)
    ->Apply(IteratorSeekArguments);

static void IteratorNext(benchmark::State& state) {
  auto compaction_style = static_cast<CompactionStyle>(state.range(0));
  uint64_t max_data = state.range(1);
  uint64_t per_key_size = state.range(2);
  uint64_t key_num = max_data / per_key_size;

  // setup DB
  static std::unique_ptr<DB> db;
  Options options;
  options.compaction_style = compaction_style;

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, key_num);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "IteratorNext");
    // load db
    auto wo = WriteOptions();
    wo.disableWAL = true;
    for (uint64_t i = 0; i < key_num; i++) {
      Status s = db->Put(wo, kg.Next(),
                         rnd.RandomString(static_cast<int>(per_key_size)));
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }

    FlushOptions fo;
    Status s = db->Flush(fo);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }

    auto db_full = static_cast_with_check<DBImpl>(db.get());
    s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
  }

  for ([[maybe_unused]] auto _ : state) {
    std::unique_ptr<Iterator> iter{nullptr};
    state.PauseTiming();
    if (!iter) {
      iter.reset(db->NewIterator(ReadOptions()));
    }
    while (!iter->Valid()) {
      iter->Seek(kg.Next());
      if (!iter->status().ok()) {
        state.SkipWithError(iter->status().ToString().c_str());
      }
    }
    state.ResumeTiming();
    iter->Next();
  }

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, kg);
  }
}

static void IteratorNextArguments(benchmark::internal::Benchmark* b) {
  for (int comp_style : {kCompactionStyleLevel, kCompactionStyleUniversal,
                         kCompactionStyleFIFO}) {
    for (int64_t max_data : {128l << 20, 512l << 20}) {
      for (int64_t per_key_size : {256, 1024}) {
        b->Args({comp_style, max_data, per_key_size});
      }
    }
  }
  b->ArgNames({"comp_style", "max_data", "per_key_size"});
}
static constexpr uint64_t kIteratorNextNum = 10l << 10;
BENCHMARK(IteratorNext)
    ->Iterations(kIteratorNextNum)
    ->Apply(IteratorNextArguments);

static void IteratorNextWithPerfContext(benchmark::State& state) {
  // setup DB
  static std::unique_ptr<DB> db;
  Options options;

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, 1024);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "IteratorNextWithPerfContext");
    // load db
    auto wo = WriteOptions();
    wo.disableWAL = true;
    for (uint64_t i = 0; i < 1024; i++) {
      Status s = db->Put(wo, kg.Next(), rnd.RandomString(1024));
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }
    auto db_full = static_cast_with_check<DBImpl>(db.get());
    Status s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
    FlushOptions fo;
    s = db->Flush(fo);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  uint64_t user_key_comparison_count = 0;
  uint64_t internal_key_skipped_count = 0;
  uint64_t find_next_user_entry_time = 0;
  uint64_t iter_next_cpu_nanos = 0;

  SetPerfLevel(kEnableTime);
  get_perf_context()->EnablePerLevelPerfContext();

  for ([[maybe_unused]] auto _ : state) {
    std::unique_ptr<Iterator> iter{nullptr};
    state.PauseTiming();
    if (!iter) {
      iter.reset(db->NewIterator(ReadOptions()));
    }
    while (!iter->Valid()) {
      iter->Seek(kg.Next());
      if (!iter->status().ok()) {
        state.SkipWithError(iter->status().ToString().c_str());
      }
    }
    get_perf_context()->Reset();
    state.ResumeTiming();

    iter->Next();
    user_key_comparison_count += get_perf_context()->user_key_comparison_count;
    internal_key_skipped_count +=
        get_perf_context()->internal_key_skipped_count;
    find_next_user_entry_time += get_perf_context()->find_next_user_entry_time;
    iter_next_cpu_nanos += get_perf_context()->iter_next_cpu_nanos;
  }

  state.counters["user_key_comparison_count"] =
      benchmark::Counter(static_cast<double>(user_key_comparison_count),
                         benchmark::Counter::kAvgIterations);
  state.counters["internal_key_skipped_count"] =
      benchmark::Counter(static_cast<double>(internal_key_skipped_count),
                         benchmark::Counter::kAvgIterations);
  state.counters["find_next_user_entry_time"] =
      benchmark::Counter(static_cast<double>(find_next_user_entry_time),
                         benchmark::Counter::kAvgIterations);
  state.counters["iter_next_cpu_nanos"] =
      benchmark::Counter(static_cast<double>(iter_next_cpu_nanos),
                         benchmark::Counter::kAvgIterations);

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, kg);
  }
}

BENCHMARK(IteratorNextWithPerfContext)->Iterations(100000);

static void IteratorPrev(benchmark::State& state) {
  auto compaction_style = static_cast<CompactionStyle>(state.range(0));
  uint64_t max_data = state.range(1);
  uint64_t per_key_size = state.range(2);
  uint64_t key_num = max_data / per_key_size;

  // setup DB
  static std::unique_ptr<DB> db;
  std::string db_name;
  Options options;
  options.compaction_style = compaction_style;

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, key_num);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "IteratorPrev");
    // load db
    auto wo = WriteOptions();
    wo.disableWAL = true;
    for (uint64_t i = 0; i < key_num; i++) {
      Status s = db->Put(wo, kg.Next(),
                         rnd.RandomString(static_cast<int>(per_key_size)));
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }

    FlushOptions fo;
    Status s = db->Flush(fo);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }

    auto db_full = static_cast_with_check<DBImpl>(db.get());
    s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
  }

  for ([[maybe_unused]] auto _ : state) {
    std::unique_ptr<Iterator> iter{nullptr};
    state.PauseTiming();
    if (!iter) {
      iter.reset(db->NewIterator(ReadOptions()));
    }
    while (!iter->Valid()) {
      iter->Seek(kg.Next());
      if (!iter->status().ok()) {
        state.SkipWithError(iter->status().ToString().c_str());
      }
    }
    state.ResumeTiming();
    iter->Prev();
  }

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, kg);
  }
}

static void IteratorPrevArguments(benchmark::internal::Benchmark* b) {
  for (int comp_style : {kCompactionStyleLevel, kCompactionStyleUniversal,
                         kCompactionStyleFIFO}) {
    for (int64_t max_data : {128l << 20, 512l << 20}) {
      for (int64_t per_key_size : {256, 1024}) {
        b->Args({comp_style, max_data, per_key_size});
      }
    }
  }
  b->ArgNames({"comp_style", "max_data", "per_key_size"});
}

static constexpr uint64_t kIteratorPrevNum = 10l << 10;
BENCHMARK(IteratorPrev)
    ->Iterations(kIteratorPrevNum)
    ->Apply(IteratorPrevArguments);

static void PrefixSeek(benchmark::State& state) {
  auto compaction_style = static_cast<CompactionStyle>(state.range(0));
  uint64_t max_data = state.range(1);
  uint64_t per_key_size = state.range(2);
  bool enable_statistics = state.range(3);
  bool enable_filter = state.range(4);
  uint64_t key_num = max_data / per_key_size;

  // setup DB
  static std::unique_ptr<DB> db;
  Options options;
  if (enable_statistics) {
    options.statistics = CreateDBStatistics();
  }
  options.compaction_style = compaction_style;
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));

  if (enable_filter) {
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  }

  auto rnd = Random(301 + state.thread_index());
  KeyGenerator kg(&rnd, key_num, key_num / 100);

  if (state.thread_index() == 0) {
    SetupDB(state, options, &db, "PrefixSeek");

    // load db
    auto wo = WriteOptions();
    wo.disableWAL = true;
    for (uint64_t i = 0; i < key_num; i++) {
      Status s = db->Put(wo, kg.Next(),
                         rnd.RandomString(static_cast<int>(per_key_size)));
      if (!s.ok()) {
        state.SkipWithError(s.ToString().c_str());
      }
    }

    FlushOptions fo;
    Status s = db->Flush(fo);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }

    auto db_full = static_cast_with_check<DBImpl>(db.get());
    s = db_full->WaitForCompact(WaitForCompactOptions());
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
      return;
    }
  }

  for ([[maybe_unused]] auto _ : state) {
    std::unique_ptr<Iterator> iter{nullptr};
    state.PauseTiming();
    if (!iter) {
      iter.reset(db->NewIterator(ReadOptions()));
    }
    state.ResumeTiming();
    iter->Seek(kg.NextPrefix());
    if (!iter->status().ok()) {
      state.SkipWithError(iter->status().ToString().c_str());
      return;
    }
  }

  if (state.thread_index() == 0) {
    TeardownDB(state, db, options, kg);
  }
}

static void PrefixSeekArguments(benchmark::internal::Benchmark* b) {
  for (int comp_style : {kCompactionStyleLevel, kCompactionStyleUniversal,
                         kCompactionStyleFIFO}) {
    for (int64_t max_data : {128l << 20, 512l << 20}) {
      for (int64_t per_key_size : {256, 1024}) {
        for (bool enable_statistics : {false, true}) {
          for (bool enable_filter : {false, true}) {
            b->Args({comp_style, max_data, per_key_size, enable_statistics,
                     enable_filter});
          }
        }
      }
    }
  }
  b->ArgNames({"comp_style", "max_data", "per_key_size", "enable_statistics",
               "enable_filter"});
}

static constexpr uint64_t kPrefixSeekNum = 10l << 10;
BENCHMARK(PrefixSeek)->Iterations(kPrefixSeekNum)->Apply(PrefixSeekArguments);
BENCHMARK(PrefixSeek)
    ->Threads(8)
    ->Iterations(kPrefixSeekNum / 8)
    ->Apply(PrefixSeekArguments);

// TODO: move it to different files, as it's testing an internal API
static void RandomAccessFileReaderRead(benchmark::State& state) {
  bool enable_statistics = state.range(0);
  constexpr int kFileNum = 10;
  auto env = Env::Default();
  auto fs = env->GetFileSystem();
  std::string db_path;
  Status s = env->GetTestDirectory(&db_path);
  if (!s.ok()) {
    state.SkipWithError(s.ToString().c_str());
    return;
  }

  // Setup multiple `RandomAccessFileReader`s with different parameters to be
  // used for test
  Random rand(301);
  std::string fname_base =
      db_path + kFilePathSeparator + "random-access-file-reader-read";
  std::vector<std::unique_ptr<RandomAccessFileReader>> readers;
  auto statistics_share = CreateDBStatistics();
  Statistics* statistics = enable_statistics ? statistics_share.get() : nullptr;
  for (int i = 0; i < kFileNum; i++) {
    std::string fname = fname_base + std::to_string(i);
    std::string content = rand.RandomString(kDefaultPageSize);
    std::unique_ptr<WritableFile> tgt_file;
    env->NewWritableFile(fname, &tgt_file, EnvOptions());
    tgt_file->Append(content);
    tgt_file->Close();

    std::unique_ptr<FSRandomAccessFile> f;
    fs->NewRandomAccessFile(fname, FileOptions(), &f, nullptr);
    int rand_num = rand.Next() % 3;
    auto temperature = rand_num == 0   ? Temperature::kUnknown
                       : rand_num == 1 ? Temperature::kWarm
                                       : Temperature::kCold;
    readers.emplace_back(new RandomAccessFileReader(
        std::move(f), fname, env->GetSystemClock().get(), nullptr, statistics,
        Histograms::HISTOGRAM_ENUM_MAX, nullptr, nullptr, {}, temperature,
        rand_num == 1));
  }

  IOOptions io_options;
  std::unique_ptr<char[]> scratch(new char[2048]);
  Slice result;
  uint64_t idx = 0;
  for ([[maybe_unused]] auto _ : state) {
    s = readers[idx++ % kFileNum]->Read(0, kDefaultPageSize / 3, io_options,
                                        &result, scratch.get(), nullptr);
    if (!s.ok()) {
      state.SkipWithError(s.ToString().c_str());
    }
  }

  // clean up
  for (int i = 0; i < kFileNum; i++) {
    std::string fname = fname_base + std::to_string(i);
    env->DeleteFile(fname);  // ignore return, okay to fail cleanup
  }
}

BENCHMARK(RandomAccessFileReaderRead)
    ->Iterations(1000000)
    ->Arg(0)
    ->Arg(1)
    ->ArgName("enable_statistics");

}  // namespace ROCKSDB_NAMESPACE

BENCHMARK_MAIN();
