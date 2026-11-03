//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "test_util/testutil.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <array>
#include <cctype>
#include <fstream>
#include <sstream>

#include "db/memtable_list.h"
#include "env/composite_env_wrapper.h"
#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/utilities/object_registry.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"

#ifndef ROCKSDB_UNITTESTS_WITH_CUSTOM_OBJECTS_FROM_STATIC_LIBS
void RegisterCustomObjects(int /*argc*/, char** /*argv*/) {}
#endif

namespace ROCKSDB_NAMESPACE::test {

const uint32_t kDefaultFormatVersion = BlockBasedTableOptions().format_version;
const std::set<uint32_t> kFooterFormatVersionsToTest{
    // Non-legacy, before big footer changes
    5U,
    // After big footer changes
    6U,
    // In case any interesting future changes
    kDefaultFormatVersion,
    BlockBasedTableOptions::kLatestSupportedFormatVersion,
    // Unpublished draft format_version 8 (index value-delta escape). Writing it
    // requires the TEST_AllowUnsupportedFormatVersion() opt-in, enabled by the
    // test files that instantiate parameterized suites over this set.
    8U,
};
const ReadOptionsNoIo kReadOptionsNoIo;

std::string RandomKey(Random* rnd, int len, RandomKeyType type) {
  // Make sure to generate a wide variety of characters so we
  // test the boundary conditions for short-key optimizations.
  static const char kTestChars[] = {'\0', '\1', 'a',    'b',    'c',
                                    'd',  'e',  '\xfd', '\xfe', '\xff'};
  std::string result;
  for (int i = 0; i < len; i++) {
    std::size_t indx = 0;
    switch (type) {
      case RandomKeyType::RANDOM:
        indx = rnd->Uniform(sizeof(kTestChars));
        break;
      case RandomKeyType::LARGEST:
        indx = sizeof(kTestChars) - 1;
        break;
      case RandomKeyType::MIDDLE:
        indx = sizeof(kTestChars) / 2;
        break;
      case RandomKeyType::SMALLEST:
        indx = 0;
        break;
    }
    result += kTestChars[indx];
  }
  return result;
}

const std::vector<UserDefinedTimestampTestMode>& GetUDTTestModes() {
  static std::vector<UserDefinedTimestampTestMode> udt_test_modes = {
      UserDefinedTimestampTestMode::kStripUserDefinedTimestamp,
      UserDefinedTimestampTestMode::kNormal,
      UserDefinedTimestampTestMode::kNone};
  return udt_test_modes;
}

bool IsUDTEnabled(const UserDefinedTimestampTestMode& test_mode) {
  return test_mode != UserDefinedTimestampTestMode::kNone;
}

bool ShouldPersistUDT(const UserDefinedTimestampTestMode& test_mode) {
  return test_mode != UserDefinedTimestampTestMode::kStripUserDefinedTimestamp;
}

Slice CompressibleString(Random* rnd, double compressed_to_fraction, int len,
                         std::string* dst) {
  int raw = static_cast<int>(len * compressed_to_fraction);
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

namespace {
class Uint64ComparatorImpl : public Comparator {
 public:
  Uint64ComparatorImpl() = default;

  const char* Name() const override { return "rocksdb.Uint64Comparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    assert(a.size() == sizeof(uint64_t) && b.size() == sizeof(uint64_t));
    const uint64_t* left = reinterpret_cast<const uint64_t*>(a.data());
    const uint64_t* right = reinterpret_cast<const uint64_t*>(b.data());
    uint64_t leftValue;
    uint64_t rightValue;
    GetUnaligned(left, &leftValue);
    GetUnaligned(right, &rightValue);
    if (leftValue == rightValue) {
      return 0;
    } else if (leftValue < rightValue) {
      return -1;
    } else {
      return 1;
    }
  }

  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {}

  void FindShortSuccessor(std::string* /*key*/) const override {}
};
}  // namespace

const Comparator* Uint64Comparator() {
  static Uint64ComparatorImpl uint64comp;
  return &uint64comp;
}

const Comparator* BytewiseComparatorWithU64TsWrapper() {
  ConfigOptions config_options;
  const Comparator* user_comparator = nullptr;
  Status s = Comparator::CreateFromString(
      config_options, "leveldb.BytewiseComparator.u64ts", &user_comparator);
  s.PermitUncheckedError();
  return user_comparator;
}

const Comparator* ReverseBytewiseComparatorWithU64TsWrapper() {
  ConfigOptions config_options;
  const Comparator* user_comparator = nullptr;
  Status s = Comparator::CreateFromString(
      config_options, "rocksdb.ReverseBytewiseComparator.u64ts",
      &user_comparator);
  s.PermitUncheckedError();
  return user_comparator;
}

void CorruptKeyType(InternalKey* ikey) {
  std::string keystr = ikey->Encode().ToString();
  keystr[keystr.size() - 8] = kTypeLogData;
  ikey->DecodeFrom(Slice(keystr.data(), keystr.size()));
}

std::string KeyStr(const std::string& user_key, const SequenceNumber& seq,
                   const ValueType& t, bool corrupt) {
  InternalKey k(user_key, seq, t);
  if (corrupt) {
    CorruptKeyType(&k);
  }
  return k.Encode().ToString();
}

std::string KeyStr(uint64_t ts, const std::string& user_key,
                   const SequenceNumber& seq, const ValueType& t,
                   bool corrupt) {
  std::string user_key_with_ts(user_key);
  std::string ts_str;
  PutFixed64(&ts_str, ts);
  user_key_with_ts.append(ts_str);
  return KeyStr(user_key_with_ts, seq, t, corrupt);
}

bool SleepingBackgroundTask::TimedWaitUntilSleeping(uint64_t wait_time) {
  auto abs_time = SystemClock::Default()->NowMicros() + wait_time;
  MutexLock l(&mutex_);
  while (!sleeping_ || !should_sleep_) {
    if (bg_cv_.TimedWait(abs_time)) {
      return true;
    }
  }
  return false;
}

bool SleepingBackgroundTask::TimedWaitUntilDone(uint64_t wait_time) {
  auto abs_time = SystemClock::Default()->NowMicros() + wait_time;
  MutexLock l(&mutex_);
  while (!done_with_sleep_) {
    if (bg_cv_.TimedWait(abs_time)) {
      return true;
    }
  }
  return false;
}

std::string RandomName(Random* rnd, const size_t len) {
  std::stringstream ss;
  for (size_t i = 0; i < len; ++i) {
    ss << static_cast<char>(rnd->Uniform(26) + 'a');
  }
  return ss.str();
}

CompressionType RandomCompressionType(Random* rnd) {
  auto ret = static_cast<CompressionType>(rnd->Uniform(6));
  while (!CompressionTypeSupported(ret)) {
    ret = static_cast<CompressionType>((static_cast<int>(ret) + 1) % 6);
  }
  return ret;
}

void RandomCompressionTypeVector(const size_t count,
                                 std::vector<CompressionType>* types,
                                 Random* rnd) {
  types->clear();
  for (size_t i = 0; i < count; ++i) {
    types->emplace_back(RandomCompressionType(rnd));
  }
}

const SliceTransform* RandomSliceTransform(Random* rnd, int pre_defined) {
  int random_num = pre_defined >= 0 ? pre_defined : rnd->Uniform(4);
  switch (random_num) {
    case 0:
      return NewFixedPrefixTransform(rnd->Uniform(20) + 1);
    case 1:
      return NewCappedPrefixTransform(rnd->Uniform(20) + 1);
    case 2:
      return NewNoopTransform();
    default:
      return nullptr;
  }
}

BlockBasedTableOptions RandomBlockBasedTableOptions(Random* rnd) {
  BlockBasedTableOptions opt;
  opt.cache_index_and_filter_blocks = rnd->Uniform(2);
  opt.pin_l0_filter_and_index_blocks_in_cache = rnd->Uniform(2);
  opt.pin_top_level_index_and_filter = rnd->Uniform(2);
  using IndexType = BlockBasedTableOptions::IndexType;
  const std::array<IndexType, 4> index_types = {
      {IndexType::kBinarySearch, IndexType::kHashSearch,
       IndexType::kTwoLevelIndexSearch, IndexType::kBinarySearchWithFirstKey}};
  opt.index_type =
      index_types[rnd->Uniform(static_cast<int>(index_types.size()))];
  opt.checksum = static_cast<ChecksumType>(rnd->Uniform(3));
  opt.block_size = rnd->Uniform(10000000);
  opt.block_size_deviation = rnd->Uniform(100);
  opt.block_restart_interval = rnd->Uniform(100);
  opt.index_block_restart_interval = rnd->Uniform(100);
  opt.whole_key_filtering = rnd->Uniform(2);

  return opt;
}

TableFactory* RandomTableFactory(Random* rnd, int pre_defined) {
  int random_num = pre_defined >= 0 ? pre_defined : rnd->Uniform(4);
  switch (random_num) {
    case 0:
      return NewPlainTableFactory();
    case 1:
      return NewCuckooTableFactory();
    default:
      return NewBlockBasedTableFactory();
  }
}

MergeOperator* RandomMergeOperator(Random* rnd) {
  return new ChanglingMergeOperator(RandomName(rnd, 10));
}

CompactionFilter* RandomCompactionFilter(Random* rnd) {
  return new ChanglingCompactionFilter(RandomName(rnd, 10));
}

CompactionFilterFactory* RandomCompactionFilterFactory(Random* rnd) {
  return new ChanglingCompactionFilterFactory(RandomName(rnd, 10));
}

void RandomInitDBOptions(DBOptions* db_opt, Random* rnd) {
  // boolean options
  db_opt->advise_random_on_open = rnd->Uniform(2);
  db_opt->allow_mmap_reads = rnd->Uniform(2);
  db_opt->allow_mmap_writes = rnd->Uniform(2);
  db_opt->use_direct_reads = rnd->Uniform(2);
  db_opt->use_direct_io_for_compaction_reads = rnd->Uniform(2);
  db_opt->use_direct_io_for_flush_and_compaction = rnd->Uniform(2);
  db_opt->create_if_missing = rnd->Uniform(2);
  db_opt->create_missing_column_families = rnd->Uniform(2);
  db_opt->enable_thread_tracking = rnd->Uniform(2);
  db_opt->error_if_exists = rnd->Uniform(2);
  db_opt->is_fd_close_on_exec = rnd->Uniform(2);
  db_opt->paranoid_checks = rnd->Uniform(2);
  db_opt->track_and_verify_wals_in_manifest = rnd->Uniform(2);
  db_opt->track_and_verify_wals = rnd->Uniform(2);
  db_opt->verify_sst_unique_id_in_manifest = rnd->Uniform(2);
  db_opt->skip_stats_update_on_db_open = rnd->Uniform(2);
  db_opt->use_adaptive_mutex = rnd->Uniform(2);
  db_opt->use_fsync = rnd->Uniform(2);
  db_opt->recycle_log_file_num = rnd->Uniform(2);
  db_opt->async_wal_precreate = rnd->Uniform(2);
  db_opt->avoid_flush_during_recovery = rnd->Uniform(2);
  db_opt->avoid_flush_during_shutdown = rnd->Uniform(2);
  db_opt->enforce_single_del_contracts = rnd->Uniform(2);

  // int options
  db_opt->max_background_compactions = rnd->Uniform(100);
  db_opt->max_background_flushes = rnd->Uniform(100);
  db_opt->max_file_opening_threads = rnd->Uniform(100);
  db_opt->max_open_files = rnd->Uniform(100);
  db_opt->table_cache_numshardbits = rnd->Uniform(100);

  // size_t options
  db_opt->db_write_buffer_size = rnd->Uniform(10000);
  db_opt->keep_log_file_num = rnd->Uniform(10000);
  db_opt->log_file_time_to_roll = rnd->Uniform(10000);
  db_opt->manifest_preallocation_size = rnd->Uniform(10000);
  db_opt->max_log_file_size = rnd->Uniform(10000);

  // std::string options
  db_opt->db_log_dir = "path/to/db_log_dir";
  db_opt->wal_dir = "path/to/wal_dir";

  // uint32_t options
  db_opt->max_subcompactions = rnd->Uniform(100000);

  // uint64_t options
  static const uint64_t uint_max = static_cast<uint64_t>(UINT_MAX);
  db_opt->WAL_size_limit_MB = uint_max + rnd->Uniform(100000);
  db_opt->WAL_ttl_seconds = uint_max + rnd->Uniform(100000);
  db_opt->bytes_per_sync = uint_max + rnd->Uniform(100000);
  db_opt->delayed_write_rate = uint_max + rnd->Uniform(100000);
  db_opt->delete_obsolete_files_period_micros = uint_max + rnd->Uniform(100000);
  db_opt->max_manifest_file_size = uint_max + rnd->Uniform(100000);
  db_opt->max_total_wal_size = uint_max + rnd->Uniform(100000);
  db_opt->wal_bytes_per_sync = uint_max + rnd->Uniform(100000);

  // unsigned int options
  db_opt->stats_dump_period_sec = rnd->Uniform(100000);
}

void RandomInitCFOptions(ColumnFamilyOptions* cf_opt, DBOptions& db_options,
                         Random* rnd) {
  cf_opt->compaction_style = (CompactionStyle)(rnd->Uniform(4));

  // boolean options
  cf_opt->report_bg_io_stats = rnd->Uniform(2);
  cf_opt->disable_auto_compactions = rnd->Uniform(2);
  cf_opt->inplace_update_support = rnd->Uniform(2);
  cf_opt->level_compaction_dynamic_level_bytes = rnd->Uniform(2);
  cf_opt->optimize_filters_for_hits = rnd->Uniform(2);
  cf_opt->paranoid_file_checks = rnd->Uniform(2);
  cf_opt->force_consistency_checks = rnd->Uniform(2);
  cf_opt->compaction_options_fifo.allow_compaction = rnd->Uniform(2);
  cf_opt->memtable_whole_key_filtering = rnd->Uniform(2);
  cf_opt->enable_blob_files = rnd->Uniform(2);
  cf_opt->enable_blob_garbage_collection = rnd->Uniform(2);
  cf_opt->strict_max_successive_merges = rnd->Uniform(2);

  // double options
  cf_opt->max_bytes_for_level_multiplier_slack = rnd->Uniform(1000) / 1000.0;
  cf_opt->memtable_prefix_bloom_size_ratio =
      static_cast<double>(rnd->Uniform(10000)) / 20000.0;
  cf_opt->blob_garbage_collection_age_cutoff = rnd->Uniform(10000) / 10000.0;
  cf_opt->blob_garbage_collection_force_threshold =
      rnd->Uniform(10000) / 10000.0;

  // int options
  cf_opt->level0_file_num_compaction_trigger = rnd->Uniform(100);
  cf_opt->level0_slowdown_writes_trigger = rnd->Uniform(100);
  cf_opt->level0_stop_writes_trigger = rnd->Uniform(100);
  cf_opt->max_bytes_for_level_multiplier = rnd->Uniform(100);
  cf_opt->max_write_buffer_number = rnd->Uniform(100);
  cf_opt->max_write_buffer_size_to_maintain = rnd->Uniform(10000);
  cf_opt->min_write_buffer_number_to_merge = rnd->Uniform(100);
  cf_opt->num_levels = rnd->Uniform(100);
  cf_opt->target_file_size_multiplier = rnd->Uniform(100);

  // vector int options
  cf_opt->max_bytes_for_level_multiplier_additional.resize(cf_opt->num_levels);
  for (int i = 0; i < cf_opt->num_levels; i++) {
    cf_opt->max_bytes_for_level_multiplier_additional[i] = rnd->Uniform(100);
  }

  // size_t options
  cf_opt->arena_block_size = rnd->Uniform(10000);
  cf_opt->inplace_update_num_locks = rnd->Uniform(10000);
  cf_opt->max_successive_merges = rnd->Uniform(10000);
  cf_opt->memtable_huge_page_size = rnd->Uniform(10000);
  cf_opt->write_buffer_size = rnd->Uniform(10000);

  // uint32_t options
  cf_opt->bloom_locality = rnd->Uniform(10000);
  cf_opt->max_bytes_for_level_base = rnd->Uniform(10000);

  // uint64_t options
  static const uint64_t uint_max = static_cast<uint64_t>(UINT_MAX);
  cf_opt->ttl =
      db_options.max_open_files == -1 ? uint_max + rnd->Uniform(10000) : 0;
  cf_opt->periodic_compaction_seconds =
      db_options.max_open_files == -1 ? uint_max + rnd->Uniform(10000) : 0;
  cf_opt->max_sequential_skip_in_iterations = uint_max + rnd->Uniform(10000);
  cf_opt->target_file_size_base = uint_max + rnd->Uniform(10000);
  cf_opt->max_compaction_bytes =
      cf_opt->target_file_size_base * rnd->Uniform(100);
  cf_opt->compaction_options_fifo.max_table_files_size =
      uint_max + rnd->Uniform(10000);
  cf_opt->min_blob_size = uint_max + rnd->Uniform(10000);
  cf_opt->blob_file_size = uint_max + rnd->Uniform(10000);
  cf_opt->blob_compaction_readahead_size = uint_max + rnd->Uniform(10000);

  // pointer typed options
  cf_opt->prefix_extractor.reset(RandomSliceTransform(rnd));
  cf_opt->table_factory.reset(RandomTableFactory(rnd));
  cf_opt->merge_operator.reset(RandomMergeOperator(rnd));
  if (cf_opt->compaction_filter) {
    delete cf_opt->compaction_filter;
  }
  cf_opt->compaction_filter = RandomCompactionFilter(rnd);
  cf_opt->compaction_filter_factory.reset(RandomCompactionFilterFactory(rnd));

  // custom typed options
  cf_opt->compression = RandomCompressionType(rnd);
  RandomCompressionTypeVector(cf_opt->num_levels,
                              &cf_opt->compression_per_level, rnd);
  cf_opt->blob_compression_type = RandomCompressionType(rnd);
}

bool IsDirectIOSupported(Env* env, const std::string& dir) {
  EnvOptions env_options;
  env_options.use_mmap_writes = false;
  env_options.use_direct_writes = true;
  std::string tmp = TempFileName(dir, 999);
  Status s;
  {
    std::unique_ptr<WritableFile> file;
    s = env->NewWritableFile(tmp, &file, env_options);
  }
  if (s.ok()) {
    s = env->DeleteFile(tmp);
  }
  return s.ok();
}

bool IsPrefetchSupported(const std::shared_ptr<FileSystem>& fs,
                         const std::string& dir) {
  bool supported = false;
  std::string tmp = TempFileName(dir, 999);
  Random rnd(301);
  std::string test_string = rnd.RandomString(4096);
  Slice data(test_string);
  IOOptions opts;
  Status s = WriteStringToFile(fs.get(), data, tmp, true, opts);
  if (s.ok()) {
    std::unique_ptr<FSRandomAccessFile> file;
    auto io_s = fs->NewRandomAccessFile(tmp, FileOptions(), &file, nullptr);
    if (io_s.ok()) {
      supported =
          !(file->Prefetch(0, data.size(), opts, nullptr).IsNotSupported());
    }
    s = fs->DeleteFile(tmp, opts, nullptr);
  }
  return s.ok() && supported;
}

size_t GetLinesCount(const std::string& fname, const std::string& pattern) {
  std::stringstream ssbuf;
  std::string line;
  size_t count = 0;

  std::ifstream inFile(fname.c_str());
  ssbuf << inFile.rdbuf();

  while (getline(ssbuf, line)) {
    if (line.find(pattern) != std::string::npos) {
      count++;
    }
  }

  return count;
}

Status CorruptFile(Env* env, const std::string& fname, int offset,
                   int bytes_to_corrupt, bool verify_checksum /*=true*/) {
  uint64_t size;
  Status s = env->GetFileSize(fname, &size);
  if (!s.ok()) {
    return s;
  } else if (offset < 0) {
    // Relative to end of file; make it absolute
    if (-offset > static_cast<int>(size)) {
      offset = 0;
    } else {
      offset = static_cast<int>(size + offset);
    }
  }
  if (offset > static_cast<int>(size)) {
    offset = static_cast<int>(size);
  }
  if (offset + bytes_to_corrupt > static_cast<int>(size)) {
    bytes_to_corrupt = static_cast<int>(size - offset);
  }

  // Do it
  std::string contents;
  s = ReadFileToString(env, fname, &contents);
  if (s.ok()) {
    for (int i = 0; i < bytes_to_corrupt; i++) {
      contents[i + offset] ^= 0x80;
    }
    s = WriteStringToFile(env, contents, fname, false /* should_sync */);
  }
  if (s.ok() && verify_checksum) {
    Options options;
    options.env = env;
    EnvOptions env_options;
    Status v = VerifySstFileChecksum(options, env_options, fname);
    assert(!v.ok());
  }
  return s;
}

Status TruncateFile(Env* env, const std::string& fname, uint64_t new_length) {
  uint64_t old_length;
  Status s = env->GetFileSize(fname, &old_length);
  if (!s.ok() || new_length == old_length) {
    return s;
  }
  // Do it
  std::string contents;
  s = ReadFileToString(env, fname, &contents);
  if (s.ok()) {
    contents.resize(static_cast<size_t>(new_length), 'b');
    s = WriteStringToFile(env, contents, fname, false /* should_sync */);
  }
  return s;
}

// Try and delete a directory if it exists
Status TryDeleteDir(Env* env, const std::string& dirname) {
  bool is_dir = false;
  Status s = env->IsDirectory(dirname, &is_dir);
  if (s.ok() && is_dir) {
    s = env->DeleteDir(dirname);
  }
  return s;
}

// Delete a directory if it exists
void DeleteDir(Env* env, const std::string& dirname) {
  TryDeleteDir(env, dirname).PermitUncheckedError();
}

FileType GetFileType(const std::string& path) {
  FileType type = kTempFile;
  std::size_t found = path.find_last_of('/');
  if (found == std::string::npos) {
    found = 0;
  }
  std::string file_name = path.substr(found);
  uint64_t number = 0;
  ParseFileName(file_name, &number, &type);
  return type;
}

uint64_t GetFileNumber(const std::string& path) {
  FileType type = kTempFile;
  std::size_t found = path.find_last_of('/');
  if (found == std::string::npos) {
    found = 0;
  }
  std::string file_name = path.substr(found);
  uint64_t number = 0;
  ParseFileName(file_name, &number, &type);
  return number;
}

Status CreateEnvFromSystem(const ConfigOptions& config_options, Env** result,
                           std::shared_ptr<Env>* guard) {
  const char* env_uri = getenv("TEST_ENV_URI");
  const char* fs_uri = getenv("TEST_FS_URI");
  if (env_uri || fs_uri) {
    return Env::CreateFromUri(config_options,
                              (env_uri != nullptr) ? env_uri : "",
                              (fs_uri != nullptr) ? fs_uri : "", result, guard);
  } else {
    // Neither specified.  Use the default
    *result = config_options.env;
    guard->reset();
    return Status::OK();
  }
}
namespace {
// A hacky skip list mem table that triggers flush after number of entries.
class SpecialMemTableRep : public MemTableRep {
 public:
  explicit SpecialMemTableRep(Allocator* allocator, MemTableRep* memtable,
                              int num_entries_flush)
      : MemTableRep(allocator),
        memtable_(memtable),
        num_entries_flush_(num_entries_flush),
        num_entries_(0) {}

  KeyHandle Allocate(const size_t len, char** buf) override {
    return memtable_->Allocate(len, buf);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(KeyHandle handle) override {
    num_entries_++;
    memtable_->Insert(handle);
  }

  void InsertConcurrently(KeyHandle handle) override {
    num_entries_++;
    memtable_->Insert(handle);
  }

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const override {
    return memtable_->Contains(key);
  }

  size_t ApproximateMemoryUsage() override {
    // Return a high memory usage when number of entries exceeds the threshold
    // to trigger a flush.
    return (num_entries_ < num_entries_flush_) ? 0 : 1024 * 1024 * 1024;
  }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    memtable_->Get(k, callback_args, callback_func);
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    return memtable_->ApproximateNumEntries(start_ikey, end_ikey);
  }

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    return memtable_->GetIterator(arena);
  }

  ~SpecialMemTableRep() override = default;

 private:
  std::unique_ptr<MemTableRep> memtable_;
  int num_entries_flush_;
  int num_entries_;
};
class SpecialSkipListFactory : public MemTableRepFactory {
 public:
  static bool Register(ObjectLibrary& library, const std::string& /*arg*/) {
    library.AddFactory<MemTableRepFactory>(
        ObjectLibrary::PatternEntry(SpecialSkipListFactory::kClassName(), true)
            .AddNumber(":"),
        [](const std::string& uri, std::unique_ptr<MemTableRepFactory>* guard,
           std::string* /* errmsg */) {
          auto colon = uri.find(':');
          if (colon != std::string::npos) {
            auto count = ParseInt(uri.substr(colon + 1));
            guard->reset(new SpecialSkipListFactory(count));
          } else {
            guard->reset(new SpecialSkipListFactory(2));
          }
          return guard->get();
        });
    return true;
  }
  // After number of inserts >= `num_entries_flush` in a mem table, trigger
  // flush.
  explicit SpecialSkipListFactory(int num_entries_flush)
      : num_entries_flush_(num_entries_flush) {}

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* /*logger*/) override {
    return new SpecialMemTableRep(
        allocator,
        factory_.CreateMemTableRep(compare, allocator, transform, nullptr),
        num_entries_flush_);
  }
  static const char* kClassName() { return "SpecialSkipListFactory"; }
  const char* Name() const override { return kClassName(); }
  std::string GetId() const override {
    std::string id = Name();
    if (num_entries_flush_ > 0) {
      id.append(":").append(std::to_string(num_entries_flush_));
    }
    return id;
  }

  bool IsInsertConcurrentlySupported() const override {
    return factory_.IsInsertConcurrentlySupported();
  }

 private:
  SkipListFactory factory_;
  int num_entries_flush_;
};
}  // namespace

MemTableRepFactory* NewSpecialSkipListFactory(int num_entries_per_flush) {
  RegisterTestLibrary();
  return new SpecialSkipListFactory(num_entries_per_flush);
}

// This method loads existing test classes into the ObjectRegistry
int RegisterTestObjects(ObjectLibrary& library, const std::string& arg) {
  size_t num_types;
  library.AddFactory<const Comparator>(
      test::SimpleSuffixReverseComparator::kClassName(),
      [](const std::string& /*uri*/,
         std::unique_ptr<const Comparator>* /*guard*/,
         std::string* /* errmsg */) {
        static test::SimpleSuffixReverseComparator ssrc;
        return &ssrc;
      });
  SpecialSkipListFactory::Register(library, arg);
  library.AddFactory<MergeOperator>(
      "Changling",
      [](const std::string& uri, std::unique_ptr<MergeOperator>* guard,
         std::string* /* errmsg */) {
        guard->reset(new test::ChanglingMergeOperator(uri));
        return guard->get();
      });
  library.AddFactory<CompactionFilter>(
      "Changling",
      [](const std::string& uri, std::unique_ptr<CompactionFilter>* /*guard*/,
         std::string* /* errmsg */) {
        return new test::ChanglingCompactionFilter(uri);
      });
  library.AddFactory<CompactionFilterFactory>(
      "Changling", [](const std::string& uri,
                      std::unique_ptr<CompactionFilterFactory>* guard,
                      std::string* /* errmsg */) {
        guard->reset(new test::ChanglingCompactionFilterFactory(uri));
        return guard->get();
      });
  library.AddFactory<SystemClock>(
      MockSystemClock::kClassName(),
      [](const std::string& /*uri*/, std::unique_ptr<SystemClock>* guard,
         std::string* /* errmsg */) {
        guard->reset(new MockSystemClock(SystemClock::Default()));
        return guard->get();
      });
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

void RegisterTestLibrary(const std::string& arg) {
  static bool registered = false;
  if (!registered) {
    registered = true;
    ObjectRegistry::Default()->AddLibrary("test", RegisterTestObjects, arg);
  }
}

const std::string kUnitTestDbId = "UnitTest";
}  // namespace ROCKSDB_NAMESPACE::test
