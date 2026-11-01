//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "file/file_prefetch_buffer.h"
#include "file/file_util.h"
#include "rocksdb/file_system.h"
#include "test_util/sync_point.h"
#ifdef GFLAGS
#include "tools/io_tracer_parser_tool.h"
#endif
#include "rocksdb/flush_block_policy.h"
#include "util/defer.h"
#include "util/random.h"

namespace {
static bool enable_io_uring = true;
extern "C" bool RocksDbIOUringEnable() { return enable_io_uring; }
}  // namespace

namespace ROCKSDB_NAMESPACE {

class MockFS;

class MockRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  MockRandomAccessFile(std::unique_ptr<FSRandomAccessFile>& file,
                       bool support_prefetch, std::atomic_int& prefetch_count,
                       bool small_buffer_alignment = false)
      : FSRandomAccessFileOwnerWrapper(std::move(file)),
        support_prefetch_(support_prefetch),
        prefetch_count_(prefetch_count),
        small_buffer_alignment_(small_buffer_alignment) {}

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* dbg) override {
    if (support_prefetch_) {
      prefetch_count_.fetch_add(1);
      return target()->Prefetch(offset, n, options, dbg);
    } else {
      return IOStatus::NotSupported("Prefetch not supported");
    }
  }

  size_t GetRequiredBufferAlignment() const override {
    return small_buffer_alignment_
               ? 1
               : FSRandomAccessFileOwnerWrapper::GetRequiredBufferAlignment();
  }

 private:
  const bool support_prefetch_;
  std::atomic_int& prefetch_count_;
  const bool small_buffer_alignment_;
};

class MockFS : public FileSystemWrapper {
 public:
  explicit MockFS(const std::shared_ptr<FileSystem>& wrapped,
                  bool support_prefetch, bool small_buffer_alignment = false)
      : FileSystemWrapper(wrapped),
        support_prefetch_(support_prefetch),
        small_buffer_alignment_(small_buffer_alignment) {}

  static const char* kClassName() { return "MockFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s;
    s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    result->reset(new MockRandomAccessFile(
        file, support_prefetch_, prefetch_count_, small_buffer_alignment_));
    return s;
  }

  void ClearPrefetchCount() { prefetch_count_ = 0; }

  bool IsPrefetchCalled() { return prefetch_count_ > 0; }

  int GetPrefetchCount() {
    return prefetch_count_.load(std::memory_order_relaxed);
  }

 private:
  const bool support_prefetch_;
  const bool small_buffer_alignment_;
  std::atomic_int prefetch_count_{0};
};

class PrefetchTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  PrefetchTest() : DBTestBase("prefetch_test", true) {}

  virtual void SetGenericOptions(Env* env, bool use_direct_io,
                                 Options& options) {
    anon::OptionsOverride options_override;
    // for !disable_io in PrefetchTest.Basic
    options_override.full_block_cache = true;
    options = CurrentOptions(options_override);
    options.write_buffer_size = 1024;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    options.env = env;
    options.disable_auto_compactions = true;
    if (use_direct_io) {
      options.use_direct_reads = true;
      options.use_direct_io_for_flush_and_compaction = true;
    }
  }

  void SetBlockBasedTableOptions(BlockBasedTableOptions& table_options) {
    table_options.no_block_cache = true;
    table_options.cache_index_and_filter_blocks = false;
    table_options.metadata_block_size = 1024;
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  }

  void VerifyScan(ReadOptions& iter_ro, ReadOptions& cmp_iter_ro,
                  const Slice* seek_key, const Slice* iterate_upper_bound,
                  bool prefix_same_as_start) const {
    assert(!(seek_key == nullptr));
    iter_ro.iterate_upper_bound = cmp_iter_ro.iterate_upper_bound =
        iterate_upper_bound;
    iter_ro.prefix_same_as_start = cmp_iter_ro.prefix_same_as_start =
        prefix_same_as_start;

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(iter_ro));
    auto cmp_iter = std::unique_ptr<Iterator>(db_->NewIterator(cmp_iter_ro));

    iter->Seek(*seek_key);
    cmp_iter->Seek(*seek_key);

    while (iter->Valid() && cmp_iter->Valid()) {
      if (iter->key() != cmp_iter->key()) {
        // Error
        ASSERT_TRUE(false);
      }
      iter->Next();
      cmp_iter->Next();
    }

    ASSERT_TRUE(!cmp_iter->Valid() && !iter->Valid());
    ASSERT_TRUE(cmp_iter->status().ok() && iter->status().ok());
  }

  void VerifySeekPrevSeek(ReadOptions& iter_ro, ReadOptions& cmp_iter_ro,
                          const Slice* seek_key,
                          const Slice* iterate_upper_bound,
                          bool prefix_same_as_start) {
    assert(!(seek_key == nullptr));
    iter_ro.iterate_upper_bound = cmp_iter_ro.iterate_upper_bound =
        iterate_upper_bound;
    iter_ro.prefix_same_as_start = cmp_iter_ro.prefix_same_as_start =
        prefix_same_as_start;

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(iter_ro));
    auto cmp_iter = std::unique_ptr<Iterator>(db_->NewIterator(cmp_iter_ro));

    // Seek
    cmp_iter->Seek(*seek_key);
    ASSERT_TRUE(cmp_iter->Valid());
    ASSERT_OK(cmp_iter->status());

    iter->Seek(*seek_key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    ASSERT_EQ(iter->key(), cmp_iter->key());

    // Prev op should pass
    cmp_iter->Prev();
    ASSERT_TRUE(cmp_iter->Valid());
    ASSERT_OK(cmp_iter->status());

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    ASSERT_EQ(iter->key(), cmp_iter->key());

    // Reseek would follow as usual
    cmp_iter->Seek(*seek_key);
    ASSERT_TRUE(cmp_iter->Valid());
    ASSERT_OK(cmp_iter->status());

    iter->Seek(*seek_key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());

    ASSERT_EQ(iter->key(), cmp_iter->key());
  }
};

INSTANTIATE_TEST_CASE_P(PrefetchTest, PrefetchTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

std::string BuildKey(int num, std::string postfix = "") {
  return "my_key_" + std::to_string(num) + postfix;
}

// This test verifies the following basic functionalities of prefetching:
// (1) If underline file system supports prefetch, and directIO is not enabled
// make sure prefetch() is called and FilePrefetchBuffer is not used.
// (2) If underline file system doesn't support prefetch, or directIO is
// enabled, make sure prefetch() is not called and FilePrefetchBuffer is
// used.
// (3) Measure read bytes, hit and miss of SST's tail prefetching during table
// open.
TEST_P(PrefetchTest, Basic) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();

  const int kNumKeys = 1100;
  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  // create first key range
  WriteBatch batch;
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), "v1"));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // create second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i, "key2"), "v2"));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // delete second key range
  batch.Clear();
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Delete(BuildKey(i, "key2")));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->Flush(FlushOptions()));

  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  const size_t num_file = metadata.size();
  // To verify SST file tail prefetch (once per file) during flush output
  // verification
  if (support_prefetch && !use_direct_io) {
    ASSERT_TRUE(fs->IsPrefetchCalled());
    ASSERT_EQ(num_file, fs->GetPrefetchCount());
    ASSERT_EQ(0, buff_prefetch_count);
    fs->ClearPrefetchCount();
  } else {
    ASSERT_FALSE(fs->IsPrefetchCalled());
    ASSERT_EQ(buff_prefetch_count, num_file);
    buff_prefetch_count = 0;
  }

  // compact database
  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  HistogramData prev_table_open_prefetch_tail_read;
  options.statistics->histogramData(TABLE_OPEN_PREFETCH_TAIL_READ_BYTES,
                                    &prev_table_open_prefetch_tail_read);
  const uint64_t prev_table_open_prefetch_tail_miss =
      options.statistics->getTickerCount(TABLE_OPEN_PREFETCH_TAIL_MISS);
  const uint64_t prev_table_open_prefetch_tail_hit =
      options.statistics->getTickerCount(TABLE_OPEN_PREFETCH_TAIL_HIT);

  HistogramData pre_compaction_prefetch_bytes;
  options.statistics->histogramData(COMPACTION_PREFETCH_BYTES,
                                    &pre_compaction_prefetch_bytes);
  ASSERT_EQ(pre_compaction_prefetch_bytes.count, 0);

  // commenting out the line below causes the example to work correctly
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  HistogramData post_compaction_prefetch_bytes;
  options.statistics->histogramData(COMPACTION_PREFETCH_BYTES,
                                    &post_compaction_prefetch_bytes);

  HistogramData cur_table_open_prefetch_tail_read;
  options.statistics->histogramData(TABLE_OPEN_PREFETCH_TAIL_READ_BYTES,
                                    &cur_table_open_prefetch_tail_read);
  const uint64_t cur_table_open_prefetch_tail_miss =
      options.statistics->getTickerCount(TABLE_OPEN_PREFETCH_TAIL_MISS);
  const uint64_t cur_table_open_prefetch_tail_hit =
      options.statistics->getTickerCount(TABLE_OPEN_PREFETCH_TAIL_HIT);

  // To verify prefetch during compaction input read
  if (support_prefetch && !use_direct_io) {
    ASSERT_TRUE(fs->IsPrefetchCalled());
    // To rule out false positive by the SST file tail prefetch during
    // compaction output verification
    ASSERT_GT(fs->GetPrefetchCount(), 1);
    ASSERT_EQ(0, buff_prefetch_count);
    fs->ClearPrefetchCount();
    ASSERT_EQ(post_compaction_prefetch_bytes.count, 0);
  } else {
    ASSERT_FALSE(fs->IsPrefetchCalled());
    // To rule out false positive by the SST file tail prefetch during
    // compaction output verification
    ASSERT_GT(buff_prefetch_count, 1);
    buff_prefetch_count = 0;

    ASSERT_GT(cur_table_open_prefetch_tail_read.count,
              prev_table_open_prefetch_tail_read.count);
    ASSERT_GT(cur_table_open_prefetch_tail_hit,
              prev_table_open_prefetch_tail_hit);
    ASSERT_GE(cur_table_open_prefetch_tail_miss,
              prev_table_open_prefetch_tail_miss);

    ASSERT_GT(post_compaction_prefetch_bytes.count, 0);

    // Not an exact match due to potential roundup/down for alignment
    auto expected_compaction_readahead_size =
        Options().compaction_readahead_size;
    ASSERT_LE(post_compaction_prefetch_bytes.max,
              expected_compaction_readahead_size * 1.1);
    ASSERT_GE(post_compaction_prefetch_bytes.max,
              expected_compaction_readahead_size * 0.9);
    ASSERT_LE(post_compaction_prefetch_bytes.average,
              expected_compaction_readahead_size * 1.1);
    ASSERT_GE(post_compaction_prefetch_bytes.average,
              expected_compaction_readahead_size * 0.9);
  }

  for (bool disable_io : {false, true}) {
    SCOPED_TRACE("disable_io: " + std::to_string(disable_io));
    ReadOptions ro;
    if (disable_io) {
      // When this is set on the second iteration, all blocks should be in
      // block cache
      ro.read_tier = ReadTier::kBlockCacheTier;
    }
    // count the keys
    {
      auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
      int num_keys = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        num_keys++;
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(num_keys, kNumKeys);
    }

    // To verify prefetch during user scan, when IO allowed
    if (disable_io) {
      ASSERT_FALSE(fs->IsPrefetchCalled());
      ASSERT_EQ(0, buff_prefetch_count);
    } else if (support_prefetch && !use_direct_io) {
      ASSERT_TRUE(fs->IsPrefetchCalled());
      fs->ClearPrefetchCount();
      ASSERT_EQ(0, buff_prefetch_count);
    } else {
      ASSERT_FALSE(fs->IsPrefetchCalled());
      ASSERT_GT(buff_prefetch_count, 0);
      buff_prefetch_count = 0;
    }
  }
  Close();
}

class PrefetchTailTest : public PrefetchTest {
 public:
  bool SupportPrefetch() const {
    return std::get<0>(GetParam()) &&
           test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);
  }

  bool UseDirectIO() const { return std::get<1>(GetParam()); }

  bool UseFilePrefetchBuffer() const {
    return !SupportPrefetch() || UseDirectIO();
  }

  Env* GetEnv(bool small_buffer_alignment = false) const {
    std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
        env_->GetFileSystem(), SupportPrefetch(), small_buffer_alignment);

    return new CompositeEnvWrapper(env_, fs);
  }

  void SetGenericOptions(Env* env, bool use_direct_io,
                         Options& options) override {
    PrefetchTest::SetGenericOptions(env, use_direct_io, options);
    options.statistics = CreateDBStatistics();
  }

  void SetBlockBasedTableOptions(
      BlockBasedTableOptions& table_options, bool partition_filters = true,
      uint64_t metadata_block_size =
          BlockBasedTableOptions().metadata_block_size,
      bool use_small_cache = false) {
    table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
    table_options.partition_filters = partition_filters;
    if (table_options.partition_filters) {
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    }
    table_options.metadata_block_size = metadata_block_size;

    if (use_small_cache) {
      LRUCacheOptions co;
      co.capacity = 1;
      std::shared_ptr<Cache> cache = NewLRUCache(co);
      table_options.block_cache = cache;
    }
  }

  int64_t GetNumIndexPartition() const {
    int64_t index_partition_counts = 0;
    TablePropertiesCollection all_table_props;
    assert(db_->GetPropertiesOfAllTables(&all_table_props).ok());
    for (const auto& name_and_table_props : all_table_props) {
      const auto& table_props = name_and_table_props.second;
      index_partition_counts += table_props->index_partitions;
    }
    return index_partition_counts;
  }
};

INSTANTIATE_TEST_CASE_P(PrefetchTailTest, PrefetchTailTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

TEST_P(PrefetchTailTest, Basic) {
  std::unique_ptr<Env> env(GetEnv());
  Options options;
  SetGenericOptions(env.get(), UseDirectIO(), options);

  BlockBasedTableOptions bbto;
  SetBlockBasedTableOptions(bbto);
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  Status s = TryReopen(options);
  if (UseDirectIO() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    ROCKSDB_GTEST_BYPASS("Direct IO is not supported");
    return;
  } else {
    ASSERT_OK(s);
  }

  ASSERT_OK(Put("k1", "v1"));

  HistogramData pre_flush_file_read;
  options.statistics->histogramData(FILE_READ_FLUSH_MICROS,
                                    &pre_flush_file_read);
  ASSERT_OK(Flush());
  HistogramData post_flush_file_read;
  options.statistics->histogramData(FILE_READ_FLUSH_MICROS,
                                    &post_flush_file_read);
  if (UseFilePrefetchBuffer()) {
    // `PartitionedFilterBlockReader/PartitionIndexReader::CacheDependencies()`
    // should read from the prefetched tail in file prefetch buffer instead of
    // initiating extra SST reads. Therefore `BlockBasedTable::PrefetchTail()`
    // should be the only SST read in table verification during flush.
    ASSERT_EQ(post_flush_file_read.count - pre_flush_file_read.count, 1);
  } else {
    // Without the prefetched tail in file prefetch buffer,
    // `PartitionedFilterBlockReader/PartitionIndexReader::CacheDependencies()`
    // will initiate extra SST reads
    ASSERT_GT(post_flush_file_read.count - pre_flush_file_read.count, 1);
  }
  ASSERT_OK(Put("k1", "v2"));
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  HistogramData pre_compaction_file_read;
  options.statistics->histogramData(FILE_READ_COMPACTION_MICROS,
                                    &pre_compaction_file_read);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  HistogramData post_compaction_file_read;
  options.statistics->histogramData(FILE_READ_COMPACTION_MICROS,
                                    &post_compaction_file_read);
  if (UseFilePrefetchBuffer()) {
    // `PartitionedFilterBlockReader/PartitionIndexReader::CacheDependencies()`
    // should read from the prefetched tail in file prefetch buffer instead of
    // initiating extra SST reads.
    //
    // Therefore the 3 reads are
    // (1) `ProcessKeyValueCompaction()` of input file 1
    // (2) `ProcessKeyValueCompaction()` of input file 2
    // (3) `BlockBasedTable::PrefetchTail()` of output file during table
    // verification in compaction
    ASSERT_EQ(post_compaction_file_read.count - pre_compaction_file_read.count,
              3);
  } else {
    // Without the prefetched tail in file prefetch buffer,
    // `PartitionedFilterBlockReader/PartitionIndexReader::CacheDependencies()`
    // as well as reading other parts of the tail (e.g, footer, table
    // properties..) will initiate extra SST reads
    ASSERT_GT(post_compaction_file_read.count - pre_compaction_file_read.count,
              3);
  }
  Close();
}

TEST_P(PrefetchTailTest, UpgradeToTailSizeInManifest) {
  if (!UseFilePrefetchBuffer()) {
    ROCKSDB_GTEST_BYPASS(
        "Upgrade to tail size in manifest is only relevant when RocksDB file "
        "prefetch buffer is used.");
  }
  if (UseDirectIO()) {
    ROCKSDB_GTEST_BYPASS(
        "To simplify testing logics with setting file's buffer alignment to "
        "be "
        "1, direct IO is required to be disabled.");
  }

  std::unique_ptr<Env> env(GetEnv(true /* small_buffer_alignment */));
  Options options;
  SetGenericOptions(env.get(), false /* use_direct_io*/, options);
  options.max_open_files = -1;
  options.write_buffer_size = 1024 * 1024;

  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options, false /* partition_filters */,
                            1 /* metadata_block_size*/,
                            true /* use_small_cache */);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SyncPoint::GetInstance()->EnableProcessing();
  // To simulate a pre-upgrade DB where file tail size is not recorded in
  // manifest
  SyncPoint::GetInstance()->SetCallBack(
      "FileMetaData::FileMetaData", [&](void* arg) {
        FileMetaData* meta = static_cast<FileMetaData*>(arg);
        meta->tail_size = 0;
      });

  ASSERT_OK(TryReopen(options));
  for (int i = 0; i < 10000; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
  }
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->ClearAllCallBacks();

  // To simulate a DB undergoing the upgrade where tail size to prefetch is
  // inferred to be a small number for files with no tail size recorded in
  // manifest.
  // "1" is chosen to be such number so that with `small_buffer_alignment ==
  // true` and `use_small_cache == true`, it would have caused one file read
  // per index partition during db open if the upgrade is done wrong.
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::TailPrefetchLen", [&](void* arg) {
        std::pair<size_t*, size_t*>* prefetch_off_len_pair =
            static_cast<std::pair<size_t*, size_t*>*>(arg);
        size_t* prefetch_off = prefetch_off_len_pair->first;
        size_t* tail_size = prefetch_off_len_pair->second;
        const size_t file_size = *prefetch_off + *tail_size;

        *tail_size = 1;
        *prefetch_off = file_size - (*tail_size);
      });

  ASSERT_OK(TryReopen(options));

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  HistogramData db_open_file_read;
  options.statistics->histogramData(FILE_READ_DB_OPEN_MICROS,
                                    &db_open_file_read);

  int64_t num_index_partition = GetNumIndexPartition();
  // If the upgrade is done right, db open will prefetch all the index
  // partitions at once, instead of doing one read per partition.
  // That is, together with `metadata_block_size == 1`, there will be more
  // index partitions than number of non index partitions reads.
  ASSERT_LT(db_open_file_read.count, num_index_partition);

  Close();
}

// This test verifies BlockBasedTableOptions.max_auto_readahead_size is
// configured dynamically.
TEST_P(PrefetchTest, ConfigureAutoMaxReadaheadSize) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  table_options.max_auto_readahead_size = 0;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  // DB open will create table readers unless we reduce the table cache
  // capacity. SanitizeOptions will set max_open_files to minimum of 20. Table
  // cache is allocated with max_open_files - 10 as capacity. So override
  // max_open_files to 10 so table cache capacity will become 0. This will
  // prevent file open during DB open and force the file to be opened during
  // Iteration.
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);

  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  Random rnd(309);
  int key_count = 0;
  const int num_keys_per_level = 100;
  // Level 0 : Keys in range [0, 99], Level 1:[100, 199], Level 2:[200, 299].
  for (int level = 2; level >= 0; level--) {
    key_count = level * num_keys_per_level;
    for (int i = 0; i < num_keys_per_level; ++i) {
      ASSERT_OK(Put(Key(key_count++), rnd.RandomString(500)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(level);
  }
  Close();
  std::vector<int> buff_prefetch_level_count = {0, 0, 0};
  ASSERT_OK(TryReopen(options));
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    fs->ClearPrefetchCount();
    buff_prefetch_count = 0;

    for (int level = 2; level >= 0; level--) {
      key_count = level * num_keys_per_level;
      switch (level) {
        case 0:
          // max_auto_readahead_size is set 0 so data and index blocks are not
          // prefetched.
          ASSERT_OK(db_->SetOptions(
              {{"block_based_table_factory", "{max_auto_readahead_size=0;}"}}));
          break;
        case 1:
          // max_auto_readahead_size is set less than
          // initial_auto_readahead_size. So readahead_size remains equal to
          // max_auto_readahead_size.
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{max_auto_readahead_size=4096;}"}}));
          break;
        case 2:
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{max_auto_readahead_size=65536;}"}}));
          break;
        default:
          assert(false);
      }
      ASSERT_OK(iter->status());
      ASSERT_OK(iter->Refresh());  // Update to latest mutable options

      for (int i = 0; i < num_keys_per_level; ++i) {
        iter->Seek(Key(key_count++));
        iter->Next();
      }

      buff_prefetch_level_count[level] = buff_prefetch_count;
      if (support_prefetch && !use_direct_io) {
        if (level == 0) {
          ASSERT_FALSE(fs->IsPrefetchCalled());
        } else {
          ASSERT_TRUE(fs->IsPrefetchCalled());
        }
        fs->ClearPrefetchCount();
      } else {
        ASSERT_FALSE(fs->IsPrefetchCalled());
        if (level == 0) {
          ASSERT_EQ(buff_prefetch_count, 0);
        } else {
          ASSERT_GT(buff_prefetch_count, 0);
        }
        buff_prefetch_count = 0;
      }
    }
  }

  if (!support_prefetch) {
    ASSERT_GT(buff_prefetch_level_count[1], buff_prefetch_level_count[2]);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies BlockBasedTableOptions.initial_auto_readahead_size is
// configured dynamically.
TEST_P(PrefetchTest, ConfigureInternalAutoReadaheadSize) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  table_options.initial_auto_readahead_size = 0;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  // DB open will create table readers unless we reduce the table cache
  // capacity. SanitizeOptions will set max_open_files to minimum of 20.
  // Table cache is allocated with max_open_files - 10 as capacity. So
  // override max_open_files to 10 so table cache capacity will become 0.
  // This will prevent file open during DB open and force the file to be
  // opened during Iteration.
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);

  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  Random rnd(309);
  int key_count = 0;
  const int num_keys_per_level = 100;
  // Level 0 : Keys in range [0, 99], Level 1:[100, 199], Level 2:[200, 299].
  for (int level = 2; level >= 0; level--) {
    key_count = level * num_keys_per_level;
    for (int i = 0; i < num_keys_per_level; ++i) {
      ASSERT_OK(Put(Key(key_count++), rnd.RandomString(500)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(level);
  }
  Close();

  ASSERT_OK(TryReopen(options));
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    fs->ClearPrefetchCount();
    buff_prefetch_count = 0;
    std::vector<int> buff_prefetch_level_count = {0, 0, 0};

    for (int level = 2; level >= 0; level--) {
      key_count = level * num_keys_per_level;
      switch (level) {
        case 0:
          // initial_auto_readahead_size is set 0 so data and index blocks are
          // not prefetched.
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{initial_auto_readahead_size=0;}"}}));
          break;
        case 1:
          // initial_auto_readahead_size and max_auto_readahead_size are set
          // same so readahead_size remains same.
          ASSERT_OK(db_->SetOptions({{"block_based_table_factory",
                                      "{initial_auto_readahead_size=4096;max_"
                                      "auto_readahead_size=4096;}"}}));
          break;
        case 2:
          ASSERT_OK(
              db_->SetOptions({{"block_based_table_factory",
                                "{initial_auto_readahead_size=65536;}"}}));
          break;
        default:
          assert(false);
      }
      ASSERT_OK(iter->status());
      ASSERT_OK(iter->Refresh());  // Update to latest mutable options

      for (int i = 0; i < num_keys_per_level; ++i) {
        iter->Seek(Key(key_count++));
        iter->Next();
      }
      ASSERT_OK(iter->status());

      buff_prefetch_level_count[level] = buff_prefetch_count;
      if (support_prefetch && !use_direct_io) {
        if (level == 0) {
          ASSERT_FALSE(fs->IsPrefetchCalled());
        } else {
          ASSERT_TRUE(fs->IsPrefetchCalled());
        }
        fs->ClearPrefetchCount();
      } else {
        ASSERT_FALSE(fs->IsPrefetchCalled());
        if (level == 0) {
          ASSERT_EQ(buff_prefetch_count, 0);
        } else {
          ASSERT_GT(buff_prefetch_count, 0);
        }
        buff_prefetch_count = 0;
      }
    }
    if (!support_prefetch) {
      ASSERT_GT(buff_prefetch_level_count[1], buff_prefetch_level_count[2]);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies BlockBasedTableOptions.num_file_reads_for_auto_readahead
// is configured dynamically.
TEST_P(PrefetchTest, ConfigureNumFilesReadsForReadaheadSize) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  const int kNumKeys = 2000;
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  table_options.num_file_reads_for_auto_readahead = 0;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  Close();
  ASSERT_OK(TryReopen(options));

  fs->ClearPrefetchCount();
  buff_prefetch_count = 0;

  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. It will prefetch the data block at the first seek since
     * num_file_reads_for_auto_readahead = 0. Data Block size is nearly 4076
     * so readahead will fetch 8 * 1024 data more initially (2 more data
     * blocks).
     */
    iter->Seek(BuildKey(0));  // Prefetch data + index block since
                              // num_file_reads_for_auto_readahead = 0.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1011));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));  // In buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));  // In buffer
    ASSERT_TRUE(iter->Valid());
    // Missed 2 blocks but they are already in buffer so no reset.
    iter->Seek(BuildKey(103));  // Already in buffer.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));  // Prefetch Data.
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 4);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 4);
      buff_prefetch_count = 0;
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies the basic functionality of implicit autoreadahead:
// - Enable implicit autoreadahead and prefetch only if sequential blocks are
//   read,
// - If data is already in buffer and few blocks are not requested to read,
//   don't reset,
// - If data blocks are sequential during read after enabling implicit
//   autoreadahead, reset readahead parameters.
TEST_P(PrefetchTest, PrefetchWhenReseek) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  const int kNumKeys = 2000;
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  fs->ClearPrefetchCount();
  buff_prefetch_count = 0;

  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. After 2 sequential reads it will prefetch the data block.
     * Data Block size is nearly 4076 so readahead will fetch 8 * 1024 data
     * more initially (2 more data blocks).
     */
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    // Missed 2 blocks but they are already in buffer so no reset.
    iter->Seek(BuildKey(103));  // Already in buffer.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 3);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 3);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reseek keys from  non sequential data blocks within same partitioned
     * index. buff_prefetch_count will be 0 in that case.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1048));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 0);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 0);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reseek keys from Single Data Block.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(10));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(100));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 0);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 0);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reseek keys from  sequential data blocks to set implicit auto readahead
     * and prefetch data but after that iterate over different (non
     * sequential) data blocks which won't prefetch any data further. So
     * buff_prefetch_count will be 1 for the first one.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // This iteration will prefetch buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1008));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(996));  // Reseek won't prefetch any data and
                                // readahead_size will be initialized to 8*1024.
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(992));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(989));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 1);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 1);
      buff_prefetch_count = 0;
    }

    // Read sequentially to confirm readahead_size is reset to initial value
    // (2 more data blocks)
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1022));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1026));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(103));  // Prefetch Data
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 2);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 2);
      buff_prefetch_count = 0;
    }
  }
  {
    /* Reseek keys from sequential partitioned index block. Since partitioned
     * index fetch are sequential, buff_prefetch_count will be 1.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1167));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1334));  // This iteration will prefetch buffer
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1499));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1667));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1847));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1999));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 1);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 1);
      buff_prefetch_count = 0;
    }
  }
  {
    /*
     * Reseek over different keys from different blocks. buff_prefetch_count
     * is set 0.
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    int i = 0;
    int j = 1000;
    do {
      iter->Seek(BuildKey(i));
      if (!iter->Valid()) {
        ASSERT_OK(iter->status());
        break;
      }
      i = i + 100;
      iter->Seek(BuildKey(j));
      j = j + 100;
    } while (i < 1000 && j < kNumKeys && iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 0);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 0);
      buff_prefetch_count = 0;
    }
  }
  {
    /* Iterates sequentially over all keys. It will prefetch the buffer.*/
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    }
    ASSERT_OK(iter->status());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 13);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 13);
      buff_prefetch_count = 0;
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies the functionality of implicit autoreadahead when caching
// is enabled:
// - If data is already in buffer and few blocks are not requested to read,
//   don't reset,
// - If block was eligible for prefetching/in buffer but found in cache, don't
// prefetch and reset.
TEST_P(PrefetchTest, PrefetchWhenReseekwithCache) {
  // First param is if the mockFS support_prefetch or not
  bool support_prefetch =
      std::get<0>(GetParam()) &&
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);

  const int kNumKeys = 2000;
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), support_prefetch);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  // Second param is if directIO is enabled or not
  bool use_direct_io = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);  // 8MB
  table_options.block_cache = cache;
  table_options.no_block_cache = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  fs->ClearPrefetchCount();
  buff_prefetch_count = 0;

  {
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. After 2 sequential reads it will prefetch the data block.
     * Data Block size is nearly 4076 so readahead will fetch 8 * 1024 data
     * more initially (2 more data blocks).
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    // Warm up the cache
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 1);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 1);
      buff_prefetch_count = 0;
    }
  }
  {
    // After caching, blocks will be read from cache (Sequential blocks)
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    iter->Seek(BuildKey(0));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1000));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1004));  // Prefetch data (not in cache).
    ASSERT_TRUE(iter->Valid());
    // Missed one sequential block but next is in already in buffer so
    // readahead will not be reset.
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    // Prefetch data but blocks are in cache so no prefetch and reset.
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1022));
    ASSERT_TRUE(iter->Valid());
    // Prefetch data with readahead_size = 4 blocks.
    iter->Seek(BuildKey(1026));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(103));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1033));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1037));
    ASSERT_TRUE(iter->Valid());

    if (support_prefetch && !use_direct_io) {
      ASSERT_EQ(fs->GetPrefetchCount(), 3);
      fs->ClearPrefetchCount();
    } else {
      ASSERT_EQ(buff_prefetch_count, 2);
      buff_prefetch_count = 0;
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

TEST_P(PrefetchTest, PrefetchWithBlockLookupAutoTuneTest) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(FileSystem::Default(), false);

  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), /*use_direct_io=*/false, options);
  options.statistics = CreateDBStatistics();
  const std::string prefix = "my_key_";
  options.prefix_extractor.reset(NewFixedPrefixTransform(prefix.size()));
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  ASSERT_OK(s);

  Random rnd(309);
  WriteBatch batch;

  // Create the DB with keys from "my_key_aaaaaaaaaa" to "my_key_zzzzzzzzzz"
  for (int i = 0; i < 26; i++) {
    std::string key = prefix;

    for (int j = 0; j < 10; j++) {
      key += char('a' + i);
      ASSERT_OK(batch.Put(key, rnd.RandomString(1000)));
    }
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = prefix + "a";

  std::string end_key = prefix;
  for (int j = 0; j < 10; j++) {
    end_key += char('a' + 25);
  }

  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  // Try with different num_file_reads_for_auto_readahead from 0 to 3.
  for (size_t i = 0; i < 3; i++) {
    std::shared_ptr<Cache> cache = NewLRUCache(1024 * 1024, 2);
    table_options.block_cache = cache;
    table_options.no_block_cache = false;
    table_options.num_file_reads_for_auto_readahead = i;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    s = TryReopen(options);
    ASSERT_OK(s);

    // Warm up the cache.
    {
      auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));

      iter->Seek(prefix + "bbb");
      ASSERT_TRUE(iter->Valid());

      iter->Seek(prefix + "ccccccccc");
      ASSERT_TRUE(iter->Valid());

      iter->Seek(prefix + "ddd");
      ASSERT_TRUE(iter->Valid());

      iter->Seek(prefix + "ddddddd");
      ASSERT_TRUE(iter->Valid());

      iter->Seek(prefix + "e");
      ASSERT_TRUE(iter->Valid());

      iter->Seek(prefix + "eeeee");
      ASSERT_TRUE(iter->Valid());

      iter->Seek(prefix + "eeeeeeeee");
      ASSERT_TRUE(iter->Valid());
    }

    ReadOptions ropts;
    ReadOptions cmp_ro;

    if (std::get<0>(GetParam())) {
      ropts.readahead_size = cmp_ro.readahead_size = 32768;
    }

    if (std::get<1>(GetParam())) {
      ropts.async_io = true;
    }

    // With and without tuning readahead_size.
    ropts.auto_readahead_size = true;
    cmp_ro.auto_readahead_size = false;
    ASSERT_OK(options.statistics->Reset());
    // Seek with a upper bound
    const std::string seek_key_str = prefix + "aaa";
    const Slice seek_key(seek_key_str);
    const std::string ub_str = prefix + "uuu";
    const Slice ub(ub_str);
    VerifyScan(ropts /* iter_ro */, cmp_ro /* cmp_iter_ro */,
               &seek_key /* seek_key */, &ub /* iterate_upper_bound */,
               false /* prefix_same_as_start */);

    // Seek with a new seek key and upper bound
    const std::string seek_key_new_str = prefix + "v";
    const Slice seek_key_new(seek_key_new_str);
    const std::string ub_new_str = prefix + "y";
    const Slice ub_new(ub_new_str);
    VerifyScan(ropts /* iter_ro */, cmp_ro /* cmp_iter_ro */,
               &seek_key_new /* seek_key */, &ub_new /* iterate_upper_bound */,
               false /* prefix_same_as_start */);

    // Seek with no upper bound, prefix_same_as_start = true
    VerifyScan(ropts /* iter_ro */, cmp_ro /* cmp_iter_ro */,
               &seek_key /* seek_key */, nullptr /* iterate_upper_bound */,
               true /* prefix_same_as_start */);
    Close();
  }
}

TEST_F(PrefetchTest, PrefetchWithBlockLookupAutoTuneWithPrev) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  // First param is if the mockFS support_prefetch or not
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(FileSystem::Default(), false);

  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), /*use_direct_io=*/false, options);
  options.statistics = CreateDBStatistics();
  const std::string prefix = "my_key_";
  options.prefix_extractor.reset(NewFixedPrefixTransform(prefix.size()));
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  std::shared_ptr<Cache> cache = NewLRUCache(1024 * 1024, 2);
  table_options.block_cache = cache;
  table_options.no_block_cache = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  ASSERT_OK(s);

  Random rnd(309);
  WriteBatch batch;

  for (int i = 0; i < 26; i++) {
    std::string key = prefix;

    for (int j = 0; j < 10; j++) {
      key += char('a' + i);
      ASSERT_OK(batch.Put(key, rnd.RandomString(1000)));
    }
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = prefix + "a";

  std::string end_key = prefix;
  for (int j = 0; j < 10; j++) {
    end_key += char('a' + 25);
  }

  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  ReadOptions ropts;
  ropts.auto_readahead_size = true;
  ReadOptions cmp_readopts = ropts;
  cmp_readopts.auto_readahead_size = false;

  const std::string seek_key_str = prefix + "bbb";
  const Slice seek_key(seek_key_str);
  const std::string ub_key = prefix + "uuu";
  const Slice ub(ub_key);

  VerifySeekPrevSeek(ropts /* iter_ro */, cmp_readopts /* cmp_iter_ro */,
                     &seek_key /* seek_key */, &ub /* iterate_upper_bound */,
                     false /* prefix_same_as_start */);

  VerifySeekPrevSeek(ropts /* iter_ro */, cmp_readopts /* cmp_iter_ro */,
                     &seek_key /* seek_key */,
                     nullptr /* iterate_upper_bound */,
                     true /* prefix_same_as_start */);
  Close();
}

// A `MockFS` that advertises async IO support. `ArenaWrappedDBIter` clears
// `ReadOptions::async_io` unless the file system advertises it, so without this
// the `async_io` test variant would run the sync path.
class AsyncIOMockFS : public MockFS {
 public:
  using MockFS::MockFS;

  void SupportedOps(int64_t& supported_ops) override {
    MockFS::SupportedOps(supported_ops);
    supported_ops |= (1 << FSSupportedOps::kAsyncIO);
  }
};

class PrefetchTrimReadaheadTestParam
    : public DBTestBase,
      public ::testing::WithParamInterface<
          std::tuple<BlockBasedTableOptions::IndexShorteningMode, bool>> {
 public:
  const std::string kPrefix = "a_prefix_";
  Random rnd = Random(309);

  PrefetchTrimReadaheadTestParam()
      : DBTestBase("prefetch_trim_readahead_test_param", true) {}
  virtual void SetGenericOptions(Env* env, Options& options) {
    options = CurrentOptions();
    options.env = env;
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();

    // To make all the data bocks fit in one file for testing purpose
    options.write_buffer_size = 1024 * 1024 * 1024;
    options.prefix_extractor.reset(NewFixedPrefixTransform(kPrefix.size()));
  }

  void SetBlockBasedTableOptions(BlockBasedTableOptions& table_options) {
    table_options.no_block_cache = false;
    table_options.index_shortening = std::get<0>(GetParam());

    // To force keys with different prefixes are in different data blocks of the
    // file for testing purpose
    table_options.block_size = 1;
    table_options.flush_block_policy_factory.reset(
        new FlushBlockBySizePolicyFactory());
  }

  void RunSeekToFirstAfterSeekIgnoresStalePrefix(bool async_io) {
    const bool auto_readahead_size = std::get<1>(GetParam());

    std::shared_ptr<MockFS> fs =
        async_io ? std::make_shared<AsyncIOMockFS>(
                       FileSystem::Default(), false /* support_prefetch */,
                       true /* small_buffer_alignment */)
                 : std::make_shared<MockFS>(FileSystem::Default(),
                                            false /* support_prefetch */,
                                            true /* small_buffer_alignment */);
    std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
    // Close the DB before the local `env` is destroyed, even if an assertion
    // below exits early: ~DBTestBase() closes it after `env` is already gone.
    Defer close_db_on_exit([this]() { Close(); });

    Options options;
    SetGenericOptions(env.get(), options);
    BlockBasedTableOptions table_options;
    SetBlockBasedTableOptions(table_options);
    // An explicit cache so both scans below start equally cold: a warm cache
    // serves every block without touching the file, which suppresses readahead
    // and would make the comparison meaningless.
    std::shared_ptr<Cache> block_cache = NewLRUCache(8 * 1024 * 1024);
    table_options.block_cache = block_cache;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    ASSERT_OK(TryReopen(options));

    constexpr int kNumKeysUnderPrefix = 40;
    constexpr int kNumOtherPrefixes = 10;
    WriteBatch batch;
    for (int i = 0; i < kNumKeysUnderPrefix; i++) {
      ASSERT_OK(batch.Put(kPrefix + Key(i), rnd.RandomString(100)));
    }
    // Keys under other prefixes, so that a trim against a prefix other than
    // "a_prefix_" has somewhere to stop short of.
    for (int i = 0; i < kNumOtherPrefixes; i++) {
      const std::string diff_prefix =
          std::string(1, char('c' + i)) + kPrefix.substr(1);
      ASSERT_OK(batch.Put(diff_prefix + Key(i), rnd.RandomString(100)));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(db_->Flush(FlushOptions()));

    // The file must stay in L0 so the merging iterator holds and reuses this
    // table iterator across Seek()/SeekToFirst() instead of rebuilding it,
    // which would discard the stale member and hide the bug.
    ASSERT_EQ(1, NumTableFilesAtLevel(0));

    ReadOptions ro;
    ro.prefix_same_as_start = true;
    ro.auto_readahead_size = auto_readahead_size;
    ro.async_io = async_io;
    // Small enough that the scan exhausts the buffer and re-prefetches, so the
    // trimming prefix is consulted repeatedly rather than once for the file.
    ro.readahead_size = 1024;

    // The `async_io` variant reaches the key-loss form of the bug only because
    // `FilePrefetchBuffer` has a second buffer, prefetched with
    // `read_curr_block=false`. Count that path so this variant cannot silently
    // decay into a duplicate of the synchronous one.
    int extra_prefetch_buff_cnt = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::PrefetchAsync:ExtraPrefetching",
        [&extra_prefetch_buff_cnt](void*) { ++extra_prefetch_buff_cnt; });
    SyncPoint::GetInstance()->EnableProcessing();
    Defer disable_sync_point([]() {
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
    });

    auto count_from_seek_to_first = [this](Iterator* iter) {
      int num_keys = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        EXPECT_EQ(kPrefix, iter->key().ToString().substr(0, kPrefix.size()));
        ++num_keys;
      }
      EXPECT_OK(iter->status());
      return num_keys;
    };

    // Control: SeekToFirst() on an iterator with nothing stale, cold cache.
    block_cache->EraseUnRefEntries();
    ASSERT_OK(options.statistics->Reset());
    int num_keys_control = 0;
    {
      auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
      num_keys_control = count_from_seek_to_first(iter.get());
    }
    const uint64_t trimmed_control =
        options.statistics->getTickerCount(READAHEAD_TRIMMED);

    // The same scan from an equally cold cache, differing only in that it
    // follows a Seek() past the last key, which assigns the trimming prefix.
    block_cache->EraseUnRefEntries();
    ASSERT_OK(options.statistics->Reset());
    extra_prefetch_buff_cnt = 0;
    int num_keys_after_seek = 0;
    {
      auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
      const std::string seek_key = std::string(1, 'z') + kPrefix.substr(1);
      iter->Seek(seek_key);
      ASSERT_OK(iter->status());
      ASSERT_FALSE(iter->Valid());
      // That Seek() must read no data block, or this scan does not start as
      // cold as the control and the two trim counts are not comparable.
      ASSERT_EQ(0, options.statistics->getTickerCount(BLOCK_CACHE_DATA_MISS));
      ASSERT_EQ(0, options.statistics->getTickerCount(READAHEAD_TRIMMED));

      num_keys_after_seek = count_from_seek_to_first(iter.get());
    }
    const uint64_t trimmed_after_seek =
        options.statistics->getTickerCount(READAHEAD_TRIMMED);

    ASSERT_EQ(kNumKeysUnderPrefix, num_keys_control);
    // Pin the trim counter down on the control side so the comparison below
    // cannot pass vacuously: without `auto_readahead_size` nothing may be
    // trimmed at all, and with it the trimming path has to be live.
    if (auto_readahead_size) {
      ASSERT_GT(trimmed_control, 0);
    } else {
      ASSERT_EQ(0, trimmed_control);
    }

    // A preceding Seek() must not change what SeekToFirst() does: this scan
    // visits the same blocks, so it must return the same keys and trim exactly
    // as much as the control -- no more. (The control's trims come from
    // block-cache hits and index bounds, which a stale prefix does not affect.)
    ASSERT_EQ(num_keys_control, num_keys_after_seek);
    ASSERT_EQ(trimmed_control, trimmed_after_seek);

    // Last, so the two assertions above stay the ones that report a
    // regression: the `async_io` variant is only worth having while it reaches
    // the extra-buffer prefetch path, and the synchronous one must not.
    if (async_io) {
      ASSERT_GT(extra_prefetch_buff_cnt, 0);
    } else {
      ASSERT_EQ(0, extra_prefetch_buff_cnt);
    }
  }
};

INSTANTIATE_TEST_CASE_P(
    PrefetchTrimReadaheadTestParam, PrefetchTrimReadaheadTestParam,
    ::testing::Combine(
        // Params are as follows -
        // Param 0 - TableOptions::index_shortening
        // Param 2 - ReadOptions::auto_readahead_size
        ::testing::Values(
            BlockBasedTableOptions::IndexShorteningMode::kNoShortening,
            BlockBasedTableOptions::IndexShorteningMode::kShortenSeparators,
            BlockBasedTableOptions::IndexShorteningMode::
                kShortenSeparatorsAndSuccessor),
        ::testing::Bool()));

TEST_P(PrefetchTrimReadaheadTestParam, PrefixSameAsStart) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  const bool auto_readahead_size = std::get<1>(GetParam());

  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), false /* support_prefetch */,
      true /* small_buffer_alignment */);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), options);
  BlockBasedTableOptions table_optoins;
  SetBlockBasedTableOptions(table_optoins);
  options.table_factory.reset(NewBlockBasedTableFactory(table_optoins));

  Status s = TryReopen(options);
  ASSERT_OK(s);

  // To create a DB with data block layout (denoted as "[...]" below ) as the
  // following:
  // ["a_prefix_0": random value]
  // ["a_prefix_1": random value]
  // ...
  // ["a_prefix_9": random value]
  // ["c_prefix_0": random value]
  // ["d_prefix_1": random value]
  // ...
  // ["l_prefix_9": random value]
  //
  // We want to verify keys not with prefix "a_prefix_" are not prefetched due
  // to trimming
  WriteBatch prefix_batch;
  for (int i = 0; i < 10; i++) {
    std::string key = kPrefix + std::to_string(i);
    ASSERT_OK(prefix_batch.Put(key, rnd.RandomString(100)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &prefix_batch));

  WriteBatch diff_prefix_batch;
  for (int i = 0; i < 10; i++) {
    std::string diff_prefix = std::string(1, char('c' + i)) + kPrefix.substr(1);
    std::string key = diff_prefix + std::to_string(i);
    ASSERT_OK(diff_prefix_batch.Put(key, rnd.RandomString(100)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &diff_prefix_batch));

  ASSERT_OK(db_->Flush(FlushOptions()));

  // To verify readahead is trimmed based on prefix by checking the counter
  // READAHEAD_TRIMMED
  ReadOptions ro;
  ro.prefix_same_as_start = true;
  ro.auto_readahead_size = auto_readahead_size;
  // Set a large readahead size to introduce readahead waste when without
  // trimming based on prefix
  ro.readahead_size = 1024 * 1024 * 1024;

  ASSERT_OK(options.statistics->Reset());
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    for (iter->Seek(kPrefix); iter->status().ok() && iter->Valid();
         iter->Next()) {
    }
  }

  auto readahead_trimmed =
      options.statistics->getTickerCount(READAHEAD_TRIMMED);

  if (auto_readahead_size) {
    ASSERT_GT(readahead_trimmed, 0);
  } else {
    ASSERT_EQ(readahead_trimmed, 0);
  }
  Close();
}

// Regression test: a `SeekToFirst()` must not trim readahead against a prefix
// left behind by an earlier `Seek()` on the same iterator. Under
// `prefix_same_as_start`, `DBIter::SeekToFirst()` derives its prefix from the
// first key it finds, so an earlier seek key describes a key range this
// positioning is not scanning, and trimming against it defeats readahead for
// the whole scan.
//
// The key count is asserted as well as the trim counter because a mis-trim can
// also end the scan early and drop keys, which the `async_io` variant below
// exhibits and explains.
TEST_P(PrefetchTrimReadaheadTestParam, SeekToFirstAfterSeekIgnoresStalePrefix) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  RunSeekToFirstAfterSeekIgnoresStalePrefix(false /* async_io */);
}

// With `async_io`, `FilePrefetchBuffer` prefetches into extra buffers with
// `read_curr_block=false`, which skips the early return in
// `BlockCacheLookupForReadAheadSize()` and lets a mis-trim escalate into
// `is_index_out_of_bound_` -- ending the scan early and silently dropping keys
// rather than only slowing it down.
TEST_P(PrefetchTrimReadaheadTestParam,
       SeekToFirstAfterSeekIgnoresStalePrefixAsyncIO) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  RunSeekToFirstAfterSeekIgnoresStalePrefix(true /* async_io */);
}

TEST_P(PrefetchTrimReadaheadTestParam, IterateUpperBoundAtEndOfIndex) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  const bool auto_readahead_size = std::get<1>(GetParam());

  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), false /* support_prefetch */,
      true /* small_buffer_alignment */);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options;
  SetGenericOptions(env.get(), options);
  options.prefix_extractor.reset();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  ASSERT_OK(TryReopen(options));

  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "key" + std::to_string(i),
                       rnd.RandomString(100)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions ro;
  ro.async_io = true;
  ro.auto_readahead_size = auto_readahead_size;
  ro.readahead_size = 1024 * 1024;
  const Slice upper_bound("keyz");
  ro.iterate_upper_bound = &upper_bound;

  ASSERT_OK(options.statistics->Reset());
  int num_keys = 0;
  Status iter_status;
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    for (iter->Seek("key0"); iter->Valid(); iter->Next()) {
      ++num_keys;
    }
    iter_status = iter->status();
  }
  auto readahead_trimmed =
      options.statistics->getTickerCount(READAHEAD_TRIMMED);

  Close();
  ASSERT_OK(iter_status);
  ASSERT_EQ(num_keys, 64);
  if (auto_readahead_size) {
    ASSERT_GT(readahead_trimmed, 0);
  } else {
    ASSERT_EQ(readahead_trimmed, 0);
  }
}

// This test verifies the functionality of ReadOptions.adaptive_readahead.
TEST_P(PrefetchTest, DBIterLevelReadAhead) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  bool is_adaptive_readahead = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  int total_keys = 0;
  for (int j = 0; j < 5; j++) {
    for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
      total_keys++;
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);
  int buff_prefetch_count = 0;
  int readahead_carry_over_count = 0;
  int num_sst_files = NumTableFilesAtLevel(2);
  size_t current_readahead_size = 0;

  // Test - Iterate over the keys sequentially.
  {
    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::Prefetch:Start",
        [&](void*) { buff_prefetch_count++; });

    // The callback checks, since reads are sequential, readahead_size doesn't
    // start from 8KB when iterator moves to next file and its called
    // num_sst_files-1 times (excluding for first file).
    SyncPoint::GetInstance()->SetCallBack(
        "BlockPrefetcher::SetReadaheadState", [&](void* arg) {
          readahead_carry_over_count++;
          size_t readahead_size = *static_cast<size_t*>(arg);
          if (readahead_carry_over_count) {
            ASSERT_GT(readahead_size, 8 * 1024);
          }
        });

    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::TryReadFromCache", [&](void* arg) {
          current_readahead_size = *static_cast<size_t*>(arg);
          ASSERT_GT(current_readahead_size, 0);
        });

    SyncPoint::GetInstance()->EnableProcessing();

    ReadOptions ro;
    if (is_adaptive_readahead) {
      ro.adaptive_readahead = true;
    }

    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(num_keys, total_keys);

    // For index and data blocks.
    if (is_adaptive_readahead) {
      ASSERT_EQ(readahead_carry_over_count, 2 * (num_sst_files - 1));
    } else {
      ASSERT_GT(buff_prefetch_count, 0);
      ASSERT_EQ(readahead_carry_over_count, 0);
    }

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  Close();
}

// This test verifies the functionality of ReadOptions.adaptive_readahead when
// async_io is enabled.
TEST_P(PrefetchTest, DBIterLevelReadAheadWithAsyncIO) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(FileSystem::Default(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  bool is_adaptive_readahead = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  int total_keys = 0;
  for (int j = 0; j < 5; j++) {
    for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
      total_keys++;
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);
  int buff_prefetch_count = 0;
  int readahead_carry_over_count = 0;
  int num_sst_files = NumTableFilesAtLevel(2);
  size_t current_readahead_size = 0;
  bool read_async_called = false;

  // Test - Iterate over the keys sequentially.
  {
    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::Prefetch:Start",
        [&](void*) { buff_prefetch_count++; });

    SyncPoint::GetInstance()->SetCallBack(
        "UpdateResults::io_uring_result",
        [&](void* /*arg*/) { read_async_called = true; });

    // The callback checks, since reads are sequential, readahead_size doesn't
    // start from 8KB when iterator moves to next file and its called
    // num_sst_files-1 times (excluding for first file).
    SyncPoint::GetInstance()->SetCallBack(
        "BlockPrefetcher::SetReadaheadState", [&](void* arg) {
          readahead_carry_over_count++;
          size_t readahead_size = *static_cast<size_t*>(arg);
          if (readahead_carry_over_count) {
            ASSERT_GT(readahead_size, 8 * 1024);
          }
        });

    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::TryReadFromCache", [&](void* arg) {
          current_readahead_size = *static_cast<size_t*>(arg);
          ASSERT_GT(current_readahead_size, 0);
        });

    SyncPoint::GetInstance()->EnableProcessing();

    ReadOptions ro;
    if (is_adaptive_readahead) {
      ro.adaptive_readahead = true;
    }
    ro.async_io = true;

    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(num_keys, total_keys);

    // For index and data blocks.
    if (is_adaptive_readahead) {
      ASSERT_EQ(readahead_carry_over_count, 2 * (num_sst_files - 1));
    } else {
      ASSERT_EQ(readahead_carry_over_count, 0);
    }

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      // Not all platforms support iouring. In that case, ReadAsync in posix
      // won't submit async requests.
      if (read_async_called) {
        ASSERT_GT(buff_prefetch_count, 0);
        ASSERT_GT(async_read_bytes.count, 0);
      } else {
        ASSERT_GT(buff_prefetch_count, 0);
        ASSERT_EQ(async_read_bytes.count, 0);
      }
    }

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  Close();
}

TEST_P(PrefetchTest, AvoidBlockCacheLookupTwice) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  bool async_io = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);  // 8MB
  table_options.block_cache = cache;
  table_options.no_block_cache = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  // Write to DB.
  {
    WriteBatch batch;
    Random rnd(309);
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));

    std::string start_key = BuildKey(0);
    std::string end_key = BuildKey(kNumKeys - 1);
    Slice least(start_key.data(), start_key.size());
    Slice greatest(end_key.data(), end_key.size());

    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));
  }

  ReadOptions ro;
  ro.async_io = async_io;
  // Iterate over the keys.
  {
    // Each block contains around 4 keys.
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    ASSERT_OK(options.statistics->Reset());

    iter->Seek(BuildKey(99));  // Prefetch data because of seek parallelization.
    ASSERT_TRUE(iter->Valid());

    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOCK_CACHE_DATA_MISS),
              1);
  }

  Close();
}

TEST_P(PrefetchTest, DBIterAsyncIONoIOUring) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 1000;
  // Set options
  bool use_direct_io = std::get<0>(GetParam());
  bool is_adaptive_readahead = std::get<1>(GetParam());

  Options options;
  SetGenericOptions(Env::Default(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  enable_io_uring = false;
  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    enable_io_uring = true;
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  int total_keys = 0;
  for (int j = 0; j < 5; j++) {
    for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
      total_keys++;
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);

  // Test - Iterate over the keys sequentially.
  {
    ReadOptions ro;
    if (is_adaptive_readahead) {
      ro.adaptive_readahead = true;
    }
    ro.async_io = true;

    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(num_keys, total_keys);

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      ASSERT_EQ(async_read_bytes.count, 0);
      ASSERT_EQ(options.statistics->getTickerCount(READ_ASYNC_MICROS), 0);
    }
  }

  {
    ReadOptions ro;
    if (is_adaptive_readahead) {
      ro.adaptive_readahead = true;
    }
    ro.async_io = true;
    ro.tailing = true;

    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(num_keys, total_keys);

    // Check stats to make sure async prefetch is done.
    {
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      ASSERT_EQ(async_read_bytes.count, 0);
      ASSERT_EQ(options.statistics->getTickerCount(READ_ASYNC_MICROS), 0);
    }
  }
  Close();

  enable_io_uring = true;
}

class PrefetchTest1 : public DBTestBase,
                      public ::testing::WithParamInterface<bool> {
 public:
  PrefetchTest1() : DBTestBase("prefetch_test1", true) {}

  virtual void SetGenericOptions(Env* env, bool use_direct_io,
                                 Options& options) {
    options = CurrentOptions();
    options.write_buffer_size = 1024;
    options.create_if_missing = true;
    options.compression = kNoCompression;
    options.env = env;
    options.disable_auto_compactions = true;
    if (use_direct_io) {
      options.use_direct_reads = true;
      options.use_direct_io_for_flush_and_compaction = true;
    }
  }

  void SetBlockBasedTableOptions(BlockBasedTableOptions& table_options) {
    table_options.no_block_cache = true;
    table_options.cache_index_and_filter_blocks = false;
    table_options.metadata_block_size = 1024;
    table_options.index_type =
        BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  }
};

INSTANTIATE_TEST_CASE_P(PrefetchTest1, PrefetchTest1, ::testing::Bool());

TEST_P(PrefetchTest1, SeekWithExtraPrefetchAsyncIO) {
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options;
  SetGenericOptions(env.get(), GetParam(), options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (GetParam() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));
  Close();
  int buff_prefetch_count = 0;
  int extra_prefetch_buff_cnt = 0;
  for (size_t i = 0; i < 3; i++) {
    table_options.num_file_reads_for_auto_readahead = i;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    s = TryReopen(options);
    ASSERT_OK(s);

    buff_prefetch_count = 0;
    extra_prefetch_buff_cnt = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::PrefetchAsync:ExtraPrefetching",
        [&](void*) { extra_prefetch_buff_cnt++; });

    SyncPoint::GetInstance()->SetCallBack(
        "FilePrefetchBuffer::Prefetch:Start",
        [&](void*) { buff_prefetch_count++; });

    SyncPoint::GetInstance()->EnableProcessing();

    ReadOptions ro;
    ro.async_io = true;
    {
      auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
      // First Seek
      iter->Seek(BuildKey(
          0));  // Prefetch data on seek because of seek parallelization.
      ASSERT_TRUE(iter->Valid());

      // Do extra prefetching in Seek only if
      // num_file_reads_for_auto_readahead = 0.
      ASSERT_EQ(extra_prefetch_buff_cnt, (i == 0 ? 1 : 0));
      // buff_prefetch_count is 2 because of index block when
      // num_file_reads_for_auto_readahead = 0.
      // If num_file_reads_for_auto_readahead > 0, index block isn't
      // prefetched.
      ASSERT_EQ(buff_prefetch_count, i == 0 ? 2 : 1);

      extra_prefetch_buff_cnt = 0;
      buff_prefetch_count = 0;
      // Reset all values of FilePrefetchBuffer on new seek.
      iter->Seek(
          BuildKey(22));  // Prefetch data because of seek parallelization.
      ASSERT_TRUE(iter->Valid());
      // Do extra prefetching in Seek only if
      // num_file_reads_for_auto_readahead = 0.
      ASSERT_EQ(extra_prefetch_buff_cnt, (i == 0 ? 1 : 0));
      ASSERT_EQ(buff_prefetch_count, 1);

      extra_prefetch_buff_cnt = 0;
      buff_prefetch_count = 0;
      // Reset all values of FilePrefetchBuffer on new seek.
      iter->Seek(
          BuildKey(33));  // Prefetch data because of seek parallelization.
      ASSERT_TRUE(iter->Valid());
      // Do extra prefetching in Seek only if
      // num_file_reads_for_auto_readahead = 0.
      ASSERT_EQ(extra_prefetch_buff_cnt, (i == 0 ? 1 : 0));
      ASSERT_EQ(buff_prefetch_count, 1);
    }
    Close();
  }
}

// This test verifies the functionality of ReadOptions.adaptive_readahead when
// reads are not sequential.
TEST_P(PrefetchTest1, NonSequentialReadsWithAdaptiveReadahead) {
  const int kNumKeys = 1000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options;
  SetGenericOptions(env.get(), GetParam(), options);
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (GetParam() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int j = 0; j < 5; j++) {
    for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
      ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);

  int buff_prefetch_count = 0;
  int set_readahead = 0;
  size_t readahead_size = 0;

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->SetCallBack(
      "BlockPrefetcher::SetReadaheadState",
      [&](void* /*arg*/) { set_readahead++; });
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::TryReadFromCache",
      [&](void* arg) { readahead_size = *static_cast<size_t*>(arg); });

  SyncPoint::GetInstance()->EnableProcessing();

  {
    // Iterate until prefetch is done.
    ReadOptions ro;
    ro.adaptive_readahead = true;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());

    while (iter->Valid() && buff_prefetch_count == 0) {
      iter->Next();
    }

    ASSERT_EQ(readahead_size, 8 * 1024);
    ASSERT_EQ(buff_prefetch_count, 1);
    ASSERT_EQ(set_readahead, 0);
    buff_prefetch_count = 0;

    // Move to last file and check readahead size fallbacks to 8KB. So next
    // readahead size after prefetch should be 8 * 1024;
    iter->Seek(BuildKey(4004));
    ASSERT_TRUE(iter->Valid());

    while (iter->Valid() && buff_prefetch_count == 0) {
      iter->Next();
    }

    ASSERT_EQ(readahead_size, 8 * 1024);
    ASSERT_EQ(set_readahead, 0);
    ASSERT_EQ(buff_prefetch_count, 1);
  }
  Close();
}

// This test verifies the functionality of adaptive_readaheadsize with cache
// and if block is found in cache, decrease the readahead_size if
// - its enabled internally by RocksDB (implicit_auto_readahead_) and,
// - readahead_size is greater than 0 and,
// - the block would have called prefetch API if not found in cache for
//   which conditions are:
//   - few/no bytes are in buffer and,
//   - block is sequential with the previous read and,
//   - num_file_reads_ + 1 (including this read) >
//   num_file_reads_for_auto_readahead_
TEST_P(PrefetchTest1, DecreaseReadAheadIfInCache) {
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs =
      std::make_shared<MockFS>(env_->GetFileSystem(), false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options;
  SetGenericOptions(env.get(), GetParam(), options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);  // 8MB
  table_options.block_cache = cache;
  table_options.no_block_cache = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (GetParam() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  int buff_prefetch_count = 0;
  size_t current_readahead_size = 0;
  size_t expected_current_readahead_size = 8 * 1024;
  size_t decrease_readahead_size = 8 * 1024;

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::TryReadFromCache",
      [&](void* arg) { current_readahead_size = *static_cast<size_t*>(arg); });

  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions ro;
  ro.adaptive_readahead = true;
  {
    /*
     * Reseek keys from sequential Data Blocks within same partitioned
     * index. After 2 sequential reads it will prefetch the data block.
     * Data Block size is nearly 4076 so readahead will fetch 8 * 1024 data
     * more initially (2 more data blocks).
     */
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    // Warm up the cache
    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    buff_prefetch_count = 0;
  }

  {
    ASSERT_OK(options.statistics->Reset());
    // After caching, blocks will be read from cache (Sequential blocks)
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    iter->Seek(
        BuildKey(0));  // In cache so it will decrease the readahead_size.
    ASSERT_TRUE(iter->Valid());
    expected_current_readahead_size = std::max(
        decrease_readahead_size,
        (expected_current_readahead_size >= decrease_readahead_size
             ? (expected_current_readahead_size - decrease_readahead_size)
             : 0));

    iter->Seek(BuildKey(1000));  // Won't prefetch the block.
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);

    iter->Seek(BuildKey(1004));  // Prefetch the block.
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);
    expected_current_readahead_size *= 2;

    iter->Seek(BuildKey(1011));
    ASSERT_TRUE(iter->Valid());

    // Eligible to Prefetch data (not in buffer) but block is in cache so no
    // prefetch will happen and will result in decrease in readahead_size.
    // readahead_size will be 8 * 1024
    iter->Seek(BuildKey(1015));
    ASSERT_TRUE(iter->Valid());
    expected_current_readahead_size = std::max(
        decrease_readahead_size,
        (expected_current_readahead_size >= decrease_readahead_size
             ? (expected_current_readahead_size - decrease_readahead_size)
             : 0));

    // 1016 is the same block as 1015. So no change in readahead_size.
    iter->Seek(BuildKey(1016));
    ASSERT_TRUE(iter->Valid());

    // Prefetch data (not in buffer) but found in cache. So decrease
    // readahead_size. Since it will 0 after decrementing so readahead_size
    // will be set to initial value.
    iter->Seek(BuildKey(1019));
    ASSERT_TRUE(iter->Valid());
    expected_current_readahead_size = std::max(
        decrease_readahead_size,
        (expected_current_readahead_size >= decrease_readahead_size
             ? (expected_current_readahead_size - decrease_readahead_size)
             : 0));

    // Prefetch next sequential data.
    iter->Seek(BuildKey(1022));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(current_readahead_size, expected_current_readahead_size);
    ASSERT_EQ(buff_prefetch_count, 2);

    buff_prefetch_count = 0;
  }
  Close();
}

// This test verifies the basic functionality of seek parallelization for
// async_io.
TEST_P(PrefetchTest1, SeekParallelizationTest) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  Options options;
  SetGenericOptions(env.get(), GetParam(), options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (GetParam() && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  int buff_prefetch_count = 0;
  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  bool read_async_called = false;
  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });

  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  {
    ASSERT_OK(options.statistics->Reset());
    // Each block contains around 4 keys.
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    iter->Seek(BuildKey(0));  // Prefetch data because of seek parallelization.
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // New data block. Since num_file_reads in FilePrefetch after this read is
    // 2, it won't go for prefetching.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // Prefetch data.
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    HistogramData async_read_bytes;
    options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
    // not all platforms support io_uring. In that case it'll fallback to
    // normal prefetching without async_io.
    if (read_async_called) {
      ASSERT_EQ(buff_prefetch_count, 2);
      ASSERT_GT(async_read_bytes.count, 0);
      ASSERT_GT(get_perf_context()->number_async_seek, 0);
    } else {
      ASSERT_EQ(buff_prefetch_count, 1);
    }
  }
  Close();
}

TEST_P(PrefetchTest1, PollErrorRecoveryDuringIteration) {
  // This end-to-end test verifies that Poll() errors during async prefetching
  // are properly propagated to the iterator. When Poll() fails, the iterator
  // should stop and return an IOError status.
  //
  // With error injection on the 3rd Poll call, the iterator reads ~231 keys
  // (out of 500) before encountering the error.

  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 500;
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = GetParam();
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    ROCKSDB_GTEST_SKIP("Direct IO not supported");
    return;
  }
  ASSERT_OK(s);

  // Write keys with known values so we can verify correctness
  std::map<std::string, std::string> expected_data;
  {
    WriteBatch batch;
    for (int i = 0; i < kNumKeys; i++) {
      std::string key = BuildKey(i);
      std::string value = "value_" + std::to_string(i) + "_" +
                          std::string(100, 'x');  // Make values ~110 bytes
      ASSERT_OK(batch.Put(key, value));
      expected_data[key] = value;
    }
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  // Set up callbacks to track async IO and inject Poll errors
  std::atomic<int> poll_call_count{0};
  std::atomic<int> poll_error_injected_count{0};
  bool read_async_called = false;

  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::PollIfNeeded:IOStatus", [&](void* arg) {
        poll_call_count++;
        int current_count = poll_call_count.load();

        // Inject error on the third Poll call to allow some keys to be read
        // first
        if (current_count == 3) {
          IOStatus* io_s = static_cast<IOStatus*>(arg);
          *io_s = IOStatus::IOError("Injected Poll error for e2e testing");
          poll_error_injected_count++;
          std::cout << "PollErrorRecoveryDuringIteration: Injected error on "
                       "Poll call #"
                    << current_count << std::endl;
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });

  SyncPoint::GetInstance()->EnableProcessing();

  // Iterate through all keys with async IO enabled
  ReadOptions ro;
  ro.async_io = true;
  ro.adaptive_readahead = true;

  int keys_read = 0;
  int data_mismatches = 0;
  Status iter_status;
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::string key = iter->key().ToString();
      std::string value = iter->value().ToString();

      auto it = expected_data.find(key);
      if (it == expected_data.end()) {
        std::cout << "PollErrorRecoveryDuringIteration: Unexpected key: " << key
                  << std::endl;
        data_mismatches++;
      } else if (it->second != value) {
        std::cout << "PollErrorRecoveryDuringIteration: Value mismatch for key "
                  << key << std::endl;
        data_mismatches++;
      }
      keys_read++;
    }
    iter_status = iter->status();
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Log results
  std::cout << "PollErrorRecoveryDuringIteration: " << "read_async_called="
            << read_async_called << ", poll_calls=" << poll_call_count.load()
            << ", poll_errors_injected=" << poll_error_injected_count.load()
            << ", keys_read=" << keys_read << ", expected_keys=" << kNumKeys
            << ", data_mismatches=" << data_mismatches
            << ", iter_status=" << iter_status.ToString() << std::endl;

  // Verify no data mismatches occurred for keys that were read
  ASSERT_EQ(data_mismatches, 0)
      << "Found " << data_mismatches << " data mismatches";

  if (read_async_called) {
    // Async IO was used - verify Poll error was injected and propagated
    ASSERT_EQ(poll_call_count.load(), 3)
        << "Expected exactly 3 Poll calls when error injected on 3rd call";
    ASSERT_EQ(poll_error_injected_count.load(), 1)
        << "Expected exactly 1 Poll error to be injected";

    // The iterator should have stopped with an error status
    ASSERT_TRUE(iter_status.IsIOError())
        << "Expected iterator to report IOError after Poll failure, got: "
        << iter_status.ToString();

    std::cout << "PollErrorRecoveryDuringIteration: Successfully verified "
                 "Poll error was injected and propagated to iterator"
              << std::endl;
  } else {
    // Async IO not supported - iterator should complete successfully
    ASSERT_OK(iter_status);
    ASSERT_EQ(keys_read, kNumKeys);
    std::cout << "PollErrorRecoveryDuringIteration: Async IO (io_uring) not "
                 "supported on this platform, verified data correctness"
              << std::endl;
  }

  // Retry iteration without error injection - verify all data is still readable
  // This confirms the Poll error didn't corrupt state
  {
    int retry_keys_read = 0;
    int retry_data_mismatches = 0;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::string key = iter->key().ToString();
      std::string value = iter->value().ToString();

      auto it = expected_data.find(key);
      if (it == expected_data.end()) {
        retry_data_mismatches++;
      } else if (it->second != value) {
        retry_data_mismatches++;
      }
      retry_keys_read++;
    }
    ASSERT_OK(iter->status())
        << "Retry iteration failed: " << iter->status().ToString();
    ASSERT_EQ(retry_keys_read, kNumKeys)
        << "Retry should read all " << kNumKeys << " keys";
    ASSERT_EQ(retry_data_mismatches, 0)
        << "Retry found " << retry_data_mismatches << " data mismatches";
    std::cout << "PollErrorRecoveryDuringIteration: Retry succeeded, read all "
              << retry_keys_read << " keys correctly" << std::endl;
  }

  Close();
}

namespace {
#ifdef GFLAGS
const int kMaxArgCount = 100;
const size_t kArgBufferSize = 100000;

void RunIOTracerParserTool(std::string trace_file) {
  std::vector<std::string> params = {"./io_tracer_parser",
                                     "-io_trace_file=" + trace_file};

  char arg_buffer[kArgBufferSize];
  char* argv[kMaxArgCount];
  int argc = 0;
  int cursor = 0;
  for (const auto& arg : params) {
    ASSERT_LE(cursor + arg.size() + 1, kArgBufferSize);
    ASSERT_LE(argc + 1, kMaxArgCount);

    snprintf(arg_buffer + cursor, arg.size() + 1, "%s", arg.c_str());

    argv[argc++] = arg_buffer + cursor;
    cursor += static_cast<int>(arg.size()) + 1;
  }
  ASSERT_EQ(0, ROCKSDB_NAMESPACE::io_tracer_parser(argc, argv));
}
#endif  // GFLAGS
}  // namespace

// Tests the default implementation of ReadAsync API with PosixFileSystem during
// prefetching.
TEST_P(PrefetchTest, ReadAsyncWithPosixFS) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 1000;
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  int total_keys = 0;
  // Write the keys.
  {
    WriteBatch batch;
    Random rnd(309);
    for (int j = 0; j < 5; j++) {
      for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
        ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
        total_keys++;
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);
  }

  int buff_prefetch_count = 0;
  bool read_async_called = false;
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Read the keys.
  {
    ASSERT_OK(options.statistics->Reset());
    get_perf_context()->Reset();

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_OK(iter->status());

    if (read_async_called) {
      ASSERT_EQ(num_keys, total_keys);
      // Check stats to make sure async prefetch is done.
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      HistogramData prefetched_bytes_discarded;
      options.statistics->histogramData(PREFETCHED_BYTES_DISCARDED,
                                        &prefetched_bytes_discarded);
      ASSERT_GT(async_read_bytes.count, 0);
      ASSERT_GT(prefetched_bytes_discarded.count, 0);
      ASSERT_EQ(get_perf_context()->number_async_seek, 0);
    } else {
      // Not all platforms support iouring. In that case, ReadAsync in posix
      // won't submit async requests.
      ASSERT_EQ(num_keys, total_keys);
    }
    ASSERT_GT(buff_prefetch_count, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}

// This test verifies implementation of seek parallelization with
// PosixFileSystem during prefetching.
TEST_P(PrefetchTest, MultipleSeekWithPosixFS) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 1000;
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  int total_keys = 0;
  // Write the keys.
  {
    WriteBatch batch;
    Random rnd(309);
    for (int j = 0; j < 5; j++) {
      for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
        ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
        total_keys++;
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);
  }
  (void)total_keys;

  int num_keys_first_batch = 0;
  int num_keys_second_batch = 0;
  // Calculate number of keys without async_io for correctness validation.
  {
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ReadOptions()));
    // First Seek.
    iter->Seek(BuildKey(450));
    while (iter->Valid() && num_keys_first_batch < 100) {
      ASSERT_OK(iter->status());
      num_keys_first_batch++;
      iter->Next();
    }
    ASSERT_OK(iter->status());

    iter->Seek(BuildKey(942));
    while (iter->Valid()) {
      ASSERT_OK(iter->status());
      num_keys_second_batch++;
      iter->Next();
    }
    ASSERT_OK(iter->status());
  }

  int buff_prefetch_count = 0;
  bool read_async_called = false;
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Read the keys using seek.
  {
    ASSERT_OK(options.statistics->Reset());
    get_perf_context()->Reset();

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    // First Seek.
    {
      iter->Seek(BuildKey(450));
      while (iter->Valid() && num_keys < 100) {
        ASSERT_OK(iter->status());
        num_keys++;
        iter->Next();
      }

      ASSERT_OK(iter->status());
      ASSERT_EQ(num_keys, num_keys_first_batch);
      // Check stats to make sure async prefetch is done.
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      if (read_async_called) {
        ASSERT_GT(async_read_bytes.count, 0);
        ASSERT_GT(get_perf_context()->number_async_seek, 0);
      } else {
        // Not all platforms support iouring. In that case, ReadAsync in posix
        // won't submit async requests.
        ASSERT_EQ(async_read_bytes.count, 0);
        ASSERT_EQ(get_perf_context()->number_async_seek, 0);
      }
    }

    // Second Seek.
    {
      num_keys = 0;
      ASSERT_OK(options.statistics->Reset());
      get_perf_context()->Reset();

      iter->Seek(BuildKey(942));
      while (iter->Valid()) {
        ASSERT_OK(iter->status());
        num_keys++;
        iter->Next();
      }

      ASSERT_OK(iter->status());
      ASSERT_EQ(num_keys, num_keys_second_batch);
      HistogramData async_read_bytes;
      options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
      HistogramData prefetched_bytes_discarded;
      options.statistics->histogramData(PREFETCHED_BYTES_DISCARDED,
                                        &prefetched_bytes_discarded);
      ASSERT_GT(prefetched_bytes_discarded.count, 0);

      if (read_async_called) {
        ASSERT_GT(buff_prefetch_count, 0);

        // Check stats to make sure async prefetch is done.
        ASSERT_GT(async_read_bytes.count, 0);
        ASSERT_GT(get_perf_context()->number_async_seek, 0);
      } else {
        // Not all platforms support iouring. In that case, ReadAsync in posix
        // won't submit async requests.
        ASSERT_EQ(async_read_bytes.count, 0);
        ASSERT_EQ(get_perf_context()->number_async_seek, 0);
      }
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// This test verifies implementation of seek parallelization with
// PosixFileSystem during prefetching.
TEST_P(PrefetchTest, SeekParallelizationTestWithPosix) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  const int kNumKeys = 2000;
  // Set options
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  WriteBatch batch;
  Random rnd(309);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::string start_key = BuildKey(0);
  std::string end_key = BuildKey(kNumKeys - 1);
  Slice least(start_key.data(), start_key.size());
  Slice greatest(end_key.data(), end_key.size());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &least, &greatest));

  int buff_prefetch_count = 0;

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  bool read_async_called = false;
  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  {
    ASSERT_OK(options.statistics->Reset());
    // `get_perf_context()` is cumulative and thread-local; reset it so the
    // `number_async_seek` assertion below is about this iterator, as the
    // sibling tests above do.
    get_perf_context()->Reset();
    // Each block contains around 4 keys.
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    iter->Seek(BuildKey(0));  // Prefetch data because of seek parallelization.
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // New data block. Since num_file_reads in FilePrefetch after this read is
    // 2, it won't go for prefetching.
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    iter->Next();
    ASSERT_TRUE(iter->Valid());

    // Prefetch data.
    iter->Next();

    ASSERT_TRUE(iter->Valid());
    HistogramData async_read_bytes;
    options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
    if (read_async_called) {
      ASSERT_GT(async_read_bytes.count, 0);
      ASSERT_GT(get_perf_context()->number_async_seek, 0);
      if (std::get<1>(GetParam())) {
        ASSERT_EQ(buff_prefetch_count, 1);
      } else {
        ASSERT_EQ(buff_prefetch_count, 2);
      }
    } else {
      // Not all platforms support iouring. In that case, ReadAsync in posix
      // won't submit async requests.
      ASSERT_EQ(async_read_bytes.count, 0);
      ASSERT_EQ(get_perf_context()->number_async_seek, 0);
    }
  }
  Close();
}

#ifdef GFLAGS
// This test verifies io_tracing with PosixFileSystem during prefetching.
TEST_P(PrefetchTest, TraceReadAsyncWithCallbackWrapper) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }

  const int kNumKeys = 1000;
  std::shared_ptr<MockFS> fs = std::make_shared<MockFS>(
      FileSystem::Default(), /*support_prefetch=*/false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));

  bool use_direct_io = std::get<0>(GetParam());
  Options options;
  SetGenericOptions(env.get(), use_direct_io, options);
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  SetBlockBasedTableOptions(table_options);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Status s = TryReopen(options);
  if (use_direct_io && (s.IsNotSupported() || s.IsInvalidArgument())) {
    // If direct IO is not supported, skip the test
    return;
  } else {
    ASSERT_OK(s);
  }

  int total_keys = 0;
  // Write the keys.
  {
    WriteBatch batch;
    Random rnd(309);
    for (int j = 0; j < 5; j++) {
      for (int i = j * kNumKeys; i < (j + 1) * kNumKeys; i++) {
        ASSERT_OK(batch.Put(BuildKey(i), rnd.RandomString(1000)));
        total_keys++;
      }
      ASSERT_OK(db_->Write(WriteOptions(), &batch));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);
  }

  int buff_prefetch_count = 0;
  bool read_async_called = false;
  ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;

  if (std::get<1>(GetParam())) {
    ro.readahead_size = 16 * 1024;
  }

  SyncPoint::GetInstance()->SetCallBack("FilePrefetchBuffer::Prefetch:Start",
                                        [&](void*) { buff_prefetch_count++; });

  SyncPoint::GetInstance()->SetCallBack(
      "UpdateResults::io_uring_result",
      [&](void* /*arg*/) { read_async_called = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Read the keys.
  {
    // Start io_tracing.
    WriteOptions write_opt;
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    std::string trace_file_path = dbname_ + "/io_trace_file";

    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_file_path, &trace_writer));
    ASSERT_OK(db_->StartIOTrace(trace_opt, std::move(trace_writer)));
    ASSERT_OK(options.statistics->Reset());

    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int num_keys = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      num_keys++;
    }
    ASSERT_OK(iter->status());

    // End the tracing.
    ASSERT_OK(db_->EndIOTrace());
    ASSERT_OK(env_->FileExists(trace_file_path));

    ASSERT_EQ(num_keys, total_keys);
    HistogramData async_read_bytes;
    options.statistics->histogramData(ASYNC_READ_BYTES, &async_read_bytes);
    if (read_async_called) {
      ASSERT_GT(buff_prefetch_count, 0);
      // Check stats to make sure async prefetch is done.
      ASSERT_GT(async_read_bytes.count, 0);
    } else {
      // Not all platforms support iouring. In that case, ReadAsync in posix
      // won't submit async requests.
      ASSERT_EQ(async_read_bytes.count, 0);
    }

    // Check the file to see if ReadAsync is logged.
    RunIOTracerParserTool(trace_file_path);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}
#endif  // GFLAGS

class FilePrefetchBufferTest : public testing::Test {
 public:
  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    env_ = Env::Default();
    fs_ = FileSystem::Default();
    test_dir_ = test::PerThreadDBPath("file_prefetch_buffer_test");
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
    stats_ = CreateDBStatistics();
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_OK(DestroyDir(env_, test_dir_));
  }

  void Write(const std::string& fname, const std::string& content) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(fname), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  void Read(const std::string& fname, const FileOptions& opts,
            std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string fpath = Path(fname);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(fpath, opts, &f, nullptr));
    reader->reset(new RandomAccessFileReader(
        std::move(f), fpath, env_->GetSystemClock().get(),
        /*io_tracer=*/nullptr, stats_.get()));
  }

  void AssertResult(const std::string& content,
                    const std::vector<FSReadRequest>& reqs) {
    for (const auto& r : reqs) {
      ASSERT_OK(r.status);
      ASSERT_EQ(r.len, r.result.size());
      ASSERT_EQ(content.substr(r.offset, r.len), r.result.ToString());
    }
  }

  FileSystem* fs() { return fs_.get(); }
  Statistics* stats() { return stats_.get(); }

 private:
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string test_dir_;
  std::shared_ptr<Statistics> stats_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }
};

TEST_F(FilePrefetchBufferTest, SeekWithBlockCacheHit) {
  std::string fname = "seek-with-block-cache-hit";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 16384;
  readahead_params.max_readahead_size = 16384;

  FilePrefetchBuffer fpb(readahead_params, true, false, fs());
  Slice result;
  // Simulate a seek of 4096 bytes at offset 0. Due to the readahead settings,
  // it will do two reads of 4096+8192 and 8192
  Status s = fpb.PrefetchAsync(IOOptions(), r.get(), 0, 4096, &result);

  // Platforms that don't have IO uring may not support async IO.
  if (s.IsNotSupported()) {
    return;
  }

  ASSERT_TRUE(s.IsTryAgain());
  // Simulate a block cache hit
  fpb.UpdateReadPattern(0, 4096, false);
  // Now read some data that straddles the two prefetch buffers - offset 8192 to
  // 16384
  IOOptions io_opts;
  io_opts.rate_limiter_priority = Env::IOPriority::IO_LOW;
  ASSERT_TRUE(fpb.TryReadFromCache(io_opts, r.get(), 8192, 8192, &result, &s));
}

// Test to ensure when PrefetchAsync is called during seek, it doesn't do any
// alignment or prefetch extra if readahead is not enabled during seek.
TEST_F(FilePrefetchBufferTest, SeekWithoutAlignment) {
  std::string fname = "seek-without-alignment";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  size_t alignment = r->file()->GetRequiredBufferAlignment();
  size_t n = alignment / 2;

  int read_async_called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::ReadAsync",
      [&](void* /*arg*/) { read_async_called++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Without readahead enabled, there will be no alignment and offset of buffer
  // will be n.
  {
    ReadaheadParams readahead_params;
    readahead_params.initial_readahead_size = 8192;
    readahead_params.max_readahead_size = 16384;
    readahead_params.implicit_auto_readahead = true;
    readahead_params.num_file_reads_for_auto_readahead = 2;
    readahead_params.num_buffers = 2;

    FilePrefetchBuffer fpb(readahead_params, /*enable=*/true,
                           /*track_min_offset=*/false, fs(), nullptr, nullptr,
                           nullptr, FilePrefetchBufferUsage::kUnknown);

    Slice result;
    // Simulate a seek of half of alignment bytes at offset n. Due to the
    // readahead settings, it won't prefetch extra or do any alignment and
    // offset of buffer will be n.
    Status s = fpb.PrefetchAsync(IOOptions(), r.get(), n, n, &result);

    // Platforms that don't have IO uring may not support async IO.
    if (s.IsNotSupported()) {
      return;
    }

    ASSERT_TRUE(s.IsTryAgain());

    IOOptions io_opts;
    io_opts.rate_limiter_priority = Env::IOPriority::IO_LOW;
    ASSERT_TRUE(fpb.TryReadFromCache(io_opts, r.get(), n, n, &result, &s));

    if (read_async_called) {
      ASSERT_EQ(fpb.GetPrefetchOffset(), n);
    }
  }

  // With readahead enabled, it will do the alignment and prefetch and offset of
  // buffer will be 0.
  {
    read_async_called = false;
    ReadaheadParams readahead_params;
    readahead_params.initial_readahead_size = 16384;
    readahead_params.max_readahead_size = 16384;
    readahead_params.num_file_reads_for_auto_readahead = 2;
    readahead_params.num_buffers = 2;
    FilePrefetchBuffer fpb(readahead_params, /*enable=*/true,
                           /*track_min_offset=*/false, fs(), nullptr, nullptr,
                           nullptr, FilePrefetchBufferUsage::kUnknown);

    Slice result;
    // Simulate a seek of half of alignment bytes at offset n.
    Status s = fpb.PrefetchAsync(IOOptions(), r.get(), n, n, &result);

    // Platforms that don't have IO uring may not support async IO.
    if (s.IsNotSupported()) {
      return;
    }

    ASSERT_TRUE(s.IsTryAgain());

    IOOptions io_opts;
    io_opts.rate_limiter_priority = Env::IOPriority::IO_LOW;
    ASSERT_TRUE(fpb.TryReadFromCache(io_opts, r.get(), n, n, &result, &s));

    if (read_async_called) {
      ASSERT_EQ(fpb.GetPrefetchOffset(), 0);
    }
  }
}

TEST_F(FilePrefetchBufferTest, NoSyncWithAsyncIO) {
  std::string fname = "seek-with-block-cache-hit";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 8192;
  readahead_params.max_readahead_size = 16384;
  readahead_params.num_buffers = 2;
  FilePrefetchBuffer fpb(readahead_params, /*enable=*/true,
                         /*track_min_offset=*/false, fs(), nullptr, nullptr,
                         nullptr, FilePrefetchBufferUsage::kUnknown);

  int read_async_called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::ReadAsync",
      [&](void* /*arg*/) { read_async_called++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Slice async_result;
  // Simulate a seek of 4000 bytes at offset 3000. Due to the readahead
  // settings, it will do two reads of 4000+4096 and 4096
  Status s = fpb.PrefetchAsync(IOOptions(), r.get(), 3000, 4000, &async_result);

  // Platforms that don't have IO uring may not support async IO
  if (s.IsNotSupported()) {
    return;
  }

  ASSERT_TRUE(s.IsTryAgain());
  IOOptions io_opts;
  io_opts.rate_limiter_priority = Env::IOPriority::IO_LOW;
  ASSERT_TRUE(fpb.TryReadFromCache(io_opts, r.get(), /*offset=*/3000,
                                   /*length=*/4000, &async_result, &s));
  // No sync call should be made.
  HistogramData sst_read_micros;
  stats()->histogramData(SST_READ_MICROS, &sst_read_micros);
  ASSERT_EQ(sst_read_micros.count, 0);

  // Number of async calls should be.
  ASSERT_EQ(read_async_called, 2);
  // Length should be 4000.
  ASSERT_EQ(async_result.size(), 4000);
  // Data correctness.
  Slice result(&content[3000], 4000);
  ASSERT_EQ(result.size(), 4000);
  ASSERT_EQ(result, async_result);
}

TEST_F(FilePrefetchBufferTest, SyncReadaheadStats) {
  std::string fname = "seek-with-block-cache-hit";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 8192;
  readahead_params.max_readahead_size = 8192;
  FilePrefetchBuffer fpb(
      readahead_params, true, false, fs(), nullptr, stats.get(),
      nullptr /* cb */, FilePrefetchBufferUsage::kUserScanPrefetch /* usage */);
  Slice result;
  // Simulate a seek of 4096 bytes at offset 0. Due to the readahead settings,
  // it will do a read of offset 0 and length - (4096 + 8192) 12288.
  Status s;
  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 0, 4096, &result, &s));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(stats->getTickerCount(PREFETCH_HITS), 0);
  ASSERT_EQ(stats->getTickerCount(PREFETCH_BYTES_USEFUL), 0);

  // Simulate a block cache hit
  fpb.UpdateReadPattern(4096, 4096, false);
  // Now read some data that'll prefetch additional data from 12288 to 24576.
  // (8192) +  8192 (readahead_size).
  ASSERT_TRUE(
      fpb.TryReadFromCache(IOOptions(), r.get(), 8192, 8192, &result, &s));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(stats->getTickerCount(PREFETCH_HITS), 0);
  ASSERT_EQ(stats->getTickerCount(PREFETCH_BYTES_USEFUL), 4096);

  ASSERT_TRUE(
      fpb.TryReadFromCache(IOOptions(), r.get(), 12288, 4096, &result, &s));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_HITS), 1);
  ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_BYTES_USEFUL), 8192);

  // Now read some data with length doesn't align with alignment and it needs
  // prefetching. Read from 16000 with length 10000 (i.e. requested end offset -
  // 26000).
  ASSERT_TRUE(
      fpb.TryReadFromCache(IOOptions(), r.get(), 16000, 10000, &result, &s));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_HITS), 0);
  ASSERT_EQ(
      stats->getAndResetTickerCount(PREFETCH_BYTES_USEFUL),
      /* 24576(end offset of the buffer) - 16000(requested offset) =*/8576);
}

TEST_F(FilePrefetchBufferTest, ForCompaction) {
  // Make sure TryReadWithCache with for_compaction=true works without file
  // system buffer reuse optimization
  std::string fname = "fs-prefetch-buffer-for-compaction";
  Random rand(0);
  std::string content = rand.RandomString(64 * 1024);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 8192;
  readahead_params.max_readahead_size = 8192;
  readahead_params.num_buffers = 1;

  FilePrefetchBuffer fpb(readahead_params, true /* enable */,
                         false /* track_min_offset */, fs(), nullptr,
                         stats.get());

  Slice result;
  Status s;
  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 0 /* offset */,
                                   3000 /* n */, &result, &s, true));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(result.size(), 3000);
  ASSERT_EQ(strncmp(result.data(), content.substr(0, 3000).c_str(), 3000), 0);

  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 3000 /* offset */,
                                   10000 /* n */, &result, &s, true));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(result.size(), 10000);
  ASSERT_EQ(strncmp(result.data(), content.substr(3000, 10000).c_str(), 10000),
            0);

  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 15000 /* offset */,
                                   4096 /* n */, &result, &s, true));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(result.size(), 4096);
  ASSERT_EQ(strncmp(result.data(), content.substr(15000, 4096).c_str(), 4096),
            0);

  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 40000 /* offset */,
                                   20000 /* n */, &result, &s, true));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(result.size(), 20000);
  ASSERT_EQ(strncmp(result.data(), content.substr(40000, 20000).c_str(), 20000),
            0);

  // Try reading past end of file
  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 60000 /* offset */,
                                   10000 /* n */, &result, &s, true));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(result.size(), 64 * 1024 - 60000);
  ASSERT_EQ(
      strncmp(result.data(), content.substr(60000, 64 * 1024 - 60000).c_str(),
              64 * 1024 - 60000),
      0);
}

TEST_F(FilePrefetchBufferTest, PollErrorPropagation) {
  // This test verifies that Poll() errors in PollIfNeeded are properly
  // propagated rather than being silently ignored.

  std::string fname = "poll-error-test";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  // Set up readahead params for async prefetching
  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 16384;
  readahead_params.max_readahead_size = 16384;

  FilePrefetchBuffer fpb(readahead_params, /*enable=*/true,
                         /*track_min_offset=*/false, fs());

  Slice result;
  // Start an async prefetch to set up async_read_in_progress_ state
  Status s = fpb.PrefetchAsync(IOOptions(), r.get(), 0, 4096, &result);

  // Skip test on platforms that don't support async IO.
  if (s.IsNotSupported()) {
    ROCKSDB_GTEST_SKIP("Async IO not supported on this platform");
    return;
  }
  ASSERT_TRUE(s.IsTryAgain());

  // With the ReadAsync sync fallback, PrefetchAsync returns TryAgain even when
  // async IO is unavailable (data is read synchronously, but data_found was
  // false at entry). Detect by checking async_read_in_progress_ on the buffer.
  {
    std::vector<std::tuple<uint64_t, size_t, bool>> buf_info(1);
    fpb.TEST_GetBufferOffsetandSize(buf_info);
    bool async_read_in_progress = std::get<2>(buf_info[0]);
    if (!async_read_in_progress) {
      ROCKSDB_GTEST_SKIP("Async IO not available (sync fallback used)");
      return;
    }
  }

  // Set up SyncPoint to inject Poll error
  SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::PollIfNeeded:IOStatus", [&](void* arg) {
        IOStatus* io_s = static_cast<IOStatus*>(arg);
        *io_s = IOStatus::IOError("Injected Poll error for testing");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // TryReadFromCache will call PollIfNeeded to complete the async read
  IOOptions io_opts;
  io_opts.rate_limiter_priority = Env::IOPriority::IO_LOW;
  Status read_status;
  bool found =
      fpb.TryReadFromCache(io_opts, r.get(), 0, 4096, &result, &read_status);

  // When PollIfNeeded fails:
  // 1. PrefetchInternal returns the error status
  // 2. TryReadFromCacheUntracked sets *status to the error and returns false
  // Therefore: found should be false, and read_status should contain the error
  ASSERT_FALSE(found) << "Expected TryReadFromCache to return false on Poll "
                         "error, but it returned true";
  ASSERT_TRUE(read_status.IsIOError())
      << "Expected IOError status, got: " << read_status.ToString();
  ASSERT_TRUE(read_status.ToString().find("Injected Poll error") !=
              std::string::npos)
      << "Expected error message to contain 'Injected Poll error', got: "
      << read_status.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(FilePrefetchBufferTest, ReadAsyncSyncFallbackOnNotSupported) {
  std::string fname = "read-async-sync-fallback";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::NotSupported();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 16384;
  readahead_params.max_readahead_size = 16384;
  readahead_params.num_buffers = 2;

  FilePrefetchBuffer fpb(readahead_params, /*enable=*/true,
                         /*track_min_offset=*/false, fs());

  Slice result;
  Status s;
  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 0, 4096, &result, &s));
  ASSERT_OK(s);
  ASSERT_EQ(result.size(), 4096);
  ASSERT_EQ(memcmp(result.data(), content.data(), 4096), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class FSBufferPrefetchTest
    : public testing::Test,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  // Mock file system supporting the kFSBuffer buffer reuse operation
  class BufferReuseFS : public FileSystemWrapper {
   public:
    explicit BufferReuseFS(const std::shared_ptr<FileSystem>& _target)
        : FileSystemWrapper(_target) {}
    ~BufferReuseFS() override {}
    const char* Name() const override { return "BufferReuseFS"; }

    IOStatus NewRandomAccessFile(const std::string& fname,
                                 const FileOptions& opts,
                                 std::unique_ptr<FSRandomAccessFile>* result,
                                 IODebugContext* dbg) override {
      class WrappedRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
       public:
        explicit WrappedRandomAccessFile(
            std::unique_ptr<FSRandomAccessFile>& file)
            : FSRandomAccessFileOwnerWrapper(std::move(file)) {}

        IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                           const IOOptions& options,
                           IODebugContext* dbg) override {
          for (size_t i = 0; i < num_reqs; ++i) {
            FSReadRequest& req = reqs[i];

            // We cannot assume that fs_scratch points to the start of
            // the read data. We can have the FSAllocationPtr point to a
            // wrapper around the result buffer in our test implementation so
            // that we can catch whenever we incorrectly make this assumption.
            // See https://github.com/meta/rocksdb/pull/13189 for more
            // context.
            char* internalData = new char[req.len];
            req.status = Read(req.offset, req.len, options, &req.result,
                              internalData, dbg);

            Slice* internalSlice = new Slice(internalData, req.len);
            FSAllocationPtr internalPtr(internalSlice, [](void* ptr) {
              delete[] static_cast<const char*>(
                  static_cast<Slice*>(ptr)->data_);
              delete static_cast<Slice*>(ptr);
            });
            req.fs_scratch = std::move(internalPtr);
          }
          return IOStatus::OK();
        }
      };

      std::unique_ptr<FSRandomAccessFile> file;
      IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
      EXPECT_OK(s);
      result->reset(new WrappedRandomAccessFile(file));
      return s;
    }

    void SupportedOps(int64_t& supported_ops) override {
      supported_ops = 1 << FSSupportedOps::kAsyncIO;
      supported_ops |= 1 << FSSupportedOps::kFSBuffer;
    }
  };

  void SetUp() override {
    SetupSyncPointsToMockDirectIO();
    env_ = Env::Default();
    bool use_async_prefetch = std::get<0>(GetParam());
    if (use_async_prefetch) {
      fs_ = FileSystem::Default();
    } else {
      fs_ = std::make_shared<BufferReuseFS>(FileSystem::Default());
    }

    test_dir_ = test::PerThreadDBPath("fs_buffer_prefetch_test");
    ASSERT_OK(fs_->CreateDir(test_dir_, IOOptions(), nullptr));
    stats_ = CreateDBStatistics();
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_OK(DestroyDir(env_, test_dir_));
  }

  void Write(const std::string& fname, const std::string& content) {
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(fs_->NewWritableFile(Path(fname), FileOptions(), &f, nullptr));
    ASSERT_OK(f->Append(content, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  void Read(const std::string& fname, const FileOptions& opts,
            std::unique_ptr<RandomAccessFileReader>* reader) {
    std::string fpath = Path(fname);
    std::unique_ptr<FSRandomAccessFile> f;
    ASSERT_OK(fs_->NewRandomAccessFile(fpath, opts, &f, nullptr));
    reader->reset(new RandomAccessFileReader(
        std::move(f), fpath, env_->GetSystemClock().get(),
        /*io_tracer=*/nullptr, stats_.get()));
  }

  FileSystem* fs() { return fs_.get(); }
  Statistics* stats() { return stats_.get(); }
  SystemClock* clock() { return env_->GetSystemClock().get(); }

 private:
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string test_dir_;
  std::shared_ptr<Statistics> stats_;

  std::string Path(const std::string& fname) { return test_dir_ + "/" + fname; }
};

// param 1: whether async IO is enabled (num_buffers_ > 1)
// param 2: whether for_compaction is set to true for TryReadFromCache requests
// 3 out of these 4 combinations are tested (async IO is not allowed for
// compaction reads)
INSTANTIATE_TEST_CASE_P(FSBufferPrefetchTest, FSBufferPrefetchTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

TEST_P(FSBufferPrefetchTest, FSBufferPrefetchStatsInternals) {
  // Check that the main buffer, the overlap_buf_, and the secondary buffer (in
  // the case of num_buffers_ > 1) are populated correctly while reading a 32
  // KiB file
  std::string fname = "fs-buffer-prefetch-stats-internals";
  Random rand(0);
  std::string content = rand.RandomString(32768);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 8192;
  readahead_params.max_readahead_size = 8192;
  bool use_async_prefetch = std::get<0>(GetParam());
  bool for_compaction = std::get<1>(GetParam());
  // We disallow async IO for compaction reads since they are background
  // operations anyways and not as latency sensitive as user-initiated reads
  if (use_async_prefetch && for_compaction) {
    return;
  }
  size_t num_buffers = use_async_prefetch ? 2 : 1;
  readahead_params.num_buffers = num_buffers;

  FilePrefetchBuffer fpb(
      readahead_params, true /* enable */, false /* track_min_offset */, fs(),
      clock(), stats.get(), nullptr /* cb */,
      FilePrefetchBufferUsage::kUserScanPrefetch /* usage */);

  int overlap_buffer_write_ct = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::CopyDataToOverlapBuffer:Complete",
      [&](void* /*arg*/) { overlap_buffer_write_ct++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Slice result;
  // Read 4096 bytes at offset 0.
  Status s;
  std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(num_buffers);
  std::pair<uint64_t, size_t> overlap_buffer_info;
  bool could_read_from_cache =
      fpb.TryReadFromCache(IOOptions(), r.get(), 0 /* offset */, 4096 /* n */,
                           &result, &s, for_compaction);
  // Platforms that don't have IO uring may not support async IO.
  // With the ReadAsync sync fallback, s will be OK even when async IO is
  // unavailable -- detect by checking if the second buffer has an async read
  // in progress.
  if (use_async_prefetch && s.IsNotSupported()) {
    return;
  }
  ASSERT_TRUE(could_read_from_cache);
  ASSERT_EQ(s, Status::OK());

  ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_HITS), 0);
  ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_BYTES_USEFUL), 0);

  ASSERT_EQ(strncmp(result.data(), content.substr(0, 4096).c_str(), 4096), 0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);
  if (use_async_prefetch) {
    bool async_read_in_progress = std::get<2>(buffer_info[1]);
    if (!async_read_in_progress) {
      // Async IO was requested but not available (e.g., no io_uring).
      // ReadAsync fell back to sync read. Skip async-specific assertions.
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      return;
    }
    // Cut the readahead of 8192 in half.
    // Overlap buffer is not used
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    // Buffers: 0-8192, 8192-12288
    ASSERT_EQ(std::get<0>(buffer_info[0]), 0);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 4096 + 8192 / 2);
    ASSERT_EQ(std::get<0>(buffer_info[1]), 4096 + 8192 / 2);
    ASSERT_EQ(std::get<1>(buffer_info[1]), 8192 / 2);
  } else {
    // Read at offset 0 with length 4096 + 8192 = 12288.
    // Overlap buffer is not used
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    // Main buffer contains the requested data + the 8192 of prefetched data
    ASSERT_EQ(std::get<0>(buffer_info[0]), 0);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 4096 + 8192);
  }

  // Simulate a block cache hit
  fpb.UpdateReadPattern(4096, 4096, false);
  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 8192 /* offset */,
                                   8192 /* n */, &result, &s, for_compaction));
  ASSERT_EQ(s, Status::OK());
  if (!for_compaction) {
    ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_HITS), 0);
    ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_BYTES_USEFUL),
              4096);  // 8192-12288
  }

  ASSERT_EQ(strncmp(result.data(), content.substr(8192, 8192).c_str(), 8192),
            0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);

  if (use_async_prefetch) {
    // Our buffers were 0-8192, 8192-12288 at the start so we had some
    // overlapping data in the second buffer
    // We clean up outdated buffers so 0-8192 gets freed for more prefetching.
    // Our remaining buffer 8192-12288 has data that we want, so we can reuse it
    // We end up with: 8192-20480, 20480-24576
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    ASSERT_EQ(std::get<0>(buffer_info[0]), 8192);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 8192 + 8192 / 2);
    ASSERT_EQ(std::get<0>(buffer_info[1]), 8192 + (8192 + 8192 / 2));
    ASSERT_EQ(std::get<1>(buffer_info[1]), 8192 / 2);
  } else {
    // We only have 0-12288 cached, so reading from 8192-16384 will trigger a
    // prefetch up through 16384 + 8192 = 24576.
    // Overlap buffer reuses bytes 8192 to 12288
    ASSERT_EQ(overlap_buffer_info.first, 8192);
    ASSERT_EQ(overlap_buffer_info.second, 8192);
    ASSERT_EQ(overlap_buffer_write_ct, 2);
    // We spill to the overlap buffer so the remaining buffer only has the
    // missing and prefetched part
    ASSERT_EQ(std::get<0>(buffer_info[0]), 12288);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 12288);
  }

  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 12288 /* offset */,
                                   4096 /* n */, &result, &s, for_compaction));
  ASSERT_EQ(s, Status::OK());

  if (!for_compaction) {
    ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_HITS), 1);
    ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_BYTES_USEFUL),
              4096);  // 12288-16384
  }

  ASSERT_EQ(strncmp(result.data(), content.substr(12288, 4096).c_str(), 4096),
            0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);

  if (use_async_prefetch) {
    // Same as before: 8192-20480, 20480-24576 (cache hit in first buffer)
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    ASSERT_EQ(std::get<0>(buffer_info[0]), 8192);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 8192 + 8192 / 2);
    ASSERT_EQ(std::get<0>(buffer_info[1]), 8192 + (8192 + 8192 / 2));
    ASSERT_EQ(std::get<1>(buffer_info[1]), 8192 / 2);
  } else {
    // The main buffer has 12288-24576, so 12288-16384 is a cache hit.
    // Overlap buffer does not get used
    fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
    ASSERT_EQ(overlap_buffer_info.first, 8192);
    ASSERT_EQ(overlap_buffer_info.second, 8192);
    ASSERT_EQ(overlap_buffer_write_ct, 2);
    // Main buffer stays the same
    ASSERT_EQ(std::get<0>(buffer_info[0]), 12288);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 12288);
  }

  // Read from 16000-26000 (start and end do not meet normal alignment)
  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 16000 /* offset */,
                                   10000 /* n */, &result, &s, for_compaction));
  ASSERT_EQ(s, Status::OK());

  if (!for_compaction) {
    ASSERT_EQ(stats->getAndResetTickerCount(PREFETCH_HITS), 0);
    ASSERT_EQ(
        stats->getAndResetTickerCount(PREFETCH_BYTES_USEFUL),
        /* 24576(end offset of the buffer) - 16000(requested offset) =*/8576);
  }

  ASSERT_EQ(strncmp(result.data(), content.substr(16000, 10000).c_str(), 10000),
            0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);
  if (use_async_prefetch) {
    // Overlap buffer reuses bytes 16000 to 20480
    ASSERT_EQ(overlap_buffer_info.first, 16000);
    ASSERT_EQ(overlap_buffer_info.second, 10000);
    // First 2 writes are reusing existing 2 buffers. Last write fills in
    // what could not be found in either.
    ASSERT_EQ(overlap_buffer_write_ct, 3);
    ASSERT_EQ(std::get<0>(buffer_info[0]), 24576);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 32768 - 24576);
    ASSERT_EQ(std::get<0>(buffer_info[1]), 32768);
    ASSERT_EQ(std::get<1>(buffer_info[1]), 4096);
    ASSERT_TRUE(std::get<2>(
        buffer_info[1]));  // in progress async request (otherwise we should not
                           // be getting 4096 for the size)
  } else {
    // Overlap buffer reuses bytes 16000 to 24576
    ASSERT_EQ(overlap_buffer_info.first, 16000);
    ASSERT_EQ(overlap_buffer_info.second, 10000);
    ASSERT_EQ(overlap_buffer_write_ct, 4);
    // Even if you try to readahead to offset 16000 + 10000 + 8192, there are
    // only 32768 bytes in the original file
    ASSERT_EQ(std::get<0>(buffer_info[0]), 12288 + 12288);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 8192);
  }
}

TEST_P(FSBufferPrefetchTest, FSBufferPrefetchUnalignedReads) {
  // Check that the main buffer, the overlap_buf_, and the secondary buffer (in
  // the case of num_buffers_ > 1) are populated correctly
  // while reading with no regard to alignment
  std::string fname = "fs-buffer-prefetch-unaligned-reads";
  Random rand(0);
  std::string content = rand.RandomString(1000);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  ReadaheadParams readahead_params;
  // Readahead size will double each time
  readahead_params.initial_readahead_size = 5;
  readahead_params.max_readahead_size = 100;
  bool use_async_prefetch = std::get<0>(GetParam());
  bool for_compaction = std::get<1>(GetParam());
  // We disallow async IO for compaction reads since they are background
  // operations anyways and their latencies are not visible to the end user
  if (use_async_prefetch && for_compaction) {
    return;
  }
  size_t num_buffers = use_async_prefetch ? 2 : 1;
  readahead_params.num_buffers = num_buffers;
  FilePrefetchBuffer fpb(readahead_params, true /* enable */,
                         false /* track_min_offset */, fs(), clock(),
                         stats.get());

  int overlap_buffer_write_ct = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FilePrefetchBuffer::CopyDataToOverlapBuffer:Complete",
      [&](void* /*arg*/) { overlap_buffer_write_ct++; });
  SyncPoint::GetInstance()->EnableProcessing();

  Slice result;
  // Read 3 bytes at offset 5
  Status s;
  std::vector<std::tuple<uint64_t, size_t, bool>> buffer_info(num_buffers);
  std::pair<uint64_t, size_t> overlap_buffer_info;
  bool could_read_from_cache =
      fpb.TryReadFromCache(IOOptions(), r.get(), 5 /* offset */, 3 /* n */,
                           &result, &s, for_compaction);
  // Platforms that don't have IO uring may not support async IO.
  if (use_async_prefetch && s.IsNotSupported()) {
    return;
  }
  ASSERT_TRUE(could_read_from_cache);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(strncmp(result.data(), content.substr(5, 3).c_str(), 3), 0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);
  if (use_async_prefetch) {
    bool async_read_in_progress = std::get<2>(buffer_info[1]);
    if (!async_read_in_progress) {
      // Async IO was requested but not available (e.g., no io_uring).
      // ReadAsync fell back to sync read. Skip async-specific assertions.
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      return;
    }
    // Overlap buffer is not used
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    // With async prefetching, we still try to align to 4096 bytes, so
    // our main buffer read and secondary buffer prefetch are rounded up
    ASSERT_EQ(std::get<0>(buffer_info[0]), 0);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 1000);
    // This buffer won't actually get filled up with data since there is nothing
    // after 1000
    ASSERT_EQ(std::get<0>(buffer_info[1]), 4096);
    ASSERT_EQ(std::get<1>(buffer_info[1]), 4096);
    ASSERT_TRUE(std::get<2>(buffer_info[1]));  // in progress async request
  } else {
    // Overlap buffer is not used
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    // Main buffer contains the requested data + 5 of prefetched data (5 - 13)
    ASSERT_EQ(std::get<0>(buffer_info[0]), 5);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 3 + 5);
  }

  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 16 /* offset */,
                                   7 /* n */, &result, &s, for_compaction));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(strncmp(result.data(), content.substr(16, 7).c_str(), 7), 0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);
  if (use_async_prefetch) {
    // Complete hit since we have the entire file loaded in the main buffer
    // The remaining requests will be the same when use_async_prefetch is true
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    ASSERT_EQ(std::get<0>(buffer_info[0]), 0);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 1000);
  } else {
    // Complete miss: read 7 bytes at offset 16
    // Overlap buffer is not used (no partial hit)
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    // Main buffer contains the requested data + 10 of prefetched data (16 - 33)
    ASSERT_EQ(std::get<0>(buffer_info[0]), 16);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 7 + 10);
  }

  // Go backwards
  if (use_async_prefetch) {
    ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 10 /* offset */,
                                     8 /* n */, &result, &s, for_compaction));
  } else {
    // TryReadFromCacheUntracked returns false since the offset
    // requested is less than the start of our buffer
    ASSERT_FALSE(fpb.TryReadFromCache(IOOptions(), r.get(), 10 /* offset */,
                                      8 /* n */, &result, &s, for_compaction));
  }
  ASSERT_EQ(s, Status::OK());

  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 27 /* offset */,
                                   6 /* n */, &result, &s, for_compaction));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(strncmp(result.data(), content.substr(27, 6).c_str(), 6), 0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);
  if (use_async_prefetch) {
    // Complete hit since we have the entire file loaded in the main buffer
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    ASSERT_EQ(std::get<0>(buffer_info[0]), 0);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 1000);
  } else {
    // Complete hit
    // Overlap buffer still not used
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    // Main buffer unchanged
    ASSERT_EQ(std::get<0>(buffer_info[0]), 16);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 7 + 10);
  }

  ASSERT_TRUE(fpb.TryReadFromCache(IOOptions(), r.get(), 30 /* offset */,
                                   20 /* n */, &result, &s, for_compaction));
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(strncmp(result.data(), content.substr(30, 20).c_str(), 20), 0);
  fpb.TEST_GetOverlapBufferOffsetandSize(overlap_buffer_info);
  fpb.TEST_GetBufferOffsetandSize(buffer_info);
  if (use_async_prefetch) {
    // Complete hit since we have the entire file loaded in the main buffer
    ASSERT_EQ(overlap_buffer_info.first, 0);
    ASSERT_EQ(overlap_buffer_info.second, 0);
    ASSERT_EQ(std::get<0>(buffer_info[0]), 0);
    ASSERT_EQ(std::get<1>(buffer_info[0]), 1000);
  } else {
    // Partial hit (overlapping with end of main buffer)
    // Overlap buffer is used because we already had 30-33
    ASSERT_EQ(overlap_buffer_info.first, 30);
    ASSERT_EQ(overlap_buffer_info.second, 20);
    ASSERT_EQ(overlap_buffer_write_ct, 2);
    // Main buffer has up to offset 50 + 20 of prefetched data
    ASSERT_EQ(std::get<0>(buffer_info[0]), 33);
    ASSERT_EQ(std::get<1>(buffer_info[0]), (50 - 33) + 20);
  }
}

TEST_P(FSBufferPrefetchTest, FSBufferPrefetchRandomized) {
  // This test is meant to find untested code paths. It does very simple
  // verifications and relies on debug assertions to catch invariant violations
  // We scan through a file reading between 0 and 16 KiB at a time
  std::string fname = "fs-buffer-prefetch-randomized";
  Random rand(0);
  std::string content = rand.RandomString(16 * 1024 * 1024);
  Write(fname, content);

  FileOptions opts;
  std::unique_ptr<RandomAccessFileReader> r;
  Read(fname, opts, &r);

  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = 512;
  readahead_params.max_readahead_size = 2048;
  bool use_async_prefetch = std::get<0>(GetParam());
  bool for_compaction = std::get<1>(GetParam());
  // Async IO is not enabled for compaction prefetching
  if (use_async_prefetch && for_compaction) {
    return;
  }
  size_t num_buffers = use_async_prefetch ? 2 : 1;
  readahead_params.num_buffers = num_buffers;

  FilePrefetchBuffer fpb(readahead_params, true /* enable */,
                         false /* track_min_offset */, fs(), clock(),
                         stats.get());

  Slice result;
  Status s;

  uint64_t offset = 0;
  Random rnd(987654);
  for (int i = 0; i < 1000; i++) {
    size_t len = rnd.Uniform(16 * 1024);
    if (offset >= content.size()) {
      std::cout << "Stopped early after " << i << " iterations" << std::endl;
      break;
    }
    bool could_read_from_cache = fpb.TryReadFromCache(
        IOOptions(), r.get(), offset, len, &result, &s, for_compaction);
    // Platforms that don't have IO uring may not support async IO.
    if (use_async_prefetch && s.IsNotSupported()) {
      return;
    }
    ASSERT_TRUE(could_read_from_cache);
    ASSERT_EQ(s, Status::OK());
    ASSERT_EQ(result.size(),
              std::min(len, content.size() - static_cast<size_t>(offset)));
    ASSERT_EQ(strncmp(result.data(),
                      content.substr(offset, offset + len).c_str(), len),
              0);
    if (i % 4 == 0) {
      // Test reads where we "skip forward" in the file more than we could read
      // ahead
      offset += len + 2 * readahead_params.max_readahead_size;
    } else if (i % 4 == 1) {
      // Test reads where we "skip forward" in the file but should have some
      // overlap with the read ahead data
      offset += len + readahead_params.max_readahead_size / 2;
    } else {
      // Test "back to back" reads (next read starts right at end of previous
      // one)
      offset += len;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
