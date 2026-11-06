//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Testing various compression features

#include <atomic>
#include <cstdlib>
#include <memory>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/block_builder.h"
#include "table/block_based/data_block_footer.h"
#include "test_util/testutil.h"
#include "util/aligned_buffer.h"
#include "util/auto_tune_compressor.h"
#include "util/coding.h"
#include "util/random.h"
#include "util/simple_mixed_compressor.h"

namespace ROCKSDB_NAMESPACE {

class DBCompressionTest : public DBTestBase {
 public:
  DBCompressionTest() : DBTestBase("compression_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBCompressionTest, PresetCompressionDict) {
  // Verifies that compression ratio improves when dictionary is enabled, and
  // improves even further when the dictionary is trained by ZSTD.
  const size_t kBlockSizeBytes = 4 << 10;
  const size_t kL0FileBytes = 128 << 10;
  const size_t kApproxPerBlockOverheadBytes = 50;
  const int kNumL0Files = 5;

  Options options;
  // Make sure to use any custom env that the test is configured with.
  options.env = CurrentOptions().env;
  options.allow_concurrent_memtable_write = false;
  options.arena_block_size = kBlockSizeBytes;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kL0FileBytes / kBlockSizeBytes));
  options.num_levels = 2;
  options.target_file_size_base = kL0FileBytes;
  options.target_file_size_multiplier = 2;
  options.write_buffer_size = kL0FileBytes;
  BlockBasedTableOptions table_options;
  table_options.block_size = kBlockSizeBytes;
  std::vector<CompressionType> compression_types;
  if (Zlib_Supported()) {
    compression_types.push_back(kZlibCompression);
  }
#if LZ4_VERSION_NUMBER >= 10400  // r124+
  compression_types.push_back(kLZ4Compression);
  compression_types.push_back(kLZ4HCCompression);
#endif  // LZ4_VERSION_NUMBER >= 10400
  if (ZSTD_Supported()) {
    compression_types.push_back(kZSTD);
  }

  enum DictionaryTypes : int {
    kWithoutDict,
    kWithDict,
    kWithZSTDfinalizeDict,
    kWithZSTDTrainedDict,
    kDictEnd,
  };

  for (auto compression_type : compression_types) {
    options.compression = compression_type;
    size_t bytes_without_dict = 0;
    size_t bytes_with_dict = 0;
    size_t bytes_with_zstd_finalize_dict = 0;
    size_t bytes_with_zstd_trained_dict = 0;
    for (int i = kWithoutDict; i < kDictEnd; i++) {
      // First iteration: compress without preset dictionary
      // Second iteration: compress with preset dictionary
      // Third iteration (zstd only): compress with zstd-trained dictionary
      //
      // To make sure the compression dictionary has the intended effect, we
      // verify the compressed size is smaller in successive iterations. Also in
      // the non-first iterations, verify the data we get out is the same data
      // we put in.
      switch (i) {
        case kWithoutDict:
          options.compression_opts.max_dict_bytes = 0;
          options.compression_opts.zstd_max_train_bytes = 0;
          break;
        case kWithDict:
          options.compression_opts.max_dict_bytes = kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = 0;
          break;
        case kWithZSTDfinalizeDict:
          if (compression_type != kZSTD ||
              !ZSTD_FinalizeDictionarySupported()) {
            continue;
          }
          options.compression_opts.max_dict_bytes = kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = kL0FileBytes;
          options.compression_opts.use_zstd_dict_trainer = false;
          break;
        case kWithZSTDTrainedDict:
          if (compression_type != kZSTD || !ZSTD_TrainDictionarySupported()) {
            continue;
          }
          options.compression_opts.max_dict_bytes = kBlockSizeBytes;
          options.compression_opts.zstd_max_train_bytes = kL0FileBytes;
          options.compression_opts.use_zstd_dict_trainer = true;
          break;
        default:
          assert(false);
      }

      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));
      CreateAndReopenWithCF({"pikachu"}, options);
      Random rnd(301);
      std::string seq_datas[10];
      for (int j = 0; j < 10; ++j) {
        seq_datas[j] =
            rnd.RandomString(kBlockSizeBytes - kApproxPerBlockOverheadBytes);
      }

      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      for (int j = 0; j < kNumL0Files; ++j) {
        for (size_t k = 0; k < kL0FileBytes / kBlockSizeBytes + 1; ++k) {
          auto key_num = j * (kL0FileBytes / kBlockSizeBytes) + k;
          ASSERT_OK(Put(1, Key(static_cast<int>(key_num)),
                        seq_datas[(key_num / 10) % 10]));
        }
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
        ASSERT_EQ(j + 1, NumTableFilesAtLevel(0, 1));
      }
      ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                            true /* disallow_trivial_move */));
      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
      ASSERT_GT(NumTableFilesAtLevel(1, 1), 0);

      // Get the live sst files size
      size_t total_sst_bytes = TotalSize(1);
      if (i == kWithoutDict) {
        bytes_without_dict = total_sst_bytes;
      } else if (i == kWithDict) {
        bytes_with_dict = total_sst_bytes;
      } else if (i == kWithZSTDfinalizeDict) {
        bytes_with_zstd_finalize_dict = total_sst_bytes;
      } else if (i == kWithZSTDTrainedDict) {
        bytes_with_zstd_trained_dict = total_sst_bytes;
      }

      for (size_t j = 0; j < kNumL0Files * (kL0FileBytes / kBlockSizeBytes);
           j++) {
        ASSERT_EQ(seq_datas[(j / 10) % 10], Get(1, Key(static_cast<int>(j))));
      }
      if (i == kWithDict) {
        ASSERT_GT(bytes_without_dict, bytes_with_dict);
      } else if (i == kWithZSTDTrainedDict) {
        // In zstd compression, it is sometimes possible that using a finalized
        // dictionary does not get as good a compression ratio as raw content
        // dictionary. But using a dictionary should always get better
        // compression ratio than not using one.
        ASSERT_TRUE(bytes_with_dict > bytes_with_zstd_finalize_dict ||
                    bytes_without_dict > bytes_with_zstd_finalize_dict);
      } else if (i == kWithZSTDTrainedDict) {
        // In zstd compression, it is sometimes possible that using a trained
        // dictionary does not get as good a compression ratio as without
        // training.
        // But using a dictionary (with or without training) should always get
        // better compression ratio than not using one.
        ASSERT_TRUE(bytes_with_dict > bytes_with_zstd_trained_dict ||
                    bytes_without_dict > bytes_with_zstd_trained_dict);
      }

      DestroyAndReopen(options);
    }
  }
}

TEST_F(DBCompressionTest, PresetCompressionDictLocality) {
  if (!ZSTD_Supported()) {
    return;
  }
  // Verifies that compression dictionary is generated from local data. The
  // verification simply checks all output SSTs have different compression
  // dictionaries. We do not verify effectiveness as that'd likely be flaky in
  // the future.
  const int kNumEntriesPerFile = 1 << 10;  // 1KB
  const int kNumBytesPerEntry = 1 << 10;   // 1KB
  const int kNumFiles = 4;
  Options options = CurrentOptions();
  options.compression = kZSTD;
  options.compression_opts.max_dict_bytes = 1 << 14;        // 16KB
  options.compression_opts.zstd_max_train_bytes = 1 << 18;  // 256KB
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.target_file_size_base = kNumEntriesPerFile * kNumBytesPerEntry;
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumEntriesPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumEntriesPerFile + j),
                    rnd.RandomString(kNumBytesPerEntry)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(1);
    ASSERT_EQ(NumTableFilesAtLevel(1), i + 1);
  }

  // Store all the dictionaries generated during a full compaction.
  std::vector<std::string> compression_dicts;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteCompressionDictBlock:RawDict",
      [&](void* arg) {
        compression_dicts.emplace_back(static_cast<Slice*>(arg)->ToString());
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions compact_range_opts;
  compact_range_opts.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(compact_range_opts, nullptr, nullptr));

  // Dictionary compression should not be so good as to compress four totally
  // random files into one. If it does then there's probably something wrong
  // with the test.
  ASSERT_GT(NumTableFilesAtLevel(1), 1);

  // Furthermore, there should be one compression dictionary generated per file.
  // And they should all be different from each other.
  ASSERT_EQ(NumTableFilesAtLevel(1),
            static_cast<int>(compression_dicts.size()));
  for (size_t i = 1; i < compression_dicts.size(); ++i) {
    std::string& a = compression_dicts[i - 1];
    std::string& b = compression_dicts[i];
    size_t alen = a.size();
    size_t blen = b.size();
    ASSERT_TRUE(alen != blen || memcmp(a.data(), b.data(), alen) != 0);
  }
}

static std::string CompressibleString(Random* rnd, int len) {
  std::string r;
  test::CompressibleString(rnd, 0.8, len, &r);
  return r;
}

TEST_F(DBCompressionTest, DynamicLevelCompressionPerLevel) {
  if (!Snappy_Supported()) {
    return;
  }
  const int kNKeys = 120;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }

  Random rnd(301);
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.db_write_buffer_size = 20480;
  options.write_buffer_size = 20480;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.target_file_size_base = 20480;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 102400;
  options.max_bytes_for_level_multiplier = 4;
  options.max_background_compactions = 1;
  options.num_levels = 5;
  options.statistics = CreateDBStatistics();

  options.compression_per_level.resize(3);
  // No compression for L0
  options.compression_per_level[0] = kNoCompression;
  // No compression for the Ln whre L0 is compacted to
  options.compression_per_level[1] = kNoCompression;
  // Snappy compression for Ln+1
  options.compression_per_level[2] = kSnappyCompression;

  OnFileDeletionListener* listener = new OnFileDeletionListener();
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);

  // Insert more than 80K. L4 should be base level. Neither L0 nor L4 should
  // be compressed, so there shouldn't be any compression.
  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put(Key(keys[i]), CompressibleString(&rnd, 4000)));
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_EQ(NumTableFilesAtLevel(3), 0);
  ASSERT_TRUE(NumTableFilesAtLevel(0) > 0 || NumTableFilesAtLevel(4) > 0);

  // Verify there was no compression
  auto num_block_compressed =
      options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
  ASSERT_EQ(num_block_compressed, 0);

  // Insert 400KB and there will be some files end up in L3. According to the
  // above compression settings for each level, there will be some compression.
  ASSERT_OK(options.statistics->Reset());
  ASSERT_EQ(num_block_compressed, 0);
  for (int i = 20; i < 120; i++) {
    ASSERT_OK(Put(Key(keys[i]), CompressibleString(&rnd, 4000)));
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_GE(NumTableFilesAtLevel(3), 1);
  ASSERT_GE(NumTableFilesAtLevel(4), 1);

  // Verify there was compression
  num_block_compressed =
      options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSED);
  ASSERT_GT(num_block_compressed, 0);

  // Make sure data in files in L3 is not compacted by removing all files
  // in L4 and calculate number of rows
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "true"},
  }));
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);

  // Ensure that L1+ files are non-overlapping and together with L0 encompass
  // full key range between smallestkey and largestkey from CF file metadata.
  int largestkey_in_prev_level = -1;
  int keys_found = 0;
  for (int level = (int)cf_meta.levels.size() - 1; level >= 0; level--) {
    int files_in_level = (int)cf_meta.levels[level].files.size();
    int largestkey_in_prev_file = -1;
    for (int j = 0; j < files_in_level; j++) {
      int smallestkey = IdFromKey(cf_meta.levels[level].files[j].smallestkey);
      int largestkey = IdFromKey(cf_meta.levels[level].files[j].largestkey);
      int num_entries = (int)cf_meta.levels[level].files[j].num_entries;
      ASSERT_EQ(num_entries, largestkey - smallestkey + 1);
      keys_found += num_entries;
      if (level > 0) {
        if (j == 0) {
          ASSERT_GT(smallestkey, largestkey_in_prev_level);
        }
        if (j > 0) {
          ASSERT_GT(smallestkey, largestkey_in_prev_file);
        }
        if (j == files_in_level - 1) {
          largestkey_in_prev_level = largestkey;
        }
      }
      largestkey_in_prev_file = largestkey;
    }
  }
  ASSERT_EQ(keys_found, kNKeys);

  for (const auto& file : cf_meta.levels[4].files) {
    listener->SetExpectedFileName(dbname_ + file.name);
    const RangeOpt ranges(file.smallestkey, file.largestkey);
    // Given verification from above, we're guaranteed that by deleting all the
    // files in [<smallestkey>, <largestkey>] range, we're effectively deleting
    // that very single file and nothing more.
    EXPECT_OK(dbfull()->DeleteFilesInRanges(dbfull()->DefaultColumnFamily(),
                                            &ranges, true /* include_end */));
  }
  listener->VerifyMatchedCount(cf_meta.levels[4].files.size());

  int num_keys = 0;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    num_keys++;
  }
  ASSERT_OK(iter->status());

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_GE(NumTableFilesAtLevel(3), 1);
  ASSERT_EQ(NumTableFilesAtLevel(4), 0);

  ASSERT_GT(SizeAtLevel(0) + SizeAtLevel(3), num_keys * 4000U + num_keys * 10U);
}

TEST_F(DBCompressionTest, DynamicLevelCompressionPerLevel2) {
  if (!Snappy_Supported() || !LZ4_Supported() || !Zlib_Supported()) {
    return;
  }
  const int kNKeys = 500;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }
  RandomShuffle(std::begin(keys), std::end(keys));

  Random rnd(301);
  Options options;
  options.create_if_missing = true;
  options.db_write_buffer_size = 6000000;
  options.write_buffer_size = 600000;
  options.max_write_buffer_number = 2;
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_stop_writes_trigger = 2;
  options.soft_pending_compaction_bytes_limit = 1024 * 1024;
  options.target_file_size_base = 20;
  options.env = env_;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = 200;
  options.max_bytes_for_level_multiplier = 8;
  options.max_background_compactions = 1;
  options.num_levels = 5;
  std::shared_ptr<mock::MockTableFactory> mtf(new mock::MockTableFactory);
  options.table_factory = mtf;

  options.compression_per_level.resize(3);
  options.compression_per_level[0] = kNoCompression;
  options.compression_per_level[1] = kLZ4Compression;
  options.compression_per_level[2] = kZlibCompression;

  DestroyAndReopen(options);
  // When base level is L4, L4 is LZ4.
  std::atomic<int> num_zlib(0);
  std::atomic<int> num_lz4(0);
  std::atomic<int> num_no(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        if (compaction->output_level() == 4) {
          ASSERT_TRUE(compaction->output_compression() == kLZ4Compression);
          num_lz4.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:output_compression", [&](void* arg) {
        auto* compression = static_cast<CompressionType*>(arg);
        ASSERT_TRUE(*compression == kNoCompression);
        num_no.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < 100; i++) {
    std::string value = rnd.RandomString(200);
    ASSERT_OK(Put(Key(keys[i]), value));
    if (i % 25 == 24) {
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  }

  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_EQ(NumTableFilesAtLevel(3), 0);
  ASSERT_GT(NumTableFilesAtLevel(4), 0);
  ASSERT_GT(num_no.load(), 2);
  ASSERT_GT(num_lz4.load(), 0);
  int prev_num_files_l4 = NumTableFilesAtLevel(4);

  // After base level turn L4->L3, L3 becomes LZ4 and L4 becomes Zlib
  num_lz4.store(0);
  num_no.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        if (compaction->output_level() == 4 && compaction->start_level() == 3) {
          ASSERT_TRUE(compaction->output_compression() == kZlibCompression);
          num_zlib.fetch_add(1);
        } else {
          ASSERT_TRUE(compaction->output_compression() == kLZ4Compression);
          num_lz4.fetch_add(1);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:output_compression", [&](void* arg) {
        auto* compression = static_cast<CompressionType*>(arg);
        ASSERT_TRUE(*compression == kNoCompression);
        num_no.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 101; i < 500; i++) {
    std::string value = rnd.RandomString(200);
    ASSERT_OK(Put(Key(keys[i]), value));
    if (i % 100 == 99) {
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 0);
  ASSERT_GT(NumTableFilesAtLevel(3), 0);
  ASSERT_GT(NumTableFilesAtLevel(4), prev_num_files_l4);
  ASSERT_GT(num_no.load(), 2);
  ASSERT_GT(num_lz4.load(), 0);
  ASSERT_GT(num_zlib.load(), 0);
}

class PresetCompressionDictTest
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<CompressionType, bool>> {
 public:
  PresetCompressionDictTest()
      : DBTestBase("compression_test_preset_dict", false /* env_do_fsync */),
        compression_type_(std::get<0>(GetParam())),
        bottommost_(std::get<1>(GetParam())) {}

 protected:
  const CompressionType compression_type_;
  const bool bottommost_;
};

INSTANTIATE_TEST_CASE_P(
    DBCompressionTest, PresetCompressionDictTest,
    ::testing::Combine(::testing::ValuesIn(GetSupportedDictCompressions()),
                       ::testing::Bool()));

TEST_P(PresetCompressionDictTest, Flush) {
  // Verifies that dictionary is generated and written during flush only when
  // `ColumnFamilyOptions::compression` enables dictionary. Also verifies the
  // size of the dictionary is within expectations according to the limit on
  // buffering set by `CompressionOptions::max_dict_buffer_bytes`.
  const size_t kValueLen = 256;
  const size_t kKeysPerFile = 1 << 10;
  const size_t kDictLen = 16 << 10;
  const size_t kBlockLen = 4 << 10;

  Options options = CurrentOptions();
  if (bottommost_) {
    options.bottommost_compression = compression_type_;
    options.bottommost_compression_opts.enabled = true;
    options.bottommost_compression_opts.max_dict_bytes = kDictLen;
    options.bottommost_compression_opts.max_dict_buffer_bytes = kBlockLen;
  } else {
    options.compression = compression_type_;
    options.compression_opts.max_dict_bytes = kDictLen;
    options.compression_opts.max_dict_buffer_bytes = kBlockLen;
  }
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kKeysPerFile));
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.block_size = kBlockLen;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  for (size_t i = 0; i <= kKeysPerFile; ++i) {
    ASSERT_OK(Put(Key(static_cast<int>(i)), rnd.RandomString(kValueLen)));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // We can use `BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT` to detect whether a
  // compression dictionary exists since dictionaries would be preloaded when
  // the flush finishes.
  if (bottommost_) {
    // Flush is never considered bottommost. This should change in the future
    // since flushed files may have nothing underneath them, like the one in
    // this test case.
    ASSERT_EQ(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
  } else {
    ASSERT_GT(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
    ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_ADD), 1);
    // Although we stop buffering after `kBlockLen` bytes, there may be up to
    // two blocks of data included in the dictionary since we only check limit
    // after each block is built. And because block cache charges for bytes used
    // by ZSTD's digested dictionary, we need a larger factor for the memory
    // overheads in that case.
    ASSERT_LE(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        (compression_type_ == kZSTD ? 10 : 2) * kBlockLen);
  }
}

TEST_P(PresetCompressionDictTest, CompactNonBottommost) {
  // Verifies that dictionary is generated and written during compaction to
  // non-bottommost level only when `ColumnFamilyOptions::compression` enables
  // dictionary. Also verifies the size of the dictionary is within expectations
  // according to the limit on buffering set by
  // `CompressionOptions::max_dict_buffer_bytes`.
  const size_t kValueLen = 256;
  const size_t kKeysPerFile = 1 << 10;
  const size_t kDictLen = 16 << 10;
  const size_t kBlockLen = 4 << 10;

  Options options = CurrentOptions();
  if (bottommost_) {
    options.bottommost_compression = compression_type_;
    options.bottommost_compression_opts.enabled = true;
    options.bottommost_compression_opts.max_dict_bytes = kDictLen;
    options.bottommost_compression_opts.max_dict_buffer_bytes = kBlockLen;
  } else {
    options.compression = compression_type_;
    options.compression_opts.max_dict_bytes = kDictLen;
    options.compression_opts.max_dict_buffer_bytes = kBlockLen;
  }
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.block_size = kBlockLen;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  for (size_t j = 0; j <= kKeysPerFile; ++j) {
    ASSERT_OK(Put(Key(static_cast<int>(j)), rnd.RandomString(kValueLen)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  for (int i = 0; i < 2; ++i) {
    for (size_t j = 0; j <= kKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(static_cast<int>(j)), rnd.RandomString(kValueLen)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("2,0,1", FilesPerLevel(0));

  PopTicker(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT);
  PopTicker(options, BLOCK_CACHE_COMPRESSION_DICT_ADD);

  // This L0->L1 compaction merges the two L0 files into L1. The produced L1
  // file is not bottommost due to the existing L2 file covering the same key-
  // range.
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ("0,1,1", FilesPerLevel(0));
  // We can use `BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT` to detect whether a
  // compression dictionary exists since dictionaries would be preloaded when
  // the compaction finishes.
  if (bottommost_) {
    ASSERT_EQ(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
  } else {
    ASSERT_GT(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        0);
    ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_ADD), 1);
    // Although we stop buffering after `kBlockLen` bytes, there may be up to
    // two blocks of data included in the dictionary since we only check limit
    // after each block is built. And because block cache charges for bytes used
    // by ZSTD's digested dictionary, we need a larger factor for the memory
    // overheads in that case.
    ASSERT_LE(
        TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
        (compression_type_ == kZSTD ? 10 : 2) * kBlockLen);
  }
}

TEST_P(PresetCompressionDictTest, CompactBottommost) {
  // Verifies that dictionary is generated and written during compaction to
  // non-bottommost level only when either `ColumnFamilyOptions::compression` or
  // `ColumnFamilyOptions::bottommost_compression` enables dictionary. Also
  // verifies the size of the dictionary is within expectations according to the
  // limit on buffering set by `CompressionOptions::max_dict_buffer_bytes`.
  const size_t kValueLen = 256;
  const size_t kKeysPerFile = 1 << 10;
  const size_t kDictLen = 16 << 10;
  const size_t kBlockLen = 4 << 10;

  Options options = CurrentOptions();
  if (bottommost_) {
    options.bottommost_compression = compression_type_;
    options.bottommost_compression_opts.enabled = true;
    options.bottommost_compression_opts.max_dict_bytes = kDictLen;
    options.bottommost_compression_opts.max_dict_buffer_bytes = kBlockLen;
  } else {
    options.compression = compression_type_;
    options.compression_opts.max_dict_bytes = kDictLen;
    options.compression_opts.max_dict_buffer_bytes = kBlockLen;
  }
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.block_size = kBlockLen;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    for (size_t j = 0; j <= kKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(static_cast<int>(j)), rnd.RandomString(kValueLen)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("2", FilesPerLevel(0));

  PopTicker(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT);
  PopTicker(options, BLOCK_CACHE_COMPRESSION_DICT_ADD);

  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_GT(
      TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
      0);
  ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_ADD), 1);
  // Although we stop buffering after `kBlockLen` bytes, there may be up to
  // two blocks of data included in the dictionary since we only check limit
  // after each block is built. And because block cache charges for bytes used
  // by ZSTD's digested dictionary, we need a larger factor for the memory
  // overheads in that case.
  ASSERT_LE(
      TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
      (compression_type_ == kZSTD ? 10 : 2) * kBlockLen);
}

class CompactionCompressionListener : public EventListener {
 public:
  explicit CompactionCompressionListener(Options* db_options)
      : db_options_(db_options) {}

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    // Figure out last level with files
    int bottommost_level = 0;
    for (int level = 0; level < db->NumberLevels(); level++) {
      std::string files_at_level;
      ASSERT_TRUE(
          db->GetProperty("rocksdb.num-files-at-level" + std::to_string(level),
                          &files_at_level));
      if (files_at_level != "0") {
        bottommost_level = level;
      }
    }

    if (db_options_->bottommost_compression != kDisableCompressionOption &&
        ci.output_level == bottommost_level) {
      ASSERT_EQ(ci.compression, db_options_->bottommost_compression);
    } else if (db_options_->compression_per_level.size() != 0) {
      ASSERT_EQ(ci.compression,
                db_options_->compression_per_level[ci.output_level]);
    } else {
      ASSERT_EQ(ci.compression, db_options_->compression);
    }
    max_level_checked = std::max(max_level_checked, ci.output_level);
  }

  int max_level_checked = 0;
  const Options* db_options_;
};

enum CompressionFailureType {
  kTestCompressionFail,
  kTestDecompressionFail,
  kTestDecompressionCorruption,
  kTestStartOfFinishFail,
};

class CompressionFailuresTest
    : public DBCompressionTest,
      public testing::WithParamInterface<std::tuple<
          CompressionFailureType, CompressionType, uint32_t, uint32_t>> {
 public:
  CompressionFailuresTest() {
    std::tie(compression_failure_type_, compression_type_,
             compression_max_dict_bytes_, compression_parallel_threads_) =
        GetParam();
  }

  CompressionFailureType compression_failure_type_ = kTestCompressionFail;
  CompressionType compression_type_ = kNoCompression;
  uint32_t compression_max_dict_bytes_ = 0;
  uint32_t compression_parallel_threads_ = 0;
};

INSTANTIATE_TEST_CASE_P(
    DBCompressionTest, CompressionFailuresTest,
    ::testing::Combine(::testing::Values(kTestCompressionFail,
                                         kTestDecompressionFail,
                                         kTestDecompressionCorruption,
                                         kTestStartOfFinishFail),
                       ::testing::ValuesIn(GetSupportedCompressions()),
                       ::testing::Values(0, 10), ::testing::Values(1, 4)));

TEST_P(CompressionFailuresTest, CompressionFailures) {
  if (compression_type_ == kNoCompression) {
    return;
  }

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 7;
  options.max_background_compactions = 1;
  options.target_file_size_base = 512;

  BlockBasedTableOptions table_options;
  table_options.block_size = 512;
  table_options.verify_compression = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  options.compression = compression_type_;
  options.compression_opts.parallel_threads = compression_parallel_threads_;
  options.compression_opts.max_dict_bytes = compression_max_dict_bytes_;
  options.bottommost_compression_opts.parallel_threads =
      compression_parallel_threads_;
  options.bottommost_compression_opts.max_dict_bytes =
      compression_max_dict_bytes_;

  if (compression_failure_type_ == kTestCompressionFail) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::CompressAndVerifyBlock:TamperWithResultType",
        [](void* arg) {
          CompressionType* ret = static_cast<CompressionType*>(arg);
          *ret = kNoCompression;
        });
  } else if (compression_failure_type_ == kTestDecompressionFail) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DecompressBlockData:TamperWithReturnValue", [](void* arg) {
          Status* ret = static_cast<Status*>(arg);
          ASSERT_OK(*ret);
          *ret = Status::Corruption("kTestDecompressionFail");
        });
  } else if (compression_failure_type_ == kTestDecompressionCorruption) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DecompressBlockData:TamperWithDecompressionOutput", [](void* arg) {
          BlockContents* contents = static_cast<BlockContents*>(arg);
          // Ensure uncompressed data != original data
          const size_t len = contents->data.size() + 1;
          std::unique_ptr<char[]> fake_data(new char[len]());
          *contents = BlockContents(std::move(fake_data), len);
        });
  } else if (compression_failure_type_ == kTestStartOfFinishFail) {
    if (compression_parallel_threads_ <= 1) {
      // skip this configuration
      return;
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::Finish:ParallelIOStatus", [&](void* arg) {
          *static_cast<IOStatus*>(arg) = IOStatus::Corruption("Seeded failure");
        });
  } else {
    abort();
  }

  std::map<std::string, std::string> key_value_written;

  const int kKeySize = 5;
  const int kValUnitSize = 16;
  const int kValSize = 256;
  Random rnd(405);

  Status s = Status::OK();

  DestroyAndReopen(options);
  // Write 10 random files
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 5; j++) {
      std::string key = rnd.RandomString(kKeySize);
      // Ensure good compression ratio
      std::string valueUnit = rnd.RandomString(kValUnitSize);
      std::string value;
      for (int k = 0; k < kValSize; k += kValUnitSize) {
        value += valueUnit;
      }
      s = Put(key, value);
      if (compression_failure_type_ == kTestCompressionFail) {
        key_value_written[key] = value;
        ASSERT_OK(s);
      }
    }
    s = Flush();
    if (compression_failure_type_ == kTestCompressionFail) {
      ASSERT_OK(s);
    }
    s = dbfull()->TEST_WaitForCompact();
    if (compression_failure_type_ == kTestCompressionFail) {
      ASSERT_OK(s);
    }
    if (i == 4) {
      // Make compression fail at the mid of table building
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    }
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  auto st = s.getState();
  if (compression_failure_type_ == kTestCompressionFail) {
    // Should be kNoCompression, check content consistency
    std::unique_ptr<Iterator> db_iter(db_->NewIterator(ReadOptions()));
    for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
      std::string key = db_iter->key().ToString();
      std::string value = db_iter->value().ToString();
      ASSERT_NE(key_value_written.find(key), key_value_written.end());
      ASSERT_EQ(key_value_written[key], value);
      key_value_written.erase(key);
    }
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(0, key_value_written.size());
  } else if (compression_failure_type_ == kTestDecompressionFail) {
    ASSERT_EQ(s.code(), Status::kCorruption);
    ASSERT_NE(st, nullptr);
    ASSERT_EQ(std::string(st), "Could not decompress: kTestDecompressionFail");
  } else if (compression_failure_type_ == kTestDecompressionCorruption) {
    ASSERT_EQ(s.code(), Status::kCorruption);
    ASSERT_NE(st, nullptr);
    ASSERT_EQ(std::string(st),
              "Decompressed block did not match pre-compression block");
  } else if (compression_failure_type_ == kTestStartOfFinishFail) {
    ASSERT_EQ(s.code(), Status::kCorruption);
    ASSERT_NE(st, nullptr);
    ASSERT_EQ(std::string(st), "Seeded failure");
  }
}

TEST_F(DBCompressionTest, VerifyCompressionChecksFinalCompressedBlockContents) {
  CompressionType compression_type = kNoCompression;
  for (CompressionType supported : GetSupportedCompressions()) {
    if (supported != kNoCompression) {
      compression_type = supported;
      break;
    }
  }
  if (compression_type == kNoCompression) {
    return;
  }

  struct SyncPointCleanup {
    ~SyncPointCleanup() {
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
    }
  } sync_point_cleanup;

  for (uint32_t parallel_threads : {1, 4}) {
    SCOPED_TRACE("parallel_threads=" + std::to_string(parallel_threads));

    Options options = CurrentOptions();
    options.compression = compression_type;
    options.compression_opts.max_compressed_bytes_per_kb = 1024;
    options.compression_opts.parallel_threads = parallel_threads;

    BlockBasedTableOptions table_options;
    table_options.block_size = 512;
    table_options.verify_compression = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    DestroyAndReopen(options);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    std::atomic<int> tamper_count{0};
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::CompressAndVerifyBlock:"
        "TamperWithCompressedDataBeforeVerify",
        [&](void* arg) {
          auto* output = static_cast<GrowableBuffer*>(arg);
          ASSERT_FALSE(output->empty());
          output->data()[output->size() - 1]++;
          tamper_count++;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(405);
    constexpr int kValUnitSize = 16;
    constexpr int kValSize = 256;
    for (int i = 0; i < 5; i++) {
      std::string value_unit = rnd.RandomString(kValUnitSize);
      std::string value;
      for (int j = 0; j < kValSize; j += kValUnitSize) {
        value += value_unit;
      }
      ASSERT_OK(Put(Key(i), value));
    }

    Status s = Flush();

    ASSERT_GT(tamper_count.load(), 0);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
}

TEST_F(DBCompressionTest, CompressionOptions) {
  if (!Zlib_Supported() || !Snappy_Supported()) {
    return;
  }

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 100;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 7;
  options.max_background_compactions = 1;

  CompactionCompressionListener* listener =
      new CompactionCompressionListener(&options);
  options.listeners.emplace_back(listener);

  const int kKeySize = 5;
  const int kValSize = 20;
  Random rnd(301);

  std::vector<uint32_t> compression_parallel_threads = {1, 4};

  std::map<std::string, std::string> key_value_written;

  for (int iter = 0; iter <= 2; iter++) {
    listener->max_level_checked = 0;

    if (iter == 0) {
      // Use different compression algorithms for different levels but
      // always use Zlib for bottommost level
      options.compression_per_level = {kNoCompression,     kNoCompression,
                                       kNoCompression,     kSnappyCompression,
                                       kSnappyCompression, kSnappyCompression,
                                       kZlibCompression};
      options.compression = kNoCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 1) {
      // Use Snappy except for bottommost level use ZLib
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kZlibCompression;
    } else if (iter == 2) {
      // Use Snappy everywhere
      options.compression_per_level = {};
      options.compression = kSnappyCompression;
      options.bottommost_compression = kDisableCompressionOption;
    }

    for (auto num_threads : compression_parallel_threads) {
      options.compression_opts.parallel_threads = num_threads;
      options.bottommost_compression_opts.parallel_threads = num_threads;

      DestroyAndReopen(options);
      // Write 10 random files
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 5; j++) {
          std::string key = rnd.RandomString(kKeySize);
          std::string value = rnd.RandomString(kValSize);
          key_value_written[key] = value;
          ASSERT_OK(Put(key, value));
        }
        ASSERT_OK(Flush());
        ASSERT_OK(dbfull()->TEST_WaitForCompact());
      }

      // Make sure that we wrote enough to check all 7 levels
      ASSERT_EQ(listener->max_level_checked, 6);

      // Make sure database content is the same as key_value_written
      std::unique_ptr<Iterator> db_iter(db_->NewIterator(ReadOptions()));
      for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
        std::string key = db_iter->key().ToString();
        std::string value = db_iter->value().ToString();
        ASSERT_NE(key_value_written.find(key), key_value_written.end());
        ASSERT_EQ(key_value_written[key], value);
        key_value_written.erase(key);
      }
      ASSERT_OK(db_iter->status());
      ASSERT_EQ(0, key_value_written.size());
    }
  }
}

TEST_F(DBCompressionTest, RoundRobinManager) {
  if (ZSTD_Supported()) {
    auto mgr =
        std::make_shared<RoundRobinManager>(GetBuiltinV2CompressionManager());

    std::vector<std::string> values;
    for (bool use_wrapper : {true}) {
      SCOPED_TRACE((use_wrapper ? "With " : "No ") + std::string("wrapper"));

      Options options = CurrentOptions();
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
      BlockBasedTableOptions bbto;
      bbto.enable_index_compression = false;
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      options.compression_manager = use_wrapper ? mgr : nullptr;
      DestroyAndReopen(options);

      Random rnd(301);
      constexpr int kCount = 13;

      // Highly compressible blocks, except 1 non-compressible. Half of the
      // compressible are morked for bypass and 1 marked for rejection. Values
      // are large enough to ensure just 1 k-v per block.
      for (int i = 0; i < kCount; ++i) {
        std::string value;
        if (i == 6) {
          // One non-compressible block
          value = rnd.RandomBinaryString(20000);
        } else {
          test::CompressibleString(&rnd, 0.1, 20000, &value);
        }
        values.push_back(value);
        ASSERT_OK(Put(Key(i), value));
        ASSERT_EQ(Get(Key(i)), value);
      }
      ASSERT_OK(Flush());

      // Ensure well-formed for reads
      for (int i = 0; i < kCount; ++i) {
        ASSERT_NE(Get(Key(i)), "NOT_FOUND");
        ASSERT_EQ(Get(Key(i)), values[i]);
      }
      ASSERT_EQ(Get(Key(kCount)), "NOT_FOUND");
    }
  }
}

TEST_F(DBCompressionTest, RandomMixedCompressionManager) {
  if (ZSTD_Supported()) {
    auto mgr = std::make_shared<RandomMixedCompressionManager>(
        GetBuiltinV2CompressionManager());
    std::vector<std::string> values;
    for (bool use_wrapper : {true}) {
      SCOPED_TRACE((use_wrapper ? "With " : "No ") + std::string("wrapper"));

      Options options = CurrentOptions();
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
      BlockBasedTableOptions bbto;
      bbto.enable_index_compression = false;
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      options.compression_manager = use_wrapper ? mgr : nullptr;
      DestroyAndReopen(options);

      Random rnd(301);
      constexpr int kCount = 13;

      // Highly compressible blocks, except 1 non-compressible. Half of the
      // compressible are morked for bypass and 1 marked for rejection. Values
      // are large enough to ensure just 1 k-v per block.
      for (int i = 0; i < kCount; ++i) {
        std::string value;
        if (i == 6) {
          // One non-compressible block
          value = rnd.RandomBinaryString(20000);
        } else {
          test::CompressibleString(&rnd, 0.1, 20000, &value);
        }
        values.push_back(value);
        ASSERT_OK(Put(Key(i), value));
        ASSERT_EQ(Get(Key(i)), value);
      }
      ASSERT_OK(Flush());

      // Ensure well-formed for reads
      for (int i = 0; i < kCount; ++i) {
        ASSERT_NE(Get(Key(i)), "NOT_FOUND");
        ASSERT_EQ(Get(Key(i)), values[i]);
      }
      ASSERT_EQ(Get(Key(kCount)), "NOT_FOUND");
    }
  }
}

namespace {
// Template parameter to distinguish data blocks vs. v4+ index blocks
template <bool kIndexBlockV4>
static Status ValidateRocksBlock(Slice data, bool use_separated_kv,
                                 uint64_t restart_interval) {
  assert(!kIndexBlockV4 ||
         !use_separated_kv);  // index blocks not currently supported
  const char* src = data.data();
  size_t srcSize = data.size();
  const char* const block_type_str =
      kIndexBlockV4 ? "Index block" : "Data block";

  // Decode footer using DataBlockFooter
  Slice input(src, srcSize);
  DataBlockFooter footer;
  Status s = footer.DecodeFrom(&input);
  if (!s.ok()) {
    return Status::Corruption(std::string(block_type_str) + " too small");
  }

  uint32_t numRestarts = footer.num_restarts;

  // Sanity check: num_restarts should be reasonable
  // TODO: also support data block hash index
  if (numRestarts > srcSize / 4 || numRestarts == 0) {
    return Status::Corruption(std::string("Invalid num_restarts in ") +
                              block_type_str);
  }

  size_t restartsSize = numRestarts * sizeof(uint32_t);
  if (input.size() < restartsSize) {
    return Status::Corruption(std::string(block_type_str) +
                              " too small for restarts array");
  }

  size_t entriesSize;
  uint32_t values_section_offset = 0;
  if (footer.separated_kv) {
    values_section_offset = footer.values_section_offset;
    entriesSize = values_section_offset;  // keys section ends at value_offset
  } else {
    entriesSize = input.size() - restartsSize;
  }
  const char* entriesEnd = src + entriesSize;

  // Parse entries
  const char* p = src;
  uint32_t cur_idx = 0;
  Slice current_value;
  while (p < entriesEnd) {
    // Parse shared_bytes varint
    uint32_t shared;
    const char* next = GetVarint32Ptr(p, entriesEnd, &shared);
    if (next == nullptr) {
      return Status::Corruption(std::string("Invalid shared_bytes varint in ") +
                                block_type_str);
    }
    p = next;

    // Parse unshared_bytes varint
    uint32_t unshared;
    next = GetVarint32Ptr(p, entriesEnd, &unshared);
    if (next == nullptr) {
      return Status::Corruption(
          std::string("Invalid unshared_bytes varint in ") + block_type_str);
    }
    p = next;

    uint32_t valueLen = 0;
    if constexpr (!kIndexBlockV4) {
      // For data blocks, parse value_length varint
      next = GetVarint32Ptr(p, entriesEnd, &valueLen);
      if (next == nullptr) {
        return Status::Corruption(
            std::string("Invalid value_length varint in ") + block_type_str);
      }
      p = next;
    }

    uint32_t value_offset = 0;
    if (cur_idx % restart_interval == 0 && use_separated_kv) {
      // For separated KV format, parse value_offset varint at restart points
      next = GetVarint32Ptr(p, entriesEnd, &value_offset);
      if (next == nullptr) {
        return Status::Corruption(
            std::string("Invalid value_offset varint in ") + block_type_str);
      }
      p = next;
    }

    // Validate key delta
    if (p + unshared > entriesEnd) {
      return Status::Corruption(
          std::string("Key delta exceeds end of entries in ") + block_type_str);
    }
    p += unshared;

    if constexpr (kIndexBlockV4) {
      // For v4 index blocks, value is self-describing (varints)
      // Parse first varint (always present)
      uint32_t v1;
      next = GetVarint32Ptr(p, entriesEnd, &v1);
      if (next == nullptr) {
        return Status::Corruption(std::string("Invalid value varint in ") +
                                  block_type_str);
      }
      p = next;

      // If shared_bytes == 0, there's a second varint
      if (shared == 0) {
        uint32_t v2;
        next = GetVarint32Ptr(p, entriesEnd, &v2);
        if (next == nullptr) {
          return Status::Corruption(
              std::string("Invalid second value varint in ") + block_type_str);
        }
        p = next;
      }
    } else {
      // For data blocks, validate value
      if (!use_separated_kv) {
        // Inline values: value follows key delta
        if (p + valueLen > entriesEnd) {
          return Status::Corruption(
              std::string("Value exceeds end of entries in ") + block_type_str);
        }
        p += valueLen;
      } else {
        // Separated KV: values are stored in a separate section
        // value_offset is relative to values section start (set at restart
        // points)
        if (cur_idx % restart_interval == 0) {
          current_value =
              Slice(src + values_section_offset + value_offset, valueLen);
        } else {
          // Non-restart entries: value immediately follows previous value
          current_value =
              Slice(current_value.data() + current_value.size(), valueLen);
        }

        if (current_value.data() + current_value.size() >
            src + srcSize - restartsSize) {
          return Status::Corruption(
              std::string("Value exceeds values section in ") + block_type_str);
        }
      }
    }

    ++cur_idx;
  }

  return Status::OK();
}
}  // anonymous namespace

class DBCompressionTestMaybeParallel
    : public DBCompressionTest,
      public testing::WithParamInterface<std::tuple<int, bool, bool>> {
 public:
  DBCompressionTestMaybeParallel()
      : DBCompressionTest(),
        parallel_threads_(std::get<0>(GetParam())),
        use_dict_(std::get<1>(GetParam())),
        separate_kv_(std::get<2>(GetParam())) {}

 protected:
  int parallel_threads_;
  bool use_dict_;
  bool separate_kv_;
};

INSTANTIATE_TEST_CASE_P(DBCompressionTest, DBCompressionTestMaybeParallel,
                        ::testing::Combine(::testing::Values(1, 4),
                                           ::testing::Values(false, true),
                                           ::testing::Values(false, true)));

TEST_P(DBCompressionTestMaybeParallel, CompressionManagerWrapper) {
  // Test that we can use a custom CompressionManager to wrap the built-in
  // CompressionManager, thus adopting a custom *strategy* based on existing
  // algorithms. This will "mark" some blocks (in their contents) as "do not
  // compress", i.e. no attempt to compress, and some blocks as "reject
  // compression", i.e. compression attempted but rejected because of ratio
  // or otherwise. These cases are distinguishable for statistics that
  // approximate "wasted effort".
  static std::string kDoNotCompress = "do_not_compress";
  static std::string kRejectCompression = "reject_compression";

  static RelaxedAtomic<int> dataCheckedCount{0};
  static RelaxedAtomic<int> indexCheckedCount{0};
  static RelaxedAtomic<int> compressCalledCount{0};
  static bool useSeparatedKV = false;

  // Set the separated KV flag for the wrappers
  useSeparatedKV = separate_kv_;

  // We also have wrappers here to help verify that when RocksDB asks to
  // specialize the Compressor for a particular kind of block, it only passes in
  // that kind of block to ensure proper grouping of related data for
  // compression. We check this by parsing the subtly distinct schemas of data
  // blocks vs. v4+ index blocks. This also ensures that structure-aware
  // compressions like OpenZL can parse the data block and index block formats.
  struct CheckDataBlockCompressorWrapper : public CompressorWrapper {
    using CompressorWrapper::CompressorWrapper;
    const char* Name() const override { return "CheckDataBlockCompressor"; }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<CheckDataBlockCompressorWrapper>(
          wrapped_->Clone());
    }

    Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                         size_t* compressed_output_size,
                         CompressionType* out_compression_type,
                         ManagedWorkingArea* working_area) override {
      dataCheckedCount.FetchAddRelaxed(1);
      // Parse and validate data block format before compressing
      Status s = ValidateRocksBlock</*kIndexBlockV4=*/false>(
          uncompressed_data, useSeparatedKV,
          BlockBasedTableOptions().block_restart_interval);
      if (!s.ok()) {
        return s;
      }
      // Delegate to wrapped compressor on success
      return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                     compressed_output_size,
                                     out_compression_type, working_area);
    }
  };

  struct CheckIndexBlockCompressorWrapper : public CompressorWrapper {
    using CompressorWrapper::CompressorWrapper;
    const char* Name() const override { return "CheckIndexBlockCompressor"; }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<CheckIndexBlockCompressorWrapper>(
          wrapped_->Clone());
    }

    Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                         size_t* compressed_output_size,
                         CompressionType* out_compression_type,
                         ManagedWorkingArea* working_area) override {
      indexCheckedCount.FetchAddRelaxed(1);
      // Parse and validate index block v4 format before compressing
      Status s = ValidateRocksBlock</*kIndexBlockV4=*/true>(
          uncompressed_data, false,
          BlockBasedTableOptions().index_block_restart_interval);
      if (!s.ok()) {
        return s;
      }
      // Delegate to wrapped compressor on success
      return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                     compressed_output_size,
                                     out_compression_type, working_area);
    }
  };

  struct MyCompressor : public CompressorWrapper {
    using CompressorWrapper::CompressorWrapper;
    const char* Name() const override { return "MyCompressor"; }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<MyCompressor>(wrapped_->Clone());
    }

    Status CompressBlock(Slice uncompressed_data, char* compressed_output,
                         size_t* compressed_output_size,
                         CompressionType* out_compression_type,
                         ManagedWorkingArea* working_area) override {
      compressCalledCount.FetchAddRelaxed(1);
      auto begin = uncompressed_data.data();
      auto end = uncompressed_data.data() + uncompressed_data.size();
      if (std::search(begin, end, kDoNotCompress.begin(),
                      kDoNotCompress.end()) != end) {
        // Do not attempt compression
        *compressed_output_size = 0;
        EXPECT_EQ(*out_compression_type, kNoCompression);
        return Status::OK();
      } else if (std::search(begin, end, kRejectCompression.begin(),
                             kRejectCompression.end()) != end) {
        // Simulate attempted & rejected compression
        *compressed_output_size = 1;
        EXPECT_EQ(*out_compression_type, kNoCompression);
        return Status::OK();
      } else {
        return wrapped_->CompressBlock(uncompressed_data, compressed_output,
                                       compressed_output_size,
                                       out_compression_type, working_area);
      }
    }

    // Also check WorkingArea handling
    struct MyWorkingArea : public WorkingArea {
      explicit MyWorkingArea(ManagedWorkingArea&& wrapped)
          : wrapped_(std::move(wrapped)) {}
      ManagedWorkingArea wrapped_;
    };

    ManagedWorkingArea ObtainWorkingArea() override {
      ManagedWorkingArea rv{
          new MyWorkingArea{CompressorWrapper::ObtainWorkingArea()}, this};
      if (GetPreferredCompressionType() == kZSTD) {
        // ZSTD should always use WorkingArea, so this is our chance to ensure
        // CompressorWrapper::ObtainWorkingArea() is properly connected
        assert(rv.get() != nullptr);
      }
      return rv;
    }

    void ReleaseWorkingArea(WorkingArea* wa) override {
      delete static_cast<MyWorkingArea*>(wa);
    }

    std::unique_ptr<Compressor> MaybeCloneSpecialized(
        CacheEntryRole block_type,
        DictConfigArgs&& dict_config) const override {
      std::unique_ptr<Compressor> result = std::make_unique<MyCompressor>(
          wrapped_->CloneMaybeSpecialized(block_type, std::move(dict_config)));
      if (block_type == CacheEntryRole::kDataBlock) {
        result = std::make_unique<CheckDataBlockCompressorWrapper>(
            std::move(result));
      } else if (block_type == CacheEntryRole::kIndexBlock) {
        result = std::make_unique<CheckIndexBlockCompressorWrapper>(
            std::move(result));
      }
      return result;
    }
  };
  struct MyManager : public CompressionManagerWrapper {
    using CompressionManagerWrapper::CompressionManagerWrapper;
    const char* Name() const override { return "MyManager"; }
    std::unique_ptr<Compressor> GetCompressorForSST(
        const FilterBuildingContext& context, const CompressionOptions& opts,
        CompressionType preferred) override {
      return std::make_unique<MyCompressor>(
          wrapped_->GetCompressorForSST(context, opts, preferred));
    }
  };
  auto mgr = std::make_shared<MyManager>(GetBuiltinV2CompressionManager());

  for (CompressionType type : GetSupportedCompressions()) {
    for (bool use_wrapper : {false, true}) {
      if (type == kNoCompression) {
        continue;
      }
      SCOPED_TRACE("Compression type: " + std::to_string(type) +
                   (use_wrapper ? " with " : " no ") + "wrapper" +
                   (separate_kv_ ? " separated_kv" : ""));

      Options options = CurrentOptions();
      options.compression = type;
      options.compression_opts.parallel_threads = parallel_threads_;
      options.compression_opts.max_dict_bytes = use_dict_ ? 4096 : 0;
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
      BlockBasedTableOptions bbto;
      bbto.enable_index_compression = true;
      bbto.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      bbto.partition_filters = true;
      bbto.filter_policy.reset(NewBloomFilterPolicy(5));
      bbto.separate_key_value_in_data_block = separate_kv_;
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      options.compression_manager = use_wrapper ? mgr : nullptr;
      DestroyAndReopen(options);

      auto PopStat = [&](Tickers t) -> uint64_t {
        return options.statistics->getAndResetTickerCount(t);
      };

      Random rnd(301);
      constexpr int kCount = 13;

      // Highly compressible blocks, except 1 non-compressible. Half of the
      // compressible are morked for bypass and 1 marked for rejection. Values
      // are large enough to ensure just 1 k-v per block.
      for (int i = 0; i < kCount; ++i) {
        std::string value;
        if (i == 6) {
          // One non-compressible block
          value = rnd.RandomBinaryString(20000);
        } else {
          test::CompressibleString(&rnd, 0.1, 20000, &value);
          if ((i % 2) == 0) {
            // Half for bypass
            value += kDoNotCompress;
          } else if (i == 7) {
            // One for rejection
            value += kRejectCompression;
          }
        }
        ASSERT_OK(Put(Key(i), value));
      }
      ASSERT_OK(Flush());

      // Index partition is compressed
      constexpr int kIdxComp = 1;
      // Top level index block is rejected for compression
      constexpr int kIdxRej = 1;

      if (use_dict_) {
        // FIXME: why don't the stats match? (for now, checking for crashes)
      } else if (use_wrapper) {
        EXPECT_EQ(kCount / 2 - 1 + kIdxComp, PopStat(NUMBER_BLOCK_COMPRESSED));
        EXPECT_EQ(kCount / 2, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
        EXPECT_EQ(1 + 1 + kIdxRej, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
      } else {
        EXPECT_EQ(kCount - 1 + kIdxComp, PopStat(NUMBER_BLOCK_COMPRESSED));
        EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
        EXPECT_EQ(1 + kIdxRej, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
      }

      // Ensure well-formed for reads
      for (int i = 0; i < kCount; ++i) {
        ASSERT_NE(Get(Key(i)), "NOT_FOUND");
      }
      ASSERT_EQ(Get(Key(kCount)), "NOT_FOUND");

      // Ensure expected checks were performed
      EXPECT_EQ(indexCheckedCount.ExchangeRelaxed(0),
                use_wrapper ? kIdxComp + kIdxRej : 0);
      EXPECT_EQ(dataCheckedCount.ExchangeRelaxed(0), use_wrapper ? kCount : 0);
      // And every use of MyCompressor went through either the data block
      // checker or index block checker
      EXPECT_EQ(compressCalledCount.ExchangeRelaxed(0),
                use_wrapper ? kIdxComp + kIdxRej + kCount : 0);
    }
  }
}

namespace {
std::string UniqueName(const std::string& base) {
  static RelaxedAtomic<int> counter{0};
  return base + std::to_string(counter.FetchAddRelaxed(1));
}
}  // anonymous namespace

TEST_P(DBCompressionTestMaybeParallel, CompressionManagerCustomCompression) {
  // Test that we can use a custom CompressionManager to implement custom
  // compression algorithms, and that there are appropriate schema guard rails
  // to ensure data is not processed by the wrong algorithm.
  using Compressor8A = test::CompressorCustomAlg<kCustomCompression8A>;
  using Compressor8B = test::CompressorCustomAlg<kCustomCompression8B>;
  using Compressor8C = test::CompressorCustomAlg<kCustomCompression8C>;

  if (!Compressor8A::Supported() || !LZ4_Supported()) {
    fprintf(stderr,
            "Prerequisite compression library not supported. Skipping\n");
    return;
  }

  class MyManager : public CompressionManager {
   public:
    explicit MyManager(const std::string& compat_name)
        : compat_name_(compat_name), name_("MyManager:" + compat_name_) {}
    const char* Name() const override { return name_.c_str(); }
    const char* CompatibilityName() const override {
      return compat_name_.c_str();
    }

    bool SupportsCompressionType(CompressionType type) const override {
      return type == kCustomCompression8A || type == kCustomCompression8B ||
             type == kCustomCompression8C ||
             GetBuiltinV2CompressionManager()->SupportsCompressionType(type);
    }

    int used_compressor8A_count_ = 0;
    int used_compressor8B_count_ = 0;
    int used_compressor8C_count_ = 0;

    std::unique_ptr<Compressor> GetCompressor(const CompressionOptions& opts,
                                              CompressionType type) override {
      switch (static_cast<unsigned char>(type)) {
        case kCustomCompression8A:
          used_compressor8A_count_++;
          return std::make_unique<Compressor8A>();
        case kCustomCompression8B:
          used_compressor8B_count_++;
          return std::make_unique<Compressor8B>();
        case kCustomCompression8C:
          used_compressor8C_count_++;
          return std::make_unique<Compressor8C>();
        // Also support built-in compression algorithms
        default:
          return GetBuiltinV2CompressionManager()->GetCompressor(opts, type);
      }
    }

    std::shared_ptr<Decompressor> GetDecompressor() override {
      return std::make_shared<test::DecompressorCustomAlg>();
    }

    RelaxedAtomic<CompressionType> last_specific_decompressor_type_{
        kNoCompression};

    std::shared_ptr<Decompressor> GetDecompressorForTypes(
        const CompressionType* types_begin,
        const CompressionType* types_end) override {
      assert(types_end > types_begin);
      last_specific_decompressor_type_.StoreRelaxed(*types_begin);
      auto decomp = std::make_shared<test::DecompressorCustomAlg>();
      decomp->SetAllowedTypes(types_begin, types_end);
      return decomp;
    }

    void AddFriend(const std::shared_ptr<CompressionManager>& mgr) {
      friends_[mgr->CompatibilityName()] = mgr;
    }
    std::shared_ptr<CompressionManager> FindCompatibleCompressionManager(
        Slice compatibility_name) override {
      std::shared_ptr<CompressionManager> rv =
          CompressionManager::FindCompatibleCompressionManager(
              compatibility_name);
      if (!rv) {
        auto it = friends_.find(compatibility_name.ToString());
        if (it != friends_.end()) {
          return it->second.lock();
        }
      }
      return rv;
    }

   private:
    std::string compat_name_;
    std::string name_;
    // weak_ptr to avoid cycles
    std::map<std::string, std::weak_ptr<CompressionManager>> friends_;
  };

  // Although these compression managers are actually compatible, we must
  // respect their distinct compatibility names and treat them as incompatible
  // (or else risk processing data incorrectly)
  // NOTE: these are not registered in ObjectRegistry to test what happens
  // when the original CompressionManager might not be available, but
  // mgr_bar will be registered during the test, with different names to
  // prevent interference between iterations.
  auto mgr_foo = std::make_shared<MyManager>("Foo");
  auto mgr_bar = std::make_shared<MyManager>(UniqueName("Bar"));

  // And this one claims to be fully compatible with the built-in compression
  // manager when it's not fully compatible (for custom CompressionTypes)
  auto mgr_claim_compatible = std::make_shared<MyManager>("BuiltinV2");

  constexpr uint16_t kValueSize = 10000;

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 20;
  BlockBasedTableOptions bbto;
  bbto.enable_index_compression = false;
  bbto.format_version = 6;  // Before custom compression alg support
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  // Claims not to use custom compression (and doesn't unless setting a custom
  // CompressionType)
  options.compression_manager = mgr_claim_compatible;
  // Use a built-in compression type with dictionary support
  options.compression = kLZ4Compression;
  options.compression_opts.max_dict_bytes = use_dict_ ? kValueSize / 2 : 0;
  options.compression_opts.parallel_threads = parallel_threads_;
  DestroyAndReopen(options);

  Random rnd(404);
  std::string value;
  ASSERT_OK(Put("a", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());

  // That data should be readable without access to the original compression
  // manager, because it used the built-in CompatibilityName and a built-in
  // CompressionType
  options.compression_manager = nullptr;
  Reopen(options);
  ASSERT_EQ(Get("a"), value);

  // Verify it was compressed
  Range r = {"a", "a0"};
  TablePropertiesCollection tables_properties;
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "LZ4");

  // Disallow setting a custom CompressionType with a CompressionManager
  // claiming to be built-in compatible.
  options.compression_manager = mgr_claim_compatible;
  options.compression = kCustomCompression8A;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  options.compression_manager = nullptr;
  options.compression = kCustomCompressionFE;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);
  options.compression =
      static_cast<CompressionType>(kLastBuiltinCompression + 1);
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  // Custom compression schema (different CompatibilityName) not supported
  // before format_version=7
  options.compression_manager = mgr_foo;
  options.compression = kLZ4Compression;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  // Set format version supporting custom compression
  bbto.format_version = 7;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  // Custom compression type not supported with built-in schema name, even
  // with format_version=7
  options.compression_manager = mgr_claim_compatible;
  options.compression = kCustomCompression8B;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);

  // Write data with mgr_claim_compatible (BuiltinV2 compat name) at fv=7
  // using a built-in compression type. This should produce the new format
  // compression_name property ("BuiltinV2;04;") and still be readable
  // without the original compression manager.
  options.compression_manager = mgr_claim_compatible;
  options.compression = kLZ4Compression;
  Reopen(options);
  ASSERT_OK(Put("a1", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("a1"), value);

  // Verify it was compressed with the new format property
  r = {"a1", "a10"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name,
            "BuiltinV2;04;");

  // Re-open without original compression manager, just with defaults.
  // Should work because the compat name is the built-in one.
  // As a sanity check to ensure the custom compression manager isn't leaking
  // into the re-open, verify that neither GetId() (used in OPTIONS files) nor
  // CompatibilityName() (used in SST files) resolves to the custom manager.
  // Both should resolve to the built-in manager instead.
  {
    ConfigOptions config_options;
    std::shared_ptr<CompressionManager> tmp;
    // GetId() is used in OPTIONS files. The custom manager's GetId()
    // ("MyManager:BuiltinV2") is not registered, so CreateFromString
    // returns OK with nullptr (unsupported options are ignored by default).
    ASSERT_OK(CompressionManager::CreateFromString(
        config_options, mgr_claim_compatible->GetId(), &tmp));
    ASSERT_EQ(tmp, nullptr);
    // CompatibilityName() is used in SST files. "BuiltinV2" resolves to
    // the built-in manager, not MyManager.
    ASSERT_OK(CompressionManager::CreateFromString(
        config_options, mgr_claim_compatible->CompatibilityName(), &tmp));
    ASSERT_NE(tmp, nullptr);
    EXPECT_STREQ(tmp->CompatibilityName(), "BuiltinV2");
    EXPECT_STRNE(tmp->Name(), mgr_claim_compatible->Name());
  }
  options.compression_manager = nullptr;
  Reopen(options);
  // Key "a" from fv=6 section still readable
  ASSERT_EQ(Get("a").size(), kValueSize);
  // Key "a1" from fv=7 section with BuiltinV2 compat name also readable
  ASSERT_EQ(Get("a1"), value);

  // Custom compression schema, but specifying a custom compression type it
  // doesn't support.
  options.compression_manager = mgr_foo;
  options.compression = kCustomCompressionF0;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kNotSupported);

  // Using a built-in compression type with fv=7 but named custom schema
  options.compression = kLZ4Compression;
  Reopen(options);
  ASSERT_OK(Put("b", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 3);
  ASSERT_EQ(Get("b"), value);

  // Verify it was compressed with LZ4
  r = {"b", "b0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  // Uses new format for "compression_name" property
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;04;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kLZ4Compression);

  // Custom compression type
  options.compression = kCustomCompression8A;
  Reopen(options);
  ASSERT_OK(Put("c", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  EXPECT_EQ(mgr_foo->used_compressor8A_count_, 0);
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);
  ASSERT_EQ(Get("c"), value);
  EXPECT_EQ(mgr_foo->used_compressor8A_count_, 1);

  // Verify it was compressed with custom format
  r = {"c", "c0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;8A;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kCustomCompression8A);

  // Also dynamically changeable, because the compression manager will respect
  // the current setting as reported under the legacy logic
  ASSERT_OK(dbfull()->SetOptions({{"compression", "kLZ4Compression"}}));
  ASSERT_OK(Put("d", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 5);
  ASSERT_EQ(Get("d"), value);

  // Verify it was compressed with LZ4
  r = {"d", "d0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;04;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kLZ4Compression);

  // Dynamically changeable to custom compressions also
  ASSERT_OK(dbfull()->SetOptions({{"compression", "kCustomCompression8B"}}));
  ASSERT_OK(Put("e", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 6);
  ASSERT_EQ(Get("e"), value);

  // Verify it was compressed with custom format
  r = {"e", "e0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(tables_properties.begin()->second->compression_name, "Foo;8B;");
  EXPECT_EQ(mgr_foo->last_specific_decompressor_type_.LoadRelaxed(),
            kCustomCompression8B);

  // Fails to re-open with incompatible compression manager (can't find
  // compression manager Foo because it's not registered nor known by Bar)
  options.compression_manager = mgr_bar;
  options.compression = kLZ4Compression;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kNotSupported);

  // But should re-open if we make Bar aware of the Foo compression manager
  mgr_bar->AddFriend(mgr_foo);
  Reopen(options);

  // Can still read everything
  ASSERT_EQ(Get("a").size(), kValueSize);
  ASSERT_EQ(Get("a1").size(), kValueSize);
  ASSERT_EQ(Get("b").size(), kValueSize);
  ASSERT_EQ(Get("c").size(), kValueSize);
  ASSERT_EQ(Get("d").size(), kValueSize);
  ASSERT_EQ(Get("e").size(), kValueSize);

  // Add a file using mgr_bar
  ASSERT_OK(Put("f", test::CompressibleString(&rnd, 0.1, kValueSize, &value)));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 7);
  ASSERT_EQ(Get("f"), value);

  // Verify it was compressed appropriately
  r = {"f", "f0"};
  tables_properties.clear();
  ASSERT_OK(db_->GetPropertiesOfTablesInRange(db_->DefaultColumnFamily(), &r, 1,
                                              &tables_properties));
  ASSERT_EQ(tables_properties.size(), 1U);
  EXPECT_LT(tables_properties.begin()->second->data_size, kValueSize / 2);
  EXPECT_EQ(mgr_bar->last_specific_decompressor_type_.LoadRelaxed(),
            kLZ4Compression);

  // Fails to re-open with incompatible compression manager (can't find
  // compression manager Bar because it's not registered nor known by Foo)
  options.compression_manager = mgr_foo;
  ASSERT_EQ(TryReopen(options).code(), Status::Code::kNotSupported);

  // Register and re-open
  auto& library = *ObjectLibrary::Default();
  library.AddFactory<CompressionManager>(
      mgr_bar->CompatibilityName(),
      [mgr_bar](const std::string& /*uri*/,
                std::unique_ptr<CompressionManager>* guard,
                std::string* /*errmsg*/) {
        *guard = std::make_unique<MyManager>(mgr_bar->CompatibilityName());
        return guard->get();
      });
  Reopen(options);

  // Can still read everything
  ASSERT_EQ(Get("a").size(), kValueSize);
  ASSERT_EQ(Get("a1").size(), kValueSize);
  ASSERT_EQ(Get("b").size(), kValueSize);
  ASSERT_EQ(Get("c").size(), kValueSize);
  ASSERT_EQ(Get("d").size(), kValueSize);
  ASSERT_EQ(Get("e").size(), kValueSize);
  ASSERT_EQ(Get("f").size(), kValueSize);

  // TODO: test old version of a compression manager unable to read a
  // compression type
}

TEST_F(DBCompressionTest, FailWhenCompressionNotSupportedTest) {
  CompressionType compressions[] = {kZlibCompression, kBZip2Compression,
                                    kLZ4Compression, kLZ4HCCompression,
                                    kXpressCompression};
  for (auto comp : compressions) {
    if (!CompressionTypeSupported(comp)) {
      // not supported, we should fail the Open()
      Options options = CurrentOptions();
      options.compression = comp;
      ASSERT_TRUE(!TryReopen(options).ok());
      // Try if CreateColumnFamily also fails
      options.compression = kNoCompression;
      ASSERT_OK(TryReopen(options));
      ColumnFamilyOptions cf_options(options);
      cf_options.compression = comp;
      ColumnFamilyHandle* handle;
      ASSERT_TRUE(!db_->CreateColumnFamily(cf_options, "name", &handle).ok());
    }
  }
}

// ===================== AutoSkip compression tests =====================
// AutoSkip is a first-class CompressionOptions feature (see
// CompressionOptions::auto_skip). These end-to-end tests drive it through the
// block-based table builder. Large values plus a small target block size make
// each Put land in roughly its own data block, so per-block AutoSkip decisions
// map to individual Puts.
class DBAutoSkipTest : public DBTestBase {
 public:
  DBAutoSkipTest() : DBTestBase("db_auto_skip_test", /*env_do_fsync=*/false) {}

  Options MakeOptions(bool auto_skip, int sample_every,
                      uint32_t parallel_threads, CompressionType type) {
    Options options = CurrentOptions();
    options.compression = type;
    options.bottommost_compression = type;
    options.compression_opts.auto_skip = auto_skip;
    options.compression_opts.auto_skip_min_sample_every = sample_every;
    options.compression_opts.parallel_threads = parallel_threads;
    options.statistics = CreateDBStatistics();
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    bbto.block_size = 4096;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    return options;
  }

  // Returns the value written, so callers can verify read-back.
  std::string PutOne(Random* rnd, int value_size, bool compressible) {
    std::string value = compressible ? std::string(value_size, 'A')
                                     : rnd->RandomBinaryString(value_size);
    EXPECT_OK(Put(Key(key_index_++), value));
    return value;
  }

  // First supported real (non-kNoCompression) codec, or kNoCompression if none.
  static CompressionType FirstSupportedCompression() {
    for (auto t : GetSupportedCompressions()) {
      if (t != kNoCompression) {
        return t;
      }
    }
    return kNoCompression;
  }

  // First supported codec that actually keeps parallel compression enabled.
  // Snappy (and the fast/accelerated levels of other codecs) recommend
  // parallel_threads=1, which forces the serial path; any other codec with a
  // non-fast level (>=1) keeps the requested parallelism.
  static CompressionType FirstParallelCapableCompression() {
    for (auto t : GetSupportedCompressions()) {
      if (t != kNoCompression && t != kSnappyCompression) {
        return t;
      }
    }
    return kNoCompression;
  }

  int key_index_ = 0;
};

TEST_F(DBAutoSkipTest, SkipsSustainedIncompressibleData) {
  const int kValueSize = 20000;
  const int kNum = 600;
  for (auto type : GetSupportedCompressions()) {
    if (type == kNoCompression) {
      continue;
    }
    SCOPED_TRACE("compression=" + std::to_string(static_cast<int>(type)));
    Options options = MakeOptions(/*auto_skip=*/true, /*sample_every=*/64,
                                  /*parallel_threads=*/1, type);
    DestroyAndReopen(options);
    Random rnd(301);
    for (int i = 0; i < kNum; ++i) {
      PutOne(&rnd, kValueSize, /*compressible=*/false);
    }
    ASSERT_OK(Flush());
    auto* stats = options.statistics.get();
    uint64_t bypassed =
        stats->getTickerCount(NUMBER_BLOCK_COMPRESSION_BYPASSED);
    uint64_t attempted =
        stats->getTickerCount(NUMBER_BLOCK_COMPRESSED) +
        stats->getTickerCount(NUMBER_BLOCK_COMPRESSION_REJECTED);
    // After the initial ramp-down and aside from occasional sampling, most
    // incompressible data blocks are bypassed rather than pointlessly
    // attempted.
    EXPECT_GT(bypassed, attempted);
  }
}

TEST_F(DBAutoSkipTest, DoesNotSkipCompressibleData) {
  const int kValueSize = 20000;
  const int kNum = 300;
  for (auto type : GetSupportedCompressions()) {
    if (type == kNoCompression) {
      continue;
    }
    SCOPED_TRACE("compression=" + std::to_string(static_cast<int>(type)));
    Options options = MakeOptions(/*auto_skip=*/true, /*sample_every=*/64,
                                  /*parallel_threads=*/1, type);
    DestroyAndReopen(options);
    Random rnd(302);
    for (int i = 0; i < kNum; ++i) {
      PutOne(&rnd, kValueSize, /*compressible=*/true);
    }
    ASSERT_OK(Flush());
    // Highly compressible data keeps the estimate below the threshold, so we
    // never enter the skip regime and never bypass a block.
    EXPECT_EQ(0, options.statistics->getTickerCount(
                     NUMBER_BLOCK_COMPRESSION_BYPASSED));
  }
}

TEST_F(DBAutoSkipTest, DisabledByDefaultMatchesNoBypass) {
  const int kValueSize = 20000;
  const int kNum = 200;
  CompressionType type = kNoCompression;
  for (auto t : GetSupportedCompressions()) {
    if (t != kNoCompression) {
      type = t;
      break;
    }
  }
  if (type == kNoCompression) {
    ROCKSDB_GTEST_SKIP("No compression library supported");
    return;
  }
  // auto_skip=false disables AutoSkip: incompressible data is
  // attempted-and-rejected (the pre-existing behavior), never
  // AutoSkip-"bypassed".
  Options options = MakeOptions(/*auto_skip=*/false, /*sample_every=*/0,
                                /*parallel_threads=*/1, type);
  DestroyAndReopen(options);
  Random rnd(303);
  for (int i = 0; i < kNum; ++i) {
    PutOne(&rnd, kValueSize, /*compressible=*/false);
  }
  ASSERT_OK(Flush());
  EXPECT_EQ(
      0, options.statistics->getTickerCount(NUMBER_BLOCK_COMPRESSION_BYPASSED));
}

TEST_F(DBAutoSkipTest, ParallelCompressionIntegrity) {
  CompressionType type = FirstParallelCapableCompression();
  if (type == kNoCompression) {
    ROCKSDB_GTEST_SKIP("No parallel-capable compression library supported");
    return;
  }
  const int kValueSize = 20000;
  const int kNum = 500;
  Options options = MakeOptions(/*auto_skip=*/true, /*sample_every=*/32,
                                /*parallel_threads=*/4, type);
  // A non-fast level keeps parallel compression enabled for every codec chosen
  // above (accelerated/fast variants would recommend parallel_threads=1).
  options.compression_opts.level = 3;
  DestroyAndReopen(options);

  // Prove we actually entered the parallel compression path rather than
  // silently falling back to serial (which would make this a weak test).
  std::atomic<int> parallel_started{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::MaybeStartParallelCompression:Started",
      [&](void* /*arg*/) { parallel_started.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(304);
  std::vector<std::string> values;
  values.reserve(kNum);
  // Alternate regimes so the estimate crosses the threshold both ways while
  // parallel workers share it.
  for (int i = 0; i < kNum; ++i) {
    bool compressible = (i / 50) % 2 == 0;
    values.push_back(PutOne(&rnd, kValueSize, compressible));
  }
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_GT(parallel_started.load(), 0);

  // All data must round-trip regardless of which blocks were skipped, and which
  // thread compressed/skipped them.
  for (int i = 0; i < kNum; ++i) {
    std::string got;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &got));
    ASSERT_EQ(values[i], got);
  }
}

TEST_F(DBAutoSkipTest, CarriesEstimateAcrossFiles) {
  CompressionType type = FirstSupportedCompression();
  if (type == kNoCompression) {
    ROCKSDB_GTEST_SKIP("No compression library supported");
    return;
  }
  const int kValueSize = 20000;
  const int kNum = 400;
  // Large sampling interval so periodic samples don't muddy the per-file
  // attempt counts compared below.
  Options options = MakeOptions(/*auto_skip=*/true, /*sample_every=*/1024,
                                /*parallel_threads=*/1, type);
  // A single flush thread guarantees consecutive flushes reuse the same thread,
  // whose thread-local carryover is exactly what this test exercises.
  options.max_background_jobs = 1;
  DestroyAndReopen(options);
  auto* stats = options.statistics.get();
  Random rnd(305);

  auto flush_and_count_attempts = [&](bool compressible) -> uint64_t {
    for (int i = 0; i < kNum; ++i) {
      PutOne(&rnd, kValueSize, compressible);
    }
    EXPECT_OK(Flush());
    uint64_t attempted =
        stats->getAndResetTickerCount(NUMBER_BLOCK_COMPRESSED) +
        stats->getAndResetTickerCount(NUMBER_BLOCK_COMPRESSION_REJECTED);
    stats->getAndResetTickerCount(NUMBER_BLOCK_COMPRESSION_BYPASSED);
    return attempted;
  };

  // Warm the flush thread's carryover down to a low (compressible) estimate so
  // this test is independent of whatever estimate earlier tests may have left
  // on the pooled flush thread.
  flush_and_count_attempts(/*compressible=*/true);

  // The first incompressible file must ramp its estimate up from that low
  // carried value, attempting many blocks before it is confident enough to
  // skip.
  uint64_t first_incompressible_attempts =
      flush_and_count_attempts(/*compressible=*/false);

  // The second incompressible file inherits the now-high estimate via the
  // thread's carryover, so it skips from the start and attempts only the forced
  // first block plus rare samples -- far fewer than the first file's ramp. If
  // carryover were broken, this file would re-ramp and attempt about as many.
  uint64_t second_incompressible_attempts =
      flush_and_count_attempts(/*compressible=*/false);

  EXPECT_GT(first_incompressible_attempts, second_incompressible_attempts + 8);
}

class CostAwareTestFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit CostAwareTestFlushBlockPolicy(const int window,
                                         const BlockBuilder& data_block_builder)
      : window_(window),
        num_keys_(0),
        data_block_builder_(data_block_builder) {}

  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    auto nth_window = num_keys_ / window_;
    if (data_block_builder_.empty()) {
      // First key in this block
      return false;
    }
    // Check every window
    if (num_keys_ % window_ == 0) {
      auto get_predictor = [&](void* arg) {
        // gets the predictor and sets the mocked cpu and io cost
        predictor_ = static_cast<IOCPUCostPredictor*>(arg);
        predictor_->CPUPredictor.SetPrediction(1000);
        predictor_->IOPredictor.SetPrediction(100);
      };
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();

      // Add syncpoint to get the cpu and io cost
      SyncPoint::GetInstance()->SetCallBack(
          "CostAwareCompressor::CompressBlockAndRecord::"
          "GetPredictor",
          get_predictor);
      SyncPoint::GetInstance()->EnableProcessing();
      // use nth window to detect test cases and set the expected
      switch (nth_window) {
        case 0:
          break;
        case 1:
          // Verify that the Mocked cpu cost and io cost are predicted correctly
          auto predicted_cpu_time = predictor_->CPUPredictor.Predict();
          auto predicted_io_bytes = predictor_->IOPredictor.Predict();
          EXPECT_EQ(predicted_io_bytes, 100);
          EXPECT_EQ(predicted_cpu_time, 1000);
          break;
      }
    }
    num_keys_++;
    return true;
  }

 private:
  int window_;
  int num_keys_;
  const BlockBuilder& data_block_builder_;
  IOCPUCostPredictor* predictor_;
};
class CostAwareTestFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit CostAwareTestFlushBlockPolicyFactory(const int window)
      : window_(window) {}

  const char* Name() const override {
    return "CostAwareTestFlushBlockPolicyFactory";
  }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& data_block_builder) const override {
    (void)data_block_builder;
    return new CostAwareTestFlushBlockPolicy(window_, data_block_builder);
  }

 private:
  int window_;
};
class DBCompressionCostPredictor : public DBTestBase {
 public:
  Options options;
  DBCompressionCostPredictor()
      : DBTestBase("db_cpuio_skip", /*env_do_fsync=*/true),
        options(CurrentOptions()) {
    options.compression_manager = CreateCostAwareCompressionManager();
    auto statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics = statistics;
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    bbto.flush_block_policy_factory.reset(
        new CostAwareTestFlushBlockPolicyFactory(10));
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);
  }
};
TEST_F(DBCompressionCostPredictor, CostAwareCompressorManager) {
  // making sure that the compression is supported
  if (!ZSTD_Supported()) {
    return;
  }
  const int kValueSize = 20000;
  int next_key = 0;
  Random rnd(231);
  auto value = rnd.RandomBinaryString(kValueSize);
  int window_size = 10;
  auto WindowWrite = [&]() {
    for (auto i = 0; i < window_size; ++i) {
      auto status = Put(Key(next_key), value);
      EXPECT_OK(status);
      next_key++;
    }
  };
  // This denotes the first window
  // Mocked to have specific cpu utilization and io cost
  WindowWrite();
  // check the predictor is predicting the correct cpu and io cost
  WindowWrite();
  ASSERT_OK(Flush());
}

// Test pre-defined dictionary compression with a custom CompressionManager
TEST_F(DBCompressionTest, PreDefinedDictionaryCompression) {
  if (!ZSTD_Supported()) {
    ROCKSDB_GTEST_BYPASS("ZSTD compression not supported");
    return;
  }

  // A custom compressor that returns a pre-defined dictionary
  class PreDefinedDictCompressor : public CompressorWrapper {
   public:
    explicit PreDefinedDictCompressor(std::unique_ptr<Compressor> wrapped,
                                      std::string dict_data)
        : CompressorWrapper(std::move(wrapped)),
          predefined_dict_(std::move(dict_data)) {}

    const char* Name() const override { return "PreDefinedDictCompressor"; }

    DictConfig GetDictGuidance(CacheEntryRole block_type) const override {
      if (block_type == CacheEntryRole::kDataBlock &&
          !predefined_dict_.empty()) {
        return DictPreDefined{/*copy*/ predefined_dict_};
      }
      return DictDisabled{};
    }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<PreDefinedDictCompressor>(wrapped_->Clone(),
                                                        predefined_dict_);
    }

    std::unique_ptr<Compressor> MaybeCloneSpecialized(
        CacheEntryRole block_type,
        DictConfigArgs&& dict_config) const override {
      // Delegate to wrapped compressor for dictionary handling
      auto specialized =
          wrapped_->MaybeCloneSpecialized(block_type, std::move(dict_config));
      if (specialized) {
        return specialized;
      }
      return nullptr;
    }

   private:
    std::string predefined_dict_;
  };

  // Custom CompatibilityName so the builtin compression manager won't be used
  static const char* kTestCompatibilityName = "PreDefinedDictTest";

  class PreDefinedDictManager : public CompressionManagerWrapper {
   public:
    explicit PreDefinedDictManager(std::shared_ptr<CompressionManager> wrapped,
                                   std::string dict_data)
        : CompressionManagerWrapper(std::move(wrapped)),
          predefined_dict_(std::move(dict_data)) {}

    const char* Name() const override { return "PreDefinedDictManager"; }

    const char* CompatibilityName() const override {
      return kTestCompatibilityName;
    }

    std::unique_ptr<Compressor> GetCompressorForSST(
        const FilterBuildingContext& context, const CompressionOptions& opts,
        CompressionType preferred) override {
      auto base = wrapped_->GetCompressorForSST(context, opts, preferred);
      if (base) {
        return std::make_unique<PreDefinedDictCompressor>(std::move(base),
                                                          predefined_dict_);
      }
      return nullptr;
    }

   private:
    std::string predefined_dict_;
  };

  // A broken manager that ignores the dictionary when decompressing.
  // This simulates a buggy decompressor that doesn't properly apply the
  // dictionary, causing ZSTD to produce wrong output when decompressing
  // dictionary-compressed data.
  class BrokenDictManager : public CompressionManagerWrapper {
   public:
    explicit BrokenDictManager(std::shared_ptr<CompressionManager> wrapped)
        : CompressionManagerWrapper(std::move(wrapped)) {}

    const char* Name() const override { return "BrokenDictManager"; }

    const char* CompatibilityName() const override {
      return kTestCompatibilityName;
    }

    std::shared_ptr<Decompressor> GetDecompressor() override {
      return std::make_shared<IgnoreDictDecompressor>(
          wrapped_->GetDecompressor());
    }

    std::shared_ptr<Decompressor> GetDecompressorOptimizeFor(
        CompressionType optimize_for_type) override {
      return std::make_shared<IgnoreDictDecompressor>(
          wrapped_->GetDecompressorOptimizeFor(optimize_for_type));
    }

    std::shared_ptr<Decompressor> GetDecompressorForTypes(
        const CompressionType* types_begin,
        const CompressionType* types_end) override {
      return std::make_shared<IgnoreDictDecompressor>(
          wrapped_->GetDecompressorForTypes(types_begin, types_end));
    }

   private:
    // A decompressor that stores the dictionary (for GetSerializedDict) but
    // ignores it during decompression, causing ZSTD to produce garbage
    class IgnoreDictDecompressor : public DecompressorWrapper {
     public:
      explicit IgnoreDictDecompressor(std::shared_ptr<Decompressor> wrapped)
          : DecompressorWrapper(std::move(wrapped)) {}

      IgnoreDictDecompressor(std::shared_ptr<Decompressor> wrapped,
                             std::string dict)
          : DecompressorWrapper(std::move(wrapped)),
            dict_(std::move(dict)),
            dict_slice_(dict_) {}

      const char* Name() const override { return "IgnoreDictDecompressor"; }

      const Slice& GetSerializedDict() const override { return dict_slice_; }

      Status MaybeCloneForDict(const Slice& serialized_dict,
                               std::unique_ptr<Decompressor>* out) override {
        // Store the dict but don't actually use it for decompression
        *out = std::make_unique<IgnoreDictDecompressor>(
            wrapped_,
            std::string(serialized_dict.data(), serialized_dict.size()));
        return Status::OK();
      }

     private:
      std::string dict_;
      Slice dict_slice_;
    };
  };

  // Create a dictionary that will be heavily referenced. The key insight is
  // that ZSTD dictionary compression works by finding matches between the input
  // data and the dictionary content. To force ZSTD to create dictionary
  // references, we need to use data that contains exact copies of dictionary
  // content.
  Random rnd(42);

  // Create a dictionary with recognizable patterns
  std::string predefined_dict;
  std::vector<std::string> dict_patterns;
  for (int i = 0; i < 50; i++) {
    std::string pattern = rnd.RandomString(200);
    dict_patterns.push_back(pattern);
    predefined_dict += pattern;
  }
  // Total dict size: 50 * 200 = 10000 bytes
  size_t kDictSize = predefined_dict.size();

  auto mgr = std::make_shared<PreDefinedDictManager>(
      GetBuiltinV2CompressionManager(), predefined_dict);

  Options options = CurrentOptions();
  options.compression = kZSTD;
  options.compression_opts.max_dict_bytes = static_cast<int>(kDictSize);
  options.compression_manager = mgr;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.enable_index_compression = true;
  // Need format_version >= 7 for custom CompatibilityName
  bbto.format_version = 7;
  // Need dictionary block load statistics
  bbto.block_cache = NewLRUCache(1 << 20);
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  // Write data that uses the same patterns from the dictionary.
  // This forces ZSTD to create back-references to the dictionary.
  std::vector<std::string> expected_values;
  for (int i = 0; i < 100; i++) {
    std::string value;
    // Compose value from random dictionary patterns - same content as dict
    for (int j = 0; j < 5; j++) {
      value +=
          dict_patterns[rnd.Uniform(static_cast<int>(dict_patterns.size()))];
    }
    expected_values.push_back(value);
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());

  // Verify dictionary was used by checking that dict bytes were inserted
  ASSERT_GE(
      TestGetTickerCount(options, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT),
      predefined_dict.size());

  // Read back data and verify correctness
  for (int i = 0; i < 100; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(value, expected_values[i]);
  }

  // Now re-open with a broken decompressor that ignores dictionary.
  // This should result in corruption on read because ZSTD will fail to
  // decompress data that references the missing dictionary content.
  Close();
  auto broken_mgr =
      std::make_shared<BrokenDictManager>(GetBuiltinV2CompressionManager());
  options.compression_manager = broken_mgr;
  // New block cache to ensure dictionary is re-loaded, because the
  // dictionary block in cache is actually associated with a decompressor
  bbto.block_cache = NewLRUCache(1 << 20);
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  ASSERT_OK(TryReopen(options));

  // Read should fail with corruption because the decompressor ignores
  // the dictionary, causing ZSTD to produce garbage output
  std::string value;
  ASSERT_EQ(db_->Get(ReadOptions(), Key(0), &value).code(),
            Status::kCorruption);
}

TEST_F(DBCompressionTest, GetRecommendedParallelThreads) {
  // Verify that built-in compressors return parallel_threads from their
  // CompressionOptions, except fast compressors override to 1
  auto mgr = GetBuiltinV2CompressionManager();
  CompressionOptions opts;

  // Default parallel_threads is 1
  opts.parallel_threads = 1;
  for (auto type : {kSnappyCompression, kZlibCompression, kLZ4Compression,
                    kLZ4HCCompression, kZSTD}) {
    if (!mgr->SupportsCompressionType(type)) {
      continue;
    }
    auto compressor = mgr->GetCompressor(opts, type);
    ASSERT_NE(compressor, nullptr);
    ASSERT_EQ(compressor->GetRecommendedParallelThreads(), 1U);
  }

  // Custom parallel_threads value (8) is returned unless a compressor overrides
  // it to 1 for being "fast". Snappy always overrides; Zlib never does. (Use a
  // positive level so it does not route ZSTD to an accelerated variant below.)
  opts.parallel_threads = 8;
  opts.level = 3;
  for (auto type : {kSnappyCompression, kZlibCompression, kZSTD}) {
    if (!mgr->SupportsCompressionType(type)) {
      continue;
    }
    auto compressor = mgr->GetCompressor(opts, type);
    ASSERT_NE(compressor, nullptr);
    uint32_t expected = type == kSnappyCompression ? 1U : 8U;
    ASSERT_EQ(compressor->GetRecommendedParallelThreads(), expected);
  }

  // LZ4 family: after unifying LZ4 and LZ4HC, the compression level (not the
  // configured type) selects the variant, and that determines parallelism:
  //   level <= 0 -> LZ4 fast -> overrides to 1
  //   level >= 1 -> LZ4HC    -> no override (parallel allowed)
  if (mgr->SupportsCompressionType(kLZ4Compression)) {
    for (auto type : {kLZ4Compression, kLZ4HCCompression}) {
      for (int level : {-10, -1, 0, 1, 4, 9, 12}) {
        SCOPED_TRACE("type=" + std::to_string(static_cast<int>(type)) +
                     " level=" + std::to_string(level));
        opts.level = level;
        auto compressor = mgr->GetCompressor(opts, type);
        ASSERT_NE(compressor, nullptr);
        uint32_t expected = level >= 1 ? 8U : 1U;
        ASSERT_EQ(compressor->GetRecommendedParallelThreads(), expected);
      }
    }
  }

  // ZSTD: accelerated (negative) levels override to 1. Level 0 is intentionally
  // left as a "backdoor" to allow parallel compression even though it otherwise
  // behaves like the fast level -1.
  if (mgr->SupportsCompressionType(kZSTD)) {
    opts.level = -1;
    auto compressor = mgr->GetCompressor(opts, kZSTD);
    ASSERT_NE(compressor, nullptr);
    ASSERT_EQ(compressor->GetRecommendedParallelThreads(), 1U);

    opts.level = 0;
    compressor = mgr->GetCompressor(opts, kZSTD);
    ASSERT_NE(compressor, nullptr);
    ASSERT_EQ(compressor->GetRecommendedParallelThreads(), 8U);
  }
}

TEST_F(DBCompressionTest, CompressionManagerOverridesParallelThreads) {
  // Test that a custom CompressionManager can override parallel_threads
  // by modifying CompressionOptions in GetCompressorForSST, and that the
  // override actually activates parallel compression.
  if (!ZSTD_Supported()) {
    ROCKSDB_GTEST_SKIP("ZSTD not supported");
    return;
  }

  // A manager that forces parallel_threads to a specific value
  class ParallelOverrideManager : public CompressionManagerWrapper {
   public:
    ParallelOverrideManager(std::shared_ptr<CompressionManager> wrapped,
                            uint32_t forced_threads)
        : CompressionManagerWrapper(std::move(wrapped)),
          forced_threads_(forced_threads) {}

    const char* Name() const override { return "ParallelOverrideManager"; }

    const char* CompatibilityName() const override {
      return wrapped_->CompatibilityName();
    }

    std::unique_ptr<Compressor> GetCompressorForSST(
        const FilterBuildingContext& context, const CompressionOptions& opts,
        CompressionType preferred) override {
      CompressionOptions modified_opts = opts;
      modified_opts.parallel_threads = forced_threads_;
      return wrapped_->GetCompressorForSST(context, modified_opts, preferred);
    }

   private:
    uint32_t forced_threads_;
  };

  Options options = CurrentOptions();
  options.compression = kZSTD;
  // Set parallel_threads=1 in the options, but the manager will override to 4
  options.compression_opts.parallel_threads = 1;

  auto mgr = std::make_shared<ParallelOverrideManager>(
      GetBuiltinV2CompressionManager(), 4);
  options.compression_manager = mgr;

  // Use sync point to verify parallel compression is activated with the
  // overridden thread count
  uint32_t observed_threads = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::MaybeStartParallelCompression:Started",
      [&](void* arg) { observed_threads = *static_cast<uint32_t*>(arg); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);

  // Write enough data to produce some blocks
  Random rnd(301);
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify parallel compression was activated with overridden thread count
  ASSERT_EQ(observed_threads, 4U);

  // Verify data is readable (parallel compression produced correct output)
  for (int i = 0; i < 100; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(value.size(), 100);
  }
}

TEST_F(DBCompressionTest, CompressorOverridesCompressionOptions) {
  // A Compressor can steer the subset of CompressionOptions that the builder
  // (rather than the Compressor) is responsible for applying by overriding
  // MaybeOverrideCompressionOptions(). Here it forces parallel_threads and we
  // verify parallel compression activates with that thread count via the
  // builder's sync point. This exercises the new hook directly, not the
  // deprecated GetRecommendedParallelThreads() bridge.
  if (!ZSTD_Supported()) {
    ROCKSDB_GTEST_SKIP("ZSTD not supported");
    return;
  }

  // Wraps a compressor and forces parallel_threads via the new hook.
  class ForceParallelCompressor : public CompressorWrapper {
   public:
    ForceParallelCompressor(std::unique_ptr<Compressor> wrapped,
                            uint32_t forced_threads)
        : CompressorWrapper(std::move(wrapped)),
          forced_threads_(forced_threads) {}

    const char* Name() const override { return "ForceParallelCompressor"; }

    void MaybeOverrideCompressionOptions(
        CompressionOptions* to_modify) const override {
      to_modify->parallel_threads = forced_threads_;
    }

    std::unique_ptr<Compressor> Clone() const override {
      return std::make_unique<ForceParallelCompressor>(wrapped_->Clone(),
                                                       forced_threads_);
    }

   private:
    uint32_t forced_threads_;
  };

  // Returns the wrapping compressor for SSTs.
  class ForceParallelManager : public CompressionManagerWrapper {
   public:
    ForceParallelManager(std::shared_ptr<CompressionManager> wrapped,
                         uint32_t forced_threads)
        : CompressionManagerWrapper(std::move(wrapped)),
          forced_threads_(forced_threads) {}

    const char* Name() const override { return "ForceParallelManager"; }

    std::unique_ptr<Compressor> GetCompressorForSST(
        const FilterBuildingContext& context, const CompressionOptions& opts,
        CompressionType preferred) override {
      auto inner = wrapped_->GetCompressorForSST(context, opts, preferred);
      if (inner == nullptr) {
        return nullptr;
      }
      return std::make_unique<ForceParallelCompressor>(std::move(inner),
                                                       forced_threads_);
    }

   private:
    uint32_t forced_threads_;
  };

  Options options = CurrentOptions();
  options.compression = kZSTD;
  // Configure single-threaded; the compressor's override raises it to 4.
  options.compression_opts.parallel_threads = 1;

  auto mgr = std::make_shared<ForceParallelManager>(
      GetBuiltinV2CompressionManager(), 4);
  options.compression_manager = mgr;

  uint32_t observed_threads = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::MaybeStartParallelCompression:Started",
      [&](void* arg) { observed_threads = *static_cast<uint32_t*>(arg); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Parallel compression activated with the compressor-overridden thread count.
  ASSERT_EQ(observed_threads, 4U);

  // Data is readable (parallel compression produced correct output).
  for (int i = 0; i < 100; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(value.size(), 100);
  }
}

TEST_F(DBCompressionTest, UnifiedLZ4LZ4HCLevels) {
  // LZ4 and LZ4HC share the same wire format and decompressor, so the
  // compression level alone selects which algorithm runs. A given non-default
  // level therefore produces identical output regardless of which of the two
  // types is configured, and the recorded compression type follows the
  // compressor actually used (LZ4 fast -> kLZ4Compression, LZ4HC ->
  // kLZ4HCCompression).
  auto mgr = GetBuiltinV2CompressionManager();
  if (!mgr->SupportsCompressionType(kLZ4Compression)) {
    ROCKSDB_GTEST_SKIP("LZ4 not supported");
    return;
  }
  auto decompressor = mgr->GetDecompressor();

  // Highly compressible input so compression is accepted.
  std::string input;
  for (int i = 0; i < 256; i++) {
    input.append("abcdefgh");
  }

  auto compress_one = [&](CompressionType configured_type, int level,
                          std::string* out, CompressionType* actual) {
    CompressionOptions opts;
    opts.level = level;
    auto compressor = mgr->GetCompressor(opts, configured_type);
    ASSERT_NE(compressor, nullptr);
    out->resize(input.size() * 2 + 1024);
    size_t out_size = out->size();
    CompressionType type_out = kNoCompression;
    ASSERT_OK(compressor->CompressBlock(input, out->data(), &out_size,
                                        &type_out, nullptr));
    ASSERT_NE(type_out, kNoCompression);
    out->resize(out_size);
    *actual = type_out;
  };

  auto round_trip = [&](const std::string& compressed, CompressionType type) {
    Decompressor::Args args;
    args.compression_type = type;
    args.compressed_data = Slice(compressed);
    ASSERT_OK(decompressor->ExtractUncompressedSize(args));
    std::string uncompressed;
    uncompressed.resize(args.uncompressed_size);
    ASSERT_OK(decompressor->DecompressBlock(args, uncompressed.data()));
    ASSERT_EQ(uncompressed, input);
  };

  // Non-default levels: the configured type does not matter (kLZ4Compression
  // and kLZ4HCCompression produce identical output and the same recorded type,
  // which the level itself selects). Levels that map to the same effective
  // algorithm parameter after clamping compress identically: LZ4 fast uses
  // acceleration 1 for level 0 or -1 and caps acceleration at 65537
  // (level <= -65537); LZ4HC caps the level at 12.
  std::string accel1;     // LZ4 fast, acceleration 1
  std::string accel_max;  // LZ4 fast, acceleration clamped to 65537
  std::string hc_max;     // LZ4HC, level clamped to 12
  for (int level : {-1, 0, -3, -65537, -65538, -1000000, -2000000000, 1, 4, 9,
                    12, 13, 100, 1000, 2000000000}) {
    SCOPED_TRACE("level=" + std::to_string(level));
    std::string out_lz4;
    std::string out_lz4hc;
    CompressionType actual_lz4;
    CompressionType actual_lz4hc;
    compress_one(kLZ4Compression, level, &out_lz4, &actual_lz4);
    compress_one(kLZ4HCCompression, level, &out_lz4hc, &actual_lz4hc);

    // The configured type does not affect the result for a given level.
    ASSERT_EQ(actual_lz4, actual_lz4hc);
    ASSERT_EQ(out_lz4, out_lz4hc);

    // The level sign selects the algorithm (and the recorded type).
    ASSERT_EQ(actual_lz4, level >= 1 ? kLZ4HCCompression : kLZ4Compression);

    round_trip(out_lz4, actual_lz4);

    // Levels that clamp to the same effective parameter compress identically.
    if (level == -1) {
      accel1 = out_lz4;
    } else if (level == 0) {
      ASSERT_EQ(out_lz4, accel1);
    } else if (level == -65537) {
      accel_max = out_lz4;
    } else if (level < -65537) {
      ASSERT_EQ(out_lz4, accel_max);
    } else if (level == 12) {
      hc_max = out_lz4;
    } else if (level > 12) {
      ASSERT_EQ(out_lz4, hc_max);
    }
  }

  // Default level: the configured type selects fast (LZ4) vs HC (LZ4HC).
  {
    std::string out;
    CompressionType actual;
    compress_one(kLZ4Compression, CompressionOptions::kDefaultCompressionLevel,
                 &out, &actual);
    ASSERT_EQ(actual, kLZ4Compression);
    round_trip(out, actual);

    compress_one(kLZ4HCCompression,
                 CompressionOptions::kDefaultCompressionLevel, &out, &actual);
    ASSERT_EQ(actual, kLZ4HCCompression);
    round_trip(out, actual);
  }
}

TEST_F(DBCompressionTest, SnappyCompressBlockRoundTrip) {
  auto mgr = GetBuiltinV2CompressionManager();
  if (!mgr->SupportsCompressionType(kSnappyCompression)) {
    ROCKSDB_GTEST_SKIP("Snappy not supported");
    return;
  }
  auto decompressor = mgr->GetDecompressor();

  // 4 KB of highly compressible bytes: Snappy calls GetAppendBuffer then calls
  // Append with the same pointer. This triggered a valgrind memcheck warning:
  // Source and destination overlap in memcpy
  std::string input(4096, 'a');

  CompressionOptions opts;
  auto compressor = mgr->GetCompressor(opts, kSnappyCompression);
  ASSERT_NE(compressor, nullptr);

  std::string output(input.size() * 2 + 1024, '\0');
  size_t out_size = output.size();
  CompressionType actual = kNoCompression;
  ASSERT_OK(compressor->CompressBlock(input, output.data(), &out_size, &actual,
                                      nullptr));
  ASSERT_EQ(actual, kSnappyCompression);
  output.resize(out_size);

  Decompressor::Args args;
  args.compression_type = kSnappyCompression;
  args.compressed_data = Slice(output);
  ASSERT_OK(decompressor->ExtractUncompressedSize(args));
  std::string recovered(args.uncompressed_size, '\0');
  ASSERT_OK(decompressor->DecompressBlock(args, recovered.data()));
  ASSERT_EQ(recovered, input);
}

TEST_F(DBCompressionTest, ZSTDLevelZeroMapsToMinusOne) {
  // ZSTD itself treats a requested compression level of 0 as "use the default
  // level" (historically 3). That makes level 0 a discontinuity in the
  // otherwise monotonic level spectrum: it would compress more strongly than
  // levels -1, -2, etc. RocksDB instead maps level 0 to -1 so the spectrum is
  // continuous, which is friendlier to auto-tuning. Verify that level 0
  // compresses identically to level -1 and differently from level 3 (the ZSTD
  // default).
  auto mgr = GetBuiltinV2CompressionManager();
  if (!mgr->SupportsCompressionType(kZSTD)) {
    ROCKSDB_GTEST_SKIP("ZSTD not supported");
    return;
  }
  auto decompressor = mgr->GetDecompressor();

  // Semi-compressible input large enough that nearby ZSTD levels (-1 vs 3)
  // produce different output.
  Random rnd(301);
  std::string input;
  for (int i = 0; i < 2000; i++) {
    input.append(rnd.RandomString(16));
    input.append(8, 'x');
  }

  auto compress_one = [&](int level, std::string* out) {
    CompressionOptions opts;
    opts.level = level;
    auto compressor = mgr->GetCompressor(opts, kZSTD);
    ASSERT_NE(compressor, nullptr);
    out->resize(input.size() * 2 + 1024);
    size_t out_size = out->size();
    CompressionType type_out = kNoCompression;
    ASSERT_OK(compressor->CompressBlock(input, out->data(), &out_size,
                                        &type_out, nullptr));
    ASSERT_EQ(type_out, kZSTD);
    out->resize(out_size);
  };

  auto round_trip = [&](const std::string& compressed) {
    Decompressor::Args args;
    args.compression_type = kZSTD;
    args.compressed_data = Slice(compressed);
    ASSERT_OK(decompressor->ExtractUncompressedSize(args));
    std::string uncompressed;
    uncompressed.resize(args.uncompressed_size);
    ASSERT_OK(decompressor->DecompressBlock(args, uncompressed.data()));
    ASSERT_EQ(uncompressed, input);
  };

  std::string out_level0;
  std::string out_level_minus1;
  std::string out_level3;
  compress_one(0, &out_level0);
  compress_one(-1, &out_level_minus1);
  compress_one(3, &out_level3);

  // Level 0 behaves like level -1, not like the ZSTD default (3).
  ASSERT_EQ(out_level0, out_level_minus1);
  ASSERT_NE(out_level0, out_level3);

  round_trip(out_level0);
  round_trip(out_level_minus1);
  round_trip(out_level3);
}

TEST_F(DBCompressionTest, ConfiguredCompressionTypeRecordedInProperties) {
  // The configured compression type is recorded as a `_type=<decimal>`
  // pseudo-option in the SST `rocksdb.compression_options` table property, so
  // it can be recovered for debugging even when the per-block recorded type
  // differs from the configured type.
  if (!CompressionTypeSupported(kLZ4Compression)) {
    ROCKSDB_GTEST_SKIP("LZ4 not supported");
    return;
  }
  for (auto type : {kLZ4Compression, kLZ4HCCompression}) {
    Options options = CurrentOptions();
    options.compression = type;
    DestroyAndReopen(options);
    Random rnd(301);
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
    }
    ASSERT_OK(Flush());

    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_FALSE(props.empty());
    std::string expected = "_type=" + std::to_string(static_cast<int>(type));
    for (const auto& kv : props) {
      ASSERT_NE(kv.second->compression_options.find(expected),
                std::string::npos)
          << "compression_options=" << kv.second->compression_options;
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE
int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
