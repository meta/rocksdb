//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <future>
#include <memory>
#include <set>

#include "db/db_test_util.h"
#include "db/version_set.h"
#include "port/lang.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "util/random.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// TODO(cbi): parameterize the test to cover user-defined timestamp cases
class DBRangeDelTest : public DBTestBase {
 public:
  DBRangeDelTest() : DBTestBase("db_range_del_test", /*env_do_fsync=*/false) {}

  std::string GetNumericStr(int key) {
    uint64_t uint64_key = static_cast<uint64_t>(key);
    std::string str;
    str.resize(8);
    memcpy(str.data(), static_cast<void*>(&uint64_key), 8);
    return str;
  }
};

TEST_F(DBRangeDelTest, NonBlockBasedTableNotSupported) {
  // TODO: figure out why MmapReads trips the iterator pinning assertion in
  // RangeDelAggregator. Ideally it would be supported; otherwise it should at
  // least be explicitly unsupported.
  for (auto config : {kPlainTableAllBytesPrefix, /* kWalDirAndMmapReads */}) {
    option_config_ = config;
    DestroyAndReopen(CurrentOptions());
    ASSERT_TRUE(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "dr1", "dr1")
                    .IsNotSupported());
  }
}

TEST_F(DBRangeDelTest, WriteBatchWithIndexNotSupported) {
  WriteBatchWithIndex indexedBatch{};
  ASSERT_TRUE(indexedBatch.DeleteRange(db_->DefaultColumnFamily(), "dr1", "dr1")
                  .IsNotSupported());
  ASSERT_TRUE(indexedBatch.DeleteRange("dr1", "dr1").IsNotSupported());
}

TEST_F(DBRangeDelTest, EndSameAsStartCoversNothing) {
  ASSERT_OK(db_->Put(WriteOptions(), "b", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "b"));
  ASSERT_EQ("val", Get("b"));
}

TEST_F(DBRangeDelTest, EndComesBeforeStartInvalidArgument) {
  ASSERT_OK(db_->Put(WriteOptions(), "b", "val"));
  ASSERT_TRUE(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "a")
          .IsInvalidArgument());
  ASSERT_EQ("val", Get("b"));
}

TEST_F(DBRangeDelTest, FlushOutputHasOnlyRangeTombstones) {
  do {
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "dr1", "dr2"));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_EQ(1, NumTableFilesAtLevel(0));
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, DictionaryCompressionWithOnlyRangeTombstones) {
  Options opts = CurrentOptions();
  opts.compression_opts.max_dict_bytes = 16384;
  Reopen(opts);
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr1",
                             "dr2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
}

TEST_F(DBRangeDelTest, CompactionOutputHasOnlyRangeTombstone) {
  do {
    Options opts = CurrentOptions();
    opts.disable_auto_compactions = true;
    opts.statistics = CreateDBStatistics();
    DestroyAndReopen(opts);

    // snapshot protects range tombstone from dropping due to becoming obsolete.
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    ASSERT_EQ(1, NumTableFilesAtLevel(0));
    ASSERT_EQ(0, NumTableFilesAtLevel(1));
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                          true /* disallow_trivial_move */));
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(1, NumTableFilesAtLevel(1));
    ASSERT_EQ(0, TestGetTickerCount(opts, COMPACTION_RANGE_DEL_DROP_OBSOLETE));
    db_->ReleaseSnapshot(snapshot);
    // Skip cuckoo memtables, which do not support snapshots. Skip non-leveled
    // compactions as the above assertions about the number of files in a level
    // do not hold true.
  } while (ChangeOptions(kRangeDelSkipConfigs | kSkipUniversalCompaction |
                         kSkipFIFOCompaction));
}

TEST_F(DBRangeDelTest, CompactionOutputFilesExactlyFilled) {
  // regression test for exactly filled compaction output files. Previously
  // another file would be generated containing all range deletions, which
  // could invalidate the non-overlapping file boundary invariant.
  const int kNumPerFile = 4;
  const int kNumFiles = 2;
  const int kFileBytes = 9 << 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  options.num_levels = 2;
  options.target_file_size_base = kFileBytes;
  BlockBasedTableOptions table_options;
  table_options.block_size_deviation = 50;  // each block holds two keys
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
      if (j == 0 && i > 0) {
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
    }
  }
  // put extra key to trigger final flush
  ASSERT_OK(Put("", ""));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, MaxCompactionBytesCutsOutputFiles) {
  // Ensures range deletion spanning multiple compaction output files that are
  // cut by max_compaction_bytes will have non-overlapping key-ranges.
  // https://github.com/meta/rocksdb/issues/1778
  const int kNumFiles = 2;
  const int kNumPerFile = 1 << 8;
  const int kBytesPerVal = 1 << 12;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.disable_auto_compactions = true;
  opts.level0_file_num_compaction_trigger = kNumFiles;
  opts.max_compaction_bytes = kNumPerFile * kBytesPerVal;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  // Want max_compaction_bytes to trigger the end of compaction output file, not
  // target_file_size_base, so make the latter much bigger
  //  opts.target_file_size_base = 100 * opts.max_compaction_bytes;
  opts.target_file_size_base = 1;
  DestroyAndReopen(opts);

  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  Random rnd(301);

  ASSERT_OK(Put(GetNumericStr(0), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(
      Put(GetNumericStr(kNumPerFile - 1), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(GetNumericStr(kNumPerFile), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(
      Put(GetNumericStr(kNumPerFile * 2 - 1), rnd.RandomString(kBytesPerVal)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(NumTableFilesAtLevel(2), 2);

  ASSERT_OK(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"target_file_size_base",
                        std::to_string(100 * opts.max_compaction_bytes)}}));

  // It spans the whole key-range, thus will be included in all output files
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             GetNumericStr(0),
                             GetNumericStr(kNumFiles * kNumPerFile - 1)));

  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 1MB (256 values, each 4K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(kBytesPerVal));
      ASSERT_OK(Put(GetNumericStr(kNumPerFile * i + j), values[j]));
    }
    // extra entry to trigger SpecialSkipListFactory's flush
    ASSERT_OK(Put(GetNumericStr(kNumPerFile), ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
  }

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr,
                                        /*column_family=*/nullptr,
                                        /*disallow_trivial_move=*/true));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GE(NumTableFilesAtLevel(1), 2);
  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);

  for (size_t i = 0; i + 1 < files[1].size(); ++i) {
    ASSERT_TRUE(InternalKeyComparator(opts.comparator)
                    .Compare(files[1][i].largest, files[1][i + 1].smallest) <
                0);
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, SentinelsOmittedFromOutputFile) {
  // Regression test for bug where sentinel range deletions (i.e., ones with
  // sequence number of zero) were included in output files.
  // snapshot protects range tombstone from dropping due to becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();

  // gaps between ranges creates sentinels in our internal representation
  std::vector<std::pair<std::string, std::string>> range_dels = {
      {"a", "b"}, {"c", "d"}, {"e", "f"}};
  for (const auto& range_del : range_dels) {
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               range_del.first, range_del.second));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
  ASSERT_GT(files[0][0].fd.smallest_seqno, 0);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, FlushRangeDelsSameStartKey) {
  ASSERT_OK(db_->Put(WriteOptions(), "b1", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "c"));
  ASSERT_OK(db_->Put(WriteOptions(), "b2", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  // first iteration verifies query correctness in memtable, second verifies
  // query correctness for a single SST file
  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
      ASSERT_EQ(1, NumTableFilesAtLevel(0));
    }
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "b1", &value).IsNotFound());
    ASSERT_OK(db_->Get(ReadOptions(), "b2", &value));
  }
}

TEST_F(DBRangeDelTest, CompactRangeDelsSameStartKey) {
  ASSERT_OK(db_->Put(WriteOptions(), "unused",
                     "val"));  // prevents empty after compaction
  ASSERT_OK(db_->Put(WriteOptions(), "b1", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "c"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(3, NumTableFilesAtLevel(0));

  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                            true /* disallow_trivial_move */));
      ASSERT_EQ(0, NumTableFilesAtLevel(0));
      ASSERT_EQ(1, NumTableFilesAtLevel(1));
    }
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), "b1", &value).IsNotFound());
  }
}

TEST_F(DBRangeDelTest, FlushRemovesCoveredKeys) {
  const int kNum = 300;
  const int kRangeBegin = 50;
  const int kRangeEnd = 250;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  DestroyAndReopen(opts);

  // Write a third before snapshot, a third between snapshot and tombstone, and
  // a third after the tombstone. Keys older than snapshot or newer than the
  // tombstone should be preserved.
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 3) {
      snapshot = db_->GetSnapshot();
    } else if (i == 2 * kNum / 3) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  for (int i = 0; i < kNum; ++i) {
    ReadOptions read_opts;
    read_opts.ignore_range_deletions = true;
    std::string value;
    if (i < kRangeBegin || i > kRangeEnd || i < kNum / 3 || i >= 2 * kNum / 3) {
      ASSERT_OK(db_->Get(read_opts, GetNumericStr(i), &value));
    } else {
      ASSERT_TRUE(db_->Get(read_opts, GetNumericStr(i), &value).IsNotFound());
    }
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, CompactionRemovesCoveredKeys) {
  const int kNumPerFile = 100;
  const int kNumFiles = 4;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.disable_auto_compactions = true;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  opts.num_levels = 2;
  opts.statistics = CreateDBStatistics();
  DestroyAndReopen(opts);

  for (int i = 0; i < kNumFiles; ++i) {
    if (i > 0) {
      // range tombstone covers first half of the previous file
      ASSERT_OK(db_->DeleteRange(
          WriteOptions(), db_->DefaultColumnFamily(),
          GetNumericStr((i - 1) * kNumPerFile),
          GetNumericStr((i - 1) * kNumPerFile + kNumPerFile / 2)));
    }
    // Make sure a given key appears in each file so compaction won't be able to
    // use trivial move, which would happen if the ranges were non-overlapping.
    // Also, we need an extra element since flush is only triggered when the
    // number of keys is one greater than SpecialSkipListFactory's limit.
    // We choose a key outside the key-range used by the test to avoid conflict.
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(kNumPerFile * kNumFiles),
                       "val"));

    for (int j = 0; j < kNumPerFile; ++j) {
      ASSERT_OK(
          db_->Put(WriteOptions(), GetNumericStr(i * kNumPerFile + j), "val"));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ((kNumFiles - 1) * kNumPerFile / 2,
            TestGetTickerCount(opts, COMPACTION_KEY_DROP_RANGE_DEL));

  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumPerFile; ++j) {
      ReadOptions read_opts;
      read_opts.ignore_range_deletions = true;
      std::string value;
      if (i == kNumFiles - 1 || j >= kNumPerFile / 2) {
        ASSERT_OK(
            db_->Get(read_opts, GetNumericStr(i * kNumPerFile + j), &value));
      } else {
        ASSERT_TRUE(
            db_->Get(read_opts, GetNumericStr(i * kNumPerFile + j), &value)
                .IsNotFound());
      }
    }
  }
}

TEST_F(DBRangeDelTest, ValidLevelSubcompactionBoundaries) {
  const int kNumPerFile = 100;
  const int kNumFiles = 4;
  const int kFileBytes = 100 << 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.max_bytes_for_level_base = 2 * kFileBytes;
  options.max_subcompactions = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  options.num_levels = 3;
  options.target_file_size_base = kFileBytes;
  options.target_file_size_multiplier = 1;
  options.max_compaction_bytes = 1500;
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    for (int j = 0; j < kNumFiles; ++j) {
      if (i > 0) {
        // delete [95,105) in two files, [295,305) in next two
        int mid = (j + (1 - j % 2)) * kNumPerFile;
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   Key(mid - 5), Key(mid + 5)));
      }
      std::vector<std::string> values;
      // Write 100KB (100 values, each 1K)
      for (int k = 0; k < kNumPerFile; k++) {
        values.push_back(rnd.RandomString(990));
        ASSERT_OK(Put(Key(j * kNumPerFile + k), values[k]));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put("", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      if (j < kNumFiles - 1) {
        // background compaction may happen early for kNumFiles'th file
        ASSERT_EQ(NumTableFilesAtLevel(0), j + 1);
      }
      if (j == options.level0_file_num_compaction_trigger - 1) {
        // When i == 1, compaction will output some files to L1, at which point
        // L1 is not bottommost so range deletions cannot be compacted away. The
        // new L1 files must be generated with non-overlapping key ranges even
        // though multiple subcompactions see the same ranges deleted, else an
        // assertion will fail.
        //
        // Only enable auto-compactions when we're ready; otherwise, the
        // oversized L0 (relative to base_level) causes the compaction to run
        // earlier.
        ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
        ASSERT_OK(dbfull()->TEST_WaitForCompact());
        ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                  {{"disable_auto_compactions", "true"}}));
        ASSERT_EQ(NumTableFilesAtLevel(0), 0);
        ASSERT_GT(NumTableFilesAtLevel(1), 0);
        ASSERT_GT(NumTableFilesAtLevel(2), 0);
      }
    }
  }
}

TEST_F(DBRangeDelTest, ValidUniversalSubcompactionBoundaries) {
  const int kNumPerFile = 100;
  const int kFilesPerLevel = 4;
  const int kNumLevels = 4;
  Options options = CurrentOptions();
  options.compaction_options_universal.min_merge_width = kFilesPerLevel;
  options.compaction_options_universal.max_merge_width = kFilesPerLevel;
  options.compaction_options_universal.size_ratio = 10;
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = kFilesPerLevel;
  options.max_subcompactions = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  options.num_levels = kNumLevels;
  options.target_file_size_base = kNumPerFile << 10;
  options.target_file_size_multiplier = 1;
  Reopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumLevels - 1; ++i) {
    for (int j = 0; j < kFilesPerLevel; ++j) {
      if (i == kNumLevels - 2) {
        // insert range deletions [95,105) in two files, [295,305) in next two
        // to prepare L1 for later manual compaction.
        int mid = (j + (1 - j % 2)) * kNumPerFile;
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   Key(mid - 5), Key(mid + 5)));
      }
      std::vector<std::string> values;
      // Write 100KB (100 values, each 1K)
      for (int k = 0; k < kNumPerFile; k++) {
        // For the highest level, use smaller value size such that it does not
        // prematurely cause auto compaction due to range tombstone adding
        // additional compensated file size
        values.push_back(rnd.RandomString((i == kNumLevels - 2) ? 600 : 990));
        ASSERT_OK(Put(Key(j * kNumPerFile + k), values[k]));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put("", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      if (j < kFilesPerLevel - 1) {
        // background compaction may happen early for kFilesPerLevel'th file
        ASSERT_EQ(NumTableFilesAtLevel(0), j + 1);
      }
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    if (i == kNumLevels - 2) {
      // For the highest level, value size is smaller (see Put() above),
      // so output file number is smaller.
      ASSERT_GT(NumTableFilesAtLevel(kNumLevels - 1 - i), kFilesPerLevel - 2);
    } else {
      ASSERT_GT(NumTableFilesAtLevel(kNumLevels - 1 - i), kFilesPerLevel - 1);
    }
  }
  // Now L1-L3 are full, when we compact L1->L2 we should see (1) subcompactions
  // happen since input level > 0; (2) range deletions are not dropped since
  // output level is not bottommost. If no file boundary assertion fails, that
  // probably means universal compaction + subcompaction + range deletion are
  // compatible.
  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      1 /* input_level */, 2 /* output_level */, CompactRangeOptions(),
      nullptr /* begin */, nullptr /* end */, true /* exclusive */,
      true /* disallow_trivial_move */,
      std::numeric_limits<uint64_t>::max() /* max_file_num_to_ignore */,
      "" /*trim_ts*/));
}

TEST_F(DBRangeDelTest, CompactionRemovesCoveredMergeOperands) {
  const int kNumPerFile = 3;
  const int kNumFiles = 3;
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(2 * kNumPerFile));
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  opts.num_levels = 2;
  Reopen(opts);

  // Iterates kNumFiles * kNumPerFile + 1 times since flushing the last file
  // requires an extra entry.
  for (int i = 0; i <= kNumFiles * kNumPerFile; ++i) {
    if (i % kNumPerFile == 0 && i / kNumPerFile == kNumFiles - 1) {
      // Delete merge operands from all but the last file
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "key", "key_"));
    }
    std::string val;
    PutFixed64(&val, i);
    ASSERT_OK(db_->Merge(WriteOptions(), "key", val));
    // we need to prevent trivial move using Puts so compaction will actually
    // process the merge operands.
    ASSERT_OK(db_->Put(WriteOptions(), "prevent_trivial_move", ""));
    if (i > 0 && i % kNumPerFile == 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
  }

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  std::string expected;
  std::string actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 45);  // 1+2+...+9
  ASSERT_EQ(expected, actual);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  expected.clear();
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  uint64_t tmp;
  Slice tmp2(actual);
  GetFixed64(&tmp2, &tmp);
  PutFixed64(&expected, 30);  // 6+7+8+9 (earlier operands covered by tombstone)
  ASSERT_EQ(expected, actual);
}

TEST_F(DBRangeDelTest, PutDeleteRangeMergeFlush) {
  // Test the sequence of operations: (1) Put, (2) DeleteRange, (3) Merge, (4)
  // Flush. The `CompactionIterator` previously had a bug where we forgot to
  // check for covering range tombstones when processing the (1) Put, causing
  // it to reappear after the flush.
  Options opts = CurrentOptions();
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Reopen(opts);

  std::string val;
  PutFixed64(&val, 1);
  ASSERT_OK(db_->Put(WriteOptions(), "key", val));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "key",
                             "key_"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key", val));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions read_opts;
  std::string expected;
  std::string actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 1);
  ASSERT_EQ(expected, actual);
}

TEST_F(DBRangeDelTest, ObsoleteTombstoneCleanup) {
  // During compaction to bottommost level, verify range tombstones older than
  // the oldest snapshot are removed, while others are preserved.
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.num_levels = 2;
  opts.statistics = CreateDBStatistics();
  Reopen(opts);

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr1",
                             "dr10"));  // obsolete after compaction
  ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "dr2",
                             "dr20"));  // protected by snapshot
  ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, TestGetTickerCount(opts, COMPACTION_RANGE_DEL_DROP_OBSOLETE));

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, TableEvictedDuringScan) {
  // The RangeDelAggregator holds pointers into range deletion blocks created by
  // table readers. This test ensures the aggregator can still access those
  // blocks even if it outlives the table readers that created them.
  //
  // DBIter always keeps readers open for L0 files. So, in order to test
  // aggregator outliving reader, we need to have deletions in L1 files, which
  // are opened/closed on-demand during the scan. This is accomplished by
  // setting kNumRanges > level0_stop_writes_trigger, which prevents deletions
  // from all lingering in L0 (there is at most one range deletion per L0 file).
  //
  // The first L1 file will contain a range deletion since its begin key is 0.
  // SeekToFirst() references that table's reader and adds its range tombstone
  // to the aggregator. Upon advancing beyond that table's key-range via Next(),
  // the table reader will be unreferenced by the iterator. Since we manually
  // call Evict() on all readers before the full scan, this unreference causes
  // the reader's refcount to drop to zero and thus be destroyed.
  //
  // When it is destroyed, we do not remove its range deletions from the
  // aggregator. So, subsequent calls to Next() must be able to use these
  // deletions to decide whether a key is covered. This will work as long as
  // the aggregator properly references the range deletion block.
  const int kNum = 25;
  const int kRangeBegin = 0;
  const int kRangeEnd = 7;
  const int kNumRanges = 5;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.level0_file_num_compaction_trigger = 4;
  opts.level0_stop_writes_trigger = 4;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  opts.num_levels = 2;
  BlockBasedTableOptions bbto;
  bbto.cache_index_and_filter_blocks = true;
  bbto.block_cache = NewLRUCache(8 << 20);
  opts.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(opts);

  // Hold a snapshot so range deletions can't become obsolete during compaction
  // to bottommost level (i.e., L1).
  const Snapshot* snapshot = db_->GetSnapshot();
  for (int i = 0; i < kNum; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
    if (i > 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    if (i >= kNum / 2 && i < kNum / 2 + kNumRanges) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
  }
  // Must be > 1 so the first L1 file can be closed before scan finishes
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(NumTableFilesAtLevel(1), 1);
  std::vector<uint64_t> file_numbers = ListTableFiles(env_, dbname_);

  ReadOptions read_opts;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());
  int expected = kRangeEnd;
  iter->SeekToFirst();
  for (auto file_number : file_numbers) {
    // This puts table caches in the state of being externally referenced only
    // so they are destroyed immediately upon iterator unreferencing.
    TableCache::Evict(dbfull()->TEST_table_cache(), file_number);
  }
  for (; iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    ++expected;
    // Keep clearing block cache's LRU so range deletion block can be freed as
    // soon as its refcount drops to zero.
    bbto.block_cache->EraseUnRefEntries();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(kNum, expected);
  delete iter;
  db_->ReleaseSnapshot(snapshot);

  // Also test proper cache handling in GetRangeTombstoneIterator,
  // via TablesRangeTombstoneSummary. (This once triggered memory leak
  // report with ASAN.)
  opts.max_open_files = 1;
  Reopen(opts);

  std::string str;
  ASSERT_OK(dbfull()->TablesRangeTombstoneSummary(db_->DefaultColumnFamily(),
                                                  100, &str));
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromMutableMemtable) {
  do {
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

    ReadOptions read_opts;
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromImmutableMemtable) {
  do {
    Options opts = CurrentOptions();
    opts.max_write_buffer_number = 3;
    opts.min_write_buffer_number_to_merge = 2;
    // SpecialSkipListFactory lets us specify maximum number of elements the
    // memtable can hold. It switches the active memtable to immutable (flush is
    // prevented by the above options) upon inserting an element that would
    // overflow the memtable.
    opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
    DestroyAndReopen(opts);

    ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
    ASSERT_OK(db_->Put(WriteOptions(), "blah", "val"));

    ReadOptions read_opts;
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, GetCoveredKeyFromSst) {
  do {
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
    // snapshot prevents key from being deleted during flush
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
    ASSERT_OK(db_->Flush(FlushOptions()));

    ReadOptions read_opts;
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, "key", &value).IsNotFound());
    db_->ReleaseSnapshot(snapshot);
  } while (ChangeOptions(kRangeDelSkipConfigs));
}

TEST_F(DBRangeDelTest, GetCoveredMergeOperandFromMemtable) {
  const int kNumMergeOps = 10;
  Options opts = CurrentOptions();
  opts.merge_operator = MergeOperators::CreateUInt64AddOperator();
  Reopen(opts);

  for (int i = 0; i < kNumMergeOps; ++i) {
    std::string val;
    PutFixed64(&val, i);
    ASSERT_OK(db_->Merge(WriteOptions(), "key", val));
    if (i == kNumMergeOps / 2) {
      // deletes [0, 5]
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "key", "key_"));
    }
  }

  ReadOptions read_opts;
  std::string expected;
  std::string actual;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 30);  // 6+7+8+9
  ASSERT_EQ(expected, actual);

  expected.clear();
  read_opts.ignore_range_deletions = true;
  ASSERT_OK(db_->Get(read_opts, "key", &actual));
  PutFixed64(&expected, 45);  // 0+1+2+...+9
  ASSERT_EQ(expected, actual);
}

TEST_F(DBRangeDelTest, GetIgnoresRangeDeletions) {
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 4;
  opts.min_write_buffer_number_to_merge = 3;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  Reopen(opts);

  ASSERT_OK(db_->Put(WriteOptions(), "sst_key", "val"));
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "imm_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Put(WriteOptions(), "mem_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  for (std::string key : {"sst_key", "imm_key", "mem_key"}) {
    std::string value;
    ASSERT_OK(db_->Get(read_opts, key, &value));
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, IteratorRemovesCoveredKeys) {
  const int kNum = 200;
  const int kRangeBegin = 50;
  const int kRangeEnd = 150;
  const int kNumPerFile = 25;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  DestroyAndReopen(opts);

  // Write half of the keys before the tombstone and half after the tombstone.
  // Only covered keys (i.e., within the range and older than the tombstone)
  // should be deleted.
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 2) {
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
  }
  ReadOptions read_opts;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    if (expected == kRangeBegin - 1) {
      expected = kNum / 2;
    } else {
      ++expected;
    }
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(kNum, expected);
  delete iter;
}

TEST_F(DBRangeDelTest, IteratorOverUserSnapshot) {
  const int kNum = 200;
  const int kRangeBegin = 50;
  const int kRangeEnd = 150;
  const int kNumPerFile = 25;
  Options opts = CurrentOptions();
  opts.comparator = test::Uint64Comparator();
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(kNumPerFile));
  DestroyAndReopen(opts);

  const Snapshot* snapshot = nullptr;
  // Put a snapshot before the range tombstone, verify an iterator using that
  // snapshot sees all inserted keys.
  for (int i = 0; i < kNum; ++i) {
    if (i == kNum / 2) {
      snapshot = db_->GetSnapshot();
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 GetNumericStr(kRangeBegin),
                                 GetNumericStr(kRangeEnd)));
    }
    ASSERT_OK(db_->Put(WriteOptions(), GetNumericStr(i), "val"));
  }
  ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_EQ(GetNumericStr(expected), iter->key());
    ++expected;
  }
  ASSERT_EQ(kNum / 2, expected);
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, IteratorIgnoresRangeDeletions) {
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 4;
  opts.min_write_buffer_number_to_merge = 3;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  Reopen(opts);

  ASSERT_OK(db_->Put(WriteOptions(), "sst_key", "val"));
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Put(WriteOptions(), "imm_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->Put(WriteOptions(), "mem_key", "val"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  ReadOptions read_opts;
  read_opts.ignore_range_deletions = true;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());
  int i = 0;
  std::string expected[] = {"imm_key", "mem_key", "sst_key"};
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++i) {
    std::string key;
    ASSERT_EQ(expected[i], iter->key());
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(3, i);
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

#ifndef ROCKSDB_UBSAN_RUN
TEST_F(DBRangeDelTest, TailingIteratorRangeTombstoneUnsupported) {
  ASSERT_OK(db_->Put(WriteOptions(), "key", "val"));
  // snapshot prevents key from being deleted during flush
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));

  // iterations check unsupported in memtable, l0, and then l1
  for (int i = 0; i < 3; ++i) {
    ReadOptions read_opts;
    read_opts.tailing = true;
    auto* iter = db_->NewIterator(read_opts);
    if (i == 2) {
      // For L1+, iterators over files are created on-demand, so need seek
      iter->SeekToFirst();
    }
    ASSERT_TRUE(iter->status().IsNotSupported());

    delete iter;
    if (i == 0) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    } else if (i == 1) {
      MoveFilesToLevel(1);
    }
  }
  db_->ReleaseSnapshot(snapshot);
}
#endif  // !ROCKSDB_UBSAN_RUN

TEST_F(DBRangeDelTest, SubcompactionHasEmptyDedicatedRangeDelFile) {
  const int kNumFiles = 2;
  const int kNumKeysPerFile = 4;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumFiles;
  options.max_subcompactions = 2;
  options.num_levels = 2;
  options.target_file_size_base = 4096;
  Reopen(options);

  // need a L1 file for subcompaction to be triggered
  ASSERT_OK(
      db_->Put(WriteOptions(), db_->DefaultColumnFamily(), Key(0), "val"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);

  // put enough keys to fill up the first subcompaction, and later range-delete
  // them so that the first subcompaction outputs no key-values. In that case
  // it'll consider making an SST file dedicated to range deletions.
  for (int i = 0; i < kNumKeysPerFile; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), Key(i),
                       std::string(1024, 'a')));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(kNumKeysPerFile)));

  // the above range tombstone can be dropped, so that one alone won't cause a
  // dedicated file to be opened. We can make one protected by snapshot that
  // must be considered. Make its range outside the first subcompaction's range
  // to exercise the tricky part of the code.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(kNumKeysPerFile + 1),
                             Key(kNumKeysPerFile + 2)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, MemtableBloomFilter) {
  // regression test for #2743. the range delete tombstones in memtable should
  // be added even when Get() skips searching due to its prefix bloom filter
  const int kMemtableSize = 1 << 20;              // 1MB
  const int kMemtablePrefixFilterSize = 1 << 13;  // 8KB
  const int kNumKeys = 1000;
  const int kPrefixLen = 8;
  Options options = CurrentOptions();
  options.memtable_prefix_bloom_size_ratio =
      static_cast<double>(kMemtablePrefixFilterSize) / kMemtableSize;
  options.prefix_extractor.reset(
      ROCKSDB_NAMESPACE::NewFixedPrefixTransform(kPrefixLen));
  options.write_buffer_size = kMemtableSize;
  Reopen(options);

  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), "val"));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(kNumKeys)));
  for (int i = 0; i < kNumKeys; ++i) {
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
  }
}

TEST_F(DBRangeDelTest, CompactionTreatsSplitInputLevelDeletionAtomically) {
  // This test originally verified that compaction treated files containing a
  // split range deletion in the input level as an atomic unit. I.e.,
  // compacting any input-level file(s) containing a portion of the range
  // deletion causes all other input-level files containing portions of that
  // same range deletion to be included in the compaction. Range deletion
  // tombstones are now truncated to sstable boundaries which removed the need
  // for that behavior (which could lead to excessively large
  // compactions).
  const int kNumFilesPerLevel = 4;
  const int kValueBytes = 4 << 10;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = kNumFilesPerLevel;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(2 /* num_entries_flush */));
  // max file size could be 2x of target file size, so set it to half of that
  options.target_file_size_base = kValueBytes / 2;
  options.max_compaction_bytes = 1500;
  // i == 0: CompactFiles
  // i == 1: CompactRange
  // i == 2: automatic compaction
  for (int i = 0; i < 3; ++i) {
    DestroyAndReopen(options);

    ASSERT_OK(Put(Key(0), ""));
    ASSERT_OK(db_->Flush(FlushOptions()));
    MoveFilesToLevel(2);
    ASSERT_EQ(1, NumTableFilesAtLevel(2));

    // snapshot protects range tombstone from dropping due to becoming obsolete.
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(0), Key(2 * kNumFilesPerLevel)));

    Random rnd(301);
    std::string value = rnd.RandomString(kValueBytes);
    for (int j = 0; j < kNumFilesPerLevel; ++j) {
      // give files overlapping key-ranges to prevent trivial move
      ASSERT_OK(Put(Key(j), value));
      ASSERT_OK(Put(Key(2 * kNumFilesPerLevel - 1 - j), value));
      if (j > 0) {
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
        ASSERT_EQ(j, NumTableFilesAtLevel(0));
      }
    }
    // put extra key to trigger final flush
    ASSERT_OK(Put("", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(kNumFilesPerLevel, NumTableFilesAtLevel(1));

    ColumnFamilyMetaData meta;
    db_->GetColumnFamilyMetaData(&meta);
    if (i == 0) {
      ASSERT_OK(db_->CompactFiles(
          CompactionOptions(), {meta.levels[1].files[0].name}, 2 /* level */));
      ASSERT_EQ(0, NumTableFilesAtLevel(1));
    } else if (i == 1) {
      auto begin_str = Key(0);
      auto end_str = Key(1);
      Slice begin = begin_str;
      Slice end = end_str;
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &begin, &end));
      ASSERT_EQ(3, NumTableFilesAtLevel(1));
    } else if (i == 2) {
      ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                {{"max_bytes_for_level_base", "10000"}}));
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ(1, NumTableFilesAtLevel(1));
    }
    ASSERT_GT(NumTableFilesAtLevel(2), 0);

    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DBRangeDelTest, RangeTombstoneEndKeyAsSstableUpperBound) {
  // Test the handling of the range-tombstone end-key as the
  // upper-bound for an sstable.

  const int kNumFilesPerLevel = 2;
  const int kValueBytes = 4 << 10;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = kNumFilesPerLevel;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(2 /* num_entries_flush */));
  // Compaction can generate files of size at most 2 * target_file_size_base.
  options.target_file_size_base = kValueBytes / 2;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  // Create an initial sstable at L2:
  //   [key000000#1,1, key000000#1,1]
  ASSERT_OK(Put(Key(0), ""));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // A snapshot protects the range tombstone from dropping due to
  // becoming obsolete.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(2 * kNumFilesPerLevel)));

  // Create 2 additional sstables in L0. Note that the first sstable
  // contains the range tombstone.
  //   [key000000#3,1, key000004#72057594037927935,15]
  //   [key000001#5,1, key000002#6,1]
  Random rnd(301);
  std::string value = rnd.RandomString(kValueBytes);
  for (int j = 0; j < kNumFilesPerLevel; ++j) {
    // Give files overlapping key-ranges to prevent a trivial move when we
    // compact from L0 to L1.
    ASSERT_OK(Put(Key(j), value));
    ASSERT_OK(Put(Key(2 * kNumFilesPerLevel - 1 - j), value));
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_EQ(j + 1, NumTableFilesAtLevel(0));
  }
  // Compact the 2 L0 sstables to L1, resulting in the following LSM. There
  // are 2 sstables generated in L1 due to the target_file_size_base setting.
  //   L1:
  //     [key000000#3,1, key000002#72057594037927935,15]
  //     [key000002#6,1, key000004#72057594037927935,15]
  //   L2:
  //     [key000000#1,1, key000000#1,1]
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
  ASSERT_EQ(
      files[1][0].largest.Encode(),
      InternalKey(Key(2), kMaxSequenceNumber, kTypeRangeDeletion).Encode());
  ASSERT_EQ(files[1][1].smallest.Encode(),
            InternalKey(Key(2), 6, kTypeValue).Encode());

  {
    // Compact the second sstable in L1:
    //   L1:
    //     [key000000#3,1, key000002#72057594037927935,15]
    //   L2:
    //     [key000000#1,1, key000000#1,1]
    //     [key000002#6,1, key000004#72057594037927935,15]
    //
    // At the same time, verify the compaction does not cause the key at the
    // endpoint (key000002#6,1) to disappear.
    ASSERT_EQ(value, Get(Key(2)));
    auto begin_str = Key(3);
    const ROCKSDB_NAMESPACE::Slice begin = begin_str;
    ASSERT_OK(dbfull()->TEST_CompactRange(1, &begin, nullptr));
    ASSERT_EQ(1, NumTableFilesAtLevel(1));
    ASSERT_EQ(2, NumTableFilesAtLevel(2));
    ASSERT_EQ(value, Get(Key(2)));
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
    ASSERT_EQ(files[2][1].smallest.Encode(),
              InternalKey(Key(2), 6, kTypeValue).Encode());
    ASSERT_EQ(
        files[2][1].largest.Encode(),
        InternalKey(Key(4), kMaxSequenceNumber, kTypeRangeDeletion).Encode());
  }

  {
    // Compact the first sstable in L1. This should be copacetic, but
    // was previously resulting in overlapping sstables in L2 due to
    // mishandling of the range tombstone end-key when used as the
    // largest key for an sstable. The resulting LSM structure should
    // be:
    //
    //   L2:
    //     [key000000#1,1, key000001#72057594037927935,15]
    //     [key000001#5,1, key000002#72057594037927935,15]
    //     [key000002#6,1, key000004#72057594037927935,15]
    auto begin_str = Key(0);
    const ROCKSDB_NAMESPACE::Slice begin = begin_str;
    ASSERT_OK(dbfull()->TEST_CompactRange(1, &begin, &begin));
    ASSERT_EQ(0, NumTableFilesAtLevel(1));
    ASSERT_EQ(3, NumTableFilesAtLevel(2));
    dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
    ASSERT_EQ(
        files[2][0].largest.Encode(),
        InternalKey(Key(1), kMaxSequenceNumber, kTypeRangeDeletion).Encode());
    ASSERT_EQ(files[2][1].smallest.Encode(),
              InternalKey(Key(1), 5, kTypeValue).Encode());
    ASSERT_EQ(
        files[2][1].largest.Encode(),
        InternalKey(Key(2), kMaxSequenceNumber, kTypeRangeDeletion).Encode());
    ASSERT_EQ(files[2][2].smallest.Encode(),
              InternalKey(Key(2), 6, kTypeValue).Encode());
    ASSERT_EQ(
        files[2][2].largest.Encode(),
        InternalKey(Key(4), kMaxSequenceNumber, kTypeRangeDeletion).Encode());
  }

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, UnorderedTombstones) {
  // Regression test for #2752. Range delete tombstones between
  // different snapshot stripes are not stored in order, so the first
  // tombstone of each snapshot stripe should be checked as a smallest
  // candidate.
  Options options = CurrentOptions();
  DestroyAndReopen(options);

  auto cf = db_->DefaultColumnFamily();

  ASSERT_OK(db_->Put(WriteOptions(), cf, "a", "a"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  ASSERT_OK(db_->DeleteRange(WriteOptions(), cf, "b", "c"));
  // Hold a snapshot to separate these two delete ranges.
  auto snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), cf, "a", "b"));
  ASSERT_OK(db_->Flush(FlushOptions(), cf));
  db_->ReleaseSnapshot(snapshot);

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(cf, &files);
  ASSERT_EQ(1, files[0].size());
  ASSERT_EQ("a", files[0][0].smallest.user_key());
  ASSERT_EQ("c", files[0][0].largest.user_key());

  std::string v;
  auto s = db_->Get(ReadOptions(), "a", &v);
  ASSERT_TRUE(s.IsNotFound());
}

class MockMergeOperator : public MergeOperator {
  // Mock non-associative operator. Non-associativity is expressed by lack of
  // implementation for any `PartialMerge*` functions.
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    assert(merge_out != nullptr);
    merge_out->new_value = merge_in.operand_list.back().ToString();
    return true;
  }

  const char* Name() const override { return "MockMergeOperator"; }
};

TEST_F(DBRangeDelTest, KeyAtOverlappingEndpointReappears) {
  // This test uses a non-associative merge operator since that is a convenient
  // way to get compaction to write out files with overlapping user-keys at the
  // endpoints. Note, however, overlapping endpoints can also occur with other
  // value types (Put, etc.), assuming the right snapshots are present.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 1 << 10;
  const int kNumFiles = 4;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.merge_operator = std::make_shared<MockMergeOperator>();
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  // Push dummy data to L3 so that our actual test files on L0-L2
  // will not be considered "bottommost" level, otherwise compaction
  // may prevent us from creating overlapping user keys
  // as on the bottommost layer MergeHelper
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "dummy"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(3);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = rnd.RandomString(kValueBytes);
      ASSERT_OK(db_->Merge(WriteOptions(), "key", value));
    }
    if (i == kNumFiles - 1) {
      // Take snapshot to prevent covered merge operands from being dropped by
      // compaction.
      snapshot = db_->GetSnapshot();
      // The DeleteRange is the last write so all merge operands are covered.
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "key", "key_"));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());

  ASSERT_OK(dbfull()->TEST_CompactRange(
      0 /* level */, nullptr /* begin */, nullptr /* end */,
      nullptr /* column_family */, true /* disallow_trivial_move */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  // Now we have multiple files at L1 all containing a single user key, thus
  // guaranteeing overlap in the file endpoints.
  ASSERT_GT(NumTableFilesAtLevel(1), 1);

  // Verify no merge operands reappeared after the compaction.
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());

  // Compact and verify again. It's worthwhile because now the files have
  // tighter endpoints, so we can verify that doesn't mess anything up.
  ASSERT_OK(dbfull()->TEST_CompactRange(
      1 /* level */, nullptr /* begin */, nullptr /* end */,
      nullptr /* column_family */, true /* disallow_trivial_move */));
  ASSERT_GT(NumTableFilesAtLevel(2), 1);
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, UntruncatedTombstoneDoesNotDeleteNewerKey) {
  // Verify a key newer than a range tombstone cannot be deleted by being
  // compacted to the bottom level (and thus having its seqnum zeroed) before
  // the range tombstone. This used to happen when range tombstones were
  // untruncated on reads such that they extended past their file boundaries.
  //
  // Test summary:
  //
  // - L1 is bottommost.
  // - A couple snapshots are strategically taken to prevent seqnums from being
  //   zeroed, range tombstone from being dropped, merge operands from being
  //   dropped, and merge operands from being combined.
  // - Left half of files in L1 all have same user key, ensuring their file
  //   boundaries overlap. In the past this would cause range tombstones to be
  //   untruncated.
  // - Right half of L1 files all have different keys, ensuring no overlap.
  // - A range tombstone spans all L1 keys, so it is stored in every L1 file.
  // - Keys in the right side of the key-range are overwritten. These are
  //   compacted down to L1 after releasing snapshots such that their seqnums
  //   will be zeroed.
  // - A full range scan is performed. If the tombstone in the left L1 files
  //   were untruncated, it would now cover keys newer than it (but with zeroed
  //   seqnums) in the right L1 files.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 1 << 10;
  const int kNumFiles = 4;
  const int kMaxKey = kNumFiles * kFileBytes / kValueBytes;
  const int kKeysOverwritten = 10;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.merge_operator = std::make_shared<MockMergeOperator>();
  options.num_levels = 2;
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  Random rnd(301);
  // - snapshots[0] prevents merge operands from being combined during
  //   compaction.
  // - snapshots[1] prevents merge operands from being dropped due to the
  //   covering range tombstone.
  const Snapshot* snapshots[] = {nullptr, nullptr};
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = rnd.RandomString(kValueBytes);
      std::string key;
      if (i < kNumFiles / 2) {
        key = Key(0);
      } else {
        key = Key(1 + i * kFileBytes / kValueBytes + j);
      }
      ASSERT_OK(db_->Merge(WriteOptions(), key, value));
    }
    if (i == 0) {
      snapshots[0] = db_->GetSnapshot();
    }
    if (i == kNumFiles - 1) {
      snapshots[1] = db_->GetSnapshot();
      // The DeleteRange is the last write so all merge operands are covered.
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 Key(0), Key(kMaxKey + 1)));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));

  auto get_key_count = [this]() -> int {
    auto* iter = db_->NewIterator(ReadOptions());
    assert(iter->status().ok());
    iter->SeekToFirst();
    int keys_found = 0;
    for (; iter->Valid(); iter->Next()) {
      ++keys_found;
    }
    EXPECT_OK(iter->status());
    delete iter;
    return keys_found;
  };

  // All keys should be covered
  ASSERT_EQ(0, get_key_count());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr /* begin_key */,
                              nullptr /* end_key */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  // Roughly the left half of L1 files should have overlapping boundary keys,
  // while the right half should not.
  ASSERT_GE(NumTableFilesAtLevel(1), kNumFiles);

  // Now overwrite a few keys that are in L1 files that definitely don't have
  // overlapping boundary keys.
  for (int i = kMaxKey; i > kMaxKey - kKeysOverwritten; --i) {
    auto value = rnd.RandomString(kValueBytes);
    ASSERT_OK(db_->Merge(WriteOptions(), Key(i), value));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));

  // The overwritten keys are in L0 now, so clearly aren't covered by the range
  // tombstone in L1.
  ASSERT_EQ(kKeysOverwritten, get_key_count());

  // Release snapshots so seqnums can be zeroed when L0->L1 happens.
  db_->ReleaseSnapshot(snapshots[0]);
  db_->ReleaseSnapshot(snapshots[1]);

  auto begin_key_storage = Key(kMaxKey - kKeysOverwritten + 1);
  auto end_key_storage = Key(kMaxKey);
  Slice begin_key(begin_key_storage);
  Slice end_key(end_key_storage);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &begin_key, &end_key));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GE(NumTableFilesAtLevel(1), kNumFiles);

  ASSERT_EQ(kKeysOverwritten, get_key_count());
}

TEST_F(DBRangeDelTest, DeletedMergeOperandReappearsIterPrev) {
  // Exposes a bug where we were using
  // `RangeDelPositioningMode::kBackwardTraversal` while scanning merge operands
  // in the forward direction. Confusingly, this case happened during
  // `DBIter::Prev`. It could cause assertion failure, or reappearing keys.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 1 << 10;
  // Need multiple keys so we can get results when calling `Prev()` after
  // `SeekToLast()`.
  const int kNumKeys = 3;
  const int kNumFiles = 4;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.merge_operator = std::make_shared<MockMergeOperator>();
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kFileBytes / kValueBytes; ++j) {
      auto value = rnd.RandomString(kValueBytes);
      ASSERT_OK(db_->Merge(WriteOptions(), Key(j % kNumKeys), value));
      if (i == 0 && j == kNumKeys) {
        // Take snapshot to prevent covered merge operands from being dropped or
        // merged by compaction.
        snapshot = db_->GetSnapshot();
        // Do a DeleteRange near the beginning so only the oldest merge operand
        // for each key is covered. This ensures the sequence of events:
        //
        // - `DBIter::Prev()` is called
        // - After several same versions of the same user key are encountered,
        //   it decides to seek using `DBIter::FindValueForCurrentKeyUsingSeek`.
        // - Binary searches to the newest version of the key, which is in the
        //   leftmost file containing the user key.
        // - Scans forwards to collect all merge operands. Eventually reaches
        //   the rightmost file containing the oldest merge operand, which
        //   should be covered by the `DeleteRange`. If `RangeDelAggregator`
        //   were not properly using `kForwardTraversal` here, that operand
        //   would reappear.
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   Key(0), Key(kNumKeys + 1)));
      }
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_EQ(kNumFiles, NumTableFilesAtLevel(0));

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr /* begin_key */,
                              nullptr /* end_key */));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_GT(NumTableFilesAtLevel(1), 1);

  auto* iter = db_->NewIterator(ReadOptions());
  ASSERT_OK(iter->status());
  iter->SeekToLast();
  int keys_found = 0;
  for (; iter->Valid(); iter->Prev()) {
    ++keys_found;
  }
  ASSERT_OK(iter->status());
  delete iter;
  ASSERT_EQ(kNumKeys, keys_found);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, SnapshotPreventsDroppedKeys) {
  const int kFileBytes = 1 << 20;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(10)));

  ASSERT_OK(db_->Flush(FlushOptions()));

  ReadOptions read_opts;
  read_opts.snapshot = snapshot;
  auto* iter = db_->NewIterator(read_opts);
  ASSERT_OK(iter->status());

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(Key(0), iter->key());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, SnapshotPreventsDroppedKeysInImmMemTables) {
  const int kFileBytes = 1 << 20;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = kFileBytes;
  Reopen(options);

  // block flush thread -> pin immtables in memory
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({
      {"SnapshotPreventsDroppedKeysInImmMemTables:AfterNewIterator",
       "DBImpl::BGWorkFlush"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(0), "a"));
  std::unique_ptr<const Snapshot, std::function<void(const Snapshot*)>>
      snapshot(db_->GetSnapshot(),
               [this](const Snapshot* s) { db_->ReleaseSnapshot(s); });

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(10)));

  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  ReadOptions read_opts;
  read_opts.snapshot = snapshot.get();
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
  ASSERT_OK(iter->status());

  TEST_SYNC_POINT("SnapshotPreventsDroppedKeysInImmMemTables:AfterNewIterator");

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(Key(0), iter->key());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBRangeDelTest, RangeTombstoneWrittenToMinimalSsts) {
  // Adapted from
  // https://github.com/cockroachdb/cockroach/blob/de8b3ea603dd1592d9dc26443c2cc92c356fbc2f/pkg/storage/engine/rocksdb_test.go#L1267-L1398.
  // Regression test for issue where range tombstone was written to more files
  // than necessary when it began exactly at the begin key in the next
  // compaction output file.
  const int kFileBytes = 1 << 20;
  const int kValueBytes = 4 << 10;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  // Have a bit of slack in the size limits but we enforce them more strictly
  // when manually flushing/compacting.
  options.max_compaction_bytes = 2 * kFileBytes;
  options.target_file_size_base = 2 * kFileBytes;
  options.write_buffer_size = 2 * kFileBytes;
  Reopen(options);

  Random rnd(301);
  for (char first_char : {'a', 'b', 'c'}) {
    for (int i = 0; i < kFileBytes / kValueBytes; ++i) {
      std::string key(1, first_char);
      key.append(Key(i));
      std::string value = rnd.RandomString(kValueBytes);
      ASSERT_OK(Put(key, value));
    }
    ASSERT_OK(db_->Flush(FlushOptions()));
    MoveFilesToLevel(2);
  }
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(3, NumTableFilesAtLevel(2));

  // Populate the memtable lightly while spanning the whole key-space. The
  // setting of `max_compaction_bytes` will cause the L0->L1 to output multiple
  // files to prevent a large L1->L2 compaction later.
  ASSERT_OK(Put("a", "val"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             "c" + Key(1), "d"));
  // Our compaction output file cutting logic currently only considers point
  // keys. So, in order for the range tombstone to have a chance at landing at
  // the start of a new file, we need a point key at the range tombstone's
  // start.
  // TODO(ajkr): remove this `Put` after file cutting accounts for range
  // tombstones (#3977).
  ASSERT_OK(Put("c" + Key(1), "value"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Ensure manual L0->L1 compaction cuts the outputs before the range tombstone
  // and the range tombstone is only placed in the second SST.
  std::string begin_key_storage("c" + Key(1));
  Slice begin_key(begin_key_storage);
  std::string end_key_storage("d");
  Slice end_key(end_key_storage);
  ASSERT_OK(dbfull()->TEST_CompactRange(
      0 /* level */, &begin_key /* begin */, &end_key /* end */,
      nullptr /* column_family */, true /* disallow_trivial_move */));
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  std::vector<LiveFileMetaData> all_metadata;
  std::vector<LiveFileMetaData> l1_metadata;
  db_->GetLiveFilesMetaData(&all_metadata);
  for (const auto& metadata : all_metadata) {
    if (metadata.level == 1) {
      l1_metadata.push_back(metadata);
    }
  }
  std::sort(l1_metadata.begin(), l1_metadata.end(),
            [&](const LiveFileMetaData& a, const LiveFileMetaData& b) {
              return options.comparator->Compare(a.smallestkey, b.smallestkey) <
                     0;
            });
  ASSERT_EQ("a", l1_metadata[0].smallestkey);
  ASSERT_EQ("a", l1_metadata[0].largestkey);
  ASSERT_EQ("c" + Key(1), l1_metadata[1].smallestkey);
  ASSERT_EQ("d", l1_metadata[1].largestkey);

  TablePropertiesCollection all_table_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&all_table_props));
  int64_t num_range_deletions = 0;
  for (const auto& name_and_table_props : all_table_props) {
    const auto& name = name_and_table_props.first;
    const auto& table_props = name_and_table_props.second;
    // The range tombstone should only be output to the second L1 SST.
    if (name.size() >= l1_metadata[1].name.size() &&
        name.substr(name.size() - l1_metadata[1].name.size())
                .compare(l1_metadata[1].name) == 0) {
      ASSERT_EQ(1, table_props->num_range_deletions);
      ++num_range_deletions;
    } else {
      ASSERT_EQ(0, table_props->num_range_deletions);
    }
  }
  ASSERT_EQ(1, num_range_deletions);
}

TEST_F(DBRangeDelTest, LevelCompactOutputCutAtRangeTombstoneForTtlFiles) {
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.compaction_pri = kMinOverlappingRatio;
  options.disable_auto_compactions = true;
  options.ttl = 24 * 60 * 60;  // 24 hours
  options.target_file_size_base = 8 << 10;
  env_->SetMockSleep();
  options.env = env_;
  DestroyAndReopen(options);

  Random rnd(301);
  // Fill some data so that future compactions are not bottommost level
  // compaction, and hence they would try cut around files for ttl
  for (int i = 5; i < 10; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_EQ("0,0,0,1", FilesPerLevel());

  for (int i = 5; i < 10; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1 << 10)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_EQ("0,1,0,1", FilesPerLevel());

  env_->MockSleepForSeconds(20 * 60 * 60);
  // Prevent range tombstone from being dropped during compaction.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(11), Key(12)));
  ASSERT_OK(Put(Key(0), rnd.RandomString(1 << 10)));
  ASSERT_OK(Flush());
  ASSERT_EQ("1,1,0,1", FilesPerLevel());
  // L0 file is new, L1 and L3 file are old and qualified for TTL
  env_->MockSleepForSeconds(10 * 60 * 60);
  MoveFilesToLevel(1);
  // L1 output should be cut into 3 files:
  // File 0: Key(0)
  // File 1: (qualified for TTL): Key(5) - Key(10)
  // File 1: DeleteRange [11, 12)
  ASSERT_EQ("0,3,0,1", FilesPerLevel());
  db_->ReleaseSnapshot(snapshot);
}

// Test SST partitioner cut after every single key
class SingleKeySstPartitioner : public SstPartitioner {
 public:
  const char* Name() const override { return "SingleKeySstPartitioner"; }

  PartitionerResult ShouldPartition(
      const PartitionerRequest& /*request*/) override {
    return kRequired;
  }

  bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                        const Slice& /*largest_user_key*/) override {
    return false;
  }
};

class SingleKeySstPartitionerFactory : public SstPartitionerFactory {
 public:
  static const char* kClassName() { return "SingleKeySstPartitionerFactory"; }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override {
    return std::unique_ptr<SstPartitioner>(new SingleKeySstPartitioner());
  }
};

TEST_F(DBRangeDelTest, CompactionEmitRangeTombstoneToSSTPartitioner) {
  Options options = CurrentOptions();
  auto factory = std::make_shared<SingleKeySstPartitionerFactory>();
  options.sst_partitioner_factory = factory;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  Random rnd(301);
  // range deletion keys are not processed when compacting to bottommost level,
  // so creating a file at older level to make the next compaction not
  // bottommost level
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), rnd.RandomString(10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);

  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(10)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(5)));
  ASSERT_OK(Flush());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(1);
  // SSTPartitioner decides to cut when range tombstone start key is passed to
  // it. Note that the range tombstone [2, 5) itself span multiple keys, but we
  // are not able to partition within its range yet.
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
}

namespace {
// Records the compaction output files that were started, and how many of them
// were empty and not kept.
class CompactionOutputListener : public EventListener {
 public:
  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& info) override {
    if (info.reason == TableFileCreationReason::kCompaction) {
      std::lock_guard<std::mutex> lock(mutex_);
      started_paths_.push_back(info.file_path);
    }
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    if (info.reason == TableFileCreationReason::kCompaction &&
        info.status.IsAborted()) {
      std::lock_guard<std::mutex> lock(mutex_);
      ++num_not_kept_;
    }
  }

  std::vector<std::string> TakeStartedPaths() {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::exchange(started_paths_, {});
  }

  int TakeNumNotKept() {
    std::lock_guard<std::mutex> lock(mutex_);
    return std::exchange(num_not_kept_, 0);
  }

 private:
  std::mutex mutex_;
  std::vector<std::string> started_paths_;
  int num_not_kept_ = 0;
};
}  // namespace

TEST_F(DBRangeDelTest, BottommostCompactionEmitRangeTombstoneToSSTPartitioner) {
  // A range tombstone after the last point key must reach the SSTPartitioner
  // at the bottommost level too. Otherwise the tombstone is appended to the
  // file holding the point key and extends that file across the partition
  // boundary, so no file starting at the boundary fits in the level.
  // The fixed-prefix factory exercises the partitioner's prefix fast path,
  // and SingleKeySstPartitionerFactory its per-key ShouldPartition() path.
  // Each Key(i) is 9 bytes, so a 9-byte prefix puts every key in its own
  // partition.
  for (const auto& factory :
       std::vector<std::shared_ptr<SstPartitionerFactory>>{
           NewSstPartitionerFixedPrefixFactory(9),
           std::make_shared<SingleKeySstPartitionerFactory>()}) {
    SCOPED_TRACE(factory->Name());
    Options options = CurrentOptions();
    options.sst_partitioner_factory = factory;
    options.disable_auto_compactions = true;
    options.level_compaction_dynamic_level_bytes = true;
    auto listener = std::make_shared<CompactionOutputListener>();
    options.listeners.push_back(listener);
    DestroyAndReopen(options);
    const int bottommost_level = options.num_levels - 1;

    ASSERT_OK(Put(Key(1), "val"));
    // Keeps the tombstone from being dropped at the bottommost level
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(2), Key(5)));
    ASSERT_OK(Flush());

    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

    ColumnFamilyMetaData cf_meta;
    db_->GetColumnFamilyMetaData(&cf_meta);
    const std::vector<SstFileMetaData>& files =
        cf_meta.levels[bottommost_level].files;
    ASSERT_EQ(2, files.size());
    ASSERT_EQ(Key(1), files[0].largestkey);
    ASSERT_EQ(Key(2), files[1].smallestkey);
    ASSERT_EQ(0, listener->TakeNumNotKept());
    listener->TakeStartedPaths();

    // Once nothing pins it, the tombstone is dropped. The file cut for it is
    // still created, but it is empty, so the compaction deletes it before
    // installing its outputs: it is never part of the LSM.
    db_->ReleaseSnapshot(snapshot);
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    ASSERT_EQ(1, NumTableFilesAtLevel(bottommost_level));
    ASSERT_EQ(1, listener->TakeNumNotKept());
    const std::vector<std::string> started = listener->TakeStartedPaths();
    ASSERT_EQ(2, started.size());
    int num_on_disk = 0;
    for (const auto& path : started) {
      const Status exists = env_->FileExists(path);
      if (exists.ok()) {
        ++num_on_disk;
      } else {
        ASSERT_TRUE(exists.IsNotFound()) << exists.ToString();
      }
    }
    ASSERT_EQ(1, num_on_disk);
  }
}

TEST_F(DBRangeDelTest, BottommostRangeTombstoneAtSameUserKeyAsPointKey) {
  // At the bottommost level a point key's seqno can be zeroed, so it sorts
  // after a range tombstone that starts at the same user key. The partitioner
  // must not cut between them, even one that requires a cut for every key.
  Options options = CurrentOptions();
  options.sst_partitioner_factory =
      std::make_shared<SingleKeySstPartitionerFactory>();
  options.disable_auto_compactions = true;
  options.level_compaction_dynamic_level_bytes = true;
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  // The tombstone is dropped.
  DestroyAndReopen(options);
  ASSERT_OK(Put(Key(1), "val"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(5)));
  ASSERT_OK(Put(Key(2), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("val", Get(Key(2)));

  // A newer tombstone covering the same user key is kept by a snapshot, so it
  // is written to every output file that overlaps it.
  DestroyAndReopen(options);
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(5)));
  ASSERT_OK(Put(Key(2), "val"));
  ASSERT_OK(Flush());
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key(4)));
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("NOT_FOUND", Get(Key(2)));
  ASSERT_EQ("val", Get(Key(2), snapshot));
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, OversizeCompactionGapBetweenPointKeyAndTombstone) {
  // L2 has 2 files
  // L2_0: 0, 1, 2, 3, 4
  // L2_1: 5, 6, 7
  // L0 has 1 file
  // L0: 0, [5, 6), 8
  // max_compaction_bytes is less than the size of L2_0 and L2_1.
  // When compacting L0 into L1, it should split into 3 files:
  // compaction output should cut before key 5 and key 8 to
  // limit future compaction size.
  const int kNumPerFile = 4;
  const int kNumFiles = 2;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = 9 * 1024;
  options.max_compaction_bytes = 9 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(2);
  ASSERT_EQ(2, NumTableFilesAtLevel(2));
  ASSERT_OK(Put(Key(0), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(5),
                             Key(6)));
  ASSERT_OK(Put(Key(8), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(3, NumTableFilesAtLevel(1));
}

TEST_F(DBRangeDelTest, OversizeCompactionGapBetweenTombstone) {
  // L2 has two files
  // L2_0: 0, 1, 2, 3, 4. L2_1: 5, 6, 7
  // L0 has two range tombstones [0, 1), [7, 8).
  // max_compaction_bytes is less than the size of L2_0.
  // When compacting L0 into L1, the two range tombstones should be
  // split into two files.
  const int kNumPerFile = 4;
  const int kNumFiles = 2;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = 9 * 1024;
  options.max_compaction_bytes = 9 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(2);
  ASSERT_EQ(2, NumTableFilesAtLevel(2));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(7),
                             Key(8)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  // This is L0 -> L1 compaction
  // The two range tombstones are broken up into two output files
  // to limit compaction size.
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
}

TEST_F(DBRangeDelTest, OversizeCompactionPointKeyWithinRangetombstone) {
  // L2 has two files
  // L2_0: 0, 1, 2, 3, 4. L2_1: 6, 7, 8
  // L0 has [0, 9) and point key 5
  // max_compaction_bytes is less than the size of L2_0.
  // When compacting L0 into L1, the compaction should cut at point key 5.
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = 9 * 1024;
  options.max_compaction_bytes = 9 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < 9; ++i) {
    if (i == 5) {
      ++i;
    }
    ASSERT_OK(Put(Key(i), rnd.RandomString(3 << 10)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(2);
  ASSERT_EQ(2, NumTableFilesAtLevel(2));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(9)));
  ASSERT_OK(Put(Key(5), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
}

TEST_F(DBRangeDelTest, OverlappedTombstones) {
  const int kNumPerFile = 4;
  const int kNumFiles = 2;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = 9 * 1024;
  options.max_compaction_bytes = 9 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(2);
  ASSERT_EQ(2, NumTableFilesAtLevel(2));

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key((kNumFiles)*kNumPerFile + 1)));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));

  // The tombstone range is not broken up into multiple SSTs which may incur a
  // large compaction with L2.
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  std::vector<std::vector<FileMetaData>> files;
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
}

TEST_F(DBRangeDelTest, OverlappedKeys) {
  const int kNumPerFile = 4;
  const int kNumFiles = 2;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = 9 * 1024;
  options.max_compaction_bytes = 9 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    std::vector<std::string> values;
    // Write 12K (4 values, each 3K)
    for (int j = 0; j < kNumPerFile; j++) {
      values.push_back(rnd.RandomString(3 << 10));
      ASSERT_OK(Put(Key(i * kNumPerFile + j), values[j]));
    }
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(2);
  ASSERT_EQ(2, NumTableFilesAtLevel(2));

  for (int i = 1; i < kNumFiles * kNumPerFile + 1; i++) {
    ASSERT_OK(Put(Key(i), "0x123"));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // The key range is broken up into three SSTs to avoid a future big compaction
  // with the grandparent
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(3, NumTableFilesAtLevel(1));

  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  // L1->L2 compaction size is limited to max_compaction_bytes
  ASSERT_EQ(3, NumTableFilesAtLevel(2));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
}

TEST_F(DBRangeDelTest, IteratorRefresh) {
  // Refreshing an iterator after a range tombstone is added should cause the
  // deleted range of keys to disappear.
  for (bool sv_changed : {false, true}) {
    ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
    ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));

    auto* iter = db_->NewIterator(ReadOptions());
    ASSERT_OK(iter->status());

    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "key2", "key3"));

    if (sv_changed) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }

    ASSERT_OK(iter->Refresh());
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    ASSERT_EQ("key1", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    delete iter;
  }
}

void VerifyIteratorReachesEnd(InternalIterator* iter) {
  ASSERT_TRUE(!iter->Valid() && iter->status().ok());
}

void VerifyIteratorReachesEnd(Iterator* iter) {
  ASSERT_TRUE(!iter->Valid() && iter->status().ok());
}

TEST_F(DBRangeDelTest, IteratorReseek) {
  // Range tombstone triggers reseek (seeking to a range tombstone end key) in
  // merging iterator. Test set up:
  //    one memtable: range tombstone [0, 1)
  //    one immutable memtable: range tombstone [1, 2)
  //    one L0 file with range tombstone [2, 3)
  //    one L1 file with range tombstone [3, 4)
  // Seek(0) should trigger cascading reseeks at all levels below memtable.
  // Seek(1) should trigger cascading reseeks at all levels below immutable
  // memtable. SeekToFirst and SeekToLast trigger no reseek.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  // L1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(4)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  // L0
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  // Immutable memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key(2)));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  std::string value;
  ASSERT_TRUE(dbfull()->GetProperty(db_->DefaultColumnFamily(),
                                    "rocksdb.num-immutable-mem-table", &value));
  ASSERT_EQ(1, std::stoi(value));
  // live memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));
  // this memtable is still active
  ASSERT_TRUE(dbfull()->GetProperty(db_->DefaultColumnFamily(),
                                    "rocksdb.num-immutable-mem-table", &value));
  ASSERT_EQ(1, std::stoi(value));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  iter->Seek(Key(0));
  // Reseeked immutable memtable, L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 3);
  VerifyIteratorReachesEnd(iter);
  get_perf_context()->Reset();
  iter->SeekForPrev(Key(1));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  VerifyIteratorReachesEnd(iter);
  get_perf_context()->Reset();
  iter->SeekToFirst();
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  VerifyIteratorReachesEnd(iter);
  iter->SeekToLast();
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  VerifyIteratorReachesEnd(iter);
  delete iter;
}

TEST_F(DBRangeDelTest, ReseekDuringNextAndPrev) {
  // Range tombstone triggers reseek during Next()/Prev() in merging iterator.
  // Test set up:
  //    memtable has: [0, 1) [2, 3)
  //    L0 has: 2
  //    L1 has: 1, 2, 3
  // Seek(0) will reseek to 1 for L0 and L1. Seek(1) will not trigger any
  // reseek. Then Next() determines 2 is covered by [2, 3), it will try to
  // reseek to 3 for L0 and L1. Similar story for Prev() and SeekForPrev() is
  // tested.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  // L1
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L0
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // Memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));

  auto iter = db_->NewIterator(ReadOptions());
  auto iter_test_forward = [&] {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(1));

    get_perf_context()->Reset();
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(3));
    // Reseeked L0 and L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

    // Next to Prev
    get_perf_context()->Reset();
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(1));
    // Reseeked L0 and L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

    // Prev to Next
    get_perf_context()->Reset();
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(3));
    // Reseeked L0 and L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

    iter->Next();
    VerifyIteratorReachesEnd(iter);
  };

  get_perf_context()->Reset();
  iter->Seek(Key(0));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  iter_test_forward();
  get_perf_context()->Reset();
  iter->Seek(Key(1));
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  iter_test_forward();

  get_perf_context()->Reset();
  iter->SeekForPrev(Key(2));
  // Reseeked L0 and L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);
  iter_test_forward();
  get_perf_context()->Reset();
  iter->SeekForPrev(Key(1));
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  iter_test_forward();

  get_perf_context()->Reset();
  iter->SeekToFirst();
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 0);
  iter_test_forward();

  iter->SeekToLast();
  iter->Prev();
  iter_test_forward();
  delete iter;
}

TEST_F(DBRangeDelTest, TombstoneFromCurrentLevel) {
  // Range tombstone triggers reseek when covering key from the same level.
  // in merging iterator. Test set up:
  //    memtable has: [0, 1)
  //    L0 has: [2, 3), 2
  //    L1 has: 1, 2, 3
  // Seek(0) will reseek to 1 for L0 and L1.
  // Then Next() will reseek to 3 for L1 since 2 in L0 is covered by [2, 3) in
  // L0.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  // L1
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L0
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // Memtable
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(1)));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  iter->Seek(Key(0));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(1));
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 2);

  get_perf_context()->Reset();
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(3));
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);

  delete iter;
}

class TombstoneTestSstPartitioner : public SstPartitioner {
 public:
  const char* Name() const override { return "SingleKeySstPartitioner"; }

  PartitionerResult ShouldPartition(
      const PartitionerRequest& request) override {
    if (cmp->Compare(*request.current_user_key, DBTestBase::Key(5)) == 0) {
      return kRequired;
    } else {
      return kNotRequired;
    }
  }

  bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                        const Slice& /*largest_user_key*/) override {
    return false;
  }

  const Comparator* cmp = BytewiseComparator();
};

class TombstoneTestSstPartitionerFactory : public SstPartitionerFactory {
 public:
  static const char* kClassName() {
    return "TombstoneTestSstPartitionerFactory";
  }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override {
    return std::unique_ptr<SstPartitioner>(new TombstoneTestSstPartitioner());
  }
};

TEST_F(DBRangeDelTest, TombstoneAcrossFileBoundary) {
  // Verify that a range tombstone across file boundary covers keys from older
  // levels. Test set up:
  //    L1_0: 1, 3, [2, 6)   L1_1: 5, 7, [2, 6) ([2, 6) is from compaction with
  //    L1_0) L2 has: 5
  // Seek(1) and then Next() should move the L1 level iterator to
  // L1_1. Check if 5 is returned after Next().
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 2 * 1024;
  options.max_compaction_bytes = 2 * 1024;

  // Make sure L1 files are split before "5"
  auto factory = std::make_shared<TombstoneTestSstPartitionerFactory>();
  options.sst_partitioner_factory = factory;

  DestroyAndReopen(options);

  Random rnd(301);
  // L2
  // the file should be smaller than max_compaction_bytes, otherwise the file
  // will be cut before 7.
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(1 << 9)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(7), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(1 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), rnd.RandomString(1 << 10)));
  // Prevent keys being compacted away
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(6)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  iter->Seek(Key(1));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(1));
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(7));
  // 1 reseek into L2 when key 5 in L2 is covered by [2, 6) from L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);

  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, NonOverlappingTombstonAtBoundary) {
  // Verify that a range tombstone across file boundary covers keys from older
  // levels.
  // Test set up:
  //    L1_0: 1, 3, [4, 7)         L1_1: 6, 8, [4, 7)
  //    L2: 5
  // L1_0's largest key: Key(6)@kMaxSequenceNumber with type kTypeRangeDeletion
  // Note that [4, 7) is at end of L1_0 and not overlapping with any point key
  // in L1_0. [4, 7) from L1_0 should cover 5 if sentinel in LevelIterator works
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 4 * 1024;
  DestroyAndReopen(options);

  Random rnd(301);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(6), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(8), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), rnd.RandomString(4 << 10)));
  // Prevent keys being compacted away
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(7)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
  InternalKey ik = InternalKey(Key(6), kMaxSequenceNumber, kTypeRangeDeletion);
  ASSERT_EQ(files[1][0].largest.Encode(), ik.Encode());

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(3));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(3));
  get_perf_context()->Reset();
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(8));
  // 1 reseek into L1 since 5 from L2 is covered by [4, 7) from L1
  ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);
  for (auto& k : {4, 5, 6}) {
    get_perf_context()->Reset();
    iter->Seek(Key(k));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), Key(8));
    // 1 reseek into L1
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count, 1);
  }
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, OlderLevelHasNewerData) {
  // L1_0: 1, 3, [2, 7)   L1_1: 5, 6 at a newer sequence number than [2, 7)
  // Compact L1_1 to L2. Seek(3) should not skip 5 or 6.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  DestroyAndReopen(options);

  Random rnd(301);
  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), rnd.RandomString(4 << 10)));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(7)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(6), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  auto key = Key(6);
  Slice begin(key);
  EXPECT_OK(dbfull()->TEST_CompactRange(1, &begin, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(3));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(5));
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), Key(6));
  delete iter;
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, LevelBoundaryDefinedByTombstone) {
  // L1 has: 1, 2, [4, 5)
  // L2 has: 4
  // Seek(3), which is over all points keys in L1, check whether
  // sentinel key from L1 works in this case.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  DestroyAndReopen(options);
  Random rnd(301);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  const Snapshot* snapshot = db_->GetSnapshot();
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(3));
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());

  get_perf_context()->Reset();
  iter->SeekForPrev(Key(5));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(2));
  db_->ReleaseSnapshot(snapshot);
  delete iter;
}

TEST_F(DBRangeDelTest, TombstoneOnlyFile) {
  // L1_0: 1, 2, L1_1: [3, 5)
  // L2: 3
  // Seek(2) then Next() should advance L1 iterator into L1_1.
  // If sentinel works with tombstone only file, it should cover the key in L2.
  // Similar story for SeekForPrev(4).
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  Random rnd(301);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(2));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(2));
  iter->Next();
  VerifyIteratorReachesEnd(iter);
  iter->SeekForPrev(Key(4));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(2));
  iter->Next();
  VerifyIteratorReachesEnd(iter);
  delete iter;
}

void VerifyIteratorKey(InternalIterator* iter,
                       const std::vector<std::string>& expected_keys,
                       bool forward = true) {
  for (auto& key : expected_keys) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->user_key(), key);
    if (forward) {
      iter->Next();
    } else {
      iter->Prev();
    }
  }
}

TEST_F(DBRangeDelTest, TombstoneOnlyLevel) {
  // L1 [3, 5)
  // L2 has: 3, 4
  // Any kind of iterator seek should skip 3 and 4 in L2.
  // L1 level iterator should produce sentinel key.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  get_perf_context()->Reset();
  uint64_t expected_reseek = 0;
  for (auto i = 0; i < 7; ++i) {
    iter->Seek(Key(i));
    VerifyIteratorReachesEnd(iter);
    if (i < 5) {
      ++expected_reseek;
    }
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              expected_reseek);
    iter->SeekForPrev(Key(i));
    VerifyIteratorReachesEnd(iter);
    if (i > 2) {
      ++expected_reseek;
    }
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              expected_reseek);
    iter->SeekToFirst();
    VerifyIteratorReachesEnd(iter);
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              ++expected_reseek);
    iter->SeekToLast();
    VerifyIteratorReachesEnd(iter);
    ASSERT_EQ(get_perf_context()->internal_range_del_reseek_count,
              ++expected_reseek);
  }
  delete iter;

  // Check L1 LevelIterator behavior
  ColumnFamilyData* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  SuperVersion* sv = cfd->GetSuperVersion();
  Arena arena;
  ReadOptions read_options;
  MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(), &arena,
                                          false /* prefix seek */);
  InternalIterator* level_iter = sv->current->TEST_GetLevelIterator(
      read_options, &merge_iter_builder, 1 /* level */, true);
  // This is needed to make LevelIterator range tombstone aware
  auto miter = merge_iter_builder.Finish();
  auto k = Key(3);
  IterKey target;
  target.SetInternalKey(k, kMaxSequenceNumber, kValueTypeForSeek);
  level_iter->Seek(target.GetInternalKey());
  // sentinel key (file boundary as a fake key)
  VerifyIteratorKey(level_iter, {Key(5)});
  VerifyIteratorReachesEnd(level_iter);

  k = Key(5);
  target.SetInternalKey(k, 0, kValueTypeForSeekForPrev);
  level_iter->SeekForPrev(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(3)}, false);
  VerifyIteratorReachesEnd(level_iter);

  level_iter->SeekToFirst();
  VerifyIteratorKey(level_iter, {Key(5)});
  VerifyIteratorReachesEnd(level_iter);

  level_iter->SeekToLast();
  VerifyIteratorKey(level_iter, {Key(3)}, false);
  VerifyIteratorReachesEnd(level_iter);

  miter->~InternalIterator();
}

TEST_F(DBRangeDelTest, TombstoneOnlyWithOlderVisibleKey) {
  // L1: [3, 5)
  // L2: 2, 4, 5
  // 2 and 5 should be visible
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), "bar"));
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), "foobar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // l1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(5)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  auto iter_test_backward = [&] {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(5));
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(2));
    iter->Prev();
    VerifyIteratorReachesEnd(iter);
  };
  auto iter_test_forward = [&] {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(2));
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(5));
    iter->Next();
    VerifyIteratorReachesEnd(iter);
  };
  iter->Seek(Key(4));
  iter_test_backward();
  iter->SeekForPrev(Key(4));
  iter->Next();
  iter_test_backward();

  iter->Seek(Key(4));
  iter->Prev();
  iter_test_forward();
  iter->SeekForPrev(Key(4));
  iter_test_forward();

  iter->SeekToFirst();
  iter_test_forward();
  iter->SeekToLast();
  iter_test_backward();

  delete iter;
}

TEST_F(DBRangeDelTest, TombstoneSentinelDirectionChange) {
  // L1: 7
  // L2: [4, 6)
  // L3: 4
  // Seek(5) will have sentinel key 6 at the top of minHeap in merging iterator.
  //  then do a prev, how would sentinel work?
  // Redo the test after Put(5) into L1 so that there is a visible key in range
  // [4, 6).
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  // L3
  ASSERT_OK(db_->Put(WriteOptions(), Key(4), "bar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(3);
  ASSERT_EQ(1, NumTableFilesAtLevel(3));
  // L2
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(6)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1
  ASSERT_OK(db_->Put(WriteOptions(), Key(7), "foobar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(5));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(7));
  iter->Prev();
  ASSERT_TRUE(!iter->Valid() && iter->status().ok());
  delete iter;

  ASSERT_OK(db_->Put(WriteOptions(), Key(5), "foobar"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  iter = db_->NewIterator(ReadOptions());
  iter->Seek(Key(5));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(5));
  iter->Prev();
  ASSERT_TRUE(!iter->Valid() && iter->status().ok());
  delete iter;
}

// Right sentinel tested in many test cases above
TEST_F(DBRangeDelTest, LeftSentinelKeyTest) {
  // L1_0: 0, 1    L1_1: [2, 3), 5
  // L2: 2
  // SeekForPrev(4) should give 1 due to sentinel key keeping [2, 3) alive.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  options.max_compaction_bytes = 2048;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_0
  Random rnd(301);
  ASSERT_OK(db_->Put(WriteOptions(), Key(0), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), "bar"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(3)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  iter->SeekForPrev(Key(4));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(1));
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(0));
  iter->Prev();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());
  delete iter;
}

TEST_F(DBRangeDelTest, LeftSentinelKeyTestWithNewerKey) {
  // L1_0: 1, 2 newer than L1_1,    L1_1: [2, 4), 5
  // L2: 3
  // SeekForPrev(4) then Prev() should give 2 and then 1.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;
  options.max_compaction_bytes = 3 * 1024;

  DestroyAndReopen(options);
  // L2
  ASSERT_OK(db_->Put(WriteOptions(), Key(3), "foo"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1_1
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), "bar"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(4)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_0
  Random rnd(301);
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  // Used to verify sequence number of iterator key later.
  auto seq = dbfull()->TEST_GetLastVisibleSequence();
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  Arena arena;
  InternalKeyComparator icmp(options.comparator);
  ReadOptions read_options;
  ScopedArenaPtr<InternalIterator> iter;
  iter.reset(
      dbfull()->NewInternalIterator(read_options, &arena, kMaxSequenceNumber));

  auto k = Key(4);
  IterKey target;
  target.SetInternalKey(k, 0 /* sequence_number */, kValueTypeForSeekForPrev);
  iter->SeekForPrev(target.GetInternalKey());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->user_key(), Key(2));
  SequenceNumber actual_seq;
  ValueType type;
  UnPackSequenceAndType(ExtractInternalKeyFooter(iter->key()), &actual_seq,
                        &type);
  ASSERT_EQ(seq, actual_seq);
  // might as well check type
  ASSERT_EQ(type, kTypeValue);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->user_key(), Key(1));
  iter->Prev();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBRangeDelTest, SentinelKeyCommonCaseTest) {
  // L1 has 3 files
  // L1_0: 1, 2     L1_1: [3, 4) 5, 6, [7, 8)     L1_2: 9
  // Check iterator operations on LevelIterator.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 3 * 1024;

  DestroyAndReopen(options);
  Random rnd(301);
  // L1_0
  ASSERT_OK(db_->Put(WriteOptions(), Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(2), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  // L1_1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(4)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(5), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Put(WriteOptions(), Key(6), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(7),
                             Key(8)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));

  // L1_2
  ASSERT_OK(db_->Put(WriteOptions(), Key(9), rnd.RandomString(4 << 10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(3, NumTableFilesAtLevel(1));

  ColumnFamilyData* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  SuperVersion* sv = cfd->GetSuperVersion();
  Arena arena;
  ReadOptions read_options;
  MergeIteratorBuilder merge_iter_builder(&cfd->internal_comparator(), &arena,
                                          false /* prefix seek */);
  InternalIterator* level_iter = sv->current->TEST_GetLevelIterator(
      read_options, &merge_iter_builder, 1 /* level */, true);
  // This is needed to make LevelIterator range tombstone aware
  auto miter = merge_iter_builder.Finish();
  auto k = Key(7);
  IterKey target;
  target.SetInternalKey(k, kMaxSequenceNumber, kValueTypeForSeek);
  level_iter->Seek(target.GetInternalKey());
  // The last Key(9) is a sentinel key.
  VerifyIteratorKey(level_iter, {Key(8), Key(9), Key(9)});
  ASSERT_TRUE(!level_iter->Valid() && level_iter->status().ok());

  k = Key(6);
  target.SetInternalKey(k, kMaxSequenceNumber, kValueTypeForSeek);
  level_iter->Seek(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(6), Key(8), Key(9), Key(9)});
  ASSERT_TRUE(!level_iter->Valid() && level_iter->status().ok());

  k = Key(4);
  target.SetInternalKey(k, 0, kValueTypeForSeekForPrev);
  level_iter->SeekForPrev(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(3), Key(2), Key(1), Key(1)}, false);
  ASSERT_TRUE(!level_iter->Valid() && level_iter->status().ok());

  k = Key(5);
  target.SetInternalKey(k, 0, kValueTypeForSeekForPrev);
  level_iter->SeekForPrev(target.GetInternalKey());
  VerifyIteratorKey(level_iter, {Key(5), Key(3), Key(2), Key(1), Key(1)},
                    false);

  level_iter->SeekToFirst();
  VerifyIteratorKey(level_iter, {Key(1), Key(2), Key(2), Key(5), Key(6), Key(8),
                                 Key(9), Key(9)});
  ASSERT_TRUE(!level_iter->Valid() && level_iter->status().ok());

  level_iter->SeekToLast();
  VerifyIteratorKey(
      level_iter,
      {Key(9), Key(9), Key(6), Key(5), Key(3), Key(2), Key(1), Key(1)}, false);
  ASSERT_TRUE(!level_iter->Valid() && level_iter->status().ok());

  miter->~InternalIterator();
}

TEST_F(DBRangeDelTest, PrefixSentinelKey) {
  // L1: ['aaaa', 'aaad'), 'bbbb'
  // L2: 'aaac', 'aaae'
  // Prefix extracts first 3 chars
  // Seek('aaab') should give 'aaae' as first key.
  // This is to test a previous bug where prefix seek sees there is no prefix in
  // the SST file, and will just set file iter to null in LevelIterator and may
  // just skip to the next SST file. But in this case, we should keep the file's
  // tombstone alive.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  Random rnd(301);

  // L2:
  ASSERT_OK(db_->Put(WriteOptions(), "aaac", rnd.RandomString(10)));
  ASSERT_OK(db_->Put(WriteOptions(), "aaae", rnd.RandomString(10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // L1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "aaaa",
                             "aaad"));
  ASSERT_OK(db_->Put(WriteOptions(), "bbbb", rnd.RandomString(10)));
  ASSERT_OK(db_->Flush(FlushOptions()));
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  auto iter = db_->NewIterator(ReadOptions());
  iter->Seek("aaab");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), "aaae");
  delete iter;
}

namespace {
Options PrefixFilterOptions(const Options& base) {
  Options options = base;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(4));
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  return options;
}

// Scans the whole DB from SeekToFirst() (or SeekToLast() if !forward).
std::vector<std::string> ScanAllKeys(DB* db, const ReadOptions& ro,
                                     bool forward) {
  std::vector<std::string> keys;
  std::unique_ptr<Iterator> iter(db->NewIterator(ro));
  if (forward) {
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
  } else {
    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      keys.push_back(iter->key().ToString());
    }
  }
  EXPECT_OK(iter->status());
  return keys;
}
}  // namespace

// Older data with prefixes aaaa, kkkk and xxxx only; newer tombstones whose
// boundary keys have prefixes cccc and pppp, which the older data lacks.
class DBRangeDelCrossPrefixTest : public DBRangeDelTest {
 protected:
  void Populate(const Options& options, int older_level) {
    DestroyAndReopen(options);
    ASSERT_OK(Put("aaaa1", "v"));
    ASSERT_OK(Put("kkkk1", "v"));
    ASSERT_OK(Put("kkkk5", "v"));
    ASSERT_OK(Put("xxxx1", "v"));
    ASSERT_OK(Flush());
    if (older_level > 0) {
      MoveFilesToLevel(older_level);
      ASSERT_EQ(1, NumTableFilesAtLevel(older_level));
    }
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "aaaa", "cccc2"));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               "pppp", "zzzz"));
  }
  const std::vector<std::string> kLiveKeys = {"kkkk1", "kkkk5"};
  const std::vector<std::string> kLiveKeysReversed = {"kkkk5", "kkkk1"};
};

TEST_F(DBRangeDelCrossPrefixTest, PrefixSameAsStartOptInRequiresSeekPrefix) {
  // Under prefix_seek_opt_in_only, a prefix_same_as_start scan must take its
  // prefix from an in-domain seek key, with or without bounds. With upper
  // bound "kkkl", SeekToLast() would otherwise take prefix "kkkl" from the
  // bound and return no keys.
  Options options = PrefixFilterOptions(CurrentOptions());
  options.prefix_seek_opt_in_only = true;
  Populate(options, /*older_level=*/1);

  const Slice lower_bound("kkkk");
  // The immediate successor of prefix "kkkk".
  const Slice upper_bound("kkkl");
  for (bool with_bounds : {false, true}) {
    SCOPED_TRACE(with_bounds ? "with bounds" : "without bounds");
    ReadOptions ro;
    ro.prefix_same_as_start = true;
    if (with_bounds) {
      ro.iterate_lower_bound = &lower_bound;
      ro.iterate_upper_bound = &upper_bound;
    }
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsNotSupported());
    iter->SeekToLast();
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsNotSupported());
    // "kkk" is shorter than the 4-byte prefix, so it is out of domain.
    iter->Seek("kkk");
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());
    iter->SeekForPrev("kkk");
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());

    // Seek() and SeekForPrev() to a key in the prefix still work.
    std::vector<std::string> keys;
    for (iter->Seek("kkkk"); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kLiveKeys, keys);
    keys.clear();
    for (iter->SeekForPrev("kkkk9"); iter->Valid(); iter->Prev()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kLiveKeysReversed, keys);
  }

  // Total order and auto_prefix_mode iterators are not affected.
  ReadOptions total_order;
  ASSERT_EQ(kLiveKeys, ScanAllKeys(db_.get(), total_order, /*forward=*/true));
  ReadOptions auto_prefix;
  auto_prefix.auto_prefix_mode = true;
  ASSERT_EQ(kLiveKeys, ScanAllKeys(db_.get(), auto_prefix, /*forward=*/true));
}

TEST_F(DBRangeDelCrossPrefixTest, AutoPrefixModeFullScanCrossPrefixTombstone) {
  ReadOptions auto_prefix;
  auto_prefix.auto_prefix_mode = true;
  for (int older_level : {0, 1}) {
    SCOPED_TRACE("older_level=" + std::to_string(older_level));
    Populate(PrefixFilterOptions(CurrentOptions()), older_level);
    ASSERT_EQ(kLiveKeys, ScanAllKeys(db_.get(), auto_prefix, /*forward=*/true));
    ASSERT_EQ(kLiveKeysReversed,
              ScanAllKeys(db_.get(), auto_prefix, /*forward=*/false));
  }
}

TEST_F(DBRangeDelTest, PrefixSameAsStartUnboundedScanNoRangeConversion) {
  // A prefix_same_as_start scan with no prefix to bound it (unbounded
  // SeekToFirst(), or a seek target outside the prefix extractor's domain) can
  // skip live keys, so it must not convert point deletes into a range
  // tombstone.
  Options options = PrefixFilterOptions(CurrentOptions());
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  ASSERT_OK(Put("kkkk1", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("aaaa1"));
  ASSERT_OK(Delete("aaaa2"));
  ASSERT_OK(Delete("aaaa3"));

  ReadOptions ro;
  ro.prefix_same_as_start = true;
  for (bool seek_to_first : {true, false}) {
    SCOPED_TRACE(seek_to_first ? "SeekToFirst" : "out-of-domain Seek");
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    if (seek_to_first) {
      iter->SeekToFirst();
    } else {
      // "aaa" is shorter than the 4-byte prefix, so it is out of domain.
      iter->Seek("aaa");
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(0, options.statistics->getTickerCount(
                     READ_PATH_RANGE_TOMBSTONES_INSERTED));
  }
  // A scan started by an in-domain Seek() still converts the same deletes.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek("aaaa");
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->Valid());
  }
  ASSERT_EQ(1, options.statistics->getTickerCount(
                   READ_PATH_RANGE_TOMBSTONES_INSERTED));
}

TEST_F(DBRangeDelTest, UnorderedWriteRangeConversionKeepsPendingWrite) {
  // With unordered_write, a write publishes its sequence number before it
  // inserts into the memtable. In this test, "b1", "b2", "b4" and "b5" are
  // deleted, and a writer of "b3" is paused after it publishes its sequence
  // number and before it inserts. The test then writes "d", so a new scan
  // reads at a sequence number above the one of "b3". The scan does not see
  // "b3", so it sees the four deletes as one run. A range tombstone over that
  // run would get the scan's sequence number, so every read at that sequence
  // number or higher would miss "b3", also after the insert of "b3"
  // finishes. With unordered_write the scan inserts no converted range
  // tombstone.
  Options options = CurrentOptions();
  options.unordered_write = true;
  // With a threshold of 2, a conversion of the four deletes would insert
  // ["b1", "c"), which covers "b3".
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("c", "vc"));
  for (const char* k : {"b1", "b2", "b4", "b5"}) {
    ASSERT_OK(Delete(k));
  }

  // Only the "b3" writer stops in the callback. The test thread's own write
  // below takes the same code path and must not stop.
  std::atomic<std::thread::id> paused_writer{};
  std::promise<void> published;
  std::promise<void> scan_done;
  std::shared_future<void> scan_done_future = scan_done.get_future().share();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:BeforeUnorderedWriteMemtable", [&](void*) {
        if (std::this_thread::get_id() == paused_writer.load()) {
          published.set_value();
          scan_done_future.wait();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  {
    port::Thread writer([&] {
      paused_writer.store(std::this_thread::get_id());
      ASSERT_OK(Put("b3", "v3"));
    });
    // This Defer releases and joins the writer, even when an assertion below
    // fails.
    Defer release_writer([&] {
      scan_done.set_value();
      writer.join();
    });
    // A bounded wait: the test fails instead of hanging if the writer never
    // reaches the callback.
    ASSERT_EQ(std::future_status::ready,
              published.get_future().wait_for(std::chrono::minutes(5)));
    // Write "d", so the iterator below gets a read sequence number above the
    // pending "b3" write's. A converted range tombstone would get the scan's
    // read sequence number, so it would cover the pending write.
    ASSERT_OK(Put("d", "vd"));

    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    std::vector<std::string> keys;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      keys.push_back(iter->key().ToString());
    }
    ASSERT_OK(iter->status());
    // "b3" is not in the memtable yet, so the scan cannot see it.
    ASSERT_EQ((std::vector<std::string>{"a", "c", "d"}), keys);
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // "b3" finished its write, so every new read must see it.
  ASSERT_EQ("v3", Get("b3"));
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek("b");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("b3", iter->key());
  }
  // No conversion was attempted: nothing was inserted or discarded.
  ASSERT_EQ(0, options.statistics->getTickerCount(
                   READ_PATH_RANGE_TOMBSTONES_INSERTED));
  ASSERT_EQ(0, options.statistics->getTickerCount(
                   READ_PATH_RANGE_TOMBSTONES_DISCARDED));
}

TEST_F(DBRangeDelTest, RefreshMemtableIter) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ReadOptions ro;
  ro.read_tier = kMemtableTier;
  std::unique_ptr<Iterator> iter{db_->NewIterator(ro)};
  ASSERT_OK(Flush());
  // First refresh reinits iter, which had a bug where
  // iter.memtable_range_tombstone_iter_ was not set to nullptr, and caused
  // subsequent refresh to double free.
  ASSERT_OK(iter->Refresh());
  ASSERT_OK(iter->Refresh());
}

TEST_F(DBRangeDelTest, RangeTombstoneRespectIterateUpperBound) {
  // Memtable: a, [b, bz)
  // Do a Seek on `a` with iterate_upper_bound being az
  // range tombstone [b, bz) should not be processed (added to and
  // popped from the min_heap in MergingIterator).
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "bar"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "bz"));

  // I could not find a cleaner way to test this without relying on
  // implementation detail. Tried to test the value of
  // `internal_range_del_reseek_count` but that did not work
  // since BlockBasedTable iterator becomes !Valid() when point key
  // is out of bound and that reseek only happens when a point key
  // is covered by some range tombstone.
  SyncPoint::GetInstance()->SetCallBack("MergeIterator::PopDeleteRangeStart",
                                        [](void*) {
                                          // there should not be any range
                                          // tombstone in the heap.
                                          FAIL();
                                        });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions read_opts;
  std::string upper_bound = "az";
  Slice upper_bound_slice = upper_bound;
  read_opts.iterate_upper_bound = &upper_bound_slice;
  std::unique_ptr<Iterator> iter{db_->NewIterator(read_opts)};
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), "a");
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBRangeDelTest, RangetombesoneCompensateFilesize) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);

  std::vector<std::string> values;
  Random rnd(301);
  // file in L2
  values.push_back(rnd.RandomString(1 << 10));
  ASSERT_OK(Put("a", values.back()));
  values.push_back(rnd.RandomString(1 << 10));
  ASSERT_OK(Put("b", values.back()));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  uint64_t l2_size = 0;
  ASSERT_OK(Size("a", "c", 0 /* cf */, &l2_size));
  ASSERT_GT(l2_size, 0);
  // file in L1
  values.push_back(rnd.RandomString(1 << 10));
  ASSERT_OK(Put("d", values.back()));
  values.push_back(rnd.RandomString(1 << 10));
  ASSERT_OK(Put("e", values.back()));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  uint64_t l1_size = 0;
  ASSERT_OK(Size("d", "f", 0 /* cf */, &l1_size));
  ASSERT_GT(l1_size, 0);

  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "f"));
  ASSERT_OK(Flush());
  // Range deletion compensated size computed during flush time
  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[0].size(), 1);
  ASSERT_EQ(level_to_files[0][0].compensated_range_deletion_size,
            l1_size + l2_size);
  ASSERT_EQ(level_to_files[1].size(), 1);
  ASSERT_EQ(level_to_files[1][0].compensated_range_deletion_size, 0);
  ASSERT_EQ(level_to_files[2].size(), 1);
  ASSERT_EQ(level_to_files[2][0].compensated_range_deletion_size, 0);

  // Range deletion compensated size computed during compaction time
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(2), 1);
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[1].size(), 1);
  ASSERT_EQ(level_to_files[1][0].compensated_range_deletion_size, l2_size);
  ASSERT_EQ(level_to_files[2].size(), 1);
  ASSERT_EQ(level_to_files[2][0].compensated_range_deletion_size, 0);
}

TEST_F(DBRangeDelTest, CompensatedSizeCountsEmptyStartKeyOnce) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);

  Random rnd(301);
  ASSERT_OK(Put("a", rnd.RandomString(1 << 10)));
  ASSERT_OK(Put("b", rnd.RandomString(1 << 10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  // The snapshot keeps both range tombstones, so the flush and the compaction
  // output two tombstone fragments that start at the empty user key.
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "", "c"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "", "c"));
  ASSERT_OK(Flush());

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[0].size(), 1);
  ASSERT_EQ(level_to_files[2].size(), 1);
  const uint64_t l2_size = level_to_files[2][0].fd.GetFileSize();
  ASSERT_GT(l2_size, 0);
  // Computed at flush time
  ASSERT_EQ(level_to_files[0][0].compensated_range_deletion_size, l2_size);

  // Computed at compaction time
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[0].size(), 0);
  ASSERT_EQ(level_to_files[1].size(), 1);
  ASSERT_EQ(level_to_files[1][0].compensated_range_deletion_size, l2_size);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, RangetombesoneCompensateFilesizePersistDuringReopen) {
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);

  std::vector<std::string> values;
  Random rnd(301);
  values.push_back(rnd.RandomString(1 << 10));
  ASSERT_OK(Put("a", values.back()));
  values.push_back(rnd.RandomString(1 << 10));
  ASSERT_OK(Put("b", values.back()));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "c"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(Flush());

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[0].size(), 1);
  ASSERT_EQ(level_to_files[1].size(), 1);
  ASSERT_EQ(level_to_files[2].size(), 1);
  uint64_t l2_size = level_to_files[2][0].fd.GetFileSize();
  uint64_t l1_size = level_to_files[1][0].fd.GetFileSize();
  ASSERT_GT(l2_size, 0);
  ASSERT_GT(l1_size, 0);
  ASSERT_EQ(level_to_files[0][0].compensated_range_deletion_size,
            l1_size + l2_size);
  ASSERT_EQ(level_to_files[1][0].compensated_range_deletion_size, l2_size);

  Reopen(opts);
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[0].size(), 1);
  ASSERT_EQ(level_to_files[0][0].compensated_range_deletion_size,
            l1_size + l2_size);
  ASSERT_EQ(level_to_files[1].size(), 1);
  ASSERT_EQ(level_to_files[1][0].compensated_range_deletion_size, l2_size);
}

TEST_F(DBRangeDelTest, SingleKeyFile) {
  // Test for a bug fix where a range tombstone could be added
  // to an SST file while is not within the file's key range.
  // Create 3 files in L0 and then compact them to L1 where all keys have the
  // same user key `Key(2)`.
  // L0_0: Key(2)@5
  // L0_1: Key(2)@4
  // L0_2: Key(2)@3, range tombstone [Key(2), Key(5))@2
  //
  // After compaction, the first output file contains Key(2)@5 and Key(2)@4.
  // Before fix, the range tombstone [Key(2), Key(5))@2 would be added to this
  // file during compaction, but it is not in this file's key range.
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.target_file_size_base = 1 << 10;
  DestroyAndReopen(opts);

  // prevent range tombstone drop
  std::vector<const Snapshot*> snapshots;
  snapshots.push_back(db_->GetSnapshot());

  // write a key to bottommost file so the compactions below
  // are not bottommost compactions and will calculate
  // compensated range tombstone size. Before bug fix, an assert would fail
  // during this process.
  Random rnd(301);
  ASSERT_OK(Put(Key(2), rnd.RandomString(8 << 10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(5)));
  snapshots.push_back(db_->GetSnapshot());
  std::vector<std::string> values;

  values.push_back(rnd.RandomString(8 << 10));
  ASSERT_OK(Put(Key(2), rnd.RandomString(8 << 10)));
  snapshots.push_back(db_->GetSnapshot());
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(2), rnd.RandomString(8 << 10)));
  snapshots.push_back(db_->GetSnapshot());
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(2), rnd.RandomString(8 << 10)));
  snapshots.push_back(db_->GetSnapshot());
  ASSERT_OK(Flush());

  ASSERT_EQ(NumTableFilesAtLevel(0), 3);
  CompactRangeOptions co;
  co.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      0, 1, co, nullptr, nullptr, true, true,
      std::numeric_limits<uint64_t>::max() /*max_file_num_to_ignore*/,
      "" /*trim_ts*/));

  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
  ASSERT_EQ(files[1][0].largest.Encode(),
            InternalKey(Key(2), 4, kTypeValue).Encode());

  for (const auto s : snapshots) {
    db_->ReleaseSnapshot(s);
  }
}

TEST_F(DBRangeDelTest, DoubleCountRangeTombstoneCompensatedSize) {
  // Test for a bug fix if a file has multiple range tombstones
  // with same start and end key but with different sequence numbers,
  // we should only calculate compensated range tombstone size
  // for one of them.
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);

  std::vector<std::string> values;
  Random rnd(301);
  // file in L2
  ASSERT_OK(Put(Key(1), rnd.RandomString(1 << 10)));
  ASSERT_OK(Put(Key(2), rnd.RandomString(1 << 10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  uint64_t l2_size = 0;
  ASSERT_OK(Size(Key(1), Key(3), 0 /* cf */, &l2_size));
  ASSERT_GT(l2_size, 0);

  // file in L1
  ASSERT_OK(Put(Key(3), rnd.RandomString(1 << 10)));
  ASSERT_OK(Put(Key(4), rnd.RandomString(1 << 10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  uint64_t l1_size = 0;
  ASSERT_OK(Size(Key(3), Key(5), 0 /* cf */, &l1_size));
  ASSERT_GT(l1_size, 0);

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key(5)));
  // so that the range tombstone above is not dropped
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(1),
                             Key(5)));
  ASSERT_OK(Flush());
  // Range deletion compensated size computed during flush time
  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[0].size(), 1);
  // instead of 2 * (l1_size + l2_size)
  ASSERT_EQ(level_to_files[0][0].compensated_range_deletion_size,
            l1_size + l2_size);

  // Range deletion compensated size computed during compaction time
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr,
                                        true /* disallow_trivial_move */));
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_EQ(level_to_files[1].size(), 1);
  ASSERT_EQ(level_to_files[1][0].compensated_range_deletion_size, l2_size);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, AddRangeDelsSameLowerAndUpperBound) {
  // Test for an edge case where CompactionOutputs::AddRangeDels()
  // is called with an empty range: `range_tombstone_lower_bound_` is not empty
  // and have the same user_key and sequence number as `next_table_min_key.
  // This used to cause file's smallest and largest key to be incorrectly set
  // such that smallest > largest, and fail some assertions in iterator and/or
  // assertion in VersionSet::ApproximateSize().
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.target_file_size_base = 1 << 10;
  DestroyAndReopen(opts);

  Random rnd(301);
  // Create file at bottommost level so the manual compaction below is
  // non-bottommost level and goes through code path in
  // versions->ApproximateSize() to calculate compensated range tombstone size
  ASSERT_OK(Put(Key(1), "v1"));
  ASSERT_OK(Put(Key(4), "v2"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  ASSERT_OK(Put(Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(Put(Key(3), rnd.RandomString(4 << 10)));
  // So Key(3) does not get dropped.
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(4)));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(3), rnd.RandomString(4 << 10)));
  ASSERT_OK(Put(Key(4), rnd.RandomString(4 << 10)));
  ASSERT_OK(Flush());

  MoveFilesToLevel(1);
  // Each file will have two keys, with Key(3) straddle between two files.
  // File 1: Key(1)@1, Key(3)@6, DeleteRange ends at Key(3)@6
  // File 2: Key(3)@4, Key(4)@7, DeleteRange start from Key(3)@4
  ASSERT_EQ(NumTableFilesAtLevel(1), 2);
  std::vector<std::vector<FileMetaData>> files;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files);
  ASSERT_EQ(files[1][0].largest.Encode(),
            InternalKey(Key(3), 6, kTypeValue).Encode());
  ASSERT_EQ(files[1][1].smallest.Encode(),
            InternalKey(Key(3), 4, kTypeValue).Encode());

  // Manually update compaction output file cutting decisions
  // to cut before range tombstone sentinel Key(3)@4
  // and the point key Key(3)@4 itself
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionOutputs::ShouldStopBefore::manual_decision", [opts](void* p) {
        auto* pair = (std::pair<bool*, const Slice>*)p;
        if ((opts.comparator->Compare(ExtractUserKey(pair->second), Key(3)) ==
             0) &&
            (GetInternalKeySeqno(pair->second) <= 4)) {
          *(pair->first) = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::string begin_key = Key(0);
  std::string end_key = Key(5);
  Slice begin_slice{begin_key};
  Slice end_slice{end_key};
  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      1, 2, CompactRangeOptions(), &begin_slice, &end_slice, true,
      true /* disallow_trivial_move */,
      std::numeric_limits<uint64_t>::max() /*max_file_num_to_ignore*/,
      "" /*trim_ts*/));
  // iterate through to check if any assertion breaks
  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  iter->SeekToFirst();
  std::vector<int> expected{1, 3, 4};
  for (auto i : expected) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(i));
    iter->Next();
  }
  ASSERT_TRUE(iter->status().ok() && !iter->Valid());
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, AddRangeDelsSingleUserKeyTombstoneOnlyFile) {
  // Test for an edge case where CompactionOutputs::AddRangeDels()
  // is called with an SST file that has no point keys, and that
  // the lower bound and upper bound have the same user key.
  // This could cause a file's smallest and largest key to be incorrectly set
  // such that smallest > largest, and fail some assertions in iterator and/or
  // assertion in VersionSet::ApproximateSize().
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.target_file_size_base = 1 << 10;
  DestroyAndReopen(opts);

  Random rnd(301);
  // Create file at bottommost level so the manual compaction below is
  // non-bottommost level and goes through code path like compensate range
  // tombstone size.
  ASSERT_OK(Put(Key(1), "v1"));
  ASSERT_OK(Put(Key(4), "v2"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  ASSERT_OK(Put(Key(1), rnd.RandomString(10)));
  // Key(3)@4
  ASSERT_OK(Put(Key(3), rnd.RandomString(10)));
  const Snapshot* snapshot1 = db_->GetSnapshot();
  // Key(3)@5
  ASSERT_OK(Put(Key(3), rnd.RandomString(10)));
  const Snapshot* snapshot2 = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(4)));
  // Key(3)@7
  ASSERT_OK(Put(Key(3), rnd.RandomString(10)));
  ASSERT_OK(Flush());

  // L0 -> L1 compaction: cut output into two files:
  // File 1: Key(1), Key(3)@7, Range tombstone ends at Key(3)@7
  // File 2: Key(3)@5, Key(3)@4, Range tombstone starts from Key(3)@5
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionOutputs::ShouldStopBefore::manual_decision", [opts](void* p) {
        auto* pair = (std::pair<bool*, const Slice>*)p;
        if ((opts.comparator->Compare(ExtractUserKey(pair->second), Key(3)) ==
             0) &&
            (GetInternalKeySeqno(pair->second) <= 6)) {
          *(pair->first) = true;
          SyncPoint::GetInstance()->DisableProcessing();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::string begin_key = Key(0);
  std::string end_key = Key(5);
  Slice begin_slice{begin_key};
  Slice end_slice{end_key};
  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      0, 1, CompactRangeOptions(), &begin_slice, &end_slice, true,
      true /* disallow_trivial_move */,
      std::numeric_limits<uint64_t>::max() /*max_file_num_to_ignore*/,
      "" /*trim_ts*/));
  ASSERT_EQ(NumTableFilesAtLevel(1), 2);

  // L1 -> L2 compaction, drop the snapshot protecting Key(3)@5.
  // Let ShouldStopBefore() return true for Key(3)@5 (delete range sentinel)
  // and Key(3)@4.
  // Output should have two files:
  // File 1: Key(1), Key(3)@7, range tombstone ends at Key(3)@7
  // File dropped: range tombstone only file (from Key(3)@5 to Key(3)@4)
  // File 2: Range tombstone starting from Key(3)@4, Key(3)@4
  db_->ReleaseSnapshot(snapshot2);
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionOutputs::ShouldStopBefore::manual_decision", [opts](void* p) {
        auto* pair = (std::pair<bool*, const Slice>*)p;
        if ((opts.comparator->Compare(ExtractUserKey(pair->second), Key(3)) ==
             0) &&
            (GetInternalKeySeqno(pair->second) <= 6)) {
          *(pair->first) = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      1, 2, CompactRangeOptions(), &begin_slice, &end_slice, true,
      true /* disallow_trivial_move */,
      std::numeric_limits<uint64_t>::max() /*max_file_num_to_ignore*/,
      "" /*trim_ts*/));
  ASSERT_EQ(NumTableFilesAtLevel(2), 2);
  // iterate through to check if any assertion breaks
  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  iter->SeekToFirst();
  std::vector<int> expected{1, 3, 4};
  for (auto i : expected) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(i));
    iter->Next();
  }
  ASSERT_TRUE(iter->status().ok() && !iter->Valid());
  db_->ReleaseSnapshot(snapshot1);
}

TEST_F(DBRangeDelTest, NonBottommostCompactionDropRangetombstone) {
  // L0: file 1: [DeleteRange[4, 5)], file 2: [3, 6, DeleteRange[8, 9)]
  // L6 file 1: [2, 3], file 2: [7, 8]
  // When compacting the two L0 files to L1, the compaction is non-bottommost
  // since the compaction key range overlaps with L6 file 1. The range tombstone
  // [4, 5) should be dropped since it does not overlap with any file in lower
  // levels. The range tombstone [8, 9) should not be dropped.
  Options opts = CurrentOptions();
  opts.level_compaction_dynamic_level_bytes = false;
  opts.num_levels = 7;
  opts.level0_file_num_compaction_trigger = 3;
  DestroyAndReopen(opts);

  Random rnd(301);
  // L6 file 1
  ASSERT_OK(Put(Key(2), rnd.RandomString(100)));
  ASSERT_OK(Put(Key(3), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  // L6 file 2
  ASSERT_OK(Put(Key(7), rnd.RandomString(100)));
  ASSERT_OK(Put(Key(8), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_EQ(NumTableFilesAtLevel(6), 2);
  // L0 file 1
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(5)));
  ASSERT_OK(Flush());
  // L0 file 2
  ASSERT_OK(Put(Key(3), rnd.RandomString(100)));
  ASSERT_OK(Put(Key(6), rnd.RandomString(100)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(8),
                             Key(9)));
  ASSERT_OK(Flush());
  // nothing is dropped during flush
  std::string property;
  db_->GetProperty(DB::Properties::kAggregatedTableProperties, &property);
  TableProperties output_tp;
  ParseTablePropertiesString(property, &output_tp);
  ASSERT_EQ(output_tp.num_range_deletions, 2);
  // Add one more L0 file to trigger L0->L1 compaction
  ASSERT_OK(Put(Key(1), rnd.RandomString(100)));
  ASSERT_OK(Put(Key(9), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  db_->GetProperty(DB::Properties::kAggregatedTableProperties, &property);
  ParseTablePropertiesString(property, &output_tp);
  ASSERT_EQ(output_tp.num_range_deletions, 1);

  // Now create a snapshot protected range tombstone [4, 5), it should not
  // be dropped.
  ASSERT_OK(Put(Key(4), rnd.RandomString(100)));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(4),
                             Key(5)));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  // All compacted to L6
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel(0));
  db_->GetProperty(DB::Properties::kAggregatedTableProperties, &property);
  ParseTablePropertiesString(property, &output_tp);
  ASSERT_EQ(output_tp.num_range_deletions, 1);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, MemtableMaxRangeDeletions) {
  // Tests option `memtable_max_range_deletions`.
  Options options = CurrentOptions();
  options.memtable_max_range_deletions = 50;
  options.level0_file_num_compaction_trigger = 5;
  DestroyAndReopen(options);

  for (int i = 0; i < 50; ++i) {
    // Intentionally delete overlapping ranges to see if the option
    // checks number of range tombstone fragments instead.
    ASSERT_OK(Put(Key(i), "val1"));
    ASSERT_OK(Put(Key(i + 1), "val2"));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(i), Key(i + 2)));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
  }
  // One more write to trigger flush.
  ASSERT_OK(Put(Key(50), "val"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // This should take effect for the next new memtable.
  ASSERT_OK(db_->SetOptions({{"memtable_max_range_deletions", "1"}}));
  ASSERT_OK(Flush());
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(50), Key(100)));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  // One more write to trigger flush.
  ASSERT_OK(Put(Key(50), "new val"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(3, NumTableFilesAtLevel(0));
}

TEST_F(DBRangeDelTest, FlushReasonStatsMemtableMaxRangeDeletions) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.disable_auto_compactions = true;
  options.memtable_max_range_deletions = 1;

  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason =
      FlushReason::kMemtableMaxRangeDeletions;
  options.listeners.push_back(flush_listener);

  DestroyAndReopen(options);

  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(10)));
  // The next write observes the pending flush request.
  ASSERT_OK(Put(Key(0), "value"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  const uint64_t range_delete_flushes =
      TestGetTickerCount(options, FLUSH_REASON_MEMTABLE_MAX_RANGE_DELETIONS);
  ASSERT_GT(range_delete_flushes, 0);
  EXPECT_EQ(0, TestGetTickerCount(options, FLUSH_REASON_WRITE_BUFFER_FULL));
  EXPECT_EQ(0, TestGetTickerCount(options, FLUSH_REASON_WRITE_BUFFER_MANAGER));
  EXPECT_EQ(0, TestGetTickerCount(
                   options,
                   ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS));
  EXPECT_EQ(0, TestGetTickerCount(options, ATOMIC_FLUSH_REQUEST_REASON_OTHER));
  EXPECT_EQ(range_delete_flushes,
            static_cast<uint64_t>(flush_listener->count.load()));
}

TEST_F(DBRangeDelTest, AtomicFlushReasonStatsMemtableMaxRangeDeletions) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.atomic_flush = true;
  options.disable_auto_compactions = true;
  options.memtable_max_range_deletions = 1;

  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason =
      FlushReason::kMemtableMaxRangeDeletions;
  options.listeners.push_back(flush_listener);

  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, Key(0), "value"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(0),
                             Key(10)));
  // The next write observes the pending flush request.
  ASSERT_OK(Put(Key(0), "value"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  const uint64_t range_delete_flushes =
      TestGetTickerCount(options, FLUSH_REASON_MEMTABLE_MAX_RANGE_DELETIONS);
  ASSERT_GE(range_delete_flushes, 2);
  EXPECT_EQ(1, TestGetTickerCount(
                   options,
                   ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS));
  EXPECT_EQ(0, TestGetTickerCount(options, FLUSH_REASON_WRITE_BUFFER_FULL));
  EXPECT_EQ(0, TestGetTickerCount(
                   options, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL));
  EXPECT_EQ(0, TestGetTickerCount(
                   options, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER));
  EXPECT_EQ(0, TestGetTickerCount(options, ATOMIC_FLUSH_REQUEST_REASON_OTHER));
  EXPECT_EQ(range_delete_flushes,
            static_cast<uint64_t>(flush_listener->count.load()));
}

TEST_F(DBRangeDelTest, RangeDelReseekAfterFileReadError) {
  // This is to test a bug that is fixed in
  // https://github.com/meta/rocksdb/pull/11786.
  Options opts = CurrentOptions();
  opts.num_levels = 7;

  // Set up LSM
  //
  // L4: F1: [key1] F2: [key2]
  // L5:   F3:[DeleteRange(key3, key6)]
  // L6:               F4:[key3, key6]
  // Will inject error when reading from F2.
  // SeekToFirst() should land on key1.
  // Next() should encounter error when reading from F2,
  // and range del reseek should not reset this status.
  Random rnd(301);
  // L6
  ASSERT_OK(Put(Key(3), rnd.RandomString(100)));
  ASSERT_OK(Put(Key(6), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  // L5
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(6)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);
  // L4
  ASSERT_OK(Put(Key(2), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  std::string fname;
  std::vector<LiveFileMetaData> live_files;
  db_->GetLiveFilesMetaData(&live_files);
  for (auto& meta : live_files) {
    if (meta.level == 4) {
      fname = meta.name;
      break;
    }
  }
  ASSERT_TRUE(!fname.empty());
  ASSERT_OK(Put(Key(1), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);

  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::Read::BeforeReturn", [&fname](void* pair_ptr) {
        auto p = static_cast<std::pair<std::string*, IOStatus*>*>(pair_ptr);
        if (p->first->find(fname) != std::string::npos) {
          *p->second = IOStatus::IOError();
          p->second->SetRetryable(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(iter->key(), Key(1));
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());
  iter.reset();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // Reverse scan
  // LSM setup
  // L4: F1: [key2]  F2: [key7, key8]
  // L5:        F3:[[key3, key6)]
  // L6:        F4:[key1, key5]
  // Ingest error when read from F1.
  // SeekToLast() should land on key8.
  // During Prev(), MergingIterator will encounter error when reading from F1
  // and do a range del reseek (it sees key5 covered by a range tombstone).
  DestroyAndReopen(opts);
  // L6
  ASSERT_OK(Put(Key(1), rnd.RandomString(100)));
  ASSERT_OK(Put(Key(5), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  // L5
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(6)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);
  // L4
  ASSERT_OK(Put(Key(2), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  live_files.clear();
  db_->GetLiveFilesMetaData(&live_files);
  for (auto& meta : live_files) {
    if (meta.level == 4) {
      fname = meta.name;
      break;
    }
  }
  ASSERT_TRUE(!fname.empty());
  ASSERT_OK(Put(Key(7), rnd.RandomString(100)));
  ASSERT_OK(Put(Key(8), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);

  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::Read::AnyOffset", [&fname](void* pair_ptr) {
        auto p = static_cast<std::pair<std::string*, IOStatus*>*>(pair_ptr);
        if (p->first->find(fname) != std::string::npos) {
          *p->second = IOStatus::IOError();
          p->second->SetRetryable(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  iter.reset(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ(iter->key(), Key(8));
  // Note that for reverse scan, DBIter will need to ensure
  // the key it returns is the one with the highest sequence number.
  // To return key7, it internally calls MergingIterator::Prev()
  // until it reaches a previous user key.
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_NOK(iter->status());
  ASSERT_TRUE(iter->status().IsIOError());

  iter.reset();
}

TEST_F(DBRangeDelTest, ReleaseSnapshotAfterIteratorCreation) {
  // Test that range tombstone code path in LevelIterator
  // does access ReadOptions::snapshot after Iterator creation.
  //
  // Put some data in L2 so that range tombstone in L1 will not be dropped.
  ASSERT_OK(Put(Key(0), "v"));
  ASSERT_OK(Put(Key(100), "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  // two L1 file with range del
  ASSERT_OK(Put(Key(1), "v"));
  ASSERT_OK(Put(Key(2), "v"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(4)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_OK(Put(Key(5), "v"));
  ASSERT_OK(Put(Key(6), "v"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(5),
                             Key(6)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  const Snapshot* snapshot = db_->GetSnapshot();
  ReadOptions ro;
  ro.snapshot = snapshot;

  Iterator* iter = db_->NewIterator(ro);
  db_->ReleaseSnapshot(snapshot);

  iter->Seek(Key(1));
  std::vector<int> expected_keys{1, 2, 6, 100};
  for (int i : expected_keys) {
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), Key(i));
    iter->Next();
  }
  ASSERT_TRUE(!iter->Valid() && iter->status().ok());

  delete iter;
}

TEST_F(DBRangeDelTest, RefreshWithSnapshot) {
  ASSERT_OK(Put(Key(4), "4"));
  ASSERT_OK(Put(Key(6), "6"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(3),
                             Key(5)));

  std::unique_ptr<Iterator> iter{db_->NewIterator(ReadOptions())};
  // Live Memtable
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(6));
  ASSERT_OK(iter->Refresh(snapshot));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(4));
  // Immutable Memtable
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(iter->Refresh(nullptr));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(6));
  ASSERT_OK(iter->Refresh(snapshot));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(4));
  // L0
  ASSERT_OK(Flush());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_OK(iter->Refresh(nullptr));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(6));
  ASSERT_OK(iter->Refresh(snapshot));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(4));
  // L1
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));
  ASSERT_OK(iter->Refresh(nullptr));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(6));
  ASSERT_OK(iter->Refresh(snapshot));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(4));
  // L1 with two file.
  // Test that when LevelIterator enters a new file,
  // it remembers which snapshot sequence number to use.
  ASSERT_OK(Put(Key(2), "2"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  ASSERT_OK(iter->Refresh(nullptr));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  // LevelIterator is at the first file
  ASSERT_EQ(iter->key(), Key(2));
  ASSERT_OK(iter->Refresh(snapshot));
  // Will enter the second file, and create a new range tombstone iterator.
  // It should use the snapshot sequence number.
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), Key(4));
  iter.reset();
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBRangeDelTest, RowCache) {
  Options options = CurrentOptions();
  options.row_cache = NewLRUCache(8 << 10);
  DestroyAndReopen(options);
  ASSERT_OK(Put(Key(3), "val"));
  ASSERT_TRUE(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(3), Key(5))
                  .IsNotSupported());
  WriteBatch wb;
  ASSERT_OK(wb.Put(Key(6), "abc"));
  ASSERT_OK(wb.DeleteRange(Key(1), Key(5)));
  ASSERT_TRUE(db_->Write(WriteOptions(), &wb).IsNotSupported());
  ASSERT_EQ(Get(Key(3)), "val");
  // By default, memtable insertion failure will turn the DB to read-only mode.
  // The check for delete range should happen before that to fail early
  // and should not turn db into read-only mdoe.
  ASSERT_OK(Put(Key(5), "foo"));
}

TEST_F(DBRangeDelTest, SeekForPrevTest) {
  // open db
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compaction_style = kCompactionStyleUniversal;

  // add SST partitioner, split sst file with prefix length 2
  options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(2);
  Reopen(options);

  // File uses SST partitioner, so it will be split into 3 files
  // SST file 1: ka1, ka2
  // SST file 2: kb1
  // SST file 3: kc1, kc2
  // Delete range covers from ka2 to kc2, which means record ka2 and kb1, kc1
  // are covered by the delete range

  std::vector<std::pair<std::string, std::string>> kv = {{"ka1", "value_1"},
                                                         {"ka2", "value_2"},
                                                         {"kb1", "value_3"},
                                                         {"kc1", "value_4"},
                                                         {"kc2", "value_5"}};
  for (auto& p : kv) {
    ASSERT_OK(Put(p.first, p.second));
  }

  ASSERT_OK(Flush());
  // Compact to Lmax, it should have seq 0 now.
  ASSERT_OK(CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Open an iterator and create a snapshot, so that keys are not deleted
  // completely by delete range in SST
  ReadOptions read_opts;
  read_opts.snapshot = db_->GetSnapshot();
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
  iter->SeekToFirst();
  // iterate all the keys and validate the value
  for (int i = 0; iter->Valid(); iter->Next()) {
    ASSERT_EQ(kv[i].first, iter->key().ToString());
    ASSERT_EQ(kv[i].second, iter->value().ToString());
    i++;
  }

  // use delete range to delete the record
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "ka2",
                             "kc2"));
  // Flush
  ASSERT_OK(Flush());
  // Compact to Lmax
  ASSERT_OK(CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Close the iterator and release the snapshot.
  ASSERT_OK(iter->status());
  iter.reset();
  db_->ReleaseSnapshot(read_opts.snapshot);

  // create second iterator, seek each key and validate result
  std::unique_ptr<Iterator> iter2(db_->NewIterator(ReadOptions()));
  // Validate keys are deleted
  iter2->SeekToFirst();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ("ka1", iter2->key().ToString());
  iter2->Next();
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ("kc2", iter2->key().ToString());
  iter2->Next();
  ASSERT_FALSE(iter2->Valid());

  // Validate seek for prev result
  for (auto& p : kv) {
    iter2->SeekForPrev(p.first);
    ASSERT_TRUE(iter2->Valid());
    if (p.first == "kc2") {
      ASSERT_EQ("kc2", iter2->key().ToString());
    } else {
      ASSERT_EQ("ka1", iter2->key().ToString());
    }
  }
  ASSERT_OK(iter2->status());
  iter2.reset();
}

namespace {
// Head mark tests (see MemTableHeadMark in db/memtable.h). The queue keys are
// "q0000", "q0001", ... A consumer scan starts with Seek("q").
std::string QKey(int i) {
  char buf[16];
  snprintf(buf, sizeof(buf), "q%04d", i);
  return buf;
}

// A mark of the active memtable, with copies of its keys.
struct HeadMarkView {
  std::string start;
  std::string end;
  std::string usable_end;
  bool lowered = false;
  bool start_exclusive = false;
  bool operator==(const HeadMarkView& o) const {
    return start == o.start && end == o.end && usable_end == o.usable_end &&
           lowered == o.lowered && start_exclusive == o.start_exclusive;
  }
};

std::ostream& operator<<(std::ostream& os, const HeadMarkView& m) {
  return os << "[" << m.start << ", " << m.end
            << ") usable_end=" << m.usable_end << (m.lowered ? " lowered" : "")
            << (m.start_exclusive ? " start_exclusive" : "");
}

// Counts the seeks that a mark moved and records every publish result.
class HeadMarkTracker {
 public:
  HeadMarkTracker() {
    SyncPoint::GetInstance()->SetCallBack("MergingIterator::SeekOverHeadMark",
                                          [this](void*) { ++uses; });
    SyncPoint::GetInstance()->SetCallBack(
        "MergingIterator::SeekForPrevOverHeadMark",
        [this](void*) { ++reverse_uses; });
    SyncPoint::GetInstance()->SetCallBack(
        "DBIter::MaybePublishHeadMark:Result", [this](void* arg) {
          results.push_back(*static_cast<HeadMarkPublishResult*>(arg));
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }
  ~HeadMarkTracker() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
  void Reset() {
    uses = 0;
    reverse_uses = 0;
    results.clear();
  }
  int uses = 0;
  int reverse_uses = 0;
  std::vector<HeadMarkPublishResult> results;
};
}  // namespace

class DBHeadMarkTest : public DBRangeDelTest {
 public:
  Options HeadMarkOptions() {
    Options options = CurrentOptions();
    // A run of 4 tombstones is enough for a mark, so short tests make marks.
    options.min_tombstones_for_range_conversion = 4;
    options.disable_auto_compactions = true;
    options.write_buffer_size = 64 << 20;
    return options;
  }

  // Puts "q0000" .. "q<n-1>", then deletes the first `deleted` of them.
  void MakeQueue(int n, int deleted) {
    for (int i = 0; i < n; ++i) {
      ASSERT_OK(Put(QKey(i), "v"));
    }
    for (int i = 0; i < deleted; ++i) {
      ASSERT_OK(Delete(QKey(i)));
    }
  }

  std::vector<HeadMarkView> ActiveMarks(
      HeadMarkDirection dir = HeadMarkDirection::kForward) {
    auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
        db_->DefaultColumnFamily());
    std::vector<HeadMarkView> views;
    for (const MemTableHeadMark& m :
         cfh->cfd()->mem()->MemTable::TEST_HeadMarks(dir)) {
      views.push_back({m.start.ToString(), m.end.ToString(),
                       m.usable_end.ToString(), m.lowered, m.start_exclusive});
    }
    return views;
  }

  // The head mark bytes of the active memtable, and their budget.
  std::pair<size_t, size_t> HeadMarkBytesAndBudget() {
    const MemTable* mem = static_cast_with_check<ColumnFamilyHandleImpl>(
                              db_->DefaultColumnFamily())
                              ->cfd()
                              ->mem();
    return {mem->TEST_HeadMarkBytes(), mem->TEST_HeadMarkByteBudget()};
  }

  // Returns an iterator of the default column family that keeps the Version
  // from before an ingestion but reads at the ingested file's sequence number
  // S. The ingested file deletes `deleted`, which must overlap an SST file and
  // no memtable key, so the file gets a new sequence number and the memtable
  // is not flushed. An iterator that references its Version before it reads
  // the sequence number gets the current Version instead (see
  // DBImpl::SuperVersionMissesIngestion()). Only an in-place
  // Iterator::Refresh() can still pair the two: one IngestExternalFiles()
  // call into the column families "other" (handles_[0]) and default pauses
  // after it has published S and installed the first column family, while
  // the iterator refreshes in place. Requires CreateColumnFamilies({"other"}).
  // Do not scan before calling it: a converted range tombstone in the default
  // memtable makes Refresh() rebuild the tree, which waits for the DB mutex
  // that the paused ingestion holds, so the test would hang. Returns nullptr
  // after a test failure, for example if the ingestion fails before it
  // pauses; every wait is bounded, so the test fails instead of hanging. It
  // turns sync point processing off at the end, so create a HeadMarkTracker
  // after calling it.
  std::unique_ptr<Iterator> IteratorWithOldVersionAtNewSeq(
      const Options& options, const std::string& deleted) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    const std::string other_file = dbname_ + "/other.sst";
    const std::string file = dbname_ + "/ingest.sst";
    {
      SstFileWriter writer(EnvOptions(), options);
      EXPECT_OK(writer.Open(other_file));
      EXPECT_OK(writer.Put("x", "v"));
      EXPECT_OK(writer.Finish());
    }
    {
      SstFileWriter writer(EnvOptions(), options);
      EXPECT_OK(writer.Open(file));
      EXPECT_OK(writer.Delete(deleted));
      EXPECT_OK(writer.Finish());
    }
    // `paused` gets true when the ingestion pauses, or false if the ingestion
    // returns without pausing. `once` makes sure only one of them sets it.
    std::atomic<bool> once{false};
    std::promise<bool> paused;
    std::promise<void> refreshed;
    std::shared_future<void> refreshed_future = refreshed.get_future().share();
    constexpr char kPause[] =
        "DBImpl::IngestExternalFiles:InstallSVForFirstCF:0";
    SyncPoint::GetInstance()->SetCallBack(kPause, [&](void*) {
      if (!once.exchange(true)) {
        paused.set_value(true);
        refreshed_future.wait_for(std::chrono::minutes(5));
      }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    port::Thread ingester([&]() {
      IngestExternalFileArg other_arg;
      other_arg.column_family = handles_[0];
      other_arg.external_files = {other_file};
      IngestExternalFileArg arg;
      arg.column_family = db_->DefaultColumnFamily();
      arg.external_files = {file};
      EXPECT_OK(db_->IngestExternalFiles({other_arg, arg}));
      if (!once.exchange(true)) {
        paused.set_value(false);
      }
    });
    // This Defer releases and joins the ingester, even after a failure.
    bool released = false;
    Defer cleanup([&]() {
      if (!released) {
        refreshed.set_value();
      }
      ingester.join();
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearCallBack(kPause);
    });
    std::future<bool> paused_future = paused.get_future();
    if (paused_future.wait_for(std::chrono::minutes(5)) !=
            std::future_status::ready ||
        !paused_future.get()) {
      ADD_FAILURE() << "The ingestion did not pause";
      return nullptr;
    }
    // The ingestion holds the DB mutex here. The default column family still
    // has its old SuperVersion, so the refresh keeps the iterator tree and
    // takes the new sequence number without the mutex.
    EXPECT_OK(iter->Refresh());
    released = true;
    refreshed.set_value();
    return iter;
  }

  // Writes the layout of the DeleteFilesInRange() tests. An L2 file holds
  // "q0", and an L1 file holds the range tombstone ["q", "q1"), which hides
  // it. "a" and "z" keep the L2 file out of the range that DeleteL1File()
  // deletes. The memtable holds the queue of MakeQueue(11, 10). "q0" sorts
  // before "q0000", so the converted range tombstone, which starts at the
  // first point tombstone q0000, does not cover it, but a mark, which starts
  // at the seek key "q", does.
  void MakeKeyHiddenByL1File() {
    ASSERT_OK(Put("a", "va"));
    ASSERT_OK(Put("q0", "old"));
    ASSERT_OK(Put("z", "vz"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(2);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "q",
                               "q1"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(1);
    MakeQueue(/*n=*/11, /*deleted=*/10);
  }

  // Deletes the L1 file of MakeKeyHiddenByL1File(), which makes "q0" visible.
  void DeleteL1File() {
    const Slice begin("q");
    const Slice end("q1");
    ASSERT_OK(DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin,
                                 &end));
    ASSERT_EQ(1, NumTableFilesAtLevel(2));
    ASSERT_EQ(0, NumTableFilesAtLevel(1));
  }

  // Returns the last key at or before `target`, or "" if there is none.
  std::string SeekForPrevLast(const std::string& target,
                              const ReadOptions& ro = ReadOptions()) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekForPrev(target);
    EXPECT_OK(iter->status());
    return iter->Valid() ? iter->key().ToString() : "";
  }

  // Returns the last key below `upper_bound` with SeekToLast(), or "".
  std::string SeekToLastBelow(const std::string& upper_bound) {
    const Slice ub(upper_bound);
    ReadOptions ro;
    ro.iterate_upper_bound = &ub;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    EXPECT_OK(iter->status());
    return iter->Valid() ? iter->key().ToString() : "";
  }

  // Returns the first key at or after `target`, or "" if there is none.
  std::string SeekFirst(const std::string& target,
                        const ReadOptions& ro = ReadOptions()) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->Seek(target);
    EXPECT_OK(iter->status());
    return iter->Valid() ? iter->key().ToString() : "";
  }
};

TEST_F(DBHeadMarkTest, ScanPublishesMarkAndNextScanUsesIt) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  HeadMarkTracker tracker;

  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(10), false}}),
            ActiveMarks());
  ASSERT_EQ(0, tracker.uses);

  // The next scan seeks over the dead prefix with the mark and finds no run
  // to publish.
  tracker.Reset();
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(1, tracker.uses);
  ASSERT_TRUE(tracker.results.empty());
  // A seek key inside the mark uses it too.
  ASSERT_EQ(QKey(10), SeekFirst(QKey(3)));
  ASSERT_EQ(2, tracker.uses);
}

TEST_F(DBHeadMarkTest, RunInTheMiddleOfAScanMakesNoMark) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/0);
  for (int i = 1; i < 11; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  HeadMarkTracker tracker;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("q");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(0), iter->key());
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(11), iter->key());
  ASSERT_OK(iter->status());
  ASSERT_TRUE(tracker.results.empty());
  ASSERT_TRUE(ActiveMarks().empty());
}

TEST_F(DBHeadMarkTest, SeveralQueuesGetSeparateMarks) {
  DestroyAndReopen(HeadMarkOptions());
  for (const char* q : {"a", "b"}) {
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(Put(q + std::to_string(i), "v"));
    }
    for (int i = 0; i < 6; ++i) {
      ASSERT_OK(Delete(q + std::to_string(i)));
    }
  }
  ASSERT_EQ("a6", SeekFirst("a"));
  ASSERT_EQ("b6", SeekFirst("b"));
  ASSERT_EQ(std::vector<HeadMarkView>(
                {{"a", "a6", "a6", false}, {"b", "b6", "b6", false}}),
            ActiveMarks());
}

TEST_F(DBHeadMarkTest, ScanSentByMarkExtendsIt) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/30, /*deleted=*/10);
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  HeadMarkTracker tracker;

  // Two more deletions are too few for an extension.
  ASSERT_OK(Delete(QKey(10)));
  ASSERT_OK(Delete(QKey(11)));
  ASSERT_EQ(QKey(12), SeekFirst("q"));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kRefusedShort},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(10), false}}),
            ActiveMarks());

  // Four tombstones after the mark are enough.
  tracker.Reset();
  ASSERT_OK(Delete(QKey(12)));
  ASSERT_OK(Delete(QKey(13)));
  ASSERT_EQ(QKey(14), SeekFirst("q"));
  ASSERT_EQ(1, tracker.uses);
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kExtended},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(14), QKey(14), false}}),
            ActiveMarks());
}

TEST_F(DBHeadMarkTest, LatePutLowersMarkAndDeleteRepairsIt) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  ASSERT_EQ(QKey(10), SeekFirst("q"));

  // A late item inside the dead prefix lowers the mark, so scans find it.
  ASSERT_OK(Put(QKey(5), "late"));
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(5), QKey(5), true}}),
            ActiveMarks());
  ASSERT_EQ(QKey(5), SeekFirst("q"));
  // A deletion inside the mark does not change it.
  ASSERT_OK(Delete(QKey(3)));
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(5), QKey(5), true}}),
            ActiveMarks());

  // After the consumer deletes the late item, the next scan sees one
  // tombstone, and the rest of the old prefix is covered by the converted
  // range tombstone. One tombstone is enough to extend a lowered mark.
  HeadMarkTracker tracker;
  ASSERT_OK(Delete(QKey(5)));
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kExtended},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(10), false}}),
            ActiveMarks());
}

TEST_F(DBHeadMarkTest, WriterDuringValidationKeepsItsLoweredMark) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  // A late put lands after the claim is stored and before the validation
  // walk. The writer must see the claim and lower it.
  bool put_done = false;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::PublishHeadMark:BeforeValidate", [&](void*) {
        if (!put_done) {
          put_done = true;
          ASSERT_OK(Put(QKey(5), "late"));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(put_done);
  // The scan did not raise usable_end over the writer's mark.
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(5), "q", true}}),
            ActiveMarks());
  ASSERT_EQ(QKey(5), SeekFirst("q"));

  // The claim moves no seek, so no scan can extend it, and its publisher gave
  // up. It must not block a new claim over its range. The consumer deletes
  // the late item and 4 more, and the next scan counts 5 tombstones ("q0000"
  // to "q0009" are under the converted range tombstone) and publishes a new
  // mark that seeks use.
  ASSERT_OK(Delete(QKey(5)));
  for (int i = 10; i < 14; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(14), SeekFirst("q"));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(14), QKey(14), false}}),
            ActiveMarks());
  tracker.Reset();
  ASSERT_EQ(QKey(14), SeekFirst("q"));
  ASSERT_EQ(1, tracker.uses);
}

TEST_F(DBHeadMarkTest, ValidationStopsAtNewerLiveEntry) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  // The scan reads at a snapshot. A put inside the run after the snapshot is
  // invisible to the scan, so the scan claims it, and the walk must stop at
  // it.
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(Put(QKey(5), "late"));
  ReadOptions ro;
  ro.snapshot = snap;
  ASSERT_EQ(QKey(10), SeekFirst("q", ro));
  db_->ReleaseSnapshot(snap);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(5), false}}),
            ActiveMarks());
  ASSERT_EQ(QKey(5), SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, RepeatedSeekWithoutProgressAllocatesNothing) {
  DestroyAndReopen(HeadMarkOptions());
  // An SST, so that every scan builds the merging iterator.
  ASSERT_OK(Put("z", "v"));
  ASSERT_OK(Flush());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(Put(QKey(5), "late"));
  ReadOptions ro;
  ro.snapshot = snap;
  // One iterator for all seeks. It keeps the range tombstones of the moment it
  // was created (none), so each seek counts the same deletions again.
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->Seek("q");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(10), iter->key());
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(5), false}}),
            ActiveMarks());

  // Each seek uses the mark, runs to "q0010" and asks to extend it. The claim
  // already reaches "q0010", and the walk stops at "q0005" again, so nothing
  // changes and the memtable allocates nothing.
  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  const size_t mem_before = cfh->cfd()->mem()->ApproximateMemoryUsage();
  HeadMarkTracker tracker;
  for (int i = 0; i < 100; ++i) {
    iter->Seek("q");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(QKey(10), iter->key());
  }
  ASSERT_EQ(100, tracker.uses);
  ASSERT_EQ(mem_before, cfh->cfd()->mem()->ApproximateMemoryUsage());
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(5), false}}),
            ActiveMarks());
  ASSERT_OK(iter->status());
  iter.reset();
  db_->ReleaseSnapshot(snap);
  // A newer reader sees the put.
  ASSERT_EQ(QKey(5), SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, ExtensionInsideClaimChecksOnlyItsRun) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/30, /*deleted=*/20);
  // Two late puts after the snapshot. The snapshot scan claims ["q",
  // "q0020"), and its walk stops at the first of them, "q0005".
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(Put(QKey(15), "late"));
  ASSERT_OK(Put(QKey(5), "late"));
  ReadOptions ro;
  ro.snapshot = snap;
  ASSERT_EQ(QKey(20), SeekFirst("q", ro));
  db_->ReleaseSnapshot(snap);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(20), QKey(5), false}}),
            ActiveMarks());

  // A newer scan is sent by the mark to "q0005", sees "q0005" to "q0014"
  // deleted and stops at the live "q0015". Its run ends inside the claim, so
  // the mark may become usable only up to "q0015", never past it. The
  // snapshot scan converted ["q0000", "q0020") at its sequence number, and a
  // scan does not count covered tombstones, so "q0006" to "q0014" are deleted
  // again above it to make the run long enough.
  for (int i = 5; i < 15; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  ASSERT_EQ(QKey(15), SeekFirst("q"));
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(20), QKey(15), false}}),
            ActiveMarks());
  ASSERT_EQ(QKey(15), SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, ValidationWalkCapKeepsCheckedPart) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  // Before the walk, add more than kMaxHeadMarkWalk (16384) newer
  // deletions inside the claim, and a newer put outside it so that the walk
  // runs. The walk visits the two entries of "q0000" (the put and its
  // deletion) and then the new deletions, and stops at the 16385th entry,
  // which is new deletion 16382 (counting from 0).
  constexpr int kNewDeletions = 17000;
  auto key_between = [](int i) {
    char buf[32];
    snprintf(buf, sizeof(buf), "q0000_%05d", i);
    return std::string(buf);
  };
  bool written = false;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::PublishHeadMark:BeforeValidate", [&](void*) {
        if (written) {
          return;
        }
        written = true;
        WriteBatch batch;
        for (int i = 0; i < kNewDeletions; ++i) {
          ASSERT_OK(batch.Delete(key_between(i)));
        }
        ASSERT_OK(batch.Put("z", "v"));
        ASSERT_OK(db_->Write(WriteOptions(), &batch));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(written);
  ASSERT_EQ(
      std::vector<HeadMarkView>({{"q", QKey(10), key_between(16382), false}}),
      ActiveMarks());
  ASSERT_EQ(QKey(10), SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, OlderSnapshotDoesNotUseMark) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  const Snapshot* snap = db_->GetSnapshot();
  // A newer write, so the mark below gets a larger seq than the snapshot.
  ASSERT_OK(Put("z", "v"));
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(1, ActiveMarks().size());

  HeadMarkTracker tracker;
  ReadOptions ro;
  ro.snapshot = snap;
  ASSERT_EQ(QKey(10), SeekFirst("q", ro));
  ASSERT_EQ(0, tracker.uses);
  db_->ReleaseSnapshot(snap);
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(1, tracker.uses);
}

TEST_F(DBHeadMarkTest, ExcludedReadersDoNotUseMarks) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(1, ActiveMarks().size());

  HeadMarkTracker tracker;
  ReadOptions ignore_range_deletions;
  ignore_range_deletions.ignore_range_deletions = true;
  ASSERT_EQ(QKey(10), SeekFirst("q", ignore_range_deletions));
  // NewIterator() rejects table_filter while conversion is on, so turn
  // conversion off first. The memtable keeps its marks.
  ASSERT_OK(
      dbfull()->SetOptions({{"min_tombstones_for_range_conversion", "0"}}));
  ASSERT_EQ(1, ActiveMarks().size());
  const std::function<bool(const TableProperties&)> keep_all =
      [](const TableProperties&) { return true; };
  ReadOptions table_filter;
  table_filter.table_filter = &keep_all;
  ASSERT_EQ(QKey(10), SeekFirst("q", table_filter));
  ASSERT_EQ(0, tracker.uses);
}

TEST_F(DBHeadMarkTest, IncompleteScanMakesNoMark) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  HeadMarkTracker tracker;
  ReadOptions ro;
  ro.max_skippable_internal_keys = 3;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->Seek("q");
  ASSERT_TRUE(iter->status().IsIncomplete());
  ASSERT_TRUE(tracker.results.empty());
  ASSERT_TRUE(ActiveMarks().empty());
}

TEST_F(DBHeadMarkTest, OverlappingRunIsNotPublished) {
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  // A run from "p" runs into the mark of "q", so it is not published.
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Delete("p" + std::to_string(i)));
  }
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(10), SeekFirst("p"));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{
          HeadMarkPublishResult::kRefusedOverlap},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(10), false}}),
            ActiveMarks());
}

TEST_F(DBHeadMarkTest, FullTableDropsNewMarkUntilACellEmpties) {
  DestroyAndReopen(HeadMarkOptions());
  // 18 queues "a".."r"; each has 5 dead items and one live item.
  auto queue = [](int q) { return std::string(1, static_cast<char>('a' + q)); };
  for (int q = 0; q < 18; ++q) {
    for (int i = 0; i < 6; ++i) {
      ASSERT_OK(Put(queue(q) + std::to_string(i), "v"));
    }
    for (int i = 0; i < 5; ++i) {
      ASSERT_OK(Delete(queue(q) + std::to_string(i)));
    }
  }
  HeadMarkTracker tracker;
  for (int q = 0; q < 17; ++q) {
    ASSERT_EQ(queue(q) + "5", SeekFirst(queue(q)));
  }
  ASSERT_EQ(16, ActiveMarks().size());
  ASSERT_EQ(HeadMarkPublishResult::kRefusedFull, tracker.results.back());

  // A put at the start of queue "a" lowers its mark to nothing, which frees
  // its cell for the next new mark. Queue "q" cannot retry yet: its first
  // scan converted its run, and DBIter does not count covered tombstones, so
  // queue "q" needs more deletions first. The new mark goes to queue "r".
  ASSERT_OK(Put("a", "v"));
  tracker.Reset();
  ASSERT_EQ("r5", SeekFirst("r"));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
      tracker.results);
  const std::vector<HeadMarkView> marks = ActiveMarks();
  ASSERT_EQ(16, marks.size());
  ASSERT_EQ("b", marks.front().start);
  ASSERT_EQ("r", marks.back().start);
}

TEST_F(DBHeadMarkTest, ImmutableMemtableMarkIsUsed) {
  Options options = HeadMarkOptions();
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  DestroyAndReopen(options);
  MakeQueue(/*n=*/20, /*deleted=*/10);
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_TRUE(ActiveMarks().empty());

  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(1, tracker.uses);
  // A late put into the new memtable is found: marks only cover their own
  // memtable and older levels.
  ASSERT_OK(Put(QKey(5), "late"));
  ASSERT_EQ(QKey(5), SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, NoPublishIntoImmutableMemtable) {
  // The iterator's active memtable becomes immutable before the scan. The
  // memtable list has already counted that memtable's memory, so the scan
  // must not publish into it.
  Options options = HeadMarkOptions();
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  DestroyAndReopen(options);
  MakeQueue(/*n=*/20, /*deleted=*/10);
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  HeadMarkTracker tracker;
  iter->Seek("q");
  ASSERT_OK(iter->status());
  ASSERT_EQ(QKey(10), iter->key());
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{
          HeadMarkPublishResult::kRefusedImmutable},
      tracker.results);
}

TEST_F(DBHeadMarkTest, MarkEndInNextPrefix) {
  Options options = PrefixFilterOptions(HeadMarkOptions());
  DestroyAndReopen(options);
  for (int i = 0; i < 6; ++i) {
    ASSERT_OK(Delete("qqqq" + std::to_string(i)));
  }
  ASSERT_OK(Put("rrrr0", "v"));
  // A total order scan publishes a mark that ends in the next prefix.
  ReadOptions total_order;
  total_order.total_order_seek = true;
  ASSERT_EQ("rrrr0", SeekFirst("qqqq", total_order));
  ASSERT_EQ(std::vector<HeadMarkView>({{"qqqq", "rrrr0", "rrrr0", false}}),
            ActiveMarks());
  // A prefix_same_as_start scan uses the mark and still stops at the end of
  // its prefix.
  HeadMarkTracker tracker;
  ReadOptions same_prefix;
  same_prefix.prefix_same_as_start = true;
  ASSERT_EQ("", SeekFirst("qqqq", same_prefix));
  ASSERT_EQ(1, tracker.uses);
  ASSERT_EQ("rrrr0", SeekFirst("rrrr", same_prefix));
}

TEST_F(DBHeadMarkTest, IngestionClearsMarks) {
  // The ingested file holds "q", which is inside the mark ["q", "q0010") but
  // not in the memtable's key range, so the ingestion does not flush.
  enum class Mode { kNewSeqno, kSeqnoZero, kIngestBehind };
  for (Mode mode : {Mode::kNewSeqno, Mode::kSeqnoZero, Mode::kIngestBehind}) {
    SCOPED_TRACE(static_cast<int>(mode));
    Options options = HeadMarkOptions();
    options.cf_allow_ingest_behind = mode == Mode::kIngestBehind;
    DestroyAndReopen(options);
    MakeQueue(/*n=*/20, /*deleted=*/10);
    ASSERT_EQ(QKey(10), SeekFirst("q"));
    ASSERT_EQ(1, ActiveMarks().size());

    const std::string file = dbname_ + "/ingest.sst";
    {
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(file));
      ASSERT_OK(writer.Put("q", "ingested"));
      ASSERT_OK(writer.Finish());
    }
    // A snapshot makes the ingestion assign a new seqno. Without it, the file
    // goes to the last level with seqno 0.
    const Snapshot* snap =
        mode == Mode::kNewSeqno ? db_->GetSnapshot() : nullptr;
    IngestExternalFileOptions ifo;
    ifo.allow_global_seqno = mode == Mode::kNewSeqno;
    ifo.ingest_behind = mode == Mode::kIngestBehind;
    ASSERT_OK(db_->IngestExternalFile({file}, ifo));
    if (snap != nullptr) {
      db_->ReleaseSnapshot(snap);
    }
    ASSERT_TRUE(ActiveMarks().empty());
    ASSERT_EQ("q", SeekFirst("q"));
  }
}

TEST_F(DBHeadMarkTest, IngestionRefusesStalePublishers) {
  Options options = HeadMarkOptions();
  DestroyAndReopen(options);
  MakeQueue(/*n=*/20, /*deleted=*/10);
  const std::string file = dbname_ + "/ingest.sst";
  {
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(file));
    ASSERT_OK(writer.Put("q", "ingested"));
    ASSERT_OK(writer.Finish());
  }
  // This iterator reads the Version before the ingestion.
  std::unique_ptr<Iterator> stale(db_->NewIterator(ReadOptions()));
  // This snapshot is older than the ingested file's seqno.
  const Snapshot* snap = db_->GetSnapshot();
  IngestExternalFileOptions ifo;
  ASSERT_OK(db_->IngestExternalFile({file}, ifo));

  HeadMarkTracker tracker;
  stale->Seek("q");
  ASSERT_OK(stale->status());
  ASSERT_EQ(QKey(10), stale->key());
  ReadOptions ro;
  ro.snapshot = snap;
  ASSERT_EQ(QKey(10), SeekFirst("q", ro));
  db_->ReleaseSnapshot(snap);
  ASSERT_EQ(std::vector<HeadMarkPublishResult>(
                {HeadMarkPublishResult::kRefusedIngest,
                 HeadMarkPublishResult::kRefusedIngest}),
            tracker.results);
  ASSERT_TRUE(ActiveMarks().empty());
  ASSERT_EQ("q", SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, IngestionDuringRaiseLeavesMarksCleared) {
  Options options = HeadMarkOptions();
  DestroyAndReopen(options);
  MakeQueue(/*n=*/20, /*deleted=*/10);
  const std::string file = dbname_ + "/ingest.sst";
  {
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(file));
    ASSERT_OK(writer.Put("q", "ingested"));
    ASSERT_OK(writer.Finish());
  }
  // The scan has allocated its raised mark but not yet taken the mark mutex.
  // The ingestion clears the table now, so the raise must find no cell. The
  // callback runs while the scan holds the memtable's immutable_mutex_ as a
  // reader. "q" sorts before every memtable key, so the ingestion does not
  // flush the memtable; a flush would wait for that lock and hang the test.
  bool ingested = false;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::RaiseHeadMarkUsableEnd:Allocated", [&](void*) {
        if (ingested) {
          return;
        }
        ingested = true;
        ASSERT_OK(db_->IngestExternalFile({file}, IngestExternalFileOptions()));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(ingested);
  ASSERT_TRUE(ActiveMarks().empty());
  ASSERT_EQ("q", SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, HeadMarkDoesNotChangePinnedVersionAfterIngestion) {
  // Iterator A keeps the Version from before an ingestion and reads at the
  // ingested file's sequence number S (see IteratorWithOldVersionAtNewSeq()).
  // A must not publish a mark from that view, and a mark that a newer scan
  // publishes at S must not move A's seeks.
  Options options = HeadMarkOptions();
  DestroyAndReopen(options);
  CreateColumnFamilies({"other"}, options);
  // An SST file holds the live key q0005. The memtable deletes the keys
  // around it.
  ASSERT_OK(Put(QKey(5), "old"));
  ASSERT_OK(Flush());
  for (int i = 0; i < 10; ++i) {
    if (i != 5) {
      ASSERT_OK(Delete(QKey(i)));
    }
  }
  ASSERT_OK(Put(QKey(10), "v"));
  std::unique_ptr<Iterator> a =
      IteratorWithOldVersionAtNewSeq(options, /*deleted=*/QKey(5));
  ASSERT_NE(nullptr, a);

  HeadMarkTracker tracker;
  // A still sees q0005, and its publish is refused.
  a->Seek("q");
  ASSERT_OK(a->status());
  ASSERT_TRUE(a->Valid());
  ASSERT_EQ(QKey(5), a->key().ToString());
  ASSERT_EQ(std::vector<HeadMarkPublishResult>(
                {HeadMarkPublishResult::kRefusedIngest}),
            tracker.results);
  ASSERT_TRUE(ActiveMarks().empty());

  // A new scan sees the ingested delete and publishes ["q", q0010).
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(10), false}}),
            ActiveMarks());

  // A must not use that mark: it would move the seek in the SST file past
  // q0005.
  tracker.Reset();
  a->Seek("q");
  ASSERT_OK(a->status());
  ASSERT_TRUE(a->Valid());
  ASSERT_EQ(QKey(5), a->key().ToString());
  ASSERT_EQ(0, tracker.uses);
  // A new scan still uses the mark.
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(1, tracker.uses);
}

TEST_F(DBHeadMarkTest, RepeatedIngestAndSeekBoundsHeadMarkMemory) {
  // Every ingestion clears the marks, and the next scan publishes the same
  // mark again. The memtable arena keeps the old marks until flush. No write
  // reaches the memtable, so nothing makes it flush. The head mark byte budget
  // stops the publishes; reads and conversion keep working.
  Options options = HeadMarkOptions();
  // One tombstone is a run, so the run of the single delete below makes a
  // mark on every scan. The converted range tombstone covers only older
  // entries, so the delete stays visible to the same scan again.
  options.min_tombstones_for_range_conversion = 1;
  // A smaller write buffer gives smaller arena blocks, so the test can see
  // the memory growth.
  options.write_buffer_size = 4 << 20;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  // A long live key makes each mark copy its end key, which is large.
  const std::string live = "c" + std::string(4000, 'x');
  ASSERT_OK(Put(live, "v"));
  ASSERT_OK(Delete("b"));
  ASSERT_EQ(live, SeekFirst("a"));
  ASSERT_EQ(1, options.statistics->getTickerCount(
                   READ_PATH_RANGE_TOMBSTONES_INSERTED));
  // The memtable property "rocksdb.cur-size-active-mem-table" is updated only
  // by writes, so read the arena usage directly.
  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd();
  const size_t mem_before = cfd->mem()->ApproximateMemoryUsage();

  HeadMarkTracker tracker;
  constexpr int kRounds = 300;
  for (int i = 0; i < kRounds; ++i) {
    // Each file has its own key after the memtable keys, so the ingestion
    // neither flushes the memtable nor overlaps an earlier file.
    char key[16];
    snprintf(key, sizeof(key), "z%04d", i);
    const std::string file = dbname_ + "/ingest" + std::to_string(i) + ".sst";
    {
      SstFileWriter writer(EnvOptions(), options);
      ASSERT_OK(writer.Open(file));
      ASSERT_OK(writer.Put(key, "ingested"));
      ASSERT_OK(writer.Finish());
    }
    ASSERT_OK(db_->IngestExternalFile({file}, IngestExternalFileOptions()));
    ASSERT_EQ(live, SeekFirst("a"));
  }
  // The first publishes succeed, the later ones run out of budget.
  ASSERT_EQ(HeadMarkPublishResult::kPublished, tracker.results.front());
  ASSERT_EQ(HeadMarkPublishResult::kRefusedBudget, tracker.results.back());
  // Without the budget, the memtable grows by more than 2 MB here.
  ASSERT_LT(cfd->mem()->ApproximateMemoryUsage(), mem_before + (1 << 20));
  // The converted range tombstone already covers the run, so no scan
  // converts again.
  ASSERT_EQ(1, options.statistics->getTickerCount(
                   READ_PATH_RANGE_TOMBSTONES_INSERTED));
  // The memtable was never flushed, which would have made an L0 file.
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
}

TEST_F(DBHeadMarkTest, SeekToEmptyKeyPublishesMark) {
  // The empty key is a valid seek target, so a mark can start at it. The copy
  // of the empty start has no bytes.
  DestroyAndReopen(HeadMarkOptions());
  MakeQueue(/*n=*/20, /*deleted=*/10);
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(10), SeekFirst(""));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{"", QKey(10), QKey(10), false}}),
            ActiveMarks());
  tracker.Reset();
  ASSERT_EQ(QKey(10), SeekFirst(""));
  ASSERT_EQ(1, tracker.uses);
}

TEST_F(DBHeadMarkTest, HeadMarkBytesStayWithinBudget) {
  // A publish or a raise reserves the bytes that it allocates before it
  // allocates them, and is refused if they do not fit in the budget (see
  // MemTable::ReserveHeadMarkBytes()). The budget in these cases is
  // kHeadMarkBaseBudget (64 KB) plus the bytes of the memtable's entries.
  const std::string kLongPart(100 << 10, ' ');
  for (bool long_end : {false, true}) {
    SCOPED_TRACE(long_end ? "long end" : "long start");
    DestroyAndReopen(HeadMarkOptions());
    MakeQueue(/*n=*/10, /*deleted=*/10);
    // The first live key, which is the end of the claim.
    const std::string live = long_end ? QKey(10) + kLongPart : QKey(10);
    ASSERT_OK(Put(live, "v"));
    HeadMarkTracker tracker;
    if (long_end) {
      // The new claim copies `live` once, which fits: the memtable's entries
      // include `live`. The raise after the validation walk needs a second
      // copy of `live`, which does not fit, so nothing becomes usable.
      ASSERT_EQ(live, SeekFirst("q"));
      ASSERT_EQ(
          std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
          tracker.results);
      ASSERT_EQ(std::vector<HeadMarkView>({{"q", live, "q", false}}),
                ActiveMarks());
    } else {
      // A seek target longer than the whole budget. ' ' sorts before '0', so
      // the target sorts before q0000. The new claim would copy the target,
      // so the publish is refused and stores nothing.
      ASSERT_EQ(live, SeekFirst("q" + kLongPart));
      ASSERT_EQ(
          std::vector<HeadMarkPublishResult>{
              HeadMarkPublishResult::kRefusedBudget},
          tracker.results);
      ASSERT_TRUE(ActiveMarks().empty());
      ASSERT_EQ(0, HeadMarkBytesAndBudget().first);
    }
    const std::pair<size_t, size_t> bytes = HeadMarkBytesAndBudget();
    ASSERT_LE(bytes.first, bytes.second);
  }
}

TEST_F(DBHeadMarkTest, DeleteFilesInRangeClearsMarks) {
  // DeleteFilesInRange() can remove an SST file whose range tombstone hides an
  // older key, so the key becomes visible again with no new sequence number.
  // A mark that a scan published while the key was hidden must not skip it.
  DestroyAndReopen(HeadMarkOptions());
  MakeKeyHiddenByL1File();
  {
    HeadMarkTracker tracker;
    ASSERT_EQ(QKey(10), SeekFirst("q"));
    ASSERT_EQ(
        std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
        tracker.results);
  }
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(10), false}}),
            ActiveMarks());

  DeleteL1File();
  ASSERT_EQ("old", Get("q0"));
  ASSERT_TRUE(ActiveMarks().empty());
  // A new scan sees "q0" and stops there.
  ASSERT_EQ("q0", SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, DeleteFilesInRangeRefusesOlderVersion) {
  // An iterator created before DeleteFilesInRange() keeps the Version that
  // has the deleted file, so it does not see "q0". Its publish is refused,
  // because its mark would make newer readers skip "q0".
  DestroyAndReopen(HeadMarkOptions());
  MakeKeyHiddenByL1File();
  std::unique_ptr<Iterator> old_iter(db_->NewIterator(ReadOptions()));
  DeleteL1File();
  {
    HeadMarkTracker tracker;
    old_iter->Seek("q");
    ASSERT_OK(old_iter->status());
    ASSERT_TRUE(old_iter->Valid());
    ASSERT_EQ(QKey(10), old_iter->key().ToString());
    ASSERT_EQ(
        std::vector<HeadMarkPublishResult>{
            HeadMarkPublishResult::kRefusedIngest},
        tracker.results);
  }
  ASSERT_TRUE(ActiveMarks().empty());
  ASSERT_EQ("q0", SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, DeleteFilesInRangeDoesNotWaitForLowering) {
  // DeleteFilesInRange() clears the marks while it holds the DB mutex. It does
  // not stop the writers, so a writer can be lowering a mark at the same time,
  // and a lowering allocates its new mark while it holds head_mark_mu_. The
  // clear must not wait for that writer.
  DestroyAndReopen(HeadMarkOptions());
  MakeKeyHiddenByL1File();
  ASSERT_EQ(QKey(10), SeekFirst("q"));
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(10), QKey(10), false}}),
            ActiveMarks());

  // The writer pauses inside its lowering, which holds head_mark_mu_, until
  // the test releases it. Every wait is bounded, so a hang fails the test.
  std::promise<void> lowering;
  std::promise<void> release;
  std::shared_future<void> release_future = release.get_future().share();
  std::atomic<bool> once{false};
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::LowerHeadMarks:Lower", [&](void*) {
        if (!once.exchange(true)) {
          lowering.set_value();
          release_future.wait_for(std::chrono::minutes(1));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer([&]() { EXPECT_OK(Put(QKey(5), "v2")); });
  bool released = false;
  Defer cleanup([&]() {
    if (!released) {
      release.set_value();
    }
    if (writer.joinable()) {
      writer.join();
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_EQ(std::future_status::ready,
            lowering.get_future().wait_for(std::chrono::minutes(1)));

  std::future<Status> deleted = std::async(std::launch::async, [&]() {
    const Slice begin("q");
    const Slice end("q1");
    return DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin,
                              &end);
  });
  // The deletion finishes while the writer still holds head_mark_mu_.
  const std::future_status deleted_status =
      deleted.wait_for(std::chrono::seconds(10));
  released = true;
  release.set_value();
  writer.join();
  ASSERT_EQ(std::future_status::ready, deleted_status);
  ASSERT_OK(deleted.get());
  ASSERT_EQ(0, NumTableFilesAtLevel(1));

  // The writer lowered the mark in the cleared table, which no reader sees.
  ASSERT_TRUE(ActiveMarks().empty());
  ASSERT_EQ("old", Get("q0"));
  ASSERT_EQ("v2", Get(QKey(5)));
  ASSERT_EQ("q0", SeekFirst("q"));
}

TEST_F(DBHeadMarkTest, FIFOCompactionPublishesNoMark) {
  // FIFO compaction deletes old SST files without ingest_sst_lock, so marks
  // are off for it. Ordinary conversion still runs.
  Options options = HeadMarkOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  options.max_open_files = -1;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  MakeQueue(/*n=*/11, /*deleted=*/10);
  {
    HeadMarkTracker tracker;
    ASSERT_EQ(QKey(10), SeekFirst("q"));
    // The scan does not try to publish.
    ASSERT_TRUE(tracker.results.empty());
  }
  ASSERT_TRUE(ActiveMarks().empty());
  ASSERT_EQ(1, options.statistics->getTickerCount(
                   READ_PATH_RANGE_TOMBSTONES_INSERTED));
}

TEST_F(DBHeadMarkTest, ConversionDoesNotCoverKeysThatAMarkSkipped) {
  // A mark of an immutable memtable moves the seek of that memtable and of
  // the older levels, while the active memtable is sought at the target. A
  // run that starts at a tombstone of the active memtable inside the mark's
  // usable range must not convert over the keys that only the mark skipped.
  // The test makes the mark wrong with the known compaction filter limitation
  // (see the Limits paragraph of MemTableHeadMark), so a conversion over a
  // skipped key would delete a key that Get() returns.
  class SkipFilter : public CompactionFilter {
   public:
    Decision FilterV2(int /*level*/, const Slice& key, ValueType /*type*/,
                      const Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      std::string* skip_until) const override {
      if (enabled && key == "p") {
        *skip_until = QKey(16);
        return Decision::kRemoveAndSkipUntil;
      }
      return Decision::kKeep;
    }
    const char* Name() const override { return "SkipFilter"; }
    // MoveFilesToLevel() also compacts, so the filter skips only when the
    // test enables it.
    std::atomic<bool> enabled{false};
  };
  SkipFilter filter;
  Options options = HeadMarkOptions();
  options.compaction_filter = &filter;
  // Keep the first memtable in memory after the switch below.
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  DestroyAndReopen(options);
  ASSERT_OK(Put(QKey(15), "old"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  // An L0 file deletes q0015. Its "p" makes the filter skip that delete.
  ASSERT_OK(Put("p", "x"));
  ASSERT_OK(Delete(QKey(15)));
  ASSERT_OK(Flush());
  for (int i : {0, 1, 2, 3, 4, 5, 6, 20, 21, 22, 23}) {
    ASSERT_OK(Put(QKey(i), "v"));
  }
  for (int i = 0; i <= 5; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  ASSERT_EQ(QKey(6), SeekFirst("q"));
  // A late put lowers the mark. The next scan, sent by the lowered mark, sees
  // a run of 3 deletes (q0005, q0006 and the L0 delete of q0015), which is
  // below the threshold of 4. It extends the mark to q0020 and converts
  // nothing, so no range tombstone covers q0015.
  ASSERT_OK(Put(QKey(5), "late"));
  ASSERT_OK(Delete(QKey(5)));
  ASSERT_OK(Delete(QKey(6)));
  ASSERT_EQ(QKey(20), SeekFirst("q"));
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(20), QKey(20), false}}),
            ActiveMarks());

  // The filter drops the delete of q0015, so Get() returns the old value
  // again, but the mark still skips q0015 (the known limitation).
  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(1, meta.levels[0].files.size());
  filter.enabled = true;
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {meta.levels[0].files[0].name}, 1));
  filter.enabled = false;
  ASSERT_EQ("old", Get(QKey(15)));
  ASSERT_EQ(QKey(20), SeekFirst("q"));

  // After a memtable switch, the mark is in an immutable memtable. The
  // active memtable gets a deleted q0003 and deletes of q0020-q0022. The next
  // scan starts its run at q0003, but the mark moved the older levels to
  // q0020, so the conversion must start at q0020.
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put(QKey(3), "late"));
  ASSERT_OK(Delete(QKey(3)));
  for (int i = 20; i <= 22; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  {
    HeadMarkTracker tracker;
    ASSERT_EQ(QKey(23), SeekFirst("q"));
    ASSERT_EQ(1, tracker.uses);
  }
  ASSERT_EQ("old", Get(QKey(15)));
  ASSERT_OK(Flush());
  Reopen(options);
  ASSERT_EQ("old", Get(QKey(15)));
}

TEST_F(DBHeadMarkTest, ConversionLimitAppliesOnlyInSeekDirection) {
  // An immutable memtable's mark moves a Seek() to q0050 in that memtable and
  // the older levels. The active memtable holds the live keys q0040 and q0060
  // and deletes q0045-q0055, from the smallest up. The seek lands at q0040.
  // Next() crosses the run, but the scan did not read the older levels below
  // q0050, so it converts only [q0050, q0060). Prev() switches direction,
  // which seeks every level again from q0060, so the run read backward
  // converts all of [q0045, q0060). Its first tombstone q0055 has the scan's
  // own seqno, so the range from Next() does not cover it.
  Options options = HeadMarkOptions();
  // Keep the first memtable in memory after the switch below.
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  DestroyAndReopen(options);
  for (int i = 10; i < 20; ++i) {
    ASSERT_OK(Put(QKey(i), "v"));
  }
  for (int i = 10; i < 20; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  ASSERT_OK(Put(QKey(50), "v"));
  ASSERT_EQ(QKey(50), SeekFirst("q"));
  ASSERT_EQ(std::vector<HeadMarkView>({{"q", QKey(50), QKey(50), false}}),
            ActiveMarks());
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put(QKey(40), "v"));
  ASSERT_OK(Put(QKey(60), "v"));
  for (int i = 45; i <= 55; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }

  HeadMarkTracker tracker;
  std::vector<std::pair<std::string, std::string>> ranges;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::AddLogicallyRedundantRangeTombstone:AddRange", [&](void* arg) {
        const auto* range = static_cast<std::pair<Slice, Slice>*>(arg);
        ranges.emplace_back(range->first.ToString(), range->second.ToString());
      });
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("q");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(40), iter->key().ToString());
  ASSERT_EQ(1, tracker.uses);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(60), iter->key().ToString());
  std::vector<std::pair<std::string, std::string>> expected = {
      {QKey(50), QKey(60)}};
  ASSERT_EQ(expected, ranges);
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(40), iter->key().ToString());
  ASSERT_OK(iter->status());
  expected.emplace_back(QKey(45), QKey(60));
  ASSERT_EQ(expected, ranges);
}

TEST_F(DBHeadMarkTest, IngestionClearsMarksOnlyIfEnabled) {
  // With conversion off, no memtable has head marks, so the ingestion does
  // not visit the memtables. After the option is turned on, the next active
  // memtable has marks enabled, and every later ingestion clears the marks.
  Options options = HeadMarkOptions();
  options.min_tombstones_for_range_conversion = 0;
  DestroyAndReopen(options);
  int clears = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ColumnFamilyData::ClearHeadMarks:Clear", [&](void*) { ++clears; });
  SyncPoint::GetInstance()->EnableProcessing();
  int files = 0;
  auto ingest = [&]() {
    const std::string file = dbname_ + "/ingest" + std::to_string(files++);
    SstFileWriter writer(EnvOptions(), options);
    ASSERT_OK(writer.Open(file));
    ASSERT_OK(writer.Put("a", "v"));
    ASSERT_OK(writer.Finish());
    ASSERT_OK(db_->IngestExternalFile({file}, IngestExternalFileOptions()));
  };
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  ingest();
  ASSERT_EQ(0, clears);
  ASSERT_OK(db_->SetOptions({{"min_tombstones_for_range_conversion", "4"}}));
  ingest();
  ASSERT_EQ(0, clears);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  ingest();
  ASSERT_EQ(1, clears);
  ASSERT_OK(db_->SetOptions({{"min_tombstones_for_range_conversion", "0"}}));
  ingest();
  ASSERT_EQ(2, clears);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBHeadMarkTest, RandomQueueMatchesModel) {
  // A producer appends items, a consumer deletes the oldest ones, and late
  // items arrive inside the dead prefix. Every scan from the queue start must
  // return the oldest live item, with marks published, used and lowered on
  // the way. Memtable switches and snapshot reads are mixed in.
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  Options options = HeadMarkOptions();
  // TEST_SwitchMemtable() does not schedule a flush, so allow enough
  // immutable memtables that a manual Flush() never waits for a stall to end.
  options.max_write_buffer_number = 64;
  // Compactions keep the L0 file count below the write stop trigger.
  options.disable_auto_compactions = false;
  DestroyAndReopen(options);

  std::set<int> live;
  int next = 0;
  HeadMarkTracker tracker;
  for (int step = 0; step < 20000; ++step) {
    const uint32_t op = rnd.Uniform(100);
    if (op < 40) {
      ASSERT_OK(Put(QKey(next), "v"));
      live.insert(next++);
    } else if (op < 75) {
      if (!live.empty()) {
        ASSERT_OK(Delete(QKey(*live.begin())));
        live.erase(live.begin());
      }
    } else if (op < 80) {
      // A late item inside the dead prefix.
      if (!live.empty() && *live.begin() > 0) {
        const int k = static_cast<int>(rnd.Uniform(*live.begin()));
        ASSERT_OK(Put(QKey(k), "late"));
        live.insert(k);
      }
    } else if (op < 81) {
      ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    } else if (op < 82) {
      ASSERT_OK(Flush());
    } else if (op < 85) {
      const Snapshot* snap = db_->GetSnapshot();
      const std::string expected = live.empty() ? "" : QKey(*live.begin());
      // A newer write before the queue range, so the snapshot is older than
      // the latest sequence number.
      ASSERT_OK(Put("a", "v"));
      ReadOptions ro;
      ro.snapshot = snap;
      ASSERT_EQ(expected, SeekFirst("q", ro));
      db_->ReleaseSnapshot(snap);
    } else {
      const std::string expected = live.empty() ? "" : QKey(*live.begin());
      ASSERT_EQ(expected, SeekFirst("q"));
    }
  }
  // The run must have exercised the marks.
  ASSERT_GT(tracker.uses, 0);
  ASSERT_FALSE(tracker.results.empty());
}

TEST_F(DBHeadMarkTest, ConcurrentQueueSeekMatchesFullScan) {
  // Producers, consumers and scanners run at the same time, so marks are
  // published, used and lowered while writers insert. Each scanner takes a
  // snapshot and compares Seek("q"), which can use a mark, with SeekToFirst(),
  // which never uses a mark, at that snapshot. The DB holds only queue keys,
  // so both must return the same key.
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Options options = HeadMarkOptions();
  options.allow_concurrent_memtable_write = true;
  DestroyAndReopen(options);

  // Six digits keep the keys of all items in queue order.
  auto key = [](int i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "q%06d", i);
    return std::string(buf);
  };
  // The producers stop after this many items, which bounds the dead prefix
  // that each SeekToFirst() walks. The test is much slower under TSAN, so
  // under TSAN it does a tenth of the work to stay within the 60 s test limit.
#ifdef __SANITIZE_THREAD__
  constexpr int kWorkDivisor = 10;
#else
  constexpr int kWorkDivisor = 1;
#endif
  constexpr int kMaxItems = 12000 / kWorkDivisor;
  constexpr int kComparisonsPerScanner = 1000 / kWorkDivisor;
  std::atomic<int> next{0};
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> uses{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MergingIterator::SeekOverHeadMark",
      [&](void*) { uses.fetch_add(1, std::memory_order_relaxed); });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<port::Thread> writers;
  for (int t = 0; t < 2; ++t) {
    // A producer appends single items and batches of 12 items, which take the
    // batch path that lowers marks by key range. A late item revives a key
    // in the dead prefix.
    writers.emplace_back([&, t]() {
      Random rnd(seed + t);
      while (!stop.load() && next.load() < kMaxItems) {
        const uint32_t op = rnd.Uniform(16);
        if (op == 0) {
          WriteBatch batch;
          const int first = next.fetch_add(12);
          for (int i = first; i < first + 12; ++i) {
            ASSERT_OK(batch.Put(key(i), "v"));
          }
          ASSERT_OK(db_->Write(WriteOptions(), &batch));
        } else if (op == 1) {
          ASSERT_OK(
              Put(key(static_cast<int>(rnd.Uniform(next.load() + 1))), "late"));
        } else {
          ASSERT_OK(Put(key(next.fetch_add(1)), "v"));
        }
      }
    });
  }
  for (int t = 0; t < 2; ++t) {
    // A consumer deletes the oldest live item.
    writers.emplace_back([&]() {
      while (!stop.load()) {
        std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
        iter->Seek("q");
        ASSERT_OK(iter->status());
        if (iter->Valid()) {
          ASSERT_OK(Delete(iter->key().ToString()));
        }
      }
    });
  }
  std::vector<port::Thread> scanners;
  for (int t = 0; t < 3; ++t) {
    scanners.emplace_back([&]() {
      for (int i = 0; i < kComparisonsPerScanner; ++i) {
        const Snapshot* snap = db_->GetSnapshot();
        ReadOptions ro;
        ro.snapshot = snap;
        std::unique_ptr<Iterator> seek(db_->NewIterator(ro));
        seek->Seek("q");
        std::unique_ptr<Iterator> full(db_->NewIterator(ro));
        full->SeekToFirst();
        ASSERT_OK(seek->status());
        ASSERT_OK(full->status());
        ASSERT_EQ(full->Valid(), seek->Valid());
        if (full->Valid()) {
          ASSERT_EQ(full->key().ToString(), seek->key().ToString());
        }
        seek.reset();
        full.reset();
        db_->ReleaseSnapshot(snap);
      }
    });
  }
  for (port::Thread& t : scanners) {
    t.join();
  }
  stop.store(true);
  for (port::Thread& t : writers) {
    t.join();
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_GT(uses.load(), 0);
}

// Reverse marks (see "Reverse marks" at MemTableHeadMark). In these tests the
// queue is consumed from its largest key: "q0010" to "q0019" are deleted and
// "q0000" to "q0009" are live.
class DBReverseHeadMarkTest : public DBHeadMarkTest {
 public:
  void MakeReverseQueue() {
    for (int i = 0; i < 20; ++i) {
      ASSERT_OK(Put(QKey(i), "v"));
    }
    for (int i = 10; i < 20; ++i) {
      ASSERT_OK(Delete(QKey(i)));
    }
  }
  static constexpr HeadMarkDirection kReverse = HeadMarkDirection::kReverse;

  // One (start, end, seqno) entry per fragment and seqno of the active
  // memtable's range tombstones, in fragment order, newest seqno first.
  using RangeDels =
      std::vector<std::tuple<std::string, std::string, SequenceNumber>>;
  RangeDels ActiveRangeDels() {
    auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
        db_->DefaultColumnFamily());
    std::unique_ptr<FragmentedRangeTombstoneIterator> iter(
        cfh->cfd()->mem()->NewRangeTombstoneIterator(
            ReadOptions(), kMaxSequenceNumber, /*immutable_memtable=*/false));
    RangeDels dels;
    if (iter == nullptr) {
      return dels;
    }
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      dels.emplace_back(iter->start_key().ToString(),
                        iter->end_key().ToString(), iter->seq());
    }
    return dels;
  }

  // Asserts that no fragment of the active memtable has more than one seqno,
  // that is, that the converted range tombstones do not nest.
  void AssertOneSeqnoPerFragment() {
    const RangeDels dels = ActiveRangeDels();
    for (size_t i = 1; i < dels.size(); ++i) {
      ASSERT_NE(std::get<0>(dels[i - 1]), std::get<0>(dels[i]))
          << "fragment with more than one seqno at " << std::get<0>(dels[i]);
    }
  }
};

TEST_F(DBReverseHeadMarkTest, SeekForPrevPublishesInclusiveMark) {
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
      tracker.results);
  // The mark claims ("q0009", "q0019"], stored in descending order.
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(19), QKey(9), QKey(9), false}}),
            ActiveMarks(kReverse));
  ASSERT_TRUE(ActiveMarks().empty());

  tracker.Reset();
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(15)));
  ASSERT_EQ(2, tracker.reverse_uses);
  ASSERT_EQ(0, tracker.uses);
  // A seek key above the mark does not use it.
  ASSERT_EQ(QKey(9), SeekForPrevLast("q1"));
  ASSERT_EQ(2, tracker.reverse_uses);
}

TEST_F(DBReverseHeadMarkTest, LowerBoundEndsMarkAtLowestTombstone) {
  // A reverse scan that stops at iterate_lower_bound has read the key below
  // the bound, but proved nothing about the keys between that key and the
  // run. The mark ends at the lowest tombstone of the run, which it excludes.
  // In this test the bound is "q0012", so the run is "q0019" down to "q0012",
  // and the scan stops at "q0011", the next key below the bound.
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  const std::string lower = QKey(12);
  const Slice lower_slice(lower);
  ReadOptions ro;
  ro.iterate_lower_bound = &lower_slice;
  ASSERT_EQ("", SeekForPrevLast(QKey(19), ro));
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(19), QKey(12), QKey(12), false}}),
            ActiveMarks(kReverse));
  // A scan without the bound uses the mark and still finds "q0009".
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  ASSERT_EQ(1, tracker.reverse_uses);
}

TEST_F(DBReverseHeadMarkTest, RunToEndOfDataEndsMarkAtLowestTombstone) {
  // When a reverse run reaches the end of the data, the scan stops at the
  // lowest tombstone of the run, and the mark excludes it. In this test every
  // key from "q0000" to "q0009" is deleted.
  DestroyAndReopen(HeadMarkOptions());
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put(QKey(i), "v"));
  }
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  ASSERT_EQ("", SeekForPrevLast(QKey(9)));
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(9), QKey(0), QKey(0), false}}),
            ActiveMarks(kReverse));
  HeadMarkTracker tracker;
  ASSERT_EQ("", SeekForPrevLast(QKey(9)));
  ASSERT_EQ(1, tracker.reverse_uses);
  // A put inside the claim lowers the mark, and the next scan finds the key.
  ASSERT_OK(Put(QKey(5), "late"));
  ASSERT_EQ(QKey(5), SeekForPrevLast(QKey(9)));
}

TEST_F(DBReverseHeadMarkTest, ExtensionInsideClaimChecksOnlyItsRun) {
  DestroyAndReopen(HeadMarkOptions());
  for (int i = 0; i < 30; ++i) {
    ASSERT_OK(Put(QKey(i), "v"));
  }
  for (int i = 10; i < 30; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  // Two late puts after the snapshot. The snapshot scan claims ("q0009",
  // "q0029"], and its walk down from "q0029" stops at "q0025".
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(Put(QKey(15), "late"));
  ASSERT_OK(Put(QKey(25), "late"));
  ReadOptions ro;
  ro.snapshot = snap;
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(29), ro));
  db_->ReleaseSnapshot(snap);
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(29), QKey(9), QKey(25), false}}),
            ActiveMarks(kReverse));

  // A newer scan is sent by the mark to "q0025", sees "q0025" down to "q0016"
  // deleted and stops at the live "q0015", inside the claim. The mark becomes
  // usable only down to "q0015". The deletions are repeated above the
  // snapshot scan's converted range tombstone, so that the scan counts them.
  for (int i = 16; i < 26; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  ASSERT_EQ(QKey(15), SeekForPrevLast(QKey(29)));
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(29), QKey(9), QKey(15), false}}),
            ActiveMarks(kReverse));
  ASSERT_EQ(QKey(15), SeekForPrevLast(QKey(29)));
}

// A prefix scan that leaves its prefix backward stops at a key of another
// prefix. Its mark must not claim the keys between that key and the run: an
// SST prefix filter can hide a whole prefix there.
TEST_F(DBReverseHeadMarkTest, PrefixScanMarkStaysInsidePrefix) {
  Options options = HeadMarkOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  table_options.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  // The SST holds only prefix "b".
  ASSERT_OK(Put("b5", "vb"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a0", "va"));
  for (const char* k : {"a1", "a2", "c1", "c2"}) {
    ASSERT_OK(Delete(k));
  }
  HeadMarkTracker tracker;

  // The prefix filter makes the SST skip the seek. The scan visits "c2" and
  // "c1", then stops at "a2" outside its prefix.
  ReadOptions prefix_ro;
  prefix_ro.prefix_same_as_start = true;
  ASSERT_EQ("", SeekForPrevLast("c9", prefix_ro));
  ASSERT_GT(options.statistics->getTickerCount(NON_LAST_LEVEL_SEEK_FILTERED) +
                options.statistics->getTickerCount(LAST_LEVEL_SEEK_FILTERED),
            0);
  // The mark ends at the lowest tombstone of the run, not at "a2".
  ASSERT_EQ(std::vector<HeadMarkView>({{"c9", "c1", "c1", false}}),
            ActiveMarks(kReverse));

  // A total order scan finds "b5", and no conversion hides it.
  ReadOptions total_ro;
  total_ro.total_order_seek = true;
  ASSERT_EQ("b5", SeekForPrevLast("c9", total_ro));
  ASSERT_EQ("vb", Get("b5"));
  ASSERT_OK(Flush());
  ASSERT_EQ("vb", Get("b5"));
  ASSERT_EQ("b5", SeekForPrevLast("c9", total_ro));
}

TEST_F(DBReverseHeadMarkTest, SeekToLastPublishesExclusiveMark) {
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  // "q0020" is the upper bound. It is live, but the scan excludes it.
  ASSERT_OK(Put(QKey(20), "v"));
  ASSERT_EQ(QKey(9), SeekToLastBelow(QKey(20)));
  const std::vector<HeadMarkView> expected(
      {{QKey(20), QKey(9), QKey(9), false, /*start_exclusive=*/true}});
  ASSERT_EQ(expected, ActiveMarks(kReverse));

  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(9), SeekToLastBelow(QKey(20)));
  ASSERT_EQ(1, tracker.reverse_uses);
  // SeekForPrev("q0020") includes "q0020", which the mark does not claim, so
  // it must not use the mark.
  ASSERT_EQ(QKey(20), SeekForPrevLast(QKey(20)));
  ASSERT_EQ(1, tracker.reverse_uses);
  // A put at the exclusive start does not change the mark.
  ASSERT_OK(Put(QKey(20), "v2"));
  ASSERT_EQ(expected, ActiveMarks(kReverse));
}

TEST_F(DBReverseHeadMarkTest, ValidationSkipsVersionsOfExclusiveStart) {
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  ASSERT_OK(Put(QKey(20), "v"));
  // The scan reads at this snapshot. After it, more than kMaxHeadMarkWalk
  // (16384) versions of the upper bound "q0020" make the validation walk run.
  // The claim excludes "q0020", so the walk must step over none of them.
  const Snapshot* snap = db_->GetSnapshot();
  constexpr int kVersions = 17000;
  WriteBatch batch;
  for (int i = 0; i < kVersions; ++i) {
    ASSERT_OK(batch.Put(QKey(20), "v"));
  }
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  std::vector<std::string> steps;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::ValidateHeadMarkRange:Step", [&](void* arg) {
        steps.push_back(static_cast<Slice*>(arg)->ToString());
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const std::string upper_bound = QKey(20);
  const Slice ub(upper_bound);
  ReadOptions ro;
  ro.snapshot = snap;
  ro.iterate_upper_bound = &ub;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    iter->SeekToLast();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(QKey(9), iter->key());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  db_->ReleaseSnapshot(snap);
  // The walk visits the put and the deletion of each key from "q0019" down to
  // "q0010", and stops at "q0009", the end of the claim.
  std::vector<std::string> expected;
  for (int i = 19; i >= 10; --i) {
    expected.push_back(QKey(i));
    expected.push_back(QKey(i));
  }
  expected.push_back(QKey(9));
  ASSERT_EQ(expected, steps);
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(20), QKey(9), QKey(9), false,
                                        /*start_exclusive=*/true}}),
            ActiveMarks(kReverse));
}

TEST_F(DBReverseHeadMarkTest, ReverseMarkDoesNotChangePinnedVersion) {
  // The reverse form of HeadMarkDoesNotChangePinnedVersionAfterIngestion:
  // iterator A keeps the Version from before an ingestion and reads at the
  // ingested file's sequence number.
  Options options = HeadMarkOptions();
  DestroyAndReopen(options);
  CreateColumnFamilies({"other"}, options);
  // An SST file holds the live key q0005. The memtable deletes the keys
  // around it, and q0000 is live.
  ASSERT_OK(Put(QKey(5), "old"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(QKey(0), "v"));
  for (int i = 1; i < 10; ++i) {
    if (i != 5) {
      ASSERT_OK(Delete(QKey(i)));
    }
  }
  std::unique_ptr<Iterator> a =
      IteratorWithOldVersionAtNewSeq(options, /*deleted=*/QKey(5));
  ASSERT_NE(nullptr, a);

  HeadMarkTracker tracker;
  // A still sees q0005, and its publish is refused.
  a->SeekForPrev(QKey(9));
  ASSERT_OK(a->status());
  ASSERT_TRUE(a->Valid());
  ASSERT_EQ(QKey(5), a->key().ToString());
  ASSERT_EQ(std::vector<HeadMarkPublishResult>(
                {HeadMarkPublishResult::kRefusedIngest}),
            tracker.results);
  ASSERT_TRUE(ActiveMarks(kReverse).empty());

  // A new scan sees the ingested delete and publishes (q0000, q0009].
  ASSERT_EQ(QKey(0), SeekForPrevLast(QKey(9)));
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(9), QKey(0), QKey(0), false}}),
            ActiveMarks(kReverse));

  // A must not use that mark.
  tracker.Reset();
  a->SeekForPrev(QKey(9));
  ASSERT_OK(a->status());
  ASSERT_TRUE(a->Valid());
  ASSERT_EQ(QKey(5), a->key().ToString());
  ASSERT_EQ(0, tracker.reverse_uses);
  ASSERT_EQ(QKey(0), SeekForPrevLast(QKey(9)));
  ASSERT_EQ(1, tracker.reverse_uses);
}

TEST_F(DBReverseHeadMarkTest, LatePutLowersReverseMark) {
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  // A late item inside the claim moves the claim's lower end up to it.
  ASSERT_OK(Put(QKey(15), "late"));
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(19), QKey(15), QKey(15), true}}),
            ActiveMarks(kReverse));
  ASSERT_EQ(QKey(15), SeekForPrevLast(QKey(19)));

  // After the consumer deletes it, one tombstone extends the lowered mark.
  HeadMarkTracker tracker;
  ASSERT_OK(Delete(QKey(15)));
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kExtended},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(19), QKey(9), QKey(9), false}}),
            ActiveMarks(kReverse));
}

TEST_F(DBReverseHeadMarkTest, ScanSentByReverseMarkExtendsIt) {
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  for (int i = 6; i < 10; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(5), SeekForPrevLast(QKey(19)));
  ASSERT_EQ(1, tracker.reverse_uses);
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kExtended},
      tracker.results);
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(19), QKey(5), QKey(5), false}}),
            ActiveMarks(kReverse));
}

TEST_F(DBReverseHeadMarkTest, ReverseValidationStopsAtNewerLiveEntry) {
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  // A put after the scan's snapshot is invisible to the scan, so the scan
  // claims it. The walk goes down from "q0019" and stops at it.
  const Snapshot* snap = db_->GetSnapshot();
  ASSERT_OK(Put(QKey(12), "late"));
  ReadOptions ro;
  ro.snapshot = snap;
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19), ro));
  db_->ReleaseSnapshot(snap);
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(19), QKey(9), QKey(12), false}}),
            ActiveMarks(kReverse));
  ASSERT_EQ(QKey(12), SeekForPrevLast(QKey(19)));
}

TEST_F(DBReverseHeadMarkTest, CascadeReachesExclusiveStart) {
  // A newer level's range tombstone moves a reverse seek to the tombstone's
  // start, which the seek then excludes. If that start equals the exclusive
  // start of an older level's mark, the mark is usable.
  Options options = HeadMarkOptions();
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  DestroyAndReopen(options);
  MakeReverseQueue();
  ASSERT_OK(Put(QKey(20), "v"));
  // The mark ("q0009", "q0020") goes into the memtable that becomes
  // immutable.
  ASSERT_EQ(QKey(9), SeekToLastBelow(QKey(20)));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             QKey(20), QKey(30)));
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(25)));
  ASSERT_EQ(1, tracker.reverse_uses);
}

TEST_F(DBReverseHeadMarkTest, ConversionEndsAtMarkUsableEnd) {
  // The first conversion uses the effective seek target as its exclusive
  // upper end. In this test the active memtable's reverse mark moves each
  // later seek, so each later conversion ends at that mark's usable end, the
  // live key where the scan before stopped. The scan did not read the keys
  // above it. The tombstone at the usable end stays a point tombstone, and
  // the converted ranges of one queue stay apart and do not nest.
  for (const bool seek_to_last : {false, true}) {
    SCOPED_TRACE(seek_to_last ? "SeekToLast" : "SeekForPrev");
    DestroyAndReopen(HeadMarkOptions());
    MakeReverseQueue();
    const std::string target = seek_to_last ? QKey(20) : QKey(19);
    auto scan = [&]() {
      return seek_to_last ? SeekToLastBelow(target) : SeekForPrevLast(target);
    };
    // No mark moved the first scan's seek, so the seek target is the
    // exclusive upper end of its conversion.
    const SequenceNumber s1 = db_->GetLatestSequenceNumber();
    ASSERT_EQ(QKey(9), scan());
    ASSERT_EQ(RangeDels({{QKey(10), target, s1}}), ActiveRangeDels());

    for (int i = 6; i < 10; ++i) {
      ASSERT_OK(Delete(QKey(i)));
    }
    const SequenceNumber s2 = db_->GetLatestSequenceNumber();
    HeadMarkTracker tracker;
    ASSERT_EQ(QKey(5), scan());
    ASSERT_EQ(1, tracker.reverse_uses);
    ASSERT_EQ(RangeDels({{QKey(6), QKey(9), s2}, {QKey(10), target, s1}}),
              ActiveRangeDels());

    for (int i = 2; i < 6; ++i) {
      ASSERT_OK(Delete(QKey(i)));
    }
    const SequenceNumber s3 = db_->GetLatestSequenceNumber();
    ASSERT_EQ(QKey(1), scan());
    ASSERT_EQ(RangeDels({{QKey(2), QKey(5), s3},
                         {QKey(6), QKey(9), s2},
                         {QKey(10), target, s1}}),
              ActiveRangeDels());
  }
}

TEST_F(DBReverseHeadMarkTest, ConversionEndsAtUsableEndWithUserRangeDeletion) {
  // A user range deletion starts below or at the mark's usable end "q0009".
  // Either way, the conversion ends at "q0009".
  for (const bool starts_at_usable_end : {false, true}) {
    SCOPED_TRACE(starts_at_usable_end ? "starts at q0009" : "starts below");
    DestroyAndReopen(HeadMarkOptions());
    MakeReverseQueue();
    const SequenceNumber s1 = db_->GetLatestSequenceNumber();
    ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
    const int del_start = starts_at_usable_end ? 9 : 8;
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               QKey(del_start), QKey(11)));
    const SequenceNumber d = db_->GetLatestSequenceNumber();
    // Delete the 4 keys below the range deletion, so the run is long enough.
    for (int i = del_start - 4; i < del_start; ++i) {
      ASSERT_OK(Delete(QKey(i)));
    }
    const SequenceNumber s2 = db_->GetLatestSequenceNumber();
    ASSERT_EQ(QKey(del_start - 5), SeekForPrevLast(QKey(19)));
    RangeDels expected;
    if (starts_at_usable_end) {
      // The conversion is [q0005, q0009).
      expected = {{QKey(5), QKey(9), s2},
                  {QKey(9), QKey(10), d},
                  {QKey(10), QKey(11), d},
                  {QKey(10), QKey(11), s1},
                  {QKey(11), QKey(19), s1}};
    } else {
      // The conversion is [q0004, q0009).
      expected = {{QKey(4), QKey(8), s2},  {QKey(8), QKey(9), s2},
                  {QKey(8), QKey(9), d},   {QKey(9), QKey(10), d},
                  {QKey(10), QKey(11), d}, {QKey(10), QKey(11), s1},
                  {QKey(11), QKey(19), s1}};
    }
    ASSERT_EQ(expected, ActiveRangeDels());
  }
}

TEST_F(DBReverseHeadMarkTest, ConversionAfterLoweredMark) {
  // A late put lowers the mark into the first scan's converted tombstone.
  // The next conversion ends at the mark's usable end "q0015", so it
  // overlaps the first scan's tombstone only in ["q0010", "q0015"). That is
  // correct: both ranges are deleted at their seqnos.
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  const SequenceNumber s1 = db_->GetLatestSequenceNumber();
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  ASSERT_OK(Put(QKey(15), "late"));
  ASSERT_EQ(QKey(15), SeekForPrevLast(QKey(19)));
  ASSERT_OK(Delete(QKey(15)));
  for (int i = 6; i < 10; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  const SequenceNumber s3 = db_->GetLatestSequenceNumber();
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(5), SeekForPrevLast(QKey(19)));
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kExtended},
      tracker.results);
  ASSERT_EQ(RangeDels({{QKey(6), QKey(10), s3},
                       {QKey(10), QKey(15), s3},
                       {QKey(10), QKey(15), s1},
                       {QKey(15), QKey(19), s1}}),
            ActiveRangeDels());
}

TEST_F(DBReverseHeadMarkTest, ConversionLimitAppliesOnlyInSeekDirection) {
  // An immutable memtable's reverse mark moves a SeekForPrev() down to q0030
  // in that memtable and the older levels. The active memtable holds the live
  // keys q0020 and q0040 and deletes q0025-q0035, from the largest down. The
  // seek lands at q0040. Prev() crosses the run, but the scan did not read
  // the older levels above q0030, so it converts only [q0025, q0030). Next()
  // switches direction, which seeks every level again from q0020, so the run
  // read forward converts all of [q0025, q0040). Its first tombstone q0025
  // has the scan's own seqno, so the range from Prev() does not cover it.
  Options options = HeadMarkOptions();
  // Keep the first memtable in memory after the switch below.
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  DestroyAndReopen(options);
  ASSERT_OK(Put(QKey(30), "v"));
  for (int i = 41; i < 50; ++i) {
    ASSERT_OK(Put(QKey(i), "v"));
  }
  for (int i = 41; i < 50; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  ASSERT_EQ(QKey(30), SeekForPrevLast(QKey(49)));
  ASSERT_EQ(std::vector<HeadMarkView>({{QKey(49), QKey(30), QKey(30), false}}),
            ActiveMarks(kReverse));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put(QKey(20), "v"));
  ASSERT_OK(Put(QKey(40), "v"));
  for (int i = 35; i >= 25; --i) {
    ASSERT_OK(Delete(QKey(i)));
  }

  HeadMarkTracker tracker;
  std::vector<std::pair<std::string, std::string>> ranges;
  SyncPoint::GetInstance()->SetCallBack(
      "MemTable::AddLogicallyRedundantRangeTombstone:AddRange", [&](void* arg) {
        const auto* range = static_cast<std::pair<Slice, Slice>*>(arg);
        ranges.emplace_back(range->first.ToString(), range->second.ToString());
      });
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekForPrev(QKey(49));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(40), iter->key().ToString());
  ASSERT_EQ(1, tracker.reverse_uses);
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(20), iter->key().ToString());
  std::vector<std::pair<std::string, std::string>> expected = {
      {QKey(25), QKey(30)}};
  ASSERT_EQ(expected, ranges);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(QKey(40), iter->key().ToString());
  ASSERT_OK(iter->status());
  expected.emplace_back(QKey(25), QKey(40));
  ASSERT_EQ(expected, ranges);
}

TEST_F(DBReverseHeadMarkTest, ImmutableMemtableMarkEndsConversion) {
  // The mark that moves the seek is in an immutable memtable. It moved the
  // seek of that memtable and of the older levels down to its usable end
  // "q0009", so the scan did not read those levels above "q0009", and the
  // active memtable's conversion ends there too.
  Options options = HeadMarkOptions();
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  DestroyAndReopen(options);
  MakeReverseQueue();
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             QKey(12), QKey(13)));
  const SequenceNumber d = db_->GetLatestSequenceNumber();
  for (int i = 6; i < 10; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  const SequenceNumber s2 = db_->GetLatestSequenceNumber();
  HeadMarkTracker tracker;
  ASSERT_EQ(QKey(5), SeekForPrevLast(QKey(19)));
  ASSERT_EQ(1, tracker.reverse_uses);
  ASSERT_EQ(RangeDels({{QKey(6), QKey(9), s2}, {QKey(12), QKey(13), d}}),
            ActiveRangeDels());
}

TEST_F(DBReverseHeadMarkTest, RefusedPublishStillEndsConversionAtMark) {
  // While the scan seeks, a late put inside the claim lowers the mark that
  // moved the seek, so the scan's publish is refused. The scan's snapshot
  // does not see the put, and its conversion still ends at the usable end of
  // the mark that moved its seek.
  DestroyAndReopen(HeadMarkOptions());
  MakeReverseQueue();
  const SequenceNumber s1 = db_->GetLatestSequenceNumber();
  ASSERT_EQ(QKey(9), SeekForPrevLast(QKey(19)));
  for (int i = 6; i < 10; ++i) {
    ASSERT_OK(Delete(QKey(i)));
  }
  const SequenceNumber s2 = db_->GetLatestSequenceNumber();
  bool put_done = false;
  std::vector<HeadMarkPublishResult> results;
  SyncPoint::GetInstance()->SetCallBack(
      "MergingIterator::SeekForPrevOverHeadMark", [&](void*) {
        if (!put_done) {
          put_done = true;
          EXPECT_OK(Put(QKey(18), "late"));
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBIter::MaybePublishHeadMark:Result", [&](void* arg) {
        results.push_back(*static_cast<HeadMarkPublishResult*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  {
    // The callbacks use local variables, so this Defer turns them off even
    // when the assertion fails.
    Defer clear_callbacks([] {
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
    });
    ASSERT_EQ(QKey(5), SeekForPrevLast(QKey(19)));
  }
  ASSERT_TRUE(put_done);
  ASSERT_EQ(
      std::vector<HeadMarkPublishResult>{
          HeadMarkPublishResult::kRefusedChanged},
      results);
  ASSERT_EQ(RangeDels({{QKey(6), QKey(9), s2}, {QKey(10), QKey(19), s1}}),
            ActiveRangeDels());
  ASSERT_EQ(QKey(18), SeekForPrevLast(QKey(19)));
}

TEST_F(DBReverseHeadMarkTest, CompactionFilterSkipHidesKeyFromMarkedScan) {
  // This test documents a known limitation (see the Limits paragraph of
  // MemTableHeadMark). A compaction filter that returns kRemoveAndSkipUntil
  // drops the entries that it skips, also a point tombstone. An older value of
  // that key in a level below the compaction then becomes visible again, with
  // no new sequence number. Compaction does not clear the marks. A reverse mark
  // claims its seek key, which its converted range tombstone leaves out, so
  // Get() returns the key while a SeekForPrev() that uses the mark skips it.
  class SkipFilter : public CompactionFilter {
   public:
    Decision FilterV2(int /*level*/, const Slice& key, ValueType /*type*/,
                      const Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      std::string* skip_until) const override {
      if (enabled && key == "a0") {
        *skip_until = "c";
        return Decision::kRemoveAndSkipUntil;
      }
      return Decision::kKeep;
    }
    const char* Name() const override { return "SkipFilter"; }
    // MoveFilesToLevel() also compacts, so the filter skips only when the
    // test enables it.
    std::atomic<bool> enabled{false};
  };
  SkipFilter filter;
  Options options = HeadMarkOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.compaction_filter = &filter;
  DestroyAndReopen(options);
  ASSERT_OK(Put("b", "old"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Put("a0", "input"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Delete("a0"));
  ASSERT_OK(Put("0", "live"));
  {
    HeadMarkTracker tracker;
    ASSERT_EQ("0", SeekForPrevLast("b"));
    ASSERT_EQ(
        std::vector<HeadMarkPublishResult>{HeadMarkPublishResult::kPublished},
        tracker.results);
  }
  // The mark claims ("0", "b"]; the converted range ["a0", "b") leaves out b.
  ASSERT_EQ(std::vector<HeadMarkView>({{"b", "0", "0", false}}),
            ActiveMarks(kReverse));
  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(1, meta.levels[1].files.size());
  filter.enabled = true;
  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {meta.levels[1].files[0].name}, 2));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  ASSERT_EQ(1, NumTableFilesAtLevel(3));
  ASSERT_EQ("old", Get("b"));
  // The limitation: the mark still skips b.
  HeadMarkTracker tracker;
  ASSERT_EQ("0", SeekForPrevLast("b"));
  ASSERT_EQ(1, tracker.reverse_uses);
}

TEST_F(DBReverseHeadMarkTest, RandomReverseQueueConversionsDoNotNest) {
  // A queue consumed from the largest key, with deletes among the 4 largest
  // live items and no late items. After every scan, the scan returns the
  // largest live item and no fragment of the active memtable has more than one
  // seqno. At the end, Get() of every key matches the model, so no conversion
  // deleted a live key. Each seek kind starts with a fresh
  // database, so each one publishes and reuses its own marks and converted
  // ranges.
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  for (const bool seek_to_last : {false, true}) {
    SCOPED_TRACE(seek_to_last ? "SeekToLast" : "SeekForPrev");
    Options options = HeadMarkOptions();
    // See RandomQueueMatchesModel.
    options.max_write_buffer_number = 64;
    options.disable_auto_compactions = false;
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);
    std::set<int> live;
    std::set<int> deleted;
    // Items are added downward from 9999.
    int next = 9999;
    HeadMarkTracker tracker;
    for (int step = 0; step < 4000; ++step) {
      const uint32_t op = rnd.Uniform(100);
      if (op < 45) {
        if (next >= 0) {
          ASSERT_OK(Put(QKey(next), "v"));
          live.insert(next--);
        }
      } else if (op < 90) {
        if (!live.empty()) {
          const int skip = static_cast<int>(
              rnd.Uniform(static_cast<int>(std::min<size_t>(4, live.size()))));
          auto it = std::prev(live.end(), 1 + skip);
          ASSERT_OK(Delete(QKey(*it)));
          deleted.insert(*it);
          live.erase(it);
        }
      } else if (op < 91) {
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
      } else {
        const std::string expected = live.empty() ? "" : QKey(*live.rbegin());
        ASSERT_EQ(expected,
                  seek_to_last ? SeekToLastBelow("r") : SeekForPrevLast("r"));
        ASSERT_NO_FATAL_FAILURE(AssertOneSeqnoPerFragment());
      }
    }
    for (const int k : live) {
      ASSERT_EQ("v", Get(QKey(k))) << k;
    }
    for (const int k : deleted) {
      ASSERT_EQ("NOT_FOUND", Get(QKey(k))) << k;
    }
    ASSERT_GT(tracker.reverse_uses, 0);
  }
}

TEST_F(DBReverseHeadMarkTest, RandomReverseQueueMatchesModel) {
  // A consumer deletes the largest live items, a producer adds items below
  // them, and late items arrive above them. Scans use SeekForPrev() or
  // SeekToLast() with an upper bound, and must return the largest live item.
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);
  Options options = HeadMarkOptions();
  // See RandomQueueMatchesModel.
  options.max_write_buffer_number = 64;
  options.disable_auto_compactions = false;
  DestroyAndReopen(options);

  std::set<int> live;
  // Items are added downward from 9999.
  int next = 9999;
  HeadMarkTracker tracker;
  for (int step = 0; step < 20000; ++step) {
    const uint32_t op = rnd.Uniform(100);
    if (op < 40) {
      if (next >= 0) {
        ASSERT_OK(Put(QKey(next), "v"));
        live.insert(next--);
      }
    } else if (op < 75) {
      if (!live.empty()) {
        ASSERT_OK(Delete(QKey(*live.rbegin())));
        live.erase(std::prev(live.end()));
      }
    } else if (op < 80) {
      // A late item inside the dead top. One in four lands on "q9999", the
      // excluded start of the marks that SeekToLastBelow("q9999") makes.
      const int top = live.empty() ? next : *live.rbegin();
      if (top < 9999) {
        const int k = rnd.OneIn(4)
                          ? 9999
                          : top + 1 + static_cast<int>(rnd.Uniform(9999 - top));
        ASSERT_OK(Put(QKey(k), "late"));
        live.insert(k);
      }
    } else if (op < 81) {
      ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    } else if (op < 82) {
      ASSERT_OK(Flush());
    } else {
      const std::string expected = live.empty() ? "" : QKey(*live.rbegin());
      if (op < 91) {
        ASSERT_EQ(expected, SeekForPrevLast(QKey(9999)));
      } else if (op < 95) {
        // The upper bound "q9999" makes a mark whose start "q9999" is
        // excluded, and items can be written at "q9999". The inclusive
        // SeekForPrev("q9999") above must not use such a mark to skip a live
        // "q9999".
        auto below = live.lower_bound(9999);
        const std::string expected_below =
            below == live.begin() ? "" : QKey(*std::prev(below));
        ASSERT_EQ(expected_below, SeekToLastBelow(QKey(9999)));
      } else {
        ASSERT_EQ(expected, SeekToLastBelow("r"));
      }
    }
  }
  ASSERT_GT(tracker.reverse_uses, 0);
  ASSERT_FALSE(tracker.results.empty());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
