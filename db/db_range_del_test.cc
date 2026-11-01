//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "db/version_set.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "test_util/testutil.h"
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
  options.merge_operator.reset(new MockMergeOperator());
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
  options.merge_operator.reset(new MockMergeOperator());
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
  options.merge_operator.reset(new MockMergeOperator());
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
