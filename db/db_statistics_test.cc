//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>

#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "monitoring/thread_status_util.h"
#include "port/stack_trace.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class DBStatisticsTest : public DBTestBase {
 public:
  DBStatisticsTest()
      : DBTestBase("db_statistics_test", /*env_do_fsync=*/true) {}
};

TEST_F(DBStatisticsTest, CompressionStatsTest) {
  for (CompressionType type : GetSupportedCompressions()) {
    if (type == kNoCompression) {
      continue;
    }
    if (type == kBZip2Compression) {
      // Weird behavior in this test
      continue;
    }
    SCOPED_TRACE("Compression type: " + std::to_string(type));

    Options options = CurrentOptions();
    options.compression = type;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    options.statistics->set_stats_level(StatsLevel::kExceptTimeForMutex);
    BlockBasedTableOptions bbto;
    bbto.enable_index_compression = false;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    auto PopStat = [&](Tickers t) -> uint64_t {
      return options.statistics->getAndResetTickerCount(t);
    };

    int kNumKeysWritten = 100;
    double compress_to = 0.5;
    // About three KVs per block
    int len = static_cast<int>(BlockBasedTableOptions().block_size / 3);
    int uncomp_est = kNumKeysWritten * (len + 20);

    Random rnd(301);
    std::string buf;

    // Check that compressions occur and are counted when compression is turned
    // on
    for (int i = 0; i < kNumKeysWritten; ++i) {
      ASSERT_OK(
          Put(Key(i), test::CompressibleString(&rnd, compress_to, len, &buf)));
    }
    ASSERT_OK(Flush());
    EXPECT_EQ(34, PopStat(NUMBER_BLOCK_COMPRESSED));
    EXPECT_NEAR2(uncomp_est, PopStat(BYTES_COMPRESSED_FROM), uncomp_est / 10);
    EXPECT_NEAR2(uncomp_est * compress_to, PopStat(BYTES_COMPRESSED_TO),
                 uncomp_est / 10);

    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_DECOMPRESSED));
    EXPECT_EQ(0, PopStat(BYTES_DECOMPRESSED_FROM));
    EXPECT_EQ(0, PopStat(BYTES_DECOMPRESSED_TO));

    // And decompressions
    for (int i = 0; i < kNumKeysWritten; ++i) {
      auto r = Get(Key(i));
    }
    EXPECT_EQ(34, PopStat(NUMBER_BLOCK_DECOMPRESSED));
    EXPECT_NEAR2(uncomp_est, PopStat(BYTES_DECOMPRESSED_TO), uncomp_est / 10);
    EXPECT_NEAR2(uncomp_est * compress_to, PopStat(BYTES_DECOMPRESSED_FROM),
                 uncomp_est / 10);

    EXPECT_EQ(0, PopStat(BYTES_COMPRESSION_BYPASSED));
    EXPECT_EQ(0, PopStat(BYTES_COMPRESSION_REJECTED));
    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));

    // Check when compression is rejected.
    DestroyAndReopen(options);

    for (int i = 0; i < kNumKeysWritten; ++i) {
      ASSERT_OK(Put(Key(i), rnd.RandomBinaryString(len)));
    }
    ASSERT_OK(Flush());
    for (int i = 0; i < kNumKeysWritten; ++i) {
      auto r = Get(Key(i));
    }
    EXPECT_EQ(34, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
    EXPECT_NEAR2(uncomp_est, PopStat(BYTES_COMPRESSION_REJECTED),
                 uncomp_est / 10);

    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSED));
    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_DECOMPRESSED));
    EXPECT_EQ(0, PopStat(BYTES_COMPRESSED_FROM));
    EXPECT_EQ(0, PopStat(BYTES_COMPRESSED_TO));
    EXPECT_EQ(0, PopStat(BYTES_COMPRESSION_BYPASSED));
    EXPECT_EQ(0, PopStat(BYTES_DECOMPRESSED_FROM));
    EXPECT_EQ(0, PopStat(BYTES_DECOMPRESSED_TO));

    // Check when compression is disabled.
    options.compression = kNoCompression;
    DestroyAndReopen(options);

    for (int i = 0; i < kNumKeysWritten; ++i) {
      ASSERT_OK(Put(Key(i), rnd.RandomBinaryString(len)));
    }
    ASSERT_OK(Flush());
    for (int i = 0; i < kNumKeysWritten; ++i) {
      auto r = Get(Key(i));
    }
    EXPECT_EQ(34, PopStat(NUMBER_BLOCK_COMPRESSION_BYPASSED));
    EXPECT_NEAR2(uncomp_est, PopStat(BYTES_COMPRESSION_BYPASSED),
                 uncomp_est / 10);

    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSED));
    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_COMPRESSION_REJECTED));
    EXPECT_EQ(0, PopStat(NUMBER_BLOCK_DECOMPRESSED));
    EXPECT_EQ(0, PopStat(BYTES_COMPRESSED_FROM));
    EXPECT_EQ(0, PopStat(BYTES_COMPRESSED_TO));
    EXPECT_EQ(0, PopStat(BYTES_COMPRESSION_REJECTED));
    EXPECT_EQ(0, PopStat(BYTES_DECOMPRESSED_FROM));
    EXPECT_EQ(0, PopStat(BYTES_DECOMPRESSED_TO));
  }
}

TEST_F(DBStatisticsTest, MutexWaitStatsDisabledByDefault) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  ASSERT_EQ(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), 0);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBStatisticsTest, MutexWaitStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  CreateAndReopenWithCF({"pikachu"}, options);
  const uint64_t kMutexWaitDelay = 100;
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT,
                                       kMutexWaitDelay);
  ASSERT_OK(Put("hello", "rocksdb"));
  // A plain Put() may not lock the DB mutex; Flush() does.
  ASSERT_OK(Flush());
  ASSERT_GE(TestGetTickerCount(options, DB_MUTEX_WAIT_MICROS), kMutexWaitDelay);
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0);
}

TEST_F(DBStatisticsTest, ResetStats) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  for (int i = 0; i < 2; ++i) {
    // pick arbitrary ticker and histogram. On first iteration they're zero
    // because db is unused. On second iteration they're zero due to Reset().
    ASSERT_EQ(0, TestGetTickerCount(options, NUMBER_KEYS_WRITTEN));
    HistogramData histogram_data;
    options.statistics->histogramData(DB_WRITE, &histogram_data);
    ASSERT_EQ(0.0, histogram_data.max);

    if (i == 0) {
      // The Put() makes some of the ticker/histogram stats nonzero until we
      // Reset().
      ASSERT_OK(Put("hello", "rocksdb"));
      ASSERT_EQ(1, TestGetTickerCount(options, NUMBER_KEYS_WRITTEN));
      options.statistics->histogramData(DB_WRITE, &histogram_data);
      ASSERT_GT(histogram_data.max, 0.0);
      ASSERT_OK(options.statistics->Reset());
    }
  }
}

TEST_F(DBStatisticsTest, ExcludeTickers) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);
  options.statistics->set_stats_level(StatsLevel::kExceptTickers);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_EQ(0, options.statistics->getTickerCount(BYTES_WRITTEN));
  options.statistics->set_stats_level(StatsLevel::kExceptHistogramOrTimers);
  Reopen(options);
  ASSERT_EQ("value", Get("foo"));
  ASSERT_GT(options.statistics->getTickerCount(BYTES_READ), 0);
}

TEST_F(DBStatisticsTest, VerifyChecksumReadStat) {
  Options options = CurrentOptions();
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  Reopen(options);

  // Expected to be populated regardless of `PerfLevel` in user thread
  SetPerfLevel(kDisable);

  {
    // Scenario 0: only WAL data. Not verified so require ticker to be zero.
    ASSERT_OK(Put("foo", "value"));
    ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
    ASSERT_OK(db_->VerifyChecksum());
    ASSERT_EQ(0,
              options.statistics->getTickerCount(VERIFY_CHECKSUM_READ_BYTES));
  }

  // Create one SST.
  ASSERT_OK(Flush());
  std::unordered_map<std::string, uint64_t> table_files;
  uint64_t table_files_size = 0;
  ASSERT_OK(GetAllDataFiles(kTableFile, &table_files, &table_files_size));

  {
    // Scenario 1: Table verified in `VerifyFileChecksums()`. This should read
    // the whole file so we require the ticker stat exactly matches the file
    // size.
    ASSERT_OK(options.statistics->Reset());
    ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
    ASSERT_EQ(table_files_size,
              options.statistics->getTickerCount(VERIFY_CHECKSUM_READ_BYTES));
  }

  {
    // Scenario 2: Table verified in `VerifyChecksum()`. This opens a
    // `TableReader` to verify each block. It can involve duplicate reads of the
    // same data so we set a lower-bound only.
    ASSERT_OK(options.statistics->Reset());
    ASSERT_OK(db_->VerifyChecksum());
    ASSERT_GE(options.statistics->getTickerCount(VERIFY_CHECKSUM_READ_BYTES),
              table_files_size);
  }
}

TEST_F(DBStatisticsTest, BlockChecksumStats) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  Reopen(options);

  // Scenario 0: only WAL data. Not verified so require ticker to be zero.
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(db_->VerifyChecksum());
  ASSERT_EQ(0,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_COMPUTE_COUNT));
  ASSERT_EQ(0,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT));

  // Scenario 1: Flushed table verified in `VerifyChecksum()`. This opens a
  // `TableReader` to verify each of the four blocks (meta-index, table
  // properties, index, and data block).
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());
  ASSERT_OK(db_->VerifyChecksum());
  ASSERT_EQ(4,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_COMPUTE_COUNT));
  ASSERT_EQ(0,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT));

  // Scenario 2: Corrupted table verified in `VerifyChecksum()`. The corruption
  // is in the fourth and final verified block, i.e., the data block.
  std::unordered_map<std::string, uint64_t> table_files;
  ASSERT_OK(GetAllDataFiles(kTableFile, &table_files));
  ASSERT_EQ(1, table_files.size());
  std::string table_name = table_files.begin()->first;
  // Assumes the data block starts at offset zero.
  ASSERT_OK(test::CorruptFile(options.env, table_name, 0 /* offset */,
                              3 /* bytes_to_corrupt */));
  ASSERT_OK(options.statistics->Reset());
  ASSERT_NOK(db_->VerifyChecksum());
  ASSERT_EQ(4,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_COMPUTE_COUNT));
  ASSERT_EQ(1,
            options.statistics->getTickerCount(BLOCK_CHECKSUM_MISMATCH_COUNT));
}

TEST_F(DBStatisticsTest, BytesWrittenStats) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kExceptHistogramOrTimers);
  Reopen(options);

  EXPECT_EQ(0, options.statistics->getAndResetTickerCount(WAL_FILE_BYTES));
  EXPECT_EQ(0, options.statistics->getAndResetTickerCount(BYTES_WRITTEN));

  const int kNumKeysWritten = 100;

  // Scenario 0: Not using transactions.
  // This will write to WAL and memtable directly.
  ASSERT_OK(options.statistics->Reset());

  for (int i = 0; i < kNumKeysWritten; ++i) {
    ASSERT_OK(Put(Key(i), "val"));
  }

  EXPECT_EQ(options.statistics->getAndResetTickerCount(WAL_FILE_BYTES),
            options.statistics->getAndResetTickerCount(BYTES_WRITTEN));

  // Scenario 1: Using transactions.
  // This should not double count BYTES_WRITTEN (issue #12061).
  for (bool enable_pipelined_write : {false, true}) {
    ASSERT_OK(options.statistics->Reset());

    // Destroy the DB to recreate as a TransactionDB.
    Destroy(options, true);

    // Create a TransactionDB.
    TransactionDB* txn_db = nullptr;
    TransactionDBOptions txn_db_opts;
    txn_db_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
    options.enable_pipelined_write = enable_pipelined_write;
    ASSERT_OK(TransactionDB::Open(options, txn_db_opts, dbname_, &txn_db));
    ASSERT_NE(txn_db, nullptr);
    db_.reset(txn_db);

    WriteOptions wopts;
    TransactionOptions txn_opts;
    Transaction* txn = txn_db->BeginTransaction(wopts, txn_opts, nullptr);
    ASSERT_NE(txn, nullptr);
    ASSERT_OK(txn->SetName("txn1"));

    for (int i = 0; i < kNumKeysWritten; ++i) {
      ASSERT_OK(txn->Put(Key(i), "val"));
    }

    // Prepare() writes to WAL, but not to memtable. (WriteCommitted)
    ASSERT_OK(txn->Prepare());
    EXPECT_NE(0, options.statistics->getTickerCount(WAL_FILE_BYTES));
    // BYTES_WRITTEN would have been non-zero previously (issue #12061).
    EXPECT_EQ(0, options.statistics->getTickerCount(BYTES_WRITTEN));

    // Commit() writes to memtable and also a commit marker to WAL.
    ASSERT_OK(txn->Commit());
    delete txn;

    // The WAL has an extra header of size `kHeader` written to it,
    // as we are writing twice to it (first during Prepare, second during
    // Commit).
    EXPECT_EQ(options.statistics->getAndResetTickerCount(WAL_FILE_BYTES),
              options.statistics->getAndResetTickerCount(BYTES_WRITTEN) +
                  WriteBatchInternal::kHeader);

    // Cleanup
    db_.reset();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
