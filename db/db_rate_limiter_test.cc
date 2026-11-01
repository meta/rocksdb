//  Copyright (c) 2022-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/file_checksum_helper.h"

namespace ROCKSDB_NAMESPACE {

class DBRateLimiterOnReadTest
    : public DBTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  explicit DBRateLimiterOnReadTest()
      : DBTestBase("db_rate_limiter_on_read_test", /*env_do_fsync=*/false),
        use_direct_io_(std::get<0>(GetParam())),
        use_block_cache_(std::get<1>(GetParam())),
        use_readahead_(std::get<2>(GetParam())) {}

  void Init(Env* env = nullptr) {
    options_ = GetOptions();
    if (env != nullptr) {
      options_.env = env;
    }
    Reopen(options_);
    for (int i = 0; i < kNumFiles; ++i) {
      for (int j = 0; j < kNumKeysPerFile; ++j) {
        ASSERT_OK(Put(Key(i * kNumKeysPerFile + j), "val"));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(1);
  }

  BlockBasedTableOptions GetTableOptions() {
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = !use_block_cache_;
    return table_options;
  }

  ReadOptions GetReadOptions() {
    ReadOptions read_options;
    read_options.rate_limiter_priority = Env::IO_USER;
    read_options.readahead_size = use_readahead_ ? kReadaheadBytes : 0;
    return read_options;
  }

  void MultiGetAllKeys(const ReadOptions& read_options) {
    const int kNumKeys = kNumFiles * kNumKeysPerFile;
    std::vector<std::string> key_bufs;
    key_bufs.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; ++i) {
      key_bufs.emplace_back(Key(i));
    }
    std::vector<Slice> keys(key_bufs.begin(), key_bufs.end());
    std::vector<Status> statuses(kNumKeys);
    std::vector<PinnableSlice> values(kNumKeys);
    db_->MultiGet(read_options, dbfull()->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());
    for (const Status& s : statuses) {
      ASSERT_OK(s);
    }
  }

  Options GetOptions() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.file_checksum_gen_factory.reset(new FileChecksumGenCrc32cFactory());
    options.rate_limiter.reset(NewGenericRateLimiter(
        1 << 20 /* rate_bytes_per_sec */, 100 * 1000 /* refill_period_us */,
        10 /* fairness */, RateLimiter::Mode::kAllIo));
    options.table_factory.reset(NewBlockBasedTableFactory(GetTableOptions()));
    options.use_direct_reads = use_direct_io_;
    return options;
  }

 protected:
  const static int kNumKeysPerFile = 1;
  const static int kNumFiles = 3;
  const static int kReadaheadBytes = 32 << 10;  // 32KB

  Options options_;
  const bool use_direct_io_;
  const bool use_block_cache_;
  const bool use_readahead_;
};

std::string GetTestNameSuffix(
    ::testing::TestParamInfo<std::tuple<bool, bool, bool>> info) {
  std::ostringstream oss;
  if (std::get<0>(info.param)) {
    oss << "DirectIO";
  } else {
    oss << "BufferedIO";
  }
  if (std::get<1>(info.param)) {
    oss << "_BlockCache";
  } else {
    oss << "_NoBlockCache";
  }
  if (std::get<2>(info.param)) {
    oss << "_Readahead";
  } else {
    oss << "_NoReadahead";
  }
  return oss.str();
}

INSTANTIATE_TEST_CASE_P(DBRateLimiterOnReadTest, DBRateLimiterOnReadTest,
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool()),
                        GetTestNameSuffix);

TEST_P(DBRateLimiterOnReadTest, Get) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  int expected = 0;
  for (int i = 0; i < kNumFiles; ++i) {
    {
      std::string value;
      ASSERT_OK(db_->Get(GetReadOptions(), Key(i * kNumKeysPerFile), &value));
      ++expected;
    }
    ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

    {
      std::string value;
      ASSERT_OK(db_->Get(GetReadOptions(), Key(i * kNumKeysPerFile), &value));
      if (!use_block_cache_) {
        ++expected;
      }
    }
    ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
  }
}

TEST_P(DBRateLimiterOnReadTest, NewMultiGet) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  const int kNumKeys = kNumFiles * kNumKeysPerFile;
  int64_t expected = 0;
  {
    std::vector<std::string> key_bufs;
    key_bufs.reserve(kNumKeys);
    std::vector<Slice> keys;
    keys.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; ++i) {
      key_bufs.emplace_back(Key(i));
      keys.emplace_back(key_bufs[i]);
    }
    std::vector<Status> statuses(kNumKeys);
    std::vector<PinnableSlice> values(kNumKeys);
    const int64_t prev_total_rl_req = options_.rate_limiter->GetTotalRequests();
    db_->MultiGet(GetReadOptions(), dbfull()->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());
    const int64_t cur_total_rl_req = options_.rate_limiter->GetTotalRequests();
    for (int i = 0; i < kNumKeys; ++i) {
      ASSERT_TRUE(statuses[i].ok());
    }
    ASSERT_GT(cur_total_rl_req, prev_total_rl_req);
    ASSERT_EQ(cur_total_rl_req - prev_total_rl_req,
              options_.rate_limiter->GetTotalRequests(Env::IO_USER));
  }
  expected += kNumKeys;
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, AsyncIOMultiGet) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ReadOptions read_options = GetReadOptions();
  read_options.fill_cache = false;
  MultiGetAllKeys(read_options);
  const int64_t sync_requests =
      options_.rate_limiter->GetTotalRequests(Env::IO_USER);
  const int64_t sync_bytes =
      options_.rate_limiter->GetTotalBytesThrough(Env::IO_USER);
  ASSERT_EQ(kNumFiles * kNumKeysPerFile, sync_requests);

  int read_async_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus",
      [&](void* /*arg*/) { ++read_async_calls; });
  SyncPoint::GetInstance()->EnableProcessing();
  read_options.async_io = true;
  MultiGetAllKeys(read_options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

#if USE_COROUTINES
  ASSERT_GT(read_async_calls, 0);
#endif  // USE_COROUTINES
  ASSERT_EQ(2 * sync_requests,
            options_.rate_limiter->GetTotalRequests(Env::IO_USER));
  ASSERT_EQ(2 * sync_bytes,
            options_.rate_limiter->GetTotalBytesThrough(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, AsyncIOMultiGetAdjacentDirectIOBlocks) {
  if (!use_direct_io_ || !IsDirectIOSupported()) {
    return;
  }
  Options options = GetOptions();
  BlockBasedTableOptions table_options = GetTableOptions();
  table_options.block_size = 1000;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  constexpr int kFiles = 4;
  constexpr int kKeysPerFile = 200;
  Random rnd(301);
  std::vector<std::string> values;
  values.reserve(kFiles * kKeysPerFile);
  for (int f = 0; f < kFiles; ++f) {
    for (int k = 0; k < kKeysPerFile; ++k) {
      values.push_back(rnd.RandomString(300 + (k % 7) * 37));
      ASSERT_OK(Put(Key(f * kKeysPerFile + k), values.back()));
    }
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(1);

  // One batch of consecutive keys from every file, so each file reads several
  // adjacent blocks, most of them starting mid-page.
  constexpr int kPickedPerFile = 8;
  std::vector<int> picked;
  std::vector<std::string> key_bufs;
  picked.reserve(kFiles * kPickedPerFile);
  key_bufs.reserve(kFiles * kPickedPerFile);
  for (int f = 0; f < kFiles; ++f) {
    for (int j = 0; j < kPickedPerFile; ++j) {
      picked.push_back(f * kKeysPerFile + 3 + j);
      key_bufs.push_back(Key(picked.back()));
    }
  }
  std::vector<Slice> keys(key_bufs.begin(), key_bufs.end());
  auto multi_get = [&](bool async_io) {
    std::vector<PinnableSlice> results(keys.size());
    std::vector<Status> statuses(keys.size());
    ReadOptions read_options = GetReadOptions();
    read_options.fill_cache = false;
    read_options.async_io = async_io;
    db_->MultiGet(read_options, dbfull()->DefaultColumnFamily(), keys.size(),
                  keys.data(), results.data(), statuses.data());
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(values[picked[i]], results[i].ToString());
    }
  };

  RateLimiter* const rate_limiter = options.rate_limiter.get();
  const int64_t requests_before = rate_limiter->GetTotalRequests(Env::IO_USER);
  const int64_t bytes_before = rate_limiter->GetTotalBytesThrough(Env::IO_USER);
  ASSERT_NO_FATAL_FAILURE(multi_get(/*async_io=*/false));
  const int64_t sync_requests =
      rate_limiter->GetTotalRequests(Env::IO_USER) - requests_before;
  const int64_t sync_bytes =
      rate_limiter->GetTotalBytesThrough(Env::IO_USER) - bytes_before;

  int read_async_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus",
      [&](void* /*arg*/) { ++read_async_calls; });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(multi_get(/*async_io=*/true));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  const int64_t async_requests = rate_limiter->GetTotalRequests(Env::IO_USER) -
                                 requests_before - sync_requests;
  const int64_t async_bytes = rate_limiter->GetTotalBytesThrough(Env::IO_USER) -
                              bytes_before - sync_bytes;

#if USE_COROUTINES
  // MultiRead() merges the adjacent aligned requests of a file into one read.
  // The async_io path reads and charges each block on its own, re-reading the
  // aligned pages that adjacent blocks share.
  ASSERT_GT(read_async_calls, 0);
  ASSERT_GT(async_requests, sync_requests);
  ASSERT_GT(async_bytes, sync_bytes);
#else
  ASSERT_EQ(async_requests, sync_requests);
  ASSERT_EQ(async_bytes, sync_bytes);
#endif  // USE_COROUTINES
}

// A file system can reject ReadAsync even though it advertises kAsyncIO, e.g.
// when io_uring fails to initialize. MultiGet then reads synchronously, and
// each read must still be charged exactly once.
class ReadAsyncNotSupportedFS : public FileSystemWrapper {
 public:
  explicit ReadAsyncNotSupportedFS(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  static const char* kClassName() { return "ReadAsyncNotSupportedFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    IOStatus s = target()->NewRandomAccessFile(fname, opts, result, dbg);
    if (s.ok()) {
      *result = std::make_unique<File>(std::move(*result));
    }
    return s;
  }

 private:
  class File : public FSRandomAccessFileOwnerWrapper {
   public:
    using FSRandomAccessFileOwnerWrapper::FSRandomAccessFileOwnerWrapper;
    IOStatus ReadAsync(FSReadRequest& /*req*/, const IOOptions& /*opts*/,
                       std::function<void(FSReadRequest&, void*)> /*cb*/,
                       void* /*cb_arg*/, void** /*io_handle*/,
                       IOHandleDeleter* /*del_fn*/,
                       IODebugContext* /*dbg*/) override {
      return IOStatus::NotSupported("ReadAsync");
    }
  };
};

TEST_P(DBRateLimiterOnReadTest, AsyncIOMultiGetSyncFallback) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  auto fs = std::make_shared<ReadAsyncNotSupportedFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Init(env.get());

  int fallbacks = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "AsyncFileReader::MultiReadAsyncImpl:SyncFallback",
      [&](void* /*arg*/) { ++fallbacks; });
  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions read_options = GetReadOptions();
  read_options.async_io = true;
  MultiGetAllKeys(read_options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  const int64_t requests =
      options_.rate_limiter->GetTotalRequests(Env::IO_USER);
  Close();

#if USE_COROUTINES
  ASSERT_GT(fallbacks, 0);
#endif  // USE_COROUTINES
  ASSERT_EQ(kNumFiles * kNumKeysPerFile, requests);
}

TEST_P(DBRateLimiterOnReadTest, OldMultiGet) {
  // The old `vector<Status>`-returning `MultiGet()` APIs use `Read()`, which
  // supports rate limiting.
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  const int kNumKeys = kNumFiles * kNumKeysPerFile;
  int expected = 0;
  {
    std::vector<std::string> key_bufs;
    key_bufs.reserve(kNumKeys);
    std::vector<Slice> keys;
    keys.reserve(kNumKeys);
    for (int i = 0; i < kNumKeys; ++i) {
      key_bufs.emplace_back(Key(i));
      keys.emplace_back(key_bufs[i]);
    }
    std::vector<std::string> values;
    std::vector<Status> statuses =
        db_->MultiGet(GetReadOptions(), keys, &values);
    for (int i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
    }
  }
  expected += kNumKeys;
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, Iterator) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  std::unique_ptr<Iterator> iter(db_->NewIterator(GetReadOptions()));
  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  int expected = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ++expected;
    ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
  }

  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    // When `use_block_cache_ == true`, the reverse scan will access the blocks
    // loaded to cache during the above forward scan, in which case no further
    // file reads are expected.
    if (!use_block_cache_) {
      ++expected;
    }
  }
  ASSERT_OK(iter->status());
  // Reverse scan does not read evenly (one block per iteration) due to
  // descending seqno ordering, so wait until after the loop to check total.
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, VerifyChecksum) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  ASSERT_OK(db_->VerifyChecksum(GetReadOptions()));
  // In BufferedIO,
  // there are 7 reads per file, each of which will be rate-limited.
  // During open:  read footer, meta index block, properties block, index block.
  // During actual checksum verification: read meta index block, verify checksum
  // in meta blocks and verify checksum in file blocks.
  //
  // In DirectIO, where we support tail prefetching, during table open, we only
  // do 1 read instead of 4 as described above. Actual checksum verification
  // reads stay the same.
#ifdef OS_WIN
  // No file system prefetch implemented for OS Win. During table open,
  // we only do 1 read for BufferedIO.
  int num_read_per_file = 4;
#else
  int num_read_per_file = (!use_direct_io_) ? 7 : 4;
#endif
  int expected = kNumFiles * num_read_per_file;

  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

TEST_P(DBRateLimiterOnReadTest, VerifyFileChecksums) {
  if (use_direct_io_ && !IsDirectIOSupported()) {
    return;
  }
  Init();

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  ASSERT_OK(db_->VerifyFileChecksums(GetReadOptions()));
  // The files are tiny so there should have just been one read per file.
  int expected = kNumFiles;
  ASSERT_EQ(expected, options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

class DBRateLimiterOnWriteTest : public DBTestBase {
 public:
  explicit DBRateLimiterOnWriteTest()
      : DBTestBase("db_rate_limiter_on_write_test", /*env_do_fsync=*/false) {}

  void Init() {
    options_ = GetOptions();
    ASSERT_OK(TryReopenWithColumnFamilies({"default"}, options_));
    Random rnd(301);
    for (int i = 0; i < kNumFiles; i++) {
      ASSERT_OK(Put(0, kStartKey, rnd.RandomString(2)));
      ASSERT_OK(Put(0, kEndKey, rnd.RandomString(2)));
      ASSERT_OK(Flush(0));
    }
  }

  Options GetOptions() {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    options.rate_limiter.reset(NewGenericRateLimiter(
        1 << 20 /* rate_bytes_per_sec */, 100 * 1000 /* refill_period_us */,
        10 /* fairness */, RateLimiter::Mode::kWritesOnly));
    options.table_factory.reset(
        NewBlockBasedTableFactory(BlockBasedTableOptions()));
    return options;
  }

 protected:
  inline const static int64_t kNumFiles = 3;
  inline const static std::string kStartKey = "a";
  inline const static std::string kEndKey = "b";
  Options options_;
};

TEST_F(DBRateLimiterOnWriteTest, Flush) {
  std::int64_t prev_total_request = 0;

  Init();

  std::int64_t actual_flush_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
      prev_total_request;
  std::int64_t exepcted_flush_request = kNumFiles;
  EXPECT_EQ(actual_flush_request, exepcted_flush_request);
  EXPECT_EQ(actual_flush_request,
            options_.rate_limiter->GetTotalRequests(Env::IO_HIGH));
}

TEST_F(DBRateLimiterOnWriteTest, Compact) {
  Init();

  // Pre-comaction:
  // level-0 : `kNumFiles` SST files overlapping on [kStartKey, kEndKey]
  std::string files_per_level_pre_compaction = std::to_string(kNumFiles);
  ASSERT_EQ(files_per_level_pre_compaction, FilesPerLevel(0 /* cf */));

  std::int64_t prev_total_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL);
  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_LOW));

  Compact(kStartKey, kEndKey);

  std::int64_t actual_compaction_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
      prev_total_request;

  // Post-comaction:
  // level-0 : 0 SST file
  // level-1 : 1 SST file
  std::string files_per_level_post_compaction = "0,1";
  ASSERT_EQ(files_per_level_post_compaction, FilesPerLevel(0 /* cf */));

  std::int64_t exepcted_compaction_request = 1;
  EXPECT_EQ(actual_compaction_request, exepcted_compaction_request);
  EXPECT_EQ(actual_compaction_request,
            options_.rate_limiter->GetTotalRequests(Env::IO_LOW));
}

class DBRateLimiterOnWriteWALTest
    : public DBRateLimiterOnWriteTest,
      public ::testing::WithParamInterface<std::tuple<
          bool /* WriteOptions::disableWal */,
          bool /* Options::manual_wal_flush */,
          Env::IOPriority /* WriteOptions::rate_limiter_priority */>> {
 public:
  static std::string GetTestNameSuffix(
      ::testing::TestParamInfo<std::tuple<bool, bool, Env::IOPriority>> info) {
    std::ostringstream oss;
    if (std::get<0>(info.param)) {
      oss << "DisableWAL";
    } else {
      oss << "EnableWAL";
    }
    if (std::get<1>(info.param)) {
      oss << "_ManualWALFlush";
    } else {
      oss << "_AutoWALFlush";
    }
    if (std::get<2>(info.param) == Env::IO_USER) {
      oss << "_RateLimitAutoWALFlush";
    } else if (std::get<2>(info.param) == Env::IO_TOTAL) {
      oss << "_NoRateLimitAutoWALFlush";
    } else {
      oss << "_RateLimitAutoWALFlushWithIncorrectPriority";
    }
    return oss.str();
  }

  explicit DBRateLimiterOnWriteWALTest()
      : disable_wal_(std::get<0>(GetParam())),
        manual_wal_flush_(std::get<1>(GetParam())),
        rate_limiter_priority_(std::get<2>(GetParam())) {}

  void Init() {
    options_ = GetOptions();
    options_.manual_wal_flush = manual_wal_flush_;
    Reopen(options_);
  }

  WriteOptions GetWriteOptions() {
    WriteOptions write_options;
    write_options.disableWAL = disable_wal_;
    write_options.rate_limiter_priority = rate_limiter_priority_;
    return write_options;
  }

 protected:
  bool disable_wal_;
  bool manual_wal_flush_;
  Env::IOPriority rate_limiter_priority_;
};

INSTANTIATE_TEST_CASE_P(
    DBRateLimiterOnWriteWALTest, DBRateLimiterOnWriteWALTest,
    ::testing::Values(std::make_tuple(false, false, Env::IO_TOTAL),
                      std::make_tuple(false, false, Env::IO_USER),
                      std::make_tuple(false, false, Env::IO_HIGH),
                      std::make_tuple(false, true, Env::IO_USER),
                      std::make_tuple(true, false, Env::IO_USER)),
    DBRateLimiterOnWriteWALTest::GetTestNameSuffix);

TEST_P(DBRateLimiterOnWriteWALTest, AutoWalFlush) {
  Init();

  const bool no_rate_limit_auto_wal_flush =
      (rate_limiter_priority_ == Env::IO_TOTAL);
  const bool valid_arg = (rate_limiter_priority_ == Env::IO_USER &&
                          !disable_wal_ && !manual_wal_flush_);

  std::int64_t prev_total_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL);
  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_USER));

  Status s = Put("foo", "v1", GetWriteOptions());

  if (no_rate_limit_auto_wal_flush || valid_arg) {
    EXPECT_TRUE(s.ok());
  } else {
    EXPECT_TRUE(s.IsInvalidArgument());
    EXPECT_TRUE(s.ToString().find("WriteOptions::rate_limiter_priority") !=
                std::string::npos);
  }

  std::int64_t actual_auto_wal_flush_request =
      options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
      prev_total_request;
  std::int64_t expected_auto_wal_flush_request = valid_arg ? 1 : 0;

  EXPECT_EQ(actual_auto_wal_flush_request, expected_auto_wal_flush_request);
  EXPECT_EQ(actual_auto_wal_flush_request,
            options_.rate_limiter->GetTotalRequests(Env::IO_USER));
}

class DBRateLimiterOnManualWALFlushTest
    : public DBRateLimiterOnWriteTest,
      public ::testing::WithParamInterface<Env::IOPriority> {
 public:
  static std::string GetTestNameSuffix(
      ::testing::TestParamInfo<Env::IOPriority> info) {
    std::ostringstream oss;
    if (info.param == Env::IO_USER) {
      oss << "RateLimitManualWALFlush";
    } else if (info.param == Env::IO_TOTAL) {
      oss << "NoRateLimitManualWALFlush";
    } else if (info.param == Env::IO_HIGH) {
      oss << "RateLimitManualWALFlushWithHighPriority";
    } else {
      oss << "RateLimitManualWALFlushWithLowPriority";
    }
    return oss.str();
  }

  explicit DBRateLimiterOnManualWALFlushTest()
      : rate_limiter_priority_(GetParam()) {}

  void Init() {
    options_ = GetOptions();
    // Enable manual WAL flush mode
    options_.manual_wal_flush = true;
    Reopen(options_);
  }

  WriteOptions GetWriteOptions() {
    WriteOptions write_options;
    // WAL must be enabled for manual WAL flush to work
    write_options.disableWAL = false;
    // In manual WAL flush mode, WAL write rate limiting should be done through
    // FlushWAL(), not WriteOptions::rate_limiter_priority
    write_options.rate_limiter_priority = Env::IO_TOTAL;
    return write_options;
  }

 protected:
  Env::IOPriority rate_limiter_priority_;
};

INSTANTIATE_TEST_CASE_P(DBRateLimiterOnManualWALFlushTest,
                        DBRateLimiterOnManualWALFlushTest,
                        ::testing::Values(Env::IO_TOTAL, Env::IO_USER,
                                          Env::IO_HIGH, Env::IO_LOW),
                        DBRateLimiterOnManualWALFlushTest::GetTestNameSuffix);

TEST_P(DBRateLimiterOnManualWALFlushTest, ManualWALFlush) {
  Init();

  const bool no_rate_limit = (rate_limiter_priority_ == Env::IO_TOTAL);

  ASSERT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL));

  for (bool sync : {false, true}) {
    std::int64_t prev_total_request =
        options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL);

    Status put_status = Put("key_" + std::to_string(sync),
                            "value_" + std::to_string(sync), GetWriteOptions());

    EXPECT_TRUE(put_status.ok());

    // Since manual_wal_flush is enabled and write_options.rate_limiter_priority
    // is IO_TOTAL, no rate limiting should have occurred for this user write
    EXPECT_EQ(0, options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
                     prev_total_request);

    // Now explicitly flush the WAL with the test's rate_limiter_priority
    prev_total_request = options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL);
    std::int64_t prev_priority_request =
        options_.rate_limiter->GetTotalRequests(rate_limiter_priority_);

    FlushWALOptions flush_options;
    flush_options.sync = sync;
    flush_options.rate_limiter_priority = rate_limiter_priority_;
    Status flush_status = db_->FlushWAL(flush_options);

    EXPECT_TRUE(flush_status.ok());

    std::int64_t manual_wal_flush_requests_total =
        options_.rate_limiter->GetTotalRequests(Env::IO_TOTAL) -
        prev_total_request;
    std::int64_t manual_wal_flush_requests_for_priority =
        options_.rate_limiter->GetTotalRequests(rate_limiter_priority_) -
        prev_priority_request;

    if (no_rate_limit) {
      EXPECT_EQ(0, manual_wal_flush_requests_total);
      EXPECT_EQ(0, manual_wal_flush_requests_for_priority);
    } else {
      EXPECT_EQ(manual_wal_flush_requests_total,
                manual_wal_flush_requests_for_priority);
      EXPECT_GT(manual_wal_flush_requests_for_priority, 0);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
