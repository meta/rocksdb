// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/sst_file_reader.h"

#include <atomic>
#include <cinttypes>
#include <cstring>
#include <memory>
#include <utility>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "env/composite_env_wrapper.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/listener.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/utilities/types_util.h"
#include "rocksdb/wide_columns.h"
#include "table/block_based/block.h"
#include "table/embedded_blob_sst.h"
#include "table/format.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/multiget_context.h"
#include "table/sst_file_writer_collectors.h"
#include "table/table_reader.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/compression.h"
#include "util/defer.h"
#include "utilities/merge_operators.h"

#if USE_COROUTINES
#include "folly/coro/BlockingWait.h"
#include "rocksdb/coro_db.h"
#endif

namespace ROCKSDB_NAMESPACE {

std::string EncodeAsString(uint64_t v) {
  char buf[16];
  snprintf(buf, sizeof(buf), "%08" PRIu64, v);
  return std::string(buf);
}

std::string EncodeAsUint64(uint64_t v) {
  std::string dst;
  PutFixed64(&dst, v);
  return dst;
}

class SstFileReaderTest : public testing::Test {
 public:
  SstFileReaderTest() {
    options_.merge_operator = MergeOperators::CreateUInt64AddOperator();
    sst_name_ = test::PerThreadDBPath("sst_file");

    Env* base_env = Env::Default();
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &base_env, &env_guard_));
    EXPECT_NE(nullptr, base_env);
    env_ = base_env;
    options_.env = env_;
  }

  ~SstFileReaderTest() override {
    if (env_->FileExists(sst_name_).ok()) {
      EXPECT_OK(env_->DeleteFile(sst_name_));
    }
  }

  void CreateFile(const std::string& file_name,
                  const std::vector<std::string>& keys) {
    SstFileWriter writer(soptions_, options_);
    ASSERT_OK(writer.Open(file_name));
    for (size_t i = 0; i + 2 < keys.size(); i += 3) {
      ASSERT_OK(writer.Put(keys[i], keys[i]));
      ASSERT_OK(writer.Merge(keys[i + 1], EncodeAsUint64(i + 1)));
      ASSERT_OK(writer.Delete(keys[i + 2]));
    }
    ASSERT_OK(writer.Finish());
  }

  void CheckFile(const std::string& file_name,
                 const std::vector<std::string>& keys,
                 bool check_global_seqno = false) {
    ReadOptions ropts;
    SstFileReader reader(options_);
    ASSERT_OK(reader.Open(file_name));
    ASSERT_OK(reader.VerifyChecksum());
    std::unique_ptr<Iterator> iter(reader.NewIterator(ropts));
    iter->SeekToFirst();
    for (size_t i = 0; i + 2 < keys.size(); i += 3) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(keys[i]), 0);
      ASSERT_EQ(iter->value().compare(keys[i]), 0);
      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(keys[i + 1]), 0);
      ASSERT_EQ(iter->value().compare(EncodeAsUint64(i + 1)), 0);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    if (check_global_seqno) {
      auto properties = reader.GetTableProperties();
      ASSERT_TRUE(properties);
      std::string hostname;
      ASSERT_OK(env_->GetHostNameString(&hostname));
      ASSERT_EQ(properties->db_host_id, hostname);
      auto& user_properties = properties->user_collected_properties;
      ASSERT_TRUE(
          user_properties.count(ExternalSstFilePropertyNames::kGlobalSeqno));
    }
  }

  void CreateFileAndCheck(const std::vector<std::string>& keys) {
    CreateFile(sst_name_, keys);
    CheckFile(sst_name_, keys);
  }

  void GetEmbeddedBlobStats(SstFileReader* reader, EmbeddedBlobStats* stats) {
    ASSERT_NE(reader, nullptr);
    ASSERT_NE(stats, nullptr);

    std::shared_ptr<const TableProperties> properties =
        reader->GetTableProperties();
    ASSERT_NE(properties, nullptr);
    const auto& user_properties = properties->user_collected_properties;
    auto stats_iter = user_properties.find(kEmbeddedBlobSstStatsPropertyName);
    ASSERT_NE(stats_iter, user_properties.end());
    ASSERT_OK(DecodeEmbeddedBlobStats(Slice(stats_iter->second), stats));
  }

  void ReadFileBytes(const std::string& file_name, uint64_t offset, size_t size,
                     std::string* bytes) {
    ASSERT_NE(bytes, nullptr);

    std::unique_ptr<FSRandomAccessFile> file;
    ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(
        file_name, FileOptions(), &file, nullptr));
    std::unique_ptr<RandomAccessFileReader> file_reader;
    file_reader.reset(new RandomAccessFileReader(std::move(file), file_name));

    std::string scratch(size, '\0');
    Slice result;
    ASSERT_OK(file_reader->Read(offset, size, IOOptions(), &result,
                                scratch.data(), nullptr));
    ASSERT_EQ(result.size(), size);
    bytes->assign(result.data(), result.size());
  }

 protected:
  Options options_;
  EnvOptions soptions_;
  std::string sst_name_;
  std::shared_ptr<Env> env_guard_;
  Env* env_;
};

#if USE_COROUTINES
TEST_F(SstFileReaderTest, CoroutinePointReads) {
  const std::vector<std::string> keys = {"a", "b", "c", "d", "e", "f"};
  CreateFile(sst_name_, keys);

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));

  int coroutine_read_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadCoroutine:SubmitReadAsync",
      [&](void*) { ++coroutine_read_count; });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions read_options;
  read_options.fill_cache = false;
  std::string string_value;
  ASSERT_OK(folly::coro::blockingWait(
      CoroDB::CoGet(&reader, read_options, keys[0], &string_value)));
  ASSERT_EQ(string_value, keys[0]);

  PinnableSlice pinnable_value;
  ASSERT_OK(folly::coro::blockingWait(
      CoroDB::CoGet(&reader, read_options, keys[3], &pinnable_value)));
  ASSERT_EQ(pinnable_value, keys[3]);

  const std::vector<Slice> read_keys = {keys[0], keys[2], "missing"};
  std::vector<std::string> string_values;
  std::vector<Status> statuses = folly::coro::blockingWait(
      CoroDB::CoMultiGet(&reader, read_options, read_keys, &string_values));
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(string_values[0], keys[0]);
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_TRUE(statuses[2].IsNotFound());

  std::vector<PinnableSlice> pinnable_values;
  statuses = folly::coro::blockingWait(
      CoroDB::CoMultiGet(&reader, read_options, read_keys, &pinnable_values));
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(pinnable_values[0], keys[0]);
  ASSERT_TRUE(statuses[1].IsNotFound());
  ASSERT_TRUE(statuses[2].IsNotFound());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  if (options_.env->GetFileSystem()->GetReadExecutor() != nullptr) {
    ASSERT_GT(coroutine_read_count, 0);
  }
}

TEST_F(SstFileReaderTest, CoroutinePointReadFallback) {
  class NoReadExecutorFileSystem final : public FileSystemWrapper {
   public:
    explicit NoReadExecutorFileSystem(const std::shared_ptr<FileSystem>& target)
        : FileSystemWrapper(target) {}

    const char* Name() const override { return "NoReadExecutorFileSystem"; }
    folly::IOExecutor* GetReadExecutor() override { return nullptr; }
    void SetReadIOExecutorThreads(int /*number*/) override {}
  };

  const std::vector<std::string> keys = {"a", "b", "c"};
  CreateFile(sst_name_, keys);

  auto file_system =
      std::make_shared<NoReadExecutorFileSystem>(env_->GetFileSystem());
  std::unique_ptr<Env> env = NewCompositeEnv(file_system);
  Options options = options_;
  options.env = env.get();
  SstFileReader reader(options);
  ASSERT_OK(reader.Open(sst_name_));

  int coroutine_read_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadCoroutine:SubmitReadAsync",
      [&](void*) { ++coroutine_read_count; });
  SyncPoint::GetInstance()->EnableProcessing();

  std::string value;
  ASSERT_OK(folly::coro::blockingWait(
      CoroDB::CoGet(&reader, ReadOptions(), keys[0], &value)));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(value, keys[0]);
  ASSERT_EQ(coroutine_read_count, 0);
}
#endif  // USE_COROUTINES

TEST_F(SstFileReaderTest, MultiGetBatchBoundaries) {
  constexpr size_t kNumKeys = MultiGetContext::MAX_BATCH_SIZE * 2 + 1;
  std::vector<std::string> key_storage;
  key_storage.reserve(kNumKeys);

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.Open(sst_name_));
  for (size_t i = 0; i < kNumKeys; ++i) {
    key_storage.emplace_back(EncodeAsString(i));
    ASSERT_OK(writer.Put(key_storage.back(), key_storage.back()));
  }
  ASSERT_OK(writer.Finish());

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));

  std::vector<Slice> keys;
  std::vector<std::string> expected_values;
  keys.reserve(key_storage.size());
  expected_values.reserve(key_storage.size());
  for (size_t i = 0; i < kNumKeys; ++i) {
    const size_t key_index = (i * 17) % kNumKeys;
    keys.emplace_back(key_storage[key_index]);
    expected_values.emplace_back(key_storage[key_index]);
  }

  std::vector<std::string> values;
  std::vector<Status> statuses = reader.MultiGet(ReadOptions(), keys, &values);
  ASSERT_EQ(statuses.size(), kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
  }
  ASSERT_EQ(values, expected_values);

  statuses = reader.MultiGet(ReadOptions(), {}, &values);
  ASSERT_TRUE(statuses.empty());
  ASSERT_TRUE(values.empty());

#if USE_COROUTINES
  statuses = folly::coro::blockingWait(
      CoroDB::CoMultiGet(&reader, ReadOptions(), keys, &values));
  ASSERT_EQ(statuses.size(), kNumKeys);
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(statuses[i]);
  }
  ASSERT_EQ(values, expected_values);

  statuses = folly::coro::blockingWait(
      CoroDB::CoMultiGet(&reader, ReadOptions(), {}, &values));
  ASSERT_TRUE(statuses.empty());
  ASSERT_TRUE(values.empty());
#endif  // USE_COROUTINES
}

class FailingAppendWritableFile : public FSWritableFileOwnerWrapper {
 public:
  FailingAppendWritableFile(std::unique_ptr<FSWritableFile>&& target,
                            std::atomic<bool>* fail_writes)
      : FSWritableFileOwnerWrapper(std::move(target)),
        fail_writes_(fail_writes) {}

  IOStatus Append(const Slice& data, const IOOptions& options,
                  IODebugContext* dbg) override {
    if (fail_writes_->load()) {
      return IOStatus::IOError("Injected embedded blob append failure");
    }
    return FSWritableFileOwnerWrapper::Append(data, options, dbg);
  }

  IOStatus Append(const Slice& data, const IOOptions& options,
                  const DataVerificationInfo& verification_info,
                  IODebugContext* dbg) override {
    if (fail_writes_->load()) {
      return IOStatus::IOError("Injected embedded blob append failure");
    }
    return FSWritableFileOwnerWrapper::Append(data, options, verification_info,
                                              dbg);
  }

 private:
  std::atomic<bool>* fail_writes_;
};

class FailingAppendFileSystem : public FileSystemWrapper {
 public:
  explicit FailingAppendFileSystem(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  const char* Name() const override { return "FailingAppendFileSystem"; }

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    IOStatus s = target()->NewWritableFile(fname, file_opts, result, dbg);
    if (s.ok()) {
      result->reset(
          new FailingAppendWritableFile(std::move(*result), &fail_writes_));
    }
    return s;
  }

  void SetFailWrites(bool fail_writes) { fail_writes_.store(fail_writes); }

 private:
  std::atomic<bool> fail_writes_{false};
};

const uint64_t kNumKeys = 100;

TEST_F(SstFileReaderTest, Basic) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsString(i));
  }
  CreateFileAndCheck(keys);
}

TEST_F(SstFileReaderTest, ReportsFileReads) {
  class CountingFileReadListener : public EventListener {
   public:
    bool ShouldBeNotifiedOnFileIO() override { return true; }

    void OnFileReadFinish(const FileOperationInfo& info) override {
      if (info.status.ok()) {
        read_count_.fetch_add(1);
        read_bytes_.fetch_add(info.length);
      }
    }

    uint64_t read_count() const { return read_count_.load(); }
    uint64_t read_bytes() const { return read_bytes_.load(); }

   private:
    std::atomic<uint64_t> read_count_{0};
    std::atomic<uint64_t> read_bytes_{0};
  };

  CreateFile(sst_name_, {"a", "b", "c"});

  options_.statistics = CreateDBStatistics();
  options_.statistics->set_stats_level(StatsLevel::kAll);
  auto listener = std::make_shared<CountingFileReadListener>();
  options_.listeners.emplace_back(listener);

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));

  EXPECT_GT(options_.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT), 0);
  EXPECT_GT(options_.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES), 0);
  HistogramData sst_read_micros;
  options_.statistics->histogramData(SST_READ_MICROS, &sst_read_micros);
  EXPECT_GT(sst_read_micros.count, 0);
  EXPECT_GT(listener->read_count(), 0);
  EXPECT_GT(listener->read_bytes(), 0);
}

TEST_F(SstFileReaderTest, MultiGetExceedingMaxBatchSize) {
  // A MultiGetContext holds at most MAX_BATCH_SIZE keys, so a larger request
  // has to be split into batches. Query the keys in descending order and mix
  // in absent ones so that results have to survive the sort back into the
  // caller's order.
  const size_t num_keys = MultiGetContext::MAX_BATCH_SIZE * 2 + 5;

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.Open(sst_name_));
  for (size_t i = 0; i < num_keys; ++i) {
    ASSERT_OK(writer.Put(EncodeAsString(i), "val" + std::to_string(i)));
  }
  ASSERT_OK(writer.Finish());

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));

  std::vector<std::string> key_storage;
  std::vector<std::string> expected_values;
  std::vector<bool> expected_found;
  for (size_t i = num_keys; i > 0; --i) {
    key_storage.emplace_back(EncodeAsString(i - 1));
    expected_values.emplace_back("val" + std::to_string(i - 1));
    expected_found.push_back(true);

    key_storage.emplace_back("absent" + std::to_string(i - 1));
    // A key that is not in the file leaves its output slot untouched.
    expected_values.emplace_back("");
    expected_found.push_back(false);
  }
  std::vector<Slice> keys(key_storage.begin(), key_storage.end());

  auto found_flags = [](const std::vector<Status>& statuses) {
    std::vector<bool> found;
    found.reserve(statuses.size());
    for (const Status& s : statuses) {
      EXPECT_TRUE(s.ok() || s.IsNotFound()) << s.ToString();
      found.push_back(s.ok());
    }
    return found;
  };

  std::vector<std::string> values;
  std::vector<Status> statuses = reader.MultiGet(ReadOptions(), keys, &values);
  EXPECT_EQ(found_flags(statuses), expected_found);
  EXPECT_EQ(values, expected_values);

  std::vector<PinnableSlice> pinnable_values;
  std::vector<Status> pinnable_statuses =
      reader.MultiGet(ReadOptions(), keys, &pinnable_values);
  std::vector<std::string> pinnable_as_strings;
  pinnable_as_strings.reserve(pinnable_values.size());
  for (const PinnableSlice& value : pinnable_values) {
    pinnable_as_strings.emplace_back(value.data(), value.size());
  }
  EXPECT_EQ(found_flags(pinnable_statuses), expected_found);
  EXPECT_EQ(pinnable_as_strings, expected_values);
}

TEST_F(SstFileReaderTest, EmbeddedBlobRoundTrip) {
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 6;
  const std::string embedded_value(4096, 'v');
  const std::string embedded_default_column_value(4096, 'd');

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
  ASSERT_OK(writer.Put("a", "tiny"));
  ASSERT_OK(writer.Put("b", embedded_value));
  ASSERT_OK(writer.PutEntity(
      "c", {{kDefaultWideColumnName, embedded_default_column_value},
            {"meta", "x"}}));

  ExternalSstFileInfo file_info;
  ASSERT_OK(writer.Finish(&file_info));
  ASSERT_GT(file_info.file_size, 0);

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  ASSERT_OK(reader.VerifyChecksum());

  std::string value;
  ASSERT_OK(reader.Get(ReadOptions(), "a", &value));
  ASSERT_EQ(value, "tiny");
  ASSERT_OK(reader.Get(ReadOptions(), "b", &value));
  ASSERT_EQ(value, embedded_value);
  ASSERT_OK(reader.Get(ReadOptions(), "c", &value));
  ASSERT_EQ(value, embedded_default_column_value);

  std::vector<std::string> key_storage = {"a", "b", "c"};
  std::vector<Slice> keys;
  keys.reserve(key_storage.size());
  for (const std::string& key : key_storage) {
    keys.emplace_back(key);
  }
  std::vector<std::string> values;
  std::vector<Status> statuses = reader.MultiGet(ReadOptions(), keys, &values);
  ASSERT_EQ(statuses.size(), keys.size());
  ASSERT_EQ(values.size(), keys.size());
  for (const Status& status : statuses) {
    ASSERT_OK(status);
  }
  EXPECT_EQ(values[0], "tiny");
  EXPECT_EQ(values[1], embedded_value);
  EXPECT_EQ(values[2], embedded_default_column_value);

  std::unique_ptr<Iterator> iter(reader.NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "a");
  EXPECT_EQ(iter->value(), "tiny");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "b");
  EXPECT_EQ(iter->value(), embedded_value);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "c");
  EXPECT_EQ(iter->value(), embedded_default_column_value);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Embedded-blob SSTs disable index value delta encoding so blob records can
  // be written interleaved with data blocks.
  std::shared_ptr<const TableProperties> props = reader.GetTableProperties();
  ASSERT_NE(props, nullptr);
  EXPECT_EQ(props->index_value_is_delta_encoded, 0);
  ASSERT_TRUE(props->referenced_blob_bytes.has_value());
  // Embedded blob records live in the SST and do not consume external blob
  // file space.
  EXPECT_EQ(*props->referenced_blob_bytes, 0U);

  EmbeddedBlobStats stats;
  GetEmbeddedBlobStats(&reader, &stats);
  EXPECT_EQ(stats.blob_count, 2);
  EXPECT_GT(stats.payload_bytes, 0);
}

// Invariant: the internal block-based table iterator must never expose an
// unresolved same-file kTypeBlobIndex internal key through key(). With
// index_type=kBinarySearchWithFirstKey and allow_unprepared_value, the iterator
// can sit in the is_at_first_key_from_index_ state and return the raw first key
// from the index (type kTypeBlobIndex) while value() resolves the same-file
// blob to its plain payload. DBIter happens to tolerate this (it re-parses the
// key after PrepareValue), but other internal-iterator consumers must not see
// an unresolved same-file blob index. Forcing one entry per data block makes
// every embedded blob the first key of a block, exercising the deferred path on
// SeekToFirst, forward block transitions, and Seek.
TEST_F(SstFileReaderTest, EmbeddedBlobIteratorKeyTypeWithFirstKeyIndex) {
  BlockBasedTableOptions bbto;
  bbto.index_type =
      BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
  // Soft limit of 1 byte starts a new data block after every entry.
  bbto.block_size = 1;
  options_.table_factory.reset(NewBlockBasedTableFactory(bbto));

  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;
  const std::string big1(1024, 'x');
  const std::string big2(1024, 'y');
  const std::string big3(1024, 'z');

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.Put("k2", big2));
  ASSERT_OK(writer.Put("k3", big3));
  ASSERT_OK(writer.Finish());

  // Open a BlockBasedTable reader directly so we can request an internal
  // iterator with allow_unprepared_value=true. The public SstFileReader
  // iterator uses allow_unprepared_value=false, which never defers.
  ImmutableOptions ioptions(options_);
  MutableCFOptions moptions(options_);
  InternalKeyComparator icomp(options_.comparator);

  uint64_t file_size = 0;
  ASSERT_OK(env_->GetFileSize(sst_name_, &file_size));

  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(sst_name_, FileOptions(),
                                                       &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(file), sst_name_));

  ReadOptions read_opts;
  read_opts.verify_checksums = true;
  TableReaderOptions table_reader_options(
      ioptions, moptions.prefix_extractor, moptions.compression_manager.get(),
      soptions_, icomp, /*block_protection_bytes_per_key=*/0);
  std::unique_ptr<TableReader> table_reader;
  ASSERT_OK(options_.table_factory->NewTableReader(
      read_opts, table_reader_options, std::move(file_reader), file_size,
      &table_reader, /*prefetch_index_and_filter_in_cache=*/true));

  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      read_opts, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kUncategorized,
      /*compaction_readahead_size=*/0, /*allow_unprepared_value=*/true));

  auto check_current = [&](const std::string& user_key,
                           const std::string& value) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    // key() must already present the resolved value type, never the raw
    // same-file blob index type.
    ParsedInternalKey parsed;
    ASSERT_OK(ParseInternalKey(iter->key(), &parsed, /*log_err_key=*/true));
    EXPECT_EQ(parsed.user_key, user_key);
    EXPECT_NE(parsed.type, kTypeBlobIndex);
    EXPECT_EQ(parsed.type, kTypeValue);
    ASSERT_TRUE(iter->PrepareValue());
    EXPECT_EQ(iter->value(), value);
  };

  const std::vector<std::pair<std::string, std::string>> expected = {
      {"k1", big1}, {"k2", big2}, {"k3", big3}};

  size_t idx = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++idx) {
    ASSERT_LT(idx, expected.size());
    check_current(expected[idx].first, expected[idx].second);
  }
  ASSERT_OK(iter->status());
  EXPECT_EQ(idx, expected.size());

  // Seek directly onto a block whose first key is an embedded blob.
  InternalKey seek_target("k2", kMaxSequenceNumber, kValueTypeForSeek);
  iter->Seek(seek_target.Encode());
  check_current("k2", big2);
}

// Regression test for a use-after-free in EmbeddedBlobResolvingIterator:
// calling key() followed by value() on the same entry must not invalidate the
// Slice previously returned by key(). The bug was that MaterializeValue()
// (triggered by value()) could overwrite resolved_internal_key_ via
// move-assignment, freeing the buffer that the key() Slice pointed to.
TEST_F(SstFileReaderTest, EmbeddedBlobKeyStableAcrossValueCall) {
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;
  const std::string big_value(4096, 'v');

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
  ASSERT_OK(writer.Put("key1", big_value));
  ASSERT_OK(writer.Put("key2", big_value));
  ASSERT_OK(writer.Finish());

  // Open a BlockBasedTable reader directly to get an InternalIterator,
  // mimicking what CompactionIterator does.
  ImmutableOptions ioptions(options_);
  MutableCFOptions moptions(options_);
  InternalKeyComparator icomp(options_.comparator);

  uint64_t file_size = 0;
  ASSERT_OK(env_->GetFileSize(sst_name_, &file_size));

  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(sst_name_, FileOptions(),
                                                       &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> file_reader(
      new RandomAccessFileReader(std::move(file), sst_name_));

  ReadOptions read_opts;
  TableReaderOptions table_reader_options(
      ioptions, moptions.prefix_extractor, moptions.compression_manager.get(),
      soptions_, icomp, /*block_protection_bytes_per_key=*/0);
  std::unique_ptr<TableReader> table_reader;
  ASSERT_OK(options_.table_factory->NewTableReader(
      read_opts, table_reader_options, std::move(file_reader), file_size,
      &table_reader, /*prefetch_index_and_filter_in_cache=*/true));

  // Use allow_unprepared_value=false so values are always materialized.
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      read_opts, moptions.prefix_extractor.get(), /*arena=*/nullptr,
      /*skip_filters=*/false, TableReaderCaller::kCompaction));

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());

  // Mimic CompactionIterator: save key Slice, then call value().
  Slice saved_key = iter->key();
  // Copy the key data for comparison (before value() potentially invalidates).
  std::string expected_key(saved_key.data(), saved_key.size());

  // Calling value() triggers MaterializeValue(). Before the fix, this would
  // free the buffer backing saved_key.
  Slice val = iter->value();
  ASSERT_EQ(val, big_value);

  // Verify the key Slice is still valid (not pointing to freed memory).
  // Under ASAN, this would catch a heap-use-after-free without the fix.
  ParsedInternalKey parsed;
  ASSERT_OK(ParseInternalKey(saved_key, &parsed, /*log_err_key=*/true));
  EXPECT_EQ(parsed.user_key, "key1");
  EXPECT_EQ(parsed.type, kTypeValue);

  // Also verify the key data hasn't changed.
  EXPECT_EQ(Slice(expected_key), saved_key);

  // Advance and repeat for the second entry.
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  saved_key = iter->key();
  val = iter->value();
  ASSERT_EQ(val, big_value);
  ASSERT_OK(ParseInternalKey(saved_key, &parsed, /*log_err_key=*/true));
  EXPECT_EQ(parsed.user_key, "key2");
  EXPECT_EQ(parsed.type, kTypeValue);
}

TEST_F(SstFileReaderTest, EmbeddedBlobRequiresFormatVersion7) {
  Options options = options_;
  BlockBasedTableOptions table_options;
  table_options.format_version = 6;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  SstFileWriter writer(soptions_, options);
  Status s = writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(SstFileReaderTest, EmbeddedBlobCompressionOptionsIgnored) {
  const std::string value(16 * 1024, 'x');

  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 1;
  embedded_blob_options.compression_type = kSnappyCompression;
  embedded_blob_options.compression_options.SetMinRatio(100.0);

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
  ASSERT_OK(writer.Put("a", value));
  ASSERT_OK(writer.Finish());

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));

  std::string read_value;
  ASSERT_OK(reader.Get(ReadOptions(), "a", &read_value));
  ASSERT_EQ(read_value, value);

  EmbeddedBlobStats stats;
  GetEmbeddedBlobStats(&reader, &stats);
  EXPECT_EQ(stats.blob_count, 1);
  EXPECT_EQ(stats.payload_bytes, value.size());
}

TEST_F(SstFileReaderTest, EmbeddedBlobDefaultMinBlobSize) {
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  const std::string small_value(2047, 's');
  const std::string large_value(2048, 'l');

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
  ASSERT_OK(writer.Put("a", small_value));
  ASSERT_OK(writer.Put("b", large_value));
  ASSERT_OK(writer.Finish());

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));

  std::string read_value;
  ASSERT_OK(reader.Get(ReadOptions(), "a", &read_value));
  ASSERT_EQ(read_value, small_value);
  ASSERT_OK(reader.Get(ReadOptions(), "b", &read_value));
  ASSERT_EQ(read_value, large_value);

  EmbeddedBlobStats stats;
  GetEmbeddedBlobStats(&reader, &stats);
  EXPECT_EQ(stats.blob_count, 1);
  EXPECT_EQ(stats.payload_bytes, large_value.size());
}

TEST_F(SstFileReaderTest, EmbeddedBlobInterleavedLayout) {
  // Force one entry per data block so blob records (written inline as values
  // are added) end up interleaved between data blocks rather than buffered in a
  // strict front prefix.
  BlockBasedTableOptions bbto;
  bbto.block_size = 1;
  options_.table_factory.reset(NewBlockBasedTableFactory(bbto));

  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 64;
  const std::string large0(4096, '0');
  const std::string large1(4096, '1');
  const std::string large2(4096, '2');

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
  // The first entry is small, so data block 0 is flushed (at file offset 0)
  // before any blob record is written.
  ASSERT_OK(writer.Put("a", "tiny"));
  ASSERT_OK(writer.Put("b", large0));
  ASSERT_OK(writer.Put("c", "tiny"));
  ASSERT_OK(writer.Put("d", large1));
  ASSERT_OK(writer.Put("e", "tiny"));
  ASSERT_OK(writer.Put("f", large2));
  ASSERT_OK(writer.Finish());

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  ASSERT_OK(reader.VerifyChecksum());

  // Index value delta encoding is off for embedded-blob SSTs (the mechanism
  // that allows interleaving blob records with data blocks), and there is more
  // than one data block.
  std::shared_ptr<const TableProperties> props = reader.GetTableProperties();
  ASSERT_NE(props, nullptr);
  EXPECT_EQ(props->index_value_is_delta_encoded, 0);
  EXPECT_GT(props->num_data_blocks, 1);

  // The blob records are interleaved with data blocks rather than buffered as a
  // contiguous front prefix: immediately after the first blob record sits a
  // data block, not the second blob's payload (which is what a strict prefix
  // layout would place there).
  const size_t first_record_size = large0.size() + kSimpleGen2BlobTrailerSize;
  std::string after_first_record;
  ReadFileBytes(sst_name_, first_record_size, 32, &after_first_record);
  EXPECT_NE(after_first_record, std::string(32, '1'));

  EmbeddedBlobStats stats;
  GetEmbeddedBlobStats(&reader, &stats);
  EXPECT_EQ(stats.blob_count, 3);
  EXPECT_EQ(stats.payload_bytes, large0.size() + large1.size() + large2.size());

  // All values, large and small, read back correctly.
  const std::vector<std::pair<std::string, std::string>> expected = {
      {"a", "tiny"}, {"b", large0}, {"c", "tiny"},
      {"d", large1}, {"e", "tiny"}, {"f", large2}};
  std::string value;
  for (const auto& kv : expected) {
    ASSERT_OK(reader.Get(ReadOptions(), kv.first, &value));
    EXPECT_EQ(value, kv.second);
  }

  std::unique_ptr<Iterator> iter(reader.NewIterator(ReadOptions()));
  size_t idx = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_LT(idx, expected.size());
    EXPECT_EQ(iter->key(), expected[idx].first);
    EXPECT_EQ(iter->value(), expected[idx].second);
    ++idx;
  }
  ASSERT_OK(iter->status());
  EXPECT_EQ(idx, expected.size());
}

// format_version >= 8 lets embedded-blob SSTs use index value-delta encoding:
// the interleaved blob records make some data blocks non-contiguous, and those
// index entries use the in-value escape (see IndexValue::EncodeTo) while the
// rest are delta encoded. This verifies correct round-trip at fv8 and that fv8
// (unlike fv7) marks the index as delta encoded, all with a > 1 index restart
// interval so deltas (and the escape) are actually produced.
TEST_F(SstFileReaderTest, EmbeddedBlobValueDeltaEscapeFv8) {
  // Writing the unpublished draft format_version 8 requires this opt-in.
  SaveAndRestore<bool> allow_draft(&TEST_AllowUnsupportedFormatVersion(), true);

  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 64;

  // Interleave small (inline) and large (embedded-blob) values so that some
  // data blocks follow a blob record (non-contiguous -> escape) and some do
  // not (contiguous -> delta).
  std::vector<std::pair<std::string, std::string>> expected;
  for (int i = 0; i < 200; ++i) {
    char keybuf[16];
    snprintf(keybuf, sizeof(keybuf), "%06d", i);
    std::string key(keybuf);
    std::string value =
        (i % 2 == 0) ? std::string(4096, static_cast<char>('a' + (i % 26)))
                     : ("tiny" + std::to_string(i));
    expected.emplace_back(std::move(key), std::move(value));
  }

  auto build_and_read = [&](uint32_t format_version, bool* index_delta_encoded,
                            int* escape_count, std::vector<std::string>* got) {
    BlockBasedTableOptions bbto;
    bbto.format_version = format_version;
    bbto.block_size = 1;                    // one entry per data block
    bbto.index_block_restart_interval = 4;  // > 1 so value delta encoding is on
    options_.table_factory.reset(NewBlockBasedTableFactory(bbto));

    // Count index entries actually written with the in-value escape, rather
    // than inferring it from the layout.
    *escape_count = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "IndexValue::EncodeTo:ValueDeltaEscape",
        [escape_count](void*) { ++*escape_count; });
    SyncPoint::GetInstance()->EnableProcessing();

    SstFileWriter writer(soptions_, options_);
    ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
    for (const auto& kv : expected) {
      ASSERT_OK(writer.Put(kv.first, kv.second));
    }
    ASSERT_OK(writer.Finish());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    SstFileReader reader(options_);
    ASSERT_OK(reader.Open(sst_name_));
    ASSERT_OK(reader.VerifyChecksum());

    std::shared_ptr<const TableProperties> props = reader.GetTableProperties();
    ASSERT_NE(props, nullptr);
    *index_delta_encoded = props->index_value_is_delta_encoded != 0;

    got->clear();
    std::unique_ptr<Iterator> iter(reader.NewIterator(ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      got->push_back(iter->value().ToString());
    }
    ASSERT_OK(iter->status());

    // Point lookups too (exercise index Seek + escape decode).
    std::string value;
    for (const auto& kv : expected) {
      ASSERT_OK(reader.Get(ReadOptions(), kv.first, &value));
      ASSERT_EQ(value, kv.second);
    }
  };

  std::vector<std::string> got7;
  std::vector<std::string> got8;
  bool delta7 = false;
  bool delta8 = false;
  int escapes7 = 0;
  int escapes8 = 0;
  build_and_read(7, &delta7, &escapes7, &got7);
  build_and_read(/*format_version=*/8, &delta8, &escapes8, &got8);

  // Reads identical across versions.
  ASSERT_EQ(static_cast<int>(got8.size()), static_cast<int>(expected.size()));
  ASSERT_EQ(got7, got8);
  std::vector<std::string> expected_values;
  for (const auto& kv : expected) {
    expected_values.push_back(kv.second);
  }
  ASSERT_EQ(got8, expected_values);

  // Escape entries were actually produced at fv8 (interleaved blob records
  // make some data blocks non-contiguous), and never at fv7.
  EXPECT_GT(escapes8, 0);
  EXPECT_EQ(escapes7, 0);
  // fv7 embedded blobs disable index value delta encoding; fv8 enables it.
  EXPECT_FALSE(delta7);
  EXPECT_TRUE(delta8);
}

TEST_F(SstFileReaderTest, EmbeddedBlobRecordCorruptionDetected) {
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 1;
  const std::string value(4096, 'v');

  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_name_, embedded_blob_options));
  // A single large value: its blob record is written first, at file offset 0,
  // so byte 100 falls within the blob payload.
  ASSERT_OK(writer.Put("a", value));
  ASSERT_OK(writer.Finish());

  // Flip a byte inside the embedded blob record's payload. The offset-keyed
  // record checksum is now the only guard against this (no range pre-check).
  {
    std::unique_ptr<RandomRWFile> rw_file;
    ASSERT_OK(env_->NewRandomRWFile(sst_name_, &rw_file, EnvOptions()));
    char scratch = 0;
    Slice chunk;
    ASSERT_OK(rw_file->Read(100, 1, &chunk, &scratch));
    ASSERT_EQ(chunk.size(), 1);
    char corrupted = static_cast<char>(chunk[0] ^ 0xff);
    ASSERT_OK(rw_file->Write(100, Slice(&corrupted, 1)));
    ASSERT_OK(rw_file->Close());
  }

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));

  std::string read_value;
  Status s = reader.Get(ReadOptions(), "a", &read_value);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(SstFileReaderTest, EmbeddedBlobAppendErrorsSurfaceEarly) {
  std::shared_ptr<FailingAppendFileSystem> fs(
      new FailingAppendFileSystem(env_->GetFileSystem()));
  std::unique_ptr<Env> failing_env(new CompositeEnvWrapper(env_, fs));

  Options options = options_;
  options.env = failing_env.get();
  EnvOptions env_options = soptions_;
  env_options.writable_file_max_buffer_size = 1;

  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 1;

  const std::string put_file = sst_name_ + "_put";
  const std::string entity_file = sst_name_ + "_entity";
  const std::string value(4096, 'v');

  {
    SstFileWriter writer(env_options, options);
    ASSERT_OK(writer.OpenWithEmbeddedBlobs(put_file, embedded_blob_options));
    fs->SetFailWrites(true);
    Status s = writer.Put("a", value);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    s = writer.Finish();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    fs->SetFailWrites(false);
  }

  {
    SstFileWriter writer(env_options, options);
    ASSERT_OK(writer.OpenWithEmbeddedBlobs(entity_file, embedded_blob_options));
    fs->SetFailWrites(true);
    Status s =
        writer.PutEntity("a", {{kDefaultWideColumnName, value}, {"meta", "m"}});
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    s = writer.Finish();
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    fs->SetFailWrites(false);
  }

  Status s = env_->DeleteFile(put_file);
  s.PermitUncheckedError();
  s = env_->DeleteFile(entity_file);
  s.PermitUncheckedError();
}

TEST_F(SstFileReaderTest, ParseTableIteratorKey) {
  // Verify that callers of the raw table iterator can decode key metadata
  // through the public SstFileReader API instead of duplicating dbformat.h.
  std::vector<std::string> keys = {EncodeAsString(0), EncodeAsString(1),
                                   EncodeAsString(2)};
  CreateFile(sst_name_, keys);

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  std::unique_ptr<Iterator> iter = reader.NewTableIterator();
  ASSERT_NE(iter, nullptr);
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ParsedEntryInfo parsed_key;
  ASSERT_OK(reader.ParseTableIteratorKey(iter->key(), &parsed_key));
  EXPECT_EQ(parsed_key.user_key, keys[0]);
  EXPECT_EQ(parsed_key.sequence, 0);
  EXPECT_EQ(parsed_key.type, kEntryPut);
  iter->Next();

  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(reader.ParseTableIteratorKey(iter->key(), &parsed_key));
  EXPECT_EQ(parsed_key.user_key, keys[1]);
  EXPECT_EQ(parsed_key.sequence, 0);
  EXPECT_EQ(parsed_key.type, kEntryMerge);
  iter->Next();

  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(reader.ParseTableIteratorKey(iter->key(), &parsed_key));
  EXPECT_EQ(parsed_key.user_key, keys[2]);
  EXPECT_EQ(parsed_key.sequence, 0);
  EXPECT_EQ(parsed_key.type, kEntryDelete);
  iter->Next();

  EXPECT_FALSE(iter->Valid());
}

TEST_F(SstFileReaderTest, ParseTableIteratorKeyFromDbGeneratedSst) {
  // Verify that the parser decodes real DB sequence numbers, not only the
  // zero sequence numbers generated by SstFileWriter-created external files.
  Options options = options_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  std::string db_name = test::PerThreadDBPath("parse_table_iterator_key_db");
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, db_name, &db));

  std::vector<std::string> keys = {EncodeAsString(0), EncodeAsString(1)};
  ASSERT_OK(db->Put(WriteOptions(), keys[0], "value0"));
  ASSERT_OK(db->Put(WriteOptions(), keys[1], "value1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  ASSERT_OK(db->GetLiveFiles(live_files, &manifest_file_size));
  std::string flushed_file;
  for (const auto& live_file : live_files) {
    if (live_file.substr(live_file.size() - 4) == ".sst") {
      ASSERT_TRUE(flushed_file.empty()) << flushed_file << " " << live_file;
      flushed_file = db_name + live_file;
    }
  }
  ASSERT_FALSE(flushed_file.empty());
  db.reset();

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(flushed_file));
  std::unique_ptr<Iterator> iter = reader.NewTableIterator();
  ASSERT_NE(iter, nullptr);
  iter->SeekToFirst();

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_TRUE(iter->Valid());
    ParsedEntryInfo parsed_key;
    ASSERT_OK(reader.ParseTableIteratorKey(iter->key(), &parsed_key));
    EXPECT_EQ(parsed_key.user_key, keys[i]);
    EXPECT_EQ(parsed_key.sequence, i + 1);
    EXPECT_EQ(parsed_key.type, kEntryPut);
    iter->Next();
  }

  EXPECT_FALSE(iter->Valid());

  ASSERT_OK(DestroyDB(db_name, options));
}

TEST_F(SstFileReaderTest, ParseTableIteratorKeyRejectsShortKey) {
  // Verify that invalid raw table keys fail through the public parser before
  // callers can accidentally interpret a missing internal-key trailer.
  CreateFile(sst_name_,
             {EncodeAsString(0), EncodeAsString(1), EncodeAsString(2)});
  SstFileReader reader(options_);
  ParsedEntryInfo parsed_key;

  const Status status = reader.ParseTableIteratorKey("short", &parsed_key);
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST_F(SstFileReaderTest, Uint64Comparator) {
  options_.comparator = test::Uint64Comparator();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFileAndCheck(keys);
}

TEST_F(SstFileReaderTest, ReadOptionsOutOfScope) {
  // Repro a bug where the SstFileReader depended on its configured ReadOptions
  // outliving it.
  options_.comparator = test::Uint64Comparator();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFile(sst_name_, keys);

  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  std::unique_ptr<Iterator> iter;
  {
    // Make sure ReadOptions go out of scope ASAP so we know the iterator
    // operations do not depend on it.
    ReadOptions ropts;
    iter.reset(reader.NewIterator(ropts));
  }
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->Next();
  }
}

TEST_F(SstFileReaderTest, ReadFileWithGlobalSeqno) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsString(i));
  }
  // Generate a SST file.
  CreateFile(sst_name_, keys);

  // Ingest the file into a db, to assign it a global sequence number.
  Options options;
  options.create_if_missing = true;
  std::string db_name = test::PerThreadDBPath("test_db");
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, db_name, &db));
  // Bump sequence number.
  ASSERT_OK(db->Put(WriteOptions(), keys[0], "foo"));
  ASSERT_OK(db->Flush(FlushOptions()));
  // Ingest the file.
  IngestExternalFileOptions ingest_options;
  ASSERT_OK(db->IngestExternalFile({sst_name_}, ingest_options));
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  ASSERT_OK(db->GetLiveFiles(live_files, &manifest_file_size));
  // Get the ingested file.
  std::string ingested_file;
  for (auto& live_file : live_files) {
    if (live_file.substr(live_file.size() - 4, std::string::npos) == ".sst") {
      if (ingested_file.empty() || ingested_file < live_file) {
        ingested_file = live_file;
      }
    }
  }
  ASSERT_FALSE(ingested_file.empty());
  db.reset();

  // Verify the file can be open and read by SstFileReader.
  CheckFile(db_name + ingested_file, keys, true /* check_global_seqno */);

  // Cleanup.
  ASSERT_OK(DestroyDB(db_name, options));
}

TEST_F(SstFileReaderTest, TimestampSizeMismatch) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Comparator is not timestamp-aware; calls to APIs taking timestamps should
  // fail.
  ASSERT_NOK(writer.Put("key", EncodeAsUint64(100), "value"));
  ASSERT_NOK(writer.Delete("another_key", EncodeAsUint64(200)));
}

class SstFileReaderTimestampTest : public testing::Test {
 public:
  SstFileReaderTimestampTest() {
    Env* env = Env::Default();
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env, &env_guard_));
    EXPECT_NE(nullptr, env);

    options_.env = env;

    options_.comparator = test::BytewiseComparatorWithU64TsWrapper();

    sst_name_ = test::PerThreadDBPath("sst_file_ts");
  }

  ~SstFileReaderTimestampTest() override {
    EXPECT_OK(options_.env->DeleteFile(sst_name_));
  }

  struct KeyValueDesc {
    KeyValueDesc(std::string k, std::string ts, std::string v)
        : key(std::move(k)), timestamp(std::move(ts)), value(std::move(v)) {}

    std::string key;
    std::string timestamp;
    std::string value;
  };

  struct InputKeyValueDesc : public KeyValueDesc {
    InputKeyValueDesc(std::string k, std::string ts, std::string v, bool is_del,
                      bool use_contig_buf)
        : KeyValueDesc(std::move(k), std::move(ts), std::move(v)),
          is_delete(is_del),
          use_contiguous_buffer(use_contig_buf) {}

    bool is_delete = false;
    bool use_contiguous_buffer = false;
  };

  struct OutputKeyValueDesc : public KeyValueDesc {
    OutputKeyValueDesc(std::string k, std::string ts, std::string v)
        : KeyValueDesc(std::move(k), std::move(ts), std::move(v)) {}
  };

  void CreateFile(const std::vector<InputKeyValueDesc>& descs,
                  ExternalSstFileInfo* file_info = nullptr) {
    SstFileWriter writer(soptions_, options_);

    ASSERT_OK(writer.Open(sst_name_));

    for (const auto& desc : descs) {
      if (desc.is_delete) {
        if (desc.use_contiguous_buffer) {
          std::string key_with_ts(desc.key + desc.timestamp);
          ASSERT_OK(writer.Delete(Slice(key_with_ts.data(), desc.key.size()),
                                  Slice(key_with_ts.data() + desc.key.size(),
                                        desc.timestamp.size())));
        } else {
          ASSERT_OK(writer.Delete(desc.key, desc.timestamp));
        }
      } else {
        if (desc.use_contiguous_buffer) {
          std::string key_with_ts(desc.key + desc.timestamp);
          ASSERT_OK(writer.Put(Slice(key_with_ts.data(), desc.key.size()),
                               Slice(key_with_ts.data() + desc.key.size(),
                                     desc.timestamp.size()),
                               desc.value));
        } else {
          ASSERT_OK(writer.Put(desc.key, desc.timestamp, desc.value));
        }
      }
    }

    ASSERT_OK(writer.Finish(file_info));
  }

  void CheckFile(const std::string& timestamp,
                 const std::vector<OutputKeyValueDesc>& descs) {
    SstFileReader reader(options_);

    ASSERT_OK(reader.Open(sst_name_));
    ASSERT_OK(reader.VerifyChecksum());

    Slice ts_slice(timestamp);

    ReadOptions read_options;
    read_options.timestamp = &ts_slice;

    std::unique_ptr<Iterator> iter(reader.NewIterator(read_options));
    iter->SeekToFirst();

    for (const auto& desc : descs) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), desc.key);
      ASSERT_EQ(iter->timestamp(), desc.timestamp);
      ASSERT_EQ(iter->value(), desc.value);

      iter->Next();
    }

    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Writes a file holding a single timestamped range tombstone. Each key gets
  // its own exactly sized heap buffer so that reading past one is caught, and
  // `timestamp` is placed right after whichever key `timestamp_follows_end_key`
  // selects -- a lone timestamp can only ever be adjacent to one of the two.
  void CreateFileWithDeleteRange(const std::string& begin_key,
                                 const std::string& end_key,
                                 const std::string& timestamp,
                                 bool timestamp_follows_end_key,
                                 ExternalSstFileInfo* file_info) {
    const std::string& adjacent_key =
        timestamp_follows_end_key ? end_key : begin_key;
    const std::string& lone_key =
        timestamp_follows_end_key ? begin_key : end_key;

    std::unique_ptr<char[]> adjacent_buf(
        new char[adjacent_key.size() + timestamp.size()]);
    memcpy(adjacent_buf.get(), adjacent_key.data(), adjacent_key.size());
    memcpy(adjacent_buf.get() + adjacent_key.size(), timestamp.data(),
           timestamp.size());
    std::unique_ptr<char[]> lone_buf(new char[lone_key.size()]);
    memcpy(lone_buf.get(), lone_key.data(), lone_key.size());

    const Slice adjacent_slice(adjacent_buf.get(), adjacent_key.size());
    const Slice lone_slice(lone_buf.get(), lone_key.size());
    const Slice timestamp_slice(adjacent_buf.get() + adjacent_key.size(),
                                timestamp.size());

    SstFileWriter writer(soptions_, options_);
    ASSERT_OK(writer.Open(sst_name_));
    if (timestamp_follows_end_key) {
      ASSERT_OK(
          writer.DeleteRange(lone_slice, adjacent_slice, timestamp_slice));
    } else {
      ASSERT_OK(
          writer.DeleteRange(adjacent_slice, lone_slice, timestamp_slice));
    }
    ASSERT_OK(writer.Finish(file_info));
  }

 protected:
  std::shared_ptr<Env> env_guard_;
  Options options_;
  EnvOptions soptions_;
  std::string sst_name_;
};

TEST_F(SstFileReaderTimestampTest, Basic) {
  std::vector<InputKeyValueDesc> input_descs;

  for (uint64_t k = 0; k < kNumKeys; k += 4) {
    // A Put with key k, timestamp k that gets overwritten by a subsequent Put
    // with timestamp (k + 1). Note that the comparator uses descending order
    // for the timestamp part, so we add the later Put first.
    input_descs.emplace_back(
        /* key */ EncodeAsString(k), /* timestamp */ EncodeAsUint64(k + 1),
        /* value */ EncodeAsString(k * 2), /* is_delete */ false,
        /* use_contiguous_buffer */ false);
    input_descs.emplace_back(
        /* key */ EncodeAsString(k), /* timestamp */ EncodeAsUint64(k),
        /* value */ EncodeAsString(k * 3), /* is_delete */ false,
        /* use_contiguous_buffer */ true);

    // A Put with key (k + 2), timestamp (k + 2) that gets cancelled out by a
    // Delete with timestamp (k + 3).  Note that the comparator uses descending
    // order for the timestamp part, so we add the Delete first.
    input_descs.emplace_back(/* key */ EncodeAsString(k + 2),
                             /* timestamp */ EncodeAsUint64(k + 3),
                             /* value */ std::string(), /* is_delete */ true,
                             /* use_contiguous_buffer */ (k % 8) == 0);
    input_descs.emplace_back(
        /* key */ EncodeAsString(k + 2), /* timestamp */ EncodeAsUint64(k + 2),
        /* value */ EncodeAsString(k * 5), /* is_delete */ false,
        /* use_contiguous_buffer */ (k % 8) != 0);
  }

  CreateFile(input_descs);

  // Note: below, we check the results as of each timestamp in the range,
  // updating the expected result as needed.
  std::vector<OutputKeyValueDesc> output_descs;

  for (uint64_t ts = 0; ts < kNumKeys; ++ts) {
    const uint64_t k = ts - (ts % 4);

    switch (ts % 4) {
      case 0:  // Initial Put for key k
        output_descs.emplace_back(/* key */ EncodeAsString(k),
                                  /* timestamp */ EncodeAsUint64(ts),
                                  /* value */ EncodeAsString(k * 3));
        break;

      case 1:  // Second Put for key k
        assert(output_descs.back().key == EncodeAsString(k));
        assert(output_descs.back().timestamp == EncodeAsUint64(ts - 1));
        assert(output_descs.back().value == EncodeAsString(k * 3));
        output_descs.back().timestamp = EncodeAsUint64(ts);
        output_descs.back().value = EncodeAsString(k * 2);
        break;

      case 2:  // Put for key (k + 2)
        output_descs.emplace_back(/* key */ EncodeAsString(k + 2),
                                  /* timestamp */ EncodeAsUint64(ts),
                                  /* value */ EncodeAsString(k * 5));
        break;

      case 3:  // Delete for key (k + 2)
        assert(output_descs.back().key == EncodeAsString(k + 2));
        assert(output_descs.back().timestamp == EncodeAsUint64(ts - 1));
        assert(output_descs.back().value == EncodeAsString(k * 5));
        output_descs.pop_back();
        break;
    }

    CheckFile(EncodeAsUint64(ts), output_descs);
  }
}

TEST_F(SstFileReaderTimestampTest, DeleteRangeTimestampAdjacentToOneKey) {
  const std::string timestamp = EncodeAsUint64(1);

  {
    // Only begin_key is followed in memory by the timestamp.
    ExternalSstFileInfo file_info;
    CreateFileWithDeleteRange("begin", "end", timestamp,
                              /* timestamp_follows_end_key */ false,
                              &file_info);
    ASSERT_EQ(file_info.smallest_range_del_key, "begin" + timestamp);
    ASSERT_EQ(file_info.largest_range_del_key, "end" + timestamp);
  }

  {
    // Only end_key is followed in memory by the timestamp, and the two keys
    // are the same length, so testing end_key's adjacency with begin_key's
    // size would match here.
    ExternalSstFileInfo file_info;
    CreateFileWithDeleteRange("aaa", "bbb", timestamp,
                              /* timestamp_follows_end_key */ true, &file_info);
    ASSERT_EQ(file_info.smallest_range_del_key, "aaa" + timestamp);
    ASSERT_EQ(file_info.largest_range_del_key, "bbb" + timestamp);
  }
}

TEST_F(SstFileReaderTimestampTest, TimestampsOutOfOrder) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Note: KVs that have the same user key disregarding timestamps should be in
  // descending order of timestamps.
  ASSERT_OK(writer.Put("key", EncodeAsUint64(1), "value1"));
  ASSERT_NOK(writer.Put("key", EncodeAsUint64(2), "value2"));
}

TEST_F(SstFileReaderTimestampTest, TimestampSizeMismatch) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Comparator expects 64-bit timestamps; timestamps with other sizes as well
  // as calls to the timestamp-less APIs should be rejected.
  ASSERT_NOK(writer.Put("key", "not_an_actual_64_bit_timestamp", "value"));
  ASSERT_NOK(writer.Delete("another_key", "timestamp_of_unexpected_size"));

  ASSERT_NOK(writer.Put("key_without_timestamp", "value"));
  ASSERT_NOK(writer.Merge("another_key_missing_a_timestamp", "merge_operand"));
  ASSERT_NOK(writer.Delete("yet_another_key_still_no_timestamp"));
  ASSERT_NOK(writer.DeleteRange("begin_key_timestamp_absent",
                                "end_key_with_a_complete_lack_of_timestamps"));
}

class SstFileReaderTimestampNotPersistedTest
    : public SstFileReaderTimestampTest {
 public:
  SstFileReaderTimestampNotPersistedTest() {
    Env* env = Env::Default();
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env, &env_guard_));
    EXPECT_NE(nullptr, env);

    options_.env = env;

    options_.comparator = test::BytewiseComparatorWithU64TsWrapper();

    options_.persist_user_defined_timestamps = false;

    sst_name_ = test::PerThreadDBPath("sst_file_ts_not_persisted");
  }

  ~SstFileReaderTimestampNotPersistedTest() override = default;
};

TEST_F(SstFileReaderTimestampNotPersistedTest, Basic) {
  std::vector<InputKeyValueDesc> input_descs;

  for (uint64_t k = 0; k < kNumKeys; k++) {
    input_descs.emplace_back(
        /* key */ EncodeAsString(k), /* timestamp */ EncodeAsUint64(0),
        /* value */ EncodeAsString(k), /* is_delete */ false,
        /* use_contiguous_buffer */ (k % 2) == 0);
  }

  ExternalSstFileInfo external_sst_file_info;

  CreateFile(input_descs, &external_sst_file_info);
  std::vector<OutputKeyValueDesc> output_descs;

  for (uint64_t k = 0; k < kNumKeys; k++) {
    output_descs.emplace_back(/* key */ EncodeAsString(k),
                              /* timestamp */ EncodeAsUint64(0),
                              /* value */ EncodeAsString(k));
  }
  CheckFile(EncodeAsUint64(0), output_descs);
  ASSERT_EQ(external_sst_file_info.smallest_key, EncodeAsString(0));
  ASSERT_EQ(external_sst_file_info.largest_key, EncodeAsString(kNumKeys - 1));
  ASSERT_EQ(external_sst_file_info.smallest_range_del_key, "");
  ASSERT_EQ(external_sst_file_info.largest_range_del_key, "");
}

TEST_F(SstFileReaderTimestampNotPersistedTest, NonMinTimestampNotAllowed) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  ASSERT_NOK(writer.Delete("baz", EncodeAsUint64(2)));
  ASSERT_OK(writer.Put("baz", EncodeAsUint64(0), "foo_val"));

  ASSERT_NOK(writer.Put("key", EncodeAsUint64(2), "value1"));
  ASSERT_OK(writer.Put("key", EncodeAsUint64(0), "value2"));

  // The `SstFileWriter::DeleteRange` API documentation specifies that
  // a range deletion tombstone added in the file does NOT delete point
  // (Put/Merge/Delete) keys in the same file. While there is no checks in
  // `SstFileWriter` to ensure this requirement is met, when such a range
  // deletion does exist, it will get over-written by point data in the same
  // file after ingestion because they have the same sequence number.
  // We allow having a point data entry and having a range deletion entry for
  // a key in the same file when timestamps are removed for the same reason.
  // After the file is ingested, the range deletion will effectively get
  // over-written by the point data since they will have the same sequence
  // number and the same user-defined timestamps.
  ASSERT_NOK(writer.DeleteRange("bar", "foo", EncodeAsUint64(2)));
  ASSERT_OK(writer.DeleteRange("bar", "foo", EncodeAsUint64(0)));

  ExternalSstFileInfo external_sst_file_info;

  ASSERT_OK(writer.Finish(&external_sst_file_info));
  ASSERT_EQ(external_sst_file_info.smallest_key, "baz");
  ASSERT_EQ(external_sst_file_info.largest_key, "key");
  ASSERT_EQ(external_sst_file_info.smallest_range_del_key, "bar");
  ASSERT_EQ(external_sst_file_info.largest_range_del_key, "foo");
}

TEST_F(SstFileReaderTimestampNotPersistedTest, KeyWithoutTimestampOutOfOrder) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  ASSERT_OK(writer.Put("foo", EncodeAsUint64(0), "value1"));
  ASSERT_NOK(writer.Put("bar", EncodeAsUint64(0), "value2"));
}

TEST_F(SstFileReaderTimestampNotPersistedTest, IncompatibleTimestampFormat) {
  SstFileWriter writer(soptions_, options_);

  ASSERT_OK(writer.Open(sst_name_));

  // Even though in this mode timestamps are not persisted, we require users
  // to call the timestamp-aware APIs only.
  ASSERT_TRUE(writer.Put("key", "not_an_actual_64_bit_timestamp", "value")
                  .IsInvalidArgument());
  ASSERT_TRUE(writer.Delete("another_key", "timestamp_of_unexpected_size")
                  .IsInvalidArgument());

  ASSERT_TRUE(writer.Put("key_without_timestamp", "value").IsInvalidArgument());
  ASSERT_TRUE(writer.Merge("another_key_missing_a_timestamp", "merge_operand")
                  .IsInvalidArgument());
  ASSERT_TRUE(
      writer.Delete("yet_another_key_still_no_timestamp").IsInvalidArgument());
  ASSERT_TRUE(writer
                  .DeleteRange("begin_key_timestamp_absent",
                               "end_key_with_a_complete_lack_of_timestamps")
                  .IsInvalidArgument());
}

TEST_F(SstFileReaderTest, VerifyNumEntriesBasic) {
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < kNumKeys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFile(sst_name_, keys);
  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  ASSERT_OK(reader.VerifyNumEntries(ReadOptions()));
}

TEST_F(SstFileReaderTest, VerifyNumEntriesDeleteRange) {
  SstFileWriter writer(soptions_, options_);
  ASSERT_OK(writer.Open(sst_name_));

  for (uint64_t i = 0; i < kNumKeys; i++) {
    ASSERT_OK(writer.Put(EncodeAsUint64(i), EncodeAsUint64(i + 1)));
  }
  ASSERT_OK(
      writer.DeleteRange(EncodeAsUint64(0), EncodeAsUint64(kNumKeys / 2)));
  ASSERT_OK(writer.Finish());
  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  ASSERT_OK(reader.VerifyNumEntries(ReadOptions()));
}

TEST_F(SstFileReaderTest, VerifyNumEntriesCorruption) {
  const int num_keys = 99;
  const int corrupted_num_keys = num_keys + 2;
  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* arg) {
        TableProperties* props = reinterpret_cast<TableProperties*>(arg);
        props->num_entries = corrupted_num_keys;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::vector<std::string> keys;
  for (uint64_t i = 0; i < num_keys; i++) {
    keys.emplace_back(EncodeAsUint64(i));
  }
  CreateFile(sst_name_, keys);
  SstFileReader reader(options_);
  ASSERT_OK(reader.Open(sst_name_));
  Status s = reader.VerifyNumEntries(ReadOptions());
  ASSERT_TRUE(s.IsCorruption());
  std::ostringstream oss;
  oss << "Table property expects " << corrupted_num_keys
      << " entries when excluding range deletions,"
      << " but scanning the table returned " << num_keys << " entries";
  ASSERT_TRUE(std::strstr(oss.str().c_str(), s.getState()));
}

class SstFileReaderTableIteratorTest : public DBTestBase {
 public:
  SstFileReaderTableIteratorTest()
      : DBTestBase("sst_file_reader_table_iterator_test",
                   /*env_do_fsync=*/false) {}

  void VerifyTableEntry(Iterator* iter, const std::string& user_key,
                        ValueType value_type,
                        std::optional<std::string> expected_value,
                        bool backward_iteration = false) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->status().ok());
    ParsedInternalKey pikey;
    ASSERT_OK(ParseInternalKey(iter->key(), &pikey, /*log_err_key=*/false));
    ASSERT_EQ(pikey.user_key, user_key);
    ASSERT_EQ(pikey.type, value_type);
    if (expected_value.has_value()) {
      ASSERT_EQ(iter->value(), expected_value.value());
    }
    if (!backward_iteration) {
      iter->Next();
    } else {
      iter->Prev();
    }
  }
};

TEST_F(SstFileReaderTableIteratorTest, Basic) {
  Options options = CurrentOptions();
  const Comparator* ucmp = BytewiseComparator();
  options.comparator = ucmp;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  // Create a L0 sst file with 4 entries, two for each user key.
  // The file should have these entries in ascending internal key order:
  // 'bar, seq: 4, type: kTypeValue => val2'
  // 'bar, seq: 3, type: kTypeDeletion'
  // 'foo, seq: 2, type: kTypeDeletion'
  // 'foo, seq: 1, type: kTypeValue => val1'
  ASSERT_OK(Put("foo", "val1"));
  const Snapshot* snapshot1 = dbfull()->GetSnapshot();
  ASSERT_OK(Delete("foo"));
  ASSERT_OK(Delete("bar"));
  const Snapshot* snapshot2 = dbfull()->GetSnapshot();
  ASSERT_OK(Put("bar", "val2"));

  ASSERT_OK(Flush());

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_TRUE(files.size() == 1);
  ASSERT_TRUE(files[0].level == 0);
  std::string file_name = files[0].directory + "/" + files[0].relative_filename;

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(file_name));
  ASSERT_OK(reader.VerifyChecksum());

  // When iterating the file as a DB iterator, only one data entry for "bar" is
  // visible.
  std::unique_ptr<Iterator> db_iter(reader.NewIterator(ReadOptions()));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key(), "bar");
  ASSERT_EQ(db_iter->value(), "val2");
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
  db_iter.reset();

  // When iterating the file with a raw table iterator, all the data entries are
  // surfaced in ascending internal key order.
  std::unique_ptr<Iterator> table_iter = reader.NewTableIterator();

  table_iter->SeekToFirst();
  VerifyTableEntry(table_iter.get(), "bar", kTypeValue, "val2");
  VerifyTableEntry(table_iter.get(), "bar", kTypeDeletion, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo", kTypeDeletion, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo", kTypeValue, "val1");
  ASSERT_FALSE(table_iter->Valid());

  std::string seek_key_buf;
  ASSERT_OK(GetInternalKeyForSeek("foo", ucmp, &seek_key_buf));
  Slice seek_target = seek_key_buf;
  table_iter->Seek(seek_target);
  VerifyTableEntry(table_iter.get(), "foo", kTypeDeletion, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo", kTypeValue, "val1");
  ASSERT_FALSE(table_iter->Valid());

  ASSERT_OK(GetInternalKeyForSeekForPrev("bar", ucmp, &seek_key_buf));
  Slice seek_for_prev_target = seek_key_buf;
  table_iter->SeekForPrev(seek_for_prev_target);
  VerifyTableEntry(table_iter.get(), "bar", kTypeDeletion, std::nullopt,
                   /*backward_iteration=*/true);
  VerifyTableEntry(table_iter.get(), "bar", kTypeValue, "val2",
                   /*backward_iteration=*/true);
  ASSERT_FALSE(table_iter->Valid());

  dbfull()->ReleaseSnapshot(snapshot1);
  dbfull()->ReleaseSnapshot(snapshot2);
  Close();
}

TEST_F(SstFileReaderTableIteratorTest, UserDefinedTimestampsEnabled) {
  Options options = CurrentOptions();
  const Comparator* ucmp = test::BytewiseComparatorWithU64TsWrapper();
  options.comparator = ucmp;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  // Create a L0 sst file with 4 entries, two for each user key.
  // The file should have these entries in ascending internal key order:
  // 'bar, ts=3, seq: 4, type: kTypeValue => val2'
  // 'bar, ts=2, seq: 3, type: kTypeDeletionWithTimestamp'
  // 'foo, ts=4, seq: 2, type: kTypeDeletionWithTimestamp'
  // 'foo, ts=3, seq: 1, type: kTypeValue => val1'
  WriteOptions wopt;
  ColumnFamilyHandle* cfd = db_->DefaultColumnFamily();
  ASSERT_OK(db_->Put(wopt, cfd, "foo", EncodeAsUint64(3), "val1"));
  ASSERT_OK(db_->Delete(wopt, cfd, "foo", EncodeAsUint64(4)));
  ASSERT_OK(db_->Delete(wopt, cfd, "bar", EncodeAsUint64(2)));
  ASSERT_OK(db_->Put(wopt, cfd, "bar", EncodeAsUint64(3), "val2"));

  ASSERT_OK(Flush());

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_TRUE(files.size() == 1);
  ASSERT_TRUE(files[0].level == 0);
  std::string file_name = files[0].directory + "/" + files[0].relative_filename;

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(file_name));
  ASSERT_OK(reader.VerifyChecksum());

  // When iterating the file as a DB iterator, only one data entry for "bar" is
  // visible.
  ReadOptions ropts;
  std::string read_ts = EncodeAsUint64(4);
  Slice read_ts_slice = read_ts;
  ropts.timestamp = &read_ts_slice;
  std::unique_ptr<Iterator> db_iter(reader.NewIterator(ropts));
  db_iter->SeekToFirst();
  ASSERT_TRUE(db_iter->Valid());
  ASSERT_EQ(db_iter->key(), "bar");
  ASSERT_EQ(db_iter->value(), "val2");
  ASSERT_EQ(db_iter->timestamp(), EncodeAsUint64(3));
  db_iter->Next();
  ASSERT_FALSE(db_iter->Valid());
  db_iter.reset();

  std::unique_ptr<Iterator> table_iter = reader.NewTableIterator();

  table_iter->SeekToFirst();
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(3), kTypeValue,
                   "val2");
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(2),
                   kTypeDeletionWithTimestamp, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(4),
                   kTypeDeletionWithTimestamp, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(3), kTypeValue,
                   "val1");
  ASSERT_FALSE(table_iter->Valid());

  std::string seek_key_buf;
  ASSERT_OK(GetInternalKeyForSeek("foo", ucmp, &seek_key_buf));
  Slice seek_target = seek_key_buf;
  table_iter->Seek(seek_target);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(4),
                   kTypeDeletionWithTimestamp, std::nullopt);
  VerifyTableEntry(table_iter.get(), "foo" + EncodeAsUint64(3), kTypeValue,
                   "val1");
  ASSERT_FALSE(table_iter->Valid());

  ASSERT_OK(GetInternalKeyForSeekForPrev("bar", ucmp, &seek_key_buf));
  Slice seek_for_prev_target = seek_key_buf;
  table_iter->SeekForPrev(seek_for_prev_target);
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(2),
                   kTypeDeletionWithTimestamp, std::nullopt,
                   /*backward_iteration=*/true);
  VerifyTableEntry(table_iter.get(), "bar" + EncodeAsUint64(3), kTypeValue,
                   "val2", /*backward_iteration=*/true);
  ASSERT_FALSE(table_iter->Valid());

  Close();
}

class SstFileReaderTableGetTest : public DBTestBase,
                                  public testing::WithParamInterface<bool> {
 public:
  SstFileReaderTableGetTest()
      : DBTestBase("sst_file_reader_table_get_test",
                   /*env_do_fsync=*/false) {}

  bool UseMultiGet() const { return GetParam(); }

  std::vector<Status> DoGet(SstFileReader& reader,
                            const std::vector<Slice>& keys,
                            std::vector<std::string>* values) {
    if (UseMultiGet()) {
      return reader.MultiGet(ReadOptions(), keys, values);
    } else {
      values->resize(keys.size());
      std::vector<Status> statuses(keys.size());
      for (size_t i = 0; i < keys.size(); ++i) {
        statuses[i] = reader.Get(ReadOptions(), keys[i], &(*values)[i]);
      }
      return statuses;
    }
  }
};

TEST_P(SstFileReaderTableGetTest, Basic) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "val1"));
  const Snapshot* snapshot1 = dbfull()->GetSnapshot();
  ASSERT_OK(Delete("foo"));
  ASSERT_OK(Delete("bar"));
  const Snapshot* snapshot2 = dbfull()->GetSnapshot();
  ASSERT_OK(Put("bar", "val2"));
  ASSERT_OK(Put("baz", "val3"));
  ASSERT_OK(Put("aaa", "val4"));
  const Snapshot* snapshot3 = dbfull()->GetSnapshot();
  ASSERT_OK(Merge("aaa", "val5"));

  ASSERT_OK(Flush());

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1);
  ASSERT_TRUE(files[0].level == 0);
  std::string file_name = files[0].directory + "/" + files[0].relative_filename;

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(file_name));
  ASSERT_OK(reader.VerifyChecksum());

  ASSERT_OK(options.statistics->Reset());

  std::vector<Slice> keys = {"fo1", "foo", "baz",
                             "bar", "aaa", "zzz_not_in_sst"};
  std::vector<std::string> values;
  auto statuses = DoGet(reader, keys, &values);

  // Non-existent key returns NotFound
  ASSERT_TRUE(statuses[0].IsNotFound());

  // Deleted key returns NotFound
  ASSERT_TRUE(statuses[1].IsNotFound());

  // Found keys
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "val3");

  ASSERT_OK(statuses[3]);
  ASSERT_EQ(values[3], "val2");

  // Merged key
  ASSERT_OK(statuses[4]);
  ASSERT_EQ(values[4], "val4,val5");

  // Bloom filter filtered key
  ASSERT_TRUE(statuses[5].IsNotFound());

  uint64_t cache_hits = options.statistics->getTickerCount(BLOCK_CACHE_HIT);
  uint64_t cache_misses = options.statistics->getTickerCount(BLOCK_CACHE_MISS);
  ASSERT_GT(cache_hits + cache_misses, 0);

  dbfull()->ReleaseSnapshot(snapshot1);
  dbfull()->ReleaseSnapshot(snapshot2);
  dbfull()->ReleaseSnapshot(snapshot3);
  Close();
}

TEST_P(SstFileReaderTableGetTest, BlobBackedWideColumnDefaultWithoutFetcher) {
  // Goal: cover the SstFileReader path where GetContext has no BlobFetcher.
  // The test writes a blob-backed wide-column entity, compacts it so the SST
  // stores blob references, and then verifies SstFileReader reports a clean
  // Corruption status instead of dereferencing a null BlobFetcher.
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 50;

  DestroyAndReopen(options);

  const std::string key = "blob_backed_entity";
  const std::string default_value(100, 'd');
  WideColumns columns{{kDefaultWideColumnName, default_value},
                      {"meta", "inline"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1);
  std::string file_name = files[0].directory + "/" + files[0].relative_filename;

  SstFileReader reader(options);
  ASSERT_OK(reader.Open(file_name));

  std::vector<Slice> keys = {key};
  std::vector<std::string> values;
  auto statuses = DoGet(reader, keys, &values);

  ASSERT_TRUE(statuses[0].IsCorruption()) << statuses[0].ToString();
  ASSERT_NE(statuses[0].ToString().find("blob fetcher"), std::string::npos)
      << statuses[0].ToString();

  Close();
}

INSTANTIATE_TEST_CASE_P(SingleAndMulti, SstFileReaderTableGetTest,
                        testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
