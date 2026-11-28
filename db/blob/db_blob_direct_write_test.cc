//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <future>
#include <limits>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/compressed_secondary_cache.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_sequential_reader.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/wide/wide_column_test_util.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/replayer.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class FixedBlobDirectWritePartitionStrategy : public BlobFilePartitionStrategy {
 public:
  using BlobFilePartitionStrategy::SelectPartition;

  explicit FixedBlobDirectWritePartitionStrategy(uint32_t partition)
      : partition_(partition) {}

  const char* Name() const override {
    return "FixedBlobDirectWritePartitionStrategy";
  }

  uint32_t SelectPartition(uint32_t /*num_partitions*/,
                           uint32_t /*column_family_id*/, const Slice& /*key*/,
                           const Slice& /*value*/) override {
    return partition_;
  }

 private:
  const uint32_t partition_;
};

class RecordingBlobDirectWritePartitionStrategy
    : public BlobFilePartitionStrategy {
 public:
  using BlobFilePartitionStrategy::SelectPartition;

  struct Call {
    uint32_t num_partitions = 0;
    uint32_t column_family_id = 0;
    std::string key;
    std::string value;
    uint32_t selected_partition = 0;
    uint64_t call_count = 0;
  };

  explicit RecordingBlobDirectWritePartitionStrategy(uint32_t partition)
      : partition_(partition) {}

  const char* Name() const override {
    return "RecordingBlobDirectWritePartitionStrategy";
  }

  uint32_t SelectPartition(uint32_t num_partitions, uint32_t column_family_id,
                           const Slice& key, const Slice& value) override {
    std::lock_guard<std::mutex> lock(mutex_);
    calls_.push_back({num_partitions, column_family_id, key.ToString(),
                      value.ToString(), partition_,
                      static_cast<uint64_t>(calls_.size()) + 1});
    return partition_;
  }

  std::vector<Call> GetCalls() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return calls_;
  }

 private:
  const uint32_t partition_;
  mutable std::mutex mutex_;
  std::vector<Call> calls_;
};

namespace {

// Matches BlobDB file names so the test filesystem can special-case only active
// blob files without changing unrelated file opens.
bool IsBlobFilePath(const std::string& fname) {
  const size_t basename_pos = fname.find_last_of("/\\");
  const std::string basename = basename_pos == std::string::npos
                                   ? fname
                                   : fname.substr(basename_pos + 1);
  uint64_t file_number = 0;
  FileType file_type = kWalFile;
  return ParseFileName(basename, &file_number, &file_type) &&
         file_type == kBlobFile;
}

class BlobFileChecksumCapturingFileSystem : public FileSystemWrapper {
 public:
  explicit BlobFileChecksumCapturingFileSystem(
      const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}

  static const char* kClassName() {
    return "BlobFileChecksumCapturingFileSystem";
  }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    if (IsBlobFilePath(fname)) {
      std::lock_guard<std::mutex> lock(mu_);
      file_checksum_ = opts.file_checksum;
      file_checksum_func_name_ = opts.file_checksum_func_name;
      ++capture_count_;
    }
    return target()->NewRandomAccessFile(fname, opts, result, dbg);
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mu_);
    file_checksum_.clear();
    file_checksum_func_name_.clear();
    capture_count_ = 0;
  }

  std::string GetFileChecksum() const {
    std::lock_guard<std::mutex> lock(mu_);
    return file_checksum_;
  }

  std::string GetFileChecksumFuncName() const {
    std::lock_guard<std::mutex> lock(mu_);
    return file_checksum_func_name_;
  }

  int GetCaptureCount() const {
    std::lock_guard<std::mutex> lock(mu_);
    return capture_count_;
  }

 private:
  mutable std::mutex mu_;
  std::string file_checksum_;
  std::string file_checksum_func_name_;
  int capture_count_ = 0;
};

// Keeps track of when an active blob file stops being writer-visible so the
// test filesystem can model current-size visibility only while the file remains
// active.
class ActiveBlobVisibilityWritableFile : public FSWritableFileOwnerWrapper {
 public:
  ActiveBlobVisibilityWritableFile(std::unique_ptr<FSWritableFile>&& target,
                                   std::function<void()> on_close)
      : FSWritableFileOwnerWrapper(std::move(target)),
        on_close_(std::move(on_close)) {}

  ~ActiveBlobVisibilityWritableFile() override { DeactivateOnce(); }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) override {
    IOStatus s = target()->Close(options, dbg);
    DeactivateOnce();
    return s;
  }

 private:
  void DeactivateOnce() {
    if (on_close_) {
      on_close_();
      on_close_ = nullptr;
    }
  }

  std::function<void()> on_close_;
};

// Simulates a remote readable file whose GetFileSize() either exposes the live
// on-disk size for default-contract active blobs or hides it for
// append-only-no-readers files.
class ActiveBlobVisibilityRandomAccessFile
    : public FSRandomAccessFileOwnerWrapper {
 public:
  ActiveBlobVisibilityRandomAccessFile(
      std::unique_ptr<FSRandomAccessFile>&& target, bool expose_current_size,
      std::shared_ptr<FileSystem> underlying_fs, std::string fname)
      : FSRandomAccessFileOwnerWrapper(std::move(target)),
        expose_current_size_(expose_current_size),
        underlying_fs_(std::move(underlying_fs)),
        fname_(std::move(fname)) {}

  IOStatus GetFileSize(uint64_t* result) override {
    if (!expose_current_size_) {
      *result = 0;
      return IOStatus::OK();
    }
    // Delegate to the underlying FS path-level GetFileSize (bypassing the
    // wrapper that reports 0 for active blobs) to simulate a remote FS that
    // exposes the current file size through the open handle.
    return underlying_fs_->GetFileSize(fname_, IOOptions(), result,
                                       /*dbg=*/nullptr);
  }

 private:
  const bool expose_current_size_;
  std::shared_ptr<FileSystem> underlying_fs_;
  std::string fname_;
};

// Models a remote filesystem where active direct-write blobs are only
// reader-visible when the writer does not promise no concurrent readers.
class RemoteBlobVisibilityFileSystem : public FileSystemWrapper {
 public:
  explicit RemoteBlobVisibilityFileSystem(const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}

  static const char* kClassName() { return "RemoteBlobVisibilityFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    std::unique_ptr<FSWritableFile> file;
    IOStatus s = target()->NewWritableFile(fname, opts, &file, dbg);
    if (!s.ok() || !IsBlobFilePath(fname)) {
      *result = std::move(file);
      return IOStatus(std::move(s));
    }

    const bool has_no_reopen_for_write_contract = HasFileOpenContract(
        opts.open_contract, FileOpenContract::kNoReopenForWrite);
    const bool has_no_readers_while_open_for_write_contract =
        HasFileOpenContract(opts.open_contract,
                            FileOpenContract::kNoReadersWhileOpenForWrite);
    {
      std::lock_guard<std::mutex> lock(mu_);
      saw_no_reopen_for_write_writer_contract_ |=
          has_no_reopen_for_write_contract;
      saw_no_readers_while_open_for_write_writer_contract_ |=
          has_no_readers_while_open_for_write_contract;
      if (!has_no_readers_while_open_for_write_contract) {
        active_blob_paths_.insert(fname);
      }
    }

    if (has_no_readers_while_open_for_write_contract) {
      *result = std::move(file);
      return IOStatus::OK();
    }

    result->reset(new ActiveBlobVisibilityWritableFile(
        std::move(file), [this, tracked_path = std::string(fname)]() {
          DeactivateBlobPath(tracked_path);
        }));
    return IOStatus::OK();
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    if (!s.ok() || !IsBlobFilePath(fname)) {
      *result = std::move(file);
      return IOStatus(std::move(s));
    }

    const bool has_no_readers_while_open_for_write_contract =
        HasFileOpenContract(opts.open_contract,
                            FileOpenContract::kNoReadersWhileOpenForWrite);
    const bool active_blob = IsActiveBlobPath(fname);
    {
      std::lock_guard<std::mutex> lock(mu_);
      saw_no_readers_while_open_for_write_reader_contract_ |=
          has_no_readers_while_open_for_write_contract;
    }

    if (!active_blob) {
      *result = std::move(file);
      return IOStatus::OK();
    }

    result->reset(new ActiveBlobVisibilityRandomAccessFile(
        std::move(file), !has_no_readers_while_open_for_write_contract, target_,
        fname));
    return IOStatus::OK();
  }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    if (IsActiveBlobPath(fname)) {
      *file_size = 0;
      return IOStatus::OK();
    }
    return target()->GetFileSize(fname, options, file_size, dbg);
  }

  bool SawNoReopenForWriteWriterContract() const {
    std::lock_guard<std::mutex> lock(mu_);
    return saw_no_reopen_for_write_writer_contract_;
  }

  bool SawNoReadersWhileOpenForWriteWriterContract() const {
    std::lock_guard<std::mutex> lock(mu_);
    return saw_no_readers_while_open_for_write_writer_contract_;
  }

  bool SawNoReadersWhileOpenForWriteReaderContract() const {
    std::lock_guard<std::mutex> lock(mu_);
    return saw_no_readers_while_open_for_write_reader_contract_;
  }

 private:
  void DeactivateBlobPath(const std::string& fname) {
    std::lock_guard<std::mutex> lock(mu_);
    active_blob_paths_.erase(fname);
  }

  bool IsActiveBlobPath(const std::string& fname) const {
    std::lock_guard<std::mutex> lock(mu_);
    return active_blob_paths_.find(fname) != active_blob_paths_.end();
  }

  mutable std::mutex mu_;
  bool saw_no_reopen_for_write_writer_contract_ = false;
  bool saw_no_readers_while_open_for_write_writer_contract_ = false;
  bool saw_no_readers_while_open_for_write_reader_contract_ = false;
  std::unordered_set<std::string> active_blob_paths_;
};

}  // namespace

class DBBlobDirectWriteTest : public DBTestBase {
 protected:
  DBBlobDirectWriteTest()
      : DBTestBase("db_blob_direct_write_test", /* env_do_fsync */ false) {}

  Options GetBlobDirectWriteCompatibleOptions() {
    return wide_column_test_util::GetBlobDirectWriteCompatibleOptions(
        GetDefaultOptions());
  }

  Options GetDirectWriteOptions() {
    return wide_column_test_util::GetDirectWriteOptions(GetDefaultOptions());
  }

  // Returns how many times `fn` syncs a blob file (BlobLogWriter::Sync()).
  // Sealing a file syncs it once, in AppendFooter().
  int CountBlobFileSyncs(const std::function<void()>& fn) {
    std::atomic<int> syncs{0};
    SyncPoint::GetInstance()->SetCallBack("BlobLogWriter::Sync",
                                          [&](void* /*arg*/) { ++syncs; });
    SyncPoint::GetInstance()->EnableProcessing();
    fn();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    return syncs.load();
  }

  size_t CountBlobFiles() {
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));

    size_t blob_files = 0;
    for (const auto& file : files) {
      if (file.size() > 5 && file.substr(file.size() - 5) == ".blob") {
        ++blob_files;
      }
    }
    return blob_files;
  }

  Status ReadBlobFileHeader(uint64_t blob_file_number, BlobLogHeader* header) {
    assert(header != nullptr);

    std::unique_ptr<FSRandomAccessFile> file;
    FileOptions file_options;
    constexpr IODebugContext* dbg = nullptr;
    const std::string blob_file_path = BlobFileName(dbname_, blob_file_number);
    FileSystem* fs = env_->GetFileSystem().get();
    SystemClock* clock = env_->GetSystemClock().get();

    Status s =
        fs->NewRandomAccessFile(blob_file_path, file_options, &file, dbg);
    if (!s.ok()) {
      return s;
    }

    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(file), blob_file_path, clock));
    BlobLogSequentialReader reader(std::move(file_reader), clock,
                                   /*statistics=*/nullptr);
    return reader.ReadHeader(header);
  }

  Status ReadBlobFileRecords(uint64_t blob_file_number, size_t num_records,
                             std::vector<BlobLogRecord>* records) {
    assert(records != nullptr);
    records->clear();

    std::unique_ptr<FSRandomAccessFile> file;
    FileOptions file_options;
    constexpr IODebugContext* dbg = nullptr;
    const std::string blob_file_path = BlobFileName(dbname_, blob_file_number);
    FileSystem* fs = env_->GetFileSystem().get();
    SystemClock* clock = env_->GetSystemClock().get();

    Status s =
        fs->NewRandomAccessFile(blob_file_path, file_options, &file, dbg);
    if (!s.ok()) {
      return s;
    }

    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(file), blob_file_path, clock));
    BlobLogSequentialReader reader(std::move(file_reader), clock,
                                   /*statistics=*/nullptr);

    BlobLogHeader header;
    s = reader.ReadHeader(&header);
    if (!s.ok()) {
      return s;
    }

    records->reserve(num_records);
    for (size_t i = 0; i < num_records; ++i) {
      BlobLogRecord record;
      s = reader.ReadRecord(&record,
                            BlobLogSequentialReader::kReadHeaderKeyBlob);
      if (!s.ok()) {
        return s;
      }
      records->push_back(std::move(record));
    }

    return Status::OK();
  }
  CompressionType GetSupportedCompressedBlobCompression() {
    static constexpr std::array<CompressionType, 6> kCandidates{
        kSnappyCompression, kLZ4Compression,   kZSTD,
        kZlibCompression,   kBZip2Compression, kLZ4HCCompression};
    for (CompressionType compression : kCandidates) {
      if (CompressionTypeSupported(compression)) {
        return compression;
      }
    }
    return kNoCompression;
  }

  void AssertOrderedTraceStoresLogicalPut(const Options& options,
                                          bool expect_blob_files) {
    const std::string trace_file = dbname_ + "/rocksdb.trace";
    Reopen(options);

    TraceOptions trace_options;
    trace_options.preserve_write_order = true;

    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_file, &trace_writer));
    ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));

    const std::string key = "bdw-key";
    const std::string value(64, 'x');
    ASSERT_OK(Put(key, value));
    ASSERT_OK(db_->EndTrace());

    ASSERT_EQ(Get(key), value);
    if (expect_blob_files) {
      ASSERT_GT(CountBlobFiles(), 0U);
    } else {
      ASSERT_EQ(CountBlobFiles(), 0U);
    }

    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(
        NewFileTraceReader(env_, EnvOptions(), trace_file, &trace_reader));

    std::vector<ColumnFamilyHandle*> handles{db_->DefaultColumnFamily()};
    std::unique_ptr<Replayer> replayer;
    ASSERT_OK(
        db_->NewDefaultReplayer(handles, std::move(trace_reader), &replayer));
    ASSERT_OK(replayer->Prepare());

    std::unique_ptr<TraceRecord> record;
    ASSERT_OK(replayer->Next(&record));
    ASSERT_NE(record, nullptr);
    ASSERT_EQ(record->GetTraceType(), kTraceWrite);

    auto* write_record = dynamic_cast<WriteQueryTraceRecord*>(record.get());
    ASSERT_NE(write_record, nullptr);

    class SingleWriteInspector : public WriteBatch::Handler {
     public:
      Status PutCF(uint32_t, const Slice& key, const Slice& value) override {
        saw_put = true;
        key_ = key.ToString();
        value_ = value.ToString();
        return Status::OK();
      }

      Status PutBlobIndexCF(uint32_t, const Slice&, const Slice&) override {
        saw_put_blob_index = true;
        return Status::OK();
      }

      bool saw_put = false;
      bool saw_put_blob_index = false;
      std::string key_;
      std::string value_;
    };

    WriteBatch traced_batch(write_record->GetWriteBatchRep().ToString());
    SingleWriteInspector inspector;
    ASSERT_OK(traced_batch.Iterate(&inspector));
    ASSERT_TRUE(inspector.saw_put);
    ASSERT_FALSE(inspector.saw_put_blob_index);
    ASSERT_EQ(inspector.key_, key);
    ASSERT_EQ(inspector.value_, value);
  }
};

TEST_F(
    DBBlobDirectWriteTest,
    DirectWriteDefaultRoundRobinSpreadsWriteBatchAcrossPartitionsBeforeAndAfterFlush) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 4;
  options.min_blob_size = 32;
  options.use_direct_reads = true;
  options.use_direct_io_for_flush_and_compaction = true;
  ASSERT_EQ(options.blob_direct_write_partition_strategy, nullptr);

  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    ROCKSDB_GTEST_SKIP("This test requires direct IO support");
    return;
  }
  ASSERT_OK(s);

  const std::array<std::string, 5> keys{
      "blob_key_0", "blob_key_1", "blob_key_2", "blob_key_3", "inline_key"};
  const std::array<std::string, 5> values{
      std::string(256, 'a'), std::string(256, 'b'), std::string(256, 'c'),
      std::string(256, 'd'), "tiny"};

  WriteBatch batch;
  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(batch.Put(keys[i], values[i]));
  }
  ASSERT_OK(batch.Delete("deleted_key"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }

  const std::array<Slice, 5> key_slices{Slice(keys[0]), Slice(keys[1]),
                                        Slice(keys[2]), Slice(keys[3]),
                                        Slice(keys[4])};
  auto assert_multiget = [&]() {
    std::array<PinnableSlice, 5> results;
    std::array<Status, 5> statuses;
    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                  key_slices.data(), results.data(), statuses.data());
    for (size_t i = 0; i < key_slices.size(); ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i], values[i]);
    }
  };
  assert_multiget();

  for (size_t i = 0; i < 4; ++i) {
    std::string value;
    bool value_found = true;
    ASSERT_TRUE(db_->KeyMayExist(ReadOptions(), db_->DefaultColumnFamily(),
                                 keys[i], &value, &value_found));
    ASSERT_FALSE(value_found);
  }

  std::string inline_value;
  bool inline_value_found = false;
  ASSERT_TRUE(db_->KeyMayExist(ReadOptions(), db_->DefaultColumnFamily(),
                               keys[4], &inline_value, &inline_value_found));
  ASSERT_TRUE(inline_value_found);
  ASSERT_EQ(inline_value, values[4]);

  ASSERT_OK(Flush());
  assert_multiget();

  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  size_t blob_files = 0;
  for (const auto& f : files) {
    if (f.size() > 5 && f.substr(f.size() - 5) == ".blob") {
      ++blob_files;
    }
  }
  ASSERT_EQ(blob_files, 4);

  Close();
  Reopen(options);

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyRoutesWritesToOneBlobFile) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_direct_write_partition_strategy =
      std::make_shared<FixedBlobDirectWritePartitionStrategy>(3);

  Reopen(options);

  const std::array<std::string, 4> keys{
      "strategy_blob_key_0", "strategy_blob_key_1", "strategy_blob_key_2",
      "strategy_blob_key_3"};
  const std::array<std::string, 4> values{
      std::string(128, 'a'), std::string(128, 'b'), std::string(128, 'c'),
      std::string(128, 'd')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  ASSERT_EQ(CountBlobFiles(), 1U);
  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 1U);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyReceivesOriginalWriteParameters) {
  const CompressionType compression = GetSupportedCompressedBlobCompression();
  if (compression == kNoCompression) {
    ROCKSDB_GTEST_SKIP("This test requires a supported blob compression");
    return;
  }

  auto strategy =
      std::make_shared<RecordingBlobDirectWritePartitionStrategy>(7);
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_compression_type = compression;
  options.blob_direct_write_partition_strategy = strategy;

  Reopen(options);

  const std::string key = "strategy_params_key";
  const std::string value(256, 'z');

  ASSERT_OK(Put(key, value));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get(key), value);

  const std::vector<RecordingBlobDirectWritePartitionStrategy::Call> calls =
      strategy->GetCalls();
  ASSERT_EQ(calls.size(), 1U);
  const auto& call = calls.front();
  ASSERT_EQ(call.num_partitions, 4U);
  ASSERT_EQ(call.column_family_id, db_->DefaultColumnFamily()->GetID());
  ASSERT_EQ(call.key, key);
  ASSERT_EQ(call.value, value);
  ASSERT_EQ(call.selected_partition, 7U);
  ASSERT_EQ(call.call_count, 1U);

  const std::vector<uint64_t> blob_file_numbers = GetBlobFileNumbers();
  ASSERT_EQ(blob_file_numbers.size(), 1U);

  BlobLogHeader header;
  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[0], &header));
  ASSERT_EQ(header.column_family_id, db_->DefaultColumnFamily()->GetID());
  ASSERT_EQ(header.compression, compression);
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyAppliesModuloToOutOfRangeReturns) {
  struct TestCase {
    uint32_t raw_partition;
    const char* suffix;
  };

  const std::array<TestCase, 3> cases{{
      {4, "equal"},
      {9, "greater"},
      {std::numeric_limits<uint32_t>::max(), "max"},
  }};

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.suffix);

    Options options = GetDirectWriteOptions();
    options.create_if_missing = true;
    options.blob_direct_write_partitions = 4;
    options.blob_file_size = 1 << 20;
    options.blob_direct_write_partition_strategy =
        std::make_shared<FixedBlobDirectWritePartitionStrategy>(
            test_case.raw_partition);

    DestroyAndReopen(options);

    const std::string key =
        std::string("partition_mod_key_") + test_case.suffix;
    const std::string value(128, test_case.suffix[0]);

    ASSERT_OK(Put(key, value));
    ASSERT_EQ(CountBlobFiles(), 1U);
    ASSERT_OK(Flush());
    ASSERT_EQ(CountBlobFiles(), 1U);
    ASSERT_EQ(Get(key), value);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCustomPartitionStrategyCalledForEachBlobInWriteBatch) {
  auto strategy =
      std::make_shared<RecordingBlobDirectWritePartitionStrategy>(2);
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_direct_write_partition_strategy = strategy;

  Reopen(options);

  const std::array<std::string, 3> blob_keys{
      "batch_blob_key_0", "batch_blob_key_1", "batch_blob_key_2"};
  const std::array<std::string, 3> blob_values{
      std::string(128, 'a'), std::string(128, 'b'), std::string(128, 'c')};
  const std::string inline_key = "batch_inline_key";
  const std::string inline_value = "tiny";

  WriteBatch batch;
  for (size_t i = 0; i < blob_keys.size(); ++i) {
    ASSERT_OK(
        batch.Put(db_->DefaultColumnFamily(), blob_keys[i], blob_values[i]));
  }
  ASSERT_OK(batch.Put(db_->DefaultColumnFamily(), inline_key, inline_value));
  ASSERT_OK(batch.Delete(db_->DefaultColumnFamily(), "batch_deleted_key"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  const auto calls = strategy->GetCalls();
  ASSERT_EQ(calls.size(), blob_keys.size());
  for (size_t i = 0; i < blob_keys.size(); ++i) {
    ASSERT_EQ(calls[i].num_partitions, 4U);
    ASSERT_EQ(calls[i].column_family_id, db_->DefaultColumnFamily()->GetID());
    ASSERT_EQ(calls[i].key, blob_keys[i]);
    ASSERT_EQ(calls[i].value, blob_values[i]);
    ASSERT_EQ(calls[i].selected_partition, 2U);
    ASSERT_EQ(calls[i].call_count, i + 1);
    ASSERT_EQ(Get(blob_keys[i]), blob_values[i]);
  }

  ASSERT_EQ(Get(inline_key), inline_value);
  ASSERT_EQ(CountBlobFiles(), 1U);
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteSharedCustomPartitionStrategyAcrossColumnFamilies) {
  Reopen(GetBlobDirectWriteCompatibleOptions());

  auto strategy =
      std::make_shared<RecordingBlobDirectWritePartitionStrategy>(1);
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.blob_file_size = 1 << 20;
  options.blob_direct_write_partition_strategy = strategy;

  ColumnFamilyHandle* first_cfh = nullptr;
  ColumnFamilyHandle* second_cfh = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options, "bdw_one", &first_cfh));
  ASSERT_OK(db_->CreateColumnFamily(options, "bdw_two", &second_cfh));

  const std::string first_key = "shared_cf_key_0";
  const std::string second_key = "shared_cf_key_1";
  const std::string first_value(128, 'x');
  const std::string second_value(128, 'y');

  ASSERT_OK(db_->Put(WriteOptions(), first_cfh, first_key, first_value));
  ASSERT_OK(db_->Put(WriteOptions(), second_cfh, second_key, second_value));

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), first_cfh, first_key, &value));
  ASSERT_EQ(value, first_value);
  ASSERT_OK(db_->Get(ReadOptions(), second_cfh, second_key, &value));
  ASSERT_EQ(value, second_value);

  const auto calls = strategy->GetCalls();
  ASSERT_EQ(calls.size(), 2U);
  std::set<uint32_t> seen_cf_ids;
  std::set<std::string> seen_keys;
  for (const auto& call : calls) {
    ASSERT_EQ(call.num_partitions, 4U);
    ASSERT_EQ(call.selected_partition, 1U);
    seen_cf_ids.insert(call.column_family_id);
    seen_keys.insert(call.key);
  }
  ASSERT_EQ(seen_cf_ids.size(), 2U);
  ASSERT_TRUE(seen_cf_ids.count(first_cfh->GetID()) > 0);
  ASSERT_TRUE(seen_cf_ids.count(second_cfh->GetID()) > 0);
  ASSERT_TRUE(seen_keys.count(first_key) > 0);
  ASSERT_TRUE(seen_keys.count(second_key) > 0);

  ASSERT_OK(db_->DestroyColumnFamilyHandle(first_cfh));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second_cfh));
}
TEST_F(DBBlobDirectWriteTest,
       DirectWriteCloseFlushesWhenShutdownFlushIsDisabled) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 4;
  options.min_blob_size = 32;
  options.avoid_flush_during_shutdown = true;

  Reopen(options);

  const std::array<std::string, 4> keys{"close_blob_key_0", "close_blob_key_1",
                                        "close_blob_key_2", "close_blob_key_3"};
  const std::array<std::string, 4> values{
      std::string(128, 'a'), std::string(128, 'b'), std::string(128, 'c'),
      std::string(128, 'd')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  Close();
  Reopen(options);

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteAutoFlushPreservesBlobGenerationOrder) {
  Options options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = false;
  options.disable_auto_compactions = true;
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;
  options.max_write_buffer_number = 4;
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(100, nullptr, false);

  Reopen(options);

  WriteOptions write_options;
  write_options.disableWAL = true;

  const std::string first_value(128, 'a');
  const std::string second_value(128, 'b');

  ASSERT_OK(db_->Put(write_options, "first_key", first_value));
  ASSERT_OK(db_->Put(write_options, "second_key", second_value));

  // The tiny shared write buffer forces the first write to queue a flush. The
  // second write then goes through PreprocessWrite(), which can switch
  // memtables before the transformed batch is inserted. By the time the test
  // inspects DB state, that auto flush may already have finished, so wait for
  // any in-flight auto flush instead of asserting on an instantaneous imm
  // count.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("first_key"), first_value);
  ASSERT_EQ(Get("second_key"), second_value);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_files.size(), 2U);

  std::vector<uint64_t> blob_file_numbers;
  blob_file_numbers.reserve(cf_meta.blob_files.size());
  for (const auto& blob_meta : cf_meta.blob_files) {
    blob_file_numbers.push_back(blob_meta.blob_file_number);
  }
  std::sort(blob_file_numbers.begin(), blob_file_numbers.end());

  std::vector<LiveFileMetaData> live_files;
  db_->GetLiveFilesMetaData(&live_files);
  ASSERT_EQ(live_files.size(), 2U);
  std::sort(live_files.begin(), live_files.end(),
            [](const LiveFileMetaData& lhs, const LiveFileMetaData& rhs) {
              return lhs.smallest_seqno < rhs.smallest_seqno;
            });

  ASSERT_EQ(live_files[0].oldest_blob_file_number, blob_file_numbers[0]);
  ASSERT_EQ(live_files[1].oldest_blob_file_number, blob_file_numbers[1]);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteCreateMissingColumnFamilyOnOpen) {
  Reopen(GetBlobDirectWriteCompatibleOptions());

  Options default_options = GetBlobDirectWriteCompatibleOptions();
  default_options.create_if_missing = false;
  default_options.create_missing_column_families = true;

  Options direct_write_options = GetDirectWriteOptions();
  direct_write_options.create_if_missing = false;
  direct_write_options.create_missing_column_families = true;

  const std::vector<std::string> cfs{kDefaultColumnFamilyName, "bdw"};
  const std::vector<Options> options{default_options, direct_write_options};
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));

  ASSERT_EQ(handles_.size(), 2U);
  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  const std::string key = "cf_key";
  const std::string value(128, 'd');
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], key, value));
  ASSERT_EQ(Get(1, key), value);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRejectsMemPurge) {
  Options options = GetDirectWriteOptions();
  options.experimental_mempurge_threshold = 1.0;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());

  options.experimental_mempurge_threshold = 0.0;
  ASSERT_OK(TryReopen(options));
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"experimental_mempurge_threshold", "1.0"}})
                  .IsNotSupported());
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRejectsUnsupportedWriteModes) {
  auto assert_rejected = [&](const Options& options,
                             const std::string& expected_substr) {
    Status s = TryReopen(options);
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_NE(s.ToString().find(expected_substr), std::string::npos)
        << s.ToString();
  };

  Options options = GetDirectWriteOptions();
  options.enable_pipelined_write = true;
  assert_rejected(options, "pipelined writes");

  options = GetDirectWriteOptions();
  options.allow_concurrent_memtable_write = true;
  assert_rejected(options, "concurrent memtable writes");

  options = GetDirectWriteOptions();
  options.unordered_write = true;
  assert_rejected(options, "unordered writes");

  options = GetDirectWriteOptions();
  options.two_write_queues = true;
  assert_rejected(options, "two write queues");
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteIteratorMixedValuesBeforeAndAfterFlush) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 20;

  Reopen(options);

  const std::array<std::string, 4> keys{"a_small", "b_large", "c_small",
                                        "d_large"};
  const std::array<std::string, 4> values{"tiny", std::string(80, 'B'), "mini",
                                          std::string(80, 'D')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  auto verify_iteration = [&]() {
    ReadOptions read_options;
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    for (size_t i = 0; i < keys.size(); ++i) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), values[i]);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (size_t i = keys.size(); i-- > 0;) {
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), values[i]);
      iter->Prev();
    }
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());

    iter->Seek("b_large");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), keys[1]);
    ASSERT_EQ(iter->value(), values[1]);
  };

  verify_iteration();
  ASSERT_OK(Flush());
  verify_iteration();
}

TEST_F(DBBlobDirectWriteTest, DirectWriteImmutableMemtableRead) {
  Options options = GetDirectWriteOptions();

  Reopen(options);

  constexpr size_t kNumKeys = 6;
  std::array<std::string, kNumKeys> keys;
  std::array<std::string, kNumKeys> values;
  for (size_t i = 0; i < kNumKeys; ++i) {
    keys[i] = "imm_key_" + std::to_string(i);
    values[i] = std::string(96 + i, static_cast<char>('A' + i));
    ASSERT_OK(Put(keys[i], values[i]));
  }

  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  auto verify_reads = [&]() {
    std::array<Slice, kNumKeys> key_slices{Slice(keys[0]), Slice(keys[1]),
                                           Slice(keys[2]), Slice(keys[3]),
                                           Slice(keys[4]), Slice(keys[5])};
    std::array<PinnableSlice, kNumKeys> results;
    std::array<Status, kNumKeys> statuses;

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_EQ(Get(keys[i]), values[i]);
    }

    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                  key_slices.data(), results.data(), statuses.data());
    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(results[i], values[i]);
    }
  };

  verify_reads();
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  verify_reads();
}

// Verifies active blob direct-write files promise no write reopen without also
// promising no concurrent readers.
TEST_F(DBBlobDirectWriteTest, DirectWriteUsesReadableContractForRemoteFile) {
  auto remote_fs =
      std::make_shared<RemoteBlobVisibilityFileSystem>(env_->GetFileSystem());
  std::unique_ptr<Env> remote_env(new CompositeEnvWrapper(env_, remote_fs));

  Options options = GetDirectWriteOptions();
  options.env = remote_env.get();
  Reopen(options);

  const std::string key = "remote_blob_key";
  const std::string value(128, 'w');

  ASSERT_OK(Put(key, value));
  ASSERT_TRUE(remote_fs->SawNoReopenForWriteWriterContract());
  ASSERT_FALSE(remote_fs->SawNoReadersWhileOpenForWriteWriterContract());

  ASSERT_EQ(Get(key), value);
  ASSERT_FALSE(remote_fs->SawNoReadersWhileOpenForWriteReaderContract());

  Close();
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRefreshesReaderAfterFlush) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 1;

  Reopen(options);

  const std::string first_key = "first_key";
  const std::string second_key = "second_key";
  const std::string third_key = "third_key";
  const std::string first_value(128, 'a');
  const std::string second_value(128, 'b');
  const std::string third_value(128, 'c');

  ASSERT_OK(Put(first_key, first_value));
  // This read opens and caches an in-flight direct-write reader before the
  // blob file is sealed by flush.
  ASSERT_EQ(Get(first_key), first_value);

  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Put(third_key, third_value));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);

  const std::array<std::string, 2> keys{second_key, third_key};
  const std::array<std::string, 2> values{second_value, third_value};
  const std::array<Slice, 2> key_slices{Slice(keys[0]), Slice(keys[1])};
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                key_slices.data(), results.data(), statuses.data());
  for (size_t i = 0; i < key_slices.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRefreshesReaderWhileFileIsGrowing) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 1;

  Reopen(options);

  const std::string first_key = "active_first_key";
  const std::string second_key = "active_second_key";
  const std::string third_key = "active_third_key";
  const std::string first_value(128, 'x');
  const std::string second_value(128, 'y');
  const std::string third_value(128, 'z');

  ASSERT_OK(Put(first_key, first_value));
  // This read caches a reader for the current active direct-write file before
  // more blob records extend the file.
  ASSERT_EQ(Get(first_key), first_value);

  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Put(third_key, third_value));

  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);

  const std::array<std::string, 2> keys{second_key, third_key};
  const std::array<std::string, 2> values{second_value, third_value};
  const std::array<Slice, 2> key_slices{Slice(keys[0]), Slice(keys[1])};
  std::array<PinnableSlice, 2> results;
  std::array<Status, 2> statuses;

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), key_slices.size(),
                key_slices.data(), results.data(), statuses.data());
  for (size_t i = 0; i < key_slices.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(results[i], values[i]);
  }
}

TEST_F(DBBlobDirectWriteTest, DirectWriteBlobFileRotationSinglePartition) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 512;

  Reopen(options);

  constexpr size_t kNumKeys = 12;
  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put("rot_key_" + std::to_string(i),
                  std::string(120, static_cast<char>('a' + i))));
  }

  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_EQ(Get("rot_key_" + std::to_string(i)),
              std::string(120, static_cast<char>('a' + i)));
  }

  ASSERT_OK(Flush());
  ASSERT_GT(CountBlobFiles(), 1U);

  Close();
  Reopen(options);

  for (size_t i = 0; i < kNumKeys; ++i) {
    ASSERT_EQ(Get("rot_key_" + std::to_string(i)),
              std::string(120, static_cast<char>('a' + i)));
  }
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCompressionUpdateReusesCachedCompressorAndRotatesFiles) {
  const CompressionType compressed = GetSupportedCompressedBlobCompression();
  if (compressed == kNoCompression) {
    return;
  }

  std::string compressed_option_value;
  ASSERT_OK(GetStringFromCompressionType(&compressed_option_value, compressed));

  Options options = GetDirectWriteOptions();
  options.blob_file_size = 1 << 20;
  options.blob_compression_type = compressed;

  Reopen(options);

  const std::string first_key = "compressed_before_update";
  const std::string second_key = "uncompressed_between_updates";
  const std::string third_key = "compressed_after_update";
  const std::string first_value(256, 'a');
  const std::string second_value(256, 'b');
  const std::string third_value(256, 'c');

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_EQ(Get(first_key), first_value);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"blob_compression_type", "kNoCompression"}}));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);

  ASSERT_OK(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"blob_compression_type", compressed_option_value}}));
  ASSERT_OK(Put(third_key, third_value));
  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);

  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 3U);

  const std::vector<uint64_t> blob_file_numbers = GetBlobFileNumbers();
  ASSERT_EQ(blob_file_numbers.size(), 3U);

  BlobLogHeader header;
  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[0], &header));
  ASSERT_EQ(header.compression, compressed);

  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[1], &header));
  ASSERT_EQ(header.compression, kNoCompression);

  ASSERT_OK(ReadBlobFileHeader(blob_file_numbers[2], &header));
  ASSERT_EQ(header.compression, compressed);

  Close();
  Reopen(options);

  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCompressionOptionsUpdateRebuildsCachedCompressor) {
#ifndef ZSTD
  ROCKSDB_GTEST_SKIP("This test requires ZSTD support");
  return;
#else
  // Goal: verify BDW picks up dynamic blob_compression_opts updates instead of
  // reusing a stale per-thread compressor keyed only by compression type.
  // We toggle the ZSTD frame checksum flag on and off, then inspect the raw
  // blob records in one blob file. With identical inputs and level, enabling
  // checksum should add exactly 4 bytes to the stored compressed blob.
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 1 << 20;
  options.blob_compression_type = kZSTD;
  options.blob_compression_opts.level = 1;
  options.blob_compression_opts.checksum = false;

  Reopen(options);

  const std::string first_key = "checksum_off_before_update";
  const std::string second_key = "checksum_on_after_update";
  const std::string third_key = "checksum_off_after_second_update";
  const std::string value(256, 'v');

  ASSERT_OK(Put(first_key, value));
  ASSERT_EQ(Get(first_key), value);

  ASSERT_OK(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"blob_compression_opts", "{level=1;checksum=true}"}}));
  ASSERT_OK(Put(second_key, value));
  ASSERT_EQ(Get(second_key), value);

  ASSERT_OK(
      db_->SetOptions(db_->DefaultColumnFamily(),
                      {{"blob_compression_opts", "{level=1;checksum=false}"}}));
  ASSERT_OK(Put(third_key, value));
  ASSERT_EQ(Get(third_key), value);

  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 1U);

  const std::vector<uint64_t> blob_file_numbers = GetBlobFileNumbers();
  ASSERT_EQ(blob_file_numbers.size(), 1U);

  std::vector<BlobLogRecord> records;
  ASSERT_OK(
      ReadBlobFileRecords(blob_file_numbers[0], /*num_records=*/3, &records));
  ASSERT_EQ(records.size(), 3U);
  ASSERT_EQ(records[0].key.ToString(), first_key);
  ASSERT_EQ(records[1].key.ToString(), second_key);
  ASSERT_EQ(records[2].key.ToString(), third_key);
  ASSERT_EQ(records[0].value.size(), records[2].value.size());
  ASSERT_EQ(records[1].value.size(), records[0].value.size() + 4);

  Close();
  Reopen(options);

  ASSERT_EQ(Get(first_key), value);
  ASSERT_EQ(Get(second_key), value);
  ASSERT_EQ(Get(third_key), value);
#endif  // ZSTD
}

TEST_F(DBBlobDirectWriteTest, DirectWriteFailedBatchTrackedAsInitialGarbage) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 1 << 20;
  options.blob_compression_type = kNoCompression;

  Reopen(options);

  const std::string failed_key = "failed_key";
  const std::string failed_value(128, 'f');
  const uint64_t failed_record_bytes =
      BlobLogRecord::kHeaderSize + failed_key.size() + failed_value.size();

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:AfterBlobDirectWrite", [](void* arg) {
        auto* status = static_cast<Status*>(arg);
        assert(status != nullptr);
        *status = Status::IOError("Injected post-BDW failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(Put(failed_key, failed_value).IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  const std::string good_key = "good_key";
  const std::string good_value(96, 'g');
  const uint64_t good_record_bytes =
      BlobLogRecord::kHeaderSize + good_key.size() + good_value.size();

  ASSERT_OK(Put(good_key, good_value));
  ASSERT_EQ(Get(failed_key), "NOT_FOUND");
  ASSERT_EQ(Get(good_key), good_value);

  ASSERT_OK(Flush());

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);

  const BlobMetaData& blob_meta = cf_meta.blob_files[0];
  ASSERT_EQ(blob_meta.total_blob_count, 2U);
  ASSERT_EQ(blob_meta.total_blob_bytes,
            failed_record_bytes + good_record_bytes);
  ASSERT_EQ(blob_meta.garbage_blob_count, 1U);
  ASSERT_EQ(blob_meta.garbage_blob_bytes, failed_record_bytes);

  Close();
  Reopen(options);

  ASSERT_EQ(Get(failed_key), "NOT_FOUND");
  ASSERT_EQ(Get(good_key), good_value);

  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);
  ASSERT_EQ(cf_meta.blob_files[0].total_blob_count, 2U);
  ASSERT_EQ(cf_meta.blob_files[0].garbage_blob_count, 1U);
  ASSERT_EQ(cf_meta.blob_files[0].garbage_blob_bytes, failed_record_bytes);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteRollbackMismatchReturnsCorruption) {
  Reopen(GetDirectWriteOptions());

  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  Status s =
      mgr->MarkBlobWriteAsGarbage(/*file_number=*/123456789,
                                  /*blob_count=*/1,
                                  /*blob_bytes=*/BlobLogRecord::kHeaderSize);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteCountTracksCreatedAndDroppedColumnFamily) {
  Reopen(GetBlobDirectWriteCompatibleOptions());

  ASSERT_FALSE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  ColumnFamilyHandle* bdw_cfh = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(GetDirectWriteOptions(), "bdw", &bdw_cfh));
  ASSERT_NE(bdw_cfh, nullptr);
  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  const std::string key = "cf_key";
  const std::string value(128, 'c');
  ASSERT_OK(db_->Put(WriteOptions(), bdw_cfh, key, value));

  PinnableSlice read_value;
  ASSERT_OK(db_->Get(ReadOptions(), bdw_cfh, key, &read_value));
  ASSERT_EQ(read_value.ToString(), value);

  ASSERT_OK(db_->DropColumnFamily(bdw_cfh));
  ASSERT_FALSE(dbfull()->HasAnyBlobDirectWriteColumnFamily());
  ASSERT_OK(db_->DestroyColumnFamilyHandle(bdw_cfh));
}

TEST_F(DBBlobDirectWriteTest,
       DirectWritePreparedGenerationsStayReusableUntilCommit) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 1 << 20;

  Reopen(options);

  ASSERT_TRUE(dbfull()->HasAnyBlobDirectWriteColumnFamily());

  auto* cfh = static_cast_with_check<ColumnFamilyHandleImpl>(
      db_->DefaultColumnFamily());
  auto* mgr = cfh->cfd()->blob_partition_manager();
  ASSERT_NE(mgr, nullptr);

  const std::string key = "retry_key";
  const std::string value(128, 'r');
  ASSERT_OK(Put(key, value));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfh->cfd()));

  std::vector<BlobFileAddition> additions1;
  std::vector<BlobFileGarbage> garbages1;
  // Simulate a flush retry: prepare once, skip commit, then prepare the same
  // generation again and verify it reuses the already sealed blob file.
  const std::vector<uint64_t> memtable_ids = {
      cfh->cfd()->imm()->GetEarliestMemTableID()};
  ASSERT_OK(mgr->PrepareFlushAdditions(WriteOptions(), memtable_ids,
                                       &additions1, &garbages1));
  ASSERT_EQ(additions1.size(), 1U);
  ASSERT_TRUE(garbages1.empty());
  ASSERT_EQ(Get(key), value);
  ASSERT_EQ(CountBlobFiles(), 1U);

  std::vector<BlobFileAddition> additions2;
  std::vector<BlobFileGarbage> garbages2;
  ASSERT_OK(mgr->PrepareFlushAdditions(WriteOptions(), memtable_ids,
                                       &additions2, &garbages2));
  ASSERT_EQ(additions2.size(), 1U);
  ASSERT_TRUE(garbages2.empty());
  ASSERT_EQ(additions2[0].GetBlobFileNumber(),
            additions1[0].GetBlobFileNumber());
  ASSERT_EQ(additions2[0].GetTotalBlobCount(),
            additions1[0].GetTotalBlobCount());
  ASSERT_EQ(additions2[0].GetTotalBlobBytes(),
            additions1[0].GetTotalBlobBytes());

  ASSERT_EQ(Get(key), value);
  ASSERT_OK(Flush());
  ASSERT_EQ(Get(key), value);
  ASSERT_EQ(CountBlobFiles(), 1U);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.blob_file_count, 1U);
  ASSERT_EQ(cf_meta.blob_files.size(), 1U);
  ASSERT_EQ(cf_meta.blob_files[0].total_blob_count, 1U);

  Close();
  Reopen(options);

  ASSERT_EQ(Get(key), value);
  ASSERT_EQ(CountBlobFiles(), 1U);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteBoundaryValues) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 20;

  Reopen(options);

  const std::string below(19, 'b');
  const std::string exact(20, 'e');
  const std::string above(21, 'a');

  ASSERT_OK(Put("below", below));
  ASSERT_OK(Put("exact", exact));
  ASSERT_OK(Put("above", above));

  ASSERT_EQ(Get("below"), below);
  ASSERT_EQ(Get("exact"), exact);
  ASSERT_EQ(Get("above"), above);

  ASSERT_OK(Flush());
  ASSERT_EQ(CountBlobFiles(), 1U);
  ASSERT_EQ(Get("below"), below);
  ASSERT_EQ(Get("exact"), exact);
  ASSERT_EQ(Get("above"), above);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteWriteBatchNoQualifyingValues) {
  Options options = GetDirectWriteOptions();
  options.min_blob_size = 1024;

  Reopen(options);

  WriteBatch batch;
  ASSERT_OK(batch.Put("k1", "small_v1"));
  ASSERT_OK(batch.Put("k2", "small_v2"));
  ASSERT_OK(batch.Delete("k3"));
  ASSERT_OK(batch.SingleDelete("k4"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_EQ(Get("k1"), "small_v1");
  ASSERT_EQ(Get("k2"), "small_v2");
  ASSERT_EQ(Get("k3"), "NOT_FOUND");
  ASSERT_EQ(CountBlobFiles(), 0U);

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("k1"), "small_v1");
  ASSERT_EQ(Get("k2"), "small_v2");
  ASSERT_EQ(CountBlobFiles(), 0U);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteDeleteAndReput) {
  Options options = GetDirectWriteOptions();

  Reopen(options);

  const std::string first_value(96, '1');
  const std::string second_value(128, '2');

  ASSERT_OK(Put("reput_key", first_value));
  ASSERT_EQ(Get("reput_key"), first_value);

  ASSERT_OK(Delete("reput_key"));
  ASSERT_EQ(Get("reput_key"), "NOT_FOUND");

  ASSERT_OK(Put("reput_key", second_value));
  ASSERT_EQ(Get("reput_key"), second_value);

  ASSERT_OK(Flush());
  ASSERT_EQ(Get("reput_key"), second_value);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteSnapshotIsolation) {
  Options options = GetDirectWriteOptions();

  Reopen(options);

  const std::string first_value(96, '1');
  const std::string second_value(128, '2');

  ASSERT_OK(Put("snap_key", first_value));
  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(Put("snap_key", second_value));
  ASSERT_OK(Put("snap_new_key", second_value));

  ASSERT_EQ(Get("snap_key"), second_value);
  ASSERT_EQ(Get("snap_new_key"), second_value);

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::string value;

  ASSERT_OK(
      db_->Get(read_options, db_->DefaultColumnFamily(), "snap_key", &value));
  ASSERT_EQ(value, first_value);
  ASSERT_TRUE(
      db_->Get(read_options, db_->DefaultColumnFamily(), "snap_new_key", &value)
          .IsNotFound());

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBlobDirectWriteTest,
       DirectWriteLazyIteratorReadSurvivesCompactionWithMultiplePartitions) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.disable_auto_compactions = true;

  Reopen(options);

  const std::array<std::string, 4> keys{"lazy_iter_key_0", "lazy_iter_key_1",
                                        "lazy_iter_key_2", "lazy_iter_key_3"};
  const std::array<std::string, 4> values{
      std::string(96, 'a'), std::string(96, 'b'), std::string(96, 'c'),
      std::string(96, 'd')};
  const std::array<std::string, 4> new_values{
      std::string(128, 'w'), std::string(128, 'x'), std::string(128, 'y'),
      std::string(128, 'z')};

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }
  ASSERT_OK(Flush());

  auto get_blob_file_number = [this](const std::string& key) -> uint64_t {
    PinnableSlice value;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db_->DefaultColumnFamily();
    get_impl_options.value = &value;
    get_impl_options.is_blob_index = &is_blob_index;
    Status s = dbfull()->GetImpl(ReadOptions(), key, get_impl_options);
    EXPECT_OK(s);
    EXPECT_TRUE(is_blob_index);
    if (!s.ok() || !is_blob_index) {
      return kInvalidBlobFileNumber;
    }

    BlobIndex blob_index;
    s = blob_index.DecodeFrom(value);
    EXPECT_OK(s);
    if (!s.ok()) {
      return kInvalidBlobFileNumber;
    }
    return blob_index.file_number();
  };

  std::array<uint64_t, 4> original_blob_files{};
  for (size_t i = 0; i < keys.size(); ++i) {
    original_blob_files[i] = get_blob_file_number(keys[i]);
    ASSERT_NE(original_blob_files[i], kInvalidBlobFileNumber);
  }

  const uint64_t oldest_blob_file =
      *std::min_element(original_blob_files.begin(), original_blob_files.end());
  size_t target_idx = 0;
  while (target_idx < original_blob_files.size() &&
         original_blob_files[target_idx] == oldest_blob_file) {
    ++target_idx;
  }
  ASSERT_LT(target_idx, original_blob_files.size());

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->Seek(keys[target_idx]);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), keys[target_idx]);
  ASSERT_TRUE(iter->value().empty());
  ASSERT_OK(iter->status());

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], new_values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  auto get_live_blob_file_numbers = [this]() -> std::set<uint64_t> {
    std::set<uint64_t> blob_file_numbers;

    VersionSet* const versions = dbfull()->GetVersionSet();
    EXPECT_NE(versions, nullptr);
    if (versions == nullptr || versions->GetColumnFamilySet() == nullptr) {
      return blob_file_numbers;
    }

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    EXPECT_NE(cfd, nullptr);
    if (cfd == nullptr) {
      return blob_file_numbers;
    }

    Version* const dummy_versions = cfd->dummy_versions();
    EXPECT_NE(dummy_versions, nullptr);
    if (dummy_versions == nullptr) {
      return blob_file_numbers;
    }

    for (Version* v = dummy_versions->Next(); v != dummy_versions;
         v = v->Next()) {
      EXPECT_NE(v, nullptr);
      if (v == nullptr) {
        continue;
      }
      for (const auto& meta : v->storage_info()->GetBlobFiles()) {
        EXPECT_NE(meta, nullptr);
        if (meta == nullptr) {
          continue;
        }
        blob_file_numbers.insert(meta->GetBlobFileNumber());
      }
    }

    return blob_file_numbers;
  };

  const auto live_blob_files = get_live_blob_file_numbers();
  ASSERT_TRUE(live_blob_files.find(original_blob_files[target_idx]) !=
              live_blob_files.end());

  ASSERT_TRUE(iter->PrepareValue());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), values[target_idx]);
}

TEST_F(
    DBBlobDirectWriteTest,
    DirectWriteLazyIteratorFromMemtableSurvivesFlushCompactionWithMultiplePartitions) {
  Options options = GetDirectWriteOptions();
  options.blob_direct_write_partitions = 4;
  options.disable_auto_compactions = true;

  Reopen(options);

  const std::array<std::string, 4> keys{
      "mem_lazy_iter_key_0", "mem_lazy_iter_key_1", "mem_lazy_iter_key_2",
      "mem_lazy_iter_key_3"};
  const std::array<std::string, 4> values{
      std::string(96, 'a'), std::string(96, 'b'), std::string(96, 'c'),
      std::string(96, 'd')};
  const std::array<std::string, 4> new_values{
      std::string(128, 'w'), std::string(128, 'x'), std::string(128, 'y'),
      std::string(128, 'z')};

  auto get_blob_file_number = [this](const std::string& key) -> uint64_t {
    PinnableSlice value;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db_->DefaultColumnFamily();
    get_impl_options.value = &value;
    get_impl_options.is_blob_index = &is_blob_index;
    Status s = dbfull()->GetImpl(ReadOptions(), key, get_impl_options);
    EXPECT_OK(s);
    EXPECT_TRUE(is_blob_index);
    if (!s.ok() || !is_blob_index) {
      return kInvalidBlobFileNumber;
    }

    BlobIndex blob_index;
    s = blob_index.DecodeFrom(value);
    EXPECT_OK(s);
    if (!s.ok()) {
      return kInvalidBlobFileNumber;
    }
    return blob_index.file_number();
  };

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }

  std::array<uint64_t, 4> original_blob_files{};
  for (size_t i = 0; i < keys.size(); ++i) {
    original_blob_files[i] = get_blob_file_number(keys[i]);
    ASSERT_NE(original_blob_files[i], kInvalidBlobFileNumber);
  }

  const uint64_t oldest_blob_file =
      *std::min_element(original_blob_files.begin(), original_blob_files.end());
  size_t target_idx = 0;
  while (target_idx < original_blob_files.size() &&
         original_blob_files[target_idx] == oldest_blob_file) {
    ++target_idx;
  }
  ASSERT_LT(target_idx, original_blob_files.size());

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->Seek(keys[target_idx]);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), keys[target_idx]);
  ASSERT_TRUE(iter->value().empty());
  ASSERT_OK(iter->status());

  ASSERT_OK(Flush());

  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], new_values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  ASSERT_TRUE(iter->PrepareValue());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), values[target_idx]);
}

TEST_F(DBBlobDirectWriteTest, DirectWriteDelayedReadUsesFinalizedFileChecksum) {
  auto capturing_fs = std::make_shared<BlobFileChecksumCapturingFileSystem>(
      env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, capturing_fs));

  Options options = GetDirectWriteOptions();
  options.env = env.get();
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  Defer close_db([this]() { Close(); });
  Reopen(options);

  const std::string key = "delayed-read-key";
  const std::string value(96, 'v');
  ASSERT_OK(Put(key, value));

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->Seek(key);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), key);
  ASSERT_TRUE(iter->value().empty());

  ASSERT_OK(Flush());

  std::vector<ColumnFamilyMetaData> column_family_metadata;
  db_->GetAllColumnFamilyMetaData(&column_family_metadata);
  ASSERT_EQ(column_family_metadata.size(), 1);
  ASSERT_EQ(column_family_metadata[0].blob_files.size(), 1);
  const BlobMetaData& blob_metadata = column_family_metadata[0].blob_files[0];
  ASSERT_FALSE(blob_metadata.checksum_value.empty());
  ASSERT_EQ(blob_metadata.checksum_method, "FileChecksumCrc32c");

  capturing_fs->Reset();
  ASSERT_TRUE(iter->PrepareValue());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->value().ToString(), value);
  ASSERT_GT(capturing_fs->GetCaptureCount(), 0);
  ASSERT_EQ(capturing_fs->GetFileChecksum(), blob_metadata.checksum_value);
  ASSERT_EQ(capturing_fs->GetFileChecksumFuncName(),
            blob_metadata.checksum_method);

  const int capture_count = capturing_fs->GetCaptureCount();
  ASSERT_EQ(Get(key), value);
  ASSERT_EQ(capturing_fs->GetCaptureCount(), capture_count);
}

TEST_F(DBBlobDirectWriteTest, OrderedTraceUsesLogicalBatchForBlobDirectWrite) {
  AssertOrderedTraceStoresLogicalPut(GetDirectWriteOptions(),
                                     /*expect_blob_files=*/true);
}

TEST_F(DBBlobDirectWriteTest,
       OrderedTraceUsesLogicalBatchForBlobDirectWritePipelinedWrite) {
  Options options = GetDirectWriteOptions();
  options.enable_pipelined_write = true;

  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();

  options.enable_blob_direct_write = false;
  AssertOrderedTraceStoresLogicalPut(options, /*expect_blob_files=*/false);
}

TEST_F(DBBlobDirectWriteTest, OverlappingFlushesPublishTheirOwnBlobFiles) {
  for (bool atomic_flush : {false, true}) {
    SCOPED_TRACE("atomic_flush=" + std::to_string(atomic_flush));
    Options options = GetDirectWriteOptions();
    options.atomic_flush = atomic_flush;
    options.max_background_jobs = 8;
    options.max_write_buffer_number = 4;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    const std::string value(1024, 'v');

    // The flush of the memtable holding "key1" waits while it writes its
    // table file, until a second flush, of the memtable holding "key2", has
    // finished its own table file.
    std::atomic<int> table_writes{0};
    std::atomic<bool> released{false};
    std::promise<void> first_flush_writing;
    std::promise<void> release_first_flush;
    std::shared_future<void> release_first_flush_future =
        release_first_flush.get_future().share();
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::WriteLevel0Table:num_memtables", [&](void*) {
          if (table_writes.fetch_add(1) == 0) {
            first_flush_writing.set_value();
            release_first_flush_future.wait();
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        atomic_flush ? "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitCV"
                     : "DBImpl::FlushMemTableToOutputFile:Finish",
        [&](void*) {
          if (!released.exchange(true)) {
            release_first_flush.set_value();
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    FlushOptions no_wait;
    no_wait.wait = false;
    no_wait.allow_write_stall = true;
    ASSERT_OK(Put("key1", value));
    ASSERT_OK(db_->Flush(no_wait));
    first_flush_writing.get_future().wait();
    ASSERT_OK(Put("key2", value));
    ASSERT_OK(db_->Flush(no_wait));

    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(dbfull()->TEST_GetBGError());
    ASSERT_EQ(2, NumTableFilesAtLevel(0));

    Reopen(options);
    ASSERT_EQ(value, Get("key1"));
    ASSERT_EQ(value, Get("key2"));
  }
}

TEST_F(DBBlobDirectWriteTest, RetryAfterOlderOverlappingFlushFails) {
  Options options = GetDirectWriteOptions();
  options.max_background_jobs = 8;
  options.max_write_buffer_number = 4;
  options.disable_auto_compactions = true;
  options.max_bgerror_resume_count = 0;
  DestroyAndReopen(options);
  const std::string value(1024, 'v');

  // The flush of the memtable holding "key2" finishes while the older flush,
  // of the memtable holding "key1", is still writing its table file. The
  // newer flush's results wait to be installed after the older one's, but the
  // older flush fails, which rolls the newer memtable back too.
  std::atomic<int> table_writes{0};
  std::atomic<bool> released{false};
  std::atomic<bool> failed{false};
  std::promise<void> first_flush_writing;
  std::promise<void> release_first_flush;
  std::shared_future<void> release_first_flush_future =
      release_first_flush.get_future().share();
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:num_memtables", [&](void*) {
        if (table_writes.fetch_add(1) == 0) {
          first_flush_writing.set_value();
          release_first_flush_future.wait();
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:Finish", [&](void*) {
        if (!released.exchange(true)) {
          release_first_flush.set_value();
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* arg) {
        if (released.load() && !failed.exchange(true)) {
          IOStatus injected = IOStatus::IOError("injected");
          injected.SetRetryable(true);
          *static_cast<Status*>(arg) = injected;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  FlushOptions no_wait;
  no_wait.wait = false;
  no_wait.allow_write_stall = true;
  ASSERT_OK(Put("key1", value));
  ASSERT_OK(db_->Flush(no_wait));
  first_flush_writing.get_future().wait();
  ASSERT_OK(Put("key2", value));
  ASSERT_OK(db_->Flush(no_wait));

  ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(failed.load());

  // Retrying the flush must still find the blob files of both memtables.
  ASSERT_OK(dbfull()->Resume());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_GetBGError());

  Reopen(options);
  ASSERT_EQ(value, Get("key1"));
  ASSERT_EQ(value, Get("key2"));
}

class BlobFileCreationStatusListener : public EventListener {
 public:
  void OnBlobFileCreated(const BlobFileCreationInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    statuses_.push_back(info.status.ToString());
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/, Status bg_error,
                            bool* /*auto_recovery*/) override {
    bg_error.PermitUncheckedError();
  }

  std::vector<std::string> GetStatuses() {
    std::lock_guard<std::mutex> lock(mutex_);
    return statuses_;
  }

 private:
  std::mutex mutex_;
  std::vector<std::string> statuses_;
};

TEST_F(DBBlobDirectWriteTest, SpaceLimitWhileSealingKeepsBlobFile) {
  Options options = GetDirectWriteOptions();
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  auto listener = std::make_shared<BlobFileCreationStatusListener>();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);
  const std::string value(1024, 'v');
  ASSERT_OK(Put("key", value));

  // The flush seals the blob file holding "key" and reaches the space limit.
  // It still commits the file, and then writes stop. As for an SST file,
  // Flush() returns the background error set after the commit.
  sst_file_manager->SetMaxAllowedSpaceUsage(1);
  ASSERT_EQ(Status::kSpaceLimit, Flush().subcode());
  ASSERT_EQ(Status::kSpaceLimit, dbfull()->TEST_GetBGError().subcode());
  ASSERT_NOK(Put("key2", "x"));

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(1U, cf_meta.blob_files.size());
  ASSERT_EQ(value, Get("key"));
  ASSERT_EQ(std::vector<std::string>{"OK"}, listener->GetStatuses());
}

TEST_F(DBBlobDirectWriteTest,
       SpaceLimitWhileSealingDoesNotBlockMemtableSwitch) {
  Options options = GetDirectWriteOptions();
  options.max_write_buffer_number = 4;
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  DestroyAndReopen(options);
  const std::string value(1024, 'v');
  ASSERT_OK(Put("key1", value));

  // While the flush reports the space limit for the blob file holding "key1",
  // a concurrent memtable switch holds the DB mutex and rotates the blob
  // generation.
  port::Thread switch_thread;
  std::atomic<bool> switch_started{false};
  std::promise<void> switch_holds_db_mutex;
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBBlobDirectWriteTest::SpaceLimitWhileSealing:SecondPut",
        "DBImpl::BackgroundCallFlush:start"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SwitchMemtable:BeforeRotateBlobGeneration", [&](void*) {
        if (switch_started.exchange(false)) {
          switch_holds_db_mutex.set_value();
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileCompletionCallback::CallBack::MaxAllowedSpaceReached",
      [&](void*) {
        if (switch_thread.joinable()) {
          return;
        }
        switch_started = true;
        switch_thread = port::Thread([&]() {
          FlushOptions no_wait;
          no_wait.wait = false;
          no_wait.allow_write_stall = true;
          db_->Flush(no_wait).PermitUncheckedError();
        });
        switch_holds_db_mutex.get_future().wait();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  sst_file_manager->SetMaxAllowedSpaceUsage(1);
  FlushOptions no_wait;
  no_wait.wait = false;
  ASSERT_OK(db_->Flush(no_wait));
  ASSERT_OK(Put("key2", value));
  TEST_SYNC_POINT("DBBlobDirectWriteTest::SpaceLimitWhileSealing:SecondPut");

  dbfull()->TEST_WaitForBackgroundWork().PermitUncheckedError();
  ASSERT_TRUE(switch_thread.joinable());
  switch_thread.join();
  dbfull()->TEST_WaitForBackgroundWork().PermitUncheckedError();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(value, Get("key1"));
  ASSERT_EQ(value, Get("key2"));
}

class IgnoreFlushNoWALErrorListener : public EventListener {
 public:
  void OnBackgroundError(BackgroundErrorReason reason,
                         Status* bg_error) override {
    std::lock_guard<std::mutex> lock(mutex_);
    reasons_.push_back(reason);
    if (reason == BackgroundErrorReason::kFlushNoWAL) {
      *bg_error = Status::OK();
    }
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/, Status bg_error,
                            bool* /*auto_recovery*/) override {
    bg_error.PermitUncheckedError();
  }

  std::vector<BackgroundErrorReason> GetReasons() {
    std::lock_guard<std::mutex> lock(mutex_);
    return reasons_;
  }

 private:
  std::mutex mutex_;
  std::vector<BackgroundErrorReason> reasons_;
};

TEST_F(DBBlobDirectWriteTest, SpaceLimitWhileSealingStopsWrites) {
  Options options = GetDirectWriteOptions();
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  auto listener = std::make_shared<IgnoreFlushNoWALErrorListener>();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);
  const std::string value(1024, 'v');
  ASSERT_OK(Put("key1", value));

  // The space limit is reported for the blob file the flush seals, with the
  // reason kFlush, so a listener that ignores kFlushNoWAL errors still stops
  // writes.
  sst_file_manager->SetMaxAllowedSpaceUsage(1);
  FlushOptions no_wait;
  no_wait.wait = false;
  ASSERT_OK(db_->Flush(no_wait));
  dbfull()->TEST_WaitForBackgroundWork().PermitUncheckedError();
  const std::vector<BackgroundErrorReason> reasons = listener->GetReasons();
  ASSERT_GT(
      std::count(reasons.begin(), reasons.end(), BackgroundErrorReason::kFlush),
      0);
  ASSERT_NOK(Put("key2", value));
}

TEST_F(DBBlobDirectWriteTest, SpaceLimitWhileWriteSealsBlobFile) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 2048;
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(env_));
  options.sst_file_manager = sst_file_manager;
  auto listener = std::make_shared<BlobFileCreationStatusListener>();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);
  const std::string value(1024, 'v');
  ASSERT_OK(Put("key1", value));

  // "key2" does not fit in the blob file holding "key1", so the write seals
  // that file and reaches the space limit. The write still succeeds, and then
  // writes stop.
  sst_file_manager->SetMaxAllowedSpaceUsage(1);
  ASSERT_OK(Put("key2", value));
  ASSERT_EQ(Status::kSpaceLimit, dbfull()->TEST_GetBGError().subcode());
  ASSERT_NOK(Put("key3", "x"));

  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_EQ(2U, CountBlobFiles());
  ASSERT_EQ(value, Get("key1"));
  ASSERT_EQ(value, Get("key2"));
  ASSERT_EQ(std::vector<std::string>{"OK"}, listener->GetStatuses());
}

class FailNthBlobFileOpenFileSystem : public FileSystemWrapper {
 public:
  FailNthBlobFileOpenFileSystem(const std::shared_ptr<FileSystem>& fs,
                                int failing_open)
      : FileSystemWrapper(fs), failing_open_(failing_open) {}

  static const char* kClassName() { return "FailNthBlobFileOpenFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    if (IsBlobFilePath(fname) && ++blob_file_opens_ == failing_open_) {
      return IOStatus::IOError("Injected blob file open error");
    }
    return target()->NewWritableFile(fname, file_opts, result, dbg);
  }

 private:
  const int failing_open_;
  std::atomic<int> blob_file_opens_{0};
};

TEST_F(DBBlobDirectWriteTest, FailedBatchAfterSealCountsGarbageInSealedFile) {
  auto fs = std::make_shared<FailNthBlobFileOpenFileSystem>(
      env_->GetFileSystem(), /*failing_open=*/3);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options = GetDirectWriteOptions();
  options.env = env.get();
  options.blob_file_size = 2048;
  Defer close_db([this]() { Close(); });
  DestroyAndReopen(options);
  const std::string value(1024, 'v');

  // An SST refers to the first blob file, so a later blob file that no SST
  // refers to is dropped only once all its blobs are garbage.
  ASSERT_OK(Put("x", value));
  ASSERT_OK(Flush());

  // "b" does not fit in the blob file holding "a" from the same batch, so the
  // batch seals that file and then fails to open the next one. "a" is garbage
  // in the sealed file, so the flush drops that file.
  WriteBatch batch;
  ASSERT_OK(batch.Put("a", value));
  ASSERT_OK(batch.Put("b", value));
  ASSERT_TRUE(db_->Write(WriteOptions(), &batch).IsIOError());

  ASSERT_OK(Put("c", value));
  ASSERT_OK(Flush());
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(2U, cf_meta.blob_files.size());
  ASSERT_EQ("NOT_FOUND", Get("a"));
  ASSERT_EQ(value, Get("x"));
  ASSERT_EQ(value, Get("c"));
}

class FailBlobFileSizeFileSystem : public FileSystemWrapper {
 public:
  explicit FailBlobFileSizeFileSystem(const std::shared_ptr<FileSystem>& fs)
      : FileSystemWrapper(fs) {}

  static const char* kClassName() { return "FailBlobFileSizeFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    if (IsBlobFilePath(fname)) {
      return IOStatus::IOError("Injected blob file size error");
    }
    return target()->GetFileSize(fname, options, file_size, dbg);
  }
};

TEST_F(DBBlobDirectWriteTest, SealedBlobFilesTrackedWhenSizeLookupFails) {
  Options options = GetDirectWriteOptions();
  options.blob_file_size = 2048;
  // Only the SstFileManager fails to look up blob file sizes.
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(
      env_,
      std::make_shared<FailBlobFileSizeFileSystem>(env_->GetFileSystem())));
  options.sst_file_manager = sst_file_manager;
  DestroyAndReopen(options);
  const std::string value(1024, 'v');

  // "key2" does not fit in the blob file holding "key1", so the write seals
  // that file, and the flush seals the one holding "key2".
  ASSERT_OK(Put("key1", value));
  ASSERT_OK(Put("key2", value));
  ASSERT_OK(Flush());
  ASSERT_EQ(2U, CountBlobFiles());

  std::unordered_map<std::string, uint64_t> blob_file_sizes;
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    const std::string path = dbname_ + "/" + file;
    if (IsBlobFilePath(path)) {
      ASSERT_OK(env_->GetFileSize(path, &blob_file_sizes[path]));
    }
  }
  std::unordered_map<std::string, uint64_t> tracked_blob_file_sizes;
  for (const auto& [path, size] : sst_file_manager->GetTrackedFiles()) {
    if (IsBlobFilePath(path)) {
      tracked_blob_file_sizes[path] = size;
    }
  }
  ASSERT_EQ(blob_file_sizes, tracked_blob_file_sizes);

  Reopen(options);
  ASSERT_EQ(value, Get("key1"));
  ASSERT_EQ(value, Get("key2"));
}

class DisableAutoRecoveryListener : public EventListener {
 public:
  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/, Status bg_error,
                            bool* auto_recovery) override {
    bg_error.PermitUncheckedError();
    *auto_recovery = false;
  }
};

TEST_F(DBBlobDirectWriteTest, FailedBlobSealLeavesMemtableFlushable) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetDirectWriteOptions();
  options.env = fault_env.get();
  options.listeners.emplace_back(
      std::make_shared<DisableAutoRecoveryListener>());
  for (bool atomic_flush : {false, true}) {
    SCOPED_TRACE("atomic_flush=" + std::to_string(atomic_flush));
    options.atomic_flush = atomic_flush;
    DestroyAndReopen(options);
    Defer close_db([this]() { Close(); });
    const std::string value(1024, 'v');
    ASSERT_OK(Put("key", value));
    // Sync the blob file now. An atomic flush first syncs the closed WALs and,
    // before them, the blob files. With nothing left to sync, the first blob
    // sync of the flush is the footer sync of the seal.
    ASSERT_OK(db_->SyncWAL());

    // The flush fails to sync the footer of the blob file holding "key".
    SyncPoint::GetInstance()->SetCallBack("BlobLogWriter::Sync", [&](void*) {
      fault_env->SetFilesystemActive(false, Status::NoSpace("injected"));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_NOK(Flush());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    fault_env->SetFilesystemActive(true);

    uint64_t flush_pending = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kMemTableFlushPending,
                                    &flush_pending));
    ASSERT_EQ(1U, flush_pending);
  }
}

// Writes "first", then "second". With a tiny blob_file_size, "second" rolls
// the file of "first" over, and the seal of that file fails. If
// `sync_before_seal`, SyncWAL() runs before "second", so the failed file has
// no unsynced records. Sets `*seal_status` to the error of the failed Put()
// and returns the status of the SyncWAL() that follows.
Status SyncWALAfterFailedRolloverSeal(DBTestBase* test, bool sync_before_seal,
                                      Status* seal_status) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(test->env_));
  Options options =
      wide_column_test_util::GetDirectWriteOptions(test->CurrentOptions());
  options.env = fault_env.get();
  options.blob_file_size = 1;
  options.listeners.emplace_back(
      std::make_shared<DisableAutoRecoveryListener>());
  test->DestroyAndReopen(options);
  Defer close_db([test]() { test->Close(); });
  const std::string value(1024, 'v');
  EXPECT_OK(test->Put("first", value));
  if (sync_before_seal) {
    EXPECT_OK(test->db_->SyncWAL());
  }

  // The seal of the full file fails to sync its footer.
  SyncPoint::GetInstance()->SetCallBack("BlobLogWriter::Sync", [&](void*) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("injected"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  *seal_status = test->Put("second", value);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_env->SetFilesystemActive(true);
  return test->db_->SyncWAL();
}

// A file whose seal failed cannot be synced again. While it has unsynced
// records, SyncWAL() must fail with the seal error, so that no WAL record
// that points to those records becomes durable.
TEST_F(DBBlobDirectWriteTest, SyncWALReturnsErrorOfFailedRolloverSeal) {
  Status seal_status;
  Status s = SyncWALAfterFailedRolloverSeal(this, /*sync_before_seal=*/false,
                                            &seal_status);
  ASSERT_TRUE(seal_status.IsNoSpace()) << seal_status.ToString();
  ASSERT_EQ(s.code(), seal_status.code()) << s.ToString();
  ASSERT_TRUE(s.IsNoSpace()) << s.ToString();
}

// An earlier SyncWAL() already synced every record of the file whose seal
// failed, so the file needs no sync, and the next SyncWAL() succeeds.
TEST_F(DBBlobDirectWriteTest, SyncWALSkipsFailedSealOfSyncedFile) {
  Status seal_status;
  Status s = SyncWALAfterFailedRolloverSeal(this, /*sync_before_seal=*/true,
                                            &seal_status);
  ASSERT_TRUE(seal_status.IsNoSpace()) << seal_status.ToString();
  ASSERT_OK(s);
}

// A flush of "default" syncs the closed WAL and, before it, the blob files of
// "other", without the DB mutex. "other" is dropped and its handle destroyed
// during that blob sync. Its manager and the BlobFileCache that the manager
// uses must stay alive until the sync ends; ASAN reports a
// heap-use-after-free otherwise.
TEST_F(DBBlobDirectWriteTest, DropColumnFamilyDuringBlobSyncBeforeWalSync) {
  Options options = GetDirectWriteOptions();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other"}, options);
  ASSERT_OK(Put(1, "large", std::string(1024, 'v')));
  ASSERT_OK(Put(0, "small", "s"));

  std::atomic<bool> dropped{false};
  SyncPoint::GetInstance()->SetCallBack("BlobLogWriter::Sync", [&](void*) {
    if (dropped.exchange(true)) {
      return;
    }
    EXPECT_OK(db_->DropColumnFamily(handles_[1]));
    EXPECT_OK(db_->DestroyColumnFamilyHandle(handles_[1]));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Flush(0));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  handles_.resize(1);
  ASSERT_TRUE(dropped);
  ASSERT_EQ(Get("small"), "s");
}

// With the WAL off, no WAL file is ever closed, so the closed-WAL sync before
// a flush in a DB with 2 column families syncs no WAL. It must then sync no
// blob file either: "other" keeps its unsynced blob file, and the flush of
// "default", which has no blob file to seal, syncs no blob file.
TEST_F(DBBlobDirectWriteTest, ClosedWalSyncWithWalOffSkipsBlobSync) {
  Options options = GetDirectWriteOptions();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other"}, options);
  WriteOptions no_wal;
  no_wal.disableWAL = true;
  ASSERT_OK(db_->Put(no_wal, handles_[1], "large", std::string(1024, 'v')));
  ASSERT_OK(db_->Put(no_wal, handles_[0], "small", "s"));
  ASSERT_EQ(CountBlobFileSyncs([&]() { ASSERT_OK(Flush(0)); }), 0);
}

// An atomic flush runs the closed-WAL sync with any number of column
// families. With the WAL off it syncs no WAL, so the only blob sync of the
// flush is the seal of the one blob file.
TEST_F(DBBlobDirectWriteTest, AtomicFlushWithWalOffSkipsBlobSync) {
  Options options = GetDirectWriteOptions();
  options.atomic_flush = true;
  DestroyAndReopen(options);
  WriteOptions no_wal;
  no_wal.disableWAL = true;
  ASSERT_OK(db_->Put(no_wal, "large", std::string(1024, 'v')));
  ASSERT_EQ(CountBlobFileSyncs([&]() { ASSERT_OK(Flush()); }), 1);
}

// The WAL is on. The flush of "default" switches the WAL and syncs the closed
// WAL. A large value then goes to "other" without a WAL record, so the current
// WAL stays empty, and the flush of "other" switches no WAL. Its closed-WAL
// sync finds no WAL left to sync, so the only blob sync of that flush is the
// seal of the blob file of "other".
TEST_F(DBBlobDirectWriteTest, ClosedWalSyncOfSyncedWalsSkipsBlobSync) {
  Options options = GetDirectWriteOptions();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other"}, options);
  ASSERT_OK(Put(0, "a", "s"));
  ASSERT_OK(Put(1, "b", "s"));
  ASSERT_OK(Flush(0));
  WriteOptions no_wal;
  no_wal.disableWAL = true;
  ASSERT_OK(db_->Put(no_wal, handles_[1], "large", std::string(1024, 'v')));
  ASSERT_EQ(CountBlobFileSyncs([&]() { ASSERT_OK(Flush(1)); }), 1);
}

TEST_F(DBBlobDirectWriteTest, FailedBlobFooterSyncFailsRetriedFlush) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetDirectWriteOptions();
  options.env = fault_env.get();
  options.listeners.emplace_back(
      std::make_shared<DisableAutoRecoveryListener>());
  DestroyAndReopen(options);
  Defer close_db([this]() { Close(); });
  const std::string value(1024, 'v');
  ASSERT_OK(Put("key", value));

  // The flush fails to sync the footer of the blob file holding "key".
  SyncPoint::GetInstance()->SetCallBack("BlobLogWriter::Sync", [&](void*) {
    fault_env->SetFilesystemActive(false, Status::NoSpace("injected"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NOK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_env->SetFilesystemActive(true);

  // That blob file can no longer be sealed, so retrying the flush must fail
  // rather than install a table file that references it.
  ASSERT_NOK(dbfull()->Resume());
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(value, Get("key"));
}

TEST_F(DBBlobDirectWriteTest, CloseAfterFailedBlobSeal) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetDirectWriteOptions();
  options.env = fault_env.get();
  options.blob_file_size = 2048;
  options.listeners.emplace_back(
      std::make_shared<DisableAutoRecoveryListener>());
  for (bool seal_on_write : {false, true}) {
    SCOPED_TRACE("seal_on_write=" + std::to_string(seal_on_write));
    DestroyAndReopen(options);
    Defer close_db([this]() { Close(); });
    const std::string value(1024, 'v');
    ASSERT_OK(Put("key1", value));

    // Sealing the blob file holding "key1" fails, either in the write that
    // does not fit in it or in the flush. The DB closes without retrying.
    SyncPoint::GetInstance()->SetCallBack("BlobLogWriter::Sync", [&](void*) {
      fault_env->SetFilesystemActive(false, Status::NoSpace("injected"));
    });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_NOK(seal_on_write ? Put("key2", value) : Flush());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    fault_env->SetFilesystemActive(true);
  }
}

// A blob sync fails, either in a write with sync=true or in SyncWAL(). The
// blob file of that sync can then never be sealed, so no flush of the
// memtable that refers to it can succeed. With paranoid_checks=false, the
// flush error alone does not stop the DB, so a caller that waits for the flush
// would wait forever: a manual Flush(), or the flush that Close() always runs
// with blob direct write. The flush error must stop the DB, so that Flush()
// and Close() return. With paranoid_checks=true, the failed blob sync already
// stopped the DB, and the flush does not change that error. The single column
// family flush and the atomic flush have separate code for this, so the test
// runs both.
TEST_F(DBBlobDirectWriteTest, FailedBlobSyncDoesNotHangFlushOrClose) {
  // One env for all cases: DestroyAndReopen() destroys the DB of the previous
  // case with the options, and so the env, of that case.
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  for (bool atomic_flush : {false, true}) {
    for (bool paranoid_checks : {false, true}) {
      for (bool use_sync_wal : {false, true}) {
        for (bool flush_before_close : {false, true}) {
          SCOPED_TRACE(
              "atomic_flush=" + std::to_string(atomic_flush) +
              " paranoid_checks=" + std::to_string(paranoid_checks) +
              " use_sync_wal=" + std::to_string(use_sync_wal) +
              " flush_before_close=" + std::to_string(flush_before_close));
          Options options = GetDirectWriteOptions();
          options.env = fault_env.get();
          options.atomic_flush = atomic_flush;
          options.paranoid_checks = paranoid_checks;
          options.listeners.emplace_back(
              std::make_shared<DisableAutoRecoveryListener>());
          DestroyAndReopen(options);
          Defer close_db([this]() { Close(); });
          const std::string value(1024, 'v');
          ASSERT_OK(Put("key1", value));

          // The first blob sync fails. The blob file holding "key1" keeps the
          // error.
          std::atomic<bool> failed{false};
          SyncPoint::GetInstance()->SetCallBack(
              "BlobLogWriter::Sync", [&](void* /*arg*/) {
                if (!failed.exchange(true)) {
                  fault_env->SetFilesystemActive(false,
                                                 Status::NoSpace("injected"));
                }
              });
          SyncPoint::GetInstance()->EnableProcessing();
          if (use_sync_wal) {
            ASSERT_NOK(db_->SyncWAL());
          } else {
            WriteOptions sync_write;
            sync_write.sync = true;
            ASSERT_NOK(db_->Put(sync_write, "key2", value));
          }
          SyncPoint::GetInstance()->DisableProcessing();
          SyncPoint::GetInstance()->ClearAllCallBacks();
          fault_env->SetFilesystemActive(true);
          const Status error_before_flush = dbfull()->TEST_GetBGError();
          ASSERT_EQ(paranoid_checks, !error_before_flush.ok())
              << error_before_flush.ToString();

          if (flush_before_close) {
            ASSERT_NOK(Flush());
            const Status bg_error = dbfull()->TEST_GetBGError();
            if (paranoid_checks) {
              ASSERT_EQ(error_before_flush.ToString(), bg_error.ToString());
              ASSERT_EQ(error_before_flush.severity(), bg_error.severity());
            } else {
              ASSERT_EQ(Status::Severity::kFatalError, bg_error.severity())
                  << bg_error.ToString();
            }
            ASSERT_NOK(Put("key3", value));
          }
          Close();
        }
      }
    }
  }
}

// Lets one thread wait, without polling, until another thread reports that
// an event happened.
class TestEvent {
 public:
  TestEvent() : cv_(&mu_) {}

  void Set() {
    mu_.Lock();
    set_ = true;
    cv_.SignalAll();
    mu_.Unlock();
  }

  // Returns false if the event is not set within kTimeoutMicros, so that a
  // broken test fails instead of hanging. The 5 minutes are far more than the
  // tests need, and less than a typical test runner timeout.
  bool Wait() {
    constexpr uint64_t kTimeoutMicros = 5ULL * 60 * 1000 * 1000;
    const uint64_t deadline =
        SystemClock::Default()->NowMicros() + kTimeoutMicros;
    mu_.Lock();
    while (!set_ && !cv_.TimedWait(deadline)) {
    }
    const bool set = set_;
    mu_.Unlock();
    return set;
  }

 private:
  port::Mutex mu_;
  port::CondVar cv_;
  bool set_ = false;
};

// Runs `fn` on a helper thread and waits for it. If `fn` does not return
// within TestEvent's timeout (kTimeoutMicros, 5 minutes), the test cannot
// recover (the hung thread holds DB locks, so closing the DB would hang too),
// so this reports the failure and aborts the process. A regression then fails
// the test instead of hanging it.
void RunOrAbortOnHang(const std::function<void()>& fn) {
  TestEvent done;
  port::Thread thread([&]() {
    fn();
    done.Set();
  });
  if (!done.Wait()) {
    fprintf(stderr, "The DB call did not return; aborting the test\n");
    std::abort();
  }
  thread.join();
}

// Fails the next sync of a blob file once fail_next_blob_sync_ is set, with an
// IOError of file scope.
class FailBlobSyncFileSystem : public FileSystemWrapper {
 public:
  explicit FailBlobSyncFileSystem(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  const char* Name() const override { return "FailBlobSyncFileSystem"; }

  IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg) override {
    IOStatus s = target()->NewWritableFile(fname, opts, result, dbg);
    uint64_t number = 0;
    FileType type = kWalFile;
    const size_t separator = fname.rfind(kFilePathSeparator);
    if (s.ok() && ParseFileName(fname.substr(separator + 1), &number, &type) &&
        type == kBlobFile) {
      result->reset(new FailingBlobFile(std::move(*result), this));
    }
    return s;
  }

  std::atomic<bool> fail_next_blob_sync_{false};

 private:
  class FailingBlobFile : public FSWritableFileOwnerWrapper {
   public:
    FailingBlobFile(std::unique_ptr<FSWritableFile>&& target,
                    FailBlobSyncFileSystem* fs)
        : FSWritableFileOwnerWrapper(std::move(target)), fs_(fs) {}

    IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
      if (fs_->fail_next_blob_sync_.exchange(false)) {
        IOStatus error = IOStatus::IOError("injected blob sync error");
        error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
        return error;
      }
      return FSWritableFileOwnerWrapper::Sync(options, dbg);
    }
    IOStatus Fsync(const IOOptions& options, IODebugContext* dbg) override {
      return Sync(options, dbg);
    }

   private:
    FailBlobSyncFileSystem* const fs_;
  };
};

// A blob sync failure in SyncWAL() is a blob error. Even with a file-scope
// IOError and paranoid_checks=true, it must not quarantine the current WAL,
// which a WAL error of file scope would do. It still sets a background error.
TEST_F(DBBlobDirectWriteTest, BlobSyncFailureInSyncWALDoesNotQuarantineWal) {
  auto fs = std::make_shared<FailBlobSyncFileSystem>(env_->GetFileSystem());
  std::unique_ptr<Env> fail_env = NewCompositeEnv(fs);
  Options options = GetDirectWriteOptions();
  options.env = fail_env.get();
  options.paranoid_checks = true;
  options.listeners.emplace_back(
      std::make_shared<DisableAutoRecoveryListener>());
  DestroyAndReopen(options);
  // Close the DB and forget `fail_env` before it is destroyed, also when an
  // assertion fails, because the test cleanup uses the last options.
  Defer forget_fail_env([&]() {
    Close();
    last_options_ = GetDirectWriteOptions();
  });

  ASSERT_OK(Put("large", std::string(1024, 'v')));
  fs->fail_next_blob_sync_ = true;
  ASSERT_NOK(db_->SyncWAL());
  ASSERT_EQ(0U, dbfull()->TEST_GetWALRecoveryCutoff());
  ASSERT_NOK(dbfull()->TEST_GetBGError());
}

// A blob file fails to seal in a flush with a file-scoped IOError, which
// ErrorHandler treats as retryable and would recover from automatically. The
// blob file holds the only copy of its values, so a retry cannot fix it: the
// flush sets kFatalError, starts no automatic recovery, and DB::Resume()
// fails. With paranoid_checks on and off, Flush() returns the error instead of
// waiting.
TEST_F(DBBlobDirectWriteTest, FailedBlobSealInFlushIsFatal) {
  for (bool paranoid_checks : {true, false}) {
    SCOPED_TRACE("paranoid_checks=" + std::to_string(paranoid_checks));
    auto fs = std::make_shared<FailBlobSyncFileSystem>(env_->GetFileSystem());
    std::unique_ptr<Env> fail_env = NewCompositeEnv(fs);
    Options options = GetDirectWriteOptions();
    options.env = fail_env.get();
    options.paranoid_checks = paranoid_checks;
    // Automatic recovery is on, with a short wait between attempts.
    options.max_bgerror_resume_count = 10;
    options.bgerror_resume_retry_interval = 1000;  // 1 ms
    DestroyAndReopen(options);
    // Close the DB and forget `fail_env` before it is destroyed, also when an
    // assertion fails, because the test cleanup uses the last options.
    Defer forget_fail_env([&]() {
      Close();
      last_options_ = GetDirectWriteOptions();
    });

    const std::string value(1024, 'v');
    ASSERT_OK(Put("large", value));
    // The only blob sync in this flush is the footer sync of the seal.
    fs->fail_next_blob_sync_ = true;
    Status flush_status;
    RunOrAbortOnHang([&]() { flush_status = Flush(); });
    ASSERT_NOK(flush_status);
    const Status bg_error = dbfull()->TEST_GetBGError();
    ASSERT_EQ(Status::Severity::kFatalError, bg_error.severity())
        << bg_error.ToString();
    ASSERT_FALSE(dbfull()->TEST_IsRecoveryInProgress());
    ASSERT_NOK(dbfull()->Resume());
    ASSERT_NOK(Put("after", value));
    ASSERT_EQ(Get("large"), value);
  }
}

// Gives every directory the default FSDirectory::Close(), which returns
// NotSupported.
class NoDirCloseFileSystem : public FileSystemWrapper {
 public:
  explicit NoDirCloseFileSystem(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  const char* Name() const override { return "NoDirCloseFileSystem"; }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    IOStatus s = target()->NewDirectory(name, io_opts, result, dbg);
    if (s.ok()) {
      result->reset(new NoCloseDirectory(std::move(*result)));
    }
    return s;
  }

 private:
  class NoCloseDirectory : public FSDirectoryWrapper {
   public:
    explicit NoCloseDirectory(std::unique_ptr<FSDirectory>&& target)
        : FSDirectoryWrapper(std::move(target)) {}

    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::NotSupported("Close");
    }
  };
};

// FSDirectory::Close() is optional, and its default returns NotSupported. The
// blob directory sync must accept that, so sync writes and SyncWAL() work on
// such a file system.
TEST_F(DBBlobDirectWriteTest, BlobDirectorySyncAcceptsUnsupportedClose) {
  auto fs = std::make_shared<NoDirCloseFileSystem>(env_->GetFileSystem());
  std::unique_ptr<Env> no_close_env = NewCompositeEnv(fs);
  Options options = GetDirectWriteOptions();
  options.env = no_close_env.get();
  DestroyAndReopen(options);
  // Close the DB and forget `no_close_env` before it is destroyed, also when
  // an assertion fails, because the test cleanup uses the last options.
  Defer forget_env([&]() {
    Close();
    last_options_ = GetDirectWriteOptions();
  });
  WriteOptions sync_write;
  sync_write.sync = true;
  // Each write creates a blob file in a new partition, so each sync also
  // syncs the blob directory.
  ASSERT_OK(db_->Put(sync_write, "large1", std::string(1024, 'a')));
  ASSERT_OK(Put("large2", std::string(1024, 'b')));
  ASSERT_OK(db_->SyncWAL());
  ASSERT_EQ(Get("large1"), std::string(1024, 'a'));
}

// Fails the next directory fsync once fail_next_dir_fsync_ is set.
class FailDirFsyncFileSystem : public FileSystemWrapper {
 public:
  explicit FailDirFsyncFileSystem(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  const char* Name() const override { return "FailDirFsyncFileSystem"; }

  IOStatus NewDirectory(const std::string& name, const IOOptions& io_opts,
                        std::unique_ptr<FSDirectory>* result,
                        IODebugContext* dbg) override {
    IOStatus s = target()->NewDirectory(name, io_opts, result, dbg);
    if (s.ok()) {
      result->reset(new FailingDirectory(std::move(*result), this));
    }
    return s;
  }

  std::atomic<bool> fail_next_dir_fsync_{false};
  std::atomic<int> failed_dir_fsyncs_{0};

 private:
  class FailingDirectory : public FSDirectoryWrapper {
   public:
    FailingDirectory(std::unique_ptr<FSDirectory>&& target,
                     FailDirFsyncFileSystem* fs)
        : FSDirectoryWrapper(std::move(target)), fs_(fs) {}

    IOStatus FsyncWithDirOptions(const IOOptions& options, IODebugContext* dbg,
                                 const DirFsyncOptions& dir_options) override {
      if (fs_->fail_next_dir_fsync_.exchange(false)) {
        ++fs_->failed_dir_fsyncs_;
        return IOStatus::IOError("injected directory fsync error");
      }
      return FSDirectoryWrapper::FsyncWithDirOptions(options, dbg, dir_options);
    }

   private:
    FailDirFsyncFileSystem* const fs_;
  };
};

// A failed fsync of the blob directory does not leave a blob file writer in a
// failed state. But with paranoid_checks=false, the failed flush is not tried
// again by itself, so a Flush() that waits for it would wait forever. The flush
// whose closed-WAL sync hits the error must stop the DB instead, as for a
// failed file sync.
TEST_F(DBBlobDirectWriteTest, FailedBlobDirectorySyncStopsDB) {
  auto fs = std::make_shared<FailDirFsyncFileSystem>(env_->GetFileSystem());
  std::unique_ptr<Env> fail_env = NewCompositeEnv(fs);
  Options options = GetDirectWriteOptions();
  options.env = fail_env.get();
  options.paranoid_checks = false;
  DestroyAndReopen(options);
  // Close the DB and forget `fail_env` before it is destroyed, also when an
  // assertion fails, because the test cleanup uses the last options.
  Defer forget_fail_env([&]() {
    Close();
    last_options_ = GetDirectWriteOptions();
  });
  CreateAndReopenWithCF({"other"}, options);

  // "other" creates a blob file, so its blob directory needs a sync. The
  // flush of "default" syncs the closed WAL and, before it, the blob files and
  // the blob directory of "other".
  const std::string value(1024, 'v');
  ASSERT_OK(Put(1, "large", value));
  ASSERT_OK(Put(0, "small", "s"));
  fs->fail_next_dir_fsync_ = true;
  Status flush_status;
  RunOrAbortOnHang([&]() { flush_status = Flush(0); });
  ASSERT_EQ(1, fs->failed_dir_fsyncs_.load());
  ASSERT_NOK(flush_status);
  ASSERT_EQ(Status::Severity::kFatalError,
            dbfull()->TEST_GetBGError().severity());
  ASSERT_NOK(dbfull()->Resume());
  ASSERT_EQ(Get(1, "large"), value);
}

// Runs the DB on FaultInjectionTestFS. SimulatePowerLoss() closes the DB,
// drops all data that was not synced, and deletes files whose directory entry
// was not synced. A value that survives was durable before the power loss.
class DBBlobDirectWritePowerLossTest : public DBBlobDirectWriteTest {
 public:
  ~DBBlobDirectWritePowerLossTest() override { Close(); }

 protected:
  DBBlobDirectWritePowerLossTest()
      : fault_fs_(
            std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem())),
        fault_env_(NewCompositeEnv(fault_fs_)) {
    fault_fs_->SetInjectUnsyncedDataLoss(true);
  }

  Options GetPowerLossOptions() {
    Options options = GetDirectWriteOptions();
    options.env = fault_env_.get();
    return options;
  }

  void SimulatePowerLoss() {
    fault_fs_->SetFilesystemActive(false);
    Close();
    fault_fs_->SetFilesystemActive(true);
    ASSERT_OK(fault_fs_->DropUnsyncedFileData());
    ASSERT_OK(
        fault_fs_->DeleteFilesCreatedAfterLastDirSync(IOOptions(), nullptr));
  }

  void AssertDefaultColumnFamilyScanOk() {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    }
    ASSERT_OK(iter->status());
  }

  // After a power loss, WAL replay inserts BlobIndex entries that point into
  // blob files that are not in the MANIFEST. Close() checks that no obsolete
  // file is still in the table cache, and it would report these files, so
  // disable file deletions first.
  void CloseKeepingRecoveredBlobFiles() {
    ASSERT_OK(db_->DisableFileDeletions());
    Close();
  }

  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> fault_env_;
};

// SyncWAL() does not keep writers out. In this test a write ("overlap") runs
// between the blob sync and the WAL fsync of SyncWAL(): its blob value is
// appended after the blob sync, and its WAL record is appended before the WAL
// fsync. The write does not wait for SyncWAL(), and after a power loss the
// write made before SyncWAL() started survives. The blob value of "overlap"
// can be lost, because the sync does not cover it.
TEST_F(DBBlobDirectWritePowerLossTest, WriteDuringSyncWALIsNotCovered) {
  Options options = GetPowerLossOptions();
  DestroyAndReopen(options);
  const std::string value(200, 'v');
  ASSERT_OK(Put("before", value));

  bool overlap_written = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWalImpl:BeforeSyncWAL", [&](void* /*arg*/) {
        if (overlap_written) {
          return;
        }
        overlap_written = true;
        // The write does not wait for SyncWAL(), so join it here, before the
        // WAL fsync.
        port::Thread writer([&]() { EXPECT_OK(Put("overlap", value)); });
        writer.join();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer clear_sync_points([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  Status sync_status;
  RunOrAbortOnHang([&]() { sync_status = db_->SyncWAL(); });
  ASSERT_OK(sync_status);
  ASSERT_TRUE(overlap_written);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  SimulatePowerLoss();
  Reopen(options);
  ASSERT_EQ(Get("before"), value);
  CloseKeepingRecoveredBlobFiles();
}

enum class WalSyncTrigger {
  kSyncWAL,
  kFlushWALWithSync,
  kSyncWriteWithoutBlob,
  kSyncWriteToOtherColumnFamily,
  kFlushOtherColumnFamily,
};

class DBBlobDirectWriteWalSyncTest
    : public DBBlobDirectWritePowerLossTest,
      public testing::WithParamInterface<WalSyncTrigger> {};

TEST_P(DBBlobDirectWriteWalSyncTest, PersistsEarlierDirectWrite) {
  const WalSyncTrigger trigger = GetParam();
  Options options = GetPowerLossOptions();
  options.manual_wal_flush = trigger == WalSyncTrigger::kFlushWALWithSync;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other"}, options);

  const std::string value(200, 'v');
  ASSERT_OK(Put(0, "key", value));
  WriteOptions sync_write;
  sync_write.sync = true;
  switch (trigger) {
    case WalSyncTrigger::kSyncWAL:
      ASSERT_OK(db_->SyncWAL());
      break;
    case WalSyncTrigger::kFlushWALWithSync:
      ASSERT_OK(db_->FlushWAL(/*sync=*/true));
      break;
    case WalSyncTrigger::kSyncWriteWithoutBlob:
      ASSERT_OK(db_->Put(sync_write, handles_[0], "small", "s"));
      break;
    case WalSyncTrigger::kSyncWriteToOtherColumnFamily:
      ASSERT_OK(db_->Put(sync_write, handles_[1], "other", value));
      break;
    case WalSyncTrigger::kFlushOtherColumnFamily:
      ASSERT_OK(Put(1, "other", value));
      ASSERT_OK(Flush(1));
      break;
  }
  SimulatePowerLoss();

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other"}, options);
  ASSERT_EQ(Get(0, "key"), value);
  AssertDefaultColumnFamilyScanOk();
  CloseKeepingRecoveredBlobFiles();
}

INSTANTIATE_TEST_CASE_P(
    DBBlobDirectWriteWalSyncTest, DBBlobDirectWriteWalSyncTest,
    ::testing::Values(WalSyncTrigger::kSyncWAL,
                      WalSyncTrigger::kFlushWALWithSync,
                      WalSyncTrigger::kSyncWriteWithoutBlob,
                      WalSyncTrigger::kSyncWriteToOtherColumnFamily,
                      WalSyncTrigger::kFlushOtherColumnFamily));

// A sync write whose blob sync fails must write no WAL record. Otherwise,
// with paranoid_checks=false, the next write reuses the sequence numbers of
// the failed write in the same WAL, a later WAL sync makes both records
// durable, and the next open fails on the sequence numbers going back. Here a
// blob directory fsync fails once for a sync batch with two large values, and
// the next sync write succeeds.
TEST_F(DBBlobDirectWritePowerLossTest, FailedBlobSyncWritesNoWalRecord) {
  auto fail_fs = std::make_shared<FailDirFsyncFileSystem>(fault_fs_);
  std::unique_ptr<Env> fail_env = NewCompositeEnv(fail_fs);
  Options options = GetPowerLossOptions();
  options.env = fail_env.get();
  options.paranoid_checks = false;
  DestroyAndReopen(options);
  // Close the DB and forget `fail_env` before it is destroyed, also when an
  // assertion fails, because the test cleanup uses the last options.
  Defer forget_fail_env([&]() {
    Close();
    last_options_ = GetPowerLossOptions();
  });

  WriteOptions sync_write;
  sync_write.sync = true;
  // The batch creates the first blob file, so the blob sync also fsyncs the
  // blob directory, and that fsync fails.
  WriteBatch failed_batch;
  ASSERT_OK(failed_batch.Put("large1", std::string(1024, 'a')));
  ASSERT_OK(failed_batch.Put("large2", std::string(1024, 'b')));
  fail_fs->fail_next_dir_fsync_ = true;
  ASSERT_NOK(db_->Write(sync_write, &failed_batch));
  ASSERT_EQ(1, fail_fs->failed_dir_fsyncs_.load());
  ASSERT_OK(db_->Put(sync_write, "small", "s"));

  SimulatePowerLoss();
  options.env = fault_env_.get();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("small"), "s");
  ASSERT_EQ(Get("large1"), "NOT_FOUND");
  ASSERT_EQ(Get("large2"), "NOT_FOUND");
}

TEST_F(DBBlobDirectWritePowerLossTest,
       SyncWritePersistsBlobFileRotatedAtMemtableSwitch) {
  Options options = GetPowerLossOptions();
  options.max_write_buffer_number = 4;
  DestroyAndReopen(options);

  test::SleepingBackgroundTask blocked_flush;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &blocked_flush,
                 Env::Priority::HIGH);
  blocked_flush.WaitUntilSleeping();
  const std::string rotated_value(200, 'r');
  const std::string synced_value(200, 's');
  ASSERT_OK(Put("rotated", rotated_value));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  WriteOptions sync_write;
  sync_write.sync = true;
  ASSERT_OK(db_->Put(sync_write, "synced", synced_value));

  // The rotated file of the switched memtable is a deferred file. The blocked
  // flush did not seal it, so only the sync write can have synced it.
  // Deactivate the file system before the flush runs, so the flush cannot
  // seal and sync the file either.
  fault_fs_->SetFilesystemActive(false);
  blocked_flush.WakeUp();
  blocked_flush.WaitUntilDone();
  SimulatePowerLoss();

  Reopen(options);
  ASSERT_EQ(Get("rotated"), rotated_value);
  ASSERT_EQ(Get("synced"), synced_value);
  AssertDefaultColumnFamilyScanOk();
  CloseKeepingRecoveredBlobFiles();
}

TEST_F(DBBlobDirectWritePowerLossTest,
       SyncWritePersistsNewBlobFileWithSeparateWalDir) {
  Options options = GetPowerLossOptions();
  options.wal_dir = alternative_wal_dir_;
  DestroyAndReopen(options);

  const std::string value(200, 'v');
  WriteOptions sync_write;
  sync_write.sync = true;
  ASSERT_OK(db_->Put(sync_write, "key", value));
  SimulatePowerLoss();

  Reopen(options);
  ASSERT_EQ(Get("key"), value);
  AssertDefaultColumnFamilyScanOk();
  CloseKeepingRecoveredBlobFiles();
}

// Blob direct write with a non-default first path for the column family:
// cf_paths={alt} (GetParam() is true) or db_paths={alt} (false). Its blob
// files must be written, synced, read and purged in that directory.
// Checkpoints stay unsupported for such a DB.
class DBBlobDirectWritePathTest : public DBBlobDirectWritePowerLossTest,
                                  public testing::WithParamInterface<bool> {
 protected:
  // Only a power-loss test uses the fault injection file system. Other tests
  // read blob files that are still open for writing, which that file system
  // does not support for unsynced data.
  Options GetPathOptions(bool power_loss = false) {
    Options options =
        power_loss ? GetPowerLossOptions() : GetDirectWriteOptions();
    alt_dir_ = dbname_ + "_blob_path";
    // DestroyDB() does not clean this directory, so remove the files that an
    // earlier test left there.
    DestroyDir(env_, alt_dir_).PermitUncheckedError();
    if (GetParam()) {
      options.cf_paths = {{alt_dir_, std::numeric_limits<uint64_t>::max()}};
    } else {
      options.db_paths = {{alt_dir_, std::numeric_limits<uint64_t>::max()}};
    }
    return options;
  }

  std::set<uint64_t> BlobFilesIn(const std::string& dir) {
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dir, &files));
    std::set<uint64_t> result;
    for (const auto& file : files) {
      uint64_t number = 0;
      FileType type = kWalFile;
      if (ParseFileName(file, &number, &type) && type == kBlobFile) {
        result.insert(number);
      }
    }
    return result;
  }

  // Checks "key" and "entity" with Get(), MultiGet(), GetEntity() and an
  // iterator.
  void VerifyValues(DB* db, const std::string& value,
                    const WideColumns& columns) {
    std::string result;
    ASSERT_OK(db->Get(ReadOptions(), "key", &result));
    ASSERT_EQ(result, value);
    std::vector<std::string> keys{"key"};
    std::vector<Slice> key_slices(keys.begin(), keys.end());
    std::vector<std::string> values;
    std::vector<Status> statuses =
        db->MultiGet(ReadOptions(), key_slices, &values);
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], value);
    PinnableWideColumns entity;
    ASSERT_OK(db->GetEntity(ReadOptions(), db->DefaultColumnFamily(), "entity",
                            &entity));
    ASSERT_EQ(entity.columns(), columns);
    std::unique_ptr<Iterator> iter(db->NewIterator(ReadOptions()));
    iter->Seek("key");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->value(), value);
    ASSERT_OK(iter->status());
  }

  std::string alt_dir_;
};

TEST_P(DBBlobDirectWritePathTest, ReadsFromFirstPath) {
  Options options = GetPathOptions();
  DestroyAndReopen(options);
  const std::string value(200, 'v');
  const std::string column_value(200, 'c');
  const WideColumns columns{{kDefaultWideColumnName, column_value},
                            {"small", "s"}};
  ASSERT_OK(Put("key", value));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "entity",
                           columns));
  VerifyValues(db_.get(), value, columns);
  ASSERT_FALSE(BlobFilesIn(alt_dir_).empty());
  ASSERT_TRUE(BlobFilesIn(dbname_).empty());

  ASSERT_OK(Flush());
  VerifyValues(db_.get(), value, columns);
  Reopen(options);
  VerifyValues(db_.get(), value, columns);
  ASSERT_TRUE(BlobFilesIn(dbname_).empty());
}

// A sync write syncs the blob file and the directory entry in alt, so both
// survive a power loss.
TEST_P(DBBlobDirectWritePathTest, SyncWriteSurvivesPowerLoss) {
  Options options = GetPathOptions(/*power_loss=*/true);
  DestroyAndReopen(options);
  const std::string value(200, 'v');
  WriteOptions sync_write;
  sync_write.sync = true;
  ASSERT_OK(db_->Put(sync_write, "key", value));
  SimulatePowerLoss();

  Reopen(options);
  ASSERT_EQ(Get("key"), value);
  CloseKeepingRecoveredBlobFiles();
}

// The blob files of a dropped column family are obsolete. The purge must find
// and delete them in alt.
TEST_P(DBBlobDirectWritePathTest, ObsoleteBlobFilesArePurged) {
  Options options = GetPathOptions();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other"}, options);
  ASSERT_OK(Put(1, "key", std::string(200, 'v')));
  ASSERT_OK(Flush(1));
  const std::set<uint64_t> dropped_files = BlobFilesIn(alt_dir_);
  ASSERT_FALSE(dropped_files.empty());

  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[1]));
  handles_.resize(1);
  dbfull()->TEST_DeleteObsoleteFiles();
  for (uint64_t file_number : dropped_files) {
    ASSERT_EQ(BlobFilesIn(alt_dir_).count(file_number), 0U) << file_number;
  }
}

// Checkpoints do not support db_paths or cf_paths, also with blob direct
// write.
TEST_P(DBBlobDirectWritePathTest, CheckpointIsNotSupported) {
  Options options = GetPathOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", std::string(200, 'v')));

  const std::string checkpoint_dir = dbname_ + "_checkpoint";
  Checkpoint* raw_checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &raw_checkpoint));
  std::unique_ptr<Checkpoint> checkpoint(raw_checkpoint);
  Status s = checkpoint->CreateCheckpoint(checkpoint_dir,
                                          /*log_size_for_flush=*/0);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
}

INSTANTIATE_TEST_CASE_P(DBBlobDirectWritePathTest, DBBlobDirectWritePathTest,
                        ::testing::Bool());

class DBBlobDirectWriteWithTimestampTest : public DBBasicTestWithTimestampBase {
 protected:
  DBBlobDirectWriteWithTimestampTest()
      : DBBasicTestWithTimestampBase(
            "db_blob_direct_write_with_timestamp_test") {}
};

TEST_F(DBBlobDirectWriteWithTimestampTest,
       DirectWriteRejectsUserDefinedTimestamps) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.min_blob_size = 32;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
