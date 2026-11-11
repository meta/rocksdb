//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/block_cache_tracer.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

namespace {
const uint64_t kBlockSize = 1024;
constexpr char kBlockKeyPrefix[] = "test-block-";
const uint32_t kCFId = 0;
const uint32_t kLevel = 1;
const uint64_t kSSTFDNumber = 100;
constexpr char kRefKeyPrefix[] = "test-get-";
const uint64_t kNumKeysInBlock = 1024;
const uint64_t kReferencedDataSize = 10;
}  // namespace

class BlockCacheTracerTest : public testing::Test {
 public:
  BlockCacheTracerTest() {
    test_path_ = test::PerThreadDBPath("block_cache_tracer_test");
    env_ = ROCKSDB_NAMESPACE::Env::Default();
    clock_ = env_->GetSystemClock().get();
    EXPECT_OK(env_->CreateDir(test_path_));
    trace_file_path_ = test_path_ + "/block_cache_trace";
  }

  ~BlockCacheTracerTest() override {
    EXPECT_OK(env_->DeleteFile(trace_file_path_));
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

  TableReaderCaller GetCaller(uint32_t key_id) {
    uint32_t n = key_id % 5;
    switch (n) {
      case 0:
        return TableReaderCaller::kPrefetch;
      case 1:
        return TableReaderCaller::kCompaction;
      case 2:
        return TableReaderCaller::kUserGet;
      case 3:
        return TableReaderCaller::kUserMultiGet;
      case 4:
        return TableReaderCaller::kUserIterator;
    }
    assert(false);
    return TableReaderCaller::kMaxBlockCacheLookupCaller;
  }

  void WriteBlockAccess(BlockCacheTraceWriter* writer, uint32_t from_key_id,
                        TraceType block_type, uint32_t nblocks) {
    assert(writer);
    for (uint32_t i = 0; i < nblocks; i++) {
      uint32_t key_id = from_key_id + i;
      BlockCacheTraceRecord record;
      record.block_type = block_type;
      record.block_size = kBlockSize + key_id;
      record.block_key = (kBlockKeyPrefix + std::to_string(key_id));
      record.access_timestamp = clock_->NowMicros();
      record.cf_id = kCFId;
      record.cf_name = kDefaultColumnFamilyName;
      record.caller = GetCaller(key_id);
      record.level = kLevel;
      record.sst_fd_number = kSSTFDNumber + key_id;
      record.is_cache_hit = false;
      record.no_insert = false;
      // Provide get_id for all callers. The writer should only write get_id
      // when the caller is either GET or MGET.
      record.get_id = key_id + 1;
      record.get_from_user_specified_snapshot = true;
      // Provide these fields for all block types.
      // The writer should only write these fields for data blocks and the
      // caller is either GET or MGET.
      record.referenced_key = (kRefKeyPrefix + std::to_string(key_id));
      record.referenced_key_exist_in_block = true;
      record.num_keys_in_block = kNumKeysInBlock;
      record.referenced_data_size = kReferencedDataSize + key_id;
      ASSERT_OK(writer->WriteBlockAccess(
          record, record.block_key, record.cf_name, record.referenced_key));
    }
  }

  BlockCacheTraceRecord GenerateAccessRecord() {
    uint32_t key_id = 0;
    BlockCacheTraceRecord record;
    record.block_type = TraceType::kBlockTraceDataBlock;
    record.block_size = kBlockSize;
    record.block_key = kBlockKeyPrefix + std::to_string(key_id);
    record.access_timestamp = clock_->NowMicros();
    record.cf_id = kCFId;
    record.cf_name = kDefaultColumnFamilyName;
    record.caller = GetCaller(key_id);
    record.level = kLevel;
    record.sst_fd_number = kSSTFDNumber + key_id;
    record.is_cache_hit = false;
    record.no_insert = false;
    record.referenced_key = kRefKeyPrefix + std::to_string(key_id);
    record.referenced_key_exist_in_block = true;
    record.num_keys_in_block = kNumKeysInBlock;
    return record;
  }

  void VerifyAccess(BlockCacheTraceReader* reader, uint32_t from_key_id,
                    TraceType block_type, uint32_t nblocks) {
    assert(reader);
    for (uint32_t i = 0; i < nblocks; i++) {
      uint32_t key_id = from_key_id + i;
      BlockCacheTraceRecord record;
      ASSERT_OK(reader->ReadAccess(&record));
      ASSERT_EQ(block_type, record.block_type);
      ASSERT_EQ(kBlockSize + key_id, record.block_size);
      ASSERT_EQ(kBlockKeyPrefix + std::to_string(key_id), record.block_key);
      ASSERT_EQ(kCFId, record.cf_id);
      ASSERT_EQ(kDefaultColumnFamilyName, record.cf_name);
      ASSERT_EQ(GetCaller(key_id), record.caller);
      ASSERT_EQ(kLevel, record.level);
      ASSERT_EQ(kSSTFDNumber + key_id, record.sst_fd_number);
      ASSERT_FALSE(record.is_cache_hit);
      ASSERT_FALSE(record.no_insert);
      if (record.caller == TableReaderCaller::kUserGet ||
          record.caller == TableReaderCaller::kUserMultiGet) {
        ASSERT_EQ(key_id + 1, record.get_id);
        ASSERT_TRUE(record.get_from_user_specified_snapshot);
        ASSERT_EQ(kRefKeyPrefix + std::to_string(key_id),
                  record.referenced_key);
      } else {
        ASSERT_EQ(BlockCacheTraceHelper::kReservedGetId, record.get_id);
        ASSERT_FALSE(record.get_from_user_specified_snapshot);
        ASSERT_EQ("", record.referenced_key);
      }
      if (block_type == TraceType::kBlockTraceDataBlock &&
          (record.caller == TableReaderCaller::kUserGet ||
           record.caller == TableReaderCaller::kUserMultiGet)) {
        ASSERT_TRUE(record.referenced_key_exist_in_block);
        ASSERT_EQ(kNumKeysInBlock, record.num_keys_in_block);
        ASSERT_EQ(kReferencedDataSize + key_id, record.referenced_data_size);
        continue;
      }
      ASSERT_FALSE(record.referenced_key_exist_in_block);
      ASSERT_EQ(0, record.num_keys_in_block);
      ASSERT_EQ(0, record.referenced_data_size);
    }
  }

  Env* env_;
  SystemClock* clock_;
  EnvOptions env_options_;
  std::string trace_file_path_;
  std::string test_path_;
};

TEST_F(BlockCacheTracerTest, AtomicWriteBeforeStartTrace) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  {
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    BlockCacheTracer writer;
    // The record should be written to the trace_file since StartTrace is not
    // called.
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains nothing.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_NOK(reader.ReadHeader(&header));
  }
}

TEST_F(BlockCacheTracerTest, AtomicWrite) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  {
    BlockCacheTraceWriterOptions trace_writer_opt;
    BlockCacheTraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    BlockCacheTracer writer;
    ASSERT_OK(
        writer.StartTrace(trace_opt, std::move(block_cache_trace_writer)));
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains one record.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    VerifyAccess(&reader, 0, TraceType::kBlockTraceDataBlock, 1);
    ASSERT_NOK(reader.ReadAccess(&record));
  }
}

TEST_F(BlockCacheTracerTest, ConsecutiveStartTrace) {
  BlockCacheTraceWriterOptions trace_writer_opt;
  BlockCacheTraceOptions trace_opt;
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, env_options_, trace_file_path_, &trace_writer));
  std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
      NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                               std::move(trace_writer));
  ASSERT_NE(block_cache_trace_writer, nullptr);
  BlockCacheTracer writer;
  ASSERT_OK(writer.StartTrace(trace_opt, std::move(block_cache_trace_writer)));
  ASSERT_NOK(writer.StartTrace(trace_opt, std::move(block_cache_trace_writer)));
  ASSERT_OK(env_->FileExists(trace_file_path_));
}

TEST_F(BlockCacheTracerTest, AtomicNoWriteAfterEndTrace) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  {
    BlockCacheTraceWriterOptions trace_writer_opt;
    BlockCacheTraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    BlockCacheTracer writer;
    ASSERT_OK(
        writer.StartTrace(trace_opt, std::move(block_cache_trace_writer)));
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    writer.EndTrace();
    // Write the record again. This time the record should not be written since
    // EndTrace is called.
    ASSERT_OK(writer.WriteBlockAccess(record, record.block_key, record.cf_name,
                                      record.referenced_key));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains one record.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    VerifyAccess(&reader, 0, TraceType::kBlockTraceDataBlock, 1);
    ASSERT_NOK(reader.ReadAccess(&record));
  }
}

TEST_F(BlockCacheTracerTest, NextGetId) {
  BlockCacheTracer writer;
  {
    BlockCacheTraceWriterOptions trace_writer_opt;
    BlockCacheTraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    // next get id should always return 0 before we call StartTrace.
    ASSERT_EQ(0, writer.NextGetId());
    ASSERT_EQ(0, writer.NextGetId());
    ASSERT_OK(
        writer.StartTrace(trace_opt, std::move(block_cache_trace_writer)));
    ASSERT_EQ(1, writer.NextGetId());
    ASSERT_EQ(2, writer.NextGetId());
    writer.EndTrace();
    // next get id should return 0.
    ASSERT_EQ(0, writer.NextGetId());
  }

  // Start trace again and next get id should return 1.
  {
    BlockCacheTraceWriterOptions trace_writer_opt;
    BlockCacheTraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    ASSERT_OK(
        writer.StartTrace(trace_opt, std::move(block_cache_trace_writer)));
    ASSERT_EQ(1, writer.NextGetId());
  }
}

TEST_F(BlockCacheTracerTest, MixedBlocks) {
  {
    // Generate a trace file containing a mix of blocks.
    BlockCacheTraceWriterOptions trace_writer_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(env_->GetSystemClock().get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    ASSERT_OK(block_cache_trace_writer->WriteHeader());
    // Write blocks of different types.
    WriteBlockAccess(block_cache_trace_writer.get(), 0,
                     TraceType::kBlockTraceUncompressionDictBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 10,
                     TraceType::kBlockTraceDataBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 20,
                     TraceType::kBlockTraceFilterBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 30,
                     TraceType::kBlockTraceIndexBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 40,
                     TraceType::kBlockTraceRangeDeletionBlock, 10);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }

  {
    // Verify trace file is generated correctly.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    // Read blocks.
    VerifyAccess(&reader, 0, TraceType::kBlockTraceUncompressionDictBlock, 10);
    VerifyAccess(&reader, 10, TraceType::kBlockTraceDataBlock, 10);
    VerifyAccess(&reader, 20, TraceType::kBlockTraceFilterBlock, 10);
    VerifyAccess(&reader, 30, TraceType::kBlockTraceIndexBlock, 10);
    VerifyAccess(&reader, 40, TraceType::kBlockTraceRangeDeletionBlock, 10);
    // Read one more record should report an error.
    BlockCacheTraceRecord record;
    ASSERT_NOK(reader.ReadAccess(&record));
  }
}

TEST_F(BlockCacheTracerTest, HumanReadableTrace) {
  BlockCacheTraceRecord record = GenerateAccessRecord();
  record.get_id = 1;
  record.referenced_key = "";
  record.caller = TableReaderCaller::kUserGet;
  record.get_from_user_specified_snapshot = true;
  record.referenced_data_size = kReferencedDataSize;
  PutFixed32(&record.referenced_key, 111);
  PutLengthPrefixedSlice(&record.referenced_key, "get_key");
  PutFixed64(&record.referenced_key, 2 << 8);
  PutLengthPrefixedSlice(&record.block_key, "block_key");
  PutVarint64(&record.block_key, 333);
  {
    // Generate a human readable trace file.
    BlockCacheHumanReadableTraceWriter writer;
    ASSERT_OK(writer.NewWritableFile(trace_file_path_, env_));
    ASSERT_OK(writer.WriteHumanReadableTraceRecord(record, 1, 1));
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    BlockCacheHumanReadableTraceReader reader(trace_file_path_);
    BlockCacheTraceHeader header;
    BlockCacheTraceRecord read_record;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_OK(reader.ReadAccess(&read_record));
    ASSERT_EQ(TraceType::kBlockTraceDataBlock, read_record.block_type);
    ASSERT_EQ(kBlockSize, read_record.block_size);
    ASSERT_EQ(kCFId, read_record.cf_id);
    ASSERT_EQ(kDefaultColumnFamilyName, read_record.cf_name);
    ASSERT_EQ(TableReaderCaller::kUserGet, read_record.caller);
    ASSERT_EQ(kLevel, read_record.level);
    ASSERT_EQ(kSSTFDNumber, read_record.sst_fd_number);
    ASSERT_FALSE(read_record.is_cache_hit);
    ASSERT_FALSE(read_record.no_insert);
    ASSERT_EQ(1, read_record.get_id);
    ASSERT_TRUE(read_record.get_from_user_specified_snapshot);
    ASSERT_TRUE(read_record.referenced_key_exist_in_block);
    ASSERT_EQ(kNumKeysInBlock, read_record.num_keys_in_block);
    ASSERT_EQ(kReferencedDataSize, read_record.referenced_data_size);
    ASSERT_EQ(record.block_key.size(), read_record.block_key.size());
    ASSERT_EQ(record.referenced_key.size(), record.referenced_key.size());
    ASSERT_EQ(112, BlockCacheTraceHelper::GetTableId(read_record));
    ASSERT_EQ(3, BlockCacheTraceHelper::GetSequenceNumber(read_record));
    ASSERT_EQ(333, BlockCacheTraceHelper::GetBlockOffsetInFile(read_record));
    // Read again should fail.
    ASSERT_NOK(reader.ReadAccess(&read_record));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
