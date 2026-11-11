//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "trace_replay/io_tracer.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

namespace {
constexpr char kDummyFile[] = "/dummy/file";

}  // namespace

class IOTracerTest : public testing::Test {
 public:
  IOTracerTest() {
    test_path_ = test::PerThreadDBPath("io_tracer_test");
    env_ = ROCKSDB_NAMESPACE::Env::Default();
    clock_ = env_->GetSystemClock().get();
    EXPECT_OK(env_->CreateDir(test_path_));
    trace_file_path_ = test_path_ + "/io_trace";
  }

  ~IOTracerTest() override {
    EXPECT_OK(env_->DeleteFile(trace_file_path_));
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

  std::string GetFileOperation(uint64_t id) {
    id = id % 4;
    switch (id) {
      case 0:
        return "CreateDir";
      case 1:
        return "GetChildren";
      case 2:
        return "FileSize";
      case 3:
        return "DeleteDir";
      default:
        assert(false);
    }
    return "";
  }

  void WriteIOOp(IOTraceWriter* writer, uint64_t nrecords) {
    assert(writer);
    for (uint64_t i = 0; i < nrecords; i++) {
      IOTraceRecord record;
      record.io_op_data = 0;
      record.trace_type = TraceType::kIOTracer;
      record.io_op_data |= (1 << IOTraceOp::kIOLen);
      record.io_op_data |= (1 << IOTraceOp::kIOOffset);
      record.file_operation = GetFileOperation(i);
      record.io_status = IOStatus::OK().ToString();
      record.file_name = kDummyFile + std::to_string(i);
      record.len = i;
      record.offset = i + 20;
      EXPECT_OK(writer->WriteIOOp(record, nullptr));
    }
  }

  void VerifyIOOp(IOTraceReader* reader, uint32_t nrecords) {
    assert(reader);
    for (uint32_t i = 0; i < nrecords; i++) {
      IOTraceRecord record;
      ASSERT_OK(reader->ReadIOOp(&record));
      ASSERT_EQ(record.file_operation, GetFileOperation(i));
      ASSERT_EQ(record.io_status, IOStatus::OK().ToString());
      ASSERT_EQ(record.len, i);
      ASSERT_EQ(record.offset, i + 20);
    }
  }

  Env* env_;
  SystemClock* clock_;
  EnvOptions env_options_;
  std::string trace_file_path_;
  std::string test_path_;
};

TEST_F(IOTracerTest, MultipleRecordsWithDifferentIOOpOptions) {
  std::string file_name = kDummyFile + std::to_string(5);
  {
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;

    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    IOTracer writer;
    ASSERT_OK(writer.StartIOTrace(clock_, trace_opt, std::move(trace_writer)));

    // Write general record.
    IOTraceRecord record0(0, TraceType::kIOTracer, 0 /*io_op_data*/,
                          GetFileOperation(0), 155 /*latency*/,
                          IOStatus::OK().ToString(), file_name);
    writer.WriteIOOp(record0, nullptr);

    // Write record with FileSize.
    uint64_t io_op_data = 0;
    io_op_data |= (1 << IOTraceOp::kIOFileSize);
    IOTraceRecord record1(0, TraceType::kIOTracer, io_op_data,
                          GetFileOperation(1), 10 /*latency*/,
                          IOStatus::OK().ToString(), file_name,
                          256 /*file_size*/);
    writer.WriteIOOp(record1, nullptr);

    // Write record with Length.
    io_op_data = 0;
    io_op_data |= (1 << IOTraceOp::kIOLen);
    IOTraceRecord record2(0, TraceType::kIOTracer, io_op_data,
                          GetFileOperation(2), 10 /*latency*/,
                          IOStatus::OK().ToString(), file_name, 100 /*length*/,
                          200 /*offset*/);
    writer.WriteIOOp(record2, nullptr);

    // Write record with Length and offset.
    io_op_data = 0;
    io_op_data |= (1 << IOTraceOp::kIOLen);
    io_op_data |= (1 << IOTraceOp::kIOOffset);
    IOTraceRecord record3(0, TraceType::kIOTracer, io_op_data,
                          GetFileOperation(3), 10 /*latency*/,
                          IOStatus::OK().ToString(), file_name, 120 /*length*/,
                          17 /*offset*/);
    writer.WriteIOOp(record3, nullptr);

    // Write record with offset.
    io_op_data = 0;
    io_op_data |= (1 << IOTraceOp::kIOOffset);
    IOTraceRecord record4(0, TraceType::kIOTracer, io_op_data,
                          GetFileOperation(4), 10 /*latency*/,
                          IOStatus::OK().ToString(), file_name, 13 /*length*/,
                          50 /*offset*/);
    writer.WriteIOOp(record4, nullptr);

    // Write record with IODebugContext.
    io_op_data = 0;
    IODebugContext dbg;
    const std::string test_request_id = "request_id_1";
    dbg.SetRequestId(&test_request_id);
    IOTraceRecord record5(0, TraceType::kIOTracer, io_op_data,
                          GetFileOperation(5), 10 /*latency*/,
                          IOStatus::OK().ToString(), file_name);
    writer.WriteIOOp(record5, &dbg);

    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file is generated correctly.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    IOTraceReader reader(std::move(trace_reader));
    IOTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));

    // Read general record.
    IOTraceRecord record0;
    ASSERT_OK(reader.ReadIOOp(&record0));
    ASSERT_EQ(record0.file_operation, GetFileOperation(0));
    ASSERT_EQ(record0.latency, 155);
    ASSERT_EQ(record0.file_name, file_name);

    // Read record with FileSize.
    IOTraceRecord record1;
    ASSERT_OK(reader.ReadIOOp(&record1));
    ASSERT_EQ(record1.file_size, 256);
    ASSERT_EQ(record1.len, 0);
    ASSERT_EQ(record1.offset, 0);

    // Read record with Length.
    IOTraceRecord record2;
    ASSERT_OK(reader.ReadIOOp(&record2));
    ASSERT_EQ(record2.len, 100);
    ASSERT_EQ(record2.file_size, 0);
    ASSERT_EQ(record2.offset, 0);

    // Read record with Length and offset.
    IOTraceRecord record3;
    ASSERT_OK(reader.ReadIOOp(&record3));
    ASSERT_EQ(record3.len, 120);
    ASSERT_EQ(record3.file_size, 0);
    ASSERT_EQ(record3.offset, 17);

    // Read record with offset.
    IOTraceRecord record4;
    ASSERT_OK(reader.ReadIOOp(&record4));
    ASSERT_EQ(record4.len, 0);
    ASSERT_EQ(record4.file_size, 0);
    ASSERT_EQ(record4.offset, 50);

    IOTraceRecord record5;
    ASSERT_OK(reader.ReadIOOp(&record5));
    ASSERT_EQ(record5.len, 0);
    ASSERT_EQ(record5.file_size, 0);
    ASSERT_EQ(record5.offset, 0);
    ASSERT_EQ(record5.request_id, "request_id_1");
    // Read one more record and it should report error.
    IOTraceRecord record6;
    ASSERT_NOK(reader.ReadIOOp(&record6));
  }
}

TEST_F(IOTracerTest, AtomicWrite) {
  std::string file_name = kDummyFile + std::to_string(0);
  {
    IOTraceRecord record(0, TraceType::kIOTracer, 0 /*io_op_data*/,
                         GetFileOperation(0), 10 /*latency*/,
                         IOStatus::OK().ToString(), file_name);
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    IOTracer writer;
    ASSERT_OK(writer.StartIOTrace(clock_, trace_opt, std::move(trace_writer)));
    writer.WriteIOOp(record, nullptr);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains one record.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    IOTraceReader reader(std::move(trace_reader));
    IOTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    // Read record and verify data.
    IOTraceRecord access_record;
    ASSERT_OK(reader.ReadIOOp(&access_record));
    ASSERT_EQ(access_record.file_operation, GetFileOperation(0));
    ASSERT_EQ(access_record.io_status, IOStatus::OK().ToString());
    ASSERT_EQ(access_record.file_name, file_name);
    ASSERT_NOK(reader.ReadIOOp(&access_record));
  }
}

TEST_F(IOTracerTest, AtomicWriteBeforeStartTrace) {
  std::string file_name = kDummyFile + std::to_string(0);
  {
    IOTraceRecord record(0, TraceType::kIOTracer, 0 /*io_op_data*/,
                         GetFileOperation(0), 0, IOStatus::OK().ToString(),
                         file_name);
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    IOTracer writer;
    // The record should not be written to the trace_file since StartIOTrace is
    // not called.
    writer.WriteIOOp(record, nullptr);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains nothing.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    IOTraceReader reader(std::move(trace_reader));
    IOTraceHeader header;
    ASSERT_NOK(reader.ReadHeader(&header));
  }
}

TEST_F(IOTracerTest, AtomicNoWriteAfterEndTrace) {
  std::string file_name = kDummyFile + std::to_string(0);
  {
    uint64_t io_op_data = 0;
    io_op_data |= (1 << IOTraceOp::kIOFileSize);
    IOTraceRecord record(
        0, TraceType::kIOTracer, io_op_data, GetFileOperation(2), 0 /*latency*/,
        IOStatus::OK().ToString(), file_name, 10 /*file_size*/);
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    IOTracer writer;
    ASSERT_OK(writer.StartIOTrace(clock_, trace_opt, std::move(trace_writer)));
    writer.WriteIOOp(record, nullptr);
    writer.EndIOTrace();
    // Write the record again. This time the record should not be written since
    // EndIOTrace is called.
    writer.WriteIOOp(record, nullptr);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Verify trace file contains one record.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    IOTraceReader reader(std::move(trace_reader));
    IOTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));

    IOTraceRecord access_record;
    ASSERT_OK(reader.ReadIOOp(&access_record));
    ASSERT_EQ(access_record.file_operation, GetFileOperation(2));
    ASSERT_EQ(access_record.io_status, IOStatus::OK().ToString());
    ASSERT_EQ(access_record.file_size, 10);
    // No more record.
    ASSERT_NOK(reader.ReadIOOp(&access_record));
  }
}

TEST_F(IOTracerTest, AtomicMultipleWrites) {
  {
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    IOTraceWriter writer(clock_, trace_opt, std::move(trace_writer));
    ASSERT_OK(writer.WriteHeader());
    // Write 10 records
    WriteIOOp(&writer, 10);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }

  {
    // Verify trace file is generated correctly.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    IOTraceReader reader(std::move(trace_reader));
    IOTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));
    // Read 10 records.
    VerifyIOOp(&reader, 10);
    // Read one more and record and it should report error.
    IOTraceRecord record;
    ASSERT_NOK(reader.ReadIOOp(&record));
  }
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
