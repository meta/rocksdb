//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//    This source code is licensed under both the GPLv2 (found in the
//    COPYING file in the root directory) and Apache 2.0 License
//    (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS
#include "tools/io_tracer_parser_tool.h"

#include <cinttypes>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>

#include "port/lang.h"
#include "rocksdb/trace_reader_writer.h"
#include "trace_replay/io_tracer.h"
#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_string(io_trace_file, "", "The IO trace file path.");

namespace ROCKSDB_NAMESPACE {

IOTraceRecordParser::IOTraceRecordParser(const std::string& input_file)
    : input_file_(input_file) {}

void IOTraceRecordParser::PrintHumanReadableHeader(
    const IOTraceHeader& header) {
  std::stringstream ss;
  ss << "Start Time: " << header.start_time
     << "\nRocksDB Major Version: " << header.rocksdb_major_version
     << "\nRocksDB Minor Version: " << header.rocksdb_minor_version << "\n";
  fprintf(stdout, "%s", ss.str().c_str());
}

void IOTraceRecordParser::PrintHumanReadableIOTraceRecord(
    const IOTraceRecord& record) {
  std::stringstream ss;
  ss << "Access Time : " << std::setw(20) << std::left
     << record.access_timestamp << ", File Name: " << std::setw(20) << std::left
     << record.file_name.c_str() << ", File Operation: " << std::setw(18)
     << std::left << record.file_operation.c_str()
     << ", Latency: " << std::setw(10) << std::left << record.latency
     << ", IO Status: " << record.io_status.c_str();

  // Each bit in io_op_data stores which corresponding info from IOTraceOp will
  // be added in the trace. Foreg, if bit at position 1 is set then
  // IOTraceOp::kIOLen (length) will be logged in the record (Since
  // IOTraceOp::kIOLen = 1 in the enum). So find all the set positions in
  // io_op_data one by one and, update corresponsing info in the trace record,
  // unset that bit to find other set bits until io_op_data = 0.
  /* Read remaining options based on io_op_data set by file operation */
  int64_t io_op_data = static_cast<int64_t>(record.io_op_data);
  while (io_op_data) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(io_op_data & -io_op_data));
    switch (set_pos) {
      case IOTraceOp::kIOFileSize:
        ss << ", File Size: " << record.file_size;
        break;
      case IOTraceOp::kIOLen:
        ss << ", Length: " << record.len;
        break;
      case IOTraceOp::kIOOffset:
        ss << ", Offset: " << record.offset;
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    io_op_data &= (io_op_data - 1);
  }

  int64_t trace_data = static_cast<int64_t>(record.trace_data);
  while (trace_data) {
    // Find the rightmost set bit.
    uint32_t set_pos = static_cast<uint32_t>(log2(trace_data & -trace_data));
    switch (set_pos) {
      case IODebugContext::TraceData::kRequestID:
        ss << ", Request Id: " << record.request_id;
        break;
      default:
        assert(false);
    }
    // unset the rightmost bit.
    trace_data &= (trace_data - 1);
  }

  ss << "\n";
  fprintf(stdout, "%s", ss.str().c_str());
}

int IOTraceRecordParser::ReadIOTraceRecords() {
  Status status;
  Env* env(Env::Default());
  std::unique_ptr<TraceReader> trace_reader;
  std::unique_ptr<IOTraceReader> io_trace_reader;

  status = NewFileTraceReader(env, EnvOptions(), input_file_, &trace_reader);
  if (!status.ok()) {
    fprintf(stderr, "%s: %s\n", input_file_.c_str(), status.ToString().c_str());
    return 1;
  }
  io_trace_reader.reset(new IOTraceReader(std::move(trace_reader)));

  // Read the header and dump it in a file.
  IOTraceHeader header{};
  status = io_trace_reader->ReadHeader(&header);
  if (!status.ok()) {
    fprintf(stderr, "%s: %s\n", input_file_.c_str(), status.ToString().c_str());
    return 1;
  }
  PrintHumanReadableHeader(header);

  // Read the records one by one and print them in human readable format.
  while (status.ok()) {
    IOTraceRecord record;
    status = io_trace_reader->ReadIOOp(&record);
    if (!status.ok()) {
      break;
    }
    PrintHumanReadableIOTraceRecord(record);
  }
  return 0;
}

int io_tracer_parser(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_io_trace_file.empty()) {
    fprintf(stderr, "IO Trace file path is empty\n");
    return 1;
  }

  IOTraceRecordParser io_tracer_parser(FLAGS_io_trace_file);
  return io_tracer_parser.ReadIOTraceRecords();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
