//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/replayer.h"

namespace ROCKSDB_NAMESPACE {

// This file contains Tracer and Replayer classes that enable capturing and
// replaying RocksDB traces.

class ColumnFamilyHandle;
class ColumnFamilyData;
class DB;
class DBImpl;
class Env;
class Slice;
class SystemClock;
class TraceReader;
class TraceWriter;
class WriteBatch;

struct ReadOptions;
struct TraceOptions;
struct WriteOptions;

extern const std::string kTraceMagic;
const unsigned int kTraceTimestampSize = 8;
const unsigned int kTraceTypeSize = 1;
const unsigned int kTracePayloadLengthSize = 4;
const unsigned int kTraceMetadataSize =
    kTraceTimestampSize + kTraceTypeSize + kTracePayloadLengthSize;

static const int kTraceFileMajorVersion = 0;
static const int kTraceFileMinorVersion = 2;

// The data structure that defines a single trace.
struct Trace {
  uint64_t ts;  // timestamp
  TraceType type;
  // Each bit in payload_map stores which corresponding struct member added in
  // the payload. Each TraceType has its corresponding payload struct. For
  // example, if bit at position 0 is set in write payload, then the write batch
  // will be addedd.
  uint64_t payload_map = 0;
  // Each trace type has its own payload_struct, which will be serialized in the
  // payload.
  std::string payload;

  void reset() {
    ts = 0;
    type = kTraceMax;
    payload_map = 0;
    payload.clear();
  }
};

enum TracePayloadType : char {
  // Each member of all query payload structs should have a corresponding flag
  // here. Make sure to add them sequentially in the order of it is added.
  kEmptyPayload = 0,
  kWriteBatchData = 1,
  kGetCFID = 2,
  kGetKey = 3,
  kIterCFID = 4,
  kIterKey = 5,
  kIterLowerBound = 6,
  kIterUpperBound = 7,
  kMultiGetSize = 8,
  kMultiGetCFIDs = 9,
  kMultiGetKeys = 10,
  // TraceOptions::include_read_write_options. Encoded after the fields above,
  // so a decoder that does not know them can skip them.
  kWriteOptions = 11,
  kGetReadOptions = 12,
  kIterReadOptions = 13,
  kMultiGetReadOptions = 14,
};

// The WriteOptions fields a query trace records with
// TraceOptions::include_read_write_options.
struct TracedWriteOptions {
  TracedWriteOptions() = default;
  explicit TracedWriteOptions(const WriteOptions& options);

  // Appends a length-prefixed encoding: fields added later go at its end, and
  // DecodeFrom() ignores what it does not know.
  void EncodeTo(std::string* dst) const;
  // Reads one encoding from the front of `input` into `*options`.
  static Status DecodeFrom(Slice* input, WriteOptions* options);

  bool sync = false;
  bool disable_wal = false;
  bool no_slowdown = false;
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;
};

// The ReadOptions fields a query trace records with
// TraceOptions::include_read_write_options. An iterator keeps one for the
// seeks it traces.
struct TracedReadOptions {
  TracedReadOptions() = default;
  explicit TracedReadOptions(const ReadOptions& options);

  // Same contract as TracedWriteOptions::EncodeTo() and DecodeFrom().
  void EncodeTo(std::string* dst) const;
  static Status DecodeFrom(Slice* input, ReadOptions* options);

  uint64_t readahead_size = 0;
  uint64_t max_skippable_internal_keys = 0;
  uint64_t value_size_soft_limit = 0;
  // Bit i holds the i-th boolean field of kReadOptionsFlags
  // (trace_replay.cc).
  uint32_t flags = 0;
  ReadTier read_tier = kReadAllTier;
  Env::IOPriority rate_limiter_priority = Env::IO_TOTAL;
};

class TracerHelper {
 public:
  // Parse the string with major and minor version only
  static Status ParseVersionStr(std::string& v_string, int* v_num);

  // Parse the trace file version and db version in trace header
  static Status ParseTraceHeader(const Trace& header, int* trace_version,
                                 int* db_version);

  // Encode a version 0.1 trace object into the given string.
  static void EncodeTrace(const Trace& trace, std::string* encoded_trace);

  // Decode a string into the given trace object.
  static Status DecodeTrace(const std::string& encoded_trace, Trace* trace);

  // Decode a string into the given trace header.
  static Status DecodeHeader(const std::string& encoded_trace, Trace* header);

  // Set the payload map based on the payload type
  static bool SetPayloadMap(uint64_t& payload_map,
                            const TracePayloadType payload_type);

  // Decode a Trace object into the corresponding TraceRecord.
  // Return Status::OK() if nothing is wrong, record will be set accordingly.
  // Return Status::NotSupported() if the trace type is not support, or the
  // corresponding error status, record will be set to nullptr.
  static Status DecodeTraceRecord(Trace* trace, int trace_file_version,
                                  std::unique_ptr<TraceRecord>* record);
};

// Tracer captures all RocksDB operations using a user-provided TraceWriter.
// Every RocksDB operation is written as a single trace. Each trace will have a
// timestamp and type, followed by the trace payload.
class Tracer {
 public:
  Tracer(SystemClock* clock, const TraceOptions& trace_options,
         std::unique_ptr<TraceWriter>&& trace_writer);
  ~Tracer();

  // Trace all write operations -- Put, Merge, Delete, SingleDelete, Write.
  // The options are recorded only with
  // TraceOptions::include_read_write_options, as are the read options below.
  Status Write(WriteBatch* write_batch, const TracedWriteOptions& options);

  // Trace Get operations.
  Status Get(ColumnFamilyHandle* cfname, const Slice& key,
             const ReadOptions& read_options);

  // Trace Iterators.
  Status IteratorSeek(const uint32_t& cf_id, const Slice& key,
                      const Slice& lower_bound, const Slice upper_bound,
                      const TracedReadOptions& read_options);
  Status IteratorSeekForPrev(const uint32_t& cf_id, const Slice& key,
                             const Slice& lower_bound, const Slice upper_bound,
                             const TracedReadOptions& read_options);

  // Trace MultiGet

  Status MultiGet(const size_t num_keys, ColumnFamilyHandle** column_families,
                  const Slice* keys, const ReadOptions& read_options);

  Status MultiGet(const size_t num_keys, ColumnFamilyHandle* column_family,
                  const Slice* keys, const ReadOptions& read_options);

  Status MultiGet(const std::vector<ColumnFamilyHandle*>& column_family,
                  const std::vector<Slice>& keys,
                  const ReadOptions& read_options);

  // Returns true if the trace is over the configured max trace file limit.
  // False otherwise.
  bool IsTraceFileOverMax();

  // Returns true if the order of write trace records must match the order of
  // the corresponding records logged to WAL and applied to the DB.
  bool IsWriteOrderPreserved() { return trace_options_.preserve_write_order; }

  // Writes a trace footer at the end of the tracing
  Status Close();

 private:
  // Write a trace header at the beginning, typically on initiating a trace,
  // with some metadata like a magic number, trace version, RocksDB version, and
  // trace format.
  Status WriteHeader();

  // Write a trace footer, typically on ending a trace, with some metadata.
  Status WriteFooter();

  // Write a single trace using the provided TraceWriter to the underlying
  // system, say, a filesystem or a streaming service.
  Status WriteTrace(const Trace& trace);

  // Shared by IteratorSeek() and IteratorSeekForPrev().
  Status IteratorSeekImpl(TraceType trace_type, uint32_t cf_id,
                          const Slice& key, const Slice& lower_bound,
                          const Slice& upper_bound,
                          const TracedReadOptions& read_options);

  // Helps in filtering and sampling of traces.
  // Returns true if a trace should be skipped, false otherwise.
  bool ShouldSkipTrace(const TraceType& type);

  SystemClock* clock_;
  TraceOptions trace_options_;
  std::unique_ptr<TraceWriter> trace_writer_;
  uint64_t trace_request_count_;
  Status trace_write_status_;
};

}  // namespace ROCKSDB_NAMESPACE
