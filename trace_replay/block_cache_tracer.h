//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <fstream>

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/block_cache_trace_writer.h"
#include "rocksdb/options.h"
#include "rocksdb/table_reader_caller.h"
#include "rocksdb/trace_reader_writer.h"
#include "trace_replay/trace_replay.h"

namespace ROCKSDB_NAMESPACE {
class Env;
class SystemClock;

extern const uint64_t kMicrosInSecond;
extern const uint64_t kSecondInMinute;
extern const uint64_t kSecondInHour;

struct BlockCacheTraceRecord;

class BlockCacheTraceHelper {
 public:
  static bool IsGetOrMultiGetOnDataBlock(TraceType block_type,
                                         TableReaderCaller caller);
  static bool IsGetOrMultiGet(TableReaderCaller caller);
  static bool IsUserAccess(TableReaderCaller caller);
  // Row key is a concatenation of the access's fd_number and the referenced
  // user key.
  static std::string ComputeRowKey(const BlockCacheTraceRecord& access);
  // The first four bytes of the referenced key in a Get request is the table
  // id.
  static uint64_t GetTableId(const BlockCacheTraceRecord& access);
  // The sequence number of a get request is the last part of the referenced
  // key.
  static uint64_t GetSequenceNumber(const BlockCacheTraceRecord& access);
  // Block offset in a file is the last varint64 in the block key.
  static uint64_t GetBlockOffsetInFile(const BlockCacheTraceRecord& access);

  static const std::string kUnknownColumnFamilyName;
  static const uint64_t kReservedGetId;
};

// Lookup context for tracing block cache accesses.
// We trace block accesses at five places:
// 1. BlockBasedTable::GetFilter
// 2. BlockBasedTable::GetUncompressedDict.
// 3. BlockBasedTable::MaybeReadAndLoadToCache. (To trace access on data, index,
// and range deletion block.)
// 4. BlockBasedTable::Get. (To trace the referenced key and whether the
// referenced key exists in a fetched data block.)
// 5. BlockBasedTable::MultiGet. (To trace the referenced key and whether the
// referenced key exists in a fetched data block.)
// The context is created at:
// 1. BlockBasedTable::Get. (kUserGet)
// 2. BlockBasedTable::MultiGet. (kUserMGet)
// 3. BlockBasedTable::NewIterator. (either kUserIterator, kCompaction, or
// external SST ingestion calls this function.)
// 4. BlockBasedTable::Open. (kPrefetch)
// 5. Index/Filter::CacheDependencies. (kPrefetch)
// 6. BlockBasedTable::ApproximateOffsetOf. (kCompaction or
// kUserApproximateSize).
struct BlockCacheLookupContext {
  BlockCacheLookupContext(const TableReaderCaller& _caller) : caller(_caller) {}
  BlockCacheLookupContext(const TableReaderCaller& _caller, uint64_t _get_id,
                          bool _get_from_user_specified_snapshot)
      : caller(_caller),
        get_id(_get_id),
        get_from_user_specified_snapshot(_get_from_user_specified_snapshot) {}
  const TableReaderCaller caller;
  // These are populated when we perform lookup/insert on block cache. The block
  // cache tracer uses these inforation when logging the block access at
  // BlockBasedTable::GET and BlockBasedTable::MultiGet.
  bool is_cache_hit = false;
  bool no_insert = false;
  TraceType block_type = TraceType::kTraceMax;
  uint64_t block_size = 0;
  std::string block_key;
  uint64_t num_keys_in_block = 0;
  // The unique id associated with Get and MultiGet. This enables us to track
  // how many blocks a Get/MultiGet request accesses. We can also measure the
  // impact of row cache vs block cache.
  uint64_t get_id = 0;
  std::string referenced_key;
  bool get_from_user_specified_snapshot = false;

  void FillLookupContext(bool _is_cache_hit, bool _no_insert,
                         TraceType _block_type, uint64_t _block_size,
                         const std::string& _block_key,
                         uint64_t _num_keys_in_block) {
    is_cache_hit = _is_cache_hit;
    no_insert = _no_insert;
    block_type = _block_type;
    block_size = _block_size;
    block_key = _block_key;
    num_keys_in_block = _num_keys_in_block;
  }
};

struct BlockCacheTraceHeader {
  uint64_t start_time;
  uint32_t rocksdb_major_version;
  uint32_t rocksdb_minor_version;
};

// BlockCacheTraceWriter captures all RocksDB block cache accesses using a
// user-provided TraceWriter. Every RocksDB operation is written as a single
// trace. Each trace will have a timestamp and type, followed by the trace
// payload.
class BlockCacheTraceWriterImpl : public BlockCacheTraceWriter {
 public:
  BlockCacheTraceWriterImpl(SystemClock* clock,
                            const BlockCacheTraceWriterOptions& trace_options,
                            std::unique_ptr<TraceWriter>&& trace_writer);
  ~BlockCacheTraceWriterImpl() override = default;
  // No copy and move.
  BlockCacheTraceWriterImpl(const BlockCacheTraceWriterImpl&) = delete;
  BlockCacheTraceWriterImpl& operator=(const BlockCacheTraceWriterImpl&) =
      delete;
  BlockCacheTraceWriterImpl(BlockCacheTraceWriterImpl&&) = delete;
  BlockCacheTraceWriterImpl& operator=(BlockCacheTraceWriterImpl&&) = delete;

  // Pass Slice references to avoid copy.
  Status WriteBlockAccess(const BlockCacheTraceRecord& record,
                          const Slice& block_key, const Slice& cf_name,
                          const Slice& referenced_key) override;

  // Write a trace header at the beginning, typically on initiating a trace,
  // with some metadata like a magic number and RocksDB version.
  Status WriteHeader() override;

 private:
  SystemClock* clock_;
  BlockCacheTraceWriterOptions trace_options_;
  std::unique_ptr<TraceWriter> trace_writer_;
};

// Write a trace record in human readable format, see
// https://github.com/meta/rocksdb/wiki/Block-cache-analysis-and-simulation-tools#trace-format
// for details.
class BlockCacheHumanReadableTraceWriter {
 public:
  ~BlockCacheHumanReadableTraceWriter();

  Status NewWritableFile(const std::string& human_readable_trace_file_path,
                         ROCKSDB_NAMESPACE::Env* env);

  Status WriteHumanReadableTraceRecord(const BlockCacheTraceRecord& access,
                                       uint64_t block_id, uint64_t get_key_id);

 private:
  char trace_record_buffer_[1024 * 1024];
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile>
      human_readable_trace_file_writer_;
};

// BlockCacheTraceReader helps read the trace file generated by
// BlockCacheTraceWriter using a user provided TraceReader.
class BlockCacheTraceReader {
 public:
  BlockCacheTraceReader(std::unique_ptr<TraceReader>&& reader);
  virtual ~BlockCacheTraceReader() = default;
  // No copy and move.
  BlockCacheTraceReader(const BlockCacheTraceReader&) = delete;
  BlockCacheTraceReader& operator=(const BlockCacheTraceReader&) = delete;
  BlockCacheTraceReader(BlockCacheTraceReader&&) = delete;
  BlockCacheTraceReader& operator=(BlockCacheTraceReader&&) = delete;

  Status ReadHeader(BlockCacheTraceHeader* header);

  Status ReadAccess(BlockCacheTraceRecord* record);

 private:
  std::unique_ptr<TraceReader> trace_reader_;
};

// Read a trace record in human readable format, see
// https://github.com/meta/rocksdb/wiki/Block-cache-analysis-and-simulation-tools#trace-format
// for detailed.
class BlockCacheHumanReadableTraceReader : public BlockCacheTraceReader {
 public:
  BlockCacheHumanReadableTraceReader(const std::string& trace_file_path);

  ~BlockCacheHumanReadableTraceReader() override;

  Status ReadHeader(BlockCacheTraceHeader* header);

  Status ReadAccess(BlockCacheTraceRecord* record);

 private:
  std::ifstream human_readable_trace_reader_;
};

// A block cache tracer. It downsamples the accesses according to
// trace_options and uses BlockCacheTraceWriter to write the access record to
// the trace file.
class BlockCacheTracer {
 public:
  BlockCacheTracer();
  ~BlockCacheTracer();
  // No copy and move.
  BlockCacheTracer(const BlockCacheTracer&) = delete;
  BlockCacheTracer& operator=(const BlockCacheTracer&) = delete;
  BlockCacheTracer(BlockCacheTracer&&) = delete;
  BlockCacheTracer& operator=(BlockCacheTracer&&) = delete;

  // Start writing block cache accesses to the trace_writer.
  Status StartTrace(const BlockCacheTraceOptions& trace_options,
                    std::unique_ptr<BlockCacheTraceWriter>&& trace_writer);

  // Stop writing block cache accesses to the trace_writer.
  void EndTrace();

  bool is_tracing_enabled() const {
    return writer_.load(std::memory_order_relaxed);
  }

  Status WriteBlockAccess(const BlockCacheTraceRecord& record,
                          const Slice& block_key, const Slice& cf_name,
                          const Slice& referenced_key);

  // GetId cycles from 1 to std::numeric_limits<uint64_t>::max().
  uint64_t NextGetId();

 private:
  BlockCacheTraceOptions trace_options_;
  // A mutex protects the writer_.
  InstrumentedMutex trace_writer_mutex_;
  std::atomic<BlockCacheTraceWriter*> writer_;
  std::atomic<uint64_t> get_id_counter_;
};

}  // namespace ROCKSDB_NAMESPACE
