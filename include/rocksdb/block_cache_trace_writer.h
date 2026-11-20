// Copyright (c) 2022, Meta Platforms, Inc. and affiliates.  All rights
// reserved. This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <utility>

#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/table_reader_caller.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"

namespace ROCKSDB_NAMESPACE {
// A record for block cache lookups/inserts. This is passed by the table
// reader to the BlockCacheTraceWriter for every block cache op.
struct BlockCacheTraceRecord {
  // Required fields for all accesses.
  uint64_t access_timestamp = 0;

  // Info related to the block being looked up or inserted
  //
  // 1. The cache key for the block
  std::string block_key;

  // 2. The type of block
  TraceType block_type = TraceType::kTraceMax;

  // 3. Size of the block
  uint64_t block_size = 0;

  // Info about the SST file the block is in
  //
  // 1. Column family ID
  uint64_t cf_id = 0;

  // 2. Column family name
  std::string cf_name;

  // 3. LSM level of the file
  uint32_t level = 0;

  // 4. SST file number
  uint64_t sst_fd_number = 0;

  // Info about the calling context
  //
  // 1. The higher level request triggering the block cache request
  TableReaderCaller caller = TableReaderCaller::kMaxBlockCacheLookupCaller;

  // 2. Cache lookup hit/miss. Not relevant for inserts
  bool is_cache_hit = false;

  // 3. Whether this request is a lookup
  bool no_insert = false;

  // Get/MultiGet specific info
  //
  // 1. A unique ID for Get/MultiGet
  uint64_t get_id = kReservedGetId;

  // 2. Whether the Get/MultiGet is from a user-specified snapshot
  bool get_from_user_specified_snapshot = false;

  // 3. The target user key in the block
  std::string referenced_key;

  // Required fields for data block and user Get/Multi-Get only.
  //
  // 1. Size of te useful data in the block
  uint64_t referenced_data_size = 0;

  // 2. Only for MultiGet, number of keys from the batch found in the block
  uint64_t num_keys_in_block = 0;

  // 3. Whether the key was found in the block or not (false positive)
  bool referenced_key_exist_in_block = false;

  static const uint64_t kReservedGetId;

  BlockCacheTraceRecord() {}

  BlockCacheTraceRecord(uint64_t _access_timestamp, std::string _block_key,
                        TraceType _block_type, uint64_t _block_size,
                        uint64_t _cf_id, std::string _cf_name, uint32_t _level,
                        uint64_t _sst_fd_number, TableReaderCaller _caller,
                        bool _is_cache_hit, bool _no_insert, uint64_t _get_id,
                        bool _get_from_user_specified_snapshot = false,
                        std::string _referenced_key = "",
                        uint64_t _referenced_data_size = 0,
                        uint64_t _num_keys_in_block = 0,
                        bool _referenced_key_exist_in_block = false)
      : access_timestamp(_access_timestamp),
        block_key(std::move(_block_key)),
        block_type(_block_type),
        block_size(_block_size),
        cf_id(_cf_id),
        cf_name(std::move(_cf_name)),
        level(_level),
        sst_fd_number(_sst_fd_number),
        caller(_caller),
        is_cache_hit(_is_cache_hit),
        no_insert(_no_insert),
        get_id(_get_id),
        get_from_user_specified_snapshot(_get_from_user_specified_snapshot),
        referenced_key(std::move(_referenced_key)),
        referenced_data_size(_referenced_data_size),
        num_keys_in_block(_num_keys_in_block),
        referenced_key_exist_in_block(_referenced_key_exist_in_block) {}
};

// Options for tracing block cache accesses
struct BlockCacheTraceOptions {
  // Specify trace sampling option, i.e. capture one per how many requests.
  // Default to 1 (capture every request).
  uint64_t sampling_frequency = 1;
};

// Options for the built-in implementation of BlockCacheTraceWriter
struct BlockCacheTraceWriterOptions {
  uint64_t max_trace_file_size = uint64_t{64} * 1024 * 1024 * 1024;
};

// BlockCacheTraceWriter is an abstract class that captures all RocksDB block
// cache accesses. Every RocksDB operation is passed to WriteBlockAccess()
// with a BlockCacheTraceRecord.
class BlockCacheTraceWriter {
 public:
  virtual ~BlockCacheTraceWriter() = default;

  // Pass Slice references to avoid copy.
  virtual Status WriteBlockAccess(const BlockCacheTraceRecord& record,
                                  const Slice& block_key, const Slice& cf_name,
                                  const Slice& referenced_key) = 0;

  // Write a trace header at the beginning, typically on initiating a trace,
  // with some metadata like a magic number and RocksDB version.
  virtual Status WriteHeader() = 0;
};

// Allocate an instance of the built-in BlockCacheTraceWriter implementation,
// that traces all block cache accesses to a user-provided TraceWriter. Each
// access is traced to a file with a timestamp and type, followed by the
// payload.
std::unique_ptr<BlockCacheTraceWriter> NewBlockCacheTraceWriter(
    SystemClock* clock, const BlockCacheTraceWriterOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer);

}  // namespace ROCKSDB_NAMESPACE
