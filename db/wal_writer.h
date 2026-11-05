//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>

#include "rocksdb/io_status.h"
#include "rocksdb/types.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class PredecessorWALInfo;
class Slice;
class WritableFileWriter;
struct WriteOptions;

// Interface for writing RocksDB WAL records and managing the underlying file.
// Implementations are responsible for their file format's record framing and
// metadata records.
class WalWriter {
 public:
  virtual ~WalWriter() = default;

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  IOStatus AddRecord(const WriteOptions& write_options, const Slice& record) {
    return AddRecord(write_options, record, 0);
  }
  virtual IOStatus AddRecord(const WriteOptions& write_options,
                             const Slice& record, SequenceNumber sequence) = 0;
  virtual IOStatus AddCompressionTypeRecord(
      const WriteOptions& write_options) = 0;
  virtual IOStatus MaybeAddPredecessorWALInfo(
      const WriteOptions& write_options,
      const PredecessorWALInfo& predecessor_wal_info) = 0;
  virtual IOStatus MaybeAddUserDefinedTimestampSizeRecord(
      const WriteOptions& write_options,
      const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz) = 0;

  virtual WritableFileWriter* file() = 0;
  virtual const WritableFileWriter* file() const = 0;
  virtual uint64_t get_log_number() const = 0;

  virtual IOStatus WriteBuffer(const WriteOptions& write_options) = 0;
  virtual IOStatus Close(const WriteOptions& write_options) = 0;
  virtual bool PublishIfClosed() = 0;
  virtual bool BufferIsEmpty() = 0;
  virtual SequenceNumber GetLastSeqnoRecorded() const = 0;

 protected:
  WalWriter() = default;
};

}  // namespace ROCKSDB_NAMESPACE
