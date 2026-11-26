//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "db/blob/blob_log_format.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;
class SystemClock;
/**
 * BlobLogWriter is the blob log stream writer. It provides an append-only
 * abstraction for writing blob data.
 *
 *
 * Look at blob_db_format.h to see the details of the record formats.
 */

class BlobLogWriter {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this BlobLogWriter is in use.
  BlobLogWriter(std::unique_ptr<WritableFileWriter>&& dest, SystemClock* clock,
                Statistics* statistics, uint64_t log_number, bool use_fsync,
                bool do_flush, uint64_t boffset = 0);
  // No copying allowed
  BlobLogWriter(const BlobLogWriter&) = delete;
  BlobLogWriter& operator=(const BlobLogWriter&) = delete;
  BlobLogWriter(BlobLogWriter&&) = delete;
  BlobLogWriter& operator=(BlobLogWriter&&) = delete;

  ~BlobLogWriter();

  // The base signature: ConstructBlobHeader(..., /*value_crc32c=*/nullptr).
  static void ConstructBlobHeader(std::string* buf, const Slice& key,
                                  const Slice& val, uint64_t expiration);
  // `value_crc32c`, if not null, is the CRC32C of `val` (see
  // BlobLogRecord::EncodeHeaderTo()).
  static void ConstructBlobHeader(std::string* buf, const Slice& key,
                                  const Slice& val, uint64_t expiration,
                                  const uint32_t* value_crc32c);

  Status AddRecord(const WriteOptions& write_options, const Slice& key,
                   const Slice& val, uint64_t* key_offset,
                   uint64_t* blob_offset);

  Status AddRecord(const WriteOptions& write_options, const Slice& key,
                   const Slice& val, uint64_t expiration, uint64_t* key_offset,
                   uint64_t* blob_offset);

  // Same as AddRecord() without expiration, but `value_crc32c` is the CRC32C
  // of `val`, already computed by the caller, so `val` is not read again for
  // the record CRC.
  Status AddRecordWithValueCrc(const WriteOptions& write_options,
                               const Slice& key, const Slice& val,
                               uint32_t value_crc32c, uint64_t* key_offset,
                               uint64_t* blob_offset);

  Status EmitPhysicalRecord(const WriteOptions& write_options,
                            const std::string& headerbuf, const Slice& key,
                            const Slice& val, uint64_t* key_offset,
                            uint64_t* blob_offset);

  Status AppendFooter(const WriteOptions& write_options, BlobLogFooter& footer,
                      std::string* checksum_method,
                      std::string* checksum_value);

  Status WriteHeader(const WriteOptions& write_options, BlobLogHeader& header);

  WritableFileWriter* file() { return dest_.get(); }

  const WritableFileWriter* file() const { return dest_.get(); }

  uint64_t get_log_number() const { return log_number_; }

  uint64_t get_file_size() const { return block_offset_; }

  Status Sync(const WriteOptions& write_options);

 private:
  std::unique_ptr<WritableFileWriter> dest_;
  SystemClock* clock_;
  Statistics* statistics_;
  uint64_t log_number_;
  uint64_t block_offset_;  // Current offset in block
  bool use_fsync_;
  bool do_flush_;

 public:
  enum ElemType { kEtNone, kEtFileHdr, kEtRecord, kEtFileFooter };
  ElemType last_elem_type_;
};

}  // namespace ROCKSDB_NAMESPACE
