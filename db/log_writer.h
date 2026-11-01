//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "db/log_format.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/compression.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriter;

namespace log {

/**
 * Writer is a general purpose log stream writer. It provides an append-only
 * abstraction for writing data. The details of the how the data is written is
 * handled by the WritableFile sub-class implementation.
 *
 * File format:
 *
 * File is broken down into variable sized records. The format of each record
 * is described below.
 *       +-----+-------------+--+----+----------+------+-- ... ----+
 * File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
 *       +-----+-------------+--+----+----------+------+-- ... ----+
 *       <--- kBlockSize ------>|<-- kBlockSize ------>|
 *  rn = variable size records
 *  P = Padding
 *
 * Data is written out in kBlockSize chunks. If next record does not fit
 * into the space left, the leftover space will be padded with \0.
 *
 * Legacy record format:
 *
 * +---------+-----------+-----------+--- ... ---+
 * |CRC (4B) | Size (2B) | Type (1B) | Payload   |
 * +---------+-----------+-----------+--- ... ---+
 *
 * CRC = 32bit hash computed over the record type and payload using CRC
 * Size = Length of the payload data
 * Type = Type of record
 *        (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
 *        The type is used to group a bunch of records together to represent
 *        blocks that are larger than kBlockSize
 * Payload = Byte stream as long as specified by the payload size
 *
 * Recyclable record format:
 *
 * +---------+-----------+-----------+----------------+--- ... ---+
 * |CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
 * +---------+-----------+-----------+----------------+--- ... ---+
 *
 * Same as above, with the addition of
 * Log number = 32bit log file number, so that we can distinguish between
 * records written by the most recent log writer vs a previous one.
 */
class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must remain live while this Writer is in use. By default
  // "*dest" is expected to be empty (initial_block_offset = 0). When
  // resuming append into an existing log file (e.g.
  // VersionSet::ReopenManifestForAppend), pass the offset within the
  // current 32 KiB block at which writes should resume so that record
  // framing aligns to the existing block layout.
  // TODO(hx235): separate WAL related parameters from general `Reader`
  // parameters
  explicit Writer(std::unique_ptr<WritableFileWriter>&& dest,
                  uint64_t log_number, bool recycle_log_files,
                  bool manual_flush = false,
                  CompressionType compressionType = kNoCompression,
                  bool track_and_verify_wals = false,
                  size_t initial_block_offset = 0);
  // No copying allowed
  Writer(const Writer&) = delete;
  void operator=(const Writer&) = delete;
  Writer(Writer&&) = delete;
  void operator=(Writer&&) = delete;

  ~Writer();

  IOStatus AddRecord(const WriteOptions& write_options, const Slice& slice,
                     const SequenceNumber& seqno = 0);
  IOStatus AddCompressionTypeRecord(const WriteOptions& write_options);
  IOStatus MaybeAddPredecessorWALInfo(const WriteOptions& write_options,
                                      const PredecessorWALInfo& info);

  // If there are column families in `cf_to_ts_sz` not included in
  // `recorded_cf_to_ts_sz_` and its user-defined timestamp size is non-zero,
  // adds a record of type kUserDefinedTimestampSizeType or
  // kRecyclableUserDefinedTimestampSizeType for these column families.
  // This timestamp size record applies to all subsequent records.
  IOStatus MaybeAddUserDefinedTimestampSizeRecord(
      const WriteOptions& write_options,
      const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz);

  WritableFileWriter* file() { return dest_.get(); }
  const WritableFileWriter* file() const { return dest_.get(); }

  uint64_t get_log_number() const { return log_number_; }

  IOStatus WriteBuffer(const WriteOptions& write_options);

  IOStatus Close(const WriteOptions& write_options);

  // If closing the writer through file(), call this afterwards to modify
  // this object's state to reflect that. Returns true if the destination file
  // has been closed. If it hasn't been closed, returns false with no change.
  bool PublishIfClosed();

  bool BufferIsEmpty();

  size_t TEST_block_offset() const { return block_offset_; }

  SequenceNumber GetLastSeqnoRecorded() const { return last_seqno_recorded_; }

 private:
  std::unique_ptr<WritableFileWriter> dest_;
  size_t block_offset_;  // Current offset in block
  uint64_t log_number_;
  bool recycle_log_files_;
  int header_size_;

  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  IOStatus EmitPhysicalRecord(const WriteOptions& write_options,
                              RecordType type, const char* ptr, size_t length);

  IOStatus MaybeHandleSeenFileWriterError();

  IOStatus MaybeSwitchToNewBlock(const WriteOptions& write_options,
                                 const std::string& content_to_write);

  // If true, it does not flush after each write. Instead it relies on the upper
  // layer to manually does the flush by calling ::WriteBuffer()
  bool manual_flush_;

  // Compression Type
  CompressionType compression_type_;
  std::unique_ptr<StreamingCompress> compress_;
  // Reusable compressed output buffer
  std::unique_ptr<char[]> compressed_buffer_;

  // The recorded user-defined timestamp size that have been written so far.
  // Since the user-defined timestamp size cannot be changed while the DB is
  // running, existing entry in this map cannot be updated.
  UnorderedMap<uint32_t, size_t> recorded_cf_to_ts_sz_;

  // See `Options::track_and_verify_wals`
  bool track_and_verify_wals_;

  SequenceNumber last_seqno_recorded_;
};

}  // namespace log
}  // namespace ROCKSDB_NAMESPACE
