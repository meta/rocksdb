//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <memory>
#include <string>

#include "db/blob/blob_log_format.h"
#include "rocksdb/slice.h"

#define MAX_HEADER_SIZE(a, b, c) (a > b ? (a > c ? a : c) : (b > c ? b : c))

namespace ROCKSDB_NAMESPACE {

class RandomAccessFileReader;
class Env;
class Statistics;
class Status;
class SystemClock;

/**
 * BlobLogSequentialReader is a general purpose log stream reader
 * implementation. The actual job of reading from the device is implemented by
 * the RandomAccessFileReader interface.
 *
 * Please see BlobLogWriter for details on the file and record layout.
 */

class BlobLogSequentialReader {
 public:
  enum ReadLevel {
    kReadHeader,
    kReadHeaderKey,
    kReadHeaderKeyBlob,
  };

  // Create a reader that will return log records from "*file_reader".
  BlobLogSequentialReader(std::unique_ptr<RandomAccessFileReader>&& file_reader,
                          SystemClock* clock, Statistics* statistics);

  // No copying allowed
  BlobLogSequentialReader(const BlobLogSequentialReader&) = delete;
  BlobLogSequentialReader& operator=(const BlobLogSequentialReader&) = delete;
  BlobLogSequentialReader(BlobLogSequentialReader&&) = delete;
  BlobLogSequentialReader& operator=(BlobLogSequentialReader&&) = delete;

  ~BlobLogSequentialReader();

  Status ReadHeader(BlobLogHeader* header);

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input. The contents filled in
  // *record will only be valid until the next mutating operation on this
  // reader.
  // If blob_offset is non-null, return offset of the blob through it.
  Status ReadRecord(BlobLogRecord* record, ReadLevel level = kReadHeader,
                    uint64_t* blob_offset = nullptr);

  // Reads one record. The record's key and value must end at or before file
  // offset read_limit, or the read returns Corruption. It checks the record
  // sizes against read_limit before it allocates buffers.
  Status ReadRecord(BlobLogRecord* record, ReadLevel level,
                    uint64_t* blob_offset, uint64_t read_limit);

  // ReadRecord() in two steps: ReadRecordHeader() reads and checks the record
  // header, and ReadRecordPayload() reads or skips the key and value. So repair
  // can skip the payloads it does not need without allocating them.
  Status ReadRecordHeader(BlobLogRecord* record, uint64_t* blob_offset,
                          uint64_t read_limit);
  Status ReadRecordPayload(BlobLogRecord* record, ReadLevel level);

  // Read the key and value through a fixed-size buffer and check the record
  // checksum. ReadAndVerifyRecordPayload(record, expected_key) also checks that
  // the key equals `expected_key`. ReadAndVerifyRecordPayloadAndGetKey() also
  // returns the key in `*key`.
  Status ReadAndVerifyRecordPayload(const BlobLogRecord& record);
  Status ReadAndVerifyRecordPayload(const BlobLogRecord& record,
                                    const Slice& expected_key);
  Status ReadAndVerifyRecordPayloadAndGetKey(const BlobLogRecord& record,
                                             std::string* key);
  // Same as above, and also returns the crc32c of the value bytes alone in
  // `*value_checksum`. `key` may be null.
  Status ReadAndVerifyRecordPayloadAndGetValueChecksum(
      const BlobLogRecord& record, std::string* key, uint32_t* value_checksum);

  Status ReadFooter(BlobLogFooter* footer);

  void ResetNextByte() { next_byte_ = 0; }

  uint64_t GetNextByte() const { return next_byte_; }

 private:
  Status ReadSlice(uint64_t size, Slice* slice, char* buf);
  Status ReadAndVerifyRecordPayloadImpl(const BlobLogRecord& record,
                                        const Slice* expected_key,
                                        std::string* copied_key,
                                        uint32_t* value_checksum = nullptr);

  const std::unique_ptr<RandomAccessFileReader> file_;
  SystemClock* clock_;

  Statistics* statistics_;

  Slice buffer_;
  char header_buf_[MAX_HEADER_SIZE(BlobLogHeader::kSize, BlobLogFooter::kSize,
                                   BlobLogRecord::kHeaderSize)];

  // which byte to read next
  uint64_t next_byte_;
};

}  // namespace ROCKSDB_NAMESPACE

#undef MAX_HEADER_SIZE
