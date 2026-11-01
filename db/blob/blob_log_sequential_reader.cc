//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "db/blob/blob_log_sequential_reader.h"

#include <algorithm>
#include <array>
#include <limits>

#include "file/random_access_file_reader.h"
#include "monitoring/statistics_impl.h"
#include "util/crc32c.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

BlobLogSequentialReader::BlobLogSequentialReader(
    std::unique_ptr<RandomAccessFileReader>&& file_reader, SystemClock* clock,
    Statistics* statistics)
    : file_(std::move(file_reader)),
      clock_(clock),
      statistics_(statistics),
      next_byte_(0) {}

BlobLogSequentialReader::~BlobLogSequentialReader() = default;

Status BlobLogSequentialReader::ReadSlice(uint64_t size, Slice* slice,
                                          char* buf) {
  assert(slice);
  assert(file_);

  StopWatch read_sw(clock_, statistics_, BLOB_DB_BLOB_FILE_READ_MICROS);
  // TODO: rate limit `BlobLogSequentialReader` reads (it appears unused?)
  Status s = file_->Read(next_byte_, static_cast<size_t>(size), IOOptions(),
                         slice, buf, nullptr);
  next_byte_ += size;
  if (!s.ok()) {
    return s;
  }
  RecordTick(statistics_, BLOB_DB_BLOB_FILE_BYTES_READ, slice->size());
  if (slice->size() != size) {
    return Status::Corruption("EOF reached while reading record");
  }
  return s;
}

Status BlobLogSequentialReader::ReadHeader(BlobLogHeader* header) {
  assert(header);
  assert(next_byte_ == 0);

  static_assert(BlobLogHeader::kSize <= sizeof(header_buf_),
                "Buffer is smaller than BlobLogHeader::kSize");

  Status s = ReadSlice(BlobLogHeader::kSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }

  if (buffer_.size() != BlobLogHeader::kSize) {
    return Status::Corruption("EOF reached before file header");
  }

  return header->DecodeFrom(buffer_);
}

Status BlobLogSequentialReader::ReadRecord(BlobLogRecord* record,
                                           ReadLevel level,
                                           uint64_t* blob_offset) {
  return ReadRecord(record, level, blob_offset,
                    std::numeric_limits<uint64_t>::max());
}

Status BlobLogSequentialReader::ReadRecord(BlobLogRecord* record,
                                           ReadLevel level,
                                           uint64_t* blob_offset,
                                           uint64_t read_limit) {
  Status status = ReadRecordHeader(record, blob_offset, read_limit);
  if (!status.ok()) {
    return status;
  }
  return ReadRecordPayload(record, level);
}

Status BlobLogSequentialReader::ReadRecordHeader(BlobLogRecord* record,
                                                 uint64_t* blob_offset,
                                                 uint64_t read_limit) {
  assert(record);
  static_assert(BlobLogRecord::kHeaderSize <= sizeof(header_buf_),
                "Buffer is smaller than BlobLogRecord::kHeaderSize");

  Status s = ReadSlice(BlobLogRecord::kHeaderSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }
  if (buffer_.size() != BlobLogRecord::kHeaderSize) {
    return Status::Corruption("EOF reached before record header");
  }

  s = record->DecodeHeaderFrom(buffer_);
  if (!s.ok()) {
    return s;
  }

  if (next_byte_ > read_limit || record->key_size > read_limit - next_byte_ ||
      record->value_size > read_limit - next_byte_ - record->key_size ||
      record->key_size > std::numeric_limits<size_t>::max() ||
      record->value_size > std::numeric_limits<size_t>::max()) {
    return Status::Corruption("Blob record exceeds the read limit");
  }
  if (blob_offset != nullptr) {
    *blob_offset = next_byte_ + record->key_size;
  }
  return Status::OK();
}

Status BlobLogSequentialReader::ReadRecordPayload(BlobLogRecord* record,
                                                  ReadLevel level) {
  assert(record != nullptr);
  const uint64_t kb_size = record->key_size + record->value_size;
  Status s;
  switch (level) {
    case kReadHeader:
      next_byte_ += kb_size;
      break;

    case kReadHeaderKey:
      record->key_buf.reset(new char[record->key_size]);
      s = ReadSlice(record->key_size, &record->key, record->key_buf.get());
      next_byte_ += record->value_size;
      break;

    case kReadHeaderKeyBlob:
      record->key_buf.reset(new char[record->key_size]);
      s = ReadSlice(record->key_size, &record->key, record->key_buf.get());
      if (s.ok()) {
        record->value_buf.reset(new char[record->value_size]);
        s = ReadSlice(record->value_size, &record->value,
                      record->value_buf.get());
      }
      if (s.ok()) {
        s = record->CheckBlobCRC();
      }
      break;
  }
  return s;
}

Status BlobLogSequentialReader::ReadAndVerifyRecordPayload(
    const BlobLogRecord& record, const Slice& expected_key) {
  return ReadAndVerifyRecordPayloadImpl(record, &expected_key, nullptr);
}

Status BlobLogSequentialReader::ReadAndVerifyRecordPayload(
    const BlobLogRecord& record) {
  return ReadAndVerifyRecordPayloadImpl(record, /*expected_key=*/nullptr,
                                        nullptr);
}

Status BlobLogSequentialReader::ReadAndVerifyRecordPayloadAndGetKey(
    const BlobLogRecord& record, std::string* key) {
  if (key == nullptr) {
    return Status::InvalidArgument("Missing copied blob record key");
  }
  key->clear();
  return ReadAndVerifyRecordPayloadImpl(record, nullptr, key);
}

Status BlobLogSequentialReader::ReadAndVerifyRecordPayloadImpl(
    const BlobLogRecord& record, const Slice* expected_key,
    std::string* copied_key) {
  if (expected_key != nullptr && record.key_size != expected_key->size()) {
    return Status::Corruption("Blob record key does not match SST reference");
  }

  constexpr size_t kBufferSize = 8192;
  std::array<char, kBufferSize> scratch{};
  uint32_t checksum = 0;
  uint64_t bytes_read = 0;
  while (bytes_read < record.key_size) {
    const size_t bytes = static_cast<size_t>(
        std::min<uint64_t>(record.key_size - bytes_read, scratch.size()));
    Slice fragment;
    Status status = ReadSlice(bytes, &fragment, scratch.data());
    if (!status.ok()) {
      return status;
    }
    if (expected_key != nullptr &&
        fragment != Slice(expected_key->data() + bytes_read, bytes)) {
      return Status::Corruption("Blob record key does not match SST reference");
    }
    if (copied_key != nullptr) {
      copied_key->append(fragment.data(), fragment.size());
    }
    checksum = crc32c::Extend(checksum, fragment.data(), fragment.size());
    bytes_read += bytes;
  }
  bytes_read = 0;
  while (bytes_read < record.value_size) {
    const size_t bytes = static_cast<size_t>(
        std::min<uint64_t>(record.value_size - bytes_read, scratch.size()));
    Slice fragment;
    Status status = ReadSlice(bytes, &fragment, scratch.data());
    if (!status.ok()) {
      return status;
    }
    checksum = crc32c::Extend(checksum, fragment.data(), fragment.size());
    bytes_read += bytes;
  }
  if (crc32c::Mask(checksum) != record.blob_crc) {
    return Status::Corruption("Blob CRC mismatch");
  }
  return Status::OK();
}

Status BlobLogSequentialReader::ReadFooter(BlobLogFooter* footer) {
  assert(footer);
  static_assert(BlobLogFooter::kSize <= sizeof(header_buf_),
                "Buffer is smaller than BlobLogFooter::kSize");

  Status s = ReadSlice(BlobLogFooter::kSize, &buffer_, header_buf_);
  if (!s.ok()) {
    return s;
  }

  if (buffer_.size() != BlobLogFooter::kSize) {
    return Status::Corruption("EOF reached before file footer");
  }

  return footer->DecodeFrom(buffer_);
}

}  // namespace ROCKSDB_NAMESPACE
