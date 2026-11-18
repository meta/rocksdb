//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "db/blob/blob_log_sequential_reader.h"

#include "file/random_access_file_reader.h"
#include "monitoring/statistics_impl.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

BlobLogSequentialReader::BlobLogSequentialReader(
    std::unique_ptr<RandomAccessFileReader>&& file_reader, SystemClock* clock,
    Statistics* statistics)
    : file_(std::move(file_reader)),
      clock_(clock),
      statistics_(statistics),
      header_buf_(),
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

  uint64_t kb_size = record->key_size + record->value_size;
  if (blob_offset != nullptr) {
    *blob_offset = next_byte_ + record->key_size;
  }

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
