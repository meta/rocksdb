//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE::log {

Writer::Writer(std::unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
               bool recycle_log_files, bool manual_flush,
               CompressionType compression_type, bool track_and_verify_wals,
               size_t initial_block_offset)
    : dest_(std::move(dest)),
      block_offset_(initial_block_offset),
      log_number_(log_number),
      recycle_log_files_(recycle_log_files),
      // Header size varies depending on whether we are recycling or not.
      header_size_(recycle_log_files ? kRecyclableHeaderSize : kHeaderSize),
      manual_flush_(manual_flush),
      compression_type_(compression_type),
      compress_(),
      track_and_verify_wals_(track_and_verify_wals),
      last_seqno_recorded_(0) {
  for (uint8_t i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
  ThreadStatus::OperationType cur_op_type =
      ThreadStatusUtil::GetThreadOperation();
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);
  if (dest_) {
    WriteBuffer(WriteOptions()).PermitUncheckedError();
  }
  ThreadStatusUtil::SetThreadOperation(cur_op_type);
}

IOStatus Writer::WriteBuffer(const WriteOptions& write_options) {
  IOStatus s = MaybeHandleSeenFileWriterError();
  if (!s.ok()) {
    return s;
  }
  IOOptions opts;
  s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }
  return dest_->Flush(opts);
}

IOStatus Writer::Close(const WriteOptions& write_options) {
  IOStatus s;
  IOOptions opts;
  s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (s.ok() && dest_) {
    s = dest_->Close(opts);
    dest_.reset();
  }
  return s;
}

bool Writer::PublishIfClosed() {
  if (dest_->IsClosed()) {
    dest_.reset();
    return true;
  } else {
    return false;
  }
}

IOStatus Writer::AddRecord(const WriteOptions& write_options,
                           const Slice& slice, const SequenceNumber& seqno) {
  IOStatus s = MaybeHandleSeenFileWriterError();
  if (!s.ok()) {
    return s;
  }
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  bool begin = true;
  int compress_remaining = 0;
  bool compress_start = false;
  if (compress_) {
    compress_->Reset();
    compress_start = true;
  }

  IOOptions opts;
  s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (s.ok()) {
    do {
      const int64_t leftover = kBlockSize - block_offset_;
      assert(leftover >= 0);
      if (leftover < header_size_) {
        // Switch to a new block
        if (leftover > 0) {
          // Fill the trailer (literal below relies on kHeaderSize and
          // kRecyclableHeaderSize being <= 11)
          assert(header_size_ <= 11);
          s = dest_->Append(Slice("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                                  static_cast<size_t>(leftover)),
                            opts);
          if (!s.ok()) {
            break;
          }
        }
        block_offset_ = 0;
      }

      // Invariant: we never leave < header_size bytes in a block.
      assert(static_cast<int64_t>(kBlockSize - block_offset_) >= header_size_);

      const size_t avail = kBlockSize - block_offset_ - header_size_;

      // Compress the record if compression is enabled.
      // Compress() is called at least once (compress_start=true) and after the
      // previous generated compressed chunk is written out as one or more
      // physical records (left=0).
      if (compress_ && (compress_start || left == 0)) {
        compress_remaining = compress_->Compress(
            slice.data(), slice.size(), compressed_buffer_.get(), &left);

        if (compress_remaining < 0) {
          // Set failure status
          s = IOStatus::IOError("Unexpected WAL compression error");
          s.SetDataLoss(true);
          break;
        } else if (left == 0) {
          // Nothing left to compress
          if (!compress_start) {
            break;
          }
        }
        compress_start = false;
        ptr = compressed_buffer_.get();
      }

      const size_t fragment_length = (left < avail) ? left : avail;

      RecordType type;
      const bool end = (left == fragment_length && compress_remaining == 0);
      if (begin && end) {
        type = recycle_log_files_ ? kRecyclableFullType : kFullType;
      } else if (begin) {
        type = recycle_log_files_ ? kRecyclableFirstType : kFirstType;
      } else if (end) {
        type = recycle_log_files_ ? kRecyclableLastType : kLastType;
      } else {
        type = recycle_log_files_ ? kRecyclableMiddleType : kMiddleType;
      }

      s = EmitPhysicalRecord(opts, type, ptr, fragment_length);
      ptr += fragment_length;
      left -= fragment_length;
      begin = false;
    } while (s.ok() && (left > 0 || compress_remaining > 0));
  }
  if (s.ok()) {
    if (!manual_flush_) {
      s = dest_->Flush(opts);
    }
  }

  if (s.ok()) {
    last_seqno_recorded_ = std::max(last_seqno_recorded_, seqno);
  }

  return s;
}

IOStatus Writer::AddCompressionTypeRecord(const WriteOptions& write_options) {
  // Should be the first record
  assert(block_offset_ == 0);

  if (compression_type_ == kNoCompression) {
    // No need to add a record
    return IOStatus::OK();
  }

  IOStatus s = MaybeHandleSeenFileWriterError();
  if (!s.ok()) {
    return s;
  }

  CompressionTypeRecord record(compression_type_);
  std::string encode;
  record.EncodeTo(&encode);
  s = EmitPhysicalRecord(write_options, kSetCompressionType, encode.data(),
                         encode.size());
  if (s.ok()) {
    if (!manual_flush_) {
      IOOptions io_opts;
      s = WritableFileWriter::PrepareIOOptions(write_options, io_opts);
      if (s.ok()) {
        s = dest_->Flush(io_opts);
      }
    }
    // Initialize fields required for compression
    const size_t max_output_buffer_len = kBlockSize - header_size_;
    CompressionOptions opts;
    constexpr uint32_t compression_format_version = 2;
    compress_ = StreamingCompress::Create(compression_type_, opts,
                                          compression_format_version,
                                          max_output_buffer_len);
    assert(compress_ != nullptr);
    compressed_buffer_ =
        std::unique_ptr<char[]>(new char[max_output_buffer_len]);
    assert(compressed_buffer_);
  } else {
    // Disable compression if the record could not be added.
    compression_type_ = kNoCompression;
  }
  return s;
}

IOStatus Writer::MaybeAddPredecessorWALInfo(const WriteOptions& write_options,
                                            const PredecessorWALInfo& info) {
  IOStatus s = MaybeHandleSeenFileWriterError();

  if (!s.ok()) {
    return s;
  }

  if (!track_and_verify_wals_ || !info.IsInitialized()) {
    return IOStatus::OK();
  }

  std::string encode;
  info.EncodeTo(&encode);

  s = MaybeSwitchToNewBlock(write_options, encode);
  if (!s.ok()) {
    return s;
  }

  RecordType type = recycle_log_files_ ? kRecyclePredecessorWALInfoType
                                       : kPredecessorWALInfoType;
  s = EmitPhysicalRecord(write_options, type, encode.data(), encode.size());

  if (!s.ok()) {
    return s;
  }

  if (!manual_flush_) {
    IOOptions io_opts;
    s = WritableFileWriter::PrepareIOOptions(write_options, io_opts);
    if (s.ok()) {
      s = dest_->Flush(io_opts);
    }
  }
  return s;
}

IOStatus Writer::MaybeAddUserDefinedTimestampSizeRecord(
    const WriteOptions& write_options,
    const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz) {
  std::vector<std::pair<uint32_t, size_t>> ts_sz_to_record;
  for (const auto& [cf_id, ts_sz] : cf_to_ts_sz) {
    if (recorded_cf_to_ts_sz_.count(cf_id) != 0) {
      // A column family's user-defined timestamp size should not be
      // updated while DB is running.
      assert(recorded_cf_to_ts_sz_[cf_id] == ts_sz);
    } else if (ts_sz != 0) {
      ts_sz_to_record.emplace_back(cf_id, ts_sz);
      recorded_cf_to_ts_sz_.insert(std::make_pair(cf_id, ts_sz));
    }
  }
  if (ts_sz_to_record.empty()) {
    return IOStatus::OK();
  }

  UserDefinedTimestampSizeRecord record(std::move(ts_sz_to_record));
  std::string encoded;
  record.EncodeTo(&encoded);
  RecordType type = recycle_log_files_ ? kRecyclableUserDefinedTimestampSizeType
                                       : kUserDefinedTimestampSizeType;

  IOStatus s = MaybeSwitchToNewBlock(write_options, encoded);
  if (!s.ok()) {
    return s;
  }

  return EmitPhysicalRecord(write_options, type, encoded.data(),
                            encoded.size());
}

bool Writer::BufferIsEmpty() { return dest_->BufferIsEmpty(); }

IOStatus Writer::EmitPhysicalRecord(const WriteOptions& write_options,
                                    RecordType t, const char* ptr, size_t n) {
  IOOptions opts;
  IOStatus s = WritableFileWriter::PrepareIOOptions(write_options, opts);
  if (!s.ok()) {
    return s;
  }
  return EmitPhysicalRecord(opts, t, ptr, n);
}

IOStatus Writer::EmitPhysicalRecord(const IOOptions& opts, RecordType t,
                                    const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes

  size_t header_size;
  char buf[kRecyclableHeaderSize];

  // Format the header
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  uint32_t crc = type_crc_[t];
  if (t < kRecyclableFullType || t == kSetCompressionType ||
      t == kPredecessorWALInfoType || t == kUserDefinedTimestampSizeType) {
    // Legacy record format
    assert(block_offset_ + kHeaderSize + n <= kBlockSize);
    header_size = kHeaderSize;
  } else {
    // Recyclable record format
    assert(block_offset_ + kRecyclableHeaderSize + n <= kBlockSize);
    header_size = kRecyclableHeaderSize;

    // Only encode low 32-bits of the 64-bit log number.  This means
    // we will fail to detect an old record if we recycled a log from
    // ~4 billion logs ago, but that is effectively impossible, and
    // even if it were we'dbe far more likely to see a false positive
    // on the 32-bit CRC.
    EncodeFixed32(buf + 7, static_cast<uint32_t>(log_number_));
    crc = crc32c::Extend(crc, buf + 7, 4);
  }

  // Compute the crc of the record type and the payload.
  uint32_t payload_crc = crc32c::Value(ptr, n);
  crc = crc32c::Crc32cCombine(crc, payload_crc, n);
  crc = crc32c::Mask(crc);  // Adjust for storage
  TEST_SYNC_POINT_CALLBACK("LogWriter::EmitPhysicalRecord:BeforeEncodeChecksum",
                           &crc);
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  IOStatus s = dest_->Append(Slice(buf, header_size), opts);
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, n), opts, Crc32cChecksum(payload_crc));
  }
  block_offset_ += header_size + n;
  return s;
}

IOStatus Writer::MaybeHandleSeenFileWriterError() {
  if (dest_->seen_error()) {
#ifndef NDEBUG
    if (dest_->seen_injected_error()) {
      std::stringstream msg;
      msg << "Seen " << FaultInjectionTestFS::kInjected
          << " error. Skip writing buffer.";
      return IOStatus::IOError(msg.str());
    }
#endif  // NDEBUG
    return IOStatus::IOError("Seen error. Skip writing buffer.");
  }
  return IOStatus::OK();
}

IOStatus Writer::MaybeSwitchToNewBlock(const WriteOptions& write_options,
                                       const std::string& content_to_write) {
  IOStatus s;
  const int64_t leftover = kBlockSize - block_offset_;
  // If there's not enough space for this record, switch to a new block.
  if (leftover < header_size_ + (int)content_to_write.size()) {
    IOOptions opts;
    s = WritableFileWriter::PrepareIOOptions(write_options, opts);
    if (!s.ok()) {
      return s;
    }

    std::vector<char> trailer(leftover, '\x00');
    s = dest_->Append(Slice(trailer.data(), trailer.size()), opts);
    if (!s.ok()) {
      return s;
    }

    block_offset_ = 0;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE::log
