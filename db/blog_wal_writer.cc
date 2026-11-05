//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog_wal_writer.h"

#include <algorithm>
#include <cassert>
#include <utility>

#include "db/blog/blog_writer.h"
#include "db/dbformat.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/udt_util.h"

namespace ROCKSDB_NAMESPACE {

// Minimum uncompressed WriteBatch size eligible for direct streaming into an
// unspecified-size record. Smaller records stay on the buffered path
// (compress into a string, emit a sized record, usually compact): below one
// output chunk streaming saves no copy, while the unspecified-size framing
// costs about 14 bytes per record plus scan-based reads. 16 KiB is the
// compact-record payload limit, so buffered records usually land in compact
// records (an incompressible input just under the line lands in a sized full
// record instead, still cheaper than unspecified-size). Both paths feed the
// same open ZSTD frame, so the stream decodes to the same bytes either way;
// only the framing differs, and it is decided before compression starts.
constexpr size_t kStreamingDirectMinUncompressedBytes = 16 * 1024;

// StreamingCompressOutput that starts an unspecified-size record on the
// first nonempty compressed chunk. Keeps the first IOStatus so the caller can
// return it with full fidelity; the Status relayed through the compressor
// only signals whether to keep going.
struct UnspecifiedSizeChunkSink : public StreamingCompressOutput {
  UnspecifiedSizeChunkSink(BlogFileWriter* writer, const WriteOptions& wo)
      : writer_(writer), write_options_(wo) {}

  Status Append(const char* data, size_t len) override {
    if (!io_status_.ok()) {
      return io_status_;
    }
    if (len == 0) {
      return Status::OK();
    }
    if (!prefix_written_) {
      io_status_ = writer_->BeginUnspecifiedSizeRecord(
          write_options_, kBlogWriteBatchRecord, kStreamingCompressionSentinel);
      if (!io_status_.ok()) {
        return io_status_;
      }
      prefix_written_ = true;
    }
    io_status_ =
        writer_->AppendUnspecifiedSizeChunk(write_options_, Slice(data, len));
    if (io_status_.ok()) {
      return Status::OK();
    }
    return io_status_;
  }

  BlogFileWriter* writer_;
  const WriteOptions& write_options_;
  IOStatus io_status_;
  bool prefix_written_ = false;
};

BlogWalWriter::BlogWalWriter(
    std::unique_ptr<WritableFileWriter>&& dest, uint64_t log_number,
    bool is_recycled, bool manual_flush, CompressionType compression_type,
    bool use_streaming_compression, bool track_and_verify_wals,
    ChecksumType checksum_type, size_t blog_timestamp_size,
    const std::string& db_id, const std::string& db_session_id,
    const std::string& db_host_id, uint64_t creation_time)
    : pending_dest_(std::move(dest)),
      log_number_(log_number),
      manual_flush_(manual_flush),
      compression_type_(compression_type),
      use_streaming_compression_(use_streaming_compression),
      track_and_verify_wals_(track_and_verify_wals) {
  header_.checksum_type = checksum_type;
  header_.compact_record_type = kBlogWriteBatchRecord;
  if (is_recycled) {
    header_.flags |= kBlogFileRecycled;
  }
  header_.GenerateWalFromUniqueId(db_id, db_session_id, log_number);
  header_.SetProperty(kBlogPropRole, "wal");
  header_.SetDiagnosticProperties(db_host_id, BlogFileHeader::kNoCfId, "",
                                  creation_time);
  if (blog_timestamp_size != 0) {
    header_.SetUint64Property(kBlogPropTimestampSize, blog_timestamp_size);
  }

  if (compression_type_ != kNoCompression) {
    std::shared_ptr<CompressionManager> compression_manager =
        GetBuiltinV2CompressionManager();
    header_.SetProperty(kBlogPropCompressionCompatibilityName,
                        compression_manager->CompatibilityName());
    if (use_streaming_compression) {
      // max_output_len only limits Compress(), not CompressAndFlush() or
      // CompressAndFlushToSink().
      streaming_compressor_ = StreamingCompress::Create(
          compression_type_, CompressionOptions(),
          /*compress_format_version=*/2, /*max_output_len=*/0);
      if (streaming_compressor_) {
        const char raw_compression_type =
            lossless_cast<char>(compression_type_);
        header_.SetProperty(
            kBlogPropWriteBatchStreamingCompressionType,
            Slice(&raw_compression_type, 1).ToString(/*hex=*/true));
      }
    } else {
      compressor_ = compression_manager->GetCompressor(CompressionOptions(),
                                                       compression_type_);
      if (compressor_) {
        compressor_working_area_ = compressor_->ObtainWorkingArea();
      }
    }
  }
}

BlogWalWriter::~BlogWalWriter() {
  ThreadStatus::OperationType cur_op_type =
      ThreadStatusUtil::GetThreadOperation();
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OperationType::OP_UNKNOWN);
  if (file()) {
    Close(WriteOptions()).PermitUncheckedError();
  }
  ThreadStatusUtil::SetThreadOperation(cur_op_type);
}

IOStatus BlogWalWriter::EnsureHeaderWritten(const WriteOptions& write_options) {
  if (header_state_ == HeaderState::kWritten) {
    return IOStatus::OK();
  }
  if (header_state_ == HeaderState::kFailed) {
    // A failed header may be partial on disk, so it must not be retried.
    return IOStatus::IOError("Blog WAL file header write failed earlier");
  }
  if (!pending_dest_) {
    return IOStatus::InvalidArgument("Blog WAL writer has no destination");
  }

  // A WAL is replayed as the writing process left it, which after a crash
  // means with nothing after the last record. The reader can still delimit
  // that record at the end of the file -- but only if the end of the file is
  // where the record ends. In a recycled file it is not: the previous
  // incarnation's bytes remain past the logical end, they carry a different
  // escape sequence, and nothing in the file says where the last record
  // stopped. Such a file must keep every record self-delimiting.
  writer_ = std::make_unique<BlogFileWriter>(
      std::move(pending_dest_), header_,
      header_.is_recycled() ? BlogUnspecifiedSizeRecords::kDisallowed
                            : BlogUnspecifiedSizeRecords::kAllowed,
      manual_flush_);
  IOStatus io_s = writer_->WriteHeader(write_options);
  header_state_ = io_s.ok() ? HeaderState::kWritten : HeaderState::kFailed;
  return io_s;
}

IOStatus BlogWalWriter::AddStreamingRecordDirect(
    const WriteOptions& write_options, const Slice& record) {
  assert(streaming_compressor_ != nullptr);
  assert(!record.empty());
  assert(!header_.is_recycled());

  UnspecifiedSizeChunkSink sink(writer_.get(), write_options);
  Status s = streaming_compressor_->CompressAndFlushToSink(record, &sink);
  if (!sink.io_status_.ok()) {
    // A chunk never reached the file, but the stream has advanced past it.
    // `s` only relays the sink failure; the sink's IOStatus is returned with
    // full fidelity instead.
    s.PermitUncheckedError();
    return sink.io_status_;
  }
  if (!s.ok()) {
    return status_to_io_status(std::move(s));
  }
  return writer_->FinishUnspecifiedSizeRecord(write_options);
}

IOStatus BlogWalWriter::AddRecord(const WriteOptions& write_options,
                                  const Slice& record,
                                  SequenceNumber first_sequence,
                                  size_t num_sequences) {
  if (closing_) {
    return IOStatus::InvalidArgument("Blog WAL writer: file is sealed");
  }
  IOStatus io_s = EnsureHeaderWritten(write_options);
  if (!io_s.ok()) {
    return io_s;
  }

  CompressionType actual_compression_type = kNoCompression;
  Slice record_to_write = record;
  bool record_already_written = false;
  if (streaming_compressor_ && !record.empty()) {
    streaming_compression_broken_ |= writer_->HasLostWrites();
    if (!streaming_compression_broken_) {
      if (!header_.is_recycled() &&
          record.size() >= kStreamingDirectMinUncompressedBytes) {
        // Large enough to stream: emit the compressed bytes directly into
        // an unspecified-size record instead of buffering them first.
        io_s = AddStreamingRecordDirect(write_options, record);
        if (!io_s.ok()) {
          streaming_compression_broken_ = true;
          return io_s;
        }
        actual_compression_type = kStreamingCompressionSentinel;
        record_already_written = true;
      } else {
        streaming_compressed_record_.clear();
        Status s = streaming_compressor_->CompressAndFlush(
            record, &streaming_compressed_record_);
        if (!s.ok()) {
          streaming_compression_broken_ = true;
          return status_to_io_status(std::move(s));
        }
        record_to_write = streaming_compressed_record_;
        actual_compression_type = kStreamingCompressionSentinel;
      }
    }
  } else if (compressor_ && !record.empty()) {
    compressed_record_.ResetForSize(record.size());
    Status s = compressor_->CompressBlock(
        record, compressed_record_.data(), &compressed_record_.MutableSize(),
        &actual_compression_type, &compressor_working_area_);
    if (!s.ok()) {
      return status_to_io_status(std::move(s));
    }
    if (actual_compression_type != kNoCompression) {
      record_to_write = Slice(compressed_record_);
    }
  }

  if (!record_already_written) {
    io_s = writer_->AddWriteBatchRecord(write_options, record_to_write,
                                        actual_compression_type);
  }
  if (io_s.ok()) {
    // Keep the max like log::Writer so no false WAL hole is reported.
    last_seqno_recorded_ = std::max(last_seqno_recorded_, first_sequence);
    if (num_sequences > 0) {
      const SequenceNumber last_sequence = first_sequence + num_sequences - 1;
      if (!has_seqno_recorded_) {
        min_seqno_recorded_ = first_sequence;
        max_seqno_recorded_ = last_sequence;
        has_seqno_recorded_ = true;
      } else {
        min_seqno_recorded_ = std::min(min_seqno_recorded_, first_sequence);
        max_seqno_recorded_ = std::max(max_seqno_recorded_, last_sequence);
      }
    }
  } else if (actual_compression_type == kStreamingCompressionSentinel) {
    streaming_compression_broken_ = true;
  }
  return io_s;
}

IOStatus BlogWalWriter::WriteFileStart(
    const WriteOptions& write_options,
    const PredecessorWALInfo& predecessor_wal_info) {
  if (use_streaming_compression_ && !streaming_compressor_) {
    return IOStatus::NotSupported(
        "Unsupported Blog WAL streaming compression type",
        CompressionTypeToString(compression_type_));
  }
  if (!use_streaming_compression_ && compression_type_ != kNoCompression &&
      !compressor_) {
    return IOStatus::NotSupported("Unsupported Blog WAL compression type",
                                  CompressionTypeToString(compression_type_));
  }
  assert(header_state_ == HeaderState::kNotWritten);

  if (track_and_verify_wals_ && predecessor_wal_info.IsInitialized()) {
    std::string encoded;
    predecessor_wal_info.EncodeTo(&encoded);
    header_.SetProperty(kBlogPropPredecessorWalInfo, encoded);
  }
  IOStatus io_s = EnsureHeaderWritten(write_options);
  if (io_s.ok()) {
    // Flush without syncing, so that even with manual_wal_flush a later write
    // failure cannot discard the header from the buffer.
    io_s = writer_->WriteBuffer(write_options);
  }
  return io_s;
}

IOStatus BlogWalWriter::MaybeAddUserDefinedTimestampSizeRecord(
    const WriteOptions& write_options,
    const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz) {
  std::vector<std::pair<uint32_t, size_t>> timestamp_sizes_to_record;
  for (const auto& [cf_id, timestamp_size] : cf_to_ts_sz) {
    UnorderedMap<uint32_t, size_t>::const_iterator existing =
        recorded_cf_to_ts_sz_.find(cf_id);
    if (existing != recorded_cf_to_ts_sz_.end()) {
      assert(existing->second == timestamp_size);
      continue;
    }
    if (timestamp_size == 0) {
      continue;
    }

    timestamp_sizes_to_record.emplace_back(cf_id, timestamp_size);
    recorded_cf_to_ts_sz_.emplace(cf_id, timestamp_size);
  }
  if (timestamp_sizes_to_record.empty()) {
    return IOStatus::OK();
  }

  UserDefinedTimestampSizeRecord timestamp_record(
      std::move(timestamp_sizes_to_record));
  std::string encoded;
  timestamp_record.EncodeTo(&encoded);
  if (closing_) {
    return IOStatus::InvalidArgument("Blog WAL writer: file is sealed");
  }
  IOStatus io_s = EnsureHeaderWritten(write_options);
  if (!io_s.ok()) {
    return io_s;
  }
  return writer_->AddUserDefinedTimestampSizeRecord(write_options, encoded);
  ;
}

WritableFileWriter* BlogWalWriter::file() {
  return writer_ ? writer_->file() : pending_dest_.get();
}

const WritableFileWriter* BlogWalWriter::file() const {
  return writer_ ? writer_->file() : pending_dest_.get();
}

uint64_t BlogWalWriter::get_log_number() const { return log_number_; }

IOStatus BlogWalWriter::WriteBuffer(const WriteOptions& write_options) {
  if (writer_) {
    IOStatus io_s = writer_->WriteBuffer(write_options);
    if (!io_s.ok() && streaming_compressor_) {
      // Buffered streaming records may not have reached the file.
      streaming_compression_broken_ = true;
    }
    return io_s;
  }
  IOOptions io_options;
  IOStatus io_s =
      WritableFileWriter::PrepareIOOptions(write_options, io_options);
  if (io_s.ok() && pending_dest_) {
    io_s = pending_dest_->Flush(io_options);
  }
  return io_s;
}

IOStatus BlogWalWriter::Close(const WriteOptions& write_options) {
  if (PublishIfClosed()) {
    return IOStatus::OK();
  }
  IOOptions io_options;
  IOStatus io_s =
      WritableFileWriter::PrepareIOOptions(write_options, io_options);
  if (!io_s.ok()) {
    return io_s;
  }

  if (writer_) {
    if (!closing_ && header_state_ == HeaderState::kWritten &&
        !file()->seen_error()) {
      closing_ = true;
      BlogFileFooterProperties properties;
      if (has_seqno_recorded_) {
        properties.SetSequenceRange(min_seqno_recorded_, max_seqno_recorded_);
      }
      io_s = writer_->WriteFooter(write_options, properties);
      if (io_s.ok()) {
        io_s = writer_->WriteBuffer(write_options);
      }
      if (io_s.ok()) {
        TEST_SYNC_POINT("BlogWalWriter::Close:BeforeTruncate");
        io_s = file()->writable_file()->Truncate(writer_->current_offset(),
                                                 io_options, /*dbg=*/nullptr);
      }
    }
    closing_ = true;
    IOStatus close_s = writer_->Close(write_options);
    if (io_s.ok()) {
      io_s = close_s;
    }
    close_s.PermitUncheckedError();
    writer_->PublishIfClosed();
  } else {
    io_s = pending_dest_->Close(io_options);
    pending_dest_.reset();
  }
  return io_s;
}

uint64_t BlogWalWriter::GetVerificationSize() const {
  return writer_ ? writer_->body_end_offset() : 0;
}

bool BlogWalWriter::PublishIfClosed() {
  if (writer_) {
    return writer_->PublishIfClosed();
  }
  if (!pending_dest_) {
    return true;
  }
  if (pending_dest_->IsClosed()) {
    pending_dest_.reset();
    return true;
  }
  return false;
}

bool BlogWalWriter::BufferIsEmpty() {
  if (!writer_ && !pending_dest_) {
    return true;
  }
  return writer_ ? writer_->BufferIsEmpty() : pending_dest_->BufferIsEmpty();
}

SequenceNumber BlogWalWriter::GetLastSeqnoRecorded() const {
  return last_seqno_recorded_;
}

}  // namespace ROCKSDB_NAMESPACE
