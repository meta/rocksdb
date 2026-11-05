//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "db/blog/blog_format.h"
#include "db/wal_writer.h"
#include "rocksdb/advanced_compression.h"
#include "rocksdb/compression_type.h"
#include "util/aligned_buffer.h"

namespace ROCKSDB_NAMESPACE {

class BlogFileWriter;
class StreamingCompress;

// WalWriter implementation for the blog file format.
class BlogWalWriter final : public WalWriter {
 public:
  // `is_recycled` marks whether this file itself was recycled (an existing
  // file overwritten with new contents), not whether the DB has WAL recycling
  // enabled. It sets kBlogFileRecycled, which tells recovery the file may
  // contain trailing stale data from a previous incarnation.
  //
  // `use_streaming_compression` compresses non-empty WriteBatch records with
  // `compression_type` as one stream spanning the file's records, which must
  // then be decompressed in order, instead of independently per record. Only
  // types with streaming support (ZSTD) are accepted. After a write error that
  // may have lost part of the stream, later records are written uncompressed.
  // In a non-recycled file the compressed bytes stream directly into
  // unspecified-size records; a recycled file cannot use that encoding and
  // buffers them into sized records instead.
  explicit BlogWalWriter(std::unique_ptr<WritableFileWriter>&& dest,
                         uint64_t log_number, bool is_recycled,
                         bool manual_flush, CompressionType compression_type,
                         bool use_streaming_compression,
                         bool track_and_verify_wals, ChecksumType checksum_type,
                         size_t blog_timestamp_size, const std::string& db_id,
                         const std::string& db_session_id,
                         const std::string& db_host_id, uint64_t creation_time);
  ~BlogWalWriter() override;

  using WalWriter::AddRecord;
  IOStatus AddRecord(const WriteOptions& write_options, const Slice& record,
                     SequenceNumber first_sequence,
                     size_t num_sequences) override;
  IOStatus WriteFileStart(
      const WriteOptions& write_options,
      const PredecessorWALInfo& predecessor_wal_info) override;
  IOStatus MaybeAddUserDefinedTimestampSizeRecord(
      const WriteOptions& write_options,
      const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz) override;

  WritableFileWriter* file() override;
  const WritableFileWriter* file() const override;
  uint64_t get_log_number() const override;

  IOStatus WriteBuffer(const WriteOptions& write_options) override;
  IOStatus Close(const WriteOptions& write_options) override;
  bool PublishIfClosed() override;
  bool BufferIsEmpty() override;
  SequenceNumber GetLastSeqnoRecorded() const override;
  uint64_t GetVerificationSize() const override;

 private:
  friend class BlogWriterTest;

  // Header outcome; a failed header is never retried (partial bytes persist).
  enum class HeaderState : uint8_t { kNotWritten, kWritten, kFailed };

  IOStatus EnsureHeaderWritten(const WriteOptions& write_options);

  // Streams one non-empty WriteBatch record through `streaming_compressor_`
  // directly into an unspecified-size file record. The caller must have
  // written the header, confirmed the stream is unbroken, and confirmed the
  // file is not recycled.
  IOStatus AddStreamingRecordDirect(const WriteOptions& write_options,
                                    const Slice& record);

  std::unique_ptr<WritableFileWriter> pending_dest_;
  std::unique_ptr<BlogFileWriter> writer_;
  BlogFileHeader header_;
  const uint64_t log_number_;
  const bool manual_flush_;
  const CompressionType compression_type_;
  const bool use_streaming_compression_;
  const bool track_and_verify_wals_;
  HeaderState header_state_ = HeaderState::kNotWritten;
  bool closing_ = false;
  // Set once a streaming-compressed record may not have reached the file.
  // Later streamed records could reference its history, so later records are
  // written uncompressed instead.
  bool streaming_compression_broken_ = false;
  std::unique_ptr<Compressor> compressor_;
  Compressor::ManagedWorkingArea compressor_working_area_;
  GrowableBuffer compressed_record_;
  std::unique_ptr<StreamingCompress> streaming_compressor_;
  std::string streaming_compressed_record_;
  UnorderedMap<uint32_t, size_t> recorded_cf_to_ts_sz_;
  // Footer sequence range over consumed sequences: min over first sequences,
  // max over last sequences (first + num - 1). `has_seqno_recorded_` is set
  // once a sequence-consuming record is seen; records with
  // `num_sequences == 0` contribute no sequences and are skipped.
  SequenceNumber min_seqno_recorded_ = 0;
  SequenceNumber max_seqno_recorded_ = 0;
  // Max over first sequences of all records. Kept for
  // GetLastSeqnoRecorded() to agree with what recovery observes (the batch
  // header sequence) for predecessor verification. NOTE: this is NOT the
  // footer's maxSequence.
  SequenceNumber last_seqno_recorded_ = 0;
  bool has_seqno_recorded_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
