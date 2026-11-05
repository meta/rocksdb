//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog_wal_reader.h"

#include <cassert>
#include <utility>

#include "db/blog/blog_format.h"
#include "db/blog/blog_reader.h"
#include "db/dbformat.h"
#include "util/cast_util.h"
#include "util/compression.h"
#include "util/udt_util.h"
#include "util/xxhash.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr size_t kStreamingUncompressChunkSize = 32 * 1024;

// Decompresses one streaming-compressed record into `output`, advancing the
// stream. Returns false if the stream data is invalid.
bool UncompressStreamingRecord(StreamingUncompress& uncompress, char* chunk,
                               const Slice& input, std::string* output) {
  output->clear();
  const char* next_input = input.data();
  int64_t remaining = 0;
  size_t chunk_size = 0;
  do {
    remaining =
        uncompress.Uncompress(next_input, input.size(), chunk, &chunk_size);
    next_input = nullptr;
    if (remaining < 0) {
      return false;
    }
    output->append(chunk, chunk_size);
    // A full chunk may leave decompressed bytes still buffered in the stream.
  } while (remaining > 0 || chunk_size == kStreamingUncompressChunkSize);
  return true;
}

}  // namespace

BlogWalReader::BlogWalReader(
    std::unique_ptr<SequentialFileReader>&& file, std::string&& initial_data,
    log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
    bool track_and_verify_wals, bool stop_replay_for_corruption,
    uint64_t min_wal_number_to_keep,
    const PredecessorWALInfo& observed_predecessor_wal_info, bool tailing)
    : reporter_(reporter),
      log_number_(log_number),
      track_and_verify_wals_(track_and_verify_wals),
      stop_replay_for_corruption_(stop_replay_for_corruption),
      tailing_(tailing),
      checksum_(checksum),
      min_wal_number_to_keep_(min_wal_number_to_keep),
      observed_predecessor_wal_info_(observed_predecessor_wal_info),
      blog_reader_(new BlogFileReader(std::move(file), nullptr, checksum,
                                      std::move(initial_data))) {}

BlogWalReader::~BlogWalReader() = default;

bool BlogWalReader::ReadRecord(Slice* record, std::string* scratch,
                               WALRecoveryMode wal_recovery_mode,
                               uint64_t* record_checksum) {
  scratch->clear();
  record->clear();
  if (eof_) {
    UnmarkEOF();
  }
  if (footer_complete_ || header_rejected_ || read_error_) {
    return false;
  }
  if (!header_read_ && !EnsureHeaderRead(wal_recovery_mode)) {
    return false;
  }
  bool allow_retry = true;
  while (true) {
    BlogRecordType record_type;
    CompressionType record_compression_type = kNoCompression;
    uint64_t record_offset = 0;
    Status s =
        blog_reader_->ReadRecord(&record_type, record, scratch, &record_offset,
                                 &record_compression_type);
    if (!s.ok()) {
      const RecoveryAction action =
          HandleReadFailure(wal_recovery_mode, s, allow_retry);
      if (action == RecoveryAction::kStop) {
        return false;
      }
      allow_retry = action != RecoveryAction::kRetry;
      continue;
    }
    allow_retry = true;
    eof_ = false;

    if (record_type == kBlogPreambleStartRecord) {
      continue;
    }
    if (record_type == kBlogUserDefinedTimestampSizeRecord) {
      ReadUserDefinedTimeStampRecord(*record);
      continue;
    }
    if (record_type >= kBlogFooterIndexRecord) {
      if (ReadFooterRecord(record_type, *record, record_offset) ==
          RecoveryAction::kStop) {
        return false;
      }
      continue;
    }
    if (record_type != kBlogWriteBatchRecord) {
      if (HandleUnexpectedType(wal_recovery_mode, record->size()) ==
          RecoveryAction::kStop) {
        return false;
      }
      continue;
    }
    if (record_compression_type == kStreamingCompressionSentinel) {
      const RecoveryAction action =
          ReadStreamingCompressedRecord(record, wal_recovery_mode);
      if (action == RecoveryAction::kStop) {
        return false;
      }
      if (action == RecoveryAction::kContinue) {
        continue;
      }
    } else if (record_compression_type != kNoCompression) {
      const RecoveryAction action = ReadCompressedRecord(
          record, record_compression_type, wal_recovery_mode);
      if (action == RecoveryAction::kStop) {
        return false;
      }
      if (action == RecoveryAction::kContinue) {
        continue;
      }
    }

    if (record_checksum != nullptr) {
      *record_checksum = XXH3_64bits(record->data(), record->size());
    }
    last_record_offset_ = record_offset;
    first_record_read_ = true;
    return true;
  }
}

BlogWalReader::RecoveryAction BlogWalReader::HandleReadFailure(
    WALRecoveryMode wal_recovery_mode, const Status& status, bool allow_retry) {
  eof_ = status.IsNotFound() || status.IsIncomplete();
  const size_t dropped_bytes = blog_reader_->last_dropped_bytes();
  if (status.IsNotFound()) {
    if (footer_started_) {
      HandleUnsealedFooter(
          Status::Corruption("Blog WAL footer is missing its locator"),
          dropped_bytes);
    }
    return RecoveryAction::kStop;
  }
  if (status.IsIncomplete()) {
    if (FailureInFooter(status)) {
      HandleUnsealedFooter(status, dropped_bytes);
    } else {
      HandleIncompleteTail(wal_recovery_mode, status, dropped_bytes);
    }
    return RecoveryAction::kStop;
  }
  if (status.IsIOError()) {
    read_error_ = true;
    ReportUnrecoverable(dropped_bytes, status);
    return RecoveryAction::kStop;
  }
  if (status.IsNotSupported()) {
    HandleUnsupportedRecord(status, dropped_bytes);
    return RecoveryAction::kStop;
  }
  if (!status.IsCorruption()) {
    ReportUnrecoverable(dropped_bytes, status);
    return RecoveryAction::kStop;
  }
  if (FailureInFooter(status)) {
    HandleUnsealedFooter(status, dropped_bytes);
    return RecoveryAction::kStop;
  }
  if (recycled_ && blog_reader_->CanRefresh()) {
    // A live recycled tail can mix a new prefix with an old body, so a
    // failure with no later record boundary may be an in-progress write
    // rather than damage. A boundary proves the writer moved past, so
    // retry with fresh bytes before declaring corruption.
    // Resuming refreshes from the last accepted record.
    const uint64_t failed_offset = blog_reader_->current_offset();
    uint64_t boundary_offset = 0;
    Status scan_status = blog_reader_->ScanForEscapeSequence(&boundary_offset);
    eof_ = scan_status.IsNotFound();
    if (scan_status.IsNotFound()) {
      return RecoveryAction::kStop;
    }
    if (!scan_status.ok()) {
      if (scan_status.IsIOError()) {
        read_error_ = true;
      }
      ReportDrop(dropped_bytes, scan_status);
      return RecoveryAction::kStop;
    }
    if (allow_retry) {
      Status refresh_status = blog_reader_->Refresh(failed_offset);
      if (!refresh_status.ok()) {
        read_error_ = true;
        ReportUnrecoverable(dropped_bytes, refresh_status);
        return RecoveryAction::kStop;
      }
      return RecoveryAction::kRetry;
    }
  }
  if (HandleUnreadableRecord(wal_recovery_mode, status,
                             blog_reader_->last_failure_stage(),
                             dropped_bytes) == RecoveryAction::kStop) {
    return RecoveryAction::kStop;
  }
  // The skipped bytes may have held a streamed record.
  streaming_broken_ = true;
  return RecoveryAction::kContinue;
}

void BlogWalReader::ReadUserDefinedTimeStampRecord(const Slice& record) {
  UserDefinedTimestampSizeRecord timestamp_record;
  Slice timestamp_record_slice(record);
  Status s = timestamp_record.DecodeFrom(&timestamp_record_slice);
  if (!s.ok() || !timestamp_record_slice.empty()) {
    ReportCorruption(record.size(),
                     "could not decode Blog timestamp size record");
    return;
  }
  s = log::Reader::UpdateRecordedTimestampSize(
      recorded_cf_to_ts_sz_, timestamp_record.GetUserDefinedTimestampSize());
  if (!s.ok()) {
    ReportCorruption(record.size(), s.getState());
  }
}

BlogWalReader::RecoveryAction BlogWalReader::ReadFooterRecord(
    BlogRecordType record_type, const Slice& record, uint64_t record_offset) {
  footer_started_ = true;
  if (record_type == kBlogFooterPropertiesRecord) {
    BlogFileFooterProperties properties;
    Status s = properties.DecodeFrom(record);
    if (!s.ok()) {
      HandleUnsealedFooter(s, record.size());
      return RecoveryAction::kStop;
    }
  }
  if (record_type != kBlogFooterLocatorRecord) {
    footer_records_.emplace_back(record_offset, record_type);
    return RecoveryAction::kContinue;
  }

  BlogFileFooterLocator locator;
  Status s = locator.DecodeFrom(record);
  if (!s.ok()) {
    HandleUnsealedFooter(s, record.size());
    return RecoveryAction::kStop;
  }
  if (locator.entries.size() != footer_records_.size()) {
    HandleUnsealedFooter(
        Status::Corruption("Blog WAL footer locator entry count mismatch"),
        record.size());
    return RecoveryAction::kStop;
  }
  uint64_t target_offset = record_offset;
  for (size_t i = 0; i < locator.entries.size(); ++i) {
    const uint64_t relative_bytes =
        uint64_t{locator.entries[i].relative_offset_4B} *
        kBlogMinRecordAlignment;
    if (relative_bytes > target_offset) {
      HandleUnsealedFooter(
          Status::Corruption("Blog WAL footer locator offset underflow"),
          record.size());
      return RecoveryAction::kStop;
    }
    target_offset -= relative_bytes;
    const auto& expected = footer_records_[footer_records_.size() - 1 - i];
    if (locator.entries[i].record_type != expected.second ||
        target_offset != expected.first) {
      HandleUnsealedFooter(
          Status::Corruption("Blog WAL footer locator does not match footer"),
          record.size());
      return RecoveryAction::kStop;
    }
  }
  footer_complete_ = true;
  eof_ = true;
  return RecoveryAction::kStop;
}

BlogWalReader::RecoveryAction BlogWalReader::ReadStreamingCompressedRecord(
    Slice* record, WALRecoveryMode wal_recovery_mode) {
  // BlogFileReader rejects the sentinel unless the header declares streaming
  // compression.
  assert(streaming_uncompress_);
  // Decoding past a missing streamed record can yield wrong data without
  // any error, so it is never attempted.
  if (streaming_broken_) {
    if (HandleUndecodableRecord(
            wal_recovery_mode,
            Status::Corruption("streaming Blog WAL record follows a "
                               "skipped or undecodable record"),
            record->size()) == RecoveryAction::kStop) {
      return RecoveryAction::kStop;
    }
    return RecoveryAction::kContinue;
  }
  if (!UncompressStreamingRecord(*streaming_uncompress_,
                                 streaming_uncompress_chunk_.get(), *record,
                                 &uncompressed_record_)) {
    streaming_broken_ = true;
    if (HandleUndecodableRecord(
            wal_recovery_mode,
            Status::Corruption(
                "could not decompress streaming Blog WAL record"),
            record->size()) == RecoveryAction::kStop) {
      return RecoveryAction::kStop;
    }
    return RecoveryAction::kContinue;
  }
  *record = Slice(uncompressed_record_);
  return RecoveryAction::kDeliver;
}

BlogWalReader::RecoveryAction BlogWalReader::ReadCompressedRecord(
    Slice* record, CompressionType compression_type,
    WALRecoveryMode wal_recovery_mode) {
  if (!decompressor_) {
    ReportCorruption(record->size(), "compressed Blog WAL has no decompressor");
    return RecoveryAction::kStop;
  }

  Decompressor::Args args;
  args.compression_type = compression_type;
  args.compressed_data = *record;
  Status s = decompressor_->ExtractUncompressedSize(args);
  if (s.ok()) {
    uncompressed_record_.resize(args.uncompressed_size);
    s = decompressor_->DecompressBlock(args, uncompressed_record_.empty()
                                                 ? nullptr
                                                 : &uncompressed_record_[0]);
  }
  if (!s.ok()) {
    // A codec this build lacks is a compatibility boundary, not damaged
    // data, so no mode may tolerate it as corruption.
    if (s.IsNotSupported()) {
      HandleUnsupportedRecord(s, record->size());
      return RecoveryAction::kStop;
    }
    if (HandleUndecodableRecord(wal_recovery_mode,
                                Status::Corruption(s.ToString()),
                                record->size()) == RecoveryAction::kStop) {
      return RecoveryAction::kStop;
    }
    return RecoveryAction::kContinue;
  }
  *record = Slice(uncompressed_record_);
  return RecoveryAction::kDeliver;
}

bool BlogWalReader::FailureInFooter(const Status& status) const {
  if (blog_reader_->last_failure_in_footer_record()) {
    return true;
  }
  return footer_started_ && (!status.IsCorruption() ||
                             blog_reader_->last_failure_stage() ==
                                 BlogReadFailureStage::kRecordValidationFailed);
}

void BlogWalReader::HandleUnsealedFooter(const Status& status, size_t bytes) {
  // The writer appends nothing after its first footer record, so damage there
  // cannot drop data. Close does not sync the footer, so a crash after a clean
  // close can leave it torn.
  eof_ = true;
  if (!tailing_) {
    ReportToleratedDrop(bytes, status, "Blog WAL footer is unsealed");
  }
}

void BlogWalReader::HandleIncompleteTail(WALRecoveryMode wal_recovery_mode,
                                         const Status& status, size_t bytes) {
  switch (wal_recovery_mode) {
    case WALRecoveryMode::kTolerateCorruptedTailRecords:
    case WALRecoveryMode::kSkipAnyCorruptedRecords:
      break;
    case WALRecoveryMode::kAbsoluteConsistency:
    case WALRecoveryMode::kPointInTimeRecovery:
      if (!tailing_) {
        ReportTornTail(bytes, status);
      }
      break;
  }
}

BlogWalReader::RecoveryAction BlogWalReader::HandleUnreadableRecord(
    WALRecoveryMode wal_recovery_mode, const Status& status,
    BlogReadFailureStage failure_stage, size_t bytes) {
  assert(status.IsCorruption());
  switch (wal_recovery_mode) {
    case WALRecoveryMode::kSkipAnyCorruptedRecords: {
      const Status scan_status = ScanForNextRecordBoundary();
      if (ReportScanFailureThenCorruption(scan_status, status, bytes)) {
        return RecoveryAction::kStop;
      }
      if (!scan_status.ok() && LooksLikeRecycledStaleTail(failure_stage)) {
        // Stale data from a previous incarnation is a clean end, not damage.
        return RecoveryAction::kStop;
      }
      ReportDrop(bytes, status);
      return scan_status.ok() ? RecoveryAction::kContinue
                              : RecoveryAction::kStop;
    }
    case WALRecoveryMode::kTolerateCorruptedTailRecords: {
      // Only damage with no record beyond it is tail damage, so always scan.
      const Status scan_status = ScanForNextRecordBoundary();
      if (ReportScanFailureThenCorruption(scan_status, status, bytes)) {
        return RecoveryAction::kStop;
      }
      if (scan_status.ok()) {
        // A boundary beyond the damage proves mid-file corruption.
        ReportDrop(bytes, status);
        return RecoveryAction::kStop;
      }
      if (LooksLikeRecycledStaleTail(failure_stage)) {
        return RecoveryAction::kStop;
      }
      ReportToleratedDrop(bytes, status, "no further Blog WAL record boundary");
      return RecoveryAction::kStop;
    }
    case WALRecoveryMode::kAbsoluteConsistency:
    case WALRecoveryMode::kPointInTimeRecovery: {
      if (!LooksLikeRecycledStaleTail(failure_stage)) {
        ReportDrop(bytes, status);
        return RecoveryAction::kStop;
      }
      // Stale tail or mid-file damage: the scan tells them apart.
      const Status scan_status = ScanForNextRecordBoundary();
      if (ReportScanFailureThenCorruption(scan_status, status, bytes)) {
        return RecoveryAction::kStop;
      }
      if (scan_status.ok()) {
        // Never resume: kPointInTimeRecovery would replay from past a hole.
        ReportDrop(bytes, status);
        return RecoveryAction::kStop;
      }
      // A clean end of this file, but stale bytes can also stand where this
      // incarnation's unsynced records were lost. Like a legacy old record,
      // this lets kPointInTimeRecovery continue into the next WAL only if its
      // sequence numbers are contiguous.
      if (wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery &&
          !tailing_ && reporter_ != nullptr) {
        reporter_->OldLogRecord(bytes);
      }
      return RecoveryAction::kStop;
    }
  }
  assert(false);  // All WALRecoveryMode values are handled above.
  ReportDrop(bytes, status);
  return RecoveryAction::kStop;
}

void BlogWalReader::HandleUnsupportedRecord(const Status& status,
                                            size_t bytes) {
  assert(status.IsNotSupported());
  // Rejected in every mode including kSkipAnyCorruptedRecords: an unknown
  // record is readable data whose meaning this build cannot know, not garbage.
  ReportUnrecoverable(bytes, status);
}

BlogWalReader::RecoveryAction BlogWalReader::HandleUnexpectedType(
    WALRecoveryMode wal_recovery_mode, size_t bytes) {
  ReportCorruption(bytes, "unexpected Blog WAL record type");
  switch (wal_recovery_mode) {
    case WALRecoveryMode::kSkipAnyCorruptedRecords:
      return RecoveryAction::kContinue;
    case WALRecoveryMode::kTolerateCorruptedTailRecords:
    case WALRecoveryMode::kAbsoluteConsistency:
    case WALRecoveryMode::kPointInTimeRecovery:
      return RecoveryAction::kStop;
  }
  assert(false);  // All WALRecoveryMode values are handled above.
  return RecoveryAction::kStop;
}

BlogWalReader::RecoveryAction BlogWalReader::HandleUndecodableRecord(
    WALRecoveryMode wal_recovery_mode, const Status& status, size_t bytes) {
  ReportDrop(bytes, status);
  switch (wal_recovery_mode) {
    case WALRecoveryMode::kSkipAnyCorruptedRecords:
      return RecoveryAction::kContinue;
    case WALRecoveryMode::kTolerateCorruptedTailRecords:
    case WALRecoveryMode::kAbsoluteConsistency:
    case WALRecoveryMode::kPointInTimeRecovery:
      return RecoveryAction::kStop;
  }
  assert(false);  // All WALRecoveryMode values are handled above.
  return RecoveryAction::kStop;
}

Status BlogWalReader::ScanForNextRecordBoundary() {
  uint64_t found_offset = 0;
  const Status s = blog_reader_->ScanForEscapeSequence(&found_offset);
  eof_ = s.IsNotFound();
  return s;
}

bool BlogWalReader::ReportScanFailureThenCorruption(const Status& scan_status,
                                                    const Status& status,
                                                    size_t bytes) {
  if (scan_status.ok() || scan_status.IsNotFound()) {
    return false;
  }
  assert(!scan_status.IsIncomplete());
  if (scan_status.IsIOError()) {
    read_error_ = true;
  }
  ReportUnrecoverable(bytes, scan_status);
  ReportDrop(bytes, status);
  return true;
}

bool BlogWalReader::LooksLikeRecycledStaleTail(
    BlogReadFailureStage failure_stage) const {
  return recycled_ &&
         failure_stage == BlogReadFailureStage::kEscapeSequenceNotFound;
}

void BlogWalReader::ReportHeaderFailure(WALRecoveryMode wal_recovery_mode,
                                        const Status& status) {
  if (!status.IsIncomplete()) {
    // Stops every mode, even while tailing.
    if (status.IsCorruption()) {
      ReportDrop(kBlogFileFixedHeaderSize, status);
    } else {
      ReportUnrecoverable(kBlogFileFixedHeaderSize, status);
    }
    return;
  }
  // An incomplete header means the writer may still be appending.
  switch (wal_recovery_mode) {
    case WALRecoveryMode::kTolerateCorruptedTailRecords:
    case WALRecoveryMode::kSkipAnyCorruptedRecords:
      break;
    case WALRecoveryMode::kAbsoluteConsistency:
    case WALRecoveryMode::kPointInTimeRecovery:
      if (!tailing_) {
        ReportTornTail(kBlogFileFixedHeaderSize, status);
      }
      break;
  }
}

void BlogWalReader::UnmarkEOF() {
  if (!eof_ || read_error_ || footer_complete_ || header_rejected_) {
    return;
  }
  eof_ = false;
  if (!blog_reader_->CanRefresh()) {
    return;
  }
  Status s;
  if (!header_read_) {
    // Nothing is consumed before the header is accepted, so start over: a
    // header from a previous incarnation (a recycled WAL renamed before the
    // writer rewrote it) is then read anew.
    std::unique_ptr<SequentialFileReader> fresh_file;
    s = blog_reader_->file()->ReopenAt(0, &fresh_file);
    if (s.ok()) {
      blog_reader_.reset(new BlogFileReader(std::move(fresh_file), nullptr,
                                            checksum_, std::string()));
    }
  } else if (recycled_) {
    // The writer may have overwritten unaccepted bytes of a live recycled
    // WAL since they were read.
    s = blog_reader_->Refresh();
  }
  if (!s.ok()) {
    read_error_ = true;
    ReportUnrecoverable(0, s);
  }
}

bool BlogWalReader::IsCompressedAndEmptyFile() {
  // A WAL with a valid header and no data record is not physically empty,
  // compressed or not.
  return header_read_ && !first_record_read_;
}

const UnorderedMap<uint32_t, size_t>& BlogWalReader::GetRecordedTimestampSize()
    const {
  return recorded_cf_to_ts_sz_;
}

uint64_t BlogWalReader::GetVerificationSize(
    uint64_t /*physical_file_size*/) const {
  return blog_reader_->body_end_offset();
}

uint64_t BlogWalReader::LastRecordOffset() { return last_record_offset_; }

bool BlogWalReader::IsEOF() { return eof_; }

uint64_t BlogWalReader::GetLogNumber() const { return log_number_; }

log::Reader::Reporter* BlogWalReader::GetReporter() const { return reporter_; }

bool BlogWalReader::EnsureHeaderRead(WALRecoveryMode wal_recovery_mode) {
  if (header_read_) {
    return true;
  }

  BlogFileHeader header;
  Status s = blog_reader_->ReadHeader(&header);
  eof_ = s.IsNotFound() || s.IsIncomplete();
  if (!s.ok()) {
    if (s.IsIOError()) {
      read_error_ = true;
    }
    ReportHeaderFailure(wal_recovery_mode, s);
    return false;
  }

  if (!BlogFileHeader::VerifyWalEscapeSequence(header.escape_sequence,
                                               log_number_)) {
    if (blog_reader_->CanRefresh()) {
      // A live recycled WAL renamed to a new log number before its header was
      // rewritten: the writer is still coming. Wait at the end of input;
      // resuming reads the header anew.
      eof_ = true;
      return false;
    }
    // With checksums enabled, header verification protects the escape bytes;
    // a log-number mismatch identifies a stale incarnation (a recycled WAL
    // renamed before its header was rewritten). With kNoChecksum, some escape
    // corruption can pass header verification and look stale. Accept that
    // ambiguity and treat the file as empty, allowing recovery of a genuinely
    // stale WAL without an error. The stale header's recycled flag describes
    // the old incarnation and must not steer this decision.
    header_rejected_ = true;
    eof_ = true;
    if (reporter_ != nullptr) {
      reporter_->OldLogRecord(kBlogFileFixedHeaderSize);
    }
    return false;
  }

  recycled_ = header.is_recycled();
  const std::string compression_compatibility_name =
      header.GetProperty(kBlogPropCompressionCompatibilityName);
  if (!compression_compatibility_name.empty()) {
    std::shared_ptr<CompressionManager> compression_manager;
    s = ResolveCompressionManagerByCompatibilityName(
        compression_compatibility_name, nullptr, &compression_manager);
    if (s.ok() && compression_manager == nullptr) {
      // Compatibility boundary, not damaged data: NotSupported so no
      // corruption-tolerance knob can mistake it for end-of-file.
      s = Status::NotSupported(
          "Blog WAL has no compatible compression manager for \"" +
          compression_compatibility_name + "\"");
    }
    if (!s.ok()) {
      header_rejected_ = true;
      if (s.IsCorruption()) {
        ReportDrop(kBlogFileFixedHeaderSize, s);
      } else {
        ReportUnrecoverable(kBlogFileFixedHeaderSize, s);
      }
      return false;
    }
    decompressor_ = compression_manager->GetDecompressor();
  }

  const std::string streaming_compression_type =
      header.GetProperty(kBlogPropWriteBatchStreamingCompressionType);
  if (!streaming_compression_type.empty()) {
    std::string raw_type;
    // BlogFileHeader::DecodeFrom validated the value as two hex digits.
    bool decoded = Slice(streaming_compression_type).DecodeHex(&raw_type);
    assert(decoded && raw_type.size() == 1);
    (void)decoded;
    streaming_uncompress_ = StreamingUncompress::Create(
        lossless_cast<CompressionType>(raw_type[0]),
        /*compress_format_version=*/2, kStreamingUncompressChunkSize);
    if (!streaming_uncompress_) {
      header_rejected_ = true;
      ReportUnrecoverable(
          kBlogFileFixedHeaderSize,
          Status::NotSupported("Blog WAL streaming compression type 0x" +
                               streaming_compression_type +
                               " is not supported"));
      return false;
    }
    streaming_uncompress_chunk_.reset(new char[kStreamingUncompressChunkSize]);
  }

  const std::string predecessor =
      header.GetProperty(kBlogPropPredecessorWalInfo);
  if (!predecessor.empty()) {
    Slice predecessor_slice(predecessor);
    PredecessorWALInfo predecessor_wal_info;
    s = predecessor_wal_info.DecodeFrom(&predecessor_slice);
    if (!s.ok() || !predecessor_slice.empty()) {
      ReportCorruption(predecessor.size(),
                       "could not decode Blog PredecessorWalInfo property");
    } else {
      log::Reader::MaybeVerifyPredecessorWALInfo(
          reporter_, wal_recovery_mode, track_and_verify_wals_,
          stop_replay_for_corruption_, min_wal_number_to_keep_,
          observed_predecessor_wal_info_, blog_reader_->file()->file_name(),
          predecessor.size(), predecessor_wal_info);
    }
  }
  header_read_ = true;
  return true;
}

void BlogWalReader::ReportCorruption(size_t bytes, const char* reason,
                                     uint64_t log_number) {
  ReportDrop(bytes, Status::Corruption(reason), log_number);
}

void BlogWalReader::ReportDrop(size_t bytes, const Status& reason,
                               uint64_t log_number) {
  if (reporter_ != nullptr) {
    reporter_->Corruption(bytes, reason, log_number);
  }
}

void BlogWalReader::ReportTornTail(size_t bytes, const Status& torn) {
  assert(torn.IsIncomplete());
  const char* msg = torn.getState();
  ReportDrop(bytes, Status::Corruption(msg != nullptr ? msg : "torn tail"));
}

void BlogWalReader::ReportUnrecoverable(size_t bytes, const Status& reason,
                                        uint64_t log_number) {
  if (reporter_ != nullptr) {
    reporter_->UnrecoverableError(bytes, reason, log_number);
  }
}

void BlogWalReader::ReportToleratedDrop(size_t bytes, const Status& reason,
                                        const char* context) {
  if (reporter_ != nullptr) {
    Status annotated = Status::CopyAppendMessage(reason, "; ", context);
    reporter_->ToleratedCorruption(bytes, annotated, log_number_);
    // Reporters may ignore tolerated loss.
    annotated.PermitUncheckedError();
  }
}

}  // namespace ROCKSDB_NAMESPACE
