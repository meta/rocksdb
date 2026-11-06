//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/blog/blog_format.h"
#include "db/blog/blog_reader.h"
#include "db/dbformat.h"
#include "db/log_reader.h"
#include "db/wal_reader.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class BlogFileReader;
class Decompressor;
class SequentialFileReader;
class StreamingUncompress;

// WalReader implementation for the blog WAL format. Owns the file framing
// reader plus all WAL-level record semantics (header properties, predecessor
// verification, timestamp sizes, decompression, tail handling).
class BlogWalReader final : public WalReader {
 public:
  // `initial_data` holds bytes already read from `file` (e.g. by a factory
  // peeking at the file magic); it is replayed before reading further.
  explicit BlogWalReader(
      std::unique_ptr<SequentialFileReader>&& file, std::string&& initial_data,
      log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
      bool track_and_verify_wals, bool stop_replay_for_corruption,
      uint64_t min_wal_number_to_keep,
      const PredecessorWALInfo& observed_predecessor_wal_info, bool tailing);
  ~BlogWalReader() override;

  using WalReader::ReadRecord;
  bool ReadRecord(Slice* record, std::string* scratch,
                  WALRecoveryMode wal_recovery_mode,
                  uint64_t* record_checksum) override;
  void UnmarkEOF() override;
  bool IsCompressedAndEmptyFile() override;
  const UnorderedMap<uint32_t, size_t>& GetRecordedTimestampSize()
      const override;
  uint64_t GetVerificationSize(uint64_t physical_file_size) const override;
  uint64_t LastRecordOffset() override;
  bool IsEOF() override;
  uint64_t GetLogNumber() const override;
  log::Reader::Reporter* GetReporter() const override;

 private:
  // Outcome requested by a record handler: stop replay (ReadRecord returns
  // false), keep scanning for the next record, retry the failed record with
  // fresh input, or deliver the current record (record-type readers only).
  enum class RecoveryAction { kStop, kContinue, kDeliver, kRetry };

  bool EnsureHeaderRead(WALRecoveryMode wal_recovery_mode);
  // Applies the recovery policy to a failed BlogFileReader::ReadRecord():
  // kContinue resumes past damage, kRetry re-reads it, kStop ends this call.
  RecoveryAction HandleReadFailure(WALRecoveryMode wal_recovery_mode,
                                   const Status& status, bool allow_retry);
  Status ScanForNextRecordBoundary();
  // True when a failed read lies in a footer record: inside a record whose
  // authenticated prefix names a footer type, or, after an accepted footer
  // record, a missing or damaged record of ours. Bytes with no escape sequence
  // of ours, such as trailing bytes after the locator, are not footer damage.
  bool FailureInFooter(const Status& status) const;
  // Ends replay at damage confined to footer records, in every recovery mode.
  void HandleUnsealedFooter(const Status& status, size_t bytes);
  void HandleIncompleteTail(WALRecoveryMode wal_recovery_mode,
                            const Status& status, size_t bytes);
  RecoveryAction HandleUnreadableRecord(WALRecoveryMode wal_recovery_mode,
                                        const Status& status,
                                        BlogReadFailureStage failure_stage,
                                        size_t bytes);
  void HandleUnsupportedRecord(const Status& status, size_t bytes);
  RecoveryAction HandleUnexpectedType(WALRecoveryMode wal_recovery_mode,
                                      size_t bytes);
  // For an intact record whose payload cannot be decompressed.
  RecoveryAction HandleUndecodableRecord(WALRecoveryMode wal_recovery_mode,
                                         const Status& status, size_t bytes);
  // Decode a user-defined timestamp-size record into recorded_cf_to_ts_sz_.
  // Never delivers.
  void ReadUserDefinedTimeStampRecord(const Slice& record);
  // Consume a footer record: stash index/properties/checksum-info for the
  // locator cross-check; on the locator, verify and seal the file.
  // Never delivers.
  RecoveryAction ReadFooterRecord(BlogRecordType record_type,
                                  const Slice& record, uint64_t record_offset);
  // Decode a streaming-compressed WriteBatch into *record for delivery.
  RecoveryAction ReadStreamingCompressedRecord(
      Slice* record, WALRecoveryMode wal_recovery_mode);
  // Decode a block-compressed WriteBatch into *record for delivery.
  RecoveryAction ReadCompressedRecord(Slice* record,
                                      CompressionType compression_type,
                                      WALRecoveryMode wal_recovery_mode);
  // Returns true if the scan itself failed. Reporters keep the first status
  // they are given, so the scan failure must be reported first.
  bool ReportScanFailureThenCorruption(const Status& scan_status,
                                       const Status& status, size_t bytes);
  // True when this file is recycled and no record of ours was identified at
  // the failure point, so the bytes may belong to a previous incarnation.
  bool LooksLikeRecycledStaleTail(BlogReadFailureStage failure_stage) const;
  void ReportHeaderFailure(WALRecoveryMode wal_recovery_mode,
                           const Status& status);
  void ReportCorruption(size_t bytes, const char* reason,
                        uint64_t log_number = kMaxSequenceNumber);
  // Corruption, which paranoid_checks=false and kSkipAnyCorruptedRecords
  // suppress.
  void ReportDrop(size_t bytes, const Status& reason,
                  uint64_t log_number = kMaxSequenceNumber);
  // A torn tail or header, reported through the Corruption channel as
  // Status::Corruption (as legacy WALs report them) with the original message.
  // `torn` must be the reader's internal Incomplete status.
  void ReportTornTail(size_t bytes, const Status& torn);
  // Unsupported format or I/O failure. I/O failures are truncated under
  // kSkipAnyCorruptedRecords or with paranoid_checks=false (like legacy
  // WALs); unsupported formats fail the open in every mode.
  void ReportUnrecoverable(size_t bytes, const Status& reason,
                           uint64_t log_number = kMaxSequenceNumber);
  // Loss the active mode accepts: logged, never fails the open. `context`
  // says why the loss is tolerated.
  void ReportToleratedDrop(size_t bytes, const Status& reason,
                           const char* context);

  log::Reader::Reporter* const reporter_;
  const uint64_t log_number_;
  const bool track_and_verify_wals_;
  const bool stop_replay_for_corruption_;
  const bool tailing_;
  const bool checksum_;
  const uint64_t min_wal_number_to_keep_;
  PredecessorWALInfo observed_predecessor_wal_info_;
  std::unique_ptr<BlogFileReader> blog_reader_;
  std::shared_ptr<Decompressor> decompressor_;
  bool header_read_ = false;
  bool recycled_ = false;
  // End of input as of the last read, reported by IsEOF(). UnmarkEOF(), or
  // the next ReadRecord(), resumes where that read stopped.
  bool eof_ = false;
  // Terminal: a failed read leaves the file position unknown.
  bool read_error_ = false;
  bool header_rejected_ = false;
  uint64_t last_record_offset_ = 0;
  bool first_record_read_ = false;
  bool footer_started_ = false;
  bool footer_complete_ = false;
  // Set once a streamed record may be missing, after which later streamed
  // records in this file cannot be decoded correctly.
  bool streaming_broken_ = false;
  std::vector<std::pair<uint64_t, BlogRecordType>> footer_records_;
  UnorderedMap<uint32_t, size_t> recorded_cf_to_ts_sz_;
  std::string uncompressed_record_;
  // Set when the header declares cross-record streaming compression.
  std::unique_ptr<StreamingUncompress> streaming_uncompress_;
  std::unique_ptr<char[]> streaming_uncompress_chunk_;
};

}  // namespace ROCKSDB_NAMESPACE
