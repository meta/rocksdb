//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wal_reader.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "db/blog/blog_format.h"
#include "db/legacy_wal_reader.h"
#include "db/log_reader.h"
#include "file/sequence_file_reader.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

namespace {

class AutoDetectingWalReader final : public WalReader {
 public:
  AutoDetectingWalReader(
      std::unique_ptr<log::Reader>&& probe_reader,
      log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
      bool track_and_verify_wals, bool stop_replay_for_corruption,
      uint64_t min_wal_number_to_keep,
      const PredecessorWALInfo& observed_predecessor_wal_info, bool buffered)
      : probe_reader_(std::move(probe_reader)),
        reporter_(reporter),
        checksum_(checksum),
        log_number_(log_number),
        track_and_verify_wals_(track_and_verify_wals),
        stop_replay_for_corruption_(stop_replay_for_corruption),
        min_wal_number_to_keep_(min_wal_number_to_keep),
        observed_predecessor_wal_info_(observed_predecessor_wal_info),
        buffered_(buffered) {}

  using WalReader::ReadRecord;
  bool ReadRecord(Slice* record, std::string* scratch,
                  WALRecoveryMode wal_recovery_mode,
                  uint64_t* record_checksum) override {
    if (!reader_ && !DetectFormat()) {
      record->clear();
      scratch->clear();
      return false;
    }
    return reader_->ReadRecord(record, scratch, wal_recovery_mode,
                               record_checksum);
  }

  void UnmarkEOF() override {
    if (reader_) {
      reader_->UnmarkEOF();
    }
  }

  bool IsCompressedAndEmptyFile() override {
    return reader_ && reader_->IsCompressedAndEmptyFile();
  }

  const UnorderedMap<uint32_t, size_t>& GetRecordedTimestampSize()
      const override {
    return reader_ ? reader_->GetRecordedTimestampSize()
                   : empty_timestamp_sizes_;
  }

  uint64_t GetVerificationSize(uint64_t physical_file_size) const override {
    return reader_ ? reader_->GetVerificationSize(physical_file_size)
                   : physical_file_size;
  }

  uint64_t LastRecordOffset() override {
    return reader_ ? reader_->LastRecordOffset() : 0;
  }

  bool IsEOF() override {
    return reader_ ? reader_->IsEOF() : probe_reader_->IsEOF();
  }

  uint64_t GetLogNumber() const override { return log_number_; }

  log::Reader::Reporter* GetReporter() const override { return reporter_; }

 private:
  bool DetectFormat() {
    Slice first_block = probe_reader_->PrimeFirstBlock();
    const size_t comparable_size =
        std::min(first_block.size(), kBlogFileMagicSize);
    if (comparable_size == 0) {
      return false;
    }

    if (memcmp(first_block.data(), kBlogFileMagic, comparable_size) != 0) {
      reader_ = std::make_unique<LegacyWalReader>(std::move(probe_reader_));
      return true;
    }
    if (first_block.size() < kBlogFileMagicSize && buffered_) {
      return false;
    }
    if (first_block.size() < kBlogFileMagicSize) {
      reader_ = std::make_unique<LegacyWalReader>(std::move(probe_reader_));
      return true;
    }

    // No blog-format reader exists yet, so blog files fall through to the
    // legacy reader as before. The settings a blog reader needs are unused.
    (void)checksum_;
    (void)track_and_verify_wals_;
    (void)stop_replay_for_corruption_;
    (void)min_wal_number_to_keep_;
    reader_ = std::make_unique<LegacyWalReader>(std::move(probe_reader_));
    return true;
  }

  std::unique_ptr<log::Reader> probe_reader_;
  std::unique_ptr<WalReader> reader_;
  log::Reader::Reporter* const reporter_;
  const bool checksum_;
  const uint64_t log_number_;
  const bool track_and_verify_wals_;
  const bool stop_replay_for_corruption_;
  const uint64_t min_wal_number_to_keep_;
  PredecessorWALInfo observed_predecessor_wal_info_;
  const bool buffered_;
  UnorderedMap<uint32_t, size_t> empty_timestamp_sizes_;
};

}  // namespace

std::unique_ptr<WalReader> NewWalReader(
    std::shared_ptr<Logger> info_log,
    std::unique_ptr<SequentialFileReader>&& file,
    log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
    bool track_and_verify_wals, bool stop_replay_for_corruption,
    uint64_t min_wal_number_to_keep,
    const PredecessorWALInfo& observed_predecessor_wal_info, bool buffered) {
  // Detect the format from the first block, read through the normal read
  // path (not a raw peek) so sync points fire and read errors are reported
  // via the reporter exactly as a direct read would. For legacy files the
  // primed reader below behaves exactly like a reader whose first ReadRecord
  // performed that same read.
  std::unique_ptr<log::Reader> reader;
  if (buffered) {
    reader.reset(new log::FragmentBufferedReader(
        info_log, std::move(file), reporter, checksum, log_number));
  } else {
    reader.reset(new log::Reader(
        info_log, std::move(file), reporter, checksum, log_number,
        track_and_verify_wals, stop_replay_for_corruption,
        min_wal_number_to_keep, observed_predecessor_wal_info));
  }
  return std::make_unique<AutoDetectingWalReader>(
      std::move(reader), reporter, checksum, log_number, track_and_verify_wals,
      stop_replay_for_corruption, min_wal_number_to_keep,
      observed_predecessor_wal_info, buffered);
}

}  // namespace ROCKSDB_NAMESPACE
