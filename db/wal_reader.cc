//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wal_reader.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "db/blog/blog_format.h"
#include "db/blog_wal_reader.h"
#include "db/legacy_wal_reader.h"
#include "file/sequence_file_reader.h"

namespace ROCKSDB_NAMESPACE {

namespace {

enum class WalFormat { kLegacy, kBlog, kPending };

WalFormat DetectWalFormat(const Slice& prefix, bool buffered) {
  const size_t comparable_size = std::min(prefix.size(), kBlogFileMagicSize);
  if (comparable_size == 0) {
    return WalFormat::kPending;
  }
  if (memcmp(prefix.data(), kBlogFileMagic, comparable_size) != 0) {
    return WalFormat::kLegacy;
  }
  if (prefix.size() < kBlogFileMagicSize) {
    return buffered ? WalFormat::kPending : WalFormat::kLegacy;
  }
  return WalFormat::kBlog;
}

}  // namespace

WalReaderOpenState::WalReaderOpenState(
    std::shared_ptr<Logger> info_log,
    std::unique_ptr<SequentialFileReader>&& file,
    log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
    bool track_and_verify_wals, bool stop_replay_for_corruption,
    uint64_t min_wal_number_to_keep,
    const PredecessorWALInfo& observed_predecessor_wal_info, bool buffered)
    : reporter_(reporter),
      checksum_(checksum),
      log_number_(log_number),
      track_and_verify_wals_(track_and_verify_wals),
      stop_replay_for_corruption_(stop_replay_for_corruption),
      min_wal_number_to_keep_(min_wal_number_to_keep),
      observed_predecessor_wal_info_(observed_predecessor_wal_info),
      buffered_(buffered) {
  if (buffered) {
    std::unique_ptr<BufferedLegacyWalReader> candidate(
        new BufferedLegacyWalReader(info_log, std::move(file), reporter,
                                    checksum, log_number));
    probe_reader_ = &candidate->reader_;
    candidate_reader_ = std::move(candidate);
  } else {
    std::unique_ptr<LegacyWalReader> candidate(new LegacyWalReader(
        info_log, std::move(file), reporter, checksum, log_number,
        track_and_verify_wals, stop_replay_for_corruption,
        min_wal_number_to_keep, observed_predecessor_wal_info));
    probe_reader_ = &candidate->reader_;
    candidate_reader_ = std::move(candidate);
  }
}

WalReaderOpenState::Result WalReaderOpenState::TryOpen(
    std::unique_ptr<WalReader>* reader) {
  assert(reader && !*reader);
  assert(candidate_reader_ && probe_reader_);
  const Slice prefix = probe_reader_->PrimeFirstBlock();
  if (probe_reader_->hasReadError()) {
    return Result::kReadError;
  }
  switch (DetectWalFormat(prefix, buffered_)) {
    case WalFormat::kPending:
      return Result::kPending;
    case WalFormat::kLegacy:
      probe_reader_ = nullptr;
      *reader = std::move(candidate_reader_);
      break;
    case WalFormat::kBlog:
      *reader = std::make_unique<BlogWalReader>(
          probe_reader_->ReleaseFile(), prefix.ToString(), reporter_, checksum_,
          log_number_, track_and_verify_wals_, stop_replay_for_corruption_,
          min_wal_number_to_keep_, observed_predecessor_wal_info_, buffered_);
      probe_reader_ = nullptr;
      candidate_reader_.reset();
      break;
  }
  return Result::kReady;
}

std::unique_ptr<WalReader> NewWalReader(
    std::shared_ptr<Logger> info_log,
    std::unique_ptr<SequentialFileReader>&& file,
    log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
    bool track_and_verify_wals, bool stop_replay_for_corruption,
    uint64_t min_wal_number_to_keep,
    const PredecessorWALInfo& observed_predecessor_wal_info, bool buffered,
    std::unique_ptr<WalReaderOpenState>* pending_open) {
  assert(pending_open);
  *pending_open = std::make_unique<WalReaderOpenState>(
      info_log, std::move(file), reporter, checksum, log_number,
      track_and_verify_wals, stop_replay_for_corruption, min_wal_number_to_keep,
      observed_predecessor_wal_info, buffered);
  std::unique_ptr<WalReader> reader;
  if ((*pending_open)->TryOpen(&reader) == WalReaderOpenState::Result::kReady) {
    pending_open->reset();
  }
  return reader;
}

Status IsBlogFormatWalFile(const std::shared_ptr<FileSystem>& fs,
                           const std::string& fname,
                           const FileOptions& file_options, bool* is_blog) {
  assert(is_blog != nullptr);
  std::unique_ptr<SequentialFileReader> file;
  Status s = SequentialFileReader::Create(fs, fname, file_options, &file,
                                          nullptr /* dbg */,
                                          nullptr /* rate_limiter */);
  if (!s.ok()) {
    return s;
  }
  char buf[kBlogFileMagicSize];
  Slice prefix;
  s = file->Read(kBlogFileMagicSize, &prefix, buf, Env::IO_TOTAL);
  if (!s.ok()) {
    return s;
  }
  *is_blog = BlogFileHeader::IsBlogFormat(prefix.data(), prefix.size());
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
