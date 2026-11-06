//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "db/log_reader.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class Logger;
class PredecessorWALInfo;
class SequentialFileReader;

// Interface for reading RocksDB WAL records.
// Implementations own their file format's record framing; callers select an
// implementation per file through NewWalReader and then use this interface,
// so no caller branches on the file format.
class WalReader {
 public:
  virtual ~WalReader() = default;

  WalReader(const WalReader&) = delete;
  WalReader& operator=(const WalReader&) = delete;

  // Overloads match log::Reader's trailing arguments. They are separate
  // overloads rather than defaulted parameters so a future signature change
  // cannot silently change meaning at existing call sites.
  bool ReadRecord(Slice* record, std::string* scratch) {
    return ReadRecord(record, scratch,
                      WALRecoveryMode::kTolerateCorruptedTailRecords, nullptr);
  }
  bool ReadRecord(Slice* record, std::string* scratch,
                  WALRecoveryMode wal_recovery_mode) {
    return ReadRecord(record, scratch, wal_recovery_mode, nullptr);
  }
  virtual bool ReadRecord(Slice* record, std::string* scratch,
                          WALRecoveryMode wal_recovery_mode,
                          uint64_t* record_checksum) = 0;

  virtual void UnmarkEOF() = 0;
  // True when no data record has been read yet but the file is not
  // physically empty: a legacy compression preamble, or a blog file header
  // (compressed or not).
  virtual bool IsCompressedAndEmptyFile() = 0;
  virtual const UnorderedMap<uint32_t, size_t>& GetRecordedTimestampSize()
      const = 0;
  // After replay, returns the size used to verify this WAL as a predecessor.
  // `physical_file_size` is the caller's file-size observation.
  virtual uint64_t GetVerificationSize(uint64_t physical_file_size) const = 0;
  virtual uint64_t LastRecordOffset() = 0;
  virtual bool IsEOF() = 0;
  virtual uint64_t GetLogNumber() const = 0;
  virtual log::Reader::Reporter* GetReporter() const = 0;

 protected:
  WalReader() = default;
};

// Retains the open file and unconsumed prefix until its format is known.
// This is opening state, not a record reader. Retry after the file grows;
// once ready, discard this state and use the returned WalReader directly.
class WalReaderOpenState {
 public:
  enum class Result { kReady, kPending, kReadError };

  WalReaderOpenState(std::shared_ptr<Logger> info_log,
                     std::unique_ptr<SequentialFileReader>&& file,
                     log::Reader::Reporter* reporter, bool checksum,
                     uint64_t log_number, bool track_and_verify_wals,
                     bool stop_replay_for_corruption,
                     uint64_t min_wal_number_to_keep,
                     const PredecessorWALInfo& observed_predecessor_wal_info,
                     bool buffered);

  // Reads through the normal file-read path, including reporter callbacks.
  // `reader` must be empty. A read error is terminal for this open file and
  // is reported through the reporter; retries do not repeat the failed I/O.
  Result TryOpen(std::unique_ptr<WalReader>* reader);

 private:
  std::unique_ptr<WalReader> candidate_reader_;
  // Borrows the candidate's embedded decoder only while opening.
  log::Reader* probe_reader_ = nullptr;
  log::Reader::Reporter* const reporter_;
  const bool checksum_;
  const uint64_t log_number_;
  const bool track_and_verify_wals_;
  const bool stop_replay_for_corruption_;
  const uint64_t min_wal_number_to_keep_;
  const PredecessorWALInfo observed_predecessor_wal_info_;
  const bool buffered_;
};

// Attempts to open a format-specific reader. If the file is empty, or has
// a partial magic prefix in buffered mode, returns null and retains the open
// state in `pending_open`. Read errors also return null with retained state;
// existing reporter policy determines how the caller handles the error.
// On success `pending_open` is empty. `buffered` selects fragment buffering,
// not whether the caller may subsequently tail a growing file.
std::unique_ptr<WalReader> NewWalReader(
    std::shared_ptr<Logger> info_log,
    std::unique_ptr<SequentialFileReader>&& file,
    log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
    bool track_and_verify_wals, bool stop_replay_for_corruption,
    uint64_t min_wal_number_to_keep,
    const PredecessorWALInfo& observed_predecessor_wal_info, bool buffered,
    std::unique_ptr<WalReaderOpenState>* pending_open);

// Sets *is_blog to whether the WAL file `fname` is in the blog format. Reads
// the file separately, for callers that must choose a read mode before the
// first WalReader::ReadRecord detects the format.
Status IsBlogFormatWalFile(const std::shared_ptr<FileSystem>& fs,
                           const std::string& fname,
                           const FileOptions& file_options, bool* is_blog);

}  // namespace ROCKSDB_NAMESPACE
