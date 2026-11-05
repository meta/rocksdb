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
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
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
  // True when the file declares compression but no data record has been
  // read yet (a compression preamble with an otherwise empty file).
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

// Creates the WalReader implementation matching the WAL file's format.
// `buffered` selects the fragment-buffering legacy reader used when tailing
// a WAL that is still being written.
std::unique_ptr<WalReader> NewWalReader(
    std::shared_ptr<Logger> info_log,
    std::unique_ptr<SequentialFileReader>&& file,
    log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
    bool track_and_verify_wals, bool stop_replay_for_corruption,
    uint64_t min_wal_number_to_keep,
    const PredecessorWALInfo& observed_predecessor_wal_info, bool buffered);

}  // namespace ROCKSDB_NAMESPACE
