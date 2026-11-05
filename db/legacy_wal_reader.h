//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "db/log_reader.h"
#include "db/wal_reader.h"
#include "rocksdb/slice.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

// Adapts an embedded normal or fragment-buffered legacy decoder to WalReader.
template <typename LogReaderType>
class LegacyWalReaderImpl final : public WalReader {
  static_assert(std::is_same_v<LogReaderType, log::Reader> ||
                std::is_same_v<LogReaderType, log::FragmentBufferedReader>);

 public:
  ~LegacyWalReaderImpl() override = default;

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
  friend class WalReaderOpenState;

  explicit LegacyWalReaderImpl(std::shared_ptr<Logger> info_log,
                               std::unique_ptr<SequentialFileReader>&& file,
                               log::Reader::Reporter* reporter, bool checksum,
                               uint64_t log_number)
    requires std::is_same_v<LogReaderType, log::FragmentBufferedReader>
      : reader_(std::move(info_log), std::move(file), reporter, checksum,
                log_number) {}

  explicit LegacyWalReaderImpl(
      std::shared_ptr<Logger> info_log,
      std::unique_ptr<SequentialFileReader>&& file,
      log::Reader::Reporter* reporter, bool checksum, uint64_t log_number,
      bool track_and_verify_wals, bool stop_replay_for_corruption,
      uint64_t min_wal_number_to_keep,
      const PredecessorWALInfo& observed_predecessor_wal_info)
    requires std::is_same_v<LogReaderType, log::Reader>
      : reader_(std::move(info_log), std::move(file), reporter, checksum,
                log_number, track_and_verify_wals, stop_replay_for_corruption,
                min_wal_number_to_keep, observed_predecessor_wal_info) {}

  LogReaderType reader_;
};

using LegacyWalReader = LegacyWalReaderImpl<log::Reader>;
using BufferedLegacyWalReader =
    LegacyWalReaderImpl<log::FragmentBufferedReader>;

}  // namespace ROCKSDB_NAMESPACE
