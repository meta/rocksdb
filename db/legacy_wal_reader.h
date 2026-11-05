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
#include "db/wal_reader.h"
#include "rocksdb/slice.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

// WalReader implementation for the legacy WAL format. Holds the underlying
// reader by pointer because the buffered and unbuffered legacy readers are
// different classes selected by the factory.
class LegacyWalReader final : public WalReader {
 public:
  explicit LegacyWalReader(std::unique_ptr<log::Reader>&& reader);
  ~LegacyWalReader() override = default;

  using WalReader::ReadRecord;
  bool ReadRecord(Slice* record, std::string* scratch,
                  WALRecoveryMode wal_recovery_mode,
                  uint64_t* record_checksum) override;
  void UnmarkEOF() override;
  bool IsCompressedAndEmptyFile() override;
  const UnorderedMap<uint32_t, size_t>& GetRecordedTimestampSize()
      const override;
  uint64_t LastRecordOffset() override;
  bool IsEOF() override;
  uint64_t GetLogNumber() const override;
  log::Reader::Reporter* GetReporter() const override;

 private:
  std::unique_ptr<log::Reader> reader_;
};

}  // namespace ROCKSDB_NAMESPACE
