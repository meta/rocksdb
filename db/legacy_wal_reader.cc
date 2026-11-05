//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/legacy_wal_reader.h"

#include <utility>

namespace ROCKSDB_NAMESPACE {

LegacyWalReader::LegacyWalReader(std::unique_ptr<log::Reader>&& reader)
    : reader_(std::move(reader)) {}

bool LegacyWalReader::ReadRecord(Slice* record, std::string* scratch,
                                 WALRecoveryMode wal_recovery_mode,
                                 uint64_t* record_checksum) {
  return reader_->ReadRecord(record, scratch, wal_recovery_mode,
                             record_checksum);
}

void LegacyWalReader::UnmarkEOF() { reader_->UnmarkEOF(); }

bool LegacyWalReader::IsCompressedAndEmptyFile() {
  return reader_->IsCompressedAndEmptyFile();
}

const UnorderedMap<uint32_t, size_t>&
LegacyWalReader::GetRecordedTimestampSize() const {
  return reader_->GetRecordedTimestampSize();
}

uint64_t LegacyWalReader::LastRecordOffset() {
  return reader_->LastRecordOffset();
}

bool LegacyWalReader::IsEOF() { return reader_->IsEOF(); }

uint64_t LegacyWalReader::GetLogNumber() const {
  return reader_->GetLogNumber();
}

log::Reader::Reporter* LegacyWalReader::GetReporter() const {
  return reader_->GetReporter();
}

}  // namespace ROCKSDB_NAMESPACE
