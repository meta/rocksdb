//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/legacy_wal_reader.h"

namespace ROCKSDB_NAMESPACE {

template <typename LogReaderType>
bool LegacyWalReaderImpl<LogReaderType>::ReadRecord(
    Slice* record, std::string* scratch, WALRecoveryMode wal_recovery_mode,
    uint64_t* record_checksum) {
  return reader_.ReadRecord(record, scratch, wal_recovery_mode,
                            record_checksum);
}

template <typename LogReaderType>
void LegacyWalReaderImpl<LogReaderType>::UnmarkEOF() {
  reader_.UnmarkEOF();
}

template <typename LogReaderType>
bool LegacyWalReaderImpl<LogReaderType>::IsCompressedAndEmptyFile() {
  return reader_.IsCompressedAndEmptyFile();
}

template <typename LogReaderType>
const UnorderedMap<uint32_t, size_t>&
LegacyWalReaderImpl<LogReaderType>::GetRecordedTimestampSize() const {
  return reader_.GetRecordedTimestampSize();
}

template <typename LogReaderType>
uint64_t LegacyWalReaderImpl<LogReaderType>::GetVerificationSize(
    uint64_t physical_file_size) const {
  return physical_file_size;
}

template <typename LogReaderType>
uint64_t LegacyWalReaderImpl<LogReaderType>::LastRecordOffset() {
  return reader_.LastRecordOffset();
}

template <typename LogReaderType>
bool LegacyWalReaderImpl<LogReaderType>::IsEOF() {
  return reader_.IsEOF();
}

template <typename LogReaderType>
uint64_t LegacyWalReaderImpl<LogReaderType>::GetLogNumber() const {
  return reader_.GetLogNumber();
}

template <typename LogReaderType>
log::Reader::Reporter* LegacyWalReaderImpl<LogReaderType>::GetReporter() const {
  return reader_.GetReporter();
}

template class LegacyWalReaderImpl<log::Reader>;
template class LegacyWalReaderImpl<log::FragmentBufferedReader>;

}  // namespace ROCKSDB_NAMESPACE
