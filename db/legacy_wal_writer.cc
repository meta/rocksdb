//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/legacy_wal_writer.h"

#include <utility>

#include "file/writable_file_writer.h"

namespace ROCKSDB_NAMESPACE {

LegacyWalWriter::LegacyWalWriter(std::unique_ptr<WritableFileWriter>&& dest,
                                 uint64_t log_number, bool recycle_log_files,
                                 bool manual_flush,
                                 CompressionType compression_type,
                                 bool track_and_verify_wals,
                                 size_t initial_block_offset)
    : writer_(std::move(dest), log_number, recycle_log_files, manual_flush,
              compression_type, track_and_verify_wals, initial_block_offset) {}

IOStatus LegacyWalWriter::AddRecord(const WriteOptions& write_options,
                                    const Slice& record,
                                    SequenceNumber first_sequence,
                                    size_t /*num_sequences*/) {
  // The legacy format persists no sequence range; only the first sequence
  // feeds GetLastSeqnoRecorded(), matching what recovery observes (the batch
  // header sequence) for predecessor verification.
  return writer_.AddRecord(write_options, record, first_sequence);
}

IOStatus LegacyWalWriter::WriteFileStart(
    const WriteOptions& write_options,
    const PredecessorWALInfo& predecessor_wal_info) {
  IOStatus io_s = writer_.AddCompressionTypeRecord(write_options);
  if (io_s.ok()) {
    io_s =
        writer_.MaybeAddPredecessorWALInfo(write_options, predecessor_wal_info);
  }
  return io_s;
}

IOStatus LegacyWalWriter::MaybeAddUserDefinedTimestampSizeRecord(
    const WriteOptions& write_options,
    const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz) {
  return cf_to_ts_sz.empty() ? IOStatus::OK()
                             : writer_.MaybeAddUserDefinedTimestampSizeRecord(
                                   write_options, cf_to_ts_sz);
}

WritableFileWriter* LegacyWalWriter::file() { return writer_.file(); }

const WritableFileWriter* LegacyWalWriter::file() const {
  return writer_.file();
}

uint64_t LegacyWalWriter::get_log_number() const {
  return writer_.get_log_number();
}

IOStatus LegacyWalWriter::WriteBuffer(const WriteOptions& write_options) {
  return writer_.WriteBuffer(write_options);
}

IOStatus LegacyWalWriter::Close(const WriteOptions& write_options) {
  return writer_.Close(write_options);
}

bool LegacyWalWriter::PublishIfClosed() { return writer_.PublishIfClosed(); }

bool LegacyWalWriter::BufferIsEmpty() { return writer_.BufferIsEmpty(); }

SequenceNumber LegacyWalWriter::GetLastSeqnoRecorded() const {
  return writer_.GetLastSeqnoRecorded();
}

uint64_t LegacyWalWriter::GetVerificationSize() const {
  return writer_.file()->GetFileSize();
}

}  // namespace ROCKSDB_NAMESPACE
