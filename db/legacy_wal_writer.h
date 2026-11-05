//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "db/log_writer.h"
#include "db/wal_writer.h"
#include "rocksdb/compression_type.h"

namespace ROCKSDB_NAMESPACE {

// WalWriter implementation for the legacy WAL format.
class LegacyWalWriter final : public WalWriter {
 public:
  explicit LegacyWalWriter(std::unique_ptr<WritableFileWriter>&& dest,
                           uint64_t log_number, bool recycle_log_files,
                           bool manual_flush, CompressionType compression_type,
                           bool track_and_verify_wals,
                           size_t initial_block_offset);
  ~LegacyWalWriter() override = default;

  using WalWriter::AddRecord;
  IOStatus AddRecord(const WriteOptions& write_options, const Slice& record,
                     SequenceNumber sequence) override;
  IOStatus AddCompressionTypeRecord(const WriteOptions& write_options) override;
  IOStatus MaybeAddPredecessorWALInfo(
      const WriteOptions& write_options,
      const PredecessorWALInfo& predecessor_wal_info) override;
  IOStatus MaybeAddUserDefinedTimestampSizeRecord(
      const WriteOptions& write_options,
      const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz) override;

  WritableFileWriter* file() override;
  const WritableFileWriter* file() const override;
  uint64_t get_log_number() const override;

  IOStatus WriteBuffer(const WriteOptions& write_options) override;
  IOStatus Close(const WriteOptions& write_options) override;
  bool PublishIfClosed() override;
  bool BufferIsEmpty() override;
  SequenceNumber GetLastSeqnoRecorded() const override;

 private:
  log::Writer writer_;
};

}  // namespace ROCKSDB_NAMESPACE
