//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <cstdint>

#include "rocksdb/io_status.h"
#include "rocksdb/types.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class PredecessorWALInfo;
class Slice;
class WritableFileWriter;
struct WriteOptions;

// Interface for writing RocksDB WAL records and managing the underlying file.
// Implementations are responsible for their file format's record framing and
// metadata records.
class WalWriter {
 public:
  virtual ~WalWriter() = default;

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  IOStatus AddRecord(const WriteOptions& write_options, const Slice& record) {
    return AddRecord(write_options, record, 0, 0);
  }
  // Appends `record` (a serialized WriteBatch for WAL use) to the file.
  //
  // `first_sequence` is the batch's header sequence number and
  // `num_sequences` is the number of sequence numbers the record consumes:
  // one per key normally, one per batch when `seq_per_batch` is set, or
  // zero when the record consumes none (e.g. an empty batch or a WAL-only
  // marker). The record's last sequence is therefore
  // `first_sequence + num_sequences - 1` when `num_sequences > 0`.
  // Records with `num_sequences == 0` contribute no sequences, so sequence
  // range tracking must skip them.
  virtual IOStatus AddRecord(const WriteOptions& write_options,
                             const Slice& record, SequenceNumber first_sequence,
                             size_t num_sequences) = 0;
  // Writes what must precede every record: the compression type and, with
  // track_and_verify_wals, `predecessor_wal_info` when it is initialized.
  // Called once, after construction and before any record. Does not sync:
  // like records, this becomes durable with the WAL's next sync.
  virtual IOStatus WriteFileStart(
      const WriteOptions& write_options,
      const PredecessorWALInfo& predecessor_wal_info) = 0;
  virtual IOStatus MaybeAddUserDefinedTimestampSizeRecord(
      const WriteOptions& write_options,
      const UnorderedMap<uint32_t, size_t>& cf_to_ts_sz) = 0;

  virtual WritableFileWriter* file() = 0;
  virtual const WritableFileWriter* file() const = 0;
  virtual uint64_t get_log_number() const = 0;

  virtual IOStatus WriteBuffer(const WriteOptions& write_options) = 0;
  // Finalizes an initialized, healthy file and closes the destination.
  // Direct destination closes can omit format-specific finalization.
  // Repeated Close() after releasing the file is a no-op returning OK.
  virtual IOStatus Close(const WriteOptions& write_options) = 0;
  // Releases the file if closed; true once released, so repeated calls are
  // safe.
  virtual bool PublishIfClosed() = 0;
  virtual bool BufferIsEmpty() = 0;
  virtual SequenceNumber GetLastSeqnoRecorded() const = 0;
  // Size recorded in the successor's PredecessorWALInfo. Blog WALs exclude
  // footer records and padding after the last body record; legacy WALs use
  // the logical file size, including buffered bytes.
  virtual uint64_t GetVerificationSize() const = 0;

 protected:
  WalWriter() = default;
};

}  // namespace ROCKSDB_NAMESPACE
