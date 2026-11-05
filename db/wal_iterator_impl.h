//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <map>
#include <memory>
#include <vector>

#include "db/version_set.h"
#include "db/wal_reader.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "rocksdb/wal_iterator.h"

namespace ROCKSDB_NAMESPACE {

// A file-scoped indeterminate WAL append can leave a complete physical record
// that is not part of the database after successful live resumption. Keep the
// end sequence of each such write group for the lifetime of the open DB so both
// existing and newly created WAL iterators stop before exposing that record.
struct WalRecoverySequenceGap {
  uint64_t wal_number;
  SequenceNumber start_sequence;
  SequenceNumber end_sequence;
  uint64_t generation;
};

class WalRecoverySequenceTracker {
 public:
  void Record(uint64_t wal_number, SequenceNumber start_sequence,
              SequenceNumber end_sequence);
  bool FirstAtOrAfter(SequenceNumber sequence,
                      WalRecoverySequenceGap* recovery_gap) const;
  uint64_t CurrentGeneration() const;

 private:
  mutable port::Mutex mutex_;
  // TODO: Bound DB-lifetime growth by pruning gaps only after their WALs are no
  // longer retained and no existing iterator snapshot can still observe them.
  // WAL purge alone is insufficient because iterators share this tracker.
  std::map<SequenceNumber, WalRecoverySequenceGap> gaps_by_end_sequence_;
  uint64_t generation_ = 0;
};

class WalFileImpl : public WalFile {
 public:
  WalFileImpl(uint64_t logNum, WalFileType logType, SequenceNumber startSeq,
              bool startSeqIsPlaceholder, uint64_t sizeBytes)
      : logNumber_(logNum),
        type_(logType),
        startSequenceIsPlaceholder_(startSeqIsPlaceholder),
        startSequence_(startSeq),
        sizeFileBytes_(sizeBytes) {}

  std::string PathName() const override {
    if (type_ == kArchivedLogFile) {
      return ArchivedLogFileName("", logNumber_);
    }
    return LogFileName("", logNumber_);
  }

  uint64_t LogNumber() const override { return logNumber_; }

  WalFileType Type() const override { return type_; }

  SequenceNumber StartSequence() const override { return startSequence_; }

  // True when the WAL holds no WriteBatch records yet, so StartSequence() is
  // only a nonzero marker that keeps the file listed, not a real sequence.
  bool StartSequenceIsPlaceholder() const {
    return startSequenceIsPlaceholder_;
  }

  uint64_t SizeFileBytes() const override { return sizeFileBytes_; }

  bool operator<(const WalFile& that) const {
    return LogNumber() < that.LogNumber();
  }

 private:
  uint64_t logNumber_;
  WalFileType type_;
  bool startSequenceIsPlaceholder_;
  SequenceNumber startSequence_;
  uint64_t sizeFileBytes_;
};

class WalIteratorImpl : public WalIterator {
 public:
  WalIteratorImpl(
      const std::string& dir, const ImmutableDBOptions* options,
      const WalIterator::ReadOptions& read_options, const EnvOptions& soptions,
      const SequenceNumber seqNum, std::unique_ptr<VectorWalPtr> files,
      VersionSet const* const versions, const bool seq_per_batch,
      const std::shared_ptr<IOTracer>& io_tracer,
      std::shared_ptr<WalRecoverySequenceTracker> recovery_sequence_tracker,
      uint64_t recovery_sequence_generation);

  bool Valid() override;

  void Next() override;

  Status status() override;

  BatchResult GetBatch() override;

 private:
  const std::string& dir_;
  const ImmutableDBOptions* options_;
  const WalIterator::ReadOptions read_options_;
  const EnvOptions& soptions_;
  SequenceNumber starting_sequence_number_;
  std::unique_ptr<VectorWalPtr> files_;
  // Used only to get latest seq. num
  // TODO(icanadi) can this be just a callback?
  VersionSet const* const versions_;
  std::shared_ptr<IOTracer> io_tracer_;
  std::shared_ptr<WalRecoverySequenceTracker> recovery_sequence_tracker_;
  uint64_t recovery_sequence_generation_;

  // State variables
  bool started_;
  bool is_valid_;  // not valid when it starts of.
  // The pending open hit a read error, which it latches; a re-seek must open
  // the file afresh rather than reuse it.
  bool pending_log_reader_read_error_ = false;
  // Once this is set to a non-OK status the iterator is spent: Next() is a
  // no-op and the status never changes again.
  Status current_status_;
  size_t current_file_index_;
  std::unique_ptr<WriteBatch> current_batch_;
  std::unique_ptr<WalReader> current_log_reader_;
  std::unique_ptr<WalReaderOpenState> pending_log_reader_;
  std::string scratch_;
  Status OpenLogFile(const WalFile* log_file,
                     std::unique_ptr<SequentialFileReader>* file);

  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    void Corruption(size_t bytes, const Status& s,
                    uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      ROCKS_LOG_ERROR(info_log, "dropping %" ROCKSDB_PRIszt " bytes; %s", bytes,
                      s.ToString().c_str());
    }
    void ToleratedCorruption(
        size_t bytes, const Status& s,
        uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      ROCKS_LOG_WARN(info_log,
                     "dropping %" ROCKSDB_PRIszt " bytes (tolerated); %s",
                     bytes, s.ToString().c_str());
    }
    virtual void Info(const char* s) { ROCKS_LOG_INFO(info_log, "%s", s); }
  } reporter_;

  SequenceNumber
      current_batch_seq_;  // sequence number at start of current batch
  SequenceNumber current_last_seq_;  // last sequence in the current batch
  // Reads from transaction log only if the writebatch record has been written
  bool RestrictedRead(Slice* record);
  // Seeks to starting_sequence_number_ reading from start_file_index in files_.
  // If strict is set, then must get a batch starting with
  // starting_sequence_number_.
  void SeekToStartSequence(uint64_t start_file_index = 0, bool strict = false);
  // Implementation of Next. SeekToStartSequence calls it internally with
  // internal=true to let it find next entry even if it has to jump gaps because
  // the iterator may start off from the first available entry but promises to
  // be continuous after that
  void NextImpl(bool internal = false);
  // Check if batch is expected, else return false
  bool IsBatchExpected(const WriteBatch* batch, SequenceNumber expected_seq);
  // Update current batch if a continuous batch is found.
  void UpdateCurrentWriteBatch(const Slice& record);
  // End the run before a batch that contains or crosses a sequence reserved
  // after an indeterminate WAL write.
  bool StopBeforeRecoveryGap(uint64_t wal_number,
                             SequenceNumber batch_last_sequence);
  Status OpenLogReader(const WalFile* file);
  bool TryOpenLogReader();
};

}  // namespace ROCKSDB_NAMESPACE
