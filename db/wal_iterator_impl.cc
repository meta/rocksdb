//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wal_iterator_impl.h"

#include <cinttypes>

#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"
#include "monitoring/statistics_impl.h"
#include "util/defer.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

void WalRecoverySequenceTracker::Record(uint64_t wal_number,
                                        SequenceNumber start_sequence,
                                        SequenceNumber end_sequence) {
  assert(wal_number != 0);
  assert(start_sequence <= end_sequence);
  MutexLock lock(&mutex_);
  auto [it, inserted] = gaps_by_end_sequence_.emplace(
      end_sequence, WalRecoverySequenceGap{wal_number, start_sequence,
                                           end_sequence, generation_ + 1});
  if (inserted) {
    ++generation_;
  } else {
    assert(it->second.wal_number == wal_number);
    assert(it->second.start_sequence == start_sequence);
  }
}

bool WalRecoverySequenceTracker::FirstAtOrAfter(
    SequenceNumber sequence, WalRecoverySequenceGap* recovery_gap) const {
  assert(recovery_gap != nullptr);
  MutexLock lock(&mutex_);
  auto it = gaps_by_end_sequence_.lower_bound(sequence);
  if (it == gaps_by_end_sequence_.end()) {
    return false;
  }
  *recovery_gap = it->second;
  return true;
}

uint64_t WalRecoverySequenceTracker::CurrentGeneration() const {
  MutexLock lock(&mutex_);
  return generation_;
}

WalIteratorImpl::WalIteratorImpl(
    const std::string& dir, const ImmutableDBOptions* options,
    const WalIterator::ReadOptions& read_options, const EnvOptions& soptions,
    const SequenceNumber seq, std::unique_ptr<VectorWalPtr> files,
    VersionSet const* const versions, [[maybe_unused]] const bool seq_per_batch,
    const std::shared_ptr<IOTracer>& io_tracer,
    std::shared_ptr<WalRecoverySequenceTracker> recovery_sequence_tracker,
    uint64_t recovery_sequence_generation,
    NextWalForTailFn next_wal_for_tail_fn)
    : dir_(dir),
      options_(options),
      read_options_(read_options),
      soptions_(soptions),
      starting_sequence_number_(seq),
      files_(std::move(files)),
      versions_(versions),
      io_tracer_(io_tracer),
      recovery_sequence_tracker_(std::move(recovery_sequence_tracker)),
      recovery_sequence_generation_(recovery_sequence_generation),
      next_wal_for_tail_fn_(std::move(next_wal_for_tail_fn)),
      started_(false),
      is_valid_(false),
      current_file_index_(0),
      current_batch_seq_(0),
      current_last_seq_(0) {
  assert(files_ != nullptr);
  assert(versions_ != nullptr);
  assert(recovery_sequence_tracker_ != nullptr);
  // WalManager::GetUpdatesSince() rejects seq_per_batch before we get here, so
  // one update always consumes exactly one sequence number below.
  assert(!seq_per_batch);
  current_status_.PermitUncheckedError();  // Clear on start
  reporter_.info_log = options_->info_log.get();
  SeekToStartSequence();  // Seek till starting sequence
}

Status WalIteratorImpl::OpenLogFile(
    const WalFile* log_file,
    std::unique_ptr<SequentialFileReader>* file_reader) {
  TEST_SYNC_POINT("WalIteratorImpl::OpenLogFile:BeforeOpen");
  FileSystemPtr fs(options_->fs, io_tracer_);
  std::unique_ptr<FSSequentialFile> file;
  std::string fname;
  Status s;
  EnvOptions optimized_env_options = fs->OptimizeForLogRead(soptions_);
  if (log_file->Type() == kArchivedLogFile) {
    fname = ArchivedLogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
  } else {
    fname = LogFileName(dir_, log_file->LogNumber());
    s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
    if (!s.ok()) {
      // Custom file systems may report a missing file as a generic I/O error.
      // Preserve the open error if the live file still exists; otherwise it
      // may have moved to the archive.
      if (fs->FileExists(fname, IOOptions(), nullptr).ok()) {
        return s;
      }
      fname = ArchivedLogFileName(dir_, log_file->LogNumber());
      s = fs->NewSequentialFile(fname, optimized_env_options, &file, nullptr);
    }
  }
  if (s.ok()) {
    file_reader->reset(new SequentialFileReader(std::move(file), fname,
                                                io_tracer_, options_->listeners,
                                                options_->rate_limiter.get()));
  }
  return s;
}

BatchResult WalIteratorImpl::GetBatch() {
  assert(is_valid_);  //  cannot call in a non valid state.
  BatchResult result;
  result.sequence = current_batch_seq_;
  result.writeBatchPtr = std::move(current_batch_);
  return result;
}

Status WalIteratorImpl::status() { return current_status_; }

bool WalIteratorImpl::Valid() { return started_ && is_valid_; }

bool WalIteratorImpl::RestrictedRead(Slice* record, Status* read_status) {
  // Don't read if no more complete entries to read from logs
  if (current_last_seq_ >= versions_->LastSequence()) {
    return false;
  }
  reporter_.read_status = read_status;
  const bool read = current_log_reader_->ReadRecord(record, &scratch_);
  reporter_.read_status = nullptr;
  return read;
}

void WalIteratorImpl::SeekToStartSequence(uint64_t start_file_index,
                                          bool strict) {
  Slice record;
  started_ = false;
  is_valid_ = false;
  // Check invariant of WalIterator when SeekToStartSequence() succeeds.
  const Defer defer([this]() {
    if (is_valid_) {
      assert(current_status_.ok());
      if (starting_sequence_number_ > current_batch_seq_) {
        assert(current_batch_seq_ < current_last_seq_);
        assert(current_last_seq_ >= starting_sequence_number_);
      }
    }
  });
  if (files_->size() <= start_file_index) {
    StopBeforeRecoveryGap(/*wal_number=*/0, kMaxSequenceNumber);
    return;
  } else if (!current_status_.ok()) {
    // Already spent; see the comment on current_status_.
    return;
  }
  Status s =
      OpenLogReader(files_->at(static_cast<size_t>(start_file_index)).get());
  if (!s.ok()) {
    current_status_ = s;
    reporter_.Info(current_status_.ToString().c_str());
    return;
  }
  while (RestrictedRead(&record, nullptr)) {
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter_.Corruption(record.size(),
                           Status::Corruption("very small log record"));
      continue;
    }
    UpdateCurrentWriteBatch(record);
    if (!current_status_.ok()) {
      return;
    }
    if (current_last_seq_ >= starting_sequence_number_) {
      if (strict && current_batch_seq_ != starting_sequence_number_) {
        current_status_ = Status::Corruption(
            "Gap in sequence number. Could not "
            "seek to required sequence number");
        reporter_.Info(current_status_.ToString().c_str());
        return;
      } else if (strict) {
        reporter_.Info(
            "Could seek required sequence number. Iterator will "
            "continue.");
      }
      is_valid_ = true;
      started_ = true;  // set started_ as we could seek till starting sequence
      return;
    } else {
      is_valid_ = false;
    }
  }

  // Could not find start sequence in first file. Normally this must be the
  // only file. Otherwise log the error and let the iterator return next entry
  // If strict is set, we want to seek exactly till the start sequence and it
  // should have been present in the file we scanned above
  if (strict) {
    current_status_ = Status::Corruption(
        "Gap in sequence number. Could not "
        "seek to required sequence number");
    reporter_.Info(current_status_.ToString().c_str());
  } else if (files_->size() != 1) {
    current_status_ = Status::Corruption(
        "Start sequence was not found, "
        "skipping to the next available");
    reporter_.Info(current_status_.ToString().c_str());
    // Let NextImpl find the next available entry. started_ remains false
    // because we don't want to check for gaps while moving to start sequence
    NextImpl(true);
  } else {
    StopBeforeRecoveryGap(files_->back()->LogNumber(), kMaxSequenceNumber);
  }
}

void WalIteratorImpl::Next() {
  if (!current_status_.ok()) {
    // Spent; the run ended and cannot be resumed. See WalIterator docs.
    return;
  }
  return NextImpl(false);
}

void WalIteratorImpl::NextImpl(bool internal) {
  Slice record;
  uint64_t tail_rotations = 0;
  is_valid_ = false;
  if (!internal && !started_) {
    // Runs every time until we can seek to the start sequence
    SeekToStartSequence();
  }
  while (true) {
    assert(current_log_reader_);
    if (current_log_reader_->IsEOF()) {
      current_log_reader_->UnmarkEOF();
    }
    while (true) {
      Status read_status;
      const bool read =
          RestrictedRead(&record, tail_rotations > 0 ? &read_status : nullptr);
      // Report errors while finding the first batch in a discovered WAL.
      // Existing snapshot reads keep their error policy.
      if (!read_status.ok() && (!read || options_->paranoid_checks)) {
        current_status_ = read_status;
        return;
      }
      if (!read) {
        break;
      }
      if (record.size() < WriteBatchInternal::kHeader) {
        if (tail_rotations > 0) {
          current_status_ = Status::Corruption("very small log record");
          reporter_.Info(current_status_.ToString().c_str());
          return;
        }
        reporter_.Corruption(record.size(),
                             Status::Corruption("very small log record"));
        continue;
      } else {
        // started_ should be true if called by application
        assert(internal || started_);
        // started_ should be false if called internally
        assert(!internal || !started_);
        UpdateCurrentWriteBatch(record);
        if (is_valid_ && tail_rotations > 0) {
          RecordTick(options_->statistics.get(), WAL_ITERATOR_TAIL_ROTATIONS,
                     tail_rotations);
        }
        if (internal && !started_) {
          started_ = true;
        }
        return;
      }
    }

    // Open the next file
    if (current_file_index_ < files_->size() - 1) {
      ++current_file_index_;
      Status s = OpenLogReader(files_->at(current_file_index_).get());
      if (!s.ok()) {
        is_valid_ = false;
        current_status_ = s;
        return;
      }
    } else {
      is_valid_ = false;
      if (StopBeforeRecoveryGap(files_->back()->LogNumber(),
                                kMaxSequenceNumber)) {
        return;
      } else if (current_last_seq_ == versions_->LastSequence()) {
        // Caught up. Not an error: the caller may call Next() again later to
        // pick up writes that have not happened yet.
        current_status_ = Status::OK();
        return;
      }
      // Discover a newer WAL, then use the ordinary read path's continuity
      // and recovery-gap checks before delivering any of its records.
      if (started_ && next_wal_for_tail_fn_) {
        const uint64_t next_number =
            next_wal_for_tail_fn_(files_->back()->LogNumber());
        if (next_number != 0) {
          std::unique_ptr<WalFile> next_wal(
              new WalFileImpl(next_number, kAliveLogFile, 0, 0));
          Status open_s = OpenLogReader(next_wal.get());
          if (!open_s.ok()) {
            current_status_ =
                open_s.IsNotFound() || open_s.IsPathNotFound()
                    ? Status::TryAgain("Successor WAL is no longer available")
                    : open_s;
            return;
          }
          // This run only moves forward, so consumed WALs are never revisited.
          files_->clear();
          files_->push_back(std::move(next_wal));
          current_file_index_ = 0;
          ++tail_rotations;
          continue;
        }
      }
      // The DB has moved on but this iterator's set of WAL files is
      // exhausted, typically because the WAL was rotated after the file
      // list was collected. The caller must build a new iterator.
      current_status_ =
          Status::TryAgain("Create a new iterator to fetch the new tail.");
      return;
    }
  }
}

bool WalIteratorImpl::IsBatchExpected(const WriteBatch* batch,
                                      const SequenceNumber expected_seq) {
  assert(batch);
  SequenceNumber batchSeq = WriteBatchInternal::Sequence(batch);
  if (batchSeq != expected_seq) {
    std::ostringstream oss;
    oss << "Discontinuity in log records. " << "Got seq=" << batchSeq << ", "
        << "Expected seq=" << expected_seq << ", "
        << "Last flushed seq=" << versions_->LastSequence() << ".";

    reporter_.Info(oss.str().c_str());
    return false;
  }
  return true;
}

void WalIteratorImpl::UpdateCurrentWriteBatch(const Slice& record) {
  std::unique_ptr<WriteBatch> batch(new WriteBatch());
  Status s = WriteBatchInternal::SetContents(batch.get(), record);
  s.PermitUncheckedError();  // TODO: What should we do with this error?

  const SequenceNumber batch_sequence =
      WriteBatchInternal::Sequence(batch.get());
  const SequenceNumber batch_last_sequence =
      batch_sequence + WriteBatchInternal::Count(batch.get()) - 1;
  SequenceNumber expected_seq = current_last_seq_ + 1;
  // If the iterator has started, then confirm that we get continuous batches
  if (started_ && !IsBatchExpected(batch.get(), expected_seq)) {
    // A run is contiguous, so a discontinuity ends it. Note that this is a
    // normal consequence of writes that bypass the WAL (WriteOptions::
    // disableWAL, IngestExternalFile()), not necessarily of data loss.
    is_valid_ = false;
    current_status_ = Status::NotFound("Gap in sequence numbers");
    return;
  }
  if (StopBeforeRecoveryGap(files_->at(current_file_index_)->LogNumber(),
                            batch_last_sequence)) {
    return;
  }

  current_batch_seq_ = batch_sequence;
  current_last_seq_ = batch_last_sequence;
  // currentBatchSeq_ can only change here
  assert(current_last_seq_ <= versions_->LastSequence());

  current_batch_ = std::move(batch);
  is_valid_ = true;
  current_status_ = Status::OK();
}

bool WalIteratorImpl::StopBeforeRecoveryGap(
    uint64_t wal_number, SequenceNumber batch_last_sequence) {
  WalRecoverySequenceGap recovery_gap{};
  if (!recovery_sequence_tracker_->FirstAtOrAfter(starting_sequence_number_,
                                                  &recovery_gap)) {
    return false;
  }

  const bool gap_known_when_iterator_created =
      recovery_gap.generation <= recovery_sequence_generation_;
  const bool reached_gap = batch_last_sequence >= recovery_gap.start_sequence &&
                           (gap_known_when_iterator_created ||
                            wal_number >= recovery_gap.wal_number);
  if (!reached_gap) {
    return false;
  }

  is_valid_ = false;
  current_status_ = Status::NotFound(
      "Gap in sequence numbers after indeterminate WAL write through "
      "sequence " +
      std::to_string(recovery_gap.end_sequence));
  reporter_.Info(current_status_.ToString().c_str());
  return true;
}

Status WalIteratorImpl::OpenLogReader(const WalFile* log_file) {
  std::unique_ptr<SequentialFileReader> file;
  Status s = OpenLogFile(log_file, &file);
  if (!s.ok()) {
    return s;
  }
  assert(file);
  current_log_reader_.reset(
      new log::Reader(options_->info_log, std::move(file), &reporter_,
                      read_options_.verify_checksums_, log_file->LogNumber()));
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
