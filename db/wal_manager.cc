//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/wal_manager.h"

#include <algorithm>
#include <cinttypes>
#include <memory>
#include <vector>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/wal_iterator_impl.h"
#include "db/write_batch_internal.h"
#include "file/file_util.h"
#include "file/filename.h"
#include "file/sequence_file_reader.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

WalManager::WalManager(const ImmutableDBOptions& db_options,
                       const FileOptions& file_options,
                       const std::shared_ptr<IOTracer>& io_tracer,
                       const bool seq_per_batch)
    : db_options_(db_options),
      file_options_(file_options),
      env_(db_options.env),
      fs_(db_options.fs, io_tracer),
      purge_wal_files_last_run_(0),
      seq_per_batch_(seq_per_batch),
      wal_dir_(db_options_.GetWalDir()),
      wal_in_db_path_(db_options_.IsWalDirSameAsDBPath()),
      io_tracer_(io_tracer),
      recovery_sequence_tracker_(
          std::make_shared<WalRecoverySequenceTracker>()) {}

Status WalManager::DeleteFile(const std::string& fname, uint64_t number) {
  auto s = env_->DeleteFile(wal_dir_ + "/" + fname);
  if (s.ok()) {
    MutexLock l(&read_first_record_cache_mutex_);
    read_first_record_cache_.erase(number);
  }
  return s;
}
Status WalManager::GetSortedWalFiles(VectorWalPtr& files, bool need_seqnos,
                                     bool include_archived) {
  // First get sorted files in db dir, then get sorted files from archived
  // dir, to avoid a race condition where a log file is moved to archived
  // dir in between.
  Status s;
  // list wal files in main db dir.
  VectorWalPtr logs;
  s = GetSortedWalsOfType(wal_dir_, logs, kAliveLogFile, need_seqnos);

  if (!include_archived || !s.ok()) {
    return s;
  }

  // Reproduce the race condition where a log file is moved
  // to archived dir, between these two sync points, used in
  // (DBWalIteratorTest,Race)
  TEST_SYNC_POINT("WalManager::GetSortedWalFiles:1");
  TEST_SYNC_POINT("WalManager::GetSortedWalFiles:2");

  files.clear();
  // list wal files in archive dir.
  std::string archivedir = ArchivalDirectory(wal_dir_);
  Status exists = env_->FileExists(archivedir);
  if (exists.ok()) {
    s = GetSortedWalsOfType(archivedir, files, kArchivedLogFile, need_seqnos);
    if (!s.ok()) {
      return s;
    }
  } else if (!exists.IsNotFound()) {
    assert(s.ok());
    return exists;
  }

  uint64_t latest_archived_log_number = 0;
  if (!files.empty()) {
    latest_archived_log_number = files.back()->LogNumber();
    ROCKS_LOG_INFO(db_options_.info_log, "Latest Archived log: %" PRIu64,
                   latest_archived_log_number);
  }

  files.reserve(files.size() + logs.size());
  for (auto& log : logs) {
    if (log->LogNumber() > latest_archived_log_number) {
      files.push_back(std::move(log));
    } else {
      // When the race condition happens, we could see the
      // same log in both db dir and archived dir. Simply
      // ignore the one in db dir. Note that, if we read
      // archived dir first, we would have missed the log file.
      ROCKS_LOG_WARN(db_options_.info_log, "%s already moved to archive",
                     log->PathName().c_str());
    }
  }

  return s;
}

Status WalManager::GetUpdatesSince(SequenceNumber seq,
                                   std::unique_ptr<WalIterator>* iter,
                                   const WalIterator::ReadOptions& read_options,
                                   VersionSet* version_set) {
  if (seq_per_batch_) {
    return Status::NotSupported();
  }

  assert(!seq_per_batch_);

  //  Get all sorted Wal Files.
  //  Do binary search and open files and find the seq number.

  const uint64_t recovery_sequence_generation =
      recovery_sequence_tracker_->CurrentGeneration();
  std::unique_ptr<VectorWalPtr> wal_files(new VectorWalPtr);
  Status s = GetSortedWalFiles(*wal_files);
  if (!s.ok()) {
    return s;
  }

  std::sort(std::begin(*wal_files), std::end(*wal_files),
            [](auto& lhs, auto& rhs) {
              return lhs->StartSequence() > rhs->StartSequence();
            });

  s = RetainProbableWalFiles(*wal_files, seq);
  if (!s.ok()) {
    return s;
  }
  iter->reset(new WalIteratorImpl(
      wal_dir_, &db_options_, read_options, file_options_, seq,
      std::move(wal_files), version_set, seq_per_batch_, io_tracer_,
      recovery_sequence_tracker_, recovery_sequence_generation));
  return (*iter)->status();
}

void WalManager::RecordIndeterminateWriteSequence(uint64_t wal_number,
                                                  SequenceNumber start_sequence,
                                                  SequenceNumber end_sequence) {
  recovery_sequence_tracker_->Record(wal_number, start_sequence, end_sequence);
}

// 1. Go through all archived files and
//    a. if ttl is enabled, delete outdated files
//    b. if archive size limit is enabled, delete empty files,
//        compute file number and size.
// 2. If size limit is enabled:
//    a. compute how many files should be deleted
//    b. get sorted non-empty archived logs
//    c. delete what should be deleted
void WalManager::PurgeObsoleteWALFiles() {
  bool const ttl_enabled = db_options_.WAL_ttl_seconds > 0;
  bool const size_limit_enabled = db_options_.WAL_size_limit_MB > 0;
  if (!ttl_enabled && !size_limit_enabled) {
    return;
  }

  int64_t current_time = 0;
  Status s = db_options_.clock->GetCurrentTime(&current_time);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log, "Can't get current time: %s",
                    s.ToString().c_str());
    assert(false);
    return;
  }
  uint64_t const now_seconds = static_cast<uint64_t>(current_time);
  uint64_t const time_to_check =
      ttl_enabled
          ? std::min(kDefaultIntervalToDeleteObsoleteWAL,
                     std::max(uint64_t{1}, db_options_.WAL_ttl_seconds / 2))
          : kDefaultIntervalToDeleteObsoleteWAL;
  uint64_t old_last_run_time = purge_wal_files_last_run_.LoadRelaxed();
  do {
    if (old_last_run_time + time_to_check > now_seconds) {
      // last run is recent enough, no need to purge
      return;
    }
  } while (!purge_wal_files_last_run_.CasWeakRelaxed(
      /*expected=*/old_last_run_time, /*desired=*/now_seconds));

  std::string archival_dir = ArchivalDirectory(wal_dir_);
  std::vector<std::string> files;
  s = env_->GetChildren(archival_dir, &files);
  if (!s.ok()) {
    ROCKS_LOG_ERROR(db_options_.info_log, "Can't get archive files: %s",
                    s.ToString().c_str());
    return;
  }

  size_t log_files_num = 0;
  uint64_t log_file_size = 0;
  for (auto& f : files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kWalFile) {
      std::string const file_path = archival_dir + "/" + f;
      if (ttl_enabled) {
        uint64_t file_m_time;
        s = env_->GetFileModificationTime(file_path, &file_m_time);
        if (!s.ok()) {
          ROCKS_LOG_WARN(db_options_.info_log,
                         "Can't get file mod time: %s: %s", file_path.c_str(),
                         s.ToString().c_str());
          continue;
        }

        // Avoid expression `now_seconds - file_m_time` when
        // `file_m_time > now_seconds` to prevent unsigned underflow in case
        // system clock goes backwards. Both timestamps are based on wall clock
        // time, which is not guaranteed to be monotonic.
        if (file_m_time <= now_seconds &&
            now_seconds - file_m_time > db_options_.WAL_ttl_seconds) {
          s = DeleteDBFile(&db_options_, file_path, archival_dir, false,
                           /*force_fg=*/!wal_in_db_path_);
          if (!s.ok()) {
            ROCKS_LOG_WARN(db_options_.info_log, "Can't delete file: %s: %s",
                           file_path.c_str(), s.ToString().c_str());
            continue;
          } else {
            MutexLock l(&read_first_record_cache_mutex_);
            read_first_record_cache_.erase(number);
          }
          continue;
        }
      }

      if (size_limit_enabled) {
        uint64_t file_size;
        s = env_->GetFileSize(file_path, &file_size);
        if (!s.ok()) {
          ROCKS_LOG_ERROR(db_options_.info_log,
                          "Unable to get file size: %s: %s", file_path.c_str(),
                          s.ToString().c_str());
          return;
        } else {
          if (file_size > 0) {
            log_file_size = std::max(log_file_size, file_size);
            ++log_files_num;
          } else {
            s = DeleteDBFile(&db_options_, file_path, archival_dir, false,
                             /*force_fg=*/!wal_in_db_path_);
            if (!s.ok()) {
              ROCKS_LOG_WARN(db_options_.info_log,
                             "Unable to delete file: %s: %s", file_path.c_str(),
                             s.ToString().c_str());
              continue;
            } else {
              MutexLock l(&read_first_record_cache_mutex_);
              read_first_record_cache_.erase(number);
            }
          }
        }
      }
    }
  }

  if (0 == log_files_num || !size_limit_enabled) {
    return;
  }

  size_t const files_keep_num = static_cast<size_t>(
      db_options_.WAL_size_limit_MB * 1024 * 1024 / log_file_size);
  if (log_files_num <= files_keep_num) {
    return;
  }

  size_t files_del_num = log_files_num - files_keep_num;
  VectorWalPtr archived_logs;
  s = GetSortedWalsOfType(archival_dir, archived_logs, kArchivedLogFile,
                          /*need_seqno=*/false);
  if (!s.ok()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Unable to get archived WALs from: %s: %s",
                   archival_dir.c_str(), s.ToString().c_str());
    files_del_num = 0;
  } else if (files_del_num > archived_logs.size()) {
    ROCKS_LOG_WARN(db_options_.info_log,
                   "Trying to delete more archived log files than "
                   "exist. Deleting all");
    files_del_num = archived_logs.size();
  }

  for (size_t i = 0; i < files_del_num; ++i) {
    std::string const file_path = archived_logs[i]->PathName();
    s = DeleteDBFile(&db_options_, wal_dir_ + "/" + file_path, wal_dir_, false,
                     /*force_fg=*/!wal_in_db_path_);
    if (!s.ok()) {
      ROCKS_LOG_WARN(db_options_.info_log, "Unable to delete file: %s: %s",
                     file_path.c_str(), s.ToString().c_str());
      continue;
    } else {
      MutexLock l(&read_first_record_cache_mutex_);
      read_first_record_cache_.erase(archived_logs[i]->LogNumber());
    }
  }
}

void WalManager::ArchiveWALFile(const std::string& fname, uint64_t number) {
  auto archived_log_name = ArchivedLogFileName(wal_dir_, number);
  // The sync point below is used in
  // (DBWalIteratorTest,Race)
  TEST_SYNC_POINT("WalManager::PurgeObsoleteFiles:1");
  Status s = env_->RenameFile(fname, archived_log_name);
  IGNORE_STATUS_IF_ERROR(s);
  // The sync point below is used in
  // (DBWalIteratorTest,Race)
  TEST_SYNC_POINT("WalManager::PurgeObsoleteFiles:2");
  // The sync point below is used in
  // (CheckPointTest, CheckpointWithArchievedLog)
  TEST_SYNC_POINT("WalManager::ArchiveWALFile");
  ROCKS_LOG_INFO(db_options_.info_log, "Move log file %s to %s -- %s\n",
                 fname.c_str(), archived_log_name.c_str(),
                 s.ToString().c_str());
}

Status WalManager::GetSortedWalsOfType(const std::string& path,
                                       VectorWalPtr& log_files,
                                       WalFileType log_type, bool need_seqnos) {
  std::vector<std::string> all_files;
  const Status status = env_->GetChildren(path, &all_files);
  if (!status.ok()) {
    return status;
  }
  log_files.reserve(all_files.size());
  for (const auto& f : all_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(f, &number, &type) && type == kWalFile) {
      SequenceNumber sequence;
      if (need_seqnos) {
        Status s = ReadFirstRecord(log_type, number, &sequence);
        if (!s.ok()) {
          return s;
        }
        if (sequence == 0) {
          // empty file
          continue;
        }
      } else {
        sequence = 0;
      }

      // Reproduce the race condition where a log file is moved
      // to archived dir, between these two sync points, used in
      // (DBWalIteratorTest,Race)
      TEST_SYNC_POINT("WalManager::GetSortedWalsOfType:1");
      TEST_SYNC_POINT("WalManager::GetSortedWalsOfType:2");

      uint64_t size_bytes;
      Status s = env_->GetFileSize(LogFileName(path, number), &size_bytes);
      // re-try in case the alive log file has been moved to archive.
      if (!s.ok() && log_type == kAliveLogFile) {
        std::string archived_file = ArchivedLogFileName(path, number);
        if (env_->FileExists(archived_file).ok()) {
          s = env_->GetFileSize(archived_file, &size_bytes);
          if (!s.ok() && env_->FileExists(archived_file).IsNotFound()) {
            // oops, the file just got deleted from archived dir! move on
            s = Status::OK();
            continue;
          }
        }
      }
      if (!s.ok()) {
        return s;
      }

      log_files.emplace_back(
          new WalFileImpl(number, log_type, sequence, size_bytes));
    }
  }
  std::sort(
      log_files.begin(), log_files.end(),
      [](const std::unique_ptr<WalFile>& a, const std::unique_ptr<WalFile>& b) {
        WalFileImpl* a_impl = static_cast_with_check<WalFileImpl>(a.get());
        WalFileImpl* b_impl = static_cast_with_check<WalFileImpl>(b.get());
        return *a_impl < *b_impl;
      });
  return status;
}

Status WalManager::RetainProbableWalFiles(VectorWalPtr& all_logs,
                                          const SequenceNumber target) {
  int64_t start = 0;  // signed to avoid overflow when target is < first file.
  int64_t end = static_cast<int64_t>(all_logs.size()) - 1;
  // Binary Search. avoid opening all files.
  while (end >= start) {
    int64_t mid = start + (end - start) / 2;  // Avoid overflow.
    SequenceNumber current_seq_num =
        all_logs.at(static_cast<size_t>(mid))->StartSequence();
    if (current_seq_num == target) {
      end = mid;
      break;
    } else if (current_seq_num < target) {
      start = mid + 1;
    } else {
      end = mid - 1;
    }
  }
  // end could be -ve.
  size_t start_index =
      static_cast<size_t>(std::max(static_cast<int64_t>(0), end));
  // The last wal file is always included
  all_logs.erase(all_logs.begin(), all_logs.begin() + start_index);
  return Status::OK();
}

Status WalManager::ReadFirstRecord(const WalFileType type,
                                   const uint64_t number,
                                   SequenceNumber* sequence) {
  *sequence = 0;
  if (type != kAliveLogFile && type != kArchivedLogFile) {
    ROCKS_LOG_ERROR(db_options_.info_log, "[WalManger] Unknown file type %s",
                    std::to_string(type).c_str());
    return Status::NotSupported("File Type Not Known " + std::to_string(type));
  }
  {
    MutexLock l(&read_first_record_cache_mutex_);
    auto itr = read_first_record_cache_.find(number);
    if (itr != read_first_record_cache_.end()) {
      *sequence = itr->second;
      return Status::OK();
    }
  }
  Status s;
  if (type == kAliveLogFile) {
    std::string fname = LogFileName(wal_dir_, number);
    s = ReadFirstLine(fname, number, sequence);
    if (!s.ok() && env_->FileExists(fname).ok()) {
      // return any error that is not caused by non-existing file
      return s;
    }
  }

  if (type == kArchivedLogFile || !s.ok()) {
    //  check if the file got moved to archive.
    std::string archived_file = ArchivedLogFileName(wal_dir_, number);
    s = ReadFirstLine(archived_file, number, sequence);
    // maybe the file was deleted from archive dir. If that's the case, return
    // Status::OK(). The caller with identify this as empty file because
    // *sequence == 0
    if (!s.ok() && env_->FileExists(archived_file).IsNotFound()) {
      return Status::OK();
    }
  }

  if (s.ok() && *sequence != 0) {
    MutexLock l(&read_first_record_cache_mutex_);
    read_first_record_cache_.insert({number, *sequence});
  }
  return s;
}

Status WalManager::GetLiveWalFile(uint64_t number,
                                  std::unique_ptr<WalFile>* log_file) {
  if (!log_file) {
    return Status::InvalidArgument("log_file not preallocated.");
  }

  if (!number) {
    return Status::PathNotFound("log file not available");
  }

  Status s;

  uint64_t size_bytes;
  s = env_->GetFileSize(LogFileName(wal_dir_, number), &size_bytes);

  if (!s.ok()) {
    return s;
  }

  log_file->reset(new WalFileImpl(number, kAliveLogFile,
                                  0,  // SequenceNumber
                                  size_bytes));

  return Status::OK();
}

// the function returns status.ok() and sequence == 0 if the file exists, but is
// empty
Status WalManager::ReadFirstLine(const std::string& fname,
                                 const uint64_t number,
                                 SequenceNumber* sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;

    Status* status;
    bool ignore_error;  // true if db_options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s,
                    uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      ROCKS_LOG_WARN(info_log, "[WalManager] %s%s: dropping %d bytes; %s",
                     (this->ignore_error ? "(ignoring error) " : ""), fname,
                     static_cast<int>(bytes), s.ToString().c_str());
      if (this->status->ok()) {
        // only keep the first error
        *this->status = s;
      }
    }
  };

  std::unique_ptr<FSSequentialFile> file;
  Status status = fs_->NewSequentialFile(
      fname, fs_->OptimizeForLogRead(file_options_), &file, nullptr);
  std::unique_ptr<SequentialFileReader> file_reader(
      new SequentialFileReader(std::move(file), fname, io_tracer_));

  if (!status.ok()) {
    return status;
  }

  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = db_options_.info_log.get();
  reporter.fname = fname.c_str();
  reporter.status = &status;
  reporter.ignore_error = !db_options_.paranoid_checks;
  log::Reader reader(db_options_.info_log, std::move(file_reader), &reporter,
                     true /*checksum*/, number);
  std::string scratch;
  Slice record;

  if (reader.ReadRecord(&record, &scratch) &&
      (status.ok() || !db_options_.paranoid_checks)) {
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      // TODO read record's till the first no corrupt entry?
    } else {
      WriteBatch batch;
      // We can overwrite an existing non-OK Status since it'd only reach here
      // with `paranoid_checks == false`.
      status = WriteBatchInternal::SetContents(&batch, record);
      if (status.ok()) {
        *sequence = WriteBatchInternal::Sequence(&batch);
        return status;
      }
    }
  }

  if (status.ok() && reader.IsCompressedAndEmptyFile()) {
    // In case of wal_compression, it writes a `kSetCompressionType` record
    // which is not associated with any sequence number. As result for an empty
    // file, GetSortedWalsOfType() will skip these WALs causing the operations
    // to fail.
    // Therefore, in order to avoid that failure, it sets sequence_number to 1
    // indicating those WALs should be included.
    *sequence = 1;
  } else {
    // ReadRecord might have returned false on EOF, which means that the log
    // file is empty. Or, a failure may have occurred while processing the first
    // entry. In any case, return status and set sequence number to 0.
    *sequence = 0;
  }
  return status;
}

}  // namespace ROCKSDB_NAMESPACE
