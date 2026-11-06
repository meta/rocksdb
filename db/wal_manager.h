//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/version_set.h"
#include "file/file_util.h"
#include "options/db_options.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"
#include "rocksdb/wal_iterator.h"
#include "util/atomic.h"

namespace ROCKSDB_NAMESPACE {

class WalRecoverySequenceTracker;

// WAL manager provides the abstraction for reading the WAL files as a single
// unit. Internally, it opens and reads the files using Reader or Writer
// abstraction.
class WalManager {
 public:
  WalManager(const ImmutableDBOptions& db_options,
             const FileOptions& file_options,
             const std::shared_ptr<IOTracer>& io_tracer,
             const bool seq_per_batch = false);

  Status GetSortedWalFiles(VectorWalPtr& files, bool need_seqnos = true,
                           bool include_archived = true);

  // Allow user to tail the WAL to find all recent changes to the
  // database that are newer than `seq_number`.
  Status GetUpdatesSince(SequenceNumber seq_number,
                         std::unique_ptr<WalIterator>* iter,
                         const WalIterator::ReadOptions& read_options,
                         VersionSet* version_set);

  // Record the end of a write group's sequence range when its WAL append has
  // an indeterminate outcome. TransactionLogIterator uses this process-lifetime
  // fence to avoid returning a physically complete record that successful
  // in-process recovery deliberately abandoned.
  void RecordIndeterminateWriteSequence(uint64_t wal_number,
                                        SequenceNumber start_sequence,
                                        SequenceNumber end_sequence);

  void PurgeObsoleteWALFiles();

  void ArchiveWALFile(const std::string& fname, uint64_t number);

  Status DeleteFile(const std::string& fname, uint64_t number);

  Status GetLiveWalFile(uint64_t number, std::unique_ptr<WalFile>* log_file);

  Status TEST_ReadFirstRecord(const WalFileType type, const uint64_t number,
                              SequenceNumber* sequence) {
    bool start_sequence_is_placeholder = false;
    return ReadFirstRecord(type, number, sequence,
                           &start_sequence_is_placeholder);
  }

  Status TEST_ReadFirstLine(const std::string& fname, const uint64_t number,
                            SequenceNumber* sequence) {
    bool start_sequence_is_placeholder = false;
    return ReadFirstLine(fname, number, sequence,
                         &start_sequence_is_placeholder);
  }

 private:
  Status GetSortedWalsOfType(const std::string& path, VectorWalPtr& log_files,
                             WalFileType type, bool need_seqnos);
  // Requires: all_logs should be sorted with earliest log file first
  // Retains all log files in all_logs which contain updates with seq no.
  // Greater Than or Equal to the requested SequenceNumber. Files whose start
  // sequence is a placeholder hold no updates and are not searched.
  Status RetainProbableWalFiles(VectorWalPtr& all_logs,
                                const SequenceNumber target);

  // ReadFirstRecord checks the read_first_record_cache_ to see if the entry
  // exists or not. If not, it will read the WAL file.
  // See ReadFirstLine for the placeholder case. A placeholder is not cached,
  // because the WAL may still gain records.
  Status ReadFirstRecord(const WalFileType type, const uint64_t number,
                         SequenceNumber* sequence,
                         bool* start_sequence_is_placeholder);

  // In case of no wal_compression, ReadFirstLine returns status.ok() and
  // sequence == 0 if the file exists, but is empty.
  // A WAL with wal_compression starts with a `kSetCompressionType` record, and
  // a blog-format WAL starts with a header; neither carries a sequence
  // number. If such a WAL has no other record, GetSortedWalsOfType() would
  // skip it, causing operations that need every WAL to fail. To include it,
  // ReadFirstLine sets sequence to 1 and *start_sequence_is_placeholder to
  // true; that sequence must not be used as a real sequence number.
  Status ReadFirstLine(const std::string& fname, const uint64_t number,
                       SequenceNumber* sequence,
                       bool* start_sequence_is_placeholder);

  // ------- state from DBImpl ------
  const ImmutableDBOptions& db_options_;
  const FileOptions file_options_;
  Env* env_;
  const FileSystemPtr fs_;

  // ------- WalManager state -------
  // cache for ReadFirstRecord() calls
  std::unordered_map<uint64_t, SequenceNumber> read_first_record_cache_;
  port::Mutex read_first_record_cache_mutex_;

  // last time when PurgeObsoleteWALFiles ran.
  RelaxedAtomic<uint64_t> purge_wal_files_last_run_;

  bool seq_per_batch_;

  const std::string& wal_dir_;

  bool wal_in_db_path_;

  // obsolete files will be deleted every this seconds if ttl deletion is
  // enabled and archive size_limit is disabled.
  static constexpr uint64_t kDefaultIntervalToDeleteObsoleteWAL = 600;

  std::shared_ptr<IOTracer> io_tracer_;

  std::shared_ptr<WalRecoverySequenceTracker> recovery_sequence_tracker_;
};

}  // namespace ROCKSDB_NAMESPACE
