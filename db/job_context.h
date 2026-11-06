//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <limits>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/log_writer.h"
#include "db/version_set.h"
#include "util/autovector.h"
#include "util/hash_containers.h"

namespace ROCKSDB_NAMESPACE {

class MemTable;
struct SuperVersion;

// The purpose of this struct is to simplify pushing work such as
// allocation/construction, de-allocation/destruction, and notifications to
// outside of holding the DB mutex.
struct SuperVersionContext {
  struct WriteStallNotification {
    WriteStallInfo write_stall_info;
    std::vector<std::shared_ptr<EventListener>> event_listeners;
  };

  autovector<SuperVersion*> superversions_to_free;
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
  autovector<WriteStallNotification> write_stall_notifications;
#endif
  std::unique_ptr<SuperVersion>
      new_superversion;  // if nullptr no new superversion

  explicit SuperVersionContext(bool create_superversion = false)
      : new_superversion(create_superversion ? new SuperVersion() : nullptr) {}

  explicit SuperVersionContext(SuperVersionContext&& other) noexcept
      : superversions_to_free(std::move(other.superversions_to_free)),
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
        write_stall_notifications(std::move(other.write_stall_notifications)),
#endif
        new_superversion(std::move(other.new_superversion)) {
  }
  // No copies
  SuperVersionContext(const SuperVersionContext& other) = delete;
  void operator=(const SuperVersionContext& other) = delete;
  void operator=(SuperVersionContext&& other) = delete;

  void NewSuperVersion() {
    new_superversion = std::unique_ptr<SuperVersion>(new SuperVersion());
  }

  inline bool HaveSomethingToDelete() const {
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
    return !superversions_to_free.empty() || !write_stall_notifications.empty();
#else
    return !superversions_to_free.empty();
#endif
  }

  void PushWriteStallNotification(WriteStallCondition old_cond,
                                  WriteStallCondition new_cond,
                                  const std::string& name,
                                  const ImmutableOptions* ioptions) {
#if !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)
    WriteStallNotification notif;
    notif.write_stall_info.cf_name = name;
    notif.write_stall_info.condition.prev = old_cond;
    notif.write_stall_info.condition.cur = new_cond;
    // copy listeners, ioptions could delete before background compaction
    //  completes
    notif.event_listeners = ioptions->listeners;
    write_stall_notifications.push_back(notif);
#else
    (void)old_cond;
    (void)new_cond;
    (void)name;
    (void)ioptions;
#endif  // !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)
  }

  void Clean() {
#if !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)
    // notify listeners on changed write stall conditions
    for (auto& notif : write_stall_notifications) {
      for (auto& listener : notif.event_listeners) {
        listener->OnStallConditionsChanged(notif.write_stall_info);
      }
    }
    write_stall_notifications.clear();
#endif
    // free superversions
    for (auto s : superversions_to_free) {
      delete s;
    }
    superversions_to_free.clear();
  }

  ~SuperVersionContext() {
#ifndef ROCKSDB_DISABLE_STALL_NOTIFICATION
    assert(write_stall_notifications.empty());
#endif
    assert(superversions_to_free.empty());
  }
};

struct JobContext {
  inline bool HaveSomethingToDelete() const {
    return !(full_scan_candidate_files.empty() && sst_delete_files.empty() &&
             blob_delete_files.empty() && log_delete_files.empty() &&
             manifest_delete_files.empty());
  }

  inline bool HaveSomethingToClean() const {
    bool sv_have_sth = false;
    for (const auto& sv_ctx : superversion_contexts) {
      if (sv_ctx.HaveSomethingToDelete()) {
        sv_have_sth = true;
        break;
      }
    }
    return memtables_to_free.size() > 0 || wals_to_free.size() > 0 ||
           job_snapshot != nullptr || sv_have_sth;
  }

  SequenceNumber GetJobSnapshotSequence() const {
    if (job_snapshot) {
      assert(job_snapshot->snapshot());
      return job_snapshot->snapshot()->GetSequenceNumber();
    }
    return kMaxSequenceNumber;
  }

  SequenceNumber GetLatestSnapshotSequence() const {
    assert(snapshot_context_initialized);
    if (snapshot_seqs.empty()) {
      return 0;
    }
    return snapshot_seqs.back();
  }

  SequenceNumber GetEarliestSnapshotSequence() const {
    assert(snapshot_context_initialized);
    if (snapshot_seqs.empty()) {
      return kMaxSequenceNumber;
    }
    return snapshot_seqs.front();
  }

  void InitSnapshotContext(SnapshotChecker* checker,
                           std::unique_ptr<ManagedSnapshot> managed_snapshot,
                           SequenceNumber earliest_write_conflict,
                           std::vector<SequenceNumber>&& snapshots) {
    if (snapshot_context_initialized) {
      return;
    }
    snapshot_context_initialized = true;
    snapshot_checker = checker;
    assert(!job_snapshot);
    job_snapshot = std::move(managed_snapshot);
    earliest_write_conflict_snapshot = earliest_write_conflict;
    snapshot_seqs = std::move(snapshots);
  }

  // Structure to store information for candidate files to delete.
  struct CandidateFileInfo {
    std::string file_name;
    std::string file_path;
    CandidateFileInfo(std::string name, std::string path)
        : file_name(std::move(name)), file_path(std::move(path)) {}
    bool operator==(const CandidateFileInfo& other) const {
      return file_name == other.file_name && file_path == other.file_path;
    }
  };

  // a list of all files that we'll consider deleting
  // (every once in a while this is filled up with all files
  // in the DB directory)
  // (filled only if we're doing full scan)
  std::vector<CandidateFileInfo> full_scan_candidate_files;

  // the list of all live sst files that cannot be deleted
  std::vector<uint64_t> sst_live;

  // the list of sst files that we need to delete
  std::vector<ObsoleteFileInfo> sst_delete_files;

  // the list of all live blob files that cannot be deleted
  std::vector<uint64_t> blob_live;

  // the list of blob files that we need to delete
  std::vector<ObsoleteBlobFileInfo> blob_delete_files;

  // a list of log files that we need to delete
  std::vector<uint64_t> log_delete_files;

  // a list of log files that we need to preserve during full purge since they
  // will be reused later
  std::vector<uint64_t> log_recycle_files;

  // Files quarantined from deletion. This list contains file numbers for files
  // that are in an ambiguous states. This includes newly generated SST files
  // and blob files from flush and compaction job whose VersionEdits' persist
  // state in Manifest are unclear. An old manifest file whose immediately
  // following new manifest file's CURRENT file creation is in an unclear state.
  // WAL logs don't have this premature deletion risk since
  // min_log_number_to_keep is only updated after successful manifest commits.
  // So this data structure doesn't track log files.
  autovector<uint64_t> files_to_quarantine;

  // Blob file numbers that PurgeObsoleteFiles must keep. This includes both
  // actively written direct-write files and sealed direct-write files that are
  // still reachable through live memtables / old SuperVersions.
  // Collected under db_mutex_ in FindObsoleteFiles so PurgeObsoleteFiles can
  // safely use the snapshot without taking DB mutex.
  UnorderedSet<uint64_t> active_blob_direct_write_files;

  // Snapshot of VersionSet's next file number taken before collecting active
  // direct-write blob files. This keeps the current purge pass from racing a
  // concurrently created blob file that was not yet part of the active set.
  uint64_t min_blob_file_number_to_keep = std::numeric_limits<uint64_t>::max();

  // a list of manifest files that we need to delete
  std::vector<std::string> manifest_delete_files;

  // a list of memtables to be free
  autovector<ReadOnlyMemTable*> memtables_to_free;

  // contexts for installing superversions for multiple column families
  std::vector<SuperVersionContext> superversion_contexts;

  autovector<log::Writer*> wals_to_free;

  // the current manifest_file_number, log_number and prev_log_number
  // that corresponds to the set of files in 'live'.
  uint64_t manifest_file_number = 0;
  uint64_t pending_manifest_file_number = 0;

  // Used for remote compaction. To prevent OPTIONS files from getting
  // purged by PurgeObsoleteFiles() of the primary host
  uint64_t min_options_file_number;
  // Exact OPTIONS file committed by the current MANIFEST. Unlike
  // min_options_file_number, this protects one authoritative file even when
  // higher-numbered uncommitted OPTIONS files exist.
  uint64_t manifest_options_file_number = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;

  uint64_t min_pending_output = 0;
  uint64_t prev_wals_total_size = 0;
  size_t num_alive_wal_files = 0;
  uint64_t size_log_to_delete = 0;

  // Snapshot taken before flush/compaction job.
  std::unique_ptr<ManagedSnapshot> job_snapshot;
  SnapshotChecker* snapshot_checker = nullptr;
  std::vector<SequenceNumber> snapshot_seqs;
  // This is the earliest snapshot that could be used for write-conflict
  // checking by a transaction.  For any user-key newer than this snapshot, we
  // should make sure not to remove evidence that a write occurred.
  SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber;

  // Unique job id
  int job_id;

  bool snapshot_context_initialized = false;

  explicit JobContext(int _job_id, bool create_superversion = false) {
    job_id = _job_id;
    superversion_contexts.emplace_back(
        SuperVersionContext(create_superversion));
  }

  // Delete the default constructor
  JobContext() = delete;

  // No copying allowed
  JobContext(const JobContext&) = delete;
  JobContext& operator=(const JobContext&) = delete;
  JobContext(JobContext&&) = delete;
  JobContext& operator=(JobContext&&) = delete;

  // For non-empty JobContext Clean() has to be called at least once before
  // before destruction (see asserts in ~JobContext()). Should be called with
  // unlocked DB mutex. Destructor doesn't call Clean() to avoid accidentally
  // doing potentially slow Clean() with locked DB mutex.
  void Clean() {
    // free superversions
    for (auto& sv_context : superversion_contexts) {
      sv_context.Clean();
    }
    // free pending memtables
    for (auto m : memtables_to_free) {
      delete m;
    }
    for (auto l : wals_to_free) {
      delete l;
    }

    memtables_to_free.clear();
    wals_to_free.clear();
    job_snapshot.reset();
  }

  ~JobContext() {
    assert(memtables_to_free.size() == 0);
    assert(wals_to_free.size() == 0);
  }
};

}  // namespace ROCKSDB_NAMESPACE
