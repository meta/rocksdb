//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "db/column_family.h"
#include "db/compaction/compaction_iterator.h"
#include "db/compaction/compaction_job.h"
#include "db/error_handler.h"
#include "db/event_helpers.h"
#include "db/external_sst_file_ingestion_job.h"
#include "db/flush_job.h"
#include "db/flush_scheduler.h"
#include "db/import_column_family_job.h"
#include "db/internal_stats.h"
#include "db/log_writer.h"
#include "db/logs_with_prep_tracker.h"
#include "db/memtable_list.h"
#include "db/periodic_task_scheduler.h"
#include "db/post_memtable_callback.h"
#include "db/pre_release_callback.h"
#include "db/range_del_aggregator.h"
#include "db/read_callback.h"
#include "db/seqno_to_time_mapping.h"
#include "db/snapshot_checker.h"
#include "db/snapshot_impl.h"
#include "db/trim_history_scheduler.h"
#include "db/version_edit.h"
#include "db/wal_manager.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "logging/event_logger.h"
#include "memtable/flush_initiator.h"
#include "memtable/wbwi_memtable.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/attribute_groups.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/user_write_callback.h"
#include "rocksdb/utilities/replayer.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "rocksdb/wal_iterator.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/merging_iterator.h"
#include "util/autovector.h"
#include "util/coro_utils.h"
#include "util/hash.h"
#include "util/hash_containers.h"
#include "util/repeatable_thread.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"
#include "util/threadpool_imp.h"

#if USE_COROUTINES
#include "rocksdb/coro_db.h"
#endif

namespace ROCKSDB_NAMESPACE {

class Arena;
class ArenaWrappedDBIter;
class Cleanable;
class FileIngestionHandleImpl;
class SameFileBlobReader;
class InMemoryStatsHistoryIterator;
class MemTable;
class PersistentStatsHistoryIterator;
class TableCache;
class TaskLimiterToken;
class Version;
class VersionEdit;
class VersionSet;
class WriteCallback;
struct JobContext;
struct ExternalSstFileInfo;
struct MemTableInfo;

// Captured under the same DB mutex hold as a subset checkpoint's MANIFEST
// boundary and live CF set. This prevents the generated OPTIONS membership
// from describing a different point in time than the copied MANIFEST.
struct SubsetCheckpointOptionsSnapshot {
  uint64_t options_file_number = 0;
  bool commit_options_file_number = false;
  DBOptions db_options;
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_options;
};

// Class to maintain directories for all database paths other than main one.
class Directories {
 public:
  IOStatus SetDirectories(FileSystem* fs, const std::string& dbname,
                          const std::string& wal_dir,
                          const std::vector<DbPath>& data_paths);

  FSDirectory* GetDataDir(size_t path_id) const {
    assert(path_id < data_dirs_.size());
    FSDirectory* ret_dir = data_dirs_[path_id].get();
    if (ret_dir == nullptr) {
      // Should use db_dir_
      return db_dir_.get();
    }
    return ret_dir;
  }

  FSDirectory* GetWalDir() {
    if (wal_dir_) {
      return wal_dir_.get();
    }
    return db_dir_.get();
  }

  FSDirectory* GetDbDir() { return db_dir_.get(); }

  IOStatus Close(const IOOptions& options, IODebugContext* dbg) {
    // close all directories for all database paths
    IOStatus s = IOStatus::OK();

    // The default implementation for Close() in Directory/FSDirectory class
    // "NotSupported" status, the upper level interface should be able to
    // handle this error so that Close() does not fail after upgrading when
    // run on FileSystems that have not implemented `Directory::Close()` or
    // `FSDirectory::Close()` yet

    if (db_dir_) {
      IOStatus temp_s = db_dir_->Close(options, dbg);
      if (!temp_s.ok() && !temp_s.IsNotSupported() && s.ok()) {
        s = std::move(temp_s);
      }
    }

    // Attempt to close everything even if one fails
    s.PermitUncheckedError();

    if (wal_dir_) {
      IOStatus temp_s = wal_dir_->Close(options, dbg);
      if (!temp_s.ok() && !temp_s.IsNotSupported() && s.ok()) {
        s = std::move(temp_s);
      }
    }

    s.PermitUncheckedError();

    for (auto& data_dir_ptr : data_dirs_) {
      if (data_dir_ptr) {
        IOStatus temp_s = data_dir_ptr->Close(options, dbg);
        if (!temp_s.ok() && !temp_s.IsNotSupported() && s.ok()) {
          s = std::move(temp_s);
        }
      }
    }

    // Ready for caller
    s.MustCheck();
    return s;
  }

 private:
  std::unique_ptr<FSDirectory> db_dir_;
  std::vector<std::unique_ptr<FSDirectory>> data_dirs_;
  std::unique_ptr<FSDirectory> wal_dir_;
};

struct DBOpenLogRecordReadReporter : public log::Reader::Reporter {
  Env* env;
  Logger* info_log;
  const char* fname;
  Status* status;  // nullptr if immutable_db_options_.paranoid_checks==false
  bool* old_log_record;
  void Corruption(size_t bytes, const Status& s,
                  uint64_t log_number = kMaxSequenceNumber) override;

  void OldLogRecord(size_t bytes) override;

  uint64_t GetCorruptedLogNumber() const { return corrupted_wal_number_; }

 private:
  uint64_t corrupted_wal_number_ = kMaxSequenceNumber;
};

// While DB is the public interface of RocksDB, and DBImpl is the actual
// class implementing it. It's the entrance of the core RocksdB engine.
// All other DB implementations, e.g. TransactionDB, BlobDB, etc, wrap a
// DBImpl internally.
// Other than functions implementing the DB interface, some public
// functions are there for other internal components to call. For
// example, TransactionDB directly calls DBImpl::WriteImpl() and
// BlobDB directly calls DBImpl::GetImpl(). Some other functions
// are for sub-components to call. For example, ColumnFamilyHandleImpl
// calls DBImpl::FindObsoleteFiles().
//
// Since it's a very large class, the definition of the functions is
// divided in several db_impl_*.cc files, besides db_impl.cc.
#if USE_COROUTINES
class DBImpl : public DB,
               public CoroDB
#else
class DBImpl : public DB
#endif
{
 public:
  DBImpl(const DBOptions& options, const std::string& dbname,
         const bool seq_per_batch = false, const bool batch_per_txn = true,
         bool read_only = false);
  // No copying or moving allowed
  DBImpl(const DBImpl&) = delete;
  void operator=(const DBImpl&) = delete;
  DBImpl(DBImpl&&) = delete;
  void operator=(DBImpl&&) = delete;

  ~DBImpl() override;

  // ---- Implementations of the DB interface ----

  using DB::Resume;
  Status Resume() override;

  using DB::Put;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& value) override;
  Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
             const Slice& key, const Slice& ts, const Slice& value) override;

  using DB::PutEntity;
  Status PutEntity(const WriteOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   const WideColumns& columns) override;
  Status PutEntity(const WriteOptions& options, const Slice& key,
                   const AttributeGroups& attribute_groups) override;

  using DB::Merge;
  Status Merge(const WriteOptions& options, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& value) override;
  Status Merge(const WriteOptions& options, ColumnFamilyHandle* column_family,
               const Slice& key, const Slice& ts, const Slice& value) override;

  using DB::Delete;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key) override;
  Status Delete(const WriteOptions& options, ColumnFamilyHandle* column_family,
                const Slice& key, const Slice& ts) override;

  using DB::SingleDelete;
  Status SingleDelete(const WriteOptions& options,
                      ColumnFamilyHandle* column_family,
                      const Slice& key) override;
  Status SingleDelete(const WriteOptions& options,
                      ColumnFamilyHandle* column_family, const Slice& key,
                      const Slice& ts) override;

  using DB::DeleteRange;
  Status DeleteRange(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key) override;
  Status DeleteRange(const WriteOptions& options,
                     ColumnFamilyHandle* column_family, const Slice& begin_key,
                     const Slice& end_key, const Slice& ts) override;

  using DB::Write;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;

  using DB::WriteWithCallback;
  Status WriteWithCallback(const WriteOptions& options, WriteBatch* updates,
                           UserWriteCallback* user_write_cb) override;

  Status IngestWriteBatchWithIndex(
      const WriteOptions& options,
      std::shared_ptr<WriteBatchWithIndex> wbwi) override;

  // Returns true if any live column family currently has blob direct write
  // enabled.
  bool HasAnyBlobDirectWriteColumnFamily();

  using DB::Get;
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(Status, Get, const ReadOptions& _read_options,
                                  ColumnFamilyHandle* column_family,
                                  const Slice& key, PinnableSlice* value,
                                  std::string* timestamp);

  using DB::GetWithMetadata;
  Status GetWithMetadata(const ReadOptions& _read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableSlice* value,
                         OutputMetadata* output_metadata) override;

  using DB::GetEntity;
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(Status, GetEntity,
                                  const ReadOptions& _read_options,
                                  ColumnFamilyHandle* column_family,
                                  const Slice& key,
                                  PinnableWideColumns* columns);
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(Status, GetEntity,
                                  const ReadOptions& _read_options,
                                  const Slice& key,
                                  PinnableAttributeGroups* result);

  using DB::GetEntityWithMetadata;
  Status GetEntityWithMetadata(const ReadOptions& _read_options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableWideColumns* columns,
                               OutputMetadata* output_metadata) override;

  Status GetEntityLazy(const ReadOptions& options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       LazyWideColumns* result) override;

  using DB::GetMergeOperands;
  Status GetMergeOperands(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, const Slice& key,
                          PinnableSlice* merge_operands,
                          GetMergeOperandsOptions* get_merge_operands_options,
                          int* number_of_operands) override {
    GetImplOptions get_impl_options;
    get_impl_options.column_family = column_family;
    get_impl_options.merge_operands = merge_operands;
    get_impl_options.get_merge_operands_options = get_merge_operands_options;
    get_impl_options.number_of_operands = number_of_operands;
    get_impl_options.get_value = false;
    return GetImpl(options, key, get_impl_options);
  }

  using DB::MultiGet;
  // This MultiGet is a batched version, which may be faster than calling Get
  // multiple times, especially if the keys have some spatial locality that
  // enables them to be queried in the same SST files/set of files. The larger
  // the batch size, the more scope for batching and performance improvement
  // The values and statuses parameters are arrays with number of elements
  // equal to keys.size(). This allows the storage for those to be alloacted
  // by the caller on the stack for small batches
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(void, MultiGet,
                                  const ReadOptions& _read_options,
                                  const size_t num_keys,
                                  ColumnFamilyHandle** column_families,
                                  const Slice* keys, PinnableSlice* values,
                                  std::string* timestamps, Status* statuses,
                                  const bool sorted_input = false);

  using DB::MultiGetWithMetadata;
  void MultiGetWithMetadata(const ReadOptions& _read_options,
                            const size_t num_keys,
                            ColumnFamilyHandle* const* column_families,
                            const Slice* keys, PinnableSlice* values,
                            Status* statuses,
                            MultiGetOutputMetadata* output_metadata,
                            const bool sorted_input = false) override;

  void MultiGetWithCallback(
      const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
      ReadCallback* callback,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys);

  using DB::MultiGetEntity;

  void MultiGetEntity(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, size_t num_keys,
                      const Slice* keys, PinnableWideColumns* results,
                      Status* statuses, bool sorted_input) override;

  void MultiGetEntity(const ReadOptions& options, size_t num_keys,
                      ColumnFamilyHandle** column_families, const Slice* keys,
                      PinnableWideColumns* results, Status* statuses,
                      bool sorted_input) override;
  void MultiGetEntity(const ReadOptions& options, size_t num_keys,
                      const Slice* keys,
                      PinnableAttributeGroups* results) override;

  using DB::MultiGetEntityWithMetadata;
  void MultiGetEntityWithMetadata(const ReadOptions& _read_options,
                                  size_t num_keys,
                                  ColumnFamilyHandle* const* column_families,
                                  const Slice* keys,
                                  PinnableWideColumns* results,
                                  Status* statuses,
                                  MultiGetOutputMetadata* output_metadata,
                                  bool sorted_input = false) override;
  void MultiGetEntityWithMetadata(
      const ReadOptions& _read_options, size_t num_keys, const Slice* keys,
      PinnableAttributeGroups* results,
      MultiGetOutputMetadata* output_metadata) override;

  void MultiGetEntityLazy(const ReadOptions& options,
                          ColumnFamilyHandle* column_family, size_t num_keys,
                          const Slice* keys, LazyWideColumnsBatch* result,
                          Status* statuses, bool sorted_input) override;

  void MultiGetEntityWithCallback(
      const ReadOptions& read_options, ColumnFamilyHandle* column_family,
      ReadCallback* callback,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys);

  Status CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                            const std::string& column_family,
                            ColumnFamilyHandle** handle) override {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    return CreateColumnFamily(ReadOptions(), WriteOptions(), cf_options,
                              column_family, handle);
  }
  virtual Status CreateColumnFamily(const ReadOptions& read_options,
                                    const WriteOptions& write_options,
                                    const ColumnFamilyOptions& cf_options,
                                    const std::string& column_family,
                                    ColumnFamilyHandle** handle);
  Status CreateColumnFamilies(
      const ColumnFamilyOptions& cf_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles) override {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    return CreateColumnFamilies(ReadOptions(), WriteOptions(), cf_options,
                                column_family_names, handles);
  }
  virtual Status CreateColumnFamilies(
      const ReadOptions& read_options, const WriteOptions& write_options,
      const ColumnFamilyOptions& cf_options,
      const std::vector<std::string>& column_family_names,
      std::vector<ColumnFamilyHandle*>* handles);

  Status CreateColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles) override {
    // TODO: plumb Env::IOActivity, Env::IOPriority
    return CreateColumnFamilies(ReadOptions(), WriteOptions(), column_families,
                                handles);
  }
  virtual Status CreateColumnFamilies(
      const ReadOptions& read_options, const WriteOptions& write_options,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles);
  Status DropColumnFamily(ColumnFamilyHandle* column_family) override;
  Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  // Returns false if key doesn't exist in the database and true if it may.
  // If value_found is not passed in as null, then return the value if found in
  // memory. On return, if value was found, then value_found will be set to true
  // , otherwise false.
  using DB::KeyMayExist;
  bool KeyMayExist(const ReadOptions& options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   std::string* value, std::string* timestamp,
                   bool* value_found = nullptr) override;

  using DB::NewIterator;
  Iterator* NewIterator(const ReadOptions& _read_options,
                        ColumnFamilyHandle* column_family) override;
  Status NewIterators(const ReadOptions& _read_options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override;
  Status NewIterators(const std::vector<ReadOptions>& read_options,
                      const std::vector<ColumnFamilyHandle*>& column_families,
                      std::vector<Iterator*>* iterators) override;

  using DB::NewMultiScan;
  std::unique_ptr<MultiScan> NewMultiScan(
      const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
      const MultiScanArgs& scan_opts) override;

  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;

  std::unique_ptr<Iterator> NewCoalescingIterator(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  std::unique_ptr<AttributeGroupIterator> NewAttributeGroupIterator(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;

  // Create a timestamped snapshot. This snapshot can be shared by multiple
  // readers. If any of them uses it for write conflict checking, then
  // is_write_conflict_boundary is true. For simplicity, set it to true by
  // default.
  std::pair<Status, std::shared_ptr<const Snapshot>> CreateTimestampedSnapshot(
      SequenceNumber snapshot_seq, uint64_t ts);
  std::shared_ptr<const SnapshotImpl> GetTimestampedSnapshot(uint64_t ts) const;
  void ReleaseTimestampedSnapshotsOlderThan(
      uint64_t ts, size_t* remaining_total_ss = nullptr);
  Status GetTimestampedSnapshots(uint64_t ts_lb, uint64_t ts_ub,
                                 std::vector<std::shared_ptr<const Snapshot>>&
                                     timestamped_snapshots) const;

  using DB::GetProperty;
  bool GetProperty(ColumnFamilyHandle* column_family, const Slice& property,
                   std::string* value) override;
  using DB::GetMapProperty;
  bool GetMapProperty(ColumnFamilyHandle* column_family, const Slice& property,
                      std::map<std::string, std::string>* value) override;
  using DB::GetIntProperty;
  bool GetIntProperty(ColumnFamilyHandle* column_family, const Slice& property,
                      uint64_t* value) override;
  using DB::GetAggregatedIntProperty;
  bool GetAggregatedIntProperty(const Slice& property,
                                uint64_t* aggregated_value) override;
  using DB::GetApproximateSizes;
  Status GetApproximateSizes(const SizeApproximationOptions& options,
                             ColumnFamilyHandle* column_family,
                             const Range* range, int n,
                             uint64_t* sizes) override;
  using DB::GetApproximateMemTableStats;
  void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                   const Range& range, uint64_t* const count,
                                   uint64_t* const size) override;
  using DB::CompactRange;
  Status CompactRange(const CompactRangeOptions& options,
                      ColumnFamilyHandle* column_family, const Slice* begin,
                      const Slice* end) override;

  using DB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1,
      std::vector<std::string>* const output_file_names = nullptr,
      CompactionJobInfo* compaction_job_info = nullptr) override;

  Status PauseBackgroundWork() override;
  Status ContinueBackgroundWork() override;

  Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override;

  void EnableManualCompaction() override;
  void DisableManualCompaction() override;
  void AbortAllCompactions() override;
  void ResumeAllCompactions() override;
  void AbortCompactions(ColumnFamilyHandle* column_family) override;
  void ResumeCompactions(ColumnFamilyHandle* column_family) override;

  using DB::SetOptions;
  Status SetOptions(
      const std::unordered_map<ColumnFamilyHandle*,
                               std::unordered_map<std::string, std::string>>&
          column_families_opts_map) override;

  Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& options_map) override;

  using DB::NumberLevels;
  int NumberLevels(ColumnFamilyHandle* column_family) override;
  using DB::Level0StopWriteTrigger;
  int Level0StopWriteTrigger(ColumnFamilyHandle* column_family) override;
  const std::string& GetName() const override;
  Env* GetEnv() const override;
  FileSystem* GetFileSystem() const override;
  using DB::GetOptions;
  Options GetOptions(ColumnFamilyHandle* column_family) const override;
  using DB::GetDBOptions;
  DBOptions GetDBOptions() const override;
  using DB::Flush;
  Status Flush(const FlushOptions& options,
               ColumnFamilyHandle* column_family) override;
  Status Flush(
      const FlushOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families) override;
  Status FlushWAL(bool sync) override {
    FlushWALOptions options;
    options.sync = sync;
    return FlushWAL(options);
  }

  Status FlushWAL(const FlushWALOptions& options) override;

  virtual Status FlushWAL(const WriteOptions& write_options, bool sync);
  bool WALBufferIsEmpty();
  Status SyncWAL() override;
  Status LockWAL() override;
  Status UnlockWAL() override;

  SequenceNumber GetLatestSequenceNumber() const override;

  // IncreaseFullHistoryTsLow(ColumnFamilyHandle*, std::string) will acquire
  // and release db_mutex
  Status IncreaseFullHistoryTsLow(ColumnFamilyHandle* column_family,
                                  std::string ts_low) override;

  // GetFullHistoryTsLow(ColumnFamilyHandle*, std::string*) will acquire and
  // release db_mutex
  Status GetFullHistoryTsLow(ColumnFamilyHandle* column_family,
                             std::string* ts_low) override;

  Status GetNewestUserDefinedTimestamp(ColumnFamilyHandle* column_family,
                                       std::string* newest_timestamp) override;

  Status GetDbIdentity(std::string& identity) const override;

  virtual Status GetDbIdentityFromIdentityFile(const IOOptions& opts,
                                               std::string* identity) const;

  Status GetDbSessionId(std::string& session_id) const override;

  ColumnFamilyHandle* DefaultColumnFamily() const override;

  ColumnFamilyHandle* PersistentStatsColumnFamily() const;

  Status Close() override;

  Status DisableFileDeletions() override;

  Status EnableFileDeletions() override;

  virtual bool IsFileDeletionsEnabled() const;

  Status GetStatsHistory(
      uint64_t start_time, uint64_t end_time,
      std::unique_ptr<StatsHistoryIterator>* stats_iterator) override;

  using DB::ResetStats;
  Status ResetStats() override;
  // All the returned filenames start with "/"
  Status GetLiveFiles(std::vector<std::string>&, uint64_t* manifest_file_size,
                      bool flush_memtable = true) override;
  Status GetSortedWalFiles(VectorWalPtr& files) override;
  Status GetSortedWalFilesImpl(VectorWalPtr& files, bool need_seqnos);

  // Get the known flushed sizes of WALs that might still be written to
  // or have pending sync.
  // NOTE: unlike alive_wal_files_, this function includes WALs that might
  // be obsolete (but not obsolete to a pending Checkpoint) and not yet fully
  // synced.
  Status GetOpenWalSizes(std::map<uint64_t, uint64_t>& number_to_size);
  Status GetCurrentWalFile(std::unique_ptr<WalFile>* current_wal_file) override;
  Status GetCreationTimeOfOldestFile(uint64_t* creation_time) override;

  Status GetUpdatesSince(SequenceNumber seq_number,
                         std::unique_ptr<WalIterator>* iter,
                         const WalIterator::ReadOptions& read_options =
                             WalIterator::ReadOptions()) override;
  Status DeleteFilesInRanges(ColumnFamilyHandle* column_family,
                             const RangeOpt* ranges, size_t n,
                             bool include_end = true);

  void GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) override;

  Status GetLiveFilesChecksumInfo(FileChecksumList* checksum_list) override;

  Status GetLiveFilesStorageInfo(
      const LiveFilesStorageInfoOptions& opts,
      std::vector<LiveFileStorageInfo>* files) override;

  // Variant of GetLiveFilesStorageInfo used by subset-CF Checkpoint. Only
  // table/blob files of the given column families are captured; the rest are
  // reported in excluded_cf_ids so the caller can reconcile the copied
  // MANIFEST (typically via AppendColumnFamilyDropsToManifest below). Caller
  // must supply a non-empty include_cf_ids containing the default CF id and a
  // non-null excluded_cf_ids; both are internal-contract preconditions.
  Status GetLiveFilesStorageInfoForSubsetCheckpoint(
      const LiveFilesStorageInfoOptions& opts,
      const std::vector<uint32_t>& include_cf_ids,
      std::vector<LiveFileStorageInfo>* files,
      std::vector<uint32_t>* excluded_cf_ids,
      SubsetCheckpointOptionsSnapshot* options_snapshot);

  // Shared body of GetLiveFilesStorageInfo and its subset-checkpoint variant.
  // include_cf_ids empty -> no filter; otherwise records skipped CF ids in
  // *excluded_cf_ids (which must be non-null in that case).
  Status GetLiveFilesStorageInfoImpl(
      const LiveFilesStorageInfoOptions& opts,
      const std::vector<uint32_t>& include_cf_ids,
      std::vector<LiveFileStorageInfo>* files,
      std::vector<uint32_t>* excluded_cf_ids,
      SubsetCheckpointOptionsSnapshot* options_snapshot);

  // Appends kColumnFamilyDrop records to the MANIFEST file at manifest_path for
  // the given column family ids. Used by Checkpoint to make a subset-CF
  // checkpoint's copied MANIFEST consistent with the reduced file set. Operates
  // only on the given file; does not mutate this DB's live state.
  Status AppendColumnFamilyDropsToManifest(
      const std::string& manifest_path, uint64_t manifest_size,
      const std::vector<uint32_t>& cf_ids,
      uint64_t committed_options_file_number);

  // Writes a previously captured subset-specific OPTIONS snapshot into the
  // checkpoint. A zero file number is a no-op for a legacy source DB.
  Status CreateOptionsFileForSubsetCheckpoint(
      const std::string& checkpoint_dir,
      const SubsetCheckpointOptionsSnapshot& options_snapshot);

  Status GetPreparedFileInfoForExternalSstIngestion(
      const std::string& file_path,
      std::shared_ptr<const PreparedFileInfo>* file_info) override;

  // Obtains the meta data of the specified column family of the DB.
  // TODO(yhchiang): output parameter is placed in the end in this codebase.
  void GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                               ColumnFamilyMetaData* metadata) override;

  // Get column family metadata with filtering based on key range and level
  void GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                               const GetColumnFamilyMetaDataOptions& options,
                               ColumnFamilyMetaData* metadata) override;

  void GetAllColumnFamilyMetaData(
      std::vector<ColumnFamilyMetaData>* metadata) override;

  Status SuggestCompactRange(ColumnFamilyHandle* column_family,
                             const Slice* begin, const Slice* end) override;

  Status PromoteL0(ColumnFamilyHandle* column_family,
                   int target_level) override;

  using DB::IngestExternalFile;
  Status IngestExternalFile(
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& external_files,
      const IngestExternalFileOptions& ingestion_options) override;

  using DB::IngestExternalFiles;
  Status IngestExternalFiles(
      const std::vector<IngestExternalFileArg>& args) override;

  using DB::PrepareFileIngestion;
  Status PrepareFileIngestion(
      const std::vector<IngestExternalFileArg>& args,
      std::unique_ptr<FileIngestionHandle>* handle) override;

  Status CommitFileIngestionHandles(
      std::vector<std::unique_ptr<FileIngestionHandle>> handles) override;

  using DB::CreateColumnFamilyWithImport;
  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& options, const std::string& column_family_name,
      const ImportColumnFamilyOptions& import_options,
      const std::vector<const ExportImportFilesMetaData*>& metadatas,
      ColumnFamilyHandle** handle) override;

  using DB::ClipColumnFamily;
  Status ClipColumnFamily(ColumnFamilyHandle* column_family,
                          const Slice& begin_key,
                          const Slice& end_key) override;

  using DB::VerifyFileChecksums;
  Status VerifyFileChecksums(const ReadOptions& read_options) override;

  using DB::VerifyChecksum;
  Status VerifyChecksum(const ReadOptions& /*read_options*/) override;
  // Verify the checksums of files in db. Currently only tables are checked.
  //
  // read_options: controls file I/O behavior, e.g. read ahead size while
  //               reading all the live table files.
  //
  // use_file_checksum: if false, verify the block checksums of all live table
  //                    in db. Otherwise, obtain the file checksums and compare
  //                    with the MANIFEST. Currently, file checksums are
  //                    recomputed by reading all table files.
  //
  // Returns: OK if there is no file whose file or block checksum mismatches.
  Status VerifyChecksumInternal(const ReadOptions& read_options,
                                bool use_file_checksum);

  Status VerifyFullFileChecksum(const std::string& file_checksum_expected,
                                const std::string& func_name_expected,
                                const std::string& fpath,
                                const ReadOptions& read_options);

  using DB::StartTrace;
  Status StartTrace(const TraceOptions& options,
                    std::unique_ptr<TraceWriter>&& trace_writer) override;

  using DB::EndTrace;
  Status EndTrace() override;

  using DB::NewDefaultReplayer;
  Status NewDefaultReplayer(const std::vector<ColumnFamilyHandle*>& handles,
                            std::unique_ptr<TraceReader>&& reader,
                            std::unique_ptr<Replayer>* replayer) override;

  using DB::StartBlockCacheTrace;
  Status StartBlockCacheTrace(
      const TraceOptions& trace_options,
      std::unique_ptr<TraceWriter>&& trace_writer) override;

  Status StartBlockCacheTrace(
      const BlockCacheTraceOptions& options,
      std::unique_ptr<BlockCacheTraceWriter>&& trace_writer) override;

  using DB::EndBlockCacheTrace;
  Status EndBlockCacheTrace() override;

  using DB::StartIOTrace;
  Status StartIOTrace(const TraceOptions& options,
                      std::unique_ptr<TraceWriter>&& trace_writer) override;

  using DB::EndIOTrace;
  Status EndIOTrace() override;

  using DB::GetPropertiesOfAllTables;
  Status GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                  TablePropertiesCollection* props) override;
  Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      TablePropertiesCollection* props) override;

  Status GetPropertiesOfTablesByLevel(
      ColumnFamilyHandle* column_family,
      std::vector<std::unique_ptr<TablePropertiesCollection>>* props_by_level)
      override;

  // ---- End of implementations of the DB interface ----
  SystemClock* GetSystemClock() const;

  struct GetImplOptions {
    ColumnFamilyHandle* column_family = nullptr;
    PinnableSlice* value = nullptr;
    PinnableWideColumns* columns = nullptr;
    std::string* timestamp = nullptr;
    bool* newer_version_present = nullptr;
    bool* value_found = nullptr;
    ReadCallback* callback = nullptr;
    bool* is_blob_index = nullptr;
    // If true return value associated with key via value pointer else return
    // all merge operands for key via merge_operands pointer
    bool get_value = true;
    // Pointer to an array of size
    // get_merge_operands_options.expected_max_number_of_operands allocated by
    // user
    PinnableSlice* merge_operands = nullptr;
    GetMergeOperandsOptions* get_merge_operands_options = nullptr;
    int* number_of_operands = nullptr;

    // Lazy wide-column mode (used by GetEntityLazy / MultiGetEntityLazy). When
    // `lazy_columns_pin` is non-null, the SST point-lookup path leaves a
    // wide-column entity's blob references *unresolved* (see
    // GetContext::SaveWideColumnEntityToColumns), so the caller can resolve
    // them on demand later. On a successful lookup GetImpl then:
    //   - sets `*lazy_columns_version` to the Version the entity was read from
    //     (the resolution context for blob references), and
    //   - transfers an extra SuperVersion reference by registering a cleanup on
    //     `*lazy_columns_pin`, so the result may outlive this call (as an
    //     iterator's result does).
    // The SameFileBlobReader for the SST that held the entity (if any) is
    // stored in `*lazy_columns_same_file_reader` by the read path, so same-file
    // ("embedded") references stay resolvable later. All three must be set
    // together. `columns` must also be set (the entity output).
    Cleanable* lazy_columns_pin = nullptr;
    const Version** lazy_columns_version = nullptr;
    const SameFileBlobReader** lazy_columns_same_file_reader = nullptr;

    // Batched lazy read (MultiGetEntityLazy). When `lazy_columns_shared_sv` is
    // non-null, GetImpl reads against this externally-owned SuperVersion at
    // `lazy_columns_snapshot_seq` (the consistent per-batch sequence number)
    // instead of acquiring its own SuperVersion and deriving its own sequence
    // number, and it does NOT transfer a per-key pin -- the batch holds one
    // shared SuperVersion pin per column family. `lazy_columns_pin` must then
    // be null; `lazy_columns_version` / `lazy_columns_same_file_reader` are
    // still filled. (Single-key GetEntityLazy leaves this null and uses
    // `lazy_columns_pin`.)
    SuperVersion* lazy_columns_shared_sv = nullptr;
    SequenceNumber lazy_columns_snapshot_seq = kMaxSequenceNumber;
  };

  DECLARE_SYNC_AND_ASYNC(Status, GetImpl, const ReadOptions& read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableSlice* value);

  DECLARE_SYNC_AND_ASYNC(Status, GetImpl, const ReadOptions& read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableSlice* value, std::string* timestamp);

  Status GetImpl(const ReadOptions& read_options,
                 ColumnFamilyHandle* column_family, const Slice& key,
                 PinnableSlice* value, std::string* timestamp,
                 bool* newer_version_present);

  // Function that Get and KeyMayExist call with no_io true or false
  // Note: 'value_found' from KeyMayExist propagates here
  // This function is also called by GetMergeOperands
  // If get_impl_options.get_value = true get value associated with
  // get_impl_options.key via get_impl_options.value
  // If get_impl_options.get_value = false get merge operands associated with
  // get_impl_options.key via get_impl_options.merge_operands
  DECLARE_SYNC_AND_ASYNC_VIRTUAL(Status, GetImpl, const ReadOptions& options,
                                 const Slice& key,
                                 GetImplOptions& get_impl_options);

  // Shared implementation of GetEntityLazy / MultiGetEntityLazy for a single
  // key: performs a lazy point lookup for `key` (leaving blob references
  // unresolved) into `*result`, which takes ownership of a SuperVersion pin so
  // it may outlive this call. `read_options` must already be finalized (e.g.
  // io_activity set). Returns OK on success, NotFound (with an empty `*result`)
  // if there is no entry, or another non-OK status on error. REQUIRES:
  // max_open_files == -1 has already been validated by the caller.
  Status GetEntityLazyImpl(const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           LazyWideColumns* result);

  // Batched-read counterpart of GetEntityLazyImpl for one key of
  // MultiGetEntityLazy: performs the lazy point lookup against the batch's
  // shared `super_version` at the batch's consistent `snapshot_seq` (rather
  // than acquiring its own), and binds the entity's resolver to that Version
  // without taking a per-key pin (the batch owns one shared pin per column
  // family).
  Status GetEntityLazyForBatch(const ReadOptions& read_options,
                               ColumnFamilyHandle* column_family,
                               SuperVersion* super_version,
                               SequenceNumber snapshot_seq, const Slice& key,
                               LazyWideColumns* result);

  // If `snapshot` == kMaxSequenceNumber, set a recent one inside the file.
  ArenaWrappedDBIter* NewIteratorImpl(const ReadOptions& options,
                                      ColumnFamilyHandleImpl* cfh,
                                      SuperVersion* sv, SequenceNumber snapshot,
                                      ReadCallback* read_callback,
                                      bool expose_blob_index = false,
                                      bool allow_refresh = true);

  virtual SequenceNumber GetLastPublishedSequence() const {
    if (last_seq_same_as_publish_seq_) {
      return versions_->LastSequence();
    } else {
      return versions_->LastPublishedSequence();
    }
  }

  // REQUIRES: joined the main write queue if two_write_queues is disabled, and
  // the second write queue otherwise.
  virtual void SetLastPublishedSequence(SequenceNumber seq);
  // Returns LastSequence in last_seq_same_as_publish_seq_
  // mode and LastAllocatedSequence otherwise. This is useful when visiblility
  // depends also on data written to the WAL but not to the memtable.
  SequenceNumber TEST_GetLastVisibleSequence() const;

  // Similar to Write() but will call the callback once on the single write
  // thread to determine whether it is safe to perform the write.
  virtual Status WriteWithCallback(const WriteOptions& write_options,
                                   WriteBatch* my_batch,
                                   WriteCallback* callback,
                                   UserWriteCallback* user_write_cb = nullptr);

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into the current
  // memtables. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  //
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  SequenceNumber GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                   bool include_history);

  // For a given key, check to see if there are any records for this key
  // in the memtables, including memtable history.  If cache_only is false,
  // SST files will also be checked.
  //
  // `key` should NOT have user-defined timestamp appended to user key even if
  // timestamp is enabled.
  //
  // If a key is found, *found_record_for_key will be set to true and
  // *seq will be set to the stored sequence number for the latest
  // operation on this key or kMaxSequenceNumber if unknown. If user-defined
  // timestamp is enabled for this column family and timestamp is not nullptr,
  // then *timestamp will be set to the stored timestamp for the latest
  // operation on this key.
  // If no key is found, *found_record_for_key will be set to false.
  //
  // Note: If cache_only=false, it is possible for *seq to be set to 0 if
  // the sequence number has been cleared from the record.  If the caller is
  // holding an active db snapshot, we know the missing sequence must be less
  // than the snapshot's sequence number (sequence numbers are only cleared
  // when there are no earlier active snapshots).
  //
  // If NotFound is returned and found_record_for_key is set to false, then no
  // record for this key was found.  If the caller is holding an active db
  // snapshot, we know that no key could have existing after this snapshot
  // (since we do not compact keys that have an earlier snapshot).
  //
  // Only records newer than or at `lower_bound_seq` are guaranteed to be
  // returned. Memtables and files may not be checked if it only contains data
  // older than `lower_bound_seq`.
  //
  // Returns OK or NotFound on success,
  // other status on unexpected error.
  // TODO(andrewkr): this API need to be aware of range deletion operations
  Status GetLatestSequenceForKey(SuperVersion* sv, const Slice& key,
                                 bool cache_only,
                                 SequenceNumber lower_bound_seq,
                                 SequenceNumber* seq, std::string* timestamp,
                                 bool* found_record_for_key,
                                 bool* is_blob_index);

  Status TraceIteratorSeek(const uint32_t& cf_id, const Slice& key,
                           const Slice& lower_bound, const Slice upper_bound,
                           const TracedReadOptions& read_options);
  Status TraceIteratorSeekForPrev(const uint32_t& cf_id, const Slice& key,
                                  const Slice& lower_bound,
                                  const Slice upper_bound,
                                  const TracedReadOptions& read_options);

  // Similar to GetSnapshot(), but also lets the db know that this snapshot
  // will be used for transaction write-conflict checking.  The DB can then
  // make sure not to compact any keys that would prevent a write-conflict from
  // being detected.
  const Snapshot* GetSnapshotForWriteConflictBoundary();

  // max_file_num_to_ignore allows bottom level compaction to filter out newly
  // compacted SST files. Setting max_file_num_to_ignore to kMaxUint64 will
  // disable the filtering
  // If `final_output_level` is not nullptr, it is set to manual compaction's
  // output level if returned status is OK, and it may or may not be set to
  // manual compaction's output level if returned status is not OK.
  Status RunManualCompaction(ColumnFamilyData* cfd, int input_level,
                             int output_level,
                             const CompactRangeOptions& compact_range_options,
                             const Slice* begin, const Slice* end,
                             bool exclusive, bool disallow_trivial_move,
                             uint64_t max_file_num_to_ignore,
                             const std::string& trim_ts,
                             int* final_output_level = nullptr);

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  // If allow_unprepared_value is true, the returned iterator may defer reading
  // the value and so will require PrepareValue() to be called before value();
  // allow_unprepared_value = false is convenient when this optimization is not
  // useful, e.g. when reading the whole column family.
  //
  // read_options.ignore_range_deletions determines whether range tombstones are
  // processed in the returned interator internally, i.e., whether range
  // tombstone covered keys are in this iterator's output.
  // @param read_options Must outlive the returned iterator.
  InternalIterator* NewInternalIterator(
      const ReadOptions& read_options, Arena* arena, SequenceNumber sequence,
      ColumnFamilyHandle* column_family = nullptr,
      bool allow_unprepared_value = false);

  // Note: to support DB iterator refresh, memtable range tombstones in the
  // underlying merging iterator needs to be refreshed. If db_iter is not
  // nullptr, db_iter->SetMemtableRangetombstoneIter() is called with the
  // memtable range tombstone iterator used by the underlying merging iterator.
  // This range tombstone iterator can be refreshed later by db_iter.
  // @param read_options Must outlive the returned iterator.
  // @param sequence The snapshot sequence captured when the DB iterator was
  // created. Child iterators must use this instead of dereferencing
  // read_options.snapshot, which may be released before lazy initialization.
  // @param scan_opts Optional bounded scan ranges used only to prune the
  // iterator tree during lazy Prepare() initialization.
  InternalIterator* NewInternalIterator(
      const ReadOptions& read_options, ColumnFamilyData* cfd,
      SuperVersion* super_version, Arena* arena, SequenceNumber sequence,
      bool allow_unprepared_value, ArenaWrappedDBIter* db_iter = nullptr,
      const MultiScanArgs* scan_opts = nullptr);

  // Release a SuperVersion held by an iterator. This preserves the cleanup
  // behavior used by materialized internal iterators even when the DB iterator
  // never needed to lazily build its child iterator tree.
  void CleanupIteratorSuperVersion(SuperVersion* super_version,
                                   bool background_purge);

  // Transfer an extra reference to `super_version` into `pin` (via a cleanup
  // callback), so `pin`'s owner keeps the SuperVersion -- and thus the data it
  // references -- alive after the current call returns, as an iterator does.
  // Shared by the lazy wide-column read path (GetEntityLazy) across the
  // primary, read-only, and secondary GetImpl overrides.
  void TransferSuperVersionPin(SuperVersion* super_version, Cleanable* pin);

  LogsWithPrepTracker* logs_with_prep_tracker() {
    return &logs_with_prep_tracker_;
  }

  struct BGJobLimits {
    int max_flushes;
    int max_compactions;
  };
  // Returns maximum background flushes and compactions allowed to be scheduled
  BGJobLimits GetBGJobLimits() const;
  // Need a static version that can be called during SanitizeOptions().
  static BGJobLimits GetBGJobLimits(int max_background_flushes,
                                    int max_background_compactions,
                                    int max_background_jobs,
                                    bool parallelize_compactions);

  // move logs pending closing from job_context to the DB queue and
  // schedule a purge
  void ScheduleBgLogWriterClose(JobContext* job_context);

  uint64_t MinLogNumberToKeep();

  uint64_t MinLogNumberToRecycle();

  // Whether the WAL is still in logs_, i.e. its writer may still be open. Such
  // a WAL must not be recycled yet: closing the old writer truncates the file
  // to its old size.
  // REQUIRES: wal_write_mutex_ not held
  bool IsWalInLogs(uint64_t wal_number);

  // Returns the lower bound file number for SSTs that won't be deleted, even if
  // they're obsolete. This lower bound is used internally to prevent newly
  // created flush/compaction output files from being deleted before they're
  // installed. This technique avoids the need for tracking the exact numbers of
  // files pending creation, although it prevents more files than necessary from
  // being deleted.
  uint64_t MinObsoleteSstNumberToKeep();

  uint64_t GetObsoleteSstFilesSize();

  uint64_t MinOptionsFileNumberToKeep();

  // Returns the list of live files in 'live' and the list
  // of all files in the filesystem in 'candidate_files'.
  // If force == false and the last call was less than
  // db_options_.delete_obsolete_files_period_micros microseconds ago,
  // it will not fill up the job_context
  void FindObsoleteFiles(JobContext* job_context, bool force,
                         bool no_full_scan = false);

  // Diffs the files listed in filenames and those that do not
  // belong to live files are possibly removed. Also, removes all the
  // files in sst_delete_files and log_delete_files.
  // It is not necessary to hold the mutex when invoking this method.
  // If FindObsoleteFiles() was run, we need to also run
  // PurgeObsoleteFiles(), even if disable_delete_obsolete_files_ is true
  void PurgeObsoleteFiles(JobContext& background_contet,
                          bool schedule_only = false);

  // Schedule a background job to actually delete obsolete files.
  void SchedulePurge();

  // Thread-safe; does not require any DB lock.
  const ShardedSnapshotList& snapshots() const { return snapshots_; }

  // Loads snapshots no newer than `max_seq` into `snap_vector` in ascending
  // order. Returns an exact oldest live snapshot sequence, or the last
  // published sequence if there are no snapshots, from a separate all-shard
  // scan. `snapshot_release_count` receives the release count observed by that
  // exact scan when non-null.
  // `oldest_write_conflict_snapshot` is filled with the oldest snapshot
  // which satisfies SnapshotImpl.is_write_conflict_boundary_ = true.
  // Thread-safe; callers that use the return values for DB-wide maintenance
  // must hold mutex_.
  SequenceNumber LoadSnapshots(std::vector<SequenceNumber>* snap_vector,
                               SequenceNumber* oldest_write_conflict_snapshot,
                               const SequenceNumber& max_seq,
                               uint64_t* snapshot_release_count) const {
    snapshots().GetAll(snap_vector, oldest_write_conflict_snapshot, max_seq);
    return snapshots().OldestOr([this]() { return GetLastPublishedSequence(); },
                                snapshot_release_count);
  }

  const ImmutableDBOptions& immutable_db_options() const {
    return immutable_db_options_;
  }

  // Cancel all background jobs, including flush, compaction, background
  // purging, stats dumping threads, etc. If `wait` = true, wait for the
  // running jobs to abort or finish before returning. Otherwise, only
  // sends the signals.
  void CancelAllBackgroundWork(bool wait);

  // Find Super version and reference it. Based on options, it might return
  // the thread local cached one.
  // Call ReturnAndCleanupSuperVersion() when it is no longer needed.
  SuperVersion* GetAndRefSuperVersion(ColumnFamilyData* cfd);

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.
  SuperVersion* GetAndRefSuperVersion(uint32_t column_family_id);

  // Un-reference the super version and clean it up if it is the last reference.
  void CleanupSuperVersion(SuperVersion* sv);

  // Un-reference the super version and return it to thread local cache if
  // needed. If it is the last reference of the super version. Clean it up
  // after un-referencing it.
  void ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd, SuperVersion* sv);

  // Similar to the previous function but looks up based on a column family id.
  // nullptr will be returned if this column family no longer exists.
  // REQUIRED: this function should only be called on the write thread.
  void ReturnAndCleanupSuperVersion(uint32_t colun_family_id, SuperVersion* sv);

  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.  Return value only valid until next call to this function or
  // mutex is released.
  ColumnFamilyHandle* GetColumnFamilyHandle(uint32_t column_family_id);

  // Same as above, should called without mutex held and not on write thread.
  std::unique_ptr<ColumnFamilyHandle> GetColumnFamilyHandleUnlocked(
      uint32_t column_family_id);

  // Returns the number of currently running flushes.
  // REQUIREMENT: mutex_ must be held when calling this function.
  int num_running_flushes() {
    mutex_.AssertHeld();
    return num_running_flushes_;
  }

  const WriteController& write_controller() { return write_controller_; }

  // hollow transactions shell used for recovery.
  // these will then be passed to TransactionDB so that
  // locks can be reacquired before writing can resume.
  struct RecoveredTransaction {
    std::string name_;
    bool unprepared_;

    struct BatchInfo {
      uint64_t log_number_;
      // TODO(lth): For unprepared, the memory usage here can be big for
      // unprepared transactions. This is only useful for rollbacks, and we
      // can in theory just keep keyset for that.
      WriteBatch* batch_;
      // Number of sub-batches. A new sub-batch is created if txn attempts to
      // insert a duplicate key,seq to memtable. This is currently used in
      // WritePreparedTxn/WriteUnpreparedTxn.
      size_t batch_cnt_;
    };

    // This maps the seq of the first key in the batch to BatchInfo, which
    // contains WriteBatch and other information relevant to the batch.
    //
    // For WriteUnprepared, batches_ can have size greater than 1, but for
    // other write policies, it must be of size 1.
    std::map<SequenceNumber, BatchInfo> batches_;

    explicit RecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch, SequenceNumber seq,
                                  size_t batch_cnt, bool unprepared)
        : name_(name), unprepared_(unprepared) {
      batches_[seq] = {log, batch, batch_cnt};
    }

    // No copying allowed
    RecoveredTransaction(const RecoveredTransaction&) = delete;
    RecoveredTransaction& operator=(const RecoveredTransaction&) = delete;
    RecoveredTransaction(RecoveredTransaction&&) = delete;
    RecoveredTransaction& operator=(RecoveredTransaction&&) = delete;

    ~RecoveredTransaction() {
      for (auto& it : batches_) {
        delete it.second.batch_;
      }
    }

    void AddBatch(SequenceNumber seq, uint64_t log_number, WriteBatch* batch,
                  size_t batch_cnt, bool unprepared) {
      assert(batches_.count(seq) == 0);
      batches_[seq] = {log_number, batch, batch_cnt};
      // Prior state must be unprepared, since the prepare batch must be the
      // last batch.
      assert(unprepared_);
      unprepared_ = unprepared;
    }
  };

  using RecoveredTransactionMap =
      std::unordered_map<std::string, RecoveredTransaction*>;

  bool allow_2pc() const { return immutable_db_options_.allow_2pc; }

  RecoveredTransactionMap recovered_transactions() {
    return recovered_transactions_;
  }

  RecoveredTransaction* GetRecoveredTransaction(const std::string& name) {
    auto it = recovered_transactions_.find(name);
    if (it == recovered_transactions_.end()) {
      return nullptr;
    } else {
      return it->second;
    }
  }

  void InsertRecoveredTransaction(const uint64_t log, const std::string& name,
                                  WriteBatch* batch, SequenceNumber seq,
                                  size_t batch_cnt, bool unprepared_batch) {
    // For WriteUnpreparedTxn, InsertRecoveredTransaction is called multiple
    // times for every unprepared batch encountered during recovery.
    //
    // If the transaction is prepared, then the last call to
    // InsertRecoveredTransaction will have unprepared_batch = false.
    auto rtxn = recovered_transactions_.find(name);
    if (rtxn == recovered_transactions_.end()) {
      recovered_transactions_[name] = new RecoveredTransaction(
          log, name, batch, seq, batch_cnt, unprepared_batch);
    } else {
      rtxn->second->AddBatch(seq, log, batch, batch_cnt, unprepared_batch);
    }
    logs_with_prep_tracker_.MarkLogAsContainingPrepSection(log);
  }

  // Deletes the recovered transaction `it` points to and returns the iterator
  // following it, like std::unordered_map::erase().
  RecoveredTransactionMap::iterator DeleteRecoveredTransaction(
      RecoveredTransactionMap::iterator it) {
    assert(it != recovered_transactions_.end());
    auto* trx = it->second;
    RecoveredTransactionMap::iterator next = recovered_transactions_.erase(it);
    for (const auto& info : trx->batches_) {
      logs_with_prep_tracker_.MarkLogAsHavingPrepSectionFlushed(
          info.second.log_number_);
    }
    delete trx;
    return next;
  }

  void DeleteRecoveredTransaction(const std::string& name) {
    RecoveredTransactionMap::iterator it = recovered_transactions_.find(name);
    DeleteRecoveredTransaction(it);
  }

  void DeleteAllRecoveredTransactions() {
    for (auto it = recovered_transactions_.begin();
         it != recovered_transactions_.end(); ++it) {
      delete it->second;
    }
    recovered_transactions_.clear();
  }

  void AddToLogsToFreeQueue(log::Writer* log_writer) {
    mutex_.AssertHeld();
    wals_to_free_queue_.push_back(log_writer);
  }

  void AddSuperVersionsToFreeQueue(SuperVersion* sv) {
    superversions_to_free_queue_.push_back(sv);
  }

  void EnableTrackPublishedSeqInSnapshotContext();
  void SetSnapshotChecker(SnapshotChecker* snapshot_checker);

  // Fill JobContext with snapshot information needed by flush and compaction.
  void InitSnapshotContext(JobContext* job_context);

  // Not thread-safe.
  void SetRecoverableStatePreReleaseCallback(PreReleaseCallback* callback);

  InstrumentedMutex* mutex() const { return &mutex_; }
  const std::atomic<uint64_t>* GetMutexHoldLogThreshold() const {
    return &mutex_hold_log_threshold_us_;
  }
  const std::shared_ptr<SlowMutexLoggingContext>& GetSlowMutexLogging() const {
    return slow_mutex_logging_;
  }

  // Initialize a brand new DB. The DB directory is expected to be empty before
  // calling it. Push new manifest file name into `new_filenames`.
  Status NewDB(std::vector<std::string>* new_filenames);

  // This is to be used only by internal rocksdb classes.
  static Status Open(const DBOptions& db_options, const std::string& name,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     std::vector<ColumnFamilyHandle*>* handles,
                     std::unique_ptr<DB>* dbptr, const bool seq_per_batch,
                     const bool batch_per_txn, const bool is_retry,
                     bool* can_retry);

  static IOStatus CreateAndNewDirectory(
      FileSystem* fs, const std::string& dirname,
      std::unique_ptr<FSDirectory>* directory);

  // find stats map from stats_history_ with smallest timestamp in
  // the range of [start_time, end_time)
  bool FindStatsByTime(uint64_t start_time, uint64_t end_time,
                       uint64_t* new_time,
                       std::map<std::string, uint64_t>* stats_map);

  // Print information of all tombstones of all iterators to the std::string
  // This is only used by ldb. The output might be capped. Tombstones
  // printed out are not guaranteed to be in any order.
  Status TablesRangeTombstoneSummary(ColumnFamilyHandle* column_family,
                                     int max_entries_to_print,
                                     std::string* out_str);

  VersionSet* GetVersionSet() const { return versions_.get(); }

  Status WaitForCompact(
      const WaitForCompactOptions& wait_for_compact_options) override;

#ifndef NDEBUG
  // Compact any files in the named level that overlap [*begin, *end]
  Status TEST_CompactRange(int level, const Slice* begin, const Slice* end,
                           ColumnFamilyHandle* column_family = nullptr,
                           bool disallow_trivial_move = false);

  Status TEST_SwitchWAL();

  bool TEST_UnableToReleaseOldestLog() { return unable_to_release_oldest_log_; }

  bool TEST_IsLogGettingFlushed() {
    return alive_wal_files_.begin()->getting_flushed;
  }

  Status TEST_SwitchMemtable(ColumnFamilyData* cfd = nullptr);

  size_t TEST_GetWBMFlushableMemUsage();
  bool TEST_IsWBMFlushInitiatorFlushable() const;

  // Force current memtable contents to be flushed.
  Status TEST_FlushMemTable(bool wait = true, bool allow_write_stall = false,
                            ColumnFamilyHandle* cfh = nullptr);

  Status TEST_FlushMemTable(ColumnFamilyData* cfd,
                            const FlushOptions& flush_opts);

  // Like TEST_FlushMemTable(bool wait, ...), but also sets
  // FlushOptions::listener_wait so the flush does not return until the
  // registered EventListener::OnFlushCompleted callbacks for the flushed
  // memtables have finished running.
  Status TEST_FlushMemTableWithListenerWait(bool allow_write_stall = false,
                                            ColumnFamilyHandle* cfh = nullptr);

  // Flush (multiple) ColumnFamilyData without using ColumnFamilyHandle. This
  // is because in certain cases, we can flush column families, wait for the
  // flush to complete, but delete the column family handle before the wait
  // finishes. For example in CompactRange.
  Status TEST_AtomicFlushMemTables(
      const autovector<ColumnFamilyData*>& provided_candidate_cfds,
      const FlushOptions& flush_opts, bool non_blocking_write_thread = false);

  void TEST_BeginWriteStall();
  void TEST_EndWriteStall();

  // Wait for background threads to complete scheduled work.
  Status TEST_WaitForBackgroundWork();

  // Wait for memtable compaction
  Status TEST_WaitForFlushMemTable(ColumnFamilyHandle* column_family = nullptr);

  Status TEST_WaitForCompact();
  Status TEST_WaitForCompact(
      const WaitForCompactOptions& wait_for_compact_options);

  // Wait for any background purge
  Status TEST_WaitForPurge();

  // Get the background error status
  Status TEST_GetBGError();

  void TEST_SetBGError(const IOStatus& error, BackgroundErrorReason reason);

  bool TEST_IsRecoveryInProgress();

  Status TEST_ResumeImpl(DBRecoverContext context);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  uint64_t TEST_MaxNextLevelOverlappingBytes(
      ColumnFamilyHandle* column_family = nullptr);

  // Return the current manifest file no.
  uint64_t TEST_Current_Manifest_FileNo();

  // Returns the number that'll be assigned to the next file that's created.
  uint64_t TEST_Current_Next_FileNo();

  // get total level0 file size. Only for testing.
  uint64_t TEST_GetLevel0TotalSize();

  void TEST_GetFilesMetaData(
      ColumnFamilyHandle* column_family,
      std::vector<std::vector<FileMetaData>>* metadata,
      std::vector<std::shared_ptr<BlobFileMetaData>>* blob_metadata = nullptr);

  void TEST_LockMutex();

  void TEST_UnlockMutex();

  InstrumentedMutex* TEST_Mutex() { return &mutex_; }

  void TEST_SignalAllBgCv();

  // REQUIRES: mutex locked
  void* TEST_BeginWrite();

  // REQUIRES: mutex locked
  // pass the pointer that you got from TEST_BeginWrite()
  void TEST_EndWrite(void* w);

  uint64_t TEST_MaxTotalInMemoryState() const {
    return max_total_in_memory_state_;
  }

  size_t TEST_LogsToFreeSize();

  uint64_t TEST_LogfileNumber();

  uint64_t TEST_wals_total_size() const {
    return wals_total_size_.LoadRelaxed();
  }

  void TEST_GetAllBlockCaches(std::unordered_set<const Cache*>* cache_set);

  // Return the lastest MutableCFOptions of a column family
  Status TEST_GetLatestMutableCFOptions(ColumnFamilyHandle* column_family,
                                        MutableCFOptions* mutable_cf_options);

  Cache* TEST_table_cache() { return table_cache_.get(); }

  WriteController& TEST_write_controler() { return write_controller_; }

  uint64_t TEST_FindMinLogContainingOutstandingPrep();
  uint64_t TEST_FindMinPrepLogReferencedByMemTable();
  size_t TEST_PreparedSectionCompletedSize();
  size_t TEST_LogsWithPrepSize();

  int TEST_BGCompactionsAllowed() const;
  int TEST_BGFlushesAllowed() const;
  int TEST_NumRunningBottomCompactions() const;
  size_t TEST_GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;
  void TEST_WaitForPeriodicTaskRun(std::function<void()> callback) const;
  SeqnoToTimeMapping TEST_GetSeqnoToTimeMapping() const;
  const autovector<uint64_t>& TEST_GetFilesToQuarantine() const;
  size_t TEST_EstimateInMemoryStatsHistorySize() const;

  uint64_t TEST_GetCurrentLogNumber() const {
    InstrumentedMutexLock l(mutex());
    assert(!logs_.empty());
    return logs_.back().number;
  }

  void TEST_DeleteObsoleteFiles();

  const std::unordered_set<uint64_t>& TEST_GetFilesGrabbedForPurge() const {
    return files_grabbed_for_purge_;
  }

  const PeriodicTaskScheduler& TEST_GetPeriodicTaskScheduler() const;

  static Status TEST_ValidateOptions(const DBOptions& db_options) {
    return ValidateOptions(db_options);
  }
#endif  // NDEBUG

  // In certain configurations, verify that the table/blob file cache only
  // contains entries for live files, to check for effective leaks of open
  // files. This can only be called when purging of obsolete files has
  // "settled," such as during parts of DB Close().
  void TEST_VerifyNoObsoleteFilesCached(bool db_mutex_already_held) const;

  // persist stats to column family "_persistent_stats"
  void PersistStats();

  // dump rocksdb.stats to LOG
  void DumpStats();

  // flush LOG out of application buffer
  void FlushInfoLog();

  // For the background timer job
  void RecordSeqnoToTimeMapping();

  // Compactions rely on an event triggers like flush/compaction/SetOptions.
  // We need to trigger periodic compactions even when there is no such trigger.
  // This function checks and schedules available compactions and will run
  // periodically.
  void TriggerPeriodicCompaction();

  // REQUIRES: DB mutex held
  std::pair<SequenceNumber, uint64_t> GetSeqnoToTimeSample() const;

  // REQUIRES: DB mutex held or during open
  void EnsureSeqnoToTimeMapping(const MinAndMaxPreserveSeconds& preserve_secs);

  // Computes the seqno->time preserve-window lower bound from
  // seqno_to_time_mapping_ and stores it on cfd's current version, so
  // bottommost file marking does not mark files whose largest seqno cannot be
  // zeroed yet (which would loop). No-op for column families without
  // preserve/preclude enabled. Returns true if the stored value changed, so
  // callers can recompute bottommost marking when the boundary moves.
  // Note: right after opening an existing DB, seqno_to_time_mapping_ may not be
  // fully reconstructed, so this bound can be imprecise until the first
  // periodic RecordSeqnoToTimeMapping. That is safe: CompactionJob folds this
  // same bound into kBottommostFiles compactions, so any marked file still
  // makes progress (never loops), and the periodic task self-corrects the
  // bound. REQUIRES: DB mutex held
  bool MaybeUpdatePreserveTimeMinSeqno(ColumnFamilyData* cfd);

  // Only called during open
  void PrepopulateSeqnoToTimeMapping(
      const MinAndMaxPreserveSeconds& preserve_secs);

  // Interface to block and signal the DB in case of stalling writes by
  // WriteBufferManager. Each DBImpl object contains ptr to WBMStallInterface.
  // When DB needs to be blocked or signalled by WriteBufferManager,
  // state_ is changed accordingly.
  class WBMStallInterface : public StallInterface {
   public:
    enum State {
      BLOCKED = 0,
      RUNNING,
    };

    WBMStallInterface() : state_cv_(&state_mutex_) {
      MutexLock lock(&state_mutex_);
      state_ = State::RUNNING;
    }

    void SetState(State state) {
      MutexLock lock(&state_mutex_);
      state_ = state;
    }

    // Change the state_ to State::BLOCKED and wait until its state is
    // changed by WriteBufferManager. When stall is cleared, Signal() is
    // called to change the state and unblock the DB.
    void Block() override {
      MutexLock lock(&state_mutex_);
      while (state_ == State::BLOCKED) {
        TEST_SYNC_POINT("WBMStallInterface::BlockDB");
        state_cv_.Wait();
      }
    }

    // Returns false when the wait times out while the DB is still blocked.
    bool BlockFor(uint64_t timeout_micros) {
      MutexLock lock(&state_mutex_);
      const std::chrono::microseconds deadline(
          SystemClock::Default()->NowMicros() + timeout_micros);
      while (state_ == State::BLOCKED) {
        TEST_SYNC_POINT("WBMStallInterface::BlockDB");
        if (SystemClock::Default()->TimedWait(&state_cv_, deadline) &&
            state_ == State::BLOCKED) {
          return false;
        }
      }
      return true;
    }

    // Called from WriteBufferManager. This function changes the state_
    // to State::RUNNING indicating the stall is cleared and DB can proceed.
    void Signal() override {
      {
        MutexLock lock(&state_mutex_);
        state_ = State::RUNNING;
      }
      state_cv_.Signal();
    }

   private:
    // Conditional variable and mutex to block and
    // signal the DB during stalling process.
    port::Mutex state_mutex_;
    port::CondVar state_cv_;
    // state represting whether DB is running or blocked because of stall by
    // WriteBufferManager.
    State state_;
  };

  // Adapts DBImpl to the shared WriteBufferManager flush registry.
  class WBMFlushInitiator : public FlushInitiator {
   public:
    WBMFlushInitiator(DBImpl* db, bool atomic_flush)
        : FlushInitiator(atomic_flush), db_(db) {}

    bool ScheduleFlush() override {
      return db_->ScheduleWriteBufferManagerFlush();
    }

    bool TryRefreshMemoryAccounting() override {
      return db_->TryRefreshFlushableMemAccounting();
    }

   private:
    DBImpl* const db_;
  };

  static void TEST_ResetDbSessionIdGen();
  static std::string GenerateDbSessionId(Env* env);

  bool seq_per_batch() const { return seq_per_batch_; }

#if USE_COROUTINES
  CoroDB* GetCoroDB() override { return this; }
#endif

 protected:
  const std::string dbname_;
  const bool read_only_;
  // TODO(peterd): unify with VersionSet::db_id_
  std::string db_id_;
  // db_session_id_ is an identifier that gets reset
  // every time the DB is opened
  std::string db_session_id_;
  // Declared before versions_ so memtables cannot outlive their allocation
  // accounting target during destruction.
  std::unique_ptr<FlushInitiator> wbm_flush_initiator_;
  std::unique_ptr<VersionSet> versions_;
  // Flag to check whether we allocated and own the info log file
  bool own_info_log_;
  Status init_logger_creation_s_;
  const DBOptions initial_db_options_;
  Env* const env_;
  std::shared_ptr<IOTracer> io_tracer_;
  const ImmutableDBOptions immutable_db_options_;
  FileSystemPtr fs_;
  MutableDBOptions mutable_db_options_;
  Statistics* stats_;
  std::atomic<uint64_t> mutex_hold_log_threshold_us_;
  std::shared_ptr<SlowMutexLoggingContext> slow_mutex_logging_;
  RecoveredTransactionMap recovered_transactions_;
  std::unique_ptr<Tracer> tracer_;
  InstrumentedMutex trace_mutex_;
  BlockCacheTracer block_cache_tracer_;

  // constant false canceled flag, used when the compaction is not manual
  const std::atomic<bool> kManualCompactionCanceledFalse_{false};

  // State below is protected by mutex_
  // With two_write_queues enabled, some of the variables that accessed during
  // WriteToWAL need different synchronization: wal_empty_, alive_wal_files_,
  // logs_, cur_wal_number_. Refer to the definition of each variable below for
  // more description.
  //
  // Protects access to most ColumnFamilyData methods, see more in comment for
  // each method.
  //
  // `mutex_` can be a hot lock in some workloads, so it deserves dedicated
  // cachelines.
  mutable CacheAlignedInstrumentedMutex mutex_;

  ColumnFamilyHandleImpl* default_cf_handle_ = nullptr;
  InternalStats* default_cf_internal_stats_ = nullptr;

  // table_cache_ provides its own synchronization
  std::shared_ptr<Cache> table_cache_;

  ErrorHandler error_handler_;

  // Unified interface for logging events
  EventLogger event_logger_;

  // only used for dynamically adjusting max_total_wal_size. it is a sum of
  // [write_buffer_size * max_write_buffer_number] over all column families
  std::atomic<uint64_t> max_total_in_memory_state_ = 0;

  // The options to access storage files
  const FileOptions file_options_;

  // Additonal options for compaction and flush
  FileOptions file_options_for_compaction_;

  std::unique_ptr<ColumnFamilyMemTablesImpl> column_family_memtables_;

  // Increase the sequence number after writing each batch, whether memtable is
  // disabled for that or not. Otherwise the sequence number is increased after
  // writing each key into memtable. This implies that when disable_memtable is
  // set, the seq is not increased at all.
  //
  // Default: false
  const bool seq_per_batch_;
  // This determines during recovery whether we expect one writebatch per
  // recovered transaction, or potentially multiple writebatches per
  // transaction. For WriteUnprepared, this is set to false, since multiple
  // batches can exist per transaction.
  //
  // Default: true
  const bool batch_per_txn_;

  // Each flush or compaction gets its own job id. this counter makes sure
  // they're unique
  std::atomic<int> next_job_id_ = 1;

  std::atomic<bool> shutting_down_ = false;
  // Protected by mutex_. This is separate from shutting_down_ because
  // OnDBShutdownBegin must fire before shutting_down_ is published, and the
  // notification releases mutex_ while invoking listeners. Marking that the
  // notification has started prevents a concurrent or reentrant
  // CancelAllBackgroundWork() call from firing it again during that unlocked
  // callback window.
  bool shutdown_notification_sent_ = false;

  // No new background jobs can be queued if true. This is used to prevent new
  // background jobs from being queued after WaitForCompact() completes waiting
  // all background jobs then attempts to close when close_db_ option is true.
  bool reject_new_background_jobs_ = false;

  // RecoveryContext struct stores the context about version edits along
  // with corresponding column_family_data and column_family_options.
  class RecoveryContext {
   public:
    RecoveryContext() = default;
    // No copying allowed
    RecoveryContext(const RecoveryContext&) = delete;
    RecoveryContext& operator=(const RecoveryContext&) = delete;
    RecoveryContext(RecoveryContext&&) = delete;
    RecoveryContext& operator=(RecoveryContext&&) = delete;

    ~RecoveryContext() {
      for (auto& edit_list : edit_lists_) {
        for (auto* edit : edit_list) {
          delete edit;
        }
      }
    }

    void UpdateVersionEdits(ColumnFamilyData* cfd, const VersionEdit& edit) {
      assert(cfd != nullptr);
      if (map_.find(cfd->GetID()) == map_.end()) {
        uint32_t size = static_cast<uint32_t>(map_.size());
        map_.emplace(cfd->GetID(), size);
        cfds_.emplace_back(cfd);
        edit_lists_.emplace_back(autovector<VersionEdit*>());
      }
      uint32_t i = map_[cfd->GetID()];
      edit_lists_[i].emplace_back(new VersionEdit(edit));
    }

    bool HasVersionEdits() const {
      for (const auto& edit_list : edit_lists_) {
        if (!edit_list.empty()) {
          return true;
        }
      }
      return false;
    }

    std::unordered_map<uint32_t, uint32_t> map_;  // cf_id to index;
    autovector<ColumnFamilyData*> cfds_;
    autovector<autovector<VersionEdit*>> edit_lists_;
    // Files found in the primary DB directory by MaybeUpdateNextFileNumber().
    // Reused for OPTIONS selection so writable open performs one directory
    // scan instead of two.
    std::vector<std::string> existing_db_files_;
    // All existing data files (SST files and Blob files) found during DB::Open.
    std::vector<std::string> existing_data_files_;
    bool is_new_db_ = false;
  };

  // Persist options to options file. Must be holding options_mutex_.
  // Will lock DB mutex if !db_mutex_already_held.
  Status WriteOptionsFile(const WriteOptions& write_options,
                          bool db_mutex_already_held);

  // Writes, renames, and directory-syncs an OPTIONS snapshot, but does not
  // publish it in VersionSet. The caller must either commit its file number in
  // MANIFEST or leave it as an unreferenced orphan.
  Status PersistOptionsFile(const WriteOptions& write_options,
                            const DBOptions& db_options,
                            const std::vector<std::string>& cf_names,
                            const std::vector<ColumnFamilyOptions>& cf_opts,
                            uint64_t* options_file_number,
                            uint64_t* options_file_size);

  // For tracked DBs, writes the OPTIONS snapshot that is committed with a
  // column-family add/drop. Returns file number zero in legacy mode.
  Status PersistOptionsFileForColumnFamilyManipulation(
      const WriteOptions& write_options,
      const std::vector<ColumnFamilyDescriptor>& added_column_families,
      const std::vector<uint32_t>& excluded_cf_ids,
      uint64_t* options_file_number, uint64_t* options_file_size);

  Status CompactRangeInternal(const CompactRangeOptions& options,
                              ColumnFamilyHandle* column_family,
                              const Slice* begin, const Slice* end,
                              const std::string& trim_ts);

  // The following two functions can only be called when:
  // 1. WriteThread::Writer::EnterUnbatched() is used.
  // 2. db_mutex is NOT held
  Status RenameTempFileToOptionsFile(const std::string& file_name,
                                     uint64_t options_file_number,
                                     uint64_t* options_file_size);
  void MaybeDeleteObsoleteOptionsFiles();
  Status DeleteObsoleteOptionsFiles(bool schedule_only);

  void NotifyOnManualFlushScheduled(autovector<ColumnFamilyData*> cfds,
                                    FlushReason flush_reason);

  void NotifyOnFlushBegin(ColumnFamilyData* cfd, FileMetaData* file_meta,
                          const MutableCFOptions& mutable_cf_options,
                          int job_id, FlushReason flush_reason);

  void NotifyOnFlushCompleted(
      ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
      std::list<std::unique_ptr<FlushJobInfo>>* flush_jobs_info);

  void NotifyOnCompactionBegin(ColumnFamilyData* cfd, Compaction* c,
                               const Status& st,
                               const CompactionJobStats& job_stats, int job_id);

  void NotifyOnCompactionCompleted(ColumnFamilyData* cfd, Compaction* c,
                                   const Status& st,
                                   const CompactionJobStats& job_stats,
                                   int job_id);
  // Fires the OnCompactionPreCommit listener callback. Mirrors
  // NotifyOnCompactionCompleted but is invoked before ReleaseCompactionFiles()
  // (i.e. while input files still have being_compacted == true). Idempotent:
  // safe to call from multiple potential release sites; only fires once per
  // compaction, and only if NotifyOnCompactionBegin previously fired.
  void NotifyOnCompactionPreCommit(ColumnFamilyData* cfd, Compaction* c,
                                   const Status& st,
                                   const CompactionJobStats& job_stats,
                                   int job_id);
  void NotifyOnDBShutdownBegin();
  void NotifyOnMemTableSealed(ColumnFamilyData* cfd,
                              const MemTableInfo& mem_table_info);

  void NotifyOnExternalFileIngested(
      ColumnFamilyData* cfd, const ExternalSstFileIngestionJob& ingestion_job);

  Status FlushAllColumnFamilies(const FlushOptions& flush_options,
                                FlushReason flush_reason);

  virtual Status FlushForGetLiveFiles(bool force_atomic_flush = false);

  void NewThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const;

  void EraseThreadStatusDbInfo() const;

  // For CFs that has updates in `wbwi`, their memtable will be switched,
  // and `wbwi` will be added as the latest immutable memtable.
  //
  // REQUIRES: this thread is currently at the front of the main writer queue.
  // @param prep_log refers to the WAL that contains prepare record
  // for the transaction based on wbwi.
  // @param assigned_seqno Sequence numbers for the ingested memtable.
  // @param last_seqno the value of versions_->LastSequence() after the write
  // ingests `wbwi` is done.
  // @param memtable_updated Whether the same write that ingests wbwi has
  // updated memtable. This is useful for determining whether to set bg
  // error when IngestWBWIAsMemtable fails.
  // @param ingest_wbwi_for_commit Whether wbwi ingestion is publishing the
  // committed data of a prepared transaction. This means a failure can leave
  // committed data durable in WAL but not published in memtables.
  // @param ignore_missing_cf If true, skip column families not found in the DB
  // instead of returning an error.
  Status IngestWBWIAsMemtable(std::shared_ptr<WriteBatchWithIndex> wbwi,
                              const WBWIMemTable::SeqnoRange& assigned_seqno,
                              uint64_t min_prep_log, SequenceNumber last_seqno,
                              bool memtable_updated,
                              bool ingest_wbwi_for_commit,
                              bool ignore_missing_cf);

  // If disable_memtable is set the application logic must guarantee that the
  // batch will still be skipped from memtable during the recovery. An excption
  // to this is seq_per_batch_ mode, in which since each batch already takes one
  // seq, it is ok for the batch to write to memtable during recovery as long as
  // it only takes one sequence number: i.e., no duplicate keys.
  // In WriteCommitted it is guarnateed since disable_memtable is used for
  // prepare batch which will be written to memtable later during the commit,
  // and in WritePrepared it is guaranteed since it will be used only for WAL
  // markers which will never be written to memtable. If the commit marker is
  // accompanied with CommitTimeWriteBatch that is not written to memtable as
  // long as it has no duplicate keys, it does not violate the one-seq-per-batch
  // policy.
  // batch_cnt is expected to be non-zero in seq_per_batch mode and
  // indicates the number of sub-patches. A sub-patch is a subset of the write
  // batch that does not have duplicate keys.
  // `callback` is called before WAL write.
  // See more in comment above WriteCallback::Callback().
  // pre_release_callback is called after WAL write and before memtable write.
  // See more in comment above PreReleaseCallback::Callback().
  // post_memtable_callback is called after memtable write but before publishing
  // the sequence number to readers.
  // `trace_batch_override`, when non-null, supplies the logical batch to trace
  // instead of `updates`. Fast paths may rewrite or rebuild `updates` before
  // apply, but tracing should still record the original user-visible batch.
  // `skip_blob_direct_write_transform` indicates the caller has already
  // performed any needed batch-level blob direct-write preprocessing.
  // `deferred_put_entities`, when non-null, carries structured `PutEntity()`
  // ops that should be materialized into `updates` after `PreprocessWrite()`
  // has selected the target memtable/blob generation.
  //
  // The main write queue. This is the only write queue that updates
  // LastSequence. When using one write queue, the same sequence also indicates
  // the last published sequence.
  struct DeferredPutEntityBatch;
  Status WriteImpl(
      const WriteOptions& options, WriteBatch* updates,
      WriteCallback* callback = nullptr,
      UserWriteCallback* user_write_cb = nullptr, uint64_t* wal_used = nullptr,
      uint64_t log_ref = 0, bool disable_memtable = false,
      uint64_t* seq_used = nullptr, size_t batch_cnt = 0,
      PreReleaseCallback* pre_release_callback = nullptr,
      PostMemTableCallback* post_memtable_callback = nullptr,
      std::shared_ptr<WriteBatchWithIndex> wbwi = nullptr,
      WriteBatch* trace_batch_override = nullptr,
      bool skip_blob_direct_write_transform = false,
      const DeferredPutEntityBatch* deferred_put_entities = nullptr);

  // Per-WriteImpl state that keeps BDW column families pinned through
  // referenced SuperVersions until the transformed write either commits or
  // rolls back.
  struct BlobDirectWriteContext;
  // High-level `PutEntity()` fast-path flow:
  //
  //   non-BDW target CF:
  //     PutEntityFastPath()
  //       -> AppendPreprocessedPutEntityToBatch()
  //       -> WritePreprocessedPutEntityBatch()
  //       -> WriteImpl(skip_blob_direct_write_transform = true)
  //
  //   BDW-enabled target CF:
  //     PutEntityFastPath()
  //       -> sort columns and stash DeferredPutEntityBatch::Op
  //       -> WritePreprocessedPutEntityBatch()
  //       -> WriteImpl(..., deferred_put_entities)
  //       -> after PreprocessWrite() picks the memtable/blob generation:
  //            AppendSortedPutEntityToBatch()
  //
  // The BDW branch defers final serialization until `WriteImpl()` knows the
  // target memtable/blob generation, avoiding the old
  // serialize -> deserialize -> serialize cycle on the write hot path.
  // Staging buffer for the `PutEntity()` fast path when blob direct write may
  // need to rewrite wide-column values. `PutEntityFastPath()` records the
  // logical operations here instead of serializing them into a `WriteBatch`
  // immediately, then passes this struct to `WriteImpl()`. Once
  // `PreprocessWrite()` has selected the memtable/blob-file generation,
  // `WriteImpl()` materializes each op directly into the final batch with blob
  // direct-write preprocessing applied. This avoids the old
  // serialize-deserialize-serialize cycle on the write hot path.
  struct DeferredPutEntityBatch {
    // The fast path sorts columns up front so `WriteImpl()` can append the
    // final serialized entity without redoing that work.
    struct Op {
      // Target column family for the logical `PutEntity()` op.
      uint32_t column_family_id = 0;
      // Owned key bytes kept alive until `WriteImpl()` rebuilds the batch.
      std::string key;
      // Pre-sorted columns ready for final serialization and blob
      // direct-write preprocessing.
      WideColumns sorted_columns;
    };

    // Logical `PutEntity()` ops collected by the fast path and replayed into
    // the final `WriteBatch` inside `WriteImpl()`.
    std::vector<Op> ops;
  };
  // Rewrites a write batch for blob direct write when the current DB and batch
  // shape are compatible, recording touched managers and rollback metadata in
  // `blob_direct_write_ctx`.
  Status MaybeTransformBatchForBlobDirectWrite(
      const WriteOptions& write_options, WriteBatch** batch,
      bool should_write_to_memtable, bool lookup_from_write_thread,
      WriteBatch* transformed_storage,
      BlobDirectWriteContext* blob_direct_write_ctx);
  // Sorts and preprocesses one `PutEntity()` op, then appends the final
  // serialized form to `batch`.
  Status AppendPreprocessedPutEntityToBatch(
      const WriteOptions& write_options, WriteBatch* batch,
      ColumnFamilyHandle* column_family, const Slice& key,
      const WideColumns& columns,
      BlobDirectWriteContext* blob_direct_write_ctx);
  // Appends a `PutEntity()` op whose columns are already sorted, allowing the
  // fast path to avoid re-sorting before final serialization.
  Status AppendSortedPutEntityToBatch(
      const WriteOptions& write_options, WriteBatch* batch,
      uint32_t column_family_id, const Slice& key,
      const WideColumns& sorted_columns,
      BlobDirectWriteContext* blob_direct_write_ctx);
  // Specialized `PutEntity()` write path for a single column family. This
  // avoids building an intermediate serialized entity when blob direct write
  // needs to rewrite the final batch inside `WriteImpl()`.
  Status PutEntityFastPath(const WriteOptions& write_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           const WideColumns& columns);
  // Specialized `PutEntity(AttributeGroups)` path that defers final batch
  // materialization until `WriteImpl()` when any target CF may use blob direct
  // write.
  Status PutEntityFastPath(const WriteOptions& write_options, const Slice& key,
                           const AttributeGroups& attribute_groups);
  // Writes a batch prepared by the `PutEntity()` fast path. When
  // `deferred_put_entities` is present, `WriteImpl()` rebuilds `batch`
  // internally after `PreprocessWrite()` and disables normal write batching to
  // keep the staging state local to this write. `trace_batch` stays separate
  // so tracing can still emit the logical `PutEntity()` batch even when the
  // applied batch is materialized later inside `WriteImpl()`.
  Status WritePreprocessedPutEntityBatch(
      const WriteOptions& write_options, WriteBatch* batch,
      WriteBatch* trace_batch,
      const DeferredPutEntityBatch* deferred_put_entities = nullptr);
  // Flushes or syncs all blob direct-write managers touched by the current
  // transformed write before the write can proceed.
  Status SyncBlobDirectWriteManagers(
      const WriteOptions& write_options,
      const BlobDirectWriteContext& blob_direct_write_ctx);

  Status PipelinedWriteImpl(const WriteOptions& options, WriteBatch* updates,
                            WriteBatch* trace_batch,
                            WriteCallback* callback = nullptr,
                            UserWriteCallback* user_write_cb = nullptr,
                            uint64_t* wal_used = nullptr, uint64_t log_ref = 0,
                            bool disable_memtable = false,
                            uint64_t* seq_used = nullptr);

  // Write only to memtables without joining any write queue
  Status UnorderedWriteMemtable(const WriteOptions& write_options,
                                WriteBatch* my_batch, WriteCallback* callback,
                                uint64_t log_ref, SequenceNumber seq,
                                const size_t sub_batch_cnt);

  // Whether the batch requires to be assigned with an order
  enum AssignOrder : bool { kDontAssignOrder, kDoAssignOrder };
  // Whether it requires publishing last sequence or not
  enum PublishLastSeq : bool { kDontPublishLastSeq, kDoPublishLastSeq };

  // Join the write_thread to write the batch only to the WAL. It is the
  // responsibility of the caller to also write the write batch to the memtable
  // if it required.
  //
  // sub_batch_cnt is expected to be non-zero when assign_order = kDoAssignOrder
  // indicating the number of sub-batches in my_batch. A sub-patch is a subset
  // of the write batch that does not have duplicate keys. When seq_per_batch is
  // not set, each key is a separate sub_batch. Otherwise each duplicate key
  // marks start of a new sub-batch.
  Status WriteImplWALOnly(
      WriteThread* write_thread, const WriteOptions& options,
      WriteBatch* updates, WriteBatch* trace_batch, WriteCallback* callback,
      UserWriteCallback* user_write_cb, uint64_t* wal_used,
      const uint64_t log_ref, uint64_t* seq_used, const size_t sub_batch_cnt,
      PreReleaseCallback* pre_release_callback, const AssignOrder assign_order,
      const PublishLastSeq publish_last_seq, const bool disable_memtable);

  // write cached_recoverable_state_ to memtable if it is not empty
  // The writer must be the leader in write_thread_ and holding mutex_
  Status WriteRecoverableState();

  // Actual implementation of Close()
  virtual Status CloseImpl();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  // recovered_seq is set to less than kMaxSequenceNumber if the log's tail is
  // skipped.
  // recovery_ctx stores the context about version edits and all those
  // edits are persisted to new Manifest after successfully syncing the new WAL.
  virtual Status Recover(
      const std::vector<ColumnFamilyDescriptor>& column_families,
      bool read_only = false, bool error_if_wal_file_exists = false,
      bool error_if_data_exists_in_wals = false, bool is_retry = false,
      uint64_t* recovered_seq = nullptr,
      RecoveryContext* recovery_ctx = nullptr, bool* can_retry = nullptr);

  virtual bool OwnTablesAndLogs() const { return true; }

  // Read/create DB identity file (as appropriate), and write DB ID to
  // version_edit if provided.
  Status SetupDBId(const WriteOptions& write_options, bool read_only,
                   bool is_new_db, bool is_retry, VersionEdit* version_edit);
  // Assign db_id_ and write DB ID to version_edit if provided.
  void SetDBId(std::string&& id, bool read_only, VersionEdit* version_edit);

  // Collect a deduplicated collection of paths used by this DB, including
  // dbname_, DBOptions.db_paths, ColumnFamilyOptions.cf_paths.
  std::set<std::string> CollectAllDBPaths();

  // REQUIRES: db mutex held when calling this function, but the db mutex can
  // be released and re-acquired. Db mutex will be held when the function
  // returns.
  // It stores all existing data files (SST and Blob) in RecoveryContext. In
  // the meantime, we find out the largest file number present in the paths, and
  // bump up the version set's next_file_number_ to be 1 + largest_file_number.
  // recovery_ctx stores the context about version edits. All those edits are
  // persisted to new Manifest after successfully syncing the new WAL.
  Status MaybeUpdateNextFileNumber(RecoveryContext* recovery_ctx);

  // Track existing data files, including both referenced and unreferenced SST
  // and Blob files in SstFileManager. This is only called during DB::Open and
  // it's called before any file deletion start so that their deletion can be
  // properly rate limited.
  // Files may not be referenced in the MANIFEST because (e.g.
  // 1. It's best effort recovery;
  // 2. The VersionEdits referencing the SST files are appended to
  // RecoveryContext, DB crashes when syncing the MANIFEST, the VersionEdits are
  // still not synced to MANIFEST during recovery.)
  //
  // If the file is referenced in Manifest (typically that's the
  // vast majority of all files), since it already has the file size
  // on record, we don't need to query the file system. Otherwise, we query the
  // file system for the size of an unreferenced file.
  // REQUIRES: mutex unlocked
  void TrackExistingDataFiles(
      const std::vector<std::string>& existing_data_files);

  // Untrack data files in sst manager. This is only called during DB::Close on
  // an unowned SstFileManager, to return it to a consistent state.
  // REQUIRES: mutex unlocked
  void UntrackDataFiles();

  // SetDbSessionId() should be called in the constuctor DBImpl()
  // to ensure that db_session_id_ gets updated every time the DB is opened
  void SetDbSessionId();

  Status FailIfCfHasTs(const ColumnFamilyHandle* column_family) const;
  Status FailIfTsMismatchCf(ColumnFamilyHandle* column_family,
                            const Slice& ts) const;
  Status FailIfTableFilterWithRangeConversion(
      const ReadOptions& read_options,
      const MutableCFOptions& mutable_cf_options) const;

  // Check that the read timestamp `ts` is at or above the `full_history_ts_low`
  // timestamp in a `SuperVersion`. It's necessary to do this check after
  // grabbing the SuperVersion. If the check passed, the referenced SuperVersion
  // this read holds on to can ensure the read won't be affected if
  // `full_history_ts_low` is increased concurrently, and this achieves that
  // without explicitly locking by piggybacking the SuperVersion.
  Status FailIfReadCollapsedHistory(const ColumnFamilyData* cfd,
                                    const SuperVersion* sv,
                                    const Slice& ts) const;

  // recovery_ctx stores the context about version edits and
  // LogAndApplyForRecovery persist all those edits to new Manifest after
  // successfully syncing new WAL.
  // LogAndApplyForRecovery should be called only once during recovery and it
  // should be called when RocksDB writes to a first new MANIFEST since this
  // recovery.
  Status LogAndApplyForRecovery(RecoveryContext& recovery_ctx);

  // Schedule background work to open and validate SST files asynchronously.
  // Called when open_files_async is enabled.
  void ScheduleAsyncFileOpening();

  // Mark async file opening as not needed. Used by subclasses that load
  // table files through a different mechanism (e.g., ReactiveVersionSet).
  void MarkAsyncFileOpenNotNeeded();

  // Background work function for async file opening.
  static void BGWorkAsyncFileOpen(void* arg);

  // Block the until any in-flight async file open work has
  // completed. No-op when open_files_async is false. Returns early if
  // shutdown begins.
  void WaitForAsyncFileOpen();

  void InvokeWalFilterIfNeededOnColumnFamilyToWalNumberMap();

  // Return true to proceed with current WAL record whose content is stored in
  // `batch`. Return false to skip current WAL record.
  bool InvokeWalFilterIfNeededOnWalRecord(uint64_t wal_number,
                                          const std::string& wal_fname,
                                          log::Reader::Reporter& reporter,
                                          Status& status, bool& stop_replay,
                                          WriteBatch& batch);

  // Indicate DB was opened successfully
  bool opened_successfully_ = false;

 private:
  friend class DB;
  friend class DBImplReadOnly;
  friend class DBImplSecondary;
  friend class ErrorHandler;
  friend class InternalStats;
  friend class PessimisticTransaction;
  friend class TransactionBaseImpl;
  friend class WriteCommittedTxn;
  friend class WritePreparedTxn;
  friend class WritePreparedTxnDB;
  friend class WriteBatchWithIndex;
  friend class WriteUnpreparedTxnDB;
  friend class WriteUnpreparedTxn;
  friend class FileIngestionHandleImpl;

  friend class ForwardIterator;
  friend struct SuperVersion;
  friend class CompactedDBImpl;
  friend class DBImplFollower;
#ifndef NDEBUG
  friend class DedicatedCompactionPoolTest;
  friend class DBTest_ConcurrentFlushWAL_Test;
  friend class DBTest_MixedSlowdownOptionsStop_Test;
  friend class DBCompactionTest_CompactBottomLevelFilesWithDeletions_Test;
  friend class
      DBCompactionTest_StandaloneRangeTombstoneSnapshotAdvanceRequeuesScheduling_Test;
  friend class DBCompactionTest_CompactionDuringShutdown_Test;
  friend class DBCompactionTest_DelayCompactBottomLevelFilesWithDeletions_Test;
  friend class DBCompactionTest_DisableCompactBottomLevelFiles_Test;
  friend class StatsHistoryTest_PersistentStatsCreateColumnFamilies_Test;
  friend class DBTest2_ReadCallbackTest_Test;
  friend class WriteCallbackPTest_WriteWithCallbackTest_Test;
  friend class XFTransactionWriteHandler;
  friend class DBBlobIndexTest;
  friend class WriteUnpreparedTransactionTest_RecoveryTest_Test;
  friend class CompactionServiceTest_PreservedOptionsLocalCompaction_Test;
  friend class CompactionServiceTest_PreservedOptionsRemoteCompaction_Test;
#endif

  // Same as HasAnyBlobDirectWriteColumnFamily(), but requires `mutex_` held.
  bool HasAnyBlobDirectWriteColumnFamilyWithLockHeld();
  // Returns true if any BDW column family still owns blob files that have not
  // yet been made visible through MANIFEST. Requires `mutex_` held.
  bool HasInFlightBlobDirectWriteFilesWithLockHeld();
  // Creates and attaches the per-CF blob direct-write partition manager when
  // the column family is opened with the feature enabled.
  void MaybeInitBlobDirectWriteColumnFamily(
      ColumnFamilyData* cfd, const ColumnFamilyOptions& cf_options,
      const std::string& column_family_name);
  // Increments the DB-level count of live blob direct-write column families.
  void RegisterBlobDirectWriteColumnFamily();
  // Decrements the DB-level count of live blob direct-write column families.
  void UnregisterBlobDirectWriteColumnFamily();

  struct CompactionState;
  struct PrepickedCompaction;
  struct PurgeFileInfo;

  struct WriteContext {
    SuperVersionContext superversion_context;
    autovector<ReadOnlyMemTable*> memtables_to_free_;

    explicit WriteContext(bool create_superversion = false)
        : superversion_context(create_superversion) {}

    // No copying allowed
    WriteContext(const WriteContext&) = delete;
    WriteContext& operator=(const WriteContext&) = delete;
    WriteContext(WriteContext&&) = delete;
    WriteContext& operator=(WriteContext&&) = delete;

    ~WriteContext() {
      superversion_context.Clean();
      for (auto& m : memtables_to_free_) {
        delete m;
      }
    }
  };

  struct WalFileNumberSize {
    explicit WalFileNumberSize(uint64_t _number) : number(_number) {}
    WalFileNumberSize() {}
    void AddSize(uint64_t new_size) { size += new_size; }
    uint64_t number;
    uint64_t size = 0;
    bool getting_flushed = false;
  };

  struct LogWriterNumber {
    // pass ownership of _writer
    LogWriterNumber(uint64_t _number, log::Writer* _writer)
        : number(_number), writer(_writer) {}

    log::Writer* ReleaseWriter() {
      auto* w = writer;
      writer = nullptr;
      return w;
    }
    Status ClearWriter() {
      Status s;
      if (writer->file()) {
        // TODO: plumb Env::IOActivity, Env::IOPriority
        s = writer->WriteBuffer(WriteOptions());
        if (attempt_truncate_size < SIZE_MAX &&
            attempt_truncate_size < writer->file()->GetFileSize()) {
          Status s2 = writer->file()->writable_file()->Truncate(
              attempt_truncate_size, IOOptions{}, nullptr);
          // This is just a best effort attempt
          s2.PermitUncheckedError();
        }
      }
      delete writer;
      writer = nullptr;
      return s;
    }

    bool IsSyncing() { return getting_synced; }

    uint64_t GetPreSyncSize() {
      assert(getting_synced);
      return pre_sync_size;
    }

    void PrepareForSync() {
      assert(!getting_synced);
      // Ensure the head of logs_ is marked as getting_synced if any is.
      getting_synced = true;
      // If last sync failed on a later WAL, this could be a fully synced
      // and closed WAL that just needs to be recorded as synced in the
      // manifest.
      if (writer->file()) {
        // Size is expected to be monotonically increasing.
        assert(writer->file()->GetFlushedSize() >= pre_sync_size);
        pre_sync_size = writer->file()->GetFlushedSize();
      }
    }

    void FinishSync() {
      assert(getting_synced);
      getting_synced = false;
    }

    void SetAttemptTruncateSize(uint64_t size) {
      assert(attempt_truncate_size == SIZE_MAX);
      attempt_truncate_size = size;
    }

    uint64_t number;
    // Visual Studio doesn't support deque's member to be noncopyable because
    // of a std::unique_ptr as a member.
    log::Writer* writer;  // own

   private:
    // true for some prefix of logs_
    bool getting_synced = false;
    // The size of the file before the sync happens. This amount is guaranteed
    // to be persisted even if appends happen during sync so it can be used for
    // tracking the synced size in MANIFEST.
    uint64_t pre_sync_size = 0;
    // When < SIZE_MAX, attempt to truncate the WAL to this size on close,
    // because a bad entry was written to it beyond that point and it likely
    // won't be recoverable with the bad entry.
    uint64_t attempt_truncate_size = SIZE_MAX;
  };

  struct WalContext {
    explicit WalContext(bool need_sync = false)
        : need_wal_sync(need_sync), need_wal_dir_sync(need_sync) {}
    bool need_wal_sync = false;
    bool need_wal_dir_sync = false;
    log::Writer* writer = nullptr;
    WalFileNumberSize* wal_file_number_size = nullptr;
    uint64_t prev_size = SIZE_MAX;
  };

  // PurgeFileInfo is a structure to hold information of files to be deleted in
  // purge_files_
  struct PurgeFileInfo {
    std::string fname;
    std::string dir_to_sync;
    FileType type;
    uint64_t number;
    int job_id;
    PurgeFileInfo(std::string fn, std::string d, FileType t, uint64_t num,
                  int jid)
        : fname(fn), dir_to_sync(d), type(t), number(num), job_id(jid) {}
  };

  // Argument required by background flush thread.
  struct BGFlushArg {
    BGFlushArg()
        : cfd_(nullptr),
          max_memtable_id_(0),
          superversion_context_(nullptr),
          flush_reason_(FlushReason::kOthers) {}
    BGFlushArg(ColumnFamilyData* cfd, uint64_t max_memtable_id,
               SuperVersionContext* superversion_context,
               FlushReason flush_reason, bool atomic_flush)
        : cfd_(cfd),
          max_memtable_id_(max_memtable_id),
          superversion_context_(superversion_context),
          flush_reason_(flush_reason),
          atomic_flush_(atomic_flush) {}

    // Column family to flush.
    ColumnFamilyData* cfd_;
    // Maximum ID of memtable to flush. In this column family, memtables with
    // IDs smaller than this value must be flushed before this flush completes.
    uint64_t max_memtable_id_;
    // Pointer to a SuperVersionContext object. After flush completes, RocksDB
    // installs a new superversion for the column family. This operation
    // requires a SuperVersionContext object (currently embedded in JobContext).
    SuperVersionContext* superversion_context_;
    FlushReason flush_reason_;
    // Whether this flush should use atomic flush code path.
    bool atomic_flush_ = false;
  };

  // Argument passed to flush thread.
  struct FlushThreadArg {
    DBImpl* db_;

    Env::Priority thread_pri_;
  };

  // Information for a manual compaction
  struct ManualCompactionState {
    ManualCompactionState(ColumnFamilyData* _cfd, int _input_level,
                          int _output_level, uint32_t _output_path_id,
                          bool _exclusive, bool _disallow_trivial_move,
                          std::atomic<bool>* _canceled)
        : cfd(_cfd),
          input_level(_input_level),
          output_level(_output_level),
          output_path_id(_output_path_id),
          exclusive(_exclusive),
          disallow_trivial_move(_disallow_trivial_move),
          canceled(_canceled ? *_canceled : canceled_internal_storage) {}
    // When _canceled is not provided by ther user, we assign the reference of
    // canceled_internal_storage to it to consolidate canceled and
    // manual_compaction_paused since DisableManualCompaction() might be
    // called

    ColumnFamilyData* cfd;
    int input_level;
    int output_level;
    uint32_t output_path_id;
    Status status;
    bool done = false;
    bool in_progress = false;    // compaction request being processed?
    bool incomplete = false;     // only part of requested range compacted
    bool exclusive;              // current behavior of only one manual
    bool disallow_trivial_move;  // Force actual compaction to run
    const InternalKey* begin = nullptr;  // nullptr means beginning of key range
    const InternalKey* end = nullptr;    // nullptr means end of key range
    InternalKey* manual_end = nullptr;   // how far we are compacting
    InternalKey tmp_storage;   // Used to keep track of compaction progress
    InternalKey tmp_storage1;  // Used to keep track of compaction progress

    // When the user provides a canceled pointer in CompactRangeOptions, the
    // above varaibe is the reference of the user-provided
    // `canceled`, otherwise, it is the reference of canceled_internal_storage
    std::atomic<bool> canceled_internal_storage = false;
    std::atomic<bool>& canceled;  // Compaction canceled pointer reference
  };
  struct PrepickedCompaction {
    // background compaction takes ownership of `compaction`.
    // TODO(hx235): consider using std::shared_ptr for easier ownership
    // management
    Compaction* compaction;
    // caller retains ownership of `manual_compaction_state` as it is reused
    // across background compactions.
    ManualCompactionState* manual_compaction_state;  // nullptr if non-manual
    // task limiter token is requested during compaction picking.
    std::unique_ptr<TaskLimiterToken> task_token;
    // If true, `compaction` is picked temporarily to express compaction intent
    // and will be released before re-picking a real compaction based on the
    // updated LSM shape when thread associated with `compaction` is ready to
    // run
    bool need_repick;
  };

  struct CompactionArg {
    // caller retains ownership of `db`.
    DBImpl* db;
    // background compaction takes ownership of `prepicked_compaction`.
    PrepickedCompaction* prepicked_compaction;
    Env::Priority compaction_pri_;
    bool separate_remote_pool = false;
  };

  static bool IsRecoveryFlush(FlushReason flush_reason) {
    return flush_reason == FlushReason::kErrorRecoveryRetryFlush ||
           flush_reason == FlushReason::kErrorRecovery;
  }
  // Initialize the built-in column family for persistent stats. Depending on
  // whether on-disk persistent stats have been enabled before, it may either
  // create a new column family and column family handle or just a column family
  // handle.
  // Required: DB mutex held
  Status InitPersistStatsColumnFamily();

  // Persistent Stats column family has two format version key which are used
  // for compatibility check. Write format version if it's created for the
  // first time, read format version and check compatibility if recovering
  // from disk. This function requires DB mutex held at entrance but may
  // release and re-acquire DB mutex in the process.
  // Required: DB mutex held
  Status PersistentStatsProcessFormatVersion();

  // `io_activity` is used to construct the ReadOptions/WriteOptions for the
  // internal recovery work (e.g. the MANIFEST write in LogAndApply). Recovery
  // is driven by flush operations, so callers pass Env::IOActivity::kFlush.
  Status ResumeImpl(DBRecoverContext context, Env::IOActivity io_activity);

  void MaybeIgnoreError(Status* s) const;

  const Status CreateArchivalDirectory();

  // Create a column family, without some of the follow-up work yet
  Status CreateColumnFamilyImpl(const ReadOptions& read_options,
                                const WriteOptions& write_options,
                                const ColumnFamilyOptions& cf_options,
                                const std::string& cf_name,
                                ColumnFamilyHandle** handle);
  Status CreateColumnFamiliesImpl(
      const ReadOptions& read_options, const WriteOptions& write_options,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles);
  Status CreateColumnFamilyWithPreparedOptions(
      const ReadOptions& read_options, const WriteOptions& write_options,
      const ColumnFamilyOptions& sanitized_cf_options,
      const std::string& cf_name, uint64_t options_file_number,
      uint64_t options_file_size, ColumnFamilyHandle** handle);

  // Follow-up work to user creating a column family or (families)
  Status WrapUpCreateColumnFamilies(
      const WriteOptions& write_options,
      const std::vector<const ColumnFamilyOptions*>& cf_options);

  Status DropColumnFamilyImpl(ColumnFamilyHandle* column_family);
  Status DropColumnFamilyWithOptionsFile(ColumnFamilyHandle* column_family,
                                         uint64_t options_file_number,
                                         uint64_t options_file_size);

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();
  // Delete obsolete files and log status and information of file deletion
  void DeleteObsoleteFileImpl(int job_id, const std::string& fname,
                              const std::string& path_to_sync, FileType type,
                              uint64_t number);
  // Returns true when a blob file must be preserved because it is still
  // tracked by blob direct write or is still footer-less on disk.
  bool ShouldKeepBlobFileDuringPurge(uint64_t number,
                                     const std::string& blob_file_path);

  // Background process needs to call
  //     auto x = CaptureCurrentFileNumberInPendingOutputs()
  //     auto file_num = versions_->NewFileNumber();
  //     <do something>
  //     ReleaseFileNumberFromPendingOutputs(x)
  // This will protect any file with number `file_num` or greater from being
  // deleted while <do something> is running.
  // -----------
  // This function will capture current file number and append it to
  // pending_outputs_. This will prevent any background process to delete any
  // file created after this point.
  std::list<uint64_t>::iterator CaptureCurrentFileNumberInPendingOutputs();
  // This function should be called with the result of
  // CaptureCurrentFileNumberInPendingOutputs(). It then marks that any file
  // created between the calls CaptureCurrentFileNumberInPendingOutputs() and
  // ReleaseFileNumberFromPendingOutputs() can now be deleted (if it's not live
  // and blocked by any other pending_outputs_ calls)
  void ReleaseFileNumberFromPendingOutputs(
      std::unique_ptr<std::list<uint64_t>::iterator>& v);

  // Rolls back one prepared file ingestion (delete its staged files, release
  // the reserved file numbers)
  void RollbackPreparedFileIngestion(FileIngestionHandleImpl* const h);

  // Similar to pending_outputs, preserve OPTIONS file. Used for remote
  // compaction.
  std::list<uint64_t>::iterator CaptureOptionsFileNumber();
  void ReleaseOptionsFileNumber(
      std::unique_ptr<std::list<uint64_t>::iterator>& v);

  // Sets bg error if there is an error writing to WAL.
  IOStatus SyncClosedWals(const WriteOptions& write_options,
                          JobContext* job_context, VersionEdit* synced_wals,
                          bool error_recovery_in_prog);

  // Flush the in-memory write buffer to storage.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful. Then
  // installs a new super version for the column family.
  Status FlushMemTableToOutputFile(ColumnFamilyData* cfd,
                                   const MutableCFOptions& mutable_cf_options,
                                   bool* madeProgress, JobContext* job_context,
                                   FlushReason flush_reason,
                                   SuperVersionContext* superversion_context,
                                   LogBuffer* log_buffer,
                                   Env::Priority thread_pri);

  // Flush the memtables of (multiple) column families to multiple files on
  // persistent storage.
  Status FlushMemTablesToOutputFiles(
      const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
      JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri);

  Status AtomicFlushMemTablesToOutputFiles(
      const autovector<BGFlushArg>& bg_flush_args, bool* made_progress,
      JobContext* job_context, LogBuffer* log_buffer, Env::Priority thread_pri);

  // REQUIRES: log_numbers are sorted in ascending order
  // corrupted_wal_found is set to true if we recover from a corrupted log file.
  Status RecoverLogFiles(const std::vector<uint64_t>& log_numbers,
                         SequenceNumber* next_sequence, bool read_only,
                         bool is_retry, bool* corrupted_wal_found,
                         RecoveryContext* recovery_ctx);

  void SetupLogFilesRecovery(
      const std::vector<uint64_t>& wal_numbers,
      std::unordered_map<int, VersionEdit>* version_edits, int* job_id,
      uint64_t* min_wal_number);

  Status ProcessLogFiles(const std::vector<uint64_t>& wal_numbers,
                         bool read_only, bool is_retry, uint64_t min_wal_number,
                         int job_id, SequenceNumber* next_sequence,
                         std::unordered_map<int, VersionEdit>* version_edits,
                         bool* corrupted_wal_found,
                         RecoveryContext* recovery_ctx);

  Status ProcessLogFile(
      uint64_t wal_number, uint64_t min_wal_number, bool is_retry,
      bool read_only, int job_id, SequenceNumber* next_sequence,
      bool* stop_replay_for_corruption, bool* stop_replay_by_wal_filter,
      uint64_t* corrupted_wal_number, bool* corrupted_wal_found,
      std::unordered_map<int, VersionEdit>* version_edits, bool* flushed,
      PredecessorWALInfo& predecessor_wal_info);

  void SetupLogFileProcessing(uint64_t wal_number);

  Status InitializeLogReader(uint64_t wal_number, bool is_retry,
                             std::string& fname,

                             bool stop_replay_for_corruption,
                             uint64_t min_wal_number,
                             const PredecessorWALInfo& predecessor_wal_info,
                             bool* const old_log_record,
                             Status* const reporter_status,
                             DBOpenLogRecordReadReporter* reporter,
                             std::unique_ptr<log::Reader>& reader);
  Status ProcessLogRecord(
      Slice record, const std::unique_ptr<log::Reader>& reader,
      const UnorderedMap<uint32_t, size_t>& running_ts_sz, uint64_t wal_number,
      const std::string& fname, bool read_only, int job_id,
      const std::function<void()>& logFileDropped,
      DBOpenLogRecordReadReporter* reporter, uint64_t* record_checksum,
      SequenceNumber* last_seqno_observed, SequenceNumber* next_sequence,
      bool* stop_replay_for_corruption, Status* status,
      bool* stop_replay_by_wal_filter,
      std::unordered_map<int, VersionEdit>* version_edits, bool* flushed);

  Status InitializeWriteBatchForLogRecord(
      Slice record, const std::unique_ptr<log::Reader>& reader,
      const UnorderedMap<uint32_t, size_t>& running_ts_sz, WriteBatch* batch,
      std::unique_ptr<WriteBatch>& new_batch, WriteBatch*& batch_to_use,
      uint64_t* record_checksum);

  void MaybeReviseStopReplayForCorruption(
      SequenceNumber sequence, SequenceNumber const* const next_sequence,
      bool* stop_replay_for_corruption);

  Status InsertLogRecordToMemtable(WriteBatch* batch_to_use,
                                   uint64_t wal_number,
                                   SequenceNumber* next_sequence,
                                   bool* has_valid_writes, bool read_only);

  Status MaybeWriteLevel0TableForRecovery(
      bool has_valid_writes, bool read_only, uint64_t wal_number, int job_id,
      SequenceNumber const* const next_sequence,
      std::unordered_map<int, VersionEdit>* version_edits, bool* flushed);

  Status HandleNonOkStatusOrOldLogRecord(
      uint64_t wal_number, SequenceNumber const* const next_sequence,
      Status status, const DBOpenLogRecordReadReporter& reporter,
      bool* old_log_record, bool* stop_replay_for_corruption,
      uint64_t* corrupted_wal_number, bool* corrupted_wal_found);

  Status UpdatePredecessorWALInfo(uint64_t wal_number,
                                  const SequenceNumber last_seqno_observed,
                                  const std::string& fname,
                                  PredecessorWALInfo& predecessor_wal_info);

  void FinishLogFileProcessing(const Status& status,
                               const SequenceNumber* next_sequence);

  // Return `Status::Corruption()` when `stop_replay_for_corruption == true` and
  // exits inconsistency between SST and WAL data
  Status MaybeHandleStopReplayForCorruptionForInconsistency(
      bool stop_replay_for_corruption, uint64_t corrupted_wal_number);

  Status MaybeFlushFinalMemtableOrRestoreActiveLogFiles(
      const std::vector<uint64_t>& wal_numbers, bool read_only, int job_id,
      bool flushed, std::unordered_map<int, VersionEdit>* version_edits,
      RecoveryContext* recovery_ctx);

  // Check that DB sequence number is not set back during recovery between
  // replaying of WAL files and between replaying of WriteBatches.
  Status CheckSeqnoNotSetBackDuringRecovery(SequenceNumber prev_next_seqno,
                                            SequenceNumber current_next_seqno);

  void FinishLogFilesRecovery(int job_id, const Status& status);
  // The following two methods are used to flush a memtable to
  // storage. The first one is used at database RecoveryTime (when the
  // database is opened) and is heavyweight because it holds the mutex
  // for the entire period. The second method WriteLevel0Table supports
  // concurrent flush memtables to storage.
  Status WriteLevel0TableForRecovery(int job_id, ColumnFamilyData* cfd,
                                     MemTable* mem, VersionEdit* edit);

  // Get the size of a log file and, if truncate is true, truncate the
  // log file to its actual size, thereby freeing preallocated space.
  // Return success even if truncate fails
  Status GetLogSizeAndMaybeTruncate(uint64_t wal_number, bool truncate,
                                    WalFileNumberSize* log);

  // Restore alive_wal_files_ and wals_total_size_ after recovery.
  // It needs to run only when there's no flush during recovery
  // (e.g. avoid_flush_during_recovery=true). May also trigger flush
  // in case wals_total_size > max_total_wal_size.
  Status RestoreAliveLogFiles(const std::vector<uint64_t>& log_numbers);

  // num_bytes: for slowdown case, delay time is calculated based on
  //            `num_bytes` going through.
  Status DelayWrite(uint64_t num_bytes, WriteThread& write_thread,
                    const WriteOptions& write_options);

  // Begin stalling of writes when memory usage increases beyond a certain
  // threshold.
  Status WriteBufferManagerStallWrites(WriteContext* write_context);

  Status ThrottleLowPriWritesIfNeeded(const WriteOptions& write_options,
                                      WriteBatch* my_batch);

  // REQUIRES: mutex locked and in write thread.
  Status ScheduleFlushes(WriteContext* context);

  void MaybeFlushStatsCF(autovector<ColumnFamilyData*>* cfds);

  Status TrimMemtableHistory(WriteContext* context);

  // Switches the current live memtable to immutable/read-only memtable.
  // A new WAL is created if the current WAL is not empty.
  // If `new_imm` is not nullptr, it will be added as the newest immutable
  // memtable, if and only if OK status is returned.
  // `last_seqno` needs to be provided if `new_imm` is not nullptr. It is
  // the value of versions_->LastSequence() after the write that ingests new_imm
  // is done.
  //
  // REQUIRES: mutex_ is held
  // REQUIRES: this thread is currently at the front of the writer queue
  // REQUIRES: this thread is currently at the front of the 2nd writer queue if
  // two_write_queues_ is true (This is to simplify the reasoning.)
  Status SwitchMemtable(ColumnFamilyData* cfd, WriteContext* context,
                        ReadOnlyMemTable* new_imm = nullptr,
                        SequenceNumber last_seqno = 0);

  // `refresh_wbm_accounting` can be false when a caller switches a batch of
  // memtables and refreshes the DB-wide accounting once after the batch.
  Status SwitchMemtableImpl(ColumnFamilyData* cfd, WriteContext* context,
                            ReadOnlyMemTable* new_imm,
                            SequenceNumber last_seqno,
                            bool refresh_wbm_accounting);

  // REQUIRES: mutex_ is held
  void UpdateWBMAccountingAfterMemtableSwitch(bool refresh_now);

  // Select and output column families qualified for atomic flush in
  // `selected_cfds`. If `provided_candidate_cfds` is non-empty, it will be used
  // as candidate CFs to select qualified ones from. Otherwise, all column
  // families are used as candidate to select from.
  //
  // REQUIRES: mutex held
  void SelectColumnFamiliesForAtomicFlush(
      autovector<ColumnFamilyData*>* selected_cfds,
      const autovector<ColumnFamilyData*>& provided_candidate_cfds = {},
      FlushReason flush_reason = FlushReason::kOthers);

  // Force current memtable contents to be flushed.
  Status FlushMemTable(ColumnFamilyData* cfd, const FlushOptions& options,
                       FlushReason flush_reason,
                       bool entered_write_thread = false);

  // Atomic-flush memtables from quanlified CFs among `provided_candidate_cfds`
  // (if non-empty) or amomg all column families and atomically record the
  // result to the MANIFEST.
  Status AtomicFlushMemTables(
      const FlushOptions& options, FlushReason flush_reason,
      const autovector<ColumnFamilyData*>& provided_candidate_cfds = {},
      bool entered_write_thread = false);

  enum class WriteThreadJoinMode { kBlocking, kNonBlocking };

  Status FlushMemTableImpl(ColumnFamilyData* cfd, const FlushOptions& options,
                           FlushReason flush_reason, bool entered_write_thread,
                           WriteThreadJoinMode join_mode, bool* made_progress);

  Status AtomicFlushMemTablesImpl(
      const FlushOptions& options, FlushReason flush_reason,
      const autovector<ColumnFamilyData*>& provided_candidate_cfds,
      bool entered_write_thread, WriteThreadJoinMode join_mode,
      bool* made_progress);

  // REQUIRES: mutex locked and write queues drained up to the recovery flush
  // fence that is about to switch memtables.
  void MaybeSyncLastSequenceWithAllocatedForRecovery(FlushReason flush_reason);

  Status RetryFlushesForErrorRecovery(FlushReason flush_reason, bool wait);

  // Wait until flushing this column family won't stall writes
  Status WaitUntilFlushWouldNotStallWrites(ColumnFamilyData* cfd,
                                           bool* flush_needed);

  // Wait for memtable flushed.
  // If flush_memtable_id is non-null, wait until the memtable with the ID
  // gets flush. Otherwise, wait until the column family don't have any
  // memtable pending flush.
  // resuming_from_bg_err indicates whether the caller is attempting to resume
  // from background error.
  // If wait_for_listener_notifications is true, also wait until the
  // EventListener::OnFlushCompleted callbacks for the flushed memtables have
  // finished running (see FlushOptions::listener_wait).
  Status WaitForFlushMemTable(
      ColumnFamilyData* cfd, const uint64_t* flush_memtable_id = nullptr,
      bool resuming_from_bg_err = false,
      std::optional<FlushReason> flush_reason = std::nullopt,
      bool wait_for_listener_notifications = false) {
    return WaitForFlushMemTables({cfd}, {flush_memtable_id},
                                 resuming_from_bg_err, flush_reason,
                                 wait_for_listener_notifications);
  }
  // Wait for memtables to be flushed for multiple column families.
  Status WaitForFlushMemTables(
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const uint64_t*>& flush_memtable_ids,
      bool resuming_from_bg_err, std::optional<FlushReason> flush_reason,
      bool wait_for_listener_notifications = false);

  inline void WaitForPendingWrites() {
    mutex_.AssertHeld();
    TEST_SYNC_POINT("DBImpl::WaitForPendingWrites:BeforeBlock");
    // In case of pipelined write is enabled, wait for all pending memtable
    // writers.
    if (immutable_db_options_.enable_pipelined_write) {
      // Memtable writers may call DB::Get in case max_successive_merges > 0,
      // which may lock mutex. Unlocking mutex here to avoid deadlock.
      mutex_.Unlock();
      write_thread_.WaitForMemTableWriters();
      mutex_.Lock();
    }

    if (immutable_db_options_.unordered_write) {
      // Wait for the ones who already wrote to the WAL to finish their
      // memtable write.
      if (pending_memtable_writes_.load() != 0) {
        // XXX: suspicious wait while holding DB mutex?
        std::unique_lock<std::mutex> guard(switch_mutex_);
        switch_cv_.wait(guard,
                        [&] { return pending_memtable_writes_.load() == 0; });
      }
    } else {
      // (Writes are finished before the next write group starts.)
    }

    // Wait for any LockWAL to clear
    while (!lock_wal_owner_thread_id_counts_.empty()) {
      bg_cv_.Wait();
    }
  }

  // TaskType is used to identify tasks in thread-pool, currently only
  // differentiate manual compaction, which could be unscheduled from the
  // thread-pool.
  enum class TaskType : uint8_t {
    kDefault = 0,
    kManualCompaction = 1,
    kCount = 2,
  };

  // Task tag is used to identity tasks in thread-pool, which is
  // dbImpl obj address + type
  inline void* GetTaskTag(TaskType type) {
    return GetTaskTag(static_cast<uint8_t>(type));
  }

  inline void* GetTaskTag(uint8_t type) {
    return static_cast<uint8_t*>(static_cast<void*>(this)) + type;
  }

  // Checks DB-local write queues; manager-wide stall state would reject healthy
  // DBs. REQUIRES: mutex_ held.
  bool WouldBlockJoiningWriteThread() {
    mutex_.AssertHeld();
    return write_thread_.GetBegunCountOfOutstandingStall() != 0 ||
           (two_write_queues_ &&
            nonmem_write_thread_.GetBegunCountOfOutstandingStall() != 0);
  }

  Status EnterWriteThreadForNonBlockingFlush(WriteThread::Writer* w,
                                             WriteThread::Writer* nonmem_w);

  // REQUIRES: mutex locked and in write thread.
  void AssignAtomicFlushSeq(const autovector<ColumnFamilyData*>& cfds);

  // REQUIRES: mutex locked and in write thread.
  Status SwitchWAL(WriteContext* write_context);

  // REQUIRES: mutex locked and in write thread.
  Status HandleWriteBufferManagerFlush(WriteContext* write_context);

  // Column families and memory eligible for a WriteBufferManager flush.
  struct FlushableCFs {
    ColumnFamilyData* largest = nullptr;  // most reclaimable memtable memory
    ColumnFamilyData* oldest = nullptr;   // smallest memtable creation seq
    size_t largest_mem = 0;
    size_t total_mem = 0;
  };

  // Shared selection for bidding and flushing. REQUIRES: mutex_ held.
  FlushableCFs CollectFlushableCFs(bool include_waiting_immutable);

  // Registers an opened read-write DB without holding mutex_.
  void MaybeRegisterFlushInitiator();

  // Refreshes the published mutable-memory counters after memtables change.
  // REQUIRES: mutex_ held.
  void RefreshFlushableMemAccounting();

  bool TryRefreshFlushableMemAccounting();

  // Queues one WBM flush; false lets the coordinator try another DB.
  bool ScheduleWriteBufferManagerFlush();

  void BackgroundCallWBMFlush();

  // REQUIRES: mutex locked
  Status PreprocessWrite(const WriteOptions& write_options,
                         WalContext* log_context, WriteContext* write_context);

  // Merge write batches in the write group into merged_batch.
  // Returns OK if merge is successful.
  // Returns Corruption if corruption in write batch is detected.
  Status MergeBatch(const WriteThread::WriteGroup& write_group,
                    WriteBatch* tmp_batch, WriteBatch** merged_batch,
                    size_t* write_with_wal, WriteBatch** to_be_cached_state);

  IOStatus WriteToWAL(const WriteBatch& merged_batch,
                      const WriteOptions& write_options,
                      log::Writer* log_writer, uint64_t* wal_used,
                      uint64_t* log_size,
                      WalFileNumberSize& wal_file_number_size,
                      SequenceNumber sequence);

  IOStatus WriteGroupToWAL(const WriteThread::WriteGroup& write_group,
                           log::Writer* log_writer, uint64_t* wal_used,
                           bool need_wal_sync, bool need_wal_dir_sync,
                           SequenceNumber sequence,
                           WalFileNumberSize& wal_file_number_size);

  IOStatus ConcurrentWriteGroupToWAL(const WriteThread::WriteGroup& write_group,
                                     uint64_t* wal_used,
                                     SequenceNumber* last_sequence,
                                     size_t seq_inc);

  // Used by WriteImpl to update bg_error_ if paranoid check is enabled.
  // Caller must hold mutex_.
  void WriteStatusCheckOnLocked(const Status& status);

  // Used by WriteImpl to update bg_error_ if paranoid check is enabled.
  void WriteStatusCheck(const Status& status);

  // Used by WriteImpl to update bg_error_ when IO error happens, e.g., write
  // WAL, sync WAL fails, if paranoid check is enabled.
  void WALIOStatusCheck(const IOStatus& status, uint64_t failed_wal_number = 0,
                        SequenceNumber failed_wal_sequence = 0,
                        SequenceNumber failed_wal_sequence_start = 0);

  // Extends the inclusive WAL-number cutoff that must not receive more I/O.
  // This is lock-free because a WAL error can race with public SyncWAL().
  void ExtendWALRecoveryCutoff(uint64_t failed_wal_number);

  // Used by WriteImpl to update bg_error_ in case of memtable insert error.
  void HandleMemTableInsertFailure(const Status& nonok_memtable_insert_status);

  Status CompactFilesImpl(const CompactionOptions& compact_options,
                          ColumnFamilyData* cfd, Version* version,
                          const std::vector<std::string>& input_file_names,
                          std::vector<std::string>* const output_file_names,
                          const int output_level, int output_path_id,
                          JobContext* job_context, LogBuffer* log_buffer,
                          CompactionJobInfo* compaction_job_info);

  // Helper function to perform trivial move by updating manifest metadata
  // without rewriting data files. This is called when IsTrivialMove() is true.
  // REQUIRES: mutex held
  // Populates the Compaction's edit with DeleteFile/AddFile entries for
  // a trivial move (no data rewriting). Does not commit the edit.
  void PrepareTrivialMoveEdit(Compaction& c, LogBuffer* log_buffer,
                              size_t& moved_files, size_t& moved_bytes);

  // REQUIRES: mutex held, PrepareTrivialMoveEdit already called
  // Commits the trivial move by calling LogAndApply on the prepared edit.
  // Returns: Status of the manifest write
  Status CommitTrivialMove(Compaction& c, bool& compaction_released);

  // REQUIRES: mutex unlocked
  void TrackOrUntrackFiles(const std::vector<std::string>& existing_data_files,
                           bool track);

  void MaybeScheduleFlushOrCompaction();
  bool UseSeparateRemoteCompactionPool() const {
    mutex_.AssertHeld();
    return immutable_db_options_.compaction_service != nullptr &&
           mutable_db_options_.use_separate_remote_compaction_pool;
  }
  int LocalCompactionsScheduled() const {
    mutex_.AssertHeld();
    return bg_compaction_scheduled_ + bg_bottom_compaction_scheduled_;
  }
  int CompactionsScheduled() const {
    mutex_.AssertHeld();
    return LocalCompactionsScheduled() + remote_coordinators_scheduled_;
  }
  Status ScheduleCompaction(CompactionArg* arg, void* tag);
  class RemoteCompactionCompletionGuard;
  void ScheduleRemoteCompaction(std::function<void()> work);
  void ScheduleRemoteCompactionLocked(std::function<void()> work);
  int RemoteCompactionPoolBaseline() const;
  void ShrinkRemoteCompactionPool();
  void ReleaseRemoteCompactionPool();
  void ConfigureCompactionExecutors(CompactionJob* job, ColumnFamilyData* cfd,
                                    const std::atomic<bool>& canceled,
                                    bool is_manual, bool separate_remote_pool);
  Status LocalCompactionStatus(ColumnFamilyData* cfd,
                               const std::atomic<bool>& canceled);
  Status RunLocalCompaction(std::function<void()> work, Env::Priority priority,
                            ColumnFamilyData* cfd,
                            const std::atomic<bool>& canceled, bool is_manual);
  IOStatus CopyTrivialCompactionFiles(Compaction* c, LogBuffer* log_buffer,
                                      std::vector<FileMetaData>* out_files,
                                      int job_id, bool db_mutex_held);
  struct LocalCompactionTask {
    DBImpl* db;
    ColumnFamilyData* cfd;
    const std::atomic<bool>* canceled;
    std::function<void()> work;
    Env::Priority priority;
    Status status;
    bool done = false;
  };
  static void BGWorkLocalCompaction(void* arg);
  static void UnscheduleLocalCompaction(void* arg);

  BackgroundJobPressure CaptureBackgroundJobPressure() const;
  void NotifyOnBackgroundJobPressureChanged();

  struct FlushRequest {
    FlushReason flush_reason;
    // Whether this flush request should use the atomic flush code path.
    bool atomic_flush = false;
    // A map from column family to flush to largest memtable id to persist for
    // each column family. Once all the memtables whose IDs are smaller than or
    // equal to this per-column-family specified value, this flush request is
    // considered to have completed its work of flushing this column family.
    // After completing the work for all column families in this request, this
    // flush is considered complete. EnqueuePendingFlush() acquires one
    // reference for each CFD when it successfully queues this request.
    // PopFirstFromFlushQueue() transfers responsibility for those references
    // to its caller, which must release each one with UnrefAndTryDelete() after
    // processing or discarding the request.
    std::unordered_map<ColumnFamilyData*, uint64_t>
        cfd_to_max_mem_id_to_persist;

#ifndef NDEBUG
    int reschedule_count = 1;
#endif /* !NDEBUG */
  };

  // In case of atomic flush, generates a `FlushRequest` for the latest atomic
  // cuts for these `cfds`. Atomic cuts are recorded in
  // `AssignAtomicFlushSeq()`. For each entry in `cfds`, all CFDs sharing the
  // same latest atomic cut must also be present.
  //
  // REQUIRES: mutex held
  void GenerateFlushRequest(const autovector<ColumnFamilyData*>& cfds,
                            FlushReason flush_reason, FlushRequest* req);

  // Below functions are for executing flush, compaction in the background. A
  // dequeue is the communication channel between threads that asks for the work
  // to be done and the available threads in the thread pool that pick it up to
  // execute it. We use these terminologies to describe the state of the work
  // and its transitions:
  // 1) It becomes pending once it's successfully enqueued into the
  //    corresponding dequeue, a work in this state is also called unscheduled.
  //    Counter `unscheduled_*_` counts work in this state.
  // 2) When `MaybeScheduleFlushOrCompaction` schedule a thread to run `BGWork*`
  //    for the work, it becomes scheduled
  //    Counter `bg_*_scheduled_` counts work in this state.
  // 3) Once the thread start to execute `BGWork*`, the work is popped from the
  //    dequeue, it is now in running state
  //    Counter `num_running_*_` counts work in this state.
  // 4) Eventually, the work is finished. We don't need to specifically track
  //    finished work.

  // Returns true if `req` is successfully enqueued.
  bool EnqueuePendingFlush(const FlushRequest& req);

  void EnqueuePendingCompaction(ColumnFamilyData* cfd);
  void SchedulePendingPurge(std::string fname, std::string dir_to_sync,
                            FileType type, uint64_t number, int job_id);
  static void BGWorkCompaction(void* arg);
  // Runs a pre-chosen universal compaction involving bottom level in a
  // separate, bottom-pri thread pool.
  static void BGWorkBottomCompaction(void* arg);
  static void BGWorkFlush(void* arg);
  static void BGWorkPurge(void* arg);
  static void UnscheduleCompactionCallback(void* arg);
  static void UnscheduleFlushCallback(void* arg);
  void BackgroundCallCompaction(PrepickedCompaction* prepicked_compaction,
                                Env::Priority thread_pri,
                                bool separate_remote_pool);
  void BackgroundCallFlush(Env::Priority thread_pri);
  void BackgroundCallPurge();
  // Recursively removes all children of the DB session temporary directory.
  // Best effort: errors are logged, not returned. REQUIRES: mutex_ not held.
  void CleanupSessionTmpDir();
  Status BackgroundCompaction(bool* madeProgress, JobContext* job_context,
                              LogBuffer* log_buffer,
                              PrepickedCompaction* prepicked_compaction,
                              Env::Priority thread_pri,
                              bool separate_remote_pool,
                              int* deferred_compactions);
  Status BackgroundFlush(bool* madeProgress, JobContext* job_context,
                         LogBuffer* log_buffer, FlushReason* reason,
                         bool* flush_rescheduled_to_retain_udt,
                         Env::Priority thread_pri);

  Compaction* CreateIntendedCompactionForwardedToBottomPriorityPool(
      Compaction* c);
  bool EnoughRoomForCompaction(ColumnFamilyData* cfd,
                               const std::vector<CompactionInputFiles>& inputs,
                               bool* sfm_bookkeeping, LogBuffer* log_buffer);

  // Request compaction tasks token from compaction thread limiter.
  // It always succeeds if force = true or limiter is disable.
  bool RequestCompactionToken(ColumnFamilyData* cfd, bool force,
                              std::unique_ptr<TaskLimiterToken>* token,
                              LogBuffer* log_buffer);

  // Return true if the `FlushRequest` can be rescheduled to retain the UDT.
  // Only true if there are user-defined timestamps in the involved MemTables
  // with newer than cutoff timestamp `full_history_ts_low` and not flushing
  // immediately will not cause entering write stall mode.
  bool ShouldRescheduleFlushRequestToRetainUDT(const FlushRequest& flush_req);

  // Schedule background tasks
  Status StartPeriodicTaskScheduler();

  // Compute the repeat period for the kTriggerCompaction task, which ensures
  // compactions not dependent on writes (flushes) are eventually triggered when
  // there are no writes (flushes). NOT thread safe; only called during DB open
  // (StartPeriodicTaskScheduler). KNOWN LIMITATION: doesn't get updated with
  // dynamic option updates. (Probably not worth the extra complexity.)
  uint64_t ComputeTriggerCompactionPeriod();

  // Cancel scheduled periodic tasks
  Status CancelPeriodicTaskScheduler();

  Status RegisterRecordSeqnoTimeWorker();

  void PrintStatistics();

  size_t EstimateInMemoryStatsHistorySize() const;

  // Return the minimum empty level that could hold the total data in the
  // input level. Return the input level, if such level could not be found.
  int FindMinimumEmptyLevelFitting(ColumnFamilyData* cfd,
                                   const MutableCFOptions& mutable_cf_options,
                                   int level);

  // Move the files in the input level to the target level.
  // If target_level < 0, automatically calculate the minimum level that could
  // hold the data set.
  Status ReFitLevel(ColumnFamilyData* cfd, int level, int target_level = -1);

  // helper functions for adding and removing from flush & compaction queues
  void AddToCompactionQueue(ColumnFamilyData* cfd);
  ColumnFamilyData* PopFirstFromCompactionQueue();
  FlushRequest PopFirstFromFlushQueue();

  struct CompactionQueueThrottled {};
  struct CompactionQueueParked {};
  struct CompactionQueuePicked {
    ColumnFamilyData* cfd;
  };
  using CompactionQueuePickResult =
      std::variant<CompactionQueuePicked, CompactionQueueThrottled,
                   CompactionQueueParked>;

  // Pick the first unthrottled compaction with task token from queue.
  CompactionQueuePickResult PickCompactionFromQueue(
      std::unique_ptr<TaskLimiterToken>* token, LogBuffer* log_buffer);
  bool IsCompactionAborted(ColumnFamilyData* cfd) const;
  bool MustWaitForCompaction(ColumnFamilyData* cfd);
  bool HasPendingManualCompaction(ColumnFamilyData* cfd);
  bool RestoreParkedCompaction(ColumnFamilyData* cfd);
  void BeginInFlightCompaction(ColumnFamilyData* cfd);
  bool EndInFlightCompaction(ColumnFamilyData* cfd);

  IOStatus SyncWalImpl(bool include_current_wal,
                       const WriteOptions& write_options,
                       JobContext* job_context, VersionEdit* synced_wals,
                       bool error_recovery_in_prog);

  // helper function to call after some of the logs_ were synced
  void MarkLogsSynced(uint64_t up_to, bool synced_dir, VersionEdit* edit);
  Status ApplyWALToManifest(const ReadOptions& read_options,
                            const WriteOptions& write_options,
                            VersionEdit* edit);
  // WALs with log number up to up_to are not synced successfully.
  void MarkLogsNotSynced(uint64_t up_to);

  enum class SnapshotLockMode {
    kTryWithoutDBMutex,
    kDBMutexHeld,
    kAcquireDBMutex,
  };
  SnapshotImpl* GetSnapshotImpl(bool is_write_conflict_boundary,
                                SnapshotLockMode lock_mode);

  // Updates compaction state after the oldest snapshot advances and also
  // reconciles `installed_cfd`, when non-null, if its Version has not processed
  // the current oldest snapshot yet. Requires mutex_.
  void UpdateOldestSnapshotAndScheduleWork(SequenceNumber oldest_snapshot,
                                           uint64_t snapshot_release_count,
                                           ColumnFamilyData* installed_cfd);
  bool ReconcileOldestSnapshotForColumnFamily(ColumnFamilyData* cfd,
                                              SequenceNumber oldest_snapshot);
  void UpdateSnapshotCompactionThresholds();

  // If snapshot_seq != kMaxSequenceNumber, then this function can only be
  // called from the write thread that publishes sequence numbers to readers.
  // For 1) write-committed, or 2) write-prepared + one-write-queue, this will
  // be the write thread performing memtable writes. For write-prepared with
  // two write queues, this will be the write thread writing commit marker to
  // the WAL.
  // If snapshot_seq == kMaxSequenceNumber, this function is called by a caller
  // ensuring no writes to the database.
  std::pair<Status, std::shared_ptr<const SnapshotImpl>>
  CreateTimestampedSnapshotImpl(SequenceNumber snapshot_seq, uint64_t ts,
                                bool lock = true);

  uint64_t GetMaxTotalWalSize() const;

  FSDirectory* GetDataDir(ColumnFamilyData* cfd, size_t path_id) const;

  Status MaybeReleaseTimestampedSnapshotsAndCheck();

  Status MaybeWriteWalMarkersToManifestOnClose();

  Status CloseHelper();

  void WaitForBackgroundWork();

  // Background threads call this function, which is just a wrapper around
  // the InstallSuperVersion() function. Background threads carry
  // sv_context to allow allocation of SuperVersion object outside of holding
  // the DB mutex.
  // All ColumnFamily state changes go through this function. Here we analyze
  // the new state and we schedule background work if we detect that the new
  // state needs flush or compaction.
  // See also InstallSuperVersionForConfigChange().
  void InstallSuperVersionAndScheduleWork(
      ColumnFamilyData* cfd, SuperVersionContext* sv_context,
      bool defer_snapshot_refresh,
      std::optional<std::shared_ptr<SeqnoToTimeMapping>>
          new_seqno_to_time_mapping = {});

  // A variant of InstallSuperVersionAndScheduleWork() that must be used for
  // new CFs or for changes to mutable_cf_options. This is so that it can
  // update seqno_to_time_mapping cached for the new SuperVersion as relevant.
  void InstallSuperVersionForConfigChange(ColumnFamilyData* cfd,
                                          SuperVersionContext* sv_context);

  bool GetIntPropertyInternal(ColumnFamilyData* cfd,
                              const DBPropertyInfo& property_info,
                              bool is_locked, uint64_t* value);
  bool GetPropertyHandleOptionsStatistics(std::string* value);

  bool HasPendingManualCompaction();
  bool HasExclusiveManualCompaction();
  void AddManualCompaction(ManualCompactionState* m);
  void RemoveManualCompaction(ManualCompactionState* m);
  bool ShouldntRunManualCompaction(ManualCompactionState* m);
  bool HaveManualCompaction(ColumnFamilyData* cfd);
  bool MCOverlap(ManualCompactionState* m, ManualCompactionState* m1);
  void UpdateFIFOCompactionStatus(const std::unique_ptr<Compaction>& c);

  // May open and read table files for table property.
  // Should not be called while holding mutex_.
  void BuildCompactionJobInfo(const ColumnFamilyData* cfd, Compaction* c,
                              const Status& st,
                              const CompactionJobStats& compaction_job_stats,
                              const int job_id,
                              CompactionJobInfo* compaction_job_info) const;
  // Reserve the next 'num' file numbers for to-be-ingested external SST files,
  // and return the current file_number in 'next_file_number'.
  // Write a version edit to the MANIFEST.
  Status ReserveFileNumbersBeforeIngestion(ColumnFamilyData* cfd, uint64_t num,
                                           uint64_t* next_file_number);

  bool ShouldPurge(uint64_t file_number) const;
  void MarkAsGrabbedForPurge(uint64_t file_number);

  size_t GetWalPreallocateBlockSize(uint64_t write_buffer_size) const;
  Env::WriteLifeTimeHint CalculateWALWriteHint() { return Env::WLTH_SHORT; }

  // Returns true when async WAL precreation is enabled and compatible with the
  // active WAL strategy. WAL recycling already avoids file creation latency, so
  // precreation is disabled when recycle_log_file_num is non-zero.
  bool AsyncWALPrecreateEnabled() const;

  // A WAL file that has a reserved file number and may have an opened writer,
  // but has not been added to DBImpl's in-memory logical WAL tracking lists
  // (logs_ and alive_wal_files_).
  struct UnpublishedWAL {
    uint64_t log_number = 0;
    std::unique_ptr<log::Writer> writer;

    UnpublishedWAL() = default;
    UnpublishedWAL(const UnpublishedWAL&) = delete;
    UnpublishedWAL& operator=(const UnpublishedWAL&) = delete;

    UnpublishedWAL(UnpublishedWAL&& other) noexcept {
      *this = std::move(other);
    }
    UnpublishedWAL& operator=(UnpublishedWAL&& other) noexcept {
      if (this != &other) {
        log_number = other.log_number;
        writer = std::move(other.writer);
        other.Reset();
      }
      return *this;
    }

    ~UnpublishedWAL() = default;

    void Reset() {
      log_number = 0;
      writer.reset();
    }
  };

  // Reserves the next WAL file number and schedules a HIGH-priority background
  // task to precreate that WAL file. A precreated WAL is not a logical WAL
  // until a foreground WAL rotation consumes it.
  void MaybeScheduleAsyncWALPrecreate(size_t preallocate_block_size);

  // Background task for opening the reserved future WAL and publishing the
  // result under mutex_.
  static void BGWorkAsyncWALPrecreate(void* arg);

  // Waits for an in-flight async WAL precreation and returns a prepared WAL if
  // one is available. If precreation failed, returns an empty WAL and lets the
  // foreground rotation create the WAL synchronously. Caller must hold mutex_.
  UnpublishedWAL WaitForAsyncWALPrecreate();

  // Opens and preallocates a WAL writer without writing logical WAL records.
  // Used by async WAL precreation and by synchronous WAL creation.
  IOStatus CreateWALWriter(const DBOptions& db_options, uint64_t log_file_num,
                           uint64_t recycle_log_number,
                           size_t preallocate_block_size,
                           UnpublishedWAL* new_wal);

  // Starts an opened WAL file by writing the initial records required before it
  // can be installed as the current WAL for foreground writes.
  IOStatus StartWALFile(const WriteOptions& write_options,
                        const PredecessorWALInfo& predecessor_wal_info,
                        log::Writer* new_log);
  IOStatus CreateWAL(const DBOptions& db_options,
                     const WriteOptions& write_options, uint64_t log_file_num,
                     uint64_t recycle_log_number, size_t preallocate_block_size,
                     const PredecessorWALInfo& predecessor_wal_info,
                     log::Writer** new_log);

  // Validate self-consistency of DB options
  static Status ValidateOptions(const DBOptions& db_options);
  // Validate self-consistency of DB options and its consistency with cf options
  static Status ValidateOptions(
      const DBOptions& db_options,
      const std::vector<ColumnFamilyDescriptor>& column_families);

  // Utility function to do some debug validation and sort the given vector
  // of MultiGet keys
  void PrepareMultiGetKeys(
      const size_t num_keys, bool sorted,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* key_ptrs);

  void MultiGetCommon(const ReadOptions& options,
                      ColumnFamilyHandle* column_family, const size_t num_keys,
                      const Slice* keys, PinnableSlice* values,
                      PinnableWideColumns* columns, std::string* timestamps,
                      Status* statuses, bool sorted_input);

  DECLARE_SYNC_AND_ASYNC(void, MultiGetCommon, const ReadOptions& options,
                         const size_t num_keys,
                         ColumnFamilyHandle** column_families,
                         const Slice* keys, PinnableSlice* values,
                         PinnableWideColumns* columns, std::string* timestamps,
                         Status* statuses,
                         std::vector<uint8_t>* newer_version_present,
                         bool sorted_input);

  // A structure to hold the information required to process MultiGet of keys
  // belonging to one column family. For a multi column family MultiGet, there
  // will be a container of these objects.
  struct MultiGetKeyRangePerCf {
    // For the batched MultiGet which relies on sorted keys, start specifies
    // the index of first key belonging to this column family in the sorted
    // list.
    size_t start;

    // For the batched MultiGet case, num_keys specifies the number of keys
    // belonging to this column family in the sorted list
    size_t num_keys;

    MultiGetKeyRangePerCf() : start(0), num_keys(0) {}

    MultiGetKeyRangePerCf(size_t first, size_t count)
        : start(first), num_keys(count) {}
  };

  // A structure to contain ColumnFamilyData and the SuperVersion obtained for
  // the consistent view of DB
  struct ColumnFamilySuperVersionPair {
    ColumnFamilyHandleImpl* cfh;
    ColumnFamilyData* cfd;

    // SuperVersion for the column family obtained in a manner that ensures a
    // consistent view across all column families in the DB
    SuperVersion* super_version;
    ColumnFamilySuperVersionPair(ColumnFamilyHandle* column_family,
                                 SuperVersion* sv)
        : cfh(static_cast<ColumnFamilyHandleImpl*>(column_family)),
          cfd(cfh->cfd()),
          super_version(sv) {}

    ColumnFamilySuperVersionPair() = default;
  };

  // A common function to obtain a consistent snapshot, which can be implicit
  // if the user doesn't specify a snapshot in read_options, across
  // multiple column families. It will attempt to get an implicit
  // snapshot without acquiring the db_mutes, but will give up after a few
  // tries and acquire the mutex if a memtable flush happens. The template
  // allows both the batched and non-batched MultiGet to call this with
  // either an std::unordered_map or autovector of column families.
  //
  // If callback is non-null, the callback is refreshed with the snapshot
  // sequence number
  //
  // `extra_sv_ref` is used to indicate whether thread-local SuperVersion
  // should be obtained with an extra ref (by GetReferencedSuperVersion()) or
  // not (by GetAndRefSuperVersion()). For instance, point lookup like MultiGet
  // does not require SuperVersion to be re-acquired throughout the entire
  // invocation (no need extra ref), while MultiCfIterators may need the
  // SuperVersion to be updated during Refresh() (requires extra ref).
  //
  // `sv_from_thread_local` being set to false indicates that the SuperVersion
  // obtained from the ColumnFamilyData, whereas true indicates they are thread
  // local.
  //
  // A non-OK status will be returned if for a column family that enables
  // user-defined timestamp feature, the specified `ReadOptions.timestamp`
  // attemps to read collapsed history.
  template <class T, typename IterDerefFuncType>
  Status MultiCFSnapshot(const ReadOptions& read_options,
                         ReadCallback* callback,
                         IterDerefFuncType iter_deref_func, T* cf_list,
                         bool extra_sv_ref, SequenceNumber* snapshot,
                         bool* sv_from_thread_local);

  // The actual implementation of the batching MultiGet. The caller is expected
  // to have acquired the SuperVersion and pass in a snapshot sequence number
  // in order to construct the LookupKeys. The start_key and num_keys specify
  // the range of keys in the sorted_keys vector for a single column family.
  DECLARE_SYNC_AND_ASYNC(
      Status, MultiGetImpl, const ReadOptions& read_options, size_t start_key,
      size_t num_keys,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys,
      SuperVersion* sv, SequenceNumber snap_seqnum, ReadCallback* callback);

  void MultiGetWithCallbackImpl(
      const ReadOptions& read_options, ColumnFamilyHandle* column_family,
      ReadCallback* callback,
      autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE>* sorted_keys);

  Status DisableFileDeletionsWithLock();

  Status IncreaseFullHistoryTsLowImpl(ColumnFamilyData* cfd,
                                      std::string ts_low);

  // Passes GetMergeOperands collection options to every lookup layer through
  // merge_context. The caller's output array is not touched here.
  static void PrepareGetMergeOperands(const GetImplOptions& get_impl_options,
                                      MergeContext* merge_context);

  // Publishes a successful GetMergeOperands lookup into the first
  // *number_of_operands output slots; later slots, and the whole array on an
  // Incomplete result, are left as the caller passed them. The output either
  // owns a copy of every operand or shares one cleanup bundle that keeps all
  // backing resources alive until the last PinnableSlice is reset. Returns
  // Incomplete when the result exceeds the caller's capacity and a plain OK
  // otherwise, so a caller must not let that OK replace a subcode it set.
  Status FinalizeGetMergeOperands(const GetImplOptions& get_impl_options,
                                  SuperVersion* super_version,
                                  MergeContext* merge_context,
                                  PinnedIteratorsManager* pinned_iters_manager,
                                  size_t* bytes_read);

  bool ShouldReferenceSuperVersion(const MergeContext& merge_context);
  // Resolves a plain value whose current payload is an encoded direct-write
  // blob index, either through `value` directly or through the default-column
  // view layered on `columns`.
  static Status ResolveDirectWritePlainValue(const Slice& key,
                                             const BlobFetcher& blob_fetcher,
                                             PinnableSlice* value,
                                             PinnableWideColumns* columns);
  // Resolves each unresolved direct-write blob-valued column in `columns` and
  // rebuilds the serialized wide-column entity in place.
  static Status ResolveDirectWriteWideColumns(const Slice& key,
                                              const BlobFetcher& blob_fetcher,
                                              PinnableWideColumns* columns);
  // Dispatches between plain-value and wide-column direct-write resolution for
  // read results observed before flush makes the corresponding blob file
  // visible through normal Version metadata.
  static bool MaybeResolveDirectWriteValue(
      const ReadOptions& read_options, const Slice& key,
      bool resolve_direct_write_value, const BlobFetcher& blob_fetcher,
      PinnableSlice* value, PinnableWideColumns* columns, Status* s,
      bool* is_blob_index, bool* value_found = nullptr);
  // Completes primary memtable hits that can still carry direct-write blob
  // references before the blob file is visible in Version metadata.
  static void PostprocessDirectWriteValueRead(
      const ReadOptions& read_options, const Slice& key,
      const std::string* timestamp, bool resolve_direct_write_value,
      const BlobFetcher* blob_fetcher, PinnableSlice* value,
      PinnableWideColumns* columns, Status* s, bool* is_blob_index,
      bool* value_found = nullptr);
  // Resolves memtable read results that still carry blob references through
  // either a raw blob-index payload in `value` or unresolved blob columns in
  // `columns`. This generic path is also used by read-only/secondary DBs. Sets
  // *did_resolve to true iff there was a reference to resolve -- in which case
  // this function has finalized `value`/`columns` (populated on success,
  // cleared on error) -- and false iff there was nothing to resolve (the caller
  // finalizes the plain value). Only call with an OK incoming status.
  static Status MaybeResolveMemtableBlobValue(
      const Slice& key, const BlobFetcher* blob_fetcher, PinnableSlice* value,
      PinnableWideColumns* columns, bool* did_resolve, bool* is_blob_index,
      bool* value_found = nullptr);
  // Completes read-only/secondary memtable Get()/GetEntity() hits. Given the
  // status from the memtable read, resolves blob-backed payloads when
  // `resolve_blob_backed_memtable_value` is true, self-pins a plain value on
  // success, and clears outputs on any error (never leaving a half-populated
  // result). When the caller explicitly requested raw blob indices via
  // `GetImplOptions::is_blob_index`, this helper leaves that payload untouched.
  // `memtable_blob_fetcher` may be null when blob support is disabled for the
  // column family. Returns the finalized status.
  static Status PostprocessMemtableValueRead(
      const Slice& key, const std::string* timestamp,
      bool resolve_blob_backed_memtable_value,
      const BlobFetcher* memtable_blob_fetcher, PinnableSlice* value,
      PinnableWideColumns* columns, Status memtable_read_status,
      bool* is_blob_index, bool* value_found = nullptr);

  template <typename IterType, typename ImplType,
            typename ErrorIteratorFuncType>
  std::unique_ptr<IterType> NewMultiCfIterator(
      const ReadOptions& _read_options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      ErrorIteratorFuncType error_iterator_func);

  bool ShouldPickCompaction(bool is_prepicked,
                            const PrepickedCompaction* prepicked_compaction);

  void ResetBottomPriCompactionIntent(ColumnFamilyData* cfd,
                                      std::unique_ptr<Compaction>& c);
  // Lock over the persistent DB state.  Non-nullptr iff successfully acquired.
  FileLock* db_lock_ = nullptr;

  // Guards changes to DB and CF options to ensure consistency between
  // * In-memory options objects
  // * Settings in effect
  // * Options file contents
  // while allowing the DB mutex to be released during slow operations like
  // persisting options file or modifying global periodic task timer.
  // Always acquired *before* DB mutex when this one is applicable.
  InstrumentedMutex options_mutex_;

  // Guards reads and writes to in-memory stats_history_.
  InstrumentedMutex stats_history_mutex_;

  // In addition to mutex_, wal_write_mutex_ protects writes to logs_ and
  // cur_wal_number_. With two_write_queues it also protects alive_wal_files_,
  // and wal_empty_. Refer to the definition of each variable below for more
  // details.
  // Note: to avoid deadlock, if needed to acquire both wal_write_mutex_ and
  // mutex_, the order should be first mutex_ and then wal_write_mutex_.
  InstrumentedMutex wal_write_mutex_;

  // If zero, manual compactions are allowed to proceed. If non-zero, manual
  // compactions may still be running, but will quickly fail with
  // `Status::Incomplete`. The value indicates how many threads have paused
  // manual compactions. It is accessed in read mode outside the DB mutex in
  // compaction code paths.
  std::atomic<int> manual_compaction_paused_ = false;

  // If non-zero, all compaction jobs (background automatic compactions,
  // manual compactions via CompactRange, and foreground CompactFiles calls)
  // are being aborted. Compactions will be signaled to stop. Any new
  // compaction job would fail immediately. The value indicates how many threads
  // have called AbortAllCompactions(). It is accessed in read mode outside the
  // DB mutex in compaction code paths.
  std::atomic<int> compaction_aborted_ = 0;

  // Number of AbortCompactions() callers waiting for a targeted compaction to
  // finish. Protected by the DB mutex.
  int per_cf_compaction_abort_waiters_ = 0;

  // This condition variable is signaled on these conditions:
  // * whenever bg_compaction_scheduled_ goes down to 0
  // * if AnyManualCompaction, whenever a compaction finishes, even if it hasn't
  // made any progress
  // * whenever a compaction made any progress
  // * whenever bg_flush_scheduled_ or bg_purge_scheduled_ value decreases
  // (i.e. whenever a flush is done, even if it didn't make any progress)
  // * whenever there is an error in background purge, flush or compaction
  // * whenever num_running_ingest_file_ goes to 0.
  // * whenever pending_purge_obsolete_files_ goes to 0.
  // * whenever disable_delete_obsolete_files_ goes to 0.
  // * whenever SetOptions successfully updates options.
  // * whenever a column family is dropped.
  InstrumentedCondVar bg_cv_;

  // Set only while ResumeImpl performs a file-scoped WAL recovery. Protected
  // by mutex_; SwitchMemtable uses it to avoid touching the failed writer.
  bool recovering_from_wal_write_error_ = false;
  uint64_t recover_wal_through_number_ = 0;

  // Once a file-scoped WAL error is reported, all writers through this
  // inclusive cutoff are quarantined until recovery detaches them. A
  // concurrent SyncWAL() reads this without taking the DB mutex.
  std::atomic<uint64_t> wal_recovery_cutoff_{0};

  ColumnFamilyHandleImpl* persist_stats_cf_handle_ = nullptr;

  bool persistent_stats_cfd_exists_ = true;

  // Writes are protected by locking both mutex_ and wal_write_mutex_, and reads
  // must be under either mutex_ or wal_write_mutex_. Since after ::Open,
  // cur_wal_number_ is currently updated only in write_thread_, it can be read
  // from the same write_thread_ without any locks.
  uint64_t cur_wal_number_ = 0;

  // Log files that we can recycle. Must be protected by db mutex_.
  std::deque<uint64_t> wal_recycle_files_;

  // The minimum log file number taht can be recycled, if log recycling is
  // enabled. This is used to ensure that log files created by previous
  // instances of the database are not recycled, as we cannot be sure they
  // were created in the recyclable format.
  uint64_t min_wal_number_to_recycle_ = 0;

  // Protected by wal_write_mutex_.
  bool wal_dir_synced_ = false;

  // Without two_write_queues, read and writes to wal_empty_ are protected by
  // mutex_. Since it is currently updated/read only in write_thread_, it can be
  // accessed from the same write_thread_ without any locks. With
  // two_write_queues writes, where it can be updated in different threads,
  // read and writes are protected by wal_write_mutex_ instead. This is to avoid
  // expensive mutex_ lock during WAL write, which update wal_empty_.
  bool wal_empty_ = true;

  // The current WAL file and those that have not been found obsolete from
  // memtable flushes. A WAL not on this list might still be pending writer
  // flush and/or sync and close and might still be in logs_. alive_wal_files_
  // is protected by mutex_ and wal_write_mutex_ with details as follows:
  // 1. read by FindObsoleteFiles() which can be called in either application
  //    thread or RocksDB bg threads, both mutex_ and wal_write_mutex_ are
  //    held.
  // 2. pop_front() by FindObsoleteFiles(), both mutex_ and wal_write_mutex_
  //    are held.
  // 3. push_back() by DBImpl::Open() and DBImpl::RestoreAliveLogFiles()
  //    (actually called by Open()), only mutex_ is held because at this point,
  //    the DB::Open() call has not returned success to application, and the
  //    only other thread(s) that can conflict are bg threads calling
  //    FindObsoleteFiles() which ensure that both mutex_ and wal_write_mutex_
  //    are held when accessing alive_wal_files_.
  // 4. read by DBImpl::Open() is protected by mutex_.
  // 5. push_back() by SwitchMemtable(). Both mutex_ and wal_write_mutex_ are
  //    held. This is done by the write group leader. Note that in the case of
  //    two-write-queues, another WAL-only write thread can be writing to the
  //    WAL concurrently. See 9.
  // 6. read by SwitchWAL() with both mutex_ and wal_write_mutex_ held. This is
  //    done by write group leader.
  // 7. read by ConcurrentWriteToWAL() by the write group leader in the case of
  //    two-write-queues. Only wal_write_mutex_ is held to protect concurrent
  //    pop_front() by FindObsoleteFiles().
  // 8. read by PreprocessWrite() by the write group leader. wal_write_mutex_
  //    is held to protect the data structure from concurrent pop_front() by
  //    FindObsoleteFiles().
  // 9. read by ConcurrentWriteToWAL() by a WAL-only write thread in the case
  //    of two-write-queues. Only wal_write_mutex_ is held. This suffices to
  //    protect the data structure from concurrent push_back() by current
  //    write group leader as well as pop_front() by FindObsoleteFiles().
  std::deque<WalFileNumberSize> alive_wal_files_;

  // Total size of all "alive" WALs (for easy access without synchronization)
  RelaxedAtomic<uint64_t> wals_total_size_{0};

  // Log files that aren't fully synced, and the current log file.
  // Synchronization:
  // 1. read by FindObsoleteFiles() which can be called either in application
  //    thread or RocksDB bg threads. wal_write_mutex_ is always held, while
  //    some reads are performed without mutex_.
  // 2. pop_front() by FindObsoleteFiles() with only wal_write_mutex_ held.
  // 3. read by DBImpl::Open() with both mutex_ and wal_write_mutex_.
  // 4. emplace_back() by DBImpl::Open() with both mutex_ and wal_write_mutex.
  //    Note that at this point, DB::Open() has not returned success to
  //    application, thus the only other thread(s) that can conflict are bg
  //    threads calling FindObsoleteFiles(). See 1.
  // 5. iteration and clear() from CloseHelper() always hold wal_write_mutex
  //    and mutex_.
  // 6. back() called by APIs FlushWAL() and LockWAL() are protected by only
  //    wal_write_mutex_. These two can be called by application threads after
  //    DB::Open() returns success to applications.
  // 7. read by SyncWAL(), another API, protected by only wal_write_mutex_.
  // 8. read by MarkLogsNotSynced() and MarkLogsSynced() are protected by
  //    wal_write_mutex_.
  // 9. erase() by MarkLogsSynced() protected by wal_write_mutex_.
  // 10. read by SyncClosedWals() protected by only wal_write_mutex_. This can
  //     happen in bg flush threads after DB::Open() returns success to
  //     applications.
  // 11. reads, e.g. front(), iteration, and back() called by PreprocessWrite()
  //     holds only the wal_write_mutex_. This is done by the write group
  //     leader. A bg thread calling FindObsoleteFiles() or MarkLogsSynced()
  //     can happen concurrently. This is fine because wal_write_mutex_ is used
  //     by all parties. See 2, 5, 9.
  // 12. reads, empty(), back() called by SwitchMemtable() hold both mutex_ and
  //     wal_write_mutex_. This happens in the write group leader.
  // 13. emplace_back() by SwitchMemtable() hold both mutex_ and
  //     wal_write_mutex_. This happens in the write group leader. Can conflict
  //     with bg threads calling FindObsoleteFiles(), MarkLogsSynced(),
  //     SyncClosedWals(), etc. as well as application threads calling
  //     FlushWAL(), SyncWAL(), LockWAL(). This is fine because all parties
  //     require at least wal_write_mutex_.
  // 14. iteration called in WriteToWAL(write_group) protected by
  //     wal_write_mutex_. This is done by write group leader when
  //     two-write-queues is disabled and write needs to sync logs.
  // 15. back() called in ConcurrentWriteToWAL() protected by wal_write_mutex_.
  //     This can be done by the write group leader if two-write-queues is
  //     enabled. It can also be done by another WAL-only write thread.
  //
  // Other observations:
  //  - back() and items with getting_synced=true are not popped,
  //  - The same thread that sets getting_synced=true will reset it.
  //  - it follows that the object referred by back() can be safely read from
  //  the write_thread_ without using mutex. Note that calling back() without
  //  mutex may be unsafe because different implementations of deque::back() may
  //  access other member variables of deque, causing undefined behaviors.
  //  Generally, do not access stl containers without proper synchronization.
  //  - it follows that the items with getting_synced=true can be safely read
  //  from the same thread that has set getting_synced=true
  std::deque<LogWriterNumber> logs_;

  // Signaled when getting_synced becomes false for some of the logs_.
  InstrumentedCondVar wal_sync_cv_;
  // This is the app-level state that is written to the WAL but will be used
  // only during recovery. Using this feature enables not writing the state to
  // memtable on normal writes and hence improving the throughput. Each new
  // write of the state will replace the previous state entirely even if the
  // keys in the two consecutive states do not overlap.
  // It is protected by wal_write_mutex_ when two_write_queues_ is enabled.
  // Otherwise only the heaad of write_thread_ can access it.
  WriteBatch cached_recoverable_state_;
  std::atomic<bool> cached_recoverable_state_empty_ = {true};

  // If this is non-empty, we need to delete these log files in background
  // threads. Protected by wal_write_mutex_.
  autovector<log::Writer*> wals_to_free_;

  // Written with mutex_ held. Read without mutex_ when creating a snapshot.
  std::atomic<bool> is_snapshot_supported_{true};

  std::map<uint64_t, std::map<std::string, uint64_t>> stats_history_;

  std::map<std::string, uint64_t> stats_slice_;

  bool stats_slice_initialized_ = false;

  Directories directories_;

  WriteBufferManager* write_buffer_manager_;

  WriteThread write_thread_;
  WriteBatch tmp_batch_;
  // The write thread when the writers have no memtable write. This will be used
  // in 2PC to batch the prepares separately from the serial commit.
  WriteThread nonmem_write_thread_;

  WriteController write_controller_;

  // Size of the last batch group. In slowdown mode, next write needs to
  // sleep if it uses up the quota.
  // Note: This is to protect memtable and compaction. If the batch only writes
  // to the WAL its size need not to be included in this.
  uint64_t last_batch_group_size_ = 0;

  FlushScheduler flush_scheduler_;

  TrimHistoryScheduler trim_history_scheduler_;

  // Shard locks inside snapshots_ are leaf locks: they may be acquired while
  // holding mutex_, but mutex_ must never be acquired while holding one.
  // GetSnapshotImpl() reads a snapshot's sequence number inside the shard lock
  // that adds it. Flush and compaction combine that ordering with their
  // DB-mutex-protected input boundary and, when required, a managed snapshot or
  // published-sequence pin in InitSnapshotContext().
  ShardedSnapshotList snapshots_;

  TimestampedSnapshotList timestamped_snapshots_;

  // For each background job, pending_outputs_ keeps the current file number at
  // the time that background job started.
  // FindObsoleteFiles()/PurgeObsoleteFiles() never deletes any file that has
  // number bigger than any of the file number in pending_outputs_. Since file
  // numbers grow monotonically, this also means that pending_outputs_ is always
  // sorted. After a background job is done executing, its file number is
  // deleted from pending_outputs_, which allows PurgeObsoleteFiles() to clean
  // it up.
  // State is protected with db mutex.
  std::list<uint64_t> pending_outputs_;

  // First file number -> reserved file count for external file ingestions.
  // Full-scan purge snapshots protect these SSTs until commit or cleanup.
  // State is protected with db mutex.
  UnorderedMap<uint64_t, uint64_t> pending_ingestion_outputs_;

  // Similar to pending_outputs_, FindObsoleteFiles()/PurgeObsoleteFiles() never
  // deletes any OPTIONS file that has number bigger than any of the file number
  // in min_options_file_numbers_.
  std::list<uint64_t> min_options_file_numbers_;

  // flush_queue_ and compaction_queue_ hold column families that we need to
  // flush and compact, respectively.
  // A column family is inserted into flush_queue_ when it satisfies condition
  // cfd->imm()->IsFlushPending()
  // A column family is inserted into compaction_queue_ when it satisfied
  // condition cfd->NeedsCompaction()
  // Column families in these collections are all Ref()-erenced.
  // TODO(icanadi) Provide some kind of ReferencedColumnFamily class that will
  // do RAII on ColumnFamilyData
  // Column families are in this queue when they need to be flushed or
  // compacted. Consumers of these queues are flush and compaction threads. When
  // column family is put on this queue, we increase unscheduled_flushes_ and
  // unscheduled_compactions_. When these variables are bigger than zero, that
  // means we need to schedule background threads for flush and compaction.
  // Once the background threads are scheduled, we decrease unscheduled_flushes_
  // and unscheduled_compactions_. That way we keep track of number of
  // compaction and flush threads we need to schedule. This scheduling is done
  // in MaybeScheduleFlushOrCompaction()
  // invariant(column family present in flush_queue_ <==>
  // ColumnFamilyData::pending_flush_ == true)
  std::deque<FlushRequest> flush_queue_;
  // invariant((column family in compaction_queue_ or
  //            column family in parked_compaction_cfds_)
  //           <=> queued_for_compaction() == true)
  //
  // "Parked" means a column family whose compaction was aborted via
  // AbortCompactions() while it was queued. When an aborted CF reaches the
  // front of compaction_queue_, PickCompactionFromQueue() moves it to
  // parked_compaction_cfds_ instead of scheduling it. The CF does not
  // immediately lose its place -- only when it reaches the front of the
  // queue is it parked. This preserves fairness for CFs behind it and
  // avoids restoring the scheduler credit (which would cause a hot re-pick
  // loop). On the final ResumeCompactions(), RestoreParkedCompaction()
  // re-enqueues the CF (or releases its ref if it no longer needs
  // compaction). Each parked CF carries exactly one Ref() from the
  // original AddToCompactionQueue() call.
  std::deque<ColumnFamilyData*> compaction_queue_;
  std::unordered_set<ColumnFamilyData*> parked_compaction_cfds_;

  // A map to store file numbers and filenames of the files to be purged
  std::unordered_map<uint64_t, PurgeFileInfo> purge_files_;

  // A vector to store the file numbers that have been assigned to certain
  // JobContext. Current implementation tracks table and blob files only.
  std::unordered_set<uint64_t> files_grabbed_for_purge_;

  // A queue to store log writers to close. Protected by db mutex_.
  std::deque<log::Writer*> wals_to_free_queue_;

  std::deque<SuperVersion*> superversions_to_free_queue_;

  int unscheduled_flushes_ = 0;

  int unscheduled_compactions_ = 0;

  // count how many background compactions are running or have been scheduled in
  // the BOTTOM pool
  int bg_bottom_compaction_scheduled_ = 0;

  // count how many background compactions are running or have been scheduled
  int bg_compaction_scheduled_ = 0;

  std::shared_ptr<RemoteCompactionThreadPool> remote_compaction_pool_;
  int remote_compaction_pool_jobs_ = 0;
  int remote_coordinators_scheduled_ = 0;
  std::array<int, 2> local_compactions_running_by_priority_{};

  // stores the number of compactions are currently running
  int num_running_compactions_ = 0;

  // stores the number of BOTTOM-priority compactions currently running
  int num_running_bottom_compactions_ = 0;

  // Number of compaction service jobs currently waiting in
  // CompactionService::Wait(), counted per executed subcompaction.
  std::atomic<int> num_running_remote_compactions_ = 0;

  // number of background memtable flush jobs, submitted to the HIGH pool
  int bg_flush_scheduled_ = 0;

  // LOW avoids consuming a flush worker while joining the target write thread.
  static constexpr Env::Priority kWBMFlushPriority = Env::Priority::LOW;

  // Outstanding cross-DB WBM flush jobs, drained during shutdown.
  int bg_wbm_flush_scheduled_ = 0;

  // stores the number of flushes are currently running
  int num_running_flushes_ = 0;

  // number of background obsolete file purge jobs, submitted to the HIGH pool
  int bg_purge_scheduled_ = 0;

  // number of pressure callbacks currently in progress (for destructor safety)
  int bg_pressure_callback_in_progress_ = 0;

  enum class AsyncFileOpenState : uint8_t {
    kNotScheduled = 0,  // Async file opening has not been scheduled.
    kScheduled,         // Async file opening is in-flight in the HIGH pool.
    kComplete,          // Async file opening has finished (or was not needed).
  };

  // Tracks whether background async file opening has been scheduled/completed.
  AsyncFileOpenState bg_async_file_open_state_ =
      AsyncFileOpenState::kNotScheduled;

  // State machine for the single async WAL precreation slot protected by
  // mutex_. Background precreation failure returns to kNotScheduled; foreground
  // rotation handles it the same as no prepared WAL and creates one
  // synchronously. kScheduled owns a reserved file number; kReady owns an
  // opened writer that has not been started or added to logical WAL tracking.
  enum class AsyncWALPrecreateState : uint8_t {
    kNotScheduled = 0,  // No WAL precreate work is in-flight or ready.
    kScheduled,         // Background task owns creation of the reserved WAL.
    kReady,             // Reserved WAL writer is open but not logically live.
  };

  // Protected by mutex_. Tracks at most one background precreated WAL. A
  // precreated WAL is only reserved empty storage until SwitchMemtable()
  // consumes it and installs it in DBImpl's in-memory logical WAL tracking
  // lists (logs_ and alive_wal_files_).
  AsyncWALPrecreateState async_wal_precreate_state_ =
      AsyncWALPrecreateState::kNotScheduled;

  // Reserved in-flight/ready precreated WAL. The writer is populated only while
  // state is kReady.
  UnpublishedWAL async_wal_precreate_wal_;

  std::deque<ManualCompactionState*> manual_compaction_dequeue_;

  // shall we disable deletion of obsolete files
  // if 0 the deletion is enabled.
  // if non-zero, files will not be getting deleted
  // This enables two different threads to call
  // EnableFileDeletions() and DisableFileDeletions()
  // without any synchronization
  int disable_delete_obsolete_files_ = 0;

  // Number of times FindObsoleteFiles has found deletable files and the
  // corresponding call to PurgeObsoleteFiles has not yet finished.
  int pending_purge_obsolete_files_ = 0;

  // last time when DeleteObsoleteFiles with full scan was executed. Originally
  // initialized with startup time.
  uint64_t delete_obsolete_files_last_run_;

  // The thread that wants to switch memtable, can wait on this cv until the
  // pending writes to memtable finishes.
  std::condition_variable switch_cv_;
  // The mutex used by switch_cv_. mutex_ should be acquired beforehand.
  std::mutex switch_mutex_;
  // Number of threads intending to write to memtable
  std::atomic<size_t> pending_memtable_writes_{0};

  // A flag indicating whether the current rocksdb database has any
  // data that is not yet persisted into either WAL or SST file.
  // Used when disableWAL is true.
  std::atomic<bool> has_unpersisted_data_{false};

  // Number of live column families with an active blob direct write partition
  // manager. This keeps the read/write fast paths at O(1) when the feature is
  // completely disabled for the DB.
  std::atomic<uint32_t> blob_direct_write_cf_count_{0};

  // if an attempt was made to flush all column families that
  // the oldest log depends on but uncommitted data in the oldest
  // log prevents the log from being released.
  // We must attempt to free the dependent memtables again
  // at a later time after the transaction in the oldest
  // log is fully commited.
  bool unable_to_release_oldest_log_{false};

  // Number of running IngestExternalFile() or CreateColumnFamilyWithImport()
  // calls.
  // REQUIRES: mutex held
  int num_running_ingest_file_ = 0;

  // Number of FileIngestionHandle objects produced by PrepareFileIngestion()
  // that have not been committed or destroyed yet.
  std::atomic<uint32_t> num_outstanding_prepared_ingestions_{0};

  WalManager wal_manager_;

  // A value of > 0 temporarily disables scheduling of background work
  int bg_work_paused_ = 0;

  // A value of > 0 temporarily disables scheduling of background compaction
  int bg_compaction_paused_ = 0;

  // Guard against multiple concurrent refitting
  bool refitting_level_ = false;

  // The minimum threshold for triggering bottommost compaction to remove
  // garbage across all column families. Protected by mutex_.
  SequenceNumber bottommost_files_mark_threshold_ = kMaxSequenceNumber;

  // The minimum threshold for triggering compactions of standalone range
  // deletion files across all column families. Protected by mutex_.
  SequenceNumber standalone_range_deletion_files_mark_threshold_ =
      kMaxSequenceNumber;

  // Largest exact oldest-snapshot sequence processed. Protected by mutex_.
  SequenceNumber last_processed_oldest_snapshot_ = 0;

  // Sum of the per-shard snapshot release counts at the last exact refresh.
  // Protected by mutex_.
  uint64_t last_processed_snapshot_release_count_ = 0;

  LogsWithPrepTracker logs_with_prep_tracker_;

  // Callback for compaction to check if a key is visible to a snapshot.
  // REQUIRES: mutex held
  std::unique_ptr<SnapshotChecker> snapshot_checker_;
  // When set, InitSnapshotContext() appends GetLastPublishedSequence() to the
  // job's snapshot_seqs (if not already present) so that flush/compaction
  // preserves the published-sequence boundary even when no explicit user
  // snapshot exists there. Unlike taking a real ManagedSnapshot, this just
  // pins the seqno into the snapshot list: no allocation, no snapshot list
  // mutation, and no SnapshotChecker.
  bool track_published_seq_in_snapshot_context_ = false;

  // Callback for when the cached_recoverable_state_ is written to memtable
  // Only to be set during initialization
  std::unique_ptr<PreReleaseCallback> recoverable_state_pre_release_callback_;

  // Scheduler to run DumpStats(), PersistStats(), and FlushInfoLog().
  // Currently, internally it has a global timer instance for running the tasks.
  PeriodicTaskScheduler periodic_task_scheduler_;

  // It contains the implementations for each periodic task.
  std::map<PeriodicTaskType, const PeriodicTaskFunc> periodic_task_functions_;

  // When set, we use a separate queue for writes that don't write to memtable.
  // In 2PC these are the writes at Prepare phase.
  const bool two_write_queues_;
  const bool manual_wal_flush_;

  // LastSequence also indicates last published sequence visibile to the
  // readers. Otherwise LastPublishedSequence should be used.
  const bool last_seq_same_as_publish_seq_;
  // It indicates that a customized gc algorithm must be used for
  // flush/compaction and if it is not provided vis SnapshotChecker, we should
  // disable gc to be safe.
  const bool use_custom_gc_;
  // Flag to indicate that the DB instance shutdown has been initiated. This
  // different from shutting_down_ atomic in that it is set at the beginning
  // of shutdown sequence, specifically in order to prevent any background
  // error recovery from going on in parallel. The latter, shutting_down_,
  // is set a little later during the shutdown after scheduling memtable
  // flushes
  std::atomic<bool> shutdown_initiated_{false};
  // Flag to indicate whether sst_file_manager object was allocated in
  // DB::Open() or passed to us
  bool own_sfm_;

  // Flag to check whether Close() has been called on this DB
  bool closed_ = false;
  // save the closing status, for re-calling the close()
  Status closing_status_;
  // mutex for DB::Close()
  InstrumentedMutex closing_mutex_;

  // Conditional variable to coordinate installation of atomic flush results.
  // With atomic flush, each bg thread installs the result of flushing multiple
  // column families, and different threads can flush different column
  // families. It's difficult to rely on one thread to perform batch
  // installation for all threads. This is different from the non-atomic flush
  // case.
  // atomic_flush_install_cv_ makes sure that threads install atomic flush
  // results sequentially. Flush results of memtables with lower IDs get
  // installed to MANIFEST first.
  InstrumentedCondVar atomic_flush_install_cv_;

  bool wal_in_db_path_ = false;
  std::atomic<uint64_t> max_total_wal_size_;

  BlobFileCompletionCallback blob_callback_;

  // Pointer to WriteBufferManager stalling interface.
  std::unique_ptr<StallInterface> wbm_stall_;

  // seqno_to_time_mapping_ stores the sequence number to time mapping, it's not
  // thread safe, both read and write need db mutex hold.
  SeqnoToTimeMapping seqno_to_time_mapping_;

  // Stop write token that is acquired when first LockWAL() is called.
  // Destroyed when last UnlockWAL() is called. Controlled by DB mutex.
  // See lock_wal_owner_thread_id_counts_
  std::unique_ptr<WriteControllerToken> lock_wal_write_token_;

  // Thread IDs of valid LockWAL() owners and their recursive lock counts. Each
  // owning thread must call UnlockWAL() the same number of times before writes
  // can resume.
  std::unordered_map<uint64_t, uint32_t> lock_wal_owner_thread_id_counts_;
};

class GetWithTimestampReadCallback : public ReadCallback {
 public:
  explicit GetWithTimestampReadCallback(SequenceNumber seq)
      : ReadCallback(seq) {}
  // A null result is used by MultiGet, whose per-key results live in
  // KeyContext/GetContext.
  void EnableNewerVersionTracking(SequenceNumber read_snapshot_seq,
                                  SequenceNumber upper_bound_seq,
                                  bool* single_key_result) {
    metadata_read_bounds_.emplace(
        MetadataReadBounds{read_snapshot_seq, upper_bound_seq});
    newer_version_present_ = single_key_result;
  }
  const MetadataReadBounds* GetMetadataReadBounds() const override {
    return metadata_read_bounds_.has_value() ? &*metadata_read_bounds_
                                             : nullptr;
  }
  bool NeedToTrackNewerVersions(
      const bool* per_key_result = nullptr) const override {
    const bool* result =
        per_key_result != nullptr ? per_key_result : newer_version_present_;
    return metadata_read_bounds_.has_value() && result != nullptr && !*result;
  }
  bool IsVisibleFullCheck(SequenceNumber seq) override {
    return seq <= max_visible_seq_;
  }
  bool IsNewerVisibleForMetadataRead(SequenceNumber seq) override {
    const MetadataReadBounds* bounds = GetMetadataReadBounds();
    return bounds != nullptr && bounds->read_snapshot_seq < seq &&
           seq <= bounds->newer_version_upper_bound_seq;
  }
  void MaybeRecordNewerVersion(SequenceNumber seq, ValueType type,
                               bool* per_key_result = nullptr) override {
    bool* result =
        per_key_result != nullptr ? per_key_result : newer_version_present_;
    if (result != nullptr && !*result &&
        (IsValueType(type) || type == kTypeRangeDeletion) &&
        IsNewerVisibleForMetadataRead(seq)) {
      *result = true;
    }
  }

 private:
  std::optional<MetadataReadBounds> metadata_read_bounds_;
  bool* newer_version_present_ = nullptr;
};

Options SanitizeOptions(const std::string& db, const Options& src,
                        bool read_only = false,
                        Status* logger_creation_s = nullptr);

DBOptions SanitizeOptions(const std::string& db, const DBOptions& src,
                          bool read_only = false,
                          Status* logger_creation_s = nullptr);

CompressionType GetCompressionFlush(const ImmutableCFOptions& ioptions,
                                    const MutableCFOptions& mutable_cf_options);

// Return a VersionEdit for the DB's recovery when the `memtables` of the
// specified column family are obsolete. Specifically, the min log number to
// keep, and the WAL files that can be deleted.
VersionEdit GetDBRecoveryEditForObsoletingMemTables(
    VersionSet* vset, const ColumnFamilyData& cfd,
    const autovector<VersionEdit*>& edit_list,
    const autovector<ReadOnlyMemTable*>& memtables,
    LogsWithPrepTracker* prep_tracker);

// Return the earliest log file to keep after the memtable flush is
// finalized.
// `cfd_to_flush` is the column family whose memtable (specified in
// `memtables_to_flush`) will be flushed and thus will not depend on any WAL
// file.
// The function is only applicable to 2pc mode.
uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list,
    const autovector<ReadOnlyMemTable*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker);
// For atomic flush.
uint64_t PrecomputeMinLogNumberToKeep2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists,
    const autovector<const autovector<ReadOnlyMemTable*>*>& memtables_to_flush,
    LogsWithPrepTracker* prep_tracker);

// In non-2PC mode, WALs with log number < the returned number can be
// deleted after the cfd_to_flush column family is flushed successfully.
uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const ColumnFamilyData& cfd_to_flush,
    const autovector<VersionEdit*>& edit_list);
// For atomic flush.
uint64_t PrecomputeMinLogNumberToKeepNon2PC(
    VersionSet* vset, const autovector<ColumnFamilyData*>& cfds_to_flush,
    const autovector<autovector<VersionEdit*>>& edit_lists);

// `cfd_to_flush` is the column family whose memtable will be flushed and thus
// will not depend on any WAL file. nullptr means no memtable is being flushed.
// The function is only applicable to 2pc mode.
uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset, const autovector<ReadOnlyMemTable*>& memtables_to_flush);
// For atomic flush.
uint64_t FindMinPrepLogReferencedByMemTable(
    VersionSet* vset,
    const autovector<const autovector<ReadOnlyMemTable*>*>& memtables_to_flush);

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) {
    *ptr = maxvalue;
  }
  if (static_cast<V>(*ptr) < minvalue) {
    *ptr = minvalue;
  }
}

inline Status DBImpl::FailIfCfHasTs(
    const ColumnFamilyHandle* column_family) const {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be null");
  }
  assert(column_family);
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  if (ucmp->timestamp_size() > 0) {
    std::ostringstream oss;
    oss << "cannot call this method on column family "
        << column_family->GetName() << " that enables timestamp";
    return Status::InvalidArgument(oss.str());
  }
  return Status::OK();
}

inline Status DBImpl::FailIfTsMismatchCf(ColumnFamilyHandle* column_family,
                                         const Slice& ts) const {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be null");
  }
  assert(column_family);
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  if (0 == ucmp->timestamp_size()) {
    std::stringstream oss;
    oss << "cannot call this method on column family "
        << column_family->GetName() << " that does not enable timestamp";
    return Status::InvalidArgument(oss.str());
  }
  const size_t ts_sz = ts.size();
  if (ts_sz != ucmp->timestamp_size()) {
    std::stringstream oss;
    oss << "Timestamp sizes mismatch: expect " << ucmp->timestamp_size() << ", "
        << ts_sz << " given";
    return Status::InvalidArgument(oss.str());
  }
  return Status::OK();
}

inline Status DBImpl::FailIfTableFilterWithRangeConversion(
    const ReadOptions& read_options,
    const MutableCFOptions& mutable_cf_options) const {
  if (HasTableFilter(read_options) &&
      mutable_cf_options.min_tombstones_for_range_conversion > 0) {
    return Status::InvalidArgument(
        "ReadOptions::table_filter is not supported when "
        "min_tombstones_for_range_conversion > 0");
  }
  return Status::OK();
}

inline Status DBImpl::FailIfReadCollapsedHistory(const ColumnFamilyData* cfd,
                                                 const SuperVersion* sv,
                                                 const Slice& ts) const {
  // Reaching to this point means the timestamp size matching sanity check in
  // `DBImpl::FailIfTsMismatchCf` already passed. So we skip that and assume
  // column family has the same user-defined timestamp format as `ts`.
  const Comparator* const ucmp = cfd->user_comparator();
  assert(ucmp);
  const std::string& full_history_ts_low = sv->full_history_ts_low;
  assert(full_history_ts_low.empty() ||
         full_history_ts_low.size() == ts.size());
  if (!full_history_ts_low.empty() &&
      ucmp->CompareTimestamp(ts, full_history_ts_low) < 0) {
    std::stringstream oss;
    oss << "Read timestamp: " << ucmp->TimestampToString(ts)
        << " is smaller than full_history_ts_low: "
        << ucmp->TimestampToString(full_history_ts_low) << std::endl;
    return Status::InvalidArgument(oss.str());
  }
  return Status::OK();
}
}  // namespace ROCKSDB_NAMESPACE
