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
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/memtable_list.h"
#include "db/snapshot_checker.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/write_batch_internal.h"
#include "db/write_controller.h"
#include "options/cf_options.h"
#include "rocksdb/compaction_job_stats.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/cast_util.h"
#include "util/hash_containers.h"
#include "util/mutexlock.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

class Version;
class VersionSet;
class VersionStorageInfo;
class MemTable;
class MemTableListVersion;
class CompactionPicker;
class Compaction;
class InternalKey;
class InternalStats;
class ColumnFamilyData;
class DBImpl;
class LogBuffer;
class InstrumentedMutex;
class InstrumentedMutexLock;
struct SuperVersionContext;
class BlobFileCache;
class BlobFilePartitionManager;
class BlobSource;
class FlushInitiator;

extern const double kIncSlowdownRatio;
// This file contains a list of data structures for managing column family
// level metadata.
//
// The basic relationships among classes declared here are illustrated as
// following:
//
//       +----------------------+    +----------------------+   +--------+
//   +---+ ColumnFamilyHandle 1 | +--+ ColumnFamilyHandle 2 |   | DBImpl |
//   |   +----------------------+ |  +----------------------+   +----+---+
//   | +--------------------------+                                  |
//   | |                               +-----------------------------+
//   | |                               |
//   | | +-----------------------------v-------------------------------+
//   | | |                                                             |
//   | | |                      ColumnFamilySet                        |
//   | | |                                                             |
//   | | +-------------+--------------------------+----------------+---+
//   | |               |                          |                |
//   | +-------------------------------------+    |                |
//   |                 |                     |    |                v
//   |   +-------------v-------------+ +-----v----v---------+
//   |   |                           | |                    |
//   |   |     ColumnFamilyData 1    | | ColumnFamilyData 2 |    ......
//   |   |                           | |                    |
//   +--->                           | |                    |
//       |                 +---------+ |                    |
//       |                 | MemTable| |                    |
//       |                 |  List   | |                    |
//       +--------+---+--+-+----+----+ +--------------------++
//                |   |  |      |
//                |   |  |      |
//                |   |  |      +-----------------------+
//                |   |  +-----------+                  |
//                v   +--------+     |                  |
//       +--------+--------+   |     |                  |
//       |                 |   |     |       +----------v----------+
// +---> |SuperVersion 1.a +----------------->                     |
//       |                 +------+  |       | MemTableListVersion |
//       +---+-------------+   |  |  |       |                     |
//           |                 |  |  |       +----+------------+---+
//           |      current    |  |  |            |            |
//           |   +-------------+  |  |mem         |            |
//           |   |                |  |            |            |
//         +-v---v-------+    +---v--v---+  +-----v----+  +----v-----+
//         |             |    |          |  |          |  |          |
//         | Version 1.a |    | memtable |  | memtable |  | memtable |
//         |             |    |   1.a    |  |   1.b    |  |   1.c    |
//         +-------------+    |          |  |          |  |          |
//                            +----------+  +----------+  +----------+
//
// DBImpl keeps a ColumnFamilySet, which references to all column families by
// pointing to respective ColumnFamilyData object of each column family.
// This is how DBImpl can list and operate on all the column families.
// ColumnFamilyHandle also points to ColumnFamilyData directly, so that
// when a user executes a query, it can directly find memtables and Version
// as well as SuperVersion to the column family, without going through
// ColumnFamilySet.
//
// ColumnFamilySet points to the latest view of the LSM-tree (list of memtables
// and SST files) indirectly, while ongoing operations may hold references
// to a current or an out-of-date SuperVersion, which in turn points to a
// point-in-time view of the LSM-tree. This guarantees the memtables and SST
// files being operated on will not go away, until the SuperVersion is
// unreferenced to 0 and destoryed.
//
// The following graph illustrates a possible referencing relationships:
//
// Column       +--------------+      current       +-----------+
// Family +---->+              +------------------->+           |
//  Data        | SuperVersion +----------+         | Version A |
//              |      3       |   imm    |         |           |
// Iter2 +----->+              |  +-------v------+  +-----------+
//              +-----+--------+  | MemtableList +----------------> Empty
//                    |           |   Version r  |  +-----------+
//                    |           +--------------+  |           |
//                    +------------------+   current| Version B |
//              +--------------+         |   +----->+           |
//              |              |         |   |      +-----+-----+
// Compaction +>+ SuperVersion +-------------+            ^
//    Job       |      2       +------+  |                |current
//              |              +----+ |  |     mem        |    +------------+
//              +--------------+    | |  +--------------------->            |
//                                  | +------------------------> MemTable a |
//                                  |          mem        |    |            |
//              +--------------+    |                     |    +------------+
//              |              +--------------------------+
//  Iter1 +-----> SuperVersion |    |                          +------------+
//              |      1       +------------------------------>+            |
//              |              +-+  |        mem               | MemTable b |
//              +--------------+ |  |                          |            |
//                               |  |    +--------------+      +-----^------+
//                               |  |imm | MemtableList |            |
//                               |  +--->+   Version s  +------------+
//                               |       +--------------+
//                               |       +--------------+
//                               |       | MemtableList |
//                               +------>+   Version t  +-------->  Empty
//                                 imm   +--------------+
//
// In this example, even if the current LSM-tree consists of Version A and
// memtable a, which is also referenced by SuperVersion, two older SuperVersion
// SuperVersion2 and Superversion1 still exist, and are referenced by a
// compaction job and an old iterator Iter1, respectively. SuperVersion2
// contains Version B, memtable a and memtable b; SuperVersion1 contains
// Version B and memtable b (mutable). As a result, Version B and memtable b
// are prevented from being destroyed or deleted.

// ColumnFamilyHandleImpl is the class that clients use to access different
// column families. It has non-trivial destructor, which gets called when client
// is done using the column family
class ColumnFamilyHandleImpl : public ColumnFamilyHandle {
 public:
  // create while holding the mutex
  ColumnFamilyHandleImpl(ColumnFamilyData* cfd, DBImpl* db,
                         InstrumentedMutex* mutex);
  // destroy without mutex
  ~ColumnFamilyHandleImpl() override;
  virtual ColumnFamilyData* cfd() const { return cfd_; }
  virtual DBImpl* db() const { return db_; }

  uint32_t GetID() const override;
  const std::string& GetName() const override;
  Status GetDescriptor(ColumnFamilyDescriptor* desc) override;
  const Comparator* GetComparator() const override;

 private:
  ColumnFamilyData* cfd_;
  DBImpl* db_;
  InstrumentedMutex* mutex_;
};

// Does not ref-count ColumnFamilyData
// We use this dummy ColumnFamilyHandleImpl because sometimes MemTableInserter
// calls DBImpl methods. When this happens, MemTableInserter need access to
// ColumnFamilyHandle (same as the client would need). In that case, we feed
// MemTableInserter dummy ColumnFamilyHandle and enable it to call DBImpl
// methods
class ColumnFamilyHandleInternal : public ColumnFamilyHandleImpl {
 public:
  ColumnFamilyHandleInternal()
      : ColumnFamilyHandleImpl(nullptr, nullptr, nullptr),
        internal_cfd_(nullptr) {}

  void SetCFD(ColumnFamilyData* _cfd) { internal_cfd_ = _cfd; }
  ColumnFamilyData* cfd() const override { return internal_cfd_; }

 private:
  ColumnFamilyData* internal_cfd_;
};

// holds references to memtable, all immutable memtables and version
struct SuperVersion {
  // Accessing members of this class is not thread-safe and requires external
  // synchronization (ie db mutex held or on write thread).
  ColumnFamilyData* cfd;
  ReadOnlyMemTable* mem;
  MemTableListVersion* imm;
  Version* current;
  // TODO: do we really need this in addition to what's in current Version?
  MutableCFOptions mutable_cf_options;
  // Version number of the current SuperVersion
  uint64_t version_number;
  WriteStallCondition write_stall_condition;
  // Each time `full_history_ts_low` collapses history, a new SuperVersion is
  // installed. This field tracks the effective `full_history_ts_low` for that
  // SuperVersion, to be used by read APIs for sanity checks. This field is
  // immutable once SuperVersion is installed. For column family that doesn't
  // enable UDT feature, this is an empty string.
  std::string full_history_ts_low;

  // An immutable snapshot of the DB's seqno to time mapping, usually shared
  // between SuperVersions.
  std::shared_ptr<const SeqnoToTimeMapping> seqno_to_time_mapping{nullptr};

  // should be called outside the mutex
  SuperVersion() = default;
  ~SuperVersion();
  SuperVersion* Ref();
  // If Unref() returns true, Cleanup() should be called with mutex held
  // before deleting this SuperVersion.
  bool Unref();

  // call these two methods with db mutex held
  // Cleanup unrefs mem, imm and current. Also, it stores all memtables
  // that needs to be deleted in to_delete vector. Unrefing those
  // objects needs to be done in the mutex
  void Cleanup();
  void Init(
      ColumnFamilyData* new_cfd, MemTable* new_mem,
      MemTableListVersion* new_imm, Version* new_current,
      std::shared_ptr<const SeqnoToTimeMapping> new_seqno_to_time_mapping);

  // Share the ownership of the seqno to time mapping object referred to in this
  // SuperVersion. To be used by the new SuperVersion to be installed after this
  // one if seqno to time mapping does not change in between these two
  // SuperVersions. Or to share the ownership of the mapping with a FlushJob.
  std::shared_ptr<const SeqnoToTimeMapping> ShareSeqnoToTimeMapping() {
    return seqno_to_time_mapping;
  }

  // Access the seqno to time mapping object in this SuperVersion.
  UnownedPtr<const SeqnoToTimeMapping> GetSeqnoToTimeMapping() const {
    return seqno_to_time_mapping.get();
  }

  // The value of dummy is not actually used. kSVInUse takes its address as a
  // mark in the thread local storage to indicate the SuperVersion is in use
  // by thread. This way, the value of kSVInUse is guaranteed to have no
  // conflict with SuperVersion object address and portable on different
  // platform.
  static int dummy;
  static void* const kSVInUse;
  static void* const kSVObsolete;

 private:
  std::atomic<uint32_t> refs;
  // We need to_delete because during Cleanup(), imm->Unref() returns
  // all memtables that we need to free through this vector. We then
  // delete all those memtables outside of mutex, during destruction
  autovector<ReadOnlyMemTable*> to_delete;
};

Status CheckCompressionSupported(const ColumnFamilyOptions& cf_options);

Status CheckConcurrentWritesSupported(const ColumnFamilyOptions& cf_options);

Status CheckCFPathsSupported(const DBOptions& db_options,
                             const ColumnFamilyOptions& cf_options);

// Applies to configured creation and to this CF's sticky/pending format state,
// including read-only recovery. A DB-wide fence from another CF is not state
// belonging to this CF.
Status CheckBlobIndirectionCompactionStyle(CompactionStyle style,
                                           bool has_blob_indirection);

ColumnFamilyOptions SanitizeCfOptions(const ImmutableDBOptions& db_options,
                                      bool read_only,
                                      const ColumnFamilyOptions& src);
// Wrap user defined table properties collector factories `from cf_options`
// into internal ones in internal_tbl_prop_coll_factories. Add a system internal
// one too.
void GetInternalTblPropCollFactory(
    const ImmutableCFOptions& ioptions,
    InternalTblPropCollFactories* internal_tbl_prop_coll_factories);

class ColumnFamilySet;

// This class keeps all the data that a column family needs.
// Most methods require DB mutex held, unless otherwise noted
class ColumnFamilyData {
 public:
  ~ColumnFamilyData();

  // thread-safe
  uint32_t GetID() const { return id_; }
  // thread-safe
  const std::string& GetName() const { return name_; }

  // Ref() can only be called from a context where the caller can guarantee
  // that ColumnFamilyData is alive (while holding a non-zero ref already,
  // holding a DB mutex, or as the leader in a write batch group).
  void Ref() { refs_.fetch_add(1); }

  // UnrefAndTryDelete() decreases the reference count and do free if needed,
  // return true if this is freed else false, UnrefAndTryDelete() can only
  // be called while holding a DB mutex, or during single-threaded recovery.
  bool UnrefAndTryDelete();

  // SetDropped() can only be called under following conditions:
  // 1) Holding a DB mutex,
  // 2) from single-threaded write thread, AND
  // 3) from single-threaded VersionSet::LogAndApply()
  // After dropping column family no other operation on that column family
  // will be executed. All the files and memory will be, however, kept around
  // until client drops the column family handle. That way, client can still
  // access data from dropped column family.
  // Column family can be dropped and still alive. In that state:
  // *) Compaction and flush is not executed on the dropped column family.
  // *) Client can continue reading from column family. Writes will fail unless
  // WriteOptions::ignore_missing_column_families is true
  // When the dropped column family is unreferenced, then we:
  // *) Remove column family from the linked list maintained by ColumnFamilySet
  // *) delete all memory associated with that column family
  // *) delete all the files associated with that column family
  void SetDropped();
  bool IsDropped() const { return dropped_.load(std::memory_order_relaxed); }

  void SetFlushSkipReschedule();

  bool GetAndClearFlushSkipReschedule();

  // thread-safe
  int NumberLevels() const { return ioptions_.num_levels; }

  void SetLogNumber(uint64_t log_number) { log_number_ = log_number; }
  uint64_t GetLogNumber() const { return log_number_; }

  // thread-safe
  const FileOptions* soptions() const;
  const ImmutableOptions& ioptions() const { return ioptions_; }
  // REQUIRES: DB mutex held
  // This returns the MutableCFOptions used by current SuperVersion
  // You should use this API to reference MutableCFOptions most of the time.
  const MutableCFOptions& GetCurrentMutableCFOptions() const {
    return super_version_->mutable_cf_options;
  }
  // REQUIRES: DB mutex held
  // This returns the latest MutableCFOptions, which may be not in effect yet.
  const MutableCFOptions& GetLatestMutableCFOptions() const {
    return mutable_cf_options_;
  }

  // REQUIRES: DB mutex held
  // Build ColumnFamiliesOptions with immutable options and latest mutable
  // options.
  ColumnFamilyOptions GetLatestCFOptions() const;

  bool is_delete_range_supported() { return is_delete_range_supported_; }

  // Validate CF options against DB options
  static Status ValidateOptions(const DBOptions& db_options,
                                const ColumnFamilyOptions& cf_options);
  // Test-only, not public API. Until the Blob GC stack is complete,
  // ValidateOptions rejects enable_blob_indirection unless a test allows it.
  static void TEST_SetAllowBlobIndirection(bool allow);
  // Parses and validates a replacement without publishing it. The caller
  // keeps the DB mutex held so the latest-options base remains stable.
  Status PrepareMutableCFOptions(
      const DBOptions& db_options,
      const std::unordered_map<std::string, std::string>& options_map,
      MutableCFOptions* mutable_cf_options) const;
  // Publishes a previously prepared replacement. REQUIRES: DB mutex held.
  void InstallMutableCFOptions(const MutableCFOptions& mutable_cf_options);
  // REQUIRES: DB mutex held
  Status SetOptions(
      const DBOptions& db_options,
      const std::unordered_map<std::string, std::string>& options_map);

  InternalStats* internal_stats() { return internal_stats_.get(); }

  MemTableList* imm() { return &imm_; }
  MemTable* mem() { return mem_; }

  bool IsEmpty() {
    return mem()->GetFirstSequenceNumber() == 0 && imm()->NumNotFlushed() == 0;
  }

  Version* dummy_versions() { return dummy_versions_; }
  Version* current() { return current_; }  // REQUIRE: DB mutex held
  void SetCurrent(Version* _current);      // REQUIRE: DB mutex held
  uint64_t GetNumLiveVersions() const;     // REQUIRE: DB mutex held
  uint64_t GetTotalSstFilesSize() const;   // REQUIRE: DB mutex held
  uint64_t GetLiveSstFilesSize() const;    // REQUIRE: DB mutex held
  uint64_t GetTotalBlobFileSize() const;   // REQUIRE: DB mutex held
  // REQUIRE: DB mutex held
  void SetMemtable(MemTable* new_mem) {
    AssignMemtableID(new_mem);
    mem_ = new_mem;
    if (ioptions_.disallow_memtable_writes) {
      mem_->MarkImmutable();
    }
  }

  void AssignMemtableID(ReadOnlyMemTable* new_imm) {
    new_imm->SetID(++last_memtable_id_);
  }

  // calculate the oldest log needed for the durability of this column family
  uint64_t OldestLogToKeep();

  // See Memtable constructor for explanation of earliest_seq param.
  // `mutable_cf_options` might need to be a saved copy if calling this without
  // holding the DB mutex.
  MemTable* ConstructNewMemtable(const MutableCFOptions& mutable_cf_options,
                                 SequenceNumber earliest_seq);
  void CreateNewMemtable(SequenceNumber earliest_seq);

  TableCache* table_cache() const { return table_cache_.get(); }
  BlobFileCache* blob_file_cache() const { return blob_file_cache_.get(); }
  BlobSource* blob_source() const { return blob_source_.get(); }
  // Returns the write-path blob partition manager for this CF, or null if BDW
  // is disabled.
  BlobFilePartitionManager* blob_partition_manager() const {
    return blob_partition_manager_.get();
  }
  // Returns a shared ownership handle for cold paths that must keep the
  // manager alive beyond the lifetime of this ColumnFamilyData.
  std::shared_ptr<BlobFilePartitionManager> blob_partition_manager_handle()
      const {
    return blob_partition_manager_;
  }
  // Installs the write-path blob partition manager for this CF.
  void SetBlobPartitionManager(std::shared_ptr<BlobFilePartitionManager> mgr);

  // See documentation in compaction_picker.h
  // REQUIRES: DB mutex held
  bool NeedsCompaction() const;
  // REQUIRES: DB mutex held
  Compaction* PickCompaction(
      const MutableCFOptions& mutable_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, LogBuffer* log_buffer,
      bool require_max_output_level = false);

  // Check if the passed range overlap with any running compactions.
  // REQUIRES: DB mutex held
  bool RangeOverlapWithCompaction(const Slice& smallest_user_key,
                                  const Slice& largest_user_key, int level,
                                  bool range_limit_exclusive) const;

  // Check if the passed ranges overlap with any unflushed memtables
  // (immutable or mutable).
  //
  // @param super_version A referenced SuperVersion that will be held for the
  //    duration of this function.
  //
  // Thread-safe
  Status RangesOverlapWithMemtables(const autovector<UserKeyRange>& ranges,
                                    SuperVersion* super_version,
                                    bool allow_data_in_errors, bool* overlap,
                                    bool range_limit_exclusive);

  // A flag to tell a manual compaction is to compact all levels together
  // instead of a specific level.
  static const int kCompactAllLevels;
  // A flag to tell a manual compaction's output is base level.
  static const int kCompactToBaseLevel;
  // REQUIRES: DB mutex held
  Compaction* CompactRange(const MutableCFOptions& mutable_cf_options,
                           const MutableDBOptions& mutable_db_options,
                           int input_level, int output_level,
                           const CompactRangeOptions& compact_range_options,
                           const InternalKey* begin, const InternalKey* end,
                           InternalKey** compaction_end, bool* manual_conflict,
                           uint64_t max_file_num_to_ignore,
                           const std::string& trim_ts);

  CompactionPicker* compaction_picker() { return compaction_picker_.get(); }
  // thread-safe
  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }
  // thread-safe
  const InternalKeyComparator& internal_comparator() const {
    return internal_comparator_;
  }

  const InternalTblPropCollFactories* internal_tbl_prop_coll_factories() const {
    return &internal_tbl_prop_coll_factories_;
  }

  SuperVersion* GetSuperVersion() { return super_version_; }
  // thread-safe
  // Return a already referenced SuperVersion to be used safely.
  SuperVersion* GetReferencedSuperVersion(DBImpl* db);
  // thread-safe
  // Get SuperVersion stored in thread local storage. If it does not exist,
  // get a reference from a current SuperVersion.
  SuperVersion* GetThreadLocalSuperVersion(DBImpl* db);
  // Try to return SuperVersion back to thread local storage. Return true on
  // success and false on failure. It fails when the thread local storage
  // contains anything other than SuperVersion::kSVInUse flag.
  bool ReturnThreadLocalSuperVersion(SuperVersion* sv);
  // thread-safe
  uint64_t GetSuperVersionNumber() const {
    return super_version_number_.load();
  }
  uint64_t GetSuperVersionNumberRelaxed() const {
    return super_version_number_.load(std::memory_order_relaxed);
  }
  // Only intended for use by DBImpl::InstallSuperVersion() and variants
  void InstallSuperVersion(SuperVersionContext* sv_context,
                           InstrumentedMutex* db_mutex,
                           std::optional<std::shared_ptr<SeqnoToTimeMapping>>
                               new_seqno_to_time_mapping = {});

  void ResetThreadLocalSuperVersions();

  // Protected by DB mutex
  void set_queued_for_flush(bool value) { queued_for_flush_ = value; }
  void set_queued_for_compaction(bool value) { queued_for_compaction_ = value; }
  bool queued_for_flush() { return queued_for_flush_; }
  bool queued_for_compaction() { return queued_for_compaction_; }

  int compaction_aborted() const {
    return compaction_aborted_.obj_.load(std::memory_order_acquire);
  }
  void inc_compaction_aborted(InstrumentedMutex* db_mutex) {
    db_mutex->AssertHeld();
    compaction_aborted_.obj_.fetch_add(1, std::memory_order_release);
  }
  int dec_compaction_aborted(InstrumentedMutex* db_mutex) {
    db_mutex->AssertHeld();
    return compaction_aborted_.obj_.fetch_sub(1, std::memory_order_acq_rel) - 1;
  }

  int num_in_flight_compactions(InstrumentedMutex* db_mutex) const {
    db_mutex->AssertHeld();
    return num_in_flight_compactions_;
  }
  void inc_num_in_flight_compactions(InstrumentedMutex* db_mutex) {
    db_mutex->AssertHeld();
    ++num_in_flight_compactions_;
  }
  void dec_num_in_flight_compactions(InstrumentedMutex* db_mutex) {
    db_mutex->AssertHeld();
    assert(num_in_flight_compactions_ > 0);
    --num_in_flight_compactions_;
  }

  static std::pair<WriteStallCondition, WriteStallCause>
  GetWriteStallConditionAndCause(
      int num_unflushed_memtables, int num_l0_files,
      uint64_t num_compaction_needed_bytes,
      const MutableCFOptions& mutable_cf_options,
      const ImmutableCFOptions& immutable_cf_options);

  // Recalculate some stall conditions, which are changed only during
  // compaction, adding new memtable and/or recalculation of compaction score.
  WriteStallCondition RecalculateWriteStallConditions(
      const MutableCFOptions& mutable_cf_options);

  void set_initialized() { initialized_.store(true); }

  bool initialized() const { return initialized_.load(); }

  const ColumnFamilyOptions& initial_cf_options() {
    return initial_cf_options_;
  }

  // created_dirs remembers directory created, so that we don't need to call
  // the same data creation operation again.
  Status AddDirectories(
      std::map<std::string, std::shared_ptr<FSDirectory>>* created_dirs);

  FSDirectory* GetDataDir(size_t path_id) const;

  // full_history_ts_low_ can only increase.
  void SetFullHistoryTsLow(std::string ts_low) {
    assert(!ts_low.empty());
    const Comparator* ucmp = user_comparator();
    assert(ucmp);
    // Guard against resurrected full_history_ts_low persisted in MANIFEST
    // from previous DB sessions. This could happen if UDT was enabled and then
    // disabled.
    if (ucmp->timestamp_size() == 0) {
      return;
    }
    if (full_history_ts_low_.empty() ||
        ucmp->CompareTimestamp(ts_low, full_history_ts_low_) > 0) {
      full_history_ts_low_ = std::move(ts_low);
    }
  }

  const std::string& GetFullHistoryTsLow() const {
    const Comparator* ucmp = user_comparator();
    assert(ucmp);
    if (ucmp->timestamp_size() == 0) {
      assert(full_history_ts_low_.empty());
    }
    return full_history_ts_low_;
  }

  // REQUIRES: DB mutex held.
  // Return true if flushing up to MemTables with ID `max_memtable_id`
  // should be postponed to retain user-defined timestamps according to the
  // user's setting. Called by background flush job.
  bool ShouldPostponeFlushToRetainUDT(uint64_t max_memtable_id);

  ThreadLocalPtr* TEST_GetLocalSV() { return local_sv_.get(); }
  WriteBufferManager* write_buffer_mgr() { return write_buffer_manager_; }
  std::shared_ptr<CacheReservationManager>
  GetFileMetadataCacheReservationManager() {
    return file_metadata_cache_res_mgr_;
  }

  static const uint32_t kDummyColumnFamilyDataId;

  // Keep track of whether the mempurge feature was ever used.
  void SetMempurgeUsed() { mempurge_used_ = true; }
  bool GetMempurgeUsed() { return mempurge_used_; }

  // Allocate and return a new epoch number
  uint64_t NewEpochNumber() { return next_epoch_number_.fetch_add(1); }

  // Get the next epoch number to be assigned
  uint64_t GetNextEpochNumber() const { return next_epoch_number_.load(); }

  // Set the next epoch number to be assigned
  void SetNextEpochNumber(uint64_t next_epoch_number) {
    next_epoch_number_.store(next_epoch_number);
  }

  // Reset the next epoch number to be assigned
  void ResetNextEpochNumber() { next_epoch_number_.store(1); }

  // Recover the next epoch number of this CF and epoch number
  // of its files (if missing)
  void RecoverEpochNumbers();

  int GetUnflushedMemTableCountForWriteStallCheck() const {
    return (mem_->IsEmpty() ? 0 : 1) + imm_.NumNotFlushed();
  }

  // thread-safe, DB mutex not needed.
  bool AllowIngestBehind() const {
    return ioptions_.cf_allow_ingest_behind || ioptions_.allow_ingest_behind;
  }

  // Per-CF reader-writer lock that serializes IngestExternalFiles with range
  // tombstone conversion.
  port::RWMutex& GetIngestSstLock() { return ingest_sst_lock_; }

 private:
  friend class ColumnFamilySet;
  ColumnFamilyData(
      uint32_t id, const std::string& name, Version* dummy_versions,
      Cache* table_cache, WriteBufferManager* write_buffer_manager,
      const ColumnFamilyOptions& options, const ImmutableDBOptions& db_options,
      const FileOptions* file_options, ColumnFamilySet* column_family_set,
      BlockCacheTracer* const block_cache_tracer,
      const std::shared_ptr<IOTracer>& io_tracer, const std::string& db_id,
      const std::string& db_session_id, bool read_only,
      bool options_are_sanitized, bool fast_sst_open = false);

  std::vector<std::string> GetDbPaths() const;

  uint32_t id_;
  const std::string name_;
  Version* dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;         // == dummy_versions->prev_

  std::atomic<int> refs_;  // outstanding references to ColumnFamilyData
  std::atomic<bool> initialized_;
  std::atomic<bool> dropped_;  // true if client dropped it

  // When user-defined timestamps in memtable only feature is enabled, this
  // flag indicates a successfully requested flush that should
  // skip being rescheduled and haven't undergone the rescheduling check yet.
  // This flag is cleared when a check skips rescheduling a FlushRequest.
  // With this flag, automatic flushes in regular cases can continue to
  // retain UDTs by getting rescheduled as usual while manual flushes and
  // error recovery flushes will proceed without getting rescheduled.
  std::atomic<bool> flush_skip_reschedule_;

  const InternalKeyComparator internal_comparator_;
  InternalTblPropCollFactories internal_tbl_prop_coll_factories_;

  const ColumnFamilyOptions initial_cf_options_;
  const ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;

  const bool is_delete_range_supported_;

  std::unique_ptr<TableCache> table_cache_;
  std::unique_ptr<BlobFileCache> blob_file_cache_;
  std::unique_ptr<BlobSource> blob_source_;
  // Per-CF manager for write-path blob direct-write files.
  std::shared_ptr<BlobFilePartitionManager> blob_partition_manager_;

  std::unique_ptr<InternalStats> internal_stats_;

  WriteBufferManager* write_buffer_manager_;

  MemTable* mem_;
  MemTableList imm_;
  SuperVersion* super_version_;

  // An ordinal representing the current SuperVersion. Updated by
  // InstallSuperVersion(), i.e. incremented every time super_version_
  // changes.
  std::atomic<uint64_t> super_version_number_;

  // Thread's local copy of SuperVersion pointer
  // This needs to be destructed before mutex_
  std::unique_ptr<ThreadLocalPtr> local_sv_;

  // pointers for a circular linked list. we use it to support iterations over
  // all column families that are alive (note: dropped column families can also
  // be alive as long as client holds a reference)
  ColumnFamilyData* next_;
  ColumnFamilyData* prev_;

  // This is the earliest log file number that contains data from this
  // Column Family. All earlier log files must be ignored and not
  // recovered from
  uint64_t log_number_;

  // An object that keeps all the compaction stats
  // and picks the next compaction
  std::unique_ptr<CompactionPicker> compaction_picker_;

  ColumnFamilySet* column_family_set_;

  std::unique_ptr<WriteControllerToken> write_controller_token_;

  // If true --> this ColumnFamily is currently present in DBImpl::flush_queue_
  bool queued_for_flush_;

  // If true, this ColumnFamily is in DBImpl::compaction_queue_ or its parked
  // compaction set.
  bool queued_for_compaction_;

  // Read outside the DB mutex by compaction workers (acquire-loaded roughly
  // every 1024 records in ProcessKeyValueCompaction). Given its own cache line
  // via CacheAlignedWrapper to avoid false sharing with the DB-mutex-written
  // fields packed around it.
  CacheAlignedWrapper<std::atomic<int>> compaction_aborted_{};

  // Per-CF count of compactions "in flight" for AbortCompactions(): a picked
  // Compaction (or one CompactFiles call), tracked from
  // BeginInFlightCompaction() until EndInFlightCompaction() at the end of
  // BackgroundCompaction, so it covers the NotifyOnCompactionCompleted window
  // that CompactionPicker's compactions_in_progress_ set does not. NOT the same
  // as DBImpl::num_running_compactions_ (which counts BackgroundCallCompaction
  // worker invocations, backs the public rocksdb.num-running-compactions
  // property, and excludes CompactFiles). Protected by the DB mutex.
  int num_in_flight_compactions_{0};

  uint64_t prev_compaction_needed_bytes_;

  // if the database was opened with 2pc enabled
  bool allow_2pc_;

  // Memtable id to track flush.
  uint64_t last_memtable_id_;

  // Directories corresponding to cf_paths.
  std::vector<std::shared_ptr<FSDirectory>> data_dirs_;

  bool db_paths_registered_;

  std::string full_history_ts_low_;

  // For charging memory usage of file metadata created for newly added files to
  // a Version associated with this CFD
  std::shared_ptr<CacheReservationManager> file_metadata_cache_res_mgr_;
  bool mempurge_used_;

  std::atomic<uint64_t> next_epoch_number_;

  // Used to synchronize IngestExternalFile with range tombstone conversion. See
  // also Memtable::ingest_seqno_barrier_.
  port::RWMutex ingest_sst_lock_;
};

// ColumnFamilySet has interesting thread-safety requirements
// * CreateColumnFamily() or RemoveColumnFamily() -- need to be protected by DB
// mutex AND executed in the write thread.
// CreateColumnFamily() should ONLY be called from VersionSet::LogAndApply() AND
// single-threaded write thread. It is also called during Recovery and in
// DumpManifest().
// RemoveColumnFamily() is only called from SetDropped(). DB mutex needs to be
// held and it needs to be executed from the write thread. SetDropped() also
// guarantees that it will be called only from single-threaded LogAndApply(),
// but this condition is not that important.
// * Iteration -- hold DB mutex. If you want to release the DB mutex in the
// body of the iteration, wrap in a RefedColumnFamilySet.
// * GetDefault() -- thread safe
// * GetColumnFamily() -- either inside of DB mutex or from a write thread
// * GetNextColumnFamilyID(), GetMaxColumnFamily(), UpdateMaxColumnFamily(),
// NumberOfColumnFamilies -- inside of DB mutex
class ColumnFamilySet {
 public:
  // ColumnFamilySet supports iteration
  class iterator {
   public:
    explicit iterator(ColumnFamilyData* cfd) : current_(cfd) {}
    // NOTE: minimum operators for for-loop iteration
    iterator& operator++() {
      current_ = current_->next_;
      return *this;
    }
    bool operator!=(const iterator& other) const {
      return this->current_ != other.current_;
    }
    ColumnFamilyData* operator*() { return current_; }

   private:
    ColumnFamilyData* current_;
  };

  ColumnFamilySet(const std::string& dbname,
                  const ImmutableDBOptions* db_options,
                  const FileOptions& file_options, Cache* table_cache,
                  WriteBufferManager* _write_buffer_manager,
                  WriteController* _write_controller,
                  BlockCacheTracer* const block_cache_tracer,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  const std::string& db_id, const std::string& db_session_id,
                  bool fast_sst_open = false);
  ~ColumnFamilySet();

  ColumnFamilyData* GetDefault() const;
  // GetColumnFamily() calls return nullptr if column family is not found
  ColumnFamilyData* GetColumnFamily(uint32_t id) const;
  ColumnFamilyData* GetColumnFamily(const std::string& name) const;
  // this call will return the next available column family ID. it guarantees
  // that there is no column family with id greater than or equal to the
  // returned value in the current running instance or anytime in RocksDB
  // instance history.
  uint32_t GetNextColumnFamilyID();
  uint32_t GetMaxColumnFamily();
  void UpdateMaxColumnFamily(uint32_t new_max_column_family);
  size_t NumberOfColumnFamilies() const;

  ColumnFamilyData* CreateColumnFamily(const std::string& name, uint32_t id,
                                       Version* dummy_version,
                                       const ColumnFamilyOptions& options,
                                       bool read_only,
                                       bool options_are_sanitized);

  const UnorderedMap<uint32_t, size_t>& GetRunningColumnFamiliesTimestampSize()
      const {
    return running_ts_sz_;
  }

  const UnorderedMap<uint32_t, size_t>&
  GetColumnFamiliesTimestampSizeForRecord() const {
    return ts_sz_for_record_;
  }

  iterator begin() { return iterator(dummy_cfd_->next_); }
  iterator end() { return iterator(dummy_cfd_); }

  Cache* get_table_cache() { return table_cache_; }

  bool GetFastSstOpen() const { return fast_sst_open_; }
  void SetFastSstOpen(bool v) { fast_sst_open_ = v; }

  WriteBufferManager* write_buffer_manager() { return write_buffer_manager_; }

  void SetFlushInitiator(FlushInitiator* flush_initiator) {
    flush_initiator_ = flush_initiator;
  }

  FlushInitiator* flush_initiator() const { return flush_initiator_; }

  void RefreshFlushableMemAccounting();

  // Coalesces accounting rebuilds while a caller removes multiple column
  // families. Both methods require the DB mutex.
  void BeginBulkFlushableMemAccountingUpdate();
  void EndBulkFlushableMemAccountingUpdate();

  WriteController* write_controller() { return write_controller_; }

 private:
  friend class ColumnFamilyData;
  // helper function that gets called from cfd destructor
  // REQUIRES: DB mutex held
  void RemoveColumnFamily(ColumnFamilyData* cfd);

  // column_families_ and column_family_data_ need to be protected:
  // * when mutating both conditions have to be satisfied:
  // 1. DB mutex locked
  // 2. thread currently in single-threaded write thread
  // * when reading, at least one condition needs to be satisfied:
  // 1. DB mutex locked
  // 2. accessed from a single-threaded write thread
  UnorderedMap<std::string, uint32_t> column_families_;
  UnorderedMap<uint32_t, ColumnFamilyData*> column_family_data_;

  // Mutating / reading `running_ts_sz_` and `ts_sz_for_record_` follow
  // the same requirements as `column_families_` and `column_family_data_`.
  // Mapping from column family id to user-defined timestamp size for all
  // running column families.
  UnorderedMap<uint32_t, size_t> running_ts_sz_;
  // Mapping from column family id to user-defined timestamp size for
  // column families with non-zero user-defined timestamp size.
  UnorderedMap<uint32_t, size_t> ts_sz_for_record_;

  int bulk_flushable_mem_accounting_depth_ = 0;
  bool bulk_flushable_mem_accounting_dirty_ = false;

  uint32_t max_column_family_;
  const FileOptions file_options_;

  ColumnFamilyData* dummy_cfd_;
  // We don't hold the refcount here, since default column family always exists
  // We are also not responsible for cleaning up default_cfd_cache_. This is
  // just a cache that makes common case (accessing default column family)
  // faster
  ColumnFamilyData* default_cfd_cache_;

  const std::string db_name_;
  const ImmutableDBOptions* const db_options_;
  Cache* table_cache_;
  WriteBufferManager* write_buffer_manager_;
  FlushInitiator* flush_initiator_ = nullptr;
  WriteController* write_controller_;
  BlockCacheTracer* const block_cache_tracer_;
  std::shared_ptr<IOTracer> io_tracer_;
  const std::string& db_id_;
  std::string db_session_id_;
  bool fast_sst_open_;
};

// A wrapper for ColumnFamilySet that supports releasing DB mutex during each
// iteration over the iterator, because the cfd is Refed and Unrefed during
// each iteration to prevent concurrent CF drop from destroying it (until
// Unref).
class RefedColumnFamilySet {
 public:
  explicit RefedColumnFamilySet(ColumnFamilySet* cfs) : wrapped_(cfs) {}

  class iterator {
   public:
    explicit iterator(ColumnFamilySet::iterator wrapped) : wrapped_(wrapped) {
      MaybeRef(*wrapped_);
    }
    // No copying allowed
    iterator(const iterator&) = delete;
    iterator& operator=(const iterator&) = delete;
    iterator(iterator&&) = delete;
    iterator& operator=(iterator&&) = delete;
    ~iterator() { MaybeUnref(*wrapped_); }
    inline void MaybeRef(ColumnFamilyData* cfd) {
      if (cfd->GetID() != ColumnFamilyData::kDummyColumnFamilyDataId) {
        cfd->Ref();
      }
    }
    inline void MaybeUnref(ColumnFamilyData* cfd) {
      if (cfd->GetID() != ColumnFamilyData::kDummyColumnFamilyDataId) {
        cfd->UnrefAndTryDelete();
      }
    }
    // NOTE: minimum operators for for-loop iteration
    inline iterator& operator++() {
      ColumnFamilyData* old = *wrapped_;
      ++wrapped_;
      // Can only unref & potentially free cfd after accessing its next_
      MaybeUnref(old);
      MaybeRef(*wrapped_);
      return *this;
    }
    inline bool operator!=(const iterator& other) const {
      return this->wrapped_ != other.wrapped_;
    }
    inline ColumnFamilyData* operator*() { return *wrapped_; }

   private:
    ColumnFamilySet::iterator wrapped_;
  };

  iterator begin() { return iterator(wrapped_->begin()); }
  iterator end() { return iterator(wrapped_->end()); }

 private:
  ColumnFamilySet* wrapped_;
};

// We use ColumnFamilyMemTablesImpl to provide WriteBatch a way to access
// memtables of different column families (specified by ID in the write batch)
class ColumnFamilyMemTablesImpl : public ColumnFamilyMemTables {
 public:
  explicit ColumnFamilyMemTablesImpl(ColumnFamilySet* column_family_set)
      : column_family_set_(column_family_set), current_(nullptr) {}

  // Constructs a ColumnFamilyMemTablesImpl equivalent to one constructed
  // with the arguments used to construct *orig.
  explicit ColumnFamilyMemTablesImpl(ColumnFamilyMemTablesImpl* orig)
      : column_family_set_(orig->column_family_set_), current_(nullptr) {}

  // sets current_ to ColumnFamilyData with column_family_id
  // returns false if column family doesn't exist
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  bool Seek(uint32_t column_family_id) override;

  // Returns log number of the selected column family
  // REQUIRES: under a DB mutex OR from a write thread
  uint64_t GetLogNumber() const override;

  // REQUIRES: Seek() called first
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  MemTable* GetMemTable() const override;

  // Returns column family handle for the selected column family
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  ColumnFamilyHandle* GetColumnFamilyHandle() override;

  // Cannot be called while another thread is calling Seek().
  // REQUIRES: use this function of DBImpl::column_family_memtables_ should be
  //           under a DB mutex OR from a write thread
  ColumnFamilyData* current() override { return current_; }

 private:
  ColumnFamilySet* column_family_set_;
  ColumnFamilyData* current_;
  ColumnFamilyHandleInternal handle_;
};

uint32_t GetColumnFamilyID(ColumnFamilyHandle* column_family);

const Comparator* GetColumnFamilyUserComparator(
    ColumnFamilyHandle* column_family);

const ImmutableOptions& GetImmutableOptions(ColumnFamilyHandle* column_family);

}  // namespace ROCKSDB_NAMESPACE
