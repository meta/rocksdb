//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#include <deque>
#include <limits>
#include <list>
#include <set>
#include <string>
#include <vector>

#include "db/logs_with_prep_tracker.h"
#include "db/memtable.h"
#include "db/range_del_aggregator.h"
#include "file/filename.h"
#include "logging/log_buffer.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/types.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;
class InternalKeyComparator;
class InstrumentedMutex;
class MergeIteratorBuilder;
class MemTableList;

struct FlushJobInfo;

// Returns true when at least one bounded scan range overlaps the user-key
// range. Unbounded scan options conservatively overlap.
bool MultiScanOverlapsUserKeyRange(const MultiScanArgs* scan_opts,
                                   const Comparator* user_comparator,
                                   const Slice& smallest_user_key,
                                   const Slice& largest_user_key);

// Returns true when the memtable may contain keys or range deletions relevant
// to the scan ranges. Iterator/status errors conservatively overlap.
bool MultiScanIntersectsMemTable(
    ReadOnlyMemTable* memtable, const ReadOptions& read_options,
    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
    const SliceTransform* prefix_extractor, const MultiScanArgs* scan_opts,
    const Comparator* user_comparator);

// keeps a list of immutable memtables (ReadOnlyMemtable*) in a vector.
// The list is immutable if refcount is bigger than one. It is used as
// a state for Get() and iterator code paths.
//
// This class is not thread-safe. External synchronization is required
// (such as holding the db mutex or being on the write thread).
class MemTableListVersion {
 public:
  explicit MemTableListVersion(size_t* parent_memtable_list_memory_usage,
                               const MemTableListVersion& old);
  explicit MemTableListVersion(size_t* parent_memtable_list_memory_usage,
                               int64_t max_write_buffer_size_to_maintain);

  void Ref();
  void Unref(autovector<ReadOnlyMemTable*>* to_delete = nullptr);

  // Search all the memtables starting from the most recent one.
  // Return the most recent value found, if any.
  //
  // If any operation was found for this key, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, ReadCallback* callback = nullptr,
           bool* is_blob_index = nullptr,
           const BlobFetcher* blob_fetcher = nullptr);

  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           const ReadOptions& read_opts, ReadCallback* callback = nullptr,
           bool* is_blob_index = nullptr,
           const BlobFetcher* blob_fetcher = nullptr) {
    SequenceNumber seq;
    return Get(key, value, columns, timestamp, s, merge_context,
               max_covering_tombstone_seq, &seq, read_opts, callback,
               is_blob_index, blob_fetcher);
  }

  void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                ReadCallback* callback,
                const BlobFetcher* blob_fetcher = nullptr);

  // Returns all the merge operands corresponding to the key by searching all
  // memtables starting from the most recent one.
  bool GetMergeOperands(const LookupKey& key, Status* s,
                        MergeContext* merge_context,
                        SequenceNumber* max_covering_tombstone_seq,
                        const ReadOptions& read_opts,
                        const BlobFetcher* blob_fetcher = nullptr);

  // Similar to Get(), but searches the Memtable history of memtables that
  // have already been flushed.  Should only be used from in-memory only
  // queries (such as Transaction validation) as the history may contain
  // writes that are also present in the SST files.
  bool GetFromHistory(const LookupKey& key, std::string* value,
                      PinnableWideColumns* columns, std::string* timestamp,
                      Status* s, MergeContext* merge_context,
                      SequenceNumber* max_covering_tombstone_seq,
                      SequenceNumber* seq, const ReadOptions& read_opts,
                      bool* is_blob_index = nullptr,
                      const BlobFetcher* blob_fetcher = nullptr);
  bool GetFromHistory(const LookupKey& key, std::string* value,
                      PinnableWideColumns* columns, std::string* timestamp,
                      Status* s, MergeContext* merge_context,
                      SequenceNumber* max_covering_tombstone_seq,
                      const ReadOptions& read_opts,
                      bool* is_blob_index = nullptr,
                      const BlobFetcher* blob_fetcher = nullptr) {
    SequenceNumber seq;
    return GetFromHistory(key, value, columns, timestamp, s, merge_context,
                          max_covering_tombstone_seq, &seq, read_opts,
                          is_blob_index, blob_fetcher);
  }

  Status AddRangeTombstoneIterators(const ReadOptions& read_opts, Arena* arena,
                                    RangeDelAggregator* range_del_agg);

  void AddIterators(const ReadOptions& options,
                    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
                    const SliceTransform* prefix_extractor,
                    std::vector<InternalIterator*>* iterator_list,
                    Arena* arena);

  // read_seq controls range tombstone visibility. It may be captured before
  // lazy iterator initialization. scan_opts can prune immutable memtables that
  // cannot intersect the requested scan ranges.
  void AddIterators(const ReadOptions& options,
                    UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping,
                    const SliceTransform* prefix_extractor,
                    MergeIteratorBuilder* merge_iter_builder,
                    bool add_range_tombstone_iter,
                    SequenceNumber read_seq = kMaxSequenceNumber,
                    const MultiScanArgs* scan_opts = nullptr,
                    const Comparator* user_comparator = nullptr);

  uint64_t GetTotalNumEntries() const;

  uint64_t GetTotalNumDeletes() const;

  ReadOnlyMemTable::MemTableStats ApproximateStats(const Slice& start_ikey,
                                                   const Slice& end_ikey) const;

  // Returns the value of MemTable::GetEarliestSequenceNumber() on the most
  // recent MemTable in this list or kMaxSequenceNumber if the list is empty.
  // If include_history=true, will also search Memtables in MemTableList
  // History.
  SequenceNumber GetEarliestSequenceNumber(bool include_history = false) const;

  // Return the first sequence number from the memtable list, which is the
  // smallest sequence number of all FirstSequenceNumber.
  // Return kMaxSequenceNumber if the list is empty.
  SequenceNumber GetFirstSequenceNumber() const;

  // REQUIRES: db_mutex held.
  void SetID(uint64_t id) { id_ = id; }

  uint64_t GetID() const { return id_; }

  int NumNotFlushed() const { return static_cast<int>(memlist_.size()); }

  int NumFlushed() const { return static_cast<int>(memlist_history_.size()); }

  // Gets the newest user defined timestamps from the immutable memtables.
  // This returns the newest user defined timestamp found in the most recent
  // immutable memtable. This should only be called when user defined timestamp
  // is enabled.
  Slice GetNewestUDT() const;

 private:
  friend class MemTableList;

  friend Status InstallMemtableAtomicFlushResults(
      const autovector<MemTableList*>* imm_lists,
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const autovector<ReadOnlyMemTable*>*>& mems_list,
      VersionSet* vset, LogsWithPrepTracker* prep_tracker,
      InstrumentedMutex* mu, const autovector<FileMetaData*>& file_meta,
      const autovector<std::list<std::unique_ptr<FlushJobInfo>>*>&
          committed_flush_jobs_info,
      autovector<ReadOnlyMemTable*>* to_delete, FSDirectory* db_directory,
      LogBuffer* log_buffer);

  // REQUIRE: m is an immutable memtable
  void Add(ReadOnlyMemTable* m, autovector<ReadOnlyMemTable*>* to_delete);
  // REQUIRE: m is an immutable memtable
  void Remove(ReadOnlyMemTable* m, autovector<ReadOnlyMemTable*>* to_delete);

  // Return true if the memtable list should be trimmed to get memory usage
  // under budget.
  bool HistoryShouldBeTrimmed(size_t usage);

  // Trim history, Return true if memtable is trimmed
  bool TrimHistory(autovector<ReadOnlyMemTable*>* to_delete, size_t usage);

  bool GetFromList(std::list<ReadOnlyMemTable*>* list, const LookupKey& key,
                   std::string* value, PinnableWideColumns* columns,
                   std::string* timestamp, Status* s,
                   MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   ReadCallback* callback = nullptr,
                   bool* is_blob_index = nullptr,
                   const BlobFetcher* blob_fetcher = nullptr);

  void AddMemTable(ReadOnlyMemTable* m);

  void UnrefMemTable(autovector<ReadOnlyMemTable*>* to_delete,
                     ReadOnlyMemTable* m);

  // Calculate the total amount of memory used by memlist_ and memlist_history_
  // excluding the last MemTable in memlist_history_. The reason for excluding
  // the last MemTable is to see if dropping the last MemTable will keep total
  // memory usage above or equal to max_write_buffer_size_to_maintain_
  size_t MemoryAllocatedBytesExcludingLast() const;

  // Whether this version contains flushed memtables that are only kept around
  // for transaction conflict checking.
  bool HasHistory() const { return !memlist_history_.empty(); }

  bool MemtableLimitExceeded(size_t usage);

  // Immutable MemTables that have not yet been flushed.
  std::list<ReadOnlyMemTable*> memlist_;

  // MemTables that have already been flushed
  // (used during Transaction validation)
  std::list<ReadOnlyMemTable*> memlist_history_;

  // Maximum size of MemTables to keep in memory (including both flushed
  // and not-yet-flushed tables).
  const int64_t max_write_buffer_size_to_maintain_;

  int refs_ = 0;

  size_t* parent_memtable_list_memory_usage_;

  // MemtableListVersion id to track for flush results checking.
  uint64_t id_ = 0;
};

// This class stores references to all the immutable memtables.
// The memtables are flushed to L0 as soon as possible and in
// any order. If there are more than one immutable memtable, their
// flushes can occur concurrently.  However, they are 'committed'
// to the manifest in FIFO order to maintain correctness and
// recoverability from a crash.
//
//
// Other than imm_flush_needed and imm_trim_needed, this class is not
// thread-safe and requires external synchronization (such as holding the db
// mutex or being on the write thread.)
class MemTableList {
 public:
  // A list of memtables.
  explicit MemTableList(int min_write_buffer_number_to_merge,
                        int64_t max_write_buffer_size_to_maintain)
      : imm_flush_needed(false),
        imm_trim_needed(false),
        min_write_buffer_number_to_merge_(min_write_buffer_number_to_merge),
        current_(new MemTableListVersion(&current_memory_usage_,
                                         max_write_buffer_size_to_maintain)),
        num_flush_not_started_(0),
        commit_in_progress_(false),
        flush_requested_(false),
        current_memory_usage_(0),
        current_memory_allocted_bytes_excluding_last_(0),
        current_has_history_(false),
        last_memtable_list_version_id_(0) {
    current_->Ref();
  }

  // No copying allowed
  MemTableList(const MemTableList&) = delete;
  MemTableList& operator=(const MemTableList&) = delete;
  MemTableList(MemTableList&&) = delete;
  MemTableList& operator=(MemTableList&&) = delete;

  // Should not delete MemTableList without making sure MemTableList::current()
  // is Unref()'d.
  ~MemTableList() {
    // All flush-completion notifications must have drained before this column
    // family's memtable list is destroyed; otherwise a reserve leaked (see
    // num_pending_flush_notifications_).
    assert(num_pending_flush_notifications_ == 0);
  }

  MemTableListVersion* current() const { return current_; }

  // so that background threads can detect non-nullptr pointer to
  // determine whether there is anything more to start flushing.
  std::atomic<bool> imm_flush_needed;

  std::atomic<bool> imm_trim_needed;

  // Returns the total number of memtables in the list that haven't yet
  // been flushed and logged.
  int NumNotFlushed() const;

  // Returns total number of memtables in the list that have been
  // completely flushed and logged.
  int NumFlushed() const;

  // Flush-completion listener notification accounting. All require the DB
  // mutex to be held (this class is not internally synchronized). See
  // num_pending_flush_notifications_ for details.
  void AddPendingFlushNotifications(int n) {
    num_pending_flush_notifications_ += n;
  }
  void SubPendingFlushNotifications(int n) {
    num_pending_flush_notifications_ -= n;
    assert(num_pending_flush_notifications_ >= 0);
  }
  int NumPendingFlushNotifications() const {
    return num_pending_flush_notifications_;
  }

  // Returns true if there is at least one memtable on which flush has
  // not yet started.
  bool IsFlushPending() const;

  // Returns true if there is at least one memtable that is pending flush or
  // flushing.
  bool IsFlushPendingOrRunning() const;

  // Returns the earliest memtables that needs to be flushed. The returned
  // memtables are guaranteed to be in the ascending order of created time.
  void PickMemtablesToFlush(uint64_t max_memtable_id,
                            autovector<ReadOnlyMemTable*>* mems,
                            uint64_t* max_next_log_number = nullptr);

  // Reset status of the given memtable list back to pending state so that
  // they can get picked up again on the next round of flush.
  //
  // @param rollback_succeeding_memtables If true, will rollback adjacent
  // younger memtables whose flush is completed. Specifically, suppose the
  // current immutable memtables are M_0,M_1...M_N ordered from youngest to
  // oldest. Suppose that the youngest memtable in `mems` is M_K. We will try to
  // rollback M_K-1, M_K-2... until the first memtable whose flush is
  // not completed. These are the memtables that would have been installed
  // by this flush job if it were to succeed. This flag is currently used
  // by non atomic_flush rollback.
  // Note that we also do rollback in `write_manifest_cb` by calling
  // `RemoveMemTablesOrRestoreFlags()`. There we rollback the entire batch so
  // it is similar to what we do here with rollback_succeeding_memtables=true.
  void RollbackMemtableFlush(const autovector<ReadOnlyMemTable*>& mems,
                             bool rollback_succeeding_memtables);

  // Try commit a successful flush in the manifest file. It might just return
  // Status::OK letting a concurrent flush to do the actual the recording.
  Status TryInstallMemtableFlushResults(
      ColumnFamilyData* cfd, const autovector<ReadOnlyMemTable*>& m,
      LogsWithPrepTracker* prep_tracker, VersionSet* vset,
      InstrumentedMutex* mu, uint64_t file_number,
      autovector<ReadOnlyMemTable*>* to_delete, FSDirectory* db_directory,
      LogBuffer* log_buffer,
      std::list<std::unique_ptr<FlushJobInfo>>* committed_flush_jobs_info,
      bool write_edits = true);

  // New memtables are inserted at the front of the list.
  // Takes ownership of the referenced held on *m by the caller of Add().
  // By default, adding memtables will flag that the memtable list needs to be
  // flushed, but in certain situations, like after a mempurge, we may want to
  // avoid flushing the memtable list upon addition of a memtable.
  void Add(ReadOnlyMemTable* m, autovector<ReadOnlyMemTable*>* to_delete);

  // Returns an estimate of the number of bytes of data in use.
  size_t ApproximateMemoryUsage();

  // Returns the cached current_memory_allocted_bytes_excluding_last_ value.
  size_t MemoryAllocatedBytesExcludingLast() const;

  // Returns the cached current_has_history_ value.
  bool HasHistory() const;

  // Updates current_memory_allocted_bytes_excluding_last_ and
  // current_has_history_ from MemTableListVersion. Must be called whenever
  // InstallNewVersion is called.
  void UpdateCachedValuesFromMemTableListVersion();

  // `usage` is the current size of the mutable Memtable. When
  // max_write_buffer_size_to_maintain is used, total size of mutable and
  // immutable memtables is checked against it to decide whether to trim
  // memtable list.
  //
  // Return true if memtable is trimmed
  bool TrimHistory(autovector<ReadOnlyMemTable*>* to_delete, size_t usage);

  // Returns an estimate of the number of bytes of data used by
  // the unflushed mem-tables.
  size_t ApproximateUnflushedMemTablesMemoryUsage();

  // Returns the memory charged to the WriteBufferManager by unflushed
  // memtables.
  size_t WBMTrackedUnflushedMemTablesMemoryUsage() const;

  // Returns WBM-tracked memory for immutable memtables whose flush has not
  // started. Requires the DB mutex, like the other MemTableList accessors.
  size_t WBMTrackedUnstartedMemTablesMemoryUsage() const;

  // Returns an estimate of the timestamp of the earliest key.
  uint64_t ApproximateOldestKeyTime() const;

  // Request a flush of all existing memtables to storage. This will cause
  // future calls to IsFlushPending() to return true if this list is non-empty
  // (regardless of the min_write_buffer_number_to_merge parameter). This flush
  // request will persist until PickMemtablesToFlush() has picked all unstarted
  // memtables.
  void FlushRequested() {
    flush_requested_ = true;
    // If there are some memtables stored in imm() that don't trigger
    // flush (eg: mempurge output memtable), then update imm_flush_needed.
    // Note: if race condition and imm_flush_needed is set to true
    // when there is num_flush_not_started_==0, then there is no
    // impact whatsoever. Imm_flush_needed is only used in an assert
    // in IsFlushPending().
    if (num_flush_not_started_ > 0) {
      imm_flush_needed.store(true, std::memory_order_release);
    }
  }

  bool HasFlushRequested() { return flush_requested_; }

  // Returns true if a trim history should be scheduled and the caller should
  // be the one to schedule it
  bool MarkTrimHistoryNeeded() {
    auto expected = false;
    return imm_trim_needed.compare_exchange_strong(
        expected, true, std::memory_order_relaxed, std::memory_order_relaxed);
  }

  void ResetTrimHistoryNeeded() {
    auto expected = true;
    imm_trim_needed.compare_exchange_strong(
        expected, false, std::memory_order_relaxed, std::memory_order_relaxed);
  }

  size_t* current_memory_usage() { return &current_memory_usage_; }

  // Returns the WAL number of the oldest WAL that contains a prepared
  // transaction that corresponds to the content in this MemTableList,
  // after memtables listed in `memtables_to_flush` are flushed and their
  // status is persisted in manifest.
  uint64_t PrecomputeMinLogContainingPrepSection(
      const std::unordered_set<ReadOnlyMemTable*>* memtables_to_flush =
          nullptr) const;

  uint64_t GetEarliestMemTableID() const {
    auto& memlist = current_->memlist_;
    if (memlist.empty()) {
      return std::numeric_limits<uint64_t>::max();
    }
    return memlist.back()->GetID();
  }

  uint64_t GetLatestMemTableID(bool for_atomic_flush) const {
    auto& memlist = current_->memlist_;
    if (memlist.empty()) {
      return 0;
    }
    if (for_atomic_flush) {
      // Scan the memtable list from new to old
      for (auto it = memlist.begin(); it != memlist.end(); ++it) {
        ReadOnlyMemTable* m = *it;
        if (m->atomic_flush_seqno_ != kMaxSequenceNumber) {
          return m->GetID();
        }
      }
      return 0;
    }
    return memlist.front()->GetID();
  }

  // DB mutex held.
  // Gets the newest user-defined timestamp for the Memtables in ascending ID
  // order, up to the `max_memtable_id`. Used by background flush job
  // to check Memtables' eligibility for flush w.r.t retaining UDTs.
  std::vector<Slice> GetTablesNewestUDT(uint64_t max_memtable_id) {
    std::vector<Slice> newest_udts;
    auto& memlist = current_->memlist_;
    // Iterating through the memlist starting at the end, the vector<MemTable*>
    // ret is filled with memtables already sorted in increasing MemTable ID.
    for (auto it = memlist.rbegin(); it != memlist.rend(); ++it) {
      ReadOnlyMemTable* m = *it;
      if (m->GetID() > max_memtable_id) {
        break;
      }
      newest_udts.push_back(m->GetNewestUDT());
    }
    return newest_udts;
  }

  void AssignAtomicFlushSeq(const SequenceNumber& seq) {
    const auto& memlist = current_->memlist_;
    // Scan the memtable list from new to old
    for (auto it = memlist.begin(); it != memlist.end(); ++it) {
      ReadOnlyMemTable* mem = *it;
      if (mem->atomic_flush_seqno_ == kMaxSequenceNumber) {
        mem->atomic_flush_seqno_ = seq;
      } else {
        // Earlier memtables must have been assigned a atomic flush seq, no
        // need to continue scan.
        break;
      }
    }
  }

  // Used only by secondary and follower instances, which collect memtables by
  // the log number that follows their contents rather than by flushing them.
  // Scanning from the oldest memtable, removes those with
  // mem->GetNextLogNumber() <= log_number and stops at the first one without,
  // so a memtable is kept while an older one is. The memtables are not freed,
  // but put into a vector for future deref and reclamation.
  void RemoveOldMemTables(uint64_t log_number,
                          autovector<ReadOnlyMemTable*>* to_delete);

  // Returns whether RemoveOldMemTables(log_number) would remove anything, so
  // that a caller can skip the call and the bookkeeping that follows it. O(1):
  // the scan stops at the oldest memtable that does not qualify, so only the
  // oldest one can decide this.
  //
  // REQUIRES: db mutex held.
  bool HasOldMemTablesToRemove(uint64_t log_number) const {
    const auto& memlist = current_->memlist_;
    return !memlist.empty() && memlist.back()->GetNextLogNumber() <= log_number;
  }

  // This API is only used by atomic date replacement. To get an edit for
  // dropping the current `MemTableListVersion`.
  VersionEdit GetEditForDroppingCurrentVersion(
      const ColumnFamilyData* cfd, VersionSet* vset,
      LogsWithPrepTracker* prep_tracker) const;

 private:
  friend Status InstallMemtableAtomicFlushResults(
      const autovector<MemTableList*>* imm_lists,
      const autovector<ColumnFamilyData*>& cfds,
      const autovector<const autovector<ReadOnlyMemTable*>*>& mems_list,
      VersionSet* vset, LogsWithPrepTracker* prep_tracker,
      InstrumentedMutex* mu, const autovector<FileMetaData*>& file_meta,
      const autovector<std::list<std::unique_ptr<FlushJobInfo>>*>&
          committed_flush_jobs_info,
      autovector<ReadOnlyMemTable*>* to_delete, FSDirectory* db_directory,
      LogBuffer* log_buffer);

  // DB mutex held
  void InstallNewVersion();

  // DB mutex held
  // Called after writing to MANIFEST
  void RemoveMemTablesOrRestoreFlags(const Status& s, ColumnFamilyData* cfd,
                                     size_t batch_count, LogBuffer* log_buffer,
                                     autovector<ReadOnlyMemTable*>* to_delete,
                                     InstrumentedMutex* mu);

  const int min_write_buffer_number_to_merge_;

  MemTableListVersion* current_;

  // the number of elements that still need flushing
  int num_flush_not_started_;

  // committing in progress
  bool commit_in_progress_;

  // Requested a flush of memtables to storage. It's possible to request that
  // a subset of memtables be flushed.
  bool flush_requested_;

  // The current memory usage.
  size_t current_memory_usage_;

  // Cached value of current_->MemoryAllocatedBytesExcludingLast().
  std::atomic<size_t> current_memory_allocted_bytes_excluding_last_;

  // Cached value of current_->HasHistory().
  std::atomic<bool> current_has_history_;

  // Last memtabe list version id, increase by 1 each time a new
  // MemtableListVersion is installed.
  uint64_t last_memtable_list_version_id_;

  // Number of committed flush results for this column family whose
  // EventListener::OnFlushCompleted callbacks have not finished running yet.
  // Incremented (by the background flush thread) at flush-commit time, before
  // the DB mutex is released to invoke the callbacks, and decremented after the
  // callbacks return. Consulted by WaitForFlushMemTables() when the caller set
  // FlushOptions::listener_wait, so that Flush(wait=true) does not return
  // before the callbacks complete. Guarded by the DB mutex.
  int num_pending_flush_notifications_ = 0;
};

// Installs memtable atomic flush results.
// In most cases, imm_lists is nullptr, and the function simply uses the
// immutable memtable lists associated with the cfds. There are unit tests that
// installs flush results for external immutable memtable lists other than the
// cfds' own immutable memtable lists, e.g. MemTableLIstTest. In this case,
// imm_lists parameter is not nullptr.
Status InstallMemtableAtomicFlushResults(
    const autovector<MemTableList*>* imm_lists,
    const autovector<ColumnFamilyData*>& cfds,
    const autovector<const autovector<ReadOnlyMemTable*>*>& mems_list,
    VersionSet* vset, LogsWithPrepTracker* prep_tracker, InstrumentedMutex* mu,
    const autovector<FileMetaData*>& file_meta,
    const autovector<std::list<std::unique_ptr<FlushJobInfo>>*>&
        committed_flush_jobs_info,
    autovector<ReadOnlyMemTable*>* to_delete, FSDirectory* db_directory,
    LogBuffer* log_buffer);
}  // namespace ROCKSDB_NAMESPACE
