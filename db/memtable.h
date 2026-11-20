//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/dbformat.h"
#include "db/kv_checksum.h"
#include "db/merge_helper.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/read_callback.h"
#include "db/seqno_to_time_mapping.h"
#include "db/version_edit.h"
#include "memory/allocator.h"
#include "memory/concurrent_arena.h"
#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "table/multiget_context.h"
#include "util/atomic.h"
#include "util/cast_util.h"
#include "util/dynamic_bloom.h"
#include "util/hash.h"
#include "util/hash_containers.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class BlobFetcher;
class BlobFilePartitionManager;
struct FlushJobInfo;
class MemTableIterator;
class MergeContext;
class SystemClock;

struct ImmutableMemTableOptions {
  explicit ImmutableMemTableOptions(const ImmutableOptions& ioptions,
                                    const MutableCFOptions& mutable_cf_options);
  size_t arena_block_size;
  uint32_t memtable_prefix_bloom_bits;
  size_t memtable_huge_page_size;
  bool memtable_whole_key_filtering;
  bool inplace_update_support;
  size_t inplace_update_num_locks;
  UpdateStatus (*inplace_callback)(char* existing_value,
                                   uint32_t* existing_value_size,
                                   Slice delta_value,
                                   std::string* merged_value);
  size_t max_successive_merges;
  bool strict_max_successive_merges;
  Statistics* statistics;
  MergeOperator* merge_operator;
  Logger* info_log;
  uint32_t protection_bytes_per_key;
  bool allow_data_in_errors;
  bool paranoid_memory_checks;
  bool memtable_verify_per_key_checksum_on_seek;
  bool memtable_batch_lookup_optimization;
};

// Batched counters to updated when inserting keys in one write batch.
// In post process of the write batch, these can be updated together.
// Only used in concurrent memtable insert case.
struct MemTablePostProcessInfo {
  uint64_t data_size = 0;
  uint64_t num_entries = 0;
  uint64_t num_deletes = 0;
  uint64_t num_range_deletes = 0;
};

using MultiGetRange = MultiGetContext::Range;

// For each CF, rocksdb maintains an active memtable that accept writes,
// and zero or more sealed memtables that we call immutable memtables.
// This interface contains all methods required for immutable memtables.
// MemTable class inherit from `ReadOnlyMemTable` and implements additional
// methods required for active memtables.
// Immutable memtable list (MemTableList) maintains a list of ReadOnlyMemTable
// objects. This interface enables feature like direct ingestion of an
// immutable memtable with custom implementation, bypassing memtable writes.
//
// Note: Many of the methods in this class have comments indicating that
// external synchronization is required as these methods are not thread-safe.
// It is up to higher layers of code to decide how to prevent concurrent
// invocation of these methods. This is usually done by acquiring either
// the db mutex or the single writer thread.
//
// Some of these methods are documented to only require external
// synchronization if this memtable is immutable. Calling MarkImmutable() is
// not sufficient to guarantee immutability.  It is up to higher layers of
// code to determine if this MemTable can still be modified by other threads.
// Eg: The Superversion stores a pointer to the current MemTable (that can
// be modified) and a separate list of the MemTables that can no longer be
// written to (aka the 'immutable memtables').
//
// MemTables are reference counted. The initial reference count
// is zero and the caller must call Ref() at least once.
class ReadOnlyMemTable {
 public:
  // Do not delete this MemTable unless Unref() indicates it not in use.
  virtual ~ReadOnlyMemTable() = default;

  virtual const char* Name() const = 0;

  // Returns an estimate of the number of bytes of data in use by this
  // data structure.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual size_t ApproximateMemoryUsage() = 0;

  // used by MemTableListVersion::MemoryAllocatedBytesExcludingLast
  virtual size_t MemoryAllocatedBytes() const = 0;

  // Returns memory charged to this memtable's WriteBufferManager. Memtable
  // implementations that do not allocate through a WBM return zero.
  virtual size_t WBMTrackedMemoryUsage() const = 0;

  // Returns a vector of unique random memtable entries of size 'sample_size'.
  //
  // Note: the entries are stored in the unordered_set as length-prefixed keys,
  //       hence their representation in the set as "const char*".
  // Note2: the size of the output set 'entries' is not enforced to be strictly
  //        equal to 'target_sample_size'. Its final size might be slightly
  //        greater or slightly less than 'target_sample_size'
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  // REQUIRES: SkipList memtable representation. This function is not
  // implemented for any other type of memtable representation (vectorrep,
  // hashskiplist,...).
  virtual void UniqueRandomSample(const uint64_t& target_sample_size,
                                  std::unordered_set<const char*>* entries) = 0;

  // Return an iterator that yields the contents of the memtable.
  //
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.  The keys returned by this
  // iterator are internal keys encoded by AppendInternalKey in the
  // db/dbformat.{h,cc} module.
  //
  // By default, it returns an iterator for prefix seek if prefix_extractor
  // is configured in Options.
  // arena: If not null, the arena needs to be used to allocate the Iterator.
  //        Calling ~Iterator of the iterator will destroy all the states but
  //        those allocated in arena.
  // seqno_to_time_mapping: it's used to support return write unix time for the
  // data, currently only needed for iterators serving user reads.
  virtual InternalIterator* NewIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, bool for_flush) = 0;

  // Returns an iterator that wraps a MemTableIterator and logically strips the
  // user-defined timestamp of each key. This API is only used by flush when
  // user-defined timestamps in MemTable only feature is enabled.
  virtual InternalIterator* NewTimestampStrippingIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, size_t ts_sz) = 0;

  // Returns an iterator that yields the range tombstones of the memtable.
  // The caller must ensure that the underlying MemTable remains live
  // while the returned iterator is live.
  // @param immutable_memtable Whether this memtable is an immutable memtable.
  // This information is not stored in memtable itself, so it needs to be
  // specified by the caller. This flag is used internally to decide whether a
  // cached fragmented range tombstone list can be returned. This cached version
  // is constructed when a memtable becomes immutable. Setting the flag to false
  // will always yield correct result, but may incur performance penalty as it
  // always creates a new fragmented range tombstone list.
  virtual FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable) = 0;

  // Returns an iterator that yields the range tombstones of the memtable and
  // logically strips the user-defined timestamp of each key (including start
  // key, and end key). This API is only used by flush when user-defined
  // timestamps in MemTable only feature is enabled.
  virtual FragmentedRangeTombstoneIterator*
  NewTimestampStrippingRangeTombstoneIterator(const ReadOptions& read_options,
                                              SequenceNumber read_seq,
                                              size_t ts_sz) = 0;

  // Used to get value associated with `key`, or Merge operands associated
  // with key, or get the latest sequence number of `key` (e.g. transaction
  // conflict checking).
  //
  // Keys are considered if they are no smaller than the parameter `key` in
  // the order defined by comparator and share the save user key with `key`.
  //
  // If do_merge = true the default behavior which is Get value for key is
  // executed. Expected behavior is described right below.
  // If memtable contains a value for key, store it in *value and return true.
  // If memtable contains a deletion for key, store NotFound() in *status and
  // return true.
  // If memtable contains Merge operation as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operand to *operands.
  //   store MergeInProgress in s, and return false.
  // If an unexpected error or corruption occurs, store Corruption() or other
  // error in *status and return true.
  // Else, return false.
  // If any operation was found, its most recent sequence number
  // will be stored in *seq on success (regardless of whether true/false is
  // returned).  Otherwise, *seq will be set to kMaxSequenceNumber.
  // On success, *s may be set to OK, NotFound, or MergeInProgress.  Any other
  // status returned indicates a corruption or other unexpected error.
  //
  // If do_merge = false then any Merge Operands encountered for key are simply
  // stored in merge_context.operands_list and never actually merged to get a
  // final value. The raw Merge Operands are eventually returned to the user.
  // @param value If not null and memtable contains a value for key, `value`
  // will be set to the result value.
  // @param column If not null and memtable contains a value/WideColumn for key,
  // `column` will be set to the result value/WideColumn.
  // Note: only one of `value` and `column` can be non-nullptr.
  // To only query for key existence or the latest sequence number of a key,
  // `value` and `column` can be both nullptr. In this case, returned status can
  // be OK, NotFound or MergeInProgress if a key is found.
  // @param immutable_memtable Whether this memtable is immutable. Used
  // internally by NewRangeTombstoneIterator(). See comment above
  // NewRangeTombstoneIterator() for more detail.
  virtual bool Get(const LookupKey& key, std::string* value,
                   PinnableWideColumns* columns, std::string* timestamp,
                   Status* s, MergeContext* merge_context,
                   SequenceNumber* max_covering_tombstone_seq,
                   SequenceNumber* seq, const ReadOptions& read_opts,
                   bool immutable_memtable, ReadCallback* callback = nullptr,
                   bool* is_blob_index = nullptr, bool do_merge = true,
                   const BlobFetcher* blob_fetcher = nullptr) = 0;
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true, const BlobFetcher* blob_fetcher = nullptr) {
    SequenceNumber seq;
    return Get(key, value, columns, timestamp, s, merge_context,
               max_covering_tombstone_seq, &seq, read_opts, immutable_memtable,
               callback, is_blob_index, do_merge, blob_fetcher);
  }

  // @param immutable_memtable Whether this memtable is immutable. Used
  // internally by NewRangeTombstoneIterator(). See comment above
  // NewRangeTombstoneIterator() for more detail.
  virtual void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                        ReadCallback* callback, bool immutable_memtable,
                        const BlobFetcher* blob_fetcher = nullptr) = 0;

  // Get total number of entries in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual uint64_t NumEntries() const = 0;

  // Get total number of point deletes in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual uint64_t NumDeletion() const = 0;

  // Get total number of range deletions in the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual uint64_t NumRangeDeletion() const = 0;

  virtual uint64_t GetDataSize() const = 0;

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual SequenceNumber GetFirstSequenceNumber() = 0;

  // Returns if there is no entry inserted to the mem table.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  virtual bool IsEmpty() const = 0;

  // Returns the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  //
  // If the earliest sequence number could not be determined,
  // kMaxSequenceNumber will be returned.
  virtual SequenceNumber GetEarliestSequenceNumber() = 0;

  virtual uint64_t GetMinLogContainingPrepSection() = 0;

  // Notify the underlying storage that no more items will be added.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  // After MarkImmutable() is called, you should not attempt to
  // write anything to this MemTable().  (Ie. do not call Add() or Update()).
  virtual void MarkImmutable() = 0;

  // Notify the underlying storage that all data it contained has been
  // persisted.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  virtual void MarkFlushed() = 0;

  struct MemTableStats {
    uint64_t size;
    uint64_t count;
  };
  virtual MemTableStats ApproximateStats(const Slice& start_ikey,
                                         const Slice& end_ikey) = 0;

  virtual const InternalKeyComparator& GetInternalKeyComparator() const = 0;

  virtual uint64_t ApproximateOldestKeyTime() const = 0;

  // Inserts a range tombstone [start_key, end_key) that is logically redundant
  // -- it is derived from existing point tombstones observed during iteration
  // and does not delete any data that isn't already deleted. This is a
  // best-effort optimization. It allows future reads to skip iterating over
  // continuous single deletion tombstones.
  //
  // Adding a range tombstone may fail if
  // - memtable switches to immutable state
  // - a range tombstone with the same key+seq already exists (duplicate insert)
  // - the per-memtable ingest seqno barrier already exceeds `seq` (an
  //   ingestion has committed an L0 file at a seq that this converted
  //   tombstone would shadow) or an ingestion is in progress.
  // - `version_number`, the number of the Version the converting iterator
  //   reads, is below the per-memtable ingest version barrier (the iterator
  //   cannot see a file ingested while this memtable was active).
  // Returns true if the range tombstone was inserted, false if skipped.
  virtual bool AddLogicallyRedundantRangeTombstone(
      SequenceNumber /*seq*/, const Slice& /*start_key*/,
      const Slice& /*end_key*/, uint64_t /*version_number*/,
      port::RWMutex& /*ingest_sst_lock*/) {
    return false;
  }

  // Returns whether a fragmented range tombstone list is already constructed
  // for this memtable. It should be constructed right before a memtable is
  // added to an immutable memtable list. Note that if a memtable does not have
  // any range tombstone, then no range tombstone list will ever be constructed
  // and true is returned in that case.
  virtual bool IsFragmentedRangeTombstonesConstructed() const = 0;

  // Get the newest user-defined timestamp contained in this MemTable. Check
  // `newest_udt_` for what newer means. This method should only be invoked for
  // an MemTable that has enabled user-defined timestamp feature and set
  // `persist_user_defined_timestamps` to false. The tracked newest UDT will be
  // used by flush job in the background to help check the MemTable's
  // eligibility for Flush.
  virtual Slice GetNewestUDT() const = 0;

  // Increase reference count.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void Ref() { ++refs_; }

  // Drop reference count.
  // If the refcount goes to zero return this memtable, otherwise return null.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  ReadOnlyMemTable* Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      ReleaseProtectedSealedBlobFiles();
      return this;
    }
    return nullptr;
  }

  // Registers sealed direct-write blob files that this memtable can still
  // resolve through lazy blob indexes. The protection lasts until the
  // memtable's final Unref(). The manager handle is shared here because
  // immutable memtables can outlive the ColumnFamilyData that created them.
  // REQUIRES: external synchronization to prevent simultaneous operations on
  // the same MemTable.
  void ProtectSealedBlobFiles(
      const std::shared_ptr<BlobFilePartitionManager>& blob_partition_manager,
      const std::vector<uint64_t>& file_numbers);

  // Returns the edits area that is needed for flushing the memtable
  VersionEdit* GetEdits() { return &edit_; }

  // Returns the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  uint64_t GetNextLogNumber() const { return mem_next_walfile_number_; }

  // Sets the next active logfile number when this memtable is about to
  // be flushed to storage
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  void SetNextLogNumber(uint64_t num) { mem_next_walfile_number_ = num; }

  // REQUIRES: db_mutex held.
  void SetID(uint64_t id) { id_ = id; }

  uint64_t GetID() const { return id_; }

  void SetFlushCompleted(bool completed) { flush_completed_ = completed; }

  uint64_t GetFileNumber() const { return file_number_; }

  void SetFileNumber(uint64_t file_num) { file_number_ = file_num; }

  void SetFlushInProgress(bool in_progress) {
    flush_in_progress_ = in_progress;
  }

  void SetFlushJobInfo(std::unique_ptr<FlushJobInfo>&& info) {
    flush_job_info_ = std::move(info);
  }

  std::unique_ptr<FlushJobInfo> ReleaseFlushJobInfo() {
    return std::move(flush_job_info_);
  }

  static void HandleTypeValue(
      const Slice& lookup_user_key, const Slice& value, bool value_pinned,
      bool do_merge, bool merge_in_progress, MergeContext* merge_context,
      const MergeOperator* merge_operator, SystemClock* clock,
      Statistics* statistics, Logger* info_log, Status* s,
      std::string* out_value, PinnableWideColumns* out_columns,
      bool* is_blob_index) {
    *s = Status::OK();

    if (!do_merge) {
      // Preserve the value with the goal of returning it as part of
      // raw merge operands to the user
      // TODO(yanqin) update MergeContext so that timestamps information
      // can also be retained.
      merge_context->PushOperand(value, value_pinned);
    } else if (merge_in_progress) {
      // `op_failure_scope` (an output parameter) is not provided (set to
      // nullptr) since a failure must be propagated regardless of its
      // value.
      if (out_value || out_columns) {
        *s = MergeHelper::TimedFullMerge(
            merge_operator, lookup_user_key, MergeHelper::kPlainBaseValue,
            value, merge_context->GetOperands(), info_log, statistics, clock,
            /* update_num_ops_stats */ true,
            /* op_failure_scope */ nullptr, out_value, out_columns);
      }
    } else if (out_value) {
      out_value->assign(value.data(), value.size());
    } else if (out_columns) {
      out_columns->SetPlainValue(value);
    }

    if (is_blob_index) {
      *is_blob_index = false;
    }
  }

  static void HandleTypeDeletion(
      const Slice& lookup_user_key, bool merge_in_progress,
      MergeContext* merge_context, const MergeOperator* merge_operator,
      SystemClock* clock, Statistics* statistics, Logger* logger, Status* s,
      std::string* out_value, PinnableWideColumns* out_columns) {
    if (merge_in_progress) {
      if (out_value || out_columns) {
        // `op_failure_scope` (an output parameter) is not provided (set to
        // nullptr) since a failure must be propagated regardless of its
        // value.
        *s = MergeHelper::TimedFullMerge(
            merge_operator, lookup_user_key, MergeHelper::kNoBaseValue,
            merge_context->GetOperands(), logger, statistics, clock,
            /* update_num_ops_stats */ true,
            /* op_failure_scope */ nullptr, out_value, out_columns);
      } else {
        // We have found a final value (a base deletion) and have newer
        // merge operands that we do not intend to merge. Nothing remains
        // to be done so assign status to OK.
        *s = Status::OK();
      }
    } else {
      *s = Status::NotFound();
    }
  }

  // Returns if a final value is found.
  static bool HandleTypeMerge(const Slice& lookup_user_key, const Slice& value,
                              bool value_pinned, bool do_merge,
                              MergeContext* merge_context,
                              const MergeOperator* merge_operator,
                              SystemClock* clock, Statistics* statistics,
                              Logger* logger, Status* s, std::string* out_value,
                              PinnableWideColumns* out_columns) {
    if (!merge_operator) {
      *s = Status::InvalidArgument(
          "merge_operator is not properly initialized.");
      // Normally we continue the loop (return true) when we see a merge
      // operand.  But in case of an error, we should stop the loop
      // immediately and pretend we have found the value to stop further
      // seek.  Otherwise, the later call will override this error status.
      return true;
    }
    merge_context->PushOperand(value, value_pinned /* operand_pinned */);
    PERF_COUNTER_ADD(internal_merge_point_lookup_count, 1);

    if (do_merge && merge_operator->ShouldMerge(
                        merge_context->GetOperandsDirectionBackward())) {
      if (out_value || out_columns) {
        // `op_failure_scope` (an output parameter) is not provided (set to
        // nullptr) since a failure must be propagated regardless of its
        // value.
        *s = MergeHelper::TimedFullMerge(
            merge_operator, lookup_user_key, MergeHelper::kNoBaseValue,
            merge_context->GetOperands(), logger, statistics, clock,
            /* update_num_ops_stats */ true,
            /* op_failure_scope */ nullptr, out_value, out_columns);
      }
      return true;
    }
    if (merge_context->get_merge_operands_options != nullptr &&
        merge_context->get_merge_operands_options->continue_cb != nullptr &&
        !merge_context->get_merge_operands_options->continue_cb(value)) {
      // We were told not to continue. `status` may be MergeInProress(),
      // overwrite to signal the end of successful get. This status
      // will be checked at the end of GetImpl().
      *s = Status::OK();
      return true;
    }

    // no final value found yet
    return false;
  }

  void MarkForFlush() { marked_for_flush_.StoreRelaxed(true); }

  bool IsMarkedForFlush() const { return marked_for_flush_.LoadRelaxed(); }

 protected:
  friend class MemTableList;

  int refs_{0};

  // These are used to manage memtable flushes to storage
  bool flush_in_progress_{false};  // started the flush
  bool flush_completed_{false};    // finished the flush
  uint64_t file_number_{0};

  // The updates to be applied to the transaction log when this
  // memtable is flushed to storage.
  VersionEdit edit_;

  // The log files earlier than this number can be deleted.
  uint64_t mem_next_walfile_number_{0};

  // Memtable id to track flush.
  uint64_t id_ = 0;

  // Sequence number of the atomic flush that is responsible for this memtable.
  // The sequence number of atomic flush is a seq, such that no writes with
  // sequence numbers greater than or equal to seq are flushed, while all
  // writes with sequence number smaller than seq are flushed.
  SequenceNumber atomic_flush_seqno_{kMaxSequenceNumber};

  // Flush job info of the current memtable.
  std::unique_ptr<FlushJobInfo> flush_job_info_;

  RelaxedAtomic<bool> marked_for_flush_{false};

 private:
  void ReleaseProtectedSealedBlobFiles();

  std::shared_ptr<BlobFilePartitionManager> protected_blob_file_manager_;
  std::vector<uint64_t> protected_blob_file_numbers_;
};

class MemTable final : public ReadOnlyMemTable {
 public:
  struct KeyComparator final : public MemTableRep::KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* prefix_len_key1,
                   const char* prefix_len_key2) const override;
    int operator()(const char* prefix_len_key,
                   const DecodedType& key) const override;
  };

  // earliest_seq should be the current SequenceNumber in the db such that any
  // key inserted into this memtable will have an equal or larger seq number.
  // (When a db is first created, the earliest sequence number will be 0).
  // If the earliest sequence number is not known, kMaxSequenceNumber may be
  // used, but this may prevent some transactions from succeeding until the
  // first key is inserted into the memtable.
  explicit MemTable(const InternalKeyComparator& comparator,
                    const ImmutableOptions& ioptions,
                    const MutableCFOptions& mutable_cf_options,
                    WriteBufferManager* write_buffer_manager,
                    SequenceNumber earliest_seq, uint32_t column_family_id,
                    FlushInitiator* flush_initiator);
  // No copying allowed
  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;
  MemTable(MemTable&&) = delete;
  MemTable& operator=(MemTable&&) = delete;

  ~MemTable() override;

  const char* Name() const override { return "MemTable"; }

  size_t ApproximateMemoryUsage() override;

  // As a cheap version of `ApproximateMemoryUsage()`, this function doesn't
  // require external synchronization. The value may be less accurate though
  size_t ApproximateMemoryUsageFast() const {
    return approximate_memory_usage_.LoadRelaxed();
  }

  size_t MemoryAllocatedBytes() const override {
    return table_->ApproximateMemoryUsage() +
           range_del_table_->ApproximateMemoryUsage() +
           arena_.MemoryAllocatedBytes();
  }

  size_t WBMTrackedMemoryUsage() const override {
    return mem_tracker_.allocated_bytes();
  }

  bool IsWBMTrackingActive() const {
    return mem_tracker_.IsFlushInitiatorActive();
  }

  void RefreshWBMTracking() {
    ReadLock rl(&immutable_mutex_);
    mem_tracker_.RefreshFlushInitiator();
  }

  void StopWBMTracking() {
    WriteLock wl(&immutable_mutex_);
    mem_tracker_.DeactivateFlushInitiator();
  }

  void UniqueRandomSample(const uint64_t& target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    // TODO(bjlemaire): at the moment, only supported by skiplistrep.
    // Extend it to all other memtable representations.
    table_->UniqueRandomSample(NumEntries(), target_sample_size, entries);
  }

  // This method heuristically determines if the memtable should continue to
  // host more data.
  bool ShouldScheduleFlush() const {
    return flush_state_.load(std::memory_order_relaxed) == FLUSH_REQUESTED;
  }

  // Returns true if a flush should be scheduled and the caller should
  // be the one to schedule it
  bool MarkFlushScheduled() {
    auto before = FLUSH_REQUESTED;
    return flush_state_.compare_exchange_strong(before, FLUSH_SCHEDULED,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed);
  }

  // Returns true if a flush has already been scheduled for this memtable
  bool HasFlushScheduled() const {
    return flush_state_.load(std::memory_order_relaxed) == FLUSH_SCHEDULED;
  }

  InternalIterator* NewIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, bool for_flush) override;

  InternalIterator* NewTimestampStrippingIterator(
      const ReadOptions& read_options,
      UnownedPtr<const SeqnoToTimeMapping> seqno_to_time_mapping, Arena* arena,
      const SliceTransform* prefix_extractor, size_t ts_sz) override;

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable) override;

  FragmentedRangeTombstoneIterator* NewTimestampStrippingRangeTombstoneIterator(
      const ReadOptions& read_options, SequenceNumber read_seq,
      size_t ts_sz) override;

  Status VerifyEncodedEntry(Slice encoded,
                            const ProtectionInfoKVOS64& kv_prot_info);

  // Add an entry into memtable that maps key to value at the
  // specified sequence number and with the specified type.
  // Typically, value will be empty if type==kTypeDeletion.
  //
  // REQUIRES: if allow_concurrent = false, external synchronization to prevent
  // simultaneous operations on the same MemTable.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  Status Add(SequenceNumber seq, ValueType type, const Slice& key,
             const Slice& value, const ProtectionInfoKVOS64* kv_prot_info,
             bool allow_concurrent = false,
             MemTablePostProcessInfo* post_process_info = nullptr,
             void** hint = nullptr);

  using ReadOnlyMemTable::Get;
  bool Get(const LookupKey& key, std::string* value,
           PinnableWideColumns* columns, std::string* timestamp, Status* s,
           MergeContext* merge_context,
           SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
           const ReadOptions& read_opts, bool immutable_memtable,
           ReadCallback* callback = nullptr, bool* is_blob_index = nullptr,
           bool do_merge = true,
           const BlobFetcher* blob_fetcher = nullptr) override;

  void MultiGet(const ReadOptions& read_options, MultiGetRange* range,
                ReadCallback* callback, bool immutable_memtable,
                const BlobFetcher* blob_fetcher = nullptr) override;

  // If `key` exists in current memtable with type value_type and the existing
  // value is at least as large as the new value, updates it in-place. Otherwise
  // adds the new value to the memtable out-of-place.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  Status Update(SequenceNumber seq, ValueType value_type, const Slice& key,
                const Slice& value, const ProtectionInfoKVOS64* kv_prot_info);

  // If `key` exists in current memtable with type `kTypeValue` and the existing
  // value is at least as large as the new value, updates it in-place. Otherwise
  // if `key` exists in current memtable with type `kTypeValue`, adds the new
  // value to the memtable out-of-place.
  //
  // Returns `Status::NotFound` if `key` does not exist in current memtable or
  // the latest version of `key` does not have `kTypeValue`.
  //
  // Returns `Status::TryAgain` if the `seq`, `key` combination already exists
  // in the memtable and `MemTableRepFactory::CanHandleDuplicatedKey()` is true.
  // The next attempt should try a larger value for `seq`.
  //
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable.
  Status UpdateCallback(SequenceNumber seq, const Slice& key,
                        const Slice& delta,
                        const ProtectionInfoKVOS64* kv_prot_info);

  // Returns the number of successive merge entries starting from the newest
  // entry for the key. The count ends when the oldest entry in the memtable
  // with which the newest entry would be merged is reached, or the count
  // reaches `limit`.
  size_t CountSuccessiveMergeEntries(const LookupKey& key, size_t limit);

  // Update counters and flush status after inserting a whole write batch
  // Used in concurrent memtable inserts.
  void BatchPostProcess(const MemTablePostProcessInfo& update_counters) {
    table_->BatchPostProcess();
    const uint64_t old_num_entries =
        num_entries_.FetchAddRelaxed(update_counters.num_entries);
    if (old_num_entries == 0 && update_counters.num_entries != 0) {
      mem_tracker_.ActivateFlushInitiator();
    }
    data_size_.FetchAddRelaxed(update_counters.data_size);
    if (update_counters.num_deletes != 0) {
      num_deletes_.FetchAddRelaxed(update_counters.num_deletes);
    }
    if (update_counters.num_range_deletes > 0) {
      num_range_deletes_.FetchAddRelaxed(update_counters.num_range_deletes);
      // noop for skip-list memtable
      // Besides correctness test in stress test, memtable flush record count
      // check will catch this if it were not noop.
      // range_del_table_->BatchPostProcess();
    }
    UpdateFlushState();
  }

  uint64_t NumEntries() const override { return num_entries_.LoadRelaxed(); }

  uint64_t NumDeletion() const override { return num_deletes_.LoadRelaxed(); }

  uint64_t NumRangeDeletion() const override {
    return num_range_deletes_.LoadRelaxed();
  }

  uint64_t GetDataSize() const override { return data_size_.LoadRelaxed(); }

  size_t write_buffer_size() const { return write_buffer_size_.LoadRelaxed(); }

  // Dynamically change the memtable's capacity. If set below the current usage,
  // the next key added will trigger a flush. Can only increase size when
  // memtable prefix bloom is disabled, since we can't easily allocate more
  // space. Non-atomic update ok because this is only called with DB mutex held.
  void UpdateWriteBufferSize(size_t new_write_buffer_size) {
    if (bloom_filter_ == nullptr ||
        new_write_buffer_size < write_buffer_size_.LoadRelaxed()) {
      write_buffer_size_.StoreRelaxed(new_write_buffer_size);
    }
  }

  bool IsEmpty() const override { return first_seqno_ == 0; }

  SequenceNumber GetFirstSequenceNumber() override {
    return first_seqno_.load(std::memory_order_relaxed);
  }

  // Returns the sequence number of the first element that was inserted
  // into the memtable.
  // REQUIRES: external synchronization to prevent simultaneous
  // operations on the same MemTable (unless this Memtable is immutable).
  void SetFirstSequenceNumber(SequenceNumber first_seqno) {
    return first_seqno_.store(first_seqno, std::memory_order_relaxed);
  }

  SequenceNumber GetEarliestSequenceNumber() override {
    // With file ingestion and empty memtable, this seqno needs to be fixed.
    return earliest_seqno_.load(std::memory_order_relaxed);
  }

  // Sets the sequence number that is guaranteed to be smaller than or equal
  // to the sequence number of any key that could be inserted into this
  // memtable. It can then be assumed that any write with a larger(or equal)
  // sequence number will be present in this memtable or a later memtable.
  // Used only for MemPurge operation
  void SetEarliestSequenceNumber(SequenceNumber earliest_seqno) {
    return earliest_seqno_.store(earliest_seqno, std::memory_order_relaxed);
  }

  // DB's latest sequence ID when the memtable is created. This number
  // may be updated to a more recent one before any key is inserted.
  SequenceNumber GetCreationSeq() const { return creation_seq_; }

  void SetCreationSeq(SequenceNumber sn) { creation_seq_ = sn; }

  // If this memtable contains data from a committed two phase transaction we
  // must take note of the log which contains that data so we can know when
  // to release that log.
  void RefLogContainingPrepSection(uint64_t log);
  uint64_t GetMinLogContainingPrepSection() override;

  void MarkImmutable() override {
    WriteLock wl(&immutable_mutex_);
    is_immutable_.StoreRelaxed(true);
    table_->MarkReadOnly();
    mem_tracker_.DoneAllocating();
  }

  void MarkFlushed() override { table_->MarkFlushed(); }

  // return true if the current MemTableRep supports merge operator.
  bool IsMergeOperatorSupported() const {
    return table_->IsMergeOperatorSupported();
  }

  // return true if the current MemTableRep supports snapshots.
  // inplace update prevents snapshots,
  bool IsSnapshotSupported() const {
    return table_->IsSnapshotSupported() && !moptions_.inplace_update_support;
  }

  MemTableStats ApproximateStats(const Slice& start_ikey,
                                 const Slice& end_ikey) override;

  // Get the lock associated for the key
  port::RWMutex* GetLock(const Slice& key);

  const InternalKeyComparator& GetInternalKeyComparator() const override {
    return comparator_.comparator;
  }

  const ImmutableMemTableOptions* GetImmutableMemTableOptions() const {
    return &moptions_;
  }

  uint64_t ApproximateOldestKeyTime() const override {
    return oldest_key_time_.load(std::memory_order_relaxed);
  }

  // Returns a heuristic flush decision
  bool ShouldFlushNow();

  // Returns the reason for a flush scheduled because this memtable decided it
  // should stop accepting more writes.
  FlushReason GetFlushReason() const;

  // Updates `fragmented_range_tombstone_list_` that will be used to serve reads
  // when this memtable becomes an immutable memtable (in some
  // MemtableListVersion::memlist_). Should be called when this memtable is
  // about to become immutable. May be called multiple times since
  // SwitchMemtable() may fail.
  void ConstructFragmentedRangeTombstones();

  bool AddLogicallyRedundantRangeTombstone(
      SequenceNumber seq, const Slice& start_key, const Slice& end_key,
      uint64_t version_number, port::RWMutex& ingest_sst_lock) override;

  // Monotonically raises ingest_seqno_barrier_ to `y` (no-op if `y` is not
  // greater than the current value). The conversion's barrier check
  // (`seq < ingest_seqno_barrier_.LoadRelaxed()`) refuses converted
  // range tombstones that would shadow a just-installed L0 file.
  //
  // REQUIRES: DB mutex held by the caller. The DB mutex serializes all
  // callers, so the load-then-store pattern is race-free without needing
  // a CAS loop. Only IngestExternalFiles calls this.
  void BumpIngestSeqnoBarrier(SequenceNumber y);

  // Monotonically raises ingest_version_barrier_ to `version_number`, the
  // number of the Version that installed an ingestion.
  //
  // REQUIRES: DB mutex held by the caller. Only IngestExternalFiles calls
  // this.
  void BumpIngestVersionBarrier(uint64_t version_number);

  bool IsFragmentedRangeTombstonesConstructed() const override {
    return fragmented_range_tombstone_list_.get() != nullptr ||
           is_range_del_table_empty_.LoadRelaxed();
  }

  //  Gets the newest user defined timestamps in the memtable. This should only
  //  be called when user defined timestamp is enabled.
  Slice GetNewestUDT() const override;

  // Returns Corruption status if verification fails.
  static Status VerifyEntryChecksum(const char* entry,
                                    uint32_t protection_bytes_per_key,
                                    bool allow_data_in_errors = false);

  // Validate the checksum of the key/value pair.
  Status ValidateKey(const char* key, bool allow_data_in_errors);

 private:
  enum FlushStateEnum { FLUSH_NOT_REQUESTED, FLUSH_REQUESTED, FLUSH_SCHEDULED };

  friend class MemTableIterator;
  friend class MemTableBackwardIterator;
  friend class MemTableList;

  KeyComparator comparator_;
  const ImmutableMemTableOptions moptions_;
  const size_t kArenaBlockSize;
  AllocTracker mem_tracker_;
  ConcurrentArena arena_;
  std::unique_ptr<MemTableRep> table_;
  std::unique_ptr<MemTableRep> range_del_table_;
  // This is OK to be relaxed access because consistency between table_ and
  // range_del_table_ is provided by explicit multi-versioning with sequence
  // numbers. It's ok for stale memory to say the range_del_table_ is empty when
  // it's actually not because if it was relevant to our read (based on sequence
  // number), the relaxed memory read would get a sufficiently updated value
  // because of the ordering provided by LastPublishedSequence().
  RelaxedAtomic<bool> is_range_del_table_empty_;

  // Set to true by MarkImmutable(). Used as a "fast-path" to avoid acquiring
  // immutable_mutex_.
  RelaxedAtomic<bool> is_immutable_{false};
  port::RWMutex immutable_mutex_;

  // Total data size of all data inserted
  RelaxedAtomic<uint64_t> data_size_;
  RelaxedAtomic<uint64_t> num_entries_;
  RelaxedAtomic<uint64_t> num_deletes_;
  RelaxedAtomic<uint64_t> num_range_deletes_;

  // Dynamically changeable memtable option
  RelaxedAtomic<size_t> write_buffer_size_;

  // The sequence number of the kv that was inserted first
  std::atomic<SequenceNumber> first_seqno_;

  // The db sequence number at the time of creation or kMaxSequenceNumber
  // if not set.
  std::atomic<SequenceNumber> earliest_seqno_;

  // Seqno of the latest ingested external SST. See also
  // ColumnFamilyData::ingest_sst_lock_.
  RelaxedAtomic<SequenceNumber> ingest_seqno_barrier_{0};

  // Number of the latest Version that installed an ingestion. See also
  // ColumnFamilyData::ingest_sst_lock_.
  RelaxedAtomic<uint64_t> ingest_version_barrier_{0};

  SequenceNumber creation_seq_;

  // the earliest log containing a prepared section
  // which has been inserted into this memtable.
  std::atomic<uint64_t> min_prep_log_referenced_;

  // rw locks for inplace updates
  std::vector<port::RWMutex> locks_;

  const SliceTransform* const prefix_extractor_;
  std::unique_ptr<DynamicBloom> bloom_filter_;

  std::atomic<FlushStateEnum> flush_state_;

  SystemClock* clock_;

  // Extract sequential insert prefixes.
  const SliceTransform* insert_with_hint_prefix_extractor_;

  // Insert hints for each prefix.
  UnorderedMapH<Slice, void*, SliceHasher32> insert_hints_;

  // Timestamp of oldest key
  std::atomic<uint64_t> oldest_key_time_;

  // keep track of memory usage in table_, arena_, and range_del_table_.
  // Gets refreshed inside `ApproximateMemoryUsage()` or `ShouldFlushNow`
  RelaxedAtomic<uint64_t> approximate_memory_usage_;

  // max range deletions in a memtable,  before automatic flushing, 0 for
  // unlimited.
  uint32_t memtable_max_range_deletions_ = 0;

  // Size in bytes for the user-defined timestamps.
  size_t ts_sz_;

  // Pointer to the newest user-defined timestamp data in this MemTable. The
  // pointed-to memory lives in the arena and remains valid for the lifetime of
  // the memtable. Stored as an atomic pointer so that concurrent range
  // tombstone inserts from the read path can safely update it via CAS.
  Atomic<const char*> newest_udt_data_{nullptr};

  // Updates flush_state_ using ShouldFlushNow()
  void UpdateFlushState();

  void UpdateOldestKeyTime();

  void GetFromTable(const LookupKey& key,
                    SequenceNumber max_covering_tombstone_seq, bool do_merge,
                    ReadCallback* callback, bool* is_blob_index,
                    std::string* value, PinnableWideColumns* columns,
                    std::string* timestamp, Status* s,
                    MergeContext* merge_context, SequenceNumber* seq,
                    bool* found_final_value, bool* merge_in_progress,
                    const BlobFetcher* blob_fetcher,
                    bool* newer_version_present = nullptr);

  // Always returns non-null and assumes certain pre-checks (e.g.,
  // is_range_del_table_empty_) are done. This is only valid during the lifetime
  // of the underlying memtable.
  // read_seq and read_options.timestamp will be used as the upper bound
  // for range tombstones.
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIteratorInternal(
      const ReadOptions& read_options, SequenceNumber read_seq,
      bool immutable_memtable);

  // The fragmented range tombstones of this memtable.
  // This is constructed when this memtable becomes immutable
  // if !is_range_del_table_empty_.
  std::unique_ptr<FragmentedRangeTombstoneList>
      fragmented_range_tombstone_list_;

  // The fragmented range tombstone of this memtable with all keys' user-defined
  // timestamps logically stripped. This is constructed and used by flush when
  // user-defined timestamps in memtable only feature is enabled.
  std::unique_ptr<FragmentedRangeTombstoneList>
      timestamp_stripping_fragmented_range_tombstone_list_;

  // makes sure there is a single range tombstone writer to invalidate cache
  std::mutex range_del_mutex_;
  CoreLocalArray<std::shared_ptr<FragmentedRangeTombstoneListCache>>
      cached_range_tombstone_;
  void UpdateEntryChecksum(const ProtectionInfoKVOS64* kv_prot_info,
                           const Slice& key, const Slice& value, ValueType type,
                           SequenceNumber s, char* checksum_ptr);

  void MaybeUpdateNewestUDT(const Slice& user_key);

  const std::function<Status(const char*, bool)> key_validation_callback_;
};

const char* EncodeKey(std::string* scratch, const Slice& target);

}  // namespace ROCKSDB_NAMESPACE
