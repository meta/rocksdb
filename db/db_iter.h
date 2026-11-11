//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl.h"
#include "db/wide/read_path_blob_resolver.h"
#include "db/wide/wide_columns_helper.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/wide_columns.h"
#include "table/iterator_wrapper.h"
#include "util/autovector.h"
#include "util/dirty_tracked.h"

namespace ROCKSDB_NAMESPACE {
class BlobFileCache;
class Version;
namespace port {
class RWMutex;
}

// This file declares the factory functions of DBIter, in its original form
// or a wrapped form with class ArenaWrappedDBIter, which is defined here.
// Class DBIter, which is declared and implemented inside db_iter.cc, is
// an iterator that converts internal keys (yielded by an InternalIterator)
// that were live at the specified sequence number into appropriate user
// keys.
// Each internal key consists of a user key, a sequence number, and a value
// type. DBIter deals with multiple key versions, tombstones, merge operands,
// etc, and exposes an Iterator.
// For example, DBIter may wrap following InternalIterator:
//    user key: AAA  value: v3   seqno: 100    type: Put
//    user key: AAA  value: v2   seqno: 97     type: Put
//    user key: AAA  value: v1   seqno: 95     type: Put
//    user key: BBB  value: v1   seqno: 90     type: Put
//    user key: BBC  value: N/A  seqno: 98     type: Delete
//    user key: BBC  value: v1   seqno: 95     type: Put
// If the snapshot passed in is 102, then the DBIter is expected to
// expose the following iterator:
//    key: AAA  value: v3
//    key: BBB  value: v1
// If the snapshot passed in is 96, then it should expose:
//    key: AAA  value: v1
//    key: BBB  value: v1
//    key: BBC  value: v1
//

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter final : public Iterator {
 public:
  // Return a new DBIter that reads from `internal_iter` at the specified
  // `sequence` number.
  //
  // @param active_mem Pointer to the active memtable that `internal_iter`
  // is reading from. If not null, the memtable can be marked for flush
  // according to options mutable_cf_options.memtable_op_scan_flush_trigger
  // and mutable_cf_options.memtable_avg_op_scan_flush_trigger.
  // @param arena_mode If true, the DBIter will be allocated from the arena.
  static DBIter* NewIter(Env* env, const ReadOptions& read_options,
                         const ImmutableOptions& ioptions,
                         const MutableCFOptions& mutable_cf_options,
                         const Comparator* user_key_comparator,
                         InternalIterator* internal_iter,
                         const Version* version, const SequenceNumber& sequence,
                         ReadCallback* read_callback,
                         ReadOnlyMemTable* active_mem,
                         ColumnFamilyHandleImpl* cfh = nullptr,
                         bool expose_blob_index = false, Arena* arena = nullptr,
                         DBImpl* db_impl = nullptr,
                         ColumnFamilyData* cfd = nullptr) {
    if (cfh != nullptr) {
      db_impl = cfh->db();
      cfd = cfh->cfd();
    }
    void* mem = arena ? arena->AllocateAligned(sizeof(DBIter))
                      : operator new(sizeof(DBIter));
    DBIter* db_iter = new (mem)
        DBIter(env, read_options, ioptions, mutable_cf_options,
               user_key_comparator, internal_iter, version, sequence, arena,
               read_callback, db_impl, cfd, expose_blob_index, active_mem);
    return db_iter;
  }

  // The following is grossly complicated. TODO: clean it up
  // Which direction is the iterator currently moving?
  // (1) When moving forward:
  //   (1a) if current_entry_is_merged_ = false, the internal iterator is
  //        positioned at the exact entry that yields this->key(), this->value()
  //   (1b) if current_entry_is_merged_ = true, the internal iterator is
  //        positioned immediately after the last entry that contributed to the
  //        current this->value(). That entry may or may not have key equal to
  //        this->key().
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction : uint8_t { kForward, kReverse };

  // LocalStatistics contain Statistics counters that will be aggregated per
  // each iterator instance and then will be sent to the global statistics when
  // the iterator is destroyed.
  //
  // The purpose of this approach is to avoid perf regression happening
  // when multiple threads bump the atomic counters from a DBIter::Next().
  struct LocalStatistics {
    explicit LocalStatistics() { ResetCounters(); }

    void ResetCounters() {
      next_count_ = 0;
      next_found_count_ = 0;
      prev_count_ = 0;
      prev_found_count_ = 0;
      bytes_read_ = 0;
      skip_count_ = 0;
    }

    void BumpGlobalStatistics(Statistics* global_statistics) {
      RecordTick(global_statistics, NUMBER_DB_NEXT, next_count_);
      RecordTick(global_statistics, NUMBER_DB_NEXT_FOUND, next_found_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV, prev_count_);
      RecordTick(global_statistics, NUMBER_DB_PREV_FOUND, prev_found_count_);
      RecordTick(global_statistics, ITER_BYTES_READ, bytes_read_);
      RecordTick(global_statistics, NUMBER_ITER_SKIP, skip_count_);
      PERF_COUNTER_ADD(iter_read_bytes, bytes_read_);
      ResetCounters();
    }

    // Map to Tickers::NUMBER_DB_NEXT
    uint64_t next_count_;
    // Map to Tickers::NUMBER_DB_NEXT_FOUND
    uint64_t next_found_count_;
    // Map to Tickers::NUMBER_DB_PREV
    uint64_t prev_count_;
    // Map to Tickers::NUMBER_DB_PREV_FOUND
    uint64_t prev_found_count_;
    // Map to Tickers::ITER_BYTES_READ
    uint64_t bytes_read_;
    // Map to Tickers::NUMBER_ITER_SKIP
    uint64_t skip_count_;
  };

  // No copying allowed
  DBIter(const DBIter&) = delete;
  void operator=(const DBIter&) = delete;
  DBIter(DBIter&&) = delete;
  void operator=(DBIter&&) = delete;

  ~DBIter() override {
    MarkMemtableForFlushForAvgTrigger();
    ThreadStatus::OperationType cur_op_type =
        ThreadStatusUtil::GetThreadOperation();
    ThreadStatusUtil::SetThreadOperation(
        ThreadStatus::OperationType::OP_UNKNOWN);
    // Release pinned data if any
    if (pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
    RecordTick(statistics_, NO_ITERATOR_DELETED);
    ResetInternalKeysSkippedCounter();
    local_stats_.BumpGlobalStatistics(statistics_);
    iter_.DeleteIter(arena_mode_);
    ThreadStatusUtil::SetThreadOperation(cur_op_type);
  }
  void SetIter(InternalIterator* iter) {
    assert(iter_.iter() == nullptr);
    iter_.Set(iter);
    iter_.iter()->SetPinnedItersMgr(&pinned_iters_mgr_);
  }

  bool Valid() const override {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    if (valid_) {
      status_.PermitUncheckedError();
    }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
    return valid_;
  }
  Slice key() const override {
    assert(valid_);
    if (timestamp_lb_) {
      return saved_key_.GetInternalKey();
    } else {
      const Slice ukey_and_ts = saved_key_.GetUserKey();
      return Slice(ukey_and_ts.data(), ukey_and_ts.size() - timestamp_size_);
    }
  }
  Slice value() const override {
    assert(valid_);

    return value_columns_state_->value();
  }

  const WideColumns& columns() const override {
    assert(valid_);
    assert(!value_columns_state_->HasLazyEntityColumns() ||
           value_columns_state_->HasMaterializedColumns());
    return value_columns_state_->wide_columns();
  }

  Status status() const override {
    if (status_.ok() && iter_.iter() != nullptr) {
      return iter_.status();
    } else {
      assert(!valid_);
      return status_;
    }
  }
  Slice timestamp() const override {
    assert(valid_);
    assert(timestamp_size_ > 0);
    if (direction_ == kReverse) {
      return saved_timestamp_;
    }
    const Slice ukey_and_ts = saved_key_.GetUserKey();
    assert(timestamp_size_ < ukey_and_ts.size());
    return ExtractTimestampFromUserKey(ukey_and_ts, timestamp_size_);
  }
  bool IsBlob() const {
    assert(valid_);
    return blob_state_->is_blob;
  }

  Status GetProperty(std::string prop_name, std::string* prop) override;

  void Next() final;
  void Prev() final;
  // 'target' does not contain timestamp, even if user timestamp feature is
  // enabled.
  void Seek(const Slice& target) final;
  void SeekForPrev(const Slice& target) final;
  void SeekToFirst() final;
  void SeekToLast() final;
  Env* env() const { return env_; }
  void set_sequence(uint64_t s) {
    sequence_ = s;
    if (read_callback_) {
      read_callback_->Refresh(s);
    }
    iter_.SetRangeDelReadSeqno(s);
  }
  void set_valid(bool v) { valid_ = v; }
  void set_status(Status s) { status_ = std::move(s); }

  bool PrepareValue() override;

  void Prepare(const MultiScanArgs& scan_opts) override;
  Status ValidateScanOptions(const MultiScanArgs& multiscan_opts) const;
  Status SetScanOptionsForPrepare(const MultiScanArgs& scan_opts);
  void PrepareInternalChildren();

 private:
  DBIter(Env* _env, const ReadOptions& read_options,
         const ImmutableOptions& ioptions,
         const MutableCFOptions& mutable_cf_options, const Comparator* cmp,
         InternalIterator* iter, const Version* version, SequenceNumber s,
         bool arena_mode, ReadCallback* read_callback, DBImpl* db_impl,
         ColumnFamilyData* cfd, bool expose_blob_index,
         ReadOnlyMemTable* active_mem);

  class BlobReader {
   public:
    BlobReader(const Version* version, const ReadOptions& read_options,
               BlobFileCache* blob_file_cache, bool allow_write_path_fallback)
        : blob_fetcher_(version, ReadOptions(read_options), blob_file_cache,
                        allow_write_path_fallback) {}

    const Slice& GetBlobValue() const { return blob_value_; }
    Status RetrieveAndSetBlobValue(const Slice& user_key,
                                   const Slice& blob_index);
    void ResetBlobValue() { blob_value_.Reset(); }
    // The blob fetcher backing this reader, for resolving wide-column entity
    // blob references (the merge path). Valid for this BlobReader's lifetime.
    const BlobFetcher& blob_fetcher() const { return blob_fetcher_; }

   private:
    PinnableSlice blob_value_;
    OwningVersionBlobFetcher blob_fetcher_;
  };
  struct BlobState {
    BlobReader reader;
    Slice lazy_blob_index;
    bool is_blob = false;

    template <typename... Args>
    explicit BlobState(Args&&... args) : reader(std::forward<Args>(args)...) {}

    void Reset() {
      reader.ResetBlobValue();
      lazy_blob_index.clear();
      is_blob = false;
    }
  };

  // Groups the current iterator result together with the backing storage and
  // lazy entity-resolution metadata it depends on. Resetting this object drops
  // all aliases into the saved entity buffer and resolver cache at once.
  class ValueColumnsState {
   public:
    ValueColumnsState(const Version* version, const ReadOptions& read_options,
                      ColumnFamilyData* cfd)
        : entity_blob_resolver_(
              version, read_options, cfd ? cfd->blob_file_cache() : nullptr,
              cfd != nullptr && cfd->blob_partition_manager() != nullptr) {}

    Slice& value() { return value_; }
    const Slice& value() const { return value_; }
    WideColumns& wide_columns() { return wide_columns_; }
    const WideColumns& wide_columns() const { return wide_columns_; }
    bool HasMaterializedColumns() const { return !wide_columns_.empty(); }
    bool HasLazyEntityColumns() const { return !lazy_entity_columns_.empty(); }

    std::string& saved_value() { return saved_value_; }
    const std::string& saved_value() const { return saved_value_; }

    std::vector<WideColumn>& lazy_entity_columns() {
      return lazy_entity_columns_;
    }
    const std::vector<WideColumn>& lazy_entity_columns() const {
      return lazy_entity_columns_;
    }

    std::vector<std::pair<size_t, BlobIndex>>& lazy_blob_columns() {
      return lazy_blob_columns_;
    }
    const std::vector<std::pair<size_t, BlobIndex>>& lazy_blob_columns() const {
      return lazy_blob_columns_;
    }

    ReadPathBlobResolver& entity_blob_resolver() {
      return entity_blob_resolver_;
    }
    const ReadPathBlobResolver& entity_blob_resolver() const {
      return entity_blob_resolver_;
    }

    std::mutex& lazy_entity_columns_mutex() const {
      return lazy_entity_columns_mutex_;
    }

    // DBIter calls this after ResetValueAndColumns() before repopulating the
    // current entry from a serialized wide-column entity.
    void AssertReadyForEntity() const {
      assert(value_.empty());
      assert(wide_columns_.empty());
      assert(lazy_entity_columns_.empty());
      assert(lazy_blob_columns_.empty());
    }

    inline void ClearSavedValue() {
      if (saved_value_.capacity() > 1048576) {
        std::string empty;
        swap(empty, saved_value_);
      } else {
        saved_value_.clear();
      }
    }

    // Preserve the serialized entity bytes when DBIter needs stable backing
    // storage for lazy column slices across iterator movement.
    void SaveEntitySliceIfNeeded(const Slice& slice) {
      if (slice.data() != saved_value_.data() ||
          slice.size() != saved_value_.size()) {
        saved_value_.assign(slice.data(), slice.size());
      }
    }

    // Clears the previous lazy entity metadata and returns the saved entity
    // buffer as input for Deserialize().
    Slice PrepareForLazyEntityDeserialize() {
      ClearLazyEntity();
      return Slice(saved_value_);
    }

    // Publishes the deserialized lazy entity metadata to the blob resolver.
    void BindLazyEntity(const Slice& user_key) {
      entity_blob_resolver_.Reset(user_key, &lazy_entity_columns_,
                                  &lazy_blob_columns_);
    }

    // Drops the lazy entity metadata and any resolver aliases into it.
    void ClearLazyEntity() {
      lazy_entity_columns_.clear();
      lazy_blob_columns_.clear();
      entity_blob_resolver_.Reset(Slice(), nullptr, nullptr);
    }

    // Clears materialized wide columns on error before DBIter invalidates
    // itself.
    void ClearWideColumns() { wide_columns_.clear(); }

    // Fast path for inline entities whose default column is already
    // materialized in wide_columns_.
    void MaybeSetValueFromMaterializedDefaultColumn() {
      if (WideColumnsHelper::HasDefaultColumn(wide_columns_)) {
        value_ = WideColumnsHelper::GetDefaultColumn(wide_columns_);
      }
    }

    void SetFromPlain(const Slice& slice) {
      assert(value_.empty());
      assert(wide_columns_.empty());
      assert(lazy_entity_columns_.empty());
      assert(lazy_blob_columns_.empty());

      value_ = slice;
      wide_columns_.emplace_back(kDefaultWideColumnName, slice);
    }

    void Reset() {
      value_.clear();
      wide_columns_.clear();
      ClearLazyEntity();
    }

   private:
    std::string saved_value_;
    // Value of the default column.
    Slice value_;
    // All columns (i.e. name-value pairs).
    WideColumns wide_columns_;
    // Lazy resolution state for V2 entities with blob columns.
    ReadPathBlobResolver entity_blob_resolver_;
    std::vector<WideColumn> lazy_entity_columns_;
    std::vector<std::pair<size_t, BlobIndex>> lazy_blob_columns_;
    mutable std::mutex lazy_entity_columns_mutex_;
  };

  // For all methods in this block:
  // PRE: iter_->Valid() && status_.ok()
  // Return false if there was an error, and status() is non-ok, valid_ = false;
  // in this case callers would usually stop what they were doing and return.
  bool ReverseToForward();
  bool ReverseToBackward();
  // Set saved_key_ to the seek key to target, with proper sequence number set.
  // It might get adjusted if the seek key is smaller than iterator lower bound.
  // target does not have timestamp.
  void SetSavedKeyToSeekTarget(const Slice& target);
  // Set saved_key_ to the seek key to target, with proper sequence number set.
  // It might get adjusted if the seek key is larger than iterator upper bound.
  // target does not have timestamp.
  void SetSavedKeyToSeekForPrevTarget(const Slice& target);
  bool FindValueForCurrentKey(bool& found_visible);
  bool FindValueForCurrentKeyUsingSeek();
  bool FindUserKeyBeforeSavedKey();
  // If `skipping_saved_key` is true, the function will keep iterating until it
  // finds a user key that is larger than `saved_key_`.
  // When prefix_ is set, the iterator stops when all keys for the prefix are
  // exhausted and the iterator is set to invalid.
  bool FindNextUserEntry(bool skipping_saved_key);
  // Internal implementation of FindNextUserEntry().
  bool FindNextUserEntryInternal(bool skipping_saved_key);
  bool ParseKey(ParsedInternalKey* key);
  bool MergeValuesNewToOld();

  // When prefix_ is set, we need to set the iterator to invalid if no more
  // entry can be found within the prefix.
  void PrevInternal();
  bool TooManyInternalKeysSkipped(bool increment = true);
  bool IsVisible(SequenceNumber sequence, const Slice& ts,
                 bool* more_recent = nullptr);

  // Temporarily pin the blocks that we encounter until ReleaseTempPinnedData()
  // is called
  void TempPinData() {
    if (!pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
  }

  // Release blocks pinned by TempPinData()
  void ReleaseTempPinnedData() {
    if (!pin_thru_lifetime_ && pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
  }

  inline void ClearSavedValue() {
    value_columns_state_.mut()->ClearSavedValue();
  }

  inline void ResetInternalKeysSkippedCounter() {
    local_stats_.skip_count_ += num_internal_keys_skipped_;
    if (valid_) {
      local_stats_.skip_count_--;
    }
    num_internal_keys_skipped_ = 0;
  }

  bool expect_total_order_inner_iter() {
    assert(expect_total_order_inner_iter_ || prefix_extractor_ != nullptr);
    return expect_total_order_inner_iter_;
  }

  // If lower bound of timestamp is given by ReadOptions.iter_start_ts, we need
  // to return versions of the same key. We cannot just skip if the key value
  // is the same but timestamps are different but fall in timestamp range.
  inline int CompareKeyForSkip(const Slice& a, const Slice& b) {
    return timestamp_lb_ != nullptr
               ? user_comparator_.Compare(a, b)
               : user_comparator_.CompareWithoutTimestamp(a, b);
  }

  void SetValueAndColumnsFromPlain(const Slice& slice) {
    value_columns_state_.mut()->SetFromPlain(slice);
  }

  bool SetValueAndColumnsFromBlobImpl(const Slice& user_key,
                                      const Slice& blob_index);
  bool SetValueAndColumnsFromBlob(const Slice& user_key,
                                  const Slice& blob_index);

  bool SetValueAndColumnsFromEntity(Slice slice);
  bool MaterializeLazyEntityColumns() const;

  bool SetValueAndColumnsFromMergeResult(const Status& merge_status,
                                         ValueType result_type);

  void ResetValueAndColumns() { value_columns_state_.Reset(); }

  void ResetBlobData() { blob_state_.Reset(); }

  // The following methods perform the actual merge operation for the
  // no/plain/blob/wide-column base value cases.
  // If user-defined timestamp is enabled, `user_key` includes timestamp.
  bool MergeWithNoBaseValue(const Slice& user_key);
  bool MergeWithPlainBaseValue(const Slice& value, const Slice& user_key);
  // If `result_operand` is non-null, a merge result that is one of the
  // existing operands or the base value is returned by reference through it
  // (no copy); otherwise, such a result is copied into saved_value().
  bool MergeWithPlainBaseValueImpl(const Slice& value, const Slice& user_key,
                                   Slice* result_operand);
  bool MergeWithBlobBaseValue(const Slice& blob_index, const Slice& user_key);
  bool MergeWithWideColumnBaseValue(const Slice& entity, const Slice& user_key);

  bool PrepareValueInternal() {
    // Capture this before PrepareValue(): PrepareValue() updates the wrapper
    // state to "prepared" on success. We still call PrepareValue()
    // unconditionally to preserve its contract/error handling, but only need
    // to re-parse ikey_ when this call may have actually materialized the
    // underlying iterator value/key.
    const bool value_was_prepared = iter_.IsValuePrepared();
    if (!iter_.PrepareValue()) {
      assert(!iter_.status().ok());
      valid_ = false;
      return false;
    }
    // ikey_ could change as BlockBasedTableIterator does Block cache
    // lookup and index_iter_ could point to different block resulting
    // in ikey_ pointing to wrong key. So ikey_ needs to be updated in
    // case of Seek/Next calls to point to right key again.
    if (!value_was_prepared && !ParseKey(&ikey_)) {
      return false;
    }
    return true;
  }

  // Record a deletion into the current contiguous tombstone run.
  // In forward iteration, first_key is set only for the first tombstone
  // (always_update_first_key=false). In reverse, keys arrive in decreasing
  // order so first_key is updated every time (always_update_first_key=true).
  void TrackContiguousTombstone(const Slice& user_key,
                                bool always_update_first_key);

  // If a contiguous tombstone run is pending, insert a range tombstone
  // (if threshold is met) and reset tracking state.  When
  // check_prefix_match is true, the insertion is skipped (but tracking is
  // still reset) if end_key is outside the seek prefix.
  void FlushPendingTombstoneRun(const Slice& end_key,
                                bool check_prefix_match = false);

  // If enough contiguous tombstones have been tracked, insert a range
  // tombstone [first_key, end_key) into the mutable memtable.
  // end_key is the exclusive upper bound -- typically the next live key.
  void MaybeInsertRangeTombstone(const Slice& end_key);
  void ResetContiguousTombstoneTracking() {
    contiguous_tombstone_count_ = 0;
    range_tomb_first_key_.Clear();
    range_tomb_end_key_.Clear();
  }

  // Returns true if there is no prefix constraint (prefix_ not set) or
  // if `key` is in the prefix extractor's domain and its prefix matches.
  // Out-of-domain keys return false when a prefix is set.
  bool PrefixCheck(const Slice& key) const {
    return !prefix_.has_value() || (prefix_extractor_->InDomain(key) &&
                                    prefix_extractor_->Transform(key).compare(
                                        prefix_->GetUserKey()) == 0);
  }

  // Returns true if a prefix should be extracted from the seek target and
  // used for prefix boundary tracking. True when prefix_same_as_start is
  // set, or when range tombstone conversion is enabled during a legacy
  // prefix seek. If target is out of domain then false is returned.
  bool ShouldSetPrefix(const Slice& target) const {
    return (prefix_same_as_start_ ||
            (min_tombstones_for_range_conversion_ > 0 &&
             !expect_total_order_inner_iter_)) &&
           prefix_extractor_->InDomain(target);
  }

  void ResetSeekState() {
    ReleaseTempPinnedData();
    ResetBlobData();
    ResetValueAndColumns();
    ResetInternalKeysSkippedCounter();
    ResetContiguousTombstoneTracking();
    prefix_.reset();
  }

  void MarkMemtableForFlushForAvgTrigger() {
    if (avg_op_scan_flush_trigger_ &&
        mem_hidden_op_scanned_since_seek_ >= memtable_op_scan_flush_trigger_ &&
        mem_hidden_op_scanned_since_seek_ >=
            static_cast<uint64_t>(iter_step_since_seek_) *
                avg_op_scan_flush_trigger_) {
      assert(memtable_op_scan_flush_trigger_ > 0);
      active_mem_->MarkForFlush();
      avg_op_scan_flush_trigger_ = 0;
      memtable_op_scan_flush_trigger_ = 0;
    }
    iter_step_since_seek_ = 1;
    mem_hidden_op_scanned_since_seek_ = 0;
  }

  void MarkMemtableForFlushForPerOpTrigger(uint64_t& mem_hidden_op_scanned) {
    if (memtable_op_scan_flush_trigger_ &&
        ikey_.sequence >= memtable_seqno_lb_) {
      if (++mem_hidden_op_scanned >= memtable_op_scan_flush_trigger_) {
        active_mem_->MarkForFlush();
        // Turn off the flush trigger checks.
        memtable_op_scan_flush_trigger_ = 0;
        avg_op_scan_flush_trigger_ = 0;
      }
      if (avg_op_scan_flush_trigger_) {
        ++mem_hidden_op_scanned_since_seek_;
      }
    }
  }

  const SliceTransform* prefix_extractor_;
  Env* const env_;
  SystemClock* clock_;
  Logger* logger_;
  UserComparatorWrapper user_comparator_;
  const MergeOperator* const merge_operator_;
  IteratorWrapper iter_;
  // TODO: blob_state_'s BlobReader (whole-value blob reads + the merge entity
  // path) and value_columns_state_'s ReadPathBlobResolver (lazy per-column
  // entity resolution) each own a Version-backed blob fetcher with the same
  // {version, read_options, blob_file_cache, allow_write_path_fallback}, so an
  // iterator holds two ReadOptions copies. Collapse to a single shared fetcher.
  // Deferred because the resolver must keep owning its fetcher for the planned
  // lazy entity read path, where the result outlives the read call and owns the
  // SuperVersion pin; unifying is best done together with that work.
  DirtyTracked<BlobState> blob_state_;
  ReadCallback* read_callback_;
  // Max visible sequence number. It is normally the snapshot seq unless we have
  // uncommitted data in db as in WriteUnCommitted.
  SequenceNumber sequence_;

  IterKey saved_key_;
  // Reusable internal key data structure. This is only used inside one function
  // and should not be used across functions. Reusing this object can reduce
  // overhead of calling construction of the function if creating it each time.
  ParsedInternalKey ikey_;

  // The approximate write time for the entry. It is deduced from the entry's
  // sequence number if the seqno to time mapping is available. For a
  // kTypeValuePreferredSeqno entry, this is the write time specified by the
  // user.
  uint64_t saved_write_unix_time_;
  DirtyTracked<ValueColumnsState> value_columns_state_;
  Slice pinned_value_;
  Statistics* statistics_;
  uint64_t max_skip_;
  uint64_t max_skippable_internal_keys_;
  uint64_t num_internal_keys_skipped_;
  const Slice* iterate_lower_bound_;
  const Slice* iterate_upper_bound_;

  // The prefix of the seek key. Set during Seek/SeekForPrev when either
  // prefix_same_as_start_ is true or the iterator uses prefix filtering
  // (!expect_total_order_inner_iter_ && InDomain(target)). Used in Next()
  // and Prev() to:
  //  - invalidate the iterator when prefix_same_as_start_ is true and keys
  //    in this prefix have been exhausted.
  //  - bound range tombstone tracking to the seek prefix when
  //    min_tombstones_for_range_conversion_ > 0.
  // Set via SetUserKey(), read via GetUserKey().
  std::optional<IterKey> prefix_;

  Status status_;

  // List of operands for merge operator.
  MergeContext merge_context_;
  LocalStatistics local_stats_;
  PinnedIteratorsManager pinned_iters_mgr_;
  // The read options a traced seek records (Tracer::IteratorSeek()).
  TracedReadOptions trace_read_options_;
  DBImpl* trace_db_;
  uint32_t trace_cf_id_;
  bool has_trace_state_;
  port::RWMutex* ingest_sst_lock_;
  // Number of the Version this iterator reads, or 0 if unknown.
  const uint64_t version_number_;
  const Slice* const timestamp_ub_;
  const Slice* const timestamp_lb_;
  const size_t timestamp_size_;
  std::string saved_timestamp_;
  std::optional<MultiScanArgs> scan_opts_;
  size_t scan_index_{0};
  ReadOnlyMemTable* const active_mem_;
  SequenceNumber memtable_seqno_lb_;
  uint32_t memtable_op_scan_flush_trigger_;
  uint32_t avg_op_scan_flush_trigger_;
  uint32_t iter_step_since_seek_;
  uint32_t mem_hidden_op_scanned_since_seek_;
  uint32_t contiguous_tombstone_count_;
  Direction direction_;
  bool valid_;
  bool current_entry_is_merged_;
  // True if we know that the current entry's seqnum is 0.
  // This information is used as that the next entry will be for another
  // user key.
  bool is_key_seqnum_zero_;
  const bool prefix_same_as_start_;
  // Means that we will pin all data blocks we read as long the Iterator
  // is not deleted, will be true if ReadOptions::pin_data is true
  const bool pin_thru_lifetime_;
  // Expect the inner iterator to maintain a total order.
  // prefix_extractor_ must be non-NULL if the value is false.
  const bool expect_total_order_inner_iter_;
  const uint32_t min_tombstones_for_range_conversion_;
  // Whether the iterator is allowed to expose blob references. Set to true when
  // the stacked BlobDB implementation is used, false otherwise.
  bool expose_blob_index_;
  bool allow_unprepared_value_;
  bool arena_mode_;

  IterKey range_tomb_first_key_;
  IterKey range_tomb_end_key_;
};
}  // namespace ROCKSDB_NAMESPACE
