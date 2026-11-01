//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <cinttypes>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "db/blob/blob_index.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_iteration_stats.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_del_aggregator.h"
#include "db/snapshot_checker.h"
#include "options/cf_options.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileBuilder;
class BlobFileCache;
class BlobFetcher;
class Version;
class PrefetchBufferCollection;
class FilePrefetchBuffer;
class Version;

// Internal implementation of WideColumnBlobResolver for compaction.
// This class enables lazy loading of blob column values during compaction
// filter processing.
class CompactionBlobResolver : public WideColumnBlobResolver {
 public:
  CompactionBlobResolver()
      : columns_(nullptr),
        blob_columns_(nullptr),
        blob_fetcher_(nullptr),
        prefetch_buffers_(nullptr),
        iter_stats_(nullptr) {}

  // Set the fixed context (blob fetcher, prefetch buffers, stats) once.
  void Init(BlobFetcher* blob_fetcher,
            PrefetchBufferCollection* prefetch_buffers,
            CompactionIterationStats* iter_stats);

  Status ResolveColumn(size_t column_index, Slice* resolved_value) override;
  bool IsBlobColumn(size_t column_index) const override;
  size_t NumColumns() const override;
  Status resolve_status() const {
    if (!resolve_error_.has_value()) {
      return Status::OK();
    }
    return *resolve_error_;
  }

  // Reset the resolver for a new entity. Clears resolved cache.
  void Reset(const Slice& user_key, const std::vector<WideColumn>* columns,
             const std::vector<std::pair<size_t, BlobIndex>>* blob_columns,
             bool track_resolve_error = false);

 private:
  Slice user_key_;
  const std::vector<WideColumn>* columns_;
  const std::vector<std::pair<size_t, BlobIndex>>* blob_columns_;
  BlobFetcher* blob_fetcher_;
  PrefetchBufferCollection* prefetch_buffers_;
  CompactionIterationStats* iter_stats_;
  bool track_resolve_error_ = false;

  // Cache for resolved blob values to avoid re-fetching. Uses a vector of
  // (column_index, PinnableSlice) pairs -- typical entities have few blob
  // columns (<5), making linear scan cheaper than hash map overhead.
  std::vector<std::pair<size_t, std::unique_ptr<PinnableSlice>>>
      resolved_cache_;
  // Sticky resolver error for the current entity. This is enabled only for the
  // lazy FilterV4 path so compaction can still fail even if the filter
  // notices the error and returns kKeep.
  std::optional<Status> resolve_error_;
};

// A wrapper of internal iterator whose purpose is to count how
// many entries there are in the iterator.
class SequenceIterWrapper : public InternalIterator {
 public:
  SequenceIterWrapper(InternalIterator* iter, const Comparator* cmp,
                      bool need_count_entries)
      : icmp_(cmp),
        inner_iter_(iter),
        need_count_entries_(need_count_entries) {}
  bool Valid() const override { return inner_iter_->Valid(); }
  Status status() const override { return inner_iter_->status(); }
  void Next() override {
    if (!inner_iter_->IsDeleteRangeSentinelKey()) {
      num_itered_++;
    }
    inner_iter_->Next();
  }
  void Seek(const Slice& target) override {
    if (!need_count_entries_) {
      has_num_itered_ = false;
      inner_iter_->Seek(target);
    } else {
      // Need to count total number of entries,
      // so we do Next() rather than Seek().
      while (inner_iter_->Valid() &&
             icmp_.Compare(inner_iter_->key(), target) < 0) {
        Next();
      }
    }
  }
  Slice key() const override { return inner_iter_->key(); }
  Slice value() const override { return inner_iter_->value(); }

  // Unused InternalIterator methods
  void SeekToFirst() override { assert(false); }
  void Prev() override { assert(false); }
  void SeekForPrev(const Slice& /* target */) override { assert(false); }
  void SeekToLast() override { assert(false); }

  uint64_t NumItered() const { return num_itered_; }
  bool HasNumItered() const { return has_num_itered_; }
  bool IsDeleteRangeSentinelKey() const override {
    assert(Valid());
    return inner_iter_->IsDeleteRangeSentinelKey();
  }

 private:
  InternalKeyComparator icmp_;
  InternalIterator* inner_iter_;  // not owned
  uint64_t num_itered_ = 0;
  bool need_count_entries_;
  bool has_num_itered_ = true;
};

class CompactionIterator {
 public:
  // A wrapper around Compaction. Has a much smaller interface, only what
  // CompactionIterator uses. Tests can override it.
  class CompactionProxy {
   public:
    virtual ~CompactionProxy() = default;

    virtual int level() const = 0;

    virtual bool KeyNotExistsBeyondOutputLevel(
        const Slice& user_key, std::vector<size_t>* level_ptrs) const = 0;

    virtual bool bottommost_level() const = 0;

    virtual int number_levels() const = 0;

    // Result includes timestamp if user-defined timestamp is enabled.
    virtual Slice GetLargestUserKey() const = 0;

    virtual bool allow_ingest_behind() const = 0;

    virtual bool allow_mmap_reads() const = 0;

    virtual bool enable_blob_garbage_collection() const = 0;

    virtual double blob_garbage_collection_age_cutoff() const = 0;

    virtual uint64_t blob_compaction_readahead_size() const = 0;

    virtual const Version* input_version() const = 0;

    virtual bool DoesInputReferenceBlobFiles() const = 0;

    virtual const Compaction* real_compaction() const = 0;

    virtual bool SupportsPerKeyPlacement() const = 0;
  };

  class RealCompaction : public CompactionProxy {
   public:
    explicit RealCompaction(const Compaction* compaction)
        : compaction_(compaction) {
      assert(compaction_);
    }

    int level() const override { return compaction_->level(); }

    bool KeyNotExistsBeyondOutputLevel(
        const Slice& user_key, std::vector<size_t>* level_ptrs) const override {
      return compaction_->KeyNotExistsBeyondOutputLevel(user_key, level_ptrs);
    }

    bool bottommost_level() const override {
      return compaction_->bottommost_level();
    }

    int number_levels() const override { return compaction_->number_levels(); }

    // Result includes timestamp if user-defined timestamp is enabled.
    Slice GetLargestUserKey() const override {
      return compaction_->GetLargestUserKey();
    }

    bool allow_ingest_behind() const override {
      return compaction_->immutable_options().cf_allow_ingest_behind ||
             compaction_->immutable_options().allow_ingest_behind;
    }

    bool allow_mmap_reads() const override {
      return compaction_->immutable_options().allow_mmap_reads;
    }

    bool enable_blob_garbage_collection() const override {
      return compaction_->enable_blob_garbage_collection();
    }

    double blob_garbage_collection_age_cutoff() const override {
      return compaction_->blob_garbage_collection_age_cutoff();
    }

    uint64_t blob_compaction_readahead_size() const override {
      return compaction_->mutable_cf_options().blob_compaction_readahead_size;
    }

    const Version* input_version() const override {
      return compaction_->input_version();
    }

    bool DoesInputReferenceBlobFiles() const override {
      return compaction_->DoesInputReferenceBlobFiles();
    }

    const Compaction* real_compaction() const override { return compaction_; }

    bool SupportsPerKeyPlacement() const override {
      return compaction_->SupportsPerKeyPlacement();
    }

   private:
    const Compaction* compaction_;
  };

  // @param must_count_input_entries Controls input entry counting accuracy vs
  // performance:
  //   - If true: `NumInputEntryScanned()` always returns the exact count of
  //   input keys
  //     scanned. The iterator will use sequential `Next()` calls instead of
  //     `Seek()` to maintain count accuracy as `Seek()` will not count the
  //     skipped input entries, which is slower but guarantees correctness.
  //   - If false: `NumInputEntryScanned()` returns the count only if no
  //   `Seek()` operations
  //     were performed on the input iterator. When compaction filters request
  //     skipping ranges of keys or other optimizations trigger seek operations,
  //     the count becomes unreliable. Always call `HasNumInputEntryScanned()`
  //     first to verify if the count is accurate before using
  //     `NumInputEntryScanned()`.
  CompactionIterator(
      InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
      SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
      SequenceNumber earliest_snapshot,
      SequenceNumber earliest_write_conflict_snapshot,
      SequenceNumber job_snapshot, const SnapshotChecker* snapshot_checker,
      Env* env, bool report_detailed_time,
      CompactionRangeDelAggregator* range_del_agg,
      BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
      bool enforce_single_del_contracts,
      const std::atomic<bool>& manual_compaction_canceled,
      bool must_count_input_entries, const Compaction* compaction = nullptr,
      const CompactionFilter* compaction_filter = nullptr,
      const std::atomic<bool>* shutting_down = nullptr,
      const std::shared_ptr<Logger> info_log = nullptr,
      const std::string* full_history_ts_low = nullptr,
      std::optional<SequenceNumber> preserve_seqno_min = {},
      const Version* input_version = nullptr,
      Env::IOActivity blob_read_io_activity = Env::IOActivity::kCompaction,
      std::optional<uint64_t> blob_gc_cutoff_file_number = {});

  // Constructor with custom CompactionProxy, used for tests.
  CompactionIterator(
      InternalIterator* input, const Comparator* cmp, MergeHelper* merge_helper,
      SequenceNumber last_sequence, std::vector<SequenceNumber>* snapshots,
      SequenceNumber earliest_snapshot,
      SequenceNumber earliest_write_conflict_snapshot,
      SequenceNumber job_snapshot, const SnapshotChecker* snapshot_checker,
      Env* env, bool report_detailed_time,
      CompactionRangeDelAggregator* range_del_agg,
      BlobFileBuilder* blob_file_builder, bool allow_data_in_errors,
      bool enforce_single_del_contracts,
      const std::atomic<bool>& manual_compaction_canceled,
      std::unique_ptr<CompactionProxy> compaction,
      bool must_count_input_entries,
      const CompactionFilter* compaction_filter = nullptr,
      const std::atomic<bool>* shutting_down = nullptr,
      const std::shared_ptr<Logger> info_log = nullptr,
      const std::string* full_history_ts_low = nullptr,
      std::optional<SequenceNumber> preserve_seqno_min = {},
      const Version* input_version = nullptr,
      Env::IOActivity blob_read_io_activity = Env::IOActivity::kCompaction,
      std::optional<uint64_t> blob_gc_cutoff_file_number = {});

  ~CompactionIterator();

  void ResetRecordCounts();

  // Non-compaction table-file creation (e.g., flush/recovery) can still need
  // blob resolution when direct-write wide entities already contain blob
  // references. Plumb a fetcher explicitly in those cases.
  void SetBlobFetcher(const Version* version, BlobFileCache* blob_file_cache,
                      Env::IOActivity io_activity,
                      bool allow_write_path_fallback);

  static uint64_t ComputeBlobGarbageCollectionCutoffFileNumber(
      const CompactionProxy* compaction);

  // Seek to the beginning of the compaction iterator output.
  //
  // REQUIRED: Call only once.
  void SeekToFirst();

  // Positions at the first output without extracting/relocating blobs yet.
  // CompactionJob uses this when an SST checkpoint may need to be persisted
  // before the current key is added to a blob file.
  void SeekToFirstWithoutPreparingOutput();

  // Produces the next record in the compaction.
  //
  // REQUIRED: SeekToFirst() has been called.
  void Next();

  // Advances without extracting/relocating blobs for the new current key.
  void NextWithoutPreparingOutput();

  // Completes output preparation once for the current key.
  void PrepareOutputIfNeeded();

  // Getters
  const Slice& key() const { return key_; }
  const Slice& value() const { return value_; }
  const Status& status() const { return status_; }
  const ParsedInternalKey& ikey() const { return ikey_; }
  inline bool Valid() const { return validity_info_.IsValid(); }
  const Slice& user_key() const {
    if (UNLIKELY(is_range_del_)) {
      return ikey_.user_key;
    }
    return current_user_key_;
  }
  const CompactionIterationStats& iter_stats() const { return iter_stats_; }
  bool HasNumInputEntryScanned() const { return input_.HasNumItered(); }

  // This method should only be used when `HasNumInputEntryScanned()` returns
  // true, unless `must_count_input_entries=true` was specified during iterator
  // creation (which ensures the count is always accurate).
  uint64_t NumInputEntryScanned() const { return input_.NumItered(); }

  // Returns true if the current valid key was already scanned/counted during
  // a lookahead operation in a previous iteration.
  //
  // REQUIRED: Valid() must be true
  bool IsCurrentKeyAlreadyScanned() const {
    assert(Valid());
    return at_next_ || merge_out_iter_.Valid();
  }

  // Returns true if an earlier input entry for the current valid user key was
  // consumed without being output.
  //
  // REQUIRED: Valid() must be true
  bool HasConsumedEarlierEntriesOfCurrentKey() const {
    assert(Valid());
    return has_consumed_earlier_entries_of_current_key_;
  }

  Status InputStatus() const { return input_.status(); }

  bool IsDeleteRangeSentinelKey() const { return is_range_del_; }

 private:
  // Processes the input stream to find the next output
  void NextFromInput();

  void NextImpl();

  // Do final preparations before presenting the output to the callee.
  void PrepareOutput();

  // Passes the output value to the blob file builder (if any), and replaces it
  // with the corresponding blob reference if it has been actually written to a
  // blob file (i.e. if it passed the value size check). Returns true if the
  // value got extracted to a blob file, false otherwise.
  bool ExtractLargeValueIfNeededImpl();

  // Extracts large values as described above, and updates the internal key's
  // type to kTypeBlobIndex if the value got extracted. Should only be called
  // for regular values (kTypeValue).
  void ExtractLargeValueIfNeeded();

  // Extracts large column values from wide column entities to blob files.
  // For each column whose value size >= min_blob_size, the value is written
  // to a blob file and replaced with a blob index reference. Should only be
  // called for wide column entities (kTypeWideColumnEntity).
  void ExtractLargeColumnValuesIfNeeded();

  // Relocates valid blobs residing in the oldest blob files if garbage
  // collection is enabled. Relocated blobs are written to new blob files or
  // inlined in the LSM tree depending on the current settings (i.e.
  // enable_blob_files and min_blob_size). Should only be called for blob
  // references (kTypeBlobIndex).
  //
  // Note: the stacked BlobDB implementation's compaction filter based GC
  // algorithm is also called from here.
  void GarbageCollectBlobIfNeeded();

  // Garbage collects blob references within wide column entities. For entities
  // that contain blob column references, this method checks if any of the
  // referenced blob files need garbage collection. If so, the blob values are
  // fetched and either relocated to new blob files or inlined based on
  // current settings. Should only be called for wide column entities
  // (kTypeWideColumnEntity) that have blob columns.
  void GarbageCollectEntityBlobsIfNeeded();

  // Helper for GarbageCollectEntityBlobsIfNeeded: Fetches blob values that need
  // garbage collection based on the cutoff file number.
  // Returns true if any blobs were fetched, false otherwise.
  // On error, sets status_ and returns false.
  bool FetchBlobsNeedingGC(
      const std::vector<WideColumn>& columns,
      const std::vector<std::pair<size_t, BlobIndex>>& blob_columns,
      std::vector<std::pair<size_t, PinnableSlice>>* fetched_blob_values);

  // Helper for GarbageCollectEntityBlobsIfNeeded: Relocates fetched blob
  // values to new blob files if blob_file_builder_ is available.
  // Returns the new blob column indices. On error, sets status_ and returns
  // empty.
  std::vector<std::pair<size_t, BlobIndex>> RelocateBlobValues(
      const std::vector<std::pair<size_t, PinnableSlice>>& fetched_blob_values);

  // Helper for GarbageCollectEntityBlobsIfNeeded: Serializes the entity after
  // garbage collection, combining original columns with fetched values and
  // new blob indices. On error, sets status_.
  void SerializeEntityAfterGC(
      const std::vector<WideColumn>& columns,
      const std::vector<std::pair<size_t, BlobIndex>>& original_blob_columns,
      const std::vector<std::pair<size_t, PinnableSlice>>& fetched_blob_values,
      const std::vector<std::pair<size_t, BlobIndex>>& new_blob_columns);

  // Invoke compaction filter if needed.
  // Return true on success, false on failures (e.g.: kIOError).
  bool InvokeFilterIfNeeded(bool* need_skip, Slice* skip_until);

  // Given a sequence number, return the sequence number of the
  // earliest snapshot that this sequence number is visible in.
  // The snapshots themselves are arranged in ascending order of
  // sequence numbers.
  // Employ a sequential search because the total number of
  // snapshots are typically small.
  inline SequenceNumber findEarliestVisibleSnapshot(
      SequenceNumber in, SequenceNumber* prev_snapshot);

  inline bool KeyCommitted(SequenceNumber sequence) {
    return snapshot_checker_ == nullptr ||
           snapshot_checker_->CheckInSnapshot(sequence, job_snapshot_) ==
               SnapshotCheckerResult::kInSnapshot;
  }

  bool DefinitelyInSnapshot(SequenceNumber seq, SequenceNumber snapshot);

  bool DefinitelyNotInSnapshot(SequenceNumber seq, SequenceNumber snapshot);

  // Extract user-defined timestamp from user key if possible and compare it
  // with *full_history_ts_low_ if applicable.
  inline void UpdateTimestampAndCompareWithFullHistoryLow() {
    if (!timestamp_size_) {
      return;
    }
    Slice ts = ExtractTimestampFromUserKey(ikey_.user_key, timestamp_size_);
    curr_ts_.assign(ts.data(), ts.size());
    if (full_history_ts_low_) {
      cmp_with_history_ts_low_ =
          cmp_->CompareTimestamp(ts, *full_history_ts_low_);
    }
  }

  static std::unique_ptr<BlobFetcher> CreateBlobFetcherIfNeeded(
      const CompactionProxy* compaction, const Version* input_version,
      Env::IOActivity blob_read_io_activity);
  static std::unique_ptr<PrefetchBufferCollection>
  CreatePrefetchBufferCollectionIfNeeded(const CompactionProxy* compaction);

  SequenceIterWrapper input_;
  const Comparator* cmp_;
  MergeHelper* merge_helper_;
  const std::vector<SequenceNumber>* snapshots_;
  // List of snapshots released during compaction.
  // findEarliestVisibleSnapshot() find them out from return of
  // snapshot_checker, and make sure they will not be returned as
  // earliest visible snapshot of an older value.
  // See WritePreparedTransactionTest::ReleaseSnapshotDuringCompaction3.
  std::unordered_set<SequenceNumber> released_snapshots_;
  const SequenceNumber earliest_write_conflict_snapshot_;
  const SequenceNumber job_snapshot_;
  const SnapshotChecker* const snapshot_checker_;
  Env* env_;
  SystemClock* clock_;
  const bool report_detailed_time_;
  CompactionRangeDelAggregator* range_del_agg_;
  BlobFileBuilder* blob_file_builder_;
  std::unique_ptr<CompactionProxy> compaction_;
  const CompactionFilter* compaction_filter_;
  const bool filter_supports_v4_;
  const std::atomic<bool>* shutting_down_;
  const std::atomic<bool>& manual_compaction_canceled_;
  const bool bottommost_level_;
  const bool visible_at_tip_;
  const SequenceNumber earliest_snapshot_;

  std::shared_ptr<Logger> info_log_;

  const bool allow_data_in_errors_;

  const bool enforce_single_del_contracts_;

  // Comes from comparator.
  const size_t timestamp_size_;

  // Lower bound timestamp to retain full history in terms of user-defined
  // timestamp. If a key's timestamp is older than full_history_ts_low_, then
  // the key *may* be eligible for garbage collection (GC). The skipping logic
  // is in `NextFromInput()` and `PrepareOutput()`.
  // If nullptr, NO GC will be performed and all history will be preserved.
  const std::string* const full_history_ts_low_;

  // State
  //
  enum ValidContext : uint8_t {
    kMerge1 = 0,
    kMerge2 = 1,
    kParseKeyError = 2,
    kCurrentKeyUncommitted = 3,
    kKeepSDAndClearPut = 4,
    kKeepTsHistory = 5,
    kKeepSDForConflictCheck = 6,
    kKeepSDForSnapshot = 7,
    kKeepSD = 8,
    kKeepDel = 9,
    kNewUserKey = 10,
    kRangeDeletion = 11,
    kSwapPreferredSeqno = 12,
  };

  struct ValidityInfo {
    inline bool IsValid() const { return rep & 1; }
    ValidContext GetContext() const {
      return static_cast<ValidContext>(rep >> 1);
    }
    inline void SetValid(uint8_t ctx) { rep = (ctx << 1) | 1; }
    inline void Invalidate() { rep = 0; }

    uint8_t rep{0};
  } validity_info_;

  // Points to a copy of the current compaction iterator output (current_key_)
  // if valid.
  Slice key_;
  // Points to the value in the underlying iterator that corresponds to the
  // current output.
  Slice value_;
  // The status is OK unless compaction iterator encounters a merge operand
  // while not having a merge operator defined.
  Status status_;
  // Stores the user key, sequence number and type of the current compaction
  // iterator output (or current key in the underlying iterator during
  // NextFromInput()).
  ParsedInternalKey ikey_;

  // Stores whether current_user_key_ is valid. If so, current_user_key_
  // stores the user key of the last key seen by the iterator.
  // If false, treat the next key to read as a new user key.
  bool has_current_user_key_ = false;
  // If false, the iterator holds a copy of the current compaction iterator
  // output (or current key in the underlying iterator during NextFromInput()).
  bool at_next_ = false;
  // Whether an earlier input entry of the current user key was not output.
  bool has_consumed_earlier_entries_of_current_key_ = false;

  // A copy of the current internal key.
  IterKey current_key_;
  Slice current_user_key_;
  std::string curr_ts_;
  SequenceNumber current_user_key_sequence_;
  SequenceNumber current_user_key_snapshot_;

  // True if the iterator has already returned a record for the current key.
  bool has_outputted_key_ = false;

  // Truncate the value of the next key and output it without applying any
  // compaction rules. This is an optimization for outputting a put after
  // a single delete. See more in `NextFromInput()` under Optimization 3.
  bool clear_and_output_next_key_ = false;

  MergeOutputIterator merge_out_iter_;
  Status merge_until_status_;
  // PinnedIteratorsManager used to pin input_ Iterator blocks while reading
  // merge operands and then releasing them after consuming them.
  PinnedIteratorsManager pinned_iters_mgr_;

  uint64_t blob_garbage_collection_cutoff_file_number_;

  std::unique_ptr<BlobFetcher> blob_fetcher_;
  std::unique_ptr<PrefetchBufferCollection> prefetch_buffers_;

  std::string blob_index_;
  PinnableSlice blob_value_;
  std::string compaction_filter_value_;
  InternalKey compaction_filter_skip_until_;
  // Buffer for storing rewritten entity with blob references after
  // extracting large column values to blob files.
  std::string rewritten_entity_;
  // Cached deserialized columns for the current wide-column entity, reused
  // across iterations to avoid per-key heap allocations. This is the full
  // sorted column set. For blob-backed columns, value() initially contains the
  // serialized BlobIndex bytes from the entity.
  std::vector<WideColumn> entity_columns_;
  // Side list for the blob-backed subset of entity_columns_. Each pair is
  // (column_index_in_entity_columns_, decoded_blob_index), so callers can tell
  // which entries in entity_columns_ are blob references without reparsing.
  std::vector<std::pair<size_t, BlobIndex>> entity_blob_columns_;
  // Temporary Slice-based view over entity_columns_ used when reserializing an
  // entity after blob extraction, to avoid copying names/values into strings.
  WideColumns entity_wide_columns_;
  // Reusable storage for existing_columns in InvokeFilterIfNeeded,
  // avoiding per-key heap allocations.
  WideColumns filter_existing_columns_;
  // Reusable blob resolver for compaction filter, avoiding per-key heap
  // allocation. Init() is called once; Reset() is called per entity.
  CompactionBlobResolver blob_resolver_;
  // True when entity_columns_/entity_blob_columns_ already describe the
  // current input record (e.g., because InvokeFilterIfNeeded() deserialized
  // it) and PrepareOutput() can skip redundant work. Reset for each candidate
  // input record and whenever the filter rewrites the entity value.
  bool entity_deserialized_{false};
  bool output_prepared_{false};
  // "level_ptrs" holds indices that remember which file of an associated
  // level we were last checking during the last call to compaction->
  // KeyNotExistsBeyondOutputLevel(). This allows future calls to the function
  // to pick off where it left off since each subcompaction's key range is
  // increasing so a later call to the function must be looking for a key that
  // is in or beyond the last file checked during the previous call
  std::vector<size_t> level_ptrs_;
  CompactionIterationStats iter_stats_;

  // Used to avoid purging uncommitted values. The application can specify
  // uncommitted values by providing a SnapshotChecker object.
  bool current_key_committed_;

  // Saved result of ucmp->CompareTimestamp(current_ts_, *full_history_ts_low_)
  int cmp_with_history_ts_low_;

  const int level_;

  // True if the previous internal key (same user key)'s sequence number has
  // just been zeroed out during bottommost compaction.
  bool last_key_seq_zeroed_{false};

  // Max seqno that can be zeroed out at last level (various reasons)
  const SequenceNumber preserve_seqno_after_ = kMaxSequenceNumber;

  void AdvanceInputIter() { input_.Next(); }

  // Advances past an input entry that will not be emitted as output.
  void AdvanceInputIterWithoutOutput() {
    has_consumed_earlier_entries_of_current_key_ = true;
    input_.Next();
  }

  void SkipUntil(const Slice& skip_until) {
    has_consumed_earlier_entries_of_current_key_ = true;
    input_.Seek(skip_until);
  }

  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }

  bool IsPausingManualCompaction() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return manual_compaction_canceled_.load(std::memory_order_relaxed);
  }

  // Stores whether the current compaction iterator output
  // is a range tombstone start key.
  bool is_range_del_{false};
};

inline bool CompactionIterator::DefinitelyInSnapshot(SequenceNumber seq,
                                                     SequenceNumber snapshot) {
  return DataIsDefinitelyInSnapshot(seq, snapshot, snapshot_checker_);
}

inline bool CompactionIterator::DefinitelyNotInSnapshot(
    SequenceNumber seq, SequenceNumber snapshot) {
  return DataIsDefinitelyNotInSnapshot(seq, snapshot, snapshot_checker_);
}

}  // namespace ROCKSDB_NAMESPACE
