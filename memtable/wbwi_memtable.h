//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include "db/memtable.h"
#include "rocksdb/utilities/write_batch_with_index.h"

namespace ROCKSDB_NAMESPACE {
// An implementation of the ReadOnlyMemTable interface based on the content
// of the given write batch with index (WBWI) object. This can be used to ingest
// a transaction (which is based on WBWI) into the DB as an immutable memtable.
//
// REQUIRES: overwrite_key to be true for the WBWI
// Since the keys in WBWI do not have sequence number, this class is responsible
// for assigning sequence numbers to the keys. This memtable needs to be
// assigned a range of sequence numbers through AssignSequenceNumbers(seqno)
// before being available for reads.
//
// The sequence number assignment uses the update count for each key
// tracked in WBWI (see WBWIIterator::GetUpdateCount()). For each key, the
// sequence number assigned is seqno.lower_bound + update_count - 1. So more
// recent updates will have higher sequence number.
//
// Since WBWI with overwrite mode keeps track of the most recent update for
// each key, this memtable contains one update per key usually. However, there
// are two exceptions:
// 1. Merge operations: Each Merge operation do not overwrite existing entries,
// if a user uses Merge, multiple entries may be kept.
// 2. Overwriten SingleDelete: this memtable needs to emit an extra
// SingleDelete even when the SD is overwritten by another update.
// Consider the following scenario:
// - WBWI has SD(k) then PUT(k, v1)
// - DB has PUT(k, v2) in L1
// - flush WBWI adds PUT(k, v1) into L0
// - live memtable contains SD(k)
// - flush live memtable and compact it with L0 will drop SD(k) and PUT(k, v1)
// - the PUT(k, v2) in L1 incorrectly becomes visible
// So during flush, iterator from this memtable will need emit overwritten
// single deletion. This SD will be assigned seqno.lower_bound.
class WBWIMemTable final : public ReadOnlyMemTable {
 public:
  struct SeqnoRange {
    SequenceNumber lower_bound = kMaxSequenceNumber;
    SequenceNumber upper_bound = kMaxSequenceNumber;
  };
  WBWIMemTable(const std::shared_ptr<WriteBatchWithIndex>& wbwi,
               const Comparator* cmp, uint32_t cf_id,
               const ImmutableOptions* immutable_options,
               const MutableCFOptions* cf_options,
               const WriteBatchWithIndex::CFStat& stat)
      : wbwi_(wbwi),
        comparator_(cmp),
        ikey_comparator_(comparator_),
        moptions_(*immutable_options, *cf_options),
        clock_(immutable_options->clock),
        // We need to include overwritten_sd_count in num_entries_ since flush
        // verifies number of entries processed and that iterator for this
        // memtable will emit overwritten SingleDelete entries during flush, See
        // comment above WBWIMemTableIterator for more detail.
        num_entries_(stat.entry_count + stat.overwritten_sd_count),
        cf_id_(cf_id) {
    assert(wbwi->GetOverwriteKey());
  }

  // No copying allowed
  WBWIMemTable(const WBWIMemTable&) = delete;
  WBWIMemTable& operator=(const WBWIMemTable&) = delete;
  WBWIMemTable(WBWIMemTable&&) = delete;
  WBWIMemTable& operator=(WBWIMemTable&&) = delete;

  ~WBWIMemTable() override { assert(refs_ == 0); }

  const char* Name() const override { return "WBWIMemTable"; }

  size_t ApproximateMemoryUsage() override {
    // FIXME: we can calculate for each CF or just divide evenly among CFs
    // Used in ReportFlushInputSize(), MemPurgeDecider, flush job event logging,
    // and InternalStats::HandleCurSizeAllMemTables
    return 0;
  }

  size_t MemoryAllocatedBytes() const override {
    // FIXME: similar to ApproximateMemoryUsage().
    //   Used in MemTableList to trim memtable history.
    return 0;
  }

  size_t WBMTrackedMemoryUsage() const override { return 0; }

  void UniqueRandomSample(
      const uint64_t& /* target_sample_size */,
      std::unordered_set<const char*>* /* entries */) override {
    // TODO: support mempurge
    assert(false);
  }

  InternalIterator* NewIterator(const ReadOptions&,
                                UnownedPtr<const SeqnoToTimeMapping>,
                                Arena* arena,
                                const SliceTransform* /* prefix_extractor */,
                                bool for_flush) override;

  // Returns an iterator that wraps a MemTableIterator and logically strips the
  // user-defined timestamp of each key. This API is only used by flush when
  // user-defined timestamps in MemTable only feature is enabled.
  InternalIterator* NewTimestampStrippingIterator(
      const ReadOptions&, UnownedPtr<const SeqnoToTimeMapping>, Arena* arena,
      const SliceTransform*, size_t) override {
    // TODO: support UDT
    assert(false);
    return NewErrorInternalIterator(
        Status::NotSupported(
            "WBWIMemTable does not support NewTimestampStrippingIterator."),
        arena);
  }

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions&, SequenceNumber, bool) override {
    // TODO: support DeleteRange
    assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
    return nullptr;
  }

  FragmentedRangeTombstoneIterator* NewTimestampStrippingRangeTombstoneIterator(
      const ReadOptions&, SequenceNumber, size_t) override {
    // TODO: support UDT
    assert(false);
    return nullptr;
  }

  // FIXME: not a good practice to use default parameter with virtual function
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

  uint64_t NumEntries() const override { return num_entries_; }

  uint64_t NumDeletion() const override {
    // FIXME: this is used for stats and event logging
    return 0;
  }

  uint64_t NumRangeDeletion() const override {
    // FIXME
    assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
    return 0;
  }

  uint64_t GetDataSize() const override {
    // FIXME: used in event logging in flush_job
    return 0;
  }

  SequenceNumber GetEarliestSequenceNumber() override {
    return assigned_seqno_.lower_bound;
  }

  bool IsEmpty() const override {
    // Ideally also check that wbwi contains updates from this CF. For now, we
    // only create WBWIMemTable for CFs with updates in wbwi.
    return wbwi_->GetWriteBatch()->Count() == 0;
  }

  SequenceNumber GetFirstSequenceNumber() override {
    return assigned_seqno_.lower_bound;
  }

  uint64_t GetMinLogContainingPrepSection() override {
    // FIXME: used to retain WAL with pending Prepare
    return min_prep_log_referenced_;
  }

  void MarkImmutable() override {}

  void MarkFlushed() override {}

  MemTableStats ApproximateStats(const Slice&, const Slice&) override {
    // FIXME: used for query planning
    return {};
  }

  const InternalKeyComparator& GetInternalKeyComparator() const override {
    return ikey_comparator_;
  }

  uint64_t ApproximateOldestKeyTime() const override {
    // FIXME: can use the time when this is added to the DB.
    return kUnknownOldestAncestorTime;
  }

  bool IsFragmentedRangeTombstonesConstructed() const override {
    assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
    return true;
  }

  Slice GetNewestUDT() const override {
    // FIXME: support UDT
    assert(false);
    return Slice();
  }

  // Assign a sequence number to the entries in this memtable.
  void AssignSequenceNumbers(const SeqnoRange& seqno_range) {
    // Not expecting to assign seqno multiple times.
    assert(assigned_seqno_.lower_bound == kMaxSequenceNumber);
    assert(assigned_seqno_.upper_bound == kMaxSequenceNumber);

    assigned_seqno_ = seqno_range;

    assert(assigned_seqno_.lower_bound <= assigned_seqno_.upper_bound);
    assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  }

  void SetMinPrepLog(uint64_t min_prep_log) {
    min_prep_log_referenced_ = min_prep_log;
  }

 private:
  inline InternalIterator* NewIterator() const;

  bool GetImpl(const LookupKey& key, std::string* value,
               PinnableWideColumns* columns, std::string* timestamp, Status* s,
               MergeContext* merge_context,
               SequenceNumber* max_covering_tombstone_seq, SequenceNumber* seq,
               const ReadOptions& read_opts, bool immutable_memtable,
               ReadCallback* callback, bool* is_blob_index, bool do_merge,
               const BlobFetcher* blob_fetcher, bool* newer_version_present);

  std::shared_ptr<WriteBatchWithIndex> wbwi_;
  const Comparator* comparator_;
  InternalKeyComparator ikey_comparator_;
  SeqnoRange assigned_seqno_;
  const ImmutableMemTableOptions moptions_;
  SystemClock* clock_;
  uint64_t min_prep_log_referenced_{0};
  uint64_t num_entries_;
  // WBWI can contains updates to multiple CFs. `cf_id_` determines which CF
  // this memtable is for.
  const uint32_t cf_id_;
};

class WBWIMemTableIterator final : public InternalIterator {
 public:
  WBWIMemTableIterator(std::unique_ptr<WBWIIterator>&& it,
                       const WBWIMemTable::SeqnoRange& assigned_seqno,
                       const Comparator* comparator, bool for_flush)
      : it_(std::move(it)),
        assigned_seqno_(assigned_seqno),
        comparator_(comparator),
        emit_overwritten_single_del_(for_flush) {
    assert(assigned_seqno_.lower_bound <= assigned_seqno_.upper_bound);
    assert(assigned_seqno_.upper_bound < kMaxSequenceNumber);
    s_.PermitUncheckedError();
  }

  // No copying allowed
  WBWIMemTableIterator(const WBWIMemTableIterator&) = delete;
  WBWIMemTableIterator& operator=(const WBWIMemTableIterator&) = delete;
  WBWIMemTableIterator(WBWIMemTableIterator&&) = delete;
  WBWIMemTableIterator& operator=(WBWIMemTableIterator&&) = delete;

  ~WBWIMemTableIterator() override = default;

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    it_->SeekToFirst();
    UpdateKey();
  }

  void SeekToLast() override {
    assert(!emit_overwritten_single_del_);
    it_->SeekToLast();
    UpdateKey();
  }

  void Seek(const Slice& target) override {
    // `emit_overwritten_single_del_` is only used for flush, which does
    // sequential forward scan from the beginning.
    assert(!emit_overwritten_single_del_);
    Slice target_user_key = ExtractUserKey(target);
    // Moves to first update >= target_user_key
    it_->Seek(target_user_key);
    SequenceNumber target_seqno = GetInternalKeySeqno(target);
    // Move to the first entry with seqno <= target_seqno for the same
    // user key or a different user key.
    while (it_->Valid() &&
           comparator_->Compare(it_->Entry().key, target_user_key) == 0 &&
           target_seqno < CurrentKeySeqno()) {
      it_->Next();
    }
    UpdateKey();
  }

  void SeekForPrev(const Slice& target) override {
    assert(!emit_overwritten_single_del_);
    Slice target_user_key = ExtractUserKey(target);
    // Moves to last update <= target_user_key
    it_->SeekForPrev(target_user_key);
    SequenceNumber target_seqno = GetInternalKeySeqno(target);
    // Move to the first entry with seqno >= target_seqno for the same
    // user key or a different user key.
    while (it_->Valid() &&
           comparator_->Compare(it_->Entry().key, target_user_key) == 0 &&
           CurrentKeySeqno() < target_seqno) {
      it_->Prev();
    }
    UpdateKey();
  }

  void Next() override {
    assert(Valid());
    if (emit_overwritten_single_del_) {
      if (it_->HasOverWrittenSingleDel() && !at_overwritten_single_del_) {
        // Merge and SingleDelete on the same key is undefined behavior.
        assert(it_->Entry().type != kMergeRecord);
        UpdateSingleDeleteKey();
        return;
      }
      at_overwritten_single_del_ = false;
    }

    it_->Next();
    UpdateKey();
  }

  bool NextAndGetResult(IterateResult* result) override {
    assert(Valid());
    Next();
    bool is_valid = Valid();
    if (is_valid) {
      result->key = key();
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
    }
    return is_valid;
  }

  void Prev() override {
    assert(!emit_overwritten_single_del_);
    assert(Valid());
    it_->Prev();
    UpdateKey();
  }

  Slice key() const override {
    assert(Valid());
    return key_;
  }

  Slice value() const override {
    assert(Valid());
    return it_->Entry().value;
  }

  Status status() const override {
    assert(it_->status().ok());
    return s_;
  }

  bool IsValuePinned() const override { return true; }

 private:
  static const std::unordered_map<WriteType, ValueType> WriteTypeToValueTypeMap;

  SequenceNumber CurrentKeySeqno() {
    assert(it_->Valid());
    assert(it_->GetUpdateCount() >= 1);
    auto seq = assigned_seqno_.lower_bound + it_->GetUpdateCount() - 1;
    assert(seq <= assigned_seqno_.upper_bound);
    return seq;
  }

  // If it_ is valid, udate key_ to an internal key containing it_ current
  // key, CurrentKeySeqno() and a type corresponding to it_ current entry type.
  void UpdateKey() {
    valid_ = it_->Valid();
    if (!Valid()) {
      key_.clear();
      return;
    }
    auto t = WriteTypeToValueTypeMap.find(it_->Entry().type);
    assert(t != WriteTypeToValueTypeMap.end());
    if (t == WriteTypeToValueTypeMap.end()) {
      key_.clear();
      valid_ = false;
      s_ = Status::Corruption("Unexpected write_batch_with_index entry type " +
                              std::to_string(it_->Entry().type));
      return;
    }
    key_buf_.SetInternalKey(it_->Entry().key, CurrentKeySeqno(), t->second);
    key_ = key_buf_.GetInternalKey();
  }

  void UpdateSingleDeleteKey() {
    assert(it_->Valid());
    assert(Valid());
    // The key that overwrites this SingleDelete will be assigned at least
    // seqno lower_bound + 1 (see CurrentKeySeqno()).
    key_buf_.SetInternalKey(it_->Entry().key, assigned_seqno_.lower_bound,
                            kTypeSingleDeletion);
    key_ = key_buf_.GetInternalKey();
    at_overwritten_single_del_ = true;
  }

  std::unique_ptr<WBWIIterator> it_;
  const WBWIMemTable::SeqnoRange assigned_seqno_;
  const Comparator* comparator_;
  IterKey key_buf_;
  // The current internal key.
  Slice key_;
  Status s_;
  bool valid_ = false;
  bool at_overwritten_single_del_ = false;
  bool emit_overwritten_single_del_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
