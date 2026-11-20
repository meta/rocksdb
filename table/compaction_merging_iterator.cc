//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include "table/compaction_merging_iterator.h"

#include "db/internal_stats.h"

namespace ROCKSDB_NAMESPACE {
class CompactionMergingIterator : public InternalIterator {
 public:
  CompactionMergingIterator(
      const InternalKeyComparator* comparator, InternalIterator** children,
      int n, bool is_arena_mode,
      std::vector<std::pair<std::unique_ptr<TruncatedRangeDelIterator>,
                            std::unique_ptr<TruncatedRangeDelIterator>**>>&
          range_tombstones,
      InternalStats* internal_stats)
      : is_arena_mode_(is_arena_mode),
        comparator_(comparator),
        current_(nullptr),
        minHeap_(CompactionHeapItemComparator(comparator_)),
        pinned_iters_mgr_(nullptr),
        internal_stats_(internal_stats),
        num_sorted_runs_recorded_(0) {
    children_.resize(n);
    for (int i = 0; i < n; i++) {
      children_[i].level = i;
      children_[i].iter.Set(children[i]);
      assert(children_[i].type == HeapItem::ITERATOR);
    }
    assert(range_tombstones.size() == static_cast<size_t>(n));
    for (auto& p : range_tombstones) {
      range_tombstone_iters_.push_back(std::move(p.first));
    }
    pinned_heap_item_.resize(n);
    for (int i = 0; i < n; ++i) {
      if (range_tombstones[i].second) {
        // for LevelIterator
        *range_tombstones[i].second = &range_tombstone_iters_[i];
      }
      pinned_heap_item_[i].level = i;
      pinned_heap_item_[i].type = HeapItem::DELETE_RANGE_START;
    }
    if (internal_stats_) {
      TEST_SYNC_POINT("CompactionMergingIterator::UpdateInternalStats");
      // The size of children_ or range_tombstone_iters_ (n) should not change
      // but to be safe, we can record the size here so we decrement by the
      // correct amount at destruction time
      num_sorted_runs_recorded_ = n;
      internal_stats_->IncrNumRunningCompactionSortedRuns(
          num_sorted_runs_recorded_);
      assert(num_sorted_runs_recorded_ <=
             internal_stats_->NumRunningCompactionSortedRuns());
    }
  }

  // No copying allowed
  CompactionMergingIterator(const CompactionMergingIterator&) = delete;
  CompactionMergingIterator& operator=(const CompactionMergingIterator&) =
      delete;
  CompactionMergingIterator(CompactionMergingIterator&&) = delete;
  CompactionMergingIterator& operator=(CompactionMergingIterator&&) = delete;

  void considerStatus(const Status& s) {
    if (!s.ok() && status_.ok()) {
      status_ = s;
    }
  }

  ~CompactionMergingIterator() override {
    if (internal_stats_) {
      assert(num_sorted_runs_recorded_ == range_tombstone_iters_.size());
      assert(num_sorted_runs_recorded_ <=
             internal_stats_->NumRunningCompactionSortedRuns());
      internal_stats_->DecrNumRunningCompactionSortedRuns(
          num_sorted_runs_recorded_);
    }

    range_tombstone_iters_.clear();

    for (auto& child : children_) {
      child.iter.DeleteIter(is_arena_mode_);
    }
    status_.PermitUncheckedError();
  }

  bool Valid() const override { return current_ != nullptr && status_.ok(); }

  Status status() const override { return status_; }

  void SeekToFirst() override;

  void Seek(const Slice& target) override;

  void Next() override;

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    if (LIKELY(current_->type == HeapItem::ITERATOR)) {
      return current_->iter.value();
    } else {
      return dummy_tombstone_val;
    }
  }

  // Here we simply relay MayBeOutOfLowerBound/MayBeOutOfUpperBound result
  // from current child iterator. Potentially as long as one of child iterator
  // report out of bound is not possible, we know current key is within bound.
  bool MayBeOutOfLowerBound() override {
    assert(Valid());
    return current_->type == HeapItem::DELETE_RANGE_START ||
           current_->iter.MayBeOutOfLowerBound();
  }

  IterBoundCheck UpperBoundCheckResult() override {
    assert(Valid());
    return current_->type == HeapItem::DELETE_RANGE_START
               ? IterBoundCheck::kUnknown
               : current_->iter.UpperBoundCheckResult();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    for (auto& child : children_) {
      child.iter.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }

  bool IsDeleteRangeSentinelKey() const override {
    assert(Valid());
    return current_->type == HeapItem::DELETE_RANGE_START;
  }

  // Compaction uses the above subset of InternalIterator interface.
  void SeekToLast() override { assert(false); }

  void SeekForPrev(const Slice&) override { assert(false); }

  void Prev() override { assert(false); }

  bool NextAndGetResult(IterateResult*) override {
    assert(false);
    return false;
  }

  bool IsKeyPinned() const override {
    assert(false);
    return false;
  }

  bool IsValuePinned() const override {
    assert(false);
    return false;
  }

  bool PrepareValue() override {
    assert(false);
    return false;
  }

 private:
  struct HeapItem {
    HeapItem() = default;

    IteratorWrapper iter;
    size_t level = 0;
    std::string tombstone_str;
    enum Type { ITERATOR, DELETE_RANGE_START };
    Type type = ITERATOR;

    explicit HeapItem(size_t _level, InternalIteratorBase<Slice>* _iter)
        : level(_level), type(Type::ITERATOR) {
      iter.Set(_iter);
    }

    void SetTombstoneForCompaction(const ParsedInternalKey&& pik) {
      tombstone_str.clear();
      AppendInternalKey(&tombstone_str, pik);
    }

    [[nodiscard]] Slice key() const {
      return type == ITERATOR ? iter.key() : tombstone_str;
    }
  };

  class CompactionHeapItemComparator {
   public:
    explicit CompactionHeapItemComparator(
        const InternalKeyComparator* comparator)
        : comparator_(comparator) {}

    bool operator()(HeapItem* a, HeapItem* b) const {
      int r = comparator_->Compare(a->key(), b->key());
      // For each file, we assume all range tombstone start keys come before
      // its file boundary sentinel key (file's meta.largest key).
      // In the case when meta.smallest = meta.largest and range tombstone start
      // key is truncated at meta.smallest, the start key will have op_type =
      // kMaxValid to make it smaller (see TruncatedRangeDelIterator
      // constructor). The following assertion validates this assumption.
      assert(a->type == b->type || r != 0);
      return r > 0;
    }

   private:
    const InternalKeyComparator* comparator_;
  };

  using CompactionMinHeap = BinaryHeap<HeapItem*, CompactionHeapItemComparator>;
  bool is_arena_mode_;
  const InternalKeyComparator* comparator_;
  // HeapItem for all child point iterators.
  std::vector<HeapItem> children_;
  // HeapItem for range tombstones. pinned_heap_item_[i] corresponds to the
  // current range tombstone from range_tombstone_iters_[i].
  std::vector<HeapItem> pinned_heap_item_;
  // range_tombstone_iters_[i] contains range tombstones in the sorted run that
  // corresponds to children_[i]. range_tombstone_iters_[i] ==
  // nullptr means the sorted run of children_[i] does not have range
  // tombstones (or the current SSTable does not have range tombstones in the
  // case of LevelIterator).
  std::vector<std::unique_ptr<TruncatedRangeDelIterator>>
      range_tombstone_iters_;
  // Used as value for range tombstone keys
  std::string dummy_tombstone_val{};

  // Skip file boundary sentinel keys.
  void FindNextVisibleKey();

  // top of minHeap_
  HeapItem* current_;
  // If any of the children have non-ok status, this is one of them.
  Status status_;
  CompactionMinHeap minHeap_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  InternalStats* internal_stats_;
  uint64_t num_sorted_runs_recorded_;
  // Process a child that is not in the min heap.
  // If valid, add to the min heap. Otherwise, check status.
  void AddToMinHeapOrCheckStatus(HeapItem*);

  HeapItem* CurrentForward() const {
    return !minHeap_.empty() ? minHeap_.top() : nullptr;
  }

  void InsertRangeTombstoneAtLevel(size_t level) {
    if (range_tombstone_iters_[level]->Valid()) {
      pinned_heap_item_[level].SetTombstoneForCompaction(
          range_tombstone_iters_[level]->start_key());
      minHeap_.push(&pinned_heap_item_[level]);
    }
  }
};

void CompactionMergingIterator::SeekToFirst() {
  minHeap_.clear();
  status_ = Status::OK();
  for (auto& child : children_) {
    child.iter.SeekToFirst();
    AddToMinHeapOrCheckStatus(&child);
  }

  for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
    if (range_tombstone_iters_[i]) {
      range_tombstone_iters_[i]->SeekToFirst();
      InsertRangeTombstoneAtLevel(i);
    }
  }

  FindNextVisibleKey();
  current_ = CurrentForward();
}

void CompactionMergingIterator::Seek(const Slice& target) {
  minHeap_.clear();
  status_ = Status::OK();
  for (auto& child : children_) {
    child.iter.Seek(target);
    AddToMinHeapOrCheckStatus(&child);
  }

  ParsedInternalKey pik;
  ParseInternalKey(target, &pik, false /* log_err_key */)
      .PermitUncheckedError();
  for (size_t i = 0; i < range_tombstone_iters_.size(); ++i) {
    if (range_tombstone_iters_[i]) {
      range_tombstone_iters_[i]->Seek(pik.user_key);
      // For compaction, output keys should all be after seek target.
      while (range_tombstone_iters_[i]->Valid() &&
             comparator_->Compare(range_tombstone_iters_[i]->start_key(), pik) <
                 0) {
        range_tombstone_iters_[i]->Next();
      }
      InsertRangeTombstoneAtLevel(i);
    }
  }

  FindNextVisibleKey();
  current_ = CurrentForward();
}

void CompactionMergingIterator::Next() {
  assert(Valid());
  // For the heap modifications below to be correct, current_ must be the
  // current top of the heap.
  assert(current_ == CurrentForward());
  // as the current points to the current record. move the iterator forward.
  if (current_->type == HeapItem::ITERATOR) {
    current_->iter.Next();
    if (current_->iter.Valid()) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      assert(current_->iter.status().ok());
      minHeap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      considerStatus(current_->iter.status());
      minHeap_.pop();
    }
  } else {
    assert(current_->type == HeapItem::DELETE_RANGE_START);
    size_t level = current_->level;
    assert(range_tombstone_iters_[level]);
    range_tombstone_iters_[level]->Next();
    if (range_tombstone_iters_[level]->Valid()) {
      pinned_heap_item_[level].SetTombstoneForCompaction(
          range_tombstone_iters_[level]->start_key());
      minHeap_.replace_top(&pinned_heap_item_[level]);
    } else {
      minHeap_.pop();
    }
  }
  FindNextVisibleKey();
  current_ = CurrentForward();
}

void CompactionMergingIterator::FindNextVisibleKey() {
  while (!minHeap_.empty()) {
    HeapItem* current = minHeap_.top();
    // IsDeleteRangeSentinelKey() here means file boundary sentinel keys.
    if (current->type != HeapItem::ITERATOR ||
        !current->iter.IsDeleteRangeSentinelKey()) {
      return;
    }
    // range tombstone start keys from the same SSTable should have been
    // exhausted
    assert(!range_tombstone_iters_[current->level] ||
           !range_tombstone_iters_[current->level]->Valid());
    // current->iter is a LevelIterator, and it enters a new SST file in the
    // Next() call here.
    current->iter.Next();
    if (current->iter.Valid()) {
      assert(current->iter.status().ok());
      minHeap_.replace_top(current);
    } else {
      considerStatus(current->iter.status());
      minHeap_.pop();
    }
    if (range_tombstone_iters_[current->level]) {
      InsertRangeTombstoneAtLevel(current->level);
    }
  }
}

void CompactionMergingIterator::AddToMinHeapOrCheckStatus(HeapItem* child) {
  if (child->iter.Valid()) {
    assert(child->iter.status().ok());
    minHeap_.push(child);
  } else {
    considerStatus(child->iter.status());
  }
}

InternalIterator* NewCompactionMergingIterator(
    const InternalKeyComparator* comparator, InternalIterator** children, int n,
    std::vector<std::pair<std::unique_ptr<TruncatedRangeDelIterator>,
                          std::unique_ptr<TruncatedRangeDelIterator>**>>&
        range_tombstone_iters,
    Arena* arena, InternalStats* stats) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyInternalIterator<Slice>(arena);
  } else {
    if (arena == nullptr) {
      return new CompactionMergingIterator(comparator, children, n,
                                           false /* is_arena_mode */,
                                           range_tombstone_iters, stats);
    } else {
      auto mem = arena->AllocateAligned(sizeof(CompactionMergingIterator));
      return new (mem) CompactionMergingIterator(comparator, children, n,
                                                 true /* is_arena_mode */,
                                                 range_tombstone_iters, stats);
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
