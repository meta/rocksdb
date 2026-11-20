//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <iterator>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "db/compaction/compaction_iteration_stats.h"
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_del_aggregator.h"
#include "db/range_tombstone_fragmenter.h"
#include "db/version_edit.h"
#include "rocksdb/comparator.h"
#include "rocksdb/types.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "util/heap.h"
#include "util/kv_map.h"

namespace ROCKSDB_NAMESPACE {

class TruncatedRangeDelIterator {
 public:
  TruncatedRangeDelIterator(
      std::unique_ptr<FragmentedRangeTombstoneIterator> iter,
      const InternalKeyComparator* icmp, const InternalKey* smallest,
      const InternalKey* largest);

  void SetRangeDelReadSeqno(SequenceNumber read_seqno) {
    iter_->SetRangeDelReadSeqno(read_seqno);
  }

  bool Valid() const;

  void Next() { iter_->TopNext(); }
  void Prev() { iter_->TopPrev(); }

  void InternalNext() { iter_->Next(); }

  // Seeks to the tombstone with the highest visible sequence number that covers
  // target (a user key). If no such tombstone exists, the position will be at
  // the earliest tombstone that ends after target.
  // REQUIRES: target is a user key.
  void Seek(const Slice& target);

  // Seeks to the first range tombstone with end_key() > target.
  void SeekInternalKey(const Slice& target);

  // Seeks to the tombstone with the highest visible sequence number that covers
  // target (a user key). If no such tombstone exists, the position will be at
  // the latest tombstone that starts before target.
  void SeekForPrev(const Slice& target);

  void SeekToFirst();
  void SeekToLast();

  ParsedInternalKey start_key() const {
    return (smallest_ == nullptr ||
            icmp_->Compare(*smallest_, iter_->parsed_start_key()) <= 0)
               ? iter_->parsed_start_key()
               : *smallest_;
  }

  ParsedInternalKey end_key() const {
    return (largest_ == nullptr ||
            icmp_->Compare(iter_->parsed_end_key(), *largest_) <= 0)
               ? iter_->parsed_end_key()
               : *largest_;
  }

  SequenceNumber seq() const { return iter_->seq(); }
  Slice timestamp() const {
    assert(icmp_->user_comparator()->timestamp_size());
    return iter_->timestamp();
  }
  void SetTimestampUpperBound(const Slice* ts_upper_bound) {
    iter_->SetTimestampUpperBound(ts_upper_bound);
  }

  std::map<SequenceNumber, std::unique_ptr<TruncatedRangeDelIterator>>
  SplitBySnapshot(const std::vector<SequenceNumber>& snapshots);

  SequenceNumber upper_bound() const { return iter_->upper_bound(); }

  SequenceNumber lower_bound() const { return iter_->lower_bound(); }

 private:
  std::unique_ptr<FragmentedRangeTombstoneIterator> iter_;
  const InternalKeyComparator* icmp_;
  const ParsedInternalKey* smallest_ = nullptr;
  const ParsedInternalKey* largest_ = nullptr;
  std::list<ParsedInternalKey> pinned_bounds_;

  const InternalKey* smallest_ikey_;
  const InternalKey* largest_ikey_;
};

struct SeqMaxComparator {
  bool operator()(const TruncatedRangeDelIterator* a,
                  const TruncatedRangeDelIterator* b) const {
    return a->seq() > b->seq();
  }
};

struct StartKeyMinComparator {
  explicit StartKeyMinComparator(const InternalKeyComparator* c) : icmp(c) {}

  bool operator()(const TruncatedRangeDelIterator* a,
                  const TruncatedRangeDelIterator* b) const {
    return icmp->Compare(a->start_key(), b->start_key()) > 0;
  }

  const InternalKeyComparator* icmp;
};

class ForwardRangeDelIterator {
 public:
  explicit ForwardRangeDelIterator(const InternalKeyComparator* icmp);

  bool ShouldDelete(const ParsedInternalKey& parsed);
  void Invalidate();

  void AddNewIter(TruncatedRangeDelIterator* iter,
                  const ParsedInternalKey& parsed) {
    iter->Seek(parsed.user_key);
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  size_t UnusedIdx() const { return unused_idx_; }
  void IncUnusedIdx() { unused_idx_++; }

 private:
  using ActiveSeqSet =
      std::multiset<TruncatedRangeDelIterator*, SeqMaxComparator>;

  struct EndKeyMinComparator {
    explicit EndKeyMinComparator(const InternalKeyComparator* c) : icmp(c) {}

    bool operator()(const ActiveSeqSet::const_iterator& a,
                    const ActiveSeqSet::const_iterator& b) const {
      return icmp->Compare((*a)->end_key(), (*b)->end_key()) > 0;
    }

    const InternalKeyComparator* icmp;
  };

  void PushIter(TruncatedRangeDelIterator* iter,
                const ParsedInternalKey& parsed) {
    if (!iter->Valid()) {
      // The iterator has been fully consumed, so we don't need to add it to
      // either of the heaps.
      return;
    }
    int cmp = icmp_->Compare(parsed, iter->start_key());
    if (cmp < 0) {
      PushInactiveIter(iter);
    } else {
      PushActiveIter(iter);
    }
  }

  void PushActiveIter(TruncatedRangeDelIterator* iter) {
    auto seq_pos = active_seqnums_.insert(iter);
    active_iters_.push(seq_pos);
  }

  TruncatedRangeDelIterator* PopActiveIter() {
    auto active_top = active_iters_.top();
    auto iter = *active_top;
    active_iters_.pop();
    active_seqnums_.erase(active_top);
    return iter;
  }

  void PushInactiveIter(TruncatedRangeDelIterator* iter) {
    inactive_iters_.push(iter);
  }

  TruncatedRangeDelIterator* PopInactiveIter() {
    auto* iter = inactive_iters_.top();
    inactive_iters_.pop();
    return iter;
  }

  const InternalKeyComparator* icmp_;
  size_t unused_idx_;
  ActiveSeqSet active_seqnums_;
  BinaryHeap<ActiveSeqSet::const_iterator, EndKeyMinComparator> active_iters_;
  BinaryHeap<TruncatedRangeDelIterator*, StartKeyMinComparator> inactive_iters_;
};

class ReverseRangeDelIterator {
 public:
  explicit ReverseRangeDelIterator(const InternalKeyComparator* icmp);

  bool ShouldDelete(const ParsedInternalKey& parsed);
  void Invalidate();

  void AddNewIter(TruncatedRangeDelIterator* iter,
                  const ParsedInternalKey& parsed) {
    iter->SeekForPrev(parsed.user_key);
    PushIter(iter, parsed);
    assert(active_iters_.size() == active_seqnums_.size());
  }

  size_t UnusedIdx() const { return unused_idx_; }
  void IncUnusedIdx() { unused_idx_++; }

 private:
  using ActiveSeqSet =
      std::multiset<TruncatedRangeDelIterator*, SeqMaxComparator>;

  struct EndKeyMaxComparator {
    explicit EndKeyMaxComparator(const InternalKeyComparator* c) : icmp(c) {}

    bool operator()(const TruncatedRangeDelIterator* a,
                    const TruncatedRangeDelIterator* b) const {
      return icmp->Compare(a->end_key(), b->end_key()) < 0;
    }

    const InternalKeyComparator* icmp;
  };
  struct StartKeyMaxComparator {
    explicit StartKeyMaxComparator(const InternalKeyComparator* c) : icmp(c) {}

    bool operator()(const ActiveSeqSet::const_iterator& a,
                    const ActiveSeqSet::const_iterator& b) const {
      return icmp->Compare((*a)->start_key(), (*b)->start_key()) < 0;
    }

    const InternalKeyComparator* icmp;
  };

  void PushIter(TruncatedRangeDelIterator* iter,
                const ParsedInternalKey& parsed) {
    if (!iter->Valid()) {
      // The iterator has been fully consumed, so we don't need to add it to
      // either of the heaps.
    } else if (icmp_->Compare(iter->end_key(), parsed) <= 0) {
      PushInactiveIter(iter);
    } else {
      PushActiveIter(iter);
    }
  }

  void PushActiveIter(TruncatedRangeDelIterator* iter) {
    auto seq_pos = active_seqnums_.insert(iter);
    active_iters_.push(seq_pos);
  }

  TruncatedRangeDelIterator* PopActiveIter() {
    auto active_top = active_iters_.top();
    auto iter = *active_top;
    active_iters_.pop();
    active_seqnums_.erase(active_top);
    return iter;
  }

  void PushInactiveIter(TruncatedRangeDelIterator* iter) {
    inactive_iters_.push(iter);
  }

  TruncatedRangeDelIterator* PopInactiveIter() {
    auto* iter = inactive_iters_.top();
    inactive_iters_.pop();
    return iter;
  }

  const InternalKeyComparator* icmp_;
  size_t unused_idx_;
  ActiveSeqSet active_seqnums_;
  BinaryHeap<ActiveSeqSet::const_iterator, StartKeyMaxComparator> active_iters_;
  BinaryHeap<TruncatedRangeDelIterator*, EndKeyMaxComparator> inactive_iters_;
};

enum class RangeDelPositioningMode { kForwardTraversal, kBackwardTraversal };
class RangeDelAggregator {
 public:
  explicit RangeDelAggregator(const InternalKeyComparator* icmp)
      : icmp_(icmp) {}
  virtual ~RangeDelAggregator() = default;

  virtual void AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
      const InternalKey* smallest = nullptr,
      const InternalKey* largest = nullptr) = 0;

  bool ShouldDelete(const Slice& ikey, RangeDelPositioningMode mode) {
    ParsedInternalKey parsed;

    Status pik_status =
        ParseInternalKey(ikey, &parsed, false /* log_err_key */);  // TODO
    assert(pik_status.ok());
    if (!pik_status.ok()) {
      return false;
    }

    return ShouldDelete(parsed, mode);
  }
  virtual bool ShouldDelete(const ParsedInternalKey& parsed,
                            RangeDelPositioningMode mode) = 0;

  virtual void InvalidateRangeDelMapPositions() = 0;

  virtual bool IsEmpty() const = 0;

  bool AddFile(uint64_t file_number) {
    return files_seen_.insert(file_number).second;
  }

 protected:
  class StripeRep {
   public:
    StripeRep(const InternalKeyComparator* icmp, SequenceNumber upper_bound,
              SequenceNumber lower_bound)
        : icmp_(icmp),
          forward_iter_(icmp),
          reverse_iter_(icmp),
          upper_bound_(upper_bound),
          lower_bound_(lower_bound) {}

    void AddTombstones(std::unique_ptr<TruncatedRangeDelIterator> input_iter) {
      iters_.push_back(std::move(input_iter));
    }

    bool IsEmpty() const { return iters_.empty(); }

    bool ShouldDelete(const ParsedInternalKey& parsed,
                      RangeDelPositioningMode mode);

    void Invalidate() {
      if (!IsEmpty()) {
        InvalidateForwardIter();
        InvalidateReverseIter();
      }
    }

    // If user-defined timestamp is enabled, `start` and `end` are user keys
    // with timestamp.
    bool IsRangeOverlapped(const Slice& start, const Slice& end,
                           bool end_exclusive);

   private:
    bool InStripe(SequenceNumber seq) const {
      return lower_bound_ <= seq && seq <= upper_bound_;
    }

    void InvalidateForwardIter() { forward_iter_.Invalidate(); }

    void InvalidateReverseIter() { reverse_iter_.Invalidate(); }

    const InternalKeyComparator* icmp_;
    std::vector<std::unique_ptr<TruncatedRangeDelIterator>> iters_;
    ForwardRangeDelIterator forward_iter_;
    ReverseRangeDelIterator reverse_iter_;
    SequenceNumber upper_bound_;
    SequenceNumber lower_bound_;
  };

  const InternalKeyComparator* icmp_;

 private:
  std::set<uint64_t> files_seen_;
};

class ReadRangeDelAggregator final : public RangeDelAggregator {
 public:
  ReadRangeDelAggregator(const InternalKeyComparator* icmp,
                         SequenceNumber upper_bound)
      : RangeDelAggregator(icmp),
        rep_(icmp, upper_bound, 0 /* lower_bound */) {}
  ~ReadRangeDelAggregator() override = default;

  using RangeDelAggregator::ShouldDelete;
  void AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
      const InternalKey* smallest = nullptr,
      const InternalKey* largest = nullptr) override;

  bool ShouldDelete(const ParsedInternalKey& parsed,
                    RangeDelPositioningMode mode) final {
    if (rep_.IsEmpty()) {
      return false;
    }
    return ShouldDeleteImpl(parsed, mode);
  }

  bool IsRangeOverlapped(const Slice& start, const Slice& end,
                         bool end_exclusive);

  void InvalidateRangeDelMapPositions() override { rep_.Invalidate(); }

  bool IsEmpty() const override { return rep_.IsEmpty(); }

 private:
  StripeRep rep_;

  bool ShouldDeleteImpl(const ParsedInternalKey& parsed,
                        RangeDelPositioningMode mode);
};

class CompactionRangeDelAggregator : public RangeDelAggregator {
 public:
  CompactionRangeDelAggregator(const InternalKeyComparator* icmp,
                               const std::vector<SequenceNumber>& snapshots,
                               const std::string* full_history_ts_low = nullptr,
                               const std::string* trim_ts = nullptr)
      : RangeDelAggregator(icmp), snapshots_(&snapshots) {
    if (full_history_ts_low) {
      ts_upper_bound_ = *full_history_ts_low;
    }
    if (trim_ts) {
      trim_ts_ = *trim_ts;
      // Range tombstone newer than `trim_ts` or `full_history_ts_low` should
      // not be considered in ShouldDelete().
      if (ts_upper_bound_.empty()) {
        ts_upper_bound_ = trim_ts_;
      } else if (!trim_ts_.empty() && icmp->user_comparator()->CompareTimestamp(
                                          trim_ts_, ts_upper_bound_) < 0) {
        ts_upper_bound_ = trim_ts_;
      }
    }
  }
  ~CompactionRangeDelAggregator() override = default;

  void AddTombstones(
      std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter,
      const InternalKey* smallest = nullptr,
      const InternalKey* largest = nullptr) override;

  using RangeDelAggregator::ShouldDelete;
  bool ShouldDelete(const ParsedInternalKey& parsed,
                    RangeDelPositioningMode mode) override;

  bool IsRangeOverlapped(const Slice& start, const Slice& end);

  void InvalidateRangeDelMapPositions() override {
    for (auto& rep : reps_) {
      rep.second.Invalidate();
    }
  }

  bool IsEmpty() const override {
    for (const auto& rep : reps_) {
      if (!rep.second.IsEmpty()) {
        return false;
      }
    }
    return true;
  }

  // Creates an iterator over all the range tombstones in the aggregator, for
  // use in compaction.
  //
  // NOTE: the internal key boundaries are used for optimization purposes to
  // reduce the number of tombstones that are passed to the fragmenter; they do
  // not guarantee that the resulting iterator only contains range tombstones
  // that cover keys in the provided range. If required, these bounds must be
  // enforced during iteration.
  std::unique_ptr<FragmentedRangeTombstoneIterator> NewIterator(
      const Slice* lower_bound = nullptr, const Slice* upper_bound = nullptr);

 private:
  std::vector<std::unique_ptr<TruncatedRangeDelIterator>> parent_iters_;
  std::map<SequenceNumber, StripeRep> reps_;

  const std::vector<SequenceNumber>* snapshots_;
  // min over full_history_ts_low and trim_ts_
  Slice ts_upper_bound_{};
  Slice trim_ts_{};
};

}  // namespace ROCKSDB_NAMESPACE
