//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "db/multi_cf_iterator_impl.h"

namespace ROCKSDB_NAMESPACE {

class CoalescingIterator : public Iterator {
 public:
  CoalescingIterator(
      const ReadOptions& read_options, const Comparator* comparator,
      std::vector<std::pair<ColumnFamilyHandle*, std::unique_ptr<Iterator>>>&&
          cfh_iter_pairs)
      : impl_(read_options, comparator, std::move(cfh_iter_pairs),
              ResetFunc(this), PopulateFunc(this)) {}
  ~CoalescingIterator() override {}

  // No copy allowed
  CoalescingIterator(const CoalescingIterator&) = delete;
  CoalescingIterator& operator=(const CoalescingIterator&) = delete;
  CoalescingIterator(CoalescingIterator&&) = delete;
  CoalescingIterator& operator=(CoalescingIterator&&) = delete;

  bool Valid() const override { return impl_.Valid(); }
  void SeekToFirst() override { impl_.SeekToFirst(); }
  void SeekToLast() override { impl_.SeekToLast(); }
  void Seek(const Slice& target) override { impl_.Seek(target); }
  void SeekForPrev(const Slice& target) override { impl_.SeekForPrev(target); }
  void Next() override { impl_.Next(); }
  void Prev() override { impl_.Prev(); }
  Slice key() const override { return impl_.key(); }
  Status status() const override { return impl_.status(); }

  Slice value() const override {
    assert(Valid());
    return value_;
  }
  const WideColumns& columns() const override {
    assert(Valid());
    return wide_columns_;
  }

  void Reset() {
    value_.clear();
    wide_columns_.clear();
    owned_columns_.clear();
  }

  bool PrepareValue() override { return impl_.PrepareValue(); }

 private:
  class ResetFunc {
   public:
    explicit ResetFunc(CoalescingIterator* iter) : iter_(iter) {}

    void operator()() const {
      assert(iter_);
      iter_->Reset();
    }

   private:
    CoalescingIterator* iter_;
  };

  class PopulateFunc {
   public:
    explicit PopulateFunc(CoalescingIterator* iter) : iter_(iter) {}

    void operator()(const autovector<MultiCfIteratorInfo>& items) const {
      assert(iter_);
      iter_->Coalesce(items);
    }

   private:
    CoalescingIterator* iter_;
  };

  MultiCfIteratorImpl<ResetFunc, PopulateFunc> impl_;
  Slice value_;
  WideColumns wide_columns_;
  std::vector<std::pair<std::string, std::string>> owned_columns_;

  struct WideColumnWithOrder {
    const WideColumn* column;
    int order;
  };

  class WideColumnWithOrderComparator {
   public:
    explicit WideColumnWithOrderComparator() {}
    bool operator()(const WideColumnWithOrder& a,
                    const WideColumnWithOrder& b) const {
      int c = a.column->name().compare(b.column->name());
      return c == 0 ? a.order - b.order > 0 : c > 0;
    }
  };

  using MinHeap =
      BinaryHeap<WideColumnWithOrder, WideColumnWithOrderComparator>;

  void Coalesce(const autovector<MultiCfIteratorInfo>& items);
};

}  // namespace ROCKSDB_NAMESPACE
