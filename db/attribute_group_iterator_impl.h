//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/multi_cf_iterator_impl.h"
#include "rocksdb/attribute_groups.h"

namespace ROCKSDB_NAMESPACE {

class AttributeGroupIteratorImpl : public AttributeGroupIterator {
 public:
  AttributeGroupIteratorImpl(
      const ReadOptions& read_options, const Comparator* comparator,
      std::vector<std::pair<ColumnFamilyHandle*, std::unique_ptr<Iterator>>>&&
          cfh_iter_pairs)
      : impl_(read_options, comparator, std::move(cfh_iter_pairs),
              ResetFunc(this), PopulateFunc(this)) {}
  ~AttributeGroupIteratorImpl() override {}

  // No copy allowed
  AttributeGroupIteratorImpl(const AttributeGroupIteratorImpl&) = delete;
  AttributeGroupIteratorImpl& operator=(const AttributeGroupIteratorImpl&) =
      delete;
  AttributeGroupIteratorImpl(AttributeGroupIteratorImpl&&) = delete;
  AttributeGroupIteratorImpl& operator=(AttributeGroupIteratorImpl&&) = delete;

  bool Valid() const override { return impl_.Valid(); }
  void SeekToFirst() override { impl_.SeekToFirst(); }
  void SeekToLast() override { impl_.SeekToLast(); }
  void Seek(const Slice& target) override { impl_.Seek(target); }
  void SeekForPrev(const Slice& target) override { impl_.SeekForPrev(target); }
  void Next() override { impl_.Next(); }
  void Prev() override { impl_.Prev(); }
  Slice key() const override { return impl_.key(); }
  Status status() const override { return impl_.status(); }

  const IteratorAttributeGroups& attribute_groups() const override {
    assert(Valid());
    return attribute_groups_;
  }

  void Reset() { attribute_groups_.clear(); }

  bool PrepareValue() override { return impl_.PrepareValue(); }

 private:
  class ResetFunc {
   public:
    explicit ResetFunc(AttributeGroupIteratorImpl* iter) : iter_(iter) {}

    void operator()() const {
      assert(iter_);
      iter_->Reset();
    }

   private:
    AttributeGroupIteratorImpl* iter_;
  };

  class PopulateFunc {
   public:
    explicit PopulateFunc(AttributeGroupIteratorImpl* iter) : iter_(iter) {}

    void operator()(const autovector<MultiCfIteratorInfo>& items) const {
      assert(iter_);
      iter_->AddToAttributeGroups(items);
    }

   private:
    AttributeGroupIteratorImpl* iter_;
  };

  MultiCfIteratorImpl<ResetFunc, PopulateFunc> impl_;
  IteratorAttributeGroups attribute_groups_;
  void AddToAttributeGroups(const autovector<MultiCfIteratorInfo>& items);
};

class EmptyAttributeGroupIterator : public AttributeGroupIterator {
 public:
  explicit EmptyAttributeGroupIterator(const Status& s) : status_(s) {}
  bool Valid() const override { return false; }
  void Seek(const Slice& /*target*/) override {}
  void SeekForPrev(const Slice& /*target*/) override {}
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Status status() const override { return status_; }

  const IteratorAttributeGroups& attribute_groups() const override {
    return kNoIteratorAttributeGroups;
  }

 private:
  Status status_;
};

inline std::unique_ptr<AttributeGroupIterator> NewAttributeGroupErrorIterator(
    const Status& status) {
  return std::make_unique<EmptyAttributeGroupIterator>(status);
}

}  // namespace ROCKSDB_NAMESPACE
