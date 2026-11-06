//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/iterator.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {
// An iterator wrapper class used to wrap an `InternalIterator` created by API
// `TableReader::NewIterator`. The `InternalIterator` should be allocated with
// the default allocator, not on an arena.
// NOTE: Callers should ensure the wrapped `InternalIterator*` is a valid
// pointer before constructing a `TableIterator` with it.
class TableIterator : public Iterator {
  void reset(InternalIterator* iter, const Slice* from_key, const Slice* to_key,
             const InternalKeyComparator* internalKeyComparator) noexcept {
    if (iter_ != nullptr) {
      delete iter_;
    }
    iter_ = iter;
    from_key_ = from_key;
    to_key_ = to_key;
    internal_key_comparator_ = internalKeyComparator;
  }

 public:
  explicit TableIterator(InternalIterator* iter, const Slice* from_key,
                         const Slice* to_key, const InternalKeyComparator* internalKeyComparator)
                         : iter_(iter),
                         from_key_(from_key),
                         to_key_(to_key),
                         internal_key_comparator_(internalKeyComparator) {

  }

  TableIterator(const TableIterator&) = delete;
  TableIterator& operator=(const TableIterator&) = delete;

  TableIterator(TableIterator&& o) noexcept {
    iter_ = o.iter_;
    from_key_ = o.from_key_;
    to_key_ = o.to_key_;
    internal_key_comparator_ = o.internal_key_comparator_;
    o.iter_ = nullptr;
    o.to_key_ = nullptr;
    o.from_key_ = nullptr;
    o.internal_key_comparator_ = nullptr;
  }

  TableIterator& operator=(TableIterator&& o) noexcept {
    reset(o.iter_, o.from_key_, o.to_key_, o.internal_key_comparator_);
    o.iter_ = nullptr;
    o.from_key_ = nullptr;
    o.to_key_ = nullptr;
    o.internal_key_comparator_ = nullptr;
    return *this;
  }

  InternalIterator* operator->() { return iter_; }
  InternalIterator* get() { return iter_; }

  ~TableIterator() override { reset(nullptr, nullptr, nullptr, nullptr); }

  bool Valid() const override {
    return iter_->Valid() &&
    (!has_from_key_() || internal_key_comparator_->Compare(*from_key_, key()) <= 0) &&
    (!has_to_key_() || internal_key_comparator_->Compare(*to_key_, key()) > 0);
  }

  void SeekToFirst() override {
    if (has_from_key_()) {
      return Seek(*from_key_);
    } else {
      return iter_->SeekToFirst();
    }
  }

  void SeekToLast() override {
    if (has_to_key_()) {
      SeekForPrev(*to_key_);
      if (iter_->Valid() && internal_key_comparator_->Compare(*to_key_, key()) == 0) {
        Prev();
      }
    } else {
      return iter_->SeekToLast();
    }
  }

  void Seek(const Slice& target) override {
    return iter_->Seek(target);
  }
  void SeekForPrev(const Slice& target) override {
    return iter_->SeekForPrev(target);
  }
  void Next() override { return iter_->Next(); }
  void Prev() override { return iter_->Prev(); }
  Slice key() const override { return iter_->key(); }
  Slice value() const override { return iter_->value(); }
  Status status() const override { return iter_->status(); }
  Status GetProperty(std::string /*prop_name*/,
                     std::string* /*prop*/) override {
    assert(false);
    return Status::NotSupported("TableIterator does not support GetProperty.");
  }

 private:
  InternalIterator* iter_;
  const Slice* from_key_;
  const Slice* to_key_;
  const InternalKeyComparator* internal_key_comparator_;

  bool has_from_key_() const {
    return from_key_ != nullptr;
  }

  bool has_to_key_() const {
    return to_key_ != nullptr;
  }
};
}  // namespace ROCKSDB_NAMESPACE
