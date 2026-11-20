//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <algorithm>
#include <array>
#include <string>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/merge_context.h"
#include "db/read_callback.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/types.h"
#include "util/async_file_reader.h"
#include "util/autovector.h"
#include "util/math.h"
#include "util/single_thread_executor.h"

namespace ROCKSDB_NAMESPACE {
class GetContext;
class PinnableWideColumns;

struct KeyContext {
  const Slice* key;
  LookupKey* lkey;
  Slice ukey_with_ts;
  Slice ukey_without_ts;
  Slice ikey;
  ColumnFamilyHandle* column_family;
  Status* s;
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq;
  bool key_exists;
  bool is_blob_index;
  bool newer_version_present;
  void* cb_arg;
  PinnableSlice* value;
  PinnableWideColumns* columns;
  std::string* timestamp;
  GetContext* get_context;

  KeyContext(ColumnFamilyHandle* col_family, const Slice& user_key,
             PinnableSlice* val, PinnableWideColumns* cols, std::string* ts,
             Status* stat)
      : key(&user_key),
        lkey(nullptr),
        column_family(col_family),
        s(stat),
        max_covering_tombstone_seq(0),
        key_exists(false),
        is_blob_index(false),
        newer_version_present(false),
        cb_arg(nullptr),
        value(val),
        columns(cols),
        timestamp(ts),
        get_context(nullptr) {}
};

// The MultiGetContext class is a container for the sorted list of keys that
// we need to lookup in a batch. Its main purpose is to make batch execution
// easier by allowing various stages of the MultiGet lookups to operate on
// subsets of keys, potentially non-contiguous. In order to accomplish this,
// it defines the following classes -
//
// MultiGetContext::Range
// MultiGetContext::Range::Iterator
// MultiGetContext::Range::IteratorWrapper
//
// Here is an example of how this can be used -
//
// {
//    MultiGetContext ctx(...);
//    MultiGetContext::Range range = ctx.GetMultiGetRange();
//
//    // Iterate to determine some subset of the keys
//    MultiGetContext::Range::Iterator start = range.begin();
//    MultiGetContext::Range::Iterator end = ...;
//
//    // Make a new range with a subset of keys
//    MultiGetContext::Range subrange(range, start, end);
//
//    // Define an auxillary vector, if needed, to hold additional data for
//    // each key
//    std::array<Foo, MultiGetContext::MAX_BATCH_SIZE> aux;
//
//    // Iterate over the subrange and the auxillary vector simultaneously
//    MultiGetContext::Range::Iterator iter = subrange.begin();
//    for (; iter != subrange.end(); ++iter) {
//      KeyContext& key = *iter;
//      Foo& aux_key = aux_iter[iter.index()];
//      ...
//    }
//  }
class MultiGetContext {
 public:
  // Limit the number of keys in a batch to this number. Benchmarks show that
  // there is negligible benefit for batches exceeding this. Keeping this < 32
  // simplifies iteration, as well as reduces the amount of stack allocations
  // that need to be performed
  static const int MAX_BATCH_SIZE = 32;

  // A bitmask of at least MAX_BATCH_SIZE - 1 bits, so that
  // Mask{1} << MAX_BATCH_SIZE is well defined
  using Mask = uint64_t;
  static_assert(MAX_BATCH_SIZE < sizeof(Mask) * 8);

  MultiGetContext(autovector<KeyContext*, MAX_BATCH_SIZE>* sorted_keys,
                  size_t begin, size_t num_keys, SequenceNumber snapshot,
                  const ReadOptions& read_opts, FileSystem* fs,
                  Statistics* stats, bool use_coro_read = false)
      : num_keys_(num_keys),
        value_mask_(0),
        value_size_(0),
        lookup_key_ptr_(reinterpret_cast<LookupKey*>(lookup_key_stack_buf)),
        use_coro_read_(use_coro_read)
#if USE_COROUTINES
        ,
        reader_(fs, stats),
        executor_(reader_)
#endif  // USE_COROUTINES
  {
    (void)fs;
    (void)stats;
    assert(num_keys <= MAX_BATCH_SIZE);
    if (num_keys > MAX_LOOKUP_KEYS_ON_STACK) {
      lookup_key_heap_buf.reset(new char[sizeof(LookupKey) * num_keys]);
      lookup_key_ptr_ = reinterpret_cast<LookupKey*>(lookup_key_heap_buf.get());
    }

    for (size_t iter = 0;
         iter < num_keys_ && /* suppress a warning */ iter < MAX_BATCH_SIZE;
         ++iter) {
      // autovector may not be contiguous storage, so make a copy
      sorted_keys_[iter] = (*sorted_keys)[begin + iter];
      sorted_keys_[iter]->lkey = new (&lookup_key_ptr_[iter])
          LookupKey(*sorted_keys_[iter]->key, snapshot, read_opts.timestamp);
      sorted_keys_[iter]->ukey_with_ts = sorted_keys_[iter]->lkey->user_key();
      sorted_keys_[iter]->ukey_without_ts = StripTimestampFromUserKey(
          sorted_keys_[iter]->lkey->user_key(),
          read_opts.timestamp == nullptr ? 0 : read_opts.timestamp->size());
      sorted_keys_[iter]->ikey = sorted_keys_[iter]->lkey->internal_key();
      sorted_keys_[iter]->timestamp = (*sorted_keys)[begin + iter]->timestamp;
      sorted_keys_[iter]->get_context =
          (*sorted_keys)[begin + iter]->get_context;
    }
  }

  // No copying allowed
  MultiGetContext(const MultiGetContext&) = delete;
  MultiGetContext& operator=(const MultiGetContext&) = delete;
  MultiGetContext(MultiGetContext&&) = delete;
  MultiGetContext& operator=(MultiGetContext&&) = delete;

  ~MultiGetContext() {
    for (size_t i = 0; i < num_keys_; ++i) {
      lookup_key_ptr_[i].~LookupKey();
    }
  }

#if USE_COROUTINES
  SingleThreadExecutor& executor() { return executor_; }

  AsyncFileReader& reader() { return reader_; }
#endif  // USE_COROUTINES

  bool use_coro_read() const { return use_coro_read_; }

 private:
  static const int MAX_LOOKUP_KEYS_ON_STACK = 16;
  alignas(
      alignof(LookupKey)) char lookup_key_stack_buf[sizeof(LookupKey) *
                                                    MAX_LOOKUP_KEYS_ON_STACK];
  std::array<KeyContext*, MAX_BATCH_SIZE> sorted_keys_;
  size_t num_keys_;
  Mask value_mask_;
  uint64_t value_size_;
  std::unique_ptr<char[]> lookup_key_heap_buf;
  LookupKey* lookup_key_ptr_;
  bool use_coro_read_ = false;
#if USE_COROUTINES
  AsyncFileReader reader_;
  SingleThreadExecutor executor_;
#endif  // USE_COROUTINES

 public:
  // MultiGetContext::Range - Specifies a range of keys, by start and end index,
  // from the parent MultiGetContext. Each range contains a bit vector that
  // indicates whether the corresponding keys need to be processed or skipped.
  // A Range object can be copy constructed, and the new object inherits the
  // original Range's bit vector. This is useful for progressively skipping
  // keys as the lookup goes through various stages. For example, when looking
  // up keys in the same SST file, a Range is created excluding keys not
  // belonging to that file. A new Range is then copy constructed and individual
  // keys are skipped based on bloom filter lookup.
  class Range {
   public:
    // MultiGetContext::Range::Iterator - A forward iterator that iterates over
    // non-skippable keys in a Range, as well as keys whose final value has been
    // found. The latter is tracked by MultiGetContext::value_mask_
    class Iterator {
     public:
      // -- iterator traits
      using self_type = Iterator;
      using value_type = KeyContext;
      using reference = KeyContext&;
      using pointer = KeyContext*;
      using difference_type = int;
      using iterator_category = std::forward_iterator_tag;

      Iterator(const Range* range, size_t idx)
          : range_(range), ctx_(range->ctx_), index_(idx) {
        while (index_ < range_->end_ &&
               (Mask{1} << index_) &
                   (range_->ctx_->value_mask_ | range_->skip_mask_ |
                    range_->invalid_mask_)) {
          index_++;
        }
      }

      Iterator(const Iterator&) = default;

      Iterator(const Iterator& other, const Range* range)
          : range_(range), ctx_(other.ctx_), index_(other.index_) {
        assert(range->ctx_ == other.ctx_);
      }
      Iterator& operator=(const Iterator&) = default;
      Iterator(Iterator&&) = default;
      Iterator& operator=(Iterator&&) = default;
      ~Iterator() = default;

      Iterator& operator++() {
        while (++index_ < range_->end_ &&
               (Mask{1} << index_) &
                   (range_->ctx_->value_mask_ | range_->skip_mask_ |
                    range_->invalid_mask_)) {
          // empty loop body
        }
        return *this;
      }

      bool operator==(Iterator other) const {
        assert(range_->ctx_ == other.range_->ctx_);
        return index_ == other.index_;
      }

      bool operator!=(Iterator other) const {
        assert(range_->ctx_ == other.range_->ctx_);
        return index_ != other.index_;
      }

      KeyContext& operator*() {
        assert(index_ < range_->end_ && index_ >= range_->start_);
        return *(ctx_->sorted_keys_[index_]);
      }

      KeyContext* operator->() {
        assert(index_ < range_->end_ && index_ >= range_->start_);
        return ctx_->sorted_keys_[index_];
      }

      size_t index() { return index_; }

     private:
      friend Range;
      const Range* range_;
      const MultiGetContext* ctx_;
      size_t index_;
    };

    Range(const Range& mget_range, const Iterator& first,
          const Iterator& last) {
      ctx_ = mget_range.ctx_;
      if (first == last) {
        // This means create an empty range based on mget_range. So just
        // set start_ and and_ to the same value
        start_ = mget_range.start_;
        end_ = start_;
      } else {
        start_ = first.index_;
        end_ = last.index_;
      }
      skip_mask_ = mget_range.skip_mask_;
      invalid_mask_ = mget_range.invalid_mask_;
      assert(start_ < 64);
      assert(end_ < 64);
    }

    Range() = default;

    Iterator begin() const { return Iterator(this, start_); }

    Iterator end() const { return Iterator(this, end_); }

    bool empty() const { return RemainingMask() == 0; }

    void SkipIndex(size_t index) { skip_mask_ |= Mask{1} << index; }

    void SkipKey(const Iterator& iter) { SkipIndex(iter.index_); }

    bool IsKeySkipped(const Iterator& iter) const {
      return skip_mask_ & (Mask{1} << iter.index_);
    }

    // Update the value_mask_ in MultiGetContext so its
    // immediately reflected in all the Range Iterators
    void MarkKeyDone(Iterator& iter) {
      ctx_->value_mask_ |= (Mask{1} << iter.index_);
    }

    bool CheckKeyDone(Iterator& iter) const {
      return ctx_->value_mask_ & (Mask{1} << iter.index_);
    }

    uint64_t KeysLeft() const { return BitsSetToOne(RemainingMask()); }

    void AddSkipsFrom(const Range& other) {
      assert(ctx_ == other.ctx_);
      skip_mask_ |= other.skip_mask_;
    }

    uint64_t GetValueSize() { return ctx_->value_size_; }

    void AddValueSize(uint64_t value_size) { ctx_->value_size_ += value_size; }

    MultiGetContext* context() const { return ctx_; }

    Range Suffix(const Range& other) const {
      size_t other_last = other.FindLastRemaining();
      size_t my_last = FindLastRemaining();

      if (my_last > other_last) {
        return Range(*this, Iterator(this, other_last),
                     Iterator(this, my_last));
      } else {
        return Range(*this, begin(), begin());
      }
    }

    // The += operator expands the number of keys in this range. The expansion
    // is always to the right, i.e start of the additional range >= end of
    // current range. There should be no overlap. Any skipped keys in rhs are
    // marked as invalid in the invalid_mask_.
    Range& operator+=(const Range& rhs) {
      assert(rhs.start_ >= end_);
      // Check for non-overlapping ranges and adjust invalid_mask_ accordingly
      if (end_ < rhs.start_) {
        invalid_mask_ |= RangeMask(end_, rhs.start_);
        skip_mask_ |= RangeMask(end_, rhs.start_);
      }
      start_ = std::min<size_t>(start_, rhs.start_);
      end_ = std::max<size_t>(end_, rhs.end_);
      skip_mask_ |= rhs.skip_mask_ & RangeMask(rhs.start_, rhs.end_);
      invalid_mask_ |= (rhs.invalid_mask_ | rhs.skip_mask_) &
                       RangeMask(rhs.start_, rhs.end_);
      assert(start_ < 64);
      assert(end_ < 64);
      return *this;
    }

    // The -= operator removes keys from this range. The removed keys should
    // come from a range completely overlapping the current range. The removed
    // keys are marked invalid in the invalid_mask_.
    Range& operator-=(const Range& rhs) {
      assert(start_ <= rhs.start_ && end_ >= rhs.end_);
      skip_mask_ |= (~rhs.skip_mask_ | rhs.invalid_mask_) &
                    RangeMask(rhs.start_, rhs.end_);
      invalid_mask_ |= (~rhs.skip_mask_ | rhs.invalid_mask_) &
                       RangeMask(rhs.start_, rhs.end_);
      return *this;
    }

    // Return a complement of the current range
    Range operator~() {
      Range res = *this;
      res.skip_mask_ = ~skip_mask_ & RangeMask(start_, end_);
      return res;
    }

   private:
    friend MultiGetContext;
    MultiGetContext* ctx_;
    size_t start_;
    size_t end_;
    Mask skip_mask_;
    Mask invalid_mask_;

    Range(MultiGetContext* ctx, size_t num_keys)
        : ctx_(ctx),
          start_(0),
          end_(num_keys),
          skip_mask_(0),
          invalid_mask_(0) {
      assert(num_keys < 64);
    }

    static Mask RangeMask(size_t start, size_t end) {
      return (((Mask{1} << (end - start)) - 1) << start);
    }

    Mask RemainingMask() const {
      return (((Mask{1} << end_) - 1) & ~((Mask{1} << start_) - 1) &
              ~(ctx_->value_mask_ | skip_mask_));
    }

    size_t FindLastRemaining() const {
      Mask mask = RemainingMask();
      size_t index = (mask >>= start_) ? start_ : 0;
      while (mask >>= 1) {
        index++;
      }
      return index;
    }
  };

  // Return the initial range that encompasses all the keys in the batch
  Range GetMultiGetRange() { return Range(this, num_keys_); }
};

}  // namespace ROCKSDB_NAMESPACE
