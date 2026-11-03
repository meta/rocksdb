//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "db/range_del_aggregator.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "table/iterator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class ArenaWrappedDBIter;
class InternalKeyComparator;

template <class TValue>
class InternalIteratorBase;
using InternalIterator = InternalIteratorBase<Slice>;

// Return an iterator that provides the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
InternalIterator* NewMergingIterator(const InternalKeyComparator* comparator,
                                     InternalIterator** children, int n,
                                     Arena* arena = nullptr,
                                     bool prefix_seek_mode = false);

// The iterator returned by NewMergingIterator() and
// MergeIteratorBuilder::Finish(). MergingIterator handles the merging of data
// from different point and/or range tombstone iterators.
class MergingIterator;

// A builder class to for an iterator that provides the union of data
// of input iterators. Two APIs are provided to add input iterators. User should
// only call one of them exclusively depending on if range tombstone should be
// processed.
class MergeIteratorBuilder {
 public:
  // comparator: the comparator used in merging comparator
  // arena: where the merging iterator needs to be allocated from.
  explicit MergeIteratorBuilder(const InternalKeyComparator* comparator,
                                Arena* arena, bool prefix_seek_mode = false,
                                const Slice* iterate_upper_bound = nullptr);
  ~MergeIteratorBuilder();

  // Add point key iterator `iter` to the merging iterator.
  void AddIterator(InternalIterator* iter);

  // Enable source priority for equal user-key/sequence pairs when the input
  // layout cannot rule them out. Otherwise retain internal-key type ordering.
  void SetSourcePriorityForEqualSequences(bool enabled);

  // Add a point key iterator and a range tombstone iterator.
  // `tombstone_iter_ptr` should and only be set by LevelIterator.
  // *tombstone_iter_ptr will be set to where the merging iterator stores
  // `tombstone_iter` when MergeIteratorBuilder::Finish() is called. This is
  // used by LevelIterator to update range tombstone iters when switching to a
  // different SST file. If a single point iterator with a nullptr range
  // tombstone iterator is provided, and the point iterator is not a level
  // iterator, then this builder will return the point iterator directly,
  // instead of creating a merging iterator on top of it. Internally, if all
  // point iterators are not LevelIterator, then range tombstone iterator is
  // only added to the merging iter if there is a non-null `tombstone_iter`.
  void AddPointAndTombstoneIterator(
      InternalIterator* point_iter,
      std::unique_ptr<TruncatedRangeDelIterator>&& tombstone_iter,
      std::unique_ptr<TruncatedRangeDelIterator>** tombstone_iter_ptr =
          nullptr);

  // Get arena used to build the merging iterator. It is called one a child
  // iterator needs to be allocated.
  Arena* GetArena() { return arena; }

  void SetMemtablePruned(bool memtable_pruned) {
    memtable_pruned_ = memtable_pruned;
  }

  // Return the result merging iterator.
  // If db_iter is not nullptr, then db_iter->SetMemtableRangetombstoneIter()
  // will be called with pointer to where the merging iterator
  // stores the memtable range tombstone iterator.
  // This is used for DB iterator to refresh memtable range tombstones.
  InternalIterator* Finish(ArenaWrappedDBIter* db_iter = nullptr);

 private:
  MergingIterator* merge_iter;
  InternalIterator* first_iter;
  bool use_merging_iter;
  Arena* arena;
  // Used to set LevelIterator.range_tombstone_iter_.
  // See AddRangeTombstoneIterator() implementation for more detail.
  std::vector<std::pair<size_t, std::unique_ptr<TruncatedRangeDelIterator>**>>
      range_del_iter_ptrs_;
  // if supplying multiscan ranges allowed us to prune the memtable range delete
  // iterator, we need to handle this in Finish()
  bool memtable_pruned_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
