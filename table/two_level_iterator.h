//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "table/iterator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

struct ReadOptions;
class InternalKeyComparator;

// TwoLevelIteratorState expects iterators are not created using the arena
struct TwoLevelIteratorState {
  TwoLevelIteratorState() {}

  virtual ~TwoLevelIteratorState() = default;
  virtual InternalIteratorBase<IndexValue>* NewSecondaryIterator(
      const BlockHandle& handle) = 0;
};

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
// Note: this function expects first_level_iter was not created using the arena
InternalIteratorBase<IndexValue>* NewTwoLevelIterator(
    TwoLevelIteratorState* state,
    InternalIteratorBase<IndexValue>* first_level_iter);

}  // namespace ROCKSDB_NAMESPACE
