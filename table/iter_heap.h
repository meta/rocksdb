//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include "db/dbformat.h"
#include "table/iterator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

// When used with std::priority_queue, this comparison functor puts the
// iterator with the max/largest key on top.
class MaxIteratorComparator {
 public:
  explicit MaxIteratorComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) const {
    return comparator_->Compare(a->key(), b->key()) < 0;
  }

 private:
  const InternalKeyComparator* comparator_;
};

// When used with std::priority_queue, this comparison functor puts the
// iterator with the min/smallest key on top.
class MinIteratorComparator {
 public:
  explicit MinIteratorComparator(const InternalKeyComparator* comparator)
      : comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) const {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }

 private:
  const InternalKeyComparator* comparator_;
};

}  // namespace ROCKSDB_NAMESPACE
