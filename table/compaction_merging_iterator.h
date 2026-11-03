//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "db/range_del_aggregator.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "table/merging_iterator.h"

namespace ROCKSDB_NAMESPACE {

/*
 * This is a simplified version of MergingIterator and is specifically used for
 * compaction. It merges the input `children` iterators into a sorted stream of
 * keys. Range tombstone start keys are also emitted to prevent oversize
 * compactions. For example, consider an L1 file with content [a, b), y, z,
 * where [a, b) is a range tombstone and y and z are point keys. This could
 * cause an oversize compaction as it can overlap with a wide range of key space
 * in L2.
 *
 * CompactionMergingIterator emits range tombstone start keys from each LSM
 * level's range tombstone iterator, and for each range tombstone
 * [start,end)@seqno, the key will be start@seqno with op_type
 * kTypeRangeDeletion unless truncated at file boundary (see detail in
 * TruncatedRangeDelIterator::start_key()).
 *
 * Caller should use CompactionMergingIterator::IsDeleteRangeSentinelKey() to
 * check if the current key is a range tombstone key.
 * TODO(cbi): IsDeleteRangeSentinelKey() is used for two kinds of keys at
 * different layers: file boundary and range tombstone keys. Separate them into
 * two APIs for clarity.
 */
class CompactionMergingIterator;
class InternalStats;

InternalIterator* NewCompactionMergingIterator(
    const InternalKeyComparator* comparator, InternalIterator** children, int n,
    bool source_priority_for_equal_sequences,
    std::vector<std::pair<std::unique_ptr<TruncatedRangeDelIterator>,
                          std::unique_ptr<TruncatedRangeDelIterator>**>>&
        range_tombstone_iters,
    Arena* arena = nullptr, InternalStats* stats = nullptr);
}  // namespace ROCKSDB_NAMESPACE
