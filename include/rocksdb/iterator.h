// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#pragma once

#include <string>

#include "rocksdb/iterator_base.h"
#include "rocksdb/options.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Iterator : public IteratorBase {
 public:
  Iterator() {}
  // No copying allowed
  Iterator(const Iterator&) = delete;
  void operator=(const Iterator&) = delete;
  Iterator(Iterator&&) = delete;
  void operator=(Iterator&&) = delete;

  ~Iterator() override {}

  // Return the value for the current entry.  If the entry is a plain key-value,
  // return the value as-is; if it is a wide-column entity, return the value of
  // the default anonymous column (see kDefaultWideColumnName) if any, or an
  // empty value otherwise.  The underlying storage for the returned slice is
  // valid only until the next modification of the iterator (i.e. the next
  // SeekToFirst/SeekToLast/Seek/SeekForPrev/Next/Prev operation).
  // REQUIRES: Valid()
  virtual Slice value() const = 0;

  // Return the wide columns for the current entry.  If the entry is a
  // wide-column entity, return it as-is; if it is a plain key-value, return it
  // as an entity with a single anonymous column (see kDefaultWideColumnName)
  // which contains the value.  The underlying storage for the returned
  // structure is valid only until the next modification of the iterator (i.e.
  // the next SeekToFirst/SeekToLast/Seek/SeekForPrev/Next/Prev operation).
  // REQUIRES: Valid()
  virtual const WideColumns& columns() const {
    assert(false);
    return kNoWideColumns;
  }

  // Property "rocksdb.iterator.is-key-pinned":
  //   If returning "1", this means that the Slice returned by key() is valid
  //   as long as the iterator is not deleted.
  //   It is guaranteed to always return "1" if
  //      - Iterator created with ReadOptions::pin_data = true
  //      - DB tables were created with
  //        BlockBasedTableOptions::use_delta_encoding = false.
  // Property "rocksdb.iterator.is-value-pinned":
  //   If returning "1", this means that the Slice returned by value() is valid
  //   as long as the iterator is not deleted.
  //   It is guaranteed to always return "1" if
  //      - Iterator created with ReadOptions::pin_data = true
  //      - The value is found in a `kTypeValue` record
  // Property "rocksdb.iterator.super-version-number":
  //   LSM version used by the iterator. The same format as DB Property
  //   kCurrentSuperVersionNumber. See its comment for more information.
  // Property "rocksdb.iterator.internal-key":
  //   Get the user-key portion of the internal key at which the iteration
  //   stopped.
  // Property "rocksdb.iterator.write-time":
  //   Get the unix time of the best estimate of the write time of the entry.
  //   Returned as 64-bit raw value (8 bytes). It can be converted to uint64_t
  //   with util method `DecodeU64Ts`. The accuracy of the write time depends on
  //   settings like preserve_internal_time_seconds. The actual write time of
  //   the entry should be the same or newer than the returned write time. So
  //   this property can be interpreted as the possible oldest write time for
  //   the entry.
  //   If the seqno to time mapping recording is not enabled,
  //   std::numeric_limits<uint64_t>::max() will be returned to indicate the
  //   write time is unknown. For data entry whose sequence number has
  //   been zeroed out (possible when they reach the last level), 0 is returned
  //   no matter whether the seqno to time recording feature is enabled or not.
  virtual Status GetProperty(std::string prop_name, std::string* prop);

  virtual Slice timestamp() const {
    assert(false);
    return Slice();
  }

  // Prepare the iterator to scan the ranges specified in scan_opts. This
  // includes prefetching relevant blocks from disk. The upper bound and
  // other table specific limits should be specified for each
  // scan for best results. If an upper bound is not specified, Prepare may
  // skip prefetching as it cannot accurately determine how much to prefetch.
  //
  // Prepare should typically be followed by Seeks to the start keys in the
  // order they're specified in scan_opts. If the user does a Seek to some
  // other target key, the iterator should disregard the scan_opts from that
  // point onwards and behave like a normal iterator. Its the user's
  // responsibility to again call Prepare().
  //
  // For an iterator from DB::NewIterator(), Prepare() discards any current
  // position, and Valid() returns false until the iterator is positioned
  // again, e.g. by Seek().
  //
  // If Prepare() is called, it overrides the iterate_upper_bound in
  // ReadOptions
  virtual void Prepare(const MultiScanArgs& /*scan_opts*/) {}
};

// Return an empty iterator (yields nothing).
Iterator* NewEmptyIterator();

// Return an empty iterator with the specified status.
Iterator* NewErrorIterator(const Status& status);

}  // namespace ROCKSDB_NAMESPACE
