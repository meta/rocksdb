// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

// The general interface for comparing two Slices are defined for both of
// Comparator and some internal data structures.
class CompareInterface {
 public:
  CompareInterface() = default;
  CompareInterface(const CompareInterface&) = default;
  CompareInterface& operator=(const CompareInterface&) = default;
  CompareInterface(CompareInterface&&) = default;
  CompareInterface& operator=(CompareInterface&&) = default;
  virtual ~CompareInterface() = default;

  // Three-way comparison.  Returns value:
  //   < 0 iff "a" < "b",
  //   == 0 iff "a" == "b",
  //   > 0 iff "a" > "b"
  // Note that Compare(a, b) also compares timestamp if timestamp size is
  // non-zero. For the same user key with different timestamps, larger (newer)
  // timestamp comes first.
  virtual int Compare(const Slice& a, const Slice& b) const = 0;
};

// A Comparator object provides a total order across slices that are
// used as keys in an sstable or a database.  A Comparator implementation
// must be thread-safe since rocksdb may invoke its methods concurrently
// from multiple threads.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class Comparator : public Customizable, public CompareInterface {
 public:
  Comparator() : timestamp_size_(0) {}

  Comparator(size_t ts_sz) : timestamp_size_(ts_sz) {}

  Comparator(const Comparator& orig) : timestamp_size_(orig.timestamp_size_) {}

  Comparator& operator=(const Comparator& rhs) {
    if (this != &rhs) {
      timestamp_size_ = rhs.timestamp_size_;
    }
    return *this;
  }

  Comparator(Comparator&& orig) noexcept
      : timestamp_size_(orig.timestamp_size_) {}

  Comparator& operator=(Comparator&& rhs) noexcept {
    timestamp_size_ = rhs.timestamp_size_;
    return *this;
  }

  ~Comparator() override {}

  static Status CreateFromString(const ConfigOptions& opts,
                                 const std::string& id,
                                 const Comparator** comp);
  static const char* Type() { return "Comparator"; }

  // The name of the comparator.  Used to check for comparator
  // mismatches (i.e., a DB created with one comparator is
  // accessed using a different comparator.
  //
  // The client of this package should switch to a new name whenever
  // the comparator implementation changes in a way that will cause
  // the relative ordering of any two keys to change.
  //
  // Names starting with "rocksdb." are reserved and should not be used
  // by any clients of this package.
  const char* Name() const override = 0;

  // Compares two slices for equality. The following invariant should always
  // hold (and is the default implementation):
  //   Equal(a, b) iff Compare(a, b) == 0
  // Overwrite only if equality comparisons can be done more efficiently than
  // three-way comparisons.
  virtual bool Equal(const Slice& a, const Slice& b) const {
    return Compare(a, b) == 0;
  }

  // Advanced functions: these are used to reduce the space requirements
  // for internal data structures like index blocks.

  // If *start < limit, changes *start to a short string in [start,limit).
  // Simple comparator implementations may return with *start unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const = 0;

  // Changes *key to a short string >= *key.
  // Simple comparator implementations may return with *key unchanged,
  // i.e., an implementation of this method that does nothing is correct.
  virtual void FindShortSuccessor(std::string* key) const = 0;

  // given two keys, determine if t is the successor of s
  // BUG: only return true if no other keys starting with `t` are ordered
  // before `t`. Otherwise, the auto_prefix_mode can omit entries within
  // iterator bounds that have same prefix as upper bound but different
  // prefix from seek key.
  virtual bool IsSameLengthImmediateSuccessor(const Slice& /*s*/,
                                              const Slice& /*t*/) const {
    return false;
  }

  // return true if two keys with different byte sequences can be regarded
  // as equal by this comparator.
  // The major use case is to determine if DataBlockHashIndex is compatible
  // with the customized comparator.
  virtual bool CanKeysWithDifferentByteContentsBeEqual() const { return true; }

  // if it is a wrapped comparator, may return the root one.
  // return itself it is not wrapped.
  virtual const Comparator* GetRootComparator() const { return this; }

  inline size_t timestamp_size() const { return timestamp_size_; }

  // Return what this Comparator considers as the maximum timestamp.
  // The default implementation only works for when `timestamp_size_` is 0,
  // subclasses for which this is not the case needs to override this function.
  virtual Slice GetMaxTimestamp() const {
    if (timestamp_size_ == 0) {
      return Slice();
    }
    assert(false);
    return Slice();
  }

  // Return what this Comparator considers as the min timestamp.
  // The default implementation only works for when `timestamp_size_` is 0,
  // subclasses for which this is not the case needs to override this function.
  virtual Slice GetMinTimestamp() const {
    if (timestamp_size_ == 0) {
      return Slice();
    }
    assert(false);
    return Slice();
  }

  // Return a human readable user-defined timestamp for debugging.
  virtual std::string TimestampToString(const Slice& /*timestamp*/) const {
    return "";
  }

  int CompareWithoutTimestamp(const Slice& a, const Slice& b) const {
    return CompareWithoutTimestamp(a, /*a_has_ts=*/true, b, /*b_has_ts=*/true);
  }

  // For two events e1 and e2 whose timestamps are t1 and t2 respectively,
  // Returns value:
  // < 0  iff t1 < t2
  // == 0 iff t1 == t2
  // > 0  iff t1 > t2
  // Note that an all-zero byte array will be the smallest (oldest) timestamp
  // of the same length, and a byte array with all bits 1 will be the largest.
  // In the future, we can extend Comparator so that subclasses can specify
  // both largest and smallest timestamps.
  virtual int CompareTimestamp(const Slice& /*ts1*/,
                               const Slice& /*ts2*/) const {
    return 0;
  }

  virtual int CompareWithoutTimestamp(const Slice& a, bool /*a_has_ts*/,
                                      const Slice& b, bool /*b_has_ts*/) const {
    return Compare(a, b);
  }

  virtual bool EqualWithoutTimestamp(const Slice& a, const Slice& b) const {
    return 0 ==
           CompareWithoutTimestamp(a, /*a_has_ts=*/true, b, /*b_has_ts=*/true);
  }

 private:
  size_t timestamp_size_;
};

// Return a builtin comparator that uses lexicographic ordering
// on unsigned bytes, so the empty string is ordered before everything
// else and a sufficiently long string of \xFF orders after anything.
// CanKeysWithDifferentByteContentsBeEqual() == false
// Returns an immortal pointer that must not be deleted by the caller.
const Comparator* BytewiseComparator();

// Return a builtin comparator that is the reverse ordering of
// BytewiseComparator(), so the empty string is ordered after everything
// else and a sufficiently long string of \xFF orders before anything.
// CanKeysWithDifferentByteContentsBeEqual() == false
// Returns an immortal pointer that must not be deleted by the caller.
const Comparator* ReverseBytewiseComparator();

// Returns a builtin comparator that enables user-defined timestamps (formatted
// as uint64_t) while ordering the user key part without UDT with a
// BytewiseComparator.
// For the same user key with different timestamps, larger (newer) timestamp
// comes first.
const Comparator* BytewiseComparatorWithU64Ts();

// Returns a builtin comparator that enables user-defined timestamps (formatted
// as uint64_t) while ordering the user key part without UDT with a
// ReverseBytewiseComparator.
// For the same user key with different timestamps, larger (newer) timestamp
// comes first.
const Comparator* ReverseBytewiseComparatorWithU64Ts();

// Decode a `U64Ts` timestamp returned by RocksDB to uint64_t.
// When a column family enables user-defined timestamp feature
// with `BytewiseComparatorWithU64Ts` or `ReverseBytewiseComparatorWithU64Ts`
// comparator, the `Iterator::timestamp()` API returns timestamp in `Slice`
// format. This util function helps to translate that `Slice` into an uint64_t
// type.
Status DecodeU64Ts(const Slice& ts, uint64_t* int_ts);

// Encode an uint64_t timestamp into a U64Ts `Slice`, to be used as
// `ReadOptions.timestamp` for a column family that enables user-defined
// timestamp feature with `BytewiseComparatorWithU64Ts` or
// `ReverseBytewiseComparatorWithU64Ts` comparator.
// Be mindful that the returned `Slice` is backed by `ts_buf`. When `ts_buf`
// is deconstructed, the returned `Slice` can no longer be used.
Slice EncodeU64Ts(uint64_t ts, std::string* ts_buf);

// Returns a `Slice` representing the maximum U64Ts timestamp.
// The returned `Slice` is backed by some static storage, so it's valid until
// program destruction.
Slice MaxU64Ts();

// Returns a `Slice` representing the minimum U64Ts timestamp.
// The returned `Slice` is backed by some static storage, so it's valid until
// program destruction.
Slice MinU64Ts();

}  // namespace ROCKSDB_NAMESPACE
