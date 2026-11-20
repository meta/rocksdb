// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Class for specifying user-defined functions which perform a
// transformation on a slice.  It is not required that every slice
// belong to the domain of a function.  Subclasses should
// define InDomain to determine which slices are in this set.

#pragma once

#include <memory>
#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
struct ConfigOptions;

// A SliceTransform is a generic pluggable way of transforming one string
// to another. Its primary use-case is in configuring RocksDB prefix Bloom
// filters, by setting prefix_extractor in ColumnFamilyOptions.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class SliceTransform : public Customizable {
 public:
  ~SliceTransform() override = default;

  // Return the name of this transformation.
  const char* Name() const override = 0;
  static const char* Type() { return "SliceTransform"; }

  // Creates and configures a new SliceTransform from the input options and id.
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& id,
                                 std::shared_ptr<const SliceTransform>* result);

  // Returns a string representation of this SliceTransform, representing the ID
  // and any additional properties.
  std::string AsString() const;

  // Extract a prefix from a specified key, partial key, iterator upper bound,
  // etc. This is normally used for building and checking prefix Bloom filters
  // but should accept any string for which InDomain() returns true.
  // See ColumnFamilyOptions::prefix_extractor for specific properties that
  // must be satisfied by prefix extractors.
  virtual Slice Transform(const Slice& key) const = 0;

  // Determine whether the specified key is compatible with the logic
  // specified in the Transform method. Keys for which InDomain returns
  // false will not be added to or queried against prefix Bloom filters.
  //
  // For example, if the Transform method returns a fixed length
  // prefix of size 4, then an invocation to InDomain("abc") returns
  // false because the specified key length(3) is shorter than the
  // prefix size of 4.
  //
  // Wiki documentation here:
  // https://github.com/meta/rocksdb/wiki/Prefix-Seek
  //
  virtual bool InDomain(const Slice& key) const = 0;

  // Returns information on maximum prefix length, if there is one.
  // If Transform(x).size() == n for some keys and otherwise < n,
  // should return true and set *len = n. Returning false is safe but
  // currently disables some auto_prefix_mode filtering.
  // Specifically, if the iterate_upper_bound is the immediate successor (see
  // Comparator::IsSameLengthImmediateSuccessor) of the seek key's prefix,
  // we require this function return true and iterate_upper_bound.size() == n
  // to recognize and optimize the prefix seek.
  // Otherwise (including FullLengthEnabled returns false, or prefix length is
  // less than maximum), Seek with auto_prefix_mode is only optimized if the
  // iterate_upper_bound and seek key have the same prefix.
  // BUG: Despite all these conditions and even with the extra condition on
  // IsSameLengthImmediateSuccessor (see it's "BUG" section), it is not
  // sufficient to ensure auto_prefix_mode returns all entries that
  // total_order_seek would return. See auto_prefix_mode "BUG" section.
  virtual bool FullLengthEnabled(size_t* /*len*/) const { return false; }

  // Transform(s)=Transform(`prefix`) for any s with `prefix` as a prefix.
  //
  // This function is not used by RocksDB, but for users. If users pass
  // Options by string to RocksDB, they might not know what prefix extractor
  // they are using. This function is to help users can determine:
  //   if they want to iterate all keys prefixing `prefix`, whether it is
  //   safe to use prefix bloom filter and seek to key `prefix`.
  // If this function returns true, this means a user can Seek() to a prefix
  // using the bloom filter. Otherwise, user needs to skip the bloom filter
  // by setting ReadOptions.total_order_seek = true.
  //
  // Here is an example: Suppose we implement a slice transform that returns
  // the first part of the string up to and including first ",":
  // 1. SameResultWhenAppended("abc,") should return true. If applying prefix
  //    bloom filter using it, all slices matching "abc,.*" will be extracted
  //    to "abc,", so any SST file or memtable containing any of those key
  //    will not be filtered out.
  // 2. SameResultWhenAppended("abc") should return false. A user will not be
  //    guaranteed to see all the keys matching "abc.*" if a user prefix
  //    seeks to "abc" against a DB with the same setting. If one SST file
  //    only contains "abcd,e", the file can be filtered out and the key will
  //    be invisible, because the prefix according to the configured extractor
  //    is "abcd,".
  //
  // i.e., an implementation always returning false is safe.
  virtual bool SameResultWhenAppended(const Slice& /*prefix*/) const {
    return false;
  }
};

// The prefix is the first `prefix_len` bytes of the key, and keys shorter
// then `prefix_len` are not InDomain.
const SliceTransform* NewFixedPrefixTransform(size_t prefix_len);

// The prefix is the first min(length(key),`cap_len`) bytes of the key, and
// all keys are InDomain.
const SliceTransform* NewCappedPrefixTransform(size_t cap_len);

// Prefix is equal to key. All keys are InDomain.
const SliceTransform* NewNoopTransform();

}  // namespace ROCKSDB_NAMESPACE
