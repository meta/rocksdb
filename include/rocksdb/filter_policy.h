// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in rocksdb and are consulted
// automatically by rocksdb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB::Get() call.
//
// Most people will want to use the builtin bloom filter support (see
// NewBloomFilterPolicy() below).

#pragma once

#include <stdlib.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "rocksdb/advanced_options.h"
#include "rocksdb/customizable.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/types.h"

namespace ROCKSDB_NAMESPACE {

struct BlockBasedTableOptions;
struct ConfigOptions;

// As of RocksDB 7.0, the details of these classes are internal
class FilterBitsBuilder;
class FilterBitsReader;

// Contextual information passed to BloomFilterPolicy at filter building time.
// Used in overriding FilterPolicy::GetBuilderWithContext(). References other
// structs because this is expected to be a temporary, stack-allocated object.
struct FilterBuildingContext {
  // This constructor is for internal use only and subject to change.
  // Keeps a reference to table_options.
  explicit FilterBuildingContext(const BlockBasedTableOptions& table_options);

  // Options for the table being built
  const BlockBasedTableOptions& table_options;

  // BEGIN from (DB|ColumnFamily)Options in effect at table creation time
  CompactionStyle compaction_style = kCompactionStyleLevel;

  // Number of LSM levels, or -1 if unknown
  int num_levels = -1;

  // An optional logger for reporting errors, warnings, etc.
  Logger* info_log = nullptr;
  // END from (DB|ColumnFamily)Options

  // Name of the column family for the table (or empty string if unknown)
  // TODO: consider changing to Slice
  std::string column_family_name;

  // Name (path) of the DB the table is being built for, or empty if there is
  // no such DB, as with SstFileWriter. Under remote compaction this is the DB
  // name as opened by the worker, which can differ from the primary's.
  // Non-owning; do not retain beyond the call receiving this context.
  Slice db_name;

  // The table level at time of constructing the SST file, or -1 if unknown
  // or N/A as in SstFileWriter. (The table file could later be used at a
  // different level.)
  int level_at_creation = -1;

  // True if known to be going into bottommost sorted run for applicable
  // key range (which might not even be last level with data). False
  // otherwise.
  bool is_bottommost = false;

  // Reason for creating the file with the filter
  TableFileCreationReason reason = TableFileCreationReason::kMisc;

  // True if the file is being built by a remote (offloaded) compaction worker
  // via CompactionService. False otherwise (including local/fallback
  // compactions, flushes, and SstFileWriter).
  bool is_remote_compaction = false;
};

// Determines what kind of filter (if any) to generate in SST files, and under
// which conditions. API users can create custom filter policies that
// defer to other built-in policies (see NewBloomFilterPolicy and
// NewRibbonFilterPolicy) based on the context provided to
// GetBuilderWithContext.
class FilterPolicy : public Customizable {
 public:
  ~FilterPolicy() override;
  static const char* Type() { return "FilterPolicy"; }

  // The name used for identifying whether a filter on disk is readable
  // by this FilterPolicy. If this FilterPolicy is part of a family that
  // can read each others filters, such as built-in BloomFilterPolcy and
  // RibbonFilterPolicy, the CompatibilityName is a shared family name,
  // while kinds of filters in the family can have distinct Customizable
  // Names. This function is pure virtual so that wrappers around built-in
  // policies are prompted to defer to CompatibilityName() of the wrapped
  // policy, which is important for compatibility.
  //
  // For custom filter policies that are not part of a read-compatible
  // family (rare), implementations may return Name().
  virtual const char* CompatibilityName() const = 0;

  // Creates a new FilterPolicy based on the input value string and returns the
  // result The value might be an ID, and ID with properties, or an old-style
  // policy string.
  // The value describes the FilterPolicy being created.
  // For BloomFilters, value may be a ":"-delimited value of the form:
  //   "bloomfilter:[bits_per_key]",
  //   e.g. ""bloomfilter:4"
  //   The above string is equivalent to calling NewBloomFilterPolicy(4).
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<const FilterPolicy>* result);

  // Return a new FilterBitsBuilder for constructing full or partitioned
  // filter blocks, or return nullptr to indicate "no filter". Custom
  // implementations should defer to a built-in FilterPolicy to get a
  // new FilterBitsBuilder, but the FilterBuildingContext can be used
  // to decide which built-in FilterPolicy to defer to.
  virtual FilterBitsBuilder* GetBuilderWithContext(
      const FilterBuildingContext&) const = 0;

  // Return a new FilterBitsReader for full or partitioned filter blocks.
  // Caller retains ownership of any buffer pointed to by the input Slice.
  // Custom implementation should defer to GetFilterBitsReader on any
  // built-in FilterPolicy, which can read filters generated by any other
  // built-in FilterPolicy.
  virtual FilterBitsReader* GetFilterBitsReader(
      const Slice& /*contents*/) const = 0;
};

// Return a new filter policy that uses a bloom filter with approximately
// the specified number of bits per key. See
// https://github.com/meta/rocksdb/wiki/RocksDB-Bloom-Filter
//
// bits_per_key: average bits allocated per key in bloom filter. A good
// choice is 9.9, which yields a filter with ~ 1% false positive rate.
// When format_version < 5, the value will be rounded to the nearest
// integer. Recommend using no more than three decimal digits after the
// decimal point, as in 6.667.
//
// To avoid configurations that are unlikely to produce good filtering
// value for the CPU overhead, bits_per_key < 0.5 is rounded down to 0.0
// which means "generate no filter", and 0.5 <= bits_per_key < 1.0 is
// rounded up to 1.0, for a 62% FP rate.
//
// The caller is responsible for eventually deleting the result, though
// this is typically handled automatically with BlockBasedTableOptions:
//   table_options.filter_policy.reset(NewBloomFilterPolicy(...));
//
// As of RocksDB 7.0, the use_block_based_builder parameter is ignored.
// (The old, inefficient block-based filter is no longer accessible in
// the public API.)
//
// Note: if you are using a custom comparator that ignores some parts
// of the keys being compared, you must not use NewBloomFilterPolicy()
// and must provide your own FilterPolicy that also ignores the
// corresponding parts of the keys.  For example, if the comparator
// ignores trailing spaces, it would be incorrect to use a
// FilterPolicy (like NewBloomFilterPolicy) that does not ignore
// trailing spaces in keys.
const FilterPolicy* NewBloomFilterPolicy(
    double bits_per_key, bool IGNORED_use_block_based_builder = false);

// A new Bloom alternative that saves about 30% space compared to
// Bloom filters, with similar query times but roughly 3-4x CPU time
// and 3x temporary space usage during construction.  For example, if
// you pass in 10 for bloom_equivalent_bits_per_key, you'll get the same
// 0.95% FP rate as Bloom filter but only using about 7 bits per key.
//
// The space savings of Ribbon filters makes sense for lower (higher
// numbered; larger; longer-lived) levels of LSM, whereas the speed of
// Bloom filters make sense for highest levels of LSM. Setting
// bloom_before_level allows for this design with Level and Universal
// compaction styles. For example, bloom_before_level=1 means that Bloom
// filters will be used in level 0, including flushes, and Ribbon
// filters elsewhere, including FIFO compaction and external SST files.
// For this option, memtable flushes are considered level -1 (so that
// flushes can be distinguished from intra-L0 compaction).
// bloom_before_level=0 (default) -> Generate Bloom filters only for
// flushes under Level and Universal compaction styles.
// bloom_before_level=-1 -> Always generate Ribbon filters (except in
// some extreme or exceptional cases).
// bloom_before_level=INT_MAX -> Always generate Bloom filters.
//
// The bloom_before_level option is mutable in the Configurable interface
// and through the SetOptions() API, as in
// db->SetOptions({{"table_factory.filter_policy.bloom_before_level", "3"}});
//
// Ribbon filters are compatible with RocksDB >= 6.15.0. Earlier
// versions reading the data will behave as if no filter was used
// (degraded performance until compaction rebuilds filters). All
// built-in FilterPolicies (Bloom or Ribbon) are able to read other
// kinds of built-in filters.
//
// Note: the current Ribbon filter schema uses some extra resources
// when constructing very large filters. For example, for 100 million
// keys in a single filter (one SST file without partitioned filters),
// 3GB of temporary, untracked memory is used, vs. 1GB for Bloom.
// However, the savings in filter space from just ~60 open SST files
// makes up for the additional temporary memory use.
//
// Also consider using optimize_filters_for_memory to save filter
// memory.
FilterPolicy* NewRibbonFilterPolicy(double bloom_equivalent_bits_per_key,
                                    int bloom_before_level = 0);

}  // namespace ROCKSDB_NAMESPACE
