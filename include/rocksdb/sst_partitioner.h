// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#pragma once

#include <memory>
#include <string>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class Slice;

enum PartitionerResult : char {
  // Partitioner does not require to create new file
  kNotRequired = 0x0,
  // Partitioner is requesting forcefully to create new file
  kRequired = 0x1
  // Additional constants can be added
};

struct PartitionerRequest {
  PartitionerRequest(const Slice& prev_user_key_,
                     const Slice& current_user_key_,
                     uint64_t current_output_file_size_)
      : prev_user_key(&prev_user_key_),
        current_user_key(&current_user_key_),
        current_output_file_size(current_output_file_size_) {}
  const Slice* prev_user_key;
  const Slice* current_user_key;
  uint64_t current_output_file_size;
};

/*
 * A SstPartitioner is a generic pluggable way of defining the partition
 * of SST files. Compaction job will split the SST files on partition boundary
 * to lower the write amplification during SST file promote to higher level.
 */
class SstPartitioner {
 public:
  virtual ~SstPartitioner() = default;

  // Return the name of this partitioner.
  virtual const char* Name() const = 0;

  // Called for the start of an SST file or block. When return value is
  // present, it must be a prefix of user_key. Partitioning is requested
  // EXACTLY at the next future key that does not share that same prefix
  // with user_key. Implications for returned prefix p on recursive next
  // user key k:
  // * if k.size() < p.size(), partition is requested before k
  // * if k[0..p-1] != p, same
  // * if p.size() == 0, no further partitions are requested (odd 0-len prefix)
  // * if !p, ambiguous; must consult ShouldPartition on every key transition
  // Note that any prefix code (https://en.wikipedia.org/wiki/Prefix_code)
  // can be used with this optimization, not just fixed-size prefixes.
  //
  // Expected to be consistent with ShouldPartition for predictable
  // partitioning behavior.
  //
  // Implementing this function (built-in SstPartitionerFixedPrefix does) is
  // highly recommended for compaction CPU efficiency.
  virtual OptSlice ShouldPartitionByPrefix(const Slice& /*user_key*/) {
    // Default: must consult ShouldPartition()
    return {};
  }

  // It is called for all keys in compaction. When partitioner want to create
  // new SST file it needs to return true. It means compaction job will finish
  // current SST file where last key is "prev_user_key" parameter and start new
  // SST file where first key is "current_user_key". Returns decision if
  // partition boundary was detected and compaction should create new file.
  virtual PartitionerResult ShouldPartition(
      const PartitionerRequest& request) = 0;

  // Called with smallest and largest keys in SST file when compaction try to do
  // trivial move. Returns true is partitioner allows to do trivial move.
  virtual bool CanDoTrivialMove(const Slice& smallest_user_key,
                                const Slice& largest_user_key) = 0;

  // Context information of a compaction run
  struct Context {
    // Does this compaction run include all data files
    bool is_full_compaction = false;
    // Is this compaction requested by the client (true),
    // or is it occurring as an automatic compaction process
    bool is_manual_compaction = false;
    // Output level for this compaction
    int output_level = 0;
    // Smallest key for compaction
    Slice smallest_user_key;
    // Largest key for compaction
    Slice largest_user_key;
  };
};

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class SstPartitionerFactory : public Customizable {
 public:
  ~SstPartitionerFactory() override = default;
  static const char* Type() { return "SstPartitionerFactory"; }
  static Status CreateFromString(
      const ConfigOptions& options, const std::string& value,
      std::shared_ptr<SstPartitionerFactory>* result);

  virtual std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& context) const = 0;

  // Returns a name that identifies this partitioner factory.
  const char* Name() const override = 0;
};

/*
 * Fixed key prefix partitioner. It splits the output SST files when prefix
 * defined by size changes.
 */
class SstPartitionerFixedPrefix : public SstPartitioner {
 public:
  explicit SstPartitionerFixedPrefix(size_t len) : len_(len) {}

  ~SstPartitionerFixedPrefix() override = default;

  const char* Name() const override { return "SstPartitionerFixedPrefix"; }

  OptSlice ShouldPartitionByPrefix(const Slice& user_key) override;

  PartitionerResult ShouldPartition(const PartitionerRequest& request) override;

  bool CanDoTrivialMove(const Slice& smallest_user_key,
                        const Slice& largest_user_key) override;

 private:
  size_t len_;
};

/*
 * Factory for fixed prefix partitioner.
 */
class SstPartitionerFixedPrefixFactory : public SstPartitionerFactory {
 public:
  explicit SstPartitionerFixedPrefixFactory(size_t len);

  ~SstPartitionerFixedPrefixFactory() override = default;

  static const char* kClassName() { return "SstPartitionerFixedPrefixFactory"; }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override;

 private:
  size_t len_;
};

std::shared_ptr<SstPartitionerFactory> NewSstPartitionerFixedPrefixFactory(
    size_t prefix_len);

}  // namespace ROCKSDB_NAMESPACE
