//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "table/block_based/data_block_hash_index.h"

namespace ROCKSDB_NAMESPACE {

class Statistics;

class BlockBuilder {
 public:
  BlockBuilder(const BlockBuilder&) = delete;
  void operator=(const BlockBuilder&) = delete;
  BlockBuilder(BlockBuilder&&) = delete;
  void operator=(BlockBuilder&&) = delete;
  ~BlockBuilder() = default;

  explicit BlockBuilder(int block_restart_interval, bool use_delta_encoding,
                        bool use_value_delta_encoding,
                        BlockBasedTableOptions::DataBlockIndexType index_type,
                        double data_block_hash_table_util_ratio, size_t ts_sz,
                        bool persist_user_defined_timestamps, bool is_user_key,
                        bool use_separated_kv_storage, Statistics* statistics,
                        double uniform_cv_threshold, bool use_common_prefix);

  // Tag for the simplified constructor below.
  struct ForMetaBlock {};

  // Simplified constructor for metadata blocks (the meta-index and properties
  // blocks): standard settings, none of the main constructor's tuning knobs
  // (statistics, uniformity, common-prefix, ...). The tag states the intent at
  // the call site and avoids a bare single-int constructor that could be
  // mistaken for a data block with silent defaults.
  BlockBuilder(ForMetaBlock, int block_restart_interval);

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // Swap the contents in BlockBuilder with buffer, then reset the BlockBuilder.
  void SwapAndReset(std::string& buffer);

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: Unless a range tombstone block, key is larger than any previously
  //           added key
  // DO NOT mix with AddWithLastKey() between Resets. For efficiency, use
  // AddWithLastKey() in contexts where previous added key is already known
  // and delta encoding might be used.
  // For efficiency, the implementation assumes the sizes of the input slices
  // are each < 4GB, and only uses the bottom 32 bits of each size. (Using a
  // dedicated Slice32 type would likely incur data movement overheads for this
  // inner-loop code.)
  void Add(const Slice& key, const Slice& value,
           const Slice& delta_value = Slice(),
           bool skip_delta_encoding = false);

  // A faster version of Add() if the previous key is already known for all
  // Add()s.
  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: Unless a range tombstone block, key is larger than any previously
  //           added key
  // REQUIRES: if AddWithLastKey has been called since last Reset(), last_key
  // is the key from most recent AddWithLastKey. (For convenience, last_key
  // is ignored on first call after creation or Reset().)
  // DO NOT mix with Add() between Resets.
  // For efficiency, the implementation assumes the sizes of the input slices
  // are each < 4GB, and only uses the bottom 32 bits of each size.
  void AddWithLastKey(const Slice& key, const Slice& value,
                      const Slice& last_key, const Slice& delta_value = Slice(),
                      bool skip_delta_encoding = false);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  inline size_t CurrentSizeEstimate() const {
    size_t est =
        estimate_ + (data_block_hash_index_builder_.Valid()
                         ? data_block_hash_index_builder_.EstimateSize()
                         : 0);
    // While building with the common-prefix feature, `estimate_` is the
    // format_version 7 size (keys encoded incrementally with the prefix still
    // present); the prefix is only stripped from restart-point keys at
    // Finish(). Subtract the prefix savings so blocks are packed close to the
    // target size. Stripping removes P bytes from each of `nr` restart keys and
    // stores the prefix once (P bytes, no length field), so the net saving is
    // exactly P * (nr - 1).
    if (use_common_prefix_ && !finishing_) {
      size_t p = first_key_prefix_.size();
      size_t nr = restarts_.size();
      size_t saved = p * (nr > 0 ? nr - 1 : 0);
      est = est > saved ? est - saved : est;
    }
    return est;
  }

  // Returns an estimated block size after appending key and value.
  size_t EstimateSizeAfterKV(const Slice& key, const Slice& value) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

  std::string& MutableBuffer() { return buffer_; }

  // Returns true if the most recently Finish()'d block was marked uniform.
  // REQUIRES: Finish() has been called.
  bool IsUniform() const { return is_uniform_; }

 private:
  inline void AddWithLastKeyImpl(const Slice& key, const Slice& value,
                                 const Slice& last_key,
                                 const Slice& delta_value,
                                 bool skip_delta_encoding, size_t buffer_size);

  // Common-prefix feature: after the block has been encoded incrementally in
  // the normal (fv4 index / fv7 data) layout, rewrite in place so that each
  // restart-point key is stored without the block's common user-key prefix,
  // which is written once in a section at the start of the block. Only restart
  // entries change; all non-restart entry bytes (and all values) are copied
  // verbatim. Handles both value-delta (index) and non-value-delta layouts.
  // REQUIRES: first_key_prefix_ non-empty and buffer_ non-empty.
  void RewriteRestartKeysStrippingPrefix();

  bool ScanForUniformity() const;

  Slice GetRestartKey(uint32_t index, const char* limit) const;

  // Returns key with timestamp stripped if applicable.
  // For efficiency and internal consistency, only uses the bottom 32 bits of
  // the key size (see API comments on Add()).
  inline const Slice MaybeStripTimestampFromKey(std::string* key_buf,
                                                const Slice& key);

  const int block_restart_interval_;
  // TODO(myabandeh): put it into a separate IndexBlockBuilder
  const bool use_delta_encoding_;
  // Refer to BlockIter::DecodeCurrentValue for format of delta encoded values
  const bool use_value_delta_encoding_;
  // Size in bytes for the user-defined timestamp to strip in a user key.
  // This is non-zero if there is user-defined timestamp in the user key and it
  // should not be persisted.
  const size_t strip_ts_sz_;
  // Whether the keys provided to build this block are user keys. If not,
  // the keys are internal keys. This will affect how timestamp stripping is
  // done for the key if `persisted_user_defined_timestamps_` is false and
  // `ts_sz_` is non-zero.
  // The timestamp stripping only applies to the keys added to the block. If the
  // value contains user defined timestamp that needed to be stripped too, such
  // as the `first_internal_key` in an `IndexValue` for an index block, the
  // value part for a range deletion entry, their timestamp should be stripped
  // before calling `BlockBuilder::Add`.
  // Timestamp stripping only applies to data block and index blocks including
  // index block for data blocks, index block for partitioned filter blocks,
  // index block for partitioned index blocks. In summary, this only applies to
  // block whose key are real user keys or internal keys created from user keys.
  const bool is_user_key_;

  std::string buffer_;              // Destination buffer
  std::vector<uint32_t> restarts_;  // Restart points
  size_t estimate_;
  int counter_;      // Number of entries emitted since restart
  bool finished_;    // Has Finish() been called?
  bool is_uniform_;  // Was the last Finish()'d block uniform?
  std::string last_key_;
  DataBlockHashIndexBuilder data_block_hash_index_builder_;
  const double uniform_cv_threshold_;
  Statistics* statistics_;

  const bool use_separated_kv_storage_;  // When enabled, keys are stored first,
                                         // followed by values in a separate
                                         // section. Value offset is stored as
                                         // varint only at restart points; for
                                         // other entries, offset is computed
                                         // as prev_offset + prev_length.
  // Common user-key prefix feature (format_version >= 8, (reverse-)bytewise
  // comparator, no UDT stripping). Keys are encoded incrementally in the
  // format_version 7 layout; the running common user-key prefix is tracked, and
  // at Finish() the restart-point keys are rewritten in place with the prefix
  // stripped and the prefix is stored once at the block start.
  const bool use_common_prefix_;
  bool finishing_ = false;  // true while Finish() rewrites the block
#ifndef NDEBUG
  bool add_with_last_key_called_ = false;
#endif

  // Grouped after the flags above to minimize padding.
  std::string values_buffer_;
  std::string first_key_prefix_;  // running common user-key prefix
};

}  // namespace ROCKSDB_NAMESPACE
