//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32 (NOTE1)
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 (explicitly stored) for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.
//
// NOTE1: omitted for format_version >= 4 index blocks, because the value is
// composed of one (shared_bytes > 0) or two (shared_bytes == 0) varints, whose
// length is self-describing.

#include "table/block_based/block_builder.h"

#include <algorithm>
#include <cassert>
#include <cmath>

#include "db/dbformat.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/comparator.h"
#include "table/block_based/block_util.h"
#include "table/block_based/data_block_footer.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Tracks whether restart-point keys are uniformly distributed using Welford's
// online algorithm to incrementally compute the coefficient of variation (CV)
// of gaps between consecutive restart keys.
class UniformDataTracker {
 public:
  void AddKey(uint64_t key_value) {
    if (num_keys_ > 0) {
      double gap = static_cast<double>(key_value - prev_key_value_);
      size_t gap_count = num_keys_;
      double delta = gap - mean_;
      mean_ += delta / static_cast<double>(gap_count);
      double delta2 = gap - mean_;
      m2_ += delta * delta2;
    }
    prev_key_value_ = key_value;
    num_keys_++;
  }

  // Returns the coefficient of variation (CV) of the key gaps, or -1.0 if
  // there are not enough data points to compute it.
  double GetCV() const {
    size_t gap_count = num_keys_ > 0 ? num_keys_ - 1 : 0;
    if (gap_count < 2 || mean_ <= 0) {
      return -1.0;
    }
    return std::sqrt(m2_ / static_cast<double>(gap_count)) / mean_;
  }

 private:
  uint64_t prev_key_value_ = 0;
  size_t num_keys_ = 0;
  double mean_ = 0;
  double m2_ = 0;
};

}  // namespace

BlockBuilder::BlockBuilder(
    int block_restart_interval, bool use_delta_encoding,
    bool use_value_delta_encoding,
    BlockBasedTableOptions::DataBlockIndexType index_type,
    double data_block_hash_table_util_ratio, size_t ts_sz,
    bool persist_user_defined_timestamps, bool is_user_key,
    bool use_separated_kv_storage, Statistics* statistics,
    double uniform_cv_threshold, bool use_common_prefix)
    : block_restart_interval_(block_restart_interval),
      use_delta_encoding_(use_delta_encoding),
      use_value_delta_encoding_(use_value_delta_encoding),
      strip_ts_sz_(persist_user_defined_timestamps ? 0 : ts_sz),
      is_user_key_(is_user_key),
      restarts_(1, 0),  // First restart point is at offset 0
      counter_(0),
      finished_(false),
      is_uniform_(false),
      uniform_cv_threshold_(uniform_cv_threshold),
      statistics_(statistics),
      use_separated_kv_storage_(use_separated_kv_storage),
      use_common_prefix_(use_common_prefix) {
  switch (index_type) {
    case BlockBasedTableOptions::kDataBlockBinarySearch:
      break;
    case BlockBasedTableOptions::kDataBlockBinaryAndHash:
      data_block_hash_index_builder_.Initialize(
          data_block_hash_table_util_ratio);
      break;
    default:
      assert(0);
  }
  assert(block_restart_interval_ >= 1);
  // The common-prefix feature relies on delta encoding and requires user keys
  // without stripped timestamps. Value delta encoding (fv4 index blocks) is
  // supported: the restart-key rewrite branches on use_value_delta_encoding_.
  assert(!use_common_prefix_ || (use_delta_encoding_ && strip_ts_sz_ == 0));
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t) +
              (use_separated_kv_storage_ ? sizeof(uint32_t) : 0);
}

BlockBuilder::BlockBuilder(ForMetaBlock, int block_restart_interval)
    : BlockBuilder(block_restart_interval, /*use_delta_encoding=*/true,
                   /*use_value_delta_encoding=*/false,
                   BlockBasedTableOptions::kDataBlockBinarySearch,
                   /*data_block_hash_table_util_ratio=*/0.75, /*ts_sz=*/0,
                   /*persist_user_defined_timestamps=*/true,
                   /*is_user_key=*/false, /*use_separated_kv_storage=*/false,
                   /*statistics=*/nullptr, /*uniform_cv_threshold=*/-1.0,
                   /*use_common_prefix=*/false) {}

void BlockBuilder::Reset() {
  buffer_.clear();
  // First restart point is at offset 0. The common-prefix rewrite may have set
  // restarts_[0] to the prefix-section offset, so reset it explicitly rather
  // than assuming resize(1) leaves it at 0.
  restarts_.assign(1, 0);
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t) +
              (use_separated_kv_storage_ ? sizeof(uint32_t) : 0);
  counter_ = 0;
  finished_ = false;
  is_uniform_ = false;
  last_key_.clear();
  if (data_block_hash_index_builder_.Valid()) {
    data_block_hash_index_builder_.Reset();
  }
  values_buffer_.clear();

  first_key_prefix_.clear();
  finishing_ = false;

#ifndef NDEBUG
  add_with_last_key_called_ = false;
#endif
}

void BlockBuilder::SwapAndReset(std::string& buffer) {
  std::swap(buffer_, buffer);
  Reset();
}

size_t BlockBuilder::EstimateSizeAfterKV(const Slice& key,
                                         const Slice& value) const {
  size_t estimate = CurrentSizeEstimate();
  // Note: this is an imprecise estimate as it accounts for the whole key size
  // instead of non-shared key size.
  estimate += key.size();
  if (strip_ts_sz_ > 0) {
    estimate -= strip_ts_sz_;
  }
  // In value delta encoding we estimate the value delta size as half the full
  // value size since only the size field of block handle is encoded.
  estimate +=
      !use_value_delta_encoding_ || (counter_ >= block_restart_interval_)
          ? value.size()
          : value.size() / 2;

  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t);  // a new restart entry.
  }

  // For separated KV storage, value_offset varint is written at restart points
  if (use_separated_kv_storage_ &&
      (counter_ == 0 || counter_ >= block_restart_interval_)) {
    estimate += VarintLength(values_buffer_.size());
  }

  estimate += sizeof(int32_t);  // varint for shared prefix length.
  // Note: this is an imprecise estimate as we will have to encoded size, one
  // for shared key and one for non-shared key.
  estimate += VarintLength(key.size());  // varint for key length.
  if (!use_value_delta_encoding_ || (counter_ >= block_restart_interval_)) {
    estimate += VarintLength(value.size());  // varint for value length.
  }

  return estimate;
}

Slice BlockBuilder::Finish() {
  // Common-prefix feature: rewrite restart-point keys in place, stripping the
  // block's common user-key prefix (stored once in the block's leading bytes,
  // [0, restarts[0])). No footer bit is needed -- a non-zero restarts[0]
  // self-signals the prefix to the reader. Only done when there is a non-empty
  // common prefix to remove.
  if (use_common_prefix_ && !buffer_.empty() && !first_key_prefix_.empty()) {
    RewriteRestartKeysStrippingPrefix();
  }

  // Safe to run after the strip above: stripping removes the same block-common
  // prefix from every restart key, so difference_offset() (hence prefix_len)
  // shrinks by exactly that length while each key's start shifts by the same
  // amount -- ReadBe64FromKey ends up reading the identical suffix bytes, so
  // the uniformity decision matches running on the original keys.
  // (ReadBe64FromKey also strips the internal trailer, so footer bytes are
  // never read as key data.)
  is_uniform_ = ScanForUniformity();

  // Append restart array
  size_t values_buffer_offset = buffer_.size();

  if (use_separated_kv_storage_) {
    buffer_.append(values_buffer_);
  }

  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }

  DataBlockFooter footer;
  footer.num_restarts = static_cast<uint32_t>(restarts_.size());
  footer.index_type = BlockBasedTableOptions::kDataBlockBinarySearch;
  footer.is_uniform = is_uniform_;
  if (data_block_hash_index_builder_.Valid() &&
      CurrentSizeEstimate() <= kMaxBlockSizeSupportedByHashIndex) {
    data_block_hash_index_builder_.Finish(buffer_);
    footer.index_type = BlockBasedTableOptions::kDataBlockBinaryAndHash;
  }

  if (use_separated_kv_storage_) {
    footer.separated_kv = true;
    footer.values_section_offset = static_cast<uint32_t>(values_buffer_offset);
  }
  footer.EncodeTo(&buffer_);
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value,
                       const Slice& delta_value, bool skip_delta_encoding) {
  // Ensure no unsafe mixing of Add and AddWithLastKey
  assert(!add_with_last_key_called_);

  AddWithLastKeyImpl(key, value, last_key_, delta_value, skip_delta_encoding,
                     buffer_.size());
  if (use_delta_encoding_) {
    // Update state
    // We used to just copy the changed data, but it appears to be
    // faster to just copy the whole thing.
    last_key_.assign(key.data(), key.size());
  }
}

void BlockBuilder::AddWithLastKey(const Slice& key, const Slice& value,
                                  const Slice& last_key_param,
                                  const Slice& delta_value,
                                  bool skip_delta_encoding) {
  // Ensure no unsafe mixing of Add and AddWithLastKey
  assert(last_key_.empty());
#ifndef NDEBUG
  add_with_last_key_called_ = false;
#endif

  // Here we make sure to use an empty `last_key` on first call after creation
  // or Reset. This is more convenient for the caller and we can be more
  // clever inside BlockBuilder. On this hot code path, we want to avoid
  // conditional jumps like `buffer_.empty() ? ... : ...` so we can use a
  // fast arithmetic operation instead, with an assertion to be sure our logic
  // is sound.
  size_t buffer_size = buffer_.size();
  size_t last_key_size = last_key_param.size();
  assert(buffer_size == 0 || buffer_size >= last_key_size - strip_ts_sz_);

  Slice last_key(last_key_param.data(), last_key_size * (buffer_size > 0));

  AddWithLastKeyImpl(key, value, last_key, delta_value, skip_delta_encoding,
                     buffer_size);
}

inline void BlockBuilder::AddWithLastKeyImpl(
    const Slice& key, const Slice& value, const Slice& last_key,
    const Slice& delta_value, bool skip_delta_encoding, size_t buffer_size) {
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);
  // Verify < 4GB assumption (see API comments on Add())
  assert(key.size() < uint64_t{1} << 32);
  assert(value.size() < uint64_t{1} << 32);
  assert(last_key.size() < uint64_t{1} << 32);
  assert(delta_value.size() < uint64_t{1} << 32);
  std::string key_buf;
  std::string last_key_buf;
  const Slice key_to_persist = MaybeStripTimestampFromKey(&key_buf, key);
  // For delta key encoding, the first key in each restart interval doesn't have
  // a last key to share bytes with.
  const Slice last_key_persisted =
      last_key.size() == 0
          ? last_key
          : MaybeStripTimestampFromKey(&last_key_buf, last_key);

  // FIXME: check/enforce that buffer_ hasn't exceeded 4GB. The concern
  // with adding that check and propagating the result is inner-loop
  // performance. This case is HIGH concern because blocks like range deletions
  // and non-partitioned indexes could pile large keys together into one block.
  const uint32_t buffer_size32 = static_cast<uint32_t>(buffer_size);
  // NOTE: assuming all slice sizes < 4GB (see API comments on Add())
  uint32_t shared = 0;  // number of bytes shared with prev key
  if (counter_ >= block_restart_interval_) {
    // Restart compression
    restarts_.push_back(buffer_size32);
    estimate_ += sizeof(uint32_t);
    counter_ = 0;
  } else if (use_delta_encoding_ && !skip_delta_encoding) {
    // See how much sharing to do with previous string
    shared = static_cast<uint32_t>(
        key_to_persist.difference_offset(last_key_persisted));
  }

  // Common-prefix feature: track the running common user-key prefix across all
  // keys in the block. The prefix is removed from restart-point keys later, in
  // RewriteRestartKeysStrippingPrefix() at Finish(). To keep this nearly free
  // in the common (stable-prefix) case, we exploit `shared` (bytes shared with
  // the previous key): a non-restart key that shares >= prefix_len bytes with
  // its predecessor still starts with the whole running prefix. For internal
  // keys, shared can extend into the footer, so cap it at the current user-key
  // length. Restart entries have shared forced to 0, so they are rechecked with
  // a real comparison (cheap: 1/restart_interval).
  if (use_common_prefix_) {
    // The reader treats every shared==0 entry as a stripped restart key and
    // prepends the block's common prefix. skip_delta_encoding would create a
    // non-restart shared==0 entry (full key stored) that the reader would
    // wrongly re-prefix, so common-prefix is gated off wherever
    // skip_delta_encoding can occur (index blocks: super_block_alignment_size
    // == 0). Enforce that invariant here.
    assert(!skip_delta_encoding);
    if (buffer_size == 0) {
      const Slice uk =
          is_user_key_ ? key_to_persist : ExtractUserKey(key_to_persist);
      first_key_prefix_.assign(uk.data(), uk.size());
    } else if (counter_ == 0) {
      // Restart entry (shared was forced to 0); recompute the real overlap.
      const Slice uk =
          is_user_key_ ? key_to_persist : ExtractUserKey(key_to_persist);
      size_t common = Slice(first_key_prefix_).difference_offset(uk);
      if (common < first_key_prefix_.size()) {
        first_key_prefix_.resize(common);
      }
    } else {
      const Slice uk =
          is_user_key_ ? key_to_persist : ExtractUserKey(key_to_persist);
      const size_t shared_user_key = std::min<size_t>(shared, uk.size());
      if (shared_user_key < first_key_prefix_.size()) {
        first_key_prefix_.resize(shared_user_key);
      }
    }
  }

  const uint32_t non_shared =
      static_cast<uint32_t>(key_to_persist.size()) - shared;
  const size_t prev_values_size = values_buffer_.size();

  // FIXME: check/enforce that values_buffer_ hasn't exceeded 4GB. The concern
  // with adding that check and propagating the result is inner-loop
  // performance. This case is low concern because it (at time of writing) only
  // applies to data blocks and those are flushed as soon as the size exceeds
  // the block size.
  const uint32_t prev_values_size32 = static_cast<uint32_t>(prev_values_size);
  const uint32_t value_size = static_cast<uint32_t>(value.size());
  if (use_value_delta_encoding_) {
    if (use_separated_kv_storage_ && counter_ == 0) {
      // Add "<shared><non_shared><value_offset>" to buffer_
      PutVarint32(&buffer_, shared, non_shared, prev_values_size32);
    } else {
      // Add "<shared><non_shared>" to buffer_
      PutVarint32(&buffer_, shared, non_shared);
    }
  } else {
    if (use_separated_kv_storage_ && counter_ == 0) {
      // Add "<shared><non_shared><value_size><value_offset>" to buffer_
      PutVarint32(&buffer_, shared, non_shared, value_size, prev_values_size32);
    } else {
      // Add "<shared><non_shared><value_size>" to buffer_
      PutVarint32(&buffer_, shared, non_shared, value_size);
    }
  }

  // Add string delta to buffer_ (using only bottom 32 bits of size for
  // consistent treatment in case of corruption)
  buffer_.append(key_to_persist.data() + shared, non_shared);

  auto& values_buffer = use_separated_kv_storage_ ? values_buffer_ : buffer_;
  // Use value delta encoding only when the key has shared bytes. This would
  // simplify the decoding, where it can figure which decoding to use simply by
  // looking at the shared bytes size.
  if (shared != 0 && use_value_delta_encoding_) {
    // Using only bottom 32 bits of size for consistent treatment in case of
    // corruption
    // NOTE: callers may pass an empty delta_value when they had no previous
    // handle to delta against, relying on shared == 0 for a block's first
    // entry. Catch that coupling breaking, which would otherwise silently
    // write a zero-length value: a real delta encoding is never empty.
    assert(!delta_value.empty());
    values_buffer.append(delta_value.data(),
                         static_cast<uint32_t>(delta_value.size()));
  } else {
    // Using only bottom 32 bits of size for consistent treatment in case of
    // corruption
    values_buffer.append(value.data(), value_size);
  }

  // TODO(yuzhangyu): make user defined timestamp work with block hash index.
  if (data_block_hash_index_builder_.Valid()) {
    // Only data blocks should be using `kDataBlockBinaryAndHash` index type.
    // And data blocks should always be built with internal keys instead of
    // user keys.
    assert(!is_user_key_);
    data_block_hash_index_builder_.Add(ExtractUserKey(key),
                                       restarts_.size() - 1);
  }

  counter_++;
  estimate_ +=
      buffer_.size() - buffer_size + values_buffer_.size() - prev_values_size;
}

void BlockBuilder::RewriteRestartKeysStrippingPrefix() {
  assert(use_common_prefix_);
  assert(!first_key_prefix_.empty());
  assert(!buffer_.empty());

  finishing_ = true;
  const uint32_t p = static_cast<uint32_t>(first_key_prefix_.size());
  const char* base = buffer_.data();
  // At this point buffer_ holds only the entries section (inline values for
  // non-separated storage; keys-only for separated, values in values_buffer_).
  const size_t keys_end = buffer_.size();

  std::string new_buf;
  new_buf.reserve(keys_end + p);
  // Common user-key prefix section at the start of the block: the raw prefix
  // bytes, with no length field. restarts[0] (the offset of the first entry,
  // recorded below) is the prefix length.
  new_buf.append(first_key_prefix_.data(), p);

  std::vector<uint32_t> new_restarts;
  const size_t num_restarts = restarts_.size();
  new_restarts.reserve(num_restarts);

  for (size_t k = 0; k < num_restarts; ++k) {
    const size_t r_start = restarts_[k];
    const size_t interval_end =
        (k + 1 < num_restarts) ? restarts_[k + 1] : keys_end;
    const char* in = base + r_start;
    const char* limit = base + interval_end;

    // Decode the restart-point entry header. The layout differs by value
    // encoding:
    //   non-V4 (data blocks):
    //     <shared=0><non_shared><value_size>[<value_offset> if separated]<key>
    //     [<value> if not separated]
    //   V4 (index blocks with value delta encoding): no value_size field --
    //     the value is a self-delimiting BlockHandle.
    //     <shared=0><non_shared>[<value_offset> if separated]<key><value>
    uint32_t shared = 0;
    uint32_t non_shared = 0;
    uint32_t value_size = 0;
    uint32_t value_offset = 0;
    in = GetVarint32Ptr(in, limit, &shared);
    in = GetVarint32Ptr(in, limit, &non_shared);
    if (!use_value_delta_encoding_) {
      in = GetVarint32Ptr(in, limit, &value_size);
    }
    if (use_separated_kv_storage_) {
      in = GetVarint32Ptr(in, limit, &value_offset);
    }
    assert(in != nullptr);
    assert(shared == 0);
    assert(non_shared >= p);
    const char* key_ptr = in;

    // Re-emit the restart entry header with the common prefix removed from the
    // key length. `shared` stays 0 so the reader still recognizes a restart
    // point (and prepends the block's common prefix).
    new_restarts.push_back(static_cast<uint32_t>(new_buf.size()));
    if (!use_value_delta_encoding_) {
      if (use_separated_kv_storage_) {
        PutVarint32(&new_buf, 0, non_shared - p, value_size, value_offset);
      } else {
        PutVarint32(&new_buf, 0, non_shared - p, value_size);
      }
    } else {
      if (use_separated_kv_storage_) {
        PutVarint32(&new_buf, 0, non_shared - p, value_offset);
      } else {
        PutVarint32(&new_buf, 0, non_shared - p);
      }
    }
    new_buf.append(key_ptr + p, non_shared - p);

    // Bulk-copy the rest of the interval verbatim: the inline value (full or
    // delta BlockHandle for V4; sized value for non-V4 non-separated; nothing
    // inline for separated storage) plus all non-restart entries, whose encoded
    // bytes are independent of the block's common prefix.
    const size_t after_key_off = (key_ptr - base) + non_shared;
    new_buf.append(base + after_key_off, interval_end - after_key_off);
  }

  buffer_.swap(new_buf);
  restarts_.swap(new_restarts);

  // Recompute estimate_ for the post-strip block so the hash-index size gate in
  // Finish() (via CurrentSizeEstimate(), which no longer adjusts once
  // finishing_ is set) sees the actual size.
  estimate_ = buffer_.size() +
              (use_separated_kv_storage_ ? values_buffer_.size() : 0) +
              restarts_.size() * sizeof(uint32_t) + sizeof(uint32_t) +
              (use_separated_kv_storage_ ? sizeof(uint32_t) : 0);
}

const Slice BlockBuilder::MaybeStripTimestampFromKey(std::string* key_buf,
                                                     const Slice& key) {
  // Only use bottom 32 bits of size for internal consistency (see API
  // comments on Add())
  Slice stripped_key(key.data(), static_cast<uint32_t>(key.size()));
  if (strip_ts_sz_ > 0) {
    if (is_user_key_) {
      stripped_key.remove_suffix(strip_ts_sz_);
    } else {
      StripTimestampFromInternalKey(key_buf, stripped_key, strip_ts_sz_);
      stripped_key = *key_buf;
    }
  }
  return stripped_key;
}

Slice BlockBuilder::GetRestartKey(uint32_t index, const char* limit) const {
  assert(index < restarts_.size());
  const char* p = buffer_.data() + restarts_[index];
  uint32_t shared;
  uint32_t non_shared;
  // When separated KV storage is enabled, restart point entries include an
  // extra value_offset varint that must be consumed to find the key delta.
  uint32_t value_offset;
  uint32_t* value_offset_ptr =
      use_separated_kv_storage_ ? &value_offset : nullptr;
  if (use_value_delta_encoding_) {
    p = DecodeKeyV4()(p, limit, &shared, &non_shared, value_offset_ptr);
  } else {
    p = DecodeKey()(p, limit, &shared, &non_shared, value_offset_ptr);
  }
  assert(p != nullptr);
  assert(shared == 0);
  (void)shared;
  return Slice(p, non_shared);
}

bool BlockBuilder::ScanForUniformity() const {
  if (uniform_cv_threshold_ < 0 || restarts_.size() < 3) {
    return false;
  }

  const char* limit = buffer_.data() + buffer_.size();

  Slice first_key = GetRestartKey(0, limit);
  Slice last_key =
      GetRestartKey(static_cast<uint32_t>(restarts_.size() - 1), limit);

  // Keys must be long enough for ReadBe64FromKey which strips internal bytes
  if (!is_user_key_ && (first_key.size() < kNumInternalBytes ||
                        last_key.size() < kNumInternalBytes)) {
    return false;
  }

  size_t prefix_len = first_key.difference_offset(last_key);

  UniformDataTracker tracker;
  for (size_t i = 0; i < restarts_.size(); i++) {
    Slice key = GetRestartKey(static_cast<uint32_t>(i), limit);
    if (!is_user_key_ && key.size() < kNumInternalBytes) {
      return false;
    }
    tracker.AddKey(ReadBe64FromKey(key, is_user_key_, prefix_len));
  }

  double cv = tracker.GetCV();
  if (statistics_ != nullptr && cv >= 0) {
    RecordInHistogram(statistics_, BLOCK_KEY_DISTRIBUTION_CV,
                      static_cast<uint64_t>(cv * 10000));
  }

  return cv >= 0 && cv < uniform_cv_threshold_;
}

}  // namespace ROCKSDB_NAMESPACE
