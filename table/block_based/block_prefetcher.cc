//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "table/block_based/block_prefetcher.h"

#include "rocksdb/file_system.h"
#include "table/block_based/block_based_table_reader.h"

namespace ROCKSDB_NAMESPACE {
void BlockPrefetcher::PrefetchIfNeeded(
    const BlockBasedTable::Rep* rep, const BlockHandle& handle,
    const size_t readahead_size, bool is_for_compaction,
    const bool no_sequential_checking, const ReadOptions& read_options,
    const std::function<void(bool, uint64_t&, uint64_t&)>& readaheadsize_cb,
    bool is_async_io_prefetch, PrefetchDirection direction) {
  if (read_options.read_tier == ReadTier::kBlockCacheTier) {
    // Disable prefetching when IO disallowed. (Note that we haven't allocated
    // any buffers yet despite the various tracked settings.)
    return;
  }

  ReadaheadParams readahead_params;
  readahead_params.initial_readahead_size = readahead_size;
  readahead_params.max_readahead_size = readahead_size;
  readahead_params.num_buffers = is_async_io_prefetch ? 2 : 1;
  readahead_params.direction = direction;

  const size_t len = BlockBasedTable::BlockSizeWithTrailer(handle);
  const size_t offset = handle.offset();
  if (is_for_compaction) {
    if (!rep->file->use_direct_io() && compaction_readahead_size_ > 0) {
      // If FS supports prefetching (readahead_limit_ will be non zero in that
      // case) and current block exists in prefetch buffer then return.
      // Compaction is always forward.
      if (offset + len <= readahead_limit_) {
        return;
      }
      IOOptions opts;
      IODebugContext dbg;
      Status s = rep->file->PrepareIOOptions(read_options, opts, &dbg);
      if (!s.ok()) {
        return;
      }
      if (rep->fs_prefetch_support) {
        s = rep->file->Prefetch(opts, offset, len + compaction_readahead_size_);
        if (s.ok()) {
          readahead_limit_ = offset + len + compaction_readahead_size_;
          return;
        } else if (!s.IsNotSupported()) {
          return;
        }
        // If FS prefetch returned NotSupported despite feature bit being set,
        // fall through to use internal prefetch buffer.
      }
    }
    // If FS prefetch is not supported, fall back to use internal prefetch
    // buffer.
    //
    // num_file_reads is used  by FilePrefetchBuffer only when
    // implicit_auto_readahead is set.
    readahead_params.initial_readahead_size = compaction_readahead_size_;
    readahead_params.max_readahead_size = compaction_readahead_size_;
    rep->CreateFilePrefetchBufferIfNotExists(
        readahead_params, &prefetch_buffer_,
        /*readaheadsize_cb=*/nullptr,
        /*usage=*/FilePrefetchBufferUsage::kCompactionPrefetch);
    return;
  }

  // Explicit user requested readahead.
  if (readahead_size > 0) {
    rep->CreateFilePrefetchBufferIfNotExists(
        readahead_params, &prefetch_buffer_, readaheadsize_cb,
        /*usage=*/FilePrefetchBufferUsage::kUserScanPrefetch);
    return;
  }

  // Implicit readahead.

  // If max_auto_readahead_size is set to be 0 by user, no data will be
  // prefetched.
  size_t max_auto_readahead_size = rep->table_options.max_auto_readahead_size;
  if (max_auto_readahead_size == 0 || initial_auto_readahead_size_ == 0) {
    return;
  }

  if (initial_auto_readahead_size_ > max_auto_readahead_size) {
    initial_auto_readahead_size_ = max_auto_readahead_size;
  }

  readahead_params.initial_readahead_size = initial_auto_readahead_size_;
  readahead_params.max_readahead_size = max_auto_readahead_size;
  readahead_params.implicit_auto_readahead = true;
  readahead_params.num_file_reads_for_auto_readahead =
      rep->table_options.num_file_reads_for_auto_readahead;

  // In case of no_sequential_checking, it will skip the num_file_reads_ and
  // will always creates the FilePrefetchBuffer.
  if (no_sequential_checking) {
    rep->CreateFilePrefetchBufferIfNotExists(
        readahead_params, &prefetch_buffer_, readaheadsize_cb,
        /*usage=*/FilePrefetchBufferUsage::kUserScanPrefetch);
    return;
  }

  // If FS supports prefetching (readahead_limit_ will be non zero in that case)
  // and current block exists in prefetch buffer then return.
  bool in_prefetch_range = false;
  if (direction == PrefetchDirection::kForward) {
    in_prefetch_range = (offset + len <= readahead_limit_);
  } else {
    // For backward, prefetch_start_ tracks lower bound of prefetched range.
    // If prefetch_start_ is 0 it means not set yet, treat as miss.
    in_prefetch_range = (prefetch_start_ != 0 && offset >= prefetch_start_ &&
                         offset + len <= readahead_limit_);
    // Alternative simple check for backward: offset >= prefetch_start_
    // We'll use offset >= prefetch_start_ as indicator current block is within
    // prefetched window.
    if (prefetch_start_ != 0) {
      in_prefetch_range = (offset >= prefetch_start_);
    } else {
      in_prefetch_range = false;
    }
  }
  if (in_prefetch_range) {
    UpdateReadPattern(offset, len);
    return;
  }

  if (!IsBlockSequential(offset, len, direction)) {
    UpdateReadPattern(offset, len);
    ResetValues(rep->table_options.initial_auto_readahead_size);
    return;
  }
  UpdateReadPattern(offset, len);

  // Implicit auto readahead, which will be enabled if the number of reads
  // reached `table_options.num_file_reads_for_auto_readahead` (default: 2)  and
  // scans are sequential.
  num_file_reads_++;
  if (num_file_reads_ <= rep->table_options.num_file_reads_for_auto_readahead) {
    return;
  }

  readahead_params.num_file_reads = num_file_reads_;
  if (rep->file->use_direct_io()) {
    rep->CreateFilePrefetchBufferIfNotExists(
        readahead_params, &prefetch_buffer_, readaheadsize_cb,
        /*usage=*/FilePrefetchBufferUsage::kUserScanPrefetch);
    return;
  }

  if (readahead_size_ > max_auto_readahead_size) {
    readahead_size_ = max_auto_readahead_size;
  }

  // If prefetch is not supported, fall back to use internal prefetch buffer.
  IOOptions opts;
  Status s = rep->file->PrepareIOOptions(read_options, opts);
  if (!s.ok()) {
    return;
  }

  if (rep->fs_prefetch_support) {
    uint64_t prefetch_offset;
    size_t prefetch_len;
    if (direction == PrefetchDirection::kForward) {
      prefetch_offset = handle.offset();
      prefetch_len =
          BlockBasedTable::BlockSizeWithTrailer(handle) + readahead_size_;
    } else {
      // Backward: prefetch from offset - readahead_size_ to offset + len
      uint64_t curr_offset = handle.offset();
      uint64_t start =
          curr_offset > readahead_size_ ? curr_offset - readahead_size_ : 0;
      prefetch_offset = start;
      prefetch_len =
          curr_offset - start + BlockBasedTable::BlockSizeWithTrailer(handle);
    }
    s = rep->file->Prefetch(opts, prefetch_offset, prefetch_len);
    if (s.ok()) {
      if (direction == PrefetchDirection::kForward) {
        readahead_limit_ = offset + len + readahead_size_;
        prefetch_start_ =
            offset;  // not really used for forward but for completeness
      } else {
        prefetch_start_ = prefetch_offset;
        readahead_limit_ = offset + len;
      }
      // Keep exponentially increasing readahead size until
      // max_auto_readahead_size.
      readahead_size_ = std::min(max_auto_readahead_size, readahead_size_ * 2);
      return;
    }
  }
  // If FS prefetch is not supported or returned NotSupported, fall back to use
  // internal prefetch buffer.
  rep->CreateFilePrefetchBufferIfNotExists(
      readahead_params, &prefetch_buffer_, readaheadsize_cb,
      /*usage=*/FilePrefetchBufferUsage::kUserScanPrefetch);
}
}  // namespace ROCKSDB_NAMESPACE
