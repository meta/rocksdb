//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/block_fetcher.h"

#include <cassert>
#include <cinttypes>
#include <string>

#include "logging/logging.h"
#include "memory/memory_allocator_impl.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_type.h"
#include "table/block_based/reader_common.h"
#include "table/format.h"
#include "table/persistent_cache_helper.h"
#include "util/aligned_buffer.h"
#include "util/compression.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {

namespace {

inline void RecordBlockReadBytePerfCounter(BlockType block_type,
                                           uint64_t block_size_with_trailer) {
  switch (block_type) {
    case BlockType::kData:
      PERF_COUNTER_ADD(data_block_read_byte, block_size_with_trailer);
      break;
    case BlockType::kFilter:
    case BlockType::kFilterPartitionIndex:
      PERF_COUNTER_ADD(filter_block_read_byte, block_size_with_trailer);
      break;
    case BlockType::kCompressionDictionary:
      PERF_COUNTER_ADD(compression_dict_block_read_byte,
                       block_size_with_trailer);
      break;
    case BlockType::kIndex:
    case BlockType::kUserDefinedIndex:
      PERF_COUNTER_ADD(index_block_read_byte, block_size_with_trailer);
      break;
    default:
      PERF_COUNTER_ADD(metadata_block_read_byte, block_size_with_trailer);
      break;
  }
}

}  // namespace

inline void BlockFetcher::ProcessTrailerIfPresent() {
  if (footer_.GetBlockTrailerSize() > 0) {
    assert(footer_.GetBlockTrailerSize() == BlockBasedTable::kBlockTrailerSize);
    if (read_options_.verify_checksums) {
      io_status_ = status_to_io_status(VerifyBlockChecksum(
          footer_, slice_.data(), block_size_, file_->file_name(),
          handle_.offset(), block_type_));
      RecordTick(ioptions_.stats, BLOCK_CHECKSUM_COMPUTE_COUNT);
      if (!io_status_.ok()) {
        assert(io_status_.IsCorruption());
        RecordTick(ioptions_.stats, BLOCK_CHECKSUM_MISMATCH_COUNT);
      }
    }
    compression_type() =
        BlockBasedTable::GetBlockCompressionType(slice_.data(), block_size_);
  } else {
    // E.g. plain table or cuckoo table
    compression_type() = kNoCompression;
  }
}

inline bool BlockFetcher::TryGetUncompressBlockFromPersistentCache() {
  if (cache_options_.persistent_cache &&
      !cache_options_.persistent_cache->IsCompressed()) {
    Status status = PersistentCacheHelper::LookupUncompressed(
        cache_options_, handle_, contents_);
    if (status.ok()) {
      // uncompressed page is found for the block handle
      return true;
    } else {
      // uncompressed page is not found
      if (ioptions_.logger && !status.IsNotFound()) {
        assert(!status.ok());
        ROCKS_LOG_INFO(ioptions_.logger,
                       "Error reading from persistent cache. %s",
                       status.ToString().c_str());
      }
    }
  }
  return false;
}

inline bool BlockFetcher::TryGetFromPrefetchBuffer() {
  if (prefetch_buffer_ != nullptr) {
    IOOptions opts;
    IODebugContext dbg;
    IOStatus io_s = file_->PrepareIOOptions(read_options_, opts, &dbg);
    if (io_s.ok()) {
      bool read_from_prefetch_buffer = prefetch_buffer_->TryReadFromCache(
          opts, file_, handle_.offset(), block_size_with_trailer_, &slice_,
          &io_s, for_compaction_);
      if (read_from_prefetch_buffer) {
        ProcessTrailerIfPresent();
        if (io_status_.ok()) {
          got_from_prefetch_buffer_ = true;
          used_buf_ = const_cast<char*>(slice_.data());
        } else if (io_status_.IsCorruption()) {
          // Returning true apparently indicates we either got some data from
          // the prefetch buffer, or we tried and encountered an error.
          return true;
        }
      }
    }
    if (!io_s.ok()) {
      io_status_ = io_s;
      return true;
    }
  }
  return got_from_prefetch_buffer_;
}

inline bool BlockFetcher::TryGetSerializedBlockFromPersistentCache() {
  if (cache_options_.persistent_cache &&
      cache_options_.persistent_cache->IsCompressed()) {
    std::unique_ptr<char[]> buf;
    io_status_ = status_to_io_status(PersistentCacheHelper::LookupSerialized(
        cache_options_, handle_, &buf, block_size_with_trailer_));
    if (io_status_.ok()) {
      heap_buf_ = CacheAllocationPtr(buf.release());
      used_buf_ = heap_buf_.get();
      slice_ = Slice(heap_buf_.get(), block_size_);
      ProcessTrailerIfPresent();
      return true;
    } else if (!io_status_.IsNotFound() && ioptions_.logger) {
      assert(!io_status_.ok());
      ROCKS_LOG_INFO(ioptions_.logger,
                     "Error reading from persistent cache. %s",
                     io_status_.ToString().c_str());
    }
  }
  return false;
}

inline void BlockFetcher::PrepareBufferForBlockFromFile() {
  // cache miss read from device
  if (block_buffer_provider_.has_value() && !maybe_compressed_) {
    Status s = AllocateReadScopedBlockBuffer(block_buffer_provider_->get(),
                                             block_size_with_trailer_, 1,
                                             &read_scoped_buf_lease_);
    if (!s.ok()) {
      io_status_ = status_to_io_status(std::move(s));
      return;
    }
    used_buf_ = read_scoped_buf_lease_.data;
  } else if ((do_uncompress_ || ioptions_.allow_mmap_reads) &&
             block_size_with_trailer_ < kDefaultStackBufferSize) {
    // If we've got a small enough chunk of data, read it in to the
    // trivially allocated stack buffer instead of needing a full malloc()
    //
    // `GetBlockContents()` cannot return this data as its lifetime is tied to
    // this `BlockFetcher`'s lifetime. That is fine because this is only used
    // in cases where we do not expect the `GetBlockContents()` result to be the
    // same buffer we are assigning here. If we guess incorrectly, there will be
    // a heap allocation and memcpy in `GetBlockContents()` to obtain the final
    // result. Considering we are eliding a heap allocation here by using the
    // stack buffer, the cost of guessing incorrectly here is one extra memcpy.
    //
    // When `do_uncompress_` is true, we expect the uncompression step will
    // allocate memory for the final result, using the read-scoped provider if
    // one is configured. However this expectation will be wrong if the block
    // turns out to already be uncompressed, which we won't know for sure until
    // after reading it. In that case provider-backed reads copy the block into
    // provider storage in `GetBlockContents()`.
    //
    // When `ioptions_.allow_mmap_reads` is true, we do not expect the file
    // reader to use the scratch buffer at all, but instead return a pointer
    // into the mapped memory. This expectation will be wrong when using a
    // file reader that does not implement mmap reads properly.
    used_buf_ = &stack_buf_[0];
  } else if (maybe_compressed_ && !do_uncompress_) {
    compressed_buf_ =
        AllocateBlock(block_size_with_trailer_, memory_allocator_compressed_);
    used_buf_ = compressed_buf_.get();
  } else {
    heap_buf_ = AllocateBlock(block_size_with_trailer_, memory_allocator_);
    used_buf_ = heap_buf_.get();
  }
}

inline void BlockFetcher::InsertCompressedBlockToPersistentCacheIfNeeded() {
  if (io_status_.ok() && read_options_.fill_cache &&
      cache_options_.persistent_cache &&
      cache_options_.persistent_cache->IsCompressed()) {
    PersistentCacheHelper::InsertSerialized(cache_options_, handle_, used_buf_,
                                            block_size_with_trailer_);
  }
}

inline void BlockFetcher::InsertUncompressedBlockToPersistentCacheIfNeeded() {
  if (io_status_.ok() && !got_from_prefetch_buffer_ &&
      read_options_.fill_cache && cache_options_.persistent_cache &&
      !cache_options_.persistent_cache->IsCompressed()) {
    // insert to uncompressed cache
    PersistentCacheHelper::InsertUncompressed(cache_options_, handle_,
                                              *contents_);
  }
}

inline void BlockFetcher::CopyBufferToHeapBuf() {
  assert(used_buf_ != heap_buf_.get());
  heap_buf_ = AllocateBlock(block_size_with_trailer_, memory_allocator_);
  memcpy(heap_buf_.get(), used_buf_, block_size_with_trailer_);
#ifndef NDEBUG
  num_heap_buf_memcpy_++;
#endif
}

inline void BlockFetcher::CopyBufferToCompressedBuf() {
  assert(used_buf_ != compressed_buf_.get());
  compressed_buf_ =
      AllocateBlock(block_size_with_trailer_, memory_allocator_compressed_);
  memcpy(compressed_buf_.get(), used_buf_, block_size_with_trailer_);
#ifndef NDEBUG
  num_compressed_buf_memcpy_++;
#endif
}

// Before - Entering this method means the block is uncompressed or do not need
// to be decompressed.
//
// The block can be in one of the following buffers:
// 1. prefetch buffer if prefetch is enabled and the block is prefetched before
// 2. stack_buf_ if block size is smaller than the stack_buf_ size and block
//    is not compressed
// 3. heap_buf_ if the block is not compressed
// 4. compressed_buf_ if the block is compressed
// 5. direct_io_buffer_ if direct IO is enabled or
// 6. underlying file_system scratch is used (FSReadRequest.fs_scratch).
//
// After - After this method, compressed blocks should be in compressed_buf_ and
// heap_buf_ points to compressed_buf_. Uncompressed blocks should be recorded
// in *contents_ with either heap ownership or read-scoped cleanup ownership.
inline void BlockFetcher::GetBlockContents() {
  if (read_scoped_buf_lease_.cleanup.get() != nullptr &&
      compression_type() == kNoCompression) {
    contents_->data = Slice(slice_.data(), block_size_);
    contents_->cleanup = std::move(read_scoped_buf_lease_.cleanup);
    contents_->backing_size = read_scoped_buf_lease_.size;
    contents_->AssertSingleOwner();
  } else if (block_buffer_provider_.has_value() &&
             compression_type() == kNoCompression) {
    Status s = CopyBufferToReadScopedBlockContents(
        Slice(slice_.data(), block_size_with_trailer_), block_size_,
        block_buffer_provider_->get(), contents_);
    if (!s.ok()) {
      io_status_ = status_to_io_status(std::move(s));
      return;
    }
  } else if (slice_.data() != used_buf_) {
    // the slice content is not the buffer provided
    *contents_ = BlockContents(Slice(slice_.data(), block_size_));
  } else {
    // page can be either uncompressed or compressed, the buffer either stack
    // or heap provided. Refer to https://github.com/meta/rocksdb/pull/4096
    if (got_from_prefetch_buffer_ || used_buf_ == &stack_buf_[0]) {
      CopyBufferToHeapBuf();
    } else if (used_buf_ == compressed_buf_.get()) {
      if (compression_type() == kNoCompression &&
          memory_allocator_ != memory_allocator_compressed_) {
        CopyBufferToHeapBuf();
      } else {
        heap_buf_ = std::move(compressed_buf_);
      }
    } else if (direct_io_buffer_.BufferStart() != nullptr || use_fs_scratch_) {
      if (compression_type() == kNoCompression) {
        CopyBufferToHeapBuf();
      } else {
        CopyBufferToCompressedBuf();
        heap_buf_ = std::move(compressed_buf_);
      }
    }
    *contents_ = BlockContents(std::move(heap_buf_), block_size_);
  }
#ifndef NDEBUG
  contents_->has_trailer = footer_.GetBlockTrailerSize() > 0;
#endif
}

}  // namespace ROCKSDB_NAMESPACE

// clang-format off
#define WITHOUT_COROUTINES
#include "table/block_fetcher_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "table/block_fetcher_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {

IOStatus BlockFetcher::ReadAsyncBlockContents() {
  if (TryGetUncompressBlockFromPersistentCache()) {
    compression_type() = kNoCompression;
#ifndef NDEBUG
    contents_->has_trailer = footer_.GetBlockTrailerSize() > 0;
#endif  // NDEBUG
    return IOStatus::OK();
  } else if (!TryGetSerializedBlockFromPersistentCache()) {
    assert(prefetch_buffer_ != nullptr);
    if (!for_compaction_) {
      IOOptions opts;
      IODebugContext dbg;
      IOStatus io_s = file_->PrepareIOOptions(read_options_, opts, &dbg);
      if (!io_s.ok()) {
        return io_s;
      }
      io_s = status_to_io_status(prefetch_buffer_->PrefetchAsync(
          opts, file_, handle_.offset(), block_size_with_trailer_, &slice_));
      if (io_s.IsTryAgain()) {
        return io_s;
      }
      if (io_s.ok()) {
        // Data Block is already in prefetch.
        got_from_prefetch_buffer_ = true;
        ProcessTrailerIfPresent();
        if (io_status_.IsCorruption() && retry_corrupt_read_) {
          got_from_prefetch_buffer_ = false;
          ReadBlock(/*retry = */ true);
        }
        if (!io_status_.ok()) {
          assert(!fs_buf_);
          return io_status_;
        }
        used_buf_ = const_cast<char*>(slice_.data());

        if (do_uncompress_ && compression_type() != kNoCompression) {
          PERF_TIMER_GUARD(block_decompress_time);
          // Process the compressed block without trailer
          slice_.size_ = block_size_;
          decomp_args_.compressed_data = slice_;
          io_status_ = status_to_io_status(DecompressSerializedBlock(
              decomp_args_, *decompressor_, contents_, ioptions_,
              memory_allocator_, block_buffer_provider_));
#ifndef NDEBUG
          num_heap_buf_memcpy_++;
#endif
        } else {
          GetBlockContents();
        }
        InsertUncompressedBlockToPersistentCacheIfNeeded();
        return io_status_;
      }
    }
    // Fallback to sequential reading of data blocks in case of io_s returns
    // error or for_compaction_is true.
    return ReadBlockContents();
  }
  return io_status_;
}

}  // namespace ROCKSDB_NAMESPACE
