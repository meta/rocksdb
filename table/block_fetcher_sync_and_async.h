//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coro_utils.h"

#if defined(WITHOUT_COROUTINES) || \
    (defined(USE_COROUTINES) && defined(WITH_COROUTINES))

namespace ROCKSDB_NAMESPACE {

// Read a block from the file and verify its checksum. Upon return, io_status_
// will be updated with the status of the read, and slice_ will be
// updated with a pointer to the data.
DEFINE_SYNC_AND_ASYNC(void, BlockFetcher::ReadBlock)(bool retry) {
  FSReadRequest read_req;
  IOOptions opts;
  IODebugContext dbg;
  io_status_ = file_->PrepareIOOptions(read_options_, opts, &dbg);
  opts.verify_and_reconstruct_read = retry;
  read_req.status.PermitUncheckedError();
  // Actual file read
  if (io_status_.ok()) {
    if (file_->use_direct_io()) {
      AlignedBuffer::Allocator direct_io_allocator;
      if (block_buffer_provider_.has_value() && !maybe_compressed_) {
        direct_io_allocator = MakeReadScopedAlignedBufferAllocator(
            block_buffer_provider_, &read_scoped_buf_lease_);
      }
      AlignedBufferAllocationContext direct_io_context{
          &direct_io_buffer_,
          direct_io_allocator ? &direct_io_allocator : nullptr};
      PERF_TIMER_GUARD(block_read_time);
#if defined(WITHOUT_COROUTINES)
      PERF_CPU_TIMER_GUARD(
          block_read_cpu_time,
          ioptions_.env ? ioptions_.env->GetSystemClock().get() : nullptr);
#endif
      read_req.offset = handle_.offset();
      read_req.len = block_size_with_trailer_;
      read_req.scratch = nullptr;
      CO_AWAIT(file_->Read, opts, &read_req, &direct_io_context, &dbg);
      io_status_ = std::move(read_req.status);
      slice_ = read_req.result;
      PERF_COUNTER_ADD(block_read_count, 1);
      used_buf_ = const_cast<char*>(slice_.data());
    } else if (use_fs_scratch_) {
      PERF_TIMER_GUARD(block_read_time);
#if defined(WITHOUT_COROUTINES)
      PERF_CPU_TIMER_GUARD(
          block_read_cpu_time,
          ioptions_.env ? ioptions_.env->GetSystemClock().get() : nullptr);
#endif
      read_req.offset = handle_.offset();
      read_req.len = block_size_with_trailer_;
      read_req.scratch = nullptr;
      AlignedBuffer direct_io_buffer;
      AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
      io_status_ = CO_AWAIT(file_->MultiRead, opts, &read_req, /*num_reqs=*/1,
                            &direct_io_context, &dbg);
      PERF_COUNTER_ADD(block_read_count, 1);

      slice_ = Slice(read_req.result.data(), read_req.result.size());
      used_buf_ = const_cast<char*>(slice_.data());
    } else {
      // It allocates/assign used_buf_
      PrepareBufferForBlockFromFile();
      if (!io_status_.ok()) {
        CO_RETURN;
      }

      PERF_TIMER_GUARD(block_read_time);
#if defined(WITHOUT_COROUTINES)
      PERF_CPU_TIMER_GUARD(
          block_read_cpu_time,
          ioptions_.env ? ioptions_.env->GetSystemClock().get() : nullptr);
#endif

      read_req.offset = handle_.offset();
      read_req.len = block_size_with_trailer_;
      read_req.scratch = used_buf_;
      CO_AWAIT(file_->Read, opts, &read_req,
               /*direct_io_buffer=*/nullptr, &dbg);
      io_status_ = std::move(read_req.status);
      slice_ = read_req.result;
      PERF_COUNTER_ADD(block_read_count, 1);
#ifndef NDEBUG
      if (slice_.data() == &stack_buf_[0]) {
        num_stack_buf_memcpy_++;
      } else if (slice_.data() == heap_buf_.get()) {
        num_heap_buf_memcpy_++;
      } else if (slice_.data() == compressed_buf_.get()) {
        num_compressed_buf_memcpy_++;
      }
#endif
    }
  }

  // TODO: introduce dedicated perf counter for range tombstones
  switch (block_type_) {
    case BlockType::kFilter:
    case BlockType::kFilterPartitionIndex:
      PERF_COUNTER_ADD(filter_block_read_count, 1);
      break;

    case BlockType::kCompressionDictionary:
      PERF_COUNTER_ADD(compression_dict_block_read_count, 1);
      break;

    case BlockType::kIndex:
    case BlockType::kUserDefinedIndex:
      PERF_COUNTER_ADD(index_block_read_count, 1);
      break;

    // Nothing to do here as we don't have counters for the other types.
    default:
      break;
  }

  PERF_COUNTER_ADD(block_read_byte, block_size_with_trailer_);
  RecordBlockReadBytePerfCounter(block_type_, block_size_with_trailer_);
  IGNORE_STATUS_IF_ERROR(io_status_);
  if (io_status_.ok()) {
    if (use_fs_scratch_ && !read_req.status.ok()) {
      io_status_ = read_req.status;
    } else if (slice_.size() != block_size_with_trailer_) {
      io_status_ = IOStatus::Corruption(
          "truncated block read from " + file_->file_name() + " offset " +
          std::to_string(handle_.offset()) + ", expected " +
          std::to_string(block_size_with_trailer_) + " bytes, got " +
          std::to_string(slice_.size()));
    }
  }

  if (io_status_.ok()) {
    ProcessTrailerIfPresent();
  }

  if (retry) {
    RecordTick(ioptions_.stats, FILE_READ_CORRUPTION_RETRY_COUNT);
  }
  if (io_status_.ok()) {
    InsertCompressedBlockToPersistentCacheIfNeeded();
    fs_buf_ = std::move(read_req.fs_scratch);
    if (retry) {
      RecordTick(ioptions_.stats, FILE_READ_CORRUPTION_RETRY_SUCCESS_COUNT);
    }
  } else {
    ReleaseFileSystemProvidedBuffer(&read_req);
    direct_io_buffer_.Release();
    compressed_buf_.reset();
    heap_buf_.reset();
    used_buf_ = nullptr;
  }
}

DEFINE_SYNC_AND_ASYNC(IOStatus, BlockFetcher::ReadBlockContents)() {
  if (TryGetUncompressBlockFromPersistentCache()) {
    compression_type() = kNoCompression;
#ifndef NDEBUG
    contents_->has_trailer = footer_.GetBlockTrailerSize() > 0;
#endif  // NDEBUG
    CO_RETURN IOStatus::OK();
  }
  if (TryGetFromPrefetchBuffer()) {
    if (io_status_.IsCorruption() && retry_corrupt_read_) {
      CO_AWAIT(ReadBlock, /*retry=*/true);
    }
    if (!io_status_.ok()) {
      assert(!fs_buf_);
      CO_RETURN io_status_;
    }
  } else if (!TryGetSerializedBlockFromPersistentCache()) {
    CO_AWAIT(ReadBlock, /*retry =*/false);
    // If the file system supports retry after corruption, then try to
    // re-read the block and see if it succeeds.
    if (io_status_.IsCorruption() && retry_corrupt_read_) {
      assert(!fs_buf_);
      CO_AWAIT(ReadBlock, /*retry=*/true);
    }
    if (!io_status_.ok()) {
      assert(!fs_buf_);
      CO_RETURN io_status_;
    }
  }

  if (do_uncompress_ && compression_type() != kNoCompression) {
    PERF_TIMER_GUARD(block_decompress_time);
    // Process the compressed block without trailer
    slice_.size_ = block_size_;
    decomp_args_.compressed_data = slice_;
    io_status_ = status_to_io_status(DecompressSerializedBlock(
        decomp_args_, *decompressor_, contents_, ioptions_, memory_allocator_,
        block_buffer_provider_));
#ifndef NDEBUG
    num_heap_buf_memcpy_++;
#endif
  } else {
    GetBlockContents();
    slice_ = Slice();
  }

  InsertUncompressedBlockToPersistentCacheIfNeeded();

  CO_RETURN io_status_;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // WITHOUT_COROUTINES || (USE_COROUTINES && WITH_COROUTINES)
