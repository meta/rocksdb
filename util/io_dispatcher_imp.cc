//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/io_dispatcher_imp.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "file/random_access_file_reader.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_dispatcher.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/cachable_entry.h"
#include "table/block_based/reader_common.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/aligned_buffer.h"
#include "util/io_prefetch_scheduler.h"

namespace ROCKSDB_NAMESPACE {

#ifndef NDEBUG
static bool BlockIndicesAreSortedByOffset(
    const std::vector<BlockHandle>& block_handles,
    const std::vector<size_t>& block_indices) {
  uint64_t prev_offset = 0;
  for (size_t i = 0; i < block_indices.size(); ++i) {
    if (block_indices[i] >= block_handles.size()) {
      return false;
    }
    const uint64_t current_offset = block_handles[block_indices[i]].offset();
    if (i > 0 && current_offset < prev_offset) {
      return false;
    }
    prev_offset = current_offset;
  }
  return true;
}
#endif

// Helper function to create and pin a block from a buffer
// Used by both ReadSet::PollAndProcessAsyncIO and IODispatcherImpl::Impl
static Status CreateAndPinBlockFromBuffer(
    const std::shared_ptr<IOJob>& job, const BlockHandle& block,
    uint64_t buffer_start_offset, const Slice& buffer_data,
    const SharedCleanablePtr& read_buffer_cleanup,
    bool read_buffer_requires_cleanup,
    CachableEntry<Block>& pinned_block_entry) {
  auto* rep = job->table->get_rep();
  const bool use_data_block_cache = ShouldUseDataBlockCacheForIterator(
      rep->table_options, job->job_options.read_options,
      rep->ioptions.allow_mmap_reads);

  // Get decompressor
  UnownedPtr<Decompressor> decompressor = rep->decompressor.get();
  CachableEntry<DecompressorDict> cached_dict;

  if (rep->uncompression_dict_reader) {
    Status s = rep->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        nullptr, job->job_options.read_options, nullptr, nullptr, &cached_dict);
    if (!s.ok()) {
      return s;
    }
    if (cached_dict.GetValue()) {
      decompressor = cached_dict.GetValue()->decompressor_.get();
    }
  }

  // Create block from buffer data
  const size_t block_size_with_trailer =
      BlockBasedTable::BlockSizeWithTrailer(block);
  if (block.offset() < buffer_start_offset) {
    return Status::Corruption("block offset precedes read buffer for " +
                              rep->file->file_name() + " offset " +
                              std::to_string(block.offset()));
  }
  const uint64_t block_offset_in_buffer = block.offset() - buffer_start_offset;
  if (block_offset_in_buffer > buffer_data.size() ||
      block_size_with_trailer >
          buffer_data.size() - static_cast<size_t>(block_offset_in_buffer)) {
    return Status::Corruption(
        "truncated block read from " + rep->file->file_name() + " offset " +
        std::to_string(block.offset()) + ", expected " +
        std::to_string(block_size_with_trailer) + " bytes in read buffer");
  }
  const char* block_data = buffer_data.data() + block_offset_in_buffer;

  // Prefetched blocks are built straight from the read buffer, bypassing
  // BlockFetcher, so the verification the non-prefetch path performs has to be
  // repeated here. Without it bit rot reaches the caller as valid keys, and on
  // the cache branch below it also enters the shared block cache, where
  // readers that never used MultiScan pick it up on a hit and never re-verify.
  if (job->job_options.read_options.verify_checksums) {
    Status checksum_status = VerifyBlockChecksum(
        rep->footer, block_data, block.size(), rep->file->file_name(),
        block.offset(), BlockType::kData);
    RecordTick(rep->ioptions.stats, BLOCK_CHECKSUM_COMPUTE_COUNT);
    if (!checksum_status.ok()) {
      RecordTick(rep->ioptions.stats, BLOCK_CHECKSUM_MISMATCH_COUNT);
      return checksum_status;
    }
  }

  if (use_data_block_cache) {
    CacheAllocationPtr data = AllocateBlock(
        block_size_with_trailer, GetMemoryAllocator(rep->table_options));
    memcpy(data.get(), block_data, block_size_with_trailer);
    BlockContents tmp_contents(std::move(data), block.size());

#ifndef NDEBUG
    tmp_contents.has_trailer = rep->footer.GetBlockTrailerSize() > 0;
#endif

    return job->table->CreateAndPinBlockInCache<Block_kData>(
        job->job_options.read_options, block, decompressor, &tmp_contents,
        &pinned_block_entry.As<Block_kData>());
  }

  BlockContents tmp_contents;
  const CompressionType compression_type =
      BlockBasedTable::GetBlockCompressionType(block_data, block.size());
  ReadScopedBlockBufferProviderRef block_buffer_provider =
      GetReadScopedBlockBufferProvider(job->job_options.read_options,
                                       rep->ioptions.allow_mmap_reads);
  Status s;
  if (compression_type == kNoCompression) {
    // Provider-backed uncompressed blocks should already be in the
    // provider-owned read buffer allocated before I/O. Attach that cleanup to
    // BlockContents so the block points at the read buffer without copying.
#ifndef NDEBUG
    SharedCleanablePtr test_read_buffer_cleanup = read_buffer_cleanup;
    TEST_SYNC_POINT_CALLBACK("CreateAndPinBlockFromBuffer:ReadBufferCleanup",
                             &test_read_buffer_cleanup);
    const SharedCleanablePtr* effective_read_buffer_cleanup =
        &test_read_buffer_cleanup;
#else
    const SharedCleanablePtr* effective_read_buffer_cleanup =
        &read_buffer_cleanup;
#endif
    if (effective_read_buffer_cleanup->get() != nullptr) {
      tmp_contents.data = Slice(block_data, block.size());
      tmp_contents.cleanup = *effective_read_buffer_cleanup;
      tmp_contents.backing_size = block_size_with_trailer;
      tmp_contents.AssertSingleOwner();
#ifndef NDEBUG
      tmp_contents.has_trailer = rep->footer.GetBlockTrailerSize() > 0;
#endif
    } else if (read_buffer_requires_cleanup) {
      s = Status::InvalidArgument(
          "read-scoped block buffer provider requires read buffer cleanup");
    } else if (block_buffer_provider.has_value()) {
      s = CopyBufferToReadScopedBlockContents(
          Slice(block_data, block_size_with_trailer), block.size(),
          block_buffer_provider->get(), &tmp_contents);
    } else {
      s = CopyBufferToHeapBlockContents(
          Slice(block_data, block_size_with_trailer), block.size(),
          GetMemoryAllocator(rep->table_options), &tmp_contents);
    }
  } else {
    // Compressed blocks cannot view the read buffer as final block contents.
    // When a provider is configured, decompression allocates provider-backed
    // output and writes the uncompressed block directly into it.
    s = DecompressSerializedBlock(block_data, block.size(), compression_type,
                                  *decompressor, &tmp_contents, rep->ioptions,
                                  GetMemoryAllocator(rep->table_options),
                                  block_buffer_provider);
  }
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<Block_kData> block_holder;
  rep->create_context.Create(&block_holder, std::move(tmp_contents));
  pinned_block_entry.As<Block_kData>().SetOwnedValue(std::move(block_holder));
  return Status::OK();
}

struct ReadScopedIOConfig {
  bool use_data_block_cache = true;
  ReadScopedBlockBufferProviderRef block_buffer_provider;
  // True only when provider-backed file-read scratch is required because the
  // block is known to be uncompressed. If this is true, the read buffer cleanup
  // must be attached directly to BlockContents; otherwise maybe-compressed
  // reads may use ordinary scratch and copy/decompress into provider-backed
  // final contents after the compression type is known.
  bool read_buffer_requires_cleanup = false;
  bool use_read_scoped_direct_io = false;
  bool use_read_scoped_scratch = false;
};

static ReadScopedIOConfig GetReadScopedIOConfig(
    const BlockBasedTableOptions& table_options, const JobOptions& job_options,
    bool allow_mmap_reads, bool data_blocks_maybe_compressed, bool direct_io) {
  ReadScopedIOConfig config;
  config.use_data_block_cache = ShouldUseDataBlockCacheForIterator(
      table_options, job_options.read_options, allow_mmap_reads);
  config.block_buffer_provider = GetReadScopedBlockBufferProvider(
      job_options.read_options, allow_mmap_reads);

  const bool use_read_scoped_buffer =
      !config.use_data_block_cache &&
      config.block_buffer_provider.has_value() && !data_blocks_maybe_compressed;
  config.read_buffer_requires_cleanup = use_read_scoped_buffer;
  config.use_read_scoped_direct_io = direct_io && use_read_scoped_buffer;
  config.use_read_scoped_scratch = !direct_io && use_read_scoped_buffer;
  return config;
}

// State for async IO operations (implementation detail)
struct AsyncIOState {
  AsyncIOState() : offset(static_cast<uint64_t>(-1)) {}
  ~AsyncIOState() { read_req.status.PermitUncheckedError(); }

  AsyncIOState(const AsyncIOState&) = delete;
  AsyncIOState& operator=(const AsyncIOState&) = delete;
  AsyncIOState(AsyncIOState&&) = delete;
  AsyncIOState& operator=(AsyncIOState&&) = delete;

  ReadScopedBlockBufferProvider::Lease read_scoped_buf_lease;
  std::unique_ptr<char[]> buf;
  AlignedBuf aligned_buf;
  AlignedBuffer direct_io_buffer;
  void* io_handle = nullptr;
  IOHandleDeleter del_fn;
  uint64_t offset;
  // Captures ReadScopedIOConfig::read_buffer_requires_cleanup until async I/O
  // completion processing can attach the read buffer cleanup to BlockContents.
  bool read_buffer_requires_cleanup = false;
  std::vector<size_t> block_indices;
  std::vector<BlockHandle> blocks;
  FSReadRequest read_req;
  SystemClock* clock = nullptr;
  Statistics* statistics = nullptr;
  uint64_t submit_time_us = 0;
  std::atomic<bool> completed{false};
};

static uint64_t GetIODispatcherNowMicros(SystemClock* clock) {
  return clock != nullptr ? clock->NowMicros() : 0;
}

// Poll reaps POSIX CQEs even when they are ready, so ignore short reap
// overhead rather than treating every POSIX completion as a foreground stall.
constexpr uint64_t kRollingPrefetchPollStallNanos = 50 * 1000;

static void RecordIODispatcherTime(Statistics* statistics,
                                   uint32_t histogram_type,
                                   uint64_t start_time_us,
                                   uint64_t end_time_us) {
  if (statistics != nullptr && start_time_us != 0 &&
      end_time_us >= start_time_us) {
    RecordTimeToHistogram(statistics, histogram_type,
                          end_time_us - start_time_us);
  }
}

static void RecordIODispatcherBlockReads(size_t block_count,
                                         uint64_t block_read_bytes) {
  PERF_COUNTER_ADD(block_read_count, block_count);
  PERF_COUNTER_ADD(block_read_byte, block_read_bytes);
  PERF_COUNTER_ADD(data_block_read_byte, block_read_bytes);
}

// ReadSet destructor - clean up IO handles
// Must call AbortIO before deleting handles to avoid use-after-free when
// io_uring completions arrive for deleted handles.
ReadSet::~ReadSet() {
  // Release memory for any blocks still owned by this ReadSet. Pending queued
  // blocks have a non-zero block_sizes_ entry, but no memory was acquired for
  // them yet, so only pinned or async-dispatched blocks should release memory.
  for (size_t i = 0; i < block_sizes_.size(); ++i) {
    if (pinned_blocks_[i].GetValue() ||
        async_io_map_.find(i) != async_io_map_.end()) {
      ReleasePrefetchMemory(i);
    }
  }

  if (async_io_map_.empty()) {
    return;
  }

  // Collect unique pending IO handles (multiple block indices may share the
  // same async_state due to coalescing)
  std::vector<void*> pending_handles;
  std::unordered_set<void*> seen_handles;
  for (auto& pair : async_io_map_) {
    auto& async_state = pair.second;
    if (async_state->io_handle != nullptr &&
        seen_handles.find(async_state->io_handle) == seen_handles.end()) {
      pending_handles.push_back(async_state->io_handle);
      seen_handles.insert(async_state->io_handle);
    }
  }

  // Abort all pending IO operations before deleting handles
  if (!pending_handles.empty() && fs_) {
    // AbortIO cancels pending requests and waits for completions
    IOStatus s = fs_->AbortIO(pending_handles);
    (void)s;  // Ignore errors in destructor
  }

  // Now safe to delete the handles
  for (auto& pair : async_io_map_) {
    DeleteAsyncIOHandle(pair.second);
  }
}

// Main Read() method - transparently handles cache, async IO, and sync reads
Status ReadSet::ReadIndex(size_t block_index, CachableEntry<Block>* out) {
  // Bounds check
  if (block_index >= pinned_blocks_.size()) {
    return Status::InvalidArgument("Block index out of range");
  }

  if (prefetch_scheduler_) {
    Status scheduling_status =
        prefetch_scheduler_->OnBlockRequired(block_index);
    scheduling_status.PermitUncheckedError();
  }

  // Case 1: Block is already available (from cache or sync read during
  // SubmitJob)
  if (pinned_blocks_[block_index].GetValue()) {
    *out = std::move(pinned_blocks_[block_index]);
    // Release memory accounting for prefetched blocks. After moving the value
    // out, ReleaseBlock() and the destructor check pinned_blocks_.GetValue()
    // which will be null, so they won't release memory again.
    ReleasePrefetchMemory(block_index);
    if (prefetch_scheduler_) {
      prefetch_scheduler_->OnBlockHandedOff(block_index);
    }
    // Note: Statistics for this block were already counted during SubmitJob
    // (either as cache hit or sync read)
    return Status::OK();
  }

  // Case 2: Block has async IO in progress - poll and process
  if (job_->job_options.read_options.async_io) {
    auto it = async_io_map_.find(block_index);
    if (it != async_io_map_.end()) {
      auto async_state = it->second;
      // Get the number of blocks in this coalesced async request BEFORE polling
      // (since PollAndProcessAsyncIO will remove entries from the map)
      size_t num_blocks_in_request = async_state->block_indices.size();
      bool poll_stalled = false;
      if (Status s =
              PollAndProcessAsyncIO(std::move(async_state), &poll_stalled);
          !s.ok()) {
        return s;
      }
      if (poll_stalled && prefetch_scheduler_) {
        prefetch_scheduler_->OnForegroundStall(block_index);
      }
      // Count all blocks that were read in this async request
      num_async_reads_ += num_blocks_in_request;

      // After polling, the block should be in pinned_blocks_
      if (pinned_blocks_[block_index].GetValue()) {
        *out = std::move(pinned_blocks_[block_index]);
        // Release memory accounting (same as case 1 above)
        ReleasePrefetchMemory(block_index);
        if (prefetch_scheduler_) {
          prefetch_scheduler_->OnBlockHandedOff(block_index);
        }
        return Status::OK();
      }
      // A block-construction error is a prefetch miss. Fall through to the
      // ordinary read path so checksum corruption gets the same reconstruction
      // retry as a non-prefetched read.
    }
  }

  // Case 3: Block needs a synchronous read. Pending and skipped blocks were
  // never charged, while a failed prefetch releases its charge before reaching
  // this fallback. A nonzero block_sizes_ entry alone does not mean memory is
  // currently reserved.
  if (prefetch_scheduler_) {
    prefetch_scheduler_->OnBlockDiscarded(block_index);
  }

  Status s = SyncRead(block_index);
  if (s.ok()) {
    *out = std::move(pinned_blocks_[block_index]);
    num_sync_reads_++;
  }
  return s;
}

Status ReadSet::ReadOffset(size_t offset, CachableEntry<Block>* out) {
  if (sorted_block_indices_.empty()) {
    return Status::InvalidArgument("ReadSet not initialized");
  }

  // Use binary search on the sorted index to find the block containing offset.
  // sorted_block_indices_ contains original indices sorted by block offset.
  const auto& block_handles = job_->block_handles;

  // Binary search for the first block whose offset is > offset, then back up
  auto it = std::upper_bound(sorted_block_indices_.begin(),
                             sorted_block_indices_.end(), offset,
                             [&block_handles](size_t off, size_t idx) {
                               return off < block_handles[idx].offset();
                             });

  // If it == begin(), offset is before all blocks
  if (it == sorted_block_indices_.begin()) {
    return Status::InvalidArgument("Offset not found in any block");
  }

  // Back up to the candidate block (largest offset <= our offset)
  --it;
  size_t candidate_idx = *it;
  const auto& handle = block_handles[candidate_idx];

  // Check if offset falls within this block
  if (offset >= handle.offset() && offset < (handle.offset() + handle.size())) {
    return ReadIndex(candidate_idx, out);
  }

  return Status::InvalidArgument("Offset not found in any block");
}

void ReadSet::ReleaseBlock(size_t block_index) {
  if (block_index >= pinned_blocks_.size()) {
    return;
  }

  if (prefetch_scheduler_) {
    prefetch_scheduler_->OnBlockDiscarded(block_index);
  }

  // Release memory for a materialized prefetched block before unpinning. Queued
  // blocks have not acquired memory yet, and pending async blocks release their
  // memory budget in ReleaseAsyncIOForBlock().
  if (pinned_blocks_[block_index].GetValue()) {
    ReleasePrefetchMemory(block_index);
  }

  // Unpin the block from cache
  pinned_blocks_[block_index].Reset();
  ReleaseAsyncIOForBlock(block_index);
}

void ReadSet::ReleasePrefetchMemory(size_t block_index) {
  if (!prefetch_scheduler_ || block_index >= block_sizes_.size() ||
      block_sizes_[block_index] == 0) {
    return;
  }

  prefetch_scheduler_->ReleaseMemory(block_sizes_[block_index]);
  block_sizes_[block_index] = 0;
}

void ReadSet::ReleaseAsyncIOForBlock(size_t block_index) {
  auto map_iter = async_io_map_.find(block_index);
  if (map_iter == async_io_map_.end()) {
    return;
  }

  std::shared_ptr<AsyncIOState> async_state = map_iter->second;
  async_io_map_.erase(map_iter);
  ReleasePrefetchMemory(block_index);

  auto block_iter = std::find(async_state->block_indices.begin(),
                              async_state->block_indices.end(), block_index);
  if (block_iter != async_state->block_indices.end()) {
    const size_t state_index =
        static_cast<size_t>(block_iter - async_state->block_indices.begin());
    async_state->block_indices.erase(block_iter);
    async_state->blocks.erase(async_state->blocks.begin() + state_index);
  }

  if (!async_state->block_indices.empty()) {
    return;
  }

  if (async_state->io_handle != nullptr && fs_ != nullptr) {
    std::vector<void*> io_handles = {async_state->io_handle};
    TEST_SYNC_POINT_CALLBACK("ReadSet::ReleaseAsyncIOForBlock:AbortIO",
                             nullptr);
    IOStatus s = fs_->AbortIO(io_handles);
    s.PermitUncheckedError();
  }
  DeleteAsyncIOHandle(async_state);
}

void ReadSet::DeleteAsyncIOHandle(
    const std::shared_ptr<AsyncIOState>& async_state) {
  if (async_state->io_handle != nullptr && async_state->del_fn != nullptr) {
    async_state->del_fn(async_state->io_handle);
    async_state->io_handle = nullptr;
  }
}

bool ReadSet::IsBlockAvailable(size_t block_index) const {
  if (block_index >= pinned_blocks_.size()) {
    return false;
  }
  // Block is available if it hasn't been released (still has a value or
  // has pending async IO)
  return pinned_blocks_[block_index].GetValue() != nullptr ||
         async_io_map_.find(block_index) != async_io_map_.end();
}

// Poll and process async IO for a specific block
Status ReadSet::PollAndProcessAsyncIO(std::shared_ptr<AsyncIOState> async_state,
                                      bool* poll_stalled) {
  auto* rep = job_->table->get_rep();

  // Poll for IO completion using FileSystem Poll API
  std::vector<void*> io_handles = {async_state->io_handle};
  const bool completed_before_poll =
      async_state->completed.load(std::memory_order_acquire);
  const uint64_t poll_start_time_ns =
      async_state->clock != nullptr ? async_state->clock->NowNanos() : 0;
  const uint64_t poll_start_time_us =
      GetIODispatcherNowMicros(async_state->clock);
  RecordIODispatcherTime(async_state->statistics,
                         IO_DISPATCHER_ASYNC_READ_PREFETCH_LEAD_MICROS,
                         async_state->submit_time_us, poll_start_time_us);
  IOStatus io_s = rep->ioptions.env->GetFileSystem()->Poll(io_handles, 1);
  const uint64_t poll_end_time_ns =
      async_state->clock != nullptr ? async_state->clock->NowNanos() : 0;
  if (poll_stalled != nullptr) {
    *poll_stalled =
        !completed_before_poll && poll_start_time_ns != 0 &&
        poll_end_time_ns >= poll_start_time_ns &&
        poll_end_time_ns - poll_start_time_ns >= kRollingPrefetchPollStallNanos;
  }
  RecordIODispatcherTime(
      async_state->statistics, IO_DISPATCHER_ASYNC_READ_POLL_WAIT_MICROS,
      poll_start_time_us, GetIODispatcherNowMicros(async_state->clock));
  if (!io_s.ok()) {
    return io_s;
  }

  // Check for read errors
  if (!async_state->read_req.status.ok()) {
    return async_state->read_req.status;
  }

  // Use the result slice from the callback which has been correctly set
  // with any necessary alignment adjustments for direct IO
  const Slice& buffer_data = async_state->read_req.result;
  SharedCleanablePtr read_buffer_cleanup;
  if (async_state->read_scoped_buf_lease.cleanup.get() != nullptr) {
    read_buffer_cleanup = std::move(async_state->read_scoped_buf_lease.cleanup);
  }

  // Process all blocks in this async request. A block-construction error is
  // local to that block: siblings can still be served, while the failed block
  // is retried through ReadIndex's ordinary read path.
  std::vector<size_t> failed_block_indices;
  for (size_t i = 0; i < async_state->block_indices.size(); ++i) {
    const size_t idx = async_state->block_indices[i];
    const auto& block_handle = async_state->blocks[i];

    Status s = CreateAndPinBlockFromBuffer(
        job_, block_handle, async_state->offset, buffer_data,
        read_buffer_cleanup, async_state->read_buffer_requires_cleanup,
        pinned_blocks_[idx]);
    if (!s.ok()) {
      pinned_blocks_[idx].Reset();
      failed_block_indices.push_back(idx);
    }
  }

  // Clean up IO handle
  DeleteAsyncIOHandle(async_state);

  // Remove from map - all blocks in this request have been processed
  for (const auto idx : async_state->block_indices) {
    async_io_map_.erase(idx);
  }

  for (const size_t idx : failed_block_indices) {
    ReleasePrefetchMemory(idx);
  }

  return Status::OK();
}

// Perform synchronous read for a specific block
// This performs a direct synchronous read from disk when the block is not in
// cache
Status ReadSet::SyncRead(size_t block_index) {
  const auto& block_handle = job_->block_handles[block_index];
  auto* rep = job_->table->get_rep();
  const bool use_data_block_cache = ShouldUseDataBlockCacheForIterator(
      rep->table_options, job_->job_options.read_options,
      rep->ioptions.allow_mmap_reads);

  // Get dictionary-aware decompressor if available
  UnownedPtr<Decompressor> decompressor = rep->decompressor.get();
  CachableEntry<DecompressorDict> cached_dict;
  if (rep->uncompression_dict_reader) {
    Status s = rep->uncompression_dict_reader->GetOrReadUncompressionDictionary(
        nullptr, job_->job_options.read_options, nullptr, nullptr,
        &cached_dict);
    if (!s.ok()) {
      return s;
    }
    if (cached_dict.GetValue()) {
      decompressor = cached_dict.GetValue()->decompressor_.get();
    }
  }

  return job_->table->RetrieveBlock<Block_kData>(
      /*prefetch_buffer=*/nullptr, job_->job_options.read_options, block_handle,
      decompressor, &pinned_blocks_[block_index].As<Block_kData>(),
      /*get_context=*/nullptr, /*lookup_context=*/nullptr,
      /*for_compaction=*/false, use_data_block_cache,
      /*async_read=*/false, use_data_block_cache);
}

struct IODispatcherImpl::Impl : public PrefetchDispatchTarget,
                                public std::enable_shared_from_this<Impl> {
  explicit Impl(const IODispatcherOptions& options);
  ~Impl() override;

  // Non-copyable and non-movable
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;

  Status SubmitJob(const std::shared_ptr<IOJob>& job,
                   std::shared_ptr<ReadSet>* read_set);

 private:
  void ReleaseFailedSyncPrefetchMemory(
      const std::shared_ptr<ReadSet>& read_set,
      const std::vector<size_t>& block_indices);

  void PrepareIORequests(
      const std::shared_ptr<IOJob>& job,
      const std::vector<size_t>& block_indices_to_read,
      const std::vector<BlockHandle>& block_handles,
      std::vector<FSReadRequest>* read_reqs,
      std::vector<std::vector<size_t>>* coalesced_block_indices,
      uint64_t* out_prefetch_bytes = nullptr,
      uint64_t* out_num_coalesced_nonadjacent = nullptr);

  // Surface actual async IO errors to caller, but allow fallback for
  // unsupported cases. Returns block indices that need sync fallback.
  std::vector<size_t> ExecuteAsyncIO(
      const std::shared_ptr<IOJob>& job,
      const std::shared_ptr<ReadSet>& read_set,
      std::vector<FSReadRequest>& read_reqs,
      const std::vector<std::vector<size_t>>& coalesced_block_indices,
      Status* out_status);

  Status ExecuteSyncIO(
      const std::shared_ptr<IOJob>& job,
      const std::shared_ptr<ReadSet>& read_set,
      std::vector<FSReadRequest>& read_reqs,
      const std::vector<std::vector<size_t>>& coalesced_block_indices);

  // Materializes the block selection made by a scheduling policy.
  Status DispatchPrefetch(const std::shared_ptr<ReadSet>& read_set,
                          const std::shared_ptr<IOJob>& job,
                          const std::vector<size_t>& block_indices,
                          bool require_async_io) override;

  std::shared_ptr<PrefetchSchedulerFactory> scheduler_factory_;
};

IODispatcherImpl::Impl::Impl(const IODispatcherOptions& options)
    : scheduler_factory_(NewPrefetchSchedulerFactory(options)) {}

IODispatcherImpl::Impl::~Impl() {}

void IODispatcherImpl::Impl::ReleaseFailedSyncPrefetchMemory(
    const std::shared_ptr<ReadSet>& read_set,
    const std::vector<size_t>& block_indices) {
  for (const size_t block_index : block_indices) {
    if (read_set->pinned_blocks_[block_index].GetValue() == nullptr) {
      read_set->ReleasePrefetchMemory(block_index);
    }
  }
}

Status IODispatcherImpl::Impl::DispatchPrefetch(
    const std::shared_ptr<ReadSet>& read_set, const std::shared_ptr<IOJob>& job,
    const std::vector<size_t>& block_indices, bool require_async_io) {
  // Sync point for testing partial prefetch - passes number of blocks being
  // dispatched
  TEST_SYNC_POINT_CALLBACK("IODispatcherImpl::DispatchPrefetch:BlockCount",
                           const_cast<std::vector<size_t>*>(&block_indices));

  // Prepare and execute IO for the given blocks
  std::vector<FSReadRequest> read_reqs;
  std::vector<std::vector<size_t>> coalesced_block_indices;
  uint64_t dispatched_bytes = 0;
  uint64_t nonadjacent = 0;
  PrepareIORequests(job, block_indices, job->block_handles, &read_reqs,
                    &coalesced_block_indices, &dispatched_bytes, &nonadjacent);

  // Match RocksDB's existing MultiRead accounting by recording before IO.
  // Charge the full request spans so bytes in coalesced gaps are included.
  // Recording once before the async/sync split avoids double-charging fallback.
  RecordIODispatcherBlockReads(block_indices.size(), dispatched_bytes);

  // Account for the prefetch IO issued by this dispatch. Counted here (once per
  // dispatched block, before the async/sync split) rather than at completion so
  // async and memory-deferred prefetches are all captured, and so blocks that
  // fall back from async to sync are not double counted.
  read_set->num_blocks_prefetched_ += block_indices.size();
  read_set->num_io_requests_ += read_reqs.size();
  read_set->prefetch_bytes_ += dispatched_bytes;
  read_set->num_coalesced_nonadjacent_ += nonadjacent;

  if (job->job_options.read_options.async_io) {
    Status async_status;
    std::vector<size_t> fallback_indices = ExecuteAsyncIO(
        job, read_set, read_reqs, coalesced_block_indices, &async_status);

    // For blocks where async is not supported, do sync IO
    if (!fallback_indices.empty()) {
      std::vector<FSReadRequest> sync_read_reqs;
      std::vector<std::vector<size_t>> sync_coalesced_indices;
      PrepareIORequests(job, fallback_indices, job->block_handles,
                        &sync_read_reqs, &sync_coalesced_indices);
      // Prefetch errors are ignored - user will get the error when reading
      Status s =
          ExecuteSyncIO(job, read_set, sync_read_reqs, sync_coalesced_indices);
      if (!s.ok()) {
        ReleaseFailedSyncPrefetchMemory(read_set, fallback_indices);
      }
      s.PermitUncheckedError();
      read_set->num_sync_reads_ += fallback_indices.size();
    }
    if (!async_status.ok()) {
      if (require_async_io) {
        return async_status;
      }
      // Async errors are also ignored - user will get the error when reading
      async_status.PermitUncheckedError();
    }
    if (require_async_io && !fallback_indices.empty()) {
      return Status::NotSupported("rolling prefetch requires async I/O");
    }
  } else {
    // Prefetch errors are ignored - user will get the error when reading
    Status s = ExecuteSyncIO(job, read_set, read_reqs, coalesced_block_indices);
    if (!s.ok()) {
      ReleaseFailedSyncPrefetchMemory(read_set, block_indices);
    }
    s.PermitUncheckedError();
    read_set->num_sync_reads_ += block_indices.size();
  }
  return Status::OK();
}

Status IODispatcherImpl::Impl::SubmitJob(const std::shared_ptr<IOJob>& job,
                                         std::shared_ptr<ReadSet>* read_set) {
  if (!read_set) {
    return Status::InvalidArgument("read_set output parameter is null");
  }

  auto rs = std::make_shared<ReadSet>();

  // Initialize ReadSet
  rs->job_ = job;
  rs->fs_ = job->table->get_rep()->ioptions.env->GetFileSystem();
  rs->pinned_blocks_.resize(job->block_handles.size());
  rs->block_sizes_.resize(job->block_handles.size(), 0);

  // Build sorted index for O(log n) ReadOffset lookups via binary search.
  // sorted_block_indices_[i] = original index of i-th smallest block by offset.
  rs->sorted_block_indices_.resize(job->block_handles.size());
  for (size_t i = 0; i < job->block_handles.size(); ++i) {
    rs->sorted_block_indices_[i] = i;
  }
  const bool block_handles_are_sorted =
      job->job_options.block_handles_are_sorted;
  assert(!block_handles_are_sorted ||
         BlockIndicesAreSortedByOffset(job->block_handles,
                                       rs->sorted_block_indices_));
  if (!block_handles_are_sorted) {
    TEST_SYNC_POINT("IODispatcherImpl::SubmitJob:SortBlockHandles");
    std::sort(rs->sorted_block_indices_.begin(),
              rs->sorted_block_indices_.end(), [&job](size_t a, size_t b) {
                return job->block_handles[a].offset() <
                       job->block_handles[b].offset();
              });
  }

  // Step 1: Check cache and pin cached blocks. Iterate in offset order so all
  // downstream private helpers can assume block index vectors are already
  // sorted.
  std::vector<size_t> block_indices_to_read;
  block_indices_to_read.reserve(job->block_handles.size());
  const bool use_data_block_cache = ShouldUseDataBlockCacheForIterator(
      job->table->get_rep()->table_options, job->job_options.read_options,
      job->table->get_rep()->ioptions.allow_mmap_reads);

  if (!use_data_block_cache) {
    block_indices_to_read = rs->sorted_block_indices_;
  } else {
    for (size_t i : rs->sorted_block_indices_) {
      const auto& data_block_handle = job->block_handles[i];

      // Lookup and pin block in cache
      Status s = job->table->LookupAndPinBlocksInCache<Block_kData>(
          job->job_options.read_options, data_block_handle,
          &(rs->pinned_blocks_)[i].As<Block_kData>());

      if (!s.ok()) {
        continue;
      }

      if (!(rs->pinned_blocks_)[i].GetValue()) {
        // Block not in cache - needs to be read from disk
        block_indices_to_read.emplace_back(i);
      }
    }
  }

  // Count cache hits (blocks that were found in cache during lookup above)
  rs->num_cache_hits_ =
      job->block_handles.size() - block_indices_to_read.size();

  if (block_indices_to_read.empty()) {
    *read_set = std::move(rs);
    return Status::OK();
  }

  // Calculate block sizes for uncached blocks
  for (const auto& idx : block_indices_to_read) {
    size_t block_size =
        BlockBasedTable::BlockSizeWithTrailer(job->block_handles[idx]);
    rs->block_sizes_[idx] = block_size;
  }

  rs->prefetch_scheduler_ = scheduler_factory_->Create(
      shared_from_this(), rs, job, block_handles_are_sorted);
  Status status = rs->prefetch_scheduler_->Start(block_indices_to_read);
  status.PermitUncheckedError();

  *read_set = std::move(rs);
  return Status::OK();
}

void IODispatcherImpl::Impl::PrepareIORequests(
    const std::shared_ptr<IOJob>& job,
    const std::vector<size_t>& block_indices_to_read,
    const std::vector<BlockHandle>& block_handles,
    std::vector<FSReadRequest>* read_reqs,
    std::vector<std::vector<size_t>>* coalesced_block_indices,
    uint64_t* out_prefetch_bytes, uint64_t* out_num_coalesced_nonadjacent) {
  assert(BlockIndicesAreSortedByOffset(block_handles, block_indices_to_read));
  assert(coalesced_block_indices->empty());
  coalesced_block_indices->resize(1);

  uint64_t nonadjacent = 0;
  for (const auto& block_idx : block_indices_to_read) {
    if (!coalesced_block_indices->back().empty()) {
      // Check if we can coalesce with previous block
      const auto& last_block_handle =
          block_handles[coalesced_block_indices->back().back()];
      uint64_t last_block_end =
          last_block_handle.offset() +
          BlockBasedTable::BlockSizeWithTrailer(last_block_handle);
      uint64_t current_start = block_handles[block_idx].offset();

      if (current_start >
          last_block_end + job->job_options.io_coalesce_threshold) {
        // Gap too large - start new IO request
        coalesced_block_indices->emplace_back();
      } else if (current_start > last_block_end) {
        // Within coalesce threshold but not adjacent
        ++nonadjacent;
      }
    }
    coalesced_block_indices->back().emplace_back(block_idx);
  }

  // Create FSReadRequest for each coalesced group
  assert(read_reqs->empty());
  read_reqs->reserve(coalesced_block_indices->size());

  uint64_t total_bytes = 0;
  for (const auto& block_indices : *coalesced_block_indices) {
    assert(!block_indices.empty());

    // Find the min and max offsets in this coalesced group
    // Since blocks are now sorted, first has min offset and last has max
    const auto& first_block_handle = block_handles[block_indices[0]];
    const auto& last_block_handle = block_handles[block_indices.back()];

    const auto start_offset = first_block_handle.offset();
    const auto end_offset =
        last_block_handle.offset() +
        BlockBasedTable::BlockSizeWithTrailer(last_block_handle);

    assert(end_offset > start_offset);

    read_reqs->emplace_back();
    read_reqs->back().offset = start_offset;
    read_reqs->back().len = end_offset - start_offset;
    read_reqs->back().scratch = nullptr;
    total_bytes += read_reqs->back().len;
  }

  if (out_prefetch_bytes) {
    *out_prefetch_bytes = total_bytes;
  }
  if (out_num_coalesced_nonadjacent) {
    *out_num_coalesced_nonadjacent = nonadjacent;
  }
}

std::vector<size_t> IODispatcherImpl::Impl::ExecuteAsyncIO(
    const std::shared_ptr<IOJob>& job, const std::shared_ptr<ReadSet>& read_set,
    std::vector<FSReadRequest>& read_reqs,
    const std::vector<std::vector<size_t>>& coalesced_block_indices,
    Status* out_status) {
  std::vector<size_t> fallback_block_indices;
  *out_status = Status::OK();

  // Get file and IO options
  auto* rep = job->table->get_rep();
  IOOptions io_opts;
  Status s =
      rep->file->PrepareIOOptions(job->job_options.read_options, io_opts);
  if (!s.ok()) {
    *out_status = s;
    return fallback_block_indices;
  }

  const bool direct_io = rep->file->use_direct_io();
  const ReadScopedIOConfig read_scoped_io = GetReadScopedIOConfig(
      rep->table_options, job->job_options, rep->ioptions.allow_mmap_reads,
      rep->decompressor != nullptr, direct_io);

  // Submit async read requests and store them in the ReadSet
  for (size_t i = 0; i < read_reqs.size(); ++i) {
    auto async_state = std::make_shared<AsyncIOState>();

    async_state->offset = read_reqs[i].offset;
    async_state->read_buffer_requires_cleanup =
        read_scoped_io.read_buffer_requires_cleanup;
    async_state->block_indices = coalesced_block_indices[i];
    async_state->read_req = std::move(read_reqs[i]);
    async_state->clock = rep->ioptions.clock;
    async_state->statistics = rep->ioptions.stats;
    async_state->submit_time_us = GetIODispatcherNowMicros(async_state->clock);

    for (const auto idx : coalesced_block_indices[i]) {
      async_state->blocks.emplace_back(job->block_handles[idx]);
    }

    AlignedBuffer::Allocator direct_io_allocator;
    AlignedBufferAllocationContext direct_io_context{
        &async_state->direct_io_buffer};
    AlignedBufferAllocationContext* direct_io_context_arg = nullptr;
    if (read_scoped_io.use_read_scoped_direct_io) {
      assert(read_scoped_io.block_buffer_provider.has_value());
      // This allocator borrows the AsyncIOState lease member and is invoked
      // synchronously by RandomAccessFileReader::ReadAsync before it returns.
      // The resulting AlignedBuffer owner keeps the read-scoped cleanup alive
      // until the async read is processed or abandoned. Uncompressed blocks
      // later attach this cleanup directly to BlockContents.
      direct_io_allocator = MakeReadScopedAlignedBufferAllocator(
          read_scoped_io.block_buffer_provider,
          &async_state->read_scoped_buf_lease);
      direct_io_context.allocator = &direct_io_allocator;
      direct_io_context_arg = &direct_io_context;
    }

    if (direct_io) {
      async_state->read_req.scratch = nullptr;
    } else if (read_scoped_io.use_read_scoped_scratch) {
      assert(read_scoped_io.block_buffer_provider.has_value());
      // Non-direct I/O writes into provider-backed scratch. For uncompressed
      // blocks this scratch becomes the final BlockContents backing.
      s = AllocateReadScopedBlockBuffer(
          read_scoped_io.block_buffer_provider->get(),
          async_state->read_req.len, 1, &async_state->read_scoped_buf_lease);
      if (!s.ok()) {
        *out_status = s;
        return fallback_block_indices;
      }
      async_state->read_req.scratch = async_state->read_scoped_buf_lease.data;
    } else {
      async_state->buf.reset(new char[async_state->read_req.len]);
      async_state->read_req.scratch = async_state->buf.get();
    }

    // Callback for async read completion
    // Store the result slice and status back into async_state so we can access
    // them after Poll() completes.
    auto cb = [](const FSReadRequest& req, void* cb_arg) {
      auto* state = static_cast<AsyncIOState*>(cb_arg);
      state->read_req.result = req.result;
      state->read_req.status = req.status;
      state->completed.store(true, std::memory_order_release);
      RecordIODispatcherTime(
          state->statistics,
          IO_DISPATCHER_ASYNC_READ_OBSERVED_COMPLETION_MICROS,
          state->submit_time_us, GetIODispatcherNowMicros(state->clock));
    };

    s = rep->file->ReadAsync(
        async_state->read_req, io_opts, cb, async_state.get(),
        &async_state->io_handle, &async_state->del_fn,
        read_scoped_io.use_read_scoped_direct_io
            ? nullptr
            : (direct_io ? &async_state->aligned_buf : nullptr),
        /*dbg=*/nullptr, direct_io_context_arg);

    if (s.IsNotSupported()) {
      // Async IO may be compiled in but unavailable at runtime. Fall back to
      // the synchronous coalesced path for these blocks.
      for (const auto idx : coalesced_block_indices[i]) {
        fallback_block_indices.push_back(idx);
      }
      continue;
    }

    if (!s.ok()) {
      // Actual error - surface to caller
      *out_status = s;
      return fallback_block_indices;
    }

    if (async_state->io_handle == nullptr) {
      // Async IO not supported - add to fallback list for sync IO
      for (const auto idx : coalesced_block_indices[i]) {
        fallback_block_indices.push_back(idx);
      }
      continue;
    }

    // Add async state to map for all blocks in this request
    for (const auto idx : async_state->block_indices) {
      read_set->async_io_map_[idx] = async_state;
    }
  }

  return fallback_block_indices;
}

Status IODispatcherImpl::Impl::ExecuteSyncIO(
    const std::shared_ptr<IOJob>& job, const std::shared_ptr<ReadSet>& read_set,
    std::vector<FSReadRequest>& read_reqs,
    const std::vector<std::vector<size_t>>& coalesced_block_indices) {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
  // Some error exits intentionally ignore FSReadRequest::status values: setup
  // failures happen before MultiRead populates them, top-level MultiRead
  // errors make per-request statuses irrelevant, and production short-circuits
  // on the first per-request error.
  auto permit_unchecked_read_req_statuses = [&read_reqs]() {
    for (const FSReadRequest& read_req : read_reqs) {
      read_req.status.PermitUncheckedError();
    }
  };
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED

  // Get file and IO options
  auto* rep = job->table->get_rep();
  IOOptions io_opts;
  if (Status s =
          rep->file->PrepareIOOptions(job->job_options.read_options, io_opts);
      !s.ok()) {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    permit_unchecked_read_req_statuses();
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
    return s;
  }

  const bool direct_io = rep->file->use_direct_io();
  const ReadScopedIOConfig read_scoped_io = GetReadScopedIOConfig(
      rep->table_options, job->job_options, rep->ioptions.allow_mmap_reads,
      rep->decompressor != nullptr, direct_io);

  // Setup scratch buffers for MultiRead
  std::unique_ptr<char[]> buf;
  std::vector<ReadScopedBlockBufferProvider::Lease> read_scoped_leases;
  std::vector<SharedCleanablePtr> read_req_cleanups;

  ReadScopedBlockBufferProvider::Lease read_scoped_direct_io_lease;
  AlignedBuffer direct_io_buffer;
  AlignedBuffer::Allocator direct_io_allocator;
  AlignedBufferAllocationContext direct_io_context{&direct_io_buffer};
  if (read_scoped_io.use_read_scoped_direct_io) {
    assert(read_scoped_io.block_buffer_provider.has_value());
    // This allocator borrows stack-local lease state from the synchronous
    // ExecuteSyncIO call. RandomAccessFileReader::MultiRead asks it to allocate
    // before returning, and uncompressed blocks later attach the lease cleanup
    // directly to BlockContents.
    direct_io_allocator = MakeReadScopedAlignedBufferAllocator(
        read_scoped_io.block_buffer_provider, &read_scoped_direct_io_lease);
    direct_io_context.allocator = &direct_io_allocator;
  }

  if (direct_io) {
    for (auto& read_req : read_reqs) {
      read_req.scratch = nullptr;
    }
  } else if (read_scoped_io.use_read_scoped_scratch) {
    assert(read_scoped_io.block_buffer_provider.has_value());
    // Non-direct I/O writes into provider-backed scratch. For uncompressed
    // blocks this scratch becomes the final BlockContents backing.
    read_scoped_leases.resize(read_reqs.size());
    for (size_t i = 0; i < read_reqs.size(); ++i) {
      if (Status s = AllocateReadScopedBlockBuffer(
              read_scoped_io.block_buffer_provider->get(), read_reqs[i].len, 1,
              &read_scoped_leases[i]);
          !s.ok()) {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
        permit_unchecked_read_req_statuses();
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
        return s;
      }
      read_reqs[i].scratch = read_scoped_leases[i].data;
    }
  } else {
    // Allocate a single contiguous buffer for all requests
    size_t total_len = 0;
    for (const auto& req : read_reqs) {
      total_len += req.len;
    }
    buf.reset(new char[total_len]);
    size_t offset = 0;
    for (auto& read_req : read_reqs) {
      read_req.scratch = buf.get() + offset;
      offset += read_req.len;
    }
  }

  // Execute MultiRead
  if (Status s =
          rep->file->MultiRead(io_opts, read_reqs.data(), read_reqs.size(),
                               &direct_io_context, /*dbg=*/nullptr);
      !s.ok()) {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    permit_unchecked_read_req_statuses();
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
    return s;
  }

  for (const auto& rq : read_reqs) {
    if (!rq.status.ok()) {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
      permit_unchecked_read_req_statuses();
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
      return rq.status;
    }
  }

  if (read_scoped_io.use_read_scoped_direct_io) {
    read_req_cleanups.assign(read_reqs.size(),
                             read_scoped_direct_io_lease.cleanup);
  } else if (read_scoped_io.use_read_scoped_scratch) {
    read_req_cleanups.resize(read_reqs.size());
    for (size_t i = 0; i < read_scoped_leases.size(); ++i) {
      read_req_cleanups[i] = std::move(read_scoped_leases[i].cleanup);
    }
  }

  // Process all blocks from the MultiRead results
  for (size_t i = 0; i < coalesced_block_indices.size(); ++i) {
    const auto& read_req = read_reqs[i];
    for (const auto& block_idx : coalesced_block_indices[i]) {
      const auto& block_handle = job->block_handles[block_idx];

      Status create_status = CreateAndPinBlockFromBuffer(
          job, block_handle, read_req.offset, read_req.result,
          i < read_req_cleanups.size() ? read_req_cleanups[i]
                                       : SharedCleanablePtr(),
          read_scoped_io.read_buffer_requires_cleanup,
          read_set->pinned_blocks_[block_idx]);
      if (!create_status.ok()) {
        return create_status;
      }
    }
  }

  return Status::OK();
}

IODispatcherImpl::IODispatcherImpl()
    : impl_(std::make_shared<Impl>(IODispatcherOptions())) {}

IODispatcherImpl::IODispatcherImpl(const IODispatcherOptions& options)
    : impl_(std::make_shared<Impl>(options)) {}

IODispatcherImpl::~IODispatcherImpl() = default;

Status IODispatcherImpl::SubmitJob(const std::shared_ptr<IOJob>& job,
                                   std::shared_ptr<ReadSet>* read_set) {
  return impl_->SubmitJob(job, read_set);
}

IODispatcher* NewIODispatcher() { return new IODispatcherImpl(); }

IODispatcher* NewIODispatcher(const IODispatcherOptions& options) {
  return new IODispatcherImpl(options);
}

}  // namespace ROCKSDB_NAMESPACE
