//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/options.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class FileSystem;
class Statistics;

// Forward declaration for internal implementation
class PrefetchScheduler;

// EXPERIMENTAL
// The interface defined in this file is experimental and the behavior and
// interface are subject to change without notice.

// Options for configuring IODispatcher behavior
struct IODispatcherOptions {
  // Maximum memory (in bytes) for prefetching across all ReadSets.
  // When this limit is reached, SubmitJob() blocks until memory is released.
  // Set to 0 (default) for unlimited prefetch memory.
  size_t max_prefetch_memory_bytes = 0;

  // Optional statistics for tracking memory limiter metrics
  Statistics* statistics = nullptr;
};

/*
 * IODispatcher is a class that allows users to submit groups of IO jobs to be
 * dispatched asynchronously (or synchronously), upon submission the
 * IODispatcher will return a ReadSet which act as an ownership object of those
 * IOs. Users read from their readset when they require the data, and either
 * poll for completion of the block, or read synchronously if the block is not
 * in cache at that point.
 *
 * ReadSets have RAII semantics, meaning on destruction they will cancel any on
 * going IO, and release the underlying pinned blocks.
 *
 * IODispatcher main goal is to act as control plane for all readers using the
 * dispatcher, allowing for future ratelimiting and smarter dispatching policies
 * in the future.
 *
 * Example 1: Basic Usage
 * ----------------------
 * // Submitting an IO job and reading blocks:
 * //
 * // std::shared_ptr<IOJob> job = std::make_shared<IOJob>();
 * // job->table = table_reader;  // Provided BlockBasedTable*
 * // job->job_options.io_coalesce_threshold = 32 * 1024;
 * // job->job_options.read_options = read_options;  // Provided ReadOptions
 * //
 * // // Populate the job with block handles (e.g., from an index/iterator)
 * // job->block_handles.push_back(handle1);
 * // job->block_handles.push_back(handle2);
 * // job->block_handles.push_back(handle3);
 * //
 * // std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher());
 * // std::shared_ptr<ReadSet> read_set;
 * // Status s = dispatcher->SubmitJob(job, &read_set);
 * // if (!s.ok()) {
 * //   // Handle submit error
 * // }
 * //
 * // // Read by index
 * // for (size_t i = 1; i < job->block_handles.size(); ++i) {
 * //   CachableEntry<Block> block_entry;
 * //   Status rs = read_set->ReadIndex(i, &block_entry);
 * //   if (!rs.ok()) {
 * //     // Handle read error
 * //     continue;
 * //   }
 * //   // Use block_entry (block contents are pinned here)
 * // }
 * //
 * // // Or read by byte offset
 * // {
 * //   size_t offset =
 static_cast<size_t>(job->block_handles.front().offset());
 * //   CachableEntry<Block> block_entry;
 * //   Status rs = read_set->ReadOffset(offset, &block_entry);
 * //   if (rs.ok()) {
 * //     // Use block_entry
 * //   }
 * // }
 * //
 * // // Stats
 * // uint64_t cache_hits = read_set->GetNumCacheHits();
 * // uint64_t async_reads = read_set->GetNumAsyncReads();
 * // uint64_t sync_reads = read_set->GetNumSyncReads();
 *
 * Example 2: Memory-Limited Prefetching
 * -------------------------------------
 * // Configure a memory budget for prefetching to prevent unbounded memory use.
 * // When the budget is exceeded, IODispatcher uses "partial prefetch":
 * //   - Dispatches as many blocks as fit in available memory (earlier first)
 * //   - Queues remaining blocks for later dispatch when memory is released
 * //   - Never blocks on SubmitJob - remaining blocks are read on-demand
 * //
 * // IODispatcherOptions opts;
 * // opts.max_prefetch_memory_bytes = 64 * 1024 * 1024;  // 64MB budget
 * // opts.statistics = db_options.statistics.get();      // Optional metrics
 * //
 * // std::unique_ptr<IODispatcher> dispatcher(NewIODispatcher(opts));
 * //
 * // // Submit a job that needs more memory than available
 * // // Partial prefetch will dispatch what fits immediately
 * // std::shared_ptr<ReadSet> read_set;
 * // Status s = dispatcher->SubmitJob(job, &read_set);  // Never blocks
 * //
 * // // Read blocks in order - earlier blocks are more likely to be prefetched
 * // for (size_t i = 0; i < job->block_handles.size(); ++i) {
 * //   CachableEntry<Block> block;
 * //   Status rs = read_set->ReadIndex(i, &block);
 * //   // Use block...
 * //
 * //   // Release block when done to free memory for pending prefetches
 * //   read_set->ReleaseBlock(i);  // Triggers dispatch of queued blocks
 * // }
 * //
 * // Memory limiting statistics (when statistics is configured):
 * // - PREFETCH_MEMORY_BYTES_GRANTED: Total bytes acquired for prefetching
 * // - PREFETCH_MEMORY_BYTES_RELEASED: Total bytes released after use
 * // - PREFETCH_MEMORY_REQUESTS_BLOCKED: Number of blocks that couldn't be
 * //   prefetched immediately due to memory pressure

 */

class BlockHandle;
struct ReadOptions;
struct AsyncIOState;

template <typename T>
class CachableEntry;
class Block;
class BlockBasedTable;

struct JobOptions {
  uint64_t io_coalesce_threshold = 16 * 1024;

  // Maximum physical span of one rolling window and the initial batch budget.
  // Each batch contains the longest prefix of windows that fits; a single
  // oversized window is submitted alone.
  uint64_t rolling_prefetch_window_size = 0;

  // Maximum adaptive rolling batch budget. Zero uses
  // rolling_prefetch_window_size. A foreground wait doubles the current budget
  // up to this limit.
  uint64_t rolling_prefetch_max_batch_size = 0;
  ReadOptions read_options;

  // True when IOJob::block_handles is already sorted by file offset. Callers
  // that can guarantee sorted order may set this to let IODispatcher skip
  // defensive sorting before cache lookup and coalescing.
  bool block_handles_are_sorted = false;
};

class IOJob {
 public:
  std::vector<BlockHandle> block_handles;

  // Table reader for accessing block cache and index
  BlockBasedTable* table = nullptr;

  // Job execution options
  JobOptions job_options;
};

/*
 * ReadSet represents a set of blocks that may be in cache, being read
 * asynchronously, or need to be read synchronously. The Read() method
 * transparently handles all three cases.
 */
class ReadSet {
 public:
  ReadSet() = default;
  ~ReadSet();

  ReadSet(const ReadSet&) = delete;
  ReadSet& operator=(const ReadSet&) = delete;
  ReadSet(ReadSet&&) noexcept = delete;
  ReadSet& operator=(ReadSet&&) noexcept = delete;

  // Read a block by index
  // - If the block is in cache, returns it immediately
  // - If the block is being read asynchronously, polls for completion and
  // returns it
  // - If the block needs to be read, performs a synchronous read and returns it
  //
  // block_index: Index into the original IOJob's block_handles vector
  // out: Output parameter for the pinned block entry
  //
  // Returns: Status::OK() on success, error status otherwise
  Status ReadIndex(size_t block_index, CachableEntry<Block>* out);
  // Read a block by offset
  // - If the block is in cache, returns it immediately
  // - If the block is being read asynchronously, polls for completion and
  // returns it
  // - If the block needs to be read, performs a synchronous read and returns it

  // block_offset: Byte Offset into the SST file of the block.

  // out: Output parameter for the pinned block entry
  Status ReadOffset(size_t offset, CachableEntry<Block>* out);

  // Release a block by index, unpinning it from cache.
  // After this call, ReadIndex() for this block will return an error.
  // This is useful for eager memory reclamation when blocks are no longer
  // needed.
  void ReleaseBlock(size_t block_index);

  // Check if a block at the given index is still available (not released).
  // Returns true if the block can be read, false otherwise.
  bool IsBlockAvailable(size_t block_index) const;

  // Statistics accessors
  uint64_t GetNumSyncReads() const { return num_sync_reads_; }
  uint64_t GetNumAsyncReads() const { return num_async_reads_; }
  uint64_t GetNumCacheHits() const { return num_cache_hits_; }
  // Blocks for which a prefetch IO was dispatched (sync + async), the number of
  // (coalesced) IO requests issued for them, the total bytes those requests
  // span, and how many blocks were coalesced across a non-adjacent gap.
  uint64_t GetNumBlocksPrefetched() const { return num_blocks_prefetched_; }
  uint64_t GetNumIORequests() const { return num_io_requests_; }
  uint64_t GetPrefetchBytes() const { return prefetch_bytes_; }
  uint64_t GetNumCoalescedNonadjacent() const {
    return num_coalesced_nonadjacent_;
  }

 private:
  friend class IODispatcherImpl;

  // Job data
  std::shared_ptr<IOJob> job_;

  // FileSystem for calling AbortIO in destructor
  std::shared_ptr<FileSystem> fs_;

  // Storage for pinned blocks (one per block handle in the job)
  std::vector<CachableEntry<Block>> pinned_blocks_;

  // Sorted index for binary search in ReadOffset.
  // sorted_block_indices_[i] is the original index of the i-th smallest block
  // by offset. Built once during SubmitJob for O(log n) ReadOffset lookups.
  std::vector<size_t> sorted_block_indices_;

  // Map from block index to async IO state for blocks being read
  // asynchronously. Multiple block indices may map to the same async state when
  // blocks are coalesced into a single IO request.
  std::unordered_map<size_t, std::shared_ptr<AsyncIOState>> async_io_map_;

  // Owns the configured scheduling policy and its per-ReadSet state. Null when
  // SubmitJob finds every requested block in cache, since that ReadSet has no
  // prefetch work or prefetch memory to schedule or release. I/O buffers and
  // asynchronous operations remain owned by ReadSet.
  std::shared_ptr<PrefetchScheduler> prefetch_scheduler_;

  // Size of each block (parallel to pinned_blocks_) for memory accounting
  std::vector<size_t> block_sizes_;

  // Statistics counters
  std::atomic<uint64_t> num_sync_reads_ = 0;
  std::atomic<uint64_t> num_async_reads_ = 0;
  std::atomic<uint64_t> num_cache_hits_ = 0;
  // Prefetch IO accounting, incremented as prefetch IO is dispatched (not on
  // completion) so async and memory-deferred prefetches are all captured.
  std::atomic<uint64_t> num_blocks_prefetched_ = 0;
  std::atomic<uint64_t> num_io_requests_ = 0;
  std::atomic<uint64_t> prefetch_bytes_ = 0;
  std::atomic<uint64_t> num_coalesced_nonadjacent_ = 0;

  // Poll and process a specific async IO request. Takes ownership by value so
  // async_io_map_ entries can be erased without destroying the state mid-loop.
  Status PollAndProcessAsyncIO(std::shared_ptr<AsyncIOState> async_state,
                               bool* poll_stalled);

  // Release memory budget acquired for a prefetched block.
  void ReleasePrefetchMemory(size_t block_index);

  // Stop tracking one block from an in-flight async IO request. If this was
  // the last block using the request, abort and delete the IO handle before the
  // async state is released.
  void ReleaseAsyncIOForBlock(size_t block_index);

  // Delete a completed or aborted async IO handle exactly once.
  void DeleteAsyncIOHandle(const std::shared_ptr<AsyncIOState>& async_state);

  // Perform synchronous read for a specific block
  Status SyncRead(size_t block_index);

};

/*
 * IODispatcher handles IO operations synchronously or asynchronously based
 * on JobOptions. When async is true, it uses ReadAsync; when false, it uses
 * standard synchronous reads.
 * */
class IODispatcher {
 protected:
  IODispatcher() = default;

 public:
  virtual ~IODispatcher() {}

  IODispatcher(const IODispatcher&) = delete;
  IODispatcher& operator=(const IODispatcher&) = delete;
  IODispatcher(IODispatcher&&) = delete;
  IODispatcher& operator=(IODispatcher&&) = delete;

  // Submit a job for IO processing
  // job: The IO job to submit
  // read_set: Output parameter that will be populated with the ReadSet on
  // success Returns: Status::OK() on success, error status otherwise
  virtual Status SubmitJob(const std::shared_ptr<IOJob>& job,
                           std::shared_ptr<ReadSet>* read_set) = 0;
};

// Create IODispatcher with default options (no memory limit)
IODispatcher* NewIODispatcher();

// Create IODispatcher with custom options
IODispatcher* NewIODispatcher(const IODispatcherOptions& options);

// TrackingIODispatcher wraps another IODispatcher and tracks all ReadSets
// created. This is useful for testing to verify IO statistics.
class TrackingIODispatcher : public IODispatcher {
 public:
  TrackingIODispatcher() : impl_(NewIODispatcher()) {}
  explicit TrackingIODispatcher(IODispatcher* impl) : impl_(impl) {}

  Status SubmitJob(const std::shared_ptr<IOJob>& job,
                   std::shared_ptr<ReadSet>* read_set) override {
    Status s = impl_->SubmitJob(job, read_set);
    if (s.ok() && read_set && *read_set) {
      read_sets_.push_back(*read_set);
    }
    return s;
  }

  // Get all ReadSets created by this dispatcher
  const std::vector<std::shared_ptr<ReadSet>>& GetReadSets() const {
    return read_sets_;
  }

  // Get aggregated statistics from all ReadSets
  uint64_t GetTotalSyncReads() const {
    uint64_t total = 0;
    for (const auto& rs : read_sets_) {
      total += rs->GetNumSyncReads();
    }
    return total;
  }

  uint64_t GetTotalAsyncReads() const {
    uint64_t total = 0;
    for (const auto& rs : read_sets_) {
      total += rs->GetNumAsyncReads();
    }
    return total;
  }

  uint64_t GetTotalCacheHits() const {
    uint64_t total = 0;
    for (const auto& rs : read_sets_) {
      total += rs->GetNumCacheHits();
    }
    return total;
  }

  // Get total IO operations (sum of all types)
  uint64_t GetTotalIOOperations() const {
    return GetTotalSyncReads() + GetTotalAsyncReads() + GetTotalCacheHits();
  }

  // Clear tracked ReadSets
  void ClearReadSets() { read_sets_.clear(); }

 private:
  std::unique_ptr<IODispatcher> impl_;
  std::vector<std::shared_ptr<ReadSet>> read_sets_;
};

}  // namespace ROCKSDB_NAMESPACE
