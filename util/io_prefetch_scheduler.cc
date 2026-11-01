//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/io_prefetch_scheduler.h"

#include <algorithm>
#include <atomic>
#include <deque>
#include <limits>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/io_dispatcher.h"
#include "table/block_based/block_based_table_reader.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace {

struct PrefetchGroup {
  std::vector<size_t> block_indices;
  size_t total_bytes = 0;
  size_t rolling_window_index = std::numeric_limits<size_t>::max();
};

enum class PrefetchGroupLimit {
  kTotalBlockBytes,
  kPhysicalExtentBytes,
};

std::vector<PrefetchGroup> CoalesceBlocks(
    const std::shared_ptr<IOJob>& job, const std::vector<size_t>& block_indices,
    size_t max_group_bytes, bool include_oversized_blocks,
    PrefetchGroupLimit group_limit) {
  std::vector<PrefetchGroup> groups;
  if (block_indices.empty()) {
    return groups;
  }

  const auto& block_handles = job->block_handles;
  const uint64_t coalesce_threshold = job->job_options.io_coalesce_threshold;
  groups.emplace_back();
  for (const size_t block_index : block_indices) {
    const size_t block_size =
        BlockBasedTable::BlockSizeWithTrailer(block_handles[block_index]);
    if (max_group_bytes > 0 && block_size > max_group_bytes &&
        !include_oversized_blocks) {
      continue;
    }

    bool start_new_group = false;
    if (!groups.back().block_indices.empty()) {
      const auto& last_handle =
          block_handles[groups.back().block_indices.back()];
      const uint64_t last_end =
          last_handle.offset() +
          BlockBasedTable::BlockSizeWithTrailer(last_handle);
      const uint64_t current_start = block_handles[block_index].offset();
      if (current_start > last_end + coalesce_threshold) {
        start_new_group = true;
      } else if (max_group_bytes > 0) {
        if (group_limit == PrefetchGroupLimit::kTotalBlockBytes) {
          start_new_group =
              groups.back().total_bytes + block_size > max_group_bytes;
        } else {
          const auto& first_handle =
              block_handles[groups.back().block_indices.front()];
          start_new_group = current_start + block_size - first_handle.offset() >
                            max_group_bytes;
        }
      }
    }
    if (start_new_group) {
      groups.emplace_back();
    }
    groups.back().block_indices.push_back(block_index);
    groups.back().total_bytes += block_size;
  }
  return groups;
}

uint64_t GetPrefetchExtentBytes(const std::shared_ptr<IOJob>& job,
                                const std::vector<size_t>& block_indices) {
  assert(!block_indices.empty());
  const auto& first = job->block_handles[block_indices.front()];
  const auto& last = job->block_handles[block_indices.back()];
  return last.offset() + BlockBasedTable::BlockSizeWithTrailer(last) -
         first.offset();
}

class DefaultPrefetchScheduler;

class DefaultPrefetchCoordinator {
 public:
  DefaultPrefetchCoordinator(size_t max_bytes, Statistics* statistics)
      : max_bytes_(max_bytes), statistics_(statistics) {}

  bool TryAcquire(size_t bytes);
  void Release(size_t bytes);
  void Enqueue(const std::shared_ptr<DefaultPrefetchScheduler>& scheduler);
  size_t MaxBytes() const { return max_bytes_; }

 private:
  void TryDispatchPending();

  const size_t max_bytes_;
  Statistics* const statistics_;
  std::atomic<size_t> bytes_in_use_{0};
  std::atomic<bool> has_pending_{false};
  port::Mutex queue_mutex_;
  std::deque<std::weak_ptr<DefaultPrefetchScheduler>> pending_;
};

class DefaultPrefetchScheduler final
    : public PrefetchScheduler,
      public std::enable_shared_from_this<DefaultPrefetchScheduler> {
 public:
  struct DispatchProgress {
    bool dispatched = false;
    bool pending = false;
  };

  DefaultPrefetchScheduler(
      std::weak_ptr<PrefetchDispatchTarget> target,
      std::weak_ptr<DefaultPrefetchCoordinator> coordinator,
      std::weak_ptr<ReadSet> read_set, std::shared_ptr<IOJob> job)
      : target_(std::move(target)),
        coordinator_(std::move(coordinator)),
        read_set_(std::move(read_set)),
        job_(std::move(job)) {}

  Status Start(const std::vector<size_t>& block_indices) override;
  void OnBlockDiscarded(size_t block_index) override;
  void ReleaseMemory(size_t bytes) override {
    if (const auto coordinator = coordinator_.lock()) {
      coordinator->Release(bytes);
    }
  }
  DispatchProgress TryDispatchAvailable();

 private:
  std::weak_ptr<PrefetchDispatchTarget> target_;
  std::weak_ptr<DefaultPrefetchCoordinator> coordinator_;
  std::weak_ptr<ReadSet> read_set_;
  std::shared_ptr<IOJob> job_;
  port::Mutex mutex_;
  std::deque<PrefetchGroup> groups_;
  std::unordered_set<size_t> pending_blocks_;
};

class RollingPrefetchScheduler final : public PrefetchScheduler {
 public:
  RollingPrefetchScheduler(std::weak_ptr<PrefetchDispatchTarget> target,
                           std::weak_ptr<ReadSet> read_set,
                           std::shared_ptr<IOJob> job, uint64_t window_size,
                           uint64_t max_batch_size)
      : target_(std::move(target)),
        read_set_(std::move(read_set)),
        job_(std::move(job)),
        window_size_(window_size),
        batch_size_(window_size),
        max_batch_size_(std::max(window_size, max_batch_size)) {}

  Status Start(const std::vector<size_t>& block_indices) override;
  Status OnBlockRequired(size_t block_index) override;
  void OnBlockHandedOff(size_t block_index) override;
  void OnBlockDiscarded(size_t block_index) override;
  void OnForegroundStall(size_t block_index) override;

 private:
  struct Batch {
    std::vector<std::vector<size_t>> windows;
    size_t last_window_index = std::numeric_limits<size_t>::max();
  };

  void RemoveBlock(size_t block_index);
  void PopCompletedWindows();
  bool AdvanceToBlock(size_t block_index);
  Batch BuildBatch() const;
  Status DispatchNext(size_t required_block_index,
                      size_t* last_dispatched_window_index = nullptr);
  void Disable() { enabled_ = false; }

  std::weak_ptr<PrefetchDispatchTarget> target_;
  std::weak_ptr<ReadSet> read_set_;
  std::shared_ptr<IOJob> job_;
  std::deque<PrefetchGroup> windows_;
  std::unordered_set<size_t> pending_blocks_;
  std::vector<size_t> batch_by_block_;
  const uint64_t window_size_;
  uint64_t batch_size_;
  const uint64_t max_batch_size_;
  size_t next_batch_ = 0;
  size_t last_read_index_ = 0;
  bool enabled_ = true;
};

class PrefetchSchedulerFactoryImpl final : public PrefetchSchedulerFactory {
 public:
  explicit PrefetchSchedulerFactoryImpl(const IODispatcherOptions& options)
      : max_prefetch_memory_bytes_(options.max_prefetch_memory_bytes),
        default_coordinator_(std::make_shared<DefaultPrefetchCoordinator>(
            options.max_prefetch_memory_bytes, options.statistics)) {}

  std::shared_ptr<PrefetchScheduler> Create(
      std::weak_ptr<PrefetchDispatchTarget> target,
      const std::shared_ptr<ReadSet>& read_set,
      const std::shared_ptr<IOJob>& job,
      bool block_handles_are_sorted) override {
    if (job->job_options.rolling_prefetch_window_size > 0 &&
        job->job_options.read_options.async_io && block_handles_are_sorted &&
        max_prefetch_memory_bytes_ == 0) {
      return std::make_shared<RollingPrefetchScheduler>(
          std::move(target), read_set, job,
          job->job_options.rolling_prefetch_window_size,
          job->job_options.rolling_prefetch_max_batch_size);
    }
    return std::make_shared<DefaultPrefetchScheduler>(
        std::move(target), default_coordinator_, read_set, job);
  }

 private:
  const size_t max_prefetch_memory_bytes_;
  std::shared_ptr<DefaultPrefetchCoordinator> default_coordinator_;
};

bool DefaultPrefetchCoordinator::TryAcquire(size_t bytes) {
  if (max_bytes_ == 0) {
    return true;
  }
  size_t current = bytes_in_use_.load(std::memory_order_relaxed);
  while (true) {
    if (current + bytes > max_bytes_) {
      RecordTick(statistics_, PREFETCH_MEMORY_REQUESTS_BLOCKED);
      return false;
    }
    if (bytes_in_use_.compare_exchange_weak(current, current + bytes,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
      RecordTick(statistics_, PREFETCH_MEMORY_BYTES_GRANTED, bytes);
      return true;
    }
  }
}

void DefaultPrefetchCoordinator::Release(size_t bytes) {
  if (max_bytes_ == 0) {
    return;
  }
  const size_t old_value =
      bytes_in_use_.fetch_sub(bytes, std::memory_order_release);
  assert(old_value >= bytes);
  (void)old_value;
  RecordTick(statistics_, PREFETCH_MEMORY_BYTES_RELEASED, bytes);
  if (has_pending_.load(std::memory_order_acquire)) {
    TryDispatchPending();
  }
}

void DefaultPrefetchCoordinator::Enqueue(
    const std::shared_ptr<DefaultPrefetchScheduler>& scheduler) {
  MutexLock lock(&queue_mutex_);
  pending_.push_back(scheduler);
  has_pending_.store(true, std::memory_order_release);
}

void DefaultPrefetchCoordinator::TryDispatchPending() {
  while (true) {
    std::weak_ptr<DefaultPrefetchScheduler> pending;
    {
      MutexLock lock(&queue_mutex_);
      if (pending_.empty()) {
        has_pending_.store(false, std::memory_order_release);
        return;
      }
      pending = std::move(pending_.front());
      pending_.pop_front();
    }
    const auto scheduler = pending.lock();
    if (!scheduler) {
      continue;
    }
    const auto progress = scheduler->TryDispatchAvailable();
    if (progress.pending) {
      MutexLock lock(&queue_mutex_);
      pending_.push_front(scheduler);
      if (!progress.dispatched) {
        return;
      }
    }
  }
}

Status DefaultPrefetchScheduler::Start(
    const std::vector<size_t>& block_indices) {
  const auto target = target_.lock();
  const auto coordinator = coordinator_.lock();
  const auto read_set = read_set_.lock();
  if (!target || !coordinator || !read_set) {
    return Status::OK();
  }

  auto groups = CoalesceBlocks(job_, block_indices, coordinator->MaxBytes(),
                               /*include_oversized_blocks=*/false,
                               PrefetchGroupLimit::kTotalBlockBytes);
  std::vector<size_t> blocks_to_dispatch;
  for (auto& group : groups) {
    if (coordinator->TryAcquire(group.total_bytes)) {
      blocks_to_dispatch.insert(blocks_to_dispatch.end(),
                                group.block_indices.begin(),
                                group.block_indices.end());
      continue;
    }
    pending_blocks_.insert(group.block_indices.begin(),
                           group.block_indices.end());
    groups_.push_back(std::move(group));
  }

  Status status;
  if (!blocks_to_dispatch.empty()) {
    status = target->DispatchPrefetch(read_set, job_, blocks_to_dispatch,
                                      /*require_async_io=*/false);
  }
  if (!groups_.empty()) {
    coordinator->Enqueue(shared_from_this());
  }
  return status;
}

void DefaultPrefetchScheduler::OnBlockDiscarded(size_t block_index) {
  MutexLock lock(&mutex_);
  pending_blocks_.erase(block_index);
}

DefaultPrefetchScheduler::DispatchProgress
DefaultPrefetchScheduler::TryDispatchAvailable() {
  const auto target = target_.lock();
  const auto coordinator = coordinator_.lock();
  const auto read_set = read_set_.lock();
  if (!target || !coordinator || !read_set) {
    return {};
  }

  std::vector<size_t> blocks_to_dispatch;
  bool has_remaining_groups = false;
  {
    MutexLock lock(&mutex_);
    while (!groups_.empty()) {
      auto& group = groups_.front();
      std::vector<size_t> remaining_blocks;
      size_t remaining_bytes = 0;
      for (const size_t block_index : group.block_indices) {
        if (pending_blocks_.count(block_index) == 0) {
          continue;
        }
        remaining_blocks.push_back(block_index);
        remaining_bytes += BlockBasedTable::BlockSizeWithTrailer(
            job_->block_handles[block_index]);
      }
      if (remaining_blocks.empty()) {
        groups_.pop_front();
        continue;
      }
      if (!coordinator->TryAcquire(remaining_bytes)) {
        group.block_indices = std::move(remaining_blocks);
        group.total_bytes = remaining_bytes;
        has_remaining_groups = true;
        break;
      }
      for (const size_t block_index : remaining_blocks) {
        blocks_to_dispatch.push_back(block_index);
        pending_blocks_.erase(block_index);
      }
      groups_.pop_front();
    }
  }

  if (!blocks_to_dispatch.empty()) {
    Status status = target->DispatchPrefetch(read_set, job_, blocks_to_dispatch,
                                             /*require_async_io=*/false);
    status.PermitUncheckedError();
  }
  return {.dispatched = !blocks_to_dispatch.empty(),
          .pending = has_remaining_groups};
}

Status RollingPrefetchScheduler::Start(
    const std::vector<size_t>& block_indices) {
  auto windows = CoalesceBlocks(job_, block_indices, window_size_,
                                /*include_oversized_blocks=*/true,
                                PrefetchGroupLimit::kPhysicalExtentBytes);
  batch_by_block_.resize(job_->block_handles.size(),
                         std::numeric_limits<size_t>::max());
  for (size_t window_index = 0; window_index < windows.size(); ++window_index) {
    auto& window = windows[window_index];
    window.rolling_window_index = window_index;
    pending_blocks_.insert(window.block_indices.begin(),
                           window.block_indices.end());
    windows_.push_back(std::move(window));
  }
  return DispatchNext(std::numeric_limits<size_t>::max());
}

Status RollingPrefetchScheduler::OnBlockRequired(size_t block_index) {
  if (!enabled_) {
    return Status::OK();
  }
  if (block_index < last_read_index_) {
    Disable();
    return Status::OK();
  }
  last_read_index_ = block_index;
  if (pending_blocks_.count(block_index) == 0) {
    return Status::OK();
  }
  return DispatchNext(block_index);
}

void RollingPrefetchScheduler::OnBlockHandedOff(size_t block_index) {
  if (!enabled_ || block_index >= batch_by_block_.size()) {
    return;
  }
  const size_t batch_end = batch_by_block_[block_index];
  if (batch_end == std::numeric_limits<size_t>::max() ||
      batch_end < next_batch_) {
    return;
  }

  size_t last_dispatched_window;
  do {
    last_dispatched_window = std::numeric_limits<size_t>::max();
    Status status = DispatchNext(std::numeric_limits<size_t>::max(),
                                 &last_dispatched_window);
    if (!status.ok()) {
      status.PermitUncheckedError();
      return;
    }
  } while (last_dispatched_window <= batch_end);
  next_batch_ = batch_end + 1;
}

void RollingPrefetchScheduler::OnBlockDiscarded(size_t block_index) {
  RemoveBlock(block_index);
}

void RollingPrefetchScheduler::OnForegroundStall(size_t block_index) {
  if (!enabled_ || batch_size_ >= max_batch_size_) {
    return;
  }
  batch_size_ =
      batch_size_ > max_batch_size_ / 2 ? max_batch_size_ : batch_size_ * 2;
  OnBlockHandedOff(block_index);
}

void RollingPrefetchScheduler::RemoveBlock(size_t block_index) {
  pending_blocks_.erase(block_index);
}

void RollingPrefetchScheduler::PopCompletedWindows() {
  while (!windows_.empty()) {
    const auto& blocks = windows_.front().block_indices;
    if (std::any_of(blocks.begin(), blocks.end(), [&](size_t block_index) {
          return pending_blocks_.count(block_index) > 0;
        })) {
      return;
    }
    windows_.pop_front();
  }
}

bool RollingPrefetchScheduler::AdvanceToBlock(size_t required_block_index) {
  if (required_block_index == std::numeric_limits<size_t>::max()) {
    return true;
  }
  while (!windows_.empty()) {
    const auto& blocks = windows_.front().block_indices;
    if (std::find(blocks.begin(), blocks.end(), required_block_index) !=
        blocks.end()) {
      break;
    }
    if (blocks.back() > required_block_index) {
      break;
    }
    for (const size_t block_index : blocks) {
      RemoveBlock(block_index);
    }
    windows_.pop_front();
  }
  const bool found =
      !windows_.empty() && pending_blocks_.count(required_block_index) > 0;
  if (!found) {
    RemoveBlock(required_block_index);
  }
  return found;
}

RollingPrefetchScheduler::Batch RollingPrefetchScheduler::BuildBatch() const {
  Batch batch;
  uint64_t batch_bytes = 0;
  for (const auto& window : windows_) {
    std::vector<size_t> blocks;
    for (const size_t block_index : window.block_indices) {
      if (pending_blocks_.count(block_index) > 0) {
        blocks.push_back(block_index);
      }
    }
    if (blocks.empty()) {
      continue;
    }
    const uint64_t window_bytes = GetPrefetchExtentBytes(job_, blocks);
    if (!batch.windows.empty() && (batch_bytes >= batch_size_ ||
                                   window_bytes > batch_size_ - batch_bytes)) {
      break;
    }
    batch.last_window_index = window.rolling_window_index;
    batch_bytes += window_bytes;
    batch.windows.push_back(std::move(blocks));
  }
  return batch;
}

Status RollingPrefetchScheduler::DispatchNext(
    size_t required_block_index, size_t* last_dispatched_window_index) {
  if (last_dispatched_window_index != nullptr) {
    *last_dispatched_window_index = std::numeric_limits<size_t>::max();
  }
  if (!enabled_) {
    return Status::OK();
  }

  const auto read_set = read_set_.lock();
  if (!read_set) {
    Disable();
    return Status::Incomplete("read set was destroyed");
  }
  if (!AdvanceToBlock(required_block_index)) {
    return Status::OK();
  }

  PopCompletedWindows();
  Batch batch = BuildBatch();
  [[maybe_unused]] const size_t window_count = batch.windows.size();
  TEST_SYNC_POINT_CALLBACK("RollingPrefetchScheduler::DispatchNext:WindowCount",
                           const_cast<size_t*>(&window_count));
  for (const auto& window : batch.windows) {
    for (const size_t block_index : window) {
      batch_by_block_[block_index] = batch.last_window_index;
    }
    const auto target = target_.lock();
    if (!target) {
      Disable();
      return Status::Incomplete("prefetch target was destroyed");
    }
    Status status = target->DispatchPrefetch(read_set, job_, window,
                                             /*require_async_io=*/true);
    if (!status.ok()) {
      for (const size_t block_index : window) {
        if (read_set->IsBlockAvailable(block_index)) {
          RemoveBlock(block_index);
        }
      }
      PopCompletedWindows();
      if (status.IsNotSupported()) {
        Disable();
      }
      return status;
    }
    for (const size_t block_index : window) {
      RemoveBlock(block_index);
    }
    PopCompletedWindows();
  }
  if (last_dispatched_window_index != nullptr) {
    *last_dispatched_window_index = batch.last_window_index;
  }
  return Status::OK();
}

}  // namespace

std::shared_ptr<PrefetchSchedulerFactory> NewPrefetchSchedulerFactory(
    const IODispatcherOptions& options) {
  return std::make_shared<PrefetchSchedulerFactoryImpl>(options);
}

}  // namespace ROCKSDB_NAMESPACE
