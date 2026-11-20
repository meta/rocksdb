//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <algorithm>
#include <atomic>
#include <memory>
#include <vector>

#include "db/dbformat.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "test_util/sync_point.h"
#include "util/autovector.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

class SnapshotList;
class ShardedSnapshotList;

// Each SnapshotImpl corresponds to a particular sequence number and is linked
// into one of the DB's per-shard doubly-linked snapshot lists.
class SnapshotImpl : public Snapshot {
 public:
  SequenceNumber number_ = 0;  // const after creation
  // It indicates the smallest uncommitted data at the time the snapshot was
  // taken. This is currently used by WritePrepared transactions to limit the
  // scope of queries to IsInSnapshot.
  SequenceNumber min_uncommitted_ = kMinUnCommittedSeq;

  SequenceNumber GetSequenceNumber() const override { return number_; }

  int64_t GetUnixTime() const override { return unix_time_; }

  uint64_t GetTimestamp() const override { return timestamp_; }

 private:
  friend class SnapshotList;
  friend class ShardedSnapshotList;

  // Index of the ShardedSnapshotList shard that holds this snapshot.
  uint32_t shard_ = 0;

  // SnapshotImpl is kept in a doubly-linked circular list
  SnapshotImpl* prev_ = nullptr;
  SnapshotImpl* next_ = nullptr;

  SnapshotList* list_ = nullptr;  // just for sanity checks

  int64_t unix_time_ = 0;

  uint64_t timestamp_ = 0;

  // Will this snapshot be used by a Transaction to do write-conflict checking?
  bool is_write_conflict_boundary_ = false;
};

class SnapshotList {
 public:
  SnapshotList() {
    list_.prev_ = &list_;
    list_.next_ = &list_;
    list_.number_ = 0xFFFFFFFFL;  // placeholder marker, for debugging
    count_ = 0;
  }

  ~SnapshotList() = default;

  // No copying allowed
  SnapshotList(const SnapshotList&) = delete;
  SnapshotList& operator=(const SnapshotList&) = delete;
  SnapshotList(SnapshotList&&) = delete;
  SnapshotList& operator=(SnapshotList&&) = delete;

  bool empty() const {
    assert(list_.next_ != &list_ || 0 == count_);
    return list_.next_ == &list_;
  }
  SnapshotImpl* oldest() const {
    assert(!empty());
    return list_.next_;
  }
  SnapshotImpl* newest() const {
    assert(!empty());
    return list_.prev_;
  }

  SnapshotImpl* New(SnapshotImpl* s, SequenceNumber seq, uint64_t unix_time,
                    bool is_write_conflict_boundary,
                    uint64_t ts = std::numeric_limits<uint64_t>::max()) {
    if (!empty()) {
      assert(newest()->number_ <= seq);
    }
    s->number_ = seq;
    s->unix_time_ = unix_time;
    s->timestamp_ = ts;
    s->is_write_conflict_boundary_ = is_write_conflict_boundary;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    count_++;
    return s;
  }

  // Removes `s` without freeing it.
  void Remove(const SnapshotImpl* s) {
    assert(s->list_ == this);
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    count_--;
  }

  // retrieve all snapshot numbers up until max_seq. They are sorted in
  // ascending order (with no duplicates).
  std::vector<SequenceNumber> GetAll(
      SequenceNumber* oldest_write_conflict_snapshot = nullptr,
      const SequenceNumber& max_seq = kMaxSequenceNumber) const {
    std::vector<SequenceNumber> ret;
    GetAll(&ret, oldest_write_conflict_snapshot, max_seq);
    return ret;
  }

  void GetAll(std::vector<SequenceNumber>* snap_vector,
              SequenceNumber* oldest_write_conflict_snapshot = nullptr,
              const SequenceNumber& max_seq = kMaxSequenceNumber) const {
    std::vector<SequenceNumber>& ret = *snap_vector;
    // So far we have no use case that would pass a non-empty vector
    assert(ret.size() == 0);

    if (oldest_write_conflict_snapshot != nullptr) {
      *oldest_write_conflict_snapshot = kMaxSequenceNumber;
    }

    if (empty()) {
      return;
    }
    const SnapshotImpl* s = &list_;
    while (s->next_ != &list_) {
      if (s->next_->number_ > max_seq) {
        break;
      }
      // Avoid duplicates
      if (ret.empty() || ret.back() != s->next_->number_) {
        ret.push_back(s->next_->number_);
      }

      if (oldest_write_conflict_snapshot != nullptr &&
          *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
          s->next_->is_write_conflict_boundary_) {
        // If this is the first write-conflict boundary snapshot in the list,
        // it is the oldest
        *oldest_write_conflict_snapshot = s->next_->number_;
      }

      s = s->next_;
    }
    return;
  }

  // get the sequence number of the most recent snapshot
  SequenceNumber GetNewest() {
    if (empty()) {
      return 0;
    }
    return newest()->number_;
  }

  int64_t GetOldestSnapshotTime() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->unix_time_;
    }
  }

  int64_t GetOldestSnapshotSequence() const {
    if (empty()) {
      return 0;
    } else {
      return oldest()->GetSequenceNumber();
    }
  }

  uint64_t count() const { return count_; }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl list_;
  uint64_t count_;
};

// A set of SnapshotLists, each protected by its own mutex, so that threads on
// different CPUs can create and release snapshots without sharing a lock.
//
// Each shard is sorted by sequence number because snapshot creation reads the
// sequence number and appends to the list while holding the shard lock.
//
// The shard locks are leaf locks with respect to the DB mutex: they may be
// acquired while holding the DB mutex, never the other way around. When more
// than one shard is locked, shards are locked in index order.
class ShardedSnapshotList {
 public:
  static constexpr uint32_t kNumShards = 16;

  enum class NewResult {
    kSuccess,
    kRetryWithDBMutex,
    kNotSupported,
    kNotCreated,
  };

  ShardedSnapshotList() : shards_(new Shard[kNumShards]) {}
  ~ShardedSnapshotList() = default;
  ShardedSnapshotList(const ShardedSnapshotList&) = delete;
  ShardedSnapshotList& operator=(const ShardedSnapshotList&) = delete;
  ShardedSnapshotList(ShardedSnapshotList&&) = delete;
  ShardedSnapshotList& operator=(ShardedSnapshotList&&) = delete;

  uint32_t num_shards() const { return kNumShards; }

  // Tries to add `s` to the shard of the current CPU. The callbacks are called
  // with the shard lock held. `should_create_fn` can leave `s` unlinked;
  // `after_create_fn` runs after linking and before releasing the shard lock.
  // If BlockNew() is in effect, a caller that does not hold the DB mutex must
  // retry while holding it. This restores the serialization that the DB mutex
  // provided before snapshot creation was sharded without blocking a thread
  // called while LogAndApply has released the DB mutex.
  template <typename SeqFn, typename IsSupportedFn, typename ShouldCreateFn,
            typename AfterCreateFn>
  NewResult TryNew(SnapshotImpl* s, SeqFn seq_fn, IsSupportedFn is_supported_fn,
                   ShouldCreateFn should_create_fn,
                   AfterCreateFn after_create_fn, uint64_t unix_time,
                   bool is_write_conflict_boundary, uint64_t ts,
                   bool db_mutex_held) {
    const int core = port::PhysicalCoreID();
    const uint32_t shard_key =
        core < 0 ? FallbackShard() : static_cast<uint32_t>(core);
    uint32_t idx = shard_key & (kNumShards - 1);
    TEST_SYNC_POINT_CALLBACK("ShardedSnapshotList::New:Shard", &idx);
    assert(idx < kNumShards);
    Shard& shard = shards_[idx];
    MutexLock l(&shard.mu);
    if (!db_mutex_held && shard.new_blocked > 0) {
      TEST_SYNC_POINT_CALLBACK("ShardedSnapshotList::New:Wait", nullptr);
      return NewResult::kRetryWithDBMutex;
    }
    if (!is_supported_fn()) {
      return NewResult::kNotSupported;
    }
    const SequenceNumber seq = seq_fn();
    if (!should_create_fn(seq)) {
      return NewResult::kNotCreated;
    }
    s->shard_ = idx;
    shard.list.New(s, seq, unix_time, is_write_conflict_boundary, ts);
    after_create_fn(seq);
    return NewResult::kSuccess;
  }

  // Removes `s`. Does not free it.
  void Remove(const SnapshotImpl* s) {
    Shard& shard = shards_[s->shard_];
    MutexLock l(&shard.mu);
    shard.list.Remove(s);
    ++shard.release_count;
  }

  // Returns the oldest snapshot sequence number, or `empty_fn()` if there is
  // no snapshot. `empty_fn` is called with all shard locks held, so when it
  // returns the last published sequence number, any snapshot created later by
  // TryNew() has a sequence number no smaller than the result.
  template <typename EmptyFn>
  SequenceNumber OldestOr(EmptyFn empty_fn,
                          uint64_t* total_release_count) const {
    LockAll();
    uint64_t releases = 0;
    for (uint32_t i = 0; i < kNumShards; ++i) {
      releases += shards_[i].release_count;
    }
    const SnapshotImpl* oldest = OldestLocked();
    const SequenceNumber result =
        oldest != nullptr ? oldest->number_ : empty_fn();
    if (total_release_count != nullptr) {
      *total_release_count = releases;
    }
    UnlockAll();
    return result;
  }

  // Runs `fn` while all shards are locked. The caller may hold the DB mutex.
  template <typename Fn>
  void RunWithAllShardsLocked(Fn fn) const {
    LockAll();
    fn();
    UnlockAll();
  }

  // Same contract as SnapshotList::GetAll(): ascending order, no duplicates,
  // only sequence numbers no larger than `max_seq`. Shards are copied one at a
  // time, so a snapshot created concurrently can be absent from the result.
  void GetAll(std::vector<SequenceNumber>* snap_vector,
              SequenceNumber* oldest_write_conflict_snapshot,
              SequenceNumber max_seq) const;

  std::vector<SequenceNumber> GetAll(
      SequenceNumber* oldest_write_conflict_snapshot,
      SequenceNumber max_seq) const {
    std::vector<SequenceNumber> ret;
    GetAll(&ret, oldest_write_conflict_snapshot, max_seq);
    return ret;
  }

  // While in effect, TryNew() makes callers that do not hold the DB mutex retry
  // with it held. External file ingestion uses this to serialize those callers
  // with its DB-mutex-protected snapshot check and installation phases. A
  // caller that already holds the DB mutex can still create a snapshot while
  // LogAndApply deliberately releases that mutex. Calls must be paired.
  void BlockNew();
  void UnblockNew();

  bool empty() const;
  // Requires BlockNew() to be in effect and the caller to hold the DB mutex.
  // Checks one shard at a time, avoiding an all-shard lock set for external
  // ingestion, which can already hold many per-column-family locks. A
  // concurrent release can make a true result conservative; the DB mutex
  // prevents a creator that bypasses the gate from being missed.
  bool AnySnapshotWhileNewBlocked() const;
  uint64_t count() const;
  // Returns 0 when empty. Snapshot timestamps are read before taking a shard
  // lock, so the result is approximate when creations are concurrent or the
  // wall clock moves backward.
  int64_t GetOldestSnapshotTime() const;
  int64_t GetOldestSnapshotSequence() const;

 private:
  struct ALIGN_AS(CACHE_LINE_SIZE) Shard {
    mutable port::Mutex mu;
    SnapshotList list;
    // Number of BlockNew() calls in effect. Protected by mu.
    int new_blocked = 0;
    // Number of snapshots removed from this shard. Protected by mu.
    uint64_t release_count = 0;
  };

  // Locks all shards in index order.
  void LockAll() const;
  void UnlockAll() const;
  // Requires all shard locks. Returns nullptr if there is no snapshot.
  const SnapshotImpl* OldestLocked() const;
  // Cached per thread when the platform cannot identify the current CPU.
  static uint32_t FallbackShard();

  std::unique_ptr<Shard[]> shards_;
};

// All operations on TimestampedSnapshotList must be protected by db mutex.
class TimestampedSnapshotList {
 public:
  explicit TimestampedSnapshotList() = default;

  std::shared_ptr<const SnapshotImpl> GetSnapshot(uint64_t ts) const {
    if (ts == std::numeric_limits<uint64_t>::max() && !snapshots_.empty()) {
      auto it = snapshots_.rbegin();
      assert(it != snapshots_.rend());
      return it->second;
    }
    auto it = snapshots_.find(ts);
    if (it == snapshots_.end()) {
      return std::shared_ptr<const SnapshotImpl>();
    }
    return it->second;
  }

  void GetSnapshots(
      uint64_t ts_lb, uint64_t ts_ub,
      std::vector<std::shared_ptr<const Snapshot>>& snapshots) const {
    assert(ts_lb < ts_ub);
    auto it_low = snapshots_.lower_bound(ts_lb);
    auto it_high = snapshots_.lower_bound(ts_ub);
    for (auto it = it_low; it != it_high; ++it) {
      snapshots.emplace_back(it->second);
    }
  }

  void AddSnapshot(const std::shared_ptr<const SnapshotImpl>& snapshot) {
    assert(snapshot);
    snapshots_.try_emplace(snapshot->GetTimestamp(), snapshot);
  }

  // snapshots_to_release receives the last references so their destruction is
  // deferred until the caller has finished mutating this DB-mutex-protected
  // timestamp index. Destroying a snapshot itself does not require DB mutex.
  void ReleaseSnapshotsOlderThan(
      uint64_t ts,
      autovector<std::shared_ptr<const SnapshotImpl>>& snapshots_to_release) {
    auto ub = snapshots_.lower_bound(ts);
    for (auto it = snapshots_.begin(); it != ub; ++it) {
      snapshots_to_release.emplace_back(it->second);
    }
    snapshots_.erase(snapshots_.begin(), ub);
  }

 private:
  std::map<uint64_t, std::shared_ptr<const SnapshotImpl>> snapshots_;
};

}  // namespace ROCKSDB_NAMESPACE
