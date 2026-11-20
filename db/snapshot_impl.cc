//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/snapshot_impl.h"

#include <algorithm>
#include <queue>

#include "rocksdb/db.h"
#include "rocksdb/snapshot.h"

namespace ROCKSDB_NAMESPACE {

ManagedSnapshot::ManagedSnapshot(DB* db)
    : db_(db), snapshot_(db->GetSnapshot()) {}

ManagedSnapshot::ManagedSnapshot(DB* db, const Snapshot* _snapshot)
    : db_(db), snapshot_(_snapshot) {}

ManagedSnapshot::~ManagedSnapshot() {
  if (snapshot_) {
    db_->ReleaseSnapshot(snapshot_);
  }
}

const Snapshot* ManagedSnapshot::snapshot() { return snapshot_; }

void ShardedSnapshotList::GetAll(std::vector<SequenceNumber>* snap_vector,
                                 SequenceNumber* oldest_write_conflict_snapshot,
                                 SequenceNumber max_seq) const {
  std::vector<SequenceNumber>& ret = *snap_vector;
  // So far we have no use case that would pass a non-empty vector
  assert(ret.empty());
  SequenceNumber oldest_ww = kMaxSequenceNumber;
  autovector<std::vector<SequenceNumber>, kNumShards> shard_seqs;
  shard_seqs.resize(kNumShards);
  size_t total = 0;
  for (uint32_t i = 0; i < kNumShards; ++i) {
    const Shard& shard = shards_[i];
    SequenceNumber shard_ww = kMaxSequenceNumber;
    {
      MutexLock l(&shard.mu);
      shard.list.GetAll(&shard_seqs[i], &shard_ww, max_seq);
    }
    oldest_ww = std::min(oldest_ww, shard_ww);
    total += shard_seqs[i].size();
    TEST_SYNC_POINT_CALLBACK("ShardedSnapshotList::GetAll:AfterShard", &i);
  }

  struct Cursor {
    SequenceNumber seq;
    uint32_t shard;
    size_t pos;
  };
  struct Greater {
    bool operator()(const Cursor& lhs, const Cursor& rhs) const {
      return lhs.seq > rhs.seq;
    }
  };
  std::vector<Cursor> heap_storage;
  heap_storage.reserve(kNumShards);
  std::priority_queue<Cursor, std::vector<Cursor>, Greater> heap(
      Greater(), std::move(heap_storage));
  for (uint32_t i = 0; i < kNumShards; ++i) {
    if (!shard_seqs[i].empty()) {
      heap.push({shard_seqs[i][0], i, 0});
    }
  }
  ret.reserve(total);
  while (!heap.empty()) {
    Cursor cursor = heap.top();
    heap.pop();
    if (ret.empty() || ret.back() != cursor.seq) {
      ret.push_back(cursor.seq);
    }
    ++cursor.pos;
    if (cursor.pos < shard_seqs[cursor.shard].size()) {
      cursor.seq = shard_seqs[cursor.shard][cursor.pos];
      heap.push(cursor);
    }
  }
  if (oldest_write_conflict_snapshot != nullptr) {
    *oldest_write_conflict_snapshot = oldest_ww;
  }
}

bool ShardedSnapshotList::empty() const { return count() == 0; }

bool ShardedSnapshotList::AnySnapshotWhileNewBlocked() const {
  for (uint32_t i = 0; i < kNumShards; ++i) {
    MutexLock l(&shards_[i].mu);
    assert(shards_[i].new_blocked > 0);
    if (!shards_[i].list.empty()) {
      return true;
    }
  }
  return false;
}

uint64_t ShardedSnapshotList::count() const {
  LockAll();
  uint64_t total = 0;
  for (uint32_t i = 0; i < kNumShards; ++i) {
    const Shard& shard = shards_[i];
    total += shard.list.count();
  }
  UnlockAll();
  return total;
}

int64_t ShardedSnapshotList::GetOldestSnapshotTime() const {
  LockAll();
  int64_t result = 0;
  bool found = false;
  for (uint32_t i = 0; i < kNumShards; ++i) {
    const Shard& shard = shards_[i];
    if (!shard.list.empty()) {
      const int64_t shard_time = shard.list.oldest()->GetUnixTime();
      result = found ? std::min(result, shard_time) : shard_time;
      found = true;
    }
  }
  UnlockAll();
  return result;
}

int64_t ShardedSnapshotList::GetOldestSnapshotSequence() const {
  return static_cast<int64_t>(
      OldestOr([]() { return SequenceNumber{0}; }, nullptr));
}

void ShardedSnapshotList::BlockNew() {
  for (uint32_t i = 0; i < kNumShards; ++i) {
    Shard& shard = shards_[i];
    MutexLock l(&shard.mu);
    ++shard.new_blocked;
  }
}

void ShardedSnapshotList::UnblockNew() {
  for (uint32_t i = 0; i < kNumShards; ++i) {
    Shard& shard = shards_[i];
    MutexLock l(&shard.mu);
    assert(shard.new_blocked > 0);
    --shard.new_blocked;
  }
}

void ShardedSnapshotList::LockAll() const {
  for (uint32_t i = 0; i < kNumShards; ++i) {
    shards_[i].mu.Lock();
  }
}

void ShardedSnapshotList::UnlockAll() const {
  for (uint32_t i = 0; i < kNumShards; ++i) {
    shards_[i].mu.Unlock();
  }
}

const SnapshotImpl* ShardedSnapshotList::OldestLocked() const {
  const SnapshotImpl* oldest = nullptr;
  for (uint32_t i = 0; i < kNumShards; ++i) {
    const Shard& shard = shards_[i];
    if (shard.list.empty()) {
      continue;
    }
    const SnapshotImpl* head = shard.list.oldest();
    if (oldest == nullptr || head->number_ < oldest->number_) {
      oldest = head;
    }
  }
  return oldest;
}

uint32_t ShardedSnapshotList::FallbackShard() {
  static std::atomic<uint32_t> next_shard{0};
  static thread_local const uint32_t shard =
      next_shard.fetch_add(1, std::memory_order_relaxed);
  return shard;
}

}  // namespace ROCKSDB_NAMESPACE
