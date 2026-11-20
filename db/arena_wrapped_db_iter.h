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

#include <memory>
#include <string>

#include "db/db_impl/db_impl.h"
#include "db/db_iter.h"
#include "db/range_del_aggregator.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"

namespace ROCKSDB_NAMESPACE {

class Arena;
class Version;

// A wrapper iterator which wraps DB Iterator and the arena, with which the DB
// iterator is supposed to be allocated. This class is used as an entry point of
// a iterator hierarchy whose memory can be allocated inline. In that way,
// accessing the iterator tree can be more cache friendly. It is also faster
// to allocate.
// When using the class's Iterator interface, the behavior is exactly
// the same as the inner DBIter.
class ArenaWrappedDBIter : public Iterator {
  struct ColumnFamilyDataUnrefDeleter {
    DBImpl* db_impl = nullptr;
    void operator()(ColumnFamilyData* cfd) const;
  };
  using ColumnFamilyDataRef =
      std::unique_ptr<ColumnFamilyData, ColumnFamilyDataUnrefDeleter>;

 public:
  ArenaWrappedDBIter() = default;
  // No copying allowed
  ArenaWrappedDBIter(const ArenaWrappedDBIter&) = delete;
  ArenaWrappedDBIter& operator=(const ArenaWrappedDBIter&) = delete;
  ArenaWrappedDBIter(ArenaWrappedDBIter&&) = delete;
  ArenaWrappedDBIter& operator=(ArenaWrappedDBIter&&) = delete;

  ~ArenaWrappedDBIter() override {
    if (db_iter_ != nullptr) {
      DestroyDBIter();
    } else {
      assert(false);
    }
  }

  // Get the arena to be used to allocate memory for DBIter to be wrapped,
  // as well as child iterators in it.
  virtual Arena* GetArena() { return &arena_; }

  const ReadOptions& GetReadOptions() { return read_options_; }

  // Set the internal iterator wrapped inside the DB Iterator. Usually it is
  // a merging iterator.
  virtual void SetIterUnderDBIter(InternalIterator* iter) {
    SetIterUnderDBIterImpl(iter);
  }

  void SetMemtableRangetombstoneIter(
      std::unique_ptr<TruncatedRangeDelIterator>* iter) {
    memtable_range_tombstone_iter_ = iter;
  }

  bool Valid() const override { return db_iter_->Valid(); }
  void SeekToFirst() override {
    if (!EnsureInternalIteratorInitialized(nullptr).ok()) {
      return;
    }
    db_iter_->SeekToFirst();
  }
  void SeekToLast() override {
    if (!EnsureInternalIteratorInitialized(nullptr).ok()) {
      return;
    }
    db_iter_->SeekToLast();
  }
  // 'target' does not contain timestamp, even if user timestamp feature is
  // enabled.
  void Seek(const Slice& target) override {
    if (!EnsureInternalIteratorInitialized(nullptr).ok()) {
      return;
    }
    MaybeAutoRefresh(true /* is_seek */, DBIter::kForward);
    db_iter_->Seek(target);
  }

  void SeekForPrev(const Slice& target) override {
    if (!EnsureInternalIteratorInitialized(nullptr).ok()) {
      return;
    }
    MaybeAutoRefresh(true /* is_seek */, DBIter::kReverse);
    db_iter_->SeekForPrev(target);
  }

  void Next() override {
    if (!EnsureInternalIteratorInitialized(nullptr).ok()) {
      return;
    }
    db_iter_->Next();
    MaybeAutoRefresh(false /* is_seek */, DBIter::kForward);
  }

  void Prev() override {
    if (!EnsureInternalIteratorInitialized(nullptr).ok()) {
      return;
    }
    db_iter_->Prev();
    MaybeAutoRefresh(false /* is_seek */, DBIter::kReverse);
  }

  Slice key() const override { return db_iter_->key(); }
  Slice value() const override { return db_iter_->value(); }
  const WideColumns& columns() const override { return db_iter_->columns(); }
  Status status() const override { return db_iter_->status(); }
  Slice timestamp() const override { return db_iter_->timestamp(); }
  bool IsBlob() const { return db_iter_->IsBlob(); }

  Status GetProperty(std::string prop_name, std::string* prop) override;

  Status Refresh() override;
  Status Refresh(const Snapshot*) override;

  bool PrepareValue() override {
    return EnsureInternalIteratorInitialized(nullptr).ok() &&
           db_iter_->PrepareValue();
  }

  void Prepare(const MultiScanArgs& scan_opts) override;

  // FIXME: we could just pass SV in for mutable cf option, version and version
  // number, but this is used by SstFileReader which does not have a SV.
  void Init(Env* env, const ReadOptions& read_options,
            const ImmutableOptions& ioptions,
            const MutableCFOptions& mutable_cf_options, const Version* version,
            const SequenceNumber& sequence, uint64_t version_number,
            ReadCallback* read_callback, ColumnFamilyHandleImpl* cfh,
            bool expose_blob_index, bool allow_refresh,
            ReadOnlyMemTable* active_mem, DBImpl* db_impl = nullptr,
            ColumnFamilyData* cfd = nullptr);

  // Store parameters used only by explicit/auto-refresh.
  void StoreRefreshInfo(ReadCallback* read_callback, bool expose_blob_index) {
    read_callback_ = read_callback;
    expose_blob_index_ = expose_blob_index;
  }

  void StoreDeferredInitInfo(DBImpl* db_impl, ColumnFamilyData* cfd,
                             SuperVersion* sv, const SequenceNumber& sequence,
                             bool allow_mark_memtable_for_flush) {
    assert(cfd != nullptr);
    db_impl_ = db_impl;
    cfd->Ref();
    cfd_ref_ = ColumnFamilyDataRef(cfd, ColumnFamilyDataUnrefDeleter{db_impl});
    deferred_cfd_ = cfd;
    deferred_sv_ = sv;
    sequence_ = sequence;
    allow_mark_memtable_for_flush_ = allow_mark_memtable_for_flush;
  }

 private:
  Status EnsureInternalIteratorInitialized(const MultiScanArgs* scan_opts);
  void SetIterUnderDBIterImpl(InternalIterator* iter) {
    db_iter_->SetIter(iter);
    internal_iter_initialized_ = true;
  }
  void CleanupDeferredSuperVersion();
  void DestroyDBIter();
  void DestroyDBIterAndArena();
  void DoRefresh(const Snapshot* snapshot, uint64_t sv_number);
  void MaybeAutoRefresh(bool is_seek, DBIter::Direction direction);

  DBIter* db_iter_ = nullptr;
  Arena arena_;
  uint64_t sv_number_ = 0;
  DBImpl* db_impl_ = nullptr;
  ColumnFamilyDataRef cfd_ref_{nullptr, ColumnFamilyDataUnrefDeleter{}};
  ColumnFamilyData* deferred_cfd_ = nullptr;
  SuperVersion* deferred_sv_ = nullptr;
  SequenceNumber sequence_ = kMaxSequenceNumber;
  bool internal_iter_initialized_ = false;
  bool prepare_called_ = false;
  ReadOptions read_options_;
  ReadOptions child_read_options_;
  ReadCallback* read_callback_ = nullptr;
  bool expose_blob_index_ = false;
  bool allow_refresh_ = true;
  bool allow_mark_memtable_for_flush_ = true;
  // If this is nullptr, it means the mutable memtable does not contain range
  // tombstone when added under this DBIter.
  std::unique_ptr<TruncatedRangeDelIterator>* memtable_range_tombstone_iter_ =
      nullptr;
};

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options, ColumnFamilyHandleImpl* cfh,
    SuperVersion* sv, const SequenceNumber& sequence,
    ReadCallback* read_callback, DBImpl* db_impl, bool expose_blob_index,
    bool allow_refresh, bool allow_mark_memtable_for_flush);
}  // namespace ROCKSDB_NAMESPACE
