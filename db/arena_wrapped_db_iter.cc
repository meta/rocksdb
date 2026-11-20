//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/arena_wrapped_db_iter.h"

#include "memory/arena.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "util/user_comparator_wrapper.h"

namespace ROCKSDB_NAMESPACE {

inline static SequenceNumber GetSeqNum(const DBImpl* db, const Snapshot* s) {
  if (s) {
    return s->GetSequenceNumber();
  } else {
    return db->GetLatestSequenceNumber();
  }
}

Status ArenaWrappedDBIter::GetProperty(std::string prop_name,
                                       std::string* prop) {
  if (prop_name == "rocksdb.iterator.super-version-number") {
    if (!internal_iter_initialized_) {
      *prop = std::to_string(sv_number_);
      return Status::OK();
    }
    // First try to pass the value returned from inner iterator.
    if (!db_iter_->GetProperty(prop_name, prop).ok()) {
      *prop = std::to_string(sv_number_);
    }
    return Status::OK();
  }
  if (!EnsureInternalIteratorInitialized(nullptr).ok()) {
    return db_iter_->status();
  }
  return db_iter_->GetProperty(std::move(prop_name), prop);
}

void ArenaWrappedDBIter::CleanupDeferredSuperVersion() {
  if (deferred_sv_ != nullptr) {
    assert(db_impl_ != nullptr);
    db_impl_->CleanupIteratorSuperVersion(
        deferred_sv_, read_options_.background_purge_on_iterator_cleanup);
    deferred_sv_ = nullptr;
  }
}

void ArenaWrappedDBIter::DestroyDBIter() {
  db_iter_->~DBIter();
  CleanupDeferredSuperVersion();
}

void ArenaWrappedDBIter::ColumnFamilyDataUnrefDeleter::operator()(
    ColumnFamilyData* cfd) const {
  if (cfd == nullptr) {
    return;
  }
  assert(db_impl != nullptr);

  InstrumentedMutexLock lock(db_impl->mutex());
  cfd->UnrefAndTryDelete();
}

void ArenaWrappedDBIter::DestroyDBIterAndArena() {
  DestroyDBIter();
  arena_.~Arena();
}

Status ArenaWrappedDBIter::EnsureInternalIteratorInitialized(
    const MultiScanArgs* scan_opts) {
  if (internal_iter_initialized_) {
    return Status::OK();
  }
  if (db_impl_ == nullptr || deferred_cfd_ == nullptr ||
      deferred_sv_ == nullptr) {
    Status s = Status::InvalidArgument(
        "Internal iterator cannot be initialized without deferred DB state");
    db_iter_->set_status(s);
    db_iter_->set_valid(false);
    return s;
  }

  const MultiScanArgs* pruning_scan_opts =
      scan_opts != nullptr && scan_opts->HasBoundedScanRanges() ? scan_opts
                                                                : nullptr;
  child_read_options_ = read_options_;
  child_read_options_.snapshot = nullptr;
  InternalIterator* internal_iter = db_impl_->NewInternalIterator(
      child_read_options_, deferred_cfd_, deferred_sv_, &arena_, sequence_,
      /*allow_unprepared_value=*/true, this, pruning_scan_opts);
  deferred_cfd_ = nullptr;
  deferred_sv_ = nullptr;
  SetIterUnderDBIterImpl(internal_iter);
  return Status::OK();
}

void ArenaWrappedDBIter::Init(
    Env* env, const ReadOptions& read_options, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, const Version* version,
    const SequenceNumber& sequence, uint64_t version_number,
    ReadCallback* read_callback, ColumnFamilyHandleImpl* cfh,
    bool expose_blob_index, bool allow_refresh, ReadOnlyMemTable* active_mem,
    DBImpl* db_impl, ColumnFamilyData* cfd) {
  read_options_ = read_options;
  child_read_options_ = read_options;
  if (!CheckFSFeatureSupport(env->GetFileSystem().get(),
                             FSSupportedOps::kAsyncIO)) {
    read_options_.async_io = false;
  }
  read_options_.total_order_seek |= ioptions.prefix_seek_opt_in_only;

  if (cfh != nullptr) {
    db_impl = cfh->db();
    cfd = cfh->cfd();
  }

  db_iter_ = DBIter::NewIter(env, read_options_, ioptions, mutable_cf_options,
                             ioptions.user_comparator,
                             /*internal_iter=*/nullptr, version, sequence,
                             read_callback, active_mem, /*cfh=*/nullptr,
                             expose_blob_index, &arena_, db_impl, cfd);

  sv_number_ = version_number;
  allow_refresh_ = allow_refresh;
  allow_mark_memtable_for_flush_ = active_mem;
  memtable_range_tombstone_iter_ = nullptr;
}

void ArenaWrappedDBIter::MaybeAutoRefresh(bool is_seek,
                                          DBIter::Direction direction) {
  if (cfd_ref_ != nullptr && read_options_.snapshot != nullptr &&
      allow_refresh_ && read_options_.auto_refresh_iterator_with_snapshot) {
    // The intent here is to capture the superversion number change
    // reasonably soon from the time it actually happened. As such,
    // we're fine with weaker synchronization / ordering guarantees
    // provided by relaxed atomic (in favor of less CPU / mem overhead).
    uint64_t cur_sv_number = cfd_ref_->GetSuperVersionNumberRelaxed();
    if ((sv_number_ != cur_sv_number) && status().ok()) {
      // Changing iterators' direction is pretty heavy-weight operation and
      // could have unintended consequences when it comes to prefix seek.
      // Therefore, we need an efficient implementation that does not duplicate
      // the effort by doing things like double seek(forprev).
      //
      // Auto refresh can be triggered on the following groups of operations:
      //
      //  1. [Seek]: Seek(), SeekForPrev()
      //  2. [Non-Seek]: Next(), Prev()
      //
      // In case of 'Seek' group, procedure is fairly straightforward as we'll
      // simply call refresh and then invoke the operation on intended target.
      //
      // In case of 'Non-Seek' group, we'll first advance the cursor by invoking
      // intended user operation (Next() or Prev()), capture the target key T,
      // refresh the iterator and then reconcile the refreshed iterator by
      // explicitly calling [Seek(T) or SeekForPrev(T)]. Below is an example
      // flow for Next(), but same principle applies to Prev():
      //
      //
      //          T0: Before the operation     T1: Execute Next()
      //                         |                      |
      //                         |         -------------
      //                         |        |   * capture the key (T)
      //           DBIter(SV#A)  |        |
      //          --------------\ /------\ /---------
      //  SV #A  |     ... ->  [ X ] -> [ T ] -> ... |
      //          -----------------------------------
      //                                / |
      //                               /  |
      //                              /  T2: Refresh iterator
      //                             /
      //           DBIter(SV#A')    /
      //          ----------------------------------
      //  SV #A' |       ... ->  [ T ] -> ...       |
      //          ----------------/ \---------------
      //                           |
      //                            ---- T3: Seek(T)
      //
      bool valid = false;
      std::string key;
      if (!is_seek && db_iter_->Valid()) {
        // The key() Slice is valid until the iterator state changes.
        // Given that refresh is heavy-weight operation it itself,
        // we should copy the target key upfront to avoid reading bad value.
        valid = true;
        key = db_iter_->key().ToString();
      }

      // It's perfectly fine to unref the corresponding superversion
      // as we rely on pinning behavior of snapshot for consistency.
      DoRefresh(read_options_.snapshot, cur_sv_number);

      if (!is_seek && valid) {  // Reconcile new iterator after Next() / Prev()
        if (direction == DBIter::kForward) {
          db_iter_->Seek(key);
        } else {
          assert(direction == DBIter::kReverse);
          db_iter_->SeekForPrev(key);
        }
      }
    }
  }
}

Status ArenaWrappedDBIter::Refresh() { return Refresh(nullptr); }

void ArenaWrappedDBIter::DoRefresh(const Snapshot* snapshot,
                                   [[maybe_unused]] uint64_t sv_number) {
  Env* env = db_iter_->env();

  // NOTE:
  //
  // Errors like file deletion (as a part of SV cleanup in ~DBIter) will be
  // present in the error log, but won't be reflected in the iterator status.
  // This is by design as we expect compaction to clean up those obsolete files
  // eventually.
  DestroyDBIterAndArena();
  new (&arena_) Arena();

  auto cfd = cfd_ref_.get();
  auto db_impl = db_impl_;
  assert(cfd != nullptr);
  assert(db_impl != nullptr);

  SuperVersion* sv = cfd->GetReferencedSuperVersion(db_impl);
  assert(sv->version_number >= sv_number);
  SequenceNumber read_seq = GetSeqNum(db_impl, snapshot);
  if (read_callback_) {
    read_callback_->Refresh(read_seq);
  }
  // TODO: Preserve Prepare() scan options across Refresh() so a refreshed
  // MultiScan iterator can rebuild the same pruned tree.
  Init(env, read_options_, cfd->ioptions(), sv->mutable_cf_options, sv->current,
       read_seq, sv->version_number, read_callback_, nullptr,
       expose_blob_index_, allow_refresh_,
       allow_mark_memtable_for_flush_ ? sv->mem : nullptr, db_impl, cfd);

  InternalIterator* internal_iter = db_impl->NewInternalIterator(
      read_options_, cfd, sv, &arena_, read_seq,
      /* allow_unprepared_value */ true, /* db_iter */ this);
  SetIterUnderDBIter(internal_iter);
  internal_iter_initialized_ = true;
}

Status ArenaWrappedDBIter::Refresh(const Snapshot* snapshot) {
  if (cfd_ref_ == nullptr || db_impl_ == nullptr || !allow_refresh_) {
    return Status::NotSupported("Creating renew iterator is not allowed.");
  }
  assert(db_iter_ != nullptr);
  auto cfd = cfd_ref_.get();
  auto db_impl = db_impl_;

  // TODO(yiwu): For last_seq_same_as_publish_seq_==false, this is not the
  // correct behavior. Will be corrected automatically when we take a snapshot
  // here for the case of WritePreparedTxnDB.
  uint64_t cur_sv_number = cfd->GetSuperVersionNumber();
  // If we recreate a new internal iterator below (NewInternalIterator()),
  // we will pass in read_options_. We need to make sure it
  // has the right snapshot.
  read_options_.snapshot = snapshot;
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:1");
  TEST_SYNC_POINT("ArenaWrappedDBIter::Refresh:2");

  if (!internal_iter_initialized_) {
    Status s = EnsureInternalIteratorInitialized(nullptr);
    if (!s.ok()) {
      return s;
    }
  }

  // Keeping the current internal iterator and only advancing its sequence
  // number relies on the memtable iterator seeing later inserts.
  const bool memtable_iter_sees_new_writes =
      cfd->ioptions().memtable_factory->IsTailingIteratorSupported();
  while (true) {
    if (sv_number_ != cur_sv_number || !memtable_iter_sees_new_writes) {
      DoRefresh(snapshot, cur_sv_number);
      break;
    } else {
      SequenceNumber read_seq = GetSeqNum(db_impl, snapshot);
      // Refresh range-tombstones in MemTable
      if (!read_options_.ignore_range_deletions) {
        SuperVersion* sv = cfd->GetThreadLocalSuperVersion(db_impl);
        TEST_SYNC_POINT_CALLBACK("ArenaWrappedDBIter::Refresh:SV", nullptr);
        auto t = sv->mem->NewRangeTombstoneIterator(
            read_options_, read_seq, false /* immutable_memtable */);
        if (!t || t->empty()) {
          // If memtable_range_tombstone_iter_ points to a non-empty tombstone
          // iterator, then it means sv->mem is not the memtable that
          // memtable_range_tombstone_iter_ points to, so SV must have changed
          // after the sv_number_ != cur_sv_number check above. We will fall
          // back to re-init the InternalIterator, and the tombstone iterator
          // will be freed during db_iter destruction there.
          if (memtable_range_tombstone_iter_) {
            assert(!*memtable_range_tombstone_iter_ ||
                   sv_number_ != cfd->GetSuperVersionNumber());
          }
          delete t;
        } else {  // current mutable memtable has range tombstones
          if (!memtable_range_tombstone_iter_) {
            delete t;
            db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
            // The memtable under DBIter did not have range tombstone before
            // refresh.
            DoRefresh(snapshot, cur_sv_number);
            break;
          } else {
            *memtable_range_tombstone_iter_ =
                std::make_unique<TruncatedRangeDelIterator>(
                    std::unique_ptr<FragmentedRangeTombstoneIterator>(t),
                    &cfd->internal_comparator(), nullptr, nullptr);
          }
        }
        db_impl->ReturnAndCleanupSuperVersion(cfd, sv);
      }
      // Check again if the latest super version number is changed
      uint64_t latest_sv_number = cfd->GetSuperVersionNumber();
      if (latest_sv_number != cur_sv_number) {
        // If the super version number is changed after refreshing,
        // fallback to Re-Init the InternalIterator
        cur_sv_number = latest_sv_number;
        continue;
      }
      db_iter_->set_sequence(read_seq);
      db_iter_->set_valid(false);
      break;
    }
  }
  return Status::OK();
}

void ArenaWrappedDBIter::Prepare(const MultiScanArgs& scan_opts) {
  if (prepare_called_) {
    db_iter_->set_status(Status::InvalidArgument(
        "Prepare called more than once on the same iterator"));
    db_iter_->set_valid(false);
    return;
  }
  prepare_called_ = true;

  Status s = db_iter_->SetScanOptionsForPrepare(scan_opts);
  if (!s.ok()) {
    return;
  }

  const MultiScanArgs* pruning_scan_opts =
      scan_opts.HasBoundedScanRanges() ? &scan_opts : nullptr;
  s = EnsureInternalIteratorInitialized(pruning_scan_opts);
  if (!s.ok()) {
    return;
  }

  db_iter_->PrepareInternalChildren();
}

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options, ColumnFamilyHandleImpl* cfh,
    SuperVersion* sv, const SequenceNumber& sequence,
    ReadCallback* read_callback, DBImpl* db_impl, bool expose_blob_index,
    bool allow_refresh, bool allow_mark_memtable_for_flush) {
  ArenaWrappedDBIter* db_iter = new ArenaWrappedDBIter();
  ColumnFamilyData* cfd = cfh->cfd();
  db_iter->Init(env, read_options, cfh->cfd()->ioptions(),
                sv->mutable_cf_options, sv->current, sequence,
                sv->version_number, read_callback, cfh, expose_blob_index,
                allow_refresh,
                allow_mark_memtable_for_flush ? sv->mem : nullptr);
  if (allow_refresh && cfd != nullptr) {
    db_iter->StoreRefreshInfo(read_callback, expose_blob_index);
  }
  db_iter->StoreDeferredInitInfo(db_impl, cfd, sv, sequence,
                                 allow_mark_memtable_for_flush);

  return db_iter;
}

}  // namespace ROCKSDB_NAMESPACE
