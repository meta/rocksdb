//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef OS_WIN

#include "utilities/transactions/lock/range/range_tree/range_tree_lock_tracker.h"

#include "utilities/transactions/lock/range/range_tree/range_tree_lock_manager.h"

namespace ROCKSDB_NAMESPACE {

RangeLockList *RangeTreeLockTracker::getOrCreateList() {
  if (range_list_) {
    return range_list_.get();
  }

  // Doesn't exist, create
  range_list_.reset(new RangeLockList());
  return range_list_.get();
}

void RangeTreeLockTracker::Track(const PointLockRequest &lock_req) {
  DBT key_dbt;
  std::string key;
  serialize_endpoint(Endpoint(lock_req.key, false), &key);
  toku_fill_dbt(&key_dbt, key.data(), key.size());
  RangeLockList *rl = getOrCreateList();
  rl->Append(lock_req.column_family_id, &key_dbt, &key_dbt);
}

void RangeTreeLockTracker::Track(const RangeLockRequest &lock_req) {
  DBT start_dbt;
  DBT end_dbt;
  std::string start_key;
  std::string end_key;

  serialize_endpoint(lock_req.start_endp, &start_key);
  serialize_endpoint(lock_req.end_endp, &end_key);

  toku_fill_dbt(&start_dbt, start_key.data(), start_key.size());
  toku_fill_dbt(&end_dbt, end_key.data(), end_key.size());

  RangeLockList *rl = getOrCreateList();
  rl->Append(lock_req.column_family_id, &start_dbt, &end_dbt);
}

PointLockStatus RangeTreeLockTracker::GetPointLockStatus(
    ColumnFamilyId /*cf_id*/, const std::string & /*key*/) const {
  // This function is not expected to be called as RangeTreeLockTracker::
  // IsPointLockSupported() returns false. Return the status which indicates
  // the point is not locked.
  PointLockStatus p;
  p.locked = false;
  p.exclusive = true;
  p.seq = 0;
  return p;
}

void RangeTreeLockTracker::Clear() { range_list_.reset(); }

void RangeLockList::Append(ColumnFamilyId cf_id, const DBT *left_key,
                           const DBT *right_key) {
  MutexLock l(&mutex_);
  // Only the transaction owner thread calls this function.
  // The same thread does the lock release, so we can be certain nobody is
  // releasing the locks concurrently.
  assert(!releasing_locks_.load());
  auto it = buffers_.find(cf_id);
  if (it == buffers_.end()) {
    // create a new one
    it = buffers_.emplace(cf_id, std::make_shared<toku::range_buffer>()).first;
    it->second->create();
  }
  it->second->append(left_key, right_key);
}

void RangeLockList::ReleaseLocks(RangeTreeLockManager *mgr,
                                 PessimisticTransaction *txn,
                                 bool all_trx_locks) {
  {
    MutexLock l(&mutex_);
    // The lt->release_locks() call below will walk range_list->buffer_. We
    // need to prevent lock escalation callback from replacing
    // range_list->buffer_ while we are doing that.
    //
    // Additional complication here is internal mutex(es) in the locktree
    // (let's call them latches):
    // - Lock escalation first obtains latches on the lock tree
    // - Then, it calls RangeTreeLockManager::on_escalate to replace
    // transaction's range_list->buffer_. = Access to that buffer must be
    // synchronized, so it will want to acquire the range_list->mutex_.
    //
    // While in this function we would want to do the reverse:
    // - Acquire range_list->mutex_ to prevent access to the range_list.
    // - Then, lt->release_locks() call will walk through the range_list
    // - and acquire latches on parts of the lock tree to remove locks from
    //   it.
    //
    // How do we avoid the deadlock? The idea is that here we set
    // releasing_locks_=true, and release the mutex.
    // All other users of the range_list must:
    // - Acquire the mutex, then check that releasing_locks_=false.
    //   (the code in this function doesnt do that as there's only one thread
    //    that releases transaction's locks)
    releasing_locks_.store(true);
  }

  for (const auto &it : buffers_) {
    // Don't try to call release_locks() if the buffer is empty! if we are
    //  not holding any locks, the lock tree might be in the STO-mode with
    //  another transaction, and our attempt to release an empty set of locks
    //  will cause an assertion failure.
    if (it.second->get_num_ranges()) {
      auto lt_ptr = mgr->GetLockTreeForCF(it.first);
      toku::locktree *lt = lt_ptr.get();

      lt->release_locks((TXNID)txn, it.second.get(), all_trx_locks);

      it.second->destroy();
      it.second->create();

      toku::lock_request::retry_all_lock_requests(lt,
                                                  wait_callback_for_locktree);
    }
  }

  Clear();
  releasing_locks_.store(false);
}

void RangeLockList::ReplaceLocks(const toku::locktree *lt,
                                 const toku::range_buffer &buffer) {
  MutexLock l(&mutex_);
  if (releasing_locks_.load()) {
    // Do nothing. The transaction is releasing its locks, so it will not care
    // about having a correct list of ranges. (In TokuDB,
    // toku_db_txn_escalate_callback() makes use of this property, too)
    return;
  }

  ColumnFamilyId cf_id = (ColumnFamilyId)lt->get_dict_id().dictid;

  auto it = buffers_.find(cf_id);
  it->second->destroy();
  it->second->create();

  toku::range_buffer::iterator iter(&buffer);
  toku::range_buffer::iterator::record rec{};
  while (iter.current(&rec)) {
    it->second->append(rec.get_left_key(), rec.get_right_key());
    iter.next();
  }
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // OS_WIN
