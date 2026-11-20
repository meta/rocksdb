/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=2:softtabstop=2:
#ifndef OS_WIN
#ident "$Id$"
/*======
This file is part of PerconaFT.


Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

    PerconaFT is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License, version 3,
    as published by the Free Software Foundation.

    PerconaFT is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with PerconaFT.  If not, see <http://www.gnu.org/licenses/>.

----------------------------------------

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
======= */

#ident \
    "Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved."

#include "locktree.h"

#include <memory.h>

#include "../portability/toku_pthread.h"
#include "../portability/toku_time.h"
#include "../util/growable_array.h"
#include "range_buffer.h"

// including the concurrent_tree here expands the templates
// and "defines" the implementation, so we do it here in
// the locktree source file instead of the header.
#include "concurrent_tree.h"

namespace toku {
// A locktree represents the set of row locks owned by all transactions
// over an open dictionary. Read and write ranges are represented as
// a left and right key which are compared with the given descriptor
// and comparison fn.
//
// Each locktree has a reference count which it manages
// but does nothing based on the value of the reference count - it is
// up to the user of the locktree to destroy it when it sees fit.

void locktree::create(locktree_manager *mgr, DICTIONARY_ID dict_id,
                      const comparator &cmp,
                      toku_external_mutex_factory_t mutex_factory) {
  m_mgr = mgr;
  m_dict_id = dict_id;

  m_cmp.create_from(cmp);
  m_reference_count = 1;
  m_userdata = nullptr;

  XCALLOC(m_rangetree);
  m_rangetree->create(&m_cmp);

  m_sto_txnid = TXNID_NONE;
  m_sto_buffer.create();
  m_sto_score = STO_SCORE_THRESHOLD;
  m_sto_end_early_count = 0;
  m_sto_end_early_time = 0;

  m_escalation_barrier = [](const DBT *, const DBT *, void *) -> bool {
    return false;
  };

  m_lock_request_info.init(mutex_factory);
}

void locktree::set_escalation_barrier_func(
    lt_escalation_barrier_check_func func, void *extra) {
  m_escalation_barrier = func;
  m_escalation_barrier_arg = extra;
}

void lt_lock_request_info::init(toku_external_mutex_factory_t mutex_factory) {
  pending_lock_requests.create();
  pending_is_empty = true;
  toku_external_mutex_init(mutex_factory, &mutex);
  retry_want = retry_done = 0;
  ZERO_STRUCT(counters);
  ZERO_STRUCT(retry_mutex);
  toku_mutex_init(locktree_request_info_retry_mutex_key, &retry_mutex, nullptr);
  toku_cond_init(locktree_request_info_retry_cv_key, &retry_cv, nullptr);
  running_retry = false;

  TOKU_VALGRIND_HG_DISABLE_CHECKING(&pending_is_empty,
                                    sizeof(pending_is_empty));
  TOKU_DRD_IGNORE_VAR(pending_is_empty);
}

void locktree::destroy(void) {
  invariant(m_reference_count == 0);
  invariant(m_lock_request_info.pending_lock_requests.size() == 0);
  m_cmp.destroy();
  m_rangetree->destroy();
  toku_free(m_rangetree);
  m_sto_buffer.destroy();
  m_lock_request_info.destroy();
}

void lt_lock_request_info::destroy(void) {
  pending_lock_requests.destroy();
  toku_external_mutex_destroy(&mutex);
  toku_mutex_destroy(&retry_mutex);
  toku_cond_destroy(&retry_cv);
}

void locktree::add_reference(void) {
  (void)toku_sync_add_and_fetch(&m_reference_count, 1);
}

uint32_t locktree::release_reference(void) {
  return toku_sync_sub_and_fetch(&m_reference_count, 1);
}

uint32_t locktree::get_reference_count(void) { return m_reference_count; }

// a container for a range/txnid pair
struct row_lock {
  keyrange range;
  TXNID txnid;
  bool is_shared;
  TxnidVector *owners;
};

// iterate over a locked keyrange and copy out all of the data,
// storing each row lock into the given growable array. the
// caller does not own the range inside the returned row locks,
// so remove from the tree with care using them as keys.
static void iterate_and_get_overlapping_row_locks(
    const concurrent_tree::locked_keyrange *lkr,
    GrowableArray<row_lock> *row_locks) {
  struct copy_fn_obj {
    GrowableArray<row_lock> *row_locks;
    bool fn(const keyrange &range, TXNID txnid, bool is_shared,
            TxnidVector *owners) {
      row_lock lock = {.range = range,
                       .txnid = txnid,
                       .is_shared = is_shared,
                       .owners = owners};
      row_locks->push(lock);
      return true;
    }
  } copy_fn;
  copy_fn.row_locks = row_locks;
  lkr->iterate(&copy_fn);
}

// given a txnid and a set of overlapping row locks, determine
// which txnids are conflicting, and store them in the conflicts
// set, if given.
static bool determine_conflicting_txnids(
    const GrowableArray<row_lock> &row_locks, const TXNID &txnid,
    txnid_set *conflicts) {
  bool conflicts_exist = false;
  const size_t num_overlaps = row_locks.get_size();
  for (size_t i = 0; i < num_overlaps; i++) {
    const row_lock lock = row_locks.fetch_unchecked(i);
    const TXNID other_txnid = lock.txnid;
    if (other_txnid != txnid) {
      if (conflicts) {
        if (other_txnid == TXNID_SHARED) {
          // Add all shared lock owners, except this transaction.
          for (TXNID shared_id : *lock.owners) {
            if (shared_id != txnid) conflicts->add(shared_id);
          }
        } else {
          conflicts->add(other_txnid);
        }
      }
      conflicts_exist = true;
    }
  }
  return conflicts_exist;
}

// how much memory does a row lock take up in a concurrent tree?
static uint64_t row_lock_size_in_tree(const row_lock &lock) {
  const uint64_t overhead = concurrent_tree::get_insertion_memory_overhead();
  return lock.range.get_memory_size() + overhead;
}

// remove and destroy the given row lock from the locked keyrange,
// then notify the memory tracker of the newly freed lock.
static void remove_row_lock_from_tree(concurrent_tree::locked_keyrange *lkr,
                                      const row_lock &lock, TXNID txnid,
                                      locktree_manager *mgr) {
  // Each owner of a shared lock was charged for it. Removing the lock for all
  // owners releases all of those charges.
  const uint64_t num_charges =
      (txnid == TXNID_ANY && lock.owners != nullptr) ? lock.owners->size() : 1;
  const uint64_t mem_released = row_lock_size_in_tree(lock) * num_charges;
  lkr->remove(lock.range, txnid);
  if (mgr != nullptr) {
    mgr->note_mem_released(mem_released);
  }
}

// insert a row lock into the locked keyrange, then notify
// the memory tracker of this newly acquired lock.
static void insert_row_lock_into_tree(concurrent_tree::locked_keyrange *lkr,
                                      const row_lock &lock,
                                      locktree_manager *mgr) {
  uint64_t mem_used = row_lock_size_in_tree(lock);
  lkr->insert(lock.range, lock.txnid, lock.is_shared);
  if (mgr != nullptr) {
    mgr->note_mem_used(mem_used);
  }
}

void locktree::sto_begin(TXNID txnid) {
  invariant(m_sto_txnid == TXNID_NONE);
  invariant(m_sto_buffer.is_empty());
  m_sto_txnid = txnid;
}

void locktree::sto_append(const DBT *left_key, const DBT *right_key,
                          bool is_write_request) {
  uint64_t buffer_mem, delta;

  // psergey: the below two lines do not make any sense
  // (and it's the same in upstream TokuDB)
  keyrange range;
  range.create(left_key, right_key);

  buffer_mem = m_sto_buffer.total_memory_size();
  m_sto_buffer.append(left_key, right_key, is_write_request);
  delta = m_sto_buffer.total_memory_size() - buffer_mem;
  if (m_mgr != nullptr) {
    m_mgr->note_mem_used(delta);
  }
}

void locktree::sto_end(void) {
  uint64_t mem_size = m_sto_buffer.total_memory_size();
  if (m_mgr != nullptr) {
    m_mgr->note_mem_released(mem_size);
  }
  m_sto_buffer.destroy();
  m_sto_buffer.create();
  m_sto_txnid = TXNID_NONE;
}

void locktree::sto_end_early_no_accounting(void *prepared_lkr) {
  sto_migrate_buffer_ranges_to_tree(prepared_lkr);
  sto_end();
  toku_unsafe_set(m_sto_score, 0);
}

void locktree::sto_end_early(void *prepared_lkr) {
  m_sto_end_early_count++;

  tokutime_t t0 = toku_time_now();
  sto_end_early_no_accounting(prepared_lkr);
  tokutime_t t1 = toku_time_now();

  m_sto_end_early_time += (t1 - t0);
}

void locktree::sto_migrate_buffer_ranges_to_tree(void *prepared_lkr) {
  // There should be something to migrate, and nothing in the rangetree.
  invariant(!m_sto_buffer.is_empty());
  invariant(m_rangetree->is_empty());

  concurrent_tree sto_rangetree;
  concurrent_tree::locked_keyrange sto_lkr;
  sto_rangetree.create(&m_cmp);

  // insert all of the ranges from the single txnid buffer into a new rangtree
  range_buffer::iterator iter(&m_sto_buffer);
  range_buffer::iterator::record rec;
  while (iter.current(&rec)) {
    sto_lkr.prepare(&sto_rangetree);
    int r = acquire_lock_consolidated(&sto_lkr, m_sto_txnid, rec.get_left_key(),
                                      rec.get_right_key(),
                                      rec.get_exclusive_flag(), nullptr);
    invariant_zero(r);
    sto_lkr.release();
    iter.next();
  }

  // Iterate the newly created rangetree and insert each range into the
  // locktree's rangetree, on behalf of the old single txnid.
  struct migrate_fn_obj {
    concurrent_tree::locked_keyrange *dst_lkr;
    bool fn(const keyrange &range, TXNID txnid, bool is_shared,
            TxnidVector *owners) {
      // There can't be multiple owners in STO mode
      invariant_zero(owners);
      dst_lkr->insert(range, txnid, is_shared);
      return true;
    }
  } migrate_fn;
  migrate_fn.dst_lkr =
      static_cast<concurrent_tree::locked_keyrange *>(prepared_lkr);
  sto_lkr.prepare(&sto_rangetree);
  sto_lkr.iterate(&migrate_fn);
  sto_lkr.remove_all();
  sto_lkr.release();
  sto_rangetree.destroy();
  invariant(!m_rangetree->is_empty());
}

bool locktree::sto_try_acquire(void *prepared_lkr, TXNID txnid,
                               const DBT *left_key, const DBT *right_key,
                               bool is_write_request) {
  if (m_rangetree->is_empty() && m_sto_buffer.is_empty() &&
      toku_unsafe_fetch(m_sto_score) >= STO_SCORE_THRESHOLD) {
    // We can do the optimization because the rangetree is empty, and
    // we know its worth trying because the sto score is big enough.
    sto_begin(txnid);
  } else if (m_sto_txnid != TXNID_NONE) {
    // We are currently doing the optimization. Check if we need to cancel
    // it because a new txnid appeared, or if the current single txnid has
    // taken too many locks already.
    if (m_sto_txnid != txnid ||
        m_sto_buffer.get_num_ranges() > STO_BUFFER_MAX_SIZE) {
      sto_end_early(prepared_lkr);
    }
  }

  // At this point the sto txnid is properly set. If it is valid, then
  // this txnid can append its lock to the sto buffer successfully.
  if (m_sto_txnid != TXNID_NONE) {
    invariant(m_sto_txnid == txnid);
    sto_append(left_key, right_key, is_write_request);
    return true;
  } else {
    invariant(m_sto_buffer.is_empty());
    return false;
  }
}

/*
  Do the same as iterate_and_get_overlapping_row_locks does, but also check for
  this:
    The set of overlapping rows locks consists of just one read-only shared
    lock with the same endpoints as specified (in that case, we can just add
    ourselves into that list)

  @return true - One compatible shared lock
         false - Otherwise
*/
static bool iterate_and_get_overlapping_row_locks2(
    const concurrent_tree::locked_keyrange *lkr, const DBT *left_key,
    const DBT *right_key, comparator *cmp, TXNID,
    GrowableArray<row_lock> *row_locks) {
  struct copy_fn_obj {
    GrowableArray<row_lock> *row_locks;
    bool first_call = true;
    bool matching_lock_found = false;
    const DBT *left_key, *right_key;
    comparator *cmp;

    bool fn(const keyrange &range, TXNID txnid, bool is_shared,
            TxnidVector *owners) {
      if (first_call) {
        first_call = false;
        if (is_shared && !(*cmp)(left_key, range.get_left_key()) &&
            !(*cmp)(right_key, range.get_right_key())) {
          matching_lock_found = true;
        }
      } else {
        // if we see multiple matching locks, it doesn't matter whether
        // the first one was matching.
        matching_lock_found = false;
      }
      row_lock lock = {.range = range,
                       .txnid = txnid,
                       .is_shared = is_shared,
                       .owners = owners};
      row_locks->push(lock);
      return true;
    }
  } copy_fn;
  copy_fn.row_locks = row_locks;
  copy_fn.left_key = left_key;
  copy_fn.right_key = right_key;
  copy_fn.cmp = cmp;
  lkr->iterate(&copy_fn);
  return copy_fn.matching_lock_found;
}

// try to acquire a lock and consolidate it with existing locks if possible
// param: lkr, a prepared locked keyrange
// return: 0 on success, DB_LOCK_NOTGRANTED if conflicting locks exist.
int locktree::acquire_lock_consolidated(void *prepared_lkr, TXNID txnid,
                                        const DBT *left_key,
                                        const DBT *right_key,
                                        bool is_write_request,
                                        txnid_set *conflicts) {
  int r = 0;
  concurrent_tree::locked_keyrange *lkr;

  keyrange requested_range;
  requested_range.create(left_key, right_key);
  lkr = static_cast<concurrent_tree::locked_keyrange *>(prepared_lkr);
  lkr->acquire(requested_range);

  // copy out the set of overlapping row locks.
  GrowableArray<row_lock> overlapping_row_locks;
  overlapping_row_locks.init();
  bool matching_shared_lock_found = false;

  if (is_write_request)
    iterate_and_get_overlapping_row_locks(lkr, &overlapping_row_locks);
  else {
    matching_shared_lock_found = iterate_and_get_overlapping_row_locks2(
        lkr, left_key, right_key, &m_cmp, txnid, &overlapping_row_locks);
    // psergey-todo: what to do now? So, we have figured we have just one
    // shareable lock. Need to add us into it as an owner but the lock
    // pointer cannot be kept?
    // A: use find_node_with_overlapping_child(key_range, nullptr);
    //  then, add ourselves to the owner list.
    // Dont' foreget to release the subtree after that.
  }

  if (matching_shared_lock_found) {
    // there is just one non-confliting matching shared lock.
    //  we are hilding a lock on it (see acquire() call above).
    //  we need to modify it to indicate there is another locker...
    if (lkr->add_shared_owner(requested_range, txnid)) {
      // Pretend shared lock uses as much memory.
      row_lock new_lock = {.range = requested_range,
                           .txnid = txnid,
                           .is_shared = false,
                           .owners = nullptr};
      uint64_t mem_used = row_lock_size_in_tree(new_lock);
      if (m_mgr) {
        m_mgr->note_mem_used(mem_used);
      }
    }
    requested_range.destroy();
    overlapping_row_locks.deinit();
    return 0;
  }

  size_t num_overlapping_row_locks = overlapping_row_locks.get_size();

  // if any overlapping row locks conflict with this request, bail out.

  bool conflicts_exist =
      determine_conflicting_txnids(overlapping_row_locks, txnid, conflicts);
  if (!conflicts_exist) {
    // there are no conflicts, so all of the overlaps are for the requesting
    // txnid. so, we must consolidate all existing overlapping ranges and the
    // requested range into one dominating range. then we insert the dominating
    // range.
    bool all_shared = !is_write_request;
    for (size_t i = 0; i < num_overlapping_row_locks; i++) {
      row_lock overlapping_lock = overlapping_row_locks.fetch_unchecked(i);
      invariant(overlapping_lock.txnid == txnid);
      requested_range.extend(m_cmp, overlapping_lock.range);
      remove_row_lock_from_tree(lkr, overlapping_lock, TXNID_ANY, m_mgr);
      all_shared = all_shared && overlapping_lock.is_shared;
    }

    row_lock new_lock = {.range = requested_range,
                         .txnid = txnid,
                         .is_shared = all_shared,
                         .owners = nullptr};
    insert_row_lock_into_tree(lkr, new_lock, m_mgr);
  } else {
    r = DB_LOCK_NOTGRANTED;
  }

  requested_range.destroy();
  overlapping_row_locks.deinit();
  return r;
}

// acquire a lock in the given key range, inclusive. if successful,
// return 0. otherwise, populate the conflicts txnid_set with the set of
// transactions that conflict with this request.
int locktree::acquire_lock(bool is_write_request, TXNID txnid,
                           const DBT *left_key, const DBT *right_key,
                           txnid_set *conflicts) {
  int r = 0;

  // we are only supporting write locks for simplicity
  // invariant(is_write_request);

  // acquire and prepare a locked keyrange over the requested range.
  // prepare is a serialzation point, so we take the opportunity to
  // try the single txnid optimization first.
  concurrent_tree::locked_keyrange lkr;
  lkr.prepare(m_rangetree);

  bool acquired =
      sto_try_acquire(&lkr, txnid, left_key, right_key, is_write_request);
  if (!acquired) {
    r = acquire_lock_consolidated(&lkr, txnid, left_key, right_key,
                                  is_write_request, conflicts);
  }

  lkr.release();
  return r;
}

int locktree::try_acquire_lock(bool is_write_request, TXNID txnid,
                               const DBT *left_key, const DBT *right_key,
                               txnid_set *conflicts, bool big_txn) {
  // All ranges in the locktree must have left endpoints <= right endpoints.
  // Range comparisons rely on this fact, so we make a paranoid invariant here.
  paranoid_invariant(m_cmp(left_key, right_key) <= 0);
  int r = m_mgr == nullptr ? 0 : m_mgr->check_current_lock_constraints(big_txn);
  if (r == 0) {
    r = acquire_lock(is_write_request, txnid, left_key, right_key, conflicts);
  }
  return r;
}

// the locktree silently upgrades read locks to write locks for simplicity
int locktree::acquire_read_lock(TXNID txnid, const DBT *left_key,
                                const DBT *right_key, txnid_set *conflicts,
                                bool big_txn) {
  return try_acquire_lock(false, txnid, left_key, right_key, conflicts,
                          big_txn);
}

int locktree::acquire_write_lock(TXNID txnid, const DBT *left_key,
                                 const DBT *right_key, txnid_set *conflicts,
                                 bool big_txn) {
  return try_acquire_lock(true, txnid, left_key, right_key, conflicts, big_txn);
}

// typedef void (*dump_callback)(void *cdata, const DBT *left, const DBT *right,
// TXNID txnid);
void locktree::dump_locks(void *cdata, dump_callback cb) {
  concurrent_tree::locked_keyrange lkr;
  keyrange range;
  range.create(toku_dbt_negative_infinity(), toku_dbt_positive_infinity());

  lkr.prepare(m_rangetree);
  lkr.acquire(range);

  TXNID sto_txn;
  if ((sto_txn = toku_unsafe_fetch(m_sto_txnid)) != TXNID_NONE) {
    // insert all of the ranges from the single txnid buffer into a new rangtree
    range_buffer::iterator iter(&m_sto_buffer);
    range_buffer::iterator::record rec;
    while (iter.current(&rec)) {
      (*cb)(cdata, rec.get_left_key(), rec.get_right_key(), sto_txn,
            !rec.get_exclusive_flag(), nullptr);
      iter.next();
    }
  } else {
    GrowableArray<row_lock> all_locks;
    all_locks.init();
    iterate_and_get_overlapping_row_locks(&lkr, &all_locks);

    const size_t n_locks = all_locks.get_size();
    for (size_t i = 0; i < n_locks; i++) {
      const row_lock lock = all_locks.fetch_unchecked(i);
      (*cb)(cdata, lock.range.get_left_key(), lock.range.get_right_key(),
            lock.txnid, lock.is_shared, lock.owners);
    }
    all_locks.deinit();
  }
  lkr.release();
  range.destroy();
}

void locktree::get_conflicts(bool is_write_request, TXNID txnid,
                             const DBT *left_key, const DBT *right_key,
                             txnid_set *conflicts) {
  // because we only support write locks, ignore this bit for now.
  (void)is_write_request;

  // preparing and acquire a locked keyrange over the range
  keyrange range;
  range.create(left_key, right_key);
  concurrent_tree::locked_keyrange lkr;
  lkr.prepare(m_rangetree);
  lkr.acquire(range);

  // copy out the set of overlapping row locks and determine the conflicts
  GrowableArray<row_lock> overlapping_row_locks;
  overlapping_row_locks.init();
  iterate_and_get_overlapping_row_locks(&lkr, &overlapping_row_locks);

  // we don't care if conflicts exist. we just want the conflicts set populated.
  (void)determine_conflicting_txnids(overlapping_row_locks, txnid, conflicts);

  lkr.release();
  overlapping_row_locks.deinit();
  range.destroy();
}

// Effect:
//  For each range in the lock tree that overlaps the given range and has
//  the given txnid, remove it.
// Rationale:
//  In the common case, there is only the range [left_key, right_key] and
//  it is associated with txnid, so this is a single tree delete.
//
//  However, consolidation and escalation change the objects in the tree
//  without telling the txn anything.  In this case, the txn may own a
//  large range lock that represents its ownership of many smaller range
//  locks.  For example, the txn may think it owns point locks on keys 1,
//  2, and 3, but due to escalation, only the object [1,3] exists in the
//  tree.
//
//  The first call for a small lock will remove the large range lock, and
//  the rest of the calls should do nothing.  After the first release,
//  another thread can acquire one of the locks that the txn thinks it
//  still owns.  That's ok, because the txn doesn't want it anymore (it
//  unlocks everything at once), but it may find a lock that it does not
//  own.
//
//  In our example, the txn unlocks key 1, which actually removes the
//  whole lock [1,3].  Now, someone else can lock 2 before our txn gets
//  around to unlocking 2, so we should not remove that lock.
void locktree::remove_overlapping_locks_for_txnid(TXNID txnid,
                                                  const DBT *left_key,
                                                  const DBT *right_key) {
  keyrange release_range;
  release_range.create(left_key, right_key);

  // acquire and prepare a locked keyrange over the release range
  concurrent_tree::locked_keyrange lkr;
  lkr.prepare(m_rangetree);
  lkr.acquire(release_range);

  // copy out the set of overlapping row locks.
  GrowableArray<row_lock> overlapping_row_locks;
  overlapping_row_locks.init();
  iterate_and_get_overlapping_row_locks(&lkr, &overlapping_row_locks);
  size_t num_overlapping_row_locks = overlapping_row_locks.get_size();

  for (size_t i = 0; i < num_overlapping_row_locks; i++) {
    row_lock lock = overlapping_row_locks.fetch_unchecked(i);
    // If this isn't our lock, that's ok, just don't remove it.
    // See rationale above.
    // psergey-todo: for shared locks, just remove ourselves from the
    //               owners.
    if (lock.txnid == txnid || (lock.owners && lock.owners->contains(txnid))) {
      remove_row_lock_from_tree(&lkr, lock, txnid, m_mgr);
    }
  }

  lkr.release();
  overlapping_row_locks.deinit();
  release_range.destroy();
}

bool locktree::sto_txnid_is_valid_unsafe(void) const {
  return toku_unsafe_fetch(m_sto_txnid) != TXNID_NONE;
}

int locktree::sto_get_score_unsafe(void) const {
  return toku_unsafe_fetch(m_sto_score);
}

bool locktree::sto_try_release(TXNID txnid) {
  bool released = false;
  if (toku_unsafe_fetch(m_sto_txnid) != TXNID_NONE) {
    // check the bit again with a prepared locked keyrange,
    // which protects the optimization bits and rangetree data
    concurrent_tree::locked_keyrange lkr;
    lkr.prepare(m_rangetree);
    if (m_sto_txnid != TXNID_NONE) {
      // this txnid better be the single txnid on this locktree,
      // or else we are in big trouble (meaning the logic is broken)
      invariant(m_sto_txnid == txnid);
      invariant(m_rangetree->is_empty());
      sto_end();
      released = true;
    }
    lkr.release();
  }
  return released;
}

// release all of the locks for a txnid whose endpoints are pairs
// in the given range buffer.
void locktree::release_locks(TXNID txnid, const range_buffer *ranges,
                             bool all_trx_locks_hint) {
  // try the single txn optimization. if it worked, then all of the
  // locks are already released, otherwise we need to do it here.
  bool released;
  if (all_trx_locks_hint) {
    // This will release all of the locks the transaction is holding
    released = sto_try_release(txnid);
  } else {
    /*
      psergey: we are asked to release *Some* of the locks the transaction
      is holding.
      We could try doing that without leaving the STO mode, but right now,
      the easiest way is to exit the STO mode and let the non-STO code path
      handle it.
    */
    if (toku_unsafe_fetch(m_sto_txnid) != TXNID_NONE) {
      // check the bit again with a prepared locked keyrange,
      // which protects the optimization bits and rangetree data
      concurrent_tree::locked_keyrange lkr;
      lkr.prepare(m_rangetree);
      if (m_sto_txnid != TXNID_NONE) {
        sto_end_early(&lkr);
      }
      lkr.release();
    }
    released = false;
  }
  if (!released) {
    range_buffer::iterator iter(ranges);
    range_buffer::iterator::record rec;
    while (iter.current(&rec)) {
      const DBT *left_key = rec.get_left_key();
      const DBT *right_key = rec.get_right_key();
      // All ranges in the locktree must have left endpoints <= right endpoints.
      // Range comparisons rely on this fact, so we make a paranoid invariant
      // here.
      paranoid_invariant(m_cmp(left_key, right_key) <= 0);
      remove_overlapping_locks_for_txnid(txnid, left_key, right_key);
      iter.next();
    }
    // Increase the sto score slightly. Eventually it will hit
    // the threshold and we'll try the optimization again. This
    // is how a previously multithreaded system transitions into
    // a single threaded system that benefits from the optimization.
    if (toku_unsafe_fetch(m_sto_score) < STO_SCORE_THRESHOLD) {
      toku_sync_fetch_and_add(&m_sto_score, 1);
    }
  }
}

// iterate over a locked keyrange and extract copies of the first N
// row locks, storing each one into the given array of size N,
// then removing each extracted lock from the locked keyrange.
static int extract_first_n_row_locks(concurrent_tree::locked_keyrange *lkr,
                                     locktree_manager *mgr, row_lock *row_locks,
                                     int num_to_extract) {
  struct extract_fn_obj {
    int num_extracted;
    int num_to_extract;
    row_lock *row_locks;
    bool fn(const keyrange &range, TXNID txnid, bool is_shared,
            TxnidVector *owners) {
      if (num_extracted < num_to_extract) {
        row_lock lock;
        lock.range.create_copy(range);
        lock.txnid = txnid;
        lock.is_shared = is_shared;
        // deep-copy the set of owners:
        if (owners)
          lock.owners = new TxnidVector(*owners);
        else
          lock.owners = nullptr;
        row_locks[num_extracted++] = lock;
        return true;
      } else {
        return false;
      }
    }
  } extract_fn;

  extract_fn.row_locks = row_locks;
  extract_fn.num_to_extract = num_to_extract;
  extract_fn.num_extracted = 0;
  lkr->iterate(&extract_fn);

  // now that the ranges have been copied out, complete
  // the extraction by removing the ranges from the tree.
  // use remove_row_lock_from_tree() so we properly track the
  // amount of memory and number of locks freed.
  int num_extracted = extract_fn.num_extracted;
  invariant(num_extracted <= num_to_extract);
  for (int i = 0; i < num_extracted; i++) {
    remove_row_lock_from_tree(lkr, row_locks[i], TXNID_ANY, mgr);
  }

  return num_extracted;
}

// Store each newly escalated lock in a range buffer for appropriate txnid.
// We'll rebuild the locktree by iterating over these ranges, and then we
// can pass back each txnid/buffer pair individually through a callback
// to notify higher layers that locks have changed.
struct txnid_range_buffer {
  TXNID txnid;
  range_buffer buffer;

  static int find_by_txnid(struct txnid_range_buffer *const &other_buffer,
                           const TXNID &txnid) {
    if (txnid < other_buffer->txnid) {
      return -1;
    } else if (other_buffer->txnid == txnid) {
      return 0;
    } else {
      return 1;
    }
  }
};

// escalate the locks in the locktree by merging adjacent
// locks that have the same txnid into one larger lock.
//
// if there's only one txnid in the locktree then this
// approach works well. if there are many txnids and each
// has locks in a random/alternating order, then this does
// not work so well.
void locktree::escalate(lt_escalate_cb after_escalate_callback,
                        void *after_escalate_callback_extra) {
  omt<struct txnid_range_buffer *, struct txnid_range_buffer *> range_buffers;
  range_buffers.create();

  // prepare and acquire a locked keyrange on the entire locktree
  concurrent_tree::locked_keyrange lkr;
  keyrange infinite_range = keyrange::get_infinite_range();
  lkr.prepare(m_rangetree);
  lkr.acquire(infinite_range);

  // if we're in the single txnid optimization, simply call it off.
  // if you have to run escalation, you probably don't care about
  // the optimization anyway, and this makes things easier.
  if (m_sto_txnid != TXNID_NONE) {
    // We are already accounting for this escalation time and
    // count, so don't do it for sto_end_early too.
    sto_end_early_no_accounting(&lkr);
  }

  // extract and remove batches of row locks from the locktree
  int num_extracted;
  const int num_row_locks_per_batch = 128;
  row_lock *XCALLOC_N(num_row_locks_per_batch, extracted_buf);

  // we always remove the "first" n because we are removing n
  // each time we do an extraction. so this loops until its empty.
  while ((num_extracted = extract_first_n_row_locks(
              &lkr, m_mgr, extracted_buf, num_row_locks_per_batch)) > 0) {
    int current_index = 0;
    while (current_index < num_extracted) {
      // every batch of extracted locks is in range-sorted order. search
      // through them and merge adjacent locks with the same txnid into
      // one dominating lock and save it to a set of escalated locks.
      //
      // first, find the index of the next row lock that
      //  - belongs to a different txnid, or
      //  - belongs to several txnids, or
      //  - is a shared lock or follows one (we could potentially merge
      //    those but currently we don't), or
      //  - is across a lock escalation barrier.
      int next_txnid_index = current_index + 1;

      while (next_txnid_index < num_extracted &&
             !extracted_buf[current_index].is_shared &&
             (extracted_buf[current_index].txnid ==
              extracted_buf[next_txnid_index].txnid) &&
             !extracted_buf[next_txnid_index].is_shared &&
             !extracted_buf[next_txnid_index].owners &&
             !m_escalation_barrier(
                 extracted_buf[current_index].range.get_right_key(),
                 extracted_buf[next_txnid_index].range.get_left_key(),
                 m_escalation_barrier_arg)) {
        next_txnid_index++;
      }

      // Create an escalated range for the current txnid that dominates
      // each range between the current indext and the next txnid's index.
      // const TXNID current_txnid = extracted_buf[current_index].txnid;
      const DBT *escalated_left_key =
          extracted_buf[current_index].range.get_left_key();
      const DBT *escalated_right_key =
          extracted_buf[next_txnid_index - 1].range.get_right_key();

      // Try to find a range buffer for the current txnid. Create one if it
      // doesn't exist. Then, append the new escalated range to the buffer. (If
      // a lock is shared by multiple txnids, append it each of txnid's lists)
      TxnidVector *owners_ptr;
      TxnidVector singleton_owner;
      if (extracted_buf[current_index].owners)
        owners_ptr = extracted_buf[current_index].owners;
      else {
        singleton_owner.insert(extracted_buf[current_index].txnid);
        owners_ptr = &singleton_owner;
      }

      for (auto cur_txnid : *owners_ptr) {
        uint32_t idx;
        struct txnid_range_buffer *existing_range_buffer;
        int r =
            range_buffers.find_zero<TXNID, txnid_range_buffer::find_by_txnid>(
                cur_txnid, &existing_range_buffer, &idx);
        if (r == DB_NOTFOUND) {
          struct txnid_range_buffer *XMALLOC(new_range_buffer);
          new_range_buffer->txnid = cur_txnid;
          new_range_buffer->buffer.create();
          new_range_buffer->buffer.append(
              escalated_left_key, escalated_right_key,
              !extracted_buf[current_index].is_shared);
          range_buffers.insert_at(new_range_buffer, idx);
        } else {
          invariant_zero(r);
          invariant(existing_range_buffer->txnid == cur_txnid);
          existing_range_buffer->buffer.append(
              escalated_left_key, escalated_right_key,
              !extracted_buf[current_index].is_shared);
        }
      }

      current_index = next_txnid_index;
    }

    // destroy the ranges copied during the extraction
    for (int i = 0; i < num_extracted; i++) {
      delete extracted_buf[i].owners;
      extracted_buf[i].range.destroy();
    }
  }
  toku_free(extracted_buf);

  // Rebuild the locktree from each range in each range buffer,
  // then notify higher layers that the txnid's locks have changed.
  //
  // (shared locks: if a lock was initially shared between transactions TRX1,
  //  TRX2, etc, we will now try to acquire it acting on behalf on TRX1, on
  //  TRX2, etc.  This will succeed and an identical shared lock will be
  //  constructed)

  invariant(m_rangetree->is_empty());
  const uint32_t num_range_buffers = range_buffers.size();
  for (uint32_t i = 0; i < num_range_buffers; i++) {
    struct txnid_range_buffer *current_range_buffer;
    int r = range_buffers.fetch(i, &current_range_buffer);
    invariant_zero(r);
    if (r == EINVAL)  // Shouldn't happen, avoid compiler warning
      continue;

    const TXNID current_txnid = current_range_buffer->txnid;
    range_buffer::iterator iter(&current_range_buffer->buffer);
    range_buffer::iterator::record rec;
    while (iter.current(&rec)) {
      keyrange range;
      range.create(rec.get_left_key(), rec.get_right_key());
      row_lock lock = {.range = range,
                       .txnid = current_txnid,
                       .is_shared = !rec.get_exclusive_flag(),
                       .owners = nullptr};
      insert_row_lock_into_tree(&lkr, lock, m_mgr);
      iter.next();
    }

    // Notify higher layers that locks have changed for the current txnid
    if (after_escalate_callback) {
      after_escalate_callback(current_txnid, this, current_range_buffer->buffer,
                              after_escalate_callback_extra);
    }
    current_range_buffer->buffer.destroy();
  }

  while (range_buffers.size() > 0) {
    struct txnid_range_buffer *buffer;
    int r = range_buffers.fetch(0, &buffer);
    invariant_zero(r);
    r = range_buffers.delete_at(0);
    invariant_zero(r);
    toku_free(buffer);
  }
  range_buffers.destroy();

  lkr.release();
}

void *locktree::get_userdata(void) const { return m_userdata; }

void locktree::set_userdata(void *userdata) { m_userdata = userdata; }

struct lt_lock_request_info *locktree::get_lock_request_info(void) {
  return &m_lock_request_info;
}

void locktree::set_comparator(const comparator &cmp) { m_cmp.inherit(cmp); }

locktree_manager *locktree::get_manager(void) const { return m_mgr; }

int locktree::compare(const locktree *lt) const {
  if (m_dict_id.dictid < lt->m_dict_id.dictid) {
    return -1;
  } else if (m_dict_id.dictid == lt->m_dict_id.dictid) {
    return 0;
  } else {
    return 1;
  }
}

DICTIONARY_ID locktree::get_dict_id() const { return m_dict_id; }

} /* namespace toku */
#endif  // OS_WIN
