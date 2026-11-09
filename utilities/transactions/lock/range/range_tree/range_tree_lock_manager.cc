//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef OS_WIN

#include "utilities/transactions/lock/range/range_tree/range_tree_lock_manager.h"

#include <algorithm>
#include <cinttypes>
#include <memory>
#include <mutex>

#include "monitoring/perf_context_imp.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/thread_local.h"
#include "utilities/transactions/lock/range/range_tree/range_tree_lock_tracker.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace ROCKSDB_NAMESPACE {

RangeLockManagerHandle* NewRangeLockManager(
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory) {
  std::shared_ptr<TransactionDBMutexFactory> use_factory;

  if (mutex_factory) {
    use_factory = mutex_factory;
  } else {
    use_factory = std::make_shared<TransactionDBMutexFactoryImpl>();
  }
  return new RangeTreeLockManager(use_factory);
}

static const char SUFFIX_INFIMUM = 0x0;
static const char SUFFIX_SUPREMUM = 0x1;

// Convert Endpoint into an internal format used for storing it in locktree
// (DBT structure is used for passing endpoints to locktree and getting back)
void serialize_endpoint(const Endpoint& endp, std::string* buf) {
  buf->push_back(endp.inf_suffix ? SUFFIX_SUPREMUM : SUFFIX_INFIMUM);
  buf->append(endp.slice.data(), endp.slice.size());
}

// Decode the endpoint from the format it is stored in the locktree (DBT) to
// the one used outside: either Endpoint or EndpointWithString
template <typename EndpointStruct>
void deserialize_endpoint(const DBT* dbt, EndpointStruct* endp) {
  assert(dbt->size >= 1);
  const char* dbt_data = (const char*)dbt->data;
  char suffix = dbt_data[0];
  assert(suffix == SUFFIX_INFIMUM || suffix == SUFFIX_SUPREMUM);
  endp->inf_suffix = (suffix == SUFFIX_SUPREMUM);
  endp->slice = decltype(EndpointStruct::slice)(dbt_data + 1, dbt->size - 1);
}

// Get a range lock on [start_key; end_key] range
Status RangeTreeLockManager::TryLock(PessimisticTransaction* txn,
                                     uint32_t column_family_id,
                                     const Endpoint& start_endp,
                                     const Endpoint& end_endp, Env*,
                                     bool exclusive) {
  toku::lock_request request;
  request.create(mutex_factory_);
  DBT start_key_dbt;
  DBT end_key_dbt;

  TEST_SYNC_POINT("RangeTreeLockManager::TryRangeLock:enter");
  std::string start_key;
  std::string end_key;
  serialize_endpoint(start_endp, &start_key);
  serialize_endpoint(end_endp, &end_key);

  toku_fill_dbt(&start_key_dbt, start_key.data(), start_key.size());
  toku_fill_dbt(&end_key_dbt, end_key.data(), end_key.size());

  auto lt = GetLockTreeForCF(column_family_id);

  // Put the key waited on into request's m_extra. See
  // wait_callback_for_locktree for details.
  std::string wait_key(start_endp.slice.data(), start_endp.slice.size());

  request.set(lt.get(), (TXNID)txn, &start_key_dbt, &end_key_dbt,
              exclusive ? toku::lock_request::WRITE : toku::lock_request::READ,
              false /* not a big txn */, &wait_key);

  // Poll for kill only when an embedder installed a kill-check callback;
  // otherwise IsKilledThunk can never fire and the 100ms wakeups are pure
  // overhead.
  constexpr uint64_t kKillPollMs = 100;
  uint64_t killed_time_msec = 0;
  int (*killed_thunk)(void*) = nullptr;
  if (killed_callback_) {
    killed_time_msec = kKillPollMs;
    killed_thunk = &IsKilledThunk;
  }
  uint64_t wait_time_msec = txn->GetLockTimeout();

  if (wait_time_msec == static_cast<uint64_t>(-1)) {
    // The transaction has no wait timeout. lock_request::wait doesn't support
    // this, it needs a number of milliseconds to wait. Pass it one year to
    // be safe.
    wait_time_msec = uint64_t(1000) * 60 * 60 * 24 * 365;
  } else {
    // convert microseconds to milliseconds
    wait_time_msec = (wait_time_msec + 500) / 1000;
  }

  std::vector<RangeDeadlockInfo> di_path;
  request.m_deadlock_cb = [&](TXNID txnid, bool is_exclusive,
                              const DBT* start_dbt, const DBT* end_dbt) {
    EndpointWithString start;
    EndpointWithString end;
    deserialize_endpoint(start_dbt, &start);
    deserialize_endpoint(end_dbt, &end);

    di_path.push_back({txnid, column_family_id, is_exclusive, std::move(start),
                       std::move(end)});
  };

  request.start();

  const int r = request.wait(wait_time_msec, killed_time_msec, killed_thunk,
                             this, wait_callback_for_locktree, nullptr);

  // Inform the txn that we are no longer waiting:
  txn->ClearWaitingTxn();

  request.destroy();
  switch (r) {
    case 0:
      break;  // fall through
    case DB_LOCK_NOTGRANTED:
      return Status::TimedOut(Status::SubCode::kLockTimeout);
    case DB_LOCK_INTERRUPTED:
      return Status::Aborted();
    case TOKUDB_OUT_OF_LOCKS:
      return Status::LockLimit();
    case DB_LOCK_DEADLOCK: {
      std::reverse(di_path.begin(), di_path.end());
      dlock_buffer_.AddNewPath(
          RangeDeadlockPath(di_path, request.get_start_time()));
      return Status::Busy(Status::SubCode::kDeadlock);
    }
    default:
      assert(0);
      return Status::LockLimit();
  }

  return Status::OK();
}

int RangeTreeLockManager::IsKilledThunk(void* extra) {
  return static_cast<RangeTreeLockManager*>(extra)->killed_callback_() ? 1 : 0;
}

// Wait callback that locktree library will call to inform us about
// the lock waits that are in progress.
void wait_callback_for_locktree(void*, toku::lock_wait_infos* infos) {
  TEST_SYNC_POINT("RangeTreeLockManager::TryRangeLock:EnterWaitingTxn");
  for (const auto& wait_info : *infos) {
    // As long as we hold the lock on the locktree's pending request queue
    // this should be safe.
    auto txn = (PessimisticTransaction*)wait_info.waiter;
    auto cf_id = (ColumnFamilyId)wait_info.ltree->get_dict_id().dictid;

    autovector<TransactionID> waitee_ids;
    for (auto waitee : wait_info.waitees) {
      waitee_ids.push_back(waitee);
    }
    txn->SetWaitingTxn(waitee_ids, cf_id, (std::string*)wait_info.m_extra);
  }

  // Here we can assume that the locktree code will now wait for some lock
  TEST_SYNC_POINT("RangeTreeLockManager::TryRangeLock:WaitingTxn");
}

void RangeTreeLockManager::UnLock(PessimisticTransaction* txn,
                                  ColumnFamilyId column_family_id,
                                  const std::string& key, Env*) {
  auto locktree = GetLockTreeForCF(column_family_id);
  std::string endp_image;
  serialize_endpoint({key.data(), key.size(), false}, &endp_image);

  DBT key_dbt;
  toku_fill_dbt(&key_dbt, endp_image.data(), endp_image.size());

  toku::range_buffer range_buf;
  range_buf.create();
  range_buf.append(&key_dbt, &key_dbt);

  locktree->release_locks((TXNID)txn, &range_buf);
  range_buf.destroy();

  toku::lock_request::retry_all_lock_requests(
      locktree.get(), wait_callback_for_locktree, nullptr);
}

void RangeTreeLockManager::UnLock(PessimisticTransaction* txn,
                                  const LockTracker& tracker, Env*) {
  const RangeTreeLockTracker* range_tracker =
      static_cast<const RangeTreeLockTracker*>(&tracker);

  RangeTreeLockTracker* range_trx_tracker =
      static_cast<RangeTreeLockTracker*>(&txn->GetTrackedLocks());
  bool all_keys = (range_trx_tracker == range_tracker);

  // tracked_locks_->range_list may hold nullptr if the transaction has never
  // acquired any locks.
  ((RangeTreeLockTracker*)range_tracker)->ReleaseLocks(this, txn, all_keys);
}

// Comparator for range-lock endpoints. This is intentionally
// direction-AGNOSTIC: it orders the user-key bytes with the column family's own
// comparator, then breaks ties on the endpoint-type suffix byte (infimum before
// supremum), which is a positional marker independent of the CF's sort
// direction.
//
// CALLER REQUIREMENT: it is the caller's responsibility to pass in a flipped
// (swapped start/end, with negated infimum/supremum flags) range for RangeLock
// to work on a REVERSE-ORDERED column family. The locktree requires every range
// to satisfy left <= right, and this comparator does NOT compensate for reverse
// ordering on the caller's behalf. MyRocks does this flip today in
// ha_rocksdb::set_range_lock() (see its "RangeFlagsShouldBeFlippedForRevCF"
// comment). Do not re-add reverse-awareness here (that double-corrects callers
// like MyRocks that already flip).
int RangeTreeLockManager::CompareDbtEndpoints(void* arg, const DBT* a_key,
                                              const DBT* b_key) {
  const char* a = (const char*)a_key->data;
  const char* b = (const char*)b_key->data;

  size_t a_len = a_key->size;
  size_t b_len = b_key->size;

  size_t min_len = std::min(a_len, b_len);

  // Compare the values. The first byte encodes the endpoint type, its value
  // is either SUFFIX_INFIMUM or SUFFIX_SUPREMUM.
  Comparator* cmp = (Comparator*)arg;
  int res = cmp->CompareWithoutTimestamp(
      Slice(a + 1, min_len - 1), /*a_has_ts=*/false, Slice(b + 1, min_len - 1),
      /*b_has_ts=*/false);
  if (!res) {
    // The user keys compared equal above, so order by the endpoint-type suffix
    // byte (a positional boundary marker: infimum before the key, supremum
    // after) -- direction-independent; see the function-level comment.
    if (b_len > min_len) {
      // a's user key is a prefix of b's; a is shorter.
      return a[0] == SUFFIX_INFIMUM ? -1 : 1;
    } else if (a_len > min_len) {
      // b's user key is a prefix of a's; b is shorter.
      return b[0] == SUFFIX_INFIMUM ? 1 : -1;
    } else {
      // Same user key: infimum sorts before supremum.
      if (a[0] < b[0]) {
        return -1;
      } else if (a[0] > b[0]) {
        return 1;
      } else {
        return 0;
      }
    }
  } else {
    return res;
  }
}

namespace {
void UnrefLockTreeMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto lock_tree_map_cache = static_cast<
      std::unordered_map<ColumnFamilyId, std::shared_ptr<toku::locktree>>*>(
      ptr);
  delete lock_tree_map_cache;
}
}  // anonymous namespace

RangeTreeLockManager::RangeTreeLockManager(
    std::shared_ptr<TransactionDBMutexFactory> mutex_factory)
    : mutex_factory_(mutex_factory),
      ltree_lookup_cache_(new ThreadLocalPtr(&UnrefLockTreeMapsCache)),
      dlock_buffer_(10) {
  ltm_.create(on_create, on_destroy, on_escalate, nullptr, mutex_factory_);
}

int RangeTreeLockManager::on_create(toku::locktree* lt, void* arg) {
  // arg is a pointer to RangeTreeLockManager
  lt->set_escalation_barrier_func(&OnEscalationBarrierCheck, arg);
  return 0;
}

bool RangeTreeLockManager::OnEscalationBarrierCheck(const DBT* a, const DBT* b,
                                                    void* extra) {
  Endpoint a_endp;
  Endpoint b_endp;
  deserialize_endpoint(a, &a_endp);
  deserialize_endpoint(b, &b_endp);
  auto self = static_cast<RangeTreeLockManager*>(extra);
  return self->barrier_func_(a_endp, b_endp);
}

void RangeTreeLockManager::SetRangeDeadlockInfoBufferSize(
    uint32_t target_size) {
  dlock_buffer_.Resize(target_size);
}

void RangeTreeLockManager::Resize(uint32_t target_size) {
  SetRangeDeadlockInfoBufferSize(target_size);
}

std::vector<RangeDeadlockPath>
RangeTreeLockManager::GetRangeDeadlockInfoBuffer() {
  return dlock_buffer_.PrepareBuffer();
}

std::vector<DeadlockPath> RangeTreeLockManager::GetDeadlockInfoBuffer() {
  std::vector<DeadlockPath> res;
  std::vector<RangeDeadlockPath> data = GetRangeDeadlockInfoBuffer();
  // report left endpoints
  for (auto it = data.begin(); it != data.end(); ++it) {
    std::vector<DeadlockInfo> path;

    for (auto it2 = it->path.begin(); it2 != it->path.end(); ++it2) {
      path.push_back(
          {it2->m_txn_id, it2->m_cf_id, it2->m_exclusive, it2->m_start.slice});
    }
    res.emplace_back(path, it->deadlock_time);
  }
  return res;
}

// @brief  Lock Escalation Callback function
//
// @param txnid   Transaction whose locks got escalated
// @param lt      Lock Tree where escalation is happening
// @param buffer  Escalation result: list of locks that this transaction now
//                owns in this lock tree.
// @param void*   Callback context
void RangeTreeLockManager::on_escalate(TXNID txnid, const toku::locktree* lt,
                                       const toku::range_buffer& buffer,
                                       void*) {
  auto txn = (PessimisticTransaction*)txnid;
  ((RangeTreeLockTracker*)&txn->GetTrackedLocks())->ReplaceLocks(lt, buffer);
}

RangeTreeLockManager::~RangeTreeLockManager() {
  autovector<void*> local_caches;
  ltree_lookup_cache_->Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<LockTreeMap*>(cache);
  }
  ltree_map_.clear();  // this will call release_lt() for all locktrees
  ltm_.destroy();
}

RangeLockManagerHandle::Counters RangeTreeLockManager::GetStatus() {
  LTM_STATUS_S ltm_status_test;
  ltm_.get_status(&ltm_status_test);
  Counters res;

  // Searching status variable by its string name is how Toku's unit tests
  // do it (why didn't they make LTM_ESCALATION_COUNT constant visible?)
  // lookup keyname in status
  for (int i = 0; i < LTM_STATUS_S::LTM_STATUS_NUM_ROWS; i++) {
    TOKU_ENGINE_STATUS_ROW status = &ltm_status_test.status[i];
    if (strcmp(status->keyname, "LTM_ESCALATION_COUNT") == 0) {
      res.escalation_count = status->value.num;
      continue;
    }
    if (strcmp(status->keyname, "LTM_WAIT_COUNT") == 0) {
      res.lock_wait_count = status->value.num;
      continue;
    }
    if (strcmp(status->keyname, "LTM_SIZE_CURRENT") == 0) {
      res.current_lock_memory = status->value.num;
    }
  }
  return res;
}

std::shared_ptr<toku::locktree> RangeTreeLockManager::MakeLockTreePtr(
    toku::locktree* lt) {
  toku::locktree_manager* ltm = &ltm_;
  return std::shared_ptr<toku::locktree>(
      lt, [ltm](toku::locktree* p) { ltm->release_lt(p); });
}

void RangeTreeLockManager::AddColumnFamily(const ColumnFamilyHandle* cfh) {
  uint32_t column_family_id = cfh->GetID();

  InstrumentedMutexLock l(&ltree_map_mutex_);
  if (ltree_map_.find(column_family_id) == ltree_map_.end()) {
    DICTIONARY_ID dict_id = {.dictid = column_family_id};
    toku::comparator cmp;
    cmp.create(CompareDbtEndpoints, (void*)cfh->GetComparator());
    toku::locktree* ltree =
        ltm_.get_lt(dict_id, cmp,
                    /* on_create_extra*/ static_cast<void*>(this));
    // This is ok to because get_lt has copied the comparator:
    cmp.destroy();

    ltree_map_.insert({column_family_id, MakeLockTreePtr(ltree)});
  }
}

void RangeTreeLockManager::RemoveColumnFamily(const ColumnFamilyHandle* cfh) {
  uint32_t column_family_id = cfh->GetID();
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep using it
  // until they release their references to it.

  // TODO what if one drops a column family while transaction(s) still have
  // locks in it?
  // locktree uses column family'c Comparator* as the criteria to do tree
  // ordering. If the comparator is gone, we won't even be able to remove the
  // elements from the locktree.
  // A possible solution might be to remove everything right now:
  //  - wait until everyone traversing the locktree are gone
  //  - remove everything from the locktree.
  //  - some transactions may have acquired locks in their LockTracker objects.
  //    Arrange something so we don't blow up when they try to release them.
  //  - ...
  // This use case (drop column family while somebody is using it) doesn't seem
  // the priority, though.

  {
    InstrumentedMutexLock l(&ltree_map_mutex_);

    auto lock_maps_iter = ltree_map_.find(column_family_id);
    assert(lock_maps_iter != ltree_map_.end());
    ltree_map_.erase(lock_maps_iter);
  }  // lock_map_mutex_

  autovector<void*> local_caches;
  ltree_lookup_cache_->Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<LockTreeMap*>(cache);
  }
}

std::shared_ptr<toku::locktree> RangeTreeLockManager::GetLockTreeForCF(
    ColumnFamilyId column_family_id) {
  // First check thread-local cache
  if (ltree_lookup_cache_->Get() == nullptr) {
    ltree_lookup_cache_->Reset(new LockTreeMap());
  }

  auto ltree_map_cache = static_cast<LockTreeMap*>(ltree_lookup_cache_->Get());

  auto it = ltree_map_cache->find(column_family_id);
  if (it != ltree_map_cache->end()) {
    // Found lock map for this column family.
    return it->second;
  }

  // Not found in local cache, grab mutex and check shared LockMaps
  InstrumentedMutexLock l(&ltree_map_mutex_);

  it = ltree_map_.find(column_family_id);
  if (it == ltree_map_.end()) {
    return nullptr;
  } else {
    // Found lock map.  Store in thread-local cache and return.
    ltree_map_cache->insert({column_family_id, it->second});
    return it->second;
  }
}

struct LOCK_PRINT_CONTEXT {
  RangeLockManagerHandle::RangeLockStatus* data;  // Save locks here
  uint32_t cfh_id;  // Column Family whose tree we are traversing
};

// Report left endpoints of the acquired locks
LockManager::PointLockStatus RangeTreeLockManager::GetPointLockStatus() {
  PointLockStatus res;
  LockManager::RangeLockStatus data = GetRangeLockStatus();
  // report left endpoints
  for (auto it = data.begin(); it != data.end(); ++it) {
    auto& val = it->second;
    res.insert({it->first, {val.start.slice, val.ids, val.exclusive}});
  }
  return res;
}

static void push_into_lock_status_data(void* param, const DBT* left,
                                       const DBT* right, TXNID txnid_arg,
                                       bool is_shared, TxnidVector* owners) {
  struct LOCK_PRINT_CONTEXT* ctx = (LOCK_PRINT_CONTEXT*)param;
  struct RangeLockInfo info;

  info.exclusive = !is_shared;

  deserialize_endpoint(left, &info.start);
  deserialize_endpoint(right, &info.end);

  if (txnid_arg != TXNID_SHARED) {
    info.ids.push_back(txnid_arg);
  } else {
    for (auto it : *owners) {
      info.ids.push_back(it);
    }
  }
  ctx->data->insert({ctx->cfh_id, info});
}

LockManager::RangeLockStatus RangeTreeLockManager::GetRangeLockStatus() {
  LockManager::RangeLockStatus data;
  {
    InstrumentedMutexLock l(&ltree_map_mutex_);
    for (const auto& it : ltree_map_) {
      LOCK_PRINT_CONTEXT ctx = {&data, it.first};
      it.second->dump_locks((void*)&ctx, push_into_lock_status_data);
    }
  }
  return data;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // OS_WIN
