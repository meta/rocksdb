//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/secondary_cache_adapter.h"

#include <atomic>

#include "cache/tiered_secondary_cache.h"
#include "monitoring/perf_context_imp.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// A distinct pointer value for marking "dummy" cache entries
struct Dummy {
  char val[7] = "kDummy";
};
const Dummy kDummy{};
Cache::ObjectPtr const kDummyObj = const_cast<Dummy*>(&kDummy);
constexpr char kTieredCacheName[] = "TieredCache";
}  // namespace

// When CacheWithSecondaryAdapter is constructed with the distribute_cache_res
// parameter set to true, it manages the entire memory budget across the
// primary and secondary cache. The secondary cache is assumed to be in
// memory, such as the CompressedSecondaryCache. When a placeholder entry
// is inserted by a CacheReservationManager instance to reserve memory,
// the CacheWithSecondaryAdapter ensures that the reservation is distributed
// proportionally across the primary/secondary caches.
//
// The primary block cache is initially sized to the sum of the primary cache
// budget + the secondary cache budget, as follows -
//   |---------    Primary Cache Configured Capacity  -----------|
//   |---Secondary Cache Budget----|----Primary Cache Budget-----|
//
// A ConcurrentCacheReservationManager member in the CacheWithSecondaryAdapter,
// pri_cache_res_,
// is used to help with tracking the distribution of memory reservations.
// Initially, it accounts for the entire secondary cache budget as a
// reservation against the primary cache. This shrinks the usable capacity of
// the primary cache to the budget that the user originally desired.
//
//   |--Reservation for Sec Cache--|-Pri Cache Usable Capacity---|
//
// When a reservation placeholder is inserted into the adapter, it is inserted
// directly into the primary cache. This means the entire charge of the
// placeholder is counted against the primary cache. To compensate and count
// a portion of it against the secondary cache, the secondary cache Deflate()
// method is called to shrink it. Since the Deflate() causes the secondary
// actual usage to shrink, it is reflected here by releasing an equal amount
// from the pri_cache_res_ reservation. The Deflate() in the secondary cache
// can be, but is not required to be, implemented using its own cache
// reservation manager.
//
// For example, if the pri/sec ratio is 70/30, and the combined capacity is
// 100MB, the intermediate and final  state after inserting a reservation
// placeholder for 10MB would be as follows -
//
//   |-Reservation for Sec Cache-|-Pri Cache Usable Capacity-|---R---|
// 1. After inserting the placeholder in primary
//   |-------  30MB -------------|------- 60MB -------------|-10MB--|
// 2. After deflating the secondary and adjusting the reservation for
//    secondary against the primary
//   |-------  27MB -------------|------- 63MB -------------|-10MB--|
//
// Likewise, when the user inserted placeholder is released, the secondary
// cache Inflate() method is called to grow it, and the pri_cache_res_
// reservation is increased by an equal amount.
//
// Another way of implementing this would have been to simply split the user
// reservation into primary and secondary components. However, this would
// require allocating a structure to track the associated secondary cache
// reservation, which adds some complexity and overhead.
//
CacheWithSecondaryAdapter::CacheWithSecondaryAdapter(
    std::shared_ptr<Cache> target,
    std::shared_ptr<SecondaryCache> secondary_cache,
    TieredAdmissionPolicy adm_policy, bool distribute_cache_res)
    : CacheWrapper(std::move(target)),
      secondary_cache_(std::move(secondary_cache)),
      adm_policy_(adm_policy),
      distribute_cache_res_(distribute_cache_res),
      placeholder_usage_(0),
      reserved_usage_(0),
      sec_reserved_(0) {
  target_->SetEvictionCallback(
      [this](const Slice& key, Handle* handle, bool was_hit) {
        return EvictionHandler(key, handle, was_hit);
      });
  if (distribute_cache_res_) {
    size_t sec_capacity = 0;
    pri_cache_res_ = std::make_shared<ConcurrentCacheReservationManager>(
        std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
            target_));
    Status s = secondary_cache_->GetCapacity(sec_capacity);
    assert(s.ok());
    // Initially, the primary cache is sized to uncompressed cache budget plsu
    // compressed secondary cache budget. The secondary cache budget is then
    // taken away from the primary cache through cache reservations. Later,
    // when a placeholder entry is inserted by the caller, its inserted
    // into the primary cache and the portion that should be assigned to the
    // secondary cache is freed from the reservation.
    s = pri_cache_res_->UpdateCacheReservation(sec_capacity);
    assert(s.ok());
    sec_cache_res_ratio_ = (double)sec_capacity / target_->GetCapacity();
  }
}

CacheWithSecondaryAdapter::~CacheWithSecondaryAdapter() {
  // `*this` will be destroyed before `*target_`, so we have to prevent
  // use after free
  target_->SetEvictionCallback({});
#ifndef NDEBUG
  if (distribute_cache_res_) {
    size_t sec_capacity = 0;
    Status s = secondary_cache_->GetCapacity(sec_capacity);
    assert(s.ok());
    assert(placeholder_usage_ == 0);
    assert(reserved_usage_ == 0);
    if (pri_cache_res_->GetTotalMemoryUsed() != sec_capacity) {
      fprintf(stdout,
              "~CacheWithSecondaryAdapter: Primary cache reservation: "
              "%zu, Secondary cache capacity: %zu, "
              "Secondary cache reserved: %zu\n",
              pri_cache_res_->GetTotalMemoryUsed(), sec_capacity,
              sec_reserved_);
    }
  }
#endif  // NDEBUG
}

bool CacheWithSecondaryAdapter::EvictionHandler(const Slice& key,
                                                Handle* handle, bool was_hit) {
  auto helper = GetCacheItemHelper(handle);
  if (helper->IsSecondaryCacheCompatible() &&
      adm_policy_ != TieredAdmissionPolicy::kAdmPolicyThreeQueue) {
    auto obj = target_->Value(handle);
    // Ignore dummy entry
    if (obj != kDummyObj) {
      bool force = false;
      if (adm_policy_ == TieredAdmissionPolicy::kAdmPolicyAllowCacheHits) {
        force = was_hit;
      } else if (adm_policy_ == TieredAdmissionPolicy::kAdmPolicyAllowAll) {
        force = true;
      }
      // Spill into secondary cache.
      secondary_cache_->Insert(key, obj, helper, force).PermitUncheckedError();
    }
  }
  // Never takes ownership of obj
  return false;
}

bool CacheWithSecondaryAdapter::ProcessDummyResult(Cache::Handle** handle,
                                                   bool erase) {
  if (*handle && target_->Value(*handle) == kDummyObj) {
    target_->Release(*handle, erase);
    *handle = nullptr;
    return true;
  } else {
    return false;
  }
}

void CacheWithSecondaryAdapter::CleanupCacheObject(
    ObjectPtr obj, const CacheItemHelper* helper) {
  if (helper->del_cb) {
    helper->del_cb(obj, memory_allocator());
  }
}

Cache::Handle* CacheWithSecondaryAdapter::Promote(
    std::unique_ptr<SecondaryCacheResultHandle>&& secondary_handle,
    const Slice& key, const CacheItemHelper* helper, Priority priority,
    Statistics* stats, bool found_dummy_entry, bool kept_in_sec_cache) {
  assert(secondary_handle->IsReady());

  ObjectPtr obj = secondary_handle->Value();
  if (!obj) {
    // Nothing found.
    return nullptr;
  }
  // Found something.
  switch (helper->role) {
    case CacheEntryRole::kFilterBlock:
      RecordTick(stats, SECONDARY_CACHE_FILTER_HITS);
      break;
    case CacheEntryRole::kIndexBlock:
      RecordTick(stats, SECONDARY_CACHE_INDEX_HITS);
      break;
    case CacheEntryRole::kDataBlock:
      RecordTick(stats, SECONDARY_CACHE_DATA_HITS);
      break;
    case CacheEntryRole::kFilterMetaBlock:
    case CacheEntryRole::kDeprecatedFilterBlock:
    case CacheEntryRole::kOtherBlock:
    case CacheEntryRole::kWriteBuffer:
    case CacheEntryRole::kCompressionDictionaryBuildingBuffer:
    case CacheEntryRole::kFilterConstruction:
    case CacheEntryRole::kBlockBasedTableReader:
    case CacheEntryRole::kFileMetadata:
    case CacheEntryRole::kBlobValue:
    case CacheEntryRole::kBlobCache:
    case CacheEntryRole::kMisc:
    default:
      break;
  }
  PERF_COUNTER_ADD(secondary_cache_hit_count, 1);
  RecordTick(stats, SECONDARY_CACHE_HITS);

  // Note: SecondaryCache::Size() is really charge (from the CreateCallback)
  size_t charge = secondary_handle->Size();
  Handle* result = nullptr;
  // Insert into primary cache, possibly as a standalone+dummy entries.
  if (secondary_cache_->SupportForceErase() && !found_dummy_entry) {
    // Create standalone and insert dummy
    // Allow standalone to be created even if cache is full, to avoid
    // reading the entry from storage.
    result =
        CreateStandalone(key, obj, helper, charge, /*allow_uncharged*/ true);
    assert(result);
    PERF_COUNTER_ADD(block_cache_standalone_handle_count, 1);

    // Insert dummy to record recent use
    // TODO: try to avoid case where inserting this dummy could overwrite a
    // regular entry
    Status s = Insert(key, kDummyObj, &kNoopCacheItemHelper, /*charge=*/0,
                      /*handle=*/nullptr, priority);
    s.PermitUncheckedError();
    // Nothing to do or clean up on dummy insertion failure
  } else {
    // Insert regular entry into primary cache.
    // Don't allow it to spill into secondary cache again if it was kept there.
    Status s = Insert(
        key, obj, kept_in_sec_cache ? helper->without_secondary_compat : helper,
        charge, &result, priority);
    if (s.ok()) {
      assert(result);
      PERF_COUNTER_ADD(block_cache_real_handle_count, 1);
    } else {
      // Create standalone result instead, even if cache is full, to avoid
      // reading the entry from storage.
      result =
          CreateStandalone(key, obj, helper, charge, /*allow_uncharged*/ true);
      assert(result);
      PERF_COUNTER_ADD(block_cache_standalone_handle_count, 1);
    }
  }
  return result;
}

Status CacheWithSecondaryAdapter::Insert(const Slice& key, ObjectPtr value,
                                         const CacheItemHelper* helper,
                                         size_t charge, Handle** handle,
                                         Priority priority,
                                         const Slice& compressed_value,
                                         CompressionType type) {
  Status s = target_->Insert(key, value, helper, charge, handle, priority);
  if (s.ok() && value == nullptr && distribute_cache_res_ && handle) {
    charge = target_->GetCharge(*handle);

    MutexLock l(&cache_res_mutex_);
    placeholder_usage_ += charge;
    // Check if total placeholder reservation is more than the overall
    // cache capacity. If it is, then we don't try to charge the
    // secondary cache because we don't want to overcharge it (beyond
    // its capacity).
    // In order to make this a bit more lightweight, we also check if
    // the difference between placeholder_usage_ and reserved_usage_ is
    // atleast kReservationChunkSize and avoid any adjustments if not.
    if ((placeholder_usage_ <= target_->GetCapacity()) &&
        ((placeholder_usage_ - reserved_usage_) >= kReservationChunkSize)) {
      reserved_usage_ = placeholder_usage_ & ~(kReservationChunkSize - 1);
      size_t new_sec_reserved =
          static_cast<size_t>(reserved_usage_ * sec_cache_res_ratio_);
      size_t sec_charge = new_sec_reserved - sec_reserved_;
      s = secondary_cache_->Deflate(sec_charge);
      assert(s.ok());
      s = pri_cache_res_->UpdateCacheReservation(sec_charge,
                                                 /*increase=*/false);
      assert(s.ok());
      sec_reserved_ += sec_charge;
    }
  }
  // Warm up the secondary cache with the compressed block. The secondary
  // cache may choose to ignore it based on the admission policy.
  if (value != nullptr && !compressed_value.empty() &&
      adm_policy_ == TieredAdmissionPolicy::kAdmPolicyThreeQueue &&
      helper->IsSecondaryCacheCompatible()) {
    Status status = secondary_cache_->InsertSaved(key, compressed_value, type);
    assert(status.ok() || status.IsNotSupported());
  }

  return s;
}

Cache::Handle* CacheWithSecondaryAdapter::Lookup(const Slice& key,
                                                 const CacheItemHelper* helper,
                                                 CreateContext* create_context,
                                                 Priority priority,
                                                 Statistics* stats) {
  // NOTE: we could just StartAsyncLookup() and Wait(), but this should be a bit
  // more efficient
  Handle* result =
      target_->Lookup(key, helper, create_context, priority, stats);
  bool secondary_compatible = helper && helper->IsSecondaryCacheCompatible();
  bool found_dummy_entry =
      ProcessDummyResult(&result, /*erase=*/secondary_compatible);
  if (!result && secondary_compatible) {
    // Try our secondary cache
    bool kept_in_sec_cache = false;
    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle =
        secondary_cache_->Lookup(key, helper, create_context, /*wait*/ true,
                                 found_dummy_entry, stats,
                                 /*out*/ kept_in_sec_cache);
    if (secondary_handle) {
      result = Promote(std::move(secondary_handle), key, helper, priority,
                       stats, found_dummy_entry, kept_in_sec_cache);
    }
  }
  return result;
}

bool CacheWithSecondaryAdapter::Release(Handle* handle,
                                        bool erase_if_last_ref) {
  if (erase_if_last_ref) {
    ObjectPtr v = target_->Value(handle);
    if (v == nullptr && distribute_cache_res_) {
      size_t charge = target_->GetCharge(handle);

      MutexLock l(&cache_res_mutex_);
      placeholder_usage_ -= charge;
      // Check if total placeholder reservation is more than the overall
      // cache capacity. If it is, then we do nothing as reserved_usage_ must
      // be already maxed out
      if ((placeholder_usage_ <= target_->GetCapacity()) &&
          (placeholder_usage_ < reserved_usage_)) {
        // Adjust reserved_usage_ in chunks of kReservationChunkSize, so
        // we don't hit this slow path too often.
        reserved_usage_ = placeholder_usage_ & ~(kReservationChunkSize - 1);
        size_t new_sec_reserved =
            static_cast<size_t>(reserved_usage_ * sec_cache_res_ratio_);
        size_t sec_charge = sec_reserved_ - new_sec_reserved;
        Status s = secondary_cache_->Inflate(sec_charge);
        assert(s.ok());
        s = pri_cache_res_->UpdateCacheReservation(sec_charge,
                                                   /*increase=*/true);
        assert(s.ok());
        sec_reserved_ -= sec_charge;
      }
    }
  }
  return target_->Release(handle, erase_if_last_ref);
}

Cache::ObjectPtr CacheWithSecondaryAdapter::Value(Handle* handle) {
  ObjectPtr v = target_->Value(handle);
  // TODO with stacked secondaries: might fail in EvictionHandler
  assert(v != kDummyObj);
  return v;
}

void CacheWithSecondaryAdapter::StartAsyncLookupOnMySecondary(
    AsyncLookupHandle& async_handle) {
  assert(!async_handle.IsPending());
  assert(async_handle.result_handle == nullptr);

  std::unique_ptr<SecondaryCacheResultHandle> secondary_handle =
      secondary_cache_->Lookup(
          async_handle.key, async_handle.helper, async_handle.create_context,
          /*wait*/ false, async_handle.found_dummy_entry, async_handle.stats,
          /*out*/ async_handle.kept_in_sec_cache);
  if (secondary_handle) {
    // TODO with stacked secondaries: Check & process if already ready?
    async_handle.pending_handle = secondary_handle.release();
    async_handle.pending_cache = secondary_cache_.get();
  }
}

void CacheWithSecondaryAdapter::StartAsyncLookup(
    AsyncLookupHandle& async_handle) {
  target_->StartAsyncLookup(async_handle);
  if (!async_handle.IsPending()) {
    bool secondary_compatible =
        async_handle.helper &&
        async_handle.helper->IsSecondaryCacheCompatible();
    async_handle.found_dummy_entry |= ProcessDummyResult(
        &async_handle.result_handle, /*erase=*/secondary_compatible);

    if (async_handle.Result() == nullptr && secondary_compatible) {
      // Not found and not pending on another secondary cache
      StartAsyncLookupOnMySecondary(async_handle);
    }
  }
}

void CacheWithSecondaryAdapter::WaitAll(AsyncLookupHandle* async_handles,
                                        size_t count) {
  if (count == 0) {
    // Nothing to do
    return;
  }
  // Requests that are pending on *my* secondary cache, at the start of this
  // function
  std::vector<AsyncLookupHandle*> my_pending;
  // Requests that are pending on an "inner" secondary cache (managed somewhere
  // under target_), as of the start of this function
  std::vector<AsyncLookupHandle*> inner_pending;

  // Initial accounting of pending handles, excluding those already handled
  // by "outer" secondary caches. (See cur->pending_cache = nullptr.)
  for (size_t i = 0; i < count; ++i) {
    AsyncLookupHandle* cur = async_handles + i;
    if (cur->pending_cache) {
      assert(cur->IsPending());
      assert(cur->helper);
      assert(cur->helper->IsSecondaryCacheCompatible());
      if (cur->pending_cache == secondary_cache_.get()) {
        my_pending.push_back(cur);
        // Mark as "to be handled by this caller"
        cur->pending_cache = nullptr;
      } else {
        // Remember as potentially needing a lookup in my secondary
        inner_pending.push_back(cur);
      }
    }
  }

  // Wait on inner-most cache lookups first
  // TODO with stacked secondaries: because we are not using proper
  // async/await constructs here yet, there is a false synchronization point
  // here where all the results at one level are needed before initiating
  // any lookups at the next level. Probably not a big deal, but worth noting.
  if (!inner_pending.empty()) {
    target_->WaitAll(async_handles, count);
  }

  // For those that failed to find something, convert to lookup in my
  // secondary cache.
  for (AsyncLookupHandle* cur : inner_pending) {
    if (cur->Result() == nullptr) {
      // Not found, try my secondary
      StartAsyncLookupOnMySecondary(*cur);
      if (cur->IsPending()) {
        assert(cur->pending_cache == secondary_cache_.get());
        my_pending.push_back(cur);
        // Mark as "to be handled by this caller"
        cur->pending_cache = nullptr;
      }
    }
  }

  // Wait on all lookups on my secondary cache
  {
    std::vector<SecondaryCacheResultHandle*> my_secondary_handles;
    for (AsyncLookupHandle* cur : my_pending) {
      my_secondary_handles.push_back(cur->pending_handle);
    }
    secondary_cache_->WaitAll(std::move(my_secondary_handles));
  }

  // Process results
  for (AsyncLookupHandle* cur : my_pending) {
    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle(
        cur->pending_handle);
    cur->pending_handle = nullptr;
    cur->result_handle = Promote(
        std::move(secondary_handle), cur->key, cur->helper, cur->priority,
        cur->stats, cur->found_dummy_entry, cur->kept_in_sec_cache);
    assert(cur->pending_cache == nullptr);
  }
}

std::string CacheWithSecondaryAdapter::GetPrintableOptions() const {
  std::string str = target_->GetPrintableOptions();
  str.append("  secondary_cache:\n");
  str.append(secondary_cache_->GetPrintableOptions());
  return str;
}

const char* CacheWithSecondaryAdapter::Name() const {
  if (distribute_cache_res_) {
    return kTieredCacheName;
  } else {
    // To the user, at least for now, configure the underlying cache with
    // a secondary cache. So we pretend to be that cache
    return target_->Name();
  }
}

// Update the total cache capacity. If we're distributing cache reservations
// to both primary and secondary, then update the pri_cache_res_reservation
// as well. At the moment, we don't have a good way of handling the case
// where the new capacity < total cache reservations.
void CacheWithSecondaryAdapter::SetCapacity(size_t capacity) {
  if (distribute_cache_res_) {
    MutexLock m(&cache_res_mutex_);
    size_t sec_capacity = static_cast<size_t>(capacity * sec_cache_res_ratio_);
    size_t old_sec_capacity = 0;

    Status s = secondary_cache_->GetCapacity(old_sec_capacity);
    if (!s.ok()) {
      return;
    }
    if (old_sec_capacity > sec_capacity) {
      // We're shrinking the cache. We do things in the following order to
      // avoid a temporary spike in usage over the configured capacity -
      // 1. Lower the secondary cache capacity
      // 2. Credit an equal amount (by decreasing pri_cache_res_) to the
      //    primary cache
      // 3. Decrease the primary cache capacity to the total budget
      s = secondary_cache_->SetCapacity(sec_capacity);
      if (s.ok()) {
        if (placeholder_usage_ > capacity) {
          // Adjust reserved_usage_ down
          reserved_usage_ = capacity & ~(kReservationChunkSize - 1);
        }
        size_t new_sec_reserved =
            static_cast<size_t>(reserved_usage_ * sec_cache_res_ratio_);
        s = pri_cache_res_->UpdateCacheReservation(
            (old_sec_capacity - sec_capacity) -
                (sec_reserved_ - new_sec_reserved),
            /*increase=*/false);
        sec_reserved_ = new_sec_reserved;
        assert(s.ok());
        target_->SetCapacity(capacity);
      }
    } else {
      // We're expanding the cache. Do it in the following order to avoid
      // unnecessary evictions -
      // 1. Increase the primary cache capacity to total budget
      // 2. Reserve additional memory in primary on behalf of secondary (by
      //    increasing pri_cache_res_ reservation)
      // 3. Increase secondary cache capacity
      target_->SetCapacity(capacity);
      s = pri_cache_res_->UpdateCacheReservation(
          sec_capacity - old_sec_capacity,
          /*increase=*/true);
      assert(s.ok());
      s = secondary_cache_->SetCapacity(sec_capacity);
      assert(s.ok());
    }
  } else {
    // No cache reservation distribution. Just set the primary cache capacity.
    target_->SetCapacity(capacity);
  }
}

Status CacheWithSecondaryAdapter::GetSecondaryCacheCapacity(
    size_t& size) const {
  return secondary_cache_->GetCapacity(size);
}

Status CacheWithSecondaryAdapter::GetSecondaryCachePinnedUsage(
    size_t& size) const {
  Status s;
  if (distribute_cache_res_) {
    MutexLock m(&cache_res_mutex_);
    size_t capacity = 0;
    s = secondary_cache_->GetCapacity(capacity);
    if (s.ok()) {
      size = capacity - pri_cache_res_->GetTotalMemoryUsed();
    } else {
      size = 0;
    }
  } else {
    size = 0;
  }
  return s;
}

// Update the secondary/primary allocation ratio (remember, the primary
// capacity is the total memory budget when distribute_cache_res_ is true).
// When the ratio changes, we may accumulate some error in the calculations
// for secondary cache inflate/deflate and pri_cache_res_ reservations.
// This is due to the rounding of the reservation amount.
//
// We rely on the current pri_cache_res_ total memory used to estimate the
// new secondary cache reservation after the ratio change. For this reason,
// once the ratio is lowered to 0.0 (effectively disabling the secondary
// cache and pri_cache_res_ total mem used going down to 0), we cannot
// increase the ratio and re-enable it, We might remove this limitation
// in the future.
Status CacheWithSecondaryAdapter::UpdateCacheReservationRatio(
    double compressed_secondary_ratio) {
  if (!distribute_cache_res_) {
    return Status::NotSupported();
  }

  MutexLock m(&cache_res_mutex_);
  size_t pri_capacity = target_->GetCapacity();
  size_t sec_capacity =
      static_cast<size_t>(pri_capacity * compressed_secondary_ratio);
  size_t old_sec_capacity = 0;
  Status s = secondary_cache_->GetCapacity(old_sec_capacity);
  if (!s.ok()) {
    return s;
  }

  // Calculate the new secondary cache reservation
  // reserved_usage_ will never be > the cache capacity, so we don't
  // have to worry about adjusting it here.
  sec_cache_res_ratio_ = compressed_secondary_ratio;
  size_t new_sec_reserved =
      static_cast<size_t>(reserved_usage_ * sec_cache_res_ratio_);
  if (sec_capacity > old_sec_capacity) {
    // We're increasing the ratio, thus ending up with a larger secondary
    // cache and a smaller usable primary cache capacity. Similar to
    // SetCapacity(), we try to avoid a temporary increase in total usage
    // beyond the configured capacity -
    // 1. A higher secondary cache ratio means it gets a higher share of
    //    cache reservations. So first account for that by deflating the
    //    secondary cache
    // 2. Increase pri_cache_res_ reservation to reflect the new secondary
    //    cache utilization (increase in capacity - increase in share of cache
    //    reservation)
    // 3. Increase secondary cache capacity
    assert(new_sec_reserved >= sec_reserved_);
    s = secondary_cache_->Deflate(new_sec_reserved - sec_reserved_);
    assert(s.ok());
    s = pri_cache_res_->UpdateCacheReservation(
        (sec_capacity - old_sec_capacity) - (new_sec_reserved - sec_reserved_),
        /*increase=*/true);
    assert(s.ok());
    sec_reserved_ = new_sec_reserved;
    s = secondary_cache_->SetCapacity(sec_capacity);
    assert(s.ok());
  } else {
    // We're shrinking the ratio. Try to avoid unnecessary evictions -
    // 1. Lower the secondary cache capacity
    // 2. Decrease pri_cache_res_ reservation to reflect lower secondary
    //    cache utilization (decrease in capacity - decrease in share of cache
    //    reservations)
    // 3. Inflate the secondary cache to give it back the reduction in its
    //    share of cache reservations
    s = secondary_cache_->SetCapacity(sec_capacity);
    if (s.ok()) {
      s = pri_cache_res_->UpdateCacheReservation(
          (old_sec_capacity - sec_capacity) -
              (sec_reserved_ - new_sec_reserved),
          /*increase=*/false);
      assert(s.ok());
      s = secondary_cache_->Inflate(sec_reserved_ - new_sec_reserved);
      assert(s.ok());
      sec_reserved_ = new_sec_reserved;
    }
  }

  return s;
}

Status CacheWithSecondaryAdapter::UpdateAdmissionPolicy(
    TieredAdmissionPolicy adm_policy) {
  adm_policy_ = adm_policy;
  return Status::OK();
}

std::shared_ptr<Cache> NewTieredCache(const TieredCacheOptions& _opts) {
  if (!_opts.cache_opts) {
    return nullptr;
  }

  TieredCacheOptions opts = _opts;
  {
    bool valid_adm_policy = true;

    switch (_opts.adm_policy) {
      case TieredAdmissionPolicy::kAdmPolicyAuto:
        // Select an appropriate default policy
        if (opts.adm_policy == TieredAdmissionPolicy::kAdmPolicyAuto) {
          if (opts.nvm_sec_cache) {
            opts.adm_policy = TieredAdmissionPolicy::kAdmPolicyThreeQueue;
          } else {
            opts.adm_policy = TieredAdmissionPolicy::kAdmPolicyPlaceholder;
          }
        }
        break;
      case TieredAdmissionPolicy::kAdmPolicyPlaceholder:
      case TieredAdmissionPolicy::kAdmPolicyAllowCacheHits:
      case TieredAdmissionPolicy::kAdmPolicyAllowAll:
        if (opts.nvm_sec_cache) {
          valid_adm_policy = false;
        }
        break;
      case TieredAdmissionPolicy::kAdmPolicyThreeQueue:
        if (!opts.nvm_sec_cache) {
          valid_adm_policy = false;
        }
        break;
      case TieredAdmissionPolicy::kAdmPolicyMax:
      default:
        valid_adm_policy = false;
    }
    if (!valid_adm_policy) {
      return nullptr;
    }
  }

  std::shared_ptr<Cache> cache;
  if (opts.cache_type == PrimaryCacheType::kCacheTypeLRU) {
    LRUCacheOptions cache_opts =
        *(static_cast_with_check<LRUCacheOptions, ShardedCacheOptions>(
            opts.cache_opts));
    cache_opts.capacity = opts.total_capacity;
    cache_opts.secondary_cache = nullptr;
    cache = cache_opts.MakeSharedCache();
  } else if (opts.cache_type == PrimaryCacheType::kCacheTypeHCC) {
    HyperClockCacheOptions cache_opts =
        *(static_cast_with_check<HyperClockCacheOptions, ShardedCacheOptions>(
            opts.cache_opts));
    cache_opts.capacity = opts.total_capacity;
    cache_opts.secondary_cache = nullptr;
    cache = cache_opts.MakeSharedCache();
  } else {
    return nullptr;
  }
  std::shared_ptr<SecondaryCache> sec_cache;
  opts.comp_cache_opts.capacity = static_cast<size_t>(
      opts.total_capacity * opts.compressed_secondary_ratio);
  sec_cache = NewCompressedSecondaryCache(opts.comp_cache_opts);

  if (opts.nvm_sec_cache) {
    if (opts.adm_policy == TieredAdmissionPolicy::kAdmPolicyThreeQueue) {
      sec_cache = std::make_shared<TieredSecondaryCache>(
          sec_cache, opts.nvm_sec_cache,
          TieredAdmissionPolicy::kAdmPolicyThreeQueue);
    } else {
      return nullptr;
    }
  }

  return std::make_shared<CacheWithSecondaryAdapter>(
      cache, sec_cache, opts.adm_policy, /*distribute_cache_res=*/true);
}

Status UpdateTieredCache(const std::shared_ptr<Cache>& cache,
                         int64_t total_capacity,
                         double compressed_secondary_ratio,
                         TieredAdmissionPolicy adm_policy) {
  if (!cache || strcmp(cache->Name(), kTieredCacheName)) {
    return Status::InvalidArgument();
  }
  CacheWithSecondaryAdapter* tiered_cache =
      static_cast<CacheWithSecondaryAdapter*>(cache.get());

  Status s;
  if (total_capacity > 0) {
    tiered_cache->SetCapacity(total_capacity);
  }
  if (compressed_secondary_ratio >= 0.0 && compressed_secondary_ratio <= 1.0) {
    s = tiered_cache->UpdateCacheReservationRatio(compressed_secondary_ratio);
  }
  if (adm_policy < TieredAdmissionPolicy::kAdmPolicyMax) {
    s = tiered_cache->UpdateAdmissionPolicy(adm_policy);
  }
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
