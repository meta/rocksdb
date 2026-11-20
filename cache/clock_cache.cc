//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/clock_cache.h"

#include <algorithm>
#include <bitset>
#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <exception>
#include <functional>
#include <numeric>
#include <string>
#include <thread>
#include <type_traits>

#include "cache/cache_key.h"
#include "cache/secondary_cache_adapter.h"
#include "logging/logging.h"
#include "port/likely.h"
#include "rocksdb/env.h"
#include "util/autovector.h"
#include "util/hash.h"
#include "util/math.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

namespace {
using SlotMeta = ClockHandle::SlotMeta;
using AcquireCounter = SlotMeta::AcquireCounter;
using ReleaseCounter = SlotMeta::ReleaseCounter;

inline uint32_t GetInitialCountdown(Cache::Priority priority) {
  // Set initial clock data from priority
  // TODO: configuration parameters for priority handling and clock cycle
  // count?
  switch (priority) {
    case Cache::Priority::HIGH:
      return ClockHandle::kHighCountdown;
    case Cache::Priority::LOW:
      return ClockHandle::kLowCountdown;
    case Cache::Priority::BOTTOM:
      return ClockHandle::kBottomCountdown;
  }
  // Switch should have been exhaustive.
  assert(false);
  // For release build, fall back on something reasonable.
  return ClockHandle::kLowCountdown;
}

inline void MarkEmpty(ClockHandle& h) {
#ifndef NDEBUG
  // Mark slot as empty, with assertion
  auto old_meta = h.meta.Exchange({});
  assert(old_meta.IsUnderConstruction());
#else
  // Mark slot as empty
  h.meta.Store({});
#endif
}

inline void FreeDataMarkEmpty(ClockHandle& h, MemoryAllocator* allocator) {
  // NOTE: in theory there's more room for parallelism if we copy the handle
  // data and delay actions like this until after marking the entry as empty,
  // but performance tests only show a regression by copying the few words
  // of data.
  h.FreeData(allocator);

  MarkEmpty(h);
}

// Called to undo the effect of referencing an entry for internal purposes,
// so it should not be marked as having been used.
inline void Unref(const ClockHandle& h, uint32_t count = 1) {
  // Pretend we never took the reference
  // WART: there's a tiny chance we release last ref to invisible
  // entry here. If that happens, we let eviction take care of it.
  SlotMeta old_meta;
  h.meta.Apply(AcquireCounter::MinusTransformPromiseNoUnderflow(count),
               &old_meta);
  assert(old_meta.GetRefcount() != 0);
  (void)old_meta;
}

inline bool ClockUpdate(ClockHandle& h, BaseClockTable::EvictionData* data,
                        bool* purgeable = nullptr) {
  SlotMeta meta;
  if (purgeable) {
    assert(*purgeable == false);
    // In AutoHCC, our eviction process follows the chain structure, so we
    // should ensure that we see the latest state of each entry, at least for
    // assertion checking.
    meta = h.meta.Load();
  } else {
    // In FixedHCC, our eviction process is a simple iteration without regard
    // to probing order, displacements, etc., so it doesn't matter if we see
    // somewhat stale data.
    meta = h.meta.LoadRelaxed();
  }

  if (!meta.IsShareable()) {
    // Only clock update Shareable entries
    if (purgeable) {
      *purgeable = true;
      // AutoHCC only: make sure we only attempt to update non-empty slots
      assert(!meta.IsEmpty());
    }
    return false;
  }
  uint32_t acquire_count = meta.GetAcquireCounter();
  uint32_t release_count = meta.GetReleaseCounter();
  if (acquire_count != release_count) {
    // Only clock update entries with no outstanding refs
    data->seen_pinned_count++;
    return false;
  }
  if (meta.IsVisible() && acquire_count > 0) {
    // Decrement clock
    uint32_t new_count =
        std::min(acquire_count - 1, uint32_t{ClockHandle::kMaxCountdown} - 1);
    // Compare-exchange in the decremented clock info, but
    // not aggressively
    SlotMeta new_meta = meta;
    new_meta.SetReleaseCounter(new_count);
    new_meta.SetAcquireCounter(new_count);
    h.meta.CasStrongRelaxed(meta, new_meta);
    return false;
  }
  // Otherwise, remove entry (either unreferenced invisible or
  // unreferenced and expired visible).
  SlotMeta construction_meta;
  construction_meta.SetUnderConstruction();
  construction_meta.SetHit(meta.GetHit());
  if (h.meta.CasStrong(meta, construction_meta)) {
    // Took ownership.
    data->freed_charge += h.GetTotalCharge();
    data->freed_count += 1;
    return true;
  } else {
    // Compare-exchange failing probably
    // indicates the entry was used, so skip it in that case.
    return false;
  }
}

// If an entry doesn't receive clock updates but is repeatedly referenced &
// released, the acquire and release counters could overflow without some
// intervention. This is that intervention, which should be inexpensive
// because it only incurs a simple, very predictable check. (Applying a bit
// mask in addition to an increment to every Release likely would be
// relatively expensive, because it's an extra atomic update.)
//
// We do have to assume that we never have many millions of simultaneous
// references to a cache handle, because we cannot represent so many
// references with the difference in counters, masked to the number of
// counter bits. Similarly, we assume there aren't millions of threads
// holding transient references (which might be "undone" rather than
// released by the way).
//
// Consider these possible states for each counter:
// low: less than kMaxCountdown
// medium: kMaxCountdown to half way to overflow + kMaxCountdown
// high: half way to overflow + kMaxCountdown, or greater
//
// And these possible states for the combination of counters:
// acquire / release
// -------   -------
// low       low       - Normal / common, with caveats (see below)
// medium    low       - Can happen while holding some refs
// high      low       - Violates assumptions (too many refs)
// low       medium    - Violates assumptions (refs underflow, etc.)
// medium    medium    - Normal (very read heavy cache)
// high      medium    - Can happen while holding some refs
// low       high      - This function is supposed to prevent
// medium    high      - Violates assumptions (refs underflow, etc.)
// high      high      - Needs CorrectNearOverflow
//
// Basically, this function detects (high, high) state (inferred from
// release alone being high) and bumps it back down to (medium, medium)
// state with the same refcount and the same logical countdown counter
// (everything > kMaxCountdown is logically the same). Note that bumping
// down to (low, low) would modify the countdown counter, so is "reserved"
// in a sense.
//
// If near-overflow correction is triggered here, there's no guarantee
// that another thread hasn't freed the entry and replaced it with another.
// Therefore, it must be the case that the correction does not affect
// entries unless they are very old (many millions of acquire-release cycles).
// (Our bit manipulation is indeed idempotent and only affects entries in
// exceptional cases.) We assume a pre-empted thread will not stall that long.
// If it did, the state could be corrupted in the (unlikely) case that the top
// bit of the acquire counter is set but not the release counter, and thus
// we only clear the top bit of the acquire counter on resumption. It would
// then appear that there are too many refs and the entry would be permanently
// pinned (which is not terrible for an exceptionally rare occurrence), unless
// it is referenced enough (at least kMaxCountdown more times) for the release
// counter to reach "high" state again and bumped back to "medium." (This
// motivates only checking for release counter in high state, not both in high
// state.)
inline void CorrectNearOverflow(SlotMeta old_meta,
                                BitFieldsAtomic<SlotMeta>& meta) {
  // We clear both top-most counter bits at the same time.
  constexpr uint32_t kCounterTopBit = uint32_t{1}
                                      << (SlotMeta::kCounterNumBits - 1);
  // The threshold for correcting "near overflow" is to ensure
  // (a) the value has a top bit set that can be cleared
  // (b) when we clear the top bit, the eviction state will be preserved
  //     (everything >= kMaxCountdown is treated equivalently)
  // As mentioned above, we only check the release count.
  constexpr uint32_t kThreshold = kCounterTopBit + ClockHandle::kMaxCountdown;

  if (UNLIKELY(old_meta.GetReleaseCounter() > kThreshold)) {
    auto clear_transform = AcquireCounter::AndTransform(kCounterTopBit - 1) +
                           ReleaseCounter::AndTransform(kCounterTopBit - 1);
    meta.ApplyRelaxed(clear_transform);
  }
}

inline bool BeginSlotInsert(const ClockHandleBasicData& proto, ClockHandle& h,
                            uint32_t initial_countdown, bool* already_matches) {
  assert(*already_matches == false);
  // Optimistically transition the slot from "empty" to
  // "under construction" (no effect on other states)
  auto set_occupied = SlotMeta::OccupiedFlag::SetTransform();
  SlotMeta old_meta;
  h.meta.Apply(set_occupied, &old_meta);

  if (old_meta.IsEmpty()) {
    // We've started inserting into an available slot, and taken
    // ownership.
    return true;
  } else if (!old_meta.IsVisible()) {
    // Slot not usable / touchable now
    return false;
  }
  // Existing, visible entry, which might be a match.
  // But first, we need to acquire a ref to read it. In fact, number of
  // refs for initial countdown, so that we boost the clock state if
  // this is a match.
  auto add_acquire =
      AcquireCounter::PlusTransformPromiseNoOverflow(initial_countdown);
  h.meta.Apply(add_acquire, &old_meta);
  // Like Lookup
  if (old_meta.IsVisible()) {
    // Acquired a read reference
    if (h.hashed_key == proto.hashed_key) {
      // Match. Release in a way that boosts the clock state
      auto add_release =
          ReleaseCounter::PlusTransformPromiseNoOverflow(initial_countdown);
      h.meta.Apply(add_release, &old_meta);
      // Correct for possible (but rare) overflow
      CorrectNearOverflow(old_meta, h.meta);
      // Insert detached instead (only if return handle needed)
      *already_matches = true;
      return false;
    } else {
      // Mismatch.
      Unref(h, initial_countdown);
    }
  } else if (UNLIKELY(old_meta.IsInvisible())) {
    // Pretend we never took the reference
    Unref(h, initial_countdown);
  } else {
    // For other states, incrementing the acquire counter has no effect
    // so we don't need to undo it.
    // Slot not usable / touchable now.
  }
  return false;
}

inline void FinishSlotInsert(const ClockHandleBasicData& proto, ClockHandle& h,
                             uint32_t initial_countdown, bool keep_ref) {
  // Save data fields
  ClockHandleBasicData* h_alias = &h;
  *h_alias = proto;

  // Transition from "under construction" state to "visible" state
  SlotMeta new_meta;
  new_meta.SetVisible();

  // Maybe with an outstanding reference
  new_meta.SetAcquireCounter(initial_countdown);
  new_meta.SetReleaseCounter(initial_countdown - (keep_ref ? 1 : 0));

#ifndef NDEBUG
  // Save the state transition, with assertion
  auto old_meta = h.meta.Exchange(new_meta);
  assert(old_meta.IsUnderConstruction());
#else
  // Save the state transition
  h.meta.Store(new_meta);
#endif
}

bool TryInsert(const ClockHandleBasicData& proto, ClockHandle& h,
               uint32_t initial_countdown, bool keep_ref,
               bool* already_matches) {
  bool b = BeginSlotInsert(proto, h, initial_countdown, already_matches);
  if (b) {
    FinishSlotInsert(proto, h, initial_countdown, keep_ref);
  }
  return b;
}

// Func must be const HandleImpl& -> void callable
template <class HandleImpl, class Func>
void ConstApplyToEntriesRange(const Func& func, const HandleImpl* begin,
                              const HandleImpl* end,
                              bool apply_if_will_be_deleted) {
  for (const HandleImpl* h = begin; h < end; ++h) {
    // Note: to avoid using compare_exchange, we have to be extra careful.
    SlotMeta old_meta = h->meta.LoadRelaxed();
    // Check if it's an entry visible to lookups
    if (apply_if_will_be_deleted || old_meta.IsVisible()) {
      if (old_meta.IsShareable()) {
        // Increment acquire counter. Note: it's possible that the entry has
        // completely changed since we loaded old_meta, but incrementing acquire
        // count is always safe. (Similar to optimistic Lookup here.)
        auto add_acquire = AcquireCounter::PlusTransformPromiseNoOverflow(1);
        h->meta.Apply(add_acquire, &old_meta);
        // Check whether we actually acquired a reference.
        if (old_meta.IsShareable()) {
          // Apply func if appropriate
          if (apply_if_will_be_deleted || old_meta.IsVisible()) {
            func(*h);
          }
          // Pretend we never took the reference
          Unref(*h);
          // No net change, so don't need to check for overflow
        } else {
          // For other states, incrementing the acquire counter has no effect
          // so we don't need to undo it. Furthermore, we cannot safely undo
          // it because we did not acquire a read reference to lock the
          // entry in a Shareable state.
        }
      }
    }
  }
}

uint32_t SanitizeEvictionEffortCap(int eviction_effort_cap) {
  eviction_effort_cap = std::max(int{1}, eviction_effort_cap);
  return static_cast<uint32_t>(eviction_effort_cap);
}

}  // namespace

void ClockHandleBasicData::FreeData(MemoryAllocator* allocator) const {
  if (helper->del_cb) {
    helper->del_cb(value, allocator);
  }
}

BaseClockTable::BaseClockTable(size_t capacity, bool strict_capacity_limit,
                               int eviction_effort_cap,
                               CacheMetadataChargePolicy metadata_charge_policy,
                               MemoryAllocator* allocator,
                               const Cache::EvictionCallback* eviction_callback,
                               const uint32_t* hash_seed)
    : capacity_(capacity),
      eec_and_scl_(EecAndScl{}
                       .With<EvictionEffortCap>(
                           SanitizeEvictionEffortCap(eviction_effort_cap))
                       .With<StrictCapacityLimit>(strict_capacity_limit)),
      metadata_charge_policy_(metadata_charge_policy),
      allocator_(allocator),
      eviction_callback_(*eviction_callback),
      hash_seed_(*hash_seed) {}

template <class HandleImpl>
HandleImpl* BaseClockTable::StandaloneInsert(
    const ClockHandleBasicData& proto) {
  // Heap allocated separate from table
  HandleImpl* h = new HandleImpl();
  ClockHandleBasicData* h_alias = h;
  *h_alias = proto;
  h->SetStandalone();
  // Single reference (standalone entries only created if returning a refed
  // Handle back to user)
  SlotMeta meta;
  meta.SetInvisible();
  meta.SetAcquireCounter(1);
  h->meta.Store(meta);
  // Keep track of how much of usage is standalone
  standalone_usage_.FetchAddRelaxed(proto.GetTotalCharge());
  return h;
}

template <class Table>
typename Table::HandleImpl* BaseClockTable::CreateStandalone(
    ClockHandleBasicData& proto, bool allow_uncharged) {
  Table& derived = static_cast<Table&>(*this);
  typename Table::InsertState state;
  derived.StartInsert(state);

  const size_t total_charge = proto.GetTotalCharge();
  // NOTE: we can use eec_and_scl as eviction_effort_cap below because
  // strict_capacity_limit=true is supposed to disable the limit on eviction
  // effort, and a large value effectively does that.
  if (eec_and_scl_.LoadRelaxed().Get<StrictCapacityLimit>()) {
    Status s = ChargeUsageMaybeEvictStrict<Table>(
        total_charge,
        /*need_evict_for_occupancy=*/false, state);
    if (!s.ok()) {
      if (allow_uncharged) {
        proto.total_charge = 0;
      } else {
        return nullptr;
      }
    }
  } else {
    // Case strict_capacity_limit == false
    bool success = ChargeUsageMaybeEvictNonStrict<Table>(
        total_charge,
        /*need_evict_for_occupancy=*/false, state);
    if (!success) {
      // Force the issue
      usage_.FetchAddRelaxed(total_charge);
    }
  }

  return StandaloneInsert<typename Table::HandleImpl>(proto);
}

template <class Table>
Status BaseClockTable::ChargeUsageMaybeEvictStrict(
    size_t total_charge, bool need_evict_for_occupancy,
    typename Table::InsertState& state) {
  const size_t capacity = capacity_.LoadRelaxed();
  if (total_charge > capacity) {
    return Status::MemoryLimit(
        "Cache entry too large for a single cache shard: " +
        std::to_string(total_charge) + " > " + std::to_string(capacity));
  }
  // Grab any available capacity, and free up any more required.
  size_t old_usage = usage_.LoadRelaxed();
  size_t new_usage;
  do {
    new_usage = std::min(capacity, old_usage + total_charge);
    if (new_usage == old_usage) {
      // No change needed
      break;
    }
  } while (!usage_.CasWeakRelaxed(old_usage, new_usage));
  // How much do we need to evict then?
  size_t need_evict_charge = old_usage + total_charge - new_usage;
  size_t request_evict_charge = need_evict_charge;
  if (UNLIKELY(need_evict_for_occupancy) && request_evict_charge == 0) {
    // Require at least 1 eviction.
    request_evict_charge = 1;
  }
  if (request_evict_charge > 0) {
    EvictionData data;
    static_cast<Table*>(this)->Evict(request_evict_charge, state, &data);
    occupancy_.FetchSub(data.freed_count);
    if (LIKELY(data.freed_charge > need_evict_charge)) {
      assert(data.freed_count > 0);
      // Evicted more than enough
      usage_.FetchSubRelaxed(data.freed_charge - need_evict_charge);
    } else if (data.freed_charge < need_evict_charge ||
               (UNLIKELY(need_evict_for_occupancy) && data.freed_count == 0)) {
      // Roll back to old usage minus evicted
      usage_.FetchSubRelaxed(data.freed_charge + (new_usage - old_usage));
      if (data.freed_charge < need_evict_charge) {
        return Status::MemoryLimit(
            "Insert failed because unable to evict entries to stay within "
            "capacity limit.");
      } else {
        return Status::MemoryLimit(
            "Insert failed because unable to evict entries to stay within "
            "table occupancy limit.");
      }
    }
    // If we needed to evict something and we are proceeding, we must have
    // evicted something.
    assert(data.freed_count > 0);
  }
  return Status::OK();
}

template <class Table>
inline bool BaseClockTable::ChargeUsageMaybeEvictNonStrict(
    size_t total_charge, bool need_evict_for_occupancy,
    typename Table::InsertState& state) {
  // For simplicity, we consider that either the cache can accept the insert
  // with no evictions, or we must evict enough to make (at least) enough
  // space. It could lead to unnecessary failures or excessive evictions in
  // some extreme cases, but allows a fast, simple protocol. If we allow a
  // race to get us over capacity, then we might never get back to capacity
  // limit if the sizes of entries allow each insertion to evict the minimum
  // charge. Thus, we should evict some extra if it's not a signifcant
  // portion of the shard capacity. This can have the side benefit of
  // involving fewer threads in eviction.
  const size_t old_usage = usage_.LoadRelaxed();
  const size_t capacity = capacity_.LoadRelaxed();
  size_t need_evict_charge;
  // NOTE: if total_charge > old_usage, there isn't yet enough to evict
  // `total_charge` amount. Even if we only try to evict `old_usage` amount,
  // there's likely something referenced and we would eat CPU looking for
  // enough to evict.
  if (old_usage + total_charge <= capacity || total_charge > old_usage) {
    // Good enough for me (might run over with a race)
    need_evict_charge = 0;
  } else {
    // Try to evict enough space, and maybe some extra
    need_evict_charge = total_charge;
    if (old_usage > capacity) {
      // Not too much to avoid thundering herd while avoiding strict
      // synchronization, such as the compare_exchange used with strict
      // capacity limit.
      need_evict_charge += std::min(capacity / 1024, total_charge) + 1;
    }
  }
  if (UNLIKELY(need_evict_for_occupancy) && need_evict_charge == 0) {
    // Special case: require at least 1 eviction if we only have to
    // deal with occupancy
    need_evict_charge = 1;
  }
  EvictionData data;
  if (need_evict_charge > 0) {
    static_cast<Table*>(this)->Evict(need_evict_charge, state, &data);
    // Deal with potential occupancy deficit
    if (UNLIKELY(need_evict_for_occupancy) && data.freed_count == 0) {
      assert(data.freed_charge == 0);
      // Can't meet occupancy requirement
      return false;
    } else {
      // Update occupancy for evictions
      occupancy_.FetchSub(data.freed_count);
    }
  }
  // Track new usage even if we weren't able to evict enough
  usage_.FetchAddRelaxed(total_charge - data.freed_charge);
  // No underflow
  assert(usage_.LoadRelaxed() < SIZE_MAX / 2);
  // Success
  return true;
}

void BaseClockTable::TrackAndReleaseEvictedEntry(ClockHandle* h) {
  bool took_value_ownership = false;
  if (eviction_callback_) {
    // For key reconstructed from hash
    UniqueId64x2 unhashed;
    took_value_ownership = eviction_callback_(
        ClockCacheShard<FixedHyperClockTable>::ReverseHash(
            h->GetHash(), &unhashed, hash_seed_),
        static_cast<Cache::Handle*>(h), h->meta.LoadRelaxed().GetHit());
  }
  if (!took_value_ownership) {
    h->FreeData(allocator_);
  }
  MarkEmpty(*h);
}

bool BaseClockTable::IsEvictionEffortExceeded(
    const BaseClockTable::EvictionData& data) const {
  auto eviction_effort_cap =
      eec_and_scl_.LoadRelaxed().GetEffectiveEvictionEffortCap();
  // Basically checks whether the ratio of useful effort to wasted effort is
  // too low, with a start-up allowance for wasted effort before any useful
  // effort.
  return (data.freed_count + 1U) * uint64_t{eviction_effort_cap} <=
         data.seen_pinned_count;
}

template <class Table>
Status BaseClockTable::Insert(const ClockHandleBasicData& proto,
                              typename Table::HandleImpl** handle,
                              Cache::Priority priority) {
  using HandleImpl = typename Table::HandleImpl;
  Table& derived = static_cast<Table&>(*this);

  typename Table::InsertState state;
  derived.StartInsert(state);

  // Do we have the available occupancy? Optimistically assume we do
  // and deal with it if we don't.
  size_t old_occupancy = occupancy_.FetchAdd(1);
  // Whether we over-committed and need an eviction to make up for it
  bool need_evict_for_occupancy =
      !derived.GrowIfNeeded(old_occupancy + 1, state);

  // Usage/capacity handling is somewhat different depending on
  // strict_capacity_limit, but mostly pessimistic.
  bool use_standalone_insert = false;
  const size_t total_charge = proto.GetTotalCharge();
  // NOTE: we can use eec_and_scl as eviction_effort_cap below because
  // strict_capacity_limit=true is supposed to disable the limit on eviction
  // effort, and a large value effectively does that.
  if (eec_and_scl_.LoadRelaxed().Get<StrictCapacityLimit>()) {
    Status s = ChargeUsageMaybeEvictStrict<Table>(
        total_charge, need_evict_for_occupancy, state);
    if (!s.ok()) {
      // Revert occupancy
      occupancy_.FetchSubRelaxed(1);
      return s;
    }
  } else {
    // Case strict_capacity_limit == false
    bool success = ChargeUsageMaybeEvictNonStrict<Table>(
        total_charge, need_evict_for_occupancy, state);
    if (!success) {
      // Revert occupancy
      occupancy_.FetchSubRelaxed(1);
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry
        // inserted into cache and evicted immediately.
        proto.FreeData(allocator_);
        return Status::OK();
      } else {
        // Need to track usage of fallback standalone insert
        usage_.FetchAddRelaxed(total_charge);
        use_standalone_insert = true;
      }
    }
  }

  if (!use_standalone_insert) {
    // Attempt a table insert, but abort if we find an existing entry for the
    // key. If we were to overwrite old entries, we would either
    // * Have to gain ownership over an existing entry to overwrite it, which
    // would only work if there are no outstanding (read) references and would
    // create a small gap in availability of the entry (old or new) to lookups.
    // * Have to insert into a suboptimal location (more probes) so that the
    // old entry can be kept around as well.

    uint32_t initial_countdown = GetInitialCountdown(priority);
    assert(initial_countdown > 0);

    HandleImpl* e =
        derived.DoInsert(proto, initial_countdown, handle != nullptr, state);

    if (e) {
      // Successfully inserted
      if (handle) {
        *handle = e;
      }
      return Status::OK();
    }
    // Not inserted
    // Revert occupancy
    occupancy_.FetchSubRelaxed(1);
    // Maybe fall back on standalone insert
    if (handle == nullptr) {
      // Revert usage
      usage_.FetchSubRelaxed(total_charge);
      // No underflow
      assert(usage_.LoadRelaxed() < SIZE_MAX / 2);
      // As if unrefed entry immdiately evicted
      proto.FreeData(allocator_);
      return Status::OK();
    }

    use_standalone_insert = true;
  }

  // Run standalone insert
  assert(use_standalone_insert);

  *handle = StandaloneInsert<HandleImpl>(proto);

  // The OkOverwritten status is used to count "redundant" insertions into
  // block cache. This implementation doesn't strictly check for redundant
  // insertions, but we instead are probably interested in how many insertions
  // didn't go into the table (instead "standalone"), which could be redundant
  // Insert or some other reason (use_standalone_insert reasons above).
  return Status::OkOverwritten();
}

void BaseClockTable::Ref(ClockHandle& h) {
  // Increment acquire counter
  SlotMeta old_meta;
  h.meta.Apply(AcquireCounter::PlusTransformPromiseNoOverflow(1), &old_meta);

  assert(old_meta.IsShareable());
  // Must have already had a reference
  assert(old_meta.GetRefcount() > 0);
  (void)old_meta;
}

#ifndef NDEBUG
void BaseClockTable::TEST_RefN(ClockHandle& h, uint32_t n) {
  // Increment acquire counter
  SlotMeta old_meta;
  h.meta.Apply(AcquireCounter::PlusTransformPromiseNoOverflow(n), &old_meta);

  assert(old_meta.IsShareable());
  (void)old_meta;
}

void BaseClockTable::TEST_ReleaseNMinus1(ClockHandle* h, uint32_t n) {
  assert(n > 0);

  // Like n-1 Releases, but assumes one more will happen in the caller to take
  // care of anything like erasing an unreferenced, invisible entry.
  SlotMeta old_meta;
  h->meta.Apply(ReleaseCounter::PlusTransformPromiseNoOverflow(n - 1),
                &old_meta);
  assert(old_meta.IsShareable());
  (void)old_meta;
}
#endif

FixedHyperClockTable::FixedHyperClockTable(
    size_t capacity, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy,
    MemoryAllocator* allocator,
    const Cache::EvictionCallback* eviction_callback, const uint32_t* hash_seed,
    const Opts& opts)
    : BaseClockTable(capacity, strict_capacity_limit, opts.eviction_effort_cap,
                     metadata_charge_policy, allocator, eviction_callback,
                     hash_seed),
      length_bits_(CalcHashBits(capacity, opts.estimated_value_size,
                                metadata_charge_policy)),
      length_bits_mask_((size_t{1} << length_bits_) - 1),
      occupancy_limit_(static_cast<size_t>((uint64_t{1} << length_bits_) *
                                           kStrictLoadFactor)),
      array_(new HandleImpl[size_t{1} << length_bits_]) {
  if (metadata_charge_policy ==
      CacheMetadataChargePolicy::kFullChargeCacheMetadata) {
    usage_.FetchAddRelaxed(size_t{GetTableSize()} * sizeof(HandleImpl));
  }

  static_assert(sizeof(HandleImpl) == 64U,
                "Expecting size / alignment with common cache line size");
}

FixedHyperClockTable::~FixedHyperClockTable() {
  // Assumes there are no references or active operations on any slot/element
  // in the table.
  for (size_t i = 0; i < GetTableSize(); i++) {
    HandleImpl& h = array_[i];
    SlotMeta meta = h.meta.LoadRelaxed();
    if (meta.IsShareable()) {
      // NOTE: Reaching here invisible is rare but possible
      assert(meta.GetRefcount() == 0);
      h.FreeData(allocator_);
#ifndef NDEBUG
      Rollback(h.hashed_key, &h);
      ReclaimEntryUsage(h.GetTotalCharge());
#endif
    } else {
      // Should be no transient "under construction" states unless a thread
      // was killed or we are being destructed while another thread is still
      // operating on the structure
      assert(meta.IsEmpty());
    }
  }

#ifndef NDEBUG
  for (size_t i = 0; i < GetTableSize(); i++) {
    assert(array_[i].displacements.LoadRelaxed() == 0);
  }
#endif

  assert(usage_.LoadRelaxed() == 0 ||
         usage_.LoadRelaxed() == size_t{GetTableSize()} * sizeof(HandleImpl));
  assert(occupancy_.LoadRelaxed() == 0);
}

void FixedHyperClockTable::StartInsert(InsertState&) {}

bool FixedHyperClockTable::GrowIfNeeded(size_t new_occupancy, InsertState&) {
  return new_occupancy <= occupancy_limit_;
}

FixedHyperClockTable::HandleImpl* FixedHyperClockTable::DoInsert(
    const ClockHandleBasicData& proto, uint32_t initial_countdown,
    bool keep_ref, InsertState&) {
  bool already_matches = false;
  HandleImpl* e = FindSlot(
      proto.hashed_key,
      [&](HandleImpl* h) {
        return TryInsert(proto, *h, initial_countdown, keep_ref,
                         &already_matches);
      },
      [&](HandleImpl* h) {
        if (already_matches) {
          // Stop searching & roll back displacements
          Rollback(proto.hashed_key, h);
          return true;
        } else {
          // Keep going
          return false;
        }
      },
      [&](HandleImpl* h, bool is_last) {
        if (is_last) {
          // Search is ending. Roll back displacements
          Rollback(proto.hashed_key, h);
        } else {
          h->displacements.FetchAddRelaxed(1);
        }
      });
  if (already_matches) {
    // Insertion skipped
    return nullptr;
  }
  if (e != nullptr) {
    // Successfully inserted
    return e;
  }
  // Else, no available slot found. Occupancy check should generally prevent
  // this, except it's theoretically possible for other threads to evict and
  // replace entries in the right order to hit every slot when it is populated.
  // Assuming random hashing, the chance of that should be no higher than
  // pow(kStrictLoadFactor, n) for n slots. That should be infeasible for
  // roughly n >= 256, so if this assertion fails, that suggests something is
  // going wrong.
  assert(GetTableSize() < 256);
  return nullptr;
}

FixedHyperClockTable::HandleImpl* FixedHyperClockTable::Lookup(
    const UniqueId64x2& hashed_key) {
  HandleImpl* e = FindSlot(
      hashed_key,
      [&](HandleImpl* h) {
        SlotMeta old_meta;
        // Mostly branch-free version (similar performance)
        /*
        h->meta.Apply(AcquireCounter::PlusTransformPromiseNoOverflow(1),
                      &old_meta);
        bool shareable = old_meta.IsShareable();
        bool visible = old_meta.IsVisible();
        bool match = (h->hashed_key == hashed_key) & visible;
        h->meta.Apply(AcquireCounter::MinusTransformPromiseNoUnderflow(
            uint32_t{shareable} & uint32_t{!match}));
        h->meta.Apply(SlotMeta::HitFlag::Or(match));
        return match;
        */
        // Optimistic lookup should pay off when the table is relatively
        // sparse.
        constexpr bool kOptimisticLookup = true;
        if (!kOptimisticLookup) {
          old_meta = h->meta.Load();
          if (!old_meta.IsVisible()) {
            return false;
          }
        }
        // (Optimistically) increment acquire counter
        h->meta.Apply(AcquireCounter::PlusTransformPromiseNoOverflow(1),
                      &old_meta);
        // Check if it's an entry visible to lookups
        if (old_meta.IsVisible()) {
          // Acquired a read reference
          if (h->hashed_key == hashed_key) {
            // Match
            // Update the hit bit
            if (eviction_callback_) {
              h->meta.ApplyRelaxed(SlotMeta::HitFlag::SetTransform());
            }
            return true;
          } else {
            // Mismatch. Pretend we never took the reference
            Unref(*h);
          }
        } else if (UNLIKELY(old_meta.IsInvisible())) {
          // Pretend we never took the reference
          Unref(*h);
        } else {
          // For other states, incrementing the acquire counter has no effect
          // so we don't need to undo it. Furthermore, we cannot safely undo
          // it because we did not acquire a read reference to lock the
          // entry in a Shareable state.
        }
        return false;
      },
      [&](HandleImpl* h) { return h->displacements.LoadRelaxed() == 0; },
      [&](HandleImpl* /*h*/, bool /*is_last*/) {});

  return e;
}

bool FixedHyperClockTable::Release(HandleImpl* h, bool useful,
                                   bool erase_if_last_ref) {
  // In contrast with LRUCache's Release, this function won't delete the handle
  // when the cache is above capacity and the reference is the last one. Space
  // is only freed up by EvictFromClock (called by Insert when space is needed)
  // and Erase. We do this to avoid an extra atomic read of the variable usage_.

  SlotMeta old_meta;
  if (useful) {
    // Increment release counter to indicate was used
    auto add_release = ReleaseCounter::PlusTransformPromiseNoOverflow(1);
    h->meta.Apply(add_release, &old_meta);
  } else {
    // Decrement acquire counter to pretend it never happened
    auto sub_acquire = AcquireCounter::MinusTransformPromiseNoUnderflow(1);
    h->meta.Apply(sub_acquire, &old_meta);
  }

  assert(old_meta.IsShareable());
  // No underflow
  assert(old_meta.GetAcquireCounter() != old_meta.GetReleaseCounter());

  if (erase_if_last_ref || UNLIKELY(old_meta.IsInvisible())) {
    // FIXME: There's a chance here that another thread could replace this
    // entry and we end up erasing the wrong one.

    // Update for last Apply op
    if (useful) {
      old_meta.SetReleaseCounter(old_meta.GetReleaseCounter() + 1);
    } else {
      old_meta.SetAcquireCounter(old_meta.GetAcquireCounter() - 1);
    }
    // Take ownership if no refs
    SlotMeta construction_meta;
    construction_meta.SetUnderConstruction();
    do {
      if (old_meta.GetRefcount() != 0) {
        // Not last ref at some point in time during this Release call
        // Correct for possible (but rare) overflow
        CorrectNearOverflow(old_meta, h->meta);
        return false;
      }
      if (!old_meta.IsShareable()) {
        // Someone else took ownership
        return false;
      }
      // Note that there's a small chance that we release, another thread
      // replaces this entry with another, reaches zero refs, and then we end
      // up erasing that other entry. That's an acceptable risk / imprecision.
    } while (!h->meta.CasWeak(old_meta, construction_meta));
    // Took ownership
    size_t total_charge = h->GetTotalCharge();
    if (UNLIKELY(h->IsStandalone())) {
      h->FreeData(allocator_);
      // Delete standalone handle
      delete h;
      standalone_usage_.FetchSubRelaxed(total_charge);
      usage_.FetchSubRelaxed(total_charge);
    } else {
      Rollback(h->hashed_key, h);
      FreeDataMarkEmpty(*h, allocator_);
      ReclaimEntryUsage(total_charge);
    }
    return true;
  } else {
    // Correct for possible (but rare) overflow
    CorrectNearOverflow(old_meta, h->meta);
    return false;
  }
}

#ifndef NDEBUG
void FixedHyperClockTable::TEST_ReleaseN(HandleImpl* h, uint32_t n) {
  if (n > 0) {
    // Do n-1 simple releases first
    TEST_ReleaseNMinus1(h, n);

    // Then the last release might be more involved
    Release(h, /*useful*/ true, /*erase_if_last_ref*/ false);
  }
}
#endif

void FixedHyperClockTable::Erase(const UniqueId64x2& hashed_key) {
  (void)FindSlot(
      hashed_key,
      [&](HandleImpl* h) {
        // Could be multiple entries in rare cases. Erase them all.
        // Optimistically increment acquire counter
        auto add_acquire = AcquireCounter::PlusTransformPromiseNoOverflow(1);
        SlotMeta old_meta;
        SlotMeta meta;
        h->meta.Apply(add_acquire, &old_meta, &meta);
        // Check if it's an entry visible to lookups
        if (meta.IsVisible()) {
          // Acquired a read reference
          if (h->hashed_key == hashed_key) {
            // Match. Take ownership if no other refs, or set invisible other
            // refs exist.
            for (;;) {
              uint32_t refcount = meta.GetRefcount();
              assert(refcount > 0);
              if (refcount > 1) {
                // Not last ref at some point in time during this Erase call
                // Set invisible
                h->meta.Apply(SlotMeta::VisibleFlag::ClearTransform());
                // And pretend we never took the reference
                Unref(*h);
                break;
              }
              SlotMeta construction_meta;
              construction_meta.SetUnderConstruction();
              if (h->meta.CasWeak(meta, construction_meta)) {
                // Took ownership
                assert(hashed_key == h->hashed_key);
                size_t total_charge = h->GetTotalCharge();
                FreeDataMarkEmpty(*h, allocator_);
                ReclaimEntryUsage(total_charge);
                // We already have a copy of hashed_key in this case, so OK to
                // delay Rollback until after releasing the entry
                Rollback(hashed_key, h);
                break;
              }
            }
          } else {
            // Mismatch. Pretend we never took the reference
            Unref(*h);
          }
        } else if (UNLIKELY(old_meta.IsInvisible())) {
          // Pretend we never took the reference
          Unref(*h);
        } else {
          // For other states, incrementing the acquire counter has no effect
          // so we don't need to undo it.
        }
        return false;
      },
      [&](HandleImpl* h) { return h->displacements.LoadRelaxed() == 0; },
      [&](HandleImpl* /*h*/, bool /*is_last*/) {});
}

void FixedHyperClockTable::EraseUnRefEntries() {
  for (size_t i = 0; i <= this->length_bits_mask_; i++) {
    HandleImpl& h = array_[i];

    SlotMeta old_meta = h.meta.LoadRelaxed();
    if (old_meta.IsShareable() && old_meta.GetRefcount() == 0) {
      SlotMeta construction_meta;
      construction_meta.SetUnderConstruction();
      if (h.meta.CasStrong(old_meta, construction_meta)) {
        // Took ownership
        size_t total_charge = h.GetTotalCharge();
        Rollback(h.hashed_key, &h);
        FreeDataMarkEmpty(h, allocator_);
        ReclaimEntryUsage(total_charge);
      }
    }
  }
}

template <typename MatchFn, typename AbortFn, typename UpdateFn>
inline FixedHyperClockTable::HandleImpl* FixedHyperClockTable::FindSlot(
    const UniqueId64x2& hashed_key, const MatchFn& match_fn,
    const AbortFn& abort_fn, const UpdateFn& update_fn) {
  // NOTE: upper 32 bits of hashed_key[0] is used for sharding
  //
  // We use double-hashing probing. Every probe in the sequence is a
  // pseudorandom integer, computed as a linear function of two random hashes,
  // which we call base and increment. Specifically, the i-th probe is base + i
  // * increment modulo the table size.
  size_t base = static_cast<size_t>(hashed_key[1]);
  // We use an odd increment, which is relatively prime with the power-of-two
  // table size. This implies that we cycle back to the first probe only
  // after probing every slot exactly once.
  // TODO: we could also reconsider linear probing, though locality benefits
  // are limited because each slot is a full cache line
  size_t increment = static_cast<size_t>(hashed_key[0]) | 1U;
  size_t first = ModTableSize(base);
  size_t current = first;
  bool is_last;
  do {
    HandleImpl* h = &array_[current];
    if (match_fn(h)) {
      return h;
    }
    if (abort_fn(h)) {
      return nullptr;
    }
    current = ModTableSize(current + increment);
    is_last = current == first;
    update_fn(h, is_last);
  } while (!is_last);
  // We looped back.
  return nullptr;
}

inline void FixedHyperClockTable::Rollback(const UniqueId64x2& hashed_key,
                                           const HandleImpl* h) {
  size_t current = ModTableSize(hashed_key[1]);
  size_t increment = static_cast<size_t>(hashed_key[0]) | 1U;
  while (&array_[current] != h) {
    array_[current].displacements.FetchSubRelaxed(1);
    current = ModTableSize(current + increment);
  }
}

inline void FixedHyperClockTable::ReclaimEntryUsage(size_t total_charge) {
  auto old_occupancy = occupancy_.FetchSub(1U);
  (void)old_occupancy;
  // No underflow
  assert(old_occupancy > 0);
  auto old_usage = usage_.FetchSubRelaxed(total_charge);
  (void)old_usage;
  // No underflow
  assert(old_usage >= total_charge);
}

inline void FixedHyperClockTable::Evict(size_t requested_charge, InsertState&,
                                        EvictionData* data) {
  // precondition
  assert(requested_charge > 0);

  // TODO: make a tuning parameter?
  constexpr size_t step_size = 4;

  // First (concurrent) increment clock pointer
  uint64_t old_clock_pointer = clock_pointer_.FetchAddRelaxed(step_size);

  // Cap the eviction effort at this thread (along with those operating in
  // parallel) circling through the whole structure kMaxCountdown times.
  // In other words, this eviction run must find something/anything that is
  // unreferenced at start of and during the eviction run that isn't reclaimed
  // by a concurrent eviction run.
  uint64_t max_clock_pointer =
      old_clock_pointer + (ClockHandle::kMaxCountdown << length_bits_);

  for (;;) {
    for (size_t i = 0; i < step_size; i++) {
      HandleImpl& h = array_[ModTableSize(Lower32of64(old_clock_pointer + i))];
      bool evicting = ClockUpdate(h, data);
      if (evicting) {
        Rollback(h.hashed_key, &h);
        TrackAndReleaseEvictedEntry(&h);
      }
    }

    // Loop exit condition
    if (data->freed_charge >= requested_charge) {
      return;
    }
    if (old_clock_pointer >= max_clock_pointer) {
      return;
    }
    if (IsEvictionEffortExceeded(*data)) {
      eviction_effort_exceeded_count_.FetchAddRelaxed(1);
      return;
    }

    // Advance clock pointer (concurrently)
    old_clock_pointer = clock_pointer_.FetchAddRelaxed(step_size);
  }
}

template <class Table>
ClockCacheShard<Table>::ClockCacheShard(
    size_t capacity, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy,
    MemoryAllocator* allocator,
    const Cache::EvictionCallback* eviction_callback, const uint32_t* hash_seed,
    const typename Table::Opts& opts)
    : CacheShardBase(metadata_charge_policy),
      table_(capacity, strict_capacity_limit, metadata_charge_policy, allocator,
             eviction_callback, hash_seed, opts) {
  // Initial charge metadata should not exceed capacity
  assert(table_.GetUsage() <= table_.GetCapacity() ||
         table_.GetCapacity() < sizeof(HandleImpl));
}

template <class Table>
void ClockCacheShard<Table>::EraseUnRefEntries() {
  table_.EraseUnRefEntries();
}

template <class Table>
void ClockCacheShard<Table>::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {
  // The state will be a simple index into the table. Even with a dynamic
  // hyper clock cache, entries will generally stay in their existing
  // slots, so we don't need to be aware of the high-level organization
  // that makes lookup efficient.
  size_t length = table_.GetTableSize();

  assert(average_entries_per_lock > 0);

  size_t index_begin = *state;
  size_t index_end = index_begin + average_entries_per_lock;
  if (index_end >= length) {
    // Going to end.
    index_end = length;
    *state = SIZE_MAX;
  } else {
    *state = index_end;
  }

  auto hash_seed = table_.GetHashSeed();
  ConstApplyToEntriesRange(
      [callback, hash_seed](const HandleImpl& h) {
        UniqueId64x2 unhashed;
        callback(ReverseHash(h.hashed_key, &unhashed, hash_seed), h.value,
                 h.GetTotalCharge(), h.helper);
      },
      table_.HandlePtr(index_begin), table_.HandlePtr(index_end), false);
}

int FixedHyperClockTable::CalcHashBits(
    size_t capacity, size_t estimated_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  double average_slot_charge = estimated_value_size * kLoadFactor;
  if (metadata_charge_policy == kFullChargeCacheMetadata) {
    average_slot_charge += sizeof(HandleImpl);
  }
  assert(average_slot_charge > 0.0);
  uint64_t num_slots =
      static_cast<uint64_t>(capacity / average_slot_charge + 0.999999);

  int hash_bits = FloorLog2((num_slots << 1) - 1);
  if (metadata_charge_policy == kFullChargeCacheMetadata) {
    // For very small estimated value sizes, it's possible to overshoot
    while (hash_bits > 0 &&
           uint64_t{sizeof(HandleImpl)} << hash_bits > capacity) {
      hash_bits--;
    }
  }
  return hash_bits;
}

template <class Table>
void ClockCacheShard<Table>::SetCapacity(size_t capacity) {
  table_.SetCapacity(capacity);
  // next Insert will take care of any necessary evictions
}

template <class Table>
void ClockCacheShard<Table>::SetStrictCapacityLimit(
    bool strict_capacity_limit) {
  table_.SetStrictCapacityLimit(strict_capacity_limit);
  // next Insert will take care of any necessary evictions
}

template <class Table>
Status ClockCacheShard<Table>::Insert(const Slice& key,
                                      const UniqueId64x2& hashed_key,
                                      Cache::ObjectPtr value,
                                      const Cache::CacheItemHelper* helper,
                                      size_t charge, HandleImpl** handle,
                                      Cache::Priority priority) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return Status::NotSupported("ClockCache only supports key size " +
                                std::to_string(kCacheKeySize) + "B");
  }
  ClockHandleBasicData proto;
  proto.hashed_key = hashed_key;
  proto.value = value;
  proto.helper = helper;
  proto.total_charge = charge;
  return table_.template Insert<Table>(proto, handle, priority);
}

template <class Table>
typename Table::HandleImpl* ClockCacheShard<Table>::CreateStandalone(
    const Slice& key, const UniqueId64x2& hashed_key, Cache::ObjectPtr obj,
    const Cache::CacheItemHelper* helper, size_t charge, bool allow_uncharged) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return nullptr;
  }
  ClockHandleBasicData proto;
  proto.hashed_key = hashed_key;
  proto.value = obj;
  proto.helper = helper;
  proto.total_charge = charge;
  return table_.template CreateStandalone<Table>(proto, allow_uncharged);
}

template <class Table>
typename ClockCacheShard<Table>::HandleImpl* ClockCacheShard<Table>::Lookup(
    const Slice& key, const UniqueId64x2& hashed_key) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return nullptr;
  }
  return table_.Lookup(hashed_key);
}

template <class Table>
bool ClockCacheShard<Table>::Ref(HandleImpl* h) {
  if (h == nullptr) {
    return false;
  }
  table_.Ref(*h);
  return true;
}

template <class Table>
bool ClockCacheShard<Table>::Release(HandleImpl* handle, bool useful,
                                     bool erase_if_last_ref) {
  if (handle == nullptr) {
    return false;
  }
  return table_.Release(handle, useful, erase_if_last_ref);
}

#ifndef NDEBUG
template <class Table>
void ClockCacheShard<Table>::TEST_RefN(HandleImpl* h, uint32_t n) {
  table_.TEST_RefN(*h, n);
}

template <class Table>
void ClockCacheShard<Table>::TEST_ReleaseN(HandleImpl* h, uint32_t n) {
  table_.TEST_ReleaseN(h, n);
}
#endif

template <class Table>
bool ClockCacheShard<Table>::Release(HandleImpl* handle,
                                     bool erase_if_last_ref) {
  return Release(handle, /*useful=*/true, erase_if_last_ref);
}

template <class Table>
void ClockCacheShard<Table>::Erase(const Slice& key,
                                   const UniqueId64x2& hashed_key) {
  if (UNLIKELY(key.size() != kCacheKeySize)) {
    return;
  }
  table_.Erase(hashed_key);
}

template <class Table>
size_t ClockCacheShard<Table>::GetUsage() const {
  return table_.GetUsage();
}

template <class Table>
size_t ClockCacheShard<Table>::GetStandaloneUsage() const {
  return table_.GetStandaloneUsage();
}

template <class Table>
size_t ClockCacheShard<Table>::GetCapacity() const {
  return table_.GetCapacity();
}

template <class Table>
size_t ClockCacheShard<Table>::GetPinnedUsage() const {
  // Computes the pinned usage by scanning the whole hash table. This
  // is slow, but avoids keeping an exact counter on the clock usage,
  // i.e., the number of not externally referenced elements.
  // Why avoid this counter? Because Lookup removes elements from the clock
  // list, so it would need to update the pinned usage every time,
  // which creates additional synchronization costs.
  size_t table_pinned_usage = 0;
  const bool charge_metadata =
      metadata_charge_policy_ == kFullChargeCacheMetadata;
  ConstApplyToEntriesRange(
      [&table_pinned_usage, charge_metadata](const HandleImpl& h) {
        SlotMeta meta = h.meta.LoadRelaxed();
        uint32_t refcount = meta.GetRefcount();
        // Holding one ref for ConstApplyToEntriesRange
        assert(refcount > 0);
        if (refcount > 1) {
          table_pinned_usage += h.GetTotalCharge();
          if (charge_metadata) {
            table_pinned_usage += sizeof(HandleImpl);
          }
        }
      },
      table_.HandlePtr(0), table_.HandlePtr(table_.GetTableSize()), true);

  return table_pinned_usage + table_.GetStandaloneUsage();
}

template <class Table>
size_t ClockCacheShard<Table>::GetOccupancyCount() const {
  return table_.GetOccupancy();
}

template <class Table>
size_t ClockCacheShard<Table>::GetOccupancyLimit() const {
  return table_.GetOccupancyLimit();
}

template <class Table>
size_t ClockCacheShard<Table>::GetTableAddressCount() const {
  return table_.GetTableSize();
}

// Explicit instantiation
template class ClockCacheShard<FixedHyperClockTable>;
template class ClockCacheShard<AutoHyperClockTable>;

template <class Table>
BaseHyperClockCache<Table>::BaseHyperClockCache(
    const HyperClockCacheOptions& opts)
    : ShardedCache<ClockCacheShard<Table>>(opts) {
  // TODO: should not need to go through two levels of pointer indirection to
  // get to table entries
  size_t per_shard = this->GetPerShardCapacity();
  MemoryAllocator* alloc = this->memory_allocator();
  this->InitShards([&](Shard* cs) {
    typename Table::Opts table_opts{opts};
    new (cs) Shard(per_shard, opts.strict_capacity_limit,
                   opts.metadata_charge_policy, alloc,
                   &this->eviction_callback_, &this->hash_seed_, table_opts);
  });
}

template <class Table>
Cache::ObjectPtr BaseHyperClockCache<Table>::Value(Handle* handle) {
  return static_cast<const typename Table::HandleImpl*>(handle)->value;
}

template <class Table>
size_t BaseHyperClockCache<Table>::GetCharge(Handle* handle) const {
  return static_cast<const typename Table::HandleImpl*>(handle)
      ->GetTotalCharge();
}

template <class Table>
const Cache::CacheItemHelper* BaseHyperClockCache<Table>::GetCacheItemHelper(
    Handle* handle) const {
  auto h = static_cast<const typename Table::HandleImpl*>(handle);
  return h->helper;
}

template <class Table>
void BaseHyperClockCache<Table>::ApplyToHandle(
    Cache* cache, Handle* handle,
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge, const CacheItemHelper* helper)>&
        callback) {
  BaseHyperClockCache<Table>* cache_ptr =
      static_cast<BaseHyperClockCache<Table>*>(cache);
  auto h = static_cast<const typename Table::HandleImpl*>(handle);
  UniqueId64x2 unhashed;
  auto hash_seed = cache_ptr->GetShard(h->GetHash()).GetTable().GetHashSeed();
  callback(
      ClockCacheShard<Table>::ReverseHash(h->hashed_key, &unhashed, hash_seed),
      h->value, h->GetTotalCharge(), h->helper);
}

namespace {

// For each cache shard, estimate what the table load factor would be if
// cache filled to capacity with average entries. This is considered
// indicative of a potential problem if the shard is essentially operating
// "at limit", which we define as high actual usage (>80% of capacity)
// or actual occupancy very close to limit (>95% of limit).
// Also, for each shard compute the recommended estimated_entry_charge,
// and keep the minimum one for use as overall recommendation.
void AddShardEvaluation(const FixedHyperClockCache::Shard& shard,
                        std::vector<double>& predicted_load_factors,
                        size_t& min_recommendation) {
  size_t usage = shard.GetUsage() - shard.GetStandaloneUsage();
  size_t capacity = shard.GetCapacity();
  double usage_ratio = 1.0 * usage / capacity;

  size_t occupancy = shard.GetOccupancyCount();
  size_t occ_limit = shard.GetOccupancyLimit();
  double occ_ratio = 1.0 * occupancy / occ_limit;
  if (usage == 0 || occupancy == 0 || (usage_ratio < 0.8 && occ_ratio < 0.95)) {
    // Skip as described above
    return;
  }

  // If filled to capacity, what would the occupancy ratio be?
  double ratio = occ_ratio / usage_ratio;
  // Given max load factor, what that load factor be?
  double lf = ratio * FixedHyperClockTable::kStrictLoadFactor;
  predicted_load_factors.push_back(lf);

  // Update min_recommendation also
  size_t recommendation = usage / occupancy;
  min_recommendation = std::min(min_recommendation, recommendation);
}

bool IsSlotOccupied(const ClockHandle& h) {
  return !h.meta.LoadRelaxed().IsEmpty();
}
}  // namespace

// NOTE: GCC might warn about subobject linkage if this is in anon namespace
template <size_t N = 500>
class LoadVarianceStats {
 public:
  std::string Report() const {
    return "Overall " + PercentStr(positive_count_, samples_) + " (" +
           std::to_string(positive_count_) + "/" + std::to_string(samples_) +
           "), Min/Max/Window = " + PercentStr(min_, N) + "/" +
           PercentStr(max_, N) + "/" + std::to_string(N) +
           ", MaxRun{Pos/Neg} = " + std::to_string(max_pos_run_) + "/" +
           std::to_string(max_neg_run_);
  }

  void Add(bool positive) {
    recent_[samples_ % N] = positive;
    if (positive) {
      ++positive_count_;
      ++cur_pos_run_;
      max_pos_run_ = std::max(max_pos_run_, cur_pos_run_);
      cur_neg_run_ = 0;
    } else {
      ++cur_neg_run_;
      max_neg_run_ = std::max(max_neg_run_, cur_neg_run_);
      cur_pos_run_ = 0;
    }
    ++samples_;
    if (samples_ >= N) {
      size_t count_set = recent_.count();
      max_ = std::max(max_, count_set);
      min_ = std::min(min_, count_set);
    }
  }

 private:
  size_t max_ = 0;
  size_t min_ = N;
  size_t positive_count_ = 0;
  size_t samples_ = 0;
  size_t max_pos_run_ = 0;
  size_t cur_pos_run_ = 0;
  size_t max_neg_run_ = 0;
  size_t cur_neg_run_ = 0;
  std::bitset<N> recent_;

  static std::string PercentStr(size_t a, size_t b) {
    if (b == 0) {
      return "??%";
    } else {
      return std::to_string(uint64_t{100} * a / b) + "%";
    }
  }
};

template <class Table>
void BaseHyperClockCache<Table>::ReportProblems(
    const std::shared_ptr<Logger>& info_log) const {
  if (info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    LoadVarianceStats slot_stats;
    uint64_t eviction_effort_exceeded_count = 0;
    this->ForEachShard([&](const BaseHyperClockCache<Table>::Shard* shard) {
      size_t count = shard->GetTableAddressCount();
      for (size_t i = 0; i < count; ++i) {
        slot_stats.Add(IsSlotOccupied(*shard->GetTable().HandlePtr(i)));
      }
      eviction_effort_exceeded_count +=
          shard->GetTable().GetEvictionEffortExceededCount();
    });
    ROCKS_LOG_AT_LEVEL(info_log, InfoLogLevel::DEBUG_LEVEL,
                       "Slot occupancy stats: %s", slot_stats.Report().c_str());
    ROCKS_LOG_AT_LEVEL(info_log, InfoLogLevel::DEBUG_LEVEL,
                       "Eviction effort exceeded: %" PRIu64,
                       eviction_effort_exceeded_count);
  }
}

void FixedHyperClockCache::ReportProblems(
    const std::shared_ptr<Logger>& info_log) const {
  BaseHyperClockCache::ReportProblems(info_log);

  uint32_t shard_count = GetNumShards();
  std::vector<double> predicted_load_factors;
  size_t min_recommendation = SIZE_MAX;
  ForEachShard([&](const FixedHyperClockCache::Shard* shard) {
    AddShardEvaluation(*shard, predicted_load_factors, min_recommendation);
  });

  if (predicted_load_factors.empty()) {
    // None operating "at limit" -> nothing to report
    return;
  }
  std::sort(predicted_load_factors.begin(), predicted_load_factors.end());

  // First, if the average load factor is within spec, we aren't going to
  // complain about a few shards being out of spec.
  // NOTE: this is only the average among cache shards operating "at limit,"
  // which should be representative of what we care about. It it normal, even
  // desirable, for a cache to operate "at limit" so this should not create
  // selection bias. See AddShardEvaluation().
  // TODO: Consider detecting cases where decreasing the number of shards
  // would be good, e.g. serious imbalance among shards.
  double average_load_factor =
      std::accumulate(predicted_load_factors.begin(),
                      predicted_load_factors.end(), 0.0) /
      shard_count;

  constexpr double kLowSpecLoadFactor = FixedHyperClockTable::kLoadFactor / 2;
  constexpr double kMidSpecLoadFactor =
      FixedHyperClockTable::kLoadFactor / 1.414;
  if (average_load_factor > FixedHyperClockTable::kLoadFactor) {
    // Out of spec => Consider reporting load factor too high
    // Estimate effective overall capacity loss due to enforcing occupancy limit
    double lost_portion = 0.0;
    int over_count = 0;
    for (double lf : predicted_load_factors) {
      if (lf > FixedHyperClockTable::kStrictLoadFactor) {
        ++over_count;
        lost_portion +=
            (lf - FixedHyperClockTable::kStrictLoadFactor) / lf / shard_count;
      }
    }
    // >= 20% loss -> error
    // >= 10% loss -> consistent warning
    // >= 1% loss -> intermittent warning
    InfoLogLevel level = InfoLogLevel::INFO_LEVEL;
    bool report = true;
    if (lost_portion > 0.2) {
      level = InfoLogLevel::ERROR_LEVEL;
    } else if (lost_portion > 0.1) {
      level = InfoLogLevel::WARN_LEVEL;
    } else if (lost_portion > 0.01) {
      int report_percent = static_cast<int>(lost_portion * 100.0);
      if (Random::GetTLSInstance()->PercentTrue(report_percent)) {
        level = InfoLogLevel::WARN_LEVEL;
      }
    } else {
      // don't report
      report = false;
    }
    if (report) {
      ROCKS_LOG_AT_LEVEL(
          info_log, level,
          "FixedHyperClockCache@%p unable to use estimated %.1f%% capacity "
          "because of full occupancy in %d/%u cache shards "
          "(estimated_entry_charge too high). "
          "Recommend estimated_entry_charge=%zu",
          this, lost_portion * 100.0, over_count, (unsigned)shard_count,
          min_recommendation);
    }
  } else if (average_load_factor < kLowSpecLoadFactor) {
    // Out of spec => Consider reporting load factor too low
    // But cautiously because low is not as big of a problem.

    // Only report if highest occupancy shard is also below
    // spec and only if average is substantially out of spec
    if (predicted_load_factors.back() < kLowSpecLoadFactor &&
        average_load_factor < kLowSpecLoadFactor / 1.414) {
      InfoLogLevel level = InfoLogLevel::INFO_LEVEL;
      if (average_load_factor < kLowSpecLoadFactor / 2) {
        level = InfoLogLevel::WARN_LEVEL;
      }
      ROCKS_LOG_AT_LEVEL(
          info_log, level,
          "FixedHyperClockCache@%p table has low occupancy at full capacity. "
          "Higher estimated_entry_charge (about %.1fx) would likely improve "
          "performance. Recommend estimated_entry_charge=%zu",
          this, kMidSpecLoadFactor / average_load_factor, min_recommendation);
    }
  }
}

// =======================================================================
//                             AutoHyperClockCache
// =======================================================================

// See AutoHyperClockTable::length_info_ etc. for how the linear hashing
// metadata is encoded. Here are some example values:
//
// Used length  | min shift  | threshold  | max shift
// 2            | 1          | 0          | 1
// 3            | 1          | 1          | 2
// 4            | 2          | 0          | 2
// 5            | 2          | 1          | 3
// 6            | 2          | 2          | 3
// 7            | 2          | 3          | 3
// 8            | 3          | 0          | 3
// 9            | 3          | 1          | 4
// ...
// Note:
// * min shift = floor(log2(used length))
// * max shift = ceil(log2(used length))
// * used length == (1 << shift) + threshold
// Also, shift=0 is never used in practice, so is reserved for "unset"

namespace {

inline int LengthInfoToMinShift(uint64_t length_info) {
  int mask_shift = BitwiseAnd(length_info, int{255});
  assert(mask_shift <= 63);
  assert(mask_shift > 0);
  return mask_shift;
}

inline size_t LengthInfoToThreshold(uint64_t length_info) {
  return static_cast<size_t>(length_info >> 8);
}

inline size_t LengthInfoToUsedLength(uint64_t length_info) {
  size_t threshold = LengthInfoToThreshold(length_info);
  int shift = LengthInfoToMinShift(length_info);
  assert(threshold < (size_t{1} << shift));
  size_t used_length = (size_t{1} << shift) + threshold;
  assert(used_length >= 2);
  return used_length;
}

inline uint64_t UsedLengthToLengthInfo(size_t used_length) {
  assert(used_length >= 2);
  int shift = FloorLog2(used_length);
  uint64_t threshold = BottomNBits(used_length, shift);
  uint64_t length_info =
      (uint64_t{threshold} << 8) + static_cast<uint64_t>(shift);
  assert(LengthInfoToUsedLength(length_info) == used_length);
  assert(LengthInfoToMinShift(length_info) == shift);
  assert(LengthInfoToThreshold(length_info) == threshold);
  return length_info;
}

// Avoid potential initialization order race with port::kPageSize
constexpr size_t kPresumedPageSize = 4096;

inline size_t GetStartingLength(size_t capacity) {
  if (capacity > kPresumedPageSize) {
    // Start with one memory page
    return kPresumedPageSize / sizeof(AutoHyperClockTable::HandleImpl);
  } else {
    // Mostly to make unit tests happy
    return 4;
  }
}

inline size_t GetHomeIndex(uint64_t hash, int shift) {
  return static_cast<size_t>(BottomNBits(hash, shift));
}

inline void GetHomeIndexAndShift(uint64_t length_info, uint64_t hash,
                                 size_t* home, int* shift) {
  int min_shift = LengthInfoToMinShift(length_info);
  size_t threshold = LengthInfoToThreshold(length_info);
  bool extra_shift = GetHomeIndex(hash, min_shift) < threshold;
  *home = GetHomeIndex(hash, min_shift + extra_shift);
  *shift = min_shift + extra_shift;
  assert(*home < LengthInfoToUsedLength(length_info));
}

// Helper function for Lookup
inline bool MatchAndRef(const UniqueId64x2* hashed_key, const ClockHandle& h,
                        int shift = 0, size_t home = 0,
                        bool* full_match_or_unknown = nullptr) {
  // Must be at least something to match
  assert(hashed_key || shift > 0);

  SlotMeta old_meta;
  SlotMeta new_meta;
  // (Optimistically) increment acquire counter.
  auto add_acquire = AcquireCounter::PlusTransformPromiseNoOverflow(1);
  h.meta.Apply(add_acquire, &old_meta, &new_meta);
  // Check if it's a referencable (sharable) entry
  if (!old_meta.IsShareable()) {
    // For non-sharable states, incrementing the acquire counter has no effect
    // so we don't need to undo it. Furthermore, we cannot safely undo
    // it because we did not acquire a read reference to lock the
    // entry in a Shareable state.
    if (full_match_or_unknown) {
      *full_match_or_unknown = true;
    }
    return false;
  }
  // Else acquired a read reference
  assert(new_meta.GetRefcount() > 0);
  if (hashed_key && h.hashed_key == *hashed_key &&
      LIKELY(old_meta.IsVisible())) {
    // Match on full key, visible
    if (full_match_or_unknown) {
      *full_match_or_unknown = true;
    }
    return true;
  } else if (shift > 0 && home == BottomNBits(h.hashed_key[1], shift)) {
    // NOTE: upper 32 bits of hashed_key[0] is used for sharding
    // Match on home address, possibly invisible
    if (full_match_or_unknown) {
      *full_match_or_unknown = false;
    }
    return true;
  } else {
    // Mismatch. Pretend we never took the reference
    Unref(h);
    if (full_match_or_unknown) {
      *full_match_or_unknown = false;
    }
    return false;
  }
}

using NextWithShift = AutoHyperClockTable::HandleImpl::NextWithShift;

// Assumes a chain rewrite lock prevents concurrent modification of
// these chain pointers
void UpgradeShiftsOnRange(AutoHyperClockTable::HandleImpl* arr,
                          size_t& frontier,
                          NextWithShift stop_before_or_new_tail, int old_shift,
                          int new_shift) {
  assert(frontier != SIZE_MAX);
  assert(new_shift == old_shift + 1);
  (void)old_shift;
  (void)new_shift;
  for (;;) {
    NextWithShift next_with_shift = arr[frontier].chain_next_with_shift.Load();
    assert(next_with_shift.GetShift() == old_shift);
    if (next_with_shift == stop_before_or_new_tail) {
      // Stopping at entry with pointer matching "stop before"
      assert(!next_with_shift.IsEnd());
      return;
    }
    if (next_with_shift.IsEnd()) {
      // Also update tail to new tail
      assert(stop_before_or_new_tail.IsEnd());
      arr[frontier].chain_next_with_shift.Store(stop_before_or_new_tail);
      // Mark nothing left to upgrade
      frontier = SIZE_MAX;
      return;
    }
    // Next is another entry to process, so upgrade and advance frontier
    arr[frontier].chain_next_with_shift.Apply(
        NextWithShift::Shift::PlusTransformPromiseNoOverflow(1U));
    assert(next_with_shift.GetShift() + 1 == new_shift);
    frontier = next_with_shift.GetNext();
  }
}

size_t CalcOccupancyLimit(size_t used_length) {
  return static_cast<size_t>(used_length * AutoHyperClockTable::kMaxLoadFactor +
                             0.999);
}

}  // namespace

// An RAII wrapper for locking a chain of entries (flag bit on the head)
// so that there is only one thread allowed to remove entries from the
// chain, or to rewrite it by splitting for Grow. Without the lock,
// all lookups and insertions at the head can proceed wait-free.
// The class also provides functions for safely manipulating the head pointer
// while holding the lock--or wanting to should it become non-empty.
//
// The flag bits on the head are such that the head cannot be locked if it
// is an empty chain, so that a "blind" FetchOr will try to lock a non-empty
// chain but have no effect on an empty chain. When a potential rewrite
// operation see an empty head pointer, there is no need to lock as the
// operation is a no-op. However, there are some cases such as CAS-update
// where locking might be required after initially not being needed, if the
// operation is forced to revisit the head pointer.
class AutoHyperClockTable::ChainRewriteLock {
 public:
  using HandleImpl = AutoHyperClockTable::HandleImpl;

  // Acquire lock if head of h is not an end
  explicit ChainRewriteLock(HandleImpl* h, RelaxedAtomic<uint64_t>& yield_count)
      : head_ptr_(&h->head_next_with_shift) {
    Acquire(yield_count);
  }

  // RAII wrap existing lock held (or end)
  explicit ChainRewriteLock(HandleImpl* h,
                            RelaxedAtomic<uint64_t>& /*yield_count*/,
                            NextWithShift already_locked_or_end)
      : head_ptr_(&h->head_next_with_shift) {
    saved_head_ = already_locked_or_end;
    // already locked or end
    assert(saved_head_.IsLocked());
  }

  // No copying allowed
  ChainRewriteLock(const ChainRewriteLock&) = delete;
  ChainRewriteLock& operator=(const ChainRewriteLock&) = delete;
  ChainRewriteLock(ChainRewriteLock&&) = delete;
  ChainRewriteLock& operator=(ChainRewriteLock&&) = delete;

  ~ChainRewriteLock() {
    if (!IsEnd()) {
      // Release lock
      NextWithShift old;
      head_ptr_->Apply(NextWithShift::LockedFlag::ClearTransform(), &old);
      assert(old.IsLockedNotEnd());
    }
  }

  void Reset(HandleImpl* h, RelaxedAtomic<uint64_t>& yield_count) {
    this->~ChainRewriteLock();
    new (this) ChainRewriteLock(h, yield_count);
  }

  // Expected current state, assuming no parallel updates.
  NextWithShift GetSavedHead() const { return saved_head_; }

  bool CasUpdate(NextWithShift next_with_shift,
                 RelaxedAtomic<uint64_t>& yield_count) {
    NextWithShift new_head =
        next_with_shift.With<NextWithShift::LockedFlag>(true);
    NextWithShift expected = GetSavedHead();
    bool success = head_ptr_->CasStrong(expected, new_head);
    if (success) {
      // Ensure IsEnd() is kept up-to-date, including for dtor
      saved_head_ = new_head;
    } else {
      // Parallel update to head, such as Insert()
      if (IsEnd()) {
        // Didn't previously hold a lock
        if (expected.IsEnd()) {
          // Still don't need to
          saved_head_ = expected;
        } else {
          // Need to acquire lock before proceeding
          Acquire(yield_count);
        }
      } else {
        // Parallel update must preserve our lock
        assert(expected.IsLockedNotEnd());
        saved_head_ = expected;
      }
    }
    return success;
  }

  bool IsEnd() const { return saved_head_.IsEnd(); }

 private:
  void Acquire(RelaxedAtomic<uint64_t>& yield_count) {
    for (;;) {
      // Acquire removal lock on the chain
      NextWithShift old_head;
      head_ptr_->Apply(NextWithShift::LockedFlag::SetTransform(), &old_head,
                       &saved_head_);
      if (!old_head.IsLockedNotEnd()) {
        // Either acquired the lock or lock not needed (end)
        assert(old_head.IsEnd() == old_head.IsLocked());
        break;
      }
      // NOTE: one of the few yield-wait loops, which is rare enough in practice
      // for its performance to be insignificant. (E.g. using C++20 atomic
      // wait/notify would likely be worse because of wasted notify costs.)
      yield_count.FetchAddRelaxed(1);
      std::this_thread::yield();
    }
  }

  BitFieldsAtomic<NextWithShift>* head_ptr_;
  NextWithShift saved_head_;
};

AutoHyperClockTable::AutoHyperClockTable(
    size_t capacity, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy,
    MemoryAllocator* allocator,
    const Cache::EvictionCallback* eviction_callback, const uint32_t* hash_seed,
    const Opts& opts)
    : BaseClockTable(capacity, strict_capacity_limit, opts.eviction_effort_cap,
                     metadata_charge_policy, allocator, eviction_callback,
                     hash_seed),
      array_(MemMapping::AllocateLazyZeroed(
          sizeof(HandleImpl) * CalcMaxUsableLength(capacity,
                                                   opts.min_avg_value_size,
                                                   metadata_charge_policy))),
      length_info_(UsedLengthToLengthInfo(GetStartingLength(capacity))),
      occupancy_limit_(
          CalcOccupancyLimit(LengthInfoToUsedLength(length_info_.Load()))),
      grow_frontier_(GetTableSize()),
      clock_pointer_mask_(
          BottomNBits(UINT64_MAX, LengthInfoToMinShift(length_info_.Load()))) {
  if (array_.Get() == nullptr) {
    fprintf(stderr,
            "Anonymous mmap for RocksDB HyperClockCache failed. Aborting.\n");
    std::terminate();
  }
  if (metadata_charge_policy ==
      CacheMetadataChargePolicy::kFullChargeCacheMetadata) {
    // NOTE: ignoring page boundaries for simplicity
    usage_.FetchAddRelaxed(size_t{GetTableSize()} * sizeof(HandleImpl));
  }

  static_assert(sizeof(HandleImpl) == 64U,
                "Expecting size / alignment with common cache line size");

  // Populate head pointers
  uint64_t length_info = length_info_.Load();
  int min_shift = LengthInfoToMinShift(length_info);
  int max_shift = min_shift + 1;
  size_t major = uint64_t{1} << min_shift;
  size_t used_length = GetTableSize();

  assert(major <= used_length);
  assert(used_length <= major * 2);

  // Initialize the initial usable set of slots. This slightly odd iteration
  // order makes it easier to get the correct shift amount on each head.
  for (size_t i = 0; i < major; ++i) {
#ifndef NDEBUG
    int shift;
    size_t home;
#endif
    if (major + i < used_length) {
      array_[i].head_next_with_shift.StoreRelaxed(
          NextWithShift::MakeEnd(i, max_shift));
      array_[major + i].head_next_with_shift.StoreRelaxed(
          NextWithShift::MakeEnd(major + i, max_shift));
#ifndef NDEBUG  // Extra invariant checking
      GetHomeIndexAndShift(length_info, i, &home, &shift);
      assert(home == i);
      assert(shift == max_shift);
      GetHomeIndexAndShift(length_info, major + i, &home, &shift);
      assert(home == major + i);
      assert(shift == max_shift);
#endif
    } else {
      array_[i].head_next_with_shift.StoreRelaxed(
          NextWithShift::MakeEnd(i, min_shift));
#ifndef NDEBUG  // Extra invariant checking
      GetHomeIndexAndShift(length_info, i, &home, &shift);
      assert(home == i);
      assert(shift == min_shift);
      GetHomeIndexAndShift(length_info, major + i, &home, &shift);
      assert(home == i);
      assert(shift == min_shift);
#endif
    }
  }
}

AutoHyperClockTable::~AutoHyperClockTable() {
  // As usual, destructor assumes there are no references or active operations
  // on any slot/element in the table.

  // It's possible that there were not enough Insert() after final concurrent
  // Grow to ensure length_info_ (published GetTableSize()) is fully up to
  // date. Probe for first unused slot to ensure we see the whole structure.
  size_t used_end = GetTableSize();
  while (used_end < array_.Count() &&
         array_[used_end].head_next_with_shift.LoadRelaxed() !=
             HandleImpl::kUnusedMarker) {
    used_end++;
  }
  // This check can be extra expensive for a cache that is just created,
  // maybe used for a small number of entries, as in a unit test, and then
  // destroyed. Only do this in rare modes. REVISED: Don't scan the whole mmap,
  // just a reasonable frontier past what we expect to have written.
#ifdef MUST_FREE_HEAP_ALLOCATIONS
  for (size_t i = used_end; i < array_.Count() && i < used_end + 64U; i++) {
    assert(array_[i].head_next_with_shift.LoadRelaxed() ==
           HandleImpl::kUnusedMarker);
    assert(array_[i].chain_next_with_shift.LoadRelaxed() ==
           HandleImpl::kUnusedMarker);
    assert(array_[i].meta.LoadRelaxed() == SlotMeta{});
  }
#endif          // MUST_FREE_HEAP_ALLOCATIONS
#ifndef NDEBUG  // Extra invariant checking
  std::vector<bool> was_populated(used_end);
  std::vector<bool> was_pointed_to(used_end);
#endif  // !NDEBUG
  for (size_t i = 0; i < used_end; i++) {
    HandleImpl& h = array_[i];
    SlotMeta meta = h.meta.LoadRelaxed();
    if (meta.IsShareable()) {
      // NOTE: Reaching here invisible is rare but possible
      assert(meta.GetRefcount() == 0);
      h.FreeData(allocator_);
#ifndef NDEBUG  // Extra invariant checking
      usage_.FetchSubRelaxed(h.total_charge);
      occupancy_.FetchSubRelaxed(1U);
      was_populated[i] = true;
      if (!h.chain_next_with_shift.LoadRelaxed().IsEnd()) {
        assert(!h.chain_next_with_shift.LoadRelaxed().IsLocked());
        size_t next = h.chain_next_with_shift.LoadRelaxed().GetNext();
        assert(!was_pointed_to[next]);
        was_pointed_to[next] = true;
      }
#endif  // !NDEBUG
    } else {
      // Should be no transient "under construction" states unless a thread
      // was killed or we are being destructed while another thread is still
      // operating on the structure
      assert(meta.IsEmpty());
    }
#ifndef NDEBUG  // Extra invariant checking
    if (!h.head_next_with_shift.LoadRelaxed().IsEnd()) {
      size_t next = h.head_next_with_shift.LoadRelaxed().GetNext();
      assert(!was_pointed_to[next]);
      was_pointed_to[next] = true;
    }
#endif  // !NDEBUG
  }
#ifndef NDEBUG  // Extra invariant checking
  // This check is not perfect, but should detect most reasonable cases
  // of abandonned or floating entries, etc.  (A floating cycle would not
  // be reported as bad.)
  for (size_t i = 0; i < used_end; i++) {
    if (was_populated[i]) {
      assert(was_pointed_to[i]);
    } else {
      assert(!was_pointed_to[i]);
    }
  }
#endif  // !NDEBUG

  // Metadata charging only follows the published table size
  assert(usage_.LoadRelaxed() == 0 ||
         usage_.LoadRelaxed() == GetTableSize() * sizeof(HandleImpl));
  assert(occupancy_.LoadRelaxed() == 0);
}

size_t AutoHyperClockTable::GetTableSize() const {
  return LengthInfoToUsedLength(length_info_.Load());
}

size_t AutoHyperClockTable::GetOccupancyLimit() const {
  return occupancy_limit_.LoadRelaxed();
}

void AutoHyperClockTable::StartInsert(InsertState& state) {
  state.saved_length_info = length_info_.Load();
}

// Because we have linked lists, bugs or even hardware errors can make it
// possible to create a cycle, which would lead to infinite loop.
// Furthermore, when we have retry cases in the code, we want to be sure
// these are not (and do not become) spin-wait loops. Given the assumption
// of quality hashing and the infeasibility of consistently recurring
// concurrent modifications to an entry or chain, we can safely bound the
// number of loop iterations in feasible operation, whether following chain
// pointers or retrying with some backtracking. A smaller limit is used for
// stress testing, to detect potential issues such as cycles or spin-waits,
// and a larger limit is used to break cycles should they occur in production.
#define CHECK_TOO_MANY_ITERATIONS(i) \
  {                                  \
    assert(i < 768);                 \
    if (UNLIKELY(i >= 4096)) {       \
      std::terminate();              \
    }                                \
  }

bool AutoHyperClockTable::GrowIfNeeded(size_t new_occupancy,
                                       InsertState& state) {
  // new_occupancy has taken into account other threads that are also trying
  // to insert, so as soon as we see sufficient *published* usable size, we
  // can declare success even if we aren't the one that grows the table.
  // However, there's an awkward state where other threads own growing the
  // table to sufficient usable size, but the udpated size is not yet
  // published. If we wait, then that likely slows the ramp-up cache
  // performance. If we unblock ourselves by ensuring we grow by at least one
  // slot, we could technically overshoot required size by number of parallel
  // threads accessing block cache. On balance considering typical cases and
  // the modest consequences of table being slightly too large, the latter
  // seems preferable.
  //
  // So if the published occupancy limit is too small, we unblock ourselves
  // by committing to growing the table by at least one slot. Also note that
  // we might need to grow more than once to actually increase the occupancy
  // limit (due to max load factor < 1.0)

  while (UNLIKELY(new_occupancy > occupancy_limit_.LoadRelaxed())) {
    // At this point we commit the thread to growing unless we've reached the
    // limit (returns false).
    if (!Grow(state)) {
      return false;
    }
  }
  // Success (didn't need to grow, or did successfully)
  return true;
}

bool AutoHyperClockTable::Grow(InsertState& state) {
  // Allocate the next grow slot
  size_t grow_home = grow_frontier_.FetchAddRelaxed(1);
  if (grow_home >= array_.Count()) {
    // Can't grow any more.
    // (Tested by unit test ClockCacheTest/Limits)
    // Make sure we don't overflow grow_frontier_ by reaching here repeatedly
    grow_frontier_.StoreRelaxed(array_.Count());
    return false;
  }
#ifdef COERCE_CONTEXT_SWITCH
  // This is useful in reproducing concurrency issues in Grow()
  while (Random::GetTLSInstance()->OneIn(2)) {
    std::this_thread::yield();
  }
#endif
  // Basically, to implement https://en.wikipedia.org/wiki/Linear_hashing
  // entries that belong in a new chain starting at grow_home will be
  // split off from the chain starting at old_home, which is computed here.
  int old_shift = FloorLog2(grow_home);
  size_t old_home = BottomNBits(grow_home, old_shift);
  assert(old_home + (size_t{1} << old_shift) == grow_home);

  // Wait here to ensure any Grow operations that would directly feed into
  // this one are finished, though the full waiting actually completes in
  // acquiring the rewrite lock for old_home in SplitForGrow. Here we ensure
  // the expected shift amount has been reached, and there we ensure the
  // chain rewrite lock has been released.
  size_t old_old_home = BottomNBits(grow_home, old_shift - 1);
  for (;;) {
    NextWithShift old_old_head =
        array_[old_old_home].head_next_with_shift.Load();
    if (old_old_head.GetShift() >= old_shift) {
      if (!old_old_head.IsLockedNotEnd()) {
        break;
      }
    }
    // NOTE: one of the few yield-wait loops, which is rare enough in practice
    // for its performance to be insignificant.
    yield_count_.FetchAddRelaxed(1);
    std::this_thread::yield();
  }

  // Do the dirty work of splitting the chain, including updating heads and
  // chain nexts for new shift amounts.
  SplitForGrow(grow_home, old_home, old_shift);

  // length_info_ can be updated any time after the new shift amount is
  // published to both heads, potentially before the end of SplitForGrow.
  // But we also can't update length_info_ until the previous Grow operation
  // (with grow_home := this grow_home - 1) has published the new shift amount
  // to both of its heads. However, we don't want to artificially wait here
  // on that Grow that is otherwise irrelevant.
  //
  // We could have each Grow operation advance length_info_ here as far as it
  // can without waiting, by checking for updated shift on the corresponding
  // old home and also stopping at an empty head value for possible grow_home.
  // However, this could increase CPU cache line sharing and in 1/64 cases
  // bring in an extra page from our mmap.
  //
  // Instead, part of the strategy is delegated to DoInsert():
  // * Here we try to bring length_info_ up to date with this grow_home as
  // much as we can without waiting. It will fall short if a previous Grow
  // is still between reserving the grow slot and making the first big step
  // to publish the new shift amount.
  // * To avoid length_info_ being perpetually out-of-date (for a small number
  // of heads) after our last Grow, we do the same when Insert has to "fall
  // forward" due to length_info_ being out-of-date.
  CatchUpLengthInfoNoWait(grow_home);

  // See usage in DoInsert()
  state.likely_empty_slot = grow_home;

  // Success
  return true;
}

// See call in Grow()
void AutoHyperClockTable::CatchUpLengthInfoNoWait(
    size_t known_usable_grow_home) {
  uint64_t current_length_info = length_info_.Load();
  size_t published_usable_size = LengthInfoToUsedLength(current_length_info);
  while (published_usable_size <= known_usable_grow_home) {
    // For when published_usable_size was grow_home
    size_t next_usable_size = published_usable_size + 1;
    uint64_t next_length_info = UsedLengthToLengthInfo(next_usable_size);

    // known_usable_grow_home is known to be ready for Lookup/Insert with
    // the new shift amount, but between that and published usable size, we
    // need to check.
    if (published_usable_size < known_usable_grow_home) {
      int old_shift = FloorLog2(next_usable_size - 1);
      size_t old_home = BottomNBits(published_usable_size, old_shift);
      int shift = array_[old_home].head_next_with_shift.Load().GetShift();
      if (shift <= old_shift) {
        // Not ready
        break;
      }
    }
    // CAS update length_info_. This only moves in one direction, so if CAS
    // fails, someone else made progress like we are trying, and we can just
    // pick up the new value and keep going as appropriate.
    if (length_info_.CasStrong(current_length_info, next_length_info)) {
      current_length_info = next_length_info;
      // Update usage_ if metadata charge policy calls for it
      if (metadata_charge_policy_ ==
          CacheMetadataChargePolicy::kFullChargeCacheMetadata) {
        // NOTE: ignoring page boundaries for simplicity
        usage_.FetchAddRelaxed(sizeof(HandleImpl));
      }
    }
    published_usable_size = LengthInfoToUsedLength(current_length_info);
  }

  // After updating lengh_info_ we can update occupancy_limit_,
  // allowing for later operations to update it before us.
  // Note: there is no AcqRelAtomic max operation, so we have to use a CAS loop
  size_t old_occupancy_limit = occupancy_limit_.LoadRelaxed();
  size_t new_occupancy_limit = CalcOccupancyLimit(published_usable_size);
  while (old_occupancy_limit < new_occupancy_limit) {
    if (occupancy_limit_.CasWeakRelaxed(old_occupancy_limit,
                                        new_occupancy_limit)) {
      break;
    }
  }
}

void AutoHyperClockTable::SplitForGrow(size_t grow_home, size_t old_home,
                                       int old_shift) {
  int new_shift = old_shift + 1;
  HandleImpl* const arr = array_.Get();

  // We implement a somewhat complicated splitting algorithm to ensure that
  // entries are always wait-free visible to Lookup, without Lookup needing
  // to double-check length_info_ to ensure every potentially relevant
  // existing entry is seen. This works step-by-step, carefully sharing
  // unmigrated parts of the chain between the source chain and the new
  // destination chain. This means that Lookup might see a partially migrated
  // chain so has to take that into consideration when checking that it hasn't
  // "jumped off" its intended chain (due to a parallel modification to an
  // "under (de)construction" entry that was found on the chain but has
  // been reassigned).
  //
  // We use a "rewrite lock" on the source and desination chains to exclude
  // removals from those, and we have a prior waiting step that ensures any Grow
  // operations feeding into this one have completed. But this process does have
  // to gracefully handle concurrent insertions to the head of the source chain,
  // and once marked ready, the destination chain.
  //
  // With those considerations, the migration starts with one "big step,"
  // potentially with retries to deal with insertions in parallel. Part of the
  // big step is to mark the two chain heads as updated with the new shift
  // amount, which redirects Lookups to the appropriate new chain.
  //
  // After that big step that updates the heads, the rewrite lock makes it
  // relatively easy to deal with the rest of the migration. Big
  // simplifications come from being able to read the hashed_key of each
  // entry on the chain without needing to hold a read reference, and
  // from never "jumping our to another chain." Concurrent insertions only
  // happen at the chain head, which is outside of what is left to migrate.
  //
  // A series of smaller steps finishes splitting apart the existing chain into
  // two distinct chains, followed by some steps to fully commit the result.
  //
  // Except for trivial cases in which all entries (or remaining entries)
  // on the input chain go to one output chain, there is an important invariant
  // after each step of migration, including after the initial "big step":
  // For each output chain, the "zero chain" (new hash bit is zero) and the
  // "one chain" (new hash bit is one) we have a "frontier" entry marking the
  // boundary between what has been migrated and what has not. One of the
  // frontiers is along the old chain after the other, and all entries between
  // them are for the same target chain as the earlier frontier. Thus, the
  // chains share linked list tails starting at the latter frontier. All
  // pointers from the new head locations to the frontier entries are marked
  // with the new shift amount, while all pointers after the frontiers use the
  // old shift amount.
  //
  // And after each step there is a strengthening step to reach a stronger
  // invariant: the frontier earlier in the original chain is advanced to be
  // immediately before the other frontier.
  //
  // Consider this original input chain,
  //
  // OldHome  -Old-> A0 -Old-> B0 -Old-> A1 -Old-> C0 -Old-> OldHome(End)
  // GrowHome (empty)
  //
  // == BIG STEP ==
  // The initial big step finds the first entry that will be on the each
  // output chain (in this case A0 and A1). We use brackets ([]) to mark them
  // as our prospective frontiers.
  //
  // OldHome  -Old-> [A0] -Old-> B0 -Old-> [A1] -Old-> C0 -Old-> OldHome(End)
  // GrowHome (empty)
  //
  // Next we speculatively update grow_home head to point to the first entry for
  // the one chain. This will not be used by Lookup until the head at old_home
  // uses the new shift amount.
  //
  // OldHome  -Old-> [A0] -Old-> B0 -Old-> [A1] -Old-> C0 -Old-> OldHome(End)
  // GrowHome --------------New------------/
  //
  // Observe that if Lookup were to use the new head at GrowHome, it would be
  // able to find all relevant entries. Finishing the initial big step
  // requires a CAS (compare_exchange) of the OldHome head because there
  // might have been parallel insertions there, in which case we roll back
  // and try again. (We might need to point GrowHome head differently.)
  //
  // OldHome  -New-> [A0] -Old-> B0 -Old-> [A1] -Old-> C0 -Old-> OldHome(End)
  // GrowHome --------------New------------/
  //
  // Upgrading the OldHome head pointer with the new shift amount, with a
  // compare_exchange, completes the initial big step, with [A0] as zero
  // chain frontier and [A1] as one chain frontier. Links before the frontiers
  // use the new shift amount and links after use the old shift amount.
  // == END BIG STEP==
  // == STRENGTHENING ==
  // Zero chain frontier is advanced to [B0] (immediately before other
  // frontier) by updating pointers with new shift amounts.
  //
  // OldHome  -New-> A0 -New-> [B0] -Old-> [A1] -Old-> C0 -Old-> OldHome(End)
  // GrowHome -------------New-----------/
  //
  // == END STRENGTHENING ==
  // == SMALL STEP #1 ==
  // From the strong invariant state, we need to find the next entry for
  // the new chain with the earlier frontier. In this case, we need to find
  // the next entry for the zero chain that comes after [B0], which in this
  // case is C0. This will be our next zero chain frontier, at least under
  // the weak invariant. To get there, we simply update the link between
  // the current two frontiers to skip over the entries irreleveant to the
  // ealier frontier chain. In this case, the zero chain skips over A1. As a
  // result, he other chain is now the "earlier."
  //
  // OldHome  -New-> A0 -New-> B0 -New-> [C0] -Old-> OldHome(End)
  // GrowHome -New-> [A1] ------Old-----/
  //
  // == END SMALL STEP #1 ==
  //
  // Repeating the cycle and end handling is not as interesting.

  // Acquire rewrite lock on zero chain (if it's non-empty)
  ChainRewriteLock zero_head_lock(&arr[old_home], yield_count_);

  // Used for locking the one chain below
  NextWithShift saved_one_head;
  // One head has not been written to
  assert(arr[grow_home].head_next_with_shift.Load() ==
         HandleImpl::kUnusedMarker);

  // old_home will also the head of the new "zero chain" -- all entries in the
  // "from" chain whose next hash bit is 0. grow_home will be head of the new
  // "one chain".

  // For these, SIZE_MAX is like nullptr (unknown)
  size_t zero_chain_frontier = SIZE_MAX;
  size_t one_chain_frontier = SIZE_MAX;
  size_t cur = SIZE_MAX;

  // Set to 0 (zero chain frontier earlier), 1 (one chain), or -1 (unknown)
  int chain_frontier_first = -1;

  // Might need to retry initial update of heads
  for (int i = 0;; ++i) {
    CHECK_TOO_MANY_ITERATIONS(i);
    assert(zero_chain_frontier == SIZE_MAX);
    assert(one_chain_frontier == SIZE_MAX);
    assert(cur == SIZE_MAX);
    assert(chain_frontier_first == -1);

    NextWithShift next_with_shift = zero_head_lock.GetSavedHead();

    // Find a single representative for each target chain, or scan the whole
    // chain if some target chain has no representative.
    for (;; ++i) {
      CHECK_TOO_MANY_ITERATIONS(i);

      // Loop invariants
      assert((chain_frontier_first < 0) == (zero_chain_frontier == SIZE_MAX &&
                                            one_chain_frontier == SIZE_MAX));
      assert((cur == SIZE_MAX) == (zero_chain_frontier == SIZE_MAX &&
                                   one_chain_frontier == SIZE_MAX));

      assert(next_with_shift.GetShift() == old_shift);

      // Check for end of original chain
      if (next_with_shift.IsEnd()) {
        cur = SIZE_MAX;
        break;
      }

      // next_with_shift is not End
      cur = next_with_shift.GetNext();

      if (BottomNBits(arr[cur].hashed_key[1], new_shift) == old_home) {
        // Entry for zero chain
        if (zero_chain_frontier == SIZE_MAX) {
          zero_chain_frontier = cur;
          if (one_chain_frontier != SIZE_MAX) {
            // Ready to update heads
            break;
          }
          // Nothing yet for one chain
          chain_frontier_first = 0;
        }
      } else {
        assert(BottomNBits(arr[cur].hashed_key[1], new_shift) == grow_home);
        // Entry for one chain
        if (one_chain_frontier == SIZE_MAX) {
          one_chain_frontier = cur;
          if (zero_chain_frontier != SIZE_MAX) {
            // Ready to update heads
            break;
          }
          // Nothing yet for zero chain
          chain_frontier_first = 1;
        }
      }

      next_with_shift = arr[cur].chain_next_with_shift.Load();
    }

    // Try to update heads for initial migration info
    // We only reached the end of the migrate-from chain already if one of the
    // target chains will be empty.
    assert((cur == SIZE_MAX) ==
           (zero_chain_frontier == SIZE_MAX || one_chain_frontier == SIZE_MAX));
    assert((chain_frontier_first < 0) ==
           (zero_chain_frontier == SIZE_MAX && one_chain_frontier == SIZE_MAX));

    // Always update one chain's head first (safe), and mark it as locked
    saved_one_head = one_chain_frontier != SIZE_MAX
                         ? NextWithShift::Make(one_chain_frontier, new_shift)
                         : NextWithShift::MakeEnd(grow_home, new_shift);
    saved_one_head.Set<NextWithShift::LockedFlag>(true);
    arr[grow_home].head_next_with_shift.Store(saved_one_head);

    // Make sure length_info_ hasn't been updated too early, as we're about
    // to make the change that makes it safe to update (e.g. in DoInsert())
    assert(LengthInfoToUsedLength(length_info_.Load()) <= grow_home);

    // Try to set zero's head.
    if (zero_head_lock.CasUpdate(
            zero_chain_frontier != SIZE_MAX
                ? NextWithShift::Make(zero_chain_frontier, new_shift)
                : NextWithShift::MakeEnd(old_home, new_shift),
            yield_count_)) {
      // Both heads successfully updated to new shift
      break;
    } else {
      // Concurrent insertion. This should not happen too many times.
      CHECK_TOO_MANY_ITERATIONS(i);
      // The easiest solution is to restart.
      zero_chain_frontier = SIZE_MAX;
      one_chain_frontier = SIZE_MAX;
      cur = SIZE_MAX;
      chain_frontier_first = -1;
      continue;
    }
  }

  // Create an RAII wrapper for the one chain rewrite lock we are already
  // holding (if was not end) and is now "published" after successful CAS on
  // zero chain head.
  ChainRewriteLock one_head_lock(&arr[grow_home], yield_count_, saved_one_head);

  // Except for trivial cases, we have something like
  // AHome -New-> [A0] -Old-> [B0] -Old-> [C0] \                        |
  // BHome --------------------New------------> [A1] -Old-> ...
  // And we need to upgrade as much as we can on the "first" chain
  // (the one eventually pointing to the other's frontier). This will
  // also finish off any case in which one of the target chains will be empty.
  if (chain_frontier_first >= 0) {
    size_t& first_frontier = chain_frontier_first == 0
                                 ? /*&*/ zero_chain_frontier
                                 : /*&*/ one_chain_frontier;
    size_t& other_frontier = chain_frontier_first != 0
                                 ? /*&*/ zero_chain_frontier
                                 : /*&*/ one_chain_frontier;
    NextWithShift stop_before_or_new_tail =
        other_frontier != SIZE_MAX
            ? /*stop before*/ NextWithShift::Make(other_frontier, old_shift)
            : /*new tail*/ NextWithShift::MakeEnd(
                  chain_frontier_first == 0 ? old_home : grow_home, new_shift);
    UpgradeShiftsOnRange(arr, first_frontier, stop_before_or_new_tail,
                         old_shift, new_shift);
  }

  if (zero_chain_frontier == SIZE_MAX) {
    // Already finished migrating
    assert(one_chain_frontier == SIZE_MAX);
    assert(cur == SIZE_MAX);
  } else {
    // Still need to migrate between two target chains
    for (int i = 0;; ++i) {
      CHECK_TOO_MANY_ITERATIONS(i);
      // Overall loop invariants
      assert(zero_chain_frontier != SIZE_MAX);
      assert(one_chain_frontier != SIZE_MAX);
      assert(cur != SIZE_MAX);
      assert(chain_frontier_first >= 0);
      size_t& first_frontier = chain_frontier_first == 0
                                   ? /*&*/ zero_chain_frontier
                                   : /*&*/ one_chain_frontier;
      size_t& other_frontier = chain_frontier_first != 0
                                   ? /*&*/ zero_chain_frontier
                                   : /*&*/ one_chain_frontier;
      assert(cur != first_frontier);
      assert(arr[first_frontier].chain_next_with_shift.Load().GetNext() ==
             other_frontier);

      NextWithShift next_with_shift = arr[cur].chain_next_with_shift.Load();

      // Check for end of original chain
      if (next_with_shift.IsEnd()) {
        // Can set upgraded tail on first chain
        NextWithShift first_new_tail = NextWithShift::MakeEnd(
            chain_frontier_first == 0 ? old_home : grow_home, new_shift);
        arr[first_frontier].chain_next_with_shift.Store(first_new_tail);
        // And upgrade remainder of other chain
        NextWithShift other_new_tail = NextWithShift::MakeEnd(
            chain_frontier_first != 0 ? old_home : grow_home, new_shift);
        UpgradeShiftsOnRange(arr, other_frontier, other_new_tail, old_shift,
                             new_shift);
        assert(other_frontier == SIZE_MAX);  // Finished
        break;
      }

      // next_with_shift is not End
      cur = next_with_shift.GetNext();

      int target_chain;
      if (BottomNBits(arr[cur].hashed_key[1], new_shift) == old_home) {
        // Entry for zero chain
        target_chain = 0;
      } else {
        assert(BottomNBits(arr[cur].hashed_key[1], new_shift) == grow_home);
        // Entry for one chain
        target_chain = 1;
      }
      if (target_chain == chain_frontier_first) {
        // Found next entry to skip to on the first chain
        NextWithShift skip_to = NextWithShift::Make(cur, new_shift);
        arr[first_frontier].chain_next_with_shift.Store(skip_to);
        first_frontier = cur;
        // Upgrade other chain up to entry before that one
        UpgradeShiftsOnRange(arr, other_frontier, next_with_shift, old_shift,
                             new_shift);
        // Swap which is marked as first
        chain_frontier_first = 1 - chain_frontier_first;
      } else {
        // Nothing to do yet, as we need to keep old generation pointers in
        // place for lookups
      }
    }
  }
}

// Variant of PurgeImplLocked: Removes all "under (de) construction" entries
// from a chain where already holding a rewrite lock
using PurgeLockedOpData = void;
// Variant of PurgeImplLocked: Clock-updates all entries in a chain, in
// addition to functionality of PurgeLocked, where already holding a rewrite
// lock. (Caller finalizes eviction on entries added to the autovector, in part
// so that we don't hold the rewrite lock while doing potentially expensive
// callback and allocator free.)
using ClockUpdateChainLockedOpData =
    autovector<AutoHyperClockTable::HandleImpl*>;

template <class OpData>
void AutoHyperClockTable::PurgeImplLocked(OpData* op_data,
                                          ChainRewriteLock& rewrite_lock,
                                          size_t home,
                                          BaseClockTable::EvictionData* data) {
  constexpr bool kIsPurge = std::is_same_v<OpData, PurgeLockedOpData>;
  constexpr bool kIsClockUpdateChain =
      std::is_same_v<OpData, ClockUpdateChainLockedOpData>;

  // Exactly one op specified
  static_assert(kIsPurge + kIsClockUpdateChain == 1);

  HandleImpl* const arr = array_.Get();

  NextWithShift next_with_shift = rewrite_lock.GetSavedHead();
  assert(!next_with_shift.IsEnd());
  int home_shift = next_with_shift.GetShift();
  (void)home;
  (void)home_shift;
  size_t next = next_with_shift.GetNext();
  assert(next < array_.Count());
  HandleImpl* h = &arr[next];
  HandleImpl* prev_to_keep = nullptr;
#ifndef NDEBUG
  NextWithShift prev_to_keep_next_with_shift{};
#endif
  // Whether there are entries between h and prev_to_keep that should be
  // purged from the chain.
  bool pending_purge = false;

  // Walk the chain, and stitch together any entries that are still
  // "shareable," possibly after clock update. prev_to_keep tells us where
  // the last "stitch back to" location is (nullptr => head).
  for (size_t i = 0;; ++i) {
    CHECK_TOO_MANY_ITERATIONS(i);

    bool purgeable = false;
    // In last iteration, h will be nullptr, to stitch together the tail of
    // the chain.
    if (h) {
      // NOTE: holding a rewrite lock on the chain prevents any "under
      // (de)construction" entries in the chain from being marked empty, which
      // allows us to access the hashed_keys without holding a read ref.
      assert(home == BottomNBits(h->hashed_key[1], home_shift));
      if constexpr (kIsClockUpdateChain) {
        // Clock update and/or check for purgeable (under (de)construction)
        if (ClockUpdate(*h, data, &purgeable)) {
          // Remember for finishing eviction
          op_data->push_back(h);
          // Entries for eviction become purgeable
          purgeable = true;
          assert(h->meta.Load().IsUnderConstruction());
        }
      } else {
        (void)op_data;
        (void)data;
        purgeable = !h->meta.Load().IsShareable();
      }
    }

    if (purgeable) {
      assert(h->meta.Load().IsUnderConstruction());
      pending_purge = true;
    } else if (pending_purge) {
      if (prev_to_keep) {
        // Update chain next to skip purgeable entries
        assert(prev_to_keep->chain_next_with_shift.Load() ==
               prev_to_keep_next_with_shift);
        prev_to_keep->chain_next_with_shift.Store(next_with_shift);
      } else if (rewrite_lock.CasUpdate(next_with_shift, yield_count_)) {
        // Managed to update head without any parallel insertions
      } else {
        // Parallel insertion must have interfered. Need to do a purge
        // from updated head to here. Since we have no prev_to_keep, there's
        // no risk of duplicate clock updates to entries. Any entries already
        // updated must have been evicted (purgeable) and it's OK to clock
        // update any new entries just inserted in parallel.
        // Can simply restart (GetSavedHead() already updated from CAS failure).
        next_with_shift = rewrite_lock.GetSavedHead();
        assert(!next_with_shift.IsEnd());
        next = next_with_shift.GetNext();
        assert(next < array_.Count());
        h = &arr[next];
        pending_purge = false;
        assert(prev_to_keep == nullptr);
        assert(next_with_shift.GetShift() == home_shift);
        continue;
      }
      pending_purge = false;
      prev_to_keep = h;
    } else {
      prev_to_keep = h;
    }

    if (h == nullptr) {
      // Reached end of the chain
      return;
    }

    // Read chain pointer
    next_with_shift = h->chain_next_with_shift.Load();
#ifndef NDEBUG
    if (prev_to_keep == h) {
      prev_to_keep_next_with_shift = next_with_shift;
    }
#endif

    assert(next_with_shift.GetShift() == home_shift);

    // Check for end marker
    if (next_with_shift.IsEnd()) {
      h = nullptr;
    } else {
      next = next_with_shift.GetNext();
      assert(next < array_.Count());
      h = &arr[next];
      assert(h != prev_to_keep);
    }
  }
}

// Variant of PurgeImpl: Removes all "under (de) construction" entries in a
// chain, such that any entry with the given key must have been purged.
using PurgeOpData = const UniqueId64x2;
// Variant of PurgeImpl: Clock-updates all entries in a chain, in addition to
// purging as appropriate. (Caller finalizes eviction on entries added to the
// autovector, in part so that we don't hold the rewrite lock while doing
// potentially expensive callback and allocator free.)
using ClockUpdateChainOpData = ClockUpdateChainLockedOpData;

template <class OpData>
void AutoHyperClockTable::PurgeImpl(OpData* op_data, size_t home,
                                    BaseClockTable::EvictionData* data) {
  // Early efforts to make AutoHCC fully wait-free ran into too many problems
  // that needed obscure and potentially inefficient work-arounds to have a
  // chance at working.
  //
  // The implementation settled on "essentially wait-free" which can be
  // achieved by locking at the level of each probing chain and only for
  // operations that might remove entries from the chain. Because parallel
  // clock updates and Grow operations are ordered, contention is very rare.
  // However, parallel insertions at any chain head have to be accommodated
  // to keep them wait-free.
  //
  // This function implements Purge and ClockUpdateChain functions (see above
  // OpData type definitions) as part of higher-level operations. This function
  // ensures the correct chain is (eventually) covered and handles rewrite
  // locking the chain. PurgeImplLocked has lower level details.
  //
  // In general, these operations and Grow are kept simpler by allowing eager
  // purging of under (de-)construction entries. For example, an Erase
  // operation might find that another thread has purged the entry from the
  // chain by the time its own purge operation acquires the rewrite lock and
  // proceeds. This is OK, and potentially reduces the number of lock/unlock
  // cycles because empty chains are not rewrite-lockable.

  constexpr bool kIsPurge = std::is_same_v<OpData, PurgeOpData>;
  constexpr bool kIsClockUpdateChain =
      std::is_same_v<OpData, ClockUpdateChainOpData>;

  // Exactly one op specified
  static_assert(kIsPurge + kIsClockUpdateChain == 1);

  int home_shift = 0;
  if constexpr (kIsPurge) {
    // Purge callers leave home unspecified, to be determined from key
    assert(home == SIZE_MAX);
    GetHomeIndexAndShift(length_info_.Load(), (*op_data)[1], &home,
                         &home_shift);
    assert(home_shift > 0);
  } else {
    assert(kIsClockUpdateChain);
    // Evict callers must specify home
    assert(home < SIZE_MAX);
  }

  HandleImpl* const arr = array_.Get();

  // Acquire the RAII rewrite lock (if not an empty chain)
  ChainRewriteLock rewrite_lock(&arr[home], yield_count_);

  if constexpr (kIsPurge) {
    // Ensure we are at the correct home for the shift in effect for the
    // chain head.
    for (;;) {
      int shift = rewrite_lock.GetSavedHead().GetShift();

      if (shift > home_shift) {
        // Found a newer shift at candidate head, which must apply to us.
        // Newer shift might not yet be reflected in length_info_ (an atomicity
        // gap in Grow), so operate as if it is. Note that other insertions
        // could happen using this shift before length_info_ is updated, and
        // it's possible (though unlikely) that multiple generations of Grow
        // have occurred. If shift is more than one generation ahead of
        // home_shift, it's possible that not all descendent homes have
        // reached the `shift` generation. Thus, we need to advance only one
        // shift at a time looking for a home+head with a matching shift
        // amount.
        home_shift++;
        home = GetHomeIndex((*op_data)[1], home_shift);
        rewrite_lock.Reset(&arr[home], yield_count_);
        continue;
      } else {
        assert(shift == home_shift);
      }
      break;
    }
  }

  // If the chain is empty, nothing to do
  if (!rewrite_lock.IsEnd()) {
    if constexpr (kIsPurge) {
      PurgeLockedOpData* locked_op_data{};
      PurgeImplLocked(locked_op_data, rewrite_lock, home, data);
    } else {
      PurgeImplLocked(op_data, rewrite_lock, home, data);
    }
  }
}

AutoHyperClockTable::HandleImpl* AutoHyperClockTable::DoInsert(
    const ClockHandleBasicData& proto, uint32_t initial_countdown,
    bool take_ref, InsertState& state) {
  size_t home;
  int orig_home_shift;
  GetHomeIndexAndShift(state.saved_length_info, proto.hashed_key[1], &home,
                       &orig_home_shift);
  HandleImpl* const arr = array_.Get();

  // We could go searching through the chain for any duplicate, but that's
  // not typically helpful, except for the REDUNDANT block cache stats.
  // (Inferior duplicates will age out with eviction.) However, we do skip
  // insertion if the home slot (or some other we happen to probe) already
  // has a match (already_matches below). This helps to keep better locality
  // when we can.
  //
  // And we can do that as part of searching for an available slot to
  // insert the new entry, because our preferred location and first slot
  // checked will be the home slot.
  //
  // As the table initially grows to size, few entries will be in the same
  // cache line as the chain head. However, churn in the cache relatively
  // quickly improves the proportion of entries sharing that cache line with
  // the chain head. Data:
  //
  // Initial population only: (cache_bench with -ops_per_thread=1)
  // Entries at home count: 29,202 (out of 129,170 entries in 94,411 chains)
  // Approximate average cache lines read to find an existing entry:
  //           129.2 / 94.4 [without the heads]
  // + (94.4 - 29.2) / 94.4 [the heads not included with entries]
  // = 2.06 cache lines
  //
  // After 10 million ops: (-threads=10 -ops_per_thread=100000)
  // Entries at home count: 67,556 (out of 129,359 entries in 94,756 chains)
  // That's a majority of entries and more than 2/3rds of chains.
  // Approximate average cache lines read to find an existing entry:
  // = 1.65 cache lines

  // Even if we aren't saving a ref to this entry (take_ref == false), we need
  // to keep a reference while we are inserting the entry into a chain, so that
  // it is not erased by another thread while trying to insert it on the chain.
  constexpr bool initial_take_ref = true;

  size_t used_length = LengthInfoToUsedLength(state.saved_length_info);
  assert(home < used_length);

  size_t idx = home;
  bool already_matches = false;
  bool already_matches_ignore = false;
  if (TryInsert(proto, arr[idx], initial_countdown, initial_take_ref,
                &already_matches)) {
    assert(idx == home);
  } else if (already_matches) {
    return nullptr;
    // Here we try to populate newly-opened slots in the table, but not
    // when we can add something to its home slot. This makes the structure
    // more performant more quickly on (initial) growth. We ignore "already
    // matches" in this case because it is unlikely and difficult to
    // incorporate logic for here cleanly and efficiently.
  } else if (UNLIKELY(state.likely_empty_slot > 0) &&
             TryInsert(proto, arr[state.likely_empty_slot], initial_countdown,
                       initial_take_ref, &already_matches_ignore)) {
    idx = state.likely_empty_slot;
  } else {
    // We need to search for an available slot outside of the home.
    // Linear hashing provides nice resizing but does typically mean
    // that some heads (home locations) have (in expectation) twice as
    // many entries mapped to them as other heads. For example if the
    // usable length is 80, then heads 16-63 are (in expectation) twice
    // as loaded as heads 0-15 and 64-79, which are using another hash bit.
    //
    // This means that if we just use linear probing (by a small constant)
    // to find an available slot, part of the structure could easily fill up
    // and resort to linear time operations even when the overall load factor
    // is only modestly high, like 70%. Even though each slot has its own CPU
    // cache line, there appears to be a small locality benefit (e.g. TLB and
    // paging) to iterating one by one, as long as we don't afoul of the
    // linear hashing imbalance.
    //
    // In a traditional non-concurrent structure, we could keep a "free list"
    // to ensure immediate access to an available slot, but maintaining such
    // a structure could require more cross-thread coordination to ensure
    // all entries are eventually available to all threads.
    //
    // The way we solve this problem is to use unit-increment linear probing
    // with a small bound, and then fall back on big jumps to have a good
    // chance of finding a slot in an under-populated region quickly if that
    // doesn't work.
    size_t i = 0;
    constexpr size_t kMaxLinearProbe = 4;
    for (; i < kMaxLinearProbe; i++) {
      idx++;
      if (idx >= used_length) {
        idx -= used_length;
      }
      if (TryInsert(proto, arr[idx], initial_countdown, initial_take_ref,
                    &already_matches)) {
        break;
      }
      if (already_matches) {
        return nullptr;
      }
    }
    if (i == kMaxLinearProbe) {
      // Keep searching, but change to a search method that should quickly
      // find any under-populated region. Switching to an increment based
      // on the golden ratio helps with that, but we also inject some minor
      // variation (less than 2%, 1 in 2^6) to avoid clustering effects on
      // this larger increment (if it were a fixed value in steady state
      // operation). Here we are primarily using upper bits of hashed_key[1]
      // while home is based on lowest bits.
      uint64_t incr_ratio = 0x9E3779B185EBCA87U + (proto.hashed_key[1] >> 6);
      size_t incr = FastRange64(incr_ratio, used_length);
      assert(incr > 0);
      size_t start = idx;
      for (;; i++) {
        idx += incr;
        if (idx >= used_length) {
          // Wrap around (faster than %)
          idx -= used_length;
        }
        if (idx == start) {
          // We have just completed a cycle that might not have covered all
          // slots. (incr and used_length could have common factors.)
          // Increment for the next cycle, which eventually ensures complete
          // iteration over the set of slots before repeating.
          idx++;
          if (idx >= used_length) {
            idx -= used_length;
          }
          start++;
          if (start >= used_length) {
            start -= used_length;
          }
          if (i >= used_length) {
            used_length = LengthInfoToUsedLength(length_info_.Load());
            if (i >= used_length * 2) {
              // Cycling back should not happen unless there is enough random
              // churn in parallel that we happen to hit each slot at a time
              // that it's occupied, which is really only feasible for small
              // structures, though with linear probing to find empty slots,
              // "small" here might be larger than for double hashing.
              assert(used_length <= 256);
              // Fall back on standalone insert in case something goes awry to
              // cause this
              return nullptr;
            }
          }
        }
        if (TryInsert(proto, arr[idx], initial_countdown, initial_take_ref,
                      &already_matches)) {
          break;
        }
        if (already_matches) {
          return nullptr;
        }
      }
    }
  }

  // Now insert into chain using head pointer
  NextWithShift next_with_shift;
  int home_shift = orig_home_shift;

  // Might need to retry
  for (int i = 0;; ++i) {
    CHECK_TOO_MANY_ITERATIONS(i);
    next_with_shift = arr[home].head_next_with_shift.Load();
    int shift = next_with_shift.GetShift();

    if (UNLIKELY(shift != home_shift)) {
      // NOTE: shift increases with table growth
      if (shift > home_shift) {
        // Must be grow in progress or completed since reading length_info.
        // Pull out one more hash bit. (See Lookup() for why we can't
        // safely jump to the shift that was read.)
        home_shift++;
        uint64_t hash_bit_mask = uint64_t{1} << (home_shift - 1);
        assert((home & hash_bit_mask) == 0);
        // BEGIN leftover updates to length_info_ for Grow()
        size_t grow_home = home + hash_bit_mask;
        assert(arr[grow_home].head_next_with_shift.Load() !=
               HandleImpl::kUnusedMarker);
        CatchUpLengthInfoNoWait(grow_home);
        // END leftover updates to length_info_ for Grow()
        home += proto.hashed_key[1] & hash_bit_mask;
        continue;
      } else {
        // Should not happen because length_info_ is only updated after both
        // old and new home heads are marked with new shift
        assert(false);
      }
    }

    // Values to update to
    NextWithShift head_next_with_shift = NextWithShift::Make(idx, home_shift);
    NextWithShift chain_next_with_shift = next_with_shift;

    // Preserve the locked state in head, without propagating to chain next
    // where it is meaningless (and not allowed)
    if (UNLIKELY(next_with_shift.IsLockedNotEnd())) {
      head_next_with_shift.Set<NextWithShift::LockedFlag>(true);
      chain_next_with_shift.Set<NextWithShift::LockedFlag>(false);
    }

    arr[idx].chain_next_with_shift.Store(chain_next_with_shift);
    if (arr[home].head_next_with_shift.CasWeak(next_with_shift,
                                               head_next_with_shift)) {
      // Success
      if (!take_ref) {
        Unref(arr[idx]);
      }
      return arr + idx;
    }
  }
}

AutoHyperClockTable::HandleImpl* AutoHyperClockTable::Lookup(
    const UniqueId64x2& hashed_key) {
  // Lookups are wait-free with low occurrence of retries, back-tracking,
  // and fallback. We do not have the benefit of holding a rewrite lock on
  // the chain so must be prepared for many kinds of mayhem, most notably
  // "falling off our chain" where a slot that Lookup has identified but
  // has not read-referenced is removed from one chain and inserted into
  // another. The full algorithm uses the following mitigation strategies to
  // ensure every relevant entry inserted before this Lookup, and not yet
  // evicted, is seen by Lookup, without excessive backtracking etc.:
  // * Keep a known good read ref in the chain for "island hopping." When
  // we observe that a concurrent write takes us off to another chain, we
  // only need to fall back to our last known good read ref (most recent
  // entry on the chain that is not "under construction," which is a transient
  // state). We don't want to compound the CPU toil of a long chain with
  // operations that might need to retry from scratch, with probability
  // in proportion to chain length.
  // * Only detect a chain is potentially incomplete because of a Grow in
  // progress by looking at shift in the next pointer tags (rather than
  // re-checking length_info_).
  // * SplitForGrow, Insert, and PurgeImplLocked ensure that there are no
  // transient states that might cause this full Lookup algorithm to skip over
  // live entries.

  // Reading length_info_ is not strictly required for Lookup, if we were
  // to increment shift sizes until we see a shift size match on the
  // relevant head pointer. Thus, reading with relaxed memory order gives
  // us a safe and almost always up-to-date jump into finding the correct
  // home and head.
  size_t home;
  int home_shift;
  GetHomeIndexAndShift(length_info_.LoadRelaxed(), hashed_key[1], &home,
                       &home_shift);
  assert(home_shift > 0);

  // The full Lookup algorithm however is not great for hot path efficiency,
  // because of the extra careful tracking described above. Overwhelmingly,
  // we can find what we're looking for with a naive linked list traversal
  // of the chain. Even if we "fall off our chain" to another, we don't
  // violate memory safety. We just won't match the key we're looking for.
  // And we would eventually reach an end state, possibly even experiencing a
  // cycle as an entry is freed and reused during our traversal (though at
  // any point in time the structure doesn't have cycles).
  //
  // So for hot path efficiency, we start with a naive Lookup attempt, and
  // then fall back on full Lookup if we don't find the correct entry. To
  // cap how much we invest into the naive Lookup, we simply cap the traversal
  // length before falling back. Also, when we do fall back on full Lookup,
  // we aren't paying much penalty by starting over. Much or most of the cost
  // of Lookup is memory latency in following the chain pointers, and the
  // naive Lookup has warmed the CPU cache for these entries, using as tight
  // of a loop as possible.

  HandleImpl* const arr = array_.Get();
  NextWithShift next_with_shift = arr[home].head_next_with_shift.LoadRelaxed();
  for (size_t i = 0; !next_with_shift.IsEnd() && i < 10; ++i) {
    HandleImpl* h = &arr[next_with_shift.GetNext()];
    // Attempt cheap key match without acquiring a read ref. This could give a
    // false positive, which is re-checked after acquiring read ref, or false
    // negative, which is re-checked in the full Lookup. Also, this is a
    // technical UB data race according to TSAN, but we don't need to read
    // a "correct" value here for correct overall behavior.
#ifdef __SANITIZE_THREAD__
    bool probably_equal = Random::GetTLSInstance()->OneIn(2);
#else
    bool probably_equal = h->hashed_key == hashed_key;
#endif
    if (probably_equal) {
      // Increment acquire counter for definitive check
      auto add_acquire = AcquireCounter::PlusTransformPromiseNoOverflow(1);
      SlotMeta old_meta;
      SlotMeta new_meta;
      h->meta.Apply(add_acquire, &old_meta, &new_meta);
      // Check if it's a referencable (sharable) entry
      if (LIKELY(old_meta.IsShareable())) {
        assert(new_meta.GetRefcount() > 0);
        if (LIKELY(h->hashed_key == hashed_key) &&
            LIKELY(old_meta.IsVisible())) {
          return h;
        } else {
          Unref(*h);
        }
      } else {
        // For non-sharable states, incrementing the acquire counter has no
        // effect so we don't need to undo it. Furthermore, we cannot safely
        // undo it because we did not acquire a read reference to lock the entry
        // in a Shareable state.
      }
    }

    next_with_shift = h->chain_next_with_shift.LoadRelaxed();
  }

  // If we get here, falling back on full Lookup algorithm.
  HandleImpl* h = nullptr;
  HandleImpl* read_ref_on_chain = nullptr;

  for (size_t i = 0;; ++i) {
    CHECK_TOO_MANY_ITERATIONS(i);
    // Read head or chain pointer
    next_with_shift = h ? h->chain_next_with_shift.Load()
                        : arr[home].head_next_with_shift.Load();
    int shift = next_with_shift.GetShift();

    // Make sure it's usable
    size_t effective_home = home;
    if (UNLIKELY(shift != home_shift)) {
      // We have potentially gone awry somehow, but it's possible we're just
      // hitting old data that is not yet completed Grow.
      // NOTE: shift bits goes up with table growth.
      if (shift < home_shift) {
        // To avoid waiting on Grow in progress, an old shift amount needs
        // to be processed as if we were still using it and (potentially
        // different or the same) the old home.
        // We can assert it's not too old, because each generation of Grow
        // waits on its ancestor in the previous generation.
        assert(shift + 1 == home_shift);
        effective_home = GetHomeIndex(home, shift);
      } else if (h == read_ref_on_chain) {
        assert(shift > home_shift);
        // At head or coming from an entry on our chain where we're holding
        // a read reference. Thus, we know the newer shift applies to us.
        // Newer shift might not yet be reflected in length_info_ (an atomicity
        // gap in Grow), so operate as if it is. Note that other insertions
        // could happen using this shift before length_info_ is updated, and
        // it's possible (though unlikely) that multiple generations of Grow
        // have occurred. If shift is more than one generation ahead of
        // home_shift, it's possible that not all descendent homes have
        // reached the `shift` generation. Thus, we need to advance only one
        // shift at a time looking for a home+head with a matching shift
        // amount.
        home_shift++;
        // Update home in case it has changed
        home = GetHomeIndex(hashed_key[1], home_shift);
        // This should be rare enough occurrence that it's simplest just
        // to restart (TODO: improve in some cases?)
        h = nullptr;
        if (read_ref_on_chain) {
          Unref(*read_ref_on_chain);
          read_ref_on_chain = nullptr;
        }
        // Didn't make progress & retry
        continue;
      } else {
        assert(shift > home_shift);
        assert(h != nullptr);
        // An "under (de)construction" entry has a new shift amount, which
        // means we have either gotten off our chain or our home shift is out
        // of date. If we revert back to saved ref, we will get updated info.
        h = read_ref_on_chain;
        // Didn't make progress & retry
        continue;
      }
    }

    // Check for end marker
    if (next_with_shift.IsEnd()) {
      // To ensure we didn't miss anything in the chain, the end marker must
      // point back to the correct home.
      if (LIKELY(next_with_shift.GetNext() == effective_home)) {
        // Complete, clean iteration of the chain, not found.
        // Clean up.
        if (read_ref_on_chain) {
          Unref(*read_ref_on_chain);
        }
        return nullptr;
      } else {
        // Something went awry. Revert back to a safe point (if we have it)
        h = read_ref_on_chain;
        // Didn't make progress & retry
        continue;
      }
    }

    // Follow the next and check for full key match, home match, or neither
    h = &arr[next_with_shift.GetNext()];
    bool full_match_or_unknown = false;
    if (MatchAndRef(&hashed_key, *h, shift, effective_home,
                    &full_match_or_unknown)) {
      // Got a read ref on next (h).
      //
      // There is a very small chance that between getting the next pointer
      // (now h) and doing MatchAndRef on it, another thread erased/evicted it
      // reinserted it into the same chain, causing us to cycle back in the
      // same chain and potentially see some entries again if we keep walking.
      // Newly-inserted entries are inserted before older ones, so we are at
      // least guaranteed not to miss anything. Here in Lookup, it's just a
      // transient, slight hiccup in performance.

      if (full_match_or_unknown) {
        // Full match.
        // Release old read ref on chain if applicable
        if (read_ref_on_chain) {
          // Pretend we never took the reference.
          Unref(*read_ref_on_chain);
        }
        // Update the hit bit
        if (eviction_callback_) {
          h->meta.ApplyRelaxed(SlotMeta::HitFlag::SetTransform());
        }
        // All done.
        return h;
      } else if (UNLIKELY(shift != home_shift) &&
                 home != BottomNBits(h->hashed_key[1], home_shift)) {
        // This chain is in a Grow operation and we've landed on an entry
        // that belongs to the wrong destination chain. We can keep going, but
        // there's a chance we'll need to backtrack back *before* this entry,
        // if the Grow finishes before this Lookup. We cannot save this entry
        // for backtracking because it might soon or already be on the wrong
        // chain.
        // NOTE: if we simply backtrack rather than continuing, we would
        // be in a wait loop (not allowed in Lookup!) until the other thread
        // finishes its Grow.
        Unref(*h);
      } else {
        // Correct home location, so we are on the right chain.
        // With new usable read ref, can release old one (if applicable).
        if (read_ref_on_chain) {
          // Pretend we never took the reference.
          Unref(*read_ref_on_chain);
        }
        // And keep the new one.
        read_ref_on_chain = h;
      }
    } else {
      if (full_match_or_unknown) {
        // Must have been an "under construction" entry. Can safely skip it,
        // but there's a chance we'll have to backtrack later
      } else {
        // Home mismatch! Revert back to a safe point (if we have it)
        h = read_ref_on_chain;
        // Didn't make progress & retry
      }
    }
  }
}

void AutoHyperClockTable::Remove(HandleImpl* h) {
  assert(h->meta.Load().IsUnderConstruction());

  const HandleImpl& c_h = *h;
  PurgeImpl(&c_h.hashed_key);
}

bool AutoHyperClockTable::TryEraseHandle(HandleImpl* h, bool holding_ref,
                                         bool mark_invisible) {
  SlotMeta meta = h->meta.Load();
  assert(!holding_ref || meta.IsShareable());

  // Take ownership if no other refs, or set invisible if other refs exist (and
  // mark_invisible is set).
  SlotMeta construction_meta;
  construction_meta.SetUnderConstruction();
  do {
    if (meta.GetRefcount() != uint32_t{holding_ref}) {
      // Not last ref at some point in time during this call
      if (mark_invisible) {
        // Set invisible
        h->meta.Apply(SlotMeta::VisibleFlag::ClearTransform());
      }
      return false;
    }
    if (!meta.IsShareable()) {
      // Someone else took ownership
      return false;
    }
    // Note that if !holding_ref, there's a small chance that we release,
    // another thread replaces this entry with another, reaches zero refs, and
    // then we end up erasing that other entry. That's an acceptable risk /
    // imprecision.
  } while (!h->meta.CasWeak(meta, construction_meta));
  // Took ownership
  // TODO? Delay freeing?
  h->FreeData(allocator_);
  size_t total_charge = h->total_charge;
  if (UNLIKELY(h->IsStandalone())) {
    // Delete detached handle
    delete h;
    standalone_usage_.FetchSubRelaxed(total_charge);
  } else {
    Remove(h);
    MarkEmpty(*h);
    occupancy_.FetchSub(1U);
  }
  usage_.FetchSubRelaxed(total_charge);
  assert(usage_.LoadRelaxed() < SIZE_MAX / 2);
  return true;
}

bool AutoHyperClockTable::Release(HandleImpl* h, bool useful,
                                  bool erase_if_last_ref) {
  // In contrast with LRUCache's Release, this function won't delete the handle
  // when the cache is above capacity and the reference is the last one. Space
  // is only freed up by Evict/PurgeImpl (called by Insert when space
  // is needed) and Erase. We do this to avoid an extra atomic read of the
  // variable usage_.

  SlotMeta old_meta;
  if (useful) {
    // Increment release counter to indicate was used
    auto add_release = ReleaseCounter::PlusTransformPromiseNoOverflow(1);
    h->meta.Apply(add_release, &old_meta);
    // Correct for possible (but rare) overflow
    CorrectNearOverflow(old_meta, h->meta);
  } else {
    // Decrement acquire counter to pretend it never happened
    auto sub_acquire = AcquireCounter::MinusTransformPromiseNoUnderflow(1);
    h->meta.Apply(sub_acquire, &old_meta);
  }

  assert(old_meta.IsShareable());
  // No underflow
  assert(old_meta.GetAcquireCounter() != old_meta.GetReleaseCounter());

  if ((erase_if_last_ref || UNLIKELY(old_meta.IsInvisible()))) {
    // FIXME: There's a chance here that another thread could replace this
    // entry and we end up erasing the wrong one.
    return TryEraseHandle(h, /*holding_ref=*/false, /*mark_invisible=*/false);
  } else {
    return false;
  }
}

#ifndef NDEBUG
void AutoHyperClockTable::TEST_ReleaseN(HandleImpl* h, uint32_t n) {
  if (n > 0) {
    // Do n-1 simple releases first
    TEST_ReleaseNMinus1(h, n);

    // Then the last release might be more involved
    Release(h, /*useful*/ true, /*erase_if_last_ref*/ false);
  }
}
#endif

void AutoHyperClockTable::Erase(const UniqueId64x2& hashed_key) {
  // Don't need to be efficient.
  // Might be one match masking another, so loop.
  while (HandleImpl* h = Lookup(hashed_key)) {
    bool gone =
        TryEraseHandle(h, /*holding_ref=*/true, /*mark_invisible=*/true);
    if (!gone) {
      // Only marked invisible, which is ok.
      // Pretend we never took the reference from Lookup.
      Unref(*h);
    }
  }
}

void AutoHyperClockTable::EraseUnRefEntries() {
  size_t usable_size = GetTableSize();
  for (size_t i = 0; i < usable_size; i++) {
    HandleImpl& h = array_[i];

    SlotMeta old_meta = h.meta.LoadRelaxed();
    if (old_meta.IsShareable() && old_meta.GetRefcount() == 0) {
      SlotMeta construction_meta;
      construction_meta.SetUnderConstruction();
      if (h.meta.CasStrong(old_meta, construction_meta)) {
        // Took ownership
        h.FreeData(allocator_);
        usage_.FetchSubRelaxed(h.total_charge);
        // NOTE: could be more efficient with a dedicated variant of
        // PurgeImpl, but this is not a common operation
        Remove(&h);
        MarkEmpty(h);
        occupancy_.FetchSub(1U);
      }
    }
  }
}

void AutoHyperClockTable::Evict(size_t requested_charge, InsertState& state,
                                EvictionData* data) {
  // precondition
  assert(requested_charge > 0);

  // We need the clock pointer to seemlessly "wrap around" at the end of the
  // table, and to be reasonably stable under Grow operations. This is
  // challenging when the linear hashing progressively opens additional
  // most-significant-hash-bits in determining home locations.

  // TODO: make a tuning parameter?
  // Up to 2x this number of homes will be evicted per step. In very rare
  // cases, possibly more, as homes of an out-of-date generation will be
  // resolved to multiple in a newer generation.
  constexpr size_t step_size = 4;

  // A clock_pointer_mask_ field separate from length_info_ enables us to use
  // the same mask (way of dividing up the space among evicting threads) for
  // iterating over the whole structure before considering changing the mask
  // at the beginning of each pass. This ensures we do not have a large portion
  // of the space that receives redundant or missed clock updates. However,
  // with two variables, for each update to clock_pointer_mask (< 64 ever in
  // the life of the cache), there will be a brief period where concurrent
  // eviction threads could use the old mask value, possibly causing redundant
  // or missed clock updates for a *small* portion of the table.
  size_t clock_pointer_mask = clock_pointer_mask_.LoadRelaxed();

  uint64_t max_clock_pointer = 0;  // unset

  // TODO: consider updating during a long eviction
  size_t used_length = LengthInfoToUsedLength(state.saved_length_info);

  autovector<HandleImpl*> to_finish_eviction;

  // Loop until enough freed, or limit reached (see bottom of loop)
  for (;;) {
    // First (concurrent) increment clock pointer
    uint64_t old_clock_pointer = clock_pointer_.FetchAddRelaxed(step_size);

    if (UNLIKELY((old_clock_pointer & clock_pointer_mask) == 0)) {
      // Back at the beginning. See if clock_pointer_mask should be updated.
      uint64_t mask = BottomNBits(
          UINT64_MAX, LengthInfoToMinShift(state.saved_length_info));
      if (clock_pointer_mask != mask) {
        clock_pointer_mask = static_cast<size_t>(mask);
        clock_pointer_mask_.StoreRelaxed(clock_pointer_mask);
      }
    }

    size_t major_step = clock_pointer_mask + 1;
    assert((major_step & clock_pointer_mask) == 0);

    for (size_t base_home = old_clock_pointer & clock_pointer_mask;
         base_home < used_length; base_home += major_step) {
      for (size_t i = 0; i < step_size; i++) {
        size_t home = base_home + i;
        if (home >= used_length) {
          break;
        }
        PurgeImpl(&to_finish_eviction, home, data);
      }
    }

    for (HandleImpl* h : to_finish_eviction) {
      TrackAndReleaseEvictedEntry(h);
      // NOTE: setting likely_empty_slot here can cause us to reduce the
      // portion of "at home" entries, probably because an evicted entry
      // is more likely to come back than a random new entry and would be
      // unable to go into its home slot.
    }
    to_finish_eviction.clear();

    // Loop exit conditions
    if (data->freed_charge >= requested_charge) {
      return;
    }

    if (max_clock_pointer == 0) {
      // Cap the eviction effort at this thread (along with those operating in
      // parallel) circling through the whole structure kMaxCountdown times.
      // In other words, this eviction run must find something/anything that is
      // unreferenced at start of and during the eviction run that isn't
      // reclaimed by a concurrent eviction run.
      // TODO: Does HyperClockCache need kMaxCountdown + 1?
      max_clock_pointer =
          old_clock_pointer +
          (uint64_t{ClockHandle::kMaxCountdown + 1} * major_step);
    }

    if (old_clock_pointer + step_size >= max_clock_pointer) {
      return;
    }

    if (IsEvictionEffortExceeded(*data)) {
      eviction_effort_exceeded_count_.FetchAddRelaxed(1);
      return;
    }
  }
}

size_t AutoHyperClockTable::CalcMaxUsableLength(
    size_t capacity, size_t min_avg_value_size,
    CacheMetadataChargePolicy metadata_charge_policy) {
  double min_avg_slot_charge = min_avg_value_size * kMaxLoadFactor;
  if (metadata_charge_policy == kFullChargeCacheMetadata) {
    min_avg_slot_charge += sizeof(HandleImpl);
  }
  assert(min_avg_slot_charge > 0.0);
  size_t num_slots =
      static_cast<size_t>(capacity / min_avg_slot_charge + 0.999999);

  const size_t slots_per_page = kPresumedPageSize / sizeof(HandleImpl);

  // Round up to page size
  return ((num_slots + slots_per_page - 1) / slots_per_page) * slots_per_page;
}

namespace {
bool IsHeadNonempty(const AutoHyperClockTable::HandleImpl& h) {
  return !h.head_next_with_shift.LoadRelaxed().IsEnd();
}
bool IsEntryAtHome(const AutoHyperClockTable::HandleImpl& h, int shift,
                   size_t home) {
  if (MatchAndRef(nullptr, h, shift, home)) {
    Unref(h);
    return true;
  } else {
    return false;
  }
}
}  // namespace

void AutoHyperClockCache::ReportProblems(
    const std::shared_ptr<Logger>& info_log) const {
  BaseHyperClockCache::ReportProblems(info_log);

  if (info_log->GetInfoLogLevel() <= InfoLogLevel::DEBUG_LEVEL) {
    LoadVarianceStats head_stats;
    size_t entry_at_home_count = 0;
    uint64_t yield_count = 0;
    this->ForEachShard([&](const Shard* shard) {
      size_t count = shard->GetTableAddressCount();
      uint64_t length_info = UsedLengthToLengthInfo(count);
      for (size_t i = 0; i < count; ++i) {
        const auto& h = *shard->GetTable().HandlePtr(i);
        head_stats.Add(IsHeadNonempty(h));
        int shift;
        size_t home;
        GetHomeIndexAndShift(length_info, i, &home, &shift);
        assert(home == i);
        entry_at_home_count += IsEntryAtHome(h, shift, home);
      }
      yield_count += shard->GetTable().GetYieldCount();
    });
    ROCKS_LOG_AT_LEVEL(info_log, InfoLogLevel::DEBUG_LEVEL,
                       "Head occupancy stats: %s", head_stats.Report().c_str());
    ROCKS_LOG_AT_LEVEL(info_log, InfoLogLevel::DEBUG_LEVEL,
                       "Entries at home count: %zu", entry_at_home_count);
    ROCKS_LOG_AT_LEVEL(info_log, InfoLogLevel::DEBUG_LEVEL,
                       "Yield count: %" PRIu64, yield_count);
  }
}

}  // namespace clock_cache

// DEPRECATED (see public API)
std::shared_ptr<Cache> NewClockCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    CacheMetadataChargePolicy metadata_charge_policy) {
  return NewLRUCache(capacity, num_shard_bits, strict_capacity_limit,
                     /* high_pri_pool_ratio */ 0.5, nullptr,
                     kDefaultToAdaptiveMutex, metadata_charge_policy,
                     /* low_pri_pool_ratio */ 0.0);
}

std::shared_ptr<Cache> HyperClockCacheOptions::MakeSharedCache() const {
  // For sanitized options
  HyperClockCacheOptions opts = *this;
  if (opts.num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  if (opts.num_shard_bits < 0) {
    // Use larger shard size to reduce risk of large entries clustering
    // or skewing individual shards.
    constexpr size_t min_shard_size = 32U * 1024U * 1024U;
    opts.num_shard_bits =
        GetDefaultCacheShardBits(opts.capacity, min_shard_size);
  }
  std::shared_ptr<Cache> cache;
  if (opts.estimated_entry_charge == 0) {
    cache = std::make_shared<clock_cache::AutoHyperClockCache>(opts);
  } else {
    cache = std::make_shared<clock_cache::FixedHyperClockCache>(opts);
  }
  if (opts.secondary_cache) {
    cache = std::make_shared<CacheWithSecondaryAdapter>(cache,
                                                        opts.secondary_cache);
  }
  return cache;
}

}  // namespace ROCKSDB_NAMESPACE
