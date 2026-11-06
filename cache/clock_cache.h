// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "cache/cache_key.h"
#include "cache/sharded_cache.h"
#include "port/mmap.h"
#include "rocksdb/cache.h"
#include "util/atomic.h"
#include "util/bit_fields.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

namespace clock_cache {

// Forward declaration of friend class.
template <class ClockCache>
class ClockCacheTest;

// HyperClockCache is an alternative to LRUCache specifically tailored for
// use as BlockBasedTableOptions::block_cache
//
// Benefits
// --------
// * Lock/wait free (no waits or spins) for efficiency under high concurrency
//   * Fixed version (estimated_entry_charge > 0) is fully lock/wait free
//   * Automatic version (estimated_entry_charge = 0) has rare waits among
//     certain insertion or erase operations that involve the same very small
//     set of entries.
// * Optimized for hot path reads. For concurrency control, most Lookup() and
// essentially all Release() are a single atomic add operation.
// * Eviction on insertion is fully parallel.
// * Uses a generalized + aging variant of CLOCK eviction that might outperform
// LRU in some cases. (For background, see
// https://en.wikipedia.org/wiki/Page_replacement_algorithm)
//
// Costs
// -----
// * FixedHyperClockCache (estimated_entry_charge > 0) - Hash table is not
// resizable (for lock-free efficiency) so capacity is not dynamically
// changeable. Rely on an estimated average value (block) size for
// space+time efficiency. (See estimated_entry_charge option details.)
// EXPERIMENTAL - This limitation is fixed in AutoHyperClockCache, activated
// with estimated_entry_charge == 0.
// * Insert usually does not (but might) overwrite a previous entry associated
// with a cache key. This is OK for RocksDB uses of Cache, though it does mess
// up our REDUNDANT block cache insertion statistics.
// * Only supports keys of exactly 16 bytes, which is what RocksDB uses for
// block cache (but not row cache or table cache).
// * Cache priorities are less aggressively enforced. Unlike LRUCache, enough
// transient LOW or BOTTOM priority items can evict HIGH priority entries that
// are not referenced recently (or often) enough.
// * If pinned entries leave little or nothing eligible for eviction,
// performance can degrade substantially, because of clock eviction eating
// CPU looking for evictable entries and because Release does not
// pro-actively delete unreferenced entries when the cache is over-full.
// Specifically, this makes this implementation more susceptible to the
// following combination:
//   * num_shard_bits is high (e.g. 6)
//   * capacity small (e.g. some MBs)
//   * some large individual entries (e.g. non-partitioned filters)
// where individual entries occupy a large portion of their shard capacity.
// This should be mostly mitigated by the implementation picking a lower
// number of cache shards than LRUCache for a given capacity (when
// num_shard_bits is not overridden; see calls to GetDefaultCacheShardBits()).
// * With strict_capacity_limit=false, respecting the capacity limit is not as
// aggressive as LRUCache. The limit might be transiently exceeded by a very
// small number of entries even when not strictly necessary, and slower to
// recover after pinning forces limit to be substantially exceeded. (Even with
// strict_capacity_limit=true, RocksDB will nevertheless transiently allocate
// memory before discovering it is over the block cache capacity, so this
// should not be a detectable regression in respecting memory limits, except
// on exceptionally small caches.)
// * In some cases, erased or duplicated entries might not be freed
// immediately. They will eventually be freed by eviction from further Inserts.
// * Internal metadata can overflow if the number of simultaneous references
// to a cache handle reaches many millions.
//
// High-level eviction algorithm
// -----------------------------
// A score (or "countdown") is maintained for each entry, initially determined
// by priority. The score is incremented on each Lookup, up to a max of 3,
// though is easily returned to previous state if useful=false with Release.
// During CLOCK-style eviction iteration, entries with score > 0 are
// decremented if currently unreferenced and entries with score == 0 are
// evicted if currently unreferenced. Note that scoring might not be perfect
// because entries can be referenced transiently within the cache even when
// there are no outside references to the entry.
//
// Cache sharding like LRUCache is used to reduce contention on usage+eviction
// state, though here the performance improvement from more shards is small,
// and (as noted above) potentially detrimental if shard capacity is too close
// to largest entry size. Here cache sharding mostly only affects cache update
// (Insert / Erase) performance, not read performance.
//
// Read efficiency (hot path)
// --------------------------
// Mostly to minimize the cost of accessing metadata blocks with
// cache_index_and_filter_blocks=true, we focus on optimizing Lookup and
// Release. In terms of concurrency, at a minimum, these operations have
// to do reference counting (and Lookup has to compare full keys in a safe
// way). Can we fold in all the other metadata tracking *for free* with
// Lookup and Release doing a simple atomic fetch_add/fetch_sub? (Assume
// for the moment that Lookup succeeds on the first probe.)
//
// We have a clever way of encoding an entry's reference count and countdown
// clock so that Lookup and Release are each usually a single atomic addition.
// In a single metadata word we have both an "acquire" count, incremented by
// Lookup, and a "release" count, incremented by Release. If useful=false,
// Release can instead decrement the acquire count. Thus the current ref
// count is (acquires - releases), and the countdown clock is min(3, acquires).
// Note that only unreferenced entries (acquires == releases) are eligible
// for CLOCK manipulation and eviction. We tolerate use of more expensive
// compare_exchange operations for cache writes (insertions and erasures).
//
// In a cache receiving many reads and little or no writes, it is possible
// for the acquire and release counters to overflow. Assuming the *current*
// refcount never reaches to many millions, we only have to correct for
// overflow in both counters in Release, not in Lookup. The overflow check
// should be only 1-2 CPU cycles per Release because it is a predictable
// branch on a simple condition on data already in registers.
//
// Slot states
// -----------
// We encode a state indicator into the same metadata word with the
// acquire and release counters. This allows bigger state transitions to
// be atomic. States:
//
// * Empty - slot is not in use and unowned. All other metadata and data is
// in an undefined state.
// * Construction - slot is exclusively owned by one thread, the thread
// successfully entering this state, for populating or freeing data
// (de-construction, same state marker).
// * Shareable (group) - slot holds an entry with counted references for
// pinning and reading, including
//   * Visible - slot holds an entry that can be returned by Lookup
//   * Invisible - slot holds an entry that is not visible to Lookup
//     (erased by user) but can be read by existing references, and ref count
//     changed by Ref and Release.
//
// A special case is "standalone" entries, which are heap-allocated handles
// not in the table. They are always Invisible and freed on zero refs.
//
// State transitions:
// Empty -> Construction (in Insert): The encoding of state enables Insert to
// perform an optimistic atomic bitwise-or to take ownership if a slot is
// empty, or otherwise make no state change.
//
// Construction -> Visible (in Insert): This can be a simple assignment to the
// metadata word because the current thread has exclusive ownership and other
// metadata is meaningless.
//
// Visible -> Invisible (in Erase): This can be a bitwise-and while holding
// a shared reference, which is safe because the change is idempotent (in case
// of parallel Erase). By the way, we never go Invisible->Visible.
//
// Shareable -> Construction (in Evict part of Insert, in Erase, and in
// Release if Invisible): This is for starting to freeing/deleting an
// unreferenced entry. We have to use compare_exchange to ensure we only make
// this transition when there are zero refs.
//
// Construction -> Empty (in same places): This is for completing free/delete
// of an entry. A "release" atomic store suffices, as we have exclusive
// ownership of the slot but have to ensure none of the data member reads are
// re-ordered after committing the state transition.
//
// Insert
// ------
// If Insert were to guarantee replacing an existing entry for a key, there
// would be complications for concurrency and efficiency. First, consider how
// many probes to get to an entry. To ensure Lookup never waits and
// availability of a key is uninterrupted, we would need to use a different
// slot for a new entry for the same key. This means it is most likely in a
// later probing position than the old version, which should soon be removed.
// (Also, an entry is too big to replace atomically, even if no current refs.)
//
// However, overwrite capability is not really needed by RocksDB. Also, we
// know from our "redundant" stats that overwrites are very rare for the block
// cache, so we should not spend much to make them effective.
//
// FixedHyperClockCache: Instead we Insert as soon as we find an empty slot in
// the probing sequence without seeing an existing (visible) entry for the same
// key. This way we only insert if we can improve the probing performance, and
// we don't need to probe beyond our insert position, assuming we are willing
// to let the previous entry for the same key die of old age (eventual eviction
// from not being used). We can reach a similar state with concurrent
// insertions, where one will pass over the other while it is "under
// construction." This temporary duplication is acceptable for RocksDB block
// cache because we know redundant insertion is rare.
// AutoHyperClockCache: Similar, except we only notice and return an existing
// match if it is found in the search for a suitable empty slot (starting with
// the same slot as the head pointer), not by following the existing chain of
// entries. Insertions are always made to the head of the chain.
//
// Another problem to solve is what to return to the caller when we find an
// existing entry whose probing position we cannot improve on, or when the
// table occupancy limit has been reached. If strict_capacity_limit=false,
// we must never fail Insert, and if a Handle* is provided, we have to return
// a usable Cache handle on success. The solution to this (typically rare)
// problem is "standalone" handles, which are usable by the caller but not
// actually available for Lookup in the Cache. Standalone handles are allocated
// independently on the heap and specially marked so that they are freed on
// the heap when their last reference is released.
//
// Usage on capacity
// -----------------
// Insert takes different approaches to usage tracking depending on
// strict_capacity_limit setting. If true, we enforce a kind of strong
// consistency where compare-exchange is used to ensure the usage number never
// exceeds its limit, and provide threads with an authoritative signal on how
// much "usage" they have taken ownership of. With strict_capacity_limit=false,
// we use a kind of "eventual consistency" where all threads Inserting to the
// same cache shard might race on reserving the same space, but the
// over-commitment will be worked out in later insertions. It is kind of a
// dance because we don't want threads racing each other too much on paying
// down the over-commitment (with eviction) either.
//
// Eviction
// --------
// A key part of Insert is evicting some entries currently unreferenced to
// make room for new entries. The high-level eviction algorithm is described
// above, but the details are also interesting. A key part is parallelizing
// eviction with a single CLOCK pointer. This works by each thread working on
// eviction pre-emptively incrementing the CLOCK pointer, and then CLOCK-
// updating or evicting the incremented-over slot(s). To reduce contention at
// the cost of possibly evicting too much, each thread increments the clock
// pointer by 4, so commits to updating at least 4 slots per batch. As
// described above, a CLOCK update will decrement the "countdown" of
// unreferenced entries, or evict unreferenced entries with zero countdown.
// Referenced entries are not updated, because we (presumably) don't want
// long-referenced entries to age while referenced. Note however that we
// cannot distinguish transiently referenced entries from cache user
// references, so some CLOCK updates might be somewhat arbitrarily skipped.
// This is OK as long as it is rare enough that eviction order is still
// pretty good.
//
// There is no synchronization on the completion of the CLOCK updates, so it
// is theoretically possible for another thread to cycle back around and have
// two threads racing on CLOCK updates to the same slot. Thus, we cannot rely
// on any implied exclusivity to make the updates or eviction more efficient.
// These updates use an opportunistic compare-exchange (no loop), where a
// racing thread might cause the update to be skipped without retry, but in
// such case the update is likely not needed because the most likely update
// to an entry is that it has become referenced. (TODO: test efficiency of
// avoiding compare-exchange loop)
//
// Release
// -------
// In the common case, Release is a simple atomic increment of the release
// counter. There is a simple overflow check that only does another atomic
// update in extremely rare cases, so costs almost nothing.
//
// If the Release specifies "not useful", we can instead decrement the
// acquire counter, which returns to the same CLOCK state as before Lookup
// or Ref.
//
// Adding a check for over-full cache on every release to zero-refs would
// likely be somewhat expensive, increasing read contention on cache shard
// metadata. Instead we are less aggressive about deleting entries right
// away in those cases.
//
// However Release tries to immediately delete entries reaching zero refs
// if (a) erase_if_last_ref is set by the caller, or (b) the entry is already
// marked invisible. Both of these are checks on values already in CPU
// registers so do not increase cross-CPU contention when not applicable.
// When applicable, they use a compare-exchange loop to take exclusive
// ownership of the slot for freeing the entry. These are rare cases
// that should not usually affect performance.
//
// Erase
// -----
// Searches for an entry like Lookup but moves it to Invisible state if found.
// This state transition is with bit operations so is idempotent and safely
// done while only holding a shared "read" reference. Like Release, it makes
// a best effort to immediately release an Invisible entry that reaches zero
// refs, but there are some corner cases where it will only be freed by the
// clock eviction process.

// ----------------------------------------------------------------------- //

struct ClockHandleBasicData : public Cache::Handle {
  Cache::ObjectPtr value = nullptr;
  const Cache::CacheItemHelper* helper = nullptr;
  // A lossless, reversible hash of the fixed-size (16 byte) cache key. This
  // eliminates the need to store a hash separately.
  UniqueId64x2 hashed_key = kNullUniqueId64x2;
  size_t total_charge = 0;

  inline size_t GetTotalCharge() const { return total_charge; }

  // Calls deleter (if non-null) on cache key and value
  void FreeData(MemoryAllocator* allocator) const;

  // Required by concept HandleImpl
  const UniqueId64x2& GetHash() const { return hashed_key; }
};

struct ClockHandle : public ClockHandleBasicData {
  // Constants for handling the atomic `meta` word, which tracks most of the
  // state of the handle. The meta word looks like this:
  // low bits                                                     high bits
  // -----------------------------------------------------------------------
  // | acquire counter      | release counter     | hit bit | state marker |
  // -----------------------------------------------------------------------

  struct SlotMeta : public BitFields<uint64_t, SlotMeta> {
    // For reading or updating counters in meta word.
    static constexpr uint8_t kCounterNumBits = 30;
    // Number of times the a reference has been acquired (or attempted)
    // since last reset by eviction processing
    using AcquireCounter =
        UnsignedBitField<SlotMeta, kCounterNumBits, NoPrevBitField>;
    // Number of times the a reference has been released (or attempted)
    // since last reset by eviction processing
    using ReleaseCounter =
        UnsignedBitField<SlotMeta, kCounterNumBits, AcquireCounter>;
    // Metadata bit in support of secondary cache
    using HitFlag = BoolBitField<SlotMeta, ReleaseCounter>;
    // Occupied means any state other than empty
    using OccupiedFlag = BoolBitField<SlotMeta, HitFlag>;
    // Shareable means the entry is reference counted (visible or invisible)
    // (only set if also occupied)
    using ShareableFlag = BoolBitField<SlotMeta, OccupiedFlag>;
    // Visible is only set if also shareable (invisible can't be found by
    // Lookup)
    using VisibleFlag = BoolBitField<SlotMeta, ShareableFlag>;

    // Convenience functions
    uint32_t GetAcquireCounter() const { return Get<AcquireCounter>(); }
    void SetAcquireCounter(uint32_t val) { Set<AcquireCounter>(val); }
    uint32_t GetReleaseCounter() const { return Get<ReleaseCounter>(); }
    void SetReleaseCounter(uint32_t val) { Set<ReleaseCounter>(val); }
    uint32_t GetRefcount() const {
      return Get<AcquireCounter>() - Get<ReleaseCounter>();
    }
    bool GetHit() const { return Get<HitFlag>(); }
    void SetHit(bool val) { Set<HitFlag>(val); }

    // Some distinct states for the various state flags
    bool IsEmpty() const {
      bool rv = !Get<OccupiedFlag>();
      if (rv) {
        assert(!Get<ShareableFlag>());
        assert(!Get<VisibleFlag>());
      }
      return rv;
    }

    bool IsUnderConstruction() const {
      bool rv = Get<OccupiedFlag>() && !Get<ShareableFlag>();
      if (rv) {
        assert(!Get<VisibleFlag>());
      }
      return rv;
    }
    void SetUnderConstruction() {
      Set<OccupiedFlag>(true);
      Set<ShareableFlag>(false);
      Set<VisibleFlag>(false);
    }

    bool IsShareable() const { return Get<ShareableFlag>(); }
    bool IsInvisible() const {
      bool rv = Get<ShareableFlag>() && !Get<VisibleFlag>();
      if (rv) {
        assert(Get<OccupiedFlag>());
      }
      return rv;
    }
    void SetInvisible() {
      Set<OccupiedFlag>(true);
      Set<ShareableFlag>(true);
      Set<VisibleFlag>(false);
    }

    bool IsVisible() const {
      bool rv = Get<ShareableFlag>() && Get<VisibleFlag>();
      if (rv) {
        assert(Get<OccupiedFlag>());
      }
      return rv;
    }
    void SetVisible() {
      Set<OccupiedFlag>(true);
      Set<ShareableFlag>(true);
      Set<VisibleFlag>(true);
    }
  };

  // Constants for initializing the countdown clock. (Countdown clock is only
  // in effect with zero refs, acquire counter == release counter, and in that
  // case the countdown clock == both of those counters.)
  static constexpr uint8_t kHighCountdown = 3;
  static constexpr uint8_t kLowCountdown = 2;
  static constexpr uint8_t kBottomCountdown = 1;
  // During clock update, treat any countdown clock value greater than this
  // value the same as this value.
  static constexpr uint8_t kMaxCountdown = kHighCountdown;
  // TODO: make these coundown values tuning parameters for eviction?

  // See above. Mutable for read reference counting.
  mutable BitFieldsAtomic<SlotMeta> meta{};
};  // struct ClockHandle

class BaseClockTable {
 public:
  struct BaseOpts {
    explicit BaseOpts(int _eviction_effort_cap)
        : eviction_effort_cap(_eviction_effort_cap) {}
    explicit BaseOpts(const HyperClockCacheOptions& opts)
        : BaseOpts(opts.eviction_effort_cap) {}
    int eviction_effort_cap;
  };

  BaseClockTable(size_t capacity, bool strict_capacity_limit,
                 int eviction_effort_cap,
                 CacheMetadataChargePolicy metadata_charge_policy,
                 MemoryAllocator* allocator,
                 const Cache::EvictionCallback* eviction_callback,
                 const uint32_t* hash_seed);

  template <class Table>
  typename Table::HandleImpl* CreateStandalone(ClockHandleBasicData& proto,
                                               bool allow_uncharged);

  template <class Table>
  Status Insert(const ClockHandleBasicData& proto,
                typename Table::HandleImpl** handle, Cache::Priority priority);

  void Ref(ClockHandle& handle);

  size_t GetOccupancy() const { return occupancy_.LoadRelaxed(); }

  size_t GetUsage() const { return usage_.LoadRelaxed(); }

  size_t GetStandaloneUsage() const { return standalone_usage_.LoadRelaxed(); }

  size_t GetCapacity() const { return capacity_.LoadRelaxed(); }

  void SetCapacity(size_t capacity) { capacity_.StoreRelaxed(capacity); }

  void SetStrictCapacityLimit(bool strict_capacity_limit) {
    if (strict_capacity_limit) {
      eec_and_scl_.ApplyRelaxed(StrictCapacityLimit::SetTransform());
    } else {
      eec_and_scl_.ApplyRelaxed(StrictCapacityLimit::ClearTransform());
    }
  }

  uint32_t GetHashSeed() const { return hash_seed_; }

  uint64_t GetYieldCount() const { return yield_count_.LoadRelaxed(); }

  uint64_t GetEvictionEffortExceededCount() const {
    return eviction_effort_exceeded_count_.LoadRelaxed();
  }

  struct EvictionData {
    size_t freed_charge = 0;
    size_t freed_count = 0;
    size_t seen_pinned_count = 0;
  };

  void TrackAndReleaseEvictedEntry(ClockHandle* h);

  bool IsEvictionEffortExceeded(const BaseClockTable::EvictionData& data) const;
#ifndef NDEBUG
  // Acquire N references
  void TEST_RefN(ClockHandle& handle, uint32_t n);
  // Helper for TEST_ReleaseN
  void TEST_ReleaseNMinus1(ClockHandle* handle, uint32_t n);
#endif

 private:  // fns
  // Creates a "standalone" handle for returning from an Insert operation that
  // cannot be completed by actually inserting into the table.
  // Updates `standalone_usage_` but not `usage_` nor `occupancy_`.
  template <class HandleImpl>
  HandleImpl* StandaloneInsert(const ClockHandleBasicData& proto);

  // Helper for updating `usage_` for new entry with given `total_charge`
  // and evicting if needed under strict_capacity_limit=true rules. This
  // means the operation might fail with Status::MemoryLimit. If
  // `need_evict_for_occupancy`, then eviction of at least one entry is
  // required, and the operation should fail if not possible.
  // NOTE: Otherwise, occupancy_ is not managed in this function
  template <class Table>
  Status ChargeUsageMaybeEvictStrict(size_t total_charge,
                                     bool need_evict_for_occupancy,
                                     typename Table::InsertState& state);

  // Helper for updating `usage_` for new entry with given `total_charge`
  // and evicting if needed under strict_capacity_limit=false rules. This
  // means that updating `usage_` always succeeds even if forced to exceed
  // capacity. If `need_evict_for_occupancy`, then eviction of at least one
  // entry is required, and the operation should return false if such eviction
  // is not possible. `usage_` is not updated in that case. Otherwise, returns
  // true, indicating success.
  // NOTE: occupancy_ is not managed in this function
  template <class Table>
  bool ChargeUsageMaybeEvictNonStrict(size_t total_charge,
                                      bool need_evict_for_occupancy,
                                      typename Table::InsertState& state);

 protected:  // data
  // We partition the following members into different cache lines
  // to avoid false sharing among Lookup, Release, Erase and Insert
  // operations in ClockCacheShard.

  // Clock algorithm sweep pointer.
  // (Relaxed: only needs to be consistent with itself.)
  RelaxedAtomic<uint64_t> clock_pointer_{};

  // Counter for number of times we yield to wait on another thread.
  // It is normal for this to occur rarely in normal operation.
  // (Relaxed: a simple stat counter.)
  RelaxedAtomic<uint64_t> yield_count_{};

  // Counter for number of times eviction effort cap is exceeded.
  // It is normal for this to occur rarely in normal operation.
  // (Relaxed: a simple stat counter.)
  RelaxedAtomic<uint64_t> eviction_effort_exceeded_count_{};

  // TODO: is this separation needed if we don't do background evictions?
  ALIGN_AS(CACHE_LINE_SIZE)
  // Number of elements in the table.
  Atomic<size_t> occupancy_{};

  // Memory usage by entries tracked by the cache (including standalone)
  Atomic<size_t> usage_{};

  // Part of usage by standalone entries (not in table)
  Atomic<size_t> standalone_usage_{};

  // Maximum total charge of all elements stored in the table.
  // (Relaxed: eventual consistency/update is OK)
  RelaxedAtomic<size_t> capacity_;

  // Encodes eviction_effort_cap (bottom 31 bits) and strict_capacity_limit
  // (top bit). See HyperClockCacheOptions::eviction_effort_cap etc.
  struct EecAndScl : public BitFields<uint32_t, EecAndScl> {
    uint32_t GetEffectiveEvictionEffortCap() const {
      // Because setting strict_capacity_limit is supposed to imply infinite
      // cap on eviction effort, we can let the bit for strict_capacity_limit
      // in the upper-most bit position to used as part of the effective cap.
      return underlying;
    }
  };
  using EvictionEffortCap = UnsignedBitField<EecAndScl, 31, NoPrevBitField>;
  using StrictCapacityLimit = BoolBitField<EecAndScl, EvictionEffortCap>;
  // (Relaxed: eventual consistency/update is OK)
  RelaxedBitFieldsAtomic<EecAndScl> eec_and_scl_;

  ALIGN_AS(CACHE_LINE_SIZE)
  const CacheMetadataChargePolicy metadata_charge_policy_;

  // From Cache, for deleter
  MemoryAllocator* const allocator_;

  // A reference to Cache::eviction_callback_
  const Cache::EvictionCallback& eviction_callback_;

  // A reference to ShardedCacheBase::hash_seed_
  const uint32_t& hash_seed_;
};

// Hash table for cache entries with size determined at creation time.
// Uses open addressing and double hashing. Since entries cannot be moved,
// the "displacements" count ensures probing sequences find entries even when
// entries earlier in the probing sequence have been removed.
class FixedHyperClockTable : public BaseClockTable {
 public:
  // Target size to be exactly a common cache line size (see static_assert in
  // clock_cache.cc)
  struct ALIGN_AS(64U) HandleImpl : public ClockHandle {
    // The number of elements that hash to this slot or a lower one, but wind
    // up in this slot or a higher one.
    // (Relaxed: within a Cache op, does not need consistency with entries
    // inserted/removed during that op. For example, a Lookup() that
    // happens-after an Insert() will see an appropriate displacements value
    // for the entry to be in a published state.)
    RelaxedAtomic<uint32_t> displacements{};

    // Whether this is a "deteched" handle that is independently allocated
    // with `new` (so must be deleted with `delete`).
    // TODO: ideally this would be packed into some other data field, such
    // as upper bits of total_charge, but that incurs a measurable performance
    // regression.
    bool standalone = false;

    inline bool IsStandalone() const { return standalone; }

    inline void SetStandalone() { standalone = true; }
  };  // struct HandleImpl

  struct Opts : public BaseOpts {
    explicit Opts(size_t _estimated_value_size, int _eviction_effort_cap)
        : BaseOpts(_eviction_effort_cap),
          estimated_value_size(_estimated_value_size) {}
    explicit Opts(const HyperClockCacheOptions& opts)
        : BaseOpts(opts.eviction_effort_cap) {
      assert(opts.estimated_entry_charge > 0);
      estimated_value_size = opts.estimated_entry_charge;
    }
    size_t estimated_value_size;
  };

  FixedHyperClockTable(size_t capacity, bool strict_capacity_limit,
                       CacheMetadataChargePolicy metadata_charge_policy,
                       MemoryAllocator* allocator,
                       const Cache::EvictionCallback* eviction_callback,
                       const uint32_t* hash_seed, const Opts& opts);
  ~FixedHyperClockTable();

  // For BaseClockTable::Insert
  struct InsertState {};

  void StartInsert(InsertState& state);

  // Returns true iff there is room for the proposed number of entries.
  bool GrowIfNeeded(size_t new_occupancy, InsertState& state);

  HandleImpl* DoInsert(const ClockHandleBasicData& proto,
                       uint32_t initial_countdown, bool take_ref,
                       InsertState& state);

  // Runs the clock eviction algorithm trying to reclaim at least
  // requested_charge. Returns how much is evicted, which could be less
  // if it appears impossible to evict the requested amount without blocking.
  void Evict(size_t requested_charge, InsertState& state, EvictionData* data);

  HandleImpl* Lookup(const UniqueId64x2& hashed_key);

  bool Release(HandleImpl* handle, bool useful, bool erase_if_last_ref);

  void Erase(const UniqueId64x2& hashed_key);

  void EraseUnRefEntries();

  size_t GetTableSize() const { return size_t{1} << length_bits_; }

  size_t GetOccupancyLimit() const { return occupancy_limit_; }

  const HandleImpl* HandlePtr(size_t idx) const { return &array_[idx]; }

#ifndef NDEBUG
  size_t& TEST_MutableOccupancyLimit() {
    return const_cast<size_t&>(occupancy_limit_);
  }

  // Release N references
  void TEST_ReleaseN(HandleImpl* handle, uint32_t n);
#endif

  // The load factor p is a real number in (0, 1) such that at all
  // times at most a fraction p of all slots, without counting tombstones,
  // are occupied by elements. This means that the probability that a random
  // probe hits an occupied slot is at most p, and thus at most 1/p probes
  // are required on average. For example, p = 70% implies that between 1 and 2
  // probes are needed on average (bear in mind that this reasoning doesn't
  // consider the effects of clustering over time, which should be negligible
  // with double hashing).
  // Because the size of the hash table is always rounded up to the next
  // power of 2, p is really an upper bound on the actual load factor---the
  // actual load factor is anywhere between p/2 and p. This is a bit wasteful,
  // but bear in mind that slots only hold metadata, not actual values.
  // Since space cost is dominated by the values (the LSM blocks),
  // overprovisioning the table with metadata only increases the total cache
  // space usage by a tiny fraction.
  static constexpr double kLoadFactor = 0.7;

  // The user can exceed kLoadFactor if the sizes of the inserted values don't
  // match estimated_value_size, or in some rare cases with
  // strict_capacity_limit == false. To avoid degenerate performance, we set a
  // strict upper bound on the load factor.
  static constexpr double kStrictLoadFactor = 0.84;

 private:  // functions
  // Returns x mod 2^{length_bits_}.
  inline size_t ModTableSize(uint64_t x) {
    return BitwiseAnd(x, length_bits_mask_);
  }

  // Returns the first slot in the probe sequence with a handle e such that
  // match_fn(e) is true. At every step, the function first tests whether
  // match_fn(e) holds. If this is false, it evaluates abort_fn(e) to decide
  // whether the search should be aborted, and if so, FindSlot immediately
  // returns nullptr. For every handle e that is not a match and not aborted,
  // FindSlot runs update_fn(e, is_last) where is_last is set to true iff that
  // slot will be the last probed because the next would cycle back to the first
  // slot probed. This function uses templates instead of std::function to
  // minimize the risk of heap-allocated closures being created.
  template <typename MatchFn, typename AbortFn, typename UpdateFn>
  inline HandleImpl* FindSlot(const UniqueId64x2& hashed_key,
                              const MatchFn& match_fn, const AbortFn& abort_fn,
                              const UpdateFn& update_fn);

  // Re-decrement all displacements in probe path starting from beginning
  // until (not including) the given handle
  inline void Rollback(const UniqueId64x2& hashed_key, const HandleImpl* h);

  // Subtracts `total_charge` from `usage_` and 1 from `occupancy_`.
  // Ideally this comes after releasing the entry itself so that we
  // actually have the available occupancy/usage that is claimed.
  // However, that means total_charge has to be saved from the handle
  // before releasing it so that it can be provided to this function.
  inline void ReclaimEntryUsage(size_t total_charge);

  MemoryAllocator* GetAllocator() const { return allocator_; }

  // Returns the number of bits used to hash an element in the hash
  // table.
  static int CalcHashBits(size_t capacity, size_t estimated_value_size,
                          CacheMetadataChargePolicy metadata_charge_policy);

 private:  // data
  // Number of hash bits used for table index.
  // The size of the table is 1 << length_bits_.
  const int length_bits_;

  // For faster computation of ModTableSize.
  const size_t length_bits_mask_;

  // Maximum number of elements the user can store in the table.
  const size_t occupancy_limit_;

  // Array of slots comprising the hash table.
  const std::unique_ptr<HandleImpl[]> array_;
};  // class FixedHyperClockTable

// Hash table for cache entries that resizes automatically based on occupancy.
// However, it depends on a contiguous memory region to grow into
// incrementally, using linear hashing, so uses an anonymous mmap so that
// only the used portion of the memory region is mapped to physical memory
// (part of RSS).
//
// This table implementation uses the same "low-level protocol" for managing
// the contens of an entry slot as FixedHyperClockTable does, captured in the
// ClockHandle struct. The provides most of the essential data safety, but
// AutoHyperClockTable is another "high-level protocol" for organizing entries
// into a hash table, with automatic resizing.
//
// This implementation is not fully wait-free but we can call it "essentially
// wait-free," and here's why. First, like FixedHyperClockCache, there is no
// locking nor other forms of waiting at the cache or shard level. Also like
// FixedHCC there is essentially an entry-level read-write lock implemented
// with atomics, but our relaxed atomicity/consistency guarantees (e.g.
// duplicate inserts are possible) mean we do not need to wait for entry
// locking. Lookups, non-erasing Releases, and non-evicting non-growing Inserts
// are all fully wait-free. Of course, these waits are not dependent on any
// external factors such as I/O.
//
// For operations that remove entries from a chain or grow the table by
// splitting a chain, there is a chain-level locking mechanism that we call a
// "rewrite" lock, and the only waits are for these locks. On average, each
// chain lock is relevant to < 2 entries each. (The average would be less than
// one entry each, but we do not lock when there's no entry to remove or
// migrate.) And a given thread can only hold two such chain locks at a time,
// more typically just one. So in that sense alone, the waiting that does exist
// is very localized.
//
// If we look closer at the operations utilizing that locking mechanism, we
// can see why it's "essentially wait-free."
// * Grow operations to increase the size of the table: each operation splits
// an existing chain into two, and chains for splitting are chosen in table
// order. Grow operations are fully parallel except for the chain locking, but
// for one Grow operation to wait on another, it has to be feeding into the
// other, which means the table has doubled in size already from other Grow
// operations without the original one finishing. So Grow operations are very
// low latency (unlike LRUCache doubling the table size in one operation) and
// very parallelizeable. (We use some tricks to break up dependencies in
// updating metadata on the usable size of the table.) And obviously Grow
// operations are very rare after the initial population of the table.
// * Evict operations (part of many Inserts): clock updates and evictions
// sweep through the structure in table order, so like Grow operations,
// parallel Evict can only wait on each other if an Evict has lingered (slept)
// long enough that the clock pointer has wrapped around the entire structure.
// * Random erasures (Erase, Release with erase_if_last_ref, etc.): these
// operations are rare and not really considered performance critical.
// Currently they're mostly used for removing placeholder cache entries, e.g.
// for memory tracking, though that could use standalone entries instead to
// avoid potential contention in table operations. It's possible that future
// enhancements could pro-actively remove cache entries from obsolete files,
// but that's not yet implemented.
class AutoHyperClockTable : public BaseClockTable {
 public:
  // Target size to be exactly a common cache line size (see static_assert in
  // clock_cache.cc)
  struct ALIGN_AS(64U) HandleImpl : public ClockHandle {
    // To orgainize AutoHyperClockTable entries into a hash table while
    // allowing the table size to grow without existing entries being moved,
    // a version of chaining is used. Rather than being heap allocated (and
    // incurring overheads to ensure memory safety) entries must go into
    // Handles ("slots") in the pre-allocated array. To improve CPU cache
    // locality, the chain head pointers are interleved with the entries;
    // specifically, a Handle contains
    // * A head pointer for a chain of entries with this "home" location.
    // * A ClockHandle, for an entry that may or may not be in the chain
    // starting from that head (but for performance ideally is on that
    // chain).
    // * A next pointer for the continuation of the chain containing this
    // entry.
    //
    // The pointers are not raw pointers, but are indices into the array,
    // and are decorated in two ways to help detect and recover from
    // relevant concurrent modifications during Lookup, so that Lookup is
    // fully wait-free:
    // * Each "with_shift" pointer contains a shift count that indicates
    // how many hash bits were used in chosing the home address for the
    // chain--specifically the next entry in the chain.
    // * The end of a chain is given a special "end" marker and refers back
    // to the head of the chain.
    // These decorated pointers use the NextWithShift bit field struct below.
    //
    // Why do we need shift on each pointer? To make Lookup wait-free, we need
    // to be able to query a chain without missing anything, and preferably
    // avoid synchronously double-checking the length_info. Without the shifts,
    // there is a risk that we start down a chain and while paused on an entry
    // that goes to a new home, we then follow the rest of the
    // partially-migrated chain to see the shared ending with the old home, but
    // for a time were following the chain for the new home, missing some
    // entries for the old home.
    //
    // Why do we need the end of the chain to loop back? If Lookup pauses
    // at an "under construction" entry, and sees that "next" is null after
    // waking up, we need something to tell whether the "under construction"
    // entry was freed and reused for another chain. Otherwise, we could
    // miss entries still on the original chain due in the presence of a
    // concurrent modification. Until an entry is fully erased from a chain,
    // it is normal to see "under construction" entries on the chain, and it
    // is not safe to read their hashed key without either a read reference
    // on the entry or a rewrite lock on the chain.
    struct NextWithShift : public BitFields<uint64_t, NextWithShift> {
      // The "shift" associated with this decorated pointer (see description
      // above).
      using Shift = UnsignedBitField<NextWithShift, 6, NoPrevBitField>;
      // Marker for the end of a chain. Must also (a) point back to the head of
      // the chain (with end marker removed), and (b) set the LockedFlag
      // (below), so that attempting to lock an empty chain has no effect (not
      // needed, as the lock is only needed for removals).
      using EndFlag = BoolBitField<NextWithShift, Shift>;
      // Marker that some thread owning writes to the chain structure (except
      // for inserts), but only if not an "end" pointer. Also called the
      // "rewrite lock."
      using LockedFlag = BoolBitField<NextWithShift, EndFlag>;
      // The "next" associated with this decorated pointer, which is an index
      // into the table's array_ (see description above).
      using Next = UnsignedBitField<NextWithShift, 56, LockedFlag>;

      bool IsLocked() const { return Get<LockedFlag>(); }
      bool IsEnd() const {
        // End flag should imply locked flag
        assert(!Get<EndFlag>() || Get<LockedFlag>());
        return Get<EndFlag>();
      }
      bool IsLockedNotEnd() const {
        // NOTE: helping GCC to optimize this simpler code:
        // return IsLocked() && !IsEnd();
        constexpr U kEndFlag = U{1} << EndFlag::kBitOffset;
        constexpr U kLockedFlag = U{1} << LockedFlag::kBitOffset;
        return (underlying & (kEndFlag | kLockedFlag)) == kLockedFlag;
      }
      auto GetNext() const { return Get<Next>(); }
      auto GetShift() const { return Get<Shift>(); }

      static NextWithShift Make(size_t next, int shift) {
        return NextWithShift{}.With<Next>(next).With<Shift>(
            static_cast<uint8_t>(shift));
      }

      static NextWithShift MakeEnd(size_t next, int shift) {
        return Make(next, shift).With<EndFlag>(true).With<LockedFlag>(true);
      }
    };

    // A marker for head_next_with_shift that indicates this HandleImpl is
    // heap allocated (standalone) rather than in the table.
    static constexpr NextWithShift kStandaloneMarker{UINT64_MAX};

    // A marker for head_next_with_shift indicating the head is not yet part
    // of the usable table, or for chain_next_with_shift indicating that the
    // entry is not present or is not yet part of a chain (must not be
    // "shareable" state).
    static constexpr NextWithShift kUnusedMarker{0};

    // See above. The head pointer is logically independent of the rest of
    // the entry, including the chain next pointer.
    BitFieldsAtomic<NextWithShift> head_next_with_shift{kUnusedMarker};
    BitFieldsAtomic<NextWithShift> chain_next_with_shift{kUnusedMarker};

    // For supporting CreateStandalone and some fallback cases.
    inline bool IsStandalone() const {
      return head_next_with_shift.Load() == kStandaloneMarker;
    }

    inline void SetStandalone() {
      head_next_with_shift.Store(kStandaloneMarker);
    }
  };  // struct HandleImpl

  struct Opts : public BaseOpts {
    explicit Opts(size_t _min_avg_value_size, int _eviction_effort_cap)
        : BaseOpts(_eviction_effort_cap),
          min_avg_value_size(_min_avg_value_size) {}

    explicit Opts(const HyperClockCacheOptions& opts)
        : BaseOpts(opts.eviction_effort_cap) {
      assert(opts.estimated_entry_charge == 0);
      min_avg_value_size = opts.min_avg_entry_charge;
    }
    size_t min_avg_value_size;
  };

  AutoHyperClockTable(size_t capacity, bool strict_capacity_limit,
                      CacheMetadataChargePolicy metadata_charge_policy,
                      MemoryAllocator* allocator,
                      const Cache::EvictionCallback* eviction_callback,
                      const uint32_t* hash_seed, const Opts& opts);
  ~AutoHyperClockTable();

  // For BaseClockTable::Insert
  struct InsertState {
    uint64_t saved_length_info = 0;
    size_t likely_empty_slot = 0;
  };

  void StartInsert(InsertState& state);

  // Does initial check for whether there's hash table room for another
  // inserted entry, possibly growing if needed. Returns true iff (after
  // the call) there is room for the proposed number of entries.
  bool GrowIfNeeded(size_t new_occupancy, InsertState& state);

  HandleImpl* DoInsert(const ClockHandleBasicData& proto,
                       uint32_t initial_countdown, bool take_ref,
                       InsertState& state);

  // Runs the clock eviction algorithm trying to reclaim at least
  // requested_charge. Returns how much is evicted, which could be less
  // if it appears impossible to evict the requested amount without blocking.
  void Evict(size_t requested_charge, InsertState& state, EvictionData* data);

  HandleImpl* Lookup(const UniqueId64x2& hashed_key);

  bool Release(HandleImpl* handle, bool useful, bool erase_if_last_ref);

  void Erase(const UniqueId64x2& hashed_key);

  void EraseUnRefEntries();

  size_t GetTableSize() const;

  size_t GetOccupancyLimit() const;

  const HandleImpl* HandlePtr(size_t idx) const { return &array_[idx]; }

#ifndef NDEBUG
  size_t& TEST_MutableOccupancyLimit() {
    return *reinterpret_cast<size_t*>(&occupancy_limit_);
  }

  // Release N references
  void TEST_ReleaseN(HandleImpl* handle, uint32_t n);
#endif

  // Maximum ratio of number of occupied slots to number of usable slots. The
  // actual load factor should float pretty close to this number, which should
  // be a nice space/time trade-off, though large swings in WriteBufferManager
  // memory could lead to low (but very much safe) load factors (only after
  // seeing high load factors). Linear hashing along with (modified) linear
  // probing to find an available slot increases potential risks of high
  // load factors, so are disallowed.
  static constexpr double kMaxLoadFactor = 0.60;

 private:  // functions
  // Returns true iff increased usable length. Due to load factor
  // considerations, GrowIfNeeded might call this more than once to make room
  // for one more entry.
  bool Grow(InsertState& state);

  // Operational details of splitting a chain into two for Grow().
  void SplitForGrow(size_t grow_home, size_t old_home, int old_shift);

  // Takes an "under construction" entry and ensures it is no longer connected
  // to its home chain (in preparaion for completing erasure and freeing the
  // slot). Note that previous operations might have already noticed it being
  // "under (de)construction" and removed it from its chain.
  void Remove(HandleImpl* h);

  // Try to take ownership of an entry and erase+remove it from the table.
  // Returns true if successful. Could fail if
  // * There are other references to the entry
  // * Some other thread has exclusive ownership or has freed it.
  bool TryEraseHandle(HandleImpl* h, bool holding_ref, bool mark_invisible);

  // Calculates the appropriate maximum table size, for creating the memory
  // mapping.
  static size_t CalcMaxUsableLength(
      size_t capacity, size_t min_avg_value_size,
      CacheMetadataChargePolicy metadata_charge_policy);

  // Shared helper function that implements removing entries from a chain
  // with proper handling to ensure all existing data is seen even in the
  // presence of concurrent insertions, etc. (See implementation.)
  template <class OpData>
  void PurgeImpl(OpData* op_data, size_t home = SIZE_MAX,
                 EvictionData* data = nullptr);

  // An RAII wrapper for locking a chain of entries for removals. See
  // implementation.
  class ChainRewriteLock;

  // Helper function for PurgeImpl while holding a ChainRewriteLock. See
  // implementation.
  template <class OpData>
  void PurgeImplLocked(OpData* op_data, ChainRewriteLock& rewrite_lock,
                       size_t home, EvictionData* data);

  // Update length_info_ as much as possible without waiting, given a known
  // usable (ready for inserts and lookups) grow_home. (Previous grow_homes
  // might not be usable yet, but we can check if they are by looking at
  // the corresponding old home.)
  void CatchUpLengthInfoNoWait(size_t known_usable_grow_home);

 private:  // data
  // mmaped area holding handles
  const TypedMemMapping<HandleImpl> array_;

  // Metadata for table size under linear hashing.
  //
  // Lowest 8 bits are the minimum number of lowest hash bits to use
  // ("min shift"). The upper 56 bits are a threshold. If that minumum number
  // of bits taken from a hash value is < this threshold, then one more bit of
  // hash value is taken and used.
  //
  // Other mechanisms (shift amounts on pointers) ensure complete availability
  // of data already in the table even if a reader only sees a completely
  // out-of-date version of this value. In the worst case, it could take
  // log time to find the correct chain, but normally this value enables
  // readers to find the correct chain on the first try.
  //
  // To maximize parallelization of Grow() operations, this field is only
  // updated opportunistically after Grow() operations and in DoInsert() where
  // it is found to be out-of-date. See CatchUpLengthInfoNoWait().
  Atomic<uint64_t> length_info_;

  // An already-computed version of the usable length times the max load
  // factor. Could be slightly out of date but GrowIfNeeded()/Grow() handle
  // that internally.
  // (Relaxed: allowed to lag behind length_info_ by a little)
  RelaxedAtomic<size_t> occupancy_limit_;

  // The next index to use from array_ upon the next Grow(). Might be ahead of
  // length_info_.
  // (Relaxed: self-contained source of truth for next grow home)
  RelaxedAtomic<size_t> grow_frontier_;

  // See explanation in AutoHyperClockTable::Evict
  // (Relaxed: allowed to lag behind clock_pointer_ and length_info_ state)
  RelaxedAtomic<size_t> clock_pointer_mask_;
};  // class AutoHyperClockTable

// A single shard of sharded cache.
template <class TableT>
class ALIGN_AS(CACHE_LINE_SIZE) ClockCacheShard final : public CacheShardBase {
 public:
  using Table = TableT;
  ClockCacheShard(size_t capacity, bool strict_capacity_limit,
                  CacheMetadataChargePolicy metadata_charge_policy,
                  MemoryAllocator* allocator,
                  const Cache::EvictionCallback* eviction_callback,
                  const uint32_t* hash_seed, const typename Table::Opts& opts);

  // For CacheShard concept
  using HandleImpl = typename Table::HandleImpl;
  // Hash is lossless hash of 128-bit key
  using HashVal = UniqueId64x2;
  using HashCref = const HashVal&;
  static inline uint32_t HashPieceForSharding(HashCref hash) {
    return Upper32of64(hash[0]);
  }
  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    assert(key.size() == kCacheKeySize);
    HashVal in;
    HashVal out;
    // NOTE: endian dependence
    // TODO: use GetUnaligned?
    std::memcpy(&in, key.data(), kCacheKeySize);
    BijectiveHash2x64(in[1], in[0] ^ seed, &out[1], &out[0]);
    return out;
  }

  // For reconstructing key from hashed_key. Requires the caller to provide
  // backing storage for the Slice in `unhashed`
  static inline Slice ReverseHash(const UniqueId64x2& hashed,
                                  UniqueId64x2* unhashed, uint32_t seed) {
    BijectiveUnhash2x64(hashed[1], hashed[0], &(*unhashed)[1], &(*unhashed)[0]);
    (*unhashed)[0] ^= seed;
    // NOTE: endian dependence
    return Slice(reinterpret_cast<const char*>(unhashed), kCacheKeySize);
  }

  // Although capacity is dynamically changeable, the number of table slots is
  // not, so growing capacity substantially could lead to hitting occupancy
  // limit.
  void SetCapacity(size_t capacity);

  void SetStrictCapacityLimit(bool strict_capacity_limit);

  Status Insert(const Slice& key, const UniqueId64x2& hashed_key,
                Cache::ObjectPtr value, const Cache::CacheItemHelper* helper,
                size_t charge, HandleImpl** handle, Cache::Priority priority);

  HandleImpl* CreateStandalone(const Slice& key, const UniqueId64x2& hashed_key,
                               Cache::ObjectPtr obj,
                               const Cache::CacheItemHelper* helper,
                               size_t charge, bool allow_uncharged);

  HandleImpl* Lookup(const Slice& key, const UniqueId64x2& hashed_key);

  bool Release(HandleImpl* handle, bool useful, bool erase_if_last_ref);

  bool Release(HandleImpl* handle, bool erase_if_last_ref = false);

  bool Ref(HandleImpl* handle);

  void Erase(const Slice& key, const UniqueId64x2& hashed_key);

  size_t GetCapacity() const;

  size_t GetUsage() const;

  size_t GetStandaloneUsage() const;

  size_t GetPinnedUsage() const;

  size_t GetOccupancyCount() const;

  size_t GetOccupancyLimit() const;

  size_t GetTableAddressCount() const;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr obj,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state);

  void EraseUnRefEntries();

  std::string GetPrintableOptions() const { return std::string{}; }

  HandleImpl* Lookup(const Slice& key, const UniqueId64x2& hashed_key,
                     const Cache::CacheItemHelper* /*helper*/,
                     Cache::CreateContext* /*create_context*/,
                     Cache::Priority /*priority*/, Statistics* /*stats*/) {
    return Lookup(key, hashed_key);
  }

  Table& GetTable() { return table_; }
  const Table& GetTable() const { return table_; }

#ifndef NDEBUG
  size_t& TEST_MutableOccupancyLimit() {
    return table_.TEST_MutableOccupancyLimit();
  }
  // Acquire/release N references
  void TEST_RefN(HandleImpl* handle, uint32_t n);
  void TEST_ReleaseN(HandleImpl* handle, uint32_t n);
#endif

 private:  // data
  Table table_;
};  // class ClockCacheShard

template <class Table>
class BaseHyperClockCache : public ShardedCache<ClockCacheShard<Table>> {
 public:
  using Base = ShardedCache<ClockCacheShard<Table>>;
  using Shard = ClockCacheShard<Table>;
  using Handle = Cache::Handle;
  using CacheItemHelper = Cache::CacheItemHelper;

  explicit BaseHyperClockCache(const HyperClockCacheOptions& opts);

  // Check the public key before ShardedCache computes its fixed-width hash.
  Status Insert(
      const Slice& key, Cache::ObjectPtr obj, const CacheItemHelper* helper,
      size_t charge, Handle** handle = nullptr,
      Cache::Priority priority = Cache::Priority::LOW,
      const Slice& compressed_value = Slice(),
      CompressionType type = CompressionType::kNoCompression) override;

  Handle* CreateStandalone(const Slice& key, Cache::ObjectPtr obj,
                           const CacheItemHelper* helper, size_t charge,
                           bool allow_uncharged) override;

  Handle* Lookup(const Slice& key, const CacheItemHelper* helper = nullptr,
                 Cache::CreateContext* create_context = nullptr,
                 Cache::Priority priority = Cache::Priority::LOW,
                 Statistics* stats = nullptr) override;

  void Erase(const Slice& key) override;

  Cache::ObjectPtr Value(Handle* handle) override;

  size_t GetCharge(Handle* handle) const override;

  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override;

  void ApplyToHandle(
      Cache* cache, Handle* handle,
      const std::function<void(const Slice& key, Cache::ObjectPtr obj,
                               size_t charge, const CacheItemHelper* helper)>&
          callback) override;

  void ReportProblems(
      const std::shared_ptr<Logger>& /*info_log*/) const override;
};

class FixedHyperClockCache
#ifdef NDEBUG
    final
#endif
    : public BaseHyperClockCache<FixedHyperClockTable> {
 public:
  using BaseHyperClockCache::BaseHyperClockCache;

  const char* Name() const override { return "FixedHyperClockCache"; }

  void ReportProblems(
      const std::shared_ptr<Logger>& /*info_log*/) const override;
};  // class FixedHyperClockCache

class AutoHyperClockCache
#ifdef NDEBUG
    final
#endif
    : public BaseHyperClockCache<AutoHyperClockTable> {
 public:
  using BaseHyperClockCache::BaseHyperClockCache;

  const char* Name() const override { return "AutoHyperClockCache"; }

  void ReportProblems(
      const std::shared_ptr<Logger>& /*info_log*/) const override;
};  // class AutoHyperClockCache

}  // namespace clock_cache

}  // namespace ROCKSDB_NAMESPACE
