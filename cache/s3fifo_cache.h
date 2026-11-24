//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cache/sharded_cache.h"
#include "port/lang.h"
#include "port/likely.h"
#include "port/mmap.h"
#include "port/port.h"
#include "table/unique_id_impl.h"
#include "util/autovector.h"
#include "util/distributed_mutex.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {
namespace s3fifo_cache {

class S3FifoHelperRegistry;

// S3FifoCache: an opt-in block cache. This file evicts with plain FIFO; the
// S3-FIFO policy (Yang et al., "FIFO Queues are All You Need for Cache
// Eviction", SOSP '23) is built on top of it in later changes.
//
// Classes. S3FifoCache is the public Cache: as ShardedCache<S3FifoCacheShard>
// it hashes the key, picks a shard from the upper hash bits and forwards the
// call, keeping almost no state of its own. S3FifoCacheShard is one
// independent small cache (its table, FIFO queue, mutex and slot
// reclamation); all the logic is there. S3FifoHandle is one entry, stored in
// one 42-byte slot of its shard's table (three slots per 128-byte group); a
// Cache::Handle* points to it.
//
// Eviction: each shard evicts its oldest unpinned entry first. A Lookup does
// not change an entry's position.
//
// Locking: Lookup, Ref and a Release that neither erases nor frees take no
// lock, and neither do the usage and occupancy getters. Insert, Erase,
// eviction, SetCapacity and the bulk operations take the shard mutex.
//
// Shard size. A shard is one independent cache with its own mutex, slot
// table and queues; a key's hash picks its shard. Each shard should be
// neither too small nor too large:
// - Too small: one large block, such as an unpartitioned index or filter
//   block (hundreds of KiB), takes a big share of a small shard and pushes
//   the rest of the shard out. In db_bench with 320 KiB index blocks, 512 KiB
//   shards missed index blocks on 24% of lookups and 4 MiB shards on 1.1%;
//   at a 32 MiB cache 4 MiB shards were 2-3x faster. So a shard is at least
//   kMinShardSize (4 MiB).
// - Too large: slot indexes are 16 bits, so a shard holds at most 65,532
//   homes (a home is a hash bucket; see Table at S3FifoCacheShard), about
//   39K entries at the 3/5 load factor. A shard larger than
//   39K x its average entry fills its table before its capacity and then
//   also evicts by count, leaving capacity unused. So a shard is at most
//   kMaxShardSize (128 MiB, which fits 4 KiB average entries).
// Left at -1, num_shard_bits keeps every shard within [4 MiB, 128 MiB] (a cache
// under 8 MiB is one shard): up to 64 shards while that keeps shards >= 4 MiB,
// and more than 64 when needed to keep them <= 128 MiB (32 MiB -> 8 shards,
// 256 MiB -> 64, 4 GiB -> 64, 16 GiB -> 128, 64 GiB -> 512).
// Set num_shard_bits explicitly when the average entry is far from 4 KiB:
// smaller entries need more shards (e.g. 512-byte blocks: at most about
// 19 MiB per shard), much larger ones may use fewer. Fewer shards put more
// inserts behind each mutex; with many more threads than CPUs and misses
// served from memory, that raises tail latency.
//
// Where entries live: most entries are in a slot of their shard's table and
// in the shard's FIFO queue. Three kinds are outside the queue:
// - an entry set aside because it stayed pinned: still in the table, on the
//   aside list, a list of pinned entries removed from their queue so
//   eviction stops re-walking them (see aside_slot_);
// - a standalone entry (from CreateStandalone, when the table has no free slot,
//   or for a slotless entry, see Layout below): outside the table, freed on its
//   last Release;
// - an entry erased or overwritten while pinned (detached): in neither the
//   table nor a queue, freed on its last Release.
//
// Slot reuse: a lock-free Lookup may still read a slot whose entry was
// removed, so the slot is reused only after epoch reclamation. See
// S3FifoCacheShard for the table, reclamation and the aside list.
//
// S3FifoHandle can be in these states:
// 1. Referenced externally AND in the slot table.
//    The entry stays in the FIFO queue. An eviction scan that meets it
//    moves it to the tail, so later scans do not re-walk it. In a mostly
//    pinned shard the scan sets it aside instead, until it is unpinned (see
//    aside_slot_).
//    (refs >= 1 && in_cache == true)
// 2. Not referenced externally AND in the slot table.
//    In the FIFO queue, or still on the aside list until an eviction
//    returns it to the queue; evictable once in the queue.
//    (refs == 0 && in_cache == true)
// 3. Referenced externally AND not in the slot table.
//    The entry is in neither the queue nor the slot table. It is freed
//    when refs drops to 0.
//    (refs >= 1 && in_cache == false)
// Unlike LRU, Lookup and Release never reorder the queue; only an eviction
// scan moves a pinned entry it meets to the tail. While refs > 0, public
// properties like value and helper must not change.
//
// Lookup, Ref and most Releases run without the shard lock, so refs and the
// in-cache bit share one atomic word (state). The bit changes only under the
// shard lock, in the same atomic operation that checks refs. Eviction claims an
// entry with a CAS that requires refs == 0 and clears state to 0, so no Lookup
// can pin it afterwards.
//
// Layout. A slot is 42 bytes, three to a 128-byte group (S3FifoSlotGroup), so
// the table costs 128/3 bytes per slot, about 71 bytes per entry at its 3/5
// load factor. No member is aligned to more than 2 bytes, so the slots pack
// without padding; the value and charge are byte arrays accessed with memcpy.
// - What a chain step reads (key, head, chain_next) and the state a hit
//   takes its ref on are bytes 0-21, first in the slot, so they never cross
//   a 64-byte line at any of the three positions (slot 1 starts at byte 42
//   and its bytes 0-21 end at byte 63). For a 16-byte key, KeyMatches reads
//   flags only after the hash matches, so a miss stays on that line. A hit
//   also reads flags and the value: for slot 1 those are on the group's
//   second line.
// - The 2-byte atomics sit at even offsets, and every slot starts at an even
//   offset, so none straddles a line.
// - aside keeps a byte of its own; the last byte (offset 41) is padding.
// - The helper is an index into the cache's S3FifoHelperRegistry, the charge
//   32 bits, and slot indexes 16 bits (so a shard holds at most 65532 homes,
//   see S3FifoCacheShard::kMaxLength). An entry the slot cannot describe (a
//   charge above UINT32_MAX, or one of the 256th and later distinct helpers) is
//   slotless. Inserted with a handle it is standalone (S3FifoStandaloneHandle,
//   which keeps the full helper pointer and charge); under
//   strict_capacity_limit only if it fits once eviction has run, as for
//   CreateStandalone(allow_uncharged=false), and otherwise it is refused.
//   Without a handle it is dropped.
struct S3FifoHandle : public Cache::Handle {
  // A key of length 16 (every block cache key) is stored as its bijective
  // 128-bit hash, as in HyperClockCache: exact, and reversible with the
  // shard's seed. Any other key is stored as [0] = its 64-bit hash and
  // [1] = a HeapKey copy of its bytes (kHeapKey). Read through GetHash.
  char key_data[16];

  // External refs (the cache itself is not counted) in the low 15 bits, plus
  // kInCache.
  std::atomic<uint16_t> state{0};
  static constexpr uint16_t kRefMask = (uint16_t{1} << 15) - 1;

  // Whether this entry is referenced by the hash table.
  static constexpr uint16_t kInCache = uint16_t{1} << 15;

  // First entry of the chain of home (slot index - kFirstSlot), the home whose
  // slot this is; 0 when that chain is empty (slot 0 is a queue head and never
  // holds an entry). It belongs to that home, not to the entry stored in this
  // slot. Written under the shard lock.
  std::atomic<uint16_t> head{0};

  // Next entry in this entry's home chain, 0 at the end. Written under the
  // shard lock; left intact when the entry is unlinked, so a reader
  // standing on it can continue.
  std::atomic<uint16_t> chain_next{0};

  // Cache::ObjectPtr, through value() and set_value().
  char value_data[8];

  // Total charge (uint32_t) of an entry in a slot, through TotalCharge().
  // A standalone entry keeps its full charge in S3FifoStandaloneHandle.
  char charge_data[4];

  // Queue links, as slot indices. Under the shard lock. The same fields also
  // link the free list, the pending_/limbo_ chains of retired slots and the
  // retire chain (next only, written by the retiring thread without the
  // lock). 0 ends a chain, the free list or pending_; kNoLink means not
  // linked in any queue.
  uint16_t next = 0;
  uint16_t prev = 0;

  // The entry's helper, as an index into the cache's S3FifoHelperRegistry
  // (shared by all shards). Unused by a standalone entry, which keeps the
  // pointer (see GetHelper).
  uint8_t helper_index = 0;

  uint8_t flags = 0;
  enum Flags : uint8_t {
    // The slot holds an entry, or one retired and not yet reclaimed.
    kOccupied = 1 << 0,
    kHeapKey = 1 << 1,
    // Not in the table: an S3FifoStandaloneHandle allocated on its own,
    // freed on its last Release.
    kStandalone = 1 << 2,
  };

  // Whether the entry is on its shard's aside list. Written under the
  // shard lock; read by a lock-free last Release, which then flags the
  // shard (see S3FifoCacheShard::aside_unpinned_).
  std::atomic<uint8_t> aside{0};

  struct HeapKey {
    size_t size;
    char data[1];
  };

  // For HandleImpl concept. Only [0] is meaningful for every key.
  UniqueId64x2 GetHash() const {
    UniqueId64x2 hash;
    std::memcpy(&hash, key_data, sizeof(hash));
    return hash;
  }
  // GetHash()[0], without copying [1].
  uint64_t HashLow() const {
    uint64_t word;
    std::memcpy(&word, key_data, sizeof(word));
    return word;
  }
  void SetHash(const UniqueId64x2& hash) {
    std::memcpy(key_data, &hash, sizeof(hash));
  }

  Cache::ObjectPtr value() const {
    Cache::ObjectPtr v;
    std::memcpy(&v, value_data, sizeof(v));
    return v;
  }
  void set_value(Cache::ObjectPtr v) { std::memcpy(value_data, &v, sizeof(v)); }

  // The charge of an entry in a slot; for any entry, use TotalCharge.
  uint32_t SlotCharge() const {
    uint32_t charge;
    std::memcpy(&charge, charge_data, sizeof(charge));
    return charge;
  }
  void SetSlotCharge(uint32_t charge) {
    std::memcpy(charge_data, &charge, sizeof(charge));
  }
  inline size_t TotalCharge() const;
  // A standalone entry's helper is its own; any other is looked up in the
  // registry of the cache the entry belongs to.
  inline const Cache::CacheItemHelper* GetHelper(
      const S3FifoHelperRegistry& registry) const;

  bool HasRefs() const {
    return (state.load(std::memory_order_relaxed) & kRefMask) > 0;
  }
  bool InCache() const {
    return (state.load(std::memory_order_relaxed) & kInCache) != 0;
  }
  bool IsStandalone() const { return (flags & kStandalone) != 0; }
  bool HasHeapKey() const { return (flags & kHeapKey) != 0; }
  const HeapKey* heap_key() const {
    assert(HasHeapKey());
    // The pointer is kept in the bytes of hash [1] (see SetKey).
    const HeapKey* key = nullptr;
    std::memcpy(&key, key_data + sizeof(uint64_t), sizeof(const HeapKey*));
    return key;
  }
  // The key of an entry whose key is not 16 bytes long. 16-byte keys need
  // the seed: see S3FifoCacheShard::KeyOf.
  Slice key() const { return Slice(heap_key()->data, heap_key()->size); }

  // Runs the deleter. The slot itself may still be read by a concurrent
  // Lookup, so it is retired separately (see Retire).
  void FreeValue(const S3FifoHelperRegistry& registry,
                 MemoryAllocator* allocator) {
    const Cache::CacheItemHelper* helper = GetHelper(registry);
    assert(helper);
    if (helper->del_cb) {
      helper->del_cb(value(), allocator);
    }
  }

  // Metadata charged per entry: the heap copy of a key that is not 16 bytes
  // long. The slot is charged with the table (GetTableMetaCharge).
  size_t CalcuMetaCharge(
      CacheMetadataChargePolicy metadata_charge_policy) const {
    if (metadata_charge_policy != kFullChargeCacheMetadata || !HasHeapKey()) {
      return 0;
    }
    return sizeof(HeapKey) - 1 + heap_key()->size;
  }

  size_t GetCharge(CacheMetadataChargePolicy metadata_charge_policy) const {
    const size_t total_charge = TotalCharge();
    if (total_charge == 0) {
      // A standalone entry CreateStandalone left uncharged: no charge at
      // all, not even for its heap key.
      return 0;
    }
    size_t meta_charge = CalcuMetaCharge(metadata_charge_policy);
    assert(total_charge >= meta_charge);
    return total_charge - meta_charge;
  }
};
static_assert(sizeof(S3FifoHandle) == 42 && alignof(S3FifoHandle) == 2,
              "three slots and 2 bytes of padding fill a 128-byte group");
static_assert(std::atomic<uint16_t>::is_always_lock_free &&
                  std::atomic<uint8_t>::is_always_lock_free,
              "a slot's atomics must not need a lock");
static_assert(sizeof(S3FifoHandle::HeapKey*) <= sizeof(uint64_t),
              "a heap-key pointer must fit in hash [1]");
static_assert(sizeof(Cache::ObjectPtr) <= sizeof(S3FifoHandle::value_data),
              "a value pointer must fit in value_data");

// Three slots per 128-byte group. Slot i is s[i % 3] of group i / 3.
struct alignas(128) S3FifoSlotGroup {
  static constexpr size_t kSlots = 3;
  S3FifoHandle s[kSlots];
  char pad[2];
};
static_assert(sizeof(S3FifoSlotGroup) == 128, "a group is two cache lines");

// An entry outside the table (S3FifoHandle::kStandalone): from
// CreateStandalone, or an insert the table cannot hold. Never in a slot, so
// it can keep what a slot narrows: the helper pointer and the full charge.
struct S3FifoStandaloneHandle : public S3FifoHandle {
  const Cache::CacheItemHelper* helper = nullptr;
  size_t total_charge = 0;
};

// The CacheItemHelpers of one cache, so that a slot holds a one-byte index
// instead of an 8-byte pointer. Append-only: an index, once handed out, names
// its helper for the life of the cache, so readers (deleters, eviction
// callbacks, GetCacheItemHelper) index it without a lock. Entries with the
// 256th and later distinct helpers are slotless (see S3FifoHandle's Layout).
class S3FifoHelperRegistry {
 public:
  static constexpr size_t kMaxHelpers = 255;
  // IndexOf's answer once every index is taken.
  static constexpr uint8_t kFull = 255;

  // helper's index, registering it on first sight; kFull if it is new and
  // the registry is full. Thread-safe: lock-free once helper is registered.
  uint8_t IndexOf(const Cache::CacheItemHelper* helper);

  const Cache::CacheItemHelper* Get(uint8_t index) const {
    assert(index < kMaxHelpers);
    // Acquire pairs with the release store in IndexOf. Every reader also
    // reached the slot through the chain's or a handle's own ordering, so
    // this is belt and braces.
    return helpers_[index].load(std::memory_order_acquire);
  }

  size_t size() const { return count_.load(std::memory_order_relaxed); }

 private:
  // Open addressing over helper pointers, at most half full. Each cell holds
  // index + 1, 0 when empty; a cell, once set, never changes, so a probe
  // without the lock either finds helper or reaches an empty cell.
  static constexpr size_t kMapCells = 512;
  static size_t CellOf(const Cache::CacheItemHelper* helper) {
    return static_cast<size_t>(
        (reinterpret_cast<uintptr_t>(helper) * uint64_t{0x9E3779B97F4A7C15}) >>
        (64 - 9));  // 9 = log2(kMapCells)
  }
  // Probes from the home cell of helper; returns its index, or sets *cell to
  // the empty cell that ended the probe and returns kFull (kFull here means not
  // registered, not that the registry is full).
  uint8_t Probe(const Cache::CacheItemHelper* helper, size_t* cell) const;

  std::array<std::atomic<const Cache::CacheItemHelper*>, kMaxHelpers>
      helpers_{};
  std::array<std::atomic<uint8_t>, kMapCells> cells_{};
  std::atomic<size_t> count_{0};
  port::Mutex mutex_;
};

size_t S3FifoHandle::TotalCharge() const {
  if (IsStandalone()) {
    return static_cast<const S3FifoStandaloneHandle*>(this)->total_charge;
  }
  return SlotCharge();
}

const Cache::CacheItemHelper* S3FifoHandle::GetHelper(
    const S3FifoHelperRegistry& registry) const {
  if (IsStandalone()) {
    return static_cast<const S3FifoStandaloneHandle*>(this)->helper;
  }
  return registry.Get(helper_index);
}

// A single shard of S3FifoCache.
//
// Table. The design follows AutoHyperClockCache (cache/clock_cache.h):
// chained hashing whose chain entries live in the slot table (coalesced, with
// one chain per home, so chains never merge), over 42-byte slots (three per
// 128-byte group, see S3FifoHandle) that never move, in an address range
// reserved up front and mapped lazily.
// - A home is a hash bucket: HomeOf picks it from the low bits of hash[0].
// - Each slot has two independent parts. The home part (head) is the first
//   entry of the chain of home i = slot index - kFirstSlot, and always
//   belongs to that home. The entry part (everything else) holds whichever
//   entry was given the slot, of any home. An entry takes its home's slot if
//   it is free, otherwise any free slot; its home's chain links to it through
//   head and chain_next. The chain walk to an entry in its home's slot reads
//   one cache line; to an entry elsewhere, two. A hit on a slot at group
//   position 1 also touches the group's second line (see S3FifoHandle).
// - Slot 0 is the dummy head of the FIFO queue (next: oldest, prev: newest);
//   slot kFirstSlot + max_length_ is the dummy head of the aside list.
// - Growth (linear hashing): the table starts with kInitialLength homes and,
//   before an insert, adds homes one at a time while the entries in the
//   table (occupancy_) would exceed 3/5 of the homes. Each step splits one
//   chain by one more hash bit; entries are relinked, never moved. A Lookup
//   racing a split can miss a present key (never crash or loop). Measured in
//   db_bench (skewed reads, 32 threads, 4 GiB, the first 5 s from a cold
//   cache): at most 9 such misses per 44M-71M lookups, and at most 0.02% of the
//   lookups that overlapped a growth step, so a miss is not retried.
// - A removed or overwritten entry's value is freed at once, but its slot
//   returns to the free list only after epoch reclamation (MaybeReclaim, or
//   Reclaim when a slot is needed now), so a lock-free Lookup never sees a slot
//   change identity under it.
//
// Capacity. Under kFullChargeCacheMetadata the slot table counts against
// capacity: 128/3 bytes per home the table has grown to (a 128-byte group
// holds three; the total is rounded up, see TableChargeFor), 171 bytes per
// shard at construction, growing with the entries (about 71 bytes per entry
// at the 3/5 load factor) and shrinking back once the shard is emptied (by
// EraseUnRefEntries, by a capacity cut that evicts every entry, or at the next
// insert after Erase or eviction empties it). A shard needs well over that much
// capacity to hold any entry.
//
// Reservation. Each shard reserves address space for
// min(kMaxLength, max(2048, shard capacity / 64)) homes up front, in 128-byte
// groups of three slots (at most 2.7 MiB), using max_capacity instead of
// capacity when it is larger, and touches it as the table grows. Below the cap
// the reservation is about 2/3 of capacity. On Linux the reservation does not
// count against the commit limit unless vm.overcommit_memory=2; on Windows it
// is committed up front. If a shard cannot reserve it, MakeSharedCache returns
// nullptr.
// Below the cap the table holds about one entry per 107 bytes of reserved
// capacity.
// The cap: slot indexes are 16 bits, so a shard holds at most kMaxLength
// (65532) homes, about 39K entries at the 3/5 load factor, whatever its
// capacity. A shard is useful up to about 39K times its average entry charge,
// e.g. about 154 MiB of 4 KiB entries or 19 MiB of 512-byte entries; past that,
// new keys are also evicted by occupancy. The default num_shard_bits keeps
// shards at most kMaxShardSize, which fits 4 KiB average entries; caches of
// smaller entries should raise num_shard_bits (see "Shard size" at the top of
// this file). Separately, below the cap, the table never grows past its
// reservation, made for max(capacity, max_capacity): a SetCapacity raise beyond
// that does not grow it, so new keys are also evicted by occupancy once it
// fills, for 4 KiB entries past about 38x max(capacity, max_capacity).
//
// Example. Starting from 4 homes, A, B, D and C are inserted in that order.
// By the low bits of their hashes, A and B belong to home 0 (B's third bit
// is 1), D to home 1 and C to home 3. B finds home 0's slot taken by A and
// lands in slot 2. Before D, the table grows to 5 homes: home 0 splits into
// homes 0 and 4, moving B to home 4 without moving its slot. D finds home 1's
// slot taken by B and lands in slot 5. Inserting C grows the table twice, to
// 7 homes, and C takes its home's slot. The result:
//
//   slot  home part (head)   entry part
//    0    FIFO queue head
//    1    home 0 -> slot 1   A
//    2    home 1 -> slot 5   B (a home-4 entry)
//    3    home 2 -> none     free
//    4    home 3 -> slot 4   C
//    5    home 4 -> slot 2   D (a home-1 entry)
//    6    home 5 -> none     free
//    7    home 6 -> none     free
//
//   FIFO queue: A -> B -> D -> C, in insertion order, unrelated to the slots.
class ALIGN_AS(CACHE_LINE_SIZE) S3FifoCacheShard final : public CacheShardBase {
 public:
  // NOTE: the eviction_callback ptr is saved, as it is assumed to be kept
  // alive in Cache. max_upper_hash_bits is unused: homes come from the low
  // bits of the hash, sharding from the high ones. helpers is the cache's
  // helper registry, shared by its shards; a shard built alone (tests) gets
  // its own.
  S3FifoCacheShard(size_t capacity, bool strict_capacity_limit,
                   bool use_adaptive_mutex,
                   CacheMetadataChargePolicy metadata_charge_policy,
                   int max_upper_hash_bits, MemoryAllocator* allocator,
                   const Cache::EvictionCallback* eviction_callback,
                   size_t max_capacity,
                   std::shared_ptr<S3FifoHelperRegistry> helpers = nullptr);
  ~S3FifoCacheShard();
  S3FifoCacheShard(const S3FifoCacheShard&) = delete;
  S3FifoCacheShard& operator=(const S3FifoCacheShard&) = delete;
  S3FifoCacheShard(S3FifoCacheShard&&) = delete;
  S3FifoCacheShard& operator=(S3FifoCacheShard&&) = delete;

 public:  // Type definitions expected as parameter to ShardedCache
  using HandleImpl = S3FifoHandle;
  using HashVal = UniqueId64x2;
  using HashCref = const HashVal&;

 public:  // Function definitions expected as parameter to ShardedCache
  static constexpr size_t kInlineKeySize = 16;

  // A 16-byte key gets HyperClockCache's bijective hash; any other key a
  // seeded 128-bit hash. Sharding uses the upper half of [0], homes the
  // lower bits of [0].
  static inline HashVal ComputeHash(const Slice& key, uint32_t seed) {
    HashVal out;
    if (key.size() == kInlineKeySize) {
      HashVal in;
      // NOTE: endian dependence, as in HyperClockCache
      std::memcpy(&in, key.data(), kInlineKeySize);
      BijectiveHash2x64(in[1], in[0] ^ seed, &out[1], &out[0]);
    } else {
      Hash2x64(key.data(), key.size(), seed, &out[1], &out[0]);
    }
    return out;
  }
  static inline uint32_t HashPieceForSharding(HashCref hash) {
    return Upper32of64(hash[0]);
  }

  // The seed ComputeHash was called with, needed to recover 16-byte keys.
  // Set once by S3FifoCache before use; standalone shards in tests use 0.
  void SetHashSeed(uint32_t seed) { hash_seed_ = seed; }

  // Sets the capacity and evicts, by the policy, until usage fits or nothing
  // unpinned remains. The slot table cannot grow past its reservation,
  // made at construction for max(capacity, max_capacity): raising capacity
  // beyond what it holds does not grow it, so once the table is full new
  // keys are also evicted by occupancy, as in HyperClockCache.
  void SetCapacity(size_t capacity);

  // Set the flag to reject insertion if the cache is full.
  void SetStrictCapacityLimit(bool strict_capacity_limit);

  // Like Cache methods, but with an extra "hash" parameter.
  // priority is ignored: FIFO has no priority pools.
  Status Insert(const Slice& key, HashCref hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                S3FifoHandle** handle, Cache::Priority priority);

  S3FifoHandle* CreateStandalone(const Slice& key, HashCref hash,
                                 Cache::ObjectPtr obj,
                                 const Cache::CacheItemHelper* helper,
                                 size_t charge, bool allow_uncharged);

  // helper, create_context, priority and stats are ignored: with no secondary
  // cache configured this is a primary-only lookup. Secondary promotion is
  // handled by CacheWithSecondaryAdapter above the shard, as with LRU.
  // A hit moves nothing.
  S3FifoHandle* Lookup(const Slice& key, HashCref hash,
                       const Cache::CacheItemHelper* helper,
                       Cache::CreateContext* create_context,
                       Cache::Priority priority, Statistics* stats);

  // Lookup takes no lock. Release and Ref take none either, unless Release
  // drops the last ref of an entry it must erase (erase_if_last_ref) or free
  // (detached or standalone). Being over capacity never makes a Release
  // erase, as in HyperClockCache: the next insert evicts. Inserts,
  // erases, eviction, SetCapacity and the bulk operations take mutex_; the
  // getters (GetUsage, GetPinnedUsage, GetOccupancyCount,
  // GetTableAddressCount) do not.
  bool Release(S3FifoHandle* handle, bool useful, bool erase_if_last_ref);
  bool Ref(S3FifoHandle* handle);
  void Erase(const Slice& key, HashCref hash);

  size_t GetUsage() const;

  // Without mutex_, as HyperClockCache does: scans the slot table, so the
  // shard lock is never held for it. Exact at quiescence. The scan is
  // O(homes in use), and the rocksdb.block-cache-pinned-usage property runs
  // it with the DB mutex held, once per column family in its aggregated
  // form; HyperClockCache has the same cost.
  size_t GetPinnedUsage() const;
  size_t GetOccupancyCount() const;

  // Homes in use: the number of homes the table has grown to.
  size_t GetTableAddressCount() const;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state);

  void EraseUnRefEntries();

  // The key of h, using buf as storage for a 16-byte key.
  Slice KeyOf(const S3FifoHandle* h, UniqueId64x2* buf) const;

  // The registry h's helper index refers to.
  const S3FifoHelperRegistry& helpers() const { return *helpers_; }

 public:  // other function definitions
  // The FIFO queue. Walk it with TEST_Next.
  void TEST_GetFIFOList(S3FifoHandle** fifo);
  S3FifoHandle* TEST_Next(const S3FifoHandle* h) const { return Next(h); }
  // The slot index of h, counted from slot 0 (the FIFO queue head).
  size_t TEST_IndexOf(const S3FifoHandle* h) const { return IndexOf(h); }

  // Number of entries in the FIFO queue, for unit tests. Takes mutex_.
  size_t TEST_GetFIFOSize();

  // Times the free list was rebuilt from the table (RebuildFreeSlots), for
  // unit tests. Takes mutex_.
  uint64_t TEST_GetFreeSlotRebuilds();

  // Whether the free list holds exactly the unoccupied slots below the used
  // homes, each once, with consistent links. Takes mutex_.
  bool TEST_FreeListMatchesTable();

  size_t TEST_GetTableOccupancyCount() const;

  // Entries on the aside list, for unit tests.
  size_t TEST_GetAsideSize();

  // Test-only: lets a test hold the shard lock to show that hits never take
  // it.
  DMutex& TEST_GetMutex() const { return mutex_; }

  // Eviction and table counters, for tests and debugging. The first six
  // count since construction; the last two are taken now.
  struct Stats {
    // Entries evicted only to keep the table's load factor once it is at
    // its reservation (new keys evicted by occupancy, not charge).
    uint64_t occupancy_evictions = 0;

    // Pinned queue heads an eviction pass moved to the tail, including heads
    // whose claim CAS lost to a concurrent Lookup.
    uint64_t pinned_rotations = 0;

    // Pinned entries an eviction pass moved to the aside list.
    uint64_t set_aside = 0;

    // Entries handed out outside the table because it had no free slot
    // (strict or not; see "Entries the table cannot hold" in the S3FifoCache
    // comment).
    uint64_t table_full_standalone = 0;

    // Entries handed out outside the table because the helper registry was
    // full (S3FifoHelperRegistry::kMaxHelpers distinct helpers already).
    uint64_t helper_overflow_standalone = 0;

    // Entries handed out outside the table because their total charge does
    // not fit a slot's 32 bits.
    uint64_t large_charge_standalone = 0;

    // Entries on the aside list.
    size_t aside_entries = 0;

    // Retired slots stamped and waiting for in-flight Lookups.
    size_t limbo_slots = 0;
  };

  // Takes mutex_; walks the aside list and limbo_.
  Stats GetStats();

  // Whether the slot table could not be reserved at construction. Such a
  // shard holds nothing and must not be used; MakeSharedCache returns
  // nullptr instead of a cache with one.
  bool ReservationFailed() const { return slots_base_ == nullptr; }

  // Prints the slot reservation (homes per shard); max_length_ is fixed at
  // construction, so no lock is needed.
  void AppendPrintableOptions(std::string& str) const;

  // Metadata charge of a table of `homes` homes: 128/3 bytes per home, the
  // share of a 128-byte group, rounded up for the whole table.
  static size_t TableChargeFor(size_t homes) {
    return (homes * sizeof(S3FifoSlotGroup) + S3FifoSlotGroup::kSlots - 1) /
           S3FifoSlotGroup::kSlots;
  }

  // The table reserves capacity / kMinAvgCharge homes (at least
  // kMinMaxLength, at most kMaxLength). A workload averaging less than about
  // 5/3 x kMinAvgCharge per entry (the 3/5 load factor) fills it before
  // capacity and is then also evicted by occupancy. HyperClockCache's
  // min_avg_entry_charge is per entry; this is per home. Above kMaxLength
  // homes (shards over 4 MiB) the cap, not kMinAvgCharge, sets the table
  // size.
  static constexpr size_t kMinAvgCharge = 64;
  // The fewest homes a shard reserves (the 2048-home floor in the class
  // comment's Reservation paragraph).
  static constexpr size_t kMinMaxLength = 2048;

  // Capped at kMaxLength (see the class comment's Reservation paragraph).
  static size_t MaxLengthFor(size_t capacity) {
    return std::min(std::max(kMinMaxLength, capacity / kMinAvgCharge),
                    kMaxLength);
  }

  // Slot indexes are 16 bits and kNoLink is 0xFFFF, so every slot index,
  // up to the aside head at kFirstSlot + max_length_, must be below it.
  static constexpr uint16_t kNoLink = 0xFFFF;
  // 65532 keeps the aside head (kFirstSlot + kMaxLength) below kNoLink
  // (checked by the static_assert below), with room for a second queue head.
  static constexpr size_t kMaxLength = 65532;

 private:
  friend class S3FifoCache;

  // Slot 0 is the queue dummy head; homes start at kFirstSlot.
  static constexpr uint16_t kFifoSlot = 0;
  static constexpr uint16_t kFirstSlot = 1;
  static_assert(kFirstSlot + kMaxLength < kNoLink,
                "the aside head, the highest slot index, is below kNoLink");

  // Homes the table starts with and the load factor it grows at: more
  // entries in the table (occupancy_) than kGrowNumerator / kGrowDenominator
  // of the homes grows the table by one home, until max_length_. A table out of
  // free slots (held by entries erased while pinned, or by retired slots not
  // yet reclaimed) also grows. kInitialLength = 4 keeps an empty shard's table
  // charge at 171 bytes (see Capacity in the class comment); 3/5 is
  // AutoHyperClockCache's load factor (kMaxLoadFactor = 0.60).
  static constexpr size_t kInitialLength = 4;
  static constexpr size_t kGrowNumerator = 3;
  static constexpr size_t kGrowDenominator = 5;

  S3FifoHandle* Slot(uint16_t index) const {
    return &slots_base_[index / S3FifoSlotGroup::kSlots]
                .s[index % S3FifoSlotGroup::kSlots];
  }
  // The slot index of table position i: a home below max_length_, or
  // max_length_ itself for the aside slot. MaxLengthFor keeps it below
  // kNoLink.
  uint16_t SlotIndexAt(size_t i) const {
    assert(i <= max_length_);
    return static_cast<uint16_t>(kFirstSlot + i);
  }
  uint16_t IndexOf(const S3FifoHandle* h) const {
    const size_t offset =
        static_cast<size_t>(reinterpret_cast<const char*>(h) -
                            reinterpret_cast<const char*>(slots_base_));
    const size_t group = offset / sizeof(S3FifoSlotGroup);
    const size_t pos = offset % sizeof(S3FifoSlotGroup) / sizeof(S3FifoHandle);
    assert(pos < S3FifoSlotGroup::kSlots);
    return static_cast<uint16_t>(group * S3FifoSlotGroup::kSlots + pos);
  }
  S3FifoHandle* Next(const S3FifoHandle* h) const { return Slot(h->next); }
  S3FifoHandle* Prev(const S3FifoHandle* h) const { return Slot(h->prev); }
  // The FIFO queue: oldest at the head, newest at the tail. It contains all
  // in-cache items, pinned and unpinned, except those set aside.
  S3FifoHandle* fifo() const { return Slot(kFifoSlot); }
  S3FifoHandle* aside() const { return Slot(aside_slot_); }

  // Home index of hash for the given linear-hashing length info.
  static size_t HomeOf(uint64_t length_info, HashCref hash);
  static bool KeyMatches(const S3FifoHandle* h, const Slice& key,
                         HashCref hash);

  // Lock-free: the entry with key, or nullptr.
  S3FifoHandle* FindInTable(const Slice& key, HashCref hash) const;

  // Under mutex_: links e at the head of its home chain, publishing it.
  void LinkIntoChain(S3FifoHandle* e);

  // Under mutex_: unlinks e from its home chain, leaving e's chain_next.
  void UnlinkFromChain(S3FifoHandle* e);

  // Under mutex_: one linear-hashing step, splitting one home's chain.
  void GrowOneHome();

  // Under mutex_: GrowOneHome, adding the step's metadata charge to usage_.
  void GrowOneHomeCharged();

  // Under mutex_: a free slot, preferring home's slot; nullptr if none. It
  // leaves the free list with next == prev == kNoLink.
  S3FifoHandle* AllocateSlot(size_t home);

  // Under mutex_: links the unoccupied slot idx at the head of the free list.
  void PushFreeSlot(uint16_t idx);

  // Under mutex_: unlinks the free slot h from the free list.
  void UnlinkFreeSlot(S3FifoHandle* h);

  // Under mutex_: whether inserting one more entry would grow the table.
  bool WouldGrowOnNewInsert() const;

  // Under mutex_: once no entry is in the table, returns it to its initial
  // homes (and their charge), or to the fewest that still cover every slot
  // in use. Heap keys of reclaimed slots go to heap_keys, as in Reclaim.
  void ShrinkTableIfEmpty(std::vector<S3FifoHandle::HeapKey*>* heap_keys);

  // Under mutex_: the free list := the unoccupied slots of homes
  // 0..used_length_-1, after a shrink drops the homes above them.
  void RebuildFreeSlots();

  // Whether an entry of total_charge cannot fit even in an empty shard, so
  // evicting for it would only empty the shard.
  bool NeverFits(size_t total_charge) const;
  size_t GetTableMetaCharge() const;

  // Metadata charge of the next growth step a new key's insert would take (one
  // home), or 0 for an overwrite, a table that need not grow, or uncharged
  // metadata. Called once per step.
  size_t TableGrowthChargeOnInsert(const Slice& key, HashCref hash) const;

  // The metadata charge of one more home: 42 or 43 bytes (128/3 per home,
  // see TableChargeFor), or 0 when metadata is not charged.
  size_t HomeCharge() const;

  // Whether the table has grown past the length an empty table shrinks to.
  bool TableAboveInitialLength() const {
    return used_length_ > std::min(kInitialLength, max_length_);
  }
  // Insert an item into the hash table and append it to the FIFO queue.
  // Older unpinned items are evicted as necessary. Frees heap_key on a
  // non-OK status.
  Status InsertItem(const Slice& key, HashCref hash,
                    S3FifoHandle::HeapKey* heap_key, Cache::ObjectPtr value,
                    const Cache::CacheItemHelper* helper, size_t total_charge,
                    S3FifoHandle** handle);

  // An entry outside the table, as CreateStandalone returns.
  static S3FifoStandaloneHandle* NewStandalone(
      HashCref hash, S3FifoHandle::HeapKey* heap_key, Cache::ObjectPtr value,
      const Cache::CacheItemHelper* helper, size_t total_charge);
  static void FreeStandalone(S3FifoHandle* h);

  // Unlinks e from the queue or the aside list.
  void FIFO_Remove(S3FifoHandle* e);

  // Links e at the tail of the FIFO queue; neither function touches state.
  void FIFO_Append(S3FifoHandle* e);

  // FIFO_Append for either list: the FIFO queue or the aside list.
  void LinkAtTail(S3FifoHandle* queue, S3FifoHandle* e);

  // Moves a linked entry to the tail of queue (the FIFO queue or aside)
  // without changing any charge.
  void MoveToTail(S3FifoHandle* e, S3FifoHandle* queue);

  // Checks up to kAsideChecksPerEviction entries at the head of the aside
  // list: an unpinned one returns to the tail of the FIFO queue, a pinned
  // one to the aside tail.
  void ReturnUnpinnedAside();

  // Visits up to kAsideReturnChunk entries at the head of the aside list,
  // like ReturnUnpinnedAside. Returns true once it has visited last, the
  // aside tail when the walk began.
  bool ReturnUnpinnedAsideChunk(S3FifoHandle* last);

  // Moves a linked, pinned entry to the aside tail.
  void SetAside(S3FifoHandle* e);

  // Release of the last ref when it must erase or free: Release's locked path.
  // Takes mutex_ itself, so the caller must not hold it (the name does not mean
  // the lock is held, as "Under mutex_" does elsewhere).
  bool ReleaseLocked(S3FifoHandle* e, bool erase_if_last_ref);

  // Erase or overwrite: clears kInCache in one step, unlinks e from the
  // queue and the table. Returns true when no ref held it, so the
  // caller frees it; otherwise it stays charged until its last Release.
  bool Detach(S3FifoHandle* e);

  // Under mutex_: removes a claimed or detached entry from the table.
  void RemoveFromTable(S3FifoHandle* e);

  // Pinned entries one pass over the queue rotates (moves to the tail)
  // before it sets the next ones aside. A pass meets each pinned entry once,
  // and this is far more than a shard normally has pinned at once, so only
  // a mostly pinned shard sets entries aside.
  static constexpr size_t kPinnedRotationsPerPass = 64;
  // Aside entries each eviction checks first (ReturnUnpinnedAside): a small
  // constant cost per eviction that slowly cycles the list. A burst of
  // unpinned aside entries is returned by the chunked walk in EvictNeed.
  static constexpr int kAsideChecksPerEviction = 2;

  // Aside entries an eviction that falls short returns between two passes
  // over the queue (see EvictNeed). Chunks let the walk stop as soon as the
  // need is met, instead of relinking the whole list first.
  static constexpr size_t kAsideReturnChunk = 64;

  // Free some space following strict FIFO policy until enough space
  // to hold (usage_ + charge) is freed or no unpinned entry remains.
  // Pinned entries the scan meets move to the tail, so long-lived pins
  // are not re-walked by every eviction. Terminates after a single pass
  // even if everything is pinned.
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.
  void EvictFromFIFO(size_t charge, autovector<S3FifoHandle*>* deleted);

  // What one eviction call must free, and what it has freed so far. Charge
  // and entries are counted separately, so zero-charge entries cannot make
  // an entry-count target evict without limit.
  struct EvictionData {
    size_t need_charge = 0;
    size_t need_count = 0;
    size_t freed_charge = 0;
    size_t freed_count = 0;
    bool Done() const {
      return freed_charge >= need_charge && freed_count >= need_count;
    }
  };

  // One eviction attempt for data: up to kAsideChecksPerEviction aside checks,
  // then one EvictFromQueue pass. If that falls short and an aside entry may
  // have been unpinned, aside entries are returned kAsideReturnChunk at a time,
  // with a pass after each chunk, until data is Done or every aside entry
  // present at the start was visited. May return with data not Done.
  void EvictNeed(EvictionData* data, autovector<S3FifoHandle*>* deleted);

  // Under mutex_: takes a claimed entry out of the table and the usage, and
  // records it in data and deleted.
  void EvictClaimed(S3FifoHandle* e, EvictionData* data,
                    autovector<S3FifoHandle*>* deleted);

  // One sweep over the queue from the head: unpinned entries are evicted,
  // pinned entries move to the tail, so long-lived pins are not re-walked
  // by every eviction, or aside once the pass has rotated
  // kPinnedRotationsPerPass of them. Stops once data is Done. Not thread
  // safe - call with mutex_ held.
  void EvictFromQueue(EvictionData* data, autovector<S3FifoHandle*>* deleted);

  // Runs the eviction callback (or deleter) for evicted entries, overwritten
  // entries no longer referenced, and a handle-less insert dropped instead of
  // cached; frees standalone ones and retires the slots of the rest.
  void NotifyEvicted(const autovector<S3FifoHandle*>& evicted_handles);

  // Epoch-based reclamation for lock-free Lookup. A slot's life: free -> in
  // use -> retired (pending_, chained through next) -> stamped (limbo_, with
  // epoch_) -> free once no reader announces this shard with an epoch at or
  // before the stamp. Every thread owns one ThreadReader for the whole process
  // (see s3fifo_cache.cc). A Lookup announces this shard and its current epoch
  // there (then a seq_cst fence) before reading the table, and clears it after.
  // Values are not deferred: deleters and eviction callbacks run as soon as an
  // entry leaves the cache.
  // Queues retired slots chained through next, values already released,
  // for StampPending. Safe without mutex_.
  void Retire(S3FifoHandle* first, S3FifoHandle* last);
  void RetireAll(const autovector<S3FifoHandle*>& handles);
  struct RetiredBatch {
    uint64_t epoch;

    // Stamping order, from 1.
    uint64_t seq;

    // Slot index of the first retired slot, chained through next.
    uint16_t first;
  };

  // Called by each insert and erase. Every kReclaimInterval calls, stamps
  // queued slots and applies the last reader scan; once per
  // ReaderWorkInterval() calls it also asks for a new scan. Returns whether the
  // caller must scan the readers (ScanReaders) once it has released mutex_: the
  // scan's cost grows with the threads in the process, so this regular scan is
  // kept out of the lock. Reclaim, on an insert that finds no free slot, and
  // ShrinkTableIfEmpty still scan under mutex_. Needs mutex_. Heap keys of
  // reclaimed slots are added to heap_keys, for the caller to free after
  // releasing mutex_.
  bool MaybeReclaim(std::vector<S3FifoHandle::HeapKey*>* heap_keys);

  // Stamps queued slots, scans the readers and frees every batch it can,
  // all under mutex_, for the rare callers that need the slots now.
  void Reclaim(std::vector<S3FifoHandle::HeapKey*>* heap_keys);

  // Under mutex_: moves pending_ to limbo_, stamped with the current epoch.
  void StampPending();

  // Under mutex_: returns to the free list the batches up to max_seq
  // stamped before oldest, the oldest epoch a scan found announced. If one of
  // them must wait and the epoch has not advanced since the newest batch was
  // stamped, advances it, so that it waits only for Lookups already in flight.
  void FreeBatches(uint64_t max_seq, uint64_t oldest,
                   std::vector<S3FifoHandle::HeapKey*>* heap_keys);

  // The oldest epoch a Lookup in this shard announces, or UINT64_MAX. It
  // covers every batch stamped before the call. Without mutex_.
  uint64_t OldestAnnouncedEpoch() const;

  // Without mutex_: the scan MaybeReclaim asked for; its result is applied
  // by a later MaybeReclaim.
  void ScanReaders();
  static void FreeHeapKeys(const std::vector<S3FifoHandle::HeapKey*>& keys);

  // Every kReclaimInterval calls, MaybeReclaim stamps pending_ and, once a scan
  // result is in, FreeBatches reads each slot it frees (next link, flags, heap
  // key), usually cache misses, under mutex_. A short interval keeps that hold
  // within the time a DistributedMutex waiter spins before sleeping.
  static constexpr uint32_t kReclaimInterval = 16;

  // Calls to MaybeReclaim (inserts and erases) between two walks over every
  // reader: max(kReclaimInterval, min(readers / kReadersPerReaderTick,
  // homes / 8)). Tables under 128 homes therefore still walk every
  // kReclaimInterval calls. A locked call pays for about kReadersPerReaderTick
  // readers on average once the table has as many homes as there are readers; a
  // smaller table walks more often, because its retired slots must come back
  // before its free room runs out.
  size_t ReaderWorkInterval() const;
  static constexpr size_t kReadersPerReaderTick = 8;

  // A size_t written only under mutex_ and read without it by the getters
  // (GetUsage, GetPinnedUsage, GetOccupancyCount).
  // Writes are relaxed load + store, so the lock holder pays no atomic
  // read-modify-write.
  class LockedSize {
   public:
    explicit LockedSize(size_t v) : v_(v) {}
    size_t Get() const { return v_.load(std::memory_order_relaxed); }
    LockedSize& operator=(size_t v) {
      v_.store(v, std::memory_order_relaxed);
      return *this;
    }
    LockedSize& operator+=(size_t d) { return *this = Get() + d; }
    LockedSize& operator-=(size_t d) { return *this = Get() - d; }

   private:
    std::atomic<size_t> v_;
  };

  // Layout. Lookup and a Release that neither erases nor frees run without
  // mutex_ and write only the slot (state) and the caller's ThreadReader;
  // the last Release of an aside entry also sets aside_unpinned_, and
  // pending_ and scan_result_ are written without mutex_ too. What hits read
  // sits on lines that no hit writes:
  // 1. Rarely written: by SetCapacity, SetStrictCapacityLimit, a growth or
  //    shrink step and an epoch advance (inside some inserts, erases,
  //    EraseUnRefEntries and SetCapacity): from capacity_ through slots_base_.
  //    Hits read only epoch_, length_info_ and slots_base_ from this group;
  //    capacity_ and strict_capacity_limit_ are here because they are rarely
  //    written, not because hits read them.
  // 2. Written by every insert, mostly under mutex_: from usage_ on.
  // The slots are in their own mapping.

  // Set by the constructor's SetCapacity call.
  LockedSize capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  uint32_t hash_seed_;

  // The epoch Lookups announce. Advanced only by FreeBatches, under mutex_
  // (called from MaybeReclaim and from Reclaim). Starts at 1: 0 means "not
  // announced" in ThreadReader::epoch and "not scanned yet" in scan_result_.
  std::atomic<uint64_t> epoch_;

  // Linear-hashing length info of the homes in use (see HomeOf), encoded as
  // (threshold << 8) | min_shift, with
  // used_length_ == (1 << min_shift) + threshold (see UsedLengthToLengthInfo).
  // Written under mutex_ by GrowOneHome and ShrinkTableIfEmpty; a shrink
  // happens only while the table is empty.
  std::atomic<uint64_t> length_info_;

  S3FifoSlotGroup* slots_base_;

  // Charge counted against capacity_: entries in the table, referenced entries
  // outside it (detached while pinned, and standalone; see
  // detached_pinned_usage_), and under kFullChargeCacheMetadata the table's own
  // charge (GetTableMetaCharge).
  ALIGN_AS(CACHE_LINE_SIZE) LockedSize usage_;

  // The slot table: kFirstSlot + max_length_ + 1 slots (the last is the
  // aside head) in groups of three, mapped lazily and zero-initialized, so a
  // fresh slot has head == 0.
  TypedMemMapping<S3FifoSlotGroup> slots_;

  // Homes in use (kFirstSlot + used_length_ slots), and the most there can
  // be.
  size_t used_length_;
  size_t max_length_;

  // Entries in the table. Read without mutex_ by GetOccupancyCount.
  LockedSize occupancy_;

  // Head of the free list, 0 when empty. The free list holds exactly the
  // unoccupied slots below the used homes, doubly linked through their
  // next and prev (unused while a slot is free; Lookup never reads them), so
  // AllocateSlot takes the home slot or the head in O(1) and reclamation
  // never rebuilds it. Under mutex_.
  uint16_t free_head_ = 0;

  // Times RebuildFreeSlots ran (only when an empty table shrinks). Under
  // mutex_.
  uint64_t free_slot_rebuilds_ = 0;

  // Counters GetStats reports; aside_entries and limbo_slots stay 0 here.
  // Under mutex_.
  Stats stats_;

  // Charge of referenced entries outside the table: detached by Erase or
  // overwrite while pinned, or standalone.
  LockedSize detached_pinned_usage_;

  // Aside list: pinned entries an eviction pass met after rotating
  // kPinnedRotationsPerPass others, so that a mostly pinned shard is not
  // re-walked by every insert. An aside entry is still in the table and
  // charged as before; ReturnUnpinnedAside relinks it into the FIFO queue
  // once it is unpinned. Its dummy head is slot kFirstSlot + max_length_, just
  // past the last home the table can grow to.
  uint16_t aside_slot_;

  // Set when an aside entry may have become unpinned: by its last Release,
  // or by SetAside if that Release raced with it. An eviction that falls
  // short then walks the aside list and returns its unpinned entries to their
  // queues (see EvictNeed), so a released burst of pins does not stay out of
  // eviction's reach. A lock-free Release sets it with a release store, which
  // EvictNeed's acquire exchange pairs with; the other stores are made under
  // mutex_ and are relaxed.
  std::atomic<bool> aside_unpinned_;

  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;

  // From Cache, needed for delete
  MemoryAllocator* const allocator_;

  // A reference to Cache::eviction_callback_
  const Cache::EvictionCallback& eviction_callback_;

  // The cache's helper registry, shared by its shards.
  const std::shared_ptr<S3FifoHelperRegistry> helpers_;

  // Slots retired since the last StampPending (run by MaybeReclaim every
  // kReclaimInterval calls, and by Reclaim), as an index chained through next,
  // 0 when none. Pushed without mutex_.
  std::atomic<uint16_t> pending_;

  // Batches stamped so far. Under mutex_.
  uint64_t stamped_seq_;

  // The batches up to this seq are covered by the scan MaybeReclaim asked
  // for and not yet applied; 0 if none. Under mutex_.
  uint64_t scan_covers_seq_;

  // That scan's OldestAnnouncedEpoch, 0 until ScanReaders stores it.
  std::atomic<uint64_t> scan_result_;

  // Stamped batches waiting for in-flight Lookups, oldest first.
  std::vector<RetiredBatch> limbo_;

  // Calls to MaybeReclaim since its last kReclaimInterval tick, when it stamps
  // pending_. Under mutex_.
  uint32_t reclaim_ticks_;

  // Calls counted toward ReaderWorkInterval(), and whether a reader scan
  // is due. Under mutex_.
  size_t reader_ticks_;
  bool scan_due_;
};

// S3FifoCache, the public Cache. The S3FifoCacheOptions comment in
// include/rocksdb/cache.h is the short user contract; this is its detail.
// - A Lookup neither promotes an entry nor protects it from a scan (see
//   Eviction at the top of this file).
// - Unlike LRUCache: an entry holds at most 32767 references (kRefMask); beyond
//   that Lookup misses and Ref returns false. It cannot be the primary cache of
//   NewTieredCache.
// - Entries the table cannot hold are not cached: slotless ones (see Layout at
//   S3FifoHandle: a charge above 4 GiB - 1, or more than 255 distinct
//   CacheItemHelpers in one cache), and any entry that finds no free slot once
//   eviction and growth have run (every slot held by an entry in the table, an
//   entry erased or overwritten while pinned, or a retired slot not yet
//   reclaimed, and the table unable to grow: at its reservation or, under
//   strict_capacity_limit, without room for the growth's metadata charge).
//   Inserted with a handle, the caller gets a handle that Lookup cannot find,
//   charged until it is released (an overwritten key is detached:
//   OkOverwritten), unless its charge would overflow usage or, under
//   strict_capacity_limit, it does not fit once unpinned entries are evicted:
//   then the insert is refused (MemoryLimit), and an overwritten key is not
//   detached (eviction for the insert may still remove it, unless pinned). So
//   under strict_capacity_limit it is accepted only if it fits, as
//   CreateStandalone(allow_uncharged=false) admits entries, and no insert takes
//   usage above capacity (usage can still exceed it after a SetCapacity cut
//   that pinned entries survive, or when the limit is turned on over capacity).
//   Without a handle it is dropped, strict or not.
// - Not charged to capacity: process-wide, one cache line per thread for the
//   most threads that have used an S3FifoCache at the same time, kept for the
//   life of the process (ThreadReader, see s3fifo_cache.cc).
// - Reservation (see Reservation at S3FifoCacheShard): at least 2048 homes
//   (about 86 KiB) and at most about 2.7 MiB per shard, e.g. about 171 MiB for
//   the 64 shards the default num_shard_bits gives caches of 256 MiB to 8 GiB.
//   max_capacity is a whole-cache value, split across shards like capacity; set
//   it to the largest capacity SetCapacity may raise the cache to. On Linux
//   with the default overcommit a larger reservation costs only address space
//   until the table grows into it; on Windows it is committed up front, under
//   vm.overcommit_memory=2 it counts toward CommitLimit, and a 32-bit process
//   has only 2-4 GiB of address space in total. It has no effect once a shard's
//   reservation is at the 65532-home cap, which shards of 4 MiB or more reach
//   (any cache of 4 MiB or more with the default num_shard_bits).
// - SetCapacity does not change the shard count, and the default num_shard_bits
//   is chosen from capacity alone (DefaultShardBits). If SetCapacity may raise
//   the capacity a lot later, set num_shard_bits for the largest capacity, so
//   that shards stay at most about 128 MiB for 4 KiB entries.
// - Each shard's table is its own memory mapping, so on Linux num_shard_bits of
//   16 or more (the automatic rule picks 16 or more above 4 TiB) can exceed
//   vm.max_map_count (65530 by default); a reservation then fails and
//   MakeSharedCache returns nullptr. It also returns nullptr for num_shard_bits
//   of 20 or more.
// - GetPinnedUsage takes no shard lock, so it runs under whatever locks its
//   caller holds; see S3FifoCacheShard::GetPinnedUsage for its cost.
// - Opt-in: reached only by constructing it (MakeSharedCache); no option string
//   or tool selects it, and it is not the default block cache.
class S3FifoCache final : public ShardedCache<S3FifoCacheShard> {
 public:
  explicit S3FifoCache(const S3FifoCacheOptions& opts);
  const char* Name() const override { return "S3FifoCache"; }
  ObjectPtr Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override;

  void ApplyToHandle(
      Cache* cache, Handle* handle,
      const std::function<void(const Slice& key, ObjectPtr obj, size_t charge,
                               const CacheItemHelper* helper)>& callback)
      override;

  // Bounds of a shard's capacity when num_shard_bits is left at -1 (see
  // "Shard size" at the top of this file).
  static constexpr size_t kMinShardSize = size_t{4} << 20;
  static constexpr size_t kMaxShardSize = size_t{128} << 20;

  // num_shard_bits for a cache of this capacity when it is left at -1.
  static int DefaultShardBits(size_t capacity);

  // Whether any shard failed to reserve its slot table (see MakeSharedCache).
  bool ReservationFailed();

  // The shards' GetStats, summed.
  S3FifoCacheShard::Stats GetStats();

  // Number of entries in the FIFO queues of all shards, for unit tests.
  size_t TEST_GetFIFOSize();

 private:
  // Shared with the shards, which may outlive this object's members: the
  // ShardedCache base destroys them after S3FifoCache's members.
  std::shared_ptr<S3FifoHelperRegistry> helpers_;
};

}  // namespace s3fifo_cache

using S3FifoCache = s3fifo_cache::S3FifoCache;
using S3FifoHandle = s3fifo_cache::S3FifoHandle;
using S3FifoCacheShard = s3fifo_cache::S3FifoCacheShard;
using S3FifoSlotGroup = s3fifo_cache::S3FifoSlotGroup;

}  // namespace ROCKSDB_NAMESPACE
