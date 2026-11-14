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
namespace fifo_cache {

// FIFO cache implementation.
//
// Entries live in 64-byte slots of a per-shard table (see FIFOCacheShard).
// The cache keeps all its entries in that table and in a FIFO queue ordered
// by insertion.
//
// FIFOHandle can be in these states:
// 1. Referenced externally AND in hash table.
//    The entry stays in the FIFO queue. An eviction scan that meets it
//    moves it to the tail, so later scans do not re-walk it. In a mostly
//    pinned shard the scan sets it aside instead, until it is unpinned (see
//    aside_slot_).
//    (refs >= 1 && in_cache == true)
// 2. Not referenced externally AND in hash table.
//    The entry is in the FIFO queue and can be evicted.
//    (refs == 0 && in_cache == true)
// 3. Referenced externally AND not in hash table.
//    The entry is in neither the queue nor the hash table. It is freed
//    when refs drops to 0.
//    (refs >= 1 && in_cache == false)
// Unlike LRU, Lookup and Release never reorder the queue; only an eviction
// scan moves a pinned entry it meets to the tail. While refs > 0, public
// properties like value and deleter must not change.
//
// Lookup, Ref and most Releases run without the shard lock, so refs and the
// in-cache bit share one atomic word (state). The bit changes only under the
// shard lock, in the same atomic operation that checks refs.
struct FIFOHandle : public Cache::Handle {
  // A key of length 16 (every block cache key) is stored as its bijective
  // 128-bit hash, as in HyperClockCache: exact, and reversible with the
  // shard's seed. Any other key is stored as [0] = its 64-bit hash and
  // [1] = a HeapKey copy of its bytes (kHeapKey).
  UniqueId64x2 hashed_key{};
  Cache::ObjectPtr value = nullptr;
  const Cache::CacheItemHelper* helper = nullptr;
  size_t total_charge = 0;
  // External refs (the cache itself is not counted) in the low bits, plus
  // kInCache.
  std::atomic<uint32_t> state{0};
  static constexpr uint32_t kRefMask = (uint32_t{1} << 30) - 1;
  // Whether this entry is referenced by the hash table.
  static constexpr uint32_t kInCache = uint32_t{1} << 30;
  // First entry of the chain of entries whose home is this slot's index;
  // 0 when empty (slot 0 never holds an entry). It belongs to the index,
  // not to the entry stored here. Written under the shard lock.
  std::atomic<uint32_t> head{0};
  // Next entry in this entry's home chain, 0 at the end. Written under the
  // shard lock; left intact when the entry is unlinked, so a reader
  // standing on it can continue.
  std::atomic<uint32_t> chain_next{0};
  // Queue links, as slot indices. Under the shard lock.
  uint32_t next = 0;
  uint32_t prev = 0;
  uint8_t flags = 0;
  enum Flags : uint8_t {
    // The slot holds an entry, or one retired and not yet reclaimed.
    kOccupied = 1 << 0,
    kHeapKey = 1 << 1,
    // Not in the table: allocated on its own, freed on its last Release.
    kStandalone = 1 << 2,
  };
  // Whether the entry is on its shard's aside list. Written under the
  // shard lock; read by a lock-free last Release, which then flags the
  // shard (see FIFOCacheShard::aside_unpinned_).
  std::atomic<uint8_t> aside{0};

  struct HeapKey {
    size_t size;
    char data[1];
  };

  // For HandleImpl concept. Only [0] is meaningful for every key.
  const UniqueId64x2& GetHash() const { return hashed_key; }

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
    // The pointer is kept in the bytes of hashed_key[1] (see SetKey).
    const HeapKey* key = nullptr;
    std::memcpy(&key, &hashed_key[1], sizeof(const HeapKey*));
    return key;
  }
  // The key of an entry whose key is not 16 bytes long. 16-byte keys need
  // the seed: see FIFOCacheShard::KeyOf.
  Slice key() const { return Slice(heap_key()->data, heap_key()->size); }

  // Runs the deleter. The slot itself may still be read by a concurrent
  // Lookup, so it is retired separately (see Retire).
  void FreeValue(MemoryAllocator* allocator) {
    assert(helper);
    if (helper->del_cb) {
      helper->del_cb(value, allocator);
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
static_assert(sizeof(FIFOHandle) <= 64, "a slot must fit one cache line");
static_assert(sizeof(FIFOHandle::HeapKey*) <= sizeof(uint64_t),
              "a heap-key pointer must fit in hashed_key[1]");

// A single shard of sharded cache.
//
// Table: HyperClockCache-style open addressing over 64-byte slots that
// never move, in an address range reserved up front and mapped lazily.
// Slot 0 is the dummy head of the FIFO queue; slot kFirstSlot + i
// is the home of index i, holds the head of the chain of entries homed at
// i, and may store any entry. An entry prefers its home slot, so a hit
// usually reads one line. The number of homes grows one at a time by
// linear hashing, splitting one chain per step; entries are relinked,
// never moved. A removed or overwritten entry's slot returns to the free
// list only after epoch reclamation (Reclaim), so a lock-free Lookup never
// sees a slot change identity under it. A Lookup racing a split can miss a
// present key (never crash or loop). Measured in db_bench (skewed reads, 32
// threads, 4 GiB, the first 5 s from a cold cache): at most 9 such misses
// per 44M-71M lookups, and at most 0.02% of the lookups that overlapped a
// growth step, so a miss is not retried.
class ALIGN_AS(CACHE_LINE_SIZE) FIFOCacheShard final : public CacheShardBase {
 public:
  // NOTE: the eviction_callback ptr is saved, as is it assumed to be kept
  // alive in Cache. max_upper_hash_bits is unused: homes come from the low
  // bits of the hash, sharding from the high ones.
  FIFOCacheShard(size_t capacity, bool strict_capacity_limit,
                 bool use_adaptive_mutex,
                 CacheMetadataChargePolicy metadata_charge_policy,
                 int max_upper_hash_bits, MemoryAllocator* allocator,
                 const Cache::EvictionCallback* eviction_callback,
                 size_t max_capacity);
  ~FIFOCacheShard();
  FIFOCacheShard(const FIFOCacheShard&) = delete;
  FIFOCacheShard& operator=(const FIFOCacheShard&) = delete;
  FIFOCacheShard(FIFOCacheShard&&) = delete;
  FIFOCacheShard& operator=(FIFOCacheShard&&) = delete;

 public:  // Type definitions expected as parameter to ShardedCache
  using HandleImpl = FIFOHandle;
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
  // Set once by FIFOCache before use; standalone shards in tests use 0.
  void SetHashSeed(uint32_t seed) { hash_seed_ = seed; }

  // Separate from constructor so caller can easily make an array of FIFOCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space. The slot table cannot grow past its reservation,
  // made at construction for max(capacity, max_capacity): raising capacity
  // beyond what it holds does not grow it, so once the table is full new
  // keys are also evicted by occupancy, as in HyperClockCache.
  void SetCapacity(size_t capacity);

  // Set the flag to reject insertion if cache if full.
  void SetStrictCapacityLimit(bool strict_capacity_limit);

  // Like Cache methods, but with an extra "hash" parameter.
  // priority is ignored: FIFO has no priority pools.
  Status Insert(const Slice& key, HashCref hash, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper, size_t charge,
                FIFOHandle** handle, Cache::Priority priority);

  FIFOHandle* CreateStandalone(const Slice& key, HashCref hash,
                               Cache::ObjectPtr obj,
                               const Cache::CacheItemHelper* helper,
                               size_t charge, bool allow_uncharged);

  // helper, create_context, priority and stats are ignored: with no secondary
  // cache configured this is a primary-only lookup. Secondary promotion is
  // handled by CacheWithSecondaryAdapter above the shard, as with LRU.
  // A hit moves nothing.
  FIFOHandle* Lookup(const Slice& key, HashCref hash,
                     const Cache::CacheItemHelper* helper,
                     Cache::CreateContext* create_context,
                     Cache::Priority priority, Statistics* stats);

  // Lookup takes no lock. Release and Ref take none either, unless Release
  // drops the last ref of an entry it must erase (erase_if_last_ref, or the
  // shard is over capacity) or free (detached or standalone). Inserts,
  // erases, eviction and the getters other than GetPinnedUsage take mutex_.
  bool Release(FIFOHandle* handle, bool useful, bool erase_if_last_ref);
  bool Ref(FIFOHandle* handle);
  void Erase(const Slice& key, HashCref hash);

  size_t GetUsage() const;
  // Without mutex_, as HyperClockCache does: scans the slot table, so the
  // lock is never held for it. Exact at quiescence.
  size_t GetPinnedUsage() const;
  size_t GetOccupancyCount() const;
  // Homes in use: slots the table has grown to.
  size_t GetTableAddressCount() const;

  void ApplyToSomeEntries(
      const std::function<void(const Slice& key, Cache::ObjectPtr value,
                               size_t charge,
                               const Cache::CacheItemHelper* helper)>& callback,
      size_t average_entries_per_lock, size_t* state);

  void EraseUnRefEntries();

  // The key of h, using buf as storage for a 16-byte key.
  Slice KeyOf(const FIFOHandle* h, UniqueId64x2* buf) const;

 public:  // other function definitions
  // The FIFO queue. Walk it with TEST_Next.
  void TEST_GetFIFOList(FIFOHandle** fifo);
  FIFOHandle* TEST_Next(const FIFOHandle* h) const { return Next(h); }

  // Retrieves number of elements in the FIFO queue, for unit test purpose
  // only.
  // Not threadsafe.
  size_t TEST_GetFIFOSize();

  size_t TEST_GetTableOccupancyCount() const;

  // Entries on the aside list, for unit tests.
  size_t TEST_GetAsideSize();

  // Test-only: lets a test hold the shard lock to show that hits never take
  // it.
  DMutex& TEST_GetMutex() const { return mutex_; }

  void AppendPrintableOptions(std::string& /*str*/) const;

 private:
  friend class FIFOCache;

  // Slot 0 is the queue dummy head; homes start at kFirstSlot.
  static constexpr uint32_t kFifoSlot = 0;
  static constexpr uint32_t kFirstSlot = 1;
  // Homes the table starts with and the load factor it grows at: more
  // occupied slots than kGrowNumerator / kGrowDenominator of the homes
  // grows the table by one home, until max_length_. A table out of free
  // slots (held by entries erased while pinned) also grows.
  static constexpr size_t kInitialLength = 4;
  static constexpr size_t kGrowNumerator = 3;
  static constexpr size_t kGrowDenominator = 5;
  // The table reserves capacity / kMinAvgCharge homes (at least
  // kMinMaxLength). A workload averaging less charge per entry fills it
  // and is then also evicted by occupancy, as in HyperClockCache.
  static constexpr size_t kMinAvgCharge = 64;
  static constexpr size_t kMinMaxLength = 2048;

  // Slot indices are 32-bit, and the mapping of kFirstSlot + homes + 1
  // slots must not overflow size_t on 32-bit builds.
  static size_t MaxLengthFor(size_t capacity) {
    constexpr size_t kMaxBySize =
        std::numeric_limits<size_t>::max() / sizeof(FIFOHandle) - kFirstSlot -
        1;
    return std::min({std::max(kMinMaxLength, capacity / kMinAvgCharge),
                     size_t{uint32_t{0x7fffffff} - kFirstSlot}, kMaxBySize});
  }
  FIFOHandle* Slot(uint32_t index) const { return slots_base_ + index; }
  // The slot index of table position i: a home below max_length_, or
  // max_length_ itself for the aside slot. MaxLengthFor keeps it in 32 bits.
  uint32_t SlotIndexAt(size_t i) const {
    assert(i <= max_length_);
    return kFirstSlot + static_cast<uint32_t>(i);
  }
  uint32_t IndexOf(const FIFOHandle* h) const {
    return static_cast<uint32_t>(h - slots_base_);
  }
  FIFOHandle* Next(const FIFOHandle* h) const { return Slot(h->next); }
  FIFOHandle* Prev(const FIFOHandle* h) const { return Slot(h->prev); }
  // The FIFO queue: oldest at the head, newest at the tail. It contains all
  // in-cache items, pinned and unpinned, except those set aside.
  FIFOHandle* fifo() const { return Slot(kFifoSlot); }
  FIFOHandle* aside() const { return Slot(aside_slot_); }

  // Home index of hash for the given linear-hashing length info.
  static size_t HomeOf(uint64_t length_info, HashCref hash);
  static bool KeyMatches(const FIFOHandle* h, const Slice& key, HashCref hash);
  // Lock-free: the entry with key, or nullptr.
  FIFOHandle* FindInTable(const Slice& key, HashCref hash) const;
  // Under mutex_: links e at the head of its home chain, publishing it.
  void LinkIntoChain(FIFOHandle* e);
  // Under mutex_: unlinks e from its home chain, leaving e's chain_next.
  void UnlinkFromChain(FIFOHandle* e);
  // Under mutex_: one linear-hashing step, splitting one home's chain.
  void GrowOneHome();
  // Under mutex_: a free slot, preferring home's slot; nullptr if none.
  FIFOHandle* AllocateSlot(size_t home);
  // Under mutex_: whether inserting one more entry would grow the table.
  bool WouldGrowOnNewInsert() const;
  // Under mutex_: once no entry is in the table, returns it to its initial
  // homes (and their charge), or to the fewest that still cover every slot
  // in use. Heap keys of reclaimed slots go to heap_keys, as in Reclaim.
  void ShrinkTableIfEmpty(std::vector<FIFOHandle::HeapKey*>* heap_keys);
  // Under mutex_: free_slots_ := the unoccupied slots below the used homes,
  // dropping stale and repeated entries.
  void RebuildFreeSlots();

  size_t GetTableMetaCharge() const;
  // Table charge that inserting a new entry would add, so admission and
  // eviction account for table growth before it happens.
  size_t TableGrowthChargeOnInsert(const Slice& key, HashCref hash) const;
  // The metadata charge of one more home: 64 bytes, or 0 when metadata
  // is not charged.
  size_t HomeCharge() const;
  // Whether the table has grown past the length an empty table shrinks to.
  bool TableAboveInitialLength() const {
    return used_length_ > std::min(kInitialLength, max_length_);
  }
  // Insert an item into the hash table and append it to the FIFO queue.
  // Older unpinned items are evicted as necessary. Frees heap_key on a
  // non-OK status.
  Status InsertItem(const Slice& key, HashCref hash,
                    FIFOHandle::HeapKey* heap_key, Cache::ObjectPtr value,
                    const Cache::CacheItemHelper* helper, size_t total_charge,
                    FIFOHandle** handle);
  // An entry outside the table, as CreateStandalone returns.
  static FIFOHandle* NewStandalone(HashCref hash, FIFOHandle::HeapKey* heap_key,
                                   Cache::ObjectPtr value,
                                   const Cache::CacheItemHelper* helper,
                                   size_t total_charge);
  static void FreeStandalone(FIFOHandle* h);

  // Unlinks e from the queue or the aside list.
  void FIFO_Remove(FIFOHandle* e);
  // Links e at the tail of the FIFO queue; neither function touches state.
  void FIFO_Append(FIFOHandle* e);
  // FIFO_Append for either list: the FIFO queue or the aside list.
  void LinkAtTail(FIFOHandle* queue, FIFOHandle* e);
  // Moves a linked entry to the tail of queue (the FIFO queue or aside)
  // without changing any charge.
  void MoveToTail(FIFOHandle* e, FIFOHandle* queue);
  // Checks up to kAsideChecksPerEviction entries at the head of the aside
  // list: an unpinned one returns to the tail of the FIFO queue, a pinned
  // one to the aside tail.
  void ReturnUnpinnedAside();
  // Returns every unpinned aside entry to the tail of the FIFO queue.
  void ReturnAllUnpinnedAside();
  // Moves a linked, pinned entry to the aside tail.
  void SetAside(FIFOHandle* e);
  // Release of the last ref when it must erase or free: takes mutex_.
  bool ReleaseLocked(FIFOHandle* e, bool erase_if_last_ref);
  // Erase or overwrite: clears kInCache in one step, unlinks e from the
  // queue and the table. Returns true when no ref held it, so the
  // caller frees it; otherwise it stays charged until its last Release.
  bool Detach(FIFOHandle* e);
  // Under mutex_: removes a claimed or detached entry from the table.
  void RemoveFromTable(FIFOHandle* e);
  // Keeps over_capacity_ equal to usage_ > capacity_. Needs mutex_.
  void SyncOverCapacity();

  // Pinned entries one pass over the queue rotates (moves to the tail)
  // before it sets the next ones aside. A pass meets each pinned entry once,
  // and this is far more than a shard normally has pinned at once, so only
  // a mostly pinned shard sets entries aside.
  static constexpr size_t kPinnedRotationsPerPass = 64;
  static constexpr int kAsideChecksPerEviction = 2;
  // Free some space following strict FIFO policy until enough space
  // to hold (usage_ + charge) is freed or no unpinned entry remains.
  // Pinned entries the scan meets move to the tail, so long-lived pins
  // are not re-walked by every eviction. Terminates after a single pass
  // even if everything is pinned.
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_.
  void EvictFromFIFO(size_t charge, autovector<FIFOHandle*>* deleted);
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
  // EvictFromFIFO's pass, until data is Done.
  void EvictNeed(EvictionData* data, autovector<FIFOHandle*>* deleted);
  // Under mutex_: takes a claimed entry out of the table and the usage, and
  // records it in data and deleted.
  void EvictClaimed(FIFOHandle* e, EvictionData* data,
                    autovector<FIFOHandle*>* deleted);

  // One sweep over the queue from the head: unpinned entries are evicted,
  // pinned entries move to the tail, so long-lived pins are not re-walked
  // by every eviction, or aside once the pass has rotated
  // kPinnedRotationsPerPass of them. Stops once data is Done. Not thread
  // safe - call with mutex_ held.
  void EvictFromQueue(EvictionData* data, autovector<FIFOHandle*>* deleted);

  void NotifyEvicted(const autovector<FIFOHandle*>& evicted_handles);

  // Epoch-based reclamation for lock-free Lookup. Every thread owns one
  // ThreadReader for the whole process (see fifo_cache.cc). A Lookup
  // announces this shard and its current epoch there (then a seq_cst fence)
  // before reading the table, and clears it after. A slot unlinked under
  // mutex_ returns to the free list only once no ThreadReader announces
  // this shard with an epoch at or before the slot's retirement, so no
  // Lookup can still be reading it. Values are not deferred: deleters and
  // eviction callbacks run as soon as an entry leaves the cache.
  // Queues retired slots chained through next, values already released,
  // for Reclaim. Safe without mutex_.
  void Retire(FIFOHandle* first, FIFOHandle* last);
  void RetireAll(const autovector<FIFOHandle*>& handles);
  struct RetiredBatch {
    uint64_t epoch;
    // Stamping order, from 1.
    uint64_t seq;
    // Slot index of the first retired slot, chained through next.
    uint32_t first;
  };
  // Every kReclaimInterval calls, stamps queued slots and applies the last
  // reader scan; once per ReaderWorkInterval() calls it also asks for a new
  // scan. Returns whether the caller must scan the readers
  // (ScanReaders) once it has released mutex_: the scan's cost grows with
  // the threads in the process, so it is kept out of the lock. Needs
  // mutex_. Heap keys of reclaimed slots are added to heap_keys, for the
  // caller to free after releasing mutex_.
  bool MaybeReclaim(std::vector<FIFOHandle::HeapKey*>* heap_keys);
  // Stamps queued slots, scans the readers and frees every batch it can,
  // all under mutex_, for the rare callers that need the slots now.
  void Reclaim(std::vector<FIFOHandle::HeapKey*>* heap_keys);
  // Under mutex_: moves pending_ to limbo_, stamped with the current epoch.
  void StampPending();
  // Under mutex_: returns to the free list the batches up to max_seq
  // stamped before oldest, the oldest epoch a scan found announced. If one
  // of them must wait, advances the epoch, so that it waits only for
  // Lookups already in flight.
  void FreeBatches(uint64_t max_seq, uint64_t oldest,
                   std::vector<FIFOHandle::HeapKey*>* heap_keys);
  // The oldest epoch a Lookup in this shard announces, or UINT64_MAX. It
  // covers every batch stamped before the call. Without mutex_.
  uint64_t OldestAnnouncedEpoch() const;
  // Without mutex_: the scan MaybeReclaim asked for; its result is applied
  // by a later MaybeReclaim.
  void ScanReaders();
  static void FreeHeapKeys(const std::vector<FIFOHandle::HeapKey*>& keys);
  // Reclaim reads every slot retired since the last run, under mutex_, and
  // a retired slot is usually a cache miss. A short interval keeps that
  // hold within the time a DistributedMutex waiter spins before sleeping.
  static constexpr uint32_t kReclaimInterval = 16;
  // Calls between two passes over every reader in the process: at least
  // kReclaimInterval, and one per kReadersPerReaderTick readers, but at most
  // an eighth of the homes in use. So a locked call pays for about
  // kReadersPerReaderTick readers on average once the table has as many
  // homes as there are readers; a smaller table scans more often, because
  // its retired slots must come back before its free room runs out.
  size_t ReaderWorkInterval() const;
  static constexpr size_t kReadersPerReaderTick = 8;

  // A size_t written only under mutex_ and read without it by Release.
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
  // mutex_ and write only the slot
  // (state) and the caller's ThreadReader. What they read sits on
  // lines that no hit and no insert writes:
  // 1. Read on every hit, written only by SetCapacity,
  //    SetStrictCapacityLimit, a table growth step, an epoch advance (which
  //    Reclaim makes only when it finds a Lookup in flight) and a change of
  //    over_capacity_: from capacity_ through slots_base_.
  // 2. Written by every insert, under mutex_: from usage_ on.
  // The slots are in their own mapping.

  // Initialized before use.
  LockedSize capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // usage_ > capacity_, kept by SyncOverCapacity so the last Release of an
  // entry need not read usage_, which every insert writes.
  std::atomic<bool> over_capacity_;

  uint32_t hash_seed_;

  // The epoch Lookups announce. Written only by Reclaim, under mutex_.
  std::atomic<uint64_t> epoch_;

  // Linear-hashing length info of the homes in use (see HomeOf). Written
  // only by GrowOneHome, under mutex_.
  std::atomic<uint64_t> length_info_;

  FIFOHandle* slots_base_;

  // Memory size for entries residing in the cache, including pinned
  // entries detached from the table but still referenced.
  ALIGN_AS(CACHE_LINE_SIZE) LockedSize usage_;

  // The slot array: kFirstSlot + max_length_ slots, mapped lazily and
  // zero-initialized, so a fresh slot has head == 0.
  TypedMemMapping<FIFOHandle> slots_;
  // Homes in use (kFirstSlot + used_length_ slots), and the most there can
  // be.
  size_t used_length_;
  size_t max_length_;
  // Entries in the table.
  size_t occupancy_;
  // Slots that may be free, possibly stale or repeated: AllocateSlot skips
  // any that is occupied.
  std::vector<uint32_t> free_slots_;

  // Charge of referenced entries outside the queues: detached by Erase or
  // overwrite while pinned, or standalone.
  LockedSize detached_pinned_usage_;

  // Aside list: pinned entries an eviction pass met after rotating
  // kPinnedRotationsPerPass others, so that a mostly pinned shard is not
  // re-walked by every insert. An aside entry is still in the table and
  // charged as before; ReturnUnpinnedAside relinks it into the FIFO queue
  // once it is unpinned. Its dummy head is the slot after the last home.
  uint32_t aside_slot_;
  // Set when an aside entry may have become unpinned: by its last Release,
  // or by SetAside if that Release raced with it. An eviction that falls
  // short then returns every unpinned aside entry (see EvictNeed), so a
  // released burst of pins does not stay out of eviction's reach.
  std::atomic<bool> aside_unpinned_;

  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
  mutable DMutex mutex_;

  // From Cache, needed for delete
  MemoryAllocator* const allocator_;

  // A reference to Cache::eviction_callback_
  const Cache::EvictionCallback& eviction_callback_;

  // Slots retired since the last Reclaim, as an index chained through next,
  // 0 when none. Pushed without mutex_.
  std::atomic<uint32_t> pending_;
  // Batches stamped so far. Under mutex_.
  uint64_t stamped_seq_;
  // The batches up to this seq are covered by the scan MaybeReclaim asked
  // for and not yet applied; 0 if none. Under mutex_.
  uint64_t scan_covers_seq_;
  // That scan's OldestAnnouncedEpoch, 0 until ScanReaders stores it.
  std::atomic<uint64_t> scan_result_;
  // Stamped batches waiting for in-flight Lookups, oldest first.
  std::vector<RetiredBatch> limbo_;
  uint32_t reclaim_ticks_;
  // Calls counted toward ReaderWorkInterval(), and whether a reader scan
  // is due. Under mutex_.
  size_t reader_ticks_;
  bool scan_due_;
};

class FIFOCache
#ifdef NDEBUG
    final
#endif
    : public ShardedCache<FIFOCacheShard> {
 public:
  explicit FIFOCache(const FIFOCacheOptions& opts);
  const char* Name() const override { return "FIFOCache"; }
  ObjectPtr Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override;

  void ApplyToHandle(
      Cache* cache, Handle* handle,
      const std::function<void(const Slice& key, ObjectPtr obj, size_t charge,
                               const CacheItemHelper* helper)>& callback)
      override;

  // Retrieves number of elements in FIFO, for unit test purpose only.
  size_t TEST_GetFIFOSize();
};

}  // namespace fifo_cache

using FIFOCache = fifo_cache::FIFOCache;
using FIFOHandle = fifo_cache::FIFOHandle;
using FIFOCacheShard = fifo_cache::FIFOCacheShard;

}  // namespace ROCKSDB_NAMESPACE
