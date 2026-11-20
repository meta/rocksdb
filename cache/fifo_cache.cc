//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/fifo_cache.h"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>

#include "cache/secondary_cache_adapter.h"
#include "port/lang.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/distributed_mutex.h"
#include "util/hash.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {
namespace fifo_cache {

namespace {

// Linear hashing over the homes, as in AutoHyperClockTable: with
// used_length == (1 << min_shift) + threshold homes, a hash whose low
// min_shift bits are below threshold uses min_shift + 1 bits.
inline uint64_t UsedLengthToLengthInfo(size_t used_length) {
  assert(used_length >= 2);
  const int shift = FloorLog2(used_length);
  const uint64_t threshold = BottomNBits(used_length, shift);
  return (threshold << 8) + lossless_cast<uint64_t>(shift);
}

inline size_t LengthInfoToUsedLength(uint64_t length_info) {
  return (size_t{1} << (length_info & 255)) +
         static_cast<size_t>(length_info >> 8);
}

// Every thread's announcement for epoch reclamation, one per thread for the
// whole process. Never freed: a thread's reader is adopted by a later
// thread once it exits.
struct ALIGN_AS(CACHE_LINE_SIZE) ThreadReader {
  // The shard a Lookup in flight is reading, valid while epoch != 0.
  std::atomic<const void*> shard{nullptr};
  std::atomic<uint64_t> epoch{0};
  std::atomic<bool> owned{true};
  ThreadReader* next = nullptr;
};

std::atomic<ThreadReader*>& ThreadReaders() {
  static std::atomic<ThreadReader*> head{nullptr};
  return head;
}
// Readers in ThreadReaders(). Sets how often a shard visits them all (see
// FIFOCacheShard::ReaderWorkInterval).
std::atomic<uint32_t>& ThreadReaderCount() {
  static std::atomic<uint32_t> count{0};
  return count;
}

ThreadReader* AcquireThreadReader() {
  for (ThreadReader* r = ThreadReaders().load(std::memory_order_acquire);
       r != nullptr; r = r->next) {
    bool owned = false;
    if (!r->owned.load(std::memory_order_relaxed) &&
        r->owned.compare_exchange_strong(owned, true,
                                         std::memory_order_acquire)) {
      return r;
    }
  }
  auto* r = new ThreadReader();
  ThreadReaderCount().fetch_add(1, std::memory_order_relaxed);
  r->next = ThreadReaders().load(std::memory_order_relaxed);
  while (!ThreadReaders().compare_exchange_weak(
      r->next, r, std::memory_order_release, std::memory_order_relaxed)) {
  }
  return r;
}

// The calling thread's reader while its ThreadReaderHolder lives. Trivially
// destructible, so a Lookup from a thread_local destructor that runs after
// the holder's reads nullptr, not a reader that another thread may have
// adopted since.
ThreadReader*& TlsReader() {
  thread_local ThreadReader* reader = nullptr;
  return reader;
}
bool& TlsReaderReleased() {
  thread_local bool released = false;
  return released;
}

struct ThreadReaderHolder {
  ThreadReaderHolder() { TlsReader() = AcquireThreadReader(); }
  ThreadReaderHolder(const ThreadReaderHolder&) = delete;
  ThreadReaderHolder& operator=(const ThreadReaderHolder&) = delete;
  ThreadReaderHolder(ThreadReaderHolder&&) = delete;
  ThreadReaderHolder& operator=(ThreadReaderHolder&&) = delete;
  ~ThreadReaderHolder() {
    ThreadReader* reader = TlsReader();
    TlsReader() = nullptr;
    TlsReaderReleased() = true;
    reader->owned.store(false, std::memory_order_release);
  }
};

// The calling thread's own reader, or nullptr once its holder is destroyed
// (the thread is exiting). A thread whose first use comes from a pthread key
// destructor creates its holder after C++ thread_local destructors have run,
// so that holder and its reader are never released: the reader stays owned,
// reachable from ThreadReaders().
ThreadReader* OwnThreadReader() {
  ThreadReader* reader = TlsReader();
  if (LIKELY(reader != nullptr)) {
    return reader;
  }
  if (!TlsReaderReleased()) {
    static thread_local ThreadReaderHolder holder;
    return TlsReader();
  }
  return nullptr;
}

// Sets *temporary when the thread's reader is already released: the
// caller then owns the returned reader for one Lookup and must disown it.
ThreadReader* MyThreadReader(bool* temporary) {
  ThreadReader* reader = OwnThreadReader();
  if (LIKELY(reader != nullptr)) {
    return reader;
  }
  *temporary = true;
  return AcquireThreadReader();
}

// nullptr if the allocation fails; callers then refuse the entry.
FIFOHandle::HeapKey* NewHeapKey(const Slice& key) {
  auto* hk = static_cast<FIFOHandle::HeapKey*>(
      malloc(sizeof(FIFOHandle::HeapKey) - 1 + key.size()));
  if (hk == nullptr) {
    return nullptr;
  }
  hk->size = key.size();
  memcpy(hk->data, key.data(), key.size());
  return hk;
}

size_t HeapKeyCharge(const FIFOHandle::HeapKey* hk,
                     CacheMetadataChargePolicy policy) {
  if (hk == nullptr || policy != kFullChargeCacheMetadata) {
    return 0;
  }
  return sizeof(FIFOHandle::HeapKey) - 1 + hk->size;
}

void SetKey(FIFOHandle* e, const UniqueId64x2& hash,
            FIFOHandle::HeapKey* heap_key, uint8_t flags) {
  e->hashed_key = hash;
  if (heap_key != nullptr) {
    uint64_t bits = 0;
    std::memcpy(&bits, &heap_key, sizeof(FIFOHandle::HeapKey*));
    e->hashed_key[1] = bits;
    flags |= FIFOHandle::kHeapKey;
  }
  e->flags = flags;
}

constexpr uint32_t kNoLink = std::numeric_limits<uint32_t>::max();

// Charge arithmetic that cannot wrap: a charge near SIZE_MAX must neither
// pass a capacity check nor corrupt usage.
constexpr size_t kMaxSize = std::numeric_limits<size_t>::max();
size_t SaturatingAdd(size_t a, size_t b) {
  return a > kMaxSize - b ? kMaxSize : a + b;
}
bool AddOverflows(size_t a, size_t b) { return a > kMaxSize - b; }
// Whether used + add <= cap, without computing used + add.
bool FitsIn(size_t used, size_t add, size_t cap) {
  return add <= cap && used <= cap - add;
}
// used + add - cap, saturated, for when !FitsIn(used, add, cap).
size_t Excess(size_t used, size_t add, size_t cap) {
  return used >= cap ? SaturatingAdd(used - cap, add) : add - (cap - used);
}

}  // namespace

size_t FIFOCacheShard::HomeOf(uint64_t length_info, HashCref hash) {
  const int min_shift = static_cast<int>(length_info & 255);
  const uint64_t threshold = length_info >> 8;
  const uint64_t lower = BottomNBits(hash[0], min_shift);
  return static_cast<size_t>(
      lower < threshold ? BottomNBits(hash[0], min_shift + 1) : lower);
}

bool FIFOCacheShard::KeyMatches(const FIFOHandle* h, const Slice& key,
                                HashCref hash) {
  if (key.size() == kInlineKeySize) {
    return !h->HasHeapKey() && h->hashed_key == hash;
  }
  return h->HasHeapKey() && h->hashed_key[0] == hash[0] && h->key() == key;
}

FIFOHandle* FIFOCacheShard::FindInTable(const Slice& key, HashCref hash) const {
  FIFOHandle* home = Slot(
      SlotIndexAt(HomeOf(length_info_.load(std::memory_order_acquire), hash)));
  TEST_SYNC_POINT("FIFOCacheShard::Lookup:HomeLoaded");
  uint32_t idx = home->head.load(std::memory_order_acquire);
  while (idx != 0) {
    FIFOHandle* h = Slot(idx);
    if (KeyMatches(h, key, hash)) {
      return h;
    }
    idx = h->chain_next.load(std::memory_order_acquire);
  }
  return nullptr;
}

void FIFOCacheShard::LinkIntoChain(FIFOHandle* e) {
  FIFOHandle* home = Slot(SlotIndexAt(
      HomeOf(length_info_.load(std::memory_order_relaxed), e->hashed_key)));
  e->chain_next.store(home->head.load(std::memory_order_relaxed),
                      std::memory_order_relaxed);
  home->head.store(IndexOf(e), std::memory_order_release);
}

void FIFOCacheShard::UnlinkFromChain(FIFOHandle* e) {
  FIFOHandle* home = Slot(SlotIndexAt(
      HomeOf(length_info_.load(std::memory_order_relaxed), e->hashed_key)));
  const uint32_t target = IndexOf(e);
  std::atomic<uint32_t>* link = &home->head;
  uint32_t idx;
  while ((idx = link->load(std::memory_order_relaxed)) != target) {
    assert(idx != 0);
    link = &Slot(idx)->chain_next;
  }
  link->store(e->chain_next.load(std::memory_order_relaxed),
              std::memory_order_release);
}

void FIFOCacheShard::GrowOneHome() {
  assert(used_length_ < max_length_);
  // used_length_ == 2^shift + split: home split, still on shift bits,
  // splits into split and used_length_ on shift + 1 bits.
  const size_t used = used_length_;
  const int shift = FloorLog2(used);
  const size_t split = used - (size_t{1} << shift);
  FIFOHandle* old_home = Slot(SlotIndexAt(split));
  FIFOHandle* new_home = Slot(SlotIndexAt(used));
  uint32_t keep = 0;
  uint32_t move = 0;
  uint32_t idx = old_home->head.load(std::memory_order_relaxed);
  while (idx != 0) {
    FIFOHandle* h = Slot(idx);
    const uint32_t next = h->chain_next.load(std::memory_order_relaxed);
    uint32_t& list =
        BottomNBits(h->hashed_key[0], shift + 1) == split ? keep : move;
    // Release: a Lookup still walking the old chain can follow this link.
    h->chain_next.store(list, std::memory_order_release);
    list = idx;
    idx = next;
  }
  new_home->head.store(move, std::memory_order_release);
  old_home->head.store(keep, std::memory_order_release);
  used_length_ = used + 1;
  length_info_.store(UsedLengthToLengthInfo(used_length_),
                     std::memory_order_release);
  free_slots_.push_back(SlotIndexAt(used));
}

FIFOHandle* FIFOCacheShard::AllocateSlot(size_t home) {
  FIFOHandle* h = Slot(SlotIndexAt(home));
  if ((h->flags & FIFOHandle::kOccupied) == 0) {
    return h;
  }
  while (!free_slots_.empty()) {
    const uint32_t idx = free_slots_.back();
    free_slots_.pop_back();
    h = Slot(idx);
    if ((h->flags & FIFOHandle::kOccupied) == 0) {
      return h;
    }
  }
  return nullptr;
}

bool FIFOCacheShard::WouldGrowOnNewInsert() const {
  return used_length_ < max_length_ &&
         (occupancy_ + 1) * kGrowDenominator > used_length_ * kGrowNumerator;
}

FIFOCacheShard::FIFOCacheShard(size_t capacity, bool strict_capacity_limit,
                               bool use_adaptive_mutex,
                               CacheMetadataChargePolicy metadata_charge_policy,
                               int /*max_upper_hash_bits*/,
                               MemoryAllocator* allocator,
                               const Cache::EvictionCallback* eviction_callback,
                               size_t max_capacity)
    : CacheShardBase(metadata_charge_policy),
      capacity_(0),
      strict_capacity_limit_(strict_capacity_limit),
      over_capacity_(false),
      hash_seed_(0),
      epoch_(1),
      length_info_(0),
      slots_base_(nullptr),
      usage_(0),
      slots_(MemMapping::AllocateLazyZeroedNoReserve(
          sizeof(FIFOHandle) *
          (kFirstSlot + MaxLengthFor(std::max(capacity, max_capacity)) + 1))),
      used_length_(0),
      max_length_(MaxLengthFor(std::max(capacity, max_capacity))),
      occupancy_(0),
      detached_pinned_usage_(0),
      aside_slot_(SlotIndexAt(max_length_)),
      aside_unpinned_(false),
      mutex_(use_adaptive_mutex),
      allocator_(allocator),
      eviction_callback_(*eviction_callback),
      pending_(0),
      stamped_seq_(0),
      scan_covers_seq_(0),
      scan_result_(0),
      reclaim_ticks_(0),
      reader_ticks_(0),
      scan_due_(false) {
  slots_base_ = slots_.Get();
  if (slots_base_ == nullptr) {
    fprintf(stderr, "Anonymous mmap for RocksDB FIFOCache failed. Aborting.\n");
    std::terminate();
  }
  // Make empty circular linked lists.
  for (uint32_t queue : {kFifoSlot, aside_slot_}) {
    Slot(queue)->next = queue;
    Slot(queue)->prev = queue;
  }
  used_length_ = std::min(kInitialLength, max_length_);
  length_info_.store(UsedLengthToLengthInfo(used_length_),
                     std::memory_order_relaxed);
  for (size_t i = used_length_; i > 0; --i) {
    free_slots_.push_back(SlotIndexAt(i - 1));
  }
  usage_ = GetTableMetaCharge();
  SetCapacity(capacity);
}

FIFOCacheShard::~FIFOCacheShard() {
  // Entries still referenced are the caller's leak, as before; their values
  // are not freed. Retired slots already released their values.
  for (size_t i = 0; i < used_length_; ++i) {
    FIFOHandle* h = Slot(SlotIndexAt(i));
    if ((h->flags & FIFOHandle::kOccupied) == 0) {
      continue;
    }
    if (h->InCache() && !h->HasRefs()) {
      h->FreeValue(allocator_);
    }
    if (h->HasHeapKey()) {
      free(const_cast<FIFOHandle::HeapKey*>(h->heap_key()));
    }
  }
}

Slice FIFOCacheShard::KeyOf(const FIFOHandle* h, UniqueId64x2* buf) const {
  if (h->HasHeapKey()) {
    return h->key();
  }
  BijectiveUnhash2x64(h->hashed_key[1], h->hashed_key[0], &(*buf)[1],
                      &(*buf)[0]);
  (*buf)[0] ^= hash_seed_;
  // NOTE: endian dependence, as in HyperClockCache
  return Slice(reinterpret_cast<const char*>(buf), kInlineKeySize);
}

void FIFOCacheShard::RemoveFromTable(FIFOHandle* e) {
  UnlinkFromChain(e);
  assert(occupancy_ > 0);
  --occupancy_;
}

void FIFOCacheShard::EraseUnRefEntries() {
  autovector<FIFOHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    for (FIFOHandle* queue : {fifo(), aside()}) {
      FIFOHandle* cur = Next(queue);
      while (cur != queue) {
        FIFOHandle* next = Next(cur);
        uint32_t s = cur->state.load(std::memory_order_relaxed);
        if ((s & FIFOHandle::kRefMask) == 0 &&
            cur->state.compare_exchange_strong(s, 0,
                                               std::memory_order_acquire)) {
          assert(s & FIFOHandle::kInCache);
          FIFO_Remove(cur);
          RemoveFromTable(cur);
          assert(usage_.Get() >= cur->total_charge);
          usage_ -= cur->total_charge;
          last_reference_list.push_back(cur);
        }
        cur = next;
      }
    }
    SyncOverCapacity();
  }

  for (FIFOHandle* entry : last_reference_list) {
    entry->FreeValue(allocator_);
  }
  RetireAll(last_reference_list);

  std::vector<FIFOHandle::HeapKey*> heap_keys;
  {
    DMutexLock l(mutex_);
    ShrinkTableIfEmpty(&heap_keys);
  }
  FreeHeapKeys(heap_keys);
}

void FIFOCacheShard::ShrinkTableIfEmpty(
    std::vector<FIFOHandle::HeapKey*>* heap_keys) {
  if (occupancy_ != 0) {
    return;
  }
  Reclaim(heap_keys);
  // Every chain is empty, so any length is a valid table, but it must keep
  // the slots still held: pinned entries erased from the table, and retired
  // slots a Lookup in flight may still read.
  size_t length = std::min(kInitialLength, max_length_);
  for (size_t i = used_length_; i > length; --i) {
    if ((Slot(SlotIndexAt(i - 1))->flags & FIFOHandle::kOccupied) != 0) {
      length = i;
    }
  }
  if (length == used_length_) {
    return;
  }
  usage_ -= GetTableMetaCharge();
  used_length_ = length;
  length_info_.store(UsedLengthToLengthInfo(used_length_),
                     std::memory_order_release);
  usage_ += GetTableMetaCharge();
  RebuildFreeSlots();
  SyncOverCapacity();
}

void FIFOCacheShard::ApplyToSomeEntries(
    const std::function<void(const Slice& key, Cache::ObjectPtr value,
                             size_t charge,
                             const Cache::CacheItemHelper* helper)>& callback,
    size_t average_entries_per_lock, size_t* state) {
  DMutexLock l(mutex_);
  assert(average_entries_per_lock > 0);
  // *state is the next slot to visit; slots never move, so growth between
  // calls neither skips nor repeats an entry.
  const size_t end = kFirstSlot + used_length_;
  const size_t begin = std::max<size_t>(*state, kFirstSlot);
  if (*state == SIZE_MAX || occupancy_ == 0 || begin >= end) {
    *state = SIZE_MAX;
    return;
  }
  // Compared by subtraction: begin + average_entries_per_lock can wrap.
  size_t stop;
  if (average_entries_per_lock >= end - begin) {
    stop = end;
    *state = SIZE_MAX;
  } else {
    stop = begin + average_entries_per_lock;
    *state = stop;
  }
  UniqueId64x2 buf;
  for (size_t i = begin; i < stop; ++i) {
    const FIFOHandle* h = Slot(static_cast<uint32_t>(i));
    if ((h->flags & FIFOHandle::kOccupied) != 0 && h->InCache()) {
      callback(KeyOf(h, &buf), h->value, h->GetCharge(metadata_charge_policy_),
               h->helper);
    }
  }
}

void FIFOCacheShard::TEST_GetFIFOList(FIFOHandle** fifo) {
  DMutexLock l(mutex_);
  *fifo = Slot(kFifoSlot);
}

size_t FIFOCacheShard::TEST_GetAsideSize() {
  DMutexLock l(mutex_);
  size_t size = 0;
  for (FIFOHandle* h = Next(aside()); h != aside(); h = Next(h)) {
    size++;
  }
  return size;
}

size_t FIFOCacheShard::TEST_GetFIFOSize() {
  DMutexLock l(mutex_);
  size_t size = 0;
  for (FIFOHandle* h = Next(fifo()); h != fifo(); h = Next(h)) {
    size++;
  }
  return size;
}

void FIFOCacheShard::FIFO_Remove(FIFOHandle* e) {
  assert(e->next != kNoLink);
  assert(e->prev != kNoLink);
  Next(e)->prev = e->prev;
  Prev(e)->next = e->next;
  e->prev = e->next = kNoLink;
  e->aside.store(0, std::memory_order_relaxed);
  // Queue membership alone does not affect usage_ (in-cache plus
  // detached-pinned).
}

void FIFOCacheShard::FIFO_Append(FIFOHandle* e) { LinkAtTail(fifo(), e); }

void FIFOCacheShard::LinkAtTail(FIFOHandle* queue, FIFOHandle* e) {
  assert(e->next == kNoLink);
  assert(e->prev == kNoLink);
  // Append to tail: queue->prev is newest and queue->next is oldest.
  const uint32_t idx = IndexOf(e);
  e->next = IndexOf(queue);
  e->prev = queue->prev;
  Prev(e)->next = idx;
  queue->prev = idx;
}

void FIFOCacheShard::MoveToTail(FIFOHandle* e, FIFOHandle* queue) {
  FIFO_Remove(e);
  LinkAtTail(queue, e);
}

void FIFOCacheShard::SetAside(FIFOHandle* e) {
  MoveToTail(e, aside());
  TEST_SYNC_POINT_CALLBACK("FIFOCacheShard::SetAside:BeforeFlag", e);
  // A last Release of e that runs concurrently either sees the flag (and
  // flags the shard) or has already dropped the ref this reload sees: both
  // sides are seq_cst.
  e->aside.store(1, std::memory_order_seq_cst);
  if ((e->state.load(std::memory_order_seq_cst) & FIFOHandle::kRefMask) == 0) {
    aside_unpinned_.store(true, std::memory_order_relaxed);
  }
}

void FIFOCacheShard::ReturnUnpinnedAside() {
  for (int i = 0; i < kAsideChecksPerEviction && Next(aside()) != aside();
       ++i) {
    TEST_SYNC_POINT("FIFOCacheShard::EvictionVisit");
    FIFOHandle* e = Next(aside());
    // A Lookup may pin e right after this check; it is then a pinned entry
    // in the queue, as after any other race with a hit.
    if (e->HasRefs()) {
      SetAside(e);
    } else {
      MoveToTail(e, fifo());
    }
  }
}

void FIFOCacheShard::ReturnAllUnpinnedAside() {
  FIFOHandle* cur = Next(aside());
  while (cur != aside()) {
    TEST_SYNC_POINT("FIFOCacheShard::EvictionVisit");
    FIFOHandle* next = Next(cur);
    if (!cur->HasRefs()) {
      MoveToTail(cur, fifo());
    }
    cur = next;
  }
}

void FIFOCacheShard::SyncOverCapacity() {
  const bool over = usage_.Get() > capacity_.Get();
  if (over_capacity_.load(std::memory_order_relaxed) != over) {
    over_capacity_.store(over, std::memory_order_relaxed);
  }
}

void FIFOCacheShard::EvictFromFIFO(size_t charge,
                                   autovector<FIFOHandle*>* deleted) {
  if (FitsIn(usage_.Get(), charge, capacity_.Get())) {
    return;
  }
  EvictionData data;
  data.need_charge = Excess(usage_.Get(), charge, capacity_.Get());
  EvictNeed(&data, deleted);
}

void FIFOCacheShard::EvictNeed(EvictionData* data,
                               autovector<FIFOHandle*>* deleted) {
  ReturnUnpinnedAside();
  EvictFromQueue(data, deleted);
  // Eviction returns only kAsideChecksPerEviction aside entries per call,
  // so after a burst of pins is released most of the shard may wait on the
  // aside list. When it falls short, return every unpinned one and go once
  // more. A shard whose aside entries all stay pinned skips the walk.
  if (!data->Done() && aside_unpinned_.load(std::memory_order_relaxed) &&
      aside_unpinned_.exchange(false, std::memory_order_acquire)) {
    ReturnAllUnpinnedAside();
    EvictFromQueue(data, deleted);
  }
}

void FIFOCacheShard::EvictClaimed(FIFOHandle* e, EvictionData* data,
                                  autovector<FIFOHandle*>* deleted) {
  RemoveFromTable(e);
  assert(usage_.Get() >= e->total_charge);
  usage_ -= e->total_charge;
  data->freed_charge += e->total_charge;
  ++data->freed_count;
  deleted->push_back(e);
}

void FIFOCacheShard::EvictFromQueue(EvictionData* data,
                                    autovector<FIFOHandle*>* deleted) {
  // Each step consumes the head: evict it, or move it to the tail (or aside)
  // if pinned. The pass ends after the entry that was newest when it began,
  // so it visits each entry at most once and an all-pinned shard terminates
  // having evicted nothing. Eviction claims the entry with a CAS that
  // requires refs == 0, so a concurrent Lookup either pins it first (and it
  // moves to the tail as pinned) or misses. The pinned move never touches
  // state.
  size_t rotated = 0;
  FIFOHandle* const last = Prev(fifo());
  bool at_last = Next(fifo()) == fifo();
  while (!at_last && !data->Done()) {
    TEST_SYNC_POINT("FIFOCacheShard::EvictionVisit");
    FIFOHandle* cur = Next(fifo());
    at_last = cur == last;
    uint32_t s = cur->state.load(std::memory_order_relaxed);
    assert(s & FIFOHandle::kInCache);
    if ((s & FIFOHandle::kRefMask) != 0 ||
        !cur->state.compare_exchange_strong(s, 0, std::memory_order_acquire)) {
      if (rotated++ < kPinnedRotationsPerPass) {
        MoveToTail(cur, fifo());
      } else {
        SetAside(cur);
      }
    } else {
      FIFO_Remove(cur);
      EvictClaimed(cur, data, deleted);
    }
  }
}

void FIFOCacheShard::NotifyEvicted(
    const autovector<FIFOHandle*>& evicted_handles) {
  autovector<FIFOHandle*> slots;
  UniqueId64x2 buf;
  for (FIFOHandle* entry : evicted_handles) {
    // was_hit is always false: FIFO keeps no hit state (frequency tracking
    // is out of scope), so eviction callbacks cannot distinguish hits.
    // A callback returning true took ownership of obj.
    if (!eviction_callback_ ||
        !eviction_callback_(KeyOf(entry, &buf), entry, false)) {
      // Free the entries here outside of mutex for performance reasons.
      entry->FreeValue(allocator_);
    }
    if (entry->IsStandalone()) {
      FreeStandalone(entry);
    } else {
      slots.push_back(entry);
    }
  }
  RetireAll(slots);
}

void FIFOCacheShard::RetireAll(const autovector<FIFOHandle*>& handles) {
  if (handles.empty()) {
    return;
  }
  for (size_t i = 0; i + 1 < handles.size(); ++i) {
    handles[i]->next = IndexOf(handles[i + 1]);
  }
  Retire(handles.front(), handles.back());
}

void FIFOCacheShard::Retire(FIFOHandle* first, FIFOHandle* last) {
  last->next = pending_.load(std::memory_order_relaxed);
  while (!pending_.compare_exchange_weak(last->next, IndexOf(first),
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
  }
}

size_t FIFOCacheShard::ReaderWorkInterval() const {
  const size_t readers = ThreadReaderCount().load(std::memory_order_relaxed);
  // Retired slots wait for the next scan, so the interval stays within an
  // eighth of the homes in use, well inside the room the load factor
  // leaves free.
  return std::max<size_t>(
      kReclaimInterval,
      std::min(readers / kReadersPerReaderTick, used_length_ / 8));
}

bool FIFOCacheShard::MaybeReclaim(
    std::vector<FIFOHandle::HeapKey*>* heap_keys) {
  if (++reclaim_ticks_ < kReclaimInterval) {
    return false;
  }
  reclaim_ticks_ = 0;
  // Work that visits every reader in the process (the scan of their
  // announcements) runs once per ReaderWorkInterval() calls: about
  // kReadersPerReaderTick readers per call once the table has as many homes as
  // there are readers, more in a smaller table (see ReaderWorkInterval).
  reader_ticks_ += kReclaimInterval;
  if (reader_ticks_ >= ReaderWorkInterval()) {
    reader_ticks_ = 0;
    scan_due_ = true;
  }
  StampPending();
  if (scan_covers_seq_ != 0) {
    const uint64_t oldest = scan_result_.exchange(0, std::memory_order_acquire);
    if (oldest == 0) {
      // Not scanned yet.
      return false;
    }
    FreeBatches(scan_covers_seq_, oldest, heap_keys);
    scan_covers_seq_ = 0;
  }
  if (!scan_due_ || limbo_.empty()) {
    return false;
  }
  scan_due_ = false;
  // The caller scans after this critical section, so after every batch
  // stamped so far was unlinked.
  scan_covers_seq_ = stamped_seq_;
  return true;
}

void FIFOCacheShard::Reclaim(std::vector<FIFOHandle::HeapKey*>* heap_keys) {
  StampPending();
  if (!limbo_.empty()) {
    FreeBatches(stamped_seq_, OldestAnnouncedEpoch(), heap_keys);
  }
}

void FIFOCacheShard::StampPending() {
  const uint32_t first = pending_.exchange(0, std::memory_order_acquire);
  if (first != 0) {
    // Only the lock holder writes epoch_.
    limbo_.push_back(
        {epoch_.load(std::memory_order_relaxed), ++stamped_seq_, first});
  }
}

uint64_t FIFOCacheShard::OldestAnnouncedEpoch() const {
  // Pairs with the fence in Lookup: a Lookup whose announcement this scan
  // misses starts after everything unlinked before the fence, so it cannot
  // reach it.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  uint64_t oldest = std::numeric_limits<uint64_t>::max();
  for (const ThreadReader* r = ThreadReaders().load(std::memory_order_acquire);
       r != nullptr; r = r->next) {
    const uint64_t announced = r->epoch.load(std::memory_order_acquire);
    if (announced != 0 && r->shard.load(std::memory_order_relaxed) == this) {
      oldest = std::min(oldest, announced);
    }
  }
  return oldest;
}

void FIFOCacheShard::ScanReaders() {
  TEST_SYNC_POINT("FIFOCacheShard::ScanReaders");
  scan_result_.store(OldestAnnouncedEpoch(), std::memory_order_release);
}

void FIFOCacheShard::FreeBatches(uint64_t max_seq, uint64_t oldest,
                                 std::vector<FIFOHandle::HeapKey*>* heap_keys) {
  size_t done = 0;
  for (; done < limbo_.size() && limbo_[done].seq <= max_seq &&
         limbo_[done].epoch < oldest;
       ++done) {
    uint32_t idx = limbo_[done].first;
    while (idx != 0) {
      FIFOHandle* h = Slot(idx);
      const uint32_t next = h->next;
      if (h->HasHeapKey()) {
        heap_keys->push_back(const_cast<FIFOHandle::HeapKey*>(h->heap_key()));
      }
      h->flags = 0;
      free_slots_.push_back(idx);
      idx = next;
    }
  }
  limbo_.erase(limbo_.begin(), limbo_.begin() + done);
  const uint64_t epoch = epoch_.load(std::memory_order_relaxed);
  if (!limbo_.empty() && limbo_.front().seq <= max_seq &&
      limbo_.back().epoch == epoch) {
    // Lookups that start from now announce a later epoch than any batch
    // left, so those batches wait only for Lookups already in flight.
    epoch_.store(epoch + 1, std::memory_order_release);
  }
  if (free_slots_.size() > 2 * (kFirstSlot + used_length_)) {
    RebuildFreeSlots();
  }
}

void FIFOCacheShard::RebuildFreeSlots() {
  free_slots_.clear();
  for (size_t i = used_length_; i > 0; --i) {
    const uint32_t idx = SlotIndexAt(i - 1);
    if ((Slot(idx)->flags & FIFOHandle::kOccupied) == 0) {
      free_slots_.push_back(idx);
    }
  }
}

void FIFOCacheShard::FreeHeapKeys(
    const std::vector<FIFOHandle::HeapKey*>& keys) {
  for (FIFOHandle::HeapKey* hk : keys) {
    free(hk);
  }
}

void FIFOCacheShard::SetCapacity(size_t capacity) {
  autovector<FIFOHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    capacity_ = capacity;
    EvictFromFIFO(0, &last_reference_list);
    SyncOverCapacity();
  }

  NotifyEvicted(last_reference_list);

  // A cut that evicts every entry leaves the grown table charged: shrink
  // the emptied table once the evicted slots are retired, as
  // EraseUnRefEntries does, so its metadata charge does not crowd out new
  // entries.
  std::vector<FIFOHandle::HeapKey*> heap_keys;
  {
    DMutexLock l(mutex_);
    if (occupancy_ == 0 && TableAboveInitialLength()) {
      ShrinkTableIfEmpty(&heap_keys);
    }
  }
  FreeHeapKeys(heap_keys);
}

void FIFOCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

FIFOHandle* FIFOCacheShard::NewStandalone(HashCref hash,
                                          FIFOHandle::HeapKey* heap_key,
                                          Cache::ObjectPtr value,
                                          const Cache::CacheItemHelper* helper,
                                          size_t total_charge) {
  FIFOHandle* e =
      new (port::cacheline_aligned_alloc(sizeof(FIFOHandle))) FIFOHandle();
  SetKey(e, hash, heap_key, FIFOHandle::kStandalone);
  e->value = value;
  e->helper = helper;
  e->total_charge = total_charge;
  e->next = e->prev = kNoLink;
  return e;
}

void FIFOCacheShard::FreeStandalone(FIFOHandle* h) {
  assert(h->IsStandalone());
  if (h->HasHeapKey()) {
    free(const_cast<FIFOHandle::HeapKey*>(h->heap_key()));
  }
  h->~FIFOHandle();
  port::cacheline_aligned_free(h);
}

Status FIFOCacheShard::InsertItem(const Slice& key, HashCref hash,
                                  FIFOHandle::HeapKey* heap_key,
                                  Cache::ObjectPtr value,
                                  const Cache::CacheItemHelper* helper,
                                  size_t total_charge, FIFOHandle** handle) {
  Status s = Status::OK();
  autovector<FIFOHandle*> last_reference_list;
  std::vector<FIFOHandle::HeapKey*> heap_keys;
  bool scan = false;
  {
    DMutexLock l(mutex_);
    scan = MaybeReclaim(&heap_keys);

    // Free the space following strict FIFO policy until enough space
    // is freed or no unpinned entry remains.
    EvictFromFIFO(total_charge, &last_reference_list);

    if (occupancy_ == 0 && TableAboveInitialLength()) {
      // A capacity cut emptied the shard while pinned entries kept the
      // table in use, and they have since gone: shrink it, so its metadata
      // charge does not crowd out the entry.
      ShrinkTableIfEmpty(&heap_keys);
    }

    FIFOHandle* e = nullptr;
    // Admitted beyond capacity only without strict_capacity_limit, with a
    // handle, and when usage_ cannot wrap.
    if (FitsIn(usage_.Get(), total_charge, capacity_.Get()) ||
        (!strict_capacity_limit_ && handle != nullptr &&
         !AddOverflows(usage_.Get(), total_charge))) {
      // Insert into the cache. Note that the cache might get larger than its
      // capacity if not enough space was freed up.
      // Eviction also makes room for a growth step's metadata charge (below).
      // Under strict_capacity_limit the table only grows when the growth
      // also fits, so an admitted insert never ends above capacity; without
      // it, usage ends above capacity only when pinned entries leave nothing
      // to evict, or an overwrite finds no free slot.
      FIFOHandle* old = FindInTable(key, hash);
      // Every entry needs a slot, so a new entry whose table must grow also
      // evicts for the growth. Under strict_capacity_limit it then grows once
      // that fits; otherwise a full table whose growth does not fit would
      // turn away every insert while unpinned entries remain. Without it,
      // the growth would otherwise leave usage over capacity, and the next
      // last-ref Release would erase whatever entry it releases.
      if (old == nullptr && WouldGrowOnNewInsert()) {
        const size_t growth = TableGrowthChargeOnInsert(key, hash);
        const size_t needed = SaturatingAdd(total_charge, growth);
        if (!FitsIn(usage_.Get(), needed, capacity_.Get())) {
          EvictionData data;
          data.need_charge = Excess(usage_.Get(), needed, capacity_.Get());
          EvictNeed(&data, &last_reference_list);
          if (strict_capacity_limit_ &&
              FitsIn(usage_.Get(), needed, capacity_.Get())) {
            const size_t meta_before = GetTableMetaCharge();
            GrowOneHome();
            usage_ += GetTableMetaCharge() - meta_before;
          }
        }
      }
      // Holding the load factor takes more than one home per entry. Each
      // step's metadata charge must fit with the entry; a step that does
      // not waits for a later insert (a table out of slots grows below).
      while (old == nullptr && WouldGrowOnNewInsert()) {
        const size_t needed =
            SaturatingAdd(total_charge, TableGrowthChargeOnInsert(key, hash));
        if (!FitsIn(usage_.Get(), needed, capacity_.Get())) {
          break;
        }
        const size_t meta_before = GetTableMetaCharge();
        GrowOneHome();
        usage_ += GetTableMetaCharge() - meta_before;
      }
      if (old == nullptr && used_length_ == max_length_ &&
          (occupancy_ + 1) * kGrowDenominator > max_length_ * kGrowNumerator) {
        // The table is as large as it gets: also evict one entry by
        // occupancy, keeping its load factor, so retired slots have time to
        // be reclaimed. An overwrite keeps occupancy, and this eviction
        // could pick old itself, which Detach below must find in cache.
        EvictionData data;
        data.need_count = 1;
        EvictNeed(&data, &last_reference_list);
      }
      const size_t home =
          HomeOf(length_info_.load(std::memory_order_relaxed), hash);
      e = AllocateSlot(home);
      if (e == nullptr) {
        Reclaim(&heap_keys);
        e = AllocateSlot(home);
      }
      if (e == nullptr && used_length_ < max_length_) {
        // Entries erased while pinned keep their slots but leave the load
        // factor's count, so the table can run out of slots below it. Grow
        // past them, under the same capacity rules as a growth step above.
        const size_t needed = SaturatingAdd(total_charge, HomeCharge());
        if (old == nullptr && !FitsIn(usage_.Get(), needed, capacity_.Get())) {
          EvictionData data;
          data.need_charge = Excess(usage_.Get(), needed, capacity_.Get());
          EvictNeed(&data, &last_reference_list);
        }
        if (strict_capacity_limit_
                ? FitsIn(usage_.Get(), needed, capacity_.Get())
                : !AddOverflows(usage_.Get(), needed)) {
          const size_t meta_before = GetTableMetaCharge();
          GrowOneHome();
          usage_ += GetTableMetaCharge() - meta_before;
          e = AllocateSlot(
              HomeOf(length_info_.load(std::memory_order_relaxed), hash));
        }
      }
      if (e != nullptr) {
        SetKey(e, hash, heap_key, FIFOHandle::kOccupied);
        e->value = value;
        e->helper = helper;
        e->total_charge = total_charge;
        e->next = e->prev = kNoLink;
        // The final state, set before the chain publishes e to Lookup.
        // Release: GetPinnedUsage reads total_charge after an acquire load
        // of state, without the chain.
        e->state.store(FIFOHandle::kInCache | (handle != nullptr ? 1 : 0),
                       std::memory_order_release);
        LinkIntoChain(e);
        ++occupancy_;
        if (old != nullptr) {
          s = Status::OkOverwritten();
          if (Detach(old)) {
            last_reference_list.push_back(old);
          }
        }
        usage_ += total_charge;
        FIFO_Append(e);
        if (handle != nullptr) {
          *handle = e;
        }
      } else {
        if (handle != nullptr && !strict_capacity_limit_) {
          // No free slot: hand out an entry outside the table, as
          // HyperClockCache does when its table is full.
          e = NewStandalone(hash, heap_key, value, helper, total_charge);
          e->state.store(1, std::memory_order_relaxed);
          usage_ += total_charge;
          detached_pinned_usage_ += total_charge;
          *handle = e;
        }
        // Accepted without a slot (standalone, or dropped below): the old
        // value must not stay visible behind an OK status. A failed strict
        // insert leaves the cache unchanged.
        if (old != nullptr && (e != nullptr || handle == nullptr)) {
          s = Status::OkOverwritten();
          if (Detach(old)) {
            last_reference_list.push_back(old);
          }
        }
      }
    }
    if (e == nullptr) {
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(
            NewStandalone(hash, heap_key, value, helper, total_charge));
      } else {
        free(heap_key);
        *handle = nullptr;
        s = Status::MemoryLimit("Insert failed due to FIFO cache being full.");
      }
    }
    SyncOverCapacity();
  }

  if (scan) {
    ScanReaders();
  }
  NotifyEvicted(last_reference_list);
  FreeHeapKeys(heap_keys);

  return s;
}

FIFOHandle* FIFOCacheShard::Lookup(const Slice& key, HashCref hash,
                                   const Cache::CacheItemHelper* /*helper*/,
                                   Cache::CreateContext* /*create_context*/,
                                   Cache::Priority /*priority*/,
                                   Statistics* /*stats*/) {
  bool temporary_reader = false;
  ThreadReader* reader = MyThreadReader(&temporary_reader);
  reader->shard.store(this, std::memory_order_relaxed);
  reader->epoch.store(epoch_.load(std::memory_order_acquire),
                      std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  FIFOHandle* e = FindInTable(key, hash);
  TEST_SYNC_POINT("FIFOCacheShard::Lookup:Found");
  uint32_t s = 0;
  if (e != nullptr) {
    s = e->state.load(std::memory_order_relaxed);
    do {
      if ((s & FIFOHandle::kInCache) == 0) {
        e = nullptr;
        break;
      }
    } while (!e->state.compare_exchange_weak(
        s, s + 1, std::memory_order_acquire, std::memory_order_relaxed));
  }
  // The ref, if taken, keeps e alive from here on.
  reader->epoch.store(0, std::memory_order_release);
  if (UNLIKELY(temporary_reader)) {
    reader->owned.store(false, std::memory_order_release);
  }
  if (e == nullptr) {
    return nullptr;
  }
  TEST_SYNC_POINT_CALLBACK("FIFOCacheShard::Lookup:AfterRef", e);
  // A hit moves nothing.
  return e;
}

bool FIFOCacheShard::Ref(FIFOHandle* e) {
  // To create another reference - entry must be already externally referenced.
  assert(e->HasRefs());
  e->state.fetch_add(1, std::memory_order_relaxed);
  return true;
}

bool FIFOCacheShard::Release(FIFOHandle* e, bool /*useful*/,
                             bool erase_if_last_ref) {
  if (e == nullptr) {
    return false;
  }
  uint32_t s = e->state.load(std::memory_order_relaxed);
  do {
    assert((s & FIFOHandle::kRefMask) > 0);
    if ((s & FIFOHandle::kRefMask) == 1 &&
        (erase_if_last_ref || (s & FIFOHandle::kInCache) == 0 ||
         over_capacity_.load(std::memory_order_relaxed))) {
      return ReleaseLocked(e, erase_if_last_ref);
    }
  } while (!e->state.compare_exchange_weak(s, s - 1, std::memory_order_seq_cst,
                                           std::memory_order_relaxed));
  // The unref and this check are seq_cst, so a concurrent SetAside of e
  // cannot miss both the flag and the unref. Once unreferenced, the slot
  // may already hold another entry; that only costs a needless walk.
  if ((s & FIFOHandle::kRefMask) == 1 &&
      e->aside.load(std::memory_order_seq_cst) != 0) {
    aside_unpinned_.store(true, std::memory_order_release);
  }
  TEST_SYNC_POINT("FIFOCacheShard::Release:AfterUnref");
  return false;
}

bool FIFOCacheShard::ReleaseLocked(FIFOHandle* e, bool erase_if_last_ref) {
  const size_t charge = e->total_charge;
  {
    DMutexLock l(mutex_);
    // Lookups may still add refs while e is in cache, so decide on each
    // snapshot. Erasing drops the last ref and both bits in one step.
    uint32_t s = e->state.load(std::memory_order_relaxed);
    uint32_t desired;
    do {
      const bool erase = (s & FIFOHandle::kRefMask) == 1 &&
                         (s & FIFOHandle::kInCache) != 0 &&
                         (erase_if_last_ref || usage_.Get() > capacity_.Get());
      desired = erase ? 0 : s - 1;
    } while (!e->state.compare_exchange_weak(
        s, desired, std::memory_order_acq_rel, std::memory_order_relaxed));
    if ((s & FIFOHandle::kRefMask) != 1) {
      return false;
    }
    if ((s & FIFOHandle::kInCache) != 0) {
      if (desired != 0) {
        // Stays in the queue at the insertion position, now unpinned.
        if (e->aside.load(std::memory_order_relaxed) != 0) {
          aside_unpinned_.store(true, std::memory_order_relaxed);
        }
        return false;
      }
      // The FIFO queue may still hold pinned entries, so unlike LRU there
      // is no emptiness assertion here.
      FIFO_Remove(e);
      RemoveFromTable(e);
    } else {
      // Detached (erased/overwritten) or standalone.
      assert(detached_pinned_usage_.Get() >= charge);
      detached_pinned_usage_ -= charge;
    }
    assert(usage_.Get() >= charge);
    usage_ -= charge;
    SyncOverCapacity();
  }

  // Free the entry here outside of mutex for performance reasons.
  e->FreeValue(allocator_);
  if (e->IsStandalone()) {
    FreeStandalone(e);
  } else {
    Retire(e, e);
  }
  return true;
}

bool FIFOCacheShard::Detach(FIFOHandle* e) {
  const uint32_t s =
      e->state.fetch_and(FIFOHandle::kRefMask, std::memory_order_acq_rel);
  assert(s & FIFOHandle::kInCache);
  FIFO_Remove(e);
  RemoveFromTable(e);
  if ((s & FIFOHandle::kRefMask) == 0) {
    assert(usage_.Get() >= e->total_charge);
    usage_ -= e->total_charge;
    return true;
  }
  // e stays charged until its last Release.
  detached_pinned_usage_ += e->total_charge;
  return false;
}

Status FIFOCacheShard::Insert(const Slice& key, HashCref hash,
                              Cache::ObjectPtr value,
                              const Cache::CacheItemHelper* helper,
                              size_t charge, FIFOHandle** handle,
                              Cache::Priority /*priority*/) {
  assert(helper);
  // value == nullptr is reserved for indicating failure in SecondaryCache
  assert(!(helper->IsSecondaryCacheCompatible() && value == nullptr));
  // Allocate the key copy here outside of the mutex.
  FIFOHandle::HeapKey* heap_key =
      key.size() == kInlineKeySize ? nullptr : NewHeapKey(key);
  if (key.size() != kInlineKeySize && heap_key == nullptr) {
    // No memory for the key copy: refuse the entry. The caller keeps
    // ownership of value, as for any failed insert.
    if (handle != nullptr) {
      *handle = nullptr;
    }
    return Status::MemoryLimit(
        "Insert failed: no memory for the key copy in FIFO cache.");
  }
  return InsertItem(
      key, hash, heap_key, value, helper,
      SaturatingAdd(charge, HeapKeyCharge(heap_key, metadata_charge_policy_)),
      handle);
}

FIFOHandle* FIFOCacheShard::CreateStandalone(
    const Slice& key, HashCref hash, Cache::ObjectPtr value,
    const Cache::CacheItemHelper* helper, size_t charge, bool allow_uncharged) {
  assert(helper);
  FIFOHandle::HeapKey* heap_key =
      key.size() == kInlineKeySize ? nullptr : NewHeapKey(key);
  if (key.size() != kInlineKeySize && heap_key == nullptr) {
    // No memory for the key copy, like a key HyperClockCache cannot hold.
    return nullptr;
  }
  FIFOHandle* e = NewStandalone(
      hash, heap_key, value, helper,
      SaturatingAdd(charge, HeapKeyCharge(heap_key, metadata_charge_policy_)));
  e->state.store(1, std::memory_order_relaxed);

  autovector<FIFOHandle*> last_reference_list;

  {
    DMutexLock l(mutex_);

    EvictFromFIFO(e->total_charge, &last_reference_list);

    // Uncharged or refused when it does not fit under strict_capacity_limit,
    // or when charging it would wrap usage_.
    if ((strict_capacity_limit_ &&
         !FitsIn(usage_.Get(), e->total_charge, capacity_.Get())) ||
        AddOverflows(usage_.Get(), e->total_charge)) {
      if (allow_uncharged) {
        e->total_charge = 0;
      } else {
        e->state.store(0, std::memory_order_relaxed);
        FreeStandalone(e);
        e = nullptr;
      }
    } else {
      usage_ += e->total_charge;
      detached_pinned_usage_ += e->total_charge;
    }
    SyncOverCapacity();
  }

  NotifyEvicted(last_reference_list);
  return e;
}

void FIFOCacheShard::Erase(const Slice& key, HashCref hash) {
  FIFOHandle* e = nullptr;
  bool last_reference = false;
  std::vector<FIFOHandle::HeapKey*> heap_keys;
  bool scan = false;
  {
    DMutexLock l(mutex_);
    scan = MaybeReclaim(&heap_keys);
    e = FindInTable(key, hash);
    if (e != nullptr) {
      last_reference = Detach(e);
    }
    SyncOverCapacity();
  }

  if (scan) {
    ScanReaders();
  }
  // Free the entry here outside of mutex for performance reasons.
  // last_reference will only be true if e != nullptr.
  if (last_reference) {
    e->FreeValue(allocator_);
    Retire(e, e);
  }
  FreeHeapKeys(heap_keys);
}

size_t FIFOCacheShard::GetUsage() const {
  DMutexLock l(mutex_);
  return usage_.Get();
}

size_t FIFOCacheShard::GetPinnedUsage() const {
  // Announced like a Lookup, so no slot is recycled during the scan: a slot
  // whose state shows a referenced entry keeps that entry's total_charge.
  bool temporary_reader = false;
  ThreadReader* reader = MyThreadReader(&temporary_reader);
  reader->shard.store(this, std::memory_order_relaxed);
  reader->epoch.store(epoch_.load(std::memory_order_acquire),
                      std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  const size_t used_length =
      LengthInfoToUsedLength(length_info_.load(std::memory_order_acquire));
  size_t pinned = detached_pinned_usage_.Get();
  for (size_t i = 0; i < used_length; ++i) {
    const FIFOHandle* h = Slot(SlotIndexAt(i));
    const uint32_t s = h->state.load(std::memory_order_acquire);
    if ((s & FIFOHandle::kInCache) != 0 && (s & FIFOHandle::kRefMask) != 0) {
      pinned += h->total_charge;
    }
  }
  reader->epoch.store(0, std::memory_order_release);
  if (UNLIKELY(temporary_reader)) {
    reader->owned.store(false, std::memory_order_release);
  }
  return pinned;
}

size_t FIFOCacheShard::GetOccupancyCount() const {
  DMutexLock l(mutex_);
  return occupancy_;
}

size_t FIFOCacheShard::GetTableAddressCount() const {
  DMutexLock l(mutex_);
  return used_length_;
}

size_t FIFOCacheShard::GetTableMetaCharge() const {
  if (metadata_charge_policy_ != kFullChargeCacheMetadata) {
    return 0;
  }
  return used_length_ * sizeof(FIFOHandle);
}

size_t FIFOCacheShard::TableGrowthChargeOnInsert(const Slice& key,
                                                 HashCref hash) const {
  // An overwrite replaces an existing entry and never grows the table; the
  // lookup only runs at the growth boundary.
  if (metadata_charge_policy_ != kFullChargeCacheMetadata ||
      !WouldGrowOnNewInsert() || FindInTable(key, hash) != nullptr) {
    return 0;
  }
  return HomeCharge();
}

size_t FIFOCacheShard::HomeCharge() const {
  // A growth step adds one home slot.
  return metadata_charge_policy_ == kFullChargeCacheMetadata
             ? sizeof(FIFOHandle)
             : 0;
}

size_t FIFOCacheShard::TEST_GetTableOccupancyCount() const {
  DMutexLock l(mutex_);
  return occupancy_;
}

void FIFOCacheShard::AppendPrintableOptions(std::string& /*str*/) const {}

FIFOCache::FIFOCache(const FIFOCacheOptions& opts) : ShardedCache(opts) {
  size_t per_shard = GetPerShardCapacity();
  const size_t num_shards = GetNumShards();
  const size_t per_shard_max = opts.max_capacity / num_shards +
                               (opts.max_capacity % num_shards != 0 ? 1 : 0);
  MemoryAllocator* alloc = memory_allocator();
  const int max_upper_hash_bits = 32 - opts.num_shard_bits;
  InitShards([&](FIFOCacheShard* cs) {
    new (cs) FIFOCacheShard(per_shard, opts.strict_capacity_limit,
                            opts.use_adaptive_mutex,
                            opts.metadata_charge_policy, max_upper_hash_bits,
                            alloc, &eviction_callback_, per_shard_max);
    cs->SetHashSeed(GetHashSeed());
  });
}

Cache::ObjectPtr FIFOCache::Value(Handle* handle) {
  auto* h = static_cast<const FIFOHandle*>(handle);
  return h->value;
}

size_t FIFOCache::GetCharge(Handle* handle) const {
  return static_cast<const FIFOHandle*>(handle)->GetCharge(
      GetShard(kNullUniqueId64x2).metadata_charge_policy_);
}

const Cache::CacheItemHelper* FIFOCache::GetCacheItemHelper(
    Handle* handle) const {
  auto* h = static_cast<const FIFOHandle*>(handle);
  return h->helper;
}

void FIFOCache::ApplyToHandle(
    Cache* cache, Handle* handle,
    const std::function<void(const Slice& key, ObjectPtr value, size_t charge,
                             const CacheItemHelper* helper)>& callback) {
  auto* cache_ptr = static_cast_with_check<FIFOCache>(cache);
  auto* h = static_cast<const FIFOHandle*>(handle);
  const FIFOCacheShard& shard = cache_ptr->GetShard(kNullUniqueId64x2);
  UniqueId64x2 buf;
  callback(shard.KeyOf(h, &buf), h->value,
           h->GetCharge(shard.metadata_charge_policy_), h->helper);
}

size_t FIFOCache::TEST_GetFIFOSize() {
  return SumOverShards(
      [](FIFOCacheShard& cs) { return cs.TEST_GetFIFOSize(); });
}

}  // namespace fifo_cache

std::shared_ptr<Cache> FIFOCacheOptions::MakeSharedCache() const {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  // For sanitized options
  FIFOCacheOptions opts = *this;
  if (opts.num_shard_bits < 0) {
    opts.num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  std::shared_ptr<Cache> cache = std::make_shared<FIFOCache>(opts);
  if (secondary_cache) {
    cache = std::make_shared<CacheWithSecondaryAdapter>(cache, secondary_cache);
  }
  return cache;
}

}  // namespace ROCKSDB_NAMESPACE
