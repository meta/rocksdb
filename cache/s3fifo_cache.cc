//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/s3fifo_cache.h"

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
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace s3fifo_cache {

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
  // The shard a Lookup in flight is reading, valid while epoch != 0. Stored
  // with release and loaded with acquire (see OldestAnnouncedEpoch). This
  // also covers a reader adopted after its thread exits: the exiting thread
  // releases `owned` after its last read, and the adopter acquires it before
  // its first store here.
  std::atomic<const void*> shard{nullptr};
  std::atomic<uint64_t> epoch{0};
  std::atomic<bool> owned{true};
  ThreadReader* next = nullptr;
  // Distinct per reader, kept when another thread adopts it. Each shard keeps
  // the ReaderSlot of reader i at index i, so a slot has one writer at a time.
  uint32_t index = 0;
};

std::atomic<ThreadReader*>& ThreadReaders() {
  static std::atomic<ThreadReader*> head{nullptr};
  return head;
}
// Readers in ThreadReaders(), which is also the next new reader's index.
// Sets how often a shard visits them all (see
// S3FifoCacheShard::ReaderWorkInterval).
std::atomic<uint32_t>& ThreadReaderCount() {
  static std::atomic<uint32_t> count{0};
  return count;
}

// Readers below ReaderSlotCount() get a ReaderSlot in every shard; a thread's
// first use prefers to reuse a reader below this index, which is
// ReaderSlotCount() except in tests
// (S3FifoCacheShard::TEST_SetPreferredReaders).
std::atomic<size_t>& PreferredReaders() {
  static std::atomic<size_t> bound{S3FifoCacheShard::ReaderSlotCount()};
  return bound;
}

// Reuses a free reader, preferring one below PreferredReaders(), which has a
// ReaderSlot in every shard. New readers go to the list head, so a free one
// past the slots, left from a time with more threads than slots, would
// otherwise often come first. Creates a reader only if none is free. Runs at
// a thread's first use (and per call of an exiting thread), so the second
// pass over the list costs only then, and only while it holds readers past
// the bound.
ThreadReader* AcquireThreadReader() {
  const size_t preferred = PreferredReaders().load(std::memory_order_relaxed);
  // Pass 0 takes only readers below `preferred`; pass 1, run only if pass 0
  // passed over a free reader at or above it, takes any. A lost race for a
  // reader moves on to the next one.
  bool passed_free = false;
  for (int pass = 0; pass < 2; ++pass) {
    for (ThreadReader* r = ThreadReaders().load(std::memory_order_acquire);
         r != nullptr; r = r->next) {
      if (r->owned.load(std::memory_order_relaxed)) {
        continue;
      }
      if (pass == 0 && r->index >= preferred) {
        passed_free = true;
        continue;
      }
      bool owned = false;
      if (r->owned.compare_exchange_strong(owned, true,
                                           std::memory_order_acquire)) {
        return r;
      }
    }
    if (!passed_free) {
      break;
    }
  }
  auto* r = new ThreadReader();
  r->index = ThreadReaderCount().fetch_add(1, std::memory_order_relaxed);
  r->next = ThreadReaders().load(std::memory_order_relaxed);
  while (!ThreadReaders().compare_exchange_weak(
      r->next, r, std::memory_order_release, std::memory_order_relaxed)) {
  }
  return r;
}

// The calling thread's reader while its ThreadReaderHolder lives. Trivially
// destructible, so a Lookup from a thread_local destructor that runs after
// the holder's destructor reads nullptr, not a reader that another thread
// may have adopted since.
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
// caller then owns the returned reader for one Lookup or GetPinnedUsage scan
// and must disown it.
ThreadReader* MyThreadReader(bool* temporary) {
  ThreadReader* reader = OwnThreadReader();
  if (LIKELY(reader != nullptr)) {
    return reader;
  }
  *temporary = true;
  return AcquireThreadReader();
}

// nullptr if the allocation fails; callers then refuse the entry.
S3FifoHandle::HeapKey* NewHeapKey(const Slice& key) {
  auto* hk = static_cast<S3FifoHandle::HeapKey*>(
      malloc(sizeof(S3FifoHandle::HeapKey) - 1 + key.size()));
  // Lets a test simulate an allocation failure.
  TEST_SYNC_POINT_CALLBACK("S3FifoCacheShard::NewHeapKey:Malloc", &hk);
  if (hk == nullptr) {
    return nullptr;
  }
  hk->size = key.size();
  memcpy(hk->data, key.data(), key.size());
  return hk;
}

size_t HeapKeyCharge(const S3FifoHandle::HeapKey* hk,
                     CacheMetadataChargePolicy policy) {
  if (hk == nullptr || policy != kFullChargeCacheMetadata) {
    return 0;
  }
  return sizeof(S3FifoHandle::HeapKey) - 1 + hk->size;
}

void SetKey(S3FifoHandle* e, const UniqueId64x2& hash,
            S3FifoHandle::HeapKey* heap_key, uint8_t flags) {
  UniqueId64x2 stored = hash;
  if (heap_key != nullptr) {
    uint64_t bits = 0;
    std::memcpy(&bits, &heap_key, sizeof(S3FifoHandle::HeapKey*));
    stored[1] = bits;
    flags |= S3FifoHandle::kHeapKey;
  }
  e->SetHash(stored);
  e->flags = flags;
}

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

uint8_t S3FifoHelperRegistry::Probe(const Cache::CacheItemHelper* helper,
                                    size_t* cell) const {
  for (size_t i = CellOf(helper);; i = (i + 1) % kMapCells) {
    const uint8_t v = cells_[i].load(std::memory_order_acquire);
    if (v == 0) {
      *cell = i;
      return kFull;
    }
    if (helpers_[v - 1].load(std::memory_order_acquire) == helper) {
      return static_cast<uint8_t>(v - 1);
    }
  }
}

uint8_t S3FifoHelperRegistry::IndexOf(const Cache::CacheItemHelper* helper) {
  size_t cell = 0;
  uint8_t index = Probe(helper, &cell);
  if (index != kFull) {
    return index;
  }
  TEST_SYNC_POINT("S3FifoHelperRegistry::IndexOf:ProbeMissed");
  if (count_.load(std::memory_order_acquire) == kMaxHelpers) {
    // A registration that filled the registry may have published helper
    // after the probe above; this acquire load orders the re-probe after it.
    return Probe(helper, &cell);
  }
  MutexLock l(&mutex_);
  // Another thread may have registered helper since the probe.
  index = Probe(helper, &cell);
  const size_t n = count_.load(std::memory_order_relaxed);
  if (index != kFull || n == kMaxHelpers) {
    return index;
  }
  // The helper is published before the cell that leads to it, so a probe
  // that finds the cell also finds the helper.
  helpers_[n].store(helper, std::memory_order_release);
  cells_[cell].store(static_cast<uint8_t>(n + 1), std::memory_order_release);
  count_.store(n + 1, std::memory_order_release);
  return static_cast<uint8_t>(n);
}

size_t S3FifoCacheShard::HomeOf(uint64_t length_info, HashCref hash) {
  const int min_shift = static_cast<int>(length_info & 255);
  const uint64_t threshold = length_info >> 8;
  const uint64_t lower = BottomNBits(hash[0], min_shift);
  return static_cast<size_t>(
      lower < threshold ? BottomNBits(hash[0], min_shift + 1) : lower);
}

bool S3FifoCacheShard::KeyMatches(const S3FifoHandle* h, const Slice& key,
                                  HashCref hash) {
  if (key.size() == kInlineKeySize) {
    // The hash first: flags is on the group's second line for a slot at
    // position 1, so a chain step that misses then reads only bytes 0-21.
    return h->GetHash() == hash && !h->HasHeapKey();
  }
  return h->HasHeapKey() && h->HashLow() == hash[0] && h->key() == key;
}

S3FifoHandle* S3FifoCacheShard::FindInTable(const Slice& key,
                                            HashCref hash) const {
  S3FifoHandle* home = Slot(
      SlotIndexAt(HomeOf(length_info_.load(std::memory_order_acquire), hash)));
  TEST_SYNC_POINT("S3FifoCacheShard::Lookup:HomeLoaded");
  uint16_t idx = home->head.load(std::memory_order_acquire);
  while (idx != 0) {
    S3FifoHandle* h = Slot(idx);
    TEST_SYNC_POINT_CALLBACK("S3FifoCacheShard::Lookup:ChainStep", h);
    if (KeyMatches(h, key, hash)) {
      return h;
    }
    idx = h->chain_next.load(std::memory_order_acquire);
  }
  return nullptr;
}

void S3FifoCacheShard::LinkIntoChain(S3FifoHandle* e) {
  S3FifoHandle* home = Slot(SlotIndexAt(
      HomeOf(length_info_.load(std::memory_order_relaxed), e->GetHash())));
  e->chain_next.store(home->head.load(std::memory_order_relaxed),
                      std::memory_order_relaxed);
  home->head.store(IndexOf(e), std::memory_order_release);
}

void S3FifoCacheShard::UnlinkFromChain(S3FifoHandle* e) {
  S3FifoHandle* home = Slot(SlotIndexAt(
      HomeOf(length_info_.load(std::memory_order_relaxed), e->GetHash())));
  const uint16_t target = IndexOf(e);
  std::atomic<uint16_t>* link = &home->head;
  uint16_t idx;
  while ((idx = link->load(std::memory_order_relaxed)) != target) {
    assert(idx != 0);
    link = &Slot(idx)->chain_next;
  }
  link->store(e->chain_next.load(std::memory_order_relaxed),
              std::memory_order_release);
}

void S3FifoCacheShard::GrowOneHome() {
  assert(used_length_ < max_length_);
  // used_length_ == 2^shift + split: home split, still on shift bits,
  // splits into split and used_length_ on shift + 1 bits.
  const size_t used = used_length_;
  const int shift = FloorLog2(used);
  const size_t split = used - (size_t{1} << shift);
  S3FifoHandle* old_home = Slot(SlotIndexAt(split));
  S3FifoHandle* new_home = Slot(SlotIndexAt(used));
  uint16_t keep = 0;
  uint16_t move = 0;
  uint16_t idx = old_home->head.load(std::memory_order_relaxed);
  while (idx != 0) {
    S3FifoHandle* h = Slot(idx);
    const uint16_t next = h->chain_next.load(std::memory_order_relaxed);
    uint16_t& list =
        BottomNBits(h->HashLow(), shift + 1) == split ? keep : move;
    // Release: a Lookup still walking the old chain can follow this link.
    h->chain_next.store(list, std::memory_order_release);
    list = idx;
    idx = next;
  }
  // Publish order: the new home's chain, then the old home's, then
  // length_info_. A Lookup that loads the new length_info_ (acquire) finds both
  // chains complete; one that loaded the old value walks the old home and may
  // miss an entry just moved to the new home (the accepted split-race miss).
  new_home->head.store(move, std::memory_order_release);
  old_home->head.store(keep, std::memory_order_release);
  used_length_ = used + 1;
  length_info_.store(UsedLengthToLengthInfo(used_length_),
                     std::memory_order_release);
  PushFreeSlot(SlotIndexAt(used));
}

void S3FifoCacheShard::GrowOneHomeCharged() {
  const size_t meta_before = GetTableMetaCharge();
  GrowOneHome();
  usage_ += GetTableMetaCharge() - meta_before;
}

void S3FifoCacheShard::PushFreeSlot(uint16_t idx) {
  S3FifoHandle* h = Slot(idx);
  assert((h->flags & S3FifoHandle::kOccupied) == 0);
  h->prev = 0;
  h->next = free_head_;
  if (free_head_ != 0) {
    Slot(free_head_)->prev = idx;
  }
  free_head_ = idx;
}

void S3FifoCacheShard::UnlinkFreeSlot(S3FifoHandle* h) {
  if (h->prev != 0) {
    Slot(h->prev)->next = h->next;
  } else {
    assert(free_head_ == IndexOf(h));
    free_head_ = h->next;
  }
  if (h->next != 0) {
    Slot(h->next)->prev = h->prev;
  }
  h->next = h->prev = kNoLink;
}

S3FifoHandle* S3FifoCacheShard::AllocateSlot(size_t home) {
  // Every unoccupied slot below the used homes is on the free list, so the
  // home slot, when free, is unlinked from wherever it is in O(1).
  S3FifoHandle* h = Slot(SlotIndexAt(home));
  if ((h->flags & S3FifoHandle::kOccupied) == 0) {
    UnlinkFreeSlot(h);
    return h;
  }
  if (free_head_ == 0) {
    return nullptr;
  }
  h = Slot(free_head_);
  UnlinkFreeSlot(h);
  return h;
}

bool S3FifoCacheShard::WouldGrowOnNewInsert() const {
  return used_length_ < max_length_ &&
         (occupancy_.Get() + 1) * kGrowDenominator >
             used_length_ * kGrowNumerator;
}

bool S3FifoCacheShard::AtOccupancyLimit() const {
  return used_length_ == max_length_ &&
         (occupancy_.Get() + 1) * kGrowDenominator >
             max_length_ * kGrowNumerator;
}

S3FifoCacheShard::S3FifoCacheShard(
    size_t capacity, bool strict_capacity_limit, bool use_adaptive_mutex,
    CacheMetadataChargePolicy metadata_charge_policy,
    int /*max_upper_hash_bits*/, MemoryAllocator* allocator,
    const Cache::EvictionCallback* eviction_callback, size_t max_capacity,
    std::shared_ptr<S3FifoHelperRegistry> helpers)
    : CacheShardBase(metadata_charge_policy),
      capacity_(0),
      strict_capacity_limit_(strict_capacity_limit),
      hash_seed_(0),
      epoch_(1),
      length_info_(0),
      slots_base_(nullptr),
      usage_(0),
      // kFirstSlot + homes + 1 slots, in whole groups of three.
      slots_(MemMapping::AllocateLazyZeroedNoReserve(
          (kFirstSlot + MaxLengthFor(std::max(capacity, max_capacity)) + 1 +
           S3FifoSlotGroup::kSlots - 1) /
          S3FifoSlotGroup::kSlots * sizeof(S3FifoSlotGroup))),
      used_length_(0),
      max_length_(MaxLengthFor(std::max(capacity, max_capacity))),
      occupancy_(0),
      probation_usage_(0),
      probation_pinned_usage_(0),
      folded_slot_sum_(0),
      fold_in_flight_(false),
      fold_sum_(0),
      detached_pinned_usage_(0),
      resident_hand_(nullptr),
      aside_slot_(SlotIndexAt(max_length_)),
      aside_unpinned_(false),
      ghost_ring_length_(0),
      ghost_head_seq_(0),
      ghost_size_(0),
      ghost_records_(0),
      mutex_(use_adaptive_mutex),
      allocator_(allocator),
      eviction_callback_(*eviction_callback),
      helpers_(helpers != nullptr ? std::move(helpers)
                                  : std::make_shared<S3FifoHelperRegistry>()),
      pending_(0),
      stamped_seq_(0),
      scan_covers_seq_(0),
      scan_result_(0),
      reclaim_ticks_(0),
      reader_ticks_(0),
      scan_due_(false) {
  // Tests fail the reservation by clearing this pointer to the first slot.
  S3FifoHandle* first_slot =
      slots_.Get() != nullptr ? &slots_.Get()->s[0] : nullptr;
  TEST_SYNC_POINT_CALLBACK("S3FifoCacheShard::S3FifoCacheShard:Reserved",
                           &first_slot);
  slots_base_ = first_slot != nullptr ? slots_.Get() : nullptr;
  if (slots_base_ == nullptr) {
    // The reservation failed: leave the shard empty (the destructor touches
    // no slot) for MakeSharedCache to report.
    used_length_ = 0;
    return;
  }
  // Make empty circular linked lists.
  for (uint16_t queue : {kProbationSlot, kResidentSlot, aside_slot_}) {
    Slot(queue)->next = queue;
    Slot(queue)->prev = queue;
  }
  resident_hand_ = resident();
  used_length_ = std::min(kInitialLength, max_length_);
  length_info_.store(UsedLengthToLengthInfo(used_length_),
                     std::memory_order_relaxed);
  for (size_t i = used_length_; i > 0; --i) {
    PushFreeSlot(SlotIndexAt(i - 1));
  }
  usage_ = GetTableMetaCharge();
  SetCapacity(capacity);
}

S3FifoCacheShard::~S3FifoCacheShard() {
  for (auto& chunk : reader_slot_chunks_) {
    delete[] chunk.load(std::memory_order_relaxed);
  }
  // Entries still referenced are the caller's leak; their values
  // are not freed. Retired slots already released their values.
  for (size_t i = 0; i < used_length_; ++i) {
    S3FifoHandle* h = Slot(SlotIndexAt(i));
    if ((h->flags & S3FifoHandle::kOccupied) == 0) {
      continue;
    }
    if (h->InCache() && !h->HasRefs()) {
      h->FreeValue(*helpers_, allocator_);
    }
    if (h->HasHeapKey()) {
      free(const_cast<S3FifoHandle::HeapKey*>(h->heap_key()));
    }
  }
}

Slice S3FifoCacheShard::KeyOf(const S3FifoHandle* h, UniqueId64x2* buf) const {
  if (h->HasHeapKey()) {
    return h->key();
  }
  const UniqueId64x2 hash = h->GetHash();
  BijectiveUnhash2x64(hash[1], hash[0], &(*buf)[1], &(*buf)[0]);
  (*buf)[0] ^= hash_seed_;
  // NOTE: endian dependence, as in HyperClockCache
  return Slice(reinterpret_cast<const char*>(buf), kInlineKeySize);
}

void S3FifoCacheShard::RemoveFromTable(S3FifoHandle* e) {
  UnlinkFromChain(e);
  assert(occupancy_.Get() > 0);
  occupancy_ -= 1;
}

void S3FifoCacheShard::EraseUnRefEntries() {
  autovector<S3FifoHandle*> last_reference_list;
  {
    DMutexLock l(mutex_);
    for (S3FifoHandle* queue : {probation(), resident(), aside()}) {
      S3FifoHandle* cur = Next(queue);
      while (cur != queue) {
        S3FifoHandle* next = Next(cur);
        uint16_t s = cur->state.load(std::memory_order_relaxed);
        // Acquire pairs with the seq_cst decrement in Release, so the last
        // holder's reads of the value happen before it is freed.
        if ((s & S3FifoHandle::kRefMask) == 0 &&
            cur->state.compare_exchange_strong(s, 0,
                                               std::memory_order_acquire)) {
          assert(s & S3FifoHandle::kInCache);
          FIFO_Remove(cur, (s & S3FifoHandle::kInProbation) != 0);
          RemoveFromTable(cur);
          assert(usage_.Get() >= cur->SlotCharge());
          usage_ -= cur->SlotCharge();
          last_reference_list.push_back(cur);
        }
        cur = next;
      }
    }
  }

  for (S3FifoHandle* entry : last_reference_list) {
    entry->FreeValue(*helpers_, allocator_);
  }
  RetireAll(last_reference_list);

  std::vector<S3FifoHandle::HeapKey*> heap_keys;
  {
    DMutexLock l(mutex_);
    ShrinkTableIfEmpty(&heap_keys);
  }
  FreeHeapKeys(heap_keys);
}

void S3FifoCacheShard::ShrinkTableIfEmpty(
    std::vector<S3FifoHandle::HeapKey*>* heap_keys) {
  if (occupancy_.Get() != 0) {
    return;
  }
  Reclaim(heap_keys);
  // Every chain is empty, so any length is a valid table, but it must keep
  // the slots still held: pinned entries erased from the table, and retired
  // slots a Lookup in flight may still read.
  size_t length = std::min(kInitialLength, max_length_);
  for (size_t i = used_length_; i > length; --i) {
    if ((Slot(SlotIndexAt(i - 1))->flags & S3FifoHandle::kOccupied) != 0) {
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
}

void S3FifoCacheShard::ApplyToSomeEntries(
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
  if (*state == SIZE_MAX || occupancy_.Get() == 0 || begin >= end) {
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
    const S3FifoHandle* h = Slot(static_cast<uint16_t>(i));
    if ((h->flags & S3FifoHandle::kOccupied) != 0 && h->InCache()) {
      callback(KeyOf(h, &buf), h->value(),
               h->GetCharge(metadata_charge_policy_), h->GetHelper(*helpers_));
    }
  }
}

size_t S3FifoCacheShard::TEST_GetProbationUsage() const {
  DMutexLock l(mutex_);
  return probation_usage_;
}

size_t S3FifoCacheShard::TEST_GetProbationUnpinnedUsage() {
  // Allocates the caller's ReaderSlot chunk before mutex_, so that
  // ProbationUnpinnedUsage, called under it, does not allocate.
  MySlot();
  DMutexLock l(mutex_);
  FoldSlots();
  return ProbationUnpinnedUsage();
}

size_t S3FifoCacheShard::TEST_GetFoldedSlotSum() const {
  DMutexLock l(mutex_);
  return folded_slot_sum_;
}

size_t S3FifoCacheShard::TEST_GetProbationBudget() const {
  DMutexLock l(mutex_);
  return ProbationBudget();
}

size_t S3FifoCacheShard::ProbationBudget() const {
  size_t base = capacity_.Get();
  if (AtOccupancyLimit()) {
    const size_t usage = usage_.Get();
    const size_t not_in_table =
        GetTableMetaCharge() + detached_pinned_usage_.Get();
    base = std::min(base, usage > not_in_table ? usage - not_in_table : 0);
  }
  return base / 100 * kProbationPercent +
         (base % 100) * kProbationPercent / 100;
}

size_t S3FifoCacheShard::TEST_GetGhostSize() const {
  DMutexLock l(mutex_);
  return ghost_size_;
}

void S3FifoCacheShard::TEST_GetProbationList(S3FifoHandle** fifo) {
  DMutexLock l(mutex_);
  *fifo = probation();
}

S3FifoCacheShard::Stats S3FifoCacheShard::GetStats() {
  DMutexLock l(mutex_);
  Stats out = stats_;
  for (S3FifoHandle* h = Next(aside()); h != aside(); h = Next(h)) {
    ++out.aside_entries;
  }
  for (const RetiredBatch& batch : limbo_) {
    for (uint16_t idx = batch.first; idx != 0; idx = Slot(idx)->next) {
      ++out.limbo_slots;
    }
  }
  return out;
}

size_t S3FifoCacheShard::TEST_GetAsideSize() {
  DMutexLock l(mutex_);
  size_t size = 0;
  for (S3FifoHandle* h = Next(aside()); h != aside(); h = Next(h)) {
    size++;
  }
  return size;
}

uint64_t S3FifoCacheShard::TEST_GetFreeSlotRebuilds() {
  DMutexLock l(mutex_);
  return free_slot_rebuilds_;
}

bool S3FifoCacheShard::TEST_FreeListMatchesTable() {
  DMutexLock l(mutex_);
  size_t unoccupied = 0;
  for (size_t i = 0; i < used_length_; ++i) {
    if ((Slot(SlotIndexAt(i))->flags & S3FifoHandle::kOccupied) == 0) {
      ++unoccupied;
    }
  }
  size_t listed = 0;
  uint16_t prev = 0;
  for (uint16_t idx = free_head_; idx != 0; idx = Slot(idx)->next) {
    const S3FifoHandle* h = Slot(idx);
    if (idx < kFirstSlot || idx >= SlotIndexAt(used_length_) ||
        (h->flags & S3FifoHandle::kOccupied) != 0 || h->prev != prev ||
        ++listed > unoccupied) {
      return false;
    }
    prev = idx;
  }
  return listed == unoccupied;
}

size_t S3FifoCacheShard::TEST_GetProbationSize() {
  DMutexLock l(mutex_);
  size_t size = 0;
  for (S3FifoHandle* h = Next(probation()); h != probation(); h = Next(h)) {
    size++;
  }
  return size;
}

void S3FifoCacheShard::FIFO_Remove(S3FifoHandle* e, bool in_probation) {
  assert(e->next != kNoLink);
  assert(e->prev != kNoLink);
  if (e == resident_hand_) {
    S3FifoHandle* next = ResidentNext(e);
    resident_hand_ = next == e ? resident() : next;
  }
  Next(e)->prev = e->prev;
  Prev(e)->next = e->next;
  e->prev = e->next = kNoLink;
  e->aside.store(0, std::memory_order_relaxed);
  if (in_probation) {
    assert(probation_usage_ >= e->SlotCharge());
    probation_usage_ -= e->SlotCharge();
  }
  // Queue membership does not affect usage_.
}

void S3FifoCacheShard::FIFO_Append(S3FifoHandle* queue, S3FifoHandle* e) {
  assert(e->InProbation() == (queue == probation()));
  LinkAtTail(queue, e);
  if (queue == probation()) {
    probation_usage_ += e->SlotCharge();
  }
}

void S3FifoCacheShard::LinkAtTail(S3FifoHandle* queue, S3FifoHandle* e) {
  assert(e->next == kNoLink);
  assert(e->prev == kNoLink);
  // Append to tail. For probation and aside queue->prev is newest and
  // queue->next is oldest. For resident the tail is just before the hand,
  // which is the dummy head itself when resident is empty.
  S3FifoHandle* before = queue == resident() ? resident_hand_ : queue;
  const uint16_t idx = IndexOf(e);
  e->next = IndexOf(before);
  e->prev = before->prev;
  Prev(e)->next = idx;
  before->prev = idx;
  if (queue == resident() && resident_hand_ == resident()) {
    resident_hand_ = e;
  }
}

void S3FifoCacheShard::MoveToTail(S3FifoHandle* e, S3FifoHandle* queue) {
  FIFO_Remove(e, /*in_probation=*/false);
  LinkAtTail(queue, e);
}

void S3FifoCacheShard::SetAside(S3FifoHandle* e) {
  MoveToTail(e, aside());
  TEST_SYNC_POINT_CALLBACK("S3FifoCacheShard::SetAside:BeforeFlag", e);
  // A last Release of e that runs concurrently either sees the flag (and
  // flags the shard) or has already dropped the ref this reload sees: both
  // sides are seq_cst.
  e->aside.store(1, std::memory_order_seq_cst);
  if ((e->state.load(std::memory_order_seq_cst) & S3FifoHandle::kRefMask) ==
      0) {
    // Relaxed: set and taken (EvictNeed) under mutex_.
    aside_unpinned_.store(true, std::memory_order_relaxed);
  }
}

void S3FifoCacheShard::ReturnUnpinnedAside() {
  for (int i = 0; i < kAsideChecksPerEviction && Next(aside()) != aside();
       ++i) {
    TEST_SYNC_POINT("S3FifoCacheShard::EvictionVisit");
    S3FifoHandle* e = Next(aside());
    // A Lookup may pin e right after this check; it is then a pinned entry
    // in its queue, as after any other race with a hit.
    if (e->HasRefs()) {
      SetAside(e);
    } else {
      MoveToTail(e, e->InProbation() ? probation() : resident());
    }
  }
}

void S3FifoCacheShard::FoldSlots() {
  for (size_t c = 0; c < kReaderSlotChunks; ++c) {
    const ReaderSlot* slots =
        reader_slot_chunks_[c].load(std::memory_order_acquire);
    if (slots == nullptr) {
      continue;
    }
    const size_t first = c * kReaderSlotsPerChunk;
    if (folded_slot_values_.size() < first + kReaderSlotsPerChunk) {
      folded_slot_values_.resize(first + kReaderSlotsPerChunk, 0);
    }
    for (size_t i = 0; i < kReaderSlotsPerChunk; ++i) {
      const size_t v =
          slots[i].probation_pinned.load(std::memory_order_relaxed);
      size_t& folded = folded_slot_values_[first + i];
      folded_slot_sum_ += v - folded;
      folded = v;
    }
  }
}

void S3FifoCacheShard::FoldReaderSlots() {
  TEST_SYNC_POINT("S3FifoCacheShard::FoldReaderSlots");
  size_t chunks = 0;
  for (size_t c = 0; c < kReaderSlotChunks; ++c) {
    if (reader_slot_chunks_[c].load(std::memory_order_acquire) != nullptr) {
      chunks = c + 1;
    }
  }
  // Chunks are never freed, so a chunk seen above is still installed below;
  // one installed after the first pass is left to the next fold.
  fold_values_.assign(chunks * kReaderSlotsPerChunk, 0);
  size_t sum = 0;
  for (size_t c = 0; c < chunks; ++c) {
    const ReaderSlot* slots =
        reader_slot_chunks_[c].load(std::memory_order_acquire);
    if (slots == nullptr) {
      continue;
    }
    for (size_t i = 0; i < kReaderSlotsPerChunk; ++i) {
      const size_t v =
          slots[i].probation_pinned.load(std::memory_order_relaxed);
      fold_values_[c * kReaderSlotsPerChunk + i] = v;
      sum += v;
    }
  }
  fold_sum_ = sum;
  DMutexLock l(mutex_);
  // A FoldSlots run during the walk (SetCapacity) is replaced by this
  // reading, at most one walk older.
  folded_slot_values_.swap(fold_values_);
  folded_slot_sum_ = fold_sum_;
  fold_in_flight_ = false;
}

size_t S3FifoCacheShard::ProbationUnpinnedUsage() {
  const ReaderSlot* own = MySlot();
  size_t own_live = 0;
  size_t own_folded = 0;
  if (own != nullptr) {
    own_live = own->probation_pinned.load(std::memory_order_relaxed);
    if (own->index < folded_slot_values_.size()) {
      own_folded = folded_slot_values_[own->index];
    }
  }
  const size_t pinned =
      probation_pinned_usage_ + folded_slot_sum_ + own_live - own_folded;
  // The caller's share is live and the others' come from the last fold, so
  // the sum wraps below zero when the caller released pins another thread
  // took since then. The pinned charge is never negative, so that means
  // none is pinned, not all.
  if (pinned > std::numeric_limits<size_t>::max() / 2) {
    return probation_usage_;
  }
  // Above probation_usage_ while a ref change is in flight, or until the
  // next fold when another thread released pins the caller took since then.
  return pinned >= probation_usage_ ? 0 : probation_usage_ - pinned;
}

bool S3FifoCacheShard::IsPlaceholder(const Cache::CacheItemHelper* helper,
                                     size_t charge) {
  return helper == &kNoopCacheItemHelper && charge == 0;
}

bool S3FifoCacheShard::IsPlaceholder(const S3FifoHandle* e) const {
  return IsPlaceholder(e->GetHelper(*helpers_),
                       e->GetCharge(metadata_charge_policy_));
}

bool S3FifoCacheShard::ReturnUnpinnedAsideChunk(S3FifoHandle* last) {
  // Consumes the aside head each step, as ReturnUnpinnedAside does, so a
  // pinned entry moves to the aside tail behind last and is not revisited.
  for (size_t i = 0; i < kAsideReturnChunk; ++i) {
    if (Next(aside()) == aside()) {
      return true;  // Empty (last == aside() when it began empty).
    }
    TEST_SYNC_POINT("S3FifoCacheShard::EvictionVisit");
    S3FifoHandle* e = Next(aside());
    const bool at_last = e == last;
    if (e->HasRefs()) {
      SetAside(e);
    } else {
      MoveToTail(e, e->InProbation() ? probation() : resident());
    }
    if (at_last) {
      return true;
    }
  }
  return false;
}

S3FifoHandle* S3FifoCacheShard::ResidentNext(S3FifoHandle* e) {
  S3FifoHandle* next = Next(e);
  return next == resident() ? Next(next) : next;
}

void S3FifoCacheShard::EvictFromFIFO(size_t charge,
                                     autovector<S3FifoHandle*>* deleted) {
  if (FitsIn(usage_.Get(), charge, capacity_.Get())) {
    return;
  }
  EvictionData data;
  data.need_charge = Excess(usage_.Get(), charge, capacity_.Get());
  EvictNeed(&data, deleted);
}

void S3FifoCacheShard::EvictNeed(EvictionData* data,
                                 autovector<S3FifoHandle*>* deleted) {
  ReturnUnpinnedAside();
  EvictFromQueues(data, deleted);
  // Eviction returns only kAsideChecksPerEviction aside entries per call,
  // so after a burst of pins is released most of the shard may wait on the
  // aside list. When it falls short, return them kAsideReturnChunk at a
  // time, evicting after each chunk, until the need is met or every aside
  // entry present at the start was visited. Stopping early leaves the flag
  // set, so a later eviction continues. A shard whose aside entries all stay
  // pinned skips the walk.
  // Acquire pairs with the release store of the flag in a lock-free Release, so
  // the walk sees that Release's unref. The stores made under mutex_ (SetAside,
  // ReleaseLocked, the re-store below) need no ordering of their own.
  if (!data->Done() && aside_unpinned_.load(std::memory_order_relaxed) &&
      aside_unpinned_.exchange(false, std::memory_order_acquire)) {
    S3FifoHandle* const last = Prev(aside());
    bool walked = false;
    while (!walked && !data->Done()) {
      walked = ReturnUnpinnedAsideChunk(last);
      EvictFromQueues(data, deleted);
    }
    if (!walked) {
      aside_unpinned_.store(true, std::memory_order_relaxed);
    }
  }
}

void S3FifoCacheShard::EvictFromQueues(EvictionData* data,
                                       autovector<S3FifoHandle*>* deleted) {
  // The budget selects the queue from the current unpinned probation
  // charge, before the incoming entry is accounted. Pinned entries are
  // excluded: they can be neither evicted nor promoted, so counting them
  // would hold probation over budget for as long as they stay pinned and
  // the resident clock would never run. Either pass can free nothing (all
  // pinned), so the other queue is always tried when need remains.
  const bool probation_first = (Next(resident()) == resident()) ||
                               (ProbationUnpinnedUsage() > ProbationBudget());
  if (probation_first) {
    EvictFromProbation(data, deleted);
    if (!data->Done()) {
      EvictFromResident(data, deleted);
    }
  } else {
    EvictFromResident(data, deleted);
    if (!data->Done()) {
      // The resident pass above fell short. The probation pass may have
      // promoted unpinned entries into resident at freq 0, which a second
      // resident pass can evict once its hand reaches them (after every older
      // resident entry), so resident runs again when the probation pass
      // promoted anything and need remains.
      const bool promoted = EvictFromProbation(data, deleted);
      if (promoted && !data->Done()) {
        EvictFromResident(data, deleted);
      }
    }
  }
}

void S3FifoCacheShard::EvictClaimed(S3FifoHandle* e, EvictionData* data,
                                    autovector<S3FifoHandle*>* deleted) {
  RemoveFromTable(e);
  assert(usage_.Get() >= e->SlotCharge());
  usage_ -= e->SlotCharge();
  data->freed_charge += e->SlotCharge();
  ++data->freed_count;
  deleted->push_back(e);
}

bool S3FifoCacheShard::EvictFromProbation(EvictionData* data,
                                          autovector<S3FifoHandle*>* deleted) {
  // Each step consumes the head: promote, evict, or move a pinned entry to
  // the tail (or aside). The pass ends after the entry that was newest when it
  // began, so it visits at most the initial number of entries and never
  // revisits a moved pin. Promotion frees nothing; the pass continues. Moves
  // touch only links and the probation charge: usage_ is unchanged. Promotion
  // and eviction claim the entry with a CAS that requires refs == 0, so a
  // concurrent Lookup either pins it first (and it moves to the tail as pinned)
  // or loses the race: after an eviction it misses; after a promotion it
  // retries and hits the now-resident entry. The pinned move never touches
  // state.
  bool promoted = false;
  size_t rotated = 0;
  S3FifoHandle* const last = Prev(probation());
  bool at_last = Next(probation()) == probation();
  while (!at_last && !data->Done()) {
    TEST_SYNC_POINT("S3FifoCacheShard::EvictionVisit");
    S3FifoHandle* cur = Next(probation());
    at_last = cur == last;
    uint16_t s = cur->state.load(std::memory_order_relaxed);
    assert(s & S3FifoHandle::kInCache);
    assert(s & S3FifoHandle::kInProbation);
    const bool promote =
        cur->freq.load(std::memory_order_relaxed) >= kPromoteThreshold;
    // Acquire pairs with the seq_cst decrement in Release, so the last
    // holder's reads of the value happen before an evicted entry is freed.
    if ((s & S3FifoHandle::kRefMask) != 0 ||
        !cur->state.compare_exchange_strong(
            s,
            promote ? static_cast<uint16_t>(s & ~S3FifoHandle::kInProbation)
                    : uint16_t{0},
            std::memory_order_acquire)) {
      if (rotated++ < kPinnedRotationsPerPass) {
        MoveToTail(cur, probation());
        ++stats_.pinned_rotations;
      } else {
        SetAside(cur);
        ++stats_.set_aside;
      }
    } else {
      if (promote) {
        FIFO_Remove(cur, /*in_probation=*/true);
        FIFO_Append(resident(), cur);
        cur->freq.store(0, std::memory_order_relaxed);
        promoted = true;
      } else {
        if (cur->freq.load(std::memory_order_relaxed) == 0) {
          ++stats_.evicted_without_hit;
        }
        FIFO_Remove(cur, /*in_probation=*/true);
        if (!IsPlaceholder(cur)) {
          GhostRecord(GhostHash(cur->GetHash()));
        }
        EvictClaimed(cur, data, deleted);
      }
    }
  }
  return promoted;
}

void S3FifoCacheShard::EvictFromResident(EvictionData* data,
                                         autovector<S3FifoHandle*>* deleted) {
  // Every step rotates (advances the hand past), evicts or sets aside the
  // head; a pass ends when the hand cycles back to its marker. The marker
  // is re-anchored by breaking out on every eviction. An eviction claims the
  // head with a CAS that requires refs == 0; losing it to a concurrent Lookup
  // is the same as meeting a pinned head.
  int idle_passes = 0;
  // Pinned entries passed in this call since the last idle pass. A pass that
  // evicts or re-anchors ends early and does not reset it, so it bounds the
  // pinned entries passed per call until an idle pass; when evicting passes
  // carry the hand around resident more than once, it counts the same pinned
  // entry again.
  size_t rotated = 0;
  while (!data->Done() && Next(resident()) != resident() &&
         idle_passes < kMaxResidentIdlePasses) {
    S3FifoHandle* start = resident_hand_;
    bool evicted_this_pass = false;
    bool reanchored = false;
    do {
      TEST_SYNC_POINT("S3FifoCacheShard::EvictionVisit");
      // The entry at the hand (unrelated to S3FifoHandle::head): the oldest in
      // clock order, which this step rotates past, evicts or sets aside.
      S3FifoHandle* head = resident_hand_;
      uint16_t s = head->state.load(std::memory_order_relaxed);
      const uint8_t freq = head->freq.load(std::memory_order_relaxed);
      if ((s & S3FifoHandle::kRefMask) == 0 && freq > 0) {
        // Recently hit: spend one hit and rotate.
        head->freq.store(freq - 1, std::memory_order_relaxed);
        resident_hand_ = ResidentNext(head);
        continue;
      }
      // Acquire pairs with the seq_cst decrement in Release, so the last
      // holder's reads of the value happen before it is freed.
      if ((s & S3FifoHandle::kRefMask) == 0 &&
          head->state.compare_exchange_strong(s, 0,
                                              std::memory_order_acquire)) {
        assert(s == S3FifoHandle::kInCache);
        FIFO_Remove(head, /*in_probation=*/false);
        EvictClaimed(head, data, deleted);
        evicted_this_pass = true;
        idle_passes = 0;
        break;
      }
      // Pinned, or the claim lost to a concurrent Lookup.
      if (rotated++ < kPinnedRotationsPerPass) {
        resident_hand_ = ResidentNext(head);
        ++stats_.pinned_rotations;
        continue;
      }
      // FIFO_Remove moves the hand past head. Setting the marker aside
      // re-anchors the pass, as an eviction does, without counting it idle:
      // every such step removes an entry.
      SetAside(head);
      ++stats_.set_aside;
      if (head == start) {
        reanchored = true;
        break;
      }
    } while (!data->Done() && Next(resident()) != resident() &&
             resident_hand_ != start);
    if (!evicted_this_pass && !reanchored) {
      ++idle_passes;
      rotated = 0;
    }
  }
}

void S3FifoCacheShard::GhostRecord(uint32_t hash) {
  ++ghost_records_;
  if (ghost_size_ == ghost_ring_length_) {
    // Double the ring, keeping each record at its sequence number. It starts
    // at 16 records (64 bytes), small for tiny shards; doubling reaches any
    // bound in a few steps.
    const size_t new_length =
        ghost_ring_length_ == 0 ? size_t{16} : ghost_ring_length_ * 2;
    std::unique_ptr<uint32_t[]> new_ring(new uint32_t[new_length]);
    for (uint64_t seq = ghost_head_seq_; seq < ghost_head_seq_ + ghost_size_;
         ++seq) {
      new_ring[seq & (new_length - 1)] =
          ghost_ring_[seq & (ghost_ring_length_ - 1)];
    }
    // Every index entry names a live record; move it with its record.
    const uint64_t head = ghost_head_seq_;
    const size_t old_mask = ghost_ring_length_ - 1;
    ghost_index_.Resize(new_length, [&](uint32_t pos) {
      const uint64_t seq = head + ((pos - head) & old_mask);
      return static_cast<uint32_t>(seq & (new_length - 1));
    });
    ghost_ring_ = std::move(new_ring);
    ghost_ring_length_ = new_length;
  }
  const uint64_t seq = ghost_head_seq_ + ghost_size_;
  const uint32_t pos = static_cast<uint32_t>(seq & (ghost_ring_length_ - 1));
  ghost_index_.Set(hash, pos);
  ghost_ring_[pos] = hash;
  ++ghost_size_;
  GhostTrim();
}

void S3FifoCacheShard::GhostTrim() {
  while (ghost_size_ > occupancy_.Get()) {
    const uint32_t pos =
        static_cast<uint32_t>(ghost_head_seq_ & (ghost_ring_length_ - 1));
    const size_t i = ghost_index_.Find(ghost_ring_[pos]);
    if (i != S3FifoGhostIndex::kNotFound && ghost_index_.PosAt(i) == pos) {
      ghost_index_.EraseAt(i);
    }
    --ghost_size_;
    ++ghost_head_seq_;
  }
}

bool S3FifoCacheShard::GhostTake(uint32_t hash) {
  return ghost_index_.Erase(hash);
}

void S3FifoCacheShard::NotifyEvicted(
    const autovector<S3FifoHandle*>& evicted_handles) {
  autovector<S3FifoHandle*> slots;
  UniqueId64x2 buf;
  for (S3FifoHandle* entry : evicted_handles) {
    // was_hit stays false: resident freq decays and promotion resets it, so
    // freq does not record ever-hit for every victim (probation victims at
    // freq 0 were never hit; see Stats::evicted_without_hit). Sticky hit
    // tracking waits for a consumer.
    // A callback returning true took ownership of obj.
    if (!eviction_callback_ ||
        !eviction_callback_(KeyOf(entry, &buf), entry, false)) {
      // Free the entries here outside of mutex for performance reasons.
      entry->FreeValue(*helpers_, allocator_);
    }
    if (entry->IsStandalone()) {
      FreeStandalone(entry);
    } else {
      slots.push_back(entry);
    }
  }
  RetireAll(slots);
}

void S3FifoCacheShard::RetireAll(const autovector<S3FifoHandle*>& handles) {
  if (handles.empty()) {
    return;
  }
  for (size_t i = 0; i + 1 < handles.size(); ++i) {
    handles[i]->next = IndexOf(handles[i + 1]);
  }
  Retire(handles.front(), handles.back());
}

void S3FifoCacheShard::Retire(S3FifoHandle* first, S3FifoHandle* last) {
  // Release pairs with the acquire exchange in StampPending, so the lock holder
  // that takes the chain reads the next links written here and in RetireAll
  // without the lock. Each push is a read-modify-write, so the exchange also
  // synchronizes with every earlier push (a release sequence).
  last->next = pending_.load(std::memory_order_relaxed);
  while (!pending_.compare_exchange_weak(last->next, IndexOf(first),
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
  }
}

size_t S3FifoCacheShard::ReaderWorkInterval() const {
  const size_t readers = ThreadReaderCount().load(std::memory_order_relaxed);
  // Retired slots wait for the next scan, so the interval stays within an
  // eighth of the homes in use, well inside the room the load factor
  // leaves free.
  return std::max<size_t>(
      kReclaimInterval,
      std::min(readers / kReadersPerReaderTick, used_length_ / 8));
}

S3FifoCacheShard::ReaderWork S3FifoCacheShard::MaybeReclaim(
    std::vector<S3FifoHandle::HeapKey*>* heap_keys) {
  ReaderWork work;
  if (++reclaim_ticks_ < kReclaimInterval) {
    return work;
  }
  reclaim_ticks_ = 0;
  // Work that visits every reader in the process (folding their pin slots and
  // scanning their announcements) runs once per ReaderWorkInterval() calls:
  // about kReadersPerReaderTick readers per call once the table has as many
  // homes as there are readers, more in a smaller table (see
  // ReaderWorkInterval).
  reader_ticks_ += kReclaimInterval;
  if (reader_ticks_ >= ReaderWorkInterval()) {
    reader_ticks_ = 0;
    if (!fold_in_flight_) {
      fold_in_flight_ = true;
      work.fold = true;
    }
    scan_due_ = true;
  }
  StampPending();
  if (scan_covers_seq_ != 0) {
    const uint64_t oldest = scan_result_.exchange(0, std::memory_order_acquire);
    if (oldest == 0) {
      // Not scanned yet.
      return work;
    }
    FreeBatches(scan_covers_seq_, oldest, heap_keys);
    scan_covers_seq_ = 0;
  }
  if (!scan_due_ || limbo_.empty()) {
    return work;
  }
  scan_due_ = false;
  // The caller scans after this critical section, so after every batch
  // stamped so far was unlinked.
  scan_covers_seq_ = stamped_seq_;
  work.scan = true;
  return work;
}

void S3FifoCacheShard::RunReaderWork(const ReaderWork& work) {
  if (work.fold) {
    FoldReaderSlots();
  }
  if (work.scan) {
    ScanReaders();
  }
}

void S3FifoCacheShard::Reclaim(std::vector<S3FifoHandle::HeapKey*>* heap_keys) {
  StampPending();
  if (!limbo_.empty()) {
    FreeBatches(stamped_seq_, OldestAnnouncedEpoch(), heap_keys);
  }
}

void S3FifoCacheShard::StampPending() {
  // Acquire pairs with the release CAS in Retire.
  const uint16_t first = pending_.exchange(0, std::memory_order_acquire);
  if (first != 0) {
    // Only the lock holder writes epoch_.
    limbo_.push_back(
        {epoch_.load(std::memory_order_relaxed), ++stamped_seq_, first});
  }
}

uint64_t S3FifoCacheShard::OldestAnnouncedEpoch() const {
  // Pairs with the fence in Lookup: a Lookup whose announcement this scan
  // misses starts after everything unlinked before the fence, so it cannot
  // reach it.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  uint64_t oldest = std::numeric_limits<uint64_t>::max();
  for (const ThreadReader* r = ThreadReaders().load(std::memory_order_acquire);
       r != nullptr; r = r->next) {
    const uint64_t announced = r->epoch.load(std::memory_order_acquire);
    // Acquire pairs with the release store of shard in Lookup and
    // GetPinnedUsage: a reader seen here on another shard has finished every
    // read of its earlier Lookup on this one, so skipping it is safe. Relaxed
    // would let a weakly ordered CPU (aarch64) publish the new shard before
    // those reads end, and this scan would free slots they still read.
    if (announced != 0 && r->shard.load(std::memory_order_acquire) == this) {
      oldest = std::min(oldest, announced);
    }
  }
  return oldest;
}

void S3FifoCacheShard::ScanReaders() {
  TEST_SYNC_POINT("S3FifoCacheShard::ScanReaders");
  scan_result_.store(OldestAnnouncedEpoch(), std::memory_order_release);
}

void S3FifoCacheShard::FreeBatches(
    uint64_t max_seq, uint64_t oldest,
    std::vector<S3FifoHandle::HeapKey*>* heap_keys) {
  size_t done = 0;
  for (; done < limbo_.size() && limbo_[done].seq <= max_seq &&
         limbo_[done].epoch < oldest;
       ++done) {
    uint16_t idx = limbo_[done].first;
    while (idx != 0) {
      S3FifoHandle* h = Slot(idx);
      const uint16_t next = h->next;
      if (h->HasHeapKey()) {
        heap_keys->push_back(const_cast<S3FifoHandle::HeapKey*>(h->heap_key()));
      }
      h->flags = 0;
      PushFreeSlot(idx);
      idx = next;
    }
  }
  limbo_.erase(limbo_.begin(), limbo_.begin() + done);
  const uint64_t epoch = epoch_.load(std::memory_order_relaxed);
  if (!limbo_.empty() && limbo_.front().seq <= max_seq &&
      limbo_.back().epoch == epoch) {
    // Lookups that start from now announce a later epoch than any batch left,
    // so those batches wait only for Lookups already in flight. Release pairs
    // with the acquire load of epoch_ in Lookup and GetPinnedUsage: one that
    // loads the new epoch sees every unlink of the slots in those batches, so
    // it cannot reach them.
    epoch_.store(epoch + 1, std::memory_order_release);
  }
}

void S3FifoCacheShard::RebuildFreeSlots() {
  ++free_slot_rebuilds_;
  free_head_ = 0;
  for (size_t i = used_length_; i > 0; --i) {
    const uint16_t idx = SlotIndexAt(i - 1);
    if ((Slot(idx)->flags & S3FifoHandle::kOccupied) == 0) {
      PushFreeSlot(idx);
    }
  }
}

void S3FifoCacheShard::FreeHeapKeys(
    const std::vector<S3FifoHandle::HeapKey*>& keys) {
  for (S3FifoHandle::HeapKey* hk : keys) {
    free(hk);
  }
}

uint32_t S3FifoCacheShard::TEST_ThreadReaderIndex() {
  const ThreadReader* reader = OwnThreadReader();
  return reader == nullptr ? std::numeric_limits<uint32_t>::max()
                           : reader->index;
}

size_t S3FifoCacheShard::TEST_SetPreferredReaders(size_t bound) {
  return PreferredReaders().exchange(bound, std::memory_order_relaxed);
}

S3FifoCacheShard::ReaderSlot* S3FifoCacheShard::MySlot() {
  const ThreadReader* reader = OwnThreadReader();
  if (UNLIKELY(reader == nullptr)) {
    // Exiting thread: its reader may already belong to another thread.
    return nullptr;
  }
  const size_t chunk = reader->index / kReaderSlotsPerChunk;
  if (UNLIKELY(chunk >= kReaderSlotChunks)) {
    return nullptr;
  }
  ReaderSlot* slots =
      reader_slot_chunks_[chunk].load(std::memory_order_acquire);
  if (UNLIKELY(slots == nullptr)) {
    auto* fresh = new ReaderSlot[kReaderSlotsPerChunk];
    for (size_t i = 0; i < kReaderSlotsPerChunk; ++i) {
      fresh[i].index = static_cast<uint32_t>(chunk * kReaderSlotsPerChunk + i);
    }
    if (reader_slot_chunks_[chunk].compare_exchange_strong(
            slots, fresh, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      slots = fresh;
    } else {
      delete[] fresh;
    }
  }
  return &slots[reader->index % kReaderSlotsPerChunk];
}

void S3FifoCacheShard::CreditProbationPin(size_t delta) {
  ReaderSlot* slot = MySlot();
  if (LIKELY(slot != nullptr)) {
    slot->AddProbationPinned(delta);
  } else {
    DMutexLock l(mutex_);
    probation_pinned_usage_ += delta;
  }
}

void S3FifoCacheShard::SetCapacity(size_t capacity) {
  autovector<S3FifoHandle*> last_reference_list;
  // Allocates the caller's ReaderSlot chunk before mutex_, so that
  // ProbationUnpinnedUsage, called under it, does not allocate.
  MySlot();
  {
    DMutexLock l(mutex_);
    capacity_ = capacity;
    // Fresh pin counts for the queue choice: folds otherwise run only once
    // per ReaderWorkInterval() calls.
    FoldSlots();
    // The probation budget is derived at each use (see ProbationBudget), so
    // it already tracks the new capacity when eviction below runs.
    EvictFromFIFO(0, &last_reference_list);
    GhostTrim();
  }

  NotifyEvicted(last_reference_list);

  // A cut that evicts every entry leaves the grown table charged: shrink
  // the emptied table once the evicted slots are retired, as
  // EraseUnRefEntries does, so its metadata charge does not crowd out new
  // entries.
  std::vector<S3FifoHandle::HeapKey*> heap_keys;
  {
    DMutexLock l(mutex_);
    if (occupancy_.Get() == 0 && TableAboveInitialLength()) {
      ShrinkTableIfEmpty(&heap_keys);
    }
  }
  FreeHeapKeys(heap_keys);
}

void S3FifoCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  DMutexLock l(mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

S3FifoStandaloneHandle* S3FifoCacheShard::NewStandalone(
    HashCref hash, S3FifoHandle::HeapKey* heap_key, Cache::ObjectPtr value,
    const Cache::CacheItemHelper* helper, size_t total_charge) {
  auto* e = new (port::cacheline_aligned_alloc(sizeof(S3FifoStandaloneHandle)))
      S3FifoStandaloneHandle();
  SetKey(e, hash, heap_key, S3FifoHandle::kStandalone);
  e->set_value(value);
  e->helper = helper;
  e->total_charge = total_charge;
  e->next = e->prev = kNoLink;
  return e;
}

void S3FifoCacheShard::FreeStandalone(S3FifoHandle* h) {
  assert(h->IsStandalone());
  if (h->HasHeapKey()) {
    free(const_cast<S3FifoHandle::HeapKey*>(h->heap_key()));
  }
  auto* standalone = static_cast<S3FifoStandaloneHandle*>(h);
  standalone->~S3FifoStandaloneHandle();
  port::cacheline_aligned_free(standalone);
}

Status S3FifoCacheShard::InsertItem(const Slice& key, HashCref hash,
                                    S3FifoHandle::HeapKey* heap_key,
                                    Cache::ObjectPtr value,
                                    const Cache::CacheItemHelper* helper,
                                    size_t total_charge,
                                    S3FifoHandle** handle) {
  // Steps, all under mutex_: 1. reclaim retired slots
  // (MaybeReclaim); 2. evict by the policy for the charge; 3. shrink a table
  // left empty; 4. grow the table for a new key; 5. at the table's limit,
  // evict one entry by occupancy; 6. allocate a slot (reclaiming, or growing
  // past pinned erased slots, if none is free); 7. fill and publish the entry
  // and detach an overwritten one; 8a. without a slot (slotless, or none
  // free), hand out a standalone entry to an insert with a handle (under
  // strict_capacity_limit admitted only if it fits); 8b. with no entry (not
  // admitted, or 8a gave none), drop the insert (no handle) or refuse it
  // (with a handle). Then eviction callbacks, frees and any reader walk
  // MaybeReclaim asked for run without the lock.
  Status s = Status::OK();
  autovector<S3FifoHandle*> last_reference_list;
  std::vector<S3FifoHandle::HeapKey*> heap_keys;
  // total_charge includes the heap key's metadata charge, as an entry's does
  // (see S3FifoHandle::GetCharge).
  const bool placeholder = IsPlaceholder(
      helper, total_charge - HeapKeyCharge(heap_key, metadata_charge_policy_));
  // A slot holds a one-byte helper index and a 32-bit charge. An entry that
  // needs more is slotless: inserted with a handle it is standalone
  // (S3FifoStandaloneHandle), except that under strict_capacity_limit it is
  // refused when it does not fit, as CreateStandalone(allow_uncharged=false)
  // does; without a handle it is dropped. Registered before mutex_, as the
  // registry has its own lock.
  const uint8_t helper_index = helpers_->IndexOf(helper);
  const bool helper_overflow = helper_index == S3FifoHelperRegistry::kFull;
  const bool large_charge = total_charge > std::numeric_limits<uint32_t>::max();
  const bool slotless = helper_overflow || large_charge;

  // Allocates the caller's ReaderSlot chunk, which eviction reads, before
  // mutex_. An insert with a handle credits its probation pin there too.
  // my_slot is the thread's ReaderSlot, not a table slot.
  ReaderSlot* const my_slot = MySlot();
  ReaderWork work;
  {
    DMutexLock l(mutex_);
    // Step 1: reclaim retired slots.
    work = MaybeReclaim(&heap_keys);

    // Free space by the eviction policy until enough space is freed or no
    // unpinned entry remains. An entry that can never fit is refused (strict)
    // or dropped (no handle) below, so evicting for it would only empty the
    // shard; so is a slotless entry without a handle. A slotless entry with a
    // handle evicts, also under strict_capacity_limit, as CreateStandalone
    // evicts before checking that its entry fits. A placeholder never uses the
    // ghost (see ghost_index_).
    const size_t ghost = placeholder || slotless
                             ? S3FifoGhostIndex::kNotFound
                             : ghost_index_.Find(GhostHash(hash));
    const bool remembered = ghost != S3FifoGhostIndex::kNotFound;
    const uint64_t ghost_records_before = ghost_records_;
    // Step 2: evict by the policy for the charge.
    const bool skip_eviction =
        handle == nullptr ? slotless || NeverFits(total_charge)
                          : strict_capacity_limit_ && NeverFits(total_charge);
    if (!skip_eviction) {
      EvictFromFIFO(total_charge, &last_reference_list);
    }

    // Step 3: shrink a table left empty.
    if (occupancy_.Get() == 0 && TableAboveInitialLength()) {
      // The table is empty but still grown: step 2 evicted every entry, every
      // entry was erased (Erase and an erasing Release never shrink), or slots
      // that blocked an earlier shrink (after a capacity cut or
      // EraseUnRefEntries) have since been freed: detached entries that were
      // still pinned, or retired slots awaiting reclamation. Shrink it, so its
      // metadata charge does not crowd out the entry.
      ShrinkTableIfEmpty(&heap_keys);
    }

    S3FifoHandle* e = nullptr;
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
      // it, usage ends above capacity only when eviction cannot free enough
      // (every remaining entry pinned, or every resident entry re-hit faster
      // than the clock sweeps; see kMaxResidentIdlePasses), or when an
      // overwrite finds no free slot and the table grows past capacity.
      S3FifoHandle* old = FindInTable(key, hash);
      // A slotless entry skips the table: growth, the occupancy eviction
      // and the slot itself.
      if (!slotless) {
        // Step 4: grow the table for a new key.
        // A new key whose insert would grow the table also evicts for the
        // growth's metadata charge. Strict: grow only if the growth then
        // fits; skipping eviction here would turn away every insert into a
        // full table while unpinned entries remain. Non-strict: evicting here
        // keeps the growth from leaving usage over capacity.
        if (old == nullptr && WouldGrowOnNewInsert()) {
          const size_t growth = TableGrowthChargeOnInsert(key, hash);
          const size_t needed = SaturatingAdd(total_charge, growth);
          if (!FitsIn(usage_.Get(), needed, capacity_.Get())) {
            EvictionData data;
            data.need_charge = Excess(usage_.Get(), needed, capacity_.Get());
            EvictNeed(&data, &last_reference_list);
            // No WouldGrowOnNewInsert() recheck: EvictNeed changes neither
            // used_length_ nor max_length_, so GrowOneHome's precondition still
            // holds. Eviction can only lower occupancy_, and then this step
            // comes one home early, already paid for and within capacity. The
            // loop below rechecks before any further step.
            if (strict_capacity_limit_ &&
                FitsIn(usage_.Get(), needed, capacity_.Get())) {
              GrowOneHomeCharged();
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
          GrowOneHomeCharged();
        }
        // Step 5: at the table's limit, evict one entry by occupancy.
        if (old == nullptr && AtOccupancyLimit()) {
          // The table is as large as it gets: also evict one entry by
          // occupancy, keeping its load factor, so retired slots have time to
          // be reclaimed. An overwrite keeps occupancy, and this eviction
          // could pick old itself, which Detach below must find in cache.
          EvictionData data;
          data.need_count = 1;
          EvictNeed(&data, &last_reference_list);
          stats_.occupancy_evictions += data.freed_count;
        }
        // Step 6: allocate a slot, reclaiming or growing if none is free.
        const size_t home =
            HomeOf(length_info_.load(std::memory_order_relaxed), hash);
        e = AllocateSlot(home);
        if (e == nullptr) {
          Reclaim(&heap_keys);
          e = AllocateSlot(home);
        }
        if (e == nullptr && used_length_ < max_length_) {
          // Entries erased while pinned keep their slots but leave the load
          // factor's count, and retired slots still wait for reclamation, so
          // the table can run out of slots below it. Grow
          // past them: under strict_capacity_limit only when the step fits,
          // otherwise even past capacity, as long as usage_ cannot wrap.
          const size_t needed = SaturatingAdd(total_charge, HomeCharge());
          if (old == nullptr &&
              !FitsIn(usage_.Get(), needed, capacity_.Get())) {
            EvictionData data;
            data.need_charge = Excess(usage_.Get(), needed, capacity_.Get());
            EvictNeed(&data, &last_reference_list);
          }
          if (strict_capacity_limit_
                  ? FitsIn(usage_.Get(), needed, capacity_.Get())
                  : !AddOverflows(usage_.Get(), needed)) {
            GrowOneHomeCharged();
            e = AllocateSlot(
                HomeOf(length_info_.load(std::memory_order_relaxed), hash));
          }
        }
      }
      // Step 7: fill and publish the entry; detach an overwritten one.
      if (e != nullptr) {
        // One ghost probe: the index from above is still valid unless
        // eviction recorded a ghost since.
        bool to_resident = false;
        if (remembered) {
          if (ghost_records_ == ghost_records_before) {
            ghost_index_.EraseAt(ghost);
            to_resident = true;
          } else {
            to_resident = GhostTake(GhostHash(hash));
          }
        }
        SetKey(e, hash, heap_key, S3FifoHandle::kOccupied);
        e->set_value(value);
        e->helper_index = helper_index;
        e->SetSlotCharge(static_cast<uint32_t>(total_charge));
        e->next = e->prev = kNoLink;
        e->freq.store(0, std::memory_order_relaxed);
        // The final state, set before the chain publishes e to Lookup.
        // Release: GetPinnedUsage reads the charge after an acquire load
        // of state, without the chain.
        e->state.store(static_cast<uint16_t>(
                           S3FifoHandle::kInCache |
                           (to_resident ? 0 : S3FifoHandle::kInProbation) |
                           (handle != nullptr ? 1 : 0)),
                       std::memory_order_release);
        if (handle != nullptr && !to_resident) {
          // Where the lock-free Release that usually ends this pin credits
          // its unpin, so the two cancel in one place. Credited to the
          // shared counter instead, the pin would stay visible to other
          // threads after that Release until the next fold. A Release with
          // erase_if_last_ref (e.g. CacheReservationManager's dummy entries)
          // ends the pin under mutex_ instead and debits
          // probation_pinned_usage_, so other threads count total_charge more
          // unpinned probation charge than there is until a fold reads this
          // slot's credit: bounded staleness that affects only the queue
          // choice, as when ReleaseLocked or Detach debits a pin that a
          // lock-free Lookup credited to a slot.
          if (my_slot != nullptr) {
            my_slot->AddProbationPinned(total_charge);
          } else {
            probation_pinned_usage_ += total_charge;
          }
        }
        LinkIntoChain(e);
        occupancy_ += 1;
        if (old != nullptr) {
          s = Status::OkOverwritten();
          if (Detach(old)) {
            last_reference_list.push_back(old);
          }
        }
        usage_ += total_charge;
        FIFO_Append(to_resident ? resident() : probation(), e);
        if (handle != nullptr) {
          *handle = e;
        }
      } else {
        // Step 8a (no slot): an insert with a handle gets a standalone entry,
        // whether slotless or it found no free slot. Under
        // strict_capacity_limit it was admitted above only if it fits,
        // CreateStandalone's rule, and growth since then was charged only
        // where the entry still fits. Step 8b drops what gets no entry here
        // (no handle).
        if (handle != nullptr) {
          // No free slot, or an entry no slot can describe: hand out an entry
          // outside the table, as HyperClockCache does when its table is
          // full.
          e = NewStandalone(hash, heap_key, value, helper, total_charge);
          e->state.store(1, std::memory_order_relaxed);
          if (helper_overflow) {
            ++stats_.helper_overflow_standalone;
          }
          if (large_charge) {
            ++stats_.large_charge_standalone;
          }
          if (!slotless) {
            ++stats_.table_full_standalone;
          }
          assert(!strict_capacity_limit_ ||
                 FitsIn(usage_.Get(), total_charge, capacity_.Get()));
          usage_ += total_charge;
          detached_pinned_usage_ += total_charge;
          *handle = e;
        }
        // Admitted above but given no slot (standalone, or dropped below): the
        // old value must not stay visible behind an OK status, so it is
        // detached. An insert that fails the admission check above never gets
        // here: without a handle it returns OK with the old value still
        // cached, as LRUCache does; with one it is refused below and keeps the
        // old value cached, though eviction for it may still have removed an
        // unpinned old entry.
        if (old != nullptr) {
          s = Status::OkOverwritten();
          if (Detach(old)) {
            last_reference_list.push_back(old);
          }
        }
      }
    }
    // Step 8b (no entry: not admitted, or admitted without a handle and given
    // no slot): dropped without a handle (OK, as if inserted and evicted at
    // once), refused with one (MemoryLimit).
    if (e == nullptr) {
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(
            NewStandalone(hash, heap_key, value, helper, total_charge));
      } else {
        free(heap_key);
        *handle = nullptr;
        s = Status::MemoryLimit("Insert failed due to S3FifoCache being full.");
      }
    }
  }

  RunReaderWork(work);
  NotifyEvicted(last_reference_list);
  FreeHeapKeys(heap_keys);

  return s;
}

S3FifoHandle* S3FifoCacheShard::Lookup(const Slice& key, HashCref hash,
                                       const Cache::CacheItemHelper* /*helper*/,
                                       Cache::CreateContext* /*create_context*/,
                                       Cache::Priority /*priority*/,
                                       Statistics* /*stats*/) {
  bool temporary_reader = false;
  ThreadReader* reader = MyThreadReader(&temporary_reader);
  // Release: the reads of this thread's previous Lookup, on any shard, are
  // done before a reader scan can see it move here (see OldestAnnouncedEpoch).
  reader->shard.store(this, std::memory_order_release);
  // Acquire pairs with the release store in FreeBatches: a Lookup that
  // announces an advanced epoch sees every unlink of the slots stamped before
  // the advance, so it cannot reach them, and they need not wait for it.
  reader->epoch.store(epoch_.load(std::memory_order_acquire),
                      std::memory_order_relaxed);
  // Pairs with the fence in OldestAnnouncedEpoch: either that scan sees this
  // announcement, or this Lookup sees every unlink made before the scan. The
  // epoch store above can be relaxed because this fence orders it.
  std::atomic_thread_fence(std::memory_order_seq_cst);
  S3FifoHandle* e = FindInTable(key, hash);
  TEST_SYNC_POINT("S3FifoCacheShard::Lookup:Found");
  uint16_t s = 0;
  if (e != nullptr) {
    s = e->state.load(std::memory_order_relaxed);
    do {
      // At the 14-bit ref cap, one more ref would carry into kInCache: miss
      // instead, as for an entry being removed.
      if ((s & S3FifoHandle::kInCache) == 0 ||
          (s & S3FifoHandle::kRefMask) == S3FifoHandle::kRefMask) {
        e = nullptr;
        break;
      }
    } while (!e->state.compare_exchange_weak(s, static_cast<uint16_t>(s + 1),
                                             std::memory_order_acquire,
                                             std::memory_order_relaxed));
  }
  // The ref, if taken, keeps e alive from here on. Release: this Lookup's
  // reads of the slot happen before a scan sees epoch 0.
  reader->epoch.store(0, std::memory_order_release);
  if (UNLIKELY(temporary_reader)) {
    reader->owned.store(false, std::memory_order_release);
  }
  if (e == nullptr) {
    return nullptr;
  }
  TEST_SYNC_POINT_CALLBACK("S3FifoCacheShard::Lookup:AfterRef", e);
  if ((s & S3FifoHandle::kRefMask) == 0 &&
      (s & S3FifoHandle::kInProbation) != 0) {
    CreditProbationPin(e->SlotCharge());
  }
  // Credit the hit, saturating at kMaxFrequency.
  const uint8_t freq = e->freq.load(std::memory_order_relaxed);
  if (freq < kMaxFrequency) {
    e->freq.store(freq + 1, std::memory_order_relaxed);
  }
  return e;
}

bool S3FifoCacheShard::Ref(S3FifoHandle* e) {
  // To create another reference - entry must be already externally referenced.
  // Refused at the 14-bit ref cap, where one more would carry into kInCache.
  uint16_t s = e->state.load(std::memory_order_relaxed);
  do {
    assert((s & S3FifoHandle::kRefMask) > 0);
    if ((s & S3FifoHandle::kRefMask) == S3FifoHandle::kRefMask) {
      return false;
    }
  } while (!e->state.compare_exchange_weak(s, static_cast<uint16_t>(s + 1),
                                           std::memory_order_relaxed));
  return true;
}

bool S3FifoCacheShard::Release(S3FifoHandle* e, bool /*useful*/,
                               bool erase_if_last_ref) {
  if (e == nullptr) {
    return false;
  }
  // Once the ref is dropped, e may be freed.
  const size_t charge = e->TotalCharge();
  uint16_t s = e->state.load(std::memory_order_relaxed);
  do {
    assert((s & S3FifoHandle::kRefMask) > 0);
    // The last ref never erases for being over capacity (as in
    // HyperClockCache): the next insert evicts in policy order instead.
    if ((s & S3FifoHandle::kRefMask) == 1 &&
        (erase_if_last_ref || (s & S3FifoHandle::kInCache) == 0)) {
      return ReleaseLocked(e, erase_if_last_ref);
    }
  } while (!e->state.compare_exchange_weak(s, static_cast<uint16_t>(s - 1),
                                           std::memory_order_seq_cst,
                                           std::memory_order_relaxed));
  // The unref and this check are seq_cst, so a concurrent SetAside of e
  // cannot miss both the flag and the unref. Once unreferenced, the slot
  // may already hold another entry; that only costs a needless walk.
  if ((s & S3FifoHandle::kRefMask) == 1 &&
      e->aside.load(std::memory_order_seq_cst) != 0) {
    // Release pairs with the acquire exchange in EvictNeed: an eviction that
    // takes the flag sees this unref, so its walk finds e unpinned (unless
    // pinned again since).
    aside_unpinned_.store(true, std::memory_order_release);
  }
  TEST_SYNC_POINT("S3FifoCacheShard::Release:AfterUnref");
  if ((s & S3FifoHandle::kRefMask) == 1 &&
      (s & S3FifoHandle::kInProbation) != 0) {
    CreditProbationPin(size_t{0} - charge);
  }
  return false;
}

bool S3FifoCacheShard::ReleaseLocked(S3FifoHandle* e, bool erase_if_last_ref) {
  const size_t charge = e->TotalCharge();
  {
    DMutexLock l(mutex_);
    // Lookups may still add refs while e is in cache, so decide on each
    // snapshot. Erasing drops the last ref and both bits in one step.
    uint16_t s = e->state.load(std::memory_order_relaxed);
    uint16_t desired;
    do {
      const bool erase = (s & S3FifoHandle::kRefMask) == 1 &&
                         (s & S3FifoHandle::kInCache) != 0 && erase_if_last_ref;
      desired = erase ? uint16_t{0} : static_cast<uint16_t>(s - 1);
    } while (!e->state.compare_exchange_weak(
        s, desired, std::memory_order_acq_rel, std::memory_order_relaxed));
    if ((s & S3FifoHandle::kRefMask) != 1) {
      return false;
    }
    if ((s & S3FifoHandle::kInProbation) != 0) {
      probation_pinned_usage_ -= charge;
    }
    if ((s & S3FifoHandle::kInCache) != 0) {
      if (desired != 0) {
        // Not expected: Release comes here without erase_if_last_ref only for
        // an entry already out of the cache, and nothing puts an entry back in
        // it, so a last ref on an entry still in the cache is erased above.
        // Kept as a fallback: the entry stays where it is (in its queue or on
        // the aside list), now unpinned.
        if (e->aside.load(std::memory_order_relaxed) != 0) {
          // Relaxed: under mutex_, as is EvictNeed, which takes it.
          aside_unpinned_.store(true, std::memory_order_relaxed);
        }
        return false;
      }
      // The queues may still hold pinned entries, so unlike LRU there
      // is no emptiness assertion here.
      FIFO_Remove(e, (s & S3FifoHandle::kInProbation) != 0);
      RemoveFromTable(e);
    } else {
      // Detached (erased/overwritten) or standalone.
      assert(detached_pinned_usage_.Get() >= charge);
      detached_pinned_usage_ -= charge;
    }
    assert(usage_.Get() >= charge);
    usage_ -= charge;
  }

  // Free the entry here outside of mutex for performance reasons.
  e->FreeValue(*helpers_, allocator_);
  if (e->IsStandalone()) {
    FreeStandalone(e);
  } else {
    Retire(e, e);
  }
  return true;
}

bool S3FifoCacheShard::Detach(S3FifoHandle* e) {
  const uint16_t s =
      e->state.fetch_and(S3FifoHandle::kRefMask, std::memory_order_acq_rel);
  assert(s & S3FifoHandle::kInCache);
  const bool in_probation = (s & S3FifoHandle::kInProbation) != 0;
  FIFO_Remove(e, in_probation);
  RemoveFromTable(e);
  if ((s & S3FifoHandle::kRefMask) == 0) {
    assert(usage_.Get() >= e->SlotCharge());
    usage_ -= e->SlotCharge();
    return true;
  }
  // e stays charged until its last Release.
  detached_pinned_usage_ += e->SlotCharge();
  if (in_probation) {
    probation_pinned_usage_ -= e->SlotCharge();
  }
  return false;
}

Status S3FifoCacheShard::Insert(const Slice& key, HashCref hash,
                                Cache::ObjectPtr value,
                                const Cache::CacheItemHelper* helper,
                                size_t charge, S3FifoHandle** handle,
                                Cache::Priority /*priority*/) {
  assert(helper);
  // value == nullptr is reserved for indicating failure in SecondaryCache
  assert(!(helper->IsSecondaryCacheCompatible() && value == nullptr));
  // Allocate the key copy here outside of the mutex.
  S3FifoHandle::HeapKey* heap_key =
      key.size() == kInlineKeySize ? nullptr : NewHeapKey(key);
  if (key.size() != kInlineKeySize && heap_key == nullptr) {
    // No memory for the key copy: refuse the entry. The caller keeps
    // ownership of value, as for any failed insert.
    if (handle != nullptr) {
      *handle = nullptr;
    }
    return Status::MemoryLimit(
        "Insert failed: no memory for the key copy in S3FifoCache.");
  }
  return InsertItem(
      key, hash, heap_key, value, helper,
      SaturatingAdd(charge, HeapKeyCharge(heap_key, metadata_charge_policy_)),
      handle);
}

S3FifoHandle* S3FifoCacheShard::CreateStandalone(
    const Slice& key, HashCref hash, Cache::ObjectPtr value,
    const Cache::CacheItemHelper* helper, size_t charge, bool allow_uncharged) {
  assert(helper);
  S3FifoHandle::HeapKey* heap_key =
      key.size() == kInlineKeySize ? nullptr : NewHeapKey(key);
  if (key.size() != kInlineKeySize && heap_key == nullptr) {
    // No memory for the key copy, like a key HyperClockCache cannot hold.
    return nullptr;
  }
  S3FifoStandaloneHandle* e = NewStandalone(
      hash, heap_key, value, helper,
      SaturatingAdd(charge, HeapKeyCharge(heap_key, metadata_charge_policy_)));
  e->state.store(1, std::memory_order_relaxed);

  autovector<S3FifoHandle*> last_reference_list;

  // Allocates the caller's ReaderSlot chunk before mutex_, so that
  // ProbationUnpinnedUsage, called under it, does not allocate.
  MySlot();
  {
    DMutexLock l(mutex_);

    if (!(strict_capacity_limit_ && NeverFits(e->total_charge))) {
      EvictFromFIFO(e->total_charge, &last_reference_list);
    }

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
  }

  NotifyEvicted(last_reference_list);
  return e;
}

void S3FifoCacheShard::Erase(const Slice& key, HashCref hash) {
  S3FifoHandle* e = nullptr;
  bool last_reference = false;
  std::vector<S3FifoHandle::HeapKey*> heap_keys;
  ReaderWork work;
  {
    DMutexLock l(mutex_);
    work = MaybeReclaim(&heap_keys);
    e = FindInTable(key, hash);
    if (e != nullptr) {
      last_reference = Detach(e);
    }
  }

  RunReaderWork(work);
  // Free the entry here outside of mutex for performance reasons.
  // last_reference will only be true if e != nullptr.
  if (last_reference) {
    e->FreeValue(*helpers_, allocator_);
    Retire(e, e);
  }
  FreeHeapKeys(heap_keys);
}

// GetUsage, GetOccupancyCount and GetTableAddressCount take no lock (blob
// cache charging calls GetUsage on every blob insert and lookup). Without the
// lock a value can be momentarily stale, for example low while
// ShrinkTableIfEmpty returns the table's homes.
size_t S3FifoCacheShard::GetUsage() const { return usage_.Get(); }

size_t S3FifoCacheShard::GetPinnedUsage() const {
  // Announced like a Lookup: while announced, a slot whose state shows a
  // referenced entry keeps that entry's charge. (A slot retired before
  // the announced epoch may still be freed and reused during the scan; its
  // state then no longer shows that entry.)
  bool temporary_reader = false;
  ThreadReader* reader = MyThreadReader(&temporary_reader);
  // Release: the reads of this thread's previous Lookup, on any shard, are
  // done before a reader scan can see it move here (see OldestAnnouncedEpoch).
  reader->shard.store(this, std::memory_order_release);
  // Acquire pairs with the release store in FreeBatches, as in Lookup.
  reader->epoch.store(epoch_.load(std::memory_order_acquire),
                      std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  const size_t used_length =
      LengthInfoToUsedLength(length_info_.load(std::memory_order_acquire));
  size_t pinned = detached_pinned_usage_.Get();
  for (size_t i = 0; i < used_length; ++i) {
    const S3FifoHandle* h = Slot(SlotIndexAt(i));
    const uint16_t s = h->state.load(std::memory_order_acquire);
    if ((s & S3FifoHandle::kInCache) != 0 &&
        (s & S3FifoHandle::kRefMask) != 0) {
      pinned += h->SlotCharge();
    }
  }
  reader->epoch.store(0, std::memory_order_release);
  if (UNLIKELY(temporary_reader)) {
    reader->owned.store(false, std::memory_order_release);
  }
  return pinned;
}

size_t S3FifoCacheShard::GetOccupancyCount() const { return occupancy_.Get(); }

size_t S3FifoCacheShard::GetTableAddressCount() const {
  return LengthInfoToUsedLength(length_info_.load(std::memory_order_acquire));
}

bool S3FifoCacheShard::NeverFits(size_t total_charge) const {
  // Even an empty shard keeps a table of kInitialLength homes, charged under
  // kFullChargeCacheMetadata.
  const size_t min_table_charge =
      metadata_charge_policy_ == kFullChargeCacheMetadata
          ? TableChargeFor(std::min(kInitialLength, max_length_))
          : 0;
  return !FitsIn(min_table_charge, total_charge, capacity_.Get());
}

size_t S3FifoCacheShard::GetTableMetaCharge() const {
  if (metadata_charge_policy_ != kFullChargeCacheMetadata) {
    return 0;
  }
  return TableChargeFor(used_length_);
}

size_t S3FifoCacheShard::TableGrowthChargeOnInsert(const Slice& key,
                                                   HashCref hash) const {
  // An overwrite replaces an existing entry and never grows the table; the
  // lookup only runs at the growth boundary.
  if (metadata_charge_policy_ != kFullChargeCacheMetadata ||
      !WouldGrowOnNewInsert() || FindInTable(key, hash) != nullptr) {
    return 0;
  }
  return HomeCharge();
}

size_t S3FifoCacheShard::HomeCharge() const {
  // A growth step adds one home slot.
  return metadata_charge_policy_ == kFullChargeCacheMetadata
             ? TableChargeFor(used_length_ + 1) - TableChargeFor(used_length_)
             : 0;
}

size_t S3FifoCacheShard::TEST_GetTableOccupancyCount() const {
  DMutexLock l(mutex_);
  return occupancy_.Get();
}

void S3FifoCacheShard::AppendPrintableOptions(std::string& str) const {
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "    reserved_homes_per_shard: %zu\n",
           max_length_);
  str.append(buffer);
}

S3FifoCache::S3FifoCache(const S3FifoCacheOptions& opts)
    : ShardedCache(opts), helpers_(std::make_shared<S3FifoHelperRegistry>()) {
  size_t per_shard = GetPerShardCapacity();
  const size_t num_shards = GetNumShards();
  const size_t per_shard_max = opts.max_capacity / num_shards +
                               (opts.max_capacity % num_shards != 0 ? 1 : 0);
  MemoryAllocator* alloc = memory_allocator();
  const int max_upper_hash_bits = 32 - opts.num_shard_bits;
  InitShards([&](S3FifoCacheShard* cs) {
    new (cs) S3FifoCacheShard(
        per_shard, opts.strict_capacity_limit, opts.use_adaptive_mutex,
        opts.metadata_charge_policy, max_upper_hash_bits, alloc,
        &eviction_callback_, per_shard_max, helpers_);
    cs->SetHashSeed(GetHashSeed());
  });
}

Cache::ObjectPtr S3FifoCache::Value(Handle* handle) {
  auto* h = static_cast<const S3FifoHandle*>(handle);
  return h->value();
}

size_t S3FifoCache::GetCharge(Handle* handle) const {
  // All shards share the cache's metadata charge policy.
  return static_cast<const S3FifoHandle*>(handle)->GetCharge(
      GetShard(kNullUniqueId64x2).metadata_charge_policy_);
}

const Cache::CacheItemHelper* S3FifoCache::GetCacheItemHelper(
    Handle* handle) const {
  auto* h = static_cast<const S3FifoHandle*>(handle);
  return h->GetHelper(*helpers_);
}

void S3FifoCache::ApplyToHandle(
    Cache* cache, Handle* handle,
    const std::function<void(const Slice& key, ObjectPtr value, size_t charge,
                             const CacheItemHelper* helper)>& callback) {
  auto* cache_ptr = static_cast_with_check<S3FifoCache>(cache);
  auto* h = static_cast<const S3FifoHandle*>(handle);
  // All shards share the cache's metadata charge policy (and the hash seed
  // KeyOf uses).
  const S3FifoCacheShard& shard = cache_ptr->GetShard(kNullUniqueId64x2);
  UniqueId64x2 buf;
  callback(shard.KeyOf(h, &buf), h->value(),
           h->GetCharge(shard.metadata_charge_policy_),
           h->GetHelper(*cache_ptr->helpers_));
}

S3FifoCacheShard::Stats S3FifoCache::GetStats() {
  S3FifoCacheShard::Stats total;
  ForEachShard([&](S3FifoCacheShard* cs) {
    const S3FifoCacheShard::Stats s = cs->GetStats();
    total.occupancy_evictions += s.occupancy_evictions;
    total.pinned_rotations += s.pinned_rotations;
    total.set_aside += s.set_aside;
    total.evicted_without_hit += s.evicted_without_hit;
    total.table_full_standalone += s.table_full_standalone;
    total.helper_overflow_standalone += s.helper_overflow_standalone;
    total.large_charge_standalone += s.large_charge_standalone;
    total.aside_entries += s.aside_entries;
    total.limbo_slots += s.limbo_slots;
  });
  return total;
}

int S3FifoCache::DefaultShardBits(size_t capacity) {
  const uint64_t cap = capacity;
  // Up to 6 bits while every shard keeps at least kMinShardSize...
  int bits = 0;
  while (bits < 6 && cap >= (uint64_t{kMinShardSize} << (bits + 1))) {
    ++bits;
  }
  // ...and more while a shard would hold more than kMaxShardSize, up to the
  // most MakeSharedCache accepts.
  while (bits < 19 && cap > (uint64_t{kMaxShardSize} << bits)) {
    ++bits;
  }
  return bits;
}

bool S3FifoCache::ReservationFailed() {
  return SumOverShards([](S3FifoCacheShard& cs) {
           return static_cast<size_t>(cs.ReservationFailed());
         }) != 0;
}

size_t S3FifoCache::TEST_GetProbationSize() {
  return SumOverShards(
      [](S3FifoCacheShard& cs) { return cs.TEST_GetProbationSize(); });
}

}  // namespace s3fifo_cache

std::shared_ptr<Cache> S3FifoCacheOptions::MakeSharedCache() const {
  if (num_shard_bits >= 20) {
    return nullptr;  // The cache cannot be sharded into too many fine pieces.
  }
  // For sanitized options
  S3FifoCacheOptions opts = *this;
  if (opts.num_shard_bits < 0) {
    opts.num_shard_bits = S3FifoCache::DefaultShardBits(capacity);
  }
  std::shared_ptr<S3FifoCache> fifo = std::make_shared<S3FifoCache>(opts);
  if (fifo->ReservationFailed()) {
    return nullptr;  // A shard could not reserve its slot table.
  }
  std::shared_ptr<Cache> cache = std::move(fifo);
  if (secondary_cache) {
    cache = std::make_shared<CacheWithSecondaryAdapter>(cache, secondary_cache);
  }
  return cache;
}

}  // namespace ROCKSDB_NAMESPACE
