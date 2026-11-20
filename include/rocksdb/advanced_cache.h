//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// APIs for customizing read caches in RocksDB.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "rocksdb/cache.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/memory_allocator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Logger;
class SecondaryCacheResultHandle;
class Statistics;

// A Cache maps keys to objects resident in memory, tracks reference counts
// on those key-object entries, and is able to remove unreferenced entries
// whenever it wants. All operations are fully thread safe except as noted.
// Inserted entries have a specified "charge" which is some quantity in
// unspecified units, typically bytes of memory used. A Cache will typically
// have a finite capacity in units of charge, and evict entries as needed
// to stay at or below that capacity.
//
// NOTE: This API is for expert use only and is intended more for customizing
// cache behavior than for calling into outside of RocksDB. It is subject to
// change as RocksDB evolves, especially the RocksDB block cache. Overriding
// CacheWrapper is the preferred way of customizing some operations on an
// existing implementation.
//
// INTERNAL: See typed_cache.h for convenient wrappers on top of this API.
// New virtual functions must also be added to CacheWrapper below.
class Cache : public Customizable {
 public:  // types hidden from API client
  // Opaque handle to an entry stored in the cache.
  struct Handle {};

 public:  // types hidden from Cache implementation
  // Pointer to cached object of unspecified type. (This type alias is
  // provided for clarity, not really for type checking.)
  using ObjectPtr = void*;

  // Opaque object providing context (settings, etc.) to create objects
  // for primary cache from saved (serialized) secondary cache entries.
  struct CreateContext {};

 public:  // type defs
  // Depending on implementation, cache entries with higher priority levels
  // could be less likely to get evicted than entries with lower priority
  // levels. The "high" priority level applies to certain SST metablocks (e.g.
  // index and filter blocks) if the option
  // cache_index_and_filter_blocks_with_high_priority is set. The "low" priority
  // level is used for other kinds of SST blocks (most importantly, data
  // blocks), as well as the above metablocks in case
  // cache_index_and_filter_blocks_with_high_priority is
  // not set. The "bottom" priority level is for BlobDB's blob values.
  enum class Priority { HIGH, LOW, BOTTOM };

  // A set of callbacks to allow objects in the primary block cache to be
  // persisted in a secondary cache. The purpose of the secondary cache
  // is to support other ways of caching the object, such as persistent or
  // compressed data, that may require the object to be parsed and transformed
  // in some way. Since the primary cache holds C++ objects and the secondary
  // cache may only hold flat data that doesn't need relocation, these
  // callbacks need to be provided by the user of the block
  // cache to do the conversion.
  // The CacheItemHelper is passed to Insert() and Lookup(). It has pointers
  // to callback functions for size, saving and deletion of the
  // object. The callbacks are defined in C-style in order to make them
  // stateless and not add to the cache metadata size.
  // Saving multiple std::function objects will take up 32 bytes per
  // function, even if its not bound to an object and does no capture.
  //
  // All the callbacks are C-style function pointers in order to simplify
  // lifecycle management. Objects in the cache can outlive the parent DB,
  // so anything required for these operations should be contained in the
  // object itself.
  //
  // The SizeCallback takes a pointer to the object and returns the size
  // of the persistable data. It can be used by the secondary cache to allocate
  // memory if needed.
  //
  // RocksDB callbacks are NOT exception-safe. A callback completing with an
  // exception can lead to undefined behavior in RocksDB, including data loss,
  // unreported corruption, deadlocks, and more.
  using SizeCallback = size_t (*)(ObjectPtr obj);

  // The SaveToCallback takes an object pointer and saves the persistable
  // data into a buffer. The secondary cache may decide to not store it in a
  // contiguous buffer, in which case this callback will be called multiple
  // times with increasing offset
  using SaveToCallback = Status (*)(ObjectPtr from_obj, size_t from_offset,
                                    size_t length, char* out_buf);

  // A function pointer type for destruction of a cache object. This will
  // typically call the destructor for the appropriate type of the object.
  // The Cache is responsible for copying and reclaiming space for the key,
  // but objects are managed in part using this callback. Generally a DeleterFn
  // can be nullptr if the ObjectPtr does not need destruction (e.g. nullptr or
  // pointer into static data).
  using DeleterFn = void (*)(ObjectPtr obj, MemoryAllocator* allocator);

  // The CreateCallback is takes in a buffer from the secondary  cache and
  // constructs an object using it. The buffer could be compressed or
  // uncompressed, as indicated by the type argument. If compressed,
  // the callback is responsible for uncompressing it using information
  // from the context, such as compression dictionary.
  // The callback doesn't have ownership of the buffer and
  // should copy the contents into its own buffer. The CreateContext* is
  // provided by Lookup and may be used to follow DB- or CF-specific settings.
  // In case of some error, non-OK is returned and the caller should ignore
  // any result in out_obj. (The implementation must clean up after itself.)
  using CreateCallback = Status (*)(const Slice& data, CompressionType type,
                                    CacheTier source, CreateContext* context,
                                    MemoryAllocator* allocator,
                                    ObjectPtr* out_obj, size_t* out_charge);

  // A struct with pointers to helper functions for spilling items from the
  // cache into the secondary cache. May be extended in the future. An
  // instance of this struct is expected to outlive the cache.
  struct CacheItemHelper {
    // Function for deleting an object on its removal from the Cache.
    // nullptr is only for entries that require no destruction, such as
    // "placeholder" cache entries with nullptr object.
    DeleterFn del_cb;  // (<- Most performance critical)
    // Next three are used for persisting values as described above.
    // If any is nullptr, then all three should be nullptr and persisting the
    // entry to/from secondary cache is not supported.
    SizeCallback size_cb;
    SaveToCallback saveto_cb;
    CreateCallback create_cb;
    // Classification of the entry for monitoring purposes in block cache.
    CacheEntryRole role;
    // Another CacheItemHelper (or this one) without secondary cache support.
    // This is provided so that items promoted from secondary cache into
    // primary cache without removal from the secondary cache can be prevented
    // from attempting re-insertion into secondary cache (for efficiency).
    const CacheItemHelper* without_secondary_compat;

    CacheItemHelper() : CacheItemHelper(CacheEntryRole::kMisc) {}

    // For helpers without SecondaryCache support
    explicit CacheItemHelper(CacheEntryRole _role, DeleterFn _del_cb = nullptr)
        : del_cb(_del_cb),
          size_cb(nullptr),
          saveto_cb(nullptr),
          create_cb(nullptr),
          role(_role),
          without_secondary_compat(this) {}

    // For helpers with SecondaryCache support
    explicit CacheItemHelper(CacheEntryRole _role, DeleterFn _del_cb,
                             SizeCallback _size_cb, SaveToCallback _saveto_cb,
                             CreateCallback _create_cb,
                             const CacheItemHelper* _without_secondary_compat)
        : del_cb(_del_cb),
          size_cb(_size_cb),
          saveto_cb(_saveto_cb),
          create_cb(_create_cb),
          role(_role),
          without_secondary_compat(_without_secondary_compat) {
      // Either all three secondary cache callbacks are non-nullptr or
      // all three are nullptr
      assert((size_cb != nullptr) == (saveto_cb != nullptr));
      assert((size_cb != nullptr) == (create_cb != nullptr));
      // without_secondary_compat points to equivalent but without
      // secondary support
      assert(role == without_secondary_compat->role);
      assert(del_cb == without_secondary_compat->del_cb);
      assert(!without_secondary_compat->IsSecondaryCacheCompatible());
    }
    inline bool IsSecondaryCacheCompatible() const {
      return size_cb != nullptr;
    }
  };

 public:  // ctor/dtor/create
  Cache(std::shared_ptr<MemoryAllocator> allocator = nullptr)
      : memory_allocator_(std::move(allocator)) {}
  // No copying allowed
  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;
  Cache(Cache&&) = delete;
  Cache& operator=(Cache&&) = delete;

  // Destroys all remaining entries by calling the associated "deleter"
  ~Cache() override {}

  static const char* Type() { return "Cache"; }

  // Creates a new Cache based on the input value string and returns the result.
  // Currently, this method can be used to create LRUCaches only
  // @param config_options
  // @param value  The value might be:
  //   - an old-style cache ("1M") -- equivalent to NewLRUCache(1024*102(
  //   - Name-value option pairs -- "capacity=1M; num_shard_bits=4;
  //     For the LRUCache, the values are defined in LRUCacheOptions.
  // @param result The new Cache object
  // @return OK if the cache was successfully created
  // @return NotFound if an invalid name was specified in the value
  // @return InvalidArgument if either the options were not valid
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& value,
                                 std::shared_ptr<Cache>* result);

 public:  // functions
  // The type of the Cache
  const char* Name() const override = 0;

  // The Insert and Lookup APIs below are intended to allow cached objects
  // to be demoted/promoted between the primary block cache and a secondary
  // cache. The secondary cache could be a non-volatile cache, and will
  // likely store the object in a different representation. They rely on a
  // per object CacheItemHelper to do the conversions.
  // The secondary cache may persist across process and system restarts,
  // and may even be moved between hosts. Therefore, the cache key must
  // be repeatable across restarts/reboots, and globally unique if
  // multiple DBs share the same cache and the set of DBs can change
  // over time.

  // Insert a mapping from key->object into the cache and assign it
  // the specified charge against the total cache capacity. If
  // strict_capacity_limit is true and cache reaches its full capacity,
  // return Status::MemoryLimit. `obj` must be non-nullptr if compatible
  // with secondary cache (helper->size_cb != nullptr), because Value() ==
  // nullptr is reserved for indicating some secondary cache failure cases.
  // On success, returns OK and takes ownership of `obj`, eventually deleting
  // it with helper->del_cb. On non-OK return, the caller maintains ownership
  // of `obj` so will often need to delete it in such cases.
  //
  // The helper argument is saved by the cache and will be used when the
  // inserted object is evicted or considered for promotion to the secondary
  // cache. Promotion to secondary cache is only enabled if helper->size_cb
  // != nullptr. The helper must outlive the cache. Callers may use
  // &kNoopCacheItemHelper as a trivial helper (no deleter for the object,
  // no secondary cache). `helper` must not be nullptr (efficiency).
  //
  // If `handle` is not nullptr and return status is OK, `handle` is set
  // to a Handle* for the entry. The caller must call this->Release(handle)
  // when the returned entry is no longer needed. If `handle` is nullptr, it is
  // as if Release is called immediately after Insert.
  //
  // Regardless of whether the item was inserted into the cache,
  // it will attempt to insert it into the secondary cache if one is
  // configured, and the helper supports it.
  // The cache implementation must support a secondary cache, otherwise
  // the item is only inserted into the primary cache. It may
  // defer the insertion to the secondary cache as it sees fit.
  //
  // Along with the object pointer, the caller may pass a Slice pointing to
  // the compressed serialized data of the object. If compressed is
  // non-empty, then the caller must pass the type indicating the compression
  // algorithm used. The cache may, optionally, also insert the compressed
  // block into one or more cache tiers.
  //
  // When the inserted entry is no longer needed, it will be destroyed using
  // helper->del_cb (if non-nullptr).
  virtual Status Insert(
      const Slice& key, ObjectPtr obj, const CacheItemHelper* helper,
      size_t charge, Handle** handle = nullptr,
      Priority priority = Priority::LOW, const Slice& compressed = Slice(),
      CompressionType type = CompressionType::kNoCompression) = 0;

  // Similar to Insert, but used for creating cache entries that cannot
  // be found with Lookup, such as for memory charging purposes. The
  // key is needed for cache sharding purposes.
  // * If allow_uncharged==true or strict_capacity_limit=false, the operation
  //   always succeeds and returns a valid Handle.
  // * If strict_capacity_limit=true and the requested charge cannot be freed
  //   up in the cache, then
  //   * If allow_uncharged==true, it's created anyway (GetCharge() == 0).
  //   * If allow_uncharged==false, returns nullptr to indicate failure.
  virtual Handle* CreateStandalone(const Slice& key, ObjectPtr obj,
                                   const CacheItemHelper* helper, size_t charge,
                                   bool allow_uncharged) = 0;

  // Lookup the key, returning nullptr if not found. If found, returns
  // a handle to the mapping that must eventually be passed to Release().
  //
  // If a non-nullptr helper argument is provided with a non-nullptr
  // create_cb, and a secondary cache is configured, then the secondary
  // cache is also queried if lookup in the primary cache fails. If found
  // in secondary cache, the provided create_db and create_context are
  // used to promote the entry to an object in the primary cache.
  // In that case, the helper may be saved and used later when the object
  // is evicted, so as usual, the pointed-to helper must outlive the cache.
  virtual Handle* Lookup(const Slice& key,
                         const CacheItemHelper* helper = nullptr,
                         CreateContext* create_context = nullptr,
                         Priority priority = Priority::LOW,
                         Statistics* stats = nullptr) = 0;

  // Convenience wrapper when secondary cache not supported
  inline Handle* BasicLookup(const Slice& key, Statistics* stats) {
    return Lookup(key, nullptr, nullptr, Priority::LOW, stats);
  }

  // Increments the reference count for the handle if it refers to an entry in
  // the cache. Returns true if refcount was incremented; otherwise, returns
  // false.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual bool Ref(Handle* handle) = 0;

  /**
   * Release a mapping returned by a previous Lookup(). A released entry might
   * still remain in cache in case it is later looked up by others. If
   * erase_if_last_ref is set then it also erases it from the cache if there is
   * no other reference to  it. Erasing it should call the deleter function that
   * was provided when the entry was inserted.
   *
   * Returns true if the entry was also erased.
   */
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual bool Release(Handle* handle, bool erase_if_last_ref = false) = 0;

  // Return the object associated with a handle returned by a successful
  // Lookup(). For historical reasons, this is also known at the "value"
  // associated with the key.
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual ObjectPtr Value(Handle* handle) = 0;

  // If the cache contains the entry for the key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const Slice& key) = 0;
  // Return a new numeric id.  May be used by multiple clients who are
  // sharding the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // sets the maximum configured capacity of the cache. When the new
  // capacity is less than the old capacity and the existing usage is
  // greater than new capacity, the implementation will do its best job to
  // purge the released entries from the cache in order to lower the usage
  virtual void SetCapacity(size_t capacity) = 0;

  // Set whether to return error on insertion when cache reaches its full
  // capacity.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;

  // Get the flag whether to return error on insertion when cache reaches its
  // full capacity.
  virtual bool HasStrictCapacityLimit() const = 0;

  // Returns the maximum configured capacity of the cache
  virtual size_t GetCapacity() const = 0;

  // Returns the memory size for the entries residing in the cache.
  virtual size_t GetUsage() const = 0;

  // Returns the number of entries currently tracked in the table. SIZE_MAX
  // means "not supported." This is used for inspecting the load factor, along
  // with GetTableAddressCount().
  virtual size_t GetOccupancyCount() const { return SIZE_MAX; }

  // Returns the number of ways the hash function is divided for addressing
  // entries. Zero means "not supported." This is used for inspecting the load
  // factor, along with GetOccupancyCount().
  virtual size_t GetTableAddressCount() const { return 0; }

  // Returns the memory size for a specific entry in the cache.
  virtual size_t GetUsage(Handle* handle) const = 0;

  // Returns the memory size for the entries in use by the system
  virtual size_t GetPinnedUsage() const = 0;

  // Returns the charge for the specific entry in the cache.
  virtual size_t GetCharge(Handle* handle) const = 0;

  // Returns the helper for the specified entry.
  virtual const CacheItemHelper* GetCacheItemHelper(Handle* handle) const = 0;

  virtual Status GetSecondaryCacheCapacity(size_t& /*size*/) const {
    return Status::NotSupported();
  }

  virtual Status GetSecondaryCachePinnedUsage(size_t& /*size*/) const {
    return Status::NotSupported();
  }

  // Call this on shutdown if you want to speed it up. Cache will disown
  // any underlying data and will not free it on delete. This call will leak
  // memory - call this only if you're shutting down the process.
  // Any attempts of using cache after this call will fail terribly.
  // Always delete the DB object before calling this method!
  virtual void DisownData() {
    // default implementation is noop
  }

  struct ApplyToAllEntriesOptions {
    // Roughly how many entries to process as a batch between steps of
    // iterating over the whole cache. Despite the name (kept for historical
    // reasons), this is fundamentally a batch size and applies regardless of
    // how a Cache implementation synchronizes: for a lock-based Cache a lock
    // is held for the duration of each batch, but the same batching also
    // benefits lock-free implementations by amortizing fixed per-batch
    // overhead. A higher value means fewer, larger batches, likely reducing
    // the total time for ApplyToAllEntries, but for a lock-based Cache it
    // increases latency for concurrent users because a lock is held longer
    // per batch. A smaller value could be helpful if the callback is
    // relatively expensive, such as using large data structures.
    size_t average_entries_per_lock = 256;
  };

  // Apply a callback to all entries in the cache. The Cache must ensure
  // thread safety but does not guarantee that a consistent snapshot of all
  // entries is iterated over if other threads are operating on the Cache
  // also.
  virtual void ApplyToAllEntries(
      const std::function<void(const Slice& key, ObjectPtr obj, size_t charge,
                               const CacheItemHelper* helper)>& callback,
      const ApplyToAllEntriesOptions& opts) = 0;

  // Apply a callback to a cache handle. The Cache must ensure the lifetime
  // of the key passed to the callback is valid for the duration of the
  // callback. The handle may not belong to the cache, but is guaranteed to
  // be type compatible.
  virtual void ApplyToHandle(
      Cache* cache, Handle* handle,
      const std::function<void(const Slice& key, ObjectPtr obj, size_t charge,
                               const CacheItemHelper* helper)>& callback) = 0;
  // Remove all entries.
  // Prerequisite: no entry is referenced.
  virtual void EraseUnRefEntries() = 0;

  std::string GetPrintableOptions() const override { return ""; }

  // Check for any warnings or errors in the operation of the cache and
  // report them to the logger. This is intended only to be called
  // periodically so does not need to be very efficient. (Obscure calling
  // conventions for Logger inherited from env.h)
  virtual void ReportProblems(
      const std::shared_ptr<Logger>& /*info_log*/) const {}

  MemoryAllocator* memory_allocator() const { return memory_allocator_.get(); }

  // See ShardedCacheOptions::hash_seed
  virtual uint32_t GetHashSeed() const { return 0; }

  // EXPERIMENTAL
  // The following APIs are experimental and might change in the future.

  // Release a mapping returned by a previous Lookup(). The "useful"
  // parameter specifies whether the data was actually used or not,
  // which may be used by the cache implementation to decide whether
  // to consider it as a hit for retention purposes. As noted elsewhere,
  // "pending" handles require Wait()/WaitAll() before Release().
  virtual bool Release(Handle* handle, bool /*useful*/,
                       bool erase_if_last_ref) {
    return Release(handle, erase_if_last_ref);
  }

  // A temporary handle structure for managing async lookups, which callers
  // of AsyncLookup() can allocate on the call stack for efficiency.
  // An AsyncLookupHandle should not be used concurrently across threads.
  struct AsyncLookupHandle {
    // Inputs, populated by caller:
    // NOTE: at least in case of stacked secondary caches, the underlying
    // key buffer must last until handle is completely waited on.
    Slice key;
    const CacheItemHelper* helper = nullptr;
    CreateContext* create_context = nullptr;
    Priority priority = Priority::LOW;
    Statistics* stats = nullptr;

    AsyncLookupHandle() {}
    AsyncLookupHandle(const Slice& _key, const CacheItemHelper* _helper,
                      CreateContext* _create_context,
                      Priority _priority = Priority::LOW,
                      Statistics* _stats = nullptr)
        : key(_key),
          helper(_helper),
          create_context(_create_context),
          priority(_priority),
          stats(_stats) {}

    // AsyncLookupHandle should only be destroyed when no longer pending
    ~AsyncLookupHandle() { assert(!IsPending()); }

    // No copies or moves (StartAsyncLookup may save a pointer to this)
    AsyncLookupHandle(const AsyncLookupHandle&) = delete;
    AsyncLookupHandle operator=(const AsyncLookupHandle&) = delete;
    AsyncLookupHandle(AsyncLookupHandle&&) = delete;
    AsyncLookupHandle operator=(AsyncLookupHandle&&) = delete;

    // Determines if the handle returned by Lookup() can give a value without
    // blocking, though Wait()/WaitAll() might be required to publish it to
    // Value(). See secondary cache compatible Lookup() above for details.
    // This call is not thread safe on "pending" handles.
    // WART/TODO with stacked secondaries: might indicate ready when one
    // result is ready (a miss) but the next lookup will block.
    bool IsReady();

    // Returns true if Wait/WaitAll is required before calling Result().
    bool IsPending();

    // Returns a Lookup()-like result if this AsyncHandle is not pending.
    // (Undefined behavior on a pending AsyncHandle.) Like Lookup(), the
    // caller is responsible for eventually Release()ing a non-nullptr
    // Handle* result.
    Handle* Result();

    // Implementation details, for RocksDB internal use only
    Handle* result_handle = nullptr;
    SecondaryCacheResultHandle* pending_handle = nullptr;
    SecondaryCache* pending_cache = nullptr;
    bool found_dummy_entry = false;
    bool kept_in_sec_cache = false;
  };

  // Starts a potentially asynchronous Lookup(), based on the populated
  // "input" fields of the async_handle. The caller is responsible for
  // keeping the AsyncLookupHandle and the key it references alive through
  // WaitAll(), and the AsyncLookupHandle alive through
  // AsyncLookupHandle::Result(). WaitAll() can only be skipped if
  // AsyncLookupHandle::IsPending() is already false after StartAsyncLookup.
  // Calling AsyncLookupHandle::Result() is essentially required so that
  // Release() can be called on non-nullptr Handle result. Wait() is a
  // concise version of WaitAll()+Result() on a single handle. After an
  // AsyncLookupHandle has completed this cycle, its input fields can be
  // updated and re-used for another StartAsyncLookup.
  //
  // Handle is thread-safe while AsyncLookupHandle is not thread-safe.
  //
  // Default implementation is appropriate for Caches without
  // true asynchronous support: defers to synchronous Lookup().
  // (AsyncLookupHandles will only get into the "pending" state with
  // SecondaryCache configured.)
  virtual void StartAsyncLookup(AsyncLookupHandle& async_handle);

  // A convenient wrapper around WaitAll() and AsyncLookupHandle::Result()
  // for a single async handle. See StartAsyncLookup().
  Handle* Wait(AsyncLookupHandle& async_handle);

  // Wait for an array of async handles to get results, so that none are left
  // in the "pending" state. Not thread safe. See StartAsyncLookup().
  // Default implementation is appropriate for Caches without true
  // asynchronous support: asserts that all handles are not pending (or not
  // expected to be handled by this cache, in case of wrapped/stacked
  // WaitAlls()).
  virtual void WaitAll(AsyncLookupHandle* /*async_handles*/, size_t /*count*/);

  // For a function called on cache entries about to be evicted. The function
  // returns `true` if it has taken ownership of the Value (object), or
  // `false` if the cache should destroy it as usual. Regardless, Ref() and
  // Release() cannot be called on this Handle that is poised for eviction.
  using EvictionCallback =
      std::function<bool(const Slice& key, Handle* h, bool was_hit)>;
  // Sets an eviction callback for this Cache. Not thread safe and only
  // supports being set once, so should only be used during initialization
  // or destruction, guaranteed before or after any thread-shared operations.
  void SetEvictionCallback(EvictionCallback&& fn);

 protected:
  std::shared_ptr<MemoryAllocator> memory_allocator_;
  EvictionCallback eviction_callback_;
};

// A wrapper around Cache that can easily be extended with instrumentation,
// etc.
class CacheWrapper : public Cache {
 public:
  explicit CacheWrapper(std::shared_ptr<Cache> target)
      : target_(std::move(target)) {}

  // Only function that derived class must provide
  // const char* Name() const override { ... }

  Status Insert(
      const Slice& key, ObjectPtr value, const CacheItemHelper* helper,
      size_t charge, Handle** handle = nullptr,
      Priority priority = Priority::LOW,
      const Slice& compressed_value = Slice(),
      CompressionType type = CompressionType::kNoCompression) override {
    return target_->Insert(key, value, helper, charge, handle, priority,
                           compressed_value, type);
  }

  Handle* CreateStandalone(const Slice& key, ObjectPtr obj,
                           const CacheItemHelper* helper, size_t charge,
                           bool allow_uncharged) override {
    return target_->CreateStandalone(key, obj, helper, charge, allow_uncharged);
  }

  Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                 CreateContext* create_context,
                 Priority priority = Priority::LOW,
                 Statistics* stats = nullptr) override {
    return target_->Lookup(key, helper, create_context, priority, stats);
  }

  bool Ref(Handle* handle) override { return target_->Ref(handle); }

  using Cache::Release;
  bool Release(Handle* handle, bool erase_if_last_ref = false) override {
    return target_->Release(handle, erase_if_last_ref);
  }

  ObjectPtr Value(Handle* handle) override { return target_->Value(handle); }

  void Erase(const Slice& key) override { target_->Erase(key); }
  uint64_t NewId() override { return target_->NewId(); }

  void SetCapacity(size_t capacity) override { target_->SetCapacity(capacity); }

  void SetStrictCapacityLimit(bool strict_capacity_limit) override {
    target_->SetStrictCapacityLimit(strict_capacity_limit);
  }

  bool HasStrictCapacityLimit() const override {
    return target_->HasStrictCapacityLimit();
  }

  size_t GetOccupancyCount() const override {
    return target_->GetOccupancyCount();
  }

  size_t GetTableAddressCount() const override {
    return target_->GetTableAddressCount();
  }

  size_t GetCapacity() const override { return target_->GetCapacity(); }

  size_t GetUsage() const override { return target_->GetUsage(); }

  size_t GetUsage(Handle* handle) const override {
    return target_->GetUsage(handle);
  }

  size_t GetPinnedUsage() const override { return target_->GetPinnedUsage(); }

  size_t GetCharge(Handle* handle) const override {
    return target_->GetCharge(handle);
  }

  const CacheItemHelper* GetCacheItemHelper(Handle* handle) const override {
    return target_->GetCacheItemHelper(handle);
  }

  void ApplyToAllEntries(
      const std::function<void(const Slice& key, ObjectPtr value, size_t charge,
                               const CacheItemHelper* helper)>& callback,
      const ApplyToAllEntriesOptions& opts) override {
    target_->ApplyToAllEntries(callback, opts);
  }

  void ApplyToHandle(
      Cache* cache, Handle* handle,
      const std::function<void(const Slice& key, ObjectPtr obj, size_t charge,
                               const CacheItemHelper* helper)>& callback)
      override {
    auto cache_ptr = static_cast<CacheWrapper*>(cache);
    target_->ApplyToHandle(cache_ptr->target_.get(), handle, callback);
  }

  void EraseUnRefEntries() override { target_->EraseUnRefEntries(); }

  void StartAsyncLookup(AsyncLookupHandle& async_handle) override {
    target_->StartAsyncLookup(async_handle);
  }

  void WaitAll(AsyncLookupHandle* async_handles, size_t count) override {
    target_->WaitAll(async_handles, count);
  }

  uint32_t GetHashSeed() const override { return target_->GetHashSeed(); }

  void ReportProblems(const std::shared_ptr<Logger>& info_log) const override {
    target_->ReportProblems(info_log);
  }

  const std::shared_ptr<Cache>& GetTarget() { return target_; }

 protected:
  std::shared_ptr<Cache> target_;
};

// Useful for cache entries requiring no clean-up, such as for cache
// reservations
extern const Cache::CacheItemHelper kNoopCacheItemHelper;

}  // namespace ROCKSDB_NAMESPACE
