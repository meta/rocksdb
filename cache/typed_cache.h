//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// APIs for accessing Cache in a type-safe and convenient way. Cache is kept
// at a low, thin level of abstraction so that different implementations can
// be plugged in, but these wrappers provide clean, convenient access to the
// most common operations.
//
// A number of template classes are needed for sharing common structure. The
// key classes are these:
//
// * PlaceholderCacheInterface - Used for making cache reservations, with
// entries that have a charge but no value.
// * BasicTypedCacheInterface<TValue> - Used for primary cache storage of
// objects of type TValue.
// * FullTypedCacheHelper<TValue, TCreateContext> - Used for secondary cache
// compatible storage of objects of type TValue.
// * For each of these, there's a "Shared" version
// (e.g. FullTypedSharedCacheInterface) that holds a shared_ptr to the Cache,
// rather than assuming external ownership by holding only a raw `Cache*`.

#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <type_traits>

#include "cache/cache_helpers.h"
#include "rocksdb/advanced_cache.h"
#include "rocksdb/advanced_options.h"

namespace ROCKSDB_NAMESPACE {

// For future consideration:
// * Pass in value to Insert with std::unique_ptr& to simplify ownership
//   transfer logic in callers
// * Make key type a template parameter (e.g. useful for table cache)
// * Closer integration with CacheHandleGuard (opt-in, so not always
//   paying the extra overhead)

#define CACHE_TYPE_DEFS()                     \
  using Priority = Cache::Priority;           \
  using Handle = Cache::Handle;               \
  using ObjectPtr = Cache::ObjectPtr;         \
  using CreateContext = Cache::CreateContext; \
  using CacheItemHelper = Cache::CacheItemHelper /* caller ; */

template <typename CachePtr>
class BaseCacheInterface {
 public:
  CACHE_TYPE_DEFS();

  // NOLINTNEXTLINE(google-explicit-constructor)
  /*implicit*/ BaseCacheInterface(CachePtr cache) : cache_(std::move(cache)) {}

  inline void Release(Handle* handle) { cache_->Release(handle); }

  inline void ReleaseAndEraseIfLastRef(Handle* handle) {
    cache_->Release(handle, /*erase_if_last_ref*/ true);
  }

  inline void RegisterReleaseAsCleanup(Handle* handle, Cleanable& cleanable) {
    cleanable.RegisterCleanup(&ReleaseCacheHandleCleanup, get(), handle);
  }

  inline Cache* get() const { return &*cache_; }

  explicit inline operator bool() const noexcept { return cache_ != nullptr; }

 protected:
  CachePtr cache_;
};

// PlaceholderCacheInterface - Used for making cache reservations, with
// entries that have a charge but no value. CacheEntryRole is required as
// a template parameter.
template <CacheEntryRole kRole, typename CachePtr = Cache*>
class PlaceholderCacheInterface : public BaseCacheInterface<CachePtr> {
 public:
  CACHE_TYPE_DEFS();
  using BaseCacheInterface<CachePtr>::BaseCacheInterface;

  inline Status Insert(const Slice& key, size_t charge, Handle** handle) {
    return this->cache_->Insert(key, /*value=*/nullptr, GetHelper(), charge,
                                handle);
  }

  static const Cache::CacheItemHelper* GetHelper() {
    static const Cache::CacheItemHelper kHelper{kRole};
    return &kHelper;
  }
};

template <CacheEntryRole kRole>
using PlaceholderSharedCacheInterface =
    PlaceholderCacheInterface<kRole, std::shared_ptr<Cache>>;

template <class TValue>
class BasicTypedCacheHelperFns {
 public:
  CACHE_TYPE_DEFS();
  // E.g. char* for char[]
  using TValuePtr = std::remove_extent_t<TValue>*;

 protected:
  inline static ObjectPtr UpCastValue(TValuePtr value) { return value; }
  inline static TValuePtr DownCastValue(ObjectPtr value) {
    return static_cast<TValuePtr>(value);
  }

  static void Delete(ObjectPtr value, MemoryAllocator* allocator) {
    // FIXME: Currently, no callers actually allocate the ObjectPtr objects
    // using the custom allocator, just subobjects that keep a reference to
    // the allocator themselves (with CacheAllocationPtr).
    if (/*DISABLED*/ false && allocator) {
      if constexpr (std::is_destructible_v<TValue>) {
        DownCastValue(value)->~TValue();
      }
      allocator->Deallocate(value);
    } else {
      // Like delete but properly handles TValue=char[] etc.
      std::default_delete<TValue>{}(DownCastValue(value));
    }
  }
};

// In its own class to try to minimize the number of distinct CacheItemHelper
// instances (e.g. don't vary by CachePtr)
template <class TValue, CacheEntryRole kRole>
class BasicTypedCacheHelper : public BasicTypedCacheHelperFns<TValue> {
 public:
  static const Cache::CacheItemHelper* GetBasicHelper() {
    static const Cache::CacheItemHelper kHelper{kRole,
                                                &BasicTypedCacheHelper::Delete};
    return &kHelper;
  }
};

// BasicTypedCacheInterface - Used for primary cache storage of objects of
// type TValue, which can be cleaned up with std::default_delete<TValue>. The
// role is provided by TValue::kCacheEntryRole or given in an optional
// template parameter.
template <class TValue, CacheEntryRole kRole = TValue::kCacheEntryRole,
          typename CachePtr = Cache*>
class BasicTypedCacheInterface : public BaseCacheInterface<CachePtr>,
                                 public BasicTypedCacheHelper<TValue, kRole> {
 public:
  CACHE_TYPE_DEFS();
  using typename BasicTypedCacheHelperFns<TValue>::TValuePtr;
  struct TypedHandle : public Handle {};
  using BasicTypedCacheHelper<TValue, kRole>::GetBasicHelper;
  // ctor
  using BaseCacheInterface<CachePtr>::BaseCacheInterface;
  struct TypedAsyncLookupHandle : public Cache::AsyncLookupHandle {
    TypedHandle* Result() {
      return static_cast<TypedHandle*>(Cache::AsyncLookupHandle::Result());
    }
  };

  inline Status Insert(const Slice& key, TValuePtr value, size_t charge,
                       TypedHandle** handle = nullptr,
                       Priority priority = Priority::LOW) {
    auto untyped_handle = reinterpret_cast<Handle**>(handle);
    return this->cache_->Insert(
        key, BasicTypedCacheHelperFns<TValue>::UpCastValue(value),
        GetBasicHelper(), charge, untyped_handle, priority);
  }

  inline TypedHandle* Lookup(const Slice& key, Statistics* stats = nullptr) {
    return static_cast<TypedHandle*>(this->cache_->BasicLookup(key, stats));
  }

  inline void StartAsyncLookup(TypedAsyncLookupHandle& async_handle) {
    assert(async_handle.helper == nullptr);
    this->cache_->StartAsyncLookup(async_handle);
  }

  inline CacheHandleGuard<TValue> Guard(TypedHandle* handle) {
    if (handle) {
      return CacheHandleGuard<TValue>(&*this->cache_, handle);
    } else {
      return {};
    }
  }

  inline std::shared_ptr<TValue> SharedGuard(TypedHandle* handle) {
    if (handle) {
      return MakeSharedCacheHandleGuard<TValue>(&*this->cache_, handle);
    } else {
      return {};
    }
  }

  inline TValuePtr Value(TypedHandle* handle) {
    return BasicTypedCacheHelperFns<TValue>::DownCastValue(
        this->cache_->Value(handle));
  }
};

// BasicTypedSharedCacheInterface - Like BasicTypedCacheInterface but with a
// shared_ptr<Cache> for keeping Cache alive.
template <class TValue, CacheEntryRole kRole = TValue::kCacheEntryRole>
using BasicTypedSharedCacheInterface =
    BasicTypedCacheInterface<TValue, kRole, std::shared_ptr<Cache>>;

// TValue must implement ContentSlice() and ~TValue
// TCreateContext must implement Create(std::unique_ptr<TValue>*, ...)
template <class TValue, class TCreateContext>
class FullTypedCacheHelperFns : public BasicTypedCacheHelperFns<TValue> {
 public:
  CACHE_TYPE_DEFS();

 protected:
  using typename BasicTypedCacheHelperFns<TValue>::TValuePtr;
  using BasicTypedCacheHelperFns<TValue>::DownCastValue;
  using BasicTypedCacheHelperFns<TValue>::UpCastValue;

  static size_t Size(ObjectPtr v) {
    TValuePtr value = DownCastValue(v);
    auto slice = value->ContentSlice();
    return slice.size();
  }

  static Status SaveTo(ObjectPtr v, size_t from_offset, size_t length,
                       char* out) {
    TValuePtr value = DownCastValue(v);
    auto slice = value->ContentSlice();
    assert(from_offset < slice.size());
    assert(from_offset + length <= slice.size());
    std::copy_n(slice.data() + from_offset, length, out);
    return Status::OK();
  }

  static Status Create(const Slice& data, CompressionType type,
                       CacheTier source, CreateContext* context,
                       MemoryAllocator* allocator, ObjectPtr* out_obj,
                       size_t* out_charge) {
    std::unique_ptr<TValue> value = nullptr;
    if (source != CacheTier::kVolatileTier) {
      return Status::InvalidArgument();
    }
    if constexpr (!std::is_empty_v<TCreateContext>) {
      TCreateContext* tcontext = static_cast<TCreateContext*>(context);
      tcontext->Create(&value, out_charge, data, type, allocator);
    } else {
      TCreateContext::Create(&value, out_charge, data, type, allocator);
    }
    *out_obj = UpCastValue(value.release());
    return Status::OK();
  }
};

// In its own class to try to minimize the number of distinct CacheItemHelper
// instances (e.g. don't vary by CachePtr)
template <class TValue, class TCreateContext, CacheEntryRole kRole>
class FullTypedCacheHelper
    : public FullTypedCacheHelperFns<TValue, TCreateContext> {
 public:
  static const Cache::CacheItemHelper* GetFullHelper() {
    static const Cache::CacheItemHelper kHelper{
        kRole,
        &FullTypedCacheHelper::Delete,
        &FullTypedCacheHelper::Size,
        &FullTypedCacheHelper::SaveTo,
        &FullTypedCacheHelper::Create,
        BasicTypedCacheHelper<TValue, kRole>::GetBasicHelper()};
    return &kHelper;
  }
};

// FullTypedCacheHelper - Used for secondary cache compatible storage of
// objects of type TValue. In addition to BasicTypedCacheInterface constraints,
// we require TValue::ContentSlice() to return persistable data. This
// simplifies usage for the normal case of simple secondary cache compatibility
// (can give you a Slice to the data already in memory). In addition to
// TCreateContext performing the role of Cache::CreateContext, it is also
// expected to provide a function Create(std::unique_ptr<TValue>* value,
// size_t* out_charge, const Slice& data, MemoryAllocator* allocator) for
// creating new TValue.
template <class TValue, class TCreateContext,
          CacheEntryRole kRole = TValue::kCacheEntryRole,
          typename CachePtr = Cache*>
class FullTypedCacheInterface
    : public BasicTypedCacheInterface<TValue, kRole, CachePtr>,
      public FullTypedCacheHelper<TValue, TCreateContext, kRole> {
 public:
  CACHE_TYPE_DEFS();
  using typename BasicTypedCacheInterface<TValue, kRole, CachePtr>::TypedHandle;
  using typename BasicTypedCacheInterface<TValue, kRole,
                                          CachePtr>::TypedAsyncLookupHandle;
  using typename BasicTypedCacheHelperFns<TValue>::TValuePtr;
  using BasicTypedCacheHelper<TValue, kRole>::GetBasicHelper;
  using FullTypedCacheHelper<TValue, TCreateContext, kRole>::GetFullHelper;
  using BasicTypedCacheHelperFns<TValue>::UpCastValue;
  using BasicTypedCacheHelperFns<TValue>::DownCastValue;
  // ctor
  using BasicTypedCacheInterface<TValue, kRole,
                                 CachePtr>::BasicTypedCacheInterface;

  // Insert with SecondaryCache compatibility (subject to CacheTier).
  // (Basic Insert() also inherited.)
  inline Status InsertFull(
      const Slice& key, TValuePtr value, size_t charge,
      TypedHandle** handle = nullptr, Priority priority = Priority::LOW,
      CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier,
      const Slice& compressed = Slice(),
      CompressionType type = CompressionType::kNoCompression) {
    auto untyped_handle = reinterpret_cast<Handle**>(handle);
    auto helper = lowest_used_cache_tier > CacheTier::kVolatileTier
                      ? GetFullHelper()
                      : GetBasicHelper();
    return this->cache_->Insert(key, UpCastValue(value), helper, charge,
                                untyped_handle, priority, compressed, type);
  }

  // Like SecondaryCache::InsertSaved, with SecondaryCache compatibility
  // (subject to CacheTier).
  inline Status InsertSaved(
      const Slice& key, const Slice& data, TCreateContext* create_context,
      Priority priority = Priority::LOW,
      CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier,
      size_t* out_charge = nullptr) {
    ObjectPtr value;
    size_t charge;
    Status st = GetFullHelper()->create_cb(
        data, kNoCompression, CacheTier::kVolatileTier, create_context,
        this->cache_->memory_allocator(), &value, &charge);
    if (out_charge) {
      *out_charge = charge;
    }
    if (st.ok()) {
      st = InsertFull(key, DownCastValue(value), charge, nullptr /*handle*/,
                      priority, lowest_used_cache_tier);
    } else {
      GetFullHelper()->del_cb(value, this->cache_->memory_allocator());
    }
    return st;
  }

  // Lookup with SecondaryCache support (subject to CacheTier).
  // (Basic Lookup() also inherited.)
  inline TypedHandle* LookupFull(
      const Slice& key, TCreateContext* create_context = nullptr,
      Priority priority = Priority::LOW, Statistics* stats = nullptr,
      CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier) {
    if (lowest_used_cache_tier > CacheTier::kVolatileTier) {
      return static_cast<TypedHandle*>(this->cache_->Lookup(
          key, GetFullHelper(), create_context, priority, stats));
    } else {
      return BasicTypedCacheInterface<TValue, kRole, CachePtr>::Lookup(key,
                                                                       stats);
    }
  }

  inline void StartAsyncLookupFull(
      TypedAsyncLookupHandle& async_handle,
      CacheTier lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier) {
    if (lowest_used_cache_tier > CacheTier::kVolatileTier) {
      async_handle.helper = GetFullHelper();
      this->cache_->StartAsyncLookup(async_handle);
    } else {
      BasicTypedCacheInterface<TValue, kRole, CachePtr>::StartAsyncLookup(
          async_handle);
    }
  }
};

// FullTypedSharedCacheInterface - Like FullTypedCacheInterface but with a
// shared_ptr<Cache> for keeping Cache alive.
template <class TValue, class TCreateContext,
          CacheEntryRole kRole = TValue::kCacheEntryRole>
using FullTypedSharedCacheInterface =
    FullTypedCacheInterface<TValue, TCreateContext, kRole,
                            std::shared_ptr<Cache>>;

#undef CACHE_TYPE_DEFS

}  // namespace ROCKSDB_NAMESPACE
