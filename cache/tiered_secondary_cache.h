//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/cache.h"
#include "rocksdb/secondary_cache.h"

namespace ROCKSDB_NAMESPACE {

// A SecondaryCache that implements stacking of a compressed secondary cache
// and a non-volatile (local flash) cache. It implements an admission
// policy of warming the bottommost tier (local flash) with compressed
// blocks from the SST on misses, and on hits in the bottommost tier,
// promoting to the compressed and/or primary block cache. The admission
// policies of the primary block cache and compressed secondary cache remain
// unchanged - promote on second access. There is no demotion ofablocks
// evicted from a tier. They are just discarded.
//
// In order to properly handle compressed blocks directly read from SSTs, and
// to allow writeback of blocks compressed by the compressed secondary
// cache in the future, we make use of the compression type and source
// cache tier arguments in InsertSaved.
class TieredSecondaryCache : public SecondaryCacheWrapper {
 public:
  TieredSecondaryCache(std::shared_ptr<SecondaryCache> comp_sec_cache,
                       std::shared_ptr<SecondaryCache> nvm_sec_cache,
                       TieredAdmissionPolicy adm_policy)
      : SecondaryCacheWrapper(comp_sec_cache), nvm_sec_cache_(nvm_sec_cache) {
#ifndef NDEBUG
    assert(adm_policy == TieredAdmissionPolicy::kAdmPolicyThreeQueue);
#else
    (void)adm_policy;
#endif
  }

  ~TieredSecondaryCache() override = default;

  const char* Name() const override { return "TieredSecondaryCache"; }

  // This is a no-op as we currently don't allow demotion (i.e
  // insertion by the upper layer) of evicted blocks.
  Status Insert(const Slice& /*key*/, Cache::ObjectPtr /*obj*/,
                const Cache::CacheItemHelper* /*helper*/,
                bool /*force_insert*/) override {
    return Status::OK();
  }

  // Warm up the nvm tier directly
  Status InsertSaved(const Slice& key, const Slice& saved,
                     CompressionType type = CompressionType::kNoCompression,
                     CacheTier source = CacheTier::kVolatileTier) override {
    return nvm_sec_cache_->InsertSaved(key, saved, type, source);
  }

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool wait, bool advise_erase,
      Statistics* stats, bool& kept_in_sec_cache) override;

  void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) override;

 private:
  struct CreateContext : public Cache::CreateContext {
    const Slice* key;
    bool advise_erase;
    const Cache::CacheItemHelper* helper;
    Cache::CreateContext* inner_ctx;
    std::shared_ptr<SecondaryCacheResultHandle> inner_handle;
    SecondaryCache* comp_sec_cache;
    Statistics* stats;
  };

  class ResultHandle : public SecondaryCacheResultHandle {
   public:
    ~ResultHandle() override = default;

    bool IsReady() override {
      if (inner_handle_ && inner_handle_->IsReady()) {
        Complete();
      }
      return ready_;
    }

    void Wait() override {
      inner_handle_->Wait();
      Complete();
    }

    size_t Size() override { return size_; }

    Cache::ObjectPtr Value() override { return value_; }

    void Complete() {
      size_ = inner_handle_->Size();
      value_ = inner_handle_->Value();
      inner_handle_.reset();
      ready_ = true;
    }

    void SetInnerHandle(std::unique_ptr<SecondaryCacheResultHandle>&& handle) {
      inner_handle_ = std::move(handle);
    }

    void SetSize(size_t size) { size_ = size; }

    void SetValue(Cache::ObjectPtr val) { value_ = val; }

    CreateContext* ctx() { return &ctx_; }

    SecondaryCacheResultHandle* inner_handle() { return inner_handle_.get(); }

   private:
    std::unique_ptr<SecondaryCacheResultHandle> inner_handle_;
    CreateContext ctx_;
    size_t size_;
    Cache::ObjectPtr value_;
    bool ready_ = false;
  };

  static void NoopDelete(Cache::ObjectPtr /*obj*/,
                         MemoryAllocator* /*allocator*/) {
    assert(false);
  }
  static size_t ZeroSize(Cache::ObjectPtr /*obj*/) {
    assert(false);
    return 0;
  }
  static Status NoopSaveTo(Cache::ObjectPtr /*from_obj*/,
                           size_t /*from_offset*/, size_t /*length*/,
                           char* /*out_buf*/) {
    assert(false);
    return Status::OK();
  }
  static Status MaybeInsertAndCreate(const Slice& data, CompressionType type,
                                     CacheTier source,
                                     Cache::CreateContext* ctx,
                                     MemoryAllocator* allocator,
                                     Cache::ObjectPtr* out_obj,
                                     size_t* out_charge);

  static const Cache::CacheItemHelper* GetHelper() {
    const static Cache::CacheItemHelper basic_helper(CacheEntryRole::kMisc,
                                                     &NoopDelete);
    const static Cache::CacheItemHelper maybe_insert_and_create_helper{
        CacheEntryRole::kMisc, &NoopDelete,           &ZeroSize,
        &NoopSaveTo,           &MaybeInsertAndCreate, &basic_helper,
    };
    return &maybe_insert_and_create_helper;
  }

  std::shared_ptr<SecondaryCache> comp_sec_cache_;
  std::shared_ptr<SecondaryCache> nvm_sec_cache_;
};

}  // namespace ROCKSDB_NAMESPACE
