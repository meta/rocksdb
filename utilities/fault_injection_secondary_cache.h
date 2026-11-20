//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/secondary_cache.h"
#include "util/random.h"
#include "util/thread_local.h"

namespace ROCKSDB_NAMESPACE {

// This class implements a custom SecondaryCache that randomly injects an
// error status into Inserts/Lookups based on a specified probability.
// Its used by db_stress to verify correctness in the presence of
// secondary cache errors.
//
class FaultInjectionSecondaryCache : public SecondaryCache {
 public:
  explicit FaultInjectionSecondaryCache(
      const std::shared_ptr<SecondaryCache>& base, uint32_t seed, int prob)
      : base_(base),
        seed_(seed),
        prob_(prob),
        thread_local_error_(new ThreadLocalPtr(DeleteThreadLocalErrorContext)) {
    if (std::strcmp(base_->Name(), "CompressedSecondaryCache") == 0) {
      base_is_compressed_sec_cache_ = true;
    }
  }

  ~FaultInjectionSecondaryCache() override = default;

  const char* Name() const override { return "FaultInjectionSecondaryCache"; }

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper,
                bool force_insert) override;

  Status InsertSaved(const Slice& /*key*/, const Slice& /*saved*/,
                     CompressionType /*type*/, CacheTier /*source*/) override {
    return Status::OK();
  }

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool wait, bool advise_erase,
      Statistics* stats, bool& kept_in_sec_cache) override;

  bool SupportForceErase() const override { return base_->SupportForceErase(); }

  void Erase(const Slice& key) override;

  void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) override;

  Status SetCapacity(size_t capacity) override {
    return base_->SetCapacity(capacity);
  }

  Status GetCapacity(size_t& capacity) override {
    return base_->GetCapacity(capacity);
  }

  std::string GetPrintableOptions() const override {
    return base_->GetPrintableOptions();
  }

 private:
  class ResultHandle : public SecondaryCacheResultHandle {
   public:
    ResultHandle(FaultInjectionSecondaryCache* cache,
                 std::unique_ptr<SecondaryCacheResultHandle>&& base)
        : cache_(cache), base_(std::move(base)), value_(nullptr), size_(0) {}

    ~ResultHandle() override = default;

    bool IsReady() override;

    void Wait() override;

    Cache::ObjectPtr Value() override;

    size_t Size() override;

    static void WaitAll(
        FaultInjectionSecondaryCache* cache,
        const std::vector<SecondaryCacheResultHandle*>& handles);

   private:
    static void UpdateHandleValue(ResultHandle* handle);

    FaultInjectionSecondaryCache* cache_;
    std::unique_ptr<SecondaryCacheResultHandle> base_;
    Cache::ObjectPtr value_;
    size_t size_;
  };

  static void DeleteThreadLocalErrorContext(void* p) {
    ErrorContext* ctx = static_cast<ErrorContext*>(p);
    delete ctx;
  }

  const std::shared_ptr<SecondaryCache> base_;
  uint32_t seed_;
  int prob_;
  bool base_is_compressed_sec_cache_{false};

  struct ErrorContext {
    Random rand;

    explicit ErrorContext(uint32_t seed) : rand(seed) {}
  };
  std::unique_ptr<ThreadLocalPtr> thread_local_error_;

  ErrorContext* GetErrorContext();
};

}  // namespace ROCKSDB_NAMESPACE
