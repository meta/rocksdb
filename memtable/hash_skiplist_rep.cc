//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <atomic>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/skiplist.h"
#include "port/port.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/options_type.h"
#include "util/murmurhash.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class HashSkipListRep : public MemTableRep {
 public:
  HashSkipListRep(const MemTableRep::KeyComparator& compare,
                  Allocator* allocator, const SliceTransform* transform,
                  size_t bucket_size, int32_t skiplist_height,
                  int32_t skiplist_branching_factor);

  void Insert(KeyHandle handle) override;

  bool Contains(const char* key) const override;

  size_t ApproximateMemoryUsage() override;

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override;

  ~HashSkipListRep() override;

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

  MemTableRep::Iterator* GetDynamicPrefixIterator(
      Arena* arena = nullptr) override;

 private:
  friend class DynamicIterator;
  using Bucket = SkipList<const char*, const MemTableRep::KeyComparator&>;

  size_t bucket_size_;

  const int32_t skiplist_height_;
  const int32_t skiplist_branching_factor_;

  // Maps slices (which are transformed user keys) to buckets of keys sharing
  // the same transform.
  std::atomic<Bucket*>* buckets_;

  // The user-supplied transform whose domain is the user keys.
  const SliceTransform* transform_;

  const MemTableRep::KeyComparator& compare_;
  // immutable after construction
  Allocator* const allocator_;

  inline size_t GetHash(const Slice& slice) const {
    return MurmurHash(slice.data(), static_cast<int>(slice.size()), 0) %
           bucket_size_;
  }
  inline Bucket* GetBucket(size_t i) const {
    return buckets_[i].load(std::memory_order_acquire);
  }
  inline Bucket* GetBucket(const Slice& slice) const {
    return GetBucket(GetHash(slice));
  }
  // Get a bucket from buckets_. If the bucket hasn't been initialized yet,
  // initialize it before returning.
  Bucket* GetInitializedBucket(const Slice& transformed);

  class Iterator : public MemTableRep::Iterator {
   public:
    explicit Iterator(Bucket* list, bool own_list = true,
                      Arena* arena = nullptr)
        : list_(list), iter_(list), own_list_(own_list), arena_(arena) {}
    // No copying allowed
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
    Iterator(Iterator&&) = delete;
    Iterator& operator=(Iterator&&) = delete;

    ~Iterator() override {
      // if we own the list, we should also delete it
      if (own_list_) {
        assert(list_ != nullptr);
        delete list_;
      }
    }

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return list_ != nullptr && iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override {
      assert(Valid());
      return iter_.key();
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override {
      assert(Valid());
      iter_.Next();
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override {
      assert(Valid());
      iter_.Prev();
    }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& internal_key, const char* memtable_key) override {
      if (list_ != nullptr) {
        const char* encoded_key = (memtable_key != nullptr)
                                      ? memtable_key
                                      : EncodeKey(&tmp_, internal_key);
        iter_.Seek(encoded_key);
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& /*internal_key*/,
                     const char* /*memtable_key*/) override {
      // not supported
      assert(false);
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToFirst() override {
      if (list_ != nullptr) {
        iter_.SeekToFirst();
      }
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToLast() override {
      if (list_ != nullptr) {
        iter_.SeekToLast();
      }
    }

   protected:
    void Reset(Bucket* list) {
      if (own_list_) {
        assert(list_ != nullptr);
        delete list_;
      }
      list_ = list;
      iter_.SetList(list);
      own_list_ = false;
    }

   private:
    // if list_ is nullptr, we should NEVER call any methods on iter_
    // if list_ is nullptr, this Iterator is not Valid()
    Bucket* list_;
    Bucket::Iterator iter_;
    // here we track if we own list_. If we own it, we are also
    // responsible for it's cleaning. This is a poor man's std::shared_ptr
    bool own_list_;
    std::unique_ptr<Arena> arena_;
    std::string tmp_;  // For passing to EncodeKey
  };

  class DynamicIterator : public HashSkipListRep::Iterator {
   public:
    explicit DynamicIterator(const HashSkipListRep& memtable_rep)
        : HashSkipListRep::Iterator(nullptr, false),
          memtable_rep_(memtable_rep) {}

    // Advance to the first entry with a key >= target
    void Seek(const Slice& k, const char* memtable_key) override {
      auto transformed = memtable_rep_.transform_->Transform(ExtractUserKey(k));
      Reset(memtable_rep_.GetBucket(transformed));
      HashSkipListRep::Iterator::Seek(k, memtable_key);
    }

    // Position at the first entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToFirst() override {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

    // Position at the last entry in collection.
    // Final state of iterator is Valid() iff collection is not empty.
    void SeekToLast() override {
      // Prefix iterator does not support total order.
      // We simply set the iterator to invalid state
      Reset(nullptr);
    }

   private:
    // the underlying memtable
    const HashSkipListRep& memtable_rep_;
  };
};

HashSkipListRep::HashSkipListRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 size_t bucket_size, int32_t skiplist_height,
                                 int32_t skiplist_branching_factor)
    : MemTableRep(allocator),
      bucket_size_(bucket_size),
      skiplist_height_(skiplist_height),
      skiplist_branching_factor_(skiplist_branching_factor),
      transform_(transform),
      compare_(compare),
      allocator_(allocator) {
  auto mem =
      allocator->AllocateAligned(sizeof(std::atomic<void*>) * bucket_size);
  buckets_ = new (mem) std::atomic<Bucket*>[bucket_size];

  for (size_t i = 0; i < bucket_size_; ++i) {
    buckets_[i].store(nullptr, std::memory_order_relaxed);
  }
}

HashSkipListRep::~HashSkipListRep() = default;

HashSkipListRep::Bucket* HashSkipListRep::GetInitializedBucket(
    const Slice& transformed) {
  size_t hash = GetHash(transformed);
  auto bucket = GetBucket(hash);
  if (bucket == nullptr) {
    auto addr = allocator_->AllocateAligned(sizeof(Bucket));
    bucket = new (addr) Bucket(compare_, allocator_, skiplist_height_,
                               skiplist_branching_factor_);
    buckets_[hash].store(bucket, std::memory_order_release);
  }
  return bucket;
}

void HashSkipListRep::Insert(KeyHandle handle) {
  auto* key = static_cast<char*>(handle);
  assert(!Contains(key));
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetInitializedBucket(transformed);
  bucket->Insert(key);
}

bool HashSkipListRep::Contains(const char* key) const {
  auto transformed = transform_->Transform(UserKey(key));
  auto bucket = GetBucket(transformed);
  if (bucket == nullptr) {
    return false;
  }
  return bucket->Contains(key);
}

size_t HashSkipListRep::ApproximateMemoryUsage() { return 0; }

void HashSkipListRep::Get(const LookupKey& k, void* callback_args,
                          bool (*callback_func)(void* arg, const char* entry)) {
  auto transformed = transform_->Transform(k.user_key());
  auto bucket = GetBucket(transformed);
  if (bucket != nullptr) {
    Bucket::Iterator iter(bucket);
    for (iter.Seek(k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }
}

MemTableRep::Iterator* HashSkipListRep::GetIterator(Arena* arena) {
  // allocate a new arena of similar size to the one currently in use
  Arena* new_arena = new Arena(allocator_->BlockSize());
  auto list = new Bucket(compare_, new_arena);
  for (size_t i = 0; i < bucket_size_; ++i) {
    auto bucket = GetBucket(i);
    if (bucket != nullptr) {
      Bucket::Iterator itr(bucket);
      for (itr.SeekToFirst(); itr.Valid(); itr.Next()) {
        list->Insert(itr.key());
      }
    }
  }
  if (arena == nullptr) {
    return new Iterator(list, true, new_arena);
  } else {
    auto mem = arena->AllocateAligned(sizeof(Iterator));
    return new (mem) Iterator(list, true, new_arena);
  }
}

MemTableRep::Iterator* HashSkipListRep::GetDynamicPrefixIterator(Arena* arena) {
  if (arena == nullptr) {
    return new DynamicIterator(*this);
  } else {
    auto mem = arena->AllocateAligned(sizeof(DynamicIterator));
    return new (mem) DynamicIterator(*this);
  }
}

struct HashSkipListRepOptions {
  static const char* kName() { return "HashSkipListRepFactoryOptions"; }
  size_t bucket_count;
  int32_t skiplist_height;
  int32_t skiplist_branching_factor;
};

static std::unordered_map<std::string, OptionTypeInfo> hash_skiplist_info = {
    {"bucket_count",
     {offsetof(struct HashSkipListRepOptions, bucket_count), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"skiplist_height",
     {offsetof(struct HashSkipListRepOptions, skiplist_height),
      OptionType::kInt32T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"branching_factor",
     {offsetof(struct HashSkipListRepOptions, skiplist_branching_factor),
      OptionType::kInt32T, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
};

class HashSkipListRepFactory : public MemTableRepFactory {
 public:
  explicit HashSkipListRepFactory(size_t bucket_count, int32_t skiplist_height,
                                  int32_t skiplist_branching_factor) {
    options_.bucket_count = bucket_count;
    options_.skiplist_height = skiplist_height;
    options_.skiplist_branching_factor = skiplist_branching_factor;
    RegisterOptions(&options_, &hash_skiplist_info);
  }

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger) override;

  static const char* kClassName() { return "HashSkipListRepFactory"; }
  static const char* kNickName() { return "prefix_hash"; }

  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

 private:
  HashSkipListRepOptions options_;
};

}  // namespace

MemTableRep* HashSkipListRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* /*logger*/) {
  return new HashSkipListRep(compare, allocator, transform,
                             options_.bucket_count, options_.skiplist_height,
                             options_.skiplist_branching_factor);
}

MemTableRepFactory* NewHashSkipListRepFactory(
    size_t bucket_count, int32_t skiplist_height,
    int32_t skiplist_branching_factor) {
  return new HashSkipListRepFactory(bucket_count, skiplist_height,
                                    skiplist_branching_factor);
}

}  // namespace ROCKSDB_NAMESPACE
