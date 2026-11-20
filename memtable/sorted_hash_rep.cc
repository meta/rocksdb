//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <new>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/arena.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/options_type.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class SortedHashRep : public MemTableRep {
 private:
  struct Node {
    Node() : next(nullptr) {}

    Node* Next() const { return next.load(std::memory_order_acquire); }

    void SetNext(Node* node) { next.store(node, std::memory_order_release); }

    void SetNextBeforePublish(Node* node) {
      next.store(node, std::memory_order_relaxed);
    }

    std::atomic<Node*> next;
    char key[1]{};
  };

  struct Bucket {
    Bucket() : head(nullptr) {}

    SpinMutex mutex;
    std::atomic<Node*> head;
  };

 public:
  static constexpr size_t kBucketSize = sizeof(Bucket);

  SortedHashRep(const MemTableRep::KeyComparator& compare, Allocator* allocator,
                size_t bucket_count)
      : MemTableRep(allocator),
        compare_(compare),
        bucket_count_(std::max<size_t>(bucket_count, 1)) {
    char* const memory =
        allocator_->AllocateAligned(kBucketSize * bucket_count_);
    buckets_ = new (memory) Bucket();
    for (size_t i = 1; i < bucket_count_; ++i) {
      new (memory + i * kBucketSize) Bucket();
    }
  }

  KeyHandle Allocate(const size_t len, char** buf) override {
    char* const memory = allocator_->AllocateAligned(offsetof(Node, key) + len);
    Node* const node = new (memory) Node();
    *buf = node->key;
    return node;
  }

  void Insert(KeyHandle handle) override {
    const bool inserted = InsertKeyInternal(handle);
    static_cast<void>(inserted);
    assert(inserted);
  }

  bool InsertKey(KeyHandle handle) override {
    return InsertKeyInternal(handle);
  }

  void InsertConcurrently(KeyHandle handle) override {
    const bool inserted = InsertKeyInternal(handle);
    static_cast<void>(inserted);
    assert(inserted);
  }

  bool InsertKeyConcurrently(KeyHandle handle) override {
    return InsertKeyInternal(handle);
  }

  bool InsertKeyWithHint(KeyHandle handle, void** /*hint*/) override {
    return InsertKeyInternal(handle);
  }

  bool InsertKeyWithHintConcurrently(KeyHandle handle,
                                     void** /*hint*/) override {
    return InsertKeyInternal(handle);
  }

  bool Contains(const char* key) const override {
    const Slice internal_key = compare_.decode_key(key);
    Node* const node =
        FindGreaterOrEqual(GetBucket(internal_key), internal_key);
    return node != nullptr && compare_(node->key, internal_key) == 0;
  }

  // Charges one full sorted view (a pointer per entry) whether or not it has
  // been built. Deriving usage from writes alone keeps it fixed once the
  // memtable stops taking writes, which immutable-memtable accounting
  // requires, without building the view on the write path. Nodes and the
  // bucket array live in the memtable arena, which MemTable accounts for
  // separately.
  size_t ApproximateMemoryUsage() override {
    return num_entries_.load(std::memory_order_relaxed) * sizeof(const char*);
  }

  void Get(const LookupKey& key, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    GetEncoded(key.memtable_key().data(), callback_args, callback_func);
  }

  Status MultiGet(size_t num_keys, const char* const* keys,
                  void** callback_args,
                  bool (*callback_func)(void* arg, const char* entry),
                  bool allow_data_in_errors, bool detect_key_out_of_order,
                  const std::function<Status(const char*, bool)>&
                      key_validation_callback) override {
    if (detect_key_out_of_order || key_validation_callback != nullptr) {
      return MemTableRep::MultiGet(
          num_keys, keys, callback_args, callback_func, allow_data_in_errors,
          detect_key_out_of_order, key_validation_callback);
    }

    for (size_t i = 0; i < num_keys; ++i) {
      GetEncoded(keys[i], callback_args[i], callback_func);
    }
    return Status::OK();
  }

  void UniqueRandomSample(const uint64_t /*num_entries*/,
                          const uint64_t target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    assert(target_sample_size > 0);
    entries->clear();

    std::vector<const char*> candidates = CollectEntries();
    if (candidates.size() <= target_sample_size) {
      entries->insert(candidates.begin(), candidates.end());
      return;
    }

    Random* const random = Random::GetTLSInstance();
    for (size_t i = target_sample_size; i < candidates.size(); ++i) {
      const uint64_t selected = random->Next64() % (i + 1);
      if (selected < target_sample_size) {
        candidates[selected] = candidates[i];
      }
    }
    entries->insert(candidates.begin(),
                    candidates.begin() + target_sample_size);
  }

  class Iterator : public MemTableRep::Iterator {
   public:
    Iterator(const MemTableRep::KeyComparator& compare,
             std::shared_ptr<const std::vector<const char*>> entries)
        : compare_(&compare),
          entries_(std::move(entries)),
          position_(entries_->size()) {}

    bool Valid() const override { return position_ < entries_->size(); }

    const char* key() const override {
      assert(Valid());
      return (*entries_)[position_];
    }

    void Next() override {
      assert(Valid());
      ++position_;
    }

    void Prev() override {
      assert(Valid());
      if (position_ == 0) {
        position_ = entries_->size();
      } else {
        --position_;
      }
    }

    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const char* const target = memtable_key != nullptr
                                     ? memtable_key
                                     : EncodeKey(&tmp_, internal_key);
      const auto it =
          std::lower_bound(entries_->begin(), entries_->end(), target,
                           [this](const char* entry, const char* sought) {
                             return (*compare_)(entry, sought) < 0;
                           });
      position_ = lossless_cast<size_t>(it - entries_->begin());
    }

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const char* const target = memtable_key != nullptr
                                     ? memtable_key
                                     : EncodeKey(&tmp_, internal_key);
      const auto it =
          std::upper_bound(entries_->begin(), entries_->end(), target,
                           [this](const char* sought, const char* entry) {
                             return (*compare_)(sought, entry) < 0;
                           });
      position_ = it == entries_->begin()
                      ? entries_->size()
                      : lossless_cast<size_t>(it - entries_->begin()) - 1;
    }

    void RandomSeek() override {
      if (entries_->empty()) {
        position_ = 0;
        return;
      }
      position_ = static_cast<size_t>(Random::GetTLSInstance()->Next64() %
                                      entries_->size());
    }

    void SeekToFirst() override {
      position_ = entries_->empty() ? entries_->size() : 0;
    }

    void SeekToLast() override {
      position_ = entries_->empty() ? entries_->size() : entries_->size() - 1;
    }

   private:
    const MemTableRep::KeyComparator* compare_;
    std::shared_ptr<const std::vector<const char*>> entries_;
    size_t position_;
    std::string tmp_;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    auto entries = GetSortedEntries();
    if (arena == nullptr) {
      return new Iterator(compare_, std::move(entries));
    }
    char* const memory = arena->AllocateAligned(sizeof(Iterator));
    return new (memory) Iterator(compare_, std::move(entries));
  }

 private:
  Bucket& GetBucket(const Slice& internal_key) const {
    return buckets_[GetSliceRangedNPHash(ExtractUserKey(internal_key),
                                         bucket_count_)];
  }

  Node* FindGreaterOrEqual(const Bucket& bucket,
                           const Slice& internal_key) const {
    Node* node = bucket.head.load(std::memory_order_acquire);
    while (node != nullptr && compare_(node->key, internal_key) < 0) {
      node = node->Next();
    }
    return node;
  }

  bool InsertKeyInternal(KeyHandle handle) {
    Node* const node = static_cast<Node*>(handle);
    const Slice internal_key = compare_.decode_key(node->key);
    Bucket& bucket = GetBucket(internal_key);
    std::lock_guard<SpinMutex> lock(bucket.mutex);

    Node* previous = nullptr;
    Node* current = bucket.head.load(std::memory_order_relaxed);
    while (current != nullptr) {
      const int comparison = compare_(current->key, internal_key);
      if (comparison >= 0) {
        if (comparison == 0) {
          return false;
        }
        break;
      }
      previous = current;
      current = current->Next();
    }

    node->SetNextBeforePublish(current);
    if (previous == nullptr) {
      bucket.head.store(node, std::memory_order_release);
    } else {
      previous->SetNext(node);
    }
    // Pairs with the acquire load in GetSortedEntries() so that a reader that
    // observes the new count also observes this node.
    num_entries_.fetch_add(1, std::memory_order_release);
    return true;
  }

  void GetEncoded(const char* key, void* callback_args,
                  bool (*callback_func)(void* arg, const char* entry)) const {
    const Slice internal_key = compare_.decode_key(key);
    Node* node = FindGreaterOrEqual(GetBucket(internal_key), internal_key);
    while (node != nullptr && callback_func(callback_args, node->key)) {
      node = node->Next();
    }
  }

  std::vector<const char*> CollectEntries() const {
    std::vector<const char*> entries;
    entries.reserve(num_entries_.load(std::memory_order_relaxed));
    for (size_t i = 0; i < bucket_count_; ++i) {
      Node* node = buckets_[i].head.load(std::memory_order_acquire);
      while (node != nullptr) {
        entries.push_back(node->key);
        node = node->Next();
      }
    }
    return entries;
  }

  std::shared_ptr<const std::vector<const char*>> GetSortedEntries() const {
    MutexLock lock(&sorted_entries_mutex_);
    const size_t version = num_entries_.load(std::memory_order_acquire);
    if (sorted_entries_ == nullptr || sorted_entries_version_ != version) {
      auto entries =
          std::make_shared<std::vector<const char*>>(CollectEntries());
      std::sort(entries->begin(), entries->end(),
                [this](const char* lhs, const char* rhs) {
                  return compare_(lhs, rhs) < 0;
                });
      sorted_entries_ = std::move(entries);
      sorted_entries_version_ = version;
    }
    return sorted_entries_;
  }

  const MemTableRep::KeyComparator& compare_;
  const size_t bucket_count_;
  Bucket* buckets_;
  std::atomic<size_t> num_entries_{0};
  mutable port::Mutex sorted_entries_mutex_;
  mutable std::shared_ptr<const std::vector<const char*>> sorted_entries_;
  mutable size_t sorted_entries_version_{0};
};

struct SortedHashRepOptions {
  static const char* kName() { return "SortedHashRepFactoryOptions"; }
  size_t bucket_count;
};

const std::unordered_map<std::string, OptionTypeInfo>&
GetSortedHashOptionInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> option_info = {
      {"bucket_count",
       {offsetof(struct SortedHashRepOptions, bucket_count), OptionType::kSizeT,
        OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
  };
  return option_info;
}

class SortedHashRepFactory : public MemTableRepFactory {
 public:
  explicit SortedHashRepFactory(size_t bucket_count) {
    options_.bucket_count = bucket_count;
    RegisterOptions(&options_, &GetSortedHashOptionInfo());
  }

  using MemTableRepFactory::CreateMemTableRep;
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& compare,
                                 Allocator* allocator,
                                 const SliceTransform* /*transform*/,
                                 Logger* /*logger*/) override {
    return new SortedHashRep(compare, allocator, options_.bucket_count);
  }

  static const char* kClassName() { return "SortedHashRepFactory"; }
  static const char* kNickName() { return "sorted_hash"; }

  const char* Name() const override { return kClassName(); }
  const char* NickName() const override { return kNickName(); }

  bool IsInsertConcurrentlySupported() const override { return true; }
  bool CanHandleDuplicatedKey() const override { return true; }
  bool IsTailingIteratorSupported() const override { return false; }

  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override {
    const Comparator* const ucmp = cf_opts.comparator;
    assert(ucmp != nullptr);
    if (ucmp->timestamp_size() > 0 ||
        ucmp->CanKeysWithDifferentByteContentsBeEqual()) {
      return Status::NotSupported(
          "SortedHashRepFactory requires a comparator without user-defined "
          "timestamps under which equal keys are byte-identical");
    }
    if (cf_opts.paranoid_memory_checks ||
        cf_opts.memtable_verify_per_key_checksum_on_seek) {
      return Status::NotSupported(
          "SortedHashRepFactory does not support paranoid_memory_checks or "
          "memtable_verify_per_key_checksum_on_seek");
    }
    // Both look up the latest entry for a key through a memtable iterator on
    // every write, which would sort the whole memtable each time.
    if (cf_opts.inplace_update_support || cf_opts.max_successive_merges > 0) {
      return Status::NotSupported(
          "SortedHashRepFactory does not support inplace_update_support or "
          "max_successive_merges");
    }
    if (options_.bucket_count == 0) {
      return Status::InvalidArgument(
          "SortedHashRepFactory bucket_count must be positive");
    }
    if (options_.bucket_count >=
        cf_opts.write_buffer_size / SortedHashRep::kBucketSize) {
      return Status::InvalidArgument(
          "SortedHashRepFactory bucket array must be smaller than "
          "write_buffer_size");
    }
    return MemTableRepFactory::ValidateOptions(db_opts, cf_opts);
  }

 private:
  SortedHashRepOptions options_{};
};

}  // namespace

MemTableRepFactory* NewSortedHashRepFactory(size_t bucket_count) {
  return new SortedHashRepFactory(bucket_count);
}

}  // namespace ROCKSDB_NAMESPACE
