//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <random>

#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/inlineskiplist.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/utilities/options_type.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
class SkipListRep : public MemTableRep {
  InlineSkipList<const MemTableRep::KeyComparator&> skip_list_;
  const MemTableRep::KeyComparator& cmp_;
  const SliceTransform* transform_;
  const size_t lookahead_;

  friend class LookaheadIterator;

 public:
  explicit SkipListRep(const MemTableRep::KeyComparator& compare,
                       Allocator* allocator, const SliceTransform* transform,
                       const size_t lookahead)
      : MemTableRep(allocator),
        skip_list_(compare, allocator),
        cmp_(compare),
        transform_(transform),
        lookahead_(lookahead) {}

  KeyHandle Allocate(const size_t len, char** buf) override {
    *buf = skip_list_.AllocateKey(len);
    return static_cast<KeyHandle>(*buf);
  }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(KeyHandle handle) override {
    skip_list_.Insert(static_cast<char*>(handle));
  }

  bool InsertKey(KeyHandle handle) override {
    return skip_list_.Insert(static_cast<char*>(handle));
  }

  void InsertWithHint(KeyHandle handle, void** hint) override {
    skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  bool InsertKeyWithHint(KeyHandle handle, void** hint) override {
    return skip_list_.InsertWithHint(static_cast<char*>(handle), hint);
  }

  void InsertWithHintConcurrently(KeyHandle handle, void** hint) override {
    skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle), hint);
  }

  bool InsertKeyWithHintConcurrently(KeyHandle handle, void** hint) override {
    return skip_list_.InsertWithHintConcurrently(static_cast<char*>(handle),
                                                 hint);
  }

  void InsertConcurrently(KeyHandle handle) override {
    skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  bool InsertKeyConcurrently(KeyHandle handle) override {
    return skip_list_.InsertConcurrently(static_cast<char*>(handle));
  }

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const override {
    return skip_list_.Contains(key);
  }

  size_t ApproximateMemoryUsage() override {
    // All memory is allocated through allocator; nothing to report here
    return 0;
  }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    SkipListRep::Iterator iter(&skip_list_);
    Slice dummy_slice;
    for (iter.Seek(dummy_slice, k.memtable_key().data());
         iter.Valid() && callback_func(callback_args, iter.key());
         iter.Next()) {
    }
  }

  Status GetAndValidate(const LookupKey& k, void* callback_args,
                        bool (*callback_func)(void* arg, const char* entry),
                        bool allow_data_in_errors, bool detect_key_out_of_order,
                        const std::function<Status(const char*, bool)>&
                            key_validation_callback) override {
    SkipListRep::Iterator iter(&skip_list_);
    Slice dummy_slice;
    Status status = iter.SeekAndValidate(
        dummy_slice, k.memtable_key().data(), allow_data_in_errors,
        detect_key_out_of_order, key_validation_callback);
    for (; iter.Valid() && status.ok() &&
           callback_func(callback_args, iter.key());
         status = iter.NextAndValidate(allow_data_in_errors)) {
    }
    return status;
  }

  Status MultiGet(size_t num_keys, const char* const* keys,
                  void** callback_args,
                  bool (*callback_func)(void* arg, const char* entry),
                  bool allow_data_in_errors, bool detect_key_out_of_order,
                  const std::function<Status(const char*, bool)>&
                      key_validation_callback) override {
    return skip_list_.MultiGet(num_keys, keys, callback_args, callback_func,
                               allow_data_in_errors, detect_key_out_of_order,
                               key_validation_callback);
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    return skip_list_.ApproximateNumEntries(start_ikey, end_ikey);
  }

  void UniqueRandomSample(const uint64_t num_entries,
                          const uint64_t target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    entries->clear();
    // Avoid divide-by-0.
    assert(target_sample_size > 0);
    assert(num_entries > 0);
    // NOTE: the size of entries is not enforced to be exactly
    // target_sample_size at the end of this function, it might be slightly
    // greater or smaller.
    SkipListRep::Iterator iter(&skip_list_);
    // There are two methods to create the subset of samples (size m)
    // from the table containing N elements:
    // 1-Iterate linearly through the N memtable entries. For each entry i,
    //   add it to the sample set with a probability
    //   (target_sample_size - entries.size() ) / (N-i).
    //
    // 2-Pick m random elements without repetition.
    // We pick Option 2 when m<sqrt(N) and
    // Option 1 when m > sqrt(N).
    if (target_sample_size >
        static_cast<uint64_t>(std::sqrt(1.0 * num_entries))) {
      Random* rnd = Random::GetTLSInstance();
      iter.SeekToFirst();
      uint64_t counter = 0;
      uint64_t num_samples_left = target_sample_size;
      for (; iter.Valid() && (num_samples_left > 0); iter.Next(), counter++) {
        // Add entry to sample set with probability
        // num_samples_left/(num_entries - counter).
        if (rnd->Next() % (num_entries - counter) < num_samples_left) {
          entries->insert(iter.key());
          num_samples_left--;
        }
      }
    } else {
      // Option 2: pick m random elements with no duplicates.
      // If Option 2 is picked, then target_sample_size<sqrt(N)
      // Using a set spares the need to check for duplicates.
      for (uint64_t i = 0; i < target_sample_size; i++) {
        // We give it 5 attempts to find a non-duplicate
        // With 5 attempts, the chances of returning `entries` set
        // of size target_sample_size is:
        // PROD_{i=1}^{target_sample_size-1} [1-(i/N)^5]
        // which is monotonically increasing with N in the worse case
        // of target_sample_size=sqrt(N), and is always >99.9% for N>4.
        // At worst, for the final pick , when m=sqrt(N) there is
        // a probability of p= 1/sqrt(N) chances to find a duplicate.
        for (uint64_t j = 0; j < 5; j++) {
          iter.RandomSeek();
          // unordered_set::insert returns pair<iterator, bool>.
          // The second element is true if an insert successfully happened.
          // If element is already in the set, this bool will be false, and
          // true otherwise.
          if ((entries->insert(iter.key())).second) {
            break;
          }
        }
      }
    }
  }

  ~SkipListRep() override = default;

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;

   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(
        const InlineSkipList<const MemTableRep::KeyComparator&>* list)
        : iter_(list) {}

    ~Iterator() override = default;

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

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
    void Seek(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.Seek(memtable_key);
      } else {
        iter_.Seek(EncodeKey(&tmp_, user_key));
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        iter_.SeekForPrev(memtable_key);
      } else {
        iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
      }
    }

    void RandomSeek() override { iter_.RandomSeek(); }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

    Status NextAndValidate(bool allow_data_in_errors) override {
      assert(Valid());
      return iter_.NextAndValidate(allow_data_in_errors);
    }

    Status SeekAndValidate(const Slice& user_key, const char* memtable_key,
                           bool allow_data_in_errors,
                           bool detect_key_out_of_order,
                           const std::function<Status(const char*, bool)>&
                               key_validation_callback) override {
      if (memtable_key != nullptr) {
        return iter_.SeekAndValidate(memtable_key, allow_data_in_errors,
                                     detect_key_out_of_order,
                                     key_validation_callback);
      } else {
        return iter_.SeekAndValidate(
            EncodeKey(&tmp_, user_key), allow_data_in_errors,
            detect_key_out_of_order, key_validation_callback);
      }
    }

    Status PrevAndValidate(bool allow_data_in_error) override {
      assert(Valid());
      return iter_.PrevAndValidate(allow_data_in_error);
    }

   protected:
    std::string tmp_;  // For passing to EncodeKey
  };

  // Iterator over the contents of a skip list which also keeps track of the
  // previously visited node. In Seek(), it examines a few nodes after it
  // first, falling back to O(log n) search from the head of the list only if
  // the target key hasn't been found.
  class LookaheadIterator : public MemTableRep::Iterator {
   public:
    explicit LookaheadIterator(const SkipListRep& rep)
        : rep_(rep), iter_(&rep_.skip_list_), prev_(iter_) {}

    ~LookaheadIterator() override = default;

    bool Valid() const override { return iter_.Valid(); }

    const char* key() const override {
      assert(Valid());
      return iter_.key();
    }

    void Next() override {
      assert(Valid());

      bool advance_prev = true;
      if (prev_.Valid()) {
        auto k1 = rep_.UserKey(prev_.key());
        auto k2 = rep_.UserKey(iter_.key());

        if (k1.compare(k2) == 0) {
          // same user key, don't move prev_
          advance_prev = false;
        } else if (rep_.transform_) {
          // only advance prev_ if it has the same prefix as iter_
          auto t1 = rep_.transform_->Transform(k1);
          auto t2 = rep_.transform_->Transform(k2);
          advance_prev = t1.compare(t2) == 0;
        }
      }

      if (advance_prev) {
        prev_ = iter_;
      }
      iter_.Next();
    }

    void Prev() override {
      assert(Valid());
      iter_.Prev();
      prev_ = iter_;
    }

    void Seek(const Slice& internal_key, const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);

      if (prev_.Valid() && rep_.cmp_(encoded_key, prev_.key()) >= 0) {
        // prev_.key() is smaller or equal to our target key; do a quick
        // linear search (at most lookahead_ steps) starting from prev_
        iter_ = prev_;

        size_t cur = 0;
        while (cur++ <= rep_.lookahead_ && iter_.Valid()) {
          if (rep_.cmp_(encoded_key, iter_.key()) <= 0) {
            return;
          }
          Next();
        }
      }

      iter_.Seek(encoded_key);
      prev_ = iter_;
    }

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      const char* encoded_key = (memtable_key != nullptr)
                                    ? memtable_key
                                    : EncodeKey(&tmp_, internal_key);
      iter_.SeekForPrev(encoded_key);
      prev_ = iter_;
    }

    void SeekToFirst() override {
      iter_.SeekToFirst();
      prev_ = iter_;
    }

    void SeekToLast() override {
      iter_.SeekToLast();
      prev_ = iter_;
    }

   protected:
    std::string tmp_;  // For passing to EncodeKey

   private:
    const SkipListRep& rep_;
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator iter_;
    InlineSkipList<const MemTableRep::KeyComparator&>::Iterator prev_;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    if (lookahead_ > 0) {
      void* mem =
          arena ? arena->AllocateAligned(sizeof(SkipListRep::LookaheadIterator))
                : operator new(sizeof(SkipListRep::LookaheadIterator));
      return new (mem) SkipListRep::LookaheadIterator(*this);
    } else {
      void* mem = arena ? arena->AllocateAligned(sizeof(SkipListRep::Iterator))
                        : operator new(sizeof(SkipListRep::Iterator));
      return new (mem) SkipListRep::Iterator(&skip_list_);
    }
  }
};
}  // namespace

static std::unordered_map<std::string, OptionTypeInfo> skiplist_factory_info = {
    {"lookahead",
     {0, OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kDontSerialize /*Since it is part of the ID*/}},
};

SkipListFactory::SkipListFactory(size_t lookahead) : lookahead_(lookahead) {
  RegisterOptions("SkipListFactoryOptions", &lookahead_,
                  &skiplist_factory_info);
}

std::string SkipListFactory::GetId() const {
  std::string id = Name();
  if (lookahead_ > 0) {
    id.append(":").append(std::to_string(lookahead_));
  }
  return id;
}

MemTableRep* SkipListFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* transform, Logger* /*logger*/) {
  return new SkipListRep(compare, allocator, transform, lookahead_);
}

}  // namespace ROCKSDB_NAMESPACE
