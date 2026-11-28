//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>
#include <vector>

#include "db/memtable.h"
#include "memory/arena.h"
#include "rocksdb/memtablerep.h"
#include "table/merging_iterator.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

class MergerTest : public testing::Test {
 public:
  MergerTest()
      : icomp_(BytewiseComparator()),
        rnd_(3),
        merging_iterator_(nullptr),
        single_iterator_(nullptr) {}
  ~MergerTest() override = default;
  std::vector<std::string> GenerateStrings(size_t len, int string_len) {
    std::vector<std::string> ret;

    for (size_t i = 0; i < len; ++i) {
      InternalKey ik(rnd_.HumanReadableString(string_len), 0,
                     ValueType::kTypeValue);
      ret.push_back(ik.Encode().ToString(false));
    }
    return ret;
  }

  void AssertEquivalence() {
    auto a = merging_iterator_.get();
    auto b = single_iterator_.get();
    if (!a->Valid()) {
      ASSERT_TRUE(!b->Valid());
    } else {
      ASSERT_TRUE(b->Valid());
      ASSERT_EQ(b->key().ToString(), a->key().ToString());
      ASSERT_EQ(b->value().ToString(), a->value().ToString());
    }
  }

  void SeekToRandom() {
    InternalKey ik(rnd_.HumanReadableString(5), 0, ValueType::kTypeValue);
    Seek(ik.Encode().ToString(false));
  }

  void Seek(const std::string& target) {
    merging_iterator_->Seek(target);
    single_iterator_->Seek(target);
  }

  void SeekToFirst() {
    merging_iterator_->SeekToFirst();
    single_iterator_->SeekToFirst();
  }

  void SeekToLast() {
    merging_iterator_->SeekToLast();
    single_iterator_->SeekToLast();
  }

  void Next(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      merging_iterator_->Next();
      single_iterator_->Next();
    }
    AssertEquivalence();
  }

  void Prev(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      merging_iterator_->Prev();
      single_iterator_->Prev();
    }
    AssertEquivalence();
  }

  void NextAndPrev(int times) {
    for (int i = 0; i < times && merging_iterator_->Valid(); ++i) {
      AssertEquivalence();
      if (rnd_.OneIn(2)) {
        merging_iterator_->Prev();
        single_iterator_->Prev();
      } else {
        merging_iterator_->Next();
        single_iterator_->Next();
      }
    }
    AssertEquivalence();
  }

  void Generate(size_t num_iterators, size_t strings_per_iterator,
                int letters_per_string) {
    std::vector<InternalIterator*> small_iterators;
    for (size_t i = 0; i < num_iterators; ++i) {
      auto strings = GenerateStrings(strings_per_iterator, letters_per_string);
      small_iterators.push_back(new VectorIterator(strings, strings, &icomp_));
      all_keys_.insert(all_keys_.end(), strings.begin(), strings.end());
    }

    merging_iterator_.reset(
        NewMergingIterator(&icomp_, small_iterators.data(),
                           static_cast<int>(small_iterators.size())));
    single_iterator_.reset(new VectorIterator(all_keys_, all_keys_, &icomp_));
  }

  InternalKeyComparator icomp_;
  Random rnd_;
  std::unique_ptr<InternalIterator> merging_iterator_;
  std::unique_ptr<InternalIterator> single_iterator_;
  std::vector<std::string> all_keys_;
};

TEST_F(MergerTest, SeekToRandomNextTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToRandomNextSmallStringsTest) {
  Generate(1000, 50, 2);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToRandomPrevTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToRandom();
    AssertEquivalence();
    Prev(50000);
  }
}

TEST_F(MergerTest, SeekToRandomRandomTest) {
  Generate(200, 50, 50);
  for (int i = 0; i < 3; ++i) {
    SeekToRandom();
    AssertEquivalence();
    NextAndPrev(5000);
  }
}

TEST_F(MergerTest, SeekToFirstTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToFirst();
    AssertEquivalence();
    Next(50000);
  }
}

TEST_F(MergerTest, SeekToLastTest) {
  Generate(1000, 50, 50);
  for (int i = 0; i < 10; ++i) {
    SeekToLast();
    AssertEquivalence();
    Prev(50000);
  }
}

namespace {
// Returns TryAgain from its first Seek() or SeekForPrev(), like a table
// iterator that started an async read, and records every seek target.
class TryAgainOnceIterator : public VectorIterator {
 public:
  using VectorIterator::VectorIterator;
  void Seek(const Slice& target) override {
    targets.push_back(target.ToString());
    // VectorIterator::Seek() may call Valid(), so clear the flag first.
    try_again_ = false;
    VectorIterator::Seek(target);
    try_again_ = targets.size() == 1;
  }
  void SeekForPrev(const Slice& target) override {
    targets.push_back(target.ToString());
    // VectorIterator::SeekForPrev() may call Valid(), so clear the flag first.
    try_again_ = false;
    VectorIterator::SeekForPrev(target);
    try_again_ = targets.size() == 1;
  }
  bool Valid() const override { return !try_again_ && VectorIterator::Valid(); }
  Status status() const override {
    return try_again_ ? Status::TryAgain() : Status::OK();
  }
  std::vector<std::string> targets;

 private:
  bool try_again_ = false;
};
}  // namespace

TEST_F(MergerTest, HeadMarkSeekRetriesTryAgainChildAtMovedKey) {
  // A memtable head mark (see MemTableHeadMark) moves the seek of the
  // memtable level and every older level. An older level that returns
  // TryAgain must be sought again at the moved key, not at the original
  // target. No level has range tombstones here.
  Options options;
  options.min_tombstones_for_range_conversion = 1;
  options.memtable_factory = std::make_shared<SkipListFactory>();
  ImmutableOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(new MemTable(
      icomp_, ioptions, MutableCFOptions(options), &wb, /*earliest_seq=*/1,
      0 /* column_family_id */, nullptr /* flush_initiator */));
  HeadMarkIngestState ingest;
  port::RWMutex ingest_sst_lock;
  ASSERT_EQ(HeadMarkPublishResult::kPublished,
            mem->PublishHeadMark(HeadMarkDirection::kForward, "q",
                                 /*start_exclusive=*/false, "q0010",
                                 /*seq=*/10, /*sender=*/nullptr,
                                 /*run_is_long=*/true, /*version_number=*/1,
                                 ingest, ingest_sst_lock));

  auto ikey = [](const std::string& user_key, SequenceNumber seq) {
    return InternalKey(user_key, seq, kTypeValue).Encode().ToString();
  };
  Arena arena;
  MergeIteratorBuilder builder(&icomp_, &arena);
  // Level 0 stands for the memtable, level 1 for an older file with an old
  // put inside the mark.
  builder.AddIterator(
      new (arena.AllocateAligned(sizeof(VectorIterator)))
          VectorIterator({ikey("q0010", 5)}, {"live"}, &icomp_));
  builder.SetHeadMarkMemTable(mem.get());
  auto* older = new (arena.AllocateAligned(sizeof(TryAgainOnceIterator)))
      TryAgainOnceIterator({ikey("q0003", 1), ikey("q0011", 1)}, {"old", "v"},
                           &icomp_);
  builder.AddIterator(older);
  InternalIterator* iter = builder.Finish();
  const MemTableHeadMark* used = nullptr;
  const MemTableHeadMark* last = nullptr;
  iter->SeekWithHeadMarks(ikey("q", kMaxSequenceNumber), /*read_seq=*/20,
                          /*read_version=*/1, mem.get(), &used, &last);
  ASSERT_NE(nullptr, used);
  ASSERT_EQ(used, last);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("q0010", ExtractUserKey(iter->key()).ToString());
  ASSERT_EQ(2, older->targets.size());
  ASSERT_EQ("q0010", ExtractUserKey(older->targets[1]).ToString());
  iter->~InternalIterator();
}

TEST_F(MergerTest, ReverseHeadMarkSeekRetriesTryAgainChildAtMovedKey) {
  // The reverse form: a reverse mark claims ("q0010", "q0019"]. An older level
  // that returns TryAgain must be sought again at the moved key "q0010".
  Options options;
  options.min_tombstones_for_range_conversion = 1;
  options.memtable_factory = std::make_shared<SkipListFactory>();
  ImmutableOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(new MemTable(
      icomp_, ioptions, MutableCFOptions(options), &wb, /*earliest_seq=*/1,
      0 /* column_family_id */, nullptr /* flush_initiator */));
  HeadMarkIngestState ingest;
  port::RWMutex ingest_sst_lock;
  ASSERT_EQ(HeadMarkPublishResult::kPublished,
            mem->PublishHeadMark(HeadMarkDirection::kReverse, "q0019",
                                 /*start_exclusive=*/false, "q0010",
                                 /*seq=*/10, /*sender=*/nullptr,
                                 /*run_is_long=*/true, /*version_number=*/1,
                                 ingest, ingest_sst_lock));

  auto ikey = [](const std::string& user_key, SequenceNumber seq,
                 ValueType type = kTypeValue) {
    return InternalKey(user_key, seq, type).Encode().ToString();
  };
  Arena arena;
  MergeIteratorBuilder builder(&icomp_, &arena);
  builder.AddIterator(
      new (arena.AllocateAligned(sizeof(VectorIterator)))
          VectorIterator({ikey("q0010", 5)}, {"live"}, &icomp_));
  builder.SetHeadMarkMemTable(mem.get());
  auto* older = new (arena.AllocateAligned(sizeof(TryAgainOnceIterator)))
      TryAgainOnceIterator({ikey("q0009", 1), ikey("q0015", 1)}, {"v", "old"},
                           &icomp_);
  builder.AddIterator(older);
  InternalIterator* iter = builder.Finish();
  const MemTableHeadMark* used = nullptr;
  const MemTableHeadMark* last = nullptr;
  iter->SeekForPrevWithHeadMarks(ikey("q0019", 0, kValueTypeForSeekForPrev),
                                 /*read_seq=*/20, /*read_version=*/1, mem.get(),
                                 &used, &last);
  ASSERT_NE(nullptr, used);
  ASSERT_EQ(used, last);
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("q0010", ExtractUserKey(iter->key()).ToString());
  ASSERT_EQ(2, older->targets.size());
  ASSERT_EQ("q0010", ExtractUserKey(older->targets[1]).ToString());
  iter->~InternalIterator();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
