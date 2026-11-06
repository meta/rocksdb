//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/range_tombstone_fragmenter.h"
#include "memory/arena.h"
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

  void Seek(std::string target) {
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

  std::string IKey(const Slice& user_key, SequenceNumber seq,
                   ValueType value_type) {
    return InternalKey(user_key, seq, value_type).Encode().ToString();
  }

  VectorIterator* NewArenaVectorIterator(Arena* arena,
                                         std::vector<std::string> keys,
                                         std::vector<std::string> values) {
    auto mem = arena->AllocateAligned(sizeof(VectorIterator));
    return new (mem)
        VectorIterator(std::move(keys), std::move(values), &icomp_);
  }

  std::unique_ptr<TruncatedRangeDelIterator> NewRangeTombstoneIter(
      const std::vector<RangeTombstone>& range_dels) {
    std::vector<std::string> keys, values;
    for (const auto& range_del : range_dels) {
      auto key_and_value = range_del.Serialize();
      keys.push_back(key_and_value.first.Encode().ToString());
      values.push_back(key_and_value.second.ToString());
    }
    auto range_del_iter = std::unique_ptr<VectorIterator>(
        new VectorIterator(keys, values, &icomp_));
    auto fragment_list = std::make_shared<FragmentedRangeTombstoneList>(
        std::move(range_del_iter), icomp_);
    auto fragment_iter = std::unique_ptr<FragmentedRangeTombstoneIterator>(
        new FragmentedRangeTombstoneIterator(fragment_list, icomp_,
                                             kMaxSequenceNumber));
    return std::unique_ptr<TruncatedRangeDelIterator>(
        new TruncatedRangeDelIterator(std::move(fragment_iter), &icomp_,
                                      nullptr, nullptr));
  }

  InternalIterator* NewSameReadSeqRangeTombstoneIterator(Arena* arena) {
    constexpr SequenceNumber kReadSeq = 10;

    MergeIteratorBuilder builder(&icomp_, arena, false /* prefix_seek_mode */);
    builder.AddPointAndTombstoneIterator(
        NewArenaVectorIterator(arena, {}, {}),
        NewRangeTombstoneIter({RangeTombstone("b", "e", kReadSeq)}));
    builder.AddPointAndTombstoneIterator(
        NewArenaVectorIterator(
            arena,
            {IKey("c", 9, kTypeDeletion), IKey("d", kReadSeq, kTypeDeletion)},
            {"", ""}),
        std::unique_ptr<TruncatedRangeDelIterator>());
    InternalIterator* iter = builder.Finish();
    iter->SetRangeDelReadSeqno(kReadSeq);
    return iter;
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

TEST_F(MergerTest, SameReadSeqRangeTombstoneDoesNotHideSameSeqPointDelete) {
  constexpr SequenceNumber kReadSeq = 10;

  {
    Arena arena;
    InternalIterator* iter = NewSameReadSeqRangeTombstoneIterator(&arena);
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(IKey("d", kReadSeq, kTypeDeletion), iter->key().ToString());
    iter->Next();
    EXPECT_FALSE(iter->Valid());
    EXPECT_OK(iter->status());
    iter->~InternalIterator();
  }

  {
    Arena arena;
    InternalIterator* iter = NewSameReadSeqRangeTombstoneIterator(&arena);
    iter->Seek(IKey("d", kReadSeq, kValueTypeForSeek));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(IKey("d", kReadSeq, kTypeDeletion), iter->key().ToString());
    iter->~InternalIterator();
  }

  {
    Arena arena;
    InternalIterator* iter = NewSameReadSeqRangeTombstoneIterator(&arena);
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(IKey("d", kReadSeq, kTypeDeletion), iter->key().ToString());
    iter->Prev();
    EXPECT_FALSE(iter->Valid());
    EXPECT_OK(iter->status());
    iter->~InternalIterator();
  }

  {
    Arena arena;
    InternalIterator* iter = NewSameReadSeqRangeTombstoneIterator(&arena);
    iter->SeekForPrev(IKey("e", 0, kValueTypeForSeekForPrev));
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(IKey("d", kReadSeq, kTypeDeletion), iter->key().ToString());
    iter->~InternalIterator();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
