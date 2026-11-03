//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <string>
#include <vector>

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

TEST_F(MergerTest, OrdinaryInputsRetainValueTypeOrdering) {
  const std::string deletion =
      InternalKey("m", 7, kTypeDeletion).Encode().ToString();
  const std::string value = InternalKey("m", 7, kTypeValue).Encode().ToString();
  InternalIterator* inputs[] = {
      new VectorIterator({deletion}, {"deleted"}, &icomp_),
      new VectorIterator({value}, {"value"}, &icomp_)};
  std::unique_ptr<InternalIterator> iter(
      NewMergingIterator(&icomp_, inputs, 2));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(value, iter->key().ToString());
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(deletion, iter->key().ToString());
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(deletion, iter->key().ToString());
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(value, iter->key().ToString());
  ASSERT_OK(iter->status());
}

TEST_F(MergerTest, ExplicitSourcePriorityOrdersEqualZeroPoints) {
  const std::string deletion =
      InternalKey("m", 0, kTypeDeletion).Encode().ToString();
  const std::string value = InternalKey("m", 0, kTypeValue).Encode().ToString();
  Arena arena;
  MergeIteratorBuilder builder(&icomp_, &arena);
  builder.SetSourcePriorityForEqualSequences(true);
  builder.AddIterator(new (arena.AllocateAligned(sizeof(VectorIterator)))
                          VectorIterator({deletion}, {"deleted"}, &icomp_));
  builder.AddIterator(new (arena.AllocateAligned(sizeof(VectorIterator)))
                          VectorIterator({value}, {"value"}, &icomp_));
  ScopedArenaPtr<InternalIterator> iter(builder.Finish());
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(deletion, iter->key().ToString());
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(value, iter->key().ToString());
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(value, iter->key().ToString());
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(deletion, iter->key().ToString());
  ASSERT_OK(iter->status());
}

TEST_F(MergerTest, SourcePriorityDirectionSwitchRetriesAsyncSeek) {
  class DelayedSeekIterator : public VectorIterator {
   public:
    using VectorIterator::VectorIterator;

    void Seek(const Slice& target) override {
      ++seek_calls;
      pending = delay_next_seek;
      delay_next_seek = false;
      if (!pending) {
        VectorIterator::Seek(target);
      }
    }
    bool Valid() const override { return !pending && VectorIterator::Valid(); }
    Status status() const override {
      return pending ? Status::TryAgain() : Status::OK();
    }

    bool delay_next_seek = false;
    bool pending = false;
    int seek_calls = 0;
  };

  const std::string upper_a =
      InternalKey("a", 0, kTypeValue).Encode().ToString();
  const std::string lower_a =
      InternalKey("a", 0, kTypeWideColumnEntity).Encode().ToString();
  const std::string b = InternalKey("b", 0, kTypeValue).Encode().ToString();
  const std::string c = InternalKey("c", 0, kTypeValue).Encode().ToString();
  Arena arena;
  MergeIteratorBuilder builder(&icomp_, &arena);
  builder.SetSourcePriorityForEqualSequences(true);
  auto* upper = new (arena.AllocateAligned(sizeof(DelayedSeekIterator)))
      DelayedSeekIterator({upper_a, b, c}, {"new", "middle", "last"}, &icomp_);
  builder.AddIterator(upper);
  builder.AddIterator(new (arena.AllocateAligned(sizeof(VectorIterator)))
                          VectorIterator({lower_a}, {"old"}, &icomp_));
  ScopedArenaPtr<InternalIterator> iter(builder.Finish());
  iter->SeekToLast();
  iter->Prev();
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(lower_a, iter->key().ToString());
  upper->seek_calls = 0;
  upper->delay_next_seek = true;
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(b, iter->key().ToString());
  EXPECT_EQ(2, upper->seek_calls);
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(lower_a, iter->key().ToString());
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(upper_a, iter->key().ToString());
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(lower_a, iter->key().ToString());
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(upper_a, iter->key().ToString());
  ASSERT_OK(iter->status());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
