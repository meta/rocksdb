//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/range_del_aggregator.h"

#include <memory>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "test_util/testutil.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

class RangeDelAggregatorTest : public testing::Test {};

namespace {

static auto bytewise_icmp = InternalKeyComparator(BytewiseComparator());

std::unique_ptr<InternalIterator> MakeRangeDelIter(
    const std::vector<RangeTombstone>& range_dels) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (const auto& range_del : range_dels) {
    auto key_and_value = range_del.Serialize();
    keys.push_back(key_and_value.first.Encode().ToString());
    values.push_back(key_and_value.second.ToString());
  }
  return std::unique_ptr<VectorIterator>(
      new VectorIterator(keys, values, &bytewise_icmp));
}

std::vector<std::unique_ptr<FragmentedRangeTombstoneList>>
MakeFragmentedTombstoneLists(
    const std::vector<std::vector<RangeTombstone>>& range_dels_list) {
  std::vector<std::unique_ptr<FragmentedRangeTombstoneList>> fragment_lists;
  for (const auto& range_dels : range_dels_list) {
    auto range_del_iter = MakeRangeDelIter(range_dels);
    fragment_lists.emplace_back(new FragmentedRangeTombstoneList(
        std::move(range_del_iter), bytewise_icmp));
  }
  return fragment_lists;
}

struct TruncatedIterScanTestCase {
  ParsedInternalKey start;
  ParsedInternalKey end;
  SequenceNumber seq;
};

struct TruncatedIterSeekTestCase {
  Slice target;
  ParsedInternalKey start;
  ParsedInternalKey end;
  SequenceNumber seq;
  bool invalid;
};

struct ShouldDeleteTestCase {
  ParsedInternalKey lookup_key;
  bool result;
};

struct IsRangeOverlappedTestCase {
  Slice start;
  Slice end;
  bool result;
};

ParsedInternalKey UncutEndpoint(const Slice& s) {
  return ParsedInternalKey(s, kMaxSequenceNumber, kTypeRangeDeletion);
}

ParsedInternalKey InternalValue(const Slice& key, SequenceNumber seq,
                                ValueType type = kTypeValue) {
  return ParsedInternalKey(key, seq, type);
}

void VerifyIterator(
    TruncatedRangeDelIterator* iter, const InternalKeyComparator& icmp,
    const std::vector<TruncatedIterScanTestCase>& expected_range_dels) {
  // Test forward iteration.
  iter->SeekToFirst();
  for (size_t i = 0; i < expected_range_dels.size(); i++, iter->Next()) {
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(0, icmp.Compare(iter->start_key(), expected_range_dels[i].start));
    EXPECT_EQ(0, icmp.Compare(iter->end_key(), expected_range_dels[i].end));
    EXPECT_EQ(expected_range_dels[i].seq, iter->seq());
  }
  EXPECT_FALSE(iter->Valid());

  // Test reverse iteration.
  iter->SeekToLast();
  std::vector<TruncatedIterScanTestCase> reverse_expected_range_dels(
      expected_range_dels.rbegin(), expected_range_dels.rend());
  for (size_t i = 0; i < reverse_expected_range_dels.size();
       i++, iter->Prev()) {
    ASSERT_TRUE(iter->Valid());
    EXPECT_EQ(0, icmp.Compare(iter->start_key(),
                              reverse_expected_range_dels[i].start));
    EXPECT_EQ(
        0, icmp.Compare(iter->end_key(), reverse_expected_range_dels[i].end));
    EXPECT_EQ(reverse_expected_range_dels[i].seq, iter->seq());
  }
  EXPECT_FALSE(iter->Valid());
}

void VerifySeek(TruncatedRangeDelIterator* iter,
                const InternalKeyComparator& icmp,
                const std::vector<TruncatedIterSeekTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    iter->Seek(test_case.target);
    if (test_case.invalid) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(0, icmp.Compare(iter->start_key(), test_case.start));
      EXPECT_EQ(0, icmp.Compare(iter->end_key(), test_case.end));
      EXPECT_EQ(test_case.seq, iter->seq());
    }
  }
}

void VerifySeekForPrev(
    TruncatedRangeDelIterator* iter, const InternalKeyComparator& icmp,
    const std::vector<TruncatedIterSeekTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    iter->SeekForPrev(test_case.target);
    if (test_case.invalid) {
      ASSERT_FALSE(iter->Valid());
    } else {
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(0, icmp.Compare(iter->start_key(), test_case.start));
      EXPECT_EQ(0, icmp.Compare(iter->end_key(), test_case.end));
      EXPECT_EQ(test_case.seq, iter->seq());
    }
  }
}

void VerifyShouldDelete(RangeDelAggregator* range_del_agg,
                        const std::vector<ShouldDeleteTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    EXPECT_EQ(
        test_case.result,
        range_del_agg->ShouldDelete(
            test_case.lookup_key, RangeDelPositioningMode::kForwardTraversal));
  }
  for (auto it = test_cases.rbegin(); it != test_cases.rend(); ++it) {
    const auto& test_case = *it;
    EXPECT_EQ(
        test_case.result,
        range_del_agg->ShouldDelete(
            test_case.lookup_key, RangeDelPositioningMode::kBackwardTraversal));
  }
}

void VerifyIsRangeOverlapped(
    ReadRangeDelAggregator* range_del_agg,
    const std::vector<IsRangeOverlappedTestCase>& test_cases) {
  for (const auto& test_case : test_cases) {
    EXPECT_EQ(test_case.result,
              range_del_agg->IsRangeOverlapped(test_case.start, test_case.end,
                                               /*end_exclusive=*/false));
  }
}

void CheckIterPosition(const RangeTombstone& tombstone,
                       const FragmentedRangeTombstoneIterator* iter) {
  // Test InternalIterator interface.
  EXPECT_EQ(tombstone.start_key_, ExtractUserKey(iter->key()));
  EXPECT_EQ(tombstone.end_key_, iter->value());
  EXPECT_EQ(tombstone.seq_, iter->seq());

  // Test FragmentedRangeTombstoneIterator interface.
  EXPECT_EQ(tombstone.start_key_, iter->start_key());
  EXPECT_EQ(tombstone.end_key_, iter->end_key());
  EXPECT_EQ(tombstone.seq_, GetInternalKeySeqno(iter->key()));
}

void VerifyFragmentedRangeDels(
    FragmentedRangeTombstoneIterator* iter,
    const std::vector<RangeTombstone>& expected_tombstones) {
  iter->SeekToFirst();
  for (size_t i = 0; i < expected_tombstones.size(); i++, iter->Next()) {
    ASSERT_TRUE(iter->Valid());
    CheckIterPosition(expected_tombstones[i], iter);
  }
  EXPECT_FALSE(iter->Valid());
}

}  // anonymous namespace

TEST_F(RangeDelAggregatorTest, EmptyTruncatedIter) {
  auto range_del_iter = MakeRangeDelIter({});
  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, bytewise_icmp,
                                           kMaxSequenceNumber));

  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp, nullptr,
                                 nullptr);

  iter.SeekToFirst();
  ASSERT_FALSE(iter.Valid());

  iter.SeekToLast();
  ASSERT_FALSE(iter.Valid());
}

TEST_F(RangeDelAggregatorTest, UntruncatedIter) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"e", "g", 8}, {"j", "n", 4}});
  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, bytewise_icmp,
                                           kMaxSequenceNumber));

  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp, nullptr,
                                 nullptr);

  VerifyIterator(
      &iter, bytewise_icmp,
      {{InternalValue("a", 10, kTypeRangeDeletion), UncutEndpoint("e"), 10},
       {InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {InternalValue("j", 4, kTypeRangeDeletion), UncutEndpoint("n"), 4}});

  VerifySeek(
      &iter, bytewise_icmp,
      {{"d", InternalValue("a", 10, kTypeRangeDeletion), UncutEndpoint("e"),
        10},
       {"e", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"ia", InternalValue("j", 4, kTypeRangeDeletion), UncutEndpoint("n"), 4},
       {"n", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */},
       {"", InternalValue("a", 10, kTypeRangeDeletion), UncutEndpoint("e"),
        10}});

  VerifySeekForPrev(
      &iter, bytewise_icmp,
      {{"d", InternalValue("a", 10, kTypeRangeDeletion), UncutEndpoint("e"),
        10},
       {"e", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"ia", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"n", InternalValue("j", 4, kTypeRangeDeletion), UncutEndpoint("n"), 4},
       {"", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */}});
}

TEST_F(RangeDelAggregatorTest, UntruncatedIterWithSnapshot) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"e", "g", 8}, {"j", "n", 4}});
  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, bytewise_icmp,
                                           9 /* snapshot */));

  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp, nullptr,
                                 nullptr);

  VerifyIterator(
      &iter, bytewise_icmp,
      {{InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {InternalValue("j", 4, kTypeRangeDeletion), UncutEndpoint("n"), 4}});

  VerifySeek(
      &iter, bytewise_icmp,
      {{"d", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"e", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"ia", InternalValue("j", 4, kTypeRangeDeletion), UncutEndpoint("n"), 4},
       {"n", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */},
       {"", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8}});

  VerifySeekForPrev(
      &iter, bytewise_icmp,
      {{"d", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */},
       {"e", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"ia", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"n", InternalValue("j", 4, kTypeRangeDeletion), UncutEndpoint("n"), 4},
       {"", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */}});
}

TEST_F(RangeDelAggregatorTest, TruncatedIterPartiallyCutTombstones) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"e", "g", 8}, {"j", "n", 4}});
  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, bytewise_icmp,
                                           kMaxSequenceNumber));

  InternalKey smallest("d", 7, kTypeValue);
  InternalKey largest("m", 9, kTypeValue);
  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp,
                                 &smallest, &largest);

  VerifyIterator(
      &iter, bytewise_icmp,
      {{InternalValue("d", 7, kTypeMaxValid), UncutEndpoint("e"), 10},
       {InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {InternalValue("j", 4, kTypeRangeDeletion),
        InternalValue("m", 8, kTypeMaxValid), 4}});

  VerifySeek(
      &iter, bytewise_icmp,
      {{"d", InternalValue("d", 7, kTypeMaxValid), UncutEndpoint("e"), 10},
       {"e", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"ia", InternalValue("j", 4, kTypeRangeDeletion),
        InternalValue("m", 8, kTypeMaxValid), 4, false /* invalid */},
       {"n", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */},
       {"", InternalValue("d", 7, kTypeMaxValid), UncutEndpoint("e"), 10}});

  VerifySeekForPrev(
      &iter, bytewise_icmp,
      {{"d", InternalValue("d", 7, kTypeMaxValid), UncutEndpoint("e"), 10},
       {"e", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"ia", InternalValue("e", 8, kTypeRangeDeletion), UncutEndpoint("g"), 8},
       {"n", InternalValue("j", 4, kTypeRangeDeletion),
        InternalValue("m", 8, kTypeMaxValid), 4, false /* invalid */},
       {"", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */}});
}

TEST_F(RangeDelAggregatorTest, TruncatedIterFullyCutTombstones) {
  auto range_del_iter =
      MakeRangeDelIter({{"a", "e", 10}, {"e", "g", 8}, {"j", "n", 4}});
  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, bytewise_icmp,
                                           kMaxSequenceNumber));

  InternalKey smallest("f", 7, kTypeValue);
  InternalKey largest("i", 9, kTypeValue);
  TruncatedRangeDelIterator iter(std::move(input_iter), &bytewise_icmp,
                                 &smallest, &largest);

  VerifyIterator(
      &iter, bytewise_icmp,
      {{InternalValue("f", 7, kTypeMaxValid), UncutEndpoint("g"), 8}});

  VerifySeek(
      &iter, bytewise_icmp,
      {{"d", InternalValue("f", 7, kTypeMaxValid), UncutEndpoint("g"), 8},
       {"f", InternalValue("f", 7, kTypeMaxValid), UncutEndpoint("g"), 8},
       {"j", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */}});

  VerifySeekForPrev(
      &iter, bytewise_icmp,
      {{"d", InternalValue("", 0, kTypeRangeDeletion), UncutEndpoint(""), 0,
        true /* invalid */},
       {"f", InternalValue("f", 7, kTypeMaxValid), UncutEndpoint("g"), 8},
       {"j", InternalValue("f", 7, kTypeMaxValid), UncutEndpoint("g"), 8}});
}

TEST_F(RangeDelAggregatorTest, SingleIterInAggregator) {
  auto range_del_iter = MakeRangeDelIter({{"a", "e", 10}, {"c", "g", 8}});
  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
      new FragmentedRangeTombstoneIterator(&fragment_list, bytewise_icmp,
                                           kMaxSequenceNumber));

  ReadRangeDelAggregator range_del_agg(&bytewise_icmp, kMaxSequenceNumber);
  range_del_agg.AddTombstones(std::move(input_iter));

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 19), false},
                                      {InternalValue("b", 9), true},
                                      {InternalValue("d", 9), true},
                                      {InternalValue("e", 7), true},
                                      {InternalValue("g", 7), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "c", true},
                                           {"d", "f", true},
                                           {"g", "l", false}});
  EXPECT_FALSE(range_del_agg.IsRangeOverlapped("_", "a", true));
}

TEST_F(RangeDelAggregatorTest, IsRangeOverlappedBeforeFirstTombstone) {
  auto range_del_iter = MakeRangeDelIter({{"c", "g", 10}, {"e", "g", 8}});
  FragmentedRangeTombstoneList fragment_list(std::move(range_del_iter),
                                             bytewise_icmp);
  ReadRangeDelAggregator range_del_agg(&bytewise_icmp, kMaxSequenceNumber);
  range_del_agg.AddTombstones(
      std::make_unique<FragmentedRangeTombstoneIterator>(
          &fragment_list, bytewise_icmp, kMaxSequenceNumber));

  // The tombstones are fragmented into [c, e) and [e, g). A query ending
  // exclusively at e must still find the first fragment.
  const struct {
    Slice start;
    Slice end;
    bool inclusive_overlap;
    bool exclusive_overlap;
  } test_cases[] = {{"a", "b", false, false}, {"a", "c", true, false},
                    {"a", "d", true, true},   {"a", "e", true, true},
                    {"a", "f", true, true},   {"a", "g", true, true},
                    {"c", "e", true, true},   {"e", "g", true, true},
                    {"g", "h", false, false}};
  for (const auto& test_case : test_cases) {
    SCOPED_TRACE(test_case.start.ToString() + ", " + test_case.end.ToString());
    EXPECT_EQ(test_case.inclusive_overlap,
              range_del_agg.IsRangeOverlapped(test_case.start, test_case.end,
                                              /*end_exclusive=*/false));
    EXPECT_EQ(test_case.exclusive_overlap,
              range_del_agg.IsRangeOverlapped(test_case.start, test_case.end,
                                              /*end_exclusive=*/true));
  }
}

TEST_F(RangeDelAggregatorTest, MultipleItersInAggregator) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  ReadRangeDelAggregator range_del_agg(&bytewise_icmp, kMaxSequenceNumber);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             kMaxSequenceNumber));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 19), true},
                                      {InternalValue("b", 19), false},
                                      {InternalValue("b", 9), true},
                                      {InternalValue("d", 9), true},
                                      {InternalValue("e", 7), true},
                                      {InternalValue("g", 7), false},
                                      {InternalValue("h", 24), true},
                                      {InternalValue("i", 24), false},
                                      {InternalValue("ii", 14), true},
                                      {InternalValue("j", 14), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "c", true},
                                           {"d", "f", true},
                                           {"g", "l", true},
                                           {"x", "y", false}});
}

TEST_F(RangeDelAggregatorTest, MultipleItersInAggregatorWithUpperBound) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  ReadRangeDelAggregator range_del_agg(&bytewise_icmp, 19);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             19 /* snapshot */));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 19), false},
                                      {InternalValue("a", 9), true},
                                      {InternalValue("b", 9), true},
                                      {InternalValue("d", 9), true},
                                      {InternalValue("e", 7), true},
                                      {InternalValue("g", 7), false},
                                      {InternalValue("h", 24), false},
                                      {InternalValue("i", 24), false},
                                      {InternalValue("ii", 14), true},
                                      {InternalValue("j", 14), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "c", true},
                                           {"d", "f", true},
                                           {"g", "l", true},
                                           {"x", "y", false}});
}

TEST_F(RangeDelAggregatorTest, MultipleTruncatedItersInAggregator) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "z", 10}}, {{"a", "z", 10}}, {{"a", "z", 10}}});
  std::vector<std::pair<InternalKey, InternalKey>> iter_bounds = {
      {InternalKey("a", 4, kTypeValue),
       InternalKey("m", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("m", 20, kTypeValue),
       InternalKey("x", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("x", 5, kTypeValue), InternalKey("zz", 30, kTypeValue)}};

  ReadRangeDelAggregator range_del_agg(&bytewise_icmp, 19);
  for (size_t i = 0; i < fragment_lists.size(); i++) {
    const auto& fragment_list = fragment_lists[i];
    const auto& bounds = iter_bounds[i];
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             19 /* snapshot */));
    range_del_agg.AddTombstones(std::move(input_iter), &bounds.first,
                                &bounds.second);
  }

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 10), false},
                                      {InternalValue("a", 9), false},
                                      {InternalValue("a", 4), true},
                                      {InternalValue("m", 10), false},
                                      {InternalValue("m", 9), true},
                                      {InternalValue("x", 10), false},
                                      {InternalValue("x", 9), false},
                                      {InternalValue("x", 5), true},
                                      {InternalValue("z", 9), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "n", true},
                                           {"l", "x", true},
                                           {"w", "z", true},
                                           {"zzz", "zz", false},
                                           {"zz", "zzz", false}});
}

TEST_F(RangeDelAggregatorTest, MultipleTruncatedItersInAggregatorSameLevel) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "z", 10}}, {{"a", "z", 10}}, {{"a", "z", 10}}});
  std::vector<std::pair<InternalKey, InternalKey>> iter_bounds = {
      {InternalKey("a", 4, kTypeValue),
       InternalKey("m", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("m", 20, kTypeValue),
       InternalKey("x", kMaxSequenceNumber, kTypeRangeDeletion)},
      {InternalKey("x", 5, kTypeValue), InternalKey("zz", 30, kTypeValue)}};

  ReadRangeDelAggregator range_del_agg(&bytewise_icmp, 19);

  auto add_iter_to_agg = [&](size_t i) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_lists[i].get(),
                                             bytewise_icmp, 19 /* snapshot */));
    range_del_agg.AddTombstones(std::move(input_iter), &iter_bounds[i].first,
                                &iter_bounds[i].second);
  };

  add_iter_to_agg(0);
  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 10), false},
                                      {InternalValue("a", 9), false},
                                      {InternalValue("a", 4), true}});

  add_iter_to_agg(1);
  VerifyShouldDelete(&range_del_agg, {{InternalValue("m", 10), false},
                                      {InternalValue("m", 9), true}});

  add_iter_to_agg(2);
  VerifyShouldDelete(&range_del_agg, {{InternalValue("x", 10), false},
                                      {InternalValue("x", 9), false},
                                      {InternalValue("x", 5), true},
                                      {InternalValue("z", 9), false}});

  VerifyIsRangeOverlapped(&range_del_agg, {{"", "_", false},
                                           {"_", "a", true},
                                           {"a", "n", true},
                                           {"l", "x", true},
                                           {"w", "z", true},
                                           {"zzz", "zz", false},
                                           {"zz", "zzz", false}});
}

TEST_F(RangeDelAggregatorTest, CompactionAggregatorNoSnapshots) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  std::vector<SequenceNumber> snapshots;
  CompactionRangeDelAggregator range_del_agg(&bytewise_icmp, snapshots);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             kMaxSequenceNumber));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  VerifyShouldDelete(&range_del_agg, {{InternalValue("a", 19), true},
                                      {InternalValue("b", 19), false},
                                      {InternalValue("b", 9), true},
                                      {InternalValue("d", 9), true},
                                      {InternalValue("e", 7), true},
                                      {InternalValue("g", 7), false},
                                      {InternalValue("h", 24), true},
                                      {InternalValue("i", 24), false},
                                      {InternalValue("ii", 14), true},
                                      {InternalValue("j", 14), false}});

  auto range_del_compaction_iter = range_del_agg.NewIterator();
  VerifyFragmentedRangeDels(range_del_compaction_iter.get(), {{"a", "b", 20},
                                                              {"b", "c", 10},
                                                              {"c", "e", 10},
                                                              {"e", "g", 8},
                                                              {"h", "i", 25},
                                                              {"ii", "j", 15}});
}

TEST_F(RangeDelAggregatorTest, CompactionAggregatorWithSnapshots) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  std::vector<SequenceNumber> snapshots{9, 19};
  CompactionRangeDelAggregator range_del_agg(&bytewise_icmp, snapshots);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             kMaxSequenceNumber));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  VerifyShouldDelete(
      &range_del_agg,
      {
          {InternalValue("a", 19), false},  // [10, 19]
          {InternalValue("a", 9), false},   // [0, 9]
          {InternalValue("b", 9), false},   // [0, 9]
          {InternalValue("d", 9), false},   // [0, 9]
          {InternalValue("d", 7), true},    // [0, 9]
          {InternalValue("e", 7), true},    // [0, 9]
          {InternalValue("g", 7), false},   // [0, 9]
          {InternalValue("h", 24), true},   // [20, kMaxSequenceNumber]
          {InternalValue("i", 24), false},  // [20, kMaxSequenceNumber]
          {InternalValue("ii", 14), true},  // [10, 19]
          {InternalValue("j", 14), false}   // [10, 19]
      });

  auto range_del_compaction_iter = range_del_agg.NewIterator();
  VerifyFragmentedRangeDels(range_del_compaction_iter.get(), {{"a", "b", 20},
                                                              {"a", "b", 10},
                                                              {"b", "c", 10},
                                                              {"c", "e", 10},
                                                              {"c", "e", 8},
                                                              {"e", "g", 8},
                                                              {"h", "i", 25},
                                                              {"ii", "j", 15}});
}

TEST_F(RangeDelAggregatorTest, CompactionAggregatorEmptyIteratorLeft) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  std::vector<SequenceNumber> snapshots{9, 19};
  CompactionRangeDelAggregator range_del_agg(&bytewise_icmp, snapshots);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             kMaxSequenceNumber));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  Slice start("_");
  Slice end("__");
}

TEST_F(RangeDelAggregatorTest, CompactionAggregatorEmptyIteratorRight) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  std::vector<SequenceNumber> snapshots{9, 19};
  CompactionRangeDelAggregator range_del_agg(&bytewise_icmp, snapshots);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             kMaxSequenceNumber));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  InternalKey start_buf("p", 0, kTypeRangeDeletion);
  InternalKey end_buf("q", 0, kTypeRangeDeletion);
  Slice start = start_buf.Encode();
  Slice end = end_buf.Encode();
  auto range_del_compaction_iter = range_del_agg.NewIterator(&start, &end);
  VerifyFragmentedRangeDels(range_del_compaction_iter.get(), {});
}

TEST_F(RangeDelAggregatorTest, CompactionAggregatorBoundedIterator) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "e", 10}, {"c", "g", 8}},
       {{"a", "b", 20}, {"h", "i", 25}, {"ii", "j", 15}}});

  std::vector<SequenceNumber> snapshots{9, 19};
  CompactionRangeDelAggregator range_del_agg(&bytewise_icmp, snapshots);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             kMaxSequenceNumber));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  InternalKey start_buf("bb", 0, kTypeRangeDeletion);
  InternalKey end_buf("e", 9, kTypeRangeDeletion);
  Slice start = start_buf.Encode();
  Slice end = end_buf.Encode();
  auto range_del_compaction_iter = range_del_agg.NewIterator(&start, &end);
  VerifyFragmentedRangeDels(range_del_compaction_iter.get(),
                            {{"a", "c", 10}, {"c", "e", 10}, {"c", "e", 8}});
}

TEST_F(RangeDelAggregatorTest,
       CompactionAggregatorBoundedIteratorExtraFragments) {
  auto fragment_lists = MakeFragmentedTombstoneLists(
      {{{"a", "d", 10}, {"c", "g", 8}},
       {{"b", "c", 20}, {"d", "f", 30}, {"h", "i", 25}, {"ii", "j", 15}}});

  std::vector<SequenceNumber> snapshots{9, 19};
  CompactionRangeDelAggregator range_del_agg(&bytewise_icmp, snapshots);
  for (const auto& fragment_list : fragment_lists) {
    std::unique_ptr<FragmentedRangeTombstoneIterator> input_iter(
        new FragmentedRangeTombstoneIterator(fragment_list.get(), bytewise_icmp,
                                             kMaxSequenceNumber));
    range_del_agg.AddTombstones(std::move(input_iter));
  }

  InternalKey start_buf("bb", 0, kTypeRangeDeletion);
  InternalKey end_buf("e", 0, kTypeRangeDeletion);
  Slice start = start_buf.Encode();
  Slice end = end_buf.Encode();
  auto range_del_compaction_iter = range_del_agg.NewIterator(&start, &end);
  VerifyFragmentedRangeDels(range_del_compaction_iter.get(), {{"a", "b", 10},
                                                              {"b", "c", 20},
                                                              {"b", "c", 10},
                                                              {"c", "d", 10},
                                                              {"c", "d", 8},
                                                              {"d", "f", 30},
                                                              {"d", "f", 8},
                                                              {"f", "g", 8}});
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
