//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/compaction/compaction_iterator.h"

#include <map>
#include <string>
#include <vector>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_file_builder.h"
#include "db/blob/blob_index.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"
#include "util/vector_iterator.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
std::string ValueWithPreferredSeqno(std::string val,
                                    SequenceNumber preferred_seqno = 0) {
  std::string result = val;
  PutFixed64(&result, preferred_seqno);
  return result;
}
}  // namespace

// Expects no merging attempts.
class NoMergingMergeOp : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* /*merge_out*/) const override {
    ADD_FAILURE();
    return false;
  }
  bool PartialMergeMulti(const Slice& /*key*/,
                         const std::deque<Slice>& /*operand_list*/,
                         std::string* /*new_value*/,
                         Logger* /*logger*/) const override {
    ADD_FAILURE();
    return false;
  }
  const char* Name() const override {
    return "CompactionIteratorTest NoMergingMergeOp";
  }
};

// Compaction filter that gets stuck when it sees a particular key,
// then gets unstuck when told to.
// Always returns Decision::kRemove.
class StallingFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& key, ValueType /*type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    int k = std::atoi(key.ToString().c_str());
    last_seen.store(k);
    while (k >= stall_at.load()) {
      std::this_thread::yield();
    }
    return Decision::kRemove;
  }

  const char* Name() const override {
    return "CompactionIteratorTest StallingFilter";
  }

  // Wait until the filter sees a key >= k and stalls at that key.
  // If `exact`, asserts that the seen key is equal to k.
  void WaitForStall(int k, bool exact = true) {
    stall_at.store(k);
    while (last_seen.load() < k) {
      std::this_thread::yield();
    }
    if (exact) {
      EXPECT_EQ(k, last_seen.load());
    }
  }

  // Filter will stall on key >= stall_at. Advance stall_at to unstall.
  mutable std::atomic<int> stall_at{0};
  // Last key the filter was called with.
  mutable std::atomic<int> last_seen{0};
};

// Compaction filter that filter out all keys.
class FilterAllKeysCompactionFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& /*key*/, ValueType /*type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    return Decision::kRemove;
  }

  const char* Name() const override { return "AllKeysCompactionFilter"; }
};

class LoggingForwardVectorIterator : public VectorIterator {
 public:
  struct Action {
    enum class Type {
      SEEK_TO_FIRST,
      SEEK,
      NEXT,
    };

    Type type;
    std::string arg;

    explicit Action(Type _type, std::string _arg = "")
        : type(_type), arg(_arg) {}

    bool operator==(const Action& rhs) const {
      return std::tie(type, arg) == std::tie(rhs.type, rhs.arg);
    }
  };

  LoggingForwardVectorIterator(const std::vector<std::string>& keys,
                               const std::vector<std::string>& values)
      : VectorIterator(keys, values) {
    current_ = keys_.size();
  }

  void SeekToFirst() override {
    log.emplace_back(Action::Type::SEEK_TO_FIRST);
    VectorIterator::SeekToFirst();
  }
  void SeekToLast() override { assert(false); }

  void Seek(const Slice& target) override {
    log.emplace_back(Action::Type::SEEK, target.ToString());
    VectorIterator::Seek(target);
  }

  void SeekForPrev(const Slice& /*target*/) override { assert(false); }

  void Next() override {
    assert(Valid());
    log.emplace_back(Action::Type::NEXT);
    VectorIterator::Next();
  }
  void Prev() override { assert(false); }

  Slice key() const override {
    assert(Valid());
    return VectorIterator::key();
  }
  Slice value() const override {
    assert(Valid());
    return VectorIterator::value();
  }

  std::vector<Action> log;
};

class FakeCompaction : public CompactionIterator::CompactionProxy {
 public:
  int level() const override { return 0; }

  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return is_bottommost_level || key_not_exists_beyond_output_level;
  }

  bool bottommost_level() const override { return is_bottommost_level; }

  int number_levels() const override { return 1; }

  Slice GetLargestUserKey() const override {
    return "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
  }

  bool allow_ingest_behind() const override { return is_allow_ingest_behind; }

  bool allow_mmap_reads() const override { return false; }

  bool enable_blob_garbage_collection() const override { return false; }

  double blob_garbage_collection_age_cutoff() const override { return 0.0; }

  uint64_t blob_compaction_readahead_size() const override { return 0; }

  const Version* input_version() const override { return nullptr; }

  bool DoesInputReferenceBlobFiles() const override { return false; }

  const Compaction* real_compaction() const override { return nullptr; }

  bool SupportsPerKeyPlacement() const override {
    return supports_per_key_placement;
  }

  bool key_not_exists_beyond_output_level = false;

  bool is_bottommost_level = false;

  bool is_allow_ingest_behind = false;

  bool supports_per_key_placement = false;
};

// A simplified snapshot checker which assumes each snapshot has a global
// last visible sequence.
class TestSnapshotChecker : public SnapshotChecker {
 public:
  explicit TestSnapshotChecker(
      SequenceNumber last_committed_sequence,
      const std::unordered_map<SequenceNumber, SequenceNumber>& snapshots =
          {{}})
      : last_committed_sequence_(last_committed_sequence),
        snapshots_(snapshots) {}

  SnapshotCheckerResult CheckInSnapshot(
      SequenceNumber seq, SequenceNumber snapshot_seq) const override {
    if (snapshot_seq == kMaxSequenceNumber) {
      return seq <= last_committed_sequence_
                 ? SnapshotCheckerResult::kInSnapshot
                 : SnapshotCheckerResult::kNotInSnapshot;
    }
    assert(snapshots_.count(snapshot_seq) > 0);
    return seq <= snapshots_.at(snapshot_seq)
               ? SnapshotCheckerResult::kInSnapshot
               : SnapshotCheckerResult::kNotInSnapshot;
  }

 private:
  SequenceNumber last_committed_sequence_;
  // A map of valid snapshot to last visible sequence to the snapshot.
  std::unordered_map<SequenceNumber, SequenceNumber> snapshots_;
};

// Test param:
//   bool: whether to pass snapshot_checker to compaction iterator.
class CompactionIteratorTest : public testing::TestWithParam<bool> {
 public:
  CompactionIteratorTest()
      : cmp_(BytewiseComparator()), icmp_(cmp_), snapshots_({}) {}

  explicit CompactionIteratorTest(const Comparator* ucmp)
      : cmp_(ucmp), icmp_(cmp_), snapshots_({}) {}

  void InitIterators(
      const std::vector<std::string>& ks, const std::vector<std::string>& vs,
      const std::vector<std::string>& range_del_ks,
      const std::vector<std::string>& range_del_vs,
      SequenceNumber last_sequence,
      SequenceNumber last_committed_sequence = kMaxSequenceNumber,
      MergeOperator* merge_op = nullptr, CompactionFilter* filter = nullptr,
      bool bottommost_level = false,
      SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber,
      bool key_not_exists_beyond_output_level = false,
      const std::string* full_history_ts_low = nullptr) {
    std::unique_ptr<InternalIterator> unfragmented_range_del_iter(
        new VectorIterator(range_del_ks, range_del_vs, &icmp_));
    auto tombstone_list = std::make_shared<FragmentedRangeTombstoneList>(
        std::move(unfragmented_range_del_iter), icmp_);
    std::unique_ptr<FragmentedRangeTombstoneIterator> range_del_iter(
        new FragmentedRangeTombstoneIterator(tombstone_list, icmp_,
                                             kMaxSequenceNumber));
    range_del_agg_.reset(new CompactionRangeDelAggregator(&icmp_, snapshots_));
    range_del_agg_->AddTombstones(std::move(range_del_iter));

    std::unique_ptr<CompactionIterator::CompactionProxy> compaction;
    if (filter || bottommost_level || key_not_exists_beyond_output_level) {
      compaction_proxy_ = new FakeCompaction();
      compaction_proxy_->is_bottommost_level = bottommost_level;
      compaction_proxy_->is_allow_ingest_behind = AllowIngestBehind();
      compaction_proxy_->key_not_exists_beyond_output_level =
          key_not_exists_beyond_output_level;
      compaction_proxy_->supports_per_key_placement = SupportsPerKeyPlacement();
      compaction.reset(compaction_proxy_);
    }
    bool use_snapshot_checker = UseSnapshotChecker() || GetParam();
    if (use_snapshot_checker || last_committed_sequence < kMaxSequenceNumber) {
      snapshot_checker_.reset(
          new TestSnapshotChecker(last_committed_sequence, snapshot_map_));
    }
    merge_helper_.reset(
        new MergeHelper(Env::Default(), cmp_, merge_op, filter, nullptr, false,
                        0 /*latest_snapshot*/, snapshot_checker_.get(),
                        0 /*level*/, nullptr /*statistics*/, &shutting_down_));

    if (c_iter_) {
      // Since iter_ is still used in ~CompactionIterator(), we call
      // ~CompactionIterator() first.
      c_iter_.reset();
    }
    iter_.reset(new LoggingForwardVectorIterator(ks, vs));
    iter_->SeekToFirst();
    c_iter_.reset(new CompactionIterator(
        iter_.get(), cmp_, merge_helper_.get(), last_sequence, &snapshots_,
        snapshots_.empty() ? kMaxSequenceNumber : snapshots_.at(0),
        earliest_write_conflict_snapshot, kMaxSequenceNumber,
        snapshot_checker_.get(), Env::Default(),
        false /* report_detailed_time */, range_del_agg_.get(),
        nullptr /* blob_file_builder */, true /*allow_data_in_errors*/,
        true /*enforce_single_del_contracts*/,
        /*manual_compaction_canceled=*/kManualCompactionCanceledFalse_,
        std::move(compaction), /*must_count_input_entries=*/false, filter,
        &shutting_down_, /*info_log=*/nullptr, full_history_ts_low));
  }

  void AddSnapshot(SequenceNumber snapshot,
                   SequenceNumber last_visible_seq = kMaxSequenceNumber) {
    snapshots_.push_back(snapshot);
    snapshot_map_[snapshot] = last_visible_seq;
  }

  virtual bool UseSnapshotChecker() const { return false; }

  virtual bool AllowIngestBehind() const { return false; }

  virtual bool SupportsPerKeyPlacement() const { return false; }

  void RunTest(
      const std::vector<std::string>& input_keys,
      const std::vector<std::string>& input_values,
      const std::vector<std::string>& expected_keys,
      const std::vector<std::string>& expected_values,
      SequenceNumber last_committed_seq = kMaxSequenceNumber,
      MergeOperator* merge_operator = nullptr,
      CompactionFilter* compaction_filter = nullptr,
      bool bottommost_level = false,
      SequenceNumber earliest_write_conflict_snapshot = kMaxSequenceNumber,
      bool key_not_exists_beyond_output_level = false,
      const std::string* full_history_ts_low = nullptr) {
    InitIterators(input_keys, input_values, {}, {}, kMaxSequenceNumber,
                  last_committed_seq, merge_operator, compaction_filter,
                  bottommost_level, earliest_write_conflict_snapshot,
                  key_not_exists_beyond_output_level, full_history_ts_low);
    c_iter_->SeekToFirst();
    for (size_t i = 0; i < expected_keys.size(); i++) {
      std::string info = "i = " + std::to_string(i);
      ASSERT_TRUE(c_iter_->Valid()) << info;
      ASSERT_OK(c_iter_->status()) << info;
      ASSERT_EQ(expected_keys[i], c_iter_->key().ToString()) << info;
      ASSERT_EQ(expected_values[i], c_iter_->value().ToString()) << info;
      c_iter_->Next();
    }
    ASSERT_OK(c_iter_->status());
    ASSERT_FALSE(c_iter_->Valid());
  }

  void ClearSnapshots() {
    snapshots_.clear();
    snapshot_map_.clear();
  }

  const Comparator* cmp_;
  const InternalKeyComparator icmp_;
  std::vector<SequenceNumber> snapshots_;
  // A map of valid snapshot to last visible sequence to the snapshot.
  std::unordered_map<SequenceNumber, SequenceNumber> snapshot_map_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::unique_ptr<LoggingForwardVectorIterator> iter_;
  std::unique_ptr<CompactionIterator> c_iter_;
  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg_;
  std::unique_ptr<SnapshotChecker> snapshot_checker_;
  std::atomic<bool> shutting_down_{false};
  const std::atomic<bool> kManualCompactionCanceledFalse_{false};
  FakeCompaction* compaction_proxy_;
};

// It is possible that the output of the compaction iterator is empty even if
// the input is not.
TEST_P(CompactionIteratorTest, EmptyResult) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValue)},
                {"", "val"}, {}, {}, 5);
  c_iter_->SeekToFirst();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

// If there is a corruption after a single deletion, the compaction should fail.
TEST_P(CompactionIteratorTest, CorruptionAfterSingleDeletion) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValue, true),
                 test::KeyStr("b", 10, kTypeValue)},
                {"", "val", "val2"}, {}, {}, 10);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 5, kTypeSingleDeletion),
            c_iter_->key().ToString());
  c_iter_->Next();
  // The iterator should now fail when encountering the corrupted key
  ASSERT_FALSE(c_iter_->Valid());
  ASSERT_FALSE(c_iter_->status().ok());
  ASSERT_TRUE(c_iter_->status().IsCorruption());
}

// Tests compatibility of TimedPut and SingleDelete. TimedPut should act as if
// it's a Put.
TEST_P(CompactionIteratorTest, TimedPutAndSingleDelete) {
  InitIterators({test::KeyStr("a", 5, kTypeSingleDeletion),
                 test::KeyStr("a", 3, kTypeValuePreferredSeqno)},
                {"", "val"}, {}, {}, 5);
  c_iter_->SeekToFirst();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, SimpleRangeDeletion) {
  InitIterators({test::KeyStr("morning", 5, kTypeValue),
                 test::KeyStr("morning", 2, kTypeValue),
                 test::KeyStr("night", 3, kTypeValue)},
                {"zao", "zao", "wan"},
                {test::KeyStr("ma", 4, kTypeRangeDeletion)}, {"mz"}, 5);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 3, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, TracksConsumedEarlierEntriesUntilNextUserKey) {
  AddSnapshot(4);
  InitIterators(
      {test::KeyStr("a", 6, kTypeValue), test::KeyStr("a", 5, kTypeValue),
       test::KeyStr("a", 3, kTypeValue), test::KeyStr("b", 2, kTypeValue)},
      {"a6", "a5", "a3", "b2"}, {}, {}, 6);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 6, kTypeValue), c_iter_->key().ToString());
  EXPECT_FALSE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());

  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 3, kTypeValue), c_iter_->key().ToString());
  EXPECT_TRUE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());

  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("b", 2, kTypeValue), c_iter_->key().ToString());
  EXPECT_FALSE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());
}

TEST_P(CompactionIteratorTest, TracksRangeDeletedEarlierEntry) {
  AddSnapshot(10);
  InitIterators({test::KeyStr("morning", 15, kTypeValue),
                 test::KeyStr("morning", 5, kTypeValue),
                 test::KeyStr("night", 40, kTypeValue)},
                {"morning15", "morning5", "night40"},
                {test::KeyStr("ma", 28, kTypeRangeDeletion)}, {"mz"}, 40);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValue), c_iter_->key().ToString());
  EXPECT_TRUE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());

  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 40, kTypeValue), c_iter_->key().ToString());
  EXPECT_FALSE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());
}

TEST_P(CompactionIteratorTest, RangeDeletionWithSnapshots) {
  AddSnapshot(10);
  std::vector<std::string> ks1;
  ks1.push_back(test::KeyStr("ma", 28, kTypeRangeDeletion));
  std::vector<std::string> vs1{"mz"};
  std::vector<std::string> ks2{test::KeyStr("morning", 15, kTypeValue),
                               test::KeyStr("morning", 5, kTypeValue),
                               test::KeyStr("night", 40, kTypeValue),
                               test::KeyStr("night", 20, kTypeValue)};
  std::vector<std::string> vs2{"zao 15", "zao 5", "wan 40", "wan 20"};
  InitIterators(ks2, vs2, ks1, vs1, 40);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 40, kTypeValue), c_iter_->key().ToString());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

// Tests compatibility of TimedPut and Range delete. TimedPut should act as if
// it's a Put.
TEST_P(CompactionIteratorTest, TimedPutAndRangeDeletion) {
  InitIterators(
      {test::KeyStr("morning", 5, kTypeValuePreferredSeqno),
       test::KeyStr("morning", 2, kTypeValuePreferredSeqno),
       test::KeyStr("night", 3, kTypeValuePreferredSeqno)},
      {ValueWithPreferredSeqno("zao5"), ValueWithPreferredSeqno("zao2"),
       ValueWithPreferredSeqno("wan")},
      {test::KeyStr("ma", 4, kTypeRangeDeletion)}, {"mz"}, 5);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValuePreferredSeqno),
            c_iter_->key().ToString());
  ASSERT_EQ(ValueWithPreferredSeqno("zao5"), c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 3, kTypeValuePreferredSeqno),
            c_iter_->key().ToString());
  ASSERT_EQ(ValueWithPreferredSeqno("wan"), c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());
}

TEST_P(CompactionIteratorTest, CompactionFilterSkipUntil) {
  class Filter : public CompactionFilter {
    Decision FilterV2(int /*level*/, const Slice& key, ValueType t,
                      const Slice& existing_value, std::string* /*new_value*/,
                      std::string* skip_until) const override {
      std::string k = key.ToString();
      std::string v = existing_value.ToString();
      // See InitIterators() call below for the sequence of keys and their
      // filtering decisions. Here we closely assert that compaction filter is
      // called with the expected keys and only them, and with the right values.
      if (k == "a") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("av50", v);
        return Decision::kKeep;
      }
      if (k == "b") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("bv60", v);
        *skip_until = "d+";
        return Decision::kRemoveAndSkipUntil;
      }
      if (k == "e") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("em71", v);
        return Decision::kKeep;
      }
      if (k == "f") {
        if (v == "fm65") {
          EXPECT_EQ(ValueType::kMergeOperand, t);
          *skip_until = "f";
        } else {
          EXPECT_EQ("fm30", v);
          EXPECT_EQ(ValueType::kMergeOperand, t);
          *skip_until = "g+";
        }
        return Decision::kRemoveAndSkipUntil;
      }
      if (k == "h") {
        EXPECT_EQ(ValueType::kValue, t);
        EXPECT_EQ("hv91", v);
        return Decision::kKeep;
      }
      if (k == "i") {
        EXPECT_EQ(ValueType::kMergeOperand, t);
        EXPECT_EQ("im95", v);
        *skip_until = "z";
        return Decision::kRemoveAndSkipUntil;
      }
      ADD_FAILURE();
      return Decision::kKeep;
    }

    const char* Name() const override {
      return "CompactionIteratorTest.CompactionFilterSkipUntil::Filter";
    }
  };

  NoMergingMergeOp merge_op;
  Filter filter;
  InitIterators(
      {test::KeyStr("a", 50, kTypeValue),  // keep
       test::KeyStr("a", 45, kTypeMerge),
       test::KeyStr("b", 60, kTypeValue),  // skip to "d+"
       test::KeyStr("b", 40, kTypeValue), test::KeyStr("c", 35, kTypeValue),
       test::KeyStr("d", 70, kTypeMerge),
       test::KeyStr("e", 71, kTypeMerge),  // keep
       test::KeyStr("f", 65, kTypeMerge),  // skip to "f", aka keep
       test::KeyStr("f", 30, kTypeMerge),  // skip to "g+"
       test::KeyStr("f", 25, kTypeValue), test::KeyStr("g", 90, kTypeValue),
       test::KeyStr("h", 91, kTypeValue),  // keep
       test::KeyStr("i", 95, kTypeMerge),  // skip to "z"
       test::KeyStr("j", 99, kTypeValue),
       test::KeyStr("k", 100, kTypeValuePreferredSeqno)},
      {"av50", "am45", "bv60", "bv40", "cv35", "dm70", "em71", "fm65", "fm30",
       "fv25", "gv90", "hv91", "im95", "jv99",
       ValueWithPreferredSeqno("kv100")},
      {}, {}, kMaxSequenceNumber, kMaxSequenceNumber, &merge_op, &filter);

  // Compaction should output just "a", "e" and "h" keys.
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("av50", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("e", 71, kTypeMerge), c_iter_->key().ToString());
  ASSERT_EQ("em71", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("h", 91, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("hv91", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_FALSE(c_iter_->Valid());

  // Check that the compaction iterator did the correct sequence of calls on
  // the underlying iterator.
  using A = LoggingForwardVectorIterator::Action;
  using T = A::Type;
  std::vector<A> expected_actions = {
      A(T::SEEK_TO_FIRST),
      A(T::NEXT),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("d+", kMaxSequenceNumber, kValueTypeForSeek)),
      A(T::NEXT),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("g+", kMaxSequenceNumber, kValueTypeForSeek)),
      A(T::NEXT),
      A(T::SEEK, test::KeyStr("z", kMaxSequenceNumber, kValueTypeForSeek))};
  ASSERT_EQ(expected_actions, iter_->log);
}

TEST_P(CompactionIteratorTest, ShuttingDownInFilter) {
  NoMergingMergeOp merge_op;
  StallingFilter filter;
  InitIterators(
      {test::KeyStr("1", 1, kTypeValue), test::KeyStr("2", 2, kTypeValue),
       test::KeyStr("3", 3, kTypeValue), test::KeyStr("4", 4, kTypeValue)},
      {"v1", "v2", "v3", "v4"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      &merge_op, &filter);
  // Don't leave tombstones (kTypeDeletion) for filtered keys.
  compaction_proxy_->key_not_exists_beyond_output_level = true;

  std::atomic<bool> seek_done{false};
  ROCKSDB_NAMESPACE::port::Thread compaction_thread([&] {
    c_iter_->SeekToFirst();
    EXPECT_FALSE(c_iter_->Valid());
    EXPECT_TRUE(c_iter_->status().IsShutdownInProgress());
    seek_done.store(true);
  });

  // Let key 1 through.
  filter.WaitForStall(1);

  // Shutdown during compaction filter call for key 2.
  filter.WaitForStall(2);
  shutting_down_.store(true);
  EXPECT_FALSE(seek_done.load());

  // Unstall filter and wait for SeekToFirst() to return.
  filter.stall_at.store(3);
  compaction_thread.join();
  assert(seek_done.load());

  // Check that filter was never called again.
  EXPECT_EQ(2, filter.last_seen.load());
}

// Same as ShuttingDownInFilter, but shutdown happens during filter call for
// a merge operand, not for a value.
TEST_P(CompactionIteratorTest, ShuttingDownInMerge) {
  NoMergingMergeOp merge_op;
  StallingFilter filter;
  InitIterators(
      {test::KeyStr("1", 1, kTypeValue), test::KeyStr("2", 2, kTypeMerge),
       test::KeyStr("3", 3, kTypeMerge), test::KeyStr("4", 4, kTypeValue)},
      {"v1", "v2", "v3", "v4"}, {}, {}, kMaxSequenceNumber, kMaxSequenceNumber,
      &merge_op, &filter);
  compaction_proxy_->key_not_exists_beyond_output_level = true;

  std::atomic<bool> seek_done{false};
  ROCKSDB_NAMESPACE::port::Thread compaction_thread([&] {
    c_iter_->SeekToFirst();
    ASSERT_FALSE(c_iter_->Valid());
    ASSERT_TRUE(c_iter_->status().IsShutdownInProgress());
    seek_done.store(true);
  });

  // Let key 1 through.
  filter.WaitForStall(1);

  // Shutdown during compaction filter call for key 2.
  filter.WaitForStall(2);
  shutting_down_.store(true);
  EXPECT_FALSE(seek_done.load());

  // Unstall filter and wait for SeekToFirst() to return.
  filter.stall_at.store(3);
  compaction_thread.join();
  assert(seek_done.load());

  // Check that filter was never called again.
  EXPECT_EQ(2, filter.last_seen.load());
}

class Filter : public CompactionFilter {
  Decision FilterV2(int /*level*/, const Slice& key, ValueType t,
                    const Slice& existing_value, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    std::string k = key.ToString();
    std::string v = existing_value.ToString();

    // See InitIterators() call below for the sequence of keys and their
    // filtering decisions. Here we closely assert that compaction filter is
    // called with the expected keys and only them, and with the right values.
    if (k == "a") {
      EXPECT_EQ(ValueType::kMergeOperand, t);
      EXPECT_EQ("av1", v);
      return Decision::kKeep;
    } else if (k == "b") {
      EXPECT_EQ(ValueType::kMergeOperand, t);
      return Decision::kKeep;
    } else if (k == "c") {
      return Decision::kKeep;
    }

    ADD_FAILURE();
    return Decision::kKeep;
  }

  const char* Name() const override {
    return "CompactionIteratorTest.SingleMergeOperand::Filter";
  }
};

class SingleMergeOp : public MergeOperator {
 public:
  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    // See InitIterators() call below for why "c" is the only key for which
    // FullMergeV2 should be called.
    EXPECT_EQ("c", merge_in.key.ToString());

    std::string temp_value;
    if (merge_in.existing_value != nullptr) {
      temp_value = merge_in.existing_value->ToString();
    }

    for (auto& operand : merge_in.operand_list) {
      temp_value.append(operand.ToString());
    }
    merge_out->new_value = temp_value;

    return true;
  }

  bool PartialMergeMulti(const Slice& key,
                         const std::deque<Slice>& operand_list,
                         std::string* new_value,
                         Logger* /*logger*/) const override {
    std::string string_key = key.ToString();
    EXPECT_TRUE(string_key == "a" || string_key == "b");

    if (string_key == "a") {
      EXPECT_EQ(1, operand_list.size());
    } else if (string_key == "b") {
      EXPECT_EQ(2, operand_list.size());
    }

    std::string temp_value;
    for (auto& operand : operand_list) {
      temp_value.append(operand.ToString());
    }
    swap(temp_value, *new_value);

    return true;
  }

  const char* Name() const override {
    return "CompactionIteratorTest SingleMergeOp";
  }

  bool AllowSingleOperand() const override { return true; }
};

TEST_P(CompactionIteratorTest, SingleMergeOperand) {
  SingleMergeOp merge_op;
  Filter filter;
  InitIterators(
      // a should invoke PartialMergeMulti with a single merge operand.
      {test::KeyStr("a", 50, kTypeMerge),
       // b should invoke PartialMergeMulti with two operands.
       test::KeyStr("b", 70, kTypeMerge), test::KeyStr("b", 60, kTypeMerge),
       // c should invoke FullMerge due to kTypeValue at the beginning.
       test::KeyStr("c", 90, kTypeMerge), test::KeyStr("c", 80, kTypeValue)},
      {"av1", "bv2", "bv1", "cv2", "cv1"}, {}, {}, kMaxSequenceNumber,
      kMaxSequenceNumber, &merge_op, &filter);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeMerge), c_iter_->key().ToString());
  ASSERT_EQ("av1", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ("bv1bv2", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_OK(c_iter_->status());
  ASSERT_EQ("cv1cv2", c_iter_->value().ToString());
}

// Tests compatibility of TimedPut and Merge operation. When a TimedPut is
// merged with some merge operand in compaction, it will become a regular Put
// and lose its preferred sequence number.
TEST_P(CompactionIteratorTest, TimedPutAndMerge) {
  SingleMergeOp merge_op;
  Filter filter;
  InitIterators({test::KeyStr("c", 90, kTypeMerge),
                 test::KeyStr("c", 80, kTypeValuePreferredSeqno)},
                {"cv2", ValueWithPreferredSeqno("cv1")}, {}, {},
                kMaxSequenceNumber, kMaxSequenceNumber, &merge_op, &filter);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("c", 90, kTypeValue), c_iter_->key().ToString());
  ASSERT_OK(c_iter_->status());
  ASSERT_EQ("cv1cv2", c_iter_->value().ToString());
}

// In bottommost level, values earlier than earliest snapshot can be output
// with sequence = 0.
TEST_P(CompactionIteratorTest, ZeroOutSequenceAtBottomLevel) {
  AddSnapshot(1);
  RunTest({test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 2, kTypeValue)},
          {"v1", "v2"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 2, kTypeValue)},
          {"v1", "v2"}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

// In bottommost level, deletions earlier than earliest snapshot can be removed
// permanently.
TEST_P(CompactionIteratorTest, RemoveDeletionAtBottomLevel) {
  AddSnapshot(1);
  RunTest(
      {test::KeyStr("a", 1, kTypeDeletion), test::KeyStr("b", 3, kTypeDeletion),
       test::KeyStr("b", 1, kTypeValue)},
      {"", "", ""},
      {test::KeyStr("b", 3, kTypeDeletion), test::KeyStr("b", 0, kTypeValue)},
      {"", ""}, kMaxSequenceNumber /*last_committed_seq*/,
      nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
      true /*bottommost_level*/);
}

// In bottommost level, single deletions earlier than earliest snapshot can be
// removed permanently.
TEST_P(CompactionIteratorTest, RemoveSingleDeletionAtBottomLevel) {
  AddSnapshot(1);
  RunTest({test::KeyStr("a", 1, kTypeSingleDeletion),
           test::KeyStr("b", 2, kTypeSingleDeletion)},
          {"", ""}, {test::KeyStr("b", 2, kTypeSingleDeletion)}, {""},
          kMaxSequenceNumber /*last_committed_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, true /*bottommost_level*/);
}

TEST_P(CompactionIteratorTest, ConvertToPutAtBottom) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  RunTest({test::KeyStr("a", 4, kTypeMerge), test::KeyStr("a", 3, kTypeMerge),
           test::KeyStr("a", 2, kTypeMerge), test::KeyStr("b", 1, kTypeValue)},
          {"a4", "a3", "a2", "b1"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 0, kTypeValue)},
          {"a2,a3,a4", "b1"}, kMaxSequenceNumber /*last_committed_seq*/,
          merge_op.get(), nullptr /*compaction_filter*/,
          true /*bottomost_level*/);
}

TEST_P(CompactionIteratorTest, TracksConsumedMergeThroughBaseValue) {
  class MustMergeOperator : public MergeOperator {
   public:
    bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                     MergeOperationOutput* merge_out) const override {
      merge_out->op_failure_scope = OpFailureScope::kMustMerge;
      return false;
    }

    const char* Name() const override { return "MustMergeOperator"; }
  } merge_operator;

  InitIterators(
      {test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 2, kTypeValue),
       test::KeyStr("b", 1, kTypeValue)},
      {"a3", "a2", "b1"}, {}, {}, kMaxSequenceNumber,
      /*last_committed_sequence=*/kMaxSequenceNumber, &merge_operator);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 3, kTypeMerge), c_iter_->key().ToString());
  EXPECT_TRUE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());

  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 2, kTypeValue), c_iter_->key().ToString());
  EXPECT_TRUE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());

  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("b", 1, kTypeValue), c_iter_->key().ToString());
  EXPECT_FALSE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());
}

TEST_P(CompactionIteratorTest, ZeroSeqOfKeyAndSnapshot) {
  AddSnapshot(0);
  const std::vector<std::string> input_keys = {
      test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 0, kTypeValue)};
  const std::vector<std::string> input_values = {"a1", "b1"};
  RunTest(input_keys, input_values, input_keys, input_values);
}

INSTANTIATE_TEST_CASE_P(CompactionIteratorTestInstance, CompactionIteratorTest,
                        testing::Values(true, false));

// Tests how CompactionIterator work together with SnapshotChecker.
class CompactionIteratorWithSnapshotCheckerTest
    : public CompactionIteratorTest {
 public:
  bool UseSnapshotChecker() const override { return true; }
};

// Uncommitted keys (keys with seq > last_committed_seq) should be output as-is
// while committed version of these keys should get compacted as usual.

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Value) {
  RunTest(
      {test::KeyStr("foo", 3, kTypeValue), test::KeyStr("foo", 2, kTypeValue),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("foo", 3, kTypeValue), test::KeyStr("foo", 2, kTypeValue)},
      {"v3", "v2"}, 2 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Deletion) {
  RunTest({test::KeyStr("foo", 2, kTypeDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("foo", 2, kTypeDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"}, 1 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_Merge) {
  auto merge_op = MergeOperators::CreateStringAppendOperator();
  RunTest(
      {test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeMerge),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeValue)},
      {"v3", "v1,v2"}, 2 /*last_committed_seq*/, merge_op.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_SingleDelete) {
  RunTest({test::KeyStr("foo", 2, kTypeSingleDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("foo", 2, kTypeSingleDeletion),
           test::KeyStr("foo", 1, kTypeValue)},
          {"", "v1"}, 1 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       PreserveUncommittedKeys_BlobIndex) {
  RunTest({test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v3", "v2", "v1"},
          {test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex)},
          {"v3", "v2"}, 2 /*last_committed_seq*/);
}

// Test compaction iterator dedup keys visible to the same snapshot.

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Value) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 3, kTypeValue),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v3", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 3, kTypeValue),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v3", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_TimedPut) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("foo", 4, kTypeValuePreferredSeqno),
           test::KeyStr("foo", 3, kTypeValuePreferredSeqno),
           test::KeyStr("foo", 2, kTypeValuePreferredSeqno),
           test::KeyStr("foo", 1, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("v4"), ValueWithPreferredSeqno("v3"),
           ValueWithPreferredSeqno("v2"), ValueWithPreferredSeqno("v1")},
          {test::KeyStr("foo", 4, kTypeValuePreferredSeqno),
           test::KeyStr("foo", 3, kTypeValuePreferredSeqno),
           test::KeyStr("foo", 1, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("v4"), ValueWithPreferredSeqno("v3"),
           ValueWithPreferredSeqno("v1")},
          3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Deletion) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeDeletion),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeDeletion),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_Merge) {
  AddSnapshot(2, 1);
  AddSnapshot(4, 3);
  auto merge_op = MergeOperators::CreateStringAppendOperator();
  RunTest(
      {test::KeyStr("foo", 5, kTypeMerge), test::KeyStr("foo", 4, kTypeMerge),
       test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 2, kTypeMerge),
       test::KeyStr("foo", 1, kTypeValue)},
      {"v5", "v4", "v3", "v2", "v1"},
      {test::KeyStr("foo", 5, kTypeMerge), test::KeyStr("foo", 4, kTypeMerge),
       test::KeyStr("foo", 3, kTypeMerge), test::KeyStr("foo", 1, kTypeValue)},
      {"v5", "v4", "v2,v3", "v1"}, 4 /*last_committed_seq*/, merge_op.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       DedupSameSnapshot_SingleDeletion) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("foo", 4, kTypeValue),
       test::KeyStr("foo", 3, kTypeSingleDeletion),
       test::KeyStr("foo", 2, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "", "v2", "v1"},
      {test::KeyStr("foo", 4, kTypeValue), test::KeyStr("foo", 1, kTypeValue)},
      {"v4", "v1"}, 3 /*last_committed_seq*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, DedupSameSnapshot_BlobIndex) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("foo", 4, kTypeBlobIndex),
           test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 2, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v4", "v3", "v2", "v1"},
          {test::KeyStr("foo", 4, kTypeBlobIndex),
           test::KeyStr("foo", 3, kTypeBlobIndex),
           test::KeyStr("foo", 1, kTypeBlobIndex)},
          {"v4", "v3", "v1"}, 3 /*last_committed_seq*/);
}

// At bottom level, sequence numbers can be zero out, and deletions can be
// removed, but only when they are visible to earliest snapshot.

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotZeroOutSequenceIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 2, kTypeValue),
           test::KeyStr("c", 3, kTypeValue)},
          {"v1", "v2", "v3"},
          {test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 2, kTypeValue),
           test::KeyStr("c", 3, kTypeValue)},
          {"v1", "v2", "v3"}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveDeletionIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest(
      {test::KeyStr("a", 1, kTypeDeletion), test::KeyStr("b", 2, kTypeDeletion),
       test::KeyStr("c", 3, kTypeDeletion)},
      {"", "", ""}, {}, {"", ""}, kMaxSequenceNumber /*last_committed_seq*/,
      nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
      true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveDeletionIfValuePresentToEarlierSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 4, kTypeDeletion),
           test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 3, kTypeValue)},
          {"", "", ""},
          {test::KeyStr("a", 4, kTypeDeletion),
           test::KeyStr("a", 0, kTypeValue), test::KeyStr("b", 3, kTypeValue)},
          {"", "", ""}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       NotRemoveSingleDeletionIfNotVisibleToEarliestSnapshot) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 1, kTypeSingleDeletion),
           test::KeyStr("b", 2, kTypeSingleDeletion),
           test::KeyStr("c", 3, kTypeSingleDeletion)},
          {"", "", ""},
          {test::KeyStr("b", 2, kTypeSingleDeletion),
           test::KeyStr("c", 3, kTypeSingleDeletion)},
          {"", ""}, kMaxSequenceNumber /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/);
}

// Single delete should not cancel out values that not visible to the
// same set of snapshots
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       SingleDeleteAcrossSnapshotBoundary) {
  AddSnapshot(2, 1);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"}, 2 /*last_committed_seq*/);
}

// Single delete should be kept in case it is not visible to the
// earliest write conflict snapshot. If a single delete is kept for this reason,
// corresponding value can be trimmed to save space.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", "v1"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /*last_committed_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, false /*bottommost_level*/,
          2 /*earliest_write_conflict_snapshot*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       TracksConsumedSingleDeleteForClearedPut) {
  AddSnapshot(2, 0);
  InitIterators(
      {test::KeyStr("a", 2, kTypeSingleDeletion),
       test::KeyStr("a", 1, kTypeValue), test::KeyStr("b", 1, kTypeValue)},
      {"", "a1", "b1"}, {}, {}, kMaxSequenceNumber,
      /*last_committed_sequence=*/2, /*merge_op=*/nullptr,
      /*filter=*/nullptr, /*bottommost_level=*/false,
      /*earliest_write_conflict_snapshot=*/2);

  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 2, kTypeSingleDeletion),
            c_iter_->key().ToString());
  EXPECT_TRUE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());

  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 1, kTypeValue), c_iter_->key().ToString());
  EXPECT_FALSE(c_iter_->IsCurrentKeyAlreadyScanned());
  EXPECT_TRUE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());

  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("b", 1, kTypeValue), c_iter_->key().ToString());
  EXPECT_FALSE(c_iter_->HasConsumedEarlierEntriesOfCurrentKey());
}

// Same as above but with a blob index. In addition to the value getting
// trimmed, the type of the KV is changed to kTypeValue.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking_BlobIndex) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeBlobIndex)},
          {"", "fake_blob_index"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /*last_committed_seq*/, nullptr /*merge_operator*/,
          nullptr /*compaction_filter*/, false /*bottommost_level*/,
          2 /*earliest_write_conflict_snapshot*/);
}

// Same as above but with a wide-column entity. In addition to the value getting
// trimmed, the type of the KV is changed to kTypeValue.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking_WideColumnEntity) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeWideColumnEntity)},
          {"", "fake_entity"},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /* last_committed_seq */, nullptr /* merge_operator */,
          nullptr /* compaction_filter */, false /* bottommost_level */,
          2 /* earliest_write_conflict_snapshot */);
}

// Same as above but with a value with preferred seqno entry. In addition to the
// value getting trimmed, the type of the KV is changed to kTypeValue.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       KeepSingleDeletionForWriteConflictChecking_TimedPut) {
  AddSnapshot(2, 0);
  RunTest({test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValuePreferredSeqno)},
          {"", ValueWithPreferredSeqno("v1")},
          {test::KeyStr("a", 2, kTypeSingleDeletion),
           test::KeyStr("a", 1, kTypeValue)},
          {"", ""}, 2 /* last_committed_seq */, nullptr /* merge_operator */,
          nullptr /* compaction_filter */, false /* bottommost_level */,
          2 /* earliest_write_conflict_snapshot */);
}

// Tests when a kTypeValuePreferredSeqno entry can have its preferred sequence
// number swapped in. The required and sufficient conditions for an entry's
// preferred sequence number to get swapped in are:
// 1) The entry is visible to the earliest snapshot, AND
// 2) No more entries with the same user key on lower levels, AND
//    This is either because:
//    2a) This is a compaction to the bottommost level, OR
//    2b) Keys do not exist beyond output level
// 3) The entry will not resurface a range deletion entry after swapping in the
//    preferred sequence number.
TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       TimedPut_NotVisibleToEarliestSnapshot_NoSwapPreferredSeqno) {
  AddSnapshot(3);
  RunTest({test::KeyStr("bar", 5, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("bv2", 2)},
          {test::KeyStr("bar", 5, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("bv2", 2), "bv1"}, 5 /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          true /*bottommost_level*/,
          kMaxSequenceNumber /*earliest_write_conflict_snapshot*/,
          true /*key_not_exists_beyond_output_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       TimedPut_MoreEntriesInLowerLevels_NoSwapPreferredSeqno) {
  // This tests mimics more entries in lower levels with `bottommost_level` and
  // `key_not_exists_beyond_output_level` set to false.
  RunTest({test::KeyStr("bar", 5, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("bv2", 2)},
          {test::KeyStr("bar", 5, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("bv2", 2)}, 5 /*last_committed_seq*/,
          nullptr /*merge_operator*/, nullptr /*compaction_filter*/,
          false /*bottommost_level*/,
          kMaxSequenceNumber /*earliest_write_conflict_snapshot*/,
          false /*key_not_exists_beyond_output_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       TimedPut_ShouldBeCoverredByRangeDeletionBeforeSwap_NoOutput) {
  InitIterators({test::KeyStr("morning", 5, kTypeValuePreferredSeqno),
                 test::KeyStr("morning", 2, kTypeValuePreferredSeqno),
                 test::KeyStr("night", 6, kTypeValue)},
                {ValueWithPreferredSeqno("zao", 3),
                 ValueWithPreferredSeqno("zao", 1), "wan"},
                {test::KeyStr("ma", 6, kTypeRangeDeletion)}, {"mz"}, 6,
                kMaxSequenceNumber /*last_committed_sequence*/,
                nullptr /*merge_op*/, nullptr /*filter*/,
                false /*bottommost_level*/,
                kMaxSequenceNumber /*earliest_write_conflict_snapshot*/,
                true /*key_not_exists_beyond_output_level*/);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 6, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("wan", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
  ASSERT_OK(c_iter_->status());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       TimedPut_WillBeHiddenByRangeDeletionAfterSwap_NoSwap) {
  InitIterators({test::KeyStr("morning", 5, kTypeValuePreferredSeqno),
                 test::KeyStr("night", 6, kTypeValue)},
                {ValueWithPreferredSeqno("zao", 3), "wan"},
                {test::KeyStr("ma", 4, kTypeRangeDeletion)}, {"mz"}, 6,
                kMaxSequenceNumber /*last_committed_sequence*/,
                nullptr /*merge_op*/, nullptr /*filter*/,
                false /*bottommost_level*/,
                kMaxSequenceNumber /*earliest_write_conflict_snapshot*/,
                true /*key_not_exists_beyond_output_level*/);
  c_iter_->SeekToFirst();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("morning", 5, kTypeValuePreferredSeqno),
            c_iter_->key().ToString());
  ASSERT_EQ(ValueWithPreferredSeqno("zao", 3), c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_TRUE(c_iter_->Valid());
  ASSERT_EQ(test::KeyStr("night", 6, kTypeValue), c_iter_->key().ToString());
  ASSERT_EQ("wan", c_iter_->value().ToString());
  c_iter_->Next();
  ASSERT_FALSE(c_iter_->Valid());
  ASSERT_OK(c_iter_->status());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       TimedPut_BottomMostLevelVisibleToEarliestSnapshot_SwapPreferredSeqno) {
  // Preferred seqno got swapped in and also zeroed out as a bottommost level
  // optimization.
  RunTest(
      {test::KeyStr("bar", 5, kTypeValuePreferredSeqno),
       test::KeyStr("bar", 4, kTypeValuePreferredSeqno),
       test::KeyStr("foo", 6, kTypeValue)},
      {ValueWithPreferredSeqno("bv2", 2), ValueWithPreferredSeqno("bv1", 1),
       "fv1"},
      {test::KeyStr("bar", 0, kTypeValue), test::KeyStr("foo", 0, kTypeValue)},
      {"bv2", "fv1"}, 6 /*last_committed_seq*/, nullptr /*merge_operator*/,
      nullptr /*compaction_filter*/, true /*bottommost_level*/);
}

TEST_F(
    CompactionIteratorWithSnapshotCheckerTest,
    TimedPut_NonBottomMostLevelVisibleToEarliestSnapshot_SwapPreferredSeqno) {
  RunTest(
      {test::KeyStr("bar", 5, kTypeValuePreferredSeqno),
       test::KeyStr("bar", 4, kTypeValuePreferredSeqno),
       test::KeyStr("foo", 6, kTypeValue)},
      {ValueWithPreferredSeqno("bv2", 2), ValueWithPreferredSeqno("bv1", 1),
       "fv1"},
      {test::KeyStr("bar", 2, kTypeValue), test::KeyStr("foo", 6, kTypeValue)},
      {"bv2", "fv1"}, 6 /*last_committed_seq*/, nullptr /*merge_operator*/,
      nullptr /*compaction_filter*/, false /*bottommost_level*/,
      kMaxSequenceNumber /*earliest_write_conflict_snapshot*/,
      true /*key_not_exists_beyond_output_level*/);
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       TimedPut_SequenceNumberAlreadyZeroedOut_ChangeType) {
  RunTest(
      {test::KeyStr("bar", 0, kTypeValuePreferredSeqno),
       test::KeyStr("bar", 0, kTypeValuePreferredSeqno),
       test::KeyStr("foo", 0, kTypeValue)},
      {ValueWithPreferredSeqno("bv2", 2), ValueWithPreferredSeqno("bv1", 1),
       "fv1"},
      {test::KeyStr("bar", 0, kTypeValue), test::KeyStr("foo", 0, kTypeValue)},
      {"bv2", "fv1"}, 6 /*last_committed_seq*/, nullptr /*merge_operator*/,
      nullptr /*compaction_filter*/, true /*bottommost_level*/);
}

// Compaction filter should keep uncommitted key as-is, and
//   * Convert the latest value to deletion, and/or
//   * if latest value is a merge, apply filter to all subsequent merges.

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_Value) {
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 2, kTypeValue), test::KeyStr("a", 1, kTypeValue),
       test::KeyStr("b", 3, kTypeValue), test::KeyStr("c", 1, kTypeValue)},
      {"v2", "v1", "v3", "v4"},
      {test::KeyStr("a", 2, kTypeValue), test::KeyStr("a", 1, kTypeDeletion),
       test::KeyStr("b", 3, kTypeValue), test::KeyStr("c", 1, kTypeDeletion)},
      {"v2", "", "v3", ""}, 1 /*last_committed_seq*/,
      nullptr /*merge_operator*/, compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_TimedPut) {
  // TODO(yuzhangyu): Add support for this type in compaction filter.
  // Type kTypeValuePreferredSeqno is not explicitly exposed in the compaction
  // filter API, so users can not operate on it through compaction filter API
  // to remove/purge/change value etc. But this type of entry can be impacted by
  // other entries' filter result, currently only kRemoveAndSkip type of result
  // can affect it.
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest({test::KeyStr("a", 2, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("v1")},
          {test::KeyStr("a", 2, kTypeValuePreferredSeqno)},
          {ValueWithPreferredSeqno("v1")}, 2 /*last_committed_seq*/,
          nullptr /*merge_operator*/, compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_Deletion) {
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 2, kTypeDeletion), test::KeyStr("a", 1, kTypeValue)},
      {"", "v1"},
      {test::KeyStr("a", 2, kTypeDeletion),
       test::KeyStr("a", 1, kTypeDeletion)},
      {"", ""}, 1 /*last_committed_seq*/, nullptr /*merge_operator*/,
      compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest,
       CompactionFilter_PartialMerge) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest({test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 2, kTypeMerge),
           test::KeyStr("a", 1, kTypeMerge)},
          {"v3", "v2", "v1"}, {test::KeyStr("a", 3, kTypeMerge)}, {"v3"},
          2 /*last_committed_seq*/, merge_op.get(), compaction_filter.get());
}

TEST_F(CompactionIteratorWithSnapshotCheckerTest, CompactionFilter_FullMerge) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  std::unique_ptr<CompactionFilter> compaction_filter(
      new FilterAllKeysCompactionFilter());
  RunTest(
      {test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 2, kTypeMerge),
       test::KeyStr("a", 1, kTypeValue)},
      {"v3", "v2", "v1"},
      {test::KeyStr("a", 3, kTypeMerge), test::KeyStr("a", 1, kTypeDeletion)},
      {"v3", ""}, 2 /*last_committed_seq*/, merge_op.get(),
      compaction_filter.get());
}

// Tests how CompactionIterator work together with AllowIngestBehind.
class CompactionIteratorWithAllowIngestBehindTest
    : public CompactionIteratorTest {
 public:
  bool AllowIngestBehind() const override { return true; }
};

// When allow_ingest_behind is set, compaction iterator is not targeting
// the bottommost level since there is no guarantee there won't be further
// data ingested under the compaction output in future.
TEST_P(CompactionIteratorWithAllowIngestBehindTest, NoConvertToPutAtBottom) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  RunTest({test::KeyStr("a", 4, kTypeMerge), test::KeyStr("a", 3, kTypeMerge),
           test::KeyStr("a", 2, kTypeMerge), test::KeyStr("b", 1, kTypeValue)},
          {"a4", "a3", "a2", "b1"},
          {test::KeyStr("a", 4, kTypeMerge), test::KeyStr("b", 1, kTypeValue)},
          {"a2,a3,a4", "b1"}, kMaxSequenceNumber /*last_committed_seq*/,
          merge_op.get(), nullptr /*compaction_filter*/,
          true /*bottomost_level*/);
}

TEST_P(CompactionIteratorWithAllowIngestBehindTest,
       MergeToPutIfEncounteredPutAtBottom) {
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendOperator();
  RunTest({test::KeyStr("a", 4, kTypeMerge), test::KeyStr("a", 3, kTypeMerge),
           test::KeyStr("a", 2, kTypeValue), test::KeyStr("b", 1, kTypeValue)},
          {"a4", "a3", "a2", "b1"},
          {test::KeyStr("a", 4, kTypeValue), test::KeyStr("b", 1, kTypeValue)},
          {"a2,a3,a4", "b1"}, kMaxSequenceNumber /*last_committed_seq*/,
          merge_op.get(), nullptr /*compaction_filter*/,
          true /*bottomost_level*/);
}

INSTANTIATE_TEST_CASE_P(CompactionIteratorWithAllowIngestBehindTestInstance,
                        CompactionIteratorWithAllowIngestBehindTest,
                        testing::Values(true, false));

class CompactionIteratorTsGcTest : public CompactionIteratorTest {
 public:
  CompactionIteratorTsGcTest()
      : CompactionIteratorTest(test::BytewiseComparatorWithU64TsWrapper()) {}
};

TEST_P(CompactionIteratorTsGcTest, NoKeyEligibleForGC) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/4, kTypeValue),
      test::KeyStr(/*ts=*/102, user_key[0], /*seq=*/3,
                   kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/104, user_key[1], /*seq=*/5, kTypeValue)};
  const std::vector<std::string> input_values = {"a3", "", "b2"};
  std::string full_history_ts_low;
  // All keys' timestamps are newer than or equal to 102, thus none of them
  // will be eligible for GC.
  PutFixed64(&full_history_ts_low, 102);
  const std::vector<std::string>& expected_keys = input_keys;
  const std::vector<std::string>& expected_values = input_values;
  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};
  for (const std::pair<bool, bool>& param : params) {
    const bool bottommost_level = param.first;
    const bool key_not_exists_beyond_output_level = param.second;
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            bottommost_level,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            key_not_exists_beyond_output_level, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, NoMergeEligibleForGc) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(10002, user_key, 102, kTypeMerge),
      test::KeyStr(10001, user_key, 101, kTypeMerge),
      test::KeyStr(10000, user_key, 100, kTypeValue)};
  const std::vector<std::string> input_values = {"2", "1", "a0"};
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendTESTOperator();
  const std::vector<std::string>& expected_keys = input_keys;
  const std::vector<std::string>& expected_values = input_values;
  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};
  for (const auto& param : params) {
    const bool bottommost_level = param.first;
    const bool key_not_exists_beyond_output_level = param.second;
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber, merge_op.get(),
            /*compaction_filter=*/nullptr, bottommost_level,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            key_not_exists_beyond_output_level,
            /*full_history_ts_low=*/nullptr);
  }
}

TEST_P(CompactionIteratorTsGcTest, AllKeysOlderThanThreshold) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/4,
                   kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key[0], /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key[0], /*seq=*/2, kTypeValue),
      test::KeyStr(/*ts=*/104, user_key[1], /*seq=*/5, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "a1", "b5"};
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, std::numeric_limits<uint64_t>::max());
  {
    // With a snapshot at seq 3, both the deletion marker and the key at 3 must
    // be preserved.
    AddSnapshot(3);
    const std::vector<std::string> expected_keys = {
        input_keys[0], input_keys[1], input_keys[3]};
    const std::vector<std::string> expected_values = {"", "a2", "b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
    ClearSnapshots();
  }
  {
    // No snapshot, the deletion marker should be preserved because the user
    // key may appear beyond output level.
    const std::vector<std::string> expected_keys = {input_keys[0],
                                                    input_keys[3]};
    const std::vector<std::string> expected_values = {"", "b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
  {
    // No snapshot, the deletion marker can be dropped because the user key
    // does not appear in higher levels.
    const std::vector<std::string> expected_keys = {input_keys[3]};
    const std::vector<std::string> expected_values = {"b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/true, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, SomeMergesOlderThanThreshold) {
  constexpr char user_key[][2] = {"a", "f"};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/25000, user_key[0], /*seq=*/2500, kTypeMerge),
      test::KeyStr(/*ts=*/19000, user_key[0], /*seq=*/2300, kTypeMerge),
      test::KeyStr(/*ts=*/18000, user_key[0], /*seq=*/1800, kTypeMerge),
      test::KeyStr(/*ts=*/16000, user_key[0], /*seq=*/1600, kTypeValue),
      test::KeyStr(/*ts=*/19000, user_key[1], /*seq=*/2000, kTypeMerge),
      test::KeyStr(/*ts=*/17000, user_key[1], /*seq=*/1700, kTypeMerge),
      test::KeyStr(/*ts=*/15000, user_key[1], /*seq=*/1600,
                   kTypeDeletionWithTimestamp)};
  const std::vector<std::string> input_values = {"25", "19", "18", "16",
                                                 "19", "17", ""};
  std::shared_ptr<MergeOperator> merge_op =
      MergeOperators::CreateStringAppendTESTOperator();
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, 20000);

  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};

  {
    AddSnapshot(1600);
    AddSnapshot(1900);
    const std::vector<std::string> expected_keys = {
        test::KeyStr(/*ts=*/25000, user_key[0], /*seq=*/2500, kTypeMerge),
        test::KeyStr(/*ts=*/19000, user_key[0], /*seq=*/2300, kTypeMerge),
        test::KeyStr(/*ts=*/18000, user_key[0], /*seq=*/1800, kTypeMerge),
        test::KeyStr(/*ts=*/16000, user_key[0], /*seq=*/1600, kTypeValue),
        test::KeyStr(/*ts=*/19000, user_key[1], /*seq=*/2000, kTypeMerge),
        test::KeyStr(/*ts=*/17000, user_key[1], /*seq=*/1700, kTypeMerge),
        test::KeyStr(/*ts=*/15000, user_key[1], /*seq=*/1600,
                     kTypeDeletionWithTimestamp)};
    const std::vector<std::string> expected_values = {"25", "19", "18", "16",
                                                      "19", "17", ""};
    for (const auto& param : params) {
      const bool bottommost_level = param.first;
      const bool key_not_exists_beyond_output_level = param.second;
      auto expected_keys_copy = expected_keys;
      auto expected_values_copy = expected_values;
      if (bottommost_level || key_not_exists_beyond_output_level) {
        // the kTypeDeletionWithTimestamp will be dropped
        expected_keys_copy.pop_back();
        expected_values_copy.pop_back();
        if (bottommost_level) {
          // seq zero
          expected_keys_copy[3] =
              test::KeyStr(/*ts=*/0, user_key[0], /*seq=*/0, kTypeValue);
        }
      }
      RunTest(input_keys, input_values, expected_keys_copy,
              expected_values_copy,
              /*last_committed_seq=*/kMaxSequenceNumber, merge_op.get(),
              /*compaction_filter=*/nullptr, bottommost_level,
              /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
              key_not_exists_beyond_output_level, &full_history_ts_low);
    }
    ClearSnapshots();
  }

  // No snapshots
  {
    const std::vector<std::string> expected_keys = {
        test::KeyStr(/*ts=*/25000, user_key[0], /*seq=*/2500, kTypeValue),
        test::KeyStr(/*ts=*/19000, user_key[1], /*seq=*/2000, kTypeValue)};
    const std::vector<std::string> expected_values = {"16,18,19,25", "17,19"};
    for (const auto& param : params) {
      const bool bottommost_level = param.first;
      const bool key_not_exists_beyond_output_level = param.second;
      auto expected_keys_copy = expected_keys;
      auto expected_values_copy = expected_values;
      if (bottommost_level) {
        expected_keys_copy[1] =
            test::KeyStr(/*ts=*/0, user_key[1], /*seq=*/0, kTypeValue);
      }
      RunTest(input_keys, input_values, expected_keys_copy,
              expected_values_copy,
              /*last_committed_seq=*/kMaxSequenceNumber, merge_op.get(),
              /*compaction_filter=*/nullptr, bottommost_level,
              /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
              key_not_exists_beyond_output_level, &full_history_ts_low);
    }
  }
}

TEST_P(CompactionIteratorTsGcTest, NewHidesOldSameSnapshot) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeValue),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "a1", "a0"};
  {
    std::string full_history_ts_low;
    // Keys whose timestamps larger than or equal to 102 will be preserved.
    PutFixed64(&full_history_ts_low, 102);
    const std::vector<std::string> expected_keys = {
        input_keys[0], input_keys[1], input_keys[2]};
    const std::vector<std::string> expected_values = {"", input_values[1],
                                                      input_values[2]};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, DropTombstones) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "", "a0"};
  const std::vector<std::string> expected_keys = {input_keys[0], input_keys[1]};
  const std::vector<std::string> expected_values = {"", "a2"};

  // Take a snapshot at seq 2.
  AddSnapshot(2);

  {
    // Non-bottommost level, but key does not exist beyond output level.
    std::string full_history_ts_low;
    PutFixed64(&full_history_ts_low, 102);
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/true, &full_history_ts_low);
  }
  {
    // Bottommost level
    std::string full_history_ts_low;
    PutFixed64(&full_history_ts_low, 102);
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/true,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, RewriteTs) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeDeletionWithTimestamp),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "", "a0"};
  const std::vector<std::string> expected_keys = {
      input_keys[0], input_keys[1], input_keys[2],
      test::KeyStr(/*ts=*/0, user_key, /*seq=*/0, kTypeValue)};
  const std::vector<std::string> expected_values = {"", "a2", "", "a0"};

  AddSnapshot(1);
  AddSnapshot(2);

  {
    // Bottommost level and need to rewrite both ts and seq.
    std::string full_history_ts_low;
    PutFixed64(&full_history_ts_low, 102);
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/true,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/true, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, SingleDeleteNoKeyEligibleForGC) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/104, user_key[0], /*seq=*/4, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/102, user_key[1], /*seq=*/2, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a3", "b2"};
  std::string full_history_ts_low;
  // All keys' timestamps are newer than or equal to 102, thus none of them
  // will be eligible for GC.
  PutFixed64(&full_history_ts_low, 102);
  const std::vector<std::string>& expected_keys = input_keys;
  const std::vector<std::string>& expected_values = input_values;
  const std::vector<std::pair<bool, bool>> params = {
      {false, false}, {false, true}, {true, true}};
  for (const std::pair<bool, bool>& param : params) {
    const bool bottommost_level = param.first;
    const bool key_not_exists_beyond_output_level = param.second;
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            bottommost_level,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            key_not_exists_beyond_output_level, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, SingleDeleteDropTombstones) {
  constexpr char user_key[] = "a";
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key, /*seq=*/4, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/102, user_key, /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/101, user_key, /*seq=*/2, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/100, user_key, /*seq=*/1, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "", "a0"};
  const std::vector<std::string> expected_keys = {input_keys[0], input_keys[1]};
  const std::vector<std::string> expected_values = {"", "a2"};

  // Take a snapshot at seq 2.
  AddSnapshot(2);
  {
    const std::vector<std::pair<bool, bool>> params = {
        {false, false}, {false, true}, {true, true}};
    for (const std::pair<bool, bool>& param : params) {
      const bool bottommost_level = param.first;
      const bool key_not_exists_beyond_output_level = param.second;
      std::string full_history_ts_low;
      PutFixed64(&full_history_ts_low, 102);
      RunTest(input_keys, input_values, expected_keys, expected_values,
              /*last_committed_seq=*/kMaxSequenceNumber,
              /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
              bottommost_level,
              /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
              key_not_exists_beyond_output_level, &full_history_ts_low);
    }
  }
}

TEST_P(CompactionIteratorTsGcTest, SingleDeleteAllKeysOlderThanThreshold) {
  constexpr char user_key[][2] = {{'a', '\0'}, {'b', '\0'}};
  const std::vector<std::string> input_keys = {
      test::KeyStr(/*ts=*/103, user_key[0], /*seq=*/4, kTypeSingleDeletion),
      test::KeyStr(/*ts=*/102, user_key[0], /*seq=*/3, kTypeValue),
      test::KeyStr(/*ts=*/104, user_key[1], /*seq=*/5, kTypeValue)};
  const std::vector<std::string> input_values = {"", "a2", "b5"};
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, std::numeric_limits<uint64_t>::max());
  {
    // With a snapshot at seq 3, both the deletion marker and the key at 3 must
    // be preserved.
    AddSnapshot(3);
    const std::vector<std::string> expected_keys = {
        input_keys[0], input_keys[1], input_keys[2]};
    const std::vector<std::string> expected_values = {"", "a2", "b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
    ClearSnapshots();
  }
  {
    // No snapshot.
    const std::vector<std::string> expected_keys = {input_keys[2]};
    const std::vector<std::string> expected_values = {"b5"};
    RunTest(input_keys, input_values, expected_keys, expected_values,
            /*last_committed_seq=*/kMaxSequenceNumber,
            /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
            /*bottommost_level=*/false,
            /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
            /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
  }
}

TEST_P(CompactionIteratorTsGcTest, ZeroSeqOfKeyAndSnapshot) {
  AddSnapshot(0);
  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, std::numeric_limits<uint64_t>::max());
  const std::vector<std::string> input_keys = {
      test::KeyStr(101, "a", 0, kTypeValue),
      test::KeyStr(102, "b", 0, kTypeValue)};
  const std::vector<std::string> input_values = {"a1", "b1"};
  RunTest(input_keys, input_values, input_keys, input_values,
          /*last_committed_seq=*/kMaxSequenceNumber,
          /*merge_operator=*/nullptr, /*compaction_filter=*/nullptr,
          /*bottommost_level=*/false,
          /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
          /*key_not_exists_beyond_output_level=*/false, &full_history_ts_low);
}

INSTANTIATE_TEST_CASE_P(CompactionIteratorTsGcTestInstance,
                        CompactionIteratorTsGcTest,
                        testing::Values(true, false));

// Test fixture for wide column entity blob extraction tests
class WideColumnEntityBlobExtractionTest : public testing::Test {
 public:
  WideColumnEntityBlobExtractionTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
  }

  void SetUp() override {
    blob_file_paths_.clear();
    blob_file_additions_.clear();
    next_file_number_ = 2;
  }

  uint64_t GetNextFileNumber() { return next_file_number_++; }

  // Helper to create a serialized wide column entity
  std::string CreateEntity(const WideColumns& columns) {
    std::string entity;
    EXPECT_OK(WideColumnSerialization::Serialize(columns, entity));
    return entity;
  }

  // Run compaction iterator with blob file builder enabled
  void RunCompactionWithBlobExtraction(
      const std::vector<std::string>& input_keys,
      const std::vector<std::string>& input_values, uint64_t min_blob_size,
      std::vector<std::string>* output_keys,
      std::vector<std::string>* output_values) {
    // Set up options with blob files enabled
    Options options;
    options.cf_paths.emplace_back(
        test::PerThreadDBPath(mock_env_.get(),
                              "WideColumnEntityBlobExtractionTest"),
        0);
    options.enable_blob_files = true;
    options.min_blob_size = min_blob_size;
    options.env = mock_env_.get();

    ImmutableOptions immutable_options(options);
    MutableCFOptions mutable_cf_options(options);

    constexpr int job_id = 1;
    constexpr uint32_t column_family_id = 0;
    constexpr char column_family_name[] = "default";
    constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

    FileOptions file_options;
    WriteOptions write_options;

    BlobFileBuilder blob_file_builder(
        [this]() { return GetNextFileNumber(); }, fs_, &immutable_options,
        &mutable_cf_options, &file_options, &write_options, "" /*db_id*/,
        "" /*db_session_id*/, job_id, column_family_id, column_family_name,
        write_hint, nullptr /*IOTracer*/,
        nullptr /*BlobFileCompletionCallback*/, BlobFileCreationReason::kFlush,
        immutable_options.cf_paths.front().path, &blob_file_paths_,
        &blob_file_additions_);

    // Set up the input iterator
    const Comparator* cmp = BytewiseComparator();
    InternalKeyComparator icmp(cmp);
    std::vector<SequenceNumber> snapshots;

    std::unique_ptr<VectorIterator> iter(
        new VectorIterator(input_keys, input_values, &icmp));

    std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
        new CompactionRangeDelAggregator(&icmp, snapshots));

    std::atomic<bool> shutting_down{false};
    const std::atomic<bool> manual_compaction_canceled{false};

    MergeHelper merge_helper(Env::Default(), cmp, nullptr /*merge_op*/,
                             nullptr /*filter*/, nullptr /*db_options*/, false,
                             0 /*latest_snapshot*/,
                             nullptr /*snapshot_checker*/, 0 /*level*/,
                             nullptr /*statistics*/, &shutting_down);

    iter->SeekToFirst();
    CompactionIterator c_iter(
        iter.get(), cmp, &merge_helper, kMaxSequenceNumber, &snapshots,
        kMaxSequenceNumber /*earliest_snapshot*/,
        kMaxSequenceNumber /*earliest_write_conflict_snapshot*/,
        kMaxSequenceNumber /*job_snapshot*/, nullptr /*snapshot_checker*/,
        Env::Default(), false /*report_detailed_time*/, range_del_agg.get(),
        &blob_file_builder, true /*allow_data_in_errors*/,
        true /*enforce_single_del_contracts*/, manual_compaction_canceled,
        nullptr /*compaction*/, false /*must_count_input_entries*/,
        nullptr /*compaction_filter*/, &shutting_down);

    c_iter.SeekToFirst();
    while (c_iter.Valid()) {
      ASSERT_OK(c_iter.status());
      output_keys->push_back(c_iter.key().ToString());
      output_values->push_back(c_iter.value().ToString());
      c_iter.Next();
    }
    ASSERT_OK(c_iter.status());

    ASSERT_OK(blob_file_builder.Finish());
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_;
  std::vector<std::string> blob_file_paths_;
  std::vector<BlobFileAddition> blob_file_additions_;
  uint64_t next_file_number_ = 2;
};

// Test extracting a single large column to blob
TEST_F(WideColumnEntityBlobExtractionTest,
       WideColumnEntityBlobExtractionSingleColumn) {
  // Create an entity with one column larger than min_blob_size
  const std::string large_value(1000, 'x');  // 1000 bytes
  WideColumns columns{{"col1", large_value}};
  std::string entity = CreateEntity(columns);

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // min_blob_size = 100, so the 1000-byte column should be extracted
  RunCompactionWithBlobExtraction(input_keys, input_values, 100, &output_keys,
                                  &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  // Verify the key is still a wide column entity
  ParsedInternalKey ikey;
  ASSERT_OK(ParseInternalKey(output_keys[0], &ikey, true));
  ASSERT_EQ(kTypeWideColumnEntity, ikey.type);

  // Verify the value has blob columns
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns));

  // The large column should now be a blob reference
  ASSERT_EQ(1, blob_columns.size());
  ASSERT_EQ(0, blob_columns[0].first);  // Column index 0

  // Verify blob index is valid
  const BlobIndex& blob_idx = blob_columns[0].second;
  ASSERT_FALSE(blob_idx.IsInlined());
  ASSERT_EQ(large_value.size(), blob_idx.size());

  // Verify blob file was created
  ASSERT_EQ(1, blob_file_additions_.size());
}

// Test extracting multiple large columns to blobs
TEST_F(WideColumnEntityBlobExtractionTest,
       WideColumnEntityBlobExtractionMultipleColumns) {
  // Create an entity with multiple large columns
  const std::string large_value1(500, 'a');
  const std::string large_value2(600, 'b');
  const std::string large_value3(700, 'c');
  WideColumns columns{
      {"col1", large_value1}, {"col2", large_value2}, {"col3", large_value3}};
  std::string entity = CreateEntity(columns);

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // min_blob_size = 100, all columns should be extracted
  RunCompactionWithBlobExtraction(input_keys, input_values, 100, &output_keys,
                                  &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  // Verify the value has all columns as blob references
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns));

  // All three columns should be blob references
  ASSERT_EQ(3, blob_columns.size());

  // Verify the blob indices
  ASSERT_EQ(0, blob_columns[0].first);
  ASSERT_EQ(large_value1.size(), blob_columns[0].second.size());
  ASSERT_EQ(1, blob_columns[1].first);
  ASSERT_EQ(large_value2.size(), blob_columns[1].second.size());
  ASSERT_EQ(2, blob_columns[2].first);
  ASSERT_EQ(large_value3.size(), blob_columns[2].second.size());
}

// Test that columns below min_blob_size are not extracted
TEST_F(WideColumnEntityBlobExtractionTest,
       WideColumnEntityNoExtractionBelowThreshold) {
  // Create an entity with columns smaller than min_blob_size
  const std::string small_value1(50, 'x');
  const std::string small_value2(80, 'y');
  WideColumns columns{{"col1", small_value1}, {"col2", small_value2}};
  std::string entity = CreateEntity(columns);

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // min_blob_size = 100, no columns should be extracted
  RunCompactionWithBlobExtraction(input_keys, input_values, 100, &output_keys,
                                  &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  // Since no columns were extracted, the entity should remain unchanged
  // (version 1 format, no blob columns)
  ASSERT_EQ(entity, output_values[0]);

  // No blob files should be created
  ASSERT_EQ(0, blob_file_additions_.size());
}

// Test entity with some columns above and some below threshold
TEST_F(WideColumnEntityBlobExtractionTest, WideColumnEntityMixedSizes) {
  // Create an entity with mixed-size columns
  // Note: columns must be in sorted order by name
  const std::string large_value(200, 'l');  // Above threshold
  const std::string small_value(50, 's');   // Below threshold
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};
  std::string entity = CreateEntity(columns);

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // min_blob_size = 100, only large_col should be extracted
  RunCompactionWithBlobExtraction(input_keys, input_values, 100, &output_keys,
                                  &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  // Verify the value has one blob column
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns));

  // Only the large column should be a blob reference
  ASSERT_EQ(1, blob_columns.size());
  ASSERT_EQ(0, blob_columns[0].first);  // Column index 0 (large_col)
  ASSERT_EQ(large_value.size(), blob_columns[0].second.size());

  // The small column should still be inline
  ASSERT_EQ(2, output_columns.size());
  ASSERT_EQ("large_col", output_columns[0].name().ToString());
  ASSERT_EQ("small_col", output_columns[1].name().ToString());
  ASSERT_EQ(small_value, output_columns[1].value().ToString());
}

// Test extracting the default column (empty column name) to blob
TEST_F(WideColumnEntityBlobExtractionTest,
       WideColumnEntityDefaultColumnBlobExtraction) {
  // Create an entity with the default column (empty name) that is large
  const std::string large_default_value(500, 'd');
  const std::string regular_value(30, 'r');
  WideColumns columns{
      {kDefaultWideColumnName, large_default_value},  // Default column
      {"regular", regular_value}};
  std::string entity = CreateEntity(columns);

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // min_blob_size = 100, only the default column should be extracted
  RunCompactionWithBlobExtraction(input_keys, input_values, 100, &output_keys,
                                  &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  // Verify the value has blob columns
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns));

  // The default column should be a blob reference
  ASSERT_EQ(1, blob_columns.size());
  ASSERT_EQ(0, blob_columns[0].first);  // Column index 0 (default column)
  ASSERT_EQ(large_default_value.size(), blob_columns[0].second.size());

  // Verify the default column name is preserved
  ASSERT_EQ(2, output_columns.size());
  ASSERT_EQ(kDefaultWideColumnName, output_columns[0].name());

  // The regular column should still be inline
  ASSERT_EQ("regular", output_columns[1].name().ToString());
  ASSERT_EQ(regular_value, output_columns[1].value().ToString());
}

// Mock BlobFetcher for testing blob GC without a real Version
class MockBlobFetcher : public BlobFetcher {
 public:
  MockBlobFetcher() = default;

  using BlobFetcher::FetchBlob;

  Status FetchBlob(const Slice& /* user_key */, const BlobIndex& blob_index,
                   FilePrefetchBuffer* /* prefetch_buffer */,
                   PinnableSlice* blob_value,
                   uint64_t* bytes_read) const override {
    auto it = blob_data_.find(
        std::make_pair(blob_index.file_number(), blob_index.offset()));
    if (it == blob_data_.end()) {
      return Status::NotFound("Blob not found in mock");
    }
    blob_value->Reset();
    blob_value->PinSelf(it->second);
    if (bytes_read) {
      *bytes_read = it->second.size();
    }
    return Status::OK();
  }

  const ReadOptions& read_options() const override { return read_options_; }

  void AddBlob(uint64_t file_number, uint64_t offset,
               const std::string& value) {
    blob_data_[std::make_pair(file_number, offset)] = value;
  }

 private:
  std::map<std::pair<uint64_t, uint64_t>, std::string> blob_data_;
  ReadOptions read_options_;
};

// FakeCompaction that supports blob garbage collection
class FakeCompactionWithBlobGC : public CompactionIterator::CompactionProxy {
 public:
  int level() const override { return 0; }

  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return is_bottommost_level;
  }

  bool bottommost_level() const override { return is_bottommost_level; }

  int number_levels() const override { return 1; }

  Slice GetLargestUserKey() const override {
    return "\xff\xff\xff\xff\xff\xff\xff\xff\xff";
  }

  bool allow_ingest_behind() const override { return false; }

  bool allow_mmap_reads() const override { return false; }

  bool enable_blob_garbage_collection() const override {
    return enable_blob_gc_;
  }

  double blob_garbage_collection_age_cutoff() const override {
    return blob_gc_age_cutoff_;
  }

  uint64_t blob_compaction_readahead_size() const override { return 0; }

  const Version* input_version() const override { return nullptr; }

  bool DoesInputReferenceBlobFiles() const override { return true; }

  const Compaction* real_compaction() const override { return nullptr; }

  bool SupportsPerKeyPlacement() const override { return false; }

  bool is_bottommost_level = true;
  // Note: enable_blob_gc_ must be false when input_version() returns nullptr
  // because ComputeBlobGarbageCollectionCutoffFileNumber asserts that version
  // is not null when blob GC is enabled. Without a real Version, we cannot
  // test the full blob GC flow, but we can test that entities with blob
  // indices pass through correctly.
  // NOTE: Integration tests for entity blob GC with a real Version exist in
  // db_wide_basic_test.cc (CompactionFilterWithBlobGC test).
  bool enable_blob_gc_ = false;
  double blob_gc_age_cutoff_ = 1.0;
};

// Test fixture for wide column entity blob GC tests
class WideColumnEntityBlobGCTest : public testing::Test {
 public:
  WideColumnEntityBlobGCTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
  }

  void SetUp() override {
    blob_file_paths_.clear();
    blob_file_additions_.clear();
    next_file_number_ = 2;
    mock_blob_fetcher_ = std::make_unique<MockBlobFetcher>();
  }

  uint64_t GetNextFileNumber() { return next_file_number_++; }

  // Helper to create a serialized entity with blob indices (version 2 format)
  std::string CreateEntityWithBlobIndices(
      const std::vector<std::pair<std::string, std::string>>& columns,
      const std::vector<std::tuple<size_t, uint64_t, uint64_t, uint64_t>>&
          blob_info) {
    // blob_info: vector of (column_index, file_number, offset, size)
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;

    for (const auto& info : blob_info) {
      size_t col_idx = std::get<0>(info);
      uint64_t file_number = std::get<1>(info);
      uint64_t offset = std::get<2>(info);
      uint64_t size = std::get<3>(info);

      std::string blob_index_str;
      BlobIndex::EncodeBlob(&blob_index_str, file_number, offset, size,
                            kNoCompression);

      BlobIndex blob_idx;
      Slice blob_slice(blob_index_str);
      Status s = blob_idx.DecodeFrom(blob_slice);
      EXPECT_OK(s);

      blob_columns.emplace_back(col_idx, blob_idx);
    }

    std::string output;
    Status s =
        WideColumnSerialization::SerializeV2(columns, blob_columns, output);
    EXPECT_OK(s);
    return output;
  }

  // Run compaction with blob GC enabled
  void RunCompactionWithBlobGC(
      const std::vector<std::string>& input_keys,
      const std::vector<std::string>& input_values,
      uint64_t gc_cutoff_file_number, uint64_t min_blob_size,
      std::vector<std::string>* output_keys,
      std::vector<std::string>* output_values,
      const CompactionFilter* compaction_filter = nullptr) {
    // Set up options with blob files enabled
    Options options;
    options.cf_paths.emplace_back(
        test::PerThreadDBPath(mock_env_.get(), "WideColumnEntityBlobGCTest"),
        0);
    options.enable_blob_files = true;
    options.min_blob_size = min_blob_size;
    options.env = mock_env_.get();
    options.enable_blob_garbage_collection = true;
    options.blob_garbage_collection_age_cutoff = 1.0;

    ImmutableOptions immutable_options(options);
    MutableCFOptions mutable_cf_options(options);

    constexpr int job_id = 1;
    constexpr uint32_t column_family_id = 0;
    constexpr char column_family_name[] = "default";
    constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

    FileOptions file_options;
    WriteOptions write_options;

    std::unique_ptr<BlobFileBuilder> blob_file_builder;
    if (min_blob_size < std::numeric_limits<uint64_t>::max()) {
      blob_file_builder = std::make_unique<BlobFileBuilder>(
          [this]() { return GetNextFileNumber(); }, fs_, &immutable_options,
          &mutable_cf_options, &file_options, &write_options, "" /*db_id*/,
          "" /*db_session_id*/, job_id, column_family_id, column_family_name,
          write_hint, nullptr /*IOTracer*/,
          nullptr /*BlobFileCompletionCallback*/,
          BlobFileCreationReason::kCompaction,
          immutable_options.cf_paths.front().path, &blob_file_paths_,
          &blob_file_additions_);
    }

    // Set up the input iterator
    const Comparator* cmp = BytewiseComparator();
    InternalKeyComparator icmp(cmp);
    std::vector<SequenceNumber> snapshots;

    std::unique_ptr<VectorIterator> iter(
        new VectorIterator(input_keys, input_values, &icmp));

    std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
        new CompactionRangeDelAggregator(&icmp, snapshots));

    std::atomic<bool> shutting_down{false};
    const std::atomic<bool> manual_compaction_canceled{false};

    MergeHelper merge_helper(Env::Default(), cmp, nullptr /*merge_op*/,
                             nullptr /*filter*/, nullptr /*db_options*/, false,
                             0 /*latest_snapshot*/,
                             nullptr /*snapshot_checker*/, 0 /*level*/,
                             nullptr /*statistics*/, &shutting_down);

    // Create compaction proxy. Note: blob GC is disabled since we don't have
    // a real Version object. These tests verify that entities with blob indices
    // (version 2 format) pass through the compaction iterator correctly.
    auto fake_compaction = std::make_unique<FakeCompactionWithBlobGC>();

    iter->SeekToFirst();

    CompactionIterator c_iter(
        iter.get(), cmp, &merge_helper, kMaxSequenceNumber, &snapshots,
        kMaxSequenceNumber /*earliest_snapshot*/,
        kMaxSequenceNumber /*earliest_write_conflict_snapshot*/,
        kMaxSequenceNumber /*job_snapshot*/, nullptr /*snapshot_checker*/,
        Env::Default(), false /*report_detailed_time*/, range_del_agg.get(),
        blob_file_builder.get(), true /*allow_data_in_errors*/,
        true /*enforce_single_del_contracts*/, manual_compaction_canceled,
        std::move(fake_compaction), false /*must_count_input_entries*/,
        compaction_filter, &shutting_down);

    c_iter.SeekToFirst();
    while (c_iter.Valid()) {
      ASSERT_OK(c_iter.status());
      output_keys->push_back(c_iter.key().ToString());
      output_values->push_back(c_iter.value().ToString());
      c_iter.Next();
    }
    ASSERT_OK(c_iter.status());

    if (blob_file_builder) {
      ASSERT_OK(blob_file_builder->Finish());
    }

    // Unused parameters kept for potential future extension
    (void)gc_cutoff_file_number;
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_;
  std::vector<std::string> blob_file_paths_;
  std::vector<BlobFileAddition> blob_file_additions_;
  uint64_t next_file_number_ = 2;
  std::unique_ptr<MockBlobFetcher> mock_blob_fetcher_;
};

// Test 1: Verify that wide column entities with blob indices (version 2 format)
// pass through the compaction iterator correctly with preserved structure.
// This tests the serialization/deserialization path for entities containing
// blob column references.
TEST_F(WideColumnEntityBlobGCTest, WideColumnEntityBlobGCRelocate) {
  // Create an entity that already has blob indices (simulating a previously
  // extracted blob)
  const std::string blob_value = std::string(500, 'B');

  // Column data with placeholder for the blob column
  std::vector<std::pair<std::string, std::string>> columns = {
      {"blob_col", blob_value},  // This will be marked as a blob
      {"inline_col", "inline_data"}};

  // Create entity with blob index for column 0
  // Blob is in file 1, which should be below our GC cutoff
  std::string entity = CreateEntityWithBlobIndices(
      columns, {{0, /* file_number */ 1, /* offset */ 100, /* size */ 500}});

  // Verify the entity was created with blob columns
  {
    bool hb = false;
    ASSERT_OK(WideColumnSerialization::HasBlobColumns(Slice(entity), hb));
    ASSERT_TRUE(hb);
  }
  {
    Slice entity_slice(entity);
    uint32_t v = 0;
    ASSERT_TRUE(GetVarint32(&entity_slice, &v));
    ASSERT_EQ(v, WideColumnSerialization::kVersion2);
  }

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // Run compaction - blob GC is not enabled (no Version available),
  // so the entity should pass through unchanged
  RunCompactionWithBlobGC(input_keys, input_values,
                          /* gc_cutoff_file_number */ 5,
                          /* min_blob_size */ 100, &output_keys,
                          &output_values);

  // The entity should pass through the compaction iterator
  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  // Verify the key is still a wide column entity
  ParsedInternalKey ikey;
  ASSERT_OK(ParseInternalKey(output_keys[0], &ikey, true));
  ASSERT_EQ(kTypeWideColumnEntity, ikey.type);

  // Entity with blob columns should be preserved through compaction
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns_out));

  // Entity structure should be preserved
  ASSERT_EQ(2, output_columns.size());
  ASSERT_EQ("blob_col", output_columns[0].name().ToString());
  ASSERT_EQ("inline_col", output_columns[1].name().ToString());

  // Blob column reference should be preserved
  ASSERT_EQ(1, blob_columns_out.size());
  ASSERT_EQ(0, blob_columns_out[0].first);  // Column index 0
  const BlobIndex& blob_idx = blob_columns_out[0].second;
  ASSERT_EQ(1, blob_idx.file_number());
  ASSERT_EQ(100, blob_idx.offset());
  ASSERT_EQ(500, blob_idx.size());
}

// Test 2: Verify that entities with blob references to different file numbers
// are correctly preserved, maintaining the exact blob index information.
TEST_F(WideColumnEntityBlobGCTest, WideColumnEntityBlobGCNoRelocateNewerFile) {
  const std::string blob_value = std::string(500, 'B');

  std::vector<std::pair<std::string, std::string>> columns = {
      {"blob_col", blob_value}, {"inline_col", "inline_data"}};

  // Create entity with blob index in file 10
  std::string entity = CreateEntityWithBlobIndices(
      columns, {{0, /* file_number */ 10, /* offset */ 100, /* size */ 500}});

  {
    bool hb = false;
    ASSERT_OK(WideColumnSerialization::HasBlobColumns(Slice(entity), hb));
    ASSERT_TRUE(hb);
  }

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // Run compaction - entity should pass through unchanged
  RunCompactionWithBlobGC(input_keys, input_values,
                          /* gc_cutoff_file_number */ 5,
                          /* min_blob_size */ 100, &output_keys,
                          &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  ParsedInternalKey ikey;
  ASSERT_OK(ParseInternalKey(output_keys[0], &ikey, true));
  ASSERT_EQ(kTypeWideColumnEntity, ikey.type);

  // The entity should remain unchanged
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns_out));

  // Blob column should still exist and reference the same file
  ASSERT_EQ(1, blob_columns_out.size());
  ASSERT_EQ(0, blob_columns_out[0].first);  // Column index 0

  const BlobIndex& blob_idx = blob_columns_out[0].second;
  ASSERT_EQ(10, blob_idx.file_number());
  ASSERT_EQ(100, blob_idx.offset());
  ASSERT_EQ(500, blob_idx.size());
}

// Test 3: Verify that entities with multiple blob columns referencing different
// blob files are correctly preserved with all blob indices intact.
TEST_F(WideColumnEntityBlobGCTest, WideColumnEntityBlobGCPartialRelocation) {
  const std::string blob_value1 = std::string(500, 'A');
  const std::string blob_value2 = std::string(600, 'B');
  const std::string blob_value3 = std::string(700, 'C');

  std::vector<std::pair<std::string, std::string>> columns = {
      {"col1", blob_value1}, {"col2", blob_value2}, {"col3", blob_value3}};

  // Create entity with multiple blob columns referencing different files
  // col1 -> file 2, col2 -> file 8, col3 -> file 3
  std::string entity = CreateEntityWithBlobIndices(
      columns, {{0, /* file */ 2, /* offset */ 100, /* size */ 500},
                {1, /* file */ 8, /* offset */ 200, /* size */ 600},
                {2, /* file */ 3, /* offset */ 300, /* size */ 700}});

  {
    bool hb = false;
    ASSERT_OK(WideColumnSerialization::HasBlobColumns(Slice(entity), hb));
    ASSERT_TRUE(hb);
  }

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // Run compaction - entity should pass through with all blob references
  RunCompactionWithBlobGC(input_keys, input_values,
                          /* gc_cutoff_file_number */ 5,
                          /* min_blob_size */ 100, &output_keys,
                          &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  ParsedInternalKey ikey;
  ASSERT_OK(ParseInternalKey(output_keys[0], &ikey, true));
  ASSERT_EQ(kTypeWideColumnEntity, ikey.type);

  // Verify the entity structure
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns_out));

  // All three columns should exist
  ASSERT_EQ(3, output_columns.size());
  ASSERT_EQ("col1", output_columns[0].name().ToString());
  ASSERT_EQ("col2", output_columns[1].name().ToString());
  ASSERT_EQ("col3", output_columns[2].name().ToString());

  // All three blob references should be preserved
  ASSERT_EQ(3, blob_columns_out.size());

  // Verify each blob index
  ASSERT_EQ(0, blob_columns_out[0].first);
  ASSERT_EQ(2, blob_columns_out[0].second.file_number());
  ASSERT_EQ(100, blob_columns_out[0].second.offset());
  ASSERT_EQ(500, blob_columns_out[0].second.size());

  ASSERT_EQ(1, blob_columns_out[1].first);
  ASSERT_EQ(8, blob_columns_out[1].second.file_number());
  ASSERT_EQ(200, blob_columns_out[1].second.offset());
  ASSERT_EQ(600, blob_columns_out[1].second.size());

  ASSERT_EQ(2, blob_columns_out[2].first);
  ASSERT_EQ(3, blob_columns_out[2].second.file_number());
  ASSERT_EQ(300, blob_columns_out[2].second.offset());
  ASSERT_EQ(700, blob_columns_out[2].second.size());
}

// Test 4: Verify that entities with small blob values (that could be inlined
// if actually GC'd) are correctly preserved with their blob indices.
TEST_F(WideColumnEntityBlobGCTest, WideColumnEntityBlobGCToInline) {
  const std::string blob_value = std::string(50, 'X');  // Small value

  // Note: Column names must be sorted alphabetically
  std::vector<std::pair<std::string, std::string>> columns = {
      {"inline", "regular"}, {"small_blob", blob_value}};

  // Create entity with blob index for column 1 (small_blob)
  std::string entity = CreateEntityWithBlobIndices(
      columns, {{1, /* file_number */ 1, /* offset */ 100, /* size */ 50}});

  {
    bool hb = false;
    ASSERT_OK(WideColumnSerialization::HasBlobColumns(Slice(entity), hb));
    ASSERT_TRUE(hb);
  }

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity};

  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;

  // High min_blob_size - in a real GC scenario, fetched values would be inlined
  RunCompactionWithBlobGC(input_keys, input_values,
                          /* gc_cutoff_file_number */ 5,
                          /* min_blob_size */ 1000,  // High threshold
                          &output_keys, &output_values);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());

  ParsedInternalKey ikey;
  ASSERT_OK(ParseInternalKey(output_keys[0], &ikey, true));
  ASSERT_EQ(kTypeWideColumnEntity, ikey.type);

  // Verify entity structure
  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> blob_columns_out;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &blob_columns_out));

  // Both columns should exist
  ASSERT_EQ(2, output_columns.size());
  ASSERT_EQ("inline", output_columns[0].name().ToString());
  ASSERT_EQ("regular", output_columns[0].value().ToString());
  ASSERT_EQ("small_blob", output_columns[1].name().ToString());

  // Blob reference should be preserved (no GC happened)
  ASSERT_EQ(1, blob_columns_out.size());
  ASSERT_EQ(1, blob_columns_out[0].first);  // Column index 1 (small_blob)
  ASSERT_EQ(1, blob_columns_out[0].second.file_number());
  ASSERT_EQ(100, blob_columns_out[0].second.offset());
  ASSERT_EQ(50, blob_columns_out[0].second.size());
}

// Test that CompactionBlobResolver handles nullptr blob_fetcher gracefully
TEST_F(WideColumnEntityBlobGCTest, BlobResolverNullFetcher) {
  // Create a resolver with nullptr blob_fetcher
  std::vector<WideColumn> columns{{"blob_col", "blob_ref_placeholder"},
                                  {"inline_col", "value1"}};

  BlobIndex blob_idx;
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, 1 /* file_number */, 0 /* offset */,
                        100 /* size */, kNoCompression);
  Slice blob_slice(encoded);
  ASSERT_OK(blob_idx.DecodeFrom(blob_slice));

  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  blob_columns.emplace_back(0, blob_idx);

  CompactionBlobResolver resolver;
  resolver.Init(nullptr /* blob_fetcher */, nullptr /* prefetch_buffers */,
                nullptr /* iter_stats */);
  resolver.Reset(Slice("test_key"), &columns, &blob_columns);

  // IsBlobColumn should work without blob_fetcher
  ASSERT_TRUE(resolver.IsBlobColumn(0));
  ASSERT_FALSE(resolver.IsBlobColumn(1));
  ASSERT_EQ(resolver.NumColumns(), 2);

  // ResolveColumn on a blob column with null fetcher should return error
  Slice resolved_value;
  Status s = resolver.ResolveColumn(0, &resolved_value);
  ASSERT_TRUE(s.IsNotSupported());

  // ResolveColumn on a non-blob column should still work.
  ASSERT_OK(resolver.ResolveColumn(1, &resolved_value));
  ASSERT_EQ("value1", resolved_value.ToString());

  // ResolveColumns uses the same behavior as repeated ResolveColumn calls.
  std::vector<Slice> resolved_values;
  ASSERT_OK(resolver.ResolveColumns({1}, &resolved_values));
  ASSERT_EQ(1U, resolved_values.size());
  ASSERT_EQ("value1", resolved_values[0].ToString());

  s = resolver.ResolveColumns({1, 0}, &resolved_values);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(resolved_values.empty());
}

// A compaction filter that always returns kKeep (no changes).
// Supports FilterV4 to enable lazy blob loading path.
class KeepAllCompactionFilter : public CompactionFilter {
 public:
  Decision FilterV2(int /*level*/, const Slice& /*key*/, ValueType /*type*/,
                    const Slice& /*existing_value*/, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    return Decision::kKeep;
  }
  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "KeepAllCompactionFilter"; }
};

class DropKeyKeepRestCompactionFilter : public CompactionFilter {
 public:
  explicit DropKeyKeepRestCompactionFilter(std::string dropped_key)
      : dropped_key_(std::move(dropped_key)) {}

  Decision FilterV4(
      int /*level*/, const Slice& key, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* /*existing_columns*/,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* /*blob_resolver*/) const override {
    if (value_type == ValueType::kWideColumnEntity && key == dropped_key_) {
      return Decision::kRemove;
    }
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override {
    return "DropKeyKeepRestCompactionFilter";
  }

 private:
  std::string dropped_key_;
};

// Test that when a compaction filter is active and returns kKeep for a
// wide column entity with blob columns, PrepareOutput skips redundant
// deserialization (since InvokeFilterIfNeeded already deserialized it).
TEST_F(WideColumnEntityBlobGCTest,
       EntityDeserializationSkippedAfterFilterKeep) {
  // Create a V2 entity with a blob column
  std::vector<std::pair<std::string, std::string>> columns = {
      {"col1", "value1"}, {"col2", std::string(100, 'X')}};

  std::string entity_value = CreateEntityWithBlobIndices(
      columns,
      {{1 /*col_idx*/, 5 /*file_number*/, 0 /*offset*/, 100 /*size*/}});

  std::vector<std::string> input_keys = {
      test::KeyStr("key1", 10, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {entity_value};

  // Track sync point hits
  int deserialize_count = 0;
  int skip_count = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput:DeserializeEntity",
      [&](void*) { deserialize_count++; });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput:SkipDeserializeEntity",
      [&](void*) { skip_count++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Set up compaction with a kKeep filter
  KeepAllCompactionFilter keep_filter;

  const Comparator* cmp = BytewiseComparator();
  InternalKeyComparator icmp(cmp);
  std::vector<SequenceNumber> snapshots;
  std::atomic<bool> shutting_down{false};
  const std::atomic<bool> manual_compaction_canceled{false};

  MergeHelper merge_helper(Env::Default(), cmp, nullptr, nullptr, nullptr,
                           false, 0, nullptr, 0, nullptr, &shutting_down);

  auto fake_compaction = std::make_unique<FakeCompactionWithBlobGC>();

  std::unique_ptr<CompactionRangeDelAggregator> range_del_agg(
      new CompactionRangeDelAggregator(&icmp, snapshots));

  std::unique_ptr<VectorIterator> iter(
      new VectorIterator(input_keys, input_values, &icmp));
  iter->SeekToFirst();

  CompactionIterator c_iter(
      iter.get(), cmp, &merge_helper, kMaxSequenceNumber, &snapshots,
      kMaxSequenceNumber, kMaxSequenceNumber, kMaxSequenceNumber, nullptr,
      Env::Default(), false, range_del_agg.get(), nullptr /*blob_file_builder*/,
      true, true, manual_compaction_canceled, std::move(fake_compaction), false,
      &keep_filter, &shutting_down);

  c_iter.SeekToFirst();
  ASSERT_TRUE(c_iter.Valid());
  ASSERT_OK(c_iter.status());

  // With the fix: InvokeFilterIfNeeded deserializes the entity, and
  // PrepareOutput skips re-deserialization (skip_count == 1).
  // Without the fix: PrepareOutput would deserialize again
  // (deserialize_count == 1).
  ASSERT_EQ(deserialize_count, 0);
  ASSERT_EQ(skip_count, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Test that state from a dropped blob-backed entity does not leak into the
// next entity processed within the same NextFromInput() loop iteration.
TEST_F(WideColumnEntityBlobGCTest,
       EntityDeserializedStateResetAfterDroppedEntity) {
  const std::string dropped_key = "drop_key";
  const std::string kept_key = "keep_key";
  const std::string kept_large_value(128, 'K');

  const std::string dropped_entity = CreateEntityWithBlobIndices(
      {{"alpha", "small"}, {"zeta", std::string(128, 'D')}},
      {{1 /*col_idx*/, 5 /*file_number*/, 0 /*offset*/, 128 /*size*/}});
  const std::string kept_entity = CreateEntityWithBlobIndices(
      {{"alpha", kept_large_value}, {"zeta", "inline"}}, {});

  std::vector<std::string> input_keys = {
      test::KeyStr(dropped_key, 10, kTypeWideColumnEntity),
      test::KeyStr(kept_key, 9, kTypeWideColumnEntity)};
  std::vector<std::string> input_values = {dropped_entity, kept_entity};

  int deserialize_count = 0;
  int skip_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput:DeserializeEntity",
      [&](void*) { ++deserialize_count; });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput:SkipDeserializeEntity",
      [&](void*) { ++skip_count; });
  SyncPoint::GetInstance()->EnableProcessing();

  DropKeyKeepRestCompactionFilter filter(dropped_key);
  std::vector<std::string> output_keys;
  std::vector<std::string> output_values;
  RunCompactionWithBlobGC(input_keys, input_values,
                          /*gc_cutoff_file_number=*/5,
                          /*min_blob_size=*/100, &output_keys, &output_values,
                          &filter);

  ASSERT_EQ(1, output_keys.size());
  ASSERT_EQ(1, output_values.size());
  ASSERT_EQ(deserialize_count, 1);
  ASSERT_EQ(skip_count, 0);

  ParsedInternalKey ikey;
  ASSERT_OK(ParseInternalKey(output_keys[0], &ikey, true));
  ASSERT_EQ(ikey.user_key.ToString(), kept_key);
  ASSERT_EQ(ikey.type, kTypeWideColumnEntity);

  Slice value_slice(output_values[0]);
  std::vector<WideColumn> output_columns;
  std::vector<std::pair<size_t, BlobIndex>> output_blob_columns;
  ASSERT_OK(WideColumnSerialization::Deserialize(value_slice, output_columns,
                                                 &output_blob_columns));

  ASSERT_EQ(2, output_columns.size());
  ASSERT_EQ("alpha", output_columns[0].name().ToString());
  ASSERT_EQ("zeta", output_columns[1].name().ToString());
  ASSERT_EQ("inline", output_columns[1].value().ToString());
  ASSERT_EQ(1, output_blob_columns.size());
  ASSERT_EQ(0U, output_blob_columns[0].first);
  ASSERT_EQ(kept_large_value.size(), output_blob_columns[0].second.size());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
