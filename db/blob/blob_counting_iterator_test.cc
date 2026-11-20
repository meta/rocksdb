//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_counting_iterator.h"

#include <string>
#include <vector>

#include "db/blob/blob_garbage_meter.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/dbformat.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/vector_iterator.h"

namespace ROCKSDB_NAMESPACE {

void CheckInFlow(const BlobGarbageMeter& blob_garbage_meter,
                 uint64_t blob_file_number, uint64_t count, uint64_t bytes) {
  const auto& flows = blob_garbage_meter.flows();

  const auto it = flows.find(blob_file_number);
  if (it == flows.end()) {
    ASSERT_EQ(count, 0);
    ASSERT_EQ(bytes, 0);
    return;
  }

  const auto& in = it->second.GetInFlow();

  ASSERT_EQ(in.GetCount(), count);
  ASSERT_EQ(in.GetBytes(), bytes);
}

TEST(BlobCountingIteratorTest, CountBlobs) {
  // Note: the input consists of three key-values: two are blob references to
  // different blob files, while the third one is a plain value.
  constexpr char user_key0[] = "key0";
  constexpr char user_key1[] = "key1";
  constexpr char user_key2[] = "key2";

  const std::vector<std::string> keys{
      test::KeyStr(user_key0, 1, kTypeBlobIndex),
      test::KeyStr(user_key1, 2, kTypeBlobIndex),
      test::KeyStr(user_key2, 3, kTypeValue)};

  constexpr uint64_t first_blob_file_number = 4;
  constexpr uint64_t first_offset = 1000;
  constexpr uint64_t first_size = 2000;

  std::string first_blob_index;
  BlobIndex::EncodeBlob(&first_blob_index, first_blob_file_number, first_offset,
                        first_size, kNoCompression);

  constexpr uint64_t second_blob_file_number = 6;
  constexpr uint64_t second_offset = 2000;
  constexpr uint64_t second_size = 4000;

  std::string second_blob_index;
  BlobIndex::EncodeBlob(&second_blob_index, second_blob_file_number,
                        second_offset, second_size, kNoCompression);

  const std::vector<std::string> values{first_blob_index, second_blob_index,
                                        "raw_value"};

  assert(keys.size() == values.size());

  VectorIterator input(keys, values);
  BlobGarbageMeter blob_garbage_meter;

  BlobCountingIterator blob_counter(&input, &blob_garbage_meter,
                                    /*track_current_key_inflow=*/false);

  constexpr uint64_t first_expected_bytes =
      first_size +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(user_key0) - 1);
  constexpr uint64_t second_expected_bytes =
      second_size +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(sizeof(user_key1) - 1);

  // Call SeekToFirst and iterate forward
  blob_counter.SeekToFirst();
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[0]);
  ASSERT_EQ(blob_counter.user_key(), user_key0);
  ASSERT_EQ(blob_counter.value(), values[0]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 1,
              first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 0, 0);

  blob_counter.Next();
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[1]);
  ASSERT_EQ(blob_counter.user_key(), user_key1);
  ASSERT_EQ(blob_counter.value(), values[1]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 1,
              first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 1,
              second_expected_bytes);

  blob_counter.Next();
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[2]);
  ASSERT_EQ(blob_counter.user_key(), user_key2);
  ASSERT_EQ(blob_counter.value(), values[2]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 1,
              first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 1,
              second_expected_bytes);

  blob_counter.Next();
  ASSERT_FALSE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 1,
              first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 1,
              second_expected_bytes);

  // Do it again using NextAndGetResult
  blob_counter.SeekToFirst();
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[0]);
  ASSERT_EQ(blob_counter.user_key(), user_key0);
  ASSERT_EQ(blob_counter.value(), values[0]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 1,
              second_expected_bytes);

  {
    IterateResult result;
    ASSERT_TRUE(blob_counter.NextAndGetResult(&result));
    ASSERT_EQ(result.key, keys[1]);
    ASSERT_EQ(blob_counter.user_key(), user_key1);
    ASSERT_TRUE(blob_counter.Valid());
    ASSERT_OK(blob_counter.status());
    ASSERT_EQ(blob_counter.key(), keys[1]);
    ASSERT_EQ(blob_counter.value(), values[1]);
    CheckInFlow(blob_garbage_meter, first_blob_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(blob_garbage_meter, second_blob_file_number, 2,
                2 * second_expected_bytes);
  }

  {
    IterateResult result;
    ASSERT_TRUE(blob_counter.NextAndGetResult(&result));
    ASSERT_EQ(result.key, keys[2]);
    ASSERT_EQ(blob_counter.user_key(), user_key2);
    ASSERT_TRUE(blob_counter.Valid());
    ASSERT_OK(blob_counter.status());
    ASSERT_EQ(blob_counter.key(), keys[2]);
    ASSERT_EQ(blob_counter.value(), values[2]);
    CheckInFlow(blob_garbage_meter, first_blob_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(blob_garbage_meter, second_blob_file_number, 2,
                2 * second_expected_bytes);
  }

  {
    IterateResult result;
    ASSERT_FALSE(blob_counter.NextAndGetResult(&result));
    ASSERT_FALSE(blob_counter.Valid());
    ASSERT_OK(blob_counter.status());
    CheckInFlow(blob_garbage_meter, first_blob_file_number, 2,
                2 * first_expected_bytes);
    CheckInFlow(blob_garbage_meter, second_blob_file_number, 2,
                2 * second_expected_bytes);
  }

  // Call SeekToLast and iterate backward
  blob_counter.SeekToLast();
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[2]);
  ASSERT_EQ(blob_counter.user_key(), user_key2);
  ASSERT_EQ(blob_counter.value(), values[2]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 2,
              2 * second_expected_bytes);

  blob_counter.Prev();
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[1]);
  ASSERT_EQ(blob_counter.user_key(), user_key1);
  ASSERT_EQ(blob_counter.value(), values[1]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 2,
              2 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 3,
              3 * second_expected_bytes);

  blob_counter.Prev();
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[0]);
  ASSERT_EQ(blob_counter.user_key(), user_key0);
  ASSERT_EQ(blob_counter.value(), values[0]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 3,
              3 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 3,
              3 * second_expected_bytes);

  blob_counter.Prev();
  ASSERT_FALSE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 3,
              3 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 3,
              3 * second_expected_bytes);

  // Call Seek for all keys (plus one that's greater than all of them)
  blob_counter.Seek(keys[0]);
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[0]);
  ASSERT_EQ(blob_counter.user_key(), user_key0);
  ASSERT_EQ(blob_counter.value(), values[0]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 3,
              3 * second_expected_bytes);

  blob_counter.Seek(keys[1]);
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[1]);
  ASSERT_EQ(blob_counter.user_key(), user_key1);
  ASSERT_EQ(blob_counter.value(), values[1]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 4,
              4 * second_expected_bytes);

  blob_counter.Seek(keys[2]);
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[2]);
  ASSERT_EQ(blob_counter.user_key(), user_key2);
  ASSERT_EQ(blob_counter.value(), values[2]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 4,
              4 * second_expected_bytes);

  blob_counter.Seek("zzz");
  ASSERT_FALSE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 4,
              4 * second_expected_bytes);

  // Call SeekForPrev for all keys (plus one that's less than all of them)
  blob_counter.SeekForPrev("aaa");
  ASSERT_FALSE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 4,
              4 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 4,
              4 * second_expected_bytes);

  blob_counter.SeekForPrev(keys[0]);
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[0]);
  ASSERT_EQ(blob_counter.user_key(), user_key0);
  ASSERT_EQ(blob_counter.value(), values[0]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 4,
              4 * second_expected_bytes);

  blob_counter.SeekForPrev(keys[1]);
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[1]);
  ASSERT_EQ(blob_counter.user_key(), user_key1);
  ASSERT_EQ(blob_counter.value(), values[1]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 5,
              5 * second_expected_bytes);

  blob_counter.SeekForPrev(keys[2]);
  ASSERT_TRUE(blob_counter.Valid());
  ASSERT_OK(blob_counter.status());
  ASSERT_EQ(blob_counter.key(), keys[2]);
  ASSERT_EQ(blob_counter.user_key(), user_key2);
  ASSERT_EQ(blob_counter.value(), values[2]);
  CheckInFlow(blob_garbage_meter, first_blob_file_number, 5,
              5 * first_expected_bytes);
  CheckInFlow(blob_garbage_meter, second_blob_file_number, 5,
              5 * second_expected_bytes);
}

TEST(BlobCountingIteratorTest, TracksCurrentKeyOnlyForResumableCompaction) {
  constexpr size_t kNumEntries = 1000;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(kNumEntries);
  values.reserve(kNumEntries);
  for (size_t i = 0; i < kNumEntries; ++i) {
    keys.push_back(
        test::KeyStr("key" + std::to_string(i), i + 1, kTypeBlobIndex));
    BlobIndex::EncodeBlob(&values.emplace_back(), /*file_number=*/7,
                          /*offset=*/i * 100, /*size=*/100, kNoCompression);
  }

  for (const bool track_current_key : {false, true}) {
    VectorIterator input(keys, values);
    BlobGarbageMeter blob_garbage_meter;
    size_t aggregate_parse_count = 0;
    size_t current_key_parse_count = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "BlobCountingIterator::UpdateAndCountBlobIfNeeded:AggregateInflow",
        [&](void*) { ++aggregate_parse_count; });
    SyncPoint::GetInstance()->SetCallBack(
        "BlobCountingIterator::UpdateAndCountBlobIfNeeded:CurrentKeyInflow",
        [&](void*) { ++current_key_parse_count; });
    SyncPoint::GetInstance()->EnableProcessing();

    BlobCountingIterator blob_counter(&input, &blob_garbage_meter,
                                      track_current_key);
    EXPECT_EQ(blob_counter.TracksCurrentKeyInflow(), track_current_key);
    const BlobGarbageMeter* current_key_meter =
        track_current_key ? &blob_counter.GetCurrentKeyInflow() : nullptr;
    for (blob_counter.SeekToFirst(); blob_counter.Valid();
         blob_counter.Next()) {
      if (track_current_key) {
        // This checks reuse of the optional meter object, not allocations made
        // by its internal map while processing each key.
        EXPECT_EQ(&blob_counter.GetCurrentKeyInflow(), current_key_meter);
      }
    }

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_OK(blob_counter.status());
    EXPECT_EQ(aggregate_parse_count, kNumEntries);
    EXPECT_EQ(current_key_parse_count, track_current_key ? kNumEntries : 0);
    const auto flow = blob_garbage_meter.flows().find(7);
    ASSERT_NE(flow, blob_garbage_meter.flows().end());
    EXPECT_EQ(flow->second.GetInFlow().GetCount(), kNumEntries);
  }
}

TEST(BlobCountingIteratorTest, CorruptBlobIndex) {
  const std::vector<std::string> keys{
      test::KeyStr("user_key", 1, kTypeBlobIndex)};
  const std::vector<std::string> values{"i_am_not_a_blob_index"};

  assert(keys.size() == values.size());

  VectorIterator input(keys, values);
  BlobGarbageMeter blob_garbage_meter;

  BlobCountingIterator blob_counter(&input, &blob_garbage_meter,
                                    /*track_current_key_inflow=*/false);

  blob_counter.SeekToFirst();
  ASSERT_FALSE(blob_counter.Valid());
  ASSERT_NOK(blob_counter.status());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
