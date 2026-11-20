//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/types_util.h"

#include "db/dbformat.h"
#include "port/stack_trace.h"
#include "rocksdb/types.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {
namespace {
std::string EncodeAsUint64(uint64_t v) {
  std::string dst;
  PutFixed64(&dst, v);
  return dst;
}
std::string IKey(const std::string& user_key, uint64_t seq, ValueType vt,
                 std::optional<uint64_t> timestamp) {
  std::string encoded;
  encoded.assign(user_key.data(), user_key.size());
  if (timestamp.has_value()) {
    PutFixed64(&encoded, timestamp.value());
  }
  PutFixed64(&encoded, PackSequenceAndType(seq, vt));
  return encoded;
}
}  // namespace

TEST(ParseEntryTest, InvalidInternalKey) {
  const Comparator* ucmp = BytewiseComparator();
  std::string invalid_ikey = "foo";
  Slice ikey_slice = invalid_ikey;
  ParsedEntryInfo parsed_entry;
  ASSERT_TRUE(ParseEntry(ikey_slice, ucmp, &parsed_entry).IsInvalidArgument());

  std::string ikey =
      IKey("foo", 3, ValueType::kTypeValue, /*timestamp=*/std::nullopt);
  ikey_slice = ikey;
  ASSERT_TRUE(
      ParseEntry(ikey_slice, nullptr, &parsed_entry).IsInvalidArgument());
}

TEST(ParseEntryTest, Basic) {
  const Comparator* ucmp = BytewiseComparator();
  std::string ikey =
      IKey("foo", 3, ValueType::kTypeValue, /*timestamp=*/std::nullopt);
  Slice ikey_slice = ikey;

  ParsedEntryInfo parsed_entry;
  ASSERT_OK(ParseEntry(ikey_slice, ucmp, &parsed_entry));
  ASSERT_EQ(parsed_entry.user_key, "foo");
  ASSERT_EQ(parsed_entry.timestamp, "");
  ASSERT_EQ(parsed_entry.sequence, 3);
  ASSERT_EQ(parsed_entry.type, EntryType::kEntryPut);

  ikey = IKey("bar", 5, ValueType::kTypeDeletion, /*timestamp=*/std::nullopt);
  ikey_slice = ikey;

  ASSERT_OK(ParseEntry(ikey_slice, ucmp, &parsed_entry));
  ASSERT_EQ(parsed_entry.user_key, "bar");
  ASSERT_EQ(parsed_entry.timestamp, "");
  ASSERT_EQ(parsed_entry.sequence, 5);
  ASSERT_EQ(parsed_entry.type, EntryType::kEntryDelete);
}

TEST(ParseEntryTest, UserKeyIncludesTimestamp) {
  const Comparator* ucmp = BytewiseComparatorWithU64Ts();
  std::string ikey = IKey("foo", 3, ValueType::kTypeValue, 50);
  Slice ikey_slice = ikey;

  ParsedEntryInfo parsed_entry;
  ASSERT_OK(ParseEntry(ikey_slice, ucmp, &parsed_entry));
  ASSERT_EQ(parsed_entry.user_key, "foo");
  ASSERT_EQ(parsed_entry.timestamp, EncodeAsUint64(50));
  ASSERT_EQ(parsed_entry.sequence, 3);
  ASSERT_EQ(parsed_entry.type, EntryType::kEntryPut);

  ikey = IKey("bar", 5, ValueType::kTypeDeletion, 30);
  ikey_slice = ikey;

  ASSERT_OK(ParseEntry(ikey_slice, ucmp, &parsed_entry));
  ASSERT_EQ(parsed_entry.user_key, "bar");
  ASSERT_EQ(parsed_entry.timestamp, EncodeAsUint64(30));
  ASSERT_EQ(parsed_entry.sequence, 5);
  ASSERT_EQ(parsed_entry.type, EntryType::kEntryDelete);
}

TEST(EncodeEntryTest, RoundTrip) {
  const struct {
    std::string user_key;
    SequenceNumber sequence;
    EntryType entry_type;
    ValueType value_type;
    std::optional<uint64_t> timestamp;
  } test_cases[] = {
      {"foo", 0, kEntryPut, kTypeValue, std::nullopt},
      {"bar", 3, kEntryDelete, kTypeDeletion, std::nullopt},
      {std::string("foo\0bar", 7), 17, kEntrySingleDelete, kTypeSingleDeletion,
       std::nullopt},
      {"foo", 42, kEntryMerge, kTypeMerge, std::nullopt},
      {"a", 100, kEntryRangeDeletion, kTypeRangeDeletion, std::nullopt},
      {"x", kMaxSequenceNumber, kEntryBlobIndex, kTypeBlobIndex, std::nullopt},
      {"k", 10, kEntryDeleteWithTimestamp, kTypeDeletionWithTimestamp, 50},
      {"e", 90, kEntryWideColumnEntity, kTypeWideColumnEntity, std::nullopt},
      {"t", 13, kEntryTimedPut, kTypeValuePreferredSeqno, std::nullopt},
      {"", kMaxSequenceNumber, kEntryPut, kTypeValue, std::nullopt},
      {"foo", 3, kEntryPut, kTypeValue, 30},
  };
  for (const auto& test_case : test_cases) {
    SCOPED_TRACE(test_case.entry_type);
    const std::string timestamp = test_case.timestamp.has_value()
                                      ? EncodeAsUint64(*test_case.timestamp)
                                      : std::string();
    const ParsedEntryInfo entry{test_case.user_key, timestamp,
                                test_case.sequence, test_case.entry_type};
    std::string encoded = "old contents";
    ASSERT_OK(EncodeEntry(entry, &encoded));
    ASSERT_EQ(encoded, IKey(test_case.user_key, test_case.sequence,
                            test_case.value_type, test_case.timestamp));

    const Comparator* comparator = test_case.timestamp.has_value()
                                       ? BytewiseComparatorWithU64Ts()
                                       : BytewiseComparator();
    ParsedEntryInfo parsed;
    ASSERT_OK(ParseEntry(encoded, comparator, &parsed));
    ASSERT_EQ(parsed.user_key, entry.user_key);
    ASSERT_EQ(parsed.timestamp, entry.timestamp);
    ASSERT_EQ(parsed.sequence, entry.sequence);
    ASSERT_EQ(parsed.type, entry.type);
  }
}

TEST(EncodeEntryTest, Buffer) {
  const ParsedEntryInfo entry{"foo", Slice(), 42, kEntryPut};
  std::string buffer(GetEncodedEntrySize(entry), '\0');
  ASSERT_OK(EncodeEntry(entry, buffer.data()));
  ASSERT_EQ(buffer, IKey("foo", 42, kTypeValue, std::nullopt));
}

TEST(EncodeEntryTest, InvalidEntry) {
  ParsedEntryInfo entry{"foo", Slice(), 3, kEntryOther};
  std::string encoded = "unchanged";
  for (EntryType type :
       {kEntryOther, static_cast<EntryType>(kEntryOther + 1)}) {
    SCOPED_TRACE(type);
    entry.type = type;
    ASSERT_TRUE(EncodeEntry(entry, &encoded).IsInvalidArgument());
    ASSERT_EQ(encoded, "unchanged");
  }

  entry.type = kEntryPut;
  entry.sequence = kMaxSequenceNumber + 1;
  ASSERT_TRUE(EncodeEntry(entry, &encoded).IsInvalidArgument());
  ASSERT_EQ(encoded, "unchanged");
}

TEST(SequenceAndTypeTest, RoundTrip) {
  const struct {
    EntryType entry_type;
    ValueType value_type;
  } test_cases[] = {
      {kEntryPut, kTypeValue},
      {kEntryDelete, kTypeDeletion},
      {kEntrySingleDelete, kTypeSingleDeletion},
      {kEntryMerge, kTypeMerge},
      {kEntryRangeDeletion, kTypeRangeDeletion},
      {kEntryBlobIndex, kTypeBlobIndex},
      {kEntryDeleteWithTimestamp, kTypeDeletionWithTimestamp},
      {kEntryWideColumnEntity, kTypeWideColumnEntity},
      {kEntryTimedPut, kTypeValuePreferredSeqno},
  };
  for (const auto& test_case : test_cases) {
    SCOPED_TRACE(test_case.entry_type);
    for (uint64_t sequence :
         {uint64_t{0}, uint64_t{0x123456789abcde}, kMaxSequenceNumber}) {
      SCOPED_TRACE(sequence);
      uint64_t packed = 0;
      ASSERT_OK(PackSequenceAndType(sequence, test_case.entry_type, &packed));
      ASSERT_EQ(packed, PackSequenceAndType(sequence, test_case.value_type));

      uint64_t decoded_sequence = 0;
      EntryType decoded_type = kEntryOther;
      UnPackSequenceAndType(packed, &decoded_sequence, &decoded_type);
      ASSERT_EQ(decoded_sequence, sequence);
      ASSERT_EQ(decoded_type, test_case.entry_type);
    }
  }
}

TEST(SequenceAndTypeTest, InvalidEntry) {
  uint64_t packed = 123;
  for (EntryType type :
       {kEntryOther, static_cast<EntryType>(kEntryOther + 1)}) {
    SCOPED_TRACE(type);
    ASSERT_TRUE(PackSequenceAndType(3, type, &packed).IsInvalidArgument());
    ASSERT_EQ(packed, 123);
  }
  ASSERT_TRUE(PackSequenceAndType(kMaxSequenceNumber + 1, kEntryPut, &packed)
                  .IsInvalidArgument());
  ASSERT_EQ(packed, 123);
}

TEST(SequenceAndTypeTest, UnrepresentedValueType) {
  for (uint64_t packed :
       {uint64_t{0x123456789abcde03}, uint64_t{0x123456789abcdeff}}) {
    uint64_t sequence = 0;
    EntryType type = kEntryPut;
    UnPackSequenceAndType(packed, &sequence, &type);
    ASSERT_EQ(sequence, 0x123456789abcde);
    ASSERT_EQ(type, kEntryOther);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
