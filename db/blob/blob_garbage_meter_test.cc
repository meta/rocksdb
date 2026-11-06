//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_garbage_meter.h"

#include <string>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blog/blog_format.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_serialization.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

BlobIndex MakeBlobIndex(uint64_t file_number, uint64_t offset, uint64_t size) {
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, file_number, offset, size, kNoCompression);
  BlobIndex blob_index;
  EXPECT_OK(blob_index.DecodeFrom(encoded));
  return blob_index;
}

}  // namespace

TEST(BlobGarbageMeterTest, MeasureGarbage) {
  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false);

  struct BlobDescriptor {
    std::string user_key;
    uint64_t blob_file_number;
    uint64_t offset;
    uint64_t size;
    CompressionType compression_type;
    bool has_in_flow;
    bool has_out_flow;

    uint64_t GetExpectedBytes() const {
      return size +
             BlobLogRecord::CalculateAdjustmentForRecordHeader(user_key.size());
    }
  };

  // Note: blob file 4 has the same inflow and outflow and hence no additional
  // garbage. Blob file 5 has less outflow than inflow and thus it does have
  // additional garbage. Blob file 6 is a newly written file (i.e. no inflow,
  // only outflow) and is thus not tracked by the meter.
  std::vector<BlobDescriptor> blobs{
      {"key", 4, 1234, 555, kLZ4Compression, true, true},
      {"other_key", 4, 6789, 101010, kLZ4Compression, true, true},
      {"yet_another_key", 5, 22222, 3456, kLZ4Compression, true, true},
      {"foo_key", 5, 77777, 8888, kLZ4Compression, true, true},
      {"bar_key", 5, 999999, 1212, kLZ4Compression, true, false},
      {"baz_key", 5, 1234567, 890, kLZ4Compression, true, false},
      {"new_key", 6, 7777, 9999, kNoCompression, false, true}};

  for (const auto& blob : blobs) {
    constexpr SequenceNumber seq = 123;
    const InternalKey key(blob.user_key, seq, kTypeBlobIndex);
    const Slice key_slice = key.Encode();

    std::string value;
    BlobIndex::EncodeBlob(&value, blob.blob_file_number, blob.offset, blob.size,
                          blob.compression_type);
    const Slice value_slice(value);

    if (blob.has_in_flow) {
      ASSERT_OK(blob_garbage_meter.ProcessInFlow(key_slice, value_slice));
    }
    if (blob.has_out_flow) {
      ASSERT_OK(blob_garbage_meter.ProcessOutFlow(key_slice, value_slice));
    }
  }

  const auto& flows = blob_garbage_meter.flows();
  ASSERT_EQ(flows.size(), 2);

  {
    const auto it = flows.find(4);
    ASSERT_NE(it, flows.end());

    const auto& flow = it->second;

    constexpr uint64_t expected_count = 2;
    const uint64_t expected_bytes =
        blobs[0].GetExpectedBytes() + blobs[1].GetExpectedBytes();

    const auto& in = flow.GetInFlow();
    ASSERT_EQ(in.GetCount(), expected_count);
    ASSERT_EQ(in.GetBytes(), expected_bytes);

    const auto& out = flow.GetOutFlow();
    ASSERT_EQ(out.GetCount(), expected_count);
    ASSERT_EQ(out.GetBytes(), expected_bytes);

    ASSERT_TRUE(flow.IsValid());
    ASSERT_FALSE(flow.HasGarbage());
  }

  {
    const auto it = flows.find(5);
    ASSERT_NE(it, flows.end());

    const auto& flow = it->second;

    const auto& in = flow.GetInFlow();

    constexpr uint64_t expected_in_count = 4;
    const uint64_t expected_in_bytes =
        blobs[2].GetExpectedBytes() + blobs[3].GetExpectedBytes() +
        blobs[4].GetExpectedBytes() + blobs[5].GetExpectedBytes();

    ASSERT_EQ(in.GetCount(), expected_in_count);
    ASSERT_EQ(in.GetBytes(), expected_in_bytes);

    const auto& out = flow.GetOutFlow();

    constexpr uint64_t expected_out_count = 2;
    const uint64_t expected_out_bytes =
        blobs[2].GetExpectedBytes() + blobs[3].GetExpectedBytes();

    ASSERT_EQ(out.GetCount(), expected_out_count);
    ASSERT_EQ(out.GetBytes(), expected_out_bytes);

    ASSERT_TRUE(flow.IsValid());
    ASSERT_TRUE(flow.HasGarbage());
    ASSERT_EQ(flow.GetGarbageCount(), expected_in_count - expected_out_count);
    ASSERT_EQ(flow.GetGarbageBytes(), expected_in_bytes - expected_out_bytes);
  }
}

TEST(BlobGarbageMeterTest, PlainValue) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeValue);
  const Slice key_slice = key.Encode();

  constexpr char value[] = "value";
  const Slice value_slice(value);

  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false);

  ASSERT_OK(blob_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_OK(blob_garbage_meter.ProcessOutFlow(key_slice, value_slice));
  ASSERT_TRUE(blob_garbage_meter.flows().empty());
}

TEST(BlobGarbageMeterTest, SameFileBlobIndex) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeBlobIndex);
  const Slice key_slice = key.Encode();

  std::string value;
  BlobIndex::EncodeBlob(&value, kCurrentFileBlobIndexFileNumber,
                        /*offset=*/123, /*size=*/456, kNoCompression);

  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false);

  ASSERT_OK(blob_garbage_meter.ProcessInFlow(key_slice, value));
  ASSERT_OK(blob_garbage_meter.ProcessOutFlow(key_slice, value));
  ASSERT_TRUE(blob_garbage_meter.flows().empty());
}

TEST(BlobGarbageMeterTest, PerFileSchemaVersionOverridesFallback) {
  BlobGarbageMeter::BlobFileSchemaVersions schema_versions{
      {4, kLegacyBlobFileSchemaVersion},
      {5, kBlogCurrentSchemaVersion},
  };
  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/true,
                                      schema_versions);

  struct BlobDescriptor {
    std::string user_key;
    uint64_t blob_file_number;
    uint64_t offset;
    uint64_t size;
    uint64_t expected_bytes;
  };

  const BlobDescriptor descriptors[] = {
      {"legacy_key", 4, 123, 555,
       555 + BlobLogRecord::CalculateAdjustmentForRecordHeader(
                 sizeof("legacy_key") - 1)},
      {"blog_key", 5, 456, 777, ComputeBlogRecordSize(777, true)},
      {"fallback_key", 6, 789, 999, ComputeBlogRecordSize(999, true)},
  };

  for (const auto& descriptor : descriptors) {
    constexpr SequenceNumber seq = 321;
    const InternalKey key(descriptor.user_key, seq, kTypeBlobIndex);
    const Slice key_slice = key.Encode();

    std::string value;
    BlobIndex::EncodeBlob(&value, descriptor.blob_file_number,
                          descriptor.offset, descriptor.size, kNoCompression);
    ASSERT_OK(blob_garbage_meter.ProcessInFlow(key_slice, value));
  }

  const auto& flows = blob_garbage_meter.flows();
  ASSERT_EQ(flows.size(), 3);
  for (const auto& descriptor : descriptors) {
    const auto it = flows.find(descriptor.blob_file_number);
    ASSERT_NE(it, flows.end());
    ASSERT_EQ(it->second.GetInFlow().GetCount(), 1);
    ASSERT_EQ(it->second.GetInFlow().GetBytes(), descriptor.expected_bytes);
  }
}

TEST(BlobGarbageMeterTest, UnsupportedSchemaVersionDiagnostic) {
  const uint8_t future_schema_version = kBlogCurrentSchemaVersion + 1;
  BlobGarbageMeter::BlobFileSchemaVersions schema_versions{
      {7, future_schema_version},
  };
  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false,
                                      schema_versions);

  constexpr SequenceNumber seq = 123;
  const InternalKey key("future_schema_key", seq, kTypeBlobIndex);
  const Slice key_slice = key.Encode();

  std::string value;
  BlobIndex::EncodeBlob(&value, /*file_number=*/7, /*offset=*/100,
                        /*size=*/200, kNoCompression);

  const Status s = blob_garbage_meter.ProcessInFlow(key_slice, value);
  ASSERT_TRUE(s.IsCorruption());

  const std::string diagnostic = s.ToString();
  ASSERT_NE(diagnostic.find("schema version " +
                            std::to_string(future_schema_version)),
            std::string::npos);
  ASSERT_NE(diagnostic.find("blob file #7"), std::string::npos);
  ASSERT_NE(
      diagnostic.find("supported schema versions are 0 (legacy) and 1.." +
                      std::to_string(kBlogCurrentSchemaVersion) + " (blog)"),
      std::string::npos);
}

TEST(BlobGarbageMeterTest, CorruptInternalKey) {
  constexpr char corrupt_key[] = "i_am_corrupt";
  const Slice key_slice(corrupt_key);

  constexpr char value[] = "value";
  const Slice value_slice(value);

  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false);

  ASSERT_NOK(blob_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_NOK(blob_garbage_meter.ProcessOutFlow(key_slice, value_slice));
}

TEST(BlobGarbageMeterTest, CorruptBlobIndex) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeBlobIndex);
  const Slice key_slice = key.Encode();

  constexpr char value[] = "i_am_not_a_blob_index";
  const Slice value_slice(value);

  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false);

  ASSERT_NOK(blob_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_NOK(blob_garbage_meter.ProcessOutFlow(key_slice, value_slice));
}

TEST(BlobGarbageMeterTest, InlinedTTLBlobIndex) {
  constexpr char user_key[] = "user_key";
  constexpr SequenceNumber seq = 123;

  const InternalKey key(user_key, seq, kTypeBlobIndex);
  const Slice key_slice = key.Encode();

  constexpr uint64_t expiration = 1234567890;
  constexpr char inlined_value[] = "inlined";

  std::string value;
  BlobIndex::EncodeInlinedTTL(&value, expiration, inlined_value);

  const Slice value_slice(value);

  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false);

  ASSERT_NOK(blob_garbage_meter.ProcessInFlow(key_slice, value_slice));
  ASSERT_NOK(blob_garbage_meter.ProcessOutFlow(key_slice, value_slice));
}

TEST(BlobGarbageMeterTest, WideColumnEntity) {
  constexpr char user_key[] = "entity_key";
  constexpr SequenceNumber seq = 123;
  const InternalKey key(user_key, seq, kTypeWideColumnEntity);
  const Slice key_slice = key.Encode();

  const std::vector<std::pair<std::string, std::string>> columns = {
      {"", "default_inline"},
      {"blob_attr", "blob_inline"},
      {"ttl", "00000001"},
  };

  const BlobIndex default_blob = MakeBlobIndex(4, 123, 555);
  const BlobIndex attr_blob = MakeBlobIndex(5, 456, 777);

  std::string inflow_value;
  ASSERT_OK(WideColumnSerialization::SerializeV2(
      columns, {{0, default_blob}, {1, attr_blob}}, inflow_value));

  std::string outflow_value;
  ASSERT_OK(WideColumnSerialization::SerializeV2(columns, {{0, default_blob}},
                                                 outflow_value));

  BlobGarbageMeter blob_garbage_meter(/*use_blog_format=*/false);
  ASSERT_OK(blob_garbage_meter.ProcessInFlow(key_slice, inflow_value));
  ASSERT_OK(blob_garbage_meter.ProcessOutFlow(key_slice, outflow_value));

  const auto& flows = blob_garbage_meter.flows();
  ASSERT_EQ(flows.size(), 2);

  constexpr size_t kUserKeySize = sizeof(user_key) - 1;
  const uint64_t default_bytes =
      default_blob.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(kUserKeySize);
  const uint64_t attr_bytes =
      attr_blob.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(kUserKeySize);

  {
    const auto it = flows.find(default_blob.file_number());
    ASSERT_NE(it, flows.end());
    ASSERT_EQ(it->second.GetInFlow().GetCount(), 1);
    ASSERT_EQ(it->second.GetInFlow().GetBytes(), default_bytes);
    ASSERT_EQ(it->second.GetOutFlow().GetCount(), 1);
    ASSERT_EQ(it->second.GetOutFlow().GetBytes(), default_bytes);
    ASSERT_FALSE(it->second.HasGarbage());
  }

  {
    const auto it = flows.find(attr_blob.file_number());
    ASSERT_NE(it, flows.end());
    ASSERT_EQ(it->second.GetInFlow().GetCount(), 1);
    ASSERT_EQ(it->second.GetInFlow().GetBytes(), attr_bytes);
    ASSERT_EQ(it->second.GetOutFlow().GetCount(), 0);
    ASSERT_EQ(it->second.GetOutFlow().GetBytes(), 0);
    ASSERT_TRUE(it->second.HasGarbage());
    ASSERT_EQ(it->second.GetGarbageCount(), 1);
    ASSERT_EQ(it->second.GetGarbageBytes(), attr_bytes);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
