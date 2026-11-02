//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_addition.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileAdditionTest : public testing::Test {
 public:
  static void TestEncodeDecode(const BlobFileAddition& blob_file_addition) {
    std::string encoded;
    blob_file_addition.EncodeTo(&encoded);

    BlobFileAddition decoded;
    Slice input(encoded);
    ASSERT_OK(decoded.DecodeFrom(&input));

    ASSERT_EQ(blob_file_addition, decoded);
  }
};

TEST_F(BlobFileAdditionTest, Empty) {
  BlobFileAddition blob_file_addition;

  ASSERT_EQ(blob_file_addition.GetBlobFileNumber(), kInvalidBlobFileNumber);
  ASSERT_EQ(blob_file_addition.GetTotalBlobCount(), 0);
  ASSERT_EQ(blob_file_addition.GetTotalBlobBytes(), 0);
  ASSERT_TRUE(blob_file_addition.GetChecksumMethod().empty());
  ASSERT_TRUE(blob_file_addition.GetChecksumValue().empty());
  ASSERT_FALSE(blob_file_addition.IsIdentityRoot());

  TestEncodeDecode(blob_file_addition);
}

TEST_F(BlobFileAdditionTest, NonEmpty) {
  constexpr uint64_t blob_file_number = 123;
  constexpr uint64_t total_blob_count = 2;
  constexpr uint64_t total_blob_bytes = 123456;
  const std::string checksum_method("SHA1");
  const std::string checksum_value(
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd");

  BlobFileAddition blob_file_addition(blob_file_number, total_blob_count,
                                      total_blob_bytes, checksum_method,
                                      checksum_value);

  ASSERT_EQ(blob_file_addition.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_addition.GetTotalBlobCount(), total_blob_count);
  ASSERT_EQ(blob_file_addition.GetTotalBlobBytes(), total_blob_bytes);
  ASSERT_EQ(blob_file_addition.GetChecksumMethod(), checksum_method);
  ASSERT_EQ(blob_file_addition.GetChecksumValue(), checksum_value);
  ASSERT_FALSE(blob_file_addition.IsIdentityRoot());

  TestEncodeDecode(blob_file_addition);
}

TEST_F(BlobFileAdditionTest, IdentityRoot) {
  BlobFileAddition blob_file_addition(
      /*blob_file_number=*/123, /*total_blob_count=*/2,
      /*total_blob_bytes=*/456, "crc32c", "checksum",
      /*is_identity_root=*/true);
  ASSERT_TRUE(blob_file_addition.IsIdentityRoot());
  TestEncodeDecode(blob_file_addition);
}

TEST_F(BlobFileAdditionTest, DecodeErrors) {
  std::string str;
  Slice slice(str);

  BlobFileAddition blob_file_addition;

  {
    const Status s = blob_file_addition.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "blob file number"));
  }

  constexpr uint64_t blob_file_number = 123;
  PutVarint64(&str, blob_file_number);
  slice = str;

  {
    const Status s = blob_file_addition.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "total blob count"));
  }

  constexpr uint64_t total_blob_count = 4567;
  PutVarint64(&str, total_blob_count);
  slice = str;

  {
    const Status s = blob_file_addition.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "total blob bytes"));
  }

  constexpr uint64_t total_blob_bytes = 12345678;
  PutVarint64(&str, total_blob_bytes);
  slice = str;

  {
    const Status s = blob_file_addition.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "checksum method"));
  }

  constexpr char checksum_method[] = "SHA1";
  PutLengthPrefixedSlice(&str, checksum_method);
  slice = str;

  {
    const Status s = blob_file_addition.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "checksum value"));
  }

  constexpr char checksum_value[] =
      "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c\x52"
      "\x5c\xbd";
  PutLengthPrefixedSlice(&str, checksum_value);
  slice = str;

  {
    const Status s = blob_file_addition.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field tag"));
  }

  constexpr uint32_t custom_tag = 2;
  PutVarint32(&str, custom_tag);
  slice = str;

  {
    const Status s = blob_file_addition.DecodeFrom(&slice);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "custom field value"));
  }
}

TEST_F(BlobFileAdditionTest, ForwardCompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileAddition::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_compatible_tag = 2;
        PutVarint32(output, forward_compatible_tag);

        PutLengthPrefixedSlice(output, "deadbeef");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t blob_file_number = 678;
  constexpr uint64_t total_blob_count = 9999;
  constexpr uint64_t total_blob_bytes = 100000000;
  const std::string checksum_method("CRC32");
  const std::string checksum_value("\x3d\x87\xff\x57");

  BlobFileAddition blob_file_addition(blob_file_number, total_blob_count,
                                      total_blob_bytes, checksum_method,
                                      checksum_value);

  TestEncodeDecode(blob_file_addition);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileAdditionTest, ForwardIncompatibleCustomField) {
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileAddition::EncodeTo::CustomFields", [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);

        constexpr uint32_t forward_incompatible_tag = (1 << 6) + 1;
        PutVarint32(output, forward_incompatible_tag);

        PutLengthPrefixedSlice(output, "foobar");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t blob_file_number = 456;
  constexpr uint64_t total_blob_count = 100;
  constexpr uint64_t total_blob_bytes = 2000000;
  const std::string checksum_method("CRC32B");
  const std::string checksum_value("\x6d\xbd\xf2\x3a");

  BlobFileAddition blob_file_addition(blob_file_number, total_blob_count,
                                      total_blob_bytes, checksum_method,
                                      checksum_value);

  std::string encoded;
  blob_file_addition.EncodeTo(&encoded);

  BlobFileAddition decoded_blob_file_addition;
  Slice input(encoded);
  const Status s = decoded_blob_file_addition.DecodeFrom(&input);

  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "Forward incompatible"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
