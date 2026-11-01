//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/blob/blob_index.h"
#include "rocksdb/advanced_options.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

static void TestEncodeDecode(const VersionEdit& edit) {
  // Encoding one `VersionEdit` and decoding it again should result in the
  // exact same `VersionEdit`. However, a special handling is applied to file
  // boundaries: `FileMetaData.smallest`, `FileMetaData.largest` when
  // user-defined timestamps should not be persisted. In that scenario, this
  // invariant does not hold. We disable this scenario in this util method to
  // enable all other test cases continue to verify this invariant, while the
  // special case is separately covered in test
  // `EncodeDecodeNewFile4HandleFileBoundary`.
  std::string encoded;
  std::string encoded2;
  edit.EncodeTo(&encoded, 0 /* ts_sz */);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2, 0 /* ts_sz */);
  ASSERT_EQ(encoded, encoded2);
}

static void TestOptionsFileNumberField(const char* field_name,
                                       uint64_t file_number,
                                       void (VersionEdit::*set)(uint64_t),
                                       bool (VersionEdit::*has)() const,
                                       uint64_t (VersionEdit::*get)() const) {
  SCOPED_TRACE(field_name);
  VersionEdit edit;
  ASSERT_FALSE((edit.*has)());
  (edit.*set)(file_number);

  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded));

  VersionEdit decoded;
  ASSERT_OK(decoded.DecodeFrom(encoded));
  ASSERT_TRUE((decoded.*has)());
  ASSERT_EQ(file_number, (decoded.*get)());
  ASSERT_TRUE(decoded.ShouldEmitPerColumnFamilyRecoveryEdit(0));
  const std::string expected =
      std::string(field_name) + ": " + std::to_string(file_number);
  ASSERT_NE(std::string::npos, decoded.DebugString().find(expected));
  const std::string expected_json =
      "\"" + std::string(field_name) + "\": " + std::to_string(file_number);
  ASSERT_NE(std::string::npos, decoded.DebugJSON(1).find(expected_json));
}

class VersionEditTest : public testing::Test {};

TEST_F(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;
  static const uint32_t kBig32Bit = 1ull << 30;

  VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig32Bit + 400 + i, 0,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion),
                 kBig + 500 + i, kBig + 600 + i, false, Temperature::kUnknown,
                 kInvalidBlobFileNumber, 888, 678,
                 kBig + 300 + i /* epoch_number */, "234", "crc32c",
                 kNullUniqueId64x2, 0, 0, true);
    edit.DeleteFile(4, kBig + 700 + i);
  }

  edit.SetComparatorName("foo");
  edit.SetPersistUserDefinedTimestamps(true);
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, EncodeDecodeNewFile4) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               300 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false, Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               301 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, false);
  edit.AddFile(5, 302, 0, 100, InternalKey("foo", kBig + 502, kTypeValue),
               InternalKey("zoo", kBig + 602, kTypeDeletion), kBig + 502,
               kBig + 602, true, Temperature::kUnknown, kInvalidBlobFileNumber,
               666, 888, 302 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);
  edit.AddFile(5, 303, 0, 100, InternalKey("foo", kBig + 503, kTypeBlobIndex),
               InternalKey("zoo", kBig + 603, kTypeBlobIndex), kBig + 503,
               kBig + 603, true, Temperature::kUnknown, 1001,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               303 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);

  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetPersistUserDefinedTimestamps(false);
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);

  std::string encoded;
  std::string encoded2;
  edit.EncodeTo(&encoded, 0 /* ts_sz */);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_FALSE(new_files[1].second.marked_for_compaction);
  ASSERT_TRUE(new_files[2].second.marked_for_compaction);
  ASSERT_TRUE(new_files[3].second.marked_for_compaction);
  ASSERT_EQ(3u, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3u, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(0u, new_files[2].second.fd.GetPathId());
  ASSERT_EQ(0u, new_files[3].second.fd.GetPathId());
  ASSERT_EQ(kInvalidBlobFileNumber,
            new_files[0].second.oldest_blob_file_number);
  ASSERT_EQ(kInvalidBlobFileNumber,
            new_files[1].second.oldest_blob_file_number);
  ASSERT_EQ(kInvalidBlobFileNumber,
            new_files[2].second.oldest_blob_file_number);
  ASSERT_EQ(1001, new_files[3].second.oldest_blob_file_number);
  ASSERT_TRUE(new_files[0].second.user_defined_timestamps_persisted);
  ASSERT_FALSE(new_files[1].second.user_defined_timestamps_persisted);
  ASSERT_TRUE(new_files[2].second.user_defined_timestamps_persisted);
  ASSERT_TRUE(new_files[3].second.user_defined_timestamps_persisted);
  ASSERT_FALSE(parsed.GetPersistUserDefinedTimestamps());
}

TEST_F(VersionEditTest, EncodeDecodeNewFile4HandleFileBoundary) {
  static const uint64_t kBig = 1ull << 50;
  size_t ts_sz = 16;
  static std::string min_ts(ts_sz, static_cast<unsigned char>(0));
  VersionEdit edit;
  std::string smallest = "foo";
  std::string largest = "zoo";
  // In real manifest writing scenarios, one `VersionEdit` should not contain
  // files with different `user_defined_timestamps_persisted` flag value.
  // This is just for testing file boundaries handling w.r.t persisting user
  // defined timestamps during `VersionEdit` encoding.
  edit.AddFile(
      3, 300, 3, 100, InternalKey(smallest + min_ts, kBig + 500, kTypeValue),
      InternalKey(largest + min_ts, kBig + 600, kTypeDeletion), kBig + 500,
      kBig + 600, true, Temperature::kUnknown, kInvalidBlobFileNumber,
      kUnknownOldestAncestorTime, kUnknownFileCreationTime,
      300 /* epoch_number */, kUnknownFileChecksum,
      kUnknownFileChecksumFuncName, kNullUniqueId64x2,
      0 /* compensated_range_deletion_size */, 0 /* tail_size */,
      false /* user_defined_timestamps_persisted */);
  edit.AddFile(3, 300, 3, 100,
               InternalKey(smallest + min_ts, kBig + 500, kTypeValue),
               InternalKey(largest + min_ts, kBig + 600, kTypeDeletion),
               kBig + 500, kBig + 600, true, Temperature::kUnknown,
               kInvalidBlobFileNumber, kUnknownOldestAncestorTime,
               kUnknownFileCreationTime, 300 /* epoch_number */,
               kUnknownFileChecksum, kUnknownFileChecksumFuncName,
               kNullUniqueId64x2, 0 /* compensated_range_deletion_size */,
               0 /* tail_size */, true /* user_defined_timestamps_persisted */);

  std::string encoded;
  edit.EncodeTo(&encoded, ts_sz);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files.size() == 2);
  ASSERT_FALSE(new_files[0].second.user_defined_timestamps_persisted);
  // First file's boundaries do not contain user-defined timestamps.
  ASSERT_EQ(InternalKey(smallest, kBig + 500, kTypeValue).Encode(),
            new_files[0].second.smallest.Encode());
  ASSERT_EQ(InternalKey(largest, kBig + 600, kTypeDeletion).Encode(),
            new_files[0].second.largest.Encode());
  ASSERT_TRUE(new_files[1].second.user_defined_timestamps_persisted);
  // Second file's boundaries contain user-defined timestamps.
  ASSERT_EQ(InternalKey(smallest + min_ts, kBig + 500, kTypeValue).Encode(),
            new_files[1].second.smallest.Encode());
  ASSERT_EQ(InternalKey(largest + min_ts, kBig + 600, kTypeDeletion).Encode(),
            new_files[1].second.largest.Encode());
}

TEST_F(VersionEditTest, ForwardCompatibleNewFile4) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               300 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false, Temperature::kUnknown, kInvalidBlobFileNumber,
               686, 868, 301 /* epoch_number */, "234", "crc32c",
               kNullUniqueId64x2, 0, 0, true);
  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetPersistUserDefinedTimestamps(true);
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  bool first = true;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        PutVarint32(str, 33);
        const std::string str1 = "random_string";
        PutLengthPrefixedSlice(str, str1);
        if (first) {
          first = false;
          PutVarint32(str, 22);
          const std::string str2 = "s";
          PutLengthPrefixedSlice(str, str2);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded, 0 /* ts_sz */);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(!first);
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_TRUE(!new_files[1].second.marked_for_compaction);
  ASSERT_EQ(3u, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3u, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(1u, parsed.GetDeletedFiles().size());
  ASSERT_TRUE(parsed.GetPersistUserDefinedTimestamps());
}

TEST_F(VersionEditTest, NewFile4NotSupportedField) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               300 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, false);

  edit.SetComparatorName("foo");
  edit.SetPersistUserDefinedTimestamps(false);
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        const std::string str1 = "s";
        PutLengthPrefixedSlice(str, str1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded, 0 /* ts_sz */);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_NOK(s);
}

TEST_F(VersionEditTest, EncodeEmptyFile) {
  VersionEdit edit;
  edit.AddFile(0, 0, 0, 0, InternalKey(), InternalKey(), 0, 0, false,
               Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               1 /*epoch_number*/, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);
  std::string buffer;
  ASSERT_TRUE(!edit.EncodeTo(&buffer, 0 /* ts_sz */));
}

TEST_F(VersionEditTest, ColumnFamilyTest) {
  VersionEdit edit;
  edit.SetColumnFamily(2);
  edit.AddColumnFamily("column_family");
  edit.SetMaxColumnFamily(5);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetColumnFamily(3);
  edit.DropColumnFamily();
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, MinLogNumberToKeep) {
  VersionEdit edit;
  edit.SetMinLogNumberToKeep(13);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetMinLogNumberToKeep(23);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, AtomicGroupTest) {
  VersionEdit edit;
  edit.MarkAtomicGroup(1);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, IgnorableField) {
  VersionEdit ve;
  std::string encoded;

  // Size of ignorable field is too large
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded,
                      0x2710 /* A field with kTagSafeIgnoreMask set */,
                      5 /* fieldlength 5 */);
  encoded += "abc";  // Only fills 3 bytes,
  ASSERT_NOK(ve.DecodeFrom(encoded));

  encoded.clear();
  // Error when seeing unidentified tag that is not ignorable
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded, 666 /* A field with kTagSafeIgnoreMask unset */,
                      3 /* fieldlength 3 */);
  encoded += "abc";  //  Fill 3 bytes
  PutVarint32Varint64(&encoded, 3 /* next file number */, 88);
  ASSERT_NOK(ve.DecodeFrom(encoded));

  // Safely ignore an identified but safely ignorable entry
  encoded.clear();
  PutVarint32Varint64(&encoded, 2 /* kLogNumber */, 66);
  // This is a customized ignorable tag
  PutVarint32Varint64(&encoded,
                      0x2710 /* A field with kTagSafeIgnoreMask set */,
                      3 /* fieldlength 3 */);
  encoded += "abc";  //  Fill 3 bytes
  PutVarint32Varint64(&encoded, 3 /* kNextFileNumber */, 88);

  ASSERT_OK(ve.DecodeFrom(encoded));

  ASSERT_TRUE(ve.HasLogNumber());
  ASSERT_TRUE(ve.HasNextFile());
  ASSERT_EQ(66, ve.GetLogNumber());
  ASSERT_EQ(88, ve.GetNextFile());
}

TEST_F(VersionEditTest, DbId) {
  VersionEdit edit;
  edit.SetDBId("ab34-cd12-435f-er00");
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetDBId("34ba-cd12-435f-er01");
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, BlobFileAdditionAndGarbage) {
  VersionEdit edit;

  const std::string checksum_method_prefix = "Hash";
  const std::string checksum_value_prefix = "Value";

  for (uint64_t blob_file_number = 1; blob_file_number <= 10;
       ++blob_file_number) {
    const uint64_t total_blob_count = blob_file_number << 10;
    const uint64_t total_blob_bytes = blob_file_number << 20;

    std::string checksum_method(checksum_method_prefix);
    AppendNumberTo(&checksum_method, blob_file_number);

    std::string checksum_value(checksum_value_prefix);
    AppendNumberTo(&checksum_value, blob_file_number);

    edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                     checksum_method, checksum_value);

    const uint64_t garbage_blob_count = total_blob_count >> 2;
    const uint64_t garbage_blob_bytes = total_blob_bytes >> 1;

    edit.AddBlobFileGarbage(blob_file_number, garbage_blob_count,
                            garbage_blob_bytes);
  }

  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, AddWalEncodeDecode) {
  VersionEdit edit;
  for (uint64_t log_number = 1; log_number <= 20; log_number++) {
    WalMetadata meta;
    bool has_size = rand() % 2 == 0;
    if (has_size) {
      meta.SetSyncedSizeInBytes(rand() % 1000);
    }
    edit.AddWal(log_number, meta);
  }
  TestEncodeDecode(edit);
}

static std::string PrefixEncodedWalAdditionWithLength(
    const std::string& encoded) {
  std::string ret;
  PutVarint32(&ret, Tag::kWalAddition2);
  PutLengthPrefixedSlice(&ret, encoded);
  return ret;
}

TEST_F(VersionEditTest, AddWalDecodeBadLogNumber) {
  std::string encoded;

  {
    // No log number.
    std::string encoded_edit = PrefixEncodedWalAdditionWithLength(encoded);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding WAL log number") !=
                std::string::npos)
        << s.ToString();
  }

  {
    // log number should be varint64,
    // but we only encode 128 which is not a valid representation of varint64.
    char c = 0;
    unsigned char* ptr = reinterpret_cast<unsigned char*>(&c);
    *ptr = 128;
    encoded.append(1, c);

    std::string encoded_edit = PrefixEncodedWalAdditionWithLength(encoded);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding WAL log number") !=
                std::string::npos)
        << s.ToString();
  }
}

TEST_F(VersionEditTest, AddWalDecodeBadTag) {
  constexpr WalNumber kLogNumber = 100;
  constexpr uint64_t kSizeInBytes = 100;

  std::string encoded;
  PutVarint64(&encoded, kLogNumber);

  {
    // No tag.
    std::string encoded_edit = PrefixEncodedWalAdditionWithLength(encoded);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding tag") != std::string::npos)
        << s.ToString();
  }

  {
    // Only has size tag, no terminate tag.
    std::string encoded_with_size = encoded;
    PutVarint32(&encoded_with_size,
                static_cast<uint32_t>(WalAdditionTag::kSyncedSize));
    PutVarint64(&encoded_with_size, kSizeInBytes);

    std::string encoded_edit =
        PrefixEncodedWalAdditionWithLength(encoded_with_size);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding tag") != std::string::npos)
        << s.ToString();
  }

  {
    // Only has terminate tag.
    std::string encoded_with_terminate = encoded;
    PutVarint32(&encoded_with_terminate,
                static_cast<uint32_t>(WalAdditionTag::kTerminate));

    std::string encoded_edit =
        PrefixEncodedWalAdditionWithLength(encoded_with_terminate);
    VersionEdit edit;
    ASSERT_OK(edit.DecodeFrom(encoded_edit));
    auto& wal_addition = edit.GetWalAdditions()[0];
    ASSERT_EQ(wal_addition.GetLogNumber(), kLogNumber);
    ASSERT_FALSE(wal_addition.GetMetadata().HasSyncedSize());
  }
}

TEST_F(VersionEditTest, AddWalDecodeNoSize) {
  constexpr WalNumber kLogNumber = 100;

  std::string encoded;
  PutVarint64(&encoded, kLogNumber);
  PutVarint32(&encoded, static_cast<uint32_t>(WalAdditionTag::kSyncedSize));
  // No real size after the size tag.

  {
    // Without terminate tag.
    std::string encoded_edit = PrefixEncodedWalAdditionWithLength(encoded);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("Error decoding WAL file size") !=
                std::string::npos)
        << s.ToString();
  }

  {
    // With terminate tag.
    PutVarint32(&encoded, static_cast<uint32_t>(WalAdditionTag::kTerminate));

    std::string encoded_edit = PrefixEncodedWalAdditionWithLength(encoded);
    VersionEdit edit;
    Status s = edit.DecodeFrom(encoded_edit);
    ASSERT_TRUE(s.IsCorruption());
    // The terminate tag is misunderstood as the size.
    ASSERT_TRUE(s.ToString().find("Error decoding tag") != std::string::npos)
        << s.ToString();
  }
}

TEST_F(VersionEditTest, AddWalDebug) {
  constexpr int n = 2;
  constexpr std::array<uint64_t, n> kLogNumbers{{10, 20}};
  constexpr std::array<uint64_t, n> kSizeInBytes{{100, 200}};

  VersionEdit edit;
  for (int i = 0; i < n; i++) {
    edit.AddWal(kLogNumbers[i], WalMetadata(kSizeInBytes[i]));
  }

  const WalAdditions& wals = edit.GetWalAdditions();

  ASSERT_TRUE(edit.IsWalAddition());
  ASSERT_EQ(wals.size(), n);
  for (int i = 0; i < n; i++) {
    const WalAddition& wal = wals[i];
    ASSERT_EQ(wal.GetLogNumber(), kLogNumbers[i]);
    ASSERT_EQ(wal.GetMetadata().GetSyncedSizeInBytes(), kSizeInBytes[i]);
  }

  std::string expected_str = "VersionEdit {\n";
  for (int i = 0; i < n; i++) {
    std::stringstream ss;
    ss << "  WalAddition: log_number: " << kLogNumbers[i]
       << " synced_size_in_bytes: " << kSizeInBytes[i] << "\n";
    expected_str += ss.str();
  }
  expected_str += "  ColumnFamily: 0\n}\n";
  ASSERT_EQ(edit.DebugString(true), expected_str);

  std::string expected_json = "{\"EditNumber\": 4, \"WalAdditions\": [";
  for (int i = 0; i < n; i++) {
    std::stringstream ss;
    ss << "{\"LogNumber\": " << kLogNumbers[i] << ", "
       << "\"SyncedSizeInBytes\": " << kSizeInBytes[i] << "}";
    if (i < n - 1) {
      ss << ", ";
    }
    expected_json += ss.str();
  }
  expected_json += "], \"ColumnFamily\": 0}";
  ASSERT_EQ(edit.DebugJSON(4, true), expected_json);
}

TEST_F(VersionEditTest, DeleteWalEncodeDecode) {
  VersionEdit edit;
  edit.DeleteWalsBefore(rand() % 100);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, DeleteWalDebug) {
  constexpr int n = 2;
  constexpr std::array<uint64_t, n> kLogNumbers{{10, 20}};

  VersionEdit edit;
  edit.DeleteWalsBefore(kLogNumbers[n - 1]);

  const WalDeletion& wal = edit.GetWalDeletion();

  ASSERT_TRUE(edit.IsWalDeletion());
  ASSERT_EQ(wal.GetLogNumber(), kLogNumbers[n - 1]);

  std::string expected_str = "VersionEdit {\n";
  {
    std::stringstream ss;
    ss << "  WalDeletion: log_number: " << kLogNumbers[n - 1] << "\n";
    expected_str += ss.str();
  }
  expected_str += "  ColumnFamily: 0\n}\n";
  ASSERT_EQ(edit.DebugString(true), expected_str);

  std::string expected_json = "{\"EditNumber\": 4, \"WalDeletion\": ";
  {
    std::stringstream ss;
    ss << "{\"LogNumber\": " << kLogNumbers[n - 1] << "}";
    expected_json += ss.str();
  }
  expected_json += ", \"ColumnFamily\": 0}";
  ASSERT_EQ(edit.DebugJSON(4, true), expected_json);
}

TEST_F(VersionEditTest, FullHistoryTsLow) {
  VersionEdit edit;
  ASSERT_FALSE(edit.HasFullHistoryTsLow());
  std::string ts = test::EncodeInt(0);
  edit.SetFullHistoryTsLow(ts);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, OptionsFileNumbers) {
  TestOptionsFileNumberField("CommittedOptionsFileNumber", 12345,
                             &VersionEdit::SetCommittedOptionsFileNumber,
                             &VersionEdit::HasCommittedOptionsFileNumber,
                             &VersionEdit::GetCommittedOptionsFileNumber);
}

// Tests that if RocksDB is downgraded, the new types of VersionEdits
// that have a tag larger than kTagSafeIgnoreMask can be safely ignored.
TEST_F(VersionEditTest, IgnorableTags) {
  SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:IgnoreIgnorableTags", [&](void* arg) {
        bool* ignore = static_cast<bool*>(arg);
        *ignore = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr uint64_t kPrevLogNumber = 100;
  constexpr uint64_t kLogNumber = 200;
  constexpr uint64_t kNextFileNumber = 300;
  constexpr uint64_t kColumnFamilyId = 400;

  VersionEdit edit;
  // Add some ignorable entries.
  for (int i = 0; i < 2; i++) {
    edit.AddWal(i + 1, WalMetadata(i + 2));
  }
  edit.SetDBId("db_id");
  edit.SetCommittedOptionsFileNumber(12345);
  // Add unignorable entries.
  edit.SetPrevLogNumber(kPrevLogNumber);
  edit.SetLogNumber(kLogNumber);
  // Add more ignorable entries.
  edit.DeleteWalsBefore(100);
  // Add unignorable entry.
  edit.SetNextFile(kNextFileNumber);
  // Add more ignorable entries.
  edit.SetFullHistoryTsLow("ts");
  // Add unignorable entry.
  edit.SetColumnFamily(kColumnFamilyId);

  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded, 0 /* ts_sz */));

  VersionEdit decoded;
  ASSERT_OK(decoded.DecodeFrom(encoded));

  // Check that all ignorable entries are ignored.
  ASSERT_FALSE(decoded.HasDbId());
  ASSERT_FALSE(decoded.HasFullHistoryTsLow());
  ASSERT_FALSE(decoded.HasCommittedOptionsFileNumber());
  ASSERT_FALSE(decoded.IsWalAddition());
  ASSERT_FALSE(decoded.IsWalDeletion());
  ASSERT_TRUE(decoded.GetWalAdditions().empty());
  ASSERT_TRUE(decoded.GetWalDeletion().IsEmpty());

  // Check that unignorable entries are still present.
  ASSERT_EQ(edit.GetPrevLogNumber(), kPrevLogNumber);
  ASSERT_EQ(edit.GetLogNumber(), kLogNumber);
  ASSERT_EQ(edit.GetNextFile(), kNextFileNumber);
  ASSERT_EQ(edit.GetColumnFamily(), kColumnFamilyId);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST(FileMetaDataTest, UpdateBoundariesBlobIndex) {
  FileMetaData meta;

  {
    constexpr uint64_t file_number = 10;
    constexpr uint32_t path_id = 0;
    constexpr uint64_t file_size = 0;

    meta.fd = FileDescriptor(file_number, path_id, file_size);
  }

  constexpr char key[] = "foo";

  constexpr uint64_t expected_oldest_blob_file_number = 20;

  // Plain old value (does not affect oldest_blob_file_number)
  {
    constexpr char value[] = "value";
    constexpr SequenceNumber seq = 200;

    ASSERT_OK(meta.UpdateBoundaries(key, value, seq, kTypeValue));
    ASSERT_EQ(meta.oldest_blob_file_number, kInvalidBlobFileNumber);
  }

  // Non-inlined, non-TTL blob index (sets oldest_blob_file_number)
  {
    constexpr uint64_t blob_file_number = 25;
    static_assert(blob_file_number > expected_oldest_blob_file_number,
                  "unexpected");

    constexpr uint64_t offset = 1000;
    constexpr uint64_t size = 100;

    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                          kNoCompression);

    constexpr SequenceNumber seq = 201;

    ASSERT_OK(meta.UpdateBoundaries(key, blob_index, seq, kTypeBlobIndex));
    ASSERT_EQ(meta.oldest_blob_file_number, blob_file_number);
  }

  // Another one, with the oldest blob file number (updates
  // oldest_blob_file_number)
  {
    constexpr uint64_t offset = 2000;
    constexpr uint64_t size = 300;

    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, expected_oldest_blob_file_number, offset,
                          size, kNoCompression);

    constexpr SequenceNumber seq = 202;

    ASSERT_OK(meta.UpdateBoundaries(key, blob_index, seq, kTypeBlobIndex));
    ASSERT_EQ(meta.oldest_blob_file_number, expected_oldest_blob_file_number);
  }

  // Inlined TTL blob index (does not affect oldest_blob_file_number)
  {
    constexpr uint64_t expiration = 9876543210;
    constexpr char value[] = "value";

    std::string blob_index;
    BlobIndex::EncodeInlinedTTL(&blob_index, expiration, value);

    constexpr SequenceNumber seq = 203;

    ASSERT_OK(meta.UpdateBoundaries(key, blob_index, seq, kTypeBlobIndex));
    ASSERT_EQ(meta.oldest_blob_file_number, expected_oldest_blob_file_number);
  }

  // Non-inlined TTL blob index (does not affect oldest_blob_file_number, even
  // though file number is smaller)
  {
    constexpr uint64_t expiration = 9876543210;
    constexpr uint64_t blob_file_number = 15;
    static_assert(blob_file_number < expected_oldest_blob_file_number,
                  "unexpected");

    constexpr uint64_t offset = 2000;
    constexpr uint64_t size = 500;

    std::string blob_index;
    BlobIndex::EncodeBlobTTL(&blob_index, expiration, blob_file_number, offset,
                             size, kNoCompression);

    constexpr SequenceNumber seq = 204;

    ASSERT_OK(meta.UpdateBoundaries(key, blob_index, seq, kTypeBlobIndex));
    ASSERT_EQ(meta.oldest_blob_file_number, expected_oldest_blob_file_number);
  }

  // Corrupt blob index
  {
    constexpr char corrupt_blob_index[] = "!corrupt!";
    constexpr SequenceNumber seq = 205;

    ASSERT_TRUE(
        meta.UpdateBoundaries(key, corrupt_blob_index, seq, kTypeBlobIndex)
            .IsCorruption());
    ASSERT_EQ(meta.oldest_blob_file_number, expected_oldest_blob_file_number);
  }

  // Invalid blob file number
  {
    constexpr uint64_t offset = 10000;
    constexpr uint64_t size = 1000;

    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, kInvalidBlobFileNumber, offset, size,
                          kNoCompression);

    constexpr SequenceNumber seq = 206;

    ASSERT_TRUE(meta.UpdateBoundaries(key, blob_index, seq, kTypeBlobIndex)
                    .IsCorruption());
    ASSERT_EQ(meta.oldest_blob_file_number, expected_oldest_blob_file_number);
  }
}

class SubcompactionProgressTest : public VersionEditTest {
 protected:
  static constexpr uint64_t kTestFileSize = 1024;
  static constexpr SequenceNumber kTestSmallestSeq = 50;
  static constexpr SequenceNumber kTestLargestSeq = 150;
  static constexpr uint64_t kTestOldestAncestorTime = 12345;
  static constexpr uint64_t kTestFileCreationTime = 67890;
  static constexpr uint64_t kTestEpochNumber = 10;
  static const std::string kTestChecksumFuncName;

  FileMetaData CreateTestFile(uint64_t file_number, const std::string& prefix) {
    FileMetaData file;
    file.fd = FileDescriptor(file_number, 0, kTestFileSize, kTestSmallestSeq,
                             kTestLargestSeq);
    file.smallest = InternalKey(prefix + "a", kTestSmallestSeq, kTypeValue);
    file.largest = InternalKey(prefix + "z", kTestLargestSeq, kTypeValue);
    file.oldest_ancestor_time = kTestOldestAncestorTime;
    file.file_creation_time = kTestFileCreationTime;
    file.epoch_number = kTestEpochNumber;
    file.file_checksum = "checksum_" + std::to_string(file_number);
    file.file_checksum_func_name = kTestChecksumFuncName;
    file.marked_for_compaction = false;
    file.temperature = Temperature::kUnknown;
    return file;
  }

  // Store external file metadata objects for testing
  // These simulate files owned by CompactionOutputs
  std::vector<FileMetaData> compaction_output_files_;
  std::vector<FileMetaData> proximal_level_compaction_output_files_;

  SubcompactionProgress CreateSubcompactionProgress(
      const std::string& next_key, uint64_t num_processed_input_records,
      uint64_t num_processed_output_records,
      uint64_t num_processed_proximal_level_output_records,
      const std::vector<uint64_t>& output_file_numbers = {},
      const std::vector<uint64_t>& proximal_file_numbers = {},
      const std::string& file_prefix = "file_") {
    SubcompactionProgress progress;
    progress.next_internal_key_to_compact = next_key;
    progress.num_processed_input_records = num_processed_input_records;
    progress.output_level_progress.SetNumProcessedOutputRecords(
        num_processed_output_records);
    progress.proximal_output_level_progress.SetNumProcessedOutputRecords(
        num_processed_proximal_level_output_records);

    for (uint64_t file_num : output_file_numbers) {
      FileMetaData file = CreateTestFile(file_num, file_prefix + "output_");
      progress.output_level_progress.AddToOutputFiles(file);
    }

    for (uint64_t file_num : proximal_file_numbers) {
      FileMetaData file = CreateTestFile(file_num, file_prefix + "proximal_");
      progress.proximal_output_level_progress.AddToOutputFiles(file);
    }

    return progress;
  }

  std::pair<const VersionEdit, const SubcompactionProgress>
  EncodeDecodeProgress(const SubcompactionProgress& progress) {
    VersionEdit edit;
    edit.SetSubcompactionProgress(progress);

    std::string encoded;
    EXPECT_TRUE(edit.EncodeTo(&encoded, 0 /* ts_sz */));

    VersionEdit decoded_edit;
    EXPECT_OK(decoded_edit.DecodeFrom(encoded));
    EXPECT_TRUE(decoded_edit.HasSubcompactionProgress());

    SubcompactionProgress decoded_progress =
        decoded_edit.GetSubcompactionProgress();

    return {std::move(decoded_edit), std::move(decoded_progress)};
  }

  void VerifyFileMetaDataEquality(const FileMetaData& expected,
                                  const FileMetaData& actual) {
    // Verify the major fields only
    ASSERT_EQ(actual.fd.GetNumber(), expected.fd.GetNumber());
    ASSERT_EQ(actual.fd.GetFileSize(), expected.fd.GetFileSize());
    ASSERT_EQ(actual.smallest.Encode(), expected.smallest.Encode());
    ASSERT_EQ(actual.largest.Encode(), expected.largest.Encode());
    ASSERT_EQ(actual.oldest_ancestor_time, expected.oldest_ancestor_time);
    ASSERT_EQ(actual.file_creation_time, expected.file_creation_time);
    ASSERT_EQ(actual.epoch_number, expected.epoch_number);
    ASSERT_EQ(actual.file_checksum, expected.file_checksum);
    ASSERT_EQ(actual.file_checksum_func_name, expected.file_checksum_func_name);
    ASSERT_EQ(actual.marked_for_compaction, expected.marked_for_compaction);
    ASSERT_EQ(actual.temperature, expected.temperature);
  }

  void VerifyProgressEquality(const SubcompactionProgress& expected,
                              const SubcompactionProgress& actual) {
    ASSERT_EQ(actual.next_internal_key_to_compact,
              expected.next_internal_key_to_compact);

    ASSERT_EQ(actual.num_processed_input_records,
              expected.num_processed_input_records);

    ASSERT_EQ(actual.has_blob_compaction_progress,
              expected.has_blob_compaction_progress);
    ASSERT_EQ(actual.blob_file_additions, expected.blob_file_additions);
    ASSERT_EQ(actual.blob_file_garbages, expected.blob_file_garbages);
    ASSERT_EQ(actual.next_blob_file_number, expected.next_blob_file_number);

    for (const bool is_proximal_level : {false, true}) {
      const SubcompactionProgressPerLevel&
          actual_subcompaction_progress_by_level =
              is_proximal_level ? actual.proximal_output_level_progress
                                : actual.output_level_progress;

      const SubcompactionProgressPerLevel&
          expected_subcompaction_progress_by_level =
              is_proximal_level ? expected.proximal_output_level_progress
                                : expected.output_level_progress;

      ASSERT_EQ(
          actual_subcompaction_progress_by_level.GetNumProcessedOutputRecords(),
          expected_subcompaction_progress_by_level
              .GetNumProcessedOutputRecords());

      ASSERT_EQ(
          actual_subcompaction_progress_by_level.GetOutputFiles().size(),
          expected_subcompaction_progress_by_level.GetOutputFiles().size());

      for (size_t i = 0;
           i < expected_subcompaction_progress_by_level.GetOutputFiles().size();
           ++i) {
        VerifyFileMetaDataEquality(
            expected_subcompaction_progress_by_level.GetOutputFiles()[i],
            actual_subcompaction_progress_by_level.GetOutputFiles()[i]);
      }
    }
  }
};

const std::string SubcompactionProgressTest::kTestChecksumFuncName = "crc32c";

TEST_F(SubcompactionProgressTest, BasicEncodeDecode) {
  // Create progress with files for both levels
  SubcompactionProgress progress = CreateSubcompactionProgress(
      "key_100",  // next_internal_key_to_compact
      500,        // num_processed_input_records
      400,        // num_processed_output_records
      100,        // num_processed_proximal_level_output_records
      {1},        // output_file_numbers
      {2},        // proximal_file_numbers
      "test_"     // file_prefix
  );

  auto [ignored, decoded_progress] = EncodeDecodeProgress(progress);

  VerifyProgressEquality(progress, decoded_progress);
}

TEST_F(SubcompactionProgressTest, OutputFilesDeltaEncodeDecode) {
  // Test Delta Encoding/Decoding
  SubcompactionProgress initial_progress = CreateSubcompactionProgress(
      "key_100",  // next_internal_key_to_compact
      100,        // num_processed_input_records
      40,         // num_processed_output_records
      60,         // num_processed_proximal_level_output_records
      {1},        // output_file_numbers
      {2},        // proximal_file_numbers
      "initial_"  // file_prefix
  );

  auto [initial_decoded_edit, ignored_1] =
      EncodeDecodeProgress(initial_progress);
  initial_progress.output_level_progress.UpdateLastPersistedOutputFilesCount();
  initial_progress.proximal_output_level_progress
      .UpdateLastPersistedOutputFilesCount();

  // Add one new output file to output and proximal level
  SubcompactionProgress updated_progress = initial_progress;
  updated_progress.next_internal_key_to_compact = "key_300";
  updated_progress.num_processed_input_records = 1000;
  updated_progress.output_level_progress.SetNumProcessedOutputRecords(400);
  updated_progress.proximal_output_level_progress.SetNumProcessedOutputRecords(
      600);

  FileMetaData new_file = CreateTestFile(3, "new_");
  updated_progress.output_level_progress.AddToOutputFiles(new_file);

  FileMetaData new_file_proximal = CreateTestFile(4, "new_");
  updated_progress.proximal_output_level_progress.AddToOutputFiles(
      new_file_proximal);

  auto [delta_decoded_edit, delta_decoded_progress] =
      EncodeDecodeProgress(updated_progress);

  ASSERT_EQ(delta_decoded_progress.next_internal_key_to_compact,
            updated_progress.next_internal_key_to_compact);

  ASSERT_EQ(delta_decoded_progress.num_processed_input_records,
            updated_progress.num_processed_input_records);

  for (const bool& is_proximal_level : {false, true}) {
    const SubcompactionProgressPerLevel& delta_progress_per_level =
        is_proximal_level
            ? delta_decoded_progress.proximal_output_level_progress
            : delta_decoded_progress.output_level_progress;

    const SubcompactionProgressPerLevel& updated_progress_per_level =
        is_proximal_level ? updated_progress.proximal_output_level_progress
                          : updated_progress.output_level_progress;

    ASSERT_EQ(delta_progress_per_level.GetNumProcessedOutputRecords(),
              updated_progress_per_level.GetNumProcessedOutputRecords());

    // Only the newly added file since last persistence should be present
    ASSERT_EQ(delta_progress_per_level.GetOutputFiles().size(), 1);

    ASSERT_EQ(delta_progress_per_level.GetOutputFiles()[0].fd.GetNumber(),
              is_proximal_level ? new_file_proximal.fd.GetNumber()
                                : new_file.fd.GetNumber());
  }

  // Test SubcompactionProgressBuilder
  SubcompactionProgressBuilder builder;
  ASSERT_FALSE(builder.HasAccumulatedSubcompactionProgress());

  ASSERT_TRUE(builder.ProcessVersionEdit(initial_decoded_edit));
  ASSERT_TRUE(builder.HasAccumulatedSubcompactionProgress());
  ASSERT_TRUE(builder.ProcessVersionEdit(delta_decoded_edit));

  const auto& accumulated_progress =
      builder.GetAccumulatedSubcompactionProgress();

  ASSERT_EQ(accumulated_progress.next_internal_key_to_compact,
            updated_progress.next_internal_key_to_compact);

  ASSERT_EQ(accumulated_progress.num_processed_input_records,
            updated_progress.num_processed_input_records);

  for (const bool& is_proximal_level : {false, true}) {
    const SubcompactionProgressPerLevel& accumulated_progress_per_level =
        is_proximal_level ? accumulated_progress.proximal_output_level_progress
                          : accumulated_progress.output_level_progress;

    const SubcompactionProgressPerLevel& updated_progress_per_level =
        is_proximal_level ? updated_progress.proximal_output_level_progress
                          : updated_progress.output_level_progress;

    ASSERT_EQ(accumulated_progress_per_level.GetNumProcessedOutputRecords(),
              updated_progress_per_level.GetNumProcessedOutputRecords());

    ASSERT_EQ(accumulated_progress_per_level.GetOutputFiles().size(),
              updated_progress_per_level.GetOutputFiles().size());

    std::set<uint64_t> accumulated_file_numbers;
    for (const auto& file : accumulated_progress_per_level.GetOutputFiles()) {
      accumulated_file_numbers.insert(file.fd.GetNumber());
    }

    std::set<uint64_t> expected_file_numbers;
    for (const auto& file : updated_progress_per_level.GetOutputFiles()) {
      expected_file_numbers.insert(file.fd.GetNumber());
    }

    ASSERT_EQ(accumulated_file_numbers, expected_file_numbers);
  }

  // ===== PART 3: Test Builder Reset =====
  builder.Clear();
  ASSERT_FALSE(builder.HasAccumulatedSubcompactionProgress());
}

TEST_F(SubcompactionProgressTest, BlobProgressDeltaEncodeDecode) {
  SubcompactionProgress initial_progress =
      CreateSubcompactionProgress("key_100", 100, 40, 0, {1});
  initial_progress.has_blob_compaction_progress = true;
  initial_progress.blob_file_additions.emplace_back(100, 2, 200, "", "");
  initial_progress.blob_file_garbages.emplace_back(50, 1, 25);
  initial_progress.next_blob_file_number = 101;

  auto [initial_decoded_edit, initial_decoded_progress] =
      EncodeDecodeProgress(initial_progress);
  VerifyProgressEquality(initial_progress, initial_decoded_progress);

  initial_progress.UpdateLastPersistedBlobFileAdditionsCount();
  SubcompactionProgress updated_progress = initial_progress;
  updated_progress.next_internal_key_to_compact = "key_200";
  updated_progress.blob_file_additions.emplace_back(101, 3, 300, "", "");
  updated_progress.blob_file_garbages = {BlobFileGarbage(50, 2, 50),
                                         BlobFileGarbage(51, 1, 10)};
  updated_progress.next_blob_file_number = 102;

  auto [delta_decoded_edit, delta_decoded_progress] =
      EncodeDecodeProgress(updated_progress);
  ASSERT_TRUE(delta_decoded_progress.has_blob_compaction_progress);
  ASSERT_EQ(delta_decoded_progress.blob_file_additions.size(), 1);
  ASSERT_EQ(delta_decoded_progress.blob_file_additions.front(),
            updated_progress.blob_file_additions.back());
  ASSERT_EQ(delta_decoded_progress.blob_file_garbages,
            updated_progress.blob_file_garbages);
  ASSERT_EQ(delta_decoded_progress.next_blob_file_number, 102);

  SubcompactionProgressBuilder builder;
  ASSERT_TRUE(builder.ProcessVersionEdit(initial_decoded_edit));
  ASSERT_TRUE(builder.ProcessVersionEdit(delta_decoded_edit));
  const SubcompactionProgress& accumulated =
      builder.GetAccumulatedSubcompactionProgress();
  ASSERT_TRUE(accumulated.has_blob_compaction_progress);
  ASSERT_EQ(accumulated.blob_file_additions,
            updated_progress.blob_file_additions);
  ASSERT_EQ(accumulated.blob_file_garbages,
            updated_progress.blob_file_garbages);
  ASSERT_EQ(accumulated.next_blob_file_number, 102);
}

TEST_F(SubcompactionProgressTest, UnknownTags) {
  SubcompactionProgress progress;
  std::string encoded;

  // 1. Test unknown ignorable tag
  progress.next_internal_key_to_compact = "test_key";
  progress.num_processed_input_records = 100;

  PutVarint32(&encoded,
              SubcompactionProgressCustomTag::kNextInternalKeyToCompact);
  PutLengthPrefixedSlice(&encoded, progress.next_internal_key_to_compact);

  PutVarint32(&encoded,
              SubcompactionProgressCustomTag::kNumProcessedInputRecords);
  std::string varint_records;
  PutVarint64(&varint_records, progress.num_processed_input_records);
  PutLengthPrefixedSlice(&encoded, varint_records);

  // Manually encode with unknown ignorable tag (has
  // SubcompactionProgressCustomTag::kSubcompactionProgressCustomTagSafeIgnoreMask
  // bit set)
  uint32_t unknown_ignorable_tag =
      SubcompactionProgressCustomTag::
          kSubcompactionProgressCustomTagSafeIgnoreMask +
      1;
  PutVarint32(&encoded, unknown_ignorable_tag);
  PutLengthPrefixedSlice(&encoded, "future_data");

  PutVarint32(&encoded,
              SubcompactionProgressCustomTag::kSubcompactionProgressTerminate);

  // Test decoding - should succeed and ignore unknown tag
  Slice input(encoded);
  SubcompactionProgress decoded_progress;
  Status s = decoded_progress.DecodeFrom(&input);
  ASSERT_OK(s);

  // Verify known fields are preserved
  ASSERT_EQ(decoded_progress.next_internal_key_to_compact,
            progress.next_internal_key_to_compact);
  ASSERT_EQ(decoded_progress.num_processed_input_records,
            progress.num_processed_input_records);

  // 2. Test unknown non-ignorable tag
  encoded.clear();
  PutVarint32(&encoded,
              SubcompactionProgressCustomTag::kNextInternalKeyToCompact);
  PutLengthPrefixedSlice(&encoded, "test_key");

  // Manually encode with unknown non-ignorable tag (do not have
  // SubcompactionProgressCustomTag::kSubcompactionProgressCustomTagSafeIgnoreMask
  // bit set)
  uint32_t unknown_critical_tag =
      SubcompactionProgressCustomTag::
          kSubcompactionProgressCustomTagSafeIgnoreMask -
      1;
  PutVarint32(&encoded, unknown_critical_tag);
  PutLengthPrefixedSlice(&encoded, "critical_future_data");
  PutVarint32(&encoded,
              SubcompactionProgressCustomTag::kSubcompactionProgressTerminate);

  // Test decoding - should fail on critical unknown tag
  Slice critical_input(encoded);
  SubcompactionProgress critical_progress;
  Status critical_status = critical_progress.DecodeFrom(&critical_input);
  ASSERT_NOK(critical_status);
  ASSERT_TRUE(critical_status.IsNotSupported());
}

TEST_F(VersionEditTest, FileOpenMetadataEncodeDecode) {
  static const uint64_t kBig = 1ull << 50;

  // Test 1: File with file_open_metadata set
  VersionEdit edit;
  edit.AddFile(3, 300, 0, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, false, Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               300 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);
  // Set file_open_metadata on the added file
  auto& new_files = edit.GetMutableNewFiles();
  ASSERT_EQ(1u, new_files.size());
  new_files[0].second.file_open_metadata = "test_metadata_content_12345";

  // Test 2: File without file_open_metadata (empty)
  edit.AddFile(4, 301, 0, 200, InternalKey("bar", kBig + 501, kTypeValue),
               InternalKey("zap", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false, Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               301 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);

  // Encode and decode
  std::string encoded;
  edit.EncodeTo(&encoded, 0 /* ts_sz */);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto& parsed_files = parsed.GetNewFiles();
  ASSERT_EQ(2u, parsed_files.size());

  // First file should have metadata
  ASSERT_EQ("test_metadata_content_12345",
            parsed_files[0].second.file_open_metadata);

  // Second file should have empty metadata
  ASSERT_TRUE(parsed_files[1].second.file_open_metadata.empty());
}

TEST_F(VersionEditTest, FileOpenMetadataForwardCompatibility) {
  // Test that a VersionEdit with kFileOpenMetadata can be decoded even if the
  // reader doesn't know the tag. Tag 17 < kCustomTagNonSafeIgnoreMask (64),
  // so it should be safely ignored.
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  edit.AddFile(3, 300, 0, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, false, Temperature::kUnknown, kInvalidBlobFileNumber,
               kUnknownOldestAncestorTime, kUnknownFileCreationTime,
               300 /* epoch_number */, kUnknownFileChecksum,
               kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0, true);
  auto& new_files = edit.GetMutableNewFiles();
  new_files[0].second.file_open_metadata = std::string(1024, 'x');

  // Encode
  std::string encoded;
  edit.EncodeTo(&encoded, 0 /* ts_sz */);

  // Decode should succeed
  VersionEdit parsed;
  ASSERT_OK(parsed.DecodeFrom(encoded));
  ASSERT_EQ(1u, parsed.GetNewFiles().size());
  ASSERT_EQ(std::string(1024, 'x'),
            parsed.GetNewFiles()[0].second.file_open_metadata);

  // Verify the tag value is safe-to-ignore
  ASSERT_EQ(0u, NewFileCustomTag::kFileOpenMetadata &
                    NewFileCustomTag::kCustomTagNonSafeIgnoreMask);
}

TEST_F(VersionEditTest, FileOpenMetadataInApproximateMemoryUsage) {
  // Use heap allocation because ApproximateMemoryUsage uses
  // malloc_usable_size which requires heap-allocated memory
  std::unique_ptr<FileMetaData> meta(new FileMetaData());
  size_t base_usage = meta->ApproximateMemoryUsage();

  meta->file_open_metadata = std::string(500, 'z');
  size_t with_metadata_usage = meta->ApproximateMemoryUsage();

  // The metadata should add to the memory usage
  ASSERT_GE(with_metadata_usage, base_usage + 500);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
