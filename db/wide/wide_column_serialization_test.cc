//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/wide_column_serialization.h"

#include <chrono>
#include <forward_list>
#include <limits>

#include "db/blob/blob_index.h"
#include "db/wide/wide_columns_helper.h"
#include "rocksdb/wide_columns.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class WideColumnSerializationTest : public testing::Test {
 protected:
  // Wrappers for private methods accessible via friend declaration.
  static Status GetVersion(const Slice& input, uint32_t& version) {
    return WideColumnSerialization::GetVersion(input, version);
  }
};

TEST_F(WideColumnSerializationTest, Construct) {
  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";

  const std::string foo_str(foo);
  const std::string bar_str(bar);

  const Slice foo_slice(foo_str);
  const Slice bar_slice(bar_str);

  {
    WideColumn column(foo, bar);
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar);
  }

  {
    WideColumn column(foo_str, bar);
    ASSERT_EQ(column.name(), foo_str);
    ASSERT_EQ(column.value(), bar);
  }

  {
    WideColumn column(foo_slice, bar);
    ASSERT_EQ(column.name(), foo_slice);
    ASSERT_EQ(column.value(), bar);
  }

  {
    WideColumn column(foo, bar_str);
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar_str);
  }

  {
    WideColumn column(foo_str, bar_str);
    ASSERT_EQ(column.name(), foo_str);
    ASSERT_EQ(column.value(), bar_str);
  }

  {
    WideColumn column(foo_slice, bar_str);
    ASSERT_EQ(column.name(), foo_slice);
    ASSERT_EQ(column.value(), bar_str);
  }

  {
    WideColumn column(foo, bar_slice);
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar_slice);
  }

  {
    WideColumn column(foo_str, bar_slice);
    ASSERT_EQ(column.name(), foo_str);
    ASSERT_EQ(column.value(), bar_slice);
  }

  {
    WideColumn column(foo_slice, bar_slice);
    ASSERT_EQ(column.name(), foo_slice);
    ASSERT_EQ(column.value(), bar_slice);
  }

  {
    constexpr char foo_name[] = "foo_name";
    constexpr char bar_value[] = "bar_value";

    WideColumn column(std::piecewise_construct,
                      std::forward_as_tuple(foo_name, sizeof(foo) - 1),
                      std::forward_as_tuple(bar_value, sizeof(bar) - 1));
    ASSERT_EQ(column.name(), foo);
    ASSERT_EQ(column.value(), bar);
  }
}

TEST_F(WideColumnSerializationTest, SerializeDeserialize) {
  WideColumns columns{{"foo", "bar"}, {"hello", "world"}};
  std::string output;

  ASSERT_OK(WideColumnSerialization::Serialize(columns, output));

  Slice input(output);
  WideColumns deserialized_columns;

  ASSERT_OK(
      WideColumnSerialization::DeserializeSimple(input, deserialized_columns));
  ASSERT_EQ(columns, deserialized_columns);

  {
    const auto it = WideColumnsHelper::Find(deserialized_columns.cbegin(),
                                            deserialized_columns.cend(), "foo");
    ASSERT_NE(it, deserialized_columns.cend());
    ASSERT_EQ(*it, deserialized_columns.front());
  }

  {
    const auto it = WideColumnsHelper::Find(
        deserialized_columns.cbegin(), deserialized_columns.cend(), "hello");
    ASSERT_NE(it, deserialized_columns.cend());
    ASSERT_EQ(*it, deserialized_columns.back());
  }

  {
    const auto it = WideColumnsHelper::Find(
        deserialized_columns.cbegin(), deserialized_columns.cend(), "fubar");
    ASSERT_EQ(it, deserialized_columns.cend());
  }

  {
    const auto it = WideColumnsHelper::Find(
        deserialized_columns.cbegin(), deserialized_columns.cend(), "snafu");
    ASSERT_EQ(it, deserialized_columns.cend());
  }
}

TEST_F(WideColumnSerializationTest, SerializeDuplicateError) {
  WideColumns columns{{"foo", "bar"}, {"foo", "baz"}};
  std::string output;

  ASSERT_TRUE(
      WideColumnSerialization::Serialize(columns, output).IsCorruption());
}

TEST_F(WideColumnSerializationTest, SerializeOutOfOrderError) {
  WideColumns columns{{"hello", "world"}, {"foo", "bar"}};
  std::string output;

  ASSERT_TRUE(
      WideColumnSerialization::Serialize(columns, output).IsCorruption());
}

TEST_F(WideColumnSerializationTest, DeserializeVersionError) {
  // Can't decode version

  std::string buf;

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST_F(WideColumnSerializationTest, DeserializeUnsupportedVersion) {
  // A version newer than kVersion2 is reported as Corruption (an unrecognized
  // serialized version is treated as bad data, consistent with the rest of the
  // codebase's format parsers).
  constexpr uint32_t future_version = 1000;

  std::string buf;
  PutVarint32(&buf, future_version);

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "version"));
}

TEST_F(WideColumnSerializationTest, FutureVersionRejectedConsistently) {
  // Every entry point must reject a future/unknown version as Corruption, so a
  // corrupt version byte cannot be misclassified (e.g. as a blob reference) by
  // any read path.
  constexpr uint32_t future_version = 1000;

  std::string buf;
  PutVarint32(&buf, future_version);
  Slice input(buf);

  WideColumns columns;
  ASSERT_TRUE(WideColumnSerialization::DeserializeSimple(input, columns)
                  .IsCorruption());

  Slice value;
  bool is_blob_reference = false;
  ASSERT_TRUE(WideColumnSerialization::GetValueOfDefaultColumn(
                  input, value, is_blob_reference)
                  .IsCorruption());

  bool has_blob_columns = false;
  ASSERT_TRUE(WideColumnSerialization::HasBlobColumns(input, has_blob_columns)
                  .IsCorruption());

  ASSERT_TRUE(WideColumnSerialization::ForEachBlobFileNumber(
                  input, [](const BlobIndex&) { return Status::OK(); })
                  .IsCorruption());
}

TEST_F(WideColumnSerializationTest, DeserializeNumberOfColumnsError) {
  // Can't decode number of columns

  std::string buf;
  PutVarint32(&buf, WideColumnSerialization::kVersion1);

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "number"));
}

TEST_F(WideColumnSerializationTest, DeserializeV2Error) {
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kVersion1);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  // Can't decode the first column name
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char first_column_name[] = "foo";
  PutLengthPrefixedSlice(&buf, first_column_name);

  // Can't decode the size of the first column value
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t first_value_size = 16;
  PutVarint32(&buf, first_value_size);

  // Can't decode the second column name
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "name"));
  }

  constexpr char second_column_name[] = "hello";
  PutLengthPrefixedSlice(&buf, second_column_name);

  // Can't decode the size of the second column value
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "value size"));
  }

  constexpr uint32_t second_value_size = 64;
  PutVarint32(&buf, second_value_size);

  // Can't decode the payload of the first column
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(first_value_size, '0');

  // Can't decode the payload of the second column
  {
    Slice input(buf);
    WideColumns columns;

    const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "payload"));
  }

  buf.append(second_value_size, 'x');

  // Success
  {
    Slice input(buf);
    WideColumns columns;

    ASSERT_OK(WideColumnSerialization::DeserializeSimple(input, columns));
  }
}

TEST_F(WideColumnSerializationTest, DeserializeV2OutOfOrder) {
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kVersion1);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  constexpr char first_column_name[] = "b";
  PutLengthPrefixedSlice(&buf, first_column_name);

  constexpr uint32_t first_value_size = 16;
  PutVarint32(&buf, first_value_size);

  constexpr char second_column_name[] = "a";
  PutLengthPrefixedSlice(&buf, second_column_name);

  Slice input(buf);
  WideColumns columns;

  const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(), "order"));
}

TEST_F(WideColumnSerializationTest, DeserializeV2RejectsRecursiveType) {
  // Manually construct a V2 entity where one column has type
  // kTypeWideColumnEntity, which would create recursive nesting.
  // Deserialization must reject this.
  std::string buf;

  PutVarint32(&buf, WideColumnSerialization::kVersion2);

  constexpr uint32_t num_columns = 2;
  PutVarint32(&buf, num_columns);

  // Section 2: COLUMN TYPES -- first column inline, second recursive
  buf.push_back(static_cast<char>(kTypeValue));
  buf.push_back(static_cast<char>(kTypeWideColumnEntity));

  // Section 3: SKIP INFO
  PutVarint32(&buf, 2);  // name_sizes_bytes (varint(1) + varint(1))
  PutVarint32(&buf, 2);  // value_sizes_bytes (varint(3) + varint(5))
  PutVarint32(&buf, 2);  // names_bytes ("a" + "b")

  // Section 4: NAME SIZES
  PutVarint32(&buf, 1);  // "a"
  PutVarint32(&buf, 1);  // "b"

  // Section 5: VALUE SIZES
  PutVarint32(&buf, 3);
  PutVarint32(&buf, 5);

  // Section 6: NAMES
  buf.append("ab");

  // Section 7: VALUES (8 bytes of placeholder data)
  buf.append(8, 'x');

  // Deserialize should reject with Corruption
  {
    Slice input(buf);
    std::vector<WideColumn> columns;
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    const Status s =
        WideColumnSerialization::Deserialize(input, columns, &blob_columns);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "Unsupported wide column ValueType"));
  }

  // Deserialize (V1-only API) should also reject
  {
    Slice input(buf);
    WideColumns columns;
    const Status s = WideColumnSerialization::DeserializeSimple(input, columns);
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_F(WideColumnSerializationTest, FastPathsRejectUnsupportedColumnType) {
  // GetValueOfDefaultColumn() and ForEachBlobFileNumber() read the COLUMN TYPES
  // section on a fast path. An unrecognized/unsupported type byte (here
  // kTypeWideColumnEntity) must be rejected as Corruption -- consistent with
  // Deserialize()/HasBlobColumns() -- rather than silently treated as an inline
  // value or a non-blob column.
  std::string buf;
  PutVarint32(&buf, WideColumnSerialization::kVersion2);
  PutVarint32(&buf, 1);  // num_columns

  // Section 2: SKIP INFO
  PutVarint32(&buf, 1);  // name_sizes_bytes (one varint: the name size 0)
  PutVarint32(&buf, 1);  // value_sizes_bytes (one varint: the value size 3)
  PutVarint32(&buf, 0);  // names_bytes (default column name is empty)

  // Section 3: COLUMN TYPES -- unsupported type for the (default) column
  buf.push_back(static_cast<char>(kTypeWideColumnEntity));

  // Section 4: NAME SIZES
  PutVarint32(&buf, 0);  // empty default column name
  // Section 5: VALUE SIZES
  PutVarint32(&buf, 3);
  // Section 6: NAMES (empty)
  // Section 7: VALUES
  buf.append("xyz");

  {
    Slice value;
    bool is_blob_reference = false;
    const Status s = WideColumnSerialization::GetValueOfDefaultColumn(
        Slice(buf), value, is_blob_reference);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "Unsupported wide column ValueType"));
  }

  {
    const Status s = WideColumnSerialization::ForEachBlobFileNumber(
        Slice(buf), [](const BlobIndex&) { return Status::OK(); });
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), "Unsupported wide column ValueType"));
  }
}

// Helper: create a BlobIndex from EncodeBlob parameters.
static BlobIndex MakeBlobIndex(uint64_t file_number, uint64_t offset,
                               uint64_t size,
                               CompressionType compression = kNoCompression) {
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, file_number, offset, size, compression);
  BlobIndex bi;
  Slice s(encoded);
  assert(bi.DecodeFrom(s).ok());
  return bi;
}

// Helper: V2 serialize -> Deserialize round-trip, returning
// deserialized columns and blob column info.
static void V2SerializeAndDeserialize(
    const std::vector<std::pair<std::string, std::string>>& columns,
    const std::vector<std::pair<size_t, BlobIndex>>& blob_columns_in,
    std::vector<WideColumn>* deserialized,
    std::vector<std::pair<size_t, BlobIndex>>* blob_columns_out,
    std::string* serialized_out) {
  ASSERT_OK(WideColumnSerialization::SerializeV2(columns, blob_columns_in,
                                                 *serialized_out));

  Slice input(*serialized_out);
  ASSERT_OK(WideColumnSerialization::Deserialize(input, *deserialized,
                                                 blob_columns_out));
  ASSERT_EQ(deserialized->size(), columns.size());
  for (size_t i = 0; i < columns.size(); ++i) {
    ASSERT_EQ((*deserialized)[i].name(), columns[i].first);
  }
}

// Helper: build WideColumns from string pairs.
static WideColumns ToWideColumns(
    const std::vector<std::pair<std::string, std::string>>& columns) {
  WideColumns wc;
  wc.reserve(columns.size());
  for (const auto& col : columns) {
    wc.emplace_back(Slice(col.first), Slice(col.second));
  }
  return wc;
}

// Helper: deserialize and verify column names match expected.first
// and column values match expected_values[i].
static void VerifyDeserialize(
    const std::string& serialized,
    const std::vector<std::pair<std::string, std::string>>& expected,
    const std::vector<std::string>& expected_values) {
  Slice input(serialized);
  WideColumns deserialized;
  ASSERT_OK(WideColumnSerialization::DeserializeSimple(input, deserialized));
  ASSERT_EQ(deserialized.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    ASSERT_EQ(deserialized[i].name(), expected[i].first);
    ASSERT_EQ(deserialized[i].value(), expected_values[i]);
  }
}

// Convenience overload: values come from expected[i].second.
static void VerifyDeserialize(
    const std::string& serialized,
    const std::vector<std::pair<std::string, std::string>>& expected) {
  std::vector<std::string> values;
  values.reserve(expected.size());
  for (const auto& col : expected) {
    values.push_back(col.second);
  }
  VerifyDeserialize(serialized, expected, values);
}

// Helper: create a random non-inlined BlobIndex using the given RNG.
// Only creates Blob or BlobTTL types (not InlinedTTL), because InlinedTTL
// stores a Slice pointing into the encoded string, which would become a
// dangling reference after this function returns.
static BlobIndex MakeRandomBlobIndex(Random& rng) {
  std::string bi_str;
  if (rng.Uniform(2) == 0) {
    BlobIndex::EncodeBlob(&bi_str, rng.Uniform(1000), rng.Uniform(10000),
                          rng.Uniform(5000), kNoCompression);
  } else {
    BlobIndex::EncodeBlobTTL(&bi_str, rng.Uniform(1000000), rng.Uniform(1000),
                             rng.Uniform(10000), rng.Uniform(5000),
                             kSnappyCompression);
  }
  BlobIndex bi;
  Slice s(bi_str);
  assert(bi.DecodeFrom(s).ok());
  return bi;
}

// Helper: V2 serialize with no blobs then GetValueOfDefaultColumn.
static void VerifyGetDefaultColumn(
    const std::vector<std::pair<std::string, std::string>>& columns,
    const Slice& expected_value) {
  std::vector<std::pair<size_t, BlobIndex>> no_blobs;
  std::string serialized;
  ASSERT_OK(
      WideColumnSerialization::SerializeV2(columns, no_blobs, serialized));

  Slice input(serialized);
  Slice value;
  bool is_blob_reference = false;
  ASSERT_OK(WideColumnSerialization::GetValueOfDefaultColumn(
      input, value, is_blob_reference));
  ASSERT_FALSE(is_blob_reference);
  ASSERT_EQ(value, expected_value);
}

TEST_F(WideColumnSerializationTest, V2GetValueOfDefaultColumn) {
  // V2 with default column present
  VerifyGetDefaultColumn({{"", "default_value"}, {"col1", "value1"}},
                         "default_value");
  // V2 without default column
  VerifyGetDefaultColumn({{"col1", "value1"}, {"col2", "value2"}}, Slice());
  // V2 with zero columns
  VerifyGetDefaultColumn({}, Slice());

  // V1 fallback
  {
    WideColumns columns{{"", "v1_default"}, {"col1", "v1"}};
    std::string serialized;
    ASSERT_OK(WideColumnSerialization::Serialize(columns, serialized));

    Slice input(serialized);
    Slice value;
    bool is_blob_reference = false;
    ASSERT_OK(WideColumnSerialization::GetValueOfDefaultColumn(
        input, value, is_blob_reference));
    ASSERT_FALSE(is_blob_reference);
    ASSERT_EQ(value, "v1_default");
  }
}

TEST_F(WideColumnSerializationTest, V2BlobColumnRejectsDeserialize) {
  std::vector<std::pair<std::string, std::string>> columns = {
      {"a", "inline"}, {"b", "placeholder"}};
  std::vector<std::pair<size_t, BlobIndex>> blob_columns = {
      {1, MakeBlobIndex(1, 2, 3)}};

  std::string serialized;
  ASSERT_OK(
      WideColumnSerialization::SerializeV2(columns, blob_columns, serialized));

  Slice input(serialized);
  WideColumns deserialized;
  ASSERT_TRUE(WideColumnSerialization::DeserializeSimple(input, deserialized)
                  .IsCorruption());
}

TEST_F(WideColumnSerializationTest, PinnableWideColumnsFallbacksToV2) {
  const std::vector<std::pair<std::string, std::string>> columns = {
      {"", "placeholder"}, {"ttl", "00000001"}, {"type", "cold"}};
  const std::vector<std::pair<size_t, BlobIndex>> blob_columns = {
      {0, MakeBlobIndex(10, 20, 30)}};

  std::string serialized;
  ASSERT_OK(
      WideColumnSerialization::SerializeV2(columns, blob_columns, serialized));

  std::vector<WideColumn> expected_columns;
  std::vector<std::pair<size_t, BlobIndex>> expected_blob_columns;
  Slice input(serialized);
  ASSERT_OK(WideColumnSerialization::Deserialize(input, expected_columns,
                                                 &expected_blob_columns));

  PinnableWideColumns result;
  ASSERT_OK(result.SetWideColumnValue(serialized));

  ASSERT_EQ(result.columns().size(), expected_columns.size());
  for (size_t i = 0; i < expected_columns.size(); ++i) {
    ASSERT_EQ(result.columns()[i].name(), expected_columns[i].name());
    ASSERT_EQ(result.columns()[i].value(), expected_columns[i].value());
  }
}

TEST_F(WideColumnSerializationTest, V2GetValueOfDefaultColumnBlobRef) {
  // When the default column (index 0) is a blob reference,
  // GetValueOfDefaultColumn succeeds and flags it via is_blob_reference,
  // returning the raw serialized BlobIndex bytes (no fetch).
  std::vector<std::pair<std::string, std::string>> columns = {
      {"", "placeholder"}, {"col1", "value1"}};
  std::vector<std::pair<size_t, BlobIndex>> blob_columns = {
      {0, MakeBlobIndex(10, 100, 500)}};

  std::string serialized;
  ASSERT_OK(
      WideColumnSerialization::SerializeV2(columns, blob_columns, serialized));

  Slice input(serialized);
  Slice value;
  bool is_blob_reference = false;
  ASSERT_OK(WideColumnSerialization::GetValueOfDefaultColumn(
      input, value, is_blob_reference));
  ASSERT_TRUE(is_blob_reference);
  ASSERT_FALSE(value.empty());

  // Resolving a non-inlined reference without a blob fetcher is Corruption.
  PinnableSlice resolved;
  ASSERT_TRUE(WideColumnSerialization::ResolveDefaultColumnBlobReference(
                  value, "user_key", /*blob_fetcher=*/nullptr, resolved)
                  .IsCorruption());
}

TEST_F(WideColumnSerializationTest, SerializeV2Errors) {
  // Blob column index out of range
  {
    std::vector<std::pair<std::string, std::string>> columns = {{"a", "val"}};
    std::vector<std::pair<size_t, BlobIndex>> blob_columns = {
        {5, MakeBlobIndex(1, 2, 3)}};  // index 5 but only 1 column

    std::string output;
    ASSERT_TRUE(
        WideColumnSerialization::SerializeV2(columns, blob_columns, output)
            .IsInvalidArgument());
  }

  // Columns out of order (V2)
  {
    std::vector<std::pair<std::string, std::string>> columns = {{"b", "val_b"},
                                                                {"a", "val_a"}};
    std::vector<std::pair<size_t, BlobIndex>> no_blobs;

    std::string output;
    ASSERT_TRUE(WideColumnSerialization::SerializeV2(columns, no_blobs, output)
                    .IsCorruption());
  }

  // Duplicate column names (V2)
  {
    std::vector<std::pair<std::string, std::string>> columns = {{"a", "val1"},
                                                                {"a", "val2"}};
    std::vector<std::pair<size_t, BlobIndex>> no_blobs;

    std::string output;
    ASSERT_TRUE(WideColumnSerialization::SerializeV2(columns, no_blobs, output)
                    .IsCorruption());
  }
}

TEST_F(WideColumnSerializationTest, BlobIndexEncodeToRoundTrip) {
  // Test EncodeTo produces identical output to static Encode methods
  // for all three blob index types.
  auto verify_encode_to = [](const std::string& encoded_static) {
    BlobIndex bi;
    Slice s(encoded_static);
    ASSERT_OK(bi.DecodeFrom(s));
    std::string encoded_instance;
    bi.EncodeTo(&encoded_instance);
    ASSERT_EQ(encoded_static, encoded_instance);
  };

  std::string blob_str;
  std::string blob_ttl_str;
  std::string inlined_str;
  BlobIndex::EncodeBlob(&blob_str, 42, 1024, 2048, kSnappyCompression);
  BlobIndex::EncodeBlobTTL(&blob_ttl_str, 9999, 10, 200, 3000,
                           kZlibCompression);
  BlobIndex::EncodeInlinedTTL(&inlined_str, 12345, "inline_data");

  verify_encode_to(blob_str);
  verify_encode_to(blob_ttl_str);
  verify_encode_to(inlined_str);
}

TEST_F(WideColumnSerializationTest, V2LayoutStructureVerification) {
  // Verify the V2 binary layout structure by manually parsing sections
  std::vector<std::pair<std::string, std::string>> columns = {
      {"aa", "val_aa"}, {"bbb", "val_bbb"}};
  std::vector<std::pair<size_t, BlobIndex>> empty_blob_columns;

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::SerializeV2(columns, empty_blob_columns,
                                                 serialized));

  Slice data(serialized);

  // Section 1: HEADER
  uint32_t version = 0;
  ASSERT_TRUE(GetVarint32(&data, &version));
  ASSERT_EQ(version, WideColumnSerialization::kVersion2);

  uint32_t num_columns = 0;
  ASSERT_TRUE(GetVarint32(&data, &num_columns));
  ASSERT_EQ(num_columns, 2u);

  // Section 2: SKIP INFO (3 varints)
  uint32_t name_sizes_bytes = 0;
  uint32_t value_sizes_bytes = 0;
  uint32_t names_bytes = 0;
  ASSERT_TRUE(GetVarint32(&data, &name_sizes_bytes));
  ASSERT_TRUE(GetVarint32(&data, &value_sizes_bytes));
  ASSERT_TRUE(GetVarint32(&data, &names_bytes));
  // name sizes: varint(2) + varint(3) = 1 + 1 = 2 bytes
  ASSERT_EQ(name_sizes_bytes, 2u);
  // value sizes: varint(6) + varint(7) = 1 + 1 = 2 bytes
  ASSERT_EQ(value_sizes_bytes, 2u);
  // names: "aa" + "bbb" = 2 + 3 = 5 bytes
  ASSERT_EQ(names_bytes, 5u);

  // Section 3: COLUMN TYPES (2 bytes, both inline)
  ASSERT_GE(data.size(), 2u);
  ASSERT_EQ(static_cast<uint8_t>(data[0]), static_cast<uint8_t>(kTypeValue));
  ASSERT_EQ(static_cast<uint8_t>(data[1]), static_cast<uint8_t>(kTypeValue));
  data.remove_prefix(2);

  // Section 4: NAME SIZES
  uint32_t ns0 = 0;
  uint32_t ns1 = 0;
  ASSERT_TRUE(GetVarint32(&data, &ns0));
  ASSERT_TRUE(GetVarint32(&data, &ns1));
  ASSERT_EQ(ns0, 2u);
  ASSERT_EQ(ns1, 3u);

  // Section 5: VALUE SIZES
  uint32_t vs0 = 0;
  uint32_t vs1 = 0;
  ASSERT_TRUE(GetVarint32(&data, &vs0));
  ASSERT_TRUE(GetVarint32(&data, &vs1));
  ASSERT_EQ(vs0, 6u);  // "val_aa" = 6
  ASSERT_EQ(vs1, 7u);  // "val_bbb" = 7

  // Section 6: COLUMN NAMES
  ASSERT_GE(data.size(), 5u);
  ASSERT_EQ(Slice(data.data(), 2), "aa");
  ASSERT_EQ(Slice(data.data() + 2, 3), "bbb");
  data.remove_prefix(5);

  // Section 7: COLUMN VALUES
  ASSERT_GE(data.size(), 13u);
  ASSERT_EQ(Slice(data.data(), 6), "val_aa");
  ASSERT_EQ(Slice(data.data() + 6, 7), "val_bbb");
}

// Randomized correctness test: serialize and deserialize with random column
// counts, name sizes, value sizes, and randomly chosen blob columns.
// Validates the full round-trip for both V1 (Serialize) and V2
// (SerializeV2) formats.
TEST_F(WideColumnSerializationTest, RandomizedSerializeDeserializeRoundTrip) {
  uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  Random rng(seed);
  SCOPED_TRACE("seed=" + std::to_string(seed));

  constexpr int kNumIterations = 100;

  for (int iter = 0; iter < kNumIterations; ++iter) {
    int num_cols = rng.Uniform(17);     // 0..16
    int name_sz = 1 + rng.Uniform(64);  // 1..64
    int val_sz = rng.Uniform(1025);     // 0..1024

    // Generate sorted column names and random values
    std::vector<std::pair<std::string, std::string>> columns;
    columns.reserve(num_cols);
    for (int c = 0; c < num_cols; ++c) {
      // Build a sorted, unique name of exactly name_sz bytes.
      // Use hex-encoded index as prefix to guarantee sort order,
      // then pad with random characters.
      char idx_str[16];
      snprintf(idx_str, sizeof(idx_str), "%04x", c);
      std::string name(idx_str);
      if (static_cast<int>(name.size()) < name_sz) {
        name.append(name_sz - name.size(),
                    static_cast<char>('a' + rng.Uniform(26)));
      }
      // Ensure exactly name_sz bytes. For name_sz < 4, use just the
      // low-order hex digits to maintain sort order.
      if (static_cast<int>(name.size()) > name_sz) {
        name = name.substr(name.size() - name_sz);
      }

      // Random value content
      std::string value(val_sz, '\0');
      for (int j = 0; j < val_sz; ++j) {
        value[j] = static_cast<char>(rng.Uniform(256));
      }
      columns.emplace_back(std::move(name), std::move(value));
    }

    // Randomly select some columns as blob columns
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    for (int c = 0; c < num_cols; ++c) {
      if (rng.Uniform(3) == 0) {  // ~33% chance of being a blob column
        blob_columns.emplace_back(c, MakeRandomBlobIndex(rng));
      }
    }

    // V2 serialize -> Deserialize round-trip
    std::string serialized;
    std::vector<WideColumn> deserialized;
    std::vector<std::pair<size_t, BlobIndex>> blob_out;
    V2SerializeAndDeserialize(columns, blob_columns, &deserialized, &blob_out,
                              &serialized);

    // Verify version and HasBlobColumns
    uint32_t v = 0;
    ASSERT_OK(GetVersion(Slice(serialized), v));
    ASSERT_EQ(v, WideColumnSerialization::kVersion2);

    bool hb = false;
    ASSERT_OK(WideColumnSerialization::HasBlobColumns(Slice(serialized), hb));
    ASSERT_EQ(hb, !blob_columns.empty());

    // Verify blob column round-trip
    ASSERT_EQ(blob_out.size(), blob_columns.size());
    for (size_t b = 0; b < blob_columns.size(); ++b) {
      ASSERT_EQ(blob_out[b].first, blob_columns[b].first);
      const BlobIndex& orig = blob_columns[b].second;
      const BlobIndex& decoded = blob_out[b].second;
      ASSERT_EQ(decoded.IsInlined(), orig.IsInlined());
      ASSERT_EQ(decoded.HasTTL(), orig.HasTTL());
      if (!decoded.IsInlined()) {
        ASSERT_EQ(decoded.file_number(), orig.file_number());
        ASSERT_EQ(decoded.offset(), orig.offset());
        ASSERT_EQ(decoded.size(), orig.size());
      }
    }

    // Verify inline column values
    size_t blob_idx = 0;
    for (int c = 0; c < num_cols; ++c) {
      if (blob_idx < blob_columns.size() &&
          blob_columns[blob_idx].first == static_cast<size_t>(c)) {
        ++blob_idx;
      } else {
        ASSERT_EQ(deserialized[c].value(), columns[c].second);
      }
    }

    // If no blob columns, also verify Deserialize() and both overloads
    if (blob_columns.empty()) {
      VerifyDeserialize(serialized, columns);

      // WideColumns overload should produce identical output
      std::string serialized2;
      WideColumns wc = ToWideColumns(columns);
      ASSERT_OK(
          WideColumnSerialization::SerializeV2(wc, blob_columns, serialized2));
      ASSERT_EQ(serialized, serialized2);
    }

    // V1 Serialize round-trip
    {
      WideColumns wc = ToWideColumns(columns);
      std::string serialized_v1;
      ASSERT_OK(WideColumnSerialization::Serialize(wc, serialized_v1));

      ASSERT_OK(GetVersion(Slice(serialized_v1), v));
      ASSERT_EQ(v, WideColumnSerialization::kVersion1);

      VerifyDeserialize(serialized_v1, columns);
    }
  }
}

TEST_F(WideColumnSerializationTest, ResolveEntityForMergeNullBlobFetcher) {
  // Create a V2 entity with a blob column reference
  std::vector<std::pair<std::string, std::string>> columns;
  columns.emplace_back("", "default_val");
  columns.emplace_back("col1", "inline_val");

  std::vector<std::pair<size_t, BlobIndex>> blob_columns;
  blob_columns.emplace_back(0, MakeBlobIndex(42, 100, 50));

  std::string serialized;
  ASSERT_OK(
      WideColumnSerialization::SerializeV2(columns, blob_columns, serialized));

  // Verify it has blob columns
  bool has_blob_columns = false;
  ASSERT_OK(WideColumnSerialization::HasBlobColumns(Slice(serialized),
                                                    has_blob_columns));
  ASSERT_TRUE(has_blob_columns);

  // Call ResolveEntityForMerge with null blob_fetcher - should return an error
  // status rather than crashing.
  std::string resolved_entity;
  Slice effective_entity;
  Status s = WideColumnSerialization::ResolveEntityForMerge(
      Slice(serialized), "user_key", nullptr /* blob_fetcher */,
      nullptr /* prefetch_buffers */, resolved_entity, effective_entity);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(WideColumnSerializationTest, DeserializeRejectsTrailingData) {
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"attr", "val"}};

  auto check = [](const std::string& serialized) {
    std::vector<WideColumn> out;
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    ASSERT_OK(WideColumnSerialization::Deserialize(Slice(serialized), out,
                                                   &blob_columns));

    // A trailing byte must be rejected as Corruption.
    const std::string with_trailing = serialized + "x";
    out.clear();
    blob_columns.clear();
    ASSERT_TRUE(WideColumnSerialization::Deserialize(Slice(with_trailing), out,
                                                     &blob_columns)
                    .IsCorruption());
  };

  // V1 layout (the underlying DeserializeV1 must reject trailing data, matching
  // the V2 layout below).
  {
    std::string serialized;
    ASSERT_OK(WideColumnSerialization::Serialize(columns, serialized));
    check(serialized);
  }

  // V2 layout with no blob columns.
  {
    std::string serialized;
    ASSERT_OK(WideColumnSerialization::SerializeV2(
        columns, {} /* blob_columns */, serialized));
    check(serialized);
  }

  // Empty (zero-column) entities must reject trailing data too.
  const WideColumns empty_columns;
  {
    std::string serialized;
    ASSERT_OK(WideColumnSerialization::Serialize(empty_columns, serialized));
    check(serialized);
  }
  {
    std::string serialized;
    ASSERT_OK(WideColumnSerialization::SerializeV2(
        empty_columns, {} /* blob_columns */, serialized));
    check(serialized);
  }
}

TEST_F(WideColumnSerializationTest, DeserializeV2SkipInfoOverflow) {
  // A V2 entity declares the byte size of its NAME SIZES, VALUE SIZES and NAMES
  // sections up front, and the parser checks the sum of the three against the
  // remaining input before deriving a pointer to each section. Skip info whose
  // sizes sum to 2^32 or more must be rejected, otherwise those pointers land
  // past the end of the entity: the first case below puts the VALUE SIZES
  // pointer exactly one byte past it (read by the per-column varint decode),
  // the other two put it gigabytes out.
  struct TestCase {
    uint32_t name_sizes_bytes;
    uint32_t value_sizes_bytes;
    uint32_t names_bytes;
    size_t trailing_bytes;
  };
  constexpr TestCase kCases[] = {
      {1, 0x80000000, 0x80000000, 1},
      {0x80000000, 0x80000000, 0, 2},
      {0xfffffff0, 0x10, 0, 2},
  };

  for (const auto& test_case : kCases) {
    SCOPED_TRACE(
        "name_sizes_bytes=" + std::to_string(test_case.name_sizes_bytes) +
        " value_sizes_bytes=" + std::to_string(test_case.value_sizes_bytes) +
        " names_bytes=" + std::to_string(test_case.names_bytes));

    std::string buf;
    PutVarint32(&buf, WideColumnSerialization::kVersion2);
    PutVarint32(&buf, 1);  // num_columns

    // Section 2: SKIP INFO
    PutVarint32(&buf, test_case.name_sizes_bytes);
    PutVarint32(&buf, test_case.value_sizes_bytes);
    PutVarint32(&buf, test_case.names_bytes);

    // Section 3: COLUMN TYPES
    buf.push_back(static_cast<char>(kTypeValue));

    // Sections 4-7 are nowhere near the declared sizes.
    buf.append(test_case.trailing_bytes, '\0');

    // Deserialize from an exactly sized allocation, so that a read past the
    // entity is an out-of-bounds read rather than a read of spare capacity.
    const std::vector<char> entity(buf.begin(), buf.end());

    std::vector<WideColumn> columns;
    std::vector<std::pair<size_t, BlobIndex>> blob_columns;
    ASSERT_TRUE(WideColumnSerialization::Deserialize(
                    Slice(entity.data(), entity.size()), columns, &blob_columns)
                    .IsCorruption());
  }
}

namespace {

size_t ExpectedPayloadSize(const WideColumns& columns) {
  size_t total = 0;
  for (const auto& column : columns) {
    total += column.name().size() + column.value().size();
  }
  return total;
}

}  // namespace

TEST(PinnableWideColumnsTest, SetPlainValue) {
  PinnableWideColumns columns;
  columns.SetPlainValue(Slice("hello"));

  ASSERT_EQ(columns.columns().size(), 1);
  EXPECT_EQ(columns.columns()[0].name(), kDefaultWideColumnName);
  EXPECT_EQ(columns.columns()[0].value(), Slice("hello"));

  // For a plain value, payload_size() equals the raw value size.
  EXPECT_EQ(columns.payload_size(), 5);
}

TEST(PinnableWideColumnsTest, SetWideColumnValueAndPayloadSize) {
  // Columns must be sorted (default/empty name first).
  const WideColumns entity_columns{
      {kDefaultWideColumnName, "default"}, {"a", "bb"}, {"ccc", "dddd"}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::Serialize(entity_columns, serialized));

  PinnableWideColumns columns;
  ASSERT_OK(columns.SetWideColumnValue(std::move(serialized)));

  ASSERT_EQ(columns.columns(), entity_columns);
  EXPECT_EQ(columns.payload_size(), ExpectedPayloadSize(entity_columns));
}

TEST(PinnableWideColumnsTest, SerializedSizeMatchesSerializedSizeV1) {
  {
    // Plain value: default column with the raw value.
    PinnableWideColumns columns;
    columns.SetPlainValue(Slice("plain-value"));

    EXPECT_EQ(columns.serialized_size(),
              WideColumnSerialization::SerializedSizeV1(columns.columns()));
  }

  {
    // V1 entity: serialized_size() equals the length of the exact bytes that
    // Serialize() produced.
    const WideColumns entity_columns{
        {kDefaultWideColumnName, "default"}, {"attr1", "val1"}, {"attr2", ""}};

    std::string serialized;
    ASSERT_OK(WideColumnSerialization::Serialize(entity_columns, serialized));
    const size_t serialized_len = serialized.size();

    PinnableWideColumns columns;
    ASSERT_OK(columns.SetWideColumnValue(std::move(serialized)));

    EXPECT_EQ(columns.serialized_size(),
              WideColumnSerialization::SerializedSizeV1(columns.columns()));
    EXPECT_EQ(columns.serialized_size(), serialized_len);
  }
}

TEST(PinnableWideColumnsTest, Reset) {
  PinnableWideColumns columns;
  columns.SetPlainValue(Slice("something"));
  ASSERT_FALSE(columns.columns().empty());

  columns.Reset();
  EXPECT_TRUE(columns.columns().empty());
  EXPECT_EQ(columns.payload_size(), 0);

  // Reusable after Reset.
  columns.SetPlainValue(Slice("again"));
  ASSERT_EQ(columns.columns().size(), 1);
  EXPECT_EQ(columns.columns()[0].value(), Slice("again"));
}

TEST(PinnableWideColumnsTest, MovePlainValueSelfPinnedStable) {
  // A short (SSO) plain value is self-pinned inside a backing node. The node
  // must not relocate on move, so the column Slice stays valid and its data
  // pointer is preserved -- the guarantee that lets us drop the old
  // re-derivation-on-move logic.
  PinnableWideColumns columns;
  columns.SetPlainValue(std::string("tiny"));

  const char* const value_data = columns.columns()[0].value().data();

  PinnableWideColumns moved(std::move(columns));
  ASSERT_EQ(moved.columns().size(), 1);
  EXPECT_EQ(moved.columns()[0].name(), kDefaultWideColumnName);
  EXPECT_EQ(moved.columns()[0].value(), Slice("tiny"));
  EXPECT_EQ(moved.columns()[0].value().data(), value_data);
}

TEST(PinnableWideColumnsTest, ResolveColumnsZeroCopy) {
  // Original entity with two inline columns.
  const WideColumns entity_columns{{kDefaultWideColumnName, "default-value"},
                                   {"blob_col", "encoded-blob-index"}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::Serialize(entity_columns, serialized));

  PinnableWideColumns columns;
  ASSERT_OK(columns.SetWideColumnValue(std::move(serialized)));

  // Inline (default) column keeps its Slice into the original entity buffer.
  const Slice inline_name = columns.columns()[0].name();
  const Slice inline_value = columns.columns()[0].value();
  const char* const inline_value_data = inline_value.data();

  // Resolve "blob_col" into its own backing buffer.
  std::forward_list<PinnableSlice> extra_buffers;
  extra_buffers.emplace_front();
  extra_buffers.front().PinSelf(Slice("resolved-blob-payload"));
  const char* const resolved_data = extra_buffers.front().data();

  WideColumns resolved_columns;
  resolved_columns.emplace_back(inline_name, inline_value);
  resolved_columns.emplace_back(columns.columns()[1].name(),
                                Slice(extra_buffers.front()));

  PinnableWideColumnsHelper::ResolveColumns(
      columns, std::move(resolved_columns), std::move(extra_buffers));

  ASSERT_EQ(columns.columns().size(), 2);

  // Inline column still points into the original entity buffer (no copy, no
  // re-serialization).
  EXPECT_EQ(columns.columns()[0].value(), Slice("default-value"));
  EXPECT_EQ(columns.columns()[0].value().data(), inline_value_data);

  // Resolved blob column points directly into the spliced-in backing buffer,
  // not into a re-serialized entity.
  EXPECT_EQ(columns.columns()[1].name(), Slice("blob_col"));
  EXPECT_EQ(columns.columns()[1].value(), Slice("resolved-blob-payload"));
  EXPECT_EQ(columns.columns()[1].value().data(), resolved_data);
}

TEST(PinnableWideColumnsTest, MultiBufferMovePreservesPointers) {
  const WideColumns entity_columns{{kDefaultWideColumnName, "default-value"},
                                   {"blob_col", "encoded-blob-index"}};

  std::string serialized;
  ASSERT_OK(WideColumnSerialization::Serialize(entity_columns, serialized));

  PinnableWideColumns columns;
  ASSERT_OK(columns.SetWideColumnValue(std::move(serialized)));

  std::forward_list<PinnableSlice> extra_buffers;
  extra_buffers.emplace_front();
  extra_buffers.front().PinSelf(Slice("a-fairly-long-resolved-blob-payload"));

  WideColumns resolved_columns;
  resolved_columns.emplace_back(columns.columns()[0].name(),
                                columns.columns()[0].value());
  resolved_columns.emplace_back(columns.columns()[1].name(),
                                Slice(extra_buffers.front()));

  PinnableWideColumnsHelper::ResolveColumns(
      columns, std::move(resolved_columns), std::move(extra_buffers));

  const char* const inline_data = columns.columns()[0].value().data();
  const char* const resolved_data = columns.columns()[1].value().data();

  // Move construction: nodes are stolen, not relocated -- data pointers stable.
  PinnableWideColumns moved(std::move(columns));
  ASSERT_EQ(moved.columns().size(), 2);
  EXPECT_EQ(moved.columns()[0].value(), Slice("default-value"));
  EXPECT_EQ(moved.columns()[1].value(),
            Slice("a-fairly-long-resolved-blob-payload"));
  EXPECT_EQ(moved.columns()[0].value().data(), inline_data);
  EXPECT_EQ(moved.columns()[1].value().data(), resolved_data);

  // Move assignment: same guarantee.
  PinnableWideColumns move_assigned;
  move_assigned = std::move(moved);
  ASSERT_EQ(move_assigned.columns().size(), 2);
  EXPECT_EQ(move_assigned.columns()[0].value(), Slice("default-value"));
  EXPECT_EQ(move_assigned.columns()[1].value(),
            Slice("a-fairly-long-resolved-blob-payload"));
  EXPECT_EQ(move_assigned.columns()[0].value().data(), inline_data);
  EXPECT_EQ(move_assigned.columns()[1].value().data(), resolved_data);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
