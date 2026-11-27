//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "cache/cache_reservation_manager.h"
#include "db/arena_wrapped_db_iter.h"
#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_reference_census.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/column_family.h"
#include "db/db_iter.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/wide/wide_column_test_util.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "file/writable_file_writer.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/table_builder.h"
#include "test_util/mock_time_env.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void CorruptPinnedBlobIndexOnCleanup(void* arg1, void* /*arg2*/) {
  auto* blob_index = static_cast<std::string*>(arg1);
  assert(blob_index != nullptr);
  assert(!blob_index->empty());
  (*blob_index)[0] = static_cast<char>(BlobIndex::Type::kUnknown);
}

class RejectingReservationManager : public CacheReservationManager {
 public:
  explicit RejectingReservationManager(size_t limit) : limit_(limit) {}

  class Handle : public CacheReservationHandle {
   public:
    explicit Handle(RejectingReservationManager* manager) : manager_(manager) {}
    ~Handle() override {
      manager_->UpdateCacheReservation(memory_used_, false)
          .PermitUncheckedError();
    }
    Status Resize(size_t memory_used) override {
      Status status = manager_->UpdateCacheReservation(
          memory_used > memory_used_ ? memory_used - memory_used_
                                     : memory_used_ - memory_used,
          memory_used > memory_used_);
      memory_used_ = memory_used;
      return status;
    }

   private:
    RejectingReservationManager* manager_;
    size_t memory_used_ = 0;
  };

  Status UpdateCacheReservation(size_t new_memory_used) override {
    memory_used_ = new_memory_used;
    if (on_update) {
      on_update(new_memory_used);
    }
    if (new_memory_used > limit_) {
      return Status::MemoryLimit("test reservation limit");
    }
    return Status::OK();
  }

  Status UpdateCacheReservation(size_t memory_used_delta,
                                bool increase) override {
    return UpdateCacheReservation(increase ? memory_used_ + memory_used_delta
                                           : memory_used_ - memory_used_delta);
  }

  Status TryIncreaseCacheReservation(size_t memory_used_delta) override {
    const size_t old_memory_used = memory_used_;
    Status status = UpdateCacheReservation(memory_used_delta, true);
    if (!status.ok()) {
      memory_used_ = old_memory_used;
    }
    return status;
  }

  Status MakeCacheReservation(
      size_t incremental_memory_used,
      std::unique_ptr<CacheReservationHandle>* handle) override {
    handle->reset(new Handle(this));
    return (*handle)->Resize(incremental_memory_used);
  }

  size_t GetTotalReservedCacheSize() override { return memory_used_; }
  size_t GetTotalMemoryUsed() override { return memory_used_; }

  // Called with the new total on every charge update.
  std::function<void(size_t)> on_update;

 private:
  size_t limit_;
  size_t memory_used_ = 0;
};

constexpr uint32_t kTestColumnFamilyId = 0;
constexpr uint64_t kTestSstFileNumber = 1;

BlobIndex DecodeBlobIndexForTest(const std::string& encoded) {
  BlobIndex blob_index;
  EXPECT_OK(blob_index.DecodeFrom(encoded));
  return blob_index;
}

// The metadata that a collector writes in a test: the data blocks, the index
// block and the descriptor.
struct MetadataForTest {
  std::vector<std::string> blocks;
  std::string index;
  BlobReferenceMetadataDescriptor descriptor;
};

// A block writer that places each block after the previous one, starting at
// offset 1000. Like a table, it leaves BlockBasedTable::kBlockTrailerSize (5
// bytes) after each block for the block trailer.
BlobReferenceMetadataCollector::BlockWriter BlockWriterForTest(
    MetadataForTest* metadata) {
  return [metadata, next_offset = uint64_t{1000}](
             const Slice& block, bool is_index_block,
             BlockHandle* handle) mutable -> Status {
    if (is_index_block) {
      metadata->index = block.ToString();
    } else {
      metadata->blocks.emplace_back(block.data(), block.size());
    }
    *handle = BlockHandle(next_offset, block.size());
    next_offset += block.size() + 5;
    return Status::OK();
  };
}

Status FinishForTest(
    BlobReferenceMetadataCollector* collector, MetadataForTest* metadata,
    std::shared_ptr<const BlobReferenceSstSummary>* summary = nullptr) {
  return collector->Finish(BlockWriterForTest(metadata), &metadata->descriptor,
                           summary);
}

// Returns the index groups that are not kDirect. BlobReferenceSstSummary holds
// the same groups.
std::vector<BlobReferenceGroup> IndirectGroupsForTest(
    const BlobReferenceIndex& index) {
  std::vector<BlobReferenceGroup> groups;
  for (const BlobReferenceIndexEntry& entry : index.groups) {
    if (entry.group.encoding_class != BlobReferenceEncodingClass::kDirect) {
      groups.push_back(entry.group);
    }
  }
  return groups;
}

BlobReferenceBlockReader BlockReaderForTest(const MetadataForTest& metadata) {
  return [&metadata](size_t block, Slice* contents) -> Status {
    if (block >= metadata.blocks.size()) {
      return Status::Corruption("Block ordinal out of range");
    }
    *contents = metadata.blocks[block];
    return Status::OK();
  };
}

// Decodes the index and walks every block.
Status DecodeAllForTest(const MetadataForTest& metadata,
                        BlobReferenceIndex* index,
                        std::vector<BlobReference>* references) {
  Status status =
      DecodeBlobReferenceIndex(metadata.index, metadata.descriptor, index);
  if (!status.ok()) {
    return status;
  }
  if (index->blocks.size() != metadata.blocks.size()) {
    return Status::Corruption("Index and block count differ");
  }
  return ForEachBlobReference(*index, BlockReaderForTest(metadata),
                              [&](const BlobReference& reference) {
                                references->push_back(reference);
                                return Status::OK();
                              });
}

// Position of the group with key `key` in `index.groups`, or
// index.groups.size() if the SST has no such group.
size_t FindGroupForTest(const BlobReferenceIndex& index,
                        const BlobReferenceGroupKey& key) {
  for (size_t i = 0; i < index.groups.size(); ++i) {
    if (index.groups[i].group.group_key() == key) {
      return i;
    }
  }
  return index.groups.size();
}

// Reads the references of one group through the index, in offset order.
Status ReadGroupForTest(const MetadataForTest& metadata,
                        const BlobReferenceIndex& index, size_t group,
                        std::vector<BlobReference>* references) {
  return ForEachBlobReferenceInGroup(index, group, BlockReaderForTest(metadata),
                                     [&](const BlobReference& reference) {
                                       references->push_back(reference);
                                       return Status::OK();
                                     });
}

// Counts the groups that take the unsorted fallback while this object is
// alive. It uses the sync point at the start of SortUnsortedGroup(), so it
// counts only in debug builds.
class UnsortedFallbackCounterForTest {
 public:
  UnsortedFallbackCounterForTest() {
    SyncPoint::GetInstance()->SetCallBack(
        "BlobReferenceMetadataCollector::SortUnsortedGroup",
        [this](void*) { ++count_; });
    SyncPoint::GetInstance()->EnableProcessing();
  }
  ~UnsortedFallbackCounterForTest() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearCallBack(
        "BlobReferenceMetadataCollector::SortUnsortedGroup");
  }
  UnsortedFallbackCounterForTest(const UnsortedFallbackCounterForTest&) =
      delete;
  UnsortedFallbackCounterForTest& operator=(
      const UnsortedFallbackCounterForTest&) = delete;

  uint64_t count() const { return count_; }

 private:
  uint64_t count_ = 0;
};

// Builds metadata bytes field by field.
struct BytesForTest {
  std::string bytes;
  BytesForTest& Byte(uint8_t value) {
    bytes.push_back(static_cast<char>(value));
    return *this;
  }
  BytesForTest& Varint(uint64_t value) {
    PutVarint64(&bytes, value);
    return *this;
  }
  BytesForTest& Fixed32(uint32_t value) {
    PutFixed32(&bytes, value);
    return *this;
  }
  BytesForTest& Append(const std::string& more) {
    bytes += more;
    return *this;
  }
};

auto ReferenceFieldsForTest(const BlobReference& reference) {
  return std::make_tuple(
      reference.encoding_class, reference.generation_file_number,
      reference.origin_file_number, reference.original_value_offset,
      reference.stored_value_size, reference.record_bytes,
      reference.compression, reference.stable_checksum,
      reference.stored_checksum);
}

BlobIndex UnflattenedForTest(uint64_t origin, uint64_t offset,
                             uint64_t stored_size) {
  std::string encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(
      &encoded, {.origin_file_number = origin,
                 .original_value_offset = offset,
                 .stored_value_size = stored_size,
                 .compression = kZSTD,
                 .stable_checksum = static_cast<uint32_t>(offset),
                 .stored_checksum = static_cast<uint32_t>(offset + 1)});
  return DecodeBlobIndexForTest(encoded);
}

BlobIndex LegacyForTest(uint64_t file_number, uint64_t offset) {
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, file_number, offset, /*size=*/100,
                        kNoCompression);
  return DecodeBlobIndexForTest(encoded);
}

}  // namespace

TEST(BlobIndexTest, SameFileBlobIndex) {
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, kCurrentFileBlobIndexFileNumber,
                        /*offset=*/123, /*size=*/456, kNoCompression);

  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  EXPECT_TRUE(blob_index.IsSameFile());
  EXPECT_EQ(blob_index.file_number(), kCurrentFileBlobIndexFileNumber);
  EXPECT_NE(blob_index.DebugString(false).find("file:same"), std::string::npos);
}

TEST(BlobIndexTest, EmptyBlobIndexIsCorruption) {
  BlobIndex blob_index;
  ASSERT_EQ(blob_index.DecodeFrom(Slice()).code(), Status::Code::kCorruption);
}

TEST(BlobIndexTest, IndirectBlobIndexRoundTrip) {
  constexpr uint64_t kGeneration = 301;
  constexpr uint64_t kOrigin = 101;
  constexpr uint64_t kOffset = 123456;
  constexpr uint64_t kSize = 789;
  constexpr uint32_t kChecksum = 0x89abcdefU;
  constexpr uint32_t kStoredChecksum = 0x01234567U;

  std::string encoded;
  BlobIndex::EncodeIndirectBlob(&encoded, kGeneration,
                                {.origin_file_number = kOrigin,
                                 .original_value_offset = kOffset,
                                 .stored_value_size = kSize,
                                 .compression = kZSTD,
                                 .stable_checksum = kChecksum,
                                 .stored_checksum = kStoredChecksum});

  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  EXPECT_TRUE(blob_index.IsIndirect());
  EXPECT_FALSE(blob_index.IsInlined());
  EXPECT_FALSE(blob_index.IsSameFile());
  EXPECT_EQ(blob_index.file_number(), kGeneration);
  EXPECT_EQ(blob_index.generation_file_number(), kGeneration);
  EXPECT_EQ(blob_index.origin_file_number(), kOrigin);
  EXPECT_EQ(blob_index.offset(), kOffset);
  EXPECT_EQ(blob_index.original_value_offset(), kOffset);
  EXPECT_EQ(blob_index.size(), kSize);
  EXPECT_EQ(blob_index.compression(), kZSTD);
  EXPECT_EQ(blob_index.stable_checksum(), kChecksum);
  EXPECT_EQ(blob_index.stored_checksum(), kStoredChecksum);

  std::string reencoded;
  blob_index.EncodeTo(&reencoded);
  EXPECT_EQ(reencoded, encoded);

  ASSERT_OK(blob_index.SetGenerationFileNumber(/*generation_file_number=*/401));
  EXPECT_EQ(blob_index.generation_file_number(), 401U);
  EXPECT_EQ(blob_index.origin_file_number(), kOrigin);
  EXPECT_EQ(blob_index.original_value_offset(), kOffset);
  EXPECT_EQ(blob_index.size(), kSize);
  EXPECT_EQ(blob_index.compression(), kZSTD);
  EXPECT_EQ(blob_index.stable_checksum(), kChecksum);
  blob_index.EncodeTo(&reencoded);
  BlobIndex flattened;
  ASSERT_OK(flattened.DecodeFrom(reencoded));
  EXPECT_EQ(flattened.generation_file_number(), 401U);
  EXPECT_EQ(flattened.origin_file_number(), kOrigin);

  // The type follows the generation: equal to the origin gives the short
  // unflattened type, above it gives the flattened type, and below it is
  // rejected.
  ASSERT_TRUE(
      blob_index.SetGenerationFileNumber(kOrigin - 1).IsInvalidArgument());
  ASSERT_OK(blob_index.SetGenerationFileNumber(kOrigin));
  blob_index.EncodeTo(&reencoded);
  ASSERT_EQ(static_cast<BlobIndex::Type>(reencoded[0]),
            BlobIndex::Type::kIndirectBlob);
  ASSERT_OK(flattened.DecodeFrom(reencoded));
  EXPECT_EQ(flattened.generation_file_number(), kOrigin);
  ASSERT_OK(blob_index.SetGenerationFileNumber(401));
  blob_index.EncodeTo(&reencoded);
  ASSERT_EQ(static_cast<BlobIndex::Type>(reencoded[0]),
            BlobIndex::Type::kIndirectFlattenedBlob);
}

// An uncompressed indirect index stores no stored CRC, so stored_checksum()
// derives it from the stable CRC. A compressed index stores it. In both cases
// the accessor, DebugString() and EncodeTo() must give the encoded values.
TEST(BlobIndexTest, IndirectStoredChecksumMatchesEncoding) {
  constexpr uint32_t kStableChecksum = 0x89abcdefU;
  constexpr uint32_t kEncodedStoredChecksum = 0x01234567U;
  for (const CompressionType compression : {kNoCompression, kZSTD}) {
    // Generation 301 is above origin 101, so it encodes a flattened index;
    // generation 101 encodes an unflattened one.
    for (const uint64_t generation : {uint64_t{101}, uint64_t{301}}) {
      SCOPED_TRACE("compression=" + CompressionTypeToString(compression) +
                   " generation=" + std::to_string(generation));
      std::string encoded;
      BlobIndex::EncodeIndirectBlob(
          &encoded, generation,
          {.origin_file_number = 101,
           .original_value_offset = 123456,
           .stored_value_size = 789,
           .compression = compression,
           .stable_checksum = kStableChecksum,
           .stored_checksum = kEncodedStoredChecksum});
      const uint32_t expected_stored_checksum =
          compression == kNoCompression
              ? BlobIndex::ExtendChecksumWithCompression(kStableChecksum,
                                                         kNoCompression)
              : kEncodedStoredChecksum;

      BlobIndex blob_index;
      ASSERT_OK(blob_index.DecodeFrom(encoded));
      EXPECT_EQ(blob_index.stable_checksum(), kStableChecksum);
      EXPECT_EQ(blob_index.stored_checksum(), expected_stored_checksum);
      EXPECT_TRUE(blob_index.fingerprint() ==
                  (StableBlobFingerprint{789, compression, kStableChecksum,
                                         expected_stored_checksum}));
      EXPECT_NE(blob_index.DebugString(/*output_hex=*/false)
                    .find(" stored_crc:" +
                          std::to_string(expected_stored_checksum) + " "),
                std::string::npos);

      std::string reencoded;
      blob_index.EncodeTo(&reencoded);
      EXPECT_EQ(reencoded, encoded);
    }
  }
}

TEST(BlobIndexTest, IndirectBlobFieldOrder) {
  // Hand-made bytes in the documented order: generation, origin, original
  // offset. All three differ, so a swapped order fails.
  std::string encoded;
  encoded.push_back(static_cast<char>(BlobIndex::Type::kIndirectFlattenedBlob));
  PutVarint64(&encoded, /*generation=*/7);
  PutVarint64(&encoded, /*origin=*/3);
  PutVarint64(&encoded, /*original_offset=*/100);
  PutVarint64(&encoded, /*size=*/50);
  encoded.push_back(static_cast<char>(kZSTD));
  PutFixed32(&encoded, /*logical_crc=*/0x11223344U);
  PutFixed32(&encoded, /*stored_crc=*/0x55667788U);

  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  EXPECT_EQ(blob_index.generation_file_number(), 7);
  EXPECT_EQ(blob_index.origin_file_number(), 3);
  EXPECT_EQ(blob_index.original_value_offset(), 100);
  EXPECT_EQ(blob_index.size(), 50);
  EXPECT_EQ(blob_index.compression(), kZSTD);
  EXPECT_EQ(blob_index.stable_checksum(), 0x11223344U);
  EXPECT_EQ(blob_index.stored_checksum(), 0x55667788U);

  std::string reencoded;
  BlobIndex::EncodeIndirectBlob(&reencoded, 7,
                                {.origin_file_number = 3,
                                 .original_value_offset = 100,
                                 .stored_value_size = 50,
                                 .compression = kZSTD,
                                 .stable_checksum = 0x11223344U,
                                 .stored_checksum = 0x55667788U});
  EXPECT_EQ(reencoded, encoded);
}

TEST(BlobIndexTest, UnflattenedBlobNormalizesGenerationToOrigin) {
  std::string encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(&encoded,
                                           {.origin_file_number = 77,
                                            .original_value_offset = 88,
                                            .stored_value_size = 99,
                                            .compression = kLZ4Compression,
                                            .stable_checksum = 1234,
                                            .stored_checksum = 4321});
  // Type 3 stores no generation: type, origin, offset, size, compression and
  // two CRCs.
  ASSERT_EQ(encoded[0], static_cast<char>(BlobIndex::Type::kIndirectBlob));
  ASSERT_EQ(encoded.size(), 1U + 1 + 1 + 1 + 1 + 4 + 4);

  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  EXPECT_EQ(blob_index.generation_file_number(), 77);
  EXPECT_EQ(blob_index.origin_file_number(), 77);
  std::string reencoded;
  blob_index.EncodeTo(&reencoded);
  EXPECT_EQ(reencoded, encoded);
}

TEST(BlobIndexTest, FlattenedBlobRoundTripAndCanonicalGeneration) {
  // Type 4 stores the generation, which must be above the origin.
  std::string encoded;
  BlobIndex::EncodeIndirectBlob(&encoded, 200,
                                {.origin_file_number = 77,
                                 .original_value_offset = 88,
                                 .stored_value_size = 99,
                                 .compression = kNoCompression,
                                 .stable_checksum = 1234,
                                 .stored_checksum = 0});
  ASSERT_EQ(encoded[0],
            static_cast<char>(BlobIndex::Type::kIndirectFlattenedBlob));
  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  EXPECT_EQ(blob_index.generation_file_number(), 200);
  EXPECT_EQ(blob_index.origin_file_number(), 77);
  std::string reencoded;
  blob_index.EncodeTo(&reencoded);
  EXPECT_EQ(reencoded, encoded);

  for (const uint64_t generation : {uint64_t{77}, uint64_t{76}}) {
    SCOPED_TRACE(generation);
    std::string bad;
    bad.push_back(static_cast<char>(BlobIndex::Type::kIndirectFlattenedBlob));
    PutVarint64(&bad, generation);
    bad.append(encoded.substr(2));
    BlobIndex decoded;
    EXPECT_TRUE(decoded.DecodeFrom(bad).IsCorruption());
  }
  {
    std::string unknown(encoded);
    unknown[0] = static_cast<char>(BlobIndex::Type::kUnknown);
    BlobIndex decoded;
    EXPECT_TRUE(decoded.DecodeFrom(unknown).IsCorruption());
  }
  for (size_t size = 0; size < encoded.size(); ++size) {
    BlobIndex decoded;
    EXPECT_TRUE(decoded.DecodeFrom(Slice(encoded.data(), size)).IsCorruption())
        << "accepted truncated size " << size;
  }
}

TEST(BlobIndexTest, IndirectBlobIndexRejectsInvalidFileNumbers) {
  // A file number must be in [1, BlobIndex::kMaxFileNumber]. The test writes
  // the type byte and the file numbers by hand and copies the other fields
  // from a valid index of origin 77 (one varint byte).
  std::string valid;
  BlobIndex::EncodeUnflattenedIndirectBlob(&valid,
                                           {.origin_file_number = 77,
                                            .original_value_offset = 88,
                                            .stored_value_size = 99,
                                            .compression = kNoCompression,
                                            .stable_checksum = 1234,
                                            .stored_checksum = 0});
  const std::string other_fields = valid.substr(2);
  const auto encode = [&](BlobIndex::Type type,
                          std::initializer_list<uint64_t> file_numbers) {
    std::string encoded(1, static_cast<char>(type));
    for (uint64_t file_number : file_numbers) {
      PutVarint64(&encoded, file_number);
    }
    return encoded + other_fields;
  };
  constexpr uint64_t kMax = BlobIndex::kMaxFileNumber;
  BlobIndex decoded;
  ASSERT_OK(decoded.DecodeFrom(encode(BlobIndex::Type::kIndirectBlob, {kMax})));
  ASSERT_OK(decoded.DecodeFrom(
      encode(BlobIndex::Type::kIndirectFlattenedBlob, {kMax, 77})));
  for (const std::string& bad :
       {encode(BlobIndex::Type::kIndirectBlob, {0}),
        encode(BlobIndex::Type::kIndirectBlob, {kMax + 1}),
        encode(BlobIndex::Type::kIndirectBlob,
               {std::numeric_limits<uint64_t>::max()}),
        encode(BlobIndex::Type::kIndirectFlattenedBlob, {kMax + 1, 77}),
        encode(BlobIndex::Type::kIndirectFlattenedBlob,
               {kMax + 2, kMax + 1})}) {
    SCOPED_TRACE(Slice(bad).ToString(/*hex=*/true));
    EXPECT_TRUE(decoded.DecodeFrom(bad).IsCorruption());
  }
}

TEST(BlobIndexTest, StableChecksumEncodingAndFlattening) {
  const std::string payload = "stored-value";
  for (CompressionType compression : {kNoCompression, kZSTD}) {
    SCOPED_TRACE(static_cast<int>(compression));
    const uint32_t logical = crc32c::Value(payload.data(), payload.size());
    const uint32_t stored =
        BlobIndex::ComputeStoredChecksum(payload, compression);
    std::string encoded;
    BlobIndex::EncodeUnflattenedIndirectBlob(
        &encoded, {.origin_file_number = 100,
                   .original_value_offset = 42,
                   .stored_value_size = payload.size(),
                   .compression = compression,
                   .stable_checksum = logical,
                   .stored_checksum = stored});
    BlobIndex index;
    ASSERT_OK(index.DecodeFrom(encoded));
    ASSERT_OK(index.Validate());
    ASSERT_EQ(index.stable_checksum(), logical);
    ASSERT_EQ(index.stored_checksum(), stored);
    // The stored CRC is written only for compressed values; for kNoCompression
    // it is derived from the logical CRC.
    std::string uncompressed;
    BlobIndex::EncodeUnflattenedIndirectBlob(
        &uncompressed, {.origin_file_number = 100,
                        .original_value_offset = 42,
                        .stored_value_size = payload.size(),
                        .compression = kNoCompression,
                        .stable_checksum = logical,
                        .stored_checksum = BlobIndex::ComputeStoredChecksum(
                            payload, kNoCompression)});
    ASSERT_EQ(encoded.size(),
              uncompressed.size() + (compression == kNoCompression ? 0 : 4));
    ASSERT_OK(index.SetGenerationFileNumber(200));
    std::string flattened;
    index.EncodeTo(&flattened);
    BlobIndex decoded;
    ASSERT_OK(decoded.DecodeFrom(flattened));
    ASSERT_EQ(decoded.generation_file_number(), 200U);
    ASSERT_EQ(decoded.stored_checksum(), stored);
    BlobReference reference;
    bool included = false;
    ASSERT_OK(BlobReference::FromBlobIndex(decoded, &reference, &included));
    ASSERT_TRUE(included);
    ASSERT_EQ(reference.stored_checksum, stored);
    for (size_t length = 0; length < encoded.size(); ++length) {
      BlobIndex truncated;
      ASSERT_TRUE(
          truncated.DecodeFrom(Slice(encoded.data(), length)).IsCorruption());
    }
  }
}

TEST(BlobIndexTest, IndirectBlobIndexRejectsMalformedEncoding) {
  std::string encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(&encoded,
                                           {.origin_file_number = 77,
                                            .original_value_offset = 88,
                                            .stored_value_size = 99,
                                            .compression = kNoCompression,
                                            .stable_checksum = 1234,
                                            .stored_checksum = 4321});

  for (size_t size = 0; size < encoded.size(); ++size) {
    BlobIndex blob_index;
    EXPECT_TRUE(
        blob_index.DecodeFrom(Slice(encoded.data(), size)).IsCorruption())
        << "accepted truncated size " << size;
  }

  {
    // An extra byte after the compression byte must make decoding fail. The
    // decoder must not accept the shifted bytes as a valid index with other
    // checksums.
    std::string old_layout(encoded);
    old_layout.insert(old_layout.begin() + 5, '\x02');
    BlobIndex blob_index;
    EXPECT_TRUE(blob_index.DecodeFrom(old_layout).IsCorruption());
  }
  {
    std::string noncanonical_origin(encoded);
    noncanonical_origin[1] = static_cast<char>(0x80 | encoded[1]);
    noncanonical_origin.insert(noncanonical_origin.begin() + 2, '\0');
    BlobIndex blob_index;
    EXPECT_TRUE(blob_index.DecodeFrom(noncanonical_origin).IsCorruption());
  }
  {
    std::string trailing(encoded);
    trailing.push_back('x');
    BlobIndex blob_index;
    EXPECT_TRUE(blob_index.DecodeFrom(trailing).IsCorruption());
  }
  {
    std::string invalid_compression(encoded);
    // Here the compression byte is at position 4, after the type byte and three
    // one-byte varints (origin, offset and size).
    invalid_compression[4] = static_cast<char>(kDisableCompressionOption);
    BlobIndex blob_index;
    EXPECT_TRUE(blob_index.DecodeFrom(invalid_compression).IsCorruption());
  }
}

namespace {

// How BuildTableForTest() hands each entry to the table builder.
enum class TableAddModeForTest {
  // TableBuilder::Add().
  kAdd,
  // TableBuilder::TryAdd() without a decoded BlobIndex.
  kTryAdd,
  // TableBuilder::TryAdd() with the caller's decode of each kTypeBlobIndex
  // value in TableBuilderAddContext::decoded_blob_index, as compaction does.
  kTryAddWithDecode,
};

// Builds one block-based SST in memory from `entries` (internal keys in
// order) with exact blob reference metadata, and returns its bytes. A fixed
// clock keeps the time properties equal across builds, so two builds of the
// same entries give the same bytes. `*status` gets the builder status.
std::string BuildTableForTest(
    const std::vector<std::pair<std::string, std::string>>& entries,
    TableAddModeForTest mode, Status* status) {
  Options options;
  options.enable_blob_indirection = true;
  // Format version 6 and above mix a random value into each block checksum
  // (FormatVersionUsesContextChecksum()), so two builds would differ. Format
  // version 5 has no such value.
  BlockBasedTableOptions table_options;
  table_options.format_version = 5;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ImmutableOptions ioptions(options);
  MockSystemClock clock(SystemClock::Default());
  ioptions.clock = &clock;
  const MutableCFOptions moptions(options);
  const InternalKeyComparator icmp(options.comparator);
  const InternalTblPropCollFactories factories;
  // TableBuilderOptions keeps references to these, so they must outlive the
  // builder.
  const ReadOptions read_options;
  const WriteOptions write_options;
  const CompressionOptions compression_options;
  // Column family 0 and file number 12 are example values; the exact
  // metadata needs a known column family and a file number.
  TableBuilderOptions tbo(
      ioptions, moptions, read_options, write_options, icmp, &factories,
      kNoCompression, compression_options, /*_column_family_id=*/0,
      kDefaultColumnFamilyName, /*_level=*/1, kUnknownNewestKeyTime,
      /*_is_bottommost=*/false, TableFileCreationReason::kCompaction,
      /*_oldest_key_time=*/0, /*_db_id=*/"db", /*_db_session_id=*/"session",
      /*_target_file_size=*/0, /*_cur_file_num=*/12);
  tbo.collect_blob_reference_metadata = true;

  auto sink = std::make_unique<test::StringSink>();
  test::StringSink* const sink_ptr = sink.get();
  WritableFileWriter file_writer(std::move(sink), "table", FileOptions());
  std::unique_ptr<TableBuilder> builder(
      options.table_factory->NewTableBuilder(tbo, &file_writer));
  for (const auto& [key, value] : entries) {
    BlobIndex decoded;
    TableBuilderAddContext context;
    if (mode == TableAddModeForTest::kTryAddWithDecode &&
        ExtractValueType(key) == kTypeBlobIndex &&
        decoded.DecodeFrom(value).ok()) {
      context.decoded_blob_index = &decoded;
    }
    if (mode == TableAddModeForTest::kAdd) {
      builder->Add(key, value);
    } else {
      EXPECT_EQ(builder->TryAdd(key, value, &context),
                TableBuilderAddResult::kAdded);
    }
  }
  if (builder->status().ok()) {
    *status = builder->Finish();
    EXPECT_OK(file_writer.Flush(IOOptions()));
  } else {
    *status = builder->status();
    builder->Abandon();
  }
  return sink_ptr->contents();
}

std::string InternalKeyForTest(const std::string& user_key,
                               SequenceNumber seqno, ValueType type) {
  std::string encoded;
  AppendInternalKey(&encoded, ParsedInternalKey(user_key, seqno, type));
  return encoded;
}

}  // namespace

// Compaction passes its own decode of each BlobIndex to the table builder.
// The SST must have the same bytes (and so the same properties and blob
// reference metadata) as when the builder decodes the value itself.
TEST(BlobReferenceMetadataTest, SharedBlobIndexDecodeGivesSameTable) {
  std::string legacy;
  BlobIndex::EncodeBlob(&legacy, /*file_number=*/5, /*offset=*/100,
                        /*size=*/50, kNoCompression);
  std::string unflattened;
  BlobIndex::EncodeUnflattenedIndirectBlob(&unflattened,
                                           {.origin_file_number = 20,
                                            .original_value_offset = 200,
                                            .stored_value_size = 60,
                                            .compression = kNoCompression,
                                            .stable_checksum = 1234,
                                            .stored_checksum = 0});
  std::string flattened;
  BlobIndex::EncodeIndirectBlob(&flattened, /*generation=*/40,
                                {.origin_file_number = 30,
                                 .original_value_offset = 300,
                                 .stored_value_size = 70,
                                 .compression = kZSTD,
                                 .stable_checksum = 5678,
                                 .stored_checksum = 8765});
  const std::vector<std::pair<std::string, std::string>> entries = {
      {InternalKeyForTest("a", 10, kTypeBlobIndex), legacy},
      {InternalKeyForTest("b", 11, kTypeValue), "value"},
      {InternalKeyForTest("c", 12, kTypeBlobIndex), unflattened},
      {InternalKeyForTest("d", 13, kTypeBlobIndex), flattened},
  };

  Status add_status;
  const std::string expected =
      BuildTableForTest(entries, TableAddModeForTest::kAdd, &add_status);
  ASSERT_OK(add_status);
  // The builder wrote the exact reference metadata.
  ASSERT_NE(expected.find(kBlobReferenceIndexBlockName), std::string::npos);
  for (const TableAddModeForTest mode :
       {TableAddModeForTest::kTryAdd, TableAddModeForTest::kTryAddWithDecode}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    Status status;
    EXPECT_EQ(BuildTableForTest(entries, mode, &status), expected);
    EXPECT_OK(status);
  }
}

// A malformed BlobIndex value has no shared decode, so the builder decodes it
// itself and returns the same error as Add().
TEST(BlobReferenceMetadataTest, MalformedBlobIndexFailsTheSameWay) {
  const std::vector<std::pair<std::string, std::string>> entries = {
      {InternalKeyForTest("a", 10, kTypeBlobIndex), "\xff"},
  };
  Status add_status;
  BuildTableForTest(entries, TableAddModeForTest::kAdd, &add_status);
  ASSERT_EQ(add_status.code(), Status::Code::kCorruption);
  for (const TableAddModeForTest mode :
       {TableAddModeForTest::kTryAdd, TableAddModeForTest::kTryAddWithDecode}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    Status status;
    BuildTableForTest(entries, mode, &status);
    EXPECT_EQ(status.ToString(), add_status.ToString());
  }
}

TEST(BlobReferenceMetadataTest, EmptyRoundTrip) {
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  EXPECT_TRUE(metadata.blocks.empty());
  EXPECT_TRUE(metadata.descriptor.IsEmpty());

  std::string encoded_descriptor;
  metadata.descriptor.EncodeTo(&encoded_descriptor);
  EXPECT_EQ(encoded_descriptor.size(),
            BlobReferenceMetadataDescriptor::kEncodedSize);
  BlobReferenceMetadataDescriptor decoded_descriptor;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(encoded_descriptor,
                                                        &decoded_descriptor));
  EXPECT_TRUE(decoded_descriptor.IsEmpty());
  EXPECT_TRUE(decoded_descriptor.HasSstIdentity());
  EXPECT_EQ(decoded_descriptor.column_family_id, 0U);
  EXPECT_EQ(decoded_descriptor.sst_file_number, 1U);

  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  EXPECT_TRUE(index.blocks.empty());
  EXPECT_TRUE(index.groups.empty());
  EXPECT_TRUE(references.empty());
}

TEST(BlobReferenceMetadataTest, RejectsOtherDescriptorVersions) {
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  std::string encoded;
  metadata.descriptor.EncodeTo(&encoded);
  BlobReferenceMetadataDescriptor decoded;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(encoded, &decoded));

  for (uint32_t version : {0U, 2U, 4U}) {
    SCOPED_TRACE(version);
    std::string other_version = encoded;
    EncodeFixed32(other_version.data() + 4, version);
    EncodeFixed32(
        other_version.data() + other_version.size() - sizeof(uint32_t),
        crc32c::Value(other_version.data(),
                      other_version.size() - sizeof(uint32_t)));
    EXPECT_TRUE(
        BlobReferenceMetadataDescriptor::DecodeFrom(other_version, &decoded)
            .IsCorruption());
  }
}

TEST(BlobReferenceMetadataTest, FinishReturnsSummaryOnlyOnRequest) {
  std::string encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(&encoded,
                                           {.origin_file_number = 42,
                                            .original_value_offset = 700,
                                            .stored_value_size = 800,
                                            .compression = kZSTD,
                                            .stable_checksum = 123,
                                            .stored_checksum = 321});
  const BlobIndex blob_index = DecodeBlobIndexForTest(encoded);
  for (const bool want_summary : {false, true}) {
    SCOPED_TRACE(want_summary);
    BlobReferenceMetadataCollector collector(
        nullptr,
        BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                 .sst_file_number = kTestSstFileNumber});
    ASSERT_OK(collector.Add(blob_index, /*user_key_size=*/0));
    MetadataForTest metadata;
    std::shared_ptr<const BlobReferenceSstSummary> summary;
    ASSERT_OK(FinishForTest(&collector, &metadata,
                            want_summary ? &summary : nullptr));
    // The descriptor's index handle points to the index block, which Finish()
    // writes after the data blocks.
    EXPECT_EQ(metadata.descriptor.index_handle.size(), metadata.index.size());
    EXPECT_EQ(metadata.descriptor.reference_count, 1U);
    ASSERT_EQ(summary != nullptr, want_summary);
    if (want_summary) {
      EXPECT_EQ(summary->file_number, kTestSstFileNumber);
      EXPECT_EQ(summary->groups.size(), 1U);
    }
    // Finish() runs only once. A second Finish() and a later Add() return
    // InvalidArgument and write nothing.
    MetadataForTest again;
    EXPECT_TRUE(FinishForTest(&collector, &again).IsInvalidArgument());
    EXPECT_TRUE(again.blocks.empty());
    EXPECT_TRUE(
        collector.Add(blob_index, /*user_key_size=*/0).IsInvalidArgument());
  }
}

TEST(BlobReferenceMetadataTest, CollectSortAndDecode) {
  std::string flattened_encoded;
  BlobIndex::EncodeIndirectBlob(&flattened_encoded, 99,
                                {.origin_file_number = 42,
                                 .original_value_offset = 900,
                                 .stored_value_size = 600,
                                 .compression = kSnappyCompression,
                                 .stable_checksum = 456,
                                 .stored_checksum = 654});
  std::string same_file_encoded;
  BlobIndex::EncodeBlob(&same_file_encoded, kCurrentFileBlobIndexFileNumber,
                        /*offset=*/100, /*size=*/200, kNoCompression);
  std::string inline_encoded;
  BlobIndex::EncodeInlinedTTL(&inline_encoded, /*expiration=*/123, "inline");

  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  ASSERT_OK(collector.Add(DecodeBlobIndexForTest(flattened_encoded),
                          /*user_key_size=*/0));
  ASSERT_OK(
      collector.Add(UnflattenedForTest(42, 700, 800), /*user_key_size=*/0));
  ASSERT_OK(collector.Add(LegacyForTest(41, 500), /*user_key_size=*/0));
  ASSERT_OK(collector.Add(DecodeBlobIndexForTest(same_file_encoded),
                          /*user_key_size=*/0));
  ASSERT_OK(collector.Add(DecodeBlobIndexForTest(inline_encoded),
                          /*user_key_size=*/0));

  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  EXPECT_EQ(metadata.descriptor.reference_count, 3);
  EXPECT_EQ(metadata.descriptor.stored_value_bytes, 1500);
  // The same-file and inlined indexes are skipped. The three small groups that
  // remain share one block.
  ASSERT_EQ(metadata.blocks.size(), 1U);

  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(index.groups.size(), 3U);
  ASSERT_EQ(references.size(), 3);
  EXPECT_EQ(references[0].encoding_class, BlobReferenceEncodingClass::kDirect);
  EXPECT_EQ(references[0].generation_file_number, 41);
  EXPECT_EQ(references[0].origin_file_number, 41);
  EXPECT_EQ(references[1].encoding_class,
            BlobReferenceEncodingClass::kIndirectUnflattened);
  EXPECT_EQ(references[2].encoding_class,
            BlobReferenceEncodingClass::kIndirectFlattened);
  EXPECT_EQ(references[2].generation_file_number, 99);
  EXPECT_EQ(references[2].origin_file_number, 42);
  EXPECT_EQ(references[2].stable_checksum, 456);
  EXPECT_EQ(references[2].stored_checksum, 654);
}

// Builds metadata with known bytes for three groups: kDirect, unflattened and
// flattened. The runs contain a zero gap, a large gap, a compression that
// differs from the run default, and a flattened gap from the end of the
// previous value. The terms are defined in blob_reference_metadata.h.
class BlobReferenceLayoutTest : public testing::Test {
 protected:
  static constexpr uint64_t kKeySize = 8;
  // Record header and user key of every record in these tests.
  static constexpr uint64_t kExtra = BlobLogRecord::kHeaderSize + kKeySize;
  static constexpr uint64_t kLargeOffset = uint64_t{1} << 40;

  static void AddReferences(BlobReferenceMetadataCollector* collector) {
    std::string encoded;
    for (uint64_t offset : {uint64_t{140}, uint64_t{190}, kLargeOffset}) {
      BlobIndex::EncodeBlob(&encoded, 41, offset, 10, kNoCompression);
      ASSERT_OK(collector->Add(DecodeBlobIndexForTest(encoded), kKeySize));
    }
    BlobIndex::EncodeUnflattenedIndirectBlob(&encoded,
                                             {.origin_file_number = 42,
                                              .original_value_offset = 140,
                                              .stored_value_size = 20,
                                              .compression = kZSTD,
                                              .stable_checksum = 1,
                                              .stored_checksum = 2});
    ASSERT_OK(collector->Add(DecodeBlobIndexForTest(encoded), kKeySize));
    BlobIndex::EncodeUnflattenedIndirectBlob(&encoded,
                                             {.origin_file_number = 42,
                                              .original_value_offset = 200,
                                              .stored_value_size = 30,
                                              .compression = kLZ4Compression,
                                              .stable_checksum = 3,
                                              .stored_checksum = 4});
    ASSERT_OK(collector->Add(DecodeBlobIndexForTest(encoded), kKeySize));
    for (const auto& [offset, size, crc] :
         {std::make_tuple(uint64_t{500}, uint64_t{7}, 5U),
          std::make_tuple(uint64_t{600}, uint64_t{8}, 6U)}) {
      BlobIndex::EncodeIndirectBlob(
          &encoded, 99,
          {.origin_file_number = 42,
           .original_value_offset = offset,
           .stored_value_size = size,
           .compression = kNoCompression,
           .stable_checksum = crc,
           .stored_checksum =
               BlobIndex::ExtendChecksumWithCompression(crc, kNoCompression)});
      ASSERT_OK(collector->Add(DecodeBlobIndexForTest(encoded), kKeySize));
    }
  }

  // A run header has no group key. It holds the count, the first offset and the
  // default compression. Each entry stores (stored size << 1) | differs. The
  // gap of an unflattened or kDirect entry excludes the record header and the
  // user key of that entry.
  static std::string DirectRun(uint64_t count = 3) {
    BytesForTest run;
    run.Varint(count).Varint(140).Byte(kNoCompression);
    run.Varint(10 << 1).Varint(kKeySize);
    run.Varint(0).Varint(10 << 1).Varint(kKeySize);
    if (count == 3) {
      run.Varint(kLargeOffset - (190 + 10 + kExtra))
          .Varint(10 << 1)
          .Varint(kKeySize);
    }
    return run.bytes;
  }
  static std::string UnflattenedRun() {
    return BytesForTest()
        .Varint(2)
        .Varint(140)
        .Byte(kZSTD)
        .Varint(20 << 1)
        .Varint(kKeySize)
        .Fixed32(1)
        .Fixed32(2)
        .Varint(0)
        .Varint((30 << 1) | 1)
        .Byte(kLZ4Compression)
        .Varint(kKeySize)
        .Fixed32(3)
        .Fixed32(4)
        .bytes;
  }
  static std::string FlattenedRun() {
    return BytesForTest()
        .Varint(2)
        .Varint(500)
        .Byte(kNoCompression)
        .Varint(7 << 1)
        .Fixed32(5)
        .Varint(600 - (500 + 7))
        .Varint(8 << 1)
        .Fixed32(6)
        .bytes;
  }

  // The group table entries of the three groups. Each position is a (start
  // block delta, run offset) pair.
  static std::string GroupTable(std::pair<uint64_t, uint64_t> direct,
                                std::pair<uint64_t, uint64_t> unflattened,
                                std::pair<uint64_t, uint64_t> flattened,
                                uint64_t direct_count = 3,
                                uint64_t flattened_generation = 99) {
    return BytesForTest()
        .Varint(3)
        .Byte(0)
        .Varint(41)
        .Varint(direct.first)
        .Varint(direct.second)
        .Varint(direct_count)
        .Varint(30)
        .Varint(3 * kKeySize)
        .Byte(1)
        .Varint(42)
        .Varint(unflattened.first)
        .Varint(unflattened.second)
        .Varint(2)
        .Varint(50)
        .Varint(2 * kKeySize)
        .Byte(2)
        .Varint(flattened_generation)
        .Varint(42)
        .Varint(flattened.first)
        .Varint(flattened.second)
        .Varint(2)
        .Varint(15)
        .bytes;
  }

  // A block table for blocks of these sizes. The first block starts at offset
  // 1000, and each block follows the trailer of the previous one.
  static std::string BlockTable(const std::vector<std::string>& blocks) {
    BytesForTest table;
    table.Varint(blocks.size()).Varint(1000);
    for (const std::string& block : blocks) {
      table.Varint(block.size());
    }
    return table.bytes;
  }

  // The index for one block that holds the three runs.
  static std::string IndexFor(const std::string& block, size_t direct_size,
                              size_t unflattened_size,
                              uint64_t direct_count = 3) {
    return BlockTable({block}) + GroupTable({0, 0}, {0, direct_size},
                                            {0, direct_size + unflattened_size},
                                            direct_count);
  }

  // Metadata with these blocks and this index. The index directly follows the
  // trailer of the last block.
  static MetadataForTest MetadataFor(std::vector<std::string> blocks,
                                     std::string index,
                                     uint64_t reference_count = 7,
                                     uint64_t stored_value_bytes = 95) {
    MetadataForTest metadata;
    uint64_t next_offset = 1000;
    for (const std::string& block : blocks) {
      next_offset += block.size() + 5;
    }
    metadata.blocks = std::move(blocks);
    metadata.index = std::move(index);
    metadata.descriptor.reference_count = reference_count;
    metadata.descriptor.stored_value_bytes = stored_value_bytes;
    metadata.descriptor.index_handle =
        BlockHandle(next_offset, metadata.index.size());
    return metadata;
  }

  static MetadataForTest Good() {
    const std::string block = DirectRun() + UnflattenedRun() + FlattenedRun();
    return MetadataFor(
        {block}, IndexFor(block, DirectRun().size(), UnflattenedRun().size()));
  }

  static bool DecodeFails(const MetadataForTest& metadata) {
    BlobReferenceIndex index;
    std::vector<BlobReference> references;
    const Status status = DecodeAllForTest(metadata, &index, &references);
    EXPECT_TRUE(status.ok() || status.IsCorruption()) << status.ToString();
    return status.IsCorruption();
  }
};

TEST_F(BlobReferenceLayoutTest, RejectsCountThatDoesNotFitItsBlocks) {
  // The decoder has no fixed limit on the reference count, but the descriptor's
  // count must fit the data blocks. A block holds at most 64 KB of entries, and
  // an entry takes at least 2 bytes, so one block cannot hold 64 KB / 2 + 1
  // references.
  MetadataForTest metadata = Good();
  ASSERT_EQ(metadata.blocks.size(), 1U);
  BlobReferenceIndex index;
  metadata.descriptor.reference_count = 64 * 1024 / 2 + 1;
  const Status status =
      DecodeBlobReferenceIndex(metadata.index, metadata.descriptor, &index);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("does not fit"), std::string::npos)
      << status.ToString();
}

TEST_F(BlobReferenceLayoutTest, RejectsFlattenedGenerationBelowOrigin) {
  // A flattened group needs generation > origin, as kIndirectFlattenedBlob
  // does, so generation 40 with origin 42 is malformed.
  const std::string block = DirectRun() + UnflattenedRun() + FlattenedRun();
  const std::string index =
      BlockTable({block}) +
      GroupTable({0, 0}, {0, DirectRun().size()},
                 {0, DirectRun().size() + UnflattenedRun().size()},
                 /*direct_count=*/3, /*flattened_generation=*/40);
  EXPECT_TRUE(DecodeFails(MetadataFor({block}, index)));
  EXPECT_FALSE(DecodeFails(Good()));
}

TEST_F(BlobReferenceLayoutTest, EncodesKnownBytes) {
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  AddReferences(&collector);
  MetadataForTest metadata;
  std::shared_ptr<const BlobReferenceSstSummary> summary;
  ASSERT_OK(FinishForTest(&collector, &metadata, &summary));
  const MetadataForTest expected = Good();
  ASSERT_EQ(metadata.blocks, expected.blocks);
  EXPECT_EQ(metadata.index, expected.index);
  EXPECT_EQ(metadata.descriptor.reference_count, 7);
  EXPECT_EQ(metadata.descriptor.stored_value_bytes, 95);

  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(references.size(), 7U);
  EXPECT_EQ(ReferenceFieldsForTest(references[2]),
            std::make_tuple(BlobReferenceEncodingClass::kDirect, uint64_t{41},
                            uint64_t{41}, kLargeOffset, uint64_t{10},
                            10 + kExtra, kNoCompression, 0U, 0U));
  EXPECT_EQ(
      ReferenceFieldsForTest(references[4]),
      std::make_tuple(BlobReferenceEncodingClass::kIndirectUnflattened,
                      uint64_t{42}, uint64_t{42}, uint64_t{200}, uint64_t{30},
                      30 + kExtra, kLZ4Compression, 3U, 4U));
  EXPECT_EQ(ReferenceFieldsForTest(references[6]),
            std::make_tuple(
                BlobReferenceEncodingClass::kIndirectFlattened, uint64_t{99},
                uint64_t{42}, uint64_t{600}, uint64_t{8},
                8 + kSimpleGen2BlobTrailerSize, kNoCompression, 6U,
                BlobIndex::ExtendChecksumWithCompression(6, kNoCompression)));

  // The groups of the build-time summary equal the index groups without the
  // kDirect group.
  ASSERT_NE(summary, nullptr);
  const auto group_fields = [](const std::vector<BlobReferenceGroup>& groups) {
    std::vector<std::tuple<BlobReferenceEncodingClass, uint64_t, uint64_t,
                           uint64_t, uint64_t, uint64_t>>
        fields;
    for (const BlobReferenceGroup& group : groups) {
      fields.emplace_back(group.encoding_class, group.generation_file_number,
                          group.origin_file_number, group.reference_count,
                          group.stored_value_bytes, group.record_bytes);
    }
    return fields;
  };
  EXPECT_EQ(group_fields(IndirectGroupsForTest(index)),
            group_fields(summary->groups));
  EXPECT_EQ(summary->groups.size(), 2U);
}

TEST_F(BlobReferenceLayoutTest, RejectsCorruptMetadata) {
  ASSERT_FALSE(DecodeFails(Good()));
  const std::string direct = DirectRun();
  const std::string unflattened = UnflattenedRun();
  const std::string flattened = FlattenedRun();
  const std::string block = direct + unflattened + flattened;
  const auto with_runs = [&](const std::string& d, const std::string& u,
                             const std::string& f) {
    return MetadataFor({d + u + f}, IndexFor(d + u + f, d.size(), u.size()));
  };
  // Replaces `old_bytes` bytes at `pos` of `run`.
  const auto patch = [](std::string run, size_t pos, size_t old_bytes,
                        const std::string& bytes) {
    return run.replace(pos, old_bytes, bytes);
  };
  const size_t run_header_size = 4;  // count, first offset (2), compression
  {
    // Non-canonical varints: the run count, and the first entry's size.
    EXPECT_TRUE(
        DecodeFails(with_runs(patch(direct, 0, 1, std::string("\x83\x00", 2)),
                              unflattened, flattened)));
    EXPECT_TRUE(DecodeFails(
        with_runs(patch(direct, run_header_size, 1, std::string("\x94\x00", 2)),
                  unflattened, flattened)));
  }
  {
    // A compression flagged as different that equals the run default.
    const std::string same_compression =
        BytesForTest().Varint((20 << 1) | 1).Byte(kZSTD).bytes;
    EXPECT_TRUE(DecodeFails(with_runs(
        direct, patch(unflattened, run_header_size, 1, same_compression),
        flattened)));
  }
  {
    // A wrong run offset is caught, although runs hold no group key. Either the
    // bytes do not decode as the group's runs, or the runs do not end where the
    // next group starts.
    for (const auto& [unflattened_offset, flattened_offset] :
         {std::make_pair(direct.size() + 1, direct.size() + unflattened.size()),
          std::make_pair(direct.size(), direct.size() + unflattened.size() - 1),
          std::make_pair(direct.size(),
                         direct.size() + unflattened.size() + 1)}) {
      SCOPED_TRACE(unflattened_offset);
      SCOPED_TRACE(flattened_offset);
      EXPECT_TRUE(DecodeFails(MetadataFor(
          {block},
          BlockTable({block}) + GroupTable({0, 0}, {0, unflattened_offset},
                                           {0, flattened_offset}))));
    }
  }
  {
    // A repeated offset in a flattened group: stored size 0 and gap 0.
    const std::string repeat = BytesForTest()
                                   .Varint(2)
                                   .Varint(500)
                                   .Byte(kNoCompression)
                                   .Varint(0)
                                   .Fixed32(5)
                                   .Varint(0)
                                   .Varint(0)
                                   .Fixed32(6)
                                   .bytes;
    const std::string index = BlockTable({repeat}) + BytesForTest()
                                                         .Varint(1)
                                                         .Byte(2)
                                                         .Varint(99)
                                                         .Varint(42)
                                                         .Varint(0)
                                                         .Varint(0)
                                                         .Varint(2)
                                                         .Varint(0)
                                                         .bytes;
    EXPECT_TRUE(DecodeFails(MetadataFor({repeat}, index, 2, 0)));
  }
  {
    // Index class rules: an unknown class, and a flattened group whose origin
    // equals its generation.
    std::string unknown_class = Good().index;
    const size_t first_class = BlockTable({block}).size() + 1;
    ASSERT_EQ(unknown_class[first_class], 0);
    unknown_class[first_class] = 3;
    EXPECT_TRUE(DecodeFails(MetadataFor({block}, unknown_class)));
    std::string same_origin = Good().index;
    const size_t origin_pos = same_origin.size() - 5;
    ASSERT_EQ(same_origin[origin_pos], 42);
    same_origin[origin_pos] = 99;
    EXPECT_TRUE(DecodeFails(MetadataFor({block}, same_origin)));
  }
  {
    // Run counts that do not match the index totals.
    EXPECT_TRUE(DecodeFails(MetadataFor(
        {block}, IndexFor(block, direct.size(), unflattened.size(), 4), 8,
        95)));
    EXPECT_TRUE(DecodeFails(MetadataFor(
        {block}, IndexFor(block, direct.size(), unflattened.size(), 2), 6,
        95)));
  }
  {
    // A run offset outside the block, read through the group reader.
    const MetadataForTest metadata = MetadataFor(
        {block},
        BlockTable({block}) +
            GroupTable({0, 0}, {0, block.size() + 10}, {0, block.size() + 20}));
    BlobReferenceIndex index;
    ASSERT_OK(
        DecodeBlobReferenceIndex(metadata.index, metadata.descriptor, &index));
    std::vector<BlobReference> references;
    EXPECT_TRUE(
        ReadGroupForTest(metadata, index, 1, &references).IsCorruption());
    EXPECT_TRUE(DecodeFails(metadata));
  }
  {
    // A group that continues in the next block, which starts with another
    // group's run instead.
    const std::string two_of_three = DirectRun(2);
    const std::string block1 = unflattened + flattened;
    const MetadataForTest metadata =
        MetadataFor({two_of_three, block1},
                    BlockTable({two_of_three, block1}) +
                        GroupTable({0, 0}, {1, 0}, {0, unflattened.size()}));
    EXPECT_TRUE(DecodeFails(metadata));
    BlobReferenceIndex decoded_index;
    ASSERT_OK(DecodeBlobReferenceIndex(metadata.index, metadata.descriptor,
                                       &decoded_index));
    std::vector<BlobReference> references;
    EXPECT_TRUE(ReadGroupForTest(metadata, decoded_index, 0, &references)
                    .IsCorruption());
  }
  {
    // Index groups out of order, trailing index bytes, totals that differ from
    // the descriptor, a block that overlaps the index, and an index that does
    // not directly follow the last block.
    const MetadataForTest good = Good();
    const std::string swapped = BlockTable({block}) + BytesForTest()
                                                          .Varint(2)
                                                          .Byte(1)
                                                          .Varint(42)
                                                          .Varint(0)
                                                          .Varint(0)
                                                          .Varint(2)
                                                          .Varint(50)
                                                          .Varint(2 * kKeySize)
                                                          .Byte(0)
                                                          .Varint(41)
                                                          .Varint(0)
                                                          .Varint(10)
                                                          .Varint(3)
                                                          .Varint(30)
                                                          .Varint(3 * kKeySize)
                                                          .bytes;
    EXPECT_TRUE(DecodeFails(MetadataFor(good.blocks, swapped, 5, 80)));
    EXPECT_TRUE(
        DecodeFails(MetadataFor(good.blocks, good.index + std::string(1, 0))));
    EXPECT_TRUE(DecodeFails(MetadataFor(good.blocks, good.index, 8, 95)));
    EXPECT_TRUE(DecodeFails(MetadataFor(good.blocks, good.index, 7, 96)));
    for (int64_t shift : {-1, 1}) {
      SCOPED_TRACE(shift);
      MetadataForTest metadata = good;
      metadata.descriptor.index_handle = BlockHandle(
          good.descriptor.index_handle.offset() + shift, good.index.size());
      EXPECT_TRUE(DecodeFails(metadata));
    }
  }
  {
    // Truncated and bit-flipped descriptors.
    const MetadataForTest good = Good();
    BlobReferenceMetadataDescriptor descriptor = good.descriptor;
    descriptor.column_family_id = kTestColumnFamilyId;
    descriptor.sst_file_number = kTestSstFileNumber;
    std::string encoded;
    descriptor.EncodeTo(&encoded);
    BlobReferenceMetadataDescriptor decoded;
    ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(encoded, &decoded));
    // The decoder rejects a descriptor without an SST identity
    // (HasSstIdentity()): an unknown column family or SST file number 0.
    for (int field = 0; field < 2; ++field) {
      SCOPED_TRACE(field);
      BlobReferenceMetadataDescriptor incomplete = descriptor;
      if (field == 0) {
        incomplete.column_family_id =
            TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
      } else {
        incomplete.sst_file_number = 0;
      }
      std::string incomplete_encoded;
      incomplete.EncodeTo(&incomplete_encoded);
      EXPECT_TRUE(BlobReferenceMetadataDescriptor::DecodeFrom(
                      incomplete_encoded, &decoded)
                      .IsCorruption());
    }
    EXPECT_TRUE(BlobReferenceMetadataDescriptor::DecodeFrom(
                    Slice(encoded.data(), encoded.size() - 1), &decoded)
                    .IsCorruption());
    encoded[10] ^= 1;
    EXPECT_TRUE(BlobReferenceMetadataDescriptor::DecodeFrom(encoded, &decoded)
                    .IsCorruption());
  }
}

TEST(BlobReferenceMetadataTest, CollectorRequiresSstIdentity) {
  // The decoder rejects a descriptor without the SST identity, so the
  // collector must not build one. Each case leaves one identity field at its
  // default (unknown) value.
  for (int field = 0; field < 2; ++field) {
    SCOPED_TRACE(field);
    BlobReferenceSstIdentity identity;
    if (field == 0) {
      identity.sst_file_number = kTestSstFileNumber;
    } else {
      identity.column_family_id = kTestColumnFamilyId;
    }
    BlobReferenceMetadataCollector collector(nullptr, identity);
    EXPECT_TRUE(
        collector.Add(UnflattenedForTest(42, 100, 40), 8).IsInvalidArgument());
    MetadataForTest metadata;
    EXPECT_TRUE(FinishForTest(&collector, &metadata).IsInvalidArgument());
  }
}

TEST(BlobReferenceMetadataTest, OverlappingReferencesFailTheBuild) {
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  // The second value starts inside the first record. Add() finds an in-order
  // overlap. The build fails, and Finish() returns the same error.
  ASSERT_OK(
      collector.Add(UnflattenedForTest(42, 1000, 100), /*user_key_size=*/0));
  EXPECT_TRUE(
      collector.Add(UnflattenedForTest(42, 1050, 100), /*user_key_size=*/0)
          .IsCorruption());
  MetadataForTest metadata;
  const Status status = FinishForTest(&collector, &metadata);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();

  // An out-of-order overlap is found when Finish() sorts the group.
  BlobReferenceMetadataCollector unsorted(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  ASSERT_OK(
      unsorted.Add(UnflattenedForTest(42, 1050, 100), /*user_key_size=*/0));
  ASSERT_OK(
      unsorted.Add(UnflattenedForTest(42, 1000, 100), /*user_key_size=*/0));
  const Status unsorted_status = FinishForTest(&unsorted, &metadata);
  EXPECT_TRUE(unsorted_status.IsCorruption()) << unsorted_status.ToString();
}

namespace {
// An unflattened reference with the given compression and consistent CRCs.
BlobIndex UnflattenedWithCompressionForTest(uint64_t origin, uint64_t offset,
                                            uint64_t stored_size,
                                            CompressionType compression) {
  const uint32_t stable = static_cast<uint32_t>(offset * 7 + origin);
  const uint32_t stored =
      compression == kNoCompression
          ? BlobIndex::ExtendChecksumWithCompression(stable, kNoCompression)
          : stable + 1;
  std::string encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(&encoded,
                                           {.origin_file_number = origin,
                                            .original_value_offset = offset,
                                            .stored_value_size = stored_size,
                                            .compression = compression,
                                            .stable_checksum = stable,
                                            .stored_checksum = stored});
  return DecodeBlobIndexForTest(encoded);
}

// Returns the references that a collector should produce from these
// BlobIndexes, in sort order (class, generation, origin, original offset).
std::vector<BlobReference> ExpectedReferencesForTest(
    const std::vector<BlobIndex>& added, uint64_t user_key_size) {
  std::vector<BlobReference> expected;
  for (const BlobIndex& blob_index : added) {
    BlobReference reference;
    bool included = false;
    EXPECT_OK(BlobReference::FromBlobIndex(blob_index, &reference, &included));
    if (reference.encoding_class !=
        BlobReferenceEncodingClass::kIndirectFlattened) {
      reference.record_bytes +=
          BlobLogRecord::CalculateAdjustmentForRecordHeader(user_key_size);
    }
    expected.push_back(reference);
  }
  std::sort(
      expected.begin(), expected.end(),
      [](const BlobReference& lhs, const BlobReference& rhs) {
        return std::tie(lhs.encoding_class, lhs.generation_file_number,
                        lhs.origin_file_number, lhs.original_value_offset) <
               std::tie(rhs.encoding_class, rhs.generation_file_number,
                        rhs.origin_file_number, rhs.original_value_offset);
      });
  return expected;
}

void ExpectSameReferencesForTest(const std::vector<BlobReference>& actual,
                                 const std::vector<BlobReference>& expected) {
  ASSERT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < actual.size(); ++i) {
    ASSERT_EQ(ReferenceFieldsForTest(actual[i]),
              ReferenceFieldsForTest(expected[i]))
        << i;
  }
}
}  // namespace

TEST(BlobReferenceMetadataTest, InterleavedGroupsDecodeInKeyOrder) {
  // References of many groups arrive interleaved, each group in rising offset
  // order, with mixed compression inside a group.
  const uint32_t seed = 20261107;
  SCOPED_TRACE("seed " + std::to_string(seed));
  Random rnd(seed);
  constexpr uint64_t kGroups = 60;
  std::vector<uint64_t> next_offset(kGroups, 10);
  std::vector<BlobIndex> added;
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (int i = 0; i < 20000; ++i) {
    const uint64_t group = rnd.Uniform(kGroups);
    const uint64_t size = 10 + rnd.Uniform(200);
    const CompressionType compression = rnd.OneIn(4) ? kNoCompression : kZSTD;
    added.push_back(UnflattenedWithCompressionForTest(
        100 + group, next_offset[group], size, compression));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/16));
    next_offset[group] +=
        size + BlobLogRecord::kHeaderSize + 16 + rnd.Uniform(3) * 50;
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  const std::vector<BlobReference> expected =
      ExpectedReferencesForTest(added, /*user_key_size=*/16);
  ExpectSameReferencesForTest(references, expected);
  ASSERT_EQ(index.groups.size(), kGroups);
  ASSERT_GT(index.blocks.size(), 1U);
  // Index totals equal the sums over each group's references.
  size_t position = 0;
  for (const BlobReferenceIndexEntry& entry : index.groups) {
    uint64_t count = 0, stored = 0, record = 0;
    while (position < expected.size() &&
           expected[position].origin_file_number ==
               entry.group.origin_file_number) {
      ++count;
      stored += expected[position].stored_value_size;
      record += expected[position].record_bytes;
      ++position;
    }
    EXPECT_EQ(entry.group.reference_count, count);
    EXPECT_EQ(entry.group.stored_value_bytes, stored);
    EXPECT_EQ(entry.group.record_bytes, record);
  }
}

// The collector does not keep the stored CRC of a kNoCompression indirect
// reference, because the metadata does not encode it. The reader derives it
// from the stable CRC. This test checks that every decoded reference still has
// the correct stored CRC:
//   - kNoCompression: ExtendChecksumWithCompression(stable CRC, kNoCompression)
//   - compressed (kLZ4Compression here): the stored CRC of the BlobIndex
// It uses unflattened and flattened references, and one group whose offsets
// arrive out of order, so that the unsorted fallback path is also checked.
TEST(BlobReferenceMetadataTest, DecodedStoredChecksumMatchesBlobIndex) {
  struct Added {
    BlobIndex blob_index;
    uint32_t expected_stored_checksum;
  };
  std::vector<Added> added;
  // Adds one indirect reference. The file numbers, offsets and CRC values are
  // example values. The stored CRC of a compressed reference differs from its
  // stable CRC, so a mix-up of the two fields fails the test.
  auto add = [&](uint64_t generation, uint64_t origin, uint64_t offset,
                 CompressionType compression) {
    const uint32_t stable = static_cast<uint32_t>(origin * 1000 + offset);
    const uint32_t stored = stable + 7;
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(&encoded, generation,
                                  {.origin_file_number = origin,
                                   .original_value_offset = offset,
                                   .stored_value_size = 40,
                                   .compression = compression,
                                   .stable_checksum = stable,
                                   .stored_checksum = stored});
    const BlobIndex blob_index = DecodeBlobIndexForTest(encoded);
    const uint32_t expected =
        compression == kNoCompression
            ? BlobIndex::ExtendChecksumWithCompression(stable, kNoCompression)
            : stored;
    // The BlobIndex itself gives the same stored CRC.
    EXPECT_EQ(blob_index.stored_checksum(), expected);
    added.push_back({blob_index, expected});
  };
  // Unflattened group of origin 50, offsets in order.
  add(50, 50, 100, kNoCompression);
  add(50, 50, 300, kLZ4Compression);
  add(50, 50, 500, kNoCompression);
  // Unflattened group of origin 51, offsets out of order (fallback path).
  add(51, 51, 900, kNoCompression);
  add(51, 51, 100, kLZ4Compression);
  add(51, 51, 500, kNoCompression);
  // Flattened group: generation 70, origin 60.
  add(70, 60, 100, kNoCompression);
  add(70, 60, 300, kLZ4Compression);

  UnsortedFallbackCounterForTest fallbacks;
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (const Added& entry : added) {
    ASSERT_OK(collector.Add(entry.blob_index, /*user_key_size=*/8));
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  // Only the group of origin 51 took the fallback.
  EXPECT_EQ(fallbacks.count(), 1U);
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(references.size(), added.size());
  for (const BlobReference& reference : references) {
    SCOPED_TRACE("origin=" + std::to_string(reference.origin_file_number) +
                 " offset=" + std::to_string(reference.original_value_offset));
    const Added* match = nullptr;
    for (const Added& entry : added) {
      if (entry.blob_index.origin_file_number() ==
              reference.origin_file_number &&
          entry.blob_index.generation_file_number() ==
              reference.generation_file_number &&
          entry.blob_index.original_value_offset() ==
              reference.original_value_offset) {
        match = &entry;
      }
    }
    ASSERT_NE(match, nullptr);
    EXPECT_EQ(reference.compression, match->blob_index.compression());
    EXPECT_EQ(reference.stable_checksum, match->blob_index.stable_checksum());
    EXPECT_EQ(reference.stored_checksum, match->expected_stored_checksum);
  }
  // The full reference also equals the one BlobReference::FromBlobIndex()
  // gives, which derives the stored CRC itself.
  std::vector<BlobIndex> blob_indexes;
  for (const Added& entry : added) {
    blob_indexes.push_back(entry.blob_index);
  }
  ExpectSameReferencesForTest(
      references, ExpectedReferencesForTest(blob_indexes, /*user_key_size=*/8));
}

namespace {
// Encodes one run of in-order references of one group, from the references
// themselves, with the layout that BlobReferenceLayoutTest describes: a run
// header (count, first offset, default compression), then one entry per
// reference. The stored CRC is in an entry only for a compressed reference.
std::string EncodeRunFromReferencesForTest(
    const std::vector<BlobReference>& references, uint64_t user_key_size) {
  const CompressionType default_compression = references[0].compression;
  const bool stores_key_size = references[0].encoding_class !=
                               BlobReferenceEncodingClass::kIndirectFlattened;
  const uint64_t record_overhead =
      stores_key_size ? BlobLogRecord::kHeaderSize + user_key_size : 0;
  BytesForTest bytes;
  bytes.Varint(references.size())
      .Varint(references[0].original_value_offset)
      .Byte(static_cast<uint8_t>(default_compression));
  for (size_t i = 0; i < references.size(); ++i) {
    const BlobReference& reference = references[i];
    if (i > 0) {
      const BlobReference& previous = references[i - 1];
      bytes.Varint(reference.original_value_offset -
                   (previous.original_value_offset +
                    previous.stored_value_size + record_overhead));
    }
    const bool differs = reference.compression != default_compression;
    bytes.Varint((reference.stored_value_size << 1) | (differs ? 1 : 0));
    if (differs) {
      bytes.Byte(static_cast<uint8_t>(reference.compression));
    }
    if (stores_key_size) {
      bytes.Varint(user_key_size);
    }
    bytes.Fixed32(reference.stable_checksum);
    if (reference.compression != kNoCompression) {
      bytes.Fixed32(reference.stored_checksum);
    }
  }
  return bytes.bytes;
}
}  // namespace

// The collector skips the stored CRC of a kNoCompression indirect reference.
// This test checks that the skip does not change the encoded metadata: the
// collector's block equals the bytes encoded from the canonical references
// that BlobReference::FromBlobIndex() gives, which carry the derived stored
// CRC. It uses one unflattened group and one flattened group, each with
// kNoCompression and kLZ4Compression references.
TEST(BlobReferenceMetadataTest, StoredChecksumSkipKeepsMetadataBytes) {
  constexpr uint64_t kKeySize = 8;
  constexpr uint64_t kStoredSize = 40;
  std::vector<BlobIndex> unflattened;
  std::vector<BlobIndex> flattened;
  // The file numbers, offsets and CRC values are example values. Offsets
  // leave a gap after each record, so no records overlap.
  const CompressionType compressions[] = {kNoCompression, kLZ4Compression,
                                          kNoCompression};
  for (size_t i = 0; i < 3; ++i) {
    const uint64_t offset = 100 + i * 200;
    const uint32_t stable = static_cast<uint32_t>(1000 + i);
    std::string encoded;
    BlobIndex::EncodeUnflattenedIndirectBlob(&encoded,
                                             {.origin_file_number = 50,
                                              .original_value_offset = offset,
                                              .stored_value_size = kStoredSize,
                                              .compression = compressions[i],
                                              .stable_checksum = stable,
                                              .stored_checksum = stable + 7});
    unflattened.push_back(DecodeBlobIndexForTest(encoded));
    BlobIndex::EncodeIndirectBlob(&encoded, /*generation=*/70,
                                  {.origin_file_number = 60,
                                   .original_value_offset = offset,
                                   .stored_value_size = kStoredSize,
                                   .compression = compressions[i],
                                   .stable_checksum = stable + 100,
                                   .stored_checksum = stable + 107});
    flattened.push_back(DecodeBlobIndexForTest(encoded));
  }

  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(collector.Add(unflattened[i], kKeySize));
    ASSERT_OK(collector.Add(flattened[i], kKeySize));
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  ASSERT_EQ(metadata.blocks.size(), 1U);

  // The canonical references carry the derived stored CRC for kNoCompression.
  const std::vector<BlobReference> unflattened_references =
      ExpectedReferencesForTest(unflattened, kKeySize);
  const std::vector<BlobReference> flattened_references =
      ExpectedReferencesForTest(flattened, kKeySize);
  for (const auto* references :
       {&unflattened_references, &flattened_references}) {
    for (const BlobReference& reference : *references) {
      if (reference.compression == kNoCompression) {
        EXPECT_EQ(reference.stored_checksum,
                  BlobIndex::ExtendChecksumWithCompression(
                      reference.stable_checksum, kNoCompression));
      }
    }
  }
  // The unflattened group sorts before the flattened group, so its run comes
  // first in the block.
  EXPECT_EQ(metadata.blocks[0],
            EncodeRunFromReferencesForTest(unflattened_references, kKeySize) +
                EncodeRunFromReferencesForTest(flattened_references, kKeySize));
}

TEST(BlobReferenceMetadataTest, EstimateBoundsRealSizeClosely) {
  // 200 groups arrive interleaved, with 16-byte user keys and mixed value sizes
  // and compression. The estimate made before Finish() is at least the real
  // metadata size and within 10% of it.
  const uint32_t seed = 20261108;
  SCOPED_TRACE("seed " + std::to_string(seed));
  Random rnd(seed);
  constexpr uint64_t kGroups = 200;
  std::vector<uint64_t> next_offset(kGroups, 10);
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (int i = 0; i < 200000; ++i) {
    const uint64_t group = rnd.Uniform(kGroups);
    const uint64_t size = 512 + rnd.Uniform(4096);
    ASSERT_OK(collector.Add(UnflattenedWithCompressionForTest(
                                100 + group, next_offset[group], size,
                                rnd.OneIn(4) ? kNoCompression : kZSTD),
                            /*user_key_size=*/16));
    next_offset[group] += size + BlobLogRecord::kHeaderSize + 16;
  }
  const uint64_t estimate = collector.EstimatedEncodedSize();
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  uint64_t real = metadata.index.size();
  for (const std::string& block : metadata.blocks) {
    real += block.size() + BlockBasedTable::kBlockTrailerSize;
  }
  EXPECT_GE(estimate, real);
  EXPECT_LE(estimate, real + real / 10);
}

TEST(BlobReferenceMetadataTest, RunsOfOneGroupMustNotOverlapAcrossBlocks) {
  // One group spans two blocks. The second run stores its first offset as a
  // full value, not as a gap, so the decoder must check it against the first
  // run. A first offset whose record would overlap the last record of the first
  // run is Corruption.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  uint64_t offset = 10;
  for (int i = 0; i < 2000; ++i) {
    ASSERT_OK(
        collector.Add(UnflattenedForTest(42, offset, 40), /*user_key_size=*/0));
    offset += 40 + BlobLogRecord::kHeaderSize;
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  ASSERT_EQ(metadata.blocks.size(), 2U);
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  // The first reference of the second block and the one before it.
  Slice header = metadata.blocks[1];
  uint64_t count = 0;
  uint64_t first_offset = 0;
  ASSERT_TRUE(GetVarint64(&header, &count));
  ASSERT_TRUE(GetVarint64(&header, &first_offset));
  const size_t second = references.size() - count;
  ASSERT_EQ(references[second].original_value_offset, first_offset);
  const BlobReference& last = references[second - 1];
  const auto with_first_offset = [&](uint64_t new_first) {
    MetadataForTest changed = metadata;
    std::string block;
    PutVarint64(&block, count);
    PutVarint64(&block, new_first);
    block.append(header.data(), header.size());
    changed.blocks[1] = block;
    // Keep the block table consistent with the new block size.
    BlobReferenceIndex changed_index = index;
    changed_index.blocks[1] =
        BlockHandle(index.blocks[1].offset(), changed.blocks[1].size());
    return ForEachBlobReference(
        changed_index, BlockReaderForTest(changed),
        [](const BlobReference&) { return Status::OK(); });
  };
  ASSERT_OK(with_first_offset(first_offset));
  for (const uint64_t bad :
       {last.original_value_offset,
        last.original_value_offset + last.stored_value_size,
        first_offset - 1}) {
    SCOPED_TRACE(bad);
    const Status status = with_first_offset(bad);
    EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  }
}

TEST(BlobReferenceMetadataTest, HugeIndexCountFailsBeforeAllocation) {
  // A tiny index that claims many blocks or many groups is rejected by the
  // input size check alone, before any reserve().
  const auto decode = [](const std::string& input) {
    BlobReferenceMetadataDescriptor descriptor;
    descriptor.index_handle = BlockHandle(1000, input.size());
    descriptor.reference_count = 1;
    BlobReferenceIndex index;
    return DecodeBlobReferenceIndex(input, descriptor, &index);
  };
  std::string blocks;
  PutVarint64(&blocks, 1 << 20);
  PutVarint64(&blocks, 0);
  EXPECT_TRUE(decode(blocks).IsCorruption());
  std::string groups;
  PutVarint64(&groups, 1);
  PutVarint64(&groups, 0);
  PutVarint64(&groups, 995);
  PutVarint64(&groups, 1 << 20);
  EXPECT_TRUE(decode(groups).IsCorruption());
}

TEST(BlobReferenceMetadataTest, GroupSpanningChunksSplitsRunsAtBlockEnds) {
  // One group spans many chunks and several blocks. Runs end only at block
  // ends, and the gaps at chunk joins decode to the right offsets.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  std::vector<BlobIndex> added;
  uint64_t offset = 10;
  for (int i = 0; i < 10000; ++i) {
    added.push_back(UnflattenedForTest(42, offset, 40));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/0));
    // Every 7th record leaves a gap of unreferenced bytes.
    offset += 40 + BlobLogRecord::kHeaderSize + (i % 7 == 0 ? 123 : 0);
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ExpectSameReferencesForTest(
      references, ExpectedReferencesForTest(added, /*user_key_size=*/0));
  ASSERT_EQ(index.groups.size(), 1U);
  ASSERT_GT(index.blocks.size(), 3U);
  // Each block holds one run of the group. The first reference of a run is the
  // one right after the last reference of the previous block.
  BlobReferenceGroupDecoder decoder(index, /*group=*/0);
  size_t next = 0;
  for (size_t block = 0; block < metadata.blocks.size(); ++block) {
    ASSERT_FALSE(decoder.done());
    ASSERT_EQ(decoder.next_block(), block);
    const Slice contents = metadata.blocks[block];
    const size_t first = next;
    ASSERT_OK(decoder.DecodeRun(contents, [&](const BlobReference& reference) {
      EXPECT_EQ(reference.original_value_offset,
                references[next++].original_value_offset);
      return Status::OK();
    }));
    ASSERT_GT(next, first);
    // Each block except the last stays within the 16 KB target. A run takes
    // only whole 512-byte chunks, so a block is about one chunk short at most.
    // The test allows a block to be up to 1 KB short.
    if (block + 1 < metadata.blocks.size()) {
      EXPECT_LE(contents.size(), 16 * 1024U);
      EXPECT_GT(contents.size(), 15 * 1024U);
    }
  }
  ASSERT_EQ(next, references.size());
}

TEST(BlobReferenceMetadataTest, DecoderRejectsBlockOver64KBAfterDecompression) {
  // The index decoder checks only the size on disk. A compressed block that
  // decompresses to more than 64 KB passes that check, so the run decoder
  // rejects it. Here the contents of a valid block are padded past 64 KB, as
  // a corrupt compressed block could decompress.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  ASSERT_OK(collector.Add(UnflattenedForTest(42, /*offset=*/10, 40),
                          /*user_key_size=*/0));
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(metadata.blocks.size(), 1U);
  std::string padded = metadata.blocks[0];
  padded.resize(64 * 1024 + 1, '\0');
  BlobReferenceGroupDecoder decoder(index, /*group=*/0);
  const Status status = decoder.DecodeRun(
      padded, [](const BlobReference& /*reference*/) { return Status::OK(); });
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(status.ToString().find("64 KB"), std::string::npos);
}

TEST(BlobReferenceMetadataTest, SmallGroupsShareOneBlock) {
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (uint64_t origin = 100; origin < 200; ++origin) {
    ASSERT_OK(
        collector.Add(UnflattenedForTest(origin, 10, 40), /*user_key_size=*/0));
    ASSERT_OK(collector.Add(UnflattenedForTest(origin, 200, 40),
                            /*user_key_size=*/0));
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(index.blocks.size(), 1U);
  ASSERT_EQ(index.groups.size(), 100U);
  for (size_t i = 0; i < index.groups.size(); ++i) {
    EXPECT_EQ(index.groups[i].start_block, 0U);
    if (i > 0) {
      EXPECT_GT(index.groups[i].run_offset, index.groups[i - 1].run_offset);
    }
  }
}

TEST(BlobReferenceMetadataTest, UnsortedGroupTakesFallback) {
  // An offset lower than the group's last one sends the group to the unsorted
  // fallback. The group still decodes in offset order.
  UnsortedFallbackCounterForTest fallbacks;
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  std::vector<BlobIndex> added;
  for (uint64_t i = 0; i < 2000; ++i) {
    // Two interleaved offset sequences in one group.
    const uint64_t offset = (i % 2 == 0 ? 0 : 1000000) + (i / 2) * 200;
    added.push_back(UnflattenedForTest(42, offset, 40));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/0));
  }
  added.push_back(UnflattenedForTest(43, 10, 40));
  ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/0));
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  EXPECT_EQ(fallbacks.count(), 1U);
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ExpectSameReferencesForTest(
      references, ExpectedReferencesForTest(added, /*user_key_size=*/0));
}

TEST(BlobReferenceMetadataTest, FallbackKeepsItsScratchCharged) {
  // While Finish() encodes a fallback group again, the cache charge still
  // includes the group's sort keys. No charge update during that time drops
  // below them.
  auto reservation_manager = std::make_shared<RejectingReservationManager>(
      std::numeric_limits<size_t>::max());
  BlobReferenceMetadataCollector collector(
      reservation_manager,
      BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                               .sst_file_number = kTestSstFileNumber});
  constexpr uint64_t kCount = 20000;
  for (uint64_t i = 0; i < kCount; ++i) {
    // Two interleaved offset sequences in one group.
    const uint64_t offset = (i % 2 == 0 ? 0 : 100000000) + (i / 2) * 200;
    ASSERT_OK(
        collector.Add(UnflattenedForTest(42, offset, 40), /*user_key_size=*/0));
  }
  bool encoding = false;
  size_t lowest = std::numeric_limits<size_t>::max();
  size_t updates = 0;
  reservation_manager->on_update = [&](size_t charge) {
    if (encoding) {
      lowest = std::min(lowest, charge);
      ++updates;
    }
  };
  SyncPoint::GetInstance()->SetCallBack(
      "BlobReferenceMetadataCollector::SortUnsortedGroup",
      [&](void*) { encoding = true; });
  SyncPoint::GetInstance()->SetCallBack(
      "BlobReferenceMetadataCollector::SortUnsortedGroup:Encoded",
      [&](void*) { encoding = false; });
  SyncPoint::GetInstance()->EnableProcessing();
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  reservation_manager->on_update = nullptr;
  ASSERT_GT(updates, 1U);
  // The charge covers one 16-byte sort key per reference plus the chunks. It
  // stays below sizeof(BlobReference) (64 bytes) per reference, the cost of
  // decoding every reference into a full BlobReference.
  EXPECT_GE(lowest, kCount * 16);
  EXPECT_LT(lowest, kCount * sizeof(BlobReference));
}

TEST(BlobReferenceMetadataTest, LargeFallbackGroupStaysCompact) {
  // A large group has every other reference out of order. While the collector
  // collects and sorts it, the charge is 16 bytes of sort key per reference
  // plus the chunks. The chunks hold the entries (about 11 to 15 bytes each
  // here, held twice while the group is encoded again) and the chunk headers.
  // The peak is about 44 bytes per reference (estimate), and the test allows
  // 56. Decoding each reference into a 64-byte BlobReference, in both an
  // unsorted buffer and a sort buffer, would need more than 100.
  auto reservation_manager = std::make_shared<RejectingReservationManager>(
      std::numeric_limits<size_t>::max());
  size_t peak = 0;
  reservation_manager->on_update = [&](size_t charge) {
    peak = std::max(peak, charge);
  };
  BlobReferenceMetadataCollector collector(
      reservation_manager,
      BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                               .sst_file_number = kTestSstFileNumber});
  constexpr uint64_t kCount = 200000;
  std::vector<BlobIndex> added;
  added.reserve(kCount);
  for (uint64_t i = 0; i < kCount; ++i) {
    // Two interleaved offset sequences in one group.
    const uint64_t offset = (i % 2 == 0 ? 0 : 100000000) + (i / 2) * 200;
    added.push_back(UnflattenedForTest(42, offset, 40));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/0));
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  reservation_manager->on_update = nullptr;
  EXPECT_LE(peak, kCount * 56);
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ExpectSameReferencesForTest(
      references, ExpectedReferencesForTest(added, /*user_key_size=*/0));
}

TEST(BlobReferenceMetadataTest, FallbackKeepsDefaultAndStaysUnderEstimate) {
  // In-order ZSTD references arrive, then one kNoCompression reference at a
  // lower offset. The fallback keeps the group's default compression (ZSTD), so
  // the ZSTD entries do not grow and the estimate stays an upper bound.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  std::vector<BlobIndex> added;
  for (uint64_t i = 0; i < 2000; ++i) {
    added.push_back(
        UnflattenedWithCompressionForTest(42, 1000 + i * 100, 40, kZSTD));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/0));
  }
  added.push_back(
      UnflattenedWithCompressionForTest(42, 10, 40, kNoCompression));
  ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/0));
  const uint64_t estimate = collector.EstimatedEncodedSize();
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  uint64_t real = metadata.index.size();
  for (const std::string& block : metadata.blocks) {
    real += block.size() + BlockBasedTable::kBlockTrailerSize;
  }
  EXPECT_GE(estimate, real);
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ExpectSameReferencesForTest(
      references, ExpectedReferencesForTest(added, /*user_key_size=*/0));
  // The first run's default byte is still ZSTD.
  Slice header = metadata.blocks[0];
  uint64_t value = 0;
  ASSERT_TRUE(GetVarint64(&header, &value));
  ASSERT_TRUE(GetVarint64(&header, &value));
  ASSERT_FALSE(header.empty());
  EXPECT_EQ(static_cast<CompressionType>(header[0]), kZSTD);
}

TEST(BlobReferenceMetadataTest, FallbackFindsRepeatInAnotherChunk) {
  // A repeat of an offset from an early chunk arrives after many chunks. The
  // fallback finds it.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (uint64_t i = 0; i < 2000; ++i) {
    ASSERT_OK(collector.Add(UnflattenedForTest(42, 10 + i * 100, 40),
                            /*user_key_size=*/0));
  }
  ASSERT_OK(collector.Add(UnflattenedForTest(42, 10 + 5 * 100, 40),
                          /*user_key_size=*/0));
  MetadataForTest metadata;
  const Status status = FinishForTest(&collector, &metadata);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("Repeated blob reference"),
            std::string::npos);
}

TEST(BlobReferenceMetadataTest, ChunkJoinsWithChangingKeySizes) {
  // User key sizes change from entry to entry, also at the chunk joins. At a
  // join, Add() stores the gap at the start of the new chunk.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  std::vector<BlobReference> expected;
  uint64_t offset = 10;
  for (uint64_t i = 0; i < 3000; ++i) {
    const uint64_t key_size = (i * 7) % 40;
    const BlobIndex blob_index = UnflattenedForTest(42, offset, 40);
    ASSERT_OK(collector.Add(blob_index, key_size));
    expected.push_back(ExpectedReferencesForTest({blob_index}, key_size)[0]);
    // The next value starts after this value, the next record's header and the
    // next record's key. Every 11th record leaves 5 unreferenced bytes.
    offset += 40 + BlobLogRecord::kHeaderSize + ((i + 1) * 7) % 40 +
              (i % 11 == 0 ? 5 : 0);
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  ASSERT_GT(metadata.blocks.size(), 1U);
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ExpectSameReferencesForTest(references, expected);
}

TEST(BlobReferenceMetadataTest, DirectAndFlattenedGroupsSpanChunks) {
  // A kDirect group and a flattened group each fill many chunks and blocks.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  std::vector<BlobIndex> added;
  for (uint64_t i = 0; i < 3000; ++i) {
    added.push_back(LegacyForTest(10, 1000 + i * 200));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/8));
  }
  for (uint64_t i = 0; i < 3000; ++i) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, 99,
        {.origin_file_number = 42,
         .original_value_offset = 10 + i * 100,
         .stored_value_size = 40,
         .compression = kZSTD,
         .stable_checksum = static_cast<uint32_t>(i),
         .stored_checksum = static_cast<uint32_t>(i)});
    added.push_back(DecodeBlobIndexForTest(encoded));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/8));
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  ASSERT_GT(metadata.blocks.size(), 2U);
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(index.groups.size(), 2U);
  ExpectSameReferencesForTest(
      references, ExpectedReferencesForTest(added, /*user_key_size=*/8));
}

TEST(BlobReferenceMetadataTest, ContinuationRunKeepsGroupDefault) {
  // The group's first reference is ZSTD, and later references mix ZSTD and
  // kNoCompression. The runs that continue the group in later blocks still have
  // the group's default compression (ZSTD).
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  std::vector<BlobIndex> added;
  for (uint64_t i = 0; i < 3000; ++i) {
    added.push_back(UnflattenedWithCompressionForTest(
        42, 10 + i * 100, 40, i == 0 || i % 3 != 0 ? kZSTD : kNoCompression));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/0));
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  ASSERT_GT(metadata.blocks.size(), 1U);
  for (size_t block = 0; block < metadata.blocks.size(); ++block) {
    Slice header = metadata.blocks[block];
    uint64_t value = 0;
    ASSERT_TRUE(GetVarint64(&header, &value));
    ASSERT_TRUE(GetVarint64(&header, &value));
    ASSERT_FALSE(header.empty());
    EXPECT_EQ(static_cast<CompressionType>(header[0]), kZSTD) << block;
  }
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ExpectSameReferencesForTest(
      references, ExpectedReferencesForTest(added, /*user_key_size=*/0));
}

TEST(BlobReferenceMetadataTest, SlabChargeFailureKeepsCollecting) {
  // The charge for the first 64 KB slab fails, as it does when the block cache
  // is full. Add() still succeeds: the slab is allocated without its charge,
  // and Finish() writes the reference.
  auto reservation_manager =
      std::make_shared<RejectingReservationManager>(/*limit=*/32 * 1024);
  BlobReferenceMetadataCollector collector(
      reservation_manager,
      BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                               .sst_file_number = kTestSstFileNumber});
  ASSERT_OK(collector.Add(UnflattenedForTest(42, 10, 40), /*user_key_size=*/0));
  EXPECT_GE(collector.TEST_ReservedMemory(), 64 * 1024U);
  EXPECT_GT(collector.uncharged_charge_failures(), 0U);
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  EXPECT_EQ(references.size(), 1U);
}

TEST(BlobReferenceMetadataTest, RunDefaultIsGroupFirstCompression) {
  // Entries whose compression differs from the group's first entry carry the
  // compression byte. Within one block, the bytes equal a direct encoding of
  // the sorted references.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  const CompressionType compressions[] = {kNoCompression, kZSTD, kZSTD,
                                          kNoCompression, kLZ4Compression};
  std::vector<BlobIndex> added;
  uint64_t offset = 10;
  for (size_t i = 0; i < 5; ++i) {
    added.push_back(
        UnflattenedWithCompressionForTest(42, offset, 30, compressions[i]));
    ASSERT_OK(collector.Add(added.back(), /*user_key_size=*/8));
    offset += 30 + BlobLogRecord::kHeaderSize + 8 + (i == 2 ? 77 : 0);
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  const std::vector<BlobReference> expected =
      ExpectedReferencesForTest(added, /*user_key_size=*/8);
  ExpectSameReferencesForTest(references, expected);

  // The direct encoding of one run: header, then the entries.
  BytesForTest bytes;
  bytes.Varint(expected.size())
      .Varint(expected[0].original_value_offset)
      .Byte(static_cast<uint8_t>(compressions[0]));
  for (size_t i = 0; i < expected.size(); ++i) {
    const BlobReference& reference = expected[i];
    if (i > 0) {
      const BlobReference& previous = expected[i - 1];
      bytes.Varint(reference.original_value_offset -
                   (previous.original_value_offset +
                    previous.stored_value_size + BlobLogRecord::kHeaderSize +
                    8));
    }
    const bool differs = reference.compression != compressions[0];
    bytes.Varint((reference.stored_value_size << 1) | (differs ? 1 : 0));
    if (differs) {
      bytes.Byte(static_cast<uint8_t>(reference.compression));
    }
    bytes.Varint(8).Fixed32(reference.stable_checksum);
    if (reference.compression != kNoCompression) {
      bytes.Fixed32(reference.stored_checksum);
    }
  }
  ASSERT_EQ(metadata.blocks.size(), 1U);
  EXPECT_EQ(metadata.blocks[0], bytes.bytes);
}

TEST(BlobReferenceMetadataTest, ChargeGrowsPerSlabAndIsReleased) {
  auto reservation_manager = std::make_shared<RejectingReservationManager>(
      std::numeric_limits<size_t>::max());
  {
    BlobReferenceMetadataCollector collector(
        reservation_manager,
        BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                 .sst_file_number = kTestSstFileNumber});
    ASSERT_OK(
        collector.Add(UnflattenedForTest(42, 10, 40), /*user_key_size=*/0));
    const size_t first = reservation_manager->GetTotalMemoryUsed();
    EXPECT_GE(first, 64 * 1024U);
    // About 11 bytes per entry, so 20000 entries need several 64 KB slabs.
    for (uint64_t i = 1; i < 20000; ++i) {
      ASSERT_OK(
          collector.Add(UnflattenedForTest(
                            42, 10 + i * (40 + BlobLogRecord::kHeaderSize), 40),
                        /*user_key_size=*/0));
    }
    const size_t grown = reservation_manager->GetTotalMemoryUsed();
    EXPECT_GE(grown, first + 2 * 64 * 1024U);
    EXPECT_EQ(grown, collector.TEST_ReservedMemory());
  }
  EXPECT_EQ(reservation_manager->GetTotalMemoryUsed(), 0U);
  {
    BlobReferenceMetadataCollector collector(
        reservation_manager,
        BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                 .sst_file_number = kTestSstFileNumber});
    ASSERT_OK(
        collector.Add(UnflattenedForTest(42, 10, 40), /*user_key_size=*/0));
    EXPECT_GT(reservation_manager->GetTotalMemoryUsed(), 0U);
  }
  EXPECT_EQ(reservation_manager->GetTotalMemoryUsed(), 0U);
}

TEST(BlobReferenceMetadataTest, GroupsSpanAndShareBlocks) {
  // Build a kDirect group, many small groups that share blocks, and one large
  // group that spans several blocks.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  uint64_t expected_count = 0;
  for (uint64_t offset = 0; offset < 50; ++offset) {
    ASSERT_OK(collector.Add(LegacyForTest(10, 1000 + offset * 200),
                            /*user_key_size=*/0));
    ++expected_count;
  }
  for (uint64_t origin = 100; origin < 400; ++origin) {
    const uint64_t refs = 1 + origin % 7;
    for (uint64_t i = 0; i < refs; ++i) {
      ASSERT_OK(collector.Add(UnflattenedForTest(origin, 10 + i * 80, 40),
                              /*user_key_size=*/0));
      ++expected_count;
    }
  }
  constexpr uint64_t kLargeOrigin = 1000;
  for (uint64_t i = 0; i < 20000; ++i) {
    ASSERT_OK(collector.Add(UnflattenedForTest(kLargeOrigin, 10 + i * 80, 40),
                            /*user_key_size=*/0));
    ++expected_count;
  }
  MetadataForTest metadata;
  std::shared_ptr<const BlobReferenceSstSummary> summary;
  ASSERT_OK(FinishForTest(&collector, &metadata, &summary));
  ASSERT_EQ(metadata.descriptor.reference_count, expected_count);

  BlobReferenceIndex index;
  std::vector<BlobReference> all;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &all));
  ASSERT_EQ(all.size(), expected_count);
  ASSERT_EQ(index.groups.size(), 302U);
  ASSERT_GE(metadata.blocks.size(), 3U);

  // Reading each group through the index gives the same references, in the same
  // order, as walking every block.
  std::vector<BlobReference> by_group;
  for (size_t group = 0; group < index.groups.size(); ++group) {
    std::vector<BlobReference> references;
    ASSERT_OK(ReadGroupForTest(metadata, index, group, &references));
    ASSERT_EQ(references.size(), index.groups[group].group.reference_count);
    by_group.insert(by_group.end(), references.begin(), references.end());
  }
  ASSERT_EQ(by_group.size(), all.size());
  for (size_t i = 0; i < all.size(); ++i) {
    ASSERT_EQ(ReferenceFieldsForTest(by_group[i]),
              ReferenceFieldsForTest(all[i]))
        << i;
  }

  // Layout: some groups start inside a block, the large group spans at least
  // three blocks, and some block holds only that group.
  EXPECT_TRUE(std::any_of(index.groups.begin(), index.groups.end(),
                          [](const BlobReferenceIndexEntry& entry) {
                            return entry.run_offset > 0;
                          }));
  const size_t large =
      FindGroupForTest(index, {BlobReferenceEncodingClass::kIndirectUnflattened,
                               kLargeOrigin, kLargeOrigin});
  ASSERT_EQ(large, index.groups.size() - 1);
  EXPECT_GE(index.blocks.size() - index.groups[large].start_block, 3U);
  // A block after the large group's start block where no group starts holds
  // only a run of the large group (the last group).
  size_t single_run_blocks = 0;
  for (size_t block = index.groups[large].start_block + 1;
       block < index.blocks.size(); ++block) {
    single_run_blocks +=
        std::none_of(index.groups.begin(), index.groups.end(),
                     [&](const BlobReferenceIndexEntry& entry) {
                       return entry.start_block == block;
                     });
  }
  EXPECT_GE(single_run_blocks, 1U);

  // The indirect groups rebuilt from the index equal the groups of the
  // build-time summary.
  const std::vector<BlobReferenceGroup> rebuilt = IndirectGroupsForTest(index);
  ASSERT_EQ(rebuilt.size(), summary->groups.size());
  ASSERT_EQ(rebuilt.size(), 301U);
  for (size_t i = 0; i < rebuilt.size(); ++i) {
    EXPECT_EQ(rebuilt[i].origin_file_number,
              summary->groups[i].origin_file_number);
    EXPECT_EQ(rebuilt[i].reference_count, summary->groups[i].reference_count);
    EXPECT_EQ(rebuilt[i].stored_value_bytes,
              summary->groups[i].stored_value_bytes);
    EXPECT_EQ(rebuilt[i].record_bytes, summary->groups[i].record_bytes);
  }
}

TEST(BlobReferenceMetadataTest, GroupSpansBlocksAtTargetSize) {
  // A reference takes about 11 encoded bytes (gap, size, user key size and two
  // CRCs), so 3000 references of one group need more than one 16 KB block.
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (uint64_t i = 0; i < 3000; ++i) {
    ASSERT_OK(collector.Add(UnflattenedForTest(42, 10 + i * 80, 40),
                            /*user_key_size=*/0));
  }
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(index.groups.size(), 1U);
  EXPECT_GE(index.blocks.size(), 2U);
  for (const std::string& block : metadata.blocks) {
    EXPECT_LE(block.size(), 16U * 1024);
  }
  std::vector<BlobReference> group;
  ASSERT_OK(ReadGroupForTest(metadata, index, 0, &group));
  EXPECT_EQ(group.size(), 3000U);
}

TEST(BlobReferenceMetadataTest, SingleBlockSst) {
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  ASSERT_OK(
      collector.Add(UnflattenedForTest(42, 700, 800), /*user_key_size=*/0));
  ASSERT_OK(
      collector.Add(UnflattenedForTest(42, 1700, 800), /*user_key_size=*/0));
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  BlobReferenceIndex index;
  std::vector<BlobReference> references;
  ASSERT_OK(DecodeAllForTest(metadata, &index, &references));
  ASSERT_EQ(index.blocks.size(), 1U);
  ASSERT_EQ(index.groups.size(), 1U);
  EXPECT_EQ(index.groups[0].group.reference_count, 2U);
  EXPECT_EQ(index.groups[0].group.stored_value_bytes, 1600U);
  EXPECT_EQ(references.size(), 2U);
}

TEST(BlobReferenceMetadataTest, EstimateCoversOneLargeGroup) {
  // EstimatedEncodedSize() is an upper bound of the metadata size. The table
  // builder adds it to the SST size estimate that decides when to cut an output
  // file. For one large group, the bound must not count one data block per
  // reference: that would cut SST outputs far below their target size. The
  // bound must still cover every block that Finish() writes.
  constexpr uint64_t kReferenceCount = 20000;
  constexpr size_t kBlockTrailerBytes = 5;
  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  for (uint64_t i = 0; i < kReferenceCount; ++i) {
    ASSERT_OK(collector.Add(UnflattenedForTest(42, 1000 + i * 200, 80),
                            /*user_key_size=*/0));
  }
  const uint64_t estimated = collector.EstimatedEncodedSize();
  EXPECT_LT(estimated, 224U * kReferenceCount / 2);
  MetadataForTest metadata;
  ASSERT_OK(FinishForTest(&collector, &metadata));
  ASSERT_GT(metadata.blocks.size(), 1U);
  uint64_t written = metadata.index.size() + kBlockTrailerBytes;
  for (const std::string& block : metadata.blocks) {
    written += block.size() + kBlockTrailerBytes;
  }
  EXPECT_GE(estimated, written);
}

TEST(BlobReferenceMetadataTest, EstimateCoversDistinctWideEncodedGroups) {
  constexpr size_t kBlockTrailerBytes = 5;
  const std::shared_ptr<Cache> cache = NewLRUCache(16 * 1024 * 1024);
  const std::shared_ptr<CacheReservationManager> reservation =
      std::make_shared<CacheReservationManagerImpl<
          CacheEntryRole::kBlobReferenceMetadataBuildingBuffer>>(cache);
  auto collector = std::make_unique<BlobReferenceMetadataCollector>(
      reservation,
      BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                               .sst_file_number = kTestSstFileNumber});
  constexpr uint64_t kReferences = 200;
  for (uint64_t i = 0; i < kReferences; ++i) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(&encoded, (1ULL << 60) + 1,
                                  {.origin_file_number = (1ULL << 59) + i,
                                   .original_value_offset = (1ULL << 63) + i,
                                   .stored_value_size = 1ULL << 40,
                                   .compression = kZSTD,
                                   .stable_checksum = 123,
                                   .stored_checksum = 456});
    ASSERT_OK(
        collector->Add(DecodeBlobIndexForTest(encoded), /*user_key_size=*/0));
  }
  const uint64_t estimated = collector->EstimatedEncodedSize();
  EXPECT_GT(cache->GetPinnedUsage(), 0U);
  uint64_t written = 0;
  BlobReferenceMetadataDescriptor descriptor;
  ASSERT_OK(collector->Finish(
      [&written](const Slice& block, bool /*is_index_block*/,
                 BlockHandle* handle) -> Status {
        *handle = BlockHandle((1ULL << 60) + written, block.size());
        written += block.size() + kBlockTrailerBytes;
        return Status::OK();
      },
      &descriptor, /*summary=*/nullptr));
  EXPECT_GE(estimated, written);
  collector.reset();
  EXPECT_EQ(cache->GetPinnedUsage(), 0U);
}

TEST(BlobReferenceMetadataTest, RepeatedReferenceFailsClosed) {
  // Each physical record is referenced at most once per SST. A repeat, with
  // equal or with different fields, fails the build.
  std::string first_encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(&first_encoded,
                                           {.origin_file_number = 42,
                                            .original_value_offset = 700,
                                            .stored_value_size = 800,
                                            .compression = kZSTD,
                                            .stable_checksum = 123,
                                            .stored_checksum = 4321});
  std::string conflicting_encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(&conflicting_encoded,
                                           {.origin_file_number = 42,
                                            .original_value_offset = 700,
                                            .stored_value_size = 800,
                                            .compression = kZSTD,
                                            .stable_checksum = 456,
                                            .stored_checksum = 4321});
  std::string other_encoded;
  BlobIndex::EncodeUnflattenedIndirectBlob(&other_encoded,
                                           {.origin_file_number = 42,
                                            .original_value_offset = 2000,
                                            .stored_value_size = 800,
                                            .compression = kZSTD,
                                            .stable_checksum = 789,
                                            .stored_checksum = 4321});
  std::string legacy_encoded;
  BlobIndex::EncodeBlob(&legacy_encoded, /*file_number=*/41, /*offset=*/500,
                        /*size=*/1000, kLZ4Compression);

  for (const std::string* repeat :
       {&first_encoded, &conflicting_encoded, &legacy_encoded}) {
    BlobReferenceMetadataCollector collector(
        nullptr,
        BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                 .sst_file_number = kTestSstFileNumber});
    ASSERT_OK(collector.Add(DecodeBlobIndexForTest(first_encoded),
                            /*user_key_size=*/0));
    ASSERT_OK(collector.Add(DecodeBlobIndexForTest(other_encoded),
                            /*user_key_size=*/0));
    ASSERT_OK(collector.Add(DecodeBlobIndexForTest(legacy_encoded),
                            /*user_key_size=*/0));
    // Add() finds a repeat of the group's last offset. Finish() finds a repeat
    // of an earlier offset when it sorts the group.
    const Status add_status =
        collector.Add(DecodeBlobIndexForTest(*repeat), /*user_key_size=*/0);
    if (!add_status.ok()) {
      EXPECT_TRUE(add_status.IsCorruption()) << add_status.ToString();
      EXPECT_NE(add_status.ToString().find("Repeated blob reference"),
                std::string::npos)
          << add_status.ToString();
    }
    MetadataForTest metadata;
    const Status status = FinishForTest(&collector, &metadata);
    EXPECT_TRUE(status.IsCorruption()) << status.ToString();
    EXPECT_NE(status.ToString().find("Repeated blob reference"),
              std::string::npos)
        << status.ToString();
  }
}

TEST(BlobReferenceMetadataTest, ValidatesExcludedSameFileReference) {
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, kCurrentFileBlobIndexFileNumber,
                        /*offset=*/500,
                        /*size=*/1000, static_cast<CompressionType>(0xff));
  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  EXPECT_TRUE(blob_index.Validate().IsCorruption());

  BlobReferenceMetadataCollector collector(
      nullptr, BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                        .sst_file_number = kTestSstFileNumber});
  EXPECT_TRUE(collector.Add(blob_index, /*user_key_size=*/0).IsCorruption());
}

TEST(BlobReferenceMetadataTest, ReservationFailureKeepsCollection) {
  // Every charge fails. The collector keeps collecting without the charge,
  // and the destructor still releases what the reservation holds.
  auto reservation_manager =
      std::make_shared<RejectingReservationManager>(/*limit=*/1);
  auto collector = std::make_unique<BlobReferenceMetadataCollector>(
      reservation_manager,
      BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                               .sst_file_number = kTestSstFileNumber});
  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, /*file_number=*/41, /*offset=*/500,
                        /*size=*/1000, kNoCompression);
  ASSERT_OK(collector->Add(DecodeBlobIndexForTest(encoded),
                           /*user_key_size=*/0));
  MetadataForTest metadata;
  std::shared_ptr<const BlobReferenceSstSummary> summary;
  ASSERT_OK(FinishForTest(collector.get(), &metadata, &summary));
  EXPECT_GT(collector->uncharged_charge_failures(), 0U);
  collector.reset();
  summary.reset();
  EXPECT_EQ(reservation_manager->GetTotalMemoryUsed(), 0);
}

TEST(BlobReferenceMetadataTest, SummaryKeepsOnlyItsOwnCharge) {
  auto reservation_manager =
      std::make_shared<RejectingReservationManager>(/*limit=*/1024 * 1024);
  auto collector = std::make_unique<BlobReferenceMetadataCollector>(
      reservation_manager,
      BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                               .sst_file_number = kTestSstFileNumber});
  ASSERT_OK(
      collector->Add(UnflattenedForTest(42, 10, 40), /*user_key_size=*/0));
  const size_t working = reservation_manager->GetTotalMemoryUsed();
  EXPECT_GE(working, 64 * 1024U);
  MetadataForTest metadata;
  std::shared_ptr<const BlobReferenceSstSummary> summary;
  ASSERT_OK(FinishForTest(collector.get(), &metadata, &summary));
  // The working buffers are freed; the summary's charge covers only itself.
  EXPECT_EQ(reservation_manager->GetTotalMemoryUsed(), summary->MemoryUsage());
  collector.reset();
  EXPECT_EQ(reservation_manager->GetTotalMemoryUsed(), summary->MemoryUsage());
  summary.reset();
  EXPECT_EQ(reservation_manager->GetTotalMemoryUsed(), 0U);
}

TEST(BlobReferenceMetadataTest, ReleasesReservationOnDestruction) {
  auto reservation_manager =
      std::make_shared<RejectingReservationManager>(/*limit=*/1024 * 1024);
  {
    BlobReferenceMetadataCollector collector(
        reservation_manager,
        BlobReferenceSstIdentity{.column_family_id = kTestColumnFamilyId,
                                 .sst_file_number = kTestSstFileNumber});
    std::string encoded;
    BlobIndex::EncodeBlob(&encoded, /*file_number=*/41, /*offset=*/500,
                          /*size=*/1000, kNoCompression);
    ASSERT_OK(
        collector.Add(DecodeBlobIndexForTest(encoded), /*user_key_size=*/0));
    EXPECT_GT(reservation_manager->GetTotalMemoryUsed(), 0);
  }
  EXPECT_EQ(reservation_manager->GetTotalMemoryUsed(), 0);
}

namespace {

BlobReferenceGroup MakeCensusGroup(uint64_t generation, uint64_t origin) {
  BlobReferenceGroup group;
  group.encoding_class = generation == origin
                             ? BlobReferenceEncodingClass::kIndirectUnflattened
                             : BlobReferenceEncodingClass::kIndirectFlattened;
  group.generation_file_number = generation;
  group.origin_file_number = origin;
  group.reference_count = 1;
  return group;
}

std::shared_ptr<const BlobReferenceSstSummary> MakeCensusFile(
    uint64_t file_number, std::vector<BlobReferenceGroup> groups) {
  auto file = std::make_shared<BlobReferenceSstSummary>();
  file->file_number = file_number;
  file->groups = std::move(groups);
  return file;
}

}  // namespace

TEST(BlobReferenceCensusTest, GroupsOfOneGenerationMakeOneHolder) {
  // A holder of a generation number is an SST that has at least one group with
  // that generation number. The census must count each SST once per generation
  // number, even if the SST has several groups with that generation number.
  //
  // Relocation file 30 holds values whose origins are blob files 10 and 20.
  // References flattened to file 30 have generation 30 and origin 10 or 20, so
  // both groups use the generation number 30.
  // - SST 100 has both groups of generation number 30 and one group of blob
  //   file 40.
  // - SST 200 has one group of generation number 30.
  const auto group = [](uint64_t generation, uint64_t origin,
                        uint64_t references) {
    BlobReferenceGroup result = MakeCensusGroup(generation, origin);
    result.reference_count = references;
    result.stored_value_bytes = 10 * references;
    result.record_bytes = 20 * references;
    return result;
  };
  const auto first = MakeCensusFile(
      100, {group(30, 10, 2), group(30, 20, 3), group(40, 40, 1)});
  const auto second = MakeCensusFile(200, {group(30, 10, 4)});
  // Added in the same delta that removes SST 200 (see below).
  const auto third = MakeCensusFile(300, {group(30, 10, 1), group(30, 20, 1)});

  std::shared_ptr<BlobReferenceCensusState> state;
  ASSERT_OK(BlobReferenceCensus::BuildState(7, {first, second}, &state));
  ASSERT_EQ(state->generation_count(), 2U);
  ASSERT_EQ(*state->FindSstFiles(30), (std::vector<uint64_t>{100, 200}));
  ASSERT_EQ(state->FindTotals(30)->reference_count, 9U);
  ASSERT_EQ(state->FindTotals(30)->stored_value_bytes, 90U);
  ASSERT_EQ(state->FindTotals(30)->record_bytes, 180U);

  // The census worker must find the same holders as BuildState(). This must
  // also hold after FinalizeWorkerBuild(), when each delta updates the encoded
  // holder sets.
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, nullptr));
  ASSERT_OK(census.AddStartingFile(token, first));
  ASSERT_OK(census.AddStartingFile(token, second));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 7, 7),
            BlobReferenceCensus::PublishResult::kPublished);
  std::string difference;
  ASSERT_TRUE(
      census.TEST_GetReadyState(7)->SameMemberships(*state, &difference))
      << difference;

  // Removing SST 100 removes it once from the holders of generation number 30,
  // and removes generation number 40, which no SST uses any more.
  BlobReferenceCensusDelta removal;
  removal.base_version_number = 7;
  removal.version_number = 8;
  removal.removed_files = {100};
  removal.removed_summaries = {first};
  std::shared_ptr<BlobReferenceCensusState> removed;
  ASSERT_OK(BlobReferenceCensus::BuildState(8, {second}, &removed));
  ASSERT_EQ(*removed->FindSstFiles(30), (std::vector<uint64_t>{200}));
  ASSERT_EQ(removed->FindSstFiles(40), nullptr);
  ASSERT_EQ(removed->FindTotals(30)->reference_count, 4U);
  ASSERT_OK(census.ApplyWorkerDelta(token, removal));
  ASSERT_EQ(census.TryPublishWorker(token, 8, 8),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(
      census.TEST_GetReadyState(8)->SameMemberships(*removed, &difference))
      << difference;

  // One delta removes SST 200 and adds SST 300. Both groups of generation
  // number 30 in SST 300 use one slot. Removals free their slots before
  // additions take the lowest free slot, so SST 300 gets slot 0, which SST 100
  // used.
  BlobReferenceCensusDelta replace;
  replace.base_version_number = 8;
  replace.version_number = 9;
  replace.removed_files = {200};
  replace.removed_summaries = {second};
  replace.added_files = {third};
  std::shared_ptr<BlobReferenceCensusState> replaced;
  ASSERT_OK(BlobReferenceCensus::BuildState(9, {third}, &replaced));
  ASSERT_EQ(*replaced->FindSstFiles(30), (std::vector<uint64_t>{300}));
  ASSERT_EQ(replaced->FindTotals(30)->reference_count, 2U);
  ASSERT_OK(census.ApplyWorkerDelta(token, replace));
  ASSERT_EQ(census.TryPublishWorker(token, 9, 9),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(
      census.TEST_GetReadyState(9)->SameMemberships(*replaced, &difference))
      << difference;
  auto garbage = census.Cancel();
}

TEST(BlobReferenceCensusTest, SlotTableAndSlotSetsMatchModel) {
  // Random SST additions and removals over kGenerations (6) generation
  // numbers. Each SST holds a random subset of the generations. After each
  // change, the slot table plus one slot set per generation must give the same
  // SSTs per generation as a simple model. The sets must also switch from list
  // to bitmap and back.
  for (uint32_t seed : {1u, 7u, 42u, 2026u}) {
    SCOPED_TRACE("seed " + std::to_string(seed));
    Random rnd(seed);
    constexpr uint64_t kGenerations = 6;
    BlobCensusSlotTable slots;
    std::array<BlobCensusSlotSet, kGenerations> sets;
    std::map<uint64_t, std::set<uint64_t>> model;  // generation -> SSTs
    std::map<uint64_t, std::vector<uint64_t>>
        sst_generations;  // SST -> generations
    std::vector<uint32_t> scratch;
    uint64_t next_file = 1;
    bool saw_bitmap = false;
    bool saw_list_after_bitmap = false;
    for (int step = 0; step < 3000; ++step) {
      SCOPED_TRACE("step " + std::to_string(step));
      // One change: remove some SSTs and add some, like a compaction.
      std::vector<uint64_t> removed_files;
      for (const auto& entry : sst_generations) {
        // In some phases, remove more SSTs than are added, so sets shrink
        // again.
        const uint32_t remove_one_in = (step / 500) % 2 == 0 ? 6 : 2;
        if (rnd.OneIn(remove_one_in)) {
          removed_files.push_back(entry.first);
        }
      }
      std::array<std::vector<uint32_t>, kGenerations> removed;
      std::array<std::vector<uint32_t>, kGenerations> added;
      for (uint64_t file : removed_files) {
        uint32_t slot = 0;
        ASSERT_OK(slots.Release(file, &slot));
        for (uint64_t generation : sst_generations[file]) {
          removed[generation].push_back(slot);
          model[generation].erase(file);
        }
        sst_generations.erase(file);
      }
      const uint32_t additions = rnd.Uniform(8);
      for (uint32_t i = 0; i < additions; ++i) {
        const uint64_t file = next_file++;
        uint32_t slot = 0;
        ASSERT_OK(slots.Assign(file, &slot));
        // Assign() must give the lowest free slot.
        for (uint32_t lower = 0; lower < slot; ++lower) {
          ASSERT_NE(slots.FileNumber(lower), 0U);
        }
        std::vector<uint64_t>& generations = sst_generations[file];
        for (uint64_t generation = 0; generation < kGenerations; ++generation) {
          if (rnd.OneIn(static_cast<int>(generation) + 1)) {
            generations.push_back(generation);
            added[generation].push_back(slot);
            model[generation].insert(file);
          }
        }
      }
      for (uint64_t generation = 0; generation < kGenerations; ++generation) {
        std::sort(removed[generation].begin(), removed[generation].end());
        std::sort(added[generation].begin(), added[generation].end());
        const bool was_bitmap = sets[generation].is_bitmap();
        ASSERT_OK(sets[generation].Update(removed[generation],
                                          added[generation], slots.slot_count(),
                                          &scratch));
        saw_bitmap |= sets[generation].is_bitmap();
        saw_list_after_bitmap |= was_bitmap && !sets[generation].is_bitmap();
        std::vector<uint32_t> held;
        sets[generation].AppendTo(&held);
        std::set<uint64_t> files;
        for (uint32_t slot : held) {
          ASSERT_TRUE(sets[generation].Contains(slot));
          files.insert(slots.FileNumber(slot));
        }
        ASSERT_EQ(files, model[generation]);
        ASSERT_EQ(sets[generation].count(), model[generation].size());
      }
      ASSERT_EQ(slots.live_count(), sst_generations.size());
    }
    EXPECT_TRUE(saw_bitmap);
    EXPECT_TRUE(saw_list_after_bitmap);
  }
}

TEST(BlobReferenceCensusTest, SlotSetInPlaceUpdatesMatchModel) {
  // A dense set is a bitmap and is updated in place; a sparse set is a list.
  // After every change, the set must hold the same slots as a model, and a new
  // set built by Assign() from the model must hold the same slots. The dense
  // case must switch between list and bitmap in both directions.
  const uint32_t seed = 20261108;
  SCOPED_TRACE("seed " + std::to_string(seed));
  Random rnd(seed);
  constexpr uint32_t kSlots = 4096;
  for (const uint32_t density_one_in : {2u, 300u}) {
    SCOPED_TRACE(density_one_in);
    BlobCensusSlotSet set;
    std::set<uint32_t> model;
    std::vector<uint32_t> scratch;
    bool saw_bitmap = false;
    bool saw_switch_to_list = false;
    bool saw_switch_to_bitmap = false;
    for (int step = 0; step < 400; ++step) {
      // Grow for 200 steps, then shrink, so dense sets switch back to lists.
      const bool shrinking = step >= 200;
      std::vector<uint32_t> removed;
      std::vector<uint32_t> added;
      for (uint32_t slot : model) {
        if (rnd.OneIn(shrinking ? 3 : 50)) {
          removed.push_back(slot);
        }
      }
      const int additions =
          density_one_in == 2 ? (shrinking ? 4 : 64) : (shrinking ? 0 : 1);
      for (int i = 0; i < additions; ++i) {
        const uint32_t slot = rnd.Uniform(kSlots);
        const bool held =
            model.count(slot) != 0 &&
            !std::binary_search(removed.begin(), removed.end(), slot);
        if (!held) {
          added.push_back(slot);
        }
      }
      std::sort(added.begin(), added.end());
      added.erase(std::unique(added.begin(), added.end()), added.end());
      const bool was_bitmap = set.is_bitmap();
      ASSERT_OK(set.Update(removed, added, kSlots, &scratch));
      for (uint32_t slot : removed) {
        model.erase(slot);
      }
      model.insert(added.begin(), added.end());
      saw_bitmap |= set.is_bitmap();
      saw_switch_to_list |= was_bitmap && !set.is_bitmap();
      saw_switch_to_bitmap |= !was_bitmap && set.is_bitmap();
      std::vector<uint32_t> held;
      set.AppendTo(&held);
      ASSERT_EQ(held, std::vector<uint32_t>(model.begin(), model.end()));
      ASSERT_EQ(set.count(), model.size());
      BlobCensusSlotSet reference;
      reference.Assign(held, kSlots);
      std::vector<uint32_t> reference_held;
      reference.AppendTo(&reference_held);
      ASSERT_EQ(reference_held, held);
      for (int i = 0; i < 16; ++i) {
        const uint32_t slot = rnd.Uniform(kSlots);
        ASSERT_EQ(set.Contains(slot), model.count(slot) != 0);
      }
    }
    if (density_one_in == 2) {
      EXPECT_TRUE(saw_bitmap);
      EXPECT_TRUE(saw_switch_to_bitmap);
      EXPECT_TRUE(saw_switch_to_list);
    }
  }
}

TEST(BlobReferenceCensusTest, SlotSetListHasExactSize) {
  // A list is encoded at its exact size, so its memory charge has no unused
  // capacity. The smallest capacity of a std::string that holds a given number
  // of bytes depends on the standard library, so the test builds a reference
  // string of the same size the same way (reserve, append, shrink_to_fit) and
  // compares with its capacity.
  const auto exact_capacity = [](size_t bytes) {
    std::string reference;
    reference.reserve(bytes);
    reference.append(bytes, '\0');
    reference.shrink_to_fit();
    return reference.capacity();
  };
  BlobCensusSlotSet set;
  std::vector<uint32_t> scratch;
  std::vector<uint32_t> slots;
  for (uint32_t slot = 0; slot < 4000; slot += 97) {
    slots.push_back(slot);
  }
  ASSERT_OK(set.Update({}, slots, 1 << 20, &scratch));
  ASSERT_FALSE(set.is_bitmap());
  size_t list_bytes = 0;
  for (size_t i = 0; i < slots.size(); ++i) {
    list_bytes += VarintLength(i == 0 ? slots[i] : slots[i] - slots[i - 1]);
  }
  EXPECT_EQ(set.MemoryUsage(), exact_capacity(list_bytes));
  // After half of the slots are removed, the list still has its exact size.
  std::vector<uint32_t> removed;
  for (size_t i = 0; i < slots.size(); i += 2) {
    removed.push_back(slots[i]);
  }
  ASSERT_OK(set.Update(removed, {}, 1 << 20, &scratch));
  std::vector<uint32_t> held;
  set.AppendTo(&held);
  list_bytes = 0;
  for (size_t i = 0; i < held.size(); ++i) {
    list_bytes += VarintLength(i == 0 ? held[i] : held[i] - held[i - 1]);
  }
  EXPECT_EQ(set.MemoryUsage(), exact_capacity(list_bytes));
}

TEST(BlobReferenceCensusTest, SlotSetSwitchToBitmapFreesList) {
  // A list that switches to a bitmap frees its buffer, so the memory charge
  // covers only the bitmap. In this example, the set has room for 4,096 slots,
  // so a full bitmap takes 512 bytes.
  BlobCensusSlotSet set;
  std::vector<uint32_t> scratch;
  // Slots 0 to 399 make a list of 400 one-byte gaps, below the 512 bytes of
  // the bitmap, so the set stays a list.
  std::vector<uint32_t> first;
  for (uint32_t slot = 0; slot < 400; ++slot) {
    first.push_back(slot);
  }
  ASSERT_OK(set.Update({}, first, 4096, &scratch));
  ASSERT_FALSE(set.is_bitmap());
  const size_t list_usage = set.MemoryUsage();
  ASSERT_GE(list_usage, 400);
  // Slots 400 to 599 make the list 600 bytes, above 512, so the set switches
  // to a bitmap. The bitmap needs 10 words (80 bytes) for slots up to 599.
  std::vector<uint32_t> more;
  for (uint32_t slot = 400; slot < 600; ++slot) {
    more.push_back(slot);
  }
  ASSERT_OK(set.Update({}, more, 4096, &scratch));
  ASSERT_TRUE(set.is_bitmap());
  // With the old list buffer still allocated, the charge would be at least
  // list_usage.
  EXPECT_LT(set.MemoryUsage(), list_usage);
}

TEST(BlobReferenceCensusTest, FailedFinalizeFailsAgain) {
  // FinalizeWorkerBuild() builds the holder sets one generation number at a
  // time and stops at the first one that does not match. A later call must
  // return the same Corruption again, not build the rest from the partial
  // state. In this example, SST 100 holds generation numbers 10 and 30, and
  // SST 200 holds 10. A wrong delta removes SST 200 with a summary that says
  // it holds 30. Generation number 10 builds its holder set, and 30 fails.
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, nullptr));
  ASSERT_OK(census.AddStartingFile(
      token,
      MakeCensusFile(100, {MakeCensusGroup(10, 10), MakeCensusGroup(30, 30)})));
  ASSERT_OK(census.AddStartingFile(
      token, MakeCensusFile(200, {MakeCensusGroup(10, 10)})));
  BlobReferenceCensusDelta delta;
  delta.base_version_number = 7;
  delta.version_number = 8;
  delta.removed_files = {200};
  delta.removed_summaries = {MakeCensusFile(200, {MakeCensusGroup(30, 30)})};
  ASSERT_OK(census.ApplyWorkerDelta(token, delta));
  const Status first = census.FinalizeWorkerBuild(token);
  ASSERT_TRUE(first.IsCorruption()) << first.ToString();
  const Status second = census.FinalizeWorkerBuild(token);
  ASSERT_TRUE(second.IsCorruption()) << second.ToString();
  EXPECT_EQ(second.ToString(), first.ToString());
}

TEST(BlobReferenceCensusTest, SlotSetRejectsMismatchedChanges) {
  BlobCensusSlotSet set;
  std::vector<uint32_t> scratch;
  ASSERT_OK(set.Update({}, {1, 5, 9}, 16, &scratch));
  // Removing a slot that the set does not hold, or adding a slot that it holds,
  // is Corruption.
  ASSERT_TRUE(set.Update({2}, {}, 16, &scratch).IsCorruption());
  ASSERT_TRUE(set.Update({}, {5}, 16, &scratch).IsCorruption());
  // A slot freed and reused by another SST in the same change stays held.
  ASSERT_OK(set.Update({5}, {5}, 16, &scratch));
  std::vector<uint32_t> held;
  set.AppendTo(&held);
  ASSERT_EQ(held, (std::vector<uint32_t>{1, 5, 9}));
  BlobCensusSlotTable slots;
  uint32_t slot = 0;
  ASSERT_OK(slots.Assign(10, &slot));
  ASSERT_TRUE(slots.Assign(10, &slot).IsCorruption());
  ASSERT_TRUE(slots.Release(11, &slot).IsCorruption());
}

TEST(BlobReferenceCensusTest, MutableWorkerPublishesOnlyCaughtUpEpoch) {
  BlobReferenceCensus census;
  size_t wakeups = 0;
  census.SetBuildWakeup(
      [](void* owner, void* count) {
        // NeedsWorkerRun() locks the census mutex, so this call also shows that
        // the wakeup runs without that mutex held.
        EXPECT_TRUE(
            static_cast<BlobReferenceCensus*>(owner)->NeedsWorkerRun(9));
        ++*static_cast<size_t*>(count);
      },
      &census, &wakeups);
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, nullptr));
  ASSERT_OK(census.AddStartingFile(
      token, MakeCensusFile(100, {MakeCensusGroup(10, 10)})));
  auto delta = std::make_shared<BlobReferenceCensusDelta>();
  delta->base_version_number = 7;
  delta->version_number = 9;
  delta->removed_files = {100};
  delta->removed_summaries = {MakeCensusFile(100, {MakeCensusGroup(10, 10)})};
  delta->added_files = {MakeCensusFile(200, {MakeCensusGroup(20, 10)})};
  BlobReferenceCensusPreparedInstall prepared;
  const uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInstall(delta, reservation, &prepared));
  ASSERT_NE(prepared.log_entry, nullptr);
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 7, 7),
            BlobReferenceCensus::PublishResult::kInstallPending);
  ASSERT_FALSE(census.NeedsWorkerRun(7));
  auto garbage = census.Install(prepared);
  // The change log owns the linked entry, and the entry owns the delta, so the
  // prepared install, which can wait in the deferred census cleanup, keeps
  // neither.
  ASSERT_EQ(prepared.log_entry, nullptr);
  ASSERT_EQ(prepared.delta, nullptr);
  ASSERT_EQ(wakeups, 1U);
  ASSERT_TRUE(census.NeedsWorkerRun(9));
  ASSERT_FALSE(census.IsReady(9));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 7, 9),
            BlobReferenceCensus::PublishResult::kQueueNotEmpty);

  BlobReferenceCensusDeltaQueue deltas;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_FALSE(overflowed);
  ASSERT_EQ(deltas.size(), 1U);
  ASSERT_OK(census.ApplyWorkerDelta(token, *deltas.front()));
  ASSERT_OK(census.AcknowledgeBuildDeltas(token, delta->QueueWeight()));
  ASSERT_EQ(census.WorkerVersionNumber(), 9U);
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 9, 9),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(census.IsReady(9));

  uint64_t gc_token = 0;
  {
    ASSERT_OK(census.BeginGcJob(9, &gc_token));
    std::shared_ptr<BlobReferenceCensusState> selected;
    ASSERT_OK(census.SnapshotSelected(gc_token, 9, {10, 20},
                                      /*gc_block_cache=*/nullptr,
                                      /*empty_files_only=*/false,
                                      /*copied_uncharged=*/nullptr, &selected));
    ASSERT_EQ(selected->GetSstCount(10), 0U);
    ASSERT_EQ(selected->GetSstCount(20), 1U);
    census.EndGcJob(gc_token);
  }

  // After the publish, the same build cursor stays and receives the deltas of
  // later installs.
  auto empty_delta = std::make_shared<BlobReferenceCensusDelta>();
  empty_delta->base_version_number = 9;
  empty_delta->version_number = 12;
  const uint64_t next_reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInstall(empty_delta, next_reservation, &prepared));
  garbage = census.Install(prepared);
  ASSERT_FALSE(census.IsReady(12));
  deltas.clear();
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_OK(census.ApplyWorkerDelta(token, *deltas.front()));
  ASSERT_OK(census.AcknowledgeBuildDeltas(token, empty_delta->QueueWeight()));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 12, 12),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(census.IsReady(12));
  ASSERT_FALSE(census.IsReady(9));
}

TEST(BlobReferenceCensusTest,
     CancelPendingInstallWakesOnlyMatchingReservation) {
  BlobReferenceCensus census;
  size_t wakeups = 0;
  census.SetBuildWakeup(
      [](void* owner, void* count) {
        EXPECT_TRUE(
            static_cast<BlobReferenceCensus*>(owner)->NeedsWorkerRun(7));
        ++*static_cast<size_t*>(count);
      },
      &census, &wakeups);
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, nullptr));
  const uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 7, 7),
            BlobReferenceCensus::PublishResult::kInstallPending);
  ASSERT_FALSE(census.NeedsWorkerRun(7));
  census.CancelInstallPreparation(reservation + 1);
  ASSERT_EQ(wakeups, 0U);
  ASSERT_FALSE(census.NeedsWorkerRun(7));
  census.CancelInstallPreparation(reservation);
  ASSERT_EQ(wakeups, 1U);
  ASSERT_TRUE(census.NeedsWorkerRun(7));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 7, 7),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_FALSE(census.NeedsWorkerRun(7));
}

TEST(BlobReferenceCensusTest, InvalidatedInstallDetachesCanceledBuildContext) {
  BlobReferenceCensus census;
  const uint64_t old_token = census.BeginBuild();
  auto context = std::make_shared<BlobReferenceCensusBuildContext>();
  context->token = old_token;
  ASSERT_TRUE(census.TrySetBuildContext(context));
  ASSERT_OK(census.ResetWorkerState(old_token, 7, nullptr, nullptr));
  size_t wakeups = 0;
  census.SetBuildWakeup(
      [](void* owner, void* count) {
        EXPECT_EQ(static_cast<BlobReferenceCensus*>(owner)->GetBuildContext(),
                  nullptr);
        ++*static_cast<size_t*>(count);
      },
      &census, &wakeups);
  const uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.FinalizeWorkerBuild(old_token));
  ASSERT_EQ(census.TryPublishWorker(old_token, 7, 7),
            BlobReferenceCensus::PublishResult::kInstallPending);
  BlobReferenceCensusPreparedInstall prepared;
  ASSERT_OK(census.PrepareInvalidation(reservation, &prepared));
  auto garbage = census.Install(prepared);
  ASSERT_EQ(garbage.build_context, context);
  ASSERT_EQ(census.GetBuildContext(), nullptr);
  ASSERT_EQ(wakeups, 1U);
  ASSERT_EQ(census.TryPublishWorker(old_token, 7, 7),
            BlobReferenceCensus::PublishResult::kStale);
  const uint64_t new_token = census.BeginBuild();
  ASSERT_NE(new_token, old_token);
  ASSERT_OK(census.ResetWorkerState(new_token, 8, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(new_token));
  ASSERT_EQ(census.TryPublishWorker(new_token, 8, 8),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(census.IsReady(8));
}

TEST(BlobReferenceCensusTest,
     CanceledWorkerSliceCannotInvalidateReplacementReadyState) {
  BlobReferenceCensus census;
  const uint64_t old_token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(old_token, 7, nullptr, nullptr));
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"BlobReferenceCensus::AddStartingFile:CapturedWorker",
        "BlobReferenceCensusTest::OldWorkerCaptured"},
       {"BlobReferenceCensusTest::ReplacementReady",
        "BlobReferenceCensus::AddStartingFile:BeforeWorkerLock"}});
  sync_point->EnableProcessing();
  auto cleanup = Defer([&] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Status old_status;
  port::Thread old_slice([&] {
    old_status = census.AddStartingFile(
        old_token, MakeCensusFile(100, {MakeCensusGroup(10, 10)}));
  });
  TEST_SYNC_POINT("BlobReferenceCensusTest::OldWorkerCaptured");
  auto garbage = census.FailBuild(old_token);
  const uint64_t new_token = census.BeginBuild();
  const Status reset_status =
      census.ResetWorkerState(new_token, 7, nullptr, nullptr);
  const Status finalize_status = census.FinalizeWorkerBuild(new_token);
  const BlobReferenceCensus::PublishResult result =
      census.TryPublishWorker(new_token, 7, 7);
  TEST_SYNC_POINT("BlobReferenceCensusTest::ReplacementReady");
  old_slice.join();
  ASSERT_OK(reset_status);
  ASSERT_OK(finalize_status);
  ASSERT_EQ(result, BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(old_status.IsAborted());
  ASSERT_TRUE(census.IsReady(7));
}

TEST(BlobReferenceCensusTest,
     InvalidationBeforeContextAttachmentRejectsOldToken) {
  BlobReferenceCensus census;
  const uint64_t old_token = census.BeginBuild();
  bool released = false;
  auto context = std::shared_ptr<BlobReferenceCensusBuildContext>(
      new BlobReferenceCensusBuildContext,
      [&](BlobReferenceCensusBuildContext* stale) {
        // GetBuildContext() locks the census mutex, so this call shows that the
        // census releases the rejected context after it unlocks.
        EXPECT_EQ(census.GetBuildContext(), nullptr);
        released = true;
        delete stale;
      });
  auto cleanup = Defer([&] {
    context.reset();
    auto canceled = census.Cancel();
  });
  context->token = old_token;
  ASSERT_OK(census.ResetWorkerState(old_token, 7, nullptr, nullptr));
  const uint64_t reservation = census.BeginInstallPreparation();
  BlobReferenceCensusPreparedInstall prepared;
  ASSERT_OK(census.PrepareInvalidation(reservation, &prepared));
  auto garbage = census.Install(prepared);
  ASSERT_EQ(garbage.build_context, nullptr);
  ASSERT_FALSE(census.TrySetBuildContext(std::move(context)));
  ASSERT_TRUE(released);
  ASSERT_EQ(census.GetBuildContext(), nullptr);
  garbage = census.FailBuild(old_token);
  const uint64_t new_token = census.BeginBuild();
  auto fresh = std::make_shared<BlobReferenceCensusBuildContext>();
  fresh->token = new_token;
  ASSERT_TRUE(census.TrySetBuildContext(fresh));
  ASSERT_OK(census.ResetWorkerState(new_token, 8, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(new_token));
  ASSERT_EQ(census.TryPublishWorker(new_token, 8, 8),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(census.IsReady(8));
}

TEST(BlobGcCandidateHeapTest, RanksUpdatesAndRetiresPhysicalFiles) {
  BlobGcCandidateHeap heap;
  heap.SetPhysicalFile({10, 1000, true});
  heap.SetPhysicalFile({20, 2000, false});
  heap.SetPhysicalFile({30, 1000, false});
  ASSERT_OK(heap.AdjustLiveBytes(10, 300, 3, true));
  ASSERT_OK(heap.AdjustLiveBytes(20, 200, 2, true));
  ASSERT_OK(heap.AdjustLiveBytes(30, 800, 8, true));
  // In this example, file 30 has 800 of its 1000 bytes live, so its garbage
  // ratio is 20%. Select() ranks files by garbage ratio and has no garbage
  // limit, so it returns file 30 as the third file. The census job then
  // applies the indirect_blob_gc_garbage_ratio option with CutSelection(). At
  // 0.5, the cut removes file 30 and keeps files 20 (90%) and 10 (70%).
  BlobGcCandidateHeap::SelectionBytes selected_bytes{};
  BlobGcCandidateHeap::Selection selected = heap.Select(&selected_bytes);
  ASSERT_EQ(selected[0], 20U);
  ASSERT_EQ(selected[1], 10U);
  ASSERT_EQ(selected[2], 30U);
  ASSERT_EQ(selected[3], 0U);
  BlobGcCandidateHeap::CutSelection(0.5, selected_bytes, &selected);
  ASSERT_EQ(selected[0], 20U);
  ASSERT_EQ(selected[1], 10U);
  ASSERT_EQ(selected[2], 0U);
  ASSERT_OK(heap.AdjustLiveBytes(30, 800, 8, false));
  ASSERT_EQ(heap.Select()[0], 30U);
  heap.DeletePhysicalFile(30);
  ASSERT_EQ(heap.Select()[0], 20U);
  ASSERT_EQ(heap.size(), 2U);
  ASSERT_TRUE(heap.IsIndirectOrigin(10));
  ASSERT_FALSE(heap.IsIndirectOrigin(20));
}

TEST(BlobReferenceCensusTest, RetirementSelectionResumesAfterCursor) {
  // In this example, generation numbers 1 to 65 route to relocation file 100,
  // and no SST holds them. One selection returns at most 64 numbers (one route
  // retirement batch). Route retirement may skip some of them, for example
  // while one of them is a live file in the input Version of a running
  // compaction. The next selection starts after the last visited number, so the
  // numbers after the first 64 are not starved.
  BlobReferenceCensus census;
  auto forward = std::make_shared<BlobRelocationState>();
  for (uint64_t generation = 1; generation <= 65; ++generation) {
    forward->SetRoute(BlobRelocationRoute(generation, 100));
  }
  forward->Seal();
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, forward, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  BlobReferenceCensus::RetirementSelection selected =
      census.SelectWorkerRetirementSources(token);
  for (size_t i = 0; i < selected.size(); ++i) {
    ASSERT_EQ(selected[i], i + 1);
  }
  selected = census.SelectWorkerRetirementSources(token);
  ASSERT_EQ(selected[0], 65U);
  ASSERT_EQ(selected[1], 1U);
}

TEST(BlobReferenceCensusTest, MetadataCommitIsExclusive) {
  // Route retirement and the indirect blob GC install both make metadata-only
  // commits. Only one may run at a time, so a second BeginMetadataCommit()
  // returns TryAgain until the first one ends.
  BlobReferenceCensus census;
  ASSERT_OK(census.BeginMetadataCommit());
  ASSERT_TRUE(census.BeginMetadataCommit().IsTryAgain());
  census.EndMetadataCommit(true);
  ASSERT_OK(census.BeginMetadataCommit());
  census.EndMetadataCommit(true);
}

TEST(BlobReferenceCensusTest, MetadataCommitFencesGcJobStart) {
  // While a metadata-only commit (BeginMetadataCommit()) is in progress,
  // BeginGcJob() returns TryAgain, because the commit can change routes that
  // the indirect blob GC job depends on. After a commit with an unknown result
  // (EndMetadataCommit(false)), BeginGcJob() returns Busy until
  // ResolveMetadataCommitAmbiguity() runs.
  BlobReferenceCensus census;
  const uint64_t build_token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(build_token, 7, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(build_token));
  ASSERT_EQ(census.TryPublishWorker(build_token, 7, 7),
            BlobReferenceCensus::PublishResult::kPublished);

  ASSERT_OK(census.BeginMetadataCommit());
  uint64_t gc_token = 0;
  ASSERT_TRUE(census.BeginGcJob(7, &gc_token).IsTryAgain());
  census.EndMetadataCommit(true);
  ASSERT_OK(census.BeginGcJob(7, &gc_token));
  // Only one GC job is registered at a time.
  uint64_t second_token = 0;
  ASSERT_TRUE(census.BeginGcJob(7, &second_token).IsBusy());
  census.EndGcJob(gc_token);

  ASSERT_OK(census.BeginMetadataCommit());
  census.EndMetadataCommit(false);
  ASSERT_TRUE(census.BeginGcJob(7, &gc_token).IsBusy());
  census.ResolveMetadataCommitAmbiguity();
  ASSERT_OK(census.BeginGcJob(7, &gc_token));
  census.EndGcJob(gc_token);
}

TEST(BlobGcCandidateHeapTest, LostRelocationFileGarbageIsNotCounted) {
  // A relocation file marked lost (best_efforts_blob_db_repair) has no
  // readable values, so its garbage is left out of the relocation garbage
  // total. While
  // references to it remain, GC cannot collect from it, so its garbage is also
  // left out of the collectable garbage (garbage-bytes-collectable of
  // rocksdb.indirect-blob-gc-stats). When no reference remains, the empty-file
  // cleanup removes it, so its bytes count as collectable again.
  //
  // In this example, relocation files 30 and 40 have 1000 bytes each. File 30
  // has 400 live bytes (600 garbage bytes), and file 40 has 900 live bytes
  // (100 garbage bytes).
  BlobGcCandidateHeap heap;
  heap.SetPhysicalFile({30, 1000, /*is_indirect_origin=*/false,
                        /*is_lost=*/false});
  heap.SetPhysicalFile({40, 1000, /*is_indirect_origin=*/false,
                        /*is_lost=*/false});
  ASSERT_OK(heap.AdjustLiveBytes(30, 400, /*count=*/4, /*add=*/true));
  ASSERT_OK(heap.AdjustLiveBytes(40, 900, /*count=*/9, /*add=*/true));
  EXPECT_EQ(heap.RelocationGarbageBytes(), 700U);
  EXPECT_EQ(heap.GetTotals().collectable_garbage_bytes, 700U);
  EXPECT_EQ(heap.Select()[0], 30U);

  // File 30 is marked lost while its references remain.
  heap.SetPhysicalFile({30, 1000, /*is_indirect_origin=*/false,
                        /*is_lost=*/true});
  EXPECT_EQ(heap.RelocationGarbageBytes(), 100U);
  EXPECT_EQ(heap.GetTotals().collectable_garbage_bytes, 100U);
  EXPECT_EQ(heap.Select()[0], 40U);
  EXPECT_EQ(heap.Select()[1], 0U);

  // Compactions remove the last references to file 30.
  ASSERT_OK(heap.AdjustLiveBytes(30, 400, /*count=*/4, /*add=*/false));
  EXPECT_EQ(heap.RelocationGarbageBytes(), 100U);
  EXPECT_EQ(heap.GetTotals().collectable_garbage_bytes, 1100U);
  EXPECT_EQ(heap.SelectEmpty()[0], 30U);

  heap.DeletePhysicalFile(30);
  EXPECT_EQ(heap.RelocationGarbageBytes(), 100U);
  EXPECT_EQ(heap.GetTotals().collectable_garbage_bytes, 100U);
}

TEST(BlobGcCandidateHeapTest, CutSelectionKeepsLeadingFilesAtRatio) {
  // The census job calls CutSelection() with the indirect_blob_gc_garbage_ratio
  // option, so that indirect blob GC gets only files with enough garbage.
  // Select() returns the files ranked by garbage ratio, best first. So the cut
  // keeps the leading files whose garbage is at least the ratio times their
  // size, and clears every file after the first one that fails. A file exactly
  // at the ratio is kept, with the same rule as the GC job check
  // (DBImpl::IndirectBlobGCJob::DecideInputs()).
  //
  // In this example, files 10 to 15 have 1000 bytes each, and 1000, 900, 500,
  // 300, 100 and 0 garbage bytes (100%, 90%, 50%, 30%, 10% and 0% garbage).
  BlobGcCandidateHeap heap;
  constexpr uint64_t kFileBytes = 1000;
  const std::array<uint64_t, 6> garbage_bytes = {1000, 900, 500, 300, 100, 0};
  for (size_t i = 0; i < garbage_bytes.size(); ++i) {
    const uint64_t file_number = 10 + i;
    heap.SetPhysicalFile({file_number, kFileBytes, false});
    if (garbage_bytes[i] != kFileBytes) {
      ASSERT_OK(heap.AdjustLiveBytes(file_number, kFileBytes - garbage_bytes[i],
                                     /*count=*/1, /*add=*/true));
    }
  }
  BlobGcCandidateHeap::SelectionBytes bytes{};
  const BlobGcCandidateHeap::Selection all = heap.Select(&bytes);
  const BlobGcCandidateHeap::Selection expected_all = {10, 11, 12, 13,
                                                       14, 15, 0,  0};
  ASSERT_EQ(all, expected_all);
  for (size_t i = 0; i < garbage_bytes.size(); ++i) {
    ASSERT_EQ(bytes[i].garbage_bytes, garbage_bytes[i]);
    ASSERT_EQ(bytes[i].total_bytes, kFileBytes);
  }

  struct CutCase {
    double ratio;
    BlobGcCandidateHeap::Selection expected;
  };
  // At 0.3, file 13 (exactly 30%) is kept. At 0.5, file 12 (exactly 50%) is
  // kept. At 1.0, only file 10 (100%) is kept.
  const CutCase cases[] = {
      {0.3, {10, 11, 12, 13, 0, 0, 0, 0}},
      {0.5, {10, 11, 12, 0, 0, 0, 0, 0}},
      {1.0, {10, 0, 0, 0, 0, 0, 0, 0}},
  };
  for (const CutCase& cut_case : cases) {
    SCOPED_TRACE("ratio=" + std::to_string(cut_case.ratio));
    BlobGcCandidateHeap::Selection selection = all;
    BlobGcCandidateHeap::CutSelection(cut_case.ratio, bytes, &selection);
    ASSERT_EQ(selection, cut_case.expected);
  }

  // The cut keeps only a leading part. In this example list, file 14 (10%)
  // comes before file 12 (50%). At 0.3, file 14 fails, so file 12 is cleared
  // too, although it passes. An empty slot (file number 0) also ends the list.
  BlobGcCandidateHeap::Selection unordered = {11, 14, 12, 0, 13, 0, 0, 0};
  BlobGcCandidateHeap::SelectionBytes unordered_bytes{};
  unordered_bytes[0] = {900, kFileBytes};
  unordered_bytes[1] = {100, kFileBytes};
  unordered_bytes[2] = {500, kFileBytes};
  unordered_bytes[4] = {300, kFileBytes};
  BlobGcCandidateHeap::CutSelection(0.3, unordered_bytes, &unordered);
  const BlobGcCandidateHeap::Selection expected_unordered = {11, 0, 0, 0,
                                                             0,  0, 0, 0};
  ASSERT_EQ(unordered, expected_unordered);
  BlobGcCandidateHeap::Selection gap = {11, 0, 13, 0, 0, 0, 0, 0};
  BlobGcCandidateHeap::SelectionBytes gap_bytes{};
  gap_bytes[0] = {900, kFileBytes};
  gap_bytes[2] = {300, kFileBytes};
  BlobGcCandidateHeap::CutSelection(0.3, gap_bytes, &gap);
  ASSERT_EQ(gap, expected_unordered);
}

TEST(BlobGcCandidateHeapTest, ExactPortableRatioOrdering) {
  const uint64_t max = std::numeric_limits<uint64_t>::max();
  ASSERT_GT(BlobGcCandidateHeap::CompareRatios(max - 1, max, max - 2, max), 0);
  ASSERT_LT(BlobGcCandidateHeap::CompareRatios(max - 2, max, max - 1, max), 0);
  ASSERT_EQ(BlobGcCandidateHeap::CompareRatios(1, 3, 2, 6), 0);
  ASSERT_GT(BlobGcCandidateHeap::CompareRatios(2, 3, 1, 2), 0);
  ASSERT_LT(BlobGcCandidateHeap::CompareRatios(1, 3, 1, 2), 0);
  ASSERT_EQ(BlobGcCandidateHeap::CompareRatios(0, max, 0, 1), 0);
  ASSERT_LT(BlobGcCandidateHeap::CompareRatios(0, max, 1, max), 0);
}

TEST(BlobReferenceCensusTest, AmbiguousMetadataCommitBlocksNextCommit) {
  // After a metadata-only commit with an unknown result
  // (EndMetadataCommit(false)), the durable state is not known. No new
  // metadata-only commit may start until ResolveMetadataCommitAmbiguity(),
  // which runs after a new MANIFEST was written. After Cancel() (column family
  // drop), BeginMetadataCommit() returns ColumnFamilyDropped.
  BlobReferenceCensus census;
  ASSERT_OK(census.BeginMetadataCommit());
  census.EndMetadataCommit(false);
  ASSERT_TRUE(census.BeginMetadataCommit().IsTryAgain());
  census.ResolveMetadataCommitAmbiguity();
  ASSERT_OK(census.BeginMetadataCommit());
  census.EndMetadataCommit(true);
  auto garbage = census.Cancel();
  ASSERT_TRUE(census.BeginMetadataCommit().IsColumnFamilyDropped());
}

TEST(BlobReferenceCensusTest, RepeatedMismatchIsTrackedPerFile) {
  BlobReferenceCensus census;
  // A first report for a file requests a rebuild.
  ASSERT_FALSE(census.ReportMismatch(10));
  // A report for another file is not a repeat: it requests a rebuild too.
  ASSERT_FALSE(census.ReportMismatch(20));
  // The same file again, with no match in between, is a repeat.
  ASSERT_TRUE(census.ReportMismatch(20));
  ASSERT_EQ(census.gc_mismatch_suppress_count(), 1U);
  // A matching indirect blob GC check clears it.
  census.ClearMismatch(20);
  ASSERT_FALSE(census.ReportMismatch(20));
}

TEST(BlobReferenceCensusTest, ClearMismatchForgetsOnlyItsOwnFile) {
  // Indirect blob GC calls ClearMismatch() for each checked input of a job when
  // no input of that job mismatched. A match of another file must keep the
  // remembered mismatch, so that a match of one file does not hide a repeat
  // mismatch of a different file. A match of the same file forgets it, so its
  // next mismatch requests a rebuild again.
  BlobReferenceCensus census;
  ASSERT_FALSE(census.ReportMismatch(20));
  // A match of file 30 keeps the mismatch of file 20: the next mismatch of
  // file 20 is a repeat.
  census.ClearMismatch(30);
  ASSERT_TRUE(census.ReportMismatch(20));
  ASSERT_EQ(census.gc_mismatch_suppress_count(), 1U);
  // A match of file 20 forgets it: the next mismatch of file 20 requests a
  // rebuild and is not counted as suppressed.
  census.ClearMismatch(20);
  ASSERT_FALSE(census.ReportMismatch(20));
  ASSERT_EQ(census.gc_mismatch_suppress_count(), 1U);
}

TEST(BlobReferenceCensusTest, MismatchReportBlocksPublish) {
  // After indirect blob GC reports a mismatch, the census is not ready, and the
  // worker of the same build cannot publish it again (for example from the
  // apply after an install) before the census job rebuilds it.
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 3, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 3, 3),
            BlobReferenceCensus::PublishResult::kPublished);
  ASSERT_TRUE(census.IsReady(3));
  ASSERT_FALSE(census.ReportMismatch(10));
  ASSERT_FALSE(census.IsReady(3));
  ASSERT_EQ(census.TryPublishWorker(token, 3, 3),
            BlobReferenceCensus::PublishResult::kStale);
  ASSERT_FALSE(census.IsReady(3));
  // The census job takes the report and fails the build. The next build can
  // publish again.
  ASSERT_TRUE(census.TakeMismatchReport());
  auto garbage = census.FailBuild(token);
  const uint64_t next_token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(next_token, 3, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(next_token));
  ASSERT_EQ(census.TryPublishWorker(next_token, 3, 3),
            BlobReferenceCensus::PublishResult::kPublished);
}

TEST(BlobGcCandidateHeapTest, OversizedFileStaysSuppressedUntilItFits) {
  // GC found that file 10 has 800 live values, and that a job can hold at most
  // 500. The file stays suppressed while its live values change but stay above
  // 500. It comes back when it has 500 live values or fewer. File 20 has a
  // normal suppression, which any change of its live bytes ends.
  BlobGcCandidateHeap heap;
  heap.SetPhysicalFile({10, 100000, false});
  heap.SetPhysicalFile({20, 100000, false});
  ASSERT_OK(heap.AdjustLiveBytes(10, 8000, 800, /*add=*/true));
  ASSERT_OK(heap.AdjustLiveBytes(20, 8000, 800, /*add=*/true));
  heap.SuppressOversized(10, /*max_live_count=*/500);
  heap.Suppress(20, /*retry_micros=*/0);
  ASSERT_EQ(heap.Select()[0], 0U);
  // A census change subtracts the old totals of a generation number, then
  // adds the new ones: 800 -> 0 -> 790 live values for file 10.
  ASSERT_OK(heap.AdjustLiveBytes(10, 8000, 800, /*add=*/false));
  ASSERT_OK(heap.AdjustLiveBytes(10, 7900, 790, /*add=*/true));
  ASSERT_OK(heap.AdjustLiveBytes(20, 100, 10, /*add=*/false));
  ASSERT_EQ(heap.Select()[0], 20U);
  ASSERT_EQ(heap.Select()[1], 0U);
  // 790 - 290 = 500 live values: file 10 fits now.
  ASSERT_OK(heap.AdjustLiveBytes(10, 2900, 290, /*add=*/false));
  ASSERT_EQ(heap.Select()[0], 10U);
  ASSERT_EQ(heap.Select()[1], 20U);
}

TEST(BlobGcCandidateHeapTest, FrontierRiseRetriesSuppressedRelocationFiles) {
  // Indirect blob GC suppressed relocation files 15, 30 and 50, for example
  // because a coarse SST pinned a generation number routed to them. When the
  // lowest coarse frontier of the Version rises from 20 to 40 (the coarse
  // frontier of an SST is the oldest indirect-origin file number a coarse SST
  // may reference), the heap retries every suppressed relocation file above the
  // old frontier (30 and 50). File 15 stays suppressed.
  BlobGcCandidateHeap heap;
  heap.SetCoarseIndirectOriginFrontier(20);
  heap.SetPhysicalFile({15, 1000, false});
  heap.SetPhysicalFile({30, 1000, false});
  heap.SetPhysicalFile({50, 1000, false});
  heap.Suppress(15, /*retry_micros=*/0);
  heap.Suppress(30, /*retry_micros=*/0);
  heap.Suppress(50, /*retry_micros=*/0);
  ASSERT_EQ(heap.Select()[0], 0U);
  heap.SetCoarseIndirectOriginFrontier(40);
  const BlobGcCandidateHeap::Selection selected = heap.Select();
  ASSERT_NE(selected[0], 0U);
  ASSERT_NE(selected[1], 0U);
  ASSERT_EQ(selected[2], 0U);
  ASSERT_NE(selected[0], 15U);
  ASSERT_NE(selected[1], 15U);
}

TEST(BlobGcCandidateHeapTest, SuppressionIsRelevantAndRetryable) {
  BlobGcCandidateHeap heap;
  heap.SetPhysicalFile({10, 1000, false});
  heap.SetPhysicalFile({20, 1000, false});
  heap.Suppress(10, /*retry_micros=*/100);
  ASSERT_EQ(heap.Select()[0], 20U);
  ASSERT_EQ(heap.RetryDue(99), 100U);
  ASSERT_EQ(heap.Select()[0], 20U);
  ASSERT_EQ(heap.RetryDue(100), 0U);
  ASSERT_EQ(heap.Select()[0], 10U);
  heap.Suppress(10, /*retry_micros=*/0);
  heap.SetPhysicalFile({20, 2000, false});
  ASSERT_EQ(heap.Select()[0], 20U);
  // File 10 is suppressed with no retry time, as after a corruption. A change
  // to another file (file 20) does not clear the suppression. A live-byte
  // change of file 10 itself does.
  ASSERT_OK(heap.AdjustLiveBytes(10, 100, 1, true));
  ASSERT_EQ(heap.Select()[1], 10U);
}

TEST(BlobReferenceCensusTest, HeapMovesLiveBytesWithChangedForwardRoutes) {
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, nullptr));
  ASSERT_OK(census.SetWorkerPhysicalFiles(token, {{10, 1000, true}}));
  BlobReferenceGroup group = MakeCensusGroup(10, 10);
  group.reference_count = 1;
  group.stored_value_bytes = 100;
  group.record_bytes = 150;
  ASSERT_OK(census.AddStartingFile(token, MakeCensusFile(100, {group})));
  ASSERT_EQ(census.SelectWorkerCandidates(token)[0], 10U);

  BlobReferenceCensusDelta delta;
  delta.base_version_number = 7;
  delta.version_number = 8;
  delta.physical_files = {{20, 1000, false}, {10, 0, true}};
  auto forward = std::make_shared<BlobRelocationState>();
  forward->SetRoute(BlobRelocationRoute(10, 20));
  forward->Seal();
  delta.forward_state = forward;
  delta.changed_routes = {10};
  ASSERT_OK(census.ApplyWorkerDelta(token, delta));
  ASSERT_EQ(census.SelectWorkerCandidates(token)[0], 20U);
  ASSERT_EQ(census.SelectWorkerCandidates(token)[1], 0U);
}

TEST(BlobReferenceCensusTest, CandidateFailureUsesRelevantRevision) {
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, nullptr));
  ASSERT_OK(census.SetWorkerPhysicalFiles(
      token, {{10, 1000, true}, {20, 1000, true}}));
  BlobGcCandidateStamp stamp;
  ASSERT_OK(census.CaptureCandidateStamp(7, {10}, &stamp));
  BlobReferenceCensusDelta delta;
  delta.base_version_number = 7;
  delta.version_number = 8;
  delta.physical_files = {{20, 2000, true}};
  ASSERT_OK(census.ApplyWorkerDelta(token, delta));
  // The delta changed only file 20, so the failure report for file 10, made
  // with the stamp of Version 7, still applies and suppresses file 10.
  census.SuppressWorkerCandidates(stamp, {10}, {}, {}, 0);
  ASSERT_EQ(census.SelectWorkerCandidates(token)[0], 20U);
  ASSERT_EQ(census.SelectWorkerCandidates(token)[1], 0U);

  ASSERT_OK(census.CaptureCandidateStamp(8, {20}, &stamp));
  delta.base_version_number = 8;
  delta.version_number = 9;
  delta.physical_files = {{20, 3000, true}};
  ASSERT_OK(census.ApplyWorkerDelta(token, delta));
  // File 20 changed after the stamp was taken, so this old failure report must
  // not suppress it, even though the file number is the same.
  census.SuppressWorkerCandidates(stamp, {20}, {}, {}, 0);
  ASSERT_EQ(census.SelectWorkerCandidates(token)[0], 20U);
  ASSERT_OK(census.CaptureCandidateStamp(9, {20}, &stamp));
  census.SuppressWorkerCandidates(stamp, {20}, {20}, {}, 100);
  ASSERT_EQ(census.SelectWorkerCandidates(token)[0], 0U);
  ASSERT_EQ(census.RetryWorkerCandidates(token, 1000099), 1000100U);
  ASSERT_EQ(census.RetryWorkerCandidates(token, 1000100), 0U);
  ASSERT_EQ(census.SelectWorkerCandidates(token)[0], 20U);
  // A second transient failure in a row doubles the retry delay, from 1 second
  // to 2 seconds. A change to the file resets the failure count. The test
  // passes the current time as an argument, so it never sleeps.
  census.SuppressWorkerCandidates(stamp, {20}, {20}, {}, 2000000);
  ASSERT_EQ(census.RetryWorkerCandidates(token, 2000001), 4000000U);
  auto canceled = census.FailBuild(token);
  const uint64_t replacement = census.BeginBuild();
  ASSERT_NE(replacement, token);
  ASSERT_OK(census.ResetWorkerState(replacement, 10, nullptr, nullptr));
  ASSERT_OK(census.SetWorkerPhysicalFiles(replacement, {{20, 1000, true}}));
  census.SuppressWorkerCandidates(stamp, {20}, {}, {}, 0);
  ASSERT_EQ(census.SelectWorkerCandidates(replacement)[0], 20U);
}

TEST(BlobReferenceCensusTest, SelectedSnapshotOwnsItsCacheReservation) {
  const std::shared_ptr<Cache> cache = NewLRUCache(16 * 1024 * 1024);
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, cache));
  ASSERT_OK(census.AddStartingFile(
      token, MakeCensusFile(100, {MakeCensusGroup(10, 10)})));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 7, 7),
            BlobReferenceCensus::PublishResult::kPublished);
  const size_t worker_charge = cache->GetPinnedUsage();
  ASSERT_GT(worker_charge, 0U);
  std::shared_ptr<BlobReferenceCensusState> selected;
  ASSERT_OK(census.ReadSelectedSnapshot(7, {10}, cache, &selected));
  ASSERT_GT(cache->GetPinnedUsage(), worker_charge);
  selected.reset();
  ASSERT_EQ(cache->GetPinnedUsage(), worker_charge);
}

TEST(BlobReferenceCensusTest, SelectedSnapshotGoesOnUnchargedWhenCacheIsFull) {
  // Route retirement's copy (ReadSelectedSnapshot()) does not fail when the
  // block cache is full: the copy stays uncharged and the census counts the
  // failed charge.
  const std::shared_ptr<Cache> cache =
      NewLRUCache(2 * 1024 * 1024, /*num_shard_bits=*/0,
                  /*strict_capacity_limit=*/true);
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, cache));
  ASSERT_OK(census.AddStartingFile(
      token, MakeCensusFile(100, {MakeCensusGroup(10, 10)})));
  cache->SetCapacity(cache->GetUsage());
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 7, 7),
            BlobReferenceCensus::PublishResult::kPublished);
  std::shared_ptr<BlobReferenceCensusState> selected;
  const uint64_t uncharged_before = census.uncharged_charge_count();
  ASSERT_OK(census.ReadSelectedSnapshot(7, {10}, cache, &selected));
  ASSERT_NE(selected, nullptr);
  ASSERT_GT(census.uncharged_charge_count(), uncharged_before);
  ASSERT_TRUE(census.IsReady(7));
}

TEST(BlobReferenceCensusTest, LogSharesCacheChargeAndReleasesOnAck) {
  const std::shared_ptr<Cache> cache = NewLRUCache(16 * 1024 * 1024);
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  size_t first_charge = 0;
  BlobReferenceCensusPreparedInstall prepared;
  for (uint64_t version = 1; version <= 4; ++version) {
    auto delta = std::make_shared<BlobReferenceCensusDelta>();
    delta->base_version_number = version;
    delta->version_number = version + 1;
    delta->retained_forward_state_bytes = 32 * 1024;
    const uint64_t reservation = census.BeginInstallPreparation();
    ASSERT_OK(
        census.PrepareInstallWithCache(delta, reservation, cache, &prepared));
    auto garbage = census.Install(prepared);
    if (version == 1) {
      first_charge = cache->GetPinnedUsage();
      ASSERT_GT(first_charge, 0U);
    } else {
      ASSERT_EQ(cache->GetPinnedUsage(), first_charge);
    }
  }
  prepared = {};
  BlobReferenceCensusDeltaQueue deltas;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_FALSE(overflowed);
  for (const std::shared_ptr<const BlobReferenceCensusDelta>& delta : deltas) {
    ASSERT_OK(census.AcknowledgeBuildDeltas(token, delta->QueueWeight()));
  }
  ASSERT_EQ(cache->GetPinnedUsage(), 0U);
}

TEST(BlobReferenceCensusTest, GcJobStaysRegisteredAcrossInvalidation) {
  // An indirect blob GC job reads no deltas after its snapshot, so an install
  // that invalidates the census does not end it. The job still holds the
  // registration (one GC job per census) until EndGcJob(). A new job cannot
  // start until the census is ready again.
  BlobReferenceCensus census;
  const uint64_t build_token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(build_token, 3, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(build_token));
  ASSERT_EQ(census.TryPublishWorker(build_token, 3, 3),
            BlobReferenceCensus::PublishResult::kPublished);

  uint64_t gc_token = 0;
  ASSERT_OK(census.BeginGcJob(3, &gc_token));
  ASSERT_TRUE(census.IsSnapshotPending());
  BlobReferenceCensusPreparedInstall prepared;
  const uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInvalidation(reservation, &prepared));
  auto garbage = census.Install(prepared);
  garbage = {};
  ASSERT_TRUE(census.IsGcJobActive(gc_token));
  ASSERT_TRUE(census.IsSnapshotPending());
  census.EndGcJob(gc_token);
  ASSERT_FALSE(census.IsGcJobActive(gc_token));
  ASSERT_FALSE(census.IsSnapshotPending());
  ASSERT_TRUE(census.BeginGcJob(3, &gc_token).IsTryAgain());
}

TEST(BlobReferenceCensusTest, GcJobPausesWorkerUntilSnapshot) {
  // While a registered indirect blob GC job has not copied its snapshot, the
  // worker applies no delta: ApplyWorkerDelta() returns TryAgain and changes
  // nothing, so the copy shows the census at the job's input Version. After
  // SnapshotSelected() the worker applies deltas again.
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 3, nullptr, nullptr));
  ASSERT_OK(census.AddStartingFile(
      token, MakeCensusFile(100, {MakeCensusGroup(10, 10)})));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 3, 3),
            BlobReferenceCensus::PublishResult::kPublished);

  uint64_t gc_token = 0;
  ASSERT_OK(census.BeginGcJob(3, &gc_token));
  ASSERT_TRUE(census.IsSnapshotPending());
  BlobReferenceCensusDelta delta;
  delta.base_version_number = 3;
  delta.version_number = 4;
  delta.added_files = {MakeCensusFile(200, {MakeCensusGroup(10, 10)})};
  ASSERT_TRUE(census.ApplyWorkerDelta(token, delta).IsTryAgain());
  ASSERT_EQ(census.WorkerVersionNumber(), 3U);

  std::shared_ptr<BlobReferenceCensusState> selected;
  ASSERT_OK(census.SnapshotSelected(gc_token, 3, {10},
                                    /*gc_block_cache=*/nullptr,
                                    /*empty_files_only=*/false,
                                    /*copied_uncharged=*/nullptr, &selected));
  ASSERT_EQ(selected->GetSstCount(10), 1U);
  ASSERT_FALSE(census.IsSnapshotPending());
  ASSERT_OK(census.ApplyWorkerDelta(token, delta));
  ASSERT_EQ(census.WorkerVersionNumber(), 4U);
  census.EndGcJob(gc_token);
  auto garbage = census.FailBuild(token);
}

TEST(BlobReferenceCensusTest, SharedLogHasBoundedReadsAndReclaimsOnDrain) {
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  std::weak_ptr<BlobReferenceChangeLogEntry> first_entry;
  BlobReferenceCensusPreparedInstall prepared;
  for (uint64_t version = 1; version <= 130; ++version) {
    auto delta = std::make_shared<BlobReferenceCensusDelta>();
    delta->base_version_number = version;
    delta->version_number = version + 1;
    const uint64_t reservation = census.BeginInstallPreparation();
    ASSERT_OK(census.PrepareInstall(delta, reservation, &prepared));
    if (version == 1) {
      first_entry = prepared.log_entry;
    }
    auto garbage = census.Install(prepared);
  }
  prepared = {};
  ASSERT_FALSE(first_entry.expired());
  size_t total = 0;
  for (;;) {
    BlobReferenceCensusDeltaQueue deltas;
    bool overflowed = false;
    ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
    ASSERT_FALSE(overflowed);
    ASSERT_LE(deltas.size(), BlobReferenceCensus::kMaxDeltasPerRead);
    if (deltas.empty()) {
      break;
    }
    for (const std::shared_ptr<const BlobReferenceCensusDelta>& delta :
         deltas) {
      ASSERT_OK(census.AcknowledgeBuildDeltas(token, delta->QueueWeight()));
      ++total;
    }
  }
  ASSERT_EQ(total, 130U);
  ASSERT_TRUE(first_entry.expired());
  auto garbage = census.FailBuild(token);
}

namespace {
// The tests below that need a full change log lower its limit
// (BlobReferenceCensus::kMaxLogMemory, 256 MB) to kTestLogLimit (1 MB, an
// example size) through the sync point
// "BlobReferenceCensus::AppendChange:Limit", so an oversized delta needs only
// about 1 MB of memory.
constexpr size_t kTestLogLimit = size_t{1} << 20;

class ScopedCensusLogLimit {
 public:
  ScopedCensusLogLimit(const ScopedCensusLogLimit&) = delete;
  ScopedCensusLogLimit& operator=(const ScopedCensusLogLimit&) = delete;
  ScopedCensusLogLimit(ScopedCensusLogLimit&&) = delete;
  ScopedCensusLogLimit& operator=(ScopedCensusLogLimit&&) = delete;
  ScopedCensusLogLimit() {
    SyncPoint::GetInstance()->SetCallBack(
        "BlobReferenceCensus::AppendChange:Limit",
        [](void* arg) { *static_cast<size_t*>(arg) = kTestLogLimit; });
    SyncPoint::GetInstance()->EnableProcessing();
  }
  ~ScopedCensusLogLimit() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
};
}  // namespace

TEST(BlobReferenceCensusTest, SingleOversizedDeltaDoesNotBlockProgress) {
  ScopedCensusLogLimit log_limit;
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  std::vector<BlobReferenceGroup> groups(
      kTestLogLimit / sizeof(BlobReferenceGroup) + 1, MakeCensusGroup(5, 5));
  auto delta = std::make_shared<BlobReferenceCensusDelta>();
  delta->base_version_number = 3;
  delta->version_number = 4;
  delta->added_files = {MakeCensusFile(30, std::move(groups))};
  BlobReferenceCensusPreparedInstall prepared;
  const uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInstall(delta, reservation, &prepared));
  auto install_garbage = census.Install(prepared);

  BlobReferenceCensusDeltaQueue deltas;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_FALSE(overflowed);
  ASSERT_EQ(deltas.size(), 1U);
  // A delta larger than the limit is kept because the cursor's queued weight
  // was below the limit (0 here), so one large install cannot block progress.
  // The queued weight is now above the limit, so the next delta makes this
  // lagging cursor overflow: the census drops the cursor instead of keeping
  // more data.
  auto next = std::make_shared<BlobReferenceCensusDelta>();
  next->base_version_number = 4;
  next->version_number = 5;
  const uint64_t next_reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInstall(next, next_reservation, &prepared));
  install_garbage = census.Install(prepared);
  deltas.clear();
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_TRUE(overflowed);
  ASSERT_TRUE(deltas.empty());
  auto context = std::make_shared<BlobReferenceCensusBuildContext>();
  context->token = token;
  ASSERT_TRUE(census.TrySetBuildContext(context));
  auto garbage = census.FailBuild(token);
  ASSERT_EQ(garbage.build_context, context);
  ASSERT_EQ(census.IsReady(4), false);
}

namespace {
// Installs a delta from `version` to `version + 1`. With `oversized`, its
// QueueWeight() is larger than kTestLogLimit; otherwise the delta is empty and
// its weight is small. Callers lower the limit with ScopedCensusLogLimit.
void InstallCensusDelta(BlobReferenceCensus* census, uint64_t version,
                        bool oversized) {
  auto delta = std::make_shared<BlobReferenceCensusDelta>();
  delta->base_version_number = version;
  delta->version_number = version + 1;
  if (oversized) {
    delta->added_files = {MakeCensusFile(
        100 + version, std::vector<BlobReferenceGroup>(
                           kTestLogLimit / sizeof(BlobReferenceGroup) + 1,
                           MakeCensusGroup(5, 5)))};
  }
  BlobReferenceCensusPreparedInstall prepared;
  const uint64_t reservation = census->BeginInstallPreparation();
  ASSERT_OK(census->PrepareInstall(delta, reservation, &prepared));
  auto garbage = census->Install(prepared);
}

// Returns a summary of SST `file_number` with `group_count` groups. With a
// non-null `charge_cache`, the summary has its own cache charge on that cache
// (a reservation manager and a reservation handle for its MemoryUsage()), as
// the summaries that table builders and compaction input reads return do.
std::shared_ptr<const BlobReferenceSstSummary> MakeCensusFileWithGroups(
    uint64_t file_number, size_t group_count,
    const std::shared_ptr<Cache>& charge_cache) {
  auto file = std::make_shared<BlobReferenceSstSummary>();
  file->file_number = file_number;
  file->groups.assign(group_count, MakeCensusGroup(5, 5));
  if (charge_cache != nullptr) {
    file->reservation_manager =
        std::make_shared<ConcurrentCacheReservationManager>(
            std::make_shared<CacheReservationManagerImpl<
                CacheEntryRole::kBlobReferenceCensus>>(charge_cache));
    EXPECT_OK(file->reservation_manager->MakeCacheReservation(
        file->MemoryUsage(), &file->reservation_handle));
  }
  return file;
}
}  // namespace

TEST(BlobReferenceCensusTest, OverflowRestartUsesRestartLimit) {
  // After an overflow restart, each build until the next publish has the
  // restart limit: 4 x the normal limit (kMaxRestartLogMemory / kMaxLogMemory;
  // the test lowers the normal limit to kTestLogLimit). The limit is checked
  // before each append, so the cursor keeps four deltas that are each a bit
  // larger than kTestLogLimit, and the fifth overflows it. A build that
  // overflows the restart limit fails; the next build still has the restart
  // limit.
  ScopedCensusLogLimit log_limit;
  BlobReferenceCensus census;
  uint64_t token = census.BeginBuild();
  // The first overflow restarts the build; a second one before any publish
  // does not restart it again.
  ASSERT_TRUE(census.UseOverflowRestart());
  ASSERT_FALSE(census.UseOverflowRestart());
  ASSERT_EQ(census.overflow_restart_count(), 1U);
  auto garbage = census.FailBuild(token);
  token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 3, nullptr, nullptr));
  for (uint64_t version = 3; version < 7; ++version) {
    std::vector<BlobReferenceGroup> groups(
        kTestLogLimit / sizeof(BlobReferenceGroup) + 1, MakeCensusGroup(5, 5));
    auto delta = std::make_shared<BlobReferenceCensusDelta>();
    delta->base_version_number = version;
    delta->version_number = version + 1;
    delta->added_files = {MakeCensusFile(30 + version, std::move(groups))};
    BlobReferenceCensusPreparedInstall prepared;
    const uint64_t reservation = census.BeginInstallPreparation();
    ASSERT_OK(census.PrepareInstall(delta, reservation, &prepared));
    auto install_garbage = census.Install(prepared);
  }
  BlobReferenceCensusDeltaQueue deltas;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_FALSE(overflowed);
  ASSERT_EQ(deltas.size(), 4U);
  deltas.clear();
  // The cursor now holds more than 4 x kTestLogLimit, so the next delta
  // overflows it.
  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 7, /*oversized=*/false));
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_TRUE(overflowed);
  garbage = census.FailBuild(token);
  // The next build (after the backoff of the census job) still has the restart
  // limit: two oversized deltas are kept.
  token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 8, nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 8, /*oversized=*/true));
  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 9, /*oversized=*/true));
  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 10, /*oversized=*/false));
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_FALSE(overflowed);
  ASSERT_EQ(deltas.size(), 3U);
  deltas.clear();
  garbage = census.FailBuild(token);
  // A later build publishes, which resets the one-restart limit.
  token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 5, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 5, 5),
            BlobReferenceCensus::PublishResult::kPublished);
  // After a publish, a later overflow may restart again.
  ASSERT_TRUE(census.UseOverflowRestart());
  ASSERT_EQ(census.overflow_restart_count(), 2U);
}

TEST(BlobReferenceCensusTest, OversizedDeltaBehindSmallDeltaDoesNotOverflow) {
  // The limit is checked before each append. A small queued delta (a flush)
  // does not make a following large delta (a compaction that rewrites a
  // level) overflow the cursor. Once the queued weight has reached the limit,
  // the next delta overflows it.
  ScopedCensusLogLimit log_limit;
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 1, /*oversized=*/false));
  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 2, /*oversized=*/true));
  BlobReferenceCensusDeltaQueue deltas;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_FALSE(overflowed);
  ASSERT_EQ(deltas.size(), 2U);

  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 3, /*oversized=*/false));
  deltas.clear();
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_TRUE(overflowed);
  auto garbage = census.FailBuild(token);
}

// Summaries made with kCensusTestGroupCount have 128 groups each (about 6 KB,
// an example size), so 200 of them hold about 1.2 MB.
constexpr size_t kCensusTestGroupCount = 128;

TEST(BlobReferenceCensusTest, ChargedSummariesCountInQueueWeightNotInCharge) {
  // A compaction delta that adds and removes 100 SSTs. Each summary has its own
  // cache charge. QueueWeight() counts all summaries, so the log limit covers
  // them. ChargeBytes() counts only the delta's own memory, so the change log
  // does not charge the summaries a second time. The same delta with
  // uncharged summaries counts them in both.
  std::shared_ptr<Cache> charge_cache = NewLRUCache(64 << 20);
  BlobReferenceCensusDelta charged;
  BlobReferenceCensusDelta uncharged;
  size_t summary_bytes = 0;
  for (uint64_t i = 0; i < 100; ++i) {
    charged.added_files.push_back(MakeCensusFileWithGroups(
        1000 + i, kCensusTestGroupCount, charge_cache));
    charged.removed_summaries.push_back(MakeCensusFileWithGroups(
        2000 + i, kCensusTestGroupCount, charge_cache));
    uncharged.added_files.push_back(
        MakeCensusFileWithGroups(1000 + i, kCensusTestGroupCount, nullptr));
    uncharged.removed_summaries.push_back(
        MakeCensusFileWithGroups(2000 + i, kCensusTestGroupCount, nullptr));
    summary_bytes += 2 * charged.added_files.back()->MemoryUsage();
  }
  ASSERT_GT(charged.QueueWeight(), summary_bytes);
  ASSERT_LT(charged.ChargeBytes(), size_t{64} << 10);
  ASSERT_GT(uncharged.QueueWeight(), summary_bytes);
  ASSERT_EQ(uncharged.ChargeBytes(), uncharged.QueueWeight());
}

TEST(BlobReferenceCensusTest, LogChargeLeavesOutChargedSummaries) {
  // PrepareInstallWithCache() charges the delta's ChargeBytes() to the block
  // cache of the change log. A delta with one charged and one uncharged summary
  // (1 MB each, example sizes) charges only the uncharged one, because the
  // charged one already has its own charge (on another cache here). The
  // reservation manager charges in steps of its dummy entry size (256 KB), so
  // the test checks a range.
  std::shared_ptr<Cache> log_cache = NewLRUCache(64 << 20);
  std::shared_ptr<Cache> summary_cache = NewLRUCache(64 << 20);
  constexpr size_t kGroups = (size_t{1} << 20) / sizeof(BlobReferenceGroup);
  auto charged = MakeCensusFileWithGroups(10, kGroups, summary_cache);
  auto uncharged = MakeCensusFileWithGroups(11, kGroups, nullptr);
  auto delta = std::make_shared<BlobReferenceCensusDelta>();
  delta->base_version_number = 1;
  delta->version_number = 2;
  delta->added_files = {charged, uncharged};
  BlobReferenceCensus census;
  BlobReferenceCensusPreparedInstall prepared;
  const uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(
      census.PrepareInstallWithCache(delta, reservation, log_cache, &prepared));
  const size_t log_charge = log_cache->GetUsage();
  EXPECT_GE(log_charge, uncharged->MemoryUsage());
  EXPECT_LT(log_charge, uncharged->MemoryUsage() + charged->MemoryUsage());
  auto garbage = census.Install(prepared);
}

TEST(BlobReferenceCensusTest, ChargedSummariesCountTowardLogLimit) {
  // A compaction delta whose summaries all have their own cache charge (about
  // 1.2 MB) is followed by a small flush delta before the census reads either.
  // The test lowers the cursor limit to 256 KB (sync point
  // "BlobReferenceCensus::AppendChange:Limit"). QueueWeight() counts the
  // charged summaries, so the compaction delta reaches the limit and the flush
  // delta overflows the build cursor. The test also counts the sync point
  // calls, so it fails if the sync point is renamed.
  int limit_calls = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobReferenceCensus::AppendChange:Limit", [&limit_calls](void* arg) {
        ++limit_calls;
        *static_cast<size_t*>(arg) = size_t{256} << 10;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  std::shared_ptr<Cache> charge_cache = NewLRUCache(64 << 20);
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();
  auto compaction = std::make_shared<BlobReferenceCensusDelta>();
  compaction->base_version_number = 1;
  compaction->version_number = 2;
  size_t summary_bytes = 0;
  for (uint64_t i = 0; i < 100; ++i) {
    compaction->added_files.push_back(MakeCensusFileWithGroups(
        1000 + i, kCensusTestGroupCount, charge_cache));
    compaction->removed_summaries.push_back(MakeCensusFileWithGroups(
        2000 + i, kCensusTestGroupCount, charge_cache));
    summary_bytes += 2 * compaction->added_files.back()->MemoryUsage();
  }
  ASSERT_GT(summary_bytes, size_t{256} << 10);
  ASSERT_GT(compaction->QueueWeight(), summary_bytes);
  auto flush = std::make_shared<BlobReferenceCensusDelta>();
  flush->base_version_number = 2;
  flush->version_number = 3;
  flush->added_files = {MakeCensusFile(3000, {})};
  for (const auto& delta : {compaction, flush}) {
    BlobReferenceCensusPreparedInstall prepared;
    const uint64_t reservation = census.BeginInstallPreparation();
    ASSERT_OK(census.PrepareInstall(delta, reservation, &prepared));
    auto garbage = census.Install(prepared);
  }
  BlobReferenceCensusDeltaQueue deltas;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_TRUE(overflowed);
  ASSERT_TRUE(deltas.empty());
  ASSERT_GT(limit_calls, 0);
  auto garbage = census.FailBuild(token);
}

TEST(BlobReferenceCensusTest, PublishEndsRestartLimit) {
  // After an overflow restart, the build has the restart limit only until it
  // publishes. A later delta that arrives when the queued weight has reached
  // the normal limit overflows the cursor again.
  ScopedCensusLogLimit log_limit;
  BlobReferenceCensus census;
  uint64_t token = census.BeginBuild();
  ASSERT_TRUE(census.UseOverflowRestart());
  auto garbage = census.FailBuild(token);
  token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(token, 3, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(token));
  ASSERT_EQ(census.TryPublishWorker(token, 3, 3),
            BlobReferenceCensus::PublishResult::kPublished);

  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 3, /*oversized=*/true));
  ASSERT_NO_FATAL_FAILURE(InstallCensusDelta(&census, 4, /*oversized=*/true));
  BlobReferenceCensusDeltaQueue deltas;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &deltas, &overflowed));
  ASSERT_TRUE(overflowed);
  garbage = census.FailBuild(token);
}

TEST(BlobReferenceCensusTest, RestartedMismatchRebuildKeepsMismatchState) {
  // A mismatch rebuild can fail and restart before it publishes: after a
  // change log overflow, a rebuild install or another failure. The census sees
  // each restart as FailBuild() and BeginBuild(), so a rebuild install
  // restarts the build in the same way as another failure. The restarted build
  // is still the rebuild that the report asked for, so its publish must keep
  // the remembered files and the count of mismatch rebuilds in a row. Each
  // case of the loop uses a different restart; kNone has no restart.
  enum class Restart { kNone, kOverflow, kOtherFailure };
  for (Restart restart :
       {Restart::kNone, Restart::kOverflow, Restart::kOtherFailure}) {
    SCOPED_TRACE("restart=" + std::to_string(static_cast<int>(restart)));
    BlobReferenceCensus census;
    uint64_t token = census.BeginBuild();
    ASSERT_OK(census.ResetWorkerState(token, 7, nullptr, nullptr));
    ASSERT_OK(census.FinalizeWorkerBuild(token));
    ASSERT_EQ(census.TryPublishWorker(token, 7, 7),
              BlobReferenceCensus::PublishResult::kPublished);
    // GC reports file 100 for the first time. The census job takes the report
    // and fails the build, and the mismatch rebuild starts.
    ASSERT_FALSE(census.ReportMismatch(100));
    ASSERT_TRUE(census.TakeMismatchReport());
    auto garbage = census.FailBuild(token);
    token = census.BeginBuild();
    if (restart == Restart::kOverflow) {
      ASSERT_TRUE(census.UseOverflowRestart());
    }
    if (restart != Restart::kNone) {
      garbage = census.FailBuild(token);
      token = census.BeginBuild();
    }
    ASSERT_OK(census.ResetWorkerState(token, 8, nullptr, nullptr));
    ASSERT_OK(census.FinalizeWorkerBuild(token));
    ASSERT_EQ(census.TryPublishWorker(token, 8, 8),
              BlobReferenceCensus::PublishResult::kPublished);
    ASSERT_EQ(census.unresolved_mismatch_file_count(), 1U);
    ASSERT_EQ(census.mismatch_rebuilds_in_a_row(), 1U);
    // The next report of file 100 is a repeat, so GC suppresses the file
    // instead of asking for one more rebuild.
    ASSERT_TRUE(census.ReportMismatch(100));
    // A later build that no report requested (here, a retry after a failure)
    // starts the mismatch state again at its first publish.
    garbage = census.FailBuild(token);
    token = census.BeginBuild();
    ASSERT_OK(census.ResetWorkerState(token, 9, nullptr, nullptr));
    ASSERT_OK(census.FinalizeWorkerBuild(token));
    ASSERT_EQ(census.TryPublishWorker(token, 9, 9),
              BlobReferenceCensus::PublishResult::kPublished);
    ASSERT_EQ(census.unresolved_mismatch_file_count(), 0U);
    ASSERT_EQ(census.mismatch_rebuilds_in_a_row(), 0U);
    garbage = census.FailBuild(token);
  }
}

TEST(BlobReferenceCensusTest, MismatchWaitStepOnlyAfterMismatchFailure) {
  // After more than two mismatch rebuilds in a row, only a build that failed
  // because of a mismatch report waits. A restart for another reason (for
  // example a rebuild install from DeleteFilesInRange()) starts at once.
  BlobReferenceCensus census;
  uint64_t token = census.BeginBuild();
  // Three first reports of files 100, 101 and 102, each with a mismatch
  // rebuild.
  for (uint64_t file : {100, 101, 102}) {
    ASSERT_FALSE(census.ReportMismatch(file));
    ASSERT_TRUE(census.TakeMismatchReport());
    auto garbage = census.FailBuild(token);
    token = census.BeginBuild();
  }
  ASSERT_TRUE(census.MismatchRebuildMustWait());
  // The running build fails with no pending report: no wait step.
  ASSERT_FALSE(census.TakeMismatchReport());
  ASSERT_EQ(census.TakeMismatchRebuildWaitStep(), 0U);
  auto garbage = census.FailBuild(token);
  token = census.BeginBuild();
  // A fourth report fails the build: it waits, and this is the first wait.
  ASSERT_FALSE(census.ReportMismatch(103));
  ASSERT_TRUE(census.TakeMismatchReport());
  ASSERT_EQ(census.TakeMismatchRebuildWaitStep(), 1U);
  garbage = census.FailBuild(token);
}

TEST(BlobReferenceCensusTest, DetachedDeltasRemainChargedUntilApplied) {
  ScopedCensusLogLimit log_limit;
  BlobReferenceCensus census;
  const uint64_t token = census.BeginBuild();

  auto first = std::make_shared<BlobReferenceCensusDelta>();
  first->base_version_number = 1;
  first->version_number = 2;
  // The first delta alone reaches the limit, so the cursor overflows at the
  // next delta unless the first one is acknowledged.
  first->added_files = {
      MakeCensusFile(10, std::vector<BlobReferenceGroup>(
                             kTestLogLimit / sizeof(BlobReferenceGroup) + 1,
                             MakeCensusGroup(5, 5)))};
  BlobReferenceCensusPreparedInstall prepared;
  uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInstall(first, reservation, &prepared));
  auto garbage = census.Install(prepared);

  BlobReferenceCensusDeltaQueue detached;
  bool overflowed = false;
  ASSERT_OK(census.TakeBuildDeltas(token, &detached, &overflowed));
  ASSERT_FALSE(overflowed);
  ASSERT_EQ(detached.size(), 1);

  auto second = std::make_shared<BlobReferenceCensusDelta>();
  second->base_version_number = 2;
  second->version_number = 3;
  second->added_files = {
      MakeCensusFile(11, std::vector<BlobReferenceGroup>(
                             kTestLogLimit / (2 * sizeof(BlobReferenceGroup)),
                             MakeCensusGroup(6, 6)))};
  reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInstall(second, reservation, &prepared));
  garbage = census.Install(prepared);

  BlobReferenceCensusDeltaQueue later;
  ASSERT_OK(census.TakeBuildDeltas(token, &later, &overflowed));
  ASSERT_TRUE(overflowed);
  ASSERT_TRUE(later.empty());
  garbage = census.FailBuild(token);
}

TEST(BlobReferenceCensusTest, CancelDetachesReadyStateAndQueuedDeltas) {
  BlobReferenceCensus census;
  std::shared_ptr<BlobReferenceCensusState> state;
  ASSERT_OK(BlobReferenceCensus::BuildState(1, {}, &state));
  const uint64_t catch_up_token = census.BeginBuild();
  ASSERT_OK(census.ResetWorkerState(catch_up_token, 1, nullptr, nullptr));
  ASSERT_OK(census.FinalizeWorkerBuild(catch_up_token));
  ASSERT_EQ(census.TryPublishWorker(catch_up_token, 1, 1),
            BlobReferenceCensus::PublishResult::kPublished);
  auto delta = std::make_shared<BlobReferenceCensusDelta>();
  delta->base_version_number = 1;
  delta->version_number = 2;
  BlobReferenceCensusPreparedInstall prepared;
  const uint64_t reservation = census.BeginInstallPreparation();
  ASSERT_OK(census.PrepareInstall(delta, reservation, &prepared));
  auto install_garbage = census.Install(prepared);

  auto canceled = census.Cancel();
  ASSERT_NE(canceled.log_entries[0], nullptr);
  ASSERT_EQ(canceled.log_entries[0]->delta, delta);
  ASSERT_EQ(census.IsReady(2), false);
  ASSERT_EQ(census.TryPublishWorker(catch_up_token, 1, 2),
            BlobReferenceCensus::PublishResult::kStale);
}

// kTypeBlobIndex is a value type used by BlobDB only. The base rocksdb
// should accept the value type on write, and report not supported value
// for reads, unless caller request for it explicitly. The base rocksdb
// doesn't understand format of actual blob index (the value).
class DBBlobIndexTest : public DBTestBase {
 public:
  enum Tier {
    kMemtable = 0,
    kImmutableMemtables = 1,
    kL0SstFile = 2,
    kLnSstFile = 3,
  };
  const std::vector<Tier> kAllTiers = {Tier::kMemtable,
                                       Tier::kImmutableMemtables,
                                       Tier::kL0SstFile, Tier::kLnSstFile};

  DBBlobIndexTest() : DBTestBase("db_blob_index_test", /*env_do_fsync=*/true) {}

  ColumnFamilyHandle* cfh() { return dbfull()->DefaultColumnFamily(); }
  ColumnFamilyHandleImpl* cfh_impl() {
    return static_cast_with_check<ColumnFamilyHandleImpl>(cfh());
  }
  ColumnFamilyData* cfd() { return cfh_impl()->cfd(); }

  Status PutBlobIndex(WriteBatch* batch, const Slice& key,
                      const Slice& blob_index) {
    return WriteBatchInternal::PutBlobIndex(batch, cfd()->GetID(), key,
                                            blob_index);
  }

  Status Write(WriteBatch* batch) {
    return dbfull()->Write(WriteOptions(), batch);
  }

  std::string GetImpl(const Slice& key, bool* is_blob_index = nullptr,
                      const Snapshot* snapshot = nullptr) {
    ReadOptions read_options;
    read_options.snapshot = snapshot;
    PinnableSlice value;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = cfh();
    get_impl_options.value = &value;
    get_impl_options.is_blob_index = is_blob_index;
    auto s = dbfull()->GetImpl(read_options, key, get_impl_options);
    if (s.IsNotFound()) {
      return "NOT_FOUND";
    }
    if (s.IsCorruption()) {
      return "CORRUPTION";
    }
    if (s.IsNotSupported()) {
      return "NOT_SUPPORTED";
    }
    if (!s.ok()) {
      return s.ToString();
    }
    return value.ToString();
  }

  std::string GetBlobIndex(const Slice& key,
                           const Snapshot* snapshot = nullptr) {
    bool is_blob_index = false;
    std::string value = GetImpl(key, &is_blob_index, snapshot);
    if (!is_blob_index) {
      return "NOT_BLOB";
    }
    return value;
  }

  ArenaWrappedDBIter* GetBlobIterator() {
    DBImpl* db_impl = dbfull();
    return db_impl->NewIteratorImpl(
        ReadOptions(), cfh_impl(), cfd()->GetReferencedSuperVersion(db_impl),
        db_impl->GetLatestSequenceNumber(), nullptr /*read_callback*/,
        true /*expose_blob_index*/);
  }

  bool MaybeResolveDirectWriteValueForTest(
      const ReadOptions& read_options, const Slice& key,
      bool resolve_direct_write_value, const Version* current,
      PinnableSlice* value, PinnableWideColumns* columns, Status* s,
      bool* is_blob_index, bool* value_found = nullptr) {
    VersionBlobFetcher blob_fetcher(current, read_options,
                                    cfd()->blob_file_cache(),
                                    /*allow_write_path_fallback=*/true);
    return DBImpl::MaybeResolveDirectWriteValue(
        read_options, key, resolve_direct_write_value, blob_fetcher, value,
        columns, s, is_blob_index, value_found);
  }

  Status MaybeResolveMemtableBlobValueForTest(
      const Slice& key, const BlobFetcher* blob_fetcher, PinnableSlice* value,
      PinnableWideColumns* columns, bool* did_resolve, bool* is_blob_index,
      bool* value_found = nullptr) {
    return DBImpl::MaybeResolveMemtableBlobValue(key, blob_fetcher, value,
                                                 columns, did_resolve,
                                                 is_blob_index, value_found);
  }

  Status PostprocessMemtableValueReadForTest(
      const Slice& key, bool resolve_blob_backed_memtable_value,
      const BlobFetcher* blob_fetcher, PinnableSlice* value,
      PinnableWideColumns* columns, Status memtable_read_status,
      bool* is_blob_index, bool* value_found = nullptr) {
    return DBImpl::PostprocessMemtableValueRead(
        key, /*timestamp=*/nullptr, resolve_blob_backed_memtable_value,
        blob_fetcher, value, columns, std::move(memtable_read_status),
        is_blob_index, value_found);
  }

  Options GetTestOptions() {
    Options options;
    options.env = CurrentOptions().env;
    options.create_if_missing = true;
    options.num_levels = 2;
    options.disable_auto_compactions = true;
    // Disable auto flushes.
    options.max_write_buffer_number = 10;
    options.min_write_buffer_number_to_merge = 10;
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    return options;
  }

  Options GetBlobTestOptions() {
    return wide_column_test_util::GetOptionsForBlobTest(GetTestOptions());
  }

  void MoveDataTo(Tier tier) {
    switch (tier) {
      case Tier::kMemtable:
        break;
      case Tier::kImmutableMemtables:
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
        break;
      case Tier::kL0SstFile:
        ASSERT_OK(Flush());
        break;
      case Tier::kLnSstFile:
        ASSERT_OK(Flush());
        ASSERT_OK(Put("a", "dummy"));
        ASSERT_OK(Put("z", "dummy"));
        ASSERT_OK(Flush());
        ASSERT_OK(
            dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
        ASSERT_EQ("0,1", FilesPerLevel());
        break;
    }
  }
};

TEST_F(DBBlobIndexTest, EmptyBlobIndexFailsFlushWithCorruption) {
  Options options = GetTestOptions();
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  WriteBatch batch;
  ASSERT_OK(PutBlobIndex(&batch, "key", Slice()));
  ASSERT_OK(Write(&batch));

  const Status status = Flush();
  ASSERT_TRUE(status.IsCorruption());
  ASSERT_NE(status.ToString().find("Empty blob index"), std::string::npos);
}

TEST_F(DBBlobIndexTest, SstFileWriterOutputStaysLegacy) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // SstFileWriter output has no DB file number, and without a CF handle it has
  // no CF ID. So the table builder writes no exact reference metadata for it,
  // with or without a CF handle.
  const auto write_external = [&](ColumnFamilyHandle* handle,
                                  const std::string& key) {
    const std::string path = dbname_ + "_" + key + ".sst";
    SstFileWriter writer(EnvOptions(), options, handle);
    EXPECT_OK(writer.Open(path));
    EXPECT_OK(writer.Put(key, "value"));
    EXPECT_OK(writer.Finish());
    return path;
  };
  ASSERT_OK(
      db_->IngestExternalFile({write_external(/*handle=*/nullptr, "no_handle")},
                              IngestExternalFileOptions()));
  ASSERT_OK(db_->IngestExternalFile(
      {write_external(db_->DefaultColumnFamily(), "with_handle")},
      IngestExternalFileOptions()));

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 2U);
  for (const auto& table : properties) {
    EXPECT_EQ(table.second->user_collected_properties.count(
                  kBlobReferenceDescriptorPropertyName),
              0U);
  }

  // RepairDB sets aside an SST that has no CF name. The SST written with a CF
  // handle must survive repair.
  Close();
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("with_handle"), "value");
}

TEST_F(DBBlobIndexTest, PrepopulateSkipsReferenceBlocks) {
  // With prepopulate_block_cache, flush and compaction warm the blocks they
  // write into the block cache. No reader looks up the blob reference blocks
  // (BlockType::kUserDefinedIndex) in the block cache, so they are not warmed.
  // The test counts the blocks that the table builder warms, by block type.
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(8 << 20);
  table_options.prepopulate_block_cache =
      BlockBasedTableOptions::PrepopulateBlockCache::kFlushAndCompaction;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  std::atomic<int> warmed_data_blocks{0};
  std::atomic<int> warmed_reference_blocks{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::InsertBlockInCacheHelper", [&](void* arg) {
        const BlockType type = *static_cast<BlockType*>(arg);
        if (type == BlockType::kData) {
          ++warmed_data_blocks;
        } else if (type == BlockType::kUserDefinedIndex) {
          ++warmed_reference_blocks;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), std::string(100, 'v')));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_GT(warmed_data_blocks.load(), 0);
  EXPECT_EQ(warmed_reference_blocks.load(), 0);
  // The compaction output has reference blocks.
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1U);
  EXPECT_NE(properties.begin()->second->user_collected_properties.find(
                kBlobReferenceDescriptorPropertyName),
            properties.begin()->second->user_collected_properties.end());
}

TEST_F(DBBlobIndexTest, FlushRecordsExactReferenceMetadata) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"exact"}, options);
  const std::string key = "exact-key";
  const std::string value(100, 'v');
  ASSERT_OK(Put(1, key, value));
  ASSERT_OK(Flush(1));
  // Flush writes only the classification. The compaction output (level 1 or
  // higher) has the exact descriptor and reference blocks.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[1], &properties));
  ASSERT_EQ(properties.size(), 1U);
  const std::string& sst_path = properties.begin()->first;
  const TableProperties& table_properties = *properties.begin()->second;
  const auto encoded = table_properties.user_collected_properties.find(
      kBlobReferenceDescriptorPropertyName);
  ASSERT_NE(encoded, table_properties.user_collected_properties.end());
  BlobReferenceMetadataDescriptor descriptor;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(encoded->second,
                                                        &descriptor));
  // The descriptor names the real CF ID and SST file number, not placeholder
  // values.
  ASSERT_TRUE(descriptor.HasSstIdentity());
  EXPECT_EQ(descriptor.column_family_id, handles_[1]->GetID());
  EXPECT_EQ(descriptor.sst_file_number, table_properties.orig_file_number);
  ASSERT_EQ(descriptor.reference_count, 1U);

  // The index block is never compressed, so the test decodes it straight from
  // the file bytes. It names one group. The group's record bytes are those of a
  // normal blob record: its header, the user key and the value.
  std::string sst;
  ASSERT_OK(ReadFileToString(env_, sst_path, &sst));
  const auto block = [&](const BlockHandle& handle) {
    return Slice(sst.data() + handle.offset(),
                 static_cast<size_t>(handle.size()));
  };
  BlobReferenceIndex index;
  ASSERT_OK(DecodeBlobReferenceIndex(block(descriptor.index_handle), descriptor,
                                     &index));
  ASSERT_EQ(index.blocks.size(), 1U);
  ASSERT_EQ(index.groups.size(), 1U);
  EXPECT_EQ(index.groups[0].group.record_bytes,
            BlobLogRecord::kHeaderSize + key.size() + value.size());
  // The reference blocks are written before the SST tail, so a table open does
  // not prefetch them.
  EXPECT_GE(
      table_properties.tail_start_offset,
      descriptor.index_handle.offset() + descriptor.index_handle.size() + 5);
  EXPECT_GT(index.blocks[0].offset(), 0U);
}

TEST_F(DBBlobIndexTest, ReferenceDataBlocksUseTableCompression) {
  // Equal values give equal entries, so a reference data block compresses well
  // when the table has a compressor, and stays raw when it does not.
  std::vector<CompressionType> compressions = {kNoCompression};
  if (Snappy_Supported()) {
    compressions.push_back(kSnappyCompression);
  }
  if (LZ4_Supported()) {
    compressions.push_back(kLZ4Compression);
  }
  for (CompressionType compression : compressions) {
    SCOPED_TRACE(static_cast<int>(compression));
    Options options = CurrentOptions();
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    options.compression = compression;
    DestroyAndReopen(options);
    for (int i = 0; i < 2000; ++i) {
      ASSERT_OK(Put(Key(i), std::string(100, 'v')));
    }
    ASSERT_OK(Flush());
    // Flush writes only the classification. The compaction output (level 1 or
    // higher) is exact.
    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

    TablePropertiesCollection properties;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
    ASSERT_EQ(properties.size(), 1U);
    const auto encoded =
        properties.begin()->second->user_collected_properties.find(
            kBlobReferenceDescriptorPropertyName);
    ASSERT_NE(encoded,
              properties.begin()->second->user_collected_properties.end());
    BlobReferenceMetadataDescriptor descriptor;
    ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(encoded->second,
                                                          &descriptor));
    std::string sst;
    ASSERT_OK(ReadFileToString(env_, properties.begin()->first, &sst));
    BlobReferenceIndex index;
    ASSERT_OK(DecodeBlobReferenceIndex(
        Slice(sst.data() + descriptor.index_handle.offset(),
              static_cast<size_t>(descriptor.index_handle.size())),
        descriptor, &index));
    ASSERT_FALSE(index.blocks.empty());
    // Each block trailer starts with the compression type. A compressed block
    // is decompressed here, as a reader must do (see
    // WriteBlobReferenceMetadata() in the table builder), because the block
    // type (BlockType::kUserDefinedIndex) tells the table's normal block read
    // path that the block is never compressed.
    const ImmutableOptions ioptions(options);
    const std::shared_ptr<Decompressor> decompressor =
        GetBuiltinV2CompressionManager()->GetDecompressor();
    MetadataForTest metadata;
    for (size_t i = 0; i < index.blocks.size(); ++i) {
      const BlockHandle& handle = index.blocks[i];
      const char* const data = sst.data() + handle.offset();
      const size_t size = static_cast<size_t>(handle.size());
      ASSERT_EQ(static_cast<CompressionType>(data[size]), compression);
      if (compression == kNoCompression) {
        metadata.blocks.emplace_back(data, size);
      } else {
        BlockContents contents;
        ASSERT_OK(DecompressSerializedBlock(
            data, size, compression, *decompressor, &contents, ioptions));
        metadata.blocks.push_back(contents.data.ToString());
      }
    }
    uint64_t count = 0;
    ASSERT_OK(ForEachBlobReference(index, BlockReaderForTest(metadata),
                                   [&](const BlobReference&) {
                                     ++count;
                                     return Status::OK();
                                   }));
    EXPECT_EQ(count, 2000U);
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. We should be able to write kTypeBlobIndex to memtables and
// SST files.
TEST_F(DBBlobIndexTest, Write) {
  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());

    std::vector<std::pair<std::string, std::string>> key_values;

    constexpr size_t num_key_values = 5;

    key_values.reserve(num_key_values);

    for (size_t i = 1; i <= num_key_values; ++i) {
      std::string key = "key" + std::to_string(i);

      std::string blob_index;
      BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210,
                                  "blob" + std::to_string(i));

      key_values.emplace_back(std::move(key), std::move(blob_index));
    }

    for (const auto& key_value : key_values) {
      WriteBatch batch;
      ASSERT_OK(PutBlobIndex(&batch, key_value.first, key_value.second));
      ASSERT_OK(Write(&batch));
    }

    MoveDataTo(tier);

    for (const auto& key_value : key_values) {
      ASSERT_EQ(GetBlobIndex(key_value.first), key_value.second);
    }
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. Get should be able to return blob index if is_blob_index is
// provided, otherwise it should return Status::NotSupported (when reading from
// memtable) or Status::Corruption (when reading from SST). Reading from SST
// returns Corruption because we can't differentiate between the application
// accidentally opening the base DB of a stacked BlobDB and actual corruption
// when using the integrated BlobDB.
TEST_F(DBBlobIndexTest, Get) {
  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210, "blob");

  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());

    WriteBatch batch;
    ASSERT_OK(batch.Put("key", "value"));
    ASSERT_OK(PutBlobIndex(&batch, "blob_key", blob_index));
    ASSERT_OK(Write(&batch));

    MoveDataTo(tier);

    // Verify normal value
    bool is_blob_index = false;
    PinnableSlice value;
    ASSERT_EQ("value", Get("key"));
    ASSERT_EQ("value", GetImpl("key"));
    ASSERT_EQ("value", GetImpl("key", &is_blob_index));
    ASSERT_FALSE(is_blob_index);

    // Verify blob index
    if (tier <= kImmutableMemtables) {
      ASSERT_TRUE(Get("blob_key", &value).IsNotSupported());
      ASSERT_EQ("NOT_SUPPORTED", GetImpl("blob_key"));
    } else {
      ASSERT_TRUE(Get("blob_key", &value).IsCorruption());
      ASSERT_EQ("CORRUPTION", GetImpl("blob_key"));
    }
    ASSERT_EQ(blob_index, GetImpl("blob_key", &is_blob_index));
    ASSERT_TRUE(is_blob_index);
  }
}

TEST_F(DBBlobIndexTest,
       MaybeResolveDirectWriteValueDecodesPinnedBlobIndexBeforeReset) {
  DestroyAndReopen(GetTestOptions());

  std::string blob_index;
  BlobIndex::EncodeBlob(&blob_index, /*file_number=*/123, /*offset=*/456,
                        /*size=*/789, kNoCompression);

  PinnableSlice value;
  value.PinSlice(Slice(blob_index), CorruptPinnedBlobIndexOnCleanup,
                 &blob_index, nullptr);

  Status s = Status::OK();
  bool is_blob_index = true;
  ASSERT_TRUE(MaybeResolveDirectWriteValueForTest(
      ReadOptions(), Slice("key"), /*resolve_direct_write_value=*/true,
      /*current=*/nullptr, &value, /*columns=*/nullptr, &s, &is_blob_index));

  ASSERT_FALSE(s.IsCorruption()) << s.ToString();
  ASSERT_TRUE(s.IsIOError() || s.IsNotFound()) << s.ToString();
  ASSERT_FALSE(is_blob_index);
  ASSERT_EQ(static_cast<char>(BlobIndex::Type::kUnknown), blob_index.front());
}

TEST_F(DBBlobIndexTest,
       MaybeResolveMemtableBlobValueWithoutFetcherFailsClosed) {
  // Goal: if a readonly/secondary memtable hit produces a blob-backed payload
  // but no BlobFetcher is available, the helper must fail closed instead of
  // handing raw blob-index bytes back to the caller as if they were the value.
  std::string blob_index;
  BlobIndex::EncodeBlob(&blob_index, /*file_number=*/123, /*offset=*/456,
                        /*size=*/789, kNoCompression);

  PinnableSlice value;
  value.GetSelf()->assign(blob_index.data(), blob_index.size());
  value.PinSelf();

  bool did_resolve = false;
  bool is_blob_index = true;
  Status s = MaybeResolveMemtableBlobValueForTest(
      Slice("key"), /*blob_fetcher=*/nullptr, &value, /*columns=*/nullptr,
      &did_resolve, &is_blob_index);

  ASSERT_TRUE(did_resolve);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_TRUE(value.empty());
  ASSERT_FALSE(is_blob_index);
}

TEST_F(DBBlobIndexTest, PostprocessMemtableValueReadClearsOutputsOnError) {
  // Regression test for the "swallowed status" FIXME: on any error,
  // PostprocessMemtableValueRead must clear the outputs and return the status,
  // never leaving a half-populated result behind a non-OK status.

  // Case 1: the incoming memtable read status is already non-OK. The resolve
  // path is skipped; the finalization block clears outputs and returns it.
  {
    PinnableSlice value;
    value.GetSelf()->assign("stale", 5);
    value.PinSelf();

    bool is_blob_index = false;
    Status s = PostprocessMemtableValueReadForTest(
        Slice("key"), /*resolve_blob_backed_memtable_value=*/true,
        /*blob_fetcher=*/nullptr, &value, /*columns=*/nullptr,
        Status::Corruption("read failed"), &is_blob_index);

    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_TRUE(value.empty());
  }

  // Case 2: the incoming status is OK but the blob-resolve path itself fails (a
  // blob-backed value with no fetcher). This is the actual scenario the FIXME
  // referred to -- previously the non-OK status from the resolve path was
  // dropped. The failure must propagate and the raw blob-index bytes must be
  // cleared rather than handed back as if they were the value.
  {
    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, /*file_number=*/123, /*offset=*/456,
                          /*size=*/789, kNoCompression);

    PinnableSlice value;
    value.GetSelf()->assign(blob_index.data(), blob_index.size());
    value.PinSelf();

    bool is_blob_index = true;
    Status s = PostprocessMemtableValueReadForTest(
        Slice("key"), /*resolve_blob_backed_memtable_value=*/true,
        /*blob_fetcher=*/nullptr, &value, /*columns=*/nullptr, Status::OK(),
        &is_blob_index);

    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_TRUE(value.empty());
  }
}

TEST_F(DBBlobIndexTest, ReadOnlyGetImplReturnsBlobIndexWhenRequested) {
  // Goal: cover the internal read-only GetImpl contract when the caller
  // explicitly asks for raw blob-index bytes via `is_blob_index`. Recovery
  // keeps the blob index in the memtable, and the read-only path must preserve
  // the encoded index instead of eagerly resolving or rejecting it.
  Options options = GetTestOptions();

  DestroyAndReopen(options);

  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /*expiration=*/9876543210, "blob");

  WriteBatch batch;
  ASSERT_OK(PutBlobIndex(&batch, "blob_key", blob_index));
  ASSERT_OK(Write(&batch));

  Close();
  options.avoid_flush_during_recovery = true;
  ASSERT_OK(ReadOnlyReopen(options));

  bool is_blob_index = false;
  ASSERT_EQ(blob_index, GetImpl("blob_key", &is_blob_index));
  ASSERT_TRUE(is_blob_index);
}

// Regression test for a same-file (embedded) BlobIndex being exposed
// unresolved through an iterator. With index_type=kBinarySearchWithFirstKey
// and allow_unprepared_value (the default for DB iterators), the block-based
// table iterator can sit in the is_at_first_key_from_index_ state and return
// the raw kTypeBlobIndex internal key from the index, while value() resolves
// the same-file blob to its plain payload. DBIter then reads the stale
// kTypeBlobIndex type and routes the already-resolved plain value through the
// blob-index path, corrupting/misreading it.
//
// Forcing one entry per data block makes every embedded blob the first key of
// a block, reliably triggering the deferred-first-key state on both SeekToFirst
// and forward block transitions.
TEST_F(DBBlobIndexTest, EmbeddedBlobIteratorWithFirstKeyIndex) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  BlockBasedTableOptions bbto;
  bbto.index_type =
      BlockBasedTableOptions::IndexType::kBinarySearchWithFirstKey;
  // Soft limit of 1 byte starts a new data block after every entry.
  bbto.block_size = 1;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  // Build an embedded-blob SST whose large values are stored as same-file blob
  // records (table entries become same-file BlobIndex references).
  const std::string sst_path = dbname_ + "/embedded_first_key.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'x');
  const std::string big2(1024, 'y');
  const std::string big3(1024, 'z');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.Put("k2", big2));
  ASSERT_OK(writer.Put("k3", big3));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Point lookups resolve same-file blobs before exposing them, so Get works.
  ASSERT_EQ(Get("k1"), big1);
  ASSERT_EQ(Get("k2"), big2);
  ASSERT_EQ(Get("k3"), big3);

  // Iterator path: the bug manifests here.
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), big2);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k3");
  EXPECT_EQ(iter->value(), big3);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // Seek directly to a block whose first key is an embedded blob.
  iter->Seek("k2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), big2);
}

TEST_F(DBBlobIndexTest, CompressedSameFileRangeIsRejectedWithBlobSource) {
  // As in base RocksDB, a range of a compressed value is not supported. The
  // table reader of a DB-owned SST (it has a BlobSource) returns Corruption for
  // one, from the scalar and the batched read, before any other check, so a
  // kBlockCacheTier read gets Corruption too. Base SSTs never hold a compressed
  // embedded blob, so the test uses a made-up same-file index.
  Options options = GetTestOptions();
  options.max_open_files = -1;
  DestroyAndReopen(options);
  const std::string sst_path = dbname_ + "/embedded_compressed_range.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;
  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", std::string(1024, 'x')));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const BlockBasedTable* table = nullptr;
  for (int level = 0; level < cfd->NumberLevels() && table == nullptr;
       ++level) {
    for (const FileMetaData* file :
         cfd->current()->storage_info()->LevelFiles(level)) {
      table = static_cast<const BlockBasedTable*>(file->fd.pinned_reader.Get());
    }
  }
  ASSERT_NE(table, nullptr);

  std::string encoded;
  BlobIndex::EncodeBlob(&encoded, kCurrentFileBlobIndexFileNumber,
                        /*offset=*/0, /*size=*/16, kSnappyCompression);
  BlobIndex index;
  ASSERT_OK(index.DecodeFrom(encoded));
  for (const ReadTier read_tier : {kReadAllTier, kBlockCacheTier}) {
    ReadOptions read_options;
    read_options.read_tier = read_tier;
    PinnableSlice value;
    EXPECT_TRUE(table
                    ->GetSameFileBlob(
                        read_options, index, /*range_offset=*/1,
                        /*range_length=*/4,
                        BlobVerifyPolicy::kVerifyIfNoAmplification, &value)
                    .IsCorruption());
    PinnableSlice batch_value;
    Status batch_status;
    SameFileBlobReadRequest request;
    request.blob_index = &index;
    request.range_offset = 1;
    request.range_length = 4;
    request.result = &batch_value;
    request.status = &batch_status;
    table->MultiGetSameFileBlob(read_options, /*num_reads=*/1, &request);
    EXPECT_TRUE(batch_status.IsCorruption()) << batch_status.ToString();
  }
  ASSERT_EQ(Get("k1"), std::string(1024, 'x'));
}

// Backward iteration (Prev / SeekForPrev) over an embedded-blob SST must work,
// including for wide-column entities whose same-file blob columns are resolved
// into an iterator-owned buffer. DBIter requires the underlying iterator's
// value to be pinned for backward iteration; EmbeddedBlobResolvingIterator
// therefore pins resolved values -- both whole-value blob payloads and rebuilt
// wide-column values -- and hands their cleanup to the PinnedIteratorsManager
// across repositioning. Before that, backward iteration over the wide-column
// entity returned NotSupported (or tripped the IsValuePinned() assertion).
TEST_F(DBBlobIndexTest, EmbeddedBlobBackwardIteration) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_backward.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  // Whole-value same-file blobs.
  const std::string big1(1024, 'x');
  const std::string big3(1024, 'z');
  // Wide-column entity: a small (inline) default column plus a large
  // (embedded, same-file blob) non-default column -- the mixed wide-column
  // path whose resolved value lands in an iterator-owned buffer.
  const std::string k2_default(2, 'd');
  const std::string k2_big_col(1024, 'c');
  const WideColumns k2_columns{{kDefaultWideColumnName, k2_default},
                               {"big", k2_big_col}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.PutEntity("k2", k2_columns));
  ASSERT_OK(writer.Put("k3", big3));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));

  // Backward scan via SeekToLast + Prev.
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k3");
  EXPECT_EQ(iter->value(), big3);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), k2_default);
  EXPECT_EQ(iter->columns(), k2_columns);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);

  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  // SeekForPrev directly onto the wide-column entity, then step back.
  iter->SeekForPrev("k2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), k2_default);
  EXPECT_EQ(iter->columns(), k2_columns);

  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);
}

// Regression test for T277566778. If resolving a same-file ("embedded") blob
// fails during compaction (e.g. a blob-region read fault), the error must be
// surfaced as-is. It must NOT be masked by feeding the raw, unresolved
// same-file BlobIndex downstream, where FileMetaData::UpdateBoundaries would
// (correctly) reject file number 0 and report a misleading "Invalid blob file
// number" corruption. The EmbeddedBlobResolvingIterator must not expose an
// unresolved same-file value on error: for eager (compaction) callers it
// resolves during positioning so the error is visible via status()/Valid()
// before value().
TEST_F(DBBlobIndexTest, EmbeddedBlobResolveErrorDuringCompactionNotMasked) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_resolve_err.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  // Wide-column entity with a large (embedded, same-file blob) column: the
  // mixed path whose resolution reads the blob region during compaction.
  const std::string big_col(1024, 'c');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big_col}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k2", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Add keys straddling the entity so a real compaction (not a trivial move)
  // reads and resolves the embedded entity.
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Put("k3", "v3"));
  ASSERT_OK(Flush());

  // Inject a resolution failure only during the compaction below, simulating a
  // blob-region read fault.
  const Status kInjected = Status::IOError("injected embedded blob read error");
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MaybeResolveEmbeddedValue:InjectError",
      [&](void* arg) { *static_cast<Status*>(arg) = kInjected; });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // The compaction must fail with the injected read error, NOT a masked
  // "Invalid blob file number" corruption.
  ASSERT_NOK(s);
  EXPECT_TRUE(s.IsIOError()) << s.ToString();
  EXPECT_EQ(s.ToString().find("Invalid blob file number"), std::string::npos)
      << s.ToString();
}

// Regression test for T277310719, the whole-value (kTypeBlobIndex) sibling of
// the wide-column-entity case above (T277566778). Both share one root cause: on
// a blob-region resolution error during compaction, the
// EmbeddedBlobResolvingIterator must not fall back to exposing the raw,
// unresolved same-file value.
//
// The whole-value variant is more dangerous than the entity variant because it
// escapes the FileMetaData::UpdateBoundaries tripwire: ResolveKeyType()
// rewrites the key type kTypeBlobIndex -> kTypeValue (no I/O, always succeeds),
// so on a masked resolution error the emitted entry is {kTypeValue, raw
// BlobIndex bytes}. UpdateBoundaries only scans kTypeBlobIndex/entity types, so
// unlike the entity variant it does NOT reject it -- the masked error slips
// through, a corrupt {kTypeValue, BlobIndex} record is written to the
// compaction output, and a later point lookup reads those raw BlobIndex bytes
// back as the value (in db_stress this surfaced as the
// ExpectedValue::IsValueBaseValid assertion, not "Invalid blob file number").
// The eager (compaction) resolver must surface the error via status()/Valid()
// before value() is consumed.
TEST_F(DBBlobIndexTest,
       EmbeddedBlobResolveErrorWholeValueDuringCompactionNotMasked) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_resolve_err_wholevalue.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  // A large whole value is stored as a same-file blob, so the table entry
  // becomes a same-file (file number 0) BlobIndex (kTypeBlobIndex) -- the
  // whole-value path whose resolution reads the blob region during compaction.
  const std::string big_val(1024, 'b');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k2", big_val));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Add keys straddling the embedded blob so a real compaction (not a trivial
  // move) reads and resolves it.
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Put("k3", "v3"));
  ASSERT_OK(Flush());

  // Inject a resolution failure only during the compaction below, simulating a
  // blob-region read fault.
  const Status kInjected = Status::IOError("injected embedded blob read error");
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MaybeResolveEmbeddedValue:InjectError",
      [&](void* arg) { *static_cast<Status*>(arg) = kInjected; });
  SyncPoint::GetInstance()->EnableProcessing();

  const Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // The compaction must fail with the injected read error. Before the fix it
  // instead succeeded (masking the error) and persisted a {kTypeValue, raw
  // BlobIndex} record; it must not do that, and it must not report a misleading
  // "Invalid blob file number" corruption either.
  ASSERT_NOK(s);
  EXPECT_TRUE(s.IsIOError()) << s.ToString();
  EXPECT_EQ(s.ToString().find("Invalid blob file number"), std::string::npos)
      << s.ToString();
}

// When a blob cache is configured, same-file ("embedded") blob reads should go
// through BlobSource: the first read misses + inserts + does a disk read, and
// the second read of the same blob is served from the blob cache. This holds on
// both the Get() and the iterator paths.
TEST_F(DBBlobIndexTest, EmbeddedBlobBlobCacheHitMiss) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();

  LRUCacheOptions co;
  co.capacity = 8 << 20;
  options.blob_cache = NewLRUCache(co);

  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_cache.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'x');
  const std::string big2(1024, 'y');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.Put("k2", big2));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  Statistics* const stats = options.statistics.get();

  // First Get: blob cache miss -> disk read -> cache insert.
  ASSERT_OK(stats->Reset());
  get_perf_context()->Reset();
  SetPerfLevel(kEnableCount);
  ASSERT_EQ(Get("k1"), big1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_MISS), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_HIT), 0);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 1);
  EXPECT_GT(stats->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ), 0);
  EXPECT_EQ(get_perf_context()->blob_read_count, 1);

  // Second Get of the same key: blob cache hit, no disk read, no insert.
  ASSERT_OK(stats->Reset());
  get_perf_context()->Reset();
  ASSERT_EQ(Get("k1"), big1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_HIT), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_MISS), 0);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 0);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ), 0);
  EXPECT_EQ(get_perf_context()->blob_read_count, 0);
  EXPECT_EQ(get_perf_context()->blob_cache_hit_count, 1);

  // Iterator path: first read of a fresh key misses + inserts.
  ASSERT_OK(stats->Reset());
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek("k2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    EXPECT_EQ(iter->value(), big2);
  }
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_MISS), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 1);

  // Iterator path: second read of the same key hits the blob cache.
  ASSERT_OK(stats->Reset());
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek("k2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    EXPECT_EQ(iter->value(), big2);
  }
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_HIT), 1);
  EXPECT_EQ(stats->getTickerCount(BLOB_DB_CACHE_ADD), 0);

  SetPerfLevel(kDisable);
}

// With the blob cache and block cache pointing at the same Cache, the SST-like
// cache-key scheme (offset >> 2 on the SST's base key) keeps embedded blob
// records disjoint from the SST's data blocks. Reading embedded blobs must not
// alias or evict data blocks, and values must remain correct.
TEST_F(DBBlobIndexTest, EmbeddedBlobSharedBlockAndBlobCache) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();

  LRUCacheOptions co;
  co.capacity = 16 << 20;
  auto shared_cache = NewLRUCache(co);
  options.blob_cache = shared_cache;

  BlockBasedTableOptions bbto;
  bbto.block_cache = shared_cache;
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_shared.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'x');
  const std::string big2(1024, 'y');

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.Put("k1", big1));
  ASSERT_OK(writer.Put("k2", big2));
  ASSERT_OK(writer.Finish());

  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  Statistics* const stats = options.statistics.get();

  // Values are correct with a shared cache, and re-reads hit both the block
  // cache (data blocks) and the blob cache (embedded payloads) independently.
  ASSERT_EQ(Get("k1"), big1);
  ASSERT_EQ(Get("k2"), big2);
  ASSERT_EQ(Get("k1"), big1);
  ASSERT_EQ(Get("k2"), big2);
  EXPECT_GT(stats->getTickerCount(BLOB_DB_CACHE_HIT), 0);
  EXPECT_GT(stats->getTickerCount(BLOCK_CACHE_HIT), 0);

  // Iterator yields correct values too.
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k1");
  EXPECT_EQ(iter->value(), big1);

  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  EXPECT_EQ(iter->key(), "k2");
  EXPECT_EQ(iter->value(), big2);

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

// Point GetEntity / MultiGetEntity of a wide-column entity whose non-default
// columns are embedded (same-file) blobs must return the correct columns and
// resolve them zero-copy: each resolved blob column value points straight at
// its pinned blob-cache buffer (identical across reads, and in a separate
// buffer per column), rather than being copied into one re-serialized entity.
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetEntityZeroCopy) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  LRUCacheOptions co;
  co.capacity = 8 << 20;
  options.blob_cache = NewLRUCache(co);
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_zerocopy.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string small_default = "d";  // inline default column
  const std::string big1(1024, 'a');      // same-file blob column
  const std::string big2(2048, 'b');      // same-file blob column
  const WideColumns columns{
      {kDefaultWideColumnName, small_default}, {"big1", big1}, {"big2", big2}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  const char* big1_ptr = nullptr;
  const char* big2_ptr = nullptr;
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);

    // columns() is sorted by name -> [default(""), big1, big2].
    big1_ptr = result.columns()[1].value().data();
    big2_ptr = result.columns()[2].value().data();
    // Multi-buffer: each blob column lives in its own backing buffer, so the
    // two values are not laid out contiguously (as they would be in a single
    // re-serialized V1 entity).
    ASSERT_NE(big1_ptr + big1.size(), big2_ptr);

    // Pointer stability across move (address-stable backing nodes).
    PinnableWideColumns moved(std::move(result));
    ASSERT_EQ(moved.columns(), columns);
    ASSERT_EQ(moved.columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(moved.columns()[2].value().data(), big2_ptr);
  }

  // A second read serves the same pinned blob-cache buffers (zero-copy): the
  // resolved column values point at the very same addresses, which a per-read
  // re-serialization would not.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);
    ASSERT_EQ(result.columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(result.columns()[2].value().data(), big2_ptr);
  }

  // MultiGetEntity: same correctness and zero-copy (cache-hit) guarantee.
  {
    const std::array<Slice, 1> keys{Slice("k")};
    std::array<PinnableWideColumns, 1> results;
    std::array<Status, 1> statuses;
    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), keys.size(),
                        keys.data(), results.data(), statuses.data());
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), columns);
    ASSERT_EQ(results[0].columns()[1].value().data(), big1_ptr);
    ASSERT_EQ(results[0].columns()[2].value().data(), big2_ptr);
  }
}

// A wide-column entity whose default column is itself an embedded (same-file)
// blob must resolve correctly on the plain Get path (default-column
// extraction) as well as GetEntity, matching the values written.
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetDefaultColumn) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_default.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big_default(1024, 'x');  // same-file blob default column
  const std::string small_attr = "s";        // inline non-default column
  const WideColumns columns{{kDefaultWideColumnName, big_default},
                            {"attr", small_attr}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Plain Get returns the (blob-backed) default column value.
  PinnableSlice value;
  ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "k", &value));
  ASSERT_EQ(value, big_default);

  // GetEntity returns all columns.
  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k", &result));
  ASSERT_EQ(result.columns(), columns);
}

// Merge over an embedded-blob wide-column base entity must resolve the base
// entity's same-file default column before merging, and GetMergeOperands must
// return the resolved default column as the base operand.
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnMergeBase) {
  Options options = GetTestOptions();  // merge_operator = StringAppend(',')
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_merge.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big_default(1024, 'D');  // same-file blob default column
  const WideColumns columns{{kDefaultWideColumnName, big_default},
                            {"attr", "s"}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Newer merge operand lives in the memtable; the embedded entity base lives
  // in the ingested SST, so the merge resolves the base via the same-file path.
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), "k", "op1"));

  PinnableSlice value;
  ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "k", &value));
  ASSERT_EQ(value, big_default + "," + "op1");

  // GetMergeOperands returns the resolved base default column plus the operand.
  std::array<PinnableSlice, 8> operands;
  GetMergeOperandsOptions get_merge_opts;
  get_merge_opts.expected_max_number_of_operands =
      static_cast<int>(operands.size());
  int num_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k", operands.data(), &get_merge_opts,
                                  &num_operands));
  ASSERT_EQ(num_operands, 2);
  ASSERT_EQ(operands[0], big_default);
  ASSERT_EQ(operands[1], "op1");
}

// With read_tier == kBlockCacheTier, an unresolved same-file blob column of a
// wide-column entity must surface Incomplete (never a raw same-file BlobIndex).
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetEntityBlockCacheTier) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_blockcache.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big1}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Warm the block cache so the data block read succeeds under kBlockCacheTier
  // and the lookup actually reaches same-file blob column resolution.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;
  PinnableWideColumns result;
  Status s =
      db_->GetEntity(read_options, db_->DefaultColumnFamily(), "k", &result);
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
}

// Companion to the test above (genuine miss -> Incomplete): with a blob cache
// configured, a warm read populates the embedded record into the blob cache, so
// a subsequent kBlockCacheTier GetEntity serves it from cache instead of
// returning Incomplete. This exercises the non-lazy Get/GetEntity embedded path
// (get_context -> EmbeddedAwareBlobFetcher -> GetSameFileBlob whole ->
// ResolveEmbeddedBlobCached), which shares the block-cache-tier behavior with
// the lazy resolver (a same-file column already in the blob cache is served).
TEST_F(DBBlobIndexTest,
       EmbeddedBlobWideColumnGetEntityBlockCacheTierServesCached) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  LRUCacheOptions co;
  co.capacity = 8 << 20;
  options.blob_cache = NewLRUCache(co);
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_blockcache_hit.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big1(1024, 'a');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big1}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  // Warm both the block cache (data block) and the blob cache (embedded record)
  // with a full read.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k",
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  // The embedded record is now cached, so a block-cache-only read serves it.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;
  PinnableWideColumns result;
  ASSERT_OK(
      db_->GetEntity(read_options, db_->DefaultColumnFamily(), "k", &result));
  ASSERT_EQ(result.columns(), columns);
}

// A corrupt embedded blob record backing a wide-column column must surface an
// error on GetEntity, not expose the raw same-file BlobIndex.
// A failure resolving an embedded blob record backing a wide-column column
// (e.g. a bad record or blob-region read fault) must surface on GetEntity, not
// expose the raw same-file BlobIndex. The failure is injected via a sync point
// so the test is independent of the on-disk byte layout (e.g. encrypted env).
TEST_F(DBBlobIndexTest, EmbeddedBlobWideColumnGetEntityCorruptionSurfaced) {
  Options options = GetTestOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const std::string sst_path = dbname_ + "/embedded_wc_corrupt.sst";
  SstFileWriterEmbeddedBlobOptions embedded_blob_options;
  embedded_blob_options.min_blob_size = 8;

  const std::string big_col(1024, 'Z');
  const WideColumns columns{{kDefaultWideColumnName, "d"}, {"big", big_col}};

  SstFileWriter writer(EnvOptions(), options);
  ASSERT_OK(writer.OpenWithEmbeddedBlobs(sst_path, embedded_blob_options));
  ASSERT_OK(writer.PutEntity("k", columns));
  ASSERT_OK(writer.Finish());
  ASSERT_OK(db_->IngestExternalFile({sst_path}, IngestExternalFileOptions()));

  const Status kInjected = Status::Corruption("injected embedded blob error");
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MaybeResolveEmbeddedValue:InjectError",
      [&](void* arg) { *static_cast<Status*>(arg) = kInjected; });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableWideColumns result;
  const Status s =
      db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "k", &result);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

class PlainBlobValueFilterV3 : public CompactionFilter {
 public:
  PlainBlobValueFilterV3(std::atomic<int>* filter_call_count,
                         std::string* observed_value)
      : filter_call_count_(filter_call_count),
        observed_value_(observed_value) {}

  Decision FilterV3(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* /*existing_columns*/,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/) const override {
    if (value_type != ValueType::kValue || existing_value == nullptr) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);
    *observed_value_ = existing_value->ToString();
    return Decision::kRemove;
  }

  const char* Name() const override { return "PlainBlobValueFilterV3"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
};

class PlainBlobValueFilterV3Factory : public CompactionFilterFactory {
 public:
  PlainBlobValueFilterV3Factory(TableFileCreationReason reason,
                                std::atomic<int>* filter_call_count,
                                std::string* observed_value)
      : reason_(reason),
        filter_call_count_(filter_call_count),
        observed_value_(observed_value) {}

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == reason_;
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<PlainBlobValueFilterV3>(filter_call_count_,
                                                    observed_value_);
  }

  const char* Name() const override { return "PlainBlobValueFilterV3Factory"; }

 private:
  TableFileCreationReason reason_;
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
};

TEST_F(DBBlobIndexTest, DirectWritePlainBlobFilterV3FlushUsesResolvedValue) {
  std::atomic<int> filter_call_count{0};
  std::string observed_value;

  Options options =
      wide_column_test_util::GetDirectWriteOptions(GetTestOptions());
  options.compaction_filter_factory =
      std::make_shared<PlainBlobValueFilterV3Factory>(
          TableFileCreationReason::kFlush, &filter_call_count, &observed_value);

  DestroyAndReopen(options);

  const std::string key = "flush_blob_key";
  const std::string large_value(10 * 1024, 'F');

  ASSERT_OK(Put(key, large_value));
  ASSERT_OK(Flush());

  ASSERT_EQ("NOT_FOUND", Get(key));
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(observed_value, large_value);
}

class PlainBlobValueFilterV4 : public CompactionFilter {
 public:
  PlainBlobValueFilterV4(std::atomic<int>* filter_call_count,
                         std::string* observed_value,
                         std::atomic<bool>* saw_nonnull_blob_resolver)
      : filter_call_count_(filter_call_count),
        observed_value_(observed_value),
        saw_nonnull_blob_resolver_(saw_nonnull_blob_resolver) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* /*existing_columns*/,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type != ValueType::kValue || existing_value == nullptr) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);
    *observed_value_ = existing_value->ToString();
    saw_nonnull_blob_resolver_->store(blob_resolver != nullptr,
                                      std::memory_order_relaxed);
    return Decision::kRemove;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "PlainBlobValueFilterV4"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
  std::atomic<bool>* saw_nonnull_blob_resolver_;
};

class PlainBlobValueFilterV4Factory : public CompactionFilterFactory {
 public:
  PlainBlobValueFilterV4Factory(TableFileCreationReason reason,
                                std::atomic<int>* filter_call_count,
                                std::string* observed_value,
                                std::atomic<bool>* saw_nonnull_blob_resolver)
      : reason_(reason),
        filter_call_count_(filter_call_count),
        observed_value_(observed_value),
        saw_nonnull_blob_resolver_(saw_nonnull_blob_resolver) {}

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == reason_;
  }

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<PlainBlobValueFilterV4>(
        filter_call_count_, observed_value_, saw_nonnull_blob_resolver_);
  }

  const char* Name() const override { return "PlainBlobValueFilterV4Factory"; }

 private:
  TableFileCreationReason reason_;
  std::atomic<int>* filter_call_count_;
  std::string* observed_value_;
  std::atomic<bool>* saw_nonnull_blob_resolver_;
};

// Blob direct-write forces a clean-shutdown flush on close so active blob
// files are registered before reopen. Use flush to exercise the non-compaction
// plain-blob FilterV4 path.
TEST_F(DBBlobIndexTest, DirectWritePlainBlobFilterV4FlushUsesResolvedValue) {
  std::atomic<int> filter_call_count{0};
  std::string observed_value;
  std::atomic<bool> saw_nonnull_blob_resolver{false};

  Options options =
      wide_column_test_util::GetDirectWriteOptions(GetTestOptions());
  options.compaction_filter_factory =
      std::make_shared<PlainBlobValueFilterV4Factory>(
          TableFileCreationReason::kFlush, &filter_call_count, &observed_value,
          &saw_nonnull_blob_resolver);

  DestroyAndReopen(options);

  const std::string key = "flush_v4_blob_key";
  const std::string large_value(10 * 1024, 'R');

  ASSERT_OK(Put(key, large_value));
  ASSERT_OK(Flush());

  ASSERT_EQ("NOT_FOUND", Get(key));
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(observed_value, large_value);
  ASSERT_FALSE(saw_nonnull_blob_resolver.load(std::memory_order_relaxed));
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. Get should NOT return Status::NotSupported/Status::Corruption
// if blob index is updated with a normal value. See the test case above for
// more details.
TEST_F(DBBlobIndexTest, Updated) {
  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /* expiration */ 9876543210, "blob");

  for (auto tier : kAllTiers) {
    DestroyAndReopen(GetTestOptions());
    WriteBatch batch;
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(PutBlobIndex(&batch, "key" + std::to_string(i), blob_index));
    }
    ASSERT_OK(Write(&batch));
    // Avoid blob values from being purged.
    const Snapshot* snapshot = dbfull()->GetSnapshot();
    ASSERT_OK(Put("key1", "new_value"));
    ASSERT_OK(Merge("key2", "a"));
    ASSERT_OK(Merge("key2", "b"));
    ASSERT_OK(Merge("key2", "c"));
    ASSERT_OK(Delete("key3"));
    ASSERT_OK(SingleDelete("key4"));
    ASSERT_OK(Delete("key5"));
    ASSERT_OK(Merge("key5", "a"));
    ASSERT_OK(Merge("key5", "b"));
    ASSERT_OK(Merge("key5", "c"));
    ASSERT_OK(dbfull()->DeleteRange(WriteOptions(), cfh(), "key6", "key9"));
    MoveDataTo(tier);
    for (int i = 0; i < 10; i++) {
      ASSERT_EQ(blob_index, GetBlobIndex("key" + std::to_string(i), snapshot));
    }
    ASSERT_EQ("new_value", Get("key1"));
    if (tier <= kImmutableMemtables) {
      ASSERT_EQ("NOT_SUPPORTED", GetImpl("key2"));
    } else {
      ASSERT_EQ("CORRUPTION", GetImpl("key2"));
    }
    ASSERT_EQ("NOT_FOUND", Get("key3"));
    ASSERT_EQ("NOT_FOUND", Get("key4"));
    ASSERT_EQ("a,b,c", GetImpl("key5"));
    for (int i = 6; i < 9; i++) {
      ASSERT_EQ("NOT_FOUND", Get("key" + std::to_string(i)));
    }
    ASSERT_EQ(blob_index, GetBlobIndex("key9"));
    dbfull()->ReleaseSnapshot(snapshot);
  }
}

// Note: the following test case pertains to the StackableDB-based BlobDB
// implementation. When a blob iterator is used, it should set the
// expose_blob_index flag for the underlying DBIter, and retrieve/return the
// corresponding blob value. If a regular DBIter is created (i.e.
// expose_blob_index is not set), it should return Status::Corruption.
TEST_F(DBBlobIndexTest, Iterate) {
  const std::vector<std::vector<ValueType>> data = {
      /*00*/ {kTypeValue},
      /*01*/ {kTypeBlobIndex},
      /*02*/ {kTypeValue},
      /*03*/ {kTypeBlobIndex, kTypeValue},
      /*04*/ {kTypeValue},
      /*05*/ {kTypeValue, kTypeBlobIndex},
      /*06*/ {kTypeValue},
      /*07*/ {kTypeDeletion, kTypeBlobIndex},
      /*08*/ {kTypeValue},
      /*09*/ {kTypeSingleDeletion, kTypeBlobIndex},
      /*10*/ {kTypeValue},
      /*11*/ {kTypeMerge, kTypeMerge, kTypeMerge, kTypeBlobIndex},
      /*12*/ {kTypeValue},
      /*13*/
      {kTypeMerge, kTypeMerge, kTypeMerge, kTypeDeletion, kTypeBlobIndex},
      /*14*/ {kTypeValue},
      /*15*/ {kTypeBlobIndex},
      /*16*/ {kTypeValue},
  };

  auto get_key = [](int index) {
    char buf[20];
    snprintf(buf, sizeof(buf), "%02d", index);
    return "key" + std::string(buf);
  };

  auto get_value = [&](int index, int version) {
    return get_key(index) + "_value" + std::to_string(version);
  };

  auto check_iterator = [&](Iterator* iterator, Status::Code expected_status,
                            const Slice& expected_value) {
    ASSERT_EQ(expected_status, iterator->status().code());
    if (expected_status == Status::kOk) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(expected_value, iterator->value());
    } else {
      ASSERT_FALSE(iterator->Valid());
    }
  };

  auto create_normal_iterator = [&]() -> Iterator* {
    return dbfull()->NewIterator(ReadOptions());
  };

  auto create_blob_iterator = [&]() -> Iterator* { return GetBlobIterator(); };

  auto check_is_blob = [&](bool is_blob) {
    return [is_blob](Iterator* iterator) {
      ASSERT_EQ(is_blob, static_cast<ArenaWrappedDBIter*>(iterator)->IsBlob());
    };
  };

  auto verify = [&](int index, Status::Code expected_status,
                    const Slice& forward_value, const Slice& backward_value,
                    const std::function<Iterator*()>& create_iterator,
                    const std::function<void(Iterator*)>& extra_check =
                        nullptr) {
    // Seek
    auto* iterator = create_iterator();
    ASSERT_OK(iterator->status());
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index));
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Next
    iterator = create_iterator();
    ASSERT_OK(iterator->Refresh());
    iterator->Seek(get_key(index - 1));
    ASSERT_TRUE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator->Next();
    check_iterator(iterator, expected_status, forward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // SeekForPrev
    iterator = create_iterator();
    ASSERT_OK(iterator->status());
    ASSERT_OK(iterator->Refresh());
    iterator->SeekForPrev(get_key(index));
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;

    // Prev
    iterator = create_iterator();
    iterator->Seek(get_key(index + 1));
    ASSERT_TRUE(iterator->Valid());
    ASSERT_OK(iterator->status());
    iterator->Prev();
    check_iterator(iterator, expected_status, backward_value);
    if (extra_check) {
      extra_check(iterator);
    }
    delete iterator;
  };

  for (auto tier : {Tier::kMemtable} /*kAllTiers*/) {
    // Avoid values from being purged.
    std::vector<const Snapshot*> snapshots;
    DestroyAndReopen(GetTestOptions());

    // fill data
    for (int i = 0; i < static_cast<int>(data.size()); i++) {
      for (int j = static_cast<int>(data[i].size()) - 1; j >= 0; j--) {
        std::string key = get_key(i);
        std::string value = get_value(i, j);
        WriteBatch batch;
        switch (data[i][j]) {
          case kTypeValue:
            ASSERT_OK(Put(key, value));
            break;
          case kTypeDeletion:
            ASSERT_OK(Delete(key));
            break;
          case kTypeSingleDeletion:
            ASSERT_OK(SingleDelete(key));
            break;
          case kTypeMerge:
            ASSERT_OK(Merge(key, value));
            break;
          case kTypeBlobIndex:
            ASSERT_OK(PutBlobIndex(&batch, key, value));
            ASSERT_OK(Write(&batch));
            break;
          default:
            FAIL();
        };
      }
      snapshots.push_back(dbfull()->GetSnapshot());
    }
    ASSERT_OK(
        dbfull()->DeleteRange(WriteOptions(), cfh(), get_key(15), get_key(16)));
    snapshots.push_back(dbfull()->GetSnapshot());
    MoveDataTo(tier);

    // Normal iterator
    verify(1, Status::kCorruption, "", "", create_normal_iterator);
    verify(3, Status::kCorruption, "", "", create_normal_iterator);
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_normal_iterator);
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_normal_iterator);
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_normal_iterator);
    verify(11, Status::kCorruption, "", "", create_normal_iterator);
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_normal_iterator);
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_normal_iterator);

    // Iterator with blob support
    verify(1, Status::kOk, get_value(1, 0), get_value(1, 0),
           create_blob_iterator, check_is_blob(true));
    verify(3, Status::kOk, get_value(3, 0), get_value(3, 0),
           create_blob_iterator, check_is_blob(true));
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_blob_iterator, check_is_blob(false));
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_blob_iterator, check_is_blob(false));
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_blob_iterator, check_is_blob(false));
    if (tier <= kImmutableMemtables) {
      verify(11, Status::kNotSupported, "", "", create_blob_iterator);
    } else {
      verify(11, Status::kCorruption, "", "", create_blob_iterator);
    }
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_blob_iterator, check_is_blob(false));
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_blob_iterator, check_is_blob(false));

    // Iterator with blob support and using seek.
    ASSERT_OK(dbfull()->SetOptions(
        cfh(), {{"max_sequential_skip_in_iterations", "0"}}));
    verify(1, Status::kOk, get_value(1, 0), get_value(1, 0),
           create_blob_iterator, check_is_blob(true));
    verify(3, Status::kOk, get_value(3, 0), get_value(3, 0),
           create_blob_iterator, check_is_blob(true));
    verify(5, Status::kOk, get_value(5, 0), get_value(5, 0),
           create_blob_iterator, check_is_blob(false));
    verify(7, Status::kOk, get_value(8, 0), get_value(6, 0),
           create_blob_iterator, check_is_blob(false));
    verify(9, Status::kOk, get_value(10, 0), get_value(8, 0),
           create_blob_iterator, check_is_blob(false));
    if (tier <= kImmutableMemtables) {
      verify(11, Status::kNotSupported, "", "", create_blob_iterator);
    } else {
      verify(11, Status::kCorruption, "", "", create_blob_iterator);
    }
    verify(13, Status::kOk,
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           get_value(13, 2) + "," + get_value(13, 1) + "," + get_value(13, 0),
           create_blob_iterator, check_is_blob(false));
    verify(15, Status::kOk, get_value(16, 0), get_value(14, 0),
           create_blob_iterator, check_is_blob(false));

    for (auto* snapshot : snapshots) {
      dbfull()->ReleaseSnapshot(snapshot);
    }
  }
}

// Use shared test utilities for Wide Column + Blob integration tests
using wide_column_test_util::GenerateLargeValue;
using wide_column_test_util::GenerateSmallValue;

// Test 1: Full roundtrip test: PutEntity with large columns -> flush ->
// compaction with blob extraction -> read back with blob resolution -> verify
TEST_F(DBBlobIndexTest, EntityBlobFlushCompactionRoundtrip) {
  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.statistics = CreateDBStatistics();

  DestroyAndReopen(options);

  // Create entities with large column values that will become blobs
  constexpr char key1[] = "entity_key1";
  constexpr char key2[] = "entity_key2";
  constexpr char key3[] = "entity_key3";

  // Large value for default column (will become blob)
  std::string large_default_value = GenerateLargeValue(100);
  // Large value for non-default columns
  std::string large_attr1_value = GenerateLargeValue(150);
  std::string large_attr2_value = GenerateLargeValue(200);
  // Small value that stays inline
  std::string small_value = GenerateSmallValue();

  // Entity 1: Large default column + large attribute
  WideColumns columns1{{kDefaultWideColumnName, large_default_value},
                       {"attr1", large_attr1_value}};

  // Entity 2: No default column, only large attributes
  WideColumns columns2{{"attr1", large_attr1_value},
                       {"attr2", large_attr2_value}};

  // Entity 3: Mixed - small default, large attribute
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"attr1", large_attr1_value}};

  // Write entities
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));

  // Verify from memtable
  auto verify = [&]() {
    // Verify entity 1
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                               &result));
      ASSERT_EQ(result.columns(), columns1);

      // Also verify Get returns the default column value
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
      ASSERT_EQ(value, large_default_value);
    }

    // Verify entity 2
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                               &result));
      ASSERT_EQ(result.columns(), columns2);

      // Get should return empty for entity without default column
      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key2, &value));
      ASSERT_TRUE(value.empty());
    }

    // Verify entity 3
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                               &result));
      ASSERT_EQ(result.columns(), columns3);

      PinnableSlice value;
      ASSERT_OK(
          db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key3, &value));
      ASSERT_EQ(value, small_value);
    }

    // Verify via iterator
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      iter->SeekToFirst();

      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key1);
      ASSERT_EQ(iter->columns(), columns1);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key2);
      ASSERT_EQ(iter->columns(), columns2);

      iter->Next();
      ASSERT_TRUE(iter->Valid());
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key(), key3);
      ASSERT_EQ(iter->columns(), columns3);

      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
    }
  };

  // Verify from memtable
  verify();

  // Flush to create SST files (and potentially blob files)
  ASSERT_OK(Flush());
  verify();

  // Add more data and flush again to create multiple files
  constexpr char key4[] = "entity_key4";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string large_value4_default = GenerateLargeValue(120);
  std::string large_value4_attr1 = GenerateLargeValue(80);
  WideColumns columns4{{kDefaultWideColumnName, large_value4_default},
                       {"attr1", large_value4_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key4,
                           columns4));
  ASSERT_OK(Flush());

  // Compact to trigger blob extraction and GC
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Explicit verification: blob files were actually created after compaction
  {
    // Verify at least one blob file exists
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_GE(blob_files.size(), 1)
        << "Expected at least one blob file after compaction with entity data";

    // Verify using DB property
    uint64_t num_blob_files = 0;
    ASSERT_TRUE(
        db_->GetIntProperty(DB::Properties::kNumBlobFiles, &num_blob_files));
    ASSERT_GE(num_blob_files, 1)
        << "kNumBlobFiles property should report at least one blob file";

    // Verify blob file size is non-zero (data was written)
    uint64_t total_blob_file_size = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalBlobFileSize,
                                    &total_blob_file_size));
    ASSERT_GT(total_blob_file_size, 0)
        << "Total blob file size should be greater than zero";

    // Verify blob statistics - confirm data was written to blob files
    uint64_t blob_bytes_written =
        options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_WRITTEN);
    ASSERT_GT(blob_bytes_written, 0)
        << "Expected non-zero bytes written to blob files";
  }

  // Verify original entities after compaction (verify() only checks key1-3,
  // so we verify them individually since key4 was added)
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key1,
                             &result));
    ASSERT_EQ(result.columns(), columns1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key2,
                             &result));
    ASSERT_EQ(result.columns(), columns2);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key3,
                             &result));
    ASSERT_EQ(result.columns(), columns3);
  }

  // Verify key4 after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key4,
                             &result));
    ASSERT_EQ(result.columns(), columns4);
  }

  // Multiple flush/compaction cycles
  for (int cycle = 0; cycle < 3; ++cycle) {
    std::string cycle_key = "cycle_key" + std::to_string(cycle);
    // Store large values in persistent strings to avoid dangling Slice
    // references
    std::string cycle_default_value = GenerateLargeValue(100);
    std::string cycle_attr_value = GenerateLargeValue(50);
    WideColumns cycle_columns{{kDefaultWideColumnName, cycle_default_value},
                              {"cycle_attr", cycle_attr_value}};
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             cycle_key, cycle_columns));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // Verify
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             cycle_key, &result));
    ASSERT_EQ(result.columns(), cycle_columns);
  }

  Close();
}

// Test 2: Entity blob GC respects snapshots - blobs referenced by snapshotted
// entities should not be GC'd
TEST_F(DBBlobIndexTest, EntityBlobGCWithSnapshot) {
  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  DestroyAndReopen(options);

  constexpr char key[] = "snapshot_key";
  std::string original_value = GenerateLargeValue(100);
  std::string updated_value = GenerateLargeValue(150);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string original_attr1_value = GenerateLargeValue(80);
  std::string updated_attr1_value = GenerateLargeValue(120);

  WideColumns original_columns{{kDefaultWideColumnName, original_value},
                               {"attr1", original_attr1_value}};
  WideColumns updated_columns{{kDefaultWideColumnName, updated_value},
                              {"attr1", updated_attr1_value}};

  // Write original entity
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           original_columns));
  ASSERT_OK(Flush());

  // Take a snapshot
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_NE(snapshot, nullptr);

  // Update the entity with new values
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                           updated_columns));
  ASSERT_OK(Flush());

  // Compact - this should trigger GC, but the old blobs should be preserved
  // due to the snapshot
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify current value
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), updated_columns);
  }

  // Verify snapshot still sees original value
  {
    ReadOptions read_options;
    read_options.snapshot = snapshot;

    PinnableWideColumns result;
    ASSERT_OK(
        db_->GetEntity(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result.columns(), original_columns);

    // Also verify Get with snapshot
    PinnableSlice value;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &value));
    ASSERT_EQ(value, original_value);
  }

  // Release snapshot and compact again - now old blobs can be GC'd
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify current value still accessible
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), updated_columns);
  }

  Close();
}

// Test 3: DB recovery replays unflushed WAL entries for entities with blob
// columns.
TEST_F(DBBlobIndexTest, EntityBlobRecoveryReplaysUnflushedWAL) {
  Options options = GetBlobTestOptions();
  options.avoid_flush_during_shutdown = true;

  DestroyAndReopen(options);

  // Write entities with various column configurations
  constexpr char key1[] = "recovery_key1";
  constexpr char key2[] = "recovery_key2";
  constexpr char key3[] = "recovery_key3";

  std::string large_value1 = GenerateLargeValue(100);
  std::string large_value2 = GenerateLargeValue(150);
  std::string small_value = GenerateSmallValue();
  // Store large values in persistent strings to avoid dangling Slice references
  std::string columns1_attr1 = GenerateLargeValue(80);
  std::string columns3_attr1 = GenerateLargeValue(200);

  WideColumns columns1{{kDefaultWideColumnName, large_value1},
                       {"attr1", columns1_attr1}};
  WideColumns columns2{{"attr1", large_value2}, {"attr2", small_value}};
  WideColumns columns3{{kDefaultWideColumnName, small_value},
                       {"attr1", columns3_attr1}};

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key1,
                           columns1));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key2,
                           columns2));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key3,
                           columns3));
  ASSERT_OK(Flush());

  // Compact to ensure blob extraction
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Write more data to WAL (not flushed) so recovery has live memtable data to
  // replay.
  constexpr char key4[] = "recovery_key4";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string columns4_default = GenerateLargeValue(120);
  WideColumns columns4{{kDefaultWideColumnName, columns4_default},
                       {"attr1", "unflushed_attr"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key4,
                           columns4));

  // Skip shutdown flush so key4 stays in WAL and must be recovered on reopen.
  Close();
  Reopen(options);

  // Verify all data is recovered correctly
  auto verify_entity = [&](const std::string& key,
                           const WideColumns& expected) {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), expected);
  };

  verify_entity(key1, columns1);
  verify_entity(key2, columns2);
  verify_entity(key3, columns3);
  verify_entity(key4, columns4);

  // Verify via Get for entities with default column
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key1, &value));
    ASSERT_EQ(value, large_value1);
  }

  // Verify via iterator
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      count++;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, 4);
  }

  // Add another unflushed entity so reopening with
  // `avoid_flush_during_recovery` also has WAL-backed recovery work to do.
  constexpr char key5[] = "recovery_key5";
  std::string columns5_default = GenerateLargeValue(140);
  WideColumns columns5{{kDefaultWideColumnName, columns5_default},
                       {"attr1", "recovered_with_avoid_flush"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key5,
                           columns5));

  Close();
  options.avoid_flush_during_recovery = true;
  Reopen(options);

  // All data should still be accessible
  verify_entity(key1, columns1);
  verify_entity(key2, columns2);
  verify_entity(key3, columns3);
  verify_entity(key4, columns4);
  verify_entity(key5, columns5);

  Close();
}

// Test 5: Entities with blob columns work correctly mixed with regular Put
// operations that also use blobs
TEST_F(DBBlobIndexTest, EntityBlobMixedWithRegularPut) {
  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  DestroyAndReopen(options);

  // Regular Put with large value (becomes blob)
  constexpr char put_key1[] = "put_key1";
  std::string put_value1 = GenerateLargeValue(100);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key1,
                     put_value1));

  // Entity with large columns
  constexpr char entity_key1[] = "entity_key1";
  std::string entity_default = GenerateLargeValue(120);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string entity_columns1_attr1 = GenerateLargeValue(80);
  WideColumns entity_columns1{{kDefaultWideColumnName, entity_default},
                              {"attr1", entity_columns1_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key1, entity_columns1));

  // Regular Put with small value (stays inline)
  constexpr char put_key2[] = "put_key2";
  std::string put_value2 = GenerateSmallValue();
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key2,
                     put_value2));

  // Entity without default column
  constexpr char entity_key2[] = "entity_key2";
  // Store large values in persistent strings to avoid dangling Slice references
  std::string entity_columns2_attr1 = GenerateLargeValue(90);
  std::string entity_columns2_attr2 = GenerateLargeValue(110);
  WideColumns entity_columns2{{"attr1", entity_columns2_attr1},
                              {"attr2", entity_columns2_attr2}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key2, entity_columns2));

  // More regular Put
  constexpr char put_key3[] = "put_key3";
  std::string put_value3 = GenerateLargeValue(200);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key3,
                     put_value3));

  // Flush
  ASSERT_OK(Flush());

  // Verify all values
  auto verify = [&]() {
    // Regular Put values
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1,
                         &value));
      ASSERT_EQ(value, put_value1);
    }
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key2,
                         &value));
      ASSERT_EQ(value, put_value2);
    }
    {
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key3,
                         &value));
      ASSERT_EQ(value, put_value3);
    }

    // Entity values
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               entity_key1, &result));
      ASSERT_EQ(result.columns(), entity_columns1);

      // Get on entity returns default column
      PinnableSlice value;
      ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), entity_key1,
                         &value));
      ASSERT_EQ(value, entity_default);
    }
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               entity_key2, &result));
      ASSERT_EQ(result.columns(), entity_columns2);
    }

    // GetEntity on regular Put returns single default column
    {
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                               put_key1, &result));
      ASSERT_EQ(result.columns().size(), 1);
      ASSERT_EQ(result.columns()[0].name(), kDefaultWideColumnName);
      ASSERT_EQ(result.columns()[0].value(), put_value1);
    }

    // Iterator should see all keys in order
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      std::vector<std::string> expected_keys = {entity_key1, entity_key2,
                                                put_key1, put_key2, put_key3};
      int idx = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ASSERT_OK(iter->status());
        ASSERT_LT(idx, static_cast<int>(expected_keys.size()));
        ASSERT_EQ(iter->key(), expected_keys[idx]);
        idx++;
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(idx, static_cast<int>(expected_keys.size()));
    }
  };

  verify();

  // Add more data and compact
  constexpr char put_key4[] = "put_key4";
  std::string put_value4 = GenerateLargeValue(150);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key4,
                     put_value4));

  constexpr char entity_key3[] = "entity_key3";
  // Store values in persistent strings to avoid dangling Slice references
  std::string entity_columns3_default = GenerateLargeValue(130);
  std::string entity_columns3_attr1 = GenerateSmallValue();
  WideColumns entity_columns3{{kDefaultWideColumnName, entity_columns3_default},
                              {"attr1", entity_columns3_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key3, entity_columns3));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify original entries after compaction (verify() only checks the original
  // 5 keys, so we verify them individually since put_key4 and entity_key3 were
  // added)
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1, &value));
    ASSERT_EQ(value, put_value1);
  }
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key2, &value));
    ASSERT_EQ(value, put_value2);
  }
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key3, &value));
    ASSERT_EQ(value, put_value3);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key1, &result));
    ASSERT_EQ(result.columns(), entity_columns1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key2, &result));
    ASSERT_EQ(result.columns(), entity_columns2);
  }

  // Verify new entries after compaction
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key4, &value));
    ASSERT_EQ(value, put_value4);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key3, &result));
    ASSERT_EQ(result.columns(), entity_columns3);
  }

  // Update some values and verify GC works correctly
  std::string updated_put_value1 = GenerateLargeValue(180);
  ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), put_key1,
                     updated_put_value1));

  std::string updated_entity_default = GenerateLargeValue(190);
  // Store large values in persistent strings to avoid dangling Slice references
  std::string updated_entity_columns1_attr1 = GenerateLargeValue(85);
  WideColumns updated_entity_columns1{
      {kDefaultWideColumnName, updated_entity_default},
      {"attr1", updated_entity_columns1_attr1}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                           entity_key1, updated_entity_columns1));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify updated values
  {
    PinnableSlice value;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), put_key1, &value));
    ASSERT_EQ(value, updated_put_value1);
  }
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             entity_key1, &result));
    ASSERT_EQ(result.columns(), updated_entity_columns1);
  }

  Close();
}

// Test 6: Verify lazy loading - when a compaction filter only accesses inline
// columns and uses the blob_resolver to check IsBlobColumn(), blob values
// are NOT read from blob files.
class LazyLoadingSmallColumnFilter : public CompactionFilter {
 public:
  LazyLoadingSmallColumnFilter(std::atomic<int>* filter_call_count,
                               std::string* last_small_col_value,
                               std::atomic<int>* resolver_check_count)
      : filter_call_count_(filter_call_count),
        last_small_col_value_(last_small_col_value),
        resolver_check_count_(resolver_check_count) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>*
      /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    // Only process wide-column entities
    if (value_type == ValueType::kWideColumnEntity) {
      assert(existing_columns != nullptr);
      (*filter_call_count_)++;

      // Use blob_resolver to check which columns are blobs and only access
      // inline columns. This demonstrates lazy loading - we never call
      // ResolveColumn on blob columns.
      if (blob_resolver != nullptr) {
        for (size_t i = 0; i < existing_columns->size(); ++i) {
          (*resolver_check_count_)++;
          if (!blob_resolver->IsBlobColumn(i)) {
            // Only access inline columns
            const auto& col = (*existing_columns)[i];
            if (col.name() == "small_col") {
              *last_small_col_value_ = col.value().ToString();
            }
          }
        }
      } else {
        // Fallback if no resolver - just access small_col
        for (const auto& col : *existing_columns) {
          if (col.name() == "small_col") {
            *last_small_col_value_ = col.value().ToString();
            break;
          }
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "LazyLoadingSmallColumnFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* last_small_col_value_;
  std::atomic<int>* resolver_check_count_;
};

class LazyLoadingSmallColumnFilterFactory : public CompactionFilterFactory {
 public:
  LazyLoadingSmallColumnFilterFactory(std::atomic<int>* filter_call_count,
                                      std::string* last_small_col_value,
                                      std::atomic<int>* resolver_check_count)
      : filter_call_count_(filter_call_count),
        last_small_col_value_(last_small_col_value),
        resolver_check_count_(resolver_check_count) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<LazyLoadingSmallColumnFilter>(
        filter_call_count_, last_small_col_value_, resolver_check_count_);
  }

  const char* Name() const override {
    return "LazyLoadingSmallColumnFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* last_small_col_value_;
  std::atomic<int>* resolver_check_count_;
};

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterSkipsBlobs) {
  // Test: When a compaction filter uses blob_resolver->IsBlobColumn() to check
  // which columns are blobs and DOESN'T call ResolveColumn on blob columns,
  // no blob I/O should occur.
  //
  // Scenario:
  // - Entity has 2 columns:
  //   1. small_col: 5 bytes (inline, < min_blob_size of 10)
  //   2. large_col: 10KB (stored in blob file)
  // - Compaction filter checks IsBlobColumn() and only accesses small_col
  // - Result: No blob bytes should be read!

  std::atomic<int> filter_call_count{0};
  std::string last_small_col_value;
  std::atomic<int> resolver_check_count{0};

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<LazyLoadingSmallColumnFilterFactory>(
          &filter_call_count, &last_small_col_value, &resolver_check_count);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  ASSERT_OK(options.statistics->Reset());

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(last_small_col_value, small_value);

  // Verify resolver was used to check columns
  ASSERT_GE(resolver_check_count.load(), 2)
      << "Expected resolver to be used to check at least 2 columns";

  // Check blob read statistics after compaction
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);

  // With lazy loading, when filter only accesses inline columns via
  // IsBlobColumn() check, NO blob should be read!
  ASSERT_EQ(blob_bytes_read, 0)
      << "Expected 0 blob bytes read when filter uses IsBlobColumn() "
         "to skip blob columns. Got "
      << blob_bytes_read << " bytes.";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

// Test 7: Verify that when a compaction filter uses the blob_resolver to
// resolve blob columns, the blob values are correctly fetched.
class BlobResolvingFilter : public CompactionFilter {
 public:
  BlobResolvingFilter(std::atomic<int>* filter_call_count,
                      std::string* resolved_large_col_value)
      : filter_call_count_(filter_call_count),
        resolved_large_col_value_(resolved_large_col_value) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>*
      /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type == ValueType::kWideColumnEntity) {
      assert(existing_columns != nullptr);
      (*filter_call_count_)++;

      // Use blob_resolver to resolve the blob column
      if (blob_resolver != nullptr) {
        for (size_t i = 0; i < existing_columns->size(); ++i) {
          const auto& col = (*existing_columns)[i];
          if (col.name() == "large_col" && blob_resolver->IsBlobColumn(i)) {
            Slice resolved_value;
            Status s = blob_resolver->ResolveColumn(i, &resolved_value);
            if (s.ok()) {
              *resolved_large_col_value_ = resolved_value.ToString();
            }
          }
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "BlobResolvingFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* resolved_large_col_value_;
};

class BlobResolvingFilterFactory : public CompactionFilterFactory {
 public:
  BlobResolvingFilterFactory(std::atomic<int>* filter_call_count,
                             std::string* resolved_large_col_value)
      : filter_call_count_(filter_call_count),
        resolved_large_col_value_(resolved_large_col_value) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<BlobResolvingFilter>(filter_call_count_,
                                                 resolved_large_col_value_);
  }

  const char* Name() const override { return "BlobResolvingFilterFactory"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* resolved_large_col_value_;
};

class BlobResolvingErrorIgnoringFilter : public CompactionFilter {
 public:
  BlobResolvingErrorIgnoringFilter(std::atomic<int>* filter_call_count,
                                   std::atomic<int>* resolve_error_count,
                                   std::string* resolve_error_status)
      : filter_call_count_(filter_call_count),
        resolve_error_count_(resolve_error_count),
        resolve_error_status_(resolve_error_status) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (value_type != ValueType::kWideColumnEntity ||
        existing_columns == nullptr || blob_resolver == nullptr) {
      return Decision::kKeep;
    }

    ++(*filter_call_count_);

    for (size_t i = 0; i < existing_columns->size(); ++i) {
      const auto& col = (*existing_columns)[i];
      if (col.name() == "large_col" && blob_resolver->IsBlobColumn(i)) {
        Slice resolved_value;
        const Status s = blob_resolver->ResolveColumn(i, &resolved_value);
        if (!s.ok()) {
          ++(*resolve_error_count_);
          *resolve_error_status_ = s.ToString();
        }
        break;
      }
    }

    // Even if the filter chooses kKeep after seeing a resolver error, the
    // compaction path should still fail and surface that read error.
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override {
    return "BlobResolvingErrorIgnoringFilter";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_error_count_;
  std::string* resolve_error_status_;
};

class BlobResolvingErrorIgnoringFilterFactory : public CompactionFilterFactory {
 public:
  BlobResolvingErrorIgnoringFilterFactory(std::atomic<int>* filter_call_count,
                                          std::atomic<int>* resolve_error_count,
                                          std::string* resolve_error_status)
      : filter_call_count_(filter_call_count),
        resolve_error_count_(resolve_error_count),
        resolve_error_status_(resolve_error_status) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<BlobResolvingErrorIgnoringFilter>(
        filter_call_count_, resolve_error_count_, resolve_error_status_);
  }

  const char* Name() const override {
    return "BlobResolvingErrorIgnoringFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::atomic<int>* resolve_error_count_;
  std::string* resolve_error_status_;
};

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterResolvesBlobs) {
  // Test: When a compaction filter uses blob_resolver->ResolveColumn() to
  // fetch blob values, the values are correctly resolved.
  //
  // Scenario:
  // - Entity has 2 columns:
  //   1. small_col: 5 bytes (inline, < min_blob_size of 10)
  //   2. large_col: 10KB (stored in blob file)
  // - Compaction filter calls ResolveColumn on large_col
  // - Result: Blob bytes should be read and value should match

  std::atomic<int> filter_call_count{0};
  std::string resolved_large_col_value;

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<BlobResolvingFilterFactory>(&filter_call_count,
                                                   &resolved_large_col_value);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  ASSERT_OK(options.statistics->Reset());

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);

  // Verify the resolved value matches the original large value
  ASSERT_EQ(resolved_large_col_value, large_value)
      << "Expected resolved blob value to match original large_value";

  // Check blob read statistics after compaction - should have read the blob
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  ASSERT_GE(blob_bytes_read, large_value.size())
      << "Expected at least " << large_value.size()
      << " bytes read from blob when filter resolves blob column";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

TEST_F(DBBlobIndexTest, EntityBlobLazyLoadingFilterMissingBlobFailsCompaction) {
  // Goal: keep lazy FilterV4 resolver failures aligned with the eager FilterV3
  // path. The filter calls ResolveColumn() on a blob-backed column, then
  // returns kKeep after observing the error. Compaction must still fail and
  // latch bg_error instead of silently keeping the entry.
  std::atomic<int> filter_call_count{0};
  std::atomic<int> resolve_error_count{0};
  std::string resolve_error_status;

  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = false;
  options.paranoid_checks = true;
  options.compaction_filter_factory =
      std::make_shared<BlobResolvingErrorIgnoringFilterFactory>(
          &filter_call_count, &resolve_error_count, &resolve_error_status);

  DestroyAndReopen(options);

  constexpr char key[] = "missing_blob_key_v4";
  const std::string large_value(10 * 1024, 'L');
  WideColumns columns{{"large_col", large_value}, {"small_col", "small"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const auto blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 1U);
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, blob_files.front())));

  const Status status =
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_FALSE(status.ok())
      << "Compaction should fail when FilterV4 lazy blob resolution hits a "
         "read error";
  ASSERT_TRUE(status.IsCorruption() || status.IsIOError() ||
              status.IsNotFound())
      << status.ToString();
  ASSERT_GE(filter_call_count.load(), 1);
  ASSERT_EQ(resolve_error_count.load(), 1);
  ASSERT_FALSE(resolve_error_status.empty());

  const Status bg_error = dbfull()->TEST_GetBGError();
  ASSERT_FALSE(bg_error.ok());
  ASSERT_TRUE(bg_error.IsCorruption() || bg_error.IsIOError() ||
              bg_error.IsNotFound())
      << bg_error.ToString();
  ASSERT_GE(static_cast<int>(bg_error.severity()),
            static_cast<int>(Status::Severity::kHardError));
}

// Test 8: Verify backward compatibility - old filters that don't
// use the blob_resolver still work correctly by accessing inline columns.
// With lazy loading, blob columns will have blob indices (not actual values),
// but inline columns work correctly.
TEST_F(DBBlobIndexTest, EntityBlobCompactionFilterAccessesOnlySmallColumn) {
  // This test verifies backward compatibility: filters that don't use
  // the blob_resolver can still access inline columns correctly.
  //
  // With lazy loading, blob columns contain blob indices (not actual values),
  // but inline columns have the correct values. Since this filter
  // only accesses the inline small_col, it should work correctly.

  std::atomic<int> filter_call_count{0};
  std::string last_small_col_value;
  std::atomic<int> resolver_check_count{0};

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  // Reuse the LazyLoadingSmallColumnFilter which uses IsBlobColumn() check
  // to only access inline columns
  options.compaction_filter_factory =
      std::make_shared<LazyLoadingSmallColumnFilterFactory>(
          &filter_call_count, &last_small_col_value, &resolver_check_count);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";

  // Small value (5 bytes) - will stay inline (min_blob_size is 10)
  std::string small_value(5, 's');

  // Large value (10KB) - will be stored as blob
  std::string large_value(10 * 1024, 'L');

  // Note: WideColumns are stored sorted by column name
  // "large_col" < "small_col" alphabetically
  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};

  // Write the entity
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Reset statistics before compaction
  ASSERT_OK(options.statistics->Reset());

  // Run compaction - this will invoke the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify the filter was called
  ASSERT_GE(filter_call_count.load(), 1);

  // The filter accessed small_col which is inline, so it should get the
  // correct value
  ASSERT_EQ(last_small_col_value, small_value);

  // With lazy loading, no blob bytes should be read since filter doesn't
  // access blob columns
  uint64_t blob_bytes_read =
      options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ);
  ASSERT_EQ(blob_bytes_read, 0)
      << "Expected 0 blob bytes read when filter only accesses inline columns";

  // Verify entity is still intact after compaction
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), columns);
  }

  Close();
}

TEST_F(DBBlobIndexTest, IntegratedBlobIterate) {
  const std::vector<std::vector<std::string>> data = {
      /*00*/ {"Put"},
      /*01*/ {"Put", "Merge", "Merge", "Merge"},
      /*02*/ {"Put"}};

  auto get_key = [](size_t index) { return ("key" + std::to_string(index)); };

  auto get_value = [&](size_t index, size_t version) {
    return get_key(index) + "_value" + std::to_string(version);
  };

  auto check_iterator = [&](Iterator* iterator, const Status& expected_status,
                            const Slice& expected_value) {
    ASSERT_EQ(expected_status, iterator->status());
    if (expected_status.ok()) {
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ(expected_value, iterator->value());
    } else {
      ASSERT_FALSE(iterator->Valid());
    }
  };

  auto verify = [&](size_t index, const Status& expected_status,
                    const Slice& expected_value) {
    // Seek
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->status());
      ASSERT_OK(iterator->Refresh());
      iterator->Seek(get_key(index));
      check_iterator(iterator, expected_status, expected_value);
    }
    // Next
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->Refresh());
      iterator->Seek(get_key(index - 1));
      ASSERT_TRUE(iterator->Valid());
      ASSERT_OK(iterator->status());
      iterator->Next();
      check_iterator(iterator, expected_status, expected_value);
    }
    // SeekForPrev
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      ASSERT_OK(iterator->status());
      ASSERT_OK(iterator->Refresh());
      iterator->SeekForPrev(get_key(index));
      check_iterator(iterator, expected_status, expected_value);
    }
    // Prev
    {
      Iterator* iterator = db_->NewIterator(ReadOptions());
      std::unique_ptr<Iterator> iterator_guard(iterator);
      iterator->Seek(get_key(index + 1));
      ASSERT_TRUE(iterator->Valid());
      ASSERT_OK(iterator->status());
      iterator->Prev();
      check_iterator(iterator, expected_status, expected_value);
    }
  };

  Options options = GetTestOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  DestroyAndReopen(options);

  // fill data
  for (size_t i = 0; i < data.size(); i++) {
    for (size_t j = 0; j < data[i].size(); j++) {
      std::string key = get_key(i);
      std::string value = get_value(i, j);
      if (data[i][j] == "Put") {
        ASSERT_OK(Put(key, value));
        ASSERT_OK(Flush());
      } else if (data[i][j] == "Merge") {
        ASSERT_OK(Merge(key, value));
        ASSERT_OK(Flush());
      }
    }
  }

  std::string expected_value = get_value(1, 0) + "," + get_value(1, 1) + "," +
                               get_value(1, 2) + "," + get_value(1, 3);
  Status expected_status;
  verify(1, expected_status, expected_value);

  // Test DBIter::FindValueForCurrentKeyUsingSeek flow.
  ASSERT_OK(dbfull()->SetOptions(cfh(),
                                 {{"max_sequential_skip_in_iterations", "0"}}));
  verify(1, expected_status, expected_value);
}

// FilterV3-only compaction filter that inspects entity column values.
// Does NOT override FilterV4, so SupportsFilterV4() returns false (default).
// This tests backward compatibility: blob columns should be eagerly resolved
// before the filter sees them, so the filter sees actual blob values (not
// raw BlobIndex bytes).
class FilterV3OnlyEntityFilter : public CompactionFilter {
 public:
  explicit FilterV3OnlyEntityFilter(std::atomic<int>* filter_call_count,
                                    std::string* observed_large_col_value)
      : filter_call_count_(filter_call_count),
        observed_large_col_value_(observed_large_col_value) {}

  Decision FilterV3(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/) const override {
    if (value_type == ValueType::kWideColumnEntity && existing_columns) {
      (*filter_call_count_)++;
      for (const auto& col : *existing_columns) {
        if (col.name() == "large_col") {
          *observed_large_col_value_ = col.value().ToString();
        }
      }
    } else if (value_type == ValueType::kValue && existing_value) {
      (*filter_call_count_)++;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "FilterV3OnlyEntityFilter"; }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_large_col_value_;
};

class FilterV3OnlyEntityFilterFactory : public CompactionFilterFactory {
 public:
  FilterV3OnlyEntityFilterFactory(std::atomic<int>* filter_call_count,
                                  std::string* observed_large_col_value)
      : filter_call_count_(filter_call_count),
        observed_large_col_value_(observed_large_col_value) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<FilterV3OnlyEntityFilter>(
        filter_call_count_, observed_large_col_value_);
  }

  const char* Name() const override {
    return "FilterV3OnlyEntityFilterFactory";
  }

 private:
  std::atomic<int>* filter_call_count_;
  std::string* observed_large_col_value_;
};

TEST_F(DBBlobIndexTest, EntityBlobFilterV3BackwardCompatibility) {
  // Test: A FilterV3-only compaction filter (no FilterV4 override) should see
  // resolved blob values, not raw BlobIndex bytes. The compaction path should
  // eagerly resolve blob columns before calling the filter.

  std::atomic<int> filter_call_count{0};
  std::string observed_large_col_value;

  Options options = GetBlobTestOptions();
  options.statistics = CreateDBStatistics();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<FilterV3OnlyEntityFilterFactory>(
          &filter_call_count, &observed_large_col_value);

  DestroyAndReopen(options);

  constexpr char key[] = "test_key";
  std::string small_value(5, 's');
  // Large value that will be stored as blob (> min_blob_size of 10)
  std::string large_value(10 * 1024, 'L');

  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Compact to trigger the compaction filter
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Filter should have been called
  ASSERT_GE(filter_call_count.load(), 1);

  // The FilterV3 filter should have seen the actual blob value, not raw
  // BlobIndex bytes. BlobIndex bytes would be much shorter than the original
  // value.
  ASSERT_EQ(observed_large_col_value, large_value);
}

TEST_F(DBBlobIndexTest, EntityBlobFilterV3MissingBlobFailsCompaction) {
  std::atomic<int> filter_call_count{0};
  std::string observed_large_col_value;

  Options options = GetBlobTestOptions();
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory =
      std::make_shared<FilterV3OnlyEntityFilterFactory>(
          &filter_call_count, &observed_large_col_value);

  DestroyAndReopen(options);

  constexpr char key[] = "missing_blob_key";
  const std::string large_value(10 * 1024, 'L');
  WideColumns columns{{"large_col", large_value}, {"small_col", "small"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  const auto blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 1U);
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, blob_files.front())));

  const Status status =
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_FALSE(status.ok()) << "Compaction should fail when a FilterV3-only "
                               "filter cannot eagerly resolve a blob-backed "
                               "entity";
  ASSERT_TRUE(status.IsCorruption() || status.IsIOError() ||
              status.IsNotFound())
      << status.ToString();
  ASSERT_EQ(filter_call_count.load(), 0);
  ASSERT_TRUE(observed_large_col_value.empty());
}

// FilterV3-only filter that removes entities with blob columns.
// Tests that kRemove works correctly with eagerly resolved blob values.
class FilterV3RemoveEntityFilter : public CompactionFilter {
 public:
  Decision FilterV3(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/) const override {
    if (value_type == ValueType::kWideColumnEntity && existing_columns) {
      return Decision::kRemove;
    }
    return Decision::kKeep;
  }

  const char* Name() const override { return "FilterV3RemoveEntityFilter"; }
};

class FilterV3RemoveEntityFilterFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<FilterV3RemoveEntityFilter>();
  }

  const char* Name() const override {
    return "FilterV3RemoveEntityFilterFactory";
  }
};

TEST_F(DBBlobIndexTest, EntityBlobFilterV3Remove) {
  // Test: A FilterV3-only filter returning kRemove should correctly delete
  // entities with eagerly resolved blob columns.

  Options options = GetBlobTestOptions();
  options.compaction_filter_factory =
      std::make_shared<FilterV3RemoveEntityFilterFactory>();

  DestroyAndReopen(options);

  constexpr char key[] = "remove_key";
  std::string large_value(10 * 1024, 'R');
  std::string small_value(5, 's');

  WideColumns columns{{"large_col", large_value}, {"small_col", small_value}};
  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());

  // Compact to trigger the filter -- entity should be removed
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Key should be gone
  PinnableSlice result;
  Status s = db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
}

// Compaction filter that drops wide column entities based on a TTL column,
// using FilterV4 with SupportsFilterV4()=true to avoid loading blob values.
class TTLBasedEntityDropFilter : public CompactionFilter {
 public:
  explicit TTLBasedEntityDropFilter(bool enabled) : enabled_(enabled) {}

  Decision FilterV4(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice* /*existing_value*/, const WideColumns* existing_columns,
      std::string* /*new_value*/,
      std::vector<std::pair<std::string, std::string>>* /*new_columns*/,
      std::string* /*skip_until*/,
      WideColumnBlobResolver* blob_resolver = nullptr) const override {
    if (!enabled_) {
      return Decision::kKeep;
    }
    if (value_type != ValueType::kWideColumnEntity || !existing_columns) {
      return Decision::kKeep;
    }
    // Read only the "ttl" column (inline, not a blob) to decide.
    // Skip blob columns entirely -- no blob I/O.
    for (size_t i = 0; i < existing_columns->size(); ++i) {
      if (blob_resolver && blob_resolver->IsBlobColumn(i)) {
        continue;  // skip blob columns
      }
      const auto& col = (*existing_columns)[i];
      if (col.name() == "ttl") {
        // TTL value "expired" means drop
        if (col.value() == "expired") {
          return Decision::kRemove;
        }
      }
    }
    return Decision::kKeep;
  }

  bool SupportsFilterV4() const override { return true; }
  const char* Name() const override { return "TTLBasedEntityDropFilter"; }

  void SetEnabled(bool enabled) { enabled_ = enabled; }

 private:
  bool enabled_;
};

class TTLBasedEntityDropFilterFactory : public CompactionFilterFactory {
 public:
  TTLBasedEntityDropFilterFactory() : enabled_(false) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<TTLBasedEntityDropFilter>(enabled_.load());
  }

  const char* Name() const override {
    return "TTLBasedEntityDropFilterFactory";
  }

  void SetEnabled(bool enabled) {
    enabled_.store(enabled, std::memory_order_relaxed);
  }

 private:
  std::atomic<bool> enabled_;
};

TEST_F(DBBlobIndexTest, PassiveGCForWideColumnEntitiesWithBlobColumns) {
  // Reproduce a production scenario where passive GC fails to remove blob
  // files referenced by dropped wide column entities.
  //
  // Setup:
  //   - Universal compaction, passive GC only (no active GC)
  //   - Small blob_file_size so one flush creates multiple blob files
  //   - PutEntity with wide columns: one large "data" column stored in blob,
  //     one small "ttl" column stored inline
  //   - FilterV4 drops entities based on TTL without loading blob values
  //   - One entity has long TTL (survives), many have "expired" TTL
  //   - After compaction, blob files that only contained dropped entities'
  //     blobs should become all-garbage and be removed
  //
  // Expected behavior: blob files with garbage >= total and empty linked_ssts
  // should be dropped by passive GC.

  auto filter_factory = std::make_shared<TTLBasedEntityDropFilterFactory>();

  Options options = GetBlobTestOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.blob_file_size = 500;  // Small: ~1 entity per blob file
  options.enable_blob_garbage_collection = false;
  options.compaction_filter_factory = filter_factory;
  options.num_levels = 7;

  DestroyAndReopen(options);

  // Large value for the "data" column (will be stored as blob)
  const std::string large_data(200, 'D');
  const std::string small_ttl_alive = "alive";
  const std::string small_ttl_expired = "expired";

  // Disable filter for initial load.
  filter_factory->SetEnabled(false);

  // Insert entities: one survivor, then many expired.
  // Sorted key order determines blob file assignment.
  // Survivor goes to the first blob file.
  ASSERT_OK(db_->PutEntity(
      WriteOptions(), db_->DefaultColumnFamily(), "aaa_survivor",
      WideColumns{{"data", large_data}, {"ttl", small_ttl_alive}}));

  // Expired entities fill subsequent blob files
  for (int i = 0; i < 8; i++) {
    char key[32];
    snprintf(key, sizeof(key), "drop_%02d", i);
    ASSERT_OK(db_->PutEntity(
        WriteOptions(), db_->DefaultColumnFamily(), key,
        WideColumns{{"data", large_data}, {"ttl", small_ttl_expired}}));
  }
  ASSERT_OK(Flush());

  // First compaction without filter (all entities survive)
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  const auto blob_files_initial = GetBlobFileNumbers();
  ASSERT_GE(blob_files_initial.size(), 3)
      << "Expected multiple blob files from entities with large columns";

  // Enable filter and trigger second compaction with overlapping data.
  filter_factory->SetEnabled(true);
  ASSERT_OK(db_->PutEntity(
      WriteOptions(), db_->DefaultColumnFamily(), "drop_00",
      WideColumns{{"data", large_data}, {"ttl", small_ttl_expired}}));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  const auto blob_files_after = GetBlobFileNumbers();

  // Verify survivor is readable.
  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             "aaa_survivor", &result));
    ASSERT_EQ(result.columns().size(), 2);
  }

  // Verify dropped entities are gone.
  for (int i = 0; i < 8; i++) {
    char key[32];
    snprintf(key, sizeof(key), "drop_%02d", i);
    PinnableSlice val;
    ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &val)
                    .IsNotFound())
        << "Expected " << key << " to be dropped by TTL filter";
  }

  // The key assertion: blob files that only contained dropped entities'
  // blobs should be removed. The count should decrease.
  ASSERT_LT(blob_files_after.size(), blob_files_initial.size())
      << "Passive GC failed: blob files not removed after entities dropped. "
      << "Before: " << blob_files_initial.size()
      << ", After: " << blob_files_after.size()
      << ". BlobGarbageMeter likely does not track kTypeWideColumnEntity.";

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
