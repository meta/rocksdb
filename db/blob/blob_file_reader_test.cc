//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <atomic>
#include <cassert>
#include <limits>
#include <string>
#include <utility>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/blob/blob_relocation_file.h"
#include "db/blob/blob_relocation_gc.h"
#include "db/blob/blob_source.h"
#include "db/dbformat.h"
#include "env/composite_env_wrapper.h"
#include "env/emulated_clock.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Creates a test blob file with `num` blobs in it.
void WriteBlobFile(const ImmutableOptions& immutable_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const std::vector<Slice>& keys,
                   const std::vector<Slice>& blobs, CompressionType compression,
                   std::vector<uint64_t>& blob_offsets,
                   std::vector<uint64_t>& blob_sizes) {
  assert(!immutable_options.cf_paths.empty());
  size_t num = keys.size();
  assert(num == blobs.size());
  assert(num == blob_offsets.size());
  assert(num == blob_sizes.size());

  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                            FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, FileOptions(), immutable_options.clock));

  constexpr Statistics* statistics = nullptr;
  constexpr bool use_fsync = false;
  constexpr bool do_flush = false;

  BlobLogWriter blob_log_writer(std::move(file_writer), immutable_options.clock,
                                statistics, blob_file_number, use_fsync,
                                do_flush);

  BlobLogHeader header(column_family_id, compression, has_ttl,
                       expiration_range_header);

  ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));

  std::vector<GrowableBuffer> compressed_blobs(num);
  std::vector<Slice> blobs_to_write(num);
  if (kNoCompression == compression) {
    for (size_t i = 0; i < num; ++i) {
      blobs_to_write[i] = blobs[i];
      blob_sizes[i] = blobs[i].size();
    }
  } else {
    auto compressor =
        GetBuiltinV2CompressionManager()->GetCompressor({}, compression);

    for (size_t i = 0; i < num; ++i) {
      ASSERT_OK(LegacyForceBuiltinCompression(*compressor,
                                              /*working_area=*/nullptr,
                                              blobs[i], &compressed_blobs[i]));
      blobs_to_write[i] = compressed_blobs[i];
      blob_sizes[i] = compressed_blobs[i].size();
    }
  }

  for (size_t i = 0; i < num; ++i) {
    uint64_t key_offset = 0;
    ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), keys[i],
                                        blobs_to_write[i], &key_offset,
                                        &blob_offsets[i]));
  }

  BlobLogFooter footer;
  footer.blob_count = num;
  footer.expiration_range = expiration_range_footer;

  std::string checksum_method;
  std::string checksum_value;
  ASSERT_OK(blob_log_writer.AppendFooter(WriteOptions(), footer,
                                         &checksum_method, &checksum_value));
}

// Creates a test blob file with a single blob in it. Note: this method
// makes it possible to test various corner cases by allowing the caller
// to specify the contents of various blob file header/footer fields.
void WriteBlobFile(const ImmutableOptions& immutable_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const Slice& key,
                   const Slice& blob, CompressionType compression,
                   uint64_t* blob_offset, uint64_t* blob_size) {
  std::vector<Slice> keys{key};
  std::vector<Slice> blobs{blob};
  std::vector<uint64_t> blob_offsets{0};
  std::vector<uint64_t> blob_sizes{0};
  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, keys, blobs, compression, blob_offsets,
                blob_sizes);
  if (blob_offset) {
    *blob_offset = blob_offsets[0];
  }
  if (blob_size) {
    *blob_size = blob_sizes[0];
  }
}

struct RelocationTestRecord {
  uint64_t origin_file_number;
  uint64_t original_value_offset;
  Slice stored_payload;
  CompressionType compression;
};

void WriteRelocationFile(const ImmutableOptions& immutable_options,
                         const MutableCFOptions& mutable_cf_options,
                         const BlockBasedTableOptions& table_options,
                         uint32_t column_family_id, uint64_t blob_file_number,
                         const std::vector<RelocationTestRecord>& records,
                         BlobRelocationFileMetadata* metadata,
                         uint64_t* file_size) {
  assert(metadata != nullptr);
  assert(file_size != nullptr);
  const std::string file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(immutable_options.fs->NewWritableFile(file_path, FileOptions(),
                                                  &file, /*dbg=*/nullptr));
  std::unique_ptr<WritableFileWriter> writer =
      std::make_unique<WritableFileWriter>(
          std::move(file), file_path, FileOptions(), immutable_options.clock);

  InternalTblPropCollFactories collector_factories;
  const InternalKeyComparator comparator(BytewiseComparator());
  const ReadOptions read_options;
  const WriteOptions write_options;
  SstFileWriterEmbeddedBlobOptions embedded_options;
  embedded_options.min_blob_size = 0;
  BlockBasedTableFactory factory(MakeBlobRelocationTableOptions(table_options));
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(
          immutable_options, mutable_cf_options, read_options, write_options,
          comparator, &collector_factories, kNoCompression,
          CompressionOptions(), column_family_id, kDefaultColumnFamilyName,
          /*level=*/-1, kUnknownNewestKeyTime,
          /*is_bottommost=*/false, TableFileCreationReason::kMisc,
          /*oldest_key_time=*/0, /*file_creation_time=*/0,
          /*db_id=*/"test-db", /*db_session_id=*/"test-session",
          /*target_file_size=*/0, blob_file_number,
          /*last_level_inclusive_max_seqno_threshold=*/kMaxSequenceNumber,
          /*db_name=*/Slice(),
          /*is_remote_compaction=*/false, &embedded_options),
      writer.get()));
  ASSERT_NE(builder, nullptr);
  ASSERT_OK(builder->StartBlobRelocationFile());
  for (const RelocationTestRecord& record : records) {
    ASSERT_OK(builder->AddBlobRelocation(
        record.origin_file_number, record.original_value_offset,
        record.stored_payload, record.compression));
  }
  ASSERT_OK(builder->Finish());
  const TableProperties properties = builder->GetTableProperties();
  ASSERT_EQ(properties.comparator_name, BytewiseComparator()->Name());
  ASSERT_FALSE(properties.user_defined_timestamps_persisted);
  ASSERT_OK(builder->GetBlobRelocationFileMetadata(metadata));
  ASSERT_OK(writer->Close(IOOptions(), /*dbg=*/nullptr));
  ASSERT_OK(immutable_options.fs->GetFileSize(file_path, IOOptions(), file_size,
                                              /*dbg=*/nullptr));
}

}  // anonymous namespace

TEST(BlobRelocationDirectoryTest, ValidatesOrderingAndChecksums) {
  BlobRelocationDirectoryBuilder builder;
  ASSERT_OK(builder.Add(/*origin_file_number=*/10,
                        /*original_value_offset=*/2,
                        /*relocated_value_offset=*/100,
                        /*stored_value_size=*/3, kNoCompression));
  ASSERT_OK(builder.Add(/*origin_file_number=*/10,
                        /*original_value_offset=*/7,
                        /*relocated_value_offset=*/108,
                        /*stored_value_size=*/4, kNoCompression));
  ASSERT_OK(builder.Add(/*origin_file_number=*/20,
                        /*original_value_offset=*/1,
                        /*relocated_value_offset=*/117,
                        /*stored_value_size=*/5, kNoCompression));
  BlobRelocationDirectory directory;
  ASSERT_OK(builder.Finish(&directory));
  ASSERT_EQ(directory.origins().size(), 2U);
  ASSERT_EQ(directory.total_entries(), 3U);
  ASSERT_EQ(directory.total_record_bytes(),
            12U + 3U * kSimpleGen2BlobTrailerSize);
  ASSERT_NE(directory.Find(10), nullptr);
  ASSERT_EQ(directory.Find(99), nullptr);
  ASSERT_OK(directory.ValidateForFileSize(kBlobRelocationFilePrefixSize +
                                          directory.total_record_bytes()));
  ASSERT_TRUE(directory
                  .ValidateForFileSize(kBlobRelocationFilePrefixSize +
                                       directory.total_record_bytes() - 1)
                  .IsCorruption());

  std::string encoded;
  directory.EncodeTo(&encoded);
  BlobRelocationDirectory decoded;
  ASSERT_OK(BlobRelocationDirectory::DecodeFrom(encoded, &decoded));
  ASSERT_EQ(decoded.directory_digest(), directory.directory_digest());

  // Two distinct offsets cannot fit in the single-point inclusive range.
  std::string impossible_cardinality = encoded;
  constexpr size_t kDirectoryHeaderSize = 40;
  constexpr size_t kLastOffsetField = 16;
  EncodeFixed64(
      impossible_cardinality.data() + kDirectoryHeaderSize + kLastOffsetField,
      /*last_original_offset=*/2);
  EncodeFixed32(
      impossible_cardinality.data() + 32,
      crc32c::Value(impossible_cardinality.data() + kDirectoryHeaderSize,
                    impossible_cardinality.size() - kDirectoryHeaderSize));
  EncodeFixed32(impossible_cardinality.data() + 36,
                crc32c::Value(impossible_cardinality.data(), 36));
  ASSERT_TRUE(
      BlobRelocationDirectory::DecodeFrom(impossible_cardinality, &decoded)
          .IsCorruption());

  std::string checksum_corruption = encoded;
  checksum_corruption.back() ^= 1;
  ASSERT_TRUE(BlobRelocationDirectory::DecodeFrom(checksum_corruption, &decoded)
                  .IsCorruption());

  BlobRelocationDirectoryBuilder duplicate;
  ASSERT_OK(duplicate.Add(10, 2, 100, 3, kNoCompression));
  ASSERT_TRUE(duplicate.Add(10, 2, 108, 4, kNoCompression).IsInvalidArgument());
  BlobRelocationDirectoryBuilder unsorted;
  ASSERT_OK(unsorted.Add(20, 1, 100, 3, kNoCompression));
  ASSERT_TRUE(unsorted.Add(10, 2, 108, 4, kNoCompression).IsInvalidArgument());
  BlobRelocationDirectoryBuilder unsupported;
  ASSERT_TRUE(
      unsupported.Add(10, 2, 100, 3, kCustomCompression80).IsInvalidArgument());

  // A rejected overflowing addition must not change the current origin.
  BlobRelocationDirectoryBuilder overflow;
  ASSERT_OK(overflow.Add(
      10, 2, 100,
      std::numeric_limits<uint64_t>::max() - kSimpleGen2BlobTrailerSize,
      kNoCompression));
  ASSERT_TRUE(overflow.Add(10, 7, 108, 1, kNoCompression).IsInvalidArgument());
  BlobRelocationDirectory after_overflow;
  ASSERT_OK(overflow.Finish(&after_overflow));
  ASSERT_EQ(after_overflow.total_entries(), 1U);

  EXPECT_LT(EncodeBlobRelocationKey(10, 99), EncodeBlobRelocationKey(11, 1));
  uint64_t origin_file_number = 0;
  uint64_t original_value_offset = 0;
  ASSERT_OK(DecodeBlobRelocationKey(EncodeBlobRelocationKey(10, 99),
                                    &origin_file_number,
                                    &original_value_offset));
  EXPECT_EQ(origin_file_number, 10U);
  EXPECT_EQ(original_value_offset, 99U);
}

TEST(BlobRelocationLiveSetTest, DeduplicatesAndRejectsConflicts) {
  const auto make_reference = [](uint64_t offset, uint64_t stored_value_size,
                                 uint64_t record_bytes,
                                 uint32_t stable_checksum) {
    BlobReference reference;
    reference.encoding_class = BlobReferenceEncodingClass::kFeatureIdentity;
    reference.generation_file_number = 10;
    reference.origin_file_number = 10;
    reference.original_value_offset = offset;
    reference.stored_value_size = stored_value_size;
    reference.record_bytes = record_bytes;
    reference.compression = kNoCompression;
    reference.stable_checksum_version = BlobIndex::kStableChecksumVersion1;
    reference.stable_checksum = stable_checksum;
    return reference;
  };

  const BlobReference first = make_reference(/*offset=*/100,
                                             /*stored_value_size=*/20,
                                             /*record_bytes=*/40,
                                             /*stable_checksum=*/1);
  const BlobReference second = make_reference(/*offset=*/200,
                                              /*stored_value_size=*/30,
                                              /*record_bytes=*/50,
                                              /*stable_checksum=*/2);
  BlobRelocationLiveSet live_set{std::shared_ptr<CacheReservationManager>()};
  ASSERT_OK(live_set.Add({second, first}));
  ASSERT_OK(live_set.Add({first}));
  ASSERT_OK(live_set.Finalize(BlobRelocationRouteSource{10, 10}));
  ASSERT_EQ(live_set.references().size(), 2U);
  EXPECT_EQ(live_set.references()[0].original_value_offset, 100U);
  EXPECT_EQ(live_set.references()[1].original_value_offset, 200U);
  EXPECT_EQ(live_set.live_stored_value_bytes(), 50U);
  EXPECT_EQ(live_set.live_record_bytes(), 90U);

  BlobReference conflict = first;
  ++conflict.stable_checksum;
  BlobRelocationLiveSet conflicting_set{
      std::shared_ptr<CacheReservationManager>()};
  ASSERT_OK(conflicting_set.Add({first, conflict}));
  ASSERT_TRUE(conflicting_set.Finalize(BlobRelocationRouteSource{10, 10})
                  .IsCorruption());
}

TEST(BlobRelocationPackingTest, FirstFitDecreasingIsDeterministicAndWhole) {
  const std::vector<BlobRelocationPackingItem> items{
      {/*origin_file_number=*/30, /*source_generation_file_number=*/300,
       /*estimated_bytes=*/40},
      {/*origin_file_number=*/10, /*source_generation_file_number=*/100,
       /*estimated_bytes=*/60},
      {/*origin_file_number=*/20, /*source_generation_file_number=*/200,
       /*estimated_bytes=*/40},
      {/*origin_file_number=*/40, /*source_generation_file_number=*/400,
       /*estimated_bytes=*/30}};
  std::vector<BlobRelocationPackingBin> bins;
  ASSERT_OK(PlanBlobRelocationPacking(
      items, /*target_output_bytes=*/100, /*hard_output_bytes=*/120,
      /*max_origins_per_output=*/3, /*max_outputs=*/3, &bins));
  ASSERT_EQ(bins.size(), 2U);
  EXPECT_EQ(bins[0].item_indexes, (std::vector<size_t>{1, 2}));
  EXPECT_EQ(bins[0].estimated_bytes, 100U);
  EXPECT_EQ(bins[1].item_indexes, (std::vector<size_t>{0, 3}));
  EXPECT_EQ(bins[1].estimated_bytes, 70U);

  const std::vector<BlobRelocationPackingItem> duplicate_origins{
      {/*origin_file_number=*/10, /*source_generation_file_number=*/100,
       /*estimated_bytes=*/60},
      {/*origin_file_number=*/10, /*source_generation_file_number=*/200,
       /*estimated_bytes=*/40}};
  ASSERT_OK(PlanBlobRelocationPacking(
      duplicate_origins, /*target_output_bytes=*/100,
      /*hard_output_bytes=*/120, /*max_origins_per_output=*/3,
      /*max_outputs=*/3, &bins));
  ASSERT_EQ(bins.size(), 2U);

  ASSERT_TRUE(PlanBlobRelocationPacking(items, /*target_output_bytes=*/100,
                                        /*hard_output_bytes=*/50,
                                        /*max_origins_per_output=*/3,
                                        /*max_outputs=*/3, &bins)
                  .IsInvalidArgument());
  ASSERT_TRUE(PlanBlobRelocationPacking(items, /*target_output_bytes=*/50,
                                        /*hard_output_bytes=*/100,
                                        /*max_origins_per_output=*/1,
                                        /*max_outputs=*/3, &bins)
                  .IsMemoryLimit());

  const std::vector<BlobRelocationPackingItem> oversized_partition{
      {/*origin_file_number=*/10, /*source_generation_file_number=*/100,
       /*estimated_bytes=*/121}};
  ASSERT_TRUE(PlanBlobRelocationPacking(
                  oversized_partition, /*target_output_bytes=*/100,
                  /*hard_output_bytes=*/120, /*max_origins_per_output=*/3,
                  /*max_outputs=*/3, &bins)
                  .IsMemoryLimit());
}

class BlobFileReaderTest : public testing::Test {
 protected:
  BlobFileReaderTest() { mock_env_.reset(MockEnv::Create(Env::Default())); }
  std::unique_ptr<Env> mock_env_;
};

TEST_F(BlobFileReaderTest, RejectsOversizedCompressedBlobBeforeAllocation) {
  if (!CompressionTypeSupported(kZlibCompression)) {
    ROCKSDB_GTEST_SKIP("Zlib is required for this test");
    return;
  }
  std::string stored_value;
  PutVarint64(&stored_value, std::numeric_limits<uint64_t>::max());
  const std::shared_ptr<Decompressor> decompressor =
      GetBuiltinV2CompressionManager()->GetDecompressorOptimizeFor(
          kZlibCompression);
  std::unique_ptr<BlobContents> result;
  ASSERT_TRUE(UncompressBlobContentsIfNeeded(
                  stored_value, kZlibCompression, decompressor.get(),
                  /*allocator=*/nullptr, mock_env_->GetSystemClock().get(),
                  /*statistics=*/nullptr, &result)
                  .IsCorruption());
  ASSERT_EQ(result, nullptr);
}

TEST_F(BlobFileReaderTest,
       RejectsUnsupportedRelocationComparatorBeforeWritingPrefix) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileReaderTest_RejectsUnsupportedRelocationComparator"),
      0);
  options.enable_blob_files = true;
  BlockBasedTableOptions table_options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);

  const auto verify_rejected = [&](const Comparator* user_comparator,
                                   const std::string& db_id,
                                   const std::string& db_session_id,
                                   uint64_t file_number) {
    const std::string file_path =
        BlobFileName(immutable_options.cf_paths.front().path, file_number);
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(immutable_options.fs->NewWritableFile(file_path, FileOptions(),
                                                    &file, /*dbg=*/nullptr));
    std::unique_ptr<WritableFileWriter> writer =
        std::make_unique<WritableFileWriter>(
            std::move(file), file_path, FileOptions(), immutable_options.clock);
    InternalTblPropCollFactories collector_factories;
    const InternalKeyComparator comparator(user_comparator);
    const ReadOptions read_options;
    const WriteOptions write_options;
    SstFileWriterEmbeddedBlobOptions embedded_options;
    embedded_options.min_blob_size = 0;
    BlockBasedTableFactory factory(
        MakeBlobRelocationTableOptions(table_options));
    std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
        TableBuilderOptions(
            immutable_options, mutable_cf_options, read_options, write_options,
            comparator, &collector_factories, kNoCompression,
            CompressionOptions(), /*column_family_id=*/1,
            kDefaultColumnFamilyName, /*level=*/-1, kUnknownNewestKeyTime,
            /*is_bottommost=*/false, TableFileCreationReason::kMisc,
            /*oldest_key_time=*/0, /*file_creation_time=*/0, db_id,
            db_session_id,
            /*target_file_size=*/0, file_number,
            /*last_level_inclusive_max_seqno_threshold=*/kMaxSequenceNumber,
            /*db_name=*/Slice(), /*is_remote_compaction=*/false,
            &embedded_options),
        writer.get()));
    ASSERT_NE(builder, nullptr);
    ASSERT_TRUE(builder->StartBlobRelocationFile().IsInvalidArgument());
    builder->Abandon();
    ASSERT_OK(writer->Close(IOOptions(), /*dbg=*/nullptr));
    uint64_t file_size = 1;
    ASSERT_OK(immutable_options.fs->GetFileSize(file_path, IOOptions(),
                                                &file_size, /*dbg=*/nullptr));
    EXPECT_EQ(file_size, 0U);
  };

  verify_rejected(ReverseBytewiseComparator(), "test-db", "test-session",
                  /*file_number=*/301);
  verify_rejected(test::BytewiseComparatorWithU64TsWrapper(), "test-db",
                  "test-session",
                  /*file_number=*/302);
  verify_rejected(BytewiseComparator(), "test-db", /*db_session_id=*/"",
                  /*file_number=*/303);
}

TEST_F(BlobFileReaderTest,
       RelocationFilePersistsItsDedicatedComparatorMetadata) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileReaderTest_RelocationFileDedicatedComparatorMetadata"),
      0);
  options.enable_blob_files = true;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = true;
  BlockBasedTableOptions table_options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  const std::string value = "value";
  const std::vector<RelocationTestRecord> records{{/*origin_file_number=*/100,
                                                   /*original_value_offset=*/7,
                                                   value, kNoCompression}};
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      /*column_family_id=*/1, /*blob_file_number=*/304, records,
                      &metadata, &file_size);
  EXPECT_GT(file_size, 0U);
}

TEST_F(BlobFileReaderTest, RelocationFileMultiOriginCompressedRoundTrip) {
  if (!CompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_SKIP("ZSTD is required for this test");
    return;
  }
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileReaderTest_RelocationFileMultiOriginCompressedRoundTrip"),
      0);
  options.enable_blob_files = true;
  BlockBasedTableOptions table_options;
  table_options.format_version = 7;
  table_options.checksum = kCRC32c;
  table_options.block_cache = NewLRUCache(1 << 20);
  options.blob_cache = NewLRUCache(1 << 20);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t relocation_file_number = 300;
  constexpr uint64_t first_origin = 100;
  constexpr uint64_t second_origin = 200;
  const std::string first_value = "uncompressed-value";
  const std::string second_value(4096, 'z');
  const std::string third_value = "other-origin";

  std::unique_ptr<Compressor> compressor =
      GetBuiltinV2CompressionManager()->GetCompressor({}, kZSTD);
  GrowableBuffer compressed_second;
  ASSERT_OK(LegacyForceBuiltinCompression(*compressor, /*working_area=*/nullptr,
                                          second_value, &compressed_second));
  const std::vector<RelocationTestRecord> records{
      {first_origin, 10, first_value, kNoCompression},
      {first_origin, 20, compressed_second, kZSTD},
      {second_origin, 7, third_value, kNoCompression}};

  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      column_family_id, relocation_file_number, records,
                      &metadata, &file_size);
  ASSERT_EQ(metadata.origin_count, 2U);
  ASSERT_EQ(metadata.entry_count, records.size());
  ASSERT_EQ(metadata.record_bytes,
            first_value.size() + compressed_second.size() + third_value.size() +
                records.size() * kSimpleGen2BlobTrailerSize);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  read_options.verify_checksums = true;
  ASSERT_OK(BlobFileReader::CreateRelocationFile(
      immutable_options, read_options, FileOptions(),
      /*blob_file_read_hist=*/nullptr, column_family_id, relocation_file_number,
      file_size, metadata.tail_size, metadata.directory_digest,
      /*current_db_session_id=*/"reader", table_options,
      mutable_cf_options.compression_manager.get(),
      /*block_protection_bytes_per_key=*/0, /*io_tracer=*/nullptr, &reader));
  ASSERT_NE(reader, nullptr);
  ASSERT_TRUE(reader->IsRelocationFile());

  std::unique_ptr<BlobFileReader> invalid_reader;
  ASSERT_TRUE(BlobFileReader::CreateRelocationFile(
                  immutable_options, read_options, FileOptions(),
                  /*blob_file_read_hist=*/nullptr, column_family_id,
                  relocation_file_number, file_size, metadata.tail_size,
                  metadata.directory_digest,
                  /*current_db_session_id=*/"", table_options,
                  mutable_cf_options.compression_manager.get(),
                  /*block_protection_bytes_per_key=*/0,
                  /*io_tracer=*/nullptr, &invalid_reader)
                  .IsInvalidArgument());
  ASSERT_TRUE(BlobFileReader::CreateRelocationFile(
                  immutable_options, read_options, FileOptions(),
                  /*blob_file_read_hist=*/nullptr, column_family_id,
                  relocation_file_number, file_size, metadata.tail_size,
                  metadata.directory_digest ^ 1,
                  /*current_db_session_id=*/"reader", table_options,
                  mutable_cf_options.compression_manager.get(),
                  /*block_protection_bytes_per_key=*/0,
                  /*io_tracer=*/nullptr, &invalid_reader)
                  .IsCorruption());
  ASSERT_TRUE(BlobFileReader::CreateRelocationFile(
                  immutable_options, read_options, FileOptions(),
                  /*blob_file_read_hist=*/nullptr, column_family_id,
                  relocation_file_number, file_size + 1, metadata.tail_size,
                  metadata.directory_digest,
                  /*current_db_session_id=*/"reader", table_options,
                  mutable_cf_options.compression_manager.get(),
                  /*block_protection_bytes_per_key=*/0,
                  /*io_tracer=*/nullptr, &invalid_reader)
                  .IsCorruption());
  ASSERT_TRUE(BlobFileReader::CreateRelocationFile(
                  immutable_options, read_options, FileOptions(),
                  /*blob_file_read_hist=*/nullptr, column_family_id + 1,
                  relocation_file_number, file_size, metadata.tail_size,
                  metadata.directory_digest,
                  /*current_db_session_id=*/"reader", table_options,
                  mutable_cf_options.compression_manager.get(),
                  /*block_protection_bytes_per_key=*/0,
                  /*io_tracer=*/nullptr, &invalid_reader)
                  .IsCorruption());

  const auto make_index = [](uint64_t origin, uint64_t original_offset,
                             uint64_t stored_size, CompressionType compression,
                             const Slice& logical_value) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, /*generation_file_number=*/origin, origin, original_offset,
        stored_size, compression, BlobIndex::kStableChecksumVersion1,
        crc32c::Value(logical_value.data(), logical_value.size()));
    BlobIndex index;
    EXPECT_OK(index.DecodeFrom(encoded));
    return index;
  };
  const BlobIndex first_index = make_index(first_origin, 10, first_value.size(),
                                           kNoCompression, first_value);
  const BlobIndex second_index = make_index(
      first_origin, 20, compressed_second.size(), kZSTD, second_value);
  const BlobIndex third_index = make_index(second_origin, 7, third_value.size(),
                                           kNoCompression, third_value);

  std::array<std::string, 3> stored_payloads;
  std::array<Status, 3> stored_statuses;
  std::array<StoredRelocationFileReadRequest, 3> stored_requests{
      StoredRelocationFileReadRequest{
          first_origin, 10, first_value.size(), kNoCompression,
          BlobIndex::kStableChecksumVersion1,
          crc32c::Value(first_value.data(), first_value.size()),
          &stored_payloads[0], &stored_statuses[0]},
      StoredRelocationFileReadRequest{
          first_origin, 20, compressed_second.size(), kZSTD,
          BlobIndex::kStableChecksumVersion1,
          crc32c::Value(second_value.data(), second_value.size()),
          &stored_payloads[1], &stored_statuses[1]},
      StoredRelocationFileReadRequest{
          second_origin, 7, third_value.size(), kNoCompression,
          BlobIndex::kStableChecksumVersion1,
          crc32c::Value(third_value.data(), third_value.size()) ^ 1,
          &stored_payloads[2], &stored_statuses[2]}};
  uint64_t stored_bytes_read = 0;
  reader->MultiGetStoredBlobsFromRelocationFile(
      read_options, stored_requests.size(), stored_requests.data(),
      /*max_stored_value_size=*/1 << 20,
      /*max_logical_value_size=*/1 << 20, &stored_bytes_read);
  ASSERT_OK(stored_statuses[0]);
  ASSERT_OK(stored_statuses[1]);
  ASSERT_TRUE(stored_statuses[2].IsCorruption());
  EXPECT_EQ(stored_payloads[0], first_value);
  EXPECT_EQ(stored_payloads[1], compressed_second.AsSlice().ToString());
  EXPECT_TRUE(stored_payloads[2].empty());
  EXPECT_EQ(stored_bytes_read, first_value.size() + compressed_second.size() +
                                   2 * kSimpleGen2BlobTrailerSize);

  PinnableSlice value;
  uint64_t bytes_read = 0;
  ASSERT_OK(reader->GetBlobFromRelocationFile(
      read_options, first_index, /*range_offset=*/0, kWholeBlobLength, &value,
      &bytes_read));
  EXPECT_EQ(value, first_value);
  EXPECT_EQ(bytes_read, first_value.size() + kSimpleGen2BlobTrailerSize);

  value.Reset();
  ASSERT_OK(reader->GetBlobFromRelocationFile(
      read_options, first_index, /*range_offset=*/2, /*range_length=*/5, &value,
      &bytes_read));
  EXPECT_EQ(value, Slice(first_value.data() + 2, 5));
  EXPECT_EQ(bytes_read, 5U);

  value.Reset();
  ASSERT_OK(reader->GetBlobFromRelocationFile(
      read_options, second_index, /*range_offset=*/3, /*range_length=*/7,
      &value, &bytes_read));
  EXPECT_EQ(value, Slice(second_value.data() + 3, 7));
  EXPECT_EQ(bytes_read, compressed_second.size() + kSimpleGen2BlobTrailerSize);

  value.Reset();
  value.PinSelf("stale");
  EXPECT_TRUE(reader
                  ->GetBlobFromRelocationFile(
                      read_options, first_index,
                      /*range_offset=*/first_value.size(),
                      /*range_length=*/std::numeric_limits<size_t>::max() - 1,
                      &value, &bytes_read)
                  .IsInvalidArgument());

  const BlobIndex unsupported_index =
      make_index(first_origin, 10, first_value.size(), kFirstCustomCompression,
                 first_value);
  EXPECT_TRUE(reader
                  ->GetBlobFromRelocationFile(
                      read_options, unsupported_index, /*range_offset=*/0,
                      kWholeBlobLength, &value, &bytes_read)
                  .IsCorruption());
  const BlobIndex oversized_index =
      make_index(first_origin, 10, kMaxSimpleGen2BlobPayloadSize + 1,
                 kNoCompression, first_value);
  EXPECT_TRUE(reader
                  ->GetBlobFromRelocationFile(
                      read_options, oversized_index, /*range_offset=*/0,
                      kWholeBlobLength, &value, &bytes_read)
                  .IsCorruption());

  std::array<PinnableSlice, 3> invalid_values;
  std::array<Status, 3> invalid_statuses;
  std::array<RelocationBlobReadRequest, 3> invalid_requests{
      RelocationBlobReadRequest{&first_index, first_value.size(),
                                std::numeric_limits<size_t>::max() - 1,
                                &invalid_values[0], &invalid_statuses[0]},
      RelocationBlobReadRequest{&unsupported_index, 0, kWholeBlobLength,
                                &invalid_values[1], &invalid_statuses[1]},
      RelocationBlobReadRequest{&oversized_index, 0, kWholeBlobLength,
                                &invalid_values[2], &invalid_statuses[2]}};
  bytes_read = std::numeric_limits<uint64_t>::max();
  reader->MultiGetBlobFromRelocationFile(read_options, invalid_requests.size(),
                                         invalid_requests.data(), &bytes_read);
  EXPECT_TRUE(invalid_statuses[0].IsInvalidArgument());
  EXPECT_TRUE(invalid_statuses[1].IsCorruption());
  EXPECT_TRUE(invalid_statuses[2].IsCorruption());
  EXPECT_EQ(bytes_read, 0U);

  std::array<PinnableSlice, 3> values;
  std::array<Status, 3> statuses;
  std::array<RelocationBlobReadRequest, 3> requests;
  const std::array<const BlobIndex*, 3> indexes{&third_index, &first_index,
                                                &second_index};
  for (size_t i = 0; i < requests.size(); ++i) {
    requests[i].blob_index = indexes[i];
    requests[i].result = &values[i];
    requests[i].status = &statuses[i];
  }
  std::atomic<size_t> relocation_multiread_calls{0};
  std::atomic<size_t> relocation_multiread_requests{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MultiReadGen2:Requests", [&](void* arg) {
        const std::vector<FSReadRequest>* const aligned_reqs =
            static_cast<std::vector<FSReadRequest>*>(arg);
        ++relocation_multiread_calls;
        relocation_multiread_requests += aligned_reqs->size();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  reader->MultiGetBlobFromRelocationFile(read_options, requests.size(),
                                         requests.data(), &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_OK(statuses[2]);
  EXPECT_EQ(values[0], third_value);
  EXPECT_EQ(values[1], first_value);
  EXPECT_EQ(values[2], second_value);
  EXPECT_EQ(bytes_read, metadata.record_bytes);
  EXPECT_EQ(relocation_multiread_calls.load(), 1U);
  EXPECT_EQ(relocation_multiread_requests.load(), requests.size());

  std::array<PinnableSlice, 2> duplicate_values;
  std::array<Status, 2> duplicate_statuses;
  std::array<RelocationBlobReadRequest, 2> duplicate_requests{
      RelocationBlobReadRequest{&first_index, 0, kWholeBlobLength,
                                &duplicate_values[0], &duplicate_statuses[0]},
      RelocationBlobReadRequest{&first_index, 0, kWholeBlobLength,
                                &duplicate_values[1], &duplicate_statuses[1]}};
  std::atomic<size_t> duplicate_multiread_calls{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MultiReadGen2:Requests", [&](void* arg) {
        const std::vector<FSReadRequest>* const read_reqs =
            static_cast<std::vector<FSReadRequest>*>(arg);
        ++duplicate_multiread_calls;
        for (size_t i = 1; i < read_reqs->size(); ++i) {
          EXPECT_LE((*read_reqs)[i - 1].offset + (*read_reqs)[i - 1].len,
                    (*read_reqs)[i].offset);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  reader->MultiGetBlobFromRelocationFile(
      read_options, duplicate_requests.size(), duplicate_requests.data(),
      &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(duplicate_statuses[0]);
  ASSERT_OK(duplicate_statuses[1]);
  EXPECT_EQ(duplicate_values[0], first_value);
  EXPECT_EQ(duplicate_values[1], first_value);
  EXPECT_EQ(duplicate_multiread_calls.load(), 2U);

  std::array<PinnableSlice, 2> overlapping_values;
  std::array<Status, 2> overlapping_statuses;
  std::array<RelocationBlobReadRequest, 2> overlapping_requests{
      RelocationBlobReadRequest{&first_index, 0, 5, &overlapping_values[0],
                                &overlapping_statuses[0]},
      RelocationBlobReadRequest{&first_index, 3, 5, &overlapping_values[1],
                                &overlapping_statuses[1]}};
  std::atomic<size_t> overlapping_multiread_calls{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MultiReadGen2:Requests", [&](void* arg) {
        const std::vector<FSReadRequest>* const read_reqs =
            static_cast<std::vector<FSReadRequest>*>(arg);
        ++overlapping_multiread_calls;
        for (size_t i = 1; i < read_reqs->size(); ++i) {
          EXPECT_LE((*read_reqs)[i - 1].offset + (*read_reqs)[i - 1].len,
                    (*read_reqs)[i].offset);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  reader->MultiGetBlobFromRelocationFile(
      read_options, overlapping_requests.size(), overlapping_requests.data(),
      &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(overlapping_statuses[0]);
  ASSERT_OK(overlapping_statuses[1]);
  EXPECT_EQ(overlapping_values[0], Slice(first_value.data(), 5));
  EXPECT_EQ(overlapping_values[1], Slice(first_value.data() + 3, 5));
  EXPECT_EQ(overlapping_multiread_calls.load(), 2U);

  for (PinnableSlice& overlapping_value : overlapping_values) {
    overlapping_value.Reset();
  }
  overlapping_statuses = {};
  ReadOptions deadline_options(read_options);
  deadline_options.deadline =
      std::chrono::microseconds(mock_env_->NowMicros() + 1000);
  EmulatedSystemClock* const mock_clock =
      static_cast_with_check<EmulatedSystemClock>(
          mock_env_->GetSystemClock().get());
  std::atomic<size_t> deadline_multiread_calls{0};
  SyncPoint::GetInstance()->SetCallBack(
      "MultiReadGen2:Requests", [&](void* /*arg*/) {
        if (++deadline_multiread_calls == 1) {
          mock_clock->MockSleepForMicroseconds(2000);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  reader->MultiGetBlobFromRelocationFile(
      deadline_options, overlapping_requests.size(),
      overlapping_requests.data(), &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(overlapping_statuses[0]);
  EXPECT_TRUE(overlapping_statuses[1].IsTimedOut());
  EXPECT_EQ(overlapping_values[0], Slice(first_value.data(), 5));
  EXPECT_TRUE(overlapping_values[1].empty());
  EXPECT_EQ(deadline_multiread_calls.load(), 1U);

  const BlobIndex missing =
      make_index(/*origin=*/999, /*original_offset=*/1, first_value.size(),
                 kNoCompression, first_value);
  value.Reset();
  EXPECT_TRUE(reader
                  ->GetBlobFromRelocationFile(
                      read_options, missing, /*range_offset=*/0,
                      kWholeBlobLength, &value, /*bytes_read=*/nullptr)
                  .IsCorruption());

  std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
  FileOptions file_options;
  BlobFileCache blob_file_cache(
      file_cache.get(), &immutable_options, &file_options, column_family_id,
      /*blob_file_read_hist=*/nullptr, /*io_tracer=*/nullptr, &table_options);
  const std::string db_id = "relocation-test-db";
  const std::string db_session_id = "relocation-test-session";
  BlobSource blob_source(immutable_options, mutable_cf_options, db_id,
                         db_session_id, &blob_file_cache);
  std::array<PinnableSlice, 3> source_values;
  std::array<Status, 3> source_statuses;
  std::array<IndirectBlobReadRequest, 3> source_requests;
  const std::array<const BlobIndex*, 3> source_indexes{
      &second_index, &third_index, &first_index};
  for (size_t i = 0; i < source_requests.size(); ++i) {
    source_requests[i].user_key = "unused";
    source_requests[i].blob_index = source_indexes[i];
    source_requests[i].result = &source_values[i];
    source_requests[i].status = &source_statuses[i];
  }
  blob_source.MultiGetIndirectBlobFromOneFile(
      read_options,
      BlobFileOpenInfo{relocation_file_number, Slice(), Slice(),
                       metadata.tail_size},
      file_size, /*is_relocation_file=*/true, metadata.directory_digest,
      mutable_cf_options.compression_manager.get(),
      /*block_protection_bytes_per_key=*/0, source_requests.size(),
      source_requests.data(), &bytes_read);
  ASSERT_OK(source_statuses[0]);
  ASSERT_OK(source_statuses[1]);
  ASSERT_OK(source_statuses[2]);
  EXPECT_EQ(source_values[0], second_value);
  EXPECT_EQ(source_values[1], third_value);
  EXPECT_EQ(source_values[2], first_value);
  EXPECT_EQ(bytes_read, metadata.record_bytes);

  CacheHandleGuard<BlobFileReader> invalid_cached_reader;
  EXPECT_TRUE(blob_file_cache
                  .GetRelocationFileReader(
                      read_options,
                      BlobFileOpenInfo{relocation_file_number, Slice(), Slice(),
                                       metadata.tail_size},
                      file_size, metadata.directory_digest,
                      /*current_db_session_id=*/"",
                      mutable_cf_options.compression_manager.get(),
                      /*block_protection_bytes_per_key=*/0,
                      &invalid_cached_reader)
                  .IsInvalidArgument());

  for (PinnableSlice& source_value : source_values) {
    source_value.Reset();
  }
  read_options.read_tier = kBlockCacheTier;
  blob_source.MultiGetIndirectBlobFromOneFile(
      read_options, BlobFileOpenInfo{/*file_number=*/999, Slice(), Slice()},
      file_size,
      /*is_relocation_file=*/true,
      /*relocation_directory_digest=*/metadata.directory_digest ^ 1,
      mutable_cf_options.compression_manager.get(),
      /*block_protection_bytes_per_key=*/0, source_requests.size(),
      source_requests.data(), &bytes_read);
  ASSERT_OK(source_statuses[0]);
  ASSERT_OK(source_statuses[1]);
  ASSERT_OK(source_statuses[2]);
  EXPECT_EQ(source_values[0], second_value);
  EXPECT_EQ(source_values[1], third_value);
  EXPECT_EQ(source_values[2], first_value);
  EXPECT_EQ(bytes_read, 0U);
}

namespace {

// Returns a stale path-level size for one target blob file while leaving the
// opened file handle untouched. This lets the test verify
// BlobFileReader::Create prefers the opened handle's size when the path stat
// lags behind.
class StalePathSizeFileSystem : public FileSystemWrapper {
 public:
  static const char* kClassName() { return "StalePathSizeFileSystem"; }

  StalePathSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                          std::string stale_path)
      : FileSystemWrapper(target), stale_path_(std::move(stale_path)) {}

  const char* Name() const override { return kClassName(); }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    if (fname == stale_path_) {
      *file_size = 0;
      return IOStatus::OK();
    }
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

 private:
  const std::string stale_path_;
};

// Makes opened blob file handles fail GetFileSize() with a hard error so the
// reader path can verify that such errors are propagated directly.
class FailingOpenFileSizeRandomAccessFile
    : public FSRandomAccessFileOwnerWrapper {
 public:
  explicit FailingOpenFileSizeRandomAccessFile(
      std::unique_ptr<FSRandomAccessFile>&& target)
      : FSRandomAccessFileOwnerWrapper(std::move(target)) {}

  IOStatus GetFileSize(uint64_t* /*result*/) override {
    return IOStatus::IOError("open file size failed");
  }
};

// Makes opened blob file handles report GetFileSize() as unsupported so the
// reader path exercises its path-level fallback logic.
class NotSupportedOpenFileSizeRandomAccessFile
    : public FSRandomAccessFileOwnerWrapper {
 public:
  explicit NotSupportedOpenFileSizeRandomAccessFile(
      std::unique_ptr<FSRandomAccessFile>&& target)
      : FSRandomAccessFileOwnerWrapper(std::move(target)) {}

  IOStatus GetFileSize(uint64_t* /*result*/) override {
    return IOStatus::NotSupported("open file size not supported");
  }
};

class OpenFileSizeFileSystem : public FileSystemWrapper {
 public:
  OpenFileSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                         std::string target_path)
      : FileSystemWrapper(target), target_path_(std::move(target_path)) {}

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s =
        FileSystemWrapper::NewRandomAccessFile(fname, options, &file, dbg);
    if (!s.ok()) {
      return IOStatus(std::move(s));
    }
    if (fname == target_path_) {
      *result = WrapTargetFile(std::move(file));
    } else {
      *result = std::move(file);
    }
    return IOStatus::OK();
  }

 protected:
  virtual std::unique_ptr<FSRandomAccessFile> WrapTargetFile(
      std::unique_ptr<FSRandomAccessFile>&& file) = 0;

 private:
  const std::string target_path_;
};

// Wraps the target blob file in FailingOpenFileSizeRandomAccessFile while
// leaving all other files alone.
class FailingOpenFileSizeFileSystem : public OpenFileSizeFileSystem {
 public:
  static const char* kClassName() { return "FailingOpenFileSizeFileSystem"; }

  FailingOpenFileSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                                std::string target_path)
      : OpenFileSizeFileSystem(target, std::move(target_path)) {}

  const char* Name() const override { return kClassName(); }

 private:
  std::unique_ptr<FSRandomAccessFile> WrapTargetFile(
      std::unique_ptr<FSRandomAccessFile>&& file) override {
    return std::unique_ptr<FSRandomAccessFile>(
        new FailingOpenFileSizeRandomAccessFile(std::move(file)));
  }
};

// Wraps the target blob file in NotSupportedOpenFileSizeRandomAccessFile while
// leaving all other files alone.
class NotSupportedOpenFileSizeFileSystem : public OpenFileSizeFileSystem {
 public:
  static const char* kClassName() {
    return "NotSupportedOpenFileSizeFileSystem";
  }

  NotSupportedOpenFileSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                                     std::string target_path)
      : OpenFileSizeFileSystem(target, std::move(target_path)) {}

  const char* Name() const override { return kClassName(); }

 private:
  std::unique_ptr<FSRandomAccessFile> WrapTargetFile(
      std::unique_ptr<FSRandomAccessFile>&& file) override {
    return std::unique_ptr<FSRandomAccessFile>(
        new NotSupportedOpenFileSizeRandomAccessFile(std::move(file)));
  }
};

// Forces the target blob file onto the NotSupported open-handle-size branch and
// then fails the fallback path-level GetFileSize() call.
class FailingFallbackPathSizeFileSystem
    : public NotSupportedOpenFileSizeFileSystem {
 public:
  static const char* kClassName() {
    return "FailingFallbackPathSizeFileSystem";
  }

  FailingFallbackPathSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                                    std::string target_path)
      : NotSupportedOpenFileSizeFileSystem(target, target_path),
        target_path_(std::move(target_path)) {}

  const char* Name() const override { return kClassName(); }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    if (fname == target_path_) {
      return IOStatus::IOError("fallback path size failed");
    }
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

 private:
  const std::string target_path_;
};

}  // namespace

TEST_F(BlobFileReaderTest, CreateReaderAndGetBlob) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_CreateReaderAndGetBlob"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr size_t num_blobs = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  const std::vector<std::string> blob_strs = {"blob1", "blob2", "blob3"};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> blobs = {blob_strs[0], blob_strs[1], blob_strs[2]};

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, keys[0], blob_offsets[0],
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blobs[0]);
    ASSERT_EQ(bytes_read, blob_sizes[0]);

    // MultiGetBlob
    bytes_read = 0;
    size_t total_size = 0;

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(keys[i], blob_offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      const auto& result = blob_reqs[i].second;

      ASSERT_OK(statuses_buf[i]);
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(result->data(), blobs[i]);
      total_size += blob_sizes[i];
    }
    ASSERT_EQ(bytes_read, total_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, keys[1], blob_offsets[1],
                              blob_sizes[1], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blobs[1]);

    const uint64_t key_size = keys[1].size();
    ASSERT_EQ(bytes_read,
              BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  blob_sizes[1]);
  }

  // Invalid offset (too close to start of file)
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[0], blob_offsets[0] - 1,
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Invalid offset (too close to end of file)
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[2], blob_offsets[2] + 1,
                              blob_sizes[2], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect compression type
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[0], blob_offsets[0],
                              blob_sizes[0], kZSTD, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect key size
  {
    constexpr char shorter_key[] = "k";
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, shorter_key,
                              blob_offsets[0] -
                                  (keys[0].size() - sizeof(shorter_key) + 1),
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice shorter_key_slice(shorter_key, sizeof(shorter_key) - 1);
    key_refs[1] = std::cref(shorter_key_slice);

    autovector<uint64_t> offsets{
        blob_offsets[0],
        blob_offsets[1] - (keys[1].size() - key_refs[1].get().size()),
        blob_offsets[2]};

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(key_refs[i], offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i == 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect key
  {
    constexpr char incorrect_key[] = "foo1";
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, incorrect_key, blob_offsets[0],
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice wrong_key_slice(incorrect_key, sizeof(incorrect_key) - 1);
    key_refs[2] = std::cref(wrong_key_slice);

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(key_refs[i], blob_offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i == num_blobs - 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect value size
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[1], blob_offsets[1],
                              blob_sizes[1] + 1, kNoCompression,
                              prefetch_buffer, allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;

    requests_buf[0] =
        BlobReadRequest(key_refs[0], blob_offsets[0], blob_sizes[0],
                        kNoCompression, nullptr, statuses_buf.data());
    requests_buf[1] =
        BlobReadRequest(key_refs[1], blob_offsets[1], blob_sizes[1] + 1,
                        kNoCompression, nullptr, &statuses_buf[1]);
    requests_buf[2] =
        BlobReadRequest(key_refs[2], blob_offsets[2], blob_sizes[2],
                        kNoCompression, nullptr, &statuses_buf[2]);

    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i != 1) {
        ASSERT_OK(statuses_buf[i]);
      } else {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      }
    }
  }
}

TEST_F(BlobFileReaderTest,
       MultiGetStoredBlobsForRelocationPreservesCompressedBytes) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileReaderTest_MultiGetStoredBlobsForRelocation"),
      0);
  options.enable_blob_files = true;
  const ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr ExpirationRange expiration_range;
  const std::array<std::string, 3> key_strings{{"key1", "key2", "key3"}};
  const std::array<std::string, 3> blob_strings{
      {std::string(4096, 'a'), std::string(4096, 'b'), std::string(4096, 'c')}};
  const std::vector<Slice> keys{key_strings[0], key_strings[1], key_strings[2]};
  const std::vector<Slice> blobs{blob_strings[0], blob_strings[1],
                                 blob_strings[2]};
  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());
  WriteBlobFile(immutable_options, column_family_id,
                /*has_ttl=*/false, expiration_range, expiration_range,
                blob_file_number, keys, blobs, kZSTD, blob_offsets, blob_sizes);

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader));

  std::array<std::string, 3> stored_payloads;
  std::array<Status, 3> statuses;
  std::array<StoredBlobRelocationReadRequest, 3> requests;
  for (size_t i = 0; i < requests.size(); ++i) {
    requests[i] = StoredBlobRelocationReadRequest{
        blob_offsets[i],
        blob_sizes[i],
        kZSTD,
        BlobIndex::kStableChecksumVersion1,
        crc32c::Value(blob_strings[i].data(), blob_strings[i].size()),
        &stored_payloads[i],
        &statuses[i]};
  }
  ++requests.back().stable_checksum;

  uint64_t bytes_read = 0;
  reader->MultiGetStoredBlobsForRelocation(
      read_options, requests.size(), requests.data(),
      /*max_stored_value_size=*/1 << 20,
      /*max_logical_value_size=*/1 << 20, &bytes_read);
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_TRUE(statuses[2].IsCorruption());
  EXPECT_EQ(stored_payloads[0].size(), blob_sizes[0]);
  EXPECT_EQ(stored_payloads[1].size(), blob_sizes[1]);
  EXPECT_TRUE(stored_payloads[2].empty());
  EXPECT_NE(stored_payloads[0], blob_strings[0]);
  EXPECT_EQ(bytes_read, blob_sizes[0] + blob_sizes[1]);

  --requests.back().stable_checksum;
  for (std::string& payload : stored_payloads) {
    payload.clear();
  }
  bytes_read = 0;
  reader->MultiGetStoredBlobsForRelocation(
      read_options, requests.size(), requests.data(),
      /*max_stored_value_size=*/1 << 20,
      /*max_logical_value_size=*/1024, &bytes_read);
  for (size_t i = 0; i < requests.size(); ++i) {
    EXPECT_TRUE(statuses[i].IsMemoryLimit());
    EXPECT_TRUE(stored_payloads[i].empty());
  }
  EXPECT_EQ(bytes_read, 0U);
}

// Verifies Create() prefers the file size reported by the opened handle over a
// stale path-level size query by returning a readable reader even when the path
// stat intentionally reports 0 for the blob file.
TEST_F(BlobFileReaderTest, CreateReaderUsesOpenedFileSizeWhenPathSizeIsStale) {
  const std::string db_path = test::PerThreadDBPath(
      mock_env_.get(),
      "BlobFileReaderTest_CreateReaderUsesOpenedFileSizeWhenPathSizeIsStale");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto stale_size_fs = std::make_shared<StalePathSizeFileSystem>(
      mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper stale_size_env(mock_env_.get(), stale_size_fs);

  Options options;
  options.env = &stale_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  read_options.verify_checksums = false;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader));
  ASSERT_NE(reader, nullptr);

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;
  ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                            kNoCompression,
                            /*prefetch_buffer=*/nullptr,
                            /*allocator=*/nullptr, &value, &bytes_read));
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(value->data(), Slice(blob));
  ASSERT_EQ(bytes_read, blob_size);
}

// Verifies Create() falls back to the path-level GetFileSize() call when the
// opened file handle reports that size queries are unsupported.
TEST_F(BlobFileReaderTest,
       CreateReaderFallsBackToPathSizeWhenOpenedFileSizeNotSupported) {
  const std::string db_path = test::PerThreadDBPath(
      mock_env_.get(),
      "BlobFileReaderTest_"
      "CreateReaderFallsBackToPathSizeWhenOpenedFileSizeNotSupported");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto not_supported_open_file_size_fs =
      std::make_shared<NotSupportedOpenFileSizeFileSystem>(
          mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper not_supported_open_file_size_env(
      mock_env_.get(), not_supported_open_file_size_fs);

  Options options;
  options.env = &not_supported_open_file_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  read_options.verify_checksums = false;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader));
  ASSERT_NE(reader, nullptr);

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;
  ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                            kNoCompression,
                            /*prefetch_buffer=*/nullptr,
                            /*allocator=*/nullptr, &value, &bytes_read));
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(value->data(), Slice(blob));
  ASSERT_EQ(bytes_read, blob_size);
}

// Verifies Create() propagates a real error from the opened file handle's
// GetFileSize() rather than masking it with any path-level fallback.
TEST_F(BlobFileReaderTest, CreateReaderPropagatesOpenedFileSizeError) {
  const std::string db_path = test::PerThreadDBPath(
      mock_env_.get(),
      "BlobFileReaderTest_CreateReaderPropagatesOpenedFileSizeError");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto failing_open_file_size_fs =
      std::make_shared<FailingOpenFileSizeFileSystem>(
          mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper failing_open_file_size_env(mock_env_.get(),
                                                 failing_open_file_size_fs);

  Options options;
  options.env = &failing_open_file_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(reader, nullptr);
}

// Verifies Create() also propagates errors from the path-level fallback size
// query when the opened file handle only reports NotSupported for GetFileSize.
TEST_F(
    BlobFileReaderTest,
    CreateReaderPropagatesFallbackPathSizeErrorWhenOpenedFileSizeNotSupported) {
  const std::string db_path =
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_"
                            "CreateReaderPropagatesFallbackPathSizeErrorWhenOpe"
                            "nedFileSizeNotSupported");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto failing_fallback_path_size_fs =
      std::make_shared<FailingFallbackPathSizeFileSystem>(
          mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper failing_fallback_path_size_env(
      mock_env_.get(), failing_fallback_path_size_fs);

  Options options;
  options.env = &failing_fallback_path_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(reader, nullptr);
}

TEST_F(BlobFileReaderTest, Malformed) {
  // Write a blob file consisting of nothing but a header, and make sure we
  // detect the error when we open it for reading

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_Malformed"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;

  {
    constexpr bool has_ttl = false;
    constexpr ExpirationRange expiration_range;

    const std::string blob_file_path =
        BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);

    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                              FileOptions()));

    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), blob_file_path, FileOptions(),
                               immutable_options.clock));

    constexpr Statistics* statistics = nullptr;
    constexpr bool use_fsync = false;
    constexpr bool do_flush = false;

    BlobLogWriter blob_log_writer(std::move(file_writer),
                                  immutable_options.clock, statistics,
                                  blob_file_number, use_fsync, do_flush);

    BlobLogHeader header(column_family_id, kNoCompression, has_ttl,
                         expiration_range);

    ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));
  }

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, TTL) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_TTL"), 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = true;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInHeader) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_ExpirationRangeInHeader"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  const ExpirationRange expiration_range_header(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr ExpirationRange expiration_range_footer;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, kNoCompression, &blob_offset,
                &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInFooter) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_ExpirationRangeInFooter"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range_header;
  const ExpirationRange expiration_range_footer(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, kNoCompression, &blob_offset,
                &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, IncorrectColumnFamily) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_IncorrectColumnFamily"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  constexpr uint32_t incorrect_column_family_id = 2;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), incorrect_column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, BlobCRCError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_BlobCRCError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::VerifyBlob:CheckBlobCRC", [](void* arg) {
        BlobLogRecord* const record = static_cast<BlobLogRecord*>(arg);
        assert(record);

        record->blob_crc = 0xfaceb00c;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                            kNoCompression, prefetch_buffer, allocator, &value,
                            &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileReaderTest, Compression) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_Compression"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob,
                kSnappyCompression, &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, prefetch_buffer, allocator,
                              &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blob);
    ASSERT_EQ(bytes_read, blob_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, prefetch_buffer, allocator,
                              &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blob);

    constexpr uint64_t key_size = sizeof(key) - 1;
    ASSERT_EQ(bytes_read,
              BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  blob_size);
  }
}

TEST_F(BlobFileReaderTest, UncompressionError) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_UncompressionError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob,
                kSnappyCompression, &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", [](void* arg) {
        auto* result = static_cast<Status*>(arg);
        assert(result);

        *result = Status::Corruption("Injected result");
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_EQ(reader
                ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                          kSnappyCompression, prefetch_buffer, allocator,
                          &value, &bytes_read)
                .code(),
            Status::Code::kCorruption);
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class BlobFileReaderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderIOErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fault_injection_env_.reset(new FaultInjectionTestEnv(mock_env_.get()));
  }

  std::unique_ptr<Env> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(BlobFileReaderTest, BlobFileReaderIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::ReadHeader:ReadFromFile",
                            "BlobFileReader::ReadFooter:ReadFromFile",
                            "BlobFileReader::GetBlob:ReadFromFile"}));

TEST_P(BlobFileReaderIOErrorTest, IOError) {
  // Simulates an I/O error during the specified step

  Options options;
  options.env = fault_injection_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(fault_injection_env_.get(),
                            "BlobFileReaderIOErrorTest_IOError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  const Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      (sync_point_ != "BlobFileReader::GetBlob:ReadFromFile");

  if (fail_during_create) {
    ASSERT_TRUE(s.IsIOError());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsIOError());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class BlobFileReaderDecodingErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderDecodingErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
  }

  std::unique_ptr<Env> mock_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(BlobFileReaderTest, BlobFileReaderDecodingErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::ReadHeader:TamperWithResult",
                            "BlobFileReader::ReadFooter:TamperWithResult",
                            "BlobFileReader::GetBlob:TamperWithResult"}));

TEST_P(BlobFileReaderDecodingErrorTest, DecodingError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderDecodingErrorTest_DecodingError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [](void* arg) {
    Slice* const slice = static_cast<Slice*>(arg);
    assert(slice);
    assert(!slice->empty());

    slice->remove_prefix(1);
  });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  const Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      sync_point_ != "BlobFileReader::GetBlob:TamperWithResult";

  if (fail_during_create) {
    ASSERT_TRUE(s.IsCorruption());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileReaderTest, MultiGetBlobWithFailedValidation) {
  // Test that MultiGetBlob correctly handles the case where some requests
  // fail validation. The adjustments vector is only populated for requests
  // that pass validation, so the indices must track correctly.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileReaderTest_MultiGetBlobWithFailedValidation"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr size_t num_blobs = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  const std::vector<std::string> blob_strs = {"blob1", "blob2", "blob3"};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> blobs = {blob_strs[0], blob_strs[1], blob_strs[2]};

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Enable checksum verification so adjustments are non-zero
  read_options.verify_checksums = true;
  constexpr MemoryAllocator* allocator = nullptr;

  // Fail the first request by giving it an invalid offset (too small).
  // This causes the first request to be skipped in the adjustments vector,
  // so adjustments has only 2 entries (for blobs 1 and 2), while blob_reqs
  // has 3 entries. Without the fix, adjustments[i] would use the wrong
  // index for the remaining valid requests.
  std::array<Status, num_blobs> statuses_buf;
  std::array<BlobReadRequest, num_blobs> requests_buf;
  autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
      blob_reqs;

  // First request: invalid offset (too small, will fail validation)
  requests_buf[0] = BlobReadRequest(keys[0], /*offset=*/0, blob_sizes[0],
                                    kNoCompression, nullptr, &statuses_buf[0]);
  // Second and third requests: valid
  requests_buf[1] = BlobReadRequest(keys[1], blob_offsets[1], blob_sizes[1],
                                    kNoCompression, nullptr, &statuses_buf[1]);
  requests_buf[2] = BlobReadRequest(keys[2], blob_offsets[2], blob_sizes[2],
                                    kNoCompression, nullptr, &statuses_buf[2]);

  for (size_t i = 0; i < num_blobs; ++i) {
    blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
  }

  uint64_t bytes_read = 0;
  reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

  // First request should fail validation
  ASSERT_TRUE(statuses_buf[0].IsCorruption());
  ASSERT_EQ(blob_reqs[0].second, nullptr);

  // Second and third requests should succeed with correct data
  ASSERT_OK(statuses_buf[1]);
  ASSERT_NE(blob_reqs[1].second, nullptr);
  ASSERT_EQ(blob_reqs[1].second->data(), blobs[1]);

  ASSERT_OK(statuses_buf[2]);
  ASSERT_NE(blob_reqs[2].second, nullptr);
  ASSERT_EQ(blob_reqs[2].second->data(), blobs[2]);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
