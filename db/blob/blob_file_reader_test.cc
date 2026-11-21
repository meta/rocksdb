//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <future>
#include <limits>
#include <set>
#include <string>
#include <utility>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/blob/blob_relocation_file.h"
#include "db/blob/blob_source.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "env/composite_env_wrapper.h"
#include "env/emulated_clock.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "rocksdb/trace_reader_writer.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/format.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/mutexlock.h"
#include "util/random.h"
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
      blobs_to_write[i] = compressed_blobs[i].AsSlice();
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

  const InternalKeyComparator comparator(BytewiseComparator());
  std::unique_ptr<BlockBasedTableBuilder> builder;
  BlobRelocationTableFileInfo file_info;
  file_info.column_family_id = column_family_id;
  file_info.column_family_name = kDefaultColumnFamilyName;
  file_info.db_id = "test-db";
  file_info.db_session_id = "test-session";
  file_info.file_number = blob_file_number;
  ASSERT_OK(NewBlobRelocationTableBuilder(
      immutable_options, mutable_cf_options, table_options, ReadOptions(),
      WriteOptions(), comparator, file_info, writer.get(), &builder));
  for (const RelocationTestRecord& record : records) {
    ASSERT_OK(builder->AddBlobRelocation(
        record.origin_file_number, record.original_value_offset,
        record.stored_payload, record.compression));
  }
  // The file serves the file numbers of its origins, each added once.
  std::set<uint64_t> served;
  for (const RelocationTestRecord& record : records) {
    served.insert(record.origin_file_number);
  }
  for (uint64_t file_number : served) {
    ASSERT_OK(builder->AddBlobRelocationServedFile(file_number));
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

// The decoded directory of a relocation file written by WriteRelocationFile.
BlobRelocationDirectory DecodeDirectory(
    const BlobRelocationFileMetadata& metadata) {
  BlobRelocationDirectory directory;
  EXPECT_OK(BlobRelocationDirectory::DecodeFrom(metadata.encoded_directory,
                                                &directory));
  return directory;
}

// The expected identity (BlobPhysicalFile) of a relocation file written by
// WriteRelocationFile.
BlobPhysicalFile RelocationPhysicalFile(
    uint64_t file_number, uint64_t file_size,
    const BlobRelocationFileMetadata& metadata) {
  return BlobPhysicalFile{BlobFileOpenInfo{file_number, Slice(), Slice()},
                          file_size, /*is_relocation=*/true, metadata.tail_size,
                          DecodeDirectory(metadata).directory_digest()};
}

// Sets a callback on `sync_point`, which passes the batched file reads
// (std::vector<FSReadRequest>*). The callback counts the calls in `*calls` and
// checks that the reads are sorted and do not overlap.
void ExpectSortedNonOverlappingReads(const char* sync_point,
                                     std::atomic<size_t>* calls) {
  SyncPoint::GetInstance()->SetCallBack(sync_point, [calls](void* arg) {
    const auto* const reads = static_cast<std::vector<FSReadRequest>*>(arg);
    ++*calls;
    for (size_t i = 1; i < reads->size(); ++i) {
      EXPECT_LE((*reads)[i - 1].offset + (*reads)[i - 1].len,
                (*reads)[i].offset);
    }
  });
}

// Opens `file` like BlobFileCache does, with a factory built from
// `table_options`.
Status OpenRelocationReader(
    const ImmutableOptions& immutable_options, const ReadOptions& read_options,
    const FileOptions& file_options, uint32_t column_family_id,
    const BlobPhysicalFile& file, const std::string& db_session_id,
    const BlockBasedTableOptions& table_options,
    CompressionManager* compression_manager,
    std::unique_ptr<BlobFileReader>* reader, bool pin_location_blocks = false) {
  const BlockBasedTableFactory factory(
      MakeBlobRelocationTableOptions(table_options));
  std::unique_ptr<BlobRelocationFileReader> relocation;
  const Status s = BlobRelocationFileReader::Open(
      immutable_options, read_options, file_options,
      /*blob_file_read_hist=*/nullptr, column_family_id, file, db_session_id,
      factory, compression_manager, /*block_protection_bytes_per_key=*/0,
      pin_location_blocks, /*io_tracer=*/nullptr,
      /*block_cache_tracer=*/nullptr, &relocation);
  if (s.ok()) {
    reader->reset(new BlobFileReader(std::move(relocation)));
  }
  return s;
}

// Builds one relocation read request with named fields.
RelocationBlobReadRequest MakeRelocationRequest(
    const BlobIndex* blob_index, uint64_t range_offset, size_t range_length,
    Status* status, SameFileBlobStandaloneRead* output) {
  RelocationBlobReadRequest request;
  request.blob_index = blob_index;
  request.range_offset = range_offset;
  request.range_length = range_length;
  request.status = status;
  request.output = output;
  return request;
}

// Reads one value from a relocation file reader: a batch of one.
Status GetFromRelocationFile(const BlobFileReader& reader,
                             const ReadOptions& read_options,
                             const BlobIndex& blob_index, uint64_t range_offset,
                             size_t range_length, MemoryAllocator* allocator,
                             std::unique_ptr<BlobContents>* result,
                             uint64_t* bytes_read) {
  Status status;
  SameFileBlobStandaloneRead output;
  RelocationBlobReadRequest request = MakeRelocationRequest(
      &blob_index, range_offset, range_length, &status, &output);
  reader.relocation()->MultiGetBlob(read_options, allocator, /*num_reads=*/1,
                                    &request, bytes_read);
  *result = std::move(output.contents);
  return status;
}

}  // anonymous namespace

TEST(BlobRelocationDirectoryTest, ValidatesOrderingAndChecksums) {
  BlobRelocationDirectoryBuilder builder;
  ASSERT_OK(builder.Add(/*origin_file_number=*/10,
                        /*original_value_offset=*/2,
                        /*stored_value_size=*/3, kNoCompression));
  ASSERT_OK(builder.Add(/*origin_file_number=*/10,
                        /*original_value_offset=*/7,
                        /*stored_value_size=*/4, kNoCompression));
  ASSERT_OK(builder.Add(/*origin_file_number=*/20,
                        /*original_value_offset=*/1,
                        /*stored_value_size=*/5, kNoCompression));
  ASSERT_OK(builder.AddServedFile(/*file_number=*/30));
  ASSERT_TRUE(builder.AddServedFile(/*file_number=*/0).IsInvalidArgument());
  ASSERT_OK(builder.AddServedFile(/*file_number=*/10));
  ASSERT_OK(builder.AddServedFile(/*file_number=*/30));
  // Values of one origin must not overlap: 7 + 4 = 11 is the end of the last
  // value, so a value at 10 overlaps it. A value that starts exactly at the
  // end is fine (see the end-based gap in blob_relocation_file.h).
  {
    BlobRelocationDirectoryBuilder overlap;
    ASSERT_OK(overlap.Add(10, 7, 4, kNoCompression));
    ASSERT_TRUE(overlap.Add(10, 10, 1, kNoCompression).IsInvalidArgument());
    BlobRelocationDirectoryBuilder adjacent;
    ASSERT_OK(adjacent.Add(10, 7, 4, kNoCompression));
    ASSERT_OK(adjacent.Add(10, 11, 1, kNoCompression));
  }
  BlobRelocationDirectory directory;
  ASSERT_OK(builder.Finish(&directory));
  // Sorted and deduplicated.
  ASSERT_EQ(directory.served_file_numbers(), std::vector<uint64_t>({10, 30}));
  ASSERT_EQ(directory.origins().size(), 2U);
  ASSERT_EQ(directory.total_entries(), 3U);
  ASSERT_EQ(directory.total_record_bytes(),
            12U + 3U * kSimpleGen2BlobTrailerSize);
  ASSERT_NE(directory.Find(10), nullptr);
  ASSERT_EQ(directory.Find(99), nullptr);
  std::string encoded;
  directory.EncodeTo(&encoded);
  // ValidateForFileSize: the record bytes must fit in the file after the
  // prefix. With one 1000-byte record, the record bytes (1000 +
  // kSimpleGen2BlobTrailerSize = 1005) are larger than the encoded directory
  // (96 bytes), so the record-bytes check decides.
  BlobRelocationDirectoryBuilder large;
  ASSERT_OK(large.Add(10, 2, 1000, kNoCompression));
  ASSERT_OK(large.AddServedFile(10));
  BlobRelocationDirectory large_directory;
  ASSERT_OK(large.Finish(&large_directory));
  ASSERT_OK(large_directory.ValidateForFileSize(
      kBlobRelocationFilePrefixSize + large_directory.total_record_bytes()));
  ASSERT_TRUE(large_directory
                  .ValidateForFileSize(kBlobRelocationFilePrefixSize +
                                       large_directory.total_record_bytes() - 1)
                  .IsCorruption());
  ASSERT_OK(directory.ValidateForFileSize(encoded.size()));
  BlobRelocationDirectory decoded;
  ASSERT_OK(BlobRelocationDirectory::DecodeFrom(encoded, &decoded));
  ASSERT_EQ(decoded.directory_digest(), directory.directory_digest());
  ASSERT_EQ(decoded.served_file_numbers(), std::vector<uint64_t>({10, 30}));
  // The encoded directory must also fit in the file.
  ASSERT_TRUE(decoded.ValidateForFileSize(encoded.size() - 1).IsCorruption());

  // Re-seals an edited directory with valid digest and header CRC.
  constexpr size_t kDirectoryHeaderSize = 48;
  constexpr size_t kDirectoryDigestOffset = 40;
  constexpr size_t kHeaderChecksumOffset = 44;
  // The digest covers header bytes 8..39 (counts and totals) and the body.
  constexpr size_t kDigestedHeaderOffset = 8;
  const auto seal_header = [&](std::string* edited) {
    EncodeFixed32(edited->data() + kHeaderChecksumOffset,
                  crc32c::Value(edited->data(), kHeaderChecksumOffset));
  };
  const auto reseal = [&](std::string* edited) {
    const uint32_t header_crc =
        crc32c::Value(edited->data() + kDigestedHeaderOffset,
                      kDirectoryDigestOffset - kDigestedHeaderOffset);
    EncodeFixed32(
        edited->data() + kDirectoryDigestOffset,
        crc32c::Extend(header_crc, edited->data() + kDirectoryHeaderSize,
                       edited->size() - kDirectoryHeaderSize));
    seal_header(edited);
  };
  // The digest covers the totals: a changed total with a valid header CRC
  // but the old digest is rejected.
  {
    constexpr size_t kTotalEntriesOffset = 24;
    std::string other_totals = encoded;
    EncodeFixed64(other_totals.data() + kTotalEntriesOffset,
                  directory.total_entries() + 1);
    seal_header(&other_totals);
    const Status status =
        BlobRelocationDirectory::DecodeFrom(other_totals, &decoded);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find("checksum mismatch"), std::string::npos)
        << status.ToString();
  }
  // Origin 10 has two entries, at offsets 2 and 7. Setting its last offset to
  // 2, equal to its first offset, leaves room for only one offset, so the
  // decoder rejects the entry.
  std::string impossible_cardinality = encoded;
  constexpr size_t kLastOffsetField = 16;
  EncodeFixed64(
      impossible_cardinality.data() + kDirectoryHeaderSize + kLastOffsetField,
      /*last_original_offset=*/2);
  reseal(&impossible_cardinality);
  ASSERT_TRUE(
      BlobRelocationDirectory::DecodeFrom(impossible_cardinality, &decoded)
          .IsCorruption());

  // A file must serve at least one old file number.
  BlobRelocationDirectoryBuilder no_source;
  ASSERT_OK(no_source.Add(10, 2, 3, kNoCompression));
  BlobRelocationDirectory unused;
  ASSERT_TRUE(no_source.Finish(&unused).IsInvalidArgument());
  BlobRelocationDirectoryBuilder one_source;
  ASSERT_OK(one_source.Add(10, 2, 3, kNoCompression));
  ASSERT_OK(one_source.AddServedFile(10));
  BlobRelocationDirectory single;
  ASSERT_OK(one_source.Finish(&single));
  std::string no_served;
  single.EncodeTo(&no_served);
  // Drop the only served file and set the served count to zero.
  constexpr size_t kServedCountOffset = 16;
  no_served.resize(no_served.size() - sizeof(uint64_t));
  EncodeFixed64(no_served.data() + kServedCountOffset, 0);
  reseal(&no_served);
  ASSERT_TRUE(
      BlobRelocationDirectory::DecodeFrom(no_served, &decoded).IsCorruption());
  // The size must match the counts exactly.
  std::string extra_bytes;
  single.EncodeTo(&extra_bytes);
  extra_bytes.append(sizeof(uint64_t), '\0');
  reseal(&extra_bytes);
  ASSERT_TRUE(BlobRelocationDirectory::DecodeFrom(extra_bytes, &decoded)
                  .IsCorruption());
  // Many origins, more than 1 MB of directory: there is no fixed limit.
  BlobRelocationDirectoryBuilder many_origins;
  for (uint64_t origin = 1; origin <= 30000; ++origin) {
    ASSERT_OK(many_origins.Add(origin, 0, 1, kNoCompression));
  }
  ASSERT_OK(many_origins.AddServedFile(1));
  BlobRelocationDirectory many;
  ASSERT_OK(many_origins.Finish(&many));
  ASSERT_EQ(many.origins().size(), 30000U);
  ASSERT_GT(many.encoded_size(), 1024U * 1024U);

  std::string checksum_corruption = encoded;
  checksum_corruption.back() ^= 1;
  ASSERT_TRUE(BlobRelocationDirectory::DecodeFrom(checksum_corruption, &decoded)
                  .IsCorruption());

  BlobRelocationDirectoryBuilder duplicate;
  ASSERT_OK(duplicate.Add(10, 2, 3, kNoCompression));
  ASSERT_TRUE(duplicate.Add(10, 2, 4, kNoCompression).IsInvalidArgument());
  BlobRelocationDirectoryBuilder unsorted;
  ASSERT_OK(unsorted.Add(20, 1, 3, kNoCompression));
  ASSERT_TRUE(unsorted.Add(10, 2, 4, kNoCompression).IsInvalidArgument());
  BlobRelocationDirectoryBuilder unsupported;
  ASSERT_TRUE(
      unsupported.Add(10, 2, 3, kCustomCompression80).IsInvalidArgument());

  // A rejected overflowing addition must not change the current origin.
  BlobRelocationDirectoryBuilder overflow;
  ASSERT_OK(overflow.Add(
      10, 2, std::numeric_limits<uint64_t>::max() - kSimpleGen2BlobTrailerSize,
      kNoCompression));
  ASSERT_TRUE(overflow.Add(10, 7, 1, kNoCompression).IsInvalidArgument());
  ASSERT_OK(overflow.AddServedFile(10));
  BlobRelocationDirectory after_overflow;
  ASSERT_OK(overflow.Finish(&after_overflow));
  ASSERT_EQ(after_overflow.total_entries(), 1U);

  // Header fields, at their byte offsets.
  constexpr size_t kOriginCountOffset = 8;
  // Counts that the input cannot hold, including a pair whose encoded size
  // would overflow.
  for (const auto& counts : std::vector<std::pair<uint64_t, uint64_t>>{
           {uint64_t{1} << 61, 1},
           {1, uint64_t{1} << 61},
           {std::numeric_limits<uint64_t>::max() / 40 + 1,
            std::numeric_limits<uint64_t>::max() / 8 + 1}}) {
    std::string huge = encoded;
    EncodeFixed64(huge.data() + kOriginCountOffset, counts.first);
    EncodeFixed64(huge.data() + kServedCountOffset, counts.second);
    reseal(&huge);
    ASSERT_TRUE(
        BlobRelocationDirectory::DecodeFrom(huge, &decoded).IsCorruption());
  }
  // Fewer bytes than the counts say.
  std::string short_body = encoded;
  short_body.resize(short_body.size() - sizeof(uint64_t));
  reseal(&short_body);
  ASSERT_TRUE(
      BlobRelocationDirectory::DecodeFrom(short_body, &decoded).IsCorruption());

  // Decode-level order checks, on directories edited after encoding: origins
  // must be increasing and served files increasing and non-zero.
  const auto with_served = [&](const std::vector<uint64_t>& served) {
    std::string edited = encoded;  // origins 10, 20; served 10, 30
    edited.resize(kDirectoryHeaderSize + 2 * 40);
    for (uint64_t file_number : served) {
      PutFixed64(&edited, file_number);
    }
    EncodeFixed64(edited.data() + kServedCountOffset, served.size());
    reseal(&edited);
    return edited;
  };
  ASSERT_OK(
      BlobRelocationDirectory::DecodeFrom(with_served({10, 30}), &decoded));
  for (const auto& served :
       std::vector<std::vector<uint64_t>>{{30, 10}, {10, 10}, {0, 10}}) {
    ASSERT_TRUE(
        BlobRelocationDirectory::DecodeFrom(with_served(served), &decoded)
            .IsCorruption());
  }
  for (const uint64_t second_origin : {uint64_t{10}, uint64_t{5}}) {
    std::string edited = encoded;
    EncodeFixed64(edited.data() + kDirectoryHeaderSize + 40, second_origin);
    reseal(&edited);
    ASSERT_TRUE(
        BlobRelocationDirectory::DecodeFrom(edited, &decoded).IsCorruption());
  }

  EXPECT_LT(EncodeBlobRelocationKey(10, 99), EncodeBlobRelocationKey(11, 1));
  uint64_t origin_file_number = 0;
  uint64_t original_value_offset = 0;
  ASSERT_OK(DecodeBlobRelocationKey(EncodeBlobRelocationKey(10, 99),
                                    &origin_file_number,
                                    &original_value_offset));
  EXPECT_EQ(origin_file_number, 10U);
  EXPECT_EQ(original_value_offset, 99U);
}

class BlobFileReaderTest : public testing::Test {
 protected:
  BlobFileReaderTest() { mock_env_.reset(MockEnv::Create(Env::Default())); }
  std::unique_ptr<Env> mock_env_;
};

TEST_F(BlobFileReaderTest, RelocationTableBuilderChecksServedFilesAndOrigins) {
  // A relocation file serves at least one old file number, and every origin
  // and every served file number is older than the file.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_RelocationBuilderChecks"),
      0);
  BlockBasedTableOptions table_options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint64_t kFileNumber = 300;
  enum class Mode { kNoServedFile, kServesItself, kNewerOrigin };
  for (const Mode mode :
       {Mode::kNoServedFile, Mode::kServesItself, Mode::kNewerOrigin}) {
    SCOPED_TRACE(static_cast<int>(mode));
    const std::string file_path =
        BlobFileName(immutable_options.cf_paths.front().path, kFileNumber);
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(immutable_options.fs->NewWritableFile(file_path, FileOptions(),
                                                    &file, /*dbg=*/nullptr));
    WritableFileWriter writer(std::move(file), file_path, FileOptions(),
                              immutable_options.clock);
    const InternalKeyComparator comparator(BytewiseComparator());
    std::unique_ptr<BlockBasedTableBuilder> builder;
    BlobRelocationTableFileInfo file_info;
    file_info.column_family_id = 1;
    file_info.column_family_name = kDefaultColumnFamilyName;
    file_info.db_id = "test-db";
    file_info.db_session_id = "test-session";
    file_info.file_number = kFileNumber;
    ASSERT_OK(NewBlobRelocationTableBuilder(
        immutable_options, mutable_cf_options, table_options, ReadOptions(),
        WriteOptions(), comparator, file_info, &writer, &builder));
    const uint64_t origin = mode == Mode::kNewerOrigin ? 400 : 100;
    const Status added =
        builder->AddBlobRelocation(origin, 10, "value", kNoCompression);
    if (mode == Mode::kNewerOrigin) {
      ASSERT_TRUE(added.IsInvalidArgument()) << added.ToString();
      builder->Abandon();
      continue;
    }
    ASSERT_OK(added);
    if (mode == Mode::kServesItself) {
      ASSERT_TRUE(builder->AddBlobRelocationServedFile(kFileNumber)
                      .IsInvalidArgument());
      builder->Abandon();
      continue;
    }
    ASSERT_FALSE(builder->Finish().ok());
    BlobRelocationFileMetadata metadata;
    ASSERT_FALSE(builder->GetBlobRelocationFileMetadata(&metadata).ok());
  }
}

TEST(BlobRelocationKeyTest, BufferEncodingMatchesStringEncoding) {
  for (const auto& [origin, offset] :
       std::vector<std::pair<uint64_t, uint64_t>>{
           {1, 0}, {42, 123456}, {0x3fffffffffffffff, ~uint64_t{0}}}) {
    char buffer[kBlobRelocationKeySize];
    EncodeBlobRelocationKeyTo(origin, offset, buffer);
    EXPECT_EQ(Slice(buffer, sizeof(buffer)),
              Slice(EncodeBlobRelocationKey(origin, offset)));
  }
}

TEST(BlobRelocationGroupTest, RoundTripAndFind) {
  // Values at original offsets 10, 25 and 1000, with record offsets that
  // follow each other (stored size + 5 trailer bytes).
  const std::array<BlobRelocationGroupValue, 3> values{
      {{10, 7}, {25, 100}, {1000, 1}}};
  std::string encoded;
  EncodeBlobRelocationGroup(/*first_record_offset=*/8, values.data(),
                            values.size(), &encoded);
  BlobRelocationGroup group;
  ASSERT_OK(DecodeBlobRelocationGroup(encoded, 1000, &group));
  ASSERT_EQ(group.count, 3U);
  const uint64_t expected_record_offsets[] = {8, 8 + 7 + 5,
                                              8 + 7 + 5 + 100 + 5};
  for (size_t i = 0; i < values.size(); ++i) {
    const BlobRelocationGroup::Value* found =
        group.Find(values[i].original_value_offset);
    ASSERT_NE(found, nullptr) << i;
    EXPECT_EQ(found->stored_value_size, values[i].stored_value_size);
    EXPECT_EQ(found->record_offset, expected_record_offsets[i]);
  }
  for (uint64_t absent : {0, 9, 11, 26, 999, 1001}) {
    EXPECT_EQ(group.Find(absent), nullptr) << absent;
  }
  // The key must hold the last value's offset.
  EXPECT_TRUE(DecodeBlobRelocationGroup(encoded, 25, &group).IsCorruption());

  // The gap is counted from the end of the value before. A value at offset 0,
  // an adjacent value (gap 0) and a large gap round-trip:
  // - offset 0, size 7: gap 0 (the end before the first value is 0);
  // - offset 7, size 3: gap 0 (the first value ends at 7);
  // - offset 1000000, size 1: gap 1000000 - 10 (the second value ends at 10).
  const std::array<BlobRelocationGroupValue, 3> edge_values{
      {{0, 7}, {7, 3}, {1000000, 1}}};
  EncodeBlobRelocationGroup(/*first_record_offset=*/8, edge_values.data(),
                            edge_values.size(), &encoded);
  std::string expected;
  PutVarint32(&expected, 3);
  PutVarint64(&expected, 8);
  for (const auto& [gap, size] : std::vector<std::pair<uint64_t, uint64_t>>{
           {0, 7}, {0, 3}, {1000000 - 10, 1}}) {
    PutVarint64(&expected, gap);
    PutVarint64(&expected, size);
  }
  EXPECT_EQ(encoded, expected);
  ASSERT_OK(DecodeBlobRelocationGroup(encoded, 1000000, &group));
  ASSERT_EQ(group.count, 3U);
  for (const BlobRelocationGroupValue& value : edge_values) {
    const BlobRelocationGroup::Value* found =
        group.Find(value.original_value_offset);
    ASSERT_NE(found, nullptr) << value.original_value_offset;
    EXPECT_EQ(found->stored_value_size, value.stored_value_size);
  }

  // A full group of kBlobRelocationGroupSize values.
  std::vector<BlobRelocationGroupValue> full;
  for (size_t i = 0; i < kBlobRelocationGroupSize; ++i) {
    full.push_back({100 + i * 50, 40});
  }
  EncodeBlobRelocationGroup(8, full.data(), full.size(), &encoded);
  ASSERT_OK(DecodeBlobRelocationGroup(
      encoded, full.back().original_value_offset, &group));
  EXPECT_EQ(group.count, kBlobRelocationGroupSize);
}

TEST(BlobRelocationGroupTest, RejectsMalformedEntries) {
  // Each case builds a raw entry value; all must fail with Corruption.
  const auto raw = [](uint32_t count, uint64_t first_record_offset,
                      const std::vector<std::pair<uint64_t, uint64_t>>& pairs,
                      const std::string& extra = std::string()) {
    std::string value;
    PutVarint32(&value, count);
    PutVarint64(&value, first_record_offset);
    for (const auto& pair : pairs) {
      PutVarint64(&value, pair.first);
      PutVarint64(&value, pair.second);
    }
    value.append(extra);
    return value;
  };
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  struct Case {
    const char* name;
    std::string value;
    uint64_t last_offset;
  };
  std::vector<std::pair<uint64_t, uint64_t>> too_many;
  for (size_t i = 0; i <= kBlobRelocationGroupSize; ++i) {
    too_many.emplace_back(1, 1);
  }
  const std::vector<Case> cases{
      {"empty", std::string(), 10},
      {"count 0", raw(0, 8, {}), 0},
      {"count 33", raw(kBlobRelocationGroupSize + 1, 8, too_many),
       kBlobRelocationGroupSize + 1},
      {"value end overflow", raw(2, 8, {{kMax, 1}, {1, 1}}), 0},
      {"gap overflow", raw(2, 8, {{10, 1}, {kMax, 1}}), 0},
      {"size 0", raw(1, 8, {{10, 0}}), 10},
      {"size too large", raw(1, 8, {{10, kMaxSimpleGen2BlobPayloadSize + 1}}),
       10},
      {"record offset overflow", raw(2, kMax - 3, {{10, 1}, {5, 1}}), 16},
      {"truncated", raw(2, 8, {{10, 1}}), 10},
      {"extra bytes", raw(1, 8, {{10, 1}}, "x"), 10},
      {"last offset mismatch", raw(1, 8, {{10, 1}}), 11},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.name);
    BlobRelocationGroup group;
    EXPECT_TRUE(DecodeBlobRelocationGroup(c.value, c.last_offset, &group)
                    .IsCorruption());
  }
}

TEST(BlobRelocationGroupTest, DecoderFuzz) {
  // Random truncations and byte flips of valid entries must give OK or
  // Corruption, never a crash, and an OK result keeps the decoder rules.
  constexpr uint32_t kSeed = 301;
  SCOPED_TRACE(kSeed);
  Random random(kSeed);
  for (int round = 0; round < 2000; ++round) {
    std::vector<BlobRelocationGroupValue> values;
    const size_t count = 1 + random.Uniform(kBlobRelocationGroupSize);
    // Each value after the first starts 0..63 bytes after the end of the value
    // before.
    uint64_t end = random.Uniform(1000);
    for (size_t i = 0; i < count; ++i) {
      const uint64_t offset = end + random.Uniform(64);
      const uint64_t size = 1 + random.Uniform(4096);
      values.push_back({offset, size});
      end = offset + size;
    }
    std::string encoded;
    EncodeBlobRelocationGroup(8 + random.Uniform(1 << 20), values.data(),
                              values.size(), &encoded);
    if (random.OneIn(2)) {
      encoded.resize(random.Uniform(static_cast<int>(encoded.size())));
    } else {
      encoded[random.Uniform(static_cast<int>(encoded.size()))] ^=
          static_cast<char>(1 + random.Uniform(255));
    }
    BlobRelocationGroup group;
    const Status status = DecodeBlobRelocationGroup(
        encoded, values.back().original_value_offset, &group);
    if (status.ok()) {
      ASSERT_GE(group.count, 1U);
      ASSERT_LE(group.count, kBlobRelocationGroupSize);
      for (size_t i = 0; i < group.count; ++i) {
        ASSERT_GE(group.values[i].stored_value_size, 1U);
        ASSERT_TRUE(i == 0 || group.values[i].original_value_offset >
                                  group.values[i - 1].original_value_offset);
      }
    } else {
      ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    }
  }
}

TEST_F(BlobFileReaderTest, RelocationLocationEntriesCoverGroupsOfOneOrigin) {
  // Origin 100 has 70 values (groups of 32, 32 and 6), origin 101 has 32 (one
  // full group) and origin 102 has 1. Every value reads back, offsets that
  // are not in the file give "entry is missing", the table has one entry per
  // group, and a batch decodes each location entry once.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "RelocationLocationEntries"), 0);
  options.enable_blob_files = true;
  BlockBasedTableOptions table_options;
  table_options.format_version = 7;
  table_options.checksum = kCRC32c;
  table_options.block_cache = NewLRUCache(8 << 20);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t relocation_file_number = 400;
  const std::vector<std::pair<uint64_t, int>> origins{
      {100, 70}, {101, 32}, {102, 1}};
  std::vector<std::string> payloads;
  std::vector<RelocationTestRecord> records;
  for (const auto& origin : origins) {
    for (int i = 0; i < origin.second; ++i) {
      payloads.push_back("value-" + std::to_string(origin.first) + "-" +
                         std::to_string(i));
    }
  }
  size_t next = 0;
  for (const auto& origin : origins) {
    for (int i = 0; i < origin.second; ++i) {
      records.push_back({origin.first, static_cast<uint64_t>(10 + i * 100),
                         payloads[next++], kNoCompression});
    }
  }
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      column_family_id, relocation_file_number, records,
                      &metadata, &file_size);
  std::unique_ptr<BlobFileReader> reader;
  ASSERT_OK(OpenRelocationReader(
      immutable_options, ReadOptions(), FileOptions(), column_family_id,
      RelocationPhysicalFile(relocation_file_number, file_size, metadata),
      /*db_session_id=*/"reader", table_options,
      mutable_cf_options.compression_manager.get(), &reader));
  ASSERT_EQ(reader->relocation()->total_entries(), records.size());

  const auto make_index = [](uint64_t origin, uint64_t offset,
                             const std::string& payload, BlobIndex* index) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, origin,
        {.origin_file_number = origin,
         .original_value_offset = offset,
         .stored_value_size = payload.size(),
         .compression = kNoCompression,
         .stable_checksum = crc32c::Value(payload.data(), payload.size()),
         .stored_checksum =
             BlobIndex::ComputeStoredChecksum(payload, kNoCompression)});
    ASSERT_OK(index->DecodeFrom(encoded));
  };
  std::vector<BlobIndex> indexes(records.size());
  for (size_t i = 0; i < records.size(); ++i) {
    make_index(records[i].origin_file_number, records[i].original_value_offset,
               payloads[i], &indexes[i]);
  }
  std::atomic<int> decodes{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobRelocationFileReader::CheckEntry:Decode",
      [&](void*) { decodes.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  const auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  // The first 32 values of origin 100 are one group: one decode for the
  // batch.
  {
    std::vector<SameFileBlobStandaloneRead> outputs(32);
    std::vector<Status> statuses(32);
    std::vector<RelocationBlobReadRequest> requests;
    for (size_t i = 0; i < 32; ++i) {
      requests.push_back(MakeRelocationRequest(&indexes[i], 0, kWholeBlobLength,
                                               &statuses[i], &outputs[i]));
    }
    reader->relocation()->MultiGetBlob(ReadOptions(), /*allocator=*/nullptr,
                                       requests.size(), requests.data(),
                                       /*bytes_read=*/nullptr);
    for (size_t i = 0; i < 32; ++i) {
      ASSERT_OK(statuses[i]) << i;
      EXPECT_EQ(outputs[i].contents->data(), payloads[i]) << i;
    }
    EXPECT_EQ(decodes.load(), 1);
  }
  for (size_t i = 0; i < records.size(); ++i) {
    std::unique_ptr<BlobContents> value;
    ASSERT_OK(GetFromRelocationFile(*reader, ReadOptions(), indexes[i],
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &value,
                                    /*bytes_read=*/nullptr))
        << i;
    EXPECT_EQ(value->data(), payloads[i]) << i;
  }
  // Absent offsets: inside a group, between groups, after the last value of
  // an origin, before the first value, and an origin the file does not hold.
  const std::vector<std::pair<uint64_t, uint64_t>> absent{
      {100, 15}, {100, 3115}, {100, 6915}, {101, 5}, {102, 11}, {103, 10}};
  for (const auto& missing : absent) {
    SCOPED_TRACE(std::to_string(missing.first) + ":" +
                 std::to_string(missing.second));
    BlobIndex index;
    make_index(missing.first, missing.second, payloads[0], &index);
    std::unique_ptr<BlobContents> value;
    const Status status = GetFromRelocationFile(
        *reader, ReadOptions(), index, /*range_offset=*/0, kWholeBlobLength,
        /*allocator=*/nullptr, &value, /*bytes_read=*/nullptr);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    EXPECT_NE(status.ToString().find("entry is missing"), std::string::npos)
        << status.ToString();
  }
  // One location entry per group: 3 + 1 + 1.
  std::unique_ptr<RandomAccessFileReader> file_reader;
  ASSERT_OK(RandomAccessFileReader::Create(
      immutable_options.fs,
      BlobFileName(immutable_options.cf_paths.front().path,
                   relocation_file_number),
      FileOptions(), &file_reader, /*dbg=*/nullptr));
  std::unique_ptr<TableProperties> properties;
  ASSERT_OK(ReadTableProperties(file_reader.get(), file_size,
                                kBlockBasedTableMagicNumber, immutable_options,
                                ReadOptions(), &properties));
  EXPECT_EQ(properties->num_entries, 5U);
}

TEST_F(BlobFileReaderTest, RelocationLocationEntryNeedsValueTypeAndSequence0) {
  // A location entry key must have type kTypeValue and sequence 0. A file
  // whose entry has type kTypeBlobIndex, or sequence 5, fails the lookup with
  // Corruption.
  for (const auto& footer : {PackSequenceAndType(0, kTypeBlobIndex),
                             PackSequenceAndType(5, kTypeValue)}) {
    SCOPED_TRACE(footer);
    Options options;
    options.env = mock_env_.get();
    options.cf_paths.emplace_back(
        test::PerThreadDBPath(
            mock_env_.get(), "RelocationEntryKeyType" + std::to_string(footer)),
        0);
    options.enable_blob_files = true;
    BlockBasedTableOptions table_options;
    table_options.format_version = 7;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    const ImmutableOptions immutable_options(options);
    const MutableCFOptions mutable_cf_options(options);
    const std::string payload = "value";
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::FinishBlobRelocationGroup:InternalKey",
        [footer](void* arg) {
          EncodeFixed64(static_cast<char*>(arg) + kBlobRelocationKeySize,
                        footer);
        });
    SyncPoint::GetInstance()->EnableProcessing();
    BlobRelocationFileMetadata metadata;
    uint64_t file_size = 0;
    WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                        /*column_family_id=*/1, /*blob_file_number=*/400,
                        {{100, 10, payload, kNoCompression}}, &metadata,
                        &file_size);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    std::unique_ptr<BlobFileReader> reader;
    ASSERT_OK(OpenRelocationReader(
        immutable_options, ReadOptions(), FileOptions(), /*column_family_id=*/1,
        RelocationPhysicalFile(400, file_size, metadata),
        /*db_session_id=*/"reader", table_options,
        mutable_cf_options.compression_manager.get(), &reader));
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, 100,
        {.origin_file_number = 100,
         .original_value_offset = 10,
         .stored_value_size = payload.size(),
         .compression = kNoCompression,
         .stable_checksum = crc32c::Value(payload.data(), payload.size()),
         .stored_checksum =
             BlobIndex::ComputeStoredChecksum(payload, kNoCompression)});
    BlobIndex index;
    ASSERT_OK(index.DecodeFrom(encoded));
    std::unique_ptr<BlobContents> value;
    const Status status = GetFromRelocationFile(
        *reader, ReadOptions(), index, /*range_offset=*/0, kWholeBlobLength,
        /*allocator=*/nullptr, &value, /*bytes_read=*/nullptr);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    EXPECT_NE(status.ToString().find("Invalid blob relocation entry key"),
              std::string::npos)
        << status.ToString();
  }
}

TEST_F(BlobFileReaderTest, RelocationTableOptionsUseStandardIndexOnly) {
  // Relocation files never carry a user-defined index, so a CF configured
  // with a custom index mode must still produce valid relocation options.
  for (const auto mode :
       {BlockBasedTableOptions::IndexMode::kCustomDefault,
        BlockBasedTableOptions::IndexMode::kCustomOnly,
        BlockBasedTableOptions::IndexMode::kStandardRequired}) {
    BlockBasedTableOptions source;
    source.index_mode = mode;
    source.use_udi_as_primary_index = true;
    const BlockBasedTableOptions relocation =
        MakeBlobRelocationTableOptions(source);
    ASSERT_EQ(relocation.GetEffectiveIndexMode(),
              BlockBasedTableOptions::IndexMode::kStandardOnly);
    ASSERT_EQ(relocation.user_defined_index_factory, nullptr);
    BlockBasedTableFactory factory(relocation);
    ASSERT_OK(factory.ValidateOptions(DBOptions(), ColumnFamilyOptions()));
  }
}

TEST_F(BlobFileReaderTest, RelocationTableOptionsForcePointLookupLayout) {
  // MakeBlobRelocationTableOptions forces the relocation table layout (no block
  // alignment, binary search data blocks with no hash index, no index key
  // shortening, 4 KB blocks, format_version >= 6), whatever the column family
  // sets.
  BlockBasedTableOptions source;
  source.block_align = true;
  source.super_block_alignment_size = 1 << 20;
  source.block_restart_interval = 1;
  source.data_block_index_type = BlockBasedTableOptions::kDataBlockBinarySearch;
  source.block_size = 64 * 1024;
  const BlockBasedTableOptions relocation =
      MakeBlobRelocationTableOptions(source);
  EXPECT_FALSE(relocation.block_align);
  EXPECT_EQ(relocation.super_block_alignment_size, 0U);
  // The CF restart interval is kept.
  EXPECT_EQ(relocation.block_restart_interval, 1);
  // Lookups land between location entry keys: no hash index, and the index
  // key of a block is its last key.
  EXPECT_EQ(relocation.data_block_index_type,
            BlockBasedTableOptions::kDataBlockBinarySearch);
  EXPECT_EQ(relocation.index_shortening,
            BlockBasedTableOptions::IndexShorteningMode::kNoShortening);
  EXPECT_EQ(relocation.block_size, 4U * 1024);
  EXPECT_GE(relocation.format_version, 6U);
}

TEST_F(BlobFileReaderTest, RelocationIndexPinningFollowsTableOptions) {
  // With cache_index_and_filter_blocks, the index block of a relocation file
  // lives in the block cache. By default it is not pinned, as for a non-L0
  // SST, so each read looks it up in the block cache. With
  // unpartitioned_pinning = kAll it is pinned, so reads do not look it up.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_RelocationIndexPinning"),
      0);
  options.enable_blob_files = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.format_version = 7;
  table_options.checksum = kCRC32c;
  table_options.cache_index_and_filter_blocks = true;
  table_options.block_cache = NewLRUCache(8 << 20);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t relocation_file_number = 400;
  constexpr uint64_t origin = 100;
  const std::string payload = "pinned-index-value";
  const std::vector<RelocationTestRecord> records{
      {origin, /*original_value_offset=*/10, payload, kNoCompression}};
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      column_family_id, relocation_file_number, records,
                      &metadata, &file_size);
  std::string encoded;
  BlobIndex::EncodeIndirectBlob(
      &encoded, origin,
      {.origin_file_number = origin,
       .original_value_offset = 10,
       .stored_value_size = payload.size(),
       .compression = kNoCompression,
       .stable_checksum = crc32c::Value(payload.data(), payload.size()),
       .stored_checksum =
           BlobIndex::ComputeStoredChecksum(payload, kNoCompression)});
  BlobIndex index;
  ASSERT_OK(index.DecodeFrom(encoded));
  Statistics* const stats = options.statistics.get();
  for (const bool pin_all : {false, true}) {
    SCOPED_TRACE(pin_all);
    BlockBasedTableOptions reader_options = table_options;
    reader_options.block_cache = NewLRUCache(8 << 20);
    if (pin_all) {
      reader_options.metadata_cache_options.unpartitioned_pinning =
          PinningTier::kAll;
    }
    std::unique_ptr<BlobFileReader> reader;
    ASSERT_OK(OpenRelocationReader(
        immutable_options, ReadOptions(), FileOptions(), column_family_id,
        RelocationPhysicalFile(relocation_file_number, file_size, metadata),
        /*db_session_id=*/"reader", reader_options,
        mutable_cf_options.compression_manager.get(), &reader));
    std::unique_ptr<BlobContents> value;
    // The first read warms the data block.
    ASSERT_OK(GetFromRelocationFile(*reader, ReadOptions(), index,
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &value,
                                    /*bytes_read=*/nullptr));
    const uint64_t index_lookups =
        stats->getTickerCount(BLOCK_CACHE_INDEX_HIT) +
        stats->getTickerCount(BLOCK_CACHE_INDEX_MISS);
    value.reset();
    ASSERT_OK(GetFromRelocationFile(*reader, ReadOptions(), index,
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &value,
                                    /*bytes_read=*/nullptr));
    ASSERT_EQ(value->data(), payload);
    const uint64_t delta = stats->getTickerCount(BLOCK_CACHE_INDEX_HIT) +
                           stats->getTickerCount(BLOCK_CACHE_INDEX_MISS) -
                           index_lookups;
    EXPECT_EQ(delta, pin_all ? 0U : 1U);
  }
}

TEST_F(BlobFileReaderTest, RelocationMultiGetReadsColdDataBlocksInOneBatch) {
  // Relocation MultiGet looks up all keys of a batch first. It reads the data
  // blocks that are not in the block cache with one batched read. Blocks that
  // are already in the block cache need no read. Keys in the same block, and
  // duplicate keys, share one block read.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_RelocationMultiGetColdBlocks"),
      0);
  options.enable_blob_files = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.format_version = 7;
  table_options.checksum = kCRC32c;
  table_options.block_cache = NewLRUCache(8 << 20);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t relocation_file_number = 400;
  constexpr uint64_t origin = 100;
  // A location entry holds 32 small values (about 85 bytes with its key, an
  // estimate), so 40000 values (1250 entries) fill about 25 data blocks of
  // 4 KB.
  constexpr int kRecords = 40000;
  std::vector<std::string> payloads;
  std::vector<RelocationTestRecord> records;
  payloads.reserve(kRecords);
  for (int i = 0; i < kRecords; ++i) {
    payloads.push_back("value-" + std::to_string(1000000 + i));
  }
  for (int i = 0; i < kRecords; ++i) {
    records.push_back({origin, static_cast<uint64_t>(10 + i * 100), payloads[i],
                       kNoCompression});
  }
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      column_family_id, relocation_file_number, records,
                      &metadata, &file_size);
  const auto open_reader = [&](const BlockBasedTableOptions& reader_options,
                               std::unique_ptr<BlobFileReader>* reader) {
    ASSERT_OK(OpenRelocationReader(
        immutable_options, ReadOptions(), FileOptions(), column_family_id,
        RelocationPhysicalFile(relocation_file_number, file_size, metadata),
        /*db_session_id=*/"reader", reader_options,
        mutable_cf_options.compression_manager.get(), reader));
  };
  std::unique_ptr<BlobFileReader> reader;
  open_reader(table_options, &reader);

  // 30 keys spread over the file, two keys next to each other (one block)
  // and one duplicate.
  std::vector<int> picks;
  for (int i = 0; i < 30; ++i) {
    picks.push_back(i * 1333);
  }
  picks.push_back(1);
  picks.push_back(1333);
  std::vector<BlobIndex> indexes(picks.size());
  for (size_t i = 0; i < picks.size(); ++i) {
    const std::string& payload = payloads[picks[i]];
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, origin,
        {.origin_file_number = origin,
         .original_value_offset = records[picks[i]].original_value_offset,
         .stored_value_size = payload.size(),
         .compression = kNoCompression,
         .stable_checksum = crc32c::Value(payload.data(), payload.size()),
         .stored_checksum =
             BlobIndex::ComputeStoredChecksum(payload, kNoCompression)});
    ASSERT_OK(indexes[i].DecodeFrom(encoded));
  }
  std::vector<SameFileBlobStandaloneRead> outputs(picks.size());
  std::vector<Status> statuses(picks.size());
  std::vector<RelocationBlobReadRequest> requests(picks.size());
  const auto reset_requests = [&]() {
    for (size_t i = 0; i < picks.size(); ++i) {
      outputs[i].contents.reset();
      statuses[i] = Status::OK();
      requests[i] = MakeRelocationRequest(&indexes[i], 0, kWholeBlobLength,
                                          &statuses[i], &outputs[i]);
    }
  };
  size_t batched_reads = 0;
  size_t blocks_read = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MultiGetRawEntries:RetrieveMultipleBlocks",
      [&](void* arg) {
        ++batched_reads;
        for (const BlockHandle& handle :
             *static_cast<
                 autovector<BlockHandle, MultiGetContext::MAX_BATCH_SIZE>*>(
                 arg)) {
          blocks_read += handle.IsNull() ? 0 : 1;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions read_options;
  read_options.verify_checksums = true;
  reset_requests();
  reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                     requests.size(), requests.data(),
                                     /*bytes_read=*/nullptr);
  for (size_t i = 0; i < picks.size(); ++i) {
    ASSERT_OK(statuses[i]) << i;
    EXPECT_EQ(outputs[i].contents->data(), payloads[picks[i]]) << i;
  }
  // Cold: one batched read of many distinct blocks (the duplicate and the
  // neighbor need no extra block).
  EXPECT_EQ(batched_reads, 1U);
  EXPECT_GT(blocks_read, 10U);
  EXPECT_LT(blocks_read, picks.size() - 1);

  // Warm: the blocks are in the block cache, so no block read.
  batched_reads = 0;
  reset_requests();
  reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                     requests.size(), requests.data(),
                                     /*bytes_read=*/nullptr);
  for (size_t i = 0; i < picks.size(); ++i) {
    ASSERT_OK(statuses[i]) << i;
  }
  EXPECT_EQ(batched_reads, 0U);

  // An offset inside the directory range of its origin but not in the table:
  // the location entry found does not hold it.
  {
    const std::string& payload = payloads[picks[3]];
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, origin,
        {.origin_file_number = origin,
         .original_value_offset = records[picks[3]].original_value_offset + 1,
         .stored_value_size = payload.size(),
         .compression = kNoCompression,
         .stable_checksum = crc32c::Value(payload.data(), payload.size()),
         .stored_checksum =
             BlobIndex::ComputeStoredChecksum(payload, kNoCompression)});
    BlobIndex absent;
    ASSERT_OK(absent.DecodeFrom(encoded));
    SameFileBlobStandaloneRead absent_output;
    Status absent_status;
    RelocationBlobReadRequest absent_request = MakeRelocationRequest(
        &absent, 0, kWholeBlobLength, &absent_status, &absent_output);
    reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                       /*num_reads=*/1, &absent_request,
                                       /*bytes_read=*/nullptr);
    EXPECT_TRUE(absent_status.IsCorruption()) << absent_status.ToString();
    EXPECT_NE(absent_status.ToString().find("entry is missing"),
              std::string::npos)
        << absent_status.ToString();
    EXPECT_EQ(absent_output.contents, nullptr);
  }

  // A reader with a cold block cache and kBlockCacheTier: Incomplete.
  BlockBasedTableOptions cold_options = table_options;
  cold_options.block_cache = NewLRUCache(8 << 20);
  std::unique_ptr<BlobFileReader> cold_reader;
  open_reader(cold_options, &cold_reader);
  ReadOptions no_io = read_options;
  no_io.read_tier = kBlockCacheTier;
  reset_requests();
  cold_reader->relocation()->MultiGetBlob(no_io, /*allocator=*/nullptr,
                                          requests.size(), requests.data(),
                                          /*bytes_read=*/nullptr);
  for (size_t i = 0; i < picks.size(); ++i) {
    EXPECT_TRUE(statuses[i].IsIncomplete()) << i << statuses[i].ToString();
  }

  // A single relocated Get with a cold cache does one data block read on the
  // normal block path and one plain positional record read, and no batched
  // block read. The block cache counts exactly one miss, so the read path
  // does not look the block up a second time.
  {
    BlockBasedTableOptions single_options = table_options;
    single_options.block_cache = NewLRUCache(8 << 20);
    std::unique_ptr<BlobFileReader> single_reader;
    open_reader(single_options, &single_reader);
    size_t single_reads = 0;
    size_t multi_reads = 0;
    SyncPoint::GetInstance()->SetCallBack("MultiReadGen2:SingleRead",
                                          [&](void*) { ++single_reads; });
    SyncPoint::GetInstance()->SetCallBack("MultiReadGen2:Requests",
                                          [&](void*) { ++multi_reads; });
    batched_reads = 0;
    Statistics* const stats = options.statistics.get();
    const uint64_t misses = stats->getTickerCount(BLOCK_CACHE_MISS);
    const uint64_t data_misses = stats->getTickerCount(BLOCK_CACHE_DATA_MISS);
    // Block cache misses are counted per level in the perf context.
    SetPerfLevel(PerfLevel::kEnableCount);
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    std::unique_ptr<BlobContents> value;
    ASSERT_OK(GetFromRelocationFile(*single_reader, read_options, indexes[3],
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &value,
                                    /*bytes_read=*/nullptr));
    uint64_t perf_misses = 0;
    for (const auto& level : *get_perf_context()->level_to_perf_context) {
      perf_misses += level.second.block_cache_miss_count;
    }
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->DisablePerLevelPerfContext();
    SetPerfLevel(PerfLevel::kDisable);
    EXPECT_EQ(value->data(), payloads[picks[3]]);
    EXPECT_EQ(batched_reads, 0U);
    EXPECT_EQ(multi_reads, 1U);
    EXPECT_EQ(single_reads, 1U);
    EXPECT_EQ(stats->getTickerCount(BLOCK_CACHE_MISS), misses + 1);
    EXPECT_EQ(stats->getTickerCount(BLOCK_CACHE_DATA_MISS), data_misses + 1);
    EXPECT_EQ(perf_misses, 1U);
    SyncPoint::GetInstance()->ClearCallBack("MultiReadGen2:SingleRead");
    SyncPoint::GetInstance()->ClearCallBack("MultiReadGen2:Requests");
  }

  // A corrupt data block fails only the keys in that block.
  size_t verified_blocks = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RetrieveMultipleBlocks:VerifyChecksum", [&](void* arg) {
        if (verified_blocks++ == 1) {
          *static_cast<Status*>(arg) = Status::Corruption("injected");
        }
      });
  reset_requests();
  cold_reader->relocation()->MultiGetBlob(read_options,
                                          /*allocator=*/nullptr,
                                          requests.size(), requests.data(),
                                          /*bytes_read=*/nullptr);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  size_t failed = 0;
  for (size_t i = 0; i < picks.size(); ++i) {
    if (statuses[i].ok()) {
      EXPECT_EQ(outputs[i].contents->data(), payloads[picks[i]]) << i;
    } else {
      EXPECT_TRUE(statuses[i].IsCorruption()) << i;
      ++failed;
    }
  }
  EXPECT_GE(failed, 1U);
  EXPECT_LT(failed, 4U);

  // A damaged record fails its checksum on the single-read path (one Get)
  // and on the batched path (two records), and only for that record.
  const std::string file_path = BlobFileName(
      immutable_options.cf_paths.front().path, relocation_file_number);
  std::string file_contents;
  ASSERT_OK(
      ReadFileToString(immutable_options.fs.get(), file_path, &file_contents));
  const size_t payload_pos = file_contents.find(payloads[picks[3]]);
  ASSERT_NE(payload_pos, std::string::npos);
  file_contents[payload_pos + 2] ^= 0x1;
  ASSERT_OK(
      WriteStringToFile(immutable_options.fs.get(), file_contents, file_path));
  BlockBasedTableOptions damaged_options = table_options;
  damaged_options.block_cache = NewLRUCache(8 << 20);
  std::unique_ptr<BlobFileReader> damaged_reader;
  open_reader(damaged_options, &damaged_reader);
  std::unique_ptr<BlobContents> damaged_value;
  EXPECT_TRUE(GetFromRelocationFile(*damaged_reader, read_options, indexes[3],
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &damaged_value,
                                    /*bytes_read=*/nullptr)
                  .IsCorruption());
  std::array<SameFileBlobStandaloneRead, 2> pair_outputs;
  std::array<Status, 2> pair_statuses;
  std::array<RelocationBlobReadRequest, 2> pair_requests{
      MakeRelocationRequest(&indexes[3], 0, kWholeBlobLength, &pair_statuses[0],
                            &pair_outputs[0]),
      MakeRelocationRequest(&indexes[4], 0, kWholeBlobLength, &pair_statuses[1],
                            &pair_outputs[1])};
  damaged_reader->relocation()->MultiGetBlob(
      read_options, /*allocator=*/nullptr, pair_requests.size(),
      pair_requests.data(), /*bytes_read=*/nullptr);
  EXPECT_TRUE(pair_statuses[0].IsCorruption()) << pair_statuses[0].ToString();
  ASSERT_OK(pair_statuses[1]);
  EXPECT_EQ(pair_outputs[1].contents->data(), payloads[picks[4]]);

  // Callers send the internal range functions only strict parts of a value (see
  // ReadPathBlobResolver::ResolveColumnRange), and these functions do not turn
  // a range into a whole read. So a range read checks no checksum, even when
  // the test passes a range that covers the whole uncompressed value. On the
  // damaged record, a range of the whole value and a strict sub-range both
  // return OK, for one request and for a batch (both use
  // BlockBasedTable::MultiGetSameFileBlob()). A range read counts only the
  // range bytes in `bytes_read`.
  const size_t damaged_size = payloads[picks[3]].size();
  ASSERT_OK(GetFromRelocationFile(*damaged_reader, read_options, indexes[3],
                                  /*range_offset=*/0, damaged_size,
                                  /*allocator=*/nullptr, &damaged_value,
                                  /*bytes_read=*/nullptr));
  EXPECT_NE(damaged_value->data(), payloads[picks[3]]);
  uint64_t range_bytes_read = 0;
  ASSERT_OK(GetFromRelocationFile(*damaged_reader, read_options, indexes[3],
                                  /*range_offset=*/0, damaged_size - 1,
                                  /*allocator=*/nullptr, &damaged_value,
                                  &range_bytes_read));
  EXPECT_EQ(range_bytes_read, damaged_size - 1);
  std::unique_ptr<BlobContents> whole_range_value;
  uint64_t whole_range_bytes_read = 0;
  ASSERT_OK(GetFromRelocationFile(*damaged_reader, read_options, indexes[4],
                                  /*range_offset=*/0, payloads[picks[4]].size(),
                                  /*allocator=*/nullptr, &whole_range_value,
                                  &whole_range_bytes_read));
  EXPECT_EQ(whole_range_value->data(), payloads[picks[4]]);
  EXPECT_EQ(whole_range_bytes_read, payloads[picks[4]].size());
  std::array<SameFileBlobStandaloneRead, 3> range_outputs;
  std::array<Status, 3> range_statuses;
  std::array<RelocationBlobReadRequest, 3> range_requests{
      MakeRelocationRequest(&indexes[3], 0, damaged_size, &range_statuses[0],
                            &range_outputs[0]),
      MakeRelocationRequest(&indexes[4], 0, payloads[picks[4]].size(),
                            &range_statuses[1], &range_outputs[1]),
      MakeRelocationRequest(&indexes[3], 0, damaged_size - 1,
                            &range_statuses[2], &range_outputs[2])};
  damaged_reader->relocation()->MultiGetBlob(
      read_options, /*allocator=*/nullptr, range_requests.size(),
      range_requests.data(), /*bytes_read=*/nullptr);
  ASSERT_OK(range_statuses[0]);
  EXPECT_NE(range_outputs[0].contents->data(), payloads[picks[3]]);
  ASSERT_OK(range_statuses[1]);
  EXPECT_EQ(range_outputs[1].contents->data(), payloads[picks[4]]);
  ASSERT_OK(range_statuses[2]);
  EXPECT_EQ(range_outputs[2].contents->size(), damaged_size - 1);
}

enum class RawLookupMode {
  kMmap,
  kDirect,
  kNoBlockCache,
  kNoFillCache,
  kBlockCacheTier
};

class RelocationRawLookupTest
    : public testing::TestWithParam<std::tuple<RawLookupMode, bool>> {};

TEST_P(RelocationRawLookupTest, ReadsCorrectValues) {
  // Relocation MultiGet in each I/O and cache mode, with more keys than one
  // lookup batch (MultiGetContext::MAX_BATCH_SIZE) and a duplicate key, or
  // with a single key (the single block read path).
  const RawLookupMode mode = std::get<0>(GetParam());
  const bool single = std::get<1>(GetParam());
  Env* const env = Env::Default();
  Options options;
  options.env = env;
  const std::string path = test::PerThreadDBPath(
      env, "RelocationRawLookup" + std::to_string(static_cast<int>(mode)) +
               (single ? "s" : "b"));
  ASSERT_OK(DestroyDir(env, path));
  ASSERT_OK(env->CreateDirIfMissing(path));
  options.cf_paths.emplace_back(path, 0);
  options.enable_blob_files = true;
  options.allow_mmap_reads = mode == RawLookupMode::kMmap;
  options.use_direct_reads = mode == RawLookupMode::kDirect;
  BlockBasedTableOptions table_options;
  table_options.format_version = 7;
  table_options.checksum = kCRC32c;
  if (mode == RawLookupMode::kNoBlockCache) {
    table_options.no_block_cache = true;
  } else {
    table_options.block_cache = NewLRUCache(8 << 20);
  }
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t relocation_file_number = 500;
  constexpr uint64_t origin = 100;
  // A location entry holds 32 small values (about 85 bytes with its key, an
  // estimate), so 40000 values (1250 entries) fill about 25 data blocks of
  // 4 KB.
  constexpr int kRecords = 40000;
  std::vector<std::string> payloads;
  std::vector<RelocationTestRecord> records;
  payloads.reserve(kRecords);
  for (int i = 0; i < kRecords; ++i) {
    payloads.push_back("value-" + std::to_string(1000000 + i));
  }
  for (int i = 0; i < kRecords; ++i) {
    records.push_back({origin, static_cast<uint64_t>(10 + i * 100), payloads[i],
                       kNoCompression});
  }
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      column_family_id, relocation_file_number, records,
                      &metadata, &file_size);
  std::unique_ptr<BlobFileReader> reader;
  const Status open_status = OpenRelocationReader(
      immutable_options, ReadOptions(), FileOptions(DBOptions(options)),
      column_family_id,
      RelocationPhysicalFile(relocation_file_number, file_size, metadata),
      /*db_session_id=*/"reader", table_options,
      mutable_cf_options.compression_manager.get(), &reader);
  if (mode == RawLookupMode::kDirect && !open_status.ok()) {
    ROCKSDB_GTEST_SKIP("Direct I/O is not supported here");
    return;
  }
  ASSERT_OK(open_status);

  std::vector<int> picks;
  if (single) {
    picks.push_back(997);
  } else {
    for (int i = 0; i < 40; ++i) {
      picks.push_back(i * 997);
    }
    picks.push_back(997);
  }
  std::vector<BlobIndex> indexes(picks.size());
  for (size_t i = 0; i < picks.size(); ++i) {
    const std::string& payload = payloads[picks[i]];
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, origin,
        {.origin_file_number = origin,
         .original_value_offset = records[picks[i]].original_value_offset,
         .stored_value_size = payload.size(),
         .compression = kNoCompression,
         .stable_checksum = crc32c::Value(payload.data(), payload.size()),
         .stored_checksum =
             BlobIndex::ComputeStoredChecksum(payload, kNoCompression)});
    ASSERT_OK(indexes[i].DecodeFrom(encoded));
  }
  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.fill_cache = mode != RawLookupMode::kNoFillCache;
  if (mode == RawLookupMode::kBlockCacheTier) {
    read_options.read_tier = kBlockCacheTier;
  }
  size_t batched_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MultiGetRawEntries:RetrieveMultipleBlocks",
      [&](void*) { ++batched_reads; });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int round = 0; round < 2; ++round) {
    SCOPED_TRACE(round);
    batched_reads = 0;
    std::vector<SameFileBlobStandaloneRead> outputs(picks.size());
    std::vector<Status> statuses(picks.size());
    std::vector<RelocationBlobReadRequest> requests(picks.size());
    for (size_t i = 0; i < picks.size(); ++i) {
      requests[i] = MakeRelocationRequest(&indexes[i], 0, kWholeBlobLength,
                                          &statuses[i], &outputs[i]);
    }
    SetPerfLevel(PerfLevel::kEnableCount);
    get_perf_context()->Reset();
    reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                       requests.size(), requests.data(),
                                       /*bytes_read=*/nullptr);
    const uint64_t block_reads = get_perf_context()->block_read_count;
    SetPerfLevel(PerfLevel::kDisable);
    if (mode == RawLookupMode::kBlockCacheTier) {
      // The block cache is cold and no I/O is allowed, also for one key.
      for (size_t i = 0; i < picks.size(); ++i) {
        EXPECT_TRUE(statuses[i].IsIncomplete()) << i << statuses[i].ToString();
      }
      EXPECT_EQ(batched_reads, 0U);
      EXPECT_EQ(block_reads, 0U);
      continue;
    }
    for (size_t i = 0; i < picks.size(); ++i) {
      ASSERT_OK(statuses[i]) << i;
      EXPECT_EQ(outputs[i].contents->data(), payloads[picks[i]]) << i;
    }
    // 41 keys are looked up in two batches. Blocks stay in the block cache
    // only with a block cache and fill_cache, and not with mmap reads (as in
    // the normal MultiGet path, mmap blocks are read from the mapping). A
    // single key never takes the batched read.
    const bool cached = mode == RawLookupMode::kDirect;
    if (single || (round > 0 && cached)) {
      EXPECT_EQ(batched_reads, 0U);
    } else {
      EXPECT_EQ(batched_reads, 2U);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(DestroyDir(env, path));
}

INSTANTIATE_TEST_CASE_P(
    BlobFileReaderTest, RelocationRawLookupTest,
    ::testing::Combine(::testing::Values(RawLookupMode::kMmap,
                                         RawLookupMode::kDirect,
                                         RawLookupMode::kNoBlockCache,
                                         RawLookupMode::kNoFillCache,
                                         RawLookupMode::kBlockCacheTier),
                       ::testing::Bool()));

// Writes one relocation file of 40000 small values of one origin for the
// pinned location block tests. A location entry holds 32 values (about 85
// bytes with its key, an estimate), so the file has about 25 data blocks of
// 4 KB, and lookups cross block boundaries.
struct PinnedBlocksTestFile {
  static constexpr uint32_t kColumnFamilyId = 1;
  static constexpr uint64_t kFileNumber = 600;
  static constexpr uint64_t kOrigin = 100;
  static constexpr int kRecords = 40000;

  // With `mmap`, the file is read through allow_mmap_reads and its blocks are
  // not compressed, so the data blocks point into the mapped file.
  PinnedBlocksTestFile(const std::string& name, std::shared_ptr<Cache> cache,
                       bool mmap = false) {
    Env* const env = Env::Default();
    options.env = env;
    path = test::PerThreadDBPath(env, name);
    EXPECT_OK(DestroyDir(env, path));
    EXPECT_OK(env->CreateDirIfMissing(path));
    options.cf_paths.emplace_back(path, 0);
    options.enable_blob_files = true;
    options.allow_mmap_reads = mmap;
    if (mmap) {
      options.compression = kNoCompression;
    }
    table_options.format_version = 7;
    table_options.checksum = kCRC32c;
    if (cache == nullptr) {
      table_options.no_block_cache = true;
    } else {
      table_options.block_cache = std::move(cache);
    }
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    immutable_options = std::make_unique<ImmutableOptions>(options);
    file_options = FileOptions(DBOptions(options));
    const MutableCFOptions mutable_cf_options(options);
    for (int i = 0; i < kRecords; ++i) {
      payloads.push_back("value-" + std::to_string(1000000 + i));
    }
    for (int i = 0; i < kRecords; ++i) {
      records.push_back({kOrigin, static_cast<uint64_t>(10 + i * 100),
                         payloads[i], kNoCompression});
    }
    WriteRelocationFile(*immutable_options, mutable_cf_options, table_options,
                        kColumnFamilyId, kFileNumber, records, &metadata,
                        &file_size);
  }

  Status Open(bool pin, std::unique_ptr<BlobFileReader>* reader,
              const ReadOptions& read_options = ReadOptions()) const {
    return OpenRelocationReader(
        *immutable_options, read_options, FileOptions(DBOptions(options)),
        kColumnFamilyId,
        RelocationPhysicalFile(kFileNumber, file_size, metadata),
        /*db_session_id=*/"reader", table_options,
        /*compression_manager=*/nullptr, reader, pin);
  }

  // Looks up every value, plus one offset after each value that the file does
  // not hold, in batches of 32. Returns one line per lookup: the status, and
  // the value when the lookup succeeded.
  std::vector<std::string> LookUpAll(const BlobFileReader& reader) const {
    std::vector<BlobIndex> indexes;
    for (int i = 0; i < kRecords; ++i) {
      for (uint64_t delta : {uint64_t{0}, uint64_t{1}}) {
        const std::string& payload = payloads[i];
        std::string encoded;
        BlobIndex::EncodeIndirectBlob(
            &encoded, kOrigin,
            {.origin_file_number = kOrigin,
             .original_value_offset = records[i].original_value_offset + delta,
             .stored_value_size = payload.size(),
             .compression = kNoCompression,
             .stable_checksum = crc32c::Value(payload.data(), payload.size()),
             .stored_checksum =
                 BlobIndex::ComputeStoredChecksum(payload, kNoCompression)});
        indexes.emplace_back();
        EXPECT_OK(indexes.back().DecodeFrom(encoded));
      }
    }
    std::vector<std::string> results;
    constexpr size_t kBatch = MultiGetContext::MAX_BATCH_SIZE;
    for (size_t begin = 0; begin < indexes.size(); begin += kBatch) {
      const size_t count = std::min(kBatch, indexes.size() - begin);
      std::vector<Status> statuses(count);
      std::vector<SameFileBlobStandaloneRead> outputs(count);
      std::vector<RelocationBlobReadRequest> requests;
      for (size_t k = 0; k < count; ++k) {
        requests.push_back(MakeRelocationRequest(&indexes[begin + k], 0,
                                                 kWholeBlobLength, &statuses[k],
                                                 &outputs[k]));
      }
      reader.relocation()->MultiGetBlob(ReadOptions(), /*allocator=*/nullptr,
                                        count, requests.data(),
                                        /*bytes_read=*/nullptr);
      for (size_t k = 0; k < count; ++k) {
        results.push_back(statuses[k].ToString() + ":" +
                          (statuses[k].ok()
                               ? outputs[k].contents->data().ToString()
                               : std::string()));
      }
    }
    return results;
  }

  Options options;
  BlockBasedTableOptions table_options;
  std::unique_ptr<ImmutableOptions> immutable_options;
  FileOptions file_options;
  std::string path;
  std::vector<std::string> payloads;
  std::vector<RelocationTestRecord> records;
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
};

TEST(BlobRelocationPinnedBlocksTest, SameResultsAsNormalLookups) {
  // With and without a block cache, a reader that keeps its location blocks
  // returns the same values and errors as a normal reader, for every value of
  // a file with about 25 data blocks and for offsets the file does not hold.
  for (bool with_cache : {true, false}) {
    SCOPED_TRACE(with_cache);
    const PinnedBlocksTestFile file(
        with_cache ? "PinnedSameResultsCache" : "PinnedSameResultsNoCache",
        with_cache ? NewLRUCache(8 << 20) : nullptr);
    std::unique_ptr<BlobFileReader> normal;
    std::unique_ptr<BlobFileReader> pinned;
    ASSERT_OK(file.Open(/*pin=*/false, &normal));
    ASSERT_OK(file.Open(/*pin=*/true, &pinned));
    EXPECT_EQ(normal->relocation()->pinned_location_block_bytes(), 0U);
    EXPECT_GT(pinned->relocation()->pinned_location_block_bytes(), 0U);
    const std::vector<std::string> expected = file.LookUpAll(*normal);
    ASSERT_EQ(expected.size(), 2U * PinnedBlocksTestFile::kRecords);
    EXPECT_EQ(expected[0], "OK:" + file.payloads[0]);
    EXPECT_EQ(expected[1].rfind("Corruption", 0), 0U) << expected[1];
    EXPECT_EQ(file.LookUpAll(*pinned), expected);
  }
}

TEST(BlobRelocationPinnedBlocksTest, PinsWhenOpenReadDoesNotFillCache) {
  // The pin reads with its own options. An open whose ReadOptions has
  // fill_cache == false still keeps every location block in the block cache,
  // and later lookups read no data block. The cold open reads the blocks with
  // batched reads (up to MultiGetContext::MAX_BATCH_SIZE (32) blocks per
  // read).
  std::shared_ptr<Cache> cache = NewLRUCache(8 << 20);
  const PinnedBlocksTestFile file("PinnedNoFillCache", cache);
  ReadOptions open_options;
  open_options.fill_cache = false;
  std::atomic<int> batched_reads{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::MultiGetRawEntries:RetrieveMultipleBlocks",
      [&](void*) { ++batched_reads; });
  SyncPoint::GetInstance()->EnableProcessing();
  std::unique_ptr<BlobFileReader> pinned;
  const Status open_status = file.Open(/*pin=*/true, &pinned, open_options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(open_status);
  EXPECT_GT(batched_reads.load(), 0);
  ASSERT_GT(pinned->relocation()->pinned_location_block_bytes(), 0U);
  EXPECT_GT(cache->GetPinnedUsage(), 0U);
  SetPerfLevel(PerfLevel::kEnableCount);
  get_perf_context()->Reset();
  const std::vector<std::string> results = file.LookUpAll(*pinned);
  EXPECT_EQ(get_perf_context()->block_read_count, 0U);
  SetPerfLevel(PerfLevel::kDisable);
  EXPECT_EQ(results[0], "OK:" + file.payloads[0]);
}

TEST(BlobRelocationPinnedBlocksTest, KeepsMemoryMappedBlocks) {
  // With allow_mmap_reads and no compression, a data block points into the
  // mapped file, and the block cache does not take it. The reader still keeps
  // the blocks (their bytes are in the mapped file, not on the heap) and
  // returns the same results as a normal reader.
  std::shared_ptr<Cache> cache = NewLRUCache(8 << 20);
  const PinnedBlocksTestFile file("PinnedMmap", cache, /*mmap=*/true);
  std::unique_ptr<BlobFileReader> normal;
  std::unique_ptr<BlobFileReader> pinned;
  ASSERT_OK(file.Open(/*pin=*/false, &normal));
  ASSERT_OK(file.Open(/*pin=*/true, &pinned));
  ASSERT_GT(pinned->relocation()->pinned_location_block_bytes(), 0U);
  EXPECT_EQ(cache->GetPinnedUsage(), 0U);
  EXPECT_EQ(file.LookUpAll(*pinned), file.LookUpAll(*normal));
}

TEST(BlobRelocationPinnedBlocksTest, PinDoesNotBlockBlobFileEvict) {
  // A relocation file open holds a stripe mutex of the BlobFileCache while it
  // pins its location blocks. Relocation files use their own stripe mutexes,
  // so Evict() of a normal blob file never waits for that open. The test picks
  // a normal blob file number that would share the stripe of the relocation
  // file if both kinds used one set of 128 stripes, and evicts it while the
  // open waits inside the pin.
  std::shared_ptr<Cache> cache = NewLRUCache(8 << 20);
  const PinnedBlocksTestFile file("PinnedStripes", cache);
  // The readers get their own cache. A reader holds the table options, which
  // hold the block cache, so a reader in the block cache would keep that cache
  // alive forever.
  std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
  BlobFileCache blob_file_cache(
      file_cache.get(), file.immutable_options.get(), &file.file_options,
      PinnedBlocksTestFile::kColumnFamilyId, /*blob_file_read_hist=*/nullptr,
      /*io_tracer=*/nullptr, &file.table_options, /*db_session_id=*/"session",
      /*block_cache_tracer=*/nullptr);
  // The same Striped type, size and hash as one shared set of 128 stripes.
  Striped<CacheAlignedWrapper<port::Mutex>> single_set(128);
  uint64_t relocation_number = PinnedBlocksTestFile::kFileNumber;
  port::Mutex* const relocation_stripe =
      &single_set.Get(GetSliceForKey(&relocation_number));
  uint64_t blob_file_number = relocation_number + 1;
  while (&single_set.Get(GetSliceForKey(&blob_file_number)) !=
         relocation_stripe) {
    ++blob_file_number;
  }

  std::promise<void> pin_started;
  std::promise<void> evict_done;
  std::shared_future<void> evict_done_future = evict_done.get_future();
  SyncPoint::GetInstance()->SetCallBack(
      "BlobRelocationFileReader::Open:PinLocationBlocks", [&](void*) {
        pin_started.set_value();
        evict_done_future.wait();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  MutableCFOptions mutable_cf_options(file.options);
  mutable_cf_options.pin_blob_relocation_location_blocks = true;
  Status open_status;
  std::thread opener([&]() {
    CacheHandleGuard<BlobFileReader> reader;
    open_status = blob_file_cache.GetRelocationFileReader(
        ReadOptions(),
        RelocationPhysicalFile(PinnedBlocksTestFile::kFileNumber,
                               file.file_size, file.metadata),
        mutable_cf_options, &reader);
  });
  pin_started.get_future().wait();
  // With one shared set of stripes, this would wait for the open forever.
  blob_file_cache.Evict(blob_file_number);
  evict_done.set_value();
  opener.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(open_status);
}

TEST(BlobRelocationPinnedBlocksTest, KeptBlocksSurviveCachePressure) {
  // The kept blocks are charged to the block cache and are not evicted. After
  // other entries fill the cache (and evict each other), a lookup reads no
  // data block.
  std::shared_ptr<Cache> cache = NewLRUCache(1 << 20, /*num_shard_bits=*/0);
  const PinnedBlocksTestFile file("PinnedCachePressure", cache);
  std::unique_ptr<BlobFileReader> pinned;
  ASSERT_OK(file.Open(/*pin=*/true, &pinned));
  ASSERT_GT(pinned->relocation()->pinned_location_block_bytes(), 0U);
  EXPECT_GT(cache->GetPinnedUsage(), 0U);
  // 64 entries of 32 KB (2 MB) do not fit in the 1 MB cache.
  static const Cache::CacheItemHelper kFillerHelper{
      CacheEntryRole::kMisc, [](Cache::ObjectPtr, MemoryAllocator*) {}};
  static char filler_object;
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(cache->Insert("filler" + std::to_string(i), &filler_object,
                            &kFillerHelper, 32 << 10));
  }
  Cache::Handle* const first_filler = cache->Lookup("filler0");
  EXPECT_EQ(first_filler, nullptr);
  if (first_filler != nullptr) {
    cache->Release(first_filler);
  }
  SetPerfLevel(PerfLevel::kEnableCount);
  get_perf_context()->Reset();
  const std::vector<std::string> results = file.LookUpAll(*pinned);
  EXPECT_EQ(get_perf_context()->block_read_count, 0U);
  SetPerfLevel(PerfLevel::kDisable);
  EXPECT_EQ(results[0], "OK:" + file.payloads[0]);
}

TEST(BlobRelocationPinnedBlocksTest, FallsBackWhenCacheCannotCharge) {
  // A block cache with strict_capacity_limit that is too small to hold the
  // blocks: Open still succeeds, keeps nothing, and lookups work normally.
  LRUCacheOptions cache_options;
  cache_options.capacity = 16 << 10;
  cache_options.num_shard_bits = 0;
  cache_options.strict_capacity_limit = true;
  const PinnedBlocksTestFile file("PinnedStrictCapacity",
                                  NewLRUCache(cache_options));
  std::unique_ptr<BlobFileReader> pinned;
  ASSERT_OK(file.Open(/*pin=*/true, &pinned));
  EXPECT_EQ(pinned->relocation()->pinned_location_block_bytes(), 0U);
  const std::vector<std::string> results = file.LookUpAll(*pinned);
  EXPECT_EQ(results[0], "OK:" + file.payloads[0]);
  EXPECT_EQ(results[2 * (PinnedBlocksTestFile::kRecords - 1)],
            "OK:" + file.payloads.back());
}

TEST(BlobRelocationPinnedBlocksTest, CloseReleasesKeptBlocks) {
  // Closing the reader releases every cache handle it held, so the pinned
  // usage of the block cache returns to where it was before the open.
  std::shared_ptr<Cache> cache = NewLRUCache(8 << 20);
  const PinnedBlocksTestFile file("PinnedCloseReleases", cache);
  const size_t pinned_before = cache->GetPinnedUsage();
  std::unique_ptr<BlobFileReader> pinned;
  ASSERT_OK(file.Open(/*pin=*/true, &pinned));
  ASSERT_GT(pinned->relocation()->pinned_location_block_bytes(), 0U);
  EXPECT_GT(cache->GetPinnedUsage(), pinned_before);
  pinned.reset();
  EXPECT_EQ(cache->GetPinnedUsage(), pinned_before);
}

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
    const InternalKeyComparator comparator(user_comparator);
    std::unique_ptr<BlockBasedTableBuilder> builder;
    BlobRelocationTableFileInfo file_info;
    file_info.column_family_id = 1;
    file_info.column_family_name = kDefaultColumnFamilyName;
    file_info.db_id = db_id;
    file_info.db_session_id = db_session_id;
    file_info.file_number = file_number;
    ASSERT_TRUE(NewBlobRelocationTableBuilder(
                    immutable_options, mutable_cf_options, table_options,
                    ReadOptions(), WriteOptions(), comparator, file_info,
                    writer.get(), &builder)
                    .IsInvalidArgument());
    ASSERT_EQ(builder, nullptr);
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

TEST_F(BlobFileReaderTest, RelocationFileForcesContextChecksums) {
  // A relocation file built from a CF with format_version < 6 still uses
  // context checksums, so its record checksums depend on the record offset.
  BlockBasedTableOptions newer;
  newer.format_version = 7;
  ASSERT_EQ(MakeBlobRelocationTableOptions(newer).format_version, 7U);

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_RelocationContextChecksums"),
      0);
  options.enable_blob_files = true;
  BlockBasedTableOptions table_options;
  table_options.format_version = 5;
  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_EQ(MakeBlobRelocationTableOptions(table_options).format_version, 6U);

  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint64_t relocation_file_number = 301;
  const std::vector<RelocationTestRecord> records{{/*origin_file_number=*/100,
                                                   /*original_value_offset=*/10,
                                                   "value", kNoCompression}};
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  ASSERT_NO_FATAL_FAILURE(
      WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                          /*column_family_id=*/1, relocation_file_number,
                          records, &metadata, &file_size));

  const std::string path = BlobFileName(immutable_options.cf_paths.front().path,
                                        relocation_file_number);
  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(immutable_options.fs->NewRandomAccessFile(path, FileOptions(),
                                                      &file, nullptr));
  RandomAccessFileReader reader(std::move(file), path);
  Footer footer;
  ASSERT_OK(ReadFooterFromFile(IOOptions(), &reader, *immutable_options.fs,
                               /*prefetch_buffer=*/nullptr, file_size,
                               &footer));
  EXPECT_GE(footer.format_version(), 6U);
  EXPECT_NE(footer.base_context_checksum(), 0U);
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
  GrowableBuffer compressed_second_buffer;
  ASSERT_OK(LegacyForceBuiltinCompression(*compressor, /*working_area=*/nullptr,
                                          second_value,
                                          &compressed_second_buffer));
  const std::string compressed_second =
      compressed_second_buffer.AsSlice().ToString();
  // Values of one origin do not overlap: the second one starts after the end
  // of the first one, with a 40-byte gap like a record header and a key.
  const uint64_t second_offset = 10 + first_value.size() + 40;
  const std::vector<RelocationTestRecord> records{
      {first_origin, 10, first_value, kNoCompression},
      {first_origin, second_offset, compressed_second, kZSTD},
      {second_origin, 7, third_value, kNoCompression}};

  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      column_family_id, relocation_file_number, records,
                      &metadata, &file_size);
  const BlobRelocationDirectory directory = DecodeDirectory(metadata);
  ASSERT_EQ(directory.origins().size(), 2U);
  ASSERT_EQ(directory.total_entries(), records.size());
  ASSERT_EQ(directory.total_record_bytes(),
            first_value.size() + compressed_second.size() + third_value.size() +
                records.size() * kSimpleGen2BlobTrailerSize);
  const BlobPhysicalFile physical_file =
      RelocationPhysicalFile(relocation_file_number, file_size, metadata);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  read_options.verify_checksums = true;
  ASSERT_OK(OpenRelocationReader(
      immutable_options, read_options, FileOptions(), column_family_id,
      physical_file, /*db_session_id=*/"reader", table_options,
      mutable_cf_options.compression_manager.get(), &reader));
  ASSERT_NE(reader, nullptr);
  ASSERT_NE(reader->relocation(), nullptr);

  const auto open_invalid = [&](uint32_t cf_id, const BlobPhysicalFile& file,
                                const std::string& db_session_id) {
    std::unique_ptr<BlobFileReader> invalid_reader;
    return OpenRelocationReader(immutable_options, read_options, FileOptions(),
                                cf_id, file, db_session_id, table_options,
                                mutable_cf_options.compression_manager.get(),
                                &invalid_reader);
  };
  ASSERT_TRUE(
      open_invalid(column_family_id, physical_file, /*db_session_id=*/"")
          .IsInvalidArgument());
  BlobPhysicalFile wrong_digest = physical_file;
  wrong_digest.directory_digest ^= 1;
  ASSERT_TRUE(
      open_invalid(column_family_id, wrong_digest, "reader").IsCorruption());
  BlobPhysicalFile wrong_size = physical_file;
  ++wrong_size.file_size;
  ASSERT_TRUE(
      open_invalid(column_family_id, wrong_size, "reader").IsCorruption());
  ASSERT_TRUE(open_invalid(column_family_id + 1, physical_file, "reader")
                  .IsCorruption());

  // A directory that names a served file or an origin that is not older
  // than the file itself is rejected at open, even with valid checksums.
  const auto open_crafted = [&](const std::function<void(std::string*)>& edit) {
    std::string crafted = metadata.encoded_directory;
    edit(&crafted);
    constexpr size_t kDigestedHeaderOffset = 8;
    constexpr size_t kDirectoryDigestOffset = 40;
    constexpr size_t kHeaderChecksumOffset = 44;
    constexpr size_t kDirectoryHeaderSize = 48;
    const uint32_t header_crc =
        crc32c::Value(crafted.data() + kDigestedHeaderOffset,
                      kDirectoryDigestOffset - kDigestedHeaderOffset);
    EncodeFixed32(
        crafted.data() + kDirectoryDigestOffset,
        crc32c::Extend(header_crc, crafted.data() + kDirectoryHeaderSize,
                       crafted.size() - kDirectoryHeaderSize));
    EncodeFixed32(crafted.data() + kHeaderChecksumOffset,
                  crc32c::Value(crafted.data(), kHeaderChecksumOffset));
    BlobRelocationDirectory crafted_directory;
    Status status =
        BlobRelocationDirectory::DecodeFrom(crafted, &crafted_directory);
    if (!status.ok()) {
      return status;
    }
    SyncPoint::GetInstance()->SetCallBack(
        "BlobRelocationFileReader::Open:EncodedDirectory",
        [&](void* arg) { *static_cast<Slice*>(arg) = crafted; });
    SyncPoint::GetInstance()->EnableProcessing();
    BlobPhysicalFile crafted_file = physical_file;
    crafted_file.directory_digest = crafted_directory.directory_digest();
    status = open_invalid(column_family_id, crafted_file, "reader");
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    return status;
  };
  {
    // The last served file is the file itself.
    const Status status = open_crafted([&](std::string* crafted) {
      EncodeFixed64(crafted->data() + crafted->size() - sizeof(uint64_t),
                    relocation_file_number);
    });
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find("not older"), std::string::npos)
        << status.ToString();
  }
  {
    // The last origin is the file itself.
    const Status status = open_crafted([&](std::string* crafted) {
      constexpr size_t kOriginCountOffset = 8;
      constexpr size_t kDirectoryHeaderSize = 48;
      constexpr size_t kOriginEntrySize = 40;
      const uint64_t origin_count =
          DecodeFixed64(crafted->data() + kOriginCountOffset);
      EncodeFixed64(crafted->data() + kDirectoryHeaderSize +
                        (origin_count - 1) * kOriginEntrySize,
                    relocation_file_number);
    });
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find("not older"), std::string::npos)
        << status.ToString();
  }

  // The stored CRC always covers `stored_value`; `stored_size` may differ to
  // build invalid indexes.
  const auto make_index = [](uint64_t origin, uint64_t original_offset,
                             uint64_t stored_size, CompressionType compression,
                             const Slice& logical_value,
                             const Slice& stored_value) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, origin,
        {.origin_file_number = origin,
         .original_value_offset = original_offset,
         .stored_value_size = stored_size,
         .compression = compression,
         .stable_checksum =
             crc32c::Value(logical_value.data(), logical_value.size()),
         .stored_checksum =
             BlobIndex::ComputeStoredChecksum(stored_value, compression)});
    BlobIndex index;
    EXPECT_OK(index.DecodeFrom(encoded));
    return index;
  };
  const BlobIndex first_index =
      make_index(first_origin, 10, first_value.size(), kNoCompression,
                 first_value, first_value);
  const BlobIndex second_index =
      make_index(first_origin, second_offset, compressed_second.size(), kZSTD,
                 second_value, compressed_second);
  const BlobIndex third_index =
      make_index(second_origin, 7, third_value.size(), kNoCompression,
                 third_value, third_value);

  // The reader keeps only the directory totals and digest.
  EXPECT_EQ(reader->relocation()->total_entries(), records.size());
  EXPECT_EQ(reader->relocation()->total_record_bytes(),
            directory.total_record_bytes());
  EXPECT_EQ(reader->relocation()->directory_digest(),
            directory.directory_digest());
  // A value of an origin the file does not hold (below, between and above its
  // origins), or a missing offset of a held origin: the key lookup misses.
  for (const auto& missing : std::vector<std::pair<uint64_t, uint64_t>>{
           {50, 10}, {150, 10}, {250, 10}, {first_origin, 15}}) {
    SCOPED_TRACE(std::to_string(missing.first) + ":" +
                 std::to_string(missing.second));
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, relocation_file_number,
        {.origin_file_number = missing.first,
         .original_value_offset = missing.second,
         .stored_value_size = first_value.size(),
         .compression = kNoCompression,
         .stable_checksum =
             crc32c::Value(first_value.data(), first_value.size()),
         .stored_checksum =
             BlobIndex::ComputeStoredChecksum(first_value, kNoCompression)});
    BlobIndex missing_index;
    ASSERT_OK(missing_index.DecodeFrom(encoded));
    std::unique_ptr<BlobContents> missing_value;
    uint64_t missing_bytes = 0;
    const Status status = GetFromRelocationFile(
        *reader, read_options, missing_index, /*range_offset=*/0,
        kWholeBlobLength, /*allocator=*/nullptr, &missing_value,
        &missing_bytes);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find("entry is missing"), std::string::npos)
        << status.ToString();
  }

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;
  ASSERT_OK(GetFromRelocationFile(*reader, read_options, first_index,
                                  /*range_offset=*/0, kWholeBlobLength,
                                  /*allocator=*/nullptr, &value, &bytes_read));
  EXPECT_EQ(value->data(), first_value);
  EXPECT_EQ(bytes_read, first_value.size() + kSimpleGen2BlobTrailerSize);

  value.reset();
  ASSERT_OK(GetFromRelocationFile(*reader, read_options, first_index,
                                  /*range_offset=*/2, /*range_length=*/5,
                                  /*allocator=*/nullptr, &value, &bytes_read));
  EXPECT_EQ(value->data(), Slice(first_value.data() + 2, 5));
  EXPECT_EQ(bytes_read, 5U);

  // As in base RocksDB, a range of a compressed value is not supported: a
  // byte range cannot be decompressed alone. A whole read of the value works.
  value.reset();
  EXPECT_TRUE(GetFromRelocationFile(*reader, read_options, second_index,
                                    /*range_offset=*/3, /*range_length=*/7,
                                    /*allocator=*/nullptr, &value, &bytes_read)
                  .IsCorruption());
  value.reset();
  ASSERT_OK(GetFromRelocationFile(*reader, read_options, second_index,
                                  /*range_offset=*/0, kWholeBlobLength,
                                  /*allocator=*/nullptr, &value, &bytes_read));
  EXPECT_EQ(value->data(), second_value);
  EXPECT_EQ(bytes_read, compressed_second.size() + kSimpleGen2BlobTrailerSize);

  value.reset();
  EXPECT_TRUE(GetFromRelocationFile(
                  *reader, read_options, first_index,
                  /*range_offset=*/first_value.size(),
                  /*range_length=*/std::numeric_limits<size_t>::max() - 1,
                  /*allocator=*/nullptr, &value, &bytes_read)
                  .IsInvalidArgument());

  const BlobIndex unsupported_index =
      make_index(first_origin, 10, first_value.size(), kFirstCustomCompression,
                 first_value, first_value);
  EXPECT_TRUE(GetFromRelocationFile(*reader, read_options, unsupported_index,
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &value, &bytes_read)
                  .IsCorruption());
  const BlobIndex oversized_index =
      make_index(first_origin, 10, kMaxSimpleGen2BlobPayloadSize + 1,
                 kNoCompression, first_value, first_value);
  EXPECT_TRUE(GetFromRelocationFile(*reader, read_options, oversized_index,
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &value, &bytes_read)
                  .IsCorruption());

  std::array<SameFileBlobStandaloneRead, 3> invalid_outputs;
  std::array<Status, 3> invalid_statuses;
  std::array<RelocationBlobReadRequest, 3> invalid_requests{
      MakeRelocationRequest(&first_index, first_value.size(),
                            std::numeric_limits<size_t>::max() - 1,
                            &invalid_statuses[0], &invalid_outputs[0]),
      MakeRelocationRequest(&unsupported_index, 0, kWholeBlobLength,
                            &invalid_statuses[1], &invalid_outputs[1]),
      MakeRelocationRequest(&oversized_index, 0, kWholeBlobLength,
                            &invalid_statuses[2], &invalid_outputs[2])};
  bytes_read = std::numeric_limits<uint64_t>::max();
  reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                     invalid_requests.size(),
                                     invalid_requests.data(), &bytes_read);
  EXPECT_TRUE(invalid_statuses[0].IsInvalidArgument());
  EXPECT_TRUE(invalid_statuses[1].IsCorruption());
  EXPECT_TRUE(invalid_statuses[2].IsCorruption());
  EXPECT_EQ(bytes_read, 0U);

  std::array<SameFileBlobStandaloneRead, 3> outputs;
  std::array<Status, 3> statuses;
  std::array<RelocationBlobReadRequest, 3> requests;
  const std::array<const BlobIndex*, 3> indexes{&third_index, &first_index,
                                                &second_index};
  for (size_t i = 0; i < requests.size(); ++i) {
    requests[i].blob_index = indexes[i];
    requests[i].status = &statuses[i];
    requests[i].output = &outputs[i];
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
  reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                     requests.size(), requests.data(),
                                     &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  ASSERT_OK(statuses[2]);
  EXPECT_EQ(outputs[0].contents->data(), third_value);
  EXPECT_EQ(outputs[1].contents->data(), first_value);
  EXPECT_EQ(outputs[2].contents->data(), second_value);
  EXPECT_EQ(bytes_read, directory.total_record_bytes());
  EXPECT_EQ(relocation_multiread_calls.load(), 1U);
  EXPECT_EQ(relocation_multiread_requests.load(), requests.size());

  std::array<SameFileBlobStandaloneRead, 2> duplicate_outputs;
  std::array<Status, 2> duplicate_statuses;
  std::array<RelocationBlobReadRequest, 2> duplicate_requests{
      MakeRelocationRequest(&first_index, 0, kWholeBlobLength,
                            &duplicate_statuses[0], &duplicate_outputs[0]),
      MakeRelocationRequest(&first_index, 0, kWholeBlobLength,
                            &duplicate_statuses[1], &duplicate_outputs[1])};
  std::atomic<size_t> duplicate_multiread_calls{0};
  ExpectSortedNonOverlappingReads("MultiReadGen2:Requests",
                                  &duplicate_multiread_calls);
  SyncPoint::GetInstance()->EnableProcessing();
  reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                     duplicate_requests.size(),
                                     duplicate_requests.data(), &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(duplicate_statuses[0]);
  ASSERT_OK(duplicate_statuses[1]);
  EXPECT_EQ(duplicate_outputs[0].contents->data(), first_value);
  EXPECT_EQ(duplicate_outputs[1].contents->data(), first_value);
  // An exact duplicate is read once and shared, and counted once.
  EXPECT_EQ(duplicate_multiread_calls.load(), 1U);
  EXPECT_EQ(duplicate_outputs[0].gen2.copied + duplicate_outputs[1].gen2.copied,
            1);
  EXPECT_EQ(bytes_read, first_value.size() + kSimpleGen2BlobTrailerSize);

  std::array<SameFileBlobStandaloneRead, 2> overlapping_outputs;
  std::array<Status, 2> overlapping_statuses;
  std::array<RelocationBlobReadRequest, 2> overlapping_requests{
      MakeRelocationRequest(&first_index, 0, 5, &overlapping_statuses[0],
                            &overlapping_outputs[0]),
      MakeRelocationRequest(&first_index, 3, 5, &overlapping_statuses[1],
                            &overlapping_outputs[1])};
  std::atomic<size_t> overlapping_multiread_calls{0};
  ExpectSortedNonOverlappingReads("MultiReadGen2:Requests",
                                  &overlapping_multiread_calls);
  SyncPoint::GetInstance()->EnableProcessing();
  reader->relocation()->MultiGetBlob(read_options, /*allocator=*/nullptr,
                                     overlapping_requests.size(),
                                     overlapping_requests.data(), &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(overlapping_statuses[0]);
  ASSERT_OK(overlapping_statuses[1]);
  EXPECT_EQ(overlapping_outputs[0].contents->data(),
            Slice(first_value.data(), 5));
  EXPECT_EQ(overlapping_outputs[1].contents->data(),
            Slice(first_value.data() + 3, 5));
  EXPECT_EQ(overlapping_multiread_calls.load(), 2U);

  for (SameFileBlobStandaloneRead& overlapping_output : overlapping_outputs) {
    overlapping_output.contents.reset();
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
  reader->relocation()->MultiGetBlob(deadline_options, /*allocator=*/nullptr,
                                     overlapping_requests.size(),
                                     overlapping_requests.data(), &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(overlapping_statuses[0]);
  EXPECT_TRUE(overlapping_statuses[1].IsTimedOut());
  EXPECT_EQ(overlapping_outputs[0].contents->data(),
            Slice(first_value.data(), 5));
  EXPECT_EQ(overlapping_outputs[1].contents, nullptr);
  EXPECT_EQ(deadline_multiread_calls.load(), 1U);

  const BlobIndex missing =
      make_index(/*origin=*/999, /*original_offset=*/1, first_value.size(),
                 kNoCompression, first_value, first_value);
  value.reset();
  EXPECT_TRUE(GetFromRelocationFile(*reader, read_options, missing,
                                    /*range_offset=*/0, kWholeBlobLength,
                                    /*allocator=*/nullptr, &value,
                                    /*bytes_read=*/nullptr)
                  .IsCorruption());

  std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
  FileOptions file_options;
  const std::string db_id = "relocation-test-db";
  const std::string db_session_id = "relocation-test-session";
  BlobFileCache blob_file_cache(
      file_cache.get(), &immutable_options, &file_options, column_family_id,
      /*blob_file_read_hist=*/nullptr, /*io_tracer=*/nullptr, &table_options,
      db_session_id, /*block_cache_tracer=*/nullptr);
  BlobSource blob_source(immutable_options, mutable_cf_options, db_id,
                         db_session_id, &blob_file_cache);
  {
    // A duplicate in a BlobSource batch is read once and counted once.
    ReadOptions no_fill = read_options;
    no_fill.fill_cache = false;
    std::array<PinnableSlice, 2> dup_values;
    std::array<Status, 2> dup_statuses;
    std::array<IndirectBlobReadRequest, 2> dup_requests;
    for (size_t i = 0; i < dup_requests.size(); ++i) {
      dup_requests[i].user_key = "unused";
      dup_requests[i].blob_index = &first_index;
      dup_requests[i].result = &dup_values[i];
      dup_requests[i].status = &dup_statuses[i];
    }
    blob_source.MultiGetIndirectBlobFromOneFile(
        no_fill, physical_file, mutable_cf_options, dup_requests.size(),
        dup_requests.data(), &bytes_read);
    ASSERT_OK(dup_statuses[0]);
    ASSERT_OK(dup_statuses[1]);
    EXPECT_EQ(dup_values[0], first_value);
    EXPECT_EQ(dup_values[1], first_value);
    EXPECT_EQ(bytes_read, first_value.size() + kSimpleGen2BlobTrailerSize);
  }
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
      read_options, physical_file, mutable_cf_options, source_requests.size(),
      source_requests.data(), &bytes_read);
  ASSERT_OK(source_statuses[0]);
  ASSERT_OK(source_statuses[1]);
  ASSERT_OK(source_statuses[2]);
  EXPECT_EQ(source_values[0], second_value);
  EXPECT_EQ(source_values[1], third_value);
  EXPECT_EQ(source_values[2], first_value);
  EXPECT_EQ(bytes_read, directory.total_record_bytes());

  {
    // A cache without a session id cannot open relocation files.
    BlobFileCache no_session_cache(
        file_cache.get(), &immutable_options, &file_options, column_family_id,
        /*blob_file_read_hist=*/nullptr, /*io_tracer=*/nullptr, &table_options,
        /*db_session_id=*/"", /*block_cache_tracer=*/nullptr);
    // BlobFileCache::Evict() is for normal blob files only.
    TableCache::Evict(file_cache.get(), relocation_file_number);
    CacheHandleGuard<BlobFileReader> invalid_cached_reader;
    EXPECT_TRUE(no_session_cache
                    .GetRelocationFileReader(read_options, physical_file,
                                             mutable_cf_options,
                                             &invalid_cached_reader)
                    .IsInvalidArgument());
  }

  for (PinnableSlice& source_value : source_values) {
    source_value.Reset();
  }
  read_options.read_tier = kBlockCacheTier;
  BlobPhysicalFile unopened_file = physical_file;
  unopened_file.open_info.file_number = 999;
  unopened_file.directory_digest ^= 1;
  blob_source.MultiGetIndirectBlobFromOneFile(
      read_options, unopened_file, mutable_cf_options, source_requests.size(),
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

namespace {

// Writes blob file `blob_file_number` with one blob and a version 2 header,
// which is the header that blob indirection writes. The header names
// `header_file_number`. A test passes another number than `blob_file_number`
// to make a header that does not match the file name.
void WriteVersion2BlobFile(const ImmutableOptions& immutable_options,
                           uint32_t column_family_id, uint64_t blob_file_number,
                           uint64_t header_file_number, const Slice& key,
                           const Slice& blob, uint64_t* blob_offset) {
  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                            FileOptions()));
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, FileOptions(), immutable_options.clock));
  BlobLogWriter blob_log_writer(std::move(file_writer), immutable_options.clock,
                                /*statistics=*/nullptr, blob_file_number,
                                /*use_fsync=*/false, /*do_flush=*/false);

  BlobLogHeader header(column_family_id, kNoCompression, /*_has_ttl=*/false,
                       ExpirationRange());
  header.version = kVersion2;
  header.file_number = header_file_number;
  ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));

  uint64_t key_offset = 0;
  ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), key, blob, &key_offset,
                                      blob_offset));

  // The footer of a version 2 file is the same as in version 1.
  BlobLogFooter footer;
  footer.blob_count = 1;
  std::string checksum_method;
  std::string checksum_value;
  ASSERT_OK(blob_log_writer.AppendFooter(WriteOptions(), footer,
                                         &checksum_method, &checksum_value));
}

}  // anonymous namespace

TEST(BlobLogHeaderTest, Version2HoldsFileNumber) {
  // A version 2 header stores the file's own number in the first
  // expiration-range field and 0 in the second one. Its flags byte is 0.
  BlobLogHeader header(/*_column_family_id=*/7, kNoCompression,
                       /*_has_ttl=*/false, ExpirationRange());
  header.version = kVersion2;
  header.file_number = 42;
  std::string encoded;
  header.EncodeTo(&encoded);
  ASSERT_EQ(encoded.size(), BlobLogHeader::kSize);

  BlobLogHeader decoded;
  ASSERT_OK(decoded.DecodeFrom(encoded));
  EXPECT_EQ(decoded.version, kVersion2);
  EXPECT_EQ(decoded.column_family_id, 7U);
  EXPECT_EQ(decoded.file_number, 42U);
  EXPECT_FALSE(decoded.has_ttl);
  EXPECT_EQ(decoded.expiration_range, ExpirationRange());

  // Byte offsets in the 30-byte header: version at 4, flags at 12, and the two
  // expiration-range fields at 14 and 22.
  const auto changed = [](const std::string& src, size_t offset,
                          const std::string& bytes) {
    std::string copy = src;
    copy.replace(offset, bytes.size(), bytes);
    return copy;
  };
  std::string version3;
  PutFixed32(&version3, 3);
  std::string zero64;
  PutFixed64(&zero64, 0);
  std::string one64;
  PutFixed64(&one64, 1);
  struct BadHeader {
    const char* name;
    std::string bytes;
    const char* message;
  };
  const std::vector<BadHeader> bad_v2{
      {"unknown version 3", changed(encoded, 4, version3),
       "Unknown header version"},
      {"file number 0", changed(encoded, 14, zero64),
       "Invalid blob file number"},
      {"nonzero second field", changed(encoded, 22, one64),
       "Invalid blob file number"},
      {"has_ttl in version 2", changed(encoded, 12, std::string(1, '\x01')),
       "Invalid blob file flags"},
      {"flag bit 2 in version 2", changed(encoded, 12, std::string(1, '\x04')),
       "Invalid blob file flags"},
  };
  for (const auto& bad : bad_v2) {
    SCOPED_TRACE(bad.name);
    BlobLogHeader rejected;
    const Status s = rejected.DecodeFrom(bad.bytes);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    EXPECT_NE(s.ToString().find(bad.message), std::string::npos)
        << s.ToString();
  }

  // Version 1 has no file number.
  BlobLogHeader v1(/*_column_family_id=*/7, kNoCompression, /*_has_ttl=*/false,
                   ExpirationRange());
  std::string encoded_v1;
  v1.EncodeTo(&encoded_v1);
  BlobLogHeader decoded_v1;
  ASSERT_OK(decoded_v1.DecodeFrom(encoded_v1));
  EXPECT_EQ(decoded_v1.version, kVersion1);
  EXPECT_EQ(decoded_v1.file_number, kInvalidBlobFileNumber);
}

TEST_F(BlobFileReaderTest, OpensVersion2File) {
  // A version 2 file whose header names the file's own number opens, and its
  // blob reads back.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_OpensVersion2File"),
      0);
  options.enable_blob_files = true;
  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 5;
  const std::string key = "key";
  const std::string blob = "blob";
  uint64_t blob_offset = 0;
  WriteVersion2BlobFile(immutable_options, column_family_id, blob_file_number,
                        /*header_file_number=*/blob_file_number, key, blob,
                        &blob_offset);

  std::unique_ptr<BlobFileReader> reader;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, ReadOptions(), FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number,
      /*io_tracer=*/nullptr, &reader));
  ASSERT_NE(reader, nullptr);

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;
  ASSERT_OK(reader->GetBlob(ReadOptions(), key, blob_offset, blob.size(),
                            kNoCompression, /*prefetch_buffer=*/nullptr,
                            /*allocator=*/nullptr, &value, &bytes_read));
  ASSERT_NE(value, nullptr);
  EXPECT_EQ(value->data(), blob);
}

TEST_F(BlobFileReaderTest, Version2FileNumberMismatch) {
  // A version 2 header names the file's own number. When it names another
  // number (for example, the file was copied or renamed), the open fails.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_Version2FileNumberMismatch"),
      0);
  options.enable_blob_files = true;
  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 5;
  uint64_t blob_offset = 0;
  WriteVersion2BlobFile(immutable_options, column_family_id, blob_file_number,
                        /*header_file_number=*/blob_file_number + 1, "key",
                        "blob", &blob_offset);

  std::unique_ptr<BlobFileReader> reader;
  const Status s = BlobFileReader::Create(
      immutable_options, ReadOptions(), FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number,
      /*io_tracer=*/nullptr, &reader);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  EXPECT_NE(s.ToString().find("Blob file number mismatch"), std::string::npos)
      << s.ToString();
}

TEST_F(BlobFileReaderTest, RelocationFileOfAnotherColumnFamilyIsRejected) {
  // A relocation file records the ID of the column family that wrote it in its
  // table properties. The reader compares it with the ID of the column family
  // that opens the file, so a wrong MANIFEST record cannot make one column
  // family read another column family's relocation file.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "RelocationFileOfAnotherColumnFamily"),
      0);
  options.enable_blob_files = true;
  BlockBasedTableOptions table_options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const MutableCFOptions mutable_cf_options(options);
  constexpr uint32_t column_family_id = 1;
  constexpr uint32_t other_column_family_id = 2;
  constexpr uint64_t relocation_file_number = 400;
  const std::string payload = "relocated-value";
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      column_family_id, relocation_file_number,
                      {{/*origin_file_number=*/100,
                        /*original_value_offset=*/10, payload, kNoCompression}},
                      &metadata, &file_size);
  const BlobPhysicalFile file =
      RelocationPhysicalFile(relocation_file_number, file_size, metadata);

  // The column family that wrote the file opens it.
  {
    std::unique_ptr<BlobFileReader> reader;
    ASSERT_OK(OpenRelocationReader(
        immutable_options, ReadOptions(), FileOptions(), column_family_id, file,
        /*db_session_id=*/"reader", table_options,
        mutable_cf_options.compression_manager.get(), &reader));
  }
  // Another column family gets Corruption.
  std::unique_ptr<BlobFileReader> reader;
  const Status status = OpenRelocationReader(
      immutable_options, ReadOptions(), FileOptions(), other_column_family_id,
      file, /*db_session_id=*/"reader", table_options,
      mutable_cf_options.compression_manager.get(), &reader);
  ASSERT_EQ(status.code(), Status::kCorruption) << status.ToString();
  ASSERT_NE(status.ToString().find("Blob relocation column family mismatch"),
            std::string::npos)
      << status.ToString();
  ASSERT_EQ(reader, nullptr);
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
      "UncompressBlobContentsIfNeeded:TamperWithResult", [](void* arg) {
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

TEST(BlobRelocationTracerTest, RelocationReadsReachBlockCacheTrace) {
  // BlobFileCache passes the DB's block cache tracer to the relocation file
  // readers it opens. So, while a block cache trace runs, the block reads of a
  // relocation file are in the trace with the caller kBlobRelocationFileRead:
  // cache misses, cache hits, and each block of a batched read of several
  // missing blocks. Without a tracer (nullptr), the same read works and
  // nothing is traced.
  //
  // For example, relocation file 700 holds three values of origin blob file
  // 100.
  constexpr uint32_t kColumnFamilyId = 1;
  constexpr uint64_t kFileNumber = 700;
  constexpr uint64_t kOrigin = 100;
  Env* const env = Env::Default();
  const std::string path =
      test::PerThreadDBPath(env, "BlobRelocationTracerTest");
  ASSERT_OK(DestroyDir(env, path));
  ASSERT_OK(env->CreateDirIfMissing(path));
  Options options;
  options.env = env;
  options.cf_paths.emplace_back(path, 0);
  options.enable_blob_files = true;
  BlockBasedTableOptions table_options;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(options);
  const FileOptions file_options{DBOptions(options)};
  const MutableCFOptions mutable_cf_options(options);
  const std::vector<std::string> payloads{"value-a", "value-b", "value-c"};
  std::vector<RelocationTestRecord> records;
  for (size_t i = 0; i < payloads.size(); ++i) {
    records.push_back(
        {kOrigin, 10 + i * 100, Slice(payloads[i]), kNoCompression});
  }
  BlobRelocationFileMetadata metadata;
  uint64_t file_size = 0;
  WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                      kColumnFamilyId, kFileNumber, records, &metadata,
                      &file_size);
  const BlobPhysicalFile physical_file =
      RelocationPhysicalFile(kFileNumber, file_size, metadata);
  std::string encoded;
  BlobIndex::EncodeIndirectBlob(
      &encoded, kOrigin,
      {.origin_file_number = kOrigin,
       .original_value_offset = records[1].original_value_offset,
       .stored_value_size = payloads[1].size(),
       .compression = kNoCompression,
       .stable_checksum = crc32c::Value(payloads[1].data(), payloads[1].size()),
       .stored_checksum =
           BlobIndex::ComputeStoredChecksum(payloads[1], kNoCompression)});
  BlobIndex blob_index;
  ASSERT_OK(blob_index.DecodeFrom(encoded));
  // Opens `file` through a new BlobFileCache with `tracer`, then reads the
  // second value `reads` times. Each call gets a new, empty block cache, so
  // the first read misses in the cache and later reads hit. The table reader
  // traces only block cache lookups, and the index block is kept in the block
  // cache so that its lookup is traced too. With `pin`, the open reads and
  // keeps all location blocks of the file
  // (pin_blob_relocation_location_blocks).
  const auto read = [&](BlockCacheTracer* tracer, const BlobPhysicalFile& file,
                        int reads, bool pin) {
    BlockBasedTableOptions read_table_options = table_options;
    read_table_options.block_cache = NewLRUCache(8 << 20);
    read_table_options.cache_index_and_filter_blocks = true;
    MutableCFOptions read_cf_options = mutable_cf_options;
    read_cf_options.pin_blob_relocation_location_blocks = pin;
    // The readers get their own cache. A reader holds the table options,
    // which hold the block cache, so a reader in the block cache would keep
    // that cache alive forever.
    std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
    BlobFileCache blob_file_cache(
        file_cache.get(), &immutable_options, &file_options, kColumnFamilyId,
        /*blob_file_read_hist=*/nullptr, /*io_tracer=*/nullptr,
        &read_table_options, /*db_session_id=*/"session", tracer);
    CacheHandleGuard<BlobFileReader> reader;
    ASSERT_OK(blob_file_cache.GetRelocationFileReader(
        ReadOptions(), file, read_cf_options, &reader));
    for (int i = 0; i < reads; ++i) {
      std::unique_ptr<BlobContents> value;
      ASSERT_OK(GetFromRelocationFile(*reader.GetValue(), ReadOptions(),
                                      blob_index, /*range_offset=*/0,
                                      kWholeBlobLength, /*allocator=*/nullptr,
                                      &value, /*bytes_read=*/nullptr));
      ASSERT_EQ(value->data(), payloads[1]);
    }
  };
  // Runs `fn` with a tracer that traces, and returns the trace records with
  // the caller kBlobRelocationFileRead.
  const auto traced = [&](const std::function<void(BlockCacheTracer*)>& fn) {
    const std::string trace_path = path + "/block_cache_trace";
    std::vector<BlockCacheTraceRecord> result;
    {
      std::unique_ptr<TraceWriter> trace_writer;
      EXPECT_OK(
          NewFileTraceWriter(env, EnvOptions(), trace_path, &trace_writer));
      BlockCacheTracer tracer;
      EXPECT_OK(tracer.StartTrace(
          BlockCacheTraceOptions(),
          NewBlockCacheTraceWriter(env->GetSystemClock().get(),
                                   BlockCacheTraceWriterOptions(),
                                   std::move(trace_writer))));
      fn(&tracer);
      tracer.EndTrace();
    }
    std::unique_ptr<TraceReader> trace_reader;
    EXPECT_OK(NewFileTraceReader(env, EnvOptions(), trace_path, &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    EXPECT_OK(reader.ReadHeader(&header));
    BlockCacheTraceRecord record;
    while (reader.ReadAccess(&record).ok()) {
      if (record.caller == TableReaderCaller::kBlobRelocationFileRead) {
        result.push_back(record);
      }
    }
    EXPECT_OK(env->DeleteFile(trace_path));
    return result;
  };
  const auto is_data_block = [](const BlockCacheTraceRecord& record) {
    return record.block_type == TraceType::kBlockTraceDataBlock;
  };

  // No tracer: the read works.
  read(/*tracer=*/nullptr, physical_file, /*reads=*/1, /*pin=*/false);

  // One cold read: the trace gets kBlobRelocationFileRead records.
  {
    const std::vector<BlockCacheTraceRecord> records_read =
        traced([&](BlockCacheTracer* tracer) {
          read(tracer, physical_file, /*reads=*/1, /*pin=*/false);
        });
    ASSERT_GT(records_read.size(), 0U);
  }

  // A second read of the same value finds the data block in the block cache.
  // The cache hit is traced too.
  {
    const std::vector<BlockCacheTraceRecord> records_read =
        traced([&](BlockCacheTracer* tracer) {
          read(tracer, physical_file, /*reads=*/2, /*pin=*/false);
        });
    ASSERT_TRUE(std::any_of(records_read.begin(), records_read.end(),
                            [&](const BlockCacheTraceRecord& record) {
                              return is_data_block(record) &&
                                     record.is_cache_hit;
                            }));
  }

  // Several cold data blocks in one call: an open with pinned location blocks
  // reads every data block of the file in batches. Each data block gets one
  // trace record, a cache miss. For example, relocation file 701 holds 5000
  // values of origin 100, so its location entries fill more than one 4 KB
  // data block.
  {
    constexpr uint64_t kLargeFileNumber = 701;
    std::vector<std::string> large_payloads;
    std::vector<RelocationTestRecord> large_records;
    for (int i = 0; i < 5000; ++i) {
      large_payloads.push_back("value-" + std::to_string(1000000 + i));
    }
    for (int i = 0; i < 5000; ++i) {
      large_records.push_back({kOrigin, static_cast<uint64_t>(10 + i * 100),
                               Slice(large_payloads[i]), kNoCompression});
    }
    BlobRelocationFileMetadata large_metadata;
    uint64_t large_file_size = 0;
    WriteRelocationFile(immutable_options, mutable_cf_options, table_options,
                        kColumnFamilyId, kLargeFileNumber, large_records,
                        &large_metadata, &large_file_size);
    std::unique_ptr<RandomAccessFileReader> file_reader;
    ASSERT_OK(RandomAccessFileReader::Create(
        immutable_options.fs, BlobFileName(path, kLargeFileNumber),
        FileOptions(), &file_reader, /*dbg=*/nullptr));
    std::unique_ptr<TableProperties> properties;
    ASSERT_OK(ReadTableProperties(
        file_reader.get(), large_file_size, kBlockBasedTableMagicNumber,
        immutable_options, ReadOptions(), &properties));
    ASSERT_GE(properties->num_data_blocks, 2U);
    // The open alone reads all data blocks, so this case reads no value.
    const BlobPhysicalFile large_file = RelocationPhysicalFile(
        kLargeFileNumber, large_file_size, large_metadata);
    const std::vector<BlockCacheTraceRecord> records_read =
        traced([&](BlockCacheTracer* tracer) {
          read(tracer, large_file, /*reads=*/0, /*pin=*/true);
        });
    std::set<std::string> data_blocks;
    size_t data_block_records = 0;
    for (const BlockCacheTraceRecord& record : records_read) {
      if (is_data_block(record)) {
        EXPECT_FALSE(record.is_cache_hit);
        data_blocks.insert(record.block_key);
        ++data_block_records;
      }
    }
    ASSERT_EQ(data_block_records, properties->num_data_blocks);
    ASSERT_EQ(data_blocks.size(), properties->num_data_blocks);
  }
  ASSERT_OK(DestroyDir(env, path));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
