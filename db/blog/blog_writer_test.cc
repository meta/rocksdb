//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_writer.h"

#include <algorithm>
#include <string>
#include <vector>

#include "db/blog/blog_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/prefix_varint.h"

namespace ROCKSDB_NAMESPACE {

class BlogWriterTest : public testing::Test {
 protected:
  // Backing storage for the in-memory file. It must outlive the sink so tests
  // can still read the bytes after writer->Close() destroys the sink object.
  std::string contents_;

  // Create a writer with the given header.
  std::unique_ptr<BlogFileWriter> MakeWriter(const BlogFileHeader& header) {
    contents_.clear();
    std::unique_ptr<FSWritableFile> sink_holder(
        new test::StringFS::StringSink(&contents_));
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions()));
    return std::make_unique<BlogFileWriter>(std::move(file_writer), header);
  }

  std::unique_ptr<BlogFileReader> MakeReaderWithReporter(
      BlogFileReader::Reporter* r) {
    return MakeReaderImpl(r);
  }

  // Create a reader over the data written so far.
  std::unique_ptr<BlogFileReader> MakeReader(bool verify_checksums = true) {
    return MakeReaderImpl(nullptr, verify_checksums);
  }

  std::unique_ptr<BlogFileReader> MakeReaderImpl(
      BlogFileReader::Reporter* reporter, bool verify_checksums = true) {
    // Create an FSSequentialFile that reads from contents_
    class MemSequentialFile : public FSSequentialFile {
     public:
      const std::string& contents_;
      size_t pos_ = 0;
      explicit MemSequentialFile(const std::string& contents)
          : contents_(contents) {}
      IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                    char* scratch, IODebugContext* /*dbg*/) override {
        size_t avail = contents_.size() - pos_;
        if (n > avail) {
          n = avail;
        }
        if (n > 0) {
          memcpy(scratch, contents_.data() + pos_, n);
          pos_ += n;
        }
        *result = Slice(scratch, n);
        return IOStatus::OK();
      }
      IOStatus Skip(uint64_t n) override {
        pos_ += static_cast<size_t>(n);
        return IOStatus::OK();
      }
    };

    auto* source = new MemSequentialFile(contents_);
    std::unique_ptr<FSSequentialFile> source_holder(source);
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source_holder), "" /* file name */));
    return std::make_unique<BlogFileReader>(std::move(file_reader), reporter,
                                            verify_checksums);
  }

  // Helper to create a default header for blob files.
  BlogFileHeader MakeBlobHeader() {
    BlogFileHeader header;
    header.checksum_type = kXXH3;
    header.compact_record_type = kBlogBlobRecord;
    header.GenerateRandomFields();
    return header;
  }

  // Helper to create a default header for WAL files.
  BlogFileHeader MakeWalHeader() {
    BlogFileHeader header;
    header.checksum_type = kXXH3;
    header.compact_record_type = kBlogWriteBatchRecord;
    header.GenerateRandomFields();
    header.SetProperty("role", "wal");
    return header;
  }

  size_t CountEscapeSequences(const BlogFileHeader& header) const {
    size_t count = 0;
    for (size_t i = 0; i + kBlogEscapeSequenceSize <= contents_.size(); ++i) {
      if (memcmp(contents_.data() + i, header.escape_sequence,
                 kBlogEscapeSequenceSize) == 0) {
        ++count;
      }
    }
    return count;
  }

  const std::string* FindFooterProperty(
      const BlogFileFooterProperties& properties,
      const std::string& property_name) const {
    for (const auto& [name, value] : properties.properties) {
      if (name == property_name) {
        return &value;
      }
    }
    return nullptr;
  }

  size_t RecordVarintLength(uint64_t record_offset) const {
    return PrefixVarint64AddlByteCount(
               contents_[record_offset + kBlogEscapeSequenceSize]) +
           1;
  }

  void RewriteFullPrefixChecksum(const BlogFileHeader& header,
                                 uint64_t record_offset) {
    const size_t varint_len = RecordVarintLength(record_offset);
    const size_t varint_offset = record_offset + kBlogEscapeSequenceSize;
    const size_t checksum_offset = varint_offset + varint_len + 2;
    const uint32_t checksum =
        ComputeBuiltinChecksum(header.checksum_type,
                               contents_.data() + varint_offset,
                               varint_len + 2) +
        ChecksumModifierForContext(header.incarnation_id(), record_offset);
    EncodeFixed32(contents_.data() + checksum_offset, checksum);
  }

  void RewriteRecordTrailerChecksum(const BlogFileHeader& header,
                                    uint64_t payload_offset,
                                    size_t payload_size) {
    const size_t trailer_offset = payload_offset + payload_size;
    const uint32_t checksum = ComputeBlogRecordChecksum(
        header.checksum_type, contents_.data() + payload_offset, payload_size,
        contents_[trailer_offset], header.incarnation_id(), payload_offset);
    EncodeFixed32(contents_.data() + trailer_offset + 1, checksum);
  }

  void ExpectHeaderInvalid(const BlogFileHeader& header) {
    std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
    const IOStatus s = writer->WriteHeader(WriteOptions());
    ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
    ASSERT_TRUE(contents_.empty());
  }

  // Construct malformed input independently of the writer's public API.
  void AppendTestRecord(const BlogFileHeader& header, BlogRecordType type,
                        uint64_t declared_length, CompressionType prefix_comp,
                        CompressionType trailer_comp, const Slice& payload) {
    const uint64_t record_offset = contents_.size();
    ASSERT_EQ(record_offset % kBlogMinRecordAlignment, 0u);
    contents_.append(header.escape_sequence, kBlogEscapeSequenceSize);
    std::string prefix;
    if (declared_length == 0) {
      PutPrefixVarint64(&prefix, 0);
      prefix.push_back(lossless_cast<char>(type));
    } else {
      char varint[kMaxPrefixVarint64Length];
      const char* end = EncodePrefixVarint64<3>(varint, declared_length);
      prefix.append(varint, lossless_cast<size_t>(end - varint));
      prefix.push_back(lossless_cast<char>(type));
      prefix.push_back(lossless_cast<char>(prefix_comp));
    }
    contents_.append(prefix);
    PutFixed32(
        &contents_,
        ComputeBuiltinChecksum(header.checksum_type, prefix.data(),
                               prefix.size()) +
            ChecksumModifierForContext(header.incarnation_id(), record_offset));
    if (declared_length != 0) {
      const uint64_t payload_offset = contents_.size();
      contents_.append(payload.data(), payload.size());
      contents_.push_back(lossless_cast<char>(trailer_comp));
      PutFixed32(&contents_,
                 ComputeBlogRecordChecksum(
                     header.checksum_type, payload.data(), payload.size(),
                     lossless_cast<char>(trailer_comp), header.incarnation_id(),
                     payload_offset));
    }
    if (type != kBlogFooterLocatorRecord) {
      BlogPadding padding(lossless_cast<uint8_t>(contents_.back()),
                          contents_.size());
      padding.AppendTo(&contents_);
    }
  }

  void ExpectRecordCorruption(bool verify_checksums, const char* message) {
    std::unique_ptr<BlogFileReader> reader = MakeReader(verify_checksums);
    BlogFileHeader read_header;
    ASSERT_OK(reader->ReadHeader(&read_header));
    BlogRecordType type;
    Slice payload;
    std::string scratch;
    const Status s = reader->ReadRecord(&type, &payload, &scratch);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_NE(s.ToString().find(message), std::string::npos) << s.ToString();
  }
};

TEST_F(BlogWriterTest, WriteAndReadHeader) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));

  ASSERT_EQ(read_header.schema_version, header.schema_version);
  ASSERT_EQ(read_header.checksum_type, header.checksum_type);
  ASSERT_EQ(read_header.compact_record_type, header.compact_record_type);
  ASSERT_EQ(memcmp(read_header.escape_sequence, header.escape_sequence,
                   kBlogEscapeSequenceSize),
            0);
}

TEST_F(BlogWriterTest, RejectsUnsupportedChecksumTypeBeforeWriting) {
  BlogFileHeader header = MakeBlobHeader();
  header.checksum_type = lossless_cast<ChecksumType>(uint8_t{0x7f});
  ExpectHeaderInvalid(header);
}

TEST_F(BlogWriterTest, AcceptsEverySupportedChecksumType) {
  for (uint8_t raw = lossless_cast<uint8_t>(kNoChecksum);
       raw <= lossless_cast<uint8_t>(kXXH3); ++raw) {
    BlogFileHeader header = MakeBlobHeader();
    header.checksum_type = lossless_cast<ChecksumType>(raw);
    std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));

    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader read_header;
    ASSERT_OK(reader->ReadHeader(&read_header));
    ASSERT_EQ(read_header.checksum_type, header.checksum_type);
  }
}

TEST_F(BlogWriterTest, RejectsEmptyBlobBeforeWriting) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  const uint64_t offset = writer->current_offset();
  uint64_t blob_offset = 123;
  const IOStatus s = writer->AddBlobRecord(WriteOptions(), Slice(),
                                           kNoCompression, &blob_offset, 0);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ(writer->current_offset(), offset);
  ASSERT_EQ(blob_offset, 123);
  ASSERT_EQ(writer->blob_stats().count, 0);
}

TEST_F(BlogWriterTest, FooterLocatorReferencesPropertiesRecord) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  BlogFileFooterProperties props;
  props.SetSequenceRange(5, 7);
  ASSERT_OK(writer->WriteFooter(WriteOptions(), props));

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  uint64_t properties_offset = 0;
  uint64_t locator_offset = 0;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, &properties_offset));
  ASSERT_EQ(type, kBlogFooterPropertiesRecord);
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, &locator_offset));
  ASSERT_EQ(type, kBlogFooterLocatorRecord);

  BlogFileFooterLocator locator;
  ASSERT_OK(locator.DecodeFrom(payload));
  ASSERT_EQ(locator.entries.size(), 1u);
  ASSERT_EQ(locator.entries[0].record_type, kBlogFooterPropertiesRecord);
  ASSERT_EQ(locator_offset - uint64_t{locator.entries[0].relative_offset_4B} *
                                 kBlogMinRecordAlignment,
            properties_offset);
}

TEST_F(BlogWriterTest, AddsLiveWriterStatsToFooterProperties) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  const std::string blob(50, 'B');
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), blob, kNoCompression, nullptr,
                                  blob.size()));

  BlogFileFooterProperties props;
  props.SetSequenceRange(5, 7);
  ASSERT_OK(writer->WriteFooter(WriteOptions(), props));

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogFooterPropertiesRecord);

  BlogFileFooterProperties read_props;
  ASSERT_OK(read_props.DecodeFrom(payload));
  ASSERT_EQ(read_props.properties.size(), 5u);
  const BlogFileWriter::BlobStats& stats = writer->blob_stats();
  const std::pair<std::string, uint64_t> expected[] = {
      {"blobCount", stats.count},
      {"blobPayloadBytes", stats.payload_bytes},
      {"blobOverheadBytes", stats.overhead_bytes},
      {"minSequence", 5},
      {"maxSequence", 7},
  };
  for (const auto& [expected_name, expected_value] : expected) {
    const std::string* encoded_value =
        FindFooterProperty(read_props, expected_name);
    ASSERT_NE(encoded_value, nullptr) << expected_name;
    Slice input(*encoded_value);
    uint64_t value = 0;
    ASSERT_TRUE(GetPrefixVarint64(&input, &value));
    ASSERT_TRUE(input.empty());
    ASSERT_EQ(value, expected_value);
  }
}

TEST_F(BlogWriterTest, AddsLiveCompressedStatsToFooterProperties) {
  BlogFileHeader header = MakeBlobHeader();
  header.SetProperty(kBlogPropCompressionCompatibilityName, "BuiltinV2");
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  const std::string compressed_blob = "compressed";
  constexpr uint64_t kUncompressedSize = 100;
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), compressed_blob, kZSTD,
                                  nullptr, kUncompressedSize));
  ASSERT_OK(writer->WriteFooter(WriteOptions(), BlogFileFooterProperties{}));

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogFooterPropertiesRecord);

  BlogFileFooterProperties read_props;
  ASSERT_OK(read_props.DecodeFrom(payload));
  ASSERT_EQ(read_props.properties.size(), 6u);
  const BlogFileWriter::BlobStats& stats = writer->blob_stats();
  const std::pair<std::string, uint64_t> expected[] = {
      {"blobCount", stats.count},
      {"blobPayloadBytes", stats.payload_bytes},
      {"blobCompressedBytes", stats.compressed_bytes},
      {"blobUncompressedBytes", kUncompressedSize},
      {"blobOverheadBytes", stats.overhead_bytes},
  };
  for (const auto& [expected_name, expected_value] : expected) {
    const std::string* encoded_value =
        FindFooterProperty(read_props, expected_name);
    ASSERT_NE(encoded_value, nullptr) << expected_name;
    Slice input(*encoded_value);
    uint64_t value = 0;
    ASSERT_TRUE(GetPrefixVarint64(&input, &value));
    ASSERT_TRUE(input.empty());
    ASSERT_EQ(value, expected_value);
  }
  const std::string* compression_types =
      FindFooterProperty(read_props, "compressionTypes");
  ASSERT_NE(compression_types, nullptr);
  ASSERT_EQ(*compression_types, std::string(1, char{kZSTD}));
}

TEST_F(BlogWriterTest, WriteSingleBlobRecord) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string blob_data(100, 'A');
  uint64_t blob_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset,
                                  blob_data.size()));
  ASSERT_GT(blob_offset, 0u);

  // Verify alignment: offset after record should be 4-byte aligned
  ASSERT_EQ(writer->current_offset() % 4, 0u);

  // Read it back
  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  uint64_t rec_offset;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, &rec_offset));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), 100u);
  ASSERT_EQ(payload.ToString(), blob_data);
}

TEST_F(BlogWriterTest, WriteMultipleBlobRecords) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::vector<std::string> blobs = {"hello", std::string(1000, 'X'), "short",
                                    std::string(500, 'Y')};
  for (const auto& blob : blobs) {
    uint64_t offset;
    ASSERT_OK(
        writer->AddBlobRecord(wo, blob, kNoCompression, &offset, blob.size()));
  }

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  for (const auto& expected : blobs) {
    BlogRecordType type;
    Slice payload;
    std::string scratch;
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    ASSERT_EQ(type, kBlogBlobRecord);
    ASSERT_EQ(payload.ToString(), expected);
  }

  // No more records
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_TRUE(reader->ReadRecord(&type, &payload, &scratch).IsNotFound());
}

TEST_F(BlogWriterTest, WriteWriteBatchRecord) {
  auto header = MakeWalHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string wb_data(200, 'W');
  ASSERT_OK(writer->AddWriteBatchRecord(wo, wb_data));

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(payload.ToString(), wb_data);
}

TEST_F(BlogWriterTest, CompactVsFullFormat) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Small blob (< 2 MiB) should use compact format (kBlogBlobRecord matches
  // compact_record_type)
  std::string small_blob(100, 'S');
  uint64_t offset1;
  ASSERT_OK(writer->AddBlobRecord(wo, small_blob, kNoCompression, &offset1,
                                  small_blob.size()));

  // Large blob (> 2 MiB) should use full format
  std::string large_blob(3 * 1024 * 1024, 'L');
  uint64_t offset2;
  ASSERT_OK(writer->AddBlobRecord(wo, large_blob, kNoCompression, &offset2,
                                  large_blob.size()));

  // Read both back
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), small_blob.size());

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), large_blob.size());
  ASSERT_EQ(payload, Slice(large_blob));
}

TEST_F(BlogWriterTest, PreambleStartRecord) {
  auto header = MakeWalHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));
  uint64_t pre_offset = writer->current_offset();
  ASSERT_EQ(pre_offset % 4, 0u) << "Pre-record offset not aligned";
  ASSERT_OK(writer->AddPreambleStartRecord(wo));
  uint64_t post_offset = writer->current_offset();
  ASSERT_EQ(post_offset % 4, 0u) << "Post-record offset not aligned";

  // Use a reporter to capture errors
  class TestReporter : public BlogFileReader::Reporter {
   public:
    std::string last_msg;
    void Corruption(size_t /*bytes*/, const Status& s) override {
      last_msg = s.ToString();
    }
  };
  TestReporter reporter;

  auto reader = MakeReaderWithReporter(&reporter);
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  Status rs = reader->ReadRecord(&type, &payload, &scratch);
  if (!rs.ok()) {
    fprintf(stderr, "ReadRecord failed: %s, Reporter: %s, EOF=%d\n",
            rs.ToString().c_str(), reporter.last_msg.c_str(), reader->IsEOF());
  }
  ASSERT_OK(rs);
  ASSERT_EQ(type, kBlogPreambleStartRecord);
  ASSERT_TRUE(payload.empty());
}

TEST_F(BlogWriterTest, FooterRecords) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Write a blob record first
  std::string blob_data(50, 'B');
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset,
                                  blob_data.size()));

  ASSERT_OK(writer->WriteFooter(wo, BlogFileFooterProperties{}));

  // Read all records
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;

  // Blob record
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);

  // Footer properties record
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogFooterPropertiesRecord);
  BlogFileFooterProperties read_props;
  ASSERT_OK(read_props.DecodeFrom(payload));
  ASSERT_EQ(read_props.properties.size(), 3u);

  // Footer locator record
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogFooterLocatorRecord);
  BlogFileFooterLocator read_locator;
  ASSERT_OK(read_locator.DecodeFrom(payload));
  ASSERT_EQ(read_locator.entries.size(), 1u);
  ASSERT_EQ(read_locator.entries[0].record_type, kBlogFooterPropertiesRecord);
}

TEST_F(BlogWriterTest, ChecksumCorruptionDetected) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string blob_data(100, 'C');
  const uint64_t record_offset = writer->current_offset();
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset,
                                  blob_data.size()));

  // Corrupt one byte in the blob payload
  std::string& contents = contents_;
  ASSERT_GT(contents.size(), blob_offset + 10);
  contents[blob_offset + 10] ^= 0xFF;

  // Reading should detect the corruption
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status corruption_status =
      reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_EQ(corruption_status.code(), Status::kCorruption)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find("record checksum mismatch"),
            std::string::npos)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find("stored = "), std::string::npos)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find(", computed = "),
            std::string::npos)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find("file offset " +
                                              std::to_string(record_offset)),
            std::string::npos)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find("size 100"), std::string::npos)
      << corruption_status.ToString();
}

TEST_F(BlogWriterTest, ReadHeaderVerifiesChecksumBeforePropertySize) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));
  std::string blob_data(100, 'H');
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset,
                                  blob_data.size()));

  // Claim a property section far larger than the file, without updating the
  // fixed header checksum.
  ASSERT_LT(contents_.size(), kBlogFileFixedHeaderSize + (1 << 20));
  EncodeFixed32(&contents_[32], 1 << 20);

  auto reader = MakeReader();
  BlogFileHeader rh;
  Status s = reader->ReadHeader(&rh);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("fixed header checksum mismatch"),
            std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, CorruptHugeLengthReturnsIncompleteWithoutThrow) {
  // A full-format record whose length prefix claims far more bytes than the
  // file holds must fail cleanly instead of throwing from an upfront
  // allocation. Length 2^60 clears kMaxBlogRecordLength but can never be
  // allocated; only a few body bytes are actually present.
  auto header = MakeWalHeader();
  auto writer = MakeWriter(header);
  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  constexpr uint64_t kHugeLength = 1ULL << 60;
  ASSERT_EQ(PrefixVarint64Length(kHugeLength), 9);
  contents_.append(header.escape_sequence, kBlogEscapeSequenceSize);
  PutPrefixVarint64(&contents_, kHugeLength);
  contents_.push_back(static_cast<char>(kBlogWriteBatchRecord));
  contents_.push_back(static_cast<char>(kNoCompression));
  contents_.append("\x00\x00\x00\x00", 4);
  contents_.append("short");

  auto reader = MakeReader(/*verify_checksums=*/false);
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type = kBlogBlobRecord;
  Slice payload;
  std::string scratch;
  Status s;
  try {
    s = reader->ReadRecord(&type, &payload, &scratch);
  } catch (const std::exception& e) {
    FAIL() << "ReadRecord threw " << e.what() << " instead of returning "
           << "Incomplete";
  }
  ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
}

TEST_F(BlogWriterTest, HeaderRejectsOversizedPropertySectionBeforeReadingIt) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  // Keep the fixed header checksum valid so the size bound rejects it.
  EncodeFixed32(contents_.data() + 32, UINT32_MAX);
  EncodeFixed32(
      contents_.data() + 36,
      ComputeBuiltinChecksum(header.checksum_type, contents_.data(), 36) +
          ChecksumModifierForContext(header.incarnation_id(), 0));

  auto reader = MakeReader();
  BlogFileHeader read_header;
  const Status s = reader->ReadHeader(&read_header);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("invalid property section size"),
            std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsNontrivialEncodingOfZeroLength) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  contents_.append(header.escape_sequence, kBlogEscapeSequenceSize);
  char encoded_length[kMaxPrefixVarint64Length];
  char* encoded_end = EncodePrefixVarint64<3>(encoded_length, 0);
  contents_.append(encoded_length,
                   lossless_cast<size_t>(encoded_end - encoded_length));

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("nontrivial length encoding"), std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsReservedRecordTypeWithoutChecksumVerification) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  const uint64_t record_offset = writer->current_offset();
  ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "value"));
  const size_t type_offset = record_offset + kBlogEscapeSequenceSize +
                             RecordVarintLength(record_offset);
  contents_[type_offset] = static_cast<char>(0x14);

  auto reader = MakeReader(/*verify_checksums=*/false);
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_NE(s.ToString().find("reserved record type"), std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsInvalidCompressionWithoutChecksumVerification) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  uint64_t payload_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), "value", kNoCompression,
                                  &payload_offset, 5));
  contents_[payload_offset + 5] = static_cast<char>(0x08);

  auto reader = MakeReader(/*verify_checksums=*/false);
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("invalid compression type"), std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, FullRecordPrefixCorruptionDetected) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  const std::string blob(16384, 'B');
  const uint64_t record_offset = writer->current_offset();
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), blob, kNoCompression, nullptr,
                                  blob.size()));
  const size_t type_offset = record_offset + kBlogEscapeSequenceSize +
                             RecordVarintLength(record_offset);
  contents_[type_offset] = static_cast<char>(kBlogWriteBatchRecord);

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("prefix checksum mismatch"), std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsCompressedRecordWithoutCompatibilityName) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  uint64_t payload_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), "compressed", kNoCompression,
                                  &payload_offset, 10));
  contents_[payload_offset + 10] = lossless_cast<char>(kSnappyCompression);
  RewriteRecordTrailerChecksum(header, payload_offset, 10);

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("without CompressionCompatibilityName"),
            std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsStreamingCompressionOnNonWriteBatch) {
  auto header = MakeBlobHeader();
  header.SetProperty(kBlogPropCompressionCompatibilityName, "BuiltinV2");
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  uint64_t payload_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), "stream", kNoCompression,
                                  &payload_offset, 6));
  contents_[payload_offset + 6] =
      lossless_cast<char>(kStreamingCompressionSentinel);
  RewriteRecordTrailerChecksum(header, payload_offset, 6);

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("only valid for WriteBatch"), std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, AcceptsDeclaredWriteBatchStreamingCompression) {
  auto header = MakeWalHeader();
  header.SetProperty(kBlogPropCompressionCompatibilityName, "BuiltinV2");
  header.SetProperty(kBlogPropWriteBatchStreamingCompressionType, "07");
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "stream",
                                        kStreamingCompressionSentinel));

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  CompressionType compression_type = kNoCompression;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, nullptr,
                               &compression_type));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(compression_type, kStreamingCompressionSentinel);
}

TEST_F(BlogWriterTest, FooterRecordUsesGeneralCompressionValidation) {
  auto header = MakeBlobHeader();
  header.SetProperty(kBlogPropCompressionCompatibilityName, "BuiltinV2");
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  const uint64_t record_offset = writer->current_offset();
  BlogFileFooterProperties props;
  props.SetSequenceRange(5, 7);
  std::string properties_payload;
  props.EncodeTo(&properties_payload);
  ASSERT_OK(writer->WriteFooter(WriteOptions(), props));
  const size_t varint_len = RecordVarintLength(record_offset);
  const size_t prefix_compression_offset =
      record_offset + kBlogEscapeSequenceSize + varint_len + 1;
  const uint64_t payload_offset =
      record_offset + kBlogEscapeSequenceSize + varint_len + 6;
  contents_[prefix_compression_offset] = static_cast<char>(kSnappyCompression);
  contents_[payload_offset + properties_payload.size()] =
      static_cast<char>(kSnappyCompression);
  RewriteFullPrefixChecksum(header, record_offset);
  RewriteRecordTrailerChecksum(header, payload_offset,
                               properties_payload.size());
  const size_t padding_offset =
      payload_offset + properties_payload.size() + kBlogBlockTrailerSize;
  const uint8_t last_meaningful_byte =
      lossless_cast<uint8_t>(contents_[padding_offset - 1]);
  const size_t next_record_offset = contents_.find(
      std::string(header.escape_sequence, kBlogEscapeSequenceSize),
      padding_offset);
  ASSERT_NE(next_record_offset, std::string::npos);
  std::fill(
      contents_.begin() + padding_offset,
      contents_.begin() + next_record_offset,
      lossless_cast<char>(ChooseBlogPaddingByte(last_meaningful_byte, false)));

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  CompressionType compression_type = kNoCompression;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, nullptr,
                               &compression_type));
  ASSERT_EQ(type, kBlogFooterPropertiesRecord);
  ASSERT_EQ(payload, properties_payload);
  ASSERT_EQ(compression_type, kSnappyCompression);
}

// Padding carries no semantic content, so a gap that mixes 0x00 and 0xFF
// bytes is skipped like any other gap; the flanking records still verify.
TEST_F(BlogWriterTest, SkipsMixedPaddingBytes) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  uint64_t first_payload_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), "a", kNoCompression,
                                  &first_payload_offset, 1));
  const uint64_t second_record_offset = writer->current_offset();
  ASSERT_OK(
      writer->AddBlobRecord(WriteOptions(), "b", kNoCompression, nullptr, 1));

  const uint64_t padding_offset =
      first_payload_offset + 1 + kBlogBlockTrailerSize;
  ASSERT_GT(second_record_offset, padding_offset + 1);
  contents_[padding_offset + 1] =
      contents_[padding_offset] == '\0' ? '\xFF' : '\0';

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(payload, "a");
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(payload, "b");
  ASSERT_TRUE(reader->ReadRecord(&type, &payload, &scratch).IsNotFound());
}

// A non-pad byte before the next alignment boundary cannot start a record
// (records only start aligned), so it is reported immediately rather than
// handed to the record parse.
TEST_F(BlogWriterTest, RejectsGarbageBeforeAlignmentBoundary) {
  // Find a payload whose record ends unaligned, leaving pre-boundary gap
  // bytes to corrupt. Canonical layout keeps this deterministic.
  std::string payload;
  size_t record_end = 0;
  bool found = false;
  for (size_t len = 1; len <= 8; ++len) {
    auto header = MakeBlobHeader();
    auto writer = MakeWriter(header);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    payload.assign(len, 'a');
    uint64_t payload_offset = 0;
    ASSERT_OK(writer->AddBlobRecord(WriteOptions(), payload, kNoCompression,
                                    &payload_offset, payload.size()));
    record_end = payload_offset + payload.size() + kBlogBlockTrailerSize;
    ASSERT_OK(
        writer->AddBlobRecord(WriteOptions(), "b", kNoCompression, nullptr, 1));
    ASSERT_OK(writer->Close(WriteOptions()));
    if (record_end % kBlogMinRecordAlignment != 0) {
      found = true;
      break;
    }
  }
  ASSERT_TRUE(found) << "no payload length ended unaligned";

  // A bitflip turning the first gap byte into garbage.
  contents_[record_end] = '\x42';

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice record;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
  ASSERT_EQ(record, payload);
  const Status s = reader->ReadRecord(&type, &record, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("non-padding byte"), std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsDataAfterFooterLocator) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  ASSERT_OK(
      writer->AddBlobRecord(WriteOptions(), "v", kNoCompression, nullptr, 1));

  ASSERT_OK(writer->WriteFooter(WriteOptions(), BlogFileFooterProperties{}));
  contents_.push_back('x');

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("footer locator is not the last"),
            std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, EscapeSequenceScanCanResumeRecordReading) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  const uint64_t record_offset = writer->current_offset();
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), "value", kNoCompression,
                                  nullptr, 5));

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  uint64_t found_offset = 0;
  ASSERT_OK(reader->ScanForEscapeSequence(&found_offset));
  ASSERT_EQ(found_offset, record_offset);

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload, "value");
}

TEST_F(BlogWriterTest, MixedRecordTypes) {
  // Use blob compact type but also write WriteBatch records (full format)
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Blob (compact format)
  std::string blob(50, 'B');
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob, kNoCompression, &blob_offset,
                                  blob.size()));

  // WriteBatch (full format since it doesn't match compact_record_type)
  std::string wb(60, 'W');
  ASSERT_OK(writer->AddWriteBatchRecord(wo, wb));

  // Another blob (compact)
  std::string blob2(70, 'D');
  uint64_t blob2_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob2, kNoCompression, &blob2_offset,
                                  blob2.size()));

  // Read all back
  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), 50u);

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(payload.size(), 60u);

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload.size(), 70u);

  const Status eof_status = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_EQ(eof_status.code(), Status::kNotFound) << eof_status.ToString();
}

TEST_F(BlogWriterTest, AlignmentInvariant) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Write records of various sizes and verify alignment is maintained
  for (int size = 1; size <= 200; ++size) {
    std::string data(size, static_cast<char>(size & 0xFF));
    uint64_t offset;
    ASSERT_OK(
        writer->AddBlobRecord(wo, data, kNoCompression, &offset, data.size()));
    ASSERT_EQ(writer->current_offset() % 4, 0u)
        << "Alignment violated after record of size " << size;
  }
}

TEST_F(BlogWriterTest, HeaderWithProperties) {
  auto header = MakeBlobHeader();
  header.SetProperty("CompressionCompatibilityName", "zstd");
  header.SetProperty("role", "blob");
  header.SetProperty("compressionSettings", "level=3");
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  // Write a blob so the reader has something after the ignorable props record.
  std::string blob_data(50, 'B');
  uint64_t blob_offset;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset,
                                  blob_data.size()));
  ASSERT_OK(writer->Sync(wo));
  ASSERT_OK(writer->Close(wo));

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));
  // Required property is in the header section
  ASSERT_EQ(rh.GetProperty("CompressionCompatibilityName"), "zstd");
  // Ignorable properties are NOT in the header (they're in a body record)
  ASSERT_EQ(rh.GetProperty("role"), "");
  ASSERT_EQ(rh.GetProperty("compressionSettings"), "");

  // The reader skips ignorable properties records, so the first record
  // returned is the blob.
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload, blob_data);
  // The skipped ignorable properties record was merged into the reader's
  // header view.
  ASSERT_EQ(reader->header().GetProperty("role"), "blob");
  ASSERT_EQ(reader->header().GetProperty("compressionSettings"), "level=3");
}

TEST_F(BlogWriterTest, NoncanonicalUnspecifiedSizeRecordReadsBack) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.seed = 11;
  config.unspecified_size_record_one_in = 1;
  config.force_full_record_one_in = 1;
  config.overlong_full_varint_one_in = 0;
  config.extra_padding_512b_one_in = 0;
  config.extra_padding_4k_one_in = 0;
  config.inject_auxiliary_record_one_in = 0;
  config.shuffle_properties_one_in = 0;
  config.split_ignorable_properties_one_in = 0;
  TEST_BlogNoncanonicalConfigScope config_scope(config);

  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  const std::string blob_data(128, 'U');
  uint64_t blob_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset,
                                  blob_data.size()));

  ASSERT_OK(writer->WriteFooter(wo, BlogFileFooterProperties{}));
  ASSERT_OK(writer->Close(wo));

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload, blob_data);
}

TEST_F(BlogWriterTest,
       NoncanonicalUnspecifiedSizeRecordIgnoresEmbeddedEscapeSequences) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.seed = 23;
  config.unspecified_size_record_one_in = 1;
  config.force_full_record_one_in = 1;
  config.overlong_full_varint_one_in = 0;
  config.extra_padding_512b_one_in = 0;
  config.extra_padding_4k_one_in = 0;
  config.inject_auxiliary_record_one_in = 0;
  config.shuffle_properties_one_in = 0;
  config.split_ignorable_properties_one_in = 0;
  TEST_BlogNoncanonicalConfigScope config_scope(config);

  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string blob_data(128, 'E');
  // Plant six aligned copies of the file escape sequence inside the payload.
  // The two preceding 0x00/0xFF bytes make each one look like a plausible
  // next-record boundary, so the reader has to reject them by checksum rather
  // than by obvious shape alone. There is intentionally no fixed lookahead
  // limit because payload bytes are unrestricted.
  const size_t escape_offsets[] = {7, 27, 47, 67, 87, 107};
  for (size_t i = 0; i < sizeof(escape_offsets) / sizeof(escape_offsets[0]);
       ++i) {
    const size_t offset = escape_offsets[i];
    blob_data[offset - 2] = (i % 2 == 0) ? '\0' : '\xFF';
    blob_data[offset - 1] = (i % 2 == 0) ? '\0' : '\xFF';
    memcpy(blob_data.data() + offset, header.escape_sequence,
           kBlogEscapeSequenceSize);
  }

  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, nullptr,
                                  blob_data.size()));

  // Append real footer records so unspecified-size parsing eventually reaches
  // a valid next-record boundary after skipping the embedded lookalikes.
  ASSERT_OK(writer->WriteFooter(wo, BlogFileFooterProperties{}));
  ASSERT_OK(writer->Close(wo));

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload, blob_data);
}

TEST_F(BlogWriterTest,
       NoncanonicalUnspecifiedSizeRecordScansAllCandidatesBeforeCorruption) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.seed = 29;
  config.unspecified_size_record_one_in = 1;
  config.force_full_record_one_in = 1;
  config.overlong_full_varint_one_in = 0;
  config.extra_padding_512b_one_in = 0;
  config.extra_padding_4k_one_in = 0;
  config.inject_auxiliary_record_one_in = 0;
  config.shuffle_properties_one_in = 0;
  config.split_ignorable_properties_one_in = 0;
  TEST_BlogNoncanonicalConfigScope config_scope(config);

  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));

  std::string blob_data(128, 'Q');
  // The first four checked candidates are embedded escape-sequence lookalikes
  // inside the payload. Their preceding 0x00/0xFF byte makes each one look
  // like a plausible padded boundary, so the reader must reject them by
  // checksum. After those four false positives, the real next-record boundary
  // becomes the fifth checked candidate.
  const size_t escape_offsets[] = {7, 27, 47, 67};
  for (size_t i = 0; i < sizeof(escape_offsets) / sizeof(escape_offsets[0]);
       ++i) {
    const size_t offset = escape_offsets[i];
    blob_data[offset - 1] = (i % 2 == 0) ? '\0' : '\xFF';
    memcpy(blob_data.data() + offset, header.escape_sequence,
           kBlogEscapeSequenceSize);
  }

  uint64_t blob_offset = 0;
  const uint64_t record_offset = writer->current_offset();
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, &blob_offset,
                                  blob_data.size()));

  const uint64_t props_offset = writer->current_offset();
  ASSERT_OK(writer->WriteFooter(wo, BlogFileFooterProperties{}));
  const size_t locator_offset = contents_.find(
      std::string(header.escape_sequence, kBlogEscapeSequenceSize),
      props_offset + kBlogEscapeSequenceSize);
  ASSERT_NE(locator_offset, std::string::npos);
  ASSERT_OK(writer->Close(wo));

  std::string& contents = contents_;
  // blob_offset is the payload start, so + blob_data.size() reaches the
  // 5-byte trailer and + 1 flips the first checksum byte. The real next-record
  // candidate at props_offset therefore also checksum-mismatches.
  ASSERT_GT(contents.size(), blob_offset + blob_data.size() + 4);
  contents[blob_offset + blob_data.size() + 1] ^= 0x80;

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status corruption_status =
      reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_EQ(corruption_status.code(), Status::kCorruption)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find(
                "unspecified-size record checksum mismatch"),
            std::string::npos)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find("record at file offset " +
                                              std::to_string(record_offset)),
            std::string::npos)
      << corruption_status.ToString();
  ASSERT_NE(corruption_status.ToString().find(
                "candidate escape sequence file offset " +
                std::to_string(locator_offset)),
            std::string::npos)
      << corruption_status.ToString();
  // Four embedded false positives, the corrupted real boundary, and the next
  // footer record boundary were all considered before EOF.
  ASSERT_NE(
      corruption_status.ToString().find("escape-sequence candidates checked=6"),
      std::string::npos)
      << corruption_status.ToString();
}

TEST_F(BlogWriterTest, NoncanonicalPaddingCanAlignToFlushBoundary) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.seed = 19;
  config.extra_padding_512b_one_in = 1;
  config.extra_padding_4k_one_in = 0;
  config.prefer_padding_with_0xff_one_in = 1;
  config.inject_auxiliary_record_one_in = 0;
  config.unspecified_size_record_one_in = 0;
  TEST_BlogNoncanonicalConfigScope config_scope(config);

  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  const std::string blob_data(17, 'P');

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, nullptr, 17));
  ASSERT_EQ(writer->current_offset() % kBlogFlushFriendlyAlignment, 0u);

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload, blob_data);
}

TEST_F(BlogWriterTest, HeaderIgnorablePropertiesCanSplitIntoMultipleRecords) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.seed = 23;
  config.split_ignorable_properties_one_in = 1;
  config.shuffle_properties_one_in = 1;
  config.inject_auxiliary_record_one_in = 0;
  TEST_BlogNoncanonicalConfigScope config_scope(config);

  auto header = MakeBlobHeader();
  header.SetProperty("role", "blob");
  header.SetProperty("compressionSettings", "level=7");
  auto writer = MakeWriter(header);

  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  ASSERT_OK(
      writer->AddBlobRecord(WriteOptions(), "x", kNoCompression, nullptr, 1));

  ASSERT_GE(CountEscapeSequences(header), 3u);
}

TEST_F(BlogWriterTest, BlobFilesCanInjectAuxiliaryWriteBatchRecords) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.seed = 29;
  config.inject_auxiliary_record_one_in = 1;
  config.max_auxiliary_payload_bytes = 8;
  config.unspecified_size_record_one_in = 0;
  TEST_BlogNoncanonicalConfigScope config_scope(config);

  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));
  ASSERT_OK(writer->AddBlobRecord(wo, "blob", kNoCompression, nullptr, 4));

  ASSERT_OK(writer->WriteFooter(wo, BlogFileFooterProperties{}));
  ASSERT_OK(writer->Close(wo));

  auto reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));

  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload, "blob");

  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_FALSE(payload.empty());
}

TEST_F(BlogWriterTest, RejectsOversizedHeaderPropertiesBeforeWriting) {
  BlogFileHeader header = MakeBlobHeader();
  header.SetProperty("PredecessorWalInfo",
                     std::string(kBlogMaxPropertySectionSize, 'x'));
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  const IOStatus s = writer->WriteHeader(WriteOptions());
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_TRUE(contents_.empty());
}

TEST_F(BlogWriterTest, RejectsOversizedFooterPropertiesBeforeWriting) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  const uint64_t offset = writer->current_offset();

  BlogFileFooterProperties properties;
  properties.properties.emplace_back(
      "oversized", std::string(kBlogMaxPropertySectionSize, 'x'));
  const IOStatus s = writer->WriteFooter(WriteOptions(), properties);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ(writer->current_offset(), offset);
}

TEST_F(BlogWriterTest, RejectsFooterPropertiesMadeOversizedByLiveStats) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  ASSERT_OK(
      writer->AddBlobRecord(WriteOptions(), "v", kNoCompression, nullptr, 1));
  const uint64_t offset = writer->current_offset();

  BlogFileFooterProperties properties;
  properties.properties.emplace_back(
      "x", std::string(kBlogMaxPropertySectionSize - 16, 'x'));
  const IOStatus s = writer->WriteFooter(WriteOptions(), properties);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ(writer->current_offset(), offset);
}

TEST_F(BlogWriterTest, RejectsNontrivialPreambleStartRecord) {
  auto header = MakeBlobHeader();
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  const uint64_t record_offset = writer->current_offset();
  ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "value"));
  const size_t type_offset = record_offset + kBlogEscapeSequenceSize +
                             RecordVarintLength(record_offset);
  contents_[type_offset] = static_cast<char>(kBlogPreambleStartRecord);

  auto reader = MakeReader(/*verify_checksums=*/false);
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find("preamble-start record must use trivial format"),
            std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsFullRecordCompressionMismatch) {
  auto header = MakeBlobHeader();
  header.SetProperty(kBlogPropCompressionCompatibilityName, "BuiltinV2");
  auto writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));

  const std::string blob(16384, 'B');
  const uint64_t record_offset = writer->current_offset();
  uint64_t payload_offset = 0;
  ASSERT_OK(writer->AddBlobRecord(WriteOptions(), blob, kNoCompression,
                                  &payload_offset, blob.size()));
  contents_[payload_offset + blob.size()] =
      static_cast<char>(kSnappyCompression);
  RewriteRecordTrailerChecksum(header, payload_offset, blob.size());

  auto reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  const Status s = reader->ReadRecord(&type, &payload, &scratch);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_EQ(reader->LastRecordOffset(), record_offset);
  ASSERT_NE(s.ToString().find("prefix/trailer compression type mismatch"),
            std::string::npos)
      << s.ToString();
}

TEST_F(BlogWriterTest, RejectsInvalidFullRecordPrefixCompression) {
  for (ChecksumType checksum : {kXXH3, kNoChecksum}) {
    for (bool verify_checksums : {false, true}) {
      for (uint64_t length : {uint64_t{1}, kBlogUnspecifiedSize}) {
        SCOPED_TRACE("checksum=" + std::to_string(checksum) +
                     " verify=" + std::to_string(verify_checksums) +
                     " length=" + std::to_string(length));
        BlogFileHeader header = MakeBlobHeader();
        header.checksum_type = checksum;
        std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
        ASSERT_OK(writer->WriteHeader(WriteOptions()));
        AppendTestRecord(header, kBlogBlobRecord, length,
                         lossless_cast<CompressionType>(uint8_t{0x08}),
                         kNoCompression, "v");
        ExpectRecordCorruption(verify_checksums, "invalid compression type");
      }
    }
  }
}

TEST_F(BlogWriterTest, RejectsCompressionMismatchWithoutChecksums) {
  for (ChecksumType checksum : {kXXH3, kNoChecksum}) {
    for (bool verify_checksums : {false, true}) {
      for (uint64_t length : {uint64_t{1}, kBlogUnspecifiedSize}) {
        SCOPED_TRACE("checksum=" + std::to_string(checksum) +
                     " verify=" + std::to_string(verify_checksums) +
                     " length=" + std::to_string(length));
        BlogFileHeader header = MakeBlobHeader();
        header.checksum_type = checksum;
        header.SetProperty(kBlogPropCompressionCompatibilityName, "BuiltinV2");
        std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
        ASSERT_OK(writer->WriteHeader(WriteOptions()));
        AppendTestRecord(header, kBlogBlobRecord, length, kSnappyCompression,
                         kNoCompression, "v");
        if (length == kBlogUnspecifiedSize) {
          AppendTestRecord(header, kBlogPreambleStartRecord, 0, kNoCompression,
                           kNoCompression, Slice());
        }
        ExpectRecordCorruption(verify_checksums,
                               "prefix/trailer compression type mismatch");
      }
    }
  }
}

TEST_F(BlogWriterTest, RejectsUnspecifiedSizePreambleStartRecord) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  AppendTestRecord(header, kBlogPreambleStartRecord, kBlogUnspecifiedSize,
                   kNoCompression, kNoCompression, "v");
  ExpectRecordCorruption(false,
                         "preamble-start record must use trivial format");
}

TEST_F(BlogWriterTest, RejectsTrivialFooterLocator) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  AppendTestRecord(header, kBlogFooterLocatorRecord, 0, kNoCompression,
                   kNoCompression, Slice());
  ExpectRecordCorruption(false, "footer locator must have a specified");
}

TEST_F(BlogWriterTest, RejectsUnspecifiedSizeFooterLocator) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  AppendTestRecord(header, kBlogFooterLocatorRecord, kBlogUnspecifiedSize,
                   kNoCompression, kNoCompression, "v");
  ExpectRecordCorruption(false, "footer locator must have a specified");
}

TEST_F(BlogWriterTest, RejectsOutOfOrderFooterRecords) {
  const BlogRecordType sequences[][2] = {
      {kBlogFooterPropertiesRecord, kBlogFooterIndexRecord},
      {kBlogFooterFileChecksumInfo, kBlogFooterIndexRecord},
      {kBlogFooterFileChecksumInfo, kBlogFooterPropertiesRecord},
      {kBlogFooterPropertiesRecord, kBlogFooterPropertiesRecord},
      {kBlogFooterFileChecksumInfo, kBlogFooterFileChecksumInfo},
  };
  for (const auto& sequence : sequences) {
    SCOPED_TRACE("first=" + std::to_string(sequence[0]) +
                 " second=" + std::to_string(sequence[1]));
    BlogFileHeader header = MakeBlobHeader();
    header.checksum_type = kNoChecksum;
    std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    for (BlogRecordType type : sequence) {
      AppendTestRecord(header, type, 1, kNoCompression, kNoCompression, "v");
    }
    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader read_header;
    ASSERT_OK(reader->ReadHeader(&read_header));
    BlogRecordType type;
    Slice payload;
    std::string scratch;
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    const Status s = reader->ReadRecord(&type, &payload, &scratch);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    ASSERT_NE(s.ToString().find("footer"), std::string::npos) << s.ToString();
  }
}

TEST_F(BlogWriterTest, AcceptsMultipleFooterIndexes) {
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  const BlogRecordType sequence[] = {
      kBlogFooterIndexRecord, kBlogFooterIndexRecord,
      kBlogFooterPropertiesRecord, kBlogFooterFileChecksumInfo};
  for (BlogRecordType type : sequence) {
    AppendTestRecord(header, type, 1, kNoCompression, kNoCompression, "v");
  }
  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  for (BlogRecordType expected : sequence) {
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    ASSERT_EQ(type, expected);
  }
}

TEST_F(BlogWriterTest, RejectsInvalidStreamingHeader) {
  for (const std::string& value : {std::string(), std::string("0"),
                                   std::string("zz"), std::string("7f")}) {
    SCOPED_TRACE(value);
    BlogFileHeader header = MakeWalHeader();
    header.SetProperty(kBlogPropWriteBatchStreamingCompressionType, value);
    std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader read_header;
    const Status s = reader->ReadHeader(&read_header);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
