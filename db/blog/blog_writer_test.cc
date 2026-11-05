//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_writer.h"

#include <string>
#include <vector>

#include "db/blog/blog_reader.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"

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
  std::unique_ptr<BlogFileReader> MakeReader() {
    return MakeReaderImpl(nullptr);
  }

  std::unique_ptr<BlogFileReader> MakeReaderImpl(
      BlogFileReader::Reporter* reporter) {
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
                                            true /* verify_checksums */);
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

  // Write footer properties
  uint64_t props_offset = writer->current_offset();
  BlogFileFooterProperties props;
  props.SetBlobCount(1);
  props.SetBlobPayloadBytes(50);
  ASSERT_OK(writer->AddFooterPropertiesRecord(wo, props));

  // Write footer locator
  BlogFileFooterLocator locator;
  uint64_t locator_offset = writer->current_offset();
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord,
       static_cast<uint32_t>((locator_offset - props_offset) / 4)});
  ASSERT_OK(writer->AddFooterLocatorRecord(wo, locator));

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
  ASSERT_EQ(read_props.properties.size(), 2u);

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

  BlogFileFooterProperties props;
  props.SetBlobCount(1);
  const uint64_t props_offset = writer->current_offset();
  ASSERT_OK(writer->AddFooterPropertiesRecord(wo, props));

  BlogFileFooterLocator locator;
  const uint64_t locator_offset = writer->current_offset();
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord,
       static_cast<uint32_t>((locator_offset - props_offset) / 4)});
  ASSERT_OK(writer->AddFooterLocatorRecord(wo, locator));
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
  // Plant four aligned copies of the file escape sequence inside the payload.
  // The two preceding 0x00/0xFF bytes make each one look like a plausible
  // next-record boundary, so the reader has to reject them by checksum rather
  // than by obvious shape alone. This stays on the success side of the bounded
  // lookahead; the corruption test below reuses four embedded candidates but
  // corrupts the real trailer so the fifth checked candidate reports
  // corruption.
  const size_t escape_offsets[] = {7, 27, 47, 67};
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

  BlogFileFooterProperties props;
  props.SetBlobCount(1);
  // Append real footer records so unspecified-size parsing eventually reaches
  // a valid next-record boundary after skipping the embedded lookalikes.
  const uint64_t props_offset = writer->current_offset();
  ASSERT_OK(writer->AddFooterPropertiesRecord(wo, props));

  BlogFileFooterLocator locator;
  const uint64_t locator_offset = writer->current_offset();
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord,
       static_cast<uint32_t>((locator_offset - props_offset) / 4)});
  ASSERT_OK(writer->AddFooterLocatorRecord(wo, locator));
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

TEST_F(
    BlogWriterTest,
    NoncanonicalUnspecifiedSizeRecordReportsChecksumCorruptionAfterLimitedLookahead) {
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

  BlogFileFooterProperties props;
  props.SetBlobCount(1);
  const uint64_t props_offset = writer->current_offset();
  ASSERT_OK(writer->AddFooterPropertiesRecord(wo, props));

  BlogFileFooterLocator locator;
  const uint64_t locator_offset = writer->current_offset();
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord,
       static_cast<uint32_t>((locator_offset - props_offset) / 4)});
  ASSERT_OK(writer->AddFooterLocatorRecord(wo, locator));
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
                std::to_string(props_offset)),
            std::string::npos)
      << corruption_status.ToString();
  // The embedded false positives use one pad byte, so their candidate payload
  // sizes are offset - 6 => 1, 21, 41, and 61. The real next-record boundary
  // contributes the fifth checked payload size, 128.
  ASSERT_NE(corruption_status.ToString().find(
                "candidate payload sizes checked=[1,21,41,61,128]"),
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

  BlogFileFooterProperties props;
  props.SetBlobCount(1);
  const uint64_t props_offset = writer->current_offset();
  ASSERT_OK(writer->AddFooterPropertiesRecord(wo, props));

  BlogFileFooterLocator locator;
  const uint64_t locator_offset = writer->current_offset();
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord,
       static_cast<uint32_t>((locator_offset - props_offset) / 4)});
  ASSERT_OK(writer->AddFooterLocatorRecord(wo, locator));
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
