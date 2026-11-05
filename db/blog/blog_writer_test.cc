//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_writer.h"

#include <algorithm>
#include <string>
#include <vector>

#include "db/blog/blog_reader.h"
#include "db/blog_wal_writer.h"
#include "db/dbformat.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/write_buffer_manager.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/prefix_varint.h"
#include "util/random.h"
#include "util/udt_util.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class BlogWriterTest : public testing::Test {
 protected:
  // Backing storage for the in-memory file. It must outlive the sink so tests
  // can still read the bytes after writer->Close() destroys the sink object.
  std::string contents_;

  // Create a writer with the given header. Defaults to the blob-file policy,
  // which permits unspecified-size records. Tests that care about a writer
  // which cannot use the encoding -- a recycled WAL -- must pass
  // BlogUnspecifiedSizeRecords::kDisallowed explicitly.
  std::unique_ptr<BlogFileWriter> MakeWriter(
      const BlogFileHeader& header,
      BlogUnspecifiedSizeRecords unspecified_size_records =
          BlogUnspecifiedSizeRecords::kAllowed) {
    contents_.clear();
    std::unique_ptr<FSWritableFile> sink_holder(
        new test::StringFS::StringSink(&contents_));
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions()));
    return std::make_unique<BlogFileWriter>(std::move(file_writer), header,
                                            unspecified_size_records);
  }

  // Config that makes every eligible record use the unspecified-size encoding
  // and otherwise keeps the on-disk layout canonical, so tests can reason
  // about exact record boundaries.
  static TEST_BlogNoncanonicalConfig UnspecifiedSizeOnlyConfig(uint64_t seed) {
    TEST_BlogNoncanonicalConfig config;
    config.enabled = true;
    config.seed = seed;
    config.unspecified_size_record_one_in = 1;
    config.force_full_record_one_in = 1;
    config.overlong_full_varint_one_in = 0;
    config.extra_padding_512b_one_in = 0;
    config.extra_padding_4k_one_in = 0;
    config.inject_auxiliary_record_one_in = 0;
    config.shuffle_properties_one_in = 0;
    config.split_ignorable_properties_one_in = 0;
    return config;
  }

  // Offset of the first unspecified-size record's escape sequence, or npos.
  size_t FindUnspecifiedSizeRecordOffset(const BlogFileHeader& header) const {
    std::string needle(header.escape_sequence, kBlogEscapeSequenceSize);
    PutPrefixVarint64(&needle, kBlogUnspecifiedSize);
    return contents_.find(needle);
  }

  // Number of unspecified-size record prefixes in the file: escape sequence
  // followed by the sentinel length varint.
  size_t CountUnspecifiedSizeRecords(const BlogFileHeader& header) const {
    std::string needle(header.escape_sequence, kBlogEscapeSequenceSize);
    PutPrefixVarint64(&needle, kBlogUnspecifiedSize);
    size_t count = 0;
    for (size_t pos = contents_.find(needle); pos != std::string::npos;
         pos = contents_.find(needle, pos + 1)) {
      ++count;
    }
    return count;
  }

  class FailingStreamingCompressor : public StreamingCompress {
   public:
    FailingStreamingCompressor()
        : StreamingCompress(kZSTD, CompressionOptions(), 2, 0) {}

    int Compress(const char*, size_t, char*, size_t*) override { return -1; }
    Status CompressAndFlush(const Slice&, std::string*) override {
      return Status::Corruption("Injected compression failure before output");
    }
    Status CompressAndFlushToSink(const Slice&,
                                  StreamingCompressOutput*) override {
      return Status::Corruption("Injected compression failure before output");
    }
    void Reset() override {}
  };

  void FailStreamingCompressionBeforeOutput(BlogWalWriter* writer) {
    writer->streaming_compressor_ =
        std::make_unique<FailingStreamingCompressor>();
  }

  uint64_t WalWriterOffset(const BlogWalWriter* writer) const {
    return writer->writer_->current_offset();
  }

  std::unique_ptr<BlogWalWriter> MakeWalWriterWithSink(
      std::unique_ptr<FSWritableFile> sink_holder, bool is_recycled,
      size_t blog_timestamp_size) {
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions()));
    return std::make_unique<BlogWalWriter>(
        std::move(file_writer), /*log_number=*/123, is_recycled,
        /*manual_flush=*/false, kNoCompression,
        /*use_streaming_compression=*/false, /*track_and_verify_wals=*/true,
        kXXH3, blog_timestamp_size, "db-id", "session-id", "host-id",
        /*creation_time=*/456);
  }

  std::unique_ptr<BlogWalWriter> MakeStreamingWalWriterWithSink(
      std::unique_ptr<FSWritableFile> sink_holder,
      CompressionType compression_type, bool is_recycled) {
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions()));
    return std::make_unique<BlogWalWriter>(
        std::move(file_writer), /*log_number=*/123, is_recycled,
        /*manual_flush=*/false, compression_type,
        /*use_streaming_compression=*/true, /*track_and_verify_wals=*/true,
        kXXH3, /*blog_timestamp_size=*/0, "db-id", "session-id", "host-id",
        /*creation_time=*/456);
  }

  std::unique_ptr<BlogWalWriter> MakeStreamingWalWriter(
      CompressionType compression_type) {
    contents_.clear();
    return MakeStreamingWalWriterWithSink(
        std::unique_ptr<FSWritableFile>(
            new test::StringFS::StringSink(&contents_)),
        compression_type, /*is_recycled=*/true);
  }

  std::unique_ptr<BlogWalWriter> MakeNonRecycledStreamingWalWriter(
      CompressionType compression_type) {
    contents_.clear();
    return MakeStreamingWalWriterWithSink(
        std::unique_ptr<FSWritableFile>(
            new test::StringFS::StringSink(&contents_)),
        compression_type, /*is_recycled=*/false);
  }

  std::unique_ptr<BlogWalWriter> MakeWalWriterWithSink(
      std::unique_ptr<FSWritableFile> sink_holder) {
    return MakeWalWriterWithSink(std::move(sink_holder),
                                 /*is_recycled=*/true,
                                 /*blog_timestamp_size=*/0);
  }

  std::unique_ptr<BlogWalWriter> MakeWalWriter(bool is_recycled,
                                               size_t blog_timestamp_size) {
    contents_.clear();
    return MakeWalWriterWithSink(
        std::unique_ptr<FSWritableFile>(
            new test::StringFS::StringSink(&contents_)),
        is_recycled, blog_timestamp_size);
  }

  std::unique_ptr<BlogWalWriter> MakeWalWriter(bool is_recycled) {
    return MakeWalWriter(is_recycled, /*blog_timestamp_size=*/0);
  }

  std::unique_ptr<BlogWalWriter> MakeWalWriter() {
    contents_.clear();
    return MakeWalWriterWithSink(std::unique_ptr<FSWritableFile>(
        new test::StringFS::StringSink(&contents_)));
  }

  // FSWritableFile whose Close fails, to pin Close-on-error behavior.
  class FailingCloseSink : public test::StringFS::StringSink {
   public:
    explicit FailingCloseSink(std::string* contents)
        : test::StringFS::StringSink(contents) {}
    IOStatus Close(const IOOptions& /*opts*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::IOError("Injected close error");
    }
  };

  std::unique_ptr<BlogWalWriter> MakeWalWriterWithFailingClose() {
    contents_.clear();
    return MakeWalWriterWithSink(
        std::unique_ptr<FSWritableFile>(new FailingCloseSink(&contents_)));
  }

  struct CloseState {
    bool fail_append = false;
    bool fail_truncate = false;
    bool fail_close = false;
    int closes = 0;
    int syncs = 0;
  };

  class CloseTrackingSink : public test::StringFS::StringSink {
   public:
    CloseTrackingSink(std::string* contents, CloseState* state)
        : test::StringFS::StringSink(contents), state_(state) {}

    using test::StringFS::StringSink::Append;
    IOStatus Append(const Slice& slice, const IOOptions& opts,
                    IODebugContext* dbg) override {
      if (state_->fail_append) {
        return IOStatus::IOError("injected footer append error");
      }
      return test::StringFS::StringSink::Append(slice, opts, dbg);
    }
    IOStatus Truncate(uint64_t size, const IOOptions& opts,
                      IODebugContext* dbg) override {
      if (state_->fail_truncate) {
        return IOStatus::IOError("injected footer truncate error");
      }
      return test::StringFS::StringSink::Truncate(size, opts, dbg);
    }
    IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override {
      ++state_->closes;
      if (state_->fail_close) {
        return IOStatus::IOError("injected file close error");
      }
      return test::StringFS::StringSink::Close(opts, dbg);
    }
    IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override {
      ++state_->syncs;
      return test::StringFS::StringSink::Sync(opts, dbg);
    }

   private:
    CloseState* state_;
  };

  // FSWritableFile that fails a single Append by ordinal (1-indexed), dropping
  // those bytes. Set fail_on to -1 to disable failures.
  class FailingAppendSink : public test::StringFS::StringSink {
   public:
    FailingAppendSink(std::string* contents, int fail_on)
        : test::StringFS::StringSink(contents), fail_on_(fail_on) {}
    using test::StringFS::StringSink::Append;
    IOStatus Append(const Slice& slice, const IOOptions& opts,
                    IODebugContext* dbg) override {
      ++append_count_;
      if (append_count_ == fail_on_) {
        return IOStatus::IOError("Injected append error");
      }
      return test::StringFS::StringSink::Append(slice, opts, dbg);
    }
    int append_count() const { return append_count_; }
    void set_fail_on(int fail_on) { fail_on_ = fail_on; }

   private:
    int fail_on_;
    int append_count_ = 0;
  };

  std::unique_ptr<BlogWalWriter> MakeWalWriterWithSinkAndBufferSize(
      std::unique_ptr<FSWritableFile> sink_holder, size_t max_buffer_size) {
    FileOptions file_opts;
    file_opts.writable_file_max_buffer_size = max_buffer_size;
    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(sink_holder), "", file_opts));
    return std::make_unique<BlogWalWriter>(
        std::move(file_writer), /*log_number=*/123, /*is_recycled=*/true,
        /*manual_flush=*/false, kNoCompression,
        /*use_streaming_compression=*/false, /*track_and_verify_wals=*/true,
        kXXH3, /*blog_timestamp_size=*/0, "db-id", "session-id", "host-id",
        /*creation_time=*/456);
  }

  // Like MakeWalWriterWithSink, but over a real named file, so Close() sees a
  // filesystem with its own notion of the file's size.
  std::unique_ptr<BlogWalWriter> MakeFileWalWriter(
      std::unique_ptr<FSWritableFile> sink_holder, const std::string& fname,
      bool is_recycled) {
    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(sink_holder), fname, FileOptions()));
    return std::make_unique<BlogWalWriter>(
        std::move(file_writer), /*log_number=*/123, is_recycled,
        /*manual_flush=*/false, kNoCompression,
        /*use_streaming_compression=*/false, /*track_and_verify_wals=*/true,
        kXXH3, /*blog_timestamp_size=*/0, "db-id", "session-id", "host-id",
        /*creation_time=*/456);
  }

  // Reads every record from `fname` through `fs`, collecting record types.
  // Returns the terminal status (NotFound on clean EOF).
  Status ReadAllRecordTypes(FileSystem* fs, const std::string& fname,
                            std::vector<BlogRecordType>* types) {
    std::unique_ptr<FSSequentialFile> file;
    IOStatus io_s =
        fs->NewSequentialFile(fname, FileOptions(), &file, /*dbg=*/nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    std::unique_ptr<SequentialFileReader> seq_reader =
        std::make_unique<SequentialFileReader>(std::move(file), fname);
    BlogFileReader reader(std::move(seq_reader), /*reporter=*/nullptr,
                          /*verify_checksums=*/true);
    BlogFileHeader header;
    Status s = reader.ReadHeader(&header);
    if (!s.ok()) {
      return s;
    }
    BlogRecordType type;
    Slice record;
    std::string scratch;
    while ((s = reader.ReadRecord(&type, &record, &scratch)).ok()) {
      types->push_back(type);
    }
    return s;
  }

  // Escape-sequence offset of the first compact-format record in the body.
  void FirstCompactRecordOffset(size_t* escape_offset) {
    Slice header_input(contents_);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    size_t pos = contents_.size() - header_input.size();
    while (pos < contents_.size() &&
           (contents_[pos] == '\0' || contents_[pos] == '\xff')) {
      ++pos;
    }
    while (true) {
      ASSERT_LT(pos + kBlogEscapeSequenceSize, contents_.size());
      ASSERT_EQ(pos % kBlogMinRecordAlignment, 0u);
      ASSERT_EQ(memcmp(contents_.data() + pos, header.escape_sequence,
                       kBlogEscapeSequenceSize),
                0);
      const size_t varint_offset = pos + kBlogEscapeSequenceSize;
      uint64_t length = 0;
      const char* const varint_end =
          GetPrefixVarint64Ptr(contents_.data() + varint_offset,
                               contents_.data() + contents_.size(), &length);
      ASSERT_TRUE(varint_end != nullptr);
      const size_t varint_size =
          static_cast<size_t>(varint_end - (contents_.data() + varint_offset));
      if (varint_size <= kBlogCompactVarintMaxBytes) {
        *escape_offset = pos;
        return;
      }
      ASSERT_NE(length, kBlogUnspecifiedSize);
      const size_t end = varint_offset + varint_size +
                         /*type+compression+prefix checksum*/ 6 +
                         static_cast<size_t>(length) + kBlogBlockTrailerSize;
      ASSERT_LT(end, contents_.size());
      const uint8_t last_byte = static_cast<uint8_t>(contents_[end - 1]);
      size_t padding =
          (kBlogMinRecordAlignment - end % kBlogMinRecordAlignment) %
          kBlogMinRecordAlignment;
      if (padding == 0 && (last_byte == 0x00 || last_byte == 0xFF)) {
        padding = kBlogMinRecordAlignment;
      }
      pos = end + padding;
    }
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

  // Decodes streaming-compressed record payloads as one ZSTD stream, in
  // vector order, into *decoded. Each fragment must be fully consumed before
  // the next is fed.
  void DecompressStreamedPayloads(const std::vector<std::string>& fragments,
                                  std::string* decoded) {
    std::unique_ptr<StreamingUncompress> uncompress =
        StreamingUncompress::Create(kZSTD, /*compress_format_version=*/2,
                                    /*max_output_len=*/1 << 20);
    ASSERT_TRUE(uncompress != nullptr);
    decoded->clear();
    std::string out(1 << 20, '\0');
    for (const std::string& fragment : fragments) {
      const char* input = fragment.data();
      int ret = 0;
      size_t output_pos = 0;
      do {
        // input_size is ignored once input is null, but must stay non-zero.
        ret = uncompress->Uncompress(input, fragment.size(), out.data(),
                                     &output_pos);
        ASSERT_GE(ret, 0);
        decoded->append(out.data(), output_pos);
        input = nullptr;
      } while (ret > 0 || output_pos == out.size());
    }
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

  // Decodes min/maxSequence from a footer properties record payload.
  void DecodeFooterSequenceRange(const Slice& record, uint64_t* min_sequence,
                                 uint64_t* max_sequence, bool* found_min,
                                 bool* found_max) {
    BlogFileFooterProperties properties;
    ASSERT_OK(properties.DecodeFrom(record));
    *found_min = false;
    *found_max = false;
    for (const auto& [name, value] : properties.properties) {
      Slice encoded(value);
      if (name == "minSequence") {
        ASSERT_TRUE(GetPrefixVarint64(&encoded, min_sequence));
        ASSERT_TRUE(encoded.empty());
        *found_min = true;
      } else if (name == "maxSequence") {
        ASSERT_TRUE(GetPrefixVarint64(&encoded, max_sequence));
        ASSERT_TRUE(encoded.empty());
        *found_max = true;
      }
    }
  }

  // Scans the file for the footer properties record, decoding min/maxSequence
  // when present.
  void ScanFooterSequenceRange(uint64_t* min_sequence, uint64_t* max_sequence,
                               bool* found_min, bool* found_max,
                               bool* found_footer) {
    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader header;
    ASSERT_OK(reader->ReadHeader(&header));
    BlogRecordType type;
    Slice record;
    std::string scratch;
    *found_footer = false;
    Status s;
    while ((s = reader->ReadRecord(&type, &record, &scratch)).ok()) {
      if (type == kBlogFooterPropertiesRecord) {
        DecodeFooterSequenceRange(record, min_sequence, max_sequence, found_min,
                                  found_max);
        *found_footer = true;
      }
    }
    ASSERT_TRUE(s.IsNotFound());
  }

  void AssertWalFooter(BlogFileReader* reader) {
    BlogRecordType type;
    Slice payload;
    std::string scratch;
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    ASSERT_EQ(type, kBlogFooterPropertiesRecord);
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    ASSERT_EQ(type, kBlogFooterLocatorRecord);
    ASSERT_TRUE(reader->ReadRecord(&type, &payload, &scratch).IsNotFound());
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

TEST_F(BlogWriterTest, WalWriterInterface) {
  std::unique_ptr<BlogWalWriter> writer_holder =
      MakeWalWriter(/*is_recycled=*/true, /*blog_timestamp_size=*/8);
  BlogWalWriter& writer = *writer_holder;

  const WriteOptions write_options;
  const PredecessorWALInfo predecessor(/*log_number=*/122,
                                       /*size_bytes=*/1000,
                                       /*last_seqno_recorded=*/40);
  ASSERT_OK(writer.WriteFileStart(write_options, predecessor));
  ASSERT_OK(writer.MaybeAddUserDefinedTimestampSizeRecord(
      write_options, {{/*cf_id=*/1, /*timestamp_size=*/8}}));
  ASSERT_OK(writer.AddRecord(write_options, "write batch", /*sequence=*/41,
                             /*num_sequences=*/1));
  ASSERT_EQ(writer.GetLastSeqnoRecorded(), 41);

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader header;
  ASSERT_OK(reader->ReadHeader(&header));
  ASSERT_EQ(header.compact_record_type, kBlogWriteBatchRecord);
  ASSERT_TRUE(header.is_recycled());
  std::string encoded_predecessor;
  predecessor.EncodeTo(&encoded_predecessor);
  ASSERT_EQ(header.GetProperty(kBlogPropPredecessorWalInfo),
            encoded_predecessor);
  uint64_t timestamp_size = 0;
  ASSERT_TRUE(
      header.GetUint64Property(kBlogPropTimestampSize, &timestamp_size));
  ASSERT_EQ(timestamp_size, 8);

  BlogRecordType type;
  Slice record;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
  ASSERT_EQ(type, kBlogUserDefinedTimestampSizeRecord);
  UserDefinedTimestampSizeRecord timestamp_record;
  ASSERT_OK(timestamp_record.DecodeFrom(&record));
  ASSERT_EQ(timestamp_record.GetUserDefinedTimestampSize(),
            (std::vector<std::pair<uint32_t, size_t>>{{1, 8}}));

  ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(record, "write batch");
}

TEST_F(BlogWriterTest, WalWriterHeaderTimestampSizeComesFromOption) {
  for (size_t blog_timestamp_size : {size_t{0}, size_t{8}}) {
    SCOPED_TRACE("blog_timestamp_size=" + std::to_string(blog_timestamp_size));
    std::unique_ptr<BlogWalWriter> writer =
        MakeWalWriter(/*is_recycled=*/true, blog_timestamp_size);
    const WriteOptions write_options;
    ASSERT_OK(writer->WriteFileStart(write_options, PredecessorWALInfo()));
    ASSERT_OK(writer->MaybeAddUserDefinedTimestampSizeRecord(
        write_options, {{/*cf_id=*/1, /*timestamp_size=*/16}}));

    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader header;
    ASSERT_OK(reader->ReadHeader(&header));
    uint64_t header_timestamp_size = 0;
    ASSERT_EQ(header.GetUint64Property(kBlogPropTimestampSize,
                                       &header_timestamp_size),
              blog_timestamp_size != 0);
    ASSERT_EQ(header_timestamp_size, blog_timestamp_size);

    BlogRecordType type;
    Slice record;
    std::string scratch;
    ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
    ASSERT_EQ(type, kBlogUserDefinedTimestampSizeRecord);
  }
}

TEST_F(BlogWriterTest, WalWriterCloseWritesSequenceFooter) {
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriter();
  const WriteOptions write_options;
  ASSERT_OK(writer->WriteFileStart(write_options, PredecessorWALInfo()));
  ASSERT_OK(writer->AddRecord(write_options, "batch-one", /*sequence=*/5,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->AddRecord(write_options, "batch-two", /*sequence=*/9,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->Close(write_options));
  ASSERT_OK(writer->Close(write_options));
  ASSERT_TRUE(writer
                  ->AddRecord(write_options, "too-late", /*sequence=*/10,
                              /*num_sequences=*/1)
                  .IsInvalidArgument());

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader header;
  ASSERT_OK(reader->ReadHeader(&header));
  BlogRecordType type;
  Slice record;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(record, "batch-one");
  ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(record, "batch-two");
  ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
  ASSERT_EQ(type, kBlogFooterPropertiesRecord);

  uint64_t min_sequence = 0;
  uint64_t max_sequence = 0;
  bool found_min = false;
  bool found_max = false;
  DecodeFooterSequenceRange(record, &min_sequence, &max_sequence, &found_min,
                            &found_max);
  ASSERT_TRUE(found_min);
  ASSERT_TRUE(found_max);
  ASSERT_EQ(min_sequence, 5);
  ASSERT_EQ(max_sequence, 9);

  ASSERT_OK(reader->ReadRecord(&type, &record, &scratch));
  ASSERT_EQ(type, kBlogFooterLocatorRecord);
  ASSERT_TRUE(reader->ReadRecord(&type, &record, &scratch).IsNotFound());
}

TEST_F(BlogWriterTest, WalWriterCloseWithBufferedUnsyncedData) {
  // Exercise close with buffered writes through the fault-injection wrapper.
  Env* env = Env::Default();
  const std::string dir = test::PerThreadDBPath("blog_wal_seal_buffered");
  ASSERT_OK(env->CreateDirIfMissing(dir));
  const std::string fname = dir + "/wal.log";
  env->DeleteFile(fname).PermitUncheckedError();

  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(env->GetFileSystem());
  fault_fs->SetFilesystemDirectWritable(false);
  fault_fs->SetInjectUnsyncedDataLoss(false);

  const WriteOptions write_options;
  uint64_t logical_size = 0;
  {
    std::unique_ptr<FSWritableFile> sink;
    ASSERT_OK(fault_fs->NewWritableFile(fname, FileOptions(), &sink,
                                        /*dbg=*/nullptr));
    std::unique_ptr<BlogWalWriter> writer =
        MakeFileWalWriter(std::move(sink), fname, /*is_recycled=*/false);
    ASSERT_OK(writer->WriteFileStart(write_options, PredecessorWALInfo()));
    ASSERT_OK(writer->AddRecord(write_options, "batch-one", /*sequence=*/5,
                                /*num_sequences=*/1));
    ASSERT_OK(writer->AddRecord(write_options, "batch-two", /*sequence=*/9,
                                /*num_sequences=*/1));
    ASSERT_OK(writer->Close(write_options));
    ASSERT_OK(
        fault_fs->GetFileSize(fname, IOOptions(), &logical_size, nullptr));
  }

  uint64_t real_size = 0;
  ASSERT_OK(env->GetFileSystem()->GetFileSize(fname, IOOptions(), &real_size,
                                              /*dbg=*/nullptr));
  ASSERT_EQ(real_size, logical_size);

  std::vector<BlogRecordType> types;
  Status s = ReadAllRecordTypes(env->GetFileSystem().get(), fname, &types);
  ASSERT_TRUE(s.IsNotFound()) << s.ToString();
  ASSERT_EQ(types,
            std::vector<BlogRecordType>(
                {kBlogWriteBatchRecord, kBlogWriteBatchRecord,
                 kBlogFooterPropertiesRecord, kBlogFooterLocatorRecord}));
  ASSERT_OK(env->DeleteFile(fname));
}

TEST_F(BlogWriterTest, WalWriterCloseTrimsRecycledTail) {
  Env* env = Env::Default();
  std::shared_ptr<FileSystem> fs = env->GetFileSystem();
  const std::string dir = test::PerThreadDBPath("blog_wal_seal_recycled");
  ASSERT_OK(env->CreateDirIfMissing(dir));
  const std::string old_fname = dir + "/old.log";
  const std::string fname = dir + "/wal.log";
  env->DeleteFile(old_fname).PermitUncheckedError();
  env->DeleteFile(fname).PermitUncheckedError();

  for (bool direct_io : {false, true}) {
    SCOPED_TRACE(direct_io);
    FileOptions file_options;
    file_options.use_direct_writes = direct_io;
    if (direct_io && !test::IsDirectIOSupported(env, dir)) {
      continue;
    }
    const std::string stale_contents(65536, '\xAB');
    {
      std::unique_ptr<FSWritableFile> stale;
      ASSERT_OK(fs->NewWritableFile(old_fname, FileOptions(), &stale,
                                    /*dbg=*/nullptr));
      ASSERT_OK(
          stale->Append(Slice(stale_contents), IOOptions(), /*dbg=*/nullptr));
      ASSERT_OK(stale->Close(IOOptions(), /*dbg=*/nullptr));
    }

    const WriteOptions write_options;
    uint64_t logical_size = 0;
    {
      std::unique_ptr<FSWritableFile> reused;
      ASSERT_OK(fs->ReuseWritableFile(fname, old_fname, file_options, &reused,
                                      /*dbg=*/nullptr));
      std::unique_ptr<BlogWalWriter> writer =
          MakeFileWalWriter(std::move(reused), fname, /*is_recycled=*/true);
      ASSERT_OK(writer->WriteFileStart(write_options, PredecessorWALInfo()));
      ASSERT_OK(writer->AddRecord(write_options, "batch-one", /*sequence=*/5,
                                  /*num_sequences=*/1));
      ASSERT_OK(writer->Close(write_options));
      ASSERT_OK(fs->GetFileSize(fname, IOOptions(), &logical_size, nullptr));
    }

    uint64_t real_size = 0;
    ASSERT_OK(fs->GetFileSize(fname, IOOptions(), &real_size, /*dbg=*/nullptr));
    ASSERT_EQ(real_size, logical_size);
    ASSERT_LT(real_size, static_cast<uint64_t>(65536));

    std::vector<BlogRecordType> types;
    Status s = ReadAllRecordTypes(fs.get(), fname, &types);
    ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    ASSERT_EQ(types, std::vector<BlogRecordType>({kBlogWriteBatchRecord,
                                                  kBlogFooterPropertiesRecord,
                                                  kBlogFooterLocatorRecord}));
    ASSERT_OK(env->DeleteFile(fname));
  }
}

TEST_F(BlogWriterTest, WalWriterKeepsHighestSequence) {
  // Keep the max like log::Writer so no false WAL hole is reported.
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriter();

  const WriteOptions write_options;
  ASSERT_OK(writer->AddRecord(write_options, "batch1", /*sequence=*/41,
                              /*num_sequences=*/1));
  ASSERT_EQ(writer->GetLastSeqnoRecorded(), 41);
  ASSERT_OK(writer->AddRecord(write_options, "batch2", /*sequence=*/12,
                              /*num_sequences=*/1));
  ASSERT_EQ(writer->GetLastSeqnoRecorded(), 41);
  ASSERT_OK(writer->AddRecord(write_options, "batch3", /*sequence=*/57,
                              /*num_sequences=*/1));
  ASSERT_EQ(writer->GetLastSeqnoRecorded(), 57);
}

TEST_F(BlogWriterTest, WalWriterCloseTracksConsumedSequenceRange) {
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriter();
  const WriteOptions write_options;
  ASSERT_OK(writer->WriteFileStart(write_options, PredecessorWALInfo()));
  // Ranges [10, 14], [20, 21], [15, 15]. The footer max must be the largest
  // last sequence (21), not the largest first sequence (20).
  ASSERT_OK(writer->AddRecord(write_options, "batch-one", /*sequence=*/10,
                              /*num_sequences=*/5));
  ASSERT_OK(writer->AddRecord(write_options, "batch-two", /*sequence=*/20,
                              /*num_sequences=*/2));
  ASSERT_OK(writer->AddRecord(write_options, "batch-three", /*sequence=*/15,
                              /*num_sequences=*/1));
  // GetLastSeqnoRecorded keeps first-sequence max semantics for predecessor
  // verification, which recovery observes from batch headers.
  ASSERT_EQ(writer->GetLastSeqnoRecorded(), 20);
  ASSERT_OK(writer->Close(write_options));

  uint64_t min_sequence = 0;
  uint64_t max_sequence = 0;
  bool found_min = false;
  bool found_max = false;
  bool found_footer = false;
  ScanFooterSequenceRange(&min_sequence, &max_sequence, &found_min, &found_max,
                          &found_footer);
  ASSERT_TRUE(found_footer);
  ASSERT_TRUE(found_min);
  ASSERT_TRUE(found_max);
  ASSERT_EQ(min_sequence, 10);
  ASSERT_EQ(max_sequence, 21);
}

TEST_F(BlogWriterTest, WalWriterEmptyRecordContributesNoSequences) {
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriter();
  const WriteOptions write_options;
  ASSERT_OK(writer->WriteFileStart(write_options, PredecessorWALInfo()));
  // An empty record (e.g. the post-recovery dummy) consumes no sequences.
  ASSERT_OK(writer->AddRecord(write_options, "empty", /*sequence=*/7,
                              /*num_sequences=*/0));
  // ... but predecessor tracking still sees its first sequence, matching
  // what recovery observes from the batch header.
  ASSERT_EQ(writer->GetLastSeqnoRecorded(), 7);
  ASSERT_OK(writer->AddRecord(write_options, "batch", /*sequence=*/10,
                              /*num_sequences=*/2));
  ASSERT_OK(writer->Close(write_options));

  uint64_t min_sequence = 0;
  uint64_t max_sequence = 0;
  bool found_min = false;
  bool found_max = false;
  bool found_footer = false;
  ScanFooterSequenceRange(&min_sequence, &max_sequence, &found_min, &found_max,
                          &found_footer);
  ASSERT_TRUE(found_footer);
  ASSERT_TRUE(found_min);
  ASSERT_TRUE(found_max);
  ASSERT_EQ(min_sequence, 10);
  ASSERT_EQ(max_sequence, 11);

  // A file with only empty records seals without a sequence range.
  std::unique_ptr<BlogWalWriter> empty_writer = MakeWalWriter();
  ASSERT_OK(empty_writer->WriteFileStart(write_options, PredecessorWALInfo()));
  ASSERT_OK(empty_writer->AddRecord(write_options, "empty", /*sequence=*/7,
                                    /*num_sequences=*/0));
  ASSERT_OK(empty_writer->Close(write_options));
  ScanFooterSequenceRange(&min_sequence, &max_sequence, &found_min, &found_max,
                          &found_footer);
  ASSERT_TRUE(found_footer);
  ASSERT_FALSE(found_min);
  ASSERT_FALSE(found_max);
}

TEST_F(BlogWriterTest, WalWriterCloseBeforeFirstRecord) {
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriter();
  ASSERT_OK(writer->Close(WriteOptions()));
  ASSERT_EQ(writer->file(), nullptr);
  ASSERT_TRUE(writer->PublishIfClosed());
  ASSERT_TRUE(writer->BufferIsEmpty());
}

TEST_F(BlogWriterTest, WalWriterClosePropagatesFileError) {
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriterWithFailingClose();
  IOStatus s = writer->Close(WriteOptions());
  ASSERT_TRUE(!s.ok());
  // Like log::Writer: released even on error; repeat Close is a no-op.
  ASSERT_EQ(writer->file(), nullptr);
  ASSERT_TRUE(writer->PublishIfClosed());
  ASSERT_TRUE(writer->BufferIsEmpty());
  ASSERT_OK(writer->Close(WriteOptions()));
}

TEST_F(BlogWriterTest, WalWriterCloseAfterRecordPropagatesFileError) {
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriterWithFailingClose();
  ASSERT_OK(writer->AddRecord(WriteOptions(), "batch", /*sequence=*/41,
                              /*num_sequences=*/1));
  IOStatus s = writer->Close(WriteOptions());
  ASSERT_TRUE(!s.ok());
  ASSERT_EQ(writer->file(), nullptr);
  ASSERT_TRUE(writer->PublishIfClosed());
  ASSERT_TRUE(writer->BufferIsEmpty());
  ASSERT_OK(writer->Close(WriteOptions()));
}

TEST_F(BlogWriterTest, WalWriterClosePreservesFirstErrorAndClosesOnce) {
  const char* expected_errors[] = {"injected footer append error",
                                   "injected footer truncate error",
                                   "injected file close error"};
  for (int failure = 0; failure < 3; ++failure) {
    SCOPED_TRACE(failure);
    contents_.clear();
    CloseState state;
    std::unique_ptr<BlogWalWriter> writer = MakeWalWriterWithSink(
        std::make_unique<CloseTrackingSink>(&contents_, &state));
    ASSERT_OK(writer->AddRecord(WriteOptions(), "batch", 5, 1));
    state.fail_append = failure == 0;
    state.fail_truncate = failure == 1;
    state.fail_close = true;
    IOStatus s = writer->Close(WriteOptions());
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_NE(s.ToString().find(expected_errors[failure]), std::string::npos);
    ASSERT_EQ(state.closes, 1);
    ASSERT_EQ(state.syncs, 0);
    ASSERT_EQ(writer->file(), nullptr);
    const std::string closed_contents = contents_;
    ASSERT_OK(writer->Close(WriteOptions()));
    writer.reset();
    ASSERT_EQ(state.closes, 1);
    ASSERT_EQ(contents_, closed_contents);
  }
}

TEST_F(BlogWriterTest, WalWriterDirectCloseOmitsFooter) {
  contents_.clear();
  CloseState state;
  std::unique_ptr<BlogWalWriter> writer = MakeWalWriterWithSink(
      std::make_unique<CloseTrackingSink>(&contents_, &state));
  ASSERT_OK(writer->AddRecord(WriteOptions(), "batch", 5, 1));
  ASSERT_OK(writer->file()->Close(IOOptions(), nullptr));
  const std::string closed_contents = contents_;
  ASSERT_OK(writer->Close(WriteOptions()));
  writer.reset();
  ASSERT_EQ(state.closes, 1);
  ASSERT_EQ(contents_, closed_contents);
}

TEST_F(BlogWriterTest, WalWriterCloseKeepsVerificationSize) {
  for (bool manual_flush : {false, true}) {
    SCOPED_TRACE(manual_flush);
    contents_.clear();
    CloseState state;
    std::unique_ptr<WritableFileWriter> dest =
        std::make_unique<WritableFileWriter>(
            std::make_unique<CloseTrackingSink>(&contents_, &state), "",
            FileOptions());
    BlogWalWriter writer(std::move(dest), 123, false, manual_flush,
                         kNoCompression, false, true, kXXH3, 0, "db-id",
                         "session-id", "host-id", 456);
    ASSERT_OK(writer.AddRecord(WriteOptions(), "batch", 5, 1));
    const uint64_t batch_end = writer.GetVerificationSize();
    ASSERT_OK(writer.MaybeAddUserDefinedTimestampSizeRecord(WriteOptions(),
                                                            {{1, 8}}));
    const uint64_t body_end = writer.GetVerificationSize();
    ASSERT_GT(body_end, batch_end);
    ASSERT_OK(writer.Close(WriteOptions()));
    ASSERT_EQ(writer.GetVerificationSize(), body_end);
    ASSERT_EQ(state.syncs, 0);

    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader header;
    ASSERT_OK(reader->ReadHeader(&header));
    BlogRecordType type;
    Slice payload;
    std::string scratch;
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    ASSERT_EQ(type, kBlogWriteBatchRecord);
    ASSERT_EQ(reader->current_offset(), batch_end);
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    ASSERT_EQ(type, kBlogUserDefinedTimestampSizeRecord);
    ASSERT_EQ(reader->current_offset(), body_end);
    AssertWalFooter(reader.get());
  }
}

TEST_F(BlogWriterTest, BodyEndExcludesPaddingAndFooter) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.extra_padding_4k_one_in = 1;
  TEST_BlogNoncanonicalConfigScope scope(config);
  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader read_header;
  ASSERT_OK(reader->ReadHeader(&read_header));
  ASSERT_EQ(writer->body_end_offset(), reader->current_offset());
  ASSERT_LT(writer->body_end_offset(), writer->current_offset());
  ASSERT_OK(writer->AddPreambleStartRecord(WriteOptions()));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogPreambleStartRecord);
  const uint64_t body_end = writer->body_end_offset();
  ASSERT_EQ(body_end, reader->current_offset());
  ASSERT_LT(body_end, writer->current_offset());
  ASSERT_OK(writer->WriteFooter(WriteOptions(), BlogFileFooterProperties{}));
  ASSERT_EQ(writer->body_end_offset(), body_end);
}

TEST_F(BlogWriterTest, WalWriterRecordAfterLostRecordReadsBack) {
  contents_.clear();
  auto* sink = new FailingAppendSink(&contents_, -1);
  std::unique_ptr<BlogWalWriter> writer =
      MakeWalWriterWithSink(std::unique_ptr<FSWritableFile>(sink));
  const WriteOptions wo;
  ASSERT_OK(writer->AddRecord(wo, "batch-one", /*sequence=*/5,
                              /*num_sequences=*/1));
  // Fail the flush of the next record; its buffered bytes are dropped.
  sink->set_fail_on(sink->append_count() + 1);
  IOStatus s = writer->AddRecord(wo, "batch-two", /*sequence=*/6,
                                 /*num_sequences=*/1);
  ASSERT_TRUE(!s.ok()) << s.ToString();
  // Like DBImpl without paranoid_checks: clear the error and keep appending.
  writer->file()->reset_seen_error();
  sink->set_fail_on(-1);
  ASSERT_OK(writer->AddRecord(wo, "batch-three", /*sequence=*/7,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->Close(wo));

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader header;
  ASSERT_OK(reader->ReadHeader(&header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(payload, "batch-one");
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(payload, "batch-three");
  AssertWalFooter(reader.get());
}

TEST_F(BlogWriterTest,
       WalWriterPreservesTimestampSizeRecordAfterLaterWriteError) {
  contents_.clear();
  auto* sink = new FailingAppendSink(&contents_, -1);
  std::unique_ptr<BlogWalWriter> writer =
      MakeWalWriterWithSink(std::unique_ptr<FSWritableFile>(sink));
  const WriteOptions wo;
  const UnorderedMap<uint32_t, size_t> ts_sizes = {{7, 8}};
  ASSERT_OK(writer->AddRecord(wo, "batch-one", /*sequence=*/1,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->MaybeAddUserDefinedTimestampSizeRecord(wo, ts_sizes));
  sink->set_fail_on(sink->append_count() + 1);
  ASSERT_NOK(writer->AddRecord(wo, "batch-two", /*sequence=*/2,
                               /*num_sequences=*/1));
  writer->file()->reset_seen_error();
  sink->set_fail_on(-1);
  ASSERT_OK(writer->MaybeAddUserDefinedTimestampSizeRecord(wo, ts_sizes));
  ASSERT_OK(writer->AddRecord(wo, "batch-three", /*sequence=*/3,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->Close(wo));

  std::string timestamp_record;
  UserDefinedTimestampSizeRecord(
      std::vector<std::pair<uint32_t, size_t>>{{7, 8}})
      .EncodeTo(&timestamp_record);
  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader header;
  ASSERT_OK(reader->ReadHeader(&header));
  std::vector<std::string> records;
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  while (true) {
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
    if (IsBlogFooterRecordType(type)) {
      break;
    }
    if (type == kBlogUserDefinedTimestampSizeRecord) {
      ASSERT_EQ(payload, timestamp_record);
      records.emplace_back("timestamp-size");
    } else {
      ASSERT_EQ(type, kBlogWriteBatchRecord);
      records.push_back(payload.ToString());
    }
  }
  ASSERT_EQ(records, (std::vector<std::string>{"batch-one", "timestamp-size",
                                               "batch-three"}));
}

TEST_F(BlogWriterTest, RealignsAfterTornRecordWithoutLostWrites) {
  // A record that fails after part of it was buffered loses nothing. Like a
  // caller without paranoid_checks, clear the error and keep appending: the
  // next record must still start aligned.
  BlogFileHeader header = MakeWalHeader();
  std::unique_ptr<BlogFileWriter> writer = MakeWriter(header);
  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));
  ASSERT_OK(writer->AddWriteBatchRecord(wo, "before"));

  // Fail the payload append after the prefix was buffered, like a concurrent
  // sync failure. Small records reach the file in a single append, so the
  // payload must be large enough to be written separately from its prefix,
  // and large enough for the full record format, whose prefix ends unaligned.
  WritableFileWriter* file = writer->file();
  bool injected = false;
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::Append:BeforePrepareWrite", [&](void* /*arg*/) {
        if (!injected) {
          injected = true;
          file->set_seen_error(IOStatus::IOError("injected sync failure"));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  IOStatus s = writer->AddWriteBatchRecord(wo, std::string(20000, 't'));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(injected);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  const uint64_t torn_end = writer->current_offset();
  ASSERT_NE(torn_end % kBlogMinRecordAlignment, 0u);

  file->reset_seen_error();
  ASSERT_OK(writer->AddWriteBatchRecord(wo, "after"));
  ASSERT_OK(writer->Sync(wo));
  const uint64_t after_start = (torn_end + kBlogMinRecordAlignment - 1) &
                               ~uint64_t{kBlogMinRecordAlignment - 1};
  ASSERT_LT(after_start + kBlogEscapeSequenceSize, contents_.size());
  ASSERT_EQ(memcmp(contents_.data() + after_start, header.escape_sequence,
                   kBlogEscapeSequenceSize),
            0);
}

TEST_F(BlogWriterTest, WalWriterRealignsAfterLostRecordTail) {
  const std::string after_loss = "after-loss";
  for (int case_idx = 1; case_idx <= 2; ++case_idx) {
    SCOPED_TRACE("case=" + std::to_string(case_idx));
    contents_.clear();
    auto* sink = new FailingAppendSink(&contents_, -1);
    std::unique_ptr<BlogWalWriter> writer = MakeWalWriterWithSinkAndBufferSize(
        std::unique_ptr<FSWritableFile>(sink), 1024 * 1024);
    const WriteOptions wo;
    ASSERT_OK(writer->WriteFileStart(wo, PredecessorWALInfo()));
    const int appends_after_header = sink->append_count();

    // A 2 MiB payload exceeds the 1 MiB buffer, so the record uses three
    // underlying appends: buffer fill, direct payload remainder, trailer.
    const std::string large(2 * 1024 * 1024 + 1, 'L');
    sink->set_fail_on(appends_after_header + (case_idx == 1 ? 3 : 2));
    IOStatus s = writer->AddRecord(wo, large, /*sequence=*/5,
                                   /*num_sequences=*/1);
    ASSERT_TRUE(!s.ok()) << s.ToString();
    const uint64_t size_after_loss = contents_.size();
    ASSERT_EQ(writer->file()->GetFlushedSize(), size_after_loss);
    if (case_idx == 1) {
      // Only the trailer flush failed; the file ends mid-record, unaligned.
      ASSERT_GT(size_after_loss, 1024 * 1024u);
      ASSERT_NE(size_after_loss % kBlogMinRecordAlignment, 0u);
    } else {
      // The direct payload write failed after its buffer-filling part flushed.
      ASSERT_GT(size_after_loss, 60000u);
      ASSERT_LT(size_after_loss, 1024 * 1024u);
    }
    const uint64_t expected_next_start =
        (size_after_loss + kBlogMinRecordAlignment - 1) &
        ~uint64_t{kBlogMinRecordAlignment - 1};

    writer->file()->reset_seen_error();
    sink->set_fail_on(-1);
    ASSERT_OK(writer->AddRecord(wo, after_loss, /*sequence=*/6,
                                /*num_sequences=*/1));
    ASSERT_OK(writer->Close(wo));

    for (uint64_t i = size_after_loss; i < expected_next_start; ++i) {
      ASSERT_EQ(contents_[i], '\0') << "i=" << i;
    }
    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader header;
    ASSERT_OK(reader->ReadHeader(&header));
    ASSERT_LT(expected_next_start + kBlogEscapeSequenceSize, contents_.size());
    ASSERT_EQ(memcmp(contents_.data() + expected_next_start,
                     header.escape_sequence, kBlogEscapeSequenceSize),
              0);

    const char* base = contents_.data();
    const char* limit = base + contents_.size();
    uint64_t length = 0;
    const char* varint_end = GetPrefixVarint64Ptr(
        base + expected_next_start + kBlogEscapeSequenceSize, limit, &length);
    ASSERT_TRUE(varint_end != nullptr);
    ASSERT_EQ(length, after_loss.size());
    const uint64_t payload_offset =
        expected_next_start + kBlogEscapeSequenceSize +
        (varint_end - (base + expected_next_start + kBlogEscapeSequenceSize));
    ASSERT_EQ(Slice(base + payload_offset, length), after_loss);
    const uint64_t trailer_offset = payload_offset + length;
    ASSERT_LT(trailer_offset + kBlogBlockTrailerSize, contents_.size());
    const char comp_byte = base[trailer_offset];
    const uint32_t stored = DecodeFixed32(base + trailer_offset + 1);
    const uint32_t computed = ComputeBlogRecordChecksum(
        header.checksum_type, base + payload_offset, length, comp_byte,
        header.incarnation_id(), payload_offset);
    ASSERT_EQ(stored, computed);
  }
}

TEST_F(BlogWriterTest, WalWriterStreamingCompressionSpansRecords) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_SKIP("Test requires ZSTD streaming support");
    return;
  }
  std::unique_ptr<BlogWalWriter> writer = MakeStreamingWalWriter(kZSTD);
  const WriteOptions wo;
  ASSERT_OK(writer->WriteFileStart(wo, PredecessorWALInfo()));

  Random rnd(301);
  std::string incompressible(4096, '\0');
  for (size_t i = 0; i < incompressible.size(); ++i) {
    incompressible[i] = static_cast<char>(rnd.Next());
  }
  ASSERT_OK(writer->AddRecord(wo, "first-batch", /*sequence=*/5,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->AddRecord(wo, "", /*sequence=*/6, /*num_sequences=*/0));
  ASSERT_OK(writer->AddRecord(wo, incompressible, /*sequence=*/7,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->AddRecord(wo, incompressible, /*sequence=*/8,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->Close(wo));

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader header;
  ASSERT_OK(reader->ReadHeader(&header));
  ASSERT_EQ(header.GetProperty(kBlogPropCompressionCompatibilityName),
            "BuiltinV2");
  ASSERT_EQ(header.GetProperty(kBlogPropWriteBatchStreamingCompressionType),
            "07");

  std::vector<std::string> stored;
  std::vector<CompressionType> trailer_types;
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  CompressionType trailer_type = kNoCompression;
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(
        reader->ReadRecord(&type, &payload, &scratch, nullptr, &trailer_type));
    ASSERT_EQ(type, kBlogWriteBatchRecord);
    stored.emplace_back(payload.data(), payload.size());
    trailer_types.push_back(trailer_type);
  }
  AssertWalFooter(reader.get());
  ASSERT_EQ(trailer_types, std::vector<CompressionType>(
                               {kStreamingCompressionSentinel, kNoCompression,
                                kStreamingCompressionSentinel,
                                kStreamingCompressionSentinel}));
  // The first copy is incompressible on its own, so it stores large; the
  // repeat references it and stores tiny. Per-record compression would store
  // both copies raw.
  ASSERT_GT(stored[2].size(), 4000u);
  ASSERT_LT(stored[3].size(), 64u);

  std::string decoded;
  DecompressStreamedPayloads({stored[0], stored[2], stored[3]}, &decoded);
  ASSERT_EQ(decoded, "first-batch" + incompressible + incompressible);
}

TEST_F(BlogWriterTest, WalWriterStreamingCompressionRequiresStreamingType) {
  std::unique_ptr<BlogWalWriter> writer =
      MakeStreamingWalWriter(kSnappyCompression);
  IOStatus s = writer->WriteFileStart(WriteOptions(), PredecessorWALInfo());
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
}

TEST_F(BlogWriterTest, WalWriterStreamingCompressionFallsBackAfterWriteError) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_SKIP("Test requires ZSTD streaming support");
    return;
  }
  contents_.clear();
  auto* sink = new FailingAppendSink(&contents_, -1);
  std::unique_ptr<BlogWalWriter> writer = MakeStreamingWalWriterWithSink(
      std::unique_ptr<FSWritableFile>(sink), kZSTD, /*is_recycled=*/true);
  const WriteOptions wo;
  ASSERT_OK(writer->AddRecord(wo, "batch-one", /*sequence=*/5,
                              /*num_sequences=*/1));
  // Fail the flush of the next streamed record; its bytes are dropped.
  sink->set_fail_on(sink->append_count() + 1);
  IOStatus s = writer->AddRecord(wo, "batch-two", /*sequence=*/6,
                                 /*num_sequences=*/1);
  ASSERT_TRUE(!s.ok()) << s.ToString();
  // Like DBImpl without paranoid_checks: clear the error and keep appending.
  // Later records fall back to raw since the stream lost history.
  writer->file()->reset_seen_error();
  sink->set_fail_on(-1);
  ASSERT_OK(writer->AddRecord(wo, "batch-three", /*sequence=*/7,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->Close(wo));

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader header;
  ASSERT_OK(reader->ReadHeader(&header));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  CompressionType trailer_type = kNoCompression;
  ASSERT_OK(
      reader->ReadRecord(&type, &payload, &scratch, nullptr, &trailer_type));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(trailer_type, kStreamingCompressionSentinel);
  const std::string streamed(payload.data(), payload.size());
  ASSERT_OK(
      reader->ReadRecord(&type, &payload, &scratch, nullptr, &trailer_type));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(trailer_type, kNoCompression);
  ASSERT_EQ(payload, "batch-three");
  AssertWalFooter(reader.get());

  std::string decoded;
  DecompressStreamedPayloads({streamed}, &decoded);
  ASSERT_EQ(decoded, "batch-one");
}

TEST_F(BlogWriterTest, WalWriterStreamingCompressionFallsBackAfterSyncError) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_SKIP("Test requires ZSTD streaming support");
    return;
  }
  for (size_t record_size : {size_t{4096}, size_t{32768}}) {
    SCOPED_TRACE(record_size);
    contents_.clear();
    auto* sink = new FailingAppendSink(&contents_, -1);
    std::unique_ptr<WritableFileWriter> dest =
        std::make_unique<WritableFileWriter>(
            std::unique_ptr<FSWritableFile>(sink), "", FileOptions());
    BlogWalWriter writer(std::move(dest), 123, false, /*manual_flush=*/true,
                         kZSTD, /*use_streaming_compression=*/true, true, kXXH3,
                         0, "db-id", "session-id", "host-id", 456);
    const WriteOptions wo;
    ASSERT_OK(writer.WriteFileStart(wo, PredecessorWALInfo()));
    const size_t header_size = contents_.size();
    const std::string record(record_size, 'A');
    ASSERT_OK(writer.AddRecord(wo, record, /*sequence=*/5,
                               /*num_sequences=*/1));
    sink->set_fail_on(sink->append_count() + 1);
    ASSERT_NOK(writer.file()->Sync(IOOptions()));
    ASSERT_EQ(contents_.size(), header_size);
    ASSERT_TRUE(writer.file()->BufferIsEmpty());
    writer.file()->reset_seen_error();
    sink->set_fail_on(-1);
    ASSERT_OK(writer.AddRecord(wo, record, /*sequence=*/7,
                               /*num_sequences=*/1));
    ASSERT_OK(writer.Close(wo));

    std::unique_ptr<BlogFileReader> reader = MakeReader();
    BlogFileHeader header;
    ASSERT_OK(reader->ReadHeader(&header));
    BlogRecordType type;
    Slice payload;
    std::string scratch;
    CompressionType trailer_type = kStreamingCompressionSentinel;
    ASSERT_OK(
        reader->ReadRecord(&type, &payload, &scratch, nullptr, &trailer_type));
    ASSERT_EQ(type, kBlogWriteBatchRecord);
    ASSERT_EQ(trailer_type, kNoCompression);
    ASSERT_EQ(payload, record);
    AssertWalFooter(reader.get());
  }
}

TEST_F(BlogWriterTest,
       WalWriterStreamingCompressionFailureBeforeOutputLeavesFileUnchanged) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_SKIP("Test requires ZSTD streaming support");
    return;
  }
  std::unique_ptr<BlogWalWriter> writer =
      MakeNonRecycledStreamingWalWriter(kZSTD);
  const WriteOptions wo;
  ASSERT_OK(writer->WriteFileStart(wo, PredecessorWALInfo()));
  ASSERT_OK(writer->AddRecord(wo, "first", /*sequence=*/1,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->file()->Sync(IOOptions()));
  const std::string before_error = contents_;
  const uint64_t before_offset = WalWriterOffset(writer.get());
  const uint64_t before_body_end = writer->GetVerificationSize();
  ASSERT_EQ(before_offset % kBlogMinRecordAlignment, 0u);

  FailStreamingCompressionBeforeOutput(writer.get());
  const IOStatus error = writer->AddRecord(wo, std::string(16384, 'X'),
                                           /*sequence=*/2, /*num_sequences=*/1);
  ASSERT_TRUE(error.IsCorruption()) << error.ToString();
  EXPECT_EQ(WalWriterOffset(writer.get()), before_offset);
  EXPECT_EQ(writer->GetVerificationSize(), before_body_end);
  ASSERT_OK(writer->WriteBuffer(wo));
  EXPECT_EQ(contents_, before_error);

  ASSERT_OK(writer->AddRecord(wo, "third", /*sequence=*/3,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->AddRecord(wo, "fourth", /*sequence=*/4,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->file()->Sync(IOOptions()));
  ASSERT_OK(writer->Close(wo));

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader header;
  ASSERT_OK(reader->ReadHeader(&header));
  ASSERT_EQ(CountUnspecifiedSizeRecords(header), 0u);
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  CompressionType compression_type = kNoCompression;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, nullptr,
                               &compression_type));
  ASSERT_EQ(type, kBlogWriteBatchRecord);
  ASSERT_EQ(compression_type, kStreamingCompressionSentinel);
  std::string decoded;
  DecompressStreamedPayloads({payload.ToString()}, &decoded);
  ASSERT_EQ(decoded, "first");

  uint64_t record_offset = 0;
  for (const std::string& expected :
       {std::string("third"), std::string("fourth")}) {
    ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch, &record_offset,
                                 &compression_type));
    ASSERT_EQ(type, kBlogWriteBatchRecord);
    ASSERT_EQ(compression_type, kNoCompression);
    ASSERT_EQ(record_offset % kBlogMinRecordAlignment, 0u);
    ASSERT_EQ(payload, expected);
    if (expected == "third") {
      ASSERT_EQ(record_offset, before_offset);
    }
  }
  AssertWalFooter(reader.get());
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

// A writer constructed with BlogUnspecifiedSizeRecords::kDisallowed never
// picks the unspecified-size encoding, however hard the noncanonical config
// pushes for it.
TEST_F(BlogWriterTest, UnspecifiedSizeRecordsSuppressedWhenDisallowed) {
  TEST_BlogNoncanonicalConfigScope config_scope(
      UnspecifiedSizeOnlyConfig(/*seed=*/11));

  BlogFileHeader header = MakeBlobHeader();
  std::unique_ptr<BlogFileWriter> writer =
      MakeWriter(header, BlogUnspecifiedSizeRecords::kDisallowed);

  WriteOptions wo;
  ASSERT_OK(writer->WriteHeader(wo));
  const std::string blob_data(128, 'U');
  ASSERT_OK(writer->AddBlobRecord(wo, blob_data, kNoCompression, nullptr,
                                  blob_data.size()));
  ASSERT_OK(writer->Close(wo));

  ASSERT_EQ(FindUnspecifiedSizeRecordOffset(header), std::string::npos);

  std::unique_ptr<BlogFileReader> reader = MakeReader();
  BlogFileHeader rh;
  ASSERT_OK(reader->ReadHeader(&rh));
  BlogRecordType type;
  Slice payload;
  std::string scratch;
  ASSERT_OK(reader->ReadRecord(&type, &payload, &scratch));
  ASSERT_EQ(type, kBlogBlobRecord);
  ASSERT_EQ(payload, blob_data);
}

// A recycled WAL keeps the previous incarnation's bytes past its logical end,
// so neither of the two boundaries an unspecified-size record can end at is
// reachable in one. The writer must not produce the encoding there, however
// hard the noncanonical config pushes for it.
TEST_F(BlogWriterTest, RecycledWalWriterNeverWritesUnspecifiedSizeRecords) {
  TEST_BlogNoncanonicalConfigScope config_scope(
      UnspecifiedSizeOnlyConfig(/*seed=*/11));

  std::unique_ptr<BlogWalWriter> writer = MakeWalWriter(/*is_recycled=*/true);
  WriteOptions wo;
  const std::string record(128, 'W');
  ASSERT_OK(writer->AddRecord(wo, record, /*first_sequence=*/1,
                              /*num_sequences=*/1));
  ASSERT_OK(writer->WriteBuffer(wo));

  Slice header_input(contents_);
  BlogFileHeader header;
  ASSERT_OK(header.DecodeFrom(&header_input));
  ASSERT_TRUE(header.is_recycled());
  ASSERT_EQ(FindUnspecifiedSizeRecordOffset(header), std::string::npos);
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
