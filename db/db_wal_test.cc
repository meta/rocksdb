//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <cstring>
#include <future>
#include <limits>

#include "db/blog/blog_format.h"
#include "db/blog/blog_reader.h"
#include "db/blog/blog_writer.h"
#include "db/blog_wal_reader.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/legacy_wal_reader.h"
#include "db/legacy_wal_writer.h"
#include "db/wal_reader.h"
#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/file_system.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/prefix_varint.h"
#include "util/random.h"
#include "util/udt_util.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

#if defined(OS_LINUX)
#include <sys/statfs.h>
#endif

#if defined(OS_LINUX) && !defined(BTRFS_SUPER_MAGIC)
#define BTRFS_SUPER_MAGIC 0x9123683E
#endif

#if defined(OS_LINUX) && !defined(TMPFS_MAGIC)
#define TMPFS_MAGIC 0x01021994
#endif

#if defined(OS_LINUX) && !defined(OVERLAYFS_SUPER_MAGIC)
#define OVERLAYFS_SUPER_MAGIC 0x794c7630
#endif

#if defined(OS_LINUX) && !defined(ZFS_SUPER_MAGIC)
#define ZFS_SUPER_MAGIC 0x2fc12fc1
#endif

namespace ROCKSDB_NAMESPACE {
class DBWALTestBase : public DBTestBase {
 protected:
  explicit DBWALTestBase(const std::string& dir_name)
      : DBTestBase(dir_name, /*env_do_fsync=*/true) {}

  std::vector<uint64_t> ListWalNumbers() {
    std::vector<uint64_t> wal_numbers;
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& file : files) {
      uint64_t number = 0;
      FileType type = kWalFile;
      if (ParseFileName(file, &number, &type) && type == kWalFile) {
        wal_numbers.push_back(number);
      }
    }
    std::sort(wal_numbers.begin(), wal_numbers.end());
    return wal_numbers;
  }

  void CreateEmptyWal(uint64_t wal_number) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(LogFileName(dbname_, wal_number), &file,
                                    EnvOptions()));
    ASSERT_OK(file->Close());
  }

  void CreateWalWithContents(uint64_t wal_number, const Slice& contents) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(LogFileName(dbname_, wal_number), &file,
                                    EnvOptions()));
    ASSERT_OK(file->Append(contents));
    ASSERT_OK(file->Close());
  }

  void WriteWalForOpenTest(bool blog, std::string* contents,
                           uint64_t* log_number) {
    Options options = CurrentOptions();
    options.use_blog_format_for_wals = blog;
    options.wal_compression = kNoCompression;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", "value"));
    ASSERT_OK(db_->FlushWAL(true));
    std::unique_ptr<LogFile> wal_file;
    ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
    *log_number = wal_file->LogNumber();
    ASSERT_OK(
        ReadFileToString(env_, LogFileName(dbname_, *log_number), contents));
  }

  void OverwriteFile(const std::string& fname, const Slice& contents) {
    std::unique_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(fname, &file, EnvOptions()));
    ASSERT_OK(file->Append(contents));
    ASSERT_OK(file->Close());
  }

  // Location of a compact-format record within a blog file body.
  struct CompactRecordLayout {
    size_t escape_offset = 0;
    size_t length_varint_offset = 0;
    size_t varint_size = 0;
    uint64_t payload_length = 0;
    // One past the record's trailer, before any padding.
    size_t end_offset = 0;
  };

  // Location of a full-format record. The type byte is followed by the
  // compression byte and the 4-byte prefix checksum.
  struct FullRecordLayout {
    size_t escape_offset = 0;
    size_t length_varint_offset = 0;
    size_t varint_size = 0;
    size_t type_offset = 0;
    uint64_t payload_length = 0;
  };

  // Offset of the next escape sequence, given one past a record's trailer.
  static size_t RecordEndWithPadding(const std::string& contents, size_t end) {
    const uint8_t last_byte = static_cast<uint8_t>(contents[end - 1]);
    size_t padding = (kBlogMinRecordAlignment - end % kBlogMinRecordAlignment) %
                     kBlogMinRecordAlignment;
    if (padding == 0 && (last_byte == 0x00 || last_byte == 0xFF)) {
      padding = kBlogMinRecordAlignment;
    }
    return end + padding;
  }

  // Stops at the nth (0-based) body record of the requested format, filling
  // whichever of `compact`/`full` matches `want_compact`.
  void FindNthRecordOfFormat(const std::string& contents, size_t n,
                             bool want_compact, CompactRecordLayout* compact,
                             FullRecordLayout* full) {
    Slice header_input(contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    size_t pos = contents.size() - header_input.size();
    while (pos < contents.size() &&
           (contents[pos] == '\0' || contents[pos] == '\xff')) {
      ++pos;
    }
    while (true) {
      ASSERT_LT(pos + kBlogEscapeSequenceSize, contents.size());
      ASSERT_EQ(pos % kBlogMinRecordAlignment, 0u);
      ASSERT_EQ(memcmp(contents.data() + pos, header.escape_sequence,
                       kBlogEscapeSequenceSize),
                0);
      const size_t varint_offset = pos + kBlogEscapeSequenceSize;
      uint64_t length = 0;
      const char* const varint_end =
          GetPrefixVarint64Ptr(contents.data() + varint_offset,
                               contents.data() + contents.size(), &length);
      ASSERT_TRUE(varint_end != nullptr);
      const size_t varint_size =
          static_cast<size_t>(varint_end - (contents.data() + varint_offset));
      const bool is_compact = varint_size <= kBlogCompactVarintMaxBytes;
      ASSERT_TRUE(is_compact || length != kBlogUnspecifiedSize);
      // Full-format records carry type + compression type + prefix checksum
      // between the length varint and the payload.
      const size_t kFullPrefixRestSize = 6;
      const size_t payload_offset =
          varint_offset + varint_size + (is_compact ? 0 : kFullPrefixRestSize);
      const size_t end =
          payload_offset + static_cast<size_t>(length) + kBlogBlockTrailerSize;
      if (is_compact == want_compact) {
        if (n == 0) {
          if (want_compact) {
            compact->escape_offset = pos;
            compact->length_varint_offset = varint_offset;
            compact->varint_size = varint_size;
            compact->payload_length = length;
            compact->end_offset = end;
          } else {
            full->escape_offset = pos;
            full->length_varint_offset = varint_offset;
            full->varint_size = varint_size;
            full->type_offset = varint_offset + varint_size;
            full->payload_length = length;
          }
          return;
        }
        --n;
      }
      ASSERT_LT(end, contents.size());
      pos = RecordEndWithPadding(contents, end);
    }
  }

  void FindNthCompactRecord(const std::string& contents, size_t n,
                            CompactRecordLayout* layout) {
    FindNthRecordOfFormat(contents, n, /*want_compact=*/true, layout, nullptr);
  }

  void FindNthFullRecord(const std::string& contents, size_t n,
                         FullRecordLayout* layout) {
    FindNthRecordOfFormat(contents, n, /*want_compact=*/false, nullptr, layout);
  }

  void FindFirstCompactRecord(const std::string& contents,
                              size_t* length_varint_offset,
                              uint64_t* payload_length) {
    CompactRecordLayout layout;
    FindNthCompactRecord(contents, 0, &layout);
    *length_varint_offset = layout.length_varint_offset;
    *payload_length = layout.payload_length;
  }

  // Fails the nth compact record's checksum while leaving its framing intact,
  // so the reader identifies the boundary and then rejects the record.
  void CorruptNthCompactRecordPayload(std::string* contents, size_t n) {
    CompactRecordLayout layout;
    FindNthCompactRecord(*contents, n, &layout);
    ASSERT_GT(layout.payload_length, 2u);
    (*contents)[layout.length_varint_offset + layout.varint_size + 2] ^= 0x01;
  }

  void CorruptFirstCompactRecordPayload(std::string* contents) {
    CorruptNthCompactRecordPayload(contents, 0);
  }

  // Drops everything after the nth compact record's trailer, so that no
  // record boundary follows the damage.
  void TruncateAfterNthCompactRecord(std::string* contents, size_t n) {
    CompactRecordLayout layout;
    FindNthCompactRecord(*contents, n, &layout);
    ASSERT_LE(layout.end_offset, contents->size());
    contents->resize(layout.end_offset);
  }

  // Re-encodes the nth compact record with the unspecified-size length
  // sentinel and drops everything after it. That record's extent is then
  // defined only by the escape sequence of a record that does not exist,
  // which is how a crash leaves the tail of a blog file that used the
  // encoding. The payload and both checksums are rebuilt for the record's new
  // framing, so the only unusual thing about the file is the encoding.
  void RewriteNthCompactRecordAsUnspecifiedSizeTail(std::string* contents,
                                                    size_t n) {
    Slice header_input(*contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));

    CompactRecordLayout layout;
    FindNthCompactRecord(*contents, n, &layout);
    const std::string payload(
        contents->data() + layout.length_varint_offset + layout.varint_size,
        static_cast<size_t>(layout.payload_length));
    const char compression_byte =
        (*contents)[layout.end_offset - kBlogBlockTrailerSize];

    std::string record(header.escape_sequence, kBlogEscapeSequenceSize);
    const size_t varint_start = record.size();
    PutPrefixVarint64(&record, kBlogUnspecifiedSize);
    record.push_back(static_cast<char>(header.compact_record_type));
    record.push_back(compression_byte);
    const uint32_t prefix_checksum =
        ComputeBuiltinChecksum(header.checksum_type,
                               record.data() + varint_start,
                               record.size() - varint_start) +
        ChecksumModifierForContext(header.incarnation_id(),
                                   layout.escape_offset);
    PutFixed32(&record, prefix_checksum);

    const uint64_t payload_file_offset = layout.escape_offset + record.size();
    record.append(payload);
    record.push_back(compression_byte);
    PutFixed32(&record, ComputeBlogRecordChecksum(
                            header.checksum_type, payload.data(),
                            payload.size(), compression_byte,
                            header.incarnation_id(), payload_file_offset));
    const BlogPadding pad(lossless_cast<uint8_t>(record.back()),
                          layout.escape_offset + record.size());
    pad.AppendTo(&record);

    contents->resize(layout.escape_offset);
    contents->append(record);
  }

  // Sets kBlogFileRecycled, as a recycled WAL file would carry.
  void MarkBlogFileRecycled(std::string* contents) {
    Slice header_input(*contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    const size_t header_len = contents->size() - header_input.size();
    header.flags = static_cast<uint8_t>(header.flags | kBlogFileRecycled);
    std::string encoded;
    header.EncodeTo(&encoded);
    ASSERT_EQ(encoded.size(), header_len);
    memcpy(&(*contents)[0], encoded.data(), header_len);
  }

  // Retypes a full-format record to a reserved value and repairs its prefix
  // checksum, so the type is the only thing the reader objects to.
  void SetFullRecordReservedType(std::string* contents,
                                 const FullRecordLayout& layout,
                                 uint8_t reserved_type) {
    Slice header_input(*contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    ASSERT_LE(layout.type_offset + 6, contents->size());
    (*contents)[layout.type_offset] = static_cast<char>(reserved_type);
    const uint32_t checksum =
        ComputeBuiltinChecksum(header.checksum_type,
                               contents->data() + layout.length_varint_offset,
                               layout.varint_size + 2) +
        ChecksumModifierForContext(header.incarnation_id(),
                                   layout.escape_offset);
    EncodeFixed32(&(*contents)[layout.type_offset + 2], checksum);
  }

  // Keeps the first status, as DBOpenLogRecordReadReporter does.
  class CountingWalReporter : public log::Reader::Reporter {
   public:
    ~CountingWalReporter() override {
      first_status.PermitUncheckedError();
      first_tolerated.PermitUncheckedError();
      last_status.PermitUncheckedError();
    }

    void Corruption(size_t /*bytes*/, const Status& s,
                    uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      ++corruption_count;
      RecordFirst(s);
      last_status = s;
    }

    void UnrecoverableError(size_t bytes, const Status& s,
                            uint64_t log_number = kMaxSequenceNumber) override {
      ++unrecoverable_count;
      Corruption(bytes, s, log_number);
    }

    void ToleratedCorruption(
        size_t /*bytes*/, const Status& s,
        uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      ++tolerated_count;
      if (first_tolerated.ok()) {
        first_tolerated = s;
      }
      last_status = s;
    }

    void OldLogRecord(size_t /*bytes*/) override { ++old_log_record_count; }

    // Reports that would fail DB::Open, whatever channel they arrived on.
    size_t failing_count() const { return corruption_count; }

    size_t corruption_count = 0;
    size_t unrecoverable_count = 0;
    size_t tolerated_count = 0;
    size_t old_log_record_count = 0;
    Status first_status;
    Status first_tolerated;
    Status last_status;

   private:
    void RecordFirst(const Status& s) {
      if (first_status.ok()) {
        first_status = s;
      }
    }
  };

  // Serves contents the test can grow between reads, as a live WAL does.
  class GrowingSequentialFile : public FSSequentialFile {
   public:
    explicit GrowingSequentialFile(const std::string* contents)
        : contents_(contents) {}

    IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                  char* scratch, IODebugContext* /*dbg*/) override {
      ++read_count;
      n = std::min(n, contents_->size() - offset_);
      if (n > 0) {
        memcpy(scratch, contents_->data() + offset_, n);
        offset_ += n;
      }
      *result = Slice(scratch, n);
      return IOStatus::OK();
    }

    IOStatus Skip(uint64_t n) override {
      offset_ += static_cast<size_t>(n);
      return IOStatus::OK();
    }

    size_t read_count = 0;

   private:
    const std::string* contents_;
    size_t offset_ = 0;
  };

  // Serves fixed contents and fails every read at or past `fail_at_offset`.
  class FaultySequentialFile : public FSSequentialFile {
   public:
    FaultySequentialFile(std::string contents, size_t fail_at_offset)
        : contents_(std::move(contents)), fail_at_(fail_at_offset) {}

    IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                  char* scratch, IODebugContext* /*dbg*/) override {
      ++read_count;
      if (offset_ >= fail_at_) {
        return IOStatus::IOError("injected read failure");
      }
      n = std::min(n, std::min(fail_at_, contents_.size()) - offset_);
      if (n > 0) {
        memcpy(scratch, contents_.data() + offset_, n);
        offset_ += n;
      }
      *result = Slice(scratch, n);
      return IOStatus::OK();
    }

    IOStatus Skip(uint64_t n) override {
      offset_ += static_cast<size_t>(n);
      return IOStatus::OK();
    }

    size_t read_count = 0;

   private:
    const std::string contents_;
    const size_t fail_at_;
    size_t offset_ = 0;
  };

  // Fails sequential reads at or past `fail_at`, surfacing a mid-file
  // device error. Reads before it behave normally.
  class FailReadsPastOffsetFile : public FSSequentialFileOwnerWrapper {
   public:
    FailReadsPastOffsetFile(std::unique_ptr<FSSequentialFile> base,
                            uint64_t fail_at)
        : FSSequentialFileOwnerWrapper(std::move(base)), fail_at_(fail_at) {}

    IOStatus Read(size_t n, const IOOptions& opts, Slice* result, char* scratch,
                  IODebugContext* dbg) override {
      if (position_ >= fail_at_) {
        return IOStatus::IOError("injected mid-file read failure");
      }
      // Clamp at the fault so the primed prefix ends here and the first
      // read touching it fails, whatever the reader's block size.
      n = std::min(n, static_cast<size_t>(fail_at_ - position_));
      IOStatus s =
          FSSequentialFileOwnerWrapper::Read(n, opts, result, scratch, dbg);
      if (s.ok()) {
        position_ += result->size();
      }
      return s;
    }

    IOStatus Skip(uint64_t n) override {
      position_ += static_cast<size_t>(n);
      return FSSequentialFileOwnerWrapper::Skip(n);
    }

    IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& opts,
                            Slice* result, char* scratch,
                            IODebugContext* dbg) override {
      if (offset >= fail_at_) {
        return IOStatus::IOError("injected mid-file read failure");
      }
      return FSSequentialFileOwnerWrapper::PositionedRead(offset, n, opts,
                                                          result, scratch, dbg);
    }

   private:
    const uint64_t fail_at_;
    uint64_t position_ = 0;
  };

  // Fails sequential reads past `fail_at` for files whose name ends with
  // `target_suffix`, so WAL recovery hits a device error mid-file.
  class FailWalReadsPastOffsetFS : public FileSystemWrapper {
   public:
    FailWalReadsPastOffsetFS(const std::shared_ptr<FileSystem>& base,
                             std::string target_suffix, uint64_t fail_at)
        : FileSystemWrapper(base),
          target_suffix_(std::move(target_suffix)),
          fail_at_(fail_at) {}

    const char* Name() const override { return "FailWalReadsPastOffsetFS"; }

    IOStatus NewSequentialFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSSequentialFile>* r,
                               IODebugContext* dbg) override {
      IOStatus s = FileSystemWrapper::NewSequentialFile(f, file_opts, r, dbg);
      if (s.ok() && f.size() >= target_suffix_.size() &&
          f.compare(f.size() - target_suffix_.size(), target_suffix_.size(),
                    target_suffix_) == 0) {
        r->reset(new FailReadsPastOffsetFile(std::move(*r), fail_at_));
      }
      return s;
    }

   private:
    const std::string target_suffix_;
    const uint64_t fail_at_;
  };

  // Fails the first FileSystem::GetFileSize for a WAL file once the WAL was
  // opened for reading, then forwards normally. Recovery reads each WAL fully
  // before UpdatePredecessorWALInfo stats it, so the single failure lands on
  // that call; later stats (alive-log restore, truncation) succeed.
  class WalGetFileSizeFailFS : public FileSystemWrapper {
   public:
    WalGetFileSizeFailFS(const std::shared_ptr<FileSystem>& base,
                         std::string target_suffix)
        : FileSystemWrapper(base), target_suffix_(std::move(target_suffix)) {}

    const char* Name() const override { return "WalGetFileSizeFailFS"; }

    IOStatus NewSequentialFile(const std::string& f,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSSequentialFile>* r,
                               IODebugContext* dbg) override {
      if (IsTarget(f)) {
        wal_opened_for_read_.store(true);
      }
      return FileSystemWrapper::NewSequentialFile(f, file_opts, r, dbg);
    }

    IOStatus GetFileSize(const std::string& f, const IOOptions& options,
                         uint64_t* file_size, IODebugContext* dbg) override {
      if (IsTarget(f) && wal_opened_for_read_.load() &&
          !failed_once_.exchange(true)) {
        failed_count_.fetch_add(1);
        return IOStatus::IOError("injected WAL GetFileSize failure");
      }
      return FileSystemWrapper::GetFileSize(f, options, file_size, dbg);
    }

    size_t failed_count() const { return failed_count_.load(); }

   private:
    bool IsTarget(const std::string& f) const {
      return f.size() >= target_suffix_.size() &&
             f.compare(f.size() - target_suffix_.size(), target_suffix_.size(),
                       target_suffix_) == 0;
    }

    const std::string target_suffix_;
    std::atomic<bool> wal_opened_for_read_{false};
    std::atomic<bool> failed_once_{false};
    std::atomic<size_t> failed_count_{0};
  };

  std::unique_ptr<WalReader> NewWalReaderOver(
      std::unique_ptr<FSSequentialFile> source, const std::string& fname,
      log::Reader::Reporter* reporter, uint64_t log_number) {
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source), fname));
    std::unique_ptr<WalReaderOpenState> pending_open;
    return NewWalReader(nullptr, std::move(file_reader), reporter,
                        /*checksum=*/true, log_number,
                        /*track_and_verify_wals=*/false,
                        /*stop_replay_for_corruption=*/false,
                        std::numeric_limits<uint64_t>::max(),
                        PredecessorWALInfo(), /*buffered=*/false,
                        &pending_open);
  }

  // Opens a WAL reader over in-memory contents, which must outlive the reader.
  std::unique_ptr<WalReader> NewMemWalReader(const std::string* contents,
                                             log::Reader::Reporter* reporter,
                                             uint64_t log_number) {
    return NewWalReaderOver(
        std::unique_ptr<FSSequentialFile>(new GrowingSequentialFile(contents)),
        "mem-blog-wal", reporter, log_number);
  }

  // Drains a WAL reader, returning how many records it yielded.
  size_t ReadAllRecords(WalReader* reader, WALRecoveryMode mode) {
    Slice record;
    std::string scratch;
    size_t records = 0;
    while (reader->ReadRecord(&record, &scratch, mode)) {
      ++records;
    }
    return records;
  }

  struct BlogWalRecordLayout {
    uint64_t record_offset;
    uint64_t payload_offset;
    uint64_t length;
  };

  // Locates the WriteBatch records in blog WAL bytes: offsets from
  // BlogFileReader plus a framing peek for payload starts.
  std::vector<BlogWalRecordLayout> ParseBlogWalRecordLayout(
      const std::string& contents) {
    std::vector<BlogWalRecordLayout> layout;
    std::unique_ptr<FSSequentialFile> source(
        new GrowingSequentialFile(&contents));
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source), "layout-wal"));
    BlogFileReader reader(std::move(file_reader), /*reporter=*/nullptr,
                          /*verify_checksums=*/true);
    BlogFileHeader header;
    if (!reader.ReadHeader(&header).ok()) {
      ADD_FAILURE() << "unreadable blog WAL header";
      return layout;
    }
    BlogRecordType type;
    Slice record;
    std::string scratch;
    uint64_t record_offset = 0;
    Status s;
    while ((s = reader.ReadRecord(&type, &record, &scratch, &record_offset))
               .ok()) {
      if (type != kBlogWriteBatchRecord) {
        continue;
      }
      const size_t varint_off = record_offset + kBlogEscapeSequenceSize;
      if (varint_off >= contents.size()) {
        ADD_FAILURE() << "record runs past end of file";
        return layout;
      }
      const size_t varint_len =
          PrefixVarint64AddlByteCount(contents[varint_off]) + 1;
      uint64_t payload_off = varint_off + varint_len;
      if (varint_len > kBlogCompactVarintMaxBytes) {
        payload_off += 6;  // type + compression + prefix checksum
      }
      layout.push_back(
          {record_offset, payload_off, static_cast<uint64_t>(record.size())});
    }
    if (!s.IsNotFound()) {
      ADD_FAILURE() << "layout walk failed: " << s.ToString();
    }
    return layout;
  }

  // Builds a two-batch hand-built WAL and returns the second record's
  // escape-sequence offset, so tests can replace what follows it.
  size_t CutBeforeSecondBlogRecord(std::string* contents, bool recycled,
                                   uint64_t log_number) {
    {
      std::unique_ptr<BlogFileWriter> writer =
          MakeHandBuiltWalWriter(contents, log_number, recycled);
      EXPECT_OK(writer->WriteHeader(WriteOptions()));
      EXPECT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-one",
                                            kNoCompression));
      EXPECT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-two",
                                            kNoCompression));
      EXPECT_OK(writer->Close(WriteOptions()));
    }
    std::vector<BlogWalRecordLayout> layout =
        ParseBlogWalRecordLayout(*contents);
    EXPECT_EQ(layout.size(), 2u);
    return static_cast<size_t>(layout[1].record_offset);
  }

  // A byte that is neither padding nor equal to `avoid`.
  static char NonPaddingByteOtherThan(char avoid) {
    return avoid == '\xab' ? '\xcd' : '\xab';
  }

  // Asserts that, in every recovery mode, `contents` yields its first record
  // and then ends as a stale recycled tail: nothing failing, tolerated, or
  // unrecoverable is reported, and only kPointInTimeRecovery gets the
  // old-record signal that checks the next WAL's continuity.
  void AssertFirstRecordThenStaleTail(const std::string& contents,
                                      uint64_t log_number) {
    for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                                 WALRecoveryMode::kAbsoluteConsistency,
                                 WALRecoveryMode::kPointInTimeRecovery,
                                 WALRecoveryMode::kSkipAnyCorruptedRecords}) {
      SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
      CountingWalReporter reporter;
      std::unique_ptr<WalReader> reader =
          NewMemWalReader(&contents, &reporter, log_number);
      ASSERT_EQ(ReadAllRecords(reader.get(), mode), 1u);
      ASSERT_EQ(reporter.failing_count(), 0u)
          << reporter.first_status.ToString();
      ASSERT_EQ(reporter.tolerated_count, 0u)
          << reporter.first_tolerated.ToString();
      ASSERT_EQ(reporter.unrecoverable_count, 0u);
      ASSERT_EQ(reporter.old_log_record_count,
                mode == WALRecoveryMode::kPointInTimeRecovery ? 1u : 0u);
    }
  }

  struct TornWalFixture {
    std::string fname;
    size_t torn_size = 0;
    size_t batch_count = 0;
  };

  // Reads a blog WAL fully: counts the WriteBatch records delivered and
  // returns the terminal status.
  static Status CountWalBatches(const std::string& contents, size_t* batches) {
    *batches = 0;
    std::unique_ptr<FSSequentialFile> source(
        new GrowingSequentialFile(&contents));
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source), "count-wal"));
    BlogFileReader reader(std::move(file_reader), /*reporter=*/nullptr,
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
      *batches += (type == kBlogWriteBatchRecord) ? 1 : 0;
    }
    return s;
  }

  // Writes key1/key2 to a blog WAL, seals it with Close(), then truncates
  // the last record's tail: mid-padding (every record intact) or mid-record
  // (the last record torn). The truncate must come after Close(): truncating
  // the live file lets Close() rewrite the footer at the writer's remembered
  // end offset, backfilling the cut range with zeros (valid padding) and
  // hiding the tear from recovery.
  void BuildTornWal(TornWalFixture* fixture, bool tear_mid_record) {
    fixture->torn_size = 0;
    Options options = CurrentOptions();
    options.use_blog_format_for_wals = true;
    // Keep the torn WAL live across closes: a shutdown flush would obsolete
    // it and the reopen would skip replaying it.
    options.avoid_flush_during_shutdown = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", "value1"));
    ASSERT_OK(Put("key2", "value2"));
    ASSERT_OK(db_->FlushWAL(true));

    std::unique_ptr<LogFile> wal_file;
    ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
    fixture->fname = LogFileName(dbname_, wal_file->LogNumber());
    std::string complete_contents;
    ASSERT_OK(ReadFileToString(env_, fixture->fname, &complete_contents));

    const std::vector<BlogWalRecordLayout> layout =
        ParseBlogWalRecordLayout(complete_contents);
    ASSERT_GE(layout.size(), 2u);
    const size_t last_end = static_cast<size_t>(layout.back().payload_offset +
                                                layout.back().length) +
                            kBlogBlockTrailerSize;
    ASSERT_LT(last_end, complete_contents.size());
    const size_t gap = complete_contents.size() - last_end;
    ASSERT_GE(gap, 2u) << "fixture lacks a 2-byte last gap; adjust keys";
    ASSERT_LE(gap, 3u) << "non-canonical trailing gap";
    const size_t torn = tear_mid_record ? last_end - 1 : last_end + 1;

    // Self-check: the torn prefix delivers the intact records, then
    // Incomplete.
    {
      size_t batches = 0;
      const Status s =
          CountWalBatches(complete_contents.substr(0, torn), &batches);
      ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
      ASSERT_EQ(batches, tear_mid_record ? layout.size() - 1 : layout.size());
    }

    Close();
    ASSERT_OK(test::TruncateFile(env_, fixture->fname, torn));
    uint64_t size_after = 0;
    ASSERT_OK(env_->GetFileSize(fixture->fname, &size_after));
    ASSERT_EQ(size_after, torn);
    fixture->torn_size = torn;
    fixture->batch_count = layout.size();
  }

  // Asserts the WAL on disk is really cut off at the fixture's torn size:
  // the intact records deliver, then Incomplete. Without this, a test whose
  // file regrew before reopen would pass without recovery ever facing the
  // tear.
  void AssertWalIsCutOff(const TornWalFixture& fixture, bool tear_mid_record) {
    uint64_t size = 0;
    ASSERT_OK(env_->GetFileSize(fixture.fname, &size));
    ASSERT_EQ(size, fixture.torn_size);
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, fixture.fname, &contents));
    size_t batches = 0;
    const Status s = CountWalBatches(contents, &batches);
    ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    ASSERT_EQ(batches,
              tear_mid_record ? fixture.batch_count - 1 : fixture.batch_count);
  }

  // Header for a hand-built WAL whose escape sequence matches `log_number`,
  // so BlogWalReader accepts it. Declares BuiltinV2 per-record compression
  // compatibility without streaming.
  BlogFileHeader MakeBuiltinV2WalHeader(uint64_t log_number) {
    BlogFileHeader header;
    header.checksum_type = kXXH3;
    header.compact_record_type = kBlogWriteBatchRecord;
    header.GenerateWalFromUniqueId("db-id", "session-id", log_number);
    header.SetProperty(kBlogPropRole, "wal");
    header.SetProperty(kBlogPropCompressionCompatibilityName, "BuiltinV2");
    return header;
  }

  // Header for a hand-built streaming WAL whose escape sequence matches
  // `log_number`, so BlogWalReader accepts it. `streaming_type` is the hex
  // value of the declared streaming type.
  BlogFileHeader MakeStreamingWalHeader(uint64_t log_number,
                                        const std::string& streaming_type) {
    BlogFileHeader header = MakeBuiltinV2WalHeader(log_number);
    header.SetProperty(kBlogPropWriteBatchStreamingCompressionType,
                       streaming_type);
    return header;
  }

  // Opens a BlogFileWriter over `contents` for a hand-built WAL.
  std::unique_ptr<BlogFileWriter> MakeHandBuiltWalWriterWithHeader(
      std::string* contents, const BlogFileHeader& header) {
    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::unique_ptr<FSWritableFile>(
                                   new test::StringFS::StringSink(contents)),
                               "" /* file name */, FileOptions()));
    return std::make_unique<BlogFileWriter>(
        std::move(file_writer), header,
        BlogUnspecifiedSizeRecords::kDisallowed);
  }

  // Opens a BlogFileWriter for a hand-built WAL declaring no compression
  // properties. `log_number` must match the reading log number.
  std::unique_ptr<BlogFileWriter> MakeHandBuiltWalWriter(std::string* contents,
                                                         uint64_t log_number,
                                                         bool recycled) {
    BlogFileHeader header;
    header.checksum_type = kXXH3;
    header.compact_record_type = kBlogWriteBatchRecord;
    if (recycled) {
      header.flags = static_cast<uint8_t>(header.flags | kBlogFileRecycled);
    }
    header.GenerateWalFromUniqueId("db-id", "session-id", log_number);
    return MakeHandBuiltWalWriterWithHeader(contents, header);
  }

  // Opens a BlogFileWriter for a hand-built streaming WAL.
  std::unique_ptr<BlogFileWriter> MakeHandBuiltStreamingWalWriter(
      std::string* contents, uint64_t log_number,
      const std::string& streaming_type) {
    return MakeHandBuiltWalWriterWithHeader(
        contents, MakeStreamingWalHeader(log_number, streaming_type));
  }

  // Opens a BlogFileWriter for a hand-built (non-streaming) WAL
  // declaring BuiltinV2 compression compatibility.
  std::unique_ptr<BlogFileWriter> MakeHandBuiltWalWriter(std::string* contents,
                                                         uint64_t log_number) {
    return MakeHandBuiltWalWriterWithHeader(contents,
                                            MakeBuiltinV2WalHeader(log_number));
  }

  // Flips a full-format record's type byte to `new_type`, repairing its
  // prefix checksum. The header is decoded from the file bytes, since
  // header construction randomizes the escape for these test IDs.
  void RewriteFullRecordType(std::string* contents, uint64_t record_offset,
                             uint8_t new_type) {
    FullRecordLayout layout;
    layout.escape_offset = record_offset;
    layout.length_varint_offset = record_offset + kBlogEscapeSequenceSize;
    Slice varint_input(contents->data() + layout.length_varint_offset,
                       contents->size() - layout.length_varint_offset);
    ASSERT_TRUE(GetPrefixVarint64(&varint_input, &layout.payload_length));
    layout.varint_size =
        contents->size() - layout.length_varint_offset - varint_input.size();
    ASSERT_GT(layout.varint_size, kBlogCompactVarintMaxBytes);
    layout.type_offset = layout.length_varint_offset + layout.varint_size;
    SetFullRecordReservedType(contents, layout, new_type);
  }

  // Compresses each input in order as one ZSTD stream; returns the
  // per-record fragments.
  std::vector<std::string> CompressStreamFragments(
      const std::vector<std::string>& inputs) {
    std::vector<std::string> fragments;
    std::unique_ptr<StreamingCompress> compress = StreamingCompress::Create(
        kZSTD, CompressionOptions(), /*compress_format_version=*/2,
        /*max_output_len=*/0);
    if (compress == nullptr) {
      ADD_FAILURE() << "ZSTD streaming compression unavailable";
      return fragments;
    }
    for (const std::string& input : inputs) {
      fragments.emplace_back();
      EXPECT_OK(compress->CompressAndFlush(input, &fragments.back()));
    }
    return fragments;
  }

  uint64_t WalLogNumberFromFname(const std::string& fname) {
    const std::string base = fname.substr(fname.find_last_of('/') + 1);
    uint64_t log_number = 0;
    FileType type = kWalFile;
    EXPECT_TRUE(ParseFileName(base, &log_number, &type));
    EXPECT_EQ(type, kWalFile);
    return log_number;
  }

  // Corrupts a compact record's length varint in place. A longer encoding
  // overwrites leading payload bytes so that later records stay put.
  void OverwriteCompactRecordLength(size_t length_varint_offset,
                                    uint64_t length,
                                    size_t expected_encoded_size,
                                    std::string* contents) {
    char encoded[kMaxPrefixVarint64Length];
    char* const end = EncodePrefixVarint64(encoded, length);
    ASSERT_EQ(static_cast<size_t>(end - encoded), expected_encoded_size);
    ASSERT_LE(expected_encoded_size, kBlogCompactVarintMaxBytes);
    ASSERT_LE(length_varint_offset + expected_encoded_size, contents->size());
    memcpy(contents->data() + length_varint_offset, encoded,
           expected_encoded_size);
  }

  void WriteThreeRecordBlogWal(const Options& options, std::string* fname) {
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", "value1"));
    ASSERT_OK(Put("key2", "value2"));
    ASSERT_OK(Put("key3", "value3"));
    VectorWalPtr log_files;
    ASSERT_OK(db_->GetSortedWalFiles(log_files));
    ASSERT_EQ(log_files.size(), 1);
    *fname = LogFileName(dbname_, log_files[0]->LogNumber());
    Close();
  }

  // Blog twin of GetCompressedWalsAfterSync's flow: rotates a WAL before its
  // first record so it stays header-only, syncs, and checks the header-only
  // WAL is listed. `streaming` selects blog_wal_streaming_compression (with
  // ZSTD); otherwise wal_compression stays kNoCompression.
  void CheckBlogWalsAfterSync(CompressionType wal_compression, bool streaming) {
    Options options = GetDefaultOptions();
    options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    options.create_if_missing = true;
    options.env = env_;
    options.avoid_flush_during_recovery = true;
    // No shutdown flush: Close must not need the background pool blocked
    // below.
    options.avoid_flush_during_shutdown = true;
    options.track_and_verify_wals_in_manifest = true;
    options.use_blog_format_for_wals = true;
    options.wal_compression = wal_compression;
    options.blog_wal_streaming_compression = streaming;

    // Stop background flush so that no WAL is deleted before it is listed.
    options.env->SetBackgroundThreads(1, Env::HIGH);
    test::SleepingBackgroundTask sleeping_task;
    options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                          &sleeping_task, Env::Priority::HIGH);
    // Release the pool on every exit, including a failed assertion, so the
    // background thread never outlives `sleeping_task`.
    Defer wake_background([&sleeping_task]() {
      sleeping_task.WakeUp();
      sleeping_task.WaitUntilDone();
    });

    DestroyAndReopen(options);

    // Write something to memtable and WAL so that wal_empty_ will be false
    // after next DB::Open().
    ASSERT_OK(Put("a", "v"));

    Reopen(options);

    // New WAL is created, thanks to !wal_empty_.
    ASSERT_OK(dbfull()->TEST_SwitchWAL());

    ASSERT_OK(Put("b", "v"));

    ASSERT_OK(db_->SyncWAL());

    VectorLogPtr wals;
    ASSERT_OK(dbfull()->GetSortedWalFiles(wals));
    // The header-only middle WAL must be listed, not dropped.
    ASSERT_EQ(wals.size(), 3u);
    std::vector<uint64_t> listed;
    for (const auto& w : wals) {
      listed.push_back(w->LogNumber());
    }
    std::vector<uint64_t> on_disk;
    {
      std::vector<std::string> files;
      ASSERT_OK(env_->GetChildren(dbname_, &files));
      for (const auto& file : files) {
        uint64_t number = 0;
        FileType type = kWalFile;
        if (ParseFileName(file, &number, &type) && type == kWalFile) {
          on_disk.push_back(number);
        }
      }
    }
    std::sort(listed.begin(), listed.end());
    std::sort(on_disk.begin(), on_disk.end());
    ASSERT_EQ(listed, on_disk);
  }

  // Reads the min/maxSequence footer properties of a sealed blog WAL file.
  // Returns NotFound if the file has no footer properties record or no
  // sequence range (e.g. it holds no sequence-consuming records).
  Status ReadBlogWalFooterSequenceRange(const std::string& fname,
                                        uint64_t* min_sequence,
                                        uint64_t* max_sequence) {
    assert(min_sequence != nullptr);
    assert(max_sequence != nullptr);
    std::unique_ptr<FSSequentialFile> file;
    FileOptions file_options;
    Status s = env_->GetFileSystem()->NewSequentialFile(fname, file_options,
                                                        &file, /*dbg=*/nullptr);
    if (!s.ok()) {
      return s;
    }
    auto file_reader =
        std::make_unique<SequentialFileReader>(std::move(file), fname);
    BlogFileReader reader(std::move(file_reader), /*reporter=*/nullptr,
                          /*verify_checksums=*/true);
    BlogFileHeader header;
    s = reader.ReadHeader(&header);
    if (!s.ok()) {
      return s;
    }
    BlogRecordType type;
    Slice record;
    std::string scratch;
    while ((s = reader.ReadRecord(&type, &record, &scratch)).ok()) {
      if (type != kBlogFooterPropertiesRecord) {
        continue;
      }
      BlogFileFooterProperties properties;
      s = properties.DecodeFrom(record);
      if (!s.ok()) {
        return s;
      }
      bool found_min = false;
      bool found_max = false;
      for (const auto& [name, value] : properties.properties) {
        Slice encoded(value);
        if (name == "minSequence") {
          if (!GetPrefixVarint64(&encoded, min_sequence) || !encoded.empty()) {
            return Status::Corruption("bad minSequence encoding");
          }
          found_min = true;
        } else if (name == "maxSequence") {
          if (!GetPrefixVarint64(&encoded, max_sequence) || !encoded.empty()) {
            return Status::Corruption("bad maxSequence encoding");
          }
          found_max = true;
        }
      }
      if (!found_min || !found_max) {
        return Status::NotFound("sequence range absent from footer");
      }
      return Status::OK();
    }
    if (s.IsNotFound()) {
      return Status::NotFound("no footer properties record");
    }
    return s;
  }

  // Creates a live blog WAL reusing a retired larger WAL file, so the
  // recycled file starts with a stale tail. Requires recycle_log_file_num > 0
  // in options. Uses TEST_SwitchMemtable (not TEST_SwitchWAL) so rotation is
  // deterministic and schedules no background flushes.
  void CreateRecycledBlogWal(const Options& options, std::string* fname,
                             uint64_t* log_number) {
    Options setup_options = options;
    // Recycling is sanitized away under Tolerate/Absolute; PIT keeps it.
    setup_options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    DestroyAndReopen(setup_options);
    for (int i = 0; i < 200; ++i) {
      ASSERT_OK(Put("bulk" + std::to_string(i), std::string(500, 'x')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    JobContext job_context(0);
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&job_context, true /* force */);
    dbfull()->TEST_UnlockMutex();
    dbfull()->PurgeObsoleteFiles(job_context);
    job_context.Clean();
    ASSERT_OK(Put("key2", "value2"));
    ASSERT_OK(dbfull()->TEST_SwitchMemtable());
    ASSERT_OK(Put("key3", "value3"));
    ASSERT_OK(Put("key3b", "value3b"));
    VectorWalPtr log_files;
    ASSERT_OK(db_->GetSortedWalFiles(log_files));
    ASSERT_EQ(log_files.size(), 2);
    *log_number = log_files[1]->LogNumber();
    *fname = LogFileName(dbname_, *log_number);
    uint64_t size = 0;
    ASSERT_OK(env_->GetFileSize(*fname, &size));
    ASSERT_GT(size, 50 * 1024);
  }

#if defined(ROCKSDB_PLATFORM_POSIX)
 public:
#if defined(ROCKSDB_FALLOCATE_PRESENT)
  bool IsFallocateSupported() {
    // Test fallocate support of running file system.
    // Skip this test if fallocate is not supported.
    std::string fname_test_fallocate = dbname_ + "/preallocate_testfile";
    int fd = -1;
    do {
      fd = open(fname_test_fallocate.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    } while (fd < 0 && errno == EINTR);
    assert(fd > 0);
    int alloc_status = fallocate(fd, 0, 0, 1);
    int err_number = errno;
    close(fd);
    assert(env_->DeleteFile(fname_test_fallocate) == Status::OK());
    if (err_number == ENOSYS || err_number == EOPNOTSUPP) {
      fprintf(stderr, "Skipped preallocated space check: %s\n",
              errnoStr(err_number).c_str());
      return false;
    }
    assert(alloc_status == 0);
    return true;
  }
#endif  // ROCKSDB_FALLOCATE_PRESENT

  uint64_t GetAllocatedFileSize(std::string file_name) {
    struct stat sbuf;
    int err = stat(file_name.c_str(), &sbuf);
    assert(err == 0);
    return sbuf.st_blocks * 512;
  }

#if defined(ROCKSDB_FALLOCATE_PRESENT)
  bool ShouldSkipAllocationCheck(const std::string& file_name) {
    (void)file_name;
#if defined(OS_LINUX)
    struct statfs fs_stat;
    if (statfs(file_name.c_str(), &fs_stat) == 0) {
      if (fs_stat.f_type ==
          static_cast<decltype(fs_stat.f_type)>(BTRFS_SUPER_MAGIC)) {
        return true;
      } else if (fs_stat.f_type ==
                 static_cast<decltype(fs_stat.f_type)>(ZFS_SUPER_MAGIC)) {
        return true;
      } else if (fs_stat.f_type ==
                 static_cast<decltype(fs_stat.f_type)>(TMPFS_MAGIC)) {
        return true;
      } else if (fs_stat.f_type ==
                 static_cast<decltype(fs_stat.f_type)>(OVERLAYFS_SUPER_MAGIC)) {
        return true;
      }
    }
#endif
    return false;
  }
#endif  // ROCKSDB_FALLOCATE_PRESENT
#endif  // ROCKSDB_PLATFORM_POSIX
};

class DBWALTest : public DBWALTestBase {
 public:
  DBWALTest() : DBWALTestBase("/db_wal_test") {}
};

// A SpecialEnv enriched to give more insight about deleted files
class EnrichedSpecialEnv : public SpecialEnv {
 public:
  explicit EnrichedSpecialEnv(Env* base) : SpecialEnv(base) {}
  Status NewSequentialFile(const std::string& f,
                           std::unique_ptr<SequentialFile>* r,
                           const EnvOptions& soptions) override {
    InstrumentedMutexLock l(&env_mutex_);
    if (f == skipped_wal) {
      deleted_wal_reopened = true;
      if (IsWAL(f) && largest_deleted_wal.size() != 0 &&
          f.compare(largest_deleted_wal) <= 0) {
        gap_in_wals = true;
      }
    }
    return SpecialEnv::NewSequentialFile(f, r, soptions);
  }
  Status DeleteFile(const std::string& fname) override {
    if (IsWAL(fname)) {
      deleted_wal_cnt++;
      InstrumentedMutexLock l(&env_mutex_);
      // If this is the first WAL, remember its name and skip deleting it. We
      // remember its name partly because the application might attempt to
      // delete the file again.
      if (skipped_wal.size() != 0 && skipped_wal != fname) {
        if (largest_deleted_wal.size() == 0 ||
            largest_deleted_wal.compare(fname) < 0) {
          largest_deleted_wal = fname;
        }
      } else {
        skipped_wal = fname;
        return Status::OK();
      }
    }
    return SpecialEnv::DeleteFile(fname);
  }
  bool IsWAL(const std::string& fname) {
    // printf("iswal %s\n", fname.c_str());
    return fname.compare(fname.size() - 3, 3, "log") == 0;
  }

  InstrumentedMutex env_mutex_;
  // the wal whose actual delete was skipped by the env
  std::string skipped_wal;
  // the largest WAL that was requested to be deleted
  std::string largest_deleted_wal;
  // number of WALs that were successfully deleted
  std::atomic<size_t> deleted_wal_cnt = {0};
  // the WAL whose delete from fs was skipped is reopened during recovery
  std::atomic<bool> deleted_wal_reopened = {false};
  // whether a gap in the WALs was detected during recovery
  std::atomic<bool> gap_in_wals = {false};
};

class DBWALTestWithEnrichedEnv : public DBTestBase {
 public:
  DBWALTestWithEnrichedEnv()
      : DBTestBase("db_wal_test", /*env_do_fsync=*/true) {
    enriched_env_ = new EnrichedSpecialEnv(env_->target());
    auto options = CurrentOptions();
    options.env = enriched_env_;
    options.allow_2pc = true;
    Reopen(options);
    delete env_;
    // to be deleted by the parent class
    env_ = enriched_env_;
  }

 protected:
  EnrichedSpecialEnv* enriched_env_;
};

// Test that the recovery would successfully avoid the gaps between the logs.
// One known scenario that could cause this is that the application issue the
// WAL deletion out of order. For the sake of simplicity in the test, here we
// create the gap by manipulating the env to skip deletion of the first WAL but
// not the ones after it.
TEST_F(DBWALTestWithEnrichedEnv, SkipDeletedWALs) {
  auto options = last_options_;
  // To cause frequent WAL deletion
  options.write_buffer_size = 128;
  Reopen(options);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::PurgeObsoleteFiles:End",
        "DBWALTestWithEnrichedEnv.SkipDeletedWALs:AfterFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions writeOpt = WriteOptions();
  for (int i = 0; i < 128 * 5; i++) {
    ASSERT_OK(dbfull()->Put(writeOpt, "foo", "v1"));
  }
  FlushOptions fo;
  fo.wait = true;
  ASSERT_OK(db_->Flush(fo));

  TEST_SYNC_POINT("DBWALTestWithEnrichedEnv.SkipDeletedWALs:AfterFlush");

  // some wals are deleted
  ASSERT_NE(0, enriched_env_->deleted_wal_cnt);
  // but not the first one
  ASSERT_NE(0, enriched_env_->skipped_wal.size());

  // Test that the WAL that was not deleted will be skipped during recovery
  options = last_options_;
  Reopen(options);
  ASSERT_FALSE(enriched_env_->deleted_wal_reopened);
  ASSERT_FALSE(enriched_env_->gap_in_wals);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, WAL) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // Both value's should be present.
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v2", Get(1, "foo"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // again both values should be present.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RollLog) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }
    ASSERT_OK(Put(1, "foo", "v4"));
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, SyncWALNotBlockWrite) {
  Options options = CurrentOptions();
  options.max_write_buffer_number = 4;
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo5", "bar5"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"WritableFileWriter::SyncWithoutFlush:1",
       "DBWALTest::SyncWALNotBlockWrite:1"},
      {"DBWALTest::SyncWALNotBlockWrite:2",
       "WritableFileWriter::SyncWithoutFlush:2"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread thread([&]() { ASSERT_OK(db_->SyncWAL()); });

  TEST_SYNC_POINT("DBWALTest::SyncWALNotBlockWrite:1");
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Put("foo3", "bar3"));
  FlushOptions fo;
  fo.wait = false;
  ASSERT_OK(db_->Flush(fo));
  ASSERT_OK(Put("foo4", "bar4"));

  TEST_SYNC_POINT("DBWALTest::SyncWALNotBlockWrite:2");

  thread.join();

  ASSERT_EQ(Get("foo1"), "bar1");
  ASSERT_EQ(Get("foo2"), "bar2");
  ASSERT_EQ(Get("foo3"), "bar3");
  ASSERT_EQ(Get("foo4"), "bar4");
  ASSERT_EQ(Get("foo5"), "bar5");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, SyncWALNotWaitWrite) {
  ASSERT_OK(Put("foo1", "bar1"));
  ASSERT_OK(Put("foo3", "bar3"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"SpecialEnv::SpecialWalFile::Append:1",
       "DBWALTest::SyncWALNotWaitWrite:1"},
      {"DBWALTest::SyncWALNotWaitWrite:2",
       "SpecialEnv::SpecialWalFile::Append:2"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread thread(
      [&]() { ASSERT_OK(Put("foo2", "bar2")); });
  // Moving this to SyncWAL before the actual fsync
  // TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:1");
  ASSERT_OK(db_->SyncWAL());
  // Moving this to SyncWAL after actual fsync
  // TEST_SYNC_POINT("DBWALTest::SyncWALNotWaitWrite:2");

  thread.join();

  ASSERT_EQ(Get("foo1"), "bar1");
  ASSERT_EQ(Get("foo2"), "bar2");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, Recover) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "baz"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Put(1, "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
  } while (ChangeWalOptions());
}

class DBWALTestWithTimestamp
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<test::UserDefinedTimestampTestMode> {
 public:
  DBWALTestWithTimestamp()
      : DBBasicTestWithTimestampBase("db_wal_test_with_timestamp") {}

  Status CreateAndReopenWithTs(const std::vector<std::string>& cfs,
                               const Options& ts_options, bool persist_udt,
                               bool avoid_flush_during_recovery = false) {
    Options default_options = CurrentOptions();
    default_options.allow_concurrent_memtable_write =
        persist_udt ? true : false;
    DestroyAndReopen(default_options);
    CreateColumnFamilies(cfs, ts_options);
    return ReopenColumnFamiliesWithTs(cfs, ts_options, persist_udt,
                                      avoid_flush_during_recovery);
  }

  Status ReopenColumnFamiliesWithTs(const std::vector<std::string>& cfs,
                                    Options ts_options, bool persist_udt,
                                    bool avoid_flush_during_recovery = false) {
    Options default_options = CurrentOptions();
    default_options.create_if_missing = false;
    default_options.use_blog_format_for_wals =
        ts_options.use_blog_format_for_wals;
    default_options.allow_concurrent_memtable_write =
        persist_udt ? true : false;
    default_options.avoid_flush_during_recovery = avoid_flush_during_recovery;
    ts_options.create_if_missing = false;

    std::vector<Options> cf_options(cfs.size(), ts_options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    cf_options.insert(cf_options.begin(), default_options);
    Close();
    return TryReopenWithColumnFamilies(cfs_plus_default, cf_options);
  }

  Status Put(uint32_t cf, const Slice& key, const Slice& ts,
             const Slice& value) {
    WriteOptions write_opts;
    return db_->Put(write_opts, handles_[cf], key, ts, value);
  }

  void CheckGet(const ReadOptions& read_opts, uint32_t cf, const Slice& key,
                const std::string& expected_value,
                const std::string& expected_ts) {
    std::string actual_value;
    std::string actual_ts;
    ASSERT_OK(
        db_->Get(read_opts, handles_[cf], key, &actual_value, &actual_ts));
    ASSERT_EQ(expected_value, actual_value);
    ASSERT_EQ(expected_ts, actual_ts);
  }
};

TEST_P(DBWALTestWithTimestamp, RecoverAndNoFlush) {
  // Set up the option that enables user defined timestmp size.
  std::string ts1;
  PutFixed64(&ts1, 1);
  Options ts_options;
  ts_options.create_if_missing = true;
  ts_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  // Test that user-defined timestamps are recovered from WAL regardless of
  // the value of this flag because UDTs are saved in WAL nonetheless.
  // We however need to explicitly disable flush during recovery by setting
  // `avoid_flush_during_recovery=true` so that we can avoid timestamps getting
  // stripped when the `persist_user_defined_timestamps` flag is false, so that
  // all written timestamps are available for testing user-defined time travel
  // read.
  bool persist_udt = test::ShouldPersistUDT(GetParam());
  ts_options.persist_user_defined_timestamps = persist_udt;
  bool avoid_flush_during_recovery = true;

  std::string full_history_ts_low;
  ReadOptions read_opts;
  do {
    Slice ts_slice = ts1;
    read_opts.timestamp = &ts_slice;
    ASSERT_OK(CreateAndReopenWithTs({"pikachu"}, ts_options, persist_udt,
                                    avoid_flush_during_recovery));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
    ASSERT_OK(Put(1, "foo", ts1, "v1"));
    ASSERT_OK(Put(1, "baz", ts1, "v5"));

    ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, ts_options, persist_udt,
                                         avoid_flush_during_recovery));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
    // Do a timestamped read with ts1 after second reopen.
    CheckGet(read_opts, 1, "foo", "v1", ts1);
    CheckGet(read_opts, 1, "baz", "v5", ts1);

    // Write more value versions for key "foo" and "bar" before and after second
    // reopen.
    std::string ts2;
    PutFixed64(&ts2, 2);
    ASSERT_OK(Put(1, "bar", ts2, "v2"));
    ASSERT_OK(Put(1, "foo", ts2, "v3"));

    ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, ts_options, persist_udt,
                                         avoid_flush_during_recovery));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
    std::string ts3;
    PutFixed64(&ts3, 3);
    ASSERT_OK(Put(1, "foo", ts3, "v4"));

    // All the key value pairs available for read:
    // "foo" -> [(ts1, "v1"), (ts2, "v3"), (ts3, "v4")]
    // "bar" -> [(ts2, "v2")]
    // "baz" -> [(ts1, "v5")]
    // Do a timestamped read with ts1 after third reopen.
    // read_opts.timestamp is set to ts1 for below reads
    CheckGet(read_opts, 1, "foo", "v1", ts1);
    std::string value;
    ASSERT_TRUE(db_->Get(read_opts, handles_[1], "bar", &value).IsNotFound());
    CheckGet(read_opts, 1, "baz", "v5", ts1);

    // Do a timestamped read with ts2 after third reopen.
    ts_slice = ts2;
    // read_opts.timestamp is set to ts2 for below reads.
    CheckGet(read_opts, 1, "foo", "v3", ts2);
    CheckGet(read_opts, 1, "bar", "v2", ts2);
    CheckGet(read_opts, 1, "baz", "v5", ts1);

    // Do a timestamped read with ts3 after third reopen.
    ts_slice = ts3;
    // read_opts.timestamp is set to ts3 for below reads.
    CheckGet(read_opts, 1, "foo", "v4", ts3);
    CheckGet(read_opts, 1, "bar", "v2", ts2);
    CheckGet(read_opts, 1, "baz", "v5", ts1);
    ASSERT_OK(db_->GetFullHistoryTsLow(handles_[1], &full_history_ts_low));
    ASSERT_TRUE(full_history_ts_low.empty());
  } while (ChangeWalOptions());
}

TEST_P(DBWALTestWithTimestamp, RecoverAndFlush) {
  // Set up the option that enables user defined timestamp size.
  std::string min_ts;
  std::string write_ts;
  PutFixed64(&min_ts, 0);
  PutFixed64(&write_ts, 1);
  Options ts_options;
  ts_options.create_if_missing = true;
  ts_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  bool persist_udt = test::ShouldPersistUDT(GetParam());
  ts_options.persist_user_defined_timestamps = persist_udt;

  std::string smallest_ukey_without_ts = "baz";
  std::string largest_ukey_without_ts = "foo";

  ASSERT_OK(CreateAndReopenWithTs({"pikachu"}, ts_options, persist_udt));
  // No flush, no sst files, because of no data.
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 0U);
  ASSERT_OK(Put(1, largest_ukey_without_ts, write_ts, "v1"));
  ASSERT_OK(Put(1, smallest_ukey_without_ts, write_ts, "v5"));

  ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, ts_options, persist_udt));
  // Memtable recovered from WAL flushed because `avoid_flush_during_recovery`
  // defaults to false, created one L0 file.
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"), 1U);

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(handles_[1], &level_to_files);
  std::string full_history_ts_low;
  ASSERT_OK(db_->GetFullHistoryTsLow(handles_[1], &full_history_ts_low));
  ASSERT_GT(level_to_files.size(), 1);
  // L0 only has one SST file.
  ASSERT_EQ(level_to_files[0].size(), 1);
  auto meta = level_to_files[0][0];
  if (persist_udt) {
    ASSERT_EQ(smallest_ukey_without_ts + write_ts, meta.smallest.user_key());
    ASSERT_EQ(largest_ukey_without_ts + write_ts, meta.largest.user_key());
    ASSERT_TRUE(full_history_ts_low.empty());
  } else {
    ASSERT_EQ(smallest_ukey_without_ts + min_ts, meta.smallest.user_key());
    ASSERT_EQ(largest_ukey_without_ts + min_ts, meta.largest.user_key());
    std::string effective_cutoff;
    Slice write_ts_slice = write_ts;
    GetFullHistoryTsLowFromU64CutoffTs(&write_ts_slice, &effective_cutoff);
    ASSERT_EQ(effective_cutoff, full_history_ts_low);
  }
}

TEST_P(DBWALTestWithTimestamp, RecoverBlogWal) {
  std::string timestamp;
  PutFixed64(&timestamp, 1);
  Options ts_options;
  ts_options.create_if_missing = true;
  ts_options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  ts_options.persist_user_defined_timestamps =
      test::ShouldPersistUDT(GetParam());
  ts_options.use_blog_format_for_wals = true;

  ASSERT_OK(CreateAndReopenWithTs({"pikachu"}, ts_options,
                                  ts_options.persist_user_defined_timestamps,
                                  /*avoid_flush_during_recovery=*/true));
  ASSERT_OK(Put(1, "key", timestamp, "value"));
  ASSERT_OK(ReopenColumnFamiliesWithTs(
      {"pikachu"}, ts_options, ts_options.persist_user_defined_timestamps,
      /*avoid_flush_during_recovery=*/true));

  Slice read_timestamp = timestamp;
  ReadOptions read_options;
  read_options.timestamp = &read_timestamp;
  CheckGet(read_options, 1, "key", "value", timestamp);
}

// Param 0: test mode for the user-defined timestamp feature
INSTANTIATE_TEST_CASE_P(
    P, DBWALTestWithTimestamp,
    ::testing::Values(
        test::UserDefinedTimestampTestMode::kStripUserDefinedTimestamp,
        test::UserDefinedTimestampTestMode::kNormal));

TEST_F(DBWALTestWithTimestamp, EnableDisableUDT) {
  Options options;
  options.create_if_missing = true;
  options.comparator = BytewiseComparator();
  bool avoid_flush_during_recovery = true;
  ASSERT_OK(CreateAndReopenWithTs({"pikachu"}, options, true /* persist_udt */,
                                  avoid_flush_during_recovery));

  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "foo", "v1"));
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "baz", "v5"));

  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = false;
  // Test handle timestamp size inconsistency in WAL when enabling user-defined
  // timestamps.
  ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, options,
                                       false /* persist_udt */,
                                       avoid_flush_during_recovery));

  std::string ts;
  PutFixed64(&ts, 0);
  Slice ts_slice = ts;
  ReadOptions read_opts;
  read_opts.timestamp = &ts_slice;
  // Pre-existing entries are treated as if they have the min timestamp.
  CheckGet(read_opts, 1, "foo", "v1", ts);
  CheckGet(read_opts, 1, "baz", "v5", ts);
  ts.clear();
  PutFixed64(&ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "foo", ts, "v2"));
  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "baz", ts, "v6"));
  CheckGet(read_opts, 1, "foo", "v2", ts);
  CheckGet(read_opts, 1, "baz", "v6", ts);

  options.comparator = BytewiseComparator();
  // Open the column family again with the UDT feature disabled. Test handle
  // timestamp size inconsistency in WAL when disabling user-defined timestamps
  ASSERT_OK(ReopenColumnFamiliesWithTs({"pikachu"}, options,
                                       true /* persist_udt */,
                                       avoid_flush_during_recovery));
  ASSERT_EQ("v2", Get(1, "foo"));
  ASSERT_EQ("v6", Get(1, "baz"));
}

TEST_F(DBWALTest, RecoverWithTableHandle) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.avoid_flush_during_recovery = false;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "foo", "v3"));
    ASSERT_OK(Put(1, "bar", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "big", std::string(100, 'a')));

    options = CurrentOptions();
    const int kSmallMaxOpenFiles = 13;
    if (option_config_ == kDBLogDir) {
      // Use this option to check not preloading files
      // Set the max open files to be small enough so no preload will
      // happen.
      options.max_open_files = kSmallMaxOpenFiles;
      // RocksDB sanitize max open files to at least 20. Modify it back.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
            int* max_open_files = static_cast<int*>(arg);
            *max_open_files = kSmallMaxOpenFiles;
          });

    } else if (option_config_ == kWalDirAndMmapReads) {
      // Use this option to check always loading all files.
      options.max_open_files = 100;
    } else {
      options.max_open_files = -1;
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

    std::vector<std::vector<FileMetaData>> files;
    dbfull()->TEST_GetFilesMetaData(handles_[1], &files);
    size_t total_files = 0;
    for (const auto& level : files) {
      total_files += level.size();
    }
    ASSERT_EQ(total_files, 3);
    for (const auto& level : files) {
      for (const auto& file : level) {
        if (options.max_open_files == kSmallMaxOpenFiles) {
          ASSERT_TRUE(file.fd.pinned_reader.Get() == nullptr);
        } else {
          ASSERT_TRUE(file.fd.pinned_reader.Get() != nullptr);
        }
      }
    }
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RecoverWithBlob) {
  // Write a value that's below the prospective size limit for blobs and another
  // one that's above. Note that blob files are not actually enabled at this
  // point.
  constexpr uint64_t min_blob_size = 10;

  constexpr char short_value[] = "short";
  static_assert(sizeof(short_value) - 1 < min_blob_size,
                "short_value too long");

  constexpr char long_value[] = "long_value";
  static_assert(sizeof(long_value) - 1 >= min_blob_size,
                "long_value too short");

  ASSERT_OK(Put("key1", short_value));
  ASSERT_OK(Put("key2", long_value));

  // There should be no files just yet since we haven't flushed.
  {
    VersionSet* const versions = dbfull()->GetVersionSet();
    ASSERT_NE(versions, nullptr);

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd, nullptr);

    Version* const current = cfd->current();
    ASSERT_NE(current, nullptr);

    const VersionStorageInfo* const storage_info = current->storage_info();
    ASSERT_NE(storage_info, nullptr);

    ASSERT_EQ(storage_info->num_non_empty_levels(), 0);
    ASSERT_TRUE(storage_info->GetBlobFiles().empty());
  }

  // Reopen the database with blob files enabled. A new table file/blob file
  // pair should be written during recovery.
  Options options;
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.avoid_flush_during_recovery = false;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  ASSERT_EQ(Get("key1"), short_value);
  ASSERT_EQ(Get("key2"), long_value);

  VersionSet* const versions = dbfull()->GetVersionSet();
  ASSERT_NE(versions, nullptr);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_EQ(l0_files.size(), 1);

  const FileMetaData* const table_file = l0_files[0];
  ASSERT_NE(table_file, nullptr);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  const auto& blob_file = blob_files.front();
  ASSERT_NE(blob_file, nullptr);

  ASSERT_EQ(table_file->smallest.user_key(), "key1");
  ASSERT_EQ(table_file->largest.user_key(), "key2");
  ASSERT_EQ(table_file->fd.smallest_seqno, 1);
  ASSERT_EQ(table_file->fd.largest_seqno, 2);
  ASSERT_EQ(table_file->oldest_blob_file_number,
            blob_file->GetBlobFileNumber());

  ASSERT_EQ(blob_file->GetTotalBlobCount(), 1);

  const InternalStats* const internal_stats = cfd->internal_stats();
  ASSERT_NE(internal_stats, nullptr);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_FALSE(compaction_stats.empty());
  ASSERT_EQ(compaction_stats[0].bytes_written, table_file->fd.GetFileSize());
  ASSERT_EQ(compaction_stats[0].bytes_written_blob,
            blob_file->GetTotalBlobBytes());
  ASSERT_EQ(compaction_stats[0].num_output_files, 1);
  ASSERT_EQ(compaction_stats[0].num_output_files_blob, 1);

  const uint64_t* const cf_stats_value = internal_stats->TEST_GetCFStatsValue();
  ASSERT_EQ(cf_stats_value[InternalStats::BYTES_FLUSHED],
            compaction_stats[0].bytes_written +
                compaction_stats[0].bytes_written_blob);
}

TEST_F(DBWALTest, RecoverWithBlobMultiSST) {
  // Write several large (4 KB) values without flushing. Note that blob files
  // are not actually enabled at this point.
  std::string large_value(1 << 12, 'a');

  constexpr int num_keys = 64;

  for (int i = 0; i < num_keys; ++i) {
    ASSERT_OK(Put(Key(i), large_value));
  }

  // There should be no files just yet since we haven't flushed.
  {
    VersionSet* const versions = dbfull()->GetVersionSet();
    ASSERT_NE(versions, nullptr);

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(cfd, nullptr);

    Version* const current = cfd->current();
    ASSERT_NE(current, nullptr);

    const VersionStorageInfo* const storage_info = current->storage_info();
    ASSERT_NE(storage_info, nullptr);

    ASSERT_EQ(storage_info->num_non_empty_levels(), 0);
    ASSERT_TRUE(storage_info->GetBlobFiles().empty());
  }

  // Reopen the database with blob files enabled and write buffer size set to a
  // smaller value. Multiple table files+blob files should be written and added
  // to the Version during recovery.
  Options options;
  options.write_buffer_size = 1 << 16;  // 64 KB
  options.enable_blob_files = true;
  options.avoid_flush_during_recovery = false;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  for (int i = 0; i < num_keys; ++i) {
    ASSERT_EQ(Get(Key(i)), large_value);
  }

  VersionSet* const versions = dbfull()->GetVersionSet();
  ASSERT_NE(versions, nullptr);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_GT(l0_files.size(), 1);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_GT(blob_files.size(), 1);

  ASSERT_EQ(l0_files.size(), blob_files.size());
}

TEST_F(DBWALTest, WALWithChecksumHandoff) {
#ifndef ROCKSDB_ASSERT_STATUS_CHECKED
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  // Close the DB before the local Env is destroyed if an ASSERT exits early.
  Defer close_db_on_exit([this]() { Close(); });
  do {
    Options options = CurrentOptions();

    options.checksum_handoff_file_types.Add(FileType::kWalFile);
    options.env = fault_fs_env.get();
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);

    CreateAndReopenWithCF({"pikachu"}, options);
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    // Both value's should be present.
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v2", Get(1, "foo"));

    writeOpt.disableWAL = true;
    // This put, data is persisted by Flush
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    writeOpt.disableWAL = false;
    // Data is persisted in the WAL
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "zoo", "v3"));
    ASSERT_OK(dbfull()->SyncWAL());
    // The hash does not match, write fails
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
    writeOpt.disableWAL = false;
    ASSERT_NOK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    // Due to the write failure, Get should not find
    ASSERT_NE("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "zoo"));
    ASSERT_EQ("v3", Get(1, "bar"));

    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
    // Each write will be similated as corrupted.
    fault_fs->IngestDataCorruptionBeforeWrite();
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v4"));
    writeOpt.disableWAL = false;
    ASSERT_NOK(dbfull()->Put(writeOpt, handles_[1], "foo", "v4"));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_NE("v4", Get(1, "foo"));
    ASSERT_NE("v4", Get(1, "bar"));
    fault_fs->NoDataCorruptionBeforeWrite();

    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
    // The file system does not provide checksum method and verification.
    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v5"));
    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v5"));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v5", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "bar"));

    Destroy(options);
  } while (ChangeWalOptions());
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
}

TEST_F(DBWALTest, LockWal) {
  do {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);

    ASSERT_OK(Put("foo", "v"));
    ASSERT_OK(Put("bar", "v"));

    ASSERT_OK(db_->LockWAL());
    // Verify writes are stopped
    WriteOptions wopts;
    wopts.no_slowdown = true;
    Status s = db_->Put(wopts, "foo", "dontcare");
    ASSERT_TRUE(s.IsIncomplete());
    {
      VectorLogPtr wals;
      ASSERT_OK(db_->GetSortedWalFiles(wals));
      ASSERT_FALSE(wals.empty());
    }
    port::Thread worker([&]() {
      Status tmp_s = db_->Flush(FlushOptions());
      ASSERT_OK(tmp_s);
    });
    FlushOptions flush_opts;
    flush_opts.wait = false;
    s = db_->Flush(flush_opts);
    ASSERT_TRUE(s.IsTryAgain());
    ASSERT_OK(db_->UnlockWAL());
    ASSERT_OK(db_->Put(WriteOptions(), "foo", "dontcare"));

    worker.join();
  } while (ChangeWalOptions());
}

class DBRecoveryTestBlobError
    : public DBWALTest,
      public testing::WithParamInterface<std::string> {
 public:
  DBRecoveryTestBlobError() : sync_point_(GetParam()) {}

  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(
    DBRecoveryTestBlobError, DBRecoveryTestBlobError,
    ::testing::ValuesIn(std::vector<std::string>{
        "BlobFileBuilder::WriteBlobToFile:AddRecord",
        "BlobFileBuilder::WriteBlobToFile:LegacyAppendFooterAndClose"}));

TEST_P(DBRecoveryTestBlobError, RecoverWithBlobError) {
  // Write a value. Note that blob files are not actually enabled at this point.
  ASSERT_OK(Put("key", "blob"));

  // Reopen with blob files enabled but make blob file writing fail during
  // recovery.
  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.enable_blob_files = true;
  options.avoid_flush_during_recovery = false;
  options.disable_auto_compactions = true;
  options.env = env_;

  ASSERT_NOK(TryReopen(options));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Make sure the files generated by the failed recovery have been deleted.
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    uint64_t number = 0;
    FileType type = kTableFile;

    if (!ParseFileName(file, &number, &type)) {
      continue;
    }

    ASSERT_NE(type, kTableFile);
    ASSERT_NE(type, kBlobFile);
  }
}

TEST_F(DBWALTest, IgnoreRecoveredLog) {
  std::string backup_logs = dbname_ + "/backup_logs";

  do {
    // delete old files in backup_logs directory
    ASSERT_OK(env_->CreateDirIfMissing(backup_logs));
    std::vector<std::string> old_files;
    ASSERT_OK(env_->GetChildren(backup_logs, &old_files));
    for (auto& file : old_files) {
      ASSERT_OK(env_->DeleteFile(backup_logs + "/" + file));
    }
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.merge_operator = MergeOperators::CreateUInt64AddOperator();
    options.wal_dir = dbname_ + "/logs";
    DestroyAndReopen(options);

    // fill up the DB
    std::string one;
    std::string two;
    PutFixed64(&one, 1);
    PutFixed64(&two, 2);
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("foo"), Slice(one)));
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("foo"), Slice(one)));
    ASSERT_OK(db_->Merge(WriteOptions(), Slice("bar"), Slice(one)));

    // copy the logs to backup
    std::vector<std::string> logs;
    ASSERT_OK(env_->GetChildren(options.wal_dir, &logs));
    for (auto& log : logs) {
      CopyFile(options.wal_dir + "/" + log, backup_logs + "/" + log);
    }

    // recover the DB
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();

    // copy the logs from backup back to wal dir
    for (auto& log : logs) {
      CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
    }
    // this should ignore the log files, recovery should not happen again
    // if the recovery happens, the same merge operator would be called twice,
    // leading to incorrect results
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));
    Close();
    Destroy(options);
    Reopen(options);
    Close();

    // copy the logs from backup back to wal dir
    ASSERT_OK(env_->CreateDirIfMissing(options.wal_dir));
    for (auto& log : logs) {
      CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
    }
    // assert that we successfully recovered only from logs, even though we
    // destroyed the DB
    Reopen(options);
    ASSERT_EQ(two, Get("foo"));
    ASSERT_EQ(one, Get("bar"));

    // Recovery will fail if DB directory doesn't exist.
    Destroy(options);
    // copy the logs from backup back to wal dir
    ASSERT_OK(env_->CreateDirIfMissing(options.wal_dir));
    for (auto& log : logs) {
      CopyFile(backup_logs + "/" + log, options.wal_dir + "/" + log);
      // we won't be needing this file no more
      ASSERT_OK(env_->DeleteFile(backup_logs + "/" + log));
    }
    Status s = TryReopen(options);
    ASSERT_NOK(s);
    Destroy(options);
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RecoveryWithEmptyLog) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v3"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v3", Get(1, "foo"));
  } while (ChangeWalOptions());
}

#if !(defined NDEBUG) || !defined(OS_WIN)
TEST_F(DBWALTest, PreallocateBlock) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1000 * 1000;
  options.max_total_wal_size = 0;

  size_t expected_preallocation_size = static_cast<size_t>(
      options.write_buffer_size + options.write_buffer_size / 10);

  DestroyAndReopen(options);

  std::atomic<int> called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());

  options.max_total_wal_size = 1000 * 1000;
  expected_preallocation_size = static_cast<size_t>(options.max_total_wal_size);
  Reopen(options);
  called.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());

  options.db_write_buffer_size = 800 * 1000;
  expected_preallocation_size =
      static_cast<size_t>(options.db_write_buffer_size);
  Reopen(options);
  called.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());

  expected_preallocation_size = 700 * 1000;
  std::shared_ptr<WriteBufferManager> write_buffer_manager =
      std::make_shared<WriteBufferManager>(static_cast<uint64_t>(700 * 1000));
  options.write_buffer_manager = write_buffer_manager;
  Reopen(options);
  called.store(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTestWalFile.GetPreallocationStatus", [&](void* arg) {
        ASSERT_TRUE(arg != nullptr);
        size_t preallocation_size = *(static_cast<size_t*>(arg));
        ASSERT_EQ(expected_preallocation_size, preallocation_size);
        called.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("", ""));
  Close();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(2, called.load());
}
#endif  // !(defined NDEBUG) || !defined(OS_WIN)

TEST_F(DBWALTest, FullPurgePreservesRecycledLog) {
  // For github issue #1303
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.recycle_log_file_num = 2;
    options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    if (i != 0) {
      options.wal_dir = alternative_wal_dir_;
    }

    DestroyAndReopen(options);
    ASSERT_OK(Put("foo", "v1"));
    VectorLogPtr log_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    ASSERT_GT(log_files.size(), 0);
    ASSERT_OK(Flush());

    // Now the original WAL is in log_files[0] and should be marked for
    // recycling.
    // Verify full purge cannot remove this file.
    JobContext job_context(0);
    dbfull()->TEST_LockMutex();
    dbfull()->FindObsoleteFiles(&job_context, true /* force */);
    dbfull()->TEST_UnlockMutex();
    dbfull()->PurgeObsoleteFiles(job_context);
    job_context.Clean();

    if (i == 0) {
      ASSERT_OK(
          env_->FileExists(LogFileName(dbname_, log_files[0]->LogNumber())));
    } else {
      ASSERT_OK(env_->FileExists(
          LogFileName(alternative_wal_dir_, log_files[0]->LogNumber())));
    }
  }
}

TEST_F(DBWALTest, FullPurgePreservesLogPendingReuse) {
  // Ensures full purge cannot delete a WAL while it's in the process of being
  // recycled. In particular, we force the full purge after a file has been
  // chosen for reuse, but before it has been renamed.
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.recycle_log_file_num = 1;
    options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    if (i != 0) {
      options.wal_dir = alternative_wal_dir_;
    }
    DestroyAndReopen(options);

    // The first flush creates a second log so writes can continue before the
    // flush finishes.
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Flush());
    // The first log can be recycled only after the flush closes its writer.
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

    // The second flush can recycle the first log. Sync points enforce the
    // full purge happens after choosing the log to recycle and before it is
    // renamed.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"DBImpl::CreateWAL:BeforeReuseWritableFile1",
         "DBWALTest::FullPurgePreservesLogPendingReuse:PreFullPurge"},
        {"DBWALTest::FullPurgePreservesLogPendingReuse:PostFullPurge",
         "DBImpl::CreateWAL:BeforeReuseWritableFile2"},
    });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ROCKSDB_NAMESPACE::port::Thread thread([&]() {
      TEST_SYNC_POINT(
          "DBWALTest::FullPurgePreservesLogPendingReuse:PreFullPurge");
      ASSERT_OK(db_->EnableFileDeletions());
      TEST_SYNC_POINT(
          "DBWALTest::FullPurgePreservesLogPendingReuse:PostFullPurge");
    });
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Flush());
    thread.join();
  }
}

TEST_F(DBWALTest, RecycledWalKeepsSyncedWritesWhenOldWriterCloses) {
  // The flush pauses either before it removes WAL A from logs_, or after it
  // removed A but before it frees the writers it released.
  for (const char* pause_point : {"FindObsoleteFiles::PostMutexUnlock",
                                  "DBImpl::BackgroundCallFlush:FilesFound"}) {
    for (bool background_close : {false, true}) {
      for (bool sync_old_wal_before_flush : {false, true}) {
        SCOPED_TRACE(std::string("pause_point=") + pause_point +
                     " background_close_inactive_wals=" +
                     std::to_string(background_close) +
                     " sync_old_wal_before_flush=" +
                     std::to_string(sync_old_wal_before_flush));
        Options options = CurrentOptions();
        options.recycle_log_file_num = 1;
        options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
        options.max_background_flushes = 1;
        options.background_close_inactive_wals = background_close;
        DestroyAndReopen(options);
        ASSERT_OK(Put("a", "small"));

        // The flush of WAL A's memtable waits until A may have been synced as
        // an inactive WAL. It then puts A on the recycle list and pauses at
        // pause_point.
        std::atomic<bool> flush_started{false};
        std::promise<void> old_wal_synced;
        std::shared_future<void> old_wal_synced_future =
            old_wal_synced.get_future().share();
        std::atomic<bool> paused{false};
        std::promise<void> reached;
        std::promise<void> resume;
        std::shared_future<void> resume_future = resume.get_future().share();
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCallFlush:start", [&](void*) {
              if (!flush_started.exchange(true)) {
                old_wal_synced_future.wait();
              }
            });
        SyncPoint::GetInstance()->SetCallBack(pause_point, [&](void*) {
          if (!paused.exchange(true)) {
            reached.set_value();
            resume_future.wait();
          }
        });
        SyncPoint::GetInstance()->EnableProcessing();
        FlushOptions no_wait;
        no_wait.wait = false;
        ASSERT_OK(db_->Flush(no_wait));
        WriteOptions sync_write;
        sync_write.sync = true;
        if (sync_old_wal_before_flush) {
          ASSERT_OK(db_->Put(sync_write, "c", "small"));
        }
        old_wal_synced.set_value();
        reached.get_future().wait();

        // The next memtable switch reuses A as WAL C, and a synced write lands
        // in C past A's old size.
        ASSERT_OK(Put("b", "small"));
        ASSERT_OK(db_->Flush(no_wait));
        const std::string big_value(64 * 1024, 'v');
        ASSERT_OK(db_->Put(sync_write, "big", big_value));

        resume.set_value();
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
        ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearAllCallBacks();

        Reopen(options);
        EXPECT_EQ(big_value, Get("big"));
      }
    }
  }
}

TEST_F(DBWALTest, GetSortedWalFiles) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    VectorLogPtr log_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    ASSERT_EQ(0, log_files.size());

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    ASSERT_EQ(1, log_files.size());
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, GetCurrentWalFile) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

    std::unique_ptr<LogFile>* bad_log_file = nullptr;
    ASSERT_NOK(dbfull()->GetCurrentWalFile(bad_log_file));

    std::unique_ptr<LogFile> log_file;
    ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));

    // nothing has been written to the log yet
    ASSERT_EQ(log_file->StartSequence(), 0);
    ASSERT_EQ(log_file->SizeFileBytes(), 0);
    ASSERT_EQ(log_file->Type(), kAliveLogFile);
    ASSERT_GT(log_file->LogNumber(), 0);

    // add some data and verify that the file size actually moves foward
    ASSERT_OK(Put(0, "foo", "v1"));
    ASSERT_OK(Put(0, "foo2", "v2"));
    ASSERT_OK(Put(0, "foo3", "v3"));

    ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));

    ASSERT_EQ(log_file->StartSequence(), 0);
    ASSERT_GT(log_file->SizeFileBytes(), 0);
    ASSERT_EQ(log_file->Type(), kAliveLogFile);
    ASSERT_GT(log_file->LogNumber(), 0);

    // force log files to cycle and add some more data, then check if
    // log number moves forward

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    for (int i = 0; i < 10; i++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    }

    ASSERT_OK(Put(0, "foo4", "v4"));
    ASSERT_OK(Put(0, "foo5", "v5"));
    ASSERT_OK(Put(0, "foo6", "v6"));

    ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));

    ASSERT_EQ(log_file->StartSequence(), 0);
    ASSERT_GT(log_file->SizeFileBytes(), 0);
    ASSERT_EQ(log_file->Type(), kAliveLogFile);
    ASSERT_GT(log_file->LogNumber(), 0);

  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, BlogWalHeaderTimestampSizeFromOption) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.blog_timestamp_size = 8;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "value"));

  std::unique_ptr<LogFile> wal_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, wal_file->LogNumber()),
                             &contents));
  Slice input(contents);
  BlogFileHeader header;
  ASSERT_OK(header.DecodeFrom(&input));
  uint64_t timestamp_size = 0;
  ASSERT_TRUE(
      header.GetUint64Property(kBlogPropTimestampSize, &timestamp_size));
  ASSERT_EQ(timestamp_size, 8);

  // Default zero omits the property on the next WAL.
  options.blog_timestamp_size = 0;
  Reopen(options);
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, wal_file->LogNumber()),
                             &contents));
  Slice input2(contents);
  ASSERT_OK(header.DecodeFrom(&input2));
  ASSERT_FALSE(header.HasProperty(kBlogPropTimestampSize));

  options.blog_timestamp_size =
      size_t{std::numeric_limits<uint16_t>::max()} + 1;
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
}

TEST_F(DBWALTest, RecoveryWithLogDataForSomeCFs) {
  // Test for regression of WAL cleanup missing files that don't contain data
  // for every column family.
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "foo", "v2"));
    uint64_t earliest_log_nums[2];
    for (int i = 0; i < 2; ++i) {
      if (i > 0) {
        ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
      }
      VectorLogPtr log_files;
      ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
      if (log_files.size() > 0) {
        earliest_log_nums[i] = log_files[0]->LogNumber();
      } else {
        earliest_log_nums[i] = std::numeric_limits<uint64_t>::max();
      }
    }
    // Check at least the first WAL was cleaned up during the recovery.
    ASSERT_LT(earliest_log_nums[0], earliest_log_nums[1]);
  } while (ChangeWalOptions());
}

TEST_F(DBWALTest, RecoverWithLargeLog) {
  do {
    {
      Options options = CurrentOptions();
      CreateAndReopenWithCF({"pikachu"}, options);
      ASSERT_OK(Put(1, "big1", std::string(200000, '1')));
      ASSERT_OK(Put(1, "big2", std::string(200000, '2')));
      ASSERT_OK(Put(1, "small3", std::string(10, '3')));
      ASSERT_OK(Put(1, "small4", std::string(10, '4')));
      ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    }

    // Make sure that if we re-open with a small write buffer size that
    // we flush table files in the middle of a large log file.
    Options options;
    options.write_buffer_size = 100000;
    options = CurrentOptions(options);
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 3);
    ASSERT_EQ(std::string(200000, '1'), Get(1, "big1"));
    ASSERT_EQ(std::string(200000, '2'), Get(1, "big2"));
    ASSERT_EQ(std::string(10, '3'), Get(1, "small3"));
    ASSERT_EQ(std::string(10, '4'), Get(1, "small4"));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 1);
  } while (ChangeWalOptions());
}

// In https://reviews.facebook.net/D20661 we change
// recovery behavior: previously for each log file each column family
// memtable was flushed, even it was empty. Now it's changed:
// we try to create the smallest number of table files by merging
// updates from multiple logs
TEST_F(DBWALTest, RecoverCheckFileAmountWithSmallWriteBuffer) {
  Options options = CurrentOptions();
  options.write_buffer_size = 5000000;
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  // Since we will reopen DB with smaller write_buffer_size,
  // each key will go to new SST file
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));
  ASSERT_OK(Put(1, Key(10), DummyString(1000000)));

  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  // Make 'dobrynia' to be flushed and new WAL file to be created
  ASSERT_OK(Put(2, Key(10), DummyString(7500000)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[2]));
  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(1));
    // Make sure 'dobrynia' was flushed: check sst files amount
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(1));
  }
  // New WAL file
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));
  ASSERT_OK(Put(3, Key(10), DummyString(1)));

  options.write_buffer_size = 4096;
  options.arena_block_size = 4096;
  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    // No inserts => default is empty
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(0));
    // First 4 keys goes to separate SSTs + 1 more SST for 2 smaller keys
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(5));
    // 1 SST for big key + 1 SST for small one
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(2));
    // 1 SST for all keys
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(1));
  }
}

// In https://reviews.facebook.net/D20661 we change
// recovery behavior: previously for each log file each column family
// memtable was flushed, even it wasn't empty. Now it's changed:
// we try to create the smallest number of table files by merging
// updates from multiple logs
TEST_F(DBWALTest, RecoverCheckFileAmount) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000;
  options.arena_block_size = 4 * 1024;
  options.avoid_flush_during_recovery = false;
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  // Make 'nikitich' memtable to be flushed
  ASSERT_OK(Put(3, Key(10), DummyString(1002400)));
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[3]));
  // 4 memtable are not flushed, 1 sst file
  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(1));
  }
  // Memtable for 'nikitich' has flushed, new WAL file has opened
  // 4 memtable still not flushed

  // Write to new WAL file
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  // Fill up 'nikitich' one more time
  ASSERT_OK(Put(3, Key(10), DummyString(1002400)));
  // make it flush
  ASSERT_OK(Put(3, Key(1), DummyString(1)));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[3]));
  // There are still 4 memtable not flushed, and 2 sst tables
  ASSERT_OK(Put(0, Key(1), DummyString(1)));
  ASSERT_OK(Put(1, Key(1), DummyString(1)));
  ASSERT_OK(Put(2, Key(1), DummyString(1)));

  {
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), static_cast<size_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(2));
  }

  ReopenWithColumnFamilies({"default", "pikachu", "dobrynia", "nikitich"},
                           options);
  {
    std::vector<uint64_t> table_files = ListTableFiles(env_, dbname_);
    // Check, that records for 'default', 'dobrynia' and 'pikachu' from
    // first, second and third WALs  went to the same SST.
    // So, there is 6 SSTs: three  for 'nikitich', one for 'default', one for
    // 'dobrynia', one for 'pikachu'
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(3));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(1));
  }
}

TEST_F(DBWALTest, SyncMultipleLogs) {
  const uint64_t kNumBatches = 2;
  const int kBatchSize = 1000;

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.write_buffer_size = 4096;
  Reopen(options);

  WriteBatch batch;
  WriteOptions wo;
  wo.sync = true;

  for (uint64_t b = 0; b < kNumBatches; b++) {
    batch.Clear();
    for (int i = 0; i < kBatchSize; i++) {
      ASSERT_OK(batch.Put(Key(i), DummyString(128)));
    }

    ASSERT_OK(dbfull()->Write(wo, &batch));
  }

  ASSERT_OK(dbfull()->SyncWAL());
}

TEST_F(DBWALTest, DISABLED_RecycleMultipleWalsCrash) {
  Options options = CurrentOptions();
  options.max_write_buffer_number = 5;
  options.track_and_verify_wals_in_manifest = true;
  options.max_bgerror_resume_count = 0;  // manual resume
  options.recycle_log_file_num = 3;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  // Disable truncating recycled WALs to new size in posix env
  // (approximating a crash)
  SyncPoint::GetInstance()->SetCallBack(
      "PosixWritableFile::Close",
      [](void* arg) { *(static_cast<size_t*>(arg)) = 0; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Re-open with desired options
  DestroyAndReopen(options);
  Defer closer([this]() { Close(); });

  // Ensure WAL recycling wasn't sanitized away
  ASSERT_EQ(db_->GetOptions().recycle_log_file_num,
            options.recycle_log_file_num);

  // Prepare external files for later ingestion
  std::string sst_files_dir = dbname_ + "/sst_files/";
  ASSERT_OK(DestroyDir(env_, sst_files_dir));
  ASSERT_OK(env_->CreateDir(sst_files_dir));
  std::string external_file1 = sst_files_dir + "file1.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(external_file1));
    ASSERT_OK(sst_file_writer.Put("external1", "ex1"));
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));
  }
  std::string external_file2 = sst_files_dir + "file2.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(external_file2));
    ASSERT_OK(sst_file_writer.Put("external2", "ex2"));
    ExternalSstFileInfo file_info;
    ASSERT_OK(sst_file_writer.Finish(&file_info));
  }

  // Populate some WALs to be recycled such that there will be extra data
  // from an old incarnation of the WAL on recovery
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("ignore1", Random::GetTLSInstance()->RandomString(500)));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("ignore2", Random::GetTLSInstance()->RandomString(500)));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->ContinueBackgroundWork());
  ASSERT_OK(Flush());
  ASSERT_OK(Put("ignore3", Random::GetTLSInstance()->RandomString(500)));
  ASSERT_OK(Flush());

  // Verify expected log files (still there for recycling)
  std::vector<FileAttributes> files;
  int log_count = 0;
  ASSERT_OK(options.env->GetChildrenFileAttributes(dbname_, &files));
  for (const auto& f : files) {
    if (EndsWith(f.name, ".log")) {
      EXPECT_GT(f.size_bytes, 500);
      ++log_count;
    }
  }
  EXPECT_EQ(log_count, 3);

  // (Re-used recipe) Generate two inactive WALs and one active WAL, with a
  // gap in sequence numbers to interfere with recovery
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  // Need a gap in sequence numbers, so e.g. ingest external file
  // with an open snapshot
  {
    ManagedSnapshot snapshot(db_.get());
    ASSERT_OK(
        db_->IngestExternalFile({external_file1}, IngestExternalFileOptions()));
  }
  ASSERT_OK(Put("key3", "val3"));
  ASSERT_OK(db_->SyncWAL());
  // Need an SST file that is logically after that WAL, so that dropping WAL
  // data is not a valid point in time.
  {
    ManagedSnapshot snapshot(db_.get());
    ASSERT_OK(
        db_->IngestExternalFile({external_file2}, IngestExternalFileOptions()));
  }

  // Approximate a crash, with respect to recycled WAL data extending past
  // the end of the current WAL data (see SyncPoint callback above)
  Close();

  // Verify recycled log files haven't been truncated
  files.clear();
  log_count = 0;
  ASSERT_OK(options.env->GetChildrenFileAttributes(dbname_, &files));
  for (const auto& f : files) {
    if (EndsWith(f.name, ".log")) {
      EXPECT_GT(f.size_bytes, 500);
      ++log_count;
    }
  }
  EXPECT_EQ(log_count, 3);

  // Verify no data loss after reopen.
  Reopen(options);
  EXPECT_EQ("val1", Get("key1"));
  EXPECT_EQ("val2", Get("key2"));  // Passes because of adjacent seqnos
  EXPECT_EQ("ex1", Get("external1"));
  EXPECT_EQ("val3", Get("key3"));  // <- ONLY FAILURE! (Not a point in time)
  EXPECT_EQ("ex2", Get("external2"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBWALTest, SyncWalPartialFailure) {
  class MyTestFileSystem : public FileSystemWrapper {
   public:
    explicit MyTestFileSystem(std::shared_ptr<FileSystem> base)
        : FileSystemWrapper(std::move(base)) {}

    static const char* kClassName() { return "MyTestFileSystem"; }
    const char* Name() const override { return kClassName(); }
    IOStatus NewWritableFile(const std::string& fname,
                             const FileOptions& file_opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
      IOStatus s = target()->NewWritableFile(fname, file_opts, result, dbg);
      if (s.ok()) {
        *result =
            std::make_unique<MyTestWritableFile>(std::move(*result), *this);
      }
      return s;
    }

    Atomic<uint32_t> syncs_before_failure_{UINT32_MAX};

   protected:
    class MyTestWritableFile : public FSWritableFileOwnerWrapper {
     public:
      MyTestWritableFile(std::unique_ptr<FSWritableFile>&& file,
                         MyTestFileSystem& fs)
          : FSWritableFileOwnerWrapper(std::move(file)), fs_(fs) {}

      IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override {
        int prev_val = fs_.syncs_before_failure_.FetchSub(1);
        if (prev_val == 0) {
          return IOStatus::IOError("fault");
        } else {
          return target()->Sync(options, dbg);
        }
      }

     protected:
      MyTestFileSystem& fs_;
    };
  };

  Options options = CurrentOptions();
  options.max_write_buffer_number = 4;
  options.track_and_verify_wals_in_manifest = true;
  options.max_bgerror_resume_count = 0;  // manual resume

  auto custom_fs =
      std::make_shared<MyTestFileSystem>(options.env->GetFileSystem());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(custom_fs));
  options.env = fault_fs_env.get();
  Reopen(options);
  Defer closer([this]() { Close(); });

  // This is the simplest way to get
  // * one inactive WAL, synced
  // * one inactive WAL, not synced, and
  // * one active WAL, not synced
  // with a single thread, to exercise as much logic as we reasonably can.
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->SyncWAL());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key3", "val3"));

  // Allow 1 of the WALs to sync, but another won't
  custom_fs->syncs_before_failure_.Store(1);
  ASSERT_NOK(db_->SyncWAL());

  // Stuck in this state. (This could previously cause a segfault.)
  ASSERT_NOK(db_->SyncWAL());

  // Can't Resume because WAL write failure is considered non-recoverable,
  // regardless of the IOStatus itself. (Can/should be fixed?)
  ASSERT_NOK(db_->Resume());

  // Verify no data loss after reopen.
  // Also Close() could previously crash in this state.
  Reopen(options);
  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ("val2", Get("key2"));
  ASSERT_EQ("val3", Get("key3"));
}

// Github issue 1339. Prior the fix we read sequence id from the first log to
// a local variable, then keep increase the variable as we replay logs,
// ignoring actual sequence id of the records. This is incorrect if some writes
// come with WAL disabled.
TEST_F(DBWALTest, PartOfWritesWithWALDisabled) {
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.env = fault_env.get();
  options.disable_auto_compactions = true;
  WriteOptions wal_on;
  WriteOptions wal_off;
  wal_on.sync = true;
  wal_on.disableWAL = false;
  wal_off.disableWAL = true;
  CreateAndReopenWithCF({"dummy"}, options);
  ASSERT_OK(Put(1, "dummy", "d1", wal_on));  // seq id 1
  ASSERT_OK(Put(1, "dummy", "d2", wal_off));
  ASSERT_OK(Put(1, "dummy", "d3", wal_off));
  ASSERT_OK(Put(0, "key", "v4", wal_on));  // seq id 4
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(0, "key", "v5", wal_on));  // seq id 5
  ASSERT_EQ("v5", Get(0, "key"));
  ASSERT_OK(dbfull()->FlushWAL(false));
  // Simulate a crash.
  fault_env->SetFilesystemActive(false);
  Close();
  fault_env->ResetState();
  ReopenWithColumnFamilies({"default", "dummy"}, options);
  // Prior to the fix, we may incorrectly recover "v5" with sequence id = 3.
  ASSERT_EQ("v5", Get(0, "key"));
  // Destroy DB before destruct fault_env.
  Destroy(options);
}

//
// Test WAL recovery for the various modes available
//
class RecoveryTestHelper {
 public:
  // Number of WAL files to generate
  static constexpr int kWALFilesCount = 10;
  // Starting number for the WAL file name like 00010.log
  static constexpr int kWALFileOffset = 10;
  // Keys to be written per WAL file
  static constexpr int kKeysPerWALFile = 133;
  // Size of the value
  static constexpr int kValueSize = 96;

  // Create WAL files with values filled in
  static void FillData(DBWALTestBase* test, const Options& options,
                       const size_t wal_count, size_t* count) {
    // Calling internal functions requires sanitized options.
    Options sanitized_options = SanitizeOptions(test->dbname_, options);
    const ImmutableDBOptions db_options(sanitized_options);

    *count = 0;

    std::shared_ptr<Cache> table_cache = NewLRUCache(50, 0);
    FileOptions file_options;
    WriteBufferManager write_buffer_manager(db_options.db_write_buffer_size);

    std::unique_ptr<VersionSet> versions;
    std::unique_ptr<WalManager> wal_manager;
    WriteController write_controller;

    versions.reset(new VersionSet(
        test->dbname_, &db_options, MutableDBOptions{options}, file_options,
        table_cache.get(), &write_buffer_manager, &write_controller,
        /*block_cache_tracer=*/nullptr,
        /*io_tracer=*/nullptr, /*db_id=*/"", /*db_session_id=*/"",
        options.daily_offpeak_time_utc,
        /*error_handler=*/nullptr, /*read_only=*/false));

    wal_manager.reset(
        new WalManager(db_options, file_options, /*io_tracer=*/nullptr));

    std::unique_ptr<WalWriter> current_log_writer;

    for (size_t j = kWALFileOffset; j < wal_count + kWALFileOffset; j++) {
      uint64_t current_log_number = j;
      std::string fname = LogFileName(test->dbname_, current_log_number);
      std::unique_ptr<WritableFileWriter> file_writer;
      ASSERT_OK(WritableFileWriter::Create(db_options.env->GetFileSystem(),
                                           fname, file_options, &file_writer,
                                           nullptr));
      WalWriter* log_writer = new LegacyWalWriter(
          std::move(file_writer), current_log_number,
          db_options.recycle_log_file_num > 0, /*manual_flush=*/false,
          db_options.wal_compression, /*track_and_verify_wals=*/false,
          /*initial_block_offset=*/0);
      ASSERT_OK(
          log_writer->WriteFileStart(WriteOptions(), PredecessorWALInfo()));
      current_log_writer.reset(log_writer);

      WriteBatch batch;
      for (int i = 0; i < kKeysPerWALFile; i++) {
        std::string key = "key" + std::to_string((*count)++);
        std::string value = test->DummyString(kValueSize);
        ASSERT_NE(current_log_writer.get(), nullptr);
        uint64_t seq = versions->LastSequence() + 1;
        batch.Clear();
        ASSERT_OK(batch.Put(key, value));
        WriteBatchInternal::SetSequence(&batch, seq);
        ASSERT_OK(current_log_writer->AddRecord(
            WriteOptions(), WriteBatchInternal::Contents(&batch)));
        versions->SetLastAllocatedSequence(seq);
        versions->SetLastPublishedSequence(seq);
        versions->SetLastSequence(seq);
      }
    }
  }

  // Recreate and fill the store with some data
  static size_t FillData(DBWALTestBase* test, Options* options) {
    options->create_if_missing = true;
    test->DestroyAndReopen(*options);
    test->Close();

    size_t count = 0;
    FillData(test, *options, kWALFilesCount, &count);
    return count;
  }

  // Read back all the keys we wrote and return the number of keys found
  static size_t GetData(DBWALTestBase* test) {
    size_t count = 0;
    for (size_t i = 0; i < kWALFilesCount * kKeysPerWALFile; i++) {
      if (test->Get("key" + std::to_string(i)) != "NOT_FOUND") {
        ++count;
      }
    }
    return count;
  }

  // Manuall corrupt the specified WAL
  static void CorruptWAL(DBWALTestBase* test, const Options& options,
                         const double off, const double len,
                         const int wal_file_id, const bool trunc = false) {
    Env* env = options.env;
    std::string fname = LogFileName(test->dbname_, wal_file_id);
    uint64_t size;
    ASSERT_OK(env->GetFileSize(fname, &size));
    ASSERT_GT(size, 0);
#ifdef OS_WIN
    // Windows disk cache behaves differently. When we truncate
    // the original content is still in the cache due to the original
    // handle is still open. Generally, in Windows, one prohibits
    // shared access to files and it is not needed for WAL but we allow
    // it to induce corruption at various tests.
    test->Close();
#endif
    if (trunc) {
      ASSERT_OK(
          test::TruncateFile(env, fname, static_cast<uint64_t>(size * off)));
    } else {
      ASSERT_OK(test::CorruptFile(env, fname, static_cast<int>(size * off + 8),
                                  static_cast<int>(size * len), false));
    }
  }
};

TEST_F(DBWALTest, TrackAndVerifyWALsRecycleWAL) {
  Options options = CurrentOptions();
  options.avoid_flush_during_shutdown = true;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.recycle_log_file_num = 1;
  options.track_and_verify_wals = true;

  DestroyAndReopen(options);

  ASSERT_OK(Put("key_ignore", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore1", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore2", "wal_to_recycle"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("key_ignore", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore1", "wal_to_recycle"));
  ASSERT_OK(Put("key_ignore2", "wal_to_recycle"));
  ASSERT_OK(Flush());
  // The second WAL can be recycled only after the flush closes its writer.
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  // Stop background flush to avoid deleting any WAL
  options.env->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_task;
  options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                        &sleeping_task, Env::Priority::HIGH);

  // Recycle the first WAL
  ASSERT_OK(Put("key1", "old_value"));
  // Recycle the second WAL
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(Put("key1", "new_value"));

  // Create a WAL hole on sequence number by truncating the first WAL to 0 byte
  VectorWalPtr log_files;
  ASSERT_OK(db_->GetSortedWalFiles(log_files));
  ASSERT_EQ(log_files.size(), 2);
  std::string log_name = LogFileName(dbname_, log_files.front()->LogNumber());
  Close();
  // Drop `Put("key1", "old_value")` in the first WAL
  ASSERT_OK(test::TruncateFile(options.env, log_name, 0 /* new_length */));

  {
    Status s = DB::Open(options, dbname_, &db_);

    ASSERT_OK(s);
  }

  ASSERT_EQ("wal_to_recycle", Get("key_ignore2"));
  ASSERT_EQ("NOT_FOUND", Get("key1"));

  Close();
}

class DBWALTrackAndVerifyWALsWithParamsTest
    : public DBWALTestBase,
      public ::testing::WithParamInterface<WALRecoveryMode> {
 public:
  DBWALTrackAndVerifyWALsWithParamsTest()
      : DBWALTestBase("/db_wal_track_and_verify_wals_with_params_test") {}
};

INSTANTIATE_TEST_CASE_P(
    DBWALTrackAndVerifyWALsWithParamsTest,
    DBWALTrackAndVerifyWALsWithParamsTest,
    ::testing::Values(WALRecoveryMode::kTolerateCorruptedTailRecords,
                      WALRecoveryMode::kAbsoluteConsistency,
                      WALRecoveryMode::kPointInTimeRecovery,
                      WALRecoveryMode::kSkipAnyCorruptedRecords));

TEST_P(DBWALTrackAndVerifyWALsWithParamsTest, Basic) {
  Options options = CurrentOptions();
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals = true;
  options.wal_recovery_mode = GetParam();

  // Stop background flush to avoid deleting any WAL
  options.env->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_task;
  options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                        &sleeping_task, Env::Priority::HIGH);

  for (int i = 0; i < 5; i++) {
    DestroyAndReopen(options);

    ASSERT_OK(Put("key1", "old_value"));
    SequenceNumber last_seqno_recorded_in_fist_wal =
        dbfull()->GetLatestSequenceNumber();

    ASSERT_OK(dbfull()->TEST_SwitchWAL());
    ASSERT_OK(Put("key1", "new_value"));

    VectorWalPtr log_files;
    ASSERT_OK(db_->GetSortedWalFiles(log_files));
    ASSERT_EQ(log_files.size(), 2);
    uint64_t first_log_number = log_files.front()->LogNumber();
    std::string first_log_name = LogFileName(dbname_, first_log_number);
    std::string second_log_name =
        LogFileName(dbname_, log_files.back()->LogNumber());

    if (i == 0) {
      // Delete the obsolete WAL and verify it will not be seen as a WAL hole
      sleeping_task.WakeUp();
      sleeping_task.WaitUntilDone();
      ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
      // Stop background flush to avoid deleting any WAL
      sleeping_task.Reset();
      options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                            &sleeping_task, Env::Priority::HIGH);
      Close();
    } else if (i == 1) {
      // Create a WAL hole on WAL number by deleting the first WAL and verify
      // the hole will be detected
      Close();
      ASSERT_OK(options.env->DeleteFile(first_log_name));
    } else if (i == 2) {
      // Create a WAL hole on sequence number by truncating the first WAL and
      // verify the hole will be detected
      Close();
      ASSERT_OK(
          test::TruncateFile(options.env, first_log_name, 0 /* new_length */));
    } else if (i == 3) {
      // Create a WAL hole on size difference by truncating the first WAL and
      // mocking a correct sequence number to force triggering corruption based
      // on size instead of sequence number and verify the hole will be detected
      Close();
      ASSERT_OK(
          test::TruncateFile(options.env, first_log_name, 0 /* new_length */));
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::UpdatePredecessorWALInfo", [&](void* arg) {
            std::pair<uint64_t, SequenceNumber*>* pair =
                static_cast<std::pair<uint64_t, SequenceNumber*>*>(arg);
            if (pair->first == first_log_number) {
              *(pair->second) = last_seqno_recorded_in_fist_wal;
            }
          });
      SyncPoint::GetInstance()->EnableProcessing();
    } else if (i == 4) {
      // Delete all wals and verify opening a DB with no WAL will be detected
      Close();
      ASSERT_OK(options.env->DeleteFile(first_log_name));
      ASSERT_OK(options.env->DeleteFile(second_log_name));
    }

    Status s;
    {
      s = DB::Open(options, dbname_, &db_);
    }

    if (i == 0) {
      ASSERT_OK(s);
      ASSERT_EQ("new_value", Get("key1"));
      continue;
    } else if (i == 3) {
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->DisableProcessing();
    } else if (i == 4) {
      ASSERT_TRUE(s.IsCorruption());
      ASSERT_TRUE(
          s.ToString().find("Opening an existing DB with no WAL files") !=
          std::string::npos);
      Close();
      continue;
    }

    if (options.wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
      ASSERT_OK(s);
      ASSERT_EQ("NOT_FOUND", Get("key1"));
    } else if (options.wal_recovery_mode ==
                   WALRecoveryMode::kAbsoluteConsistency ||
               options.wal_recovery_mode ==
                   WALRecoveryMode::kTolerateCorruptedTailRecords) {
      ASSERT_TRUE(s.IsCorruption());
      std::string msg;
      if (i == 1) {
        msg = "Missing WAL";
      } else if (i == 2) {
        msg = "Mismatched last sequence number recorded in the WAL";
      } else if (i == 3) {
        msg = "Mismatched size of the WAL";
      }
      ASSERT_TRUE(s.ToString().find(msg) != std::string::npos);
    } else {
      ASSERT_OK(s);
      ASSERT_EQ("new_value", Get("key1"));
    }

    Close();
  }
}

class DBWALTestWithParams
    : public DBWALTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, int, int, CompressionType, bool>> {
 public:
  DBWALTestWithParams() : DBWALTestBase("/db_wal_test_with_params") {}
};

INSTANTIATE_TEST_CASE_P(
    Wal, DBWALTestWithParams,
    ::testing::Combine(::testing::Bool(), ::testing::Range(0, 4, 1),
                       ::testing::Range(RecoveryTestHelper::kWALFileOffset,
                                        RecoveryTestHelper::kWALFileOffset +
                                            RecoveryTestHelper::kWALFilesCount,
                                        1),
                       ::testing::Values(CompressionType::kNoCompression,
                                         CompressionType::kZSTD),
                       ::testing::Bool()));

class DBWALTestWithParamsVaryingRecoveryMode
    : public DBWALTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, int, int, WALRecoveryMode, CompressionType>> {
 public:
  DBWALTestWithParamsVaryingRecoveryMode()
      : DBWALTestBase("/db_wal_test_with_params_mode") {}
};

INSTANTIATE_TEST_CASE_P(
    Wal, DBWALTestWithParamsVaryingRecoveryMode,
    ::testing::Combine(
        ::testing::Bool(), ::testing::Range(0, 4, 1),
        ::testing::Range(RecoveryTestHelper::kWALFileOffset,
                         RecoveryTestHelper::kWALFileOffset +
                             RecoveryTestHelper::kWALFilesCount,
                         1),
        ::testing::Values(WALRecoveryMode::kTolerateCorruptedTailRecords,
                          WALRecoveryMode::kAbsoluteConsistency,
                          WALRecoveryMode::kPointInTimeRecovery,
                          WALRecoveryMode::kSkipAnyCorruptedRecords),
        ::testing::Values(CompressionType::kNoCompression,
                          CompressionType::kZSTD)));

// Test scope:
// - We expect to open the data store when there is incomplete trailing writes
// at the end of any of the logs
// - We do not expect to open the data store for corruption
TEST_P(DBWALTestWithParams, kTolerateCorruptedTailRecords) {
  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file

  // Fill data for testing
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);
  // test checksum failure or parsing
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .3,
                                 /*len%=*/.1, wal_file_id, trunc);

  options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  if (trunc) {
    options.create_if_missing = false;
    ASSERT_OK(TryReopen(options));
    const size_t recovered_row_count = RecoveryTestHelper::GetData(this);
    ASSERT_TRUE(corrupt_offset == 0 || recovered_row_count > 0);
    ASSERT_LT(recovered_row_count, row_count);
  } else {
    ASSERT_NOK(TryReopen(options));
  }
}

// Test scope:
// We don't expect the data store to be opened if there is any corruption
// (leading, middle or trailing -- incomplete writes or corruption)
TEST_P(DBWALTestWithParams, kAbsoluteConsistency) {
  // Verify clean slate behavior
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(RecoveryTestHelper::GetData(this), row_count);

  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  // WAL compression type
  CompressionType compression_type = std::get<3>(GetParam());
  options.wal_compression = compression_type;

  if (trunc && corrupt_offset == 0) {
    return;
  }

  // fill with new date
  RecoveryTestHelper::FillData(this, &options);
  // corrupt the wal
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .33,
                                 /*len%=*/.1, wal_file_id, trunc);
  // verify
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.create_if_missing = false;
  ASSERT_NOK(TryReopen(options));
}

// Test scope:
// We don't expect the data store to be opened if there is any inconsistency
// between WAL and SST files
TEST_F(DBWALTest, kPointInTimeRecoveryCFConsistency) {
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;

  // Create DB with multiple column families.
  CreateAndReopenWithCF({"one", "two"}, options);
  ASSERT_OK(Put(1, "key1", "val1"));
  ASSERT_OK(Put(2, "key2", "val2"));

  // Record the offset at this point
  Env* env = options.env;
  uint64_t wal_file_id = dbfull()->TEST_LogfileNumber();
  std::string fname = LogFileName(dbname_, wal_file_id);
  uint64_t offset_to_corrupt;
  ASSERT_OK(env->GetFileSize(fname, &offset_to_corrupt));
  ASSERT_GT(offset_to_corrupt, 0);

  ASSERT_OK(Put(1, "key3", "val3"));
  // Corrupt WAL at location of key3
  ASSERT_OK(test::CorruptFile(env, fname, static_cast<int>(offset_to_corrupt),
                              4, false));
  ASSERT_OK(Put(2, "key4", "val4"));
  ASSERT_OK(Put(1, "key5", "val5"));
  ASSERT_OK(Flush(2));

  // PIT recovery & verify
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  ASSERT_NOK(TryReopenWithColumnFamilies({"default", "one", "two"}, options));
}

TEST_F(DBWALTest, RaceInstallFlushResultsWithWalObsoletion) {
  Options options = CurrentOptions();
  options.env = env_;
  options.track_and_verify_wals_in_manifest = true;
  // The following make sure there are two bg flush threads.
  options.max_background_jobs = 8;

  DestroyAndReopen(options);

  const std::string cf1_name("cf1");
  CreateAndReopenWithCF({cf1_name}, options);
  assert(handles_.size() == 2);

  {
    dbfull()->TEST_LockMutex();
    ASSERT_LE(2, dbfull()->GetBGJobLimits().max_flushes);
    dbfull()->TEST_UnlockMutex();
  }

  ASSERT_OK(dbfull()->PauseBackgroundWork());

  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "foo", "value"));
  ASSERT_OK(db_->Put(WriteOptions(), "foo", "value"));

  ASSERT_OK(dbfull()->TEST_FlushMemTable(
      /*wait=*/false, /*allow_write_stall=*/true, handles_[1]));

  ASSERT_OK(db_->Put(WriteOptions(), "foo", "value"));

  ASSERT_OK(dbfull()->TEST_FlushMemTable(
      /*wait=*/false, /*allow_write_stall=*/true, handles_[0]));

  bool called = false;
  std::atomic<int> bg_flush_threads{0};
  std::atomic<bool> wal_synced{false};
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void* /*arg*/) {
        int cur = bg_flush_threads.load();
        int desired = cur + 1;
        if (cur > 0 ||
            !bg_flush_threads.compare_exchange_strong(cur, desired)) {
          while (!wal_synced.load()) {
            // Wait until the other bg flush thread finishes committing WAL sync
            // operation to the MANIFEST.
          }
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:CommitWal:1",
      [&](void* /*arg*/) { wal_synced.store(true); });
  // This callback will be called when the first bg flush thread reaches the
  // point before entering the MANIFEST write queue after flushing the SST
  // file.
  // The purpose of the sync points here is to ensure both bg flush threads
  // finish computing `min_wal_number_to_keep` before any of them updates the
  // `log_number` for the column family that's being flushed.
  SyncPoint::GetInstance()->SetCallBack(
      "MemTableList::TryInstallMemtableFlushResults:AfterComputeMinWalToKeep",
      [&](void* /*arg*/) {
        dbfull()->mutex()->AssertHeld();
        if (!called) {
          // We are the first bg flush thread in the MANIFEST write queue.
          // We set up the dependency between sync points for two threads that
          // will be executing the same code.
          // For the interleaving of events, see
          // https://github.com/meta/rocksdb/pull/9715.
          // bg flush thread1 will release the db mutex while in the MANIFEST
          // write queue. In the meantime, bg flush thread2 locks db mutex and
          // computes the min_wal_number_to_keep (before thread1 writes to
          // MANIFEST thus before cf1->log_number is updated). Bg thread2 joins
          // the MANIFEST write queue afterwards and bg flush thread1 proceeds
          // with writing to MANIFEST.
          called = true;
          SyncPoint::GetInstance()->LoadDependency({
              {"VersionSet::LogAndApply:WriteManifestStart",
               "DBWALTest::RaceInstallFlushResultsWithWalObsoletion:BgFlush2"},
              {"DBWALTest::RaceInstallFlushResultsWithWalObsoletion:BgFlush2",
               "VersionSet::LogAndApply:WriteManifest"},
          });
        } else {
          // The other bg flush thread has already been in the MANIFEST write
          // queue, and we are after.
          TEST_SYNC_POINT(
              "DBWALTest::RaceInstallFlushResultsWithWalObsoletion:BgFlush2");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->ContinueBackgroundWork());

  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));

  ASSERT_TRUE(called);

  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  std::unique_ptr<DB> db1;
  Status s = DB::OpenForReadOnly(options, dbname_, &db1);
  ASSERT_OK(s);
  assert(db1);
}

TEST_F(DBWALTest, FixSyncWalOnObseletedWalWithNewManifestCausingMissingWAL) {
  Options options = CurrentOptions();
  // Small size to force manifest creation
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  options.track_and_verify_wals_in_manifest = true;
  DestroyAndReopen(options);

  // Accumulate memtable m1 and create the 1st wal (i.e, 4.log)
  ASSERT_OK(Put(Key(1), ""));
  ASSERT_OK(Put(Key(2), ""));
  ASSERT_OK(Put(Key(3), ""));

  const std::string wal_file_path = db_->GetName() + "/000004.log";

  // Coerce the following sequence of events:
  // (1) Flush() marks 4.log to be obsoleted, 8.log to be the latest (i.e,
  // active) log and release the lock
  // (2) SyncWAL() proceeds with the lock. It
  // creates a new manifest and syncs all the inactive wals before the latest
  // (i.e, active log), which is 4.log. Note that SyncWAL() is not aware of the
  // fact that 4.log has marked as to be obseleted. Such wal
  // sync will then add a WAL addition record of 4.log to the new manifest
  // without any special treatment. Prior to the fix, there is no WAL deletion
  // record to offset it. (3) BackgroundFlush() will eventually purge 4.log.

  bool wal_synced = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FindObsoleteFiles::PostMutexUnlock", [&](void*) {
        ASSERT_OK(env_->FileExists(wal_file_path));
        uint64_t pre_sync_wal_manifest_no =
            dbfull()->TEST_Current_Manifest_FileNo();
        ASSERT_OK(db_->SyncWAL());
        uint64_t post_sync_wal_manifest_no =
            dbfull()->TEST_Current_Manifest_FileNo();
        bool new_manifest_created =
            post_sync_wal_manifest_no == pre_sync_wal_manifest_no + 1;
        ASSERT_TRUE(new_manifest_created);
        wal_synced = true;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  ASSERT_TRUE(wal_synced);
  // BackgroundFlush() purged 4.log
  // because the memtable associated with the WAL was flushed and new WAL was
  // created (i.e, 8.log)
  ASSERT_TRUE(env_->FileExists(wal_file_path).IsNotFound());

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // To verify the corruption of "Missing WAL with log number: 4" under
  // `options.track_and_verify_wals_in_manifest = true` is fixed.
  //
  // Before the fix, `db_->SyncWAL()` will sync and record WAL addtion of the
  // obseleted WAL 4.log in a new manifest without any special treament.
  // This will result in missing-wal corruption in DB::Reopen().
  Status s = TryReopen(options);
  EXPECT_OK(s);
}

// Test scope:
// - We expect to open data store under all circumstances
// - We expect only data upto the point where the first error was encountered
TEST_P(DBWALTestWithParams, kPointInTimeRecovery) {
  const int maxkeys =
      RecoveryTestHelper::kWALFilesCount * RecoveryTestHelper::kKeysPerWALFile;

  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  // WAL compression type
  CompressionType compression_type = std::get<3>(GetParam());

  // Fill data for testing
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  options.wal_compression = compression_type;
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);

  // Corrupt the wal
  // The offset here was 0.3 which cuts off right at the end of a
  // valid fragment after wal zstd compression checksum is enabled,
  // so changed the value to 0.33.
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .33,
                                 /*len%=*/.1, wal_file_id, trunc);

  // Verify
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));

  // Probe data for invariants
  size_t recovered_row_count = RecoveryTestHelper::GetData(this);
  ASSERT_LT(recovered_row_count, row_count);

  // Verify a prefix of keys were recovered. But not in the case of full WAL
  // truncation, because we have no way to know there was a corruption when
  // truncation happened on record boundaries (preventing recovery holes in
  // that case requires using `track_and_verify_wals_in_manifest`).
  if (!trunc || corrupt_offset != 0) {
    bool expect_data = true;
    for (size_t k = 0; k < maxkeys; ++k) {
      bool found = Get("key" + std::to_string(k)) != "NOT_FOUND";
      if (expect_data && !found) {
        expect_data = false;
      }
      ASSERT_EQ(found, expect_data);
    }
  }

  const size_t min = RecoveryTestHelper::kKeysPerWALFile *
                     (wal_file_id - RecoveryTestHelper::kWALFileOffset);
  ASSERT_GE(recovered_row_count, min);
  if (!trunc && corrupt_offset != 0) {
    const size_t max = RecoveryTestHelper::kKeysPerWALFile *
                       (wal_file_id - RecoveryTestHelper::kWALFileOffset + 1);
    ASSERT_LE(recovered_row_count, max);
  }
}

// Test scope:
// - We expect to open the data store under all scenarios
// - We expect to have recovered records past the corruption zone
TEST_P(DBWALTestWithParams, kSkipAnyCorruptedRecords) {
  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  // WAL compression type
  CompressionType compression_type = std::get<3>(GetParam());

  // Fill data for testing
  Options options = CurrentOptions();
  options.track_and_verify_wals = std::get<4>(GetParam());
  options.wal_compression = compression_type;
  const size_t row_count = RecoveryTestHelper::FillData(this, &options);

  // Corrupt the WAL
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .3,
                                 /*len%=*/.1, wal_file_id, trunc);

  // Verify behavior
  options.wal_recovery_mode = WALRecoveryMode::kSkipAnyCorruptedRecords;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));

  // Probe data for invariants
  size_t recovered_row_count = RecoveryTestHelper::GetData(this);
  ASSERT_LT(recovered_row_count, row_count);

  if (!trunc) {
    ASSERT_TRUE(corrupt_offset != 0 || recovered_row_count > 0);
  }
}

TEST_F(DBWALTest, AvoidFlushDuringRecovery) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.avoid_flush_during_recovery = false;

  // Test with flush after recovery.
  Reopen(options);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "v3"));
  ASSERT_OK(Put("bar", "v4"));
  ASSERT_EQ(1, TotalTableFiles());
  // Reopen DB. Check if WAL logs flushed.
  Reopen(options);
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v4", Get("bar"));
  ASSERT_EQ(2, TotalTableFiles());

  // Test without flush after recovery.
  options.avoid_flush_during_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v5"));
  ASSERT_OK(Put("bar", "v6"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "v7"));
  ASSERT_OK(Put("bar", "v8"));
  ASSERT_EQ(1, TotalTableFiles());
  // Reopen DB. WAL logs should not be flushed this time.
  Reopen(options);
  ASSERT_EQ("v7", Get("foo"));
  ASSERT_EQ("v8", Get("bar"));
  ASSERT_EQ(1, TotalTableFiles());

  // Force flush with allow_2pc.
  options.avoid_flush_during_recovery = true;
  options.allow_2pc = true;
  ASSERT_OK(Put("foo", "v9"));
  ASSERT_OK(Put("bar", "v10"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "v11"));
  ASSERT_OK(Put("bar", "v12"));
  Reopen(options);
  ASSERT_EQ("v11", Get("foo"));
  ASSERT_EQ("v12", Get("bar"));
  ASSERT_EQ(3, TotalTableFiles());
}

TEST_F(DBWALTest, WalCleanupAfterAvoidFlushDuringRecovery) {
  // Verifies WAL files that were present during recovery, but not flushed due
  // to avoid_flush_during_recovery, will be considered for deletion at a later
  // stage. We check at least one such file is deleted during Flush().
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.avoid_flush_during_recovery = true;
  Reopen(options);

  ASSERT_OK(Put("foo", "v1"));
  Reopen(options);
  for (int i = 0; i < 2; ++i) {
    if (i > 0) {
      // Flush() triggers deletion of obsolete tracked files
      ASSERT_OK(Flush());
    }
    VectorLogPtr log_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(log_files));
    if (i == 0) {
      ASSERT_GT(log_files.size(), 0);
    } else {
      ASSERT_EQ(0, log_files.size());
    }
  }
}

TEST_F(DBWALTest, RecoverWithoutFlush) {
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 64 * 1024 * 1024;

  size_t count = RecoveryTestHelper::FillData(this, &options);
  auto validateData = [this, count]() {
    for (size_t i = 0; i < count; i++) {
      ASSERT_NE(Get("key" + std::to_string(i)), "NOT_FOUND");
    }
  };
  Reopen(options);
  validateData();
  // Insert some data without flush
  ASSERT_OK(Put("foo", "foo_v1"));
  ASSERT_OK(Put("bar", "bar_v1"));
  Reopen(options);
  validateData();
  ASSERT_EQ(Get("foo"), "foo_v1");
  ASSERT_EQ(Get("bar"), "bar_v1");
  // Insert again and reopen
  ASSERT_OK(Put("foo", "foo_v2"));
  ASSERT_OK(Put("bar", "bar_v2"));
  Reopen(options);
  validateData();
  ASSERT_EQ(Get("foo"), "foo_v2");
  ASSERT_EQ(Get("bar"), "bar_v2");
  // manual flush and insert again
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("foo"), "foo_v2");
  ASSERT_EQ(Get("bar"), "bar_v2");
  ASSERT_OK(Put("foo", "foo_v3"));
  ASSERT_OK(Put("bar", "bar_v3"));
  Reopen(options);
  validateData();
  ASSERT_EQ(Get("foo"), "foo_v3");
  ASSERT_EQ(Get("bar"), "bar_v3");
}

TEST_F(DBWALTest, RecoverWithoutFlushMultipleCF) {
  const std::string kSmallValue = "v";
  const std::string kLargeValue = DummyString(1024);
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;

  auto countWalFiles = [this]() {
    VectorLogPtr log_files;
    if (!dbfull()->GetSortedWalFiles(log_files).ok()) {
      return size_t{0};
    }
    return log_files.size();
  };

  // Create DB with multiple column families and multiple log files.
  CreateAndReopenWithCF({"one", "two"}, options);
  ASSERT_OK(Put(0, "key1", kSmallValue));
  ASSERT_OK(Put(1, "key2", kLargeValue));
  ASSERT_OK(Flush(1));
  ASSERT_EQ(1, countWalFiles());
  ASSERT_OK(Put(0, "key3", kSmallValue));
  ASSERT_OK(Put(2, "key4", kLargeValue));
  ASSERT_OK(Flush(2));
  ASSERT_EQ(2, countWalFiles());

  // Reopen, insert and flush.
  options.db_write_buffer_size = 64 * 1024 * 1024;
  ReopenWithColumnFamilies({"default", "one", "two"}, options);
  ASSERT_EQ(Get(0, "key1"), kSmallValue);
  ASSERT_EQ(Get(1, "key2"), kLargeValue);
  ASSERT_EQ(Get(0, "key3"), kSmallValue);
  ASSERT_EQ(Get(2, "key4"), kLargeValue);
  // Insert more data.
  ASSERT_OK(Put(0, "key5", kLargeValue));
  ASSERT_OK(Put(1, "key6", kLargeValue));
  ASSERT_EQ(3, countWalFiles());
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "key7", kLargeValue));
  ASSERT_OK(dbfull()->FlushWAL(false));
  ASSERT_EQ(4, countWalFiles());

  // Reopen twice and validate.
  for (int i = 0; i < 2; i++) {
    ReopenWithColumnFamilies({"default", "one", "two"}, options);
    ASSERT_EQ(Get(0, "key1"), kSmallValue);
    ASSERT_EQ(Get(1, "key2"), kLargeValue);
    ASSERT_EQ(Get(0, "key3"), kSmallValue);
    ASSERT_EQ(Get(2, "key4"), kLargeValue);
    ASSERT_EQ(Get(0, "key5"), kLargeValue);
    ASSERT_EQ(Get(1, "key6"), kLargeValue);
    ASSERT_EQ(Get(2, "key7"), kLargeValue);
    ASSERT_EQ(4, countWalFiles());
  }
}

// In this test we are trying to do the following:
//   1. Create a DB with corrupted WAL log;
//   2. Open with avoid_flush_during_recovery = true;
//   3. Append more data without flushing, which creates new WAL log.
//   4. Open again. See if it can correctly handle previous corruption.
TEST_P(DBWALTestWithParamsVaryingRecoveryMode,
       RecoverFromCorruptedWALWithoutFlush) {
  const int kAppendKeys = 100;
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.create_if_missing = false;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 64 * 1024 * 1024;

  auto getAll = [this]() {
    std::vector<std::pair<std::string, std::string>> data;
    ReadOptions ropt;
    Iterator* iter = dbfull()->NewIterator(ropt);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      data.emplace_back(iter->key().ToString(), iter->value().ToString());
    }
    EXPECT_OK(iter->status());
    delete iter;
    return data;
  };

  bool trunc = std::get<0>(GetParam());  // Corruption style
  // Corruption offset position
  int corrupt_offset = std::get<1>(GetParam());
  int wal_file_id = std::get<2>(GetParam());  // WAL file
  WALRecoveryMode recovery_mode = std::get<3>(GetParam());
  // WAL compression type
  CompressionType compression_type = std::get<4>(GetParam());

  options.wal_recovery_mode = recovery_mode;
  options.wal_compression = compression_type;
  // Create corrupted WAL
  RecoveryTestHelper::FillData(this, &options);
  RecoveryTestHelper::CorruptWAL(this, options, corrupt_offset * .3,
                                 /*len%=*/.1, wal_file_id, trunc);
  // Skip the test if DB won't open.
  if (!TryReopen(options).ok()) {
    ASSERT_TRUE(options.wal_recovery_mode ==
                    WALRecoveryMode::kAbsoluteConsistency ||
                (!trunc && options.wal_recovery_mode ==
                               WALRecoveryMode::kTolerateCorruptedTailRecords));
    return;
  }
  ASSERT_OK(TryReopen(options));
  // Append some more data.
  for (int k = 0; k < kAppendKeys; k++) {
    std::string key = "extra_key" + std::to_string(k);
    std::string value = DummyString(RecoveryTestHelper::kValueSize);
    ASSERT_OK(Put(key, value));
  }
  // Save data for comparison.
  auto data = getAll();
  // Reopen. Verify data.
  ASSERT_OK(TryReopen(options));
  auto actual_data = getAll();
  ASSERT_EQ(data, actual_data);
}

// Tests that total log size is recovered if we set
// avoid_flush_during_recovery=true.
// Flush should trigger if max_total_wal_size is reached.
TEST_F(DBWALTest, RestoreTotalLogSizeAfterRecoverWithoutFlush) {
  auto test_listener = std::make_shared<FlushCounterListener>();
  test_listener->expected_flush_reason = FlushReason::kWalFull;

  constexpr size_t kKB = 1024;
  constexpr size_t kMB = 1024 * 1024;
  Options options = CurrentOptions();
  options.avoid_flush_during_recovery = true;
  options.max_total_wal_size = 1 * kMB;
  options.listeners.push_back(test_listener);
  // Have to open DB in multi-CF mode to trigger flush when
  // max_total_wal_size is reached.
  CreateAndReopenWithCF({"one"}, options);
  // Write some keys and we will end up with one log file which is slightly
  // smaller than 1MB.
  std::string value_100k(100 * kKB, 'v');
  std::string value_300k(300 * kKB, 'v');
  ASSERT_OK(Put(0, "foo", "v1"));
  for (int i = 0; i < 9; i++) {
    ASSERT_OK(Put(1, "key" + std::to_string(i), value_100k));
  }
  // Get log files before reopen.
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  uint64_t log_size_before = log_files_before[0]->SizeFileBytes();
  ASSERT_GT(log_size_before, 900 * kKB);
  ASSERT_LT(log_size_before, 1 * kMB);
  ReopenWithColumnFamilies({"default", "one"}, options);
  // Write one more value to make log larger than 1MB.
  ASSERT_OK(Put(1, "bar", value_300k));
  // Get log files again. A new log file will be opened.
  VectorLogPtr log_files_after_reopen;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_after_reopen));
  ASSERT_EQ(2, log_files_after_reopen.size());
  ASSERT_EQ(log_files_before[0]->LogNumber(),
            log_files_after_reopen[0]->LogNumber());
  ASSERT_GT(log_files_after_reopen[0]->SizeFileBytes() +
                log_files_after_reopen[1]->SizeFileBytes(),
            1 * kMB);
  // Write one more key to trigger flush.
  ASSERT_OK(Put(0, "foo", "v2"));
  for (auto* h : handles_) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(h));
  }
  // Flushed two column families.
  ASSERT_EQ(2, test_listener->count.load());
}

#if defined(ROCKSDB_PLATFORM_POSIX)
#if defined(ROCKSDB_FALLOCATE_PRESENT)
// Tests that we will truncate the preallocated space of the last log from
// previous.
TEST_F(DBWALTest, TruncateLastLogAfterRecoverWithoutFlush) {
  constexpr size_t kKB = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = true;
  if (mem_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  ASSERT_OK(Put("foo", "v1"));
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  auto& file_before = log_files_before[0];
  ASSERT_LT(file_before->SizeFileBytes(), 1 * kKB);
  // The log file has preallocated space.
  {
    std::string fname = dbname_ + file_before->PathName();
    uint64_t allocated = GetAllocatedFileSize(fname);
    if (ShouldSkipAllocationCheck(fname)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              fname.c_str());
    } else if (allocated < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              allocated, preallocated_size, fname.c_str());
    } else {
      ASSERT_GE(allocated, preallocated_size);
    }
  }
  Reopen(options);
  VectorLogPtr log_files_after;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_after));
  ASSERT_EQ(1, log_files_after.size());
  ASSERT_LT(log_files_after[0]->SizeFileBytes(), 1 * kKB);
  // The preallocated space should be truncated.
  {
    std::string fname = dbname_ + file_before->PathName();
    if (!ShouldSkipAllocationCheck(fname)) {
      ASSERT_LT(GetAllocatedFileSize(fname), preallocated_size);
    }
  }
}

// Tests that we will truncate the preallocated space of the last log from
// previous.
TEST_F(DBWALTest, TruncateLastLogAfterRecoverWithFlush) {
  constexpr size_t kKB = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = false;
  options.avoid_flush_during_shutdown = true;
  if (mem_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  ASSERT_OK(Put("foo", "v1"));
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  auto& file_before = log_files_before[0];
  ASSERT_LT(file_before->SizeFileBytes(), 1 * kKB);
  {
    std::string fname = dbname_ + file_before->PathName();
    uint64_t allocated = GetAllocatedFileSize(fname);
    if (ShouldSkipAllocationCheck(fname)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              fname.c_str());
    } else if (allocated < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              allocated, preallocated_size, fname.c_str());
    } else {
      ASSERT_GE(allocated, preallocated_size);
    }
  }
  // The log file has preallocated space.
  Close();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::PurgeObsoleteFiles:Begin",
        "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover"},
       {"DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate",
        "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  port::Thread reopen_thread([&]() { Reopen(options); });

  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover");
  // After the flush during Open, the log file should get deleted.  However,
  // if  the process is in a crash loop, the log file may not get
  // deleted and thte preallocated space will keep accumulating. So we need
  // to ensure it gets trtuncated.
  {
    std::string fname = dbname_ + file_before->PathName();
    if (!ShouldSkipAllocationCheck(fname)) {
      EXPECT_LT(GetAllocatedFileSize(fname), preallocated_size);
    }
  }
  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate");
  reopen_thread.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBWALTest, TruncateLastLogAfterRecoverWALEmpty) {
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = false;
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem/non-encrypted  environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  Close();
  std::vector<std::string> filenames;
  std::string last_log;
  uint64_t last_log_num = 0;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  for (const auto& fname : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname, &number, &type, nullptr)) {
      if (type == kWalFile && number > last_log_num) {
        last_log = fname;
      }
    }
  }
  ASSERT_NE(last_log, "");
  last_log = dbname_ + '/' + last_log;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::PurgeObsoleteFiles:Begin",
        "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover"},
       {"DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate",
        "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PosixWritableFile::Close",
      [](void* arg) { *(static_cast<size_t*>(arg)) = 0; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Preallocate space for the empty log file. This could happen if WAL data
  // was buffered in memory and the process crashed.
  std::unique_ptr<WritableFile> log_file;
  ASSERT_OK(env_->ReopenWritableFile(last_log, &log_file, EnvOptions()));
  log_file->SetPreallocationBlockSize(preallocated_size);
  log_file->PrepareWrite(0, 4096);
  log_file.reset();

  {
    uint64_t allocated = GetAllocatedFileSize(last_log);
    if (ShouldSkipAllocationCheck(last_log)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              last_log.c_str());
    } else if (allocated < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              allocated, preallocated_size, last_log.c_str());
    } else {
      ASSERT_GE(allocated, preallocated_size);
    }
  }

  port::Thread reopen_thread([&]() { Reopen(options); });

  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterRecover");
  // The preallocated space should be truncated.
  if (!ShouldSkipAllocationCheck(last_log)) {
    EXPECT_LT(GetAllocatedFileSize(last_log), preallocated_size);
  }
  TEST_SYNC_POINT(
      "DBWALTest::TruncateLastLogAfterRecoverWithFlush:AfterTruncate");
  reopen_thread.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBWALTest, ReadOnlyRecoveryNoTruncate) {
  constexpr size_t kKB = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.avoid_flush_during_recovery = true;
  if (mem_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem environment");
    return;
  }
  if (!IsFallocateSupported()) {
    return;
  }

  // create DB and close with file truncate disabled
  std::atomic_bool enable_truncate{false};

  SyncPoint::GetInstance()->SetCallBack("PosixWritableFile::Close",
                                        [&](void* arg) {
                                          if (!enable_truncate) {
                                            *(static_cast<size_t*>(arg)) = 0;
                                          }
                                        });
  SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);
  size_t preallocated_size =
      dbfull()->TEST_GetWalPreallocateBlockSize(options.write_buffer_size);
  ASSERT_OK(Put("foo", "v1"));
  VectorLogPtr log_files_before;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_before));
  ASSERT_EQ(1, log_files_before.size());
  auto& file_before = log_files_before[0];
  ASSERT_LT(file_before->SizeFileBytes(), 1 * kKB);
  // The log file has preallocated space.
  std::string fname = dbname_ + file_before->PathName();
  auto db_size = GetAllocatedFileSize(fname);
  {
    if (ShouldSkipAllocationCheck(fname)) {
      fprintf(stderr,
              "Skipping preallocation check on this filesystem for %s\n",
              fname.c_str());
    } else if (db_size < preallocated_size) {
      fprintf(stderr,
              "Warning: allocated size (%lu) less than preallocated size (%lu) "
              "for %s, skipping check. This may indicate the filesystem does "
              "not support preallocation or does not report it.\n",
              db_size, preallocated_size, fname.c_str());
    } else {
      ASSERT_GE(db_size, preallocated_size);
    }
  }
  Close();

  // enable truncate and open DB as readonly, the file should not be truncated
  // and DB size is not changed.
  enable_truncate = true;
  ASSERT_OK(ReadOnlyReopen(options));
  VectorLogPtr log_files_after;
  ASSERT_OK(dbfull()->GetSortedWalFiles(log_files_after));
  ASSERT_EQ(1, log_files_after.size());
  ASSERT_LT(log_files_after[0]->SizeFileBytes(), 1 * kKB);
  ASSERT_EQ(log_files_after[0]->PathName(), file_before->PathName());
  // The preallocated space should NOT be truncated.
  // the DB size is almost the same.
  {
    std::string fname2 = dbname_ + file_before->PathName();
    if (!ShouldSkipAllocationCheck(fname2)) {
      ASSERT_NEAR(GetAllocatedFileSize(fname2), db_size, db_size / 100);
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // ROCKSDB_FALLOCATE_PRESENT
#endif  // ROCKSDB_PLATFORM_POSIX

// Reports a configurable preallocation capability and counts
// ReopenWritableFile() calls on WAL files.
class PreallocationTestFs : public FileSystemWrapper {
 public:
  PreallocationTestFs(const std::shared_ptr<FileSystem>& target,
                      bool preallocation_supported)
      : FileSystemWrapper(target),
        preallocation_supported_(preallocation_supported) {}

  const char* Name() const override { return "PreallocationTestFs"; }

  bool IsPreallocationSupported() override { return preallocation_supported_; }

  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    uint64_t number;
    FileType type;
    if (ParseFileName(fname.substr(fname.find_last_of('/') + 1), &number,
                      &type) &&
        type == kWalFile) {
      wal_reopen_count_.FetchAddRelaxed(1);
    }
    return target()->ReopenWritableFile(fname, file_opts, result, dbg);
  }

  void SetPreallocationSupported(bool supported) {
    preallocation_supported_ = supported;
  }

  int GetWalReopenCount() const { return wal_reopen_count_.LoadRelaxed(); }

  void ResetWalReopenCount() { wal_reopen_count_.StoreRelaxed(0); }

 private:
  bool preallocation_supported_;
  RelaxedAtomic<int> wal_reopen_count_{0};
};

// Tests that the last WAL is reopened for truncation during recovery only if
// the FileSystem reports that it supports preallocation.
TEST_F(DBWALTest, SkipWalTruncateIfPreallocationNotSupported) {
  // The Env must outlive the loop since DestroyAndReopen() destroys the
  // previous DB using the previous iteration's options.
  auto test_fs = std::make_shared<PreallocationTestFs>(
      env_->GetFileSystem(), /*preallocation_supported=*/true);
  std::unique_ptr<Env> test_env(NewCompositeEnv(test_fs));
  for (bool preallocation_supported : {true, false}) {
    // avoid_flush_during_recovery = true restores the WAL as alive, while
    // false flushes the recovered data and leaves the WAL obsolete.
    for (bool avoid_flush_during_recovery : {true, false}) {
      SCOPED_TRACE(
          "preallocation_supported=" + std::to_string(preallocation_supported) +
          " avoid_flush_during_recovery=" +
          std::to_string(avoid_flush_during_recovery));
      Options options = CurrentOptions();
      options.env = test_env.get();
      options.avoid_flush_during_recovery = avoid_flush_during_recovery;
      options.avoid_flush_during_shutdown = true;
      test_fs->SetPreallocationSupported(preallocation_supported);
      DestroyAndReopen(options);
      ASSERT_OK(Put("foo", "v1"));
      Close();

      test_fs->ResetWalReopenCount();
      Reopen(options);
      ASSERT_EQ(preallocation_supported ? 1 : 0, test_fs->GetWalReopenCount());
      ASSERT_EQ("v1", Get("foo"));
      Close();
    }
  }
}

// Tests that a read-only open never reopens a WAL for truncation, even if the
// FileSystem supports preallocation.
TEST_F(DBWALTest, ReadOnlyOpenSkipsWalTruncate) {
  Options options = CurrentOptions();
  auto test_fs = std::make_shared<PreallocationTestFs>(
      env_->GetFileSystem(), /*preallocation_supported=*/true);
  std::unique_ptr<Env> test_env(NewCompositeEnv(test_fs));
  options.env = test_env.get();
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1"));
  Close();

  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v1", Get("foo"));
  Close();
  ASSERT_EQ(0, test_fs->GetWalReopenCount());

  // The same WAL is truncated by a read-write open.
  Reopen(options);
  ASSERT_EQ(1, test_fs->GetWalReopenCount());
  Close();
}

TEST_F(DBWALTest, WalInManifestButNotInSortedWals) {
  Options options = CurrentOptions();
  options.track_and_verify_wals_in_manifest = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;

  // Build a way to make wal files selectively go missing
  bool wals_go_missing = false;
  struct MissingWalFs : public FileSystemWrapper {
    MissingWalFs(const std::shared_ptr<FileSystem>& t,
                 bool* _wals_go_missing_flag)
        : FileSystemWrapper(t), wals_go_missing_flag(_wals_go_missing_flag) {}
    bool* wals_go_missing_flag;
    IOStatus GetChildren(const std::string& dir, const IOOptions& io_opts,
                         std::vector<std::string>* r,
                         IODebugContext* dbg) override {
      IOStatus s = target_->GetChildren(dir, io_opts, r, dbg);
      if (s.ok() && *wals_go_missing_flag) {
        for (size_t i = 0; i < r->size();) {
          if (EndsWith(r->at(i), ".log")) {
            r->erase(r->begin() + i);
          } else {
            ++i;
          }
        }
      }
      return s;
    }
    const char* Name() const override { return "MissingWalFs"; }
  };
  auto my_fs =
      std::make_shared<MissingWalFs>(env_->GetFileSystem(), &wals_go_missing);
  std::unique_ptr<Env> my_env(NewCompositeEnv(my_fs));
  options.env = my_env.get();

  CreateAndReopenWithCF({"blah"}, options);

  // Currently necessary to get a WAL tracked in manifest; see
  // https://github.com/meta/rocksdb/issues/10080
  ASSERT_OK(Put(0, "x", "y"));
  ASSERT_OK(db_->SyncWAL());
  ASSERT_OK(Put(1, "x", "y"));
  ASSERT_OK(db_->SyncWAL());
  ASSERT_OK(Flush(1));

  ASSERT_FALSE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  std::vector<std::unique_ptr<LogFile>> wals;
  ASSERT_OK(db_->GetSortedWalFiles(wals));
  wals_go_missing = true;
  ASSERT_NOK(db_->GetSortedWalFiles(wals));
  wals_go_missing = false;
  Close();
}

TEST_F(DBWALTest, WalTermTest) {
  Options options = CurrentOptions();
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));

  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;

  WriteBatch batch;
  ASSERT_OK(batch.Put("foo", "bar"));
  batch.MarkWalTerminationPoint();
  ASSERT_OK(batch.Put("foo2", "bar2"));

  ASSERT_OK(dbfull()->Write(wo, &batch));

  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
  ASSERT_EQ("bar", Get(1, "foo"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo2"));
}

TEST_F(DBWALTest, GetCompressedWalsAfterSync) {
  if (db_->GetOptions().wal_compression == kNoCompression) {
    ROCKSDB_GTEST_BYPASS("stream compression not present");
    return;
  }
  Options options = GetDefaultOptions();
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.create_if_missing = true;
  options.env = env_;
  options.avoid_flush_during_recovery = true;
  options.track_and_verify_wals_in_manifest = true;
  // Enable WAL compression so that the newly-created WAL will be non-empty
  // after DB open, even if point-in-time WAL recovery encounters no
  // corruption.
  options.wal_compression = kZSTD;
  DestroyAndReopen(options);

  // Write something to memtable and WAL so that wal_empty_ will be false after
  // next DB::Open().
  ASSERT_OK(Put("a", "v"));

  Reopen(options);

  // New WAL is created, thanks to !wal_empty_.
  ASSERT_OK(dbfull()->TEST_SwitchWAL());

  ASSERT_OK(Put("b", "v"));

  ASSERT_OK(db_->SyncWAL());

  VectorLogPtr wals;
  Status s = dbfull()->GetSortedWalFiles(wals);
  ASSERT_OK(s);
}

TEST_F(DBWALTest, GetBlogWalsAfterSync) {
  CheckBlogWalsAfterSync(kNoCompression, /*streaming=*/false);
}

TEST_F(DBWALTest, GetBlogWalsAfterSyncStreaming) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_BYPASS("Test requires ZSTD streaming support");
    return;
  }
  CheckBlogWalsAfterSync(kZSTD, /*streaming=*/true);
}

TEST_F(DBWALTest, EmptyWalReopenTest) {
  Options options = CurrentOptions();
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);

  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));

  {
    std::vector<std::string> files;
    int num_wal_files = 0;
    ASSERT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& file : files) {
      uint64_t number = 0;
      FileType type = kWalFile;
      if (ParseFileName(file, &number, &type) && type == kWalFile) {
        num_wal_files++;
      }
    }

    ASSERT_EQ(num_wal_files, 1);
  }
}

TEST_F(DBWALTest, AsyncWalPrecreateWaitsForPendingFile) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: cover the race where foreground WAL rotation reaches the switch
  // before the background precreate publishes its result. The sync points hold
  // the async worker after it has created the file but before it marks it
  // ready, then verify the foreground writer waits instead of allocating a
  // higher WAL number.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:BeforePublish",
       "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:WorkerBlocked"},
      {"DBWALTest::AsyncWalPrecreateWaitsForPendingFile:AllowPublish",
       "DBImpl::BGWorkAsyncWALPrecreate:Publish"},
      {"DBImpl::WaitForAsyncWALPrecreate:BeforeWait",
       "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:ForegroundWaiting"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:WorkerBlocked");

  ASSERT_OK(Put("fill", "value"));
  Status writer_status;
  port::Thread writer(
      [&]() { writer_status = dbfull()->TEST_SwitchMemtable(); });

  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:ForegroundWaiting");
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateWaitsForPendingFile:AllowPublish");
  writer.join();
  ASSERT_OK(writer_status);
  ASSERT_OK(Put("trigger", "value"));
  ASSERT_EQ("value", Get("trigger"));
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_WAITED));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_MISS));
}

TEST_F(DBWALTest, AsyncWalPrecreateConsumesReadyFile) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: exercise the no-wait consumption path. The background task publishes
  // a prepared future WAL before the foreground memtable switch, and the switch
  // must install exactly that reserved WAL number instead of allocating
  // another.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateConsumesReadyFile:PrecreateDone"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateConsumesReadyFile:PrecreateDone");

  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_GE(wal_numbers.size(), 2);
  const uint64_t prepared_wal_number = wal_numbers.back();
  ASSERT_NE(prepared_wal_number, dbfull()->TEST_GetCurrentLogNumber());

  ASSERT_OK(Put("fill", "value"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_EQ(prepared_wal_number, dbfull()->TEST_GetCurrentLogNumber());
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_MISS));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_WAITED));
}

TEST_F(DBWALTest, BlogWalAsyncPrecreateInitializesOnActivation) {
  Options options = CurrentOptions();
  options.env = env_;
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.use_blog_format_for_wals = true;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::BlogWalAsyncPrecreateInitializesOnActivation:PrecreateDone"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::BlogWalAsyncPrecreateInitializesOnActivation:PrecreateDone");

  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_GE(wal_numbers.size(), 2);
  const uint64_t prepared_wal_number = wal_numbers.back();
  ASSERT_NE(prepared_wal_number, dbfull()->TEST_GetCurrentLogNumber());
  uint64_t prepared_size = 1;
  ASSERT_OK(env_->GetFileSize(LogFileName(dbname_, prepared_wal_number),
                              &prepared_size));
  ASSERT_EQ(0, prepared_size);

  ASSERT_OK(Put("fill", "value"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_EQ(prepared_wal_number, dbfull()->TEST_GetCurrentLogNumber());

  std::string contents;
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, prepared_wal_number),
                             &contents));
  ASSERT_TRUE(BlogFileHeader::IsBlogFormat(contents.data(), contents.size()));
}

TEST_F(DBWALTest, WalObsoleteWalClosedTwice) {
  // The obsolete WAL closes twice (one setting); repeat Close() must no-op.
  for (bool use_blog_format : {false, true}) {
    for (bool background_close_inactive_wals : {false, true}) {
      SCOPED_TRACE(std::string("use_blog_format=") +
                   (use_blog_format ? "true" : "false") +
                   std::string(", background_close_inactive_wals=") +
                   (background_close_inactive_wals ? "true" : "false"));
      Options options = CurrentOptions();
      options.use_blog_format_for_wals = use_blog_format;
      options.background_close_inactive_wals = background_close_inactive_wals;
      DestroyAndReopen(options);

      // Flush retires key1's WAL so it is closed and purged while the DB is
      // open.
      ASSERT_OK(Put("key1", "value1"));
      ASSERT_OK(Flush());
      ASSERT_OK(Put("key2", "value2"));
      ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

      ASSERT_EQ("value1", Get("key1"));
      ASSERT_EQ("value2", Get("key2"));

      Reopen(options);
      ASSERT_EQ("value1", Get("key1"));
      ASSERT_EQ("value2", Get("key2"));
    }
  }
}

TEST_F(DBWALTest, SelectBlogWalWriterAtOpen) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  DestroyAndReopen(options);

  std::unique_ptr<LogFile> wal_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, wal_file->LogNumber()),
                             &contents));
  ASSERT_TRUE(BlogFileHeader::IsBlogFormat(contents.data(), contents.size()));

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(db_->FlushWAL(true));
  ASSERT_EQ("value", Get("key"));

  std::unique_ptr<TransactionLogIterator> iterator;
  ASSERT_OK(db_->GetUpdatesSince(/*seq_number=*/1, &iterator));
  ASSERT_TRUE(iterator->Valid());

  options.use_blog_format_for_wals = false;
  Reopen(options);
  ASSERT_EQ("value", Get("key"));

  ASSERT_OK(Put("legacy-key", "legacy-value"));
  options.use_blog_format_for_wals = true;
  Reopen(options);
  ASSERT_EQ("value", Get("key"));
  ASSERT_EQ("legacy-value", Get("legacy-key"));
}

TEST_F(DBWALTest, WalReaderFactoryReturnsConcreteReaders) {
  for (bool blog : {false, true}) {
    std::string contents;
    uint64_t log_number = 0;
    WriteWalForOpenTest(blog, &contents, &log_number);
    ASSERT_FALSE(contents.empty());
    for (bool buffered : {false, true}) {
      SCOPED_TRACE(testing::Message()
                   << "blog=" << blog << " buffered=" << buffered);
      CountingWalReporter reporter;
      std::unique_ptr<SequentialFileReader> file(new SequentialFileReader(
          std::make_unique<GrowingSequentialFile>(&contents), "factory-wal"));
      std::unique_ptr<WalReaderOpenState> pending_open;
      std::unique_ptr<WalReader> reader =
          NewWalReader(nullptr, std::move(file), &reporter, true, log_number,
                       false, false, std::numeric_limits<uint64_t>::max(),
                       PredecessorWALInfo(), buffered, &pending_open);
      ASSERT_NE(nullptr, reader);
      ASSERT_EQ(nullptr, pending_open);
      ASSERT_EQ(blog, dynamic_cast<BlogWalReader*>(reader.get()) != nullptr);
      ASSERT_EQ(!blog && !buffered,
                dynamic_cast<LegacyWalReader*>(reader.get()) != nullptr);
      ASSERT_EQ(!blog && buffered, dynamic_cast<BufferedLegacyWalReader*>(
                                       reader.get()) != nullptr);
      ASSERT_EQ(log_number, reader->GetLogNumber());
      ASSERT_EQ(&reporter, reader->GetReporter());
      Slice record;
      std::string scratch;
      ASSERT_TRUE(reader->ReadRecord(&record, &scratch));
      WriteBatch batch;
      ASSERT_OK(WriteBatchInternal::SetContents(&batch, record));
      ASSERT_EQ(1U, WriteBatchInternal::Sequence(&batch));
      ASSERT_EQ(1, WriteBatchInternal::Count(&batch));
      ASSERT_FALSE(reader->ReadRecord(&record, &scratch));
      ASSERT_EQ(0U, reporter.failing_count());
    }
  }
}

TEST_F(DBWALTest, WalReaderFactoryRetriesGrowingFiles) {
  for (bool blog : {false, true}) {
    std::string contents;
    uint64_t log_number = 0;
    WriteWalForOpenTest(blog, &contents, &log_number);
    ASSERT_FALSE(contents.empty());
    for (bool buffered : {false, true}) {
      SCOPED_TRACE(testing::Message()
                   << "blog=" << blog << " buffered=" << buffered);
      std::string visible;
      CountingWalReporter reporter;
      std::unique_ptr<SequentialFileReader> file(new SequentialFileReader(
          std::make_unique<GrowingSequentialFile>(&visible), "growing-wal"));
      std::unique_ptr<WalReaderOpenState> pending_open;
      std::unique_ptr<WalReader> reader =
          NewWalReader(nullptr, std::move(file), &reporter, true, log_number,
                       false, false, std::numeric_limits<uint64_t>::max(),
                       PredecessorWALInfo(), buffered, &pending_open);
      ASSERT_EQ(nullptr, reader);
      ASSERT_NE(nullptr, pending_open);
      ASSERT_EQ(WalReaderOpenState::Result::kPending,
                pending_open->TryOpen(&reader));
      if (blog && buffered) {
        for (size_t prefix_size = 1; prefix_size < kBlogFileMagicSize;
             ++prefix_size) {
          visible.assign(contents.data(), prefix_size);
          ASSERT_EQ(WalReaderOpenState::Result::kPending,
                    pending_open->TryOpen(&reader));
          ASSERT_EQ(nullptr, reader);
          ASSERT_EQ(0U, reporter.failing_count());
        }
      }
      visible = contents;
      ASSERT_EQ(WalReaderOpenState::Result::kReady,
                pending_open->TryOpen(&reader));
      pending_open.reset();
      ASSERT_EQ(blog, dynamic_cast<BlogWalReader*>(reader.get()) != nullptr);
      ASSERT_EQ(!blog && !buffered,
                dynamic_cast<LegacyWalReader*>(reader.get()) != nullptr);
      ASSERT_EQ(!blog && buffered, dynamic_cast<BufferedLegacyWalReader*>(
                                       reader.get()) != nullptr);
      Slice record;
      std::string scratch;
      ASSERT_TRUE(reader->ReadRecord(&record, &scratch));
      ASSERT_FALSE(reader->ReadRecord(&record, &scratch));
      ASSERT_EQ(0U, reporter.failing_count());
    }
  }
}

TEST_F(DBWALTest, BufferedLegacyWalReaderKeepsPrimedPartialRecord) {
  std::string contents;
  uint64_t log_number = 0;
  WriteWalForOpenTest(/*blog=*/false, &contents, &log_number);
  ASSERT_GT(contents.size(), log::kHeaderSize);
  for (size_t prefix_size = log::kHeaderSize; prefix_size < contents.size();
       ++prefix_size) {
    SCOPED_TRACE("prefix_size=" + std::to_string(prefix_size));
    std::string visible(contents.data(), prefix_size);
    CountingWalReporter reporter;
    std::unique_ptr<GrowingSequentialFile> source =
        std::make_unique<GrowingSequentialFile>(&visible);
    GrowingSequentialFile* source_ptr = source.get();
    std::unique_ptr<SequentialFileReader> file(
        new SequentialFileReader(std::move(source), "primed-legacy-wal"));
    std::unique_ptr<WalReaderOpenState> pending_open;
    std::unique_ptr<WalReader> reader =
        NewWalReader(nullptr, std::move(file), &reporter, true, log_number,
                     false, false, std::numeric_limits<uint64_t>::max(),
                     PredecessorWALInfo(), /*buffered=*/true, &pending_open);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(nullptr, pending_open);
    ASSERT_EQ(1U, source_ptr->read_count);
    ASSERT_NE(nullptr, dynamic_cast<BufferedLegacyWalReader*>(reader.get()));
    Slice record;
    std::string scratch;
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch));
    ASSERT_EQ(0U, reporter.failing_count());
    visible = contents;
    reader->UnmarkEOF();
    const size_t primed_read_count = source_ptr->read_count;
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch));
    ASSERT_EQ(primed_read_count, source_ptr->read_count);
    WriteBatch batch;
    ASSERT_OK(WriteBatchInternal::SetContents(&batch, record));
    ASSERT_EQ(1U, WriteBatchInternal::Sequence(&batch));
    ASSERT_EQ(1, WriteBatchInternal::Count(&batch));
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch));
    ASSERT_EQ(0U, reporter.failing_count());
  }
}

TEST_F(DBWALTest, WalReaderFactoryReadErrorsAreTerminal) {
  for (size_t prefix_size : {size_t{0}, size_t{3}}) {
    SCOPED_TRACE("prefix_size=" + std::to_string(prefix_size));
    CountingWalReporter reporter;
    std::string contents(kBlogFileMagic, kBlogFileMagicSize);
    std::unique_ptr<FaultySequentialFile> source =
        std::make_unique<FaultySequentialFile>(contents, prefix_size);
    FaultySequentialFile* source_ptr = source.get();
    std::unique_ptr<SequentialFileReader> file(
        new SequentialFileReader(std::move(source), "faulty-prefix"));
    std::unique_ptr<WalReaderOpenState> pending_open;
    std::unique_ptr<WalReader> reader =
        NewWalReader(nullptr, std::move(file), &reporter, true, 1, false, false,
                     std::numeric_limits<uint64_t>::max(), PredecessorWALInfo(),
                     true, &pending_open);
    ASSERT_EQ(nullptr, reader);
    ASSERT_NE(nullptr, pending_open);
    ASSERT_EQ(1U, source_ptr->read_count);
    if (prefix_size != 0) {
      ASSERT_EQ(0U, reporter.failing_count());
    }
    ASSERT_EQ(WalReaderOpenState::Result::kReadError,
              pending_open->TryOpen(&reader));
    ASSERT_EQ(1U, reporter.failing_count());
    ASSERT_TRUE(reporter.first_status.IsIOError());
    const size_t failed_read_count = source_ptr->read_count;
    for (int retry = 0; retry < 3; ++retry) {
      ASSERT_EQ(WalReaderOpenState::Result::kReadError,
                pending_open->TryOpen(&reader));
    }
    ASSERT_EQ(failed_read_count, source_ptr->read_count);
    ASSERT_EQ(1U, reporter.failing_count());
  }
}

TEST_F(DBWALTest, BlogWalReaderDefersDetectionAndRetriesPartialRecord) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(db_->FlushWAL(true));

  std::unique_ptr<LogFile> wal_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
  std::string complete_contents;
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, wal_file->LogNumber()),
                             &complete_contents));

  const uint32_t property_size = DecodeFixed32(
      complete_contents.data() + kBlogFilePropertySectionSizeOffset);
  size_t record_offset = kBlogFileFixedHeaderSize + property_size;
  while (record_offset < complete_contents.size() &&
         (complete_contents[record_offset] == '\0' ||
          complete_contents[record_offset] == '\xff')) {
    ++record_offset;
  }
  ASSERT_LT(record_offset + kBlogEscapeSequenceSize + 1,
            complete_contents.size());

  size_t compact_length_varint_offset = 0;
  uint64_t compact_payload_length = 0;
  FindFirstCompactRecord(complete_contents, &compact_length_varint_offset,
                         &compact_payload_length);
  const size_t compact_record_offset =
      compact_length_varint_offset - kBlogEscapeSequenceSize;
  const size_t compact_record_end =
      compact_length_varint_offset +
      PrefixVarint64AddlByteCount(
          complete_contents[compact_length_varint_offset]) +
      1 + static_cast<size_t>(compact_payload_length) + kBlogBlockTrailerSize;

  // A tailing reader (for example a secondary DB, which constructs its reader
  // with buffered=true) must treat every partially visible prefix of a compact
  // record as an incomplete trailing record in every recovery mode, never as a
  // length that overran a later record boundary.
  for (WALRecoveryMode wal_recovery_mode :
       {WALRecoveryMode::kTolerateCorruptedTailRecords,
        WALRecoveryMode::kAbsoluteConsistency,
        WALRecoveryMode::kPointInTimeRecovery,
        WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("wal_recovery_mode=" +
                 std::to_string(static_cast<int>(wal_recovery_mode)));
    std::string visible_contents;
    CountingWalReporter reporter;
    std::unique_ptr<FSSequentialFile> source(
        new GrowingSequentialFile(&visible_contents));
    std::unique_ptr<SequentialFileReader> file_reader(new SequentialFileReader(
        std::move(source), "growing-blog-wal" /* file name */));
    std::unique_ptr<WalReaderOpenState> pending_open;
    std::unique_ptr<WalReader> reader = NewWalReader(
        nullptr, std::move(file_reader), &reporter, /*checksum=*/true,
        wal_file->LogNumber(), /*track_and_verify_wals=*/false,
        /*stop_replay_for_corruption=*/false,
        std::numeric_limits<uint64_t>::max(), PredecessorWALInfo(),
        /*buffered=*/true, &pending_open);

    Slice record;
    std::string scratch;
    ASSERT_EQ(nullptr, reader);
    ASSERT_NE(nullptr, pending_open);
    ASSERT_EQ(WalReaderOpenState::Result::kPending,
              pending_open->TryOpen(&reader));

    visible_contents.assign(complete_contents.data(), 5);
    ASSERT_EQ(WalReaderOpenState::Result::kPending,
              pending_open->TryOpen(&reader));

    visible_contents.assign(complete_contents.data(),
                            record_offset + kBlogEscapeSequenceSize + 1);
    ASSERT_EQ(WalReaderOpenState::Result::kReady,
              pending_open->TryOpen(&reader));
    pending_open.reset();
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch, wal_recovery_mode));

    for (size_t visible_end = compact_record_offset + 1;
         visible_end < compact_record_end; ++visible_end) {
      visible_contents.assign(complete_contents.data(), visible_end);
      ASSERT_FALSE(reader->ReadRecord(&record, &scratch, wal_recovery_mode));
      ASSERT_EQ(reporter.failing_count(), 0u)
          << "visible_end=" << visible_end << " "
          << reporter.first_status.ToString();
    }

    visible_contents.assign(complete_contents);
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch, wal_recovery_mode));
    ASSERT_GE(record.size(), WriteBatchInternal::kHeader);
    ASSERT_EQ(reporter.failing_count(), 0u) << reporter.first_status.ToString();
  }
}

TEST_F(DBWALTest, BlogWalReaderTailingPartialHeaderIsSilent) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(db_->FlushWAL(true));

  std::unique_ptr<LogFile> wal_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
  std::string complete_contents;
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, wal_file->LogNumber()),
                             &complete_contents));

  const uint32_t property_size = DecodeFixed32(
      complete_contents.data() + kBlogFilePropertySectionSizeOffset);
  size_t record_offset = kBlogFileFixedHeaderSize + property_size;
  while (record_offset < complete_contents.size() &&
         (complete_contents[record_offset] == '\0' ||
          complete_contents[record_offset] == '\xff')) {
    ++record_offset;
  }
  ASSERT_LT(record_offset + kBlogEscapeSequenceSize, complete_contents.size());

  // A tailing reader (for example a secondary DB, which constructs its reader
  // with buffered=true) must treat every partially visible header prefix as
  // an incomplete tail in every recovery mode, never as a loud corruption.
  for (WALRecoveryMode wal_recovery_mode :
       {WALRecoveryMode::kTolerateCorruptedTailRecords,
        WALRecoveryMode::kAbsoluteConsistency,
        WALRecoveryMode::kPointInTimeRecovery,
        WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("wal_recovery_mode=" +
                 std::to_string(static_cast<int>(wal_recovery_mode)));
    std::string visible_contents;
    CountingWalReporter reporter;
    std::unique_ptr<FSSequentialFile> source(
        new GrowingSequentialFile(&visible_contents));
    std::unique_ptr<SequentialFileReader> file_reader(new SequentialFileReader(
        std::move(source), "growing-blog-wal" /* file name */));
    std::unique_ptr<WalReaderOpenState> pending_open;
    std::unique_ptr<WalReader> reader = NewWalReader(
        nullptr, std::move(file_reader), &reporter, /*checksum=*/true,
        wal_file->LogNumber(), /*track_and_verify_wals=*/false,
        /*stop_replay_for_corruption=*/false,
        std::numeric_limits<uint64_t>::max(), PredecessorWALInfo(),
        /*buffered=*/true, &pending_open);

    Slice record;
    std::string scratch;
    for (size_t visible_end = kBlogFileMagicSize;
         visible_end <= record_offset + kBlogEscapeSequenceSize;
         ++visible_end) {
      SCOPED_TRACE("visible_end=" + std::to_string(visible_end));
      visible_contents.assign(complete_contents.data(), visible_end);
      if (pending_open) {
        ASSERT_EQ(WalReaderOpenState::Result::kReady,
                  pending_open->TryOpen(&reader));
        pending_open.reset();
      }
      ASSERT_FALSE(reader->ReadRecord(&record, &scratch, wal_recovery_mode));
      ASSERT_EQ(reporter.failing_count(), 0u)
          << "visible_end=" << visible_end << " "
          << reporter.first_status.ToString();
    }
  }
}

// Sibling of the header-silence test past delivery: a tailing reader that
// already delivered record 1 stays silent when the file ends anywhere inside
// the following gap (or inside record 2). Fresh reader per sweep point so
// every point asserts the same delivery sequence.
TEST_F(DBWALTest, BlogWalReaderTailingTornGapAfterDeliveryIsSilent) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(db_->FlushWAL(true));

  std::unique_ptr<LogFile> wal_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
  std::string complete_contents;
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, wal_file->LogNumber()),
                             &complete_contents));

  const uint32_t property_size = DecodeFixed32(
      complete_contents.data() + kBlogFilePropertySectionSizeOffset);
  size_t first_offset = kBlogFileFixedHeaderSize + property_size;
  while (first_offset < complete_contents.size() &&
         (complete_contents[first_offset] == '\0' ||
          complete_contents[first_offset] == '\xff')) {
    ++first_offset;
  }
  ASSERT_LT(first_offset, complete_contents.size());

  std::string visible_contents;
  auto make_tailing_reader = [&](CountingWalReporter* reporter) {
    std::unique_ptr<FSSequentialFile> source(
        new GrowingSequentialFile(&visible_contents));
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source), "growing-blog-wal"));
    std::unique_ptr<WalReaderOpenState> pending_open;
    return NewWalReader(nullptr, std::move(file_reader), reporter,
                        /*checksum=*/true, wal_file->LogNumber(),
                        /*track_and_verify_wals=*/false,
                        /*stop_replay_for_corruption=*/false,
                        std::numeric_limits<uint64_t>::max(),
                        PredecessorWALInfo(), /*buffered=*/true, &pending_open);
  };
  constexpr WALRecoveryMode kProbeMode =
      WALRecoveryMode::kTolerateCorruptedTailRecords;

  // Control: the complete file delivers both batches, then clean EOF.
  CountingWalReporter control_reporter;
  std::string rec1_control;
  std::string rec2_control;
  visible_contents = complete_contents;
  {
    std::unique_ptr<WalReader> reader = make_tailing_reader(&control_reporter);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch, kProbeMode));
    rec1_control = record.ToString();
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch, kProbeMode));
    rec2_control = record.ToString();
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch, kProbeMode));
    ASSERT_EQ(control_reporter.failing_count(), 0u);
  }

  // Locate record 1's end empirically: the smallest visible prefix delivering
  // it. (Parsing the boundary would have to replicate preamble and ignorable
  // record skipping.) Delivery implies completeness and vice versa, so the
  // minimum is exact; a regression that withholds complete records fails here
  // instead of silently shrinking the sweep below.
  CountingWalReporter search_reporter;
  size_t rec1_end = 0;
  for (size_t end = first_offset + 1; end <= complete_contents.size(); ++end) {
    visible_contents.assign(complete_contents.data(), end);
    std::unique_ptr<WalReader> reader = make_tailing_reader(&search_reporter);
    Slice record;
    std::string scratch;
    if (reader->ReadRecord(&record, &scratch, kProbeMode) &&
        record.ToString() == rec1_control) {
      rec1_end = end;
      break;
    }
  }
  ASSERT_GT(rec1_end, first_offset);
  ASSERT_LT(rec1_end, complete_contents.size());
  ASSERT_EQ(search_reporter.failing_count(), 0u);

  for (WALRecoveryMode wal_recovery_mode :
       {WALRecoveryMode::kTolerateCorruptedTailRecords,
        WALRecoveryMode::kAbsoluteConsistency,
        WALRecoveryMode::kPointInTimeRecovery,
        WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("wal_recovery_mode=" +
                 std::to_string(static_cast<int>(wal_recovery_mode)));
    CountingWalReporter reporter;
    for (size_t visible_end = rec1_end; visible_end <= complete_contents.size();
         ++visible_end) {
      SCOPED_TRACE("visible_end=" + std::to_string(visible_end));
      visible_contents.assign(complete_contents.data(), visible_end);
      std::unique_ptr<WalReader> reader = make_tailing_reader(&reporter);
      Slice record;
      std::string scratch;
      ASSERT_TRUE(reader->ReadRecord(&record, &scratch, wal_recovery_mode));
      ASSERT_EQ(record.ToString(), rec1_control);
      int extra_records = 0;
      while (reader->ReadRecord(&record, &scratch, wal_recovery_mode)) {
        ASSERT_EQ(record.ToString(), rec2_control);
        ++extra_records;
      }
      ASSERT_LE(extra_records, 1);
      ASSERT_EQ(reporter.failing_count(), 0u)
          << reporter.first_status.ToString();
    }
  }
}

TEST_F(DBWALTest, BlogWalPredecessorVerificationUsesBodySize) {
  for (bool streaming : {false, true}) {
    if (streaming && !StreamingCompressionTypeSupported(kZSTD)) {
      continue;
    }
    for (bool sealed : {false, true}) {
      for (bool blog_successor : {false, true}) {
        SCOPED_TRACE(streaming);
        SCOPED_TRACE(sealed);
        SCOPED_TRACE(blog_successor);
        Options options = CurrentOptions();
        options.use_blog_format_for_wals = true;
        options.track_and_verify_wals = true;
        options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
        options.avoid_flush_during_shutdown = true;
        options.max_write_buffer_number = 10;
        options.blog_wal_streaming_compression = streaming;
        options.wal_compression = streaming ? kZSTD : kNoCompression;
        DestroyAndReopen(options);
        ASSERT_OK(Put("first", "value-one"));
        ASSERT_OK(db_->FlushWAL(false));
        std::unique_ptr<LogFile> predecessor_file;
        ASSERT_OK(dbfull()->GetCurrentWalFile(&predecessor_file));
        const std::string predecessor_name =
            LogFileName(dbname_, predecessor_file->LogNumber());
        std::string unsealed;
        ASSERT_OK(ReadFileToString(env_, predecessor_name, &unsealed));
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
        ASSERT_OK(Put("second", "value-two"));
        std::unique_ptr<LogFile> successor_file;
        ASSERT_OK(dbfull()->GetCurrentWalFile(&successor_file));
        const std::string successor_name =
            LogFileName(dbname_, successor_file->LogNumber());
        Close();

        std::string successor_contents;
        ASSERT_OK(ReadFileToString(env_, successor_name, &successor_contents));
        Slice input(successor_contents);
        BlogFileHeader successor_header;
        ASSERT_OK(successor_header.DecodeFrom(&input));
        const std::string encoded =
            successor_header.GetProperty(kBlogPropPredecessorWalInfo);
        Slice predecessor_input(encoded);
        PredecessorWALInfo predecessor_info;
        ASSERT_OK(predecessor_info.DecodeFrom(&predecessor_input));
        ASSERT_TRUE(predecessor_input.empty());
        ASSERT_EQ(predecessor_info.GetLogNumber(),
                  predecessor_file->LogNumber());
        if (!sealed) {
          OverwriteFile(predecessor_name, unsealed);
        }
        if (!blog_successor) {
          std::unique_ptr<FSWritableFile> dest;
          ASSERT_OK(env_->GetFileSystem()->NewWritableFile(
              successor_name, FileOptions(), &dest, nullptr));
          LegacyWalWriter writer(
              std::make_unique<WritableFileWriter>(
                  std::move(dest), successor_name, FileOptions()),
              successor_file->LogNumber(), false, false, kNoCompression, true,
              0);
          ASSERT_OK(writer.WriteFileStart(WriteOptions(), predecessor_info));
          WriteBatch batch;
          ASSERT_OK(batch.Put("second", "value-two"));
          WriteBatchInternal::SetSequence(&batch, 2);
          ASSERT_OK(writer.AddRecord(
              WriteOptions(), WriteBatchInternal::Contents(&batch), 2, 1));
          ASSERT_OK(writer.Close(WriteOptions()));
        }
        uint64_t physical_size = 0;
        ASSERT_OK(env_->GetFileSize(predecessor_name, &physical_size));
        ASSERT_GE(physical_size, predecessor_info.GetSizeBytes());
        if (sealed) {
          ASSERT_GT(physical_size, predecessor_info.GetSizeBytes());
        }
        ASSERT_OK(TryReopen(options));
        ASSERT_EQ("value-one", Get("first"));
        ASSERT_EQ("value-two", Get("second"));
        Close();
      }
    }
  }
}

TEST_F(DBWALTest, BlogWalVerificationSizePreservesFooterRecoveryModes) {
  constexpr uint64_t kLogNumber = 101;
  std::string complete;
  std::unique_ptr<BlogFileWriter> writer =
      MakeHandBuiltWalWriter(&complete, kLogNumber, false);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch"));
  ASSERT_OK(writer->AddIgnorablePropertiesRecord(WriteOptions(),
                                                 {{"diagnostic", "tail"}}));
  const uint64_t body_end = writer->body_end_offset();
  const size_t unsealed_size = complete.size();
  ASSERT_OK(writer->WriteFooter(WriteOptions(), BlogFileFooterProperties{}));
  ASSERT_OK(writer->Close(WriteOptions()));
  // Reports per footer shape, indexed as {kAbsoluteConsistency,
  // kPointInTimeRecovery, kTolerateCorruptedTailRecords,
  // kSkipAnyCorruptedRecords}. Damage confined to footer records ends the WAL
  // cleanly in every mode and is only logged as a tolerated drop. A byte after
  // an intact locator is not footer damage and keeps the trailing-bytes policy.
  constexpr size_t kExpectedReports[5][4] = {
      {0, 0, 0, 0},  // Complete footer
      {0, 0, 0, 0},  // No footer
      {0, 0, 0, 0},  // Truncated locator
      {0, 0, 0, 0},  // Corrupt locator
      {1, 1, 0, 1},  // Byte after the locator
  };
  constexpr size_t kExpectedToleratedReports[5][4] = {
      {0, 0, 0, 0},  // Complete footer
      {0, 0, 0, 0},  // No footer
      {1, 1, 1, 1},  // Truncated locator
      {1, 1, 1, 1},  // Corrupt locator
      {0, 0, 1, 0},  // Byte after the locator
  };
  for (int shape = 0; shape < 5; ++shape) {
    std::string contents = complete;
    if (shape == 1) {
      contents.resize(unsealed_size);
    } else if (shape == 2) {
      contents.pop_back();
    } else if (shape == 3) {
      contents.back() ^= 1;
    } else if (shape == 4) {
      contents.push_back('x');
    }
    const WALRecoveryMode modes[] = {
        WALRecoveryMode::kAbsoluteConsistency,
        WALRecoveryMode::kPointInTimeRecovery,
        WALRecoveryMode::kTolerateCorruptedTailRecords,
        WALRecoveryMode::kSkipAnyCorruptedRecords};
    for (size_t m = 0; m < 4; ++m) {
      SCOPED_TRACE(shape);
      SCOPED_TRACE(lossless_cast<int>(modes[m]));
      CountingWalReporter reporter;
      std::unique_ptr<WalReader> reader =
          NewWalReaderOver(std::unique_ptr<FSSequentialFile>(
                               new GrowingSequentialFile(&contents)),
                           "/contract-wal", &reporter, kLogNumber);
      ASSERT_EQ(ReadAllRecords(reader.get(), modes[m]), 1u);
      ASSERT_EQ(reporter.failing_count(), kExpectedReports[shape][m]);
      ASSERT_EQ(reporter.tolerated_count, kExpectedToleratedReports[shape][m]);
      ASSERT_EQ(reader->GetVerificationSize(contents.size()), body_end);
    }
  }
}

TEST_F(DBWALTest, BlogWalTornFooterEndsCleanly) {
  // Close does not sync the footer, so a crash can keep any prefix of it.
  // Every tear ends the WAL cleanly except one inside the first footer
  // record's prefix: before the prefix checksum authenticates the footer
  // type, the tear is indistinguishable from a torn data record. Like the DB
  // writer, record a sequence range so the properties use the full format.
  constexpr uint64_t kLogNumber = 101;
  std::string complete;
  std::unique_ptr<BlogFileWriter> writer =
      MakeHandBuiltWalWriter(&complete, kLogNumber, false);
  ASSERT_OK(writer->WriteHeader(WriteOptions()));
  ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch"));
  const size_t footer_start = complete.size();
  BlogFileFooterProperties properties;
  properties.SetSequenceRange(1, 1);
  ASSERT_OK(writer->WriteFooter(WriteOptions(), properties));
  ASSERT_OK(writer->Close(WriteOptions()));
  const char* varint_start =
      complete.data() + footer_start + kBlogEscapeSequenceSize;
  uint64_t length = 0;
  const char* varint_end = GetPrefixVarint64Ptr(
      varint_start, complete.data() + complete.size(), &length);
  ASSERT_NE(varint_end, nullptr);
  ASSERT_GT(lossless_cast<size_t>(varint_end - varint_start),
            kBlogCompactVarintMaxBytes);
  const size_t authenticated_prefix_end =
      lossless_cast<size_t>(varint_end - complete.data()) +
      kBlogFullFormatPrefixRestSize;
  for (size_t cut = footer_start + 1; cut < complete.size(); ++cut) {
    const std::string contents = complete.substr(0, cut);
    for (WALRecoveryMode mode : {WALRecoveryMode::kAbsoluteConsistency,
                                 WALRecoveryMode::kPointInTimeRecovery,
                                 WALRecoveryMode::kTolerateCorruptedTailRecords,
                                 WALRecoveryMode::kSkipAnyCorruptedRecords}) {
      SCOPED_TRACE(cut);
      SCOPED_TRACE(lossless_cast<int>(mode));
      CountingWalReporter reporter;
      std::unique_ptr<WalReader> reader =
          NewWalReaderOver(std::unique_ptr<FSSequentialFile>(
                               new GrowingSequentialFile(&contents)),
                           "/torn-footer-wal", &reporter, kLogNumber);
      ASSERT_EQ(ReadAllRecords(reader.get(), mode), 1u);
      const bool reports_torn_tail =
          cut < authenticated_prefix_end &&
          (mode == WALRecoveryMode::kAbsoluteConsistency ||
           mode == WALRecoveryMode::kPointInTimeRecovery);
      ASSERT_EQ(reporter.failing_count(), reports_torn_tail ? 1u : 0u);
    }
  }
}

TEST_F(DBWALTest, BlogWalTornFooterOpensWithAbsoluteConsistency) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);
  WriteOptions sync_write;
  sync_write.sync = true;
  ASSERT_OK(Put("key", "value", sync_write));
  std::unique_ptr<LogFile> wal;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal));
  const std::string wal_name = LogFileName(dbname_, wal->LogNumber());
  Close();

  // The footer appended by Close is the only unsynced data; tear its locator.
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, wal_name, &contents));
  contents.pop_back();
  OverwriteFile(wal_name, contents);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("value", Get("key"));
  Close();
}

TEST_F(DBWALTest, BlogWalSealedOnShutdownAfterRotation) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  options.max_write_buffer_number = 10;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "value1"));
  std::unique_ptr<LogFile> old_wal;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&old_wal));
  const std::string old_wal_name = LogFileName(dbname_, old_wal->LogNumber());
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  auto verify_sealed = [&](const std::string& fname) {
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, fname, &contents));
    Slice input(contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&input));
    BlogFileFooterLocator locator;
    ASSERT_TRUE(VerifyBlogFooterLocator(
        contents.data(), contents.size(), /*buffer_file_offset=*/0,
        header.escape_sequence, header.checksum_type, header.incarnation_id(),
        &locator));
    ASSERT_FALSE(locator.entries.empty());
  };

  ASSERT_OK(Put("key2", "value2"));
  std::unique_ptr<LogFile> active_wal;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&active_wal));
  const std::string active_wal_name =
      LogFileName(dbname_, active_wal->LogNumber());
  Close();
  verify_sealed(old_wal_name);
  verify_sealed(active_wal_name);

  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_EQ("value1", Get("key1"));
  ASSERT_EQ("value2", Get("key2"));
}

TEST_F(DBWALTest, BlogWalCompression) {
  if (!CompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_BYPASS("ZSTD is not supported");
    return;
  }

  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.wal_compression = kZSTD;
  DestroyAndReopen(options);

  const std::string value(10000, 'v');
  ASSERT_OK(Put("key", value));
  Reopen(options);
  ASSERT_EQ(value, Get("key"));
}

TEST_F(DBWALTest, BlogWalStreamingCompression) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_BYPASS("Test requires ZSTD streaming support");
    return;
  }
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.wal_compression = kZSTD;
  options.blog_wal_streaming_compression = true;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
  }
  // Larger than the reader's 32 KiB output chunk, so decoding drains it.
  const std::string big_value(100 * 1024, 'b');
  ASSERT_OK(Put("big-key", big_value));

  std::unique_ptr<LogFile> wal_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
  std::string wal_contents;
  ASSERT_OK(ReadFileToString(env_, LogFileName(dbname_, wal_file->LogNumber()),
                             &wal_contents));
  Slice header_input(wal_contents);
  BlogFileHeader header;
  ASSERT_OK(header.DecodeFrom(&header_input));
  ASSERT_EQ(header.GetProperty(kBlogPropWriteBatchStreamingCompressionType),
            "07");
  Close();

  // All values below must come from WAL replay, not flushed SSTs.
  std::vector<std::string> children;
  ASSERT_OK(env_->GetChildren(dbname_, &children));
  for (const auto& child : children) {
    uint64_t number = 0;
    FileType type = kWalFile;
    if (ParseFileName(child, &number, &type)) {
      ASSERT_NE(type, kTableFile) << child;
    }
  }

  auto verify_all = [&]() {
    for (int i = 0; i < 100; ++i) {
      ASSERT_EQ("value" + std::to_string(i), Get("key" + std::to_string(i)));
    }
    ASSERT_EQ(big_value, Get("big-key"));
  };

  // Reading works with the option on ...
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  verify_all();
  Close();

  // ... and existing files stay readable with the option off.
  options.blog_wal_streaming_compression = false;
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  verify_all();
}

// A crash that cuts off only padding still fails a strict open: every record
// is intact, but the torn tail is reported like any other torn tail. This
// matches the legacy WAL behavior pinned by kAbsoluteConsistency.
TEST_F(DBWALTest, BlogWalTornGapFailsAbsoluteConsistency) {
  TornWalFixture fixture;
  BuildTornWal(&fixture, /*tear_mid_record=*/false);
  ASSERT_GT(fixture.torn_size, 0u);
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.create_if_missing = false;
  const Status s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  AssertWalIsCutOff(fixture, /*tear_mid_record=*/false);
}

// A torn record still fails a strict open; the last batch is lost.
TEST_F(DBWALTest, BlogWalTornRecordFailsAbsoluteConsistency) {
  TornWalFixture fixture;
  BuildTornWal(&fixture, /*tear_mid_record=*/true);
  ASSERT_GT(fixture.torn_size, 0u);
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.create_if_missing = false;
  const Status s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  AssertWalIsCutOff(fixture, /*tear_mid_record=*/true);
}

// Point-in-time recovery stops at the torn gap with every record replayed.
TEST_F(DBWALTest, BlogWalTornGapStopsPointInTimeRecovery) {
  TornWalFixture fixture;
  BuildTornWal(&fixture, /*tear_mid_record=*/false);
  ASSERT_GT(fixture.torn_size, 0u);
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.create_if_missing = false;
  // Keep the replayed WAL on disk so the cut-off assertion below can read
  // it; otherwise recovery flushes and deletes it as obsolete.
  options.avoid_flush_during_recovery = true;
  ASSERT_OK(TryReopen(options));
  AssertWalIsCutOff(fixture, /*tear_mid_record=*/false);
  ASSERT_EQ("value1", Get("key1"));
  ASSERT_EQ("value2", Get("key2"));
}

TEST_F(DBWALTest, BlogWalStreamingCompressionRequiresStreamingType) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.blog_wal_streaming_compression = true;
  Destroy(options);
  for (CompressionType compression : {kNoCompression, kSnappyCompression}) {
    options.wal_compression = compression;
    Status s = DB::Open(options, dbname_, &db_);
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    ASSERT_NE(s.ToString().find("blog_wal_streaming_compression requires "
                                "wal_compression=kZSTD"),
              std::string::npos)
        << s.ToString();
  }
}

TEST_F(DBWALTest, BlogWalStreamingCompressionRequiresBlogFormat) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = false;
  options.wal_compression = kZSTD;
  options.blog_wal_streaming_compression = true;
  Destroy(options);
  Status s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_NE(s.ToString().find("blog_wal_streaming_compression requires "
                              "use_blog_format_for_wals=true"),
            std::string::npos)
      << s.ToString();
}

TEST_F(DBWALTest, BlogWalReaderStreamingRecords) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_BYPASS("Test requires ZSTD streaming support");
    return;
  }
  constexpr uint64_t kLogNumber = 7;
  constexpr WALRecoveryMode kMode =
      WALRecoveryMode::kTolerateCorruptedTailRecords;
  const WriteOptions wo;

  // Streamed, raw, then streamed: the raw record is not part of the stream.
  std::string ordered_contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltStreamingWalWriter(&ordered_contents, kLogNumber, "07");
    ASSERT_OK(writer->WriteHeader(wo));
    std::vector<std::string> fragments =
        CompressStreamFragments({"streamed-one", "streamed-two"});
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[0],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, "raw-one", kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[1],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->Close(wo));
  }
  {
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&ordered_contents, &reporter, kLogNumber);
    Slice record;
    std::string scratch;
    std::vector<std::string> records;
    while (reader->ReadRecord(&record, &scratch, kMode)) {
      records.emplace_back(record.data(), record.size());
    }
    ASSERT_EQ(records, std::vector<std::string>(
                           {"streamed-one", "raw-one", "streamed-two"}));
    ASSERT_EQ(reporter.failing_count(), 0u) << reporter.first_status.ToString();
  }

  // A streamed record whose stream predecessor is missing: the raw record
  // reads, then decompression fails.
  Random rnd(117);
  std::string history(4096, '\0');
  for (size_t i = 0; i < history.size(); ++i) {
    history[i] = static_cast<char>(rnd.Next());
  }
  std::string missing_contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltStreamingWalWriter(&missing_contents, kLogNumber, "07");
    ASSERT_OK(writer->WriteHeader(wo));
    std::vector<std::string> fragments =
        CompressStreamFragments({history, history});
    // Tiny output proves the fragment references the missing history, so
    // the decompression failure below is meaningful, not accidental.
    ASSERT_LT(fragments[1].size(), 64u);
    ASSERT_OK(writer->AddWriteBatchRecord(wo, "raw-one", kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[1],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->Close(wo));
  }
  {
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&missing_contents, &reporter, kLogNumber);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch, kMode));
    ASSERT_EQ(record, "raw-one");
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch, kMode));
    ASSERT_EQ(reporter.failing_count(), 1u);
    ASSERT_NE(
        reporter.first_status.ToString().find("could not decompress streaming"),
        std::string::npos)
        << reporter.first_status.ToString();
  }
}

TEST_F(DBWALTest, BlogWalStreamingRecoveryAfterBrokenStream) {
  if (!StreamingCompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_BYPASS("Test requires ZSTD streaming support");
    return;
  }
  constexpr uint64_t kLogNumber = 7;
  const WriteOptions wo;

  // r2 references r1, so a missing r1 makes r2 undecodable rather than
  // silently wrong.
  Random rnd(1211);
  std::string history(4096, '\0');
  for (size_t i = 0; i < history.size(); ++i) {
    history[i] = static_cast<char>(rnd.Next());
  }
  const std::vector<std::string> fragments =
      CompressStreamFragments({history, history, "r3"});
  // Tiny output proves the fragment references the missing history, so a
  // decompression failure below is meaningful, not accidental.
  ASSERT_LT(fragments[1].size(), 64u);

  struct BrokenStreamCase {
    std::string name;
    std::string contents;
    std::vector<std::string> skip_any_records;
    std::vector<std::string> stopped_records;
  };
  // r1 is left out of the file: the first streamed record has no stream
  // start to decode from.
  BrokenStreamCase missing_case;
  missing_case.name = "missing-r1";
  {
    std::unique_ptr<BlogFileWriter> writer = MakeHandBuiltStreamingWalWriter(
        &missing_case.contents, kLogNumber, "07");
    ASSERT_OK(writer->WriteHeader(wo));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[1],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, "raw", kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[2],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->Close(wo));
  }
  missing_case.skip_any_records = {"raw"};
  // The first record is undecodable, so the other modes stop with nothing.
  missing_case.stopped_records = {};
  // r2's payload is damaged: the record is skipped, breaking the stream.
  BrokenStreamCase corrupt_case;
  corrupt_case.name = "corrupt-r2";
  {
    std::unique_ptr<BlogFileWriter> writer = MakeHandBuiltStreamingWalWriter(
        &corrupt_case.contents, kLogNumber, "07");
    ASSERT_OK(writer->WriteHeader(wo));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[0],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[1],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, "raw", kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, fragments[2],
                                          kStreamingCompressionSentinel));
    ASSERT_OK(writer->Close(wo));
  }
  std::vector<BlogWalRecordLayout> layout =
      ParseBlogWalRecordLayout(corrupt_case.contents);
  ASSERT_EQ(layout.size(), 4u);
  corrupt_case.contents[layout[1].payload_offset + 2] ^= 0x01;
  corrupt_case.skip_any_records = {history, "raw"};
  corrupt_case.stopped_records = {history};

  for (const BrokenStreamCase* c : {&missing_case, &corrupt_case}) {
    SCOPED_TRACE(c->name);
    // SkipAny recovers around the break; r3 is dropped without decoding.
    {
      CountingWalReporter reporter;
      std::unique_ptr<WalReader> reader =
          NewMemWalReader(&c->contents, &reporter, kLogNumber);
      Slice record;
      std::string scratch;
      std::vector<std::string> records;
      while (reader->ReadRecord(&record, &scratch,
                                WALRecoveryMode::kSkipAnyCorruptedRecords)) {
        records.emplace_back(record.data(), record.size());
      }
      ASSERT_EQ(records, c->skip_any_records);
      ASSERT_EQ(reporter.failing_count(), 2u);
      ASSERT_NE(reporter.last_status.ToString().find(
                    "streaming Blog WAL record follows a skipped or "
                    "undecodable record"),
                std::string::npos)
          << reporter.last_status.ToString();
    }
    // Other modes stop at the first undecodable record.
    for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                                 WALRecoveryMode::kAbsoluteConsistency,
                                 WALRecoveryMode::kPointInTimeRecovery}) {
      SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
      CountingWalReporter reporter;
      std::unique_ptr<WalReader> reader =
          NewMemWalReader(&c->contents, &reporter, kLogNumber);
      Slice record;
      std::string scratch;
      std::vector<std::string> records;
      while (reader->ReadRecord(&record, &scratch, mode)) {
        records.emplace_back(record.data(), record.size());
      }
      ASSERT_EQ(records, c->stopped_records);
      ASSERT_EQ(reporter.failing_count(), 1u);
    }
  }
}

TEST_F(DBWALTest, BlogWalReaderUnsupportedStreamingType) {
  // A header declaring a streaming type this build cannot decode (Snappy)
  // is reported as NotSupported, not corruption or a misread. The writer
  // refuses to produce such a file, so the header is encoded directly.
  // No ZSTD guard: rejection happens before any decoder is created.
  constexpr uint64_t kLogNumber = 7;
  std::string contents;
  BlogFileHeader header = MakeStreamingWalHeader(kLogNumber, "01");
  header.EncodeTo(&contents);
  const BlogPadding pad(lossless_cast<uint8_t>(contents.back()),
                        contents.size(), kBlogMinRecordAlignment,
                        /*prefer_0xff_when_legal=*/false);
  pad.AppendTo(&contents);

  CountingWalReporter reporter;
  std::unique_ptr<WalReader> reader =
      NewMemWalReader(&contents, &reporter, kLogNumber);
  Slice record;
  std::string scratch;
  ASSERT_FALSE(reader->ReadRecord(
      &record, &scratch, WALRecoveryMode::kTolerateCorruptedTailRecords));
  ASSERT_EQ(reporter.failing_count(), 1u);
  ASSERT_TRUE(reporter.first_status.IsNotSupported())
      << reporter.first_status.ToString();
  ASSERT_NE(reporter.first_status.ToString().find(
                "only ZSTD (0x07) currently supported"),
            std::string::npos)
      << reporter.first_status.ToString();
}

TEST_F(DBWALTest, BlogWalReaderUnsupportedRecordCompression) {
  // A record whose trailer compression byte names a codec this build cannot
  // decode (a custom type under BuiltinV2) is readable data of unknown
  // meaning, not damage: every recovery mode must report it as unrecoverable
  // (NotSupported) and replay nothing, like an unknown record type. No codec
  // guard: custom types are never built in.
  constexpr uint64_t kLogNumber = 7;
  const WriteOptions wo;

  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, kLogNumber);
    ASSERT_OK(writer->WriteHeader(wo));
    // Encoded uncompressed size followed by opaque bytes: size extraction
    // succeeds, then decoding fails with NotSupported.
    std::string custom_payload;
    PutVarint64(&custom_payload, 8);
    custom_payload.append("custom-data");
    ASSERT_OK(writer->AddWriteBatchRecord(wo, custom_payload,
                                          kFirstCustomCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(wo, "after", kNoCompression));
    ASSERT_OK(writer->Close(wo));
  }

  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kAbsoluteConsistency,
                               WALRecoveryMode::kPointInTimeRecovery,
                               WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE(lossless_cast<int>(mode));
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&contents, &reporter, kLogNumber);
    ASSERT_EQ(ReadAllRecords(reader.get(), mode), 0u);
    ASSERT_EQ(reporter.failing_count(), 1u);
    ASSERT_EQ(reporter.unrecoverable_count, 1u);
    ASSERT_TRUE(reporter.first_status.IsNotSupported())
        << reporter.first_status.ToString();
    ASSERT_NE(reporter.first_status.ToString().find("not built-in"),
              std::string::npos)
        << reporter.first_status.ToString();
  }
}

TEST_F(DBWALTest, BlogWalRecoveryMidFileVsTailDamage) {
  const struct {
    int corrupt_idx;
    WALRecoveryMode mode;
    bool expect_ok;
    std::vector<std::string> present;
    std::vector<std::string> absent;
  } cases[] = {
      // Mid-file damage: SkipAny resumes past it, Tolerate/Absolute
      // refuse, PointInTime stops before it.
      {1,
       WALRecoveryMode::kSkipAnyCorruptedRecords,
       true,
       {"key1", "key3"},
       {"key2"}},
      {1, WALRecoveryMode::kTolerateCorruptedTailRecords, false, {}, {}},
      {1, WALRecoveryMode::kAbsoluteConsistency, false, {}, {}},
      {1,
       WALRecoveryMode::kPointInTimeRecovery,
       true,
       {"key1"},
       {"key2", "key3"}},
      // Footer damage (torn seal; corrupt_idx -1 truncates the file tail
      // inside the footer): every mode accepts it.
      {-1,
       WALRecoveryMode::kSkipAnyCorruptedRecords,
       true,
       {"key1", "key2", "key3"},
       {}},
      {-1,
       WALRecoveryMode::kTolerateCorruptedTailRecords,
       true,
       {"key1", "key2", "key3"},
       {}},
      {-1,
       WALRecoveryMode::kAbsoluteConsistency,
       true,
       {"key1", "key2", "key3"},
       {}},
      {-1,
       WALRecoveryMode::kPointInTimeRecovery,
       true,
       {"key1", "key2", "key3"},
       {}},
  };
  for (const auto& c : cases) {
    SCOPED_TRACE("corrupt_idx=" + std::to_string(c.corrupt_idx) +
                 " mode=" + std::to_string(static_cast<int>(c.mode)));
    Options options = CurrentOptions();
    options.use_blog_format_for_wals = true;
    options.avoid_flush_during_shutdown = true;
    options.track_and_verify_wals = false;
    std::string fname;
    WriteThreeRecordBlogWal(options, &fname);
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, fname, &contents));
    if (c.corrupt_idx < 0) {
      ASSERT_GT(contents.size(), 64u);
      contents.resize(contents.size() - 10);
    } else {
      std::vector<BlogWalRecordLayout> layout =
          ParseBlogWalRecordLayout(contents);
      ASSERT_EQ(layout.size(), 3u);
      ASSERT_GT(layout[c.corrupt_idx].length, 8u);
      contents[layout[c.corrupt_idx].payload_offset + 2] ^= 0x01;
    }
    OverwriteFile(fname, contents);

    options.wal_recovery_mode = c.mode;
    Status s = DB::Open(options, dbname_, &db_);
    if (!c.expect_ok) {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
      continue;
    }
    ASSERT_OK(s);
    for (const std::string& key : c.present) {
      ASSERT_EQ(std::string("value") + key.back(), Get(key));
    }
    for (const std::string& key : c.absent) {
      ASSERT_EQ("NOT_FOUND", Get(key));
    }
    Close();
  }
}

TEST_F(DBWALTest, BlogWalRecycledStaleTailTruncatesSilently) {
  for (int recycle_log_file_num : {1, 0}) {
    SCOPED_TRACE("recycle_log_file_num=" +
                 std::to_string(recycle_log_file_num));
    Options options = CurrentOptions();
    options.use_blog_format_for_wals = true;
    options.recycle_log_file_num = recycle_log_file_num;
    options.avoid_flush_during_shutdown = true;
    options.write_buffer_size = 10 * 1024 * 1024;
    options.max_write_buffer_number = 10;
    options.compression = kNoCompression;
    options.track_and_verify_wals = false;

    std::string fname;
    if (recycle_log_file_num > 0) {
      uint64_t log_number = 0;
      CreateRecycledBlogWal(options, &fname, &log_number);
    } else {
      WriteThreeRecordBlogWal(options, &fname);
    }
    Close();

    // Stale bytes past the seal carry no escape sequence of ours.
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, fname, &contents));
    contents.append(128, '\xab');
    OverwriteFile(fname, contents);

    options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
    Status s = DB::Open(options, dbname_, &db_);
    if (recycle_log_file_num > 0) {
      ASSERT_OK(s);
      ASSERT_EQ("value3", Get("key3"));
      ASSERT_EQ("value3b", Get("key3b"));
      Close();
    } else {
      ASSERT_TRUE(s.IsCorruption()) << s.ToString();
    }
  }
}

TEST_F(DBWALTest, BlogWalRecycledStaleTailStopsPointInTimeReplay) {
  // Unsynced records of a recycled WAL lost back to the previous
  // incarnation's bytes look like its stale tail. kPointInTimeRecovery must
  // then skip a later WAL that does not continue the sequence.
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.recycle_log_file_num = 1;
  options.avoid_flush_during_shutdown = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_write_buffer_number = 10;
  options.compression = kNoCompression;
  options.track_and_verify_wals = false;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  std::string recycled_fname;
  uint64_t recycled_log_number = 0;
  CreateRecycledBlogWal(options, &recycled_fname, &recycled_log_number);
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key4", "value4"));
  Close();

  // Replace key3b's record and everything after it with bytes that carry no
  // escape sequence of this WAL.
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, recycled_fname, &contents));
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 2u);
  const size_t lost_from = static_cast<size_t>(layout[1].record_offset);
  contents.replace(lost_from, std::string::npos, contents.size() - lost_from,
                   '\xab');
  OverwriteFile(recycled_fname, contents);

  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_EQ("value2", Get("key2"));
  ASSERT_EQ("value3", Get("key3"));
  ASSERT_EQ("NOT_FOUND", Get("key3b"));
  ASSERT_EQ("NOT_FOUND", Get("key4"));
  Close();
}

TEST_F(DBWALTest, BlogWalReaderStaleIncarnationDeclaredEmpty) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  std::string fname;
  WriteThreeRecordBlogWal(options, &fname);
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, fname, &contents));

  // A recycled WAL renamed to a new log number before its header was
  // rewritten: the file is declared empty, not corruption.
  CountingWalReporter reporter;
  std::unique_ptr<WalReader> reader =
      NewMemWalReader(&contents, &reporter, WalLogNumberFromFname(fname) + 100);
  Slice record;
  std::string scratch;
  ASSERT_FALSE(reader->ReadRecord(&record, &scratch,
                                  WALRecoveryMode::kAbsoluteConsistency));
  ASSERT_EQ(reporter.failing_count(), 0u);
  ASSERT_EQ(reporter.tolerated_count, 0u);
  ASSERT_EQ(reporter.unrecoverable_count, 0u);
}

TEST_F(DBWALTest, BlogWalReaderUnsupportedRecordIsUnrecoverable) {
  constexpr uint64_t kLogNumber = 7;
  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, kLogNumber, /*recycled=*/false);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    ASSERT_OK(writer->AddWriteBatchRecord(
        WriteOptions(), std::string(20 * 1024, 'r'), kNoCompression));
    ASSERT_OK(writer->Close(WriteOptions()));
  }
  // Flip the full-format record's type byte to a reserved value, keeping
  // its prefix checksum valid.
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 1u);
  RewriteFullRecordType(&contents, layout[0].record_offset, 0x14);

  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&contents, &reporter, kLogNumber);
    Slice record;
    std::string scratch;
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch, mode));
    // Reported on the unrecoverable channel, which no mode suppresses for
    // unsupported formats.
    ASSERT_EQ(reporter.unrecoverable_count, 1u);
    ASSERT_TRUE(reporter.first_status.IsNotSupported())
        << reporter.first_status.ToString();
  }
}

TEST_F(DBWALTest, BlogWalReaderToleratedTailDrop) {
  constexpr uint64_t kLogNumber = 7;
  constexpr WALRecoveryMode kMode =
      WALRecoveryMode::kTolerateCorruptedTailRecords;
  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, kLogNumber, /*recycled=*/false);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-one",
                                          kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-two",
                                          kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-three",
                                          kNoCompression));
    ASSERT_OK(writer->Close(WriteOptions()));
  }
  // Unsealed, so the damaged last record has no boundary beyond it.
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 3u);
  contents[layout[2].payload_offset + 2] ^= 0x01;

  CountingWalReporter reporter;
  std::unique_ptr<WalReader> reader =
      NewMemWalReader(&contents, &reporter, kLogNumber);
  Slice record;
  std::string scratch;
  ASSERT_TRUE(reader->ReadRecord(&record, &scratch, kMode));
  ASSERT_EQ(record, "batch-one");
  ASSERT_TRUE(reader->ReadRecord(&record, &scratch, kMode));
  ASSERT_EQ(record, "batch-two");
  ASSERT_FALSE(reader->ReadRecord(&record, &scratch, kMode));
  ASSERT_EQ(reporter.failing_count(), 0u);
  ASSERT_EQ(reporter.tolerated_count, 1u);
  ASSERT_NE(reporter.first_tolerated.ToString().find(
                "no further Blog WAL record boundary"),
            std::string::npos)
      << reporter.first_tolerated.ToString();
}

TEST_F(DBWALTest, BlogWalReaderIoErrorIsUnrecoverable) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  std::string fname;
  WriteThreeRecordBlogWal(options, &fname);
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, fname, &contents));
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 3u);

  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    // Reads fail inside the second record: the first record reads, then
    // the device error stops the read on the unrecoverable channel. The +8
    // keeps the fault past the padding-skip lookahead; the primed prefix is
    // served from the first block read, so only the second record faults.
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader = NewWalReaderOver(
        std::unique_ptr<FSSequentialFile>(new FaultySequentialFile(
            contents, static_cast<size_t>(layout[1].record_offset) + 8)),
        "faulty-blog-wal", &reporter, WalLogNumberFromFname(fname));
    Slice record;
    std::string scratch;
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch, mode));
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch, mode));
    ASSERT_EQ(reporter.unrecoverable_count, 1u);
    ASSERT_TRUE(reporter.first_status.IsIOError())
        << reporter.first_status.ToString();
  }
}

TEST_F(DBWALTest, BlogWalSkipAnyResumesPastCompactLengthOverrun) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals = false;
  options.wal_recovery_mode = WALRecoveryMode::kSkipAnyCorruptedRecords;
  std::string fname;
  WriteThreeRecordBlogWal(options, &fname);
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, fname, &contents));
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 3u);
  // Inflate the first record's unauthenticated compact length so its
  // framing consumes the second record; the trailer read then lands
  // inside the second record and fails its checksum. The reader must
  // rewind and resume at the real second boundary.
  const size_t varint_off = layout[0].record_offset + kBlogEscapeSequenceSize;
  Slice varint_input(contents.data() + varint_off,
                     contents.size() - varint_off);
  uint64_t old_length = 0;
  ASSERT_TRUE(GetPrefixVarint64(&varint_input, &old_length));
  const size_t old_varint_len =
      contents.size() - varint_off - varint_input.size();
  const uint64_t new_length =
      layout[1].payload_offset + 2 - layout[0].payload_offset;
  ASSERT_GT(new_length, old_length);
  OverwriteCompactRecordLength(varint_off, new_length, old_varint_len,
                               &contents);
  OverwriteFile(fname, contents);

  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_EQ("NOT_FOUND", Get("key1"));
  ASSERT_EQ("value2", Get("key2"));
  ASSERT_EQ("value3", Get("key3"));
}

TEST_F(DBWALTest, BlogWalRecoveryReadErrorFailsOpenExceptSkipAny) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals = false;
  std::string fname;
  WriteThreeRecordBlogWal(options, &fname);
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, fname, &contents));
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 3u);

  // Header and format detection read fine; the first record read fails,
  // so the error flows through the recovery loop in every mode.
  auto fail_fs = std::make_shared<FailWalReadsPastOffsetFS>(
      env_->GetFileSystem(), ".log", layout[0].record_offset);
  std::unique_ptr<Env> fail_env(NewCompositeEnv(fail_fs));
  // Holders for the per-mode fault env below; declared before the Defer so
  // a fatal assert with the DB open still closes against a live Env.
  std::shared_ptr<FailWalReadsPastOffsetFS> mode_fail_fs;
  std::unique_ptr<Env> mode_fail_env;
  Defer close_db_on_exit([this]() { Close(); });
  options.env = fail_env.get();
  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kAbsoluteConsistency,
                               WALRecoveryMode::kPointInTimeRecovery}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    options.wal_recovery_mode = mode;
    Status s = DB::Open(options, dbname_, &db_);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
  }

  // SkipAny truncates replay at the device error like legacy WALs: the
  // open succeeds with nothing replayed past the failure.
  options.wal_recovery_mode = WALRecoveryMode::kSkipAnyCorruptedRecords;
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_EQ("NOT_FOUND", Get("key1"));
  ASSERT_EQ("NOT_FOUND", Get("key2"));
  ASSERT_EQ("NOT_FOUND", Get("key3"));
  Close();

  // paranoid_checks=false also truncates on I/O errors like legacy WALs,
  // in every mode. Each iteration rebuilds the WAL: a successful open
  // purges it on close.
  options.paranoid_checks = false;
  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kAbsoluteConsistency,
                               WALRecoveryMode::kPointInTimeRecovery}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    options.env = env_;
    options.wal_recovery_mode = mode;
    WriteThreeRecordBlogWal(options, &fname);
    ASSERT_OK(ReadFileToString(env_, fname, &contents));
    layout = ParseBlogWalRecordLayout(contents);
    ASSERT_EQ(layout.size(), 3u);
    mode_fail_fs = std::make_shared<FailWalReadsPastOffsetFS>(
        env_->GetFileSystem(), ".log", layout[0].record_offset);
    mode_fail_env = NewCompositeEnv(mode_fail_fs);
    options.env = mode_fail_env.get();
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_EQ("NOT_FOUND", Get("key1"));
    ASSERT_EQ("NOT_FOUND", Get("key2"));
    ASSERT_EQ("NOT_FOUND", Get("key3"));
    Close();
  }
}

TEST_F(DBWALTest, LegacyWalGetFileSizeFailureFailsOpen) {
  // Complement of BlogWalRecoveryReadErrorFailsOpenExceptSkipAny: a
  // GetFileSize failure for a fully-read WAL surfaces from
  // UpdatePredecessorWALInfo, not the reader, so paranoid_checks=false must
  // not swallow it -- the open fails as at base.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.paranoid_checks = false;
  options.avoid_flush_during_shutdown = true;
  // Holders for the per-mode fault env below; declared before the Defer so
  // a fatal assert with the DB open still closes against a live Env.
  std::shared_ptr<WalGetFileSizeFailFS> mode_fail_fs;
  std::unique_ptr<Env> mode_fail_env;
  Defer close_db_on_exit([this]() { Close(); });
  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kPointInTimeRecovery}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    options.env = env_;
    options.wal_recovery_mode = mode;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", "value"));
    Close();
    mode_fail_fs =
        std::make_shared<WalGetFileSizeFailFS>(env_->GetFileSystem(), ".log");
    mode_fail_env = NewCompositeEnv(mode_fail_fs);
    options.env = mode_fail_env.get();
    Status s = DB::Open(options, dbname_, &db_);
    ASSERT_TRUE(s.IsIOError()) << s.ToString();
    ASSERT_NE(s.ToString().find("injected WAL GetFileSize failure"),
              std::string::npos)
        << s.ToString();
    ASSERT_EQ(mode_fail_fs->failed_count(), 1u);
  }
}

TEST_F(DBWALTest, WalOpenFailureSkippedWithoutParanoidChecks) {
  // With paranoid_checks=false, a WAL that cannot be opened during recovery
  // is skipped and later WALs still replay.
  Options options = CurrentOptions();
  options.paranoid_checks = false;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));
  Reopen(options);
  ASSERT_OK(Put("key2", "value2"));
  VectorWalPtr wals;
  ASSERT_OK(db_->GetSortedWalFiles(wals));
  ASSERT_EQ(wals.size(), 2u);
  const std::string first_wal = LogFileName(dbname_, wals[0]->LogNumber());
  Close();

  // Delete the first WAL after recovery has listed it, before it is opened.
  bool deleted = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RecoverLogFiles:BeforeReadWal", [&](void* /*arg*/) {
        if (!deleted) {
          deleted = true;
          ASSERT_OK(env_->DeleteFile(first_wal));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  options.avoid_flush_during_recovery = false;
  Status s = TryReopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(s);
  ASSERT_TRUE(deleted);
  ASSERT_EQ("NOT_FOUND", Get("key1"));
  ASSERT_EQ("value2", Get("key2"));
}

TEST_F(DBWALTest, BlogWalRecoveryReadErrorSkipAnyPreservesPrefix) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals = false;
  std::string fname;
  WriteThreeRecordBlogWal(options, &fname);
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, fname, &contents));
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 3u);

  // A fault at the second record's payload start replays the first record
  // before the device error truncates the rest: SkipAny preserves the
  // prefix like legacy WALs.
  auto fail_fs = std::make_shared<FailWalReadsPastOffsetFS>(
      env_->GetFileSystem(), ".log", layout[1].payload_offset);
  std::unique_ptr<Env> fail_env(NewCompositeEnv(fail_fs));
  Defer close_db_on_exit([this]() { Close(); });
  options.env = fail_env.get();
  options.wal_recovery_mode = WALRecoveryMode::kSkipAnyCorruptedRecords;
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_EQ("value1", Get("key1"));
  ASSERT_EQ("NOT_FOUND", Get("key2"));
  ASSERT_EQ("NOT_FOUND", Get("key3"));
  Close();
}

TEST_F(DBWALTest, BlogWalRecoveryUnsupportedRecordFailsOpen) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals = false;
  std::string fname;
  WriteThreeRecordBlogWal(options, &fname);
  const uint64_t log_number = WalLogNumberFromFname(fname);

  // A WAL whose first record has a reserved type: no valid batch precedes
  // the failure, so every mode must refuse to open.
  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, log_number, /*recycled=*/false);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    ASSERT_OK(writer->AddWriteBatchRecord(
        WriteOptions(), std::string(20 * 1024, 'r'), kNoCompression));
    ASSERT_OK(writer->Close(WriteOptions()));
  }
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 1u);
  RewriteFullRecordType(&contents, layout[0].record_offset, 0x14);
  OverwriteFile(fname, contents);

  // SkipAny and paranoid_checks=false suppress corruption, but an
  // unreadable format is unrecoverable and must still fail the open.
  options.wal_recovery_mode = WALRecoveryMode::kSkipAnyCorruptedRecords;
  Status s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();

  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();

  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  options.paranoid_checks = false;
  s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();

  options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  options.paranoid_checks = true;
  s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
}

TEST_F(DBWALTest, BlogWalReaderUnexpectedTypeHandling) {
  constexpr uint64_t kLogNumber = 7;
  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, kLogNumber, /*recycled=*/false);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    uint64_t blob_offset = 0;
    ASSERT_OK(writer->AddBlobRecord(WriteOptions(), "not-a-batch",
                                    kNoCompression, &blob_offset,
                                    strlen("not-a-batch")));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-after-blob",
                                          kNoCompression));
    ASSERT_OK(writer->Close(WriteOptions()));
  }

  // SkipAny reports the unexpected type but continues past it.
  {
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&contents, &reporter, kLogNumber);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch,
                                   WALRecoveryMode::kSkipAnyCorruptedRecords));
    ASSERT_EQ(record, "batch-after-blob");
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch,
                                    WALRecoveryMode::kSkipAnyCorruptedRecords));
    ASSERT_EQ(reporter.failing_count(), 1u);
  }
  // Other modes stop at it.
  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kAbsoluteConsistency,
                               WALRecoveryMode::kPointInTimeRecovery}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&contents, &reporter, kLogNumber);
    Slice record;
    std::string scratch;
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch, mode));
    ASSERT_EQ(reporter.failing_count(), 1u);
  }
}

TEST_F(DBWALTest, BlogWalReaderScanFailureReportsUnrecoverableFirst) {
  constexpr uint64_t kLogNumber = 7;
  constexpr WALRecoveryMode kMode = WALRecoveryMode::kSkipAnyCorruptedRecords;
  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, kLogNumber, /*recycled=*/false);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-one",
                                          kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-two",
                                          kNoCompression));
    ASSERT_OK(writer->Close(WriteOptions()));
  }
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 2u);
  contents[layout[0].payload_offset + 2] ^= 0x01;
  // Fail right past the first record's trailer: the corrupt record reads
  // fully, then the boundary scan itself hits the device error.
  const size_t fail_at =
      layout[0].payload_offset + layout[0].length + kBlogBlockTrailerSize + 1;

  CountingWalReporter reporter;
  std::unique_ptr<WalReader> reader =
      NewWalReaderOver(std::unique_ptr<FSSequentialFile>(
                           new FaultySequentialFile(contents, fail_at)),
                       "faulty-blog-wal", &reporter, kLogNumber);
  Slice record;
  std::string scratch;
  ASSERT_FALSE(reader->ReadRecord(&record, &scratch, kMode));
  // The scan failure is reported first (reporters keep the first status),
  // then the original corruption.
  ASSERT_EQ(reporter.unrecoverable_count, 1u);
  ASSERT_EQ(reporter.failing_count(), 2u);
  ASSERT_TRUE(reporter.first_status.IsIOError())
      << reporter.first_status.ToString();
}

TEST_F(DBWALTest, BlogWalReaderRecycledStaleTail) {
  constexpr uint64_t kLogNumber = 7;
  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, kLogNumber, /*recycled=*/true);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-one",
                                          kNoCompression));
    ASSERT_OK(writer->Close(WriteOptions()));
  }
  // Stale bytes from the previous incarnation carry no escape of ours.
  contents.append(128, '\xab');

  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kAbsoluteConsistency,
                               WALRecoveryMode::kPointInTimeRecovery,
                               WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&contents, &reporter, kLogNumber);
    Slice record;
    std::string scratch;
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch, mode));
    ASSERT_EQ(record, "batch-one");
    ASSERT_FALSE(reader->ReadRecord(&record, &scratch, mode));
    // A stale tail is a clean end in every mode: no failing, tolerated, or
    // unrecoverable report.
    ASSERT_EQ(reporter.failing_count(), 0u) << reporter.first_status.ToString();
    ASSERT_EQ(reporter.tolerated_count, 0u)
        << reporter.first_tolerated.ToString();
    ASSERT_EQ(reporter.unrecoverable_count, 0u);
  }
}

TEST_F(DBWALTest, BlogWalReaderUnsealedRecycledStaleTailReportsOldRecord) {
  constexpr uint64_t kLogNumber = 7;
  std::string contents;
  {
    std::unique_ptr<BlogFileWriter> writer =
        MakeHandBuiltWalWriter(&contents, kLogNumber, /*recycled=*/true);
    ASSERT_OK(writer->WriteHeader(WriteOptions()));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-one",
                                          kNoCompression));
    ASSERT_OK(writer->AddWriteBatchRecord(WriteOptions(), "batch-two",
                                          kNoCompression));
    ASSERT_OK(writer->Close(WriteOptions()));
  }
  // Stale bytes from the previous incarnation replace the second record and
  // the footer.
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_EQ(layout.size(), 2u);
  contents.resize(static_cast<size_t>(layout[1].record_offset));
  contents.append(128, '\xab');

  for (WALRecoveryMode mode : {WALRecoveryMode::kTolerateCorruptedTailRecords,
                               WALRecoveryMode::kAbsoluteConsistency,
                               WALRecoveryMode::kPointInTimeRecovery,
                               WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("mode=" + std::to_string(static_cast<int>(mode)));
    CountingWalReporter reporter;
    std::unique_ptr<WalReader> reader =
        NewMemWalReader(&contents, &reporter, kLogNumber);
    ASSERT_EQ(ReadAllRecords(reader.get(), mode), 1u);
    ASSERT_EQ(reporter.failing_count(), 0u) << reporter.first_status.ToString();
    ASSERT_EQ(reporter.tolerated_count, 0u)
        << reporter.first_tolerated.ToString();
    // Only kPointInTimeRecovery needs the signal to check the next WAL.
    const size_t expected_old_records =
        mode == WALRecoveryMode::kPointInTimeRecovery ? 1u : 0u;
    ASSERT_EQ(reporter.old_log_record_count, expected_old_records);
  }
}

// Fewer stale bytes than an escape sequence after the last record of a
// recycled WAL end it exactly like a longer stale tail.
TEST_F(DBWALTest, BlogWalReaderShortRecycledStaleTail) {
  constexpr uint64_t kLogNumber = 7;
  for (size_t stale_len :
       {size_t{1}, size_t{3}, size_t{4}, size_t{8}, size_t{9}}) {
    SCOPED_TRACE("stale_len=" + std::to_string(stale_len));
    std::string contents;
    const size_t cut =
        CutBeforeSecondBlogRecord(&contents, /*recycled=*/true, kLogNumber);
    const char stale = NonPaddingByteOtherThan(contents[cut]);
    contents.resize(cut);
    contents.append(stale_len, stale);
    AssertFirstRecordThenStaleTail(contents, kLogNumber);
  }
}

// A torn prefix of this incarnation's own escape sequence reads as a stale
// tail whether or not stale bytes follow it: a recycled file cannot tell the
// two apart, so the position of the previous incarnation's EOF must not
// change the outcome.
TEST_F(DBWALTest, BlogWalReaderRecycledOwnTornEscapeIsStaleTail) {
  constexpr uint64_t kLogNumber = 7;
  for (size_t stale_after : {size_t{0}, size_t{28}}) {
    for (size_t torn_len : {size_t{1}, size_t{4}, size_t{9}}) {
      SCOPED_TRACE("stale_after=" + std::to_string(stale_after) +
                   " torn_len=" + std::to_string(torn_len));
      std::string contents;
      const size_t cut =
          CutBeforeSecondBlogRecord(&contents, /*recycled=*/true, kLogNumber);
      const char stale = NonPaddingByteOtherThan(contents[cut + torn_len]);
      contents.resize(cut + torn_len);
      contents.append(stale_after, stale);
      AssertFirstRecordThenStaleTail(contents, kLogNumber);
    }
  }
}

// Without recycling, a short tail can only be a torn write of this file, so
// kAbsoluteConsistency and kPointInTimeRecovery report it as corruption.
TEST_F(DBWALTest, BlogWalReaderShortNonRecycledTailIsTornTail) {
  constexpr uint64_t kLogNumber = 7;
  for (size_t tail_len : {size_t{1}, size_t{4}, size_t{9}}) {
    std::string contents;
    const size_t cut =
        CutBeforeSecondBlogRecord(&contents, /*recycled=*/false, kLogNumber);
    const char junk = NonPaddingByteOtherThan(contents[cut]);
    contents.resize(cut);
    contents.append(tail_len, junk);
    for (WALRecoveryMode mode : {WALRecoveryMode::kAbsoluteConsistency,
                                 WALRecoveryMode::kPointInTimeRecovery}) {
      SCOPED_TRACE("tail_len=" + std::to_string(tail_len) +
                   " mode=" + std::to_string(static_cast<int>(mode)));
      CountingWalReporter reporter;
      std::unique_ptr<WalReader> reader =
          NewMemWalReader(&contents, &reporter, kLogNumber);
      ASSERT_EQ(ReadAllRecords(reader.get(), mode), 1u);
      ASSERT_EQ(reporter.failing_count(), 1u);
      ASSERT_TRUE(reporter.first_status.IsCorruption())
          << reporter.first_status.ToString();
    }
  }
}

// A header torn by a crash is reported as corruption, like a legacy WAL's
// truncated header, by readers that are not tailing the file.
TEST_F(DBWALTest, BlogWalReaderTornHeaderIsCorruption) {
  constexpr uint64_t kLogNumber = 7;
  std::string complete;
  CutBeforeSecondBlogRecord(&complete, /*recycled=*/false, kLogNumber);
  const uint32_t property_size =
      DecodeFixed32(complete.data() + kBlogFilePropertySectionSizeOffset);
  for (size_t cut :
       {size_t{kBlogFileMagicSize}, size_t{kBlogFileFixedHeaderSize - 1},
        size_t{kBlogFileFixedHeaderSize + property_size - 1}}) {
    const std::string contents = complete.substr(0, cut);
    for (WALRecoveryMode mode : {WALRecoveryMode::kAbsoluteConsistency,
                                 WALRecoveryMode::kPointInTimeRecovery}) {
      SCOPED_TRACE("cut=" + std::to_string(cut) +
                   " mode=" + std::to_string(static_cast<int>(mode)));
      CountingWalReporter reporter;
      std::unique_ptr<WalReader> reader =
          NewMemWalReader(&contents, &reporter, kLogNumber);
      ASSERT_EQ(ReadAllRecords(reader.get(), mode), 0u);
      ASSERT_EQ(reporter.failing_count(), 1u);
      ASSERT_TRUE(reporter.first_status.IsCorruption())
          << reporter.first_status.ToString();
    }
  }
}

// DB::Open, whose WAL reader cannot reopen the file, accepts a short stale
// tail in a recycled WAL under kAbsoluteConsistency.
TEST_F(DBWALTest, BlogWalShortRecycledStaleTailOpensAbsoluteConsistency) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.recycle_log_file_num = 1;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  DestroyAndReopen(options);

  // Roll WALs until the live one is a recycled file.
  std::string fname;
  std::string contents;
  bool recycled = false;
  for (int i = 0; i < 8 && !recycled; ++i) {
    ASSERT_OK(Put("roll" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
    std::unique_ptr<LogFile> wal_file;
    ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
    fname = LogFileName(dbname_, wal_file->LogNumber());
    ASSERT_OK(ReadFileToString(env_, fname, &contents));
    Slice header_input(contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    recycled = header.is_recycled();
  }
  ASSERT_TRUE(recycled) << "never got a recycled WAL";

  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key4", "value4"));
  ASSERT_OK(db_->FlushWAL(true));
  Close();

  // Replace key4's record and the footer with four stale bytes.
  ASSERT_OK(ReadFileToString(env_, fname, &contents));
  std::vector<BlogWalRecordLayout> layout = ParseBlogWalRecordLayout(contents);
  ASSERT_GE(layout.size(), 2u);
  const size_t cut = static_cast<size_t>(layout.back().record_offset);
  const char stale = NonPaddingByteOtherThan(contents[cut]);
  contents.resize(cut);
  contents.append(4, stale);
  ASSERT_OK(WriteStringToFile(env_, contents, fname, /*should_sync=*/true));

  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("value3", Get("key3"));
  ASSERT_EQ("NOT_FOUND", Get("key4"));
}

TEST_F(DBWALTest, BlogWalChecksumChangeAcrossReopen) {
  // The reader verifies against each file's stored checksum type.
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.blog_checksum = kCRC32c;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "value"));
  Close();

  options.blog_checksum = kXXH3;
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_EQ("value", Get("key"));
  Close();
}

TEST_F(DBWALTest, BlogWalDetectsMissingPredecessorWal) {
  // A deleted middle WAL exercises the predecessor-mismatch path in blog mode.
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.track_and_verify_wals = true;
  options.avoid_flush_during_shutdown = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  // Keep the unflushed memtables around rather than stalling writes.
  options.max_write_buffer_number = 10;

  // Stop background flush so that no WAL is deleted before one is picked.
  options.env->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_task;
  options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                        &sleeping_task, Env::Priority::HIGH);

  // Three WALs so recovery observes one predecessor before the hole.
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key3", "value3"));

  VectorWalPtr log_files;
  ASSERT_OK(db_->GetSortedWalFiles(log_files));
  ASSERT_EQ(log_files.size(), 3);
  const std::string middle_log_name =
      LogFileName(dbname_, log_files[1]->LogNumber());
  Close();
  ASSERT_OK(options.env->DeleteFile(middle_log_name));

  // The hole must be reported as corruption, not crash the reader.
  Status s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
}

TEST_F(DBWALTest, BlogWalRecycledMidFileCorruptionStillReported) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.recycle_log_file_num = 1;
  options.avoid_flush_during_shutdown = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_write_buffer_number = 10;
  options.compression = kNoCompression;
  options.track_and_verify_wals = false;

  std::string recycled_fname;
  uint64_t recycled_number = 0;
  CreateRecycledBlogWal(options, &recycled_fname, &recycled_number);
  Close();

  // Corrupt one payload byte of the first record; the intact second record
  // proves genuine data extends past the damage.
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, recycled_fname, &contents));
  const uint32_t prop_size =
      DecodeFixed32(contents.data() + kBlogFilePropertySectionSizeOffset);
  size_t pos = kBlogFileFixedHeaderSize + prop_size;
  while (pos < contents.size() &&
         (contents[pos] == '\0' || contents[pos] == '\xff')) {
    ++pos;
  }
  ASSERT_EQ(pos % 4, 0);
  pos += 10;
  Slice rest(contents.data() + pos, contents.size() - pos);
  uint32_t record_len = 0;
  ASSERT_TRUE(GetPrefixVarint32(&rest, &record_len));
  const size_t payload = contents.size() - rest.size();
  ASSERT_GT(record_len, 8);
  contents[payload + 2] ^= 0x01;
  OverwriteFile(recycled_fname, contents);

  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  Status s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}
// The last record of a WAL abandoned by a crashed process has nothing after
// it, so an unspecified-size record there is delimited only by the end of the
// file. It is fully written and durable, and every recovery mode must replay
// it rather than drop it.
// Config that makes every eligible record use the unspecified-size encoding
// and otherwise keeps the on-disk layout canonical.
TEST_BlogNoncanonicalConfig UnspecifiedSizeOnlyWalConfig(uint64_t seed) {
  TEST_BlogNoncanonicalConfig config;
  config.enabled = true;
  config.seed = seed;
  config.force_full_record_one_in = 1;
  config.unspecified_size_record_one_in = 1;
  config.overlong_full_varint_one_in = 0;
  config.inject_auxiliary_record_one_in = 0;
  config.extra_padding_512b_one_in = 0;
  config.extra_padding_4k_one_in = 0;
  config.prefer_padding_with_0xff_one_in = 0;
  config.shuffle_properties_one_in = 0;
  config.split_ignorable_properties_one_in = 0;
  return config;
}

// The other half of the policy: a fresh file ends exactly at its last written
// byte, so the encoding stays available and the writer really does emit it.
// The recycled flag is per-file, so this holds even when the DB has recycling
// enabled (the first WAL of a fresh DB has no recyclable predecessor). This
// covers the writer and the reader's end-of-file termination together, which
// is what keeps the crash test able to regress the reader fix.
TEST_F(DBWALTest, BlogWalNonRecycledUnspecifiedSizeRecordReplays) {
  for (int recycle_log_file_num : {0, 1}) {
    SCOPED_TRACE("recycle_log_file_num=" +
                 std::to_string(recycle_log_file_num));
    Options options = CurrentOptions();
    options.use_blog_format_for_wals = true;
    options.recycle_log_file_num = recycle_log_file_num;
    options.avoid_flush_during_shutdown = true;

    std::string wal_name;
    std::string contents;
    {
      TEST_BlogNoncanonicalConfigScope config_scope(
          UnspecifiedSizeOnlyWalConfig(/*seed=*/17));
      DestroyAndReopen(options);
      WriteOptions write_options;
      write_options.sync = true;
      ASSERT_OK(db_->Put(write_options, "key1", "value1"));
      ASSERT_OK(db_->Put(write_options, "key2", "value2"));
      std::unique_ptr<LogFile> wal_file;
      ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
      wal_name = LogFileName(dbname_, wal_file->LogNumber());
      Close();
    }
    ASSERT_OK(ReadFileToString(env_, wal_name, &contents));

    // The writer really did choose the encoding, so the rest of this test is
    // not vacuous.
    Slice header_input(contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    ASSERT_FALSE(header.is_recycled());
    std::string unspecified_size_prefix(header.escape_sequence,
                                        kBlogEscapeSequenceSize);
    PutPrefixVarint64(&unspecified_size_prefix, kBlogUnspecifiedSize);
    ASSERT_NE(contents.find(unspecified_size_prefix), std::string::npos);

    // Find where the footer starts, and cut there: a crash after the last
    // synced WriteBatch but before the WAL was sealed leaves exactly that.
    uint64_t footer_offset = 0;
    {
      std::unique_ptr<FSSequentialFile> file;
      ASSERT_OK(env_->GetFileSystem()->NewSequentialFile(
          wal_name, FileOptions(), &file, /*dbg=*/nullptr));
      BlogFileReader reader(
          std::make_unique<SequentialFileReader>(std::move(file), wal_name),
          /*reporter=*/nullptr, /*verify_checksums=*/true);
      BlogFileHeader read_header;
      ASSERT_OK(reader.ReadHeader(&read_header));
      BlogRecordType type;
      Slice record;
      std::string scratch;
      uint64_t record_offset = 0;
      do {
        ASSERT_OK(reader.ReadRecord(&type, &record, &scratch, &record_offset));
      } while (!IsBlogFooterRecordType(type));
      footer_offset = record_offset;
    }
    ASSERT_LT(footer_offset, contents.size());
    contents.resize(lossless_cast<size_t>(footer_offset));
    OverwriteFile(wal_name, contents);

    for (WALRecoveryMode wal_recovery_mode :
         {WALRecoveryMode::kTolerateCorruptedTailRecords,
          WALRecoveryMode::kAbsoluteConsistency,
          WALRecoveryMode::kPointInTimeRecovery,
          WALRecoveryMode::kSkipAnyCorruptedRecords}) {
      SCOPED_TRACE("wal_recovery_mode=" +
                   std::to_string(static_cast<int>(wal_recovery_mode)));
      options.wal_recovery_mode = wal_recovery_mode;
      ASSERT_OK(DB::Open(options, dbname_, &db_));
      ASSERT_EQ("value1", Get("key1"));
      ASSERT_EQ("value2", Get("key2"));
      Close();
    }
  }
}

TEST_F(DBWALTest, BlogWalUnspecifiedSizeLastRecordReplays) {
  for (WALRecoveryMode wal_recovery_mode :
       {WALRecoveryMode::kTolerateCorruptedTailRecords,
        WALRecoveryMode::kAbsoluteConsistency,
        WALRecoveryMode::kPointInTimeRecovery,
        WALRecoveryMode::kSkipAnyCorruptedRecords}) {
    SCOPED_TRACE("wal_recovery_mode=" +
                 std::to_string(static_cast<int>(wal_recovery_mode)));
    Options options = CurrentOptions();
    options.use_blog_format_for_wals = true;
    options.avoid_flush_during_shutdown = true;
    std::string fname;
    WriteThreeRecordBlogWal(options, &fname);

    std::string contents;
    ASSERT_OK(ReadFileToString(env_, fname, &contents));
    RewriteNthCompactRecordAsUnspecifiedSizeTail(&contents, 2);
    OverwriteFile(fname, contents);

    options.wal_recovery_mode = wal_recovery_mode;
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ("value1", Get("key1"));
    ASSERT_EQ("value2", Get("key2"));
    ASSERT_EQ("value3", Get("key3"));
    Close();
  }
}

TEST_F(DBWALTest, BlogWalNonRecycledTrailingGarbageStillReported) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));
  VectorWalPtr log_files;
  ASSERT_OK(db_->GetSortedWalFiles(log_files));
  ASSERT_EQ(log_files.size(), 1);
  const std::string fname = LogFileName(dbname_, log_files[0]->LogNumber());
  Close();

  std::string contents;
  ASSERT_OK(ReadFileToString(env_, fname, &contents));
  contents.append(128, '\x5a');
  OverwriteFile(fname, contents);

  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  Status s = DB::Open(options, dbname_, &db_);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
}

namespace {
// Controls WAL direct writes, and whether ReopenWritableFile is supported.
class WalIOTestFS : public FileSystemWrapper {
 public:
  explicit WalIOTestFS(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}
  const char* Name() const override { return "WalIOTestFS"; }
  FileOptions OptimizeForLogWrite(const FileOptions& options,
                                  const DBOptions& db_options) const override {
    FileOptions result =
        FileSystemWrapper::OptimizeForLogWrite(options, db_options);
    result.use_direct_writes = direct_io;
    return result;
  }
  IOStatus ReopenWritableFile(const std::string& fname,
                              const FileOptions& file_opts,
                              std::unique_ptr<FSWritableFile>* result,
                              IODebugContext* dbg) override {
    if (!reopen_supported) {
      return IOStatus::NotSupported("ReopenWritableFile");
    }
    return target()->ReopenWritableFile(fname, file_opts, result, dbg);
  }
  bool direct_io = false;
  bool reopen_supported = true;
};
}  // namespace

TEST_F(DBWALTest, BlogWalCloseAfterMemtableFailureSkipsSeal) {
  std::shared_ptr<WalIOTestFS> fs =
      std::make_shared<WalIOTestFS>(Env::Default()->GetFileSystem());
  std::unique_ptr<Env> wal_env = NewCompositeEnv(fs);
  Defer close_db([&] { Close(); });
  for (int config = 0; config < 8; ++config) {
    SCOPED_TRACE(config);
    fs->direct_io = (config & 2) != 0;
    fs->reopen_supported = (config & 4) == 0;
    if (fs->direct_io && (!fs->reopen_supported ||
                          !test::IsDirectIOSupported(wal_env.get(), dbname_))) {
      // Direct I/O truncation after Close requires ReopenWritableFile.
      continue;
    }
    Options options = CurrentOptions();
    options.env = wal_env.get();
    options.manual_wal_flush = (config & 1) != 0;
    options.use_blog_format_for_wals = true;
    options.avoid_flush_during_shutdown = true;
    DestroyAndReopen(options);

    ASSERT_OK(Put("good", "value"));
    ASSERT_OK(db_->FlushWAL(false));
    std::unique_ptr<LogFile> wal;
    ASSERT_OK(dbfull()->GetCurrentWalFile(&wal));
    const std::string wal_name = LogFileName(dbname_, wal->LogNumber());
    std::string good_prefix;
    ASSERT_OK(ReadFileToString(wal_env.get(), wal_name, &good_prefix));
    std::unique_ptr<FSSequentialFile> file;
    ASSERT_OK(fs->NewSequentialFile(wal_name, FileOptions(), &file, nullptr));
    BlogFileReader reader(
        std::make_unique<SequentialFileReader>(std::move(file), wal_name),
        nullptr, true);
    BlogFileHeader prefix_header;
    ASSERT_OK(reader.ReadHeader(&prefix_header));
    BlogRecordType type;
    Slice record;
    std::string scratch;
    ASSERT_OK(reader.ReadRecord(&type, &record, &scratch));
    ASSERT_EQ(type, kBlogWriteBatchRecord);
    CompactRecordLayout layout;
    FindNthCompactRecord(good_prefix, 0, &layout);
    const size_t body_end = layout.end_offset;
    const BlogPadding padding(lossless_cast<uint8_t>(good_prefix[body_end - 1]),
                              body_end);
    good_prefix.resize(body_end + padding.count);

    // A batch for an unknown column family appends to the WAL, then fails
    // memtable insert, recording a truncate back to the pre-write size.
    // (The raw-ID Put bypasses the handle validation in WriteBatch::Put.)
    WriteBatch bad;
    ASSERT_OK(WriteBatchInternal::Put(&bad, /*column_family_id=*/100, "bad",
                                      "value"));
    ASSERT_TRUE(db_->Write(WriteOptions(), &bad).IsInvalidArgument());

    std::atomic<int> skipped_seal_count{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::ClearWriter:SkippedSealForTruncate",
        [&](void*) { ++skipped_seal_count; });
    SyncPoint::GetInstance()->EnableProcessing();
    Defer cleanup_sync_point([] {
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
    });

    Close();
    ASSERT_EQ(skipped_seal_count.load(), 1);

    // The truncated WAL is left unsealed: no footer locator at its tail.
    std::string contents;
    ASSERT_OK(ReadFileToString(wal_env.get(), wal_name, &contents));
    ASSERT_EQ(contents, good_prefix);
    Slice input(contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&input));
    ASSERT_FALSE(VerifyBlogFooterLocator(
        contents.data(), contents.size(),
        /*buffer_file_offset=*/0, header.escape_sequence, header.checksum_type,
        header.incarnation_id()));
    // Recovery replays the good prefix; the failed batch is gone.
    Reopen(options);
    ASSERT_EQ("value", Get("good"));
    Close();
  }
}

TEST_F(DBWALTest, LegacyWalCloseAfterMemtableFailureTruncatesWithoutReopen) {
  std::shared_ptr<WalIOTestFS> fs =
      std::make_shared<WalIOTestFS>(Env::Default()->GetFileSystem());
  fs->reopen_supported = false;
  std::unique_ptr<Env> wal_env = NewCompositeEnv(fs);
  Defer close_db([&] { Close(); });
  Options options = CurrentOptions();
  options.env = wal_env.get();
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("good", "value"));
  std::unique_ptr<LogFile> wal;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&wal));
  const std::string wal_name = LogFileName(dbname_, wal->LogNumber());
  std::string good_prefix;
  ASSERT_OK(ReadFileToString(wal_env.get(), wal_name, &good_prefix));

  // A batch for an unknown column family appends to the WAL, then fails
  // memtable insert, recording a truncate back to the pre-write size.
  WriteBatch bad;
  ASSERT_OK(
      WriteBatchInternal::Put(&bad, /*column_family_id=*/100, "bad", "value"));
  ASSERT_TRUE(db_->Write(WriteOptions(), &bad).IsInvalidArgument());
  std::string with_bad;
  ASSERT_OK(ReadFileToString(wal_env.get(), wal_name, &with_bad));
  ASSERT_GT(with_bad.size(), good_prefix.size());

  Close();
  std::string contents;
  ASSERT_OK(ReadFileToString(wal_env.get(), wal_name, &contents));
  ASSERT_EQ(contents, good_prefix);
}

TEST_F(DBWALTest, BlogWalFooterSequenceRangeCoversMultiKeyBatches) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  // Sequences 1-3 and 4-5. The footer maxSequence must be the last sequence
  // consumed (5), not the largest batch-start sequence (4).
  WriteBatch batch1;
  ASSERT_OK(batch1.Put("k1", "v1"));
  ASSERT_OK(batch1.Put("k2", "v2"));
  ASSERT_OK(batch1.Put("k3", "v3"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch1));
  WriteBatch batch2;
  ASSERT_OK(batch2.Put("k4", "v4"));
  ASSERT_OK(batch2.Put("k5", "v5"));
  ASSERT_OK(db_->Write(WriteOptions(), &batch2));
  ASSERT_EQ(dbfull()->GetLatestSequenceNumber(), 5);

  std::unique_ptr<LogFile> old_wal;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&old_wal));
  const std::string old_wal_name = LogFileName(dbname_, old_wal->LogNumber());
  Close();

  uint64_t min_sequence = 0;
  uint64_t max_sequence = 0;
  ASSERT_OK(ReadBlogWalFooterSequenceRange(old_wal_name, &min_sequence,
                                           &max_sequence));
  ASSERT_EQ(min_sequence, 1);
  ASSERT_EQ(max_sequence, 5);
}

TEST_F(DBWALTest, BlogWalRecycledFlagOnlyForReusedFile) {
  Options options = CurrentOptions();
  options.use_blog_format_for_wals = true;
  options.recycle_log_file_num = 1;
  options.avoid_flush_during_shutdown = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_write_buffer_number = 10;
  options.compression = kNoCompression;
  options.track_and_verify_wals = false;

  // Fresh WAL: no recyclable file exists yet, so the flag must be clear even
  // though recycling is enabled.
  {
    DestroyAndReopen(options);
    ASSERT_OK(Put("key1", "value1"));
    std::unique_ptr<LogFile> wal_file;
    ASSERT_OK(dbfull()->GetCurrentWalFile(&wal_file));
    const std::string fname = LogFileName(dbname_, wal_file->LogNumber());
    Close();
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, fname, &contents));
    Slice header_input(contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    ASSERT_FALSE(header.is_recycled());
  }

  // Recycled WAL: reuses a retired larger file, so the flag must be set.
  {
    std::string recycled_fname;
    uint64_t recycled_number = 0;
    CreateRecycledBlogWal(options, &recycled_fname, &recycled_number);
    Close();
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, recycled_fname, &contents));
    Slice header_input(contents);
    BlogFileHeader header;
    ASSERT_OK(header.DecodeFrom(&header_input));
    ASSERT_TRUE(header.is_recycled());
  }
}

TEST_F(DBWALTest, AsyncWalPrecreateRecoveryToleratesActualFutureWal) {
  Options options = CurrentOptions();
  options.env = env_;
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.track_and_verify_wals = true;
  options.track_and_verify_wals_in_manifest = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;

  // Goal: cover the actual async precreation path, not a hand-forged empty WAL.
  // The background task opens the future WAL, clean close releases the
  // unpublished writer and leaves the empty future WAL behind, and recovery
  // must tolerate that file while replaying the preceding WAL with real user
  // data.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateRecoveryToleratesActualFutureWal:"
       "PrecreateDone"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  DestroyAndReopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateRecoveryToleratesActualFutureWal:"
      "PrecreateDone");

  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_GE(wal_numbers.size(), 2);
  const uint64_t future_wal_number = wal_numbers.back();
  uint64_t future_wal_size = 0;
  ASSERT_OK(env_->GetFileSize(LogFileName(dbname_, future_wal_number),
                              &future_wal_size));
  ASSERT_EQ(0, future_wal_size);

  ASSERT_OK(Put("key", "value"));
  Close();

  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("value", Get("key"));
  ASSERT_OK(Put("after-recovery", "value2"));
  ASSERT_EQ("value2", Get("after-recovery"));
}

TEST_F(DBWALTest, AsyncWalPrecreateDeletesFileOnStartFailure) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: force failure after consuming a prepared WAL but before it becomes a
  // logical WAL. The failed, unpublished file must be deleted and its writer
  // destroyed outside mutex_.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateDeletesFileOnStartFailure:PrecreateDone"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateDeletesFileOnStartFailure:PrecreateDone");
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::StartWALFile:AfterWriteFileStart", [](void* arg) {
        *static_cast<IOStatus*>(arg) =
            IOStatus::IOError("injected async WAL start failure");
      });
  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_GE(wal_numbers.size(), 2);
  const uint64_t prepared_wal_number = wal_numbers.back();

  ASSERT_OK(Put("fill", "value"));
  ASSERT_NOK(dbfull()->TEST_SwitchMemtable());
  ASSERT_TRUE(
      env_->FileExists(LogFileName(dbname_, prepared_wal_number)).IsNotFound());
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
}

TEST_F(DBWALTest, AsyncWalPrecreateFailureFallsBackToSyncCreation) {
  Options options = CurrentOptions();
  options.env = env_;
  options.statistics = CreateDBStatistics();
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.write_buffer_size = 1024;
  options.max_write_buffer_number = 4;

  // Goal: fail the background precreate file open after normal DB open has
  // created the current WAL. The foreground switch should observe no prepared
  // WAL, create one synchronously, and expose both failure and miss counters.
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BGWorkAsyncWALPrecreate:Done",
       "DBWALTest::AsyncWalPrecreateFailureFallsBackToSyncCreation:"
       "PrecreateDone"},
  });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Open:AfterDeleteFiles",
      [&](void*) { env_->non_writable_count_ = 1; });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([&] {
    env_->non_writable_count_ = 0;
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Reopen(options);
  TEST_SYNC_POINT(
      "DBWALTest::AsyncWalPrecreateFailureFallsBackToSyncCreation:"
      "PrecreateDone");
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_FAILED));

  ASSERT_OK(Put("fill", "value"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_MISS));
}

TEST_F(DBWALTest, ReadOnlyErrorIfWalFileExistsIgnoresEmptyWals) {
  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  Close();

  // Goal: preserve error_if_wal_file_exists for WALs containing data while
  // allowing empty future WALs left by async precreation. This creates empty
  // WAL files directly so the test isolates the read-only open contract.
  const std::vector<uint64_t> existing_wal_numbers = ListWalNumbers();
  const uint64_t empty_wal_number =
      existing_wal_numbers.empty() ? 1 : existing_wal_numbers.back() + 1;
  CreateEmptyWal(empty_wal_number);

  DBOptions db_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName,
                               ColumnFamilyOptions(options));
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(db_options, dbname_, column_families, &handles,
                                &read_only_db,
                                true /* error_if_wal_file_exists */));
  for (ColumnFamilyHandle* handle : handles) {
    ASSERT_OK(read_only_db->DestroyColumnFamilyHandle(handle));
  }
  read_only_db.reset();
  handles.clear();

  CreateWalWithContents(empty_wal_number + 1, Slice("not empty"));
  ASSERT_NOK(DB::OpenForReadOnly(db_options, dbname_, column_families, &handles,
                                 &read_only_db,
                                 true /* error_if_wal_file_exists */));
}

TEST_F(DBWALTest, RecoveryToleratesEmptyFutureWal) {
  Options options = CurrentOptions();
  options.env = env_;
  options.track_and_verify_wals = true;
  options.track_and_verify_wals_in_manifest = true;
  options.avoid_flush_during_recovery = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", "value"));
  Close();

  const std::vector<uint64_t> wal_numbers = ListWalNumbers();
  ASSERT_FALSE(wal_numbers.empty());
  const uint64_t future_wal_number = wal_numbers.back() + 1;
  CreateEmptyWal(future_wal_number);

  // Goal: model a crash after async WAL precreation but before consumption.
  // Recovery should process the zero-byte future WAL, mark its file number
  // used, and keep replaying the real preceding WAL without reporting a WAL
  // chain or MANIFEST-tracking error.
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("value", Get("key"));
  ASSERT_OK(Put("after-recovery", "value2"));
  ASSERT_EQ("value2", Get("after-recovery"));
}

TEST_F(DBWALTest, RecoveryFlushSwitchWALOnEmptyMemtable) {
  Options options = CurrentOptions();
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  options.env = fault_fs_env.get();
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  // Make sure the memtable switch in recovery flush happened after test checks
  // the memtable is empty.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBWALTest.RecoveryFlushSwitchWALOnEmptyMemtable:"
        "AfterCheckMemtableEmpty",
        "RecoverFromRetryableBGIOError:BeforeStart"}});
  SyncPoint::GetInstance()->EnableProcessing();
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataWrite, 7 /* seed*/, 1 /* one_in */,
      true /* retryable */, false /* has_data_loss*/);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataWrite);

  WriteOptions wo;
  wo.sync = true;
  Status s = Put("k", "old_v", wo);
  ASSERT_TRUE(s.IsIOError());
  // To verify the key is not in memtable nor SST
  ASSERT_TRUE(static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())
                  ->cfd()
                  ->mem()
                  ->IsEmpty());
  ASSERT_EQ("NOT_FOUND", Get("k"));
  TEST_SYNC_POINT(
      "DBWALTest.RecoveryFlushSwitchWALOnEmptyMemtable:"
      "AfterCheckMemtableEmpty");
  SyncPoint::GetInstance()->DisableProcessing();

  fault_fs->DisableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataWrite);

  // Keep trying write until recovery of the previous IO error finishes
  while (!s.ok()) {
    options.env->SleepForMicroseconds(1000);
    s = Put("k", "new_v");
  }

  // If recovery flush didn't switch WAL, we will end up having two duplicate
  // WAL entries with same seqno and same key that violate assertion during WAL
  // recovery and fail DB reopen
  options.avoid_flush_during_recovery = false;
  Reopen(options);

  ASSERT_EQ("new_v", Get("k"));
  Destroy(options);
}

TEST_F(DBWALTest, WALWriteErrorNoRecovery) {
  Options options = CurrentOptions();
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  options.env = fault_fs_env.get();
  options.manual_wal_flush = true;
  DestroyAndReopen(options);
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kWrite, 7 /* seed*/, 1 /* one_in */,
      true /* retryable */, false /* has_data_loss*/);
  fault_fs->EnableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  ASSERT_OK(Put("k", "v"));
  Status s;
  s = db_->FlushWAL(false);
  ASSERT_TRUE(s.IsIOError());
  s = dbfull()->TEST_GetBGError();
  ASSERT_EQ(s.severity(), Status::Severity::kFatalError);
  ASSERT_FALSE(dbfull()->TEST_IsRecoveryInProgress());
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);
  Destroy(options);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
