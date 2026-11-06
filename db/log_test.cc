//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "utilities/memory_allocators.h"

namespace ROCKSDB_NAMESPACE::log {

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

// Param type is tuple<int, bool, CompressionType>
// get<0>(tuple): non-zero if recycling log, zero if regular log
// get<1>(tuple): true if allow retry after read EOF, false otherwise
// get<2>(tuple): type of compression used
class LogTest
    : public ::testing::TestWithParam<std::tuple<int, bool, CompressionType>> {
 private:
  class StringSource : public FSSequentialFile {
   public:
    Slice& contents_;
    bool force_error_;
    size_t force_error_position_;
    bool force_eof_;
    size_t force_eof_position_;
    bool returned_partial_;
    bool fail_after_read_partial_;
    explicit StringSource(Slice& contents, bool fail_after_read_partial)
        : contents_(contents),
          force_error_(false),
          force_error_position_(0),
          force_eof_(false),
          force_eof_position_(0),
          returned_partial_(false),
          fail_after_read_partial_(fail_after_read_partial) {}

    IOStatus Read(size_t n, const IOOptions& /*opts*/, Slice* result,
                  char* scratch, IODebugContext* /*dbg*/) override {
      if (fail_after_read_partial_) {
        EXPECT_TRUE(!returned_partial_) << "must not Read() after eof/error";
      }

      if (force_error_) {
        if (force_error_position_ >= n) {
          force_error_position_ -= n;
        } else {
          *result = Slice(contents_.data(), force_error_position_);
          contents_.remove_prefix(force_error_position_);
          force_error_ = false;
          returned_partial_ = true;
          return IOStatus::Corruption("read error");
        }
      }

      if (contents_.size() < n) {
        n = contents_.size();
        returned_partial_ = true;
      }

      if (force_eof_) {
        if (force_eof_position_ >= n) {
          force_eof_position_ -= n;
        } else {
          force_eof_ = false;
          n = force_eof_position_;
          returned_partial_ = true;
        }
      }

      // By using scratch we ensure that caller has control over the
      // lifetime of result.data()
      memcpy(scratch, contents_.data(), n);
      *result = Slice(scratch, n);

      contents_.remove_prefix(n);
      return IOStatus::OK();
    }

    IOStatus Skip(uint64_t n) override {
      if (n > contents_.size()) {
        contents_.clear();
        return IOStatus::NotFound("in-memory file skipepd past end");
      }

      contents_.remove_prefix(n);

      return IOStatus::OK();
    }
  };

  class ReportCollector : public Reader::Reporter {
   public:
    size_t dropped_bytes_;
    std::string message_;

    ReportCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status& status,
                    uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }
  };

  std::string& dest_contents() { return sink_->contents_; }

  const std::string& dest_contents() const { return sink_->contents_; }

  void reset_source_contents() { source_->contents_ = dest_contents(); }

  Slice reader_contents_;
  test::StringSink* sink_;
  StringSource* source_;
  ReportCollector report_;

 protected:
  std::unique_ptr<Writer> writer_;
  std::unique_ptr<Reader> reader_;
  bool allow_retry_read_;
  CompressionType compression_type_;

 public:
  LogTest()
      : reader_contents_(),
        sink_(new test::StringSink(&reader_contents_)),
        source_(new StringSource(reader_contents_, !std::get<1>(GetParam()))),
        allow_retry_read_(std::get<1>(GetParam())),
        compression_type_(std::get<2>(GetParam())) {
    std::unique_ptr<FSWritableFile> sink_holder(sink_);
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(sink_holder), "" /* don't care */, FileOptions()));
    Writer* writer =
        new Writer(std::move(file_writer), 123, std::get<0>(GetParam()), false,
                   compression_type_);
    writer_.reset(writer);
    std::unique_ptr<FSSequentialFile> source_holder(source_);
    std::unique_ptr<SequentialFileReader> file_reader(
        new SequentialFileReader(std::move(source_holder), "" /* file name */));
    if (allow_retry_read_) {
      reader_.reset(new FragmentBufferedReader(nullptr, std::move(file_reader),
                                               &report_, true /* checksum */,
                                               123 /* log_number */));
    } else {
      reader_.reset(new Reader(nullptr, std::move(file_reader), &report_,
                               true /* checksum */, 123 /* log_number */));
    }
  }

  Slice* get_reader_contents() { return &reader_contents_; }

  void Write(const std::string& msg,
             const UnorderedMap<uint32_t, size_t>* cf_to_ts_sz = nullptr) {
    if (cf_to_ts_sz != nullptr && !cf_to_ts_sz->empty()) {
      ASSERT_OK(writer_->MaybeAddUserDefinedTimestampSizeRecord(WriteOptions(),
                                                                *cf_to_ts_sz));
    }
    ASSERT_OK(writer_->AddRecord(WriteOptions(), Slice(msg)));
  }

  size_t WrittenBytes() const { return dest_contents().size(); }

  std::string Read(const WALRecoveryMode wal_recovery_mode =
                       WALRecoveryMode::kTolerateCorruptedTailRecords,
                   UnorderedMap<uint32_t, size_t>* cf_to_ts_sz = nullptr) {
    std::string scratch;
    Slice record;
    bool ret = false;
    uint64_t record_checksum;
    ret = reader_->ReadRecord(&record, &scratch, wal_recovery_mode,
                              &record_checksum);
    if (cf_to_ts_sz != nullptr) {
      *cf_to_ts_sz = reader_->GetRecordedTimestampSize();
    }
    if (ret) {
      if (!allow_retry_read_) {
        // allow_retry_read_ means using FragmentBufferedReader which does not
        // support record checksum yet.
        uint64_t actual_record_checksum =
            XXH3_64bits(record.data(), record.size());
        EXPECT_EQ(actual_record_checksum, record_checksum);
      }
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, char delta) {
    dest_contents()[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_contents()[offset] = new_byte;
  }

  void ShrinkSize(int bytes) { sink_->Drop(bytes); }

  void FixChecksum(int header_offset, int len, bool recyclable) {
    // Compute crc of type/len/data
    int header_size = recyclable ? kRecyclableHeaderSize : kHeaderSize;
    uint32_t crc = crc32c::Value(&dest_contents()[header_offset + 6],
                                 header_size - 6 + len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_contents()[header_offset], crc);
  }

  void ForceError(size_t position = 0) {
    source_->force_error_ = true;
    source_->force_error_position_ = position;
  }

  size_t DroppedBytes() const { return report_.dropped_bytes_; }

  std::string ReportMessage() const { return report_.message_; }

  void ForceEOF(size_t position = 0) {
    source_->force_eof_ = true;
    source_->force_eof_position_ = position;
  }

  void UnmarkEOF() {
    source_->returned_partial_ = false;
    reader_->UnmarkEOF();
  }

  bool IsEOF() { return reader_->IsEOF(); }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }

  void CheckRecordAndTimestampSize(
      std::string record, UnorderedMap<uint32_t, size_t>& expected_ts_sz) {
    UnorderedMap<uint32_t, size_t> recorded_ts_sz;
    ASSERT_EQ(record,
              Read(WALRecoveryMode::
                       kTolerateCorruptedTailRecords /* wal_recovery_mode */,
                   &recorded_ts_sz));
    EXPECT_EQ(expected_ts_sz, recorded_ts_sz);
  }
};

TEST_P(LogTest, Empty) { ASSERT_EQ("EOF", Read()); }

TEST_P(LogTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_P(LogTest, ReadWriteWithTimestampSize) {
  UnorderedMap<uint32_t, size_t> ts_sz_one = {
      {1, sizeof(uint64_t)},
  };
  Write("foo", &ts_sz_one);
  Write("bar");
  UnorderedMap<uint32_t, size_t> ts_sz_two = {{2, sizeof(char)}};
  Write("", &ts_sz_two);
  Write("xxxx");

  CheckRecordAndTimestampSize("foo", ts_sz_one);
  CheckRecordAndTimestampSize("bar", ts_sz_one);
  UnorderedMap<uint32_t, size_t> expected_ts_sz_two;
  // User-defined timestamp size records are accumulated and applied to
  // subsequent records.
  expected_ts_sz_two.insert(ts_sz_one.begin(), ts_sz_one.end());
  expected_ts_sz_two.insert(ts_sz_two.begin(), ts_sz_two.end());
  CheckRecordAndTimestampSize("", expected_ts_sz_two);
  CheckRecordAndTimestampSize("xxxx", expected_ts_sz_two);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_P(LogTest, ReadWriteWithTimestampSizeZeroTimestampIgnored) {
  UnorderedMap<uint32_t, size_t> ts_sz_one = {{1, sizeof(uint64_t)}};
  Write("foo", &ts_sz_one);
  UnorderedMap<uint32_t, size_t> ts_sz_two(ts_sz_one.begin(), ts_sz_one.end());
  ts_sz_two.insert(std::make_pair(2, 0));
  Write("bar", &ts_sz_two);

  CheckRecordAndTimestampSize("foo", ts_sz_one);
  CheckRecordAndTimestampSize("bar", ts_sz_one);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_P(LogTest, ManyBlocks) {
  for (int i = 0; i < 100000; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, Fragmentation) {
  Write("small");
  Write(BigString("medium", 50000));
  Write(BigString("large", 100000));
  ASSERT_EQ("small", Read());
  ASSERT_EQ(BigString("medium", 50000), Read());
  ASSERT_EQ(BigString("large", 100000), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, MarginalTrailer) {
  // Make a trailer that is exactly the same length as an empty record.
  int header_size =
      std::get<0>(GetParam()) ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size), WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record.
  int header_size =
      std::get<0>(GetParam()) ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size), WrittenBytes());
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_P(LogTest, ShortTrailer) {
  int header_size =
      std::get<0>(GetParam()) ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size + 4;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size + 4), WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, AlignedEof) {
  int header_size =
      std::get<0>(GetParam()) ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size + 4;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size + 4), WrittenBytes());
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in log_reader.cc follow:

TEST_P(LogTest, ReadError) {
  Write("foo");
  ForceError();
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ((unsigned int)kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST_P(LogTest, BadRecordType) {
  Write("foo");
  // Type is stored in header[6]
  IncrementByte(6, 100);
  FixChecksum(0, 3, false);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("unknown record type"));
}

TEST_P(LogTest, IgnorableRecordType) {
  Write("foo");
  // Type is stored in header[6]
  SetByte(6, static_cast<char>(kRecordTypeSafeIgnoreMask + 100));
  FixChecksum(0, 3, false);
  ASSERT_EQ("EOF", Read());
  // The new type has value 129 and masked to be ignorable if unknown
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_P(LogTest, TruncatedTrailingRecordIsIgnored) {
  Write("foo");
  ShrinkSize(4);  // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  // Truncated last record is ignored, not treated as an error
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_P(LogTest, TruncatedTrailingRecordIsNotIgnored) {
  if (allow_retry_read_) {
    // If read retry is allowed, then truncated trailing record should not
    // raise an error.
    return;
  }
  Write("foo");
  ShrinkSize(4);  // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  // Truncated last record is not ignored, treated as an error
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError("Corruption: truncated header"));
}

TEST_P(LogTest, BadLength) {
  if (allow_retry_read_) {
    // If read retry is allowed, then we should not raise an error when the
    // record length specified in header is longer than data currently
    // available. It's possible that the body of the record is not written yet.
    return;
  }
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  int header_size = recyclable_log ? kRecyclableHeaderSize : kHeaderSize;
  const int kPayloadSize = kBlockSize - header_size;
  Write(BigString("bar", kPayloadSize));
  Write("foo");
  // Least significant size byte is stored in header[4].
  IncrementByte(4, 1);
  if (!recyclable_log) {
    ASSERT_EQ("foo", Read());
    ASSERT_EQ(kBlockSize, DroppedBytes());
    ASSERT_EQ("OK", MatchError("bad record length"));
  } else {
    ASSERT_EQ("EOF", Read());
  }
}

TEST_P(LogTest, BadLengthAtEndIsIgnored) {
  if (allow_retry_read_) {
    // If read retry is allowed, then we should not raise an error when the
    // record length specified in header is longer than data currently
    // available. It's possible that the body of the record is not written yet.
    return;
  }
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_P(LogTest, BadLengthAtEndIsNotIgnored) {
  if (allow_retry_read_) {
    // If read retry is allowed, then we should not raise an error when the
    // record length specified in header is longer than data currently
    // available. It's possible that the body of the record is not written yet.
    return;
  }
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError("Corruption: truncated record body"));
}

TEST_P(LogTest, ChecksumMismatch) {
  Write("foooooo");
  IncrementByte(0, 14);
  ASSERT_EQ("EOF", Read());
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  if (!recyclable_log) {
    ASSERT_EQ(14U, DroppedBytes());
    ASSERT_EQ("OK", MatchError("checksum mismatch"));
  } else {
    ASSERT_EQ(0U, DroppedBytes());
    ASSERT_EQ("", ReportMessage());
  }
}

TEST_P(LogTest, UnexpectedMiddleType) {
  Write("foo");
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  SetByte(6, static_cast<char>(recyclable_log ? kRecyclableMiddleType
                                              : kMiddleType));
  FixChecksum(0, 3, !!recyclable_log);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_P(LogTest, UnexpectedLastType) {
  Write("foo");
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  SetByte(6,
          static_cast<char>(recyclable_log ? kRecyclableLastType : kLastType));
  FixChecksum(0, 3, !!recyclable_log);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_P(LogTest, UnexpectedFullType) {
  Write("foo");
  Write("bar");
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  SetByte(
      6, static_cast<char>(recyclable_log ? kRecyclableFirstType : kFirstType));
  FixChecksum(0, 3, !!recyclable_log);
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_P(LogTest, UnexpectedFirstType) {
  Write("foo");
  Write(BigString("bar", 100000));
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  SetByte(
      6, static_cast<char>(recyclable_log ? kRecyclableFirstType : kFirstType));
  FixChecksum(0, 3, !!recyclable_log);
  ASSERT_EQ(BigString("bar", 100000), Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

// Regression test derived from a physical WAL observed during recovery: a
// fragmented record is abandoned after a zero-length kZeroType padding record,
// and the next fragmented record must start with a fresh logical checksum.
TEST_P(LogTest, BadRecordInFragmentedRecordResetsRecordChecksum) {
  if (allow_retry_read_) {
    return;
  }

  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  int header_size = recyclable_log ? kRecyclableHeaderSize : kHeaderSize;
  int payload_size = kBlockSize - header_size;
  Write(BigString("foo", 3 * payload_size));
  std::string second = BigString("bar", 3 * payload_size);
  Write(second);

  // Make the middle physical record look like the zero-length padding record
  // that ReadPhysicalRecord() maps to kBadRecord.
  int middle_header = kBlockSize;
  SetByte(middle_header + 4, 0);
  SetByte(middle_header + 5, 0);
  SetByte(middle_header + 6, static_cast<char>(kZeroType));

  ASSERT_EQ(second, Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("OK", MatchError("error in middle of record"));
}

TEST_P(LogTest, MissingLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0U, DroppedBytes());
}

TEST_P(LogTest, MissingLastIsNotIgnored) {
  if (allow_retry_read_) {
    // If read retry is allowed, then truncated trailing record should not
    // raise an error.
    return;
  }
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError("Corruption: error reading trailing data"));
}

TEST_P(LogTest, PartialLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0U, DroppedBytes());
}

TEST_P(LogTest, PartialLastIsNotIgnored) {
  if (allow_retry_read_) {
    // If read retry is allowed, then truncated trailing record should not
    // raise an error.
    return;
  }
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError("Corruption: truncated record body"));
}

TEST_P(LogTest, ErrorJoinsRecords) {
  // Consider two fragmented records:
  //    first(R1) last(R1) first(R2) last(R2)
  // where the middle two fragments disappear.  We do not want
  // first(R1),last(R2) to get joined and returned as a valid record.

  // Write records that span two blocks
  Write(BigString("foo", kBlockSize));
  Write(BigString("bar", kBlockSize));
  Write("correct");

  // Wipe the middle block
  for (unsigned int offset = kBlockSize; offset < 2 * kBlockSize; offset++) {
    SetByte(offset, 'x');
  }

  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  if (!recyclable_log) {
    ASSERT_EQ("correct", Read());
    ASSERT_EQ("EOF", Read());
    size_t dropped = DroppedBytes();
    ASSERT_LE(dropped, 2 * kBlockSize + 100);
    ASSERT_GE(dropped, 2 * kBlockSize);
  } else {
    ASSERT_EQ("EOF", Read());
  }
}

TEST_P(LogTest, ClearEofSingleBlock) {
  Write("foo");
  Write("bar");
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  int header_size = recyclable_log ? kRecyclableHeaderSize : kHeaderSize;
  ForceEOF(3 + header_size + 2);
  ASSERT_EQ("foo", Read());
  UnmarkEOF();
  ASSERT_EQ("bar", Read());
  ASSERT_TRUE(IsEOF());
  ASSERT_EQ("EOF", Read());
  Write("xxx");
  UnmarkEOF();
  ASSERT_EQ("xxx", Read());
  ASSERT_TRUE(IsEOF());
}

TEST_P(LogTest, ClearEofMultiBlock) {
  size_t num_full_blocks = 5;
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  int header_size = recyclable_log ? kRecyclableHeaderSize : kHeaderSize;
  size_t n = (kBlockSize - header_size) * num_full_blocks + 25;
  Write(BigString("foo", n));
  Write(BigString("bar", n));
  ForceEOF(n + num_full_blocks * header_size + header_size + 3);
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_TRUE(IsEOF());
  UnmarkEOF();
  ASSERT_EQ(BigString("bar", n), Read());
  ASSERT_TRUE(IsEOF());
  Write(BigString("xxx", n));
  UnmarkEOF();
  ASSERT_EQ(BigString("xxx", n), Read());
  ASSERT_TRUE(IsEOF());
}

TEST_P(LogTest, ClearEofError) {
  // If an error occurs during Read() in UnmarkEOF(), the records contained
  // in the buffer should be returned on subsequent calls of ReadRecord()
  // until no more full records are left, whereafter ReadRecord() should return
  // false to indicate that it cannot read any further.

  Write("foo");
  Write("bar");
  UnmarkEOF();
  ASSERT_EQ("foo", Read());
  ASSERT_TRUE(IsEOF());
  Write("xxx");
  ForceError(0);
  UnmarkEOF();
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, ClearEofError2) {
  Write("foo");
  Write("bar");
  UnmarkEOF();
  ASSERT_EQ("foo", Read());
  Write("xxx");
  ForceError(3);
  UnmarkEOF();
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST_P(LogTest, Recycle) {
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  if (!recyclable_log) {
    return;  // test is only valid for recycled logs
  }
  Write("foo");
  Write("bar");
  Write("baz");
  Write("bif");
  Write("blitz");
  while (get_reader_contents()->size() < log::kBlockSize * 2) {
    Write("xxxxxxxxxxxxxxxx");
  }
  std::unique_ptr<FSWritableFile> sink(
      new test::OverwritingStringSink(get_reader_contents()));
  std::unique_ptr<WritableFileWriter> dest_holder(new WritableFileWriter(
      std::move(sink), "" /* don't care */, FileOptions()));
  Writer recycle_writer(std::move(dest_holder), 123, true);
  ASSERT_OK(recycle_writer.AddRecord(WriteOptions(), Slice("foooo")));
  ASSERT_OK(recycle_writer.AddRecord(WriteOptions(), Slice("bar")));
  ASSERT_GE(get_reader_contents()->size(), log::kBlockSize * 2);
  ASSERT_EQ("foooo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, RecycleWithTimestampSize) {
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  if (!recyclable_log) {
    return;  // test is only valid for recycled logs
  }
  UnorderedMap<uint32_t, size_t> ts_sz_one = {
      {1, sizeof(uint32_t)},
  };
  Write("foo", &ts_sz_one);
  Write("bar");
  Write("baz");
  Write("bif");
  Write("blitz");
  while (get_reader_contents()->size() < log::kBlockSize * 2) {
    Write("xxxxxxxxxxxxxxxx");
  }
  std::unique_ptr<FSWritableFile> sink(
      new test::OverwritingStringSink(get_reader_contents()));
  std::unique_ptr<WritableFileWriter> dest_holder(new WritableFileWriter(
      std::move(sink), "" /* don't care */, FileOptions()));
  Writer recycle_writer(std::move(dest_holder), 123, true);
  UnorderedMap<uint32_t, size_t> ts_sz_two = {
      {2, sizeof(uint64_t)},
  };
  ASSERT_OK(recycle_writer.MaybeAddUserDefinedTimestampSizeRecord(
      WriteOptions(), ts_sz_two));
  ASSERT_OK(recycle_writer.AddRecord(WriteOptions(), Slice("foooo")));
  ASSERT_OK(recycle_writer.AddRecord(WriteOptions(), Slice("bar")));
  ASSERT_GE(get_reader_contents()->size(), log::kBlockSize * 2);
  CheckRecordAndTimestampSize("foooo", ts_sz_two);
  CheckRecordAndTimestampSize("bar", ts_sz_two);
  ASSERT_EQ("EOF", Read());
}

// Validates that `MaybeAddUserDefinedTimestampSizeRecord`` adds padding to the
// tail of a block and switches to a new block, if there's not enough space for
// the record.
TEST_P(LogTest, TimestampSizeRecordPadding) {
  bool recyclable_log = (std::get<0>(GetParam()) != 0);
  const size_t header_size =
      recyclable_log ? kRecyclableHeaderSize : kHeaderSize;
  const size_t data_len = kBlockSize - 2 * header_size;

  const auto first_str = BigString("foo", data_len);
  Write(first_str);

  UnorderedMap<uint32_t, size_t> ts_sz = {
      {2, sizeof(uint64_t)},
  };
  ASSERT_OK(
      writer_->MaybeAddUserDefinedTimestampSizeRecord(WriteOptions(), ts_sz));
  ASSERT_LT(writer_->TEST_block_offset(), kBlockSize);

  const auto second_str = BigString("bar", 1000);
  Write(second_str);

  ASSERT_EQ(first_str, Read());
  CheckRecordAndTimestampSize(second_str, ts_sz);
}

// Do NOT enable compression for this instantiation.
INSTANTIATE_TEST_CASE_P(
    Log, LogTest,
    ::testing::Combine(::testing::Values(0, 1), ::testing::Bool(),
                       ::testing::Values(CompressionType::kNoCompression)));

class RetriableLogTest : public ::testing::TestWithParam<int> {
 private:
  class ReportCollector : public Reader::Reporter {
   public:
    size_t dropped_bytes_;
    std::string message_;

    ReportCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status& status,
                    uint64_t /*log_number*/ = kMaxSequenceNumber) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }
  };

  Slice contents_;
  test::StringSink* sink_;
  std::unique_ptr<Writer> log_writer_;
  Env* env_;
  const std::string test_dir_;
  const std::string log_file_;
  std::unique_ptr<WritableFileWriter> writer_;
  std::unique_ptr<SequentialFileReader> reader_;
  ReportCollector report_;
  std::unique_ptr<FragmentBufferedReader> log_reader_;

 public:
  RetriableLogTest()
      : contents_(),
        sink_(new test::StringSink(&contents_)),
        log_writer_(nullptr),
        env_(Env::Default()),
        test_dir_(test::PerThreadDBPath("retriable_log_test")),
        log_file_(test_dir_ + "/log"),
        writer_(nullptr),
        reader_(nullptr),
        log_reader_(nullptr) {
    std::unique_ptr<FSWritableFile> sink_holder(sink_);
    std::unique_ptr<WritableFileWriter> wfw(new WritableFileWriter(
        std::move(sink_holder), "" /* file name */, FileOptions()));
    log_writer_.reset(new Writer(std::move(wfw), 123, GetParam()));
  }

  Status SetupTestEnv() {
    Status s;
    FileOptions fopts;
    auto fs = env_->GetFileSystem();
    s = fs->CreateDirIfMissing(test_dir_, IOOptions(), nullptr);
    std::unique_ptr<FSWritableFile> writable_file;
    if (s.ok()) {
      s = fs->NewWritableFile(log_file_, fopts, &writable_file, nullptr);
    }
    if (s.ok()) {
      writer_.reset(
          new WritableFileWriter(std::move(writable_file), log_file_, fopts));
      EXPECT_NE(writer_, nullptr);
    }
    std::unique_ptr<FSSequentialFile> seq_file;
    if (s.ok()) {
      s = fs->NewSequentialFile(log_file_, fopts, &seq_file, nullptr);
    }
    if (s.ok()) {
      reader_.reset(new SequentialFileReader(std::move(seq_file), log_file_));
      EXPECT_NE(reader_, nullptr);
      log_reader_.reset(new FragmentBufferedReader(
          nullptr, std::move(reader_), &report_, true /* checksum */,
          123 /* log_number */));
      EXPECT_NE(log_reader_, nullptr);
    }
    return s;
  }

  std::string contents() { return sink_->contents_; }

  void Encode(const std::string& msg) {
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), Slice(msg)));
  }

  void Write(const Slice& data) {
    ASSERT_OK(writer_->Append(data, IOOptions()));
    ASSERT_OK(writer_->Fsync(IOOptions()));
  }

  bool TryRead(std::string* result) {
    assert(result != nullptr);
    result->clear();
    std::string scratch;
    Slice record;
    bool r = log_reader_->ReadRecord(&record, &scratch);
    if (r) {
      result->assign(record.data(), record.size());
      return true;
    } else {
      return false;
    }
  }
};

TEST_P(RetriableLogTest, TailLog_PartialHeader) {
  ASSERT_OK(SetupTestEnv());
  std::vector<int> remaining_bytes_in_last_record;
  size_t header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  bool eof = false;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"RetriableLogTest::TailLog:AfterPart1",
        "RetriableLogTest::TailLog:BeforeReadRecord"},
       {"FragmentBufferedLogReader::TryReadMore:FirstEOF",
        "RetriableLogTest::TailLog:BeforePart2"}});
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "FragmentBufferedLogReader::TryReadMore:FirstEOF",
      [&](void* /*arg*/) { eof = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  size_t delta = header_size - 1;
  port::Thread log_writer_thread([&]() {
    size_t old_sz = contents().size();
    Encode("foo");
    size_t new_sz = contents().size();
    std::string part1 = contents().substr(old_sz, delta);
    std::string part2 =
        contents().substr(old_sz + delta, new_sz - old_sz - delta);
    Write(Slice(part1));
    TEST_SYNC_POINT("RetriableLogTest::TailLog:AfterPart1");
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforePart2");
    Write(Slice(part2));
  });

  std::string record;
  port::Thread log_reader_thread([&]() {
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforeReadRecord");
    while (!TryRead(&record)) {
    }
  });
  log_reader_thread.join();
  log_writer_thread.join();
  ASSERT_EQ("foo", record);
  ASSERT_TRUE(eof);
}

TEST_P(RetriableLogTest, TailLog_FullHeader) {
  ASSERT_OK(SetupTestEnv());
  std::vector<int> remaining_bytes_in_last_record;
  size_t header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  bool eof = false;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"RetriableLogTest::TailLog:AfterPart1",
        "RetriableLogTest::TailLog:BeforeReadRecord"},
       {"FragmentBufferedLogReader::TryReadMore:FirstEOF",
        "RetriableLogTest::TailLog:BeforePart2"}});
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "FragmentBufferedLogReader::TryReadMore:FirstEOF",
      [&](void* /*arg*/) { eof = true; });
  SyncPoint::GetInstance()->EnableProcessing();

  size_t delta = header_size + 1;
  port::Thread log_writer_thread([&]() {
    size_t old_sz = contents().size();
    Encode("foo");
    size_t new_sz = contents().size();
    std::string part1 = contents().substr(old_sz, delta);
    std::string part2 =
        contents().substr(old_sz + delta, new_sz - old_sz - delta);
    Write(Slice(part1));
    TEST_SYNC_POINT("RetriableLogTest::TailLog:AfterPart1");
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforePart2");
    Write(Slice(part2));
    ASSERT_TRUE(eof);
  });

  std::string record;
  port::Thread log_reader_thread([&]() {
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforeReadRecord");
    while (!TryRead(&record)) {
    }
  });
  log_reader_thread.join();
  log_writer_thread.join();
  ASSERT_EQ("foo", record);
}

TEST_P(RetriableLogTest, NonBlockingReadFullRecord) {
  // Clear all sync point callbacks even if this test does not use sync point.
  // It is necessary, otherwise the execute of this test may hit a sync point
  // with which a callback is registered. The registered callback may access
  // some dead variable, causing segfault.
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(SetupTestEnv());
  size_t header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  size_t delta = header_size - 1;
  size_t old_sz = contents().size();
  Encode("foo-bar");
  size_t new_sz = contents().size();
  std::string part1 = contents().substr(old_sz, delta);
  std::string part2 =
      contents().substr(old_sz + delta, new_sz - old_sz - delta);
  Write(Slice(part1));
  std::string record;
  ASSERT_FALSE(TryRead(&record));
  ASSERT_TRUE(record.empty());
  Write(Slice(part2));
  ASSERT_TRUE(TryRead(&record));
  ASSERT_EQ("foo-bar", record);
}

INSTANTIATE_TEST_CASE_P(bool, RetriableLogTest, ::testing::Values(0, 2));

class CompressionLogTest : public LogTest {
 public:
  Status SetupTestEnv() {
    return writer_->AddCompressionTypeRecord(WriteOptions());
  }
};

TEST_P(CompressionLogTest, Empty) {
  CompressionType compression_type = std::get<2>(GetParam());
  if (!StreamingCompressionTypeSupported(compression_type)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  ASSERT_OK(SetupTestEnv());
  const bool compression_enabled =
      std::get<2>(GetParam()) == kNoCompression ? false : true;
  // If WAL compression is enabled, a record is added for the compression type
  const int compression_record_size = compression_enabled ? kHeaderSize + 4 : 0;
  ASSERT_EQ(compression_record_size, WrittenBytes());
  ASSERT_EQ("EOF", Read());
}

TEST_P(CompressionLogTest, ReadWrite) {
  CompressionType compression_type = std::get<2>(GetParam());
  if (!StreamingCompressionTypeSupported(compression_type)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  ASSERT_OK(SetupTestEnv());
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_P(CompressionLogTest, ReadWriteWithTimestampSize) {
  CompressionType compression_type = std::get<2>(GetParam());
  if (!StreamingCompressionTypeSupported(compression_type)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  ASSERT_OK(SetupTestEnv());
  UnorderedMap<uint32_t, size_t> ts_sz_one = {
      {1, sizeof(uint64_t)},
  };
  Write("foo", &ts_sz_one);
  Write("bar");
  UnorderedMap<uint32_t, size_t> ts_sz_two = {{2, sizeof(char)}};
  Write("", &ts_sz_two);
  Write("xxxx");

  CheckRecordAndTimestampSize("foo", ts_sz_one);
  CheckRecordAndTimestampSize("bar", ts_sz_one);
  UnorderedMap<uint32_t, size_t> expected_ts_sz_two;
  // User-defined timestamp size records are accumulated and applied to
  // subsequent records.
  expected_ts_sz_two.insert(ts_sz_one.begin(), ts_sz_one.end());
  expected_ts_sz_two.insert(ts_sz_two.begin(), ts_sz_two.end());
  CheckRecordAndTimestampSize("", expected_ts_sz_two);
  CheckRecordAndTimestampSize("xxxx", expected_ts_sz_two);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_P(CompressionLogTest, ManyBlocks) {
  CompressionType compression_type = std::get<2>(GetParam());
  if (!StreamingCompressionTypeSupported(compression_type)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  ASSERT_OK(SetupTestEnv());
  for (int i = 0; i < 100000; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_P(CompressionLogTest, Fragmentation) {
  CompressionType compression_type = std::get<2>(GetParam());
  if (!StreamingCompressionTypeSupported(compression_type)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  ASSERT_OK(SetupTestEnv());
  Random rnd(301);
  const std::vector<std::string> wal_entries = {
      "small",
      rnd.RandomBinaryString(3 * kBlockSize / 2),  // Spans into block 2
      rnd.RandomBinaryString(3 * kBlockSize),      // Spans into block 5
  };
  for (const std::string& wal_entry : wal_entries) {
    Write(wal_entry);
  }

  for (const std::string& wal_entry : wal_entries) {
    ASSERT_EQ(wal_entry, Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_P(CompressionLogTest, AlignedFragmentation) {
  CompressionType compression_type = std::get<2>(GetParam());
  if (!StreamingCompressionTypeSupported(compression_type)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  ASSERT_OK(SetupTestEnv());
  Random rnd(301);
  int num_filler_records = 0;
  // Keep writing small records until the next record will be aligned at the
  // beginning of the block.
  while ((WrittenBytes() & (kBlockSize - 1)) >= kHeaderSize) {
    char entry = 'a';
    ASSERT_OK(writer_->AddRecord(WriteOptions(), Slice(&entry, 1)));
    num_filler_records++;
  }
  const std::vector<std::string> wal_entries = {
      rnd.RandomBinaryString(3 * kBlockSize),
  };
  for (const std::string& wal_entry : wal_entries) {
    Write(wal_entry);
  }

  for (int i = 0; i < num_filler_records; ++i) {
    ASSERT_EQ("a", Read());
  }
  for (const std::string& wal_entry : wal_entries) {
    ASSERT_EQ(wal_entry, Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_P(CompressionLogTest, ChecksumMismatch) {
  const CompressionType kCompressionType = std::get<2>(GetParam());
  const bool kCompressionEnabled = kCompressionType != kNoCompression;
  const bool kRecyclableLog = (std::get<0>(GetParam()) != 0);
  if (!StreamingCompressionTypeSupported(kCompressionType)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  ASSERT_OK(SetupTestEnv());

  Write("foooooo");
  int header_len;
  if (kRecyclableLog) {
    header_len = kRecyclableHeaderSize;
  } else {
    header_len = kHeaderSize;
  }
  int compression_record_len;
  if (kCompressionEnabled) {
    compression_record_len = header_len + 4;
  } else {
    compression_record_len = 0;
  }
  IncrementByte(compression_record_len + header_len /* offset */,
                14 /* delta */);

  ASSERT_EQ("EOF", Read());
  if (!kRecyclableLog) {
    ASSERT_GT(DroppedBytes(), 0U);
    ASSERT_EQ("OK", MatchError("checksum mismatch"));
  } else {
    ASSERT_EQ(0U, DroppedBytes());
    ASSERT_EQ("", ReportMessage());
  }
}

INSTANTIATE_TEST_CASE_P(
    Compression, CompressionLogTest,
    ::testing::Combine(::testing::Values(0, 1), ::testing::Bool(),
                       ::testing::Values(CompressionType::kNoCompression,
                                         CompressionType::kZSTD)));

class StreamingCompressionTest
    : public ::testing::TestWithParam<std::tuple<int, CompressionType>> {};

TEST_P(StreamingCompressionTest, Basic) {
  size_t input_size = std::get<0>(GetParam());
  CompressionType compression_type = std::get<1>(GetParam());
  if (!StreamingCompressionTypeSupported(compression_type)) {
    ROCKSDB_GTEST_SKIP("Test requires support for compression type");
    return;
  }
  CompressionOptions opts;
  constexpr uint32_t compression_format_version = 2;
  auto compress = StreamingCompress::Create(
      compression_type, opts, compression_format_version, kBlockSize);
  auto uncompress = StreamingUncompress::Create(
      compression_type, compression_format_version, kBlockSize);
  std::unique_ptr<MemoryAllocator> allocator(new DefaultMemoryAllocator());
  std::string input_buffer = BigString("abc", input_size);
  std::vector<std::string> compressed_buffers;
  size_t remaining;
  // Call compress till the entire input is consumed
  do {
    char* output_buffer = (char*)allocator->Allocate(kBlockSize);
    size_t output_pos;
    remaining = compress->Compress(input_buffer.c_str(), input_size,
                                   output_buffer, &output_pos);
    if (output_pos > 0) {
      std::string compressed_buffer;
      compressed_buffer.assign(output_buffer, output_pos);
      compressed_buffers.emplace_back(std::move(compressed_buffer));
    }
    allocator->Deallocate((void*)output_buffer);
  } while (remaining > 0);
  std::string uncompressed_buffer;
  int ret_val = 0;
  size_t output_pos;
  char* uncompressed_output_buffer = (char*)allocator->Allocate(kBlockSize);
  // Uncompress the fragments and concatenate them.
  for (int i = 0; i < (int)compressed_buffers.size(); i++) {
    // Call uncompress till either the entire input is consumed or the output
    // buffer size is equal to the allocated output buffer size.
    const char* input = compressed_buffers[i].c_str();
    do {
      ret_val = uncompress->Uncompress(input, compressed_buffers[i].size(),
                                       uncompressed_output_buffer, &output_pos);
      input = nullptr;
      if (output_pos > 0) {
        std::string uncompressed_fragment;
        uncompressed_fragment.assign(uncompressed_output_buffer, output_pos);
        uncompressed_buffer += uncompressed_fragment;
      }
    } while (ret_val > 0 || output_pos == kBlockSize);
  }
  allocator->Deallocate((void*)uncompressed_output_buffer);
  // The final return value from uncompress() should be 0.
  ASSERT_EQ(ret_val, 0);
  ASSERT_EQ(input_buffer, uncompressed_buffer);
}

INSTANTIATE_TEST_CASE_P(
    StreamingCompression, StreamingCompressionTest,
    ::testing::Combine(::testing::Values(10, 100, 1000, kBlockSize,
                                         kBlockSize * 2),
                       ::testing::Values(CompressionType::kZSTD)));

}  // namespace ROCKSDB_NAMESPACE::log

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
