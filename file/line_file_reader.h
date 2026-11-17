//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <array>

#include "file/sequence_file_reader.h"

namespace ROCKSDB_NAMESPACE {

// A wrapper on top of Env::SequentialFile for reading text lines from a file.
// Lines are delimited by '\n'. The last line may or may not include a
// trailing newline. Uses SequentialFileReader internally.
class LineFileReader {
 private:
  std::array<char, 8192> buf_;
  SequentialFileReader sfr_;
  IOStatus io_status_;
  const char* buf_begin_ = buf_.data();
  const char* buf_end_ = buf_.data();
  size_t line_number_ = 0;
  bool at_eof_ = false;

 public:
  // See SequentialFileReader constructors
  template <typename... Args>
  explicit LineFileReader(Args&&... args)
      : sfr_(std::forward<Args&&>(args)...) {}

  static IOStatus Create(const std::shared_ptr<FileSystem>& fs,
                         const std::string& fname, const FileOptions& file_opts,
                         std::unique_ptr<LineFileReader>* reader,
                         IODebugContext* dbg, RateLimiter* rate_limiter);

  LineFileReader(const LineFileReader&) = delete;
  LineFileReader& operator=(const LineFileReader&) = delete;
  LineFileReader(LineFileReader&&) = delete;
  LineFileReader& operator=(LineFileReader&&) = delete;
  ~LineFileReader() = default;

  // Reads another line from the file, returning true on success and saving
  // the line to `out`, without delimiter, or returning false on failure. You
  // must check GetStatus() to determine whether the failure was just
  // end-of-file (OK status) or an I/O error (another status).
  // The internal rate limiter will be charged at the specified priority.
  bool ReadLine(std::string* out, Env::IOPriority rate_limiter_priority);

  // Returns the number of the line most recently returned from ReadLine.
  // Return value is unspecified if ReadLine has returned false due to
  // I/O error. After ReadLine returns false due to end-of-file, return
  // value is the last returned line number, or equivalently the total
  // number of lines returned.
  size_t GetLineNumber() const { return line_number_; }

  // Returns any error encountered during read. The error is considered
  // permanent and no retry or recovery is attempted with the same
  // LineFileReader.
  const IOStatus& GetStatus() const { return io_status_; }

  // See SequentialFileReader::SetIOType()
  void SetIOType(IOType io_type) { sfr_.SetIOType(io_type); }
};

}  // namespace ROCKSDB_NAMESPACE
