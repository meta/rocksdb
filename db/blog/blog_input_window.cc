//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blog/blog_input_window.h"

#include <algorithm>
#include <cassert>
#include <cstring>

#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {

Status WithFileOffset(const Status& s, uint64_t file_offset,
                      const std::string& context) {
  return Status::CopyAppendMessage(
      s, " at ", "file offset " + std::to_string(file_offset) + "; " + context);
}

}  // namespace

BlogInputWindow::BlogInputWindow(std::unique_ptr<SequentialFileReader>&& file,
                                 std::string&& initial_data)
    : file_(std::move(file)),
      window_(std::move(initial_data)),
      fill_(window_.size()) {
  // The deferred error is consumed only if a later Peek() needs the file.
  read_error_.PermitUncheckedError();
}

Status BlogInputWindow::PeekSlow(size_t n, Slice* result) {
  if (external_read_active_) {
    *result = Retained();
    return read_error_.ok()
               ? Status::Incomplete("input advanced past the retained window")
               : read_error_;
  }
  const size_t missing = n - Retained().size();
  // Buffered inputs over-read to the next kReadBufferSize-aligned file
  // offset: small requests then share one refill, and reads stay aligned
  // (stdio-backed files split unaligned reads into an extra syscall and
  // copy). Direct I/O keeps exact-size requests.
  size_t fetch = missing;
  if (!file_->use_direct_io()) {
    const uint64_t fill_offset = window_base_ + fill_;
    const uint64_t end = (fill_offset + missing + kReadBufferSize - 1) /
                         kReadBufferSize * kReadBufferSize;
    fetch = lossless_cast<size_t>(end - fill_offset);
  }
  if (fill_ + fetch > window_.size() &&
      Retained().size() <= window_.size() / 4) {
    // Reclaim the accepted prefix only while the move is cheap; a large
    // in-progress record grows the window instead of being moved.
    Compact();
  }
  Status s = read_error_.ok() ? ReadIntoBuffer(fetch) : read_error_;
  if (Retained().size() >= n) {
    // A read-ahead failure past the requested bytes is reported by the next
    // Peek() that needs the file.
    s.PermitUncheckedError();
    *result = Slice(Retained().data(), n);
    return Status::OK();
  }
  *result = Retained();
  return s;
}

Status BlogInputWindow::ReadIntoBuffer(size_t n) {
  constexpr size_t kInitialChunkSize = 8 << 20;
  size_t chunk = kInitialChunkSize;
  size_t remaining = n;
  while (remaining > 0) {
    const size_t want = std::min(remaining, chunk);
    const size_t have = fill_;
    if (have + want > window_.size()) {
      // Grow geometrically with a floor so refills reuse already-initialized
      // bytes instead of zero-filling each chunk.
      window_.resize(
          std::max({have + want, window_.size() * 2, kMinWindowSize}));
    }
    Slice read_result;
    IOStatus ios =
        file_->Read(want, &read_result, window_.data() + have, Env::IO_TOTAL);
    if (!ios.ok()) {
      read_error_ =
          WithFileOffset(static_cast<Status>(ios), window_base_ + have,
                         "while reading " + std::to_string(want) + " bytes");
      read_error_.PermitUncheckedError();
      return read_error_;
    }
    if (read_result.data() != window_.data() + have && !read_result.empty()) {
      memcpy(window_.data() + have, read_result.data(), read_result.size());
    }
    fill_ = have + read_result.size();
    if (read_result.size() < want) {
      return Status::Incomplete("Blog reader: unexpected EOF at file offset " +
                                std::to_string(window_base_ + fill_));
    }
    remaining -= want;
    if (chunk < remaining && chunk <= SIZE_MAX / 2) {
      chunk *= 2;
    }
  }
  return Status::OK();
}

bool BlogInputWindow::Reserve(size_t n) {
  if (n > window_.max_size()) {
    return false;
  }
  // size() >= fill_ >= parse_pos_, so the subtraction cannot underflow.
  if (n > window_.size() - parse_pos_) {
    Compact();
    if (n > window_.size()) {
      // Leave room for the next record's read-ahead so it does not force a
      // compaction that moves this record.
      window_.resize(std::max(n + kReadBufferSize, window_.size() * 2));
    }
  }
  return true;
}

Status BlogInputWindow::ReadAhead(size_t n, Slice* result, char* scratch) {
  *result = Slice();
  if (!read_error_.ok()) {
    return read_error_;
  }
  const uint64_t offset = window_base_ + fill_ + external_read_bytes_;
  external_read_active_ = true;
  IOStatus ios = file_->Read(n, result, scratch, Env::IO_TOTAL);
  if (!ios.ok()) {
    *result = Slice();
    read_error_ = WithFileOffset(ios, offset, "while reading ahead");
    read_error_.PermitUncheckedError();
    return read_error_;
  }
  if (result->data() != scratch && !result->empty()) {
    memcpy(scratch, result->data(), result->size());
    *result = Slice(scratch, result->size());
  }
  external_read_bytes_ += result->size();
  return result->size() == n
             ? Status::OK()
             : Status::Incomplete("unexpected EOF at file offset " +
                                  std::to_string(offset + result->size()));
}

Status BlogInputWindow::SkipAhead(uint64_t n) {
  if (!read_error_.ok()) {
    return read_error_;
  }
  external_read_active_ = true;
  IOStatus ios = file_->Skip(n);
  if (!ios.ok()) {
    read_error_ =
        WithFileOffset(ios, window_base_ + fill_ + external_read_bytes_,
                       "while skipping input");
    read_error_.PermitUncheckedError();
    return read_error_;
  }
  external_read_bytes_ += n;
  return Status::OK();
}

void BlogInputWindow::AdoptReadAhead(uint64_t offset, const Slice& bytes) {
  assert(offset + bytes.size() == window_base_ + fill_ + external_read_bytes_);
  if (window_.size() < bytes.size()) {
    window_.resize(bytes.size());
  }
  if (!bytes.empty()) {
    memcpy(window_.data(), bytes.data(), bytes.size());
  }
  window_base_ = offset;
  parse_pos_ = 0;
  fill_ = bytes.size();
  external_read_bytes_ = 0;
  external_read_active_ = false;
}

void BlogInputWindow::Compact() {
  if (parse_pos_ == 0) {
    return;
  }
  memmove(window_.data(), window_.data() + parse_pos_, fill_ - parse_pos_);
  fill_ -= parse_pos_;
  window_base_ += parse_pos_;
  parse_pos_ = 0;
}

Status BlogInputWindow::Refresh() { return Refresh(ParseOffset()); }

Status BlogInputWindow::Refresh(uint64_t offset) {
  assert(CanReopen());
  std::unique_ptr<SequentialFileReader> fresh_file;
  Status s = file_->ReopenAt(offset, &fresh_file);
  if (!s.ok()) {
    return WithFileOffset(s, offset, "while reopening input");
  }
  file_ = std::move(fresh_file);
  window_base_ = offset;
  fill_ = parse_pos_ = 0;
  external_read_bytes_ = 0;
  external_read_active_ = false;
  read_error_ = Status::OK();
  read_error_.PermitUncheckedError();
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
