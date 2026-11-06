//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cassert>
#include <cstdint>
#include <memory>
#include <string>

#include "file/sequence_file_reader.h"
#include "port/likely.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// A peekable byte window over a sequential file. Bytes are inspected in
// place with Peek() and committed with Advance(); unaccepted bytes stay
// retained across calls, so a parse that runs out of input can be retried
// from the same position once the file grows.
//
// End of input is not sticky: a short read only describes the file at that
// moment, and the next Peek() that needs more bytes reads again. Callers
// decide when to stop polling.
class BlogInputWindow {
 public:
  BlogInputWindow(std::unique_ptr<SequentialFileReader>&& file,
                  std::string&& initial_data);

  // Ensure the next n unaccepted bytes are retained, reading from the file as
  // needed, and view them in *result. Never moves the parse cursor.
  // Returns OK once n bytes are retained, Incomplete when the file ends first
  // (*result views the retained partial bytes), or the read error. Buffered
  // inputs read ahead; direct I/O reads exactly what is missing.
  Status Peek(size_t n, Slice* result) {
    if (LIKELY(fill_ - parse_pos_ >= n)) {
      *result = Slice(window_.data() + parse_pos_, n);
      return Status::OK();
    }
    return PeekSlow(n, result);
  }

  // All retained, unaccepted bytes. Any window mutation may invalidate
  // previously returned views.
  Slice Retained() const {
    return Slice(window_.data() + parse_pos_, fill_ - parse_pos_);
  }

  // File offset of Retained()[0].
  uint64_t ParseOffset() const { return window_base_ + parse_pos_; }

  // File offset of the next byte read from the file: the end of the retained
  // bytes, plus any input read or skipped past them by ReadAhead() or
  // SkipAhead().
  uint64_t ReadOffset() const {
    return window_base_ + fill_ + external_read_bytes_;
  }

  // Commit the first n retained bytes as accepted, moving the parse cursor.
  void Advance(size_t n) {
    assert(n <= Retained().size());
    parse_pos_ += n;
  }

  // Make room for n retained bytes without reading, so a large record is
  // read in place. Use only for trusted lengths. Returns false when n
  // exceeds the maximum window size.
  bool Reserve(size_t n);

  // Read forward past the buffered end into caller-owned storage. Does not
  // move the parse cursor or retain these bytes. Extending the window after
  // this operation requires AdoptReadAhead() or Refresh().
  Status ReadAhead(size_t n, Slice* result, char* scratch);

  // Advance the existing file handle past externally consumed input, without
  // changing the retained bytes or parse cursor.
  Status SkipAhead(uint64_t n);

  // Adopt an external read's unread suffix and move the parse cursor to its
  // absolute file offset.
  // The suffix must end at the current file position and must not alias the
  // window. Keeps the file handle and allocation, and performs no I/O.
  void AdoptReadAhead(uint64_t offset, const Slice& bytes);

  // Discard unaccepted bytes and reopen the file at the parse cursor, so the
  // next Peek() observes the file's current contents. Also clears a deferred
  // read error. Requires CanReopen().
  Status Refresh();
  // Reopen at a saved parse offset, discarding retained and scanned bytes.
  Status Refresh(uint64_t offset);

  bool CanReopen() const { return file_->CanReopen(); }
  SequentialFileReader* file() { return file_.get(); }

 private:
  friend class BlogWriterTest;

  Status PeekSlow(size_t n, Slice* result);
  // Append up to n bytes in progressively larger chunks, stopping at the
  // first short read, so corrupt lengths bound allocation to bytes actually
  // present.
  Status ReadIntoBuffer(size_t n);
  // Drop the accepted prefix, moving unaccepted bytes to the front.
  void Compact();

  static constexpr size_t kReadBufferSize = 32 * 1024;
  static constexpr size_t kMinWindowSize = 1024 * 1024;

  std::unique_ptr<SequentialFileReader> file_;
  // window_[i] is the file byte at window_base_ + i for i < fill_;
  // window_.size() is allocated room. parse_pos_ is the first unaccepted
  // byte.
  std::string window_;
  size_t fill_ = 0;
  uint64_t window_base_ = 0;
  size_t parse_pos_ = 0;
  uint64_t external_read_bytes_ = 0;
  bool external_read_active_ = false;
  // A failed read leaves the file position undefined, so no further reads
  // are issued until Refresh(). Deferred while earlier bytes satisfy Peek().
  Status read_error_;
};

}  // namespace ROCKSDB_NAMESPACE
