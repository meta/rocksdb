//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <algorithm>
#include <cassert>
#include <functional>
#include <utility>

#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/file_system.h"
#include "rocksdb/status.h"
namespace ROCKSDB_NAMESPACE {

// This file contains utilities to handle the alignment of pages and buffers.

// Truncate to a multiple of page_size, which is also a page boundary. This
// helps to figuring out the right alignment.
// Example:
//   TruncateToPageBoundary(4096, 5000)  => 4096
//   TruncateToPageBoundary((4096, 10000) => 8192
inline size_t TruncateToPageBoundary(size_t page_size, size_t s) {
  s -= (s & (page_size - 1));
  assert((s % page_size) == 0);
  return s;
}

// Round up x to a multiple of y.
// Example:
//   Roundup(13, 5)   => 15
//   Roundup(201, 16) => 208
inline size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

// Round down x to a multiple of y.
// Example:
//   Rounddown(13, 5)   => 10
//   Rounddown(201, 16) => 192
inline size_t Rounddown(size_t x, size_t y) { return (x / y) * y; }

// AlignedBuffer manages a buffer by taking alignment into consideration, and
// aligns the buffer start and end positions. It is mainly used for direct I/O,
// though it can be used other purposes as well.
// It also supports expanding the managed buffer, and copying whole or part of
// the data from old buffer into the new expanded buffer. Such a copy especially
// helps in cases avoiding an IO to re-fetch the data from disk.
//
// Example:
//   AlignedBuffer buf;
//   buf.Alignment(alignment);
//   buf.AllocateNewBuffer(user_requested_buf_size);
//   ...
//   buf.AllocateNewBuffer(2*user_requested_buf_size, /*copy_data*/ true,
//                         copy_offset, copy_len);
class AlignedBuffer {
 public:
  struct ExternalAllocation {
    char* data = nullptr;
    size_t size = 0;
    FSAllocationPtr owner;
  };

  using Allocator = std::function<Status(size_t size, size_t alignment,
                                         ExternalAllocation* out)>;

 private:
  size_t alignment_;
  FSAllocationPtr buf_;
  size_t capacity_;
  size_t cursize_;
  char* bufstart_;

 public:
  AlignedBuffer()
      : alignment_(), capacity_(0), cursize_(0), bufstart_(nullptr) {}

  AlignedBuffer(AlignedBuffer&& o) noexcept { *this = std::move(o); }

  AlignedBuffer& operator=(AlignedBuffer&& o) noexcept {
    alignment_ = std::move(o.alignment_);
    buf_ = std::move(o.buf_);
    capacity_ = std::move(o.capacity_);
    cursize_ = std::move(o.cursize_);
    bufstart_ = std::move(o.bufstart_);
    return *this;
  }

  AlignedBuffer(const AlignedBuffer&) = delete;

  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  ~AlignedBuffer() = default;

  static bool isAligned(const void* ptr, size_t alignment) {
    return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
  }

  static bool isAligned(size_t n, size_t alignment) {
    return n % alignment == 0;
  }

  size_t Alignment() const { return alignment_; }

  size_t Capacity() const { return capacity_; }

  size_t CurrentSize() const { return cursize_; }

  const char* BufferStart() const { return bufstart_; }

  char* BufferStart() { return bufstart_; }

  void Clear() { cursize_ = 0; }

  FSAllocationPtr Release() {
    cursize_ = 0;
    capacity_ = 0;
    bufstart_ = nullptr;
    return std::move(buf_);
  }

  void Alignment(size_t alignment) {
    assert(alignment > 0);
    assert((alignment & (alignment - 1)) == 0);
    alignment_ = alignment;
  }

  // Points the buffer to the result without allocating extra
  // memory or performing any data copies. Takes ownership of the
  // FSAllocationPtr. This method is called when we want to reuse the buffer
  // provided by the file system
  void SetBuffer(Slice& result, FSAllocationPtr new_buf) {
    alignment_ = 1;
    capacity_ = result.size();
    cursize_ = result.size();
    buf_ = std::move(new_buf);
    assert(buf_.get() != nullptr);
    // Note: bufstart_ must point to result.data() and not new_buf, which can
    // point to any arbitrary object
    bufstart_ = const_cast<char*>(result.data());
  }

  // Allocates a new buffer and sets the start position to the first aligned
  // byte.
  //
  // requested_capacity: requested new buffer capacity. This capacity will be
  //     rounded up based on alignment.
  // copy_data: Copy data from old buffer to new buffer. If copy_offset and
  //     copy_len are not passed in and the new requested capacity is bigger
  //     than the existing buffer's capacity, the data in the existing buffer is
  //     fully copied over to the new buffer.
  // copy_offset: Copy data from this offset in old buffer.
  // copy_len: Number of bytes to copy.
  //
  // The function does nothing if the new requested_capacity is smaller than
  // the current buffer capacity and copy_data is true i.e. the old buffer is
  // retained as is.
  void AllocateNewBuffer(size_t requested_capacity, bool copy_data = false,
                         uint64_t copy_offset = 0, size_t copy_len = 0) {
    assert(alignment_ > 0);
    assert((alignment_ & (alignment_ - 1)) == 0);

    copy_len = copy_len > 0 ? copy_len : cursize_;
    if (copy_data && requested_capacity < copy_len) {
      // If we are downsizing to a capacity that is smaller than the current
      // data in the buffer -- Ignore the request.
      return;
    }

    size_t new_capacity = Roundup(requested_capacity, alignment_);
    char* new_buf = new char[new_capacity + alignment_];
    char* new_bufstart = reinterpret_cast<char*>(
        (reinterpret_cast<uintptr_t>(new_buf) + (alignment_ - 1)) &
        ~static_cast<uintptr_t>(alignment_ - 1));

    if (copy_data) {
      assert(bufstart_ + copy_offset + copy_len <= bufstart_ + cursize_);
      memcpy(new_bufstart, bufstart_ + copy_offset, copy_len);
      cursize_ = copy_len;
    } else {
      cursize_ = 0;
    }

    bufstart_ = new_bufstart;
    capacity_ = new_capacity;
    // buf_ is a FSAllocationPtr which takes in a deleter
    // we can just wrap the regular default delete that would have been called
    buf_ = std::unique_ptr<void, std::function<void(void*)>>(
        static_cast<void*>(new_buf),
        [](void* p) { delete[] static_cast<char*>(p); });
  }

  // Allocates a fresh buffer, using the external allocator when provided and
  // RocksDB heap memory otherwise. This overload does not preserve old
  // contents, and callers must check its returned Status. Heap-only callers
  // should use the void overload above so CHECK_STATUS builds do not create an
  // ignored Status.
  Status AllocateNewBuffer(size_t requested_capacity,
                           const Allocator* allocator) {
    if (allocator == nullptr) {
      AllocateNewBuffer(requested_capacity);
      return Status::OK();
    }

    assert(alignment_ > 0);
    assert((alignment_ & (alignment_ - 1)) == 0);

    const size_t new_capacity = Roundup(requested_capacity, alignment_);
    ExternalAllocation allocation;
    Status s = (*allocator)(new_capacity, alignment_, &allocation);
    if (!s.ok()) {
      return s;
    }
    if (allocation.data == nullptr) {
      return Status::InvalidArgument(
          "AlignedBuffer allocator returned null data");
    }
    if (allocation.size < new_capacity) {
      return Status::InvalidArgument(
          "AlignedBuffer allocator returned short buffer");
    }
    if (!isAligned(allocation.data, alignment_)) {
      return Status::InvalidArgument(
          "AlignedBuffer allocator returned misaligned buffer");
    }
    if (!isAligned(allocation.size, alignment_)) {
      return Status::InvalidArgument(
          "AlignedBuffer allocator returned misaligned size");
    }
    if (allocation.owner.get() == nullptr) {
      return Status::InvalidArgument(
          "AlignedBuffer allocator returned null owner");
    }

    bufstart_ = allocation.data;
    capacity_ = allocation.size;
    cursize_ = 0;
    buf_ = std::move(allocation.owner);
    return Status::OK();
  }

  // Append to the buffer.
  //
  // src         : source to copy the data from.
  // append_size : number of bytes to copy from src.
  // Returns the number of bytes appended.
  //
  // If append_size is more than the remaining buffer size only the
  // remaining-size worth of bytes are copied.
  size_t Append(const char* src, size_t append_size) {
    size_t buffer_remaining = capacity_ - cursize_;
    size_t to_copy = std::min(append_size, buffer_remaining);

    if (to_copy > 0) {
      memcpy(bufstart_ + cursize_, src, to_copy);
      cursize_ += to_copy;
    }
    return to_copy;
  }

  // Read from the buffer.
  //
  // dest      : destination buffer to copy the data to.
  // offset    : the buffer offset to start reading from.
  // read_size : the number of bytes to copy from the buffer to dest.
  // Returns the number of bytes read/copied to dest.
  size_t Read(char* dest, size_t offset, size_t read_size) const {
    assert(offset < cursize_);

    size_t to_read = 0;
    if (offset < cursize_) {
      to_read = std::min(cursize_ - offset, read_size);
    }
    if (to_read > 0) {
      memcpy(dest, bufstart_ + offset, to_read);
    }
    return to_read;
  }

  // Pad to the end of alignment with "padding"
  void PadToAlignmentWith(int padding) {
    size_t total_size = Roundup(cursize_, alignment_);
    size_t pad_size = total_size - cursize_;

    if (pad_size > 0) {
      assert((pad_size + cursize_) <= capacity_);
      memset(bufstart_ + cursize_, padding, pad_size);
      cursize_ += pad_size;
    }
  }

  void PadWith(size_t pad_size, int padding) {
    assert((pad_size + cursize_) <= capacity_);
    memset(bufstart_ + cursize_, padding, pad_size);
    cursize_ += pad_size;
  }

  // After a partial flush move the tail to the beginning of the buffer.
  void RefitTail(size_t tail_offset, size_t tail_size) {
    if (tail_size > 0) {
      memmove(bufstart_, bufstart_ + tail_offset, tail_size);
    }
    cursize_ = tail_size;
  }

  // Returns a place to start appending.
  // WARNING: Note that it is possible to write past the end of the buffer if
  // the buffer is modified without using the write APIs or encapsulation
  // offered by AlignedBuffer. It is up to the user to guard against such
  // errors.
  char* Destination() { return bufstart_ + cursize_; }

  void Size(size_t cursize) { cursize_ = cursize; }
};

// Related to std::string but more easily avoids zeroing out a buffer that's
// going to be overwritten anyway.
class GrowableBuffer {
 public:
  GrowableBuffer() : capacity_(0) {}
  ~GrowableBuffer() { free(data_); }
  // No copies
  GrowableBuffer(const GrowableBuffer&) = delete;
  GrowableBuffer& operator=(const GrowableBuffer&) = delete;
  // Movable
  GrowableBuffer(GrowableBuffer&& other) noexcept
      : data_(other.data_), size_(other.size_), capacity_(other.capacity_) {
    other.data_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
  }
  GrowableBuffer& operator=(GrowableBuffer&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    free(data_);
    data_ = other.data_;
    size_ = other.size_;
    capacity_ = other.capacity_;
    other.data_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
    return *this;
  }

  char* data() { return data_; }
  const char* data() const { return data_; }

  size_t size() const { return size_; }
  size_t& MutableSize() { return size_; }

  bool empty() const { return size_ == 0; }

  void Reset() { size_ = 0; }
  void ResetForSize(size_t new_size) {
    if (new_size > capacity_) {
      free(data_);
      size_t new_capacity = std::max(capacity_ * 2, new_size);
      new_capacity = std::max(size_t{64}, new_capacity);
      data_ = static_cast<char*>(malloc(new_capacity));
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      capacity_ = malloc_usable_size(data_);
#else
      capacity_ = new_capacity;
#endif
      // Warm the memory in CPU cache
      for (size_t i = 0; i < new_capacity; i += CACHE_LINE_SIZE) {
        data_[i] = 1;
      }
    }
    size_ = new_size;
  }

  Slice AsSlice() const { return Slice(data_, size_); }
  operator Slice() const { return AsSlice(); }

 private:
  char* data_ = nullptr;
  size_t size_ = 0;
  size_t capacity_;
};

}  // namespace ROCKSDB_NAMESPACE
