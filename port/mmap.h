//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef OS_WIN
#include "port/win/port_win.h"
// ^^^ For proper/safe inclusion of windows.h. Must come first.
#include <memoryapi.h>
#else
#include <sys/mman.h>
#endif  // OS_WIN

#include <cstdint>
#include <utility>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

// An RAII wrapper for mmaped memory
class MemMapping {
 public:
  static constexpr bool kHugePageSupported =
#if defined(MAP_HUGETLB) || defined(FILE_MAP_LARGE_PAGES)
      true;
#else
      false;
#endif

  // Allocate memory requesting to be backed by huge pages
  static MemMapping AllocateHuge(size_t length);

  // Allocate memory that is only lazily mapped to resident memory and
  // guaranteed to be zero-initialized. Note that some platforms like
  // Linux allow memory over-commit, where only the used portion of memory
  // matters, while other platforms require enough swap space (page file) to
  // back the full mapping.
  static MemMapping AllocateLazyZeroed(size_t length);

  // Like AllocateLazyZeroed, but on Linux the mapping does not count against
  // the commit limit (MAP_NORESERVE; Linux ignores it under
  // vm.overcommit_memory=2), so a large reservation of which little is
  // touched does not inflate committed memory. As with any overcommitted
  // memory, touching a page when memory is exhausted fails at that point.
  // Elsewhere the same as AllocateLazyZeroed.
  static MemMapping AllocateLazyZeroedNoReserve(size_t length);

  // No copies
  MemMapping(const MemMapping&) = delete;
  MemMapping& operator=(const MemMapping&) = delete;
  // Move
  MemMapping(MemMapping&&) noexcept;
  MemMapping& operator=(MemMapping&&) noexcept;

  // Releases the mapping
  ~MemMapping();

  inline void* Get() const { return addr_; }
  // The requested length of the mapping, which may be smaller than the
  // actual usable length
  inline size_t Length() const { return length_; }

  // Return the mapping as a Slice (zero-copy view of the mapped memory)
  inline Slice AsSlice() const {
    return Slice(static_cast<const char*>(addr_), length_);
  }

 private:
  MemMapping() {}

  // The mapped memory, or nullptr on failure / not supported
  void* addr_ = nullptr;
  // The known usable number of bytes starting at that address
  size_t length_ = 0;

#ifdef OS_WIN
  HANDLE page_file_handle_ = NULL;
#endif  // OS_WIN

  static MemMapping AllocateAnonymous(size_t length, bool huge,
                                      bool no_reserve);
};

// Simple MemMapping wrapper that presents the memory as an array of T.
// For example,
//  TypedMemMapping<uint64_t> arr = MemMapping::AllocateLazyZeroed(num_bytes);
template <typename T>
class TypedMemMapping : public MemMapping {
 public:
  /*implicit*/ TypedMemMapping(MemMapping&& v) noexcept
      : MemMapping(std::move(v)) {}
  TypedMemMapping& operator=(MemMapping&& v) noexcept {
    MemMapping& base = *this;
    base = std::move(v);
    return *this;
  }

  inline T* Get() const { return static_cast<T*>(MemMapping::Get()); }
  inline size_t Count() const { return MemMapping::Length() / sizeof(T); }

  inline T& operator[](size_t index) const { return Get()[index]; }
};

}  // namespace ROCKSDB_NAMESPACE
