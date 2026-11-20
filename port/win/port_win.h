//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// See port_example.h for documentation for the following types/functions.

#pragma once

// Always want minimum headers
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <windows.h>
//^^ <windows.h> should be included first before other system lib
#include <intrin.h>
#include <malloc.h>
#include <process.h>
#include <stdint.h>
#include <string.h>

#include <cassert>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <string>
#include <thread>

#include "port/win/win_thread.h"
#include "rocksdb/port_defs.h"

#undef min
#undef max
#undef DeleteFile
#undef GetCurrentTime

#ifndef strcasecmp
#define strcasecmp _stricmp
#endif

#undef GetCurrentTime
#undef DeleteFile

#ifndef _SSIZE_T_DEFINED
using ssize_t = SSIZE_T;
#endif

// size_t printf formatting named in the manner of C99 standard formatting
// strings such as PRIu64
// in fact, we could use that one
#ifndef ROCKSDB_PRIszt
#define ROCKSDB_PRIszt "Iu"
#endif

#ifdef _MSC_VER
#define __attribute__(A)

#endif

namespace ROCKSDB_NAMESPACE {

#define PREFETCH(addr, rw, locality)

extern const bool kDefaultToAdaptiveMutex;

namespace port {

// "Windows is designed to run on little-endian computer architectures."
// https://docs.microsoft.com/en-us/windows/win32/sysinfo/registry-value-types
constexpr bool kLittleEndian = true;
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {
 public:
  static const char* kName() { return "std::mutex"; }

  explicit Mutex(bool IGNORED_adaptive = kDefaultToAdaptiveMutex)
#ifndef NDEBUG
      : locked_(false)
#endif
  {
    (void)IGNORED_adaptive;
  }

  ~Mutex();

  void Lock() {
    mutex_.lock();
#ifndef NDEBUG
    locked_ = true;
#endif
  }

  void Unlock() {
#ifndef NDEBUG
    locked_ = false;
#endif
    mutex_.unlock();
  }

  bool TryLock() {
    bool ret = mutex_.try_lock();
#ifndef NDEBUG
    if (ret) {
      locked_ = true;
    }
#endif
    return ret;
  }

  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld() const {
#ifndef NDEBUG
    assert(locked_);
#endif
  }

  // Also implement std Lockable
  inline void lock() { Lock(); }
  inline void unlock() { Unlock(); }
  inline bool try_lock() { return TryLock(); }

  // Mutex is move only with lock ownership transfer
  Mutex(const Mutex&) = delete;
  void operator=(const Mutex&) = delete;

 private:
  friend class CondVar;

  std::mutex& getLock() { return mutex_; }

  std::mutex mutex_;
#ifndef NDEBUG
  bool locked_;
#endif
};

class RWMutex {
 public:
  RWMutex() { InitializeSRWLock(&srwLock_); }
  // No copying allowed
  RWMutex(const RWMutex&) = delete;
  void operator=(const RWMutex&) = delete;

  void ReadLock() { AcquireSRWLockShared(&srwLock_); }

  bool TryReadLock() { return TryAcquireSRWLockShared(&srwLock_) != 0; }

  void WriteLock() { AcquireSRWLockExclusive(&srwLock_); }

  bool TryWriteLock() { return TryAcquireSRWLockExclusive(&srwLock_) != 0; }

  void ReadUnlock() { ReleaseSRWLockShared(&srwLock_); }

  void WriteUnlock() { ReleaseSRWLockExclusive(&srwLock_); }

  // Empty as in POSIX
  void AssertHeld() const {}

 private:
  SRWLOCK srwLock_;
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu) : mu_(mu) {}

  ~CondVar();

  Mutex* GetMutex() const { return mu_; }

  void Wait();
  bool TimedWait(uint64_t expiration_time);
  void Signal();
  void SignalAll();

  // Condition var is not copy/move constructible
  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  CondVar(CondVar&&) = delete;
  CondVar& operator=(CondVar&&) = delete;

 private:
  std::condition_variable cv_;
  Mutex* mu_;
};

#ifdef _POSIX_THREADS
using Thread = std::jthread;
#else
// Wrapper around the platform efficient
// or otherwise preferrable implementation
using Thread = WindowsThread;
#endif

// OnceInit type helps emulate
// Posix semantics with initialization
// adopted in the project
struct OnceType {
  struct Init {};

  OnceType() {}
  OnceType(const Init&) {}
  OnceType(const OnceType&) = delete;
  OnceType& operator=(const OnceType&) = delete;

  std::once_flag flag_;
};

#define LEVELDB_ONCE_INIT port::OnceType::Init()
void InitOnce(OnceType* once, void (*initializer)());

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64U
#endif

#ifdef ROCKSDB_JEMALLOC
// Separate inlines so they can be replaced if needed
void* jemalloc_aligned_alloc(size_t size, size_t alignment) noexcept;
void jemalloc_aligned_free(void* p) noexcept;
#endif

inline void* cacheline_aligned_alloc(size_t size) {
#ifdef ROCKSDB_JEMALLOC
  return jemalloc_aligned_alloc(size, CACHE_LINE_SIZE);
#else
  return _aligned_malloc(size, CACHE_LINE_SIZE);
#endif
}

inline void cacheline_aligned_free(void* memblock) {
#ifdef ROCKSDB_JEMALLOC
  jemalloc_aligned_free(memblock);
#else
  _aligned_free(memblock);
#endif
}

extern const size_t kPageSize;

// Part of C++11
#define ALIGN_AS(n) alignas(n)

static inline void AsmVolatilePause() {
#if defined(_M_IX86) || defined(_M_X64) || defined(_M_ARM64) || defined(_M_ARM)
  YieldProcessor();
#endif
  // it would be nice to get "wfe" on ARM here
}

int PhysicalCoreID();

// For Thread Local Storage abstraction
using pthread_key_t = DWORD;

inline int pthread_key_create(pthread_key_t* key, void (*destructor)(void*)) {
  // Not used
  (void)destructor;

  pthread_key_t k = TlsAlloc();
  if (TLS_OUT_OF_INDEXES == k) {
    return ENOMEM;
  }

  *key = k;
  return 0;
}

inline int pthread_key_delete(pthread_key_t key) {
  if (!TlsFree(key)) {
    return EINVAL;
  }
  return 0;
}

inline int pthread_setspecific(pthread_key_t key, const void* value) {
  if (!TlsSetValue(key, const_cast<void*>(value))) {
    return ENOMEM;
  }
  return 0;
}

inline void* pthread_getspecific(pthread_key_t key) {
  void* result = TlsGetValue(key);
  if (!result) {
    if (GetLastError() != ERROR_SUCCESS) {
      errno = EINVAL;
    } else {
      errno = NOERROR;
    }
  }
  return result;
}

// UNIX equiv although errno numbers will be off
// using C-runtime to implement. Note, this does not
// feel space with zeros in case the file is extended.
int truncate(const char* path, int64_t length);
int Truncate(std::string path, int64_t length);
void Crash(const std::string& srcfile, int srcline);
// See ImmediateExit in port/port_posix.h for documentation.
[[noreturn]] void ImmediateExit(int code);
int GetMaxOpenFiles();
std::string utf16_to_utf8(const std::wstring& utf16);
std::wstring utf8_to_utf16(const std::string& utf8);

using ThreadId = int;

void SetCpuPriority(ThreadId id, CpuPriority priority);

int64_t GetProcessID();

// Uses platform APIs to generate a 36-character RFC-4122 UUID. Returns
// true on success or false on failure.
bool GenerateRfcUuid(std::string* output);

}  // namespace port

#ifdef ROCKSDB_WINDOWS_UTF8_FILENAMES

#define RX_FILESTRING std::wstring
#define RX_FN(a) ROCKSDB_NAMESPACE::port::utf8_to_utf16(a)
#define FN_TO_RX(a) ROCKSDB_NAMESPACE::port::utf16_to_utf8(a)
#define RX_FNCMP(a, b) ::wcscmp(a, RX_FN(b).c_str())
#define RX_FNLEN(a) ::wcslen(a)

#define RX_DeleteFile DeleteFileW
#define RX_CreateFile CreateFileW
#define RX_CreateFileMapping CreateFileMappingW
#define RX_GetFileAttributesEx GetFileAttributesExW
#define RX_FindFirstFileEx FindFirstFileExW
#define RX_FindNextFile FindNextFileW
#define RX_WIN32_FIND_DATA WIN32_FIND_DATAW
#define RX_CreateDirectory CreateDirectoryW
#define RX_RemoveDirectory RemoveDirectoryW
#define RX_GetFileAttributesEx GetFileAttributesExW
#define RX_MoveFileEx MoveFileExW
#define RX_CreateHardLink CreateHardLinkW
#define RX_PathIsRelative PathIsRelativeW
#define RX_GetCurrentDirectory GetCurrentDirectoryW
#define RX_GetDiskFreeSpaceEx GetDiskFreeSpaceExW
#define RX_PathIsDirectory PathIsDirectoryW

#else

#define RX_FILESTRING std::string
#define RX_FN(a) a
#define FN_TO_RX(a) a
#define RX_FNCMP(a, b) strcmp(a, b)
#define RX_FNLEN(a) strlen(a)

#define RX_DeleteFile DeleteFileA
#define RX_CreateFile CreateFileA
#define RX_CreateFileMapping CreateFileMappingA
#define RX_GetFileAttributesEx GetFileAttributesExA
#define RX_FindFirstFileEx FindFirstFileExA
#define RX_CreateDirectory CreateDirectoryA
#define RX_FindNextFile FindNextFileA
#define RX_WIN32_FIND_DATA WIN32_FIND_DATAA
#define RX_CreateDirectory CreateDirectoryA
#define RX_RemoveDirectory RemoveDirectoryA
#define RX_GetFileAttributesEx GetFileAttributesExA
#define RX_MoveFileEx MoveFileExA
#define RX_CreateHardLink CreateHardLinkA
#define RX_PathIsRelative PathIsRelativeA
#define RX_GetCurrentDirectory GetCurrentDirectoryA
#define RX_GetDiskFreeSpaceEx GetDiskFreeSpaceExA
#define RX_PathIsDirectory PathIsDirectoryA

#endif

using port::pthread_getspecific;
using port::pthread_key_create;
using port::pthread_key_delete;
using port::pthread_key_t;
using port::pthread_setspecific;
using port::truncate;

}  // namespace ROCKSDB_NAMESPACE
