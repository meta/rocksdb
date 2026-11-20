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

#include <thread>

#include "rocksdb/port_defs.h"
#include "rocksdb/rocksdb_namespace.h"

// size_t printf formatting named in the manner of C99 standard formatting
// strings such as PRIu64
// in fact, we could use that one
#define ROCKSDB_PRIszt "zu"

#define __declspec(S)

#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)
#include <machine/endian.h>
#if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
#define PLATFORM_IS_LITTLE_ENDIAN \
  (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
#endif
#elif defined(OS_AIX)
#include <arpa/nameser_compat.h>
#include <sys/types.h>
#define PLATFORM_IS_LITTLE_ENDIAN (BYTE_ORDER == LITTLE_ENDIAN)
#include <alloca.h>
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) || \
    defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID)
#include <sys/endian.h>
#include <sys/types.h>
#define PLATFORM_IS_LITTLE_ENDIAN (_BYTE_ORDER == _LITTLE_ENDIAN)
#else
#include <endian.h>
#endif
#include <pthread.h>
#include <stdint.h>
#include <string.h>

#include <limits>
#include <string>

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) || defined(OS_NETBSD) ||        \
    defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID) || \
    defined(CYGWIN) || defined(OS_AIX)
// Use fread/fwrite/fflush on platforms without _unlocked variants
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) || defined(OS_OPENBSD) || \
    defined(OS_DRAGONFLYBSD)
// Use fsync() on platforms without fdatasync()
#define fdatasync fsync
#endif

#if defined(OS_ANDROID) && __ANDROID_API__ < 9
// fdatasync() was only introduced in API level 9 on Android. Use fsync()
// when targeting older platforms.
#define fdatasync fsync
#endif

namespace ROCKSDB_NAMESPACE {

extern const bool kDefaultToAdaptiveMutex;

namespace port {
constexpr bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {
 public:
  static const char* kName() { return "pthread_mutex_t"; }

  explicit Mutex(bool adaptive = kDefaultToAdaptiveMutex);
  // No copying
  Mutex(const Mutex&) = delete;
  void operator=(const Mutex&) = delete;
  Mutex(Mutex&&) = delete;
  void operator=(Mutex&&) = delete;

  ~Mutex();

  void Lock();
  void Unlock();

  bool TryLock();

  // This will fail assertion if the mutex is not locked.
  // It does NOT verify that mutex is held by a calling thread.
  void AssertHeld() const;

  // Also implement std Lockable
  inline void lock() { Lock(); }
  inline void unlock() { Unlock(); }
  inline bool try_lock() { return TryLock(); }

 private:
  friend class CondVar;
  pthread_mutex_t mu_;
#ifndef NDEBUG
  bool locked_ = false;
#endif
};

class RWMutex {
 public:
  RWMutex();
  // No copying allowed
  RWMutex(const RWMutex&) = delete;
  void operator=(const RWMutex&) = delete;
  RWMutex(RWMutex&&) = delete;
  void operator=(RWMutex&&) = delete;

  ~RWMutex();

  void ReadLock();
  bool TryReadLock();
  void WriteLock();
  bool TryWriteLock();
  void ReadUnlock();
  void WriteUnlock();
  void AssertHeld() const {}

 private:
  pthread_rwlock_t mu_;  // the underlying platform mutex
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();

  Mutex* GetMutex() const { return mu_; }

  void Wait();
  // Timed condition wait.  Returns true if timeout occurred.
  bool TimedWait(uint64_t abs_time_us);
  void Signal();
  void SignalAll();

 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

using Thread = std::thread;

static inline void AsmVolatilePause() {
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("isb");
#elif defined(__powerpc64__)
  asm volatile("or 27,27,27");
#elif defined(__loongarch64)
  asm volatile("dbar 0");
#endif
  // it's okay for other platforms to be no-ops
}

// Returns -1 if not available on this platform
int PhysicalCoreID();

using OnceType = pthread_once_t;
#define LEVELDB_ONCE_INIT PTHREAD_ONCE_INIT
void InitOnce(OnceType* once, void (*initializer)());

#ifndef CACHE_LINE_SIZE
// To test behavior with non-native cache line size, e.g. for
// Bloom filters, set TEST_CACHE_LINE_SIZE to the desired test size.
// This disables ALIGN_AS to keep it from failing compilation.
#ifdef TEST_CACHE_LINE_SIZE
#define CACHE_LINE_SIZE TEST_CACHE_LINE_SIZE
#define ALIGN_AS(n) /*empty*/
#else
#if defined(__s390__)
#if defined(__GNUC__) && __GNUC__ < 7
#define CACHE_LINE_SIZE 64U
#else
#define CACHE_LINE_SIZE 256U
#endif
#elif defined(__powerpc__) || defined(__aarch64__)
#define CACHE_LINE_SIZE 128U
#else
#define CACHE_LINE_SIZE 64U
#endif
#define ALIGN_AS(n) alignas(n)
#endif
#endif

static_assert((CACHE_LINE_SIZE & (CACHE_LINE_SIZE - 1)) == 0,
              "Cache line size must be a power of 2 number of bytes");

void* cacheline_aligned_alloc(size_t size);

void cacheline_aligned_free(void* memblock);

#if defined(__aarch64__)
//  __builtin_prefetch(..., 1) turns into a prefetch into prfm pldl3keep. On
// arm64 we want this as close to the core as possible to turn it into a
// L1 prefetech unless locality == 0 in which case it will be turned into a
// non-temporal prefetch
#define PREFETCH(addr, rw, locality) \
  __builtin_prefetch(addr, rw, locality >= 1 ? 3 : locality)
#else
#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)
#endif

void Crash(const std::string& srcfile, int srcline);

// Terminates the process immediately with the given exit code, bypassing the
// usual shutdown path: no atexit handlers run and no static/global destructors
// are invoked (POSIX _exit(); same on Windows). This is the safe way to abort
// from a process that still has background threads running (e.g. RocksDB's
// compaction/flush threads). A normal exit() would tear down static objects
// those threads are concurrently accessing, causing cross-thread
// use-after-free. Use this instead of exit() once a DB has been opened and
// background threads may be live.
[[noreturn]] void ImmediateExit(int code);

int GetMaxOpenFiles();

extern const size_t kPageSize;

using ThreadId = pid_t;

void SetCpuPriority(ThreadId id, CpuPriority priority);

int64_t GetProcessID();

// Uses platform APIs to generate a 36-character RFC-4122 UUID. Returns
// true on success or false on failure.
bool GenerateRfcUuid(std::string* output);

}  // namespace port
}  // namespace ROCKSDB_NAMESPACE
