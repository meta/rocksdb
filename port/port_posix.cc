//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#if !defined(OS_WIN)

#include "port/port_posix.h"

#include <cassert>
#if defined(__i386__) || defined(__x86_64__)
#include <cpuid.h>
#endif
#include <sched.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>

#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

// We want to give users opportunity to default all the mutexes to adaptive if
// not specified otherwise. This enables a quick way to conduct various
// performance related experiements.
//
// NB! Support for adaptive mutexes is turned on by definining
// ROCKSDB_PTHREAD_ADAPTIVE_MUTEX during the compilation. If you use RocksDB
// build environment then this happens automatically; otherwise it's up to the
// consumer to define the identifier.
#ifdef ROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX
const bool kDefaultToAdaptiveMutex = true;
#else
const bool kDefaultToAdaptiveMutex = false;
#endif

namespace port {

static int PthreadCall(const char* label, int result) {
  if (result != 0 && result != ETIMEDOUT && result != EBUSY) {
    fprintf(stderr, "pthread %s: %s\n", label, errnoStr(result).c_str());
    abort();
  }
  return result;
}

Mutex::Mutex(bool adaptive) {
  (void)adaptive;
#ifdef ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
  if (!adaptive) {
    PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
  } else {
    pthread_mutexattr_t mutex_attr;
    PthreadCall("init mutex attr", pthread_mutexattr_init(&mutex_attr));
    PthreadCall("set mutex attr", pthread_mutexattr_settype(
                                      &mutex_attr, PTHREAD_MUTEX_ADAPTIVE_NP));
    PthreadCall("init mutex", pthread_mutex_init(&mu_, &mutex_attr));
    PthreadCall("destroy mutex attr", pthread_mutexattr_destroy(&mutex_attr));
  }
#else
  PthreadCall("init mutex", pthread_mutex_init(&mu_, nullptr));
#endif  // ROCKSDB_PTHREAD_ADAPTIVE_MUTEX
}

Mutex::~Mutex() { PthreadCall("destroy mutex", pthread_mutex_destroy(&mu_)); }

void Mutex::Lock() {
  PthreadCall("lock", pthread_mutex_lock(&mu_));
#ifndef NDEBUG
  locked_ = true;
#endif
}

void Mutex::Unlock() {
#ifndef NDEBUG
  locked_ = false;
#endif
  PthreadCall("unlock", pthread_mutex_unlock(&mu_));
}

bool Mutex::TryLock() {
  bool ret = PthreadCall("trylock", pthread_mutex_trylock(&mu_)) == 0;
#ifndef NDEBUG
  if (ret) {
    locked_ = true;
  }
#endif
  return ret;
}

void Mutex::AssertHeld() const {
#ifndef NDEBUG
  assert(locked_);
#endif
}

CondVar::CondVar(Mutex* mu) : mu_(mu) {
  PthreadCall("init cv", pthread_cond_init(&cv_, nullptr));
}

CondVar::~CondVar() { PthreadCall("destroy cv", pthread_cond_destroy(&cv_)); }

void CondVar::Wait() {
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  PthreadCall("wait", pthread_cond_wait(&cv_, &mu_->mu_));
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
}

bool CondVar::TimedWait(uint64_t abs_time_us) {
  struct timespec ts;
  ts.tv_sec = static_cast<time_t>(abs_time_us / 1000000);
  ts.tv_nsec = static_cast<suseconds_t>((abs_time_us % 1000000) * 1000);

#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  int err = pthread_cond_timedwait(&cv_, &mu_->mu_, &ts);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  if (err == ETIMEDOUT) {
    return true;
  }
  if (err != 0) {
    PthreadCall("timedwait", err);
  }
  return false;
}

void CondVar::Signal() { PthreadCall("signal", pthread_cond_signal(&cv_)); }

void CondVar::SignalAll() {
  PthreadCall("broadcast", pthread_cond_broadcast(&cv_));
}

RWMutex::RWMutex() {
  PthreadCall("init mutex", pthread_rwlock_init(&mu_, nullptr));
}

RWMutex::~RWMutex() {
  PthreadCall("destroy mutex", pthread_rwlock_destroy(&mu_));
}

void RWMutex::ReadLock() {
  PthreadCall("read lock", pthread_rwlock_rdlock(&mu_));
}

bool RWMutex::TryReadLock() {
  int result = pthread_rwlock_tryrdlock(&mu_);
  if (result == 0) {
    return true;
  }
  if (result == EBUSY) {
    return false;
  }
  PthreadCall("try read lock", result);
  return false;
}

void RWMutex::WriteLock() {
  PthreadCall("write lock", pthread_rwlock_wrlock(&mu_));
}

bool RWMutex::TryWriteLock() {
  int result = pthread_rwlock_trywrlock(&mu_);
  if (result == 0) {
    return true;
  }
  if (result == EBUSY) {
    return false;
  }
  PthreadCall("try write lock", result);
  return false;
}

void RWMutex::ReadUnlock() {
  PthreadCall("read unlock", pthread_rwlock_unlock(&mu_));
}

void RWMutex::WriteUnlock() {
  PthreadCall("write unlock", pthread_rwlock_unlock(&mu_));
}

int PhysicalCoreID() {
#if defined(ROCKSDB_SCHED_GETCPU_PRESENT) &&                             \
    (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 35))
  // sched_getcpu uses VDSO getcpu() syscall since glibc 2.22 on x86_64, and
  // restartable sequences since glibc 2.35 everywhere. This is the
  // fastest/preferred method if available. The macro just checks for glibc
  // >= 2.35 since it was released in 2022.
  int cpuno = sched_getcpu();
  if (cpuno < 0) {
    return -1;
  }
  return cpuno;
#elif defined(__x86_64__) || defined(__i386__)
  // clang/gcc both provide cpuid.h, which defines __get_cpuid(), for x86_64 and
  // i386.
  unsigned eax, ebx = 0, ecx, edx;
  if (!__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
    return -1;
  }
  return ebx >> 24;
#else
  // give up, the caller can generate a random number or something.
  return -1;
#endif
}

void InitOnce(OnceType* once, void (*initializer)()) {
  PthreadCall("once", pthread_once(once, initializer));
}

void Crash(const std::string& srcfile, int srcline) {
  fprintf(stdout, "Crashing at %s:%d\n", srcfile.c_str(), srcline);
  fflush(stdout);
  kill(getpid(), SIGTERM);
}

void ImmediateExit(int code) { _exit(code); }

int GetMaxOpenFiles() {
#if defined(RLIMIT_NOFILE)
  struct rlimit no_files_limit;
  if (getrlimit(RLIMIT_NOFILE, &no_files_limit) != 0) {
    return -1;
  }
  // protect against overflow
  if (static_cast<uintmax_t>(no_files_limit.rlim_cur) >=
      static_cast<uintmax_t>(std::numeric_limits<int>::max())) {
    return std::numeric_limits<int>::max();
  }
  return static_cast<int>(no_files_limit.rlim_cur);
#else
  return -1;
#endif
}

void* cacheline_aligned_alloc(size_t size) {
#if __GNUC__ < 5 && defined(__SANITIZE_ADDRESS__)
  return malloc(size);
#elif (_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600 || defined(__APPLE__))
  void* m;
  errno = posix_memalign(&m, CACHE_LINE_SIZE, size);
  return errno ? nullptr : m;
#else
  return malloc(size);
#endif
}

void cacheline_aligned_free(void* memblock) { free(memblock); }

static size_t GetPageSize() {
#if defined(OS_LINUX) || defined(_SC_PAGESIZE)
  long v = sysconf(_SC_PAGESIZE);
  if (v >= 1024) {
    return static_cast<size_t>(v);
  }
#endif
  // Default assume 4KB
  return 4U * 1024U;
}

const size_t kPageSize = GetPageSize();

void SetCpuPriority(ThreadId id, CpuPriority priority) {
#ifdef OS_LINUX
  sched_param param;
  param.sched_priority = 0;
  switch (priority) {
    case CpuPriority::kHigh:
      sched_setscheduler(id, SCHED_OTHER, &param);
      setpriority(PRIO_PROCESS, id, -20);
      break;
    case CpuPriority::kNormal:
      sched_setscheduler(id, SCHED_OTHER, &param);
      setpriority(PRIO_PROCESS, id, 0);
      break;
    case CpuPriority::kLow:
      sched_setscheduler(id, SCHED_OTHER, &param);
      setpriority(PRIO_PROCESS, id, 19);
      break;
    case CpuPriority::kIdle:
      sched_setscheduler(id, SCHED_IDLE, &param);
      break;
    default:
      assert(false);
  }
#else
  (void)id;
  (void)priority;
#endif
}

int64_t GetProcessID() { return getpid(); }

bool GenerateRfcUuid(std::string* output) {
  output->clear();
  std::ifstream f("/proc/sys/kernel/random/uuid");
  std::getline(f, /*&*/ *output);
  if (output->size() == 36) {
    return true;
  } else {
    output->clear();
    return false;
  }
}

}  // namespace port
}  // namespace ROCKSDB_NAMESPACE

#endif
