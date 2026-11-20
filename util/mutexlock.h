//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <assert.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "port/port.h"
#include "util/fastrange.h"
#include "util/hash.h"

namespace ROCKSDB_NAMESPACE {

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class MutexLock {
 public:
  explicit MutexLock(port::Mutex* mu) : mu_(mu) { this->mu_->Lock(); }
  // No copying allowed
  MutexLock(const MutexLock&) = delete;
  void operator=(const MutexLock&) = delete;
  MutexLock(MutexLock&&) = delete;
  void operator=(MutexLock&&) = delete;

  ~MutexLock() { this->mu_->Unlock(); }

 private:
  port::Mutex* const mu_;
};

//
// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released when the
// object goes out of scope.
//
class ReadLock {
 public:
  explicit ReadLock(port::RWMutex* mu) : mu_(mu) { this->mu_->ReadLock(); }
  // No copying allowed
  ReadLock(const ReadLock&) = delete;
  void operator=(const ReadLock&) = delete;
  ReadLock(ReadLock&&) = delete;
  void operator=(ReadLock&&) = delete;

  ~ReadLock() { this->mu_->ReadUnlock(); }

 private:
  port::RWMutex* const mu_;
};

//
// Try to acquire a ReadLock on the specified RWMutex without blocking.
//
class TryReadLock {
 public:
  explicit TryReadLock(port::RWMutex* mu)
      : mu_(mu), owns_(mu_->TryReadLock()) {}
  // No copying allowed
  TryReadLock(const TryReadLock&) = delete;
  void operator=(const TryReadLock&) = delete;
  TryReadLock(TryReadLock&&) = delete;
  void operator=(TryReadLock&&) = delete;

  ~TryReadLock() {
    if (owns_) {
      mu_->ReadUnlock();
    }
  }

  bool OwnsLock() const { return owns_; }

 private:
  port::RWMutex* const mu_;
  const bool owns_;
};

//
// Automatically unlock a locked mutex when the object is destroyed
//
class ReadUnlock {
 public:
  explicit ReadUnlock(port::RWMutex* mu) : mu_(mu) { mu->AssertHeld(); }
  // No copying allowed
  ReadUnlock(const ReadUnlock&) = delete;
  ReadUnlock& operator=(const ReadUnlock&) = delete;
  ReadUnlock(ReadUnlock&&) = delete;
  ReadUnlock& operator=(ReadUnlock&&) = delete;

  ~ReadUnlock() { mu_->ReadUnlock(); }

 private:
  port::RWMutex* const mu_;
};

//
// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class WriteLock {
 public:
  explicit WriteLock(port::RWMutex* mu) : mu_(mu) { this->mu_->WriteLock(); }
  // No copying allowed
  WriteLock(const WriteLock&) = delete;
  void operator=(const WriteLock&) = delete;
  WriteLock(WriteLock&&) = delete;
  void operator=(WriteLock&&) = delete;

  ~WriteLock() { this->mu_->WriteUnlock(); }

 private:
  port::RWMutex* const mu_;
};

//
// Try to acquire a WriteLock on the specified RWMutex without blocking.
// If acquired, the lock is released when the object goes out of scope.
// If not acquired, the destructor is a no-op. Use OwnsLock() to check
// whether the lock was acquired.
//
class TryWriteLock {
 public:
  explicit TryWriteLock(port::RWMutex* mu)
      : mu_(mu), owns_(mu_->TryWriteLock()) {}
  // No copying allowed
  TryWriteLock(const TryWriteLock&) = delete;
  void operator=(const TryWriteLock&) = delete;
  TryWriteLock(TryWriteLock&&) = delete;
  void operator=(TryWriteLock&&) = delete;

  ~TryWriteLock() {
    if (owns_) {
      mu_->WriteUnlock();
    }
  }

  bool OwnsLock() const { return owns_; }

 private:
  port::RWMutex* const mu_;
  const bool owns_;
};

//
// SpinMutex has very low overhead for low-contention cases.  Method names
// are chosen so you can use std::unique_lock or std::lock_guard with it.
//
class SpinMutex {
 public:
  SpinMutex() : locked_(false) {}

  bool try_lock() {
    auto currently_locked = locked_.load(std::memory_order_relaxed);
    return !currently_locked &&
           locked_.compare_exchange_weak(currently_locked, true,
                                         std::memory_order_acquire,
                                         std::memory_order_relaxed);
  }

  void lock() {
    for (size_t tries = 0;; ++tries) {
      if (try_lock()) {
        // success
        break;
      }
      port::AsmVolatilePause();
      if (tries > 100) {
        std::this_thread::yield();
      }
    }
  }

  void unlock() { locked_.store(false, std::memory_order_release); }

 private:
  std::atomic<bool> locked_;
};

// For preventing false sharing, especially for mutexes.
// NOTE: if a mutex is less than half the size of a cache line, it would
// make more sense for Striped structure below to pack more than one mutex
// into each cache line, as this would only reduce contention for the same
// amount of space and cache sharing. However, a mutex is often 40 bytes out
// of a 64 byte cache line.
template <class T>
struct ALIGN_AS(CACHE_LINE_SIZE) CacheAlignedWrapper {
  T obj_;
};
template <class T>
struct Unwrap {
  using type = T;
  static type& Go(T& t) { return t; }
};
template <class T>
struct Unwrap<CacheAlignedWrapper<T>> {
  using type = T;
  static type& Go(CacheAlignedWrapper<T>& t) { return t.obj_; }
};

//
// Inspired by Guava: https://github.com/google/guava/wiki/StripedExplained
// A striped Lock. This offers the underlying lock striping similar
// to that of ConcurrentHashMap in a reusable form, and extends it for
// semaphores and read-write locks. Conceptually, lock striping is the technique
// of dividing a lock into many <i>stripes</i>, increasing the granularity of a
// single lock and allowing independent operations to lock different stripes and
// proceed concurrently, instead of creating contention for a single lock.
//
template <class T, class Key = Slice, class Hash = SliceNPHasher64>
class Striped {
 public:
  explicit Striped(size_t stripe_count)
      : stripe_count_(stripe_count), data_(new T[stripe_count]) {}

  using Unwrapped = typename Unwrap<T>::type;
  Unwrapped& Get(const Key& key, uint64_t seed = 0) {
    size_t index = FastRangeGeneric(hash_(key, seed), stripe_count_);
    return Unwrap<T>::Go(data_[index]);
  }

  size_t ApproximateMemoryUsage() const {
    // NOTE: could use malloc_usable_size() here, but that could count unmapped
    // pages and could mess up unit test OccLockBucketsTest::CacheAligned
    return sizeof(*this) + stripe_count_ * sizeof(T);
  }

 private:
  size_t stripe_count_;
  std::unique_ptr<T[]> data_;
  Hash hash_;
};

}  // namespace ROCKSDB_NAMESPACE
