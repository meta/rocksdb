//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <version>

#if defined(__cpp_lib_source_location) && __has_include(<source_location>)
#include <source_location>
#endif

#include "monitoring/statistics_impl.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/thread_status.h"
#include "test_util/sync_point.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
class InstrumentedCondVar;

#if defined(__cpp_lib_source_location) && __has_include(<source_location>) && \
    !defined(ROCKSDB_TEST_MUTEX_LOCATION_FALLBACK)
using MutexSourceLocation = std::source_location;
#else
// Older libc++ lacks source_location; compiler builtins preserve caller sites.
class MutexSourceLocation {
 public:
#if defined(__GNUC__) || defined(__clang__)
  static constexpr MutexSourceLocation current(
      const char* file = __builtin_FILE(), uint32_t line = __builtin_LINE(),
      const char* function = __builtin_FUNCTION()) noexcept {
    return MutexSourceLocation(file, line, function);
  }
#else
  static constexpr MutexSourceLocation current() noexcept { return {}; }
#endif
  constexpr MutexSourceLocation() noexcept = default;
  constexpr const char* file_name() const noexcept { return file_; }
  constexpr uint32_t line() const noexcept { return line_; }
  constexpr const char* function_name() const noexcept { return function_; }

 private:
  constexpr MutexSourceLocation(const char* file, uint32_t line,
                                const char* function) noexcept
      : file_(file), function_(function), line_(line) {}
  const char* file_ = "";
  const char* function_ = "";
  uint32_t line_ = 0;
};
#endif

struct SlowMutexLoggingContext {
  SlowMutexLoggingContext(Statistics* s, SystemClock* c, Env* e,
                          std::shared_ptr<Logger> l,
                          const std::atomic<uint64_t>* threshold)
      : stats(s),
        clock(c),
        env(e),
        logger(std::move(l)),
        threshold_us(threshold) {}

  Statistics* stats;
  SystemClock* clock;
  Env* env;
  std::shared_ptr<Logger> logger;
  const std::atomic<uint64_t>* threshold_us;
  std::atomic<bool> paused{false};
  std::atomic<bool> stopped{false};
  std::mutex emission_mutex;
};

// Enter before taking DB closing locks, not from the context's own Logv().
// Failed close attempts resume probes unless the logger was closed.
class SlowMutexLoggingPause {
 public:
  explicit SlowMutexLoggingPause(
      std::shared_ptr<SlowMutexLoggingContext> context)
      : context_(std::move(context)) {
    if (context_) {
      lock_ = std::unique_lock<std::mutex>(context_->emission_mutex);
      context_->paused.store(true, std::memory_order_relaxed);
    }
  }
  SlowMutexLoggingPause(const SlowMutexLoggingPause&) = delete;
  SlowMutexLoggingPause& operator=(const SlowMutexLoggingPause&) = delete;
  SlowMutexLoggingPause(SlowMutexLoggingPause&&) = delete;
  SlowMutexLoggingPause& operator=(SlowMutexLoggingPause&&) = delete;
  ~SlowMutexLoggingPause() {
    if (context_) {
      context_->paused.store(false, std::memory_order_relaxed);
    }
  }

 private:
  std::shared_ptr<SlowMutexLoggingContext> context_;
  std::unique_lock<std::mutex> lock_;
};

// Configured only for explicitly selected DB-wide mutexes, before first use.
// Start/Stop require ownership; FlushLogs must run after the actual unlock.
// Stop receives compiler-backed strings; it retains only sampled release sites.
class SlowMutexTimer {
 public:
  SlowMutexTimer(std::shared_ptr<SlowMutexLoggingContext> context,
                 const char* name, const void* mutex);
  SlowMutexTimer(const SlowMutexTimer&) = delete;
  SlowMutexTimer& operator=(const SlowMutexTimer&) = delete;
  SlowMutexTimer(SlowMutexTimer&&) = delete;
  SlowMutexTimer& operator=(SlowMutexTimer&&) = delete;
  ~SlowMutexTimer() = default;

  void Start();
  void Stop(const MutexSourceLocation& location, const char* release_kind);
  static void FlushLogs();
  Statistics* GetStatistics() const { return context_->stats; }

 private:
  std::shared_ptr<SlowMutexLoggingContext> context_;
  const char* name_;
  const void* mutex_;
  uint64_t start_ns_ = 0;
  uint64_t last_log_ns_ = 0;
  ThreadStatus::OperationType operation_ = ThreadStatus::OP_UNKNOWN;
  bool active_ = false;
  bool logged_ = false;
};

// Defers emission across callers that own other, unmeasured locks.
class SlowMutexLogScope {
 public:
  explicit SlowMutexLogScope(bool armed) : armed_(armed) {
    if (armed_) {
      Begin();
    }
  }
  ~SlowMutexLogScope() { Release(); }
  void Release() {
    if (armed_) {
      armed_ = false;
      End();
    }
  }
  SlowMutexLogScope(const SlowMutexLogScope&) = delete;
  SlowMutexLogScope& operator=(const SlowMutexLogScope&) = delete;
  SlowMutexLogScope(SlowMutexLogScope&&) = delete;
  SlowMutexLogScope& operator=(SlowMutexLogScope&&) = delete;

 private:
  static void Begin();
  static void End();
  bool armed_;
};

// Compiler-supplied locations are defaulted at caller-facing methods so direct
// releases need no call-site edits. Scope-exit locations identify the guard
// declaration, not a compiler-synthesized destructor or closing-brace line.
// No stack capture or symbolization is performed.
// Preserves std::mutex locking semantics while timing a selected mutex.
class SlowMutexLock {
 public:
  SlowMutexLock(std::mutex* mutex, SlowMutexTimer* timer,
                MutexSourceLocation location = MutexSourceLocation::current())
      : mutex_(mutex), timer_(timer), location_(location) {
    mutex_->lock();
    if (timer_) {
      timer_->Start();
    }
  }
  ~SlowMutexLock() {
    if (timer_) {
      timer_->Stop(location_, "scope_exit");
    }
    mutex_->unlock();
    if (timer_) {
      SlowMutexTimer::FlushLogs();
    }
  }
  SlowMutexLock(const SlowMutexLock&) = delete;
  SlowMutexLock& operator=(const SlowMutexLock&) = delete;
  SlowMutexLock(SlowMutexLock&&) = delete;
  SlowMutexLock& operator=(SlowMutexLock&&) = delete;

 private:
  std::mutex* mutex_;
  SlowMutexTimer* timer_;
  MutexSourceLocation location_;
};

// A wrapper class for port::Mutex that provides additional layer
// for collecting stats and instrumentation.
class InstrumentedMutex {
 public:
  explicit InstrumentedMutex(bool adaptive = false)
      : mutex_(adaptive), stats_(nullptr), clock_(nullptr), stats_code_(0) {}

  explicit InstrumentedMutex(SystemClock* clock, bool adaptive = false)
      : mutex_(adaptive), stats_(nullptr), clock_(clock), stats_code_(0) {}

  InstrumentedMutex(Statistics* stats, SystemClock* clock, int stats_code,
                    bool adaptive = false)
      : mutex_(adaptive),
        stats_(stats),
        clock_(clock),
        stats_code_(stats_code) {}

#ifdef COERCE_CONTEXT_SWITCH
  InstrumentedMutex(Statistics* stats, SystemClock* clock, int stats_code,
                    InstrumentedCondVar* bg_cv, bool adaptive = false)
      : mutex_(adaptive),
        stats_(stats),
        clock_(clock),
        stats_code_(stats_code),
        bg_cv_(bg_cv) {}
#endif

  InstrumentedMutex(const InstrumentedMutex&) = delete;
  InstrumentedMutex& operator=(const InstrumentedMutex&) = delete;
  InstrumentedMutex(InstrumentedMutex&&) = delete;
  InstrumentedMutex& operator=(InstrumentedMutex&&) = delete;
  ~InstrumentedMutex() {
    if (has_slow_timer_) {
      delete slow_timer_;
    }
  }

  void Lock();

  bool TryLock() {
    if (!mutex_.TryLock()) {
      return false;
    }
    if (has_slow_timer_) {
      slow_timer_->Start();
    }
    return true;
  }

  void Unlock(MutexSourceLocation location = MutexSourceLocation::current()) {
    Unlock(location, "unlock");
  }

  void Unlock(const MutexSourceLocation& location, const char* release_kind) {
    const bool tracked = has_slow_timer_;
    if (tracked) {
      slow_timer_->Stop(location, release_kind);
    }
    mutex_.Unlock();
    TEST_SYNC_POINT_CALLBACK("InstrumentedMutex::Unlock:AfterUnlock", this);
    if (tracked) {
      SlowMutexTimer::FlushLogs();
    }
  }

  // First valid configuration wins; call only before the mutex's first use.
  void ConfigureSlowHoldLogging(
      std::shared_ptr<SlowMutexLoggingContext> context, const char* name);

  void AssertHeld() const { mutex_.AssertHeld(); }

 private:
  Statistics* GetStatistics() const {
    return has_slow_timer_ ? slow_timer_->GetStatistics() : stats_;
  }
  void LockInternal();
  friend class InstrumentedCondVar;
  port::Mutex mutex_;
  // The optional timer owns this slot and retains the borrowed Statistics.
  // The tag fits the existing padding, keeping cache-aligned mutexes compact.
  union {
    Statistics* stats_;
    SlowMutexTimer* slow_timer_;
  };
  SystemClock* clock_;
  int stats_code_;
  bool has_slow_timer_ = false;
#ifdef COERCE_CONTEXT_SWITCH
  InstrumentedCondVar* bg_cv_ = nullptr;
#endif
};

class ALIGN_AS(CACHE_LINE_SIZE) CacheAlignedInstrumentedMutex
    : public InstrumentedMutex {
  using InstrumentedMutex::InstrumentedMutex;
};
static_assert(alignof(CacheAlignedInstrumentedMutex) != CACHE_LINE_SIZE ||
              sizeof(CacheAlignedInstrumentedMutex) % CACHE_LINE_SIZE == 0);

// RAII wrapper for InstrumentedMutex
class InstrumentedMutexLock {
 public:
  explicit InstrumentedMutexLock(
      InstrumentedMutex* mutex,
      MutexSourceLocation location = MutexSourceLocation::current())
      : mutex_(mutex), location_(location) {
    mutex_->Lock();
  }

  ~InstrumentedMutexLock() { mutex_->Unlock(location_, "scope_exit"); }

 private:
  InstrumentedMutex* const mutex_;
  MutexSourceLocation location_;
  InstrumentedMutexLock(const InstrumentedMutexLock&) = delete;
  void operator=(const InstrumentedMutexLock&) = delete;
  InstrumentedMutexLock(InstrumentedMutexLock&&) = delete;
  void operator=(InstrumentedMutexLock&&) = delete;
};

// Release the unmeasured outer mutex before emitting nested hold records.
class DeferredInstrumentedMutexLock {
 public:
  DeferredInstrumentedMutexLock(
      InstrumentedMutex* mutex, bool armed,
      MutexSourceLocation location = MutexSourceLocation::current())
      : defer_logs_(armed), mutex_(mutex), location_(location) {
    mutex_->Lock();
  }
  ~DeferredInstrumentedMutexLock() { Release(location_, "scope_exit"); }
  void Unlock(MutexSourceLocation location = MutexSourceLocation::current()) {
    Release(location, "unlock");
  }
  DeferredInstrumentedMutexLock(const DeferredInstrumentedMutexLock&) = delete;
  DeferredInstrumentedMutexLock& operator=(
      const DeferredInstrumentedMutexLock&) = delete;
  DeferredInstrumentedMutexLock(DeferredInstrumentedMutexLock&&) = delete;
  DeferredInstrumentedMutexLock& operator=(DeferredInstrumentedMutexLock&&) =
      delete;

 private:
  void Release(const MutexSourceLocation& location, const char* release_kind) {
    if (mutex_) {
      mutex_->Unlock(location, release_kind);
      mutex_ = nullptr;
      defer_logs_.Release();
    }
  }
  SlowMutexLogScope defer_logs_;
  InstrumentedMutex* mutex_;
  MutexSourceLocation location_;
};

// RAII wrapper for temporary releasing InstrumentedMutex inside
// InstrumentedMutexLock
class InstrumentedMutexUnlock {
 public:
  explicit InstrumentedMutexUnlock(
      InstrumentedMutex* mutex,
      MutexSourceLocation location = MutexSourceLocation::current())
      : mutex_(mutex) {
    mutex_->Unlock(location, "temporary_unlock");
  }

  ~InstrumentedMutexUnlock() { mutex_->Lock(); }

 private:
  InstrumentedMutex* const mutex_;
  InstrumentedMutexUnlock(const InstrumentedMutexUnlock&) = delete;
  void operator=(const InstrumentedMutexUnlock&) = delete;
  InstrumentedMutexUnlock(InstrumentedMutexUnlock&&) = delete;
  void operator=(InstrumentedMutexUnlock&&) = delete;
};

class InstrumentedCondVar {
 public:
  explicit InstrumentedCondVar(InstrumentedMutex* instrumented_mutex)
      : cond_(&(instrumented_mutex->mutex_)),
        instrumented_mutex_(instrumented_mutex),
        clock_(instrumented_mutex->clock_),
        stats_code_(instrumented_mutex->stats_code_) {}

  void Wait(MutexSourceLocation location = MutexSourceLocation::current());

  bool TimedWait(uint64_t abs_time_us,
                 MutexSourceLocation location = MutexSourceLocation::current());

  void Signal() { cond_.Signal(); }

  void SignalAll() { cond_.SignalAll(); }

 private:
  void WaitInternal(const MutexSourceLocation& location);
  bool TimedWaitInternal(uint64_t abs_time_us,
                         const MutexSourceLocation& location);
  port::CondVar cond_;
  InstrumentedMutex* instrumented_mutex_;
  SystemClock* clock_;
  int stats_code_;
};

}  // namespace ROCKSDB_NAMESPACE
