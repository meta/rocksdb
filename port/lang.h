// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef FALLTHROUGH_INTENDED
#if defined(__clang__)
#define FALLTHROUGH_INTENDED [[clang::fallthrough]]
#elif defined(__GNUC__) && __GNUC__ >= 7
#define FALLTHROUGH_INTENDED [[gnu::fallthrough]]
#else
#define FALLTHROUGH_INTENDED \
  do {                       \
  } while (0)
#endif
#endif

#define DECLARE_DEFAULT_MOVES(Name) \
  Name(Name&&) noexcept = default;  \
  Name& operator=(Name&&) = default

// ASAN (Address sanitizer)

#if defined(__clang__)
#if defined(__has_feature)
#if __has_feature(address_sanitizer)
#define MUST_FREE_HEAP_ALLOCATIONS 1
#endif  // __has_feature(address_sanitizer)
#endif  // defined(__has_feature)
#else   // __clang__
#ifdef __SANITIZE_ADDRESS__
#define MUST_FREE_HEAP_ALLOCATIONS 1
#endif  // __SANITIZE_ADDRESS__
#endif  // __clang__

#ifdef ROCKSDB_VALGRIND_RUN
#define MUST_FREE_HEAP_ALLOCATIONS 1
#endif  // ROCKSDB_VALGRIND_RUN

// Coding guidelines say to avoid static objects with non-trivial destructors,
// because it's easy to cause trouble (UB) in static destruction. This
// macro makes it easier to define static objects that are normally never
// destructed, except are destructed when running under ASAN. This should
// avoid unexpected, unnecessary destruction behavior in production.
// Note that constructor arguments can be provided as in
//   STATIC_AVOID_DESTRUCTION(Foo, foo)(arg1, arg2);
#ifdef MUST_FREE_HEAP_ALLOCATIONS
#define STATIC_AVOID_DESTRUCTION(Type, name) static Type name
constexpr bool kMustFreeHeapAllocations = true;
#else
#define STATIC_AVOID_DESTRUCTION(Type, name) static Type& name = *new Type
constexpr bool kMustFreeHeapAllocations = false;
#endif

// TSAN (Thread sanitizer)

// For simplicity, standardize on the GCC define
#if defined(__clang__)
#if defined(__has_feature) && __has_feature(thread_sanitizer)
#define __SANITIZE_THREAD__ 1
#endif  // __has_feature(thread_sanitizer)
#endif  // __clang__

#ifdef __SANITIZE_THREAD__
#define TSAN_SUPPRESSION __attribute__((no_sanitize("thread")))
#else
#define TSAN_SUPPRESSION
#endif  // TSAN_SUPPRESSION

// Fail in debug build with a useful message, for automatically grouping
// related failures
#define DEBUG_FAIL(msg) assert(false && msg)

// Compile-time CPU feature testing compatibility
//
// A way to be extra sure these defines have been included.
#define ASSERT_FEATURE_COMPAT_HEADER() \
  static_assert(true, "Semicolon required") /* empty */

// MSVC doesn't support the same defines that gcc and clang provide
// but does some like __AVX__. Here we can infer some features from others.
#ifdef __AVX__ && (_MSC_VER)
#define __SSE4_2__ 1
#define __PCLMUL__ 1
#endif  // __AVX__

// A way to disable PCLMUL
#ifdef NO_PCLMUL
#undef __PCLMUL__
#endif

// popcnt is generally implied by SSE4.2
#if defined(__SSE4_2__)
#define __POPCNT__ 1
#endif

// A way to disable POPCNT
#ifdef NO_POPCNT
#undef __POPCNT__
#endif
