//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// This file includes the common definitions used in the port/,
// the public API (this directory), and other directories

#pragma once

#include "rocksdb/rocksdb_namespace.h"

// Add symbol visibiity macros for shared library exports
//
// Introduce ROCKSDB_API macro to control which symbols get exported in shared
// libs. Helps keep the ABI cleaner by hiding internal stuff.
//
#ifndef ROCKSDB_API
#if defined(ROCKSDB_DLL)
#if defined(_WIN32)
#if defined(ROCKSDB_LIBRARY_EXPORTS)
#define ROCKSDB_API __declspec(dllexport)
#else
#define ROCKSDB_API __declspec(dllimport)
#endif
#else
#if defined(ROCKSDB_LIBRARY_EXPORTS)
#define ROCKSDB_API __attribute__((visibility("default")))
#else
#define ROCKSDB_API
#endif
#endif
#else
#define ROCKSDB_API
#endif
#endif

namespace ROCKSDB_NAMESPACE {

namespace port {
class CondVar;
}

enum class CpuPriority {
  kIdle = 0,
  kLow = 1,
  kNormal = 2,
  kHigh = 3,
};

}  // namespace ROCKSDB_NAMESPACE
