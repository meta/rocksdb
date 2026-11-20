// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <string>
#include <unordered_map>

#include "rocksdb/rocksdb_namespace.h"

// NOTE: in 'main' development branch, this should be the *next*
// minor or major version number planned for release.
#define ROCKSDB_MAJOR 11
#define ROCKSDB_MINOR 15
#define ROCKSDB_PATCH 0

// Make it easy to do conditional compilation based on version checks, i.e.
// #if ROCKSDB_VERSION_GE(4, 5, 6)
// int thisCoderequiresVersion_4_5_6_OrGreater;
// #else
// int thisCodeIsForOlderVersions;
// #endif
#define ROCKSDB_MAKE_VERSION_INT(a, b, c) ((a) * 1000000 + (b) * 1000 + (c))
#define ROCKSDB_VERSION_INT \
  ROCKSDB_MAKE_VERSION_INT(ROCKSDB_MAJOR, ROCKSDB_MINOR, ROCKSDB_PATCH)
#define ROCKSDB_VERSION_GE(a, b, c) \
  (ROCKSDB_VERSION_INT >= ROCKSDB_MAKE_VERSION_INT(a, b, c))

namespace ROCKSDB_NAMESPACE {
// Returns a set of properties indicating how/when/where this version of RocksDB
// was created.
const std::unordered_map<std::string, std::string>& GetRocksBuildProperties();

// Returns the current version of RocksDB as a string (e.g. "6.16.0").
// If with_patch is true, the patch is included (6.16.x).
// Otherwise, only major and minor version is included (6.16)
std::string GetRocksVersionAsString(bool with_patch = true);

// Gets the set of build properties (@see GetRocksBuildProperties) into a
// string. Properties are returned one-per-line, with the first line being:
// "<program> from RocksDB <version>.
// If verbose is true, the full set of properties is
// printed. If verbose is false, only the version information (@see
// GetRocksVersionString) is printed.
std::string GetRocksBuildInfoAsString(const std::string& program,
                                      bool verbose = false);
}  // namespace ROCKSDB_NAMESPACE
