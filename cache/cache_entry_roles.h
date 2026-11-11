//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <array>
#include <cstdint>

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {

extern const std::array<std::string, kNumCacheEntryRoles>
    kCacheEntryRoleToCamelString;
extern const std::array<std::string, kNumCacheEntryRoles>
    kCacheEntryRoleToHyphenString;

}  // namespace ROCKSDB_NAMESPACE
