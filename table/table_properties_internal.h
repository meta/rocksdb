//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// Prepared metadata omits referenced_blob_bytes because existing consumers
// reject unknown serialized fields. The omission can be removed once every
// prepared-metadata consumer tolerates unknown fields.
Status SerializeTablePropertiesForPreparedFileInfo(const ConfigOptions& opts,
                                                   const TableProperties& props,
                                                   std::string* output);

#ifndef NDEBUG
void TEST_SetRandomTableProperties(TableProperties* props);
#endif
}  // namespace ROCKSDB_NAMESPACE
