// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <mutex>

#include "file/random_access_file_reader.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/cuckoo/cuckoo_table_factory.h"
#include "table/plain/plain_table_factory.h"

namespace ROCKSDB_NAMESPACE {

FSRandomAccessFile* AsFSRandomAccessFile(RandomAccessFileReader* reader) {
  return reader;
}

static void RegisterTableFactories(const std::string& /*arg*/) {
  static std::once_flag loaded;
  std::call_once(loaded, []() {
    auto library = ObjectLibrary::Default();
    library->AddFactory<TableFactory>(
        TableFactory::kBlockBasedTableName(),
        [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
           std::string* /* errmsg */) {
          guard->reset(new BlockBasedTableFactory());
          return guard->get();
        });
    library->AddFactory<TableFactory>(
        TableFactory::kPlainTableName(),
        [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
           std::string* /* errmsg */) {
          guard->reset(new PlainTableFactory());
          return guard->get();
        });
    library->AddFactory<TableFactory>(
        TableFactory::kCuckooTableName(),
        [](const std::string& /*uri*/, std::unique_ptr<TableFactory>* guard,
           std::string* /* errmsg */) {
          guard->reset(new CuckooTableFactory());
          return guard->get();
        });
  });
}

Status TableFactory::CreateFromString(const ConfigOptions& config_options,
                                      const std::string& value,
                                      std::shared_ptr<TableFactory>* factory) {
  RegisterTableFactories("");
  return LoadSharedObject<TableFactory>(config_options, value, factory);
}
}  // namespace ROCKSDB_NAMESPACE
