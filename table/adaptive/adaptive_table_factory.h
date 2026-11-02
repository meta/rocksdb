// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <string>

#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {

struct EnvOptions;

class Status;
class RandomAccessFile;
class WritableFile;
class Table;
class TableBuilder;

// Shared capability/options query for a direct or adaptive block-based writer.
// A null result means that newly written tables cannot carry Blob GC metadata.
const BlockBasedTableOptions* GetBlockBasedTableWriterOptions(
    const TableFactory* table_factory);

class AdaptiveTableFactory : public TableFactory {
 public:
  // Method to allow CheckedCast to work for this class.
  static const char* kClassName() { return "AdaptiveTableFactory"; }

  explicit AdaptiveTableFactory(
      std::shared_ptr<TableFactory> table_factory_to_write,
      std::shared_ptr<TableFactory> block_based_table_factory,
      std::shared_ptr<TableFactory> plain_table_factory,
      std::shared_ptr<TableFactory> cuckoo_table_factory);

  const char* Name() const override { return "AdaptiveTableFactory"; }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table,
      bool prefetch_index_and_filter_in_cache = true) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override;

  std::string GetPrintableOptions() const override;

  // Returns the block-based options used for newly written tables, or nullptr
  // when the configured write factory uses another table format.
  const BlockBasedTableOptions* GetBlockBasedTableWriterOptions() const;

  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<AdaptiveTableFactory>(*this);
  }

 private:
  std::shared_ptr<TableFactory> table_factory_to_write_;
  std::shared_ptr<TableFactory> block_based_table_factory_;
  std::shared_ptr<TableFactory> plain_table_factory_;
  std::shared_ptr<TableFactory> cuckoo_table_factory_;
};

}  // namespace ROCKSDB_NAMESPACE
