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

// Returns the BlockBasedTableOptions that `table_factory` uses to write new
// tables: the factory's own options for a BlockBasedTableFactory, or the
// options of its write factory for an AdaptiveTableFactory. Returns nullptr
// when `table_factory` is null or writes another table format. The column
// family passes the result to BlobFileCache as the relocation table options;
// with nullptr, relocation file reads return NotSupported.
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

  const char* Name() const override { return kClassName(); }

  Status FileIsValidTable(const IOOptions& io_options, FSRandomAccessFile& file,
                          uint64_t file_size, bool* matches) const override;

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
