// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/adaptive/adaptive_table_factory.h"

#include "port/port.h"
#include "table/format.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

AdaptiveTableFactory::AdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> plain_table_factory,
    std::shared_ptr<TableFactory> cuckoo_table_factory)
    : table_factory_to_write_(table_factory_to_write),
      block_based_table_factory_(block_based_table_factory),
      plain_table_factory_(plain_table_factory),
      cuckoo_table_factory_(cuckoo_table_factory) {
  if (!plain_table_factory_) {
    plain_table_factory_.reset(NewPlainTableFactory());
  }
  if (!block_based_table_factory_) {
    block_based_table_factory_.reset(NewBlockBasedTableFactory());
  }
  if (!cuckoo_table_factory_) {
    cuckoo_table_factory_.reset(NewCuckooTableFactory());
  }
  if (!table_factory_to_write_) {
    table_factory_to_write_ = block_based_table_factory_;
  }
}

Status AdaptiveTableFactory::FileIsValidTable(const IOOptions& io_options,
                                              FSRandomAccessFile& file,
                                              uint64_t file_size,
                                              bool* matches) const {
  uint64_t magic_number;
  Status status =
      ReadTableMagicNumber(io_options, file, file_size, &magic_number);
  *matches = status.ok() && (magic_number == kBlockBasedTableMagicNumber ||
                             magic_number == kPlainTableMagicNumber ||
                             magic_number == kLegacyPlainTableMagicNumber ||
                             magic_number == kCuckooTableMagicNumber);
  return status;
}

Status AdaptiveTableFactory::NewTableReader(
    const ReadOptions& ro, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool prefetch_index_and_filter_in_cache) const {
  Footer footer;
  IOOptions opts;
  auto s =
      ReadFooterFromFile(opts, file.get(), *table_reader_options.ioptions.fs,
                         nullptr /* prefetch_buffer */, file_size, &footer);
  if (!s.ok()) {
    return s;
  }
  if (footer.table_magic_number() == kPlainTableMagicNumber ||
      footer.table_magic_number() == kLegacyPlainTableMagicNumber) {
    return plain_table_factory_->NewTableReader(
        table_reader_options, std::move(file), file_size, table);
  } else if (footer.table_magic_number() == kBlockBasedTableMagicNumber) {
    return block_based_table_factory_->NewTableReader(
        ro, table_reader_options, std::move(file), file_size, table,
        prefetch_index_and_filter_in_cache);
  } else if (footer.table_magic_number() == kCuckooTableMagicNumber) {
    return cuckoo_table_factory_->NewTableReader(
        table_reader_options, std::move(file), file_size, table);
  } else {
    return Status::NotSupported("Unidentified table format");
  }
}

TableBuilder* AdaptiveTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options,
    WritableFileWriter* file) const {
  return table_factory_to_write_->NewTableBuilder(table_builder_options, file);
}

std::string AdaptiveTableFactory::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  if (table_factory_to_write_) {
    snprintf(buffer, kBufferSize, "  write factory (%s) options:\n%s\n",
             (table_factory_to_write_->Name() ? table_factory_to_write_->Name()
                                              : ""),
             table_factory_to_write_->GetPrintableOptions().c_str());
    ret.append(buffer);
  }
  if (plain_table_factory_) {
    snprintf(buffer, kBufferSize, "  %s options:\n%s\n",
             plain_table_factory_->Name() ? plain_table_factory_->Name() : "",
             plain_table_factory_->GetPrintableOptions().c_str());
    ret.append(buffer);
  }
  if (block_based_table_factory_) {
    snprintf(
        buffer, kBufferSize, "  %s options:\n%s\n",
        (block_based_table_factory_->Name() ? block_based_table_factory_->Name()
                                            : ""),
        block_based_table_factory_->GetPrintableOptions().c_str());
    ret.append(buffer);
  }
  if (cuckoo_table_factory_) {
    snprintf(buffer, kBufferSize, "  %s options:\n%s\n",
             cuckoo_table_factory_->Name() ? cuckoo_table_factory_->Name() : "",
             cuckoo_table_factory_->GetPrintableOptions().c_str());
    ret.append(buffer);
  }
  return ret;
}

TableFactory* NewAdaptiveTableFactory(
    std::shared_ptr<TableFactory> table_factory_to_write,
    std::shared_ptr<TableFactory> block_based_table_factory,
    std::shared_ptr<TableFactory> plain_table_factory,
    std::shared_ptr<TableFactory> cuckoo_table_factory) {
  return new AdaptiveTableFactory(table_factory_to_write,
                                  block_based_table_factory,
                                  plain_table_factory, cuckoo_table_factory);
}

}  // namespace ROCKSDB_NAMESPACE
