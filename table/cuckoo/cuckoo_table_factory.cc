// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/cuckoo/cuckoo_table_factory.h"

#include "db/dbformat.h"
#include "options/configurable_helper.h"
#include "rocksdb/utilities/options_type.h"
#include "table/cuckoo/cuckoo_table_builder.h"
#include "table/cuckoo/cuckoo_table_reader.h"
#include "table/format.h"

namespace ROCKSDB_NAMESPACE {

Status CuckooTableFactory::FileIsValidTable(const IOOptions& io_options,
                                            FSRandomAccessFile& file,
                                            uint64_t file_size,
                                            bool* matches) const {
  uint64_t magic_number;
  Status status =
      ReadTableMagicNumber(io_options, file, file_size, &magic_number);
  *matches = status.ok() && magic_number == kCuckooTableMagicNumber;
  return status;
}

Status CuckooTableFactory::NewTableReader(
    const ReadOptions& /*ro*/, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  std::unique_ptr<CuckooTableReader> new_reader(new CuckooTableReader(
      table_reader_options.ioptions, std::move(file), file_size,
      table_reader_options.internal_comparator.user_comparator(), nullptr));
  Status s = new_reader->status();
  if (s.ok()) {
    *table = std::move(new_reader);
  }
  return s;
}

TableBuilder* CuckooTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options,
    WritableFileWriter* file) const {
  // TODO: change builder to take the option struct
  return new CuckooTableBuilder(
      file, table_options_.hash_table_ratio, 64,
      table_options_.max_search_depth,
      table_builder_options.internal_comparator.user_comparator(),
      table_options_.cuckoo_block_size, table_options_.use_module_hash,
      table_options_.identity_as_first_hash, nullptr /* get_slice_hash */,
      table_builder_options.column_family_id,
      table_builder_options.column_family_name, table_builder_options.db_id,
      table_builder_options.db_session_id, table_builder_options.cur_file_num);
}

std::string CuckooTableFactory::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(2000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  hash_table_ratio: %lf\n",
           table_options_.hash_table_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  max_search_depth: %u\n",
           table_options_.max_search_depth);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  cuckoo_block_size: %u\n",
           table_options_.cuckoo_block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  identity_as_first_hash: %d\n",
           table_options_.identity_as_first_hash);
  ret.append(buffer);
  return ret;
}

static std::unordered_map<std::string, OptionTypeInfo> cuckoo_table_type_info =
    {
        {"hash_table_ratio",
         {offsetof(struct CuckooTableOptions, hash_table_ratio),
          OptionType::kDouble, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"max_search_depth",
         {offsetof(struct CuckooTableOptions, max_search_depth),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"cuckoo_block_size",
         {offsetof(struct CuckooTableOptions, cuckoo_block_size),
          OptionType::kUInt32T, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"identity_as_first_hash",
         {offsetof(struct CuckooTableOptions, identity_as_first_hash),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
        {"use_module_hash",
         {offsetof(struct CuckooTableOptions, use_module_hash),
          OptionType::kBoolean, OptionVerificationType::kNormal,
          OptionTypeFlags::kNone}},
};

CuckooTableFactory::CuckooTableFactory(const CuckooTableOptions& table_options)
    : table_options_(table_options) {
  RegisterOptions(&table_options_, &cuckoo_table_type_info);
}

TableFactory* NewCuckooTableFactory(const CuckooTableOptions& table_options) {
  return new CuckooTableFactory(table_options);
}

}  // namespace ROCKSDB_NAMESPACE
