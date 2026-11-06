// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/plain/plain_table_factory.h"

#include <cstdint>
#include <memory>

#include "db/dbformat.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/utilities/customizable_util.h"
#include "rocksdb/utilities/object_registry.h"
#include "rocksdb/utilities/options_type.h"
#include "table/plain/plain_table_builder.h"
#include "table/plain/plain_table_reader.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
static std::unordered_map<std::string, OptionTypeInfo> plain_table_type_info = {
    {"user_key_len",
     {offsetof(struct PlainTableOptions, user_key_len), OptionType::kUInt32T,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"bloom_bits_per_key",
     {offsetof(struct PlainTableOptions, bloom_bits_per_key), OptionType::kInt,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"hash_table_ratio",
     {offsetof(struct PlainTableOptions, hash_table_ratio), OptionType::kDouble,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"index_sparseness",
     {offsetof(struct PlainTableOptions, index_sparseness), OptionType::kSizeT,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"huge_page_tlb_size",
     {offsetof(struct PlainTableOptions, huge_page_tlb_size),
      OptionType::kSizeT, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"encoding_type",
     {offsetof(struct PlainTableOptions, encoding_type),
      OptionType::kEncodingType, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
    {"full_scan_mode",
     {offsetof(struct PlainTableOptions, full_scan_mode), OptionType::kBoolean,
      OptionVerificationType::kNormal, OptionTypeFlags::kNone}},
    {"store_index_in_file",
     {offsetof(struct PlainTableOptions, store_index_in_file),
      OptionType::kBoolean, OptionVerificationType::kNormal,
      OptionTypeFlags::kNone}},
};

PlainTableFactory::PlainTableFactory(const PlainTableOptions& options)
    : table_options_(options) {
  RegisterOptions(&table_options_, &plain_table_type_info);
}

Status PlainTableFactory::NewTableReader(
    const ReadOptions& /*ro*/, const TableReaderOptions& table_reader_options,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  return PlainTableReader::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_reader_options.internal_comparator, std::move(file), file_size,
      table, table_options_.bloom_bits_per_key, table_options_.hash_table_ratio,
      table_options_.index_sparseness, table_options_.huge_page_tlb_size,
      table_options_.full_scan_mode, table_reader_options.immortal,
      table_reader_options.prefix_extractor.get());
}

TableBuilder* PlainTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options,
    WritableFileWriter* file) const {
  // Ignore the skip_filters flag. PlainTable format is optimized for small
  // in-memory dbs. The skip_filters optimization is not useful for plain
  // tables
  //
  return new PlainTableBuilder(
      table_builder_options.ioptions, table_builder_options.moptions,
      table_builder_options.internal_tbl_prop_coll_factories,
      table_builder_options.column_family_id,
      table_builder_options.level_at_creation, file,
      table_options_.user_key_len, table_options_.encoding_type,
      table_options_.index_sparseness, table_options_.bloom_bits_per_key,
      table_builder_options.column_family_name, 6,
      table_options_.huge_page_tlb_size, table_options_.hash_table_ratio,
      table_options_.store_index_in_file, table_builder_options.db_id,
      table_builder_options.db_session_id, table_builder_options.cur_file_num);
}

std::string PlainTableFactory::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  user_key_len: %u\n",
           table_options_.user_key_len);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  bloom_bits_per_key: %d\n",
           table_options_.bloom_bits_per_key);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  hash_table_ratio: %lf\n",
           table_options_.hash_table_ratio);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_sparseness: %" ROCKSDB_PRIszt "\n",
           table_options_.index_sparseness);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  huge_page_tlb_size: %" ROCKSDB_PRIszt "\n",
           table_options_.huge_page_tlb_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  encoding_type: %d\n",
           table_options_.encoding_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  full_scan_mode: %d\n",
           table_options_.full_scan_mode);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  store_index_in_file: %d\n",
           table_options_.store_index_in_file);
  ret.append(buffer);
  return ret;
}

Status GetPlainTableOptionsFromString(const ConfigOptions& config_options,
                                      const PlainTableOptions& table_options,
                                      const std::string& opts_str,
                                      PlainTableOptions* new_table_options) {
  std::unordered_map<std::string, std::string> opts_map;
  Status s = StringToMap(opts_str, &opts_map);
  if (!s.ok()) {
    return s;
  }

  s = GetPlainTableOptionsFromMap(config_options, table_options, opts_map,
                                  new_table_options);
  // Translate any errors (NotFound, NotSupported, to InvalidArgument
  if (s.ok() || s.IsInvalidArgument()) {
    return s;
  } else {
    return Status::InvalidArgument(s.getState());
  }
}

static int RegisterBuiltinMemTableRepFactory(ObjectLibrary& library,
                                             const std::string& /*arg*/) {
  // The MemTableRepFactory built-in classes will be either a class
  // (VectorRepFactory) or a nickname (vector), followed optionally by ":#",
  // where # is the "size" of the factory.
  auto AsPattern = [](const std::string& name, const std::string& alt) {
    auto pattern = ObjectLibrary::PatternEntry(name, true);
    pattern.AnotherName(alt);
    pattern.AddNumber(":");
    return pattern;
  };
  library.AddFactory<MemTableRepFactory>(
      AsPattern(VectorRepFactory::kClassName(), VectorRepFactory::kNickName()),
      [](const std::string& uri, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /*errmsg*/) {
        auto colon = uri.find(':');
        if (colon != std::string::npos) {
          size_t count = ParseSizeT(uri.substr(colon + 1));
          guard->reset(new VectorRepFactory(count));
        } else {
          guard->reset(new VectorRepFactory());
        }
        return guard->get();
      });
  library.AddFactory<MemTableRepFactory>(
      AsPattern(SkipListFactory::kClassName(), SkipListFactory::kNickName()),
      [](const std::string& uri, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /*errmsg*/) {
        auto colon = uri.find(':');
        if (colon != std::string::npos) {
          size_t lookahead = ParseSizeT(uri.substr(colon + 1));
          guard->reset(new SkipListFactory(lookahead));
        } else {
          guard->reset(new SkipListFactory());
        }
        return guard->get();
      });
  library.AddFactory<MemTableRepFactory>(
      AsPattern("HashLinkListRepFactory", "hash_linkedlist"),
      [](const std::string& uri, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /*errmsg*/) {
        // Expecting format: hash_linkedlist:<hash_bucket_count>
        auto colon = uri.find(':');
        if (colon != std::string::npos) {
          size_t hash_bucket_count = ParseSizeT(uri.substr(colon + 1));
          guard->reset(NewHashLinkListRepFactory(hash_bucket_count));
        } else {
          guard->reset(NewHashLinkListRepFactory());
        }
        return guard->get();
      });
  library.AddFactory<MemTableRepFactory>(
      AsPattern("HashSortedVectorRepFactory", "hash_sv"),
      [](const std::string& uri, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /*errmsg*/) {
        auto colon = uri.find(":");
        if (colon != std::string::npos) {
          size_t hash_bucket_count = ParseSizeT(uri.substr(colon + 1));
          guard->reset(NewHashSortedVectorRepFactory(hash_bucket_count));
        } else {
          guard->reset(NewHashSortedVectorRepFactory());
        }
        return guard->get();
      });
  library.AddFactory<MemTableRepFactory>(
      AsPattern("HashSkipListRepFactory", "prefix_hash"),
      [](const std::string& uri, std::unique_ptr<MemTableRepFactory>* guard,
         std::string* /*errmsg*/) {
        // Expecting format: prefix_hash:<hash_bucket_count>
        auto colon = uri.find(':');
        if (colon != std::string::npos) {
          size_t hash_bucket_count = ParseSizeT(uri.substr(colon + 1));
          guard->reset(NewHashSkipListRepFactory(hash_bucket_count));
        } else {
          guard->reset(NewHashSkipListRepFactory());
        }
        return guard->get();
      });
  library.AddFactory<MemTableRepFactory>(
      "cuckoo",
      [](const std::string& /*uri*/,
         std::unique_ptr<MemTableRepFactory>* /*guard*/, std::string* errmsg) {
        *errmsg = "cuckoo hash memtable is not supported anymore.";
        return nullptr;
      });

  size_t num_types;
  return static_cast<int>(library.GetFactoryCount(&num_types));
}

Status GetMemTableRepFactoryFromString(
    const std::string& opts_str, std::unique_ptr<MemTableRepFactory>* result) {
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;
  config_options.ignore_unknown_options = false;
  return MemTableRepFactory::CreateFromString(config_options, opts_str, result);
}

Status MemTableRepFactory::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::unique_ptr<MemTableRepFactory>* result) {
  static std::once_flag once;
  std::call_once(once, [&]() {
    RegisterBuiltinMemTableRepFactory(*(ObjectLibrary::Default().get()), "");
  });
  std::string id;
  std::unordered_map<std::string, std::string> opt_map;
  Status status = Customizable::GetOptionsMap(config_options, result->get(),
                                              value, &id, &opt_map);
  if (!status.ok()) {  // GetOptionsMap failed
    return status;
  } else if (value.empty()) {
    // No Id and no options.  Clear the object
    result->reset();
    return Status::OK();
  } else if (id.empty()) {  // We have no Id but have options.  Not good
    return Status::NotSupported("Cannot reset object ", id);
  } else {
    status = NewUniqueObject<MemTableRepFactory>(config_options, id, opt_map,
                                                 result);
  }
  return status;
}

Status MemTableRepFactory::CreateFromString(
    const ConfigOptions& config_options, const std::string& value,
    std::shared_ptr<MemTableRepFactory>* result) {
  std::unique_ptr<MemTableRepFactory> factory;
  Status s = CreateFromString(config_options, value, &factory);
  if (factory && s.ok()) {
    result->reset(factory.release());
  }
  return s;
}

Status GetPlainTableOptionsFromMap(
    const ConfigOptions& config_options, const PlainTableOptions& table_options,
    const std::unordered_map<std::string, std::string>& opts_map,
    PlainTableOptions* new_table_options) {
  assert(new_table_options);
  PlainTableFactory ptf(table_options);
  Status s = ptf.ConfigureFromMap(config_options, opts_map);
  if (s.ok()) {
    *new_table_options = *(ptf.GetOptions<PlainTableOptions>());
  } else {
    // Restore "new_options" to the default "base_options".
    *new_table_options = table_options;
  }
  return s;
}

TableFactory* NewPlainTableFactory(const PlainTableOptions& options) {
  return new PlainTableFactory(options);
}

const std::string PlainTablePropertyNames::kEncodingType =
    "rocksdb.plain.table.encoding.type";

const std::string PlainTablePropertyNames::kBloomVersion =
    "rocksdb.plain.table.bloom.version";

const std::string PlainTablePropertyNames::kNumBloomBlocks =
    "rocksdb.plain.table.bloom.numblocks";

}  // namespace ROCKSDB_NAMESPACE
