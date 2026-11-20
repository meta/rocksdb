// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "util/murmurhash.h"

namespace ROCKSDB_NAMESPACE {

const uint32_t kCuckooMurmurSeedMultiplier = 816922183;
static inline uint64_t CuckooHash(
    const Slice& user_key, uint32_t hash_cnt, bool use_module_hash,
    uint64_t table_size_, bool identity_as_first_hash,
    uint64_t (*get_slice_hash)(const Slice&, uint32_t, uint64_t)) {
#if !defined NDEBUG || defined OS_WIN
  // This part is used only in unit tests but we have to keep it for Windows
  // build as we run test in both debug and release modes under Windows.
  if (get_slice_hash != nullptr) {
    return get_slice_hash(user_key, hash_cnt, table_size_);
  }
#else
  (void)get_slice_hash;
#endif

  uint64_t value = 0;
  if (hash_cnt == 0 && identity_as_first_hash) {
    value = (*reinterpret_cast<const int64_t*>(user_key.data()));
  } else {
    value = MurmurHash(user_key.data(), static_cast<int>(user_key.size()),
                       kCuckooMurmurSeedMultiplier * hash_cnt);
  }
  if (use_module_hash) {
    return value % table_size_;
  } else {
    return value & (table_size_ - 1);
  }
}

// Cuckoo Table is designed for applications that require fast point lookups
// but not fast range scans.
//
// Some assumptions:
// - Key length and Value length are fixed.
// - Does not support Snapshot.
// - Does not support Merge operations.
// - Does not support prefix bloom filters.
class CuckooTableFactory : public TableFactory {
 public:
  explicit CuckooTableFactory(
      const CuckooTableOptions& table_option = CuckooTableOptions());

  // Method to allow CheckedCast to work for this class
  static const char* kClassName() { return kCuckooTableName(); }
  const char* Name() const override { return kCuckooTableName(); }

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

  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<CuckooTableFactory>(*this);
  }

 private:
  CuckooTableOptions table_options_;
};

}  // namespace ROCKSDB_NAMESPACE
