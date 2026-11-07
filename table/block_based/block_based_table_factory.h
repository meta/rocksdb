//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "cache/cache_reservation_manager.h"
#include "port/port.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/table.h"

namespace ROCKSDB_NAMESPACE {
struct ColumnFamilyOptions;
struct ConfigOptions;
struct DBOptions;
struct EnvOptions;

class BlockBasedTableBuilder;
class RandomAccessFileReader;
class WritableFileWriter;

// TODO: deprecate this class as it can be replaced with
// `FileMetaData::tail_size`
//
// A class used to track actual bytes written from the tail in the recent SST
// file opens, and provide a suggestion for following open.
class TailPrefetchStats {
 public:
  void RecordEffectiveSize(size_t len);
  // 0 indicates no information to determine.
  size_t GetSuggestedPrefetchSize();

 private:
  const static size_t kNumTracked = 32;
  size_t records_[kNumTracked];
  port::Mutex mutex_;
  size_t next_ = 0;
  size_t num_records_ = 0;
};

Status ValidateIndexMode(BlockBasedTableOptions::IndexMode index_mode);

class BlockBasedTableFactory : public TableFactory {
 public:
  explicit BlockBasedTableFactory(
      const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

  // Method to allow CheckedCast to work for this class
  static const char* kClassName() { return kBlockBasedTableName(); }

  const char* Name() const override { return kBlockBasedTableName(); }

  Status FileIsValidTable(const IOOptions& io_options, FSRandomAccessFile& file,
                          uint64_t file_size, bool* matches) const override;

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override;

  // Valdates the specified DB Options.
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override;
  Status PrepareOptions(const ConfigOptions& opts) override;

  std::string GetPrintableOptions() const override;

  bool IsDeleteRangeSupported() const override { return true; }

  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<BlockBasedTableFactory>(*this);
  }

  TailPrefetchStats* tail_prefetch_stats() {
    return &shared_state_->tail_prefetch_stats;
  }

 protected:
  Status ConfigureOptions(
      const ConfigOptions& config_options,
      const std::unordered_map<std::string, std::string>& opts_map,
      std::unordered_map<std::string, std::string>* unused) override;
  const void* GetOptionsPtr(const std::string& name) const override;
  Status ParseOption(const ConfigOptions& config_options,
                     const OptionTypeInfo& opt_info,
                     const std::string& opt_name, const std::string& opt_value,
                     void* opt_ptr) override;
  void InitializeOptions();

 private:
  void UpdateIndexMode();

  BlockBasedTableOptions table_options_;
  bool index_mode_explicit_;
  bool skip_standard_index_ = false;
  // Share some state among cloned instances
  struct SharedState {
    std::shared_ptr<CacheReservationManager> table_reader_cache_res_mgr;
    TailPrefetchStats tail_prefetch_stats;
  };
  std::shared_ptr<SharedState> shared_state_;
};

extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;
extern const std::string kPropTrue;
extern const std::string kPropFalse;
}  // namespace ROCKSDB_NAMESPACE
