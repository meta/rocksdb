//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "db/version_edit.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/io_status.h"
#include "rocksdb/table.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/kv_map.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace mock {
using KVPair = std::pair<std::string, std::string>;
using KVVector = std::vector<KVPair>;

KVVector MakeMockFile(std::initializer_list<KVPair> l = {});
void SortKVVector(KVVector* kv_vector,
                  const Comparator* ucmp = BytewiseComparator());

struct MockTableFileSystem {
  port::Mutex mutex;
  std::map<uint32_t, KVVector> files;
};

class MockTableFactory : public TableFactory {
 public:
  enum MockCorruptionMode {
    kCorruptNone,
    kCorruptKey,
    kCorruptValue,
    kCorruptReorderKey,
  };

  MockTableFactory();
  static const char* kClassName() { return "MockTable"; }
  const char* Name() const override { return kClassName(); }
  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const override;
  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override;

  // This function will directly create mock table instead of going through
  // MockTableBuilder. file_contents has to have a format of <internal_key,
  // value>. Those key-value pairs will then be inserted into the mock table.
  Status CreateMockTable(Env* env, const std::string& fname,
                         KVVector file_contents);

  std::string GetPrintableOptions() const override { return std::string(); }

  void SetCorruptionMode(MockCorruptionMode mode) { corrupt_mode_ = mode; }

  void SetKeyValueSize(size_t size) { key_value_size_ = size; }
  // This function will assert that only a single file exists and that the
  // contents are equal to file_contents
  void AssertSingleFile(const KVVector& file_contents);
  void AssertLatestFiles(const std::vector<KVVector>& files_contents);
  std::unique_ptr<TableFactory> Clone() const override {
    return nullptr;  // Not implemented
  }

 private:
  Status GetAndWriteNextID(WritableFileWriter* file, uint32_t* id) const;
  Status GetIDFromFile(RandomAccessFileReader* file, uint32_t* id) const;

  mutable MockTableFileSystem file_system_;
  mutable std::atomic<uint32_t> next_id_;
  MockCorruptionMode corrupt_mode_;

  size_t key_value_size_ = 1;
};

class MockTableReader : public TableReader {
 public:
  explicit MockTableReader(const mock::KVVector& table) : table_(table) {
    tp_.num_entries = table_.size();
    tp_.num_range_deletions = 0;
    tp_.raw_key_size = 1;
    tp_.raw_value_size = 1;
  }
  explicit MockTableReader(const mock::KVVector& table,
                           const TableProperties& tp)
      : table_(table), tp_(tp) {}

  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* prefix_extractor,
                                Arena* arena, bool skip_filters,
                                TableReaderCaller caller,
                                size_t compaction_readahead_size = 0,
                                bool allow_unprepared_value = false) override;

  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters = false) override;

  uint64_t ApproximateOffsetOf(const ReadOptions& /*read_options*/,
                               const Slice& /*key*/,
                               TableReaderCaller /*caller*/) override {
    return 0;
  }

  uint64_t ApproximateSize(const ReadOptions& /*read_options*/,
                           const Slice& /*start*/, const Slice& /*end*/,
                           TableReaderCaller /*caller*/) override {
    return 0;
  }

  size_t ApproximateMemoryUsage() const override { return 0; }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return std::make_shared<const TableProperties>(tp_);
  }

  ~MockTableReader() override = default;

 private:
  const KVVector& table_;
  TableProperties tp_;
};
}  // namespace mock
}  // namespace ROCKSDB_NAMESPACE
