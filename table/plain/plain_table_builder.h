//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <string>
#include <vector>

#include "db/version_edit.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "table/plain/plain_table_bloom.h"
#include "table/plain/plain_table_index.h"
#include "table/plain/plain_table_key_coding.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

class BlockBuilder;
class BlockHandle;
class WritableFile;
class TableBuilder;

// The builder class of PlainTable. For description of PlainTable format
// See comments of class PlainTableFactory, where instances of
// PlainTableReader are created.
class PlainTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table it is
  // building in *file.  Does not close the file.  It is up to the
  // caller to close the file after calling Finish(). The output file
  // will be part of level specified by 'level'.  A value of -1 means
  // that the caller does not know which level the output file will reside.
  PlainTableBuilder(
      const ImmutableOptions& ioptions, const MutableCFOptions& moptions,
      const InternalTblPropCollFactories* internal_tbl_prop_coll_factories,
      uint32_t column_family_id, int level_at_creation,
      WritableFileWriter* file, uint32_t user_key_size,
      EncodingType encoding_type, size_t index_sparseness,
      uint32_t bloom_bits_per_key, const std::string& column_family_name,
      uint32_t num_probes = 6, size_t huge_page_tlb_size = 0,
      double hash_table_ratio = 0, bool store_index_in_file = false,
      const std::string& db_id = "", const std::string& db_session_id = "",
      uint64_t file_number = 0);
  // No copying allowed
  PlainTableBuilder(const PlainTableBuilder&) = delete;
  void operator=(const PlainTableBuilder&) = delete;
  PlainTableBuilder(PlainTableBuilder&&) = delete;
  void operator=(PlainTableBuilder&&) = delete;

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~PlainTableBuilder() override;

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  void Add(const Slice& key, const Slice& value) override;

  // Return non-ok iff some error has been detected.
  Status status() const override { return status_; }

  // Return non-ok iff some error happens during IO.
  IOStatus io_status() const override { return io_status_; }

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Status Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  void Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  TableProperties GetTableProperties() const override { return properties_; }

  bool SaveIndexInFile() const { return store_index_in_file_; }

  // Get file checksum
  std::string GetFileChecksum() const override;

  // Get file checksum function name
  const char* GetFileChecksumFuncName() const override;

  void SetSeqnoTimeTableProperties(const SeqnoToTimeMapping& relevant_mapping,
                                   uint64_t uint_64) override;

 private:
  Arena arena_;
  const ImmutableOptions& ioptions_;
  const MutableCFOptions& moptions_;
  std::vector<std::unique_ptr<InternalTblPropColl>>
      table_properties_collectors_;

  BloomBlockBuilder bloom_block_;
  std::unique_ptr<PlainTableIndexBuilder> index_builder_;

  WritableFileWriter* file_;
  uint64_t offset_ = 0;
  uint32_t bloom_bits_per_key_;
  size_t huge_page_tlb_size_;
  Status status_;
  IOStatus io_status_;
  TableProperties properties_;
  PlainTableKeyEncoder encoder_;

  bool store_index_in_file_;

  std::vector<uint32_t> keys_or_prefixes_hashes_;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.

  const SliceTransform* prefix_extractor_;

  Slice GetPrefix(const Slice& target) const {
    assert(target.size() >= 8);  // target is internal key
    return GetPrefixFromUserKey(ExtractUserKey(target));
  }

  Slice GetPrefix(const ParsedInternalKey& target) const {
    return GetPrefixFromUserKey(target.user_key);
  }

  Slice GetPrefixFromUserKey(const Slice& user_key) const {
    if (!IsTotalOrderMode()) {
      return prefix_extractor_->Transform(user_key);
    } else {
      // Use empty slice as prefix if prefix_extractor is not set.
      // In that case,
      // it falls back to pure binary search and
      // total iterator seek is supported.
      return Slice();
    }
  }

  bool IsTotalOrderMode() const { return (prefix_extractor_ == nullptr); }
};

}  // namespace ROCKSDB_NAMESPACE
