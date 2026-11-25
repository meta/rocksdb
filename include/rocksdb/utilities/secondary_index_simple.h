//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/secondary_index.h"

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
//
// A simple secondary index implementation that indexes the specified column
// as-is.

class SimpleSecondaryIndex : public SecondaryIndex {
 public:
  explicit SimpleSecondaryIndex(std::string primary_column_name);

  void SetPrimaryColumnFamily(ColumnFamilyHandle* column_family) override;
  void SetSecondaryColumnFamily(ColumnFamilyHandle* column_family) override;

  ColumnFamilyHandle* GetPrimaryColumnFamily() const override;
  ColumnFamilyHandle* GetSecondaryColumnFamily() const override;

  Slice GetPrimaryColumnName() const override;

  Status UpdatePrimaryColumnValue(
      const Slice& primary_key, const Slice& primary_column_value,
      std::optional<std::variant<Slice, std::string>>* updated_column_value)
      const override;

  Status GetSecondaryKeyPrefix(
      const Slice& primary_key, const Slice& primary_column_value,
      std::variant<Slice, std::string>* secondary_key_prefix) const override;

  Status FinalizeSecondaryKeyPrefix(
      std::variant<Slice, std::string>* secondary_key_prefix) const override;

  Status GetSecondaryValue(const Slice& primary_key,
                           const Slice& primary_column_value,
                           const Slice& original_column_value,
                           std::optional<std::variant<Slice, std::string>>*
                               secondary_value) const override;

 private:
  std::string primary_column_name_;
  ColumnFamilyHandle* primary_column_family_{};
  ColumnFamilyHandle* secondary_column_family_{};
};

}  // namespace ROCKSDB_NAMESPACE
