//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/secondary_index.h"

namespace faiss {
struct IndexIVF;
}

namespace ROCKSDB_NAMESPACE {

// EXPERIMENTAL
//
// A SecondaryIndex implementation that wraps a FAISS inverted file based index.
// Indexes the embedding in the specified primary column using the given
// pre-trained faiss::IndexIVF object. Can be used to perform
// K-nearest-neighbors queries.

class FaissIVFIndex : public SecondaryIndex {
 public:
  // Constructs a FaissIVFIndex object. Takes ownership of the given
  // faiss::IndexIVF instance.
  // PRE: index is not nullptr
  FaissIVFIndex(std::unique_ptr<faiss::IndexIVF>&& index,
                std::string primary_column_name);
  ~FaissIVFIndex() override;

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

  // Performs a K-nearest-neighbors vector similarity search for the target
  // using the given secondary index iterator, where K is given by the parameter
  // neighbors and the number of inverted lists to search is given by the
  // parameter probes. The resulting primary keys and distances are returned in
  // the result output parameter. Note that the search may return less than the
  // requested number of results if the inverted lists probed are exhausted
  // before finding K items.
  //
  // The parameter it should be non-nullptr and point to a secondary index
  // iterator corresponding to this index. The search target should be of the
  // correct dimension (i.e. target.size() == dim * sizeof(float), where dim is
  // the dimensionality of the index), neighbors and probes should be positive,
  // and result should be non-nullptr.
  //
  // Returns OK on success, InvalidArgument if the preconditions above are not
  // met, or some other non-OK status if there is an error during the search.
  Status FindKNearestNeighbors(
      SecondaryIndexIterator* it, const Slice& target, size_t neighbors,
      size_t probes, std::vector<std::pair<std::string, float>>* result) const;

 private:
  struct KNNContext;
  class Adapter;

  std::unique_ptr<Adapter> adapter_;
  std::unique_ptr<faiss::IndexIVF> index_;
  std::string primary_column_name_;
  ColumnFamilyHandle* primary_column_family_{};
  ColumnFamilyHandle* secondary_column_family_{};
};

// Helper methods to convert embeddings from a span of floats to Slice or vice
// versa

// Convert the given span of floats of size dim to a Slice.
// PRE: embedding points to a contiguous span of floats of size dim
inline Slice ConvertFloatsToSlice(const float* embedding, size_t dim) {
  return Slice(reinterpret_cast<const char*>(embedding), dim * sizeof(float));
}

// Convert the given Slice to a span of floats of size dim.
// PRE: embedding.size() == dim * sizeof(float)
// Returns nullptr if the precondition is violated.
inline const float* ConvertSliceToFloats(const Slice& embedding, size_t dim) {
  if (embedding.size() != dim * sizeof(float)) {
    return nullptr;
  }

  return reinterpret_cast<const float*>(embedding.data());
}

}  // namespace ROCKSDB_NAMESPACE
