// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2013 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/types.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Slice;
class SliceTransform;

// Interface for lazily resolving blob values within wide-column entities.
// This allows compaction filters to access blob column values on-demand,
// avoiding unnecessary I/O for blob columns that the filter doesn't need.
//
// Usage in CompactionFilter::FilterV4():
//   if (blob_resolver != nullptr) {
//     if (blob_resolver->IsBlobColumn(0)) {
//       Slice resolved_value;
//       Status s = blob_resolver->ResolveColumn(0, &resolved_value);
//       if (!s.ok()) { /* handle error */ }
//       // Use resolved_value...
//     }
//   }
//
// Thread safety: A resolver instance is used by a single thread (the
// compaction thread). All resolved values remain valid until FilterV4
// returns. Multiple columns can be resolved and their values compared
// without copying.
class WideColumnBlobResolver {
 public:
  virtual ~WideColumnBlobResolver() = default;

  // Resolve the value for the column at the given index.
  // Returns OK on success, with resolved_value pointing to the data.
  // The resolved_value remains valid until FilterV4 returns.
  //
  // For blob columns: fetches the blob value from the blob file.
  // For non-blob (inline) columns: returns the inline value directly.
  //
  // Returns an error status if:
  // - column_index is out of bounds
  // - I/O error occurred while fetching the blob
  //
  // If ResolveColumn fails, the compaction filter should stop processing the
  // current entry and return kKeep to avoid applying a partial decision.
  // RocksDB will fail the compaction after FilterV4() returns so the resolver
  // error is surfaced without silently keeping the entry.
  virtual Status ResolveColumn(size_t column_index, Slice* resolved_value) = 0;

  // Resolve multiple columns in the order provided by `column_indices`.
  // The returned slices remain valid until FilterV4 returns.
  //
  // The default implementation calls ResolveColumn() repeatedly. Resolver
  // implementations may override this to batch or optimize I/O in the future.
  virtual Status ResolveColumns(const std::vector<size_t>& column_indices,
                                std::vector<Slice>* resolved_values) {
    assert(resolved_values != nullptr);

    resolved_values->clear();
    resolved_values->reserve(column_indices.size());

    for (size_t column_index : column_indices) {
      Slice resolved_value;
      Status s = ResolveColumn(column_index, &resolved_value);
      if (!s.ok()) {
        resolved_values->clear();
        return s;
      }
      resolved_values->push_back(resolved_value);
    }

    return Status::OK();
  }

  // Check if the column at the given index is a blob reference (vs inline).
  // Returns false if column_index is out of bounds.
  virtual bool IsBlobColumn(size_t column_index) const = 0;

  // Returns the total number of columns in the entity.
  virtual size_t NumColumns() const = 0;
};

// CompactionFilter allows an application to modify/delete a key-value during
// table file creation.
//
// Some general notes:
//
// * RocksDB snapshots do not guarantee to preserve the state of the DB in the
// presence of CompactionFilter. Data seen from a snapshot might disappear after
// a table file created with a `CompactionFilter` is installed. If you use
// snapshots, think twice about whether you want to use `CompactionFilter` and
// whether you are using it in a safe way.
//
// * If multithreaded compaction is being used *and* a single CompactionFilter
// instance was supplied via Options::compaction_filter, CompactionFilter
// methods may be called from different threads concurrently.  The application
// must ensure that such calls are thread-safe. If the CompactionFilter was
// created by a factory, then it will only ever be used by a single thread that
// is doing the table file creation, and this call does not need to be
// thread-safe.  However, multiple filters may be in existence and operating
// concurrently.
//
// * The key passed to the filtering methods includes the timestamp if
// user-defined timestamps are enabled.
//
// * Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionFilter : public Customizable {
 public:
  // Determines how the compaction filter interacts with the merge operator
  // during compaction.
  enum class MergeOrder {
    // (Default) Merge operands are offered to the filter individually before
    // the merge operator combines them. Results of merging are not offered
    // to the filter in the same compaction.
    kFilterBeforeMerge,
    // The merge operator runs first and the filter sees its results.
    // Merging only combines a key's entries that are among the inputs of the
    // current compaction (and not across snapshot boundaries); data in files
    // outside the compaction is not visible to the merge or the filter. So:
    // * If the inputs include the key's base (a Put or a tombstone), or the
    //   compaction is bottommost and no older entries for the key remain,
    //   the full merge result is offered as kValue or kWideColumnEntity.
    // * Otherwise, the key's operands in the inputs are offered as
    //   kMergeOperand: combined into one by PartialMergeMulti when possible,
    //   or individually if partial merge declines or there is only one.
    //   The base and any older operands outside the compaction are not seen.
    // Results changed by the filter are not merged again in the same
    // compaction. Only applies to compaction (not flush or recovery).
    // Filters that implement only FilterV2 or FilterV3 also work.
    kMergeBeforeFilter,
  };

  // Returns the merge order this filter requires. Must return the same value
  // for the lifetime of the filter. Can be called concurrently and without
  // the DB mutex, so it must be cheap, must not block, and must not call
  // into the DB.
  //
  // Notes on kMergeBeforeFilter:
  // * A merge result is offered to the filter only if none of its operands
  //   is visible to any snapshot. Plain values are still offered regardless
  //   of snapshots, as with kFilterBeforeMerge.
  // * kPurge on a value or entity is treated as kRemove (ordinary Delete),
  //   because SingleDelete is incompatible with Merge history. Do not use
  //   SingleDelete on affected keys, even after switching back to
  //   kFilterBeforeMerge.
  // * kPurge and kChangeWideColumnEntity are not supported for kMergeOperand
  //   and fail the compaction.
  // * How operands are grouped into partial results can differ between
  //   compactions, and SST files keep no record of earlier groupings, so
  //   filter decisions must be correct for any grouping.
  // * Not supported with user-defined timestamps, WritePrepared or
  //   WriteUnprepared transactions (SnapshotChecker), or stacked BlobDB.
  //   DB open and column family creation reject these configurations.
  // * Filter wrappers must forward this value from the wrapped filter.
  virtual MergeOrder GetMergeOrder() const {
    return MergeOrder::kFilterBeforeMerge;
  }

  // Value type of the key-value passed to the compaction filter's
  // FilterV2/V3/V4 methods.
  enum ValueType {
    // Plain key-value
    kValue,
    // Merge operand
    kMergeOperand,
    // Used internally by the old stacked BlobDB implementation; this value type
    // is never passed to application code. Note that when using the new
    // integrated BlobDB, values stored separately as blobs are retrieved and
    // presented to FilterV2/V3/V4 with the type kValue above.
    kBlobIndex,
    // Wide-column entity
    kWideColumnEntity,
  };

  // Potential decisions that can be returned by the compaction filter's
  // FilterV2/V3/V4 and FilterBlobByKey methods. See decision-specific caveats
  // and constraints below.
  enum class Decision {
    // Keep the current key-value as-is.
    kKeep,

    // Remove the current key-value. Note that the semantics of removal are
    // dependent on the value type. If the current key-value is a plain
    // key-value or a wide-column entity, it is converted to a tombstone
    // (Delete), resulting in the deletion of any earlier versions of the key.
    // If it is a merge operand, it is simply dropped. Note: if you are using
    // a TransactionDB, it is not recommended to filter out merge operands.
    // If a Merge operation is filtered out, TransactionDB may not realize there
    // is a write conflict and may allow a Transaction that should have failed
    // to Commit. Instead, it is better to implement any Merge filtering inside
    // the MergeOperator.
    kRemove,

    // Change the value of the current key-value. If the current key-value is a
    // plain key-value or a merge operand, its value is updated but its value
    // type remains the same. If the current key-value is a wide-column entity,
    // it is converted to a plain key-value with the new value specified.
    kChangeValue,

    // Remove all key-values with key in [key, *skip_until). This range of keys
    // will be skipped in a way that potentially avoids some IO operations
    // compared to removing the keys one by one. Note that removal in this case
    // means dropping the key-value regardless of value type; in other words, in
    // contrast with kRemove, plain values and entities are not converted to
    // tombstones.
    //
    // *skip_until <= key is treated the same as Decision::kKeep (since the
    // range [key, *skip_until) is empty).
    //
    // Caveats:
    // * The keys are skipped even if there are snapshots containing them,
    //   i.e. values removed by kRemoveAndSkipUntil can disappear from a
    //   snapshot - beware if you're using TransactionDB or DB::GetSnapshot().
    // * If value for a key was overwritten or merged into (multiple Put()s
    //   or Merge()s), and `CompactionFilter` skips this key with
    //   kRemoveAndSkipUntil, it's possible that it will remove only
    //   the new value, exposing the old value that was supposed to be
    //   overwritten.
    // * Doesn't work with PlainTableFactory in prefix mode.
    // * If you use kRemoveAndSkipUntil for table files created by compaction,
    //   consider also reducing compaction_readahead_size option.
    kRemoveAndSkipUntil,

    // Used internally by the old stacked BlobDB implementation. Returning this
    // decision from application code is not supported.
    kChangeBlobIndex,

    // Used internally by the old stacked BlobDB implementation. Returning this
    // decision from application code is not supported.
    kIOError,

    // Remove the current key-value by converting it to a SingleDelete-type
    // tombstone. Only supported for plain-key values and wide-column entities;
    // not supported for merge operands. All the caveats related to
    // SingleDeletes apply.
    kPurge,

    // Change the current key-value to the wide-column entity specified. If the
    // current key-value is already a wide-column entity, only its columns are
    // updated; if it is a plain key-value, it is converted to a wide-column
    // entity with the specified columns. Not supported for merge operands.
    // Only applicable to FilterV3/V4.
    kChangeWideColumnEntity,

    // When using the integrated BlobDB implementation, it may be possible for
    // applications to make a filtering decision for a given blob based on
    // the key only without actually reading the blob value, which saves some
    // I/O; see the FilterBlobByKey method below. Returning kUndetermined from
    // FilterBlobByKey signals that making a decision solely based on the
    // key is not possible; in this case, RocksDB reads the blob value and
    // passes the key-value to the regular filtering method. Only applicable to
    // FilterBlobByKey; returning this value from FilterV2/V3/V4 is not
    // supported.
    kUndetermined,
  };

  // Used internally by the old stacked BlobDB implementation.
  enum class BlobDecision { kKeep, kChangeValue, kCorruption, kIOError };

  // Context information for a table file creation.
  struct Context {
    // Whether this table file is created as part of a compaction including all
    // table files.
    bool is_full_compaction;
    // Whether this table file is created as part of a compaction requested by
    // the client.
    bool is_manual_compaction;
    // The lowest level among all the input files (if any) used in table
    // creation
    int input_start_level = kUnknownStartLevel;
    // The column family that will contain the created table file.
    uint32_t column_family_id;
    // Reason this table file is being created.
    TableFileCreationReason reason;
    // Map from all the input files (if any) used in table creation to their
    // table properties. When there are such input files but RocksDB fail to
    // load their table properties, `input_table_properties` will be an empty
    // map.
    TablePropertiesCollection input_table_properties;

    static const int kUnknownStartLevel = -1;
  };

  ~CompactionFilter() override = default;
  static const char* Type() { return "CompactionFilter"; }
  static Status CreateFromString(const ConfigOptions& config_options,
                                 const std::string& name,
                                 const CompactionFilter** result);

  // The table file creation process invokes this method before adding a kv to
  // the table file. A return value of false indicates that the kv should be
  // preserved in the new table file and a return value of true indicates
  // that this key-value should be removed (that is, converted to a tombstone).
  // The application can inspect the existing value of the key and make decision
  // based on it.
  //
  // With the default merge order, key-values that are results of merge
  // operations during table file creation are not passed into this function.
  // kMergeBeforeFilter offers eligible full results through FilterV4, which
  // can delegate here. With the default order and a mix of Put()s
  // and Merge()s on a same key, we only guarantee to process the merge operands
  // through the `CompactionFilter`s. Put()s might be processed, or might not.
  //
  // When the value is to be preserved, the application has the option
  // to modify the existing_value and pass it back through new_value.
  // value_changed needs to be set to true in this case.
  virtual bool Filter(int /*level*/, const Slice& /*key*/,
                      const Slice& /*existing_value*/,
                      std::string* /*new_value*/,
                      bool* /*value_changed*/) const {
    return false;
  }

  // The table file creation process invokes this method on every merge operand.
  // If this method returns true, the merge operand will be ignored and not
  // written out in the new table file.
  //
  // Note: If you are using a TransactionDB, it is not recommended to implement
  // FilterMergeOperand().  If a Merge operation is filtered out, TransactionDB
  // may not realize there is a write conflict and may allow a Transaction to
  // Commit that should have failed.  Instead, it is better to implement any
  // Merge filtering inside the MergeOperator.
  //
  // Note: Under WritePrepared, WriteUnprepared, and WriteCommitted
  // TransactionDB, this method is skipped for operands at or below the latest
  // published sequence, typically every operand in the input.
  virtual bool FilterMergeOperand(int /*level*/, const Slice& /*key*/,
                                  const Slice& /*operand*/) const {
    return false;
  }

  // A unified API for plain values and merge operands that may
  // return a variety of decisions (see Decision above). The `value_type`
  // parameter indicates the type of the key-value and the `existing_value`
  // contains the current value or merge operand. The `new_value` output
  // parameter can be used to set the updated value or merge operand when the
  // kChangeValue decision is made by the filter. See the description of
  // kRemoveAndSkipUntil above for the semantics of the `skip_until` output
  // parameter, and see Decision above for more information on the semantics of
  // the potential return values.
  //
  // The default implementation uses Filter() and FilterMergeOperand().
  // If you're overriding this method, no need to override the other two.
  virtual Decision FilterV2(int level, const Slice& key, ValueType value_type,
                            const Slice& existing_value, std::string* new_value,
                            std::string* /*skip_until*/) const {
    switch (value_type) {
      case ValueType::kValue: {
        bool value_changed = false;
        bool rv = Filter(level, key, existing_value, new_value, &value_changed);
        if (rv) {
          return Decision::kRemove;
        }
        return value_changed ? Decision::kChangeValue : Decision::kKeep;
      }

      case ValueType::kMergeOperand: {
        bool rv = FilterMergeOperand(level, key, existing_value);
        return rv ? Decision::kRemove : Decision::kKeep;
      }

      case ValueType::kBlobIndex:
        return Decision::kKeep;

      default:
        assert(false);
        return Decision::kKeep;
    }
  }

  // Wide column aware unified API. Called for plain values, merge operands, and
  // wide-column entities; the `value_type` parameter indicates the type of the
  // key-value. When the key-value is a plain value or a merge operand, the
  // `existing_value` parameter contains the existing value and the
  // `existing_columns` parameter is invalid (nullptr). When the key-value is a
  // wide-column entity, the `existing_columns` parameter contains the wide
  // columns of the existing entity and the `existing_value` parameter is
  // invalid (nullptr). The `new_value` output parameter can be used to set the
  // updated value or merge operand when the kChangeValue decision is made by
  // the filter. The `new_columns` output parameter can be used to specify
  // the pairs of column names and column values when the
  // kChangeWideColumnEntity decision is returned. See the description of
  // kRemoveAndSkipUntil above for the semantics of the `skip_until` output
  // parameter, and see Decision above for more information on the semantics of
  // the potential return values.
  //
  // For compatibility, the default implementation keeps all wide-column
  // entities, and falls back to FilterV2 for plain values and merge operands.
  // If you override this method, there is no need to override FilterV2 (or
  // Filter/FilterMergeOperand).
  //
  // Note: prefer overriding FilterV4 instead for new code; FilterV4 extends
  // this API with blob column lazy loading support.
  virtual Decision FilterV3(
      int level, const Slice& key, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* new_value,
      std::vector<std::pair<std::string, std::string>>* /* new_columns */,
      std::string* skip_until) const {
    (void)existing_columns;

    assert(!existing_value || !existing_columns);
    assert(value_type == ValueType::kWideColumnEntity || existing_value);
    assert(value_type != ValueType::kWideColumnEntity || existing_columns);

    if (value_type == ValueType::kWideColumnEntity) {
      return Decision::kKeep;
    }

    return FilterV2(level, key, value_type, *existing_value, new_value,
                    skip_until);
  }

  // Extends FilterV3 with blob column lazy loading support. Called for plain
  // values, merge operands, and wide-column entities.
  //
  // For `ValueType::kValue`, `existing_value` contains the user value. This
  // is true whether the value was stored inline or behind a BlobIndex; plain
  // blob-backed values are eagerly resolved before FilterV4() is invoked.
  //
  // When the entity has blob columns (columns whose values are stored in blob
  // files), the `blob_resolver` parameter provides a way to lazily resolve
  // blob values on-demand. If `blob_resolver` is non-null:
  // - `existing_columns` contains all columns, but blob columns will have
  //   blob index references as their values (not the actual blob data)
  // - Call `blob_resolver->IsBlobColumn(idx)` to check if a column is a blob
  // - Call `blob_resolver->ResolveColumn(idx, &value)` or
  //   `blob_resolver->ResolveColumns(indices, &values)` to fetch blob values
  //
  // This lazy loading mechanism allows filters to avoid I/O for blob columns
  // they don't need to access.
  //
  // `blob_resolver` is non-null only when SupportsFilterV4() returns true AND
  // the entity has blob columns. For entities without blob columns, for
  // non-entity value types, and when SupportsFilterV4() returns false,
  // `blob_resolver` is nullptr and all column values in `existing_columns`
  // are already fully resolved.
  //
  // For compatibility, the default implementation delegates to FilterV3.
  // If you override this method, there is no need to override
  // FilterV3/FilterV2/Filter/FilterMergeOperand.
  virtual Decision FilterV4(
      int level, const Slice& key, ValueType value_type,
      const Slice* existing_value, const WideColumns* existing_columns,
      std::string* new_value,
      std::vector<std::pair<std::string, std::string>>* new_columns,
      std::string* skip_until, WideColumnBlobResolver* blob_resolver) const {
    (void)blob_resolver;

    return FilterV3(level, key, value_type, existing_value, existing_columns,
                    new_value, new_columns, skip_until);
  }

  // Returns true if the filter overrides FilterV4 and can handle
  // WideColumnBlobResolver (lazy blob loading). When false (default),
  // FilterV4 delegates to FilterV3 with fully resolved column values --
  // blob columns are eagerly fetched before the filter is called, ensuring
  // backward compatibility with FilterV3-only filters.
  //
  // Override this to return true if your filter overrides FilterV4 and
  // uses WideColumnBlobResolver for lazy blob loading.
  virtual bool SupportsFilterV4() const { return false; }

  // Internal (BlobDB) use only. Do not override in application code.
  virtual BlobDecision PrepareBlobOutput(const Slice& /* key */,
                                         const Slice& /* existing_value */,
                                         std::string* /* new_value */) const {
    return BlobDecision::kKeep;
  }

  // This function is deprecated. Snapshots will always be ignored for
  // `CompactionFilter`s, because we realized that not ignoring snapshots
  // doesn't provide the guarantee we initially thought it would provide.
  // Repeatable reads will not be guaranteed anyway. If you override the
  // function and returns false, we will fail the table file creation.
  virtual bool IgnoreSnapshots() const { return true; }

  // Returns a name that identifies this `CompactionFilter`.
  // The name will be printed to LOG file on start up for diagnosis.
  const char* Name() const override = 0;

  // Internal (BlobDB) use only. Do not override in application code.
  virtual bool IsStackedBlobDbInternalCompactionFilter() const { return false; }

  // In the case of BlobDB, it may be possible to reach a decision with only
  // the key without reading the actual value, saving some I/O operations.
  // Keys where the value is stored separately in a blob file will be
  // passed to this method. If the method returns a supported decision other
  // than kUndetermined, it will be considered final and performed without
  // reading the existing value. Returning kUndetermined continues with the
  // usual value-based filtering path: RocksDB eagerly resolves the blob and
  // invokes FilterV4()/FilterV3()/FilterV2() with ValueType::kValue so legacy
  // filters observe the underlying user value rather than the BlobIndex
  // encoding. If RocksDB cannot access the blob in the current table-file
  // creation context, table-file creation fails. The output parameters
  // `new_value` and `skip_until` are applicable to the decisions kChangeValue
  // and kRemoveAndSkipUntil respectively, and have the same semantics as
  // the corresponding parameters of FilterV2/V3/V4.
  virtual Decision FilterBlobByKey(int /*level*/, const Slice& /*key*/,
                                   std::string* /*new_value*/,
                                   std::string* /*skip_until*/) const {
    return Decision::kUndetermined;
  }
};

// Each thread of work involving creating table files will create a new
// `CompactionFilter` according to `ShouldFilterTableFileCreation()`. This
// allows the application to know about the different ongoing threads of work
// and makes it unnecessary for `CompactionFilter` to provide thread-safety.
//
// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class CompactionFilterFactory : public Customizable {
 public:
  // Returns the merge order of filters created by this factory. Every
  // non-null filter it creates must return the same value from
  // CompactionFilter::GetMergeOrder(); otherwise the compaction fails with
  // InvalidArgument. A null filter means no filtering for that compaction,
  // which is allowed with either order. Same stability and concurrency
  // requirements as CompactionFilter::GetMergeOrder().
  //
  // With kMergeBeforeFilter, ShouldFilterTableFileCreation() must return
  // false for every reason other than kCompaction (checked at DB open and
  // column family creation). Flush, recovery, and other table file creation
  // do not run this order, because they can see only part of a key's history
  // and would offer the filter results that differ from compaction.
  virtual CompactionFilter::MergeOrder GetMergeOrder() const {
    return CompactionFilter::MergeOrder::kFilterBeforeMerge;
  }

  ~CompactionFilterFactory() override = default;
  static const char* Type() { return "CompactionFilterFactory"; }
  static Status CreateFromString(
      const ConfigOptions& config_options, const std::string& name,
      std::shared_ptr<CompactionFilterFactory>* result);

  // Returns whether a thread creating table files for the specified `reason`
  // should invoke `CreateCompactionFilter()` and pass KVs through the returned
  // filter.
  virtual bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const {
    // For backward compatibility, default implementation only applies
    // `CompactionFilter` to files generated by compaction.
    return reason == TableFileCreationReason::kCompaction;
  }

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) = 0;

  // Returns a name that identifies this `CompactionFilter` factory.
  const char* Name() const override = 0;
};

}  // namespace ROCKSDB_NAMESPACE
