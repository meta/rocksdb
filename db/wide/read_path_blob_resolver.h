//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_constants.h"
#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "rocksdb/cleanable.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/wide_columns.h"

namespace ROCKSDB_NAMESPACE {

class Version;
class SameFileBlobReader;

// TODO: ReadPathBlobResolver and CompactionBlobResolver (in
// compaction_iterator.h) share significant logic for blob column resolution
// and caching. A refactoring into a common base class or shared utility
// could reduce duplication. The two classes differ in fetcher ownership and
// their surrounding contexts (read path vs compaction path with stats
// tracking).
//
// Enables lazy (on-demand) resolution of blob column values in the read path.
// When a wide-column entity contains blob references (V2 format), the resolver
// stores the blob metadata and fetches blob values only when explicitly
// requested via ResolveColumn(). Resolved values are cached to avoid
// re-fetching.
//
// Used by both the iterator path (DBIter) and the point-lookup path
// (GetEntity via PinnableWideColumns).
//
// Thread safety: not thread-safe. A resolver instance is used by a single
// thread at a time. The Version* must remain valid for the lifetime of the
// resolver (ensured by SuperVersion pinning in the caller).
class ReadPathBlobResolver {
 public:
  ReadPathBlobResolver(const Version* version, const ReadOptions& read_options,
                       BlobFileCache* blob_file_cache = nullptr,
                       bool allow_write_path_fallback = false);

  // Reset the resolver for a new entity. Clears all cached values.
  // The columns and blob_columns pointers must remain valid for the lifetime
  // of the resolver (or until the next Reset call).
  //
  // same_file_reader: when non-null, same-file ("embedded") blob references are
  // resolved against it (the SST that held the entity) via an
  // EmbeddedAwareBlobFetcher; it must outlive the resolver (ensured, on the
  // lazy read path, by the same SuperVersion pin that keeps the Version alive,
  // combined with immortal table readers when max_open_files == -1). When null
  // (e.g. the DBIter path, which resolves embedded refs separately), only
  // separate-file references are resolvable.
  void Reset(const Slice& user_key, const std::vector<WideColumn>* columns,
             const std::vector<std::pair<size_t, BlobIndex>>* blob_columns,
             const SameFileBlobReader* same_file_reader = nullptr);

  // Resolve the value for the column at the given index.
  // For blob columns, fetches the blob value from the blob file (or returns
  // from cache if already resolved). For inline columns, returns the inline
  // value directly.
  // Returns an error status if:
  // - column_index is out of bounds
  // - I/O error occurred while fetching the blob
  Status ResolveColumn(size_t column_index, Slice* resolved_value);

  // Resolve a byte sub-range [range_offset, range_offset + range_length) of the
  // column at `column_index` into *result (zero-copy). `range_length` may be
  // larger than the remaining bytes (or SIZE_MAX for "to the end"); it is
  // clamped to the column's logical size. An offset at/past the end yields an
  // empty result (not an error).
  //
  // Reads only the requested bytes -- skipping whole-record checksum
  // verification and blob-cache population -- only for a strict sub-range of an
  // uncompressed blob reference (in a separate blob file, or
  // embedded/same-file) that is not already resolved, when !force_verify and
  // the read is servable (a Version-backed range read for separate-file refs,
  // or a SameFileBlobReader for embedded refs). Every other case (inline
  // column, already-resolved column, inlined blob, compressed reference, a
  // whole-column read, or force_verify) resolves the whole column (caching it,
  // verifying under ReadOptions::verify_checksums) and slices the requested
  // range out of it.
  //
  // Unlike ResolveColumn, a partial read is NOT cached: *result owns (or pins)
  // its own bytes, independent of this resolver's whole-column cache.
  Status ResolveColumnRange(size_t column_index, uint64_t range_offset,
                            size_t range_length, bool force_verify,
                            PinnableSlice* result);

  // How the cross-key batch coalescing path
  // (LazyWideColumnsBatch::MultiResolve) should resolve a byte-range read of
  // one column.
  enum class LazyColumnReadPlan {
    // Resolve individually via ResolveColumnRange (inline column, already
    // resolved, TTL-inlined blob, empty range, force-verify, or an unservable
    // reference); no cross-key coalescing benefit.
    kServeIndividually,
    // Fetch the whole blob value, then cache + slice; coalesce per blob file.
    kFetchWholeSeparateFile,
    // Fetch the whole embedded value, then cache + slice; coalesce per SST.
    kFetchWholeSameFile,
    // Fetch only the (uncompressed) sub-range; coalesce per blob file.
    kFetchRangeSeparateFile,
    // Fetch only the (uncompressed) embedded sub-range; coalesce per SST.
    kFetchRangeSameFile,
  };

  struct LazyColumnReadClassification {
    LazyColumnReadPlan plan = LazyColumnReadPlan::kServeIndividually;
    // For fetch plans: the reference to read (owned by blob_columns_).
    const BlobIndex* blob_index = nullptr;
    // For range-fetch plans: the effective (clamped) sub-range to read.
    uint64_t range_offset = 0;
    size_t range_length = 0;
  };

  // Classifies how a byte-range read of `column_index` should be resolved by
  // the batch coalescing path. The decision mirrors ResolveColumnRange exactly,
  // so that pre-fetching the whole/range values (per the returned plan) and
  // then calling ResolveColumnRange for the whole reads yields the same bytes
  // with no redundant I/O. Out-of-bounds / non-blob / cached / inlined /
  // empty-range / force-verify / unservable reads return kServeIndividually.
  LazyColumnReadClassification ClassifyColumnRange(size_t column_index,
                                                   uint64_t range_offset,
                                                   size_t range_length,
                                                   bool force_verify) const;

  // Inserts an externally fetched whole column value into the resolver's cache
  // (idempotent: if the column is already cached the passed value is dropped
  // and the existing entry kept). Used by the batch coalescing path, which
  // fetches whole blob values in bulk; a subsequent ResolveColumnRange of that
  // column then slices from the cache with no I/O.
  void AdoptResolvedWholeColumn(size_t column_index, PinnableSlice&& value);

  // Accessors used by the batch coalescing path to group and dispatch reads.
  const Version* version() const { return blob_fetcher_.version(); }
  void SetVersion(const Version* version) { blob_fetcher_.SetVersion(version); }
  const SameFileBlobReader* same_file_reader() const {
    return same_file_reader_;
  }
  const ReadOptions& read_options() const {
    return blob_fetcher_.read_options();
  }
  const Slice& user_key() const { return user_key_; }

  // Resolve multiple columns in the order provided by `column_indices`.
  // Resolved blob values are cached exactly as if ResolveColumn() were called
  // repeatedly.
  Status ResolveColumns(const std::vector<size_t>& column_indices,
                        std::vector<Slice>* resolved_values);

  // Resolve all unresolved blob columns at once.
  Status ResolveAllColumns();

  // Check if the column at the given index is an unresolved blob reference.
  // Returns false if column_index is out of bounds or the column is inline
  // or already resolved.
  bool IsUnresolvedColumn(size_t column_index) const;

  // Returns true if any blob columns have not yet been resolved.
  bool HasUnresolvedColumns() const;

  // Returns the total number of columns in the entity.
  size_t NumColumns() const;

  // Register a cleanup function that will be called when the resolver is
  // destroyed. Used to pin resources (e.g., SuperVersion) that must remain
  // alive while the resolver exists.
  void RegisterCleanup(Cleanable::CleanupFunction function, void* arg1,
                       void* arg2) {
    cleanable_.RegisterCleanup(function, arg1, arg2);
  }

 private:
  // Maps the public (ReadOptions::verify_checksums, force_verify) pair to the
  // internal 3-valued verify policy, once at this boundary (see
  // BlobVerifyPolicy). Every downstream blob read is driven by the resulting
  // policy rather than by re-deriving verification from two bools.
  static BlobVerifyPolicy DeriveVerifyPolicy(bool verify_checksums,
                                             bool force_verify);

  // Reads a blob reference into *out, applying `policy`. range_length ==
  // kWholeBlobLength selects the whole value; any other length selects the
  // strict sub-range [range_offset, range_offset + range_length). Routes
  // same-file ("embedded") references to the current SST's SameFileBlobReader
  // and separate-file references to the Version-backed fetcher.
  Status FetchBlobRef(const BlobIndex& blob_index, uint64_t range_offset,
                      size_t range_length, BlobVerifyPolicy policy,
                      PinnableSlice* out);

  // Shared implementation of ResolveColumn / the whole-column path of
  // ResolveColumnRange: resolves and caches the whole column value under the
  // given verify policy (kVerifyIfPresent forces whole-record verification even
  // when ReadOptions::verify_checksums is off; see LazyColumnReadRequest).
  Status ResolveColumnInternal(size_t column_index, BlobVerifyPolicy policy,
                               Slice* resolved_value);

  // Owns its ReadOptions: a resolver's lifetime is independent of the caller
  // that created it (e.g. it may outlive the originating ReadOptions once
  // returned as part of a lazy result).
  OwningVersionBlobFetcher blob_fetcher_;

  Slice user_key_;
  const std::vector<WideColumn>* columns_ = nullptr;
  const std::vector<std::pair<size_t, BlobIndex>>* blob_columns_ = nullptr;
  // Non-null on the lazy read path for entities with same-file blob references;
  // see Reset().
  const SameFileBlobReader* same_file_reader_ = nullptr;

  // Cache for resolved blob values to avoid re-fetching.
  // Uses a vector of (column_index, PinnableSlice) pairs. Typical entities
  // have few blob columns (<5), making linear scan cheaper than hash map
  // overhead. PinnableSlice values need stable addresses, so we use
  // unique_ptr to prevent invalidation when the vector grows.
  std::vector<std::pair<size_t, std::unique_ptr<PinnableSlice>>>
      resolved_cache_;

  // Cleanable for pinning resources (e.g., SuperVersion).
  Cleanable cleanable_;
};

}  // namespace ROCKSDB_NAMESPACE
