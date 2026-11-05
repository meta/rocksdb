//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/wide/read_path_blob_resolver.h"

#include <cassert>

#include "db/blob/blob_fetcher.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/version_set.h"
#include "db/wide/blob_column_resolver_util.h"

namespace ROCKSDB_NAMESPACE {

ReadPathBlobResolver::ReadPathBlobResolver(
    const Version* version, const ReadOptions& read_options,
    BlobFileCache* blob_file_cache, bool allow_write_path_fallback,
    CompressionManager* configured_compression_manager)
    : blob_fetcher_(version, ReadOptions(read_options), blob_file_cache,
                    allow_write_path_fallback, configured_compression_manager) {
}

void ReadPathBlobResolver::Reset(
    const Slice& user_key, const std::vector<WideColumn>* columns,
    const std::vector<std::pair<size_t, BlobIndex>>* blob_columns,
    const SameFileBlobReader* same_file_reader) {
  user_key_ = user_key;
  columns_ = columns;
  blob_columns_ = blob_columns;
  same_file_reader_ = same_file_reader;
  resolved_cache_.clear();
}

Status ReadPathBlobResolver::ResolveColumn(size_t column_index,
                                           Slice* resolved_value) {
  // Whole-column resolution uses the captured ReadOptions' verify_checksums
  // (no force_verify).
  return ResolveColumnInternal(
      column_index,
      DeriveVerifyPolicy(blob_fetcher_.read_options().verify_checksums,
                         /*force_verify=*/false),
      resolved_value);
}

BlobVerifyPolicy ReadPathBlobResolver::DeriveVerifyPolicy(bool verify_checksums,
                                                          bool force_verify) {
  if (force_verify) {
    return BlobVerifyPolicy::kVerifyIfPresent;
  }
  return verify_checksums ? BlobVerifyPolicy::kVerifyIfNoAmplification
                          : BlobVerifyPolicy::kSkip;
}

Status ReadPathBlobResolver::FetchBlobRef(const BlobIndex& blob_index,
                                          uint64_t range_offset,
                                          size_t range_length,
                                          BlobVerifyPolicy policy,
                                          PinnableSlice* out) {
  if (blob_index.IsSameFile()) {
    // Same-file ("embedded") references need the originating SST's reader.
    if (same_file_reader_ == nullptr) {
      return Status::Corruption(
          "Cannot resolve same-file blob reference: no same-file reader");
    }
    return same_file_reader_->GetSameFileBlob(blob_fetcher_.read_options(),
                                              blob_index, range_offset,
                                              range_length, policy, out);
  }
  return blob_fetcher_.FetchBlobRange(user_key_, blob_index, range_offset,
                                      range_length, policy, out,
                                      /*bytes_read=*/nullptr);
}

Status ReadPathBlobResolver::ResolveColumnInternal(size_t column_index,
                                                   BlobVerifyPolicy policy,
                                                   Slice* resolved_value) {
  assert(columns_);
  assert(resolved_value);

  Status status = Status::OK();
  if (column_index >= columns_->size()) {
    status = Status::InvalidArgument("Column index out of bounds");
  } else {
    const BlobIndex* blob_index_ptr =
        blob_resolver_util::FindBlobColumn(blob_columns_, column_index);

    if (blob_index_ptr == nullptr) {
      // Inline column -- return the value directly
      *resolved_value = (*columns_)[column_index].value();
    } else {
      // Check if already resolved
      PinnableSlice* cached =
          blob_resolver_util::FindInCache(resolved_cache_, column_index);
      if (cached != nullptr) {
        *resolved_value = *cached;
      } else {
        const BlobIndex& blob_index = *blob_index_ptr;

        // Handle inlined blobs
        if (blob_index.IsInlined()) {
          *resolved_value = blob_resolver_util::CacheInlinedBlob(
              resolved_cache_, column_index, blob_index);
        } else {
          resolved_cache_.emplace_back(column_index,
                                       std::make_unique<PinnableSlice>());
          auto& new_entry = resolved_cache_.back();

          // Whole-column read of a blob reference, cached. FetchBlobRef routes
          // same-file vs separate-file references and applies the verify policy
          // (which folds in force_verify: kVerifyIfPresent verifies the whole
          // record even when ReadOptions::verify_checksums is off).
          status =
              FetchBlobRef(blob_index, /*range_offset=*/0, kWholeBlobLength,
                           policy, new_entry.second.get());
          if (!status.ok()) {
            resolved_cache_.pop_back();
          } else {
            *resolved_value = *new_entry.second;
          }
        }
      }
    }
  }
  return status;
}

namespace {

// Pin [range_offset, range_offset + range_length) of `whole` into *result,
// clamped to `whole`'s size (offset at/past end -> empty). The bytes are not
// copied or owned here: *result points into `whole`, whose storage must outlive
// *result (the resolver's whole-column cache / entity buffer, which lives as
// long as this resolver).
void PinClampedSubRange(const Slice& whole, uint64_t range_offset,
                        size_t range_length, PinnableSlice* result) {
  result->Reset();
  if (range_offset >= whole.size()) {
    result->PinSlice(Slice(), nullptr);
    return;
  }
  const size_t off = static_cast<size_t>(range_offset);
  const size_t avail = whole.size() - off;
  const size_t len = range_length > avail ? avail : range_length;
  result->PinSlice(Slice(whole.data() + off, len), nullptr);
}

}  // namespace

Status ReadPathBlobResolver::ResolveColumnRange(size_t column_index,
                                                uint64_t range_offset,
                                                size_t range_length,
                                                bool force_verify,
                                                PinnableSlice* result) {
  assert(columns_);

  if (column_index >= columns_->size()) {
    return Status::InvalidArgument("Column index out of bounds");
  }

  // Derive the verify policy once from (verify_checksums, force_verify) and let
  // it drive both the partial-vs-whole decision below and the downstream read.
  const BlobVerifyPolicy policy = DeriveVerifyPolicy(
      blob_fetcher_.read_options().verify_checksums, force_verify);

  // No output buffer: the caller only wants to surface any I/O / integrity
  // error (and honor force_verify). Resolve the whole column and return; there
  // is nothing to slice into.
  if (result == nullptr) {
    Slice ignored;
    return ResolveColumnInternal(column_index, policy, &ignored);
  }

  const BlobIndex* blob_index_ptr =
      blob_resolver_util::FindBlobColumn(blob_columns_, column_index);

  // Decide whether the I/O-saving partial path applies. It requires a blob
  // reference that is: not already resolved (else we slice the cached whole
  // value), uncompressed (a strict sub-range of a compressed record can't be
  // decompressed in isolation), a strict sub-range (a whole-column read takes
  // the verifying + cache-filling path), and a policy that permits skipping
  // verification (kVerifyIfPresent forces a whole verified read). The read then
  // goes to either the separate-file range fetcher (needs a Version) or, for a
  // same-file / embedded reference, the current SST's SameFileBlobReader.
  if (blob_index_ptr != nullptr &&
      policy != BlobVerifyPolicy::kVerifyIfPresent &&
      blob_resolver_util::FindInCache(resolved_cache_, column_index) ==
          nullptr) {
    const BlobIndex& blob_index = *blob_index_ptr;
    const bool separate_file_ok =
        !blob_index.IsSameFile() && blob_fetcher_.SupportsRangeRead();
    const bool same_file_ok =
        blob_index.IsSameFile() && same_file_reader_ != nullptr;
    if (!blob_index.IsInlined() && blob_index.compression() == kNoCompression &&
        (separate_file_ok || same_file_ok)) {
      const uint64_t value_size = blob_index.size();
      const bool strict_subrange =
          range_offset > 0 || range_length < value_size;
      if (strict_subrange) {
        result->Reset();
        if (range_offset >= value_size || range_length == 0) {
          // Nothing to read; empty (not an error), no I/O.
          result->PinSlice(Slice(), nullptr);
          return Status::OK();
        }
        const size_t avail = static_cast<size_t>(value_size - range_offset);
        const size_t actual_len = range_length > avail ? avail : range_length;
        return FetchBlobRef(blob_index, range_offset, actual_len, policy,
                            result);
      }
    }
  }

  // Full path: resolve the whole column (inline value directly, blob reference
  // via the resolver's cache, filling it on a miss and verifying per policy),
  // then slice out the requested range.
  Slice whole;
  Status s = ResolveColumnInternal(column_index, policy, &whole);
  if (!s.ok()) {
    return s;
  }
  PinClampedSubRange(whole, range_offset, range_length, result);
  return Status::OK();
}

ReadPathBlobResolver::LazyColumnReadClassification
ReadPathBlobResolver::ClassifyColumnRange(size_t column_index,
                                          uint64_t range_offset,
                                          size_t range_length,
                                          bool force_verify) const {
  LazyColumnReadClassification c;

  if (columns_ == nullptr || column_index >= columns_->size()) {
    return c;  // kServeIndividually (ResolveColumnRange -> InvalidArgument)
  }
  const BlobIndex* blob_index_ptr =
      blob_resolver_util::FindBlobColumn(blob_columns_, column_index);
  if (blob_index_ptr == nullptr) {
    return c;  // inline column
  }
  if (blob_resolver_util::FindInCache(resolved_cache_, column_index) !=
      nullptr) {
    return c;  // already resolved -> slice from cache
  }
  const BlobIndex& blob_index = *blob_index_ptr;
  if (blob_index.IsInlined()) {
    return c;  // TTL-inlined -> ResolveColumnRange caches inlined + slices
  }
  const BlobVerifyPolicy policy = DeriveVerifyPolicy(
      blob_fetcher_.read_options().verify_checksums, force_verify);
  if (policy == BlobVerifyPolicy::kVerifyIfPresent) {
    // Force-verify: served individually to avoid a mixed-verify coalesced read.
    return c;
  }

  const bool same_file = blob_index.IsSameFile();
  // A read is coalesceable only if it is servable the same way
  // ResolveColumnRange would serve it without falling back to a whole
  // individual read: same-file needs the SST reader; separate-file needs a
  // Version-backed range-capable fetcher (SupportsRangeRead() also excludes
  // direct-write fallback mode, whose reads are not manifest-visible and cannot
  // go through Version::MultiGetBlobLazy).
  const bool servable = same_file ? (same_file_reader_ != nullptr)
                                  : blob_fetcher_.SupportsRangeRead();
  if (!servable) {
    return c;  // kServeIndividually
  }

  if (blob_index.compression() == kNoCompression) {
    const uint64_t value_size = blob_index.size();
    const bool strict_subrange = range_offset > 0 || range_length < value_size;
    if (strict_subrange) {
      if (range_offset >= value_size || range_length == 0) {
        // Empty range -> ResolveColumnRange returns empty with no I/O.
        return c;  // kServeIndividually
      }
      const size_t avail = static_cast<size_t>(value_size - range_offset);
      c.blob_index = &blob_index;
      c.range_offset = range_offset;
      c.range_length = range_length > avail ? avail : range_length;
      c.plan = same_file ? LazyColumnReadPlan::kFetchRangeSameFile
                         : LazyColumnReadPlan::kFetchRangeSeparateFile;
      return c;
    }
  }

  // Whole fetch (whole-column read, or a compressed reference resolved whole).
  c.blob_index = &blob_index;
  c.plan = same_file ? LazyColumnReadPlan::kFetchWholeSameFile
                     : LazyColumnReadPlan::kFetchWholeSeparateFile;
  return c;
}

void ReadPathBlobResolver::AdoptResolvedWholeColumn(size_t column_index,
                                                    PinnableSlice&& value) {
  if (blob_resolver_util::FindInCache(resolved_cache_, column_index) !=
      nullptr) {
    return;  // already resolved; keep the existing entry (idempotent)
  }
  resolved_cache_.emplace_back(
      column_index, std::make_unique<PinnableSlice>(std::move(value)));
}

Status ReadPathBlobResolver::ResolveColumns(
    const std::vector<size_t>& column_indices,
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

Status ReadPathBlobResolver::ResolveAllColumns() {
  assert(columns_);

  if (!blob_columns_) {
    return Status::OK();
  }

  for (const auto& blob_col : *blob_columns_) {
    // ResolveColumn internally checks the cache, so we can just call it
    // directly; it will no-op for already resolved columns.
    Slice resolved_value;
    Status s = ResolveColumn(blob_col.first, &resolved_value);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

bool ReadPathBlobResolver::IsUnresolvedColumn(size_t column_index) const {
  if (!columns_ || column_index >= columns_->size()) {
    return false;
  }

  if (!blob_resolver_util::IsBlobColumnIndex(blob_columns_, column_index)) {
    return false;
  }

  return blob_resolver_util::FindInCache(resolved_cache_, column_index) ==
         nullptr;
}

bool ReadPathBlobResolver::HasUnresolvedColumns() const {
  if (!blob_columns_ || blob_columns_->empty()) {
    return false;
  }

  for (const auto& blob_col : *blob_columns_) {
    if (blob_resolver_util::FindInCache(resolved_cache_, blob_col.first) ==
        nullptr) {
      return true;
    }
  }

  return false;
}

size_t ReadPathBlobResolver::NumColumns() const {
  if (!columns_) {
    return 0;
  }
  return columns_->size();
}

}  // namespace ROCKSDB_NAMESPACE
