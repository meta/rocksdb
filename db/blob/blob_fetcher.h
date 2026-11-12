//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <utility>

#include "db/blob/blob_constants.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class BlobFileCache;
class Version;
class Slice;
class FilePrefetchBuffer;
class PinnableSlice;
class BlobIndex;
class SameFileBlobReader;

// Abstract interface for blob retrieval on the read path. An implementation
// resolves a BlobIndex to its value, pinning it into a PinnableSlice. Callers
// hold a plain const BlobFetcher* so resolution can be intercepted by a
// decorator (e.g. EmbeddedAwareBlobFetcher) without their knowledge.
class BlobFetcher {
 public:
  virtual ~BlobFetcher() = default;

  // Convenience overload: decodes `blob_index_slice` into a BlobIndex and
  // dispatches to the virtual overload below. Non-virtual; shared by all
  // implementations.
  Status FetchBlob(const Slice& user_key, const Slice& blob_index_slice,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value, uint64_t* bytes_read) const;

  virtual Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                           FilePrefetchBuffer* prefetch_buffer,
                           PinnableSlice* blob_value,
                           uint64_t* bytes_read) const = 0;

  // The read options this fetcher operates under. Used (via an
  // EmbeddedAwareBlobFetcher decorator) to resolve same-file blob references,
  // which need the current read_tier.
  virtual const ReadOptions& read_options() const = 0;
};

// Shared implementation of the two Version-backed BlobFetchers below. Reads
// through a Version, optionally falling back to direct-write blob files that
// are not yet manifest-visible. Everything except the ReadOptions is stored
// here; the concrete subclass decides whether it references a caller-owned
// ReadOptions (VersionBlobFetcher) or owns its own copy
// (OwningVersionBlobFetcher). FetchBlob reads the options via the virtual
// read_options() so both storage strategies share one implementation.
class VersionBlobFetcherBase : public BlobFetcher {
 public:
  // Un-hide the base's Slice-based convenience overload.
  using BlobFetcher::FetchBlob;

  Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value,
                   uint64_t* bytes_read) const override;

  // Fetches the value of a separate-file blob reference into *blob_value,
  // applying `verify_policy` (see BlobVerifyPolicy). range_length ==
  // kWholeBlobLength reads the whole value (delegating to FetchBlob, which also
  // handles the direct-write fallback, and forcing whole-record checksum
  // verification for kVerifyIfPresent even when ReadOptions::verify_checksums
  // is off). Any other length reads only the sub-range [range_offset,
  // range_offset + range_length) directly from the blob file, reading just
  // those bytes on a cache miss -- no record-header/key read, no whole-record
  // checksum, no cache fill (see Version::GetBlobRange /
  // BlobSource::GetBlobRange). A strict sub-range is only valid when
  // SupportsRangeRead() is true and verify_policy != kVerifyIfPresent
  // (verifying a strict sub-range would have to amplify the read to the whole
  // record); callers check that first and take the whole-value path otherwise.
  // Not routed through the same-file (embedded) decorator -- same-file
  // references are resolved by the caller through a SameFileBlobReader.
  Status FetchBlobRange(const Slice& user_key, const BlobIndex& blob_index,
                        uint64_t range_offset, size_t range_length,
                        BlobVerifyPolicy verify_policy,
                        PinnableSlice* blob_value, uint64_t* bytes_read) const;

  // True if this fetcher has enough context to resolve a (non-inlined,
  // non-same-file) blob reference: either a Version to read through, or the
  // direct-write fallback (a blob file cache) enabled. Callers that may hold a
  // null Version (e.g. DBIter) can check this to surface a clean Corruption on
  // an unexpected blob index instead of dereferencing null in FetchBlob.
  bool CanResolve() const {
    return version_ != nullptr ||
           (allow_write_path_fallback_ && blob_file_cache_ != nullptr);
  }

  // True if this fetcher can serve an I/O-saving byte-range read: it reads
  // through a Version (FetchBlobRange goes to Version::GetBlobRange) and is not
  // in direct-write fallback mode (whose reads resolve whole records instead).
  bool SupportsRangeRead() const {
    return version_ != nullptr && !allow_write_path_fallback_;
  }

  // The Version this fetcher reads separate-file blob references through (may
  // be null for a direct-write-fallback-only fetcher). Used by the cross-key
  // lazy coalescing path to group and dispatch reads via
  // Version::MultiGetBlobLazy.
  const Version* version() const { return version_; }
  void SetVersion(const Version* version) { version_ = version; }

 protected:
  VersionBlobFetcherBase(const Version* version, BlobFileCache* blob_file_cache,
                         bool allow_write_path_fallback)
      : version_(version),
        blob_file_cache_(blob_file_cache),
        allow_write_path_fallback_(allow_write_path_fallback) {}

  const Version* version_;
  BlobFileCache* blob_file_cache_;
  bool allow_write_path_fallback_;
};

// The default BlobFetcher for transient/stack use: holds a reference to a
// caller-owned ReadOptions rather than a copy, avoiding a per-fetcher copy of
// the (growing) ReadOptions on the hot path. The referenced ReadOptions must
// outlive the fetcher, so this is only appropriate for fetchers whose lifetime
// is bounded by the caller's (e.g. a stack local per Get / per resolution).
// Use OwningVersionBlobFetcher for lifetime-independent fetchers.
class VersionBlobFetcher : public VersionBlobFetcherBase {
 public:
  VersionBlobFetcher(const Version* version, const ReadOptions& read_options,
                     BlobFileCache* blob_file_cache = nullptr,
                     bool allow_write_path_fallback = false)
      : VersionBlobFetcherBase(version, blob_file_cache,
                               allow_write_path_fallback),
        read_options_(read_options) {}

  // Reject binding to a temporary ReadOptions, which would leave a dangling
  // reference. Callers that cannot guarantee the ReadOptions outlives the
  // fetcher must use OwningVersionBlobFetcher instead.
  VersionBlobFetcher(const Version* version, ReadOptions&& read_options,
                     BlobFileCache* blob_file_cache = nullptr,
                     bool allow_write_path_fallback = false) = delete;

  const ReadOptions& read_options() const override { return read_options_; }

 private:
  const ReadOptions& read_options_;
};

// A BlobFetcher that owns its ReadOptions, for fetchers whose lifetime is
// independent of the caller that created them (the lazy read-path resolver and
// compaction). Takes ownership of the ReadOptions by rvalue so ownership
// transfer is explicit and no hidden copy is made on the way in; later reads
// never touch caller state. Callers holding a borrowed ReadOptions they cannot
// move must make the copy explicit (e.g. ReadOptions(read_options)).
class OwningVersionBlobFetcher : public VersionBlobFetcherBase {
 public:
  OwningVersionBlobFetcher(const Version* version, ReadOptions&& read_options,
                           BlobFileCache* blob_file_cache = nullptr,
                           bool allow_write_path_fallback = false)
      : VersionBlobFetcherBase(version, blob_file_cache,
                               allow_write_path_fallback),
        read_options_(std::move(read_options)) {}

  const ReadOptions& read_options() const override { return read_options_; }

 private:
  ReadOptions read_options_;
};

// A BlobFetcher decorator that resolves same-file ("embedded") blob references
// against a SameFileBlobReader (the current SST) and delegates all other
// references to a base BlobFetcher. It is cheap to construct as a stack local
// per resolution: the same-file target is per-SST while the base fetcher is one
// per-Get object shared across SSTs, so an immutable per-SST decorator avoids
// shared-mutable-state hazards under async MultiGet.
//
// A decorator is "enabled" when it has a same-file reader and "disabled"
// (unusable) when constructed with a null `same_file_reader` (e.g. via the
// default constructor). A disabled decorator must not be routed through
// directly; callers construct one unconditionally and use EffectiveFetcher(),
// which returns this decorator when enabled or the undecorated base fetcher
// otherwise. This avoids std::optional / branching at call sites while adding
// no indirection on the common (non-embedded) path.
//
// The base fetcher may be null for an entity whose blob columns are all
// same-file (no separate-file blob support wired, e.g. SstFileReader); a
// non-same-file reference then surfaces as a Corruption rather than silently.
class EmbeddedAwareBlobFetcher : public BlobFetcher {
 public:
  explicit EmbeddedAwareBlobFetcher(
      const BlobFetcher* base = nullptr,
      const SameFileBlobReader* same_file_reader = nullptr)
      : base_(base), same_file_reader_(same_file_reader) {}

  // The fetcher to route resolution through: this decorator when it has a
  // same-file reader, otherwise the (undecorated) base fetcher. Only an enabled
  // decorator's FetchBlob is ever invoked.
  const BlobFetcher* EffectiveFetcher() const {
    return same_file_reader_ != nullptr ? this : base_;
  }

  // Un-hide the base's Slice-based convenience overload.
  using BlobFetcher::FetchBlob;

  Status FetchBlob(const Slice& user_key, const BlobIndex& blob_index,
                   FilePrefetchBuffer* prefetch_buffer,
                   PinnableSlice* blob_value,
                   uint64_t* bytes_read) const override;

  const ReadOptions& read_options() const override {
    // Same-file resolution uses the base fetcher's read options; when there is
    // no base (e.g. SstFileReader) a default (kReadAllTier) suffices.
    static const ReadOptions kDefaultReadOptions;
    return base_ != nullptr ? base_->read_options() : kDefaultReadOptions;
  }

 private:
  const BlobFetcher* base_;
  // Null iff this decorator is disabled (see class comment); otherwise the
  // same-file blob reader for the current SST.
  const SameFileBlobReader* same_file_reader_;
};

}  // namespace ROCKSDB_NAMESPACE
