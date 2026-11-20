//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <limits>
#include <string>

#include "db/blob/blob_index.h"
#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "rocksdb/slice.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE {

// Wraps a block-based table iterator and resolves same-file ("embedded") blob
// references so that callers never see an unresolved same-file BlobIndex. It
// makes an embedded entry look like an ordinary key/value entry:
//   - key(): a whole-value same-file BlobIndex's internal key has its value
//     type rewritten from kTypeBlobIndex back to kTypeValue.
//   - value(): the referenced same-file blob payload is materialized (a wide-
//     column entity's same-file blob columns are rewritten inline).
// Traditional (separate-file) BlobIndex entries and plain entries pass through
// untouched.
//
// Why a wrapper instead of putting this in BlockBasedTableIterator:
//   - The wrapped iterator is created with allow_unprepared_value=false, so it
//     always materializes the data block and never sits in the "first key from
//     index" deferred state. That keeps the "never expose an unresolved
//     same-file blob index" invariant structural rather than relying on every
//     downstream consumer re-checking the key type after PrepareValue().
//   - For lazy callers (allow_unprepared_value=true) the wrapper preserves
//     value laziness: key() does only the cheap key-type rewrite (no
//     blob-region I/O) and the payload is read lazily in value()/PrepareValue()
//     (such callers must honor PrepareValue()'s result). For eager callers
//     (allow_unprepared_value=false, e.g. compaction) the wrapper resolves the
//     value during positioning, so a resolution error (blob-region I/O or
//     corruption) surfaces via status()/Valid() BEFORE value() is consumed and
//     value() never exposes an unresolved same-file BlobIndex.
//   - It keeps the embedded-blob concern out of the hot, complex
//     BlockBasedTableIterator, and is only instantiated for SSTs that actually
//     carry an embedded blob segment.
//
// Only created when the table advertises an embedded blob segment and the
// caller is not the SST dump tool (which must keep seeing raw BlobIndex
// values).
//
// kAllowUnpreparedValue mirrors the caller's allow_unprepared_value and is a
// template parameter so the lazy (hot, user-iteration) path carries no
// eager-resolution branch: when false (eager, e.g. compaction) the wrapper
// resolves during positioning; when true (lazy) it resolves in
// value()/PrepareValue(). Prefer the EagerEmbeddedBlobResolvingIterator /
// LazyEmbeddedBlobResolvingIterator aliases (below) at call sites.
template <bool kAllowUnpreparedValue>
class EmbeddedBlobResolvingIterator : public InternalIterator {
 public:
  // `iter` is owned by this wrapper. `arena_mode` must match how `iter` (and
  // this wrapper) were allocated so the destructor frees `iter` correctly.
  EmbeddedBlobResolvingIterator(const BlockBasedTable* table,
                                const ReadOptions& read_options,
                                InternalIterator* iter, bool arena_mode)
      : table_(table),
        read_options_(read_options),
        iter_(iter),
        arena_mode_(arena_mode) {
    status_.PermitUncheckedError();
  }

  ~EmbeddedBlobResolvingIterator() override {
    if (arena_mode_) {
      iter_->~InternalIteratorBase<Slice>();
    } else {
      delete iter_;
    }
  }

  // No copying allowed
  EmbeddedBlobResolvingIterator(const EmbeddedBlobResolvingIterator&) = delete;
  EmbeddedBlobResolvingIterator& operator=(
      const EmbeddedBlobResolvingIterator&) = delete;
  EmbeddedBlobResolvingIterator(EmbeddedBlobResolvingIterator&&) = delete;
  EmbeddedBlobResolvingIterator& operator=(EmbeddedBlobResolvingIterator&&) =
      delete;

  bool Valid() const override { return iter_->Valid() && status_.ok(); }

  void SeekToFirst() override {
    ResetState();
    iter_->SeekToFirst();
    MaybeEagerlyMaterialize();
  }
  void SeekToLast() override {
    ResetState();
    iter_->SeekToLast();
    MaybeEagerlyMaterialize();
  }
  void Seek(const Slice& target) override {
    ResetState();
    iter_->Seek(target);
    MaybeEagerlyMaterialize();
  }
  void SeekForPrev(const Slice& target) override {
    ResetState();
    iter_->SeekForPrev(target);
    MaybeEagerlyMaterialize();
  }
  void Next() override {
    ResetState();
    iter_->Next();
    MaybeEagerlyMaterialize();
  }
  bool NextAndGetResult(IterateResult* result) override {
    ResetState();
    iter_->Next();
    if (!Valid()) {
      return false;
    }
    // For eager callers, resolve now so a resolution error is visible via
    // Valid()/status() before value() is read.
    MaybeEagerlyMaterialize();
    if (!Valid()) {
      return false;
    }
    result->key = key();
    result->bound_check_result = iter_->UpperBoundCheckResult();
    // Eager callers (kAllowUnpreparedValue=false) get a fully-resolved value;
    // lazy callers must call PrepareValue().
    result->value_prepared = !kAllowUnpreparedValue;
    // key()/MaybeEagerlyMaterialize() may have set a status while resolving.
    return Valid();
  }
  void Prev() override {
    ResetState();
    iter_->Prev();
    MaybeEagerlyMaterialize();
  }

  Slice key() const override {
    assert(Valid());
    if (ResolveKeyType() && key_resolved_) {
      return Slice(resolved_internal_key_);
    }
    // ResolveKeyType() may have set a (corruption-only, no I/O) error. This
    // const accessor falls back to the raw key; the error is still surfaced
    // through status()/Valid(), so permit it from being unchecked here.
    status_.PermitUncheckedError();
    return iter_->key();
  }

  Slice user_key() const override { return ExtractUserKey(key()); }

  Slice value() const override {
    assert(Valid());
    if (MaterializeValue() && value_resolved_) {
      // The resolved value -- a whole-value blob payload (pinned in the blob
      // cache or an owned buffer) or a rebuilt wide-column value (an owned
      // buffer) -- lives in resolved_pinned_value_ and is returned without a
      // copy.
      return Slice(resolved_pinned_value_);
    }
    // MaterializeValue() may have set an error (corruption or blob-region I/O).
    // Fall back to the raw value; the error is surfaced via status()/Valid().
    status_.PermitUncheckedError();
    return iter_->value();
  }

  bool PrepareValue() override {
    assert(Valid());
    if (!iter_->PrepareValue()) {
      return false;
    }
    return MaterializeValue();
  }

  Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    return iter_->status();
  }

  uint64_t write_unix_time() const override { return iter_->write_unix_time(); }

  bool IsKeyPinned() const override {
    // The rewritten key is owned by this wrapper, so it is not pinned.
    if (key_resolved_) {
      return false;
    }
    return iter_->IsKeyPinned();
  }
  bool IsValuePinned() const override {
    if (value_resolved_) {
      // The resolved value -- a whole-value blob payload (pinned in the blob
      // cache or an owned buffer) or a rebuilt wide-column value (an owned
      // buffer) -- lives in resolved_pinned_value_. The IsValuePinned()
      // contract requires the value to stay valid until the iterator is deleted
      // / ReleasePinnedData is called, so only advertise it as pinned when a
      // PinnedIteratorsManager is active to take over the pin's cleanup across
      // repositioning (see ResetState).
      return value_is_pinned_ && pinned_iters_mgr_ != nullptr &&
             pinned_iters_mgr_->PinningEnabled();
    }
    return iter_->IsValuePinned();
  }

  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    iter_->SetPinnedItersMgr(pinned_iters_mgr);
  }

  IterBoundCheck UpperBoundCheckResult() override {
    return iter_->UpperBoundCheckResult();
  }
  bool MayBeOutOfLowerBound() override { return iter_->MayBeOutOfLowerBound(); }

  void SetRangeDelReadSeqno(SequenceNumber read_seqno) override {
    iter_->SetRangeDelReadSeqno(read_seqno);
  }
  bool IsDeleteRangeSentinelKey() const override {
    return iter_->IsDeleteRangeSentinelKey();
  }

  void GetReadaheadState(ReadaheadFileInfo* readahead_file_info) override {
    iter_->GetReadaheadState(readahead_file_info);
  }
  void SetReadaheadState(ReadaheadFileInfo* readahead_file_info) override {
    iter_->SetReadaheadState(readahead_file_info);
  }

  Status GetProperty(std::string prop_name, std::string* prop) override {
    return iter_->GetProperty(std::move(prop_name), prop);
  }

  void Prepare(const MultiScanArgs* scan_opts) override {
    iter_->Prepare(scan_opts);
  }

 private:
  // Cleanup for a heap-allocated std::string holding a rebuilt (wide-column)
  // value pinned into resolved_pinned_value_.
  static void ReleaseResolvedValueBuffer(void* arg1, void* /*arg2*/) {
    delete static_cast<std::string*>(arg1);
  }

  // For eager callers (kAllowUnpreparedValue=false), resolve the current
  // entry's value during positioning. This makes a resolution error (blob
  // I/O or corruption) observable through status()/Valid() before value() is
  // consumed, upholding the "callers never see an unresolved same-file
  // BlobIndex" invariant even on error. Compiled out for lazy callers, which
  // resolve in value()/PrepareValue() and must honor PrepareValue()'s result.
  void MaybeEagerlyMaterialize() {
    if constexpr (!kAllowUnpreparedValue) {
      if (iter_->Valid()) {
        MaterializeValue();
      }
    }
  }

  void ResetState() {
    status_.PermitUncheckedError();
    status_ = Status::OK();
    key_prepared_ = false;
    value_prepared_ = false;
    key_resolved_ = false;
    value_resolved_ = false;
    resolved_internal_key_.clear();
    if (value_is_pinned_) {
      // If a PinnedIteratorsManager is active it has been told (via
      // IsValuePinned) that the previous entry's pinned value stays valid until
      // ReleasePinnedData; hand the pin's cleanup to it so the slice outlives
      // this reposition. Otherwise just release the pin now.
      if (pinned_iters_mgr_ != nullptr && pinned_iters_mgr_->PinningEnabled()) {
        resolved_pinned_value_.DelegateCleanupsTo(pinned_iters_mgr_);
      }
      value_is_pinned_ = false;
    }
    resolved_pinned_value_.Reset();
  }

  // Cheap, key-only resolution: for a whole-value same-file BlobIndex entry,
  // computes the logical internal key with its value type rewritten from
  // kTypeBlobIndex to kTypeValue. Only touches data already in the data block
  // (the key and the inline serialized BlobIndex) -- it does NOT read the blob
  // region. No-op (returns true) for entries that are not a same-file
  // BlobIndex. Returns false and sets `status_` (corruption only) on a
  // malformed key/BlobIndex.
  bool ResolveKeyType() const {
    if (!iter_->Valid()) {
      return true;
    }
    if (key_prepared_) {
      return status_.ok();
    }
    key_prepared_ = true;

    const Slice current_key = iter_->key();
    if (ExtractValueType(current_key) != kTypeBlobIndex) {
      return true;
    }

    ParsedInternalKey parsed_key;
    // Only data-corruption errors are possible here (no I/O).
    status_ = ParseInternalKey(current_key, &parsed_key, false /* log_err */);
    if (!status_.ok()) {
      return false;
    }

    BlobIndex blob_index;
    // Decodes the inline BlobIndex from the data block; still no blob-region
    // IO.
    status_ = blob_index.DecodeFrom(iter_->value());
    if (!status_.ok()) {
      return false;
    }
    if (!blob_index.IsSameFile()) {
      // Traditional (separate-file) BlobDB index; leave the key type as
      // kTypeBlobIndex for the higher layers to resolve.
      return true;
    }

    InternalKey resolved_key(parsed_key.user_key, parsed_key.sequence,
                             kTypeValue);
    const Slice encoded_resolved_key = resolved_key.Encode();
    resolved_internal_key_.assign(encoded_resolved_key.data(),
                                  encoded_resolved_key.size());
    key_resolved_ = true;
    return true;
  }

  // Heavyweight value resolution: materializes the logical value for the
  // current entry. For a same-file BlobIndex entry this reads the blob payload
  // (may do I/O); for a wide-column entity it rewrites same-file blob columns
  // inline. For a BlobIndex entry it first resolves the key type so key() and
  // value() agree. No-op (returns true) for plain entries. Returns false and
  // sets `status_` (corruption or I/O error) on failure.
  bool MaterializeValue() const {
    if (!iter_->Valid()) {
      return true;
    }

    const Slice current_key = iter_->key();
    const ValueType value_type = ExtractValueType(current_key);
    if (value_type != kTypeBlobIndex && value_type != kTypeWideColumnEntity) {
      return true;
    }

    if (value_type == kTypeBlobIndex && !ResolveKeyType()) {
      return false;
    }
    if (value_prepared_) {
      return status_.ok();
    }
    value_prepared_ = true;

    std::string resolved_key;
    std::string resolved_value;
    bool resolved = false;
    bool value_pinned = false;
    status_ = table_->MaybeResolveEmbeddedValue(
        read_options_, current_key, iter_->value(), &resolved_key,
        &resolved_value, &resolved, &resolved_pinned_value_, &value_pinned);
    if (!status_.ok()) {
      return false;
    }
    if (!resolved) {
      return true;
    }

    if (!resolved_key.empty() && !key_resolved_) {
      resolved_internal_key_ = std::move(resolved_key);
      key_resolved_ = true;
    }
    if (!value_pinned) {
      // Built (wide-column) value: move it into a heap buffer and pin it into
      // resolved_pinned_value_ so the value is pinnable like a whole-value
      // blob. DBIter requires a pinned value to back up over an entry
      // (Prev/SeekForPrev); the buffer's cleanup is handed to the
      // PinnedIteratorsManager across repositioning (see ResetState).
      auto* owned_value = new std::string(std::move(resolved_value));
      resolved_pinned_value_.PinSlice(Slice(*owned_value),
                                      &ReleaseResolvedValueBuffer, owned_value,
                                      nullptr);
    }
    // Either way the resolved value now lives, pinned, in
    // resolved_pinned_value_.
    value_is_pinned_ = true;
    value_resolved_ = true;
    return true;
  }

  const BlockBasedTable* table_;
  const ReadOptions& read_options_;
  InternalIterator* iter_;
  const bool arena_mode_;
  PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;

  // Cached resolution state for the current entry, reset on every reposition.
  // Mutable because resolution is driven lazily from the const key()/value()
  // accessors.
  mutable Status status_;
  mutable std::string resolved_internal_key_;
  // Holds the resolved value for the current entry without a copy: a
  // whole-value same-file blob payload (pinned in the blob cache, or an owned
  // buffer when no cache is configured) or a rebuilt wide-column value (an
  // owned heap buffer). Populated whenever value_is_pinned_ is true.
  mutable PinnableSlice resolved_pinned_value_;
  mutable bool key_prepared_ = false;
  mutable bool value_prepared_ = false;
  mutable bool key_resolved_ = false;
  mutable bool value_resolved_ = false;
  mutable bool value_is_pinned_ = false;
};

// Eager variant (allow_unprepared_value=false, e.g. compaction): resolves the
// value during positioning so a resolution error surfaces via status()/Valid()
// before value() is consumed.
using EagerEmbeddedBlobResolvingIterator = EmbeddedBlobResolvingIterator<false>;
// Lazy variant (allow_unprepared_value=true, e.g. user iteration): resolves in
// value()/PrepareValue(); callers must honor PrepareValue()'s result.
using LazyEmbeddedBlobResolvingIterator = EmbeddedBlobResolvingIterator<true>;

}  // namespace ROCKSDB_NAMESPACE
