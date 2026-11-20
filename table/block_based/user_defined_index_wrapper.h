//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <memory>
#include <string>

#include "db/seqno_to_time_mapping.h"
#include "rocksdb/index_factory.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_type.h"
#include "table/block_based/index_builder.h"

namespace ROCKSDB_NAMESPACE {

// ---------------------------------------------------------------------------
// Adapter classes for custom IndexFactory implementations.
//
// IndexFactoryIteratorWrapper adapts IndexFactoryIterator (public interface,
// user keys) to InternalIteratorBase<IndexValue> (internal interface, internal
// keys). This adapter is necessary because BlockBasedTableIterator expects
// the internal iterator interface.
//
// IndexFactoryReaderWrapper dispatches NewIterator calls to either the
// standard IndexReader or the custom IndexFactoryReader based on the
// index_mode config (kCustomDefault/kCustomOnly) or ReadOptions::read_index.
// ---------------------------------------------------------------------------

// Forward declaration for the reader wrapper.
class IndexFactoryIteratorWrapper;

class IndexFactoryIteratorWrapper : public InternalIteratorBase<IndexValue> {
 public:
  explicit IndexFactoryIteratorWrapper(
      std::unique_ptr<IndexFactoryIterator>&& udi_iter)
      : udi_iter_(std::move(udi_iter)), valid_(false) {}

  ~IndexFactoryIteratorWrapper() override = default;

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    status_ = udi_iter_->SeekToFirstAndGetResult(&result_);
    UpdateValidAndKey();
  }

  void SeekToLast() override {
    status_ = udi_iter_->SeekToLastAndGetResult(&result_);
    UpdateValidAndKey();
  }

  void Seek(const Slice& target) override {
    // Per-read hot path. The target is a well-formed internal key; skip
    // ParseInternalKey validation and use the inline extractors. The
    // 8-byte footer already matches SeekContext::target_tag's encoding.
    if (UNLIKELY(target.size() < kNumInternalBytes)) {
      status_ = Status::Corruption("Invalid internal key (too short)");
      UpdateValidAndKey();
      return;
    }
    IndexFactoryIterator::SeekContext ctx;
    ctx.target_tag = ExtractInternalKeyFooter(target);
    status_ =
        udi_iter_->SeekAndGetResult(ExtractUserKey(target), &result_, ctx);
    UpdateValidAndKey();
  }

  void Next() override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    UpdateValidAndKey();
  }

  bool NextAndGetResult(IterateResult* result) override {
    status_ = udi_iter_->NextAndGetResult(&result_);
    UpdateValidAndKey();
    if (status_.ok()) {
      if (valid_) {
        result->key = key();
      }
      result->bound_check_result = result_.bound_check_result;
      result->value_prepared = result_.value_prepared;
    }
    return valid_;
  }

  void SeekForPrev(const Slice& /*target*/) override {
    // BlockBasedTableIterator never calls SeekForPrev on an index
    // iterator (it uses Seek + FindKeyBackward instead).
    valid_ = false;
    status_ = Status::NotSupported("SeekForPrev not supported");
  }

  void Prev() override {
    status_ = udi_iter_->PrevAndGetResult(&result_);
    UpdateValidAndKey();
  }

  Slice key() const override { return Slice(*ikey_.const_rep()); }

  IndexValue value() const override { return cached_value_; }

  Status status() const override { return status_; }

  void Prepare(const MultiScanArgs* scan_opts) override {
    if (scan_opts) {
      udi_iter_->Prepare(scan_opts->GetScanRanges().data(),
                         scan_opts->GetScanRanges().size());
    }
  }

  IterBoundCheck UpperBoundCheckResult() override {
    return result_.bound_check_result;
  }

 private:
  // Refresh valid_, ikey_, and cached_value_ from result_ after every UDI
  // positioning operation.
  void UpdateValidAndKey() {
    if (status_.ok()) {
      // kUnknown and kOutOfBound both mean no valid position.
      valid_ = result_.bound_check_result == IterBoundCheck::kInbound;
      if (valid_) {
        SetInternalKeyFromUDIResult();
      }
    } else {
      valid_ = false;
    }
  }

  // Build the internal-key form of the current separator. UDI returns
  // user keys; the index iterator contract requires internal keys.
  // seq=0 / kTypeValue gives the correct upper-bound semantics for a
  // separator: any data key with the same user key compares less.
  void SetInternalKeyFromUDIResult() {
    ikey_.Set(result_.key, 0, ValueType::kTypeValue);
    CacheCurrentValue();
  }

  // Cache so repeated value() calls (multiple per block in
  // BlockBasedTableIterator) avoid a virtual dispatch.
  void CacheCurrentValue() {
    auto handle = udi_iter_->value();
    cached_value_ =
        IndexValue(BlockHandle(handle.offset, handle.size), Slice());
  }

  std::unique_ptr<IndexFactoryIterator> udi_iter_;
  IterateResult result_;
  InternalKey ikey_;
  IndexValue cached_value_;
  Status status_;
  bool valid_;
};

class IndexFactoryReaderWrapper : public BlockBasedTable::IndexReader {
 public:
  // @udi_is_primary: use UDI for all reads (default dispatch), including
  //   internal operations like compaction and VerifyChecksum that don't
  //   set ReadOptions::read_index.
  IndexFactoryReaderWrapper(
      const std::string& name,
      std::unique_ptr<BlockBasedTable::IndexReader>&& reader,
      std::unique_ptr<IndexFactoryReader>&& udi_reader, bool udi_is_primary,
      bool standard_index_is_stub, size_t index_block_size)
      : name_(name),
        reader_(std::move(reader)),
        udi_reader_(std::move(udi_reader)),
        udi_is_primary_(udi_is_primary),
        standard_index_is_stub_(standard_index_is_stub),
        index_block_size_(index_block_size) {}

  InternalIteratorBase<IndexValue>* NewIterator(
      const ReadOptions& read_options, bool disable_prefix_seek,
      IndexBlockIter* iter, GetContext* get_context,
      BlockCacheLookupContext* lookup_context) override {
    // kDefault       -> follow udi_is_primary_. A legacy factory request can
    //                   select a secondary custom index.
    // kBuiltin       -> standard index.
    // kPreferCustom  -> custom index. The "prefer" fallback runs at SST
    //                   open: SSTs lacking a UDI block don't get this
    //                   wrapper, so they reach the standard reader
    //                   directly. Once the wrapper is installed, the
    //                   selection is strict (failures surface as
    //                   Status::Corruption).
    bool use_udi;
    switch (read_options.read_index) {
      case ReadOptions::ReadIndex::kBuiltin:
        use_udi = false;
        break;
      case ReadOptions::ReadIndex::kPreferCustom:
        use_udi = true;
        break;
      case ReadOptions::ReadIndex::kDefault:
      default:
        // An SST written in kCustomOnly has an empty stub where the
        // standard index would be, so the custom index is the only way to
        // read it. Route to it even when the current index_mode says the
        // standard index is primary, so lowering index_mode does not brick
        // reads of SSTs already on disk. An explicit kBuiltin request
        // still fails below.
        use_udi = udi_is_primary_ || standard_index_is_stub_;
        break;
    }
    if (read_options.read_index != ReadOptions::ReadIndex::kBuiltin &&
        !(read_options.read_index == ReadOptions::ReadIndex::kDefault &&
          use_udi) &&
        read_options.table_index_factory != nullptr) {
      const char* requested_name = read_options.table_index_factory->Name();
      if (name_ == requested_name) {
        use_udi = true;
      } else {
        return NewErrorInternalIterator<IndexValue>(Status::InvalidArgument(
            "Bad index name: " + std::string(requested_name) +
            ". Only supported UDI is " + name_));
      }
    }

    if (use_udi) {
      std::unique_ptr<IndexFactoryIterator> udi_iter =
          udi_reader_->NewIterator(read_options);
      if (udi_iter) {
        return new IndexFactoryIteratorWrapper(std::move(udi_iter));
      }
      return NewErrorInternalIterator<IndexValue>(
          Status::Corruption("Could not create UDI iterator"));
    }
    if (standard_index_is_stub_) {
      // Only reachable via an explicit read_index=kBuiltin request.
      return NewErrorInternalIterator<IndexValue>(Status::InvalidArgument(
          "Cannot read via the standard index because this SST was written "
          "with only a custom index"));
    }

    return reader_->NewIterator(read_options, disable_prefix_seek, iter,
                                get_context, lookup_context);
  }

  Status CacheDependencies(const ReadOptions& ro, bool pin,
                           FilePrefetchBuffer* tail_prefetch_buffer) override {
    // The standard index is fully populated in kStandardDefault,
    // kStandardRequired, and kCustomDefault modes. In kCustomOnly, it is an
    // empty stub.
    return reader_->CacheDependencies(ro, pin, tail_prefetch_buffer);
  }

  size_t ApproximateMemoryUsage() const override {
    size_t usage = udi_reader_->ApproximateMemoryUsage();
    if (udi_reader_->MemoryUsageIncludesIndexBlock()) {
      // The block's owner charges the serialized bytes, so drop them from an
      // inclusive estimate. Saturate for readers that underestimate.
      usage -= std::min(usage, index_block_size_);
    }
    usage += reader_->ApproximateMemoryUsage();
    return usage;
  }

  void EraseFromCacheBeforeDestruction(
      uint32_t uncache_aggressiveness) override {
    reader_->EraseFromCacheBeforeDestruction(uncache_aggressiveness);
  }

 private:
  const std::string name_;
  std::unique_ptr<BlockBasedTable::IndexReader> reader_;
  std::unique_ptr<IndexFactoryReader> udi_reader_;
  const bool udi_is_primary_;
  const bool standard_index_is_stub_;
  const size_t index_block_size_;
};

}  // namespace ROCKSDB_NAMESPACE
