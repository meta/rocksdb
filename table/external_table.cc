//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/external_table.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <unordered_map>

#include "db/dbformat.h"
#include "db/range_tombstone_fragmenter.h"
#include "logging/logging.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/utilities/types_util.h"
#include "table/block_based/block.h"
#include "table/block_based/cachable_entry.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/range_del_block.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/coro_utils.h"
#include "util/string_util.h"

#if USE_COROUTINES
#include "rocksdb/coro_external_table.h"
#endif

namespace ROCKSDB_NAMESPACE {

static_assert(static_cast<uint8_t>(ExternalTableBuilderNewTableReason::kNone) ==
              static_cast<uint8_t>(TableBuilderNewTableReason::kNone));
static_assert(
    static_cast<uint8_t>(
        ExternalTableBuilderNewTableReason::kRowClassificationChanged) ==
    static_cast<uint8_t>(
        TableBuilderNewTableReason::kRowClassificationChanged));
static_assert(
    static_cast<uint8_t>(
        ExternalTableBuilderNewTableReason::kSchemaIdChanged) ==
    static_cast<uint8_t>(TableBuilderNewTableReason::kSchemaIdChanged));
static_assert(
    static_cast<uint8_t>(
        ExternalTableBuilderNewTableReason::kSchemaVersionIncompatible) ==
    static_cast<uint8_t>(
        TableBuilderNewTableReason::kSchemaVersionIncompatible));
static_assert(
    static_cast<uint8_t>(
        ExternalTableBuilderNewTableReason::kUnsupportedEntryType) ==
    static_cast<uint8_t>(TableBuilderNewTableReason::kUnsupportedEntryType));
static_assert(
    static_cast<uint8_t>(ExternalTableBuilderNewTableReason::kBuilderPolicy) ==
    static_cast<uint8_t>(TableBuilderNewTableReason::kBuilderPolicy));

namespace {

Status GetValueType(EntryType entry_type, ValueType* value_type) {
  switch (entry_type) {
    case kEntryPut:
      *value_type = kTypeValue;
      return Status::OK();
    case kEntryDelete:
      *value_type = kTypeDeletion;
      return Status::OK();
    case kEntrySingleDelete:
      *value_type = kTypeSingleDeletion;
      return Status::OK();
    case kEntryMerge:
      *value_type = kTypeMerge;
      return Status::OK();
    case kEntryBlobIndex:
      *value_type = kTypeBlobIndex;
      return Status::OK();
    case kEntryWideColumnEntity:
      *value_type = kTypeWideColumnEntity;
      return Status::OK();
    case kEntryRangeDeletion:
    case kEntryDeleteWithTimestamp:
    case kEntryTimedPut:
      return Status::NotSupported(
          "External table entry type " +
          std::to_string(static_cast<unsigned int>(entry_type)) +
          " is not supported");
    case kEntryOther:
      return Status::Corruption("Invalid external table entry type");
  }
  return Status::Corruption("Invalid external table entry type");
}

// Returns the effective full-mode key after applying any file-wide sequence
// number. effective_entry->user_key aliases stored_internal_key.
Status ParseExternalTableEffectiveEntry(
    const Slice& stored_internal_key,
    const InternalKeyComparator& internal_comparator,
    SequenceNumber global_seqno, ParsedInternalKey* effective_entry) {
  ParsedEntryInfo entry;
  Status status = ParseEntry(stored_internal_key,
                             internal_comparator.user_comparator(), &entry);
  if (!status.ok()) {
    return status;
  }

  ValueType value_type;
  status = GetValueType(entry.type, &value_type);
  if (!status.ok()) {
    return status;
  }

  SequenceNumber sequence = entry.sequence;
  if (global_seqno != kDisableGlobalSequenceNumber) {
    if (sequence != 0) {
      return Status::Corruption(
          "External table entry has a nonzero sequence number in global "
          "sequence number mode");
    }
    sequence = global_seqno;
  }
  *effective_entry = ParsedInternalKey(ExtractUserKey(stored_internal_key),
                                       sequence, value_type);
  return Status::OK();
}

// Leaves lookup_key unchanged unless a global sequence number is active.
// Global-seqno files store sequence zero, so their seek boundary is rewritten;
// value_type selects the forward or reverse boundary. A rewritten lookup_key
// aliases stored_lookup_key.
Status SetExternalTableGlobalSeqnoLookupKey(Slice* lookup_key,
                                            SequenceNumber global_seqno,
                                            ValueType value_type,
                                            IterKey* stored_lookup_key) {
  if (global_seqno == kDisableGlobalSequenceNumber) {
    return Status::OK();
  }

  ParsedInternalKey parsed_lookup_key;
  Status status = ParseInternalKey(*lookup_key, &parsed_lookup_key,
                                   /*log_err_key=*/false);
  if (!status.ok()) {
    return status;
  }
  stored_lookup_key->SetInternalKey(parsed_lookup_key.user_key, 0, value_type);
  *lookup_key = stored_lookup_key->GetInternalKey();
  return Status::OK();
}

// Adapts full-mode reader callbacks to GetContext. Reset() initializes the
// fixed storage used by MultiGet().
class ExternalTableGetContextImpl final : public ExternalTableGetContext {
 public:
  ExternalTableGetContextImpl() = default;

  ExternalTableGetContextImpl(GetContext* get_context,
                              const InternalKeyComparator* comparator,
                              const Slice& lookup_key,
                              SequenceNumber global_seqno) {
    Reset(get_context, comparator, lookup_key, global_seqno);
  }

  void Reset(GetContext* get_context, const InternalKeyComparator* comparator,
             const Slice& lookup_key, SequenceNumber global_seqno) {
    get_context_ = get_context;
    comparator_ = comparator;
    lookup_key_ = lookup_key;
    global_seqno_ = global_seqno;
  }

  Status Save(const Slice& stored_internal_key, PinnableSlice* value,
              bool* continue_reading) override;

 private:
  GetContext* get_context_ = nullptr;
  const InternalKeyComparator* comparator_ = nullptr;
  Slice lookup_key_;
  SequenceNumber global_seqno_ = kDisableGlobalSequenceNumber;
};

Status ExternalTableGetContextImpl::Save(const Slice& stored_internal_key,
                                         PinnableSlice* value,
                                         bool* continue_reading) {
  assert(value != nullptr);
  assert(continue_reading != nullptr);
  *continue_reading = false;

  ParsedInternalKey parsed_entry;
  Status status = ParseExternalTableEffectiveEntry(
      stored_internal_key, *comparator_, global_seqno_, &parsed_entry);
  if (!status.ok()) {
    return status;
  }
  // Applying global_seqno can make this same-user entry newer than the lookup
  // snapshot. Skip it as an effective-key seek would.
  if (global_seqno_ != kDisableGlobalSequenceNumber &&
      comparator_->user_comparator()->EqualWithoutTimestamp(
          parsed_entry.user_key, ExtractUserKey(lookup_key_)) &&
      comparator_->Compare(parsed_entry, lookup_key_) < 0) {
    value->Reset();
    *continue_reading = true;
    return Status::OK();
  }

  bool matched = false;
  Status read_status;
  // GetContext decides whether it needs another older entry, for example to
  // finish resolving a merge chain.
  *continue_reading =
      get_context_->SaveValue(parsed_entry, *value, &matched, &read_status,
                              value->IsPinned() ? value : nullptr);
  value->Reset();
  return read_status;
}

// Keeps translated keys and per-key GetContext adapters alive during a
// full-mode MultiGet, and maps its dense results back to the input range.
class ExternalTableMultiGetContextImpl final
    : public ExternalTableMultiGetContext {
 public:
  ExternalTableMultiGetContextImpl(const MultiGetContext::Range& mget_range,
                                   const InternalKeyComparator& comparator,
                                   SequenceNumber global_seqno) {
    for (auto iter = mget_range.begin(); iter != mget_range.end(); ++iter) {
      Entry& entry = entries_[size_];
      entry.lookup_key = iter->ikey;
      Status status = SetExternalTableGlobalSeqnoLookupKey(
          &entry.lookup_key, global_seqno, kValueTypeForSeek,
          &entry.stored_lookup_key);
      if (!status.ok()) {
        *iter->s = status;
        continue;
      }
      entry.output_status = iter->s;
      entry.get_context.Reset(iter->get_context, &comparator, iter->ikey,
                              global_seqno);
      ++size_;
    }
  }

  size_t Size() const override { return size_; }

  ExternalTableMultiGetRequest GetRequest(size_t index) override {
    Entry& entry = entries_[index];
    return {entry.lookup_key, &entry.get_context};
  }

  void ApplyStatuses(std::vector<Status>* statuses) const {
    assert(statuses->size() == size_);
    for (size_t i = 0; i < size_; ++i) {
      Status& status = (*statuses)[i];
      // NotFound is table-local; let RocksDB continue searching older files.
      *entries_[i].output_status =
          status.IsNotFound() ? Status::OK() : std::move(status);
    }
  }

 private:
  struct Entry {
    Slice lookup_key;
    Status* output_status = nullptr;
    IterKey stored_lookup_key;
    ExternalTableGetContextImpl get_context;
  };

  std::array<Entry, MultiGetContext::MAX_BATCH_SIZE> entries_;
  size_t size_ = 0;
};

// Translates basic user keys and full stored keys into RocksDB's effective
// internal-key order, including global-seqno seek correction.
template <ExternalTableMode Mode>
class ExternalTableIteratorAdapter : public InternalIterator {
 public:
  ExternalTableIteratorAdapter(ExternalTableIteratorBase* iterator,
                               const InternalKeyComparator& internal_comparator,
                               SequenceNumber global_seqno)
      : iterator_(iterator),
        internal_comparator_(internal_comparator),
        global_seqno_(global_seqno),
        valid_(false),
        status_(iterator != nullptr ? iterator->status() : Status::OK()) {}

  // No copying allowed
  ExternalTableIteratorAdapter(const ExternalTableIteratorAdapter&) = delete;
  ExternalTableIteratorAdapter& operator=(const ExternalTableIteratorAdapter&) =
      delete;
  ExternalTableIteratorAdapter(ExternalTableIteratorAdapter&&) = delete;
  ExternalTableIteratorAdapter& operator=(ExternalTableIteratorAdapter&&) =
      delete;

  ~ExternalTableIteratorAdapter() override {}

  bool Valid() const override { return valid_; }

  void SeekToFirst() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToFirst();
      UpdateKey();
    }
  }

  void SeekToLast() override {
    status_ = Status::OK();
    if (iterator_) {
      iterator_->SeekToLast();
      UpdateKey();
    }
  }

  void Seek(const Slice& target) override {
    status_ = Status::OK();
    valid_ = false;
    if (iterator_) {
      if constexpr (Mode == ExternalTableMode::kFull) {
        Slice external_target = target;
        IterKey stored_target;
        status_ = SetExternalTableGlobalSeqnoLookupKey(
            &external_target, global_seqno_, kValueTypeForSeek, &stored_target);
        if (status_.ok()) {
          iterator_->Seek(external_target);
        }
      } else {
        ParsedEntryInfo lookup_key;
        status_ = ParseEntry(target, internal_comparator_.user_comparator(),
                             &lookup_key);
        if (status_.ok()) {
          iterator_->Seek(lookup_key.user_key);
        }
      }
      if (status_.ok()) {
        UpdateKey();
        if (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts ||
            global_seqno_ != kDisableGlobalSequenceNumber) {
          // Advance until the translated result satisfies InternalIterator's
          // Seek() contract.
          while (valid_ && internal_comparator_.Compare(key(), target) < 0) {
            iterator_->Next();
            UpdateKey();
          }
        }
      }
    }
  }

  void SeekForPrev(const Slice& target) override {
    status_ = Status::OK();
    valid_ = false;
    if (iterator_) {
      if constexpr (Mode == ExternalTableMode::kFull) {
        Slice external_target = target;
        IterKey stored_target;
        status_ = SetExternalTableGlobalSeqnoLookupKey(
            &external_target, global_seqno_, kValueTypeForSeekForPrev,
            &stored_target);
        if (status_.ok()) {
          iterator_->SeekForPrev(external_target);
        }
      } else {
        ParsedEntryInfo lookup_key;
        status_ = ParseEntry(target, internal_comparator_.user_comparator(),
                             &lookup_key);
        if (status_.ok()) {
          iterator_->SeekForPrev(lookup_key.user_key);
        }
      }
      if (status_.ok()) {
        UpdateKey();
        if (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts ||
            global_seqno_ != kDisableGlobalSequenceNumber) {
          // Retreat until the translated result satisfies SeekForPrev().
          while (valid_ && internal_comparator_.Compare(key(), target) > 0) {
            iterator_->Prev();
            UpdateKey();
          }
        }
      }
    }
  }

  void Next() override {
    if (iterator_) {
      iterator_->Next();
      UpdateKey();
    }
  }

  bool NextAndGetResult(IterateResult* result) override {
    if (iterator_) {
      valid_ = iterator_->NextAndGetResult(&result_);
      result->value_prepared = result_.value_prepared;
      result->bound_check_result = result_.bound_check_result;
      if (valid_) {
        UpdateKey(result_.key);
        if (valid_) {
          result->key = key();
        }
      } else {
        status_ = iterator_->status();
      }
    } else {
      valid_ = false;
    }
    return valid_;
  }

  bool PrepareValue() override {
    if (iterator_ && !result_.value_prepared) {
      valid_ = iterator_->PrepareValue();
      status_ = iterator_->status();
      if (!status_.ok()) {
        valid_ = false;
      }
      result_.value_prepared = true;
    }
    return valid_;
  }

  IterBoundCheck UpperBoundCheckResult() override {
    if (iterator_) {
      result_.bound_check_result = iterator_->UpperBoundCheckResult();
    }
    return result_.bound_check_result;
  }

  void Prev() override {
    if (iterator_) {
      iterator_->Prev();
      UpdateKey();
    }
  }

  Slice key() const override {
    if (iterator_) {
      return key_.GetInternalKey();
    }
    return Slice();
  }

  Slice value() const override {
    if (iterator_) {
      return iterator_->value();
    }
    return Slice();
  }

  Status status() const override { return status_; }

  void Prepare(const MultiScanArgs* scan_opts) override {
    if (!iterator_) {
      return;
    }
    if constexpr (Mode == ExternalTableMode::kFull) {
      // TODO: Support full-mode MultiScan preparation.
      iterator_->Prepare(nullptr, 0);
    } else if (scan_opts) {
      iterator_->Prepare(scan_opts->GetScanRanges().data(), scan_opts->size());
    } else {
      iterator_->Prepare(nullptr, 0);
    }
  }

 private:
  std::unique_ptr<ExternalTableIteratorBase> iterator_;
  const InternalKeyComparator& internal_comparator_;
  const SequenceNumber global_seqno_;
  IterKey key_;
  bool valid_;
  Status status_;
  IterateResult result_;

  void UpdateKey(OptSlice res = OptSlice()) {
    if (iterator_) {
      valid_ = iterator_->Valid();
      status_ = iterator_->status();
      if (valid_ && status_.ok()) {
        if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
          key_.SetInternalKey(
              res.has_value() ? res.value() : iterator_->key(),
              global_seqno_ == kDisableGlobalSequenceNumber ? 0 : global_seqno_,
              kTypeValue);
          return;
        } else {
          const Slice stored_key =
              res.has_value() ? res.value() : iterator_->key();
          ParsedInternalKey parsed_entry;
          status_ = ParseExternalTableEffectiveEntry(
              stored_key, internal_comparator_, global_seqno_, &parsed_entry);
          if (!status_.ok()) {
            valid_ = false;
            return;
          }
          if (global_seqno_ == kDisableGlobalSequenceNumber) {
            // The external iterator keeps stored_key valid until it moves,
            // matching InternalIterator's borrowed-key lifetime.
            key_.SetInternalKey(stored_key, /*copy=*/false);
          } else {
            key_.SetInternalKey(parsed_entry);
          }
        }
      } else if (!status_.ok()) {
        valid_ = false;
      }
    }
  }
};

// Prefer a RocksDB properties block, but allow the external implementation to
// provide TableProperties directly.
template <ExternalTableMode Mode>
Status LoadExternalTableProperties(
    const ImmutableOptions& ioptions, ExternalTableReaderBase<Mode>* reader,
    std::shared_ptr<const TableProperties>* table_properties) {
  std::unique_ptr<char[]> property_block;
  uint64_t property_block_size = 0;
  Status status =
      reader->GetPropertiesBlock(&property_block, &property_block_size);
  std::shared_ptr<TableProperties> loaded_properties;
  if (status.ok()) {
    auto parsed_properties = std::make_unique<TableProperties>();
    BlockContents block_contents(std::move(property_block),
                                 property_block_size);
    Block block(std::move(block_contents));
    status = ParsePropertiesBlock(ioptions, block, parsed_properties,
                                  nullptr /* global_seqno_value_offset */);
    if (!status.ok()) {
      return status;
    }
    loaded_properties = std::move(parsed_properties);
  } else {
    if (!status.IsNotSupported()) {
      return status;
    }
    loaded_properties =
        std::make_shared<TableProperties>(*reader->GetTableProperties());
  }

  if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
    if (!status.ok()) {
      loaded_properties->key_largest_seqno = 0;
      loaded_properties->key_smallest_seqno = 0;
    }
  }
  *table_properties = std::move(loaded_properties);
  return Status::OK();
}

Status ParseRangeDelBlock(
    BlockContents&& block_contents,
    const InternalKeyComparator& internal_comparator,
    SequenceNumber global_seqno, bool user_defined_timestamps_persisted,
    std::unique_ptr<FragmentedRangeTombstoneList>* fragmented_range_dels) {
  assert(fragmented_range_dels != nullptr);
  fragmented_range_dels->reset();
  if (!block_contents.own_bytes()) {
    return Status::InvalidArgument(
        "Range deletion block contents must own their bytes");
  }

  CachableEntry<Block> parsed_block;
  parsed_block.SetOwnedValue(
      std::make_unique<Block>(std::move(block_contents)));
  // The parsed block is transferred to the iterator below, and the fragmented
  // list pins that iterator when it references block-backed keys or values.
  std::unique_ptr<InternalIterator> iter(
      parsed_block.GetValue()->NewDataIterator(
          internal_comparator.user_comparator(), global_seqno,
          /*iter=*/nullptr, /*stats=*/nullptr,
          /*block_contents_pinned=*/true, user_defined_timestamps_persisted));
  Status status = iter->status();
  if (!status.ok()) {
    return status;
  }

  parsed_block.TransferTo(iter.get());
  std::vector<SequenceNumber> snapshots;
  *fragmented_range_dels = std::make_unique<FragmentedRangeTombstoneList>(
      std::move(iter), internal_comparator, false /* for_compaction */,
      snapshots, user_defined_timestamps_persisted);
  return Status::OK();
}

template <ExternalTableMode Mode>
Status LoadExternalTableRangeDeletions(
    ExternalTableReaderBase<Mode>* reader,
    const InternalKeyComparator& internal_comparator,
    const TableProperties& table_properties, SequenceNumber global_seqno,
    std::unique_ptr<FragmentedRangeTombstoneList>* fragmented_range_dels) {
  if (table_properties.num_range_deletions == 0) {
    return Status::OK();
  }

  std::unique_ptr<char[]> range_deletion_block;
  uint64_t range_deletion_block_size = 0;
  Status status = reader->GetRangeDeletionBlock(&range_deletion_block,
                                                &range_deletion_block_size);
  if (status.IsNotSupported()) {
    return Status::Corruption(
        "External table has range deletions but no range-deletion block");
  }
  if (!status.ok()) {
    return status;
  }
  BlockContents block_contents(std::move(range_deletion_block),
                               range_deletion_block_size);
  status = ParseRangeDelBlock(
      std::move(block_contents), internal_comparator, global_seqno,
      table_properties.user_defined_timestamps_persisted,
      fragmented_range_dels);
  if (!status.ok()) {
    return status;
  }
  if (*fragmented_range_dels == nullptr ||
      (*fragmented_range_dels)->num_unfragmented_tombstones() !=
          table_properties.num_range_deletions) {
    return Status::Corruption(
        "External table range-deletion count does not match table properties");
  }
  return Status::OK();
}

template <ExternalTableMode Mode>
class ExternalTableReaderAdapter : public TableReader {
 public:
  ExternalTableReaderAdapter(
      const InternalKeyComparator& internal_comparator,
      std::unique_ptr<ExternalTableReaderBase<Mode>>&& reader,
      std::shared_ptr<const TableProperties> table_properties,
      SequenceNumber global_seqno,
      std::unique_ptr<FragmentedRangeTombstoneList> fragmented_range_dels)
      : internal_comparator_(internal_comparator),
        reader_(std::move(reader)),
        table_properties_(std::move(table_properties)),
        global_seqno_(global_seqno),
        fragmented_range_dels_(std::move(fragmented_range_dels)) {}

  ~ExternalTableReaderAdapter() override {}

  // No copying allowed
  ExternalTableReaderAdapter(const ExternalTableReaderAdapter&) = delete;
  ExternalTableReaderAdapter& operator=(const ExternalTableReaderAdapter&) =
      delete;
  ExternalTableReaderAdapter(ExternalTableReaderAdapter&&) = delete;
  ExternalTableReaderAdapter& operator=(ExternalTableReaderAdapter&&) = delete;

  InternalIterator* NewIterator(
      const ReadOptions& read_options, const SliceTransform* prefix_extractor,
      Arena* arena, bool /* skip_filters */, TableReaderCaller /* caller */,
      size_t /* compaction_readahead_size */ = 0,
      bool /* allow_unprepared_value */ = false) override {
    auto iterator = reader_->NewIterator(read_options, prefix_extractor);
    if (arena == nullptr) {
      return new ExternalTableIteratorAdapter<Mode>(
          iterator, internal_comparator_, global_seqno_);
    } else {
      auto* mem =
          arena->AllocateAligned(sizeof(ExternalTableIteratorAdapter<Mode>));
      return new (mem) ExternalTableIteratorAdapter<Mode>(
          iterator, internal_comparator_, global_seqno_);
    }
  }

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      const ReadOptions& read_options) override {
    if (fragmented_range_dels_ == nullptr) {
      return nullptr;
    }
    SequenceNumber snapshot = kMaxSequenceNumber;
    if (read_options.snapshot != nullptr) {
      snapshot = read_options.snapshot->GetSequenceNumber();
    }
    return new FragmentedRangeTombstoneIterator(fragmented_range_dels_,
                                                internal_comparator_, snapshot,
                                                read_options.timestamp);
  }

  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(
      SequenceNumber read_seqno, const Slice* timestamp) override {
    if (fragmented_range_dels_ == nullptr) {
      return nullptr;
    }
    return new FragmentedRangeTombstoneIterator(
        fragmented_range_dels_, internal_comparator_, read_seqno, timestamp);
  }

  uint64_t ApproximateOffsetOf(const ReadOptions&, const Slice&,
                               TableReaderCaller) override {
    return 0;
  }

  uint64_t ApproximateSize(const ReadOptions&, const Slice&, const Slice&,
                           TableReaderCaller) override {
    return 0;
  }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }

  size_t ApproximateMemoryUsage() const override { return 0; }

  DECLARE_SYNC_AND_ASYNC_OVERRIDE(Status, Get, const ReadOptions& read_options,
                                  const Slice& key, GetContext* get_context,
                                  const SliceTransform* prefix_extractor,
                                  bool skip_filters = false);

  DECLARE_SYNC_AND_ASYNC_OVERRIDE(void, MultiGet,
                                  const ReadOptions& read_options,
                                  const MultiGetContext::Range* mget_range,
                                  const SliceTransform* prefix_extractor,
                                  bool skip_filters = false);

  Status VerifyChecksum(const ReadOptions& read_options,
                        TableReaderCaller /*caller*/,
                        bool /*meta_blocks_only*/ = false) override {
    return reader_->VerifyChecksum(read_options);
  }

 private:
  const InternalKeyComparator& internal_comparator_;
  std::unique_ptr<ExternalTableReaderBase<Mode>> reader_;
  std::shared_ptr<const TableProperties> table_properties_;
  const SequenceNumber global_seqno_;
  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_range_dels_;
};

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

// clang-format off
#define WITHOUT_COROUTINES
#include "table/external_table_reader_sync_and_async.h"
#undef WITHOUT_COROUTINES
#define WITH_COROUTINES
#include "table/external_table_reader_sync_and_async.h"
#undef WITH_COROUTINES
// clang-format on

namespace ROCKSDB_NAMESPACE {
namespace {

// Converts RocksDB's internal Add() stream to the representation selected by
// Mode and maintains the table properties expected by the rest of RocksDB.
template <ExternalTableMode Mode>
class ExternalTableBuilderAdapter : public TableBuilder {
 public:
  explicit ExternalTableBuilderAdapter(
      const TableBuilderOptions& topts,
      std::unique_ptr<ExternalTableBuilderBase>&& builder,
      bool support_range_deletions)
      : builder_(std::move(builder)),
        ioptions_(topts.ioptions),
        support_range_deletions_(support_range_deletions),
        range_del_block_builder_(
            topts.internal_comparator.user_comparator()->timestamp_size(),
            topts.ioptions.persist_user_defined_timestamps) {
    properties_.num_data_blocks = 1;
    properties_.index_size = 0;
    properties_.filter_size = 0;
    properties_.format_version = 0;
    properties_.key_largest_seqno = 0;
    properties_.key_smallest_seqno = std::numeric_limits<uint64_t>::max();
    properties_.column_family_id = topts.column_family_id;
    properties_.column_family_name = topts.column_family_name;
    properties_.db_id = topts.db_id;
    properties_.db_session_id = topts.db_session_id;
    properties_.db_host_id = topts.ioptions.db_host_id;
    if (!ReifyDbHostIdProperty(topts.ioptions.env, &properties_.db_host_id)
             .ok()) {
      ROCKS_LOG_INFO(topts.ioptions.logger,
                     "db_host_id property will not be set");
    }
    properties_.orig_file_number = topts.cur_file_num;
    properties_.comparator_name = topts.ioptions.user_comparator != nullptr
                                      ? topts.ioptions.user_comparator->Name()
                                      : "nullptr";
    properties_.prefix_extractor_name =
        topts.moptions.prefix_extractor != nullptr
            ? topts.moptions.prefix_extractor->AsString()
            : "nullptr";

    for (auto& factory : *topts.internal_tbl_prop_coll_factories) {
      assert(factory);
      std::unique_ptr<InternalTblPropColl> collector{
          factory->CreateInternalTblPropColl(topts.column_family_id,
                                             topts.level_at_creation,
                                             topts.ioptions.num_levels)};
      if (collector) {
        table_properties_collectors_.emplace_back(std::move(collector));
      }
    }
  }

  void Add(const Slice& key, const Slice& value) override {
    TableBuilderAddContext context;
    const TableBuilderAddResult result = TryAdd(key, value, &context);
    if (result == TableBuilderAddResult::kRequiresNewTable) {
      status_ = Status::InvalidArgument(
          "External table builder requested a new table through Add");
    } else if (result == TableBuilderAddResult::kError && status_.ok()) {
      status_ = Status::InvalidArgument(
          "External table builder returned an "
          "error without a non-OK status");
    }
  }

  TableBuilderAddResult TryAdd(const Slice& key, const Slice& value,
                               TableBuilderAddContext* context) override {
    assert(context != nullptr);
    context->new_table_reason = TableBuilderNewTableReason::kNone;
    if (!status_.ok()) {
      return TableBuilderAddResult::kError;
    }

    ParsedEntryInfo entry;
    ValueType value_type = kTypeMaxValid;
    bool is_range_deletion = false;
    status_ = PrepareEntry(key, entry, value_type, is_range_deletion);
    if (!status_.ok()) {
      return TableBuilderAddResult::kError;
    }
    if (value_type == kTypeMaxValid) {
      status_ = Status::Corruption("External table entry value type is unset");
      return TableBuilderAddResult::kError;
    }

    if (is_range_deletion) {
      range_del_block_builder_.Add(key, value);
    } else {
      ExternalTableBuilderAddContext external_context;
      ExternalTableBuilderAddResult result;
      if constexpr (Mode == ExternalTableMode::kFull) {
        result = builder_->TryAdd(key, value, &external_context);
      } else {
        result = builder_->TryAdd(entry.user_key, value, &external_context);
      }

      if (result == ExternalTableBuilderAddResult::kRequiresNewTable) {
        status_ = builder_->status();
        if (!status_.ok()) {
          return TableBuilderAddResult::kError;
        }
        context->new_table_reason =
            ToTableBuilderNewTableReason(external_context.new_table_reason);
        return TableBuilderAddResult::kRequiresNewTable;
      }

      if (result == ExternalTableBuilderAddResult::kError) {
        status_ = builder_->status();
        if (status_.ok()) {
          status_ = Status::InvalidArgument(
              "External table builder returned an error without a non-OK "
              "status");
        }
        return TableBuilderAddResult::kError;
      }

      assert(result == ExternalTableBuilderAddResult::kAdded);
      status_ = builder_->status();
    }
    if (!status_.ok()) {
      return TableBuilderAddResult::kError;
    }

    RecordAcceptedEntry(key, value, entry, value_type);
    return TableBuilderAddResult::kAdded;
  }

  Status status() const override {
    if (status_.ok()) {
      return builder_->status();
    } else {
      return status_;
    }
  }

  IOStatus io_status() const override { return status_to_io_status(status()); }

  Status Finish() override {
    status_ = status();
    if (!status_.ok()) {
      builder_->Abandon();
      return status_;
    }

    // Approximate the data size
    properties_.data_size =
        properties_.raw_key_size + properties_.raw_value_size;

    PropertyBlockBuilder property_block_builder;
    property_block_builder.AddTableProperty(properties_);
    UserCollectedProperties more_user_collected_properties;
    NotifyCollectTableCollectorsOnFinish(
        table_properties_collectors_, ioptions_.logger, &property_block_builder,
        more_user_collected_properties, properties_.readable_properties);
    properties_.user_collected_properties.insert(
        more_user_collected_properties.begin(),
        more_user_collected_properties.end());

    Slice prop_block = property_block_builder.Finish();
    // The external implementation may supply TableProperties directly when it
    // does not persist RocksDB's properties block.
    status_ = builder_->PutPropertiesBlock(prop_block);
    properties_block_persisted_ = status_.ok();
    if (!status_.ok() && !status_.IsNotSupported()) {
      builder_->Abandon();
      return status_;
    }

    if (!range_del_block_builder_.empty()) {
      status_ =
          builder_->PutRangeDeletionBlock(range_del_block_builder_.Finish());
      if (!status_.ok()) {
        builder_->Abandon();
        return status_;
      }
    }

    status_ = builder_->Finish();

    return status_;
  }

  void Abandon() override { builder_->Abandon(); }

  uint64_t FileSize() const override { return builder_->FileSize(); }

  uint64_t NumEntries() const override { return properties_.num_entries; }

  bool IsEmpty() const override {
    return properties_.num_entries == 0 && properties_.num_range_deletions == 0;
  }

  bool NeedCompact() const override {
    for (const auto& collector : table_properties_collectors_) {
      if (collector->NeedCompact()) {
        return true;
      }
    }
    return false;
  }

  TableProperties GetTableProperties() const override {
    if (properties_block_persisted_) {
      return properties_;
    }
    // Overlay entry properties tracked here when no RocksDB properties block
    // was persisted.
    TableProperties properties = builder_->GetTableProperties();
    properties.num_entries = properties_.num_entries;
    properties.raw_key_size = properties_.raw_key_size;
    properties.raw_value_size = properties_.raw_value_size;
    properties.key_largest_seqno = properties_.key_largest_seqno;
    properties.key_smallest_seqno = properties_.key_smallest_seqno;
    properties.num_deletions = properties_.num_deletions;
    properties.num_range_deletions = properties_.num_range_deletions;
    properties.num_merge_operands = properties_.num_merge_operands;
    for (const auto& property : properties_.user_collected_properties) {
      properties.user_collected_properties.insert_or_assign(property.first,
                                                            property.second);
    }
    return properties;
  }

  std::string GetFileChecksum() const override {
    return builder_->GetFileChecksum();
  }

  const char* GetFileChecksumFuncName() const override {
    return builder_->GetFileChecksumFuncName();
  }

 private:
  TableBuilderNewTableReason ToTableBuilderNewTableReason(
      ExternalTableBuilderNewTableReason reason) const {
    switch (reason) {
      case ExternalTableBuilderNewTableReason::kNone:
        return TableBuilderNewTableReason::kNone;
      case ExternalTableBuilderNewTableReason::kRowClassificationChanged:
        return TableBuilderNewTableReason::kRowClassificationChanged;
      case ExternalTableBuilderNewTableReason::kSchemaIdChanged:
        return TableBuilderNewTableReason::kSchemaIdChanged;
      case ExternalTableBuilderNewTableReason::kSchemaVersionIncompatible:
        return TableBuilderNewTableReason::kSchemaVersionIncompatible;
      case ExternalTableBuilderNewTableReason::kUnsupportedEntryType:
        return TableBuilderNewTableReason::kUnsupportedEntryType;
      case ExternalTableBuilderNewTableReason::kBuilderPolicy:
        return TableBuilderNewTableReason::kBuilderPolicy;
    }
    assert(false);
    return TableBuilderNewTableReason::kBuilderPolicy;
  }

  Status PrepareEntry(const Slice& key, ParsedEntryInfo& entry,
                      ValueType& value_type, bool& is_range_deletion) {
    is_range_deletion = false;

    Status status = ParseEntry(key, ioptions_.user_comparator, &entry);
    if (!status.ok()) {
      return status;
    }
    if (!entry.timestamp.empty()) {
      return Status::NotSupported(
          "External table does not support user-defined timestamps");
    }
    is_range_deletion = entry.type == kEntryRangeDeletion;
    if constexpr (Mode == ExternalTableMode::kOnlyZeroSeqnoAndPuts) {
      if (entry.sequence != 0 ||
          (entry.type != kEntryPut &&
           (!is_range_deletion || !support_range_deletions_))) {
        if (support_range_deletions_) {
          return Status::NotSupported(
              "Basic external table factory only supports sequence-zero Put "
              "and range-deletion entries");
        } else {
          return Status::NotSupported(
              "Basic external table factory only supports sequence-zero Put "
              "entries");
        }
      }
      if (!is_range_deletion) {
        value_type = kTypeValue;
        return Status::OK();
      }
    }

    if (is_range_deletion) {
      value_type = kTypeRangeDeletion;
      return Status::OK();
    }

    return GetValueType(entry.type, &value_type);
  }

  void RecordAcceptedEntry(const Slice& key, const Slice& value,
                           const ParsedEntryInfo& entry, ValueType value_type) {
    properties_.key_largest_seqno =
        std::max(properties_.key_largest_seqno, entry.sequence);
    properties_.key_smallest_seqno =
        std::min(properties_.key_smallest_seqno, entry.sequence);
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    if (value_type == kTypeDeletion || value_type == kTypeSingleDeletion) {
      properties_.num_deletions++;
    } else if (value_type == kTypeRangeDeletion) {
      properties_.num_deletions++;
      properties_.num_range_deletions++;
    } else if (value_type == kTypeMerge) {
      properties_.num_merge_operands++;
    }
    NotifyCollectTableCollectorsOnAdd(key, value, /*file_size=*/0,
                                      table_properties_collectors_,
                                      ioptions_.logger);
  }
  Status status_;
  std::unique_ptr<ExternalTableBuilderBase> builder_;
  const ImmutableOptions& ioptions_;
  const bool support_range_deletions_;
  RangeDelBlockBuilder range_del_block_builder_;
  TableProperties properties_;
  std::vector<std::unique_ptr<InternalTblPropColl>>
      table_properties_collectors_;
  bool properties_block_persisted_ = false;
};

struct ExternalTableFactoryAdapterOptions {
  static const char* kName() { return "ExternalTableFactoryAdapterOptions"; }

  std::string config;
};

static const std::unordered_map<std::string, OptionTypeInfo>&
GetExternalTableFactoryAdapterOptionsTypeInfo() {
  static const std::unordered_map<std::string, OptionTypeInfo> type_info = {
      {"external_table_config",
       OptionTypeInfo(offsetof(ExternalTableFactoryAdapterOptions, config),
                      OptionType::kString)
           .SetSerializeFunc([](const ConfigOptions&, const std::string&,
                                const void* addr, std::string* value) {
             const std::string* config = static_cast<const std::string*>(addr);
             *value = "{" + EscapeOptionString(*config) + "}";
             return Status::OK();
           })}};
  return type_info;
}

// Adapts a mode-specific external factory to RocksDB's TableFactory interface.
template <ExternalTableMode Mode>
class ExternalTableFactoryAdapter : public TableFactory {
 public:
  static const char* kClassName() { return "ExternalTableFactoryAdapter"; }

  explicit ExternalTableFactoryAdapter(
      std::shared_ptr<ExternalTableFactoryBase<Mode>> inner)
      : inner_(std::move(inner)),
        supports_range_deletions_(inner_->IsDeleteRangeSupported()) {
    RegisterOptions(&options_,
                    &GetExternalTableFactoryAdapterOptionsTypeInfo());
  }

  const char* Name() const override { return inner_->Name(); }

  bool IsDeleteRangeSupported() const override {
    // This TableFactory capability gates live DB writes. Basic mode only
    // supports sequence-zero range deletions from external files.
    return Mode == ExternalTableMode::kFull && supports_range_deletions_;
  }

  bool IsInstanceOf(const std::string& name) const override {
    return name == kClassName() || TableFactory::IsInstanceOf(name);
  }

  Status PrepareOptions(const ConfigOptions& config_options) override {
    Status status = TableFactory::PrepareOptions(config_options);
    if (status.ok()) {
      status = inner_->Configure(options_.config);
    }
    if (status.ok()) {
      supports_range_deletions_ = inner_->IsDeleteRangeSupported();
    }
    return status;
  }

  using TableFactory::NewTableReader;
  Status NewTableReader(
      const ReadOptions& ro, const TableReaderOptions& topts,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool /* prefetch_index_and_filter_in_cache */) const override {
    if (topts.ioptions.user_comparator != nullptr &&
        topts.ioptions.user_comparator->timestamp_size() != 0) {
      return Status::NotSupported(
          "External table does not support user-defined timestamps");
    }
    std::unique_ptr<ExternalTableReaderBase<Mode>> reader;
    FileOptions fopts(topts.env_options);
    fopts.io_options.io_activity = ro.io_activity;
    // Files opened by external readers lack MANIFEST checksum metadata.
    fopts.file_checksum_func_name = kNoFileChecksumFuncName;
    ExternalTableOptions ext_topts(
        topts.prefix_extractor, topts.ioptions.user_comparator,
        &topts.internal_comparator, topts.ioptions.fs, fopts,
        topts.column_family_name, &topts);
    const std::string file_name = file->file_name();
    auto status = inner_->NewTableReader(ro, file_name, ext_topts,
                                         std::move(file), file_size, &reader);
    if (!status.ok()) {
      return status;
    }

    std::shared_ptr<const TableProperties> table_properties;
    status = LoadExternalTableProperties<Mode>(topts.ioptions, reader.get(),
                                               &table_properties);
    if (!status.ok()) {
      return status;
    }
    if (table_properties->num_range_deletions != 0 &&
        !supports_range_deletions_) {
      return Status::Corruption(
          "External table has range deletions but the factory does not "
          "support them");
    }

    // TableCache passes file_meta.fd.largest_seqno from the current Version
    // through TableReaderOptions. For an ingested file, that MANIFEST-backed
    // value is the DB-assigned global seqno. Resolve it once for readers and
    // iterators.
    SequenceNumber global_seqno;
    status = GetGlobalSequenceNumber(*table_properties, topts.largest_seqno,
                                     &global_seqno);
    if (!status.ok()) {
      return status;
    }

    std::unique_ptr<FragmentedRangeTombstoneList> fragmented_range_dels;
    status = LoadExternalTableRangeDeletions(
        reader.get(), topts.internal_comparator, *table_properties,
        global_seqno, &fragmented_range_dels);
    if (!status.ok()) {
      return status;
    }

    table_reader->reset(new ExternalTableReaderAdapter<Mode>(
        topts.internal_comparator, std::move(reader),
        std::move(table_properties), global_seqno,
        std::move(fragmented_range_dels)));
    return Status::OK();
  }

  using TableFactory::NewTableBuilder;
  TableBuilder* NewTableBuilder(const TableBuilderOptions& topts,
                                WritableFileWriter* file) const override {
    std::unique_ptr<ExternalTableBuilderBase> builder;
    ExternalTableBuilderOptions ext_topts(
        topts.read_options, topts.write_options,
        topts.moptions.prefix_extractor, topts.ioptions.user_comparator,
        topts.column_family_name, topts.reason, topts.ioptions.fs, &topts);
    builder.reset(inner_->NewTableBuilder(ext_topts, file->file_name(), file));
    if (builder) {
      return new ExternalTableBuilderAdapter<Mode>(topts, std::move(builder),
                                                   supports_range_deletions_);
    }
    return nullptr;
  }

  std::unique_ptr<TableFactory> Clone() const override { return nullptr; }

  // Lets the wrapped factory reject column families it cannot serve.
  Status ValidateOptions(const DBOptions& db_opts,
                         const ColumnFamilyOptions& cf_opts) const override {
    Status status = TableFactory::ValidateOptions(db_opts, cf_opts);
    if (status.ok()) {
      status = inner_->ValidateOptions(db_opts, cf_opts);
    }
    return status;
  }

  const Customizable* Inner() const override { return inner_.get(); }

  std::shared_ptr<ExternalTableFactory> inner() const { return inner_; }

 private:
  std::shared_ptr<ExternalTableFactoryBase<Mode>> inner_;
  ExternalTableFactoryAdapterOptions options_;
  // Avoid invoking external code when RocksDB queries this capability while
  // holding the DB mutex.
  bool supports_range_deletions_;
};

template <ExternalTableMode Mode>
std::unique_ptr<TableFactory> NewExternalTableFactoryImpl(
    std::shared_ptr<ExternalTableFactoryBase<Mode>> inner_factory) {
  return std::make_unique<ExternalTableFactoryAdapter<Mode>>(
      std::move(inner_factory));
}

}  // namespace

std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<ExternalTableFactory> inner_factory) {
  return NewExternalTableFactoryImpl(std::move(inner_factory));
}

std::unique_ptr<TableFactory> NewExternalTableFactory(
    std::shared_ptr<FullExternalTableFactory> inner_factory) {
  return NewExternalTableFactoryImpl(std::move(inner_factory));
}

std::shared_ptr<ExternalTableFactory> GetWrappedExternalTableFactory(
    const TableFactory* table_factory) {
  if (table_factory == nullptr) {
    return nullptr;
  }
  const auto* adapter = table_factory->CheckedCast<
      ExternalTableFactoryAdapter<ExternalTableMode::kOnlyZeroSeqnoAndPuts>>();
  return adapter == nullptr ? nullptr : adapter->inner();
}

}  // namespace ROCKSDB_NAMESPACE
