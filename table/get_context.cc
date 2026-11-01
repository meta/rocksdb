//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/get_context.h"

#include <forward_list>
#include <vector>

#include "db/blob/blob_fetcher.h"
#include "db/blob/blob_index.h"
#include "db/blob/same_file_blob_reader.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/read_callback.h"
#include "db/wide/wide_column_serialization.h"
#include "db/wide/wide_columns_helper.h"
#include "monitoring/file_read_sample.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics_impl.h"
#include "port/likely.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {

GetContext::GetContext(
    const Comparator* ucmp, const MergeOperator* merge_operator, Logger* logger,
    Statistics* statistics, GetState init_state, const Slice& user_key,
    PinnableSlice* pinnable_val, PinnableWideColumns* columns,
    std::string* timestamp, bool* value_found, MergeContext* merge_context,
    bool do_merge, SequenceNumber* _max_covering_tombstone_seq,
    SystemClock* clock, SequenceNumber* seq,
    PinnedIteratorsManager* _pinned_iters_mgr, ReadCallback* callback,
    bool* is_blob_index, uint64_t tracing_get_id, BlobFetcher* blob_fetcher,
    const SameFileBlobReader** lazy_columns_same_file_reader)
    : ucmp_(ucmp),
      merge_operator_(merge_operator),
      logger_(logger),
      statistics_(statistics),
      state_(init_state),
      user_key_(user_key),
      pinnable_val_(pinnable_val),
      columns_(columns),
      timestamp_(timestamp),
      value_found_(value_found),
      merge_context_(merge_context),
      max_covering_tombstone_seq_(_max_covering_tombstone_seq),
      clock_(clock),
      seq_(seq),
      replay_log_(nullptr),
      pinned_iters_mgr_(_pinned_iters_mgr),
      callback_(callback),
      do_merge_(do_merge),
      is_blob_index_(is_blob_index),
      tracing_get_id_(tracing_get_id),
      blob_fetcher_(blob_fetcher),
      lazy_columns_same_file_reader_(lazy_columns_same_file_reader) {
  if (seq_) {
    *seq_ = kMaxSequenceNumber;
  }
  sample_ = should_sample_file_read();
}

GetContext::GetContext(const Comparator* ucmp,
                       const MergeOperator* merge_operator, Logger* logger,
                       Statistics* statistics, GetState init_state,
                       const Slice& user_key, PinnableSlice* pinnable_val,
                       PinnableWideColumns* columns, bool* value_found,
                       MergeContext* merge_context, bool do_merge,
                       SequenceNumber* _max_covering_tombstone_seq,
                       SystemClock* clock, SequenceNumber* seq,
                       PinnedIteratorsManager* _pinned_iters_mgr,
                       ReadCallback* callback, bool* is_blob_index,
                       uint64_t tracing_get_id, BlobFetcher* blob_fetcher,
                       const SameFileBlobReader** lazy_columns_same_file_reader)
    : GetContext(ucmp, merge_operator, logger, statistics, init_state, user_key,
                 pinnable_val, columns, /*timestamp=*/nullptr, value_found,
                 merge_context, do_merge, _max_covering_tombstone_seq, clock,
                 seq, _pinned_iters_mgr, callback, is_blob_index,
                 tracing_get_id, blob_fetcher, lazy_columns_same_file_reader) {}

void GetContext::appendToReplayLog(ValueType type, Slice value, Slice ts) {
  if (replay_log_) {
    if (replay_log_->empty()) {
      // Optimization: in the common case of only one operation in the
      // log, we allocate the exact amount of space needed.
      replay_log_->reserve(1 + VarintLength(value.size()) + value.size());
    }
    replay_log_->push_back(type);
    PutLengthPrefixedSlice(replay_log_, value);

    // If cf enables ts, there should always be a ts following each value
    if (ucmp_->timestamp_size() > 0) {
      assert(ts.size() == ucmp_->timestamp_size());
      PutLengthPrefixedSlice(replay_log_, ts);
    }
  }
}

// Called from TableCache::Get and Table::Get when file/block in which
// key may exist are not there in TableCache/BlockCache respectively. In this
// case we can't guarantee that key does not exist and are not permitted to do
// IO to be certain.Set the status=kFound and value_found=false to let the
// caller know that key may exist but is not there in memory
void GetContext::MarkKeyMayExist() {
  state_ = kFound;
  if (value_found_ != nullptr) {
    *value_found_ = false;
  }
}

void GetContext::RecordNewerVersionIfNeeded(SequenceNumber seq,
                                            ValueType type) {
  // Caller already filtered out the no-tracking case via
  // NeedToTrackNewerVersions().
  assert(callback_ != nullptr);
  callback_->MaybeRecordNewerVersion(
      seq, type, has_newer_version_result_ ? newer_version_present_ : nullptr);
}

Status GetContext::SaveWideColumnEntityToPinnable(
    const Slice& user_key, const Slice& entity, Cleanable* value_pinner,
    const SameFileBlobReader* same_file_reader) {
  assert(pinnable_val_ != nullptr);

  // Fast path: extract the default column without full deserialization. An
  // inline default value is pinned zero-copy; only a blob-referenced default
  // needs a fetch (ResolveDefaultColumnBlobReference owns the null-fetcher
  // handling).
  Slice value_of_default;
  bool is_blob_reference = false;
  Status status = WideColumnSerialization::GetValueOfDefaultColumn(
      entity, value_of_default, is_blob_reference);
  if (!status.ok()) {
    return status;
  }
  if (!is_blob_reference) {
    if (LIKELY(value_pinner != nullptr)) {
      pinnable_val_->PinSlice(value_of_default, value_pinner);
    } else {
      pinnable_val_->PinSelf(value_of_default);
    }
    return Status::OK();
  }
  EmbeddedAwareBlobFetcher embedded_fetcher(blob_fetcher_, same_file_reader);
  return WideColumnSerialization::ResolveDefaultColumnBlobReference(
      value_of_default, user_key, embedded_fetcher.EffectiveFetcher(),
      *pinnable_val_);
}

Status GetContext::SaveWideColumnEntityToColumns(
    const Slice& user_key, const Slice& entity, Cleanable* value_pinner,
    const SameFileBlobReader* same_file_reader) {
  assert(columns_ != nullptr);

  // Pin (or copy) the serialized entity as the base backing buffer; this also
  // parses it and records any blob-valued columns that still need resolving.
  Status status = columns_->SetWideColumnValue(entity, value_pinner);
  if (!status.ok()) {
    return status;
  }

  if (!has_newer_version_result_ && lazy_columns_same_file_reader_ != nullptr) {
    // Lazy mode (GetEntityLazy): leave blob references unresolved so the caller
    // can resolve them on demand (by column / byte range). Capture the
    // SameFileBlobReader (the SST that held this entity, if it has embedded
    // blobs) so same-file references stay resolvable later; it is null for an
    // entity with only separate-file (or no) blob references.
    *lazy_columns_same_file_reader_ = same_file_reader;
    return status;
  }

  if (PinnableWideColumnsHelper::GetUnresolvedBlobColumnIndices(*columns_)
          .empty()) {
    return status;
  }

  // TODO: Add lazy resolution support for GetEntity point lookups. This
  // requires SuperVersion pinning on PinnableWideColumns to keep the Version*
  // alive after GetImpl returns. Currently, lazy_column_resolution only takes
  // effect for iterators (DBIter path).
  //
  // Eager path: resolve blob columns into their own address-stable backing
  // buffers and splice them in, so inline columns keep zero-copy Slices into
  // the pinned entity and no re-serialization is needed. A null blob_fetcher_
  // with blob columns present is reported as Corruption inside the resolver.
  // For an embedded-blob SST, `same_file_reader` composes an embedded-aware
  // fetcher so same-file blob columns are pinned zero-copy from this SST too.
  EmbeddedAwareBlobFetcher embedded_fetcher(blob_fetcher_, same_file_reader);
  WideColumns resolved_columns;
  std::forward_list<PinnableSlice> extra_buffers;
  bool resolved = false;
  status = WideColumnSerialization::ResolveEntityBlobColumnsMultiBuffer(
      PinnableWideColumnsHelper::GetSerializedEntity(*columns_), user_key,
      embedded_fetcher.EffectiveFetcher(), nullptr /* prefetch_buffers */,
      resolved_columns, extra_buffers, resolved, nullptr /* total_bytes_read */,
      nullptr /* num_blobs_resolved */);
  if (status.ok()) {
    assert(resolved);
    PinnableWideColumnsHelper::ResolveColumns(
        *columns_, std::move(resolved_columns), std::move(extra_buffers));
  } else {
    columns_->Reset();
  }
  return status;
}

Status GetContext::PushWideColumnEntityDefaultOperand(
    const Slice& user_key, const Slice& entity, Cleanable* value_pinner,
    const SameFileBlobReader* same_file_reader) {
  Slice value_of_default;
  bool is_blob_reference = false;
  Status status = WideColumnSerialization::GetValueOfDefaultColumn(
      entity, value_of_default, is_blob_reference);
  if (!status.ok()) {
    return status;
  }
  if (!is_blob_reference) {
    push_operand(value_of_default, value_pinner);
    return status;
  }

  EmbeddedAwareBlobFetcher embedded_fetcher(blob_fetcher_, same_file_reader);
  PinnableSlice resolved_default;
  status = WideColumnSerialization::ResolveDefaultColumnBlobReference(
      value_of_default, user_key, embedded_fetcher.EffectiveFetcher(),
      resolved_default);
  if (status.ok()) {
    // Resolved value is backed by this stack-local PinnableSlice, so copy it
    // into MergeContext instead of pinning its storage.
    push_operand(Slice(resolved_default), nullptr);
  }
  return status;
}

void GetContext::SaveValue(const Slice& value, SequenceNumber seq,
                           Cleanable* value_pinner) {
  assert(state_ == kNotFound);
  assert(ucmp_->timestamp_size() == 0);

  TEST_SYNC_POINT_CALLBACK("GetContext::SaveValue::Simple", this);

  appendToReplayLog(kTypeValue, value, Slice());

  state_ = kFound;
  if (seq_ != nullptr && *seq_ == kMaxSequenceNumber) {
    *seq_ = seq;
  }
  if (is_blob_index_ != nullptr) {
    *is_blob_index_ = false;
  }
  if (LIKELY(pinnable_val_ != nullptr)) {
    if (LIKELY(value_pinner != nullptr)) {
      pinnable_val_->PinSlice(value, value_pinner);
    } else {
      pinnable_val_->PinSelf(value);
    }
  }
}

void GetContext::ReportCounters() {
  if (get_context_stats_.num_cache_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_HIT, get_context_stats_.num_cache_hit);
  }
  if (get_context_stats_.num_cache_index_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_HIT,
               get_context_stats_.num_cache_index_hit);
  }
  if (get_context_stats_.num_cache_data_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_HIT,
               get_context_stats_.num_cache_data_hit);
  }
  if (get_context_stats_.num_cache_filter_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_HIT,
               get_context_stats_.num_cache_filter_hit);
  }
  if (get_context_stats_.num_cache_compression_dict_hit > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_HIT,
               get_context_stats_.num_cache_compression_dict_hit);
  }
  if (get_context_stats_.num_cache_index_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_MISS,
               get_context_stats_.num_cache_index_miss);
  }
  if (get_context_stats_.num_cache_filter_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_MISS,
               get_context_stats_.num_cache_filter_miss);
  }
  if (get_context_stats_.num_cache_data_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_MISS,
               get_context_stats_.num_cache_data_miss);
  }
  if (get_context_stats_.num_cache_compression_dict_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_MISS,
               get_context_stats_.num_cache_compression_dict_miss);
  }
  if (get_context_stats_.num_cache_bytes_read > 0) {
    RecordTick(statistics_, BLOCK_CACHE_BYTES_READ,
               get_context_stats_.num_cache_bytes_read);
  }
  if (get_context_stats_.num_cache_miss > 0) {
    RecordTick(statistics_, BLOCK_CACHE_MISS,
               get_context_stats_.num_cache_miss);
  }
  if (get_context_stats_.num_cache_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_ADD, get_context_stats_.num_cache_add);
  }
  if (get_context_stats_.num_cache_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_ADD_REDUNDANT,
               get_context_stats_.num_cache_add_redundant);
  }
  if (get_context_stats_.num_cache_bytes_write > 0) {
    RecordTick(statistics_, BLOCK_CACHE_BYTES_WRITE,
               get_context_stats_.num_cache_bytes_write);
  }
  if (get_context_stats_.num_cache_index_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_ADD,
               get_context_stats_.num_cache_index_add);
  }
  if (get_context_stats_.num_cache_index_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_ADD_REDUNDANT,
               get_context_stats_.num_cache_index_add_redundant);
  }
  if (get_context_stats_.num_cache_index_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_INDEX_BYTES_INSERT,
               get_context_stats_.num_cache_index_bytes_insert);
  }
  if (get_context_stats_.num_cache_data_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_ADD,
               get_context_stats_.num_cache_data_add);
  }
  if (get_context_stats_.num_cache_data_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_ADD_REDUNDANT,
               get_context_stats_.num_cache_data_add_redundant);
  }
  if (get_context_stats_.num_cache_data_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_DATA_BYTES_INSERT,
               get_context_stats_.num_cache_data_bytes_insert);
  }
  if (get_context_stats_.num_cache_filter_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_ADD,
               get_context_stats_.num_cache_filter_add);
  }
  if (get_context_stats_.num_cache_filter_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_ADD_REDUNDANT,
               get_context_stats_.num_cache_filter_add_redundant);
  }
  if (get_context_stats_.num_cache_filter_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_FILTER_BYTES_INSERT,
               get_context_stats_.num_cache_filter_bytes_insert);
  }
  if (get_context_stats_.num_cache_compression_dict_add > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_ADD,
               get_context_stats_.num_cache_compression_dict_add);
  }
  if (get_context_stats_.num_cache_compression_dict_add_redundant > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_ADD_REDUNDANT,
               get_context_stats_.num_cache_compression_dict_add_redundant);
  }
  if (get_context_stats_.num_cache_compression_dict_bytes_insert > 0) {
    RecordTick(statistics_, BLOCK_CACHE_COMPRESSION_DICT_BYTES_INSERT,
               get_context_stats_.num_cache_compression_dict_bytes_insert);
  }
}

bool GetContext::SaveValue(const ParsedInternalKey& parsed_key,
                           const Slice& value, bool* matched,
                           Status* read_status, Cleanable* value_pinner,
                           const SameFileBlobReader* same_file_reader) {
  assert(matched);
  assert((State() != kMerge && parsed_key.type != kTypeMerge) ||
         merge_context_ != nullptr);
  if (ucmp_->EqualWithoutTimestamp(parsed_key.user_key, user_key_)) {
    *matched = true;
    if (callback_ != nullptr) {
      if (UNLIKELY(NeedToTrackNewerVersions())) {
        RecordNewerVersionIfNeeded(parsed_key.sequence, parsed_key.type);
      }
      // If the value is not in the snapshot, skip it.
      if (!callback_->IsVisible(parsed_key.sequence)) {
        return true;  // to continue to the next seq
      }
    }

    if (seq_ != nullptr) {
      // Set the sequence number if it is uninitialized
      if (*seq_ == kMaxSequenceNumber) {
        *seq_ = parsed_key.sequence;
      }
      if (max_covering_tombstone_seq_) {
        *seq_ = std::max(*seq_, *max_covering_tombstone_seq_);
      }
    }

    size_t ts_sz = ucmp_->timestamp_size();
    Slice ts;

    if (ts_sz > 0) {
      // ensure always have ts if cf enables ts.
      ts = ExtractTimestampFromUserKey(parsed_key.user_key, ts_sz);
      if (timestamp_ != nullptr) {
        if (!timestamp_->empty()) {
          assert(ts_sz == timestamp_->size());
          // `timestamp` can be set before `SaveValue` is ever called
          // when max_covering_tombstone_seq_ was set.
          // If this key has a higher sequence number than range tombstone,
          // then timestamp should be updated. `ts_from_rangetombstone_` is
          // set to false afterwards so that only the key with highest seqno
          // updates the timestamp.
          if (ts_from_rangetombstone_) {
            assert(max_covering_tombstone_seq_);
            if (parsed_key.sequence > *max_covering_tombstone_seq_) {
              timestamp_->assign(ts.data(), ts.size());
              ts_from_rangetombstone_ = false;
            }
          }
        }
        // TODO optimize for small size ts
        const std::string kMaxTs(ts_sz, '\xff');
        if (timestamp_->empty() ||
            ucmp_->CompareTimestamp(*timestamp_, kMaxTs) == 0) {
          timestamp_->assign(ts.data(), ts.size());
        }
      }
    }
    appendToReplayLog(parsed_key.type, value, ts);

    auto type = parsed_key.type;
    Slice unpacked_value = value;
    if ((type == kTypeBlobIndex || type == kTypeWideColumnEntity) &&
        blob_fetcher_ != nullptr && blob_fetcher_->HasBlobValueLoss()) {
      bool lost = false;
      const Status status = blob_fetcher_->IsBlobValueLost(type, value, &lost);
      if (!status.ok()) {
        state_ = kCorrupt;
        *read_status = status;
        return false;
      }
      if (lost) {
        // Reuse ordinary deletion/merge handling without exposing an older
        // version of this key. No stable cache or lazy column is consulted.
        type = kTypeDeletion;
      }
    }
    // Key matches. Process it
    if ((type == kTypeValue || type == kTypeValuePreferredSeqno ||
         type == kTypeMerge || type == kTypeBlobIndex ||
         type == kTypeWideColumnEntity || type == kTypeDeletion ||
         type == kTypeDeletionWithTimestamp || type == kTypeSingleDeletion) &&
        max_covering_tombstone_seq_ != nullptr &&
        *max_covering_tombstone_seq_ > parsed_key.sequence) {
      // Note that deletion types are also considered, this is for the case
      // when we need to return timestamp to user. If a range tombstone has a
      // higher seqno than point tombstone, its timestamp should be returned.
      type = kTypeRangeDeletion;
    }
    switch (type) {
      case kTypeValue:
      case kTypeValuePreferredSeqno:
      case kTypeBlobIndex:
      case kTypeWideColumnEntity:
        assert(State() == kNotFound || State() == kMerge);
        if (type == kTypeValuePreferredSeqno) {
          unpacked_value = ParsePackedValueForValue(value);
        }
        if (type == kTypeBlobIndex) {
          if (is_blob_index_ == nullptr) {
            // Blob value not supported. Stop.
            state_ = kUnexpectedBlobIndex;
            return false;
          }
        }

        if (is_blob_index_ != nullptr) {
          *is_blob_index_ = (type == kTypeBlobIndex);
        }

        if (State() == kNotFound) {
          state_ = kFound;
          if (do_merge_) {
            if (type == kTypeBlobIndex && ucmp_->timestamp_size() != 0) {
              ukey_with_ts_found_.PinSelf(parsed_key.user_key);
            }
            if (LIKELY(pinnable_val_ != nullptr)) {
              Slice value_to_use = unpacked_value;

              if (type == kTypeWideColumnEntity) {
                const Status s = SaveWideColumnEntityToPinnable(
                    parsed_key.user_key, unpacked_value, value_pinner,
                    same_file_reader);
                if (!s.ok()) {
                  if (s.IsIncomplete()) {
                    MarkKeyMayExist();
                    *read_status = s;
                  } else {
                    state_ = kCorrupt;
                    *read_status = s;
                  }
                  return false;
                }
              } else {
                // Non-entity type
                if (LIKELY(value_pinner != nullptr)) {
                  // If the backing resources for the value are provided, pin
                  // them
                  pinnable_val_->PinSlice(value_to_use, value_pinner);
                } else {
                  TEST_SYNC_POINT_CALLBACK("GetContext::SaveValue::PinSelf",
                                           this);
                  // Otherwise copy the value
                  pinnable_val_->PinSelf(value_to_use);
                }
              }
            } else if (columns_ != nullptr) {
              if (type == kTypeWideColumnEntity) {
                const Status s = SaveWideColumnEntityToColumns(
                    parsed_key.user_key, unpacked_value, value_pinner,
                    same_file_reader);
                if (!s.ok()) {
                  if (s.IsIncomplete()) {
                    MarkKeyMayExist();
                    *read_status = s;
                  } else {
                    state_ = kCorrupt;
                    *read_status = s;
                  }
                  return false;
                }
              } else {
                columns_->SetPlainValue(unpacked_value, value_pinner);
              }
            }
          } else {
            // It means this function is called as part of DB GetMergeOperands
            // API and the current value should be part of
            // merge_context_->operand_list
            if (type == kTypeBlobIndex) {
              PinnableSlice pin_val;
              if (GetBlobValue(parsed_key.user_key, unpacked_value, &pin_val,
                               read_status) == false) {
                return false;
              }
              Slice blob_value(pin_val);
              push_operand(blob_value, nullptr);
            } else if (type == kTypeWideColumnEntity) {
              const Status s = PushWideColumnEntityDefaultOperand(
                  parsed_key.user_key, unpacked_value, value_pinner,
                  same_file_reader);
              if (!s.ok()) {
                if (s.IsIncomplete()) {
                  MarkKeyMayExist();
                  *read_status = s;
                } else {
                  state_ = kCorrupt;
                  *read_status = s;
                }
                return false;
              }
            } else {
              assert(type == kTypeValue || type == kTypeValuePreferredSeqno);
              push_operand(unpacked_value, value_pinner);
            }
          }
        } else if (State() == kMerge) {
          assert(merge_operator_ != nullptr);
          if (type == kTypeBlobIndex) {
            PinnableSlice pin_val;
            if (GetBlobValue(parsed_key.user_key, unpacked_value, &pin_val,
                             read_status) == false) {
              return false;
            }
            Slice blob_value(pin_val);
            state_ = kFound;
            if (do_merge_) {
              const Status s = MergeWithPlainBaseValue(blob_value);
              if (!s.ok()) {
                *read_status = s;
                return false;
              }
            } else {
              // It means this function is called as part of DB GetMergeOperands
              // API and the current value should be part of
              // merge_context_->operand_list
              push_operand(blob_value, nullptr);
            }
          } else if (type == kTypeWideColumnEntity) {
            state_ = kFound;

            if (do_merge_) {
              const Status s = MergeWithWideColumnBaseValue(unpacked_value,
                                                            same_file_reader);
              if (!s.ok()) {
                *read_status = s;
                return false;
              }
            } else {
              // It means this function is called as part of DB GetMergeOperands
              // API and the current value should be part of
              // merge_context_->operand_list
              const Status s = PushWideColumnEntityDefaultOperand(
                  parsed_key.user_key, unpacked_value, value_pinner,
                  same_file_reader);
              if (!s.ok()) {
                if (s.IsIncomplete()) {
                  MarkKeyMayExist();
                  *read_status = s;
                } else {
                  state_ = kCorrupt;
                  *read_status = s;
                }
                return false;
              }
            }
          } else {
            assert(type == kTypeValue || type == kTypeValuePreferredSeqno);

            state_ = kFound;
            if (do_merge_) {
              const Status s = MergeWithPlainBaseValue(unpacked_value);
              if (!s.ok()) {
                *read_status = s;
                return false;
              }
            } else {
              // It means this function is called as part of DB GetMergeOperands
              // API and the current value should be part of
              // merge_context_->operand_list
              push_operand(unpacked_value, value_pinner);
            }
          }
        }
        return false;

      case kTypeDeletion:
      case kTypeDeletionWithTimestamp:
      case kTypeSingleDeletion:
      case kTypeRangeDeletion:
        // TODO(noetzli): Verify correctness once merge of single-deletes
        // is supported
        assert(State() == kNotFound || State() == kMerge);
        if (State() == kNotFound) {
          state_ = kDeleted;
        } else if (State() == kMerge) {
          state_ = kFound;
          if (do_merge_) {
            const Status s = MergeWithNoBaseValue();
            if (!s.ok()) {
              *read_status = s;
              return false;
            }
          }
          // If do_merge_ = false then the current value shouldn't be part of
          // merge_context_->operand_list
        }
        return false;

      case kTypeMerge:
        assert(State() == kNotFound || State() == kMerge);
        state_ = kMerge;
        // value_pinner is not set from plain_table_reader.cc for example.
        push_operand(value, value_pinner);
        PERF_COUNTER_ADD(internal_merge_point_lookup_count, 1);

        if (do_merge_ && merge_operator_ != nullptr &&
            merge_operator_->ShouldMerge(
                merge_context_->GetOperandsDirectionBackward())) {
          state_ = kFound;
          const Status s = MergeWithNoBaseValue();
          if (!s.ok()) {
            *read_status = s;
            return false;
          }
          return false;
        }
        if (merge_context_->get_merge_operands_options != nullptr &&
            merge_context_->get_merge_operands_options->continue_cb !=
                nullptr &&
            !merge_context_->get_merge_operands_options->continue_cb(value)) {
          state_ = kFound;
          return false;
        }
        return true;

      default:
        assert(false);
        break;
    }
  }

  // State() could be Corrupt, merge or notfound
  return false;
}

Status GetContext::PostprocessMerge(const Status& merge_status) {
  if (!merge_status.ok()) {
    if (merge_status.subcode() == Status::SubCode::kMergeOperatorFailed) {
      state_ = kMergeOperatorFailed;
    } else {
      state_ = kCorrupt;
    }
    return merge_status;
  }

  if (LIKELY(pinnable_val_ != nullptr)) {
    pinnable_val_->PinSelf();
  }
  return Status::OK();
}

Status GetContext::MergeWithNoBaseValue() {
  assert(do_merge_);
  assert(pinnable_val_ || columns_);
  assert(!pinnable_val_ || !columns_);

  // `op_failure_scope` (an output parameter) is not provided (set to nullptr)
  // since a failure must be propagated regardless of its value.
  const Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key_, MergeHelper::kNoBaseValue,
      merge_context_->GetOperands(), logger_, statistics_, clock_,
      /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
      pinnable_val_ ? pinnable_val_->GetSelf() : nullptr, columns_);
  return PostprocessMerge(s);
}

Status GetContext::MergeWithPlainBaseValue(const Slice& value) {
  assert(do_merge_);
  assert(pinnable_val_ || columns_);
  assert(!pinnable_val_ || !columns_);

  // `op_failure_scope` (an output parameter) is not provided (set to nullptr)
  // since a failure must be propagated regardless of its value.
  const Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key_, MergeHelper::kPlainBaseValue, value,
      merge_context_->GetOperands(), logger_, statistics_, clock_,
      /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
      pinnable_val_ ? pinnable_val_->GetSelf() : nullptr, columns_);
  return PostprocessMerge(s);
}

Status GetContext::MergeWithWideColumnBaseValue(
    const Slice& entity, const SameFileBlobReader* same_file_reader) {
  assert(do_merge_);
  assert(pinnable_val_ || columns_);
  assert(!pinnable_val_ || !columns_);

  // Resolve V2 entity blob columns if present, since TimedFullMerge only
  // supports V1 format. For an embedded-blob SST, `same_file_reader` composes
  // an embedded-aware fetcher so same-file blob columns resolve too.
  EmbeddedAwareBlobFetcher embedded_fetcher(blob_fetcher_, same_file_reader);
  std::string resolved_entity;
  Slice effective_entity;
  Status s_resolve = WideColumnSerialization::ResolveEntityForMerge(
      entity, user_key_, embedded_fetcher.EffectiveFetcher(),
      nullptr /* prefetch_buffers */, resolved_entity, effective_entity);
  if (!s_resolve.ok()) {
    if (s_resolve.IsIncomplete()) {
      MarkKeyMayExist();
      return s_resolve;
    }
    state_ = kCorrupt;
    return s_resolve;
  }

  // `op_failure_scope` (an output parameter) is not provided (set to nullptr)
  // since a failure must be propagated regardless of its value.
  const Status s = MergeHelper::TimedFullMerge(
      merge_operator_, user_key_, MergeHelper::kWideBaseValue, effective_entity,
      merge_context_->GetOperands(), logger_, statistics_, clock_,
      /* update_num_ops_stats */ true, /* op_failure_scope */ nullptr,
      pinnable_val_ ? pinnable_val_->GetSelf() : nullptr, columns_);
  return PostprocessMerge(s);
}

bool GetContext::GetBlobValue(const Slice& user_key, const Slice& blob_index,
                              PinnableSlice* blob_value, Status* read_status) {
  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr uint64_t* bytes_read = nullptr;

  *read_status = blob_fetcher_->FetchBlob(user_key, blob_index, prefetch_buffer,
                                          blob_value, bytes_read);
  if (!read_status->ok()) {
    if (read_status->IsIncomplete()) {
      // FIXME: this code is not covered by unit tests
      MarkKeyMayExist();
      return false;
    }
    state_ = kCorrupt;
    return false;
  }
  *is_blob_index_ = false;
  return true;
}

void GetContext::push_operand(const Slice& value, Cleanable* value_pinner) {
  // TODO(yanqin) preserve timestamps information in merge_context
  if (pinned_iters_mgr() && pinned_iters_mgr()->PinningEnabled() &&
      value_pinner != nullptr) {
    value_pinner->DelegateCleanupsTo(pinned_iters_mgr());
    merge_context_->PushOperand(value, true /*value_pinned*/);
  } else {
    merge_context_->PushOperand(value, false);
  }
}

Status replayGetContextLog(const Slice& replay_log, const Slice& user_key,
                           GetContext* get_context, Cleanable* value_pinner,
                           SequenceNumber seq_no) {
  Slice s = replay_log;
  Slice ts;
  size_t ts_sz = get_context->TimestampSize();
  bool ret = false;

  while (s.size()) {
    auto type = static_cast<ValueType>(*s.data());
    s.remove_prefix(1);
    Slice value;
    ret = GetLengthPrefixedSlice(&s, &value);
    assert(ret);

    bool dont_care __attribute__((__unused__));

    // Use a copy to prevent modifying user_key. Modification of user_key
    // could result to potential cache miss.
    std::string user_key_str = user_key.ToString();
    ParsedInternalKey ikey = ParsedInternalKey(user_key_str, seq_no, type);

    // If ts enabled for current cf, there will always be ts appended after each
    // piece of value.
    if (ts_sz > 0) {
      ret = GetLengthPrefixedSlice(&s, &ts);
      assert(ts_sz == ts.size());
      assert(ret);
      ikey.SetTimestamp(ts);
    }

    (void)ret;

    Status read_status = Status::OK();
    const bool keep_replaying = get_context->SaveValue(
        ikey, value, &dont_care, &read_status, value_pinner);
    if (!read_status.ok()) {
      return read_status;
    }
    if (!keep_replaying) {
      // SaveValue() reached a terminal state for this row-cache replay.
      break;
    }
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
