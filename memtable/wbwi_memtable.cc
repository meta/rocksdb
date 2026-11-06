//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "memtable/wbwi_memtable.h"

#include "db/memtable.h"

namespace ROCKSDB_NAMESPACE {

const std::unordered_map<WriteType, ValueType>
    WBWIMemTableIterator::WriteTypeToValueTypeMap = {
        {kPutRecord, kTypeValue},
        {kMergeRecord, kTypeMerge},
        {kDeleteRecord, kTypeDeletion},
        {kSingleDeleteRecord, kTypeSingleDeletion},
        {kDeleteRangeRecord, kTypeRangeDeletion},
        {kPutEntityRecord, kTypeWideColumnEntity},
        // Only the above record types are added to WBWI.
        // kLogDataRecord, kXIDRecord, kUnknownRecord
};

InternalIterator* WBWIMemTable::NewIterator(
    const ReadOptions&, UnownedPtr<const SeqnoToTimeMapping>, Arena* arena,
    const SliceTransform* /* prefix_extractor */, bool for_flush) {
  // Ingested WBWIMemTable should have an assigned seqno
  assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  assert(assigned_seqno_.lower_bound != kMaxSequenceNumber);
  assert(arena);
  auto mem = arena->AllocateAligned(sizeof(WBWIMemTableIterator));
  auto iter = std::make_unique<WBWIIterator>(wbwi_->NewIterator(cf_id_));
  return new (mem) WBWIMemTableIterator(std::move(iter), assigned_seqno_, comparator_, for_flush);
}

inline InternalIterator* WBWIMemTable::NewIterator() const {
  assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  assert(assigned_seqno_.lower_bound != kMaxSequenceNumber);
  return new WBWIMemTableIterator(
      std::unique_ptr<WBWIIterator>(wbwi_->NewIterator(cf_id_)),
      assigned_seqno_, comparator_, /*for_flush=*/false);
}

bool WBWIMemTable::Get(const LookupKey& key, std::string* value,
                       PinnableWideColumns* columns, std::string* timestamp,
                       Status* s, MergeContext* merge_context,
                       SequenceNumber* max_covering_tombstone_seq,
                       SequenceNumber* out_seq, const ReadOptions& read_opts,
                       bool immutable_memtable, ReadCallback* callback,
                       bool* is_blob_index, bool do_merge,
                       const BlobFetcher* blob_fetcher) {
  return GetImpl(key, value, columns, timestamp, s, merge_context,
                 max_covering_tombstone_seq, out_seq, read_opts,
                 immutable_memtable, callback, is_blob_index, do_merge,
                 blob_fetcher, /*newer_version_present=*/nullptr);
}

bool WBWIMemTable::GetImpl(const LookupKey& key, std::string* value,
                           PinnableWideColumns* columns, std::string* timestamp,
                           Status* s, MergeContext* merge_context,
                           SequenceNumber* max_covering_tombstone_seq,
                           SequenceNumber* out_seq, const ReadOptions&,
                           bool immutable_memtable, ReadCallback* callback,
                           bool* is_blob_index, bool do_merge,
                           const BlobFetcher* blob_fetcher,
                           bool* newer_version_present) {
  assert(s->ok() || s->IsMergeInProgress());
  (void)immutable_memtable;
  (void)timestamp;
  (void)columns;
  (void)blob_fetcher;
  assert(immutable_memtable);
  assert(!timestamp);  // TODO: support UDT
  // IngestWriteBatchWithIndex() is rejected while any live column family has
  // blob direct write enabled, so WBWI should never need blob resolution.
  assert(blob_fetcher == nullptr);
  assert(assigned_seqno_.upper_bound != kMaxSequenceNumber);
  assert(assigned_seqno_.lower_bound != kMaxSequenceNumber);
  // WBWI does not support DeleteRange yet.
  assert(!wbwi_->GetWriteBatch()->HasDeleteRange());
  assert(merge_context);

  *out_seq = kMaxSequenceNumber;
  [[maybe_unused]] SequenceNumber read_seq =
      GetInternalKeySeqno(key.internal_key());
  // This is memtable is a single write batch, no snapshot can be taken within
  // assigned seqnos for this memtable.
  assert(read_seq >= assigned_seqno_.upper_bound ||
         read_seq < assigned_seqno_.lower_bound);
  std::unique_ptr<InternalIterator> iter{NewIterator()};
  iter->Seek(key.internal_key());
  const Slice lookup_user_key = key.user_key();
  bool merge_in_progress = s->IsMergeInProgress();

  while (iter->Valid() && comparator_->EqualWithoutTimestamp(
                              ExtractUserKey(iter->key()), lookup_user_key)) {
    uint64_t tag = ExtractInternalKeyFooter(iter->key());
    ValueType type;
    SequenceNumber seq;
    UnPackSequenceAndType(tag, &seq, &type);
    // Unsupported operations.
    assert(type != kTypeBlobIndex);
    assert(type != kTypeWideColumnEntity);
    assert(type != kTypeValuePreferredSeqno);
    assert(type != kTypeDeletionWithTimestamp);
    if (UNLIKELY(callback != nullptr &&
                 callback->NeedToTrackNewerVersions(newer_version_present))) {
      callback->MaybeRecordNewerVersion(seq, type, newer_version_present);
    }
    if (!callback || callback->IsVisible(seq)) {
      if (*out_seq == kMaxSequenceNumber) {
        *out_seq = std::max(seq, *max_covering_tombstone_seq);
      }
      if (*max_covering_tombstone_seq > seq) {
        type = kTypeRangeDeletion;
      }
      switch (type) {
        case kTypeValue: {
          HandleTypeValue(lookup_user_key, iter->value(), iter->IsValuePinned(),
                          do_merge, merge_in_progress, merge_context,
                          moptions_.merge_operator, clock_,
                          moptions_.statistics, moptions_.info_log, s, value,
                          columns, is_blob_index);
          assert(seq <= read_seq);
          return /*found_final_value=*/true;
        }
        case kTypeDeletion:
        case kTypeSingleDeletion:
        case kTypeRangeDeletion: {
          HandleTypeDeletion(lookup_user_key, merge_in_progress, merge_context,
                             moptions_.merge_operator, clock_,
                             moptions_.statistics, moptions_.info_log, s, value,
                             columns);
          assert(seq <= read_seq);
          return /*found_final_value=*/true;
        }
        case kTypeMerge: {
          merge_in_progress = true;
          if (ReadOnlyMemTable::HandleTypeMerge(
                  lookup_user_key, iter->value(), iter->IsValuePinned(),
                  do_merge, merge_context, moptions_.merge_operator, clock_,
                  moptions_.statistics, moptions_.info_log, s, value,
                  columns)) {
            return true;
          }
          break;
        }
        default: {
          std::string msg(
              "Unrecognized or unsupported value type for "
              "WBWI-based memtable: " +
              std::to_string(static_cast<int>(type)) + ". ");
          msg.append("User key: " +
                     ExtractUserKey(iter->key()).ToString(/*hex=*/true) + ". ");
          msg.append("seq: " + std::to_string(seq) + ".");
          *s = Status::Corruption(msg.c_str());
          return /*found_final_value=*/true;
        }
      }
    }
    // Current key is a merge key or not visible
    assert(merge_in_progress || (callback && !callback->IsVisible(seq)));
    iter->Next();
  }
  if (!iter->status().ok() &&
      (s->ok() || s->IsMergeInProgress() || s->IsNotFound())) {
    *s = iter->status();
    // stop further look up
    return true;
  }
  if (merge_in_progress) {
    assert(s->ok() || s->IsMergeInProgress());
    *s = Status::MergeInProgress();
  }
  return /*found_final_value=*/false;
}

void WBWIMemTable::MultiGet(const ReadOptions& read_options,
                            MultiGetRange* range, ReadCallback* callback,
                            bool immutable_memtable,
                            const BlobFetcher* blob_fetcher) {
  (void)immutable_memtable;
  (void)blob_fetcher;
  // Should only be used as immutable memtable.
  assert(immutable_memtable);
  // IngestWriteBatchWithIndex() is rejected while any live column family has
  // blob direct write enabled, so WBWI should never need blob resolution.
  assert(blob_fetcher == nullptr);
  // TODO: reuse the InternalIterator created in Get().
  for (auto iter = range->begin(); iter != range->end(); ++iter) {
    SequenceNumber dummy_seq = 0;
    bool found_final_value = GetImpl(
        *iter->lkey, iter->value ? iter->value->GetSelf() : nullptr,
        iter->columns, iter->timestamp, iter->s, &(iter->merge_context),
        &(iter->max_covering_tombstone_seq), &dummy_seq, read_options, true,
        callback, nullptr, true, /*blob_fetcher=*/nullptr,
        callback != nullptr && callback->GetMetadataReadBounds() != nullptr
            ? &iter->newer_version_present
            : nullptr);
    if (found_final_value) {
      if (iter->s->ok() || iter->s->IsNotFound()) {
        if (iter->value) {
          iter->value->PinSelf();
          range->AddValueSize(iter->value->size());
        } else {
          assert(iter->columns);
          range->AddValueSize(iter->columns->payload_size());
        }
      }
      range->MarkKeyDone(iter);
      if (range->GetValueSize() > read_options.value_size_soft_limit) {
        // Set all remaining keys in range to Abort
        for (auto range_iter = range->begin(); range_iter != range->end();
             ++range_iter) {
          range->MarkKeyDone(range_iter);
          *(range_iter->s) = Status::Aborted();
        }
        break;
      }
    }
  }
}
}  // namespace ROCKSDB_NAMESPACE
