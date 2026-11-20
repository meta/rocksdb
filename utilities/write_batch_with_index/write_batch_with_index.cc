//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/write_batch_with_index.h"

#include <cassert>
#include <memory>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/wide/wide_columns_helper.h"
#include "memory/arena.h"
#include "rocksdb/comparator.h"
#include "rocksdb/iterator.h"
#include "util/cast_util.h"
#include "util/string_util.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"

namespace ROCKSDB_NAMESPACE {
struct WriteBatchWithIndex::Rep {
  explicit Rep(const Comparator* index_comparator, size_t reserved_bytes = 0,
               size_t max_bytes = 0, bool _overwrite_key = false,
               size_t protection_bytes_per_key = 0)
      : write_batch(reserved_bytes, max_bytes, protection_bytes_per_key,
                    index_comparator ? index_comparator->timestamp_size() : 0),
        comparator(index_comparator, &write_batch),
        skip_list(comparator, &arena),
        last_sub_batch_offset(0),
        sub_batch_cnt(1),
        overwrite_key(_overwrite_key),
        op_count(0) {}
  ReadableWriteBatch write_batch;
  WriteBatchEntryComparator comparator;
  Arena arena;
  WriteBatchEntrySkipList skip_list;
  // The starting offset of the last sub-batch. A sub-batch starts right before
  // inserting a key that is a duplicate of a key in the last sub-batch. Zero,
  // the default, means that no duplicate key is detected so far.
  size_t last_sub_batch_offset;
  // Total number of sub-batches in the write batch. Default is 1.
  size_t sub_batch_cnt;

  const bool overwrite_key;
  // Tracks ids of CFs that have updates in this WBWI, number of updates and
  // number of overwritten single deletions per cf. Useful for WBWIMemTable
  // when this WBWI is ingested into a DB.
  std::unordered_map<uint32_t, WriteBatchWithIndex::CFStat> cf_id_to_stat;
  size_t op_count;

  // In overwrite mode, find the existing entry for the same key and update it
  // to point to the current entry if this is not a Merge operation.
  // Return true if the key is found and updated.
  // most_recent_entry_update_count will be set to the update count of the
  // most recent index entry for `key` if exists.
  bool UpdateExistingEntryWithCfId(uint32_t column_family_id, const Slice& key,
                                   WriteType type, size_t last_entry_offset,
                                   uint32_t* most_recent_entry_update_count);

  // Add or update the index for the given `key` of `type`
  // at `last_entry_offset` in the write batch.
  //
  // The index is updated in place if this WBWI is in overwrite mode
  // and this is not a Merge operation.
  //
  // If `cf_cmp` is provided and there is no compartor stored
  // for this cf, it will be stored as the comparator to use for this cf.
  void AddOrUpdateIndexWithCfId(uint32_t cf_id, const Slice& key,
                                WriteType type, size_t last_entry_offset,
                                const Comparator* cf_cmp = nullptr);

  void AddOrUpdateIndex(ColumnFamilyHandle* cfh, const Slice& key,
                        WriteType type, size_t last_entry_offset) {
    AddOrUpdateIndexWithCfId(GetColumnFamilyID(cfh), key, type,
                             last_entry_offset,
                             GetColumnFamilyUserComparator(cfh));
  }

  void AddOrUpdateIndex(const Slice& key, WriteType type,
                        size_t last_entry_offset) {
    AddOrUpdateIndexWithCfId(0, key, type, last_entry_offset);
  }

  // Add a new index entry pointing to the the entry at `last_entry_offset`
  // in the write batch. `most_recent_entry_update_count` will be used to
  // initialize the update count of the new index entry.
  void AddNewEntry(uint32_t column_family_id, WriteType type,
                   size_t last_entry_offset,
                   uint32_t most_recent_entry_update_count);

  // Clear all updates buffered in this batch.
  void Clear();
  void ClearIndex();

  // Rebuild index by reading all records from the batch.
  // Returns non-ok status on corruption.
  Status ReBuildIndex();
};

bool WriteBatchWithIndex::Rep::UpdateExistingEntryWithCfId(
    uint32_t column_family_id, const Slice& key, WriteType type,
    size_t last_entry_offset, uint32_t* most_recent_entry_update_count) {
  assert(most_recent_entry_update_count);
  if (!overwrite_key) {
    return false;
  }

  WBWIIteratorImpl iter(column_family_id, &skip_list, &write_batch,
                        &comparator);
  iter.Seek(key);
  if (!iter.Valid()) {
    return false;
  } else if (!iter.MatchesKey(column_family_id, key)) {
    return false;
  }
  // Seek() and MatchesKey() guarantees that we are at the first entry with
  // key == `key`, which is the most recently update to `key`.
  WriteBatchIndexEntry* most_recent_entry =
      const_cast<WriteBatchIndexEntry*>(iter.GetRawEntry());
  *most_recent_entry_update_count = most_recent_entry->update_count;

  if (LIKELY(last_sub_batch_offset <= most_recent_entry->offset)) {
    last_sub_batch_offset = last_entry_offset;
    sub_batch_cnt++;
  }
  if (most_recent_entry->has_single_del &&
      !most_recent_entry->has_overwritten_single_del) {
    cf_id_to_stat[column_family_id].overwritten_sd_count++;
    most_recent_entry->has_overwritten_single_del = true;
  }
  if (type == kSingleDeleteRecord) {
    most_recent_entry->has_single_del = true;
  }
  // Some sanity check for using Merge and SD on the same key.
  if (iter.Entry().type == kSingleDeleteRecord) {
    assert(type != kMergeRecord);
  }
  if (type == kMergeRecord) {
    assert(iter.Entry().type != kSingleDeleteRecord);
    return false;
  } else {
    // We still increment the update count when updating in-place. This is
    // useful for WBWIMemTable when it needs to emit overwritten SingleDeletes.
    // The overwritten SingleDelete will be assigned sequence number as if
    // update_count is 0. So any later update should have update_count > 0.
    most_recent_entry->update_count++;
    most_recent_entry->offset = last_entry_offset;
    return true;
  }
}

void WriteBatchWithIndex::Rep::AddOrUpdateIndexWithCfId(
    uint32_t cf_id, const Slice& key, WriteType type, size_t last_entry_offset,
    const Comparator* cf_cmp) {
  op_count++;
  uint32_t update_count = 0;
  if (!UpdateExistingEntryWithCfId(cf_id, key, type, last_entry_offset,
                                   &update_count)) {
    if (cf_cmp) {
      comparator.SetComparatorForCF(cf_id, cf_cmp);
    }
    AddNewEntry(cf_id, type, last_entry_offset, update_count + 1);
  }
}

void WriteBatchWithIndex::Rep::AddNewEntry(uint32_t column_family_id,
                                           WriteType type,
                                           size_t last_entry_offset,
                                           uint32_t update_count) {
  const std::string& wb_data = write_batch.Data();
  Slice entry_ptr = Slice(wb_data.data() + last_entry_offset,
                          wb_data.size() - last_entry_offset);
  // Extract key
  Slice key;
  bool success =
      ReadKeyFromWriteBatchEntry(&entry_ptr, &key, column_family_id != 0);
#ifdef NDEBUG
  (void)success;
#endif
  assert(success);

  const Comparator* const ucmp = comparator.GetComparator(column_family_id);
  size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;

  if (ts_sz > 0) {
    key.remove_suffix(ts_sz);
  }

  auto* mem = arena.Allocate(sizeof(WriteBatchIndexEntry));
  auto* index_entry = new (mem) WriteBatchIndexEntry(
      last_entry_offset, column_family_id, key.data() - wb_data.data(),
      key.size(), update_count);
  skip_list.Insert(index_entry);

  if (type == kSingleDeleteRecord) {
    index_entry->has_single_del = true;
  }
  cf_id_to_stat[column_family_id].entry_count++;
}

void WriteBatchWithIndex::Rep::Clear() {
  write_batch.Clear();
  ClearIndex();
}

void WriteBatchWithIndex::Rep::ClearIndex() {
  skip_list.~WriteBatchEntrySkipList();
  arena.~Arena();
  new (&arena) Arena();
  new (&skip_list) WriteBatchEntrySkipList(comparator, &arena);
  last_sub_batch_offset = 0;
  sub_batch_cnt = 1;
  cf_id_to_stat.clear();
  op_count = 0;
}

Status WriteBatchWithIndex::Rep::ReBuildIndex() {
  Status s;

  ClearIndex();

  if (write_batch.Count() == 0) {
    // Nothing to re-index
    return s;
  }

  size_t offset = WriteBatchInternal::GetFirstOffset(&write_batch);

  Slice input(write_batch.Data());
  input.remove_prefix(offset);

  // Loop through all entries in Rep and add each one to the index
  uint32_t found = 0;
  while (s.ok() && !input.empty()) {
    Slice key;
    Slice value;
    Slice blob;
    Slice xid;
    uint32_t column_family_id = 0;  // default
    uint64_t unix_write_time = 0;
    char tag = 0;

    // set offset of current entry for call to AddNewEntry()
    size_t last_entry_offset = input.data() - write_batch.Data().data();

    s = ReadRecordFromWriteBatch(&input, &tag, &column_family_id, &key, &value,
                                 &blob, &xid, &unix_write_time);
    if (!s.ok()) {
      break;
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        found++;
        AddOrUpdateIndexWithCfId(column_family_id, key, kPutRecord,
                                 last_entry_offset);
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        found++;
        AddOrUpdateIndexWithCfId(column_family_id, key, kDeleteRecord,
                                 last_entry_offset);
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        found++;
        AddOrUpdateIndexWithCfId(column_family_id, key, kSingleDeleteRecord,
                                 last_entry_offset);
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        found++;
        AddOrUpdateIndexWithCfId(column_family_id, key, kMergeRecord,
                                 last_entry_offset);
        break;
      case kTypeLogData:
      case kTypeBeginPrepareXID:
      case kTypeBeginPersistedPrepareXID:
      case kTypeBeginUnprepareXID:
      case kTypeEndPrepareXID:
      case kTypeCommitXID:
      case kTypeCommitXIDAndTimestamp:
      case kTypeRollbackXID:
      case kTypeNoop:
        break;
      case kTypeColumnFamilyWideColumnEntity:
      case kTypeWideColumnEntity:
        found++;
        AddOrUpdateIndexWithCfId(column_family_id, key, kPutEntityRecord,
                                 last_entry_offset);
        break;
      case kTypeColumnFamilyValuePreferredSeqno:
      case kTypeValuePreferredSeqno:
        // TimedPut is not supported in Transaction APIs.
        return Status::Corruption(
            "unexpected WriteBatch tag in ReBuildIndex",
            std::to_string(static_cast<unsigned int>(tag)));
      default:
        return Status::Corruption(
            "unknown WriteBatch tag in ReBuildIndex",
            std::to_string(static_cast<unsigned int>(tag)));
    }
  }

  if (s.ok() && found != write_batch.Count()) {
    s = Status::Corruption("WriteBatch has wrong count");
  }

  return s;
}

WriteBatchWithIndex::WriteBatchWithIndex(
    const Comparator* default_index_comparator, size_t reserved_bytes,
    bool overwrite_key, size_t max_bytes, size_t protection_bytes_per_key)
    : rep(new Rep(default_index_comparator, reserved_bytes, max_bytes,
                  overwrite_key, protection_bytes_per_key)) {}

WriteBatchWithIndex::~WriteBatchWithIndex() = default;

WriteBatchWithIndex::WriteBatchWithIndex(WriteBatchWithIndex&&) = default;

WriteBatchWithIndex& WriteBatchWithIndex::operator=(WriteBatchWithIndex&&) =
    default;

WriteBatch* WriteBatchWithIndex::GetWriteBatch() { return &rep->write_batch; }

size_t WriteBatchWithIndex::SubBatchCnt() { return rep->sub_batch_cnt; }

WBWIIterator* WriteBatchWithIndex::NewIterator() {
  return new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch,
                              &(rep->comparator));
}

WBWIIterator* WriteBatchWithIndex::NewIterator(
    ColumnFamilyHandle* column_family) {
  return new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                              &(rep->skip_list), &rep->write_batch,
                              &(rep->comparator));
}

WBWIIterator* WriteBatchWithIndex::NewIterator(uint32_t cf_id) const {
  return new WBWIIteratorImpl(cf_id, &(rep->skip_list), &rep->write_batch,
                              &(rep->comparator));
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(
    ColumnFamilyHandle* column_family, Iterator* base_iterator,
    const ReadOptions* read_options) {
  WBWIIteratorImpl* wbwiii;
  if (read_options != nullptr) {
    wbwiii = new WBWIIteratorImpl(
        GetColumnFamilyID(column_family), &(rep->skip_list), &rep->write_batch,
        &rep->comparator, read_options->iterate_lower_bound,
        read_options->iterate_upper_bound);
  } else {
    wbwiii = new WBWIIteratorImpl(GetColumnFamilyID(column_family),
                                  &(rep->skip_list), &rep->write_batch,
                                  &rep->comparator);
  }

  return new BaseDeltaIterator(column_family, base_iterator, wbwiii,
                               GetColumnFamilyUserComparator(column_family),
                               read_options);
}

Iterator* WriteBatchWithIndex::NewIteratorWithBase(
    Iterator* base_iterator, const ReadOptions* read_options) {
  WBWIIteratorImpl* wbwiii;
  // default column family's comparator
  if (read_options != nullptr) {
    wbwiii = new WBWIIteratorImpl(
        0, &(rep->skip_list), &rep->write_batch, &rep->comparator,
        read_options->iterate_lower_bound, read_options->iterate_upper_bound);
  } else {
    wbwiii = new WBWIIteratorImpl(0, &(rep->skip_list), &rep->write_batch,
                                  &rep->comparator);
  }

  return new BaseDeltaIterator(nullptr, base_iterator, wbwiii,
                               rep->comparator.default_comparator(),
                               /* read_options */ nullptr);
}

Status WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.Put(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kPutRecord, last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.Put(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kPutRecord, last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::Put(ColumnFamilyHandle* column_family,
                                const Slice& /*key*/, const Slice& /*ts*/,
                                const Slice& /*value*/) {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be nullptr");
  }
  // TODO: support WBWI::Put() with timestamp.
  return Status::NotSupported();
}

Status WriteBatchWithIndex::PutEntity(ColumnFamilyHandle* column_family,
                                      const Slice& key,
                                      const WideColumns& columns) {
  assert(rep);
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  const Status s = rep->write_batch.PutEntity(column_family, key, columns);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kPutEntityRecord,
                          last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& key) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.Delete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kDeleteRecord, last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(const Slice& key) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.Delete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kDeleteRecord, last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::Delete(ColumnFamilyHandle* column_family,
                                   const Slice& /*key*/, const Slice& /*ts*/) {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be nullptr");
  }
  // TODO: support WBWI::Delete() with timestamp.
  return Status::NotSupported();
}

Status WriteBatchWithIndex::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& key) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.SingleDelete(column_family, key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kSingleDeleteRecord,
                          last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(const Slice& key) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.SingleDelete(key);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kSingleDeleteRecord, last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::SingleDelete(ColumnFamilyHandle* column_family,
                                         const Slice& /*key*/,
                                         const Slice& /*ts*/) {
  if (!column_family) {
    return Status::InvalidArgument("column family handle cannot be nullptr");
  }
  // TODO: support WBWI::SingleDelete() with timestamp.
  return Status::NotSupported();
}

Status WriteBatchWithIndex::Merge(ColumnFamilyHandle* column_family,
                                  const Slice& key, const Slice& value) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.Merge(column_family, key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(column_family, key, kMergeRecord, last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::Merge(const Slice& key, const Slice& value) {
  size_t last_entry_offset = rep->write_batch.GetDataSize();
  auto s = rep->write_batch.Merge(key, value);
  if (s.ok()) {
    rep->AddOrUpdateIndex(key, kMergeRecord, last_entry_offset);
  }
  return s;
}

Status WriteBatchWithIndex::PutLogData(const Slice& blob) {
  return rep->write_batch.PutLogData(blob);
}

void WriteBatchWithIndex::Clear() { rep->Clear(); }

namespace {
Status PostprocessStatusBatchOnly(const Status& s,
                                  WBWIIteratorImpl::Result result) {
  if (result == WBWIIteratorImpl::kDeleted ||
      result == WBWIIteratorImpl::kNotFound) {
    s.PermitUncheckedError();
    return Status::NotFound();
  }

  if (result == WBWIIteratorImpl::kMergeInProgress) {
    s.PermitUncheckedError();
    return Status::MergeInProgress();
  }

  assert(result == WBWIIteratorImpl::kFound ||
         result == WBWIIteratorImpl::kError);
  return s;
}
}  // anonymous namespace

Status WriteBatchWithIndex::GetFromBatch(ColumnFamilyHandle* column_family,
                                         const DBOptions& /* options */,
                                         const Slice& key, std::string* value) {
  MergeContext merge_context;
  Status s;
  auto result = WriteBatchWithIndexInternal::GetFromBatch(
      this, column_family, key, &merge_context, value, &s);

  return PostprocessStatusBatchOnly(s, result);
}

Status WriteBatchWithIndex::GetEntityFromBatch(
    ColumnFamilyHandle* column_family, const Slice& key,
    PinnableWideColumns* columns) {
  if (!column_family) {
    return Status::InvalidArgument(
        "Cannot call GetEntityFromBatch without a column family handle");
  }

  if (!columns) {
    return Status::InvalidArgument(
        "Cannot call GetEntityFromBatch without a PinnableWideColumns object");
  }

  MergeContext merge_context;
  Status s;
  auto result = WriteBatchWithIndexInternal::GetEntityFromBatch(
      this, column_family, key, &merge_context, columns, &s);

  return PostprocessStatusBatchOnly(s, result);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s = GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                             &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, db->DefaultColumnFamily(), key,
                           pinnable_val);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              std::string* value) {
  assert(value != nullptr);
  PinnableSlice pinnable_val(value);
  assert(!pinnable_val.IsPinned());
  auto s =
      GetFromBatchAndDB(db, read_options, column_family, key, &pinnable_val);
  if (s.ok() && pinnable_val.IsPinned()) {
    value->assign(pinnable_val.data(), pinnable_val.size());
  }  // else value is already assigned
  return s;
}

Status WriteBatchWithIndex::GetFromBatchAndDB(DB* db,
                                              const ReadOptions& read_options,
                                              ColumnFamilyHandle* column_family,
                                              const Slice& key,
                                              PinnableSlice* pinnable_val) {
  return GetFromBatchAndDB(db, read_options, column_family, key, pinnable_val,
                           nullptr);
}

void WriteBatchWithIndex::MergeAcrossBatchAndDBImpl(
    ColumnFamilyHandle* column_family, const Slice& key,
    const PinnableWideColumns& existing, const MergeContext& merge_context,
    std::string* value, PinnableWideColumns* columns, Status* status) {
  assert(value || columns);
  assert(!value || !columns);
  assert(status);

  if (status->ok()) {
    if (WideColumnsHelper::HasDefaultColumnOnly(existing.columns())) {
      *status = WriteBatchWithIndexInternal::MergeKeyWithBaseValue(
          column_family, key, MergeHelper::kPlainBaseValue,
          WideColumnsHelper::GetDefaultColumn(existing.columns()),
          merge_context, value, columns);
    } else {
      *status = WriteBatchWithIndexInternal::MergeKeyWithBaseValue(
          column_family, key, MergeHelper::kWideBaseValue, existing.columns(),
          merge_context, value, columns);
    }
  } else {
    assert(status->IsNotFound());
    *status = WriteBatchWithIndexInternal::MergeKeyWithNoBaseValue(
        column_family, key, merge_context, value, columns);
  }
}

void WriteBatchWithIndex::MergeAcrossBatchAndDB(
    ColumnFamilyHandle* column_family, const Slice& key,
    const PinnableWideColumns& existing, const MergeContext& merge_context,
    PinnableSlice* value, Status* status) {
  assert(value);
  assert(status);

  std::string result_value;
  constexpr PinnableWideColumns* result_entity = nullptr;
  MergeAcrossBatchAndDBImpl(column_family, key, existing, merge_context,
                            &result_value, result_entity, status);

  if (status->ok()) {
    *value->GetSelf() = std::move(result_value);
    value->PinSelf();
  }
}

void WriteBatchWithIndex::MergeAcrossBatchAndDB(
    ColumnFamilyHandle* column_family, const Slice& key,
    const PinnableWideColumns& existing, const MergeContext& merge_context,
    PinnableWideColumns* columns, Status* status) {
  assert(columns);
  assert(status);

  constexpr std::string* value = nullptr;
  MergeAcrossBatchAndDBImpl(column_family, key, existing, merge_context, value,
                            columns, status);
}

Status WriteBatchWithIndex::GetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableSlice* pinnable_val, ReadCallback* callback) {
  assert(db);
  assert(pinnable_val);

  if (!column_family) {
    column_family = db->DefaultColumnFamily();
  }

  const Comparator* const ucmp = rep->comparator.GetComparator(column_family);
  size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;
  if (ts_sz > 0 && !read_options.timestamp) {
    return Status::InvalidArgument("Must specify timestamp");
  }

  pinnable_val->Reset();

  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  MergeContext merge_context;
  Status s;

  auto result = WriteBatchWithIndexInternal::GetFromBatch(
      this, column_family, key, &merge_context, pinnable_val->GetSelf(), &s);

  if (result == WBWIIteratorImpl::kFound) {
    pinnable_val->PinSelf();
    return s;
  }

  assert(!s.ok() == (result == WBWIIteratorImpl::kError));
  if (result == WBWIIteratorImpl::kError) {
    return s;
  }

  if (result == WBWIIteratorImpl::kDeleted) {
    return Status::NotFound();
  }

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  DBImpl::GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;

  // Note: we have to retrieve all columns if we have to merge KVs from the
  // batch and the DB; otherwise, the default column is sufficient.
  PinnableWideColumns existing;

  if (result == WBWIIteratorImpl::kMergeInProgress) {
    get_impl_options.columns = &existing;
  } else {
    assert(result == WBWIIteratorImpl::kNotFound);
    get_impl_options.value = pinnable_val;
  }

  get_impl_options.callback = callback;
  s = static_cast_with_check<DBImpl>(db->GetRootDB())
          ->GetImpl(read_options, key, get_impl_options);

  if (result == WBWIIteratorImpl::kMergeInProgress) {
    if (s.ok() || s.IsNotFound()) {  // DB lookup succeeded
      MergeAcrossBatchAndDB(column_family, key, existing, merge_context,
                            pinnable_val, &s);
    }
  }

  return s;
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input) {
  MultiGetFromBatchAndDB(db, read_options, column_family, num_keys, keys,
                         values, statuses, sorted_input, nullptr);
}

void WriteBatchWithIndex::MultiGetFromBatchAndDB(
    DB* db, const ReadOptions& read_options, ColumnFamilyHandle* column_family,
    const size_t num_keys, const Slice* keys, PinnableSlice* values,
    Status* statuses, bool sorted_input, ReadCallback* callback) {
  assert(db);
  assert(keys);
  assert(values);
  assert(statuses);

  if (!column_family) {
    column_family = db->DefaultColumnFamily();
  }

  const Comparator* const ucmp = rep->comparator.GetComparator(column_family);
  size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;
  if (ts_sz > 0 && !read_options.timestamp) {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = Status::InvalidArgument("Must specify timestamp");
    }
    return;
  }

  struct MergeTuple {
    MergeTuple(const Slice& _key, Status* _s, MergeContext&& _merge_context,
               PinnableSlice* _value)
        : key(_key),
          s(_s),
          merge_context(std::move(_merge_context)),
          value(_value) {
      assert(s);
      assert(value);
    }

    Slice key;
    Status* s;
    PinnableWideColumns existing;
    MergeContext merge_context;
    PinnableSlice* value;
  };

  autovector<MergeTuple, MultiGetContext::MAX_BATCH_SIZE> merges;

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_contexts;

  // Since the lifetime of the WriteBatch is the same as that of the transaction
  // we cannot pin it as otherwise the returned value will not be available
  // after the transaction finishes.
  for (size_t i = 0; i < num_keys; ++i) {
    const Slice& key = keys[i];
    MergeContext merge_context;
    std::string batch_value;
    Status* const s = &statuses[i];
    auto result = WriteBatchWithIndexInternal::GetFromBatch(
        this, column_family, key, &merge_context, &batch_value, s);

    PinnableSlice* const pinnable_val = &values[i];
    pinnable_val->Reset();

    if (result == WBWIIteratorImpl::kFound) {
      *pinnable_val->GetSelf() = std::move(batch_value);
      pinnable_val->PinSelf();
      continue;
    }

    if (result == WBWIIteratorImpl::kDeleted) {
      *s = Status::NotFound();
      continue;
    }

    if (result == WBWIIteratorImpl::kError) {
      continue;
    }

    // Note: we have to retrieve all columns if we have to merge KVs from the
    // batch and the DB; otherwise, the default column is sufficient.
    // The columns field will be populated by the loop below to prevent issues
    // with dangling pointers.
    if (result == WBWIIteratorImpl::kMergeInProgress) {
      merges.emplace_back(key, s, std::move(merge_context), pinnable_val);
      key_contexts.emplace_back(column_family, key, /* value */ nullptr,
                                /* columns */ nullptr, /* timestamp */ nullptr,
                                s);
      continue;
    }

    assert(result == WBWIIteratorImpl::kNotFound);
    key_contexts.emplace_back(column_family, key, pinnable_val,
                              /* columns */ nullptr,
                              /* timestamp */ nullptr, s);
  }

  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  sorted_keys.reserve(key_contexts.size());

  size_t merges_idx = 0;
  for (KeyContext& key_context : key_contexts) {
    if (!key_context.value) {
      assert(*key_context.key == merges[merges_idx].key);

      key_context.columns = &merges[merges_idx].existing;
      ++merges_idx;
    }

    sorted_keys.emplace_back(&key_context);
  }

  // Did not find key in batch OR could not resolve Merges.  Try DB.
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->PrepareMultiGetKeys(sorted_keys.size(), sorted_input, &sorted_keys);
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->MultiGetWithCallback(read_options, column_family, callback,
                             &sorted_keys);

  for (const auto& merge : merges) {
    if (merge.s->ok() || merge.s->IsNotFound()) {  // DB lookup succeeded
      MergeAcrossBatchAndDB(column_family, merge.key, merge.existing,
                            merge.merge_context, merge.value, merge.s);
    }
  }
}

Status WriteBatchWithIndex::GetEntityFromBatchAndDB(
    DB* db, const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
    const Slice& key, PinnableWideColumns* columns, ReadCallback* callback) {
  if (!db) {
    return Status::InvalidArgument(
        "Cannot call GetEntityFromBatchAndDB without a DB object");
  }

  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kGetEntity) {
    return Status::InvalidArgument(
        "Can only call GetEntityFromBatchAndDB with `ReadOptions::io_activity` "
        "set to `Env::IOActivity::kUnknown` or `Env::IOActivity::kGetEntity`");
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kGetEntity;
  }

  if (!column_family) {
    return Status::InvalidArgument(
        "Cannot call GetEntityFromBatchAndDB without a column family handle");
  }

  const Comparator* const ucmp = rep->comparator.GetComparator(column_family);
  size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;
  if (ts_sz > 0) {
    if (!read_options.timestamp) {
      return Status::InvalidArgument("Must specify timestamp");
    }

    if (read_options.timestamp->size() != ts_sz) {
      return Status::InvalidArgument(
          "Timestamp size does not match the timestamp size of the "
          "column family");
    }
  } else {
    if (read_options.timestamp) {
      return Status::InvalidArgument(
          "Cannot specify timestamp since the column family does not have "
          "timestamps enabled");
    }
  }

  if (!columns) {
    return Status::InvalidArgument(
        "Cannot call GetEntityFromBatchAndDB without a PinnableWideColumns "
        "object");
  }

  columns->Reset();

  MergeContext merge_context;
  Status s;

  auto result = WriteBatchWithIndexInternal::GetEntityFromBatch(
      this, column_family, key, &merge_context, columns, &s);

  assert(!s.ok() == (result == WBWIIteratorImpl::kError));

  if (result == WBWIIteratorImpl::kFound ||
      result == WBWIIteratorImpl::kError) {
    return s;
  }

  if (result == WBWIIteratorImpl::kDeleted) {
    return Status::NotFound();
  }

  assert(result == WBWIIteratorImpl::kMergeInProgress ||
         result == WBWIIteratorImpl::kNotFound);

  PinnableWideColumns existing;

  DBImpl::GetImplOptions get_impl_options;
  get_impl_options.column_family = column_family;
  get_impl_options.columns =
      (result == WBWIIteratorImpl::kMergeInProgress) ? &existing : columns;
  get_impl_options.callback = callback;

  s = static_cast_with_check<DBImpl>(db->GetRootDB())
          ->GetImpl(read_options, key, get_impl_options);

  if (result == WBWIIteratorImpl::kMergeInProgress) {
    if (s.ok() || s.IsNotFound()) {  // DB lookup succeeded
      MergeAcrossBatchAndDB(column_family, key, existing, merge_context,
                            columns, &s);
    }
  }

  return s;
}

void WriteBatchWithIndex::MultiGetEntityFromBatchAndDB(
    DB* db, const ReadOptions& _read_options, ColumnFamilyHandle* column_family,
    size_t num_keys, const Slice* keys, PinnableWideColumns* results,
    Status* statuses, bool sorted_input, ReadCallback* callback) {
  assert(statuses);

  if (!db) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntityFromBatchAndDB without a DB object");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    return;
  }

  if (_read_options.io_activity != Env::IOActivity::kUnknown &&
      _read_options.io_activity != Env::IOActivity::kMultiGetEntity) {
    const Status s = Status::InvalidArgument(
        "Can only call MultiGetEntityFromBatchAndDB with "
        "`ReadOptions::io_activity` set to `Env::IOActivity::kUnknown` or "
        "`Env::IOActivity::kMultiGetEntity`");
    for (size_t i = 0; i < num_keys; ++i) {
      if (statuses[i].ok()) {
        statuses[i] = s;
      }
    }
    return;
  }

  ReadOptions read_options(_read_options);
  if (read_options.io_activity == Env::IOActivity::kUnknown) {
    read_options.io_activity = Env::IOActivity::kMultiGetEntity;
  }

  if (!column_family) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntityFromBatchAndDB without a column family "
        "handle");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    return;
  }

  const Comparator* const ucmp = rep->comparator.GetComparator(column_family);
  const size_t ts_sz = ucmp ? ucmp->timestamp_size() : 0;
  if (ts_sz > 0) {
    if (!read_options.timestamp) {
      const Status s = Status::InvalidArgument("Must specify timestamp");
      for (size_t i = 0; i < num_keys; ++i) {
        statuses[i] = s;
      }
      return;
    }

    if (read_options.timestamp->size() != ts_sz) {
      const Status s = Status::InvalidArgument(
          "Timestamp size does not match the timestamp size of the "
          "column family");
      for (size_t i = 0; i < num_keys; ++i) {
        statuses[i] = s;
      }
      return;
    }
  } else {
    if (read_options.timestamp) {
      const Status s = Status::InvalidArgument(
          "Cannot specify timestamp since the column family does not have "
          "timestamps enabled");
      for (size_t i = 0; i < num_keys; ++i) {
        statuses[i] = s;
      }
      return;
    }
  }

  if (!keys) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntityFromBatchAndDB without keys");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    return;
  }

  if (!results) {
    const Status s = Status::InvalidArgument(
        "Cannot call MultiGetEntityFromBatchAndDB without "
        "PinnableWideColumns objects");
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = s;
    }
    return;
  }

  struct MergeTuple {
    MergeTuple(const Slice& _key, Status* _s, MergeContext&& _merge_context,
               PinnableWideColumns* _columns)
        : key(_key),
          s(_s),
          merge_context(std::move(_merge_context)),
          columns(_columns) {
      assert(s);
      assert(columns);
    }

    Slice key;
    Status* s;
    PinnableWideColumns existing;
    MergeContext merge_context;
    PinnableWideColumns* columns;
  };

  autovector<MergeTuple, MultiGetContext::MAX_BATCH_SIZE> merges;

  autovector<KeyContext, MultiGetContext::MAX_BATCH_SIZE> key_contexts;

  for (size_t i = 0; i < num_keys; ++i) {
    const Slice& key = keys[i];
    MergeContext merge_context;
    PinnableWideColumns* const columns = &results[i];
    Status* const s = &statuses[i];

    columns->Reset();

    auto result = WriteBatchWithIndexInternal::GetEntityFromBatch(
        this, column_family, key, &merge_context, columns, s);

    if (result == WBWIIteratorImpl::kFound ||
        result == WBWIIteratorImpl::kError) {
      continue;
    }

    if (result == WBWIIteratorImpl::kDeleted) {
      *s = Status::NotFound();
      continue;
    }

    if (result == WBWIIteratorImpl::kMergeInProgress) {
      merges.emplace_back(key, s, std::move(merge_context), columns);

      // The columns field will be populated by the loop below to prevent issues
      // with dangling pointers.
      key_contexts.emplace_back(column_family, key, /* value */ nullptr,
                                /* columns */ nullptr, /* timestamp */ nullptr,
                                s);
      continue;
    }

    assert(result == WBWIIteratorImpl::kNotFound);
    key_contexts.emplace_back(column_family, key, /* value */ nullptr, columns,
                              /* timestamp */ nullptr, s);
  }

  autovector<KeyContext*, MultiGetContext::MAX_BATCH_SIZE> sorted_keys;
  sorted_keys.reserve(key_contexts.size());

  size_t merges_idx = 0;
  for (KeyContext& key_context : key_contexts) {
    if (!key_context.columns) {
      assert(*key_context.key == merges[merges_idx].key);

      key_context.columns = &merges[merges_idx].existing;
      ++merges_idx;
    }

    sorted_keys.emplace_back(&key_context);
  }

  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->PrepareMultiGetKeys(sorted_keys.size(), sorted_input, &sorted_keys);
  static_cast_with_check<DBImpl>(db->GetRootDB())
      ->MultiGetEntityWithCallback(read_options, column_family, callback,
                                   &sorted_keys);

  for (const auto& merge : merges) {
    if (merge.s->ok() || merge.s->IsNotFound()) {  // DB lookup succeeded
      MergeAcrossBatchAndDB(column_family, merge.key, merge.existing,
                            merge.merge_context, merge.columns, merge.s);
    }
  }
}

void WriteBatchWithIndex::SetSavePoint() { rep->write_batch.SetSavePoint(); }

Status WriteBatchWithIndex::RollbackToSavePoint() {
  Status s = rep->write_batch.RollbackToSavePoint();

  if (s.ok()) {
    rep->sub_batch_cnt = 1;
    rep->last_sub_batch_offset = 0;
    s = rep->ReBuildIndex();
  }

  return s;
}

Status WriteBatchWithIndex::PopSavePoint() {
  return rep->write_batch.PopSavePoint();
}

void WriteBatchWithIndex::SetMaxBytes(size_t max_bytes) {
  rep->write_batch.SetMaxBytes(max_bytes);
}

size_t WriteBatchWithIndex::GetDataSize() const {
  return rep->write_batch.GetDataSize();
}

const Comparator* WriteBatchWithIndexInternal::GetUserComparator(
    const WriteBatchWithIndex& wbwi, uint32_t cf_id) {
  const WriteBatchEntryComparator& ucmps = wbwi.rep->comparator;
  return ucmps.GetComparator(cf_id);
}

const std::unordered_map<uint32_t, WriteBatchWithIndex::CFStat>&
WriteBatchWithIndex::GetCFStats() const {
  return rep->cf_id_to_stat;
}

size_t WriteBatchWithIndex::GetWBWIOpCount() const { return rep->op_count; }

bool WriteBatchWithIndex::GetOverwriteKey() const { return rep->overwrite_key; }
}  // namespace ROCKSDB_NAMESPACE
