//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/udt_util.h"

#include "db/dbformat.h"
#include "rocksdb/types.h"
#include "util/coding.h"
#include "util/write_batch_util.h"

namespace ROCKSDB_NAMESPACE {
namespace {
enum class RecoveryType {
  kNoop,
  kUnrecoverable,
  kStripTimestamp,
  kPadTimestamp,
};

RecoveryType GetRecoveryType(const size_t running_ts_sz,
                             const std::optional<size_t>& recorded_ts_sz) {
  if (running_ts_sz == 0) {
    if (!recorded_ts_sz.has_value()) {
      // A column family id not recorded is equivalent to that column family has
      // zero timestamp size.
      return RecoveryType::kNoop;
    }
    return RecoveryType::kStripTimestamp;
  }

  assert(running_ts_sz != 0);

  if (!recorded_ts_sz.has_value()) {
    return RecoveryType::kPadTimestamp;
  }

  if (running_ts_sz != *recorded_ts_sz) {
    return RecoveryType::kUnrecoverable;
  }

  return RecoveryType::kNoop;
}

bool AllRunningColumnFamiliesConsistent(
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz) {
  for (const auto& [cf_id, ts_sz] : running_ts_sz) {
    auto record_it = record_ts_sz.find(cf_id);
    RecoveryType recovery_type =
        GetRecoveryType(ts_sz, record_it != record_ts_sz.end()
                                   ? std::optional<size_t>(record_it->second)
                                   : std::nullopt);
    if (recovery_type != RecoveryType::kNoop) {
      return false;
    }
  }
  return true;
}

Status CheckWriteBatchTimestampSizeConsistency(
    const WriteBatch* batch,
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz,
    TimestampSizeConsistencyMode check_mode, bool* ts_need_recovery) {
  std::vector<uint32_t> column_family_ids;
  Status status =
      CollectColumnFamilyIdsFromWriteBatch(*batch, &column_family_ids);
  if (!status.ok()) {
    return status;
  }
  for (const auto& cf_id : column_family_ids) {
    auto running_iter = running_ts_sz.find(cf_id);
    if (running_iter == running_ts_sz.end()) {
      // Ignore dropped column family referred to in a WriteBatch regardless of
      // its consistency.
      continue;
    }
    auto record_iter = record_ts_sz.find(cf_id);
    RecoveryType recovery_type = GetRecoveryType(
        running_iter->second, record_iter != record_ts_sz.end()
                                  ? std::optional<size_t>(record_iter->second)
                                  : std::nullopt);
    if (recovery_type != RecoveryType::kNoop) {
      if (check_mode == TimestampSizeConsistencyMode::kVerifyConsistency) {
        return Status::InvalidArgument(
            "WriteBatch contains timestamp size inconsistency.");
      }

      if (recovery_type == RecoveryType::kUnrecoverable) {
        return Status::InvalidArgument(
            "WriteBatch contains unrecoverable timestamp size inconsistency.");
      }

      // If any column family needs reconciliation, it will mark the whole
      // WriteBatch to need recovery and rebuilt.
      *ts_need_recovery = true;
    }
  }
  return Status::OK();
}

enum class ToggleUDT {
  kUnchanged,
  kEnableUDT,
  kDisableUDT,
  kInvalidChange,
};

ToggleUDT CompareComparator(const Comparator* new_comparator,
                            const std::string& old_comparator_name) {
  static const char* kUDTSuffix = ".u64ts";
  static const Slice kSuffixSlice = kUDTSuffix;
  static const size_t kSuffixSize = 6;
  size_t ts_sz = new_comparator->timestamp_size();
  (void)ts_sz;
  Slice new_ucmp_name(new_comparator->Name());
  Slice old_ucmp_name(old_comparator_name);
  if (new_ucmp_name.compare(old_ucmp_name) == 0) {
    return ToggleUDT::kUnchanged;
  }
  if (new_ucmp_name.size() == old_ucmp_name.size() + kSuffixSize &&
      new_ucmp_name.starts_with(old_ucmp_name) &&
      new_ucmp_name.ends_with(kSuffixSlice)) {
    assert(ts_sz == 8);
    return ToggleUDT::kEnableUDT;
  }
  if (old_ucmp_name.size() == new_ucmp_name.size() + kSuffixSize &&
      old_ucmp_name.starts_with(new_ucmp_name) &&
      old_ucmp_name.ends_with(kSuffixSlice)) {
    assert(ts_sz == 0);
    return ToggleUDT::kDisableUDT;
  }
  return ToggleUDT::kInvalidChange;
}
}  // namespace

TimestampRecoveryHandler::TimestampRecoveryHandler(
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz, bool seq_per_batch,
    bool batch_per_txn)
    : running_ts_sz_(running_ts_sz),
      record_ts_sz_(record_ts_sz),
      // Write after commit currently uses one seq per key (instead of per
      // batch). So seq_per_batch being false indicates write_after_commit
      // approach.
      write_after_commit_(!seq_per_batch),
      // WriteUnprepared can write multiple WriteBatches per transaction, so
      // batch_per_txn being false indicates write_before_prepare.
      write_before_prepare_(!batch_per_txn),
      new_batch_(new WriteBatch()),
      handler_valid_(true),
      new_batch_diff_from_orig_batch_(false) {}

Status TimestampRecoveryHandler::PutCF(uint32_t cf, const Slice& key,
                                       const Slice& value) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::Put(new_batch_.get(), cf, new_key, value);
}

Status TimestampRecoveryHandler::PutEntityCF(uint32_t cf, const Slice& key,
                                             const Slice& entity) {
  std::string new_key_buf;
  Slice new_key;
  Status status = TimestampRecoveryHandler::ReconcileTimestampDiscrepancy(
      cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  Slice entity_copy = entity;
  WideColumns columns;
  if (!WideColumnSerialization::DeserializeSimple(entity_copy, columns).ok()) {
    return Status::Corruption("Unable to deserialize entity",
                              entity.ToString(/* hex */ true));
  }

  return WriteBatchInternal::PutEntity(new_batch_.get(), cf, new_key, columns);
}

Status TimestampRecoveryHandler::TimedPutCF(uint32_t cf, const Slice& key,
                                            const Slice& value,
                                            uint64_t write_time) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::TimedPut(new_batch_.get(), cf, new_key, value,
                                      write_time);
}

Status TimestampRecoveryHandler::DeleteCF(uint32_t cf, const Slice& key) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::Delete(new_batch_.get(), cf, new_key);
}

Status TimestampRecoveryHandler::SingleDeleteCF(uint32_t cf, const Slice& key) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::SingleDelete(new_batch_.get(), cf, new_key);
}

Status TimestampRecoveryHandler::DeleteRangeCF(uint32_t cf,
                                               const Slice& begin_key,
                                               const Slice& end_key) {
  std::string new_begin_key_buf;
  Slice new_begin_key;
  std::string new_end_key_buf;
  Slice new_end_key;
  Status status = ReconcileTimestampDiscrepancy(
      cf, begin_key, &new_begin_key_buf, &new_begin_key);
  if (!status.ok()) {
    return status;
  }
  status = ReconcileTimestampDiscrepancy(cf, end_key, &new_end_key_buf,
                                         &new_end_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::DeleteRange(new_batch_.get(), cf, new_begin_key,
                                         new_end_key);
}

Status TimestampRecoveryHandler::MergeCF(uint32_t cf, const Slice& key,
                                         const Slice& value) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::Merge(new_batch_.get(), cf, new_key, value);
}

Status TimestampRecoveryHandler::PutBlobIndexCF(uint32_t cf, const Slice& key,
                                                const Slice& value) {
  std::string new_key_buf;
  Slice new_key;
  Status status =
      ReconcileTimestampDiscrepancy(cf, key, &new_key_buf, &new_key);
  if (!status.ok()) {
    return status;
  }
  return WriteBatchInternal::PutBlobIndex(new_batch_.get(), cf, new_key, value);
}

Status TimestampRecoveryHandler::MarkBeginPrepare(bool unprepare) {
  // Transaction policy change requires empty WAL and User-defined timestamp is
  // only supported for write committed txns.
  // WriteBatch::Iterate has will handle this based on
  // handler->WriteAfterCommit() and handler->WriteBeforePrepare().
  if (unprepare) {
    return Status::InvalidArgument(
        "Handle user defined timestamp setting change is not supported for"
        "write unprepared policy. The WAL must be emptied.");
  }
  return WriteBatchInternal::InsertBeginPrepare(new_batch_.get(),
                                                write_after_commit_,
                                                /* unprepared_batch */ false);
}

Status TimestampRecoveryHandler::MarkEndPrepare(const Slice& name) {
  return WriteBatchInternal::InsertEndPrepare(new_batch_.get(), name);
}

Status TimestampRecoveryHandler::MarkCommit(const Slice& name) {
  return WriteBatchInternal::MarkCommit(new_batch_.get(), name);
}

Status TimestampRecoveryHandler::MarkCommitWithTimestamp(
    const Slice& name, const Slice& commit_ts) {
  return WriteBatchInternal::MarkCommitWithTimestamp(new_batch_.get(), name,
                                                     commit_ts);
}

Status TimestampRecoveryHandler::MarkRollback(const Slice& name) {
  return WriteBatchInternal::MarkRollback(new_batch_.get(), name);
}

Status TimestampRecoveryHandler::MarkNoop(bool /*empty_batch*/) {
  return WriteBatchInternal::InsertNoop(new_batch_.get());
}

Status TimestampRecoveryHandler::ReconcileTimestampDiscrepancy(
    uint32_t cf, const Slice& key, std::string* new_key_buf, Slice* new_key) {
  assert(handler_valid_);
  auto running_iter = running_ts_sz_.find(cf);
  if (running_iter == running_ts_sz_.end()) {
    // The column family referred to by the WriteBatch is no longer running.
    // Copy over the entry as is to the new WriteBatch.
    *new_key = key;
    return Status::OK();
  }
  size_t running_ts_sz = running_iter->second;
  auto record_iter = record_ts_sz_.find(cf);
  std::optional<size_t> record_ts_sz =
      record_iter != record_ts_sz_.end()
          ? std::optional<size_t>(record_iter->second)
          : std::nullopt;
  RecoveryType recovery_type = GetRecoveryType(running_ts_sz, record_ts_sz);

  switch (recovery_type) {
    case RecoveryType::kNoop:
      *new_key = key;
      break;
    case RecoveryType::kStripTimestamp:
      assert(record_ts_sz.has_value());
      *new_key = StripTimestampFromUserKey(key, *record_ts_sz);
      new_batch_diff_from_orig_batch_ = true;
      break;
    case RecoveryType::kPadTimestamp:
      AppendKeyWithMinTimestamp(new_key_buf, key, running_ts_sz);
      *new_key = *new_key_buf;
      new_batch_diff_from_orig_batch_ = true;
      break;
    case RecoveryType::kUnrecoverable:
      return Status::InvalidArgument(
          "Unrecoverable timestamp size inconsistency encountered by "
          "TimestampRecoveryHandler.");
    default:
      assert(false);
  }
  return Status::OK();
}

Status HandleWriteBatchTimestampSizeDifference(
    const WriteBatch* batch,
    const UnorderedMap<uint32_t, size_t>& running_ts_sz,
    const UnorderedMap<uint32_t, size_t>& record_ts_sz,
    TimestampSizeConsistencyMode check_mode, bool seq_per_batch,
    bool batch_per_txn, std::unique_ptr<WriteBatch>* new_batch) {
  // Quick path to bypass checking the WriteBatch.
  if (AllRunningColumnFamiliesConsistent(running_ts_sz, record_ts_sz)) {
    return Status::OK();
  }
  bool need_recovery = false;
  Status status = CheckWriteBatchTimestampSizeConsistency(
      batch, running_ts_sz, record_ts_sz, check_mode, &need_recovery);
  if (!status.ok()) {
    return status;
  } else if (need_recovery) {
    assert(new_batch);
    SequenceNumber sequence = WriteBatchInternal::Sequence(batch);
    TimestampRecoveryHandler recovery_handler(running_ts_sz, record_ts_sz,
                                              seq_per_batch, batch_per_txn);
    status = batch->Iterate(&recovery_handler);
    if (!status.ok()) {
      return status;
    } else {
      *new_batch = recovery_handler.TransferNewBatch();
      WriteBatchInternal::SetSequence(new_batch->get(), sequence);
    }
  }
  return Status::OK();
}

Status ValidateUserDefinedTimestampsOptions(
    const Comparator* new_comparator, const std::string& old_comparator_name,
    bool new_persist_udt, bool old_persist_udt,
    bool* mark_sst_files_has_no_udt) {
  size_t ts_sz = new_comparator->timestamp_size();
  ToggleUDT res = CompareComparator(new_comparator, old_comparator_name);
  switch (res) {
    case ToggleUDT::kUnchanged:
      if (old_persist_udt == new_persist_udt) {
        return Status::OK();
      }
      if (ts_sz == 0) {
        return Status::OK();
      }
      return Status::InvalidArgument(
          "Cannot toggle the persist_user_defined_timestamps flag for a column "
          "family with user-defined timestamps feature enabled.");
    case ToggleUDT::kEnableUDT:
      if (!new_persist_udt) {
        *mark_sst_files_has_no_udt = true;
        return Status::OK();
      }
      return Status::InvalidArgument(
          "Cannot open a column family and enable user-defined timestamps "
          "feature without setting persist_user_defined_timestamps flag to "
          "false.");
    case ToggleUDT::kDisableUDT:
      if (!old_persist_udt) {
        return Status::OK();
      }
      return Status::InvalidArgument(
          "Cannot open a column family and disable user-defined timestamps "
          "feature if its existing persist_user_defined_timestamps flag is not "
          "false.");
    case ToggleUDT::kInvalidChange:
      return Status::InvalidArgument(
          new_comparator->Name(),
          "does not match existing comparator " + old_comparator_name);
    default:
      break;
  }
  return Status::InvalidArgument(
      "Unsupported user defined timestamps settings change.");
}

void GetFullHistoryTsLowFromU64CutoffTs(Slice* cutoff_ts,
                                        std::string* full_history_ts_low) {
  uint64_t cutoff_udt_ts = 0;
  [[maybe_unused]] bool format_res = GetFixed64(cutoff_ts, &cutoff_udt_ts);
  assert(format_res);
  PutFixed64(full_history_ts_low, cutoff_udt_ts + 1);
}

void GetU64CutoffTsFromFullHistoryTsLow(Slice* full_history_ts_low,
                                        std::string* cutoff_ts) {
  uint64_t full_history_ts_low_int = 0;
  [[maybe_unused]] bool format_res =
      GetFixed64(full_history_ts_low, &full_history_ts_low_int);
  assert(format_res);
  assert(full_history_ts_low_int > 0);
  if (full_history_ts_low_int > 0) {
    PutFixed64(cutoff_ts, full_history_ts_low_int - 1);
  } else {
    PutFixed64(cutoff_ts, 0);
  }
}

std::tuple<OptSlice, OptSlice> MaybeAddTimestampsToRange(
    const OptSlice& start, const OptSlice& end, size_t ts_sz,
    std::string* start_with_ts, std::string* end_with_ts, bool exclusive_end) {
  OptSlice ret_start;
  OptSlice ret_end;
  if (start) {
    if (ts_sz == 0) {
      ret_start = *start;
    } else {
      // Maximum timestamp means including all keys with any timestamp for start
      AppendKeyWithMaxTimestamp(start_with_ts, *start, ts_sz);
      ret_start = Slice(*start_with_ts);
    }
  }
  if (end) {
    if (ts_sz == 0) {
      ret_end = *end;
    } else {
      if (exclusive_end) {
        // Append a maximum timestamp as the range limit is exclusive:
        // [start, end)
        AppendKeyWithMaxTimestamp(end_with_ts, *end, ts_sz);
      } else {
        // Append a minimum timestamp to end so the range limit is inclusive:
        // [start, end]
        AppendKeyWithMinTimestamp(end_with_ts, *end, ts_sz);
      }
      ret_end = Slice(*end_with_ts);
    }
  }
  return std::make_tuple(ret_start, ret_end);
}
}  // namespace ROCKSDB_NAMESPACE
