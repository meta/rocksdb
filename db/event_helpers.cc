//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/event_helpers.h"

#include "file/file_io_listener.h"
#include "rocksdb/convenience.h"
#include "rocksdb/listener.h"
#include "rocksdb/utilities/customizable_util.h"

namespace ROCKSDB_NAMESPACE {
Status EventListener::CreateFromString(const ConfigOptions& config_options,
                                       const std::string& id,
                                       std::shared_ptr<EventListener>* result) {
  return LoadSharedObject<EventListener>(config_options, id, result);
}

namespace {
template <class T>
inline T SafeDivide(T a, T b) {
  return b == 0 ? 0 : a / b;
}
}  // anonymous namespace

void EventHelpers::AppendCurrentTime(JSONWriter* jwriter) {
  *jwriter << "time_micros"
           << std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::system_clock::now().time_since_epoch())
                  .count();
}

void EventHelpers::NotifyTableFileCreationStarted(
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    const std::string& db_name, const std::string& cf_name,
    const std::string& file_path, int job_id, TableFileCreationReason reason) {
  if (listeners.empty()) {
    return;
  }
  TableFileCreationBriefInfo info;
  info.db_name = db_name;
  info.cf_name = cf_name;
  info.file_path = file_path;
  info.job_id = job_id;
  info.reason = reason;
  for (auto& listener : listeners) {
    const ScopedEventListenerCallback callback_scope(listener.get());
    listener->OnTableFileCreationStarted(info);
  }
}

void EventHelpers::NotifyOnBackgroundError(
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    BackgroundErrorReason reason, Status* bg_error, InstrumentedMutex* db_mutex,
    bool* auto_recovery) {
  if (listeners.empty()) {
    return;
  }
  db_mutex->AssertHeld();
  // release lock while notifying events
  db_mutex->Unlock();
  for (auto& listener : listeners) {
    const ScopedEventListenerCallback callback_scope(listener.get());
    listener->OnBackgroundError(reason, bg_error);
    bg_error->PermitUncheckedError();
    if (*auto_recovery) {
      listener->OnErrorRecoveryBegin(reason, *bg_error, auto_recovery);
    }
  }
  db_mutex->Lock();
}

void EventHelpers::LogAndNotifyTableFileCreationFinished(
    EventLogger* event_logger,
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    const std::string& db_name, const std::string& cf_name,
    const std::string& file_path, int job_id, const FileDescriptor& fd,
    uint64_t oldest_blob_file_number, const TableProperties& table_properties,
    TableFileCreationReason reason, const Status& s,
    const std::string& file_checksum,
    const std::string& file_checksum_func_name) {
  if (!event_logger && listeners.empty()) {
    s.PermitUncheckedError();
    return;
  }

  if (event_logger) {
    JSONWriter jwriter;
    AppendCurrentTime(&jwriter);
    jwriter << "cf_name" << cf_name << "job" << job_id << "event"
            << "table_file_creation" << "file_number" << fd.GetNumber()
            << "file_size" << fd.GetFileSize() << "file_checksum"
            << Slice(file_checksum).ToString(true) << "file_checksum_func_name"
            << file_checksum_func_name << "smallest_seqno" << fd.smallest_seqno
            << "largest_seqno" << fd.largest_seqno;

    // table_properties
    {
      jwriter << "table_properties";
      jwriter.StartObject();

      // basic properties:
      jwriter << "data_size" << table_properties.data_size << "index_size"
              << table_properties.index_size << "index_partitions"
              << table_properties.index_partitions << "top_level_index_size"
              << table_properties.top_level_index_size
              << "index_key_is_user_key"
              << table_properties.index_key_is_user_key
              << "index_value_is_delta_encoded"
              << table_properties.index_value_is_delta_encoded << "filter_size"
              << table_properties.filter_size << "raw_key_size"
              << table_properties.raw_key_size << "raw_average_key_size"
              << SafeDivide(table_properties.raw_key_size,
                            table_properties.num_entries)
              << "raw_value_size" << table_properties.raw_value_size
              << "raw_average_value_size"
              << SafeDivide(table_properties.raw_value_size,
                            table_properties.num_entries)
              << "num_data_blocks" << table_properties.num_data_blocks
              << "num_entries" << table_properties.num_entries
              << "num_filter_entries" << table_properties.num_filter_entries
              << "num_deletions" << table_properties.num_deletions
              << "num_merge_operands" << table_properties.num_merge_operands
              << "num_range_deletions" << table_properties.num_range_deletions
              << "format_version" << table_properties.format_version
              << "fixed_key_len" << table_properties.fixed_key_len
              << "filter_policy" << table_properties.filter_policy_name
              << "column_family_name" << table_properties.column_family_name
              << "column_family_id" << table_properties.column_family_id
              << "comparator" << table_properties.comparator_name
              << "user_defined_timestamps_persisted"
              << table_properties.user_defined_timestamps_persisted
              << "key_largest_seqno" << table_properties.key_largest_seqno
              << "key_smallest_seqno" << table_properties.key_smallest_seqno
              << "merge_operator" << table_properties.merge_operator_name
              << "prefix_extractor_name"
              << table_properties.prefix_extractor_name << "property_collectors"
              << table_properties.property_collectors_names << "compression"
              << table_properties.compression_name << "compression_options"
              << table_properties.compression_options << "creation_time"
              << table_properties.oldest_ancestor_time << "oldest_key_time"
              << table_properties.oldest_key_time << "newest_key_time"
              << table_properties.newest_key_time << "file_creation_time"
              << table_properties.file_creation_time
              << "slow_compression_estimated_data_size"
              << table_properties.slow_compression_estimated_data_size
              << "fast_compression_estimated_data_size"
              << table_properties.fast_compression_estimated_data_size
              << "db_id" << table_properties.db_id << "db_session_id"
              << table_properties.db_session_id << "orig_file_number"
              << table_properties.orig_file_number << "seqno_to_time_mapping";

      if (table_properties.seqno_to_time_mapping.empty()) {
        jwriter << "N/A";
      } else {
        SeqnoToTimeMapping tmp;
        Status status = tmp.DecodeFrom(table_properties.seqno_to_time_mapping);
        if (status.ok()) {
          jwriter << tmp.ToHumanString();
        } else {
          jwriter << "Invalid";
        }
      }

      // user collected properties
      for (const auto& prop : table_properties.readable_properties) {
        jwriter << prop.first << prop.second;
      }
      jwriter.EndObject();
    }

    if (oldest_blob_file_number != kInvalidBlobFileNumber) {
      jwriter << "oldest_blob_file_number" << oldest_blob_file_number;
    }

    jwriter << "status" << s.ToString();

    jwriter.EndObject();

    event_logger->Log(jwriter);
  }

  if (listeners.empty()) {
    return;
  }
  TableFileCreationInfo info;
  info.db_name = db_name;
  info.cf_name = cf_name;
  info.file_path = file_path;
  info.file_size = fd.file_size;
  info.job_id = job_id;
  info.table_properties = table_properties;
  info.reason = reason;
  info.status = s;
  info.file_checksum = file_checksum;
  info.file_checksum_func_name = file_checksum_func_name;
  for (auto& listener : listeners) {
    const ScopedEventListenerCallback callback_scope(listener.get());
    listener->OnTableFileCreated(info);
  }
  info.status.PermitUncheckedError();
}

void EventHelpers::LogAndNotifyTableFileDeletion(
    EventLogger* event_logger, int job_id, uint64_t file_number,
    const std::string& file_path, const Status& status,
    const std::string& dbname, const std::string& db_session_id,
    const std::vector<std::shared_ptr<EventListener>>& listeners) {
  if (!event_logger && listeners.empty()) {
    status.PermitUncheckedError();
    return;
  }

  if (event_logger) {
    JSONWriter jwriter;
    AppendCurrentTime(&jwriter);

    jwriter << "job" << job_id << "event" << "table_file_deletion"
            << "file_number" << file_number << "db_session_id" << db_session_id
            << "status" << status.ToString();

    jwriter.EndObject();

    event_logger->Log(jwriter);
  }

  if (listeners.empty()) {
    return;
  }
  TableFileDeletionInfo info;
  info.db_name = dbname;
  info.job_id = job_id;
  info.file_path = file_path;
  info.status = status;
  for (auto& listener : listeners) {
    const ScopedEventListenerCallback callback_scope(listener.get());
    listener->OnTableFileDeleted(info);
  }
  info.status.PermitUncheckedError();
}

void EventHelpers::NotifyOnErrorRecoveryEnd(
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    const Status& old_bg_error, const Status& new_bg_error,
    InstrumentedMutex* db_mutex) {
  if (!listeners.empty()) {
    db_mutex->AssertHeld();
    // Make copies before releasing mutex to avoid race.
    Status old_bg_error_cp = old_bg_error;
    Status new_bg_error_cp = new_bg_error;
    // release lock while notifying events
    db_mutex->Unlock();
    TEST_SYNC_POINT("NotifyOnErrorRecoveryEnd:MutexUnlocked:1");
    TEST_SYNC_POINT("NotifyOnErrorRecoveryEnd:MutexUnlocked:2");
    for (auto& listener : listeners) {
      BackgroundErrorRecoveryInfo info;
      info.old_bg_error = old_bg_error_cp;
      info.new_bg_error = new_bg_error_cp;
      const ScopedEventListenerCallback callback_scope(listener.get());
      listener->OnErrorRecoveryCompleted(old_bg_error_cp);
      listener->OnErrorRecoveryEnd(info);
      info.old_bg_error.PermitUncheckedError();
      info.new_bg_error.PermitUncheckedError();
    }
    db_mutex->Lock();
  } else {
    old_bg_error.PermitUncheckedError();
    new_bg_error.PermitUncheckedError();
  }
}

void EventHelpers::NotifyBlobFileCreationStarted(
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    const std::string& db_name, const std::string& cf_name,
    const std::string& file_path, int job_id,
    BlobFileCreationReason creation_reason) {
  if (listeners.empty()) {
    return;
  }
  BlobFileCreationBriefInfo info(db_name, cf_name, file_path, job_id,
                                 creation_reason);
  for (const auto& listener : listeners) {
    const ScopedEventListenerCallback callback_scope(listener.get());
    listener->OnBlobFileCreationStarted(info);
  }
}

void EventHelpers::LogAndNotifyBlobFileCreationFinished(
    EventLogger* event_logger,
    const std::vector<std::shared_ptr<EventListener>>& listeners,
    const std::string& db_name, const std::string& cf_name,
    const std::string& file_path, int job_id, uint64_t file_number,
    BlobFileCreationReason creation_reason, const Status& s,
    const std::string& file_checksum,
    const std::string& file_checksum_func_name, uint64_t total_blob_count,
    uint64_t total_blob_bytes) {
  if (!event_logger && listeners.empty()) {
    s.PermitUncheckedError();
    return;
  }

  if (event_logger) {
    JSONWriter jwriter;
    AppendCurrentTime(&jwriter);
    jwriter << "cf_name" << cf_name << "job" << job_id << "event"
            << "blob_file_creation" << "file_number" << file_number
            << "total_blob_count" << total_blob_count << "total_blob_bytes"
            << total_blob_bytes << "file_checksum" << file_checksum
            << "file_checksum_func_name" << file_checksum_func_name << "status"
            << s.ToString();

    jwriter.EndObject();
    event_logger->Log(jwriter);
  }

  if (listeners.empty()) {
    return;
  }
  BlobFileCreationInfo info(db_name, cf_name, file_path, job_id,
                            creation_reason, total_blob_count, total_blob_bytes,
                            s, file_checksum, file_checksum_func_name);
  for (const auto& listener : listeners) {
    const ScopedEventListenerCallback callback_scope(listener.get());
    listener->OnBlobFileCreated(info);
  }
  info.status.PermitUncheckedError();
}

void EventHelpers::LogAndNotifyBlobFileDeletion(
    EventLogger* event_logger,
    const std::vector<std::shared_ptr<EventListener>>& listeners, int job_id,
    uint64_t file_number, const std::string& file_path, const Status& status,
    const std::string& dbname, const std::string& db_session_id) {
  if (!event_logger && listeners.empty()) {
    status.PermitUncheckedError();
    return;
  }

  if (event_logger) {
    JSONWriter jwriter;
    AppendCurrentTime(&jwriter);

    jwriter << "job" << job_id << "event" << "blob_file_deletion"
            << "file_number" << file_number << "db_session_id" << db_session_id
            << "status" << status.ToString();

    jwriter.EndObject();
    event_logger->Log(jwriter);
  }
  if (listeners.empty()) {
    return;
  }
  BlobFileDeletionInfo info(dbname, file_path, job_id, status);
  for (const auto& listener : listeners) {
    const ScopedEventListenerCallback callback_scope(listener.get());
    listener->OnBlobFileDeleted(info);
  }
  info.status.PermitUncheckedError();
}

}  // namespace ROCKSDB_NAMESPACE
