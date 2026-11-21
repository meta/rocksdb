//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "db/column_family.h"
#include "db/version_edit.h"
#include "logging/event_logger.h"
#include "rocksdb/listener.h"
#include "rocksdb/table_properties.h"

namespace ROCKSDB_NAMESPACE {

// Blob reference fields of a table_file_creation event, for an SST with exact
// blob reference metadata.
struct TableFileBlobReferenceInfo {
  uint64_t count = 0;
  // True when the blob reference cap closed the output (see
  // CompactionOutputs::Output::closed_by_blob_reference_limit).
  bool closed_by_limit = false;
};

class EventHelpers {
 public:
  static void AppendCurrentTime(JSONWriter* json_writer);
  static void NotifyTableFileCreationStarted(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id, TableFileCreationReason reason);
  static void NotifyOnBackgroundError(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      BackgroundErrorReason reason, Status* bg_error,
      InstrumentedMutex* db_mutex, bool* auto_recovery);
  static void LogAndNotifyTableFileCreationFinished(
      EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id, const FileDescriptor& fd,
      uint64_t oldest_blob_file_number, const TableProperties& table_properties,
      TableFileCreationReason reason, const Status& s,
      const std::string& file_checksum,
      const std::string& file_checksum_func_name,
      const TableFileBlobReferenceInfo* blob_references);
  static void LogAndNotifyTableFileDeletion(
      EventLogger* event_logger, int job_id, uint64_t file_number,
      const std::string& file_path, const Status& status,
      const std::string& db_name, const std::string& db_session_id,
      const std::vector<std::shared_ptr<EventListener>>& listeners);
  static void NotifyOnErrorRecoveryEnd(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const Status& old_bg_error, const Status& new_bg_error,
      InstrumentedMutex* db_mutex);

  static void NotifyBlobFileCreationStarted(
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id,
      BlobFileCreationReason creation_reason);

  static void LogAndNotifyBlobFileCreationFinished(
      EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const std::string& db_name, const std::string& cf_name,
      const std::string& file_path, int job_id, uint64_t file_number,
      BlobFileCreationReason creation_reason, const Status& s,
      const std::string& file_checksum,
      const std::string& file_checksum_func_name, uint64_t total_blob_count,
      uint64_t total_blob_bytes);

  static void LogAndNotifyBlobFileDeletion(
      EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners, int job_id,
      uint64_t file_number, const std::string& file_path, const Status& status,
      const std::string& db_name, const std::string& db_session_id);

 private:
  static void LogAndNotifyTableFileCreation(
      EventLogger* event_logger,
      const std::vector<std::shared_ptr<EventListener>>& listeners,
      const FileDescriptor& fd, const TableFileCreationInfo& info);
};

}  // namespace ROCKSDB_NAMESPACE
