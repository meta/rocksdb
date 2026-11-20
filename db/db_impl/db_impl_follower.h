//  Copyright (c) 2024-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/db_impl/db_impl_secondary.h"
#include "logging/logging.h"
#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

class DBImplFollower : public DBImplSecondary {
 public:
  DBImplFollower(const DBOptions& db_options, std::unique_ptr<Env>&& env,
                 const std::string& dbname, std::string src_path);
  ~DBImplFollower() override;

  Status Close() override;

 protected:
  bool OwnTablesAndLogs() const override {
    // TODO: Change this to true once we've properly implemented file
    // deletion for the read scaling case
    return true;
  }

  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool /*readonly*/, bool /*error_if_wal_file_exists*/,
                 bool /*error_if_data_exists_in_wals*/,
                 bool /*is_retry*/ = false, uint64_t* = nullptr,
                 RecoveryContext* /*recovery_ctx*/ = nullptr,
                 bool* /*can_retry*/ = nullptr) override;

 private:
  friend class DB;

  Status TryCatchUpWithLeader();
  void PeriodicRefresh();

  std::unique_ptr<Env> env_guard_;
  std::unique_ptr<port::Thread> catch_up_thread_;
  std::atomic<bool> stop_requested_;
  std::string src_path_;
  port::Mutex mu_;
  port::CondVar cv_;
  std::unique_ptr<std::list<uint64_t>::iterator> pending_outputs_inserted_elem_;
};
}  // namespace ROCKSDB_NAMESPACE
