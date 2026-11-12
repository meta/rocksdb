//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

struct TransactionDBOptions;
class TransactionDB;
class OptimisticTransactionDB;

namespace blob_db {
struct BlobDBOptions;
class BlobDB;
}  // namespace blob_db

/*
 * ToolHooks is currently a WORK IN PROGRESS, API is subject to change.
 * ToolHooks is a class that allows users to override the default behavior of
 * the various OpenDB calls used by db_bench_tool.  This allows users to easily
 * extend the functionality of db_bench_tool to support their own open
 * implementations.
 */
class ToolHooks {
 public:
  ToolHooks() = default;
  virtual ~ToolHooks() = default;
  virtual Status Open(const Options& db_options, const std::string& name,
                      std::unique_ptr<DB>* dbptr) = 0;
  virtual Status Open(
      const DBOptions& db_options, const std::string& name,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles,
      std::unique_ptr<DB>* dbptr) = 0;
  virtual Status OpenForReadOnly(const Options& options,
                                 const std::string& name,
                                 std::unique_ptr<DB>* dbptr,
                                 bool error_if_wal_file_exists) = 0;
  virtual Status OpenForReadOnly(
      const Options& options, const std::string& name,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles,
      std::unique_ptr<DB>* dbptr) = 0;
  virtual Status OpenTransactionDB(const Options& db_options,
                                   const TransactionDBOptions& txn_db_options,
                                   const std::string& dbname,
                                   TransactionDB** dbptr) = 0;
  virtual Status OpenTransactionDB(
      const DBOptions& db_options, const TransactionDBOptions& txn_db_options,
      const std::string& dbname,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles, TransactionDB** dbptr) = 0;
  virtual Status OpenOptimisticTransactionDB(
      const Options& options, const std::string& dbname,
      OptimisticTransactionDB** dbptr) = 0;
  virtual Status OpenOptimisticTransactionDB(
      const DBOptions& db_options, const std::string& dbname,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles,
      OptimisticTransactionDB** dbptr) = 0;
  virtual Status OpenAsSecondary(const Options& options,
                                 const std::string& name,
                                 const std::string& secondary_path,
                                 std::unique_ptr<DB>* dbptr) = 0;
  virtual Status OpenAsFollower(const Options& options, const std::string& name,
                                const std::string& leader_path,
                                std::unique_ptr<DB>* dbptr) = 0;
  virtual Status Open(const Options& options,
                      const blob_db::BlobDBOptions& bdb_options,
                      const std::string& dbname, blob_db::BlobDB** blob_db) = 0;
  virtual void Exit(int status) = 0;
};

class DefaultHooks : public ToolHooks {
 public:
  DefaultHooks() = default;
  ~DefaultHooks() override = default;
  Status Open(const Options& db_options, const std::string& name,
              std::unique_ptr<DB>* dbptr) override;
  Status Open(const DBOptions& db_options, const std::string& name,
              const std::vector<ColumnFamilyDescriptor>& column_families,
              std::vector<ColumnFamilyHandle*>* handles,
              std::unique_ptr<DB>* dbptr) override;
  Status OpenForReadOnly(const Options& options, const std::string& name,
                         std::unique_ptr<DB>* dbptr,
                         bool error_if_wal_file_exists) override;
  Status OpenForReadOnly(
      const Options& options, const std::string& name,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles,
      std::unique_ptr<DB>* dbptr) override;
  Status OpenTransactionDB(const Options& db_options,
                           const TransactionDBOptions& txn_db_options,
                           const std::string& dbname,
                           TransactionDB** dbptr) override;
  Status OpenTransactionDB(
      const DBOptions& db_options, const TransactionDBOptions& txn_db_options,
      const std::string& dbname,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles,
      TransactionDB** dbptr) override;
  Status OpenOptimisticTransactionDB(const Options& options,
                                     const std::string& dbname,
                                     OptimisticTransactionDB** dbptr) override;
  Status OpenOptimisticTransactionDB(
      const DBOptions& db_options, const std::string& dbname,
      const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles,
      OptimisticTransactionDB** dbptr) override;
  Status OpenAsSecondary(const Options& options, const std::string& name,
                         const std::string& secondary_path,
                         std::unique_ptr<DB>* dbptr) override;
  Status OpenAsFollower(const Options& options, const std::string& name,
                        const std::string& leader_path,
                        std::unique_ptr<DB>* dbptr) override;
  Status Open(const Options& options, const blob_db::BlobDBOptions& bdb_options,
              const std::string& dbname, blob_db::BlobDB** blob_db) override;

  // NOLINTNEXTLINE(concurrency-mt-unsafe)
  void Exit(int status) override { exit(status); }
};

extern DefaultHooks defaultHooks;

}  // namespace ROCKSDB_NAMESPACE
